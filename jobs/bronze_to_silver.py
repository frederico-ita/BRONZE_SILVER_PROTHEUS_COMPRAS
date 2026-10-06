"""SC7: ler bronze, limpar, deduplicar e aplicar merge/delete no Iceberg."""
import argparse
import os
import re
import unicodedata
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import awswrangler as wr
import boto3
import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv

DEFAULTS = {
    "MERGE_KEYS": "R_E_C_N_O_", "DELETION_COLUMN": "D_E_L_E_T_",
    "DATE_COLUMN": "_airbyte_extracted_at", "DATE_FORMAT": "ISO8601",
    "CSV_SEPARATOR": ";", "ENCODING": "utf-8", "DECIMAL_SEPARATOR": ".",
    "C7_EMISSAO_FORMAT": "%Y%m%d", "C7_DATPRF_FORMAT": "%Y%m%d",
}
REQUIRED = "SOURCE_BUCKET SOURCE_PREFIX DATABASE TABLE TABLE_LOCATION TEMP_PATH S3_OUTPUT WORKGROUP".split()
PROCESSING_ENV_KEYS = [*REQUIRED, *DEFAULTS]
CODES = "c7_filial c7_num c7_item c7_produto c7_numsc c7_local c7_fornece c7_loja".split()
NUMBERS = "c7_quant c7_quje c7_preco c7_total".split()
DATES = ["c7_emissao", "c7_datprf"]
# Origem em snake_case: nome desejado na silver. Somente estas colunas e as
# colunas tecnicas (chaves, exclusao, extracao e particoes) serao gravadas.
COLUMN_MAP = {
    "c7_filial": "c7_filial", "c7_num": "c7_num", "c7_item": "c7_item",
    "c7_produto": "c7_produto", "c7_quant": "c7_quant", "c7_quje": "c7_quje",
    "c7_preco": "c7_preco", "c7_total": "c7_total", "c7_numsc": "c7_numsc",
    "c7_emissao": "c7_emissao", "c7_datprf": "c7_datprf", "c7_local": "c7_local",
    "c7_fornece": "c7_fornece", "c7_loja": "c7_loja",
}


def snake_case(name):
    name = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").lower()


def resolve_s3_path(path, bucket=None, allow_bucket_root=False):
    path = path.strip()
    if "://" in path and not path.startswith("s3://"):
        raise ValueError("Use um prefixo ou URI s3://")
    if not path.startswith("s3://"):
        if not bucket or not re.fullmatch(r"[a-z0-9.-]+", bucket):
            raise ValueError("SILVER_BUCKET deve conter o nome do bucket")
        path = "s3://" + bucket + "/" + path.lstrip("/")
    if allow_bucket_root and re.fullmatch(r"s3://[a-z0-9.-]+/?", path):
        return path.rstrip("/") + "/"
    if not re.fullmatch(r"s3://[a-z0-9.-]+/[^:/].*", path):
        raise ValueError("Caminho S3 deve conter bucket e prefixo")
    return path


def load_config(environ=None):
    env = dict(os.environ if environ is None else environ)
    env["SOURCE_BUCKET"] = env.get("SOURCE_BUCKET") or env.get("BRONZE_BUCKET", "")
    missing = [key for key in REQUIRED if not env.get(key, "").strip()]
    if missing:
        raise ValueError("Variáveis obrigatórias: " + ", ".join(missing))
    config = {key.lower(): env.get(key, DEFAULTS.get(key, "")).strip() for key in PROCESSING_ENV_KEYS}
    for key in ["table_location", "temp_path", "s3_output"]:
        config[key] = resolve_s3_path(config[key], env.get("SILVER_BUCKET"), allow_bucket_root=key == "s3_output")
    paths = [config[k].rstrip("/") + "/" for k in ["table_location", "temp_path", "s3_output"]]
    paths.append("s3://" + config["source_bucket"] + "/" + config["source_prefix"])
    if not config["source_prefix"].endswith("/") or any(
        a.startswith(b) or b.startswith(a) for i, a in enumerate(paths) for b in paths[i + 1:]
    ):
        raise ValueError("Use prefixos separados, com SOURCE_PREFIX terminado em /")
    config["merge_keys"] = [snake_case(key) for key in config["merge_keys"].split(",")]
    for key in ["deletion_column", "date_column"]:
        config[key] = snake_case(config[key])
    config["column_date_formats"] = {col: config[col + "_format"] for col in DATES}
    config["dtype"] = dict.fromkeys(CODES + [config["deletion_column"]], "string")
    config["dtype"].update(dict.fromkeys(NUMBERS, "decimal(18,6)"))
    config["dtype"].update(dict.fromkeys(DATES, "string"))
    config["dtype"]["r_e_c_n_o"] = "bigint"
    config["columns"] = dict(COLUMN_MAP)
    for col in [*config["merge_keys"], "r_e_c_n_o", config["deletion_column"],
                config["date_column"], "year", "month", "day"]:
        config["columns"].setdefault(col, col)
    names = list(config["columns"].values())
    if len(set(names)) != len(names) or any(not name or snake_case(name) != name for name in names):
        raise ValueError("COLUMN_MAP: nomes de destino devem ser unicos e em snake_case")
    return config


def clean_value(value):
    if not isinstance(value, str):
        return value
    value = unicodedata.normalize("NFC", value)
    value = "".join(c for c in value if c.isspace() or unicodedata.category(c) not in {"Cc", "Cf", "Cs"})
    return " ".join(value.split())


def decimal_value(value, separator):
    if pd.isna(value) or value == "":
        return None
    text = str(value)
    if separator not in {".", ","} or (separator == "," and isinstance(value, str) and "." in text):
        raise ValueError("Separador decimal inválido")
    number = Decimal(text.replace(",", ".") if separator == "," else text)
    if not number.is_finite() or abs(number) >= 10**12 or number != number.quantize(Decimal("0.000001")):
        raise ValueError("Número fora de decimal(18,6)")
    return number


def remove_duplicates(frame, keys):
    if not keys or frame[keys].isna().any().any() or frame[keys].eq("").any().any():
        raise ValueError("Chave de merge nula ou vazia")
    return frame.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)


def convert_dates(values, date_format, utc=False):
    """Tenta pandas; para datas fora do limite, usa datetime e milissegundos."""
    try:
        return pd.to_datetime(values, format=date_format, utc=utc)
    except pd.errors.OutOfBoundsDatetime:
        def parse(value):
            if pd.isna(value) or value == "":
                return None
            text = str(value)
            if date_format == "ISO8601":
                return datetime.fromisoformat(text.replace("Z", "+00:00"))
            return datetime.strptime(text, date_format)

        return pd.Series([parse(value) for value in values], index=values.index,
                         dtype="datetime64[ms, UTC]" if utc else "datetime64[ms]")


def transform_file(frame, config):
    frame = frame.copy()
    frame.columns = [snake_case(col) for col in frame.columns]
    if frame.columns.duplicated().any() or any(not col for col in frame.columns):
        raise ValueError("Nomes de colunas inválidos ou duplicados")
    selected = [col for col in config["columns"] if col not in {"year", "month", "day"}]
    frame = frame.loc[:, selected].copy()
    for col in frame.select_dtypes(include=["object", "string"]):
        if col not in DATES:
            frame[col] = frame[col].map(clean_value)
    for col in CODES + [config["deletion_column"]]:
        if col not in frame:
            continue
        if not frame[col].dropna().map(lambda value: isinstance(value, str)).all():
            raise ValueError("Código deve chegar como texto: " + col)
        frame[col] = frame[col].astype("string")
    for col in NUMBERS:
        if col not in frame:
            continue
        values = frame[col].map(lambda value: decimal_value(value, config["decimal_separator"]))
        frame[col] = pd.array(values, dtype=pd.ArrowDtype(pa.decimal128(18, 6)))
    frame["r_e_c_n_o"] = pd.array(frame["r_e_c_n_o"].replace("", None), dtype="Int64")
    for col in DATES:
        if col in frame:
            frame[col] = frame[col].astype("string")
    date_col = config["date_column"]
    try:
        dates = convert_dates(frame[date_col].astype("string"), config["date_format"], utc=True)
    except (ValueError, OverflowError) as error:
        raise ValueError(f"Erro na coluna {date_col}: {error}") from error
    if dates.isna().any() or not frame[config["deletion_column"]].isin(["", "*"]).all():
        raise ValueError("Data de extração ou marca de exclusão inválida")
    frame[date_col] = dates.dt.tz_localize(None).dt.floor("ms")
    frame["year"], frame["month"], frame["day"] = dates.dt.year, dates.dt.month, dates.dt.day
    for col in frame.columns[frame.dtypes.eq(object)]:
        frame[col] = frame[col].convert_dtypes()
        if frame[col].dtype == "object":
            frame[col] = frame[col].astype("string")
    frame = remove_duplicates(frame, config["merge_keys"])
    return frame.loc[:, list(config["columns"])].rename(columns=config["columns"])


def read_file(bucket, key, config, session, version_id=None):
    if bucket != config["source_bucket"] or not key.startswith(config["source_prefix"]):
        raise ValueError("Objeto fora da origem configurada")
    options = dict(path=["s3://" + bucket + "/" + key], boto3_session=session, version_id=version_id)
    extension = key.rsplit(".", 1)[-1].lower()
    if extension == "csv":
        return wr.s3.read_csv(**options, dtype="string", keep_default_na=False,
                              sep=config["csv_separator"], encoding=config["encoding"])
    if extension == "parquet":
        return wr.s3.read_parquet(**options)
    if extension in {"json", "jsonl", "ndjson"}:
        return wr.s3.read_json(**options, lines=extension != "json", dtype=False, convert_dates=False)
    raise ValueError("Formato não suportado: " + extension)


def write_frame(frame, config, session):
    columns = config["columns"]
    merge_keys = [columns[col] for col in config["merge_keys"]]
    deleted = frame[columns[config["deletion_column"]]].eq("*")
    active, removed = frame.loc[~deleted], frame.loc[deleted, merge_keys]
    options = {key: config[key] for key in ["database", "table", "s3_output", "workgroup"]}
    options.update(temp_path=config["temp_path"].rstrip("/") + "/" + uuid4().hex + "/",
                   merge_cols=merge_keys, keep_files=False, boto3_session=session)
    if not active.empty:
        wr.athena.to_iceberg(df=active, **options, table_location=config["table_location"],
                            partition_cols=[columns[col] for col in ["year", "month", "day"]],
                            merge_condition="update", schema_evolution=True, fill_missing_columns_in_df=False,
                            dtype={columns[col]: dtype for col, dtype in config["dtype"].items() if col in columns})
    if not removed.empty and wr.catalog.does_table_exist(
        database=config["database"], table=config["table"], boto3_session=session
    ):
        wr.athena.delete_from_iceberg_table(df=removed, **options)
    return len(active)


def validate_schema(frame, expected, config):
    """Compara tipos sem executar DDL ou escrever no S3."""
    dtype = {config["columns"][col]: kind for col, kind in config["dtype"].items()
             if col in config["columns"]}
    actual, _ = wr.catalog.extract_athena_types(df=frame, index=False, dtype=dtype)
    missing = sorted(set(expected) - set(actual))
    incompatible = {}
    for col in set(expected) & set(actual):
        old, new = expected[col].replace(" ", ""), actual[col].replace(" ", "")
        decimals = [re.fullmatch(r"decimal\((\d+),(\d+)\)", kind) for kind in [old, new]]
        widening = all(decimals) and decimals[0][2] == decimals[1][2] and int(decimals[0][1]) <= int(decimals[1][1])
        if old != new and (old, new) not in {("int", "bigint"), ("float", "double")} and not widening:
            incompatible[col] = (old, new)
    if missing or incompatible:
        raise ValueError(f"Schema incompativel antes da escrita: ausentes={missing}; tipos={incompatible}")
    return actual


def process_batch(bucket, sources, config, session):
    """Valida o lote inteiro e guarda os frames em disco local antes de escrever."""
    if not sources:
        return 0
    expected = wr.catalog.get_table_types(database=config["database"], table=config["table"],
                                          boto3_session=session) or {}
    with TemporaryDirectory(prefix="silver-validation-") as folder:
        paths = []
        for key, version in sources:
            try:
                frame = transform_file(read_file(bucket, key, config, session, version), config)
                if frame.empty:
                    continue
                active = frame.loc[frame[config["columns"][config["deletion_column"]]].eq("")]
                if not active.empty:
                    expected = validate_schema(active, expected, config)
                    partitions = [config["columns"][col] for col in ["year", "month", "day"]]
                    if len(active[partitions].drop_duplicates()) > 100:
                        raise ValueError("Mais de 100 particoes no arquivo")
                path = Path(folder) / f"{len(paths)}.pkl"
                frame.to_pickle(path)
                paths.append(path)
            except Exception as error:
                raise ValueError(f"Validacao falhou no arquivo {key}: {error}") from error
        print(f"Validacao concluida: {len(sources)} arquivos. Iniciando escrita.")
        # Le somente os arquivos locais produzidos acima, nunca pickle da origem.
        return sum(write_frame(pd.read_pickle(path), config, session) for path in paths)


def process_file(bucket, key, config, session, version_id=None):
    return process_batch(bucket, [(key, version_id)], config, session)


def process_prefix(config, session):
    """Lista todos os Parquets e valida o lote antes da primeira escrita."""
    bucket = config["source_bucket"]
    pages = session.client("s3").get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=config["source_prefix"]
    )
    sources = []
    for page in pages:
        for item in page.get("Contents", []):
            if item["Key"].lower().endswith(".parquet"):
                sources.append((item["Key"], None))
    rows = process_batch(bucket, sources, config, session)
    print(f"Arquivos Parquet processados: {len(sources)}; registros enviados ao merge: {rows}")
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env")
    for name in ["source-bucket", "source-key", "source-version-id"]:
        parser.add_argument("--" + name)
    for name in PROCESSING_ENV_KEYS:
        parser.add_argument("--env-" + name)
    args, _ = parser.parse_known_args(argv)
    load_dotenv(args.env_file, override=False)
    for name in PROCESSING_ENV_KEYS:
        value = getattr(args, "env_" + name)
        if value is not None:
            os.environ[name] = value
    key = args.source_key or os.getenv("SOURCE_KEY")
    version = args.source_version_id or os.getenv("SOURCE_VERSION_ID")
    if version and not key:
        parser.error("SOURCE_VERSION_ID exige um arquivo específico em --source-key ou SOURCE_KEY")
    config = load_config()
    if args.source_bucket and args.source_bucket != config["source_bucket"]:
        raise ValueError("Objeto fora da origem configurada")
    session = boto3.Session(region_name=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"))
    if key:
        return process_file(config["source_bucket"], key, config, session, version)
    return process_prefix(config, session)


if __name__ == "__main__":  # pragma: no cover
    main()
