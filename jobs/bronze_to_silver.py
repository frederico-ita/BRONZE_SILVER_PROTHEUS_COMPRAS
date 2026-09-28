"""SC7: ler bronze, limpar, deduplicar e aplicar merge/delete no Iceberg."""
import argparse
import os
import re
import unicodedata
from decimal import Decimal
from uuid import uuid4

import awswrangler as wr
import boto3
import pandas as pd
from dotenv import load_dotenv

DEFAULTS = {
    "MERGE_KEYS": "r_e_c_n_o", "DELETION_COLUMN": "d_e_l_e_t_d",
    "DATE_COLUMN": "extraction_date", "DATE_FORMAT": "ISO8601",
    "CSV_SEPARATOR": ";", "ENCODING": "utf-8", "DECIMAL_SEPARATOR": ".",
    "C7_EMISSAO_FORMAT": "%Y%m%d", "C7_DATPRF_FORMAT": "%Y%m%d",
}
REQUIRED = "SOURCE_BUCKET SOURCE_PREFIX DATABASE TABLE TABLE_LOCATION TEMP_PATH S3_OUTPUT WORKGROUP".split()
PROCESSING_ENV_KEYS = [*REQUIRED, *DEFAULTS]
CODES = "c7_filial c7_num c7_item c7_produto c7_numsc c7_local c7_fornece c7_loja".split()
NUMBERS = "c7_quant c7_quje c7_preco c7_total".split()
DATES = ["c7_emissao", "c7_datprf"]


def snake_case(name):
    name = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").lower()


def resolve_s3_path(path, bucket=None):
    path = path.strip()
    if "://" in path and not path.startswith("s3://"):
        raise ValueError("Use um prefixo ou URI s3://")
    if not path.startswith("s3://"):
        if not bucket or not re.fullmatch(r"[a-z0-9.-]+", bucket):
            raise ValueError("SILVER_BUCKET deve conter o nome do bucket")
        path = "s3://" + bucket + "/" + path.lstrip("/")
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
        config[key] = resolve_s3_path(config[key], env.get("SILVER_BUCKET"))
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
    config["dtype"].update(dict.fromkeys(DATES, "date"))
    config["dtype"]["r_e_c_n_o"] = "bigint"
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


def remove_duplicates(frame, keys, date_column):
    if not keys or frame[keys].isna().any().any() or frame[keys].eq("").any().any():
        raise ValueError("Chave de merge nula ou vazia")
    frame = frame.drop_duplicates()
    latest = frame.groupby(keys)[date_column].transform("max")
    frame = frame.loc[frame[date_column].eq(latest)]
    if frame.duplicated(keys).any():
        raise ValueError("Mesma chave e data com valores conflitantes")
    return frame.reset_index(drop=True)


def transform_file(frame, config):
    frame = frame.copy()
    frame.columns = [snake_case(col) for col in frame.columns]
    if frame.columns.duplicated().any() or any(not col for col in frame.columns):
        raise ValueError("Nomes de colunas inválidos ou duplicados")
    frame = frame.drop(columns=["c7_tipo"], errors="ignore")
    for col in frame.select_dtypes(include=["object", "string"]):
        frame[col] = frame[col].map(clean_value)
    for col in CODES + [config["deletion_column"]]:
        if not frame[col].dropna().map(lambda value: isinstance(value, str)).all():
            raise ValueError("Código deve chegar como texto: " + col)
        frame[col] = frame[col].astype("string")
    for col in NUMBERS:
        frame[col] = frame[col].map(lambda value: decimal_value(value, config["decimal_separator"]))
    frame["r_e_c_n_o"] = pd.array(frame["r_e_c_n_o"].replace("", None), dtype="Int64")
    for col in DATES:
        if config["column_date_formats"][col] == "%Y%m%d" and frame[col].astype("string").str.fullmatch(r"\d{1,7}").any():
            raise ValueError("Data deve usar YYYYMMDD")
        frame[col] = pd.to_datetime(frame[col].replace("", None), format=config["column_date_formats"][col]).dt.date
    date_col = config["date_column"]
    dates = pd.to_datetime(frame[date_col].astype("string"), format=config["date_format"], utc=True)
    if dates.isna().any() or not frame[config["deletion_column"]].isin(["", "*"]).all():
        raise ValueError("Data de extração ou marca de exclusão inválida")
    frame[date_col] = dates.dt.tz_localize(None).dt.floor("ms")
    frame["year"], frame["month"], frame["day"] = dates.dt.year, dates.dt.month, dates.dt.day
    return remove_duplicates(frame, config["merge_keys"], date_col)


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


def process_file(bucket, key, config, session, version_id=None):
    frame = transform_file(read_file(bucket, key, config, session, version_id), config)
    deleted = frame[config["deletion_column"]].eq("*")
    active, removed = frame.loc[~deleted], frame.loc[deleted, config["merge_keys"]]
    options = {key: config[key] for key in ["database", "table", "s3_output", "workgroup"]}
    options.update(temp_path=config["temp_path"].rstrip("/") + "/" + uuid4().hex + "/",
                   merge_cols=config["merge_keys"], keep_files=False, boto3_session=session)
    if not active.empty:
        wr.athena.to_iceberg(df=active, **options, table_location=config["table_location"],
                            partition_cols=["year", "month", "day"], merge_condition="update",
                            dtype=config["dtype"], schema_evolution=False, fill_missing_columns_in_df=False)
    if not removed.empty and wr.catalog.does_table_exist(
        database=config["database"], table=config["table"], boto3_session=session
    ):
        wr.athena.delete_from_iceberg_table(df=removed, **options)
    return len(active)


def process_prefix(config, session):
    """Processa os Parquets do prefixo, inclusive subpastas, um por vez."""
    bucket = config["source_bucket"]
    pages = session.client("s3").get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=config["source_prefix"]
    )
    files, rows = 0, 0
    for page in pages:
        for item in page.get("Contents", []):
            if item["Key"].lower().endswith(".parquet"):
                rows += process_file(bucket, item["Key"], config, session)
                files += 1
    print(f"Arquivos Parquet processados: {files}; registros enviados ao merge: {rows}")
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
