"""Glue Python Shell: S3 -> pandas -> Athena MERGE -> Iceberg, sem Spark."""

import argparse
import logging
import os
import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import PurePosixPath
from urllib.parse import urlparse
from uuid import uuid4

import awswrangler as wr
import boto3
import pandas as pd
from dotenv import load_dotenv

LOGGER = logging.getLogger(__name__)
PARTITIONS = ["year", "month", "day"]
PROCESSING_ENV_KEYS = (
    "SOURCE_BUCKET", "SOURCE_PREFIX", "DATABASE", "TABLE", "TABLE_LOCATION",
    "TEMP_PATH", "S3_OUTPUT", "WORKGROUP", "MERGE_KEYS", "DELETION_COLUMN",
    "DATE_COLUMN", "DATE_FORMAT", "CSV_SEPARATOR", "ENCODING", "DECIMAL_SEPARATOR",
    "C7_EMISSAO_FORMAT", "C7_DATPRF_FORMAT",
)
SC7_DTYPES = {
    "c7_filial": "string", "c7_tipo": "string", "c7_num": "string",
    "c7_item": "string", "c7_produto": "string", "c7_quant": "decimal(18,6)",
    "c7_quje": "decimal(18,6)", "c7_preco": "decimal(18,6)", "c7_total": "decimal(18,6)",
    "c7_numsc": "string", "c7_emissao": "date", "c7_datprf": "date",
    "c7_local": "string", "c7_fornece": "string", "c7_loja": "string",
    "d_e_l_e_t_d": "string", "r_e_c_n_o": "bigint",
}


def snake_case(name):
    """Normaliza nomes; não remove acentos do conteúdo dos registros."""
    name = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").lower()
    if not name:
        raise ValueError("Nome de coluna vazio após normalização")
    return "col_" + name if name[0].isdigit() else name


def normalize_columns(frame):
    names = [snake_case(col) for col in frame.columns]
    if len(names) != len(set(names)):
        raise ValueError("Colunas colidem após normalização para snake_case")
    result = frame.copy()
    result.columns = names
    return result


def clean_value(value):
    """Preserva tipos, nulos, acentos e pontuação; elimina controles invisíveis."""
    if not isinstance(value, str):
        return value
    value = unicodedata.normalize("NFC", value)
    value = "".join(
        " " if char.isspace() else char
        for char in value
        if char.isspace() or unicodedata.category(char) not in {"Cc", "Cf", "Cs"}
    )
    return re.sub(r"\s+", " ", value).strip()


def clean_strings(frame):
    result = frame.copy()
    for col in result.select_dtypes(include=["object", "string"]).columns:
        result[col] = result[col].map(clean_value)
    return result


def parse_decimal(value, precision, scale, separator="."):
    """Converte números sem arredondar silenciosamente nem inferir separador de milhar."""
    if pd.isna(value) or value == "":
        return None
    text = str(value)
    if separator == ",":
        if "." in text and isinstance(value, str):
            raise ValueError("Separador numérico incompatível")
        text = text.replace(",", ".")
    if not re.fullmatch(r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", text):
        raise ValueError("Número inválido")
    try:
        with localcontext() as context:
            context.prec = max(precision, 38) + 2
            number = Decimal(text)
            converted = number.quantize(Decimal(1).scaleb(-scale))
            if number != converted or abs(converted) >= Decimal(10) ** (precision - scale):
                raise ValueError("Número excede a precisão ou escala configurada")
            return converted
    except InvalidOperation as error:
        raise ValueError("Número excede a precisão configurada") from error


def parse_business_date(value, date_format):
    if pd.isna(value) or value == "":
        return None
    if isinstance(value, (datetime, date)):
        return value.date() if isinstance(value, datetime) else value
    text = str(value)
    # strptime aceita datas compactas com menos de oito dígitos; rejeitá-las.
    if date_format == "%Y%m%d" and not re.fullmatch(r"\d{8}", text):
        raise ValueError("Data deve usar YYYYMMDD")
    return datetime.strptime(text, date_format).date()


def apply_column_types(frame, config):
    """Aplica o contrato da tabela antes da deduplicação e da escrita no Athena."""
    result = frame.copy()
    for column, target_type in config.get("dtype", {}).items():
        if column not in result:
            raise ValueError("Coluna obrigatória ausente: " + column)
        try:
            if target_type == "string":
                if result[column].dropna().map(lambda value: not isinstance(value, str)).any():
                    raise ValueError("Código deve chegar como texto para preservar zeros à esquerda")
                result[column] = result[column].astype("string")
            elif target_type == "date":
                date_format = config.get("column_date_formats", {}).get(column, "%Y%m%d")
                result[column] = result[column].map(lambda value: parse_business_date(value, date_format))
            elif target_type == "bigint":
                numbers = result[column].map(lambda value: parse_decimal(value, 19, 0))
                if any(value is not None and not -(2**63) <= value < 2**63 for value in numbers):
                    raise ValueError("Inteiro fora do intervalo bigint")
                result[column] = pd.array(numbers, dtype="Int64")
            else:
                match = re.fullmatch(r"decimal\((\d+),(\d+)\)", target_type)
                if not match:
                    raise ValueError("Tipo não suportado: " + target_type)
                precision, scale = map(int, match.groups())
                if not 0 <= scale <= precision <= 38 or precision == 0:
                    raise ValueError("Precisão/escala decimal inválida")
                separator = config.get("decimal_separator", ".")
                if separator not in {".", ","}:
                    raise ValueError("decimal_separator deve ser . ou ,")
                result[column] = result[column].map(
                    lambda value: parse_decimal(value, precision, scale, separator)
                )
        except (ValueError, TypeError, OverflowError) as error:
            raise ValueError("Falha na coluna " + column + ": " + str(error)) from error
    return result


def add_date_partitions(frame, date_column="extraction_date", date_format="ISO8601"):
    """Datas sem timezone são UTC; partições usam o dia em UTC."""
    date_column = snake_case(date_column)
    if date_column not in frame:
        raise ValueError("Coluna de data ausente: " + date_column)
    if set(PARTITIONS).intersection(frame.columns):
        raise ValueError("year/month/day são nomes reservados para partições")
    result = frame.copy()
    # Datas numéricas precisam de formato explícito; evita interpretá-las como nanos.
    values = result[date_column].astype("string")
    dates = pd.to_datetime(values, format=date_format, errors="coerce", utc=True)
    if dates.isna().any():
        raise ValueError("Data de extração nula ou inválida")
    # Athena Iceberg usa timestamps sem timezone, com precisão de milissegundos.
    result[date_column] = dates.dt.tz_localize(None).dt.floor("ms")
    result["year"] = dates.dt.year.astype("int32")
    result["month"] = dates.dt.month.astype("int32")
    result["day"] = dates.dt.day.astype("int32")
    return result


def remove_duplicates(frame, merge_keys, date_column="extraction_date"):
    """Mantém a extração mais recente por chave; rejeita empates conflitantes."""
    keys = [snake_case(key) for key in merge_keys]
    date_column = snake_case(date_column)
    if not keys or len(keys) != len(set(keys)):
        raise ValueError("Informe chaves únicas para o merge")
    if not set(keys + [date_column]).issubset(frame.columns):
        raise ValueError("Colunas de chave ou data ausentes")
    if frame[keys].isna().any().any() or frame[keys].eq("").any().any():
        raise ValueError("Chaves de merge não podem ser nulas ou vazias")
    result = frame.drop_duplicates()
    latest = result.groupby(keys, dropna=False)[date_column].transform("max")
    result = result.loc[result[date_column].eq(latest)]
    if result.duplicated(subset=keys).any():
        raise ValueError("Registros conflitantes com mesma chave e data de extração")
    return result.reset_index(drop=True)


def validate_config(config):
    required = ["source_bucket", "source_prefix", "database", "table", "merge_keys",
                "table_location", "temp_path", "s3_output", "workgroup"]
    for key in required:
        if not config.get(key):
            raise ValueError("Configuração obrigatória: " + key)
    if not isinstance(config["merge_keys"], list):
        raise ValueError("merge_keys deve ser uma lista")
    if not config["source_prefix"].endswith("/"):
        raise ValueError("source_prefix deve terminar em /")
    keys = [snake_case(key) for key in config["merge_keys"]]
    if len(keys) != len(set(keys)) or set(keys).intersection(PARTITIONS):
        raise ValueError("Chaves repetidas ou reservadas")
    for key in ["database", "table"]:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", config[key]):
            raise ValueError("Identificador inválido: " + key)
    for key in ["table_location", "temp_path", "s3_output"]:
        uri = urlparse(config[key])
        if uri.scheme != "s3" or not uri.netloc or uri.path in {"", "/"}:
            raise ValueError("URI S3 deve incluir bucket e prefixo: " + key)
    paths = [config[key].rstrip("/") + "/" for key in
             ["table_location", "temp_path", "s3_output"]]
    paths.append("s3://" + config["source_bucket"] + "/" + config["source_prefix"].rstrip("/") + "/")
    if any(a.startswith(b) or b.startswith(a)
           for i, a in enumerate(paths) for b in paths[i + 1:]):
        raise ValueError("Prefixos de origem, destino, staging e resultados devem ser separados")
    return config


def load_config(environ=None):
    """Lê somente configuração de processamento; credenciais ficam com o boto3."""
    env = os.environ if environ is None else environ
    required = ["SOURCE_BUCKET", "SOURCE_PREFIX", "DATABASE", "TABLE",
                "TABLE_LOCATION", "TEMP_PATH", "S3_OUTPUT", "WORKGROUP"]
    missing = [key for key in required if not env.get(key, "").strip()]
    if missing:
        raise ValueError("Variáveis de ambiente obrigatórias: " + ", ".join(missing))
    config = {key.lower(): env[key].strip() for key in required}
    config.update({
        "merge_keys": [snake_case(key.strip()) for key in env.get("MERGE_KEYS", "r_e_c_n_o").split(",")],
        "deletion_column": snake_case(env.get("DELETION_COLUMN", "d_e_l_e_t_d")),
        "date_column": snake_case(env.get("DATE_COLUMN", "extraction_date")),
        "date_format": env.get("DATE_FORMAT", "ISO8601"),
        "csv_separator": env.get("CSV_SEPARATOR", ";"),
        "encoding": env.get("ENCODING", "utf-8"),
        "decimal_separator": env.get("DECIMAL_SEPARATOR", "."),
        "column_date_formats": {
            "c7_emissao": env.get("C7_EMISSAO_FORMAT", "%Y%m%d"),
            "c7_datprf": env.get("C7_DATPRF_FORMAT", "%Y%m%d"),
        },
        "dtype": dict(SC7_DTYPES),
    })
    config["dtype"].pop("d_e_l_e_t_d")
    config["dtype"][config["deletion_column"]] = "string"
    return validate_config(config)


def read_file(bucket, key, config, session, version_id=None):
    """Lê somente o objeto recebido; não varre novamente o bucket."""
    if bucket != config["source_bucket"] or not key.startswith(config["source_prefix"]):
        raise ValueError("Objeto fora da origem configurada")
    options = {"path": ["s3://" + bucket + "/" + key], "boto3_session": session}
    if version_id:
        options["version_id"] = version_id
    extension = PurePosixPath(key).suffix.lower()
    if extension == ".csv":
        return wr.s3.read_csv(**options, dtype="string", keep_default_na=False,
                              sep=config.get("csv_separator", ","),
                              encoding=config.get("encoding", "utf-8"))
    if extension == ".parquet":
        return wr.s3.read_parquet(**options)
    if extension in {".json", ".jsonl", ".ndjson"}:
        return wr.s3.read_json(**options, lines=extension != ".json", dtype=False,
                               convert_dates=False)
    raise ValueError("Formato não suportado: " + extension)


def transform_file(frame, config):
    result = clean_strings(normalize_columns(frame))
    result = apply_column_types(result, config)
    result = add_date_partitions(result, config.get("date_column", "extraction_date"),
                                 config.get("date_format", "ISO8601"))
    return remove_duplicates(result, config["merge_keys"], config.get("date_column", "extraction_date"))


def merge_iceberg(frame, config, session):
    if frame.empty:
        return 0
    # UUID isola arquivos temporários entre tentativas; não altera dados do registro.
    wr.athena.to_iceberg(
        df=frame, database=config["database"], table=config["table"],
        table_location=config["table_location"],
        temp_path=config["temp_path"].rstrip("/") + "/" + uuid4().hex + "/",
        s3_output=config["s3_output"], workgroup=config["workgroup"],
        partition_cols=PARTITIONS,
        merge_cols=[snake_case(key) for key in config["merge_keys"]],
        merge_condition="update", merge_match_nulls=False, keep_files=False,
        schema_evolution=False, fill_missing_columns_in_df=False,
        dtype=config.get("dtype"), boto3_session=session,
    )
    return len(frame)


def split_deleted(frame, config):
    column = config.get("deletion_column")
    if not column:
        return frame, frame.iloc[:0]
    column = snake_case(column)
    if column not in frame:
        raise ValueError("Coluna de exclusão ausente: " + column)
    flags = frame[column]
    if flags.isna().any() or not flags.isin(["", "*"]).all():
        raise ValueError("Marca de exclusão deve ser branco (ativo) ou * (excluído)")
    deleted = flags.eq("*")
    return frame.loc[~deleted].copy(), frame.loc[deleted].copy()


def delete_iceberg(frame, config, session):
    if frame.empty:
        return 0
    if not wr.catalog.does_table_exist(database=config["database"], table=config["table"],
                                       boto3_session=session):
        return 0
    keys = [snake_case(key) for key in config["merge_keys"]]
    wr.athena.delete_from_iceberg_table(
        df=frame[keys], database=config["database"], table=config["table"], merge_cols=keys,
        temp_path=config["temp_path"].rstrip("/") + "/" + uuid4().hex + "/",
        s3_output=config["s3_output"], workgroup=config["workgroup"],
        keep_files=False, boto3_session=session,
    )
    return len(frame)


def process_file(bucket, key, config, session, version_id=None):
    frame = read_file(bucket, key, config, session, version_id)
    result = transform_file(frame, config)
    active, deleted = split_deleted(result, config)
    rows = merge_iceberg(active, config, session)
    delete_keys = delete_iceberg(deleted, config, session)
    LOGGER.info("Tabela=%s linhas_lidas=%d linhas_merge=%d chaves_delete=%d",
                config["table"], len(frame), rows, delete_keys)
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--source-bucket")
    parser.add_argument("--source-key")
    parser.add_argument("--source-version-id", default=None)
    for name in PROCESSING_ENV_KEYS:
        parser.add_argument("--env-" + name)
    args, _ = parser.parse_known_args(argv)  # Glue injeta seus próprios argumentos.
    load_dotenv(dotenv_path=args.env_file, override=False)
    for name in PROCESSING_ENV_KEYS:
        value = getattr(args, "env_" + name)
        if value is not None:
            os.environ[name] = value
    key = args.source_key or os.getenv("SOURCE_KEY")
    if not key:
        parser.error("Informe --source-key ou SOURCE_KEY")
    config = load_config()
    session = boto3.Session(region_name=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"))
    return process_file(args.source_bucket or config["source_bucket"], key, config, session,
                        args.source_version_id or os.getenv("SOURCE_VERSION_ID"))


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    main()
