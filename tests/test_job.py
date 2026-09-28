from unittest.mock import Mock

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from jobs import bronze_to_silver as job


@pytest.mark.parametrize("value,expected", [
    (" Data Extração ", "data_extracao"), ("HTTPResponseID", "http_response_id"),
    ("extractionDate", "extraction_date"), ("123 código", "col_123_codigo"),
    ("C7_NUM", "c7_num"),
])
def test_snake_case(value, expected):
    assert job.snake_case(value) == expected


def test_invalid_column_and_collision():
    with pytest.raises(ValueError, match="vazio"):
        job.snake_case("!!!")
    with pytest.raises(ValueError, match="colidem"):
        job.normalize_columns(pd.DataFrame(columns=["A B", "a_b"]))


@pytest.mark.parametrize("value,expected", [
    ("  São\tPaulo\n  ", "São Paulo"), ("A\x00B\u200bC\ufeff", "ABC"),
    ("  R$ 1.234,56 / café! ", "R$ 1.234,56 / café!"),
    ("cafe\u0301", "café"), (None, None), (42, 42), ("  ", ""),
])
def test_clean_value(value, expected):
    assert job.clean_value(value) == expected


def test_clean_preserves_input_nulls_and_types():
    frame = pd.DataFrame({"texto": [" x ", None], "numero": [1, 2],
                          "string": pd.Series([" y ", pd.NA], dtype="string")})
    original = frame.copy(deep=True)
    result = job.clean_strings(frame)
    assert result.loc[0, "texto"] == "x"
    assert result.loc[0, "numero"] == 1
    assert pd.isna(result.loc[1, "string"])
    assert_frame_equal(frame, original)


def test_date_alias_timezone_and_precision():
    frame = pd.DataFrame({"data_extracao": ["2026-01-31T23:30:00.123456-03:00"]})
    result = job.add_date_partitions(frame, "Data Extração")
    assert result.loc[0, ["year", "month", "day"]].tolist() == [2026, 2, 1]
    assert result.loc[0, "data_extracao"] == pd.Timestamp("2026-02-01 02:30:00.123")
    assert list(frame.columns) == ["data_extracao"]


def test_explicit_numeric_date_format():
    result = job.add_date_partitions(pd.DataFrame({"extraction_date": [20260928]}),
                                    date_format="%Y%m%d")
    assert result.loc[0, "day"] == 28


@pytest.mark.parametrize("frame,message", [
    (pd.DataFrame({"x": [1]}), "ausente"),
    (pd.DataFrame({"extraction_date": ["invalid"]}), "inválida"),
    (pd.DataFrame({"extraction_date": [None]}), "inválida"),
    (pd.DataFrame({"extraction_date": ["2026-01-01"], "year": [2000]}), "reservados"),
])
def test_invalid_partitions(frame, message):
    with pytest.raises(ValueError, match=message):
        job.add_date_partitions(frame)


def test_latest_composite_keys_and_idempotent_transform(config):
    config["merge_keys"] = ["id", "filial"]
    frame = pd.DataFrame({"ID": ["001", "001", "001", "001"], "Filial": ["01", "01", "01", "02"],
                          "ExtractionDate": ["2026-01-01", "2026-02-02", "2026-02-02", "2026-01-01"],
                          "Descrição": ["velho", " novo ", "novo", " outro "]})
    original = frame.copy(deep=True)
    result = job.transform_file(frame, config)
    assert len(result) == 2
    assert result["descricao"].tolist() == ["novo", "outro"]
    assert result["id"].tolist() == ["001", "001"]
    assert_frame_equal(result, job.transform_file(frame, config))
    assert_frame_equal(frame, original)


@pytest.mark.parametrize("keys,frame,message", [
    ([], pd.DataFrame(), "chaves"),
    (["id", "ID"], pd.DataFrame(), "chaves"),
    (["id"], pd.DataFrame({"x": [1]}), "ausentes"),
    (["id"], pd.DataFrame({"id": [None], "extraction_date": [1]}), "nulas"),
    (["id"], pd.DataFrame({"id": [""], "extraction_date": [1]}), "vazias"),
    (["id"], pd.DataFrame({"id": [1, 1], "extraction_date": [1, 1], "x": ["a", "b"]}), "conflitantes"),
])
def test_invalid_dedup(keys, frame, message):
    with pytest.raises(ValueError, match=message):
        job.remove_duplicates(frame, keys)


@pytest.mark.parametrize("field,value", [
    ("source_bucket", ""), ("merge_keys", "id"), ("merge_keys", ["id", "ID"]),
    ("source_prefix", "compras"),
    ("merge_keys", ["year"]), ("database", "DROP TABLE"), ("table_location", "s3://silver/"),
    ("temp_path", "s3://silver/iceberg/"), ("s3_output", "file:///tmp/results"),
    ("table_location", "s3://bronze/compras/silver/"),
])
def test_invalid_config(config, field, value):
    config[field] = value
    with pytest.raises(ValueError):
        job.validate_config(config)


def test_load_config(processing_env):
    result = job.load_config(processing_env)
    assert result["source_bucket"] == "bronze"
    assert result["merge_keys"] == ["r_e_c_n_o"]
    with pytest.raises(ValueError, match="SOURCE_BUCKET"):
        job.load_config({})


@pytest.mark.parametrize("extension,reader,extra", [
    ("csv", "read_csv", {"dtype": "string", "keep_default_na": False, "sep": ",", "encoding": "utf-8"}),
    ("parquet", "read_parquet", {}),
    ("json", "read_json", {"lines": False, "dtype": False, "convert_dates": False}),
    ("jsonl", "read_json", {"lines": True, "dtype": False, "convert_dates": False}),
    ("ndjson", "read_json", {"lines": True, "dtype": False, "convert_dates": False}),
])
def test_read_exact_object(config, monkeypatch, extension, reader, extra):
    mock = Mock(return_value=pd.DataFrame({"id": [1]}))
    monkeypatch.setattr(job.wr.s3, reader, mock)
    session = Mock()
    # '+' e '%' são parte literal da chave EventBridge: não usar unquote_plus.
    key = "compras/a+b%20." + extension
    assert len(job.read_file("bronze", key, config, session, "version-1")) == 1
    mock.assert_called_once_with(path=["s3://bronze/" + key], boto3_session=session,
                                 version_id="version-1", **extra)


@pytest.mark.parametrize("bucket,key", [("other", "compras/a.csv"), ("bronze", "other/a.csv"),
                                         ("bronze", "compras/a.xlsx")])
def test_reject_source_or_format(config, bucket, key):
    with pytest.raises(ValueError):
        job.read_file(bucket, key, config, Mock())


def test_merge_contract_and_empty(config, monkeypatch):
    writer = Mock()
    monkeypatch.setattr(job.wr.athena, "to_iceberg", writer)
    session = Mock()
    frame = pd.DataFrame({"id": [1]})
    assert job.merge_iceberg(frame, config, session) == 1
    kwargs = writer.call_args.kwargs
    assert kwargs["merge_cols"] == ["id"]
    assert kwargs["merge_condition"] == "update"
    assert kwargs["partition_cols"] == ["year", "month", "day"]
    assert kwargs["table_location"] == config["table_location"]
    assert kwargs["keep_files"] is False
    assert kwargs["fill_missing_columns_in_df"] is False
    assert kwargs["schema_evolution"] is False
    path = kwargs["temp_path"]
    job.merge_iceberg(frame, config, session)
    assert writer.call_args.kwargs["temp_path"] != path
    writer.reset_mock()
    assert job.merge_iceberg(pd.DataFrame(), config, session) == 0
    writer.assert_not_called()


def test_pipeline_replay_simulated_merge(config, monkeypatch):
    """Simula o contrato de merge por chave, sem afirmar validar o engine Athena."""
    source = pd.DataFrame({"ID": ["001", "001"], "ExtractionDate": ["2026-09-28"] * 2})
    monkeypatch.setattr(job.wr.s3, "read_csv", Mock(return_value=source))
    target = pd.DataFrame()

    def simulated_merge(**kwargs):
        nonlocal target
        target = pd.concat([target, kwargs["df"]]).drop_duplicates(kwargs["merge_cols"], keep="last")

    monkeypatch.setattr(job.wr.athena, "to_iceberg", simulated_merge)
    for _ in range(2):
        assert job.process_file("bronze", "compras/a.csv", config, Mock()) == 1
    assert len(target) == 1
    assert target["id"].iloc[0] == "001"


def test_failure_propagates_and_does_not_write_invalid_data(config, monkeypatch):
    monkeypatch.setattr(job.wr.s3, "read_csv", Mock(return_value=pd.DataFrame({"id": [1]})))
    writer = Mock()
    monkeypatch.setattr(job.wr.athena, "to_iceberg", writer)
    with pytest.raises(ValueError):
        job.process_file("bronze", "compras/a.csv", config, Mock())
    writer.assert_not_called()
    writer.side_effect = RuntimeError("Athena failed")
    with pytest.raises(RuntimeError, match="Athena failed"):
        job.merge_iceberg(pd.DataFrame({"id": [1]}), config, Mock())


def test_empty_file(config, monkeypatch):
    reader = Mock(return_value=pd.DataFrame(columns=["id", "extraction_date"]))
    writer = Mock()
    monkeypatch.setattr(job.wr.s3, "read_csv", reader)
    monkeypatch.setattr(job.wr.athena, "to_iceberg", writer)
    assert job.process_file("bronze", "compras/a.csv", config, Mock()) == 0
    writer.assert_not_called()


def test_main(config, monkeypatch):
    monkeypatch.setattr(job, "load_dotenv", Mock())
    session = Mock()
    monkeypatch.setattr(job.boto3, "Session", Mock(return_value=session))
    loader = Mock(return_value=config)
    processor = Mock(return_value=3)
    monkeypatch.setattr(job, "load_config", loader)
    monkeypatch.setattr(job, "process_file", processor)
    assert job.main(["--source-bucket", "bronze",
                     "--source-key", "compras/a.csv", "--source-version-id", "v1", "--JOB_NAME", "test"]) == 3
    processor.assert_called_once_with("bronze", "compras/a.csv", config, session, "v1")


def test_main_required_args(monkeypatch):
    monkeypatch.setattr(job, "load_dotenv", Mock())
    monkeypatch.delenv("SOURCE_KEY", raising=False)
    with pytest.raises(SystemExit):
        job.main([])
