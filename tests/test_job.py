from decimal import Decimal, InvalidOperation
from unittest.mock import Mock

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from jobs import bronze_to_silver as job
from tests.test_sc7 import record, sc7_config


@pytest.mark.parametrize("value,expected", [("C7_NUM", "c7_num"), ("Data Extração", "data_extracao"),
                                           ("extractionDate", "extraction_date")])
def test_names(value, expected):
    assert job.snake_case(value) == expected


@pytest.mark.parametrize("value,expected", [("  São\tPaulo\n", "São Paulo"),
    ("A\x00B\u200bC", "ABC"), ("cafe\u0301", "café"), (None, None), (42, 42)])
def test_clean(value, expected):
    assert job.clean_value(value) == expected


@pytest.mark.parametrize("value,expected", [("", None), (None, None), ("0.1", Decimal("0.1")),
                                            ("12.123456", Decimal("12.123456")), (-2, Decimal("-2"))])
def test_decimal(value, expected):
    assert job.decimal_value(value, ".") == expected


@pytest.mark.parametrize("value", ["1.1234567", "1000000000000", "NaN", "Infinity", "1.234,56"])
def test_invalid_decimal(value):
    with pytest.raises((ValueError, InvalidOperation)):
        job.decimal_value(value, ".")


def test_decimal_separator():
    assert job.decimal_value("1,25", ",") == Decimal("1.25")
    assert job.decimal_value(1.25, ",") == Decimal("1.25")
    for value, separator in [("1.25", ","), ("1", ";")]:
        with pytest.raises(ValueError):
            job.decimal_value(value, separator)


def test_transform_latest_partition_and_input(sc7_config, record):
    newer = dict(record, C7_TOTAL="200", extraction_date="2026-09-30T23:30:00-03:00")
    source = pd.DataFrame([record, newer, newer])
    original = source.copy(deep=True)
    result = job.transform_file(source, sc7_config)
    assert len(result) == 1
    assert result.loc[0, "c7_total"] == Decimal("200")
    assert result.loc[0, ["year", "month", "day"]].tolist() == [2026, 10, 1]
    assert_frame_equal(source, original)


@pytest.mark.parametrize("column,value", [("R_E_C_N_O", None), ("R_E_C_N_O", "1.5"),
    ("R_E_C_N_O", "9223372036854775808"), ("C7_NUM", 123), ("C7_EMISSAO", "20260230"),
    ("C7_EMISSAO", "2026111"), ("extraction_date", None), ("extraction_date", "invalid"),
    ("D_E_L_E_T_D", None), ("D_E_L_E_T_D", "S")])
def test_invalid_rows(sc7_config, record, column, value):
    record[column] = value
    with pytest.raises((ValueError, TypeError, OverflowError)):
        job.transform_file(pd.DataFrame([record]), sc7_config)


def test_schema_conflicts_and_missing(sc7_config, record):
    for extra in [{"c7_num": "duplicate"}, {"!!!": "invalid"}]:
        with pytest.raises(ValueError):
            job.transform_file(pd.DataFrame([dict(record, **extra)]), sc7_config)
    del record["C7_NUM"]
    with pytest.raises(KeyError):
        job.transform_file(pd.DataFrame([record]), sc7_config)


def test_optional_blanks(sc7_config, record):
    record.update(C7_QUANT="", C7_DATPRF="")
    result = job.transform_file(pd.DataFrame([record]), sc7_config)
    assert pd.isna(result.loc[0, "c7_quant"])
    assert pd.isna(result.loc[0, "c7_datprf"])


def test_conflicting_ties(sc7_config, record):
    with pytest.raises(ValueError, match="conflitantes"):
        job.transform_file(pd.DataFrame([record, dict(record, C7_TOTAL="200")]), sc7_config)


@pytest.mark.parametrize("extension,reader", [("csv", "read_csv"), ("parquet", "read_parquet"),
    ("json", "read_json"), ("jsonl", "read_json"), ("ndjson", "read_json")])
def test_read_exact_version(sc7_config, monkeypatch, extension, reader):
    mock = Mock(return_value=pd.DataFrame())
    monkeypatch.setattr(job.wr.s3, reader, mock)
    key = "compras/sc7/a+b%20." + extension
    job.read_file("bronze", key, sc7_config, Mock(), "v1")
    options = mock.call_args.kwargs
    assert options["path"] == ["s3://bronze/" + key]
    assert options["version_id"] == "v1"
    if extension == "csv":
        assert options["dtype"] == "string"
        assert options["keep_default_na"] is False


@pytest.mark.parametrize("bucket,key", [("other", "compras/sc7/a.csv"), ("bronze", "other/a.csv"),
                                         ("bronze", "compras/sc7/a.xlsx")])
def test_invalid_source(sc7_config, bucket, key):
    with pytest.raises(ValueError):
        job.read_file(bucket, key, sc7_config, Mock())


def test_merge_delete_and_replay(sc7_config, record, monkeypatch):
    source = pd.DataFrame([record, dict(record, D_E_L_E_T_D="*", extraction_date="2026-09-29"),
                           dict(record, R_E_C_N_O="2")])
    target = {123456789: "old"}
    monkeypatch.setattr(job.wr.s3, "read_csv", Mock(return_value=source))
    monkeypatch.setattr(job.wr.catalog, "does_table_exist", Mock(return_value=True))
    def merge(**kw):
        assert kw["partition_cols"] == ["year", "month", "day"]
        assert kw["merge_cols"] == ["r_e_c_n_o"]
        assert kw["merge_condition"] == "update"
        assert kw["fill_missing_columns_in_df"] is False
        target.update({key: "new" for key in kw["df"]["r_e_c_n_o"]})
    def delete(**kw):
        assert kw["df"].columns.tolist() == ["r_e_c_n_o"]
        for key in kw["df"]["r_e_c_n_o"]:
            target.pop(key, None)
    monkeypatch.setattr(job.wr.athena, "to_iceberg", merge)
    monkeypatch.setattr(job.wr.athena, "delete_from_iceberg_table", delete)
    for _ in range(2):
        assert job.process_file("bronze", "compras/sc7/a.csv", sc7_config, Mock()) == 1
        assert target == {2: "new"}


@pytest.mark.parametrize("exists", [True, False])
def test_only_deletes(sc7_config, record, monkeypatch, exists):
    record["D_E_L_E_T_D"] = "*"
    monkeypatch.setattr(job.wr.s3, "read_csv", Mock(return_value=pd.DataFrame([record])))
    monkeypatch.setattr(job.wr.catalog, "does_table_exist", Mock(return_value=exists))
    writer, deleter = Mock(), Mock()
    monkeypatch.setattr(job.wr.athena, "to_iceberg", writer)
    monkeypatch.setattr(job.wr.athena, "delete_from_iceberg_table", deleter)
    assert job.process_file("bronze", "compras/sc7/a.csv", sc7_config, Mock()) == 0
    writer.assert_not_called()
    assert deleter.call_count == int(exists)


def test_empty_and_failure(sc7_config, record, monkeypatch):
    reader = Mock(return_value=pd.DataFrame(columns=record))
    writer = Mock(side_effect=RuntimeError("Athena failed"))
    monkeypatch.setattr(job.wr.s3, "read_csv", reader)
    monkeypatch.setattr(job.wr.athena, "to_iceberg", writer)
    assert job.process_file("bronze", "compras/sc7/a.csv", sc7_config, Mock()) == 0
    writer.assert_not_called()
    reader.return_value = pd.DataFrame([record])
    with pytest.raises(RuntimeError):
        job.process_file("bronze", "compras/sc7/a.csv", sc7_config, Mock())


def test_main_missing_key(monkeypatch):
    monkeypatch.setattr(job, "load_dotenv", Mock())
    monkeypatch.delenv("SOURCE_KEY", raising=False)
    with pytest.raises(SystemExit):
        job.main([])
