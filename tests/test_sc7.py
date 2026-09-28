from datetime import date, datetime
from decimal import Decimal
from unittest.mock import Mock

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from jobs import bronze_to_silver as job


@pytest.fixture
def sc7_config(processing_env):
    return job.load_config(processing_env)


@pytest.fixture
def record():
    return {
        "C7_FILIAL": " 01 ", "C7_TIPO": "1", "C7_NUM": "000123", "C7_ITEM": "0001",
        "C7_PRODUTO": "000042", "C7_QUANT": "10.500000", "C7_QUJE": "2",
        "C7_PRECO": "12.123456", "C7_TOTAL": "127.296288", "C7_NUMSC": "000987",
        "C7_EMISSAO": "20260928", "C7_DATPRF": "20261005", "C7_LOCAL": "01",
        "C7_FORNECE": "000001", "C7_LOJA": "01", "D_E_L_E_T_D": " ",
        "R_E_C_N_O": "123456789", "extraction_date": "2026-09-28T18:20:30Z",
    }


def test_sc7_schema_and_no_mutation(sc7_config, record):
    source = pd.DataFrame([record])
    original = source.copy(deep=True)
    result = job.transform_file(source, sc7_config)
    row = result.iloc[0]
    assert row["c7_filial"] == "01"
    assert row["c7_num"] == "000123"
    assert row["c7_produto"] == "000042"
    assert row["c7_quje"] == Decimal("2.000000")
    assert row["c7_preco"] == Decimal("12.123456")
    assert row["c7_total"] == Decimal("127.296288")
    assert row["c7_emissao"] == date(2026, 9, 28)
    assert row["c7_datprf"] == date(2026, 10, 5)
    assert row["r_e_c_n_o"] == 123456789
    assert row["d_e_l_e_t_d"] == ""
    assert row[["year", "month", "day"]].tolist() == [2026, 9, 28]
    assert_frame_equal(source, original)
    assert sc7_config["merge_keys"] == ["r_e_c_n_o"]


@pytest.mark.parametrize("value,expected", [(None, None), ("", None),
    ("12.123456", Decimal("12.123456")), (0.1, Decimal("0.100000")),
    ("-2", Decimal("-2.000000")), ("1e-6", Decimal("0.000001"))])
def test_decimal(value, expected):
    assert job.parse_decimal(value, 18, 6) == expected


@pytest.mark.parametrize("value", ["1.234,56", "NaN", "Infinity", True, "12.1234567",
                                       "1000000000000", "1e999"])
def test_invalid_decimal(value):
    with pytest.raises(ValueError):
        job.parse_decimal(value, 18, 6)


def test_explicit_decimal_separator(sc7_config, record):
    sc7_config["decimal_separator"] = ","
    for col in ["C7_QUANT", "C7_QUJE", "C7_PRECO", "C7_TOTAL"]:
        record[col] = record[col].replace(".", ",")
    result = job.transform_file(pd.DataFrame([record]), sc7_config)
    assert result.loc[0, "c7_preco"] == Decimal("12.123456")
    assert job.parse_decimal(1.25, 18, 6, ",") == Decimal("1.25")
    with pytest.raises(ValueError):
        job.parse_decimal("1.25", 18, 6, ",")


@pytest.mark.parametrize("value,expected", [("", None), (None, None),
    (20260928, date(2026, 9, 28)), (date(2026, 9, 28), date(2026, 9, 28)),
    (datetime(2026, 9, 28, 10), date(2026, 9, 28))])
def test_business_dates(value, expected):
    assert job.parse_business_date(value, "%Y%m%d") == expected


@pytest.mark.parametrize("value", ["20260230", "20261301", "2026111", "00000000", "2026-09-28"])
def test_invalid_business_date(value):
    with pytest.raises(ValueError):
        job.parse_business_date(value, "%Y%m%d")


def test_null_optional_values_and_alternative_date_format(sc7_config, record):
    record.update(C7_QUANT="", C7_DATPRF=" ", C7_EMISSAO="28/09/2026")
    sc7_config["column_date_formats"]["c7_emissao"] = "%d/%m/%Y"
    result = job.transform_file(pd.DataFrame([record]), sc7_config)
    assert pd.isna(result.loc[0, "c7_quant"])
    assert pd.isna(result.loc[0, "c7_datprf"])
    assert result.loc[0, "c7_emissao"] == date(2026, 9, 28)


@pytest.mark.parametrize("column,value", [("C7_NUM", 123), ("R_E_C_N_O", "1.5"),
    ("R_E_C_N_O", "9223372036854775808"), ("R_E_C_N_O", ""), ("C7_PRECO", "erro")])
def test_reject_corrupt_sc7_values(sc7_config, record, column, value):
    record[column] = value
    with pytest.raises(ValueError):
        job.transform_file(pd.DataFrame([record]), sc7_config)


def test_missing_schema_column(sc7_config, record):
    del record["C7_NUM"]
    with pytest.raises(ValueError, match="c7_num"):
        job.transform_file(pd.DataFrame([record]), sc7_config)


@pytest.mark.parametrize("dtype", ["float", "decimal(0,0)", "decimal(39,6)", "decimal(5,6)"])
def test_invalid_type_contract(dtype):
    with pytest.raises(ValueError):
        job.apply_column_types(pd.DataFrame({"value": [1]}), {"dtype": {"value": dtype}})


def test_invalid_separator():
    with pytest.raises(ValueError):
        job.apply_column_types(pd.DataFrame({"value": [1]}),
                               {"dtype": {"value": "decimal(18,6)"}, "decimal_separator": ";"})


def test_recno_keeps_distinct_records_with_same_business_key(sc7_config, record):
    second = dict(record, R_E_C_N_O="987654321")
    assert len(job.transform_file(pd.DataFrame([record, second]), sc7_config)) == 2


def test_latest_deletion_wins_before_splitting(sc7_config, record):
    deletion = dict(record, D_E_L_E_T_D=" * ", extraction_date="2026-09-29T00:00:00Z")
    result = job.transform_file(pd.DataFrame([deletion, record]), sc7_config)
    active, deleted = job.split_deleted(result, sc7_config)
    assert active.empty
    assert len(deleted) == 1
    assert deleted.iloc[0]["r_e_c_n_o"] == 123456789


@pytest.mark.parametrize("value", [None, "S", "1"])
def test_invalid_deletion_flag(value):
    with pytest.raises(ValueError):
        job.split_deleted(pd.DataFrame({"flag": [value]}), {"deletion_column": "flag"})


def test_missing_deletion_flag():
    with pytest.raises(ValueError, match="ausente"):
        job.split_deleted(pd.DataFrame(), {"deletion_column": "flag"})


def test_delete_contract_and_nonexistent_table(sc7_config, monkeypatch):
    exists = Mock(return_value=False)
    delete = Mock()
    monkeypatch.setattr(job.wr.catalog, "does_table_exist", exists)
    monkeypatch.setattr(job.wr.athena, "delete_from_iceberg_table", delete)
    frame = pd.DataFrame({"r_e_c_n_o": pd.array([1], dtype="Int64"), "extra": ["ignored"]})
    session = Mock()
    assert job.delete_iceberg(frame, sc7_config, session) == 0
    delete.assert_not_called()
    exists.return_value = True
    assert job.delete_iceberg(frame, sc7_config, session) == 1
    kwargs = delete.call_args.kwargs
    assert kwargs["df"].columns.tolist() == ["r_e_c_n_o"]
    assert kwargs["merge_cols"] == ["r_e_c_n_o"]
    assert kwargs["keep_files"] is False
    assert kwargs["boto3_session"] is session
    delete.side_effect = RuntimeError("delete failed")
    with pytest.raises(RuntimeError, match="delete failed"):
        job.delete_iceberg(frame, sc7_config, session)


def test_pipeline_deletes_existing_rows_and_replays(sc7_config, record, monkeypatch):
    target = {123456789: "old"}
    deleted = dict(record, D_E_L_E_T_D="*", extraction_date="2026-09-29")
    active = dict(record, R_E_C_N_O="987654321")
    monkeypatch.setattr(job.wr.s3, "read_csv", Mock(return_value=pd.DataFrame([record, deleted, active])))
    monkeypatch.setattr(job.wr.catalog, "does_table_exist", Mock(return_value=True))

    def merge(**kwargs):
        for recno in kwargs["df"]["r_e_c_n_o"]:
            target[recno] = "new"

    def delete(**kwargs):
        for recno in kwargs["df"]["r_e_c_n_o"]:
            target.pop(recno, None)

    monkeypatch.setattr(job.wr.athena, "to_iceberg", merge)
    monkeypatch.setattr(job.wr.athena, "delete_from_iceberg_table", delete)
    for _ in range(2):
        assert job.process_file(sc7_config["source_bucket"], "compras/sc7/file.csv", sc7_config, Mock()) == 1
        assert target == {987654321: "new"}


def test_only_deletions_do_not_insert(sc7_config, record, monkeypatch):
    record["D_E_L_E_T_D"] = "*"
    monkeypatch.setattr(job.wr.s3, "read_csv", Mock(return_value=pd.DataFrame([record])))
    monkeypatch.setattr(job.wr.catalog, "does_table_exist", Mock(return_value=True))
    writer = Mock()
    deleter = Mock()
    monkeypatch.setattr(job.wr.athena, "to_iceberg", writer)
    monkeypatch.setattr(job.wr.athena, "delete_from_iceberg_table", deleter)
    assert job.process_file(sc7_config["source_bucket"], "compras/sc7/file.csv", sc7_config, Mock()) == 0
    writer.assert_not_called()
    deleter.assert_called_once()
