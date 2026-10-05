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
        "C7_FORNECE": "000001", "C7_LOJA": "01", "D_E_L_E_T_": " ",
        "R_E_C_N_O_": "123456789", "_airbyte_extracted_at": "2026-09-28T18:20:30Z",
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
    assert row["c7_emissao"] == "20260928"
    assert row["c7_datprf"] == "20261005"
    assert row["r_e_c_n_o"] == 123456789
    assert row["d_e_l_e_t"] == ""
    assert row[["year", "month", "day"]].tolist() == [2026, 9, 28]
    assert_frame_equal(source, original)
    assert sc7_config["merge_keys"] == ["r_e_c_n_o"]
    assert sc7_config["date_column"] == "airbyte_extracted_at"
    assert "airbyte_extracted_at" in result.columns
    assert "extraction_date" not in result.columns


@pytest.mark.parametrize("value", [1, 1.0, "1", None])
def test_c7_tipo_discarded_before_validation(sc7_config, record, value):
    record["C7_TIPO"] = value
    result = job.transform_file(pd.DataFrame([record]), sc7_config)
    assert "c7_tipo" not in result.columns
    assert "c7_tipo" not in sc7_config["dtype"]


def test_c7_tipo_not_required(sc7_config, record):
    record.pop("C7_TIPO")
    assert len(job.transform_file(pd.DataFrame([record]), sc7_config)) == 1


@pytest.mark.parametrize("column,value", [
    ("_airbyte_extracted_at", "data-invalida"),
])
def test_date_conversion_error_identifies_column(sc7_config, record, column, value):
    record[column] = value
    source = pd.DataFrame([record])
    original = source.copy(deep=True)
    with pytest.raises(ValueError, match=f"Erro na coluna {job.snake_case(column)}:") as caught:
        job.transform_file(source, sc7_config)
    cause = caught.value.__cause__
    assert isinstance(cause, ValueError)
    assert str(cause) in str(caught.value)
    assert_frame_equal(source, original)


@pytest.mark.parametrize("column", ["C7_EMISSAO", "C7_DATPRF"])
@pytest.mark.parametrize("value", ["29241003", "20260230", "2026103", "invalida", "", " 20261003 ", 29241003, None])
def test_business_dates_preserved_as_text(sc7_config, record, column, value):
    record[column] = value
    source = pd.DataFrame([record])
    original = source.copy(deep=True)
    result = job.transform_file(source, sc7_config)
    actual = result.loc[0, job.snake_case(column)]
    assert pd.isna(actual) if value is None else actual == str(value)
    assert str(result[job.snake_case(column)].dtype) == "string"
    assert sc7_config["dtype"][job.snake_case(column)] == "string"
    assert_frame_equal(source, original)


@pytest.mark.parametrize("column,value", [
    ("_airbyte_extracted_at", "2924-10-03T00:00:00Z"),
])
def test_dates_outside_nanosecond_range_preserved(sc7_config, record, column, value):
    import pyarrow as pa

    record[column] = value
    source = pd.DataFrame([record])
    original = source.copy(deep=True)
    result = job.transform_file(source, sc7_config)
    parsed = result.loc[0, job.snake_case(column)]
    assert (parsed.year, parsed.month, parsed.day) == (2924, 10, 3)
    if column == "_airbyte_extracted_at":
        assert result.loc[0, ["year", "month", "day"]].tolist() == [2924, 10, 3]
    assert pa.Table.from_pandas(result).num_rows == 1
    assert_frame_equal(source, original)


def test_date_fallback_preserves_null_and_timezone():
    dates = job.convert_dates(pd.Series(["2924-10-03T23:30:00-03:00", None, ""]), "ISO8601", utc=True)
    assert dates.iloc[0].day == 4
    assert dates.iloc[0].hour == 2
    assert dates.iloc[1:].isna().all()


def test__airbyte_extracted_at_fallback_custom_format(sc7_config, record):
    sc7_config["date_format"] = "%Y%m%d"
    record["_airbyte_extracted_at"] = "29241003"
    result = job.transform_file(pd.DataFrame([record]), sc7_config)
    assert result.loc[0, ["year", "month", "day"]].tolist() == [2924, 10, 3]


@pytest.mark.parametrize("empty", [False, True])
def test_no_object_columns_and_all_null_athena_schema(sc7_config, record, empty):
    sc7_config["columns"]["airbyte_meta"] = "airbyte_meta"
    record["_airbyte_meta"] = None
    record["C7_PRECO"] = None
    source = pd.DataFrame([record])
    if empty:
        source = source.iloc[:0]
    original = source.copy(deep=True)
    result = job.transform_file(source, sc7_config)
    assert not result.dtypes.eq(object).any()
    assert str(result["airbyte_meta"].dtype) == "string"
    assert result["airbyte_meta"].isna().all()
    assert str(result["c7_preco"].dtype) == "decimal128(18, 6)[pyarrow]"
    types, _ = job.wr.catalog.extract_athena_types(df=result, index=False)
    assert types["airbyte_meta"] == "string"
    assert types["c7_preco"] == "decimal(18,6)"
    assert types["r_e_c_n_o"] == "bigint"
    assert_frame_equal(source, original)


def test_extra_object_columns_use_concrete_types(sc7_config, record):
    for col in ["extra_text", "extra_integer", "extra_boolean", "extra_mixed"]:
        sc7_config["columns"][col] = col
    source = pd.DataFrame([dict(record, extra_text=" abc ", extra_integer=7,
                               extra_boolean=True, extra_mixed="abc"),
                           dict(record, R_E_C_N_O_="2", extra_text=None, extra_integer=None,
                                extra_boolean=None, extra_mixed=12)], dtype=object)
    result = job.transform_file(source, sc7_config)
    assert not result.dtypes.eq(object).any()
    assert result["extra_text"].iloc[0] == "abc"
    assert pd.api.types.is_numeric_dtype(result["extra_integer"])
    assert result["extra_integer"].iloc[0] == 7
    assert str(result["extra_boolean"].dtype) == "boolean"
    assert result["extra_mixed"].tolist() == ["abc", "12"]
    assert result["c7_preco"].iloc[0] == Decimal("12.123456")


