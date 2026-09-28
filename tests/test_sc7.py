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


