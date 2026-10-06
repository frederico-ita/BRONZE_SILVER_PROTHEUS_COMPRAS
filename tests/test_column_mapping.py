from unittest.mock import Mock

import pandas as pd
import pytest

from jobs import bronze_to_silver as job
from tests.test_sc7 import record, sc7_config


def test_unselected_columns_never_reach_iceberg(sc7_config, record, monkeypatch):
    source = pd.DataFrame([dict(record, S_T_A_M_P=pd.Timestamp("2026-10-05"), _airbyte_meta=None)])
    monkeypatch.setattr(job, "read_file", Mock(return_value=source))
    writer = Mock()
    monkeypatch.setattr(job.wr.athena, "to_iceberg", writer)
    job.process_file("bronze", "compras/sc7/file.parquet", sc7_config, Mock())
    sent = writer.call_args.kwargs
    assert list(sent["df"]) == list(sc7_config["columns"].values())
    assert "s_t_a_m_p" not in sent["df"]
    assert "airbyte_meta" not in sent["df"]
    assert sent["schema_evolution"] is True
    assert "S_T_A_M_P" in source  # A origem permanece intacta.


def test_selected_new_column_is_sent_for_evolution(sc7_config, record, monkeypatch):
    sc7_config["columns"]["s_t_a_m_p"] = "atualizado_em"
    source = pd.DataFrame([dict(record, S_T_A_M_P=pd.Timestamp("2026-10-05"))])
    monkeypatch.setattr(job, "read_file", Mock(return_value=source))
    writer = Mock()
    monkeypatch.setattr(job.wr.athena, "to_iceberg", writer)
    job.process_file("bronze", "compras/sc7/file.parquet", sc7_config, Mock())
    sent = writer.call_args.kwargs
    assert sent["df"]["atualizado_em"].iloc[0] == pd.Timestamp("2026-10-05")
    assert "s_t_a_m_p" not in sent["df"]
    assert sent["schema_evolution"] is True


def test_renaming_preserves_types_merge_delete_and_partitions(processing_env, record, monkeypatch):
    aliases = {"c7_num": "numero_pedido", "c7_total": "valor_total", "r_e_c_n_o": "recno",
               "d_e_l_e_t": "excluido", "airbyte_extracted_at": "extraido_em", "year": "ano"}
    monkeypatch.setattr(job, "COLUMN_MAP", dict(job.COLUMN_MAP, **aliases))
    config = job.load_config(processing_env)
    monkeypatch.setattr(job.wr.catalog, "get_table_types", Mock(return_value=None))
    source = pd.DataFrame([record, dict(record, R_E_C_N_O_="2", D_E_L_E_T_="*")])
    monkeypatch.setattr(job, "read_file", Mock(return_value=source))
    writer, deleter = Mock(), Mock()
    monkeypatch.setattr(job.wr.athena, "to_iceberg", writer)
    monkeypatch.setattr(job.wr.athena, "delete_from_iceberg_table", deleter)
    monkeypatch.setattr(job.wr.catalog, "does_table_exist", Mock(return_value=True))
    assert job.process_file("bronze", "compras/sc7/file.parquet", config, Mock()) == 1
    sent = writer.call_args.kwargs
    assert sent["merge_cols"] == ["recno"]
    assert sent["partition_cols"] == ["ano", "month", "day"]
    assert sent["dtype"]["numero_pedido"] == "string"
    assert sent["dtype"]["valor_total"] == "decimal(18,6)"
    assert sent["dtype"]["recno"] == "bigint"
    assert sent["df"]["numero_pedido"].tolist() == ["000123"]
    assert not sent["df"].dtypes.eq(object).any()
    assert not set(aliases).intersection(sent["df"].columns)
    deleted = deleter.call_args.kwargs
    assert deleted["merge_cols"] == ["recno"]
    assert deleted["df"].to_dict("records") == [{"recno": 2}]


@pytest.mark.parametrize("column", ["c7_num", "c7_total", "c7_emissao"])
def test_unselected_business_column_is_not_required(sc7_config, record, column):
    del sc7_config["columns"][column]
    del record[column.upper()]
    assert column not in job.transform_file(pd.DataFrame([record]), sc7_config)


def test_missing_selected_column_is_not_silently_filled(sc7_config, record):
    sc7_config["columns"]["s_t_a_m_p"] = "atualizado_em"
    with pytest.raises(KeyError, match="s_t_a_m_p"):
        job.transform_file(pd.DataFrame([record]), sc7_config)


@pytest.mark.parametrize("name", ["c7_num", "year", "Numero Pedido", ""])
def test_invalid_or_duplicate_target_name(processing_env, monkeypatch, name):
    monkeypatch.setitem(job.COLUMN_MAP, "c7_filial", name)
    with pytest.raises(ValueError, match="COLUMN_MAP"):
        job.load_config(processing_env)
