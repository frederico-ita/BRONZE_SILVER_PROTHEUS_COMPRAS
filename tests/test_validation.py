from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from jobs import bronze_to_silver as job
from tests.test_sc7 import record, sc7_config


def test_last_file_invalid_prevents_all_writes(sc7_config, record, monkeypatch):
    reader = Mock(side_effect=[pd.DataFrame([record]), pd.DataFrame([dict(record, C7_NUM=123)])])
    monkeypatch.setattr(job, "read_file", reader)
    writer = Mock()
    monkeypatch.setattr(job, "write_frame", writer)
    with pytest.raises(ValueError, match="b.parquet"):
        job.process_batch("bronze", [("a.parquet", None), ("b.parquet", None)], sc7_config, Mock())
    writer.assert_not_called()


def test_all_files_read_once_before_write_and_local_files_removed(sc7_config, record, monkeypatch):
    events, paths = [], []
    def read(*args):
        events.append("read")
        return pd.DataFrame([record])
    original = pd.read_pickle
    def restore(path):
        paths.append(Path(path))
        return original(path)
    def write(frame, *args):
        events.append("write")
        assert frame.loc[0, "c7_num"] == "000123"
        return 1
    monkeypatch.setattr(job, "read_file", read)
    monkeypatch.setattr(job, "write_frame", write)
    monkeypatch.setattr(pd, "read_pickle", restore)
    assert job.process_batch("bronze", [("a", None), ("b", None)], sc7_config, Mock()) == 2
    assert events == ["read", "read", "write", "write"]
    assert all(not path.exists() for path in paths)


def test_catalog_extra_column_blocks_before_write(sc7_config, record, monkeypatch):
    monkeypatch.setattr(job.wr.catalog, "get_table_types", Mock(return_value={"old_column": "string"}))
    monkeypatch.setattr(job, "read_file", Mock(return_value=pd.DataFrame([record])))
    writer = Mock()
    monkeypatch.setattr(job, "write_frame", writer)
    with pytest.raises(ValueError, match="old_column"):
        job.process_file("bronze", "a", sc7_config, Mock())
    writer.assert_not_called()


@pytest.mark.parametrize("old,new,allowed", [
    ("int", "bigint", True), ("float", "double", True),
    ("decimal(10, 2)", "decimal(18,2)", True),
    ("decimal(18,2)", "decimal(10,2)", False),
    ("decimal(18,2)", "decimal(18,3)", False),
    ("string", "timestamp", False), ("bigint", "int", False),
])
def test_type_compatibility(sc7_config, monkeypatch, old, new, allowed):
    monkeypatch.setattr(job.wr.catalog, "extract_athena_types", Mock(return_value=({"x": new}, {})))
    if allowed:
        assert job.validate_schema(pd.DataFrame(), {"x": old}, sc7_config) == {"x": new}
    else:
        with pytest.raises(ValueError, match="tipos"):
            job.validate_schema(pd.DataFrame(), {"x": old}, sc7_config)


def test_new_column_type_conflict_later_in_batch(sc7_config, record, monkeypatch):
    sc7_config["columns"]["extra"] = "extra"
    monkeypatch.setattr(job, "read_file", Mock(side_effect=[
        pd.DataFrame([dict(record, extra="text")]), pd.DataFrame([dict(record, extra=42)])]))
    writer = Mock()
    monkeypatch.setattr(job, "write_frame", writer)
    with pytest.raises(ValueError, match="extra"):
        job.process_batch("bronze", [("a", None), ("b", None)], sc7_config, Mock())
    writer.assert_not_called()


def test_empty_batch_does_not_access_catalog(sc7_config):
    assert job.process_batch("bronze", [], sc7_config, Mock()) == 0
    job.wr.catalog.get_table_types.assert_not_called()


def test_partition_limit_before_write(sc7_config, record, monkeypatch):
    records = [dict(record, R_E_C_N_O_=str(i), _airbyte_extracted_at=date.isoformat())
               for i, date in enumerate(pd.date_range("2026-01-01", periods=101, tz="UTC"))]
    monkeypatch.setattr(job, "read_file", Mock(return_value=pd.DataFrame(records)))
    writer = Mock()
    monkeypatch.setattr(job, "write_frame", writer)
    with pytest.raises(ValueError, match="100 particoes"):
        job.process_file("bronze", "a", sc7_config, Mock())
    writer.assert_not_called()
