import socket

import pytest


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Qualquer tentativa de rede nos testes deve falhar, inclusive metadados AWS."""
    def blocked(*args, **kwargs):
        raise AssertionError("Rede proibida nos testes")

    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", blocked)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


@pytest.fixture
def processing_env():
    return {
        "SOURCE_BUCKET": "bronze", "SOURCE_PREFIX": "compras/sc7/",
        "DATABASE": "silver", "TABLE": "sc7_pedidos_compra",
        "TABLE_LOCATION": "s3://silver/iceberg/sc7/",
        "TEMP_PATH": "s3://silver/staging/", "S3_OUTPUT": "s3://silver/results/",
        "WORKGROUP": "silver",
    }


@pytest.fixture
def config():
    return {
        "source_bucket": "bronze", "source_prefix": "compras/",
        "database": "silver", "table": "compras", "merge_keys": ["id"],
        "table_location": "s3://silver/iceberg/compras/",
        "temp_path": "s3://silver/staging/", "s3_output": "s3://silver/results/",
        "workgroup": "silver", "date_column": "airbyte_extracted_at",
    }
