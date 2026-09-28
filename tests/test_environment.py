import os
from unittest.mock import Mock, patch

import pytest
from dotenv import dotenv_values

from jobs import bronze_to_silver as job


@pytest.mark.parametrize("name", ["SOURCE_BUCKET", "SOURCE_PREFIX", "DATABASE", "TABLE",
                                  "TABLE_LOCATION", "TEMP_PATH", "S3_OUTPUT", "WORKGROUP"])
def test_required_environment_variables(processing_env, name):
    processing_env[name] = " "
    with pytest.raises(ValueError, match=name):
        job.load_config(processing_env)


def test_configuration_overrides_and_isolation(processing_env):
    processing_env.update(MERGE_KEYS="C7_FILIAL, R_E_C_N_O_", DELETION_COLUMN="D_E_L_E_T_",
                          DATE_COLUMN="Data Extração", DATE_FORMAT="%Y%m%d",
                          CSV_SEPARATOR="|", ENCODING="latin-1", DECIMAL_SEPARATOR=",",
                          C7_EMISSAO_FORMAT="%d/%m/%Y", C7_DATPRF_FORMAT="%Y-%m-%d")
    config = job.load_config(processing_env)
    assert config["merge_keys"] == ["c7_filial", "r_e_c_n_o"]
    assert config["deletion_column"] == "d_e_l_e_t"
    assert config["dtype"]["d_e_l_e_t"] == "string"
    assert "d_e_l_e_t_d" not in config["dtype"]
    assert config["date_column"] == "data_extracao"
    assert config["date_format"] == "%Y%m%d"
    assert config["csv_separator"] == "|"
    assert config["encoding"] == "latin-1"
    assert config["decimal_separator"] == ","
    assert config["column_date_formats"] == {"c7_emissao": "%d/%m/%Y", "c7_datprf": "%Y-%m-%d"}
    config["dtype"]["c7_num"] = "bigint"
    assert job.load_config(processing_env)["dtype"]["c7_num"] == "string"


def test_example_environment_is_valid():
    # Lê somente o exemplo público, nunca o .env pessoal.
    config = job.load_config(dotenv_values(".env.example"))
    assert config["table"] == "sc7_pedidos_compra"


def test_local_dotenv_precedence_and_credentials_not_in_config(tmp_path, processing_env, monkeypatch):
    env_file = tmp_path / "synthetic.env"
    values = dict(processing_env, SOURCE_KEY="compras/sc7/local.csv", SOURCE_VERSION_ID="v2",
                  AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="fake-key",
                  AWS_SECRET_ACCESS_KEY="fake-secret", WORKGROUP="from_file")
    env_file.write_text("\n".join(key + "=" + value for key, value in values.items()), encoding="utf-8")
    factory = Mock()
    processor = Mock(return_value=1)
    monkeypatch.setattr(job.boto3, "Session", factory)
    monkeypatch.setattr(job, "process_file", processor)
    with patch.dict(os.environ, {"WORKGROUP": "from_environment"}, clear=True):
        assert job.main(["--env-file", str(env_file)]) == 1
        assert os.environ["AWS_ACCESS_KEY_ID"] == "fake-key"
        config = processor.call_args.args[2]
        assert config["workgroup"] == "from_environment"
        assert not any("AWS" in key or "secret" in key for key in config)
        factory.assert_called_once_with(region_name="us-east-1")
        assert processor.call_args.args[1] == "compras/sc7/local.csv"
        assert processor.call_args.args[4] == "v2"


def test_without_dotenv_and_cli_precedence(tmp_path, processing_env, monkeypatch):
    factory = Mock()
    processor = Mock(return_value=2)
    monkeypatch.setattr(job.boto3, "Session", factory)
    monkeypatch.setattr(job, "process_file", processor)
    with patch.dict(os.environ, dict(processing_env, SOURCE_KEY="old.csv", SOURCE_VERSION_ID="old",
                                    AWS_REGION="sa-east-1", AWS_DEFAULT_REGION="us-east-1"), clear=True):
        assert job.main(["--env-file", str(tmp_path / "absent.env"), "--source-key",
                         "compras/sc7/new.csv", "--source-version-id", "new"]) == 2
        factory.assert_called_once_with(region_name="sa-east-1")
        assert processor.call_args.args[1] == "compras/sc7/new.csv"
        assert processor.call_args.args[4] == "new"
