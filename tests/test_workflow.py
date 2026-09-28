import os
from pathlib import Path
from unittest.mock import Mock, patch

import boto3
from jobs import bronze_to_silver as job_script
import pytest
import yaml
from botocore.session import Session
from botocore.validate import validate_parameters


@pytest.fixture
def workflow_deploy(tmp_path, monkeypatch, processing_env):
    workflow = yaml.safe_load(Path(".github/workflows/deploy.yml").read_text(encoding="utf-8"))
    step = next(s for s in workflow["jobs"]["deploy"]["steps"]
                if s.get("name") == "Publish script and create or update Glue job")
    code = step["run"].split("\n", 1)[1].rsplit("\nPY", 1)[0]
    model = Session().get_service_model("glue")
    glue, s3 = Mock(), Mock()
    missing = type("EntityNotFoundException", (Exception,), {})
    glue.exceptions.EntityNotFoundException = missing
    glue.meta.service_model = model
    glue.get_job.side_effect = missing()
    session = Mock()
    session.client.side_effect = lambda name: {"glue": glue, "s3": s3}[name]
    monkeypatch.setattr(boto3, "Session", Mock(return_value=session))
    env = dict(processing_env, AWS_REGION="us-east-1", GLUE_JOB_NAME="sc7",
               ARTIFACTS_BUCKET="artifacts", GITHUB_SHA="commit", GITHUB_RUN_ID="42",
               AWS_ROLE_ARN="arn:aws:iam::123456789012:role/JobsGlue",
               AWS_SECRET_ACCESS_KEY="fake-secret-not-forwarded")
    with patch.dict(os.environ, env, clear=True):
        yield compile(code, "deploy.yml", "exec"), glue, s3, model


@pytest.mark.parametrize("destination,prefix", [
    ("artifacts", ""), ("artifacts/scripts", "scripts/"),
    ("artifacts/scripts/", "scripts/"), ("artifacts/glue/sc7", "glue/sc7/"),
    ("s3://artifacts", ""), ("s3://artifacts/scripts", "scripts/"),
    (" s3://artifacts/glue/sc7/ ", "glue/sc7/"),
])
def test_create_when_missing(workflow_deploy, destination, prefix):
    code, glue, s3, model = workflow_deploy
    os.environ["ARTIFACTS_BUCKET"] = destination
    exec(code, {})
    request = glue.create_job.call_args.kwargs
    validate_parameters(request, model.operation_model("CreateJob").input_shape)
    assert request["Command"]["Name"] == "pythonshell"
    assert request["Command"]["PythonVersion"] == "3.9"
    assert request["Role"].endswith(":role/JobsGlue")
    assert request["DefaultArguments"]["--env-MERGE_KEYS"] == "r_e_c_n_o"
    assert "fake-secret-not-forwarded" not in str(request)
    expected_key = prefix + "releases/sc7/commit/42/bronze_to_silver.py"
    s3.upload_file.assert_called_once_with("jobs/bronze_to_silver.py", "artifacts", expected_key)
    assert request["Command"]["ScriptLocation"] == "s3://artifacts/" + expected_key
    glue.update_job.assert_not_called()


def test_update_preserves_role_connections_and_timeout(workflow_deploy):
    code, glue, s3, model = workflow_deploy
    glue.get_job.side_effect = None
    glue.get_job.return_value = {"Job": {
        "Name": "sc7", "Role": "existing-role", "Timeout": 45,
        "Command": {"Name": "pythonshell", "PythonVersion": "3.9", "ScriptLocation": "s3://old/script.py"},
        "Connections": {"Connections": ["private-network"]}, "MaxCapacity": 1, "AllocatedCapacity": 1,
        "DefaultArguments": {"--custom": "keep", "--source-key": "old.csv", "--config-s3": "old.json"},
    }}
    exec(code, {})
    request = glue.update_job.call_args.kwargs
    validate_parameters(request, model.operation_model("UpdateJob").input_shape)
    update = request["JobUpdate"]
    assert update["Role"] == "existing-role"
    assert update["Connections"]["Connections"] == ["private-network"]
    assert update["Timeout"] == 45
    assert update["DefaultArguments"]["--custom"] == "keep"
    assert "--source-key" not in update["DefaultArguments"]
    assert "--config-s3" not in update["DefaultArguments"]
    assert "AllocatedCapacity" not in update
    glue.create_job.assert_not_called()


def test_generated_arguments_reach_processing_script(workflow_deploy, monkeypatch):
    code, glue, _, _ = workflow_deploy
    exec(code, {})
    arguments = glue.create_job.call_args.kwargs["DefaultArguments"]
    monkeypatch.setattr(job_script, "load_dotenv", Mock())
    process = Mock(return_value=1)
    monkeypatch.setattr(job_script, "process_file", process)
    argv = ["--source-key", "compras/sc7/test.csv"]
    for key, value in arguments.items():
        argv.extend([key, value])
    assert job_script.main(argv) == 1
    assert process.call_args.args[2]["merge_keys"] == ["r_e_c_n_o"]
    assert process.call_args.args[2]["table_location"] == "s3://silver/iceberg/sc7/"


@pytest.mark.parametrize("failure", ["missing_role", "access_denied", "wrong_type", "argument_conflict", "upload"])
def test_failures_do_not_create_or_update(workflow_deploy, failure):
    code, glue, s3, model = workflow_deploy
    if failure == "missing_role":
        os.environ.pop("AWS_ROLE_ARN")
    elif failure == "access_denied":
        glue.get_job.side_effect = RuntimeError("AccessDenied")
    elif failure in {"wrong_type", "argument_conflict"}:
        glue.get_job.side_effect = None
        glue.get_job.return_value = {"Job": {
            "Command": {"Name": "glueetl" if failure == "wrong_type" else "pythonshell", "PythonVersion": "3.9"},
            "NonOverridableArguments": {"--library-set": "analytics"},
        }}
    else:
        s3.upload_file.side_effect = RuntimeError("upload failed")
    with pytest.raises((ValueError, RuntimeError)):
        exec(code, {})
    glue.create_job.assert_not_called()
    glue.update_job.assert_not_called()
    if failure != "upload":
        s3.upload_file.assert_not_called()
