import copy
from unittest.mock import Mock

import pytest

from scripts import deploy_glue as deploy
from jobs import bronze_to_silver as job


@pytest.fixture
def existing():
    return {"Name": "sc7", "Role": "existing-role", "CreatedOn": "yesterday",
            "Command": {"Name": "pythonshell", "PythonVersion": "3.9", "ScriptLocation": "s3://old/script.py"},
            "MaxCapacity": 1, "AllocatedCapacity": 1, "Timeout": 60,
            "Connections": {"Connections": ["private-network"]},
            "DefaultArguments": {"--custom": "keep", "--source-key": "old.csv", "--config-s3": "old.json"}}


def test_update_preserves_job_and_removes_legacy(existing, processing_env):
    original = copy.deepcopy(existing)
    arguments = deploy.processing_arguments(processing_env)
    update = deploy.build_update(existing, "s3://new/script.py", "pandas==2.2.1", arguments,
                                 set(existing) - {"Name", "CreatedOn"})
    assert existing == original
    assert update["Role"] == "existing-role"
    assert update["Connections"] == existing["Connections"]
    assert update["Timeout"] == 60
    assert "AllocatedCapacity" not in update
    assert update["Command"]["ScriptLocation"] == "s3://new/script.py"
    assert update["DefaultArguments"]["--custom"] == "keep"
    assert "--source-key" not in update["DefaultArguments"]
    assert "--config-s3" not in update["DefaultArguments"]
    assert update["DefaultArguments"]["--env-MERGE_KEYS"] == "r_e_c_n_o"
    assert update["ExecutionProperty"]["MaxConcurrentRuns"] == 1


@pytest.mark.parametrize("command", [{}, {"Name": "glueetl"}, {"Name": "pythonshell", "PythonVersion": "3.6"}])
def test_wrong_job_rejected(existing, command):
    existing["Command"] = command
    with pytest.raises(ValueError, match="Python Shell"):
        deploy.build_update(existing, "uri", "modules", {}, existing)


def test_non_overridable_conflict(existing):
    existing["NonOverridableArguments"] = {"--library-set": "analytics"}
    with pytest.raises(ValueError, match="NonOverridable"):
        deploy.build_update(existing, "uri", "modules", {}, existing)


def test_modules(tmp_path):
    path = tmp_path / "requirements.txt"
    path.write_text("# comment\npandas==2.2.1\n\nnumpy==1.26.4\n")
    assert deploy.dependency_modules(path) == "pandas==2.2.1,numpy==1.26.4"
    for content in ["", "pandas>=2", "-r other.txt"]:
        path.write_text(content)
        with pytest.raises(ValueError):
            deploy.dependency_modules(path)


def test_deploy_existing_only(existing, processing_env):
    env = dict(processing_env, GLUE_JOB_NAME="sc7", ARTIFACTS_BUCKET="artifacts",
               GITHUB_SHA="commit", GITHUB_RUN_ID="42", AWS_SECRET_ACCESS_KEY="never-forward")
    glue, s3, session = Mock(), Mock(), Mock()
    session.client.side_effect = lambda name: {"glue": glue, "s3": s3}[name]
    glue.get_job.return_value = {"Job": existing}
    glue.meta.service_model.shape_for.return_value.members = set(existing) - {"Name", "CreatedOn"}
    uri = deploy.deploy(session, env)
    assert uri == "s3://artifacts/releases/sc7/commit/42/bronze_to_silver.py"
    s3.upload_file.assert_called_once()
    glue.update_job.assert_called_once()
    assert "never-forward" not in str(glue.update_job.call_args)
    glue.create_job.assert_not_called()
    s3.upload_file.side_effect = RuntimeError("upload failed")
    glue.update_job.reset_mock()
    with pytest.raises(RuntimeError):
        deploy.deploy(session, env)
    glue.update_job.assert_not_called()
    glue.get_job.side_effect = RuntimeError("job missing")
    s3.upload_file.reset_mock()
    with pytest.raises(RuntimeError):
        deploy.deploy(session, env)
    s3.upload_file.assert_not_called()


def test_missing_deploy_variables():
    with pytest.raises(ValueError, match="GLUE_JOB_NAME"):
        deploy.deploy(Mock(), {})


def test_deploy_resolves_prefixes_before_passing_to_glue(processing_env):
    processing_env.pop("SOURCE_BUCKET")
    processing_env.update(BRONZE_BUCKET="bronze", SILVER_BUCKET="silver",
                          TABLE_LOCATION="iceberg/sc7/", TEMP_PATH="staging/", S3_OUTPUT="results/")
    arguments = deploy.processing_arguments(processing_env)
    assert arguments["--env-SOURCE_BUCKET"] == "bronze"
    assert arguments["--env-TABLE_LOCATION"] == "s3://silver/iceberg/sc7/"
    assert arguments["--env-TEMP_PATH"] == "s3://silver/staging/"
    assert arguments["--env-S3_OUTPUT"] == "s3://silver/results/"


def test_run_success_and_poll():
    glue = Mock()
    glue.start_job_run.return_value = {"JobRunId": "jr_test"}
    glue.get_job_run.side_effect = [{"JobRun": {"JobRunState": state}} for state in ["RUNNING", "SUCCEEDED"]]
    sleep = Mock()
    assert deploy.run_and_wait(glue, "sc7", "file.csv", "v1", sleep=sleep, clock=lambda: 0) == "jr_test"
    sleep.assert_called_once_with(30)
    assert glue.start_job_run.call_args.kwargs["Arguments"]["--source-version-id"] == "v1"


@pytest.mark.parametrize("state", ["FAILED", "TIMEOUT", "STOPPED", "ERROR", "EXPIRED"])
def test_run_failure(state):
    glue = Mock()
    glue.start_job_run.return_value = {"JobRunId": "jr_test"}
    glue.get_job_run.return_value = {"JobRun": {"JobRunState": state}}
    with pytest.raises(RuntimeError, match=state):
        deploy.run_and_wait(glue, "sc7", "file.csv", clock=lambda: 0)


def test_poll_timeout():
    glue = Mock()
    glue.start_job_run.return_value = {"JobRunId": "jr_test"}
    with pytest.raises(TimeoutError):
        deploy.run_and_wait(glue, "sc7", "file.csv", clock=Mock(side_effect=[0, 4000]))


@pytest.mark.parametrize("key", ["", "file.csv"])
def test_deploy_main(monkeypatch, key):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("GLUE_JOB_NAME", "sc7")
    monkeypatch.setenv("RUN_SOURCE_KEY", key)
    monkeypatch.setattr(deploy.boto3, "Session", Mock())
    monkeypatch.setattr(deploy, "deploy", Mock(return_value="s3://artifacts/script.py"))
    run = Mock()
    monkeypatch.setattr(deploy, "run_and_wait", run)
    deploy.main()
    assert run.call_count == bool(key)


def test_glue_arguments_to_environment(monkeypatch, processing_env):
    monkeypatch.setattr(job, "load_dotenv", Mock())
    monkeypatch.setattr(job.boto3, "Session", Mock())
    processor = Mock(return_value=1)
    monkeypatch.setattr(job, "process_file", processor)
    for name in job.PROCESSING_ENV_KEYS:
        monkeypatch.delenv(name, raising=False)
    args = ["--source-key", "compras/sc7/file.csv"]
    for key, value in deploy.processing_arguments(processing_env).items():
        args.extend([key, value])
    assert job.main(args) == 1
    assert processor.call_args.args[2]["source_bucket"] == "bronze"
    assert processor.call_args.args[2]["merge_keys"] == ["r_e_c_n_o"]
