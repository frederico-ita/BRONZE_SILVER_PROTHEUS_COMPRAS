"""Publica somente o código em um job Glue Python Shell existente."""
import copy
import os
import re
import time
from pathlib import Path

import boto3

from jobs.bronze_to_silver import PROCESSING_ENV_KEYS, load_config


def dependency_modules(path):
    lines = [line.strip() for line in Path(path).read_text(encoding="utf-8").splitlines()
             if line.strip() and not line.lstrip().startswith("#")]
    if not lines or any(not re.fullmatch(r"[A-Za-z0-9_.-]+==[A-Za-z0-9_.+-]+", line) for line in lines):
        raise ValueError("requirements.txt deve conter versões fixadas com ==")
    return ",".join(lines)


def processing_arguments(env):
    config = load_config(env)
    # Expande todos os padrões para evitar herdar configurações antigas do job.
    values = {key: str(config[key.lower()]) for key in PROCESSING_ENV_KEYS
              if key.lower() in config}
    values["MERGE_KEYS"] = ",".join(config["merge_keys"])
    values["C7_EMISSAO_FORMAT"] = config["column_date_formats"]["c7_emissao"]
    values["C7_DATPRF_FORMAT"] = config["column_date_formats"]["c7_datprf"]
    return {"--env-" + key: value for key, value in values.items()}


def build_update(job, script_uri, modules, arguments, allowed_fields):
    command = job.get("Command", {})
    if command.get("Name") != "pythonshell" or command.get("PythonVersion") != "3.9":
        raise ValueError("O job existente deve ser Python Shell 3.9")
    managed = dict(arguments, **{"--additional-python-modules": modules, "--library-set": "none"})
    reserved = set(managed) | {"--config-s3", "--source-bucket", "--source-key", "--source-version-id", "--env-file"}
    if reserved.intersection(job.get("NonOverridableArguments", {})):
        raise ValueError("NonOverridableArguments conflitam com os parâmetros gerenciados pelo deploy")
    update = {key: copy.deepcopy(value) for key, value in job.items() if key in allowed_fields}
    update["Command"]["ScriptLocation"] = script_uri
    update["DefaultArguments"] = {key: value for key, value in job.get("DefaultArguments", {}).items()
                                  if key not in reserved and not key.startswith("--env-")}
    update["DefaultArguments"].update(managed)
    update["ExecutionProperty"] = {"MaxConcurrentRuns": 1}
    if "MaxCapacity" in update:
        update.pop("AllocatedCapacity", None)
    return update


def deploy(session, env):
    for key in ["GLUE_JOB_NAME", "ARTIFACTS_BUCKET", "GITHUB_SHA", "GITHUB_RUN_ID"]:
        if not env.get(key, "").strip():
            raise ValueError("Variável obrigatória para deploy: " + key)
    arguments = processing_arguments(env)
    modules = dependency_modules("requirements.txt")
    glue = session.client("glue")
    name = env["GLUE_JOB_NAME"]
    job = glue.get_job(JobName=name)["Job"]  # Não cria um job caso esteja ausente.
    key = "releases/" + name + "/" + env["GITHUB_SHA"] + "/" + env["GITHUB_RUN_ID"] + "/bronze_to_silver.py"
    uri = "s3://" + env["ARTIFACTS_BUCKET"] + "/" + key
    fields = glue.meta.service_model.shape_for("JobUpdate").members
    update = build_update(job, uri, modules, arguments, fields)
    session.client("s3").upload_file("jobs/bronze_to_silver.py", env["ARTIFACTS_BUCKET"], key)
    glue.update_job(JobName=name, JobUpdate=update)
    return uri


def run_and_wait(glue, name, key, version_id="", timeout=3900, sleep=time.sleep, clock=time.monotonic):
    arguments = {"--source-key": key}
    if version_id:
        arguments["--source-version-id"] = version_id
    run_id = glue.start_job_run(JobName=name, Arguments=arguments)["JobRunId"]
    print("Glue JobRunId:", run_id, flush=True)
    deadline = clock() + timeout
    while clock() < deadline:
        state = glue.get_job_run(JobName=name, RunId=run_id)["JobRun"]["JobRunState"]
        if state == "SUCCEEDED":
            return run_id
        if state not in {"STARTING", "RUNNING", "WAITING", "STOPPING"}:
            raise RuntimeError("Execução Glue " + run_id + " terminou com estado " + state)
        sleep(30)
    raise TimeoutError("Tempo de acompanhamento excedido para " + run_id + "; o job pode continuar na AWS")


def main():
    session = boto3.Session(region_name=os.environ["AWS_REGION"])
    uri = deploy(session, os.environ)
    print("Script publicado:", uri)
    key = os.getenv("RUN_SOURCE_KEY", "")
    if key:
        run_and_wait(session.client("glue"), os.environ["GLUE_JOB_NAME"], key,
                     os.getenv("RUN_SOURCE_VERSION_ID", ""))


if __name__ == "__main__":  # pragma: no cover
    main()
