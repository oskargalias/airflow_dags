"""Build Airflow DAGs that execute a Python project from GitHub.

Credentials stay out of DAG files and command arguments. Private repositories
are cloned through a temporary GIT_ASKPASS helper using an Airflow Variable.
"""

from __future__ import annotations

import os
import re
import shlex
import stat
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def _relative_path(value: str, field: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ValueError(f"{field} must be a relative path inside the repository")
    return path


def validate_main_config(main_config: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize the declarative DAG configuration."""

    config = dict(main_config)
    repository = config.get("repo_full_name")
    entrypoint = config.get("entrypoint")

    if not isinstance(repository, str) or not _REPOSITORY_RE.fullmatch(repository):
        raise ValueError("repo_full_name must look like 'owner/repository'")
    if not isinstance(entrypoint, str):
        raise ValueError("entrypoint is required")
    _relative_path(entrypoint, "entrypoint")

    requirements = config.get("requirements")
    if requirements is not None:
        if not isinstance(requirements, str):
            raise ValueError("requirements must be a relative path or null")
        _relative_path(requirements, "requirements")

    working_directory = config.get("working_directory", ".")
    if not isinstance(working_directory, str):
        raise ValueError("working_directory must be a relative path")
    if working_directory != ".":
        _relative_path(working_directory, "working_directory")

    arguments = config.get("arguments", [])
    if not isinstance(arguments, list) or not all(isinstance(item, str) for item in arguments):
        raise ValueError("arguments must be a list of strings")

    environment = config.get("environment", {})
    secret_variables = config.get("secret_variables", {})
    for field, mapping in (("environment", environment), ("secret_variables", secret_variables)):
        if not isinstance(mapping, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in mapping.items()
        ):
            raise ValueError(f"{field} must be a string-to-string mapping")

    timeout = config.get("timeout_minutes", 60)
    retries = config.get("retries", 0)
    if not isinstance(timeout, int) or timeout < 1:
        raise ValueError("timeout_minutes must be a positive integer")
    if not isinstance(retries, int) or retries < 0:
        raise ValueError("retries must be a non-negative integer")

    config.setdefault("schedule", None)
    config.setdefault("ref", None)
    config.setdefault("requirements", None)
    config.setdefault("working_directory", ".")
    config.setdefault("arguments", [])
    config.setdefault("environment", {})
    config.setdefault("secret_variables", {})
    config.setdefault("github_token_variable", "github_token")
    config.setdefault("timeout_minutes", 60)
    config.setdefault("retries", 0)
    config.setdefault("tags", ["github-project"])
    return config


def _run(command: list[str], *, cwd: Path | None = None, env: Mapping[str, str] | None = None) -> None:
    print(f"+ {shlex.join(command)}", flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def _askpass_script(directory: Path) -> Path:
    script = directory / "git-askpass.sh"
    script.write_text(
        "#!/bin/sh\n"
        "case \"$1\" in\n"
        "  *Username*) printf '%s\\n' 'x-access-token' ;;\n"
        "  *Password*) printf '%s\\n' \"$GITHUB_TOKEN\" ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    script.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    return script


def _clone_repository(config: Mapping[str, Any], destination: Path, token: str | None) -> str:
    repository = config["repo_full_name"]
    url = f"https://github.com/{repository}.git"
    clone_env = os.environ.copy()
    clone_env["GIT_TERMINAL_PROMPT"] = "0"

    if token:
        askpass = _askpass_script(destination.parent)
        clone_env["GIT_ASKPASS"] = str(askpass)
        clone_env["GITHUB_TOKEN"] = token

    ref = config.get("ref")
    if ref:
        destination.mkdir()
        _run(["git", "init", "--quiet", str(destination)], env=clone_env)
        _run(["git", "-C", str(destination), "remote", "add", "origin", url], env=clone_env)
        _run(
            ["git", "-C", str(destination), "fetch", "--quiet", "--depth", "1", "origin", str(ref)],
            env=clone_env,
        )
        _run(["git", "-C", str(destination), "checkout", "--quiet", "--detach", "FETCH_HEAD"], env=clone_env)
    else:
        _run(["git", "clone", "--quiet", "--depth", "1", url, str(destination)], env=clone_env)

    return subprocess.check_output(
        ["git", "-C", str(destination), "rev-parse", "HEAD"], text=True
    ).strip()


def _project_environment(temporary_path: Path) -> dict[str, str]:
    """Create a minimal environment without Airflow or database credentials."""

    home = temporary_path / "home"
    temp = temporary_path / "tmp"
    home.mkdir()
    temp.mkdir()
    environment = {
        "HOME": str(home),
        "TMPDIR": str(temp),
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "PYTHONUNBUFFERED": "1",
    }
    for name in ("LANG", "LC_ALL", "SSL_CERT_FILE"):
        if value := os.environ.get(name):
            environment[name] = value
    return environment


def _automation_context_environment(context: Mapping[str, Any]) -> dict[str, str]:
    """Expose a small, non-secret subset of the Airflow run context."""

    mapping = {
        "data_interval_start": "AUTOMATION_DATA_INTERVAL_START",
        "data_interval_end": "AUTOMATION_DATA_INTERVAL_END",
        "logical_date": "AUTOMATION_LOGICAL_DATE",
        "run_id": "AUTOMATION_RUN_ID",
    }
    environment: dict[str, str] = {}
    for context_name, environment_name in mapping.items():
        value = context.get(context_name)
        if value is None:
            continue
        environment[environment_name] = value.isoformat() if hasattr(value, "isoformat") else str(value)
    return environment


def run_git_project(main_config: Mapping[str, Any]) -> None:
    """Clone, isolate dependencies and execute one configured project."""

    from airflow.models import Variable
    from airflow.operators.python import get_current_context

    config = validate_main_config(main_config)
    token = Variable.get(config["github_token_variable"], default_var=None)

    with tempfile.TemporaryDirectory(prefix="airflow-project-") as temporary:
        temporary_path = Path(temporary)
        repository_path = temporary_path / "repository"
        commit = _clone_repository(config, repository_path, token)
        token = None
        print(f"Checked out {config['repo_full_name']} at {commit}", flush=True)

        entrypoint = repository_path / Path(*_relative_path(config["entrypoint"], "entrypoint").parts)
        if not entrypoint.is_file():
            raise FileNotFoundError(f"Entrypoint does not exist: {config['entrypoint']}")

        working_directory = repository_path
        if config["working_directory"] != ".":
            working_directory = repository_path / Path(
                *_relative_path(config["working_directory"], "working_directory").parts
            )
        if not working_directory.is_dir():
            raise NotADirectoryError(f"Working directory does not exist: {config['working_directory']}")

        virtualenv = temporary_path / "venv"
        _run([sys.executable, "-m", "venv", str(virtualenv)])
        project_python = virtualenv / "bin" / "python"
        project_env = _project_environment(temporary_path)

        if config["requirements"]:
            requirements = repository_path / Path(
                *_relative_path(config["requirements"], "requirements").parts
            )
            if not requirements.is_file():
                raise FileNotFoundError(f"Requirements file does not exist: {config['requirements']}")
            _run(
                [
                    str(project_python),
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "--requirement",
                    str(requirements),
                ],
                env=project_env,
            )

        project_env.update(config["environment"])
        project_env.update(_automation_context_environment(get_current_context()))
        for environment_name, airflow_variable in config["secret_variables"].items():
            project_env[environment_name] = Variable.get(airflow_variable)
        _run(
            [str(project_python), str(entrypoint), *config["arguments"]],
            cwd=working_directory,
            env=project_env,
        )


def _parse_start_date(value: Any) -> datetime:
    if value is None:
        return datetime(2024, 1, 1, tzinfo=timezone.utc)
    if not isinstance(value, str):
        raise ValueError("start_date must be an ISO-8601 string")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("start_date must include a timezone offset")
    return parsed


def build_git_project_dag(dag_id: str, main_config: Mapping[str, Any]):
    """Return a one-task Airflow DAG from a small declarative config."""

    from airflow import DAG
    from airflow.operators.python import PythonOperator

    config = validate_main_config(main_config)
    dag = DAG(
        dag_id=dag_id,
        schedule=config["schedule"],
        start_date=_parse_start_date(config.get("start_date")),
        catchup=False,
        max_active_runs=1,
        tags=config["tags"],
        default_args={
            "retries": config["retries"],
            "retry_delay": timedelta(minutes=5),
        },
    )
    PythonOperator(
        task_id="run_project",
        python_callable=run_git_project,
        op_kwargs={"main_config": config},
        execution_timeout=timedelta(minutes=config["timeout_minutes"]),
        dag=dag,
    )
    return dag
