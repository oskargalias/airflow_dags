# GitHub projects in Airflow

Each Airflow DAG is a small `main_config`. Project code remains in its own
GitHub repository and is cloned fresh for every run, so merging project code is
enough to change the next execution.

## Add a project

1. Copy `project_dag.py.example` to `<dag_id>.py`.
2. Set `repo_full_name`, `ref`, `entrypoint`, optional `requirements`, and
   `schedule`.
3. Open a pull request to this repository and merge it into `main`.
4. The server synchronizes `main` automatically; the DAG appears in Airflow
   within a few minutes.

Minimal example:

```python
from git_project_runner import build_git_project_dag

main_config = {
    "repo_full_name": "oskargalias/my-project",
    "ref": "main",
    "entrypoint": "main.py",
    "requirements": "requirements.txt",
    "schedule": "0 9 * * *",
    "start_date": "2026-01-01T00:00:00+03:00",
}

dag = build_git_project_dag("my_project", main_config)
```

## Configuration

- `repo_full_name`: GitHub `owner/repository`.
- `ref`: branch, tag, or commit SHA. Omit to use the default branch.
- `entrypoint`: Python file inside the repository.
- `requirements`: optional requirements file. Dependencies are installed into
  a temporary virtual environment for each run.
- `arguments`: optional command-line arguments.
- `working_directory`: optional working directory inside the repository.
- `schedule`: Airflow cron/preset or `None` for a manual DAG.
- `environment`: non-secret environment variables committed with the DAG.
- `secret_variables`: mapping of process environment names to Airflow Variable
  names. Never commit secret values.
- `timeout_minutes`, `retries`, `tags`: execution controls.

Private repositories use the Airflow Variable `github_token`. The token is
passed to Git through a temporary `GIT_ASKPASS` helper, never embedded in a URL
or command argument, and is not passed to the project process. Project and pip
processes receive a minimal environment, not Airflow's database and broker
configuration.

## Trust boundary

Requirements files and project scripts execute code. Only point a DAG at a
repository and ref you trust. If a project needs system packages or stronger
isolation, give it a dedicated container image instead of bloating the shared
Airflow worker.
