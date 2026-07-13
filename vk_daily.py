from git_project_runner import build_git_project_dag


# The project code lives in its own GitHub repository. Merging a change there is
# enough for the next run to pick it up; no code is copied into Airflow manually.
main_config = {
    "repo_full_name": "oskargalias/standard_server_jupyter",
    "ref": "master",
    "entrypoint": "VK2/scripts/daily.py",
    "requirements": "VK2/req.txt",
    "schedule": "27 10 * * *",
    "start_date": "2024-09-17T00:00:00+03:00",
    "timeout_minutes": 120,
    "retries": 1,
    "tags": ["github-project", "vk"],
}


dag = build_git_project_dag("vk_daily", main_config)
