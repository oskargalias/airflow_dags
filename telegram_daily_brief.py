from git_project_runner import build_git_project_dag


# Prompt and project code are pulled from GitHub for every run. The selected
# Telegram chat and internal service tokens live in encrypted Airflow Variables.
main_config = {
    "repo_full_name": "oskargalias/telegram-daily-brief",
    "ref": "main",
    "entrypoint": "src/main.py",
    "requirements": "requirements.txt",
    "schedule": "0 20 * * *",
    "start_date": "2026-07-14T20:00:00+03:00",
    "timeout_minutes": 30,
    "retries": 2,
    "environment": {
        "TELEGRAM_BRIDGE_URL": "http://telegram-bridge:8080",
        "CODEX_RUNNER_URL": "http://codex-runner:8080",
        "TELEGRAM_MESSAGE_LIMIT": "1000",
    },
    "secret_variables": {
        "TELEGRAM_CHAT": "telegram_daily_chat",
        "TELEGRAM_BRIDGE_TOKEN": "telegram_bridge_token",
        "CODEX_RUNNER_TOKEN": "codex_runner_token",
    },
    "tags": ["github-project", "telegram", "codex"],
}


dag = build_git_project_dag("telegram_daily_brief", main_config)
