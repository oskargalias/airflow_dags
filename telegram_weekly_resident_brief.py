from git_project_runner import build_git_project_dag


# The same GitHub project serves a separate chat and prompt. Airflow owns only
# the schedule and runtime parameters; Telegram/Codex credentials remain in
# encrypted Variables and never enter the repository.
main_config = {
    "repo_full_name": "oskargalias/telegram-daily-brief",
    "ref": "main",
    "entrypoint": "src/main.py",
    "requirements": "requirements.txt",
    "schedule": "0 20 * * 0",
    "start_date": "2026-01-01T00:00:00+03:00",
    "timeout_minutes": 30,
    "retries": 2,
    "environment": {
        "TELEGRAM_BRIDGE_URL": "http://telegram-bridge:8080",
        "CODEX_RUNNER_URL": "http://codex-runner:8080",
        "TELEGRAM_MESSAGE_LIMIT": "2000",
        "BRIEF_LOOKBACK_HOURS": "168",
        "BRIEF_PROMPT_PATH": "prompts/weekly_brief.md",
    },
    "secret_variables": {
        "TELEGRAM_CHAT": "telegram_weekly_resident_chat",
        "TELEGRAM_BRIDGE_TOKEN": "telegram_bridge_token",
        "CODEX_RUNNER_TOKEN": "codex_runner_token",
    },
    "tags": ["github-project", "telegram", "codex", "weekly"],
}


dag = build_git_project_dag("telegram_weekly_resident_brief", main_config)
