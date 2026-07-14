from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _main_config() -> dict[str, object]:
    tree = ast.parse(
        (ROOT / "telegram_weekly_resident_brief.py").read_text(encoding="utf-8")
    )
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "main_config"
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("main_config assignment not found")


class TelegramWeeklyConfigTests(unittest.TestCase):
    def test_weekly_chat_has_separate_schedule_and_prompt(self) -> None:
        main_config = _main_config()
        self.assertEqual(main_config["schedule"], "0 20 * * 0")
        self.assertEqual(main_config["environment"]["BRIEF_LOOKBACK_HOURS"], "168")
        self.assertEqual(
            main_config["environment"]["BRIEF_PROMPT_PATH"],
            "prompts/weekly_brief.md",
        )
        self.assertEqual(
            main_config["secret_variables"]["TELEGRAM_CHAT"],
            "telegram_weekly_resident_chat",
        )


if __name__ == "__main__":
    unittest.main()
