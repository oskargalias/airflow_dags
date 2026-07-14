import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from git_project_runner import _automation_context_environment, _project_environment


class ProjectEnvironmentTest(unittest.TestCase):
    def test_airflow_credentials_are_not_inherited(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict(
            os.environ,
            {
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "secret-database-url",
                "AIRFLOW__CELERY__BROKER_URL": "secret-broker-url",
            },
        ):
            environment = _project_environment(Path(temporary))

        self.assertNotIn("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", environment)
        self.assertNotIn("AIRFLOW__CELERY__BROKER_URL", environment)
        self.assertIn("PATH", environment)

    def test_only_safe_context_fields_are_exported(self):
        environment = _automation_context_environment(
            {
                "data_interval_start": "2026-07-13T20:00:00+03:00",
                "data_interval_end": "2026-07-14T20:00:00+03:00",
                "run_id": "scheduled__2026-07-14",
                "connection": "must-not-leak",
            }
        )
        self.assertEqual(
            environment["AUTOMATION_DATA_INTERVAL_START"],
            "2026-07-13T20:00:00+03:00",
        )
        self.assertEqual(environment["AUTOMATION_RUN_ID"], "scheduled__2026-07-14")
        self.assertNotIn("connection", environment)
