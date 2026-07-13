import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from git_project_runner import _project_environment


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
