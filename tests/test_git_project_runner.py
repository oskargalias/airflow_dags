import unittest

from git_project_runner import validate_main_config


class ValidateMainConfigTest(unittest.TestCase):
    def test_minimal_config_is_normalized(self):
        config = validate_main_config(
            {
                "repo_full_name": "owner/project",
                "entrypoint": "scripts/main.py",
            }
        )
        self.assertEqual(config["working_directory"], ".")
        self.assertIsNone(config["requirements"])
        self.assertEqual(config["arguments"], [])

    def test_parent_traversal_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_main_config(
                {
                    "repo_full_name": "owner/project",
                    "entrypoint": "../secret.py",
                }
            )

    def test_shell_command_is_not_a_supported_config_shape(self):
        with self.assertRaises(ValueError):
            validate_main_config(
                {
                    "repo_full_name": "owner/project",
                    "entrypoint": "main.py",
                    "arguments": "--unsafe string",
                }
            )


if __name__ == "__main__":
    unittest.main()
