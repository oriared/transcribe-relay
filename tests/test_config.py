import tempfile
import unittest
from pathlib import Path

from transcribe_relay.config import load_config

CONFIG = """host = "127.0.0.1"
port = 8000
data_dir = "jobs"
max_upload_bytes = 1024
buzz_command = ["flatpak", "run", "io.github.chidiwilliams.Buzz"]
buzz_args = []
buzz_timeout_seconds = 120
[clients]
first = "secret-example"
"""


class ConfigTests(unittest.TestCase):
    def test_paths_relative_to_config_and_launcher_arguments_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(CONFIG)
            config = load_config(path)
            self.assertEqual(config.data_dir, path.parent / "jobs")
            self.assertEqual(config.buzz_command[:2], ["flatpak", "run"])

    def test_invalid_config_does_not_print_token(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            for contents in (
                CONFIG + 'second = "secret-example"',
                CONFIG.replace("= 120", "= 0"),
            ):
                with self.subTest(), self.assertRaises(ValueError) as raised:
                    path.write_text(contents)
                    load_config(path)
                self.assertNotIn("secret-example", str(raised.exception))
