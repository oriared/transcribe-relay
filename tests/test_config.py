import os
import unittest
from pathlib import Path
from unittest.mock import patch

from transcribe_relay.config import load_config

ENVIRONMENT = {
    "TRANSCRIBE_RELAY_HOST": "127.0.0.1",
    "TRANSCRIBE_RELAY_PORT": "8000",
    "TRANSCRIBE_RELAY_DATA_DIR": "jobs",
    "TRANSCRIBE_RELAY_CLIENTS": '{"first": "secret-example"}',
    "TRANSCRIBE_RELAY_MAX_UPLOAD_BYTES": "1024",
    "TRANSCRIBE_RELAY_BUZZ_COMMAND": (
        '["flatpak", "run", "io.github.chidiwilliams.Buzz"]'
    ),
    "TRANSCRIBE_RELAY_BUZZ_ARGS": "[]",
    "TRANSCRIBE_RELAY_BUZZ_TIMEOUT_SECONDS": "120",
}


class ConfigTests(unittest.TestCase):
    def test_minimal_environment(self):
        env = {
            k: ENVIRONMENT[k]
            for k in (
                "TRANSCRIBE_RELAY_HOST",
                "TRANSCRIBE_RELAY_PORT",
                "TRANSCRIBE_RELAY_CLIENTS",
            )
        }
        with patch.dict(os.environ, env, clear=True):
            config = load_config()
        self.assertEqual(config.data_dir, Path.cwd() / "relay-data")
        self.assertEqual(config.max_upload_bytes, 1024**3)
        self.assertEqual(config.buzz_timeout_seconds, 3600)
        self.assertEqual(config.buzz_command, ["buzz"])
        self.assertEqual(config.buzz_args, [])

    def test_model_options_are_forwarded_without_overriding_other_arguments(self):
        env = ENVIRONMENT | {
            "TRANSCRIBE_RELAY_BUZZ_MODEL_TYPE": "whisper",
            "TRANSCRIBE_RELAY_BUZZ_MODEL_SIZE": "small",
            "TRANSCRIBE_RELAY_BUZZ_ARGS": '["--word-timestamps"]',
        }
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                load_config().buzz_args,
                [
                    "--word-timestamps",
                    "--model-type",
                    "whisper",
                    "--model-size",
                    "small",
                ],
            )
            os.environ["TRANSCRIBE_RELAY_BUZZ_ARGS"] = '["-s","tiny"]'
            with self.assertRaisesRegex(ValueError, "configured twice"):
                load_config()

    def test_extra_args_cannot_override_russian_transcription(self):
        for args in ('["--language=en"]', '["-l","en"]', '["--task","translate"]'):
            with (
                self.subTest(args=args),
                patch.dict(
                    os.environ,
                    ENVIRONMENT | {"TRANSCRIBE_RELAY_BUZZ_ARGS": args},
                    clear=True,
                ),
                self.assertRaisesRegex(ValueError, "task and language are fixed"),
            ):
                load_config()

    def test_relative_paths_use_current_working_directory_and_json_is_preserved(self):
        with patch.dict(os.environ, ENVIRONMENT, clear=True):
            config = load_config()

        self.assertEqual(config.data_dir, Path.cwd() / "jobs")
        self.assertEqual(config.clients["first"].get_secret_value(), "secret-example")
        self.assertEqual(config.buzz_command[:2], ["flatpak", "run"])
        self.assertEqual(config.buzz_args, [])

    def test_missing_and_blank_values_are_rejected(self):
        for value in (None, "", "  "):
            environment = ENVIRONMENT.copy()
            if value is None:
                del environment["TRANSCRIBE_RELAY_HOST"]
            else:
                environment["TRANSCRIBE_RELAY_HOST"] = value
            with (
                self.subTest(value=value),
                patch.dict(os.environ, environment, clear=True),
                self.assertRaisesRegex(ValueError, "TRANSCRIBE_RELAY_HOST"),
            ):
                load_config()

    def test_invalid_integer_does_not_expose_value(self):
        secret_value = "not-an-integer-secret"
        environment = ENVIRONMENT | {"TRANSCRIBE_RELAY_PORT": secret_value}
        with (
            patch.dict(os.environ, environment, clear=True),
            self.assertRaises(ValueError) as raised,
        ):
            load_config()

        message = str(raised.exception)
        self.assertIn("TRANSCRIBE_RELAY_PORT must be an integer", message)
        self.assertNotIn(secret_value, message)

    def test_invalid_json_does_not_expose_value(self):
        secret_value = '{"private": invalid}'
        environment = ENVIRONMENT | {"TRANSCRIBE_RELAY_CLIENTS": secret_value}
        with (
            patch.dict(os.environ, environment, clear=True),
            self.assertRaises(ValueError) as raised,
        ):
            load_config()

        message = str(raised.exception)
        self.assertIn("TRANSCRIBE_RELAY_CLIENTS must be valid JSON", message)
        self.assertNotIn(secret_value, message)

    def test_invalid_config_does_not_print_token(self):
        environment = ENVIRONMENT | {
            "TRANSCRIBE_RELAY_BUZZ_TIMEOUT_SECONDS": "0",
        }
        with (
            patch.dict(os.environ, environment, clear=True),
            self.assertRaises(ValueError) as raised,
        ):
            load_config()

        self.assertNotIn("secret-example", str(raised.exception))
