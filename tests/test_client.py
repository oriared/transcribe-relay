import hashlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, call, patch
from uuid import uuid4

from transcribe_relay.client import Client, Settings, Unauthorized
from transcribe_relay.client_http import ApiError, NetworkError, OutputConflict, Relay
from transcribe_relay.client_state import Journal


class ClientTests(unittest.TestCase):
    def setUp(self):
        notifier = patch("transcribe_relay.client.notify")
        self.notify = notifier.start()
        self.addCleanup(notifier.stop)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.watch = self.root / "watch"
        self.state = self.root / "state"
        self.watch.mkdir()
        self.state.mkdir()
        self.relay = Mock(spec=Relay)
        self.open_journal()
        self.journal.scan(0)
        self.media = self.watch / "lecture.wav"
        self.media.write_bytes(b"speech")
        self.journal.scan(2)
        self.journal.scan(12)
        self.job = {"id": str(uuid4()), "status": "completed", "error": None}

    def open_journal(self):
        self.journal = Journal(self.state, self.watch)
        self.addCleanup(self.journal.close)
        self.client = Client(self.journal, self.relay)

    def test_lost_upload_reply_and_ack_resume_from_disk(self):
        self.relay.upload.side_effect = [NetworkError("disconnected"), self.job]
        self.client.process(12)
        self.client.process(14)
        self.assertEqual(self.relay.upload.call_count, 1)
        self.journal.close()
        self.open_journal()
        self.client.process(18)
        first, retry = self.relay.upload.call_args_list
        self.assertEqual(first.args[3:], retry.args[3:])
        self.assertEqual(first.args[4], hashlib.sha256(b"speech").hexdigest())
        self.relay.download.side_effect = lambda jid, path: path.write_text("text")
        self.client.process(19)
        self.relay.ack.side_effect = [NetworkError("lost ack reply"), None]
        self.client.process(20)
        self.journal.close()
        self.open_journal()
        self.client.process(26)
        self.assertEqual(self.journal.due(100), [])
        self.assertEqual(self.media.read_bytes(), b"speech")
        self.assertEqual(self.media.with_name("lecture.wav.txt").read_text(), "text")
        self.assertEqual(self.relay.upload.call_count, 2)
        self.relay.download.assert_called_once()
        self.assertEqual(self.relay.ack.call_count, 2)
        self.assertEqual(
            self.notify.call_args_list,
            [
                call("Файл принят сервером", "lecture.wav"),
                call("Расшифровка сохранена", "lecture.wav.txt"),
            ],
        )

    def test_output_conflict_never_acknowledges(self):
        self.relay.upload.return_value = self.job
        self.relay.download.side_effect = OutputConflict()
        self.client.process(12)
        self.client.process(13)
        self.client.process(30)
        self.relay.ack.assert_not_called()
        self.relay.download.assert_called_once()
        self.notify.assert_called_with("Конфликт файла результата", "lecture.wav.txt")

    def test_database_error_prevents_upload(self):
        self.journal.update = Mock(side_effect=sqlite3.OperationalError("disk full"))
        with self.assertRaises(sqlite3.OperationalError):
            self.client.process(12)
        self.relay.upload.assert_not_called()

    def test_auth_failure_stops_without_consuming_key(self):
        self.relay.upload.side_effect = ApiError(401, "unauthorized")
        with self.assertRaises(Unauthorized):
            self.client.process(12)
        self.assertEqual(
            self.journal.due(13)[0]["key"], self.relay.upload.call_args.args[3]
        )

    def test_environment_paths_and_token_privacy(self):
        previous_cwd = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, previous_cwd)
        environment = {
            "TRANSCRIBE_RELAY_SERVER_URL": "http://localhost",
            "TRANSCRIBE_RELAY_TOKEN": "private-token",
            "TRANSCRIBE_RELAY_WATCH_DIR": "watch",
            "TRANSCRIBE_RELAY_STATE_DIR": "state",
        }
        with patch.dict(os.environ, environment, clear=True):
            settings = Settings.load()
        self.assertEqual(settings.watch_dir, self.watch)
        self.assertEqual(settings.state_dir, self.state)
        self.assertNotIn("private-token", repr(settings))

    def test_missing_and_blank_environment_values_are_rejected_privately(self):
        environment = {
            "TRANSCRIBE_RELAY_SERVER_URL": "http://localhost",
            "TRANSCRIBE_RELAY_TOKEN": "private-token",
            "TRANSCRIBE_RELAY_WATCH_DIR": str(self.watch),
        }
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(
                ValueError, "TRANSCRIBE_RELAY_STATE_DIR"
            ) as caught:
                Settings.load()
        self.assertNotIn("private-token", str(caught.exception))

        environment["TRANSCRIBE_RELAY_STATE_DIR"] = " "
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(ValueError, "TRANSCRIBE_RELAY_STATE_DIR"):
                Settings.load()

    def test_invalid_token_and_nested_state_directory_are_rejected(self):
        environment = {
            "TRANSCRIBE_RELAY_SERVER_URL": "http://localhost",
            "TRANSCRIBE_RELAY_TOKEN": "private token",
            "TRANSCRIBE_RELAY_WATCH_DIR": str(self.watch),
            "TRANSCRIBE_RELAY_STATE_DIR": str(self.state),
        }
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(ValueError, "printable ASCII"):
                Settings.load()

        environment["TRANSCRIBE_RELAY_TOKEN"] = "private-token"
        environment["TRANSCRIBE_RELAY_STATE_DIR"] = str(self.watch / "private")
        with patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(ValueError, "outside watch_dir"):
                Settings.load()
