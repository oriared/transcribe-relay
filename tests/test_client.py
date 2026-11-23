import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

from transcribe_relay.client import Client, Settings, Unauthorized
from transcribe_relay.client_http import ApiError, NetworkError, OutputConflict, Relay
from transcribe_relay.client_state import Journal


class ClientTests(unittest.TestCase):
    def setUp(self):
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

    def test_output_conflict_never_acknowledges(self):
        self.relay.upload.return_value = self.job
        self.relay.download.side_effect = OutputConflict()
        self.client.process(12)
        self.client.process(13)
        self.client.process(30)
        self.relay.ack.assert_not_called()
        self.relay.download.assert_called_once()

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

    def test_config_paths_and_token_privacy(self):
        config = self.root / "client.toml"
        config.write_text(
            'server_url="http://localhost"\ntoken="private-token"\nwatch_dir="watch"\nstate_dir="state"\n'
        )
        settings = Settings.load(config)
        self.assertEqual(settings.watch_dir, self.watch)
        self.assertEqual(settings.state_dir, self.state)
        self.assertNotIn("private-token", repr(settings))
        config.write_text(config.read_text().replace('="state"', '="watch/private"'))
        with self.assertRaises(ValueError):
            Settings.load(config)
