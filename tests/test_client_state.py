"""Tests for durable client file discovery and operation state."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from transcribe_relay.client_state import Journal
from transcribe_relay.media import EXTENSIONS


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.state_dir = self.root / "state"
        self.watch_dir = self.root / "watch"
        self.watch_dir.mkdir()
        self.extension = sorted(EXTENSIONS)[0]
        self.journal = Journal(self.state_dir, self.watch_dir)

    def tearDown(self):
        self.journal.close()
        self.temporary_directory.cleanup()

    def path(self, name: str = "recording") -> Path:
        return self.watch_dir / f"{name}{self.extension}"

    def write_version(
        self, path: Path, content: bytes, *, mtime_ns: int
    ) -> tuple[int, int]:
        path.write_bytes(content)
        os.utime(path, ns=(mtime_ns, mtime_ns))
        return len(content), mtime_ns

    def reopen(self, watch_dir: Path | None = None):
        self.journal.close()
        self.journal = Journal(self.state_dir, watch_dir or self.watch_dir)

    def test_initial_snapshot_is_baseline_and_new_file_needs_ten_seconds(self):
        existing = self.path("existing")
        self.write_version(existing, b"old", mtime_ns=100)

        self.journal.scan(1.0)
        self.journal.scan(20.0)
        self.assertEqual(self.journal.due(20.0), [])

        added = self.path("added")
        self.write_version(added, b"new", mtime_ns=200)
        self.journal.scan(21.0)
        self.journal.scan(30.999)
        self.assertEqual(self.journal.due(30.999), [])

        self.journal.scan(31.0)
        rows = self.journal.due(31.0)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(
            list(row.keys()),
            [
                "key",
                "path",
                "size",
                "mtime_ns",
                "digest",
                "job_id",
                "output",
                "stage",
            ],
        )
        self.assertEqual(row["path"], str(added.absolute()))
        self.assertEqual(row["size"], 3)
        self.assertEqual(row["mtime_ns"], 200)
        self.assertIsNone(row["digest"])
        self.assertIsNone(row["job_id"])
        self.assertEqual(row["output"], str(added.absolute()) + ".txt")
        self.assertEqual(row["stage"], "upload")

    def test_empty_file_waits_for_stable_nonempty_content(self):
        self.journal.scan(0)
        path = self.path()
        path.touch()
        self.journal.scan(1)
        self.journal.scan(20)
        self.assertEqual(self.journal.due(20), [])
        path.write_bytes(b"audio")
        self.journal.scan(21)
        self.journal.scan(31)
        self.assertEqual(self.journal.due(31)[0]["size"], 5)

    def test_stability_clock_restarts_on_size_or_mtime_change(self):
        path = self.path()
        self.journal.scan(0.0)
        self.write_version(path, b"one", mtime_ns=100)
        self.journal.scan(1.0)

        self.write_version(path, b"two", mtime_ns=200)
        self.journal.scan(10.0)
        self.journal.scan(19.999)
        self.assertEqual(self.journal.due(20.0), [])

        self.journal.scan(20.0)
        self.assertEqual(self.journal.due(20.0)[0]["mtime_ns"], 200)

    def test_restart_preserves_observation_uuid_and_scheduling(self):
        path = self.path()
        self.journal.scan(0.0)
        self.write_version(path, b"content", mtime_ns=100)
        self.journal.scan(2.0)
        self.reopen()

        self.journal.scan(12.0)
        first = self.journal.due(12.0)[0]
        key = first["key"]
        self.reopen()

        self.assertEqual(self.journal.due(12.0)[0]["key"], key)
        self.journal.scan(30.0)
        self.assertEqual([row["key"] for row in self.journal.due(30.0)], [key])

    def test_change_while_offline_is_detected_after_restart(self):
        path = self.path()
        self.write_version(path, b"baseline", mtime_ns=100)
        self.journal.scan(0.0)
        self.journal.close()
        self.write_version(path, b"offline change", mtime_ns=200)
        self.journal = Journal(self.state_dir, self.watch_dir)

        self.journal.scan(20.0)
        self.assertEqual(self.journal.due(29.999), [])
        self.journal.scan(30.0)

        row = self.journal.due(30.0)[0]
        self.assertEqual(row["size"], len(b"offline change"))
        self.assertEqual(row["mtime_ns"], 200)

    def test_update_commits_fields_and_none_leaves_values_unchanged(self):
        path = self.path()
        self.journal.scan(0.0)
        self.write_version(path, b"content", mtime_ns=100)
        self.journal.scan(1.0)
        self.journal.scan(11.0)
        key = self.journal.due(11.0)[0]["key"]

        self.journal.update(
            key,
            stage="poll",
            digest="a" * 64,
            job_id="server-job",
            error="temporary",
            next_at=50.0,
        )
        self.reopen()
        self.assertEqual(self.journal.due(49.0), [])
        row = self.journal.due(50.0)[0]
        self.assertEqual(row["stage"], "poll")
        self.assertEqual(row["digest"], "a" * 64)
        self.assertEqual(row["job_id"], "server-job")

        self.journal.update(key, digest=None, job_id=None, next_at=None)
        self.reopen()
        unchanged = self.journal.due(50.0)[0]
        self.assertEqual(unchanged["digest"], "a" * 64)
        self.assertEqual(unchanged["job_id"], "server-job")

        self.journal.update(key, stage="done")
        self.assertEqual(self.journal.due(100.0), [])

    def test_changed_file_obsoletes_unaccepted_upload(self):
        path = self.path()
        self.journal.scan(0.0)
        self.write_version(path, b"one", mtime_ns=100)
        self.journal.scan(1.0)
        self.journal.scan(11.0)
        old_key = self.journal.due(11.0)[0]["key"]

        self.write_version(path, b"two", mtime_ns=200)
        self.journal.scan(12.0)
        self.assertEqual(self.journal.due(100.0), [])
        self.journal.scan(22.0)

        rows = self.journal.due(22.0)
        self.assertEqual(len(rows), 1)
        self.assertNotEqual(rows[0]["key"], old_key)
        self.assertEqual(rows[0]["mtime_ns"], 200)

    def test_accepted_old_version_survives_and_repeated_version_is_new_work(self):
        path = self.path()
        original = self.write_version(path, b"one", mtime_ns=100)
        self.journal.scan(0.0)

        self.write_version(path, b"two", mtime_ns=200)
        self.journal.scan(1.0)
        self.journal.scan(11.0)
        accepted = self.journal.due(11.0)[0]
        self.journal.update(
            accepted["key"], stage="poll", job_id="accepted", next_at=100.0
        )

        self.write_version(path, b"one", mtime_ns=original[1])
        self.journal.scan(12.0)
        self.journal.scan(22.0)
        rows = self.journal.due(100.0)

        self.assertEqual(len(rows), 2)
        by_stage = {row["stage"]: row for row in rows}
        self.assertEqual(by_stage["poll"]["key"], accepted["key"])
        self.assertEqual(by_stage["poll"]["job_id"], "accepted")
        self.assertEqual(by_stage["upload"]["mtime_ns"], original[1])
        self.assertNotEqual(by_stage["upload"]["key"], accepted["key"])

    def test_missing_file_obsoletes_upload_but_not_accepted_job(self):
        path = self.path()
        self.journal.scan(0.0)
        self.write_version(path, b"one", mtime_ns=100)
        self.journal.scan(1.0)
        self.journal.scan(11.0)
        first = self.journal.due(11.0)[0]
        path.unlink()
        self.journal.scan(12.0)
        self.assertEqual(self.journal.due(100.0), [])

        self.write_version(path, b"two", mtime_ns=200)
        self.journal.scan(13.0)
        self.journal.scan(23.0)
        second = self.journal.due(23.0)[0]
        self.journal.update(second["key"], stage="download", job_id="job", next_at=30.0)
        path.unlink()
        self.journal.scan(24.0)
        self.assertEqual(self.journal.due(30.0)[0]["key"], second["key"])
        self.assertNotEqual(first["key"], second["key"])

    def test_missing_or_inaccessible_directory_does_not_create_partial_baseline(self):
        path = self.path()
        self.write_version(path, b"baseline", mtime_ns=100)
        with patch(
            "transcribe_relay.client_state.os.scandir", side_effect=PermissionError
        ):
            self.journal.scan(0.0)

        self.journal.scan(1.0)
        self.journal.scan(20.0)
        self.assertEqual(self.journal.due(20.0), [])

        self.watch_dir.rename(self.root / "away")
        self.journal.scan(21.0)
        self.assertEqual(self.journal.due(21.0), [])

    def test_ignores_nested_files_symlinks_and_unsupported_extensions(self):
        target = self.path("target")
        self.write_version(target, b"target", mtime_ns=100)
        nested_dir = self.watch_dir / "nested"
        nested_dir.mkdir()
        self.write_version(
            nested_dir / f"nested{self.extension}", b"nested", mtime_ns=100
        )
        (self.watch_dir / "unsupported.txt").write_text("text", encoding="utf-8")
        (self.watch_dir / f"link{self.extension}").symlink_to(target)

        self.journal.scan(0.0)
        target.unlink()
        new_target = self.path("new")
        self.write_version(new_target, b"new", mtime_ns=200)
        self.journal.scan(1.0)
        self.journal.scan(11.0)

        rows = self.journal.due(11.0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["path"], str(new_target.absolute()))

    def test_journal_is_bound_to_absolute_watch_directory(self):
        self.journal.close()
        other = self.root / "other"
        other.mkdir()
        with self.assertRaises(ValueError):
            Journal(self.state_dir, other)
        self.journal = Journal(self.state_dir, self.watch_dir)


if __name__ == "__main__":
    unittest.main()
