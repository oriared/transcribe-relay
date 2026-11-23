"""Tests for durable server job state and file cleanup."""

import shutil
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from transcribe_relay.storage import Store


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temporary_directory.name)
        self.store = Store(self.data_dir)

    def tearDown(self):
        self.store.close()
        self.temporary_directory.cleanup()

    def reserve(
        self,
        key: str,
        *,
        client_id: str = "client-a",
        filename: str = "recording.wav",
        size: int = 12,
        sha256: str = "a" * 64,
    ):
        row, created = self.store.reserve(client_id, key, filename, size, sha256)
        self.assertTrue(created)
        return row

    def queue_with_input(
        self,
        key: str,
        *,
        client_id: str = "client-a",
        filename: str = "recording.wav",
    ):
        row = self.reserve(
            key, client_id=client_id, filename=filename, size=len(b"input")
        )
        input_path = self.store.input_path(row)
        input_path.parent.mkdir(parents=True)
        input_path.write_bytes(b"input")
        self.store.enqueue(row["id"])
        return row

    def complete_job(self, key: str = "key"):
        row = self.queue_with_input(key)
        claimed = self.store.claim()
        self.assertIsNotNone(claimed)
        self.store.finish(row["id"], None)
        (self.store.job_dir(row["id"]) / "result.txt").write_text(
            "result", encoding="utf-8"
        )
        return row

    def reopen(self):
        self.store.close()
        self.store = Store(self.data_dir)

    def test_reserve_is_idempotent_without_checking_metadata(self):
        first = self.reserve("same-key")

        second, created = self.store.reserve(
            "client-a",
            "same-key",
            "different.mp3",
            999,
            "b" * 64,
        )

        self.assertFalse(created)
        self.assertEqual(second["id"], first["id"])
        self.assertEqual(second["filename"], "recording.wav")
        self.assertEqual(second["size"], 12)
        self.assertEqual(second["sha256"], "a" * 64)
        self.assertEqual(second["status"], "uploading")
        self.assertIsNone(second["error"])
        self.assertEqual(
            list(second.keys()),
            ["id", "status", "error", "filename", "size", "sha256"],
        )
        self.assertFalse(self.store.job_dir(first["id"]).exists())

    def test_idempotency_keys_and_job_reads_are_scoped_to_client(self):
        first = self.reserve("shared-key")
        second = self.reserve("shared-key", client_id="client-b")

        self.assertNotEqual(second["id"], first["id"])
        self.assertEqual(self.store.get("client-a", first["id"])["id"], first["id"])
        self.assertIsNone(self.store.get("client-b", first["id"]))
        self.assertIsNone(self.store.get("missing-client", first["id"]))
        self.assertIsNone(self.store.get("client-a", str(uuid.uuid4())))

    def test_abort_removes_only_an_uploading_reservation_and_its_files(self):
        row = self.reserve("retryable")
        job_dir = self.store.job_dir(row["id"])
        job_dir.mkdir(parents=True)
        (job_dir / "partial").write_bytes(b"partial")

        self.store.abort(row["id"])

        self.assertIsNone(self.store.get("client-a", row["id"]))
        self.assertFalse(job_dir.exists())
        retried = self.reserve("retryable")
        self.assertNotEqual(retried["id"], row["id"])

        self.store.enqueue(retried["id"])
        self.store.abort(retried["id"])
        self.assertEqual(self.store.get("client-a", retried["id"])["status"], "queued")

    def test_claim_uses_fifo_order_and_finish_records_outcome(self):
        with patch("transcribe_relay.storage.time.time_ns", side_effect=[200, 100]):
            later = self.queue_with_input("later")
            earlier = self.queue_with_input("earlier")

        first_claim = self.store.claim()
        self.assertEqual(first_claim["id"], earlier["id"])
        self.assertEqual(first_claim["status"], "running")
        self.store.finish(first_claim["id"], "buzz_exit")
        failed = self.store.get("client-a", first_claim["id"])
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["error"], "buzz_exit")

        second_claim = self.store.claim()
        self.assertEqual(second_claim["id"], later["id"])
        self.store.finish(second_claim["id"], None)
        completed = self.store.get("client-a", second_claim["id"])
        self.assertEqual(completed["status"], "completed")
        self.assertIsNone(completed["error"])
        self.assertIsNone(self.store.claim())

    def test_claim_breaks_equal_arrival_times_by_uuid(self):
        larger_id = uuid.UUID(int=2)
        smaller_id = uuid.UUID(int=1)
        with (
            patch("transcribe_relay.storage.time.time_ns", return_value=100),
            patch(
                "transcribe_relay.storage.uuid.uuid4",
                side_effect=[larger_id, smaller_id],
            ),
        ):
            larger = self.queue_with_input("larger")
            smaller = self.queue_with_input("smaller")

        self.assertEqual(larger["id"], str(larger_id))
        self.assertEqual(smaller["id"], str(smaller_id))
        self.assertEqual(self.store.claim()["id"], smaller["id"])

    def test_input_path_uses_only_the_lowercase_original_extension(self):
        row = self.reserve("unicode", filename="совещание.Final.MP3")

        self.assertEqual(
            self.store.input_path(row),
            self.data_dir / "jobs" / row["id"] / "input.mp3",
        )

    def test_recover_removes_uploads_and_reconciles_queued_and_running_jobs(self):
        uploading = self.reserve("uploading")
        uploading_dir = self.store.job_dir(uploading["id"])
        uploading_dir.mkdir(parents=True)
        (uploading_dir / "partial").write_bytes(b"partial")

        running = self.queue_with_input("running")
        self.assertEqual(self.store.claim()["id"], running["id"])
        missing = self.reserve("missing")
        self.store.enqueue(missing["id"])
        queued = self.queue_with_input("queued")
        truncated = self.queue_with_input("truncated")
        self.store.input_path(truncated).write_bytes(b"i")

        self.reopen()
        self.store.recover()

        self.assertIsNone(self.store.get("client-a", uploading["id"]))
        self.assertFalse(uploading_dir.exists())
        missing_after = self.store.get("client-a", missing["id"])
        self.assertEqual(missing_after["status"], "failed")
        self.assertEqual(missing_after["error"], "input_missing")
        self.assertEqual(
            self.store.get("client-a", truncated["id"])["error"], "input_missing"
        )
        running_after = self.store.get("client-a", running["id"])
        self.assertEqual(running_after["status"], "failed")
        self.assertEqual(running_after["error"], "server_restarted")
        self.assertEqual(self.store.get("client-a", queued["id"])["status"], "queued")
        self.assertEqual(self.store.claim()["id"], queued["id"])

    def test_cleanup_releases_failed_upload_after_deletion_becomes_possible(self):
        row = self.reserve("retry-key")
        self.store.job_dir(row["id"]).mkdir()
        with (
            patch(
                "transcribe_relay.storage.shutil.rmtree", side_effect=PermissionError
            ),
            self.assertLogs("transcribe_relay.storage", level="WARNING"),
        ):
            self.store.abort(row["id"])
        self.store.cleanup()
        self.assertFalse(self.store.job_dir(row["id"]).exists())
        self.reserve("retry-key")

    def test_acknowledge_is_durable_before_cleanup_and_recover_retries_failure(self):
        row = self.complete_job()
        job_dir = self.store.job_dir(row["id"])

        def fail_after_check(path):
            self.assertEqual(path, job_dir)
            stored = self.store.get("client-a", row["id"])
            self.assertEqual(stored["status"], "acknowledged")
            raise PermissionError("busy")

        with (
            self.assertLogs("transcribe_relay.storage", level="WARNING"),
            patch("transcribe_relay.storage.shutil.rmtree", fail_after_check),
        ):
            self.store.acknowledge(row["id"])

        self.assertTrue(job_dir.exists())
        acknowledged = self.store.get("client-a", row["id"])
        self.assertEqual(acknowledged["status"], "acknowledged")
        self.assertIsNone(acknowledged["error"])

        self.reopen()
        self.store.recover()
        self.assertFalse(job_dir.exists())
        self.assertEqual(
            self.store.get("client-a", row["id"])["status"], "acknowledged"
        )

        with patch(
            "transcribe_relay.storage.shutil.rmtree", wraps=shutil.rmtree
        ) as remove:
            self.store.cleanup()
            self.store.acknowledge(row["id"])
        remove.assert_not_called()


if __name__ == "__main__":
    unittest.main()
