"""Persistent server job state and per-job file locations."""

from __future__ import annotations

import logging
import shutil
import sqlite3
import time
import uuid
from pathlib import Path

_LOGGER = logging.getLogger(__name__)
_PUBLIC_COLUMNS = "id, status, error, filename, size, sha256"


class Store:
    """Store server job state in SQLite and manage completed job cleanup."""

    def __init__(self, data_dir: Path):
        self._jobs_dir = data_dir / "jobs"
        self._jobs_dir.mkdir(mode=0o700, exist_ok=True)
        self._connection = sqlite3.connect(data_dir / "state.sqlite3")
        self._connection.row_factory = sqlite3.Row
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                filename TEXT NOT NULL,
                size INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN (
                        'uploading', 'queued', 'running', 'completed',
                        'failed', 'acknowledged'
                    )
                ),
                error TEXT,
                created_at INTEGER NOT NULL,
                cleanup_pending INTEGER NOT NULL DEFAULT 0 CHECK (
                    cleanup_pending IN (0, 1)
                ),
                UNIQUE (client_id, idempotency_key)
            );

            CREATE INDEX IF NOT EXISTS jobs_queue_fifo
                ON jobs (created_at, id)
                WHERE status = 'queued';

            CREATE INDEX IF NOT EXISTS jobs_cleanup_pending
                ON jobs (id)
                WHERE cleanup_pending = 1;
            """
        )

    def close(self) -> None:
        self._connection.close()

    def recover(self) -> None:
        """Reconcile database state after an unclean server shutdown."""
        queued_rows = self._connection.execute(
            f"SELECT {_PUBLIC_COLUMNS} FROM jobs WHERE status = 'queued'"
        ).fetchall()
        missing_ids = []
        for row in queued_rows:
            path = self.input_path(row)
            if not path.is_file() or path.stat().st_size != row["size"]:
                missing_ids.append(row["id"])

        with self._connection:
            self._connection.execute(
                "UPDATE jobs SET cleanup_pending = 1 WHERE status = 'uploading'"
            )
            self._connection.execute(
                """
                UPDATE jobs
                SET status = 'failed', error = 'server_restarted'
                WHERE status = 'running'
                """
            )
            self._connection.executemany(
                """
                UPDATE jobs
                SET status = 'failed', error = 'input_missing'
                WHERE id = ? AND status = 'queued'
                """,
                ((job_id,) for job_id in missing_ids),
            )

        self.cleanup()

    def reserve(
        self,
        client_id: str,
        key: str,
        filename: str,
        size: int,
        sha256: str,
    ) -> tuple[sqlite3.Row, bool]:
        """Reserve an upload or return the row already using its client key."""
        job_id = str(uuid.uuid4())
        with self._connection:
            cursor = self._connection.execute(
                """
                INSERT INTO jobs (
                    id, client_id, idempotency_key, filename, size, sha256,
                    status, error, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'uploading', NULL, ?)
                ON CONFLICT (client_id, idempotency_key) DO NOTHING
                """,
                (
                    job_id,
                    client_id,
                    key,
                    filename,
                    size,
                    sha256,
                    time.time_ns(),
                ),
            )
            row = self._connection.execute(
                f"""
                SELECT {_PUBLIC_COLUMNS}
                FROM jobs
                WHERE client_id = ? AND idempotency_key = ?
                """,
                (client_id, key),
            ).fetchone()

        if row is None:
            raise RuntimeError("reserved job could not be read")
        return row, cursor.rowcount == 1

    def get(self, client_id: str, id: str) -> sqlite3.Row | None:
        """Return a job only when it belongs to the requesting client."""
        return self._connection.execute(
            f"""
            SELECT {_PUBLIC_COLUMNS}
            FROM jobs
            WHERE client_id = ? AND id = ?
            """,
            (client_id, id),
        ).fetchone()

    def abort(self, id: str) -> None:
        """Discard an upload reservation and any partial files."""
        with self._connection:
            cursor = self._connection.execute(
                "UPDATE jobs SET cleanup_pending = 1 WHERE id = ? AND status = 'uploading'",
                (id,),
            )
        if cursor.rowcount == 1 and self._remove_job_dir(id):
            with self._connection:
                self._connection.execute("DELETE FROM jobs WHERE id = ?", (id,))

    def enqueue(self, id: str) -> None:
        with self._connection:
            self._connection.execute(
                """
                UPDATE jobs
                SET status = 'queued', error = NULL
                WHERE id = ? AND status = 'uploading'
                """,
                (id,),
            )

    def claim(self) -> sqlite3.Row | None:
        """Atomically claim the oldest queued job."""
        with self._connection:
            return self._connection.execute(
                f"""
                UPDATE jobs
                SET status = 'running', error = NULL
                WHERE id = (
                    SELECT id
                    FROM jobs
                    WHERE status = 'queued'
                    ORDER BY created_at, id
                    LIMIT 1
                )
                RETURNING {_PUBLIC_COLUMNS}
                """
            ).fetchone()

    def finish(self, id: str, error: str | None) -> None:
        status = "completed" if error is None else "failed"
        with self._connection:
            self._connection.execute(
                """
                UPDATE jobs
                SET status = ?, error = ?
                WHERE id = ? AND status = 'running'
                """,
                (status, error, id),
            )

    def acknowledge(self, id: str) -> None:
        """Persist acknowledgement before best-effort deletion of job files."""
        with self._connection:
            self._connection.execute(
                """
                UPDATE jobs
                SET status = 'acknowledged', error = NULL, cleanup_pending = 1
                WHERE id = ? AND status = 'completed'
                """,
                (id,),
            )
            row = self._connection.execute(
                """
                SELECT cleanup_pending
                FROM jobs
                WHERE id = ? AND status = 'acknowledged'
                """,
                (id,),
            ).fetchone()

        if row is not None and row["cleanup_pending"] and self._remove_job_dir(id):
            self._mark_clean(id)

    def cleanup(self) -> None:
        """Retry pending deletions; keep only acknowledged receipts."""
        pending_ids = [
            row["id"]
            for row in self._connection.execute(
                "SELECT id FROM jobs WHERE cleanup_pending = 1"
            )
        ]
        cleaned_ids = [job_id for job_id in pending_ids if self._remove_job_dir(job_id)]
        if not cleaned_ids:
            return

        with self._connection:
            self._connection.executemany(
                """
                UPDATE jobs
                SET cleanup_pending = 0
                WHERE id = ? AND status = 'acknowledged'
                """,
                ((job_id,) for job_id in cleaned_ids),
            )
            self._connection.executemany(
                "DELETE FROM jobs WHERE id = ? AND status = 'uploading'",
                ((job_id,) for job_id in cleaned_ids),
            )

    def job_dir(self, id: str) -> Path:
        return self._jobs_dir / id

    def input_path(self, row: sqlite3.Row) -> Path:
        suffix = Path(row["filename"]).suffix.lower()
        return self.job_dir(row["id"]) / f"input{suffix}"

    def _mark_clean(self, job_id: str) -> None:
        with self._connection:
            self._connection.execute(
                """
                UPDATE jobs
                SET cleanup_pending = 0
                WHERE id = ? AND status = 'acknowledged'
                """,
                (job_id,),
            )

    def _remove_job_dir(self, job_id: str) -> bool:
        try:
            shutil.rmtree(self.job_dir(job_id))
        except FileNotFoundError:
            return True
        except OSError:
            _LOGGER.warning("Could not remove files for job %s", job_id, exc_info=True)
            return False
        return True
