"""Durable client-side file observations and operation journal."""

from __future__ import annotations

import logging
import os
import sqlite3
import stat
import uuid
from pathlib import Path

from .media import EXTENSIONS

_DUE_COLUMNS = "key, path, size, mtime_ns, digest, job_id, output, stage"
_ACTIVE_STAGES = ("upload", "poll", "download", "saved")
_STABILITY_SECONDS = 10.0


class Journal:
    """Track stable file versions and durable client operations in SQLite."""

    def __init__(self, state_dir: Path, watch_dir: Path):
        state_dir.mkdir(parents=True, exist_ok=True)
        self._watch_dir = Path(os.path.abspath(watch_dir))
        self._connection = sqlite3.connect(state_dir / "client.sqlite3")
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    name TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS observations (
                    path TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    stable_since REAL NOT NULL,
                    emitted INTEGER NOT NULL CHECK (emitted IN (0, 1))
                );

                CREATE TABLE IF NOT EXISTS operations (
                    key TEXT PRIMARY KEY,
                    path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    digest TEXT,
                    job_id TEXT,
                    output TEXT NOT NULL,
                    stage TEXT NOT NULL CHECK (
                        stage IN (
                            'upload', 'poll', 'download', 'saved',
                            'failed', 'done', 'obsolete'
                        )
                    ),
                    error TEXT,
                    next_at REAL
                );

                CREATE INDEX IF NOT EXISTS operations_due
                    ON operations (stage, next_at);
                """
            )
            self._bind_watch_dir()
        except BaseException:
            self._connection.close()
            raise

    def _bind_watch_dir(self) -> None:
        bound = self._connection.execute(
            "SELECT value FROM metadata WHERE name = 'watch_dir'"
        ).fetchone()
        watch_dir = os.fspath(self._watch_dir)
        if bound is not None:
            if bound["value"] != watch_dir:
                raise ValueError(
                    f"journal is bound to {bound['value']!r}, not {watch_dir!r}"
                )
            return
        with self._connection:
            self._connection.execute(
                "INSERT INTO metadata (name, value) VALUES ('watch_dir', ?)",
                (watch_dir,),
            )

    def close(self) -> None:
        self._connection.close()

    def _snapshot(self) -> dict[str, tuple[int, int]] | None:
        try:
            snapshot = {}
            with os.scandir(self._watch_dir) as entries:
                for entry in entries:
                    if Path(entry.name).suffix.lower() not in EXTENSIONS:
                        continue
                    try:
                        info = entry.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if stat.S_ISREG(info.st_mode):
                        snapshot[entry.path] = (info.st_size, info.st_mtime_ns)
            return snapshot
        except OSError as exc:
            logging.getLogger(__name__).warning(
                "Cannot scan %s: %s", self._watch_dir, exc
            )
            return None

    def scan(self, now: float) -> None:
        """Observe one complete directory snapshot and enqueue stable versions."""
        snapshot = self._snapshot()
        if snapshot is None:
            return

        initialized = self._connection.execute(
            "SELECT 1 FROM metadata WHERE name = 'initialized'"
        ).fetchone()
        if initialized is None:
            with self._connection:
                self._connection.executemany(
                    """
                    INSERT INTO observations (
                        path, size, mtime_ns, stable_since, emitted
                    ) VALUES (?, ?, ?, ?, 1)
                    """,
                    (
                        (path, size, mtime_ns, now)
                        for path, (size, mtime_ns) in sorted(snapshot.items())
                    ),
                )
                self._connection.execute(
                    "INSERT INTO metadata (name, value) VALUES ('initialized', '1')"
                )
            return

        with self._connection:
            rows = self._connection.execute(
                "SELECT path, size, mtime_ns, stable_since, emitted FROM observations"
            ).fetchall()
            observations = {row["path"]: row for row in rows}

            missing = observations.keys() - snapshot.keys()
            if missing:
                self._connection.executemany(
                    "DELETE FROM observations WHERE path = ?",
                    ((path,) for path in missing),
                )
                self._obsolete_uploads(missing)

            for path, (size, mtime_ns) in sorted(snapshot.items()):
                previous = observations.get(path)
                if previous is None:
                    self._connection.execute(
                        """
                        INSERT INTO observations (
                            path, size, mtime_ns, stable_since, emitted
                        ) VALUES (?, ?, ?, ?, 0)
                        """,
                        (path, size, mtime_ns, now),
                    )
                    continue

                if previous["size"] != size or previous["mtime_ns"] != mtime_ns:
                    self._connection.execute(
                        """
                        UPDATE observations
                        SET size = ?, mtime_ns = ?, stable_since = ?, emitted = 0
                        WHERE path = ?
                        """,
                        (size, mtime_ns, now, path),
                    )
                    self._obsolete_uploads((path,))
                    continue

                if (
                    size
                    and not previous["emitted"]
                    and now - previous["stable_since"] >= _STABILITY_SECONDS
                ):
                    key = str(uuid.uuid4())
                    output = os.path.abspath(f"{path}.txt")
                    self._connection.execute(
                        """
                        INSERT INTO operations (
                            key, path, size, mtime_ns, digest, job_id, output,
                            stage, error, next_at
                        ) VALUES (?, ?, ?, ?, NULL, NULL, ?, 'upload', NULL, ?)
                        """,
                        (key, path, size, mtime_ns, output, now),
                    )
                    self._connection.execute(
                        "UPDATE observations SET emitted = 1 WHERE path = ?",
                        (path,),
                    )

    def _obsolete_uploads(self, paths) -> None:
        self._connection.executemany(
            """
            UPDATE operations
            SET stage = 'obsolete', next_at = NULL
            WHERE path = ? AND stage = 'upload' AND job_id IS NULL
            """,
            ((path,) for path in paths),
        )

    def due(self, now: float) -> list[sqlite3.Row]:
        """Return active operations whose next attempt is due."""
        placeholders = ", ".join("?" for _ in _ACTIVE_STAGES)
        return self._connection.execute(
            f"""
            SELECT {_DUE_COLUMNS}
            FROM operations
            WHERE stage IN ({placeholders})
              AND (next_at IS NULL OR next_at <= ?)
            ORDER BY next_at, rowid
            """,
            (*_ACTIVE_STAGES, now),
        ).fetchall()

    def update(
        self,
        key: str,
        *,
        stage: str | None = None,
        digest: str | None = None,
        job_id: str | None = None,
        error: str | None = None,
        next_at: float | None = None,
    ) -> None:
        """Durably update supplied operation fields; ``None`` means unchanged."""
        assignments = []
        values: list[str | float] = []
        for name, value in (
            ("stage", stage),
            ("digest", digest),
            ("job_id", job_id),
            ("error", error),
            ("next_at", next_at),
        ):
            if value is not None:
                assignments.append(f"{name} = ?")
                values.append(value)
        if not assignments:
            return
        values.append(key)
        with self._connection:
            self._connection.execute(
                f"UPDATE operations SET {', '.join(assignments)} WHERE key = ?",
                values,
            )
