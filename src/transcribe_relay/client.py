"""Watch a directory and retrieve Buzz transcripts. Run with --config client.toml."""

import argparse
import errno
import fcntl
import hashlib
import logging
import os
import sqlite3
import stat
import sys
import time
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path

from .client_http import ApiError, NetworkError, OutputConflict, Relay
from .client_state import Journal
from .notifications import notify

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    server_url: str
    token: str = field(repr=False)
    watch_dir: Path
    state_dir: Path

    @classmethod
    def load(cls, path: Path):
        with path.open("rb") as stream:
            values = tomllib.load(stream)
        if values.keys() != {"server_url", "token", "watch_dir", "state_dir"}:
            raise ValueError(
                "expected server_url, token, watch_dir and state_dir in config"
            )
        if any(
            not isinstance(v, str) or not v.strip() or "\0" in v
            for v in values.values()
        ):
            raise ValueError("configuration values must be nonempty strings")
        if any(not 33 <= ord(c) <= 126 for c in values["token"]):
            raise ValueError(
                "token must contain printable ASCII characters without spaces"
            )
        for key in ("watch_dir", "state_dir"):
            values[key] = (path.parent / values[key]).resolve()
        if values["state_dir"].is_relative_to(values["watch_dir"]):
            raise ValueError("state_dir must be outside watch_dir")
        return cls(**values)


class Unauthorized(Exception):
    """The operator must correct the credentials before continuing."""


def matches(info: os.stat_result, row: sqlite3.Row) -> bool:
    return stat.S_ISREG(info.st_mode) and (info.st_size, info.st_mtime_ns) == (
        row["size"],
        row["mtime_ns"],
    )


def open_media(path: Path):
    # A regular file discovered on a scan may have been replaced by a FIFO or symlink.
    return os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb")


class Client:
    def __init__(self, journal: Journal, relay: Relay):
        self.journal = journal
        self.relay = relay

    def _upload(self, row: sqlite3.Row):
        path = Path(row["path"])
        key = row["key"]
        with open_media(path) as stream:
            if not matches(os.fstat(stream.fileno()), row):
                return None
            digest = row["digest"]
            if digest is None:
                hasher = hashlib.sha256()
                while chunk := stream.read(256 * 1024):
                    hasher.update(chunk)
                digest = hasher.hexdigest()
                stream.seek(0)
                if not matches(os.fstat(stream.fileno()), row):
                    return None
                self.journal.update(key, digest=digest)
            if not matches(path.lstat(), row):
                return None
            reply = self.relay.upload(path.name, stream, row["size"], key, digest)
            # Once accepted, the old version has its own server job even if the
            # pathname changed during upload. Never discard the returned UUID.
            if not matches(os.fstat(stream.fileno()), row):
                LOG.info("Source changed during upload: %s", path)
            return reply

    def process(self, now: float) -> None:
        started = time.monotonic()

        def next_poll() -> float:
            # A large upload can take longer than the polling interval.
            return now + (time.monotonic() - started) + 5

        for row in self.journal.due(now):
            key = row["key"]
            path = Path(row["path"])
            stage = row["stage"]
            try:
                if stage in ("upload", "poll"):
                    if stage == "upload":
                        reply = self._upload(row)
                        if reply is None:
                            self.journal.update(key, stage="obsolete")
                            continue
                    else:
                        reply = self.relay.status(row["job_id"])
                    state = reply["status"]
                    if state in ("queued", "running"):
                        next_stage = "poll"
                    elif state == "completed":
                        next_stage = "download"
                    elif state == "acknowledged":
                        next_stage = "done"
                    else:
                        next_stage = "failed"
                        LOG.error(
                            "Transcription failed for %s: %s", path, reply["error"]
                        )
                    self.journal.update(
                        key,
                        stage=next_stage,
                        job_id=reply["id"],
                        error=reply["error"],
                        next_at=next_poll() if next_stage == "poll" else 0,
                    )
                    if next_stage == "failed":
                        notify("Ошибка транскрибации", f"{path.name}: {reply['error']}")
                    elif stage == "upload" and next_stage != "done":
                        notify("Файл принят сервером", path.name)
                elif stage == "download":
                    self.relay.download(row["job_id"], Path(row["output"]))
                    self.journal.update(key, stage="saved", next_at=0)
                    notify("Расшифровка сохранена", Path(row["output"]).name)
                elif stage == "saved":
                    self.relay.ack(row["job_id"])
                    self.journal.update(key, stage="done")
            except ApiError as exc:
                if exc.status == 401:
                    raise Unauthorized(
                        "server rejected the client token (401)"
                    ) from exc
                if exc.status >= 500 or (
                    exc.status == 409 and exc.detail == "upload_in_progress"
                ):
                    LOG.warning("Retrying %s for %s: HTTP %d", stage, path, exc.status)
                    self.journal.update(key, next_at=next_poll())
                else:
                    LOG.error(
                        "%s failed for %s: HTTP %d %s",
                        stage,
                        path,
                        exc.status,
                        exc.detail,
                    )
                    self.journal.update(
                        key, stage="failed", error=f"HTTP {exc.status}: {exc.detail}"
                    )
                    notify("Ошибка обработки файла", f"{path.name}: HTTP {exc.status}")
            except OutputConflict:
                LOG.error("Existing transcript differs: %s", row["output"])
                self.journal.update(key, stage="failed", error="output_conflict")
                notify("Конфликт файла результата", Path(row["output"]).name)
            except (NetworkError, OSError) as exc:
                LOG.warning("%s for %s: %s", stage, path, exc)
                self.journal.update(key, next_at=next_poll())


def run(config: Settings) -> None:
    relay = Relay(config.server_url, config.token)
    if not config.watch_dir.is_dir():
        raise ValueError(f"watch_dir is not a directory: {config.watch_dir}")
    config.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (config.state_dir / "client.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise ValueError("another client is using state_dir") from exc
            raise
        with closing(Journal(config.state_dir, config.watch_dir)) as journal:
            client = Client(journal, relay)
            next_scan = 0.0
            while True:
                if time.monotonic() >= next_scan:
                    journal.scan(time.time())
                    next_scan = time.monotonic() + 2
                client.process(time.time())
                time.sleep(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        run(Settings.load(args.config))
    except KeyboardInterrupt:
        return
    except (OSError, ValueError, Unauthorized, sqlite3.Error) as exc:
        parser.exit(1, f"client: {exc}\n")


if __name__ == "__main__":
    main()
