"""Tests for Buzz process orchestration and the sequential worker."""

from __future__ import annotations

import asyncio
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from transcribe_relay.buzz import run_buzz
from transcribe_relay.worker import run_worker

_FAKE_BUZZ = r"""
import os
import subprocess
import sys
import time
from pathlib import Path


def option(name, default=None):
    try:
        return sys.argv[sys.argv.index(name) + 1]
    except ValueError:
        return default


assert option("--task") == "transcribe" and option("--language") == "ru"
output = Path(option("--output-directory"))
mode = option("--fake-mode", "empty")
marker_value = option("--marker")
marker = Path(marker_value) if marker_value else None
record_value = option("--record")
record = Path(record_value) if record_value else None

if record:
    with record.open("a", encoding="utf-8") as stream:
        stream.write("start\n")

if mode == "exit":
    raise SystemExit(7)
if mode == "no-output":
    pass
elif mode == "invalid":
    (output / "bad.txt").write_bytes(b"\xff")
elif mode == "ambiguous":
    (output / "one.txt").write_text("one", encoding="utf-8")
    (output / "two.txt").write_text("two", encoding="utf-8")
elif mode == "early":
    (output / "early.txt").write_text("not final yet", encoding="utf-8")
    marker.write_text("ready", encoding="utf-8")
    time.sleep(30)
elif mode == "hang-child":
    code = (
        "import time; from pathlib import Path; "
        f"time.sleep(2); Path({str(marker)!r}).write_text('escaped')"
    )
    subprocess.Popen([sys.executable, "-c", code])
    Path(str(marker) + ".ready").write_text("ready", encoding="utf-8")
    time.sleep(30)
else:
    (output / "result.txt").write_bytes(b"" if mode == "empty" else b"text")
    if mode == "record":
        time.sleep(0.1)

if record:
    with record.open("a", encoding="utf-8") as stream:
        stream.write("end\n")
"""


async def _wait_for_path(path: Path, seconds: float = 3.0) -> None:
    async with asyncio.timeout(seconds):
        while not path.exists():
            await asyncio.sleep(0.01)


class FakeStore:
    def __init__(self, root: Path, count: int = 1):
        self.root = root
        root.mkdir()
        self.rows = []
        self.finished = asyncio.Event()
        self.cleanup_calls = 0
        for number in range(count):
            job_id = str(number)
            job_dir = root / job_id
            job_dir.mkdir()
            input_path = job_dir / "input.wav"
            input_path.write_bytes(b"audio")
            self.rows.append(
                {
                    "id": job_id,
                    "filename": "запись.wav",
                    "status": "queued",
                    "input": input_path,
                    "error": None,
                }
            )

    def cleanup(self):
        self.cleanup_calls += 1

    def claim(self):
        for row in self.rows:
            if row["status"] == "queued":
                row["status"] = "running"
                return row
        return None

    def finish(self, job_id, error):
        row = self.rows[int(job_id)]
        row["status"] = "failed" if error else "completed"
        row["error"] = error
        if all(item["status"] in {"completed", "failed"} for item in self.rows):
            self.finished.set()

    def job_dir(self, job_id):
        return self.root / job_id

    def input_path(self, row):
        return row["input"]


class BuzzTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.script = self.root / "fake_buzz.py"
        self.script.write_text(textwrap.dedent(_FAKE_BUZZ), encoding="utf-8")

    def tearDown(self):
        self.temporary.cleanup()

    def make_job(self):
        job_dir = self.root / "job"
        job_dir.mkdir()
        input_path = job_dir / "input.wav"
        input_path.write_bytes(b"audio")
        return job_dir, input_path

    async def invoke(self, mode, timeout=3):
        job_dir, input_path = self.make_job()
        error = await run_buzz(
            job_dir,
            input_path,
            [sys.executable, str(self.script)],
            ["--fake-mode", mode],
            timeout,
        )
        return error, job_dir

    async def test_empty_utf8_output_is_durably_published(self):
        error, job_dir = await self.invoke("empty")
        self.assertIsNone(error)
        self.assertEqual((job_dir / "result.txt").read_bytes(), b"")

    async def test_success_requires_exactly_one_valid_utf8_txt(self):
        for mode in ("no-output", "invalid", "ambiguous"):
            with self.subTest(mode=mode):
                error, _ = await self.invoke(mode)
                self.assertEqual(error, "buzz_output")
                (self.root / "job").rename(self.root / f"used-{mode}")

    async def test_nonzero_exit_has_stable_error(self):
        error, _ = await self.invoke("exit")
        self.assertEqual(error, "buzz_exit")

    async def test_start_failure_has_stable_error(self):
        job_dir, input_path = self.make_job()
        error = await run_buzz(job_dir, input_path, [str(self.root / "missing")], [], 1)
        self.assertEqual(error, "buzz_start")

    async def test_timeout_terminates_descendant_processes(self):
        marker = self.root / "escaped"
        job_dir, input_path = self.make_job()
        error = await run_buzz(
            job_dir,
            input_path,
            [sys.executable, str(self.script)],
            ["--fake-mode", "hang-child", "--marker", str(marker)],
            1,
        )
        self.assertEqual(error, "buzz_timeout")
        await asyncio.sleep(1.2)
        self.assertFalse(marker.exists())

    async def test_cancellation_terminates_descendants_and_is_bounded(self):
        marker = self.root / "cancelled-child"
        ready = Path(str(marker) + ".ready")
        job_dir, input_path = self.make_job()
        task = asyncio.create_task(
            run_buzz(
                job_dir,
                input_path,
                [sys.executable, str(self.script)],
                ["--fake-mode", "hang-child", "--marker", str(marker)],
                10,
            )
        )
        await _wait_for_path(ready)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 6)
        await asyncio.sleep(2.1)
        self.assertFalse(marker.exists())


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from unittest.mock import patch

        self.notify = self.enterContext(patch("transcribe_relay.worker._notify"))
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.script = self.root / "fake_buzz.py"
        self.script.write_text(textwrap.dedent(_FAKE_BUZZ), encoding="utf-8")

    def tearDown(self):
        self.temporary.cleanup()

    async def test_output_is_not_published_before_process_exits(self):
        marker = self.root / "early"
        store = FakeStore(self.root / "jobs")
        task = asyncio.create_task(
            run_worker(
                store,
                [sys.executable, str(self.script)],
                ["--fake-mode", "early", "--marker", str(marker)],
                60,
                asyncio.Event(),
            )
        )
        await _wait_for_path(marker)
        self.assertEqual(store.rows[0]["status"], "running")
        self.assertFalse((store.job_dir("0") / "result.txt").exists())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(store.rows[0]["status"], "running")

    async def test_jobs_run_sequentially(self):
        record = self.root / "record"
        store = FakeStore(self.root / "jobs", count=2)
        task = asyncio.create_task(
            run_worker(
                store,
                [sys.executable, str(self.script)],
                ["--fake-mode", "record", "--record", str(record)],
                3,
                asyncio.Event(),
            )
        )
        await asyncio.wait_for(store.finished.wait(), 5)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(
            record.read_text(encoding="utf-8").splitlines(),
            [
                "start",
                "end",
                "start",
                "end",
            ],
        )
        self.assertEqual(
            [row["status"] for row in store.rows],
            [
                "completed",
                "completed",
            ],
        )
        self.assertGreaterEqual(store.cleanup_calls, 2)
        self.assertEqual(
            [call.args for call in self.notify.call_args_list[:3]],
            [
                ("Начата транскрибация", "запись.wav"),
                ("Транскрибация завершена", "запись.wav"),
                ("Начата транскрибация", "запись.wav"),
            ],
        )
