"""Run the Buzz CLI and durably publish its text output."""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
from pathlib import Path
from typing import BinaryIO

_TERMINATE_GRACE_SECONDS = 5.0


def _prepare_output(job_dir: Path) -> tuple[Path, BinaryIO]:
    output_dir = job_dir / "buzz-output"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir()
    return output_dir, (job_dir / "buzz.log").open("wb")


async def _stop_process_group(process: asyncio.subprocess.Process) -> None:
    """Terminate the subprocess session, escalating if it does not stop."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    try:
        await asyncio.wait_for(process.wait(), _TERMINATE_GRACE_SECONDS)
    except TimeoutError:
        pass

    # The direct child may exit while one of its descendants ignores SIGTERM.
    # Probe the process group rather than treating the child's exit as cleanup.
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return

    if process.returncode is None:
        try:
            await asyncio.wait_for(process.wait(), _TERMINATE_GRACE_SECONDS)
        except TimeoutError:
            # SIGKILL cannot be handled by the child. Avoid holding server
            # shutdown if the event loop's child watcher itself is unhealthy.
            return


def _publish_result(output_dir: Path, job_dir: Path) -> bool:
    try:
        candidates = [
            entry
            for entry in output_dir.iterdir()
            if entry.suffix == ".txt" and not entry.is_symlink() and entry.is_file()
        ]
        if len(candidates) != 1:
            return False

        source = candidates[0]
        with source.open("r", encoding="utf-8", errors="strict") as text_file:
            while text_file.read(1024 * 1024):
                pass
        with source.open("rb") as result_file:
            os.fsync(result_file.fileno())
        os.replace(source, job_dir / "result.txt")
        directory_fd = os.open(job_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except (OSError, UnicodeError):
        return False
    return True


async def run_buzz(
    job_dir: Path,
    input_path: Path,
    command: list[str],
    args: list[str],
    timeout: int,
) -> str | None:
    """Run one Buzz transcription and return its stable error code, if any."""
    try:
        output_dir, log_file = _prepare_output(job_dir)
    except OSError:
        return "buzz_start"

    process: asyncio.subprocess.Process | None = None
    try:
        argv = [
            *command,
            "add",
            "--task",
            "transcribe",
            "--txt",
            "--hide-gui",
            "--output-directory",
            str(output_dir),
            *args,
            str(input_path),
        ]
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=log_file,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        except (OSError, ValueError):
            return "buzz_start"

        try:
            try:
                return_code = await asyncio.wait_for(process.wait(), timeout)
            except TimeoutError:
                await _stop_process_group(process)
                return "buzz_timeout"
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(_stop_process_group(process))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                # Cleanup remains scheduled even if shutdown cancellation repeats.
                pass
            raise

        if return_code != 0:
            return "buzz_exit"
        if not _publish_result(output_dir, job_dir):
            return "buzz_output"
        return None
    finally:
        log_file.close()
