"""Sequential server-side transcription worker."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from .buzz import run_buzz
from .notifications import notify as _notify

if TYPE_CHECKING:
    from .storage import Store

_IDLE_CLEANUP_SECONDS = 5.0


async def run_worker(
    store: Store,
    command: list[str],
    args: list[str],
    timeout: int,
    wake: asyncio.Event,
) -> None:
    """Process queued jobs one at a time until this task is cancelled.

    Startup recovery is deliberately owned by ``Store.recover`` and must run
    before this coroutine is started.
    """
    while True:
        # Clearing before querying durable state avoids losing a notification:
        # an enqueue racing with this section is either claimed below or leaves
        # the event set for the subsequent idle wait.
        wake.clear()
        store.cleanup()
        row = store.claim()
        if row is None:
            try:
                await asyncio.wait_for(wake.wait(), _IDLE_CLEANUP_SECONDS)
            except TimeoutError:
                pass
            continue

        job_id = row["id"]
        filename = row["filename"]
        await asyncio.to_thread(_notify, "Начата транскрибация", filename)
        error = await run_buzz(
            store.job_dir(job_id),
            store.input_path(row),
            command,
            args,
            timeout,
        )
        store.finish(job_id, error)
        if error is None:
            await asyncio.to_thread(_notify, "Транскрибация завершена", filename)
        else:
            await asyncio.to_thread(
                _notify, "Ошибка транскрибации", f"{filename}: {error}"
            )
