"""HTTP service for the transcription queue."""

import argparse
import asyncio
import errno
import fcntl
import hashlib
import hmac
import logging
import os
import sqlite3
import sys
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Annotated
from uuid import UUID

if sys.version_info < (3, 12):
    raise SystemExit("Transcribe Relay server requires Python 3.12 or newer")
else:
    import uvicorn
    from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    from pydantic import BaseModel
    from starlette.requests import ClientDisconnect
    from starlette.responses import StreamingResponse

    from .config import ServerConfig, load_config
    from .media import EXTENSIONS
    from .storage import Store
    from .worker import run_worker


class JobStatus(BaseModel):
    id: UUID
    status: str
    error: str | None


def create_app(config: ServerConfig) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        config.data_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        with (config.data_dir / "server.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            store = Store(config.data_dir)
            try:
                store.recover()
                app.state.store = store
                wake = app.state.wake = asyncio.Event()
                worker = asyncio.create_task(
                    run_worker(
                        store,
                        config.buzz_command,
                        config.buzz_args,
                        config.buzz_timeout_seconds,
                        wake,
                    )
                )
                try:
                    yield
                finally:
                    worker.cancel()
                    with suppress(asyncio.CancelledError):
                        await worker
            finally:
                store.close()

    app = FastAPI(title="Transcribe Relay", lifespan=lifespan)
    bearer = HTTPBearer(auto_error=False)

    async def authenticate(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ):
        if credentials is not None:
            supplied = credentials.credentials.encode()
            for client, secret in config.clients.items():
                if hmac.compare_digest(supplied, secret.get_secret_value().encode()):
                    return client
        raise HTTPException(401, "unauthorized", headers={"WWW-Authenticate": "Bearer"})

    Client = Annotated[str, Depends(authenticate)]

    def lookup(client: str, id: UUID):
        row = app.state.store.get(client, str(id))
        if row is None or row["status"] == "uploading":
            raise HTTPException(404, "job_not_found")
        return row

    @app.post(
        "/v1/jobs",
        response_model=JobStatus,
        status_code=202,
        responses={200: {"model": JobStatus}},
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "application/octet-stream": {
                        "schema": {"type": "string", "format": "binary"}
                    }
                },
            }
        },
    )
    async def upload(
        request: Request,
        response: Response,
        client: Client,
        filename: str,
        idempotency_key: Annotated[UUID, Header()],
        x_content_sha256: Annotated[str, Header(pattern=r"^[0-9a-fA-F]{64}$")],
        content_length: Annotated[int | None, Header()] = None,
    ):
        if (
            request.headers.get("content-type", "").split(";")[0].lower()
            != "application/octet-stream"
        ):
            raise HTTPException(415, "invalid_content_type")
        if content_length is None:
            raise HTTPException(411, "length_required")
        if content_length <= 0:
            raise HTTPException(422, "invalid_length")
        if content_length > config.max_upload_bytes:
            raise HTTPException(413, "upload_too_large")
        if (
            any(c in filename for c in ("/", "\\", "\0"))
            or Path(filename).suffix.lower() not in EXTENSIONS
        ):
            raise HTTPException(422, "invalid_filename")
        digest = x_content_sha256.lower()
        store: Store = app.state.store
        try:
            row, created = store.reserve(
                client, str(idempotency_key), filename, content_length, digest
            )
            if not created:
                if (row["filename"], row["size"], row["sha256"]) != (
                    filename,
                    content_length,
                    digest,
                ):
                    raise HTTPException(409, "idempotency_conflict")
                if row["status"] == "uploading":
                    raise HTTPException(409, "upload_in_progress")
                response.status_code = 200
                return dict(row)
            job_id = row["id"]
            queued = False
            try:
                folder = store.job_dir(job_id)
                folder.mkdir(mode=0o700)
                partial = folder / "upload.part"
                hashed = hashlib.sha256()
                count = 0
                with partial.open("xb") as target:
                    async for chunk in request.stream():
                        count += len(chunk)
                        if count > config.max_upload_bytes:
                            raise HTTPException(413, "upload_too_large")
                        if count > content_length:
                            raise HTTPException(422, "length_mismatch")
                        target.write(chunk)
                        hashed.update(chunk)
                    if count != content_length or hashed.hexdigest() != digest:
                        raise HTTPException(422, "invalid_payload")
                    target.flush()
                    os.fsync(target.fileno())
                partial.replace(store.input_path(row))
                sync_dir(folder)
                sync_dir(folder.parent)
                store.enqueue(job_id)
                queued = True
            finally:
                if not queued:
                    store.abort(job_id)
        except ClientDisconnect as exc:
            raise HTTPException(422, "incomplete_upload") from exc
        except (OSError, sqlite3.OperationalError) as exc:
            if (
                getattr(exc, "errno", None) in (errno.ENOSPC, errno.EDQUOT)
                or getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL
            ):
                raise HTTPException(507, "storage_full") from exc
            raise
        app.state.wake.set()
        return JobStatus(id=job_id, status="queued", error=None)

    @app.get("/v1/jobs/{id}", response_model=JobStatus)
    async def status(id: UUID, client: Client):
        return dict(lookup(client, id))

    @app.get(
        "/v1/jobs/{id}/result",
        response_class=StreamingResponse,
        responses={200: {"content": {"text/plain": {"schema": {"type": "string"}}}}},
    )
    async def result(id: UUID, client: Client):
        state = lookup(client, id)["status"]
        if state == "acknowledged":
            raise HTTPException(410, "result_removed")
        if state != "completed":
            raise HTTPException(409, "result_not_ready")
        # Open before yielding: concurrent ack may unlink the path, not this fd.
        file = (app.state.store.job_dir(str(id)) / "result.txt").open("rb")

        def chunks():
            with file:
                while chunk := file.read(65536):
                    yield chunk

        return StreamingResponse(
            chunks(),
            media_type="text/plain; charset=utf-8",
            headers={"Content-Length": str(os.fstat(file.fileno()).st_size)},
        )

    @app.post("/v1/jobs/{id}/ack", status_code=204)
    async def acknowledge(id: UUID, client: Client):
        if lookup(client, id)["status"] not in ("completed", "acknowledged"):
            raise HTTPException(409, "result_not_ready")
        app.state.store.acknowledge(str(id))
        return Response(status_code=204)

    return app


def sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    try:
        settings = load_config()
    except ValueError as exc:
        parser.error(str(exc))
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
