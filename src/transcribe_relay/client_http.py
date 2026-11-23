"""Standard-library HTTP transport for the relay client."""

from __future__ import annotations

import http.client
import json
import os
import secrets
import stat
from pathlib import Path
from typing import BinaryIO
from urllib.parse import quote, urlsplit
from uuid import UUID


class ApiError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail


class NetworkError(Exception):
    """The server response could not be obtained or was not valid HTTP."""


class OutputConflict(Exception):
    """The result path already contains different or unsafe content."""


class _BoundedReader:
    def __init__(self, source: BinaryIO, remaining: int):
        self.source = source
        self.remaining = remaining
        self.local_error: OSError | None = None

    def read(self, amount: int = -1) -> bytes:
        if self.remaining == 0:
            return b""
        if amount < 0 or amount > self.remaining:
            amount = self.remaining
        try:
            data = self.source.read(amount)
            if not data:
                raise OSError("source truncated during upload")
        except OSError as exc:
            self.local_error = exc
            raise
        if not isinstance(data, bytes):
            raise TypeError("binary upload stream returned non-bytes data")
        self.remaining -= len(data)
        return data


class Relay:
    def __init__(self, server_url: str, token: str):
        url = urlsplit(server_url)
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "server_url must be an HTTP(S) URL without credentials, query, or fragment"
            )
        try:
            port = url.port
        except ValueError as exc:
            raise ValueError("invalid server URL port") from exc
        self._https = url.scheme == "https"
        self._host = url.hostname
        self._port = port
        path = url.path.rstrip("/")
        self._base = path if path else ""
        self._token = token

    def _connection(self) -> http.client.HTTPConnection:
        cls = http.client.HTTPSConnection if self._https else http.client.HTTPConnection
        return cls(self._host, self._port, timeout=30)

    def _request(self, method: str, path: str, *, body=None, headers=None):
        conn = self._connection()
        merged = {"Authorization": f"Bearer {self._token}"}
        if headers:
            merged.update(headers)
        try:
            conn.request(method, self._base + path, body=body, headers=merged)
            return conn, conn.getresponse()
        except (OSError, http.client.HTTPException) as exc:
            conn.close()
            if isinstance(body, _BoundedReader) and body.local_error is exc:
                raise
            raise NetworkError(str(exc)) from exc

    @staticmethod
    def _error(response: http.client.HTTPResponse) -> None:
        if response.status < 400:
            return
        try:
            raw = response.read(8192)
            value = json.loads(raw)
            detail = value.get("detail") if isinstance(value, dict) else None
            if not isinstance(detail, str):
                detail = raw.decode("utf-8", "replace") or response.reason
        except (OSError, http.client.HTTPException) as exc:
            raise NetworkError(str(exc)) from exc
        except (UnicodeDecodeError, json.JSONDecodeError):
            detail = "invalid error response"
        raise ApiError(response.status, detail)

    @staticmethod
    def _job(response: http.client.HTTPResponse) -> dict:
        Relay._error(response)
        if response.status not in (200, 202):
            raise NetworkError("unexpected job HTTP status")
        try:
            raw = response.read(8192)
            value = json.loads(raw)
        except (
            OSError,
            http.client.HTTPException,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:
            raise NetworkError("invalid JSON response") from exc
        statuses = ("queued", "running", "completed", "acknowledged", "failed")
        job_id = value.get("id") if isinstance(value, dict) else None
        if not isinstance(job_id, str) or value.get("status") not in statuses:
            raise NetworkError("invalid job response")
        if "error" not in value or (
            value["error"] is not None and not isinstance(value["error"], str)
        ):
            raise NetworkError("invalid job error")
        try:
            UUID(job_id)
        except ValueError as exc:
            raise NetworkError("invalid job response") from exc
        return value

    def upload(
        self, name: str, file: BinaryIO, size: int, key: str, digest: str
    ) -> dict:
        body = _BoundedReader(file, size)
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(size),
            "Idempotency-Key": key,
            "X-Content-SHA256": digest,
        }
        conn, response = self._request(
            "POST",
            "/v1/jobs?filename=" + quote(name, safe=""),
            body=body,
            headers=headers,
        )
        try:
            return self._job(response)
        finally:
            conn.close()

    def status(self, job_id: str) -> dict:
        conn, response = self._request("GET", "/v1/jobs/" + quote(job_id, safe=""))
        try:
            job = self._job(response)
            if job["id"] != job_id:
                raise NetworkError("status returned for a different job")
            return job
        finally:
            conn.close()

    def ack(self, job_id: str) -> None:
        conn, response = self._request(
            "POST",
            "/v1/jobs/" + quote(job_id, safe="") + "/ack",
            body=b"",
            headers={"Content-Length": "0"},
        )
        try:
            self._error(response)
            try:
                response.read()
            except (OSError, http.client.HTTPException) as exc:
                raise NetworkError(str(exc)) from exc
            if response.status != 204:
                raise NetworkError("invalid acknowledgement response")
        finally:
            conn.close()

    def download(self, job_id: str, output: Path) -> None:
        conn, response = self._request(
            "GET", "/v1/jobs/" + quote(job_id, safe="") + "/result"
        )
        temp = output.parent / f".relay-{secrets.token_hex(12)}.tmp"
        try:
            self._error(response)
            if (
                response.status != 200
                or response.headers.get_content_type() != "text/plain"
            ):
                raise NetworkError("unexpected result response")
            length_text = response.getheader("Content-Length")
            transfer = response.getheader("Transfer-Encoding")
            try:
                length = int(length_text) if length_text is not None else -1
            except ValueError as exc:
                raise NetworkError("invalid result Content-Length") from exc
            if length < 0 or transfer is not None:
                raise NetworkError("invalid result framing")
            received = 0
            target = os.fdopen(
                os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb"
            )
            try:
                with target:
                    while received < length:
                        try:
                            chunk = response.read(min(65536, length - received))
                        except (OSError, http.client.HTTPException) as exc:
                            raise NetworkError(str(exc)) from exc
                        if not chunk:
                            raise NetworkError("truncated result")
                        target.write(chunk)
                        received += len(chunk)
                    target.flush()
                    os.fsync(target.fileno())
                self._publish(temp, output)
            finally:
                try:
                    temp.unlink()
                except FileNotFoundError:
                    pass
        finally:
            conn.close()

    @staticmethod
    def _publish(temp: Path, output: Path) -> None:
        try:
            os.link(temp, output, follow_symlinks=False)
        except FileExistsError:
            try:
                fd = os.open(output, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            except OSError as exc:
                raise OutputConflict(str(output)) from exc
            try:
                with os.fdopen(fd, "rb") as current, temp.open("rb") as candidate:
                    if not stat.S_ISREG(os.fstat(fd).st_mode):
                        raise OutputConflict(str(output))
                    while True:
                        left = current.read(65536)
                        right = candidate.read(65536)
                        if left != right:
                            raise OutputConflict(str(output))
                        if not left:
                            os.fsync(fd)
                            break
            except OutputConflict:
                raise
            except OSError as exc:
                raise OutputConflict(str(output)) from exc
        fd = os.open(output.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
