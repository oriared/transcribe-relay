import hashlib
import http.client
import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import uvicorn

from transcribe_relay.config import ServerConfig
from transcribe_relay.server import create_app


class ServerTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        script = self.root / "cli.py"
        script.write_text(
            "import pathlib, sys\n"
            'out = pathlib.Path(sys.argv[sys.argv.index("--output-directory") + 1])\n'
            '(out / "transcript.txt").write_text("Текст", encoding="utf-8")\n'
        )
        settings = ServerConfig(
            host="127.0.0.1",
            port=8000,
            data_dir=self.root,
            clients={"a": "alpha", "b": "beta"},
            max_upload_bytes=64,
            buzz_command=[sys.executable, str(script)],
            buzz_args=[],
            buzz_timeout_seconds=5,
        )
        sock = socket.socket()
        self.addCleanup(sock.close)
        sock.bind(("127.0.0.1", 0))
        self.port = sock.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(create_app(settings), log_level="error"))
        thread = threading.Thread(
            target=server.run, kwargs={"sockets": [sock]}, daemon=True
        )
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(setattr, server, "should_exit", True)
        deadline = time.monotonic() + 5
        while not server.started:
            if not thread.is_alive() or time.monotonic() > deadline:
                self.fail("server startup failed")
            time.sleep(0.01)

    def request(self, method, path, body=None, headers=None, token="alpha"):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            conn.request(
                method,
                path,
                body,
                {"Authorization": f"Bearer {token}", **(headers or {})},
            )
            response = conn.getresponse()
            raw = response.read()
            if response.headers.get_content_type() == "application/json":
                raw = json.loads(raw)
            return response.status, raw
        finally:
            conn.close()

    def upload(self, key, body=b"media", name="звук.WAV", digest=None):
        return self.request(
            "POST",
            "/v1/jobs?filename=" + quote(name),
            body,
            {
                "Content-Type": "application/octet-stream",
                "Idempotency-Key": key,
                "X-Content-SHA256": digest or hashlib.sha256(body).hexdigest(),
            },
        )

    def test_download_and_ack_are_separate_idempotent_operations(self):
        key = str(uuid4())
        code, job = self.upload(key)
        self.assertEqual((code, job["status"], job["error"]), (202, "queued", None))
        path = "/v1/jobs/" + job["id"]
        self.assertEqual(self.request("GET", path, token="invalid")[0], 401)
        for method, suffix in [("GET", ""), ("GET", "/result"), ("POST", "/ack")]:
            self.assertEqual(self.request(method, path + suffix, token="beta")[0], 404)
        self.assertEqual(self.upload(key, b"other")[0], 409)
        deadline = time.monotonic() + 5
        while self.request("GET", path)[1]["status"] != "completed":
            if time.monotonic() >= deadline:
                self.fail(f"job did not complete: {self.request('GET', path)}")
            time.sleep(0.01)
        for _ in range(2):
            self.assertEqual(
                self.request("GET", path + "/result"), (200, "Текст".encode())
            )
        for _ in range(2):
            self.assertEqual(self.request("POST", path + "/ack"), (204, b""))
        self.assertEqual(self.request("GET", path + "/result")[0], 410)
        self.assertEqual(self.upload(key), (200, {**job, "status": "acknowledged"}))

    def test_rejected_upload_does_not_consume_key(self):
        key = str(uuid4())
        self.assertEqual(self.upload(key, b"")[0], 422)
        self.assertEqual(self.upload(key, b"a" * 65)[0], 413)
        self.assertEqual(self.upload(key, name="../escape.wav")[0], 422)
        self.assertEqual(self.upload(key, digest="0" * 64)[0], 422)
        self.assertEqual(self.upload(key)[0], 202)

    def test_concurrent_retry_and_interrupted_upload(self):
        key = str(uuid4())
        body = b"media"
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        self.addCleanup(conn.close)
        conn.putrequest("POST", "/v1/jobs?filename=input.wav")
        for name, value in {
            "Content-Length": str(len(body)),
            "Idempotency-Key": key,
            "Authorization": "Bearer alpha",
            "Content-Type": "application/octet-stream",
            "X-Content-SHA256": hashlib.sha256(body).hexdigest(),
        }.items():
            conn.putheader(name, value)
        conn.endheaders(body[:1])
        deadline = time.monotonic() + 5
        while not list(self.root.rglob("upload.part")):
            if time.monotonic() > deadline:
                self.fail("partial upload not received")
            time.sleep(0.01)
        self.assertEqual(
            self.upload(key, name="input.wav"), (409, {"detail": "upload_in_progress"})
        )
        conn.close()
        while True:
            status, _ = self.upload(key, name="input.wav")
            if status != 409:
                self.assertEqual(status, 202)
                break
            if time.monotonic() > deadline:
                self.fail("interrupted upload still reserved")
            time.sleep(0.01)
