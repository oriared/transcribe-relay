import hashlib
import http.server
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from transcribe_relay.client_http import NetworkError, OutputConflict, Relay


class _Handler(http.server.BaseHTTPRequestHandler):
    result = b"transcript\n"
    advertised = None
    uploaded = b""

    def log_message(self, format, *args):
        pass

    def do_POST(self):
        if self.path.endswith("/ack"):
            self.send_response(204)
            self.end_headers()
            return
        size = int(self.headers["Content-Length"])
        type(self).uploaded = self.rfile.read(size)
        body = b'{"id":"00000000-0000-0000-0000-000000000001","status":"queued","error":null}'
        self.send_response(202)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.endswith("/result"):
            body = type(self).result
            length = type(self).advertised
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header(
                "Content-Length", str(len(body) if length is None else length)
            )
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
            if length is not None and length > len(body):
                self.connection.shutdown(socket.SHUT_RDWR)
            return
        body = b'{"id":"00000000-0000-0000-0000-000000000001","status":"completed","error":null}'
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ClientHttpTests(unittest.TestCase):
    def setUp(self):
        _Handler.result = b"transcript\n"
        _Handler.advertised = None
        _Handler.uploaded = b""
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.addCleanup(self.server.server_close)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.server.shutdown)
        self.relay = Relay(f"http://127.0.0.1:{self.server.server_port}", "secret")
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def test_url_validation(self):
        for url in (
            "ftp://host",
            "http://user@host",
            "http://host?a=b",
            "http://host/#x",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                Relay(url, "token")

    def test_upload_is_bounded_to_declared_size(self):
        import io

        body = b"declared-extra"
        result = self.relay.upload(
            "sound.wav",
            io.BytesIO(body),
            8,
            "key",
            hashlib.sha256(body[:8]).hexdigest(),
        )
        self.assertEqual(result["status"], "queued")
        self.assertEqual(_Handler.uploaded, body[:8])

    def test_identical_existing_result_is_accepted(self):
        output = self.root / "sound.wav.txt"
        output.write_bytes(_Handler.result)
        self.relay.download("job-1", output)
        self.assertEqual(output.read_bytes(), _Handler.result)
        self.assertEqual(list(self.root.iterdir()), [output])

    def test_different_existing_result_conflicts_without_replacement(self):
        output = self.root / "sound.wav.txt"
        output.write_bytes(b"mine")
        with self.assertRaises(OutputConflict):
            self.relay.download("job-1", output)
        self.assertEqual(output.read_bytes(), b"mine")
        self.assertEqual(list(self.root.iterdir()), [output])

    def test_symlink_result_conflicts(self):
        actual = self.root / "actual"
        actual.write_bytes(_Handler.result)
        output = self.root / "sound.wav.txt"
        output.symlink_to(actual)
        with self.assertRaises(OutputConflict):
            self.relay.download("job-1", output)
        self.assertTrue(output.is_symlink())
        self.assertEqual(actual.read_bytes(), _Handler.result)

    def test_truncated_stream_is_network_error_and_leaves_no_temp(self):
        _Handler.advertised = len(_Handler.result) + 10
        output = self.root / "sound.wav.txt"
        with self.assertRaises(NetworkError):
            self.relay.download("job-1", output)
        self.assertEqual(list(self.root.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
