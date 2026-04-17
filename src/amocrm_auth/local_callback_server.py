from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


@dataclass(frozen=True)
class CallbackResult:
    code: str
    state: str
    referer: str


class _CallbackState:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.payload: CallbackResult | None = None


class _CallbackHandler(BaseHTTPRequestHandler):
    state_ref: _CallbackState
    expected_path: str
    logger: logging.Logger

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != self.expected_path:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")
            return

        qs = parse_qs(parsed.query)
        code = str((qs.get("code") or [""])[0]).strip()
        state = str((qs.get("state") or [""])[0]).strip()
        referer = str((qs.get("referer") or [""])[0]).strip()

        self.state_ref.payload = CallbackResult(code=code, state=state, referer=referer)
        self.state_ref.event.set()

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(
            """
<!doctype html>
<html><body>
<h3>amoCRM authorization callback received.</h3>
<p>You can close this window and return to the terminal.</p>
</body></html>
            """.encode("utf-8")
        )

    def log_message(self, fmt: str, *args: Any) -> None:  # silence default
        self.logger.debug("callback-server: " + fmt, *args)


class LocalCallbackServer:
    def __init__(self, *, host: str, port: int, path: str, logger: logging.Logger) -> None:
        self.host = host
        self.port = port
        self.path = path if path.startswith("/") else "/" + path
        self.logger = logger
        self._state = _CallbackState()
        self._httpd: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._httpd is not None:
            return

        state_ref = self._state
        expected_path = self.path
        logger = self.logger

        class Handler(_CallbackHandler):
            pass

        Handler.state_ref = state_ref
        Handler.expected_path = expected_path
        Handler.logger = logger

        self._httpd = HTTPServer((self.host, self.port), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, name="amocrm-callback", daemon=True)
        self._thread.start()
        self.logger.info("amoCRM callback server started: http://%s:%s%s", self.host, self.port, self.path)

    def wait_for_code(self, timeout_seconds: int) -> CallbackResult:
        ok = self._state.event.wait(timeout=max(1, int(timeout_seconds)))
        if not ok or self._state.payload is None:
            raise RuntimeError(f"Timeout waiting for amoCRM callback on {self.host}:{self.port}{self.path}")
        return self._state.payload

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        self._thread = None
        self.logger.info("amoCRM callback server stopped")
