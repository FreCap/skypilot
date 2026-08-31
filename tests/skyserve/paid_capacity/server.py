"""Deterministic one-slot HTTP worker for paid-capacity qualification."""

import argparse
import http.server
import json
import socketserver
import threading
import time
from typing import Any


class _State:
    """Process-local occupancy exposed through the async-capacity contract."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._running = 0

    def change_running(self, delta: int) -> None:
        with self._lock:
            self._running += delta

    def running(self) -> int:
        with self._lock:
            return self._running


_STATE = _State()


class _Handler(http.server.BaseHTTPRequestHandler):
    """Minimal handler; it intentionally has no package dependencies."""

    server_version = 'SkyServePaidCapacityTest/1'

    def log_message(
            self,
            format: str,  # pylint: disable=redefined-builtin
            *args: Any) -> None:
        del format, args

    def _write_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, sort_keys=True).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Scale pressure may be cancelled after the provider target is met.
            pass

    def do_GET(self) -> None:  # pylint: disable=invalid-name
        if self.path == '/health':
            self._write_json(200, {'status': 'ok'})
            return
        self._write_json(404, {'error': 'not found'})

    def do_POST(self) -> None:  # pylint: disable=invalid-name
        if self.path != '/v1/models/model:predict':
            self._write_json(404, {'error': 'not found'})
            return
        try:
            content_length = int(self.headers.get('Content-Length', '0'))
            payload = json.loads(self.rfile.read(content_length) or b'{}')
        except (ValueError, json.JSONDecodeError):
            self._write_json(400, {'error': 'invalid JSON'})
            return
        if payload.get('action') == 'async_capacity':
            self._write_json(
                200, {
                    'status': 'READY',
                    'running_count': _STATE.running(),
                    'predict_concurrency': 1,
                    'max_workers': 1,
                })
            return
        duration = payload.get('duration_seconds', 0)
        if (not isinstance(duration, (int, float)) or
                isinstance(duration, bool) or not 0 <= duration <= 120):
            self._write_json(400, {'error': 'invalid duration_seconds'})
            return
        request_id = payload.get('request_id')
        if not isinstance(request_id, str) or not request_id:
            self._write_json(400, {'error': 'request_id is required'})
            return
        _STATE.change_running(1)
        try:
            time.sleep(float(duration))
            self._write_json(200, {'request_id': request_id, 'status': 'ok'})
        finally:
            _STATE.change_running(-1)


class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8080)
    args = parser.parse_args()
    _Server(('0.0.0.0', args.port), _Handler).serve_forever()


if __name__ == '__main__':
    main()
