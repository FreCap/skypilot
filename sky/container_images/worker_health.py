"""Dependency-free health and Prometheus surface for image workers."""

from __future__ import annotations

import dataclasses
from http import server
import threading
import time
from typing import Any


@dataclasses.dataclass(frozen=True)
class HealthSnapshot:
    live: bool
    ready: bool
    in_flight: int
    heartbeat_successes: int
    heartbeat_failures: int


class WorkerHealth:
    """Tracks loop and PostgreSQL heartbeat progress with monotonic clocks."""

    def __init__(self, kind: str, *, liveness_deadline_seconds: int = 30):
        if liveness_deadline_seconds <= 0:
            raise ValueError('Worker liveness deadline must be positive.')
        self.kind = kind
        self.liveness_deadline_seconds = liveness_deadline_seconds
        self._lock = threading.Lock()
        self._last_tick = time.monotonic()
        self._registered = False
        self._heartbeat_ready = False
        self._in_flight = 0
        self._heartbeat_successes = 0
        self._heartbeat_failures = 0

    def registered(self) -> None:
        with self._lock:
            self._registered = True
            self._last_tick = time.monotonic()

    def tick(self, in_flight: int) -> None:
        if in_flight < 0:
            raise ValueError('Worker in-flight count cannot be negative.')
        with self._lock:
            self._last_tick = time.monotonic()
            self._in_flight = in_flight

    def heartbeat(self, success: bool) -> None:
        with self._lock:
            self._last_tick = time.monotonic()
            self._heartbeat_ready = success
            if success:
                self._heartbeat_successes += 1
            else:
                self._heartbeat_failures += 1

    def snapshot(self) -> HealthSnapshot:
        with self._lock:
            live = (time.monotonic() - self._last_tick
                    <= self.liveness_deadline_seconds)
            return HealthSnapshot(live=live,
                                  ready=(live and self._registered and
                                         self._heartbeat_ready),
                                  in_flight=self._in_flight,
                                  heartbeat_successes=self._heartbeat_successes,
                                  heartbeat_failures=self._heartbeat_failures)

    def metrics(self) -> bytes:
        snapshot = self.snapshot()
        kind = self.kind.replace('"', '')
        labels = f'kind="{kind}"'
        lines = (
            '# HELP skypilot_image_worker_live Main claim loop is progressing.',
            '# TYPE skypilot_image_worker_live gauge',
            f'skypilot_image_worker_live{{{labels}}} {int(snapshot.live)}',
            '# HELP skypilot_image_worker_ready PostgreSQL heartbeats succeed.',
            '# TYPE skypilot_image_worker_ready gauge',
            f'skypilot_image_worker_ready{{{labels}}} {int(snapshot.ready)}',
            '# TYPE skypilot_image_worker_in_flight gauge',
            f'skypilot_image_worker_in_flight{{{labels}}} {snapshot.in_flight}',
            '# TYPE skypilot_image_worker_heartbeat_success_total counter',
            f'skypilot_image_worker_heartbeat_success_total{{{labels}}} '
            f'{snapshot.heartbeat_successes}',
            '# TYPE skypilot_image_worker_heartbeat_failure_total counter',
            f'skypilot_image_worker_heartbeat_failure_total{{{labels}}} '
            f'{snapshot.heartbeat_failures}',
        )
        return ('\n'.join(lines) + '\n').encode()


class _Handler(server.BaseHTTPRequestHandler):
    """Serves worker liveness, readiness, and Prometheus metrics."""

    health: WorkerHealth

    def do_GET(self) -> None:  # pylint: disable=invalid-name
        snapshot = self.health.snapshot()
        if self.path == '/live':
            self._respond(200 if snapshot.live else 503, b'live\n')
        elif self.path == '/ready':
            self._respond(200 if snapshot.ready else 503, b'ready\n')
        elif self.path == '/metrics':
            self._respond(200,
                          self.health.metrics(),
                          content_type='text/plain; version=0.0.4')
        else:
            self._respond(404, b'not found\n')

    def _respond(self,
                 status: int,
                 body: bytes,
                 *,
                 content_type: str = 'text/plain') -> None:
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format_string: str, *args: Any) -> None:
        del format_string, args


class HealthServer:
    """Owns one daemon HTTP server and its bounded shutdown."""

    def __init__(self, health: WorkerHealth, port: int):
        if not 1 <= port <= 65535:
            raise ValueError('Worker health port is invalid.')
        handler = type('WorkerHealthHandler', (_Handler,), {'health': health})
        self._server = server.ThreadingHTTPServer(('0.0.0.0', port), handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        name='image-worker-health',
                                        daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
