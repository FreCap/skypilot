"""One-way migration from the trusted local request DB to PostgreSQL."""
# pylint: disable=protected-access

from __future__ import annotations

import argparse
from collections.abc import Sequence
import dataclasses
import datetime
import hashlib
import json
import os
import pathlib
import sqlite3
import stat
import sys
from typing import Any, TYPE_CHECKING
import uuid

import orjson
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky import sky_logging

if TYPE_CHECKING:
    from sky.server.requests import requests as requests_lib

logger = sky_logging.init_logger(__name__)

CUTOVER_GATE_PATH_ENV_VAR = 'SKYPILOT_API_REQUEST_CUTOVER_GATE_PATH'
DEFAULT_CUTOVER_GATE_PATH = '~/.sky/api-request-cutover.json'
CUTOVER_METADATA_KEY = 'sqlite-to-postgres-cutover.v1'
_CUTOVER_FORMAT_VERSION = 1
_CUTOVER_LOCK_KEY = 'skypilot:api-request-sqlite-cutover:v1'


class RequestCutoverInProgressError(RuntimeError):
    """Raised when the legacy request store no longer accepts submissions."""


@dataclasses.dataclass(frozen=True)
class CutoverReport:
    """Auditable result of one logical request-store migration."""

    request_count: int
    queue_count: int
    logical_sha256: str
    source_path: str
    interrupted_request_ids: tuple[str, ...]
    completed_at: str
    already_completed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def gate_path() -> pathlib.Path:
    """Return the shared maintenance-gate path."""
    configured = os.environ.get(CUTOVER_GATE_PATH_ENV_VAR,
                                DEFAULT_CUTOVER_GATE_PATH)
    return pathlib.Path(configured).expanduser().resolve()


def legacy_submissions_blocked() -> bool:
    """Whether a pre-cutover backend must reject new submissions."""
    if os.environ.get('SKYPILOT_API_REQUEST_BACKEND') == 'postgres':
        return False
    return gate_path().exists()


def require_legacy_submissions_allowed() -> None:
    """Reject a new SQLite-backed request after the cutover gate is set."""
    if legacy_submissions_blocked():
        raise RequestCutoverInProgressError(
            'The API request store is being migrated to PostgreSQL. '
            'New submissions are temporarily unavailable; retry after the '
            'cutover completes.')


def _write_gate(payload: dict[str, Any]) -> pathlib.Path:
    path = gate_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f'.{path.name}.{uuid.uuid4().hex}.tmp')
    temporary.write_text(json.dumps(payload, sort_keys=True) + '\n',
                         encoding='utf-8')
    os.chmod(temporary, stat.S_IRUSR | stat.S_IWUSR)
    os.replace(temporary, path)
    return path


def block_legacy_submissions(sqlite_path: str) -> pathlib.Path:
    """Create the shared maintenance gate before draining legacy work."""
    source = pathlib.Path(sqlite_path).expanduser().resolve()
    payload = {
        'format_version': _CUTOVER_FORMAT_VERSION,
        'phase': 'blocked',
        'source_path': str(source),
        'blocked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    path = gate_path()
    if path.exists():
        existing = json.loads(path.read_text(encoding='utf-8'))
        if existing.get('source_path') != str(source):
            raise RuntimeError(
                f'Cutover gate {path} belongs to '
                f'{existing.get("source_path")!r}, not {str(source)!r}.')
        return path
    return _write_gate(payload)


def _load_sqlite_requests(source: pathlib.Path) -> list[requests_lib.Request]:
    # Imported lazily so the lightweight middleware gate does not import the
    # request database and its transitive dependencies.
    # pylint: disable=import-outside-toplevel
    from sky.server.requests import requests as requests_lib

    # The first successful cutover makes the source and any WAL sidecars
    # read-only.  Open it read-only from the beginning so verification and an
    # idempotent rerun never ask SQLite to acquire a write lock or create a
    # journal against that frozen source.
    connection = sqlite3.connect(f'{source.as_uri()}?mode=ro', uri=True)
    try:
        columns = {
            str(row[1]) for row in connection.execute(
                f'PRAGMA table_info({requests_lib.REQUEST_TABLE})')
        }
        required = {
            'request_id', 'name', 'entrypoint', 'request_body', 'status',
            'created_at', 'return_value', 'error', 'pid', 'cluster_name',
            'schedule_type', 'user_id'
        }
        missing = sorted(required - columns)
        if missing:
            raise RuntimeError(
                f'Legacy request database {source} is missing required '
                f'columns: {", ".join(missing)}.')
        selected = [
            column for column in requests_lib.REQUEST_COLUMNS
            if column in columns
        ]
        rows = connection.execute(f'SELECT {", ".join(selected)} '
                                  f'FROM {requests_lib.REQUEST_TABLE} '
                                  'ORDER BY request_id').fetchall()
        return [
            requests_lib.Request.from_row(
                requests_lib._update_request_row_fields(row, selected))
            for row in rows
        ]
    finally:
        connection.close()


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value.astimezone(
            datetime.timezone.utc).isoformat(timespec='microseconds')
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _canonical_request_values(request: requests_lib.Request) -> dict[str, Any]:
    # pylint: disable=import-outside-toplevel
    from sky.server.requests import postgres

    values = postgres._request_values_for_db(request)
    values.pop('updated_at')
    return _jsonable(values)


def _logical_hash(requests: Sequence[requests_lib.Request]) -> str:
    digest = hashlib.sha256()
    for request in sorted(requests, key=lambda item: item.request_id):
        digest.update(
            orjson.dumps(_canonical_request_values(request),
                         option=orjson.OPT_SORT_KEYS))
        digest.update(b'\n')
    return digest.hexdigest()


def _report_from_marker(value: dict[str, Any], *,
                        already_completed: bool) -> CutoverReport:
    return CutoverReport(
        request_count=int(value['request_count']),
        queue_count=int(value['queue_count']),
        logical_sha256=str(value['logical_sha256']),
        source_path=str(value['source_path']),
        interrupted_request_ids=tuple(value['interrupted_request_ids']),
        completed_at=str(value['completed_at']),
        already_completed=already_completed,
    )


def _make_source_read_only(source: pathlib.Path) -> None:
    for path in (source, source.with_name(source.name + '-wal'),
                 source.with_name(source.name + '-shm')):
        if not path.exists():
            continue
        mode = stat.S_IMODE(path.stat().st_mode)
        os.chmod(path, mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))


def import_legacy_requests(
    sqlite_path: str,
    *,
    confirm_source_writers_stopped: bool,
    interrupt_running: bool = False,
    make_source_read_only: bool = True,
) -> CutoverReport:
    """Atomically import trusted SQLite rows and commit a cutover marker.

    The caller must first set the shared gate, drain ordinary work, and stop
    every process that can write the SQLite database. There is deliberately no
    dual-write mode.
    """
    if not confirm_source_writers_stopped:
        raise RuntimeError(
            'Refusing cutover without explicit confirmation that every '
            'legacy SQLite writer has stopped.')
    source = pathlib.Path(sqlite_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f'Legacy request database not found: {source}')
    block_legacy_submissions(str(source))

    # Runtime imports avoid making the middleware gate import the request
    # persistence graph.
    # pylint: disable=import-outside-toplevel
    from sky.server.requests import postgres
    from sky.server.requests import requests as requests_lib

    requests = _load_sqlite_requests(source)
    running = [
        request for request in requests
        if request.status is requests_lib.RequestStatus.RUNNING
    ]
    if running and not interrupt_running:
        raise RuntimeError(
            'Legacy requests are still RUNNING: '
            f'{", ".join(request.request_id for request in running)}. '
            'Drain them or rerun with interrupt_running=True after all source '
            'writers have stopped.')
    engine = postgres.initialize_and_get_db()
    with engine.begin() as connection:
        # Serialize the absent-marker case as well as reruns.  A row lock
        # cannot protect a marker that does not exist yet, so concurrent first
        # importers need a transaction-scoped advisory lock.
        connection.execute(
            sqlalchemy.text('SELECT pg_advisory_xact_lock('
                            'hashtextextended(CAST(:lock_key AS text), 0))'),
            {'lock_key': _CUTOVER_LOCK_KEY})
        marker = connection.execute(
            sqlalchemy.select(postgres.STORE_METADATA.c.value).where(
                postgres.STORE_METADATA.c.key ==
                CUTOVER_METADATA_KEY).with_for_update()).scalar_one_or_none()
        if marker is None:
            cutover_time = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            completed_at = cutover_time.isoformat()
        else:
            completed_at = str(marker['completed_at'])
            cutover_time = datetime.datetime.fromisoformat(completed_at)

        # A frozen RUNNING source row must map to the same PostgreSQL row on
        # every rerun.  Reuse the marker's original cutover time instead of
        # assigning a fresh finished_at that changes the logical hash.
        interrupted_ids: list[str] = []
        for request in running:
            request.status = requests_lib.RequestStatus.CANCELLED
            request.should_retry = True
            request.pid = None
            request.finished_at = cutover_time.timestamp()
            request.interrupted_reason = (
                'Interrupted at the one-way SQLite to PostgreSQL cutover '
                'boundary.')
            interrupted_ids.append(request.request_id)

        expected_queue = [
            request for request in requests
            if request.status in (requests_lib.RequestStatus.PENDING,
                                  requests_lib.RequestStatus.WAITING)
        ]
        logical_sha256 = _logical_hash(requests)
        marker_value = {
            'format_version': _CUTOVER_FORMAT_VERSION,
            'request_count': len(requests),
            'queue_count': len(expected_queue),
            'logical_sha256': logical_sha256,
            'source_path': str(source),
            'interrupted_request_ids': sorted(interrupted_ids),
            'completed_at': completed_at,
        }
        if marker is not None:
            existing = _report_from_marker(marker, already_completed=True)
            if (existing.source_path != str(source) or
                    existing.logical_sha256 != logical_sha256 or
                    existing.request_count != len(requests) or
                    existing.queue_count != len(expected_queue)):
                raise RuntimeError(
                    'PostgreSQL already contains a cutover marker for a '
                    'different logical source.')
            report = existing
        else:
            existing_count = int(
                connection.execute(
                    sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                                     ).select_from(
                                         postgres.REQUESTS)).scalar_one())
            if existing_count:
                raise RuntimeError(
                    'PostgreSQL request rows already exist without a cutover '
                    'marker. Refusing to merge two request histories.')
            for request in requests:
                connection.execute(
                    postgresql.insert(postgres.REQUESTS).values(
                        **postgres._request_values_for_db(request)))
            for request in expected_queue:
                connection.execute(
                    postgresql.insert(postgres.QUEUE).values(
                        **postgres._queue_values(request)))

            restored_rows = connection.execute(
                sqlalchemy.select(postgres.REQUESTS).order_by(
                    postgres.REQUESTS.c.request_id)).mappings().all()
            restored = [
                postgres._request_from_mapping(row) for row in restored_rows
            ]
            restored_hash = _logical_hash(restored)
            restored_queue_count = int(
                connection.execute(
                    sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                                     ).select_from(
                                         postgres.QUEUE)).scalar_one())
            if (len(restored) != len(requests) or
                    restored_queue_count != len(expected_queue) or
                    restored_hash != logical_sha256):
                raise RuntimeError(
                    'PostgreSQL cutover verification failed before the marker '
                    'could be committed.')
            connection.execute(
                postgresql.insert(postgres.STORE_METADATA).values(
                    key=CUTOVER_METADATA_KEY,
                    value=marker_value,
                    updated_at=sqlalchemy.func.clock_timestamp()))
            report = _report_from_marker(marker_value, already_completed=False)

    if make_source_read_only:
        _make_source_read_only(source)
    _write_gate({
        'format_version': _CUTOVER_FORMAT_VERSION,
        'phase': 'cutover-complete',
        'source_path': str(source),
        'logical_sha256': report.logical_sha256,
        'completed_at': report.completed_at,
    })
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Safely cut the API request store over to PostgreSQL.')
    subparsers = parser.add_subparsers(dest='command', required=True)
    block = subparsers.add_parser(
        'block', help='block new legacy-backed request submissions')
    block.add_argument('--sqlite-path', required=True)
    migrate = subparsers.add_parser(
        'import', help='import rows after every SQLite writer has stopped')
    migrate.add_argument('--sqlite-path', required=True)
    migrate.add_argument('--confirm-source-writers-stopped',
                         action='store_true',
                         required=True)
    migrate.add_argument('--interrupt-running', action='store_true')
    migrate.add_argument('--keep-source-writable', action='store_true')
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == 'block':
        path = block_legacy_submissions(args.sqlite_path)
        print(
            json.dumps({
                'gate_path': str(path),
                'phase': 'blocked',
            },
                       sort_keys=True))
        return 0
    report = import_legacy_requests(
        args.sqlite_path,
        confirm_source_writers_stopped=args.confirm_source_writers_stopped,
        interrupt_running=args.interrupt_running,
        make_source_read_only=not args.keep_source_writable)
    print(json.dumps(report.as_dict(), sort_keys=True))
    return 0


if __name__ == '__main__':
    sys.exit(main())
