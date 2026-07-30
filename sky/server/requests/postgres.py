"""PostgreSQL request persistence and leased queue delivery."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from collections.abc import Generator
import contextlib
import datetime
import os
import signal
import time
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext import asyncio as sqlalchemy_async

from sky import sky_logging
from sky.server import daemons
from sky.server.requests import preconditions
from sky.server.requests import registry as request_registry
from sky.server.requests import requests as requests_lib
from sky.server.requests import storage as request_storage
from sky.server.requests.queues import base as queue_base
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

logger = sky_logging.init_logger(__name__)

REQUEST_BACKEND_ENV_VAR = 'SKYPILOT_API_REQUEST_BACKEND'
POSTGRES_REQUEST_BACKEND = 'postgres'
SERVER_INSTANCE_ID_ENV_VAR = 'SKYPILOT_API_SERVER_INSTANCE_ID'

_CLAIM_LEASE_SECONDS = 30
_CLAIM_HEARTBEAT_INTERVAL_SECONDS = 10
_MAX_EXPIRED_CLAIMS_PER_SWEEP = 100

_METADATA = sqlalchemy.MetaData()
REQUESTS = sqlalchemy.Table(
    'api_requests',
    _METADATA,
    sqlalchemy.Column('request_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('handler_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('payload_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('payload_format', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('payload_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('producer_version', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('payload_json', postgresql.JSONB, nullable=False),
    sqlalchemy.Column('execution_class', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('status', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('return_value', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('error', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('pid', sqlalchemy.Integer),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('cluster_name', sqlalchemy.Text),
    sqlalchemy.Column('schedule_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('user_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('status_msg', sqlalchemy.Text),
    sqlalchemy.Column('should_retry', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('finished_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('file_mounts_blob_id', sqlalchemy.Text),
    sqlalchemy.Column('ignore_return_value', sqlalchemy.Boolean,
                      nullable=False),
    sqlalchemy.Column('retryable', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('execution_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('claim_token', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('worker_instance_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('lease_expires_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('heartbeat_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('cancel_requested_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('cancel_acknowledged_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('interrupted_reason', sqlalchemy.Text),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
)
QUEUE = sqlalchemy.Table(
    'api_request_queue',
    _METADATA,
    sqlalchemy.Column('request_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('schedule_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('priority', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('available_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('enqueued_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('sequence',
                      sqlalchemy.BigInteger,
                      sqlalchemy.Identity(),
                      nullable=False,
                      unique=True),
    sqlalchemy.Column('ignore_return_value', sqlalchemy.Boolean,
                      nullable=False),
    sqlalchemy.Column('retryable', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('precondition_type', sqlalchemy.Text),
    sqlalchemy.Column('precondition_payload',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('precondition_deadline',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('precondition_attempts',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('delivery_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('claim_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
)
STORE_METADATA = sqlalchemy.Table(
    'api_request_store_metadata',
    _METADATA,
    sqlalchemy.Column('key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('value', postgresql.JSONB, nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
)

_DATETIME_FIELDS = frozenset({
    'created_at',
    'finished_at',
    'lease_expires_at',
    'heartbeat_at',
    'cancel_requested_at',
    'cancel_acknowledged_at',
})


def ensure_server_instance_id() -> str:
    """Return one stable UUID inherited by every process in a server pod."""
    value = os.environ.get(SERVER_INSTANCE_ID_ENV_VAR)
    if value is None:
        value = str(uuid.uuid4())
        os.environ[SERVER_INSTANCE_ID_ENV_VAR] = value
    try:
        return str(uuid.UUID(value))
    except ValueError as e:
        raise ValueError(
            f'{SERVER_INSTANCE_ID_ENV_VAR} must be a UUID, got {value!r}.'
        ) from e


def _utc_datetime(timestamp: float | None) -> datetime.datetime | None:
    if timestamp is None:
        return None
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


def _timestamp(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value.timestamp()
    return value


def _request_values_for_db(request: requests_lib.Request) -> dict[str, Any]:
    values = request.durable_values()
    for field in _DATETIME_FIELDS:
        values[field] = _utc_datetime(values.get(field))
    for field in ('claim_token', 'worker_instance_id'):
        if values.get(field) is not None:
            values[field] = uuid.UUID(str(values[field]))
    values['updated_at'] = sqlalchemy.func.clock_timestamp()
    return values


def _request_from_mapping(
        mapping: sqlalchemy.engine.RowMapping) -> requests_lib.Request:
    values = dict(mapping)
    for field in _DATETIME_FIELDS:
        values[field] = _timestamp(values.get(field))
    return requests_lib.Request.from_durable_values(values)


def _initialize_schema(engine: sqlalchemy.engine.Engine) -> None:
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'The durable API request backend requires PostgreSQL.')
    migration_utils.safe_alembic_upgrade(
        engine,
        migration_utils.API_REQUESTS_DB_NAME,
        migration_utils.API_REQUESTS_VERSION,
        mode=migration_utils.configured_migration_mode())


_DB_MANAGER = db_utils.DatabaseManager('api_requests', _initialize_schema)


def initialize_and_get_db() -> sqlalchemy.engine.Engine:
    """Initialize the request schema and return its synchronous engine."""
    return _DB_MANAGER.get_engine()


async def _get_async_engine() -> sqlalchemy_async.AsyncEngine:
    return await _DB_MANAGER.get_async_engine()


def _queue_values(request: requests_lib.Request) -> dict[str, Any]:
    now = sqlalchemy.func.clock_timestamp()
    return {
        'request_id': request.request_id,
        'schedule_type': request.schedule_type.value,
        'priority': 0,
        'available_at': now,
        'enqueued_at': now,
        'ignore_return_value': request.ignore_return_value,
        'retryable': request.retryable,
        'precondition_type': request.precondition_type,
        'precondition_payload': request.precondition_payload,
        'precondition_deadline': _utc_datetime(request.precondition_deadline),
        'precondition_attempts': 0,
        'delivery_state': 'queued',
        'claim_generation': None,
        'updated_at': now,
    }


def _request_filter_statement(
    req_filter: requests_lib.RequestTaskFilter,) -> sqlalchemy.sql.Select:
    statement = sqlalchemy.select(REQUESTS)
    if req_filter.status is not None:
        statement = statement.where(
            REQUESTS.c.status.in_(
                [status.value for status in req_filter.status]))
    if req_filter.cluster_names is not None:
        statement = statement.where(
            REQUESTS.c.cluster_name.in_(req_filter.cluster_names))
    if req_filter.user_id is not None:
        statement = statement.where(REQUESTS.c.user_id == req_filter.user_id)
    if req_filter.exclude_request_names is not None:
        statement = statement.where(
            REQUESTS.c.name.not_in(req_filter.exclude_request_names))
    if req_filter.include_request_names is not None:
        statement = statement.where(
            REQUESTS.c.name.in_(req_filter.include_request_names))
    if req_filter.finished_before is not None:
        before = _utc_datetime(req_filter.finished_before)
        if req_filter.include_missing_finished_at:
            statement = statement.where(
                sqlalchemy.or_(
                    REQUESTS.c.finished_at < before,
                    sqlalchemy.and_(
                        REQUESTS.c.finished_at.is_(None),
                        REQUESTS.c.status.in_([
                            status.value for status in
                            requests_lib.RequestStatus.finished_status()
                        ]), REQUESTS.c.created_at < before)))
        else:
            statement = statement.where(REQUESTS.c.finished_at < before)
    if req_filter.finished_after is not None:
        after = _utc_datetime(req_filter.finished_after)
        statement = statement.where(
            sqlalchemy.or_(REQUESTS.c.finished_at >= after,
                           REQUESTS.c.finished_at.is_(None)))
    if req_filter.sort:
        statement = statement.order_by(REQUESTS.c.created_at.desc())
    if req_filter.limit is not None:
        statement = statement.limit(req_filter.limit)
    return statement


class PostgresRequestBackend(request_storage.RequestBackend):
    """PostgreSQL implementation of request persistence."""

    def __init__(self) -> None:
        request_registry.register_builtin_handlers()
        self._instance_id = ensure_server_instance_id()

    @property
    def uses_durable_queue(self) -> bool:
        return True

    @property
    def claim_heartbeat_interval_seconds(self) -> float:
        return _CLAIM_HEARTBEAT_INTERVAL_SECONDS

    @property
    def instance_id(self) -> str:
        return self._instance_id

    def _fenced_request_predicates(
            self,
            request_id: str) -> tuple[sqlalchemy.ColumnElement[bool], ...]:
        """Return write predicates for the claim in this execution context."""
        claim = request_storage.active_execution_claim()
        if claim is None:
            return ()
        if claim.request_id != request_id:
            return (sqlalchemy.false(),)
        return (
            REQUESTS.c.execution_generation == claim.execution_generation,
            REQUESTS.c.claim_token == uuid.UUID(claim.claim_token),
            REQUESTS.c.worker_instance_id == uuid.UUID(self._instance_id),
            REQUESTS.c.lease_expires_at > sqlalchemy.func.clock_timestamp(),
        )

    def get_request(
            self,
            request_id: str,
            fields: list[str] | None = None) -> requests_lib.Request | None:
        del fields
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == request_id)).mappings().first()
        return _request_from_mapping(row) if row is not None else None

    async def get_request_async(
            self,
            request_id: str,
            fields: list[str] | None = None) -> requests_lib.Request | None:
        del fields
        engine = await _get_async_engine()
        async with engine.connect() as connection:
            result = await connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == request_id))
            row = result.mappings().first()
        return _request_from_mapping(row) if row is not None else None

    @contextlib.contextmanager
    def update_request(
            self, request_id: str
    ) -> Generator[requests_lib.Request | None, None, None]:
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(
                        request_id)).with_for_update()).mappings().first()
            request = _request_from_mapping(row) if row is not None else None
            yield request
            if request is not None:
                values = _request_values_for_db(request)
                values.pop('request_id')
                connection.execute(
                    sqlalchemy.update(REQUESTS).where(
                        REQUESTS.c.request_id == request_id,
                        *self._fenced_request_predicates(request_id)).values(
                            **values))

    @contextlib.asynccontextmanager
    async def update_request_async(
            self, request_id: str
    ) -> AsyncGenerator[requests_lib.Request | None, None]:
        engine = await _get_async_engine()
        async with engine.begin() as connection:
            result = await connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(
                        request_id)).with_for_update())
            row = result.mappings().first()
            request = _request_from_mapping(row) if row is not None else None
            yield request
            if request is not None:
                values = _request_values_for_db(request)
                values.pop('request_id')
                await connection.execute(
                    sqlalchemy.update(REQUESTS).where(
                        REQUESTS.c.request_id == request_id,
                        *self._fenced_request_predicates(request_id)).values(
                            **values))

    async def create_if_not_exists_async(self,
                                         request: requests_lib.Request) -> bool:
        engine = await _get_async_engine()
        values = _request_values_for_db(request)
        async with engine.begin() as connection:
            result = await connection.execute(
                postgresql.insert(REQUESTS).values(
                    **values).on_conflict_do_nothing(
                        index_elements=[REQUESTS.c.request_id]).returning(
                            REQUESTS.c.request_id))
            inserted = result.scalar_one_or_none() is not None
            if inserted and request.should_enqueue:
                await connection.execute(
                    postgresql.insert(QUEUE).values(
                        **_queue_values(request)).on_conflict_do_nothing(
                            index_elements=[QUEUE.c.request_id]))
        return inserted

    async def create_or_refresh_internal_daemon_async(
            self, request: requests_lib.Request) -> bool:
        request.should_enqueue = True
        engine = await _get_async_engine()
        values = _request_values_for_db(request)
        async with engine.begin() as connection:
            result = await connection.execute(
                postgresql.insert(REQUESTS).values(
                    **values).on_conflict_do_nothing(
                        index_elements=[REQUESTS.c.request_id]).returning(
                            REQUESTS.c.request_id))
            inserted = result.scalar_one_or_none() is not None
            if inserted:
                await connection.execute(
                    postgresql.insert(QUEUE).values(
                        **_queue_values(request)).on_conflict_do_nothing(
                            index_elements=[QUEUE.c.request_id]))
                return True

            result = await connection.execute(
                sqlalchemy.select(REQUESTS.c.status).where(
                    REQUESTS.c.request_id ==
                    request.request_id).with_for_update())
            existing_status = result.scalar_one()
            terminal = existing_status in {
                status.value
                for status in requests_lib.RequestStatus.finished_status()
            }
            refreshed_values: dict[str, Any] = {
                'name': values['name'],
                'handler_name': values['handler_name'],
                'payload_type': values['payload_type'],
                'payload_format': values['payload_format'],
                'payload_version': values['payload_version'],
                'producer_version': values['producer_version'],
                'payload_json': values['payload_json'],
                'execution_class': values['execution_class'],
                'schedule_type': values['schedule_type'],
                'updated_at': sqlalchemy.func.clock_timestamp(),
            }
            if terminal:
                refreshed_values.update({
                    'status': requests_lib.RequestStatus.PENDING.value,
                    'return_value': None,
                    'error': None,
                    'pid': None,
                    'status_msg': None,
                    'should_retry': False,
                    'finished_at': None,
                    'claim_token': None,
                    'worker_instance_id': None,
                    'lease_expires_at': None,
                    'heartbeat_at': None,
                    'cancel_requested_at': None,
                    'cancel_acknowledged_at': None,
                    'interrupted_reason': None,
                })
            await connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == request.request_id).values(
                        **refreshed_values))
            if terminal:
                await connection.execute(
                    sqlalchemy.delete(QUEUE).where(
                        QUEUE.c.request_id == request.request_id))
                await connection.execute(
                    postgresql.insert(QUEUE).values(**_queue_values(request)))
                return True
        return False

    async def delete_orphan_internal_daemons_async(
        self,
        internal_daemons: list[daemons.InternalRequestDaemon],
    ) -> None:
        keep_ids = [daemon.id for daemon in internal_daemons]
        engine = await _get_async_engine()
        statement = sqlalchemy.delete(REQUESTS).where(
            REQUESTS.c.request_id.like('%-daemon'))
        if keep_ids:
            statement = statement.where(REQUESTS.c.request_id.not_in(keep_ids))
        async with engine.begin() as connection:
            await connection.execute(statement)

    def query_requests(
        self, req_filter: requests_lib.RequestTaskFilter
    ) -> list[requests_lib.Request]:
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            rows = connection.execute(
                _request_filter_statement(req_filter)).mappings().all()
        return [_request_from_mapping(row) for row in rows]

    async def query_requests_async(
        self, req_filter: requests_lib.RequestTaskFilter
    ) -> list[requests_lib.Request]:
        engine = await _get_async_engine()
        async with engine.connect() as connection:
            result = await connection.execute(
                _request_filter_statement(req_filter))
            rows = result.mappings().all()
        return [_request_from_mapping(row) for row in rows]

    async def delete_requests(self, request_ids: list[str]) -> None:
        if not request_ids:
            return
        engine = await _get_async_engine()
        async with engine.begin() as connection:
            await connection.execute(
                sqlalchemy.delete(REQUESTS).where(
                    REQUESTS.c.request_id.in_(request_ids)))

    async def update_status_async(self, request_id: str,
                                  status: requests_lib.RequestStatus) -> None:
        engine = await _get_async_engine()
        async with engine.begin() as connection:
            await connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(request_id)).values(
                        status=status.value,
                        updated_at=sqlalchemy.func.clock_timestamp()))

    async def update_status_msg_async(self, request_id: str,
                                      status_msg: str) -> None:
        engine = await _get_async_engine()
        async with engine.begin() as connection:
            await connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(request_id)).values(
                        status_msg=status_msg,
                        updated_at=sqlalchemy.func.clock_timestamp()))

    def try_mark_running(self,
                         request_id: str,
                         pid: int | None,
                         execution_generation: int = 0,
                         claim_token: str | None = None) -> bool:
        engine = initialize_and_get_db()
        statement = sqlalchemy.update(REQUESTS).where(
            REQUESTS.c.request_id == request_id,
            REQUESTS.c.status.in_([
                status.value
                for status in requests_lib.RequestStatus.executable_statuses()
            ]))
        if claim_token is not None:
            statement = statement.where(
                REQUESTS.c.execution_generation == execution_generation,
                REQUESTS.c.claim_token == uuid.UUID(claim_token),
                REQUESTS.c.worker_instance_id == uuid.UUID(self._instance_id),
                REQUESTS.c.lease_expires_at > sqlalchemy.func.clock_timestamp())
        else:
            # Direct coroutine execution is allowed only for requests that
            # were intentionally created without a durable queue delivery.
            statement = statement.where(~sqlalchemy.exists().where(
                QUEUE.c.request_id == REQUESTS.c.request_id))
        statement = statement.values(
            status=requests_lib.RequestStatus.RUNNING.value,
            pid=pid,
            status_msg=None,
            heartbeat_at=sqlalchemy.func.clock_timestamp(),
            updated_at=sqlalchemy.func.clock_timestamp())
        with engine.begin() as connection:
            result = connection.execute(statement)
        return result.rowcount == 1

    def heartbeat_claim(self, claim: request_storage.ExecutionClaim) -> bool:
        """Extend a lease only for its current generation and owner."""
        engine = initialize_and_get_db()
        now = sqlalchemy.func.clock_timestamp()
        claimed_delivery = sqlalchemy.exists().where(
            QUEUE.c.request_id == claim.request_id,
            QUEUE.c.delivery_state == 'claimed',
            QUEUE.c.claim_generation == claim.execution_generation)
        statement = sqlalchemy.update(REQUESTS).where(
            REQUESTS.c.request_id == claim.request_id,
            REQUESTS.c.execution_generation == claim.execution_generation,
            REQUESTS.c.claim_token == uuid.UUID(claim.claim_token),
            REQUESTS.c.worker_instance_id == uuid.UUID(self._instance_id),
            REQUESTS.c.lease_expires_at > now,
            REQUESTS.c.status.in_([
                status.value
                for status in requests_lib.RequestStatus.active_statuses()
            ]), claimed_delivery).values(
                heartbeat_at=now,
                lease_expires_at=(
                    now + datetime.timedelta(seconds=_CLAIM_LEASE_SECONDS)),
                updated_at=now)
        with engine.begin() as connection:
            result = connection.execute(statement)
        return result.rowcount == 1

    def set_request_finished(self,
                             request_id: str,
                             status: requests_lib.RequestStatus,
                             error: BaseException | None = None,
                             result: Any | None = None) -> bool:
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(
                        request_id)).with_for_update()).mappings().first()
            if row is None:
                return False
            request = _request_from_mapping(row)
            if request.status in requests_lib.RequestStatus.finished_status():
                return False
            request.status = status
            request.finished_at = time.time()
            if error is not None:
                request.set_error(error)
            if result is not None:
                request.set_return_value(result)
            values = _request_values_for_db(request)
            values.pop('request_id')
            update_result = connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    *self._fenced_request_predicates(request_id)).values(
                        **values))
            if update_result.rowcount == 1:
                connection.execute(
                    sqlalchemy.delete(QUEUE).where(
                        QUEUE.c.request_id == request_id))
                return True
            return False

    async def set_request_finished_async(self,
                                         request_id: str,
                                         status: requests_lib.RequestStatus,
                                         error: BaseException | None = None,
                                         result: Any | None = None) -> bool:
        return await asyncio.to_thread(self.set_request_finished, request_id,
                                       status, error, result)

    def kill_requests(self,
                      request_ids: list[str] | None = None,
                      user_id: str | None = None) -> list[str]:
        engine = initialize_and_get_db()
        active = [
            status.value
            for status in requests_lib.RequestStatus.active_statuses()
        ]
        statement = sqlalchemy.select(REQUESTS).where(
            REQUESTS.c.status.in_(active), REQUESTS.c.name != 'sky.api_cancel')
        if request_ids is not None:
            statement = statement.where(REQUESTS.c.request_id.in_(request_ids))
        if user_id is not None:
            statement = statement.where(REQUESTS.c.user_id == user_id)
        cancelled: list[str] = []
        local_pids: list[tuple[str, int, uuid.UUID | None]] = []
        now = sqlalchemy.func.clock_timestamp()
        with engine.begin() as connection:
            rows = connection.execute(
                statement.with_for_update()).mappings().all()
            database_now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            for row in rows:
                if daemons.is_daemon_request_id(row['request_id']):
                    continue
                cancelled.append(row['request_id'])
                connection.execute(
                    sqlalchemy.update(REQUESTS).where(
                        REQUESTS.c.request_id == row['request_id']).values(
                            status=requests_lib.RequestStatus.CANCELLED.value,
                            cancel_requested_at=now,
                            finished_at=now,
                            updated_at=now))
                connection.execute(
                    sqlalchemy.delete(QUEUE).where(
                        QUEUE.c.request_id == row['request_id']))
                if (row['pid'] is not None and
                        row['claim_token'] is not None and
                        str(row['worker_instance_id']) == self._instance_id and
                        row['lease_expires_at'] is not None and
                        row['lease_expires_at'] > database_now):
                    local_pids.append((row['request_id'], int(row['pid']),
                                       row['claim_token']))
        for request_id, pid, claim_token in local_pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                continue
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.update(REQUESTS).where(
                        REQUESTS.c.request_id == request_id,
                        REQUESTS.c.claim_token == claim_token).values(
                            cancel_acknowledged_at=(
                                sqlalchemy.func.clock_timestamp()),
                            updated_at=sqlalchemy.func.clock_timestamp()))
        return cancelled

    async def kill_request_async(self, request_id: str) -> bool:
        return bool(await asyncio.to_thread(self.kill_requests, [request_id],
                                            None))

    async def get_latest_request_id_async(self) -> str | None:
        engine = await _get_async_engine()
        async with engine.connect() as connection:
            result = await connection.execute(
                sqlalchemy.select(REQUESTS.c.request_id).order_by(
                    REQUESTS.c.created_at.desc()).limit(1))
            return result.scalar_one_or_none()

    def get_requests_with_prefix(self,
                                 request_id_prefix: str,
                                 fields: list[str] | None = None
                                ) -> list[requests_lib.Request] | None:
        del fields
        engine = initialize_and_get_db()
        escaped = db_utils.glob_to_like_pattern(request_id_prefix) + '%'
        with engine.connect() as connection:
            rows = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id.like(
                        escaped,
                        escape=db_utils.LIKE_ESCAPE_CHAR))).mappings().all()
        return ([_request_from_mapping(row) for row in rows] if rows else None)

    async def get_requests_async_with_prefix(
            self,
            request_id_prefix: str,
            fields: list[str] | None = None
    ) -> list[requests_lib.Request] | None:
        return await asyncio.to_thread(self.get_requests_with_prefix,
                                       request_id_prefix, fields)

    async def get_request_status_async(
            self,
            request_id: str,
            include_msg: bool = False) -> requests_lib.StatusWithMsg | None:
        columns = [REQUESTS.c.status]
        if include_msg:
            columns.append(REQUESTS.c.status_msg)
        engine = await _get_async_engine()
        async with engine.connect() as connection:
            result = await connection.execute(
                sqlalchemy.select(*columns).where(
                    REQUESTS.c.request_id == request_id))
            row = result.first()
        if row is None:
            return None
        return requests_lib.StatusWithMsg(requests_lib.RequestStatus(row[0]),
                                          row[1] if include_msg else None)

    async def get_api_request_ids_start_with(self,
                                             incomplete: str) -> list[str]:
        engine = await _get_async_engine()
        pattern = db_utils.glob_to_like_pattern(incomplete) + '%'
        active_first = sqlalchemy.case(
            (REQUESTS.c.status.in_(['PENDING', 'WAITING', 'RUNNING']), 0),
            else_=1)
        async with engine.connect() as connection:
            result = await connection.execute(
                sqlalchemy.select(REQUESTS.c.request_id).where(
                    REQUESTS.c.request_id.like(
                        pattern, escape=db_utils.LIKE_ESCAPE_CHAR)).order_by(
                            active_first,
                            REQUESTS.c.created_at.desc()).limit(1000))
            return list(result.scalars())

    def get_active_file_mounts_blob_ids(self) -> set[str]:
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            return set(
                connection.execute(
                    sqlalchemy.select(
                        REQUESTS.c.file_mounts_blob_id).distinct().where(
                            REQUESTS.c.status.in_([
                                status.value for status in
                                requests_lib.RequestStatus.active_statuses()
                            ]), REQUESTS.c.file_mounts_blob_id.is_not(
                                None))).scalars())

    def get_shutdown_active_requests(self) -> list[tuple[str, str]]:
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            return [(str(row[0]), str(row[1])) for row in connection.execute(
                sqlalchemy.select(REQUESTS.c.request_id, REQUESTS.c.name).where(
                    REQUESTS.c.status.in_([
                        status.value for status in
                        requests_lib.RequestStatus.active_statuses()
                    ]))).all()]

    def reset_on_startup(self) -> None:
        initialize_and_get_db()

    def recover_on_startup(self) -> bool:
        """Validate durable state without wiping or re-enqueueing rows."""
        engine = initialize_and_get_db()
        active = [
            status.value
            for status in requests_lib.RequestStatus.active_statuses()
        ]
        with engine.begin() as connection:
            # Coroutine requests intentionally have no queue delivery. They
            # cannot survive loss of the all-role compatibility process, so
            # preserve the row and surface a retry instead of silently
            # manufacturing a queue message with different execution
            # semantics.
            connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.status.in_(active), ~sqlalchemy.exists().where(
                        QUEUE.c.request_id == REQUESTS.c.request_id)).values(
                            status=requests_lib.RequestStatus.CANCELLED.value,
                            should_retry=True,
                            finished_at=sqlalchemy.func.clock_timestamp(),
                            interrupted_reason=(
                                'The compatibility process stopped while a '
                                'non-queued coroutine request was active.'),
                            updated_at=sqlalchemy.func.clock_timestamp()))
        # Queue rows are already durable and must not be copied back through
        # the legacy in-memory re-enqueue path.
        return False


class PostgresQueueBackend(queue_base.QueueBackend):
    """One schedule-class view over the durable PostgreSQL queue."""

    def __init__(self, schedule_type: str):
        self._schedule_type = schedule_type
        self._instance_id = ensure_server_instance_id()

    def put(self, item: queue_base.QueueItemLike) -> None:
        normalized = queue_base.normalize_queue_item(item)
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == normalized.request_id).
                with_for_update()).mappings().first()
            if row is None:
                return
            queue_row = connection.execute(
                sqlalchemy.select(QUEUE).where(
                    QUEUE.c.request_id == normalized.request_id).
                with_for_update()).mappings().first()
            if queue_row is None:
                if row['status'] not in ('PENDING', 'WAITING'):
                    return
                connection.execute(
                    postgresql.insert(QUEUE).values(
                        request_id=normalized.request_id,
                        schedule_type=row['schedule_type'],
                        priority=0,
                        available_at=sqlalchemy.func.clock_timestamp(),
                        enqueued_at=sqlalchemy.func.clock_timestamp(),
                        ignore_return_value=normalized.ignore_return_value,
                        retryable=normalized.retryable,
                        precondition_type=None,
                        precondition_payload=None,
                        precondition_deadline=None,
                        precondition_attempts=0,
                        delivery_state='queued',
                        claim_generation=None,
                        updated_at=sqlalchemy.func.clock_timestamp()))
                return
            if queue_row['delivery_state'] == 'queued':
                return
            if (normalized.claim_token is None or row['claim_token'] is None or
                    str(row['claim_token']) != normalized.claim_token or
                    int(row['execution_generation'])
                    != normalized.execution_generation or
                    str(row['worker_instance_id']) != self._instance_id):
                logger.warning('Ignoring stale requeue for request '
                               f'{normalized.request_id}.')
                return
            result = connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == normalized.request_id,
                    REQUESTS.c.execution_generation ==
                    normalized.execution_generation,
                    REQUESTS.c.claim_token == uuid.UUID(normalized.claim_token),
                    REQUESTS.c.worker_instance_id == uuid.UUID(
                        self._instance_id), REQUESTS.c.lease_expires_at
                    > sqlalchemy.func.clock_timestamp()).values(
                        status=requests_lib.RequestStatus.WAITING.value,
                        pid=None,
                        claim_token=None,
                        worker_instance_id=None,
                        lease_expires_at=None,
                        heartbeat_at=None,
                        updated_at=sqlalchemy.func.clock_timestamp()))
            if result.rowcount != 1:
                logger.warning('Ignoring expired requeue for request '
                               f'{normalized.request_id}.')
                return
            connection.execute(
                sqlalchemy.update(QUEUE).where(
                    QUEUE.c.request_id == normalized.request_id).values(
                        delivery_state='queued',
                        claim_generation=None,
                        available_at=sqlalchemy.func.clock_timestamp(),
                        updated_at=sqlalchemy.func.clock_timestamp()))

    async def put_async(self, item: queue_base.QueueItemLike) -> None:
        await asyncio.to_thread(self.put, item)

    def _reap_expired_claims(self,
                             connection: sqlalchemy.engine.Connection) -> None:
        now = sqlalchemy.func.clock_timestamp()
        rows = connection.execute(
            sqlalchemy.select(REQUESTS, QUEUE).join(
                QUEUE, QUEUE.c.request_id == REQUESTS.c.request_id).where(
                    QUEUE.c.schedule_type == self._schedule_type,
                    QUEUE.c.delivery_state == 'claimed',
                    REQUESTS.c.lease_expires_at
                    < sqlalchemy.func.clock_timestamp()).limit(
                        _MAX_EXPIRED_CLAIMS_PER_SWEEP).with_for_update(
                            skip_locked=True)).mappings().all()
        for row in rows:
            registration = request_registry.resolve_handler(row['handler_name'])
            replayable = registration.replay_policy in (
                request_registry.ReplayPolicy.READ_ONLY,
                request_registry.ReplayPolicy.RECONCILE)
            if replayable:
                connection.execute(
                    sqlalchemy.update(REQUESTS).where(
                        REQUESTS.c.request_id == row['request_id']).values(
                            status=requests_lib.RequestStatus.WAITING.value,
                            pid=None,
                            claim_token=None,
                            worker_instance_id=None,
                            lease_expires_at=None,
                            heartbeat_at=None,
                            status_msg='Execution owner lost; reconciling',
                            updated_at=now))
                connection.execute(
                    sqlalchemy.update(QUEUE).where(
                        QUEUE.c.request_id == row['request_id']).values(
                            delivery_state='queued',
                            claim_generation=None,
                            available_at=now,
                            updated_at=now))
            else:
                connection.execute(
                    sqlalchemy.update(REQUESTS).where(
                        REQUESTS.c.request_id == row['request_id']).values(
                            status=requests_lib.RequestStatus.CANCELLED.value,
                            should_retry=True,
                            finished_at=now,
                            interrupted_reason=(
                                'Execution lease expired with an ambiguous '
                                'mutating outcome.'),
                            updated_at=now))
                connection.execute(
                    sqlalchemy.delete(QUEUE).where(
                        QUEUE.c.request_id == row['request_id']))

    def _candidate(
        self, connection: sqlalchemy.engine.Connection
    ) -> sqlalchemy.engine.RowMapping | None:
        return connection.execute(
            sqlalchemy.select(
                QUEUE, REQUESTS.c.execution_class,
                sqlalchemy.func.clock_timestamp().label('_database_now')).join(
                    REQUESTS,
                    REQUESTS.c.request_id == QUEUE.c.request_id).where(
                        QUEUE.c.schedule_type == self._schedule_type,
                        QUEUE.c.delivery_state == 'queued', QUEUE.c.available_at
                        <= sqlalchemy.func.clock_timestamp()).order_by(
                            QUEUE.c.priority.desc(),
                            QUEUE.c.sequence).limit(1)).mappings().first()

    def get(self) -> queue_base.QueueItem | None:
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            self._reap_expired_claims(connection)
            candidate = self._candidate(connection)
        if candidate is None:
            return None

        met = True
        status_msg = None
        precondition_error: BaseException | None = None
        deadline = candidate['precondition_deadline']
        if deadline is not None and deadline <= candidate['_database_now']:
            precondition_error = TimeoutError(
                f'Request {candidate["request_id"]} precondition timed out.')
        elif candidate['precondition_type'] is not None:
            try:
                met, status_msg = preconditions.check_once(
                    candidate['precondition_type'],
                    candidate['precondition_payload'], candidate['request_id'])
            except BaseException as e:  # pylint: disable=broad-exception-caught
                precondition_error = e

        with engine.begin() as connection:
            locked = connection.execute(
                sqlalchemy.select(QUEUE, REQUESTS).join(
                    REQUESTS,
                    REQUESTS.c.request_id == QUEUE.c.request_id).where(
                        QUEUE.c.request_id == candidate['request_id'],
                        QUEUE.c.delivery_state == 'queued', QUEUE.c.available_at
                        <= sqlalchemy.func.clock_timestamp()).with_for_update(
                            skip_locked=True)).mappings().first()
            if locked is None:
                return None
            if precondition_error is not None:
                request = _request_from_mapping(locked)
                request.status = requests_lib.RequestStatus.FAILED
                request.finished_at = time.time()
                request.set_error(precondition_error)
                values = _request_values_for_db(request)
                values.pop('request_id')
                connection.execute(
                    sqlalchemy.update(REQUESTS).where(
                        REQUESTS.c.request_id ==
                        candidate['request_id']).values(**values))
                connection.execute(
                    sqlalchemy.delete(QUEUE).where(
                        QUEUE.c.request_id == candidate['request_id']))
                return None
            if not met:
                interval = float(candidate['precondition_payload'].get(
                    'check_interval', 1))
                connection.execute(
                    sqlalchemy.update(QUEUE).where(
                        QUEUE.c.request_id == candidate['request_id']).values(
                            available_at=(sqlalchemy.func.clock_timestamp() +
                                          datetime.timedelta(seconds=interval)),
                            precondition_attempts=(
                                QUEUE.c.precondition_attempts + 1),
                            updated_at=sqlalchemy.func.clock_timestamp()))
                if status_msg is not None:
                    connection.execute(
                        sqlalchemy.update(REQUESTS).where(
                            REQUESTS.c.request_id ==
                            candidate['request_id']).values(
                                status_msg=status_msg,
                                updated_at=sqlalchemy.func.clock_timestamp()))
                return None

            generation = int(locked['execution_generation']) + 1
            token = uuid.uuid4()
            result = connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == candidate['request_id'],
                    REQUESTS.c.status.in_([
                        status.value for status in
                        requests_lib.RequestStatus.executable_statuses()
                    ])).values(
                        execution_generation=generation,
                        claim_token=token,
                        worker_instance_id=uuid.UUID(self._instance_id),
                        lease_expires_at=(
                            sqlalchemy.func.clock_timestamp() +
                            datetime.timedelta(seconds=_CLAIM_LEASE_SECONDS)),
                        heartbeat_at=sqlalchemy.func.clock_timestamp(),
                        updated_at=sqlalchemy.func.clock_timestamp()))
            if result.rowcount != 1:
                connection.execute(
                    sqlalchemy.delete(QUEUE).where(
                        QUEUE.c.request_id == candidate['request_id']))
                return None
            connection.execute(
                sqlalchemy.update(QUEUE).where(
                    QUEUE.c.request_id == candidate['request_id']).values(
                        delivery_state='claimed',
                        claim_generation=generation,
                        updated_at=sqlalchemy.func.clock_timestamp()))
            return queue_base.QueueItem(request_id=candidate['request_id'],
                                        ignore_return_value=bool(
                                            locked['ignore_return_value']),
                                        retryable=bool(locked['retryable']),
                                        execution_generation=generation,
                                        claim_token=str(token))

    def qsize(self) -> int:
        engine = initialize_and_get_db()
        with engine.connect() as connection:
            return int(
                connection.execute(
                    sqlalchemy.select(
                        sqlalchemy.func.count()  # pylint: disable=not-callable
                    ).select_from(QUEUE).where(
                        QUEUE.c.schedule_type == self._schedule_type,
                        QUEUE.c.delivery_state == 'queued')).scalar_one())


class PostgresQueueFactory(queue_base.QueueBackendFactory):
    """Create schedule-specific views over one PostgreSQL queue table."""

    def create_queue(self, schedule_type: str) -> queue_base.QueueBackend:
        return PostgresQueueBackend(schedule_type)
