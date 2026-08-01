"""PostgreSQL request persistence and leased queue delivery."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Generator
import contextlib
import datetime
import os
import signal
import threading
import time
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext import asyncio as sqlalchemy_async

import sky
from sky import sky_logging
from sky.events import api_models as event_api_models
from sky.server import constants as server_constants
from sky.server import daemons
from sky.server.events import emission as event_emission
from sky.server.events import models as event_models
from sky.server.requests import postgres_schema
from sky.server.requests import preconditions
from sky.server.requests import registry as request_registry
from sky.server.requests import requests as requests_lib
from sky.server.requests import storage as request_storage
from sky.server.requests.queues import base as queue_base
from sky.utils import locks
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

logger = sky_logging.init_logger(__name__)

REQUEST_BACKEND_ENV_VAR = 'SKYPILOT_API_REQUEST_BACKEND'
POSTGRES_REQUEST_BACKEND = 'postgres'
SERVER_INSTANCE_ID_ENV_VAR = 'SKYPILOT_API_SERVER_INSTANCE_ID'
SERVER_ROLE_ENV_VAR = 'SKYPILOT_API_SERVER_ROLE'
CONTROLLER_GENERATION_ENV_VAR = (server_constants.CONTROLLER_GENERATION_ENV_VAR)
CONTROLLER_INSTANCE_ID_ENV_VAR = (
    server_constants.CONTROLLER_INSTANCE_ID_ENV_VAR)
ROLE_DRAIN_MARKER_PATH = '/var/run/skypilot/draining'

_CLAIM_LEASE_SECONDS = 30
_CLAIM_HEARTBEAT_INTERVAL_SECONDS = 10
_MAX_EXPIRED_CLAIMS_PER_SWEEP = 100
_INSTANCE_HEARTBEAT_INTERVAL_SECONDS = 5
_INSTANCE_STALE_AFTER_SECONDS = 20
_VALID_SERVER_ROLES = frozenset({'all', 'api', 'executor', 'controller'})
_CONTROLLER_LEADERSHIP_KEY = 'api-controller'
_CONTROLLER_LEADER_LOCK_ID = 'skypilot:api-controller-leader:v1'
_CONTROLLER_GENERATION_LOCK_PREFIX = ('skypilot:api-controller-generation:v1:')


def role_is_draining() -> bool:
    """Return whether Kubernetes has started the pod drain interval."""
    return os.path.exists(ROLE_DRAIN_MARKER_PATH)


_METADATA = postgres_schema.metadata
REQUESTS = postgres_schema.REQUESTS
QUEUE = postgres_schema.QUEUE
STORE_METADATA = postgres_schema.STORE_METADATA
SERVER_INSTANCES = postgres_schema.SERVER_INSTANCES
CONTROLLER_LEADERSHIP = postgres_schema.CONTROLLER_LEADERSHIP
CONTROLLER_ACTION_RESERVATIONS = (
    postgres_schema.CONTROLLER_ACTION_RESERVATIONS)
_PG_LOCKS = postgres_schema.PG_LOCKS

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


def _validate_server_role(role: str) -> str:
    if role not in _VALID_SERVER_ROLES:
        raise ValueError(f'Invalid API server role {role!r}; expected one of '
                         f'{sorted(_VALID_SERVER_ROLES)}.')
    return role


def _supported_handlers(role: str) -> list[str]:
    registrations = request_registry.registered_handlers()
    if role == 'api':
        return []
    if role == 'controller':
        return sorted(registration.name
                      for registration in registrations
                      if registration.execution_class is
                      request_registry.ExecutionClass.CONTROLLER)
    if role == 'executor':
        return sorted(registration.name
                      for registration in registrations
                      if registration.execution_class is
                      request_registry.ExecutionClass.NORMAL)
    # The compatibility all-role process remains the only consumer for both
    # execution classes.
    return sorted(registration.name for registration in registrations)


def _supported_payload_versions() -> dict[str, dict[str, int]]:
    return {
        requests_lib.DURABLE_PAYLOAD_FORMAT: {
            'minimum': requests_lib.DURABLE_PAYLOAD_VERSION,
            'maximum': requests_lib.DURABLE_PAYLOAD_VERSION,
        }
    }


class ServerInstanceLease:
    """PostgreSQL-backed liveness and readiness for one role supervisor."""

    def __init__(
        self,
        role: str,
        *,
        heartbeat_interval_seconds: float = (
            _INSTANCE_HEARTBEAT_INTERVAL_SECONDS),
        stale_after_seconds: float = _INSTANCE_STALE_AFTER_SECONDS,
    ) -> None:
        self.role = _validate_server_role(role)
        self.instance_id = ensure_server_instance_id()
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._stale_after_seconds = stale_after_seconds
        self._ready = False
        self._draining = False
        self._health_detail: dict[str, Any] = {'phase': 'initializing'}
        self._last_success_monotonic: float | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._heartbeat_lock = threading.Lock()

    def _values(self, *, include_started_at: bool) -> dict[str, Any]:
        now = sqlalchemy.func.clock_timestamp()
        with self._state_lock:
            ready = self._ready
            draining = self._draining
            health_detail = dict(self._health_detail)
        if role_is_draining():
            draining = True
            ready = False
            health_detail = {'phase': 'draining'}
        values: dict[str, Any] = {
            'instance_id': uuid.UUID(self.instance_id),
            'role': self.role,
            'pod_name': os.environ.get('HOSTNAME'),
            'pod_uid': os.environ.get('SKYPILOT_POD_UID'),
            'pod_ip': os.environ.get('POD_IP'),
            'version': sky.__version__,
            'heartbeat_at': now,
            'draining_at': now if draining else None,
            'ready': ready and not draining,
            'health_detail': health_detail,
            'supported_handlers': _supported_handlers(self.role),
            'supported_payload_versions': _supported_payload_versions(),
        }
        if include_started_at:
            values['started_at'] = now
        return values

    def _record_heartbeat_success(self) -> None:
        with self._state_lock:
            self._last_success_monotonic = time.monotonic()

    def _register_unlocked(self) -> None:
        engine = initialize_and_get_db()
        values = self._values(include_started_at=True)
        update_values = dict(values)
        update_values.pop('instance_id')
        with engine.begin() as connection:
            connection.execute(
                postgresql.insert(SERVER_INSTANCES).values(
                    **values).on_conflict_do_update(
                        index_elements=[SERVER_INSTANCES.c.instance_id],
                        set_=update_values))
        self._record_heartbeat_success()

    def _register(self) -> None:
        with self._heartbeat_lock:
            self._register_unlocked()

    def _heartbeat(self, *, lock_timeout: float | None = None) -> bool:
        if lock_timeout is None:
            acquired = self._heartbeat_lock.acquire()
        else:
            acquired = self._heartbeat_lock.acquire(timeout=lock_timeout)
        if not acquired:
            return False
        try:
            engine = initialize_and_get_db()
            values = self._values(include_started_at=False)
            values.pop('instance_id')
            with engine.begin() as connection:
                result = connection.execute(
                    sqlalchemy.update(SERVER_INSTANCES).where(
                        SERVER_INSTANCES.c.instance_id == uuid.UUID(
                            self.instance_id)).values(**values))
            if result.rowcount != 1:
                self._register_unlocked()
                return True
            self._record_heartbeat_success()
            return True
        finally:
            self._heartbeat_lock.release()

    def start(self) -> None:
        """Register the instance and begin heartbeating."""
        self._register()
        if self._thread is not None:
            return

        def heartbeat_loop() -> None:
            while not self._stop_event.wait(self._heartbeat_interval_seconds):
                try:
                    self._heartbeat()
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(f'Failed to heartbeat {self.role} instance '
                                   f'{self.instance_id}: {e}')

        self._thread = threading.Thread(
            target=heartbeat_loop,
            name=f'skypilot-{self.role}-instance-heartbeat',
            daemon=True)
        self._thread.start()

    def set_ready(self,
                  ready: bool,
                  *,
                  health_detail: dict[str, Any] | None = None) -> None:
        """Publish an immediate readiness transition."""
        with self._state_lock:
            self._ready = ready
            if health_detail is not None:
                self._health_detail = health_detail
        self._heartbeat()

    def is_locally_ready(self) -> bool:
        """Return readiness using the latest successful database heartbeat."""
        if role_is_draining():
            return False
        with self._state_lock:
            last_success = self._last_success_monotonic
            return (self._ready and not self._draining and
                    last_success is not None and time.monotonic() - last_success
                    <= self._stale_after_seconds)

    def stop(self) -> None:
        """Mark the instance draining before stopping its heartbeat."""
        with self._state_lock:
            self._draining = True
            self._ready = False
            self._health_detail = {'phase': 'draining'}
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1, self._heartbeat_interval_seconds *
                                          2))
        try:
            if not self._heartbeat(lock_timeout=1):
                logger.warning(f'Timed out marking {self.role} instance '
                               f'{self.instance_id} draining.')
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(f'Failed to mark {self.role} instance '
                           f'{self.instance_id} draining: {e}')


def current_instance_is_ready() -> bool:
    """Check the current supervisor's durable readiness using the DB clock."""
    if role_is_draining():
        return False
    instance_id = uuid.UUID(ensure_server_instance_id())
    engine = initialize_and_get_db()
    with engine.connect() as connection:
        return bool(
            connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(SERVER_INSTANCES).where(
                    SERVER_INSTANCES.c.instance_id == instance_id,
                    SERVER_INSTANCES.c.ready,
                    SERVER_INSTANCES.c.draining_at.is_(None),
                    SERVER_INSTANCES.c.heartbeat_at
                    >= sqlalchemy.func.clock_timestamp() - datetime.timedelta(
                        seconds=_INSTANCE_STALE_AFTER_SECONDS))).scalar_one())


def recent_legacy_controller_consumers(quiescence_seconds: float) -> list[str]:
    """Return recent all/executor instances that can claim controller work.

    M2 executors advertised controller handlers and marked themselves draining
    before their worker pools had fully exited. M3 leaders therefore wait for a
    full termination-grace window after the last such heartbeat, not merely for
    Ready=false, before starting the specialized controller pool.
    """
    if quiescence_seconds < 0:
        raise ValueError('Controller cutover quiescence must be non-negative.')
    controller_handlers = frozenset(_supported_handlers('controller'))
    engine = initialize_and_get_db()
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                SERVER_INSTANCES.c.instance_id,
                SERVER_INSTANCES.c.supported_handlers).where(
                    SERVER_INSTANCES.c.role.in_(['all', 'executor']),
                    SERVER_INSTANCES.c.heartbeat_at
                    >= sqlalchemy.func.clock_timestamp() - datetime.timedelta(
                        seconds=quiescence_seconds))).mappings().all()
    blockers = []
    for row in rows:
        advertised = row['supported_handlers']
        if not isinstance(advertised, list):
            blockers.append(str(row['instance_id']))
            continue
        if controller_handlers.intersection(advertised):
            blockers.append(str(row['instance_id']))
    return blockers


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


_DB_MANAGER = db_utils.DatabaseManager('api_requests',
                                       _initialize_schema,
                                       engine_namespace='api-requests-control')


def initialize_and_get_db() -> sqlalchemy.engine.Engine:
    """Initialize the request schema and return its synchronous engine."""
    return _DB_MANAGER.get_engine()


async def _get_async_engine() -> sqlalchemy_async.AsyncEngine:
    return await _DB_MANAGER.get_async_engine()


def _controller_owner_from_environment() -> tuple[str, int] | None:
    """Return the split-role controller owner, or None in compatibility mode."""
    role = os.environ.get(SERVER_ROLE_ENV_VAR, 'all')
    if role == 'all':
        return None
    if role != 'controller':
        raise RuntimeError(
            f'Controller-owned work cannot run in the {role!r} server role.')
    instance_id = os.environ.get(CONTROLLER_INSTANCE_ID_ENV_VAR)
    generation = os.environ.get(CONTROLLER_GENERATION_ENV_VAR)
    if instance_id is None or generation is None:
        raise RuntimeError(
            'The controller role has no active leadership generation.')
    try:
        uuid.UUID(instance_id)
        parsed_generation = int(generation)
    except (TypeError, ValueError) as e:
        raise RuntimeError(
            'The controller leadership identity is invalid.') from e
    if parsed_generation <= 0:
        raise RuntimeError('The controller leadership generation must be '
                           'positive.')
    return instance_id, parsed_generation


def controller_owner_from_environment() -> tuple[str, int] | None:
    """Return the current split-role controller identity, if one is active.

    This public seam lets controller-owned subsystems persist the same outer
    generation without duplicating environment parsing or treating a
    pod-local PID as a durable owner.
    """
    return _controller_owner_from_environment()


def _pg_advisory_lock_key(
    lock: sqlalchemy.sql.Alias,) -> sqlalchemy.ColumnElement[int]:
    """Reconstruct an int8 advisory key from one ``pg_locks`` row."""
    high_bits = sqlalchemy.cast(lock.c.classid, sqlalchemy.BigInteger)
    low_bits = sqlalchemy.cast(lock.c.objid, sqlalchemy.BigInteger)
    shift = sqlalchemy.literal(32, type_=sqlalchemy.Integer())
    return high_bits.op('<<')(shift).op('|')(low_bits)


def _controller_session_locks_are_live() -> sqlalchemy.ColumnElement[bool]:
    """Prove both controller locks are held by the recorded PG session."""
    election_lock = _PG_LOCKS.alias('controller_election_lock')
    generation_lock = _PG_LOCKS.alias('controller_generation_lock')
    base_lock_key = locks.postgres_lock_key(_CONTROLLER_LEADER_LOCK_ID)
    statement = sqlalchemy.select(sqlalchemy.literal(1)).select_from(
        election_lock.join(
            generation_lock,
            generation_lock.c.pid == election_lock.c.pid)).where(
                election_lock.c.pid == CONTROLLER_LEADERSHIP.c.lock_backend_pid,
                election_lock.c.locktype == 'advisory',
                election_lock.c.objsubid == 1,
                election_lock.c.mode == 'ExclusiveLock',
                election_lock.c.granted,
                _pg_advisory_lock_key(election_lock) == base_lock_key,
                generation_lock.c.locktype == 'advisory',
                generation_lock.c.objsubid == 1,
                generation_lock.c.mode == 'ExclusiveLock',
                generation_lock.c.granted,
                _pg_advisory_lock_key(generation_lock) ==
                CONTROLLER_LEADERSHIP.c.generation_lock_key)
    return sqlalchemy.exists(statement)


def _controller_leadership_is_current_predicate(
    instance_id: Any,
    generation: Any,
) -> sqlalchemy.ColumnElement[bool]:
    """Build the row and live-session controller ownership predicate."""
    return sqlalchemy.and_(
        CONTROLLER_LEADERSHIP.c.leadership_key == _CONTROLLER_LEADERSHIP_KEY,
        CONTROLLER_LEADERSHIP.c.generation == generation,
        CONTROLLER_LEADERSHIP.c.instance_id == instance_id,
        CONTROLLER_LEADERSHIP.c.released_at.is_(None),
        _controller_session_locks_are_live(),
    )


def _current_controller_leadership_statement(
    instance_id: str,
    generation: int,
    *,
    lock: bool = False,
) -> sqlalchemy.sql.Select:
    """Build the durable ownership lookup used to serialize leader writes."""
    statement = sqlalchemy.select(CONTROLLER_LEADERSHIP.c.generation).where(
        _controller_leadership_is_current_predicate(uuid.UUID(instance_id),
                                                    generation))
    if lock:
        # FOR SHARE makes generation advancement wait for this short
        # transaction. A stale generation either commits before the handoff or
        # observes the replacement generation and fails closed.
        statement = statement.with_for_update(read=True)
    return statement


def current_controller_leadership_statement(
    instance_id: str,
    generation: int,
    *,
    lock: bool = False,
) -> sqlalchemy.sql.Select:
    """Build the live controller ownership lookup for a caller transaction.

    With ``lock=True`` the caller holds a shared lock on the singleton
    leadership row until its transaction commits. Generation advancement uses
    an update of that row, so a subsystem claim and a handoff have one
    serialization point even when they use different SQLAlchemy engines.
    """
    return _current_controller_leadership_statement(instance_id,
                                                    generation,
                                                    lock=lock)


def _lock_current_controller_leadership(
        connection: sqlalchemy.engine.Connection, instance_id: str,
        generation: int) -> bool:
    return connection.execute(
        _current_controller_leadership_statement(
            instance_id, generation,
            lock=True)).scalar_one_or_none() is not None


async def _lock_environment_controller_leadership(
        connection: sqlalchemy_async.AsyncConnection) -> None:
    """Serialize a controller-maintenance write with generation advancement."""
    owner = _controller_owner_from_environment()
    if owner is None:
        return
    result = await connection.execute(
        _current_controller_leadership_statement(*owner, lock=True))
    if result.scalar_one_or_none() is None:
        raise RuntimeError('Controller leadership changed before the durable '
                           'maintenance write.')


class ControllerLeaderLease:
    """Dedicated-session controller leadership with a durable generation."""

    def __init__(self, instance_id: str | None = None) -> None:
        self.instance_id = instance_id or ensure_server_instance_id()
        self.generation: int | None = None
        self._generation_lock_key: int | None = None
        self._lock = locks.PostgresLock(_CONTROLLER_LEADER_LOCK_ID, timeout=0)

    def _advance_generation(self, connection: Any) -> tuple[int, int]:
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO api_controller_leadership (
                    leadership_key, generation, instance_id,
                    lock_backend_pid, generation_lock_key, acquired_at,
                    heartbeat_at, released_at
                )
                VALUES (%s, 1, %s::uuid, pg_backend_pid(), 0,
                        clock_timestamp(), clock_timestamp(), NULL)
                ON CONFLICT (leadership_key) DO UPDATE SET
                    generation =
                        api_controller_leadership.generation + 1,
                    instance_id = EXCLUDED.instance_id,
                    lock_backend_pid = pg_backend_pid(),
                    generation_lock_key = 0,
                    acquired_at = clock_timestamp(),
                    heartbeat_at = clock_timestamp(),
                    released_at = NULL
                RETURNING generation
                """, (_CONTROLLER_LEADERSHIP_KEY, self.instance_id))
            generation = int(cursor.fetchone()[0])
            generation_lock_key = locks.postgres_lock_key(
                f'{_CONTROLLER_GENERATION_LOCK_PREFIX}{generation}')
            if generation_lock_key == locks.postgres_lock_key(
                    _CONTROLLER_LEADER_LOCK_ID):
                raise RuntimeError(
                    'Controller election and generation lock keys collided.')
            cursor.execute('SELECT pg_try_advisory_lock(%s)',
                           (generation_lock_key,))
            if not bool(cursor.fetchone()[0]):
                raise RuntimeError(
                    'Failed to acquire the controller generation lock.')
            cursor.execute(
                """
                UPDATE api_controller_leadership
                SET generation_lock_key = %s
                WHERE leadership_key = %s
                  AND generation = %s
                  AND instance_id = %s::uuid
                  AND lock_backend_pid = pg_backend_pid()
                  AND released_at IS NULL
                """, (generation_lock_key, _CONTROLLER_LEADERSHIP_KEY,
                      generation, self.instance_id))
            if cursor.rowcount != 1:
                raise RuntimeError(
                    'Controller generation changed during lock binding.')
            connection.commit()
            return generation, generation_lock_key
        except BaseException:
            connection.rollback()
            raise
        finally:
            cursor.close()

    def try_acquire(self) -> bool:
        """Acquire leadership without blocking and advance its generation."""
        if self.generation is not None:
            return True
        try:
            self._lock.acquire(blocking=False)
        except locks.LockTimeout:
            return False
        try:
            generation, generation_lock_key = self._lock.run_in_lock_session(
                self._advance_generation)
            self.generation = generation
            self._generation_lock_key = generation_lock_key
        except BaseException:
            self._lock.release()
            raise
        logger.info('Acquired API controller leadership generation '
                    f'{self.generation} as {self.instance_id}.')
        return True

    def heartbeat(self) -> bool:
        """Prove the lock session and refresh only this durable generation."""
        if self.generation is None or not self._lock.is_session_alive():
            return False

        def update(connection: Any) -> bool:
            cursor = connection.cursor()
            try:
                assert self._generation_lock_key is not None
                cursor.execute(
                    """
                    UPDATE api_controller_leadership
                    SET heartbeat_at = clock_timestamp()
                    WHERE leadership_key = %s
                      AND generation = %s
                      AND instance_id = %s::uuid
                      AND lock_backend_pid = pg_backend_pid()
                      AND generation_lock_key = %s
                      AND released_at IS NULL
                    """, (_CONTROLLER_LEADERSHIP_KEY, self.generation,
                          self.instance_id, self._generation_lock_key))
                matched = cursor.rowcount == 1
                if matched:
                    connection.commit()
                else:
                    connection.rollback()
                return matched
            except BaseException:
                connection.rollback()
                raise
            finally:
                cursor.close()

        try:
            return self._lock.run_in_lock_session(update)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Controller leadership heartbeat failed for '
                           f'generation {self.generation}: {e}')
            return False

    def backend_pid(self) -> int | None:
        """Return the exact lock-session backend PID for failure injection."""
        if self.generation is None:
            return None

        def get_pid(connection: Any) -> int:
            cursor = connection.cursor()
            try:
                cursor.execute('SELECT pg_backend_pid()')
                pid = int(cursor.fetchone()[0])
                connection.commit()
                return pid
            except BaseException:
                connection.rollback()
                raise
            finally:
                cursor.close()

        try:
            return self._lock.run_in_lock_session(get_pid)
        except Exception:  # pylint: disable=broad-except
            return None

    def release(self) -> None:
        """Release this generation after marking it durably inactive."""
        generation = self.generation
        generation_lock_key = self._generation_lock_key
        if (generation is not None and generation_lock_key is not None and
                self._lock.is_session_alive()):

            def mark_released(connection: Any) -> None:
                cursor = connection.cursor()
                try:
                    cursor.execute(
                        """
                        UPDATE api_controller_leadership
                        SET released_at = clock_timestamp(),
                            heartbeat_at = clock_timestamp()
                        WHERE leadership_key = %s
                          AND generation = %s
                          AND instance_id = %s::uuid
                          AND lock_backend_pid = pg_backend_pid()
                          AND generation_lock_key = %s
                          AND released_at IS NULL
                        """, (_CONTROLLER_LEADERSHIP_KEY, generation,
                              self.instance_id, generation_lock_key))
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                finally:
                    cursor.close()

            try:
                self._lock.run_in_lock_session(mark_released)
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Failed to mark controller generation '
                               f'{generation} released: {e}')
        self._lock.release()
        self.generation = None
        self._generation_lock_key = None


def controller_leadership_is_current(instance_id: str, generation: int) -> bool:
    """Return whether the instance owns the current unreleased generation."""
    engine = initialize_and_get_db()
    with engine.connect() as connection:
        return connection.execute(
            _current_controller_leadership_statement(
                instance_id, generation)).scalar_one_or_none() is not None


def fence_stale_controller_claims(instance_id: str,
                                  generation: int) -> dict[str, int]:
    """Fence claims from older controller owners before this leader starts."""
    engine = initialize_and_get_db()
    replayed = 0
    interrupted = 0
    now = sqlalchemy.func.clock_timestamp()
    with engine.begin() as connection:
        if not _lock_current_controller_leadership(connection, instance_id,
                                                   generation):
            raise RuntimeError('Controller leadership changed before stale '
                               'claims could be fenced.')
        rows = connection.execute(
            sqlalchemy.select(REQUESTS, QUEUE).join(
                QUEUE, QUEUE.c.request_id == REQUESTS.c.request_id).where(
                    REQUESTS.c.execution_class ==
                    request_registry.ExecutionClass.CONTROLLER.value,
                    REQUESTS.c.status.in_([
                        status.value for status in
                        requests_lib.RequestStatus.active_statuses()
                    ]), QUEUE.c.delivery_state == 'claimed',
                    sqlalchemy.or_(
                        REQUESTS.c.worker_instance_id != uuid.UUID(instance_id),
                        REQUESTS.c.controller_generation.is_(None),
                        REQUESTS.c.controller_generation
                        != generation)).with_for_update()).mappings().all()
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
                            controller_generation=None,
                            lease_expires_at=None,
                            heartbeat_at=None,
                            status_msg='Controller leadership changed; '
                            'reconciling',
                            updated_at=now))
                connection.execute(
                    sqlalchemy.update(QUEUE).where(
                        QUEUE.c.request_id == row['request_id']).values(
                            delivery_state='queued',
                            claim_generation=None,
                            available_at=now,
                            updated_at=now))
                replayed += 1
                continue
            transitioned = _terminalize_locked_request(
                connection,
                row,
                status=requests_lib.RequestStatus.CANCELLED,
                cause=event_api_models.EventCause.CONTROLLER_LEADERSHIP_LOST,
                values={
                    'pid': None,
                    'claim_token': None,
                    'worker_instance_id': None,
                    'lease_expires_at': None,
                    'heartbeat_at': None,
                    'should_retry': True,
                    'finished_at': now,
                    'interrupted_reason':
                        ('Controller leadership changed with an ambiguous '
                         'mutating outcome.'),
                })
            if not transitioned:
                continue
            connection.execute(
                sqlalchemy.update(CONTROLLER_ACTION_RESERVATIONS).where(
                    CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id ==
                    row['request_id'],
                    CONTROLLER_ACTION_RESERVATIONS.c.state.in_(
                        ['reserved', 'running'])).values(state='ambiguous',
                                                         reconciliation_at=now,
                                                         updated_at=now))
            interrupted += 1
    return {'replayed': replayed, 'interrupted': interrupted}


async def run_distributed_singleton(
    lock_name: str,
    task_factory: Callable[[], Coroutine[Any, Any, None]],
    *,
    retry_interval_seconds: float = 5,
    connection_check_interval_seconds: float = 5,
) -> None:
    """Run one coroutine while this process owns a PostgreSQL session lock.

    The dedicated connection is also the failure detector. If the session
    becomes unusable, the owned task is cancelled before another process can
    acquire the released lock and start a replacement.
    """
    lock_statement = sqlalchemy.text(
        'SELECT pg_try_advisory_lock('
        'hashtextextended(CAST(:lock_name AS text), 0))')
    unlock_statement = sqlalchemy.text(
        'SELECT pg_advisory_unlock('
        'hashtextextended(CAST(:lock_name AS text), 0))')
    while True:
        owned_task: asyncio.Task | None = None
        try:
            engine = await _get_async_engine()
            async with engine.connect() as connection:
                acquired = bool(
                    (await
                     connection.execute(lock_statement,
                                        {'lock_name': lock_name})).scalar_one())
                if not acquired:
                    await asyncio.sleep(retry_interval_seconds)
                    continue
                logger.info(f'Acquired distributed singleton {lock_name}.')
                owned_task = asyncio.create_task(task_factory())
                try:
                    while not owned_task.done():
                        done, _ = await asyncio.wait(
                            {owned_task},
                            timeout=connection_check_interval_seconds)
                        if done:
                            await owned_task
                            return
                        await connection.execute(sqlalchemy.text('SELECT 1'))
                finally:
                    if owned_task is not None and not owned_task.done():
                        owned_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await owned_task
                    with contextlib.suppress(Exception):
                        await connection.execute(unlock_statement,
                                                 {'lock_name': lock_name})
                    logger.info(f'Released distributed singleton {lock_name}.')
        except asyncio.CancelledError:
            if owned_task is not None and not owned_task.done():
                owned_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await owned_task
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(
                f'Distributed singleton {lock_name} lost its PostgreSQL '
                f'session: {e}')
            await asyncio.sleep(retry_interval_seconds)


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


def _controller_claim_is_current() -> sqlalchemy.ColumnElement[bool]:
    """Correlated predicate fencing controller writes by durable generation."""
    current_leadership = sqlalchemy.exists().where(
        _controller_leadership_is_current_predicate(
            REQUESTS.c.worker_instance_id, REQUESTS.c.controller_generation))
    # The single-process ``all`` role is the rollback-compatible mode. It
    # predates controller generations and deliberately keeps its historical
    # unfenced controller execution until the split-role cutover is complete.
    compatibility_claim = sqlalchemy.false()
    if os.environ.get(SERVER_ROLE_ENV_VAR, 'all') == 'all':
        compatibility_claim = REQUESTS.c.controller_generation.is_(None)
    return sqlalchemy.or_(
        REQUESTS.c.execution_class
        != request_registry.ExecutionClass.CONTROLLER.value,
        compatibility_claim,
        sqlalchemy.and_(REQUESTS.c.controller_generation.is_not(None),
                        current_leadership))


def _reserve_controller_action(connection: sqlalchemy.engine.Connection,
                               request: sqlalchemy.engine.RowMapping,
                               instance_id: str,
                               controller_generation: int | None) -> bool:
    """Reserve a non-replayable controller mutation for this generation."""
    if request['execution_class'] != (
            request_registry.ExecutionClass.CONTROLLER.value):
        return True
    registration = request_registry.resolve_handler(request['handler_name'])
    if registration.replay_policy is not request_registry.ReplayPolicy.NEVER:
        return True
    if controller_generation is None:
        return False
    values = {
        'logical_action_id': request['request_id'],
        'resource_identity': request['cluster_name'] or request['request_id'],
        'action_type': request['handler_name'],
        'state': 'reserved',
        'controller_generation': controller_generation,
        'controller_instance_id': uuid.UUID(instance_id),
        'created_at': sqlalchemy.func.clock_timestamp(),
        'updated_at': sqlalchemy.func.clock_timestamp(),
    }
    inserted = connection.execute(
        postgresql.insert(CONTROLLER_ACTION_RESERVATIONS).values(
            **values).on_conflict_do_nothing(index_elements=[
                CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id
            ]).returning(CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id)
    ).scalar_one_or_none()
    if inserted is not None:
        return True
    existing = connection.execute(
        sqlalchemy.select(CONTROLLER_ACTION_RESERVATIONS).where(
            CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id ==
            request['request_id']).with_for_update()).mappings().one()
    return (existing['controller_generation'] == controller_generation and
            str(existing['controller_instance_id']) == instance_id and
            existing['state'] in ('reserved', 'running'))


def _mark_controller_action_state(
    connection: sqlalchemy.engine.Connection,
    request_id: str,
    instance_id: str,
    state: str,
) -> None:
    """Advance this controller generation's optional action reservation."""
    connection.execute(
        sqlalchemy.update(CONTROLLER_ACTION_RESERVATIONS).where(
            CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id == request_id,
            CONTROLLER_ACTION_RESERVATIONS.c.controller_instance_id ==
            uuid.UUID(instance_id),
            CONTROLLER_ACTION_RESERVATIONS.c.controller_generation ==
            sqlalchemy.select(REQUESTS.c.controller_generation).where(
                REQUESTS.c.request_id == request_id).scalar_subquery(),
            CONTROLLER_ACTION_RESERVATIONS.c.state.in_([
                'reserved', 'running'
            ])).values(state=state,
                       updated_at=sqlalchemy.func.clock_timestamp()))


def _terminalize_locked_request(
    connection: sqlalchemy.engine.Connection,
    request_row: sqlalchemy.engine.RowMapping | dict[str, Any],
    *,
    status: requests_lib.RequestStatus,
    cause: event_api_models.EventCause,
    values: dict[str, Any],
    extra_predicates: tuple[sqlalchemy.ColumnElement[bool], ...] = (),
    delete_queue: bool = True,
) -> bool:
    """Commit one guarded terminal transition and its event atomically.

    The caller must hold a row lock for ``request_row`` in this transaction.
    """
    if status not in requests_lib.RequestStatus.finished_status():
        raise ValueError(f'Not a terminal request status: {status.value}')
    existing_status = requests_lib.RequestStatus(str(request_row['status']))
    if existing_status in requests_lib.RequestStatus.finished_status():
        return False

    terminal_values = dict(values)
    terminal_values['status'] = status.value
    terminal_values['updated_at'] = sqlalchemy.func.clock_timestamp()
    result = connection.execute(
        sqlalchemy.update(REQUESTS).where(
            REQUESTS.c.request_id == request_row['request_id'],
            REQUESTS.c.status.in_([
                active.value
                for active in requests_lib.RequestStatus.active_statuses()
            ]), *extra_predicates).values(**terminal_values))
    if result.rowcount != 1:
        return False

    emission_row = dict(request_row)
    emission_row.update({
        key: value
        for key, value in terminal_values.items()
        if not isinstance(value, sqlalchemy.sql.elements.ClauseElement)
    })
    emission_row['status'] = status.value
    if delete_queue:
        connection.execute(
            sqlalchemy.delete(QUEUE).where(
                QUEUE.c.request_id == request_row['request_id']))
    # Allocate the globally ordered event sequence after all generic request
    # and delivery work, minimizing how long this transaction holds the
    # sequence metadata row lock. Caller-specific reservation updates may
    # still follow in the same short transaction.
    event_emission.emit_terminal_event(connection,
                                       emission_row,
                                       status=status.value,
                                       cause=cause)
    return True


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
            _controller_claim_is_current(),
        )

    def _claim_predicates(
            self, execution_generation: int, claim_token: uuid.UUID
    ) -> tuple[sqlalchemy.ColumnElement[bool], ...]:
        return (
            REQUESTS.c.execution_generation == execution_generation,
            REQUESTS.c.claim_token == claim_token,
            REQUESTS.c.worker_instance_id == uuid.UUID(self._instance_id),
            REQUESTS.c.lease_expires_at > sqlalchemy.func.clock_timestamp(),
            _controller_claim_is_current(),
        )

    def _update_event_context(
        self,
        request_id: str,
        update: Callable[[event_models.EventContext],
                         event_models.EventContext],
    ) -> bool:
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS.c.event_context).where(
                    REQUESTS.c.request_id == request_id,
                    REQUESTS.c.status.in_([
                        status.value for status in
                        requests_lib.RequestStatus.active_statuses()
                    ]), *self._fenced_request_predicates(
                        request_id)).with_for_update()).mappings().first()
            if row is None:
                return False
            # Requests written by an older binary during a rolling upgrade
            # intentionally opt out. They must still execute normally.
            if row['event_context'] is None:
                return True
            context = event_models.EventContext.model_validate(
                row['event_context'])
            updated = update(context)
            result = connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == request_id,
                    REQUESTS.c.status.in_([
                        status.value for status in
                        requests_lib.RequestStatus.active_statuses()
                    ]), *self._fenced_request_predicates(request_id)).values(
                        event_context=updated.durable_dict(),
                        updated_at=sqlalchemy.func.clock_timestamp()))
            if result.rowcount != 1:
                raise RuntimeError(
                    f'Operational event context update lost its execution '
                    f'fence for {request_id}.')
        return True

    def set_event_workspace(self, request_id: str, workspace: str) -> bool:

        def update(
                context: event_models.EventContext
        ) -> event_models.EventContext:
            if context.workspace is not None and context.workspace != workspace:
                raise RuntimeError(
                    f'Operational event workspace changed for {request_id}.')
            return context.with_workspace(workspace)

        return self._update_event_context(request_id, update)

    def set_event_target_id(self, request_id: str, target_id: str) -> bool:

        def update(
                context: event_models.EventContext
        ) -> event_models.EventContext:
            current = context.targets[0].id
            if current is not None and current != target_id:
                raise RuntimeError(
                    f'Operational event target changed for {request_id}.')
            return context.with_primary_target_id(target_id)

        return self._update_event_context(request_id, update)

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
                original_status = requests_lib.RequestStatus(row['status'])
                if (original_status
                        in requests_lib.RequestStatus.active_statuses() and
                        request.status
                        in requests_lib.RequestStatus.finished_status()):
                    if request.terminal_cause is None:
                        raise RuntimeError(
                            'PostgreSQL terminal request updates require a '
                            'closed terminal cause.')
                    _terminalize_locked_request(
                        connection,
                        row,
                        status=request.status,
                        cause=event_api_models.EventCause(
                            request.terminal_cause),
                        values=values,
                        extra_predicates=self._fenced_request_predicates(
                            request_id))
                elif (original_status
                      in requests_lib.RequestStatus.finished_status() and
                      request.status != original_status):
                    raise RuntimeError('A terminal PostgreSQL request cannot '
                                       'be reopened through update_request().')
                else:
                    connection.execute(
                        sqlalchemy.update(REQUESTS).where(
                            REQUESTS.c.request_id == request_id,
                            *self._fenced_request_predicates(
                                request_id)).values(**values))

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
                original_status = requests_lib.RequestStatus(row['status'])
                if (original_status
                        in requests_lib.RequestStatus.active_statuses() and
                        request.status
                        in requests_lib.RequestStatus.finished_status()):
                    if request.terminal_cause is None:
                        raise RuntimeError(
                            'PostgreSQL terminal request updates require a '
                            'closed terminal cause.')
                    await connection.run_sync(
                        lambda sync_connection: _terminalize_locked_request(
                            sync_connection,
                            row,
                            status=request.status,
                            cause=event_api_models.EventCause(request.
                                                              terminal_cause),
                            values=values,
                            extra_predicates=self._fenced_request_predicates(
                                request_id)))
                elif (original_status
                      in requests_lib.RequestStatus.finished_status() and
                      request.status != original_status):
                    raise RuntimeError('A terminal PostgreSQL request cannot '
                                       'be reopened through update_request().')
                else:
                    await connection.execute(
                        sqlalchemy.update(REQUESTS).where(
                            REQUESTS.c.request_id == request_id,
                            *self._fenced_request_predicates(
                                request_id)).values(**values))

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
            await _lock_environment_controller_leadership(connection)
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
                    'controller_generation': None,
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
            await _lock_environment_controller_leadership(connection)
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
        if status in requests_lib.RequestStatus.finished_status():
            raise ValueError('Use a cause-aware terminal transition for '
                             'PostgreSQL requests.')
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
            statement = statement.where(*self._claim_predicates(
                execution_generation, uuid.UUID(claim_token)))
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
            if result.rowcount == 1:
                _mark_controller_action_state(connection, request_id,
                                              self._instance_id, 'running')
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
            *self._claim_predicates(claim.execution_generation,
                                    uuid.UUID(claim.claim_token)),
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

    def interrupt_cancelled_claim(
            self, claim: request_storage.ExecutionClaim) -> bool:
        """Deliver durable cancellation intent to this owning executor."""
        engine = initialize_and_get_db()
        claim_token = uuid.UUID(claim.claim_token)
        with engine.begin() as connection:
            row = connection.execute(
                sqlalchemy.select(REQUESTS.c.pid).where(
                    REQUESTS.c.request_id == claim.request_id,
                    *self._claim_predicates(claim.execution_generation,
                                            claim_token), REQUESTS.c.status ==
                    requests_lib.RequestStatus.CANCELLED.value,
                    REQUESTS.c.cancel_requested_at.is_not(None),
                    REQUESTS.c.cancel_acknowledged_at.is_(
                        None)).with_for_update()).first()
            if row is None or row.pid is None:
                return False
            pid = int(row.pid)
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            # The process already observed an equivalent termination event.
            pass
        except OSError as e:
            logger.warning(
                f'Failed to interrupt cancelled request {claim.request_id} '
                f'owned by {self._instance_id}: {e}')
            return False
        with engine.begin() as connection:
            result = connection.execute(
                sqlalchemy.update(REQUESTS).where(
                    REQUESTS.c.request_id == claim.request_id,
                    *self._claim_predicates(claim.execution_generation,
                                            claim_token),
                    REQUESTS.c.cancel_requested_at.is_not(None)).values(
                        cancel_acknowledged_at=(
                            sqlalchemy.func.clock_timestamp()),
                        updated_at=sqlalchemy.func.clock_timestamp()))
        return result.rowcount == 1

    def set_request_finished(self,
                             request_id: str,
                             status: requests_lib.RequestStatus,
                             error: BaseException | None = None,
                             result: Any | None = None) -> bool:
        if status == requests_lib.RequestStatus.SUCCEEDED:
            cause = event_api_models.EventCause.HANDLER_SUCCEEDED
        elif status == requests_lib.RequestStatus.FAILED:
            cause = event_api_models.EventCause.HANDLER_FAILED
        else:
            cause = event_api_models.EventCause.EXPLICIT_CANCEL
        return self.transition_request_terminal(request_id,
                                                status,
                                                cause.value,
                                                error=error,
                                                result=result)

    def transition_request_terminal(self,
                                    request_id: str,
                                    status: requests_lib.RequestStatus,
                                    cause: str,
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
            transitioned = _terminalize_locked_request(
                connection,
                row,
                status=status,
                cause=event_api_models.EventCause(cause),
                values=values,
                extra_predicates=self._fenced_request_predicates(request_id))
            if transitioned:
                action_state = ('completed' if status
                                == requests_lib.RequestStatus.SUCCEEDED else
                                'failed')
                _mark_controller_action_state(connection, request_id,
                                              self._instance_id, action_state)
                return True
            return False

    async def set_request_finished_async(self,
                                         request_id: str,
                                         status: requests_lib.RequestStatus,
                                         error: BaseException | None = None,
                                         result: Any | None = None) -> bool:
        return await asyncio.to_thread(self.set_request_finished, request_id,
                                       status, error, result)

    async def transition_request_terminal_async(
            self,
            request_id: str,
            status: requests_lib.RequestStatus,
            cause: str,
            error: BaseException | None = None,
            result: Any | None = None) -> bool:
        return await asyncio.to_thread(self.transition_request_terminal,
                                       request_id, status, cause, error, result)

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
                transitioned = _terminalize_locked_request(
                    connection,
                    row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=event_api_models.EventCause.EXPLICIT_CANCEL,
                    values={
                        'cancel_requested_at': now,
                        'finished_at': now,
                    })
                if not transitioned:
                    continue
                cancelled.append(row['request_id'])
                connection.execute(
                    sqlalchemy.update(CONTROLLER_ACTION_RESERVATIONS).where(
                        CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id ==
                        row['request_id'],
                        CONTROLLER_ACTION_RESERVATIONS.c.state.in_(
                            ['reserved',
                             'running'])).values(state='ambiguous',
                                                 reconciliation_at=now,
                                                 updated_at=now))
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
            rows = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.status.in_(active), ~sqlalchemy.exists().where(
                        QUEUE.c.request_id == REQUESTS.c.request_id)).
                with_for_update()).mappings().all()
            for row in rows:
                now = sqlalchemy.func.clock_timestamp()
                _terminalize_locked_request(
                    connection,
                    row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=event_api_models.EventCause.COMPATIBILITY_RESTART,
                    values={
                        'should_retry': True,
                        'finished_at': now,
                        'interrupted_reason':
                            ('The compatibility process stopped while a '
                             'non-queued coroutine request was active.'),
                    })
        # Queue rows are already durable and must not be copied back through
        # the legacy in-memory re-enqueue path.
        return False


class PostgresQueueBackend(queue_base.QueueBackend):
    """One schedule-class view over the durable PostgreSQL queue."""

    def __init__(
        self,
        schedule_type: str,
        *,
        execution_classes: frozenset[str] | None = None,
        controller_generation: int | None = None,
    ):
        self._schedule_type = schedule_type
        self._instance_id = ensure_server_instance_id()
        valid_classes = frozenset(
            execution_class.value
            for execution_class in request_registry.ExecutionClass)
        if execution_classes is not None and not execution_classes:
            raise ValueError('At least one execution class must be allowed.')
        if (execution_classes is not None and
                not execution_classes.issubset(valid_classes)):
            raise ValueError('Invalid queue execution classes: '
                             f'{sorted(execution_classes - valid_classes)}')
        if (execution_classes is not None and
                request_registry.ExecutionClass.CONTROLLER.value
                in execution_classes and controller_generation is None and
                os.environ.get(SERVER_ROLE_ENV_VAR, 'all') != 'all'):
            raise ValueError('A controller-scoped queue requires an active '
                             'controller generation.')
        self._execution_classes = execution_classes
        self._controller_generation = controller_generation

    def _role_predicates(self) -> tuple[sqlalchemy.ColumnElement[bool], ...]:
        predicates: list[sqlalchemy.ColumnElement[bool]] = []
        if self._execution_classes is not None:
            predicates.append(
                REQUESTS.c.execution_class.in_(self._execution_classes))
        if self._controller_generation is not None:
            predicates.append(sqlalchemy.exists().where(
                _controller_leadership_is_current_predicate(
                    uuid.UUID(self._instance_id), self._controller_generation)))
        return tuple(predicates)

    def _lock_controller_leadership(
            self, connection: sqlalchemy.engine.Connection) -> bool:
        if self._controller_generation is None:
            return True
        return _lock_current_controller_leadership(connection,
                                                   self._instance_id,
                                                   self._controller_generation)

    def put(self, item: queue_base.QueueItemLike) -> None:
        normalized = queue_base.normalize_queue_item(item)
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            if not self._lock_controller_leadership(connection):
                logger.warning('Ignoring requeue after controller leadership '
                               f'changed for {normalized.request_id}.')
                return
            row = connection.execute(
                sqlalchemy.select(REQUESTS).where(
                    REQUESTS.c.request_id == normalized.request_id,
                    *self._role_predicates()).with_for_update()).mappings(
                    ).first()
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
                    > sqlalchemy.func.clock_timestamp(),
                    _controller_claim_is_current()).values(
                        status=requests_lib.RequestStatus.WAITING.value,
                        pid=None,
                        claim_token=None,
                        worker_instance_id=None,
                        controller_generation=None,
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
                            controller_generation=None,
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
                transitioned = _terminalize_locked_request(
                    connection,
                    row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=(event_api_models.EventCause.EXECUTION_LEASE_EXPIRED),
                    values={
                        'pid': None,
                        'claim_token': None,
                        'worker_instance_id': None,
                        'lease_expires_at': None,
                        'heartbeat_at': None,
                        'should_retry': True,
                        'finished_at': now,
                        'interrupted_reason':
                            ('Execution lease expired with an ambiguous '
                             'mutating outcome.'),
                    })
                if not transitioned:
                    continue
                if row['execution_class'] == (
                        request_registry.ExecutionClass.CONTROLLER.value):
                    connection.execute(
                        sqlalchemy.update(CONTROLLER_ACTION_RESERVATIONS).where(
                            CONTROLLER_ACTION_RESERVATIONS.c.logical_action_id
                            == row['request_id'],
                            CONTROLLER_ACTION_RESERVATIONS.c.state.in_(
                                ['reserved',
                                 'running'])).values(state='ambiguous',
                                                     reconciliation_at=now,
                                                     updated_at=now))

    def _candidate(
        self, connection: sqlalchemy.engine.Connection
    ) -> sqlalchemy.engine.RowMapping | None:
        statement = sqlalchemy.select(
            QUEUE, REQUESTS.c.execution_class,
            sqlalchemy.func.clock_timestamp().label('_database_now')).join(
                REQUESTS, REQUESTS.c.request_id == QUEUE.c.request_id).where(
                    QUEUE.c.schedule_type == self._schedule_type,
                    QUEUE.c.delivery_state == 'queued', QUEUE.c.available_at
                    <= sqlalchemy.func.clock_timestamp(),
                    *self._role_predicates())
        return connection.execute(
            statement.order_by(QUEUE.c.priority.desc(),
                               QUEUE.c.sequence).limit(1)).mappings().first()

    def get(self) -> queue_base.QueueItem | None:
        engine = initialize_and_get_db()
        with engine.begin() as connection:
            if not self._lock_controller_leadership(connection):
                return None
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
            except Exception as e:  # pylint: disable=broad-exception-caught
                precondition_error = e

        with engine.begin() as connection:
            if not self._lock_controller_leadership(connection):
                return None
            locked = connection.execute(
                sqlalchemy.select(QUEUE, REQUESTS).join(
                    REQUESTS,
                    REQUESTS.c.request_id == QUEUE.c.request_id).where(
                        QUEUE.c.request_id == candidate['request_id'],
                        QUEUE.c.delivery_state == 'queued', QUEUE.c.available_at
                        <= sqlalchemy.func.clock_timestamp(),
                        *self._role_predicates()).with_for_update(
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
                _terminalize_locked_request(
                    connection,
                    locked,
                    status=requests_lib.RequestStatus.FAILED,
                    cause=event_api_models.EventCause.PRECONDITION_FAILED,
                    values=values)
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
            execution_class = locked['execution_class']
            controller_generation = (
                self._controller_generation if execution_class
                == request_registry.ExecutionClass.CONTROLLER.value else None)
            claim_statement = sqlalchemy.update(REQUESTS).where(
                REQUESTS.c.request_id == candidate['request_id'],
                REQUESTS.c.status.in_([
                    status.value for status in
                    requests_lib.RequestStatus.executable_statuses()
                ]))
            if controller_generation is not None:
                claim_statement = claim_statement.where(
                    sqlalchemy.exists().where(
                        _controller_leadership_is_current_predicate(
                            uuid.UUID(self._instance_id),
                            controller_generation)))
            result = connection.execute(
                claim_statement.values(
                    execution_generation=generation,
                    claim_token=token,
                    worker_instance_id=uuid.UUID(self._instance_id),
                    controller_generation=controller_generation,
                    lease_expires_at=(
                        sqlalchemy.func.clock_timestamp() +
                        datetime.timedelta(seconds=_CLAIM_LEASE_SECONDS)),
                    heartbeat_at=sqlalchemy.func.clock_timestamp(),
                    updated_at=sqlalchemy.func.clock_timestamp()))
            if result.rowcount != 1:
                return None
            if (controller_generation is not None and
                    not _reserve_controller_action(connection, locked,
                                                   self._instance_id,
                                                   controller_generation)):
                now = sqlalchemy.func.clock_timestamp()
                reservation_row = dict(locked)
                reservation_row.update({
                    'execution_generation': generation,
                    'claim_token': token,
                    'worker_instance_id': uuid.UUID(self._instance_id),
                    'controller_generation': controller_generation,
                })
                _terminalize_locked_request(
                    connection,
                    reservation_row,
                    status=requests_lib.RequestStatus.CANCELLED,
                    cause=(event_api_models.EventCause.
                           CONTROLLER_RESERVATION_CONFLICT),
                    values={
                        'pid': None,
                        'claim_token': None,
                        'worker_instance_id': None,
                        'lease_expires_at': None,
                        'heartbeat_at': None,
                        'should_retry': True,
                        'finished_at': now,
                        'interrupted_reason':
                            ('Controller action is already owned by a '
                             'different leadership generation.'),
                    })
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
        statement = sqlalchemy.select(
            sqlalchemy.func.count()  # pylint: disable=not-callable
        ).select_from(
            QUEUE.join(REQUESTS,
                       REQUESTS.c.request_id == QUEUE.c.request_id)).where(
                           QUEUE.c.schedule_type == self._schedule_type,
                           QUEUE.c.delivery_state == 'queued')
        if self._execution_classes is not None:
            statement = statement.where(
                REQUESTS.c.execution_class.in_(self._execution_classes))
        with engine.connect() as connection:
            return int(connection.execute(statement).scalar_one())


class PostgresQueueFactory(queue_base.QueueBackendFactory):
    """Create schedule-specific views over one PostgreSQL queue table."""

    def __init__(
        self,
        *,
        execution_classes: frozenset[str] | None = None,
        controller_generation: int | None = None,
    ) -> None:
        self._execution_classes = execution_classes
        self._controller_generation = controller_generation

    def create_queue(self, schedule_type: str) -> queue_base.QueueBackend:
        return PostgresQueueBackend(
            schedule_type,
            execution_classes=self._execution_classes,
            controller_generation=self._controller_generation)
