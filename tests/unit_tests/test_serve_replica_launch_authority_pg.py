"""Real-PostgreSQL tests for SkyServe replica-launch authority guards."""
# pylint: disable=protected-access,redefined-outer-name,unused-import
# pylint: disable=unexpected-keyword-arg

import concurrent.futures
import contextlib
import hashlib
import pickle
import threading
import time
import typing

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_serve_resource_action_state_pg import postgres_engine

from sky import global_user_state
from sky import global_user_state_schema
from sky.serve import constants
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import service_spec
from sky.utils import locks
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_SERVICE_NAME = 'launch-authority-primary'
_OTHER_SERVICE_NAME = 'launch-authority-peer'
_SERVICE_HASH = '11111111-1111-4111-8111-111111111111'
_OTHER_SERVICE_HASH = '22222222-2222-4222-8222-222222222222'
_RECREATED_SERVICE_HASH = '33333333-3333-4333-8333-333333333333'
_CONTROLLER_PID = 123
_CONTROLLER_IP = '10.0.0.1'
_LIFECYCLE_EPOCH = 4
_REPLACEMENT_CONTROLLER_PID = 456
_REPLACEMENT_CONTROLLER_IP = '10.0.0.2'
_WAIT_TIMEOUT_SECONDS = 10


def _service_spec() -> service_spec.SkyServiceSpec:
    return service_spec.SkyServiceSpec(
        readiness_path='/health',
        initial_delay_seconds=0,
        readiness_timeout_seconds=5,
        endpoint_probe_interval_seconds=1,
        lb_stream_timeout_seconds=10,
        min_replicas=1,
        lb_high_availability=False,
    )


@pytest.fixture
def launch_authority_database(postgres_engine, monkeypatch):  # noqa: F811
    """Install an isolated real PostgreSQL database as the Serve database."""
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    # Version elections read the alembic revision and the Kueue admission
    # tables (#1659), so the fixture must carry the migrated schema rather
    # than bare metadata.  Serve055 requires the global users(id) table.
    global_user_state_schema.user_table.create(postgres_engine, checkfirst=True)
    config = migration_utils.get_alembic_config(postgres_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, migration_utils.SERVE_VERSION)
    # Final deletion sweeps the bound launch requests of the service.
    migration_utils.safe_alembic_upgrade(postgres_engine,
                                         migration_utils.API_REQUESTS_DB_NAME,
                                         migration_utils.API_REQUESTS_VERSION)
    monkeypatch.setattr(serve_state._db_manager, '_engine', postgres_engine)
    _seed_service(postgres_engine, _SERVICE_NAME, _SERVICE_HASH)
    _seed_service(postgres_engine, _OTHER_SERVICE_NAME, _OTHER_SERVICE_HASH)
    return postgres_engine


def _seed_service(engine: sqlalchemy.engine.Engine, service_name: str,
                  service_hash: str) -> None:
    config, config_digest, snapshot_id = _controller_config_snapshot(
        service_name, 1)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name=service_name, epoch=_LIFECYCLE_EPOCH))
        connection.execute(serve_state.services_table.insert().values(
            name=service_name,
            status=serve_state.ServiceStatus.READY.value,
            current_version=1,
            pool=0,
            controller_pid=_CONTROLLER_PID,
            controller_ip=_CONTROLLER_IP,
            hash=service_hash,
            lifecycle_epoch=_LIFECYCLE_EPOCH,
            logical_replica_semantics=0,
            lb_ha_enabled=0))
        connection.execute(serve_state.version_specs_table.insert().values(
            service_name=service_name,
            version=1,
            spec=pickle.dumps(_service_spec()),
            yaml_content='service: v1\n',
            controller_config=config,
            controller_config_digest=config_digest,
            controller_config_snapshot_id=snapshot_id,
            controller_applied_at=time.time()))


def _mark_shutting_down(engine: sqlalchemy.engine.Engine,
                        service_name: str) -> None:
    """Move a seeded service into the lifecycle final deletion requires."""
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == service_name).values(
                    status=serve_state.ServiceStatus.SHUTTING_DOWN.value))


def _controller_config_snapshot(
        service_name: str,
        version: int) -> serve_state.ControllerConfigSnapshot:
    config = f'config-for-{service_name}-v{version}'.encode()
    digest = hashlib.sha256(config).hexdigest()
    snapshot_id = hashlib.sha256(
        f'snapshot-for-{service_name}-v{version}'.encode()).hexdigest()
    return config, digest, snapshot_id


def _launch_context(
        service_name: str,
        service_hash: str,
        version: int = 1,
        controller_pid: int = _CONTROLLER_PID,
        controller_ip: str = _CONTROLLER_IP) -> dict[str, typing.Any]:
    return {
        constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: service_name,
        constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: service_hash,
        constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: version,
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: controller_pid,
        constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: controller_ip,
    }


def _commit_version(service_name: str,
                    version: int = 2) -> serve_state.VersionCommitResult:
    config, digest, snapshot_id = _controller_config_snapshot(
        service_name, version)
    return serve_state.add_or_update_version(
        service_name,
        version,
        _service_spec(),
        f'service: v{version}\n',
        ha_recovery_script=(
            f'#!/bin/sh\n{constants.VERSIONED_HA_CONFIG_RECOVERY_MARKER}\n'),
        controller_config=config,
        controller_config_digest=digest,
        controller_config_snapshot_id=snapshot_id,
    )


def _create_service(service_name: str, service_hash: str) -> bool:
    config, digest, snapshot_id = _controller_config_snapshot(service_name, 1)
    return serve_state.add_service(
        service_name,
        controller_job_id=1,
        policy='round-robin',
        requested_resources_str='H200:1',
        load_balancing_policy='round_robin',
        status=serve_state.ServiceStatus.READY,
        tls_encrypted=False,
        pool=False,
        controller_pid=_REPLACEMENT_CONTROLLER_PID,
        entrypoint='python app.py',
        spec=_service_spec(),
        yaml_content='service: v1\n',
        controller_ip=_REPLACEMENT_CONTROLLER_IP,
        service_hash=service_hash,
        controller_config=config,
        controller_config_digest=digest,
        controller_config_snapshot_id=snapshot_id,
    )


def _advisory_lock_key(engine: sqlalchemy.engine.Engine,
                       service_name: str) -> int:
    lock_id = serve_state._replica_launch_authority_lock_id(
        service_name, engine)
    return locks.postgres_lock_key(lock_id)


def _advisory_lock_count(engine: sqlalchemy.engine.Engine, lock_key: int,
                         mode: str, granted: bool) -> int:
    with engine.connect() as connection:
        return int(
            connection.execute(
                sqlalchemy.text(
                    'SELECT count(*) FROM pg_locks '
                    "WHERE locktype = 'advisory' "
                    'AND database = ('
                    '  SELECT oid FROM pg_database '
                    '  WHERE datname = current_database()'
                    ') '
                    'AND ((classid::bigint << 32) | objid::bigint) = '
                    ':lock_key '
                    'AND mode = :mode AND granted = :granted'), {
                        'lock_key': lock_key,
                        'mode': mode,
                        'granted': granted,
                    }).scalar_one())


def _wait_for_advisory_lock(engine: sqlalchemy.engine.Engine, lock_key: int,
                            mode: str, granted: bool) -> None:
    deadline = time.monotonic() + _WAIT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if _advisory_lock_count(engine, lock_key, mode, granted) > 0:
            return
        time.sleep(0.02)
    raise AssertionError('Timed out waiting for PostgreSQL advisory lock '
                         f'key={lock_key}, mode={mode}, granted={granted}.')


def _run_writer_behind_shared_guard(
    engine: sqlalchemy.engine.Engine,
    service_name: str,
    writer: typing.Callable[[], typing.Any],
) -> typing.Any:
    writer_started = threading.Event()

    def observed_writer() -> typing.Any:
        writer_started.set()
        return writer()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        with serve_state.service_replica_launch_authority_guard(service_name):
            future = executor.submit(observed_writer)
            assert writer_started.wait(_WAIT_TIMEOUT_SECONDS)
            _wait_for_advisory_lock(engine,
                                    _advisory_lock_key(engine, service_name),
                                    'ExclusiveLock', False)
            assert not future.done()
        return future.result(timeout=_WAIT_TIMEOUT_SECONDS)


def test_shared_provider_guard_blocks_same_service_version_commit(
        launch_authority_database):
    engine = launch_authority_database
    writer_started = threading.Event()

    def writer() -> serve_state.VersionCommitResult:
        writer_started.set()
        return _commit_version(_SERVICE_NAME)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        with serve_state.service_replica_launch_authority_guard(_SERVICE_NAME):
            future = executor.submit(writer)
            assert writer_started.wait(_WAIT_TIMEOUT_SECONDS)
            _wait_for_advisory_lock(engine,
                                    _advisory_lock_key(engine, _SERVICE_NAME),
                                    'ExclusiveLock', False)
            assert not future.done()
            assert serve_state.service_replica_launch_fence_holds(
                _launch_context(_SERVICE_NAME, _SERVICE_HASH))

        assert future.result(timeout=_WAIT_TIMEOUT_SECONDS
                            ) is serve_state.VersionCommitResult.COMMITTED

    assert not serve_state.service_replica_launch_fence_holds(
        _launch_context(_SERVICE_NAME, _SERVICE_HASH))
    assert serve_state.service_replica_launch_fence_holds(
        _launch_context(_SERVICE_NAME, _SERVICE_HASH, 2))


def test_shared_provider_guard_blocks_quarantine_and_stales_reader(
        launch_authority_database):
    engine = launch_authority_database
    assert _commit_version(
        _SERVICE_NAME) is serve_state.VersionCommitResult.COMMITTED
    version_two = _launch_context(_SERVICE_NAME, _SERVICE_HASH, 2)
    assert serve_state.service_replica_launch_fence_holds(version_two)

    result = _run_writer_behind_shared_guard(
        engine, _SERVICE_NAME,
        lambda: serve_state.quarantine_version(_SERVICE_NAME, 2, 'invalid'))

    assert result is True
    assert not serve_state.service_replica_launch_fence_holds(version_two)
    assert serve_state.service_replica_launch_fence_holds(
        _launch_context(_SERVICE_NAME, _SERVICE_HASH))


def test_shared_provider_guard_blocks_applied_receipt_election(
        launch_authority_database):
    engine = launch_authority_database
    assert _commit_version(_SERVICE_NAME,
                           2) is serve_state.VersionCommitResult.COMMITTED
    assert _commit_version(_SERVICE_NAME,
                           3) is serve_state.VersionCommitResult.COMMITTED
    assert serve_state.quarantine_version(_SERVICE_NAME, 3, 'invalid')
    version_one = _launch_context(_SERVICE_NAME, _SERVICE_HASH)
    version_two = _launch_context(_SERVICE_NAME, _SERVICE_HASH, 2)
    assert serve_state.service_replica_launch_fence_holds(version_one)
    assert not serve_state.service_replica_launch_fence_holds(version_two)

    result = _run_writer_behind_shared_guard(
        engine, _SERVICE_NAME,
        lambda: serve_state.mark_version_controller_applied(
            _SERVICE_NAME, 2, _SERVICE_HASH, (_CONTROLLER_PID, _CONTROLLER_IP)))

    assert result is True
    assert not serve_state.service_replica_launch_fence_holds(version_one)
    assert serve_state.service_replica_launch_fence_holds(version_two)


def test_shared_provider_guard_blocks_owner_takeover_and_stales_reader(
        launch_authority_database):
    engine = launch_authority_database
    old_owner = _launch_context(_SERVICE_NAME, _SERVICE_HASH)
    new_owner = _launch_context(_SERVICE_NAME,
                                _SERVICE_HASH,
                                controller_pid=_REPLACEMENT_CONTROLLER_PID,
                                controller_ip=_REPLACEMENT_CONTROLLER_IP)
    assert serve_state.service_replica_launch_fence_holds(old_owner)

    result = _run_writer_behind_shared_guard(
        engine, _SERVICE_NAME,
        lambda: serve_state.update_service_controller_pid_if_owner(
            _SERVICE_NAME, _SERVICE_HASH, _CONTROLLER_PID, _CONTROLLER_IP,
            _REPLACEMENT_CONTROLLER_PID, _REPLACEMENT_CONTROLLER_IP))

    assert result is True
    assert not serve_state.service_replica_launch_fence_holds(old_owner)
    assert serve_state.service_replica_launch_fence_holds(new_owner)


def test_shared_provider_guard_blocks_launch_blocking_status(
        launch_authority_database):
    engine = launch_authority_database
    launch_context = _launch_context(_SERVICE_NAME, _SERVICE_HASH)
    assert serve_state.service_replica_launch_fence_holds(launch_context)

    result = _run_writer_behind_shared_guard(
        engine, _SERVICE_NAME,
        lambda: serve_state.set_service_status_and_active_versions_if_owner(
            _SERVICE_NAME, _SERVICE_HASH, _CONTROLLER_PID, _CONTROLLER_IP,
            serve_state.ServiceStatus.SHUTTING_DOWN))

    assert result is True
    assert not serve_state.service_replica_launch_fence_holds(launch_context)


def test_provider_fence_rejects_persisted_service_launch_during_hold(
        launch_authority_database, monkeypatch):
    """A request replayed after the maintenance rollout cannot provision."""
    _ = launch_authority_database
    launch_context = _launch_context(_SERVICE_NAME, _SERVICE_HASH)
    assert serve_state.service_replica_launch_fence_holds(launch_context)

    monkeypatch.setenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR, 'true')

    assert not serve_state.service_replica_launch_fence_holds(launch_context)


def test_provider_fence_keeps_pool_launches_available_during_hold(
        launch_authority_database, monkeypatch):
    engine = launch_authority_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == _SERVICE_NAME).values(
                    pool=1))
    launch_context = _launch_context(_SERVICE_NAME, _SERVICE_HASH)
    monkeypatch.setenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR, 'true')

    assert serve_state.service_replica_launch_fence_holds(launch_context)


def test_provider_fence_rejects_noncanonical_pool_discriminator_during_hold(
        launch_authority_database, monkeypatch):
    engine = launch_authority_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == _SERVICE_NAME).values(
                    pool=2))
    launch_context = _launch_context(_SERVICE_NAME, _SERVICE_HASH)
    monkeypatch.setenv(constants.SERVE_CONTROLLER_HOLD_ENV_VAR, 'true')

    assert not serve_state.service_replica_launch_fence_holds(launch_context)


def test_shared_provider_guard_blocks_deletion_and_stales_reader(
        launch_authority_database):
    engine = launch_authority_database
    launch_context = _launch_context(_SERVICE_NAME, _SERVICE_HASH)
    assert serve_state.service_replica_launch_fence_holds(launch_context)
    _mark_shutting_down(engine, _SERVICE_NAME)

    result = _run_writer_behind_shared_guard(
        engine, _SERVICE_NAME, lambda: serve_state.remove_service_completely(
            _SERVICE_NAME, _SERVICE_HASH, (_CONTROLLER_PID, _CONTROLLER_IP)))

    assert result is True
    assert not serve_state.service_replica_launch_fence_holds(launch_context)


def test_shared_provider_guard_blocks_same_name_creation(
        launch_authority_database):
    engine = launch_authority_database
    old_context = _launch_context(_SERVICE_NAME, _SERVICE_HASH)
    _mark_shutting_down(engine, _SERVICE_NAME)
    assert serve_state.remove_service_completely(
        _SERVICE_NAME, _SERVICE_HASH, (_CONTROLLER_PID, _CONTROLLER_IP))
    assert not serve_state.service_replica_launch_fence_holds(old_context)

    result = _run_writer_behind_shared_guard(
        engine, _SERVICE_NAME,
        lambda: _create_service(_SERVICE_NAME, _RECREATED_SERVICE_HASH))

    assert result is True
    assert not serve_state.service_replica_launch_fence_holds(old_context)
    assert serve_state.service_replica_launch_fence_holds(
        _launch_context(_SERVICE_NAME,
                        _RECREATED_SERVICE_HASH,
                        controller_pid=_REPLACEMENT_CONTROLLER_PID,
                        controller_ip=_REPLACEMENT_CONTROLLER_IP))


def test_terminated_provider_guard_session_fails_validity_closed(
        launch_authority_database):
    engine = launch_authority_database
    lock_key = _advisory_lock_key(engine, _SERVICE_NAME)

    with serve_state.service_replica_launch_authority_guard(
            _SERVICE_NAME) as guard:
        assert isinstance(guard, locks.PostgresLock)
        assert serve_state.service_replica_launch_authority_guard_is_valid(
            guard)
        assert guard._connection is not None
        with contextlib.closing(guard._connection.cursor()) as cursor:
            cursor.execute('SELECT pg_backend_pid()')
            guard_backend_pid = cursor.fetchone()[0]
        guard._connection.commit()

        with engine.begin() as connection:
            terminated = connection.execute(
                sqlalchemy.text('SELECT pg_terminate_backend(:pid)'), {
                    'pid': guard_backend_pid
                }).scalar_one()
        assert terminated

        deadline = time.monotonic() + _WAIT_TIMEOUT_SECONDS
        while (serve_state.service_replica_launch_authority_guard_is_valid(
                guard) and time.monotonic() < deadline):
            time.sleep(0.02)
        assert not serve_state.service_replica_launch_authority_guard_is_valid(
            guard)

    assert _advisory_lock_count(engine, lock_key, 'ShareLock', True) == 0


class _ObservedPostgresLockClock:
    """Pause a selected contender after its first failed try-lock probe."""

    def __init__(self, later_reader: threading.local,
                 retry_waiting: threading.Event,
                 retry_allowed: threading.Event) -> None:
        self._later_reader = later_reader
        self._retry_waiting = retry_waiting
        self._retry_allowed = retry_allowed

    @staticmethod
    def monotonic() -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        if getattr(self._later_reader, 'active', False):
            self._retry_waiting.set()
            if not self._retry_allowed.wait(_WAIT_TIMEOUT_SECONDS):
                raise AssertionError('Timed out waiting to retry the shared '
                                     'replica-launch guard.')
            return
        time.sleep(seconds)


def test_queued_writer_precedes_later_reader_and_reader_rejects_stale_version(
        launch_authority_database, monkeypatch):
    engine = launch_authority_database
    later_reader = threading.local()
    retry_waiting = threading.Event()
    retry_allowed = threading.Event()
    order: list[str] = []
    order_lock = threading.Lock()
    monkeypatch.setattr(
        locks, 'time',
        _ObservedPostgresLockClock(later_reader, retry_waiting, retry_allowed))

    def writer() -> serve_state.VersionCommitResult:
        result = _commit_version(_SERVICE_NAME)
        with order_lock:
            order.append('writer')
        return result

    def reader() -> bool:
        later_reader.active = True
        with serve_state.service_replica_launch_authority_guard(_SERVICE_NAME):
            with order_lock:
                order.append('reader')
            return serve_state.service_replica_launch_fence_holds(
                _launch_context(_SERVICE_NAME, _SERVICE_HASH))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        try:
            with serve_state.service_replica_launch_authority_guard(
                    _SERVICE_NAME):
                writer_future = executor.submit(writer)
                _wait_for_advisory_lock(
                    engine, _advisory_lock_key(engine, _SERVICE_NAME),
                    'ExclusiveLock', False)
                reader_future = executor.submit(reader)
                # The later shared guard must fail its first try-lock probe
                # instead of bypassing the already queued exclusive writer.
                assert retry_waiting.wait(_WAIT_TIMEOUT_SECONDS)
                assert not writer_future.done()
                assert not reader_future.done()

            assert writer_future.result(
                timeout=_WAIT_TIMEOUT_SECONDS
            ) is serve_state.VersionCommitResult.COMMITTED
            retry_allowed.set()
            assert reader_future.result(timeout=_WAIT_TIMEOUT_SECONDS) is False
        finally:
            retry_allowed.set()

    assert order == ['writer', 'reader']


def test_two_shared_provider_guards_overlap(launch_authority_database):
    del launch_authority_database
    first_entered = threading.Event()
    second_entered = threading.Event()
    release = threading.Event()

    def reader(entered: threading.Event) -> None:
        with serve_state.service_replica_launch_authority_guard(_SERVICE_NAME):
            entered.set()
            if not release.wait(_WAIT_TIMEOUT_SECONDS):
                raise AssertionError('Timed out waiting to release readers.')

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(reader, first_entered)
        assert first_entered.wait(_WAIT_TIMEOUT_SECONDS)
        second = executor.submit(reader, second_entered)
        try:
            # The second guard enters while the first still owns its session.
            assert second_entered.wait(_WAIT_TIMEOUT_SECONDS)
            assert not first.done()
            assert not second.done()
        finally:
            release.set()
        first.result(timeout=_WAIT_TIMEOUT_SECONDS)
        second.result(timeout=_WAIT_TIMEOUT_SECONDS)


def test_different_service_writer_does_not_block(launch_authority_database):
    del launch_authority_database
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        with serve_state.service_replica_launch_authority_guard(_SERVICE_NAME):
            future = executor.submit(_commit_version, _OTHER_SERVICE_NAME)
            assert future.result(timeout=_WAIT_TIMEOUT_SECONDS
                                ) is serve_state.VersionCommitResult.COMMITTED
            assert serve_state.service_replica_launch_fence_holds(
                _launch_context(_SERVICE_NAME, _SERVICE_HASH))
            assert serve_state.service_replica_launch_fence_holds(
                _launch_context(_OTHER_SERVICE_NAME, _OTHER_SERVICE_HASH, 2))


def test_guards_and_writers_use_exact_serve_engine(launch_authority_database,
                                                   monkeypatch):
    engine = launch_authority_database
    seen_engines: list[sqlalchemy.engine.Engine] = []
    original_get_lock_engine = db_utils.get_postgres_lock_engine

    def record_lock_engine(
            candidate: sqlalchemy.engine.Engine) -> sqlalchemy.engine.Engine:
        seen_engines.append(candidate)
        return original_get_lock_engine(candidate)

    def reject_global_engine() -> typing.NoReturn:
        raise AssertionError('Replica-launch guards must not use the global '
                             'user-state database engine.')

    monkeypatch.setattr(db_utils, 'get_postgres_lock_engine',
                        record_lock_engine)
    monkeypatch.setattr(global_user_state, 'initialize_and_get_db',
                        reject_global_engine)

    with serve_state.service_replica_launch_authority_guard(
            _SERVICE_NAME) as guard:
        assert isinstance(guard, locks.PostgresLock)
        assert guard._engine is engine
        assert guard._connection is not None
        with contextlib.closing(guard._connection.cursor()) as cursor:
            cursor.execute('SELECT current_database()')
            guard_database = cursor.fetchone()[0]
        with engine.connect() as connection:
            serve_database = connection.execute(
                sqlalchemy.text('SELECT current_database()')).scalar_one()
        assert guard_database == serve_database

    assert _commit_version(
        _SERVICE_NAME) is serve_state.VersionCommitResult.COMMITTED
    assert len(seen_engines) >= 2
    assert all(candidate is engine for candidate in seen_engines)
