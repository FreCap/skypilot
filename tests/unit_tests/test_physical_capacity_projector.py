"""Focused unit tests for the bounded evidence repository/projector."""
# pylint: disable=protected-access

import datetime
import threading
import time
import types
from unittest import mock
import uuid

import pytest
import sqlalchemy

from sky.jobs import managed_job_refresh_thread
from sky.physical_capacity import config
from sky.physical_capacity import contracts
from sky.physical_capacity import hashing
from sky.physical_capacity import metrics
from sky.physical_capacity import models
from sky.physical_capacity import projector
from sky.physical_capacity import repository
from sky.server import runtime
from sky.utils.db import db_utils


def _selector() -> contracts.ServeSourceSelector:
    return contracts.ServeSourceSelector(
        workspace='default',
        source_kind=models.ProjectionSourceKind.SERVE_SERVICE,
        service_name='svc')


def _identity() -> repository.ControllerIdentity:
    return repository.ControllerIdentity(str(uuid.uuid4()), 1)


def test_isolated_engine_uses_explicit_policy_not_global(monkeypatch):
    base = sqlalchemy.create_engine(
        'postgresql+psycopg2://user:secret@localhost/database')
    isolated = mock.Mock(spec=sqlalchemy.engine.Engine)
    monkeypatch.setattr(db_utils, '_postgres_isolated_engine_cache', {})
    monkeypatch.setattr(db_utils, '_max_connections', 99)
    create = mock.Mock(return_value=isolated)
    monkeypatch.setattr(sqlalchemy, 'create_engine', create)

    first = db_utils.get_isolated_postgres_engine(
        base,
        namespace='capacity-test',
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=False,
        application_name='capacity-test')
    second = db_utils.get_isolated_postgres_engine(
        base,
        namespace='capacity-test',
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=False,
        application_name='capacity-test')

    assert first is isolated
    assert second is isolated
    create.assert_called_once()
    kwargs = create.call_args.kwargs
    assert kwargs['poolclass'] is sqlalchemy.pool.QueuePool
    assert kwargs['pool_size'] == 1
    assert kwargs['max_overflow'] == 0
    assert kwargs['pool_timeout'] == 1
    assert kwargs['pool_pre_ping'] is False
    assert kwargs['connect_args'] == {
        'connect_timeout': 5,
        'application_name': 'capacity-test',
    }
    base.dispose()


@pytest.mark.parametrize('usable,expected', [(2, 1), (17, 16)])
def test_shadow_reserves_exactly_one_connection(usable, expected):
    capacity = config.CapacityConfig(mode=config.CapacityMode.SHADOW,
                                     sources=(_selector(),),
                                     pilot_end_utc='2030-01-01T00:00:00Z')
    assert runtime._ordinary_db_connections_after_capacity_reservation(
        capacity, usable) == expected


@pytest.mark.parametrize('usable', [None, 0, 1])
def test_shadow_connection_reservation_fails_closed(usable):
    capacity = config.CapacityConfig(mode=config.CapacityMode.SHADOW,
                                     sources=(_selector(),),
                                     pilot_end_utc='2030-01-01T00:00:00Z')
    with pytest.raises(RuntimeError, match='at least two'):
        runtime._ordinary_db_connections_after_capacity_reservation(
            capacity, usable)


def test_disabled_does_not_reserve_connection():
    capacity = config.CapacityConfig()
    assert runtime._ordinary_db_connections_after_capacity_reservation(
        capacity, None) is None
    assert runtime._ordinary_db_connections_after_capacity_reservation(
        capacity, 7) == 7


def test_database_failure_classification():
    assert repository._database_failure(
        sqlalchemy.exc.TimeoutError('checkout')).code == 'database_unavailable'
    operational = sqlalchemy.exc.OperationalError('connect', {}, Exception())
    assert repository._database_failure(
        operational).code == 'database_unavailable'
    statement = sqlalchemy.exc.ProgrammingError('select', {}, Exception())
    assert repository._database_failure(
        statement).code == 'database_statement_failed'

    class QueryCanceled(Exception):

        pgcode = '57014'

    canceled = sqlalchemy.exc.OperationalError('select', {}, QueryCanceled())
    assert repository._database_failure(
        canceled, long_operation=True).code == 'scan_timeout'
    assert repository._database_failure(
        canceled).code == 'database_statement_failed'

    interface = sqlalchemy.exc.InterfaceError('connect', {}, Exception())
    assert repository._database_failure(
        interface).code == 'database_unavailable'


def test_short_watchdog_starts_after_checkout(monkeypatch):
    events: list[str] = []
    intervals: list[float] = []

    class FakeTimer:
        """Record watchdog lifecycle without starting a real thread."""

        def __init__(self, interval, function, args):
            del function, args
            events.append('timer-created')
            intervals.append(interval)

        def start(self):
            events.append('timer-started')

        def cancel(self):
            events.append('timer-canceled')

        def join(self):
            events.append('timer-joined')

    class FakeTransaction:

        def commit(self):
            events.append('commit')

        def rollback(self):
            events.append('rollback')

    class FakeConnection:
        """Minimal connection implementing the short-transaction seam."""

        def __enter__(self):
            events.append('checkout-returned')
            return self

        def __exit__(self, *args):
            del args
            events.append('checked-in')

        def begin(self):
            events.append('begin')
            return FakeTransaction()

        def exec_driver_sql(self, statement):
            events.append(statement)

    class FakeEngine:

        def connect(self):
            events.append('checkout-started')
            return FakeConnection()

    monkeypatch.setattr(repository.threading, 'Timer', FakeTimer)
    instance = object.__new__(repository.ScanRepository)
    instance._engine = FakeEngine()
    instance._cancelled = threading.Event()
    instance._active_lock = threading.Lock()
    instance._active_connection = None

    assert instance._run_short_transaction(
        lambda connection: events.append('operation') or 'ok') == 'ok'

    assert events.index('checkout-returned') < events.index('timer-created')
    assert events.index('timer-started') < events.index('begin')
    assert events.index('commit') < events.index('timer-canceled')
    assert events.index('timer-joined') < events.index('checked-in')
    assert len(intervals) == 1
    assert 0 < intervals[0] <= 2.0


def test_timestamp_is_ascii_and_exact():
    instant = repository._parse_timestamp('2026-07-31T12:34:56Z')
    assert repository.format_timestamp(instant) == '2026-07-31T12:34:56Z'
    with pytest.raises(ValueError):
        repository._parse_timestamp('２０２６-07-31T12:34:56Z')
    with pytest.raises(ValueError):
        repository._parse_timestamp('2026-07-31T12:34:56+00:00')


def test_slot_uses_partition_jitter_and_utc_lattice():
    partition = contracts.SourcePartition(
        'default', models.ProjectionSourceKind.SERVE_SERVICE)
    partition_hash = hashing.source_partition_hash(partition)
    now = datetime.datetime(2026,
                            7,
                            31,
                            12,
                            34,
                            56,
                            tzinfo=datetime.timezone.utc)
    slot = projector._slot_for(partition_hash, now)
    jitter = hashing.slot_jitter_seconds(partition_hash)
    assert slot <= now
    assert (int(slot.timestamp()) - jitter) % 900 == 0
    assert now - slot < datetime.timedelta(seconds=900)


def test_metric_labels_are_closed():
    with pytest.raises(ValueError, match='source kind'):
        metrics.record_scan(workspace='default',
                            source_kind='other',
                            succeeded=True,
                            duration_seconds=0,
                            lag_seconds=0)
    with pytest.raises(ValueError, match='finding'):
        metrics.record_scan(workspace='default',
                            source_kind='serve_service',
                            succeeded=True,
                            duration_seconds=0,
                            lag_seconds=0,
                            findings={'selector_name': 1})


def test_digest_is_computed_inside_repeatable_read_callback(monkeypatch):
    events: list[str] = []

    class FakeReader:

        source_rows = 0
        rows_seen = 0

        def check_deadline(self):
            events.append('deadline')

    class FakeRepository:

        def read_evidence(self, handle, callback):
            del handle
            events.append('snapshot-open')
            value = callback(FakeReader())
            events.append('snapshot-commit')
            return value

    findings = contracts.FindingCounts(selectors_missing=1)
    result = contracts.PartitionEvidenceResult(records=(),
                                               findings=findings,
                                               rows_seen=0)

    def adapter(*args, **kwargs):
        del args, kwargs
        events.append('adapter')
        return result

    monkeypatch.setattr(hashing, 'evidence_inventory_digest',
                        lambda records: events.append('digest') or 'd' * 64)
    capacity = config.CapacityConfig(mode=config.CapacityMode.SHADOW,
                                     sources=(_selector(),),
                                     pilot_end_utc='2030-01-01T00:00:00Z')
    instance = object.__new__(projector.EvidenceProjector)
    instance._config = capacity
    instance._controller = _identity()
    instance._repository = FakeRepository()
    instance._scan_adapter = adapter
    partition = capacity.partitions[0]
    handle = repository.ScanHandle(uuid.uuid4(), partition,
                                   hashing.source_partition_hash(partition),
                                   's' * 64, capacity.pilot_end_utc,
                                   '2029-12-01T00:00:00Z', instance._controller,
                                   0.0)

    actual_result, digest = instance._scan_partition(partition, handle)

    assert actual_result is result
    assert digest == 'd' * 64
    assert events == [
        'snapshot-open', 'adapter', 'deadline', 'digest', 'deadline',
        'snapshot-commit'
    ]


def test_disabled_start_does_not_initialize_capacity_database(monkeypatch):
    initialize = mock.Mock()
    monkeypatch.setattr(projector.state, 'initialize_and_get_db', initialize)
    assert projector.start_controller_projector(config.CapacityConfig(),
                                                controller_instance_id=str(
                                                    uuid.uuid4()),
                                                controller_generation=1) is None
    initialize.assert_not_called()


def test_activation_rejects_seventeenth_configured_partition():
    partitions = tuple(
        contracts.SourcePartition(f'workspace-{index}',
                                  models.ProjectionSourceKind.SERVE_SERVICE)
        for index in range(17))
    instance = object.__new__(repository.ScanRepository)
    with pytest.raises(ValueError, match='At most 16'):
        instance.load_activation_snapshot(partitions, '2030-01-01T00:00:00Z')


def test_publication_retries_serialization_with_exact_delays(monkeypatch):

    class SerializationFailure(Exception):

        pgcode = '40001'

    failure = sqlalchemy.exc.OperationalError('update', {},
                                              SerializationFailure())
    calls = 0
    delays = []

    def run_short(operation, *, outer_deadline=None):
        del operation, outer_deadline
        nonlocal calls
        calls += 1
        if calls == 1:
            return None
        if calls < 4:
            raise failure
        return datetime.datetime.now(tz=datetime.timezone.utc)

    monkeypatch.setattr(repository.time, 'sleep', delays.append)
    instance = object.__new__(repository.ScanRepository)
    instance._cancelled = threading.Event()
    instance._run_short_transaction = run_short
    partition = contracts.SourcePartition(
        'default', models.ProjectionSourceKind.SERVE_SERVICE)
    handle = repository.ScanHandle(uuid.uuid4(), partition,
                                   hashing.source_partition_hash(partition),
                                   'a' * 64, '2030-01-01T00:00:00Z',
                                   '2029-12-01T00:00:00Z', _identity(),
                                   time.monotonic())

    published = instance.publish_completed(handle,
                                           rows_seen=0,
                                           finding_counts={},
                                           inventory_digest='b' * 64)

    assert published.scan_id == handle.scan_id
    assert calls == 4
    assert delays == [0.05, 0.1]


def test_source_snapshot_retries_fresh_and_cancellation_stops_reopen(
        monkeypatch):

    class SerializationFailure(Exception):

        pgcode = '40001'

    failure = sqlalchemy.exc.OperationalError('select', {},
                                              SerializationFailure())

    class FakeTransaction:

        def commit(self):
            return None

        def rollback(self):
            return None

    class FakeConnection:
        """Minimal repeatable-read connection used for retry accounting."""

        def execution_options(self, **kwargs):
            del kwargs
            return self

        def __enter__(self):
            return self

        def __exit__(self, *args):
            del args

        def begin(self):
            return FakeTransaction()

        def exec_driver_sql(self, statement):
            del statement

    class FakeEngine:

        def __init__(self):
            self.connects = 0

        def connect(self):
            self.connects += 1
            return FakeConnection()

    def make_repository():
        instance = object.__new__(repository.ScanRepository)
        instance._engine = FakeEngine()
        instance._cancelled = threading.Event()
        instance._active_lock = threading.Lock()
        instance._active_connection = None
        instance._prove_controller = lambda connection, lock: None
        return instance

    partition = contracts.SourcePartition(
        'default', models.ProjectionSourceKind.SERVE_SERVICE)
    handle = repository.ScanHandle(uuid.uuid4(), partition,
                                   hashing.source_partition_hash(partition),
                                   'a' * 64, '2030-01-01T00:00:00Z',
                                   '2029-12-01T00:00:00Z', _identity(),
                                   time.monotonic())
    readers = []

    def make_reader(connection, *, deadline_monotonic):
        del connection, deadline_monotonic
        reader = object()
        readers.append(reader)
        return reader

    monkeypatch.setattr(repository.source_queries, 'PartitionSourceCache',
                        make_reader)
    delays = []
    monkeypatch.setattr(repository.time, 'sleep', delays.append)
    instance = make_repository()

    def eventually_succeed(reader):
        if len(readers) < 3:
            raise failure
        return reader

    result = instance.read_evidence(handle, eventually_succeed)
    assert result is readers[2]
    assert len({id(reader) for reader in readers}) == 3
    assert instance._engine.connects == 3
    assert delays == [0.05, 0.1]

    readers.clear()
    delays.clear()
    instance = make_repository()
    with pytest.raises(repository.ScanFailure) as exhausted:
        instance.read_evidence(handle, lambda reader:
                               (_ for _ in ()).throw(failure))
    assert exhausted.value.code == 'serialization_exhausted'
    assert len(readers) == 3
    assert instance._engine.connects == 3
    assert delays == [0.05, 0.1]

    readers.clear()
    instance = make_repository()

    def cancel_during_backoff(delay):
        assert delay == 0.05
        instance.cancel_active()

    monkeypatch.setattr(repository.time, 'sleep', cancel_during_backoff)
    with pytest.raises(repository.ScanFailure) as canceled:
        instance.read_evidence(handle, lambda reader:
                               (_ for _ in ()).throw(failure))
    assert canceled.value.code == 'scan_timeout'
    assert len(readers) == 1
    assert instance._engine.connects == 1


def test_internal_fencing_marks_projector_unhealthy(monkeypatch):
    selector = _selector()
    capacity = config.CapacityConfig(mode=config.CapacityMode.SHADOW,
                                     sources=(selector,),
                                     pilot_end_utc='2030-01-01T00:00:00Z')
    partition = capacity.partitions[0]
    identity = _identity()
    handle = repository.ScanHandle(uuid.uuid4(), partition,
                                   hashing.source_partition_hash(partition),
                                   'a' * 64, capacity.pilot_end_utc,
                                   '2029-12-01T00:00:00Z', identity,
                                   time.monotonic())

    class FakeRepository:

        def begin_scan(self, *args):
            del args
            return handle

        def publish_completed(self, *args, **kwargs):
            del args, kwargs
            raise repository.ControllerFencedError('fenced')

    result = contracts.PartitionEvidenceResult(
        records=(),
        findings=contracts.FindingCounts(selectors_missing=1),
        rows_seen=0)
    instance = object.__new__(projector.EvidenceProjector)
    instance._config = capacity
    instance._controller = identity
    instance._repository = FakeRepository()
    instance._scan_partition = lambda *args: (result, 'b' * 64)
    instance._stop = threading.Event()
    instance._healthy = True
    health = mock.Mock()
    monkeypatch.setattr(metrics, 'set_projector_health', health)

    instance._run_one(partition)

    assert instance._stop.is_set()
    assert instance.healthy is False
    health.assert_called_once_with(healthy=False, expired=False)


def test_failure_cas_fencing_stops_projector(monkeypatch):
    selector = _selector()
    capacity = config.CapacityConfig(mode=config.CapacityMode.SHADOW,
                                     sources=(selector,),
                                     pilot_end_utc='2030-01-01T00:00:00Z')
    partition = capacity.partitions[0]
    identity = _identity()
    handle = repository.ScanHandle(uuid.uuid4(), partition,
                                   hashing.source_partition_hash(partition),
                                   'a' * 64, capacity.pilot_end_utc,
                                   '2029-12-01T00:00:00Z', identity,
                                   time.monotonic())

    class FakeRepository:
        """Repository fenced while publishing a failed scan."""

        def current_database_time(self):
            return datetime.datetime(2029, 12, 1, tzinfo=datetime.timezone.utc)

        def begin_scan(self, *args):
            del args
            return handle

        def publish_failed(self, *args):
            del args
            raise repository.ControllerFencedError('fenced during failure CAS')

    instance = object.__new__(projector.EvidenceProjector)
    instance._config = capacity
    instance._controller = identity
    instance._repository = FakeRepository()
    instance._scan_partition = lambda *args: (_ for _ in ()).throw(
        ValueError('source failed'))
    instance._stop = threading.Event()
    instance._healthy = True
    health = mock.Mock()
    monkeypatch.setattr(metrics, 'set_projector_health', health)

    instance._run()

    assert instance.healthy is False
    health.assert_called_once_with(healthy=False, expired=False)


def test_uncommitted_failure_cas_emits_no_scan_metrics(monkeypatch):
    selector = _selector()
    capacity = config.CapacityConfig(mode=config.CapacityMode.SHADOW,
                                     sources=(selector,),
                                     pilot_end_utc='2030-01-01T00:00:00Z')
    partition = capacity.partitions[0]
    identity = _identity()
    handle = repository.ScanHandle(uuid.uuid4(), partition,
                                   hashing.source_partition_hash(partition),
                                   'a' * 64, capacity.pilot_end_utc,
                                   '2029-12-01T00:00:00Z', identity,
                                   time.monotonic())

    class FakeRepository:
        """Repository whose best-effort failure CAS loses its race."""

        def begin_scan(self, *args):
            del args
            return handle

        def publish_failed(self, *args):
            del args
            return False

    instance = object.__new__(projector.EvidenceProjector)
    instance._config = capacity
    instance._controller = identity
    instance._repository = FakeRepository()
    instance._scan_partition = lambda *args: (_ for _ in ()).throw(
        ValueError('source failed'))
    instance._stop = threading.Event()
    instance._healthy = True
    instance._clock = lambda: datetime.datetime(
        2029, 12, 1, tzinfo=datetime.timezone.utc)
    record_scan = mock.Mock()
    monkeypatch.setattr(metrics, 'record_scan', record_scan)

    instance._run_one(partition)

    record_scan.assert_not_called()
    assert instance.healthy is True
    assert not instance._stop.is_set()


def test_expiry_fencing_cannot_report_healthy(monkeypatch):
    selector = _selector()
    capacity = config.CapacityConfig(mode=config.CapacityMode.SHADOW,
                                     sources=(selector,),
                                     pilot_end_utc='2030-01-01T00:00:00Z')
    partition = capacity.partitions[0]

    class FakeRepository:

        def current_database_time(self):
            return datetime.datetime(2030, 1, 1, tzinfo=datetime.timezone.utc)

        def finalize_stale(self, value):
            assert value == partition
            raise repository.ControllerFencedError('fenced')

    instance = object.__new__(projector.EvidenceProjector)
    instance._config = capacity
    instance._repository = FakeRepository()
    instance._snapshot = repository.ActivationSnapshot(
        capacity.pilot_end_utc, frozenset((partition,)),
        datetime.datetime(2029, 12, 1, tzinfo=datetime.timezone.utc))
    instance._stop = threading.Event()
    instance._healthy = True
    health = mock.Mock()
    monkeypatch.setattr(metrics, 'set_projector_health', health)

    instance._run()

    assert instance._stop.is_set()
    assert instance.healthy is False
    health.assert_called_once_with(healthy=False, expired=False)


def _mock_controller_runtime(monkeypatch, *, projector_stop_fails=False):
    events = []

    class FakeInstanceLease:

        role = 'controller'
        instance_id = str(uuid.uuid4())

        def set_ready(self, ready, *, health_detail):
            events.append(('ready', ready, health_detail['phase']))

    class FakeLeaderLease:
        """Record leadership acquisition and release ordering."""

        def __init__(self):
            self.instance_id = FakeInstanceLease.instance_id
            self.generation = None

        def try_acquire(self):
            events.append('try-acquire')
            self.generation = 7
            return True

        def backend_pid(self):
            return 123

        def release(self):
            events.append('lease-release')
            self.generation = None

    class FakeHealthServer:

        def start(self):
            events.append('health-start')

        def stop(self):
            events.append('health-stop')

    class FakeShutdownEvent:
        """Request a graceful exit from the first supervision wait."""

        def is_set(self):
            return False

        def wait(self, timeout):
            del timeout
            # Leadership succeeds in the first probe.  The next wait is the
            # supervision loop and requests an ordinary graceful drain.
            return True

        def set(self):
            events.append('shutdown-set')

    class FakeProjector:

        healthy = True

    class FakeWorker:

        def request_shutdown(self):
            events.append('worker-stop-request')

    class FakeBackground:

        def run(self, coroutine):
            events.append('controller-requests-init')
            coroutine.close()

        def stop(self):
            events.append('background-stop')

    fake_lease = FakeLeaderLease()
    fake_projector = FakeProjector()
    fake_worker = FakeWorker()
    fake_background = FakeBackground()
    instance_lease = FakeInstanceLease()

    monkeypatch.setattr(runtime.signal, 'signal', lambda *args: None)
    monkeypatch.setattr(runtime.threading, 'Event', FakeShutdownEvent)
    monkeypatch.setattr(runtime.request_postgres, 'ControllerLeaderLease',
                        lambda instance_id: fake_lease)
    monkeypatch.setattr(runtime, '_RoleHealthServer',
                        lambda *args: FakeHealthServer())
    monkeypatch.setattr(runtime.request_postgres,
                        'recent_legacy_controller_consumers',
                        lambda seconds: [])
    monkeypatch.setattr(
        runtime.request_postgres, 'fence_stale_controller_claims',
        lambda *args: events.append('fence-stale') or {
            'replayed': 0,
            'interrupted': 0
        })
    monkeypatch.setattr(
        runtime.capacity_projector_lib, 'start_controller_projector', lambda *
        args, **kwargs: events.append('projector-start') or fake_projector)

    def stop_projector(value):
        assert value is fake_projector
        events.append('projector-stop')
        if projector_stop_fails:
            raise RuntimeError('projector stop failed')

    monkeypatch.setattr(runtime.capacity_projector_lib,
                        'stop_controller_projector', stop_projector)
    monkeypatch.setattr(runtime.clean_env_module, 'capture_clean_server_env',
                        lambda: events.append('capture-env'))
    monkeypatch.setattr(
        runtime.executor, 'start',
        lambda *args, **kwargs: events.append('worker-start') or
        (None, [fake_worker]))
    monkeypatch.setattr(
        runtime, '_start_background_loop',
        lambda *args: events.append('background-start') or fake_background)
    monkeypatch.setattr(runtime, '_start_surface_interrupted_cluster_launches',
                        lambda: events.append('surface-start'))
    monkeypatch.setattr(runtime, '_kill_local_controller_children',
                        lambda: events.append('children-fail-stop'))
    monkeypatch.setattr(runtime, '_request_worker_shutdown',
                        lambda *args, **kwargs: events.append('workers-joined'))
    monkeypatch.setattr(runtime, '_stop_queue_server',
                        lambda value: events.append('queue-stop'))
    monkeypatch.setattr(managed_job_refresh_thread,
                        'start_managed_job_refresh_daemon',
                        lambda: events.append('managed-refresh-start'))

    state = runtime.RuntimeState('controller', mock.Mock(), instance_lease,
                                 False, config.CapacityConfig())
    args = types.SimpleNamespace(host='127.0.0.1',
                                 role_health_port=1,
                                 metrics_port=2)
    return state, args, events


def test_controller_activation_precedes_workers_and_readiness(monkeypatch):
    state, args, events = _mock_controller_runtime(monkeypatch)

    runtime._run_controller_role(state, args)

    activating = ('ready', False, 'activating-controller')
    leading = ('ready', True, 'leading')
    assert events.index('try-acquire') < events.index(activating)
    assert events[events.index('try-acquire') + 1] == activating
    assert events.index(activating) < events.index('fence-stale')
    assert events.index('fence-stale') < events.index('projector-start')
    assert events.index('projector-start') < events.index('worker-start')
    assert events.index('worker-start') < events.index(leading)
    assert events.index('projector-stop') < events.index('lease-release')
    assert events.index('children-fail-stop') < events.index('lease-release')
    assert events.index('workers-joined') < events.index('lease-release')


def test_controller_drain_failure_fail_stops_before_release(monkeypatch):
    state, args, events = _mock_controller_runtime(monkeypatch,
                                                   projector_stop_fails=True)

    class FailStop(RuntimeError):
        pass

    def fail_stop(code):
        events.append(('process-exit', code))
        raise FailStop()

    monkeypatch.setattr(runtime.os, '_exit', fail_stop)

    with pytest.raises(FailStop):
        runtime._run_controller_role(state, args)

    assert 'projector-stop' in events
    assert 'children-fail-stop' in events
    assert 'workers-joined' in events
    assert ('process-exit', 1) in events
    assert 'lease-release' not in events
