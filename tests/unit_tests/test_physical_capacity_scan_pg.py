"""PostgreSQL activation, scan atomicity, and leadership-fencing tests."""
# pylint: disable=protected-access,redefined-outer-name

import datetime
import os
import shutil
import threading
import time
import uuid

import pytest
import sqlalchemy

from sky import global_user_state
from sky.physical_capacity import adapters
from sky.physical_capacity import contracts
from sky.physical_capacity import hashing
from sky.physical_capacity import models
from sky.physical_capacity import repository
from sky.physical_capacity import schema as capacity_schema
from sky.physical_capacity import source_queries
from sky.server.requests import postgres as request_postgres
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_POSTGRES_URI = os.environ.get('SKYPILOT_TEST_POSTGRES_URI')
testcontainers_postgres = (None if _POSTGRES_URI is not None else
                           pytest.importorskip('testcontainers.postgres'))
pytest.importorskip('psycopg2')

pytestmark = [
    pytest.mark.skipif(
        _POSTGRES_URI is None and shutil.which('docker') is None,
        reason='docker unavailable; skipping evidence-scan PostgreSQL tests'),
    pytest.mark.xdist_group(name='physical_capacity_scan_pg'),
]

_CAPACITY_TABLES_EXCEPT_SCANS = (
    'capacity_groups',
    'capacity_group_intents',
    'capacity_allocations',
    'capacity_allocation_desires',
)


@pytest.fixture(scope='module')
def postgres_engine():
    if _POSTGRES_URI is not None:
        engine = sqlalchemy.create_engine(_POSTGRES_URI)
        try:
            yield engine
        finally:
            engine.dispose()
        return

    assert testcontainers_postgres is not None
    container = None
    try:
        container = testcontainers_postgres.PostgresContainer('postgres:16')
        container.start()
    except Exception as error:  # pylint: disable=broad-except
        pytest.skip(f'could not start postgres container: {error}')
    assert container is not None
    engine = sqlalchemy.create_engine(container.get_connection_url())
    try:
        yield engine
    finally:
        engine.dispose()
        container.stop()


@pytest.fixture(scope='module')
def scan_engine(postgres_engine):
    schema_name = f'capacity_scan_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {schema_name}')
    schema_url = postgres_engine.url.update_query_dict(
        {'options': f'-csearch_path={schema_name}'})
    engine = sqlalchemy.create_engine(schema_url, pool_size=8, max_overflow=0)
    try:
        migrations = (
            (migration_utils.GLOBAL_USER_STATE_DB_NAME,
             migration_utils.GLOBAL_USER_STATE_VERSION, 'bootstrap'),
            (migration_utils.SERVE_DB_NAME, migration_utils.SERVE_VERSION,
             'upgrade'),
            (migration_utils.SPOT_JOBS_DB_NAME,
             migration_utils.SPOT_JOBS_VERSION, 'upgrade'),
            (migration_utils.API_REQUESTS_DB_NAME,
             migration_utils.API_REQUESTS_VERSION, 'upgrade'),
            (migration_utils.CAPACITY_STATE_DB_NAME,
             migration_utils.CAPACITY_STATE_VERSION, 'upgrade'),
        )
        for section, revision, mode in migrations:
            migration_utils.safe_alembic_upgrade(engine,
                                                 section,
                                                 revision,
                                                 mode=mode)
        yield engine
    finally:
        engine.dispose()
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')


@pytest.fixture(autouse=True)
def clean_scan_state(scan_engine):
    with scan_engine.begin() as connection:
        connection.exec_driver_sql(
            'TRUNCATE TABLE capacity_projection_scans CASCADE')
        connection.exec_driver_sql('DELETE FROM api_controller_leadership')
    yield
    with scan_engine.begin() as connection:
        connection.exec_driver_sql(
            'TRUNCATE TABLE capacity_projection_scans CASCADE')
        connection.exec_driver_sql('DELETE FROM api_controller_leadership')


def _acquire_leader(scan_engine, monkeypatch):
    monkeypatch.setattr(global_user_state, 'initialize_and_get_db',
                        lambda: scan_engine)
    lease = request_postgres.ControllerLeaderLease(str(uuid.uuid4()))
    assert lease.try_acquire()
    assert lease.generation is not None
    identity = repository.ControllerIdentity(lease.instance_id,
                                             lease.generation)
    return lease, identity


def _database_now(engine):
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.transaction_timestamp())).scalar_one()


def _pilot_end(now):
    return repository.format_timestamp(now + datetime.timedelta(days=1))


def _scan_missing_service(scan_repository, handle, selector, identity):
    partition = contracts.selector_partition(selector)

    def read(reader):
        result = adapters.scan_partition(
            partition, (selector,), (),
            reader,
            controller_instance_id=identity.instance_id,
            controller_generation=identity.generation)
        result.validate(1)
        reader.check_deadline()
        digest = hashing.evidence_inventory_digest(result.records)
        reader.check_deadline()
        return result, digest

    return scan_repository.read_evidence(handle, read)


def test_activation_atomic_scan_and_leadership_handoff(scan_engine,
                                                       monkeypatch):
    lease_one, identity_one = _acquire_leader(scan_engine, monkeypatch)
    repository_one = repository.ScanRepository(scan_engine, identity_one)
    lease_two = None
    repository_two = None
    try:
        assert repository_one.validate_schema(
            source_queries.source_schema_requirements()).startswith(
                'capacity_scan_')

        selector = contracts.ServeSourceSelector(
            workspace='default',
            source_kind=models.ProjectionSourceKind.SERVE_SERVICE,
            service_name='missing-service')
        service_partition = contracts.selector_partition(selector)
        pool_partition = contracts.SourcePartition(
            'default', models.ProjectionSourceKind.SERVE_POOL)
        partitions = (service_partition, pool_partition)
        pilot_end = _pilot_end(_database_now(scan_engine))
        snapshot = repository_one.load_activation_snapshot((service_partition,),
                                                           pilot_end)
        assert snapshot.durable_partitions == frozenset((service_partition,))

        service_handle = repository_one.begin_scan(
            service_partition, hashing.source_partition_hash(service_partition),
            'a' * 64, pilot_end)
        assert service_handle is not None
        with scan_engine.connect() as connection:
            running = connection.execute(
                sqlalchemy.select(capacity_schema.PROJECTION_SCANS).where(
                    capacity_schema.PROJECTION_SCANS.c.scan_id ==
                    service_handle.scan_id)).mappings().one()
        assert running['state'] == 'running'
        assert running['completed_at'] is None
        assert running['cursor']['inventory_digest'] is None
        assert running['finding_counts'] == {}

        # Long repeatable-read evidence work never locks the leadership row;
        # the dedicated lease session can heartbeat while the scan is live.
        read_started = threading.Event()
        read_errors = []

        def slow_read():
            try:
                repository_one._read_snapshot(
                    service_handle, lambda connection: (
                        read_started.set(),
                        connection.execute(
                            sqlalchemy.text('SELECT pg_sleep(1.5)')),
                    ))
            except BaseException as error:  # pylint: disable=broad-except
                read_errors.append(error)

        read_thread = threading.Thread(target=slow_read)
        read_thread.start()
        assert read_started.wait(timeout=1)
        heartbeat_started = time.monotonic()
        assert lease_one.heartbeat()
        assert time.monotonic() - heartbeat_started < 1
        read_thread.join(timeout=3)
        assert not read_thread.is_alive()
        assert not read_errors

        result, digest = _scan_missing_service(repository_one, service_handle,
                                               selector, identity_one)
        assert result.rows_seen == 0
        assert result.findings.selectors_missing == 1
        published = repository_one.publish_completed(
            service_handle,
            rows_seen=result.rows_seen,
            finding_counts=result.findings.to_dict(),
            inventory_digest=digest)
        assert published.scan_id == service_handle.scan_id
        assert published.digest_changed is False

        with scan_engine.connect() as connection:
            completed = connection.execute(
                sqlalchemy.select(capacity_schema.PROJECTION_SCANS).where(
                    capacity_schema.PROJECTION_SCANS.c.scan_id ==
                    service_handle.scan_id)).mappings().one()
            other_counts = {
                table: connection.execute(
                    sqlalchemy.text(f'SELECT count(*) FROM {table}')
                ).scalar_one() for table in _CAPACITY_TABLES_EXCEPT_SCANS
            }
        assert completed['state'] == 'completed'
        assert completed['completed_at'] == completed['updated_at']
        assert completed['rows_seen'] == result.rows_seen
        assert completed['finding_counts'] == result.findings.to_dict()
        assert set(completed['finding_counts']) == {
            key.value for key in contracts.FindingKey
        }
        assert completed['cursor'] == service_handle.running_cursor() | {
            'inventory_digest': digest
        }
        assert completed['controller_instance_id'] == uuid.UUID(
            identity_one.instance_id)
        assert completed['controller_generation'] == identity_one.generation
        assert other_counts == {
            table: 0 for table in _CAPACITY_TABLES_EXCEPT_SCANS
        }

        # One database-authoritative UTC slot is terminal and cannot be
        # inserted again, even with a changed projection scope.
        assert repository_one.begin_scan(
            service_partition, hashing.source_partition_hash(service_partition),
            'b' * 64, pilot_end) is None

        # Expanding configuration grows the durable union under the same
        # immutable end.  The next activation can omit that partition, but
        # persisted evidence keeps it in the union.
        expanded = repository_one.load_activation_snapshot(
            partitions, pilot_end)
        assert expanded.durable_partitions == frozenset(partitions)

        pool_handle = repository_one.begin_scan(
            pool_partition, hashing.source_partition_hash(pool_partition),
            'c' * 64, pilot_end)
        assert pool_handle is not None
        with scan_engine.begin() as connection:
            connection.exec_driver_sql('SET LOCAL enable_seqscan = off')
            running_plan = '\n'.join(
                connection.execute(
                    sqlalchemy.text("""
                        EXPLAIN
                        SELECT scan_id
                        FROM capacity_projection_scans
                        WHERE workspace = :workspace
                          AND source_kind = :source_kind
                          AND source_partition_hash = :partition_hash
                          AND state = 'running'
                    """), {
                        'workspace': pool_partition.workspace,
                        'source_kind': pool_partition.source_kind.value,
                        'partition_hash':
                            hashing.source_partition_hash(pool_partition),
                    }).scalars())
            completed_plan = '\n'.join(
                connection.execute(
                    sqlalchemy.text("""
                        EXPLAIN
                        SELECT cursor ->> 'scheduled_slot_utc'
                        FROM capacity_projection_scans
                        WHERE workspace = :workspace
                          AND source_kind = :source_kind
                          AND state IN ('completed', 'failed')
                          AND completed_at >=
                              transaction_timestamp() - INTERVAL '35 days'
                        ORDER BY completed_at DESC
                        LIMIT 4000
                    """), {
                        'workspace': service_partition.workspace,
                        'source_kind': service_partition.source_kind.value,
                    }).scalars())
        assert 'uq_capacity_projection_scans_running_partition' in running_plan
        assert 'ix_capacity_projection_scans_completed' in completed_plan

        # A released generation cannot publish or failure-CAS its running row.
        lease_one.release()
        with pytest.raises(repository.ControllerFencedError):
            repository_one.publish_completed(pool_handle,
                                             rows_seen=0,
                                             finding_counts={},
                                             inventory_digest='d' * 64)
        with pytest.raises(repository.ControllerFencedError):
            repository_one.publish_failed(
                pool_handle, repository.ScanFailure('source_decode_failed'))
        repository_one.close()

        lease_two, identity_two = _acquire_leader(scan_engine, monkeypatch)
        repository_two = repository.ScanRepository(scan_engine, identity_two)
        snapshot_two = repository_two.load_activation_snapshot(
            (service_partition,), pilot_end)
        assert snapshot_two.durable_partitions == frozenset(partitions)
        with pytest.raises(ValueError, match='differs'):
            repository_two.load_activation_snapshot(
                (service_partition,),
                repository.format_timestamp(
                    _database_now(scan_engine) + datetime.timedelta(days=2)))
        with scan_engine.begin() as connection:
            stale_time = (sqlalchemy.func.clock_timestamp() -
                          datetime.timedelta(minutes=11))
            connection.execute(
                sqlalchemy.update(capacity_schema.PROJECTION_SCANS).where(
                    capacity_schema.PROJECTION_SCANS.c.scan_id ==
                    pool_handle.scan_id).values(started_at=stale_time,
                                                updated_at=stale_time))
        assert repository_two.finalize_stale(pool_partition) == 1
        with scan_engine.connect() as connection:
            failed = connection.execute(
                sqlalchemy.select(capacity_schema.PROJECTION_SCANS).where(
                    capacity_schema.PROJECTION_SCANS.c.scan_id ==
                    pool_handle.scan_id)).mappings().one()
        assert failed['state'] == 'failed'
        assert failed['error_code'] == 'stale_scan'
        assert failed['cursor']['inventory_digest'] is None
        assert failed['finding_counts'] == {}
    finally:
        if repository_two is not None:
            repository_two.close()
        if lease_two is not None:
            lease_two.release()
        repository_one.close()
        lease_one.release()


def test_activation_rejects_generic_wrong_typed_cursor(scan_engine,
                                                       monkeypatch):
    lease, identity = _acquire_leader(scan_engine, monkeypatch)
    scan_repository = repository.ScanRepository(scan_engine, identity)
    try:
        partition = contracts.SourcePartition(
            'default', models.ProjectionSourceKind.SERVE_SERVICE)
        partition_hash = hashing.source_partition_hash(partition)
        now = _database_now(scan_engine)
        jitter = hashing.slot_jitter_seconds(partition_hash)
        slot_number = (int(now.timestamp()) - jitter) // 900
        slot = datetime.datetime.fromtimestamp(slot_number * 900 + jitter,
                                               tz=datetime.timezone.utc)
        pilot_end = _pilot_end(now)
        malformed_cursor = {
            # A generic C1 JSON object that spells "1" is not a typed C2
            # activation anchor; mapping_version must be the JSON number 1.
            'mapping_version': '1',
            'phase': 'full_snapshot',
            'projection_scope_hash': 'e' * 64,
            'pilot_end_utc': pilot_end,
            'scheduled_slot_utc': repository.format_timestamp(slot),
            'inventory_digest': None,
        }
        with scan_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(capacity_schema.PROJECTION_SCANS).values(
                    scan_id=uuid.uuid4(),
                    workspace=partition.workspace,
                    source_kind=partition.source_kind.value,
                    source_partition_hash=partition_hash,
                    cursor_schema_version=1,
                    cursor=malformed_cursor,
                    state='running',
                    controller_instance_id=uuid.UUID(identity.instance_id),
                    controller_generation=identity.generation,
                    rows_seen=0,
                    finding_counts={},
                    error_code=None,
                    started_at=now,
                    updated_at=now,
                    completed_at=None))

        with pytest.raises(repository.ScanFailure) as exception:
            scan_repository.load_activation_snapshot((partition,), pilot_end)
        assert exception.value.code == 'source_decode_failed'

        def replace_with_cursors(cursors):
            with scan_engine.begin() as connection:
                connection.exec_driver_sql(
                    'TRUNCATE TABLE capacity_projection_scans CASCADE')
                for cursor in cursors:
                    connection.execute(
                        sqlalchemy.insert(
                            capacity_schema.PROJECTION_SCANS).values(
                                scan_id=uuid.uuid4(),
                                workspace=partition.workspace,
                                source_kind=partition.source_kind.value,
                                source_partition_hash=partition_hash,
                                cursor_schema_version=1,
                                cursor=cursor,
                                state='failed',
                                controller_instance_id=uuid.UUID(
                                    identity.instance_id),
                                controller_generation=identity.generation,
                                rows_seen=0,
                                finding_counts={},
                                error_code='stale_scan',
                                started_at=now,
                                updated_at=now,
                                completed_at=now))

        valid_cursor = {
            'mapping_version': 1,
            'phase': 'full_snapshot',
            'projection_scope_hash': 'e' * 64,
            'pilot_end_utc': pilot_end,
            'scheduled_slot_utc': repository.format_timestamp(slot),
            'inventory_digest': None,
        }
        misaligned_cursor = dict(valid_cursor)
        misaligned_cursor['scheduled_slot_utc'] = repository.format_timestamp(
            slot + datetime.timedelta(seconds=1))
        replace_with_cursors((misaligned_cursor,))
        with pytest.raises(repository.ScanFailure) as exception:
            scan_repository.load_activation_snapshot((partition,), pilot_end)
        assert exception.value.code == 'source_decode_failed'

        replace_with_cursors((valid_cursor, valid_cursor))
        with pytest.raises(repository.ScanFailure) as exception:
            scan_repository.load_activation_snapshot((partition,), pilot_end)
        assert exception.value.code == 'source_conflict'
    finally:
        scan_repository.close()
        lease.release()


def test_database_watchdogs_cancel_and_leave_pool_idle(scan_engine,
                                                       monkeypatch):
    lease, identity = _acquire_leader(scan_engine, monkeypatch)
    scan_repository = repository.ScanRepository(scan_engine, identity)
    isolated_engine = scan_repository.engine
    try:
        short_started = time.monotonic()
        with pytest.raises(sqlalchemy.exc.OperationalError) as short_error:
            scan_repository._run_short_transaction(
                lambda connection: connection.execute(
                    sqlalchemy.text('SELECT pg_sleep(3)')))
        assert repository._sqlstate(short_error.value) == '57014'
        assert time.monotonic() - short_started < 2
        assert db_utils.isolated_postgres_engine_checked_out(
            isolated_engine) == 0

        partition = contracts.SourcePartition(
            'default', models.ProjectionSourceKind.SERVE_SERVICE)
        handle = repository.ScanHandle(
            uuid.uuid4(),
            partition,
            hashing.source_partition_hash(partition),
            'f' * 64,
            _pilot_end(_database_now(scan_engine)),
            repository.format_timestamp(_database_now(scan_engine)),
            identity,
            # Leave less than the 30-second cap so the test proves that a
            # server 57014 from the long profile maps to scan_timeout.
            time.monotonic() - 28.5)
        long_started = time.monotonic()
        with pytest.raises(repository.ScanFailure) as long_error:
            scan_repository._read_snapshot(
                handle, lambda connection: connection.execute(
                    sqlalchemy.text('SELECT pg_sleep(3)')))
        assert long_error.value.code == 'scan_timeout'
        assert time.monotonic() - long_started < 2
        assert db_utils.isolated_postgres_engine_checked_out(
            isolated_engine) == 0
    finally:
        scan_repository.close()
        lease.release()
    assert db_utils.isolated_postgres_engine_checked_out(isolated_engine) == 0


def test_activation_rejects_53777th_global_row(scan_engine, monkeypatch):
    lease, identity = _acquire_leader(scan_engine, monkeypatch)
    scan_repository = repository.ScanRepository(scan_engine, identity)
    try:
        # Phase one must reject the sentinel row before any cursor contents
        # are fetched.  These remain valid generic C1 rows so the test cannot
        # accidentally rely on a table constraint to enforce the C2 bound.
        with scan_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO capacity_projection_scans (
                        scan_id, workspace, source_kind,
                        source_partition_hash, cursor_schema_version, cursor,
                        state, controller_instance_id, controller_generation,
                        rows_seen, finding_counts, error_code, started_at,
                        updated_at, completed_at
                    )
                    SELECT md5(series::text)::uuid,
                           'default', 'serve_service', repeat('a', 64),
                           1, '{}'::jsonb, 'failed',
                           CAST(:instance_id AS uuid),
                           :generation, 0, '{}'::jsonb, 'stale_scan',
                           sampled.now, sampled.now, sampled.now
                    FROM generate_series(1, 53777) AS series
                    CROSS JOIN (SELECT clock_timestamp() AS now) AS sampled
                """), {
                    'instance_id': identity.instance_id,
                    'generation': identity.generation,
                })
        partition = contracts.SourcePartition(
            'default', models.ProjectionSourceKind.SERVE_SERVICE)
        with pytest.raises(repository.ScanFailure) as exception:
            scan_repository.load_activation_snapshot(
                (partition,), _pilot_end(_database_now(scan_engine)))
        assert exception.value.code == 'row_limit_exceeded'
    finally:
        scan_repository.close()
        lease.release()


def test_activation_rejects_3362_rows_in_one_partition(scan_engine,
                                                       monkeypatch):
    lease, identity = _acquire_leader(scan_engine, monkeypatch)
    scan_repository = repository.ScanRepository(scan_engine, identity)
    try:
        partition = contracts.SourcePartition(
            'default', models.ProjectionSourceKind.SERVE_SERVICE)
        partition_hash = hashing.source_partition_hash(partition)
        now = _database_now(scan_engine)
        jitter = hashing.slot_jitter_seconds(partition_hash)
        slot_number = (int(now.timestamp()) - jitter) // 900
        latest_slot = datetime.datetime.fromtimestamp(slot_number * 900 +
                                                      jitter,
                                                      tz=datetime.timezone.utc)
        pilot_end = repository.format_timestamp(latest_slot +
                                                datetime.timedelta(seconds=900))
        rows = []
        for index in range(3_362):
            # The 35-day half-open interval contains exactly 3,361 lattice
            # slots.  The sentinel row must therefore collide with an exact
            # valid slot and be rejected by the tuple-uniqueness proof.
            slot_index = min(index, 3_360)
            slot = latest_slot - datetime.timedelta(seconds=900 * slot_index)
            rows.append({
                'scan_id': uuid.uuid4(),
                'workspace': partition.workspace,
                'source_kind': partition.source_kind.value,
                'source_partition_hash': partition_hash,
                'cursor_schema_version': 1,
                'cursor': {
                    'mapping_version': 1,
                    'phase': 'full_snapshot',
                    'projection_scope_hash': 'a' * 64,
                    'pilot_end_utc': pilot_end,
                    'scheduled_slot_utc': repository.format_timestamp(slot),
                    'inventory_digest': None,
                },
                'state': 'failed',
                'controller_instance_id': uuid.UUID(identity.instance_id),
                'controller_generation': identity.generation,
                'rows_seen': 0,
                'finding_counts': {},
                'error_code': 'stale_scan',
                'started_at': now,
                'updated_at': now,
                'completed_at': now,
            })
        with scan_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(capacity_schema.PROJECTION_SCANS), rows)

        with pytest.raises(repository.ScanFailure) as exception:
            scan_repository.load_activation_snapshot((partition,), pilot_end)
        assert exception.value.code == 'source_conflict'
    finally:
        scan_repository.close()
        lease.release()
