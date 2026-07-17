"""Real-Postgres layer for the reserved-fill broker tests.

Production serve-state runs on Postgres (SKYPILOT_DB_CONNECTION_URI), so the
broker SQL (dialect-switched upserts, lease CAS rowcount, prune predicate)
and the alembic migration chain must be exercised on a real PG engine, not
only sqlite. This module:

- boots a throwaway postgres:16 container once per session via
  testcontainers (skipping the whole module cleanly when docker is
  unavailable or the container cannot start);
- re-runs the FULL sqlite round-mechanics/claims/prune/lease/epoch suite
  from test_reserved_fill_broker.py against the PG engine by overriding its
  `broker_engine` fixture (the test bodies are inherited, not copied);
- runs the alembic chain exactly as serve_state.create_table does on a
  fresh database, twice (idempotency);
- races two threads through run_round_if_stale so the publish CAS and the
  PostgresLock advisory path are exercised for real.
"""
# pylint: disable=cell-var-from-loop,missing-class-docstring
# pylint: disable=protected-access,redefined-outer-name,unused-import
import datetime
import shutil
import threading
import time
from unittest import mock
import uuid

import pytest
import sqlalchemy
from sqlalchemy import create_engine
from test_reserved_fill_broker import _broker_db  # noqa: F401
from test_reserved_fill_broker import clock  # noqa: F401
# The sqlite suite: its DB-touching test classes are re-collected below
# against the PG `broker_engine` override; `clock` and `_broker_db` are the
# fixtures those inherited test bodies request (importing a fixture function
# registers it in this module, where fixture resolution picks up the PG
# `broker_engine` defined here instead of the sqlite one).
import test_reserved_fill_broker as sqlite_suite

from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker as broker
from sky.serve import serve_history
from sky.serve import serve_state
from sky.utils import locks
from sky.utils.db import migration_utils

psycopg2 = pytest.importorskip('psycopg2')
testcontainers_postgres = pytest.importorskip('testcontainers.postgres')

_PG_IMAGE = 'postgres:16'

pytestmark = pytest.mark.skipif(
    shutil.which('docker') is None,
    reason='docker unavailable; skipping real-Postgres broker tests')


@pytest.fixture(scope='session')
def pg_server():
    """One throwaway postgres:16 container for the whole session.

    Yields the started PostgresContainer (testcontainers handles port
    mapping and readiness). Skips (never fails) when the container cannot
    start or never becomes ready: CI without a working docker daemon must
    not go red on this module.
    """
    container = testcontainers_postgres.PostgresContainer(_PG_IMAGE)
    try:
        container.start()
    except Exception as e:  # pylint: disable=broad-except
        pytest.skip(f'could not start postgres container: {e}')
    try:
        yield container
    finally:
        container.stop()


def _create_database(pg_server, dbname: str) -> str:
    """Creates a fresh database on the server; returns its SQLAlchemy URL."""
    host = pg_server.get_container_host_ip()
    port = pg_server.get_exposed_port(pg_server.port)
    conn = psycopg2.connect(host=host,
                            port=port,
                            user=pg_server.username,
                            password=pg_server.password,
                            dbname=pg_server.dbname)
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        conn.close()
    return (f'postgresql://{pg_server.username}:{pg_server.password}'
            f'@{host}:{port}/{dbname}')


@pytest.fixture(scope='session')
def _pg_mechanics_url(pg_server):
    """One database reused by every round-mechanics test (reset per test)."""
    return _create_database(pg_server, 'broker_mechanics')


@pytest.fixture
def broker_engine(_pg_mechanics_url):
    """Overrides the sqlite suite's engine with a real-PG one.

    drop_all resets the reused database to empty; `_broker_db` (imported
    from the sqlite suite) then runs create_all against it, exactly as it
    does for sqlite.
    """
    engine = create_engine(_pg_mechanics_url)
    serve_state.Base.metadata.drop_all(engine)
    yield engine
    engine.dispose()


# ============ The full sqlite DB-touching suite, on Postgres ============
# Empty subclasses re-collect every inherited test method in this module,
# where the PG `broker_engine` override applies. Pure-math classes
# (water-fill / entitlements / feeds / damping) touch no DB and are not
# repeated.


@pytest.mark.usefixtures('_broker_db')
class TestFixtureWiring:

    def test_broker_engine_is_postgres(self):
        """Guards the override: if fixture resolution ever stopped picking
        this module's `broker_engine`, the inherited suite would silently
        re-run on sqlite and this module would test nothing new."""
        engine = serve_state._db_manager.get_engine()
        assert engine.dialect.name == 'postgresql'


class TestSingleClaimantFastPathPG(sqlite_suite.TestSingleClaimantFastPath):
    pass


class TestMultiClaimantRoundsPG(sqlite_suite.TestMultiClaimantRounds):
    pass


class TestBlackoutPG(sqlite_suite.TestBlackout):
    pass


class TestClaimLifecyclePG(sqlite_suite.TestClaimLifecycle):
    pass


class TestEpochFencingPG(sqlite_suite.TestEpochFencing):
    pass


class TestStaleWriterFencePG(sqlite_suite.TestStaleWriterFence):
    pass


class TestAtomicPersistFencePG(sqlite_suite.TestAtomicPersistFence):
    pass


class TestRoundPersistExclusionPG(sqlite_suite.TestRoundPersistExclusion):
    pass


class TestFencePendingFailsClosedPG(sqlite_suite.TestFencePendingFailsClosed):
    pass


class TestOrphanFillRowDebitPG(sqlite_suite.TestOrphanFillRowDebit):
    pass


class TestReplicaSnapshotDebitPG(sqlite_suite.TestReplicaSnapshotDebit):
    pass


class TestGroupedReplicaSnapshotPG:
    """The grouped replica query is portable to the production DB dialect."""

    def test_groups_replica_rows(self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        for service_id in range(3):
            service_name = f'svc-{service_id}'
            serve_state.add_or_update_replicas(
                service_name,
                [(replica_id,
                  replica_managers.ReplicaInfo(replica_id=replica_id,
                                               cluster_name=service_name,
                                               replica_port='8080',
                                               is_spot=False,
                                               location=None,
                                               version=1,
                                               resources_override=None))
                 for replica_id in range(2)])

        grouped = serve_state.get_replica_infos_grouped()

        assert set(grouped) == {'svc-0', 'svc-1', 'svc-2'}
        assert all(len(infos) == 2 for infos in grouped.values())


class TestServiceLivenessSnapshotPG:
    """The slim liveness query is portable to the production DB dialect."""

    def test_filters_mode_and_requires_version_row(self, broker_engine,
                                                   monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        assert serve_state.add_service(name='serve-a',
                                       controller_job_id=1,
                                       policy='policy',
                                       requested_resources_str='1x[CPU:1+]',
                                       load_balancing_policy='round_robin',
                                       status=serve_state.ServiceStatus.READY,
                                       tls_encrypted=False,
                                       pool=False,
                                       controller_pid=11,
                                       entrypoint='entry',
                                       spec=None,
                                       yaml_content='service: {}',
                                       controller_ip='10.0.0.1',
                                       service_hash='hash-a',
                                       resource_scope='scope-a')
        assert serve_state.add_service(name='pool-a',
                                       controller_job_id=2,
                                       policy='policy',
                                       requested_resources_str='1x[CPU:1+]',
                                       load_balancing_policy='round_robin',
                                       status=serve_state.ServiceStatus.READY,
                                       tls_encrypted=False,
                                       pool=True,
                                       controller_pid=22,
                                       entrypoint='entry',
                                       spec=None,
                                       yaml_content='service: {}')
        with sqlalchemy.orm.Session(broker_engine) as session:
            session.execute(serve_state.services_table.insert().values(
                name='serve-orphan',
                controller_job_id=3,
                status=serve_state.ServiceStatus.READY.value,
                requested_resources_str='1x[CPU:1+]',
                pool=0,
                controller_pid=33,
                hash='orphan-hash',
                entrypoint='entry'))
            session.commit()

        records = serve_state.get_service_liveness_snapshots(pool=False)

        assert [record['name'] for record in records] == ['serve-a']
        assert records[0]['status'] == serve_state.ServiceStatus.READY


# TestSqliteFenceBusySkip is deliberately not re-collected here: it pins
# sqlite-only busy-degradation semantics (the PG fence blocks on the FOR
# SHARE row lock instead of returning False).


class TestExpiredLeaseFenceMarkerPG(sqlite_suite.TestExpiredLeaseFenceMarker):
    pass


class TestMidQueryDemandBindDebitPG(sqlite_suite.TestMidQueryDemandBindDebit):
    pass


class TestMidQueryFillBindAttributionPG(
        sqlite_suite.TestMidQueryFillBindAttribution):
    pass


class TestFedLaunchBootSurvivalPG(sqlite_suite.TestFedLaunchBootSurvival):
    pass


class TestDrainWindowConservationPG(sqlite_suite.TestDrainWindowConservation):
    pass


# ========================= Migration chain on PG =========================


class TestMigrationChainPG:

    def test_full_chain_creates_broker_tables_and_reruns_idempotently(
            self, pg_server):
        """Fresh PG database -> the exact upgrade path create_table runs.

        safe_alembic_upgrade to SERVE_VERSION must create the three broker
        tables and the phantom_streak column; re-running it must be a
        no-op (idempotent), matching every controller process calling
        create_table on the same shared database.
        """
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            for _ in range(2):  # fresh upgrade, then idempotent re-run
                migration_utils.safe_alembic_upgrade(
                    engine, migration_utils.SERVE_DB_NAME,
                    migration_utils.SERVE_VERSION)
                inspector = sqlalchemy.inspect(engine)
                tables = set(inspector.get_table_names())
                assert {
                    'ephemeral_storage_cleanup_intents',
                    'reserved_fill_claims',
                    'reserved_fill_rounds',
                    'reserved_fill_lease',
                    'service_lifecycle_fences',
                    'demand_capacity_observations',
                    'serve_replica_status_history',
                    'serve_request_activity_history',
                }.issubset(tables), tables
                service_columns = {
                    column['name']
                    for column in inspector.get_columns('services')
                }
                assert {
                    'lifecycle_epoch',
                    'resource_scope',
                    'logical_replica_semantics',
                    'workspace',
                }.issubset(service_columns)
                version_columns = {
                    column['name']
                    for column in inspector.get_columns('version_specs')
                }
                assert {'created_at', 'created_by'} <= version_columns
                cleanup_intent_columns = {
                    column['name'] for column in inspector.get_columns(
                        'ephemeral_storage_cleanup_intents')
                }
                assert {
                    'lifecycle_epoch',
                    'provisional',
                    'resource_scope',
                    'storage_generation',
                    'yaml_content',
                }.issubset(cleanup_intent_columns)
                columns = {
                    column['name']
                    for column in inspector.get_columns('reserved_fill_rounds')
                }
                assert 'phantom_streak' in columns, columns
                assert 'shrink_baseline' in columns, columns
                assert 'fence_pending' in columns, columns
        finally:
            engine.dispose()


# =================== Aggregate Serve history on PG ====================


@pytest.fixture
def history_engine(pg_server, monkeypatch):
    url = _create_database(pg_server, f'history_{uuid.uuid4().hex[:8]}')
    engine = create_engine(url)
    serve_state.Base.metadata.create_all(engine)
    serve_history.metadata.create_all(engine)
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    yield engine
    engine.dispose()


class TestServeStatusHistoryPG:

    def test_snapshot_groups_physical_rows_and_zero_capacity(
            self, history_engine):
        services = serve_state.services_table
        replicas = serve_state.replicas_table
        with history_engine.begin() as connection:
            connection.execute(sqlalchemy.insert(services), [{
                'name': 'svc',
                'hash': 'hash-a',
                'current_version': 2,
                'pool': 0,
            }, {
                'name': 'empty',
                'hash': 'hash-empty',
                'current_version': 7,
                'pool': 0,
            }, {
                'name': 'pool',
                'hash': 'hash-pool',
                'current_version': 1,
                'pool': 1,
            }])
            connection.execute(sqlalchemy.insert(replicas), [{
                'service_name': 'svc',
                'replica_id': 1,
                'status': 'READY',
                'version': 1,
            }, {
                'service_name': 'svc',
                'replica_id': 2,
                'status': 'FAILED_PROBING',
                'version': 1,
            }, {
                'service_name': 'svc',
                'replica_id': 3,
                'status': 'PROVISIONING',
                'version': 2,
            }, {
                'service_name': 'pool',
                'replica_id': 1,
                'status': 'READY',
                'version': 1,
            }])

        timestamp = 1784207110.0
        assert serve_history.record_status_snapshot(timestamp) == 3
        history = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 1)
        assert [(row['version'], row['ready_count'], row['provisioning_count'],
                 row['errored_count'], row['total_count'])
                for row in history['samples']] == [
                    (1, 1, 0, 1, 2),
                    (2, 0, 1, 0, 1),
                ]
        empty = serve_history.get_status_history('empty',
                                                 timestamp=timestamp + 1)
        assert len(empty['samples']) == 1
        assert empty['samples'][0]['version'] == 7
        assert empty['samples'][0]['total_count'] == 0

    def test_same_minute_upsert_and_incarnation_filter(self, history_engine):
        services = serve_state.services_table
        replicas = serve_state.replicas_table
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(services).values(name='svc',
                                                   hash='old-hash',
                                                   current_version=1,
                                                   pool=0))
            connection.execute(
                sqlalchemy.insert(replicas).values(service_name='svc',
                                                   replica_id=1,
                                                   status='READY',
                                                   version=1))
        timestamp = 1784207110.0
        serve_history.record_status_snapshot(timestamp)
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(replicas).values(status='SHUTTING_DOWN'))
        serve_history.record_status_snapshot(timestamp + 30)
        old_history = serve_history.get_status_history('svc',
                                                       timestamp=timestamp + 31)
        assert len(old_history['samples']) == 1
        assert old_history['samples'][0]['ready_count'] == 0
        assert old_history['samples'][0]['stopping_count'] == 1

        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(services).values(hash='new-hash'))
            connection.execute(sqlalchemy.delete(replicas))
        serve_history.record_status_snapshot(timestamp + 70)
        current = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 71)
        assert current['service_hash'] == 'new-hash'
        assert len(current['samples']) == 1
        assert current['samples'][0]['total_count'] == 0

    def test_request_history_is_idempotent_additive_and_incarnation_scoped(
            self, history_engine):
        timestamp = 1784207110.0
        bucket_start = int(timestamp) // 60 * 60

        def request_history(count):
            return {
                'bucket_seconds': 60,
                'buckets': [{
                    'bucket_start': bucket_start,
                    'request_count': count,
                }],
            }

        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc', hash='hash-a', current_version=1, pool=0))

        assert serve_history.record_request_activity('svc', 'hash-a',
                                                     'pod-a:process-a',
                                                     request_history(3),
                                                     timestamp) == 1
        # Stale/out-of-order retry cannot decrement the exact counter.
        serve_history.record_request_activity('svc', 'hash-a',
                                              'pod-a:process-a',
                                              request_history(2), timestamp + 1)
        serve_history.record_request_activity('svc', 'hash-a',
                                              'pod-a:process-a',
                                              request_history(5), timestamp + 2)
        # A concurrently live maxSurge process receives distinct requests, so
        # its cumulative counter is additive.
        serve_history.record_request_activity('svc', 'hash-a',
                                              'pod-b:process-b',
                                              request_history(7), timestamp + 3)

        history = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 4)
        assert history['request_samples'] == [{
            'timestamp': float(bucket_start),
            'request_count': 12,
        }]
        assert history['requests_last_hour'] == 12
        assert history['request_window_seconds'] == 3600

        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    serve_state.services_table).values(hash='hash-b'))
        current = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 5)
        assert current['service_hash'] == 'hash-b'
        assert current['request_samples'] == []
        assert current['requests_last_hour'] == 0

    def test_hourly_snapshot_prunes_rows_older_than_three_days(
            self, history_engine):
        now = 1784210400.0  # Exact UTC hour, so the bounded cleanup runs.
        old_bucket = datetime.datetime.fromtimestamp(
            now - 73 * 3600, datetime.timezone.utc).replace(second=0,
                                                            microsecond=0)
        table = serve_history.serve_replica_status_history_table
        request_table = serve_history.serve_request_activity_history_table
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc', hash='hash', current_version=1, pool=0))
            connection.execute(
                sqlalchemy.insert(table).values(service_name='old',
                                                service_hash='old-hash',
                                                version=1,
                                                bucket_start=old_bucket,
                                                observed_at=old_bucket,
                                                ready_count=1,
                                                provisioning_count=0,
                                                not_ready_count=0,
                                                errored_count=0,
                                                preempted_count=0,
                                                stopping_count=0,
                                                total_count=1))
            connection.execute(
                sqlalchemy.insert(request_table).values(
                    service_name='old',
                    service_hash='old-hash',
                    reporter_session_id='pod:process',
                    bucket_start=old_bucket,
                    observed_at=old_bucket,
                    request_count=1))

        serve_history.record_status_snapshot(now)

        with history_engine.connect() as connection:
            assert connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(table).where(
                    table.c.service_name == 'old')).scalar_one() == 0
            assert connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(request_table).where(
                    request_table.c.service_name == 'old')).scalar_one() == 0


# ======================= Concurrency smoke on PG =======================


@pytest.fixture
def _pg_concurrency_db(pg_server, monkeypatch):
    """PG serve DB with the REAL lock path (no nullcontext, no fake clock).

    locks.get_lock detects the backend (and PostgresLock borrows its
    advisory-lock connection) through global_user_state's engine; pointing
    that at the serve PG engine takes the advisory path exactly as a
    Postgres-backed api-server pod does.
    """
    url = _create_database(pg_server, f'concurrency_{uuid.uuid4().hex[:8]}')
    engine = create_engine(url)
    serve_state.Base.metadata.create_all(engine)
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    monkeypatch.setattr(locks.global_user_state, 'initialize_and_get_db',
                        lambda: engine)
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        mock.Mock(return_value=[]))
    broker.clear_caches()
    yield engine
    broker.clear_caches()
    engine.dispose()


@pytest.mark.usefixtures('_pg_concurrency_db')
class TestConcurrentRoundsPG:

    def test_exactly_one_publisher_per_round(self):
        """Two pollers race run_round_if_stale: one drives, one reads.

        Exercises for real the two production serializers: the PostgresLock
        advisory round lock (the reader blocks while the publisher holds
        it) and the lease-epoch publish CAS (a double-publish would make
        one publish return False and its allocation None). The query
        sleeps to hold the lock across the whole race window; three
        iterations on fresh pools keep the smoke deterministic-enough.
        """
        for iteration in range(3):
            pool = broker.make_pool_key(f'ctx-{iteration}', 'A100')
            for name in ('svc-a', 'svc-b'):
                broker.upsert_claim(name,
                                    pool_key=pool,
                                    weight=1.0,
                                    floor_replicas=0,
                                    gpus_per_replica=1,
                                    holdings_fill=0,
                                    launchable=True)
            query_calls = []
            barrier = threading.Barrier(2)
            results = {}
            errors = []

            def query():
                query_calls.append(1)
                # Hold the round (and thus the advisory lock) long enough
                # that the losing thread must actually wait on it.
                time.sleep(0.3)
                return broker.PoolObservation(free_slots=10,
                                              gpu_names=('A100',))

            def run(name, pool=pool):
                try:
                    barrier.wait(timeout=30)
                    results[name] = broker.run_round_if_stale(
                        name, pool, query, poll_interval_seconds=300.0)
                except Exception as e:  # pylint: disable=broad-except
                    errors.append(e)

            threads = [
                threading.Thread(target=run, args=(name,))
                for name in ('svc-a', 'svc-b')
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=120)
                assert not thread.is_alive(), 'round thread hung'
            assert not errors, errors
            # Exactly one thread drove the round; the other read it fresh
            # under the lock without re-querying the cluster.
            assert len(query_calls) == 1
            alloc_a, alloc_b = results['svc-a'], results['svc-b']
            assert alloc_a is not None and alloc_b is not None
            assert alloc_a.round_id == 1 and alloc_b.round_id == 1
            assert alloc_a.epoch == alloc_b.epoch
            round_row = serve_state.get_reserved_fill_round(pool)
            assert round_row is not None
            assert int(round_row['round_id']) == 1
