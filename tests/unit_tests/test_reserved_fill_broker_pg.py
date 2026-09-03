"""Real-Postgres layer for the reserved-fill broker tests.

Production serve-state runs on Postgres (SKYPILOT_DB_CONNECTION_URI), so the
broker SQL (dialect-switched upserts, lease CAS rowcount, prune predicate)
and the alembic migration chain must be exercised on a real PG engine, not
only sqlite. This module:

- boots a throwaway postgres:16 container once per session via
  testcontainers (skipping locally when Docker is unavailable, but failing
  the required unit-test CI lane);
- re-runs the FULL sqlite round-mechanics/claims/prune/lease/epoch suite
  from test_reserved_fill_broker.py against the PG engine by overriding its
  `broker_engine` fixture (the test bodies are inherited, not copied);
- runs the alembic chain exactly as serve_state.create_table does on a
  fresh database, twice (idempotency);
- races two threads through run_round_if_stale so the publish CAS and the
  PostgresLock advisory path are exercised for real.
"""
# pylint: disable=cell-var-from-loop,missing-class-docstring
# pylint: disable=protected-access,redefined-outer-name,unexpected-keyword-arg
# pylint: disable=unused-import
import contextlib
import datetime
import importlib
import os
import shutil
import threading
import time
from unittest import mock
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from test_reserved_fill_broker import _broker_db  # noqa: F401
from test_reserved_fill_broker import clock  # noqa: F401
# The sqlite suite: its DB-touching test classes are re-collected below
# against the PG `broker_engine` override; `clock` and `_broker_db` are the
# fixtures those inherited test bodies request (importing a fixture function
# registers it in this module, where fixture resolution picks up the PG
# `broker_engine` defined here instead of the sqlite one).
import test_reserved_fill_broker as sqlite_suite

from sky import clouds
from sky import estimated_spend
from sky import global_user_state
from sky import global_user_state_schema
from sky.serve import capacity_admission_schema
from sky.serve import constants
from sky.serve import lb_ha
from sky.serve import ordinary_launch_binding
from sky.serve import paid_capacity
from sky.serve import placement_contract_normalization
from sky.serve import placement_history
from sky.serve import placement_policy
from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker as broker
from sky.serve import serve_history
from sky.serve import serve_state
from sky.serve import service_spec
from sky.serve import spot_placer
from sky.utils import common_utils
from sky.utils import locks
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_POSTGRES_REQUIRED = os.environ.get('SKYPILOT_REQUIRE_SERVE_POSTGRES') == '1'
_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')


def _required_import(module: str):
    try:
        return importlib.import_module(module)
    except ImportError:
        if _POSTGRES_REQUIRED:
            pytest.fail(f'{module} is required for Serve PostgreSQL tests.',
                        pytrace=False)
        pytest.skip(f'{module} unavailable; skipping real-PostgreSQL tests.',
                    allow_module_level=True)


psycopg2 = _required_import('psycopg2')
testcontainers_postgres = _required_import('testcontainers.postgres')

_PG_IMAGE = 'postgres:16'
_DOCKER_UNAVAILABLE = shutil.which('docker') is None

pytestmark = pytest.mark.skipif(
    _DOCKER_UNAVAILABLE and not _POSTGRES_REQUIRED,
    reason='docker unavailable; skipping real-Postgres broker tests')
if _DOCKER_UNAVAILABLE and _POSTGRES_REQUIRED:
    pytest.fail('Docker is required for Serve PostgreSQL tests.', pytrace=False)


@pytest.fixture(autouse=True)
def _isolate_disposable_database_migrations(monkeypatch):
    """Do not serialize migrations across independent throwaway databases."""

    @contextlib.contextmanager
    def _unlocked(_section):
        yield

    monkeypatch.setattr(migration_utils, 'db_lock', _unlocked)


def _service_spec(*, pool: bool = False) -> service_spec.SkyServiceSpec:
    return service_spec.SkyServiceSpec.from_yaml_config({
        'pool': {},
        'workers': 1,
    } if pool else {
        'replicas': 1,
    })


def _paid_cap_service_spec(limit: int) -> service_spec.SkyServiceSpec:
    return service_spec.SkyServiceSpec(
        readiness_path='/health',
        initial_delay_seconds=60,
        readiness_timeout_seconds=30,
        endpoint_probe_interval_seconds=10,
        lb_stream_timeout_seconds=60,
        min_replicas=0,
        max_replicas=32,
        target_concurrency_per_replica=1,
        graceful_drain_async_occupancy=True,
        spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
        max_live_paid_gpu_units=limit)


def _v1_service_spec() -> service_spec.SkyServiceSpec:
    spec = _service_spec()
    contract = spec.placement_contract
    spec.__dict__.update(contract._legacy_v1_persisted_fields())
    spec.__dict__[placement_policy.ROLLBACK_REPLICA_UNIT_FIELD] = False
    return spec


@pytest.fixture(scope='session')
def pg_server():
    """One throwaway postgres:16 container for the whole session.

    Yields the started PostgresContainer (testcontainers handles port
    mapping and readiness). Local runs skip when the container cannot start;
    the required unit-test CI lane fails instead.
    """
    if _POSTGRES_URL is not None:
        yield _POSTGRES_URL
        return
    container = testcontainers_postgres.PostgresContainer(_PG_IMAGE)
    try:
        container.start()
    except Exception as e:  # pylint: disable=broad-except
        if _POSTGRES_REQUIRED:
            pytest.fail(f'could not start required postgres container: {e}',
                        pytrace=False)
        pytest.skip(f'could not start postgres container: {e}')
    try:
        yield container
    finally:
        container.stop()


def _create_database(pg_server,
                     dbname: str,
                     *,
                     template: str | None = None) -> str:
    """Creates a fresh database on the server; returns its SQLAlchemy URL."""
    if isinstance(pg_server, str):
        admin_engine = create_engine(pg_server, isolation_level='AUTOCOMMIT')
        quoted = admin_engine.dialect.identifier_preparer.quote(dbname)
        template_clause = ''
        if template is not None:
            quoted_template = admin_engine.dialect.identifier_preparer.quote(
                template)
            template_clause = f' TEMPLATE {quoted_template}'
        try:
            with admin_engine.connect() as connection:
                connection.exec_driver_sql(
                    f'CREATE DATABASE {quoted}{template_clause}')
        finally:
            admin_engine.dispose()
        database_url = sqlalchemy.engine.make_url(pg_server).set(
            database=dbname).render_as_string(hide_password=False)
        engine = create_engine(database_url)
        try:
            global_user_state_schema.user_table.create(engine, checkfirst=True)
        finally:
            engine.dispose()
        return database_url
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
            template_clause = ('' if template is None else
                               f' TEMPLATE "{template}"')
            cursor.execute(f'CREATE DATABASE "{dbname}"{template_clause}')
    finally:
        conn.close()
    database_url = (f'postgresql://{pg_server.username}:{pg_server.password}'
                    f'@{host}:{port}/{dbname}')
    engine = create_engine(database_url)
    try:
        global_user_state_schema.user_table.create(engine, checkfirst=True)
    finally:
        engine.dispose()
    return database_url


def _drop_database(pg_server, dbname: str) -> None:
    """Drop one isolated test database after terminating its test sessions."""
    if isinstance(pg_server, str):
        admin_engine = create_engine(pg_server, isolation_level='AUTOCOMMIT')
        quoted = admin_engine.dialect.identifier_preparer.quote(dbname)
        try:
            with admin_engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text('SELECT pg_terminate_backend(pid) '
                                    'FROM pg_stat_activity '
                                    'WHERE datname = :database '
                                    'AND pid <> pg_backend_pid()'),
                    {'database': dbname})
                connection.exec_driver_sql(f'DROP DATABASE {quoted}')
        finally:
            admin_engine.dispose()
        return
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
            cursor.execute(
                'SELECT pg_terminate_backend(pid) FROM pg_stat_activity '
                'WHERE datname = %s AND pid <> pg_backend_pid()', (dbname,))
            cursor.execute(f'DROP DATABASE "{dbname}"')
    finally:
        conn.close()


@pytest.fixture(scope='session')
def _pg_mechanics_template(pg_server):
    """One current-head template cloned for isolated mechanics tests."""
    database_name = f'broker_template_{uuid.uuid4().hex[:10]}'
    url = _create_database(pg_server, database_name)
    engine = create_engine(url)
    try:
        migration_utils.safe_alembic_upgrade(engine,
                                             migration_utils.SERVE_DB_NAME,
                                             migration_utils.SERVE_VERSION)
        # Final PostgreSQL service deletion performs its request-layer
        # census (api_requests, queue and retention pins) on the same
        # database as the Serve tables, exactly as the central deployment
        # does; the template must therefore carry both schemas.
        migration_utils.safe_alembic_upgrade(
            engine, migration_utils.API_REQUESTS_DB_NAME,
            migration_utils.API_REQUESTS_VERSION)
    finally:
        engine.dispose()
    try:
        yield database_name
    finally:
        _drop_database(pg_server, database_name)


@pytest.fixture
def broker_engine(pg_server, _pg_mechanics_template):
    """Overrides the sqlite suite's engine with a real-PG one.

    The session fixture installs the exact current migration once. Each test
    clones that template, so append-only triggers and every other PostgreSQL
    contract stay enabled without sharing data between test cases.
    """
    database_name = f'broker_case_{uuid.uuid4().hex[:10]}'
    url = _create_database(pg_server,
                           database_name,
                           template=_pg_mechanics_template)
    engine = create_engine(url)
    try:
        yield engine
    finally:
        engine.dispose()
        _drop_database(pg_server, database_name)


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


@pytest.mark.usefixtures('_broker_db')
class TestPlacementContractWriteBoundaryPG:

    def test_new_writes_reject_v1_and_store_only_raw_v2(self):
        with pytest.raises(ValueError, match='mirror-free v2'):
            serve_state.add_service(name='legacy-registration',
                                    controller_job_id=1,
                                    policy='policy',
                                    requested_resources_str='1x[CPU:1+]',
                                    load_balancing_policy='round_robin',
                                    status=serve_state.ServiceStatus.READY,
                                    tls_encrypted=False,
                                    pool=False,
                                    controller_pid=11,
                                    entrypoint='entry',
                                    spec=_v1_service_spec(),
                                    yaml_content='service: {}')
        engine = serve_state._db_manager.get_engine()
        with sqlalchemy.orm.Session(engine) as session:
            assert session.execute(
                sqlalchemy.select(serve_state.services_table.c.name).where(
                    serve_state.services_table.c.name ==
                    'legacy-registration')).first() is None

        assert serve_state.add_service(name='v2-boundary',
                                       controller_job_id=1,
                                       policy='policy',
                                       requested_resources_str='1x[CPU:1+]',
                                       load_balancing_policy='round_robin',
                                       status=serve_state.ServiceStatus.READY,
                                       tls_encrypted=False,
                                       pool=False,
                                       controller_pid=11,
                                       entrypoint='entry',
                                       spec=_service_spec(),
                                       yaml_content='service: {}')
        assert serve_state.add_version('v2-boundary') == 2
        with pytest.raises(ValueError, match='mirror-free v2'):
            serve_state.add_or_update_version('v2-boundary', 2,
                                              _v1_service_spec(),
                                              'service: {legacy: true}')
        with sqlalchemy.orm.Session(engine) as session:
            placeholder = session.execute(
                sqlalchemy.select(serve_state.version_specs_table).where(
                    serve_state.version_specs_table.c.service_name ==
                    'v2-boundary', serve_state.version_specs_table.c.version ==
                    2)).mappings().one()
        assert placeholder['yaml_content'] is None

        assert serve_state.add_or_update_version(
            'v2-boundary', 2, _service_spec(), 'service: {current: true}') is (
                serve_state.VersionCommitResult.COMMITTED)
        with sqlalchemy.orm.Session(engine) as session:
            payload = session.execute(
                sqlalchemy.select(serve_state.version_specs_table.c.spec).where(
                    serve_state.version_specs_table.c.service_name ==
                    'v2-boundary', serve_state.version_specs_table.c.version ==
                    2)).scalar_one()
        assert placement_contract_normalization.analyze_spec_pickle(
            payload).classification is (
                placement_contract_normalization.Classification.EXPLICIT_V2)


class TestSingleClaimantFastPathPG(sqlite_suite.TestSingleClaimantFastPath):
    pass


class TestMultiClaimantRoundsPG(sqlite_suite.TestMultiClaimantRounds):
    pass


class TestBlackoutPG(sqlite_suite.TestBlackout):
    pass


class TestClaimLifecyclePG(sqlite_suite.TestClaimLifecycle):

    def test_service_teardown_requires_lifecycle_fence(self):
        # PostgreSQL final deletion is one same-name authority census.  A
        # non-pool service row whose durable name fence is missing cannot
        # prove which incarnation is retiring, so the whole transaction
        # must roll back: the claim row is the observable witness that no
        # child row was deleted before the barrier failed.
        sqlite_suite._upsert('svc-a')
        engine = serve_state._db_manager.get_engine()
        with sqlalchemy.orm.Session(engine) as session:
            session.execute(serve_state.services_table.insert().values(
                name='svc-a',
                hash='incarnation-a',
                lifecycle_epoch=1,
                status=serve_state.ServiceStatus.SHUTTING_DOWN.value))
            session.commit()
        with pytest.raises(
                ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                match='lifecycle fence'):
            serve_state.remove_service_completely('svc-a', 'incarnation-a')
        with sqlalchemy.orm.Session(engine) as session:
            remaining = session.execute(
                sqlalchemy.select(serve_state.services_table.c.name).where(
                    serve_state.services_table.c.name == 'svc-a')).scalar()
        assert remaining == 'svc-a'
        assert {
            row['service_name'] for row in serve_state.get_reserved_fill_claims(
                pool_key=sqlite_suite._POOL)
        } == {'svc-a'}


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


class TestConnectionLocalPaidAdmissionPG(
        sqlite_suite.TestConnectionLocalPaidAdmission):
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


class TestPaidCapacityAuthorityPG:
    """Global paid launch claims use real PostgreSQL locking semantics."""

    @staticmethod
    def _add_service(
            name: str,
            service_hash: str,
            pid: int,
            *,
            spec_override: service_spec.SkyServiceSpec | None = None) -> None:
        assert serve_state.add_service(name=name,
                                       controller_job_id=1,
                                       policy='policy',
                                       requested_resources_str='1x[AWS(L4):1]',
                                       load_balancing_policy='round_robin',
                                       status=serve_state.ServiceStatus.READY,
                                       tls_encrypted=False,
                                       pool=False,
                                       controller_pid=pid,
                                       entrypoint='entry',
                                       spec=(spec_override or _service_spec()),
                                       yaml_content='service: {}',
                                       controller_ip='10.0.0.1',
                                       service_hash=service_hash,
                                       resource_scope=f'scope-{name}')

    @staticmethod
    def _info(service_name: str,
              replica_id: int) -> replica_managers.ReplicaInfo:
        return replica_managers.ReplicaInfo(
            replica_id=replica_id,
            cluster_name=f'{service_name}-{replica_id}',
            replica_port='8080',
            is_spot=True,
            location=None,
            version=1,
            resources_override={'use_spot': True})

    @staticmethod
    def _paid_pool(
        zone: str,
        instance_type: str,
        *,
        accelerator: str = 'L4',
        accelerator_count: int = 1,
    ) -> tuple[spot_placer.Location, str]:
        location = spot_placer.Location(
            cloud=clouds.AWS(),
            region='us-east-1',
            zone=zone,
            accelerators={accelerator: accelerator_count},
            use_spot=True,
            instance_type=instance_type)
        return location, paid_capacity.pool_key(location,
                                                workspace='workspace',
                                                num_nodes=1,
                                                aws_account_id='123456789012')

    @classmethod
    def _test_pool(cls,
                   alias: str,
                   *,
                   accelerator: str = 'L4',
                   accelerator_count: int = 1) -> str:
        """Return a canonical exact provider pool for a readable test alias."""
        _, pool_key = cls._paid_pool(f'test-zone-{alias}',
                                     f'test-instance-{alias}',
                                     accelerator=accelerator,
                                     accelerator_count=accelerator_count)
        return pool_key

    @classmethod
    def _paid_batch_spec(
        cls,
        replica_id: int,
        pool_key: str,
        *,
        frontier_key: paid_capacity.FrontierKey = ('l4',),
        planner_bound: bool = False,
    ) -> paid_capacity.PaidClaimPersistenceSpec:
        info = cls._info('svc', replica_id)
        capacity_plan_claim = None
        if planner_bound:
            capacity_plan_claim = {
                'capacity_plan_generation': 1,
                'capacity_plan_sha256': 'a' * 64,
                'demand_feed_generation': 1,
                'demand_source_epoch': 1,
                'capacity_plan_accelerator': 'l4',
                'capacity_plan_units': 1,
            }
        candidate = paid_capacity.PaidClaimCandidate(
            replica_id=replica_id,
            replica_info=info,
            location=None,  # type: ignore[arg-type]
            priority=20,
            capacity_plan_claim=capacity_plan_claim)
        return paid_capacity.PaidClaimPersistenceSpec(candidate=candidate,
                                                      pool_key=pool_key,
                                                      frontier_key=frontier_key,
                                                      frontier_limit=100)

    def test_atomic_paid_batch_commits_large_wave_once(self, broker_engine,
                                                       monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        specs = [
            self._paid_batch_spec(replica_id, self._test_pool('pool'))
            for replica_id in range(1, 101)
        ]
        validate = mock.Mock()
        monkeypatch.setattr(serve_state.capacity_admission,
                            'validate_paid_claim_in_connection', validate)
        results = serve_state.try_add_replicas_with_paid_capacity_claims(
            'svc',
            'hash',
            specs,
            base_limit=100,
            max_limit=100,
            service_limit=100,
            now=100,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1'),
            frontier_default_limit=100)

        assert results == ['acquired'] * 100
        assert validate.call_count == 100
        with sqlalchemy.orm.Session(broker_engine) as session:
            replica_count = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name ==
                    'svc')).scalar_one()
            claim_count = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(serve_state.paid_capacity_claims_table).where(
                    serve_state.paid_capacity_claims_table.c.service_name ==
                    'svc')).scalar_one()
        assert replica_count == 100
        assert claim_count == 100

    def test_atomic_paid_batch_saturation_continues_and_waiter_wins(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        pool_a = self._test_pool('pool-a')
        pool_b = self._test_pool('pool-b')
        specs = [
            self._paid_batch_spec(1, pool_b),
            self._paid_batch_spec(2, pool_b),
            self._paid_batch_spec(3, pool_a),
        ]
        lock_order = []
        lock_pool = serve_state._paid_capacity_pool_row_for_update

        def _record_lock(session, pool_key):
            lock_order.append(pool_key)
            return lock_pool(session, pool_key)

        monkeypatch.setattr(serve_state, '_paid_capacity_pool_row_for_update',
                            _record_lock)

        results = serve_state.try_add_replicas_with_paid_capacity_claims(
            'svc',
            'hash',
            specs,
            base_limit=1,
            max_limit=1,
            service_limit=10,
            now=100,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1'),
            frontier_default_limit=100)

        assert results == ['acquired', 'saturated', 'acquired']
        assert lock_order == [pool_a, pool_b]
        with sqlalchemy.orm.Session(broker_engine) as session:
            claims = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.replica_id).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc').order_by(serve_state.paid_capacity_claims_table.
                                        c.replica_id)).scalars().all()
            waiters = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table.c.pool_key).where(
                        serve_state.paid_capacity_waiters_table.c.service_name
                        == 'svc')).scalars().all()
        assert claims == [1, 3]
        assert waiters == [pool_b]

    def test_atomic_paid_batch_locks_all_pools_before_stale_claim_cleanup(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        pool_a = self._test_pool('pool-a')
        pool_z = self._test_pool('pool-z')
        with sqlalchemy.orm.Session(broker_engine) as session:
            session.execute(
                serve_state.paid_capacity_pools_table.insert().values(
                    pool_key=pool_z,
                    current_limit=1,
                    successes_since_resize=0,
                    updated_at=1))
            session.execute(
                serve_state.paid_capacity_claims_table.insert().values(
                    service_name='svc',
                    service_hash='hash',
                    replica_id=999,
                    pool_key=pool_z,
                    priority=20,
                    claimed_at=1))
            session.commit()

        events = []
        lock_pool = serve_state._paid_capacity_pool_row_for_update
        delete_claims = serve_state._delete_paid_capacity_claims_in_session

        def _record_lock(session, pool_key):
            events.append(f'lock:{pool_key}')
            return lock_pool(session, pool_key)

        def _record_delete(session, identities):
            if identities:
                events.append('delete')
            return delete_claims(session, identities)

        monkeypatch.setattr(serve_state, '_paid_capacity_pool_row_for_update',
                            _record_lock)
        monkeypatch.setattr(serve_state,
                            '_delete_paid_capacity_claims_in_session',
                            _record_delete)

        results = serve_state.try_add_replicas_with_paid_capacity_claims(
            'svc',
            'hash', [self._paid_batch_spec(1, pool_a)],
            base_limit=1,
            max_limit=1,
            service_limit=10,
            now=100,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1'),
            frontier_default_limit=100)

        assert results == ['acquired']
        assert events[:3] == [f'lock:{pool_a}', f'lock:{pool_z}', 'delete']

    def test_atomic_paid_batch_plan_conflict_rolls_back_phase_a(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        pool_a = self._test_pool('pool-a')
        pool_b = self._test_pool('pool-b')
        specs = [
            self._paid_batch_spec(1, pool_b),
            self._paid_batch_spec(2, pool_a),
        ]
        monkeypatch.setattr(
            serve_state.capacity_admission, 'validate_paid_claim_in_connection',
            mock.Mock(side_effect=serve_state.capacity_admission.
                      CapacityAdmissionConflict('stale plan')))

        with pytest.raises(
                serve_state.capacity_admission.CapacityAdmissionConflict,
                match='stale plan'):
            serve_state.try_add_replicas_with_paid_capacity_claims(
                'svc',
                'hash',
                specs,
                base_limit=2,
                max_limit=2,
                service_limit=10,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'),
                frontier_default_limit=100)

        with sqlalchemy.orm.Session(broker_engine) as session:
            assert session.execute(
                sqlalchemy.select(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name ==
                    'svc')).first() is None
            assert session.execute(
                sqlalchemy.select(serve_state.paid_capacity_claims_table).where(
                    serve_state.paid_capacity_claims_table.c.service_name ==
                    'svc')).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table).where(
                        serve_state.paid_capacity_waiters_table.c.service_name
                        == 'svc')).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_pools_table)).first() is None

    def test_legacy_paid_batch_rejects_promoted_service_without_writes(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        with broker_engine.begin() as connection:
            controller_incarnation = connection.execute(
                sqlalchemy.select(
                    serve_state.services_table.c.controller_incarnation).
                where(serve_state.services_table.c.name == 'svc')).scalar_one()
            connection.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == 'svc').values(
                        demand_source_mode='DURABLE_FEED',
                        demand_source_epoch=1,
                        demand_authority_capable=True,
                        demand_authority_controller_incarnation=(
                            controller_incarnation),
                        demand_authority_protocol_version=1))
        _, pool_key = self._paid_pool('us-east-1a', 'g6.xlarge')

        with pytest.raises(
                ValueError,
                match='Prospective durable paid capacity requires fused '
                'admission'):
            serve_state.try_add_replicas_with_paid_capacity_claims(
                'svc',
                'hash', [self._paid_batch_spec(1, pool_key)],
                base_limit=1,
                max_limit=1,
                service_limit=10,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'),
                frontier_default_limit=100)

        with sqlalchemy.orm.Session(broker_engine) as session:
            for table in (serve_state.replicas_table,
                          serve_state.paid_capacity_claims_table,
                          serve_state.paid_capacity_waiters_table,
                          serve_state.paid_capacity_pools_table):
                assert session.execute(sqlalchemy.select(table)).first() is None

    def test_atomic_paid_batch_replay_rejects_reserved_row_and_rolls_back(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        existing = self._paid_batch_spec(1, self._test_pool('pool-z'))
        assert serve_state.try_add_replicas_with_paid_capacity_claims(
            'svc',
            'hash', [existing],
            base_limit=2,
            max_limit=2,
            service_limit=10,
            now=100,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1'),
            frontier_default_limit=100) == ['acquired']

        with sqlalchemy.orm.Session(broker_engine) as session:
            stored_state = session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.replica_state).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        1)).scalar_one()
            reserved_state = dict(stored_state)
            reserved_state['reserved_fill'] = True
            reserved_state['is_zero_cost'] = False
            decoded = serve_state.decode_replica_state_for_authority(
                serve_state._REPLICA_STATE_VERSION, reserved_state)
            assert decoded.reserved_fill is True
            assert decoded.is_zero_cost is False
            session.execute(
                sqlalchemy.update(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id == 1).values(
                        replica_state=reserved_state))
            session.commit()

        with sqlalchemy.orm.Session(broker_engine) as session:
            replica_before = dict(
                session.execute(
                    sqlalchemy.select(serve_state.replicas_table).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        1)).mappings().one())
            claim_before = dict(
                session.execute(
                    sqlalchemy.select(
                        serve_state.paid_capacity_claims_table).where(
                            serve_state.paid_capacity_claims_table.c.
                            service_name == 'svc',
                            serve_state.paid_capacity_claims_table.c.replica_id
                            == 1)).mappings().one())
            pools_before = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_pools_table.c.pool_key).order_by(
                        serve_state.paid_capacity_pools_table.c.pool_key)
            ).scalars().all()
            waiters_before = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table.c.pool_key).
                order_by(serve_state.paid_capacity_waiters_table.c.pool_key
                        )).scalars().all()

        fresh = self._paid_batch_spec(2, self._test_pool('pool-a'))
        with pytest.raises(ValueError, match='zero-cost or reserved-fill row'):
            serve_state.try_add_replicas_with_paid_capacity_claims(
                'svc',
                'hash', [existing, fresh],
                base_limit=2,
                max_limit=2,
                service_limit=10,
                now=101,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'),
                frontier_default_limit=100)

        with sqlalchemy.orm.Session(broker_engine) as session:
            replica_after = dict(
                session.execute(
                    sqlalchemy.select(serve_state.replicas_table).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        1)).mappings().one())
            claim_after = dict(
                session.execute(
                    sqlalchemy.select(
                        serve_state.paid_capacity_claims_table).where(
                            serve_state.paid_capacity_claims_table.c.
                            service_name == 'svc',
                            serve_state.paid_capacity_claims_table.c.replica_id
                            == 1)).mappings().one())
            assert replica_after == replica_before
            assert claim_after == claim_before
            assert session.execute(
                sqlalchemy.select(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id ==
                    2)).first() is None
            assert session.execute(
                sqlalchemy.select(serve_state.paid_capacity_claims_table).where(
                    serve_state.paid_capacity_claims_table.c.service_name ==
                    'svc', serve_state.paid_capacity_claims_table.c.replica_id
                    == 2)).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_pools_table.c.pool_key).order_by(
                        serve_state.paid_capacity_pools_table.c.pool_key)
            ).scalars().all() == pools_before
            assert session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table.c.pool_key).
                order_by(serve_state.paid_capacity_waiters_table.c.pool_key
                        )).scalars().all() == waiters_before

    def test_atomic_paid_batch_member_insert_fault_rolls_back_phase_a(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        pool_a = self._test_pool('pool-a')
        pool_b = self._test_pool('pool-b')
        replica_inserts = 0

        def _fail_second_replica_insert(conn, cursor, statement, parameters,
                                        context, executemany):
            del conn, cursor, parameters, context, executemany
            nonlocal replica_inserts
            if statement.lstrip().startswith('INSERT INTO replicas'):
                replica_inserts += 1
                if replica_inserts == 2:
                    raise RuntimeError('injected second member insert fault')

        sqlalchemy.event.listen(broker_engine, 'before_cursor_execute',
                                _fail_second_replica_insert)
        try:
            with pytest.raises(RuntimeError,
                               match='injected second member insert fault'):
                serve_state.try_add_replicas_with_paid_capacity_claims(
                    'svc',
                    'hash', [
                        self._paid_batch_spec(1, pool_b),
                        self._paid_batch_spec(2, pool_a),
                    ],
                    base_limit=2,
                    max_limit=2,
                    service_limit=10,
                    now=100,
                    success_ttl_seconds=60,
                    waiter_ttl_seconds=30,
                    expected_controller_owner=(11, '10.0.0.1'),
                    frontier_default_limit=100)
        finally:
            sqlalchemy.event.remove(broker_engine, 'before_cursor_execute',
                                    _fail_second_replica_insert)

        assert replica_inserts == 2
        with sqlalchemy.orm.Session(broker_engine) as session:
            assert session.execute(
                sqlalchemy.select(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name ==
                    'svc')).first() is None
            assert session.execute(
                sqlalchemy.select(serve_state.paid_capacity_claims_table).where(
                    serve_state.paid_capacity_claims_table.c.service_name ==
                    'svc')).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table).where(
                        serve_state.paid_capacity_waiters_table.c.service_name
                        == 'svc')).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_pools_table)).first() is None

    def test_outcome_persistence_reports_only_committed_claim_pools(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        _, pool_key = self._paid_pool('us-east-1a', 'g6.xlarge')
        claimed = self._info('svc', 1)
        unclaimed = self._info('svc', 2)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            1,
            claimed,
            pool_key=pool_key,
            priority=20,
            base_limit=2,
            max_limit=8,
            now=100,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        assert serve_state.add_or_update_replica('svc', 2, unclaimed)
        claimed.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        unclaimed.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        applied_pool_keys: set[str] = set()

        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(1, claimed), (2, unclaimed)], {
                1: paid_capacity.LaunchOutcome.CAPACITY_FAILURE,
                2: paid_capacity.LaunchOutcome.CAPACITY_FAILURE,
            },
            base_limit=2,
            max_limit=8,
            now=101,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'),
            applied_outcome_pool_keys=applied_pool_keys)
        assert applied_pool_keys == {pool_key}

        no_claim_pool_keys: set[str] = set()
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(1, claimed)],
            {1: paid_capacity.LaunchOutcome.CAPACITY_FAILURE},
            base_limit=2,
            max_limit=8,
            now=102,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'),
            applied_outcome_pool_keys=no_claim_pool_keys)
        assert not no_claim_pool_keys

    def test_priority_waiter_and_success_failure_ramp(self, broker_engine,
                                                      monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('low', 'hash-low', 11)
        self._add_service('high', 'hash-high', 22)
        pool_key = self._test_pool('priority-ramp')

        def _claim(service_name, service_hash, pid, replica_id, priority, now):
            return serve_state.try_add_replica_with_paid_capacity_claim(
                service_name,
                service_hash,
                replica_id,
                self._info(service_name, replica_id),
                pool_key=pool_key,
                priority=priority,
                base_limit=2,
                max_limit=8,
                now=now,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'))

        assert _claim('low', 'hash-low', 11, 1, 20, 100) == 'acquired'
        assert _claim('low', 'hash-low', 11, 2, 20, 100) == 'acquired'
        assert _claim('high', 'hash-high', 22, 1, 50, 101) == 'saturated'

        low_one = serve_state.get_replica_info_from_id('low', 1)
        assert low_one is not None
        low_one.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'low',
            'hash-low', [(1, low_one)],
            {1: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=2,
            max_limit=8,
            now=102,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'))

        assert (_claim('low', 'hash-low', 11, 3, 20,
                       103) == 'higher_priority_waiting')
        assert _claim('high', 'hash-high', 22, 1, 50, 103) == 'acquired'

        low_two = serve_state.get_replica_info_from_id('low', 2)
        high_one = serve_state.get_replica_info_from_id('high', 1)
        assert low_two is not None and high_one is not None
        low_two.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        high_one.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'low',
            'hash-low', [(2, low_two)],
            {2: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=2,
            max_limit=8,
            now=104,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'))
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'high',
            'hash-high', [(1, high_one)],
            {1: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=2,
            max_limit=8,
            now=104,
            success_ttl_seconds=60,
            expected_controller_owner=(22, '10.0.0.1'))
        states = serve_state.get_paid_capacity_pool_states(
            [pool_key],
            base_limit=2,
            max_limit=8,
            now=104,
            success_ttl_seconds=60)
        assert states[pool_key]['current_limit'] == 4

        assert _claim('high', 'hash-high', 22, 2, 50, 105) == 'acquired'
        failed = serve_state.get_replica_info_from_id('high', 2)
        assert failed is not None
        failed.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'high',
            'hash-high', [(2, failed)],
            {2: paid_capacity.LaunchOutcome.CAPACITY_FAILURE},
            base_limit=2,
            max_limit=8,
            now=106,
            success_ttl_seconds=60,
            expected_controller_owner=(22, '10.0.0.1'))
        states = serve_state.get_paid_capacity_pool_states(
            [pool_key],
            base_limit=2,
            max_limit=8,
            now=106,
            success_ttl_seconds=60)
        assert states[pool_key]['current_limit'] == 2

    def test_equal_priority_waiters_break_ties_by_first_wait_time(
            self, broker_engine, monkeypatch):
        # The best-waiter query orders by ``priority DESC, first_wait_at,
        # service_name``. When two services wait at the same priority for one
        # freed slot, the one that started waiting first must win, even if its
        # service name sorts last. The service names are chosen so a lost
        # ``first_wait_at`` tiebreaker (falling back to ``service_name``) would
        # award the slot to the later waiter instead.
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('holder', 'hash-holder', 11)
        self._add_service('z-early', 'hash-early', 22)
        self._add_service('a-late', 'hash-late', 33)
        owners = {
            'holder': ('hash-holder', 11),
            'z-early': ('hash-early', 22),
            'a-late': ('hash-late', 33),
        }

        def _claim(name: str, replica_id: int, now: float) -> str:
            service_hash, pid = owners[name]
            return serve_state.try_add_replica_with_paid_capacity_claim(
                name,
                service_hash,
                replica_id,
                self._info(name, replica_id),
                pool_key=self._test_pool('pool'),
                priority=30,
                base_limit=1,
                max_limit=8,
                now=now,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'))

        # ``holder`` takes the sole slot; the other two register waiters at
        # distinct wait times while the pool is saturated.
        assert _claim('holder', 1, 100) == 'acquired'
        assert _claim('z-early', 1, 100) == 'saturated'
        assert _claim('a-late', 1, 101) == 'saturated'

        # Free the slot by resolving the holder's launch.
        holder = serve_state.get_replica_info_from_id('holder', 1)
        assert holder is not None
        holder.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'holder',
            'hash-holder', [(1, holder)],
            {1: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=1,
            max_limit=8,
            now=102,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'))

        # The later waiter still defers to the earlier one for the freed slot,
        # and the earlier waiter acquires it despite sorting last by name.
        assert _claim('a-late', 1, 103) == 'higher_priority_waiting'
        assert _claim('z-early', 1, 103) == 'acquired'

    def test_quota_failure_closes_pool_through_outcomes(self, broker_engine,
                                                        monkeypatch):
        # A typed QUOTA failure must close the shared pool exactly like a
        # capacity failure when driven through the integrated outcomes
        # transaction: zero admission during the cooldown, then a single probe.
        # The existing integrated failure tests only exercise CAPACITY_FAILURE.
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)

        first = self._info('svc', 1)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            1,
            first,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=100,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        first.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(1, first)],
            {1: paid_capacity.LaunchOutcome.QUOTA_FAILURE},
            base_limit=2,
            max_limit=8,
            now=110,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            expected_controller_owner=(11, '10.0.0.1'))

        closed = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=2,
            max_limit=8,
            now=115,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10)[self._test_pool('pool')]
        assert closed['admission_state'] == 'cooldown'
        assert closed['admission_limit'] == 0
        assert closed['remaining'] == 0
        assert closed['last_failure_at'] == 110
        assert closed['current_limit'] == 2

        # No new claim is admitted while the quota cooldown is in effect.
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            2,
            self._info('svc', 2),
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=115,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'saturated'

        # After the cooldown exactly one probe reopens.
        probing = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=2,
            max_limit=8,
            now=121,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10)[self._test_pool('pool')]
        assert probing['admission_state'] == 'probe'
        assert probing['admission_limit'] == 1
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            3,
            self._info('svc', 3),
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=121,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'

    def test_service_envelope_serializes_distinct_pool_claims(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        barrier = threading.Barrier(12)
        results = []
        errors = []

        def _claim(replica_id: int) -> None:
            try:
                barrier.wait(timeout=20)
                results.append(
                    serve_state.try_add_replica_with_paid_capacity_claim(
                        'svc',
                        'hash',
                        replica_id,
                        self._info('svc', replica_id),
                        pool_key=self._test_pool(f'pool-{replica_id}'),
                        priority=20,
                        base_limit=4,
                        max_limit=8,
                        service_limit=3,
                        now=100,
                        success_ttl_seconds=60,
                        waiter_ttl_seconds=30,
                        expected_controller_owner=(11, '10.0.0.1')))
            except Exception as e:  # pylint: disable=broad-except
                errors.append(e)

        threads = [
            threading.Thread(target=_claim, args=(replica_id,))
            for replica_id in range(1, 13)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
            assert not thread.is_alive(), 'service admission thread hung'

        assert not errors, errors
        assert results.count('acquired') == 3
        assert results.count('service_saturated') == 9
        with sqlalchemy.orm.Session(broker_engine) as session:
            claim_pool_keys = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.pool_key).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc')).scalars().all()
            pool_keys = session.execute(
                sqlalchemy.select(serve_state.paid_capacity_pools_table.c.
                                  pool_key)).scalars().all()
            replica_ids = session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.replica_id).where(
                        serve_state.replicas_table.c.service_name ==
                        'svc')).scalars().all()
            waiter_count = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(
                    serve_state.paid_capacity_waiters_table)).scalar_one()
        assert len(claim_pool_keys) == 3
        assert set(pool_keys) == set(claim_pool_keys)
        assert len(replica_ids) == 3
        assert waiter_count == 0

    def test_paid_gpu_cap_zero_rejects_without_creating_state(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc',
                          'hash',
                          11,
                          spec_override=_paid_cap_service_spec(0))

        result = serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            1,
            self._info('svc', 1),
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=4,
            max_limit=8,
            max_live_paid_gpu_units=0,
            now=100,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1'))
        stale_caller_result = (
            serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                2,
                self._info('svc', 2),
                pool_key=self._test_pool('pool'),
                priority=20,
                base_limit=4,
                max_limit=8,
                max_live_paid_gpu_units=None,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1')))

        assert result == 'service_saturated'
        assert stale_caller_result == 'service_saturated'
        assert serve_state.get_replica_info_from_id('svc', 1) is None
        assert serve_state.get_replica_info_from_id('svc', 2) is None
        with sqlalchemy.orm.Session(broker_engine) as session:
            assert session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(
                    serve_state.paid_capacity_claims_table)).scalar_one() == 0
            assert session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(
                    serve_state.paid_capacity_pools_table)).scalar_one() == 0

    def test_paid_gpu_cap_fails_closed_on_unattributable_live_rows(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc',
                          'hash',
                          11,
                          spec_override=_paid_cap_service_spec(8))
        for replica_id in range(1, 7):
            assert serve_state.add_or_update_replica(
                'svc', replica_id, self._info('svc', replica_id))
        retained_states = {
            # Diverse malformed pre-attribution JSON shapes must all fail
            # closed while provider cleanup remains unproven.
            1: {},
            2: {
                'is_zero_cost': None,
                'planned_capacity': 2,
            },
            3: {
                'is_zero_cost': 'true',
                'planned_capacity': '8',
            },
            4: {
                'is_zero_cost': {},
                'planned_capacity': 3,
            },
            # A JSON-only zero-cost marker cannot override the relational
            # attribution columns used by current replicas.
            5: {
                'is_zero_cost': True,
                'planned_capacity': 100,
            },
            # The cleanup scalar alone is not proof without its matching JSON
            # status copy.
            6: {
                'is_zero_cost': False,
                'planned_capacity': 100,
            },
        }
        with sqlalchemy.orm.Session(broker_engine) as session:
            for replica_id, replica_state in retained_states.items():
                sky_down_status = (common_utils.ProcessStatus.SUCCEEDED.value
                                   if replica_id == 6 else
                                   common_utils.ProcessStatus.FAILED.value
                                   if replica_id == 4 else None)
                session.execute(
                    sqlalchemy.update(serve_state.replicas_table).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        replica_id).values(replica_state=replica_state,
                                           sky_down_status=sky_down_status))
            session.commit()

        candidate = self._info('svc', 7)
        candidate.planned_capacity = 2

        def _claim() -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                7,
                candidate,
                pool_key=self._test_pool('pool'),
                priority=20,
                base_limit=8,
                max_limit=16,
                max_live_paid_gpu_units=8,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'))

        # Historical rows without an exact provider pool cannot be assigned a
        # physical GPU debit, so admission fails closed while any remain live.
        assert _claim() == 'service_saturated'
        assert serve_state.get_replica_info_from_id('svc', 7) is None

        with sqlalchemy.orm.Session(broker_engine) as session:
            for replica_id, replica_state in retained_states.items():
                cleaned_state = dict(replica_state)
                cleaned_state['status_property'] = {
                    'sky_down_status':
                        common_utils.ProcessStatus.SUCCEEDED.value,
                }
                session.execute(
                    sqlalchemy.update(serve_state.replicas_table).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        replica_id).values(
                            replica_state=cleaned_state,
                            sky_down_status=(
                                common_utils.ProcessStatus.SUCCEEDED.value)))
            session.commit()

        assert _claim() == 'acquired'
        assert serve_state.get_replica_info_from_id('svc', 7) is not None

    def test_paid_gpu_cap_allows_exact_claim_replay_without_new_debit(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc',
                          'hash',
                          11,
                          spec_override=_paid_cap_service_spec(8))
        first = self._info('svc', 1)
        first.planned_capacity = 8
        pool_key = self._test_pool('eight-gpu', accelerator_count=8)

        def _claim(replica_id: int, info, limit: int) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                replica_id,
                info,
                pool_key=pool_key,
                priority=20,
                base_limit=8,
                max_limit=16,
                max_live_paid_gpu_units=limit,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'))

        assert _claim(1, first, 8) == 'acquired'
        assert _claim(1, first, 0) == 'acquired'
        assert _claim(2, self._info('svc', 2), 8) == 'service_saturated'

        with sqlalchemy.orm.Session(broker_engine) as session:
            claim_ids = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.replica_id).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc')).scalars().all()
        assert claim_ids == [1]

    def test_paid_gpu_cap_serializes_multi_gpu_admission(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc',
                          'hash',
                          11,
                          spec_override=_paid_cap_service_spec(6))
        barrier = threading.Barrier(8)
        results = []
        errors = []

        def _claim(replica_id: int) -> None:
            try:
                info = self._info('svc', replica_id)
                info.planned_capacity = 2
                barrier.wait(timeout=20)
                results.append(
                    serve_state.try_add_replica_with_paid_capacity_claim(
                        'svc',
                        'hash',
                        replica_id,
                        info,
                        pool_key=self._test_pool(f'pool-{replica_id}',
                                                 accelerator_count=2),
                        priority=20,
                        base_limit=8,
                        max_limit=16,
                        max_live_paid_gpu_units=6,
                        now=100,
                        success_ttl_seconds=60,
                        waiter_ttl_seconds=30,
                        expected_controller_owner=(11, '10.0.0.1')))
            except Exception as error:  # pylint: disable=broad-except
                errors.append(error)

        threads = [
            threading.Thread(target=_claim, args=(replica_id,))
            for replica_id in range(1, 9)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
            assert not thread.is_alive(), 'paid GPU admission thread hung'

        assert not errors, errors
        assert results.count('acquired') == 3
        assert results.count('service_saturated') == 5
        with sqlalchemy.orm.Session(broker_engine) as session:
            claim_count = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(serve_state.paid_capacity_claims_table).where(
                    serve_state.paid_capacity_claims_table.c.service_name ==
                    'svc')).scalar_one()
            replica_count = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name ==
                    'svc')).scalar_one()
        assert claim_count == 3
        assert replica_count == 3

    def test_stale_dynamic_snapshots_serialize_last_service_slot(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)

        def _claim(replica_id: int) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                replica_id,
                self._info('svc', replica_id),
                pool_key=self._test_pool(f'pool-{replica_id}'),
                priority=20,
                base_limit=4,
                max_limit=8,
                service_limit=24,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'))

        for replica_id in range(1, 24):
            assert _claim(replica_id) == 'acquired'

        barrier = threading.Barrier(2)
        results = []
        errors = []

        def _race(replica_id: int) -> None:
            try:
                barrier.wait(timeout=20)
                results.append(_claim(replica_id))
            except Exception as error:  # pylint: disable=broad-except
                errors.append(error)

        threads = [
            threading.Thread(target=_race, args=(replica_id,))
            for replica_id in (24, 25)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
            assert not thread.is_alive(), 'dynamic service admission hung'

        assert not errors, errors
        assert sorted(results) == ['acquired', 'service_saturated']
        with sqlalchemy.orm.Session(broker_engine) as session:
            claim_count = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(serve_state.paid_capacity_claims_table).where(
                    serve_state.paid_capacity_claims_table.c.service_name ==
                    'svc')).scalar_one()
        assert claim_count == 24

    def test_service_envelope_preserves_legacy_overage_and_prunes_stale(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)

        def _claim(replica_id: int, *, service_limit: int | None = None) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                replica_id,
                self._info('svc', replica_id),
                pool_key=self._test_pool(f'pool-{replica_id}'),
                priority=20,
                base_limit=4,
                max_limit=8,
                service_limit=service_limit,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'))

        for replica_id in range(1, 5):
            assert _claim(replica_id) == 'acquired'
        assert _claim(5, service_limit=3) == 'service_saturated'

        with sqlalchemy.orm.Session(broker_engine) as session:
            session.execute(
                sqlalchemy.update(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id.in_([
                        1, 2
                    ])).values(status=serve_state.ReplicaStatus.READY.value))
            session.commit()

        assert _claim(5, service_limit=3) == 'acquired'
        with sqlalchemy.orm.Session(broker_engine) as session:
            claim_ids = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.replica_id).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc').order_by(serve_state.paid_capacity_claims_table.
                                        c.replica_id)).scalars().all()
        assert claim_ids == [3, 4, 5]

    def test_service_envelope_fill_withdraws_cross_pool_waiters(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('low', 'hash-low', 11)
        self._add_service('high', 'hash-high', 22)

        def _claim(service_name: str, service_hash: str, pid: int,
                   replica_id: int, pool_key: str, priority: int) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                service_name,
                service_hash,
                replica_id,
                self._info(service_name, replica_id),
                pool_key=pool_key,
                priority=priority,
                base_limit=1,
                max_limit=4,
                service_limit=1,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'))

        pool_a = self._test_pool('pool-a')
        pool_b = self._test_pool('pool-b')
        assert _claim('low', 'hash-low', 11, 1, pool_a, 20) == 'acquired'
        assert _claim('high', 'hash-high', 22, 1, pool_a, 50) == 'saturated'
        assert _claim('high', 'hash-high', 22, 2, pool_b, 50) == 'acquired'

        with sqlalchemy.orm.Session(broker_engine) as session:
            waiters = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table.c.pool_key).where(
                        serve_state.paid_capacity_waiters_table.c.service_name
                        == 'high')).scalars().all()
        assert waiters == []

    def test_saturation_wins_before_priority_deferral(self, broker_engine,
                                                      monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('low', 'hash-low', 11)
        self._add_service('high', 'hash-high', 22)

        def _claim(service_name, service_hash, pid, replica_id, priority):
            return serve_state.try_add_replica_with_paid_capacity_claim(
                service_name,
                service_hash,
                replica_id,
                self._info(service_name, replica_id),
                pool_key=self._test_pool('pool'),
                priority=priority,
                base_limit=1,
                max_limit=4,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'))

        assert _claim('low', 'hash-low', 11, 1, 20) == 'acquired'
        assert _claim('high', 'hash-high', 22, 1, 50) == 'saturated'
        assert _claim('low', 'hash-low', 11, 2, 20) == 'saturated'

    @pytest.mark.parametrize(
        'stale_mode',
        ['terminal_replica', 'missing_replica', 'replaced_service'])
    def test_claim_admission_reconciles_stale_claims(self, broker_engine,
                                                     monkeypatch, stale_mode):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('owner', 'hash-owner', 11)
        self._add_service('peer', 'hash-peer', 22)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'owner',
            'hash-owner',
            1,
            self._info('owner', 1),
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=1,
            max_limit=4,
            now=100,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'

        with sqlalchemy.orm.Session(broker_engine) as session:
            if stale_mode == 'terminal_replica':
                session.execute(
                    sqlalchemy.update(serve_state.replicas_table).where(
                        serve_state.replicas_table.c.service_name == 'owner',
                        serve_state.replicas_table.c.replica_id == 1).values(
                            status=serve_state.ReplicaStatus.READY.value))
            elif stale_mode == 'missing_replica':
                session.execute(
                    sqlalchemy.delete(serve_state.replicas_table).where(
                        serve_state.replicas_table.c.service_name == 'owner',
                        serve_state.replicas_table.c.replica_id == 1))
            else:
                assert stale_mode == 'replaced_service'
                session.execute(
                    sqlalchemy.update(serve_state.services_table).where(
                        serve_state.services_table.c.name == 'owner').values(
                            hash='replacement-hash'))
            session.commit()

        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'peer',
            'hash-peer',
            1,
            self._info('peer', 1),
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=1,
            max_limit=4,
            now=101,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(22, '10.0.0.1')) == 'acquired'

        with sqlalchemy.orm.Session(broker_engine) as session:
            claims = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.service_name,
                    serve_state.paid_capacity_claims_table.c.service_hash,
                    serve_state.paid_capacity_claims_table.c.replica_id,
                )).all()
        assert claims == [('peer', 'hash-peer', 1)]

    def test_capacity_failure_wins_same_outcome_batch(self, broker_engine,
                                                      monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        infos = []
        for replica_id in (1, 2):
            info = self._info('svc', replica_id)
            assert serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                replica_id,
                info,
                pool_key=self._test_pool('pool'),
                priority=20,
                base_limit=2,
                max_limit=8,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
            infos.append((replica_id, info))
        with sqlalchemy.orm.Session(broker_engine) as session:
            session.execute(
                sqlalchemy.update(serve_state.paid_capacity_pools_table).where(
                    serve_state.paid_capacity_pools_table.c.pool_key ==
                    self._test_pool('pool')).values(current_limit=4,
                                                    successes_since_resize=3,
                                                    last_success_at=100,
                                                    updated_at=100))
            session.commit()
        infos[0][1].status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        infos[1][1].status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)

        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash',
            infos, {
                1: paid_capacity.LaunchOutcome.SUCCESS,
                2: paid_capacity.LaunchOutcome.CAPACITY_FAILURE,
            },
            base_limit=2,
            max_limit=8,
            now=110,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'))

        state = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=2,
            max_limit=8,
            now=110,
            success_ttl_seconds=60)[self._test_pool('pool')]
        assert state['current_limit'] == 2
        assert state['successes_since_resize'] == 0
        assert state['last_success_at'] is None
        assert state['last_failure_at'] == 110

    def test_failure_cooldown_allows_one_global_successful_probe(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        owners = {
            'failed': ('hash-failed', 11),
            'probe-a': ('hash-a', 22),
            'probe-b': ('hash-b', 33),
        }
        for name, (service_hash, pid) in owners.items():
            self._add_service(name, service_hash, pid)

        failed = self._info('failed', 1)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'failed',
            'hash-failed',
            1,
            failed,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=100,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        failed.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'failed',
            'hash-failed', [(1, failed)],
            {1: paid_capacity.LaunchOutcome.CAPACITY_FAILURE},
            base_limit=2,
            max_limit=8,
            now=110,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            expected_controller_owner=(11, '10.0.0.1'))

        def _claim(name: str, now: float) -> str:
            service_hash, pid = owners[name]
            return serve_state.try_add_replica_with_paid_capacity_claim(
                name,
                service_hash,
                1,
                self._info(name, 1),
                pool_key=self._test_pool('pool'),
                priority=20,
                base_limit=2,
                max_limit=8,
                now=now,
                success_ttl_seconds=60,
                failure_cooldown_seconds=10,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'))

        assert _claim('probe-a', 119) == 'saturated'
        closed = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=2,
            max_limit=8,
            now=119,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10)[self._test_pool('pool')]
        assert closed['admission_state'] == 'cooldown'
        assert closed['admission_limit'] == 0
        assert closed['remaining'] == 0

        barrier = threading.Barrier(2)
        results = {}
        errors = []

        def _race(name: str) -> None:
            try:
                barrier.wait(timeout=20)
                results[name] = _claim(name, 120)
            except Exception as e:  # pylint: disable=broad-except
                errors.append(e)

        threads = [
            threading.Thread(target=_race, args=(name,))
            for name in ('probe-a', 'probe-b')
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
            assert not thread.is_alive(), 'probe admission thread hung'
        assert not errors, errors
        assert list(results.values()).count('acquired') == 1
        assert set(results.values()) <= {
            'acquired', 'saturated', 'higher_priority_waiting'
        }

        probing = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=2,
            max_limit=8,
            now=120,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10)[self._test_pool('pool')]
        assert probing['current_limit'] == 1
        assert probing['admission_state'] == 'probe'
        assert probing['admission_limit'] == 1
        assert probing['active_claims'] == 1
        assert probing['remaining'] == 0

        winner = next(
            name for name, result in results.items() if result == 'acquired')
        service_hash, pid = owners[winner]
        probe = serve_state.get_replica_info_from_id(winner, 1)
        assert probe is not None
        probe.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            winner,
            service_hash, [(1, probe)],
            {1: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=2,
            max_limit=8,
            now=121,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            expected_controller_owner=(pid, '10.0.0.1'))
        reopened = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=2,
            max_limit=8,
            now=121,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10)[self._test_pool('pool')]
        assert reopened['admission_state'] == 'active'
        assert reopened['admission_limit'] == 2
        assert reopened['current_limit'] == 2
        assert reopened['successes_since_resize'] == 1
        assert reopened['last_failure_at'] is None

    def test_capacity_failed_probe_restarts_cooldown(self, broker_engine,
                                                     monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)

        first = self._info('svc', 1)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            1,
            first,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=100,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        first.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(1, first)],
            {1: paid_capacity.LaunchOutcome.CAPACITY_FAILURE},
            base_limit=2,
            max_limit=8,
            now=110,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            expected_controller_owner=(11, '10.0.0.1'))

        probe = self._info('svc', 2)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            2,
            probe,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=120,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        probe.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(2, probe)],
            {2: paid_capacity.LaunchOutcome.CAPACITY_FAILURE},
            base_limit=2,
            max_limit=8,
            now=121,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            expected_controller_owner=(11, '10.0.0.1'))

        state = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=2,
            max_limit=8,
            now=130,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10)[self._test_pool('pool')]
        assert state['last_failure_at'] == 121
        assert state['admission_state'] == 'cooldown'
        assert state['admission_limit'] == 0
        assert state['remaining'] == 0

    def test_other_failure_releases_probe_without_restarting_cooldown(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)

        first = self._info('svc', 1)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            1,
            first,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=100,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        first.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(1, first)],
            {1: paid_capacity.LaunchOutcome.CAPACITY_FAILURE},
            base_limit=2,
            max_limit=8,
            now=110,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            expected_controller_owner=(11, '10.0.0.1'))

        probe = self._info('svc', 2)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            2,
            probe,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=120,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        probe.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(2, probe)],
            {2: paid_capacity.LaunchOutcome.OTHER_FAILURE},
            base_limit=2,
            max_limit=8,
            now=121,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            expected_controller_owner=(11, '10.0.0.1'))

        state = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=2,
            max_limit=8,
            now=121,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10)[self._test_pool('pool')]
        assert state['last_failure_at'] == 110
        assert state['admission_state'] == 'probe'
        assert state['remaining'] == 1
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            3,
            self._info('svc', 3),
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=122,
            success_ttl_seconds=60,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'

    def test_legacy_limit_is_normalized_while_holding_pool_lock(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        with broker_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.paid_capacity_pools_table).values(
                    pool_key=self._test_pool('legacy'),
                    current_limit=60,
                    successes_since_resize=59,
                    last_success_at=100,
                    updated_at=100))

        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            1,
            self._info('svc', 1),
            pool_key=self._test_pool('legacy'),
            priority=20,
            base_limit=4,
            max_limit=480,
            now=101,
            success_ttl_seconds=600,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        state = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('legacy')],
            base_limit=4,
            max_limit=480,
            now=101,
            success_ttl_seconds=600)[self._test_pool('legacy')]
        assert state['current_limit'] == 4
        assert state['successes_since_resize'] == 0
        assert state['last_success_at'] is None

    def test_legacy_overage_drains_and_old_marker_overwrite_stays_sticky(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)

        infos = {}
        for replica_id in range(1, 5):
            infos[replica_id] = self._info('svc', replica_id)
            assert serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                replica_id,
                infos[replica_id],
                pool_key=self._test_pool('pool'),
                priority=20,
                base_limit=4,
                max_limit=480,
                now=100,
                success_ttl_seconds=600,
                failure_cooldown_seconds=10,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        infos[4].status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(4, infos[4])],
            {4: paid_capacity.LaunchOutcome.CAPACITY_FAILURE},
            base_limit=4,
            max_limit=480,
            now=110,
            success_ttl_seconds=600,
            failure_cooldown_seconds=10,
            expected_controller_owner=(11, '10.0.0.1'))

        overage = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=4,
            max_limit=480,
            now=120,
            success_ttl_seconds=600,
            failure_cooldown_seconds=10)[self._test_pool('pool')]
        assert overage['admission_limit'] == 1
        assert overage['active_claims'] == 3
        assert overage['legacy_overage'] == 2
        assert overage['remaining'] == 0
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            5,
            self._info('svc', 5),
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=4,
            max_limit=480,
            now=120,
            success_ttl_seconds=600,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'saturated'

        for replica_id in range(1, 4):
            infos[replica_id].status_property.sky_launch_status = (
                common_utils.ProcessStatus.FAILED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash',
            [(replica_id, infos[replica_id]) for replica_id in range(1, 4)], {
                replica_id: paid_capacity.LaunchOutcome.OTHER_FAILURE
                for replica_id in range(1, 4)
            },
            base_limit=4,
            max_limit=480,
            now=121,
            success_ttl_seconds=600,
            failure_cooldown_seconds=10,
            expected_controller_owner=(11, '10.0.0.1'))

        old_binary_probe = self._info('svc', 5)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            5,
            old_binary_probe,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=4,
            max_limit=480,
            now=122,
            success_ttl_seconds=600,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        with broker_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(serve_state.paid_capacity_pools_table).where(
                    serve_state.paid_capacity_pools_table.c.pool_key ==
                    self._test_pool('pool')).values(current_limit=60,
                                                    updated_at=122.5))
        old_binary_probe.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(5, old_binary_probe)],
            {5: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=4,
            max_limit=480,
            now=123,
            success_ttl_seconds=600,
            failure_cooldown_seconds=10,
            expected_controller_owner=(11, '10.0.0.1'))
        sticky = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=4,
            max_limit=480,
            now=123,
            success_ttl_seconds=600,
            failure_cooldown_seconds=10)[self._test_pool('pool')]
        assert sticky['current_limit'] == 60
        assert sticky['last_failure_at'] == 110
        assert sticky['active_claims'] == 0

        fresh_probe = self._info('svc', 6)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            6,
            fresh_probe,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=4,
            max_limit=480,
            now=124,
            success_ttl_seconds=600,
            failure_cooldown_seconds=10,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        fresh_probe.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(6, fresh_probe)],
            {6: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=4,
            max_limit=480,
            now=125,
            success_ttl_seconds=600,
            failure_cooldown_seconds=10,
            expected_controller_owner=(11, '10.0.0.1'))
        reopened = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=4,
            max_limit=480,
            now=125,
            success_ttl_seconds=600,
            failure_cooldown_seconds=10)[self._test_pool('pool')]
        assert reopened['current_limit'] == 4
        assert reopened['last_failure_at'] is None

    def test_post_lock_database_clock_crosses_cooldown_boundary(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        with broker_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.paid_capacity_pools_table).values(
                    pool_key=self._test_pool('pool'),
                    current_limit=4,
                    successes_since_resize=0,
                    updated_at=0))

        entered_ensure = threading.Event()
        original_ensure = serve_state._ensure_paid_capacity_pool_in_session

        def _signal_ensure(*args, **kwargs):
            entered_ensure.set()
            return original_ensure(*args, **kwargs)

        monkeypatch.setattr(serve_state,
                            '_ensure_paid_capacity_pool_in_session',
                            _signal_ensure)
        results = []
        errors = []

        def _claim() -> None:
            try:
                results.append(
                    serve_state.try_add_replica_with_paid_capacity_claim(
                        'svc',
                        'hash',
                        1,
                        self._info('svc', 1),
                        pool_key=self._test_pool('pool'),
                        priority=20,
                        base_limit=4,
                        max_limit=480,
                        now=None,
                        success_ttl_seconds=600,
                        failure_cooldown_seconds=1,
                        waiter_ttl_seconds=30,
                        expected_controller_owner=(11, '10.0.0.1')))
            except Exception as e:  # pylint: disable=broad-except
                errors.append(e)

        with sqlalchemy.orm.Session(broker_engine) as blocker:
            blocker.execute(
                sqlalchemy.select(serve_state.paid_capacity_pools_table).where(
                    serve_state.paid_capacity_pools_table.c.pool_key ==
                    self._test_pool('pool')).with_for_update())
            failure_at = float(
                blocker.execute(
                    sqlalchemy.update(serve_state.paid_capacity_pools_table).
                    where(serve_state.paid_capacity_pools_table.c.pool_key ==
                          self._test_pool('pool')).values(
                              last_failure_at=sqlalchemy.extract(
                                  'epoch', sqlalchemy.func.clock_timestamp()),
                              updated_at=sqlalchemy.extract(
                                  'epoch',
                                  sqlalchemy.func.clock_timestamp())).returning(
                                      serve_state.paid_capacity_pools_table.c.
                                      last_failure_at)).scalar_one())
            thread = threading.Thread(target=_claim)
            thread.start()
            assert entered_ensure.wait(timeout=20)
            # A transaction-stable/pre-lock clock remains inside cooldown;
            # clock_timestamp() sampled after this lock is released does not.
            time.sleep(1.25)
            blocker.commit()
        thread.join(timeout=20)
        assert not thread.is_alive(), 'clock-ordering claim thread hung'
        assert not errors, errors
        assert results == ['acquired']
        with broker_engine.connect() as connection:
            claimed_at = float(
                connection.execute(
                    sqlalchemy.select(serve_state.paid_capacity_claims_table.c.
                                      claimed_at)).scalar_one())
        assert claimed_at >= failure_at + 1

    def test_outcome_failure_uses_post_lock_database_clock(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        failed = self._info('svc', 1)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            1,
            failed,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=4,
            max_limit=480,
            now=100,
            success_ttl_seconds=600,
            failure_cooldown_seconds=1,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        failed.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)

        entered_pool_lock = threading.Event()
        original_pool_lock = serve_state._paid_capacity_pool_row_for_update

        def _signal_pool_lock(*args, **kwargs):
            entered_pool_lock.set()
            return original_pool_lock(*args, **kwargs)

        monkeypatch.setattr(serve_state, '_paid_capacity_pool_row_for_update',
                            _signal_pool_lock)
        results = []
        errors = []

        def _record_failure() -> None:
            try:
                results.append(
                    serve_state.
                    add_or_update_replicas_with_paid_capacity_outcomes(
                        'svc',
                        'hash', [(1, failed)],
                        {1: paid_capacity.LaunchOutcome.CAPACITY_FAILURE},
                        base_limit=4,
                        max_limit=480,
                        now=None,
                        success_ttl_seconds=600,
                        failure_cooldown_seconds=1,
                        expected_controller_owner=(11, '10.0.0.1')))
            except Exception as e:  # pylint: disable=broad-except
                errors.append(e)

        with sqlalchemy.orm.Session(broker_engine) as blocker:
            blocker.execute(
                sqlalchemy.select(serve_state.paid_capacity_pools_table).where(
                    serve_state.paid_capacity_pools_table.c.pool_key ==
                    self._test_pool('pool')).with_for_update())
            thread = threading.Thread(target=_record_failure)
            thread.start()
            assert entered_pool_lock.wait(timeout=20)
            time.sleep(1.25)
            release_floor = float(
                blocker.execute(
                    sqlalchemy.select(
                        sqlalchemy.extract(
                            'epoch',
                            sqlalchemy.func.clock_timestamp()))).scalar_one())
            blocker.commit()
        thread.join(timeout=20)
        assert not thread.is_alive(), 'clock-ordering outcome thread hung'
        assert not errors, errors
        assert results == [True]

        state = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=4,
            max_limit=480,
            now=None,
            success_ttl_seconds=600,
            failure_cooldown_seconds=1)[self._test_pool('pool')]
        assert state['last_failure_at'] >= release_floor
        assert state['admission_state'] == 'cooldown'

    @pytest.mark.parametrize('status', [
        serve_state.ServiceStatus.SHUTTING_DOWN,
        serve_state.ServiceStatus.FAILED_CLEANUP
    ])
    def test_teardown_status_allows_outcome_persistence(self, broker_engine,
                                                        monkeypatch, status):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        info = self._info('svc', 1)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            1,
            info,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=100,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        with sqlalchemy.orm.Session(broker_engine) as session:
            session.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == 'svc').values(
                        status=status.value))
            session.commit()

        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(1, info)], {1: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=2,
            max_limit=8,
            now=101,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'))
        state = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=2,
            max_limit=8,
            now=101,
            success_ttl_seconds=60)[self._test_pool('pool')]
        assert state['active_claims'] == 0
        assert state['successes_since_resize'] == 1

    def test_late_success_selected_before_failure_does_not_rebuild_ramp(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)

        def _claim(replica_id: int, now: float) -> replica_managers.ReplicaInfo:
            info = self._info('svc', replica_id)
            assert serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                replica_id,
                info,
                pool_key=self._test_pool('pool'),
                priority=20,
                base_limit=2,
                max_limit=8,
                now=now,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
            return info

        slow = _claim(1, 100)
        failed = _claim(2, 110)
        failed.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(2, failed)],
            {2: paid_capacity.LaunchOutcome.CAPACITY_FAILURE},
            base_limit=2,
            max_limit=8,
            now=120,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'))
        # Recovery may retry the same durable claim after the failure. That
        # retry must not rewrite its original selection timestamp and turn an
        # older launch into newer positive evidence.
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            1,
            slow,
            pool_key=self._test_pool('pool'),
            priority=20,
            base_limit=2,
            max_limit=8,
            now=125,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        with broker_engine.connect() as connection:
            claimed_at = connection.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.claimed_at).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc',
                        serve_state.paid_capacity_claims_table.c.replica_id ==
                        1)).scalar_one()
        assert claimed_at == 100
        slow.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash', [(1, slow)], {1: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=2,
            max_limit=8,
            now=130,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'))

        state = serve_state.get_paid_capacity_pool_states(
            [self._test_pool('pool')],
            base_limit=2,
            max_limit=8,
            now=130,
            success_ttl_seconds=60)[self._test_pool('pool')]
        assert state['current_limit'] == 2
        assert state['successes_since_resize'] == 0
        assert state['last_success_at'] is None
        assert state['last_failure_at'] == 120

    def test_outcome_batch_upserts_more_than_postgresql_chunk(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        infos = [(replica_id, self._info('svc', replica_id))
                 for replica_id in range(301)]

        assert serve_state.add_or_update_replicas('svc', infos)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'hash',
            infos, {},
            base_limit=60,
            max_limit=480,
            now=100,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'))
        assert len(serve_state.get_replica_infos('svc')) == 301

    def test_concurrent_services_cannot_oversubscribe_one_pool(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc-a', 'hash-a', 11)
        self._add_service('svc-b', 'hash-b', 22)
        barrier = threading.Barrier(8)
        results = []
        result_lock = threading.Lock()

        def _run(index: int) -> None:
            service_name, service_hash, pid = (('svc-a', 'hash-a',
                                                11) if index % 2 == 0 else
                                               ('svc-b', 'hash-b', 22))
            barrier.wait()
            result = serve_state.try_add_replica_with_paid_capacity_claim(
                service_name,
                service_hash,
                index + 1,
                self._info(service_name, index + 1),
                pool_key=self._test_pool('shared-pool'),
                priority=20,
                base_limit=3,
                max_limit=12,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'))
            with result_lock:
                results.append(result)

        threads = [
            threading.Thread(target=_run, args=(index,)) for index in range(8)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        assert all(not thread.is_alive() for thread in threads)
        assert results.count('acquired') == 3
        assert set(results) <= {
            'acquired', 'saturated', 'higher_priority_waiting'
        }

    @pytest.mark.parametrize('service_limit,rejection', [
        (None, 'feedback_pending'),
        (2, 'service_saturated'),
    ])
    def test_frontier_cross_pool_race_admits_only_one_final_slot(
            self, broker_engine, monkeypatch, service_limit, rejection):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        location_a, pool_a = self._paid_pool('us-east-1a', 'g6.xlarge')
        _, pool_b = self._paid_pool('us-east-1b', 'g6.2xlarge')
        _, pool_c = self._paid_pool('us-east-1c', 'g6.4xlarge')
        frontier_key = paid_capacity.frontier_key(location_a)

        def _claim(replica_id: int, pool_key: str) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                replica_id,
                self._info('svc', replica_id),
                pool_key=pool_key,
                priority=20,
                base_limit=4,
                max_limit=16,
                service_limit=service_limit,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'),
                frontier_key=frontier_key,
                frontier_limit=2)

        assert _claim(1, pool_a) == 'acquired'
        barrier = threading.Barrier(2)
        results = {}
        errors = []
        result_lock = threading.Lock()

        def _race(replica_id: int, pool_key: str) -> None:
            try:
                barrier.wait(timeout=20)
                result = _claim(replica_id, pool_key)
                with result_lock:
                    results[replica_id] = result
            except Exception as e:  # pylint: disable=broad-except
                errors.append(e)

        candidates = {2: pool_b, 3: pool_c}
        threads = [
            threading.Thread(target=_race, args=(replica_id, pool_key))
            for replica_id, pool_key in candidates.items()
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
            assert not thread.is_alive(), 'frontier admission thread hung'

        assert not errors, errors
        assert list(results.values()).count('acquired') == 1
        assert list(results.values()).count(rejection) == 1
        winner_id = next(replica_id for replica_id, result in results.items()
                         if result == 'acquired')
        winner_pool = candidates[winner_id]
        with sqlalchemy.orm.Session(broker_engine) as session:
            replicas = session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.replica_id).where(
                        serve_state.replicas_table.c.service_name ==
                        'svc')).scalars().all()
            claims = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.replica_id,
                    serve_state.paid_capacity_claims_table.c.pool_key).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc')).all()
            waiters = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table.c.pool_key).where(
                        serve_state.paid_capacity_waiters_table.c.service_name
                        == 'svc')).scalars().all()
            pools = session.execute(
                sqlalchemy.select(serve_state.paid_capacity_pools_table.c.
                                  pool_key)).scalars().all()

        assert set(replicas) == {1, winner_id}
        assert set(claims) == {(1, pool_a), (winner_id, winner_pool)}
        assert waiters == []
        if service_limit is None:
            # Frontier arbitration materializes the full sorted candidate
            # pool union, including the losing candidate's durable pool row.
            assert set(pools) == {pool_a, pool_b, pool_c}
        else:
            # The service cap can fail from the owner-locked census before
            # any downstream pool row is needed.
            assert set(pools) == {pool_a, winner_pool}

    def test_expanded_frontier_race_admits_only_one_third_pool(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        location_a, pool_a = self._paid_pool('us-east-1a', 'g6.xlarge')
        _, pool_b = self._paid_pool('us-east-1b', 'g6.2xlarge')
        _, pool_c = self._paid_pool('us-east-1c', 'g6.4xlarge')
        _, pool_d = self._paid_pool('us-east-1d', 'g6.8xlarge')
        frontier_key = paid_capacity.frontier_key(location_a)

        def _claim(replica_id: int, pool_key: str) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                replica_id,
                self._info('svc', replica_id),
                pool_key=pool_key,
                priority=20,
                base_limit=4,
                max_limit=16,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'),
                frontier_key=frontier_key,
                frontier_limit=3,
                frontier_default_limit=2,
                frontier_limits_by_key={frontier_key: 3})

        assert _claim(1, pool_a) == 'acquired'
        assert _claim(2, pool_b) == 'acquired'
        barrier = threading.Barrier(2)
        results = {}
        errors = []
        result_lock = threading.Lock()

        def _race(replica_id: int, pool_key: str) -> None:
            try:
                barrier.wait(timeout=20)
                result = _claim(replica_id, pool_key)
                with result_lock:
                    results[replica_id] = result
            except Exception as e:  # pylint: disable=broad-except
                errors.append(e)

        candidates = {3: pool_c, 4: pool_d}
        threads = [
            threading.Thread(target=_race, args=(replica_id, pool_key))
            for replica_id, pool_key in candidates.items()
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
            assert not thread.is_alive(), 'expanded frontier thread hung'

        assert not errors, errors
        assert list(results.values()).count('acquired') == 1
        assert list(results.values()).count('feedback_pending') == 1
        winner_id = next(replica_id for replica_id, result in results.items()
                         if result == 'acquired')
        winner_pool = candidates[winner_id]
        with sqlalchemy.orm.Session(broker_engine) as session:
            claims = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.replica_id,
                    serve_state.paid_capacity_claims_table.c.pool_key).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc')).all()
            pools = session.execute(
                sqlalchemy.select(serve_state.paid_capacity_pools_table.c.
                                  pool_key)).scalars().all()

        assert set(claims) == {
            (1, pool_a),
            (2, pool_b),
            (winner_id, winner_pool),
        }
        assert set(pools) == {pool_a, pool_b, pool_c, pool_d}

    def test_paid_claim_redrive_cannot_change_exact_pool(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        location, pool_a = self._paid_pool('us-east-1a', 'g6.xlarge')
        _, pool_b = self._paid_pool('us-east-1b', 'g6.2xlarge')
        frontier_key = paid_capacity.frontier_key(location)
        info = self._info('svc', 1)

        def _claim(pool_key: str,
                   now: float,
                   candidate_info: replica_managers.ReplicaInfo = info) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                1,
                candidate_info,
                pool_key=pool_key,
                priority=20,
                base_limit=4,
                max_limit=16,
                service_limit=1,
                now=now,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'),
                frontier_key=frontier_key,
                frontier_limit=1)

        assert _claim(pool_a, 100) == 'acquired'
        assert _claim(pool_a, 200) == 'acquired'
        redrive = self._info('svc', 1)
        redrive.replica_record_id = info.replica_record_id
        with pytest.raises(ValueError, match='cannot move between exact'):
            _claim(pool_b, 300, redrive)

        with sqlalchemy.orm.Session(broker_engine) as session:
            claims = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.pool_key,
                    serve_state.paid_capacity_claims_table.c.claimed_at).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc')).all()
            pools = session.execute(
                sqlalchemy.select(serve_state.paid_capacity_pools_table.c.
                                  pool_key)).scalars().all()
            waiters = session.execute(
                sqlalchemy.select(serve_state.paid_capacity_waiters_table.c.
                                  pool_key)).scalars().all()
            replica_pool = session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.paid_capacity_pool_key).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        1)).scalar_one()

        assert claims == [(pool_a, 100)]
        assert pools == [pool_a]
        assert waiters == []
        assert replica_pool == pool_a

    def test_paid_claim_redrive_preserves_claim_and_requires_same_record(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        original = self._info('svc', 1)

        def _claim(info: replica_managers.ReplicaInfo, priority: int,
                   now: float) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                1,
                info,
                pool_key=self._test_pool('pool'),
                priority=priority,
                base_limit=1,
                max_limit=4,
                now=now,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'))

        assert _claim(original, 20, 100) == 'acquired'
        original.version = 2
        assert _claim(original, 21, 200) == 'acquired'

        replacement = self._info('svc', 1)
        replacement.version = 99
        assert replacement.replica_record_id != original.replica_record_id
        assert _claim(replacement, 99, 300) == 'ownership_lost'

        persisted = serve_state.get_replica_info_from_id('svc', 1)
        assert persisted is not None
        assert persisted.replica_record_id == original.replica_record_id
        assert persisted.version == 1
        with sqlalchemy.orm.Session(broker_engine) as session:
            claim = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.pool_key,
                    serve_state.paid_capacity_claims_table.c.priority,
                    serve_state.paid_capacity_claims_table.c.claimed_at).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc',
                        serve_state.paid_capacity_claims_table.c.replica_id ==
                        1)).one()
        assert claim == (self._test_pool('pool'), 20, 100)

    def test_frontier_fill_withdraws_ineligible_priority_waiter(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('low', 'hash-low', 11)
        self._add_service('high', 'hash-high', 22)
        location_a, pool_a = self._paid_pool('us-east-1a', 'g6.xlarge')
        _, pool_b = self._paid_pool('us-east-1b', 'g6.2xlarge')
        _, pool_c = self._paid_pool('us-east-1c', 'g6.4xlarge')
        frontier_key = paid_capacity.frontier_key(location_a)

        def _claim(service_name: str, service_hash: str, pid: int,
                   replica_id: int, pool_key: str, priority: int,
                   now: float) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                service_name,
                service_hash,
                replica_id,
                self._info(service_name, replica_id),
                pool_key=pool_key,
                priority=priority,
                base_limit=1,
                max_limit=4,
                now=now,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'),
                frontier_key=frontier_key,
                frontier_limit=2)

        assert _claim('low', 'hash-low', 11, 1, pool_a, 20, 100) == 'acquired'
        assert _claim('high', 'hash-high', 22, 1, pool_a, 50,
                      101) == 'saturated'
        assert _claim('high', 'hash-high', 22, 2, pool_b, 50, 102) == 'acquired'
        assert _claim('high', 'hash-high', 22, 3, pool_c, 50, 103) == 'acquired'

        with sqlalchemy.orm.Session(broker_engine) as session:
            high_waiters = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table.c.pool_key).where(
                        serve_state.paid_capacity_waiters_table.c.service_name
                        == 'high')).scalars().all()
        assert high_waiters == []

        low_one = serve_state.get_replica_info_from_id('low', 1)
        assert low_one is not None
        low_one.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        assert serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'low',
            'hash-low', [(1, low_one)],
            {1: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=1,
            max_limit=4,
            now=104,
            success_ttl_seconds=60,
            expected_controller_owner=(11, '10.0.0.1'))

        assert _claim('low', 'hash-low', 11, 2, pool_a, 20, 105) == 'acquired'

    def test_waiter_cleanup_uses_per_card_dynamic_frontier_limits(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        l4_location, l4_a = self._paid_pool('us-east-1a', 'g6.xlarge')
        _, l4_b = self._paid_pool('us-east-1b', 'g6.2xlarge')
        _, l4_waiter = self._paid_pool('us-east-1c', 'g6.4xlarge')
        a100_location, a100_a = self._paid_pool('us-east-1a',
                                                'p4d.24xlarge',
                                                accelerator='A100')
        _, a100_b = self._paid_pool('us-east-1b',
                                    'p4de.24xlarge',
                                    accelerator='A100')
        _, a100_waiter = self._paid_pool('us-east-1c',
                                         'p4d.48xlarge',
                                         accelerator='A100')
        pool_keys = (l4_a, l4_b, l4_waiter, a100_a, a100_b, a100_waiter)
        with sqlalchemy.orm.Session(broker_engine) as session:
            session.execute(
                sqlalchemy.insert(serve_state.paid_capacity_pools_table), [{
                    'pool_key': pool_key,
                    'current_limit': 4,
                    'successes_since_resize': 0,
                    'updated_at': 100,
                } for pool_key in pool_keys])
            session.execute(
                sqlalchemy.insert(serve_state.paid_capacity_waiters_table), [{
                    'pool_key': pool_key,
                    'service_name': 'svc',
                    'service_hash': 'hash',
                    'priority': 20,
                    'first_wait_at': 100,
                    'heartbeat_at': 100,
                } for pool_key in (l4_waiter, a100_waiter)])
            serve_state._withdraw_ineligible_frontier_waiters_in_session(
                session,
                'svc',
                'hash', [(1, l4_a), (2, l4_b), (3, a100_a), (4, a100_b)],
                frontier_limit=2,
                frontier_limits_by_key={
                    paid_capacity.frontier_key(l4_location): 3,
                    paid_capacity.frontier_key(a100_location): 2,
                })
            session.commit()

        with sqlalchemy.orm.Session(broker_engine) as session:
            waiters = session.execute(
                sqlalchemy.select(serve_state.paid_capacity_waiters_table.c.
                                  pool_key)).scalars().all()
        assert waiters == [l4_waiter]

    def test_waiter_cleanup_failure_rolls_back_paid_claim_atomically(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('low', 'hash-low', 11)
        self._add_service('high', 'hash-high', 22)
        location, pool_a = self._paid_pool('us-east-1a', 'g6.xlarge')
        _, pool_b = self._paid_pool('us-east-1b', 'g6.2xlarge')
        _, pool_c = self._paid_pool('us-east-1c', 'g6.4xlarge')
        frontier_key = paid_capacity.frontier_key(location)

        def _claim(service_name: str,
                   service_hash: str,
                   pid: int,
                   replica_id: int,
                   pool_key: str,
                   priority: int,
                   now: float,
                   *,
                   frontier: bool = False) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                service_name,
                service_hash,
                replica_id,
                self._info(service_name, replica_id),
                pool_key=pool_key,
                priority=priority,
                base_limit=1,
                max_limit=4,
                now=now,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'),
                frontier_key=(frontier_key if frontier else None),
                frontier_limit=(2 if frontier else None))

        assert _claim('low', 'hash-low', 11, 1, pool_a, 20, 100) == 'acquired'
        assert _claim('high', 'hash-high', 22, 1, pool_a, 50,
                      100) == 'saturated'
        assert _claim('high',
                      'hash-high',
                      22,
                      2,
                      pool_b,
                      50,
                      101,
                      frontier=True) == 'acquired'

        original_withdraw = (
            serve_state._withdraw_ineligible_frontier_waiters_in_session)

        def _fail_cleanup(*_args, **_kwargs):
            raise RuntimeError('simulated cleanup crash')

        monkeypatch.setattr(serve_state,
                            '_withdraw_ineligible_frontier_waiters_in_session',
                            _fail_cleanup)
        with pytest.raises(RuntimeError, match='simulated cleanup crash'):
            _claim('high', 'hash-high', 22, 3, pool_c, 50, 102, frontier=True)

        with sqlalchemy.orm.Session(broker_engine) as session:
            high_claims = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.replica_id,
                    serve_state.paid_capacity_claims_table.c.pool_key).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'high')).all()
            high_waiters = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table.c.pool_key).where(
                        serve_state.paid_capacity_waiters_table.c.service_name
                        == 'high')).scalars().all()
        assert high_claims == [(2, pool_b)]
        assert high_waiters == [pool_a]

        monkeypatch.setattr(serve_state,
                            '_withdraw_ineligible_frontier_waiters_in_session',
                            original_withdraw)
        assert _claim('high',
                      'hash-high',
                      22,
                      3,
                      pool_c,
                      50,
                      102,
                      frontier=True) == 'acquired'
        with sqlalchemy.orm.Session(broker_engine) as session:
            high_claims = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.replica_id,
                    serve_state.paid_capacity_claims_table.c.pool_key).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'high')).all()
            high_waiters = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table.c.pool_key).where(
                        serve_state.paid_capacity_waiters_table.c.service_name
                        == 'high')).scalars().all()
        assert set(high_claims) == {(2, pool_b), (3, pool_c)}
        assert high_waiters == []

    def test_frontier_waiter_cleanup_avoids_cross_pool_deadlock(
            self, broker_engine, monkeypatch):
        """Canonical pool-union locking serializes crossed cleanup safely."""
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc-a', 'hash-a', 11)
        self._add_service('svc-b', 'hash-b', 22)
        location, pool_a1 = self._paid_pool('us-east-1a', 'g6.xlarge')
        _, pool_b1 = self._paid_pool('us-east-1b', 'g6.2xlarge')
        _, pool_a2 = self._paid_pool('us-east-1c', 'g6.4xlarge')
        _, pool_b2 = self._paid_pool('us-east-1d', 'g6.8xlarge')
        frontier_key = paid_capacity.frontier_key(location)

        def _claim(service_name: str, service_hash: str, pid: int,
                   replica_id: int, pool_key: str) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                service_name,
                service_hash,
                replica_id,
                self._info(service_name, replica_id),
                pool_key=pool_key,
                priority=20,
                base_limit=4,
                max_limit=16,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'),
                frontier_key=frontier_key,
                frontier_limit=2)

        assert _claim('svc-a', 'hash-a', 11, 1, pool_a1) == 'acquired'
        assert _claim('svc-b', 'hash-b', 22, 1, pool_b1) == 'acquired'
        with sqlalchemy.orm.Session(broker_engine) as session:
            serve_state._ensure_paid_capacity_pool_in_session(
                session, broker_engine, pool_a2, 4, 0)
            serve_state._ensure_paid_capacity_pool_in_session(
                session, broker_engine, pool_b2, 4, 0)
            session.execute(
                sqlalchemy.insert(
                    serve_state.paid_capacity_waiters_table).values([
                        {
                            'pool_key': pool_b2,
                            'service_name': 'svc-a',
                            'service_hash': 'hash-a',
                            'priority': 10,
                            'first_wait_at': 0,
                            'heartbeat_at': 0,
                        },
                        {
                            'pool_key': pool_a2,
                            'service_name': 'svc-b',
                            'service_hash': 'hash-b',
                            'priority': 10,
                            'first_wait_at': 0,
                            'heartbeat_at': 0,
                        },
                    ]))
            session.commit()

        start_barrier = threading.Barrier(2)
        cleanup_calls = []
        original_withdraw = (
            serve_state._withdraw_ineligible_frontier_waiters_in_session)

        def _record_withdraw(*args, **kwargs):
            cleanup_calls.append(args[1])
            return original_withdraw(*args, **kwargs)

        monkeypatch.setattr(serve_state,
                            '_withdraw_ineligible_frontier_waiters_in_session',
                            _record_withdraw)
        results = {}
        errors = []
        result_lock = threading.Lock()

        def _run(service_name: str, service_hash: str, pid: int,
                 pool_key: str) -> None:
            try:
                start_barrier.wait(timeout=20)
                result = _claim(service_name, service_hash, pid, 2, pool_key)
                with result_lock:
                    results[service_name] = result
            except Exception as e:  # pylint: disable=broad-except
                errors.append(e)

        threads = [
            threading.Thread(target=_run,
                             args=('svc-a', 'hash-a', 11, pool_a2)),
            threading.Thread(target=_run,
                             args=('svc-b', 'hash-b', 22, pool_b2)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
            assert not thread.is_alive(), 'cross-pool cleanup thread hung'

        assert not errors, errors
        assert results == {'svc-a': 'acquired', 'svc-b': 'acquired'}
        assert sorted(cleanup_calls) == ['svc-a', 'svc-b']
        with sqlalchemy.orm.Session(broker_engine) as session:
            waiters = session.execute(
                sqlalchemy.select(serve_state.paid_capacity_waiters_table.c.
                                  pool_key)).scalars().all()
        assert waiters == []

    def test_overwide_hidden_same_card_claims_block_only_new_card_pool(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        location_a, pool_a = self._paid_pool('us-east-1a',
                                             'g6.xlarge',
                                             accelerator_count=1)
        _, pool_b = self._paid_pool('us-east-1b',
                                    'g6.12xlarge',
                                    accelerator_count=4)
        _, pool_c = self._paid_pool('us-east-1c',
                                    'g6.48xlarge',
                                    accelerator_count=8)
        _, pool_d = self._paid_pool('us-east-1d',
                                    'g6e.xlarge',
                                    accelerator_count=1)
        location_a100, pool_a100 = self._paid_pool('us-east-1f',
                                                   'p4d.24xlarge',
                                                   accelerator='A100',
                                                   accelerator_count=8)
        l4_frontier = paid_capacity.frontier_key(location_a)
        a100_frontier = paid_capacity.frontier_key(location_a100)

        def _claim(
                replica_id: int,
                pool_key: str,
                *,
                frontier_key: paid_capacity.FrontierKey | None = None) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                replica_id,
                self._info('svc', replica_id),
                pool_key=pool_key,
                priority=20,
                base_limit=4,
                max_limit=16,
                now=100 + replica_id,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'),
                frontier_key=frontier_key,
                frontier_limit=(2 if frontier_key is not None else None))

        # Simulate claims admitted by a pre-frontier controller. Their pool
        # keys are absent from the new caller's active candidate set, but the
        # locked service-wide re-read must still count every L4 shape.
        assert _claim(1, pool_a) == 'acquired'
        assert _claim(2, pool_b) == 'acquired'
        assert _claim(3, pool_c) == 'acquired'

        assert _claim(4, pool_d, frontier_key=l4_frontier) == 'feedback_pending'
        assert _claim(5, pool_a, frontier_key=l4_frontier) == 'acquired'
        assert _claim(6, pool_a100, frontier_key=a100_frontier) == 'acquired'

        with sqlalchemy.orm.Session(broker_engine) as session:
            claims = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.replica_id,
                    serve_state.paid_capacity_claims_table.c.pool_key).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc')).all()
            waiters = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_waiters_table.c.pool_key).where(
                        serve_state.paid_capacity_waiters_table.c.service_name
                        == 'svc')).scalars().all()

        assert set(claims) == {
            (1, pool_a),
            (2, pool_b),
            (3, pool_c),
            (5, pool_a),
            (6, pool_a100),
        }
        assert serve_state.get_replica_info_from_id('svc', 4) is None
        assert waiters == []

    def test_unattributable_legacy_claims_fail_closed_before_waiter_cleanup(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        location_l4, pool_l4 = self._paid_pool('us-east-1a', 'g6.xlarge')
        _, pool_a100 = self._paid_pool('us-east-1b',
                                       'p4d.24xlarge',
                                       accelerator='A100',
                                       accelerator_count=8)

        def _claim(replica_id: int,
                   pool_key: str,
                   *,
                   frontier_key: paid_capacity.FrontierKey | None = None,
                   frontier_limit: int | None = None) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'hash',
                replica_id,
                self._info('svc', replica_id),
                pool_key=pool_key,
                priority=20,
                base_limit=4,
                max_limit=16,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(11, '10.0.0.1'),
                frontier_key=frontier_key,
                frontier_limit=frontier_limit)

        legacy_a = self._info('svc', 1)
        legacy_b = self._info('svc', 2)
        assert serve_state.add_or_update_replica('svc', 1, legacy_a)
        assert serve_state.add_or_update_replica('svc', 2, legacy_b)
        assert serve_state.adopt_paid_capacity_claims(
            'svc',
            'hash', [(1, 'legacy-pool-a', 20, legacy_a),
                     (2, 'legacy-pool-b', 20, legacy_b)],
            base_limit=4,
            now=100,
            expected_controller_owner=(11, '10.0.0.1'))
        with sqlalchemy.orm.Session(broker_engine) as session:
            serve_state._ensure_paid_capacity_pool_in_session(
                session, broker_engine, pool_l4, 4, 100)
            serve_state._ensure_paid_capacity_pool_in_session(
                session, broker_engine, pool_a100, 4, 100)
            session.execute(
                sqlalchemy.insert(
                    serve_state.paid_capacity_waiters_table).values([
                        {
                            'pool_key': pool_l4,
                            'service_name': 'svc',
                            'service_hash': 'hash',
                            'priority': 50,
                            'first_wait_at': 100,
                            'heartbeat_at': 100,
                        },
                        {
                            'pool_key': pool_a100,
                            'service_name': 'svc',
                            'service_hash': 'hash',
                            'priority': 50,
                            'first_wait_at': 100,
                            'heartbeat_at': 100,
                        },
                    ]))
            session.commit()

        assert _claim(3,
                      pool_l4,
                      frontier_key=paid_capacity.frontier_key(location_l4),
                      frontier_limit=2) == 'service_saturated'
        with sqlalchemy.orm.Session(broker_engine) as session:
            waiters = session.execute(
                sqlalchemy.select(serve_state.paid_capacity_waiters_table.c.
                                  pool_key)).scalars().all()
            claim_ids = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table.c.replica_id).where(
                        serve_state.paid_capacity_claims_table.c.service_name ==
                        'svc').order_by(serve_state.paid_capacity_claims_table.
                                        c.replica_id)).scalars().all()

        assert set(waiters) == {pool_l4, pool_a100}
        assert claim_ids == [1, 2]
        assert serve_state.get_replica_info_from_id('svc', 3) is None

    def test_stale_snapshot_loses_at_atomic_claim(self, broker_engine,
                                                  monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc-a', 'hash-a', 11)
        self._add_service('svc-b', 'hash-b', 22)
        pool_key = self._test_pool('shared-pool')

        snapshot = serve_state.get_paid_capacity_pool_states(
            [pool_key],
            base_limit=1,
            max_limit=4,
            now=100,
            success_ttl_seconds=60)
        assert snapshot[pool_key]['remaining'] == 1

        def _claim(service_name: str, service_hash: str, pid: int) -> str:
            return serve_state.try_add_replica_with_paid_capacity_claim(
                service_name,
                service_hash,
                1,
                self._info(service_name, 1),
                pool_key=pool_key,
                priority=20,
                base_limit=1,
                max_limit=4,
                now=101,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'))

        assert _claim('svc-a', 'hash-a', 11) == 'acquired'
        assert _claim('svc-b', 'hash-b', 22) == 'saturated'
        assert serve_state.get_replica_info_from_id('svc-b', 1) is None

    def test_existing_claim_retry_ignores_new_priority_waiter(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('low', 'hash-low', 11)
        self._add_service('high', 'hash-high', 22)
        infos = {}

        def _claim(service_name: str, service_hash: str, pid: int,
                   replica_id: int, priority: int) -> str:
            identity = (service_name, replica_id)
            if identity not in infos:
                infos[identity] = self._info(service_name, replica_id)
            return serve_state.try_add_replica_with_paid_capacity_claim(
                service_name,
                service_hash,
                replica_id,
                infos[identity],
                pool_key=self._test_pool('shared-pool'),
                priority=priority,
                base_limit=1,
                max_limit=4,
                now=100,
                success_ttl_seconds=60,
                waiter_ttl_seconds=30,
                expected_controller_owner=(pid, '10.0.0.1'))

        assert _claim('low', 'hash-low', 11, 1, 20) == 'acquired'
        assert _claim('low', 'hash-low', 11, 2, 20) == 'saturated'
        assert _claim('high', 'hash-high', 22, 1, 50) == 'saturated'
        assert _claim('low', 'hash-low', 11, 1, 20) == 'acquired'
        with sqlalchemy.orm.Session(broker_engine) as session:
            waiters = session.execute(
                sqlalchemy.select(serve_state.paid_capacity_waiters_table.c.
                                  service_name)).scalars().all()
        assert set(waiters) == {'low', 'high'}

    def test_unattributable_legacy_row_does_not_debit_unrelated_pool(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('legacy', 'hash-legacy', 11)
        self._add_service('new', 'hash-new', 22)
        legacy = self._info('legacy', 1)
        assert serve_state.add_or_update_replica('legacy', 1, legacy)

        acquired = serve_state.try_add_replica_with_paid_capacity_claim(
            'new',
            'hash-new',
            1,
            self._info('new', 1),
            pool_key=self._test_pool('shared-pool'),
            priority=20,
            base_limit=1,
            max_limit=4,
            now=100,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(22, '10.0.0.1'))
        assert acquired == 'acquired'
        assert serve_state.get_replica_info_from_id('new', 1) is not None

    def test_restart_adopts_legacy_pending_row_and_owner_fences(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        info = self._info('svc', 1)
        assert serve_state.add_or_update_replica('svc', 1, info)
        assert serve_state.adopt_paid_capacity_claims(
            'svc',
            'hash', [(1, self._test_pool('pool'), 20, info)],
            base_limit=60,
            now=100,
            expected_controller_owner=(11, '10.0.0.1'))

        with sqlalchemy.orm.Session(broker_engine) as session:
            row = session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.paid_capacity_pool_key).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id == 1)).one()
            replica_before = dict(
                session.execute(
                    sqlalchemy.select(serve_state.replicas_table).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        1)).mappings().one())
            claim_before = dict(
                session.execute(
                    sqlalchemy.select(serve_state.paid_capacity_claims_table)).
                mappings().one())
        assert row[0] == self._test_pool('pool')
        assert claim_before['priority'] == 20
        assert claim_before['claimed_at'] == 0

        # Controller restart uses the minimum priority.  Adoption reasserts
        # the replica/pool edge but must not rewrite either half of the
        # admission receipt, even if its in-memory snapshot is stale.
        stale_info = self._info('svc', 1)
        stale_info.replica_record_id = info.replica_record_id
        stale_info.version = 99
        stale_info.cluster_name = 'stale-restart-snapshot'
        stale_info.replica_port = '9999'
        assert serve_state.adopt_paid_capacity_claims(
            'svc',
            'hash', [(1, self._test_pool('pool'), 0, stale_info)],
            base_limit=60,
            now=200,
            expected_controller_owner=(11, '10.0.0.1'))
        with sqlalchemy.orm.Session(broker_engine) as session:
            replica_after = dict(
                session.execute(
                    sqlalchemy.select(serve_state.replicas_table).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        1)).mappings().one())
            claim_after = dict(
                session.execute(
                    sqlalchemy.select(serve_state.paid_capacity_claims_table)).
                mappings().one())
        assert replica_after == replica_before
        assert claim_after == claim_before

        with pytest.raises(ValueError, match='cannot move between exact'):
            serve_state.adopt_paid_capacity_claims(
                'svc',
                'hash', [(1, self._test_pool('other'), 0, info)],
                base_limit=60,
                now=300,
                expected_controller_owner=(11, '10.0.0.1'))
        with sqlalchemy.orm.Session(broker_engine) as session:
            assert dict(
                session.execute(
                    sqlalchemy.select(serve_state.paid_capacity_claims_table)).
                mappings().one()) == claim_before

        restored = serve_state.get_replica_info_from_id('svc', 1)
        assert restored is not None
        assert restored.paid_capacity_pool_key == self._test_pool('pool')
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            2,
            self._info('svc', 2),
            pool_key=self._test_pool('pool'),
            priority=50,
            base_limit=60,
            max_limit=480,
            now=101,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(999, '10.0.0.1')) == 'ownership_lost'
        assert serve_state.get_replica_info_from_id('svc', 2) is None

    def _acquire_paid_claim(self, replica_id: int, pool_key: str,
                            priority: int) -> replica_managers.ReplicaInfo:
        info = self._info('svc', replica_id)
        assert serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            'hash',
            replica_id,
            info,
            pool_key=pool_key,
            priority=priority,
            base_limit=60,
            max_limit=480,
            now=100,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=(11, '10.0.0.1')) == 'acquired'
        persisted = serve_state.get_replica_info_from_id('svc', replica_id)
        assert persisted is not None
        return persisted

    @staticmethod
    def _receipt_rows(engine, replica_id: int) -> tuple[dict, dict]:
        """Read the complete replica and claim halves of one receipt."""
        with sqlalchemy.orm.Session(engine) as session:
            replica = dict(
                session.execute(
                    sqlalchemy.select(serve_state.replicas_table).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        replica_id)).mappings().one())
            claim = dict(
                session.execute(
                    sqlalchemy.select(
                        serve_state.paid_capacity_claims_table).where(
                            serve_state.paid_capacity_claims_table.c.
                            service_name == 'svc',
                            serve_state.paid_capacity_claims_table.c.replica_id
                            == replica_id)).mappings().one())
        return replica, claim

    @staticmethod
    def _corrupt_replica_state(engine, replica_id: int, **fields) -> None:
        """Model external corruption of the versioned JSON replica state.

        Only the JSON half drifts; the projected column still matches the
        claim, so the claim stays valid and the split reaches the writers.
        """
        with engine.begin() as connection:
            state = connection.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.replica_state).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        replica_id)).scalar_one()
            connection.exec_driver_sql(
                "SET LOCAL session_replication_role = 'replica'")
            connection.execute(
                sqlalchemy.update(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id ==
                    replica_id).values(replica_state={
                        **state,
                        **fields
                    }))

    def test_restart_adoption_mixed_batch_keeps_retained_receipt(
            self, broker_engine, monkeypatch):
        """One restart batch composes both adoption dispositions.

        The retained claim stays byte-identical while the claimless legacy
        row still takes the insert branch at the supplied restart priority.
        """
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        pool_a = self._test_pool('pool-a')
        pool_b = self._test_pool('pool-b')
        retained = self._acquire_paid_claim(1, pool_a, 20)
        legacy = self._info('svc', 2)
        assert serve_state.add_or_update_replica('svc', 2, legacy)
        replica_before, claim_before = self._receipt_rows(broker_engine, 1)
        assert claim_before['priority'] == 20

        stale = self._info('svc', 1)
        stale.replica_record_id = retained.replica_record_id
        stale.version = 99
        stale.cluster_name = 'stale-restart-snapshot'
        assert serve_state.adopt_paid_capacity_claims(
            'svc',
            'hash', [(1, pool_a, 0, stale), (2, pool_b, 0, legacy)],
            base_limit=60,
            now=200,
            expected_controller_owner=(11, '10.0.0.1'))

        assert self._receipt_rows(broker_engine,
                                  1) == (replica_before, claim_before)
        replica_2, claim_2 = self._receipt_rows(broker_engine, 2)
        assert replica_2['paid_capacity_pool_key'] == pool_b
        assert (claim_2['pool_key'], claim_2['priority'],
                claim_2['claimed_at']) == (pool_b, 0, 0)
        with sqlalchemy.orm.Session(broker_engine) as session:
            pools = set(
                session.execute(
                    sqlalchemy.select(serve_state.paid_capacity_pools_table.c.
                                      pool_key)).scalars())
        assert {pool_a, pool_b} <= pools

    @pytest.mark.parametrize('split', ['pool_key', 'reserved_fill'])
    def test_restart_adoption_fails_closed_on_split_receipt(
            self, broker_engine, monkeypatch, split):
        """A retained claim whose replica half drifted is never repaired."""
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        self._add_service('svc', 'hash', 11)
        pool_a = self._test_pool('pool-a')
        retained = self._acquire_paid_claim(1, pool_a, 20)
        if split == 'pool_key':
            self._corrupt_replica_state(
                broker_engine,
                1,
                paid_capacity_pool_key=self._test_pool('pool-b'))
        else:
            self._corrupt_replica_state(broker_engine,
                                        1,
                                        reserved_fill_pool_key='reserved')
        replica_before, claim_before = self._receipt_rows(broker_engine, 1)

        stale = self._info('svc', 1)
        stale.replica_record_id = retained.replica_record_id
        with pytest.raises(ValueError,
                           match='lost its exact provider-pool identity'):
            serve_state.adopt_paid_capacity_claims(
                'svc',
                'hash', [(1, pool_a, 0, stale)],
                base_limit=60,
                now=200,
                expected_controller_owner=(11, '10.0.0.1'))
        assert self._receipt_rows(broker_engine,
                                  1) == (replica_before, claim_before)


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
                                       spec=_service_spec(),
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
                                       spec=_service_spec(pool=True),
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


class TestServiceWorkspaceBackfillPG:
    """Legacy workspace adoption is a production-dialect fenced write."""

    def test_backfill_is_null_only_and_incarnation_fenced(
            self, broker_engine, monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        assert serve_state.add_service(name='svc-legacy',
                                       controller_job_id=1,
                                       policy='policy',
                                       requested_resources_str='1x[CPU:1+]',
                                       load_balancing_policy='round_robin',
                                       status=serve_state.ServiceStatus.READY,
                                       tls_encrypted=False,
                                       pool=False,
                                       controller_pid=11,
                                       entrypoint='entry',
                                       spec=_service_spec(),
                                       yaml_content='service: {}',
                                       workspace=None,
                                       service_hash='incarnation-a')

        assert not serve_state.set_service_workspace_if_owner(
            'svc-legacy', 'research', 'incarnation-b')
        assert serve_state.get_service_from_name(
            'svc-legacy')['workspace'] is None

        assert serve_state.set_service_workspace_if_owner(
            'svc-legacy', 'research', 'incarnation-a')
        assert serve_state.get_service_from_name(
            'svc-legacy')['workspace'] == 'research'

        assert not serve_state.set_service_workspace_if_owner(
            'svc-legacy', 'other', 'incarnation-a')
        assert serve_state.get_service_from_name(
            'svc-legacy')['workspace'] == 'research'


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
                    'serve_request_activity_daily',
                    'serve_response_time_history',
                    'serve_prediction_time_history',
                    'serve_autoscaler_history',
                    'serve_placement_events',
                    'paid_capacity_pools',
                    'paid_capacity_claims',
                    'paid_capacity_waiters',
                }.issubset(tables), tables
                autoscaler_columns = {
                    column['name'] for column in inspector.get_columns(
                        'serve_autoscaler_history')
                }
                assert {
                    'accelerator_breakdown',
                    'accelerator_breakdown_observed_at',
                }.issubset(autoscaler_columns)
                service_columns = {
                    column['name']
                    for column in inspector.get_columns('services')
                }
                assert {
                    'lifecycle_epoch',
                    'resource_scope',
                    'logical_replica_semantics',
                    'workspace',
                    'lb_ha_enabled',
                    'lb_active_slot',
                    'lb_cutover_generation',
                    'lb_pending_slot',
                    'lb_cutover_phase',
                    'lb_drain_started_at',
                    'lb_demand_handoff_generation',
                    'lb_demand_handoff_snapshot',
                    'lb_demand_handoff_complete_at',
                    'lb_last_demand_snapshot',
                    'spot_placement_state',
                    'cost_rebalance_state',
                }.issubset(service_columns)
                version_columns = {
                    column['name']
                    for column in inspector.get_columns('version_specs')
                }
                assert {
                    'created_at',
                    'created_by',
                    'submitted_yaml_content',
                    'quarantined_at',
                    'quarantine_reason',
                } <= version_columns
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
                assert 'feed_by_accelerator' in columns, columns
                protocol_columns = {
                    column['name'] for column in inspector.get_columns(
                        'reserved_fill_protocol_state')
                }
                assert {
                    'deployment_uid',
                    'pod_inventory_count',
                    'pod_inventory_sha256',
                }.issubset(protocol_columns)
                request_columns = {
                    column['name']: column for column in inspector.get_columns(
                        'serve_request_activity_history')
                }
                assert 'rejected_count' in request_columns, request_columns
                assert request_columns['rejected_count']['default'] is not None
                assert request_columns['rejection_count_available'][
                    'default'] is not None
                assert {
                    'classified_request_count',
                    'counted_rejected_count',
                }.issubset(request_columns)
                daily_request_columns = {
                    column['name']: column for column in inspector.get_columns(
                        'serve_request_activity_daily')
                }
                assert {
                    'classified_request_count',
                    'counted_rejected_count',
                    'classified_first_bucket_start',
                    'classified_last_bucket_start',
                    'classification_incomplete',
                }.issubset(daily_request_columns)
                assert daily_request_columns['classification_incomplete'][
                    'default'] is not None
                response_columns = {
                    column['name'] for column in inspector.get_columns(
                        'serve_response_time_history')
                }
                assert {
                    'response_count',
                    'status_1xx_counts',
                    'status_2xx_counts',
                    'status_3xx_counts',
                    'status_4xx_counts',
                    'status_5xx_counts',
                }.issubset(response_columns)
                replica_indexes = {
                    index['name'] for index in inspector.get_indexes('replicas')
                }
                assert 'replicas_service_version_idx' in replica_indexes
                prediction_columns = {
                    column['name'] for column in inspector.get_columns(
                        'serve_prediction_time_history')
                }
                assert {
                    'prediction_count',
                    'succeeded_counts',
                    'failed_counts',
                }.issubset(prediction_columns)
                status_columns = {
                    column['name'] for column in inspector.get_columns(
                        'serve_replica_status_history')
                }
                assert {
                    'ready_reserved_count',
                    'logical_ready_count',
                    'logical_ready_reserved_count',
                    'logical_provisioning_count',
                    'logical_not_ready_count',
                    'logical_errored_count',
                    'logical_preempted_count',
                    'logical_stopping_count',
                    'logical_total_count',
                }.issubset(status_columns)
                with engine.connect() as connection:
                    revision = connection.execute(
                        sqlalchemy.text(
                            'SELECT version_num FROM '
                            'alembic_version_serve_state_db')).scalar_one()
                assert revision == migration_utils.SERVE_VERSION
        finally:
            engine.dispose()

    def test_revision_029_restart_state_survives_rollback_and_reupgrade(
            self, pg_server):
        """The additive evidence remains JSONB across image rollback."""
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '031')
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.text(
                        "INSERT INTO services "
                        "(name, status, policy, requested_resources_str, "
                        "load_balancing_policy, tls_encrypted, pool, hash, "
                        "spot_placement_state, cost_rebalance_state) VALUES "
                        "('restart-safe', 'READY', 'test', 'test', 'round_robin', "
                        "0, 0, 'incarnation', "
                        "CAST(:spot_state AS JSONB), "
                        "CAST(:cost_state AS JSONB))"), {
                            'spot_state': '{"version": 1, "benches": []}',
                            'cost_state': '{"version": 1, "candidates": []}',
                        })

            config = migration_utils.get_alembic_config(
                engine, migration_utils.SERVE_DB_NAME)
            alembic_command.downgrade(config, '028')
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '031')

            inspector = sqlalchemy.inspect(engine)
            service_columns = {
                column['name']: column
                for column in inspector.get_columns('services')
            }
            assert isinstance(service_columns['spot_placement_state']['type'],
                              postgresql.JSONB)
            assert isinstance(service_columns['cost_rebalance_state']['type'],
                              postgresql.JSONB)
            with engine.connect() as connection:
                row = connection.execute(
                    sqlalchemy.text(
                        'SELECT spot_placement_state, cost_rebalance_state '
                        "FROM services WHERE name = 'restart-safe'")).one()
            assert row[0] == {'version': 1, 'benches': []}
            assert row[1] == {'version': 1, 'candidates': []}
        finally:
            engine.dispose()

    def test_revision_031_daily_history_survives_rollback_and_reupgrade(
            self, pg_server):
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '031')
            daily = serve_history.serve_request_activity_daily_table
            day = datetime.datetime(2026, 7, 27, tzinfo=datetime.timezone.utc)
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.insert(daily).values(day_start=day,
                                                    service_name='svc',
                                                    service_hash='hash-a',
                                                    first_bucket_start=day,
                                                    last_bucket_start=day,
                                                    request_count=7,
                                                    observed_at=day))

            config = migration_utils.get_alembic_config(
                engine, migration_utils.SERVE_DB_NAME)
            alembic_command.downgrade(config, '030')
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 migration_utils.SERVE_VERSION)

            with engine.connect() as connection:
                assert connection.execute(
                    sqlalchemy.select(daily.c.request_count)).scalar_one() == 7
                revision = connection.execute(
                    sqlalchemy.text(
                        'SELECT version_num FROM '
                        'alembic_version_serve_state_db')).scalar_one()
            assert revision == migration_utils.SERVE_VERSION
        finally:
            engine.dispose()

    def test_revision_032_latches_preexisting_attempt_history(self, pg_server):
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '031')
            day = datetime.datetime(2026, 7, 31, tzinfo=datetime.timezone.utc)
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.text(
                        'INSERT INTO serve_request_activity_daily '
                        '(day_start, service_name, service_hash, '
                        'first_bucket_start, last_bucket_start, request_count, '
                        'observed_at) VALUES '
                        '(:day, :name, :hash, :day, :day, 7, :day)'), {
                            'day': day,
                            'name': 'legacy',
                            'hash': 'legacy-hash',
                        })

            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '032')
            with engine.connect() as connection:
                row = connection.execute(
                    sqlalchemy.text(
                        'SELECT classified_request_count, '
                        'counted_rejected_count, classification_incomplete '
                        'FROM serve_request_activity_daily')).one()
                constraints = {
                    constraint['name']
                    for constraint in sqlalchemy.inspect(engine).
                    get_check_constraints('serve_request_activity_daily')
                }
            assert row == (None, None, True)
            assert 'serve_request_activity_daily_classified_pair' in constraints
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                with engine.begin() as connection:
                    connection.execute(
                        sqlalchemy.text(
                            'INSERT INTO serve_request_activity_daily '
                            '(day_start, service_name, service_hash, '
                            'first_bucket_start, last_bucket_start, '
                            'request_count, classified_request_count, '
                            'counted_rejected_count, '
                            'classified_first_bucket_start, '
                            'classified_last_bucket_start, observed_at) VALUES '
                            '(:day, :name, :hash, :day, :day, 1, 1, NULL, '
                            ':day, :day, :day)'), {
                                'day': day + datetime.timedelta(days=1),
                                'name': 'invalid',
                                'hash': 'invalid-hash',
                            })

            config = migration_utils.get_alembic_config(
                engine, migration_utils.SERVE_DB_NAME)
            alembic_command.downgrade(config, '031')
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '032')
            with engine.connect() as connection:
                assert connection.execute(
                    sqlalchemy.text(
                        'SELECT classification_incomplete FROM '
                        'serve_request_activity_daily')).scalar_one() is True
        finally:
            engine.dispose()

    def test_revision_027_downgrades_cleanly_to_026(self, pg_server):
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '031')
            config = migration_utils.get_alembic_config(
                engine, migration_utils.SERVE_DB_NAME)
            alembic_command.downgrade(config, '026')

            inspector = sqlalchemy.inspect(engine)
            assert not {
                'paid_capacity_pools',
                'paid_capacity_claims',
                'paid_capacity_waiters',
            } & set(inspector.get_table_names())
            assert 'paid_capacity_pool_key' not in {
                column['name'] for column in inspector.get_columns('replicas')
            }
            with engine.connect() as connection:
                revision = connection.execute(
                    sqlalchemy.text(
                        'SELECT version_num FROM '
                        'alembic_version_serve_state_db')).scalar_one()
            assert revision == '026'

            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 migration_utils.SERVE_VERSION)
            inspector = sqlalchemy.inspect(engine)
            assert {
                'paid_capacity_pools',
                'paid_capacity_claims',
                'paid_capacity_waiters',
            } <= set(inspector.get_table_names())
        finally:
            engine.dispose()

    @pytest.mark.parametrize('layout', [
        'upstream_022',
        'upstream_023',
        'upstream_024',
        'managed_preview_022',
        'managed_preview_023',
        'managed_preview_024',
    ])
    def test_revision_025_converges_every_colliding_serve_layout(
            self, pg_server, layout):
        """Every deployed 022/023/024 lineage converges through revision 026."""
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '021')
            with engine.begin() as connection:
                # Revision 001 uses current model metadata. Remove fields and
                # indexes that did not exist in the historical binaries before
                # constructing each exact ambiguous stamp.
                connection.execute(
                    sqlalchemy.text(
                        'ALTER TABLE services DROP COLUMN IF EXISTS workspace'))
                connection.execute(
                    sqlalchemy.text('ALTER TABLE version_specs DROP COLUMN '
                                    'IF EXISTS quarantined_at'))
                connection.execute(
                    sqlalchemy.text('ALTER TABLE version_specs DROP COLUMN '
                                    'IF EXISTS quarantine_reason'))
                connection.execute(
                    sqlalchemy.text(
                        'DROP INDEX IF EXISTS replicas_service_version_idx'))
                connection.execute(
                    sqlalchemy.text(
                        'DROP TABLE IF EXISTS serve_response_time_history'))
                connection.execute(
                    sqlalchemy.text(
                        'DROP TABLE IF EXISTS serve_prediction_time_history'))

            managed_preview = layout.startswith('managed_preview')
            if managed_preview:
                with engine.begin() as connection:
                    connection.execute(
                        sqlalchemy.text(
                            'ALTER TABLE services ADD COLUMN workspace TEXT'))
            if layout.startswith(
                    'upstream_') or layout == 'managed_preview_024':
                serve_history.serve_response_time_history_table.create(
                    engine, checkfirst=True)
            if layout in ('upstream_023', 'upstream_024'):
                serve_history.serve_prediction_time_history_table.create(
                    engine, checkfirst=True)
            if layout == 'upstream_024':
                with engine.begin() as connection:
                    connection.execute(
                        sqlalchemy.text('ALTER TABLE version_specs ADD COLUMN '
                                        'quarantined_at DOUBLE PRECISION'))
                    connection.execute(
                        sqlalchemy.text('ALTER TABLE version_specs ADD COLUMN '
                                        'quarantine_reason TEXT'))
            if layout in ('managed_preview_023', 'managed_preview_024'):
                with engine.begin() as connection:
                    connection.execute(
                        sqlalchemy.text(
                            'CREATE INDEX replicas_service_version_idx '
                            'ON replicas (service_name, version)'))

            revision = layout.rsplit('_', maxsplit=1)[-1]
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.text('UPDATE '
                                    'alembic_version_serve_state_db '
                                    'SET version_num = :revision'),
                    {'revision': revision})

            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 migration_utils.SERVE_VERSION)

            inspector = sqlalchemy.inspect(engine)
            assert 'workspace' in {
                column['name'] for column in inspector.get_columns('services')
            }
            assert {
                'quarantined_at',
                'quarantine_reason',
            } <= {
                column['name']
                for column in inspector.get_columns('version_specs')
            }
            assert {
                'serve_response_time_history',
                'serve_prediction_time_history',
            } <= set(inspector.get_table_names())
            assert 'replicas_service_version_idx' in {
                index['name'] for index in inspector.get_indexes('replicas')
            }
            with engine.connect() as connection:
                # Selecting the current model proves the formerly skipped
                # workspace column no longer causes runtime reads to fail.
                connection.execute(
                    sqlalchemy.select(serve_state.services_table).limit(1))
                final_revision = connection.execute(
                    sqlalchemy.text(
                        'SELECT version_num FROM '
                        'alembic_version_serve_state_db')).scalar_one()
            assert final_revision == migration_utils.SERVE_VERSION
        finally:
            engine.dispose()

    def test_revision_026_rebuilds_invalid_concurrent_index_residue(
            self, pg_server):
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '025')
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.text(
                        'DROP INDEX IF EXISTS replicas_service_version_idx'))
                connection.execute(
                    sqlalchemy.text('CREATE INDEX replicas_service_version_idx '
                                    'ON replicas (service_name, version)'))
                connection.execute(
                    sqlalchemy.text(
                        'UPDATE pg_index SET indisvalid = FALSE '
                        "WHERE indexrelid = 'replicas_service_version_idx'"
                        '::regclass'))

            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '026')

            with engine.connect() as connection:
                is_valid = connection.execute(
                    sqlalchemy.text(
                        'SELECT indisvalid FROM pg_index '
                        "WHERE indexrelid = 'replicas_service_version_idx'"
                        '::regclass')).scalar_one()
                revision = connection.execute(
                    sqlalchemy.text(
                        'SELECT version_num FROM '
                        'alembic_version_serve_state_db')).scalar_one()
            assert is_valid is True
            assert revision == '026'
        finally:
            engine.dispose()

    @pytest.mark.parametrize(
        ('index_name', 'index_ddl'), [
            ('replicas_service_version_idx',
             'CREATE INDEX replicas_service_version_idx '
             'ON replicas (replica_id)'),
            ('replicas_service_version_idx',
             'CREATE INDEX replicas_service_version_idx '
             "ON replicas (service_name, version) WHERE status = 'READY'"),
            ('replicas_service_version_idx',
             'CREATE INDEX replicas_service_version_idx '
             'ON replicas (service_name, (version + 0))'),
            ('replicas_service_version_idx',
             'CREATE INDEX replicas_service_version_idx '
             'ON replicas (service_name, version) INCLUDE (status)'),
            ('replicas_service_version_idx',
             'CREATE INDEX replicas_service_version_idx '
             'ON replicas (service_name DESC, version)'),
            ('replicas_service_version_idx',
             'CREATE UNIQUE INDEX replicas_service_version_idx '
             'ON replicas (service_name, version)'),
            ('replicas_service_version_idx',
             'CREATE INDEX replicas_service_version_idx '
             'ON replicas USING hash (service_name)'),
            ('replicas_service_status_idx',
             'CREATE INDEX replicas_service_status_idx '
             'ON replicas (status, service_name)'),
        ],
        ids=('wrong-columns', 'partial', 'expression', 'included-column',
             'descending', 'unique', 'wrong-method', 'status-order'))
    def test_revision_026_rejects_malformed_same_name_indexes(
            self, pg_server, index_name, index_ddl):
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '025')
            with engine.begin() as connection:
                preparer = connection.dialect.identifier_preparer
                connection.exec_driver_sql(
                    f'DROP INDEX IF EXISTS {preparer.quote(index_name)}')
                connection.exec_driver_sql(index_ddl)

            with pytest.raises(RuntimeError, match='unexpected shape'):
                migration_utils.safe_alembic_upgrade(
                    engine, migration_utils.SERVE_DB_NAME, '026')

            with engine.connect() as connection:
                revision = connection.execute(
                    sqlalchemy.text(
                        'SELECT version_num FROM '
                        'alembic_version_serve_state_db')).scalar_one()
            assert revision == '025'
        finally:
            engine.dispose()

    @pytest.mark.parametrize('preview_workspace_016', [False, True])
    def test_revision_022_reconciles_conflicting_revision_016_layouts(
            self, pg_server, preview_workspace_016):
        """Both pre-merge revision 016 schemas converge on PostgreSQL."""
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '015')
            with engine.begin() as connection:
                collision_columns = ('workspace', 'lb_ha_enabled',
                                     'lb_active_slot', 'lb_cutover_generation',
                                     'lb_pending_slot', 'lb_cutover_phase',
                                     'lb_drain_started_at',
                                     'lb_demand_handoff_generation',
                                     'lb_demand_handoff_snapshot',
                                     'lb_demand_handoff_complete_at',
                                     'lb_last_demand_snapshot')
                for column in collision_columns:
                    connection.exec_driver_sql(
                        f'ALTER TABLE services DROP COLUMN IF EXISTS {column}')
                if preview_workspace_016:
                    connection.exec_driver_sql(
                        'ALTER TABLE services ADD COLUMN workspace TEXT')
                else:
                    for definition in (
                            'lb_ha_enabled INTEGER NOT NULL DEFAULT 0',
                            'lb_active_slot TEXT',
                            'lb_cutover_generation INTEGER NOT NULL DEFAULT 0',
                            'lb_pending_slot TEXT',
                            "lb_cutover_phase TEXT NOT NULL DEFAULT 'STABLE'",
                            'lb_drain_started_at DOUBLE PRECISION',
                            'lb_demand_handoff_generation INTEGER',
                            'lb_demand_handoff_snapshot TEXT',
                            'lb_demand_handoff_complete_at DOUBLE PRECISION',
                            'lb_last_demand_snapshot TEXT'):
                        connection.exec_driver_sql(
                            f'ALTER TABLE services ADD COLUMN {definition}')
                connection.execute(serve_state.services_table.insert().values(
                    name='legacy-svc'))
                connection.execute(
                    serve_state.version_specs_table.insert().values(
                        service_name='legacy-svc',
                        version=1,
                        created_at=1.0,
                        created_by='legacy-writer'))
                connection.execute(
                    sqlalchemy.text('UPDATE alembic_version_serve_state_db '
                                    "SET version_num = '016'"))

            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 migration_utils.SERVE_VERSION)

            inspector = sqlalchemy.inspect(engine)
            service_columns = {
                column['name']: column
                for column in inspector.get_columns('services')
            }
            assert service_columns['workspace']['nullable']
            assert {
                'lb_ha_enabled',
                'lb_active_slot',
                'lb_cutover_generation',
                'lb_pending_slot',
                'lb_cutover_phase',
                'lb_drain_started_at',
                'lb_demand_handoff_generation',
                'lb_demand_handoff_snapshot',
                'lb_demand_handoff_complete_at',
                'lb_last_demand_snapshot',
            } <= set(service_columns)
            version_columns = {
                column['name']
                for column in inspector.get_columns('version_specs')
            }
            assert 'yaml_content' in version_columns
            assert 'submitted_yaml_content' in version_columns
            replica_columns = {
                column['name'] for column in inspector.get_columns('replicas')
            }
            assert {
                'replica_info',
                'replica_state_version',
                'status',
                'sky_down_status',
                'version',
                'cluster_name',
                'created_at',
                'is_spot',
                'paid_capacity_pool_key',
                'replica_state',
            } <= replica_columns
            assert {
                'paid_capacity_pools',
                'paid_capacity_claims',
                'paid_capacity_waiters',
            } <= set(inspector.get_table_names())
            with engine.connect() as connection:
                workspace = connection.execute(
                    sqlalchemy.text(
                        'SELECT workspace FROM services WHERE name = :name'), {
                            'name': 'legacy-svc'
                        }).scalar_one()
                version_row = connection.execute(
                    sqlalchemy.text(
                        'SELECT created_at, created_by, yaml_content FROM '
                        'version_specs WHERE service_name = :name AND '
                        'version = 1'), {
                            'name': 'legacy-svc'
                        }).one()
                connection.execute(
                    sqlalchemy.select(serve_state.replicas_table).limit(1))
            assert workspace is None
            assert tuple(version_row) == (1.0, 'legacy-writer', None)
        finally:
            engine.dispose()

    def test_revision_022_repairs_preview_revision_018_collision(
            self, pg_server):
        """A preview DB stamped 018 gains the current 018 placement table."""
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '017')
            with engine.begin() as connection:
                # The preview's revision 018 added ``workspace``.  Revision
                # 001 creates tables from the current model metadata, so a
                # fresh test database already has that column by revision 017.
                # Stamping it 018 without running the current revision 018
                # faithfully models the historical collision: workspace is
                # present while the placement-events table is absent.
                connection.execute(
                    sqlalchemy.text('UPDATE alembic_version_serve_state_db '
                                    "SET version_num = '018'"))

            assert 'serve_placement_events' not in set(
                sqlalchemy.inspect(engine).get_table_names())
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 migration_utils.SERVE_VERSION)

            inspector = sqlalchemy.inspect(engine)
            assert 'serve_placement_events' in set(inspector.get_table_names())
            assert 'workspace' in {
                column['name'] for column in inspector.get_columns('services')
            }
            with engine.connect() as connection:
                version = connection.execute(
                    sqlalchemy.text(
                        'SELECT version_num FROM '
                        'alembic_version_serve_state_db')).scalar_one()
            assert version == migration_utils.SERVE_VERSION
        finally:
            engine.dispose()

    def test_revision_022_repairs_preview_revision_021_collision(
            self, pg_server):
        """A preview DB stamped 021 gains canonical revision 021 columns."""
        url = _create_database(pg_server, f'migration_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '020')
            with engine.begin() as connection:
                # The former feature revision 021 persisted workspace but did
                # not contain the exact-accelerator columns now owned by the
                # canonical revision 021. Remove metadata-created copies and
                # stamp 021 to reproduce that deployed preview state.
                connection.execute(
                    sqlalchemy.text(
                        'ALTER TABLE serve_autoscaler_history DROP COLUMN IF '
                        'EXISTS accelerator_breakdown_observed_at'))
                connection.execute(
                    sqlalchemy.text(
                        'ALTER TABLE serve_autoscaler_history DROP COLUMN IF '
                        'EXISTS accelerator_breakdown'))
                connection.execute(
                    sqlalchemy.text('UPDATE alembic_version_serve_state_db '
                                    "SET version_num = '021'"))

            before_columns = {
                column['name'] for column in sqlalchemy.inspect(
                    engine).get_columns('serve_autoscaler_history')
            }
            assert 'accelerator_breakdown' not in before_columns
            assert 'accelerator_breakdown_observed_at' not in before_columns

            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 migration_utils.SERVE_VERSION)

            after_columns = {
                column['name'] for column in sqlalchemy.inspect(
                    engine).get_columns('serve_autoscaler_history')
            }
            assert {
                'accelerator_breakdown',
                'accelerator_breakdown_observed_at',
            } <= after_columns
            with engine.connect() as connection:
                version = connection.execute(
                    sqlalchemy.text(
                        'SELECT version_num FROM '
                        'alembic_version_serve_state_db')).scalar_one()
            assert version == migration_utils.SERVE_VERSION
        finally:
            engine.dispose()


class TestLbCutoverAuthorityPG:
    """The production-dialect CAS chain fences every phase transition."""

    def test_cutover_cas_and_crash_recovery_states(self, broker_engine,
                                                   monkeypatch):
        monkeypatch.setattr(serve_state._db_manager, '_engine', broker_engine)
        serve_state.Base.metadata.create_all(broker_engine)
        with sqlalchemy.orm.Session(broker_engine) as session:
            session.execute(serve_state.services_table.insert().values(
                name='ha-service',
                controller_job_id=1,
                status=serve_state.ServiceStatus.READY.value,
                controller_pid=77,
                controller_ip='10.0.0.7',
                hash='incarnation',
                lifecycle_epoch=11,
                lb_ha_enabled=1,
                lb_active_slot=lb_ha.LbSlot.A.value,
                lb_cutover_generation=1,
                lb_pending_slot=None,
                lb_cutover_phase=lb_ha.LbCutoverPhase.STABLE.value))
            session.commit()

        owner = (77, '10.0.0.7')
        assert serve_state.begin_lb_cutover('ha-service', 'incarnation',
                                            (78, '10.0.0.8'), 11,
                                            lb_ha.LbSlot.A, 1,
                                            lb_ha.LbSlot.B) is None
        assert serve_state.begin_lb_cutover('ha-service', 'incarnation', owner,
                                            12, lb_ha.LbSlot.A, 1,
                                            lb_ha.LbSlot.B) is None
        demand_snapshot = lb_ha.DemandSnapshot(
            (10, 20),
            4,
            2,
            in_flight={'http://replica': 1},
            unknown_in_flight_urls=('http://unknown',),
            compatibility_profiles=(lb_ha.CompatibilityDemand(
                50, ('A100',), 2, 10.0),),
            queued_compatibility_profiles=(lb_ha.CompatibilityDemand(
                50, ('A100',), 3),))
        assert serve_state.record_lb_active_demand_snapshot(
            'ha-service', 'incarnation', owner, 11, lb_ha.LbSlot.A, 1,
            demand_snapshot)
        assert serve_state.get_lb_last_demand_snapshot(
            'ha-service') == demand_snapshot
        assert not serve_state.record_lb_active_demand_snapshot(
            'ha-service', 'incarnation', owner, 11, lb_ha.LbSlot.B, 1,
            demand_snapshot)
        preparing = serve_state.begin_lb_cutover('ha-service', 'incarnation',
                                                 owner, 11, lb_ha.LbSlot.A, 1,
                                                 lb_ha.LbSlot.B)
        assert preparing == lb_ha.LbCutoverState(
            enabled=True,
            active_slot=lb_ha.LbSlot.A,
            generation=2,
            pending_slot=lb_ha.LbSlot.B,
            phase=lb_ha.LbCutoverPhase.PREPARING,
            lifecycle_epoch=11)
        assert serve_state.get_lb_demand_handoff('ha-service') == (
            2, demand_snapshot, None)
        assert serve_state.begin_lb_cutover('ha-service', 'incarnation', owner,
                                            11, lb_ha.LbSlot.A, 1,
                                            lb_ha.LbSlot.B) is None
        assert not serve_state.commit_lb_cutover(
            'ha-service', 'stale-incarnation', owner, 11, lb_ha.LbSlot.A,
            lb_ha.LbSlot.B, 2)
        assert serve_state.commit_lb_cutover('ha-service', 'incarnation', owner,
                                             11, lb_ha.LbSlot.A, lb_ha.LbSlot.B,
                                             2)
        completed_at = serve_state.mark_lb_demand_handoff_complete(
            'ha-service', 'incarnation', owner, 11, 2)
        assert completed_at is not None
        assert serve_state.mark_lb_demand_handoff_complete(
            'ha-service', 'incarnation', owner, 11, 2) == completed_at
        generation, restored, restored_at = serve_state.get_lb_demand_handoff(
            'ha-service')
        handoff = lb_ha.DemandHandoff(30)
        handoff.restore(generation, restored, restored_at)
        assert handoff.generation == 2
        assert handoff.snapshot == demand_snapshot

        draining = serve_state.get_lb_cutover_state('ha-service')
        assert draining is not None
        assert draining.active_slot is lb_ha.LbSlot.B
        assert draining.pending_slot is lb_ha.LbSlot.A
        assert draining.phase is lb_ha.LbCutoverPhase.DRAINING
        assert serve_state.finish_lb_cutover_drain('ha-service', 'incarnation',
                                                   owner, 11, lb_ha.LbSlot.B,
                                                   lb_ha.LbSlot.A, 2)

        preparing = serve_state.begin_lb_cutover('ha-service', 'incarnation',
                                                 owner, 11, lb_ha.LbSlot.B, 2,
                                                 lb_ha.LbSlot.A)
        assert preparing is not None
        assert serve_state.abort_lb_cutover_preparation('ha-service',
                                                        'incarnation', owner,
                                                        11, lb_ha.LbSlot.B,
                                                        lb_ha.LbSlot.A, 3)
        stable = serve_state.get_lb_cutover_state('ha-service')
        assert stable is not None
        assert stable.active_slot is lb_ha.LbSlot.B
        assert stable.generation == 3
        assert stable.pending_slot is None
        assert stable.phase is lb_ha.LbCutoverPhase.STABLE
        assert serve_state.get_lb_demand_handoff('ha-service') == (None, None,
                                                                   None)

        with serve_state.lb_cutover_kubernetes_guard(
                'ha-service', 'incarnation', owner, 11, lb_ha.LbSlot.B, 3,
                lb_ha.LbCutoverPhase.STABLE, None) as guarded:
            assert guarded
        with serve_state.lb_cutover_kubernetes_guard(
                'ha-service', 'incarnation', owner, 12, lb_ha.LbSlot.B, 3,
                lb_ha.LbCutoverPhase.STABLE, None) as guarded:
            assert not guarded

        assert serve_state.begin_lb_ha_rollback('ha-service', 'incarnation',
                                                owner, 11, lb_ha.LbSlot.B, 3)
        assert serve_state.finish_lb_ha_rollback('ha-service', 'incarnation',
                                                 owner, 11, lb_ha.LbSlot.B, 3)
        rolled_back = serve_state.get_lb_cutover_state('ha-service')
        assert rolled_back is not None
        assert not rolled_back.enabled
        assert rolled_back.phase is lb_ha.LbCutoverPhase.STABLE
        assert serve_state.get_lb_last_demand_snapshot('ha-service') is None

        assert serve_state.begin_lb_ha_migration('ha-service', 'incarnation',
                                                 owner, 11)
        migrating = serve_state.get_lb_cutover_state('ha-service')
        assert migrating is not None
        assert migrating.enabled
        assert migrating.phase is lb_ha.LbCutoverPhase.MIGRATING
        assert serve_state.finish_lb_ha_migration('ha-service', 'incarnation',
                                                  owner, 11)


# =================== Aggregate Serve history on PG ====================


@pytest.fixture
def history_engine(pg_server, monkeypatch):
    url = _create_database(pg_server, f'history_{uuid.uuid4().hex[:8]}')
    engine = create_engine(url)
    serve_state.Base.metadata.create_all(engine)
    serve_history.metadata.create_all(engine)
    # Status history joins the committed capacity plan head (#1838), so the
    # PostgreSQL fixture must carry the admission tables alembic would create.
    capacity_admission_schema.metadata.create_all(engine)
    placement_history.metadata.create_all(engine)
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    placement_history.reset_request_buffer()
    yield engine
    placement_history.reset_request_buffer()
    engine.dispose()


class TestServePlacementHistoryPG:

    def test_flush_pages_and_scopes_exact_incarnation(self, history_engine):
        del history_engine  # Fixture initializes the PostgreSQL metadata.
        now = time.time()
        for timestamp, service_hash, outcome in [
            (now, 'hash-a', 'capacity_failed'),
            (now + 1, 'hash-a', 'succeeded'),
            (now + 2, 'hash-b', 'quota_failed'),
        ]:
            assert placement_history.record_event(
                service_name='svc',
                service_hash=service_hash,
                request_id='request-a',
                cluster_name='svc-1',
                outcome=outcome,
                provider='AWS',
                region='us-east-1',
                zone='us-east-1a',
                instance_type='g6.4xlarge',
                num_nodes=1,
                hourly_price=0.25,
                error_summary='\x1b[31m capacity\n unavailable \x1b[0m',
                timestamp=timestamp)

        assert placement_history.flush_request_buffer() == 3
        first = placement_history.get_history('svc',
                                              'hash-a',
                                              limit=1,
                                              timestamp=now + 3)
        assert [event['outcome'] for event in first['events']] == ['succeeded']
        assert first['next_cursor'] is not None
        second = placement_history.get_history('svc',
                                               'hash-a',
                                               limit=1,
                                               cursor=first['next_cursor'],
                                               timestamp=now + 3)
        assert [event['outcome'] for event in second['events']
               ] == ['capacity_failed']
        assert second['events'][0]['error_summary'] == 'capacity unavailable'
        assert first['outcome_counts'] == {
            'capacity_failed': 1,
            'succeeded': 1,
        }

        recreated = placement_history.get_history('svc',
                                                  'hash-b',
                                                  timestamp=now + 3)
        assert [event['outcome'] for event in recreated['events']
               ] == ['quota_failed']


class TestServeStatusHistoryPG:

    def test_snapshot_excludes_cleaned_retained_failure(self, history_engine):
        services = serve_state.services_table
        replicas = serve_state.replicas_table
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(services).values(name='svc',
                                                   hash='hash-a',
                                                   current_version=1,
                                                   pool=0))
            connection.execute(
                sqlalchemy.insert(replicas).values(service_name='svc',
                                                   replica_id=1,
                                                   status='FAILED_PROBING',
                                                   sky_down_status='SUCCEEDED',
                                                   version=1))

        timestamp = 1784207110.0
        serve_history.record_status_snapshot(timestamp)
        history = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 1)

        assert len(history['samples']) == 1
        assert history['samples'][0]['total_count'] == 0
        assert history['samples'][0]['errored_count'] == 0

    def test_snapshot_groups_capacity_modes_reserved_ready_and_zero_capacity(
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
                'replica_state': {
                    'planned_capacity': 8,
                    'reserved_fill': True,
                },
            }, {
                'service_name': 'svc',
                'replica_id': 2,
                'status': 'FAILED_PROBING',
                'version': 1,
                'replica_state': {
                    'planned_capacity': 4,
                    'reserved_fill': True,
                },
            }, {
                'service_name': 'svc',
                'replica_id': 3,
                'status': 'PROVISIONING',
                'version': 2,
                'replica_state': {
                    'planned_capacity': 2,
                    'reserved_fill': False,
                },
            }, {
                'service_name': 'pool',
                'replica_id': 1,
                'status': 'READY',
                'version': 1,
                'replica_state': {
                    'planned_capacity': 16,
                    'reserved_fill': True,
                },
            }])

        timestamp = 1784207110.0
        assert serve_history.record_status_snapshot(timestamp) == 3
        history = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 1)
        assert [
            (row['version'], row['ready_count'], row['ready_reserved_count'],
             row['provisioning_count'], row['errored_count'],
             row['total_count'], row['logical_ready_count'],
             row['logical_ready_reserved_count'],
             row['logical_provisioning_count'], row['logical_errored_count'],
             row['logical_total_count']) for row in history['samples']
        ] == [
            (1, 1, 1, 0, 1, 2, 8, 8, 0, 4, 12),
            (2, 0, 0, 1, 0, 1, 0, 0, 2, 0, 2),
        ]
        empty = serve_history.get_status_history('empty',
                                                 timestamp=timestamp + 1)
        assert len(empty['samples']) == 1
        assert empty['samples'][0]['version'] == 7
        assert empty['samples'][0]['total_count'] == 0
        assert empty['samples'][0]['logical_total_count'] == 0

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

        stale = serve_history.get_status_history(
            'svc', timestamp=timestamp + 71, expected_service_hash='old-hash')
        assert stale['available'] is False
        assert not stale['samples']

    def test_request_history_is_idempotent_additive_and_incarnation_scoped(
            self, history_engine):
        timestamp = 1784207110.0
        bucket_start = int(timestamp) // 60 * 60

        def request_history(count, rejected=0):
            return {
                'bucket_seconds': 60,
                'buckets': [{
                    'bucket_start': bucket_start,
                    'request_count': count,
                    'rejected_count': rejected,
                }],
            }

        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc', hash='hash-a', current_version=1, pool=0))

        assert serve_history.record_request_activity('svc', 'hash-a',
                                                     'pod-a:process-a',
                                                     request_history(3, 1),
                                                     timestamp) == 1
        # Stale/out-of-order retry cannot decrement the exact counter.
        serve_history.record_request_activity('svc', 'hash-a',
                                              'pod-a:process-a',
                                              request_history(2), timestamp + 1)
        serve_history.record_request_activity('svc', 'hash-a',
                                              'pod-a:process-a',
                                              request_history(5,
                                                              2), timestamp + 2)
        # A concurrently live maxSurge process receives distinct requests, so
        # its cumulative counter is additive.
        serve_history.record_request_activity('svc', 'hash-a',
                                              'pod-b:process-b',
                                              request_history(7,
                                                              3), timestamp + 3)

        history = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 4)
        assert history['request_samples'] == [{
            'timestamp': float(bucket_start),
            'request_count': 12,
            'rejected_count': 5,
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
        assert not current['request_samples']
        assert current['requests_last_hour'] == 0

    def test_request_history_persists_explicit_idle_coverage(
            self, history_engine):
        timestamp = 1784207110.0
        current_bucket = int(timestamp) // 60 * 60
        covered_bucket = current_bucket - 60
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc',
                    hash='hash-a',
                    current_version=1,
                    pool=0,
                    lb_ha_enabled=1,
                    lb_active_slot='a',
                    lb_cutover_generation=1))

        heartbeat = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': covered_bucket,
                'request_count': 0,
                'rejected_count': 0,
                'coverage_complete': True,
            }],
        }
        authority = serve_history.RequestHistoryCoverageAuthority(
            reporter_slot=lb_ha.LbSlot.A,
            applied_role=lb_ha.LbRole.ACTIVE,
            applied_generation=1)
        assert serve_history.record_request_activity(
            'svc',
            'hash-a',
            'pod-a:process-a',
            heartbeat,
            timestamp,
            coverage_authority=authority) == 1

        history = serve_history.get_status_history('svc', timestamp=timestamp)
        assert history['request_samples'] == [{
            'timestamp': float(covered_bucket),
            'request_count': 0,
            'rejected_count': 0,
        }]
        assert history['rejection_history_available'] is True

    @pytest.mark.parametrize(
        ('phase', 'expected_rows'),
        [
            (lb_ha.LbCutoverPhase.STABLE, 1),
            (lb_ha.LbCutoverPhase.DRAINING, 1),
            (lb_ha.LbCutoverPhase.PREPARING, 0),
            (lb_ha.LbCutoverPhase.MIGRATING, 0),
            (lb_ha.LbCutoverPhase.ROLLING_BACK, 0),
        ],
    )
    def test_idle_coverage_requires_selector_committed_cutover_phase(
            self, history_engine, phase, expected_rows):
        timestamp = 1784207110.0
        covered_bucket = int(timestamp) // 60 * 60 - 60
        pending_slot = ('b'
                        if phase in (lb_ha.LbCutoverPhase.PREPARING,
                                     lb_ha.LbCutoverPhase.DRAINING) else None)
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc',
                    hash='hash-a',
                    current_version=1,
                    pool=0,
                    lb_ha_enabled=1,
                    lb_active_slot='a',
                    lb_cutover_generation=2,
                    lb_pending_slot=pending_slot,
                    lb_cutover_phase=phase.value))

        heartbeat = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': covered_bucket,
                'request_count': 0,
                'rejected_count': 0,
                'coverage_complete': True,
            }],
        }
        authority = serve_history.RequestHistoryCoverageAuthority(
            reporter_slot=lb_ha.LbSlot.A,
            applied_role=lb_ha.LbRole.ACTIVE,
            applied_generation=2)
        assert serve_history.record_request_activity(
            'svc',
            'hash-a',
            'pod-a:process-a',
            heartbeat,
            timestamp,
            coverage_authority=authority) == expected_rows

    def test_db_cutover_fences_stale_idle_coverage_but_keeps_events(
            self, history_engine):
        """A former ACTIVE cannot fill a gap before learning its demotion."""
        timestamp = 1784207110.0
        current_bucket = int(timestamp) // 60 * 60
        stale_authority = serve_history.RequestHistoryCoverageAuthority(
            reporter_slot=lb_ha.LbSlot.A,
            applied_role=lb_ha.LbRole.ACTIVE,
            applied_generation=1)
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc',
                    hash='hash-a',
                    current_version=1,
                    pool=0,
                    lb_ha_enabled=1,
                    lb_active_slot='a',
                    lb_cutover_generation=1))
            # PostgreSQL commits the cutover before the old process receives
            # and applies its DRAINING role heartbeat.
            connection.execute(
                sqlalchemy.update(serve_state.services_table).where(
                    serve_state.services_table.c.name == 'svc').values(
                        lb_active_slot='b', lb_cutover_generation=2))

        report = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': current_bucket - 60,
                'request_count': 0,
                'rejected_count': 0,
                'coverage_complete': True,
            }, {
                'bucket_start': current_bucket,
                'request_count': 2,
                'rejected_count': 0,
            }],
        }
        assert serve_history.record_request_activity(
            'svc',
            'hash-a',
            'pod-a:process-a',
            report,
            timestamp,
            coverage_authority=stale_authority) == 1

        history = serve_history.get_status_history('svc', timestamp=timestamp)
        assert history['request_samples'] == [{
            'timestamp': float(current_bucket),
            'request_count': 2,
            'rejected_count': 0,
        }]

    @pytest.mark.parametrize('missing_payload_authority', [True, False])
    def test_unknown_or_non_ha_reporter_cannot_create_idle_coverage(
            self, history_engine, missing_payload_authority):
        timestamp = 1784207110.0
        covered_bucket = int(timestamp) // 60 * 60 - 60
        service_values = {
            'name': 'svc',
            'hash': 'hash-a',
            'current_version': 1,
            'pool': 0,
        }
        coverage_authority = (None if missing_payload_authority else
                              serve_history.RequestHistoryCoverageAuthority(
                                  reporter_slot=lb_ha.LbSlot.A,
                                  applied_role=lb_ha.LbRole.ACTIVE,
                                  applied_generation=1))
        if missing_payload_authority:
            service_values.update(lb_ha_enabled=1,
                                  lb_active_slot='a',
                                  lb_cutover_generation=1)
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(
                    serve_state.services_table).values(**service_values))

        heartbeat = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': covered_bucket,
                'request_count': 0,
                'rejected_count': 0,
                'coverage_complete': True,
            }],
        }
        assert serve_history.record_request_activity(
            'svc',
            'hash-a',
            'pod-a:process-a',
            heartbeat,
            timestamp,
            coverage_authority=coverage_authority) == 0
        assert serve_history.get_status_history(
            'svc', timestamp=timestamp)['request_samples'] == []

    def test_classification_only_row_does_not_fabricate_idle_coverage(
            self, history_engine):
        timestamp = 1784207110.0
        bucket_start = int(timestamp) // 60 * 60
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc', hash='hash-a', current_version=1, pool=0))

        classification = {
            'classification_version': 1,
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': bucket_start,
                'classified_request_count': 1,
                'counted_rejected_count': 1,
            }],
        }
        assert serve_history.record_request_classification(
            'svc', 'hash-a', 'pod-a:process-a', classification, timestamp) == 1

        history = serve_history.get_status_history('svc', timestamp=timestamp)
        assert history['request_samples'] == []
        assert history['rejection_history_available'] is False

    def test_classification_support_cannot_bypass_idle_authority_fence(
            self, history_engine):
        timestamp = 1784207110.0
        covered_bucket = int(timestamp) // 60 * 60 - 60
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc',
                    hash='hash-a',
                    current_version=1,
                    pool=0,
                    lb_ha_enabled=1,
                    lb_active_slot='b',
                    lb_cutover_generation=2))

        classification = {
            'classification_version': 1,
            'bucket_seconds': 60,
            'buckets': [],
        }
        stale_heartbeat = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': covered_bucket,
                'request_count': 0,
                'rejected_count': 0,
                'coverage_complete': True,
            }],
        }
        assert serve_history.record_request_classification(
            'svc',
            'hash-a',
            'pod-a:process-a',
            classification,
            timestamp,
            request_history=stale_heartbeat) == 0
        assert serve_history.get_status_history(
            'svc', timestamp=timestamp)['request_samples'] == []

    def test_classification_support_preserves_arrivals_before_second_writer(
            self, history_engine):
        timestamp = 1784207110.0
        bucket_start = int(timestamp) // 60 * 60
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc', hash='hash-a', current_version=1, pool=0))

        classification = {
            'classification_version': 1,
            'bucket_seconds': 60,
            'buckets': [],
        }
        request_history = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': bucket_start,
                'request_count': 3,
                'rejected_count': 0,
            }],
        }
        assert serve_history.record_request_classification(
            'svc',
            'hash-a',
            'pod-a:process-a',
            classification,
            timestamp,
            request_history=request_history) == 1

        history = serve_history.get_status_history('svc', timestamp=timestamp)
        assert history['request_samples'] == [{
            'timestamp': float(bucket_start),
            'request_count': 3,
            'rejected_count': 0,
        }]

    def test_request_classification_is_exact_monotonic_and_version_safe(
            self, history_engine):
        day = datetime.datetime(2026, 7, 31, tzinfo=datetime.timezone.utc)
        timestamp = day.timestamp() + 30
        bucket_start = int(day.timestamp())

        def request_history(count):
            return {
                'bucket_seconds': 60,
                'buckets': [{
                    'bucket_start': bucket_start,
                    'request_count': count,
                    'rejected_count': 0,
                }],
            }

        def classification_history(classified, rejected):
            return {
                'classification_version': 1,
                'bucket_seconds': 60,
                'buckets': [{
                    'bucket_start': bucket_start,
                    'classified_request_count': classified,
                    'counted_rejected_count': rejected,
                }],
            }

        assert serve_history.record_request_activity('svc', 'hash-a', 'current',
                                                     request_history(3),
                                                     timestamp) == 1
        raw = serve_history.serve_request_activity_history_table
        with history_engine.connect() as connection:
            unclassified = connection.execute(
                sqlalchemy.select(raw.c.classified_request_count,
                                  raw.c.counted_rejected_count)).one()
        assert unclassified == (None, None)
        assert serve_history.record_request_classification(
            'svc',
            'hash-a',
            'current',
            classification_history(3, 1),
            timestamp + 1,
            request_history=request_history(3)) == 1
        # Independent stale deliveries cannot lower either component.
        serve_history.record_request_classification(
            'svc',
            'hash-a',
            'current',
            classification_history(2, 0),
            timestamp + 2,
            request_history=request_history(3))
        # A legacy reporter remains distinguishable from a capable reporter.
        serve_history.record_request_activity('mixed', 'hash-b', 'legacy',
                                              request_history(4), timestamp)

        with history_engine.connect() as connection:
            rows = connection.execute(
                sqlalchemy.select(
                    raw.c.service_name,
                    raw.c.request_count,
                    raw.c.classified_request_count,
                    raw.c.counted_rejected_count,
                    raw.c.rejection_count_available,
                ).order_by(raw.c.service_name)).fetchall()
        assert rows == [('mixed', 4, None, None, True), ('svc', 3, 3, 1, True)]

        assert serve_history.rollup_request_activity_daily(timestamp + 60) == 2
        summary = serve_history.get_daily_request_summary(
            history_engine,
            bucket_start,
            bucket_start, [{
                'day_start_utc': bucket_start
            }],
            table_limit=50,
            chart_limit=1)
        exact = summary['non_rejected']
        assert exact['available']
        assert exact['coverage'] == 'partial'
        assert exact['complete_by_day'] == [False]
        assert exact['total_request_count'] == 2
        assert exact['services'] == [{
            'service_name': 'svc',
            'request_count': 2,
            'coverage': 'complete',
            'complete_by_day': [True],
        }, {
            'service_name': 'mixed',
            'request_count': 0,
            'coverage': 'unavailable',
            'complete_by_day': [False],
        }]
        assert exact['series'] == [{
            'service_name': 'svc',
            'request_count_by_day': [2],
        }, {
            'is_other': True,
            'request_count_by_day': [None],
        }]

        daily = serve_history.serve_request_activity_daily_table
        with history_engine.begin() as connection:
            connection.execute(sqlalchemy.delete(raw))
        assert serve_history.rollup_request_activity_daily(timestamp + 120) == 0
        with history_engine.connect() as connection:
            daily_rows = connection.execute(
                sqlalchemy.select(
                    daily.c.service_name,
                    daily.c.classified_request_count,
                    daily.c.counted_rejected_count,
                    daily.c.classification_incomplete,
                ).order_by(daily.c.service_name)).fetchall()
        assert daily_rows == [('mixed', None, None, True), ('svc', 3, 1, False)]

    def test_valid_classification_survives_malformed_arrival_snapshot(
            self, history_engine):
        day = datetime.datetime(2026, 7, 31, tzinfo=datetime.timezone.utc)
        timestamp = day.timestamp() + 30
        bucket_start = int(day.timestamp())
        classification_history = {
            'classification_version': 1,
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': bucket_start,
                'classified_request_count': 2,
                'counted_rejected_count': 1,
            }],
        }
        malformed_request_history = {
            'bucket_seconds': 60,
            'buckets': 'not-a-list',
        }

        assert serve_history.record_request_classification(
            'svc',
            'hash-a',
            'reporter',
            classification_history,
            timestamp,
            request_history=malformed_request_history) == 1
        with pytest.raises(ValueError, match='buckets must be a list'):
            serve_history.record_request_activity('svc', 'hash-a', 'reporter',
                                                  malformed_request_history,
                                                  timestamp)

        raw = serve_history.serve_request_activity_history_table
        with history_engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(
                    raw.c.request_count,
                    raw.c.classified_request_count,
                    raw.c.counted_rejected_count,
                )).one()
        assert row == (0, 2, 1)

    def test_request_classification_constraints_reject_one_sided_nulls(
            self, history_engine):
        day = datetime.datetime(2026, 7, 31, tzinfo=datetime.timezone.utc)
        raw = serve_history.serve_request_activity_history_table
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            with history_engine.begin() as connection:
                connection.execute(
                    sqlalchemy.insert(raw).values(
                        service_name='svc',
                        service_hash='hash-a',
                        reporter_session_id='reporter',
                        bucket_start=day,
                        observed_at=day,
                        request_count=1,
                        classified_request_count=1,
                        counted_rejected_count=None))

        daily = serve_history.serve_request_activity_daily_table
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            with history_engine.begin() as connection:
                connection.execute(
                    sqlalchemy.insert(daily).values(
                        day_start=day,
                        service_name='svc',
                        service_hash='hash-a',
                        first_bucket_start=day,
                        last_bucket_start=day,
                        request_count=1,
                        classified_request_count=1,
                        counted_rejected_count=None,
                        classified_first_bucket_start=day,
                        classified_last_bucket_start=day,
                        observed_at=day))

    def test_empty_classification_atomically_promotes_support_buckets(
            self, history_engine):
        day = datetime.datetime(2026, 7, 31, tzinfo=datetime.timezone.utc)
        timestamp = day.timestamp() + 30
        bucket_start = int(day.timestamp())
        request_history = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': bucket_start,
                'request_count': 2,
                'rejected_count': 0,
            }],
        }
        empty_classification = {
            'classification_version': 1,
            'bucket_seconds': 60,
            'buckets': [],
        }
        serve_history.record_request_activity('svc', 'hash-a', 'reporter',
                                              request_history, timestamp)
        assert serve_history.record_request_classification(
            'svc',
            'hash-a',
            'reporter',
            empty_classification,
            timestamp,
            request_history=request_history) == 1

        raw = serve_history.serve_request_activity_history_table
        with history_engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(raw.c.request_count,
                                  raw.c.classified_request_count,
                                  raw.c.counted_rejected_count)).one()
        assert row == (2, 0, 0)

        bad_classification = {
            **empty_classification,
            'buckets': [{
                'bucket_start': bucket_start,
                'classified_request_count': 1,
                'counted_rejected_count': 2,
            }],
        }
        with pytest.raises(ValueError, match='cannot exceed'):
            serve_history.validate_request_classification_history(
                bad_classification, timestamp)
        with pytest.raises(ValueError, match='unsupported'):
            serve_history.validate_request_classification_history(
                {
                    **empty_classification,
                    'classification_version': True,
                }, timestamp)

    def test_daily_incomplete_latch_survives_late_classification_support(
            self, history_engine):
        day = datetime.datetime(2026, 7, 31, tzinfo=datetime.timezone.utc)
        timestamp = day.timestamp() + 30
        bucket_start = int(day.timestamp())
        request_history = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': bucket_start,
                'request_count': 1,
                'rejected_count': 0,
            }],
        }
        classification_history = {
            'classification_version': 1,
            'bucket_seconds': 60,
            'buckets': [],
        }

        serve_history.record_request_activity('svc', 'hash-a', 'reporter',
                                              request_history, timestamp)
        assert serve_history.rollup_request_activity_daily(timestamp) == 1

        serve_history.record_request_classification(
            'svc',
            'hash-a',
            'reporter',
            classification_history,
            timestamp + 1,
            request_history=request_history)
        assert serve_history.rollup_request_activity_daily(timestamp + 2) == 1

        daily = serve_history.serve_request_activity_daily_table
        with history_engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(daily.c.request_count,
                                  daily.c.classified_request_count,
                                  daily.c.counted_rejected_count,
                                  daily.c.classification_incomplete)).one()
        assert row == (1, 0, 0, True)

    def test_non_rejected_zero_range_after_coverage_is_available(
            self, history_engine):
        first_day = datetime.datetime(2026, 7, 30, tzinfo=datetime.timezone.utc)
        selected_day = first_day + datetime.timedelta(days=1)
        daily = serve_history.serve_request_activity_daily_table
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(daily),
                [
                    {
                        'day_start': first_day,
                        'service_name': 'svc',
                        'service_hash': 'hash-a',
                        'first_bucket_start': first_day,
                        'last_bucket_start': first_day,
                        'request_count': 1,
                        'classified_request_count': 1,
                        'counted_rejected_count': 0,
                        'classified_first_bucket_start': first_day,
                        'classified_last_bucket_start': first_day,
                        'classification_incomplete': False,
                        'observed_at': first_day,
                    },
                    {
                        # This is the durable shape of a legacy pre-admission
                        # rejection-only minute. It has no attempt to classify and
                        # must remain an exact zero after service coverage begins.
                        'day_start': selected_day,
                        'service_name': 'svc',
                        'service_hash': 'hash-a',
                        'first_bucket_start': selected_day,
                        'last_bucket_start': selected_day,
                        'request_count': 0,
                        'classified_request_count': None,
                        'counted_rejected_count': None,
                        'classified_first_bucket_start': None,
                        'classified_last_bucket_start': None,
                        'classification_incomplete': False,
                        'observed_at': selected_day,
                    }
                ])

        selected_epoch = int(selected_day.timestamp())
        summary = serve_history.get_daily_request_summary(
            history_engine,
            selected_epoch,
            selected_epoch, [{
                'day_start_utc': selected_epoch
            }],
            table_limit=50,
            chart_limit=8)
        assert summary['non_rejected'] == {
            'available': True,
            'definition': 'non_rejected_inbound_requests',
            'coverage_start_utc': int(first_day.timestamp()),
            'coverage': 'complete',
            'complete_by_day': [True],
            'total_request_count': 0,
            'services': [{
                'service_name': 'svc',
                'request_count': 0,
                'coverage': 'complete',
                'complete_by_day': [True],
            }],
            'series': [{
                'service_name': 'svc',
                'request_count_by_day': [0],
            }],
        }

    def test_daily_request_rollup_is_monotonic_and_groups_incarnations(
            self, history_engine):
        day = datetime.datetime(2026, 7, 27, tzinfo=datetime.timezone.utc)
        request_table = serve_history.serve_request_activity_history_table
        with history_engine.begin() as connection:
            connection.execute(sqlalchemy.insert(request_table), [{
                'service_name': 'svc',
                'service_hash': 'hash-a',
                'reporter_session_id': 'reporter-a',
                'bucket_start': day + datetime.timedelta(minutes=1),
                'observed_at': day + datetime.timedelta(minutes=2),
                'request_count': 3,
                'rejected_count': 0,
                'rejection_count_available': True,
            }, {
                'service_name': 'svc',
                'service_hash': 'hash-a',
                'reporter_session_id': 'reporter-b',
                'bucket_start': day + datetime.timedelta(minutes=1),
                'observed_at': day + datetime.timedelta(minutes=2),
                'request_count': 4,
                'rejected_count': 0,
                'rejection_count_available': True,
            }, {
                'service_name': 'svc',
                'service_hash': 'hash-b',
                'reporter_session_id': 'reporter-c',
                'bucket_start': day + datetime.timedelta(minutes=2),
                'observed_at': day + datetime.timedelta(minutes=3),
                'request_count': 2,
                'rejected_count': 0,
                'rejection_count_available': True,
            }, {
                'service_name': 'other',
                'service_hash': 'hash-other',
                'reporter_session_id': 'reporter-d',
                'bucket_start': day + datetime.timedelta(minutes=3),
                'observed_at': day + datetime.timedelta(minutes=4),
                'request_count': 1,
                'rejected_count': 0,
                'rejection_count_available': True,
            }, {
                'service_name': 'svc',
                'service_hash': 'hash-b',
                'reporter_session_id': 'reporter-c',
                'bucket_start': day + datetime.timedelta(days=1, minutes=1),
                'observed_at': day + datetime.timedelta(days=1, minutes=2),
                'request_count': 5,
                'rejected_count': 0,
                'rejection_count_available': True,
            }])
            # UTC grouping must not depend on the database session timezone.
            connection.execute(
                sqlalchemy.text("SET TIME ZONE 'America/Los_Angeles'"))

        timestamp = (day + datetime.timedelta(days=1, minutes=5)).timestamp()
        assert serve_history.rollup_request_activity_daily(timestamp) == 4
        days = [{
            'day_start_utc': int(day.timestamp())
        }, {
            'day_start_utc': int((day + datetime.timedelta(days=1)).timestamp())
        }]
        summary = serve_history.get_daily_request_summary(
            history_engine,
            int(day.timestamp()),
            int((day + datetime.timedelta(days=1)).timestamp()),
            days,
            table_limit=50,
            chart_limit=1)
        assert summary['available']
        assert summary['coverage_start_utc'] == int(
            (day + datetime.timedelta(minutes=1)).timestamp())
        assert summary['total_request_count'] == 15
        assert summary['services'] == [{
            'service_name': 'svc',
            'request_count': 14,
        }, {
            'service_name': 'other',
            'request_count': 1,
        }]
        assert summary['series'] == [{
            'service_name': 'svc',
            'request_count_by_day': [9, 5],
        }, {
            'is_other': True,
            'request_count_by_day': [1, 0],
        }]

        # A late cumulative update increases the daily rollup.
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(request_table).where(
                    request_table.c.reporter_session_id == 'reporter-a').values(
                        request_count=8))
        serve_history.rollup_request_activity_daily(timestamp + 60)
        summary = serve_history.get_daily_request_summary(
            history_engine,
            int(day.timestamp()),
            int((day + datetime.timedelta(days=1)).timestamp()),
            days,
            table_limit=50,
            chart_limit=1)
        assert summary['total_request_count'] == 20

        # Pruning the raw source cannot decrement durable daily totals.
        with history_engine.begin() as connection:
            connection.execute(sqlalchemy.delete(request_table))
        assert serve_history.rollup_request_activity_daily(timestamp + 120) == 0
        summary = serve_history.get_daily_request_summary(
            history_engine,
            int(day.timestamp()),
            int((day + datetime.timedelta(days=1)).timestamp()),
            days,
            table_limit=50,
            chart_limit=1)
        assert summary['total_request_count'] == 20

    def test_daily_request_coverage_query_is_bounded_to_first_day(
            self, history_engine):
        day = datetime.datetime(2026, 7, 27, tzinfo=datetime.timezone.utc)
        daily = serve_history.serve_request_activity_daily_table
        with history_engine.begin() as connection:
            connection.execute(sqlalchemy.insert(daily), [{
                'day_start': day + datetime.timedelta(days=offset),
                'service_name': f'svc-{offset}',
                'service_hash': f'hash-{offset}',
                'first_bucket_start': day + datetime.timedelta(
                    days=offset, minutes=offset + 1),
                'last_bucket_start': day +
                                     datetime.timedelta(days=offset, hours=1),
                'request_count': offset + 1,
                'observed_at': day + datetime.timedelta(days=offset, hours=2),
            } for offset in range(3)])

        statements = []

        def capture_statement(_connection, _cursor, statement, _parameters,
                              _context, _executemany):
            statements.append(' '.join(statement.split()))

        sqlalchemy.event.listen(history_engine, 'before_cursor_execute',
                                capture_statement)
        try:
            summary = serve_history.get_daily_request_summary(
                history_engine,
                int(day.timestamp()),
                int((day + datetime.timedelta(days=2)).timestamp()), [{
                    'day_start_utc': int(
                        (day + datetime.timedelta(days=offset)).timestamp())
                } for offset in range(3)],
                table_limit=50,
                chart_limit=8)
        finally:
            sqlalchemy.event.remove(history_engine, 'before_cursor_execute',
                                    capture_statement)

        assert summary['coverage_start_utc'] == int(
            (day + datetime.timedelta(minutes=1)).timestamp())
        coverage_query = next(statement for statement in statements
                              if 'min(serve_request_activity_daily.'
                              'first_bucket_start)' in statement)
        assert ('WHERE serve_request_activity_daily.day_start = '
                '(SELECT min(serve_request_activity_daily.day_start)'
               ) in coverage_query

    def test_estimated_spend_joins_daily_service_cost_and_requests(
            self, history_engine, monkeypatch):
        global_user_state.estimated_spend_daily_table.create(history_engine)
        global_user_state.estimated_spend_state_table.create(history_engine)
        monkeypatch.setattr(global_user_state, 'initialize_and_get_db',
                            mock.Mock(return_value=history_engine))
        day = datetime.datetime(2026, 7, 29, tzinfo=datetime.timezone.utc)
        day_start = int(day.timestamp())
        observed_at = day + datetime.timedelta(hours=1)
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(
                    serve_history.serve_request_activity_daily_table),
                [{
                    'day_start': day,
                    'service_name': 'svc',
                    'service_hash': 'hash-a',
                    'first_bucket_start': day,
                    'last_bucket_start': day + datetime.timedelta(minutes=59),
                    'request_count': 5,
                    'classified_request_count': 5,
                    'counted_rejected_count': 1,
                    'classified_first_bucket_start': day,
                    'classified_last_bucket_start':
                        day + datetime.timedelta(minutes=59),
                    'classification_incomplete': False,
                    'observed_at': observed_at,
                }, {
                    'day_start': day,
                    'service_name': 'zero-svc',
                    'service_hash': 'hash-b',
                    'first_bucket_start': day,
                    'last_bucket_start': day + datetime.timedelta(minutes=59),
                    'request_count': 2,
                    'classified_request_count': 2,
                    'counted_rejected_count': 0,
                    'classified_first_bucket_start': day,
                    'classified_last_bucket_start':
                        day + datetime.timedelta(minutes=59),
                    'classification_incomplete': False,
                    'observed_at': observed_at,
                }, {
                    'day_start': day,
                    'service_name': 'unknown-svc',
                    'service_hash': 'hash-c',
                    'first_bucket_start': day,
                    'last_bucket_start': day + datetime.timedelta(minutes=59),
                    'request_count': 1,
                    'classified_request_count': 1,
                    'counted_rejected_count': 0,
                    'classified_first_bucket_start': day,
                    'classified_last_bucket_start':
                        day + datetime.timedelta(minutes=59),
                    'classification_incomplete': False,
                    'observed_at': observed_at,
                }])
            connection.execute(
                sqlalchemy.insert(
                    global_user_state.estimated_spend_daily_table), [{
                        'day_start_utc': day_start,
                        'cluster_hash': 'svc-replica-1',
                        'cluster_name': 'svc-1',
                        'workload_type': 'service',
                        'workload_id': 'svc',
                        'cloud': 'AWS',
                        'use_spot': False,
                        'machine_seconds': 3600,
                        'catalog_hourly_rate': 2.0,
                        'estimated_cost': 2.0,
                        'exclusion_reason': None,
                        'updated_at': int(observed_at.timestamp()),
                    }, {
                        'day_start_utc': day_start,
                        'cluster_hash': 'svc-replica-2',
                        'cluster_name': 'svc-2',
                        'workload_type': 'service',
                        'workload_id': 'svc',
                        'cloud': 'Kubernetes',
                        'use_spot': False,
                        'machine_seconds': 3600,
                        'catalog_hourly_rate': None,
                        'estimated_cost': None,
                        'exclusion_reason': 'kubernetes',
                        'updated_at': int(observed_at.timestamp()),
                    }, {
                        'day_start_utc': day_start,
                        'cluster_hash': 'zero-svc-replica-1',
                        'cluster_name': 'zero-svc-1',
                        'workload_type': 'service',
                        'workload_id': 'zero-svc',
                        'cloud': 'Kubernetes',
                        'use_spot': False,
                        'machine_seconds': 3600,
                        'catalog_hourly_rate': None,
                        'estimated_cost': None,
                        'exclusion_reason': 'kubernetes',
                        'updated_at': int(observed_at.timestamp()),
                    }, {
                        'day_start_utc': day_start,
                        'cluster_hash': 'unknown-svc-replica-1',
                        'cluster_name': 'unknown-svc-1',
                        'workload_type': 'service',
                        'workload_id': 'unknown-svc',
                        'cloud': 'AWS',
                        'use_spot': False,
                        'machine_seconds': 3600,
                        'catalog_hourly_rate': None,
                        'estimated_cost': None,
                        'exclusion_reason': 'unknown_price',
                        'updated_at': int(observed_at.timestamp()),
                    }])
            connection.execute(
                sqlalchemy.insert(
                    global_user_state.estimated_spend_state_table).values(
                        singleton_id=1,
                        last_success_at=int(observed_at.timestamp()),
                        backfill_complete=True,
                        coverage_start_utc=day_start,
                    ))
        monkeypatch.setattr(estimated_spend.time, 'time',
                            mock.Mock(return_value=observed_at.timestamp()))

        response = estimated_spend.get_estimated_spend(days=1)

        attempt_services = {
            service['service_name']: service
            for service in response['service_requests']['services']
        }
        assert attempt_services['svc']['request_count'] == 5
        assert attempt_services['svc']['ratio_request_count'] == 0
        assert attempt_services['svc']['estimated_cost_per_request'] is None
        assert attempt_services['svc']['cost_coverage'] == 'unavailable'

        exact = response['service_requests']['non_rejected']
        services = {
            service['service_name']: service for service in exact['services']
        }
        service = services['svc']
        assert service['service_name'] == 'svc'
        assert service['request_count'] == 4
        assert service['ratio_request_count'] == 4
        assert service['estimated_cost'] == 2.0
        assert service['estimated_cost_per_request'] == 0.5
        assert service['cost_coverage'] == 'complete'
        assert service['priced_machine_seconds'] == 7200
        assert service['excluded_machine_seconds'] == 0
        assert exact['series'][0]['estimated_cost_per_request_by_day'] == [0.5]
        zero_service = services['zero-svc']
        assert zero_service['estimated_cost'] == 0
        assert zero_service['estimated_cost_per_request'] == 0
        assert zero_service['cost_coverage'] == 'complete'
        assert zero_service['priced_machine_seconds'] == 3600
        assert zero_service['excluded_machine_seconds'] == 0
        unknown_service = services['unknown-svc']
        assert unknown_service['estimated_cost_per_request'] is None
        assert unknown_service['cost_coverage'] == 'partial'
        assert unknown_service['priced_machine_seconds'] == 0
        assert unknown_service['excluded_machine_seconds'] == 3600

    def test_prediction_time_history_is_idempotent_and_reporter_additive(
            self, history_engine):
        timestamp = 1784207110.0
        bucket_start = int(timestamp) // 60 * 60
        bucket_count = constants.LB_PREDICTION_TIME_BUCKET_COUNT

        def prediction_history(successes, errors=0):
            success_counts = [0] * bucket_count
            error_counts = [0] * bucket_count
            success_counts[3] = successes
            error_counts[7] = errors
            return {
                'bucket_seconds': 60,
                'histogram_version': 1,
                'buckets': [{
                    'bucket_start': bucket_start,
                    'outcome_counts': {
                        'succeeded': success_counts,
                        'failed': error_counts,
                    },
                }],
            }

        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc', hash='hash-a', current_version=1, pool=0))

        assert serve_history.record_prediction_times('svc', 'hash-a',
                                                     'pod-a:process-a',
                                                     prediction_history(3, 1),
                                                     timestamp) == 1
        # A stale snapshot cannot decrement the reporter's cumulative arrays.
        serve_history.record_prediction_times('svc', 'hash-a',
                                              'pod-a:process-a',
                                              prediction_history(2),
                                              timestamp + 1)
        serve_history.record_prediction_times('svc', 'hash-a',
                                              'pod-a:process-a',
                                              prediction_history(5, 2),
                                              timestamp + 2)
        # A concurrently active reporter contributes distinct terminal
        # observations.
        serve_history.record_prediction_times('svc', 'hash-a',
                                              'pod-b:process-b',
                                              prediction_history(7, 3),
                                              timestamp + 3)
        # An equal cumulative retry advances report recency without claiming a
        # new terminal observation.
        serve_history.record_prediction_times('svc', 'hash-a',
                                              'pod-b:process-b',
                                              prediction_history(7, 3),
                                              timestamp + 4)

        history = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 5)
        assert history['prediction_time_histogram_version'] == 1
        assert history['prediction_time_bucket_upper_bounds_seconds'] == list(
            constants.LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS)
        assert len(history['prediction_time_samples']) == 1
        assert history[
            'prediction_time_latest_hour_reported_at'] == timestamp + 4
        sample = history['prediction_time_samples'][0]
        assert sample['timestamp'] == float(bucket_start)
        assert sample['outcome_counts']['succeeded'][3] == 12
        assert sample['outcome_counts']['failed'][7] == 5

        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    serve_state.services_table).values(hash='hash-b'))
        current = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 6)
        assert current['service_hash'] == 'hash-b'
        assert not current['prediction_time_samples']

    def test_prediction_time_history_rejects_invalid_array_shape(
            self, history_engine):
        timestamp = 1784207110.0
        bucket_start = int(timestamp) // 60 * 60
        invalid = {
            'bucket_seconds': 60,
            'histogram_version': 1,
            'buckets': [{
                'bucket_start': bucket_start,
                'outcome_counts': {
                    'succeeded': [1],
                },
            }],
        }
        with pytest.raises(ValueError, match='fixed number'):
            serve_history.record_prediction_times('svc', 'hash-a', 'reporter',
                                                  invalid, timestamp)

        prediction_table = serve_history.serve_prediction_time_history_table
        valid_zero_counts = [0] * constants.LB_PREDICTION_TIME_BUCKET_COUNT
        with pytest.raises(sqlalchemy.exc.IntegrityError), history_engine.begin(
        ) as connection:
            connection.execute(
                sqlalchemy.insert(prediction_table).values(
                    service_name='svc',
                    service_hash='hash-a',
                    reporter_session_id='reporter',
                    bucket_start=datetime.datetime.fromtimestamp(
                        bucket_start, datetime.timezone.utc),
                    observed_at=datetime.datetime.fromtimestamp(
                        timestamp, datetime.timezone.utc),
                    prediction_count=1,
                    succeeded_counts=[1],
                    failed_counts=valid_zero_counts,
                ))

    def test_autoscaler_history_retains_latest_state_and_minute_peaks(
            self, history_engine):
        timestamp = 1784207110.0
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc', hash='hash-a', current_version=1, pool=0))

        base = {
            'version': 1,
            'replica_unit': 'physical_backend',
            'demand_target': 5,
            'capacity_target': 10,
            'ready_capacity': 4,
            'provisioning_capacity': 3,
            'total_capacity': 12,
            'peak_in_flight': 2,
            'peak_queue_depth': 1,
            'accelerator_breakdown': {
                'configured_accelerators': ['A100', 'A100-80GB'],
                'min_replicas': {
                    'A100-80GB': 1
                },
                'demand_target': {
                    'A100': 2,
                    'A100-80GB': 3
                },
                'warm_retention_target': {
                    'A100': 2
                },
                'cold_launch_authority': {
                    'A100-80GB': 1
                },
                'ready_capacity': {
                    'A100': 2,
                    'A100-80GB': 2
                },
                'provisioning_capacity': {
                    'A100': 1,
                    'A100-80GB': 2
                },
                'total_capacity': {
                    'A100': 4,
                    'A100-80GB': 8
                },
                'zero_cost_ready_capacity': {
                    'A100': 1
                },
                'fill_target': {
                    'A100': 5,
                    'A100-80GB': 5
                },
                'free_reserved_slots': {
                    'A100': 2
                },
            },
        }
        assert serve_history.record_autoscaler_snapshot('svc',
                                                        'hash-a',
                                                        'a' * 32,
                                                        timestamp=timestamp,
                                                        **base) == 1
        # An older observation cannot replace state, but its peaks remain
        # meaningful for the minute.
        serve_history.record_autoscaler_snapshot(
            'svc',
            'hash-a',
            'a' * 32,
            timestamp=timestamp - 1,
            **{
                **base,
                'demand_target': 2,
                'capacity_target': 2,
                'ready_capacity': 2,
                'provisioning_capacity': 0,
                'total_capacity': 2,
                'peak_in_flight': 5,
                'peak_queue_depth': 4,
                'accelerator_breakdown': None,
            })
        serve_history.record_autoscaler_snapshot(
            'svc',
            'hash-a',
            'b' * 32,
            timestamp=timestamp + 20,
            **{
                **base,
                'version': 2,
                'demand_target': 6,
                'capacity_target': 12,
                'ready_capacity': 8,
                'provisioning_capacity': 2,
                'total_capacity': 14,
                'peak_in_flight': 3,
                'peak_queue_depth': 7,
                'accelerator_breakdown': {
                    **base['accelerator_breakdown'],
                    'demand_target': {
                        'A100': 3,
                        'A100-80GB': 3,
                    },
                },
            })

        history = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 21)
        assert history['autoscaler_samples'] == [{
            'timestamp': float(int(timestamp) // 60 * 60),
            'observed_at': timestamp + 20,
            'controller_session_id': 'b' * 32,
            'version': 2,
            'replica_unit': 'physical_backend',
            'demand_target': 6,
            'capacity_target': 12,
            'ready_capacity': 8,
            'provisioning_capacity': 2,
            'total_capacity': 14,
            'peak_in_flight': 5,
            'peak_queue_depth': 7,
            'accelerator_breakdown': {
                'version': 1,
                'configured_accelerators': ['A100', 'A100-80GB'],
                'min_replicas': {
                    'A100': 0,
                    'A100-80GB': 1
                },
                'demand_target': {
                    'A100': 3,
                    'A100-80GB': 3
                },
                'warm_retention_target': {
                    'A100': 2,
                    'A100-80GB': 0
                },
                'cold_launch_authority': {
                    'A100': 0,
                    'A100-80GB': 1
                },
                'ready_capacity': {
                    'A100': 2,
                    'A100-80GB': 2
                },
                'provisioning_capacity': {
                    'A100': 1,
                    'A100-80GB': 2
                },
                'total_capacity': {
                    'A100': 4,
                    'A100-80GB': 8
                },
                'zero_cost_ready_capacity': {
                    'A100': 1,
                    'A100-80GB': 0
                },
                'fill_target': {
                    'A100': 5,
                    'A100-80GB': 5
                },
                'free_reserved_slots': {
                    'A100': 2,
                    'A100-80GB': 0
                },
            },
        }]

        # An old rolling-upgrade writer can win an equal-timestamp upsert while
        # knowing only aggregate columns. Timestamp equality alone must not
        # make the prior exact-card map look coherent with its new aggregate.
        table = serve_history.serve_autoscaler_history_table
        equal_observed_at = datetime.datetime.fromtimestamp(
            timestamp + 20, datetime.timezone.utc)
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(table).values(
                    observed_at=equal_observed_at,
                    controller_session_id='c' * 32,
                    demand_target=4,
                    capacity_target=9,
                    ready_capacity=7,
                    provisioning_capacity=1,
                    total_capacity=11).where(table.c.service_name == 'svc',
                                             table.c.service_hash == 'hash-a'))
        equal_mixed = serve_history.get_status_history('svc',
                                                       timestamp=timestamp +
                                                       20.5)
        equal_sample = equal_mixed['autoscaler_samples'][0]
        assert equal_sample['demand_target'] == 4
        assert equal_sample['accelerator_breakdown'] is None

        # A rolling-upgrade writer that knows only aggregate columns may
        # advance observed_at without touching the new JSONB column. The
        # timestamp fence must hide that stale card assignment.
        newer_observed_at = datetime.datetime.fromtimestamp(
            timestamp + 21, datetime.timezone.utc)
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(table).values(
                    observed_at=newer_observed_at).where(
                        table.c.service_name == 'svc',
                        table.c.service_hash == 'hash-a'))
        mixed = serve_history.get_status_history('svc',
                                                 timestamp=timestamp + 22)
        assert mixed['autoscaler_samples'][0]['accelerator_breakdown'] is None

    def test_autoscaler_history_serializes_legacy_and_new_capacity_semantics(
            self, history_engine):
        timestamp = 1784207110.0
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc', hash='hash-a', current_version=1, pool=0))

        snapshot = {
            'version': 1,
            'replica_unit': 'physical_backend',
            'demand_target': 0,
            'capacity_target': 0,
            'ready_capacity': 0,
            'provisioning_capacity': 0,
            'total_capacity': 0,
        }
        capacity_semantics_version = (
            serve_history.ACCELERATOR_BREAKDOWN_CAPACITY_SEMANTICS_VERSION)
        serve_history.record_autoscaler_snapshot(
            'svc',
            'hash-a',
            'a' * 32,
            accelerator_breakdown={'configured_accelerators': ['A100']},
            timestamp=timestamp,
            **snapshot)
        serve_history.record_autoscaler_snapshot(
            'svc',
            'hash-a',
            'a' * 32,
            accelerator_breakdown={
                'capacity_semantics_version': capacity_semantics_version,
                'configured_accelerators': ['A100'],
            },
            timestamp=timestamp + 60,
            **snapshot)

        history = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 61)
        samples_by_timestamp = {
            sample['timestamp']: sample
            for sample in history['autoscaler_samples']
        }
        legacy = samples_by_timestamp[float(int(timestamp) // 60 *
                                            60)]['accelerator_breakdown']
        current = samples_by_timestamp[float(int(timestamp) // 60 * 60 +
                                             60)]['accelerator_breakdown']

        assert legacy['version'] == constants.LB_REQUEST_ACCELERATORS_VERSION
        assert 'capacity_semantics_version' not in legacy
        assert current['version'] == constants.LB_REQUEST_ACCELERATORS_VERSION
        assert current['capacity_semantics_version'] == 2

    def test_mixed_reporter_rejection_history_is_not_false_zero(
            self, history_engine):
        timestamp = 1784207110.0
        bucket_start = int(timestamp) // 60 * 60
        with history_engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(serve_state.services_table).values(
                    name='svc', hash='hash-a', current_version=1, pool=0))

        legacy = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': bucket_start,
                'request_count': 2,
            }],
        }
        current = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': bucket_start,
                'request_count': 3,
                'rejected_count': 0,
            }],
        }
        serve_history.record_request_activity('svc', 'hash-a', 'legacy', legacy,
                                              timestamp)
        serve_history.record_request_activity('svc', 'hash-a', 'current',
                                              current, timestamp)

        history = serve_history.get_status_history('svc',
                                                   timestamp=timestamp + 1)
        assert history['rejection_history_available'] is True
        assert history['request_samples'] == [{
            'timestamp': float(bucket_start),
            'request_count': 5,
            'rejected_count': None,
        }]

    def test_hourly_snapshot_prunes_rows_older_than_three_days(
            self, history_engine):
        now = 1784210400.0  # Exact UTC hour, so the bounded cleanup runs.
        old_bucket = datetime.datetime.fromtimestamp(
            now - 73 * 3600, datetime.timezone.utc).replace(second=0,
                                                            microsecond=0)
        table = serve_history.serve_replica_status_history_table
        request_table = serve_history.serve_request_activity_history_table
        response_table = serve_history.serve_response_time_history_table
        prediction_table = serve_history.serve_prediction_time_history_table
        autoscaler_table = serve_history.serve_autoscaler_history_table
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
                    request_count=1,
                    rejected_count=0))
            zero_counts = [0] * constants.LB_RESPONSE_TIME_BUCKET_COUNT
            connection.execute(
                sqlalchemy.insert(response_table).values(
                    service_name='old',
                    service_hash='old-hash',
                    reporter_session_id='pod:process',
                    bucket_start=old_bucket,
                    observed_at=old_bucket,
                    response_count=1,
                    status_1xx_counts=zero_counts,
                    status_2xx_counts=[1] + zero_counts[1:],
                    status_3xx_counts=zero_counts,
                    status_4xx_counts=zero_counts,
                    status_5xx_counts=zero_counts))
            prediction_zero_counts = [
                0
            ] * constants.LB_PREDICTION_TIME_BUCKET_COUNT
            connection.execute(
                sqlalchemy.insert(prediction_table).values(
                    service_name='old',
                    service_hash='old-hash',
                    reporter_session_id='pod:process',
                    bucket_start=old_bucket,
                    observed_at=old_bucket,
                    prediction_count=1,
                    succeeded_counts=[1] + prediction_zero_counts[1:],
                    failed_counts=prediction_zero_counts))
            connection.execute(
                sqlalchemy.insert(autoscaler_table).values(
                    service_name='old',
                    service_hash='old-hash',
                    bucket_start=old_bucket,
                    observed_at=old_bucket,
                    controller_session_id='a' * 32,
                    version=1,
                    replica_unit='physical_backend',
                    demand_target=1,
                    capacity_target=1,
                    ready_capacity=1,
                    provisioning_capacity=0,
                    total_capacity=1,
                    peak_in_flight=None,
                    peak_queue_depth=None))

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
            assert connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(response_table).where(
                    response_table.c.service_name == 'old')).scalar_one() == 0
            assert connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(prediction_table).where(
                    prediction_table.c.service_name == 'old')).scalar_one() == 0
            assert connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.count()  # pylint: disable=not-callable
                ).select_from(autoscaler_table).where(
                    autoscaler_table.c.service_name == 'old')).scalar_one() == 0


# ======================= Concurrency smoke on PG =======================


@pytest.fixture
def _pg_concurrency_db(pg_server, _pg_mechanics_template, monkeypatch):
    """PG serve DB with the REAL lock path (no nullcontext, no fake clock).

    locks.get_lock detects the backend (and PostgresLock borrows its
    advisory-lock connection) through global_user_state's engine; pointing
    that at the serve PG engine takes the advisory path exactly as a
    Postgres-backed api-server pod does.
    """
    database_name = f'concurrency_{uuid.uuid4().hex[:10]}'
    url = _create_database(pg_server,
                           database_name,
                           template=_pg_mechanics_template)
    engine = create_engine(url)
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    monkeypatch.setattr(locks.global_user_state, 'initialize_and_get_db',
                        lambda: engine)
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        mock.Mock(return_value=[]))
    broker.clear_caches()
    try:
        yield engine
    finally:
        broker.clear_caches()
        engine.dispose()
        _drop_database(pg_server, database_name)


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

            def query(query_calls=query_calls):  # pylint: disable=dangerous-default-value
                query_calls.append(1)
                # Hold the round (and thus the advisory lock) long enough
                # that the losing thread must actually wait on it.
                time.sleep(0.3)
                return broker.PoolObservation(free_slots=10,
                                              gpu_names=('A100',))

            # Defaults deliberately freeze this iteration for both threads.
            # pylint: disable=dangerous-default-value
            def run(name,
                    pool=pool,
                    barrier=barrier,
                    results=results,
                    errors=errors):
                try:
                    barrier.wait(timeout=30)
                    results[name] = broker.run_round_if_stale(
                        name, pool, query, poll_interval_seconds=300.0)
                except Exception as e:  # pylint: disable=broad-except
                    errors.append(e)

            # pylint: enable=dangerous-default-value

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


def test_advisory_lock_does_not_consume_ordinary_pool(pg_server, monkeypatch):
    """Nested session locks leave a one-slot ORM QueuePool available."""
    url = _create_database(pg_server, f'lock_pool_{uuid.uuid4().hex[:8]}')
    engine = create_engine(url,
                           poolclass=sqlalchemy.pool.QueuePool,
                           pool_size=1,
                           max_overflow=0,
                           pool_timeout=1)
    monkeypatch.setattr(locks.global_user_state, 'initialize_and_get_db',
                        lambda: engine)
    lock = locks.PostgresLock('dedicated-lock-session')
    nested_lock = locks.PostgresLock('nested-lock-session')
    successor = locks.PostgresLock('dedicated-lock-session')
    connection_url = engine.url.render_as_string(hide_password=False)

    try:
        lock.acquire()
        nested_lock.acquire()
        assert engine.pool.checkedout() == 0

        def _get_holder_pid(connection):
            cursor = connection.cursor()
            try:
                cursor.execute('SELECT pg_backend_pid()')
                pid = cursor.fetchone()[0]
                connection.commit()
                return pid
            finally:
                cursor.close()

        holder_pid = lock.run_in_lock_session(_get_holder_pid)
        with engine.connect() as observer:
            state, transaction_started, application_name = observer.execute(
                sqlalchemy.text('SELECT state, xact_start, application_name '
                                'FROM pg_stat_activity WHERE pid = :pid'), {
                                    'pid': holder_pid
                                }).one()
        assert state == 'idle'
        assert transaction_started is None
        assert application_name == 'skypilot-advisory-lock'
        assert engine.pool.checkedout() == 0

        contender = locks.PostgresLock('dedicated-lock-session')
        with pytest.raises(locks.LockTimeout):
            contender.acquire(blocking=False)
        assert engine.pool.checkedout() == 0

        # Only the two acquired locks may retain sessions; the failed contender
        # commits and disconnects before returning LockTimeout. Closing the
        # client socket does not synchronously remove the row from
        # pg_stat_activity, though -- PostgreSQL reaps the backend process on
        # its own schedule -- so poll for the contender to drain instead of
        # racing the reaper. A contender that really leaked its session keeps
        # the count at three for the whole window and still fails the assert.
        deadline = time.monotonic() + 10
        while True:
            with engine.connect() as observer:
                lock_sessions = observer.execute(
                    sqlalchemy.text('SELECT count(*) FROM pg_stat_activity '
                                    'WHERE datname = current_database() '
                                    'AND application_name = :application_name'),
                    {
                        'application_name': 'skypilot-advisory-lock'
                    }).scalar_one()
            if lock_sessions <= 2 or time.monotonic() >= deadline:
                break
            time.sleep(0.05)
        assert lock_sessions == 2

        lock.release()
        successor.acquire(blocking=False)
    finally:
        successor.release()
        nested_lock.release()
        lock.release()
        engine.dispose()
        lock_engine = db_utils._postgres_lock_engine_cache.pop(
            connection_url, None)
        if lock_engine is not None:
            lock_engine.dispose()


def test_held_advisory_lock_leaves_pool_free_for_protected_query(
        pg_server, monkeypatch):
    """Reproduce the exact circular wait #936 fixes, end to end.

    ``test_advisory_lock_does_not_consume_ordinary_pool`` asserts the necessary
    precondition (``pool.checkedout() == 0``).  This test asserts the sufficient
    condition the bug actually broke: while an advisory lock is held, the
    protected code can still complete an *ordinary* pooled ORM checkout on a
    strict one-slot ``QueuePool(pool_size=1, max_overflow=0)`` engine.

    Pre-fix (``engine.raw_connection()`` off the ordinary pool) the lock
    consumed the single slot, so ``engine.connect()`` below blocked for
    ``pool_timeout`` and raised ``sqlalchemy.exc.TimeoutError`` -- the reported
    ``QueuePool timeout``/``idle in transaction`` wedge.  The dedicated
    ``NullPool`` lock session keeps the slot free so the query returns.
    """
    url = _create_database(pg_server, f'lock_circular_{uuid.uuid4().hex[:8]}')
    pool_timeout = 3
    engine = create_engine(url,
                           poolclass=sqlalchemy.pool.QueuePool,
                           pool_size=1,
                           max_overflow=0,
                           pool_timeout=pool_timeout)
    monkeypatch.setattr(locks.global_user_state, 'initialize_and_get_db',
                        lambda: engine)
    lock = locks.PostgresLock('circular-wait-lock')
    connection_url = engine.url.render_as_string(hide_password=False)

    try:
        lock.acquire()
        assert engine.pool.checkedout() == 0

        started = time.monotonic()
        with engine.connect() as protected:
            assert protected.execute(sqlalchemy.text('SELECT 1')).scalar() == 1
        # A completed checkout proves no circular wait; guard against a silent
        # near-timeout regression that still technically returns.
        assert time.monotonic() - started < pool_timeout
        assert engine.pool.checkedout() == 0
    finally:
        lock.release()
        engine.dispose()
        lock_engine = db_utils._postgres_lock_engine_cache.pop(
            connection_url, None)
        if lock_engine is not None:
            lock_engine.dispose()


# =========== Utilization-gate version-skew invariant (#966), on PG ===========
# The gate's anti-skew guard (activity_ts) only holds if a pre-gate binary's
# upsert leaves the three new claim columns FROZEN while advancing
# heartbeat_ts, and if migration 030 makes an existing populated row read as
# ungated. Both are properties of real Postgres ON CONFLICT / ALTER TABLE
# semantics that the in-memory unit tests (which hand-build the frozen row as
# a dict) cannot exercise. The design marks this test mandatory.


def _old_binary_upsert_claim(engine, *, service_name, pool_key, heartbeat_ts):
    """Emulate a pre-gate binary's claim upsert.

    Its values dict omits demonstrated_need / boot_hold / activity_ts, so the
    ON CONFLICT DO UPDATE set_ (which iterates that dict) updates only the
    columns that binary knows and leaves the three gate columns untouched --
    exactly the frozen-signal skew the gate must reject.
    """
    table = serve_state.reserved_fill_claims_table
    values = {
        'service_name': service_name,
        'pool_key': pool_key,
        'weight': 1.0,
        'floor_replicas': 0,
        'gpus_per_replica': 1,
        'holdings_fill': 40,
        'effective_cap': None,
        'launchable': 1,
        'heartbeat_ts': heartbeat_ts,
    }
    insert_stmt = serve_state._upsert_insert_func(engine)(table).values(
        **values)
    insert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=['service_name'],
        set_={
            key: getattr(insert_stmt.excluded, key)
            for key in values
            if key != 'service_name'
        })
    with engine.begin() as connection:
        connection.execute(insert_stmt)


@pytest.mark.usefixtures('_broker_db')
class TestUtilizationGateSkewPG:

    def test_old_binary_upsert_freezes_signal_so_the_round_reads_blind(self):
        engine = serve_state._db_manager.get_engine()
        pool_key = 'skew-pool'
        # New binary writes a PAIRED idle signal (need 0) at t=1000: fresh,
        # so a real service WOULD be gated and decayed on it.
        serve_state.upsert_reserved_fill_claim(service_name='svc',
                                               pool_key=pool_key,
                                               weight=1.0,
                                               floor_replicas=0,
                                               gpus_per_replica=1,
                                               holdings_fill=40,
                                               effective_cap=None,
                                               launchable=True,
                                               heartbeat_ts=1000.0,
                                               demonstrated_need=0,
                                               boot_hold=False,
                                               activity_ts=1000.0)
        fresh = {
            row['service_name']: row
            for row in serve_state.get_reserved_fill_claims(pool_key)
        }['svc']
        fresh_signal = broker._activity_input(fresh)
        assert fresh_signal.armed is True
        assert fresh_signal.blind is False

        # A pre-gate binary heartbeats the SAME row 61s later, omitting the
        # gate columns. Their ON CONFLICT set_ leaves them frozen.
        _old_binary_upsert_claim(engine,
                                 service_name='svc',
                                 pool_key=pool_key,
                                 heartbeat_ts=1061.0)
        row = {
            r['service_name']: r
            for r in serve_state.get_reserved_fill_claims(pool_key)
        }['svc']
        assert row['heartbeat_ts'] == 1061.0
        assert row['activity_ts'] == 1000.0  # FROZEN, not refreshed to 1061
        assert row['demonstrated_need'] == 0  # FROZEN
        # lag 61 > RESERVED_FILL_ACTIVITY_MAX_LAG_SECONDS (60) ->
        # armed-but-blind, so a frozen zero first gets blind grace rather
        # than being trusted as confirmed idle.
        stale_signal = broker._activity_input(row)
        assert stale_signal.armed is True
        assert stale_signal.blind is True


class TestMigration030PopulatedClaimsPG:

    def test_migration_030_on_populated_pre_030_claims_reads_ungated(
            self, pg_server):
        # Stand a DB up at revision 029, populate a claim (no gate columns),
        # then upgrade to 030: the row must gain NULL gate columns and read as
        # ungated -- today's exact behavior for an already-live claimant.
        url = _create_database(pg_server, f'skew030_{uuid.uuid4().hex[:8]}')
        engine = create_engine(url)
        try:
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 '029')
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.text(
                        'INSERT INTO reserved_fill_claims (service_name, '
                        'pool_key, weight, floor_replicas, gpus_per_replica, '
                        'holdings_fill, launchable, heartbeat_ts) VALUES '
                        "('legacy', 'p', 1.0, 0, 1, 40, 1, 1000.0)"))
            migration_utils.safe_alembic_upgrade(engine,
                                                 migration_utils.SERVE_DB_NAME,
                                                 migration_utils.SERVE_VERSION)
            inspector = sqlalchemy.inspect(engine)
            claim_cols = {
                column['name']
                for column in inspector.get_columns('reserved_fill_claims')
            }
            round_cols = {
                column['name']
                for column in inspector.get_columns('reserved_fill_rounds')
            }
            assert {'demonstrated_need', 'boot_hold',
                    'activity_ts'} <= claim_cols, claim_cols
            assert 'utilization_state' in round_cols, round_cols
            with engine.connect() as connection:
                got = connection.execute(
                    sqlalchemy.text(
                        'SELECT demonstrated_need, boot_hold, activity_ts, '
                        'heartbeat_ts FROM reserved_fill_claims WHERE '
                        "service_name = 'legacy'")).mappings().one()
            assert got['demonstrated_need'] is None
            assert got['boot_hold'] is None
            assert got['activity_ts'] is None
            legacy_signal = broker._activity_input(dict(got))
            assert legacy_signal.armed is False
            assert legacy_signal.blind is True
        finally:
            engine.dispose()
