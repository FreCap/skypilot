"""Tests for grouped pool resource accounting in managed jobs state."""

# pylint: disable=protected-access,redefined-outer-name

import asyncio
import contextlib
import shutil

import filelock
import pytest
from sqlalchemy import create_engine
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import create_async_engine

from sky.jobs import state
from sky.jobs.state import ManagedJobStatus
from sky.resources import Resources


@pytest.fixture
def managed_jobs_db(tmp_path, monkeypatch):
    """Isolated SQLite DB for pool-resource accounting tests."""
    db_path = tmp_path / 'managed_jobs_testing.db'
    engine = create_engine(f'sqlite:///{db_path}')
    async_engine = create_async_engine(
        f'sqlite+aiosqlite:///{db_path}',
        connect_args={'timeout': 30},
    )

    @contextlib.contextmanager
    def _tmp_db_lock(section_name: str):
        lock_path = tmp_path / f'.{section_name}.lock'
        with filelock.FileLock(str(lock_path), timeout=10):
            yield

    monkeypatch.setattr(state.migration_utils, 'db_lock', _tmp_db_lock)
    monkeypatch.setattr(state._db_manager, '_engine', engine)
    monkeypatch.setattr(state._db_manager, '_engine_async', async_engine)

    state.create_table(engine)
    try:
        yield engine
    finally:
        asyncio.run(async_engine.dispose())
        engine.dispose()


@pytest.fixture(scope='module')
def postgres_engine():
    """Isolated PostgreSQL 16 DB for the JSON query regression."""
    if shutil.which('docker') is None:
        pytest.skip('docker unavailable; skipping real-PostgreSQL test')
    testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
    pytest.importorskip('psycopg2')
    container = testcontainers_postgres.PostgresContainer('postgres:16')
    try:
        container.start()
    except Exception as error:  # pylint: disable=broad-except
        pytest.skip(f'could not start PostgreSQL container: {error}')
    engine = create_engine(container.get_connection_url())
    state.Base.metadata.create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()
        container.stop()


def _insert_task(engine, job_id: int, task_id: int, *, status: ManagedJobStatus,
                 full_resources):
    with orm.Session(engine) as session:
        session.execute(
            state.sqlalchemy.insert(state.spot_table).values(
                spot_job_id=job_id,
                task_id=task_id,
                task_name=f'task-{task_id}',
                status=status.value,
                full_resources=full_resources,
            ))
        session.commit()


def _new_pool_job(engine, *, pool: str, status: ManagedJobStatus,
                  full_resources, cluster_name: str) -> int:
    job_id = state.set_job_info_without_job_id(name=f'job-{pool}',
                                               workspace='ws',
                                               entrypoint='entry',
                                               pool=pool,
                                               pool_hash=None,
                                               user_hash='u')
    _insert_task(engine,
                 job_id,
                 0,
                 status=status,
                 full_resources=full_resources)
    state.set_current_cluster_name(job_id, cluster_name)
    return job_id


def test_grouped_pool_resource_accounting_fails_closed_on_empty_job(
        managed_jobs_db):
    """Any empty resource request makes that worker unavailable to fit logic."""
    _new_pool_job(managed_jobs_db,
                  pool='pool-a',
                  status=ManagedJobStatus.RUNNING,
                  full_resources=Resources().to_yaml_config(),
                  cluster_name='replica-1')
    _new_pool_job(managed_jobs_db,
                  pool='pool-a',
                  status=ManagedJobStatus.RUNNING,
                  full_resources=Resources(cpus='2').to_yaml_config(),
                  cluster_name='replica-1')
    replica_2_job = _new_pool_job(
        managed_jobs_db,
        pool='pool-a',
        status=ManagedJobStatus.RUNNING,
        full_resources=Resources(cpus='1').to_yaml_config(),
        cluster_name='replica-2')
    _insert_task(managed_jobs_db,
                 replica_2_job,
                 1,
                 status=ManagedJobStatus.SUCCEEDED,
                 full_resources=Resources(cpus='1').to_yaml_config())
    _new_pool_job(managed_jobs_db,
                  pool='pool-b',
                  status=ManagedJobStatus.RUNNING,
                  full_resources=Resources(cpus='9').to_yaml_config(),
                  cluster_name='replica-3')

    grouped = state.get_pool_worker_used_resources_by_cluster('pool-a')
    replica_2_ids = set(
        state.get_nonterminal_job_ids_by_pool('pool-a', 'replica-2'))
    replica_2_resources = state.get_pool_worker_used_resources(replica_2_ids)

    assert grouped is not None
    assert grouped['replica-1'].is_empty()
    assert float(grouped['replica-2'].cpus) == pytest.approx(1.0)
    assert replica_2_resources is not None
    assert float(replica_2_resources.cpus) == pytest.approx(1.0)


def test_pool_worker_used_resources_ignores_terminal_task_history(
        managed_jobs_db):
    """Only the current nonterminal task should count for a job."""
    job_id = state.set_job_info_without_job_id(name='job-pool-a',
                                               workspace='ws',
                                               entrypoint='entry',
                                               pool='pool-a',
                                               pool_hash=None,
                                               user_hash='u')
    _insert_task(managed_jobs_db,
                 job_id,
                 0,
                 status=ManagedJobStatus.SUCCEEDED,
                 full_resources=Resources(cpus='1').to_yaml_config())
    _insert_task(managed_jobs_db,
                 job_id,
                 1,
                 status=ManagedJobStatus.RUNNING,
                 full_resources=Resources(cpus='2').to_yaml_config())
    state.set_current_cluster_name(job_id, 'replica-1')

    grouped = state.get_pool_worker_used_resources_by_cluster('pool-a')
    replica_1_ids = set(
        state.get_nonterminal_job_ids_by_pool('pool-a', 'replica-1'))
    replica_1_resources = state.get_pool_worker_used_resources(replica_1_ids)

    assert grouped is not None
    assert float(grouped['replica-1'].cpus) == pytest.approx(2.0)
    assert replica_1_resources is not None
    assert float(replica_1_resources.cpus) == pytest.approx(2.0)


def test_pool_worker_used_resources_counts_one_nonterminal_task_per_job(
        managed_jobs_db):
    """Serial jobs account for their lowest current nonterminal task only."""
    job_id = state.set_job_info_without_job_id(name='job-pool-a',
                                               workspace='ws',
                                               entrypoint='entry',
                                               pool='pool-a',
                                               pool_hash=None,
                                               user_hash='u')
    _insert_task(managed_jobs_db,
                 job_id,
                 0,
                 status=ManagedJobStatus.RUNNING,
                 full_resources=Resources(cpus='2').to_yaml_config())
    _insert_task(managed_jobs_db,
                 job_id,
                 1,
                 status=ManagedJobStatus.PENDING,
                 full_resources=Resources(cpus='7').to_yaml_config())
    state.set_current_cluster_name(job_id, 'replica-1')

    grouped = state.get_pool_worker_used_resources_by_cluster('pool-a')
    replica_1_resources = state.get_pool_worker_used_resources({job_id})

    assert grouped is not None
    assert float(grouped['replica-1'].cpus) == pytest.approx(2.0)
    assert replica_1_resources is not None
    assert float(replica_1_resources.cpus) == pytest.approx(2.0)


def test_pool_worker_resource_query_does_not_distinct_postgres_json():
    """The PostgreSQL query ranks scalar identity, never the JSON payload."""
    ranked = state._ranked_nonterminal_job_resources(pool='pool-a')
    query = state.sqlalchemy.select(
        ranked.c.current_cluster_name,
        ranked.c.spot_job_id,
        ranked.c.full_resources,
    ).where(ranked.c.task_rank == 1)

    sql = str(query.compile(dialect=postgresql.dialect()))

    assert 'DISTINCT' not in sql.upper()
    assert 'row_number() OVER' in sql
    assert 'full_resources' in sql


def test_pool_worker_resource_accounting_executes_with_postgres_json(
        postgres_engine, monkeypatch):
    """The production PostgreSQL JSON type is accepted by both query paths."""
    monkeypatch.setattr(state._db_manager, '_engine', postgres_engine)
    job_id = _new_pool_job(
        postgres_engine,
        pool='postgres-pool',
        status=ManagedJobStatus.RUNNING,
        full_resources=Resources(cpus='3').to_yaml_config(),
        cluster_name='postgres-replica',
    )

    grouped = state.get_pool_worker_used_resources_by_cluster('postgres-pool')
    worker = state.get_pool_worker_used_resources({job_id})

    assert grouped is not None
    assert float(grouped['postgres-replica'].cpus) == pytest.approx(3.0)
    assert worker is not None
    assert float(worker.cpus) == pytest.approx(3.0)
