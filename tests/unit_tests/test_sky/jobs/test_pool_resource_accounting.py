"""Tests for grouped pool resource accounting in managed jobs state."""

# pylint: disable=protected-access,redefined-outer-name

import contextlib

import filelock
import pytest
from sqlalchemy import create_engine
from sqlalchemy import orm
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
    yield engine


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
