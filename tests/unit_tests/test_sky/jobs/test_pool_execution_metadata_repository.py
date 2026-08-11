"""Characterization tests for managed-job pool execution metadata."""

import asyncio
import contextlib
import inspect
import json
import pickle
from typing import Any

import filelock
import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import create_async_engine

from sky.jobs import state
from sky.jobs.status_types import ManagedJobStatus


@pytest.fixture
def managed_jobs_db(tmp_path, monkeypatch):
    """Provide one isolated database to sync and async state functions."""
    db_path = tmp_path / 'managed_jobs.db'
    engine = create_engine(f'sqlite:///{db_path}')
    async_engine = create_async_engine(f'sqlite+aiosqlite:///{db_path}',
                                        connect_args={'timeout': 30})

    @contextlib.contextmanager
    def _tmp_db_lock(_section: str):
        lock_path = tmp_path / f'.{_section}.lock'
        with filelock.FileLock(str(lock_path), timeout=10):
            yield

    monkeypatch.setattr(state.migration_utils, 'db_lock', _tmp_db_lock)
    monkeypatch.setattr(state._db_manager, '_engine', engine)
    monkeypatch.setattr(state._db_manager, '_engine_async', async_engine)
    state.create_table(engine)
    try:
        yield engine, async_engine
    finally:
        asyncio.run(async_engine.dispose())
        engine.dispose()


@contextlib.contextmanager
def _count_sql_statements(engine):
    counts = {'n': 0}

    def _count(*_args, **_kwargs):
        counts['n'] += 1

    event.listen(engine, 'before_cursor_execute', _count)
    try:
        yield counts
    finally:
        event.remove(engine, 'before_cursor_execute', _count)


def _new_pool_job(engine,
                  *,
                  pool: str = 'pool-a',
                  execution: str = 'parallel') -> int:
    job_id = state.set_job_info_without_job_id(
        name='job',
        workspace='workspace',
        entrypoint='echo hi',
        pool=pool,
        pool_hash='pool-hash',
        user_hash='user-hash',
    )
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(state.job_info_table).where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    execution=execution))
        session.execute(
            sqlalchemy.insert(state.spot_table).values(
                spot_job_id=job_id,
                task_id=0,
                task_name='task',
                status=ManagedJobStatus.PENDING.value,
            ))
        session.commit()
    return job_id


def test_pool_execution_metadata_public_contract():
    expected_signatures = {
        'get_pool_from_job_id': '(job_id: int) -> str | None',
        'get_pool_and_current_cluster_name':
            '(job_id: int) -> tuple[str | None, str | None]',
        'get_pool_and_execution_from_job_id_async':
            '(job_id: int) -> tuple[str | None, str | None]',
        'set_current_cluster_name':
            '(job_id: int, current_cluster_name: str) -> None',
        'set_job_infra':
            '(job_id: int, cloud: str | None = None, region: str | None = '
            'None, zone: str | None = None, current_node_names: list[str] | '
            'None = None) -> None',
        'update_job_full_resources':
            '(job_id: int, full_resources_json: dict[str, typing.Any]) -> None',
        'set_job_id_on_pool_cluster_async':
            '(job_id: int, job_id_on_pool_cluster: int) -> None',
        'get_pool_submit_info':
            '(job_id: int) -> tuple[str | None, int | None]',
        'get_pool_submit_info_async':
            '(job_id: int) -> tuple[str | None, int | None]',
    }
    for name, expected_signature in expected_signatures.items():
        function = getattr(state, name)
        assert function.__name__ == name
        assert function.__module__ == 'sky.jobs.state'
        assert str(inspect.signature(function)) == expected_signature
        assert pickle.loads(pickle.dumps(function)) is function

    decorated = (
        state.get_pool_from_job_id,
        state.get_pool_and_current_cluster_name,
        state.get_pool_and_execution_from_job_id_async,
        state.set_job_infra,
        state.get_pool_submit_info,
        state.get_pool_submit_info_async,
    )
    for function in decorated:
        assert function.__wrapped__.__name__ == function.__name__
        assert function.__wrapped__.__module__ == 'sky.jobs.state'


@pytest.mark.asyncio
async def test_pool_execution_metadata_round_trip_and_query_budgets(
        managed_jobs_db):
    engine, async_engine = managed_jobs_db
    job_id = _new_pool_job(engine)

    with _count_sql_statements(engine) as counts:
        assert state.get_pool_from_job_id(job_id) == 'pool-a'
    assert counts['n'] == 1

    with _count_sql_statements(engine) as counts:
        assert state.get_pool_and_current_cluster_name(job_id) == ('pool-a',
                                                                  None)
    assert counts['n'] == 1

    with _count_sql_statements(async_engine.sync_engine) as counts:
        assert await state.get_pool_and_execution_from_job_id_async(
            job_id) == ('pool-a', 'parallel')
    assert counts['n'] == 1

    with _count_sql_statements(engine) as counts:
        state.set_current_cluster_name(job_id, 'replica-a')
    assert counts['n'] == 1

    with _count_sql_statements(engine) as counts:
        state.set_job_infra(job_id,
                            cloud='AWS',
                            region='us-east-1',
                            zone='us-east-1a',
                            current_node_names=['head-a', 'worker-a'])
    assert counts['n'] == 2

    with _count_sql_statements(engine) as counts:
        state.set_job_infra(job_id,
                            current_node_names=['head-b', 'worker-a'])
    assert counts['n'] == 2

    resources: dict[str, Any] = {'cloud': 'aws', 'accelerators': {'A10G': 1}}
    with _count_sql_statements(engine) as counts:
        state.update_job_full_resources(job_id, resources)
    assert counts['n'] == 1

    with _count_sql_statements(async_engine.sync_engine) as counts:
        await state.set_job_id_on_pool_cluster_async(job_id, 47)
    assert counts['n'] == 1

    with _count_sql_statements(engine) as counts:
        assert state.get_pool_submit_info(job_id) == ('replica-a', 47)
    assert counts['n'] == 1

    with _count_sql_statements(async_engine.sync_engine) as counts:
        assert await state.get_pool_submit_info_async(job_id) == ('replica-a',
                                                                  47)
    assert counts['n'] == 1

    with orm.Session(engine) as session:
        job_row = session.execute(
            sqlalchemy.select(
                state.job_info_table.c.cloud,
                state.job_info_table.c.region,
                state.job_info_table.c.zone,
                state.job_info_table.c.node_names,
            ).where(state.job_info_table.c.spot_job_id == job_id)).one()
        full_resources = session.execute(
            sqlalchemy.select(state.spot_table.c.full_resources).where(
                state.spot_table.c.spot_job_id == job_id,
                state.spot_table.c.task_id == 0)).scalar_one()

    assert job_row[:3] == ('AWS', 'us-east-1', 'us-east-1a')
    assert json.loads(job_row.node_names) == [['head-a', 'head-b'],
                                             ['worker-a']]
    assert full_resources == resources


@pytest.mark.asyncio
async def test_pool_execution_metadata_missing_rows_and_noop_update(
        managed_jobs_db):
    engine, async_engine = managed_jobs_db
    missing_job_id = 999_999

    with _count_sql_statements(engine) as counts:
        state.set_job_infra(missing_job_id)
    assert counts['n'] == 0

    assert state.get_pool_from_job_id(missing_job_id) is None
    assert state.get_pool_and_current_cluster_name(missing_job_id) == (None,
                                                                      None)
    assert await state.get_pool_and_execution_from_job_id_async(
        missing_job_id) == (None, None)
    assert state.get_pool_submit_info(missing_job_id) == (None, None)

    with _count_sql_statements(async_engine.sync_engine) as counts:
        assert await state.get_pool_submit_info_async(missing_job_id) == (None,
                                                                         None)
    assert counts['n'] == 1
