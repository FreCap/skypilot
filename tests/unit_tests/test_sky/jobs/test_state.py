"""Unit tests for sky.jobs.state."""
# pylint: disable=protected-access

import asyncio
import contextlib
import time
from typing import Optional

import filelock
import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import orm
from sqlalchemy.ext.asyncio import create_async_engine

from sky.jobs import state
from sky.jobs.state import ManagedJobStatus


@pytest.fixture
def _mock_managed_jobs_db_conn(tmp_path, monkeypatch):
    """Isolated SQLite DB for sky.jobs.state (sync + async engines)."""
    db_path = tmp_path / 'managed_jobs_testing.db'
    engine = create_engine(f'sqlite:///{db_path}')
    async_engine = create_async_engine(
        f'sqlite+aiosqlite:///{db_path}',
        connect_args={'timeout': 30},
    )

    @contextlib.contextmanager
    def _tmp_db_lock(_section: str):
        lock_path = tmp_path / f'.{_section}.lock'
        with filelock.FileLock(str(lock_path), timeout=10):
            yield

    monkeypatch.setattr(state.migration_utils, 'db_lock', _tmp_db_lock)
    monkeypatch.setattr(state._db_manager, '_engine', engine)
    monkeypatch.setattr(state._db_manager, '_engine_async', async_engine)

    # Create schema via migrations
    state.create_table(engine)
    try:
        yield engine
    finally:
        asyncio.run(async_engine.dispose())
        engine.dispose()


def _insert_task(
    engine,
    job_id: int,
    task_id: int,
    *,
    status: ManagedJobStatus,
    end_at: Optional[float] = None,
    local_log_file: Optional[str] = None,
    logs_cleaned_at: Optional[float] = None,
):
    with orm.Session(engine) as session:
        session.execute(
            state.sqlalchemy.insert(state.spot_table).values(
                spot_job_id=job_id,
                task_id=task_id,
                task_name=f'task-{task_id}',
                status=status.value,
                end_at=end_at,
                local_log_file=local_log_file,
                logs_cleaned_at=logs_cleaned_at,
            ))
        session.commit()


def _insert_job_info(engine,
                     *,
                     controller_logs_cleaned_at: Optional[float] = None):
    with orm.Session(engine) as session:
        # Insert row; let PK autoincrement.
        engine = state._db_manager.get_engine()
        if (engine.dialect.name == state.db_utils.SQLAlchemyDialect.SQLITE.value
           ):
            insert_func = state.sqlite.insert
        elif (engine.dialect.name ==
              state.db_utils.SQLAlchemyDialect.POSTGRESQL.value):
            insert_func = state.postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')

        insert_stmt = insert_func(state.job_info_table).values(
            name='job',
            schedule_state=state.ManagedJobScheduleState.INACTIVE.value,
            controller_logs_cleaned_at=controller_logs_cleaned_at,
        )
        result = session.execute(insert_stmt)
        # SQLite: lastrowid holds PK
        job_id = result.lastrowid
        session.commit()
        return job_id


@contextlib.contextmanager
def _count_sql_statements(engine):
    counts = {'n': 0}

    def _count(*args, **kwargs):
        del args, kwargs
        counts['n'] += 1

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _count)
    try:
        yield counts
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute', _count)


def _set_controller_process(engine, job_id, pid, started_at):
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(state.job_info_table).where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    controller_pid=pid,
                    controller_pid_started_at=started_at,
                ))
        session.commit()


def _get_api_access_token_rows(engine):
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(state.api_access_token_table).order_by(
                state.api_access_token_table.c.job_id)).fetchall()
    return [(row.job_id, row.token_id) for row in rows]


def test_get_job_event_task_contexts_uses_one_slim_query(
        _mock_managed_jobs_db_conn, monkeypatch):
    engine = _mock_managed_jobs_db_conn
    _insert_task(engine, 7, 0, status=ManagedJobStatus.PENDING)
    _insert_task(engine, 7, 1, status=ManagedJobStatus.RUNNING)
    with engine.begin() as connection:
        connection.execute(state.job_info_table.insert().values(
            spot_job_id=7,
            name='job-name',
            schedule_state=state.ManagedJobScheduleState.INACTIVE.value,
            pool='pool-a',
            dag_yaml_content='large-yaml',
            original_user_yaml_path='/tmp/should-not-be-read.yaml',
        ))

    monkeypatch.setattr(
        'builtins.open', lambda *args, **kwargs:
        (_ for _ in
         ()).throw(AssertionError('user yaml path should not be opened')))
    statements = []

    def _capture(conn, cursor, statement, parameters, context, executemany):
        del conn, cursor, parameters, context, executemany
        statements.append(statement.lower())

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _capture)
    try:
        task_contexts = state.get_job_event_task_contexts(7)
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute', _capture)

    assert task_contexts == [
        {
            'task_id': 0,
            'task_name': 'task-0',
            'pool': 'pool-a',
        },
        {
            'task_id': 1,
            'task_name': 'task-1',
            'pool': 'pool-a',
        },
    ]
    assert len(statements) == 1, statements
    sql = statements[0]
    assert 'metadata' not in sql
    assert 'dag_yaml_content' not in sql
    assert 'original_user_yaml_content' not in sql
    assert 'original_user_yaml_path' not in sql


def test_get_job_event_task_contexts_keeps_jobs_without_job_info(
        _mock_managed_jobs_db_conn):
    """A managed job with no ``job_info`` row still resolves its tasks.

    The LEFT OUTER JOIN is load-bearing: managed jobs created before the
    ``job_info`` table existed have a ``spot`` row and no ``job_info`` row.
    Narrowing it to an inner join drops those tasks, and because
    ``get_job_events`` merges cluster events best-effort -- it swallows every
    exception and returns the unmerged timeline -- the loss is silent. Pin that
    the row survives and that a missing ``job_info`` reads as "no pool" rather
    than as an excluded task.
    """
    engine = _mock_managed_jobs_db_conn
    _insert_task(engine, 11, 0, status=ManagedJobStatus.RUNNING)

    assert state.get_job_event_task_contexts(11) == [{
        'task_id': 0,
        'task_name': 'task-0',
        'pool': None,
    }]


@pytest.mark.asyncio
async def test_image_recovery_generation_tracks_durable_recovery_epoch(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    _insert_task(engine, 42, 0, status=ManagedJobStatus.RUNNING)
    with engine.begin() as connection:
        connection.execute(state.spot_table.update().where(
            state.spot_table.c.spot_job_id == 42,
            state.spot_table.c.task_id == 0).values(recovery_count=2))

    assert await state.get_image_recovery_generation_async(42, 0) == 2
    with engine.begin() as connection:
        connection.execute(state.spot_table.update().where(
            state.spot_table.c.spot_job_id == 42,
            state.spot_table.c.task_id == 0).values(
                status=ManagedJobStatus.RECOVERING.value))
    assert await state.get_image_recovery_generation_async(42, 0) == 3
    with engine.begin() as connection:
        connection.execute(state.spot_table.update().where(
            state.spot_table.c.spot_job_id == 42,
            state.spot_table.c.task_id == 0).values(
                status=ManagedJobStatus.RUNNING.value, recovery_count=3))
    assert await state.get_image_recovery_generation_async(42, 0) == 3
    with pytest.raises(ValueError, match='does not exist'):
        await state.get_image_recovery_generation_async(404, 0)


def test_job_task_terminal_lookup_uses_only_exact_index_keys(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    with engine.begin() as connection:
        connection.execute(state.spot_table.insert(), [{
            'spot_job_id': 42,
            'task_id': task_id,
            'task_name': f'task-{task_id}',
            'status': (ManagedJobStatus.SUCCEEDED.value
                       if task_id == 1999 else ManagedJobStatus.RUNNING.value),
        } for task_id in range(2000)])
    statements = []

    def record(_connection, _cursor, statement, _parameters, _context,
               _executemany):
        statements.append(statement)

    sqlalchemy.event.listen(engine, 'before_cursor_execute', record)
    try:
        result = state.get_job_task_terminal_states([(42, 1999), (42, 3000)])
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute', record)

    assert result == {(42, 1999): True}
    assert len(statements) == 1
    assert '(spot.spot_job_id, spot.task_id) IN (VALUES' in statements[0]
    indexes = {
        index['name']: index['column_names']
        for index in sqlalchemy.inspect(engine).get_indexes('spot')
    }
    assert indexes['ix_spot_job_task'] == ['spot_job_id', 'task_id']


@pytest.mark.asyncio
async def test_get_statuses_async_batches_latest_task_semantics(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    _insert_task(engine, 1, 0, status=ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, 1, 1, status=ManagedJobStatus.RUNNING)
    _insert_task(engine, 1, 2, status=ManagedJobStatus.PENDING)
    _insert_task(engine, 2, 0, status=ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, 2, 1, status=ManagedJobStatus.FAILED)

    async_engine = state._db_manager._engine_async
    assert async_engine is not None
    with _count_sql_statements(async_engine.sync_engine) as counts:
        statuses = await state.get_statuses_async([2, 1, 3, 1])

    assert counts['n'] == 1, counts
    assert statuses == {
        2: ManagedJobStatus.FAILED,
        1: ManagedJobStatus.RUNNING,
        3: None,
    }


@pytest.mark.asyncio
async def test_get_statuses_async_bounds_chunk_queries_and_empty_input(
        _mock_managed_jobs_db_conn, monkeypatch):
    engine = _mock_managed_jobs_db_conn
    for job_id in range(1, 6):
        _insert_task(engine, job_id, 0, status=ManagedJobStatus.SUCCEEDED)
    monkeypatch.setattr(state, '_STATUS_CHECK_JOB_ID_CHUNK', 2)

    async_engine = state._db_manager._engine_async
    assert async_engine is not None
    with _count_sql_statements(async_engine.sync_engine) as counts:
        statuses = await state.get_statuses_async([1, 2, 3, 4, 5, 1])

    assert counts['n'] == 3, counts
    assert statuses == {
        job_id: ManagedJobStatus.SUCCEEDED for job_id in range(1, 6)
    }

    with _count_sql_statements(async_engine.sync_engine) as counts:
        assert await state.get_statuses_async([]) == {}
    assert counts['n'] == 0, counts


@pytest.mark.asyncio
async def test_get_statuses_async_materializes_one_row_per_job(
        _mock_managed_jobs_db_conn, monkeypatch):
    engine = _mock_managed_jobs_db_conn
    for task_id in range(1000):
        _insert_task(engine,
                     11,
                     task_id,
                     status=(ManagedJobStatus.RUNNING
                             if task_id == 999 else ManagedJobStatus.SUCCEEDED))
    for task_id in range(800):
        _insert_task(engine, 12, task_id, status=ManagedJobStatus.SUCCEEDED)

    observed_row_counts = []
    original_execute = state.sql_async.AsyncSession.execute

    async def _record_fetchall(self, *args, **kwargs):
        result = await original_execute(self, *args, **kwargs)

        class _ResultProxy:
            """Record the fetched row count without changing result behavior."""

            def __init__(self, inner):
                self._inner = inner

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def fetchall(self):
                rows = self._inner.fetchall()
                observed_row_counts.append(len(rows))
                return rows

        return _ResultProxy(result)

    monkeypatch.setattr(state.sql_async.AsyncSession, 'execute',
                        _record_fetchall)

    statuses = await state.get_statuses_async([11, 12])

    assert statuses == {
        11: ManagedJobStatus.RUNNING,
        12: ManagedJobStatus.SUCCEEDED,
    }
    assert observed_row_counts == [2], observed_row_counts


@pytest.mark.asyncio
async def test_latest_task_status_queries_preserve_duplicate_nonterminal(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    with engine.begin() as connection:
        connection.execute(state.spot_table.insert(), [{
            'spot_job_id': 1,
            'task_id': 0,
            'task_name': 'active-first',
            'status': ManagedJobStatus.PENDING.value,
        }, {
            'spot_job_id': 1,
            'task_id': 0,
            'task_name': 'terminal-last',
            'status': ManagedJobStatus.SUCCEEDED.value,
        }, {
            'spot_job_id': 2,
            'task_id': 0,
            'task_name': 'terminal-first',
            'status': ManagedJobStatus.FAILED.value,
        }, {
            'spot_job_id': 2,
            'task_id': 0,
            'task_name': 'active-last',
            'status': ManagedJobStatus.RECOVERING.value,
        }])

    expected = {
        1: ManagedJobStatus.PENDING,
        2: ManagedJobStatus.RECOVERING,
    }
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    with orm.Session(engine) as session:
        selected_rows = session.execute(
            state._latest_task_status_query([1, 2],
                                            terminal_status_values)).fetchall()
    assert len(selected_rows) == 2
    assert state.get_latest_task_id_status(1) == (0, expected[1])
    assert state.get_latest_task_id_status(2) == (0, expected[2])
    assert await state.get_latest_task_id_status_async(1) == (0, expected[1])
    assert await state.get_latest_task_id_status_async(2) == (0, expected[2])
    assert await state.get_statuses_async([1, 2]) == expected


def test_get_latest_task_id_status_uses_one_latest_row(
        _mock_managed_jobs_db_conn, monkeypatch):
    engine = _mock_managed_jobs_db_conn
    _insert_task(engine, 1, 0, status=ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, 1, 1, status=ManagedJobStatus.RUNNING)
    _insert_task(engine, 1, 2, status=ManagedJobStatus.PENDING)
    _insert_task(engine, 2, 0, status=ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, 2, 1, status=ManagedJobStatus.FAILED)

    monkeypatch.setattr(
        state, '_get_all_task_ids_statuses', lambda *_args, **_kwargs:
        (_ for _ in
         ()).throw(AssertionError('full task snapshot must not run')))

    observed_rows = []
    original_execute = state.orm.Session.execute

    def _record_fetchone(self, *args, **kwargs):
        result = original_execute(self, *args, **kwargs)

        class _ResultProxy:
            """Capture bounded latest-task row reads."""

            def __init__(self, inner):
                self._inner = inner

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def fetchone(self):
                row = self._inner.fetchone()
                observed_rows.append(0 if row is None else 1)
                return row

            def fetchall(self):
                raise AssertionError('latest-task lookup must stay bounded')

        return _ResultProxy(result)

    monkeypatch.setattr(state.orm.Session, 'execute', _record_fetchone)

    assert state.get_latest_task_id_status(1) == (1, ManagedJobStatus.RUNNING)
    assert state.get_latest_task_id_status(2) == (1, ManagedJobStatus.FAILED)
    assert state.get_latest_task_id_status(3) == (None, None)
    assert observed_rows == [1, 1, 0], observed_rows


@pytest.mark.asyncio
async def test_get_latest_task_id_status_async_uses_one_latest_row(
        _mock_managed_jobs_db_conn, monkeypatch):
    engine = _mock_managed_jobs_db_conn
    _insert_task(engine, 1, 0, status=ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, 1, 1, status=ManagedJobStatus.RUNNING)
    _insert_task(engine, 1, 2, status=ManagedJobStatus.PENDING)
    _insert_task(engine, 2, 0, status=ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, 2, 1, status=ManagedJobStatus.FAILED)

    async def _no_full_snapshot(*_args, **_kwargs):
        raise AssertionError('full task snapshot must not run')

    monkeypatch.setattr(state, 'get_all_task_ids_statuses_async',
                        _no_full_snapshot)

    observed_rows = []
    original_execute = state.sql_async.AsyncSession.execute

    async def _record_fetchone(self, *args, **kwargs):
        result = await original_execute(self, *args, **kwargs)

        class _ResultProxy:
            """Capture bounded latest-task row reads."""

            def __init__(self, inner):
                self._inner = inner

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def fetchone(self):
                row = self._inner.fetchone()
                observed_rows.append(0 if row is None else 1)
                return row

            def fetchall(self):
                raise AssertionError('latest-task lookup must stay bounded')

        return _ResultProxy(result)

    monkeypatch.setattr(state.sql_async.AsyncSession, 'execute',
                        _record_fetchone)

    assert await state.get_latest_task_id_status_async(1) == (
        1, ManagedJobStatus.RUNNING)
    assert await state.get_latest_task_id_status_async(2) == (
        1, ManagedJobStatus.FAILED)
    assert await state.get_latest_task_id_status_async(3) == (None, None)
    assert observed_rows == [1, 1, 0], observed_rows


@pytest.mark.asyncio
async def test_get_latest_task_id_status_async_retries_transient_db_error(
        _mock_managed_jobs_db_conn, monkeypatch):
    engine = _mock_managed_jobs_db_conn
    _insert_task(engine, 1, 0, status=ManagedJobStatus.RUNNING)

    attempts = 0
    original_execute = state.sql_async.AsyncSession.execute

    async def _fail_once(self, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sqlalchemy.exc.OperationalError(
                statement='SELECT latest task',
                params={},
                orig=ConnectionError('transient disconnect'),
            )
        return await original_execute(self, *args, **kwargs)

    async def _no_backoff(_delay):
        return None

    monkeypatch.setattr(state.sql_async.AsyncSession, 'execute', _fail_once)
    monkeypatch.setattr(state.db_retries.asyncio, 'sleep', _no_backoff)

    assert await state.get_latest_task_id_status_async(1) == (
        0, ManagedJobStatus.RUNNING)
    assert attempts == 2


def test_set_api_access_token_ids_batches_upserts(_mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn

    with _count_sql_statements(engine) as counts:
        state.set_api_access_token_ids([3, 1, 2, 2], 'shared-token')

    assert counts['n'] == 1, counts
    assert _get_api_access_token_rows(engine) == [
        (1, 'shared-token'),
        (2, 'shared-token'),
        (3, 'shared-token'),
    ]

    with _count_sql_statements(engine) as counts:
        state.set_api_access_token_ids([1, 3], 'replacement-token')

    assert counts['n'] == 1, counts
    assert _get_api_access_token_rows(engine) == [
        (1, 'replacement-token'),
        (2, 'shared-token'),
        (3, 'replacement-token'),
    ]


def test_set_api_access_token_ids_empty_input_uses_no_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn

    with _count_sql_statements(engine) as counts:
        state.set_api_access_token_ids([], 'unused-token')

    assert counts['n'] == 0, counts
    assert _get_api_access_token_rows(engine) == []


def test_set_api_access_token_ids_rolls_back_entire_batch(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'CREATE TRIGGER reject_later_token_chunk '
            'BEFORE INSERT ON api_access_tokens '
            'WHEN NEW.job_id = 1001 '
            "BEGIN SELECT RAISE(ABORT, 'rejected token'); END")

    with pytest.raises(sqlalchemy.exc.DBAPIError, match='rejected token'):
        state.set_api_access_token_ids(list(range(1, 1002)), 'shared-token')

    assert _get_api_access_token_rows(engine) == []


def test_api_access_token_cleanup_lookup_uses_one_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    terminal_job = _insert_job_info(engine)
    _insert_task(engine, terminal_job, 0, status=ManagedJobStatus.SUCCEEDED)
    state.set_api_access_token_ids([terminal_job], 'terminal-token')

    with _count_sql_statements(engine) as counts:
        token_id = state.get_releasable_api_access_token_id(terminal_job)

    assert token_id == 'terminal-token'
    assert counts['n'] == 1, counts


@pytest.mark.parametrize('active_status', [
    ManagedJobStatus.PENDING,
    ManagedJobStatus.RUNNING,
    ManagedJobStatus.CANCELLING,
])
def test_api_access_token_cleanup_waits_for_every_shared_job(
        _mock_managed_jobs_db_conn, active_status):
    engine = _mock_managed_jobs_db_conn
    finished_job = _insert_job_info(engine)
    active_job = _insert_job_info(engine)
    _insert_task(engine, finished_job, 0, status=ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, active_job, 0, status=ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, active_job, 1, status=active_status)
    state.set_api_access_token_ids([finished_job, active_job], 'shared-token')

    with _count_sql_statements(engine) as counts:
        token_id = state.get_releasable_api_access_token_id(finished_job)

    assert token_id is None
    assert counts['n'] == 1, counts


def test_api_access_token_cleanup_releases_after_every_shared_job_finishes(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    succeeded_job = _insert_job_info(engine)
    cancelled_job = _insert_job_info(engine)
    _insert_task(engine, succeeded_job, 0, status=ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, cancelled_job, 0, status=ManagedJobStatus.CANCELLED)
    state.set_api_access_token_ids([succeeded_job, cancelled_job],
                                   'shared-token')

    with _count_sql_statements(engine) as counts:
        token_id = state.get_releasable_api_access_token_id(cancelled_job)

    assert token_id == 'shared-token'
    assert counts['n'] == 1, counts


def test_api_access_token_cleanup_fails_closed_for_missing_task_row(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    finished_job = _insert_job_info(engine)
    missing_task_job = _insert_job_info(engine)
    _insert_task(engine, finished_job, 0, status=ManagedJobStatus.SUCCEEDED)
    state.set_api_access_token_ids([finished_job, missing_task_job],
                                   'shared-token')

    with _count_sql_statements(engine) as counts:
        token_id = state.get_releasable_api_access_token_id(finished_job)

    assert token_id is None
    assert counts['n'] == 1, counts


def test_api_access_token_cleanup_missing_association_is_one_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn

    with _count_sql_statements(engine) as counts:
        token_id = state.get_releasable_api_access_token_id(999999)

    assert token_id is None
    assert counts['n'] == 1, counts


def test_get_job_controller_processes_batches_and_normalizes(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    current_job = _insert_job_info(engine)
    legacy_job = _insert_job_info(engine)
    no_controller_job = _insert_job_info(engine)
    _set_controller_process(engine, current_job, 101, 1001.5)
    _set_controller_process(engine, legacy_job, -202, None)

    with _count_sql_statements(engine) as counts:
        records = state.get_job_controller_processes([
            current_job,
            legacy_job,
            no_controller_job,
            999999,
            current_job,
        ])

    assert records == {
        current_job: state.ControllerPidRecord(pid=101, started_at=1001.5),
        legacy_job: state.ControllerPidRecord(pid=202, started_at=None),
    }
    assert counts['n'] == 1, counts


def test_get_job_controller_processes_empty_input_uses_no_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn

    with _count_sql_statements(engine) as counts:
        assert not state.get_job_controller_processes([])

    assert counts['n'] == 0, counts


def test_get_job_controller_processes_chunks_deduped_batches(
        _mock_managed_jobs_db_conn, monkeypatch):
    engine = _mock_managed_jobs_db_conn
    first_job = _insert_job_info(engine)
    second_job = _insert_job_info(engine)
    third_job = _insert_job_info(engine)
    _set_controller_process(engine, first_job, 101, 1001.5)
    _set_controller_process(engine, second_job, 202, 1002.5)
    _set_controller_process(engine, third_job, -303, None)
    monkeypatch.setattr(state, '_STATUS_CHECK_JOB_ID_CHUNK', 2)

    with _count_sql_statements(engine) as counts:
        records = state.get_job_controller_processes([
            first_job,
            second_job,
            third_job,
            999999,
            first_job,
            second_job,
            third_job,
        ])

    assert records == {
        first_job: state.ControllerPidRecord(pid=101, started_at=1001.5),
        second_job: state.ControllerPidRecord(pid=202, started_at=1002.5),
        third_job: state.ControllerPidRecord(pid=303, started_at=None),
    }
    # Four unique ids with a chunk size of two must stay bounded to two
    # statement-sized reads; duplicates must not create extra chunk work.
    assert counts['n'] == 2, counts


def test_scheduler_set_waiting_empty_input_uses_no_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn

    with _count_sql_statements(engine) as counts:
        state.scheduler_set_waiting([], '/tmp/dag.yaml', '/tmp/user.yaml',
                                    '/tmp/env', None, 100)

    assert counts['n'] == 0, counts


def test_scheduler_set_waiting_deduplicates_repeated_job_ids(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    job_id = state.set_job_info_without_job_id(
        name='waiting',
        workspace='team-a',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )

    with _count_sql_statements(engine) as counts:
        state.scheduler_set_waiting([job_id, job_id], '/tmp/dag.yaml',
                                    '/tmp/user.yaml', '/tmp/env', None, 100)

    assert state.get_job_schedule_state(job_id) is (
        state.ManagedJobScheduleState.WAITING)
    assert (state.get_job_file_contents(job_id)['dag_yaml_content'] ==
            '/tmp/dag.yaml')
    assert counts['n'] == 1, counts


def test_get_job_controller_process_reuses_bulk_reader(monkeypatch):
    record = state.ControllerPidRecord(pid=101, started_at=1001.5)
    calls = []

    def reader(job_ids):
        calls.append(job_ids)
        return {7: record} if job_ids == [7] else {}

    monkeypatch.setattr(state, 'get_job_controller_processes', reader)

    assert state.get_job_controller_process(7) == record
    assert state.get_job_controller_process(8) is None
    assert calls == [[7], [8]]


def test_get_log_stream_context_reads_one_recovery_snapshot(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    job_id = _insert_job_info(engine)
    _insert_task(engine, job_id, 3, status=ManagedJobStatus.RUNNING)
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(state.job_info_table).where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    pool='pool-a',
                    current_cluster_name='replica-a',
                    job_id_on_pool_cluster=41,
                ))
        session.commit()

    with _count_sql_statements(engine) as counts:
        context = state.get_log_stream_context(job_id, 3)

    assert context == ('pool-a', 'replica-a', 41, 'task-3')
    assert counts['n'] == 1, counts

    state.set_current_cluster_name(job_id, 'replica-b')
    with _count_sql_statements(engine) as counts:
        recovered_context = state.get_log_stream_context(job_id, 3)

    assert recovered_context == ('pool-a', 'replica-b', 41, 'task-3')
    assert counts['n'] == 1, counts


def test_get_log_stream_context_missing_task_is_one_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    job_id = _insert_job_info(engine)

    with _count_sql_statements(engine) as counts:
        context = state.get_log_stream_context(job_id, 999)

    assert context == (None, None, None, None)
    assert counts['n'] == 1, counts


def test_get_log_stream_context_keeps_task_without_job_info(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    _insert_task(engine, 999, 3, status=ManagedJobStatus.RUNNING)

    with _count_sql_statements(engine) as counts:
        context = state.get_log_stream_context(999, 3)

    assert context == (None, None, None, 'task-3')
    assert counts['n'] == 1, counts


def test_get_latest_log_stream_snapshot_reads_one_recovery_snapshot(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    job_id = _insert_job_info(engine)
    _insert_task(engine, job_id, 2, status=ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, job_id, 3, status=ManagedJobStatus.RUNNING)
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(state.job_info_table).where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    pool='pool-a',
                    current_cluster_name='replica-a',
                    job_id_on_pool_cluster=41,
                ))
        session.commit()

    with _count_sql_statements(engine) as counts:
        snapshot = state.get_latest_log_stream_snapshot(job_id)

    assert snapshot == state.JobLogStreamSnapshot(3, ManagedJobStatus.RUNNING,
                                                  'pool-a', 'replica-a', 41,
                                                  'task-3')
    assert counts['n'] == 1, counts

    state.set_current_cluster_name(job_id, 'replica-b')
    with _count_sql_statements(engine) as counts:
        recovered_snapshot = state.get_latest_log_stream_snapshot(job_id)

    assert recovered_snapshot == state.JobLogStreamSnapshot(
        3, ManagedJobStatus.RUNNING, 'pool-a', 'replica-b', 41, 'task-3')
    assert counts['n'] == 1, counts


def test_get_latest_log_stream_snapshot_missing_job_is_one_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn

    with _count_sql_statements(engine) as counts:
        snapshot = state.get_latest_log_stream_snapshot(999)

    assert snapshot == state.JobLogStreamSnapshot(None, None, None, None, None,
                                                  None)
    assert counts['n'] == 1, counts


def test_get_latest_log_stream_snapshot_keeps_task_without_job_info(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    _insert_task(engine, 999, 3, status=ManagedJobStatus.RUNNING)

    with _count_sql_statements(engine) as counts:
        snapshot = state.get_latest_log_stream_snapshot(999)

    assert snapshot == state.JobLogStreamSnapshot(3, ManagedJobStatus.RUNNING,
                                                  None, None, None, 'task-3')
    assert counts['n'] == 1, counts


def test_get_controller_log_follow_state_reads_one_lifecycle_snapshot(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    job_id = _insert_job_info(engine)
    _insert_task(engine, job_id, 0, status=ManagedJobStatus.SUCCEEDED)
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(state.job_info_table).where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value))
        session.commit()

    with _count_sql_statements(engine) as counts:
        follow_state = state.get_controller_log_follow_state(job_id)

    assert follow_state == state.ControllerLogFollowState(
        ManagedJobStatus.SUCCEEDED, state.ManagedJobScheduleState.ALIVE)
    assert counts['n'] == 1, counts

    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(state.job_info_table).where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    schedule_state=state.ManagedJobScheduleState.DONE.value))
        session.commit()

    with _count_sql_statements(engine) as counts:
        finalized_state = state.get_controller_log_follow_state(job_id)

    assert finalized_state == state.ControllerLogFollowState(
        ManagedJobStatus.SUCCEEDED, state.ManagedJobScheduleState.DONE)
    assert counts['n'] == 1, counts


def test_get_controller_log_follow_state_missing_job_is_one_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn

    with _count_sql_statements(engine) as counts:
        follow_state = state.get_controller_log_follow_state(999)

    assert follow_state == state.ControllerLogFollowState(None, None)
    assert counts['n'] == 1, counts


def test_get_controller_log_follow_state_keeps_legacy_task_in_one_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    _insert_task(engine, 999, 0, status=ManagedJobStatus.SUCCEEDED)

    with _count_sql_statements(engine) as counts:
        follow_state = state.get_controller_log_follow_state(999)

    assert follow_state == state.ControllerLogFollowState(
        ManagedJobStatus.SUCCEEDED, None)
    assert follow_state.__class__.__module__ == 'sky.jobs.state'
    assert counts['n'] == 1, counts


def test_get_task_log_stream_snapshot_reads_one_task_snapshot(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    job_id = _insert_job_info(engine)
    _insert_task(engine, job_id, 2, status=ManagedJobStatus.RUNNING)
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(state.job_info_table).where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    pool='pool-a',
                    current_cluster_name='replica-a',
                    job_id_on_pool_cluster=41,
                ))
        session.commit()

    with _count_sql_statements(engine) as counts:
        snapshot = state.get_task_log_stream_snapshot(job_id, 2)

    assert snapshot == state.JobLogStreamSnapshot(2, ManagedJobStatus.RUNNING,
                                                  'pool-a', 'replica-a', 41,
                                                  'task-2')
    assert counts['n'] == 1, counts

    state.set_current_cluster_name(job_id, 'replica-b')
    with _count_sql_statements(engine) as counts:
        recovered_snapshot = state.get_task_log_stream_snapshot(job_id, 2)

    assert recovered_snapshot == state.JobLogStreamSnapshot(
        2, ManagedJobStatus.RUNNING, 'pool-a', 'replica-b', 41, 'task-2')
    assert counts['n'] == 1, counts


def test_get_task_log_stream_snapshot_missing_task_is_one_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    job_id = _insert_job_info(engine)

    with _count_sql_statements(engine) as counts:
        snapshot = state.get_task_log_stream_snapshot(job_id, 999)

    assert snapshot == state.JobLogStreamSnapshot(None, None, None, None, None,
                                                  None)
    assert counts['n'] == 1, counts


def test_get_task_id_name_status_log_reads_one_task_row(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    job_id = _insert_job_info(engine)
    _insert_task(engine,
                 job_id,
                 2,
                 status=ManagedJobStatus.SUCCEEDED,
                 local_log_file='/tmp/task-2.log',
                 logs_cleaned_at=123.0)

    with _count_sql_statements(engine) as counts:
        row = state.get_task_id_name_status_log(job_id, 2)

    assert row == (2, 'task-2', ManagedJobStatus.SUCCEEDED, '/tmp/task-2.log',
                   123.0)
    assert counts['n'] == 1, counts


def test_get_task_id_name_status_log_missing_task_is_one_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    job_id = _insert_job_info(engine)

    with _count_sql_statements(engine) as counts:
        row = state.get_task_id_name_status_log(job_id, 999)

    assert row is None
    assert counts['n'] == 1, counts


def test_get_pool_and_current_cluster_name_reads_one_row(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    job_id = _insert_job_info(engine)
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(state.job_info_table).where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    pool='pool-a',
                    current_cluster_name='replica-a',
                ))
        session.commit()

    with _count_sql_statements(engine) as counts:
        context = state.get_pool_and_current_cluster_name(job_id)

    assert context == ('pool-a', 'replica-a')
    assert counts['n'] == 1, counts

    state.set_current_cluster_name(job_id, 'replica-b')
    with _count_sql_statements(engine) as counts:
        refreshed_context = state.get_pool_and_current_cluster_name(job_id)

    assert refreshed_context == ('pool-a', 'replica-b')
    assert counts['n'] == 1, counts


def test_get_pool_and_current_cluster_name_missing_job_is_one_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn

    with _count_sql_statements(engine) as counts:
        context = state.get_pool_and_current_cluster_name(999999)

    assert context == (None, None)
    assert counts['n'] == 1, counts


def test_get_job_cancellation_states_batches_lifecycle_snapshot(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    active_job = state.set_job_info_without_job_id(
        name='active',
        workspace='team-a',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    completed_job = state.set_job_info_without_job_id(
        name='completed',
        workspace='team-b',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    legacy_job = _insert_job_info(engine)
    _set_controller_process(engine, active_job, 101, 1001.5)
    _set_controller_process(engine, completed_job, -202, None)

    _insert_task(engine, active_job, 0, status=state.ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, active_job, 1, status=state.ManagedJobStatus.RUNNING)
    _insert_task(engine, active_job, 2, status=state.ManagedJobStatus.PENDING)
    _insert_task(engine,
                 completed_job,
                 0,
                 status=state.ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, completed_job, 1, status=state.ManagedJobStatus.FAILED)
    _insert_task(engine, legacy_job, 0, status=state.ManagedJobStatus.STARTING)

    with _count_sql_statements(engine) as counts:
        snapshots = state.get_job_cancellation_states(
            [active_job, completed_job, legacy_job, 999999, active_job])

    assert snapshots == {
        active_job: state.JobCancellationState(state.ManagedJobStatus.RUNNING,
                                               'team-a'),
        completed_job: state.JobCancellationState(state.ManagedJobStatus.FAILED,
                                                  'team-b'),
    }
    assert counts['n'] == 1, counts


def test_get_job_cancellation_state_rows_use_latest_task_only(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    active_job = state.set_job_info_without_job_id(
        name='active',
        workspace='team-a',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    completed_job = state.set_job_info_without_job_id(
        name='completed',
        workspace='team-b',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    legacy_job = _insert_job_info(engine)
    _set_controller_process(engine, active_job, 101, 1001.5)
    _set_controller_process(engine, completed_job, -202, None)

    for task_id in range(50):
        _insert_task(
            engine,
            active_job,
            task_id,
            status=(state.ManagedJobStatus.SUCCEEDED
                    if task_id == 0 else state.ManagedJobStatus.RUNNING
                    if task_id == 1 else state.ManagedJobStatus.PENDING),
        )
    for task_id in range(30):
        _insert_task(
            engine,
            completed_job,
            task_id,
            status=(state.ManagedJobStatus.FAILED
                    if task_id == 29 else state.ManagedJobStatus.SUCCEEDED),
        )
    _insert_task(engine, legacy_job, 0, status=state.ManagedJobStatus.STARTING)

    with _count_sql_statements(engine) as counts:
        rows = state._fetch_job_cancellation_state_rows(
            [active_job, completed_job, legacy_job, 999999, active_job])

    assert rows == [
        (active_job, 1, state.ManagedJobStatus.RUNNING.value, 'team-a'),
        (completed_job, 29, state.ManagedJobStatus.FAILED.value, 'team-b'),
    ]
    assert counts['n'] == 1, counts


def test_get_job_cancellation_state_rows_preserve_duplicate_nonterminal_latest_task(  # pylint: disable=line-too-long
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    active_job = state.set_job_info_without_job_id(
        name='active',
        workspace='team-a',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    _set_controller_process(engine, active_job, 101, 1001.5)
    with engine.begin() as connection:
        connection.execute(state.spot_table.insert(), [{
            'spot_job_id': active_job,
            'task_id': 0,
            'task_name': 'task-0',
            'status': state.ManagedJobStatus.RUNNING.value,
        }, {
            'spot_job_id': active_job,
            'task_id': 0,
            'task_name': 'task-0',
            'status': state.ManagedJobStatus.SUCCEEDED.value,
        }])

    with _count_sql_statements(engine) as counts:
        rows = state._fetch_job_cancellation_state_rows(
            [active_job, active_job])

    assert rows == [(active_job, 0, state.ManagedJobStatus.RUNNING.value,
                     'team-a')]
    assert counts['n'] == 1, counts


def test_get_job_cancellation_states_preserve_duplicate_nonterminal_latest_task(  # pylint: disable=line-too-long
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn
    active_job = state.set_job_info_without_job_id(
        name='active',
        workspace='team-a',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    _set_controller_process(engine, active_job, 101, 1001.5)
    with engine.begin() as connection:
        connection.execute(state.spot_table.insert(), [{
            'spot_job_id': active_job,
            'task_id': 0,
            'task_name': 'task-0',
            'status': state.ManagedJobStatus.RUNNING.value,
        }, {
            'spot_job_id': active_job,
            'task_id': 0,
            'task_name': 'task-0',
            'status': state.ManagedJobStatus.SUCCEEDED.value,
        }])

    with _count_sql_statements(engine) as counts:
        snapshots = state.get_job_cancellation_states([active_job, active_job])

    assert snapshots == {
        active_job: state.JobCancellationState(state.ManagedJobStatus.RUNNING,
                                               'team-a'),
    }
    assert counts['n'] == 1, counts


def test_get_job_cancellation_states_chunking_preserves_snapshots(
        _mock_managed_jobs_db_conn, monkeypatch):
    engine = _mock_managed_jobs_db_conn
    active_job = state.set_job_info_without_job_id(
        name='active',
        workspace='team-a',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    completed_job = state.set_job_info_without_job_id(
        name='completed',
        workspace='team-b',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    _insert_task(engine, active_job, 0, status=state.ManagedJobStatus.RUNNING)
    _insert_task(engine,
                 completed_job,
                 0,
                 status=state.ManagedJobStatus.SUCCEEDED)
    _insert_task(engine, completed_job, 1, status=state.ManagedJobStatus.FAILED)

    full = state.get_job_cancellation_states([active_job, completed_job])

    monkeypatch.setattr(state, '_STATUS_CHECK_JOB_ID_CHUNK', 1)
    chunked = state.get_job_cancellation_states(
        [active_job, completed_job, 999999, active_job])

    assert chunked == full


def test_get_job_cancellation_states_empty_input_uses_no_query(
        _mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn

    with _count_sql_statements(engine) as counts:
        assert not state.get_job_cancellation_states([])

    assert counts['n'] == 0, counts


def test_get_task_logs_to_clean_basic(_mock_managed_jobs_db_conn):
    now = time.time()
    retention = 60

    # Prepare one job with multiple tasks
    job_id = state.set_job_info_without_job_id(
        name='job-a',
        workspace='ws',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    engine = state._db_manager.get_engine()
    # Qualifies: terminal + old + not cleaned
    _insert_task(
        engine,
        job_id,
        0,
        status=ManagedJobStatus.SUCCEEDED,
        end_at=now - 120,
        local_log_file='/tmp/a.log',
        logs_cleaned_at=None,
    )
    # Not old enough
    _insert_task(
        engine,
        job_id,
        1,
        status=ManagedJobStatus.SUCCEEDED,
        end_at=now - 30,
        local_log_file='/tmp/b.log',
        logs_cleaned_at=None,
    )
    # Already cleaned
    _insert_task(
        engine,
        job_id,
        2,
        status=ManagedJobStatus.FAILED,
        end_at=now - 120,
        local_log_file='/tmp/c.log',
        logs_cleaned_at=now - 10,
    )
    # Non-terminal
    _insert_task(
        engine,
        job_id,
        3,
        status=ManagedJobStatus.RUNNING,
        end_at=None,
        local_log_file='/tmp/d.log',
        logs_cleaned_at=None,
    )
    # Terminal and old, but local_log_file is None -> should not qualify
    _insert_task(
        engine,
        job_id,
        6,
        status=ManagedJobStatus.SUCCEEDED,
        end_at=now - 200,
        local_log_file=None,
        logs_cleaned_at=None,
    )

    state.scheduler_set_done(job_id)

    res = state.get_task_logs_to_clean(retention, batch_size=10)
    # Only task 0 should be returned
    assert len(res) == 1
    assert res[0]['job_id'] == job_id
    assert res[0]['task_id'] == 0
    assert res[0]['local_log_file'] == '/tmp/a.log'

    # Batch size respected: add two more qualifying tasks
    _insert_task(
        engine,
        job_id,
        4,
        status=ManagedJobStatus.CANCELLED,
        end_at=now - 200,
        local_log_file='/tmp/e.log',
        logs_cleaned_at=None,
    )
    _insert_task(
        engine,
        job_id,
        5,
        status=ManagedJobStatus.SUCCEEDED,
        end_at=now - 300,
        local_log_file='/tmp/f.log',
        logs_cleaned_at=None,
    )

    res2 = state.get_task_logs_to_clean(retention, batch_size=2)
    assert len(res2) == 2  # limited by batch size


def test_set_task_logs_cleaned(_mock_managed_jobs_db_conn):
    now = time.time()
    retention = 60

    job_id = state.set_job_info_without_job_id(
        name='job-b',
        workspace='ws',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    engine = state._db_manager.get_engine()
    _insert_task(
        engine,
        job_id,
        0,
        status=ManagedJobStatus.SUCCEEDED,
        end_at=now - 120,
        local_log_file='/tmp/a.log',
        logs_cleaned_at=None,
    )

    state.scheduler_set_done(job_id)

    res = state.get_task_logs_to_clean(retention, batch_size=10)
    assert len(res) == 1

    ts = now
    state.set_task_logs_cleaned([(job_id, 0)], ts)

    # Verify updated
    with orm.Session(engine) as session:
        row = session.execute(
            state.sqlalchemy.select(state.spot_table.c.logs_cleaned_at).where(
                state.sqlalchemy.and_(
                    state.spot_table.c.spot_job_id == job_id,
                    state.spot_table.c.task_id == 0))).fetchone()
        assert row is not None
        assert row[0] == ts

    # Should no longer be returned
    res2 = state.get_task_logs_to_clean(retention, batch_size=10)
    assert res2 == []


def test_get_controller_logs_to_clean_basic(_mock_managed_jobs_db_conn):
    now = time.time()
    retention = 60

    # Job A: qualifies (max end_at old, controller logs not cleaned)
    engine = state._db_manager.get_engine()
    job_a = _insert_job_info(engine, controller_logs_cleaned_at=None)
    _insert_task(
        engine,
        job_a,
        0,
        status=ManagedJobStatus.SUCCEEDED,
        end_at=now - 200,
        local_log_file='/tmp/a0.log',
        logs_cleaned_at=None,
    )
    _insert_task(
        engine,
        job_a,
        1,
        status=ManagedJobStatus.FAILED,
        end_at=now - 150,
        local_log_file='/tmp/a1.log',
        logs_cleaned_at=None,
    )
    state.scheduler_set_done(job_a)

    # Job B: not old enough
    job_b = _insert_job_info(engine, controller_logs_cleaned_at=None)
    _insert_task(
        engine,
        job_b,
        0,
        status=ManagedJobStatus.SUCCEEDED,
        end_at=now - 30,
        local_log_file='/tmp/b0.log',
        logs_cleaned_at=None,
    )
    state.scheduler_set_done(job_b)

    # Job C: already cleaned controller logs
    job_c = _insert_job_info(engine, controller_logs_cleaned_at=now - 10)
    _insert_task(
        engine,
        job_c,
        0,
        status=ManagedJobStatus.SUCCEEDED,
        end_at=now - 200,
        local_log_file='/tmp/c0.log',
        logs_cleaned_at=None,
    )
    state.scheduler_set_done(job_c)

    # Job D: terminal but end_at is None -> does not qualify
    job_d = _insert_job_info(engine, controller_logs_cleaned_at=None)
    _insert_task(
        engine,
        job_d,
        0,
        status=ManagedJobStatus.CANCELLED,
        end_at=None,
        local_log_file='/tmp/d0.log',
        logs_cleaned_at=None,
    )
    state.scheduler_set_done(job_d)

    res = state.get_controller_logs_to_clean(retention, batch_size=10)
    job_ids = {r['job_id'] for r in res}
    assert job_ids == {job_a}

    # Batch size respected: clone more qualifying jobs
    job_e = _insert_job_info(engine, controller_logs_cleaned_at=None)
    _insert_task(
        engine,
        job_e,
        0,
        status=ManagedJobStatus.SUCCEEDED,
        end_at=now - 400,
        local_log_file='/tmp/e0.log',
        logs_cleaned_at=None,
    )
    state.scheduler_set_done(job_e)
    job_f = _insert_job_info(engine, controller_logs_cleaned_at=None)
    _insert_task(
        engine,
        job_f,
        0,
        status=ManagedJobStatus.FAILED,
        end_at=now - 500,
        local_log_file='/tmp/f0.log',
        logs_cleaned_at=None,
    )
    state.scheduler_set_done(job_f)

    res2 = state.get_controller_logs_to_clean(retention, batch_size=2)
    assert len(res2) == 2


def test_set_controller_logs_cleaned(_mock_managed_jobs_db_conn):
    now = time.time()

    engine = state._db_manager.get_engine()
    job_id = _insert_job_info(engine, controller_logs_cleaned_at=None)

    state.set_controller_logs_cleaned([job_id], now)

    with orm.Session(engine) as session:
        row = session.execute(
            state.sqlalchemy.select(
                state.job_info_table.c.controller_logs_cleaned_at).where(
                    state.job_info_table.c.spot_job_id == job_id)).fetchone()
        assert row is not None
        assert row[0] == now


def test_get_active_file_mounts_blob_ids(_mock_managed_jobs_db_conn):
    engine = _mock_managed_jobs_db_conn

    # Non-terminal job holding a blob -> should be returned.
    active_job = state.set_job_info_without_job_id(
        name='active',
        workspace='ws',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
        file_mounts_blob_id='blob-active',
    )
    _insert_task(engine, active_job, 0, status=ManagedJobStatus.RUNNING)

    # Terminal job -> should NOT be returned even though it has a blob.
    terminal_job = state.set_job_info_without_job_id(
        name='done',
        workspace='ws',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
        file_mounts_blob_id='blob-done',
    )
    _insert_task(engine, terminal_job, 0, status=ManagedJobStatus.SUCCEEDED)

    # Non-terminal job without a blob -> should NOT be returned.
    no_blob_job = state.set_job_info_without_job_id(
        name='no-blob',
        workspace='ws',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    _insert_task(engine, no_blob_job, 0, status=ManagedJobStatus.PENDING)

    # Queued (non-terminal) job -> should be returned.
    queued_job = state.set_job_info_without_job_id(
        name='queued',
        workspace='ws',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
        file_mounts_blob_id='blob-queued',
    )
    _insert_task(engine, queued_job, 0, status=ManagedJobStatus.PENDING)

    # Recovering job -> should be returned (long-tail case that motivated
    # this ref tracking).
    recovering_job = state.set_job_info_without_job_id(
        name='recovering',
        workspace='ws',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
        file_mounts_blob_id='blob-recovering',
    )
    _insert_task(engine, recovering_job, 0, status=ManagedJobStatus.RECOVERING)

    blob_ids = state.get_active_file_mounts_blob_ids()
    assert blob_ids == {'blob-active', 'blob-queued', 'blob-recovering'}


@pytest.mark.asyncio
async def test_get_file_mounts_blob_id_async(_mock_managed_jobs_db_conn):
    blob_job = state.set_job_info_without_job_id(
        name='with-blob',
        workspace='ws',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
        file_mounts_blob_id='blob-async',
    )
    null_job = state.set_job_info_without_job_id(
        name='without-blob',
        workspace='ws',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )

    assert await state.get_file_mounts_blob_id_async(blob_job) == 'blob-async'
    assert await state.get_file_mounts_blob_id_async(null_job) is None
    assert await state.get_file_mounts_blob_id_async(999_999) is None


def _new_pool_job(engine,
                  *,
                  pool: str,
                  status: ManagedJobStatus,
                  cluster_name=None) -> int:
    """Create a managed job in `pool` with optional `current_cluster_name`."""
    job_id = state.set_job_info_without_job_id(
        name=f'job-{pool}',
        workspace='ws',
        entrypoint='entry',
        pool=pool,
        pool_hash=None,
        user_hash='u',
    )
    _insert_task(engine, job_id, 0, status=status)
    if cluster_name is not None:
        state.set_current_cluster_name(job_id, cluster_name)
    return job_id


def test_get_nonterminal_job_ids_by_pool_grouped(_mock_managed_jobs_db_conn):
    """Verify the batched grouped query matches the per-call helper."""
    engine = state._db_manager.get_engine()

    # Pool A: unassigned job, two replicas with one nonterminal job each,
    # one job on a replica that is already SUCCEEDED (should be excluded).
    unassigned_a = _new_pool_job(engine,
                                 pool='pool-a',
                                 status=ManagedJobStatus.PENDING)
    r1_running_a = _new_pool_job(engine,
                                 pool='pool-a',
                                 status=ManagedJobStatus.RUNNING,
                                 cluster_name='replica-1')
    r1_recovering_a = _new_pool_job(engine,
                                    pool='pool-a',
                                    status=ManagedJobStatus.RECOVERING,
                                    cluster_name='replica-1')
    r2_running_a = _new_pool_job(engine,
                                 pool='pool-a',
                                 status=ManagedJobStatus.RUNNING,
                                 cluster_name='replica-2')
    _new_pool_job(engine,
                  pool='pool-a',
                  status=ManagedJobStatus.SUCCEEDED,
                  cluster_name='replica-1')  # terminal -> filtered

    # Pool B: separate pool to ensure the filter is scoped correctly.
    _new_pool_job(engine, pool='pool-b', status=ManagedJobStatus.RUNNING)

    grouped = state.get_nonterminal_job_ids_by_pool_grouped('pool-a')

    assert set(grouped.keys()) == {None, 'replica-1', 'replica-2'}
    assert grouped[None] == [unassigned_a]
    assert grouped['replica-1'] == sorted([r1_running_a, r1_recovering_a])
    assert grouped['replica-2'] == [r2_running_a]

    # Grouped result must agree with the legacy per-call helper.
    assert sorted(grouped['replica-1']) == sorted(
        state.get_nonterminal_job_ids_by_pool('pool-a',
                                              cluster_name='replica-1'))
    assert sorted(grouped['replica-2']) == sorted(
        state.get_nonterminal_job_ids_by_pool('pool-a',
                                              cluster_name='replica-2'))
    all_jobs_a = sorted(state.get_nonterminal_job_ids_by_pool('pool-a'))
    flattened = sorted(j for ids in grouped.values() for j in ids)
    assert flattened == all_jobs_a


def test_get_nonterminal_job_ids_by_pool_grouped_empty(
        _mock_managed_jobs_db_conn):
    """No jobs in pool -> empty dict (not raise)."""
    assert not state.get_nonterminal_job_ids_by_pool_grouped('nope')


def test_get_nonterminal_job_ids_by_pool_grouped_all_terminal(
        _mock_managed_jobs_db_conn):
    """Pool with only finished jobs should also yield an empty grouping."""
    engine = state._db_manager.get_engine()
    _new_pool_job(engine,
                  pool='pool-done',
                  status=ManagedJobStatus.SUCCEEDED,
                  cluster_name='replica-x')
    _new_pool_job(engine, pool='pool-done', status=ManagedJobStatus.FAILED)
    assert not state.get_nonterminal_job_ids_by_pool_grouped('pool-done')


def test_get_nonterminal_job_status_counts_by_pool_uses_one_grouped_query(
        _mock_managed_jobs_db_conn):
    """Pool job-status badges should come from one slim grouped query."""
    engine = state._db_manager.get_engine()
    _new_pool_job(engine, pool='pool-a', status=ManagedJobStatus.PENDING)
    _new_pool_job(engine, pool='pool-a', status=ManagedJobStatus.RUNNING)
    _new_pool_job(engine, pool='pool-a', status=ManagedJobStatus.RECOVERING)
    _new_pool_job(engine, pool='pool-a', status=ManagedJobStatus.SUCCEEDED)

    multi_task_job = state.set_job_info_without_job_id(
        name='multi-task-pool-a',
        workspace='ws',
        entrypoint='entry',
        pool='pool-a',
        pool_hash=None,
        user_hash='u',
    )
    _insert_task(engine, multi_task_job, 0, status=ManagedJobStatus.RUNNING)
    _insert_task(engine, multi_task_job, 1, status=ManagedJobStatus.PENDING)

    _new_pool_job(engine, pool='pool-b', status=ManagedJobStatus.RUNNING)

    with _count_sql_statements(engine) as counts:
        result = state.get_nonterminal_job_status_counts_by_pool('pool-a')

    assert counts['n'] == 1, counts
    assert result == {
        ManagedJobStatus.PENDING.value: 2,
        ManagedJobStatus.RUNNING.value: 2,
        ManagedJobStatus.RECOVERING.value: 1,
    }


def test_get_pending_jobs_count_by_pool_counts_distinct_jobs(
        _mock_managed_jobs_db_conn):
    """Pending queue length should count jobs once, even with many tasks."""
    engine = state._db_manager.get_engine()

    pending_multi_task = state.set_job_info_without_job_id(
        name='pending-multi',
        workspace='ws',
        entrypoint='entry',
        pool='pool-a',
        pool_hash=None,
        user_hash='u',
    )
    _insert_task(engine, pending_multi_task, 0, status=ManagedJobStatus.PENDING)
    _insert_task(engine, pending_multi_task, 1, status=ManagedJobStatus.PENDING)

    pending_single = _new_pool_job(engine,
                                   pool='pool-a',
                                   status=ManagedJobStatus.PENDING)
    _new_pool_job(engine, pool='pool-a', status=ManagedJobStatus.RUNNING)
    _new_pool_job(engine, pool='pool-a', status=ManagedJobStatus.SUCCEEDED)
    _new_pool_job(engine, pool='pool-b', status=ManagedJobStatus.PENDING)

    assert state.get_pending_jobs_count_by_pool('pool-a') == 2
    assert state.get_pending_jobs_count_by_pool('pool-b') == 1
    assert pending_multi_task != pending_single


def test_get_pending_jobs_count_by_pool_excludes_jobs_with_assigned_worker(
        _mock_managed_jobs_db_conn):
    """Assigned pool jobs should not still count as queued demand."""
    engine = state._db_manager.get_engine()

    queued_job = _new_pool_job(engine,
                               pool='pool-a',
                               status=ManagedJobStatus.PENDING)

    assigned_multi_task = state.set_job_info_without_job_id(
        name='assigned-multi',
        workspace='ws',
        entrypoint='entry',
        pool='pool-a',
        pool_hash=None,
        user_hash='u',
    )
    _insert_task(engine,
                 assigned_multi_task,
                 0,
                 status=ManagedJobStatus.RUNNING)
    _insert_task(engine,
                 assigned_multi_task,
                 1,
                 status=ManagedJobStatus.PENDING)
    state.set_current_cluster_name(assigned_multi_task, 'replica-1')

    assert state.get_pending_jobs_count_by_pool('pool-a') == 1
    assert assigned_multi_task != queued_job


def test_get_pending_jobs_count_by_pool_uses_single_aggregate_query(
        _mock_managed_jobs_db_conn):
    """Queue length should stay one grouped query, not scale with tasks."""
    engine = state._db_manager.get_engine()
    pending_multi_task = state.set_job_info_without_job_id(
        name='pending-multi-query-shape',
        workspace='ws',
        entrypoint='entry',
        pool='pool-a',
        pool_hash=None,
        user_hash='u',
    )
    for task_id in range(50):
        _insert_task(engine,
                     pending_multi_task,
                     task_id,
                     status=ManagedJobStatus.PENDING)

    with _count_sql_statements(engine) as counts:
        assert state.get_pending_jobs_count_by_pool('pool-a') == 1

    assert counts['n'] == 1, counts


def test_get_task_logs_to_clean_excludes_failed_tasks(
        _mock_managed_jobs_db_conn):
    """Excluded (job, task) pairs must not occupy batch slots."""
    now = time.time()
    engine = state._db_manager.get_engine()
    job_id = state.set_job_info_without_job_id(
        name='job-exclude',
        workspace='ws',
        entrypoint='entry',
        pool=None,
        pool_hash=None,
        user_hash='u',
    )
    for task_id in range(3):
        _insert_task(
            engine,
            job_id,
            task_id,
            status=ManagedJobStatus.SUCCEEDED,
            end_at=now - 200,
            local_log_file=f'/tmp/t{task_id}.log',
            logs_cleaned_at=None,
        )
    state.scheduler_set_done(job_id)

    res = state.get_task_logs_to_clean(60,
                                       batch_size=2,
                                       exclude_tasks={(job_id, 0), (job_id, 1)})
    assert [(r['job_id'], r['task_id']) for r in res] == [(job_id, 2)]

    # Empty/None exclusion keeps the original behavior.
    assert len(state.get_task_logs_to_clean(60, batch_size=10)) == 3
    assert len(
        state.get_task_logs_to_clean(60, batch_size=10,
                                     exclude_tasks=set())) == 3


def test_get_controller_logs_to_clean_excludes_failed_jobs(
        _mock_managed_jobs_db_conn):
    """Excluded job ids must not occupy batch slots."""
    now = time.time()
    engine = state._db_manager.get_engine()
    job_ids = []
    for idx in range(3):
        job_id = _insert_job_info(engine, controller_logs_cleaned_at=None)
        _insert_task(
            engine,
            job_id,
            0,
            status=ManagedJobStatus.SUCCEEDED,
            end_at=now - 200,
            local_log_file=f'/tmp/c{idx}.log',
            logs_cleaned_at=None,
        )
        state.scheduler_set_done(job_id)
        job_ids.append(job_id)

    res = state.get_controller_logs_to_clean(
        60, batch_size=2, exclude_job_ids={job_ids[0], job_ids[1]})
    assert [r['job_id'] for r in res] == [job_ids[2]]

    assert len(state.get_controller_logs_to_clean(60, batch_size=10)) == 3
    assert len(
        state.get_controller_logs_to_clean(60,
                                           batch_size=10,
                                           exclude_job_ids=set())) == 3
