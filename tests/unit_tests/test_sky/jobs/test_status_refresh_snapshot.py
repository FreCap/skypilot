"""Tests for the managed-jobs refresh snapshot helper."""

# pylint: disable=invalid-name,unused-import
# Fixture imports are referenced indirectly by pytest, and the fixture names
# intentionally mirror the shared helpers they exercise.

from sqlalchemy import event
from test_jobs_state import _mock_managed_jobs_db_conn
from test_jobs_state import _seed_multi_task_job
from test_jobs_state import _seed_test_jobs

from sky.jobs import state
from sky.jobs import utils as jobs_utils


def _insert_legacy_running_job(engine, *, workspace='default') -> int:
    with engine.begin() as connection:
        result = connection.execute(state.job_info_table.insert().values(
            name='legacy-job',
            workspace=workspace,
            schedule_state=None,
        ))
        job_id = result.lastrowid
        connection.execute(state.spot_table.insert().values(
            spot_job_id=job_id,
            task_id=0,
            task_name='task-0',
            status=state.ManagedJobStatus.RUNNING.value,
        ))
    return job_id


class TestGetJobsToCheckStatusInfo:
    """Coverage for the one-query refresh snapshot helper."""

    def test_matches_job_id_helper_and_slim_snapshot(self, _seed_test_jobs):
        job_ids = state.get_jobs_to_check_status()
        info = state.get_jobs_to_check_status_info()

        assert list(info) == job_ids
        assert info == state.get_jobs_status_check_info(job_ids)

    def test_done_nonterminal_job_keeps_terminal_sibling_tasks(
            self, _mock_managed_jobs_db_conn, _seed_multi_task_job):
        pipeline_id = _seed_multi_task_job['pipeline_job_id']
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == pipeline_id).values(
                    schedule_state=state.ManagedJobScheduleState.DONE.value))
            session.commit()

        info = state.get_jobs_to_check_status_info([pipeline_id])

        assert list(info) == [pipeline_id]
        tasks = info[pipeline_id]['tasks']
        assert [task['task_id'] for task in tasks] == [0, 1]
        assert [task['status'] for task in tasks] == [
            state.ManagedJobStatus.SUCCEEDED,
            state.ManagedJobStatus.RUNNING,
        ]

    def test_legacy_job_is_excluded_instead_of_crashing(
            self, _mock_managed_jobs_db_conn, _seed_test_jobs):
        legacy_job_id = _insert_legacy_running_job(_mock_managed_jobs_db_conn)

        info = state.get_jobs_to_check_status_info([legacy_job_id])

        assert not info
        modern_info = state.get_jobs_to_check_status_info()
        assert legacy_job_id not in modern_info
        assert modern_info

    def test_batched_job_ids_filter_matches_full_snapshot(
            self, _seed_test_jobs):
        full = state.get_jobs_to_check_status_info()
        job_ids = list(full)
        assert len(job_ids) >= 2

        batched = state.get_jobs_to_check_status_info(job_ids)
        assert batched == full

        subset = state.get_jobs_to_check_status_info(job_ids[:1])
        assert list(subset) == job_ids[:1]

        assert not state.get_jobs_to_check_status_info([])

    def test_batched_job_ids_chunking_merges_all_chunks(self, _seed_test_jobs,
                                                        monkeypatch):
        full = state.get_jobs_to_check_status_info()
        job_ids = list(full)
        assert len(job_ids) >= 2

        # Force multiple IN (...) chunks and verify the merged result is
        # identical to the single-query snapshot.
        monkeypatch.setattr(state, '_STATUS_CHECK_JOB_ID_CHUNK', 1)
        batched = state.get_jobs_to_check_status_info(job_ids)
        assert batched == full

    def test_status_check_helper_dedupes_duplicate_job_ids(
            self, _mock_managed_jobs_db_conn, _seed_test_jobs, monkeypatch):
        monkeypatch.setattr(state, '_STATUS_CHECK_JOB_ID_CHUNK', 1)
        job_id = _seed_test_jobs['job_id1']
        expected = state.get_jobs_status_check_info([job_id])[job_id]
        select_count = 0

        def _before_cursor_execute(conn, cursor, statement, parameters, context,
                                   executemany):
            del conn, cursor, parameters, context, executemany
            nonlocal select_count
            if statement.lstrip().upper().startswith('SELECT'):
                select_count += 1

        event.listen(_mock_managed_jobs_db_conn, 'before_cursor_execute',
                     _before_cursor_execute)
        try:
            info = state.get_jobs_status_check_info([job_id, job_id])
        finally:
            event.remove(_mock_managed_jobs_db_conn, 'before_cursor_execute',
                         _before_cursor_execute)

        assert info == {job_id: expected}
        assert select_count == 1

    def test_duplicate_job_ids_do_not_duplicate_tasks_or_queries(
            self, _mock_managed_jobs_db_conn, _seed_test_jobs, monkeypatch):
        full = state.get_jobs_to_check_status_info()
        job_id = next(iter(full))
        expected = full[job_id]
        monkeypatch.setattr(state, '_STATUS_CHECK_JOB_ID_CHUNK', 1)
        select_count = 0

        def _before_cursor_execute(conn, cursor, statement, parameters, context,
                                   executemany):
            del conn, cursor, parameters, context, executemany
            nonlocal select_count
            if statement.lstrip().upper().startswith('SELECT'):
                select_count += 1

        event.listen(_mock_managed_jobs_db_conn, 'before_cursor_execute',
                     _before_cursor_execute)
        try:
            info = state.get_jobs_to_check_status_info([job_id, job_id])
        finally:
            event.remove(_mock_managed_jobs_db_conn, 'before_cursor_execute',
                         _before_cursor_execute)

        assert info == {job_id: expected}
        assert select_count == 1

    def test_refresh_snapshot_issues_single_select(self,
                                                   _mock_managed_jobs_db_conn,
                                                   _seed_test_jobs):
        del _seed_test_jobs
        select_count = 0

        def _before_cursor_execute(conn, cursor, statement, parameters, context,
                                   executemany):
            del conn, cursor, parameters, context, executemany
            nonlocal select_count
            if statement.lstrip().upper().startswith('SELECT'):
                select_count += 1

        event.listen(_mock_managed_jobs_db_conn, 'before_cursor_execute',
                     _before_cursor_execute)
        try:
            info = state.get_jobs_to_check_status_info()
        finally:
            event.remove(_mock_managed_jobs_db_conn, 'before_cursor_execute',
                         _before_cursor_execute)

        assert info
        assert select_count == 1

    def test_cancel_legacy_job_is_not_cancellable_through_modern_snapshot(
            self, _mock_managed_jobs_db_conn, tmp_path, monkeypatch):
        legacy_job_id = _insert_legacy_running_job(_mock_managed_jobs_db_conn)
        monkeypatch.setattr('sky.jobs.constants.CONSOLIDATED_SIGNAL_PATH',
                            str(tmp_path))
        monkeypatch.setattr(jobs_utils, '_controller_is_restarting',
                            lambda: False)

        result = jobs_utils.cancel_jobs_by_id([legacy_job_id],
                                              current_workspace='default')

        assert result == 'No job to cancel.'
        assert not (tmp_path / str(legacy_job_id)).exists()
