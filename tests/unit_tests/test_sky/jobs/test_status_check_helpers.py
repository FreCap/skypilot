"""Tests for slim managed-job status-refresh helpers."""

# pylint: disable=invalid-name,unused-import
# Fixture imports are referenced indirectly by pytest, and the fixture names
# intentionally mirror the shared helpers they exercise.

import pytest
from test_jobs_state import _mock_managed_jobs_db_conn
from test_jobs_state import _seed_multi_task_job
from test_jobs_state import _seed_test_jobs

from sky.jobs import state


class TestGetJobsStatusCheckInfoLaunchIdentity:
    """The refresh snapshot must preserve task launch identity."""

    def test_task_name_uses_launch_identity_not_public_job_name(
            self, _seed_test_jobs):
        # The seed sets job_info.name='test-job-a' but task_name='task0'.
        job_id1 = _seed_test_jobs['job_id1']
        info = state.get_jobs_status_check_info([job_id1])
        assert info[job_id1]['tasks'][0]['task_name'] == 'task0'

    def test_multi_task_task_names_follow_task_order(self,
                                                     _seed_multi_task_job):
        pipeline_id = _seed_multi_task_job['pipeline_job_id']
        info = state.get_jobs_status_check_info([pipeline_id])
        task_names = [t['task_name'] for t in info[pipeline_id]['tasks']]
        assert task_names == ['extract', 'transform']

    def test_snapshot_preserves_launch_attempt_fields(self, _seed_test_jobs):
        job_id = _seed_test_jobs['job_id1']
        slim_tasks = state.get_jobs_status_check_info([job_id])[job_id]['tasks']
        full_tasks = state.get_managed_job_tasks(job_id)

        assert len(slim_tasks) == len(full_tasks)
        for slim, full in zip(slim_tasks, full_tasks):
            assert slim['task_name'] == full['task_name']
            assert slim['submitted_at'] == full.get('submitted_at')
            assert slim['start_at'] == full.get('start_at')
            assert slim['last_recovered_at'] == full.get('last_recovered_at')

    def test_snapshot_keeps_outer_controller_owner(self, _seed_test_jobs,
                                                   _mock_managed_jobs_db_conn):
        job_id = _seed_test_jobs['job_id1']
        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    controller_instance_id='controller-a',
                    controller_generation=19))
            session.commit()

        info = state.get_jobs_status_check_info([job_id])[job_id]

        assert info['controller_instance_id'] == 'controller-a'
        assert info['controller_generation'] == 19


class TestGetJobStatusCheckState:
    """Coverage for the one-row destructive recheck helper."""

    def test_missing_job_returns_none(self, _mock_managed_jobs_db_conn):
        assert state.get_job_status_check_state(999999) is None

    def test_matches_job_info_fields(self, _seed_test_jobs):
        for job_id in _seed_test_jobs.values():
            tasks = state.get_managed_job_tasks(job_id)
            info = state.get_job_status_check_state(job_id)
            assert info is not None
            assert info['schedule_state'] == tasks[0]['schedule_state']
            assert info['controller_pid'] == tasks[0]['controller_pid']
            assert info['controller_pid_started_at'] == tasks[0].get(
                'controller_pid_started_at')
            assert info['controller_instance_id'] == tasks[0].get(
                'controller_instance_id')
            assert info['controller_generation'] == tasks[0].get(
                'controller_generation')
            assert info['all_tasks_terminal'] == all(
                task['status'].is_terminal() for task in tasks)

    def test_tracks_terminality_across_multi_task_jobs(self,
                                                       _seed_multi_task_job):
        pipeline_id = _seed_multi_task_job['pipeline_job_id']
        pipeline_info = state.get_job_status_check_state(pipeline_id)
        assert pipeline_info is not None
        assert pipeline_info['all_tasks_terminal'] is False

        failed_job_id = _seed_multi_task_job['failed_job_id']
        failed_info = state.get_job_status_check_state(failed_job_id)
        assert failed_info is not None
        assert failed_info['all_tasks_terminal'] is True


class TestHasJobsRequiringRecoveryGraceWait:
    """Coverage for the leader handoff grace-wait classifier."""

    def test_empty_database_returns_false(self, _mock_managed_jobs_db_conn):
        assert state.has_jobs_requiring_recovery_grace_wait() is False

    def test_active_job_returns_true(self, _mock_managed_jobs_db_conn):
        job_id = state.set_job_info_without_job_id(name='active-job',
                                                   workspace='ws1',
                                                   entrypoint='ep',
                                                   pool=None,
                                                   pool_hash=None,
                                                   user_hash='user1')
        state.set_pending(job_id, 0, 'task0', '{}', '{}')
        state.scheduler_set_waiting([job_id], '/tmp/dag.yaml', '/tmp/user.yaml',
                                    '/tmp/env', None, 100)

        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    schedule_state=state.ManagedJobScheduleState.ALIVE.value))
            session.commit()

        assert state.has_jobs_requiring_recovery_grace_wait() is True

    @pytest.mark.parametrize('schedule_state', [
        state.ManagedJobScheduleState.INACTIVE.value,
        state.ManagedJobScheduleState.WAITING.value,
    ])
    def test_pure_backlog_without_controller_claim_returns_false(
            self, _mock_managed_jobs_db_conn, schedule_state):
        job_id = state.set_job_info_without_job_id(name='backlog-only',
                                                   workspace='ws1',
                                                   entrypoint='ep',
                                                   pool=None,
                                                   pool_hash=None,
                                                   user_hash='user1')
        state.set_pending(job_id, 0, 'task0', '{}', '{}')
        if schedule_state == state.ManagedJobScheduleState.WAITING.value:
            state.scheduler_set_waiting([job_id], '/tmp/dag.yaml',
                                        '/tmp/user.yaml', '/tmp/env', None, 100)

        assert state.has_jobs_requiring_recovery_grace_wait() is False

    def test_waiting_job_with_controller_claim_returns_true(
            self, _mock_managed_jobs_db_conn):
        job_id = state.set_job_info_without_job_id(name='pending-only',
                                                   workspace='ws1',
                                                   entrypoint='ep',
                                                   pool=None,
                                                   pool_hash=None,
                                                   user_hash='user1')
        state.set_pending(job_id, 0, 'task0', '{}', '{}')
        state.scheduler_set_waiting([job_id], '/tmp/dag.yaml', '/tmp/user.yaml',
                                    '/tmp/env', None, 100)

        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update().where(
                state.job_info_table.c.spot_job_id == job_id).values(
                    controller_pid=1234))
            session.commit()

        assert state.has_jobs_requiring_recovery_grace_wait() is True

    def test_launching_job_without_pid_still_returns_true(
            self, _mock_managed_jobs_db_conn):
        job_id = state.set_job_info_without_job_id(name='launching-no-pid',
                                                   workspace='ws1',
                                                   entrypoint='ep',
                                                   pool=None,
                                                   pool_hash=None,
                                                   user_hash='user1')
        state.set_pending(job_id, 0, 'task0', '{}', '{}')

        with state.orm.Session(_mock_managed_jobs_db_conn) as session:
            session.execute(state.job_info_table.update(
            ).where(state.job_info_table.c.spot_job_id == job_id).values(
                schedule_state=state.ManagedJobScheduleState.LAUNCHING.value))
            session.commit()

        assert state.has_jobs_requiring_recovery_grace_wait() is True
