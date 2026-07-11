"""Tests for slim managed-job status-refresh helpers."""

# pylint: disable=invalid-name,unused-import
# Fixture imports are referenced indirectly by pytest, and the fixture names
# intentionally mirror the shared helpers they exercise.

from sky.jobs import state
from tests.unit_tests.test_sky.jobs.test_jobs_state import (
    _mock_managed_jobs_db_conn)
from tests.unit_tests.test_sky.jobs.test_jobs_state import _seed_multi_task_job
from tests.unit_tests.test_sky.jobs.test_jobs_state import _seed_test_jobs


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
