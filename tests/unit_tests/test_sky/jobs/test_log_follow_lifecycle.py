"""Managed-job log-follow lifecycle regression tests."""

# pylint: disable=protected-access

from unittest import mock

from sky import exceptions
from sky.jobs import state as managed_job_state
from sky.jobs import utils as jobs_utils


class _FakeHandle:
    """Minimal resource handle accepted by the stream implementation."""


class _FakeBackend:
    """Records the remote log and status calls made by one follow cycle."""

    def __init__(self):
        self.tail_calls = 0
        self.status_calls = 0

    def tail_logs(self, *args, **kwargs):
        del args, kwargs
        self.tail_calls += 1
        return exceptions.JobExitCode.SUCCEEDED.value

    def get_job_status(self, *args, **kwargs):
        del args, kwargs
        self.status_calls += 1
        return {1: jobs_utils.job_lib.JobStatus.SUCCEEDED}


class TestWaitForNextTask:
    """Checks the lifecycle and polling boundaries between JobGroup tasks."""

    def test_terminal_status_ends_wait_without_sleep(self, monkeypatch):
        status_read = mock.Mock(
            return_value=(0, managed_job_state.ManagedJobStatus.FAILED))
        sleep = mock.Mock()
        monkeypatch.setattr(managed_job_state, 'get_latest_task_id_status',
                            status_read)
        monkeypatch.setattr(jobs_utils.time, 'sleep', sleep)

        result = jobs_utils._wait_for_next_task(job_id=42, current_task_id=0)

        assert result == (0, managed_job_state.ManagedJobStatus.FAILED)
        status_read.assert_called_once_with(42)
        sleep.assert_not_called()

    def test_cancelling_status_ends_wait_without_sleep(self, monkeypatch):
        status_read = mock.Mock(
            return_value=(0, managed_job_state.ManagedJobStatus.CANCELLING))
        sleep = mock.Mock()
        monkeypatch.setattr(managed_job_state, 'get_latest_task_id_status',
                            status_read)
        monkeypatch.setattr(jobs_utils.time, 'sleep', sleep)

        result = jobs_utils._wait_for_next_task(job_id=42, current_task_id=0)

        assert result == (0, managed_job_state.ManagedJobStatus.CANCELLING)
        status_read.assert_called_once_with(42)
        sleep.assert_not_called()

    def test_running_status_waits_for_next_task(self, monkeypatch):
        status_read = mock.Mock(side_effect=[
            (0, managed_job_state.ManagedJobStatus.RUNNING),
            (1, managed_job_state.ManagedJobStatus.RUNNING),
        ])
        sleep = mock.Mock()
        monkeypatch.setattr(managed_job_state, 'get_latest_task_id_status',
                            status_read)
        monkeypatch.setattr(jobs_utils.time, 'sleep', sleep)

        result = jobs_utils._wait_for_next_task(job_id=42, current_task_id=0)

        assert result == (1, managed_job_state.ManagedJobStatus.RUNNING)
        assert status_read.call_args_list == [mock.call(42), mock.call(42)]
        sleep.assert_called_once_with(jobs_utils.JOB_STATUS_CHECK_GAP_SECONDS)


class TestStreamLogsByIdLifecycle:
    """Checks integration with the full managed-job log follower."""

    def test_terminal_transition_between_tasks_ends_follow(self, monkeypatch):
        backend = _FakeBackend()
        status_read = mock.Mock(side_effect=[
            managed_job_state.ManagedJobStatus.RUNNING,
            managed_job_state.ManagedJobStatus.FAILED,
        ])
        latest_status_read = mock.Mock(side_effect=[
            (0, managed_job_state.ManagedJobStatus.RUNNING),
            (0, managed_job_state.ManagedJobStatus.FAILED),
        ])
        sleep = mock.Mock()
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks',
                            mock.Mock(return_value=2))
        monkeypatch.setattr(managed_job_state, 'get_status', status_read)
        monkeypatch.setattr(managed_job_state, 'get_latest_task_id_status',
                            latest_status_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(managed_job_state, 'get_pool_from_job_id',
                            mock.Mock(return_value=None))
        monkeypatch.setattr(managed_job_state, 'get_task_name',
                            mock.Mock(return_value='first'))
        monkeypatch.setattr(jobs_utils, 'generate_managed_job_cluster_name',
                            mock.Mock(return_value='cluster'))
        monkeypatch.setattr(jobs_utils.global_user_state,
                            'get_handle_from_cluster_name',
                            mock.Mock(return_value=_FakeHandle()))
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayResourceHandle',
                            _FakeHandle)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayBackend',
                            mock.Mock(return_value=backend))
        monkeypatch.setattr(jobs_utils.managed_job_runtime, 'is_registered',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils.time, 'sleep', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42, follow=True)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.from_managed_job_status(
            managed_job_state.ManagedJobStatus.FAILED)
        assert latest_status_read.call_count == 2
        assert backend.tail_calls == 1
        assert backend.status_calls == 1
        sleep.assert_not_called()
