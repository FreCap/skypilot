"""Managed-job log-follow lifecycle regression tests."""

# pylint: disable=protected-access

from unittest import mock

import pytest

from sky import exceptions
from sky.jobs import state as managed_job_state
from sky.jobs import utils as jobs_utils


class _FakeHandle:
    """Minimal resource handle accepted by the stream implementation."""


class _FakeBackend:
    """Records the remote log and status calls made by one follow cycle."""

    def __init__(self):
        self.tail_calls = 0
        self.tail_kwargs = None
        self.status_calls = 0

    def tail_logs(self, *args, **kwargs):
        del args
        self.tail_calls += 1
        self.tail_kwargs = kwargs
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

    @pytest.mark.parametrize(
        ('context', 'expected_cluster', 'expected_pool_job_id',
         'expected_tail_calls'), [
             ((None, None, None, 'first'), 'cluster', None, 1),
             (('pool-a', 'pool-cluster', 73, 'first'), 'pool-cluster', 73, 1),
             ((None, None, None, None), None, None, 0),
         ])
    def test_terminal_transition_between_tasks_ends_follow(
            self, monkeypatch, context, expected_cluster, expected_pool_job_id,
            expected_tail_calls):
        backend = _FakeBackend()
        status_read = mock.Mock(
            side_effect=AssertionError('scalar status poll used'))
        latest_status_read = mock.Mock(side_effect=[
            (0, managed_job_state.ManagedJobStatus.RUNNING),
            (0, managed_job_state.ManagedJobStatus.FAILED),
        ])
        num_tasks_read = mock.Mock(return_value=2)
        sleep = mock.Mock()
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        context_read = mock.Mock(return_value=context)
        generate_cluster_name = mock.Mock(return_value='cluster')
        handle_lookup = mock.Mock(return_value=_FakeHandle())

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks', num_tasks_read)
        monkeypatch.setattr(managed_job_state, 'get_status', status_read)
        monkeypatch.setattr(managed_job_state, 'get_latest_task_id_status',
                            latest_status_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(managed_job_state, 'get_log_stream_context',
                            context_read)
        monkeypatch.setattr(
            managed_job_state, 'get_pool_from_job_id',
            mock.Mock(side_effect=AssertionError('scalar pool read used')))
        monkeypatch.setattr(
            managed_job_state, 'get_pool_submit_info',
            mock.Mock(
                side_effect=AssertionError('scalar pool target read used')))
        monkeypatch.setattr(
            managed_job_state, 'get_task_name',
            mock.Mock(side_effect=AssertionError('scalar task read used')))
        monkeypatch.setattr(jobs_utils, 'generate_managed_job_cluster_name',
                            generate_cluster_name)
        monkeypatch.setattr(jobs_utils.global_user_state,
                            'get_handle_from_cluster_name', handle_lookup)
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
        num_tasks_read.assert_called_once_with(42)
        status_read.assert_not_called()
        context_read.assert_called_once_with(42, 0)
        if expected_cluster is None:
            handle_lookup.assert_not_called()
        else:
            handle_lookup.assert_called_once_with(expected_cluster)
        if context[0] is None and context[3] is not None:
            generate_cluster_name.assert_called_once_with('first', 42)
        else:
            generate_cluster_name.assert_not_called()
        assert backend.tail_calls == expected_tail_calls
        assert backend.status_calls == expected_tail_calls
        if expected_tail_calls:
            assert backend.tail_kwargs is not None
            assert backend.tail_kwargs['job_id'] == expected_pool_job_id
            sleep.assert_not_called()
        else:
            assert (sleep.call_count == jobs_utils.JOB_STATUS_CHECK_GAP_SECONDS)

    def test_terminal_task_filter_refreshes_immediately_stale_snapshot(
            self, monkeypatch, tmp_path):
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        log_path = tmp_path / 'task.log'
        log_path.write_text('waiting\n')
        initial_rows = [(1, 'eval', managed_job_state.ManagedJobStatus.RUNNING,
                         '', None)]
        terminal_rows = [(1, 'eval',
                          managed_job_state.ManagedJobStatus.SUCCEEDED,
                          str(log_path), None)]
        latest_status_read = mock.Mock(
            return_value=(1, managed_job_state.ManagedJobStatus.SUCCEEDED))
        status_read = mock.Mock(
            side_effect=AssertionError('scalar status poll used'))
        get_num_tasks = mock.Mock(return_value=1)
        task_info_read = mock.Mock(side_effect=[initial_rows, terminal_rows])
        sleep = mock.Mock()

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks', get_num_tasks)
        monkeypatch.setattr(managed_job_state,
                            'get_all_task_ids_names_statuses_logs',
                            task_info_read)
        monkeypatch.setattr(managed_job_state, 'get_status', status_read)
        monkeypatch.setattr(managed_job_state, 'get_latest_task_id_status',
                            latest_status_read)
        monkeypatch.setattr(jobs_utils.time, 'sleep', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42,
                                                          follow=False,
                                                          task='eval')

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        get_num_tasks.assert_not_called()
        status_read.assert_not_called()
        assert task_info_read.call_args_list == [mock.call(42), mock.call(42)]
        latest_status_read.assert_called_once_with(42)
        sleep.assert_not_called()

    def test_terminal_task_filter_refreshes_snapshot_after_wait(
            self, monkeypatch, tmp_path):
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        log_path = tmp_path / 'task.log'
        log_path.write_text('waiting\n')
        initial_rows = [(1, 'eval', managed_job_state.ManagedJobStatus.RUNNING,
                         '', None)]
        terminal_rows = [(1, 'eval',
                          managed_job_state.ManagedJobStatus.SUCCEEDED,
                          str(log_path), None)]
        latest_status_read = mock.Mock(side_effect=[
            (1, None),
            (1, managed_job_state.ManagedJobStatus.SUCCEEDED),
        ])
        status_read = mock.Mock(
            side_effect=AssertionError('scalar status poll used'))
        get_num_tasks = mock.Mock(return_value=1)
        task_info_read = mock.Mock(side_effect=[initial_rows, terminal_rows])
        sleep = mock.Mock()

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks', get_num_tasks)
        monkeypatch.setattr(managed_job_state,
                            'get_all_task_ids_names_statuses_logs',
                            task_info_read)
        monkeypatch.setattr(managed_job_state, 'get_status', status_read)
        monkeypatch.setattr(managed_job_state, 'get_latest_task_id_status',
                            latest_status_read)
        monkeypatch.setattr(jobs_utils.time, 'sleep', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42,
                                                          follow=False,
                                                          task='eval')

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        get_num_tasks.assert_not_called()
        status_read.assert_not_called()
        assert task_info_read.call_args_list == [mock.call(42), mock.call(42)]
        assert latest_status_read.call_args_list == [
            mock.call(42), mock.call(42)
        ]
        sleep.assert_called_once_with(1)
