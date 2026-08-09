"""Managed-job log-follow lifecycle regression tests."""

# pylint: disable=protected-access

import asyncio
from typing import Any
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


def test_stream_logs_facade_preserves_id_dispatch_and_arguments(monkeypatch):
    """The generated-command entrypoint must keep its historical dispatch."""
    stream_by_id = mock.Mock(return_value=('streamed', 0))
    monkeypatch.setattr(jobs_utils, 'stream_logs_by_id', stream_by_id)

    result = jobs_utils.stream_logs(job_id=42,
                                    job_name='ignored-name',
                                    follow=False,
                                    tail=17,
                                    tail_offset=9,
                                    task='eval')

    assert result == ('streamed', 0)
    stream_by_id.assert_called_once_with(42, False, 17, 9, 'eval')


class TestWaitForNextTask:
    """Checks the lifecycle and polling boundaries between JobGroup tasks."""

    def test_terminal_status_ends_wait_without_sleep(self, monkeypatch):
        terminal_snapshot = managed_job_state.JobLogStreamSnapshot(
            0, managed_job_state.ManagedJobStatus.FAILED, None, None, None,
            None)
        snapshot_read = mock.Mock(return_value=terminal_snapshot)
        sleep = mock.Mock()
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        result = jobs_utils._wait_for_next_task(job_id=42, current_task_id=0)

        assert result == terminal_snapshot
        snapshot_read.assert_called_once_with(42)
        sleep.assert_not_called()

    def test_cancelling_status_ends_wait_without_sleep(self, monkeypatch):
        cancelling_snapshot = managed_job_state.JobLogStreamSnapshot(
            0, managed_job_state.ManagedJobStatus.CANCELLING, None, None, None,
            None)
        snapshot_read = mock.Mock(return_value=cancelling_snapshot)
        sleep = mock.Mock()
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        result = jobs_utils._wait_for_next_task(job_id=42, current_task_id=0)

        assert result == cancelling_snapshot
        snapshot_read.assert_called_once_with(42)
        sleep.assert_not_called()

    def test_running_status_waits_for_next_task(self, monkeypatch):
        current_snapshot = managed_job_state.JobLogStreamSnapshot(
            0, managed_job_state.ManagedJobStatus.RUNNING, None, None, None,
            'first')
        next_snapshot = managed_job_state.JobLogStreamSnapshot(
            1, managed_job_state.ManagedJobStatus.RUNNING, 'pool-a',
            'pool-cluster', 73, 'second')
        snapshot_read = mock.Mock(side_effect=[
            current_snapshot,
            next_snapshot,
        ])
        sleep = mock.Mock()
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        result = jobs_utils._wait_for_next_task(job_id=42, current_task_id=0)

        assert result == next_snapshot
        assert snapshot_read.call_args_list == [mock.call(42), mock.call(42)]
        sleep.assert_called_once_with(jobs_utils.JOB_STATUS_CHECK_GAP_SECONDS)

    def test_cancellation_stops_wait_before_next_snapshot(self, monkeypatch):
        current_snapshot = managed_job_state.JobLogStreamSnapshot(
            0, managed_job_state.ManagedJobStatus.RUNNING, None, None, None,
            'first')
        snapshot_read = mock.Mock(side_effect=[
            current_snapshot,
            AssertionError('re-read snapshot after cancellation'),
        ])
        wait = mock.Mock(side_effect=asyncio.CancelledError())
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(jobs_utils,
                            '_sleep_log_follow_wait',
                            wait,
                            raising=False)
        monkeypatch.setattr(
            jobs_utils.time, 'sleep',
            mock.Mock(side_effect=AssertionError('used raw sleep')))

        with pytest.raises(asyncio.CancelledError):
            jobs_utils._wait_for_next_task(job_id=42, current_task_id=0)

        snapshot_read.assert_called_once_with(42)
        wait.assert_called_once_with(jobs_utils.JOB_STATUS_CHECK_GAP_SECONDS)


class TestInitialLogStreamSnapshot:
    """Checks the wait loop before the first stream-target snapshot appears."""

    def test_cancellation_stops_before_second_snapshot(self, monkeypatch):
        snapshot_read = mock.Mock(side_effect=[
            managed_job_state.JobLogStreamSnapshot(1, None, None, None, None,
                                                   'first'),
            AssertionError('re-read snapshot after cancellation'),
        ])
        wait = mock.Mock(side_effect=asyncio.CancelledError())
        monkeypatch.setattr(jobs_utils,
                            '_sleep_log_follow_wait',
                            wait,
                            raising=False)
        monkeypatch.setattr(
            jobs_utils.time, 'sleep',
            mock.Mock(side_effect=AssertionError('used raw sleep')))

        with pytest.raises(asyncio.CancelledError):
            jobs_utils._wait_for_initial_log_stream_snapshot(snapshot_read)

        snapshot_read.assert_called_once_with()
        wait.assert_called_once_with(1)


class TestStreamLogsByIdLifecycle:
    """Checks integration with the full managed-job log follower."""

    @pytest.mark.parametrize('task_filter', [None, 5, 'eval'])
    def test_batch_jobs_delegate_directly_to_controller_logs(
            self, monkeypatch, task_filter):
        batch_stream = mock.Mock(return_value=('controller-log', 0))

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(
            jobs_utils.rich_utils, 'safe_status',
            mock.Mock(side_effect=AssertionError(
                'status display should not start for batch '
                'log routing')))
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=True))
        monkeypatch.setattr(
            managed_job_state, 'get_num_tasks',
            mock.Mock(side_effect=AssertionError('task count queried')))
        monkeypatch.setattr(
            managed_job_state, 'get_all_task_ids_names_statuses_logs',
            mock.Mock(side_effect=AssertionError('task rows scanned')))
        monkeypatch.setattr(
            managed_job_state, 'get_task_id_name_status_log',
            mock.Mock(side_effect=AssertionError('task row queried')))
        monkeypatch.setattr(
            managed_job_state, 'get_latest_log_stream_snapshot',
            mock.Mock(side_effect=AssertionError('latest snapshot queried')))
        monkeypatch.setattr(
            managed_job_state, 'get_task_log_stream_snapshot',
            mock.Mock(side_effect=AssertionError('task snapshot queried')))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(side_effect=AssertionError('whole-job status queried')))
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('scalar latest-task queried')))
        monkeypatch.setattr(jobs_utils, 'stream_logs', batch_stream)

        message, exit_code = jobs_utils.stream_logs_by_id(42,
                                                          follow=False,
                                                          tail=17,
                                                          tail_offset=3,
                                                          task=task_filter)

        assert message == 'controller-log'
        assert exit_code == 0
        batch_stream.assert_called_once_with(42,
                                             job_name=None,
                                             controller=True,
                                             follow=False,
                                             tail=17,
                                             tail_offset=3)

    @pytest.mark.parametrize(
        ('status', 'cluster_name', 'expected_handle_calls'), [
            (managed_job_state.ManagedJobStatus.PENDING, None, 0),
            (managed_job_state.ManagedJobStatus.STARTING, None, 0),
            (managed_job_state.ManagedJobStatus.RECOVERING, None, 0),
        ])
    def test_no_follow_returns_nonstreamable_snapshot_without_waiting(
            self, monkeypatch, status, cluster_name, expected_handle_calls):
        backend: Any = _FakeBackend()
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        snapshot_read = mock.Mock(
            return_value=managed_job_state.JobLogStreamSnapshot(
                0, status, None, cluster_name, None, None))
        handle_lookup = mock.Mock(return_value=None)

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks',
                            mock.Mock(return_value=1))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(side_effect=AssertionError('scalar status poll used')))
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils.global_user_state,
                            'get_handle_from_cluster_name', handle_lookup)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayResourceHandle',
                            _FakeHandle)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayBackend',
                            mock.Mock(return_value=backend))
        monkeypatch.setattr(
            jobs_utils, '_sleep_log_follow_wait',
            mock.Mock(side_effect=AssertionError('snapshot mode waited')))

        message, exit_code = jobs_utils.stream_logs_by_id(42, follow=False)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        snapshot_read.assert_called_once_with(42)
        assert handle_lookup.call_count == expected_handle_calls
        assert backend.tail_calls == 0
        assert backend.status_calls == 0

    def test_no_follow_refreshes_stale_missing_stream_target_to_terminal_logs(
            self, monkeypatch, tmp_path, capsys):
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        log_path = tmp_path / 'task.log'
        log_path.write_text('finished\n', encoding='utf-8')
        snapshot_read = mock.Mock(side_effect=[
            managed_job_state.JobLogStreamSnapshot(
                0, managed_job_state.ManagedJobStatus.RUNNING, None,
                'missing-cluster', None, 'first'),
            managed_job_state.JobLogStreamSnapshot(
                0, managed_job_state.ManagedJobStatus.SUCCEEDED, None, None,
                None, 'first'),
        ])
        task_rows_read = mock.Mock(return_value=[
            (0, 'first', managed_job_state.ManagedJobStatus.SUCCEEDED,
             str(log_path), None),
        ])
        backend = _FakeBackend()
        handle_lookup = mock.Mock(return_value=None)
        generate_cluster_name = mock.Mock(return_value='generated-cluster')

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks',
                            mock.Mock(return_value=1))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(side_effect=AssertionError('scalar status poll used')))
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(managed_job_state,
                            'get_all_task_ids_names_statuses_logs',
                            task_rows_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils, 'generate_managed_job_cluster_name',
                            generate_cluster_name)
        monkeypatch.setattr(jobs_utils.global_user_state,
                            'get_handle_from_cluster_name', handle_lookup)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayResourceHandle',
                            _FakeHandle)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayBackend',
                            mock.Mock(return_value=backend))
        monkeypatch.setattr(
            jobs_utils, '_sleep_log_follow_wait',
            mock.Mock(side_effect=AssertionError('snapshot mode waited')))

        message, exit_code = jobs_utils.stream_logs_by_id(42, follow=False)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        assert snapshot_read.call_args_list == [mock.call(42), mock.call(42)]
        task_rows_read.assert_called_once_with(42)
        generate_cluster_name.assert_called_once_with('first', 42)
        handle_lookup.assert_called_once_with('generated-cluster')
        assert backend.tail_calls == 0
        assert backend.status_calls == 0
        output = capsys.readouterr().out
        assert 'Job finished (status: SUCCEEDED).' in output

    def test_no_follow_missing_stream_target_still_returns_empty_fast(
            self, monkeypatch):
        backend = _FakeBackend()
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        snapshot_read = mock.Mock(side_effect=[
            managed_job_state.JobLogStreamSnapshot(
                0, managed_job_state.ManagedJobStatus.RUNNING, None,
                'missing-cluster', None, 'first'),
            managed_job_state.JobLogStreamSnapshot(
                0, managed_job_state.ManagedJobStatus.RUNNING, None,
                'missing-cluster', None, 'first'),
        ])
        handle_lookup = mock.Mock(return_value=None)
        task_rows_read = mock.Mock(
            side_effect=AssertionError('read terminal rows for running job'))
        generate_cluster_name = mock.Mock(return_value='generated-cluster')

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks',
                            mock.Mock(return_value=1))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(side_effect=AssertionError('scalar status poll used')))
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(managed_job_state,
                            'get_all_task_ids_names_statuses_logs',
                            task_rows_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils, 'generate_managed_job_cluster_name',
                            generate_cluster_name)
        monkeypatch.setattr(jobs_utils.global_user_state,
                            'get_handle_from_cluster_name', handle_lookup)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayResourceHandle',
                            _FakeHandle)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayBackend',
                            mock.Mock(return_value=backend))
        monkeypatch.setattr(
            jobs_utils, '_sleep_log_follow_wait',
            mock.Mock(side_effect=AssertionError('snapshot mode waited')))

        message, exit_code = jobs_utils.stream_logs_by_id(42, follow=False)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        assert snapshot_read.call_args_list == [mock.call(42), mock.call(42)]
        generate_cluster_name.assert_called_once_with('first', 42)
        handle_lookup.assert_called_once_with('generated-cluster')
        assert backend.tail_calls == 0
        assert backend.status_calls == 0

    def test_next_task_handoff_reuses_detecting_snapshot(self, monkeypatch):
        backend = mock.MagicMock(spec=_FakeBackend)
        running = managed_job_state.ManagedJobStatus.RUNNING
        cancelling = managed_job_state.ManagedJobStatus.CANCELLING
        current_task = {'id': 0}
        snapshot_reads = mock.Mock()

        def snapshot_read(
                job_id: int) -> managed_job_state.JobLogStreamSnapshot:
            assert job_id == 42
            snapshot_reads()
            task_id = current_task['id']
            snapshot = managed_job_state.JobLogStreamSnapshot(
                task_id, running, 'pool-a', f'pool-cluster-{task_id}',
                70 + task_id, f'task-{task_id}')
            if task_id == 1:
                # Model a fast following transition after task 1 was observed.
                # The detecting snapshot must still own task 1's log handoff.
                current_task['id'] = 2
            return snapshot

        tailed_job_ids = []

        def tail_logs(*args, **kwargs):
            del args
            tailed_job_ids.append(kwargs['job_id'])
            if len(tailed_job_ids) == 1:
                current_task['id'] = 1
            return exceptions.JobExitCode.SUCCEEDED.value

        backend.tail_logs = mock.Mock(side_effect=tail_logs)
        backend.get_job_status = mock.Mock(side_effect=[
            {
                1: jobs_utils.job_lib.JobStatus.SUCCEEDED
            },
            {
                1: jobs_utils.job_lib.JobStatus.CANCELLED
            },
        ])
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        handle_lookup = mock.Mock(return_value=_FakeHandle())

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks',
                            mock.Mock(return_value=3))
        monkeypatch.setattr(managed_job_state, 'get_status',
                            mock.Mock(return_value=cancelling))
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('scalar latest-task poll '
                                                 'used')))
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils.global_user_state,
                            'get_handle_from_cluster_name', handle_lookup)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayResourceHandle',
                            _FakeHandle)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayBackend',
                            mock.Mock(return_value=backend))
        monkeypatch.setattr(jobs_utils.managed_job_runtime, 'is_registered',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', mock.Mock())

        message, exit_code = jobs_utils.stream_logs_by_id(42, follow=True)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.from_managed_job_status(
            cancelling)
        assert tailed_job_ids == [70, 71]
        assert snapshot_reads.call_count == 2
        assert handle_lookup.call_args_list == [
            mock.call('pool-cluster-0'),
            mock.call('pool-cluster-1'),
        ]

    def test_recovered_target_reuses_post_wait_snapshot(self, monkeypatch):
        backend = _FakeBackend()
        running = managed_job_state.ManagedJobStatus.RUNNING
        snapshot_read = mock.Mock(side_effect=[
            managed_job_state.JobLogStreamSnapshot(0, running, None, None, None,
                                                   None),
            managed_job_state.JobLogStreamSnapshot(0, running, 'pool-a',
                                                   'pool-cluster', 73, 'first'),
        ])
        status_read = mock.Mock(
            return_value=managed_job_state.ManagedJobStatus.SUCCEEDED)
        sleep = mock.Mock()
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        handle_lookup = mock.Mock(return_value=_FakeHandle())

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks',
                            mock.Mock(return_value=1))
        monkeypatch.setattr(managed_job_state, 'get_status', status_read)
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('scalar latest-task poll '
                                                 'used')))
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils.global_user_state,
                            'get_handle_from_cluster_name', handle_lookup)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayResourceHandle',
                            _FakeHandle)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayBackend',
                            mock.Mock(return_value=backend))
        monkeypatch.setattr(jobs_utils.managed_job_runtime, 'is_registered',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42, follow=True)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        assert snapshot_read.call_count == 2
        status_read.assert_called_once_with(42)
        handle_lookup.assert_called_once_with('pool-cluster')
        assert backend.tail_calls == 1
        assert backend.status_calls == 1
        assert sleep.call_count == jobs_utils.JOB_STATUS_CHECK_GAP_SECONDS

    def test_initial_running_snapshot_skips_scalar_status_poll(
            self, monkeypatch):
        backend = _FakeBackend()
        running = managed_job_state.ManagedJobStatus.RUNNING
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        snapshot_read = mock.Mock(
            return_value=managed_job_state.JobLogStreamSnapshot(
                0, running, 'pool-a', 'pool-cluster', 73, 'first'))
        handle_lookup = mock.Mock(return_value=_FakeHandle())

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks',
                            mock.Mock(return_value=1))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(
                return_value=managed_job_state.ManagedJobStatus.SUCCEEDED))
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('scalar latest-task poll '
                                                 'used')))
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils.global_user_state,
                            'get_handle_from_cluster_name', handle_lookup)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayResourceHandle',
                            _FakeHandle)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayBackend',
                            mock.Mock(return_value=backend))
        monkeypatch.setattr(jobs_utils.managed_job_runtime, 'is_registered',
                            mock.Mock(return_value=False))

        message, exit_code = jobs_utils.stream_logs_by_id(42, follow=True)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        snapshot_read.assert_called_once_with(42)
        handle_lookup.assert_called_once_with('pool-cluster')
        assert backend.tail_calls == 1
        assert backend.status_calls == 1

    def test_cancellation_while_log_target_is_not_ready_stops_before_refresh(
            self, monkeypatch):
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        snapshot = managed_job_state.JobLogStreamSnapshot(
            0, managed_job_state.ManagedJobStatus.STARTING, None, None, None,
            'first')
        snapshot_read = mock.Mock(side_effect=[
            snapshot,
            AssertionError('re-read snapshot after cancellation'),
        ])
        handle_lookup = mock.Mock(return_value=None)
        generate_cluster_name = mock.Mock(return_value='generated-cluster')
        backend = _FakeBackend()
        wait = mock.Mock(side_effect=asyncio.CancelledError())

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks',
                            mock.Mock(return_value=1))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(
                side_effect=AssertionError('polled status after cancellation')))
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('scalar latest-task poll '
                                                 'used')))
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils, 'read_provision_status_from_log',
                            mock.Mock(return_value=(0, None)))
        monkeypatch.setattr(jobs_utils, 'generate_managed_job_cluster_name',
                            generate_cluster_name)
        monkeypatch.setattr(jobs_utils.global_user_state,
                            'get_handle_from_cluster_name', handle_lookup)
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayBackend',
                            mock.Mock(return_value=backend))
        monkeypatch.setattr(jobs_utils,
                            '_sleep_log_follow_wait',
                            wait,
                            raising=False)
        monkeypatch.setattr(
            jobs_utils.time, 'sleep',
            mock.Mock(side_effect=AssertionError('used raw sleep')))

        with pytest.raises(asyncio.CancelledError):
            jobs_utils.stream_logs_by_id(42, follow=True)

        snapshot_read.assert_called_once_with(42)
        generate_cluster_name.assert_called_once_with('first', 42)
        handle_lookup.assert_called_once_with('generated-cluster')
        wait.assert_called_once_with(jobs_utils._PROVISION_LOG_POLL_GAP_SECONDS)
        assert backend.tail_calls == 0
        assert backend.status_calls == 0

    def test_filtered_task_snapshot_stops_on_requested_task_terminal_state(
            self, monkeypatch):
        backend = _FakeBackend()
        running = managed_job_state.ManagedJobStatus.RUNNING
        succeeded = managed_job_state.ManagedJobStatus.SUCCEEDED
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        lookup = managed_job_state.TaskLogStreamLookup(
            snapshot=managed_job_state.JobLogStreamSnapshot(
                0, succeeded, None, None, None, 'first'),
            local_log_file=None,
            logs_cleaned_at=None,
            num_tasks=2,
        )
        lookup_read = mock.Mock(side_effect=[lookup, lookup])

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(
            managed_job_state, 'get_all_task_ids_names_statuses_logs',
            mock.Mock(return_value=[
                (0, 'first', running, None, None),
                (1, 'second', running, None, None),
            ]))
        monkeypatch.setattr(
            managed_job_state, 'get_task_id_name_status_log',
            mock.Mock(side_effect=AssertionError('task row lookup used')))
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('scalar latest-task poll '
                                                 'used')))
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_lookup',
                            lookup_read)
        monkeypatch.setattr(
            managed_job_state, 'get_task_log_stream_snapshot',
            mock.Mock(side_effect=AssertionError('task snapshot poll used')))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(side_effect=AssertionError('whole-job status poll used')))
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(jobs_utils.backends, 'CloudVmRayBackend',
                            mock.Mock(return_value=backend))
        monkeypatch.setattr(jobs_utils.managed_job_runtime, 'is_registered',
                            mock.Mock(return_value=False))
        monkeypatch.setattr(
            jobs_utils, '_sleep_log_follow_wait',
            mock.Mock(side_effect=AssertionError('waited on a later task')))

        message, exit_code = jobs_utils.stream_logs_by_id(42,
                                                          follow=True,
                                                          task=0)

        assert 'terminal state SUCCEEDED' in message
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        assert lookup_read.call_args_list == [
            mock.call(42, 0), mock.call(42, 0)
        ]
        assert backend.tail_calls == 0
        assert backend.status_calls == 0

    @pytest.mark.parametrize(
        'terminal_status',
        managed_job_state.ManagedJobStatus.terminal_statuses(),
    )
    @pytest.mark.parametrize(
        ('context', 'expected_cluster', 'expected_pool_job_id',
         'expected_tail_calls'), [
             ((None, None, None, 'first'), 'cluster', None, 1),
             (('pool-a', 'pool-cluster', 73, 'first'), 'pool-cluster', 73, 1),
             ((None, None, None, None), None, None, 0),
         ])
    def test_terminal_transition_between_tasks_ends_follow(
            self, monkeypatch, context, expected_cluster, expected_pool_job_id,
            expected_tail_calls, terminal_status):
        backend = _FakeBackend()
        status_read = mock.Mock(
            side_effect=AssertionError('redundant scalar status poll used'))
        snapshot_read = mock.Mock(side_effect=[
            managed_job_state.JobLogStreamSnapshot(
                0,
                managed_job_state.ManagedJobStatus.RUNNING,
                context[0],
                context[1],
                context[2],
                context[3],
            ),
            managed_job_state.JobLogStreamSnapshot(
                0,
                terminal_status,
                context[0],
                context[1],
                context[2],
                context[3],
            ),
        ])
        num_tasks_read = mock.Mock(return_value=2)
        sleep = mock.Mock()
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        context_read = mock.Mock(
            side_effect=AssertionError('scalar log context read used'))
        generate_cluster_name = mock.Mock(return_value='cluster')
        handle_lookup = mock.Mock(return_value=_FakeHandle())

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks', num_tasks_read)
        monkeypatch.setattr(managed_job_state, 'get_status', status_read)
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('scalar latest-task poll '
                                                 'used')))
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
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
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42, follow=True)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.from_managed_job_status(
            terminal_status)
        assert snapshot_read.call_count == 2
        num_tasks_read.assert_called_once_with(42)
        status_read.assert_not_called()
        context_read.assert_not_called()
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

    def test_filtered_task_final_wait_uses_task_status_scope(self, monkeypatch):
        backend = _FakeBackend()
        running = managed_job_state.ManagedJobStatus.RUNNING
        succeeded = managed_job_state.ManagedJobStatus.SUCCEEDED
        lookup_read = mock.Mock(
            return_value=managed_job_state.TaskLogStreamLookup(
                snapshot=managed_job_state.JobLogStreamSnapshot(
                    1, running, None, 'filtered-cluster', None, 'eval'),
                local_log_file=None,
                logs_cleaned_at=None,
                num_tasks=2,
            ))
        filtered_snapshot_read = mock.Mock(side_effect=[
            managed_job_state.JobLogStreamSnapshot(
                1, running, None, 'filtered-cluster', None, 'eval'),
            managed_job_state.JobLogStreamSnapshot(
                1, succeeded, None, 'filtered-cluster', None, 'eval'),
        ])
        sleep = mock.Mock()
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        handle_lookup = mock.Mock(return_value=_FakeHandle())
        generate_cluster_name = mock.Mock(return_value='filtered-cluster')

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(
            managed_job_state, 'get_all_task_ids_names_statuses_logs',
            mock.Mock(side_effect=AssertionError('whole-task scan used')))
        monkeypatch.setattr(
            managed_job_state, 'get_num_tasks',
            mock.Mock(side_effect=AssertionError(
                'count query used on found task-id filter')))
        monkeypatch.setattr(
            managed_job_state, 'get_task_id_name_status_log',
            mock.Mock(side_effect=AssertionError('task row lookup used')))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(side_effect=AssertionError('whole-job status poll used')))
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('scalar latest-task poll '
                                                 'used')))
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_lookup',
                            lookup_read)
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_snapshot',
                            filtered_snapshot_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
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
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42,
                                                          follow=True,
                                                          task=1)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        lookup_read.assert_called_once_with(42, 1)
        assert filtered_snapshot_read.call_args_list == [
            mock.call(42, 1),
            mock.call(42, 1),
        ]
        generate_cluster_name.assert_called_once_with('eval', 42)
        handle_lookup.assert_called_once_with('filtered-cluster')
        assert backend.tail_calls == 1
        assert backend.status_calls == 1
        sleep.assert_called_once_with(1)

    def test_filtered_task_retry_refresh_uses_task_status_scope(
            self, monkeypatch):
        backend = _FakeBackend()
        running = managed_job_state.ManagedJobStatus.RUNNING
        succeeded = managed_job_state.ManagedJobStatus.SUCCEEDED
        lookup_read = mock.Mock(
            return_value=managed_job_state.TaskLogStreamLookup(
                snapshot=managed_job_state.JobLogStreamSnapshot(
                    1, running, None, 'filtered-cluster', None, 'eval'),
                local_log_file=None,
                logs_cleaned_at=None,
                num_tasks=2,
            ))
        filtered_snapshot_read = mock.Mock(side_effect=[
            managed_job_state.JobLogStreamSnapshot(
                1, running, None, 'filtered-cluster', None, 'eval'),
            managed_job_state.JobLogStreamSnapshot(
                1, succeeded, None, 'filtered-cluster', None, 'eval'),
        ])
        sleep = mock.Mock()
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        handle_lookup = mock.Mock(return_value=_FakeHandle())
        generate_cluster_name = mock.Mock(return_value='filtered-cluster')
        backend.tail_logs = mock.Mock(return_value=255)

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(
            managed_job_state, 'get_all_task_ids_names_statuses_logs',
            mock.Mock(side_effect=AssertionError('whole-task scan used')))
        monkeypatch.setattr(
            managed_job_state, 'get_num_tasks',
            mock.Mock(side_effect=AssertionError(
                'count query used on found task-id filter')))
        monkeypatch.setattr(
            managed_job_state, 'get_task_id_name_status_log',
            mock.Mock(side_effect=AssertionError('task row lookup used')))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(side_effect=AssertionError('whole-job status poll used')))
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('scalar latest-task poll '
                                                 'used')))
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_lookup',
                            lookup_read)
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_snapshot',
                            filtered_snapshot_read)
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
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
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42,
                                                          follow=True,
                                                          task=1)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        lookup_read.assert_called_once_with(42, 1)
        assert filtered_snapshot_read.call_args_list == [
            mock.call(42, 1),
            mock.call(42, 1),
        ]
        generate_cluster_name.assert_called_once_with('eval', 42)
        handle_lookup.assert_called_once_with('filtered-cluster')
        backend.tail_logs.assert_called_once()
        assert backend.status_calls == 0
        sleep.assert_called_once_with(3 *
                                      jobs_utils.JOB_STATUS_CHECK_GAP_SECONDS)

    def test_unfiltered_final_wait_keeps_whole_job_status_poll(
            self, monkeypatch):
        backend = _FakeBackend()
        running = managed_job_state.ManagedJobStatus.RUNNING
        succeeded = managed_job_state.ManagedJobStatus.SUCCEEDED
        snapshot_read = mock.Mock(
            return_value=managed_job_state.JobLogStreamSnapshot(
                0, running, None, 'cluster', None, 'first'))
        status_read = mock.Mock(side_effect=[running, succeeded])
        sleep = mock.Mock()
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        handle_lookup = mock.Mock(return_value=_FakeHandle())
        generate_cluster_name = mock.Mock(return_value='cluster')

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks',
                            mock.Mock(return_value=1))
        monkeypatch.setattr(managed_job_state, 'get_status', status_read)
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('scalar latest-task poll '
                                                 'used')))
        monkeypatch.setattr(managed_job_state, 'get_latest_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(
            managed_job_state, 'get_task_log_stream_snapshot',
            mock.Mock(side_effect=AssertionError('task snapshot poll used for '
                                                 'unfiltered final wait')))
        monkeypatch.setattr(managed_job_state, 'is_batch_job',
                            mock.Mock(return_value=False))
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
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42, follow=True)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        snapshot_read.assert_called_once_with(42)
        assert status_read.call_args_list == [mock.call(42), mock.call(42)]
        generate_cluster_name.assert_called_once_with('first', 42)
        handle_lookup.assert_called_once_with('cluster')
        assert backend.tail_calls == 1
        assert backend.status_calls == 1
        sleep.assert_called_once_with(1)

    def test_terminal_task_filter_refreshes_immediately_stale_snapshot(
            self, monkeypatch, tmp_path):
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        log_path = tmp_path / 'task.log'
        log_path.write_text('waiting\n')
        initial_rows = [(1, 'eval', managed_job_state.ManagedJobStatus.RUNNING,
                         '', None)]
        status_read = mock.Mock(
            side_effect=AssertionError('scalar status poll used'))
        get_num_tasks = mock.Mock(return_value=1)
        task_info_read = mock.Mock(return_value=initial_rows)
        lookup_read = mock.Mock(
            return_value=managed_job_state.TaskLogStreamLookup(
                snapshot=managed_job_state.JobLogStreamSnapshot(
                    1, managed_job_state.ManagedJobStatus.SUCCEEDED, None, None,
                    None, 'eval'),
                local_log_file=str(log_path),
                logs_cleaned_at=None,
                num_tasks=1,
            ))
        sleep = mock.Mock()
        snapshot_read = mock.Mock(
            return_value=managed_job_state.JobLogStreamSnapshot(
                1, managed_job_state.ManagedJobStatus.SUCCEEDED, None, None,
                None, 'eval'))

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks', get_num_tasks)
        monkeypatch.setattr(managed_job_state,
                            'get_all_task_ids_names_statuses_logs',
                            task_info_read)
        monkeypatch.setattr(
            managed_job_state, 'get_task_id_name_status_log',
            mock.Mock(side_effect=AssertionError('task row lookup used')))
        monkeypatch.setattr(managed_job_state, 'get_status', status_read)
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('whole-job status poll used')))
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_lookup',
                            lookup_read)
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42,
                                                          follow=False,
                                                          task='eval')

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        get_num_tasks.assert_not_called()
        status_read.assert_not_called()
        snapshot_read.assert_called_once_with(42, 1)
        task_info_read.assert_called_once_with(42)
        lookup_read.assert_called_once_with(42, 1)
        sleep.assert_not_called()

    def test_terminal_task_id_filter_skips_whole_task_scan(
            self, monkeypatch, tmp_path):
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        log_path = tmp_path / 'task.log'
        log_path.write_text('waiting\n')
        lookup = managed_job_state.TaskLogStreamLookup(
            snapshot=managed_job_state.JobLogStreamSnapshot(
                1, managed_job_state.ManagedJobStatus.SUCCEEDED, None, None,
                None, 'eval'),
            local_log_file=str(log_path),
            logs_cleaned_at=None,
            num_tasks=1,
        )
        lookup_read = mock.Mock(side_effect=[lookup, lookup])
        sleep = mock.Mock()

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(
            managed_job_state, 'get_all_task_ids_names_statuses_logs',
            mock.Mock(side_effect=AssertionError('whole-task scan used')))
        monkeypatch.setattr(
            managed_job_state, 'get_num_tasks',
            mock.Mock(side_effect=AssertionError(
                'count query used on found task-id filter')))
        monkeypatch.setattr(
            managed_job_state, 'get_task_id_name_status_log',
            mock.Mock(side_effect=AssertionError('task row lookup used')))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(side_effect=AssertionError('scalar status poll used')))
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('whole-job status poll used')))
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_lookup',
                            lookup_read)
        monkeypatch.setattr(
            managed_job_state, 'get_task_log_stream_snapshot',
            mock.Mock(side_effect=AssertionError('task snapshot poll used')))
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42,
                                                          follow=False,
                                                          task=1)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        assert lookup_read.call_args_list == [
            mock.call(42, 1), mock.call(42, 1)
        ]
        sleep.assert_not_called()

    def test_terminal_task_id_filter_preserves_missing_job(self, monkeypatch):
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        count_read = mock.Mock(return_value=0)
        lookup_read = mock.Mock(side_effect=[
            managed_job_state.TaskLogStreamLookup(
                snapshot=managed_job_state.JobLogStreamSnapshot(
                    1, managed_job_state.ManagedJobStatus.SUCCEEDED, None, None,
                    None, 'eval'),
                local_log_file=None,
                logs_cleaned_at=None,
                num_tasks=1,
            ),
            managed_job_state.TaskLogStreamLookup(
                snapshot=managed_job_state.JobLogStreamSnapshot(
                    None, None, None, None, None, None),
                local_log_file=None,
                logs_cleaned_at=None,
                num_tasks=0,
            ),
        ])
        sleep = mock.Mock()

        monkeypatch.setattr(jobs_utils.threading, 'Thread', mock.Mock())
        monkeypatch.setattr(jobs_utils.select, 'select',
                            mock.Mock(return_value=([], [], [])))
        monkeypatch.setattr(jobs_utils.rich_utils, 'safe_status',
                            mock.Mock(return_value=status_display))
        monkeypatch.setattr(
            managed_job_state, 'get_all_task_ids_names_statuses_logs',
            mock.Mock(side_effect=AssertionError('whole-task scan used')))
        monkeypatch.setattr(managed_job_state, 'get_num_tasks', count_read)
        monkeypatch.setattr(
            managed_job_state, 'get_task_id_name_status_log',
            mock.Mock(side_effect=AssertionError('task row lookup used')))
        monkeypatch.setattr(
            managed_job_state, 'get_status',
            mock.Mock(side_effect=AssertionError('scalar status poll used')))
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('whole-job status poll used')))
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_lookup',
                            lookup_read)
        monkeypatch.setattr(
            managed_job_state, 'get_task_log_stream_snapshot',
            mock.Mock(side_effect=AssertionError('task snapshot poll used')))
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42,
                                                          follow=False,
                                                          task=1)

        assert message == 'No task found matching 1 in job 42. Valid task IDs are 0.'
        assert exit_code == exceptions.JobExitCode.NOT_FOUND
        assert lookup_read.call_args_list == [
            mock.call(42, 1), mock.call(42, 1)
        ]
        count_read.assert_not_called()
        sleep.assert_not_called()

    def test_terminal_task_filter_refreshes_snapshot_after_wait(
            self, monkeypatch, tmp_path):
        status_display = mock.MagicMock()
        status_display.__enter__.return_value = status_display
        log_path = tmp_path / 'task.log'
        log_path.write_text('waiting\n')
        initial_rows = [(1, 'eval', managed_job_state.ManagedJobStatus.RUNNING,
                         '', None)]
        status_read = mock.Mock(
            side_effect=AssertionError('scalar status poll used'))
        get_num_tasks = mock.Mock(return_value=1)
        task_info_read = mock.Mock(return_value=initial_rows)
        lookup_read = mock.Mock(
            return_value=managed_job_state.TaskLogStreamLookup(
                snapshot=managed_job_state.JobLogStreamSnapshot(
                    1, managed_job_state.ManagedJobStatus.SUCCEEDED, None, None,
                    None, 'eval'),
                local_log_file=str(log_path),
                logs_cleaned_at=None,
                num_tasks=1,
            ))
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
        monkeypatch.setattr(
            managed_job_state, 'get_task_id_name_status_log',
            mock.Mock(side_effect=AssertionError('task row lookup used')))
        monkeypatch.setattr(managed_job_state, 'get_status', status_read)
        monkeypatch.setattr(
            managed_job_state, 'get_latest_task_id_status',
            mock.Mock(side_effect=AssertionError('whole-job status poll used')))
        snapshot_read = mock.Mock(side_effect=[
            managed_job_state.JobLogStreamSnapshot(1, None, None, None, None,
                                                   'eval'),
            managed_job_state.JobLogStreamSnapshot(
                1, managed_job_state.ManagedJobStatus.SUCCEEDED, None, None,
                None, 'eval'),
        ])
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_snapshot',
                            snapshot_read)
        monkeypatch.setattr(managed_job_state, 'get_task_log_stream_lookup',
                            lookup_read)
        monkeypatch.setattr(jobs_utils, '_sleep_log_follow_wait', sleep)

        message, exit_code = jobs_utils.stream_logs_by_id(42,
                                                          follow=False,
                                                          task='eval')

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
        get_num_tasks.assert_not_called()
        status_read.assert_not_called()
        task_info_read.assert_called_once_with(42)
        lookup_read.assert_called_once_with(42, 1)
        assert snapshot_read.call_args_list == [
            mock.call(42, 1), mock.call(42, 1)
        ]
        sleep.assert_called_once_with(1)
