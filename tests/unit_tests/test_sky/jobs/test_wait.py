"""Unit tests for sky.jobs.server.core.wait()."""
# pylint: disable=missing-class-docstring,unused-argument
import time
from typing import Optional
from unittest import mock

import pytest

from sky import exceptions
from sky.jobs import state as managed_job_state
from sky.jobs.server import core as jobs_core
from sky.jobs.state import ManagedJobStatus
from sky.schemas.api import responses


def _make_record(
    job_id: int,
    status: Optional[ManagedJobStatus],
    job_name: str = 'train',
    task_id: int = 0,
    task_name: Optional[str] = None,
) -> responses.ManagedJobRecord:
    return responses.ManagedJobRecord(
        job_id=job_id,
        job_name=job_name,
        status=status,
        task_id=task_id,
        task_name=task_name or job_name,
    )


def _make_task_lookup(
    status: Optional[ManagedJobStatus],
    *,
    task_id: Optional[int] = 0,
    task_name: Optional[str] = 'train',
    num_tasks: int = 1,
) -> managed_job_state.TaskLogStreamLookup:
    return managed_job_state.TaskLogStreamLookup(
        snapshot=managed_job_state.JobLogStreamSnapshot(task_id, status, None,
                                                        None, None, task_name),
        local_log_file=None,
        logs_cleaned_at=None,
        num_tasks=num_tasks,
    )


# ──────────────────────────────────────────────────────────────────────
# Validation tests
# ──────────────────────────────────────────────────────────────────────


class TestWaitValidation:

    def test_both_name_and_job_id_raises(self):
        with pytest.raises(ValueError, match='Cannot specify both'):
            jobs_core.wait(name='foo', job_id=1, timeout=None, poll_interval=15)

    def test_neither_name_nor_job_id_raises(self):
        with pytest.raises(ValueError, match='Must specify either'):
            jobs_core.wait(name=None,
                           job_id=None,
                           timeout=None,
                           poll_interval=15)

    def test_poll_interval_too_small_raises(self):
        with pytest.raises(ValueError, match='at least 5 seconds'):
            jobs_core.wait(name=None, job_id=1, timeout=None, poll_interval=2)


# ──────────────────────────────────────────────────────────────────────
# Single-task tests
# ──────────────────────────────────────────────────────────────────────


class TestWaitSingleTask:

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_already_succeeded(self, mock_sleep, mock_queue, mock_statuses):
        mock_statuses.return_value = [(0, ManagedJobStatus.SUCCEEDED)]

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=15)

        assert result == exceptions.JobExitCode.SUCCEEDED
        mock_sleep.assert_not_called()
        mock_statuses.assert_called_once_with(1)
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_already_failed(self, mock_sleep, mock_queue, mock_statuses):
        mock_statuses.return_value = [(0, ManagedJobStatus.FAILED)]

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=15)

        assert result == exceptions.JobExitCode.FAILED
        mock_sleep.assert_not_called()
        mock_statuses.assert_called_once_with(1)
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_transitions_to_succeeded(self, mock_sleep, mock_queue,
                                      mock_statuses):
        mock_statuses.side_effect = [
            [(0, ManagedJobStatus.RUNNING)],
            [(0, ManagedJobStatus.SUCCEEDED)],
        ]

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=5)

        assert result == exceptions.JobExitCode.SUCCEEDED
        mock_sleep.assert_called_once_with(5)
        assert mock_statuses.call_args_list == [mock.call(1), mock.call(1)]
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_transitions_to_cancelled(self, mock_sleep, mock_queue,
                                      mock_statuses):
        mock_statuses.side_effect = [
            [(0, ManagedJobStatus.RUNNING)],
            [(0, ManagedJobStatus.CANCELLED)],
        ]

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=5)

        assert result == exceptions.JobExitCode.CANCELLED
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_job_not_found(self, mock_sleep, mock_queue, mock_statuses):
        mock_statuses.return_value = []

        with pytest.raises(ValueError, match='not found'):
            jobs_core.wait(name=None, job_id=99, timeout=None, poll_interval=5)
        mock_queue.assert_not_called()

    @pytest.mark.parametrize('status', [
        ManagedJobStatus.FAILED,
        ManagedJobStatus.FAILED_SETUP,
        ManagedJobStatus.FAILED_PRECHECKS,
        ManagedJobStatus.FAILED_NO_RESOURCE,
        ManagedJobStatus.FAILED_CONTROLLER,
    ])
    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_all_failure_statuses_return_failed(self, mock_sleep, mock_queue,
                                                mock_statuses, status):
        mock_statuses.return_value = [(0, status)]

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=5)

        assert result == exceptions.JobExitCode.FAILED
        mock_queue.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# Timeout tests
# ──────────────────────────────────────────────────────────────────────


class TestWaitTimeout:

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    def test_timeout_uses_monotonic_deadline_and_bounds_sleep(
            self, mock_queue, mock_statuses, monkeypatch):
        now = [0.0]
        sleeps = []

        def _sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds

        monkeypatch.setattr(
            time, 'time',
            mock.Mock(side_effect=AssertionError(
                'wait timeout used wall-clock time')))
        monkeypatch.setattr(time, 'monotonic', lambda: now[0])
        monkeypatch.setattr(time, 'sleep', _sleep)
        mock_statuses.return_value = [(0, ManagedJobStatus.RUNNING)]

        with pytest.raises(TimeoutError, match='Timed out.*2 seconds'):
            jobs_core.wait(name=None, job_id=1, timeout=2, poll_interval=5)

        assert sleeps == [2]
        mock_statuses.assert_called_once_with(1)
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    @mock.patch.object(time, 'monotonic')
    def test_timeout_raises(self, mock_monotonic, mock_sleep, mock_queue,
                            mock_statuses):
        mock_monotonic.side_effect = [0.0, 0.0, 31.0]
        mock_statuses.side_effect = [
            [(0, ManagedJobStatus.RUNNING)],
            [(0, ManagedJobStatus.RUNNING)],
        ]

        with pytest.raises(TimeoutError, match='Timed out.*30 seconds'):
            jobs_core.wait(name=None, job_id=1, timeout=30, poll_interval=5)
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    @mock.patch.object(time, 'time')
    def test_timeout_none_keeps_polling(self, mock_time, mock_sleep, mock_queue,
                                        mock_statuses):
        """With timeout=None, polling continues until terminal."""
        mock_time.return_value = 0.0
        mock_statuses.side_effect = [
            [(0, ManagedJobStatus.PENDING)],
            [(0, ManagedJobStatus.STARTING)],
            [(0, ManagedJobStatus.SUCCEEDED)],
        ]

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=5)

        assert result == exceptions.JobExitCode.SUCCEEDED
        assert mock_sleep.call_count == 2
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    @mock.patch.object(time, 'monotonic')
    def test_timeout_message_includes_status(self, mock_monotonic, mock_sleep,
                                             mock_queue, mock_statuses):
        mock_monotonic.side_effect = [0.0, 0.0, 100.0]
        mock_statuses.side_effect = [
            [(0, ManagedJobStatus.RECOVERING)],
            [(0, ManagedJobStatus.RECOVERING)],
        ]

        with pytest.raises(TimeoutError, match='RECOVERING'):
            jobs_core.wait(name=None, job_id=1, timeout=60, poll_interval=5)
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_zero_timeout_returns_already_terminal_job(self, mock_sleep,
                                                       mock_queue,
                                                       mock_statuses):
        mock_statuses.return_value = [(0, ManagedJobStatus.SUCCEEDED)]

        result = jobs_core.wait(name=None, job_id=1, timeout=0, poll_interval=5)

        assert result == exceptions.JobExitCode.SUCCEEDED
        mock_sleep.assert_not_called()
        mock_statuses.assert_called_once_with(1)
        mock_queue.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# Name resolution tests
# ──────────────────────────────────────────────────────────────────────


class TestWaitNameResolution:

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_name_resolves_to_job_id(self, mock_sleep, mock_queue,
                                     mock_statuses):
        record = _make_record(42, ManagedJobStatus.SUCCEEDED, job_name='my-job')
        mock_queue.return_value = ([record], 1, {}, 1)
        mock_statuses.return_value = [(0, ManagedJobStatus.SUCCEEDED)]

        result = jobs_core.wait(name='my-job',
                                job_id=None,
                                timeout=None,
                                poll_interval=5)

        assert result == exceptions.JobExitCode.SUCCEEDED
        mock_queue.assert_called_once_with(refresh=False, name_match='my-job')
        mock_statuses.assert_called_once_with(42)

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_name_picks_latest_job_id(self, mock_sleep, mock_queue,
                                      mock_statuses):
        records = [
            _make_record(10, ManagedJobStatus.FAILED, job_name='dup'),
            _make_record(20, ManagedJobStatus.SUCCEEDED, job_name='dup'),
        ]
        mock_queue.return_value = (records, 2, {}, 2)
        mock_statuses.return_value = [(0, ManagedJobStatus.SUCCEEDED)]

        result = jobs_core.wait(name='dup',
                                job_id=None,
                                timeout=None,
                                poll_interval=5)

        assert result == exceptions.JobExitCode.SUCCEEDED
        # Should have resolved to job_id=20 (the latest).
        mock_queue.assert_called_once_with(refresh=False, name_match='dup')
        mock_statuses.assert_called_once_with(20)

    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_name_not_found_raises(self, mock_sleep, mock_queue):
        mock_queue.return_value = ([], 0, {}, 0)

        with pytest.raises(ValueError, match='No managed job found'):
            jobs_core.wait(name='nonexistent',
                           job_id=None,
                           timeout=None,
                           poll_interval=5)

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_name_filters_exact_match(self, mock_sleep, mock_queue,
                                      mock_statuses):
        """name_match may return partial matches; wait() filters to exact."""
        records = [
            _make_record(1, ManagedJobStatus.SUCCEEDED, job_name='train'),
            _make_record(2, ManagedJobStatus.RUNNING, job_name='train-v2'),
        ]
        mock_queue.return_value = (records, 2, {}, 2)
        mock_statuses.return_value = [(0, ManagedJobStatus.SUCCEEDED)]

        result = jobs_core.wait(name='train',
                                job_id=None,
                                timeout=None,
                                poll_interval=5)

        assert result == exceptions.JobExitCode.SUCCEEDED
        mock_queue.assert_called_once_with(refresh=False, name_match='train')
        mock_statuses.assert_called_once_with(1)


# ──────────────────────────────────────────────────────────────────────
# JobGroup (multi-task) tests
# ──────────────────────────────────────────────────────────────────────


class TestWaitJobGroup:

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_all_tasks_terminal(self, mock_sleep, mock_queue, mock_statuses):
        mock_statuses.return_value = [
            (0, ManagedJobStatus.SUCCEEDED),
            (1, ManagedJobStatus.SUCCEEDED),
            (2, ManagedJobStatus.SUCCEEDED),
        ]

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=5)

        assert result == exceptions.JobExitCode.SUCCEEDED
        mock_sleep.assert_not_called()
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_partial_terminal_keeps_polling(self, mock_sleep, mock_queue,
                                            mock_statuses):
        mock_statuses.side_effect = [
            [
                (0, ManagedJobStatus.SUCCEEDED),
                (1, ManagedJobStatus.RUNNING),
            ],
            [
                (0, ManagedJobStatus.SUCCEEDED),
                (1, ManagedJobStatus.SUCCEEDED),
            ],
        ]

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=5)

        assert result == exceptions.JobExitCode.SUCCEEDED
        mock_sleep.assert_called_once_with(5)
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state, 'get_all_task_ids_statuses')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_worst_exit_code_across_tasks(self, mock_sleep, mock_queue,
                                          mock_statuses):
        mock_statuses.return_value = [
            (0, ManagedJobStatus.SUCCEEDED),
            (1, ManagedJobStatus.FAILED),
        ]

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=5)

        assert result == exceptions.JobExitCode.FAILED

    @mock.patch.object(jobs_core.managed_job_state,
                       'get_task_log_stream_lookup')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_task_filter_by_int(self, mock_sleep, mock_queue, mock_lookup):
        """With task=1, only wait for task_id=1, ignore task_id=0."""
        mock_lookup.return_value = _make_task_lookup(ManagedJobStatus.SUCCEEDED,
                                                     task_id=1,
                                                     task_name='train',
                                                     num_tasks=2)

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=5,
                                task=1)

        assert result == exceptions.JobExitCode.SUCCEEDED
        mock_sleep.assert_not_called()
        mock_lookup.assert_called_once_with(1, 1)
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state,
                       'get_task_log_stream_lookup_by_name')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_task_filter_by_str(self, mock_sleep, mock_queue, mock_lookup):
        mock_lookup.return_value = _make_task_lookup(ManagedJobStatus.SUCCEEDED,
                                                     task_id=1,
                                                     task_name='train',
                                                     num_tasks=2)

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=5,
                                task='train')

        assert result == exceptions.JobExitCode.SUCCEEDED
        mock_sleep.assert_not_called()
        mock_lookup.assert_called_once_with(1, 'train')
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state,
                       'get_task_log_stream_lookup')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_task_filter_not_found(self, mock_sleep, mock_queue, mock_lookup):
        mock_lookup.return_value = _make_task_lookup(None,
                                                     task_id=None,
                                                     task_name=None,
                                                     num_tasks=1)

        with pytest.raises(ValueError, match='No task matching'):
            jobs_core.wait(name=None,
                           job_id=1,
                           timeout=None,
                           poll_interval=5,
                           task=99)
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state,
                       'get_task_log_stream_lookup')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_task_filter_missing_job_raises_job_not_found(
            self, mock_sleep, mock_queue, mock_lookup):
        mock_lookup.return_value = _make_task_lookup(None,
                                                     task_id=None,
                                                     task_name=None,
                                                     num_tasks=0)

        with pytest.raises(ValueError, match='Managed job 1 not found'):
            jobs_core.wait(name=None,
                           job_id=1,
                           timeout=None,
                           poll_interval=5,
                           task=99)
        mock_queue.assert_not_called()

    @mock.patch.object(jobs_core.managed_job_state,
                       'get_task_log_stream_lookup')
    @mock.patch.object(jobs_core, 'queue_v2_api')
    @mock.patch('time.sleep')
    def test_task_filter_waits_for_specific_task(self, mock_sleep, mock_queue,
                                                 mock_lookup):
        """task=0 is still RUNNING while task=1 is done; keeps polling."""
        mock_lookup.side_effect = [
            _make_task_lookup(ManagedJobStatus.RUNNING,
                              task_id=0,
                              task_name='task-0',
                              num_tasks=2),
            _make_task_lookup(ManagedJobStatus.FAILED,
                              task_id=0,
                              task_name='task-0',
                              num_tasks=2),
        ]

        result = jobs_core.wait(name=None,
                                job_id=1,
                                timeout=None,
                                poll_interval=5,
                                task=0)

        assert result == exceptions.JobExitCode.FAILED
        mock_sleep.assert_called_once_with(5)
        assert mock_lookup.call_args_list == [mock.call(1, 0), mock.call(1, 0)]
        mock_queue.assert_not_called()
