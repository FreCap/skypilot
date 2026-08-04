"""Unit tests for sky.jobs.scheduler.kill_local_job_controllers.

Used during shutdown (lock-loss suicide and uvicorn graceful shutdown) to
prevent split-brain: this replica's controllers must not outlive the
moment another replica's refresh daemon could acquire the consolidation
lock. The helper must be best-effort — it runs on shutdown paths where
raising would either prevent SIGTERM or stall drain.
"""
import asyncio
import signal
from unittest import mock

import pytest

from sky import exceptions
from sky.jobs import scheduler
from sky.jobs import state as managed_job_state


def _record(pid: int, started_at: float = 0.0):
    return managed_job_state.ControllerPidRecord(pid=pid, started_at=started_at)


def _submission_files(tmp_path):
    dag = tmp_path / 'dag.yaml'
    user = tmp_path / 'user.yaml'
    env = tmp_path / 'env'
    dag.write_text('name: dag\n', encoding='utf-8')
    user.write_text('name: user\n', encoding='utf-8')
    env.write_text('KEY=value\n', encoding='utf-8')
    return dag, user, env


@pytest.mark.parametrize('idempotent', [False, True])
def test_job_done_delegates_atomic_transition_without_preread(idempotent):
    with mock.patch.object(
            scheduler.state,
            'get_job_schedule_state',
            side_effect=AssertionError('schedule state pre-read used')) as get_state, \
            mock.patch.object(scheduler.state,
                              'scheduler_set_done') as set_done:
        scheduler.job_done(7, idempotent=idempotent)

    get_state.assert_not_called()
    set_done.assert_called_once_with(7, idempotent)


@pytest.mark.asyncio
@pytest.mark.parametrize('idempotent', [False, True])
async def test_job_done_async_delegates_atomic_transition_without_preread(
        idempotent):
    with mock.patch.object(
            scheduler.state,
            'get_job_schedule_state_async',
            new_callable=mock.AsyncMock,
            side_effect=AssertionError('schedule state pre-read used')) as get_state, \
            mock.patch.object(
                scheduler.state,
                'scheduler_set_done_async',
                new_callable=mock.AsyncMock) as set_done:
        await scheduler.job_done_async(7, idempotent=idempotent)

    get_state.assert_not_awaited()
    set_done.assert_awaited_once_with(7, idempotent)


@pytest.mark.asyncio
async def test_scheduled_launch_records_backoff_and_releases_slot():
    starting: set[int] = set()
    starting_lock = asyncio.Lock()
    starting_signal = asyncio.Condition(starting_lock)

    with mock.patch.object(
            scheduler.state,
            'get_pool_and_execution_from_job_id_async',
            new_callable=mock.AsyncMock,
            return_value=(None, None)), mock.patch.object(
                                   scheduler.state,
                                   'scheduler_set_launching_async',
                                   new_callable=mock.AsyncMock) as set_launching, \
            mock.patch.object(
                scheduler.state,
                'scheduler_set_backoff_async',
                new_callable=mock.AsyncMock) as set_backoff, \
            mock.patch.object(
                scheduler.state,
                'scheduler_set_alive_async',
                new_callable=mock.AsyncMock) as set_alive:
        with pytest.raises(exceptions.NoClusterLaunchedError):
            async with scheduler.scheduled_launch(7, starting, starting_lock,
                                                  starting_signal):
                assert starting == {7}
                raise exceptions.NoClusterLaunchedError()

    assert starting == set()
    set_launching.assert_awaited_once_with(7)
    set_backoff.assert_awaited_once_with(7)
    set_alive.assert_not_awaited()


@pytest.mark.asyncio
async def test_job_resumed_restores_alive_without_launching():
    with mock.patch.object(scheduler.state,
                           'scheduler_set_alive_async',
                           new_callable=mock.AsyncMock) as set_alive:
        await scheduler.job_resumed(7)

    set_alive.assert_awaited_once_with(7)


@pytest.mark.asyncio
async def test_scheduled_launch_uses_preclaimed_slot_at_capacity():
    starting = {7}
    starting_lock = asyncio.Lock()
    starting_signal = asyncio.Condition(starting_lock)
    body_entered = asyncio.Event()

    async def launch():
        async with scheduler.scheduled_launch(7, starting, starting_lock,
                                              starting_signal):
            body_entered.set()

    with mock.patch.object(
            scheduler.state,
            'get_pool_and_execution_from_job_id_async',
            new_callable=mock.AsyncMock,
            return_value=(None, None)), mock.patch.object(
                scheduler.state,
                'scheduler_set_launching_async',
                new_callable=mock.AsyncMock) as set_launching, \
            mock.patch.object(
                scheduler.state,
                'scheduler_set_alive_async',
                new_callable=mock.AsyncMock) as set_alive, \
            mock.patch.object(scheduler.controller_utils,
                              'LAUNCHES_PER_WORKER', 1):
        launch_task = asyncio.create_task(launch())
        try:
            await asyncio.wait_for(body_entered.wait(), timeout=1)
            await asyncio.wait_for(launch_task, timeout=1)
        finally:
            launch_task.cancel()
            await asyncio.gather(launch_task, return_exceptions=True)

    assert starting == set()
    set_launching.assert_awaited_once_with(7)
    set_alive.assert_awaited_once_with(7)


class _YieldingLock(asyncio.Lock):
    """Lock whose acquire always yields to the event loop first.

    Deterministically widens the window between two separate lock
    acquisitions so a check-then-act split across them is observable.
    """

    async def acquire(self):
        await asyncio.sleep(0)
        return await super().acquire()


@pytest.mark.asyncio
async def test_scheduled_launch_capacity_check_and_claim_are_atomic():
    starting: set[int] = set()
    starting_lock = _YieldingLock()
    starting_signal = asyncio.Condition(starting_lock)
    max_concurrent = 0

    async def run_one(job_id: int):
        nonlocal max_concurrent
        async with scheduler.scheduled_launch(job_id, starting, starting_lock,
                                              starting_signal):
            async with starting_lock:
                max_concurrent = max(max_concurrent, len(starting))
            await asyncio.sleep(0)

    with mock.patch.object(
            scheduler.state,
            'get_pool_and_execution_from_job_id_async',
            new_callable=mock.AsyncMock,
            return_value=(None, None)), \
            mock.patch.object(scheduler.state,
                              'scheduler_set_launching_async',
                              new_callable=mock.AsyncMock), \
            mock.patch.object(scheduler.state,
                              'scheduler_set_alive_async',
                              new_callable=mock.AsyncMock), \
            mock.patch.object(scheduler.controller_utils,
                              'LAUNCHES_PER_WORKER', 1):
        await asyncio.gather(*(run_one(job_id) for job_id in range(1, 5)))

    assert max_concurrent == 1
    assert starting == set()


@pytest.mark.asyncio
async def test_scheduled_launch_releases_slot_when_set_launching_fails():
    starting: set[int] = set()
    starting_lock = asyncio.Lock()
    starting_signal = asyncio.Condition(starting_lock)

    with mock.patch.object(
            scheduler.state,
            'get_pool_and_execution_from_job_id_async',
            new_callable=mock.AsyncMock,
            return_value=(None, None)), \
            mock.patch.object(scheduler.state,
                              'scheduler_set_launching_async',
                              new_callable=mock.AsyncMock,
                              side_effect=RuntimeError('db down')), \
            mock.patch.object(scheduler.state,
                              'scheduler_set_backoff_async',
                              new_callable=mock.AsyncMock) as set_backoff, \
            mock.patch.object(scheduler.state,
                              'scheduler_set_alive_async',
                              new_callable=mock.AsyncMock) as set_alive:
        with pytest.raises(RuntimeError):
            async with scheduler.scheduled_launch(7, starting, starting_lock,
                                                  starting_signal):
                pytest.fail('body must not run when the DB transition fails')

    assert starting == set()
    set_backoff.assert_not_awaited()
    set_alive.assert_not_awaited()


def test_submit_jobs_uses_one_snapshot_and_filters_live_controllers(tmp_path):
    dag, user, env = _submission_files(tmp_path)
    records = {
        1: _record(101, 1001.0),
        2: _record(202, 1002.0),
    }

    def _is_alive(record):
        return record.pid == 101

    with mock.patch.object(
            scheduler.state,
            'get_job_controller_processes',
            return_value=records) as get_processes, mock.patch.object(
                scheduler.state,
                'get_job_controller_process',
                side_effect=AssertionError('scalar lookup used')), \
            mock.patch.object(scheduler.managed_job_utils,
                              'controller_process_alive',
                              side_effect=_is_alive) as is_alive, \
            mock.patch.object(scheduler.state,
                              'scheduler_set_waiting') as set_waiting, \
            mock.patch.object(scheduler,
                              'maybe_start_controllers') as start_controllers:
        scheduler.submit_jobs([1, 2, 3], str(dag), str(user), str(env), 50,
                              'normal')

    get_processes.assert_called_once_with([1, 2, 3])
    assert [call.args[0].pid for call in is_alive.call_args_list] == [101, 202]
    set_waiting.assert_called_once_with([2, 3], 'name: dag\n', 'name: user\n',
                                        'KEY=value\n', None, 50, 'normal')
    start_controllers.assert_called_once_with(from_scheduler=True)


def test_submit_jobs_returns_before_file_reads_when_every_controller_is_live(
        tmp_path):
    missing = tmp_path / 'must-not-be-read'
    records = {
        1: _record(101, 1001.0),
        2: _record(202, 1002.0),
    }

    with mock.patch.object(
            scheduler.state,
            'get_job_controller_processes',
            return_value=records) as get_processes, mock.patch.object(
                scheduler.state,
                'get_job_controller_process',
                side_effect=AssertionError('scalar lookup used')), \
            mock.patch.object(scheduler.managed_job_utils,
                              'controller_process_alive',
                              return_value=True), \
            mock.patch.object(scheduler.state,
                              'scheduler_set_waiting') as set_waiting, \
            mock.patch.object(scheduler,
                              'maybe_start_controllers') as start_controllers:
        scheduler.submit_jobs([1, 2], str(missing), str(missing), str(missing),
                              50)

    get_processes.assert_called_once_with([1, 2])
    set_waiting.assert_not_called()
    start_controllers.assert_not_called()


def test_submit_jobs_empty_input_returns_without_work(tmp_path):
    missing = tmp_path / 'must-not-be-read'

    with mock.patch.object(
            scheduler.state,
            'get_job_controller_processes',
            side_effect=AssertionError('controller lookup used')), \
            mock.patch.object(
                scheduler.state,
                'scheduler_set_waiting',
                side_effect=AssertionError('state transition used')), \
            mock.patch.object(
                scheduler,
                'maybe_start_controllers',
                side_effect=AssertionError('controller start used')):
        scheduler.submit_jobs([], str(missing), str(missing), str(missing), 50)


def test_submit_jobs_deduplicates_ids_before_controller_checks_and_submit(
        tmp_path):
    dag, user, env = _submission_files(tmp_path)
    records = {
        1: _record(101, 1001.0),
        2: _record(202, 1002.0),
    }

    with mock.patch.object(
            scheduler.state,
            'get_job_controller_processes',
            return_value=records) as get_processes, mock.patch.object(
                scheduler.state,
                'get_job_controller_process',
                side_effect=AssertionError('scalar lookup used')), \
            mock.patch.object(scheduler.managed_job_utils,
                              'controller_process_alive',
                              return_value=False) as is_alive, \
            mock.patch.object(scheduler.state,
                              'scheduler_set_waiting') as set_waiting, \
            mock.patch.object(scheduler,
                              'maybe_start_controllers') as start_controllers:
        scheduler.submit_jobs([1, 2, 1, 2], str(dag), str(user), str(env), 50)

    get_processes.assert_called_once_with([1, 2])
    assert [call.args[0].pid for call in is_alive.call_args_list] == [101, 202]
    set_waiting.assert_called_once_with([1, 2], 'name: dag\n', 'name: user\n',
                                        'KEY=value\n', None, 50, None)
    start_controllers.assert_called_once_with(from_scheduler=True)


class TestKillLocalConsolidationControllers:
    """Tests shutdown cleanup for consolidated controller processes."""

    def test_pid_reader_ignores_legacy_and_malformed_entries(
            self, monkeypatch, tmp_path):
        pid_file = tmp_path / 'job_controller_pid'
        pid_file.write_text('\n'.join([
            '101,1700000000.0',
            '202',
            'bad,1700000001.0',
            '303,not-a-float',
            '404,1700000002.5',
        ]),
                            encoding='utf-8')
        monkeypatch.setattr(scheduler, 'JOB_CONTROLLER_PID_PATH', str(pid_file))

        assert scheduler.get_controller_process_records() == [
            _record(101, 1700000000.0),
            _record(404, 1700000002.5),
        ]

    def test_no_pid_file_returns_zero(self):
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=[]):
            assert scheduler.kill_local_job_controllers() == 0

    def test_records_none_returns_zero(self):
        """Helper must tolerate the PID-file read failing (returns None)."""
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=None):
            assert scheduler.kill_local_job_controllers() == 0

    def test_signals_live_records(self):
        recs = [_record(101), _record(202), _record(303)]
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=recs), \
                mock.patch.object(scheduler.managed_job_utils,
                                  'controller_process_alive',
                                  return_value=True), \
                mock.patch.object(scheduler.os, 'kill') as kill_mock:
            n = scheduler.kill_local_job_controllers()
        assert n == 3
        kill_mock.assert_has_calls([
            mock.call(101, signal.SIGTERM),
            mock.call(202, signal.SIGTERM),
            mock.call(303, signal.SIGTERM)
        ],
                                   any_order=True)

    def test_skips_dead_records(self):
        """Stale entries (process exited or wrong started_at) are skipped —
        otherwise we'd SIGTERM unrelated PIDs that the OS reused."""
        recs = [_record(101), _record(202)]
        alive_lookup = {101: True, 202: False}
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=recs), \
                mock.patch.object(
                    scheduler.managed_job_utils,
                    'controller_process_alive',
                    side_effect=lambda r: alive_lookup[r.pid]), \
                mock.patch.object(scheduler.os, 'kill') as kill_mock:
            n = scheduler.kill_local_job_controllers()
        assert n == 1
        kill_mock.assert_called_once_with(101, signal.SIGTERM)

    def test_tolerates_process_lookup_error(self):
        """Race between alive-check and kill: the PID died in between.
        Not counted as signaled, but doesn't abort the loop."""
        recs = [_record(101), _record(202)]
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=recs), \
                mock.patch.object(scheduler.managed_job_utils,
                                  'controller_process_alive',
                                  return_value=True), \
                mock.patch.object(
                    scheduler.os, 'kill',
                    side_effect=[ProcessLookupError(), None]) as kill_mock:
            n = scheduler.kill_local_job_controllers()
        assert n == 1  # Only the second succeeded.
        assert kill_mock.call_count == 2

    def test_continues_on_oserror(self):
        """Per-PID OSError (e.g. EPERM) must not stop the rest."""
        recs = [_record(101), _record(202)]
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=recs), \
                mock.patch.object(scheduler.managed_job_utils,
                                  'controller_process_alive',
                                  return_value=True), \
                mock.patch.object(
                    scheduler.os, 'kill',
                    side_effect=[OSError('EPERM'), None]):
            n = scheduler.kill_local_job_controllers()
        assert n == 1

    def test_custom_signal(self):
        recs = [_record(101)]
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=recs), \
                mock.patch.object(scheduler.managed_job_utils,
                                  'controller_process_alive',
                                  return_value=True), \
                mock.patch.object(scheduler.os, 'kill') as kill_mock:
            scheduler.kill_local_job_controllers(sig=signal.SIGKILL)
        kill_mock.assert_called_once_with(101, signal.SIGKILL)

    def test_fail_stop_kills_validated_process_group_and_descendants(self):
        recs = [_record(101, 1700000000.0)]
        process = mock.Mock()
        process.create_time.return_value = 1700000000.0
        descendant = mock.Mock()
        process.children.return_value = [descendant]
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=recs), \
                mock.patch.object(scheduler.managed_job_utils,
                                  'controller_process_alive',
                                  return_value=True), \
                mock.patch.object(scheduler.psutil,
                                  'Process',
                                  return_value=process), \
                mock.patch.object(scheduler.os,
                                  'getpgrp',
                                  return_value=999), \
                mock.patch.object(scheduler.os,
                                  'getpgid',
                                  return_value=700), \
                mock.patch.object(scheduler.os, 'killpg') as kill_group:
            n = scheduler.fail_stop_local_job_controllers()

        assert n == 1
        process.children.assert_called_once_with(recursive=True)
        kill_group.assert_called_once_with(700, signal.SIGKILL)
        process.kill.assert_not_called()
        descendant.kill.assert_called_once_with()

    def test_fail_stop_revalidates_process_start_time(self):
        recs = [_record(101, 1700000000.0)]
        process = mock.Mock()
        process.create_time.return_value = 1700000001.0
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=recs), \
                mock.patch.object(scheduler.managed_job_utils,
                                  'controller_process_alive',
                                  return_value=True), \
                mock.patch.object(scheduler.psutil,
                                  'Process',
                                  return_value=process), \
                mock.patch.object(scheduler.os, 'getpgid') as get_group, \
                mock.patch.object(scheduler.os, 'killpg') as kill_group:
            n = scheduler.fail_stop_local_job_controllers()

        assert n == 0
        get_group.assert_not_called()
        kill_group.assert_not_called()
        process.kill.assert_not_called()

    def test_fail_stop_never_kills_supervisor_process_group(self):
        recs = [_record(101, 1700000000.0)]
        process = mock.Mock()
        process.create_time.return_value = 1700000000.0
        process.children.return_value = []
        with mock.patch.object(scheduler,
                               'get_controller_process_records',
                               return_value=recs), \
                mock.patch.object(scheduler.managed_job_utils,
                                  'controller_process_alive',
                                  return_value=True), \
                mock.patch.object(scheduler.psutil,
                                  'Process',
                                  return_value=process), \
                mock.patch.object(scheduler.os,
                                  'getpgrp',
                                  return_value=700), \
                mock.patch.object(scheduler.os,
                                  'getpgid',
                                  return_value=700), \
                mock.patch.object(scheduler.os, 'killpg') as kill_group:
            n = scheduler.fail_stop_local_job_controllers()

        assert n == 1
        kill_group.assert_not_called()
        process.kill.assert_called_once_with()
