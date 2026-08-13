"""Unit tests for durable managed-job scheduling transitions."""
import asyncio
from unittest import mock

import pytest

from sky import exceptions
from sky.jobs import scheduler


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


def test_submit_jobs_persists_every_deduplicated_id_without_spawning(tmp_path):
    dag, user, env = _submission_files(tmp_path)

    with mock.patch.object(scheduler.state,
                           'scheduler_set_waiting') as set_waiting:
        scheduler.submit_jobs([1, 2, 1, 2], str(dag), str(user), str(env), 50,
                              'normal')

    set_waiting.assert_called_once_with([1, 2], 'name: dag\n', 'name: user\n',
                                        'KEY=value\n', None, 50, 'normal')


def test_submit_jobs_empty_input_returns_before_file_reads(tmp_path):
    missing = tmp_path / 'must-not-be-read'

    with mock.patch.object(scheduler.state,
                           'scheduler_set_waiting',
                           side_effect=AssertionError('state transition used')):
        scheduler.submit_jobs([], str(missing), str(missing), str(missing), 50)
