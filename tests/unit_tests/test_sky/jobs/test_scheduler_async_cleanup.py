"""Cancellation-safety tests for managed-jobs scheduler bookkeeping."""

import asyncio
from unittest import mock

import pytest

from sky import exceptions
from sky.jobs import scheduler
from sky.utils import asyncio_utils


@pytest.mark.asyncio
async def test_scheduled_launch_releases_slot_after_repeated_cancel():
    # The regression exercises the scheduler's retained shield-task set.
    # pylint: disable=protected-access
    starting: set[int] = set()
    starting_lock = asyncio.Lock()
    starting_signal = asyncio.Condition(starting_lock)
    entered = asyncio.Event()
    cleanup_waiting = asyncio.Event()
    original_acquire = starting_lock.acquire

    async def track_cleanup_waiter():
        cleanup_waiting.set()
        return await original_acquire()

    async def run_launch():
        async with scheduler.scheduled_launch(7, starting, starting_lock,
                                              starting_signal):
            entered.set()
            await asyncio.Future()

    background_before = set(asyncio_utils._background_tasks)
    with mock.patch.object(
            scheduler.state,
            'get_pool_and_execution_from_job_id_async',
            new_callable=mock.AsyncMock,
            return_value=(None, None)), \
         mock.patch.object(scheduler.state,
                           'scheduler_set_launching_async',
                           new_callable=mock.AsyncMock) as set_launching, \
         mock.patch.object(scheduler.state,
                           'scheduler_set_alive_async',
                           new_callable=mock.AsyncMock) as set_alive:
        task = asyncio.create_task(run_launch())
        await entered.wait()
        await original_acquire()
        try:
            with mock.patch.object(starting_lock,
                                   'acquire',
                                   side_effect=track_cleanup_waiter):
                task.cancel()
                await cleanup_waiting.wait()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                cleanup_tasks = (asyncio_utils._background_tasks -
                                 background_before)
                assert len(cleanup_tasks) == 1
        finally:
            starting_lock.release()

    await asyncio.gather(*cleanup_tasks)
    assert starting == set()
    set_launching.assert_awaited_once_with(7)
    set_alive.assert_not_awaited()


@pytest.mark.parametrize('outcome', ['alive', 'backoff'])
@pytest.mark.asyncio
async def test_scheduled_launch_finishes_outcome_before_releasing_slot(outcome):
    starting: set[int] = set()
    starting_lock = asyncio.Lock()
    starting_signal = asyncio.Condition(starting_lock)
    transition_started = asyncio.Event()
    allow_transition = asyncio.Event()
    transition_completed = asyncio.Event()

    async def blocked_transition(job_id):
        assert job_id == 7
        transition_started.set()
        await allow_transition.wait()
        transition_completed.set()

    async def unexpected_transition(_job_id):
        pytest.fail('the other launch outcome must not be recorded')

    async def run_launch():
        async with scheduler.scheduled_launch(7, starting, starting_lock,
                                              starting_signal):
            if outcome == 'backoff':
                raise exceptions.NoClusterLaunchedError()

    set_alive = (blocked_transition
                 if outcome == 'alive' else unexpected_transition)
    set_backoff = (blocked_transition
                   if outcome == 'backoff' else unexpected_transition)
    with mock.patch.object(
            scheduler.state,
            'get_pool_and_execution_from_job_id_async',
            new_callable=mock.AsyncMock,
            return_value=(None, None)), \
         mock.patch.object(scheduler.state,
                           'scheduler_set_launching_async',
                           new_callable=mock.AsyncMock), \
         mock.patch.object(scheduler.state,
                           'scheduler_set_alive_async', new=set_alive), \
         mock.patch.object(scheduler.state,
                           'scheduler_set_backoff_async', new=set_backoff):
        task = asyncio.create_task(run_launch())
        await asyncio.wait_for(transition_started.wait(), timeout=1)

        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)

        try:
            assert not task.done()
            assert starting == {7}
        finally:
            # Let the retained transition finish even if an assertion fails,
            # so the test cannot leak a cancellation-resistant task.
            allow_transition.set()
            task_result = await asyncio.gather(task, return_exceptions=True)

    assert isinstance(task_result[0], asyncio.CancelledError)
    assert transition_completed.is_set()
    assert starting == set()
