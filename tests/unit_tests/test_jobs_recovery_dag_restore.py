"""Tests for DAG restoration in the failover recovery strategies.

Both FAILOVER and EAGER_NEXT_REGION temporarily mutate the shared in-memory
DAG task before the constrained relaunch attempt (FAILOVER pins the
previously launched cloud/region via ``task.set_resources``;
EAGER_NEXT_REGION blocks it via ``task.blocked_resources``). The DAG lives
for the whole controller process and is reused by every later launch and
recovery attempt, so the mutation must be reverted even when ``_launch``
raises (cancellation, or a DB error surfacing from the scheduler context
manager that ``raise_on_failure=False`` does not cover). A leaked
constraint permanently shrinks the job's failover search space.
"""
import asyncio

import pytest

import sky
from sky.jobs import recovery_strategy
from sky.resources import Resources


def _make_executor(cls):
    """Build an executor without running __init__ (needs a real backend)."""
    executor = cls.__new__(cls)
    task = sky.Task(run='echo hi')
    original_resources = Resources()
    task.set_resources({original_resources})
    dag = sky.Dag()
    dag.add(task)
    executor.dag = dag
    executor._launched_resources = Resources()
    return executor, task, original_resources


async def _noop():
    return None


def _run(coro):
    return asyncio.run(coro)


@pytest.mark.parametrize('exc',
                         [RuntimeError('db error'),
                          asyncio.CancelledError()])
def test_failover_restores_resources_when_launch_raises(exc):
    executor, task, original_resources = _make_executor(
        recovery_strategy.FailoverStrategyExecutor)
    executor._try_cancel_jobs = _noop

    async def _raising_launch(*args, **kwargs):
        # The constraint must be applied at this point.
        assert task.resources != {original_resources}
        raise exc

    executor._launch = _raising_launch

    with pytest.raises(type(exc)):
        _run(executor.recover())
    assert task.resources == {original_resources}


def test_failover_restores_resources_on_success():
    executor, task, original_resources = _make_executor(
        recovery_strategy.FailoverStrategyExecutor)
    executor._try_cancel_jobs = _noop

    async def _launch(*args, **kwargs):
        return 123.0

    executor._launch = _launch

    assert _run(executor.recover()) == 123.0
    assert task.resources == {original_resources}


@pytest.mark.parametrize('exc',
                         [RuntimeError('db error'),
                          asyncio.CancelledError()])
def test_eager_failover_clears_blocked_resources_when_launch_raises(exc):
    executor, task, _ = _make_executor(
        recovery_strategy.EagerFailoverStrategyExecutor)
    executor._cleanup_cluster = lambda: None

    async def _raising_launch(*args, **kwargs):
        assert task.blocked_resources
        raise exc

    executor._launch = _raising_launch

    with pytest.raises(type(exc)):
        _run(executor.recover())
    assert task.blocked_resources is None


def test_eager_failover_clears_blocked_resources_on_success():
    executor, task, _ = _make_executor(
        recovery_strategy.EagerFailoverStrategyExecutor)
    executor._cleanup_cluster = lambda: None

    async def _launch(*args, **kwargs):
        return 456.0

    executor._launch = _launch

    assert _run(executor.recover()) == 456.0
    assert task.blocked_resources is None
