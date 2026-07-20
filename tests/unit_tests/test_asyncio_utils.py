"""Tests for asyncio utilities."""

import asyncio
import sys

import pytest

from sky.utils import asyncio_utils

# The ownership set is the state under test for cancellation behavior.
# pylint: disable=protected-access


@pytest.mark.asyncio
async def test_shield_returns_inner_result():

    @asyncio_utils.shield
    async def work():
        return 42

    assert await work() == 42


@pytest.mark.asyncio
async def test_shield_keeps_inner_task_alive_after_parent_cancellation():
    started = asyncio.Event()
    release = asyncio.Event()
    completed = asyncio.Event()
    initially_owned = set(asyncio_utils._background_tasks)

    @asyncio_utils.shield
    async def work():
        started.set()
        await release.wait()
        completed.set()

    outer = asyncio.create_task(work())
    await started.wait()
    outer.cancel()

    with pytest.raises(asyncio.CancelledError):
        await outer

    newly_owned = asyncio_utils._background_tasks - initially_owned
    assert len(newly_owned) == 1
    inner = newly_owned.pop()
    assert not inner.done()

    release.set()
    await completed.wait()
    await inner
    await asyncio.sleep(0)

    assert inner not in asyncio_utils._background_tasks


@pytest.mark.asyncio
async def test_shield_reports_failure_after_parent_cancellation():
    started = asyncio.Event()
    release = asyncio.Event()
    failure_reported = asyncio.Event()
    reported_contexts = []
    initially_owned = set(asyncio_utils._background_tasks)
    loop = asyncio.get_running_loop()
    previous_exception_handler = loop.get_exception_handler()

    def exception_handler(_loop, context):
        reported_contexts.append(context)
        failure_reported.set()

    @asyncio_utils.shield
    async def work():
        started.set()
        await release.wait()
        raise RuntimeError('shielded child failed')

    loop.set_exception_handler(exception_handler)
    try:
        outer = asyncio.create_task(work())
        await started.wait()
        outer.cancel()

        with pytest.raises(asyncio.CancelledError):
            await outer

        newly_owned = asyncio_utils._background_tasks - initially_owned
        assert len(newly_owned) == 1
        inner = newly_owned.pop()

        release.set()
        await asyncio.wait_for(failure_reported.wait(), timeout=1)
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous_exception_handler)

    assert inner not in asyncio_utils._background_tasks
    assert len(reported_contexts) == 1
    context = reported_contexts[0]
    if sys.version_info >= (3, 14):
        assert context['message'] == 'RuntimeError exception in shielded future'
        assert context['future'] is inner
    else:
        assert context['message'] == 'Exception in shielded background task'
        assert context['task'] is inner
    assert isinstance(context['exception'], RuntimeError)
    assert str(context['exception']) == 'shielded child failed'


@pytest.mark.asyncio
async def test_shield_ignores_cancelled_inner_task():
    started = asyncio.Event()
    reported_contexts = []
    initially_owned = set(asyncio_utils._background_tasks)
    loop = asyncio.get_running_loop()
    previous_exception_handler = loop.get_exception_handler()

    def exception_handler(_loop, context):
        reported_contexts.append(context)

    @asyncio_utils.shield
    async def work():
        started.set()
        await asyncio.Event().wait()

    loop.set_exception_handler(exception_handler)
    try:
        outer = asyncio.create_task(work())
        await started.wait()
        outer.cancel()

        with pytest.raises(asyncio.CancelledError):
            await outer

        newly_owned = asyncio_utils._background_tasks - initially_owned
        assert len(newly_owned) == 1
        inner = newly_owned.pop()
        inner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await inner
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous_exception_handler)

    assert inner not in asyncio_utils._background_tasks
    assert not reported_contexts
