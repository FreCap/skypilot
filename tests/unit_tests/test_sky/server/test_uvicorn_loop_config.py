"""Tests for the serving-loop lag-debug configuration in sky.server.uvicorn.

The lag threshold must be applied to the loop created by asyncio.run() (the
loop that actually serves), from inside the serving coroutine. Configuring a
loop before asyncio.run() would target a bystander loop and, on Python 3.14+,
raise RuntimeError outright.
"""
import asyncio

from sky.server import uvicorn as sky_uvicorn


def test_lag_config_applies_to_running_loop():
    state = {}

    async def probe():
        sky_uvicorn._configure_running_loop_lag_debug(0.25)
        loop = asyncio.get_running_loop()
        state['debug'] = loop.get_debug()
        state['slow'] = loop.slow_callback_duration

    asyncio.run(probe())
    assert state['debug'] is True
    assert state['slow'] == 0.25


def test_lag_config_noop_when_threshold_unset():
    state = {}

    async def probe():
        sky_uvicorn._configure_running_loop_lag_debug(None)
        loop = asyncio.get_running_loop()
        state['debug'] = loop.get_debug()

    asyncio.run(probe())
    assert state['debug'] is False


def test_lag_config_requires_running_loop():
    # Outside a running loop this must raise instead of silently configuring
    # a bystander loop (the pre-fix behavior on Python <= 3.13).
    try:
        sky_uvicorn._configure_running_loop_lag_debug(0.25)
    except RuntimeError:
        pass
    else:
        raise AssertionError('expected RuntimeError with no running loop')
