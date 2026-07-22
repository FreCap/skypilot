"""Tests for completion-only SkyServe response-time observation."""

import asyncio
from unittest import mock

import pytest

from sky.serve import load_balancer_http


def _scope(path='/predict'):
    return {
        'type': 'http',
        'method': 'POST',
        'path': path,
    }


async def _receive():
    return {'type': 'http.request', 'body': b'', 'more_body': False}


def test_records_only_after_terminal_body_is_sent():
    aggregator = mock.Mock()
    sent = []

    async def send_message(message):
        sent.append(message)

    async def app(scope, receive, send):
        del scope, receive
        await send({'type': 'http.response.start', 'status': 201})
        await send({
            'type': 'http.response.body',
            'body': b'a',
            'more_body': True,
        })
        aggregator.add_response_time.assert_not_called()
        await send({
            'type': 'http.response.body',
            'body': b'b',
            'more_body': False,
        })

    middleware = load_balancer_http._ResponseTimeMiddleware(  # pylint: disable=protected-access
        app, aggregator)
    asyncio.run(middleware(_scope(), _receive, send_message))

    aggregator.add_response_time.assert_called_once()
    duration, status = aggregator.add_response_time.call_args.args
    assert duration >= 0
    assert status == 201


def test_midstream_failure_is_not_a_completed_response():
    aggregator = mock.Mock()

    async def app(scope, receive, send):
        del scope, receive
        await send({'type': 'http.response.start', 'status': 200})
        await send({
            'type': 'http.response.body',
            'body': b'partial',
            'more_body': True,
        })
        raise RuntimeError('upstream reset')

    middleware = load_balancer_http._ResponseTimeMiddleware(  # pylint: disable=protected-access
        app, aggregator)
    with pytest.raises(RuntimeError, match='upstream reset'):
        asyncio.run(middleware(_scope(), _receive, mock.AsyncMock()))
    aggregator.add_response_time.assert_not_called()


def test_terminal_send_failure_is_not_a_completed_response():
    aggregator = mock.Mock()

    async def app(scope, receive, send):
        del scope, receive
        await send({'type': 'http.response.start', 'status': 200})
        await send({
            'type': 'http.response.body',
            'body': b'complete',
            'more_body': False,
        })

    async def disconnected_send(message):
        if message.get('type') == 'http.response.body':
            raise BrokenPipeError('client disconnected')

    middleware = load_balancer_http._ResponseTimeMiddleware(  # pylint: disable=protected-access
        app, aggregator)
    with pytest.raises(BrokenPipeError, match='client disconnected'):
        asyncio.run(middleware(_scope(), _receive, disconnected_send))
    aggregator.add_response_time.assert_not_called()


def test_pre_response_failure_is_classified_as_5xx():
    aggregator = mock.Mock()

    async def app(scope, receive, send):
        del scope, receive, send
        raise RuntimeError('handler failed')

    middleware = load_balancer_http._ResponseTimeMiddleware(  # pylint: disable=protected-access
        app, aggregator)
    with pytest.raises(RuntimeError, match='handler failed'):
        asyncio.run(middleware(_scope(), _receive, mock.AsyncMock()))
    assert aggregator.add_response_time.call_args.args[1] == 500


def test_internal_load_balancer_routes_are_excluded():
    aggregator = mock.Mock()

    async def app(scope, receive, send):
        del scope, receive
        await send({'type': 'http.response.start', 'status': 200})
        await send({
            'type': 'http.response.body',
            'body': b'ok',
            'more_body': False,
        })

    middleware = load_balancer_http._ResponseTimeMiddleware(  # pylint: disable=protected-access
        app, aggregator)
    asyncio.run(middleware(_scope('/_lb/health'), _receive, mock.AsyncMock()))
    aggregator.add_response_time.assert_not_called()
