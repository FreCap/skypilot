"""Characterization tests for API server authentication middleware."""

import asyncio
import base64
import hashlib
import json
import pickle
import threading
from unittest import mock

import fastapi
import pytest

from sky.server import config as server_config
from sky.server import server
from sky.server.auth import middleware as auth_middleware
from sky.server.auth import user_registration
from sky.utils import common_utils

# The legacy server import surface intentionally exposes these helpers.
# pylint: disable=protected-access


def test_server_preserves_auth_middleware_import_surface():
    symbol_names = (
        '_basic_auth_401_response',
        '_bearer_auth_401_response',
        '_try_set_basic_auth_user',
        'RBACMiddleware',
        '_extract_identity_from_jwt',
        '_extract_user_from_header',
        '_get_auth_user_header',
        '_generate_auth_token',
        'InitializeRequestAuthUserMiddleware',
        'BasicAuthMiddleware',
        'BearerTokenMiddleware',
        'InternalServeControllerSyncAuthMiddleware',
        'InternalServeControllerApiAuthMiddleware',
        'AuthProxyMiddleware',
    )

    for symbol_name in symbol_names:
        server_symbol = getattr(server, symbol_name)
        assert server_symbol is getattr(auth_middleware, symbol_name)
        assert server_symbol.__module__ == 'sky.server.server'
        assert pickle.loads(pickle.dumps(server_symbol)) is server_symbol


def test_generate_auth_token_preserves_proxy_identity_and_cookies(monkeypatch):
    proxy_config = server_config.ExternalProxyConfig(
        enabled=True,
        header_name='X-Auth-Request-Email',
        header_format='plaintext')
    monkeypatch.setattr(server.server_config, 'load_external_proxy_config',
                        lambda: proxy_config)
    request = mock.Mock()
    request.headers = {'X-Auth-Request-Email': 'user@example.com'}
    request.cookies = {'session': 'cookie-value'}

    encoded_token = server._generate_auth_token(request)

    token = json.loads(base64.b64decode(encoded_token))
    expected_user_id = hashlib.md5(
        b'user@example.com',
        usedforsecurity=False).hexdigest()[:common_utils.USER_HASH_LENGTH]
    assert token == {
        'v': 1,
        'user': expected_user_id,
        'cookies': {
            'session': 'cookie-value'
        },
    }


def test_generate_auth_token_preserves_anonymous_user(monkeypatch):
    monkeypatch.setattr(server.server_config, 'load_external_proxy_config',
                        lambda: server_config.ExternalProxyConfig(enabled=True))
    request = mock.Mock()
    request.headers = {}
    request.cookies = {}

    encoded_token = server._generate_auth_token(request)

    assert json.loads(base64.b64decode(encoded_token)) == {
        'v': 1,
        'user': None,
        'cookies': {},
    }


@pytest.mark.asyncio
async def test_auth_proxy_finishes_new_user_role_after_cancellation(
        monkeypatch):
    proxy_config = server_config.ExternalProxyConfig(
        enabled=True,
        header_name='X-Auth-Request-Email',
        header_format='plaintext')
    monkeypatch.setattr(auth_middleware.server_config,
                        'load_external_proxy_config', lambda: proxy_config)
    middleware = auth_middleware.AuthProxyMiddleware(
        app=mock.AsyncMock()).middleware
    request = mock.Mock(spec=fastapi.Request)
    request.headers = {'X-Auth-Request-Email': 'user@example.com'}
    request.state = mock.Mock()
    request.state.auth_user = None

    worker_started = threading.Event()
    release_worker = threading.Event()
    role_assigned = threading.Event()

    def add_or_update_user(user):
        del user
        worker_started.set()
        assert release_worker.wait(timeout=5)
        return True

    def add_user_if_not_exists(user_id):
        del user_id
        role_assigned.set()

    monkeypatch.setattr(user_registration.global_user_state,
                        'add_or_update_user', add_or_update_user)
    monkeypatch.setattr(user_registration.permission.permission_service,
                        'add_user_if_not_exists', add_user_if_not_exists)
    call_next = mock.AsyncMock(return_value=fastapi.Response(status_code=204))

    dispatch_task = asyncio.create_task(middleware.dispatch(request, call_next))
    assert await asyncio.to_thread(worker_started.wait, 5)
    dispatch_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dispatch_task
    release_worker.set()

    assert await asyncio.to_thread(role_assigned.wait, 5)
    call_next.assert_not_awaited()


@pytest.mark.asyncio
async def test_user_registration_preserves_existing_user_role(monkeypatch):
    user = mock.Mock(id='existing-user')
    add_or_update_user = mock.Mock(return_value=False)
    add_user_if_not_exists = mock.Mock()
    monkeypatch.setattr(user_registration.global_user_state,
                        'add_or_update_user', add_or_update_user)
    monkeypatch.setattr(user_registration.permission.permission_service,
                        'add_user_if_not_exists', add_user_if_not_exists)

    await user_registration.add_or_update_user_with_default_role(user)

    add_or_update_user.assert_called_once_with(user)
    add_user_if_not_exists.assert_not_called()
