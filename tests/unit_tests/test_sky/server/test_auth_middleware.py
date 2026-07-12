"""Characterization tests for API server authentication middleware."""

import base64
import hashlib
import json
import pickle
from unittest import mock

from sky.server import config as server_config
from sky.server import server
from sky.server.auth import middleware as auth_middleware
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
