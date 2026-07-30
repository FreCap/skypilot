"""Unit tests for sky.server.requests.payloads module."""
import types
from unittest import mock

import pytest

from sky import serve
from sky import skypilot_config
from sky.serve import constants as serve_constants
from sky.server.requests import payloads
from sky.skylet import constants
from sky.usage import usage_lib


@pytest.mark.parametrize(('body_type', 'body_kwargs'), [
    (payloads.ServeUpBody, {
        'task': 'name: task',
        'service_name': 'service',
    }),
    (payloads.ServeUpdateBody, {
        'task': 'name: task',
        'service_name': 'service',
        'mode': serve.UpdateMode.ROLLING,
    }),
    (payloads.JobsPoolApplyBody, {
        'task': 'name: task',
        'workers': 1,
        'pool_name': 'pool',
        'mode': serve.UpdateMode.ROLLING,
    }),
],
                         ids=['serve-up', 'serve-update', 'jobs-pool-apply'])
def test_single_task_payloads_reject_multiple_tasks(body_type, body_kwargs):
    body = body_type(**body_kwargs)
    dag = types.SimpleNamespace(
        tasks=[mock.sentinel.first, mock.sentinel.second])

    with mock.patch.object(payloads.common,
                           'process_mounts_in_task_on_api_server',
                           return_value=dag):
        with pytest.raises(ValueError, match='Must only specify one task'):
            body.to_kwargs()


def test_request_body_env_vars_includes_expected_keys(monkeypatch):
    monkeypatch.setattr(usage_lib.messages.usage, 'run_id', 'run-id')

    server_env = f'{constants.SKYPILOT_SERVER_ENV_VAR_PREFIX}BAR'
    monkeypatch.setenv(server_env, 'server-value')
    monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG,
                       '/tmp/config.yaml')
    monkeypatch.setenv(skypilot_config.ENV_VAR_GLOBAL_CONFIG,
                       '/tmp/global.yaml')
    monkeypatch.setenv(skypilot_config.ENV_VAR_PROJECT_CONFIG,
                       '/tmp/project.yaml')
    monkeypatch.setenv(constants.ENV_VAR_DB_CONNECTION_URI, 'db-uri')
    monkeypatch.setenv(serve_constants.EXTERNAL_LB_ENABLED_ENV_VAR, 'false')
    monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'controller')
    monkeypatch.setenv('SKYPILOT_CONTROLLER_CUTOVER_QUIESCENCE_SECONDS', '70')
    monkeypatch.setenv('SKYPILOT_POD_UID', 'pod-uid')
    monkeypatch.setenv(constants.SKY_API_SERVER_URL_ENV_VAR,
                       'http://stable-api-service')
    monkeypatch.setenv(constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR,
                       'sky_server_token')

    monkeypatch.setattr(payloads.common, 'is_api_server_local', lambda: True)
    local_env = payloads.request_body_env_vars()
    assert server_env not in local_env
    assert local_env[
        skypilot_config.ENV_VAR_SKYPILOT_CONFIG] == '/tmp/config.yaml'
    assert constants.ENV_VAR_DB_CONNECTION_URI not in local_env
    assert serve_constants.EXTERNAL_LB_ENABLED_ENV_VAR not in local_env
    assert 'SKYPILOT_API_SERVER_ROLE' not in local_env
    assert 'SKYPILOT_CONTROLLER_CUTOVER_QUIESCENCE_SECONDS' not in local_env
    assert 'SKYPILOT_POD_UID' not in local_env
    assert constants.SKY_API_SERVER_URL_ENV_VAR not in local_env
    assert constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR not in local_env
    assert skypilot_config.ENV_VAR_GLOBAL_CONFIG not in local_env
    assert skypilot_config.ENV_VAR_PROJECT_CONFIG not in local_env

    monkeypatch.setattr(payloads.common, 'is_api_server_local', lambda: False)
    remote_env = payloads.request_body_env_vars()
    assert 'AWS_PROFILE' not in remote_env
    assert skypilot_config.ENV_VAR_SKYPILOT_CONFIG not in remote_env
    assert skypilot_config.ENV_VAR_GLOBAL_CONFIG not in remote_env
    assert skypilot_config.ENV_VAR_PROJECT_CONFIG not in remote_env
    assert constants.CLIENT_USER_HASH_ENV_VAR not in remote_env
    assert serve_constants.EXTERNAL_LB_ENABLED_ENV_VAR not in remote_env
    assert 'SKYPILOT_API_SERVER_ROLE' not in remote_env
    assert 'SKYPILOT_CONTROLLER_CUTOVER_QUIESCENCE_SECONDS' not in remote_env
    assert 'SKYPILOT_POD_UID' not in remote_env
    assert constants.SKY_API_SERVER_URL_ENV_VAR not in remote_env
    assert constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR not in remote_env


def test_request_body_env_vars_client_user_hash_with_basic_auth(monkeypatch):
    """client user hash env var is included when basic auth is enabled."""
    monkeypatch.setattr(usage_lib.messages.usage, 'run_id', 'run-id')
    monkeypatch.setattr(payloads.common, 'is_api_server_local', lambda: True)
    monkeypatch.setattr(payloads.common, 'basic_auth_enabled', True)
    monkeypatch.setattr(payloads.common, 'client_user_hash', 'abcd1234')

    env_vars = payloads.request_body_env_vars()
    assert env_vars[constants.CLIENT_USER_HASH_ENV_VAR] == 'abcd1234'


def test_request_body_env_vars_client_user_hash_none_with_basic_auth(
        monkeypatch):
    """client user hash env var is skipped when basic auth is enabled but hash is None."""
    monkeypatch.setattr(usage_lib.messages.usage, 'run_id', 'run-id')
    monkeypatch.setattr(payloads.common, 'is_api_server_local', lambda: True)
    monkeypatch.setattr(payloads.common, 'basic_auth_enabled', True)
    monkeypatch.setattr(payloads.common, 'client_user_hash', None)

    env_vars = payloads.request_body_env_vars()
    assert constants.CLIENT_USER_HASH_ENV_VAR not in env_vars


def test_persisted_payload_strips_server_owned_kubernetes_autoscaler():
    body = payloads.ServeUpBody(task='name: task',
                                service_name='service',
                                override_skypilot_config={
                                    'active_workspace': 'workspace',
                                    'kubernetes': {
                                        'autoscaler': 'generic',
                                        'ports': 'podip',
                                        'context_configs': {
                                            'research': {
                                                'autoscaler': 'generic',
                                                'provision_timeout': 15,
                                            },
                                            'other': {
                                                'provision_timeout': 30,
                                            },
                                        },
                                    },
                                })

    payloads.validate_task_request_body_for_persistence(body)

    assert body.override_skypilot_config == {
        'active_workspace': 'workspace',
        'kubernetes': {
            'ports': 'podip',
            'context_configs': {
                'research': {
                    'provision_timeout': 15,
                },
                'other': {
                    'provision_timeout': 30,
                },
            },
        },
    }
