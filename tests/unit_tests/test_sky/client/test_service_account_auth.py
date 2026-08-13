"""Tests for service account authentication client module."""

import os
import unittest.mock as mock

import pytest

from sky.client import service_account_auth
from sky.server import versions
from sky.skylet import constants
from sky.utils import controller_capability

_CONTROLLER_CAPABILITY = 'A' * 43


class TestServiceAccountAuth:
    """Test cases for service account authentication."""

    @mock.patch.dict(
        os.environ, {constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'sky_test_token'})
    def test_get_service_account_token_from_env(self):
        """Test getting service account token from environment variable."""
        token = service_account_auth._get_service_account_token()
        assert token == 'sky_test_token'

    @mock.patch.dict(os.environ, {}, clear=True)
    @mock.patch('sky.skypilot_config.get_nested')
    def test_get_service_account_token_from_config(self, mock_get_nested):
        """Test getting service account token from config file."""
        mock_get_nested.return_value = 'sky_config_token'

        token = service_account_auth._get_service_account_token()
        assert token == 'sky_config_token'

        # Verify the correct config path is used
        mock_get_nested.assert_called_once_with(
            ('api_server', 'service_account_token'), default_value=None)

    @mock.patch.dict(os.environ, {}, clear=True)
    @mock.patch('sky.skypilot_config.get_nested')
    def test_no_service_account_token(self, mock_get_nested):
        """Test no token returned when none available."""
        mock_get_nested.return_value = None

        token = service_account_auth._get_service_account_token()
        assert token is None

    @mock.patch.dict(os.environ,
                     {constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'invalid_token'})
    def test_invalid_token_format_env(self):
        """Test validation of token format from environment."""
        with pytest.raises(ValueError) as exc_info:
            service_account_auth._get_service_account_token()

        assert 'Invalid service account token format' in str(exc_info.value)
        assert 'Token must start with "sky_"' in str(exc_info.value)

    @mock.patch.dict(os.environ, {}, clear=True)
    @mock.patch('sky.skypilot_config.get_nested')
    def test_invalid_token_format_config(self, mock_get_nested):
        """Test validation of token format from config."""
        mock_get_nested.return_value = 'invalid_token'

        with pytest.raises(ValueError) as exc_info:
            service_account_auth._get_service_account_token()

        assert 'Invalid service account token format in config' in str(
            exc_info.value)
        assert 'Token must start with "sky_"' in str(exc_info.value)

    @mock.patch.dict(
        os.environ, {constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'sky_test_token'})
    def test_get_service_account_headers_with_token(self):
        """Test getting headers when token is available."""
        headers = service_account_auth.get_service_account_headers()
        assert headers == {'Authorization': 'Bearer sky_test_token'}

    @mock.patch.dict(os.environ, {}, clear=True)
    @mock.patch('sky.skypilot_config.get_nested')
    def test_get_service_account_headers_no_token(self, mock_get_nested):
        """Test getting headers when no token is available."""
        mock_get_nested.return_value = None

        headers = service_account_auth.get_service_account_headers()
        assert headers == {}

    @mock.patch.dict(os.environ, {
        'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        'SKYPILOT_SERVER_CONTROLLER_GENERATION': '22',
    },
                     clear=True)
    @mock.patch('sky.skypilot_config.get_nested', return_value=None)
    def test_controller_origin_headers_are_server_owned(self, _mock_get_nested):
        with mock.patch.object(controller_capability,
                               'get_process_local',
                               return_value=_CONTROLLER_CAPABILITY):
            headers = service_account_auth.get_service_account_headers()

        assert headers == {
            'X-SkyPilot-Controller-Instance-ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
            'X-SkyPilot-Controller-Generation': '22',
            'X-SkyPilot-Controller-Origin-Capability': _CONTROLLER_CAPABILITY,
        }

    @mock.patch.dict(os.environ, {
        'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        'SKYPILOT_SERVER_CONTROLLER_GENERATION': '22',
    },
                     clear=True)
    @mock.patch('sky.skypilot_config.get_nested', return_value=None)
    def test_controller_origin_headers_use_process_local_authority(
            self, _mock_get_nested):
        controller_capability.install_process_local(_CONTROLLER_CAPABILITY)
        try:
            headers = service_account_auth.get_service_account_headers()
        finally:
            controller_capability.clear_process_local()

        assert headers == {
            'X-SkyPilot-Controller-Instance-ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
            'X-SkyPilot-Controller-Generation': '22',
            'X-SkyPilot-Controller-Origin-Capability': _CONTROLLER_CAPABILITY,
        }

    @mock.patch.dict(
        os.environ, {constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'sky_test_token'},
        clear=True)
    def test_originless_controller_handler_emits_only_ordinary_auth(self):
        from sky.server.requests import executor

        def originless_controller_handler():
            return service_account_auth.get_service_account_headers()

        controller_capability.clear_process_local()
        with executor._controller_execution_environment(
                22, '96d9d1f6-8ba4-402b-85f5-27db321fd504'):
            headers = originless_controller_handler()

        assert headers == {'Authorization': 'Bearer sky_test_token'}

    @mock.patch.dict(os.environ, {
        constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'sky_test_token',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_OWNER_MODE': 'postgres',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_GENERATION': '22',
    },
                     clear=True)
    def test_all_mode_runtime_owner_does_not_authorize_normal_sdk_work(self):
        controller_capability.install_process_local(_CONTROLLER_CAPABILITY)
        try:
            headers = service_account_auth.get_service_account_headers()
        finally:
            controller_capability.clear_process_local()

        assert headers == {'Authorization': 'Bearer sky_test_token'}

    @pytest.mark.parametrize('managed_field', [
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_OWNER_MODE',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_INSTANCE_ID',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_GENERATION',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_OWNER_PID',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_OWNER_START_TICKS',
        'SKYPILOT_SERVER_MANAGED_JOB_ID',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_SLOT_ID',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_READY_FD',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_CAPABILITY_FD',
    ])
    @mock.patch('sky.skypilot_config.get_nested', return_value=None)
    def test_any_managed_authority_field_without_capability_fails_closed(
            self, _mock_get_nested, managed_field):
        with mock.patch.dict(os.environ, {managed_field: '1'}, clear=True):
            with pytest.raises(RuntimeError,
                               match='require controller capability'):
                service_account_auth.get_service_account_headers()

    @mock.patch.dict(os.environ, {
        'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        'SKYPILOT_SERVER_CONTROLLER_GENERATION': '22',
        'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY': _CONTROLLER_CAPABILITY,
    },
                     clear=True)
    @mock.patch('sky.skypilot_config.get_nested', return_value=None)
    def test_environment_authority_is_rejected_even_with_process_local(
            self, _mock_get_nested):
        controller_capability.install_process_local(_CONTROLLER_CAPABILITY)
        try:
            with pytest.raises(RuntimeError, match='must not be inherited'):
                service_account_auth.get_service_account_headers()
        finally:
            controller_capability.clear_process_local()

    @pytest.mark.parametrize('environment', [{
        'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY': _CONTROLLER_CAPABILITY,
    }, {
        'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY': _CONTROLLER_CAPABILITY,
    }, {
        'SKYPILOT_SERVER_CONTROLLER_GENERATION': '22',
        'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY': _CONTROLLER_CAPABILITY,
    }, {
        'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': 'not-a-uuid',
        'SKYPILOT_SERVER_CONTROLLER_GENERATION': '22',
        'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY': _CONTROLLER_CAPABILITY,
    }, {
        'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        'SKYPILOT_SERVER_CONTROLLER_GENERATION': '0',
        'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY': _CONTROLLER_CAPABILITY,
    }, {
        'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        'SKYPILOT_SERVER_CONTROLLER_GENERATION': '22',
        'SKYPILOT_SERVER_CONTROLLER_ORIGIN_CAPABILITY': 'guess',
    }])
    @mock.patch('sky.skypilot_config.get_nested', return_value=None)
    def test_invalid_controller_origin_fails_closed(self, _mock_get_nested,
                                                    environment):
        with mock.patch.dict(os.environ, environment, clear=True):
            with pytest.raises(RuntimeError, match='Controller SDK request'):
                service_account_auth.get_service_account_headers()

    @mock.patch.dict(os.environ, {
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_GENERATION': '22',
        'SKYPILOT_SERVER_MANAGED_JOB_ID': '7',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_SLOT_ID': '2',
        'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT': '907b2c34-2f1f-4d79-ab14-43e5324e8a70',
    },
                     clear=True)
    @mock.patch('sky.skypilot_config.get_nested', return_value=None)
    def test_managed_job_origin_includes_capability_only_as_complete_tuple(
            self, _mock_get_nested):
        with mock.patch.object(controller_capability,
                               'get_process_local',
                               return_value=_CONTROLLER_CAPABILITY):
            headers = service_account_auth.get_service_account_headers()

        assert headers == {
            'X-SkyPilot-Controller-Instance-ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
            'X-SkyPilot-Controller-Generation': '22',
            'X-SkyPilot-Controller-Origin-Capability': _CONTROLLER_CAPABILITY,
            'X-SkyPilot-Managed-Job-ID': '7',
            'X-SkyPilot-Managed-Job-Controller-Slot-ID': '2',
            'X-SkyPilot-Managed-Job-Controller-Slot-Attempt': '907b2c34-2f1f-4d79-ab14-43e5324e8a70',
        }

    @mock.patch.dict(
        os.environ,
        {
            'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
            'SKYPILOT_SERVER_CONTROLLER_GENERATION': '22',
            # Context-local identity intentionally overrides inherited job fields.
            'SKYPILOT_SERVER_MANAGED_JOB_ID': '999',
            'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_SLOT_ID': '8',
            'SKYPILOT_SERVER_MANAGED_JOB_CONTROLLER_SLOT_ATTEMPT': '2cb23690-d752-4da0-a93f-4713b9f5980b',
        },
        clear=True)
    @mock.patch('sky.skypilot_config.get_nested', return_value=None)
    def test_context_local_managed_job_origin_overrides_job_environment(
            self, _mock_get_nested):
        origin = (7, '96d9d1f6-8ba4-402b-85f5-27db321fd504', 22, 2,
                  '907b2c34-2f1f-4d79-ab14-43e5324e8a70')
        token = versions.set_managed_job_origin(origin)
        try:
            with mock.patch.object(controller_capability,
                                   'get_process_local',
                                   return_value=_CONTROLLER_CAPABILITY):
                headers = service_account_auth.get_service_account_headers()
        finally:
            versions.reset_managed_job_origin(token)

        assert headers['X-SkyPilot-Managed-Job-ID'] == '7'
        assert headers['X-SkyPilot-Managed-Job-Controller-Slot-ID'] == '2'
        assert headers['X-SkyPilot-Managed-Job-Controller-Slot-Attempt'] == (
            origin[4])

    @mock.patch.dict(os.environ, {
        'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        'SKYPILOT_SERVER_CONTROLLER_GENERATION': '22',
    },
                     clear=True)
    @mock.patch('sky.skypilot_config.get_nested', return_value=None)
    def test_context_local_origin_rejects_outer_environment_disagreement(
            self, _mock_get_nested):
        origin = (7, '1a536dc5-487d-4566-bfa4-1f401d8d3b5e', 22, 2,
                  '907b2c34-2f1f-4d79-ab14-43e5324e8a70')
        token = versions.set_managed_job_origin(origin)
        try:
            with mock.patch.object(controller_capability,
                                   'get_process_local',
                                   return_value=_CONTROLLER_CAPABILITY):
                with pytest.raises(RuntimeError,
                                   match='context and environment'):
                    service_account_auth.get_service_account_headers()
        finally:
            versions.reset_managed_job_origin(token)

    @mock.patch.dict(os.environ, {
        'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID': '96d9d1f6-8ba4-402b-85f5-27db321fd504',
        'SKYPILOT_SERVER_CONTROLLER_GENERATION': '22',
    },
                     clear=True)
    @mock.patch('sky.skypilot_config.get_nested', return_value=None)
    def test_context_local_origin_resets_without_leakage(
            self, _mock_get_nested):
        origin = (7, '96d9d1f6-8ba4-402b-85f5-27db321fd504', 22, 2,
                  '907b2c34-2f1f-4d79-ab14-43e5324e8a70')
        token = versions.set_managed_job_origin(origin)
        try:
            with mock.patch.object(controller_capability,
                                   'get_process_local',
                                   return_value=_CONTROLLER_CAPABILITY):
                scoped_headers = (
                    service_account_auth.get_service_account_headers())
        finally:
            versions.reset_managed_job_origin(token)
        with mock.patch.object(controller_capability,
                               'get_process_local',
                               return_value=_CONTROLLER_CAPABILITY):
            unscoped_headers = service_account_auth.get_service_account_headers(
            )

        assert 'X-SkyPilot-Managed-Job-ID' in scoped_headers
        assert 'X-SkyPilot-Managed-Job-ID' not in unscoped_headers
        assert versions.get_managed_job_origin() is None

    @mock.patch.dict(
        os.environ, {constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'sky_test_token'})
    @mock.patch('sky.skypilot_config.get_nested')
    def test_env_variable_priority(self, mock_get_nested):
        """Test that environment variable takes priority over config."""
        mock_get_nested.return_value = 'sky_config_token'

        token = service_account_auth._get_service_account_token()
        # Should get env token, not config token
        assert token == 'sky_test_token'

        # Config should not be called when env var is present
        mock_get_nested.assert_not_called()

    @mock.patch.dict(
        os.environ,
        {constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'sky_valid_token'})
    def test_headers_with_valid_env_token(self):
        """Test headers generation with valid environment token."""
        headers = service_account_auth.get_service_account_headers()
        assert headers == {'Authorization': 'Bearer sky_valid_token'}

    @mock.patch.dict(os.environ,
                     {constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: 'invalid_token'})
    def test_headers_with_invalid_env_token(self):
        """Test headers generation fails with invalid environment token."""
        with pytest.raises(ValueError) as exc_info:
            service_account_auth.get_service_account_headers()

        assert 'Invalid service account token format' in str(exc_info.value)

    @mock.patch.dict(os.environ, {}, clear=True)
    @mock.patch('sky.skypilot_config.get_nested')
    def test_headers_with_invalid_config_token(self, mock_get_nested):
        """Test headers generation fails with invalid config token."""
        mock_get_nested.return_value = 'bad_token_format'

        with pytest.raises(ValueError) as exc_info:
            service_account_auth.get_service_account_headers()

        assert 'Invalid service account token format in config' in str(
            exc_info.value)

    @mock.patch.dict(os.environ, {}, clear=True)
    @mock.patch('sky.skypilot_config.get_nested')
    def test_empty_token_from_config(self, mock_get_nested):
        """Test empty/None token from config returns no headers."""
        mock_get_nested.return_value = None

        headers = service_account_auth.get_service_account_headers()
        assert headers == {}

    @mock.patch.dict(os.environ, {}, clear=True)
    @mock.patch('sky.skypilot_config.get_nested')
    def test_valid_config_token(self, mock_get_nested):
        """Test valid token from config works correctly."""
        mock_get_nested.return_value = 'sky_valid_config_token'

        headers = service_account_auth.get_service_account_headers()
        assert headers == {'Authorization': 'Bearer sky_valid_config_token'}

    @mock.patch.dict(os.environ, {constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR: ''})
    def test_empty_env_token_falls_back_to_config(self):
        """Test empty environment token falls back to config."""
        with mock.patch('sky.skypilot_config.get_nested') as mock_get_nested:
            mock_get_nested.return_value = 'sky_config_fallback'

            token = service_account_auth._get_service_account_token()
            assert token == 'sky_config_fallback'

            # Should check config since env token is empty
            mock_get_nested.assert_called_once_with(
                ('api_server', 'service_account_token'), default_value=None)
