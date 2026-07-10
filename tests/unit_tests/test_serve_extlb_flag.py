"""External-LB capability is a process environment contract."""
from unittest import mock

import jsonschema
import pytest

from sky import skypilot_config
from sky.serve import constants
from sky.serve import serve_utils
from sky.utils import schemas


@pytest.mark.parametrize(
    ('value', 'expected'),
    [('true', True), ('TRUE', True), ('false', False), (None, False)],
)
def test_external_lb_capability_uses_only_environment(value, expected):
    env = ({
        constants.EXTERNAL_LB_ENABLED_ENV_VAR: value
    } if value is not None else {})
    # Persisted snapshot and live-server config must never participate, even
    # when they would disagree with the explicit platform capability.
    with mock.patch.dict(serve_utils.os.environ, env, clear=True), \
         mock.patch.object(skypilot_config,
                           'get_nested',
                           side_effect=AssertionError('snapshot read')), \
         mock.patch.object(
             skypilot_config,
             'get_effective_server_config',
             side_effect=AssertionError('live server config read')):
        assert serve_utils.is_external_load_balancer_mode() is expected


def test_platform_env_forces_serve_consolidation():
    # External-only mode cannot provision a dedicated controller VM. The API
    # pod's capability therefore implies consolidation without another knob.
    with mock.patch.dict(
            serve_utils.os.environ,
            {constants.EXTERNAL_LB_ENABLED_ENV_VAR: 'true'}), \
         mock.patch.object(skypilot_config, 'get_nested') as config_read:
        assert serve_utils.is_consolidation_mode(pool=False)
    config_read.assert_not_called()


def test_platform_env_does_not_change_pool_consolidation():
    with mock.patch.dict(
            serve_utils.os.environ,
            {constants.EXTERNAL_LB_ENABLED_ENV_VAR: 'true'}), \
         mock.patch.object(
             serve_utils.controller_utils,
             'is_jobs_consolidation_mode',
             return_value=False) as jobs_consolidation:
        assert not serve_utils.is_consolidation_mode(pool=True)
    jobs_consolidation.assert_called_once()


def test_legacy_flag_is_serve_only_schema_compatibility():
    schema = schemas.get_config_schema()
    legacy_value = {'controller': {'external_load_balancer': True}}
    # Existing persisted Serve configs remain readable across the rollout,
    # although the value is ignored at runtime.
    jsonschema.validate(legacy_value, schema['properties']['serve'])
    # The shared controller-schema helper must not accidentally expose a
    # nonsensical jobs.controller.external_load_balancer setting.
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(legacy_value, schema['properties']['jobs'])
