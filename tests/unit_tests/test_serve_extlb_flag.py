"""The extlb topology flag must come from the LIVE server config.

Consolidation-mode controllers run under a per-service SKYPILOT_CONFIG
snapshot frozen at `serve up`. Reading the flag from the loaded config
split-brained pre-flag services: the server (live DB config) advertised
the external-LB DNS while the controller (snapshot) kept an in-pod LB and
never created the LB objects — a permanently dangling endpoint (observed
live on boltz-l4-fleet).
"""
# pylint: disable=protected-access
import unittest
from unittest import mock

from sky import skypilot_config
from sky.serve import constants
from sky.serve import serve_utils
from sky.skylet import constants as skylet_constants
from sky.utils import config_utils


def _config(flag):
    return config_utils.Config(
        {'serve': {
            'controller': {
                'external_load_balancer': flag
            }
        }})


class TestExtlbFlagSource(unittest.TestCase):

    def setUp(self):
        serve_utils._external_lb_mode_cache = None

    def tearDown(self):
        serve_utils._external_lb_mode_cache = None

    def test_server_env_reads_live_server_config_not_snapshot(self):
        # Loaded (snapshot) config says OFF; live server config says ON.
        with mock.patch.dict(
                serve_utils.os.environ,
                {skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER: 'true'}), \
             mock.patch.object(skypilot_config, 'get_nested',
                               return_value=False) as snapshot_read, \
             mock.patch.object(skypilot_config, 'get_effective_server_config',
                               return_value=_config(True)):
            self.assertTrue(serve_utils.is_external_load_balancer_mode())
        snapshot_read.assert_not_called()

    def test_client_env_unchanged(self):
        with mock.patch.dict(serve_utils.os.environ, {}, clear=False):
            serve_utils.os.environ.pop(
                skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER, None)
            with mock.patch.object(
                    skypilot_config, 'get_nested',
                    return_value=True) as snapshot_read, \
                 mock.patch.object(
                    skypilot_config,
                    'get_effective_server_config') as server_read:
                self.assertTrue(serve_utils.is_external_load_balancer_mode())
        snapshot_read.assert_called_once()
        server_read.assert_not_called()

    def test_server_value_cached_per_process(self):
        with mock.patch.dict(
                serve_utils.os.environ,
                {skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER: 'true'}), \
             mock.patch.object(skypilot_config, 'get_effective_server_config',
                               return_value=_config(True)) as server_read:
            self.assertTrue(serve_utils.is_external_load_balancer_mode())
            self.assertTrue(serve_utils.is_external_load_balancer_mode())
        server_read.assert_called_once()

    def test_platform_env_is_single_source_of_truth(self):
        with mock.patch.dict(
                serve_utils.os.environ, {
                    constants.EXTERNAL_LB_ENABLED_ENV_VAR: 'true',
                    skylet_constants.ENV_VAR_IS_SKYPILOT_SERVER: 'true',
                }), \
             mock.patch.object(
                 skypilot_config,
                 'get_effective_server_config') as server_read:
            self.assertTrue(serve_utils.is_external_load_balancer_mode())
        server_read.assert_not_called()

    def test_platform_env_forces_serve_consolidation(self):
        # External-only mode cannot provision a dedicated controller VM. The
        # Helm capability signal therefore implies consolidation even when an
        # older persisted config omitted the separate setting.
        with mock.patch.dict(
                serve_utils.os.environ,
                {constants.EXTERNAL_LB_ENABLED_ENV_VAR: 'true'}), \
             mock.patch.object(skypilot_config,
                               'get_nested') as config_read:
            self.assertTrue(serve_utils.is_consolidation_mode(pool=False))
        config_read.assert_not_called()

    def test_platform_env_does_not_change_pool_consolidation(self):
        with mock.patch.dict(
                serve_utils.os.environ,
                {constants.EXTERNAL_LB_ENABLED_ENV_VAR: 'true'}), \
             mock.patch.object(
                 serve_utils.controller_utils,
                 'is_jobs_consolidation_mode',
                 return_value=False) as jobs_consolidation:
            self.assertFalse(serve_utils.is_consolidation_mode(pool=True))
        jobs_consolidation.assert_called_once()
