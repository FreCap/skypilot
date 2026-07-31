import copy
import os
import pathlib
import re
import unittest
from unittest import mock

from seed_config import _read_bool_env
from seed_config import deep_merge
from seed_config import PRUNE_RETIRED_KEYS_ENV
from seed_config import prune_retired_serve_controller_keys
from seed_config import seed
from sqlalchemy.engine import Engine
import yaml


def _engine_with_config(config: object) -> tuple[Engine, mock.MagicMock]:
    engine = mock.MagicMock(spec=Engine)
    connection = mock.MagicMock()
    engine.begin.return_value.__enter__.return_value = connection
    connection.execute.return_value.fetchone.return_value = (yaml.safe_dump(
        config, sort_keys=False),)
    return engine, connection


def _written_config(connection: mock.MagicMock) -> dict:
    parameters = connection.execute.call_args_list[1].args[1]
    return yaml.safe_load(parameters['v'])


class DeepMergeTest(unittest.TestCase):

    def test_nested_mappings_merge_and_lists_replace(self) -> None:
        base = {
            'aws': {
                'use_ssm': False,
                'regions': ['us-east-1'],
            },
            'runtime_only': True,
        }
        override = {
            'aws': {
                'use_ssm': True,
                'regions': ['us-west-2'],
            },
        }

        self.assertEqual(
            deep_merge(base, override),
            {
                'aws': {
                    'use_ssm': True,
                    'regions': ['us-west-2'],
                },
                'runtime_only': True,
            },
        )

    def test_does_not_mutate_inputs(self) -> None:
        base = {'nested': {'left': 1}}
        override = {'nested': {'right': 2}}
        original_base = copy.deepcopy(base)
        original_override = copy.deepcopy(override)

        deep_merge(base, override)

        self.assertEqual(base, original_base)
        self.assertEqual(override, original_override)


class PruneRetiredServeControllerKeysTest(unittest.TestCase):

    def test_prunes_only_retired_keys(self) -> None:
        existing = {
            'allowed_clouds': ['aws'],
            'serve': {
                'bucket': 's3://controller-state',
                'controller': {
                    'consolidation_mode': True,
                    'external_load_balancer': True,
                    'resources': {
                        'cpus': '4+'
                    },
                },
            },
        }
        original = copy.deepcopy(existing)

        self.assertEqual(
            prune_retired_serve_controller_keys(existing),
            {
                'allowed_clouds': ['aws'],
                'serve': {
                    'bucket': 's3://controller-state',
                    'controller': {
                        'resources': {
                            'cpus': '4+'
                        }
                    },
                },
            },
        )
        self.assertEqual(existing, original)

    def test_collapses_empty_parent_mappings(self) -> None:
        config = {
            'allowed_clouds': ['aws'],
            'serve': {
                'controller': {
                    'consolidation_mode': True,
                    'external_load_balancer': True,
                }
            },
        }

        self.assertEqual(
            prune_retired_serve_controller_keys(config),
            {'allowed_clouds': ['aws']},
        )

    def test_rejects_non_mapping_parents_without_mutation(self) -> None:
        for config in ({'serve': None}, {'serve': {'controller': 'enabled'}}):
            with self.subTest(config=config):
                original = copy.deepcopy(config)
                with self.assertRaisesRegex(ValueError, 'not a mapping'):
                    prune_retired_serve_controller_keys(config)
                self.assertEqual(config, original)

    def test_leaves_config_without_controller_unchanged(self) -> None:
        for config in (
            {
                'allowed_clouds': ['aws']
            },
            {
                'serve': {
                    'bucket': 's3://controller-state'
                }
            },
        ):
            with self.subTest(config=config):
                self.assertEqual(
                    prune_retired_serve_controller_keys(config),
                    config,
                )


class SeedTest(unittest.TestCase):

    def test_runtime_keys_survive_and_workspaces_replace_wholesale(
            self) -> None:
        existing = {
            'runtime_only': True,
            'workspaces': {
                'retired': {
                    'private': True
                }
            },
        }
        engine, connection = _engine_with_config(existing)

        seed(engine, {'workspaces': {'current': {'private': True}}})

        self.assertEqual(
            _written_config(connection),
            {
                'runtime_only': True,
                'workspaces': {
                    'current': {
                        'private': True
                    }
                },
            },
        )

    def test_current_config_is_a_noop(self) -> None:
        engine, connection = _engine_with_config({'allowed_clouds': ['aws']})

        seed(engine, {'allowed_clouds': ['aws']})

        self.assertEqual(connection.execute.call_count, 1)

    def test_opt_in_false_preserves_retired_keys_during_write(self) -> None:
        existing = {
            'serve': {
                'controller': {
                    'consolidation_mode': True,
                    'external_load_balancer': True,
                }
            }
        }
        engine, connection = _engine_with_config(existing)

        seed(engine, {'allowed_clouds': ['aws']})

        self.assertEqual(connection.execute.call_count, 2)
        written = _written_config(connection)
        self.assertEqual(written['serve'], existing['serve'])
        self.assertEqual(written['allowed_clouds'], ['aws'])

    def test_opt_in_prunes_retired_keys(self) -> None:
        existing = {
            'allowed_clouds': ['aws'],
            'serve': {
                'controller': {
                    'consolidation_mode': True,
                    'external_load_balancer': True,
                }
            },
        }
        engine, connection = _engine_with_config(existing)

        seed(engine, {}, prune_retired_controller_keys=True)

        self.assertEqual(
            _written_config(connection),
            {'allowed_clouds': ['aws']},
        )

    def test_malformed_nested_mapping_is_rejected_only_when_pruning(
            self) -> None:
        malformed_configs = (
            {
                'serve': None
            },
            {
                'serve': {
                    'controller': ['unexpected']
                }
            },
        )
        for existing in malformed_configs:
            with self.subTest(existing=existing, pruning=False):
                engine, connection = _engine_with_config(existing)
                seed(engine, {})
                self.assertEqual(connection.execute.call_count, 1)

            with self.subTest(existing=existing, pruning=True):
                engine, connection = _engine_with_config(existing)
                with self.assertRaisesRegex(SystemExit, 'not a mapping'):
                    seed(engine, {}, prune_retired_controller_keys=True)
                self.assertEqual(connection.execute.call_count, 1)

    def test_pruning_does_not_hide_malformed_persisted_path_behind_override(
            self) -> None:
        engine, connection = _engine_with_config({'serve': None})
        desired = {'serve': {'controller': {'resources': {'cpus': '4+'}}}}

        with self.assertRaisesRegex(SystemExit, 'not a mapping'):
            seed(engine, desired, prune_retired_controller_keys=True)

        self.assertEqual(connection.execute.call_count, 1)

    def test_non_mapping_root_is_never_overwritten(self) -> None:
        engine, connection = _engine_with_config(['unexpected'])

        with self.assertRaisesRegex(SystemExit,
                                    'api_server_config is not a mapping'):
            seed(engine, {})

        self.assertEqual(connection.execute.call_count, 1)


class ReadBoolEnvTest(unittest.TestCase):

    def test_defaults_to_false(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(_read_bool_env(PRUNE_RETIRED_KEYS_ENV))

    def test_accepts_explicit_values(self) -> None:
        for raw_value, expected in (('false', False), ('true', True)):
            with self.subTest(raw_value=raw_value):
                with mock.patch.dict(
                        os.environ,
                    {PRUNE_RETIRED_KEYS_ENV: raw_value},
                        clear=True,
                ):
                    self.assertIs(
                        _read_bool_env(PRUNE_RETIRED_KEYS_ENV),
                        expected,
                    )

    def test_rejects_invalid_value(self) -> None:
        with mock.patch.dict(
                os.environ,
            {PRUNE_RETIRED_KEYS_ENV: 'yes'},
                clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, 'must be true or false'):
                _read_bool_env(PRUNE_RETIRED_KEYS_ENV)


class ControlPlaneModuleSourceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        module_dir = pathlib.Path(__file__).resolve().parents[1]
        cls.module_hcl = (module_dir / 'skypilot.tf').read_text()
        cls.config_seed_hcl = (module_dir / 'config_seed.tf').read_text()
        cls.variables_hcl = (module_dir / 'variables.tf').read_text()

    def test_module_rejects_duplicate_api_server_extra_env_names(self) -> None:
        variable = re.search(
            r'variable "api_server_extra_envs" \{(?P<body>.*?)\n\}',
            self.variables_hcl,
            re.DOTALL,
        )
        self.assertIsNotNone(variable)
        assert variable is not None
        body = variable.group('body')
        self.assertIn('length(var.api_server_extra_envs) == length(toset([',
                      body)
        self.assertIn('for env in var.api_server_extra_envs : env.name', body)
        self.assertIn(
            'api_server_extra_envs must not contain duplicate names',
            body,
        )

    def test_assembled_extra_envs_reject_generated_user_collision(self) -> None:
        assembly = re.search(
            r'all_extra_envs\s*=\s*concat\(\s*'
            r'jsondecode\(jsonencode\(local\.gcp_envs\)\),\s*'
            r'jsondecode\(jsonencode\(var\.api_server_extra_envs\)\),\s*'
            r'jsondecode\(jsonencode\(local\.catalog_envs\)\),\s*'
            r'\)',
            self.module_hcl,
        )
        self.assertIsNotNone(assembly)
        self.assertIn(
            'for env in local.all_extra_envs : env.name',
            self.module_hcl,
        )
        self.assertIn('duplicate_extra_env_names = toset([', self.module_hcl)
        self.assertIn('if candidate == env_name', self.module_hcl)
        self.assertIn(
            'condition     = length(local.duplicate_extra_env_names) == 0',
            self.module_hcl,
        )
        self.assertIn(
            'after assembling generated and user-provided values',
            self.module_hcl,
        )
        self.assertIn('{ name = "GOOGLE_APPLICATION_CREDENTIALS"',
                      self.module_hcl)

    def test_extra_helm_values_blank_default_normalizes_to_map(self) -> None:
        variable = re.search(
            r'variable "extra_helm_values" \{(?P<body>.*?)\n\}',
            self.variables_hcl,
            re.DOTALL,
        )
        self.assertIsNotNone(variable)
        assert variable is not None
        self.assertIn('default     = ""', variable.group('body'))
        self.assertIn(
            'trimspace(var.extra_helm_values) == "" ? "{}" : '
            'var.extra_helm_values',
            self.module_hcl,
        )

    def test_extra_helm_values_requires_map_shapes(self) -> None:
        required_fragments = (
            'extra_helm_values_decoded = try(',
            'yamldecode(local.extra_helm_values_normalized)',
            'extra_helm_values_is_map = can(keys('
            'local.extra_helm_values_decoded))',
            'extra_helm_api_service_present = contains(',
            'can(keys(try(local.extra_helm_values_decoded.apiService, null)))',
            'condition     = local.extra_helm_values_valid_yaml',
            'condition     = local.extra_helm_values_is_map',
            'condition     = local.extra_helm_api_service_is_map',
            'extra_helm_values.apiService must be a YAML map when present',
        )
        for fragment in required_fragments:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, self.module_hcl)

    def test_extra_helm_values_cannot_replace_module_owned_arrays(self) -> None:
        owned_keys = re.search(
            r'module_owned_api_service_array_keys\s*=\s*toset\(\['
            r'(?P<body>.*?)\n\s*\]\)',
            self.module_hcl,
            re.DOTALL,
        )
        self.assertIsNotNone(owned_keys)
        assert owned_keys is not None
        self.assertEqual(
            set(re.findall(r'"([^"]+)"', owned_keys.group('body'))),
            {'extraEnvs', 'extraVolumes', 'extraVolumeMounts'},
        )
        top_level_owned_keys = re.search(
            r'module_owned_top_level_array_keys\s*=\s*toset\(\['
            r'(?P<body>.*?)\n\s*\]\)',
            self.module_hcl,
            re.DOTALL,
        )
        self.assertIsNotNone(top_level_owned_keys)
        assert top_level_owned_keys is not None
        self.assertEqual(
            set(re.findall(r'"([^"]+)"', top_level_owned_keys.group('body'))),
            {'extraInitContainers'},
        )
        self.assertIn(
            'condition     = length(local.redefined_api_service_array_keys) '
            '== 0 && length(local.redefined_top_level_array_keys) == 0',
            self.module_hcl,
        )
        self.assertIn(
            'extra_helm_values must not redefine apiService.extraEnvs',
            self.module_hcl,
        )

    def test_pruning_behavior_is_part_of_seed_generation(self) -> None:
        variable = re.search(
            r'variable "prune_retired_serve_controller_keys" '
            r'\{(?P<body>.*?)\n\}',
            self.variables_hcl,
            re.DOTALL,
        )
        self.assertIsNotNone(variable)
        assert variable is not None
        self.assertRegex(variable.group('body'),
                         r'(?m)^\s*default\s*=\s*false$')
        self.assertIn(
            'prune_retired_serve_controller_keys = '
            'var.prune_retired_serve_controller_keys',
            self.config_seed_hcl,
        )
        self.assertIn(
            'name  = "SKYPILOT_PRUNE_RETIRED_SERVE_CONTROLLER_KEYS"',
            self.config_seed_hcl,
        )

    def test_helper_image_is_part_of_seed_generation(self) -> None:
        self.assertIn(
            'seed_image = var.operations_helper_image != null ? '
            'var.operations_helper_image : (',
            self.config_seed_hcl,
        )
        generation = re.search(
            r'config_hash = substr\(sha256\(jsonencode\(\{(?P<body>.*?)\}\)\)',
            self.config_seed_hcl,
            re.DOTALL,
        )
        self.assertIsNotNone(generation)
        assert generation is not None
        self.assertRegex(
            generation.group('body'),
            r'(?m)^\s*image\s*=\s*local\.seed_image$',
        )
        self.assertIn(
            'name      = "skypilot-seed-config-${local.config_hash}"',
            self.config_seed_hcl,
        )

    def test_api_restart_uses_helm_readiness_budget(self) -> None:
        self.assertRegex(
            self.config_seed_hcl,
            r'(?m)^\s*api_server_rollout_timeout_seconds\s*=\s*600$',
        )
        self.assertIn(
            '--timeout=${local.api_server_rollout_timeout_seconds}s',
            self.config_seed_hcl,
        )
        self.assertRegex(
            self.module_hcl,
            r'(?m)^\s*timeout\s*=\s*'
            r'local\.api_server_rollout_timeout_seconds$',
        )


if __name__ == '__main__':
    unittest.main()
