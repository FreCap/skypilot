import copy
import os
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


if __name__ == '__main__':
    unittest.main()
