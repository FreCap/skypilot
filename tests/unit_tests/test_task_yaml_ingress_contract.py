"""Characterization tests for the Task YAML ingress facade."""

# pylint: disable=protected-access

import copy
import inspect
from typing import Any, get_type_hints
from unittest import mock

import pytest

from sky import resources as resources_lib
from sky import task
from sky.data import storage as storage_lib
from sky.serve import service_spec
from sky.utils import yaml_utils


def test_from_yaml_config_preserves_facade_and_consumes_config() -> None:
    descriptor = inspect.getattr_static(task.Task, 'from_yaml_config')
    assert isinstance(descriptor, staticmethod)
    signature = inspect.signature(task.Task.from_yaml_config)
    assert tuple(signature.parameters) == ('config', 'env_overrides',
                                           'secrets_overrides')
    for parameter in signature.parameters.values():
        assert parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert signature.parameters['config'].default is inspect.Parameter.empty
    assert signature.parameters['env_overrides'].default is None
    assert signature.parameters['secrets_overrides'].default is None
    assert get_type_hints(task.Task.from_yaml_config) == {
        'config': dict[str, Any],
        'env_overrides': list[tuple[str, str]] | None,
        'secrets_overrides': list[tuple[str, str]] | None,
        'return': task.Task,
    }
    assert task.Task.from_yaml_config.__module__ == 'sky.task'
    assert task.Task.from_yaml_config.__qualname__ == 'Task.from_yaml_config'

    config = {
        'name': 'yaml-ingress',
        'run': 'echo $PUBLIC',
        'envs': {
            'PUBLIC': 'visible',
        },
        'secrets': {
            'TOKEN': 'private',
        },
        'resources': {
            'cpus': '2+',
        },
    }
    original = copy.deepcopy(config)
    parsed = task.Task.from_yaml_config(config)

    assert not config
    assert parsed.name == 'yaml-ingress'
    assert parsed.run == 'echo $PUBLIC'
    assert parsed.envs == {'PUBLIC': 'visible'}
    assert task.get_plaintext_secrets(parsed.secrets) == {'TOKEN': 'private'}
    assert yaml_utils.safe_load(parsed._user_specified_yaml) == original


def test_from_yaml_config_preserves_ingress_helper_facades() -> None:
    assert task._fill_in_env_vars({'path': '$ROOT/data'}, {'ROOT': '/mnt'}) == {
        'path': '/mnt/data'
    }
    assert task._parse_secret_name('secrets:workspace.token') == ('token',
                                                                  'workspace')


def test_from_yaml_config_preserves_overrides_and_secret_refs() -> None:
    config = {
        'envs': {
            'MODEL': None,
        },
        'secrets': ['secrets:workspace.api_token'],
        'file_mounts': {
            '/models/$MODEL': 's3://models/${MODEL}',
        },
        'volumes': {
            '/cache': '${MODEL}-cache',
        },
    }

    parsed = task.Task.from_yaml_config(
        config,
        env_overrides=[('MODEL', 'llama')],
        secrets_overrides=[('INLINE_TOKEN', 'override')],
    )

    assert not config
    assert parsed.envs == {'MODEL': 'llama'}
    assert task.get_plaintext_secrets(parsed.secrets) == {
        'INLINE_TOKEN': 'override'
    }
    assert parsed.file_mounts == {'/models/llama': 's3://models/llama'}
    assert parsed.volumes == {'/cache': 'llama-cache'}
    assert parsed.managed_secret_refs == [
        task.ManagedSecretRef(name='api_token', scope_override='workspace')
    ]


def test_from_yaml_config_preserves_nested_constructor_cardinality() -> None:
    config = {
        'run': 'python3 -m http.server 8080',
        'file_mounts': {
            '/dataset': {
                'name': 'dataset',
                'source': None,
                'store': None,
                'persistent': True,
            },
        },
        'resources': {
            'ports': 8080,
            'cpus': '2+',
        },
        'service': {
            'readiness_probe': '/',
            'replicas': 1,
        },
    }

    with mock.patch.object(
            storage_lib.Storage,
            'from_yaml_config',
            wraps=storage_lib.Storage.from_yaml_config) as storage_from_yaml, \
         mock.patch.object(
             resources_lib.Resources,
             'from_yaml_config',
             wraps=resources_lib.Resources.from_yaml_config) as resources_from_yaml, \
         mock.patch.object(
             service_spec.SkyServiceSpec,
             'from_yaml_config',
             wraps=service_spec.SkyServiceSpec.from_yaml_config) as service_from_yaml:
        parsed = task.Task.from_yaml_config(config)

    assert not config
    assert parsed.storage_mounts['/dataset'].name == 'dataset'
    assert len(parsed.resources) == 1
    assert parsed.service is not None
    storage_from_yaml.assert_called_once()
    resources_from_yaml.assert_called_once()
    service_from_yaml.assert_called_once()


@pytest.mark.parametrize(
    ('config', 'message'),
    [
        ({
            'envs': {
                'MISSING': None
            }
        }, "Environment variable 'MISSING' is None"),
        ({
            'service': {},
            'pool': {}
        }, 'Cannot set both service and pool in the same task'),
    ],
)
def test_from_yaml_config_preserves_representative_errors(
    config: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        task.Task.from_yaml_config(config)
