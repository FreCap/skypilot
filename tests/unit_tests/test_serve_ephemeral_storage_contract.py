"""Tests for SkyServe's typed ephemeral-storage cleanup contract."""

import copy

import pytest

from sky import task as task_lib
from sky.serve import constants
from sky.serve import ephemeral_storage_contract as contract
from sky.serve import serve_utils
from sky.utils import yaml_utils

_RESOURCE_SCOPE = 'incarnation-a'
_STORAGE_GENERATION = 'generation-1'


def _scope(resource_scope: object = _RESOURCE_SCOPE,
           storage_generation: object = _STORAGE_GENERATION,
           storage_mounts: object = None) -> dict[str, object]:
    if storage_mounts is None:
        storage_mounts = []
    scope_id = contract.canonical_ephemeral_storage_scope_id(
        _RESOURCE_SCOPE, _STORAGE_GENERATION)
    return {
        'resource_scope': resource_scope,
        'scope_id': scope_id,
        'storage_generation': storage_generation,
        'storage_mounts': storage_mounts,
    }


def _yaml(scope: object = None, **top_level: object) -> str:
    if scope is None:
        scope = _scope()
    config = {
        '_metadata': {
            constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY: scope,
        },
        **top_level,
    }
    return yaml_utils.dump_yaml_str(config)


@pytest.mark.parametrize(('resource_scope', 'storage_generation'), [
    ('incarnation-a', 'generation-1'),
    ('a/b:c', '00000000000000000000000000000000'),
    ('scope-é', 'generation-雪'),
])
def test_canonical_scope_id_matches_writer(resource_scope: str,
                                           storage_generation: str) -> None:
    assert contract.canonical_ephemeral_storage_scope_id(
        resource_scope,
        storage_generation) == serve_utils.generate_ephemeral_storage_scope_id(
            resource_scope, storage_generation)


def test_parse_exact_internal_metadata_scope() -> None:
    parsed = contract.parse_ephemeral_storage_scope(_yaml())

    assert parsed == contract.EphemeralStorageScope(
        resource_scope=_RESOURCE_SCOPE,
        scope_id=contract.canonical_ephemeral_storage_scope_id(
            _RESOURCE_SCOPE, _STORAGE_GENERATION),
        storage_generation=_STORAGE_GENERATION,
        storage_mounts=(),
    )


def test_parse_scope_from_task_yaml_serialization() -> None:
    task = task_lib.Task(
        _metadata={constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY: _scope()})
    yaml_content = yaml_utils.dump_yaml_str(task.to_yaml_config())

    assert contract.parse_ephemeral_storage_scope(
        yaml_content) == contract.EphemeralStorageScope(
            resource_scope=_RESOURCE_SCOPE,
            scope_id=contract.canonical_ephemeral_storage_scope_id(
                _RESOURCE_SCOPE, _STORAGE_GENERATION),
            storage_generation=_STORAGE_GENERATION,
            storage_mounts=(),
        )


@pytest.mark.parametrize('yaml_content', [None, '{}', '_metadata: {}'])
def test_absent_scope_returns_none(yaml_content: str | None) -> None:
    assert contract.parse_ephemeral_storage_scope(yaml_content) is None


def test_storage_scope_under_metadata_alias_fails_closed() -> None:
    yaml_content = yaml_utils.dump_yaml_str({
        'metadata': {
            constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY: _scope(),
        }
    })

    with pytest.raises(contract.EphemeralStorageContractError):
        contract.parse_ephemeral_storage_scope(yaml_content)


def test_unrelated_metadata_does_not_create_a_scope() -> None:
    yaml_content = yaml_utils.dump_yaml_str(
        {'metadata': {
            'user_annotation': 'not-a-cleanup-contract'
        }})

    assert contract.parse_ephemeral_storage_scope(yaml_content) is None


@pytest.mark.parametrize('yaml_content', [
    '',
    '[]',
    '_metadata: false',
    f'_metadata:\n  {constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY}: '
    'false',
    '_metadata: &metadata\n  '
    f'{constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY}: *metadata',
    '_metadata:\n  key: first\n  key: second',
])
def test_malformed_yaml_or_metadata_fails_closed(yaml_content: str) -> None:
    with pytest.raises(contract.EphemeralStorageContractError):
        contract.parse_ephemeral_storage_scope(yaml_content)


@pytest.mark.parametrize(('field', 'value'), [
    ('resource_scope', ''),
    ('resource_scope', False),
    ('scope_id', ''),
    ('scope_id', True),
    ('scope_id', 'sv0000000000'),
    ('storage_generation', ''),
    ('storage_generation', False),
    ('storage_mounts', {}),
    ('storage_mounts', [False]),
])
def test_scope_field_types_and_identity_fail_closed(field: str,
                                                    value: object) -> None:
    scope = _scope()
    scope[field] = value

    with pytest.raises(contract.EphemeralStorageContractError):
        contract.parse_ephemeral_storage_scope(_yaml(scope))


@pytest.mark.parametrize('mutation', ['missing', 'unknown'])
def test_partial_or_unknown_scope_fields_fail_closed(mutation: str) -> None:
    scope = _scope()
    if mutation == 'missing':
        del scope['storage_generation']
    else:
        scope['legacy_alias'] = 'ignored-by-old-readers'

    with pytest.raises(contract.EphemeralStorageContractError):
        contract.parse_ephemeral_storage_scope(_yaml(scope))


@pytest.mark.parametrize('top_level', [
    {},
    {
        'file_mounts': None,
        'storage_mounts': None,
        'volumes': None,
        'volume_mounts': None,
        'workdir': None,
    },
    {
        'file_mounts': {},
        'storage_mounts': {},
        'volumes': {},
        'volume_mounts': [],
    },
])
def test_zero_deletion_target_projection_accepts_only_zero_graph(
        top_level: dict[str, object]) -> None:
    projection = contract.require_zero_deletion_target_projection(
        _yaml(**top_level))

    assert projection.to_dict() == {
        'resource_scope': _RESOURCE_SCOPE,
        'scope_id': contract.canonical_ephemeral_storage_scope_id(
            _RESOURCE_SCOPE, _STORAGE_GENERATION),
        'storage_generation': _STORAGE_GENERATION,
        'file_mount_target_count': 0,
        'storage_mount_target_count': 0,
        'volume_target_count': 0,
        'volume_mount_target_count': 0,
        'workdir_target_count': 0,
        'scoped_storage_mount_target_count': 0,
    }


@pytest.mark.parametrize('top_level', [
    {
        'file_mounts': False
    },
    {
        'file_mounts': {
            '/data': 's3://bucket'
        }
    },
    {
        'storage_mounts': []
    },
    {
        'storage_mounts': {
            '/data': {}
        }
    },
    {
        'volumes': []
    },
    {
        'volumes': {
            '/scratch': {
                'size': '10Gi'
            }
        }
    },
    {
        'volume_mounts': {}
    },
    {
        'volume_mounts': ['/scratch']
    },
    {
        'workdir': ''
    },
    {
        'workdir': '.'
    },
])
def test_zero_deletion_target_projection_rejects_nonzero_or_wrong_type(
        top_level: dict[str, object]) -> None:
    with pytest.raises(contract.EphemeralStorageContractError):
        contract.require_zero_deletion_target_projection(_yaml(**top_level))


def test_zero_deletion_target_projection_requires_scope() -> None:
    with pytest.raises(contract.EphemeralStorageContractError):
        contract.require_zero_deletion_target_projection('{}')


def test_zero_deletion_target_projection_rejects_scoped_mount() -> None:
    scope = copy.deepcopy(_scope())
    scope['storage_mounts'] = ['/data']

    with pytest.raises(contract.EphemeralStorageContractError):
        contract.require_zero_deletion_target_projection(_yaml(scope))
