"""Tests for policy-free provider execution-capsule leaf contracts."""

import copy
import dataclasses
import itertools
import typing

import pytest

from sky.serve import resource_actions as actions

JsonObject = dict[str, typing.Any]
Factory = typing.Callable[[], JsonObject]


class _FalseyTuple(tuple[typing.Any, ...]):

    def __bool__(self) -> bool:
        return False


class _EqualToAnything:

    def __eq__(self, other: object) -> bool:
        del other
        return True

    def __ne__(self, other: object) -> bool:
        del other
        return False


class _LengthSpoofingBytes(bytes):

    def __len__(self) -> int:
        return 1


class _BoundSpoofingString(str):

    def encode(self, encoding: str = 'utf-8', errors: str = 'strict') -> bytes:
        return _LengthSpoofingBytes(super().encode(encoding, errors))


class _UncontractedMutationEffect(
        actions.ProviderKubernetesObjectMutationEffectV1):

    def canonical_value(self) -> JsonObject:
        value = super().canonical_value()
        value['uncontracted'] = 'hidden'
        return value


def _request_identity() -> JsonObject:
    return {
        'cleaned_user': 'alice-example',
        'original_user': 'Alice.Example@example.com',
        'frozen_user_hash': 'a1b2c3d4',
    }


def _scheduling() -> JsonObject:
    return {
        'node_count': 1,
        'use_spot': False,
        'accelerator': None,
        'node_selector': [],
        'allowed_nodes': [],
        'avoid_accelerator_label_keys': [
            'cloud.google.com/gke-accelerator',
            'cloud.google.com/gke-tpu-accelerator',
        ],
        'runtime_class_name': None,
        'priority_class_name': None,
        'queue': None,
        'kueue': False,
        'dws': False,
        'autoscaler': None,
        'detected_network_type': 'default',
    }


def _storage() -> JsonObject:
    return {
        'persistent_volumes': [],
        'object_stores': [],
        'file_mounts': [],
        'workdir': None,
        'fuse': False,
        'docker_cache': False,
        'auto_mounts': False,
    }


def _metadata() -> JsonObject:
    return {
        'global_labels': [],
        'custom_pod_config': None,
        'custom_metadata': [],
        'reserved_labels_injected_last': True,
    }


def _security() -> JsonObject:
    return {
        'tls_material': None,
        'managed_secrets': [],
        'task_secrets': [],
        'service_account_bootstrap': False,
        'rbac_bootstrap': False,
    }


def _effect(sequence: int, role: str, kind: str) -> JsonObject:
    return {'sequence': sequence, 'role': role, 'kind': kind}


def _create_effects() -> list[JsonObject]:
    return [
        _effect(0, 'head_ssh_service', 'Service'),
        _effect(1, 'head_service', 'Service'),
        _effect(2, 'head_pod', 'Pod'),
    ]


def _delete_effects() -> list[JsonObject]:
    return [
        _effect(0, 'head_service', 'Service'),
        _effect(1, 'head_ssh_service', 'Service'),
        _effect(2, 'head_pod', 'Pod'),
    ]


def _mutation_effect() -> JsonObject:
    return _create_effects()[0]


def _launch_mutation() -> JsonObject:
    return {
        'role_map_contract': 'ProviderKubernetesObjectRoleMapV1',
        'create_effects': _create_effects(),
        'delete_effects': _delete_effects(),
        'job_effect': 'one_action_keyed_skylet_submit',
        'allowed_patches': [],
        'allowed_updates': [],
        'allowed_collection_deletes': [],
        'delete_requires_identity_labels_and_uid_precondition': True,
        'create_409': 'exact_admitted_readback_or_conflict',
        'create_422': 'terminal_no_rewrite',
    }


def _down_mutation() -> JsonObject:
    return {
        'role_map_contract': 'ProviderKubernetesObjectRoleMapV1',
        'delete_effects': _delete_effects(),
        'delete_requires_identity_labels_and_uid_precondition': True,
        'cluster_record_removal': 'same_uuid_exact_handle_after_absence_v1',
        'allowed_creates': [],
        'allowed_patches': [],
        'allowed_updates': [],
        'allowed_collection_deletes': [],
    }


_CONTRACT_CASES: tuple[tuple[Factory, type[typing.Any], int, str], ...] = (
    (_request_identity, actions.ProviderKubernetesRequestIdentityV1, 106,
     'b8d3e1868f60cd269865324d1a0566ff01db20f6020629be9f551a77be35d507'),
    (_scheduling, actions.ProviderKubernetesSchedulingContractV1, 341,
     '7fbd25d5ce2166cca4a1e1106428c972b4825a67d10ed9641968f8c9edc38fd1'),
    (_storage, actions.ProviderKubernetesStorageContractV1, 130,
     '3244b42817f3172b74bffd1e4a1b8f93afa3efcc4187ece1ab8040803821d779'),
    (_metadata, actions.ProviderKubernetesMetadataContractV1, 103,
     '5dc6c0b36c4ea7bf4d7b76703acedd7a25d145078a1b37ff4a20c51a5be10d21'),
    (_security, actions.ProviderKubernetesSecurityContractV1, 117,
     'b176319743bbcc52e74001d7af971a8d193f7b82e28a7866edcd366b0635d331'),
    (_mutation_effect, actions.ProviderKubernetesObjectMutationEffectV1, 57,
     '3c57b5cc56b166da718858171cc12ef0ad5081267e5932576f4c3be92ab5d28b'),
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1, 677,
     '163974e80e52257325c096ce9f87a55abf2a1c69d6c82de5858df35229d9d98d'),
    (_down_mutation, actions.ProviderKubernetesDownMutationContractV1, 456,
     '9d5200dd09292ee3dfdb9a1b8e7bcfa30e48836c5de94676f6c2e5efba7a38e3'),
)


@pytest.mark.parametrize(
    ('factory', 'contract', 'expected_size', 'expected_sha256'),
    _CONTRACT_CASES)
def test_capsule_leaf_canonical_golden(factory: Factory,
                                       contract: type[typing.Any],
                                       expected_size: int,
                                       expected_sha256: str) -> None:
    raw = factory()
    parsed = contract.from_value(raw)
    assert parsed.canonical_value() == raw
    assert len(parsed.canonical_bytes) == expected_size
    assert parsed.sha256 == expected_sha256
    assert contract.from_value(parsed.canonical_value()) == parsed
    with pytest.raises(dataclasses.FrozenInstanceError):
        parsed.canonical_bytes = b'changed'


@pytest.mark.parametrize(
    ('factory', 'contract', '_expected_size', '_expected_sha256'),
    _CONTRACT_CASES)
def test_capsule_leaf_rejects_unknown_or_missing_top_level_fields(
        factory: Factory, contract: type[typing.Any], _expected_size: int,
        _expected_sha256: str) -> None:
    raw = factory()
    raw['unknown'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        contract.from_value(raw)

    raw = factory()
    raw.pop(next(iter(raw)))
    with pytest.raises(ValueError, match='unknown or missing'):
        contract.from_value(raw)


@pytest.mark.parametrize(('factory', 'contract', 'field'), [
    (_request_identity, actions.ProviderKubernetesRequestIdentityV1,
     'cleaned_user'),
    (_scheduling, actions.ProviderKubernetesSchedulingContractV1,
     'node_selector'),
    (_scheduling, actions.ProviderKubernetesSchedulingContractV1,
     'avoid_accelerator_label_keys'),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'file_mounts'),
    (_metadata, actions.ProviderKubernetesMetadataContractV1,
     'custom_metadata'),
    (_security, actions.ProviderKubernetesSecurityContractV1, 'task_secrets'),
    (_mutation_effect, actions.ProviderKubernetesObjectMutationEffectV1,
     'role'),
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1,
     'create_effects'),
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1,
     'allowed_patches'),
    (_down_mutation, actions.ProviderKubernetesDownMutationContractV1,
     'delete_effects'),
    (_down_mutation, actions.ProviderKubernetesDownMutationContractV1,
     'allowed_creates'),
])
def test_capsule_leaf_rejects_cycles_before_recursive_serialization(
        factory: Factory, contract: type[typing.Any], field: str) -> None:
    raw = factory()
    cycle: list[typing.Any] = []
    cycle.append(cycle)
    raw[field] = cycle
    with pytest.raises((TypeError, ValueError)):
        contract.from_value(raw)


def test_capsule_leaf_rejects_deep_values_without_recursion_error() -> None:
    raw = _scheduling()
    deep: list[typing.Any] = []
    for _ in range(1100):
        deep = [deep]
    raw['node_selector'] = deep
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesSchedulingContractV1.from_value(raw)


@pytest.mark.parametrize('field',
                         ['cleaned_user', 'original_user', 'frozen_user_hash'])
@pytest.mark.parametrize('value', ['', 1, 'x' * 1025, 'a\x00b', 'e\u0301'])
def test_request_identity_rejects_noncanonical_text(field: str,
                                                    value: object) -> None:
    raw = _request_identity()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesRequestIdentityV1.from_value(raw)


def test_request_identity_does_not_invent_contextual_equalities() -> None:
    raw = _request_identity()
    raw['cleaned_user'] = 'different-clean-user'
    raw['frozen_user_hash'] = 'x' * 1024
    assert actions.ProviderKubernetesRequestIdentityV1.from_value(
        raw).canonical_value() == raw


def test_request_identity_rejects_bound_spoofing_string_subclass() -> None:
    raw = _request_identity()
    value = _BoundSpoofingString('x' * 2000)
    assert len(value.encode()) == 1
    raw['cleaned_user'] = value
    with pytest.raises(TypeError, match='must be text'):
        actions.ProviderKubernetesRequestIdentityV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('node_count', 0),
    ('node_count', 2),
    ('node_count', True),
    ('node_count', 1.0),
    ('node_count', '1'),
    ('use_spot', True),
    ('use_spot', 0),
    ('accelerator', {}),
    ('node_selector', ['example']),
    ('allowed_nodes', ['node-a']),
    ('runtime_class_name', 'runtime'),
    ('priority_class_name', 'priority'),
    ('queue', 'queue'),
    ('kueue', True),
    ('kueue', 0),
    ('dws', True),
    ('dws', 0),
    ('autoscaler', {}),
    ('detected_network_type', 'overlay'),
])
def test_scheduling_rejects_noncanonical_fixed_fields(field: str,
                                                      value: object) -> None:
    raw = _scheduling()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesSchedulingContractV1.from_value(raw)


@pytest.mark.parametrize('value', [
    [
        'cloud.google.com/gke-tpu-accelerator',
        'cloud.google.com/gke-accelerator'
    ],
    ['duplicate', 'duplicate'],
    [''],
    [1],
    ['a\x00b'],
    ['e\u0301'],
    ['x' * 254],
    [f'key-{index:03d}' for index in range(257)],
])
def test_scheduling_rejects_noncanonical_avoid_accelerator_keys(
        value: object) -> None:
    raw = _scheduling()
    raw['avoid_accelerator_label_keys'] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesSchedulingContractV1.from_value(raw)


def test_scheduling_accepts_canonical_text_and_list_bounds() -> None:
    raw = _scheduling()
    raw['avoid_accelerator_label_keys'] = [
        f'key-{index:03d}' for index in range(256)
    ]
    assert len(
        actions.ProviderKubernetesSchedulingContractV1.from_value(
            raw).avoid_accelerator_label_keys) == 256

    raw['avoid_accelerator_label_keys'] = ['x' * 253]
    parsed = actions.ProviderKubernetesSchedulingContractV1.from_value(raw)
    assert parsed.avoid_accelerator_label_keys == ('x' * 253,)


def test_scheduling_enforces_exact_canonical_object_size_bound() -> None:
    raw = _scheduling()
    label_keys = [f'{index:03d}' + 'x' * 250 for index in range(256)]
    label_keys[-2] = label_keys[-2][:-17]
    label_keys[-1] = label_keys[-1][:-250]
    raw['avoid_accelerator_label_keys'] = label_keys
    assert len(actions.canonical_json_bytes(raw)) == 65_536
    parsed = actions.ProviderKubernetesSchedulingContractV1.from_value(raw)
    assert len(parsed.canonical_bytes) == 65_536

    raw['avoid_accelerator_label_keys'][-1] += 'x'
    with pytest.raises(ValueError, match='exceeds 65536 bytes'):
        actions.ProviderKubernetesSchedulingContractV1.from_value(raw)
    with pytest.raises(ValueError, match='exceeds 65536 bytes'):
        dataclasses.replace(parsed,
                            avoid_accelerator_label_keys=(
                                *parsed.avoid_accelerator_label_keys[:-1],
                                parsed.avoid_accelerator_label_keys[-1] + 'x'))


def test_capsule_leaf_checks_raw_cardinality_before_copy_or_child_parse(
) -> None:
    raw = _scheduling()
    raw['node_selector'] = [object()] * 10_000
    with pytest.raises(ValueError, match='node_selector must be empty'):
        actions.ProviderKubernetesSchedulingContractV1.from_value(raw)

    raw = _scheduling()
    raw['avoid_accelerator_label_keys'] = [object()] * 257
    with pytest.raises(ValueError, match='at most 256'):
        actions.ProviderKubernetesSchedulingContractV1.from_value(raw)

    launch = _launch_mutation()
    launch['create_effects'] = [object()] * 10_000
    with pytest.raises(ValueError, match='exactly three'):
        actions.ProviderKubernetesLaunchMutationContractV1.from_value(launch)

    down = _down_mutation()
    down['delete_effects'] = [object()] * 10_000
    with pytest.raises(ValueError, match='exactly three'):
        actions.ProviderKubernetesDownMutationContractV1.from_value(down)


@pytest.mark.parametrize(('factory', 'contract', 'empty_fields'), [
    (_scheduling, actions.ProviderKubernetesSchedulingContractV1,
     ('node_selector', 'allowed_nodes', 'avoid_accelerator_label_keys')),
    (_storage, actions.ProviderKubernetesStorageContractV1,
     ('persistent_volumes', 'object_stores', 'file_mounts')),
    (_metadata, actions.ProviderKubernetesMetadataContractV1,
     ('global_labels', 'custom_metadata')),
    (_security, actions.ProviderKubernetesSecurityContractV1,
     ('managed_secrets', 'task_secrets')),
])
def test_capsule_leaf_requires_lists_on_wire_and_tuples_directly(
        factory: Factory, contract: type[typing.Any],
        empty_fields: tuple[str, ...]) -> None:
    for field in empty_fields:
        raw = factory()
        raw[field] = tuple(raw[field])
        with pytest.raises((TypeError, ValueError)):
            contract.from_value(raw)

        parsed = contract.from_value(factory())
        with pytest.raises(TypeError, match='must be a tuple'):
            dataclasses.replace(parsed, **{field: list(getattr(parsed, field))})


@pytest.mark.parametrize(('factory', 'contract', 'field'), [
    (_scheduling, actions.ProviderKubernetesSchedulingContractV1,
     'node_selector'),
    (_scheduling, actions.ProviderKubernetesSchedulingContractV1,
     'avoid_accelerator_label_keys'),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'file_mounts'),
    (_metadata, actions.ProviderKubernetesMetadataContractV1,
     'custom_metadata'),
    (_security, actions.ProviderKubernetesSecurityContractV1, 'task_secrets'),
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1,
     'allowed_patches'),
    (_down_mutation, actions.ProviderKubernetesDownMutationContractV1,
     'allowed_creates'),
])
def test_capsule_leaf_rejects_falsey_nonempty_tuple_subclasses(
        factory: Factory, contract: type[typing.Any], field: str) -> None:
    parsed = contract.from_value(factory())
    with pytest.raises(TypeError, match='must be a tuple'):
        dataclasses.replace(parsed, **{field: _FalseyTuple(('hidden',))})


@pytest.mark.parametrize(('factory', 'contract', 'field'), [
    (_scheduling, actions.ProviderKubernetesSchedulingContractV1,
     'detected_network_type'),
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1,
     'role_map_contract'),
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1,
     'job_effect'),
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1,
     'create_409'),
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1,
     'create_422'),
    (_down_mutation, actions.ProviderKubernetesDownMutationContractV1,
     'role_map_contract'),
    (_down_mutation, actions.ProviderKubernetesDownMutationContractV1,
     'cluster_record_removal'),
])
def test_capsule_leaf_rejects_equality_spoofing_fixed_literals(
        factory: Factory, contract: type[typing.Any], field: str) -> None:
    parsed = contract.from_value(factory())
    with pytest.raises(ValueError):
        dataclasses.replace(parsed, **{field: _EqualToAnything()})


@pytest.mark.parametrize(('factory', 'contract', 'field', 'value'), [
    (_storage, actions.ProviderKubernetesStorageContractV1,
     'persistent_volumes', ['pvc']),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'object_stores',
     ['s3']),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'file_mounts',
     ['/mnt']),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'workdir', '/work'),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'fuse', True),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'fuse', 0),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'docker_cache',
     True),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'docker_cache', 0),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'auto_mounts',
     True),
    (_storage, actions.ProviderKubernetesStorageContractV1, 'auto_mounts', 0),
    (_metadata, actions.ProviderKubernetesMetadataContractV1, 'global_labels',
     ['label']),
    (_metadata, actions.ProviderKubernetesMetadataContractV1,
     'custom_pod_config', {}),
    (_metadata, actions.ProviderKubernetesMetadataContractV1, 'custom_metadata',
     ['metadata']),
    (_metadata, actions.ProviderKubernetesMetadataContractV1,
     'reserved_labels_injected_last', False),
    (_metadata, actions.ProviderKubernetesMetadataContractV1,
     'reserved_labels_injected_last', 1),
    (_security, actions.ProviderKubernetesSecurityContractV1, 'tls_material',
     'secret'),
    (_security, actions.ProviderKubernetesSecurityContractV1, 'managed_secrets',
     ['secret']),
    (_security, actions.ProviderKubernetesSecurityContractV1, 'task_secrets',
     ['secret']),
    (_security, actions.ProviderKubernetesSecurityContractV1,
     'service_account_bootstrap', True),
    (_security, actions.ProviderKubernetesSecurityContractV1,
     'service_account_bootstrap', 0),
    (_security, actions.ProviderKubernetesSecurityContractV1, 'rbac_bootstrap',
     True),
    (_security, actions.ProviderKubernetesSecurityContractV1, 'rbac_bootstrap',
     0),
])
def test_absence_contracts_reject_every_nonliteral(factory: Factory,
                                                   contract: type[typing.Any],
                                                   field: str,
                                                   value: object) -> None:
    raw = factory()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        contract.from_value(raw)


@pytest.mark.parametrize(
    ('sequence', 'role', 'kind'),
    itertools.product(range(3),
                      ('head_ssh_service', 'head_service', 'head_pod'),
                      ('Service', 'Pod')))
def test_mutation_effect_leaf_accepts_every_scalar_union(
        sequence: int, role: str, kind: str) -> None:
    raw = _effect(sequence, role, kind)
    assert actions.ProviderKubernetesObjectMutationEffectV1.from_value(
        raw).canonical_value() == raw


@pytest.mark.parametrize(('field', 'value'), [
    ('sequence', -1),
    ('sequence', 3),
    ('sequence', True),
    ('sequence', 0.0),
    ('sequence', '0'),
    ('role', 'worker'),
    ('role', 1),
    ('kind', 'Deployment'),
    ('kind', 1),
])
def test_mutation_effect_rejects_values_outside_scalar_union(
        field: str, value: object) -> None:
    raw = _mutation_effect()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesObjectMutationEffectV1.from_value(raw)


@pytest.mark.parametrize('field', ['create_effects', 'delete_effects'])
@pytest.mark.parametrize('mutation', [
    'empty', 'missing', 'extra', 'duplicate', 'reordered', 'wrong_role',
    'wrong_kind', 'wrong_sequence'
])
def test_launch_mutation_rejects_every_effect_list_mismatch(
        field: str, mutation: str) -> None:
    raw = _launch_mutation()
    effects = raw[field]
    if mutation == 'empty':
        effects.clear()
    elif mutation == 'missing':
        effects.pop()
    elif mutation == 'extra':
        effects.append(copy.deepcopy(effects[-1]))
    elif mutation == 'duplicate':
        effects[1] = copy.deepcopy(effects[0])
    elif mutation == 'reordered':
        effects[0], effects[1] = effects[1], effects[0]
    elif mutation == 'wrong_role':
        effects[0]['role'] = 'head_pod'
    elif mutation == 'wrong_kind':
        effects[0]['kind'] = 'Pod'
    else:
        effects[0]['sequence'] = 2
    expected_message = ('exactly three'
                        if mutation in ('empty', 'missing',
                                        'extra') else 'exact protocol order')
    with pytest.raises(ValueError, match=expected_message):
        actions.ProviderKubernetesLaunchMutationContractV1.from_value(raw)


@pytest.mark.parametrize('mutation', [
    'empty', 'missing', 'extra', 'duplicate', 'reordered', 'wrong_role',
    'wrong_kind', 'wrong_sequence'
])
def test_down_mutation_rejects_every_effect_list_mismatch(
        mutation: str) -> None:
    raw = _down_mutation()
    effects = raw['delete_effects']
    if mutation == 'empty':
        effects.clear()
    elif mutation == 'missing':
        effects.pop()
    elif mutation == 'extra':
        effects.append(copy.deepcopy(effects[-1]))
    elif mutation == 'duplicate':
        effects[1] = copy.deepcopy(effects[0])
    elif mutation == 'reordered':
        effects[0], effects[1] = effects[1], effects[0]
    elif mutation == 'wrong_role':
        effects[0]['role'] = 'head_pod'
    elif mutation == 'wrong_kind':
        effects[0]['kind'] = 'Pod'
    else:
        effects[0]['sequence'] = 2
    expected_message = ('exactly three'
                        if mutation in ('empty', 'missing',
                                        'extra') else 'exact protocol order')
    with pytest.raises(ValueError, match=expected_message):
        actions.ProviderKubernetesDownMutationContractV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('role_map_contract', 'other'),
    ('job_effect', 'generic_submit'),
    ('allowed_patches', [{}]),
    ('allowed_updates', [{}]),
    ('allowed_collection_deletes', [{}]),
    ('delete_requires_identity_labels_and_uid_precondition', False),
    ('delete_requires_identity_labels_and_uid_precondition', 1),
    ('create_409', 'retry'),
    ('create_422', 'rewrite'),
])
def test_launch_mutation_rejects_every_nonliteral(field: str,
                                                  value: object) -> None:
    raw = _launch_mutation()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesLaunchMutationContractV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('role_map_contract', 'other'),
    ('delete_requires_identity_labels_and_uid_precondition', False),
    ('delete_requires_identity_labels_and_uid_precondition', 1),
    ('cluster_record_removal', 'remove_early'),
    ('allowed_creates', [{}]),
    ('allowed_patches', [{}]),
    ('allowed_updates', [{}]),
    ('allowed_collection_deletes', [{}]),
])
def test_down_mutation_rejects_every_nonliteral(field: str,
                                                value: object) -> None:
    raw = _down_mutation()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesDownMutationContractV1.from_value(raw)


@pytest.mark.parametrize(('factory', 'contract', 'fields'), [
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1,
     ('create_effects', 'delete_effects', 'allowed_patches', 'allowed_updates',
      'allowed_collection_deletes')),
    (_down_mutation, actions.ProviderKubernetesDownMutationContractV1,
     ('delete_effects', 'allowed_creates', 'allowed_patches', 'allowed_updates',
      'allowed_collection_deletes')),
])
def test_mutation_contract_requires_lists_on_wire_and_tuples_directly(
        factory: Factory, contract: type[typing.Any],
        fields: tuple[str, ...]) -> None:
    for field in fields:
        raw = factory()
        raw[field] = tuple(raw[field])
        with pytest.raises((TypeError, ValueError)):
            contract.from_value(raw)

        parsed = contract.from_value(factory())
        with pytest.raises(TypeError, match='must be a tuple'):
            dataclasses.replace(parsed, **{field: list(getattr(parsed, field))})


@pytest.mark.parametrize(('factory', 'contract', 'effect_field'), [
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1,
     'create_effects'),
    (_launch_mutation, actions.ProviderKubernetesLaunchMutationContractV1,
     'delete_effects'),
    (_down_mutation, actions.ProviderKubernetesDownMutationContractV1,
     'delete_effects'),
])
def test_mutation_contract_requires_typed_direct_effects(
        factory: Factory, contract: type[typing.Any],
        effect_field: str) -> None:
    parsed = contract.from_value(factory())
    with pytest.raises(ValueError, match='typed mutation effects'):
        dataclasses.replace(parsed,
                            **{effect_field: tuple(factory()[effect_field])})

    with pytest.raises(TypeError, match='must be a tuple'):
        dataclasses.replace(
            parsed,
            **{effect_field: _FalseyTuple(getattr(parsed, effect_field))})


def test_mutation_contract_rejects_effect_subclass_with_hidden_wire_fields(
) -> None:
    parsed = actions.ProviderKubernetesLaunchMutationContractV1.from_value(
        _launch_mutation())
    first = parsed.create_effects[0]
    uncontracted = _UncontractedMutationEffect(sequence=first.sequence,
                                               role=first.role,
                                               kind=first.kind)
    assert uncontracted.canonical_value()['uncontracted'] == 'hidden'
    with pytest.raises(ValueError, match='typed mutation effects'):
        dataclasses.replace(parsed,
                            create_effects=(uncontracted,
                                            *parsed.create_effects[1:]))


def test_mutation_contracts_reject_cross_kind_keys() -> None:
    launch = _launch_mutation()
    launch['cluster_record_removal'] = 'same_uuid_exact_handle_after_absence_v1'
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesLaunchMutationContractV1.from_value(launch)

    down = _down_mutation()
    down['job_effect'] = 'one_action_keyed_skylet_submit'
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesDownMutationContractV1.from_value(down)
