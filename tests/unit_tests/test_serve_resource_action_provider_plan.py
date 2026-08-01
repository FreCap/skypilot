"""Pure prerequisite, object-plan, and renderer contract tests."""

import builtins
import copy
import dataclasses

import pytest

from sky.serve import resource_actions as actions

_CLUSTER_UUID = '11111111-1111-4111-8111-111111111111'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'


def _artifact(path: str = 'contracts/reviewed.json', marker: str = 'a') -> dict:
    return {'repo_path': path, 'byte_size': 17, 'sha256': marker * 64}


def _source() -> dict:
    return {
        'store': 'serve_version_specs',
        'service_name': 'service-a',
        'service_incarnation': '33333333-3333-4333-8333-333333333333',
        'service_version': 1,
        'yaml_content_sha256': 'b' * 64,
        'workspace': 'workspace-a',
    }


def _service_account() -> dict:
    return {
        'namespace': 'serve-canary',
        'name': 'serve-workload',
        'uid': 'uid-serve-workload',
        'resource_version': '123',
        'labels': [{
            'key': 'app',
            'value': 'serve'
        }],
        'annotations': [{
            'key': 'example.com/reviewed',
            'value': 'true'
        }],
        'automount_service_account_token': False,
        'image_pull_secrets': [],
        'legacy_secret_refs': [],
    }


def _prerequisite_spec(kind: str) -> dict:
    if kind == 'Namespace':
        return {
            'kind': kind,
            'labels': [{
                'key': 'app',
                'value': 'serve'
            }, {
                'key': 'team',
                'value': 'inference'
            }],
            'annotations': [{
                'key': 'example.com/a',
                'value': 'one'
            }, {
                'key': 'example.com/z',
                'value': 'two'
            }],
        }
    if kind == 'ServiceAccount':
        return {'kind': kind, 'projection': _service_account()}
    contracts = {
        'NetworkPolicy': 'serve_action_network_policy_v1',
        'ValidatingAdmissionPolicy': 'serve_action_validating_policy_v1',
        'ValidatingAdmissionPolicyBinding': 'serve_action_validating_binding_v1',
    }
    return {
        'kind': kind,
        'contract': contracts[kind],
        'manifest': _artifact(f'prerequisites/{kind}.json'),
    }


def _prerequisite(kind: str) -> dict:
    api_versions = {
        'Namespace': 'v1',
        'ServiceAccount': 'v1',
        'NetworkPolicy': 'networking.k8s.io/v1',
        'ValidatingAdmissionPolicy': 'admissionregistration.k8s.io/v1',
        'ValidatingAdmissionPolicyBinding': 'admissionregistration.k8s.io/v1',
    }
    namespaces = {
        'Namespace': None,
        'ServiceAccount': 'serve-canary',
        'NetworkPolicy': 'serve-canary',
        'ValidatingAdmissionPolicy': None,
        'ValidatingAdmissionPolicyBinding': None,
    }
    names = {
        'Namespace': 'serve-canary',
        'ServiceAccount': 'serve-workload',
        'NetworkPolicy': 'serve-network',
        'ValidatingAdmissionPolicy': 'serve-validation',
        'ValidatingAdmissionPolicyBinding': 'serve-validation-binding',
    }
    uids = {
        'ServiceAccount': 'uid-serve-workload',
    }
    resource_versions = {
        'ServiceAccount': '123',
    }
    spec = _prerequisite_spec(kind)
    return {
        'api_version': api_versions[kind],
        'kind': kind,
        'namespace': namespaces[kind],
        'name': names[kind],
        'uid': uids.get(kind, f'uid-{kind.lower()}'),
        'resource_version': resource_versions.get(kind, '456'),
        'deletion_timestamp': None,
        'spec': spec,
        'spec_sha256': actions.canonical_sha256(spec),
    }


def _identity_labels() -> list[dict]:
    return [{
        'key': 'skypilot-cluster-name',
        'value': 'svc-replica',
    }, {
        'key': 'skypilot.co/cluster-record-uuid',
        'value': _CLUSTER_UUID,
    }, {
        'key': 'skypilot.co/serve-replica-incarnation',
        'value': _REPLICA_UUID,
    }]


def _object_plan(role: str) -> dict:
    role_contracts = {
        'head_ssh_service': (0, 'Service', 'svc-replica-head-ssh'),
        'head_service': (1, 'Service', 'svc-replica-head'),
        'head_pod': (2, 'Pod', 'svc-replica-head'),
    }
    sequence, kind, name = role_contracts[role]
    labels = _identity_labels()
    body_labels = {label['key']: label['value'] for label in labels}
    body_labels['role-specific'] = role
    request_body = {
        'apiVersion': 'v1',
        'kind': kind,
        'metadata': {
            'namespace': 'serve-canary',
            'name': name,
            'labels': body_labels,
        },
        'spec': {
            'reviewedProfile': 'direct-pod-v1'
        },
    }
    requested_semantic = copy.deepcopy(request_body)
    requested_semantic['admissionDefaults'] = {'explicit': True}
    return {
        'sequence': sequence,
        'role': role,
        'api_version': 'v1',
        'kind': kind,
        'namespace': 'serve-canary',
        'name': name,
        'required_identity_labels': labels,
        'request_body': request_body,
        'request_body_sha256': actions.canonical_sha256(request_body),
        'requested_semantic': requested_semantic,
        'requested_semantic_sha256':
            actions.canonical_sha256(requested_semantic),
        'comparison_contract': 'kubernetes_admitted_object_v1',
        'normalization_profile':
            _artifact('contracts/admitted-object-normalization.json'),
    }


def _renderer() -> dict:
    return {
        'contract': 'serve_prebooted_direct_pod_v1',
        'outer_template': _artifact('renderer/outer.j2', '1'),
        'node_fragment': _artifact('renderer/node.yaml', '2'),
        'binding_schema': _artifact('renderer/bindings.json', '3'),
        'config_access_inventory': _artifact('renderer/config.json', '4'),
        'admitted_object_normalization': _artifact('renderer/normalize.py',
                                                   '5'),
        'source': _source(),
    }


def _rehash_request_body(raw: dict) -> None:
    raw['request_body_sha256'] = actions.canonical_sha256(raw['request_body'])


def test_prerequisite_kind_map_is_exact_and_immutable() -> None:
    actual = {
        kind.value: (entry.api_version, entry.scope) for kind, entry in
        actions.PROVIDER_KUBERNETES_PREREQUISITE_KIND_MAP_V1.items()
    }
    assert actual == {
        'Namespace': ('v1', 'cluster'),
        'ServiceAccount': ('v1', 'namespaced'),
        'NetworkPolicy': ('networking.k8s.io/v1', 'namespaced'),
        'ValidatingAdmissionPolicy':
            ('admissionregistration.k8s.io/v1', 'cluster'),
        'ValidatingAdmissionPolicyBinding':
            ('admissionregistration.k8s.io/v1', 'cluster'),
    }
    with pytest.raises(TypeError):
        actions.PROVIDER_KUBERNETES_PREREQUISITE_KIND_MAP_V1[  # type: ignore[index]
            actions.ProviderKubernetesPrerequisiteKindV1.NAMESPACE] = object()
    entry = actions.PROVIDER_KUBERNETES_PREREQUISITE_KIND_MAP_V1[
        actions.ProviderKubernetesPrerequisiteKindV1.NAMESPACE]
    with pytest.raises(dataclasses.FrozenInstanceError):
        entry.scope = 'namespaced'  # type: ignore[misc]


def test_object_role_map_is_exact_immutable_and_drives_topology() -> None:
    entries = actions.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1
    assert [(entry.plan_sequence, entry.role.value, entry.kind.value,
             entry.name_rule, entry.create_sequence, entry.delete_sequence)
            for entry in entries] == [
                (0, 'head_ssh_service', 'Service', 'workload_name_plus_-ssh', 0,
                 1),
                (1, 'head_service', 'Service', 'workload_name', 1, 0),
                (2, 'head_pod', 'Pod', 'workload_name', 2, 2),
            ]
    with pytest.raises(dataclasses.FrozenInstanceError):
        entries[0].plan_sequence = 1  # type: ignore[misc]
    assert actions.ProviderPodTopologyV1._EXPECTED_ROLES == tuple(  # pylint: disable=protected-access
        entry.role for entry in entries)
    assert actions.ProviderPodTopologyMutableObjectV1._ROLE_KINDS == {  # pylint: disable=protected-access
        entry.role: entry.kind for entry in entries
    }


@pytest.mark.parametrize(('kind', 'spec_type'), [
    ('Namespace', actions.ProviderKubernetesNamespacePrerequisiteSpecV1),
    ('ServiceAccount',
     actions.ProviderKubernetesServiceAccountPrerequisiteSpecV1),
    ('NetworkPolicy',
     actions.ProviderKubernetesNetworkPolicyPrerequisiteSpecV1),
    ('ValidatingAdmissionPolicy',
     actions.ProviderKubernetesValidatingAdmissionPolicyPrerequisiteSpecV1),
    ('ValidatingAdmissionPolicyBinding', actions.
     ProviderKubernetesValidatingAdmissionPolicyBindingPrerequisiteSpecV1),
])
def test_all_prerequisite_variants_roundtrip_and_recompute_hash(
        kind: str, spec_type: type) -> None:
    raw = _prerequisite(kind)
    parsed = actions.ProviderKubernetesPrerequisiteV1.from_value(raw)

    assert isinstance(parsed.spec, spec_type)
    assert parsed.canonical_value() == raw
    assert parsed.spec_sha256 == parsed.spec.sha256
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert actions.ProviderKubernetesPrerequisiteV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes


@pytest.mark.parametrize(('kind', 'contract'), [
    ('NetworkPolicy', 'serve_action_network_policy_v1'),
    ('ValidatingAdmissionPolicy', 'serve_action_validating_policy_v1'),
    ('ValidatingAdmissionPolicyBinding', 'serve_action_validating_binding_v1'),
])
def test_manifest_prerequisite_specs_reject_wrong_kind_or_contract(
        kind: str, contract: str) -> None:
    spec = _prerequisite_spec(kind)
    spec['contract'] = f'{contract}-other'
    raw = _prerequisite(kind)
    raw['spec'] = spec
    raw['spec_sha256'] = actions.canonical_sha256(spec)
    with pytest.raises(ValueError, match='contract'):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)

    spec = _prerequisite_spec(kind)
    spec['kind'] = 'Namespace'
    with pytest.raises(ValueError):
        ({
            'NetworkPolicy':
                actions.ProviderKubernetesNetworkPolicyPrerequisiteSpecV1,
            'ValidatingAdmissionPolicy':
                actions.
                ProviderKubernetesValidatingAdmissionPolicyPrerequisiteSpecV1,
            'ValidatingAdmissionPolicyBinding':
                actions.
                ProviderKubernetesValidatingAdmissionPolicyBindingPrerequisiteSpecV1,
        }[kind].from_value(spec))


def test_prerequisite_rejects_outer_and_inner_kind_substitution() -> None:
    raw = _prerequisite('NetworkPolicy')
    raw['spec'] = _prerequisite_spec('ServiceAccount')
    raw['spec_sha256'] = actions.canonical_sha256(raw['spec'])
    with pytest.raises(ValueError, match='outer and spec kinds'):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('api_version', 'v2'),
    ('namespace', 'must-be-null'),
    ('deletion_timestamp', '2026-08-01T00:00:00Z'),
    ('spec_sha256', 'f' * 64),
])
def test_namespace_prerequisite_rejects_outer_contract_mismatch(
        field: str, value: object) -> None:
    raw = _prerequisite('Namespace')
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)


@pytest.mark.parametrize('kind', ['ServiceAccount', 'NetworkPolicy'])
def test_namespaced_prerequisite_rejects_null_namespace(kind: str) -> None:
    raw = _prerequisite(kind)
    raw['namespace'] = None
    with pytest.raises(ValueError, match='namespace'):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)


@pytest.mark.parametrize('field',
                         ['namespace', 'name', 'uid', 'resource_version'])
def test_service_account_prerequisite_requires_outer_projection_identity(
        field: str) -> None:
    raw = _prerequisite('ServiceAccount')
    raw[field] = 'different'
    with pytest.raises(ValueError, match='outer identity'):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)


@pytest.mark.parametrize('field', ['labels', 'annotations'])
def test_namespace_spec_rejects_unsorted_or_duplicate_key_sets(
        field: str) -> None:
    raw = _prerequisite('Namespace')
    raw['spec'][field].reverse()
    raw['spec_sha256'] = actions.canonical_sha256(raw['spec'])
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)

    raw = _prerequisite('Namespace')
    raw['spec'][field].append(copy.deepcopy(raw['spec'][field][0]))
    raw['spec_sha256'] = actions.canonical_sha256(raw['spec'])
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)


def test_prerequisite_is_closed_and_requires_typed_direct_spec() -> None:
    raw = _prerequisite('Namespace')
    raw['unknown'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)

    raw = _prerequisite('Namespace')
    with pytest.raises(TypeError, match='invalid type'):
        actions.ProviderKubernetesPrerequisiteV1(**
                                                 raw)  # type: ignore[arg-type]


@pytest.mark.parametrize('role',
                         ['head_ssh_service', 'head_service', 'head_pod'])
def test_object_plan_roundtrip_exact_identity_and_extra_body_labels(
        role: str) -> None:
    raw = _object_plan(role)
    parsed = actions.ProviderKubernetesObjectPlanV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert parsed.request_body_sha256 == parsed.request_body.sha256
    assert parsed.requested_semantic_sha256 == parsed.requested_semantic.sha256
    assert parsed.request_body.canonical_value(
    )['metadata']['labels']['role-specific'] == role
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert actions.ProviderKubernetesObjectPlanV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes


@pytest.mark.parametrize(('field', 'value'), [
    ('sequence', True),
    ('sequence', 3),
    ('role', 'head_pod'),
    ('kind', 'Pod'),
])
def test_object_plan_rejects_sequence_role_kind_mismatch(
        field: str, value: object) -> None:
    raw = _object_plan('head_ssh_service')
    raw[field] = value
    with pytest.raises(ValueError):
        actions.ProviderKubernetesObjectPlanV1.from_value(raw)


@pytest.mark.parametrize(('role', 'name'), [
    ('head_ssh_service', 'svc-replica-head'),
    ('head_service', 'svc-replica-head-ssh'),
    ('head_pod', 'svc-replica'),
    ('head_pod', 'Upper-head'),
])
def test_object_plan_rejects_wrong_suffix_or_non_dns_name(role: str,
                                                          name: str) -> None:
    raw = _object_plan(role)
    raw['name'] = name
    raw['request_body']['metadata']['name'] = name
    _rehash_request_body(raw)
    with pytest.raises(ValueError):
        actions.ProviderKubernetesObjectPlanV1.from_value(raw)


@pytest.mark.parametrize('key', [
    'skypilot.co/cluster-record-uuid',
    'skypilot.co/serve-replica-incarnation',
])
def test_object_plan_rejects_noncanonical_identity_uuid(key: str) -> None:
    raw = _object_plan('head_pod')
    for label in raw['required_identity_labels']:
        if label['key'] == key:
            label['value'] = 'not-a-uuid'
    raw['request_body']['metadata']['labels'][key] = 'not-a-uuid'
    _rehash_request_body(raw)
    with pytest.raises(ValueError, match='UUID'):
        actions.ProviderKubernetesObjectPlanV1.from_value(raw)


def test_object_plan_requires_exact_sorted_identity_label_set_and_display(
) -> None:
    raw = _object_plan('head_service')
    raw['required_identity_labels'].reverse()
    with pytest.raises(ValueError, match='sorted'):
        actions.ProviderKubernetesObjectPlanV1.from_value(raw)

    raw = _object_plan('head_service')
    raw['required_identity_labels'].pop()
    with pytest.raises(ValueError, match='exact three'):
        actions.ProviderKubernetesObjectPlanV1.from_value(raw)

    raw = _object_plan('head_service')
    raw['required_identity_labels'].append({
        'key': 'zz-extra',
        'value': 'value'
    })
    with pytest.raises(ValueError, match='exact three'):
        actions.ProviderKubernetesObjectPlanV1.from_value(raw)

    raw = _object_plan('head_service')
    raw['required_identity_labels'][0]['value'] = 'different'
    raw['request_body']['metadata']['labels'][
        'skypilot-cluster-name'] = 'different'
    _rehash_request_body(raw)
    with pytest.raises(ValueError, match='display'):
        actions.ProviderKubernetesObjectPlanV1.from_value(raw)


@pytest.mark.parametrize(('path', 'value'), [
    (('apiVersion',), 'v2'),
    (('kind',), 'Service'),
    (('metadata',), None),
    (('metadata', 'namespace'), 'other'),
    (('metadata', 'name'), 'other-head'),
    (('metadata', 'labels'), []),
    (('metadata', 'labels', 'skypilot-cluster-name'), 'other'),
    (('metadata', 'labels', 'skypilot.co/cluster-record-uuid'), 'other'),
    (('metadata', 'labels', 'skypilot.co/serve-replica-incarnation'), 'other'),
])
def test_object_plan_rejects_request_body_identity_mismatch(
        path: tuple[str, ...], value: object) -> None:
    raw = _object_plan('head_pod')
    target = raw['request_body']
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    _rehash_request_body(raw)
    with pytest.raises(ValueError, match='request body'):
        actions.ProviderKubernetesObjectPlanV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('api_version', 'V1'),
    ('request_body_sha256', 'f' * 64),
    ('requested_semantic_sha256', 'f' * 64),
    ('comparison_contract', 'other'),
    ('normalization_profile', 'artifact-by-hash'),
])
def test_object_plan_rejects_literals_hashes_and_untyped_artifact(
        field: str, value: object) -> None:
    raw = _object_plan('head_pod')
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesObjectPlanV1.from_value(raw)


def test_object_plan_does_not_compare_semantic_to_normalizer_at_leaf() -> None:
    raw = _object_plan('head_pod')
    raw['requested_semantic'] = {'deliberately': 'unverified-at-leaf'}
    raw['requested_semantic_sha256'] = actions.canonical_sha256(
        raw['requested_semantic'])

    parsed = actions.ProviderKubernetesObjectPlanV1.from_value(raw)
    assert parsed.requested_semantic.canonical_value() == {
        'deliberately': 'unverified-at-leaf'
    }


def test_object_plan_leaf_does_not_claim_pod_renderer_semantics() -> None:
    raw = _object_plan('head_pod')
    raw['request_body']['spec']['nodeName'] = 'deliberately-unchecked-at-leaf'
    _rehash_request_body(raw)

    parsed = actions.ProviderKubernetesObjectPlanV1.from_value(raw)
    assert parsed.request_body.canonical_value()['spec']['nodeName'] == (
        'deliberately-unchecked-at-leaf')


@pytest.mark.parametrize('field', ['request_body', 'requested_semantic'])
def test_object_plan_bounds_json_preimages_before_recursive_outer_parser(
        field: str) -> None:
    cyclic: dict[str, object] = {}
    cyclic['self'] = cyclic

    depth_seventeen: object = {'leaf': 1}
    for index in range(16):
        depth_seventeen = {f'level{index}': depth_seventeen}

    aggregate_overflow = {'root': [list(range(256)) for _ in range(16)]}
    for value, match in ((cyclic, 'cycle'), (depth_seventeen, 'depth'),
                         (aggregate_overflow, 'aggregate')):
        raw = _object_plan('head_pod')
        raw[field] = value
        with pytest.raises(ValueError, match=match):
            actions.ProviderKubernetesObjectPlanV1.from_value(raw)


def test_object_plan_json_preimages_are_detached_and_directly_typed() -> None:
    raw = _object_plan('head_pod')
    parsed = actions.ProviderKubernetesObjectPlanV1.from_value(raw)
    committed = parsed.canonical_bytes
    raw['request_body']['metadata']['labels']['later'] = 'mutation'
    returned = parsed.request_body.canonical_value()
    returned['metadata']['labels']['also-later'] = 'mutation'

    assert parsed.canonical_bytes == committed
    assert 'later' not in parsed.request_body.canonical_value(
    )['metadata']['labels']
    with pytest.raises(TypeError, match='invalid type'):
        dataclasses.replace(parsed, request_body={})
    with pytest.raises(TypeError, match='invalid type'):
        dataclasses.replace(parsed, requested_semantic={})


def test_object_plan_is_closed() -> None:
    raw = _object_plan('head_pod')
    raw['unknown'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesObjectPlanV1.from_value(raw)


def test_renderer_roundtrip_and_accepts_equal_or_swapped_refs_at_leaf() -> None:
    raw = _renderer()
    parsed = actions.ProviderKubernetesRendererV1.from_value(raw)
    assert parsed.canonical_value() == raw
    assert parsed.sha256 == actions.canonical_sha256(raw)

    same = _artifact('renderer/shared.json', 'c')
    for field in ('outer_template', 'node_fragment', 'binding_schema',
                  'config_access_inventory', 'admitted_object_normalization'):
        raw[field] = copy.deepcopy(same)
    equal_refs = actions.ProviderKubernetesRendererV1.from_value(raw)
    assert equal_refs.outer_template == equal_refs.admitted_object_normalization

    swapped = _renderer()
    swapped['outer_template'], swapped['node_fragment'] = (
        swapped['node_fragment'], swapped['outer_template'])
    swapped_refs = actions.ProviderKubernetesRendererV1.from_value(swapped)
    assert swapped_refs.outer_template.repo_path == 'renderer/node.yaml'
    assert swapped_refs.node_fragment.repo_path == 'renderer/outer.j2'


@pytest.mark.parametrize(('field', 'value'), [
    ('contract', 'other'),
    ('outer_template', 'hash-only'),
    ('source', {
        'store': 'serve_version_specs'
    }),
])
def test_renderer_rejects_wrong_literal_or_untyped_child(
        field: str, value: object) -> None:
    raw = _renderer()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesRendererV1.from_value(raw)


def test_renderer_is_closed_and_direct_construction_is_typed() -> None:
    raw = _renderer()
    raw['unknown'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesRendererV1.from_value(raw)

    parsed = actions.ProviderKubernetesRendererV1.from_value(_renderer())
    with pytest.raises(TypeError, match='invalid type'):
        dataclasses.replace(parsed, outer_template={})


def test_pure_leaves_never_open_or_execute_artifacts(
        monkeypatch: pytest.MonkeyPatch) -> None:

    def _forbidden_open(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError('pure provider leaves must not open artifacts')

    monkeypatch.setattr(builtins, 'open', _forbidden_open)
    actions.ProviderKubernetesPrerequisiteV1.from_value(
        _prerequisite('NetworkPolicy'))
    actions.ProviderKubernetesObjectPlanV1.from_value(_object_plan('head_pod'))
    actions.ProviderKubernetesRendererV1.from_value(_renderer())
