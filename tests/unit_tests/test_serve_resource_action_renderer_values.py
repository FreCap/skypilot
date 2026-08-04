"""Canonical value contracts at the pure Kubernetes renderer boundary."""

# pylint: disable=protected-access

import copy
import json
from pathlib import Path

import pytest
import test_serve_resource_action_launch_execution_config as fixtures

from sky.serve import resource_actions as actions

_EVIDENCE_DIR = (Path(__file__).resolve().parents[2] / 'docs' / 'designs' /
                 'evidence' / 'skyserve-resource-action-renderer-v1')
_BINDING_ROWS = (
    ('head_labels', 'object'),
    ('head_name', 'string'),
    ('head_pod_labels', 'object'),
    ('head_pod_name', 'string'),
    ('head_service_selector', 'object'),
    ('head_ssh_labels', 'object'),
    ('head_ssh_name', 'string'),
    ('image_pull_policy', 'string'),
    ('original_user', 'string'),
    ('pod_cpu_limit', 'string'),
    ('pod_cpu_request', 'string'),
    ('pod_memory_limit', 'string'),
    ('pod_memory_request', 'string'),
    ('replica_id_text', 'string'),
    ('target_namespace', 'string'),
    ('workload_image', 'string'),
    ('workload_service_account', 'string'),
)


def _seed_raw() -> dict:
    raw = fixtures._capsule_raw()
    del raw['objects']
    return raw


def _renderer_input_raw() -> dict:
    target = fixtures._target()
    return {
        'version': 1,
        'contract': 'validated_launch_spec_v1',
        'resource_identity': fixtures._resource_identity(),
        'sky_cluster_name': target['sky_cluster_name'],
        'sky_cluster_record_uuid': target['sky_cluster_record_uuid'],
        'name_basis': target['kubernetes']['name_basis'],
        'seed': _seed_raw(),
        'retained_source': fixtures._content_source(),
    }


def _evidence_body(filename: str) -> dict:
    return json.loads((_EVIDENCE_DIR / filename).read_bytes())


def test_capsule_seed_and_renderer_input_are_exact_round_trips() -> None:
    seed = actions.ProviderKubernetesExecutionCapsuleSeedV1.from_value(
        _seed_raw())
    assert set(seed.canonical_value()) == (
        actions.ProviderKubernetesExecutionCapsuleV1._KEYS - {'objects'})
    renderer_input = actions.ProviderKubernetesRendererInputV1.from_value(
        _renderer_input_raw())
    assert len(renderer_input.canonical_value()) == 8
    assert (actions.ProviderKubernetesRendererInputV1.from_value(
        renderer_input.canonical_value()).canonical_bytes ==
            renderer_input.canonical_bytes)
    assert fixtures._capsule().canonical_value()['objects']


@pytest.mark.parametrize('mutation', ('source', 'cluster_uuid', 'name_basis'))
def test_renderer_input_rejects_independent_identity_mismatch(
        mutation: str) -> None:
    raw = _renderer_input_raw()
    if mutation == 'source':
        raw['retained_source']['workspace'] = 'wrong-workspace'
    elif mutation == 'cluster_uuid':
        raw['sky_cluster_record_uuid'] = (
            '44444444-4444-4444-8444-444444444444')
    else:
        raw['name_basis']['display_name'] = 'another-name'
    with pytest.raises(ValueError):
        actions.ProviderKubernetesRendererInputV1.from_value(raw)


def test_renderer_input_and_seed_reject_unknown_or_cross_field_values() -> None:
    raw_input = _renderer_input_raw()
    raw_input['objects'] = []
    with pytest.raises(ValueError, match='unknown or missing fields'):
        actions.ProviderKubernetesRendererInputV1.from_value(raw_input)

    raw_seed = _seed_raw()
    raw_seed['config_projection_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='hash does not match'):
        actions.ProviderKubernetesExecutionCapsuleSeedV1.from_value(raw_seed)


def test_resolved_binding_set_requires_all_frozen_rows_in_order() -> None:
    raw_bindings = [{
        'sequence': sequence,
        'name': name,
        'json_type': json_type,
        'value': ({
            'bound': name
        } if json_type == 'object' else name),
    } for sequence, (name, json_type) in enumerate(_BINDING_ROWS)]
    raw = {
        'version': 1,
        'contract': 'skypilot.serve.prebooted-direct-pod.resolved-bindings.v1',
        'bindings': raw_bindings,
    }
    typed = actions.ResolvedProviderKubernetesBindingSetV1.from_value(raw)
    assert typed.canonical_value() == raw
    assert (actions.ResolvedProviderKubernetesBindingSetV1.from_value(
        typed.canonical_value()).canonical_bytes == typed.canonical_bytes)

    crossed = copy.deepcopy(raw)
    crossed['bindings'][0]['name'] = 'head_name'
    with pytest.raises(ValueError, match='frozen table row'):
        actions.ResolvedProviderKubernetesBindingSetV1.from_value(crossed)


@pytest.mark.parametrize(
    ('role', 'filename'),
    ((actions.ProviderObjectRoleV1.HEAD_SSH_SERVICE,
      'head_ssh_service.request.json'),
     (actions.ProviderObjectRoleV1.HEAD_SERVICE, 'head_service.request.json'),
     (actions.ProviderObjectRoleV1.HEAD_POD, 'head_pod.request.json')))
def test_validated_body_accepts_exact_retained_request_evidence(
        role: actions.ProviderObjectRoleV1, filename: str) -> None:
    raw = {'role': role.value, 'body': _evidence_body(filename)}
    typed = actions.ValidatedKubernetesServeThreeObjectBodyV1.from_value(raw)
    assert typed.canonical_value() == raw


def test_validated_body_rejects_admission_drift_and_wrong_direct_type() -> None:
    raw = {
        'role': actions.ProviderObjectRoleV1.HEAD_POD.value,
        'body': _evidence_body('head_pod.request.json'),
    }
    raw['body']['spec']['nodeName'] = 'worker-a'
    with pytest.raises(ValueError, match='unknown or missing fields'):
        actions.ValidatedKubernetesServeThreeObjectBodyV1.from_value(raw)
    with pytest.raises(TypeError, match='CanonicalJsonObject'):
        actions.ValidatedKubernetesServeThreeObjectBodyV1(
            role=actions.ProviderObjectRoleV1.HEAD_POD,
            body=_evidence_body(
                'head_pod.request.json'))  # type: ignore[arg-type]


def test_completed_capsule_accepts_only_the_new_body_and_semantic_contract(
) -> None:
    legacy = fixtures._capsule_raw()
    ssh_plan = legacy['objects'][0]
    ssh_plan['request_body']['spec'] = {
        'ports': [{
            'protocol': 'TCP',
            'port': 22,
            'targetPort': 22,
        }]
    }
    ssh_plan['request_body_sha256'] = actions.canonical_sha256(
        ssh_plan['request_body'])
    with pytest.raises(ValueError, match='unknown or missing fields'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(legacy)

    unnormalized = fixtures._capsule_raw()
    head_plan = unnormalized['objects'][1]
    head_plan['requested_semantic'] = copy.deepcopy(head_plan['request_body'])
    head_plan['requested_semantic_sha256'] = actions.canonical_sha256(
        head_plan['requested_semantic'])
    with pytest.raises(ValueError, match='exact request normalization'):
        actions.ProviderKubernetesExecutionCapsuleV1.from_value(unnormalized)


def test_request_and_admitted_normalization_are_closed_round_trips() -> None:
    semantic = _evidence_body('head_ssh_service.request.json')
    request = actions.ProviderKubernetesRequestNormalizationV1.from_value({
        'requested_semantic': semantic,
        'requested_allocation_intent': 'allocate_single_stack_cluster_ip',
    })
    assert (actions.ProviderKubernetesRequestNormalizationV1.from_value(
        request.canonical_value()).canonical_bytes == request.canonical_bytes)

    admitted = actions.ProviderKubernetesAdmittedNormalizationV1.from_value({
        'admitted_semantic': semantic,
        'server_allocations': [{
            'json_pointer': '/spec/clusterIP',
            'allocator': 'api_server',
            'value': '10.0.0.7',
        }, {
            'json_pointer': '/spec/clusterIPs',
            'allocator': 'api_server',
            'value': ['10.0.0.7'],
        }, {
            'json_pointer': '/spec/ipFamilies',
            'allocator': 'api_server',
            'value': ['IPv4'],
        }, {
            'json_pointer': '/spec/ipFamilyPolicy',
            'allocator': 'api_server',
            'value': 'SingleStack',
        }],
    })
    assert len(admitted.server_allocations) == 4
    partial = admitted.canonical_value()
    partial['server_allocations'].pop()
    with pytest.raises(ValueError, match='invalid atomic shape'):
        actions.ProviderKubernetesAdmittedNormalizationV1.from_value(partial)
