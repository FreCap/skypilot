"""Tests for pure SkyServe provider artifact normalization contracts."""

from __future__ import annotations

import copy
import hashlib

import pytest

from sky.serve import resource_action_provider_artifacts as artifacts
from sky.serve import resource_actions as actions

# The tests intentionally pin the private byte fixture that defines this
# module's exact frozen-v1 contract.
# pylint: disable=protected-access,missing-kwoa

_CLUSTER_UUID = '00000000-0000-4000-8000-000000000001'
_REPLICA_UUID = '00000000-0000-4000-8000-000000000002'
_IMAGE = ('361913687221.dkr.ecr.us-east-1.amazonaws.com/'
          'skypilot-ha@sha256:'
          '8bc1295d5cb873861576aaf0806665e89b2d325194da8dd61fa5752f0593d174')
_SEMANTIC_HASHES = {
    actions.ProviderObjectRoleV1.HEAD_SSH_SERVICE: '01f85e19668f5ce16850181367f80ad4bb83d2ba2b3db1e314cbf023f583f2c3',
    actions.ProviderObjectRoleV1.HEAD_SERVICE: 'b9f6e3e86df0c26dfe4da1576fe58ba9fd07af0c75c06be920bc5ac65520dd15',
    actions.ProviderObjectRoleV1.HEAD_POD: 'eb037b6c53d4900a22532126b08a20eff9144f755a2bbb9e3c24da57d51ddb38',
}


def _artifact_ref(raw_bytes: bytes) -> actions.ProviderRepoArtifactRefV1:
    return actions.ProviderRepoArtifactRefV1(
        repo_path=('sky/serve/resource_action_artifacts/'
                   'kubernetes_renderer_v1/'
                   'admitted_object_normalization.json'),
        byte_size=len(raw_bytes),
        sha256=hashlib.sha256(raw_bytes).hexdigest())


def _normalization_raw_bytes() -> bytes:
    return artifacts._EXPECTED_NORMALIZATION_CONTRACT_BYTES + b'\n'


def _normalization_artifact(
) -> artifacts.ResolvedProviderKubernetesNormalizationArtifactV1:
    raw_bytes = _normalization_raw_bytes()
    return artifacts.ResolvedProviderKubernetesNormalizationArtifactV1.from_verified_bytes(
        _artifact_ref(raw_bytes), raw_bytes)


def _selector() -> dict[str, str]:
    return {
        'component': 'ra-schema-v1-head',
        'skypilot-cluster-name': 'ra-schema-v1',
        'skypilot.co/cluster-record-uuid': _CLUSTER_UUID,
        'skypilot.co/serve-replica-incarnation': _REPLICA_UUID,
    }


def _labels(role: actions.ProviderObjectRoleV1) -> dict[str, str]:
    labels = {
        'skypilot-cluster-name': 'ra-schema-v1',
        'skypilot-user': 'renderer-probe',
        'skypilot.co/cluster-record-uuid': _CLUSTER_UUID,
        'skypilot.co/serve-replica-incarnation': _REPLICA_UUID,
    }
    if role is actions.ProviderObjectRoleV1.HEAD_POD:
        labels['component'] = 'ra-schema-v1-head'
    else:
        labels['service-role'] = role.value
    return labels


def _request_body(role: actions.ProviderObjectRoleV1) -> dict:
    if role is actions.ProviderObjectRoleV1.HEAD_SSH_SERVICE:
        return {
            'apiVersion': 'v1',
            'kind': 'Service',
            'metadata': {
                'labels': _labels(role),
                'name': 'ra-schema-v1-head-ssh',
                'namespace': 'skypilot-ha-workloads',
            },
            'spec': {
                'internalTrafficPolicy': 'Cluster',
                'ports': [{
                    'port': 22,
                    'protocol': 'TCP',
                    'targetPort': 22,
                }],
                'selector': _selector(),
                'sessionAffinity': 'None',
                'type': 'ClusterIP',
            },
        }
    if role is actions.ProviderObjectRoleV1.HEAD_SERVICE:
        return {
            'apiVersion': 'v1',
            'kind': 'Service',
            'metadata': {
                'labels': _labels(role),
                'name': 'ra-schema-v1-head',
                'namespace': 'skypilot-ha-workloads',
            },
            'spec': {
                'clusterIP': 'None',
                'internalTrafficPolicy': 'Cluster',
                'selector': _selector(),
                'sessionAffinity': 'None',
                'type': 'ClusterIP',
            },
        }
    return {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'annotations': {
                'skypilot-user': 'renderer-probe',
            },
            'labels': _labels(role),
            'name': 'ra-schema-v1-head',
            'namespace': 'skypilot-ha-workloads',
        },
        'spec': {
            'automountServiceAccountToken': False,
            'containers': [{
                'env': [{
                    'name': 'SKYPILOT_SERVE_REPLICA_ID',
                    'value': '7',
                }],
                'image': _IMAGE,
                'imagePullPolicy': 'Always',
                'name': 'ray-node',
                'ports': [{
                    'containerPort': port,
                    'protocol': 'TCP',
                } for port in (10001, 10002, 10003, 10004, 46590)],
                'resources': {
                    'limits': {
                        'cpu': '1',
                        'memory': '1G',
                    },
                    'requests': {
                        'cpu': '1',
                        'memory': '1G',
                    },
                },
                'terminationMessagePath': '/dev/termination-log',
                'terminationMessagePolicy': 'File',
            }],
            'dnsPolicy': 'ClusterFirst',
            'enableServiceLinks': True,
            'preemptionPolicy': 'PreemptLowerPriority',
            'priority': 0,
            'restartPolicy': 'Always',
            'schedulerName': 'default-scheduler',
            'securityContext': {},
            'serviceAccount': 'skypilot-service-account',
            'serviceAccountName': 'skypilot-service-account',
            'terminationGracePeriodSeconds': 30,
            'tolerations': [{
                'effect': 'NoExecute',
                'key': 'node.kubernetes.io/not-ready',
                'operator': 'Exists',
                'tolerationSeconds': 300,
            }, {
                'effect': 'NoExecute',
                'key': 'node.kubernetes.io/unreachable',
                'operator': 'Exists',
                'tolerationSeconds': 300,
            }],
        },
    }


def _validated_body(
    role: actions.ProviderObjectRoleV1,
) -> actions.ValidatedKubernetesServeThreeObjectBodyV1:
    return actions.ValidatedKubernetesServeThreeObjectBodyV1(
        role=role,
        body=actions.CanonicalJsonObject.from_value(_request_body(role)))


def _admitted_object(role: actions.ProviderObjectRoleV1,
                     *,
                     pod_node_name: str | None = None) -> dict:
    admitted = copy.deepcopy(_request_body(role))
    admitted['metadata'].update({
        'creationTimestamp': '2026-08-02T14:50:08Z',
        'generation': 1,
        'uid': '4d77e0c7-63bb-402c-a358-146b6216dd00',
    })
    admitted['status'] = ({
        'phase': 'Pending',
        'qosClass': 'Guaranteed',
    } if role is actions.ProviderObjectRoleV1.HEAD_POD else {
        'loadBalancer': {}
    })
    if role is actions.ProviderObjectRoleV1.HEAD_SSH_SERVICE:
        admitted['spec'].update({
            'clusterIP': '172.20.0.0',
            'clusterIPs': ['172.20.0.0'],
            'ipFamilies': ['IPv4'],
            'ipFamilyPolicy': 'SingleStack',
        })
    elif role is actions.ProviderObjectRoleV1.HEAD_SERVICE:
        admitted['spec'].update({
            'clusterIPs': ['None'],
            'ipFamilies': ['IPv4'],
            'ipFamilyPolicy': 'SingleStack',
        })
    elif pod_node_name is not None:
        admitted['spec']['nodeName'] = pod_node_name
    return admitted


def test_raw_artifact_requires_exact_canonical_json_and_lf() -> None:
    raw = _normalization_raw_bytes()
    parsed = artifacts.RawCanonicalRendererArtifactBytesV1.from_verified_bytes(
        _artifact_ref(raw), raw)
    assert parsed.raw_bytes == raw
    assert parsed.canonical_value()['comparison_contract'] == (
        'kubernetes_admitted_object_v1')

    for malformed in (raw[:-1], raw + b'\n', b'{ "a":1}\n', b'{"a":1,"a":2}\n',
                      b'{"a":NaN}\n', b'{"a":1.0}\n'):
        with pytest.raises((TypeError, ValueError)):
            artifacts.RawCanonicalRendererArtifactBytesV1.from_verified_bytes(
                _artifact_ref(malformed), malformed)

    empty_core_group = b'{"api_group":""}\n'
    assert artifacts.RawCanonicalRendererArtifactBytesV1.from_verified_bytes(
        _artifact_ref(empty_core_group),
        empty_core_group).canonical_value() == {
            'api_group': ''
        }


def test_raw_artifact_rejects_reference_drift_and_untyped_inputs() -> None:
    raw = _normalization_raw_bytes()
    reference = _artifact_ref(raw)
    with pytest.raises(ValueError, match='byte size'):
        artifacts.RawCanonicalRendererArtifactBytesV1.from_verified_bytes(
            actions.ProviderRepoArtifactRefV1(reference.repo_path,
                                              reference.byte_size + 1,
                                              reference.sha256), raw)
    with pytest.raises(ValueError, match='SHA-256'):
        artifacts.RawCanonicalRendererArtifactBytesV1.from_verified_bytes(
            actions.ProviderRepoArtifactRefV1(reference.repo_path,
                                              reference.byte_size, 'f' * 64),
            raw)
    with pytest.raises(TypeError, match='reference'):
        artifacts.RawCanonicalRendererArtifactBytesV1(artifact_ref={},
                                                      raw_bytes=raw)
    with pytest.raises(TypeError, match='exact bytes'):
        artifacts.RawCanonicalRendererArtifactBytesV1(artifact_ref=reference,
                                                      raw_bytes=bytearray(raw))


def test_normalization_contract_is_exact_and_resolved_pair_is_typed() -> None:
    artifact = _normalization_artifact()
    assert artifact.contract.canonical_bytes == (
        artifacts._EXPECTED_NORMALIZATION_CONTRACT_BYTES)
    assert artifact.canonical_value() == {
        'artifact_ref': artifact.artifact_ref.canonical_value(),
        'contract': artifact.contract.canonical_value(),
    }

    drifted = artifact.contract.canonical_value()
    drifted['unknown'] = None
    with pytest.raises(ValueError, match='exact v1'):
        artifacts.KubernetesAdmittedObjectNormalizationV1.from_value(drifted)
    with pytest.raises(TypeError, match='contract'):
        artifacts.ResolvedProviderKubernetesNormalizationArtifactV1(
            artifact_ref=artifact.artifact_ref,
            contract={})  # type: ignore[arg-type]

    wrong_path = actions.ProviderRepoArtifactRefV1(
        repo_path=('sky/serve/resource_action_artifacts/'
                   'kubernetes_renderer_v1/outer_template.json'),
        byte_size=artifact.artifact_ref.byte_size,
        sha256=artifact.artifact_ref.sha256)
    with pytest.raises(ValueError, match='repository path'):
        artifacts.ResolvedProviderKubernetesNormalizationArtifactV1(
            artifact_ref=wrong_path, contract=artifact.contract)

    wrong_preimage = actions.ProviderRepoArtifactRefV1(
        repo_path=artifact.artifact_ref.repo_path,
        byte_size=artifact.artifact_ref.byte_size,
        sha256='f' * 64)
    with pytest.raises(ValueError, match='exact contract bytes'):
        artifacts.ResolvedProviderKubernetesNormalizationArtifactV1(
            artifact_ref=wrong_preimage, contract=artifact.contract)


@pytest.mark.parametrize('role', tuple(actions.ProviderObjectRoleV1))
def test_request_normalization_matches_evidence_semantics(
        role: actions.ProviderObjectRoleV1) -> None:
    normalized = artifacts.normalize_kubernetes_request_object_v1(
        role, _validated_body(role), _normalization_artifact())
    assert normalized.requested_semantic.sha256 == _SEMANTIC_HASHES[role]
    expected_intents = {
        actions.ProviderObjectRoleV1.HEAD_SSH_SERVICE: 'allocate_single_stack_cluster_ip',
        actions.ProviderObjectRoleV1.HEAD_SERVICE: 'headless_single_stack',
        actions.ProviderObjectRoleV1.HEAD_POD: 'schedule_one_node',
    }
    assert normalized.requested_allocation_intent == expected_intents[role]
    if role is actions.ProviderObjectRoleV1.HEAD_SERVICE:
        assert 'clusterIP' not in normalized.requested_semantic.canonical_value(
        )['spec']


def test_request_normalization_rejects_crossed_shape_and_untyped_artifact(
) -> None:
    artifact = _normalization_artifact()
    validated = _validated_body(actions.ProviderObjectRoleV1.HEAD_SERVICE)
    with pytest.raises(ValueError, match='role'):
        artifacts.normalize_kubernetes_request_object_v1(
            actions.ProviderObjectRoleV1.HEAD_POD, validated, artifact)
    with pytest.raises(TypeError, match='artifact'):
        artifacts.normalize_kubernetes_request_object_v1(
            actions.ProviderObjectRoleV1.HEAD_SERVICE, validated,
            artifact.artifact_ref)


@pytest.mark.parametrize('role', tuple(actions.ProviderObjectRoleV1))
def test_admitted_normalization_matches_request_semantics_and_allocations(
        role: actions.ProviderObjectRoleV1) -> None:
    admitted = _admitted_object(role)
    normalized = artifacts.normalize_kubernetes_admitted_object_v1(
        role, admitted, _normalization_artifact(), require_pod_node_name=False)
    assert normalized.admitted_semantic.sha256 == _SEMANTIC_HASHES[role]
    expected_count = (0 if role is actions.ProviderObjectRoleV1.HEAD_POD else 4)
    assert len(normalized.server_allocations) == expected_count


def test_admitted_pod_node_name_parameter_has_both_exact_branches() -> None:
    role = actions.ProviderObjectRoleV1.HEAD_POD
    artifact = _normalization_artifact()
    scheduled = _admitted_object(role, pod_node_name='ip-10-0-0-7.ec2.internal')
    for required in (False, True):
        normalized = artifacts.normalize_kubernetes_admitted_object_v1(
            role, scheduled, artifact, require_pod_node_name=required)
        assert normalized.admitted_semantic.sha256 == _SEMANTIC_HASHES[role]
        assert [
            allocation.json_pointer
            for allocation in normalized.server_allocations
        ] == ['/spec/nodeName']

    with pytest.raises(ValueError, match='required server allocation'):
        artifacts.normalize_kubernetes_admitted_object_v1(
            role, _admitted_object(role), artifact, require_pod_node_name=True)

    with pytest.raises(TypeError, match='require_pod_node_name'):
        normalizer = getattr(artifacts,
                             'normalize_kubernetes_admitted_object_v1')
        normalizer(role, _admitted_object(role), artifact)


@pytest.mark.parametrize('invalid', [0, 1, 'false', None, object()])
def test_admitted_parameter_requires_exact_builtin_bool_before_object(
        invalid: object) -> None:
    with pytest.raises(TypeError, match='built-in bool'):
        artifacts.normalize_kubernetes_admitted_object_v1(
            actions.ProviderObjectRoleV1.HEAD_POD, {},
            _normalization_artifact(),
            require_pod_node_name=invalid)  # type: ignore[arg-type]


def test_admitted_service_requires_atomic_quartet_and_consistency() -> None:
    role = actions.ProviderObjectRoleV1.HEAD_SSH_SERVICE
    artifact = _normalization_artifact()
    for mutation in (
            lambda body: body['spec'].pop('clusterIPs'),
            lambda body: body['spec'].__setitem__('clusterIPs', ['10.0.0.9']),
            lambda body: body['spec'].__setitem__('ipFamilies', ['IPv6'])):
        admitted = _admitted_object(role)
        mutation(admitted)
        with pytest.raises(ValueError):
            artifacts.normalize_kubernetes_admitted_object_v1(
                role, admitted, artifact, require_pod_node_name=False)


def test_admitted_deletion_gate_precedes_strip_and_unknown_fields_are_retained(
) -> None:
    role = actions.ProviderObjectRoleV1.HEAD_SERVICE
    artifact = _normalization_artifact()
    deleting = _admitted_object(role)
    deleting['metadata']['deletionTimestamp'] = '2026-08-02T15:00:00Z'
    with pytest.raises(ValueError, match='deletion timestamp'):
        artifacts.normalize_kubernetes_admitted_object_v1(
            role, deleting, artifact, require_pod_node_name=False)

    injected = _admitted_object(role)
    injected['spec']['unreviewed'] = ['retained', 'in-order']
    normalized = artifacts.normalize_kubernetes_admitted_object_v1(
        role, injected, artifact, require_pod_node_name=False)
    assert normalized.admitted_semantic.canonical_value(
    )['spec']['unreviewed'] == ['retained', 'in-order']
    assert normalized.admitted_semantic.sha256 != _SEMANTIC_HASHES[role]
