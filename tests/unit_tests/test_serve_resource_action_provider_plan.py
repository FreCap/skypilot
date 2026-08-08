"""Pure prerequisite, object-plan, and renderer contract tests."""

# pylint: disable=protected-access

import builtins
import copy
import dataclasses
import operator

import pytest

from sky.serve import resource_actions as actions

_CLUSTER_UUID = '11111111-1111-4111-8111-111111111111'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'
_PREREQUISITE_ROLE_KINDS = (
    ('authority_release_namespace', 'Namespace'),
    ('target_namespace', 'Namespace'),
    ('kube_system_namespace', 'Namespace'),
    ('serve_lb_slot_0_namespace', 'Namespace'),
    ('serve_lb_slot_1_namespace', 'Namespace'),
    ('caller_service_account', 'ServiceAccount'),
    ('workload_service_account', 'ServiceAccount'),
    ('serve_lb_slot_0_service_account', 'ServiceAccount'),
    ('serve_lb_slot_1_service_account', 'ServiceAccount'),
    ('endpoint_network_policy', 'NetworkPolicy'),
    ('validating_admission_policy', 'ValidatingAdmissionPolicy'),
    ('validating_admission_policy_binding', 'ValidatingAdmissionPolicyBinding'),
)
_DEFAULT_PREREQUISITE_ROLES = {
    'Namespace': 'target_namespace',
    'ServiceAccount': 'workload_service_account',
    'NetworkPolicy': 'endpoint_network_policy',
    'ValidatingAdmissionPolicy': 'validating_admission_policy',
    'ValidatingAdmissionPolicyBinding': 'validating_admission_policy_binding',
}


class _UncontractedPrerequisite(actions.ProviderKubernetesPrerequisiteV1):

    def canonical_value(self) -> dict:
        value = super().canonical_value()
        value['uncontracted'] = 'hidden'
        return value


class _TupleSubclass(tuple):
    pass


class _ListSubclass(list):
    pass


class _DictSubclass(dict):
    pass


class _EqualitySpoofingString(str):
    """Text whose Python equality lies about its canonical value."""

    def __eq__(self, other: object) -> bool:
        del other
        return True

    def __ne__(self, other: object) -> bool:
        del other
        return False

    __hash__ = str.__hash__


class _HashSpoofingString(str):

    def __hash__(self) -> int:
        return super().__hash__() ^ 1


class _LengthSpoofingBytes(bytes):

    def __len__(self) -> int:
        return 1


class _BoundSpoofingString(str):

    def encode(self, encoding: str = 'utf-8', errors: str = 'strict') -> bytes:
        return _LengthSpoofingBytes(super().encode(encoding, errors))


class _IntegerSubclass(int):
    pass


class _SpoofedCanonicalJsonObject(actions.CanonicalJsonObject):
    """Wrapper whose public value and hash conceal different committed data."""

    def canonical_value(self) -> dict:
        value = super().canonical_value()
        value['uncontracted'] = 'hidden'
        return value

    @property
    def sha256(self) -> str:
        return actions.canonical_sha256(super().canonical_value())


class _UncontractedLabel(actions.ProviderLabelV1):

    def canonical_value(self) -> dict:
        value = super().canonical_value()
        value['uncontracted'] = 'hidden'
        return value


class _UncontractedAnnotation(actions.ProviderAnnotationV1):

    def canonical_value(self) -> dict:
        value = super().canonical_value()
        value['uncontracted'] = 'hidden'
        return value


class _UncontractedNamespaceSpec(
        actions.ProviderKubernetesNamespacePrerequisiteSpecV1):

    def canonical_value(self) -> dict:
        value = super().canonical_value()
        value['uncontracted'] = 'hidden'
        return value


class _UncontractedServiceAccountProjection(
        actions.ProviderKubernetesServiceAccountProjectionV1):

    def canonical_value(self) -> dict:
        value = super().canonical_value()
        value['uncontracted'] = 'hidden'
        return value


class _UncontractedArtifact(actions.ProviderRepoArtifactRefV1):

    def canonical_value(self) -> dict:
        value = super().canonical_value()
        value['uncontracted'] = 'hidden'
        return value


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


def _prerequisite(kind: str, *, role: str | None = None) -> dict:
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
        'role': role or _DEFAULT_PREREQUISITE_ROLES[kind],
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


def _set_namespace_identity(raw: dict, *, name: str, uid: str) -> None:
    raw['name'] = name
    raw['uid'] = uid


def _set_service_account_identity(raw: dict, *, namespace: str, name: str,
                                  uid: str) -> None:
    raw['namespace'] = namespace
    raw['name'] = name
    raw['uid'] = uid
    projection = raw['spec']['projection']
    projection['namespace'] = namespace
    projection['name'] = name
    projection['uid'] = uid
    raw['spec_sha256'] = actions.canonical_sha256(raw['spec'])


def _prerequisite_inventory() -> list[dict]:
    authority_release = _prerequisite('Namespace',
                                      role='authority_release_namespace')
    _set_namespace_identity(authority_release,
                            name='skypilot-ha',
                            uid='uid-skypilot-ha')
    target = _prerequisite('Namespace', role='target_namespace')
    _set_namespace_identity(target, name='serve-canary', uid='uid-serve-canary')
    kube_system = _prerequisite('Namespace', role='kube_system_namespace')
    _set_namespace_identity(kube_system,
                            name='kube-system',
                            uid='uid-kube-system')
    lb_slot_zero_namespace = copy.deepcopy(authority_release)
    lb_slot_zero_namespace['role'] = 'serve_lb_slot_0_namespace'
    lb_slot_one_namespace = copy.deepcopy(authority_release)
    lb_slot_one_namespace['role'] = 'serve_lb_slot_1_namespace'

    caller = _prerequisite('ServiceAccount', role='caller_service_account')
    _set_service_account_identity(caller,
                                  namespace='skypilot-ha',
                                  name='authority-worker',
                                  uid='uid-authority-worker')
    workload = _prerequisite('ServiceAccount', role='workload_service_account')
    _set_service_account_identity(workload,
                                  namespace='serve-canary',
                                  name='serve-workload',
                                  uid='uid-serve-workload')
    lb_slot_zero = _prerequisite('ServiceAccount',
                                 role='serve_lb_slot_0_service_account')
    _set_service_account_identity(lb_slot_zero,
                                  namespace='skypilot-ha',
                                  name='serve-lb-slot-0',
                                  uid='uid-serve-lb-slot-0')
    lb_slot_one = _prerequisite('ServiceAccount',
                                role='serve_lb_slot_1_service_account')
    _set_service_account_identity(lb_slot_one,
                                  namespace='skypilot-ha',
                                  name='serve-lb-slot-1',
                                  uid='uid-serve-lb-slot-1')
    return [
        authority_release,
        target,
        kube_system,
        lb_slot_zero_namespace,
        lb_slot_one_namespace,
        caller,
        workload,
        lb_slot_zero,
        lb_slot_one,
        _prerequisite('NetworkPolicy', role='endpoint_network_policy'),
        _prerequisite('ValidatingAdmissionPolicy',
                      role='validating_admission_policy'),
        _prerequisite('ValidatingAdmissionPolicyBinding',
                      role='validating_admission_policy_binding'),
    ]


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


def _renderer_request_body(
    role: str,
    *,
    namespace: str = 'serve-canary',
    provider_cluster_name: str = 'svc-replica',
    cleaned_user: str = 'effectiveexamplecom',
    original_user: str = 'effective@example.com',
    cluster_uuid: str = _CLUSTER_UUID,
    replica_uuid: str = _REPLICA_UUID,
    workload_service_account: str = 'serve-workload',
    workload_image: str = 'registry.example/runtime:approved@sha256:' +
    '1' * 64,
    image_pull_policy: str = 'Always',
    replica_id_text: str = '7',
    pod_cpu_request: str = '0.5',
    pod_cpu_limit: str = '0.5',
    pod_memory_request: str = '1.23G',
    pod_memory_limit: str = '1.23G',
) -> dict:
    workload_name = f'{provider_cluster_name}-head'
    name = (f'{workload_name}-ssh'
            if role == 'head_ssh_service' else workload_name)
    labels = {
        'skypilot-cluster-name': provider_cluster_name,
        'skypilot-user': cleaned_user,
        'skypilot.co/cluster-record-uuid': cluster_uuid,
        'skypilot.co/serve-replica-incarnation': replica_uuid,
    }
    if role == 'head_pod':
        labels['component'] = workload_name
    else:
        labels['service-role'] = role
    metadata = {
        'labels': labels,
        'name': name,
        'namespace': namespace,
    }
    selector = {
        'component': workload_name,
        'skypilot-cluster-name': provider_cluster_name,
        'skypilot.co/cluster-record-uuid': cluster_uuid,
        'skypilot.co/serve-replica-incarnation': replica_uuid,
    }
    if role == 'head_ssh_service':
        kind = 'Service'
        spec = {
            'internalTrafficPolicy': 'Cluster',
            'ports': [{
                'port': 22,
                'protocol': 'TCP',
                'targetPort': 22,
            }],
            'selector': selector,
            'sessionAffinity': 'None',
            'type': 'ClusterIP',
        }
    elif role == 'head_service':
        kind = 'Service'
        spec = {
            'clusterIP': 'None',
            'internalTrafficPolicy': 'Cluster',
            'selector': selector,
            'sessionAffinity': 'None',
            'type': 'ClusterIP',
        }
    else:
        kind = 'Pod'
        metadata['annotations'] = {'skypilot-user': original_user}
        spec = {
            'automountServiceAccountToken': False,
            'containers': [{
                'env': [{
                    'name': 'SKYPILOT_SERVE_REPLICA_ID',
                    'value': replica_id_text,
                }],
                'image': workload_image,
                'imagePullPolicy': image_pull_policy,
                'name': 'ray-node',
                'ports': [{
                    'containerPort': port,
                    'protocol': 'TCP',
                } for port in (10001, 10002, 10003, 10004, 46590)],
                'resources': {
                    'limits': {
                        'cpu': pod_cpu_limit,
                        'memory': pod_memory_limit,
                    },
                    'requests': {
                        'cpu': pod_cpu_request,
                        'memory': pod_memory_request,
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
            'serviceAccount': workload_service_account,
            'serviceAccountName': workload_service_account,
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
        }
    return {
        'apiVersion': 'v1',
        'kind': kind,
        'metadata': metadata,
        'spec': spec,
    }


def _renderer_requested_semantic(role: str, request_body: dict) -> dict:
    semantic = copy.deepcopy(request_body)
    if role == 'head_service':
        del semantic['spec']['clusterIP']
    return semantic


def _renderer_artifact_ref(role: str) -> dict:
    # These are immutable fixture commitments for persisted V1 capsules.  The
    # retired renderer artifacts are intentionally absent from new packages,
    # but historical capsule parsing must retain their exact preimages.
    artifacts = {
        'admitted_object_normalization':
            (3033,
             '3ab35d775ff1324587c1c10854d5de8572ce127a8541dc08d85349be06e8f850'
            ),
        'binding_schema':
            (4520,
             '2c64a3ed8ee6ac3108fbf13d509ef348c73937d60473b5f697b24ee077611aef'
            ),
        'config_access_inventory':
            (23710,
             '19901e8e0491a4e9f957f7ff2a1244fc1baff132c37015c9e8e726af2d538f13'
            ),
        'node_fragment':
            (1632,
             '2000b68c74ccb6710e43b03963cf31f40c35ec879743977a3e3ba6ff3baa43db'
            ),
        'outer_template':
            (972,
             '769039b9c25956833032fb670148797c3ba74cd5a12253faf1e99443a27444b8'
            ),
    }
    byte_size, sha256 = artifacts[role]
    return {
        'repo_path': ('sky/serve/resource_action_artifacts/'
                      f'kubernetes_renderer_v1/{role}.json'),
        'byte_size': byte_size,
        'sha256': sha256,
    }


def _object_plan(role: str) -> dict:
    role_contracts = {
        'head_ssh_service': (0, 'Service', 'svc-replica-head-ssh'),
        'head_service': (1, 'Service', 'svc-replica-head'),
        'head_pod': (2, 'Pod', 'svc-replica-head'),
    }
    sequence, kind, name = role_contracts[role]
    labels = _identity_labels()
    request_body = _renderer_request_body(role)
    requested_semantic = _renderer_requested_semantic(role, request_body)
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
            _renderer_artifact_ref('admitted_object_normalization'),
    }


def _renderer() -> dict:
    return {
        'contract': 'serve_prebooted_direct_pod_v1',
        'outer_template': _renderer_artifact_ref('outer_template'),
        'node_fragment': _renderer_artifact_ref('node_fragment'),
        'binding_schema': _renderer_artifact_ref('binding_schema'),
        'config_access_inventory':
            _renderer_artifact_ref('config_access_inventory'),
        'admitted_object_normalization':
            _renderer_artifact_ref('admitted_object_normalization'),
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


def test_prerequisite_role_map_is_exact_and_immutable() -> None:
    entries = actions.PROVIDER_KUBERNETES_PREREQUISITE_ROLE_MAP_V1
    assert [
        (entry.sequence, entry.role.value, entry.kind.value)
        for entry in entries
    ] == [(sequence, role, kind)
          for sequence, (role, kind) in enumerate(_PREREQUISITE_ROLE_KINDS)]
    assert tuple(
        role.value
        for role in actions.ProviderKubernetesPrerequisiteRoleV1) == tuple(
            role for role, _ in _PREREQUISITE_ROLE_KINDS)
    with pytest.raises(dataclasses.FrozenInstanceError):
        entries[0].sequence = 1  # type: ignore[misc]
    with pytest.raises(TypeError):
        operator.setitem(entries, 0, entries[1])


def test_prerequisite_inventory_roundtrips_exact_bare_role_map() -> None:
    raw = _prerequisite_inventory()
    parsed = actions._provider_kubernetes_prerequisite_inventory_from_value(
        raw, name='test prerequisite inventory')

    assert [item.canonical_value() for item in parsed] == raw
    assert tuple(item.role.value for item in parsed) == tuple(
        role for role, _ in _PREREQUISITE_ROLE_KINDS)
    assert actions._provider_kubernetes_prerequisite_inventory_tuple(
        parsed, name='test prerequisite inventory') is parsed


def test_prerequisite_inventory_bounds_raw_cardinality_before_child_parse(
) -> None:
    raw = _prerequisite_inventory()
    with pytest.raises(TypeError, match='must be a list'):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            tuple(raw), name='test prerequisite inventory')
    with pytest.raises(TypeError, match='must be a list'):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            _ListSubclass(raw), name='test prerequisite inventory')
    for value in ([], raw[:-1], [object()] * 10_000):
        with pytest.raises(ValueError, match='exactly 12'):
            actions._provider_kubernetes_prerequisite_inventory_from_value(
                value, name='test prerequisite inventory')

    cycle: list[object] = []
    cycle.append(cycle)
    with pytest.raises(ValueError, match='exactly 12'):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            cycle, name='test prerequisite inventory')


def test_prerequisite_inventory_requires_exact_direct_types() -> None:
    parsed = actions._provider_kubernetes_prerequisite_inventory_from_value(
        _prerequisite_inventory(), name='test prerequisite inventory')
    with pytest.raises(TypeError, match='must be a tuple'):
        actions._provider_kubernetes_prerequisite_inventory_tuple(
            list(parsed), name='test prerequisite inventory')
    with pytest.raises(TypeError, match='must be a tuple'):
        actions._provider_kubernetes_prerequisite_inventory_tuple(
            _TupleSubclass(parsed), name='test prerequisite inventory')

    first = parsed[0]
    uncontracted = _UncontractedPrerequisite(
        role=first.role,
        api_version=first.api_version,
        kind=first.kind,
        namespace=first.namespace,
        name=first.name,
        uid=first.uid,
        resource_version=first.resource_version,
        deletion_timestamp=first.deletion_timestamp,
        spec=first.spec,
        spec_sha256=first.spec_sha256)
    assert uncontracted.canonical_value()['uncontracted'] == 'hidden'
    with pytest.raises(ValueError, match='exact typed prerequisites'):
        actions._provider_kubernetes_prerequisite_inventory_tuple(
            (uncontracted, *parsed[1:]), name='test prerequisite inventory')


def test_prerequisite_wire_requires_exact_dicts_and_keys() -> None:
    raw = _prerequisite_inventory()
    raw[0] = _DictSubclass(raw[0])
    with pytest.raises(TypeError, match='must be an object'):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')

    raw = _prerequisite_inventory()
    first = raw[0]
    role = first.pop('role')
    first[_HashSpoofingString('role')] = role
    with pytest.raises(TypeError, match='keys must be text'):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')


@pytest.mark.parametrize('mutation', [
    'api_version',
    'manifest_contract',
    'spec_sha256',
    'service_account_name',
    'duplicate_uid_hash',
    'duplicate_key_hash',
])
def test_prerequisite_wire_rejects_scalar_equality_or_hash_spoofing(
        mutation: str) -> None:
    raw = _prerequisite_inventory()
    if mutation == 'api_version':
        raw[0]['api_version'] = _EqualitySpoofingString('wrong-api')
    elif mutation == 'manifest_contract':
        raw[9]['spec']['contract'] = _EqualitySpoofingString('wrong-contract')
    elif mutation == 'spec_sha256':
        raw[0]['spec_sha256'] = _EqualitySpoofingString('f' * 64)
    elif mutation == 'service_account_name':
        raw[5]['name'] = _EqualitySpoofingString('wrong-name')
    elif mutation == 'duplicate_uid_hash':
        raw[1]['uid'] = _HashSpoofingString(raw[0]['uid'])
    else:
        raw[1]['name'] = _HashSpoofingString(raw[0]['name'])
    with pytest.raises(TypeError):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')


def test_shallow_prerequisite_leaves_reject_bound_spoofing_scalars() -> None:
    oversized = _BoundSpoofingString('x' * 2_000)
    with pytest.raises(TypeError):
        actions.ProviderLabelV1.from_value({'key': oversized, 'value': 'value'})
    with pytest.raises(TypeError):
        actions.ProviderAnnotationV1.from_value({
            'key': 'example.com/key',
            'value': oversized
        })
    with pytest.raises(TypeError):
        actions.ProviderRepoArtifactRefV1.from_value({
            'repo_path': oversized,
            'byte_size': 1,
            'sha256': 'a' * 64,
        })
    with pytest.raises(TypeError):
        actions.ProviderRepoArtifactRefV1.from_value({
            'repo_path': 'artifact.json',
            'byte_size': _IntegerSubclass(2**100),
            'sha256': 'a' * 64,
        })


def test_prerequisite_tree_rejects_nested_typed_subclasses() -> None:
    parsed = actions._provider_kubernetes_prerequisite_inventory_from_value(
        _prerequisite_inventory(), name='test prerequisite inventory')
    namespace = parsed[0]
    assert type(namespace.spec) is (
        actions.ProviderKubernetesNamespacePrerequisiteSpecV1)
    namespace_spec = namespace.spec
    label = namespace_spec.labels[0]
    annotation = namespace_spec.annotations[0]
    with pytest.raises(ValueError, match='typed labels'):
        dataclasses.replace(namespace_spec,
                            labels=(_UncontractedLabel(label.key, label.value),
                                    *namespace_spec.labels[1:]))
    with pytest.raises(ValueError, match='typed annotations'):
        dataclasses.replace(namespace_spec,
                            annotations=(_UncontractedAnnotation(
                                annotation.key, annotation.value),
                                         *namespace_spec.annotations[1:]))
    with pytest.raises(TypeError, match='must be a tuple'):
        dataclasses.replace(namespace_spec,
                            labels=_TupleSubclass(namespace_spec.labels))

    hidden_spec = _UncontractedNamespaceSpec(
        kind=namespace_spec.kind,
        labels=namespace_spec.labels,
        annotations=namespace_spec.annotations)
    with pytest.raises(TypeError, match='spec has an invalid type'):
        dataclasses.replace(namespace,
                            spec=hidden_spec,
                            spec_sha256=hidden_spec.sha256)

    service_account = parsed[5]
    assert type(service_account.spec) is (
        actions.ProviderKubernetesServiceAccountPrerequisiteSpecV1)
    projection = service_account.spec.projection
    hidden_projection = _UncontractedServiceAccountProjection(
        namespace=projection.namespace,
        name=projection.name,
        uid=projection.uid,
        resource_version=projection.resource_version,
        labels=projection.labels,
        annotations=projection.annotations,
        automount_service_account_token=(
            projection.automount_service_account_token),
        image_pull_secrets=projection.image_pull_secrets,
        legacy_secret_refs=projection.legacy_secret_refs)
    with pytest.raises(TypeError, match='projection has an invalid type'):
        dataclasses.replace(service_account.spec, projection=hidden_projection)

    manifest_prerequisite = parsed[9]
    assert type(manifest_prerequisite.spec) is (
        actions.ProviderKubernetesNetworkPolicyPrerequisiteSpecV1)
    manifest = manifest_prerequisite.spec.manifest
    hidden_manifest = _UncontractedArtifact(manifest.repo_path,
                                            manifest.byte_size, manifest.sha256)
    with pytest.raises(TypeError, match='manifest has an invalid type'):
        dataclasses.replace(manifest_prerequisite.spec,
                            manifest=hidden_manifest)


@pytest.mark.parametrize('mutation',
                         ['missing', 'extra', 'swap', 'duplicate_role'])
def test_prerequisite_inventory_rejects_role_map_order_or_cardinality(
        mutation: str) -> None:
    raw = _prerequisite_inventory()
    if mutation == 'missing':
        raw.pop()
    elif mutation == 'extra':
        raw.append(copy.deepcopy(raw[-1]))
    elif mutation == 'swap':
        raw[1], raw[2] = raw[2], raw[1]
    else:
        raw[1] = copy.deepcopy(raw[0])
    expected = 'exactly 12' if mutation in ('missing', 'extra') else 'role-map'
    with pytest.raises(ValueError, match=expected):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')


@pytest.mark.parametrize('alias_index', [0, 3, 4])
@pytest.mark.parametrize('field', ['name', 'uid', 'resource_version', 'spec'])
def test_prerequisite_inventory_requires_byte_equal_namespace_aliases(
        alias_index: int, field: str) -> None:
    raw = _prerequisite_inventory()
    alias = raw[alias_index]
    if field == 'spec':
        alias['spec']['labels'].append({'key': 'zz-extra', 'value': 'value'})
        alias['spec_sha256'] = actions.canonical_sha256(alias['spec'])
    else:
        alias[field] = f'different-{field}'
    with pytest.raises(ValueError, match='Namespace aliases'):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')


def test_prerequisite_inventory_does_not_normalize_alias_role_order() -> None:
    raw = _prerequisite_inventory()
    raw[3], raw[4] = raw[4], raw[3]
    with pytest.raises(ValueError, match='role-map'):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')


@pytest.mark.parametrize('mutation', [
    'namespace_same_key',
    'namespace_same_uid',
    'service_account_same_key',
    'service_account_same_uid',
    'cross_kind_same_uid',
])
def test_prerequisite_inventory_rejects_every_nonalias_collision(
        mutation: str) -> None:
    raw = _prerequisite_inventory()
    authority_release, target = raw[0], raw[1]
    caller, workload = raw[5], raw[6]
    if mutation == 'namespace_same_key':
        target['name'] = authority_release['name']
    elif mutation == 'namespace_same_uid':
        target['uid'] = authority_release['uid']
    elif mutation == 'service_account_same_key':
        _set_service_account_identity(workload,
                                      namespace=caller['namespace'],
                                      name=caller['name'],
                                      uid=workload['uid'])
    elif mutation == 'service_account_same_uid':
        _set_service_account_identity(workload,
                                      namespace=workload['namespace'],
                                      name=workload['name'],
                                      uid=caller['uid'])
    else:
        raw[9]['uid'] = target['uid']
    with pytest.raises(ValueError, match='distinct live keys and UIDs'):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')


def test_prerequisite_inventory_allows_distinct_cross_kind_or_namespace_keys(
) -> None:
    raw = _prerequisite_inventory()
    caller, workload, network_policy = raw[5], raw[6], raw[9]
    _set_service_account_identity(workload,
                                  namespace=workload['namespace'],
                                  name=caller['name'],
                                  uid=workload['uid'])
    network_policy['name'] = workload['name']

    parsed = actions._provider_kubernetes_prerequisite_inventory_from_value(
        raw, name='test prerequisite inventory')

    assert parsed[5].name == parsed[6].name
    assert parsed[5].namespace != parsed[6].namespace
    assert parsed[6].name == parsed[9].name
    assert parsed[6].kind != parsed[9].kind


def test_prerequisite_inventory_leaves_caller_automount_for_capsule_binding(
) -> None:
    parsed = actions._provider_kubernetes_prerequisite_inventory_from_value(
        _prerequisite_inventory(), name='test prerequisite inventory')

    assert parsed[5].spec.projection.automount_service_account_token is False


def test_prerequisite_parser_rejects_nested_cycles_without_recursion() -> None:
    raw = _prerequisite_inventory()
    raw[0]['spec'] = raw[0]
    with pytest.raises((TypeError, ValueError)):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')

    raw = _prerequisite_inventory()
    label = {'key': 'app'}
    label['value'] = label
    raw[0]['spec']['labels'] = [label]
    with pytest.raises((TypeError, ValueError)):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')

    raw = _prerequisite_inventory()
    artifact = {
        'repo_path': 'prerequisites/network-policy.json',
        'byte_size': 1
    }
    artifact['sha256'] = artifact
    raw[9]['spec']['manifest'] = artifact
    with pytest.raises((TypeError, ValueError)):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')


@pytest.mark.parametrize(('role_index', 'field'), [
    (0, 'labels'),
    (0, 'annotations'),
    (5, 'labels'),
    (5, 'annotations'),
    (5, 'image_pull_secrets'),
    (5, 'legacy_secret_refs'),
])
def test_prerequisite_parser_bounds_nested_lists_before_child_parse(
        role_index: int, field: str) -> None:
    raw = _prerequisite_inventory()
    spec = raw[role_index]['spec']
    target = spec if role_index == 0 else spec['projection']
    target[field] = [object()] * 10_000

    with pytest.raises(ValueError, match='at most 256'):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')


def test_prerequisite_parser_rejects_nested_list_subclasses() -> None:
    raw = _prerequisite_inventory()
    raw[0]['spec']['labels'] = _ListSubclass(raw[0]['spec']['labels'])
    with pytest.raises(TypeError, match='must be a list'):
        actions._provider_kubernetes_prerequisite_inventory_from_value(
            raw, name='test prerequisite inventory')


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


@pytest.mark.parametrize(('role', 'kind'), _PREREQUISITE_ROLE_KINDS)
def test_all_prerequisite_roles_roundtrip_and_recompute_hash(
        role: str, kind: str) -> None:
    spec_types = {
        'Namespace': actions.ProviderKubernetesNamespacePrerequisiteSpecV1,
        'ServiceAccount':
            actions.ProviderKubernetesServiceAccountPrerequisiteSpecV1,
        'NetworkPolicy':
            actions.ProviderKubernetesNetworkPolicyPrerequisiteSpecV1,
        'ValidatingAdmissionPolicy':
            actions.
            ProviderKubernetesValidatingAdmissionPolicyPrerequisiteSpecV1,
        'ValidatingAdmissionPolicyBinding':
            actions.
            ProviderKubernetesValidatingAdmissionPolicyBindingPrerequisiteSpecV1,
    }
    raw = _prerequisite(kind, role=role)
    parsed = actions.ProviderKubernetesPrerequisiteV1.from_value(raw)

    assert isinstance(parsed.spec, spec_types[kind])
    assert parsed.role.value == role
    assert parsed.canonical_value() == raw
    assert parsed.spec_sha256 == parsed.spec.sha256
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert actions.ProviderKubernetesPrerequisiteV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes


@pytest.mark.parametrize(('role', 'expected_kind'), _PREREQUISITE_ROLE_KINDS)
def test_every_prerequisite_role_rejects_a_wrong_kind(
        role: str, expected_kind: str) -> None:
    wrong_kind = ('ServiceAccount'
                  if expected_kind != 'ServiceAccount' else 'Namespace')
    raw = _prerequisite(wrong_kind)
    raw['role'] = role
    with pytest.raises(ValueError, match='semantic role'):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)


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

    raw = _prerequisite('Namespace')
    raw['role'] = 'unknown'
    with pytest.raises(ValueError, match='role'):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)

    raw = _prerequisite('Namespace')
    del raw['role']
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesPrerequisiteV1.from_value(raw)


@pytest.mark.parametrize('role',
                         ['head_ssh_service', 'head_service', 'head_pod'])
def test_object_plan_roundtrip_exact_identity_and_role_labels(
        role: str) -> None:
    raw = _object_plan(role)
    parsed = actions.ProviderKubernetesObjectPlanV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert parsed.request_body_sha256 == parsed.request_body.sha256
    assert parsed.requested_semantic_sha256 == parsed.requested_semantic.sha256
    body = parsed.request_body.canonical_value()
    labels = body['metadata']['labels']
    assert len(labels) == 5
    if role == 'head_pod':
        assert labels['component'] == body['metadata']['name']
    else:
        assert labels['service-role'] == role
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


@pytest.mark.parametrize(('role', 'mutation'), [
    ('head_pod', 'extra'),
    ('head_service', 'missing'),
    ('head_ssh_service', 'mis_role'),
])
def test_object_plan_rejects_nonexact_role_specific_body_labels(
        role: str, mutation: str) -> None:
    raw = _object_plan(role)
    labels = raw['request_body']['metadata']['labels']
    if mutation == 'extra':
        labels['extra'] = 'not-reviewed'
    elif mutation == 'missing':
        del labels['skypilot-user']
    else:
        labels['service-role'] = 'head_service'
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


def test_object_plan_accepts_exact_canonical_json_object_embeddings() -> None:
    parsed = actions.ProviderKubernetesObjectPlanV1.from_value(
        _object_plan('head_pod'))

    assert type(parsed.request_body) is actions.CanonicalJsonObject
    assert type(parsed.requested_semantic) is actions.CanonicalJsonObject
    replaced = dataclasses.replace(parsed,
                                   request_body=parsed.request_body,
                                   requested_semantic=parsed.requested_semantic)
    assert replaced.canonical_bytes == parsed.canonical_bytes


@pytest.mark.parametrize(('field', 'message'), [
    ('request_body', 'object plan request_body has an invalid type.'),
    ('requested_semantic',
     'object plan requested_semantic has an invalid type.'),
])
def test_object_plan_rejects_canonical_json_object_subclass_embeddings(
        field: str, message: str) -> None:
    parsed = actions.ProviderKubernetesObjectPlanV1.from_value(
        _object_plan('head_pod'))
    spoofed = _SpoofedCanonicalJsonObject(
        getattr(parsed, field).canonical_value())

    with pytest.raises(TypeError) as error:
        dataclasses.replace(parsed, **{field: spoofed})
    assert str(error.value) == message


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
    assert swapped_refs.outer_template.repo_path == _renderer_artifact_ref(
        'node_fragment')['repo_path']
    assert swapped_refs.node_fragment.repo_path == _renderer_artifact_ref(
        'outer_template')['repo_path']


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
