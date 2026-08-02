"""Pure completed-launch and down-addressing provider DTO tests."""

import copy
import dataclasses

import pytest

from sky.serve import resource_actions as actions

_CLUSTER_UUID = '11111111-1111-4111-8111-111111111111'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'
_OBSERVED_AT = '2026-08-01T05:06:07.123456Z'
_EXPECTED_SSH_RESOLVED_BYTES = (
    b'{"kind":"Service","name":"svc-replica-head-ssh","namespace":'
    b'"serve-canary","observed_semantic_sha256":'
    b'"ede587f0b94eba52aa8eb05c2b4d5af3152507f4cbd3b9727857b36fdeb53df9",'
    b'"role":"head_ssh_service","server_allocations":[{"allocator":'
    b'"api_server","json_pointer":"/spec/clusterIP","value":"10.0.0.7"},'
    b'{"allocator":"api_server","json_pointer":"/spec/clusterIPs","value":'
    b'["10.0.0.7"]},{"allocator":"api_server","json_pointer":'
    b'"/spec/ipFamilies","value":["IPv4"]},{"allocator":"api_server",'
    b'"json_pointer":"/spec/ipFamilyPolicy","value":"SingleStack"}],'
    b'"uid":"uid-head-ssh-service"}')


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
    """Text whose hash differs from its canonical string hash."""

    def __hash__(self) -> int:
        return super().__hash__() ^ 1


class _LengthSpoofingString(str):
    """Text whose direct length understates its canonical content."""

    def __len__(self) -> int:
        return 1


class _LengthSpoofingBytes(bytes):
    """Encoded bytes whose length understates their canonical content."""

    def __len__(self) -> int:
        return 1


class _BoundSpoofingString(str):
    """Text whose encoded bytes try to evade byte-size bounds."""

    def encode(self, encoding: str = 'utf-8', errors: str = 'strict') -> bytes:
        return _LengthSpoofingBytes(super().encode(encoding, errors))


_SPOOFING_STRING_TYPES = (_EqualitySpoofingString, _HashSpoofingString,
                          _LengthSpoofingString, _BoundSpoofingString)


def _artifact() -> dict:
    return {
        'repo_path': 'contracts/admitted-object-normalization.py',
        'byte_size': 17,
        'sha256': 'a' * 64,
    }


def _scope() -> dict:
    return {
        'version': 1,
        'context_name': 'kubernetes',
        'context_identity': ['skypilot-in-cluster-identity-kubernetes'],
        'in_cluster': True,
        'namespace': 'serve-canary',
        'transport': {
            'version': 1,
            'server_origin': {
                'scheme': 'https',
                'host': '10.0.0.1',
                'port': 443,
                'path': '/',
            },
            'tls_server_name': 'kubernetes.default.svc',
            'ca_cert_der_base64': ['MAMCAQE='],
        },
        'kube_system_namespace_uid': 'uid-kube-system',
        'target_namespace_uid': 'uid-serve-canary',
        'api_server_git_version': 'v1.33.1',
        'caller_service_account_namespace': 'skypilot-system',
        'caller_service_account_name': 'authority-worker',
        'caller_service_account_uid': 'uid-authority-worker',
        'workload_service_account_namespace': 'serve-canary',
        'workload_service_account_name': 'serve-workload',
        'workload_service_account_uid': 'uid-serve-workload',
    }


def _workspace_identity() -> dict:
    return {
        'version': 1,
        'workspace': 'workspace-a',
        'kubernetes_scope': _scope(),
    }


def _target() -> dict:
    scope_sha256 = actions.ProviderKubernetesScopeV1.from_value(_scope()).sha256
    topology = {
        'version': 1,
        'kind': 'single_direct_pod_two_services',
        'node_count': 1,
        'application_port': '8080',
        'resources_ports': ['8080'],
        'mutable_objects': [{
            'kind': kind,
            'role': role,
            'name': name,
            'labels': [{
                'key': key,
                'value': value
            } for key, value in sorted({
                **{
                    item['key']: item['value'] for item in _identity_labels()
                },
                'role-specific': role,
            }.items())],
        } for role, kind, name in (('head_ssh_service', 'Service',
                                    'svc-replica-head-ssh'),
                                   ('head_service', 'Service',
                                    'svc-replica-head'), ('head_pod', 'Pod',
                                                          'svc-replica-head'))],
        'shared_prerequisites': 'preexisting_read_only',
    }
    return {
        'version': 1,
        'profile': 'pod_cluster_v1',
        'cloud': 'kubernetes',
        'region': None,
        'zone': None,
        'sky_cluster_name': 'svc',
        'sky_cluster_record_uuid': _CLUSTER_UUID,
        'kubernetes': {
            'scope': _scope(),
            'cluster_fingerprint_sha256': scope_sha256,
            'namespace': 'serve-canary',
            'name_basis': {
                'version': 1,
                'display_name': 'svc',
                'frozen_user_hash': 'replica',
                'max_length': 42,
                'cluster_name_hash_length': 8,
            },
            'provider_cluster_name': 'svc-replica',
            'workload_kind': 'Pod',
            'workload_name': 'svc-replica-head',
            'cluster_record_uuid_label': _CLUSTER_UUID,
            'replica_incarnation_label': _REPLICA_UUID,
            'topology': topology,
        },
    }


def _resource_snapshot() -> dict:
    scope_sha256 = actions.ProviderKubernetesScopeV1.from_value(_scope()).sha256
    return {
        'version': 1,
        'cloud': 'kubernetes',
        'cluster_fingerprint_sha256': scope_sha256,
        'namespace': 'serve-canary',
        'instance_type': '2CPU--4GB',
        'accelerator': None,
        'cpus': '2',
        'memory': '4',
        'image_id': 'docker:registry.example/runtime@sha256:' + '9' * 64,
        'disk_size_gb': 50,
        'disk_tier': None,
        'ports': ['8080'],
        'labels': [],
        'use_spot': False,
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
    contracts = {
        'head_ssh_service': (0, 'Service', 'svc-replica-head-ssh'),
        'head_service': (1, 'Service', 'svc-replica-head'),
        'head_pod': (2, 'Pod', 'svc-replica-head'),
    }
    sequence, kind, name = contracts[role]
    labels = _identity_labels()
    body_labels = {item['key']: item['value'] for item in labels}
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
        'normalization_profile': _artifact(),
    }


def _allocation(pointer: str, allocator: str, value: object) -> dict:
    return {'json_pointer': pointer, 'allocator': allocator, 'value': value}


def _service_allocations(role: str, *, family: str = 'IPv4') -> list[dict]:
    if role == 'head_ssh_service':
        cluster_ip = '10.0.0.7' if family == 'IPv4' else '2001:db8::7'
    else:
        cluster_ip = 'None'
    return [
        _allocation('/spec/clusterIP', 'api_server', cluster_ip),
        _allocation('/spec/clusterIPs', 'api_server', [cluster_ip]),
        _allocation('/spec/ipFamilies', 'api_server', [family]),
        _allocation('/spec/ipFamilyPolicy', 'api_server', 'SingleStack'),
    ]


def _pod_allocations(*, scheduled: bool = True) -> list[dict]:
    if not scheduled:
        return []
    return [_allocation('/spec/nodeName', 'scheduler', 'node-a.example')]


def _uid(role: str) -> str:
    return {
        'head_ssh_service': 'uid-head-ssh-service',
        'head_service': 'uid-head-service',
        'head_pod': 'uid-head-pod',
    }[role]


def _resolved_object(role: str, *, scheduled: bool = True) -> dict:
    plan = _object_plan(role)
    allocations = (_pod_allocations(scheduled=scheduled)
                   if role == 'head_pod' else _service_allocations(role))
    return {
        'role': role,
        'kind': plan['kind'],
        'namespace': plan['namespace'],
        'name': plan['name'],
        'uid': _uid(role),
        'observed_semantic_sha256': plan['requested_semantic_sha256'],
        'server_allocations': allocations,
    }


def _slot(index: int, *, committed: bool, scheduled: bool = True) -> dict:
    role = ('head_ssh_service', 'head_service', 'head_pod')[index]
    return {
        'sequence': index,
        'role': role,
        'disposition': 'committed' if committed else 'unknown',
        'object':
            (_resolved_object(role, scheduled=scheduled) if committed else None
            ),
    }


def _partial_target(committed_count: int, *, scheduled: bool = True) -> dict:
    target = actions.ProviderLocatorV1.from_value(_target())
    return {
        'version': 1,
        'requested_target_sha256': target.sha256,
        'kubernetes_objects': [
            _slot(index, committed=index < committed_count, scheduled=scheduled)
            for index in range(3)
        ],
    }


def _handle() -> dict:
    scope = actions.ProviderKubernetesScopeV1.from_value(_scope())
    target = actions.ProviderLocatorV1.from_value(_target())
    resources = actions.ProviderPodResourceSnapshotV1.from_value(
        _resource_snapshot())
    config = {
        'context_mode': 'in_cluster',
        'scope_sha256': scope.sha256,
        'namespace': 'serve-canary',
        'port_mode': 'podip',
        'use_internal_ips': True,
        'application_port': '8080',
        'pod_name': 'svc-replica-head',
        'pod_uid': _uid('head_pod'),
        'node_name': 'node-a.example',
        'pod_ip': '2001:db8::17',
        'head_service_uid': _uid('head_service'),
        'head_ssh_service_uid': _uid('head_ssh_service'),
        'ambient_fallback': False,
    }
    return {
        'version': 1,
        'cluster_record_uuid': _CLUSTER_UUID,
        'cluster_name': 'svc',
        'cluster_name_on_cloud': 'svc-replica',
        'requested_target_sha256': target.sha256,
        'launched_resources_sha256': resources.sha256,
        'provider_config': config,
        'provider_config_sha256': actions.canonical_sha256(config),
    }


def _cleanup_object(role: str,
                    *,
                    committed: bool = True,
                    scheduled: bool = True) -> dict:
    plan = _object_plan(role)
    allocations = (_pod_allocations(scheduled=scheduled)
                   if role == 'head_pod' else _service_allocations(role))
    return {
        'sequence': plan['sequence'],
        'role': role,
        'plan': plan,
        'committed_uid': _uid(role) if committed else None,
        'committed_server_allocations': allocations if committed else [],
    }


def _cleanup_target(*,
                    basis_kind: str = 'completed_launch',
                    committed_count: int = 3,
                    scheduled: bool = True,
                    exact_handle: bool | None = None) -> dict:
    if exact_handle is None:
        exact_handle = basis_kind == 'completed_launch'
    roles = ('head_ssh_service', 'head_service', 'head_pod')
    target = actions.ProviderLocatorV1.from_value(_target())
    return {
        'version': 1,
        'basis_kind': basis_kind,
        'requested_target_sha256': target.sha256,
        'cluster_name': 'svc',
        'cluster_record_uuid': _CLUSTER_UUID,
        'objects': [
            _cleanup_object(role,
                            committed=index < committed_count,
                            scheduled=scheduled)
            for index, role in enumerate(roles)
        ],
        'cluster_row_disposition': 'exact_handle'
                                   if exact_handle else 'not_found',
        'handle': _handle() if exact_handle else None,
        'observed_at': _OBSERVED_AT,
    }


@pytest.mark.parametrize('role',
                         ['head_ssh_service', 'head_service', 'head_pod'])
def test_resolved_object_roundtrip_hash_and_role_allocations(role: str) -> None:
    raw = _resolved_object(role)
    parsed = actions.ProviderKubernetesResolvedObjectV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert actions.ProviderKubernetesResolvedObjectV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes
    assert parsed.has_complete_allocations
    if role == 'head_ssh_service':
        assert parsed.canonical_bytes == _EXPECTED_SSH_RESOLVED_BYTES
        assert parsed.sha256 == (
            '560339a1985755ab966b5077238b681bcd551ec794bce8bc13cffcb12fde0deb')


def test_resolved_pod_allows_unscheduled_partial_but_marks_it_incomplete(
) -> None:
    raw = _resolved_object('head_pod', scheduled=False)
    parsed = actions.ProviderKubernetesResolvedObjectV1.from_value(raw)

    assert not parsed.server_allocations
    assert not parsed.has_complete_allocations


@pytest.mark.parametrize(('mutate', 'match'), [
    (lambda value: value.update({'kind': 'Pod'}), 'role and kind'),
    (lambda value: value['server_allocations'].pop(), 'quartet'),
    (lambda value: value['server_allocations'].reverse(), 'canonical order'),
    (lambda value: value['server_allocations'][2].update({'value': ['IPv6']}),
     'address family'),
    (lambda value: value['server_allocations'][0].update({'value': 'None'}),
     'must have a cluster IP'),
])
def test_resolved_ssh_service_rejects_role_quartet_and_family_conflicts(
        mutate, match: str) -> None:
    raw = _resolved_object('head_ssh_service')
    mutate(raw)
    with pytest.raises((TypeError, ValueError), match=match):
        actions.ProviderKubernetesResolvedObjectV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('clusterIP', '10.0.0.7'),
    ('clusterIPs', ['10.0.0.7']),
    ('ipFamilyPolicy', 'PreferDualStack'),
])
def test_resolved_headless_service_rejects_non_headless_or_dual_stack(
        field: str, value: object) -> None:
    raw = _resolved_object('head_service')
    indices = {'clusterIP': 0, 'clusterIPs': 1, 'ipFamilyPolicy': 3}
    raw['server_allocations'][indices[field]]['value'] = value
    with pytest.raises(ValueError):
        actions.ProviderKubernetesResolvedObjectV1.from_value(raw)


def test_resolved_service_accepts_canonical_ipv6_family() -> None:
    raw = _resolved_object('head_ssh_service')
    raw['server_allocations'] = _service_allocations('head_ssh_service',
                                                     family='IPv6')
    parsed = actions.ProviderKubernetesResolvedObjectV1.from_value(raw)
    assert parsed.server_allocations[0].value.canonical_value() == '2001:db8::7'


@pytest.mark.parametrize('committed_count', range(4))
def test_partial_target_accepts_every_committed_prefix(
        committed_count: int) -> None:
    raw = _partial_target(committed_count, scheduled=False)
    parsed = actions.PartialResolvedProviderTargetV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert sum(slot.object is not None
               for slot in parsed.kubernetes_objects) == committed_count
    parsed.validate_requested_target(
        actions.ProviderLocatorV1.from_value(_target()))
    if committed_count == 2:
        assert len(parsed.canonical_bytes) == 1_476
        assert parsed.sha256 == (
            'b7313adc3b1a189ed50e4f40233f8e3002315a0b3d90b0ffa7d07aff41acd508')


def test_resolved_slot_rejects_nullability_role_order_and_nonprefix() -> None:
    bad = _slot(0, committed=True)
    bad['object'] = None
    with pytest.raises(ValueError, match='require an object'):
        actions.ProviderKubernetesResolvedObjectSlotV1.from_value(bad)

    bad = _slot(0, committed=False)
    bad['object'] = _resolved_object('head_ssh_service')
    with pytest.raises(ValueError, match='unknown slots require null'):
        actions.ProviderKubernetesResolvedObjectSlotV1.from_value(bad)

    bad = _slot(0, committed=True)
    bad['role'] = 'head_service'
    with pytest.raises(ValueError, match='sequence and role'):
        actions.ProviderKubernetesResolvedObjectSlotV1.from_value(bad)

    bad_target = _partial_target(1)
    bad_target['kubernetes_objects'][2] = _slot(2, committed=True)
    with pytest.raises(ValueError, match='prefix'):
        actions.PartialResolvedProviderTargetV1.from_value(bad_target)


def test_handle_roundtrip_hash_and_all_bound_preimages() -> None:
    raw = _handle()
    parsed = actions.ProviderKubernetesHandleV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert len(parsed.canonical_bytes) == 857
    assert parsed.sha256 == (
        'dfc7d947c1b36a525074cbeecb2746a74e1c4473f622b705acde97054119bcd8')
    assert actions.ProviderKubernetesHandleV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes
    parsed.validate_requested_target(
        actions.ProviderLocatorV1.from_value(_target()))
    parsed.validate_launched_resources(
        actions.ProviderPodResourceSnapshotV1.from_value(_resource_snapshot()))
    parsed.validate_workspace_identity(
        actions.ProviderWorkspaceIdentityV1.from_value(_workspace_identity()))
    target = actions.ProviderLocatorV1.from_value(_target())
    assert target.kubernetes is not None
    assert (parsed.provider_config.scope_sha256 ==
            target.kubernetes.cluster_fingerprint_sha256)


@pytest.mark.parametrize(('field', 'value'), [
    ('context_mode', 'kubeconfig'),
    ('port_mode', 'loadbalancer'),
    ('use_internal_ips', False),
    ('use_internal_ips', 1),
    ('application_port', '08080'),
    ('pod_name', 'Pod_Name'),
    ('node_name', 'Node-A'),
    ('pod_ip', '2001:DB8::17'),
    ('pod_ip', 'fe80::1%eth0'),
    ('ambient_fallback', True),
    ('ambient_fallback', 0),
])
def test_handle_provider_config_rejects_wrong_literal_ip_and_names(
        field: str, value: object) -> None:
    raw = _handle()
    raw['provider_config'][field] = value
    raw['provider_config_sha256'] = actions.canonical_sha256(
        raw['provider_config'])
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesHandleV1.from_value(raw)


@pytest.mark.parametrize(('field', 'spoofed_value', 'message'), [
    ('context_mode', 'in_cluster',
     'handle provider_config context_mode must be in_cluster.'),
    ('port_mode', 'podip', 'handle provider_config port_mode must be podip.'),
])
@pytest.mark.parametrize('spoof_type',
                         _SPOOFING_STRING_TYPES,
                         ids=('equality', 'hash', 'length', 'bound'))
def test_handle_provider_config_literals_reject_spoofing_subclasses(
        field: str, spoofed_value: str, message: str,
        spoof_type: type[str]) -> None:
    spoofed_value = spoof_type(spoofed_value)
    raw_config = _handle()['provider_config']
    raw_config[field] = spoofed_value
    with pytest.raises(ValueError) as wire_error:
        actions.ProviderKubernetesHandleProviderConfigV1.from_value(raw_config)
    assert str(wire_error.value) == message

    parsed = actions.ProviderKubernetesHandleProviderConfigV1.from_value(
        _handle()['provider_config'])
    with pytest.raises(ValueError) as direct_error:
        dataclasses.replace(parsed, **{field: spoofed_value})
    assert str(direct_error.value) == message


def test_handle_rejects_provider_config_hash_and_pod_name_mapping() -> None:
    raw = _handle()
    raw['provider_config_sha256'] = '0' * 64
    with pytest.raises(ValueError, match='hash does not match'):
        actions.ProviderKubernetesHandleV1.from_value(raw)

    raw = _handle()
    raw['cluster_name_on_cloud'] = 'other'
    with pytest.raises(ValueError, match='Pod name'):
        actions.ProviderKubernetesHandleV1.from_value(raw)

    raw = _handle()
    raw['cluster_name_on_cloud'] = 'replacement'
    raw['provider_config']['pod_name'] = 'replacement-head'
    raw['provider_config_sha256'] = actions.canonical_sha256(
        raw['provider_config'])
    parsed = actions.ProviderKubernetesHandleV1.from_value(raw)
    with pytest.raises(ValueError, match='requested target'):
        parsed.validate_requested_target(
            actions.ProviderLocatorV1.from_value(_target()))


def test_workspace_identity_roundtrip_hash_and_typed_scope() -> None:
    raw = _workspace_identity()
    parsed = actions.ProviderWorkspaceIdentityV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert len(parsed.canonical_bytes) == 816
    assert parsed.sha256 == (
        '241ad8ccaa5093bd19b610826989b10fd99b3a1bd73872989491bba03d08c4f3')
    assert actions.ProviderWorkspaceIdentityV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes
    with pytest.raises(TypeError, match='invalid type'):
        actions.ProviderWorkspaceIdentityV1(
            version=1, workspace='workspace-a',
            kubernetes_scope=_scope())  # type: ignore[arg-type]


def test_completed_cleanup_roundtrip_hash_handle_and_delete_projection(
) -> None:
    raw = _cleanup_target()
    parsed = actions.ProviderKubernetesCleanupTargetV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert len(parsed.canonical_bytes) == 7_152
    assert parsed.sha256 == (
        '49d6435cf094c3fb4410e1b4405560854c8e0be37b9213db163ce038e88e3e5f')
    assert actions.ProviderKubernetesCleanupTargetV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes
    assert [item.role.value for item in parsed.objects
           ] == ['head_ssh_service', 'head_service', 'head_pod']
    assert [item.role.value for item in parsed.objects_in_delete_order
           ] == ['head_service', 'head_ssh_service', 'head_pod']
    parsed.validate_requested_target(
        actions.ProviderLocatorV1.from_value(_target()))


@pytest.mark.parametrize('committed_count', range(4))
def test_partial_cleanup_not_found_accepts_every_uid_prefix(
        committed_count: int) -> None:
    raw = _cleanup_target(basis_kind='partial_launch_cleanup',
                          committed_count=committed_count,
                          scheduled=False,
                          exact_handle=False)
    parsed = actions.ProviderKubernetesCleanupTargetV1.from_value(raw)

    assert parsed.handle is None
    assert parsed.canonical_value() == raw


def test_partial_cleanup_may_retain_a_complete_exact_handle() -> None:
    raw = _cleanup_target(basis_kind='partial_launch_cleanup',
                          exact_handle=True)
    parsed = actions.ProviderKubernetesCleanupTargetV1.from_value(raw)
    assert parsed.handle is not None


def test_cleanup_target_rejects_disposition_and_basis_nullability() -> None:
    raw = _cleanup_target()
    raw['cluster_row_disposition'] = 'not_found'
    raw['handle'] = None
    with pytest.raises(ValueError, match='completed cleanup target'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw)

    raw = _cleanup_target(basis_kind='partial_launch_cleanup',
                          exact_handle=False)
    raw['cluster_row_disposition'] = 'exact_handle'
    with pytest.raises(ValueError, match='requires a handle'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw)

    raw = _cleanup_target(basis_kind='partial_launch_cleanup',
                          exact_handle=True)
    raw['cluster_row_disposition'] = 'not_found'
    with pytest.raises(ValueError, match='requires null'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw)


def test_completed_cleanup_requires_all_uids_and_pod_node_name() -> None:
    raw = _cleanup_target()
    raw['objects'][2]['committed_uid'] = None
    raw['objects'][2]['committed_server_allocations'] = []
    with pytest.raises(ValueError, match='all three committed UIDs'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw)

    raw = _cleanup_target()
    raw['objects'][2]['committed_server_allocations'] = []
    with pytest.raises(ValueError, match='scheduler nodeName'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw)


def test_cleanup_object_rejects_allocations_without_uid_and_plan_mismatch(
) -> None:
    raw = _cleanup_object('head_service', committed=False)
    raw['committed_server_allocations'] = _service_allocations('head_service')
    with pytest.raises(ValueError, match='without a committed UID'):
        actions.ProviderKubernetesCleanupObjectV1.from_value(raw)

    raw = _cleanup_object('head_service')
    raw['plan'] = _object_plan('head_pod')
    with pytest.raises(ValueError, match='embedded plan'):
        actions.ProviderKubernetesCleanupObjectV1.from_value(raw)


def test_cleanup_target_rejects_nonprefix_plan_group_and_cluster_uuid() -> None:
    raw = _cleanup_target(basis_kind='partial_launch_cleanup',
                          committed_count=1,
                          exact_handle=False)
    raw['objects'][2] = _cleanup_object('head_pod')
    with pytest.raises(ValueError, match='prefix'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw)

    raw = _cleanup_target()
    raw['objects'][0]['plan']['namespace'] = 'other'
    raw['objects'][0]['plan']['request_body']['metadata']['namespace'] = 'other'
    raw['objects'][0]['plan']['request_body_sha256'] = actions.canonical_sha256(
        raw['objects'][0]['plan']['request_body'])
    with pytest.raises(ValueError, match='share namespace'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw)

    raw = _cleanup_target()
    raw['cluster_record_uuid'] = '33333333-3333-4333-8333-333333333333'
    with pytest.raises(ValueError, match='cluster UUID'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw)


@pytest.mark.parametrize(('object_index', 'field'), [
    (0, 'head_ssh_service_uid'),
    (1, 'head_service_uid'),
    (2, 'pod_uid'),
])
def test_cleanup_exact_handle_rejects_every_replacement_uid(
        object_index: int, field: str) -> None:
    raw = _cleanup_target()
    raw['objects'][object_index]['committed_uid'] = 'replacement-uid'
    with pytest.raises(ValueError, match='exact handle conflicts'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw)

    raw = _cleanup_target()
    raw['handle']['provider_config'][field] = 'replacement-uid'
    raw['handle']['provider_config_sha256'] = actions.canonical_sha256(
        raw['handle']['provider_config'])
    with pytest.raises(ValueError, match='exact handle conflicts'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw)


def test_cleanup_exact_handle_rejects_target_name_node_and_hash_conflicts(
) -> None:
    mutations = (
        lambda value: value['handle'].update({'cluster_name': 'other'}),
        lambda value: value['handle'].update(
            {'requested_target_sha256': '0' * 64}),
        lambda value: value['handle']['provider_config'].update(
            {'node_name': 'node-b.example'}),
    )
    for mutate in mutations:
        raw = _cleanup_target()
        mutate(raw)
        raw['handle']['provider_config_sha256'] = actions.canonical_sha256(
            raw['handle']['provider_config'])
        with pytest.raises(ValueError, match='exact handle conflicts'):
            actions.ProviderKubernetesCleanupTargetV1.from_value(raw)


def test_new_contracts_are_closed_and_immutable() -> None:
    values_and_parsers = (
        (_resolved_object('head_pod'),
         actions.ProviderKubernetesResolvedObjectV1.from_value),
        (_slot(0, committed=True),
         actions.ProviderKubernetesResolvedObjectSlotV1.from_value),
        (_partial_target(1),
         actions.PartialResolvedProviderTargetV1.from_value),
        (_handle(), actions.ProviderKubernetesHandleV1.from_value),
        (_cleanup_object('head_pod'),
         actions.ProviderKubernetesCleanupObjectV1.from_value),
        (_cleanup_target(),
         actions.ProviderKubernetesCleanupTargetV1.from_value),
        (_workspace_identity(), actions.ProviderWorkspaceIdentityV1.from_value),
    )
    for raw, parser in values_and_parsers:
        unknown = copy.deepcopy(raw)
        unknown['unknown'] = None
        with pytest.raises(ValueError, match='unknown or missing'):
            parser(unknown)
        parsed = parser(raw)
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(parsed, '_immutability_probe', None)


def test_pre_release_flattened_resolved_target_wire_is_rejected() -> None:
    raw = {
        'version': 1,
        'requested_target_sha256': 'a' * 64,
        'provider_resource_id': 'pod/svc-replica-head',
        'workload_uid': 'uid-head-pod',
        'provider_operation_id': None,
        'resolved_at': _OBSERVED_AT,
    }
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ResolvedProviderTargetV1.from_value(raw)

    incomplete_current_shape = copy.deepcopy(raw)
    incomplete_current_shape['kubernetes_objects'] = []
    with pytest.raises(ValueError, match='exactly three'):
        actions.ResolvedProviderTargetV1.from_value(incomplete_current_shape)


def test_canonical_locator_wire_embeds_complete_addressing_preimages() -> None:
    parsed = actions.ProviderLocatorV1.from_value(_target())
    assert parsed.canonical_bytes == actions.canonical_json_bytes(
        parsed.canonical_value())
    assert parsed.kubernetes is not None
    assert parsed.kubernetes.cluster_fingerprint_sha256 == (
        parsed.kubernetes.scope.sha256)
    assert set(parsed.kubernetes.canonical_value()) == {
        'scope', 'cluster_fingerprint_sha256', 'namespace', 'name_basis',
        'provider_cluster_name', 'workload_kind', 'workload_name',
        'cluster_record_uuid_label', 'replica_incarnation_label', 'topology'
    }
    missing_scope = _target()
    del missing_scope['kubernetes']['scope']
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderLocatorV1.from_value(missing_scope)


def test_new_exact_collections_reject_cardinality_before_child_parsing(
) -> None:
    cyclic_allocation: dict[str, object] = {
        'json_pointer': '/spec/clusterIP',
        'allocator': 'api_server',
    }
    cyclic_allocation['value'] = cyclic_allocation

    raw_resolved = _resolved_object('head_ssh_service')
    raw_resolved['server_allocations'] = [cyclic_allocation] * 5
    with pytest.raises(ValueError, match='quartet'):
        actions.ProviderKubernetesResolvedObjectV1.from_value(raw_resolved)

    raw_partial = _partial_target(3)
    raw_partial['kubernetes_objects'].append(raw_partial)
    with pytest.raises(ValueError, match='exactly three'):
        actions.PartialResolvedProviderTargetV1.from_value(raw_partial)

    raw_cleanup_object = _cleanup_object('head_service')
    raw_cleanup_object['committed_server_allocations'] = [cyclic_allocation] * 5
    with pytest.raises(ValueError, match='cardinality'):
        actions.ProviderKubernetesCleanupObjectV1.from_value(raw_cleanup_object)

    raw_cleanup_target = _cleanup_target()
    raw_cleanup_target['objects'].append(raw_cleanup_target)
    with pytest.raises(ValueError, match='exactly three'):
        actions.ProviderKubernetesCleanupTargetV1.from_value(raw_cleanup_target)


def test_new_nested_parsers_reject_cycles_before_recursive_serialization(
) -> None:
    raw_slot = _slot(0, committed=True)
    raw_slot['object'] = raw_slot
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesResolvedObjectSlotV1.from_value(raw_slot)

    raw_config = _handle()['provider_config']
    raw_config['pod_ip'] = raw_config
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesHandleProviderConfigV1.from_value(raw_config)

    raw_handle = _handle()
    raw_handle['provider_config'] = raw_handle
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesHandleV1.from_value(raw_handle)

    raw_workspace = _workspace_identity()
    raw_workspace['kubernetes_scope'] = raw_workspace
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderWorkspaceIdentityV1.from_value(raw_workspace)
