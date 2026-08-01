"""Pure frozen Kubernetes transport and scope contract tests."""

import base64
import copy
import dataclasses

import pytest

from sky.serve import resource_actions as actions

_OBSERVED_AT = '2026-08-01T05:06:07.123456Z'
_EXPECTED_SCOPE_BYTES = (
    b'{"api_server_git_version":"v1.33.1","caller_service_account_name":'
    b'"authority-worker","caller_service_account_namespace":"skypilot-system",'
    b'"caller_service_account_uid":"uid-authority-worker","context_identity":'
    b'["skypilot-in-cluster-identity-kubernetes"],"context_name":"kubernetes",'
    b'"in_cluster":true,"kube_system_namespace_uid":"uid-kube-system",'
    b'"namespace":"serve-canary","target_namespace_uid":"uid-serve-canary",'
    b'"transport":{"ca_cert_der_base64":["MAMCAQE=","MAMCAQI="],'
    b'"server_origin":{"host":"10.0.0.1","path":"/","port":443,'
    b'"scheme":"https"},"tls_server_name":"kubernetes.default.svc",'
    b'"version":1},"version":1,"workload_service_account_name":'
    b'"serve-workload","workload_service_account_namespace":"serve-canary",'
    b'"workload_service_account_uid":"uid-serve-workload"}')
_EXPECTED_SCOPE_SHA256 = (
    '6427392beea1d2a3d09af95b9343e7a7b5a29bcc5024e20e6a880f20fda29a90')


def _transport() -> dict:
    return {
        'version': 1,
        'server_origin': {
            'scheme': 'https',
            'host': '10.0.0.1',
            'port': 443,
            'path': '/',
        },
        'tls_server_name': 'kubernetes.default.svc',
        # Canonical RFC 4648 encodings of two distinct nonempty DER byte
        # sequences.  Their encoded order is the committed set order.
        'ca_cert_der_base64': ['MAMCAQE=', 'MAMCAQI='],
    }


def _scope() -> dict:
    return {
        'version': 1,
        'context_name': 'kubernetes',
        'context_identity': ['skypilot-in-cluster-identity-kubernetes'],
        'in_cluster': True,
        'namespace': 'serve-canary',
        'transport': _transport(),
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


def _scope_read(disposition: str = 'complete') -> dict:
    return {
        'disposition': disposition,
        'scope': _scope() if disposition == 'complete' else None,
        'observed_at': _OBSERVED_AT,
    }


def test_literal_scope_canonical_bytes_and_hash() -> None:
    scope = actions.ProviderKubernetesScopeV1.from_value(_scope())

    assert scope.canonical_bytes == _EXPECTED_SCOPE_BYTES
    assert scope.sha256 == _EXPECTED_SCOPE_SHA256


def test_transport_scope_and_scope_read_roundtrip() -> None:
    transport = actions.ProviderKubernetesTransportIdentityV1.from_value(
        _transport())
    scope = actions.ProviderKubernetesScopeV1.from_value(_scope())
    scope_read = actions.ProviderKubernetesScopeReadV1.from_value(_scope_read())
    roundtripped_transport = (
        actions.ProviderKubernetesTransportIdentityV1.from_value(
            transport.canonical_value()))
    roundtripped_scope = actions.ProviderKubernetesScopeV1.from_value(
        scope.canonical_value())
    roundtripped_read = actions.ProviderKubernetesScopeReadV1.from_value(
        scope_read.canonical_value())

    assert transport.canonical_value() == _transport()
    assert roundtripped_transport.canonical_bytes == transport.canonical_bytes
    assert scope.canonical_value() == _scope()
    assert roundtripped_scope.canonical_bytes == scope.canonical_bytes
    assert scope_read.canonical_value() == _scope_read()
    assert roundtripped_read.canonical_bytes == scope_read.canonical_bytes


def test_general_scope_accepts_kubeconfig_identity_and_null_tls_name() -> None:
    value = _scope()
    value['in_cluster'] = False
    value['context_name'] = 'canary-context'
    value['context_identity'] = ['cluster_user_serve-canary']
    value['transport']['tls_server_name'] = None

    parsed = actions.ProviderKubernetesScopeV1.from_value(value)

    assert parsed.in_cluster is False
    assert parsed.context_identity == ('cluster_user_serve-canary',)
    assert parsed.transport.tls_server_name is None


@pytest.mark.parametrize(('factory', 'parser'), [
    (_transport, actions.ProviderKubernetesTransportIdentityV1.from_value),
    (_scope, actions.ProviderKubernetesScopeV1.from_value),
    (_scope_read, actions.ProviderKubernetesScopeReadV1.from_value),
])
def test_closed_top_level_objects_reject_unknown_and_missing_keys(
        factory, parser) -> None:
    unknown = factory()
    unknown['unknown'] = 'value'
    with pytest.raises(ValueError, match='unknown or missing'):
        parser(unknown)

    missing = factory()
    del missing[next(iter(missing))]
    with pytest.raises(ValueError, match='unknown or missing'):
        parser(missing)


def test_server_origin_is_closed() -> None:
    unknown = _transport()
    unknown['server_origin']['query'] = 'forbidden'
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesTransportIdentityV1.from_value(unknown)

    missing = _transport()
    del missing['server_origin']['host']
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesTransportIdentityV1.from_value(missing)


@pytest.mark.parametrize('certificates', [
    [],
    [''],
    ['MAMCAQE'],
    ['MAMCAQE=\n'],
    ['MAMCAQE==='],
    ['AB=='],
    ['not-base64!'],
    [1],
    ('MAMCAQE=',),
    ['MAMCAQE=', 'MAMCAQE='],
    ['MAMCAQI=', 'MAMCAQE='],
])
def test_ca_certificate_set_requires_canonical_sorted_rfc4648_base64(
        certificates) -> None:
    value = _transport()
    value['ca_cert_der_base64'] = certificates

    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesTransportIdentityV1.from_value(value)


def test_transport_rejects_oversize_canonical_object() -> None:
    value = _transport()
    # Four distinct maximum-size scalars fit their individual bounds while
    # making the enclosing transport exceed its canonical-object budget.
    value['ca_cert_der_base64'] = sorted(
        base64.b64encode(bytes([byte]) * 12_288).decode('ascii')
        for byte in range(4))

    with pytest.raises(ValueError, match='exceeds 65536 bytes'):
        actions.ProviderKubernetesTransportIdentityV1.from_value(value)


@pytest.mark.parametrize(('decoded_size', 'encoded_size'), [
    (777, 1036),
    (12_288, 16_384),
])
def test_ca_certificate_scalar_accepts_observed_and_maximum_sizes(
        decoded_size: int, encoded_size: int) -> None:
    value = _transport()
    # 777 bytes is the DER size observed on boltz-test. This pure DTO freezes
    # encoding and size only; the live normalizer is responsible for X.509.
    certificate = base64.b64encode(b'\x01' * decoded_size).decode('ascii')
    assert len(certificate) == encoded_size
    value['ca_cert_der_base64'] = [certificate]

    parsed = actions.ProviderKubernetesTransportIdentityV1.from_value(value)

    assert parsed.ca_cert_der_base64 == (certificate,)


def test_ca_certificate_scalar_rejects_16388_bytes_under_object_limit() -> None:
    value = _transport()
    certificate = base64.b64encode(b'\x01' * 12_289).decode('ascii')
    assert len(certificate) == 16_388
    value['ca_cert_der_base64'] = [certificate]

    with pytest.raises(ValueError, match='16384'):
        actions.ProviderKubernetesTransportIdentityV1.from_value(value)


def test_ca_certificate_set_rejects_257_items() -> None:
    value = _transport()
    value['ca_cert_der_base64'] = ['MAMCAQE='] * 257

    with pytest.raises(ValueError, match='1..256'):
        actions.ProviderKubernetesTransportIdentityV1.from_value(value)


def test_direct_transport_requires_a_tuple_ca_set() -> None:
    transport = actions.ProviderKubernetesTransportIdentityV1.from_value(
        _transport())

    with pytest.raises(ValueError, match='tuple'):
        dataclasses.replace(
            transport,
            ca_cert_der_base64=list(  # type: ignore[arg-type]
                transport.ca_cert_der_base64))


@pytest.mark.parametrize(('path', 'invalid'), [
    (('version',), False),
    (('version',), 2),
    (('server_origin', 'scheme'), 'http'),
    (('server_origin', 'host'), ''),
    (('server_origin', 'host'), 'x' * 1025),
    (('server_origin', 'host'), 'bad\x00host'),
    (('server_origin', 'host'), 'e\u0301.example'),
    (('server_origin', 'port'), False),
    (('server_origin', 'port'), 0),
    (('server_origin', 'port'), -1),
    (('server_origin', 'port'), 2**63),
    (('server_origin', 'port'), 443.0),
    (('server_origin', 'path'), ''),
    (('tls_server_name',), 1),
])
def test_transport_rejects_invalid_literals_ports_and_text(path,
                                                           invalid) -> None:
    value = _transport()
    target = value
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = invalid

    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesTransportIdentityV1.from_value(value)


@pytest.mark.parametrize(('field', 'invalid'), [
    ('version', False),
    ('version', 2),
    ('context_name', ''),
    ('context_name', 'bad\x00context'),
    ('context_name', 'e\u0301'),
    ('context_identity', []),
    ('context_identity', 'not-a-list'),
    ('context_identity', ['']),
    ('context_identity', ['identity'] * 257),
    ('in_cluster', 1),
    ('namespace', ''),
    ('target_namespace_uid', 7),
])
def test_scope_rejects_invalid_versions_text_tuples_and_bools(field,
                                                              invalid) -> None:
    value = _scope()
    value[field] = invalid

    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesScopeV1.from_value(value)


def test_direct_scope_requires_a_tuple_context_identity() -> None:
    scope = actions.ProviderKubernetesScopeV1.from_value(_scope())

    with pytest.raises(ValueError, match='tuple'):
        dataclasses.replace(
            scope,
            context_identity=list(  # type: ignore[arg-type]
                scope.context_identity))


def test_context_identity_tuple_order_is_preserved_and_semantic() -> None:
    first_value = _scope()
    first_value['context_identity'] = ['identity-z', 'identity-a']
    second_value = copy.deepcopy(first_value)
    second_value['context_identity'] = ['identity-a', 'identity-z']

    first = actions.ProviderKubernetesScopeV1.from_value(first_value)
    second = actions.ProviderKubernetesScopeV1.from_value(second_value)

    assert first.context_identity == ('identity-z', 'identity-a')
    assert first.canonical_value()['context_identity'] == [
        'identity-z', 'identity-a'
    ]
    assert first.sha256 != second.sha256


@pytest.mark.parametrize(
    'disposition',
    ['not_found', 'forbidden', 'timeout', 'transport_error', 'malformed'])
def test_failed_scope_read_dispositions_require_null_scope(
        disposition: str) -> None:
    complete = _scope_read(disposition)
    parsed = actions.ProviderKubernetesScopeReadV1.from_value(complete)
    assert parsed.scope is None

    complete['scope'] = _scope()
    with pytest.raises(ValueError, match='failed reads require null'):
        actions.ProviderKubernetesScopeReadV1.from_value(complete)


def test_complete_scope_read_requires_scope() -> None:
    value = _scope_read()
    value['scope'] = None

    with pytest.raises(ValueError, match='complete.*require a scope'):
        actions.ProviderKubernetesScopeReadV1.from_value(value)


@pytest.mark.parametrize('observed_at', [
    '2026-08-01T05:06:07Z',
    '2026-08-01T05:06:07.123456+00:00',
    '2026-02-30T05:06:07.123456Z',
    1,
])
def test_scope_read_rejects_noncanonical_timestamps(observed_at) -> None:
    value = _scope_read('timeout')
    value['observed_at'] = observed_at

    with pytest.raises(ValueError):
        actions.ProviderKubernetesScopeReadV1.from_value(value)


def test_scope_read_rejects_unknown_disposition() -> None:
    value = _scope_read('timeout')
    value['disposition'] = 'connection_refused'

    with pytest.raises(ValueError, match='unsupported'):
        actions.ProviderKubernetesScopeReadV1.from_value(value)


def test_workload_service_account_must_be_in_target_namespace() -> None:
    value = _scope()
    value['workload_service_account_namespace'] = 'other-namespace'

    with pytest.raises(ValueError, match='must equal the target namespace'):
        actions.ProviderKubernetesScopeV1.from_value(value)


@pytest.mark.parametrize('field', [
    'namespace',
    'caller_service_account_namespace',
    'workload_service_account_namespace',
])
def test_namespace_fields_accept_253_utf8_bytes(field: str) -> None:
    value = _scope()
    namespace = 'n' * 253
    value[field] = namespace
    if field in ('namespace', 'workload_service_account_namespace'):
        value['namespace'] = namespace
        value['workload_service_account_namespace'] = namespace

    parsed = actions.ProviderKubernetesScopeV1.from_value(value)

    assert getattr(parsed, field) == namespace


@pytest.mark.parametrize('field', [
    'namespace',
    'caller_service_account_namespace',
    'workload_service_account_namespace',
])
def test_namespace_fields_reject_254_utf8_bytes(field: str) -> None:
    value = _scope()
    namespace = 'n' * 254
    value[field] = namespace
    if field in ('namespace', 'workload_service_account_namespace'):
        value['namespace'] = namespace
        value['workload_service_account_namespace'] = namespace

    with pytest.raises(ValueError, match='1..253'):
        actions.ProviderKubernetesScopeV1.from_value(value)


def test_kube_system_target_requires_matching_namespace_uid() -> None:
    value = _scope()
    value['namespace'] = 'kube-system'
    value['workload_service_account_namespace'] = 'kube-system'

    with pytest.raises(ValueError, match='namespace name and namespace UIDs'):
        actions.ProviderKubernetesScopeV1.from_value(value)


def test_matching_namespace_uids_require_kube_system_target() -> None:
    value = _scope()
    value['target_namespace_uid'] = value['kube_system_namespace_uid']

    with pytest.raises(ValueError, match='namespace name and namespace UIDs'):
        actions.ProviderKubernetesScopeV1.from_value(value)


def test_kube_system_target_with_matching_namespace_uid_is_consistent() -> None:
    value = _scope()
    value['namespace'] = 'kube-system'
    value['target_namespace_uid'] = value['kube_system_namespace_uid']
    value['workload_service_account_namespace'] = 'kube-system'

    parsed = actions.ProviderKubernetesScopeV1.from_value(value)

    assert parsed.target_namespace_uid == parsed.kube_system_namespace_uid


def test_same_service_account_name_with_different_uid_is_contradictory(
) -> None:
    value = _scope()
    value['caller_service_account_namespace'] = value['namespace']
    value['caller_service_account_name'] = value[
        'workload_service_account_name']

    with pytest.raises(ValueError, match='contradict'):
        actions.ProviderKubernetesScopeV1.from_value(value)


def test_different_service_account_name_with_same_uid_is_contradictory(
) -> None:
    value = _scope()
    value['caller_service_account_uid'] = value['workload_service_account_uid']

    with pytest.raises(ValueError, match='contradict'):
        actions.ProviderKubernetesScopeV1.from_value(value)


def test_same_service_account_identity_is_internally_consistent() -> None:
    value = _scope()
    value['caller_service_account_namespace'] = value['namespace']
    value['caller_service_account_name'] = value[
        'workload_service_account_name']
    value['caller_service_account_uid'] = value['workload_service_account_uid']

    parsed = actions.ProviderKubernetesScopeV1.from_value(value)
    assert parsed.caller_service_account_uid == (
        parsed.workload_service_account_uid)


def test_contracts_are_frozen() -> None:
    scope = actions.ProviderKubernetesScopeV1.from_value(_scope())

    with pytest.raises(dataclasses.FrozenInstanceError):
        scope.namespace = 'other'  # type: ignore[misc]
