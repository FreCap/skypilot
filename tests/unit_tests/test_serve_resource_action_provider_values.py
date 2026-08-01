"""Pure bounded provider-value and Kubernetes resource contract tests."""

import copy
import dataclasses

import pytest

from sky.serve import resource_actions as actions


def _artifact(path: str = 'images/runtime.json') -> dict:
    return {'repo_path': path, 'byte_size': 17, 'sha256': 'a' * 64}


def _image() -> dict:
    return {
        'source': 'explicit',
        'qualification': {
            'requested_reference': 'registry.example/runtime:approved@sha256:' +
                                   '1' * 64,
            'oci_manifest_digest': 'sha256:' + '1' * 64,
            'oci_config_digest': 'sha256:' + '2' * 64,
            'qualification_artifact': _artifact(),
        },
        'auth_strategy': 'anonymous',
        'implementation_contract': 'kubernetes_serve_prebooted_runtime_v1',
    }


def _resources() -> dict:
    return {
        'source_cpus': '0.5',
        'source_memory_gb': '1.23',
        'pod_cpu_request': '0.5',
        'pod_cpu_limit': '0.5',
        'pod_memory_request': '1.23G',
        'pod_memory_limit': '1.23G',
        'translation_contract': 'sky_to_k8s_exact_resources_v1',
        'set_pod_resource_limits': True,
        'resource_limit_multiplier': 1,
        'live_allocatable_clamp': False,
        'accelerator': None,
        'ephemeral_storage': None,
        'image': _image(),
        'image_pull_policy': 'Always',
        'application_port': '8080',
        'resources_ports': ['8080'],
        'port_mode': 'podip',
    }


def _allocation(pointer: str, allocator: str, value: object) -> dict:
    return {
        'json_pointer': pointer,
        'allocator': allocator,
        'value': value,
    }


@pytest.mark.parametrize('value', [
    '1',
    '9223372036854775807',
    '0.5',
    '0.001',
    '1.23',
    '999.999',
    '9223372036854775806.999',
])
def test_canonical_positive_decimal_accepts_exact_domain(value: str) -> None:
    assert actions._canonical_positive_decimal_text(  # pylint: disable=protected-access
        value, name='test') == value


@pytest.mark.parametrize('value', [
    '',
    '0',
    '00',
    '01',
    '.5',
    '1.',
    '1.0',
    '1.230',
    '0.000',
    '0.0001',
    '+1',
    '-1',
    ' 1',
    '1 ',
    '1e1',
    '1G',
    '9223372036854775807.001',
    '9223372036854775808',
    '１２',
    None,
    1,
])
def test_canonical_positive_decimal_rejects_alternate_spellings(
        value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        actions._canonical_positive_decimal_text(  # pylint: disable=protected-access
            value, name='test')


def test_canonical_json_value_roundtrip_order_hash_and_integer_bounds() -> None:
    value = {'z': [None, True, False, -(2**63)], 'a': 2**63 - 1}
    parsed = actions.CanonicalJsonValue.from_value(value)

    assert parsed.canonical_bytes == (
        b'{"a":9223372036854775807,"z":[null,true,false,'
        b'-9223372036854775808]}')
    assert parsed.canonical_value() == value
    assert parsed.sha256 == actions.canonical_sha256(value)
    assert actions.CanonicalJsonValue(value).canonical_bytes == (
        parsed.canonical_bytes)


@pytest.mark.parametrize('value', [-(2**63) - 1, 2**63, 0.0, 1.5, b'x', {1}])
def test_canonical_json_value_rejects_non_json_and_numeric_overflow(
        value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        actions.CanonicalJsonValue(value)


def test_canonical_json_text_and_key_bounds_are_utf8_and_nfc() -> None:
    maximum_multibyte_text = '\N{LATIN SMALL LETTER E WITH ACUTE}' * 512
    parsed = actions.CanonicalJsonObject(
        {maximum_multibyte_text: maximum_multibyte_text})
    assert parsed.canonical_value() == {
        maximum_multibyte_text: maximum_multibyte_text
    }

    invalid_values = [
        '',
        '\N{LATIN SMALL LETTER E WITH ACUTE}' * 513,
        'e\N{COMBINING ACUTE ACCENT}',
        {
            '': 1
        },
        {
            'x' * 1_025: 1
        },
        {
            'e\N{COMBINING ACUTE ACCENT}': 1
        },
        {
            '\N{LATIN SMALL LETTER E WITH ACUTE}': 1,
            'e\N{COMBINING ACUTE ACCENT}': 2
        },
        {
            1: 'value'
        },
    ]
    for value in invalid_values:
        with pytest.raises((TypeError, ValueError)):
            actions.CanonicalJsonValue(value)


def test_canonical_json_rejects_tuples_and_reference_cycles() -> None:
    with pytest.raises(TypeError):
        actions.CanonicalJsonValue(('not', 'an', 'array'))
    with pytest.raises(TypeError):
        actions.CanonicalJsonValue({'nested': ['ok', ('tuple',)]})

    cyclic_list: list[object] = []
    cyclic_list.append(cyclic_list)
    with pytest.raises(ValueError, match='cycle'):
        actions.CanonicalJsonValue(cyclic_list)
    cyclic_object: dict[str, object] = {}
    cyclic_object['self'] = cyclic_object
    with pytest.raises(ValueError, match='cycle'):
        actions.CanonicalJsonObject(cyclic_object)


def test_canonical_json_container_member_and_depth_boundaries() -> None:
    assert len(actions.CanonicalJsonValue(list(
        range(256))).canonical_value()) == 256
    with pytest.raises(ValueError, match='at most 256'):
        actions.CanonicalJsonValue(list(range(257)))

    depth_sixteen: object = 1
    for _ in range(16):
        depth_sixteen = [depth_sixteen]
    actions.CanonicalJsonValue(depth_sixteen)
    depth_seventeen = [depth_sixteen]
    with pytest.raises(ValueError, match='depth'):
        actions.CanonicalJsonValue(depth_seventeen)


def test_canonical_json_aggregate_member_boundary() -> None:
    at_limit = [list(range(255)) for _ in range(16)]
    # Root has 16 elements and its children have 16 * 255: exactly 4096.
    actions.CanonicalJsonValue(at_limit)
    over_limit = copy.deepcopy(at_limit)
    over_limit[0].append(255)
    with pytest.raises(ValueError, match='aggregate'):
        actions.CanonicalJsonValue(over_limit)


def test_canonical_json_encoded_size_boundary() -> None:
    # Brackets, commas, and quotes contribute 193 bytes.  The string payloads
    # contribute 65,343 bytes, producing exactly 65,536 canonical bytes.
    at_limit = ['x' * 1_020] + ['x' * 1_021 for _ in range(63)]
    parsed = actions.CanonicalJsonValue(at_limit)
    assert len(parsed.canonical_bytes) == 65_536
    over_limit = list(at_limit)
    over_limit[0] += 'x'
    with pytest.raises(ValueError, match='65536'):
        actions.CanonicalJsonValue(over_limit)


def test_canonical_json_wrappers_are_immutable_and_return_detached_values(
) -> None:
    source = {'nested': ['first']}
    parsed = actions.CanonicalJsonObject(source)
    committed_bytes = parsed.canonical_bytes
    source['nested'].append('mutated')
    returned = parsed.canonical_value()
    returned['nested'].append('also-mutated')

    assert parsed.canonical_bytes == committed_bytes
    assert parsed.canonical_value() == {'nested': ['first']}
    with pytest.raises(dataclasses.FrozenInstanceError):
        parsed._canonical_bytes = b'{}'  # type: ignore[misc]  # pylint: disable=protected-access


@pytest.mark.parametrize('value', [None, True, 1, 'text', ['array']])
def test_canonical_json_object_requires_object_root(value: object) -> None:
    actions.CanonicalJsonValue(value)
    with pytest.raises(TypeError, match='object root'):
        actions.CanonicalJsonObject(value)


@pytest.mark.parametrize(('pointer', 'allocator', 'value'), [
    ('/spec/clusterIP', 'api_server', '10.0.0.1'),
    ('/spec/clusterIP', 'api_server', '2001:db8::1'),
    ('/spec/clusterIP', 'api_server', 'None'),
    ('/spec/clusterIPs', 'api_server', ['10.0.0.1']),
    ('/spec/clusterIPs', 'api_server', ['2001:db8::1']),
    ('/spec/clusterIPs', 'api_server', ['None']),
    ('/spec/ipFamilies', 'api_server', ['IPv4']),
    ('/spec/ipFamilies', 'api_server', ['IPv6']),
    ('/spec/ipFamilyPolicy', 'api_server', 'SingleStack'),
    ('/spec/nodeName', 'scheduler', 'node-1.zone-a'),
    ('/spec/nodeName', 'scheduler', '.'.join(
        ('a' * 63, 'b' * 63, 'c' * 63, 'd' * 61))),
])
def test_server_allocation_accepts_exact_pointer_dispatch(
        pointer: str, allocator: str, value: object) -> None:
    raw = _allocation(pointer, allocator, value)
    parsed = actions.ProviderKubernetesServerAllocationV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert actions.ProviderKubernetesServerAllocationV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes


@pytest.mark.parametrize(('pointer', 'allocator', 'value'), [
    ('/spec/unknown', 'api_server', 'value'),
    ('/spec/clusterIP', 'scheduler', '10.0.0.1'),
    ('/spec/nodeName', 'api_server', 'node-a'),
    ('/spec/clusterIP', 'api_server', ''),
    ('/spec/clusterIP', 'api_server', '010.0.0.1'),
    ('/spec/clusterIP', 'api_server', '2001:DB8::1'),
    ('/spec/clusterIP', 'api_server', 'fe80::1%eth0'),
    ('/spec/clusterIP', 'api_server', ['10.0.0.1']),
    ('/spec/clusterIPs', 'api_server', []),
    ('/spec/clusterIPs', 'api_server', ['10.0.0.1', '10.0.0.2']),
    ('/spec/clusterIPs', 'api_server', ['bad']),
    ('/spec/ipFamilies', 'api_server', 'IPv4'),
    ('/spec/ipFamilies', 'api_server', []),
    ('/spec/ipFamilies', 'api_server', ['IPv5']),
    ('/spec/ipFamilyPolicy', 'api_server', 'PreferDualStack'),
    ('/spec/nodeName', 'scheduler', ''),
    ('/spec/nodeName', 'scheduler', 'Node-A'),
    ('/spec/nodeName', 'scheduler', 'node_name'),
    ('/spec/nodeName', 'scheduler', '-node'),
    ('/spec/nodeName', 'scheduler', 'node.'),
    ('/spec/nodeName', 'scheduler', 'a' * 64),
    ('/spec/nodeName', 'scheduler', '.'.join(
        ('a' * 63, 'b' * 63, 'c' * 63, 'd' * 62))),
])
def test_server_allocation_rejects_malformed_pointer_allocator_and_value(
        pointer: str, allocator: str, value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesServerAllocationV1.from_value(
            _allocation(pointer, allocator, value))


def test_server_allocation_direct_construction_requires_immutable_value(
) -> None:
    with pytest.raises(TypeError, match='invalid type'):
        actions.ProviderKubernetesServerAllocationV1(  # type: ignore[arg-type]
            json_pointer='/spec/clusterIP',
            allocator='api_server',
            value='10.0.0.1')
    parsed = actions.ProviderKubernetesServerAllocationV1(
        json_pointer='/spec/clusterIP',
        allocator='api_server',
        value=actions.CanonicalJsonValue('10.0.0.1'))
    assert parsed.canonical_value()['value'] == '10.0.0.1'


def test_server_allocation_bounds_value_before_recursive_outer_parser() -> None:
    cyclic: list[object] = []
    cyclic.append(cyclic)

    depth_seventeen: object = 1
    for _ in range(17):
        depth_seventeen = [depth_seventeen]

    aggregate_overflow = [list(range(256)) for _ in range(16)]
    for value, match in ((cyclic, 'cycle'), (depth_seventeen, 'depth'),
                         (aggregate_overflow, 'aggregate')):
        with pytest.raises(ValueError, match=match):
            actions.ProviderKubernetesServerAllocationV1.from_value(
                _allocation('/spec/clusterIP', 'api_server', value))


def test_server_allocation_shallow_parser_keeps_closed_object_contract(
) -> None:
    missing = _allocation('/spec/clusterIP', 'api_server', '10.0.0.1')
    missing.pop('allocator')
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesServerAllocationV1.from_value(missing)

    unknown = _allocation('/spec/clusterIP', 'api_server', '10.0.0.1')
    unknown['unknown'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesServerAllocationV1.from_value(unknown)


def test_resource_contract_roundtrip_hash_and_exact_equalities() -> None:
    raw = _resources()
    parsed = actions.ProviderKubernetesResourceContractV1.from_value(raw)

    assert parsed.canonical_value() == raw
    assert parsed.sha256 == actions.canonical_sha256(raw)
    assert actions.ProviderKubernetesResourceContractV1.from_value(
        parsed.canonical_value()).canonical_bytes == parsed.canonical_bytes


@pytest.mark.parametrize(('field', 'value'), [
    ('translation_contract', 'other'),
    ('set_pod_resource_limits', False),
    ('set_pod_resource_limits', 1),
    ('resource_limit_multiplier', 2),
    ('resource_limit_multiplier', True),
    ('resource_limit_multiplier', 1.0),
    ('live_allocatable_clamp', True),
    ('live_allocatable_clamp', 0),
    ('accelerator', 'A100'),
    ('ephemeral_storage', '1G'),
    ('image_pull_policy', 'IfNotPresent'),
    ('port_mode', 'loadbalancer'),
])
def test_resource_contract_rejects_nonliteral_fields(field: str,
                                                     value: object) -> None:
    raw = _resources()
    raw[field] = value
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesResourceContractV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('pod_cpu_request', '500m'),
    ('pod_cpu_limit', '1'),
    ('pod_memory_request', '1.23Gi'),
    ('pod_memory_limit', '1.230G'),
])
def test_resource_contract_rejects_quantity_mismatch(field: str,
                                                     value: object) -> None:
    raw = _resources()
    raw[field] = value
    with pytest.raises(ValueError):
        actions.ProviderKubernetesResourceContractV1.from_value(raw)


@pytest.mark.parametrize(('field', 'value'), [
    ('source_cpus', '0'),
    ('source_cpus', '+1'),
    ('source_cpus', '1e1'),
    ('source_memory_gb', '1.0'),
    ('source_memory_gb', '1.2340'),
])
def test_resource_contract_rejects_noncanonical_source_decimal(
        field: str, value: object) -> None:
    raw = _resources()
    raw[field] = value
    if field == 'source_cpus':
        raw['pod_cpu_request'] = value
        raw['pod_cpu_limit'] = value
    else:
        raw['pod_memory_request'] = f'{value}G'
        raw['pod_memory_limit'] = f'{value}G'
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesResourceContractV1.from_value(raw)


@pytest.mark.parametrize(('application_port', 'resources_ports'), [
    ('0', ['0']),
    ('65536', ['65536']),
    ('08080', ['08080']),
    ('8080', []),
    ('8080', ['8081']),
    ('8080', ['8080', '8081']),
    ('8080', '8080'),
])
def test_resource_contract_rejects_invalid_or_unequal_ports(
        application_port: str, resources_ports: object) -> None:
    raw = _resources()
    raw['application_port'] = application_port
    raw['resources_ports'] = resources_ports
    with pytest.raises((TypeError, ValueError)):
        actions.ProviderKubernetesResourceContractV1.from_value(raw)


def test_resource_contract_is_closed_and_direct_construction_is_typed() -> None:
    unknown = _resources()
    unknown['unknown'] = None
    with pytest.raises(ValueError, match='unknown or missing'):
        actions.ProviderKubernetesResourceContractV1.from_value(unknown)

    direct = _resources()
    with pytest.raises(TypeError, match='invalid type'):
        actions.ProviderKubernetesResourceContractV1(
            **direct)  # type: ignore[arg-type]

    direct['image'] = actions.ProviderPodImageV1.from_value(direct['image'])
    direct['resources_ports'] = tuple(direct['resources_ports'])
    parsed = actions.ProviderKubernetesResourceContractV1(
        **direct)  # type: ignore[arg-type]
    with pytest.raises(dataclasses.FrozenInstanceError):
        parsed.source_cpus = '1'  # type: ignore[misc]
