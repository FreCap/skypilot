"""Pure artifact contracts and normalizers for SkyServe resource actions.

This module deliberately performs no filesystem, provider, database, or
dispatch I/O.  The renderer's separately inventoried artifact resolver owns
descriptor-safe reads and passes exact verified bytes to the constructors
below.
"""

from __future__ import annotations

import dataclasses
import hashlib
import ipaddress
import json
from typing import Any
import unicodedata

from sky.serve import resource_actions as actions

_MAX_ARTIFACT_BYTES = 65_536
_MAX_JSON_DEPTH = 16
_MAX_JSON_CONTAINER_MEMBERS = 256
_MAX_JSON_AGGREGATE_MEMBERS = 4_096
_MAX_JSON_TEXT_BYTES = 1_024
_MAX_SIGNED_64_BIT = 2**63 - 1
_NORMALIZATION_ARTIFACT_PATH = (
    'sky/serve/resource_action_artifacts/kubernetes_renderer_v1/'
    'admitted_object_normalization.json')


def _reject_json_constant(value: str) -> None:
    raise ValueError(f'nonfinite JSON number is forbidden: {value}')


def _reject_json_float(value: str) -> None:
    raise TypeError(f'floating-point JSON number is forbidden: {value}')


def _object_from_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    normalized_keys: set[str] = set()
    for key, value in pairs:
        normalized = unicodedata.normalize('NFC', key)
        if normalized in normalized_keys:
            raise ValueError('renderer artifact has duplicate JSON keys.')
        normalized_keys.add(normalized)
        result[key] = value
    return result


def _parse_and_validate_canonical_artifact_json(
        canonical_json: bytes) -> dict[str, Any]:
    try:
        text = canonical_json.decode('utf-8')
    except UnicodeDecodeError as e:
        raise ValueError('renderer artifact must be valid UTF-8.') from e
    try:
        value = json.loads(text,
                           object_pairs_hook=_object_from_pairs,
                           parse_constant=_reject_json_constant,
                           parse_float=_reject_json_float)
    except json.JSONDecodeError as e:
        raise ValueError(
            'renderer artifact must be valid RFC 8259 JSON.') from e
    if type(value) is not dict:
        raise TypeError('renderer artifact must have a JSON object root.')

    aggregate_members = 0
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        item_type = type(item)
        if item is None or item_type is bool:
            continue
        if item_type is int:
            if item < -_MAX_SIGNED_64_BIT - 1 or item > _MAX_SIGNED_64_BIT:
                raise ValueError('renderer artifact integers must fit signed '
                                 '64-bit.')
            continue
        if item_type is str:
            try:
                size = len(item.encode('utf-8'))
            except UnicodeEncodeError as e:
                raise ValueError(
                    'renderer artifact text must be valid UTF-8.') from e
            if size > _MAX_JSON_TEXT_BYTES:
                raise ValueError('renderer artifact text exceeds 1024 UTF-8 '
                                 'bytes.')
            if unicodedata.normalize('NFC', item) != item:
                raise ValueError(
                    'renderer artifact text must be NFC-normalized.')
            continue
        if item_type is not list and item_type is not dict:
            raise TypeError('renderer artifact contains a value outside the '
                            'JSON domain.')
        if depth > _MAX_JSON_DEPTH:
            raise ValueError('renderer artifact JSON nesting is too deep.')
        if len(item) > _MAX_JSON_CONTAINER_MEMBERS:
            raise ValueError('renderer artifact JSON container has too many '
                             'members.')
        aggregate_members += len(item)
        if aggregate_members > _MAX_JSON_AGGREGATE_MEMBERS:
            raise ValueError('renderer artifact JSON has too many aggregate '
                             'members.')
        if item_type is dict:
            children = []
            for key, child in item.items():
                if type(key) is not str or not key:
                    raise ValueError('renderer artifact JSON keys must be '
                                     'nonempty text.')
                if (len(key.encode('utf-8')) > _MAX_JSON_TEXT_BYTES or
                        unicodedata.normalize('NFC', key) != key):
                    raise ValueError('renderer artifact JSON keys must be '
                                     'bounded NFC text.')
                children.append(child)
        else:
            children = item
        stack.extend((child, depth + 1) for child in reversed(children))

    if actions.canonical_json_bytes(value) != canonical_json:
        raise ValueError(
            'renderer artifact JSON is not compact canonical JSON.')
    return value


@dataclasses.dataclass(frozen=True)
class RawCanonicalRendererArtifactBytesV1:
    """One pinned canonical JSON artifact, including its required final LF."""

    artifact_ref: actions.ProviderRepoArtifactRefV1
    raw_bytes: bytes = dataclasses.field(repr=False)

    def __post_init__(self) -> None:
        if type(self.artifact_ref) is not actions.ProviderRepoArtifactRefV1:
            raise TypeError('renderer artifact reference has an invalid type.')
        if type(self.raw_bytes) is not bytes:
            raise TypeError('renderer artifact raw bytes must be exact bytes.')
        if not self.raw_bytes or len(self.raw_bytes) > _MAX_ARTIFACT_BYTES:
            raise ValueError('renderer artifact raw bytes must be 1..65536 '
                             'bytes.')
        if len(self.raw_bytes) != self.artifact_ref.byte_size:
            raise ValueError('renderer artifact byte size does not match its '
                             'reference.')
        if hashlib.sha256(
                self.raw_bytes).hexdigest() != self.artifact_ref.sha256:
            raise ValueError('renderer artifact SHA-256 does not match its '
                             'reference.')
        if not self.raw_bytes.endswith(b'\n'):
            raise ValueError('renderer artifact must end in exactly one LF.')
        canonical_json = self.raw_bytes[:-1]
        if not canonical_json or canonical_json.endswith(b'\n'):
            raise ValueError('renderer artifact must end in exactly one LF.')
        _parse_and_validate_canonical_artifact_json(canonical_json)

    @classmethod
    def from_verified_bytes(
        cls,
        artifact_ref: actions.ProviderRepoArtifactRefV1,
        raw_bytes: bytes,
    ) -> RawCanonicalRendererArtifactBytesV1:
        return cls(artifact_ref=artifact_ref, raw_bytes=raw_bytes)

    def canonical_value(self) -> dict[str, Any]:
        return _parse_and_validate_canonical_artifact_json(self.raw_bytes[:-1])


_EXPECTED_NORMALIZATION_CONTRACT_BYTES = actions.canonical_json_bytes({
    'schema': 'skypilot.kubernetes.admitted-object-normalization.v1',
    'comparison_contract': 'kubernetes_admitted_object_v1',
    'request_schema': 'KubernetesServeThreeObjectBodySchemaV1',
    'readback_preconditions': [{
        'pointer': '/metadata/deletionTimestamp',
        'allowed': ['absent', None],
    }],
    'strip_top_level': ['status'],
    'strip_metadata': [
        'uid',
        'resourceVersion',
        'generation',
        'creationTimestamp',
        'deletionTimestamp',
        'managedFields',
    ],
    'request_allocation_rules': [{
        'sequence': 0,
        'role': 'head_ssh_service',
        'kind': 'Service',
        'present': [],
        'absent': [
            '/spec/clusterIP',
            '/spec/clusterIPs',
            '/spec/ipFamilies',
            '/spec/ipFamilyPolicy',
        ],
        'intent': 'allocate_single_stack_cluster_ip',
        'semantic_removals': [],
    }, {
        'sequence': 1,
        'role': 'head_service',
        'kind': 'Service',
        'present': [{
            'sequence': 0,
            'json_pointer': '/spec/clusterIP',
            'value': 'None',
        }],
        'absent': [
            '/spec/clusterIPs',
            '/spec/ipFamilies',
            '/spec/ipFamilyPolicy',
        ],
        'intent': 'headless_single_stack',
        'semantic_removals': ['/spec/clusterIP'],
    }, {
        'sequence': 2,
        'role': 'head_pod',
        'kind': 'Pod',
        'present': [],
        'absent': ['/spec/nodeName'],
        'intent': 'schedule_one_node',
        'semantic_removals': [],
    }],
    'admitted_parameters': [{
        'sequence': 0,
        'name': 'require_pod_node_name',
        'kind': 'keyword_only',
        'type': 'builtin_bool',
        'required': True,
        'default': 'absent',
    }],
    'admitted_allocation_rules': [{
        'sequence': 0,
        'role': 'head_ssh_service',
        'kind': 'Service',
        'cardinality': 'exactly_4',
        'parameter_cardinality': None,
        'entries': [{
            'sequence': 0,
            'json_pointer': '/spec/clusterIP',
            'allocator': 'api_server',
            'value_schema': 'canonical_ip_text',
        }, {
            'sequence': 1,
            'json_pointer': '/spec/clusterIPs',
            'allocator': 'api_server',
            'value_schema': 'singleton_cluster_ip',
        }, {
            'sequence': 2,
            'json_pointer': '/spec/ipFamilies',
            'allocator': 'api_server',
            'value_schema': 'singleton_matching_ip_family',
        }, {
            'sequence': 3,
            'json_pointer': '/spec/ipFamilyPolicy',
            'allocator': 'api_server',
            'value_schema': 'literal_SingleStack',
        }],
        'constraints': [
            'clusterIPs_0_equals_clusterIP',
            'ipFamilies_0_matches_clusterIP',
        ],
    }, {
        'sequence': 1,
        'role': 'head_service',
        'kind': 'Service',
        'cardinality': 'exactly_4',
        'parameter_cardinality': None,
        'entries': [{
            'sequence': 0,
            'json_pointer': '/spec/clusterIP',
            'allocator': 'api_server',
            'value_schema': 'literal_None',
        }, {
            'sequence': 1,
            'json_pointer': '/spec/clusterIPs',
            'allocator': 'api_server',
            'value_schema': 'singleton_literal_None',
        }, {
            'sequence': 2,
            'json_pointer': '/spec/ipFamilies',
            'allocator': 'api_server',
            'value_schema': 'singleton_IPv4_or_IPv6',
        }, {
            'sequence': 3,
            'json_pointer': '/spec/ipFamilyPolicy',
            'allocator': 'api_server',
            'value_schema': 'literal_SingleStack',
        }],
        'constraints': ['clusterIP_and_clusterIPs_0_are_None'],
    }, {
        'sequence': 2,
        'role': 'head_pod',
        'kind': 'Pod',
        'cardinality': None,
        'parameter_cardinality': {
            'parameter': 'require_pod_node_name',
            'false_value': 'zero_or_1',
            'true_value': 'exactly_1',
        },
        'entries': [{
            'sequence': 0,
            'json_pointer': '/spec/nodeName',
            'allocator': 'scheduler',
            'value_schema': 'kubernetes_dns_subdomain',
        }],
        'constraints': [
            'absent_only_in_unscheduled_partial_evidence',
            'write_once_when_present',
        ],
    }],
    'array_order': 'preserve',
    'unknown_path': 'conflict',
    'retained_defaults': 'all_explicit_in_request',
})


@dataclasses.dataclass(frozen=True, init=False)
class KubernetesAdmittedObjectNormalizationV1:
    """The one closed parsed admitted-object normalization contract."""

    _canonical: actions.CanonicalJsonObject = dataclasses.field(repr=False)

    def __init__(self, value: Any) -> None:
        canonical = actions.CanonicalJsonObject.from_value(value)
        if canonical.canonical_bytes != _EXPECTED_NORMALIZATION_CONTRACT_BYTES:
            raise ValueError('admitted-object normalization contract is not '
                             'the exact v1 document.')
        object.__setattr__(self, '_canonical', canonical)

    @classmethod
    def from_value(cls, value: Any) -> KubernetesAdmittedObjectNormalizationV1:
        return cls(value)

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical.canonical_bytes

    @property
    def sha256(self) -> str:
        return self._canonical.sha256

    def canonical_value(self) -> dict[str, Any]:
        value = self._canonical.canonical_value()
        assert type(value) is dict
        return value


@dataclasses.dataclass(frozen=True)
class ResolvedProviderKubernetesNormalizationArtifactV1:
    """Pinned reference paired with the exact parsed normalization contract."""

    artifact_ref: actions.ProviderRepoArtifactRefV1
    contract: KubernetesAdmittedObjectNormalizationV1

    def __post_init__(self) -> None:
        if type(self.artifact_ref) is not actions.ProviderRepoArtifactRefV1:
            raise TypeError('normalization artifact reference has an invalid '
                            'type.')
        if type(self.contract) is not KubernetesAdmittedObjectNormalizationV1:
            raise TypeError('normalization artifact contract has an invalid '
                            'type.')
        if self.artifact_ref.repo_path != _NORMALIZATION_ARTIFACT_PATH:
            raise ValueError('normalization artifact reference has an '
                             'unexpected repository path.')
        raw_bytes = self.contract.canonical_bytes + b'\n'
        if (self.artifact_ref.byte_size != len(raw_bytes) or
                self.artifact_ref.sha256
                != hashlib.sha256(raw_bytes).hexdigest()):
            raise ValueError('normalization artifact reference does not bind '
                             'the exact contract bytes.')

    @classmethod
    def from_verified_bytes(
        cls,
        artifact_ref: actions.ProviderRepoArtifactRefV1,
        raw_bytes: bytes,
    ) -> ResolvedProviderKubernetesNormalizationArtifactV1:
        raw = RawCanonicalRendererArtifactBytesV1.from_verified_bytes(
            artifact_ref, raw_bytes)
        return cls(artifact_ref=artifact_ref,
                   contract=KubernetesAdmittedObjectNormalizationV1.from_value(
                       raw.canonical_value()))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'artifact_ref': self.artifact_ref.canonical_value(),
            'contract': self.contract.canonical_value(),
        }


def normalize_kubernetes_request_object_v1(
    role: actions.ProviderObjectRoleV1,
    validated_request_body: actions.ValidatedKubernetesServeThreeObjectBodyV1,
    normalization_artifact: ResolvedProviderKubernetesNormalizationArtifactV1,
) -> actions.ProviderKubernetesRequestNormalizationV1:
    """Project one already-validated request body into comparison semantics."""

    if type(role) is not actions.ProviderObjectRoleV1:
        raise TypeError('request normalization role has an invalid type.')
    if type(validated_request_body
           ) is not actions.ValidatedKubernetesServeThreeObjectBodyV1:
        raise TypeError('request normalization body has an invalid type.')
    if validated_request_body.role is not role:
        raise ValueError('request normalization body role does not match.')
    if type(normalization_artifact
           ) is not ResolvedProviderKubernetesNormalizationArtifactV1:
        raise TypeError('request normalization artifact has an invalid type.')

    contract = normalization_artifact.contract
    contract_value = contract.canonical_value()
    matching_rules = [
        rule for rule in contract_value['request_allocation_rules']
        if rule['role'] == role.value
    ]
    if len(matching_rules) != 1:
        raise ValueError(
            'normalization contract has no unique request rule for '
            'role.')
    rule = matching_rules[0]
    semantic = validated_request_body.body.canonical_value()
    if semantic.get('kind') != rule['kind']:
        raise ValueError('request normalization object kind does not match.')
    spec = semantic.get('spec')
    if type(spec) is not dict:
        raise ValueError('request normalization spec must be an object.')
    for present in rule['present']:
        key = present['json_pointer'].removeprefix('/spec/')
        if key not in spec or spec[key] != present['value']:
            raise ValueError('request normalization required allocation intent '
                             'is missing or unequal.')
    for pointer in rule['absent']:
        key = pointer.removeprefix('/spec/')
        if key in spec:
            raise ValueError('request normalization allocation field must be '
                             'absent.')
    for pointer in rule['semantic_removals']:
        key = pointer.removeprefix('/spec/')
        if key not in spec:
            raise ValueError('request normalization semantic removal is '
                             'missing.')
        del spec[key]

    return actions.ProviderKubernetesRequestNormalizationV1(
        requested_semantic=actions.CanonicalJsonObject.from_value(semantic),
        requested_allocation_intent=rule['intent'])


def normalize_kubernetes_admitted_object_v1(
    role: actions.ProviderObjectRoleV1,
    admitted_object: dict[str, Any],
    normalization_artifact: ResolvedProviderKubernetesNormalizationArtifactV1,
    *,
    require_pod_node_name: bool,
) -> actions.ProviderKubernetesAdmittedNormalizationV1:
    """Separate one admitted object's semantic bytes from typed allocations."""

    if type(require_pod_node_name) is not bool:
        raise TypeError('require_pod_node_name must be a built-in bool.')
    if type(role) is not actions.ProviderObjectRoleV1:
        raise TypeError('admitted normalization role has an invalid type.')
    if type(admitted_object) is not dict:
        raise TypeError('admitted Kubernetes object must be an exact dict.')
    if type(normalization_artifact
           ) is not ResolvedProviderKubernetesNormalizationArtifactV1:
        raise TypeError('admitted normalization artifact has an invalid type.')

    contract = normalization_artifact.contract
    contract_value = contract.canonical_value()
    matching_rules = [
        rule for rule in contract_value['admitted_allocation_rules']
        if rule['role'] == role.value
    ]
    if len(matching_rules) != 1:
        raise ValueError('normalization contract has no unique admitted rule '
                         'for role.')
    rule = matching_rules[0]
    if admitted_object.get('kind') != rule['kind']:
        raise ValueError('admitted normalization object kind does not match.')
    metadata = admitted_object.get('metadata')
    spec = admitted_object.get('spec')
    if type(metadata) is not dict or type(spec) is not dict:
        raise ValueError('admitted object metadata and spec must be objects.')
    deletion_timestamp = metadata.get('deletionTimestamp', None)
    if deletion_timestamp is not None:
        raise ValueError('admitted object has a nonnull deletion timestamp.')

    semantic = dict(admitted_object)
    semantic_metadata = dict(metadata)
    semantic_spec = dict(spec)
    semantic['metadata'] = semantic_metadata
    semantic['spec'] = semantic_spec
    for key in contract_value['strip_top_level']:
        semantic.pop(key, None)
    for key in contract_value['strip_metadata']:
        semantic_metadata.pop(key, None)

    allocations: list[actions.ProviderKubernetesServerAllocationV1] = []
    for entry in rule['entries']:
        key = entry['json_pointer'].removeprefix('/spec/')
        if key not in semantic_spec:
            if (role is actions.ProviderObjectRoleV1.HEAD_POD and
                    not require_pod_node_name):
                continue
            raise ValueError('admitted object is missing a required server '
                             'allocation.')
        raw_value = semantic_spec.pop(key)
        allocations.append(
            actions.ProviderKubernetesServerAllocationV1(
                json_pointer=entry['json_pointer'],
                allocator=entry['allocator'],
                value=actions.CanonicalJsonValue.from_value(raw_value)))

    if role in (actions.ProviderObjectRoleV1.HEAD_SSH_SERVICE,
                actions.ProviderObjectRoleV1.HEAD_SERVICE):
        if len(allocations) != 4:
            raise ValueError(
                'admitted Service requires its complete allocation '
                'quartet.')
        cluster_ip = allocations[0].value.canonical_value()
        cluster_ips = allocations[1].value.canonical_value()
        ip_families = allocations[2].value.canonical_value()
        if role is actions.ProviderObjectRoleV1.HEAD_SSH_SERVICE:
            if cluster_ip == 'None':
                raise ValueError('admitted SSH Service requires a cluster IP.')
            expected_family = ('IPv4'
                               if ipaddress.ip_address(cluster_ip).version == 4
                               else 'IPv6')
            if cluster_ips != [cluster_ip] or ip_families != [expected_family]:
                raise ValueError('admitted SSH Service allocations disagree.')
        elif (cluster_ip != 'None' or cluster_ips != ['None'] or
              ip_families not in (['IPv4'], ['IPv6'])):
            raise ValueError('admitted headless Service allocations disagree.')
    elif require_pod_node_name and len(allocations) != 1:
        raise ValueError('admitted Pod requires the scheduler nodeName '
                         'allocation.')

    return actions.ProviderKubernetesAdmittedNormalizationV1(
        admitted_semantic=actions.CanonicalJsonObject.from_value(semantic),
        server_allocations=tuple(allocations))
