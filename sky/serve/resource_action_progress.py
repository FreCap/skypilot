"""Closed SkyServe provider-progress contract for the API006 journal.

This module is deliberately pure.  It parses and validates the bounded JSON
cursor owned by SkyServe, proves monotonic cursor transitions, derives the one
legal retry seed, and constructs launch supersession quiescence.  It performs
no provider, request-manager, or Serve-state I/O.
"""

from __future__ import annotations

from collections.abc import Mapping
import copy
import dataclasses
import datetime
import enum
import ipaddress
import re
from typing import Any, ClassVar, TypeAlias
import uuid

from sky.serve import resource_actions as provider_values
from sky.server.requests import requests as requests_lib
from sky.server.requests import resource_actions as kernel_actions

_MAX_OBJECT_BYTES = 65_536
_MAX_TEXT_BYTES = 1_024
_MAX_SHORT_TEXT_BYTES = 253
_MAX_POSTGRES_BIGINT = 2**63 - 1
_MAX_RESOURCE_ACTION_ATTEMPT_V1 = 2_147_483_647
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_DECIMAL_PORT_RE = re.compile(r'^[1-9][0-9]{0,4}$')
_UTC_TIMESTAMP_RE = re.compile(r'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:'
                               r'[0-9]{2}\.[0-9]{6}Z$')

_ROLE_ORDER = tuple(
    entry.role
    for entry in provider_values.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1)
_ROLE_BY_SEQUENCE = dict(enumerate(_ROLE_ORDER))
_DELETE_ORDER = tuple(
    entry.role
    for entry in sorted(provider_values.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1,
                        key=lambda entry: entry.delete_sequence))


class LaunchProgressPhaseV1(str, enum.Enum):
    """Closed launch-provider progress phases for API006."""

    CREATE_INTENT = 'CREATE_INTENT'
    OBJECTS_PARTIAL = 'OBJECTS_PARTIAL'
    OBJECTS_EXACT = 'OBJECTS_EXACT'
    HANDLE_INTENT = 'HANDLE_INTENT'
    HANDLE_COMMITTED = 'HANDLE_COMMITTED'
    RUNTIME_READY = 'RUNTIME_READY'
    JOB_INTENT = 'JOB_INTENT'
    JOB_COMMITTED = 'JOB_COMMITTED'
    JOB_RUNNING = 'JOB_RUNNING'
    ENDPOINT_RESOLVED = 'ENDPOINT_RESOLVED'
    SUCCEEDED = 'SUCCEEDED'


class DownProgressPhaseV1(str, enum.Enum):
    TARGET_RESOLVED = 'TARGET_RESOLVED'
    DELETE_INTENT = 'DELETE_INTENT'
    DELETE_PARTIAL = 'DELETE_PARTIAL'
    ABSENCE_EXACT = 'ABSENCE_EXACT'
    HANDLE_REMOVE_INTENT = 'HANDLE_REMOVE_INTENT'
    HANDLE_REMOVED = 'HANDLE_REMOVED'
    SUCCEEDED = 'SUCCEEDED'


class _CanonicalContract:
    """Encoding helpers shared by progress-specific closed DTOs."""

    def canonical_value(self) -> kernel_actions.JsonObject:
        raise NotImplementedError

    @property
    def canonical_bytes(self) -> bytes:
        encoded = kernel_actions.canonical_json_bytes(self.canonical_value())
        if len(encoded) > _MAX_OBJECT_BYTES:
            raise ValueError(
                f'{type(self).__name__} exceeds {_MAX_OBJECT_BYTES} bytes.')
        return encoded

    @property
    def sha256(self) -> str:
        # Keep direct typed construction under the same bound as parsed JSON.
        _ = self.canonical_bytes
        return kernel_actions.canonical_sha256(self.canonical_value())


def _closed_object(value: Any, *, name: str,
                   keys: frozenset[str]) -> kernel_actions.JsonObject:
    if type(value) is not dict:
        raise TypeError(f'{name} must be a JSON object.')
    # This wrapper rejects subclasses, floats, non-NFC text, cycles, excessive
    # depth/member counts, and values above the API006 65,536-byte bound.
    detached = provider_values.CanonicalJsonObject.from_value(
        value).canonical_value()
    if set(detached) != keys:
        raise ValueError(f'{name} has unknown or missing fields.')
    return detached


def _closed_list(value: Any, *, name: str, maximum: int = 256) -> list[Any]:
    if type(value) is not list:
        raise TypeError(f'{name} must be a JSON array.')
    if len(value) > maximum:
        raise ValueError(f'{name} contains too many entries.')
    # Detach before any child parser sees caller-owned mutable state.
    return copy.deepcopy(value)


def _version_one(value: Any, *, name: str) -> int:
    if type(value) is not int or value != 1:
        raise ValueError(f'{name} must be integer 1.')
    return value


def _positive_integer(value: Any, *, name: str) -> int:
    if type(value) is not int or value <= 0 or value > _MAX_POSTGRES_BIGINT:
        raise ValueError(f'{name} must be a positive signed-64-bit integer.')
    return value


def _resource_action_attempt(value: Any, *, name: str) -> int:
    attempt = _positive_integer(value, name=name)
    if attempt > _MAX_RESOURCE_ACTION_ATTEMPT_V1:
        raise ValueError(f'{name} exceeds the resource-action attempt domain.')
    return attempt


def _nonnegative_integer(value: Any, *, name: str) -> int:
    if type(value) is not int or value < 0 or value > _MAX_POSTGRES_BIGINT:
        raise ValueError(f'{name} must be a nonnegative signed-64-bit integer.')
    return value


def _text(value: Any,
          *,
          name: str,
          maximum_bytes: int = _MAX_TEXT_BYTES) -> str:
    if type(value) is not str:
        raise TypeError(f'{name} must be text.')
    size = len(value.encode('utf-8'))
    if '\x00' in value or size == 0 or size > maximum_bytes:
        raise ValueError(f'{name} must be 1..{maximum_bytes} UTF-8 bytes.')
    # CanonicalJsonObject has already checked NFC.  Keep direct child-parser
    # calls fail closed as well.
    if provider_values.CanonicalJsonValue.from_value(
            value).canonical_value() != value:
        raise ValueError(f'{name} must be canonical text.')
    return value


def _optional_text(value: Any, *, name: str) -> str | None:
    if value is None:
        return None
    return _text(value, name=name)


def _sha256(value: Any, *, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be lowercase SHA-256 hex.')
    return value


def _optional_sha256(value: Any, *, name: str) -> str | None:
    if value is None:
        return None
    return _sha256(value, name=name)


def _uuid(value: Any, *, name: str) -> uuid.UUID:
    if type(value) is not str:
        raise TypeError(f'{name} must be canonical UUID text.')
    try:
        parsed = uuid.UUID(value)
    except ValueError as e:
        raise ValueError(f'{name} must be a UUID.') from e
    if str(parsed) != value:
        raise ValueError(f'{name} must be lowercase hyphenated UUID text.')
    return parsed


def _timestamp(value: Any, *, name: str) -> str:
    if type(value) is not str or _UTC_TIMESTAMP_RE.fullmatch(value) is None:
        raise ValueError(
            f'{name} must be UTC RFC 3339 with six fractional digits.')
    try:
        parsed = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError as e:
        raise ValueError(f'{name} is not a valid UTC timestamp.') from e
    if parsed.strftime('%Y-%m-%dT%H:%M:%S.%fZ') != value:
        raise ValueError(f'{name} is not canonical.')
    return value


def _timestamp_from_datetime(value: datetime.datetime, *, name: str) -> str:
    if type(value) is not datetime.datetime:
        raise TypeError(f'{name} must be an exact datetime.')
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f'{name} must be timezone-aware.')
    return value.astimezone(
        datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def _canonical_ip(value: Any, *, name: str) -> str:
    text = _text(value, name=name, maximum_bytes=_MAX_SHORT_TEXT_BYTES)
    if not text.isascii() or '%' in text:
        raise ValueError(f'{name} must be canonical zone-free IP text.')
    try:
        address = ipaddress.ip_address(text)
    except ValueError as e:
        raise ValueError(f'{name} must be IPv4 or IPv6 text.') from e
    if str(address) != text:
        raise ValueError(f'{name} must use canonical IP spelling.')
    return text


def _decimal_port(value: Any, *, name: str) -> str:
    text = _text(value, name=name, maximum_bytes=5)
    if _DECIMAL_PORT_RE.fullmatch(text) is None or int(text) > 65_535:
        raise ValueError(f'{name} must be canonical decimal port text.')
    return text


def _same_bytes(left: _CanonicalContract, right: _CanonicalContract) -> bool:
    return left.canonical_bytes == right.canonical_bytes


ProviderResolvedTargetV1: TypeAlias = provider_values.ResolvedProviderTargetV1


class ProviderObjectReadDispositionV1(str, enum.Enum):
    PRESENT = 'present'
    NOT_FOUND = 'not_found'
    UNCERTAIN = 'uncertain'


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesObjectEvidenceV1(_CanonicalContract):
    """One exact-name CoreV1 object read in a lifecycle observation."""

    value: provider_values.CanonicalJsonObject
    role: provider_values.ProviderObjectRoleV1
    kind: provider_values.ProviderPodTopologyMutableObjectKindV1
    read_disposition: ProviderObjectReadDispositionV1
    uid: str | None
    requested_semantic_sha256: str
    normalized_observed_semantic: provider_values.CanonicalJsonObject | None
    observed_semantic_sha256: str | None
    spec_match: bool | None
    server_allocations: tuple[
        provider_values.ProviderKubernetesServerAllocationV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'role', 'api_version', 'kind', 'namespace', 'name', 'query_mode',
        'read_disposition', 'uid', 'cluster_name_label',
        'cluster_record_uuid_label', 'replica_incarnation_label',
        'requested_semantic_sha256', 'normalized_observed_semantic',
        'observed_semantic_sha256', 'spec_match', 'server_allocations',
        'deletion_timestamp', 'pod_phase', 'ready'
    })

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesObjectEvidenceV1:
        raw = _closed_object(value,
                             name='Kubernetes object observation',
                             keys=cls._KEYS)
        try:
            role = provider_values.ProviderObjectRoleV1(raw['role'])
            kind = provider_values.ProviderPodTopologyMutableObjectKindV1(
                raw['kind'])
            disposition = ProviderObjectReadDispositionV1(
                raw['read_disposition'])
        except (TypeError, ValueError) as e:
            raise ValueError('Kubernetes object observation has an unsupported '
                             'enum value.') from e
        role_index = _ROLE_ORDER.index(role)
        expected_kind = (
            provider_values.PROVIDER_KUBERNETES_OBJECT_ROLE_MAP_V1[role_index].
            kind)
        if kind is not expected_kind:
            raise ValueError('object observation role and kind do not match.')
        if raw['api_version'] != 'v1':
            raise ValueError('object observation api_version must be v1.')
        _text(raw['namespace'],
              name='object_observation.namespace',
              maximum_bytes=_MAX_SHORT_TEXT_BYTES)
        _text(raw['name'],
              name='object_observation.name',
              maximum_bytes=_MAX_SHORT_TEXT_BYTES)
        if raw['query_mode'] != 'exact_name_get_then_validate_labels':
            raise ValueError('object observation query mode is unsupported.')
        uid = _optional_text(raw['uid'], name='object_observation.uid')
        for field in ('cluster_name_label', 'replica_incarnation_label'):
            _optional_text(raw[field], name=f'object_observation.{field}')
        if raw['cluster_record_uuid_label'] is not None:
            _uuid(raw['cluster_record_uuid_label'],
                  name='object_observation.cluster_record_uuid_label')
        if raw['replica_incarnation_label'] is not None:
            _uuid(raw['replica_incarnation_label'],
                  name='object_observation.replica_incarnation_label')
        requested_hash = _sha256(
            raw['requested_semantic_sha256'],
            name='object_observation.requested_semantic_sha256')
        semantic = (None if raw['normalized_observed_semantic'] is None else
                    provider_values.CanonicalJsonObject.from_value(
                        raw['normalized_observed_semantic']))
        observed_hash = _optional_sha256(
            raw['observed_semantic_sha256'],
            name='object_observation.observed_semantic_sha256')
        if (semantic is None) != (observed_hash is None):
            raise ValueError('observed semantic bytes and hash must have equal '
                             'presence.')
        if semantic is not None and observed_hash != semantic.sha256:
            raise ValueError('observed semantic hash does not match its bytes.')
        spec_match = raw['spec_match']
        if spec_match is not None and type(spec_match) is not bool:
            raise TypeError('object observation spec_match must be Boolean or '
                            'null.')
        allocations = tuple(
            provider_values.ProviderKubernetesServerAllocationV1.from_value(
                item) for item in _closed_list(
                    raw['server_allocations'],
                    name='object observation server_allocations'))
        if raw['deletion_timestamp'] is not None:
            _timestamp(raw['deletion_timestamp'],
                       name='object_observation.deletion_timestamp')
        if raw['pod_phase'] is not None and raw['pod_phase'] not in (
                'Pending', 'Running', 'Succeeded', 'Failed', 'Unknown'):
            raise ValueError('object observation Pod phase is unsupported.')
        if raw['ready'] is not None and type(raw['ready']) is not bool:
            raise TypeError('object observation ready must be Boolean or null.')
        if kind is provider_values.ProviderPodTopologyMutableObjectKindV1.SERVICE:
            if raw['pod_phase'] is not None or raw['ready'] is not None:
                raise ValueError('Service observation cannot contain Pod '
                                 'phase/readiness.')
        if disposition is ProviderObjectReadDispositionV1.NOT_FOUND:
            response_fields = ('uid', 'cluster_name_label',
                               'cluster_record_uuid_label',
                               'replica_incarnation_label',
                               'normalized_observed_semantic',
                               'observed_semantic_sha256', 'spec_match',
                               'deletion_timestamp', 'pod_phase', 'ready')
            if (any(raw[field] is not None for field in response_fields) or
                    allocations):
                raise ValueError('not-found object observation contains '
                                 'response-derived evidence.')
        elif disposition is ProviderObjectReadDispositionV1.PRESENT:
            if (uid is None or semantic is None or observed_hash is None or
                    spec_match is None or raw['cluster_name_label'] is None or
                    raw['cluster_record_uuid_label'] is None or
                    raw['replica_incarnation_label'] is None):
                raise ValueError('present object observation requires complete '
                                 'identity and semantic evidence.')
            if spec_match and observed_hash != requested_hash:
                raise ValueError('matching object observation has unequal '
                                 'semantic hashes.')
        return cls(provider_values.CanonicalJsonObject.from_value(raw), role,
                   kind, disposition, uid, requested_hash, semantic,
                   observed_hash, spec_match, allocations)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()


@dataclasses.dataclass(frozen=True)
class ProviderPodObservationEvidenceV1(_CanonicalContract):
    """Three-role observation made through one frozen live CoreV1 client."""

    version: int
    frozen_scope: provider_values.ProviderKubernetesScopeV1
    observed_scope_before: provider_values.ProviderKubernetesScopeReadV1
    observed_scope_after: provider_values.ProviderKubernetesScopeReadV1
    objects: tuple[ProviderKubernetesObjectEvidenceV1, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'source', 'frozen_scope', 'observed_scope_before',
        'observed_scope_after', 'objects'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='Pod observation evidence version')
        if (type(self.frozen_scope)
                is not provider_values.ProviderKubernetesScopeV1 or
                type(self.observed_scope_before)
                is not provider_values.ProviderKubernetesScopeReadV1 or
                type(self.observed_scope_after)
                is not provider_values.ProviderKubernetesScopeReadV1):
            raise TypeError('Pod observation contains an invalid scope type.')
        if (type(self.objects) is not tuple or len(self.objects) != 3 or any(
                type(item) is not ProviderKubernetesObjectEvidenceV1
                for item in self.objects)):
            raise ValueError('Pod observation requires exactly three typed '
                             'object entries.')
        if tuple(item.role for item in self.objects) != _ROLE_ORDER:
            raise ValueError('Pod observation objects are not in canonical '
                             'role order.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderPodObservationEvidenceV1:
        raw = _closed_object(value,
                             name='Pod observation evidence',
                             keys=cls._KEYS)
        if raw['source'] != 'core_v1_exact_get_same_live_client':
            raise ValueError('Pod observation source is unsupported.')
        objects = _closed_list(raw['objects'], name='Pod observation objects')
        return cls(
            version=raw['version'],
            frozen_scope=provider_values.ProviderKubernetesScopeV1.from_value(
                raw['frozen_scope']),
            observed_scope_before=(
                provider_values.ProviderKubernetesScopeReadV1.from_value(
                    raw['observed_scope_before'])),
            observed_scope_after=(
                provider_values.ProviderKubernetesScopeReadV1.from_value(
                    raw['observed_scope_after'])),
            objects=tuple(
                ProviderKubernetesObjectEvidenceV1.from_value(item)
                for item in objects))

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'version': 1,
            'source': 'core_v1_exact_get_same_live_client',
            'frozen_scope': self.frozen_scope.canonical_value(),
            'observed_scope_before':
                self.observed_scope_before.canonical_value(),
            'observed_scope_after': self.observed_scope_after.canonical_value(),
            'objects': [item.canonical_value() for item in self.objects],
        }

    @property
    def scope_is_authoritative(self) -> bool:
        before = self.observed_scope_before.scope
        after = self.observed_scope_after.scope
        return (before is not None and after is not None and
                before.canonical_bytes == self.frozen_scope.canonical_bytes and
                after.canonical_bytes == self.frozen_scope.canonical_bytes)


class ProviderObservationStateV1(str, enum.Enum):
    PRESENT = 'present'
    ABSENT = 'absent'
    CONFLICT = 'conflict'
    UNCERTAIN = 'uncertain'


class ProviderObservationCertaintyV1(str, enum.Enum):
    AUTHORITATIVE = 'authoritative'
    EVENTUALLY_CONSISTENT = 'eventually_consistent'
    UNKNOWN = 'unknown'


_OBSERVATION_DERIVED_FIELDS = (
    'observed_provider_operation_id',
    'observed_provider_resource_id',
    'observed_cluster_record_uuid',
    'observed_workload_uid',
    'observed_replica_incarnation_label',
    'resolved_target',
    'ready',
)


def _require_null_observation_projection(raw: Mapping[str, Any], *,
                                         state: str) -> None:
    if any(raw[field] is not None for field in _OBSERVATION_DERIVED_FIELDS):
        raise ValueError(
            f'authoritative {state} observation contains a top-level '
            'identity/readiness projection.')


def _validate_authoritative_present_projection(
    raw: Mapping[str, Any],
    evidence: ProviderPodObservationEvidenceV1,
    resolved: ProviderResolvedTargetV1 | None,
) -> None:
    if resolved is None or any(
            item.spec_match is not True for item in evidence.objects):
        raise ValueError('authoritative present observation requires three '
                         'exact present objects and a resolved target.')
    object_values = tuple(item.canonical_value() for item in evidence.objects)
    identity_labels = tuple(
        (item['cluster_name_label'], item['cluster_record_uuid_label'],
         item['replica_incarnation_label']) for item in object_values)
    if len(set(identity_labels)) != 1:
        raise ValueError('authoritative present object identity labels differ.')
    for observed, retained in zip(evidence.objects,
                                  resolved.kubernetes_objects):
        value = observed.canonical_value()
        if (retained.role is not observed.role or
                retained.kind is not observed.kind or
                retained.namespace != value['namespace'] or
                retained.name != value['name'] or
                retained.uid != observed.uid or
                retained.observed_semantic_sha256
                != observed.observed_semantic_sha256 or
                retained.server_allocations != observed.server_allocations):
            raise ValueError('authoritative present resolved object differs '
                             'from its exact read evidence.')
    pod = evidence.objects[2]
    pod_value = object_values[2]
    cluster_uuid_label = identity_labels[0][1]
    replica_label = identity_labels[0][2]
    expected_resource_id = f"pod/{pod_value['name']}"
    if (resolved.provider_resource_id != expected_resource_id or
            raw['observed_provider_operation_id']
            != resolved.provider_operation_id or
            raw['observed_provider_resource_id']
            != resolved.provider_resource_id or
            raw['observed_cluster_record_uuid'] != cluster_uuid_label or
            raw['observed_workload_uid'] != pod.uid or
            raw['observed_replica_incarnation_label'] != replica_label or
            raw['ready'] != pod_value['ready']):
        raise ValueError('authoritative present top-level projection differs '
                         'from its exact object/resolved evidence.')


def _validate_authoritative_observation_projection(
    raw: Mapping[str, Any],
    state: ProviderObservationStateV1,
    evidence: ProviderPodObservationEvidenceV1,
    resolved: ProviderResolvedTargetV1 | None,
) -> None:
    dispositions = tuple(item.read_disposition for item in evidence.objects)
    if ProviderObjectReadDispositionV1.UNCERTAIN in dispositions:
        raise ValueError('authoritative observation contains an uncertain '
                         'object read.')
    all_present = dispositions == (ProviderObjectReadDispositionV1.PRESENT,) * 3
    all_absent = dispositions == (
        ProviderObjectReadDispositionV1.NOT_FOUND,) * 3
    if all_present:
        if state in (ProviderObservationStateV1.CONFLICT,
                     ProviderObservationStateV1.UNCERTAIN):
            if (state is ProviderObservationStateV1.UNCERTAIN and any(
                    item.spec_match is not True for item in evidence.objects)):
                raise ValueError('authoritative all-present uncertain '
                                 'observation contains a conflicting object.')
            _require_null_observation_projection(raw, state=state.value)
        elif state is ProviderObservationStateV1.PRESENT:
            _validate_authoritative_present_projection(raw, evidence, resolved)
        else:
            raise ValueError('authoritative all-present disposition vector '
                             'requires present, uncertain, or conflict state.')
        return
    if all_absent:
        if state is not ProviderObservationStateV1.ABSENT:
            raise ValueError('authoritative all-NotFound disposition vector '
                             'requires absent state.')
        _require_null_observation_projection(raw, state='absent')
        return
    if state is ProviderObservationStateV1.UNCERTAIN:
        if any(item.spec_match is not True
               for item in evidence.objects
               if item.read_disposition is
               ProviderObjectReadDispositionV1.PRESENT):
            raise ValueError('authoritative mixed uncertain observation '
                             'contains a conflicting present object.')
    elif state is not ProviderObservationStateV1.CONFLICT:
        raise ValueError('authoritative mixed present/NotFound disposition '
                         'vector requires uncertain or conflict state.')
    _require_null_observation_projection(raw, state=state.value)


@dataclasses.dataclass(frozen=True)
class ProviderLifecycleObservationV1(_CanonicalContract):
    """Progress-specific complete observation, including its hash preimage."""

    value: provider_values.CanonicalJsonObject
    state: ProviderObservationStateV1
    certainty: ProviderObservationCertaintyV1
    target_sha256: str
    resolved_target: ProviderResolvedTargetV1 | None
    evidence: ProviderPodObservationEvidenceV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'target_sha256', 'state', 'certainty',
        'observed_provider_operation_id', 'observed_provider_resource_id',
        'observed_cluster_record_uuid', 'observed_workload_uid',
        'observed_replica_incarnation_label', 'resolved_target', 'ready',
        'evidence', 'evidence_sha256', 'observed_at'
    })

    @classmethod
    def from_value(cls, value: Any) -> ProviderLifecycleObservationV1:
        raw = _closed_object(value,
                             name='provider lifecycle observation',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='observation version')
        target_hash = _sha256(raw['target_sha256'],
                              name='observation.target_sha256')
        try:
            state = ProviderObservationStateV1(raw['state'])
            certainty = ProviderObservationCertaintyV1(raw['certainty'])
        except (TypeError, ValueError) as e:
            raise ValueError(
                'observation state/certainty is unsupported.') from e
        for field in ('observed_provider_operation_id',
                      'observed_provider_resource_id', 'observed_workload_uid'):
            _optional_text(raw[field], name=f'observation.{field}')
        if raw['observed_cluster_record_uuid'] is not None:
            _uuid(raw['observed_cluster_record_uuid'],
                  name='observation.observed_cluster_record_uuid')
        if raw['observed_replica_incarnation_label'] is not None:
            _uuid(raw['observed_replica_incarnation_label'],
                  name='observation.observed_replica_incarnation_label')
        resolved = (None if raw['resolved_target'] is None else
                    ProviderResolvedTargetV1.from_value(raw['resolved_target']))
        if resolved is not None and resolved.requested_target_sha256 != target_hash:
            raise ValueError('observation resolved target hash differs from '
                             'the observation target.')
        if raw['ready'] is not None and type(raw['ready']) is not bool:
            raise TypeError('observation ready must be Boolean or null.')
        evidence = ProviderPodObservationEvidenceV1.from_value(raw['evidence'])
        evidence_hash = _sha256(raw['evidence_sha256'],
                                name='observation.evidence_sha256')
        if evidence_hash != evidence.sha256:
            raise ValueError('observation evidence hash does not match its '
                             'complete preimage.')
        _timestamp(raw['observed_at'], name='observation.observed_at')
        if certainty is ProviderObservationCertaintyV1.AUTHORITATIVE:
            if not evidence.scope_is_authoritative:
                raise ValueError('authoritative observation requires exact '
                                 'before/after frozen scope reads.')
            _validate_authoritative_observation_projection(
                raw, state, evidence, resolved)
        return cls(provider_values.CanonicalJsonObject.from_value(raw), state,
                   certainty, target_hash, resolved, evidence)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()

    def validate_requested_target(
            self, target: provider_values.ProviderLocatorV1) -> None:
        if self.target_sha256 != target.sha256:
            raise ValueError('observation does not match the frozen target.')
        if self.resolved_target is not None and (
                self.resolved_target.requested_target_sha256 != target.sha256):
            raise ValueError('resolved observation target is crossed.')

    def validate_action_context(self, context: _ActionContext) -> None:
        """Bind observation scope, object plans, and labels to one action."""

        target = context.requested_target
        self.validate_requested_target(target)
        kubernetes = target.kubernetes
        if (not target.is_authoritative_pod_locator or kubernetes is None or
                self.evidence.frozen_scope.canonical_bytes
                != kubernetes.scope.canonical_bytes):
            raise ValueError('observation does not use the frozen Kubernetes '
                             'action scope.')
        commitments = context.launch_object_commitments
        if commitments is None:
            raise ValueError('observation requires immutable object-plan '
                             'commitments; down PriorLaunchBasisV1 is absent.')
        expected_labels = (kubernetes.provider_cluster_name,
                           kubernetes.cluster_record_uuid_label,
                           kubernetes.replica_incarnation_label)
        for sequence, (observed, topology) in enumerate(
                zip(self.evidence.objects,
                    kubernetes.topology.mutable_objects)):
            value = observed.canonical_value()
            _, semantic_sha256, namespace, name = commitments[sequence]
            if (observed.role is not topology.role or
                    observed.kind is not topology.kind or
                    value['namespace'] != kubernetes.namespace or
                    value['namespace'] != namespace or
                    value['name'] != topology.name or value['name'] != name or
                    observed.requested_semantic_sha256 != semantic_sha256):
                raise ValueError('observation role object differs from its '
                                 'immutable action plan.')
            labels = (value['cluster_name_label'],
                      value['cluster_record_uuid_label'],
                      value['replica_incarnation_label'])
            if (observed.read_disposition
                    is ProviderObjectReadDispositionV1.PRESENT and
                    self.state is not ProviderObservationStateV1.CONFLICT and
                    labels != expected_labels):
                raise ValueError('present observation identity labels differ '
                                 'from the immutable action locator.')
            if (observed.read_disposition
                    is ProviderObjectReadDispositionV1.UNCERTAIN and
                    self.state is not ProviderObservationStateV1.CONFLICT and
                    any(actual is not None and actual != expected
                        for actual, expected in zip(labels, expected_labels))):
                raise ValueError('uncertain observation contains a crossed '
                                 'identity label.')
        if (self.state is ProviderObservationStateV1.PRESENT and
            (self.value.canonical_value()['observed_cluster_record_uuid']
             != str(target.sky_cluster_record_uuid) or
             self.value.canonical_value()['observed_replica_incarnation_label']
             != kubernetes.replica_incarnation_label)):
            raise ValueError('present observation top-level identity differs '
                             'from the immutable action locator.')


@dataclasses.dataclass(frozen=True)
class ProviderRuntimeArtifactMeasurementV1(_CanonicalContract):
    """One measured runtime artifact bound to its immutable binding."""

    role: provider_values.ProviderWorkloadArtifactRoleV1
    binding_sha256: str
    observed_tree_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'role', 'binding_sha256', 'observed_tree_sha256',
        'matches_expected_manifest'
    })

    def __post_init__(self) -> None:
        if type(self.role
               ) is not provider_values.ProviderWorkloadArtifactRoleV1:
            raise TypeError('runtime artifact measurement role has an invalid '
                            'type.')
        object.__setattr__(
            self, 'binding_sha256',
            _sha256(self.binding_sha256,
                    name='runtime_measurement.binding_sha256'))
        object.__setattr__(
            self, 'observed_tree_sha256',
            _sha256(self.observed_tree_sha256,
                    name='runtime_measurement.observed_tree_sha256'))

    @classmethod
    def from_value(cls, value: Any) -> ProviderRuntimeArtifactMeasurementV1:
        raw = _closed_object(value,
                             name='runtime artifact measurement',
                             keys=cls._KEYS)
        try:
            role = provider_values.ProviderWorkloadArtifactRoleV1(raw['role'])
        except (TypeError, ValueError) as e:
            raise ValueError('runtime artifact measurement role is '
                             'unsupported.') from e
        if raw['matches_expected_manifest'] is not True:
            raise ValueError('runtime artifact measurement must match its '
                             'expected manifest.')
        return cls(role, raw['binding_sha256'], raw['observed_tree_sha256'])

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'role': self.role.value,
            'binding_sha256': self.binding_sha256,
            'observed_tree_sha256': self.observed_tree_sha256,
            'matches_expected_manifest': True,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesRuntimeEvidenceV1(_CanonicalContract):
    """Exact prebooted runtime and startup-probe evidence."""

    value: provider_values.CanonicalJsonObject
    pod_uid: str
    requested_image: provider_values.ProviderOCIImageQualificationV1
    observed_runtime_image: provider_values.ProviderRuntimeImageIdentityV1
    runtime_contract_sha256: str
    artifact_measurements: tuple[ProviderRuntimeArtifactMeasurementV1, ...]
    skylet_state_store_uuid: uuid.UUID

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'pod_uid', 'container_name', 'requested_image',
        'observed_runtime_image', 'container_started',
        'startup_probe_succeeded', 'runtime_contract_sha256',
        'artifact_measurements', 'ray_health', 'skylet_health',
        'skylet_state_store_uuid', 'observed_at'
    })

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesRuntimeEvidenceV1:
        raw = _closed_object(value,
                             name='Kubernetes runtime evidence',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='runtime evidence version')
        pod_uid = _text(raw['pod_uid'], name='runtime_evidence.pod_uid')
        if raw['container_name'] != 'ray-node':
            raise ValueError('runtime evidence container must be ray-node.')
        requested = provider_values.ProviderOCIImageQualificationV1.from_value(
            raw['requested_image'])
        observed = provider_values.ProviderRuntimeImageIdentityV1.from_value(
            raw['observed_runtime_image'])
        if (observed.qualified_oci_manifest_digest
                != requested.oci_manifest_digest or
                observed.qualified_oci_config_digest
                != requested.oci_config_digest or
                observed.qualification_artifact_sha256
                != requested.qualification_artifact.sha256):
            raise ValueError('runtime evidence observed image differs from its '
                             'qualified requested image.')
        if (raw['container_started'] is not True or
                raw['startup_probe_succeeded'] is not True):
            raise ValueError('runtime evidence requires a started container '
                             'and successful startup probe.')
        runtime_hash = _sha256(raw['runtime_contract_sha256'],
                               name='runtime_evidence.runtime_contract_sha256')
        measurements = tuple(
            ProviderRuntimeArtifactMeasurementV1.from_value(item) for item in
            _closed_list(raw['artifact_measurements'],
                         name='runtime evidence artifact_measurements'))
        expected_roles = tuple(provider_values.ProviderWorkloadArtifactRoleV1)
        if tuple(item.role for item in measurements) != expected_roles:
            raise ValueError('runtime evidence measurements must contain the '
                             'exact six artifact roles in protocol order.')
        if raw['ray_health'] != 'ready' or raw['skylet_health'] != 'ready':
            raise ValueError('runtime evidence requires ready Ray and Skylet.')
        state_store_uuid = _uuid(
            raw['skylet_state_store_uuid'],
            name='runtime_evidence.skylet_state_store_uuid')
        _timestamp(raw['observed_at'], name='runtime_evidence.observed_at')
        return cls(provider_values.CanonicalJsonObject.from_value(raw), pod_uid,
                   requested, observed, runtime_hash, measurements,
                   state_store_uuid)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesEndpointEvidenceV1(_CanonicalContract):
    """Exact PodIP endpoint derived from a validated committed handle."""

    value: provider_values.CanonicalJsonObject
    pod_uid: str
    pod_ip: str
    application_port: str
    provider_config_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'pod_uid', 'pod_ip', 'application_port',
        'provider_config_sha256', 'resolution', 'observed_at'
    })

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesEndpointEvidenceV1:
        raw = _closed_object(value,
                             name='Kubernetes endpoint evidence',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='endpoint evidence version')
        pod_uid = _text(raw['pod_uid'], name='endpoint_evidence.pod_uid')
        pod_ip = _canonical_ip(raw['pod_ip'], name='endpoint_evidence.pod_ip')
        port = _decimal_port(raw['application_port'],
                             name='endpoint_evidence.application_port')
        config_hash = _sha256(raw['provider_config_sha256'],
                              name='endpoint_evidence.provider_config_sha256')
        if raw['resolution'] != 'exact_handle_podip':
            raise ValueError('endpoint evidence resolution is unsupported.')
        _timestamp(raw['observed_at'], name='endpoint_evidence.observed_at')
        return cls(provider_values.CanonicalJsonObject.from_value(raw), pod_uid,
                   pod_ip, port, config_hash)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()


def _worker_identity_without_observed_at(
    worker: provider_values.ProviderAuthorityWorkerIdentityV1,
) -> kernel_actions.JsonObject:
    value = worker.canonical_value()
    del value['observed_at']
    return value


@dataclasses.dataclass(frozen=True)
class ProviderAuthorityWorkerAttemptAttestationV1(_CanonicalContract):
    """Attempt-scoped authority-worker identity around one execution."""

    request_id: uuid.UUID
    request_execution_generation: int
    request_worker_id: str
    claimed_cursor_sha256: str | None
    before: provider_values.ProviderAuthorityWorkerIdentityV1
    after: provider_values.ProviderAuthorityWorkerIdentityV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'request_id', 'request_execution_generation', 'request_worker_id',
        'claimed_cursor_sha256', 'before', 'after'
    })

    def __post_init__(self) -> None:
        if type(self.request_id) is not uuid.UUID:
            raise TypeError(
                'worker attestation request ID has an invalid type.')
        object.__setattr__(
            self, 'request_execution_generation',
            _positive_integer(
                self.request_execution_generation,
                name='worker_attestation.request_execution_generation'))
        object.__setattr__(
            self, 'request_worker_id',
            str(
                _uuid(self.request_worker_id,
                      name='worker_attestation.request_worker_id')))
        object.__setattr__(
            self, 'claimed_cursor_sha256',
            _optional_sha256(self.claimed_cursor_sha256,
                             name='worker_attestation.claimed_cursor_sha256'))
        if type(self.before
               ) is not provider_values.ProviderAuthorityWorkerIdentityV1:
            raise TypeError('worker attestation before has an invalid type.')
        if (self.after is not None and type(self.after)
                is not provider_values.ProviderAuthorityWorkerIdentityV1):
            raise TypeError('worker attestation after has an invalid type.')
        if (self.after is not None and
                _worker_identity_without_observed_at(self.before)
                != _worker_identity_without_observed_at(self.after)):
            raise ValueError('worker attestation before/after identities '
                             'differ.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderAuthorityWorkerAttemptAttestationV1:
        raw = _closed_object(value,
                             name='authority-worker attempt attestation',
                             keys=cls._KEYS)
        return cls(
            request_id=_uuid(raw['request_id'],
                             name='worker_attestation.request_id'),
            request_execution_generation=raw['request_execution_generation'],
            request_worker_id=raw['request_worker_id'],
            claimed_cursor_sha256=raw['claimed_cursor_sha256'],
            before=provider_values.ProviderAuthorityWorkerIdentityV1.from_value(
                raw['before']),
            after=(None if raw['after'] is None else
                   provider_values.ProviderAuthorityWorkerIdentityV1.from_value(
                       raw['after'])))

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'request_id': str(self.request_id),
            'request_execution_generation': self.request_execution_generation,
            'request_worker_id': self.request_worker_id,
            'claimed_cursor_sha256': self.claimed_cursor_sha256,
            'before': self.before.canonical_value(),
            'after':
                (None if self.after is None else self.after.canonical_value()),
        }

    @property
    def origin_key(self) -> tuple[int, str]:
        return (self.request_execution_generation, self.request_worker_id)

    def validate_execution_fence(
            self, fence: kernel_actions.AttemptExecutionFence) -> None:
        if (str(self.request_id) != fence.request_id or
                self.request_execution_generation != fence.execution_generation
                or self.request_worker_id != str(fence.worker_instance_id)):
            raise ValueError('worker attestation differs from the current '
                             'request execution fence.')


@dataclasses.dataclass(frozen=True)
class ProviderLaunchEffectClaimV1(_CanonicalContract):
    """Immutable intent/evidence origin for one launch effect."""

    version: int
    launch_attempt: int
    request_id: uuid.UUID
    request_execution_generation: int
    worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1
    worker_attestation_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'launch_attempt', 'request_id',
        'request_execution_generation', 'worker_attestation',
        'worker_attestation_sha256'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='launch effect claim version')
        object.__setattr__(
            self, 'launch_attempt',
            _resource_action_attempt(self.launch_attempt,
                                     name='launch_effect_claim.launch_attempt'))
        if type(self.request_id) is not uuid.UUID:
            raise TypeError('launch effect claim request ID has invalid type.')
        object.__setattr__(
            self, 'request_execution_generation',
            _positive_integer(
                self.request_execution_generation,
                name='launch_effect_claim.request_execution_generation'))
        if type(self.worker_attestation) is not (
                ProviderAuthorityWorkerAttemptAttestationV1):
            raise TypeError('launch effect claim attestation has invalid type.')
        object.__setattr__(
            self, 'worker_attestation_sha256',
            _sha256(self.worker_attestation_sha256,
                    name='launch_effect_claim.worker_attestation_sha256'))
        if self.worker_attestation_sha256 != self.worker_attestation.sha256:
            raise ValueError('launch effect claim attestation hash does not '
                             'match its complete preimage.')
        if (self.worker_attestation.request_id != self.request_id or
                self.worker_attestation.request_execution_generation
                != self.request_execution_generation):
            raise ValueError('launch effect claim and attestation identities '
                             'differ.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchEffectClaimV1:
        raw = _closed_object(value, name='launch effect claim', keys=cls._KEYS)
        return cls(
            version=raw['version'],
            launch_attempt=raw['launch_attempt'],
            request_id=_uuid(raw['request_id'],
                             name='launch_effect_claim.request_id'),
            request_execution_generation=raw['request_execution_generation'],
            worker_attestation=(
                ProviderAuthorityWorkerAttemptAttestationV1.from_value(
                    raw['worker_attestation'])),
            worker_attestation_sha256=raw['worker_attestation_sha256'])

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'version': 1,
            'launch_attempt': self.launch_attempt,
            'request_id': str(self.request_id),
            'request_execution_generation': self.request_execution_generation,
            'worker_attestation': self.worker_attestation.canonical_value(),
            'worker_attestation_sha256': self.worker_attestation_sha256,
        }

    @property
    def origin_key(self) -> tuple[int, int]:
        return (self.launch_attempt, self.request_execution_generation)

    def validate_action(self, action_id: uuid.UUID) -> None:
        expected_request_id = kernel_actions.request_id_for_attempt(
            action_id, self.launch_attempt)
        if str(self.request_id) != expected_request_id:
            raise ValueError('launch effect claim request ID differs from its '
                             'action attempt.')


@dataclasses.dataclass(frozen=True)
class ProviderLaunchCommittedEffectEvidenceV1(_CanonicalContract):
    """One immutable C<i> committed-effect record."""

    value: provider_values.CanonicalJsonObject
    effect_sequence: int
    effect_kind: str
    role: provider_values.ProviderObjectRoleV1 | None
    intent_phase: str
    intent_origin: ProviderLaunchEffectClaimV1
    evidence_commit_origin: ProviderLaunchEffectClaimV1
    object_at_commit: provider_values.ProviderKubernetesResolvedObjectV1 | None
    intended_handle: provider_values.ProviderKubernetesHandleV1 | None
    job_at_commit: provider_values.ProviderSkyletJobEvidenceV1 | None

    _CREATE_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'evidence_kind', 'effect_sequence', 'effect_kind', 'role',
        'intent_phase', 'intent_origin', 'evidence_commit_origin',
        'commit_disposition', 'request_body_sha256',
        'requested_semantic_sha256', 'object_at_commit'
    })
    _HANDLE_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'evidence_kind', 'effect_sequence', 'effect_kind', 'role',
        'intent_phase', 'intent_origin', 'evidence_commit_origin',
        'write_disposition', 'intended_handle', 'intended_handle_sha256'
    })
    _JOB_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'evidence_kind', 'effect_sequence', 'effect_kind', 'role',
        'intent_phase', 'intent_origin', 'evidence_commit_origin',
        'commit_disposition', 'submit_request_sha256', 'job_at_commit'
    })

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchCommittedEffectEvidenceV1:
        if type(value) is not dict:
            raise TypeError('launch committed effect must be a JSON object.')
        evidence_kind = value.get('evidence_kind')
        if evidence_kind == 'core_v1_create_committed':
            keys = cls._CREATE_KEYS
        elif evidence_kind == 'cluster_record_insert_committed':
            keys = cls._HANDLE_KEYS
        elif evidence_kind == 'skylet_job_submit_committed':
            keys = cls._JOB_KEYS
        else:
            raise ValueError('launch committed evidence kind is unsupported.')
        raw = _closed_object(value, name='launch committed effect', keys=keys)
        _version_one(raw['version'], name='committed effect version')
        sequence = _nonnegative_integer(raw['effect_sequence'],
                                        name='committed_effect.effect_sequence')
        intent_origin = ProviderLaunchEffectClaimV1.from_value(
            raw['intent_origin'])
        evidence_origin = ProviderLaunchEffectClaimV1.from_value(
            raw['evidence_commit_origin'])
        if evidence_origin.origin_key < intent_origin.origin_key:
            raise ValueError('committed evidence origin precedes its intent '
                             'origin.')
        role: provider_values.ProviderObjectRoleV1 | None = None
        object_at_commit = None
        intended_handle = None
        job_at_commit = None
        if evidence_kind == 'core_v1_create_committed':
            if sequence not in (0, 1, 2):
                raise ValueError('CoreV1 committed effect sequence is invalid.')
            if raw['effect_kind'] != 'core_v1_create' or raw[
                    'intent_phase'] != LaunchProgressPhaseV1.CREATE_INTENT.value:
                raise ValueError('CoreV1 committed effect kind/phase is '
                                 'invalid.')
            try:
                role = provider_values.ProviderObjectRoleV1(raw['role'])
            except (TypeError, ValueError) as e:
                raise ValueError(
                    'CoreV1 committed effect role is invalid.') from e
            if role is not _ROLE_BY_SEQUENCE[sequence]:
                raise ValueError('CoreV1 committed effect sequence and role '
                                 'differ.')
            disposition = raw['commit_disposition']
            if disposition not in ('created', 'adopted_exact'):
                raise ValueError('CoreV1 commit disposition is unsupported.')
            _sha256(raw['request_body_sha256'],
                    name='committed_effect.request_body_sha256')
            semantic_hash = _sha256(
                raw['requested_semantic_sha256'],
                name='committed_effect.requested_semantic_sha256')
            object_at_commit = (
                provider_values.ProviderKubernetesResolvedObjectV1.from_value(
                    raw['object_at_commit']))
            if (object_at_commit.role is not role or
                    object_at_commit.observed_semantic_sha256 != semantic_hash):
                raise ValueError('CoreV1 committed object differs from its '
                                 'role or requested semantic hash.')
            _validate_mutating_disposition_origins(disposition, intent_origin,
                                                   evidence_origin)
        elif evidence_kind == 'cluster_record_insert_committed':
            if (sequence != 3 or
                    raw['effect_kind'] != 'cluster_record_insert' or
                    raw['role'] is not None or raw['intent_phase']
                    != LaunchProgressPhaseV1.HANDLE_INTENT.value):
                raise ValueError('cluster-record committed effect metadata is '
                                 'invalid.')
            disposition = raw['write_disposition']
            if disposition not in ('inserted', 'adopted_exact'):
                raise ValueError('cluster-record write disposition is '
                                 'unsupported.')
            intended_handle = provider_values.ProviderKubernetesHandleV1.from_value(
                raw['intended_handle'])
            intended_hash = _sha256(
                raw['intended_handle_sha256'],
                name='committed_effect.intended_handle_sha256')
            if intended_hash != intended_handle.sha256:
                raise ValueError('committed intended-handle hash does not '
                                 'match its complete preimage.')
            _validate_mutating_disposition_origins(disposition, intent_origin,
                                                   evidence_origin)
        else:
            if (sequence != 4 or raw['effect_kind'] != 'skylet_job_submit' or
                    raw['role'] is not None or raw['intent_phase']
                    != LaunchProgressPhaseV1.JOB_INTENT.value):
                raise ValueError('Skylet committed effect metadata is invalid.')
            disposition = raw['commit_disposition']
            if disposition not in ('submitted', 'adopted_exact'):
                raise ValueError('Skylet commit disposition is unsupported.')
            submit_hash = _sha256(raw['submit_request_sha256'],
                                  name='committed_effect.submit_request_sha256')
            job_at_commit = provider_values.ProviderSkyletJobEvidenceV1.from_value(
                raw['job_at_commit'])
            retained = job_at_commit.retained_submit_request
            if (job_at_commit.read_disposition.value != 'present' or
                    retained is None or retained.sha256 != submit_hash or
                    job_at_commit.job_id is None):
                raise ValueError('Skylet committed effect requires exact '
                                 'present fsync-committed job evidence.')
            _validate_mutating_disposition_origins(disposition, intent_origin,
                                                   evidence_origin)
        return cls(provider_values.CanonicalJsonObject.from_value(raw),
                   sequence, raw['effect_kind'], role, raw['intent_phase'],
                   intent_origin, evidence_origin, object_at_commit,
                   intended_handle, job_at_commit)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()

    def validate_action(self, action_id: uuid.UUID) -> None:
        self.intent_origin.validate_action(action_id)
        self.evidence_commit_origin.validate_action(action_id)


def _validate_mutating_disposition_origins(
        disposition: str, intent_origin: ProviderLaunchEffectClaimV1,
        evidence_origin: ProviderLaunchEffectClaimV1) -> None:
    if disposition in ('created', 'inserted', 'submitted'):
        if intent_origin.canonical_bytes != evidence_origin.canonical_bytes:
            raise ValueError('direct mutation disposition requires byte-equal '
                             'intent/evidence origins.')
    elif evidence_origin.origin_key > intent_origin.origin_key:
        # A later checkpoint may only be an exact adoption; the caller has
        # already selected adopted_exact to reach this branch.
        return
    elif evidence_origin.origin_key < intent_origin.origin_key:
        raise ValueError('adopted evidence origin precedes its intent origin.')
    elif evidence_origin.canonical_bytes != intent_origin.canonical_bytes:
        raise ValueError('same-generation adopted evidence changed its exact '
                         'execution provenance.')


def _parse_committed_effects(
    value: Any,) -> tuple[ProviderLaunchCommittedEffectEvidenceV1, ...]:
    effects = tuple(
        ProviderLaunchCommittedEffectEvidenceV1.from_value(item)
        for item in _closed_list(value, name='launch committed_effects'))
    if tuple(item.effect_sequence for item in effects) != tuple(
            range(len(effects))):
        raise ValueError('launch committed effects must be a strictly '
                         'contiguous prefix.')
    if len(effects) > 5:
        raise ValueError('launch committed effects exceed the five-effect '
                         'protocol.')
    for previous, current in zip(effects, effects[1:]):
        if current.intent_origin.origin_key < (
                previous.evidence_commit_origin.origin_key):
            raise ValueError('later effect intent origin precedes the previous '
                             'committed evidence origin.')
    return effects


def _resolved_objects_match_effects(
    objects: tuple[provider_values.ProviderKubernetesResolvedObjectV1, ...],
    effects: tuple[ProviderLaunchCommittedEffectEvidenceV1, ...],
) -> None:
    for sequence, current_object in enumerate(objects):
        evidence = effects[sequence].object_at_commit
        if evidence is None:
            raise ValueError('launch object prefix lacks committed object '
                             'evidence.')
        evidence_value = evidence.canonical_value()
        current_value = current_object.canonical_value()
        evidence_allocations = evidence_value.pop('server_allocations')
        current_allocations = current_value.pop('server_allocations')
        if evidence_value != current_value:
            raise ValueError('launch object commitment changed after its '
                             'evidence checkpoint.')
        if sequence < 2:
            if evidence_allocations != current_allocations:
                raise ValueError('Service allocation commitment changed after '
                                 'its evidence checkpoint.')
        elif not (evidence_allocations == current_allocations or
                  (evidence_allocations == [] and
                   len(current_allocations) == 1)):
            raise ValueError('Pod allocation changed outside the one '
                             'scheduler nodeName append.')


def _stable_job_evidence(
        committed: provider_values.ProviderSkyletJobEvidenceV1,
        current: provider_values.ProviderSkyletJobEvidenceV1) -> None:
    stable_fields = ('protocol', 'submission_key', 'job_contract_sha256',
                     'job_spec_sha256', 'retained_submit_request',
                     'state_store_uuid', 'job_id')
    committed_value = committed.canonical_value()
    current_value = current.canonical_value()
    if any(committed_value[field] != current_value[field]
           for field in stable_fields):
        raise ValueError('later job evidence changed immutable job identity.')
    if current.read_disposition.value != 'present':
        raise ValueError('later launch job evidence must remain present.')
    if (current.record_revision is None or committed.record_revision is None or
            current.record_revision < committed.record_revision or
            current.run_epoch is None or committed.run_epoch is None or
            current.run_epoch < committed.run_epoch):
        raise ValueError('later job evidence regressed revision or run epoch.')
    committed_state = committed_value['durable_state']
    current_state = current_value['durable_state']
    terminal_states = frozenset({'SUCCEEDED', 'FAILED', 'BLOCKED'})
    normal_order = {
        'COMMITTED_PENDING_START': 0,
        'START_INTENT': 1,
        'START_COMMITTED': 2,
        'RUNNING': 3,
    }
    if committed_state in terminal_states:
        if current_state != committed_state:
            raise ValueError('later job evidence changed a terminal durable '
                             'state.')
    elif current_state in terminal_states:
        pass
    elif current_state == 'RECOVERY_PENDING':
        if committed_state not in ('START_INTENT', 'START_COMMITTED', 'RUNNING',
                                   'RECOVERY_PENDING'):
            raise ValueError('later job evidence entered recovery from an '
                             'unsupported durable state.')
    elif committed_state == 'RECOVERY_PENDING':
        if (current_state not in ('START_INTENT', 'START_COMMITTED', 'RUNNING')
                or current.run_epoch <= committed.run_epoch):
            raise ValueError('recovered job evidence must start a strictly '
                             'newer run epoch.')
    elif committed_state in normal_order and current_state in normal_order:
        if (normal_order[current_state] < normal_order[committed_state] and
                current.run_epoch <= committed.run_epoch):
            raise ValueError('later job evidence regressed durable state '
                             'without a newer recovery epoch.')
    else:
        raise ValueError('later job evidence has an unsupported durable state '
                         'transition.')
    if ((current.run_epoch != committed.run_epoch or
         current_state != committed_state) and
            current.record_revision <= committed.record_revision):
        raise ValueError('changed job state/run epoch requires a strictly '
                         'newer record revision.')


def _validate_launch_prefix_observation(
    observation: ProviderLifecycleObservationV1,
    current_objects: tuple[provider_values.ProviderKubernetesResolvedObjectV1,
                           ...],
    effects: tuple[ProviderLaunchCommittedEffectEvidenceV1, ...],
    *,
    complete: bool,
) -> None:
    present_count = len(current_objects)
    expected_dispositions = (
        (ProviderObjectReadDispositionV1.PRESENT,) * present_count +
        (ProviderObjectReadDispositionV1.NOT_FOUND,) * (3 - present_count))
    actual_dispositions = tuple(
        item.read_disposition for item in observation.evidence.objects)
    expected_state = (ProviderObservationStateV1.PRESENT if complete else
                      ProviderObservationStateV1.ABSENT if present_count == 0
                      else ProviderObservationStateV1.UNCERTAIN)
    if (actual_dispositions != expected_dispositions or
            observation.state is not expected_state or observation.certainty
            is not ProviderObservationCertaintyV1.AUTHORITATIVE):
        raise ValueError('launch object checkpoint observation differs from '
                         'its exact committed-prefix matrix.')
    if complete:
        if (observation.resolved_target is None or
                observation.resolved_target.kubernetes_objects
                != current_objects):
            raise ValueError('OBJECTS_EXACT observation resolved target '
                             'differs from the cursor target.')
    elif observation.resolved_target is not None:
        raise ValueError('partial launch object observation has a resolved '
                         'target projection.')
    for sequence, retained in enumerate(current_objects):
        observed = observation.evidence.objects[sequence]
        observed_value = observed.canonical_value()
        effect_value = effects[sequence].canonical_value()
        if (observed.role is not retained.role or
                observed.kind is not retained.kind or
                observed_value['namespace'] != retained.namespace or
                observed_value['name'] != retained.name or
                observed.uid != retained.uid or
                observed.requested_semantic_sha256
                != effect_value['requested_semantic_sha256'] or
                observed.observed_semantic_sha256
                != retained.observed_semantic_sha256 or
                observed.server_allocations != retained.server_allocations):
            raise ValueError('launch object checkpoint present entry differs '
                             'from its current committed object.')


@dataclasses.dataclass(frozen=True)
class ProviderLaunchProgressV1(_CanonicalContract):
    """One closed launch cursor variant from the literal v1 phase table."""

    value: provider_values.CanonicalJsonObject
    phase: LaunchProgressPhaseV1
    committed_effects: tuple[ProviderLaunchCommittedEffectEvidenceV1, ...]
    role: provider_values.ProviderObjectRoleV1 | None = None
    intent_origin: ProviderLaunchEffectClaimV1 | None = None
    known_objects: provider_values.PartialResolvedProviderTargetV1 | None = None
    resolved_target: ProviderResolvedTargetV1 | None = None
    intended_handle: provider_values.ProviderKubernetesHandleV1 | None = None
    handle: provider_values.ProviderKubernetesHandleV1 | None = None
    runtime_evidence: ProviderKubernetesRuntimeEvidenceV1 | None = None
    submit_request: provider_values.ProviderSkyletSubmitRequestV1 | None = None
    job: provider_values.ProviderSkyletJobEvidenceV1 | None = None
    endpoint: ProviderKubernetesEndpointEvidenceV1 | None = None
    pre_observation: ProviderLifecycleObservationV1 | None = None
    post_observation: ProviderLifecycleObservationV1 | None = None
    success_observation: ProviderLifecycleObservationV1 | None = None

    _COMMON: ClassVar[frozenset[str]] = frozenset(
        {'version', 'action_kind', 'phase', 'committed_effects'})
    _EXTRA_KEYS: ClassVar[dict[LaunchProgressPhaseV1, frozenset[str]]] = {
        LaunchProgressPhaseV1.CREATE_INTENT: frozenset(
            {'role', 'intent_origin', 'known_objects', 'pre_observation'}),
        LaunchProgressPhaseV1.OBJECTS_PARTIAL: frozenset(
            {'known_objects', 'post_observation'}),
        LaunchProgressPhaseV1.OBJECTS_EXACT: frozenset(
            {'resolved_target', 'post_observation'}),
        LaunchProgressPhaseV1.HANDLE_INTENT: frozenset(
            {'intent_origin', 'resolved_target', 'intended_handle'}),
        LaunchProgressPhaseV1.HANDLE_COMMITTED: frozenset(
            {'resolved_target', 'handle'}),
        LaunchProgressPhaseV1.RUNTIME_READY: frozenset(
            {'resolved_target', 'handle', 'runtime_evidence'}),
        LaunchProgressPhaseV1.JOB_INTENT: frozenset({
            'intent_origin', 'resolved_target', 'handle', 'runtime_evidence',
            'submit_request'
        }),
        LaunchProgressPhaseV1.JOB_COMMITTED: frozenset(
            {'resolved_target', 'handle', 'runtime_evidence', 'job'}),
        LaunchProgressPhaseV1.JOB_RUNNING: frozenset(
            {'resolved_target', 'handle', 'runtime_evidence', 'job'}),
        LaunchProgressPhaseV1.ENDPOINT_RESOLVED: frozenset({
            'resolved_target', 'handle', 'runtime_evidence', 'job', 'endpoint'
        }),
        LaunchProgressPhaseV1.SUCCEEDED: frozenset({
            'resolved_target', 'handle', 'runtime_evidence', 'job', 'endpoint',
            'success_observation'
        }),
    }

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchProgressV1:
        if type(value) is not dict:
            raise TypeError('launch progress cursor must be a JSON object.')
        try:
            phase = LaunchProgressPhaseV1(value.get('phase'))
        except (TypeError, ValueError) as e:
            raise ValueError('launch progress phase is unsupported.') from e
        raw = _closed_object(value,
                             name='launch progress cursor',
                             keys=cls._COMMON | cls._EXTRA_KEYS[phase])
        _version_one(raw['version'], name='launch progress version')
        if raw['action_kind'] != kernel_actions.ActionKind.LAUNCH.value:
            raise ValueError('launch progress action_kind must be launch.')
        effects = _parse_committed_effects(raw['committed_effects'])

        kwargs: dict[str, Any] = {}
        if 'role' in raw:
            try:
                kwargs['role'] = provider_values.ProviderObjectRoleV1(
                    raw['role'])
            except (TypeError, ValueError) as e:
                raise ValueError('launch intent role is unsupported.') from e
        if 'intent_origin' in raw:
            kwargs['intent_origin'] = ProviderLaunchEffectClaimV1.from_value(
                raw['intent_origin'])
        if 'known_objects' in raw:
            kwargs['known_objects'] = (
                provider_values.PartialResolvedProviderTargetV1.from_value(
                    raw['known_objects']))
        if 'resolved_target' in raw:
            kwargs['resolved_target'] = ProviderResolvedTargetV1.from_value(
                raw['resolved_target'])
        if 'intended_handle' in raw:
            kwargs['intended_handle'] = (
                provider_values.ProviderKubernetesHandleV1.from_value(
                    raw['intended_handle']))
        if 'handle' in raw:
            kwargs[
                'handle'] = provider_values.ProviderKubernetesHandleV1.from_value(
                    raw['handle'])
        if 'runtime_evidence' in raw:
            kwargs['runtime_evidence'] = (
                ProviderKubernetesRuntimeEvidenceV1.from_value(
                    raw['runtime_evidence']))
        if 'submit_request' in raw:
            kwargs['submit_request'] = (
                provider_values.ProviderSkyletSubmitRequestV1.from_value(
                    raw['submit_request']))
        if 'job' in raw:
            kwargs[
                'job'] = provider_values.ProviderSkyletJobEvidenceV1.from_value(
                    raw['job'])
        if 'endpoint' in raw:
            kwargs[
                'endpoint'] = ProviderKubernetesEndpointEvidenceV1.from_value(
                    raw['endpoint'])
        for field in ('pre_observation', 'post_observation',
                      'success_observation'):
            if field in raw:
                kwargs[field] = ProviderLifecycleObservationV1.from_value(
                    raw[field])
        parsed = cls(provider_values.CanonicalJsonObject.from_value(raw), phase,
                     effects, **kwargs)
        parsed._validate_literal_phase_row()
        return parsed

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()

    @property
    def is_intent(self) -> bool:
        return self.phase in (LaunchProgressPhaseV1.CREATE_INTENT,
                              LaunchProgressPhaseV1.HANDLE_INTENT,
                              LaunchProgressPhaseV1.JOB_INTENT)

    @property
    def is_succeeded(self) -> bool:
        return self.phase is LaunchProgressPhaseV1.SUCCEEDED

    @property
    def current_intent_sequence(self) -> int | None:
        if self.phase is LaunchProgressPhaseV1.CREATE_INTENT:
            assert self.role is not None
            return _ROLE_ORDER.index(self.role)
        if self.phase is LaunchProgressPhaseV1.HANDLE_INTENT:
            return 3
        if self.phase is LaunchProgressPhaseV1.JOB_INTENT:
            return 4
        return None

    def _validate_literal_phase_row(self) -> None:
        effect_count = len(self.committed_effects)
        expected_count: int
        if self.phase is LaunchProgressPhaseV1.CREATE_INTENT:
            if self.role is None or self.intent_origin is None or self.known_objects is None:
                raise ValueError(
                    'CREATE_INTENT lacks its closed intent fields.')
            expected_count = _ROLE_ORDER.index(self.role)
            committed_slots = tuple(
                slot.object
                for slot in self.known_objects.kubernetes_objects
                if slot.object is not None)
            if len(committed_slots) != expected_count:
                raise ValueError('CREATE_INTENT role is not the first unknown '
                                 'object role.')
            if self.pre_observation is None:
                raise ValueError('CREATE_INTENT lacks its exact prefix '
                                 'pre-observation.')
            _validate_launch_prefix_observation(self.pre_observation,
                                                committed_slots,
                                                self.committed_effects,
                                                complete=False)
        elif self.phase is LaunchProgressPhaseV1.OBJECTS_PARTIAL:
            assert self.known_objects is not None
            committed_slots = tuple(
                slot.object
                for slot in self.known_objects.kubernetes_objects
                if slot.object is not None)
            expected_count = len(committed_slots)
            if expected_count not in (1, 2, 3):
                raise ValueError('OBJECTS_PARTIAL requires one to three '
                                 'committed slots.')
            if (expected_count == 3 and committed_slots[2].server_allocations):
                raise ValueError('three-slot OBJECTS_PARTIAL requires absent '
                                 'Pod nodeName allocation.')
            if self.post_observation is None:
                raise ValueError('OBJECTS_PARTIAL lacks its exact prefix '
                                 'post-observation.')
            _validate_launch_prefix_observation(self.post_observation,
                                                committed_slots,
                                                self.committed_effects,
                                                complete=False)
        elif self.phase is LaunchProgressPhaseV1.OBJECTS_EXACT:
            expected_count = 3
            assert self.resolved_target is not None
            if self.post_observation is None:
                raise ValueError('OBJECTS_EXACT lacks authoritative exact '
                                 'post-observation.')
            _validate_launch_prefix_observation(
                self.post_observation,
                self.resolved_target.kubernetes_objects,
                self.committed_effects,
                complete=True)
            assert self.post_observation.resolved_target is not None
            if (self.post_observation.resolved_target.canonical_bytes
                    != self.resolved_target.canonical_bytes):
                raise ValueError('OBJECTS_EXACT observation is not byte-equal '
                                 'to its resolved target.')
        elif self.phase is LaunchProgressPhaseV1.HANDLE_INTENT:
            expected_count = 3
        elif self.phase in (LaunchProgressPhaseV1.HANDLE_COMMITTED,
                            LaunchProgressPhaseV1.RUNTIME_READY,
                            LaunchProgressPhaseV1.JOB_INTENT):
            expected_count = 4
        else:
            expected_count = 5
        if effect_count != expected_count:
            raise ValueError(f'{self.phase.value} requires exactly '
                             f'{expected_count} committed effects.')

        if self.known_objects is not None:
            objects = tuple(slot.object
                            for slot in self.known_objects.kubernetes_objects
                            if slot.object is not None)
            _resolved_objects_match_effects(objects, self.committed_effects)
        if self.resolved_target is not None:
            _resolved_objects_match_effects(
                self.resolved_target.kubernetes_objects, self.committed_effects)

        if self.is_intent:
            assert self.intent_origin is not None
            sequence = self.current_intent_sequence
            assert sequence is not None
            if sequence != effect_count:
                raise ValueError('launch intent sequence differs from its '
                                 'committed prefix.')
            if (effect_count and self.intent_origin.origin_key < self.
                    committed_effects[-1].evidence_commit_origin.origin_key):
                raise ValueError(
                    'launch intent origin precedes prior committed '
                    'evidence.')

        if self.intended_handle is not None:
            assert self.resolved_target is not None
            _validate_handle_target(self.intended_handle, self.resolved_target)
        if self.handle is not None:
            assert self.resolved_target is not None
            _validate_handle_target(self.handle, self.resolved_target)
            handle_effect = self.committed_effects[3]
            if (handle_effect.intended_handle is None or
                    handle_effect.intended_handle.canonical_bytes
                    != self.handle.canonical_bytes):
                raise ValueError('committed cursor handle differs from C3.')
        if self.runtime_evidence is not None:
            assert self.handle is not None
            assert self.resolved_target is not None
            config = self.handle.provider_config
            if (self.runtime_evidence.pod_uid != config.pod_uid or
                    self.runtime_evidence.pod_uid
                    != self.resolved_target.workload_uid):
                raise ValueError('runtime evidence Pod UID differs from the '
                                 'resolved handle.')
        if self.job is not None:
            commit = self.committed_effects[4].job_at_commit
            assert commit is not None
            if (self.phase in (LaunchProgressPhaseV1.JOB_RUNNING,
                               LaunchProgressPhaseV1.ENDPOINT_RESOLVED,
                               LaunchProgressPhaseV1.SUCCEEDED) and
                (self.job.read_disposition.value != 'present' or
                 self.job.canonical_value()['durable_state'] != 'RUNNING')):
                raise ValueError('running and later launch phases require '
                                 'exact present RUNNING job evidence.')
            _stable_job_evidence(commit, self.job)
            if (self.runtime_evidence is not None and self.job.state_store_uuid
                    != self.runtime_evidence.skylet_state_store_uuid):
                raise ValueError('job evidence state-store UUID differs from '
                                 'runtime evidence.')
        if self.endpoint is not None:
            assert self.handle is not None
            config = self.handle.provider_config
            if (self.endpoint.pod_uid != config.pod_uid or
                    self.endpoint.pod_ip != config.pod_ip or
                    self.endpoint.application_port != config.application_port or
                    self.endpoint.provider_config_sha256
                    != self.handle.provider_config_sha256):
                raise ValueError('endpoint evidence differs from the committed '
                                 'handle provider config.')
        if self.success_observation is not None:
            assert self.resolved_target is not None
            if (self.success_observation.state
                    is not ProviderObservationStateV1.PRESENT or
                    self.success_observation.certainty
                    is not ProviderObservationCertaintyV1.AUTHORITATIVE or
                    self.success_observation.resolved_target is None or
                    self.success_observation.resolved_target.canonical_bytes
                    != self.resolved_target.canonical_bytes or
                    self.success_observation.canonical_value()['ready']
                    is not True):
                raise ValueError('launch SUCCEEDED requires authoritative '
                                 'ready present observation of the exact '
                                 'target.')

    def validate_action_context(self, context: _ActionContext) -> None:
        if context.action_kind is not kernel_actions.ActionKind.LAUNCH:
            raise ValueError('launch cursor is bound to a non-launch action.')
        for effect in self.committed_effects:
            effect.validate_action(context.action_id)
            _validate_attestation_cohort(
                effect.intent_origin.worker_attestation, context)
            _validate_attestation_cohort(
                effect.evidence_commit_origin.worker_attestation, context)
            if effect.effect_sequence < 3:
                if context.launch_object_commitments is None:
                    raise ValueError('launch object effects require the exact '
                                     'immutable capsule object plans.')
                request_body_sha256, semantic_sha256, namespace, name = (
                    context.launch_object_commitments[effect.effect_sequence])
                effect_value = effect.canonical_value()
                committed_object = effect.object_at_commit
                assert committed_object is not None
                if (effect_value['request_body_sha256'] != request_body_sha256
                        or effect_value['requested_semantic_sha256']
                        != semantic_sha256 or
                        committed_object.namespace != namespace or
                        committed_object.name != name):
                    raise ValueError('launch committed object effect differs '
                                     'from its immutable capsule object plan.')
            elif effect.effect_sequence == 4:
                job = effect.job_at_commit
                assert job is not None and job.retained_submit_request is not None
                _validate_skylet_binding(job.retained_submit_request, context)
        if self.intent_origin is not None:
            self.intent_origin.validate_action(context.action_id)
            _validate_attestation_cohort(self.intent_origin.worker_attestation,
                                         context)
        if self.submit_request is not None:
            _validate_skylet_binding(self.submit_request, context)
        if self.job is not None and self.job.retained_submit_request is not None:
            _validate_skylet_binding(self.job.retained_submit_request, context)
        for target in (self.known_objects, self.resolved_target):
            if target is not None and target.requested_target_sha256 != (
                    context.requested_target.sha256):
                raise ValueError('launch cursor target differs from the frozen '
                                 'action target.')
        for handle in (self.intended_handle, self.handle):
            if handle is not None:
                handle.validate_requested_target(context.requested_target)
                if context.launch_workspace_identity is None:
                    raise ValueError('launch handle lacks its immutable '
                                     'workspace/scope identity binding.')
                handle.validate_workspace_identity(
                    context.launch_workspace_identity)
                if handle.launched_resources_sha256 != context.resources_sha256:
                    raise ValueError('launch handle resources hash differs '
                                     'from the immutable action plan.')
                if (handle.cluster_record_uuid
                        != context.requested_target.sky_cluster_record_uuid):
                    raise ValueError('launch handle cluster UUID differs from '
                                     'the immutable action target.')
        if self.runtime_evidence is not None:
            runtime = self.runtime_evidence
            if (context.launch_image_qualification is None or
                    runtime.requested_image.canonical_bytes
                    != context.launch_image_qualification.canonical_bytes or
                    runtime.runtime_contract_sha256
                    != context.launch_runtime_contract_sha256 or
                    context.launch_artifact_bindings is None):
                raise ValueError('runtime evidence differs from the immutable '
                                 'launch runtime capsule.')
            actual_bindings = tuple((item.role.value, item.binding_sha256,
                                     item.observed_tree_sha256)
                                    for item in runtime.artifact_measurements)
            if actual_bindings != context.launch_artifact_bindings:
                raise ValueError('runtime artifact evidence differs from the '
                                 'exact six immutable bindings.')
        if (self.endpoint is not None and self.endpoint.application_port
                != context.launch_application_port):
            raise ValueError('endpoint application port differs from the '
                             'immutable launch capsule.')
        for observation in (self.pre_observation, self.post_observation,
                            self.success_observation):
            if observation is not None:
                observation.validate_action_context(context)

    def validate_successor(self, successor: ProviderLaunchProgressV1) -> None:
        if self.canonical_bytes == successor.canonical_bytes:
            return
        old_effects = self.committed_effects
        new_effects = successor.committed_effects
        if len(new_effects) < len(old_effects) or any(
                left.canonical_bytes != right.canonical_bytes
                for left, right in zip(old_effects, new_effects)):
            raise ValueError('launch committed-effect prefix changed or '
                             'regressed.')
        for field in ('resolved_target', 'handle', 'runtime_evidence',
                      'endpoint'):
            current_value = getattr(self, field)
            if current_value is None:
                continue
            successor_value = getattr(successor, field)
            if (successor_value is None or current_value.canonical_bytes
                    != successor_value.canonical_bytes):
                raise ValueError(f'launch successor changed or erased {field}.')
        if self.job is not None:
            if successor.job is None:
                raise ValueError('launch successor erased job evidence.')
            _stable_job_evidence(self.job, successor.job)
        edge = (self.phase, successor.phase)
        allowed_simple_edges = {
            (LaunchProgressPhaseV1.OBJECTS_EXACT,
             LaunchProgressPhaseV1.HANDLE_INTENT),
            (LaunchProgressPhaseV1.HANDLE_COMMITTED,
             LaunchProgressPhaseV1.RUNTIME_READY),
            (LaunchProgressPhaseV1.RUNTIME_READY,
             LaunchProgressPhaseV1.JOB_INTENT),
            (LaunchProgressPhaseV1.JOB_COMMITTED,
             LaunchProgressPhaseV1.JOB_RUNNING),
            (LaunchProgressPhaseV1.JOB_RUNNING,
             LaunchProgressPhaseV1.ENDPOINT_RESOLVED),
            (LaunchProgressPhaseV1.ENDPOINT_RESOLVED,
             LaunchProgressPhaseV1.SUCCEEDED),
            (LaunchProgressPhaseV1.JOB_COMMITTED,
             LaunchProgressPhaseV1.JOB_COMMITTED),
            (LaunchProgressPhaseV1.JOB_RUNNING,
             LaunchProgressPhaseV1.JOB_RUNNING),
            (LaunchProgressPhaseV1.ENDPOINT_RESOLVED,
             LaunchProgressPhaseV1.ENDPOINT_RESOLVED),
        }
        if self.phase is LaunchProgressPhaseV1.CREATE_INTENT:
            sequence = self.current_intent_sequence
            assert sequence is not None and self.intent_origin is not None
            valid_post = successor.phase is LaunchProgressPhaseV1.OBJECTS_PARTIAL
            if sequence == 2 and successor.phase is LaunchProgressPhaseV1.OBJECTS_EXACT:
                valid_post = True
            if (not valid_post or len(new_effects) != len(old_effects) + 1 or
                    new_effects[-1].effect_sequence != sequence or
                    new_effects[-1].intent_origin.canonical_bytes
                    != self.intent_origin.canonical_bytes):
                raise ValueError('CREATE_INTENT successor does not atomically '
                                 'append its matching committed evidence.')
            return
        if self.phase is LaunchProgressPhaseV1.OBJECTS_PARTIAL:
            assert self.known_objects is not None
            count = len(old_effects)
            if count < 3:
                if (successor.phase is not LaunchProgressPhaseV1.CREATE_INTENT
                        or successor.role is not _ROLE_BY_SEQUENCE[count] or
                        len(new_effects) != count):
                    raise ValueError('OBJECTS_PARTIAL must enter the next '
                                     'canonical create intent.')
                return
            if successor.phase is not LaunchProgressPhaseV1.OBJECTS_EXACT:
                raise ValueError('three-slot OBJECTS_PARTIAL may only advance '
                                 'to OBJECTS_EXACT.')
            return
        if self.phase is LaunchProgressPhaseV1.HANDLE_INTENT:
            if (successor.phase is not LaunchProgressPhaseV1.HANDLE_COMMITTED or
                    len(new_effects) != 4 or self.intent_origin is None or
                    new_effects[-1].intent_origin.canonical_bytes
                    != self.intent_origin.canonical_bytes):
                raise ValueError(
                    'HANDLE_INTENT successor must append exact C3.')
            assert self.intended_handle is not None
            committed_handle = new_effects[-1].intended_handle
            assert committed_handle is not None and successor.handle is not None
            if (committed_handle.canonical_bytes
                    != self.intended_handle.canonical_bytes or
                    successor.handle.canonical_bytes
                    != self.intended_handle.canonical_bytes):
                raise ValueError('HANDLE_INTENT successor changed the exact '
                                 'intended handle across C3.')
            return
        if self.phase is LaunchProgressPhaseV1.JOB_INTENT:
            if (successor.phase is not LaunchProgressPhaseV1.JOB_COMMITTED or
                    len(new_effects) != 5 or self.intent_origin is None or
                    new_effects[-1].intent_origin.canonical_bytes
                    != self.intent_origin.canonical_bytes):
                raise ValueError('JOB_INTENT successor must append exact C4.')
            assert self.submit_request is not None
            committed_job = new_effects[-1].job_at_commit
            assert committed_job is not None
            retained_request = committed_job.retained_submit_request
            if (retained_request is None or retained_request.canonical_bytes
                    != self.submit_request.canonical_bytes):
                raise ValueError('JOB_INTENT successor C4 retained request '
                                 'differs from the exact current submit '
                                 'request.')
            return
        if edge not in allowed_simple_edges:
            raise ValueError('launch progress edge is not in the literal v1 '
                             'phase graph.')


def _validate_handle_target(handle: provider_values.ProviderKubernetesHandleV1,
                            target: ProviderResolvedTargetV1) -> None:
    if handle.requested_target_sha256 != target.requested_target_sha256:
        raise ValueError('handle and resolved target hashes differ.')
    ssh_service, head_service, pod = target.kubernetes_objects
    config = handle.provider_config
    if (config.namespace != pod.namespace or config.pod_name != pod.name or
            config.pod_uid != pod.uid or
            config.head_service_uid != head_service.uid or
            config.head_ssh_service_uid != ssh_service.uid):
        raise ValueError('handle provider config differs from resolved object '
                         'identities.')
    node_allocations = pod.server_allocations
    if (not node_allocations or
            node_allocations[0].value.canonical_value() != config.node_name):
        raise ValueError('handle node name differs from resolved Pod '
                         'allocation.')


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesDeleteObjectV1(_CanonicalContract):
    """One immutable Kubernetes delete-plan object and its observed state."""

    plan_sequence: int
    role: provider_values.ProviderObjectRoleV1
    expected_uid: str | None
    state: str
    requested_semantic_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'plan_sequence', 'role', 'expected_uid', 'state',
        'requested_semantic_sha256'
    })

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesDeleteObjectV1:
        raw = _closed_object(value,
                             name='Kubernetes delete object',
                             keys=cls._KEYS)
        sequence = _nonnegative_integer(raw['plan_sequence'],
                                        name='delete_object.plan_sequence')
        if sequence not in _ROLE_BY_SEQUENCE:
            raise ValueError('delete object plan sequence is unsupported.')
        try:
            role = provider_values.ProviderObjectRoleV1(raw['role'])
        except (TypeError, ValueError) as e:
            raise ValueError('delete object role is unsupported.') from e
        if role is not _ROLE_BY_SEQUENCE[sequence]:
            raise ValueError('delete object sequence and role differ.')
        expected_uid = _optional_text(raw['expected_uid'],
                                      name='delete_object.expected_uid')
        if raw['state'] not in ('present_exact', 'absent_exact'):
            raise ValueError('delete object state is unsupported.')
        requested_hash = _sha256(raw['requested_semantic_sha256'],
                                 name='delete_object.requested_semantic_sha256')
        return cls(sequence, role, expected_uid, raw['state'], requested_hash)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'plan_sequence': self.plan_sequence,
            'role': self.role.value,
            'expected_uid': self.expected_uid,
            'state': self.state,
            'requested_semantic_sha256': self.requested_semantic_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ProviderKubernetesDeleteTargetV1(_CanonicalContract):
    """Exact Kubernetes cleanup target and monotonic absence evidence."""

    version: int
    requested_target_sha256: str
    prior_launch_basis_sha256: str
    objects: tuple[ProviderKubernetesDeleteObjectV1, ...]
    observation: ProviderLifecycleObservationV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'requested_target_sha256', 'prior_launch_basis_sha256',
        'objects', 'observation'
    })

    @classmethod
    def from_value(cls, value: Any) -> ProviderKubernetesDeleteTargetV1:
        raw = _closed_object(value,
                             name='Kubernetes delete target',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='delete target version')
        target_hash = _sha256(raw['requested_target_sha256'],
                              name='delete_target.requested_target_sha256')
        basis_hash = _sha256(raw['prior_launch_basis_sha256'],
                             name='delete_target.prior_launch_basis_sha256')
        objects = tuple(
            ProviderKubernetesDeleteObjectV1.from_value(item)
            for item in _closed_list(raw['objects'],
                                     name='delete target objects'))
        if (len(objects) != 3 or tuple(
            (item.plan_sequence, item.role) for item in objects) != tuple(
                enumerate(_ROLE_ORDER))):
            raise ValueError('delete target requires exactly three canonical '
                             'role-map objects.')
        observation = ProviderLifecycleObservationV1.from_value(
            raw['observation'])
        if observation.target_sha256 != target_hash:
            raise ValueError('delete-target observation targets another '
                             'resource.')
        if observation.certainty is not ProviderObservationCertaintyV1.AUTHORITATIVE:
            raise ValueError('delete target requires one authoritative exact '
                             'three-role observation.')
        for planned, observed in zip(objects, observation.evidence.objects):
            if (planned.role is not observed.role or planned.role
                    is not _ROLE_BY_SEQUENCE[planned.plan_sequence] or
                    planned.requested_semantic_sha256
                    != observed.requested_semantic_sha256):
                raise ValueError('delete-target role/hash differs from its '
                                 'observation entry.')
            if planned.state == 'present_exact':
                if (observed.read_disposition
                        is not ProviderObjectReadDispositionV1.PRESENT or
                        observed.spec_match is not True or
                        planned.expected_uid is None or
                        planned.expected_uid != observed.uid):
                    raise ValueError('present_exact delete target differs from '
                                     'its exact present observation entry.')
            elif (observed.read_disposition
                  is not ProviderObjectReadDispositionV1.NOT_FOUND):
                raise ValueError('absent_exact delete target requires the '
                                 'same role to be exact NotFound.')
        return cls(1, target_hash, basis_hash, objects, observation)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'version': 1,
            'requested_target_sha256': self.requested_target_sha256,
            'prior_launch_basis_sha256': self.prior_launch_basis_sha256,
            'objects': [item.canonical_value() for item in self.objects],
            'observation': self.observation.canonical_value(),
        }

    @property
    def present_roles(self) -> tuple[provider_values.ProviderObjectRoleV1, ...]:
        return tuple(
            item.role for item in self.objects if item.state == 'present_exact')

    @property
    def unknown_uid_extension_roles(
            self) -> tuple[provider_values.ProviderObjectRoleV1, ...]:
        """Return invalid present roles that still lack an exact-read UID."""

        return tuple(
            item.role
            for item in self.objects
            if item.state == 'present_exact' and item.expected_uid is None)

    @property
    def first_present_delete_role(
            self) -> provider_values.ProviderObjectRoleV1 | None:
        present = set(self.present_roles)
        return next((role for role in _DELETE_ORDER if role in present), None)

    def validate_monotonic_successor(
        self,
        successor: ProviderKubernetesDeleteTargetV1,
        deleted_role: provider_values.ProviderObjectRoleV1 | None = None
    ) -> None:
        if (self.requested_target_sha256 != successor.requested_target_sha256 or
                self.prior_launch_basis_sha256
                != successor.prior_launch_basis_sha256):
            raise ValueError('delete target immutable hashes changed.')
        changes: list[provider_values.ProviderObjectRoleV1] = []
        for old, new in zip(self.objects, successor.objects):
            if (old.plan_sequence != new.plan_sequence or
                    old.role is not new.role or
                    old.expected_uid != new.expected_uid or
                    old.requested_semantic_sha256
                    != new.requested_semantic_sha256):
                raise ValueError('delete target immutable object commitment '
                                 'changed.')
            if old.state != new.state:
                if old.state != 'present_exact' or new.state != 'absent_exact':
                    raise ValueError('delete target object state regressed.')
                changes.append(old.role)
        if deleted_role is None:
            if changes:
                raise ValueError('read-only down edge changed delete state.')
        elif changes != [deleted_role]:
            raise ValueError('delete checkpoint must mark exactly its intended '
                             'role absent.')


@dataclasses.dataclass(frozen=True)
class ProviderClusterRecordRemovalEvidenceV1(_CanonicalContract):
    """Canonical evidence for conditional provider-handle removal."""

    value: provider_values.CanonicalJsonObject
    cluster_name: str
    expected_cluster_record_uuid: uuid.UUID
    disposition: str
    removed_handle: provider_values.ProviderKubernetesHandleV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'cluster_name', 'expected_cluster_record_uuid',
        'disposition', 'removed_handle', 'removed_handle_sha256', 'observed_at'
    })

    @classmethod
    def from_value(cls, value: Any) -> ProviderClusterRecordRemovalEvidenceV1:
        raw = _closed_object(value,
                             name='cluster-record removal evidence',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='handle removal evidence version')
        cluster_name = _text(raw['cluster_name'],
                             name='handle_removal.cluster_name')
        cluster_uuid = _uuid(raw['expected_cluster_record_uuid'],
                             name='handle_removal.expected_cluster_record_uuid')
        disposition = raw['disposition']
        if disposition not in ('removed_exact', 'already_absent'):
            raise ValueError('handle removal disposition is unsupported.')
        removed = (None if raw['removed_handle'] is None else
                   provider_values.ProviderKubernetesHandleV1.from_value(
                       raw['removed_handle']))
        removed_hash = _optional_sha256(
            raw['removed_handle_sha256'],
            name='handle_removal.removed_handle_sha256')
        if (removed is None) != (removed_hash is None):
            raise ValueError(
                'removed handle and hash must have equal presence.')
        if removed is not None and removed_hash != removed.sha256:
            raise ValueError('removed handle hash does not match its preimage.')
        if disposition == 'removed_exact':
            if (removed is None or
                    removed.cluster_record_uuid != cluster_uuid or
                    removed.cluster_name != cluster_name):
                raise ValueError('removed_exact requires the expected complete '
                                 'handle.')
        elif removed is not None:
            raise ValueError('already_absent requires null removed handle.')
        _timestamp(raw['observed_at'], name='handle_removal.observed_at')
        return cls(provider_values.CanonicalJsonObject.from_value(raw),
                   cluster_name, cluster_uuid, disposition, removed)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()


@dataclasses.dataclass(frozen=True)
class ProviderDownProgressV1(_CanonicalContract):
    """One closed down cursor variant from the literal v1 phase graph."""

    value: provider_values.CanonicalJsonObject
    phase: DownProgressPhaseV1
    delete_target: ProviderKubernetesDeleteTargetV1
    role: provider_values.ProviderObjectRoleV1 | None = None
    absence_observation: ProviderLifecycleObservationV1 | None = None
    expected_handle: provider_values.ProviderKubernetesHandleV1 | None = None
    handle_removal: ProviderClusterRecordRemovalEvidenceV1 | None = None

    _COMMON: ClassVar[frozenset[str]] = frozenset(
        {'version', 'action_kind', 'phase', 'delete_target'})
    _EXTRA_KEYS: ClassVar[dict[DownProgressPhaseV1, frozenset[str]]] = {
        DownProgressPhaseV1.TARGET_RESOLVED: frozenset(),
        DownProgressPhaseV1.DELETE_INTENT: frozenset({'role'}),
        DownProgressPhaseV1.DELETE_PARTIAL: frozenset(),
        DownProgressPhaseV1.ABSENCE_EXACT: frozenset({'absence_observation'}),
        DownProgressPhaseV1.HANDLE_REMOVE_INTENT: frozenset(
            {'absence_observation', 'expected_handle'}),
        DownProgressPhaseV1.HANDLE_REMOVED: frozenset(
            {'absence_observation', 'handle_removal'}),
        DownProgressPhaseV1.SUCCEEDED: frozenset(
            {'absence_observation', 'handle_removal'}),
    }

    @classmethod
    def from_value(cls, value: Any) -> ProviderDownProgressV1:
        if type(value) is not dict:
            raise TypeError('down progress cursor must be a JSON object.')
        try:
            phase = DownProgressPhaseV1(value.get('phase'))
        except (TypeError, ValueError) as e:
            raise ValueError('down progress phase is unsupported.') from e
        raw = _closed_object(value,
                             name='down progress cursor',
                             keys=cls._COMMON | cls._EXTRA_KEYS[phase])
        _version_one(raw['version'], name='down progress version')
        if raw['action_kind'] != kernel_actions.ActionKind.DOWN.value:
            raise ValueError('down progress action_kind must be down.')
        target = ProviderKubernetesDeleteTargetV1.from_value(
            raw['delete_target'])
        role = None
        if 'role' in raw:
            try:
                role = provider_values.ProviderObjectRoleV1(raw['role'])
            except (TypeError, ValueError) as e:
                raise ValueError(
                    'down delete-intent role is unsupported.') from e
        absence = (None if 'absence_observation' not in raw else
                   ProviderLifecycleObservationV1.from_value(
                       raw['absence_observation']))
        expected_handle = (
            None if 'expected_handle' not in raw or raw['expected_handle']
            is None else provider_values.ProviderKubernetesHandleV1.from_value(
                raw['expected_handle']))
        removal = (None if 'handle_removal' not in raw else
                   ProviderClusterRecordRemovalEvidenceV1.from_value(
                       raw['handle_removal']))
        parsed = cls(provider_values.CanonicalJsonObject.from_value(raw), phase,
                     target, role, absence, expected_handle, removal)
        parsed._validate_literal_phase_row()
        return parsed

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()

    @property
    def is_intent(self) -> bool:
        return self.phase in (DownProgressPhaseV1.DELETE_INTENT,
                              DownProgressPhaseV1.HANDLE_REMOVE_INTENT)

    @property
    def is_succeeded(self) -> bool:
        return self.phase is DownProgressPhaseV1.SUCCEEDED

    def _validate_literal_phase_row(self) -> None:
        present_role = self.delete_target.first_present_delete_role
        if self.phase is DownProgressPhaseV1.DELETE_INTENT:
            if self.role is None or self.role is not present_role:
                raise ValueError('DELETE_INTENT role must be the first present '
                                 'role in literal delete order.')
        if self.phase is DownProgressPhaseV1.DELETE_PARTIAL:
            if len(self.delete_target.present_roles) == 3:
                raise ValueError('DELETE_PARTIAL requires at least one exact '
                                 'absence checkpoint.')
        if self.absence_observation is not None:
            if self.delete_target.present_roles:
                raise ValueError('absence phase retains a present delete role.')
            if (self.absence_observation.state
                    is not ProviderObservationStateV1.ABSENT or
                    self.absence_observation.certainty
                    is not ProviderObservationCertaintyV1.AUTHORITATIVE or
                    self.absence_observation.target_sha256
                    != self.delete_target.requested_target_sha256):
                raise ValueError('down absence phase requires authoritative '
                                 'absence for the exact target.')
        if self.expected_handle is not None:
            if self.expected_handle.requested_target_sha256 != (
                    self.delete_target.requested_target_sha256):
                raise ValueError('expected removal handle targets another '
                                 'resource.')
        if self.handle_removal is not None and self.expected_handle is not None:
            # HANDLE_REMOVED does not serialize expected_handle, so this check
            # is relevant only to direct typed construction.
            if (self.handle_removal.removed_handle is not None and
                    self.handle_removal.removed_handle.canonical_bytes
                    != self.expected_handle.canonical_bytes):
                raise ValueError('removed handle differs from expected handle.')

    def validate_action_context(self, context: _ActionContext) -> None:
        if context.action_kind is not kernel_actions.ActionKind.DOWN:
            raise ValueError('down cursor is bound to a non-down action.')
        if self.delete_target.requested_target_sha256 != (
                context.requested_target.sha256):
            raise ValueError('down delete target differs from the immutable '
                             'action target.')
        cleanup = context.down_cleanup_target
        if (context.down_prior_launch_basis_sha256 is not None and
                self.delete_target.prior_launch_basis_sha256
                != context.down_prior_launch_basis_sha256):
            raise ValueError('down delete target differs from its exact '
                             'PriorLaunchBasisV1/capsule binding.')
        if cleanup is not None:
            for planned, committed, observed in zip(
                    self.delete_target.objects, cleanup.objects,
                    self.delete_target.observation.evidence.objects):
                if (planned.plan_sequence != committed.sequence or
                        planned.role is not committed.role or
                        planned.requested_semantic_sha256
                        != committed.plan.requested_semantic_sha256):
                    raise ValueError(
                        'down delete object differs from its exact '
                        'cleanup object commitment.')
                if committed.committed_uid is None:
                    # An exact read may learn a UID that the partial-launch
                    # basis did not know.  Retain that delete precondition
                    # after the object becomes absent; successor validation
                    # makes the value write-once across the transition.
                    expected_uid = (observed.uid
                                    if planned.state == 'present_exact' else
                                    planned.expected_uid)
                else:
                    expected_uid = committed.committed_uid
                if planned.expected_uid != expected_uid:
                    raise ValueError(
                        'down delete UID differs from its committed or exact-read '
                        'cleanup identity.')
                if (planned.state == 'present_exact' and
                        committed.committed_uid is not None and
                        observed.server_allocations
                        != committed.committed_server_allocations):
                    raise ValueError('down present observation allocations '
                                     'differ from the cleanup commitment.')
        self.delete_target.observation.validate_action_context(context)
        if (cleanup is None and self.delete_target.unknown_uid_extension_roles):
            raise ValueError('down unknown-UID extension requires contextual '
                             'PriorLaunchBasisV1 validation.')
        if self.absence_observation is not None:
            self.absence_observation.validate_action_context(context)
        if self.expected_handle is not None:
            self.expected_handle.validate_requested_target(
                context.requested_target)
        if (cleanup is not None and
                self.phase is DownProgressPhaseV1.HANDLE_REMOVE_INTENT and
            ((self.expected_handle is None) != (cleanup.handle is None) or
             self.expected_handle is not None and cleanup.handle is not None and
             self.expected_handle.canonical_bytes
             != cleanup.handle.canonical_bytes)):
            raise ValueError('down expected handle differs from the exact '
                             'cleanup handle commitment.')
        if self.handle_removal is not None:
            if (self.handle_removal.cluster_name
                    != context.requested_target.sky_cluster_name or
                    self.handle_removal.expected_cluster_record_uuid
                    != context.requested_target.sky_cluster_record_uuid):
                raise ValueError('handle-removal evidence differs from the '
                                 'immutable action cluster identity.')
            removed = self.handle_removal.removed_handle
            if (cleanup is not None and removed is not None and
                (cleanup.handle is None or
                 removed.canonical_bytes != cleanup.handle.canonical_bytes)):
                raise ValueError('removed handle differs from the exact '
                                 'cleanup handle commitment.')

    def validate_successor(self, successor: ProviderDownProgressV1) -> None:
        if self.canonical_bytes == successor.canonical_bytes:
            return
        edge = (self.phase, successor.phase)
        if self.phase is DownProgressPhaseV1.TARGET_RESOLVED:
            expected_phase = (DownProgressPhaseV1.DELETE_INTENT
                              if self.delete_target.present_roles else
                              DownProgressPhaseV1.ABSENCE_EXACT)
            if successor.phase is not expected_phase:
                raise ValueError('TARGET_RESOLVED must enter DELETE_INTENT '
                                 'when an object is present, or advance '
                                 'directly to ABSENCE_EXACT when all objects '
                                 'are already absent.')
            self.delete_target.validate_monotonic_successor(
                successor.delete_target)
            return
        if self.phase is DownProgressPhaseV1.DELETE_INTENT:
            if successor.phase is not DownProgressPhaseV1.DELETE_PARTIAL:
                raise ValueError(
                    'DELETE_INTENT must advance to DELETE_PARTIAL.')
            assert self.role is not None
            self.delete_target.validate_monotonic_successor(
                successor.delete_target, self.role)
            return
        if self.phase is DownProgressPhaseV1.DELETE_PARTIAL:
            self.delete_target.validate_monotonic_successor(
                successor.delete_target)
            if self.delete_target.present_roles:
                if successor.phase is not DownProgressPhaseV1.DELETE_INTENT:
                    raise ValueError('DELETE_PARTIAL with a present role must '
                                     'enter its next DELETE_INTENT.')
            elif successor.phase is not DownProgressPhaseV1.ABSENCE_EXACT:
                raise ValueError('DELETE_PARTIAL with no present role must '
                                 'advance to ABSENCE_EXACT.')
            return
        simple_edges = {
            (DownProgressPhaseV1.ABSENCE_EXACT,
             DownProgressPhaseV1.HANDLE_REMOVE_INTENT),
            (DownProgressPhaseV1.HANDLE_REMOVE_INTENT,
             DownProgressPhaseV1.HANDLE_REMOVED),
            (DownProgressPhaseV1.HANDLE_REMOVED, DownProgressPhaseV1.SUCCEEDED),
        }
        if edge not in simple_edges:
            raise ValueError('down progress edge is not in the literal v1 '
                             'phase graph.')
        self.delete_target.validate_monotonic_successor(successor.delete_target)
        if (self.absence_observation is not None and
                successor.absence_observation is not None and
                self.absence_observation.canonical_bytes
                != successor.absence_observation.canonical_bytes):
            raise ValueError('down absence evidence changed after commitment.')
        if self.phase is DownProgressPhaseV1.HANDLE_REMOVE_INTENT:
            if (successor.handle_removal is None or
                    self.expected_handle is not None and
                    successor.handle_removal.removed_handle is not None and
                    successor.handle_removal.removed_handle.canonical_bytes
                    != self.expected_handle.canonical_bytes):
                raise ValueError(
                    'HANDLE_REMOVE_INTENT successor has mismatched '
                    'removal evidence.')
        if self.phase is DownProgressPhaseV1.HANDLE_REMOVED:
            assert self.handle_removal is not None
            assert successor.handle_removal is not None
            if self.handle_removal.canonical_bytes != (
                    successor.handle_removal.canonical_bytes):
                raise ValueError('handle removal evidence changed before '
                                 'SUCCEEDED.')


ProviderLifecycleCursorV1 = ProviderLaunchProgressV1 | ProviderDownProgressV1


@dataclasses.dataclass(frozen=True)
class ProviderLifecycleProgressV1(_CanonicalContract):
    """Attempt-scoped API006 envelope around one lifecycle cursor."""

    version: int
    cursor: ProviderLifecycleCursorV1
    worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'cursor', 'worker_attestation'})

    def __post_init__(self) -> None:
        _version_one(self.version, name='provider progress version')
        if type(self.cursor) not in (ProviderLaunchProgressV1,
                                     ProviderDownProgressV1):
            raise TypeError('provider progress cursor has an invalid type.')
        if (self.worker_attestation is not None and
                type(self.worker_attestation)
                is not ProviderAuthorityWorkerAttemptAttestationV1):
            raise TypeError('provider progress worker attestation has an '
                            'invalid type.')
        if (self.worker_attestation is not None and
                type(self.cursor) is ProviderLaunchProgressV1 and
                self.cursor.intent_origin is not None):
            origin = self.cursor.intent_origin.worker_attestation
            current = self.worker_attestation
            same_execution = (origin.request_id == current.request_id and
                              origin.request_execution_generation
                              == current.request_execution_generation)
            if same_execution and not _attestation_can_complete(
                    origin, current):
                raise ValueError('current launch intent origin differs from '
                                 'the attempt envelope attestation.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLifecycleProgressV1:
        raw = _closed_object(value,
                             name='provider lifecycle progress',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='provider progress version')
        cursor_value = raw['cursor']
        if type(cursor_value) is not dict:
            raise TypeError('provider progress cursor must be an object.')
        action_kind = cursor_value.get('action_kind')
        if action_kind == kernel_actions.ActionKind.LAUNCH.value:
            cursor: ProviderLifecycleCursorV1 = (
                ProviderLaunchProgressV1.from_value(cursor_value))
        elif action_kind == kernel_actions.ActionKind.DOWN.value:
            cursor = ProviderDownProgressV1.from_value(cursor_value)
        else:
            raise ValueError('provider progress cursor action kind is '
                             'unsupported.')
        attestation = (None if raw['worker_attestation'] is None else
                       ProviderAuthorityWorkerAttemptAttestationV1.from_value(
                           raw['worker_attestation']))
        return cls(1, cursor, attestation)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'version': 1,
            'cursor': self.cursor.canonical_value(),
            'worker_attestation': (None if self.worker_attestation is None else
                                   self.worker_attestation.canonical_value()),
        }

    @property
    def action_kind(self) -> kernel_actions.ActionKind:
        return (kernel_actions.ActionKind.LAUNCH if type(self.cursor)
                is ProviderLaunchProgressV1 else kernel_actions.ActionKind.DOWN)

    @property
    def is_succeeded(self) -> bool:
        return self.cursor.phase.value == 'SUCCEEDED'

    def validate_action_context(self, context: _ActionContext) -> None:
        if self.action_kind is not context.action_kind:
            raise ValueError('provider progress action kind differs from its '
                             'immutable action.')
        if self.worker_attestation is not None:
            _validate_attestation_cohort(self.worker_attestation, context)
        self.cursor.validate_action_context(context)

    def validate_successor(self,
                           successor: ProviderLifecycleProgressV1) -> None:
        if type(self.cursor) is not type(successor.cursor):
            raise ValueError('provider progress successor changed action kind.')
        if type(self.cursor) is ProviderLaunchProgressV1:
            assert type(successor.cursor) is ProviderLaunchProgressV1
            self.cursor.validate_successor(successor.cursor)
        else:
            assert type(self.cursor) is ProviderDownProgressV1
            assert type(successor.cursor) is ProviderDownProgressV1
            self.cursor.validate_successor(successor.cursor)


@dataclasses.dataclass(frozen=True)
class _ActionContext:
    """Immutable action bindings required by the pure progress reducer."""

    action_id: uuid.UUID
    action_kind: kernel_actions.ActionKind
    requested_target: provider_values.ProviderLocatorV1
    resources_sha256: str
    launch_object_commitments: tuple[tuple[str, str, str, str], ...] | None = (
        None)
    launch_skylet_binding: tuple[str, bytes, str] | None = None
    executor_cohort: provider_values.ProviderAuthorityWorkerCohortV1 | None = (
        None)
    launch_workspace_identity: provider_values.ProviderWorkspaceIdentityV1 | None = (
        None)
    launch_application_port: str | None = None
    launch_image_qualification: provider_values.ProviderOCIImageQualificationV1 | None = (
        None)
    launch_runtime_contract_sha256: str | None = None
    launch_artifact_bindings: tuple[tuple[str, str, str], ...] | None = None
    down_prior_launch_basis_sha256: str | None = None
    down_cleanup_target: provider_values.ProviderKubernetesCleanupTargetV1 | None = (
        None)

    @classmethod
    def from_record(cls, action: kernel_actions.ActionRecord) -> _ActionContext:
        if action.domain != 'serve' or action.resource_type != 'replica':
            raise ValueError('provider progress requires a Serve replica '
                             'action.')
        spec = provider_values.ServeReplicaActionSpecV1.from_value(
            action.immutable_spec)
        if (action.immutable_spec_sha256 != spec.sha256 or
                action.action_id != spec.action_id):
            raise ValueError('action immutable spec hash/identity differs from '
                             'its typed Serve spec.')
        plan = spec.provider_plan
        if (action.action_type != plan.action_kind.value or
                action.desired_generation
                != plan.resource_identity.desired_generation):
            raise ValueError('action indexed kind/generation differs from its '
                             'immutable provider plan.')
        expected_resource_identity = plan.resource_identity.action_identity(
            plan.action_kind).resource_identity
        if action.resource_identity != expected_resource_identity:
            raise ValueError('action resource identity differs from its '
                             'immutable provider plan.')
        launch_object_commitments = None
        launch_skylet_binding = None
        executor_cohort = None
        launch_workspace_identity = None
        launch_application_port = None
        launch_image_qualification = None
        launch_runtime_contract_sha256 = None
        launch_artifact_bindings = None
        down_prior_launch_basis_sha256 = None
        down_cleanup_target = None
        if plan.action_kind is kernel_actions.ActionKind.LAUNCH:
            launch = spec.invocation.require_launch()
            capsule = launch.execution_config.capsule
            launch_object_commitments = tuple(
                (item.request_body_sha256, item.requested_semantic_sha256,
                 item.namespace, item.name) for item in capsule.objects)
            job_submission = capsule.post_provision.job_submission
            launch_skylet_binding = (
                job_submission.contract.sha256,
                job_submission.run_source.canonical_bytes,
                launch.replica_id_text,
            )
            executor_cohort = capsule.executor_cohort
            launch_workspace_identity = provider_values.ProviderWorkspaceIdentityV1(
                version=1,
                workspace=capsule.config_projection.workspace,
                kubernetes_scope=capsule.scope)
            launch_application_port = capsule.resources.application_port
            launch_image_qualification = capsule.resources.image.qualification
            runtime_artifacts = capsule.post_provision.runtime_artifacts
            launch_runtime_contract_sha256 = kernel_actions.canonical_sha256(
                [item.canonical_value() for item in runtime_artifacts])
            launch_artifact_bindings = tuple(
                (item.role.value, item.sha256, item.source_manifest.sha256)
                for item in runtime_artifacts)
        else:
            down = spec.invocation.require_down()
            down_capsule = down.execution_config.capsule
            cleanup = down_capsule.cleanup_target
            launch_object_commitments = tuple(
                (item.plan.request_body_sha256,
                 item.plan.requested_semantic_sha256, item.plan.namespace,
                 item.plan.name) for item in cleanup.objects)
            executor_cohort = down_capsule.executor_cohort
            down_prior_launch_basis_sha256 = down.prior_launch_basis.sha256
            down_cleanup_target = cleanup
        return cls(action.action_id, plan.action_kind, plan.requested_target,
                   plan.resources_snapshot_sha256, launch_object_commitments,
                   launch_skylet_binding, executor_cohort,
                   launch_workspace_identity, launch_application_port,
                   launch_image_qualification, launch_runtime_contract_sha256,
                   launch_artifact_bindings, down_prior_launch_basis_sha256,
                   down_cleanup_target)


def _validate_skylet_binding(
    request: provider_values.ProviderSkyletSubmitRequestV1,
    context: _ActionContext,
) -> None:
    binding = context.launch_skylet_binding
    if binding is None:
        raise ValueError('Skylet progress requires its immutable launch '
                         'execution-capsule binding.')
    job_contract_sha256, run_source_bytes, replica_id_text = binding
    if (request.submission_key != context.action_id or
            request.job_contract_sha256 != job_contract_sha256 or
            request.job_spec.source.canonical_bytes != run_source_bytes or
            request.job_spec.replica_id != replica_id_text or
            request.job_spec.environment_replica_id != replica_id_text):
        raise ValueError('Skylet submit request differs from the immutable '
                         'launch execution capsule/action binding.')


def _validate_attestation_cohort(
    attestation: ProviderAuthorityWorkerAttemptAttestationV1,
    context: _ActionContext,
) -> None:
    if context.executor_cohort is None:
        raise ValueError('authority-worker attestation requires the frozen '
                         'execution cohort.')
    attestation.before.validate_for_cohort(context.executor_cohort)
    if attestation.after is not None:
        attestation.after.validate_for_cohort(context.executor_cohort)


def _validate_progress_attempt_binding(
    progress: ProviderLifecycleProgressV1,
    attempt: kernel_actions.AttemptRecord,
) -> None:
    """Bind attempt-scoped envelope state and all historical origins."""

    _resource_action_attempt(attempt.attempt, name='attempt.attempt')
    if (progress.worker_attestation is not None and
            str(progress.worker_attestation.request_id) != attempt.request_id):
        raise ValueError('progress worker attestation belongs to another '
                         'action attempt request.')
    cursor = progress.cursor
    if type(cursor) is not ProviderLaunchProgressV1:
        return
    claims: list[ProviderLaunchEffectClaimV1] = []
    for effect in cursor.committed_effects:
        claims.extend((effect.intent_origin, effect.evidence_commit_origin))
    if cursor.intent_origin is not None:
        claims.append(cursor.intent_origin)
    if any(claim.launch_attempt > attempt.attempt for claim in claims):
        raise ValueError('launch progress contains an origin from a future '
                         'action attempt.')


def _validate_progress_operation_ids(
    progress: ProviderLifecycleProgressV1,
    attempt: kernel_actions.AttemptRecord,
) -> None:
    """Bind every nested provider-operation projection to its journal cell."""

    expected = attempt.provider_operation_id
    targets: list[ProviderResolvedTargetV1] = []
    observations: list[ProviderLifecycleObservationV1] = []
    cursor = progress.cursor
    if type(cursor) is ProviderLaunchProgressV1:
        if cursor.resolved_target is not None:
            targets.append(cursor.resolved_target)
        observations.extend(item for item in (cursor.pre_observation,
                                              cursor.post_observation,
                                              cursor.success_observation)
                            if item is not None)
    else:
        assert type(cursor) is ProviderDownProgressV1
        observations.append(cursor.delete_target.observation)
        if cursor.absence_observation is not None:
            observations.append(cursor.absence_observation)
    for observation in observations:
        observed = observation.canonical_value(
        )['observed_provider_operation_id']
        if observed is not None and observed != expected:
            raise ValueError('nested observation provider operation ID differs '
                             'from the attempt journal.')
        if observation.resolved_target is not None:
            targets.append(observation.resolved_target)
    if any(target.provider_operation_id is not None and
           target.provider_operation_id != expected for target in targets):
        raise ValueError('nested resolved-target provider operation ID differs '
                         'from the attempt journal.')


def _is_down_pre_submission_cursor(
        progress: ProviderLifecycleProgressV1) -> bool:
    cursor = progress.cursor
    return (type(cursor) is ProviderDownProgressV1 and cursor.phase in (
        DownProgressPhaseV1.TARGET_RESOLVED,
        DownProgressPhaseV1.DELETE_INTENT,
        DownProgressPhaseV1.ABSENCE_EXACT,
        DownProgressPhaseV1.HANDLE_REMOVE_INTENT,
    ))


def _is_first_progress_cursor(progress: ProviderLifecycleProgressV1) -> bool:
    cursor = progress.cursor
    if type(cursor) is ProviderLaunchProgressV1:
        return (cursor.phase is LaunchProgressPhaseV1.CREATE_INTENT and
                cursor.role is _ROLE_ORDER[0] and not cursor.committed_effects)
    assert type(cursor) is ProviderDownProgressV1
    return cursor.phase is DownProgressPhaseV1.TARGET_RESOLVED


def _is_exact_inherited_cursor(
    progress: ProviderLifecycleProgressV1,
    predecessor: kernel_actions.AttemptRecord | None,
    attempt: kernel_actions.AttemptRecord,
    *,
    allow_bound_attestation: bool,
) -> bool:
    if (predecessor is None or attempt.attempt != predecessor.attempt + 1 or
            predecessor.mutation_boundary
            is not kernel_actions.MutationBoundary.SETTLED or
            predecessor.provider_progress is None):
        return False
    predecessor_progress = ProviderLifecycleProgressV1.from_value(
        predecessor.provider_progress)
    if predecessor_progress.is_succeeded:
        return False
    seed_value = predecessor_progress.canonical_value()
    seed_value['worker_attestation'] = None
    seed = ProviderLifecycleProgressV1.from_value(seed_value)
    if not allow_bound_attestation:
        return progress.canonical_bytes == seed.canonical_bytes
    attestation = progress.worker_attestation
    return (attestation is not None and
            progress.cursor.canonical_bytes == seed.cursor.canonical_bytes and
            attestation.claimed_cursor_sha256 == seed.cursor.sha256)


def _is_admitted_intent_committed_cursor(
    progress: ProviderLifecycleProgressV1,
    predecessor: kernel_actions.AttemptRecord | None,
    attempt: kernel_actions.AttemptRecord,
) -> bool:
    """Return whether API006 admits this cursor at the intent watermark."""

    return (_is_first_progress_cursor(progress) or
            _is_down_pre_submission_cursor(progress) or
            _is_exact_inherited_cursor(
                progress, predecessor, attempt, allow_bound_attestation=True))


class ServeProviderProgressContractV1:
    """SkyServe implementation of the generic API006 progress protocol."""

    @staticmethod
    def retry_seed(
        action: kernel_actions.ActionRecord,
        lineage_predecessor: kernel_actions.AttemptRecord | None,
        predecessor: kernel_actions.AttemptRecord,
    ) -> Mapping[str, Any] | None:
        ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, lineage_predecessor, predecessor, None)
        if (predecessor.mutation_boundary
                is not kernel_actions.MutationBoundary.SETTLED or
                predecessor.attempt >= _MAX_RESOURCE_ACTION_ATTEMPT_V1):
            raise ValueError('retry seed requires a settled predecessor below '
                             'the attempt maximum.')
        context = _ActionContext.from_record(action)
        progress = _validate_retry_authorizing_predecessor(
            action, predecessor, context)
        if progress is None:
            return None
        value = progress.canonical_value()
        value['worker_attestation'] = None
        return value

    @staticmethod
    def validate_attempt_snapshot(
        action: kernel_actions.ActionRecord,
        predecessor: kernel_actions.AttemptRecord | None,
        attempt: kernel_actions.AttemptRecord,
        execution_fence: kernel_actions.AttemptExecutionFence | None,
    ) -> None:
        del execution_fence
        context = _ActionContext.from_record(action)
        expected_request_id = kernel_actions.request_id_for_attempt(
            action.action_id, attempt.attempt)
        if (attempt.action_id != action.action_id or
                attempt.request_id != expected_request_id):
            raise ValueError('attempt identity differs from its action.')
        if (attempt.mutation_boundary is kernel_actions.MutationBoundary.SETTLED
                and type(attempt.typed_outcome) is dict and
                type(attempt.typed_outcome.get('basis')) is dict and
                attempt.typed_outcome['basis'].get('basis_kind')
                == 'request_terminal_fallback'):
            _validate_settled_fallback_outcome(action, predecessor, attempt,
                                               context)
            return
        if attempt.provider_progress is None:
            if (attempt.provider_io_boundary
                    is not kernel_actions.ProviderIOBoundary.NOT_STARTED or
                    attempt.provider_progress_sha256 is not None or
                    attempt.provider_progress_revision != 0 or
                    attempt.provider_operation_id is not None):
                raise ValueError(
                    'null progress requires the exact revision-zero '
                    'pre-I/O shape.')
            if (attempt.mutation_boundary
                    is not kernel_actions.MutationBoundary.SETTLED and
                    attempt.mutation_boundary
                    is not kernel_actions.MutationBoundary.NOT_STARTED):
                raise ValueError('active revision-zero journal has crossed '
                                 'mutation/provider boundaries.')
            if attempt.mutation_boundary is kernel_actions.MutationBoundary.SETTLED:
                _validate_settled_handler_outcome(action, predecessor, attempt,
                                                  context, None)
            return
        progress = ProviderLifecycleProgressV1.from_value(
            attempt.provider_progress)
        _validate_progress_attempt_binding(progress, attempt)
        _validate_progress_operation_ids(progress, attempt)
        progress.validate_action_context(context)
        if (attempt.provider_progress_sha256 != progress.sha256 or
                attempt.provider_progress_revision <= 0):
            raise ValueError('attempt progress hash/revision is invalid.')
        boundary = attempt.provider_io_boundary
        if boundary is kernel_actions.ProviderIOBoundary.NOT_STARTED:
            if (attempt.provider_progress_revision != 1 or
                    progress.worker_attestation is not None or
                    not _is_exact_inherited_cursor(
                        progress,
                        predecessor,
                        attempt,
                        allow_bound_attestation=False)):
                raise ValueError(
                    'inherited progress differs from the exact NOT_STARTED '
                    'revision-one seed.')
        elif boundary is kernel_actions.ProviderIOBoundary.INTENT_COMMITTED:
            if not _is_admitted_intent_committed_cursor(progress, predecessor,
                                                        attempt):
                raise ValueError('cursor is not admitted at the '
                                 'INTENT_COMMITTED watermark.')
        elif boundary is not (
                kernel_actions.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS):
            raise ValueError('provider I/O boundary is unsupported.')
        if (boundary is not kernel_actions.ProviderIOBoundary.NOT_STARTED and
                progress.worker_attestation is None):
            raise ValueError('crossed provider progress lacks worker '
                             'attestation.')
        if attempt.mutation_boundary is not kernel_actions.MutationBoundary.SETTLED:
            expected_mutation = kernel_actions.MutationBoundary(boundary.value)
            if attempt.mutation_boundary is not expected_mutation:
                raise ValueError('active attempt mutation/provider boundaries '
                                 'are crossed.')
        if attempt.mutation_boundary is kernel_actions.MutationBoundary.SETTLED:
            _validate_settled_handler_outcome(action, predecessor, attempt,
                                              context, progress)

    @staticmethod
    def validate_progress_transition(
        action: kernel_actions.ActionRecord,
        predecessor: kernel_actions.AttemptRecord | None,
        attempt: kernel_actions.AttemptRecord,
        execution_fence: kernel_actions.AttemptExecutionFence,
        proposed_progress: kernel_actions.JsonObject,
    ) -> None:
        ServeProviderProgressContractV1.validate_attempt_snapshot(
            action, predecessor, attempt, execution_fence)
        context = _ActionContext.from_record(action)
        proposed = ProviderLifecycleProgressV1.from_value(proposed_progress)
        _validate_progress_attempt_binding(proposed, attempt)
        _validate_progress_operation_ids(proposed, attempt)
        proposed.validate_action_context(context)
        if proposed.worker_attestation is None:
            raise ValueError('proposed progress requires current worker '
                             'attestation.')
        proposed.worker_attestation.validate_execution_fence(execution_fence)
        if attempt.provider_progress is None:
            if (attempt.provider_io_boundary
                    is not kernel_actions.ProviderIOBoundary.NOT_STARTED or
                    attempt.mutation_boundary
                    is not kernel_actions.MutationBoundary.NOT_STARTED):
                raise ValueError('fresh first intent requires the exact '
                                 'NOT_STARTED attempt journal.')
            if proposed.worker_attestation.claimed_cursor_sha256 is not None:
                raise ValueError('fresh progress attestation must claim a null '
                                 'cursor hash.')
            if type(proposed.cursor) is ProviderLaunchProgressV1:
                if (proposed.cursor.phase
                        is not LaunchProgressPhaseV1.CREATE_INTENT or
                        proposed.cursor.role is not _ROLE_ORDER[0]):
                    raise ValueError('fresh launch progress must start at the '
                                     'first CREATE_INTENT.')
            elif proposed.cursor.phase is not DownProgressPhaseV1.TARGET_RESOLVED:
                raise ValueError('fresh down progress must start at '
                                 'TARGET_RESOLVED.')
            _validate_current_claim_checkpoint(attempt, proposed, None)
            return

        current = ProviderLifecycleProgressV1.from_value(
            attempt.provider_progress)
        _validate_progress_attempt_binding(current, attempt)
        _validate_progress_operation_ids(current, attempt)
        current.validate_action_context(context)
        if attempt.provider_io_boundary is (
                kernel_actions.ProviderIOBoundary.NOT_STARTED):
            if not _is_exact_inherited_cursor(
                    proposed, predecessor, attempt,
                    allow_bound_attestation=True):
                raise ValueError('inherited seed may only bind the current '
                                 'attestation to its byte-equal cursor.')
            return

        down_pre_submission_edge = (
            type(current.cursor) is ProviderDownProgressV1 and
            type(proposed.cursor) is ProviderDownProgressV1 and
            not current.cursor.is_intent and
            _is_down_pre_submission_cursor(proposed))
        if (attempt.provider_io_boundary
                is kernel_actions.ProviderIOBoundary.INTENT_COMMITTED and
                current.cursor.canonical_bytes
                != proposed.cursor.canonical_bytes and
                not down_pre_submission_edge):
            raise ValueError('post-call/effect cursor requires '
                             'SUBMITTED_OR_AMBIGUOUS before progress advances.')

        current.validate_successor(proposed)
        old_attestation = current.worker_attestation
        assert old_attestation is not None
        new_attestation = proposed.worker_attestation
        if (old_attestation.request_execution_generation ==
                new_attestation.request_execution_generation):
            if (old_attestation.request_id != new_attestation.request_id or
                    old_attestation.request_worker_id
                    != new_attestation.request_worker_id or
                    old_attestation.claimed_cursor_sha256
                    != new_attestation.claimed_cursor_sha256 or
                    old_attestation.before.canonical_bytes
                    != new_attestation.before.canonical_bytes or
                (old_attestation.after is not None and
                 (new_attestation.after is None or
                  old_attestation.after.canonical_bytes
                  != new_attestation.after.canonical_bytes))):
                raise ValueError('same-generation worker attestation changed '
                                 'outside after:null completion.')
        else:
            if (new_attestation.request_execution_generation
                    <= old_attestation.request_execution_generation):
                raise ValueError('replacement execution generation must be '
                                 'strictly newer.')
            if new_attestation.claimed_cursor_sha256 != current.cursor.sha256:
                raise ValueError('replacement execution must claim the exact '
                                 'carried cursor hash.')
        _validate_current_claim_checkpoint(attempt, proposed, current)

    @staticmethod
    def validate_reduction(
        action: kernel_actions.ActionRecord,
        predecessor: kernel_actions.AttemptRecord | None,
        attempt: kernel_actions.AttemptRecord,
        reduction: kernel_actions.ActionReduction,
        context: kernel_actions.ReductionContext,
    ) -> None:
        if context.terminal_request.status is requests_lib.RequestStatus.SUCCEEDED:
            try:
                expected = reduce_handler_terminal_result_v1(
                    action, predecessor, attempt, context)
            except (TypeError, ValueError):
                expected = reduce_request_terminal_fallback_v1(
                    action, predecessor, attempt, context)
        else:
            expected = reduce_request_terminal_fallback_v1(
                action, predecessor, attempt, context)
        actual = reduction.normalized()
        expected = expected.normalized()
        if (actual.kernel_state is not expected.kernel_state or
                kernel_actions.canonical_json_bytes(actual.typed_outcome)
                != kernel_actions.canonical_json_bytes(expected.typed_outcome)
                or kernel_actions.canonical_json_bytes(actual.result)
                != kernel_actions.canonical_json_bytes(expected.result) or
                actual.retry_after_seconds != expected.retry_after_seconds or
                actual.terminal_disposition != expected.terminal_disposition):
            raise ValueError('action reduction differs from the exact '
                             'handler-result reduction.')


def _claim_for_attempt(
    attempt: kernel_actions.AttemptRecord,
    attestation: ProviderAuthorityWorkerAttemptAttestationV1
) -> ProviderLaunchEffectClaimV1:
    return ProviderLaunchEffectClaimV1(
        version=1,
        launch_attempt=attempt.attempt,
        request_id=uuid.UUID(attempt.request_id),
        request_execution_generation=attestation.request_execution_generation,
        worker_attestation=attestation,
        worker_attestation_sha256=attestation.sha256)


def _validate_current_claim_checkpoint(
        attempt: kernel_actions.AttemptRecord,
        proposed: ProviderLifecycleProgressV1,
        current: ProviderLifecycleProgressV1 | None) -> None:
    if type(proposed.cursor) is not ProviderLaunchProgressV1:
        return
    attestation = proposed.worker_attestation
    assert attestation is not None
    claim = _claim_for_attempt(attempt, attestation)
    cursor = proposed.cursor
    if cursor.intent_origin is not None:
        retained_origin = None
        if (current is not None and
                type(current.cursor) is ProviderLaunchProgressV1 and
                current.cursor.canonical_bytes == cursor.canonical_bytes):
            retained_origin = current.cursor.intent_origin
        if (retained_origin is None and
                cursor.intent_origin.canonical_bytes != claim.canonical_bytes):
            raise ValueError(
                'new launch intent origin differs from the current '
                'execution claim.')
    old_count = (0 if current is None or
                 type(current.cursor) is not ProviderLaunchProgressV1 else len(
                     current.cursor.committed_effects))
    if len(cursor.committed_effects) > old_count:
        if len(cursor.committed_effects) != old_count + 1:
            raise ValueError('one progress commit may append only one effect.')
        appended = cursor.committed_effects[-1]
        if appended.evidence_commit_origin.canonical_bytes != claim.canonical_bytes:
            raise ValueError('committed effect evidence origin differs from '
                             'the current execution claim.')


@dataclasses.dataclass(frozen=True)
class ProviderLaunchEffectDefinitiveNoEffectV1(_CanonicalContract):
    """One of the three effect-specific definitive no-effect proofs."""

    value: provider_values.CanonicalJsonObject
    proof_kind: str

    _CORE_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'proof_kind', 'request_body_sha256', 'response_status',
        'post_observation'
    })
    _CLUSTER_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'proof_kind', 'intended_handle_sha256', 'transaction_result',
        'cluster_name', 'expected_cluster_record_uuid', 'post_read_disposition',
        'observed_cluster_record_uuid', 'observed_handle', 'observed_at'
    })
    _SKYLET_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'proof_kind', 'submit_request_sha256', 'rejection',
        'post_job', 'pending_start_outbox', 'active_run_token'
    })

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchEffectDefinitiveNoEffectV1:
        if type(value) is not dict:
            raise TypeError('definitive no-effect proof must be an object.')
        proof_kind = value.get('proof_kind')
        if proof_kind == 'core_v1_422_no_create':
            raw = _closed_object(value,
                                 name='CoreV1 no-create proof',
                                 keys=cls._CORE_KEYS)
            _version_one(raw['version'], name='no-effect proof version')
            _sha256(raw['request_body_sha256'],
                    name='no_effect.request_body_sha256')
            if raw['response_status'] != 422 or type(
                    raw['response_status']) is not int:
                raise ValueError('CoreV1 no-create proof requires status 422.')
            ProviderLifecycleObservationV1.from_value(raw['post_observation'])
        elif proof_kind == 'cluster_record_no_commit':
            raw = _closed_object(value,
                                 name='cluster-record no-commit proof',
                                 keys=cls._CLUSTER_KEYS)
            _version_one(raw['version'], name='no-effect proof version')
            _sha256(raw['intended_handle_sha256'],
                    name='no_effect.intended_handle_sha256')
            _text(raw['cluster_name'], name='no_effect.cluster_name')
            expected_uuid = _uuid(raw['expected_cluster_record_uuid'],
                                  name='no_effect.expected_cluster_record_uuid')
            observed_uuid = (None if raw['observed_cluster_record_uuid'] is None
                             else _uuid(raw['observed_cluster_record_uuid'],
                                        name=('no_effect.'
                                              'observed_cluster_record_uuid')))
            observed_handle = (
                None if raw['observed_handle'] is None else
                provider_values.ProviderKubernetesHandleV1.from_value(
                    raw['observed_handle']))
            matrix = (raw['transaction_result'], raw['post_read_disposition'])
            if matrix == ('rolled_back', 'not_found'):
                if observed_uuid is not None or observed_handle is not None:
                    raise ValueError('rolled-back no-commit proof requires '
                                     'exact NotFound.')
            elif matrix == ('conflict_no_write', 'different_identity_conflict'):
                if (observed_uuid is None or observed_uuid == expected_uuid or
                        observed_handle is None or
                        observed_handle.cluster_record_uuid != observed_uuid or
                        observed_handle.cluster_name != raw['cluster_name']):
                    raise ValueError('cluster conflict proof requires a '
                                     'different-UUID exact handle.')
            else:
                raise ValueError('cluster-record no-commit proof row is not in '
                                 'the literal matrix.')
            _timestamp(raw['observed_at'], name='no_effect.observed_at')
        elif proof_kind == 'skylet_rejected_before_job_commit':
            raw = _closed_object(value,
                                 name='Skylet no-commit proof',
                                 keys=cls._SKYLET_KEYS)
            _version_one(raw['version'], name='no-effect proof version')
            _sha256(raw['submit_request_sha256'],
                    name='no_effect.submit_request_sha256')
            if raw['rejection'] not in ('same_key_different_spec',
                                        'schema_rejected'):
                raise ValueError('Skylet rejection is unsupported.')
            post_job = provider_values.ProviderSkyletJobEvidenceV1.from_value(
                raw['post_job'])
            if (post_job.job_contract_sha256 is None or
                    post_job.job_spec_sha256 is None):
                raise ValueError('Skylet rejection lacks expected job hashes.')
            if raw['pending_start_outbox'] is not False or raw[
                    'active_run_token'] is not False:
                raise ValueError('Skylet no-effect proof requires no pending '
                                 'outbox or active run token.')
            if raw['rejection'] == 'schema_rejected':
                if (post_job.read_disposition.value != 'not_found' or
                        post_job.retained_submit_request is not None):
                    raise ValueError('schema rejection requires exact job '
                                     'NotFound evidence.')
            else:
                if (post_job.read_disposition.value != 'conflict' or
                        post_job.retained_submit_request is None or
                        post_job.durable_state is None or
                        post_job.durable_state.value
                        not in ('SUCCEEDED', 'FAILED', 'BLOCKED')):
                    raise ValueError('same-key conflict requires a terminal '
                                     'byte-different retained job.')
        else:
            raise ValueError('definitive no-effect proof kind is unsupported.')
        return cls(provider_values.CanonicalJsonObject.from_value(raw),
                   proof_kind)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()


_CORE_V1_422_DISPOSITION_VECTORS = (
    (ProviderObjectReadDispositionV1.NOT_FOUND,) * 3,
    (ProviderObjectReadDispositionV1.PRESENT,
     ProviderObjectReadDispositionV1.NOT_FOUND,
     ProviderObjectReadDispositionV1.NOT_FOUND),
    (ProviderObjectReadDispositionV1.PRESENT,
     ProviderObjectReadDispositionV1.PRESENT,
     ProviderObjectReadDispositionV1.NOT_FOUND),
)


def _validate_core_v1_422_no_create_observation(
    failing_sequence: int,
    cursor: ProviderLaunchProgressV1,
    observation: ProviderLifecycleObservationV1,
) -> None:
    expected_dispositions = _CORE_V1_422_DISPOSITION_VECTORS[failing_sequence]
    actual_dispositions = tuple(
        item.read_disposition for item in observation.evidence.objects)
    expected_state = (ProviderObservationStateV1.ABSENT if failing_sequence == 0
                      else ProviderObservationStateV1.UNCERTAIN)
    if (actual_dispositions != expected_dispositions or
            observation.state is not expected_state or observation.certainty
            is not ProviderObservationCertaintyV1.AUTHORITATIVE):
        raise ValueError('CoreV1 422 no-create proof differs from its literal '
                         'failing-sequence observation matrix.')
    for sequence in range(failing_sequence):
        committed = cursor.committed_effects[sequence]
        retained = committed.object_at_commit
        assert retained is not None
        observed = observation.evidence.objects[sequence]
        observed_value = observed.canonical_value()
        committed_value = committed.canonical_value()
        if (observed.role is not retained.role or
                observed.kind is not retained.kind or
                observed_value['namespace'] != retained.namespace or
                observed_value['name'] != retained.name or
                observed.uid != retained.uid or
                observed.requested_semantic_sha256
                != committed_value['requested_semantic_sha256'] or
                observed.observed_semantic_sha256
                != retained.observed_semantic_sha256 or
                observed.server_allocations != retained.server_allocations):
            raise ValueError('CoreV1 422 prior present observation differs '
                             'from its exact committed launch evidence.')


@dataclasses.dataclass(frozen=True)
class ProviderLaunchNoEffectResolutionV1(_CanonicalContract):
    """Original-claim N<i> input retained in the handler terminal result."""

    value: provider_values.CanonicalJsonObject
    effect_sequence: int
    effect_kind: str
    role: provider_values.ProviderObjectRoleV1 | None
    intent_phase: str
    intent_cursor_sha256: str
    intent_origin: ProviderLaunchEffectClaimV1
    resolution_origin: ProviderLaunchEffectClaimV1
    resolution: str
    evidence_sha256: str | None
    definitive_no_effect: ProviderLaunchEffectDefinitiveNoEffectV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'effect_sequence', 'effect_kind', 'role', 'intent_phase',
        'intent_cursor_sha256', 'intent_origin', 'resolution_origin',
        'resolution', 'evidence_sha256', 'definitive_no_effect'
    })

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchNoEffectResolutionV1:
        raw = _closed_object(value,
                             name='launch no-effect resolution',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='no-effect resolution version')
        sequence = _nonnegative_integer(raw['effect_sequence'],
                                        name='no_effect.effect_sequence')
        if sequence > 4:
            raise ValueError('no-effect sequence exceeds launch effect table.')
        expected_kind = (
            'core_v1_create' if sequence < 3 else
            'cluster_record_insert' if sequence == 3 else 'skylet_job_submit')
        expected_phase = ('CREATE_INTENT' if sequence < 3 else
                          'HANDLE_INTENT' if sequence == 3 else 'JOB_INTENT')
        expected_role = _ROLE_BY_SEQUENCE.get(sequence)
        role = None
        if raw['role'] is not None:
            try:
                role = provider_values.ProviderObjectRoleV1(raw['role'])
            except (TypeError, ValueError) as e:
                raise ValueError('no-effect role is unsupported.') from e
        if (raw['effect_kind'] != expected_kind or
                raw['intent_phase'] != expected_phase or
                role is not expected_role):
            raise ValueError('no-effect sequence/kind/role/phase metadata '
                             'differs from the literal effect table.')
        cursor_hash = _sha256(raw['intent_cursor_sha256'],
                              name='no_effect.intent_cursor_sha256')
        intent_origin = ProviderLaunchEffectClaimV1.from_value(
            raw['intent_origin'])
        resolution_origin = ProviderLaunchEffectClaimV1.from_value(
            raw['resolution_origin'])
        if intent_origin.canonical_bytes != resolution_origin.canonical_bytes:
            raise ValueError('no-effect resolution must retain the byte-equal '
                             'original effect claim.')
        resolution = raw['resolution']
        if resolution not in ('definitive_no_effect', 'call_not_entered'):
            raise ValueError('no-effect resolution is unsupported.')
        evidence_hash = _optional_sha256(raw['evidence_sha256'],
                                         name='no_effect.evidence_sha256')
        proof = (None if raw['definitive_no_effect'] is None else
                 ProviderLaunchEffectDefinitiveNoEffectV1.from_value(
                     raw['definitive_no_effect']))
        if resolution == 'call_not_entered':
            if evidence_hash is not None or proof is not None:
                raise ValueError('call_not_entered requires null proof/hash.')
        else:
            if proof is None or evidence_hash != proof.sha256:
                raise ValueError('definitive no-effect proof hash does not '
                                 'match its complete preimage.')
            expected_proofs = ({'core_v1_422_no_create'} if sequence < 3 else
                               {'cluster_record_no_commit'} if sequence == 3
                               else {'skylet_rejected_before_job_commit'})
            if proof.proof_kind not in expected_proofs:
                raise ValueError('no-effect proof kind differs from its effect '
                                 'sequence.')
        return cls(provider_values.CanonicalJsonObject.from_value(raw),
                   sequence, expected_kind, role, expected_phase, cursor_hash,
                   intent_origin, resolution_origin, resolution, evidence_hash,
                   proof)

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()

    def validate_cursor(self,
                        cursor: ProviderLaunchProgressV1,
                        action_id: uuid.UUID,
                        attempt: int,
                        context: _ActionContext | None = None) -> None:
        if (not cursor.is_intent or
                cursor.current_intent_sequence != self.effect_sequence or
                cursor.intent_origin is None or
                cursor.intent_origin.canonical_bytes
                != self.intent_origin.canonical_bytes or
                cursor.sha256 != self.intent_cursor_sha256):
            raise ValueError('no-effect resolution does not match the exact '
                             'current intent cursor.')
        self.intent_origin.validate_action(action_id)
        self.resolution_origin.validate_action(action_id)
        if context is not None:
            _validate_attestation_cohort(self.intent_origin.worker_attestation,
                                         context)
            _validate_attestation_cohort(
                self.resolution_origin.worker_attestation, context)
        if self.resolution_origin.launch_attempt != attempt:
            raise ValueError(
                'a retry-local handler cannot resolve an inherited '
                'intent from another attempt.')
        if self.definitive_no_effect is None:
            return
        proof = self.definitive_no_effect.canonical_value()
        if self.effect_sequence < 3:
            if context is None or context.launch_object_commitments is None:
                raise ValueError('CoreV1 no-effect proof requires the exact '
                                 'immutable capsule object plan.')
            expected_request_hash = context.launch_object_commitments[
                self.effect_sequence][0]
            if proof['request_body_sha256'] != expected_request_hash:
                raise ValueError('CoreV1 no-effect request hash differs from '
                                 'the immutable capsule object plan.')
            observation = ProviderLifecycleObservationV1.from_value(
                proof['post_observation'])
            observation.validate_action_context(context)
            _validate_core_v1_422_no_create_observation(self.effect_sequence,
                                                        cursor, observation)
        elif self.effect_sequence == 3:
            intended = cursor.intended_handle
            assert intended is not None
            if (proof['intended_handle_sha256'] != intended.sha256 or
                    proof['cluster_name'] != intended.cluster_name or
                    proof['expected_cluster_record_uuid'] != str(
                        intended.cluster_record_uuid)):
                raise ValueError('cluster-record no-effect proof differs from '
                                 'the exact current intended handle.')
        else:
            submit = cursor.submit_request
            assert submit is not None
            if proof['submit_request_sha256'] != submit.sha256:
                raise ValueError('Skylet no-effect proof differs from the '
                                 'exact current submit request.')
            post_job = provider_values.ProviderSkyletJobEvidenceV1.from_value(
                proof['post_job'])
            runtime = cursor.runtime_evidence
            assert runtime is not None
            if (post_job.submission_key != submit.submission_key or
                    post_job.submission_key != action_id or
                    post_job.job_contract_sha256 != submit.job_contract_sha256
                    or post_job.job_spec_sha256 != submit.job_spec_sha256 or
                    post_job.state_store_uuid
                    != runtime.skylet_state_store_uuid):
                raise ValueError('Skylet no-effect post-job evidence differs '
                                 'from the exact intent request/action/runtime '
                                 'binding.')
            if context is not None:
                _validate_skylet_binding(submit, context)
            if proof['rejection'] == 'same_key_different_spec':
                retained = post_job.retained_submit_request
                assert retained is not None
                if retained.canonical_bytes == submit.canonical_bytes:
                    raise ValueError('same-key Skylet conflict retained the '
                                     'exact expected submit request.')


@dataclasses.dataclass(frozen=True)
class ProviderLaunchEffectQuiescenceV1(_CanonicalContract):
    """Reducer-owned E<i> or N<i> quiescence entry."""

    value: provider_values.CanonicalJsonObject

    _EVIDENCE_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'effect_sequence', 'effect_kind', 'role', 'intent_phase',
        'resolution', 'evidence_sha256', 'committed_evidence',
        'definitive_no_effect'
    })
    _NO_EFFECT_KEYS: ClassVar[frozenset[str]] = (_EVIDENCE_KEYS | frozenset(
        {'intent_origin', 'resolution_origin'}))

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchEffectQuiescenceV1:
        if type(value) is not dict:
            raise TypeError('launch quiescence effect must be an object.')
        resolution = value.get('resolution')
        if resolution == 'evidence_committed':
            raw = _closed_object(value,
                                 name='launch evidence quiescence effect',
                                 keys=cls._EVIDENCE_KEYS)
            _nonnegative_integer(raw['effect_sequence'],
                                 name='quiescence.effect_sequence')
            evidence = ProviderLaunchCommittedEffectEvidenceV1.from_value(
                raw['committed_evidence'])
            if (raw['effect_sequence'] != evidence.effect_sequence or
                    raw['effect_kind'] != evidence.effect_kind or raw['role']
                    != (None if evidence.role is None else evidence.role.value)
                    or raw['intent_phase'] != evidence.intent_phase or
                    raw['evidence_sha256'] != evidence.sha256 or
                    raw['definitive_no_effect'] is not None):
                raise ValueError('evidence quiescence entry differs from its '
                                 'complete committed evidence.')
        elif resolution in ('call_not_entered', 'definitive_no_effect'):
            raw = _closed_object(value,
                                 name='launch no-effect quiescence effect',
                                 keys=cls._NO_EFFECT_KEYS)
            sequence = _nonnegative_integer(raw['effect_sequence'],
                                            name='quiescence.effect_sequence')
            if sequence > 4:
                raise ValueError('quiescence effect sequence is unsupported.')
            expected_kind = ('core_v1_create'
                             if sequence < 3 else 'cluster_record_insert'
                             if sequence == 3 else 'skylet_job_submit')
            expected_phase = (
                'CREATE_INTENT' if sequence < 3 else
                'HANDLE_INTENT' if sequence == 3 else 'JOB_INTENT')
            expected_role = _ROLE_BY_SEQUENCE.get(sequence)
            if (raw['effect_kind'] != expected_kind or
                    raw['intent_phase'] != expected_phase or raw['role']
                    != (None if expected_role is None else expected_role.value)
                    or raw['committed_evidence'] is not None):
                raise ValueError('no-effect quiescence metadata differs from '
                                 'the literal launch effect table.')
            intent = ProviderLaunchEffectClaimV1.from_value(
                raw['intent_origin'])
            resolver = ProviderLaunchEffectClaimV1.from_value(
                raw['resolution_origin'])
            if intent.canonical_bytes != resolver.canonical_bytes:
                raise ValueError('quiescence no-effect origins must be '
                                 'byte-equal.')
            evidence_hash = _optional_sha256(raw['evidence_sha256'],
                                             name='quiescence.evidence_sha256')
            proof = (None if raw['definitive_no_effect'] is None else
                     ProviderLaunchEffectDefinitiveNoEffectV1.from_value(
                         raw['definitive_no_effect']))
            if resolution == 'call_not_entered':
                if evidence_hash is not None or proof is not None:
                    raise ValueError('call_not_entered quiescence requires '
                                     'null proof fields.')
            elif proof is None or evidence_hash != proof.sha256:
                raise ValueError('definitive quiescence proof hash differs '
                                 'from its complete preimage.')
        else:
            raise ValueError('launch quiescence resolution is unsupported.')
        _version_one(raw['version'], name='quiescence effect version')
        return cls(provider_values.CanonicalJsonObject.from_value(raw))

    @classmethod
    def from_committed(
        cls, evidence: ProviderLaunchCommittedEffectEvidenceV1
    ) -> ProviderLaunchEffectQuiescenceV1:
        value = {
            'version': 1,
            'effect_sequence': evidence.effect_sequence,
            'effect_kind': evidence.effect_kind,
            'role': None if evidence.role is None else evidence.role.value,
            'intent_phase': evidence.intent_phase,
            'resolution': 'evidence_committed',
            'evidence_sha256': evidence.sha256,
            'committed_evidence': evidence.canonical_value(),
            'definitive_no_effect': None,
        }
        return cls(provider_values.CanonicalJsonObject.from_value(value))

    @classmethod
    def from_resolution(
        cls, resolution: ProviderLaunchNoEffectResolutionV1
    ) -> ProviderLaunchEffectQuiescenceV1:
        value = {
            'version': 1,
            'effect_sequence': resolution.effect_sequence,
            'effect_kind': resolution.effect_kind,
            'role': None if resolution.role is None else resolution.role.value,
            'intent_phase': resolution.intent_phase,
            'resolution': resolution.resolution,
            'evidence_sha256': resolution.evidence_sha256,
            'committed_evidence': None,
            'intent_origin': resolution.intent_origin.canonical_value(),
            'resolution_origin': resolution.resolution_origin.canonical_value(),
            'definitive_no_effect':
                (None if resolution.definitive_no_effect is None else
                 resolution.definitive_no_effect.canonical_value()),
        }
        return cls(provider_values.CanonicalJsonObject.from_value(value))

    def canonical_value(self) -> kernel_actions.JsonObject:
        return self.value.canonical_value()


@dataclasses.dataclass(frozen=True)
class ProviderLaunchSupersessionQuiescenceV1(_CanonicalContract):
    """Final quiescence object constructed only by the Serve reducer."""

    launch_action_id: uuid.UUID
    launch_attempt: int
    request_id: uuid.UUID
    handler_terminal_result_sha256: str
    launch_provider_cursor_sha256: str
    effects: tuple[ProviderLaunchEffectQuiescenceV1, ...]
    settled_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'launch_action_id', 'launch_attempt', 'request_id',
        'request_terminal_state', 'active_claim',
        'handler_terminal_result_sha256', 'launch_provider_cursor_sha256',
        'effects', 'settled_at'
    })

    def __post_init__(self) -> None:
        if type(self.launch_action_id) is not uuid.UUID or type(
                self.request_id) is not uuid.UUID:
            raise TypeError('quiescence action/request IDs must be UUIDs.')
        object.__setattr__(
            self, 'launch_attempt',
            _resource_action_attempt(self.launch_attempt,
                                     name='quiescence.launch_attempt'))
        object.__setattr__(
            self, 'handler_terminal_result_sha256',
            _sha256(self.handler_terminal_result_sha256,
                    name='quiescence.handler_terminal_result_sha256'))
        object.__setattr__(
            self, 'launch_provider_cursor_sha256',
            _sha256(self.launch_provider_cursor_sha256,
                    name='quiescence.launch_provider_cursor_sha256'))
        if (type(self.effects) is not tuple or not self.effects or any(
                type(item) is not ProviderLaunchEffectQuiescenceV1
                for item in self.effects)):
            raise ValueError('quiescence requires its exact nonempty typed '
                             'effect list.')
        object.__setattr__(
            self, 'settled_at',
            _timestamp(self.settled_at, name='quiescence.settled_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ProviderLaunchSupersessionQuiescenceV1:
        raw = _closed_object(value,
                             name='launch supersession quiescence',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='quiescence version')
        if (raw['request_terminal_state'] != 'SUCCEEDED' or
                raw['active_claim'] is not False):
            raise ValueError('quiescence requires terminal SUCCEEDED with no '
                             'active claim.')
        effects = tuple(
            ProviderLaunchEffectQuiescenceV1.from_value(item) for item in
            _closed_list(raw['effects'], name='quiescence.effects', maximum=5))
        return cls(
            launch_action_id=_uuid(raw['launch_action_id'],
                                   name='quiescence.launch_action_id'),
            launch_attempt=raw['launch_attempt'],
            request_id=_uuid(raw['request_id'], name='quiescence.request_id'),
            handler_terminal_result_sha256=raw[
                'handler_terminal_result_sha256'],
            launch_provider_cursor_sha256=raw['launch_provider_cursor_sha256'],
            effects=effects,
            settled_at=raw['settled_at'])

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'version': 1,
            'launch_action_id': str(self.launch_action_id),
            'launch_attempt': self.launch_attempt,
            'request_id': str(self.request_id),
            'request_terminal_state': 'SUCCEEDED',
            'active_claim': False,
            'handler_terminal_result_sha256':
                self.handler_terminal_result_sha256,
            'launch_provider_cursor_sha256': self.launch_provider_cursor_sha256,
            'effects': [item.canonical_value() for item in self.effects],
            'settled_at': self.settled_at,
        }


def build_launch_supersession_quiescence_v1(
    action: kernel_actions.ActionRecord,
    attempt: kernel_actions.AttemptRecord,
    *,
    request_terminal_state: str,
    active_claim: bool,
    handler_terminal_result_sha256: str,
    request_settled_at: datetime.datetime | str,
    launch_no_effect_resolution: Mapping[str, Any] | None,
) -> ProviderLaunchSupersessionQuiescenceV1:
    """Build the literal reducer-owned quiescence row for partial handoff.

    The owner-fenced caller must derive ``active_claim`` and the terminal-result
    hash from the locked terminal request row.  This pure function rejects any
    caller-supplied quiescence list and constructs every E/N entry itself.
    """
    context = _ActionContext.from_record(action)
    if context.action_kind is not kernel_actions.ActionKind.LAUNCH:
        raise ValueError('supersession quiescence is launch-only.')
    if request_terminal_state != 'SUCCEEDED' or active_claim is not False:
        raise ValueError(
            'quiescence requires terminal SUCCEEDED with no active '
            'request claim.')
    if (attempt.action_id != action.action_id or
            attempt.request_id != kernel_actions.request_id_for_attempt(
                action.action_id, attempt.attempt) or
            attempt.provider_progress is None or
            attempt.provider_progress_sha256 is None or
            attempt.provider_progress_revision <= 0 or
            attempt.provider_io_boundary
            is kernel_actions.ProviderIOBoundary.NOT_STARTED):
        raise ValueError('quiescence requires a crossed, nonnull final API006 '
                         'cursor.')
    progress = ProviderLifecycleProgressV1.from_value(attempt.provider_progress)
    _validate_progress_attempt_binding(progress, attempt)
    progress.validate_action_context(context)
    if progress.sha256 != attempt.provider_progress_sha256:
        raise ValueError('final API006 envelope hash does not match its bytes.')
    if type(progress.cursor) is not ProviderLaunchProgressV1:
        raise ValueError('quiescence final cursor must be launch.')
    cursor = progress.cursor
    if cursor.phase is LaunchProgressPhaseV1.SUCCEEDED:
        raise ValueError('successful launch cursor cannot become partial '
                         'cleanup quiescence.')
    entries = [
        ProviderLaunchEffectQuiescenceV1.from_committed(effect)
        for effect in cursor.committed_effects
    ]
    if cursor.is_intent:
        if launch_no_effect_resolution is None:
            raise ValueError('current launch intent requires its exact N<i> '
                             'resolution.')
        resolution = ProviderLaunchNoEffectResolutionV1.from_value(
            launch_no_effect_resolution)
        resolution.validate_cursor(cursor, action.action_id, attempt.attempt,
                                   context)
        entries.append(
            ProviderLaunchEffectQuiescenceV1.from_resolution(resolution))
    elif launch_no_effect_resolution is not None:
        raise ValueError('E-only launch phase rejects a no-effect resolution.')
    if type(request_settled_at) is datetime.datetime:
        settled_at = _timestamp_from_datetime(request_settled_at,
                                              name='request_settled_at')
    elif type(request_settled_at) is str:
        settled_at = _timestamp(request_settled_at, name='request_settled_at')
    else:
        raise TypeError('request_settled_at must be an exact datetime or str.')
    return ProviderLaunchSupersessionQuiescenceV1(
        launch_action_id=action.action_id,
        launch_attempt=attempt.attempt,
        request_id=uuid.UUID(attempt.request_id),
        handler_terminal_result_sha256=handler_terminal_result_sha256,
        launch_provider_cursor_sha256=cursor.sha256,
        effects=tuple(entries),
        settled_at=settled_at)


class HandlerReductionKindV1(str, enum.Enum):
    DOMAIN = 'domain'
    SUPERSEDE_TO_DOWN = 'supersede_to_down'


class ProviderResultDispositionV1(str, enum.Enum):
    SUCCEEDED = 'succeeded'
    RETRYABLE = 'retryable'
    UNCERTAIN = 'uncertain'
    TERMINAL_ERROR = 'terminal_error'
    CANCELLED = 'cancelled'


class ProviderResultCertaintyV1(str, enum.Enum):
    OBSERVED = 'observed'
    PROVIDER_ACKNOWLEDGED = 'provider_acknowledged'
    UNKNOWN = 'unknown'


class ProviderResultRetryClassV1(str, enum.Enum):
    TRANSIENT = 'transient'
    CAPACITY = 'capacity'
    QUOTA = 'quota'
    RATE_LIMITED = 'rate_limited'
    OBSERVATION_REQUIRED = 'observation_required'


@dataclasses.dataclass(frozen=True)
class ServeReplicaActionProviderResultV1(_CanonicalContract):
    """Closed provider tuple retained in terminal returns and outcomes."""

    disposition: ProviderResultDispositionV1
    certainty: ProviderResultCertaintyV1
    provider_operation_id: str | None
    provider_code: str | None
    retry_class: ProviderResultRetryClassV1 | None
    retry_after_seconds: int | None
    observation: ProviderLifecycleObservationV1 | None
    normalized_message: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'disposition', 'certainty', 'provider_operation_id', 'provider_code',
        'retry_class', 'retry_after_seconds', 'observation',
        'normalized_message'
    })

    def __post_init__(self) -> None:
        if type(self.disposition) is not ProviderResultDispositionV1:
            raise TypeError('provider result disposition has an invalid type.')
        if type(self.certainty) is not ProviderResultCertaintyV1:
            raise TypeError('provider result certainty has an invalid type.')
        object.__setattr__(
            self, 'provider_operation_id',
            _optional_text(self.provider_operation_id,
                           name='provider_result.provider_operation_id'))
        object.__setattr__(
            self, 'provider_code',
            _optional_text(self.provider_code,
                           name='provider_result.provider_code'))
        if (self.retry_class is not None and
                type(self.retry_class) is not ProviderResultRetryClassV1):
            raise TypeError('provider result retry class has an invalid type.')
        if self.retry_after_seconds is not None:
            object.__setattr__(
                self, 'retry_after_seconds',
                _nonnegative_integer(
                    self.retry_after_seconds,
                    name='provider_result.retry_after_seconds'))
        if (self.observation is not None and
                type(self.observation) is not ProviderLifecycleObservationV1):
            raise TypeError('provider result observation has an invalid type.')
        object.__setattr__(
            self, 'normalized_message',
            _optional_text(self.normalized_message,
                           name='provider_result.normalized_message'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ServeReplicaActionProviderResultV1:
        raw = _closed_object(value,
                             name='Serve replica action provider result',
                             keys=cls._KEYS)
        try:
            disposition = ProviderResultDispositionV1(raw['disposition'])
            certainty = ProviderResultCertaintyV1(raw['certainty'])
            retry_class = (None if raw['retry_class'] is None else
                           ProviderResultRetryClassV1(raw['retry_class']))
        except (TypeError, ValueError) as e:
            raise ValueError('provider result contains an unsupported enum '
                             'value.') from e
        observation = (None if raw['observation'] is None else
                       ProviderLifecycleObservationV1.from_value(
                           raw['observation']))
        return cls(disposition=disposition,
                   certainty=certainty,
                   provider_operation_id=raw['provider_operation_id'],
                   provider_code=raw['provider_code'],
                   retry_class=retry_class,
                   retry_after_seconds=raw['retry_after_seconds'],
                   observation=observation,
                   normalized_message=raw['normalized_message'])

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'disposition': self.disposition.value,
            'certainty': self.certainty.value,
            'provider_operation_id': self.provider_operation_id,
            'provider_code': self.provider_code,
            'retry_class':
                (None if self.retry_class is None else self.retry_class.value),
            'retry_after_seconds': self.retry_after_seconds,
            'observation': (None if self.observation is None else
                            self.observation.canonical_value()),
            'normalized_message': self.normalized_message,
        }

    def with_provider_operation_id(
            self,
            operation_id: str | None) -> ServeReplicaActionProviderResultV1:
        value = self.canonical_value()
        value['provider_operation_id'] = operation_id
        return type(self).from_value(value)


@dataclasses.dataclass(frozen=True)
class ServeReplicaActionHandlerTerminalResultV1(_CanonicalContract):
    """Exact typed return emitted by a Serve resource-action handler."""

    version: int
    action_id: uuid.UUID
    action_kind: kernel_actions.ActionKind
    attempt: int
    request_id: uuid.UUID
    request_execution_generation: int
    handler_name: str
    reduction_kind: HandlerReductionKindV1
    request_input_sha256: str
    final_provider_progress_sha256: str | None
    worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1
    worker_attestation_sha256: str
    provider_result: ServeReplicaActionProviderResultV1
    normalized_provider_error: provider_values.ProviderErrorV1 | None
    launch_no_effect_resolution: ProviderLaunchNoEffectResolutionV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'result_kind', 'action_id', 'action_kind', 'attempt',
        'request_id', 'request_execution_generation', 'handler_name',
        'reduction_kind', 'request_input_sha256',
        'final_provider_progress_sha256', 'worker_attestation',
        'worker_attestation_sha256', 'provider_result',
        'normalized_provider_error', 'launch_no_effect_resolution'
    })

    def __post_init__(self) -> None:
        _version_one(self.version, name='handler terminal result version')
        if type(self.action_id) is not uuid.UUID or type(
                self.request_id) is not uuid.UUID:
            raise TypeError(
                'handler terminal action/request IDs must be UUIDs.')
        if type(self.action_kind) is not kernel_actions.ActionKind:
            raise TypeError('handler terminal action kind has an invalid type.')
        object.__setattr__(
            self, 'attempt',
            _resource_action_attempt(self.attempt,
                                     name='handler_result.attempt'))
        object.__setattr__(
            self, 'request_execution_generation',
            _positive_integer(
                self.request_execution_generation,
                name='handler_result.request_execution_generation'))
        expected_handler = f'serve_resource_action_{self.action_kind.value}'
        if self.handler_name != expected_handler:
            raise ValueError('handler terminal name differs from action kind.')
        if type(self.reduction_kind) is not HandlerReductionKindV1:
            raise TypeError('handler reduction kind has an invalid type.')
        if (self.reduction_kind is HandlerReductionKindV1.SUPERSEDE_TO_DOWN and
                self.action_kind is not kernel_actions.ActionKind.LAUNCH):
            raise ValueError('supersede_to_down is launch-only.')
        object.__setattr__(
            self, 'request_input_sha256',
            _sha256(self.request_input_sha256,
                    name='handler_result.request_input_sha256'))
        object.__setattr__(
            self, 'final_provider_progress_sha256',
            _optional_sha256(
                self.final_provider_progress_sha256,
                name='handler_result.final_provider_progress_sha256'))
        if type(self.worker_attestation) is not (
                ProviderAuthorityWorkerAttemptAttestationV1):
            raise TypeError('handler result worker attestation has an invalid '
                            'type.')
        object.__setattr__(
            self, 'worker_attestation_sha256',
            _sha256(self.worker_attestation_sha256,
                    name='handler_result.worker_attestation_sha256'))
        if self.worker_attestation_sha256 != self.worker_attestation.sha256:
            raise ValueError('handler result worker-attestation hash differs '
                             'from its complete preimage.')
        if (self.worker_attestation.request_id != self.request_id or
                self.worker_attestation.request_execution_generation
                != self.request_execution_generation):
            raise ValueError('handler result worker attestation differs from '
                             'its request execution identity.')
        if type(self.provider_result) is not ServeReplicaActionProviderResultV1:
            raise TypeError('handler provider result has an invalid type.')
        if (self.normalized_provider_error is not None and
                type(self.normalized_provider_error)
                is not provider_values.ProviderErrorV1):
            raise TypeError('handler normalized provider error has an invalid '
                            'type.')
        if (self.launch_no_effect_resolution is not None and
                type(self.launch_no_effect_resolution)
                is not ProviderLaunchNoEffectResolutionV1):
            raise TypeError('handler launch no-effect resolution has an '
                            'invalid type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ServeReplicaActionHandlerTerminalResultV1:
        raw = _closed_object(value,
                             name='Serve handler terminal result',
                             keys=cls._KEYS)
        if raw['result_kind'] != 'serve_resource_action_handler_terminal_v1':
            raise ValueError('handler terminal result kind is unsupported.')
        try:
            action_kind = kernel_actions.ActionKind(raw['action_kind'])
            reduction_kind = HandlerReductionKindV1(raw['reduction_kind'])
        except (TypeError, ValueError) as e:
            raise ValueError('handler terminal result discriminator is '
                             'unsupported.') from e
        return cls(
            version=raw['version'],
            action_id=_uuid(raw['action_id'], name='handler_result.action_id'),
            action_kind=action_kind,
            attempt=raw['attempt'],
            request_id=_uuid(raw['request_id'],
                             name='handler_result.request_id'),
            request_execution_generation=raw['request_execution_generation'],
            handler_name=raw['handler_name'],
            reduction_kind=reduction_kind,
            request_input_sha256=raw['request_input_sha256'],
            final_provider_progress_sha256=raw[
                'final_provider_progress_sha256'],
            worker_attestation=(
                ProviderAuthorityWorkerAttemptAttestationV1.from_value(
                    raw['worker_attestation'])),
            worker_attestation_sha256=raw['worker_attestation_sha256'],
            provider_result=ServeReplicaActionProviderResultV1.from_value(
                raw['provider_result']),
            normalized_provider_error=(
                None if raw['normalized_provider_error'] is None else
                provider_values.ProviderErrorV1.from_value(
                    raw['normalized_provider_error'])),
            launch_no_effect_resolution=(
                None if raw['launch_no_effect_resolution'] is None else
                ProviderLaunchNoEffectResolutionV1.from_value(
                    raw['launch_no_effect_resolution'])))

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'version': 1,
            'result_kind': 'serve_resource_action_handler_terminal_v1',
            'action_id': str(self.action_id),
            'action_kind': self.action_kind.value,
            'attempt': self.attempt,
            'request_id': str(self.request_id),
            'request_execution_generation': self.request_execution_generation,
            'handler_name': self.handler_name,
            'reduction_kind': self.reduction_kind.value,
            'request_input_sha256': self.request_input_sha256,
            'final_provider_progress_sha256':
                self.final_provider_progress_sha256,
            'worker_attestation': self.worker_attestation.canonical_value(),
            'worker_attestation_sha256': self.worker_attestation_sha256,
            'provider_result': self.provider_result.canonical_value(),
            'normalized_provider_error':
                (None if self.normalized_provider_error is None else
                 self.normalized_provider_error.canonical_value()),
            'launch_no_effect_resolution':
                (None if self.launch_no_effect_resolution is None else
                 self.launch_no_effect_resolution.canonical_value()),
        }


@dataclasses.dataclass(frozen=True)
class ServeReplicaActionRequestReturnV1(_CanonicalContract):
    """Closed request-return envelope around one terminal handler result."""

    terminal_result: ServeReplicaActionHandlerTerminalResultV1
    terminal_result_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'return_type', 'terminal_result', 'terminal_result_sha256'})

    def __post_init__(self) -> None:
        if type(self.terminal_result) is not (
                ServeReplicaActionHandlerTerminalResultV1):
            raise TypeError('request return terminal result has an invalid '
                            'type.')
        object.__setattr__(
            self, 'terminal_result_sha256',
            _sha256(self.terminal_result_sha256,
                    name='request_return.terminal_result_sha256'))
        if self.terminal_result_sha256 != self.terminal_result.sha256:
            raise ValueError('request return terminal-result hash differs '
                             'from its complete preimage.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ServeReplicaActionRequestReturnV1:
        raw = _closed_object(value,
                             name='Serve resource-action request return',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='request return version')
        if raw['return_type'] != (
                'serve_replica_action_handler_terminal_result_v1'):
            raise ValueError('request return type is unsupported.')
        return cls(terminal_result=(
            ServeReplicaActionHandlerTerminalResultV1.from_value(
                raw['terminal_result'])),
                   terminal_result_sha256=raw['terminal_result_sha256'])

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'version': 1,
            'return_type': 'serve_replica_action_handler_terminal_result_v1',
            'terminal_result': self.terminal_result.canonical_value(),
            'terminal_result_sha256': self.terminal_result_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ServeLaunchNoIoAttemptProjectionV1(_CanonicalContract):
    """One reducer-owned revision-zero launch-attempt accumulator link."""

    attempt: int
    request_id: uuid.UUID
    request_input_sha256: str
    request_terminal_state: str
    settled_at: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'attempt', 'request_id', 'request_input_sha256', 'mutation_boundary',
        'provider_io_boundary', 'provider_progress_revision',
        'provider_progress_sha256', 'provider_operation_id',
        'request_terminal_state', 'settled_at'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'attempt',
            _resource_action_attempt(self.attempt, name='no_io.attempt'))
        if type(self.request_id) is not uuid.UUID:
            raise TypeError('no-I/O request ID must be a UUID.')
        object.__setattr__(
            self, 'request_input_sha256',
            _sha256(self.request_input_sha256,
                    name='no_io.request_input_sha256'))
        if self.request_terminal_state not in ('SUCCEEDED', 'FAILED',
                                               'CANCELLED'):
            raise ValueError('no-I/O request terminal state is unsupported.')
        object.__setattr__(self, 'settled_at',
                           _timestamp(self.settled_at, name='no_io.settled_at'))
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ServeLaunchNoIoAttemptProjectionV1:
        raw = _closed_object(value,
                             name='launch no-I/O attempt projection',
                             keys=cls._KEYS)
        revision = _nonnegative_integer(raw['provider_progress_revision'],
                                        name='no_io.provider_progress_revision')
        if (raw['mutation_boundary'] != 'SETTLED' or
                raw['provider_io_boundary'] != 'NOT_STARTED' or revision != 0 or
                raw['provider_progress_sha256'] is not None or
                raw['provider_operation_id'] is not None):
            raise ValueError('launch no-I/O projection is not the exact '
                             'revision-zero settled shape.')
        return cls(attempt=raw['attempt'],
                   request_id=_uuid(raw['request_id'], name='no_io.request_id'),
                   request_input_sha256=raw['request_input_sha256'],
                   request_terminal_state=raw['request_terminal_state'],
                   settled_at=raw['settled_at'])

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'attempt': self.attempt,
            'request_id': str(self.request_id),
            'request_input_sha256': self.request_input_sha256,
            'mutation_boundary': 'SETTLED',
            'provider_io_boundary': 'NOT_STARTED',
            'provider_progress_revision': 0,
            'provider_progress_sha256': None,
            'provider_operation_id': None,
            'request_terminal_state': self.request_terminal_state,
            'settled_at': self.settled_at,
        }


@dataclasses.dataclass(frozen=True)
class ServeLaunchNoIoPrefixV1(_CanonicalContract):
    """O(1) reducer-owned hash-chain prefix for revision-zero launches."""

    count: int
    previous_prefix_sha256: str | None
    current_attempt: ServeLaunchNoIoAttemptProjectionV1 | None
    prefix_sha256: str

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'count', 'previous_prefix_sha256', 'current_attempt',
        'prefix_sha256'
    })

    def __post_init__(self) -> None:
        count = _nonnegative_integer(self.count, name='no_io_prefix.count')
        if count > _MAX_RESOURCE_ACTION_ATTEMPT_V1:
            raise ValueError('no-I/O prefix count exceeds attempt domain.')
        object.__setattr__(self, 'count', count)
        object.__setattr__(
            self, 'previous_prefix_sha256',
            _optional_sha256(self.previous_prefix_sha256,
                             name='no_io_prefix.previous_prefix_sha256'))
        if (self.current_attempt is not None and type(self.current_attempt)
                is not ServeLaunchNoIoAttemptProjectionV1):
            raise TypeError('no-I/O prefix current attempt has an invalid '
                            'type.')
        object.__setattr__(
            self, 'prefix_sha256',
            _sha256(self.prefix_sha256, name='no_io_prefix.prefix_sha256'))
        if count == 0:
            if (self.previous_prefix_sha256 is not None or
                    self.current_attempt is not None or
                    self.prefix_sha256 != kernel_actions.canonical_sha256([])):
                raise ValueError('count-zero no-I/O prefix has invalid '
                                 'members or hash.')
        else:
            if (self.current_attempt is None or
                    self.current_attempt.attempt != count or
                ((count == 1) != (self.previous_prefix_sha256 is None))):
                raise ValueError('positive no-I/O prefix has an invalid '
                                 'immediate link.')
            preimage = {
                'previous_prefix_sha256': self.previous_prefix_sha256,
                'current_attempt': self.current_attempt.canonical_value(),
            }
            if self.prefix_sha256 != kernel_actions.canonical_sha256(preimage):
                raise ValueError('no-I/O prefix hash differs from its '
                                 'immediate-link preimage.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ServeLaunchNoIoPrefixV1:
        raw = _closed_object(value, name='launch no-I/O prefix', keys=cls._KEYS)
        _version_one(raw['version'], name='no-I/O prefix version')
        current = (None if raw['current_attempt'] is None else
                   ServeLaunchNoIoAttemptProjectionV1.from_value(
                       raw['current_attempt']))
        return cls(count=raw['count'],
                   previous_prefix_sha256=raw['previous_prefix_sha256'],
                   current_attempt=current,
                   prefix_sha256=raw['prefix_sha256'])

    @classmethod
    def append(
        cls,
        previous: ServeLaunchNoIoPrefixV1 | None,
        current: ServeLaunchNoIoAttemptProjectionV1,
    ) -> ServeLaunchNoIoPrefixV1:
        expected_attempt = 1 if previous is None else previous.count + 1
        if current.attempt != expected_attempt:
            raise ValueError('no-I/O prefix append is not the immediate '
                             'attempt successor.')
        previous_hash = None if previous is None else previous.prefix_sha256
        preimage = {
            'previous_prefix_sha256': previous_hash,
            'current_attempt': current.canonical_value(),
        }
        return cls(count=current.attempt,
                   previous_prefix_sha256=previous_hash,
                   current_attempt=current,
                   prefix_sha256=kernel_actions.canonical_sha256(preimage))

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'version': 1,
            'count': self.count,
            'previous_prefix_sha256': self.previous_prefix_sha256,
            'current_attempt': (None if self.current_attempt is None else
                                self.current_attempt.canonical_value()),
            'prefix_sha256': self.prefix_sha256,
        }


@dataclasses.dataclass(frozen=True)
class ServeReplicaActionHandlerOutcomeV1(_CanonicalContract):
    """Reducer-owned handler-basis member of ServeReplicaActionOutcomeV1."""

    handler_terminal_result_sha256: str
    provider_result: ServeReplicaActionProviderResultV1
    supersession_quiescence: ProviderLaunchSupersessionQuiescenceV1 | None
    launch_no_io_prefix: ServeLaunchNoIoPrefixV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'basis', 'provider_result', 'supersession_quiescence',
        'launch_no_io_prefix'
    })
    _BASIS_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'basis_kind', 'request_terminal_state',
        'handler_terminal_result_sha256', 'direct_no_effect_cancellation',
        'request_fallback_evidence'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'handler_terminal_result_sha256',
            _sha256(self.handler_terminal_result_sha256,
                    name='handler_outcome.terminal_result_sha256'))
        if type(self.provider_result) is not ServeReplicaActionProviderResultV1:
            raise TypeError('handler outcome provider result has an invalid '
                            'type.')
        if (self.supersession_quiescence is not None and
                type(self.supersession_quiescence)
                is not ProviderLaunchSupersessionQuiescenceV1):
            raise TypeError('handler outcome quiescence has an invalid type.')
        if (self.launch_no_io_prefix is not None and
                type(self.launch_no_io_prefix) is not ServeLaunchNoIoPrefixV1):
            raise TypeError('handler outcome no-I/O prefix has an invalid '
                            'type.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls, value: Any) -> ServeReplicaActionHandlerOutcomeV1:
        raw = _closed_object(value,
                             name='Serve handler action outcome',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='handler outcome version')
        basis = _closed_object(raw['basis'],
                               name='handler outcome basis',
                               keys=cls._BASIS_KEYS)
        _version_one(basis['version'], name='handler outcome basis version')
        if (basis['basis_kind'] != 'handler_terminal_result' or
                basis['request_terminal_state'] != 'SUCCEEDED' or
                basis['direct_no_effect_cancellation'] is not None or
                basis['request_fallback_evidence'] is not None):
            raise ValueError('handler outcome basis is not the exact '
                             'terminal-result member.')
        return cls(
            handler_terminal_result_sha256=basis[
                'handler_terminal_result_sha256'],
            provider_result=ServeReplicaActionProviderResultV1.from_value(
                raw['provider_result']),
            supersession_quiescence=(
                None if raw['supersession_quiescence'] is None else
                ProviderLaunchSupersessionQuiescenceV1.from_value(
                    raw['supersession_quiescence'])),
            launch_no_io_prefix=(None if raw['launch_no_io_prefix'] is None else
                                 ServeLaunchNoIoPrefixV1.from_value(
                                     raw['launch_no_io_prefix'])))

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'version': 1,
            'basis': {
                'version': 1,
                'basis_kind': 'handler_terminal_result',
                'request_terminal_state': 'SUCCEEDED',
                'handler_terminal_result_sha256':
                    self.handler_terminal_result_sha256,
                'direct_no_effect_cancellation': None,
                'request_fallback_evidence': None,
            },
            'provider_result': self.provider_result.canonical_value(),
            'supersession_quiescence':
                (None if self.supersession_quiescence is None else
                 self.supersession_quiescence.canonical_value()),
            'launch_no_io_prefix':
                (None if self.launch_no_io_prefix is None else
                 self.launch_no_io_prefix.canonical_value()),
        }


@dataclasses.dataclass(frozen=True)
class ServeReplicaActionRequestFallbackEvidenceV1(_CanonicalContract):
    """Closed request-terminal evidence used when no valid handler return exists."""

    request_id: uuid.UUID
    attempt: int
    fallback_reason: str
    request_terminal_state: str
    request_finished_at: str
    journal_class: str
    provider_io_boundary: kernel_actions.ProviderIOBoundary
    provider_progress_revision: int
    provider_progress_sha256: str | None
    provider_operation_id: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'request_id', 'attempt', 'fallback_reason',
        'request_terminal_state', 'request_finished_at', 'active_claim',
        'journal_class', 'provider_io_boundary', 'provider_progress_revision',
        'provider_progress_sha256', 'provider_operation_id'
    })
    _REASONS: ClassVar[frozenset[str]] = frozenset({
        'missing_handler_return', 'invalid_handler_return', 'request_failed',
        'request_cancelled'
    })
    _JOURNAL_CLASSES: ClassVar[frozenset[str]] = frozenset({
        'not_started_empty', 'valid_nonterminal', 'valid_succeeded', 'invalid'
    })

    def __post_init__(self) -> None:
        if type(self.request_id) is not uuid.UUID:
            raise TypeError('fallback evidence request_id must be a UUID.')
        object.__setattr__(
            self, 'attempt',
            _resource_action_attempt(self.attempt,
                                     name='fallback_evidence.attempt'))
        if self.fallback_reason not in self._REASONS:
            raise ValueError('fallback evidence reason is unsupported.')
        if self.request_terminal_state not in ('SUCCEEDED', 'FAILED',
                                               'CANCELLED'):
            raise ValueError('fallback request terminal state is unsupported.')
        allowed_reason = ({
            'FAILED': frozenset({'request_failed'}),
            'CANCELLED': frozenset({'request_cancelled'}),
            'SUCCEEDED': frozenset(
                {'missing_handler_return', 'invalid_handler_return'}),
        })[self.request_terminal_state]
        if self.fallback_reason not in allowed_reason:
            raise ValueError('fallback reason differs from request terminal '
                             'state.')
        _timestamp(self.request_finished_at,
                   name='fallback_evidence.request_finished_at')
        if self.journal_class not in self._JOURNAL_CLASSES:
            raise ValueError('fallback journal class is unsupported.')
        if type(self.provider_io_boundary) is not (
                kernel_actions.ProviderIOBoundary):
            raise TypeError('fallback provider I/O boundary has invalid type.')
        _nonnegative_integer(self.provider_progress_revision,
                             name='fallback progress revision')
        _optional_sha256(self.provider_progress_sha256,
                         name='fallback progress sha256')
        _optional_text(self.provider_operation_id,
                       name='fallback provider operation ID')
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ServeReplicaActionRequestFallbackEvidenceV1:
        raw = _closed_object(value,
                             name='request fallback evidence',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='fallback evidence version')
        if raw['active_claim'] is not False:
            raise ValueError('fallback evidence requires active_claim=false.')
        try:
            boundary = kernel_actions.ProviderIOBoundary(
                raw['provider_io_boundary'])
        except (TypeError, ValueError) as e:
            raise ValueError(
                'fallback provider I/O boundary is unsupported.') from e
        return cls(request_id=_uuid(raw['request_id'],
                                    name='fallback_evidence.request_id'),
                   attempt=raw['attempt'],
                   fallback_reason=raw['fallback_reason'],
                   request_terminal_state=raw['request_terminal_state'],
                   request_finished_at=raw['request_finished_at'],
                   journal_class=raw['journal_class'],
                   provider_io_boundary=boundary,
                   provider_progress_revision=raw['provider_progress_revision'],
                   provider_progress_sha256=raw['provider_progress_sha256'],
                   provider_operation_id=raw['provider_operation_id'])

    def canonical_value(self) -> kernel_actions.JsonObject:
        return {
            'version': 1,
            'request_id': str(self.request_id),
            'attempt': self.attempt,
            'fallback_reason': self.fallback_reason,
            'request_terminal_state': self.request_terminal_state,
            'request_finished_at': self.request_finished_at,
            'active_claim': False,
            'journal_class': self.journal_class,
            'provider_io_boundary': self.provider_io_boundary.value,
            'provider_progress_revision': self.provider_progress_revision,
            'provider_progress_sha256': self.provider_progress_sha256,
            'provider_operation_id': self.provider_operation_id,
        }


@dataclasses.dataclass(frozen=True)
class ServeReplicaActionRequestFallbackOutcomeV1(_CanonicalContract):
    """Reducer-owned request-terminal-fallback outcome member."""

    evidence: ServeReplicaActionRequestFallbackEvidenceV1
    provider_result: ServeReplicaActionProviderResultV1
    launch_no_io_prefix: ServeLaunchNoIoPrefixV1 | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'basis', 'provider_result', 'supersession_quiescence',
        'launch_no_io_prefix'
    })
    _BASIS_KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'basis_kind', 'request_terminal_state',
        'handler_terminal_result_sha256', 'direct_no_effect_cancellation',
        'request_fallback_evidence'
    })

    def __post_init__(self) -> None:
        if type(self.evidence) is not (
                ServeReplicaActionRequestFallbackEvidenceV1):
            raise TypeError('fallback outcome evidence has an invalid type.')
        if type(self.provider_result) is not ServeReplicaActionProviderResultV1:
            raise TypeError(
                'fallback outcome provider result has invalid type.')
        if (self.launch_no_io_prefix is not None and
                type(self.launch_no_io_prefix) is not ServeLaunchNoIoPrefixV1):
            raise TypeError('fallback outcome no-I/O prefix has invalid type.')
        _validate_fallback_provider_tuple(self.evidence.journal_class,
                                          self.provider_result)
        _ = self.canonical_bytes

    @classmethod
    def from_value(cls,
                   value: Any) -> ServeReplicaActionRequestFallbackOutcomeV1:
        raw = _closed_object(value,
                             name='Serve request fallback outcome',
                             keys=cls._KEYS)
        _version_one(raw['version'], name='fallback outcome version')
        basis = _closed_object(raw['basis'],
                               name='fallback outcome basis',
                               keys=cls._BASIS_KEYS)
        _version_one(basis['version'], name='fallback basis version')
        if (basis['basis_kind'] != 'request_terminal_fallback' or
                basis['handler_terminal_result_sha256'] is not None or
                basis['direct_no_effect_cancellation'] is not None or
                raw['supersession_quiescence'] is not None):
            raise ValueError('fallback outcome has a crossed basis/null shape.')
        evidence = ServeReplicaActionRequestFallbackEvidenceV1.from_value(
            basis['request_fallback_evidence'])
        if basis['request_terminal_state'] != evidence.request_terminal_state:
            raise ValueError('fallback basis terminal state differs from its '
                             'evidence.')
        return cls(
            evidence=evidence,
            provider_result=ServeReplicaActionProviderResultV1.from_value(
                raw['provider_result']),
            launch_no_io_prefix=(None if raw['launch_no_io_prefix'] is None else
                                 ServeLaunchNoIoPrefixV1.from_value(
                                     raw['launch_no_io_prefix'])))

    def canonical_value(self) -> kernel_actions.JsonObject:
        evidence = self.evidence.canonical_value()
        return {
            'version': 1,
            'basis': {
                'version': 1,
                'basis_kind': 'request_terminal_fallback',
                'request_terminal_state': self.evidence.request_terminal_state,
                'handler_terminal_result_sha256': None,
                'direct_no_effect_cancellation': None,
                'request_fallback_evidence': evidence,
            },
            'provider_result': self.provider_result.canonical_value(),
            'supersession_quiescence': None,
            'launch_no_io_prefix':
                (None if self.launch_no_io_prefix is None else
                 self.launch_no_io_prefix.canonical_value()),
        }


PersistedServeReplicaActionOutcomeV1 = (
    ServeReplicaActionHandlerOutcomeV1 |
    ServeReplicaActionRequestFallbackOutcomeV1)


def _parse_persisted_outcome(
    value: Any,) -> PersistedServeReplicaActionOutcomeV1:
    if type(value) is not dict or type(value.get('basis')) is not dict:
        raise ValueError('Serve action outcome lacks its closed basis.')
    basis_kind = value['basis'].get('basis_kind')
    if basis_kind == 'handler_terminal_result':
        return ServeReplicaActionHandlerOutcomeV1.from_value(value)
    if basis_kind == 'request_terminal_fallback':
        return ServeReplicaActionRequestFallbackOutcomeV1.from_value(value)
    raise ValueError('Serve action outcome basis is unsupported.')


def _validate_persisted_provider_result_shape(
    result: ServeReplicaActionProviderResultV1,) -> None:
    if result.certainty is ProviderResultCertaintyV1.PROVIDER_ACKNOWLEDGED:
        raise ValueError('handler outcome cannot use provider_acknowledged '
                         'certainty.')
    if result.disposition is ProviderResultDispositionV1.SUCCEEDED:
        if (result.certainty is not ProviderResultCertaintyV1.OBSERVED or
                result.provider_code is not None or
                result.retry_class is not None or
                result.retry_after_seconds is not None or
                result.observation is None or
                result.normalized_message is not None):
            raise ValueError('persisted S provider tuple is invalid.')
    elif result.disposition is ProviderResultDispositionV1.CANCELLED:
        if (result.certainty is not ProviderResultCertaintyV1.OBSERVED or
                result.provider_code is not None or
                result.retry_class is not None or
                result.retry_after_seconds is not None or
                result.observation is not None or
                result.normalized_message is not None):
            raise ValueError('persisted Q provider tuple is invalid.')
    elif result.disposition is ProviderResultDispositionV1.RETRYABLE:
        if (result.certainty is not ProviderResultCertaintyV1.UNKNOWN or
                result.retry_class
                not in (ProviderResultRetryClassV1.TRANSIENT,
                        ProviderResultRetryClassV1.CAPACITY,
                        ProviderResultRetryClassV1.QUOTA,
                        ProviderResultRetryClassV1.RATE_LIMITED) or
                result.retry_after_seconds is None or
                result.retry_after_seconds > 3600 or
                result.observation is not None):
            raise ValueError('persisted R provider tuple is invalid.')
    elif result.disposition is ProviderResultDispositionV1.UNCERTAIN:
        if (result.certainty is not ProviderResultCertaintyV1.UNKNOWN or
                result.retry_class
                is not ProviderResultRetryClassV1.OBSERVATION_REQUIRED or
                result.retry_after_seconds != 60 or
                result.observation is not None):
            raise ValueError('persisted U provider tuple is invalid.')
    elif result.disposition is ProviderResultDispositionV1.TERMINAL_ERROR:
        if (result.certainty is not ProviderResultCertaintyV1.UNKNOWN or
                result.retry_class is not None or
                result.retry_after_seconds is not None or
                result.observation is not None):
            raise ValueError('persisted B provider tuple is invalid.')


def _validate_fallback_provider_tuple(
    journal_class: str,
    result: ServeReplicaActionProviderResultV1,
) -> None:
    value = result.canonical_value()
    if journal_class == 'not_started_empty':
        expected = {
            'disposition': 'retryable',
            'certainty': 'observed',
            'provider_operation_id': None,
            'provider_code': None,
            'retry_class': 'transient',
            'retry_after_seconds': 60,
            'observation': None,
            'normalized_message': None,
        }
        if value != expected:
            raise ValueError('fallback P0 provider tuple is invalid.')
        return
    if journal_class == 'valid_nonterminal':
        if (result.disposition is not ProviderResultDispositionV1.UNCERTAIN or
                result.certainty is not ProviderResultCertaintyV1.UNKNOWN or
                result.provider_code is not None or result.retry_class
                is not ProviderResultRetryClassV1.OBSERVATION_REQUIRED or
                result.retry_after_seconds != 60 or
                result.observation is not None or
                result.normalized_message is not None):
            raise ValueError('fallback O provider tuple is invalid.')
        return
    if journal_class == 'valid_succeeded':
        if (result.disposition is not ProviderResultDispositionV1.SUCCEEDED or
                result.certainty is not ProviderResultCertaintyV1.OBSERVED or
                result.provider_code is not None or
                result.retry_class is not None or
                result.retry_after_seconds is not None or
                result.observation is None or
                result.normalized_message is not None):
            raise ValueError('fallback S provider tuple is invalid.')
        return
    if journal_class == 'invalid':
        if (result.disposition is not ProviderResultDispositionV1.TERMINAL_ERROR
                or result.certainty is not ProviderResultCertaintyV1.UNKNOWN or
                result.provider_code is not None or
                result.retry_class is not None or
                result.retry_after_seconds is not None or
                result.observation is not None or
                result.normalized_message is not None):
            raise ValueError('fallback X provider tuple is invalid.')
        return
    raise ValueError('fallback provider tuple has unsupported journal class.')


def _validate_retry_authorizing_predecessor(
    action: kernel_actions.ActionRecord,
    predecessor: kernel_actions.AttemptRecord,
    context: _ActionContext,
) -> ProviderLifecycleProgressV1 | None:
    """Validate the complete retained state that can authorize attempt n+1."""

    if (predecessor.typed_outcome is None or predecessor.typed_outcome_sha256
            != kernel_actions.canonical_sha256(predecessor.typed_outcome) or
            predecessor.settled_at is None or predecessor.request_terminal_state
            not in ('SUCCEEDED', 'FAILED', 'CANCELLED')):
        raise ValueError('retry predecessor lacks its exact settled outcome.')
    outcome = _parse_persisted_outcome(predecessor.typed_outcome)
    result = outcome.provider_result
    if type(outcome) is ServeReplicaActionHandlerOutcomeV1:
        if predecessor.request_terminal_state != 'SUCCEEDED':
            raise ValueError('handler retry predecessor request did not '
                             'terminalize as SUCCEEDED.')
        _validate_persisted_provider_result_shape(result)
        if (result.disposition not in (ProviderResultDispositionV1.RETRYABLE,
                                       ProviderResultDispositionV1.UNCERTAIN) or
                outcome.supersession_quiescence is not None):
            raise ValueError('handler predecessor outcome does not authorize '
                             'retry or observation.')
    else:
        assert type(outcome) is ServeReplicaActionRequestFallbackOutcomeV1
        evidence = outcome.evidence
        if (str(evidence.request_id) != predecessor.request_id or
                evidence.attempt != predecessor.attempt or
                evidence.request_terminal_state
                != predecessor.request_terminal_state or
                evidence.provider_io_boundary
                is not predecessor.provider_io_boundary or
                evidence.provider_progress_revision
                != predecessor.provider_progress_revision or
                evidence.provider_progress_sha256
                != predecessor.provider_progress_sha256 or
                evidence.provider_operation_id
                != predecessor.provider_operation_id):
            raise ValueError('fallback retry predecessor differs from its '
                             'retained attempt journal.')
        _validate_fallback_provider_tuple(evidence.journal_class, result)
        if evidence.journal_class not in ('not_started_empty',
                                          'valid_nonterminal'):
            raise ValueError('fallback predecessor outcome does not authorize '
                             'retry or observation.')
    if result.provider_operation_id != predecessor.provider_operation_id:
        raise ValueError('retry predecessor provider operation ID differs from '
                         'its attempt journal.')

    progress: ProviderLifecycleProgressV1 | None = None
    if predecessor.provider_progress is None:
        if (predecessor.provider_io_boundary
                is not kernel_actions.ProviderIOBoundary.NOT_STARTED or
                predecessor.provider_progress_sha256 is not None or
                predecessor.provider_progress_revision != 0 or
                predecessor.provider_operation_id is not None):
            raise ValueError(
                'null retry cursor requires the exact revision-zero '
                'pre-I/O journal.')
    else:
        progress = ProviderLifecycleProgressV1.from_value(
            predecessor.provider_progress)
        if (predecessor.provider_progress_sha256 != progress.sha256 or
                predecessor.provider_progress_revision <= 0):
            raise ValueError('retry predecessor progress hash/revision is '
                             'invalid.')
        _validate_progress_attempt_binding(progress, predecessor)
        _validate_progress_operation_ids(progress, predecessor)
        progress.validate_action_context(context)
        if progress.is_succeeded:
            raise ValueError('SUCCEEDED provider progress cannot seed a retry.')
        boundary = predecessor.provider_io_boundary
        if boundary is kernel_actions.ProviderIOBoundary.NOT_STARTED:
            if (predecessor.provider_progress_revision != 1 or
                    progress.worker_attestation is not None or
                    predecessor.provider_operation_id is not None):
                raise ValueError('inherited retry predecessor has a crossed '
                                 'NOT_STARTED cursor.')
        elif boundary in (
                kernel_actions.ProviderIOBoundary.INTENT_COMMITTED,
                kernel_actions.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS):
            if progress.worker_attestation is None:
                raise ValueError('crossed retry predecessor lacks its worker '
                                 'attestation.')
        else:
            raise ValueError('retry predecessor provider boundary is '
                             'unsupported.')

    expected_fallback_class = ('not_started_empty'
                               if progress is None else 'valid_nonterminal')
    if (type(outcome) is ServeReplicaActionRequestFallbackOutcomeV1 and
            outcome.evidence.journal_class != expected_fallback_class):
        raise ValueError('fallback retry authorization differs from its exact '
                         'journal class.')

    prefix = outcome.launch_no_io_prefix
    should_have_prefix = (action.action_type
                          == kernel_actions.ActionKind.LAUNCH.value and
                          progress is None)
    if should_have_prefix:
        if prefix is None:
            raise ValueError('revision-zero launch retry lacks its no-I/O '
                             'prefix.')
        current = prefix.current_attempt
        assert current is not None
        expected_previous_presence = predecessor.attempt > 1
        if (prefix.count != predecessor.attempt or
            (prefix.previous_prefix_sha256
             is not None) != expected_previous_presence or
                current.attempt != predecessor.attempt or
                str(current.request_id) != predecessor.request_id or
                current.request_input_sha256 != predecessor.request_input_sha256
                or current.request_terminal_state
                != predecessor.request_terminal_state or
                current.settled_at != _timestamp_from_datetime(
                    predecessor.settled_at, name='predecessor.settled_at')):
            raise ValueError('revision-zero launch retry prefix differs from '
                             'its retained attempt projection.')
    elif prefix is not None:
        raise ValueError('crossed/down retry predecessor contains a no-I/O '
                         'prefix.')
    return progress


def _validate_settled_quiescence(
    action: kernel_actions.ActionRecord,
    attempt: kernel_actions.AttemptRecord,
    context: _ActionContext,
    cursor: ProviderLaunchProgressV1,
    outcome: ServeReplicaActionHandlerOutcomeV1,
) -> None:
    quiescence = outcome.supersession_quiescence
    result = outcome.provider_result
    if quiescence is None:
        if result.disposition is ProviderResultDispositionV1.CANCELLED:
            raise ValueError('handler-basis cancelled outcome lacks reducer '
                             'quiescence.')
        return
    if (attempt.provider_io_boundary
            is not kernel_actions.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS or
            cursor.phase is LaunchProgressPhaseV1.SUCCEEDED or
            result.disposition is not ProviderResultDispositionV1.CANCELLED or
            quiescence.launch_action_id != action.action_id or
            quiescence.launch_attempt != attempt.attempt or
            str(quiescence.request_id) != attempt.request_id or
            quiescence.handler_terminal_result_sha256
            != outcome.handler_terminal_result_sha256 or
            quiescence.launch_provider_cursor_sha256 != cursor.sha256 or
            attempt.request_terminal_state != 'SUCCEEDED'):
        raise ValueError('settled launch quiescence differs from the retained '
                         'attempt/cursor/outcome.')
    expected_evidence = tuple(
        ProviderLaunchEffectQuiescenceV1.from_committed(effect)
        for effect in cursor.committed_effects)
    if len(quiescence.effects) != len(expected_evidence) + int(
            cursor.is_intent):
        raise ValueError('settled launch quiescence has the wrong literal '
                         'effect count.')
    for actual, expected in zip(quiescence.effects, expected_evidence):
        if actual.canonical_bytes != expected.canonical_bytes:
            raise ValueError('settled launch quiescence changed committed '
                             'effect evidence.')
    if cursor.is_intent:
        assert cursor.intent_origin is not None
        raw = quiescence.effects[-1].canonical_value()
        intent = ProviderLaunchEffectClaimV1.from_value(raw['intent_origin'])
        resolver = ProviderLaunchEffectClaimV1.from_value(
            raw['resolution_origin'])
        if (raw['effect_sequence'] != cursor.current_intent_sequence or
                intent.canonical_bytes != cursor.intent_origin.canonical_bytes
                or resolver.canonical_bytes != intent.canonical_bytes or
                resolver.launch_attempt != attempt.attempt):
            raise ValueError('settled launch intent quiescence has the wrong '
                             'original-claim resolution.')
        _validate_attestation_cohort(intent.worker_attestation, context)
        resolution = ProviderLaunchNoEffectResolutionV1.from_value({
            'version': 1,
            'effect_sequence': raw['effect_sequence'],
            'effect_kind': raw['effect_kind'],
            'role': raw['role'],
            'intent_phase': raw['intent_phase'],
            'intent_cursor_sha256': cursor.sha256,
            'intent_origin': raw['intent_origin'],
            'resolution_origin': raw['resolution_origin'],
            'resolution': raw['resolution'],
            'evidence_sha256': raw['evidence_sha256'],
            'definitive_no_effect': raw['definitive_no_effect'],
        })
        resolution.validate_cursor(cursor, action.action_id, attempt.attempt,
                                   context)


def _validate_settled_no_io_prefix(
    action: kernel_actions.ActionRecord,
    predecessor: kernel_actions.AttemptRecord | None,
    attempt: kernel_actions.AttemptRecord,
    outcome: PersistedServeReplicaActionOutcomeV1,
    progress: ProviderLifecycleProgressV1 | None,
) -> None:
    prefix = outcome.launch_no_io_prefix
    should_have_prefix = (action.action_type
                          == kernel_actions.ActionKind.LAUNCH.value and
                          progress is None)
    if not should_have_prefix:
        if prefix is not None:
            raise ValueError('settled crossed/down outcome has a launch '
                             'no-I/O prefix.')
        return
    if prefix is None or attempt.settled_at is None:
        raise ValueError('settled revision-zero launch lacks its no-I/O '
                         'prefix/settlement time.')
    current = prefix.current_attempt
    assert current is not None
    if (attempt.provider_operation_id is not None or
            prefix.count != attempt.attempt or
            current.attempt != attempt.attempt or
            str(current.request_id) != attempt.request_id or
            current.request_input_sha256 != attempt.request_input_sha256 or
            current.request_terminal_state != attempt.request_terminal_state or
            current.settled_at != _timestamp_from_datetime(
                attempt.settled_at, name='attempt.settled_at')):
        raise ValueError('settled no-I/O prefix current projection differs '
                         'from the retained attempt.')
    previous = _predecessor_no_io_prefix(predecessor, attempt.attempt)
    expected_previous = None if previous is None else previous.prefix_sha256
    if prefix.previous_prefix_sha256 != expected_previous:
        raise ValueError('settled no-I/O prefix immediate predecessor link '
                         'differs.')


def _validate_settled_handler_outcome(
    action: kernel_actions.ActionRecord,
    predecessor: kernel_actions.AttemptRecord | None,
    attempt: kernel_actions.AttemptRecord,
    context: _ActionContext,
    progress: ProviderLifecycleProgressV1 | None,
) -> None:
    if (attempt.typed_outcome is None or attempt.typed_outcome_sha256
            != kernel_actions.canonical_sha256(attempt.typed_outcome) or
            attempt.request_terminal_state != 'SUCCEEDED' or
            attempt.settled_at is None):
        raise ValueError('settled handler attempt lacks its exact typed '
                         'outcome/terminal evidence.')
    outcome = ServeReplicaActionHandlerOutcomeV1.from_value(
        attempt.typed_outcome)
    result = outcome.provider_result
    _validate_persisted_provider_result_shape(result)
    if result.provider_operation_id != attempt.provider_operation_id:
        raise ValueError('settled nested provider operation ID differs from '
                         'the attempt journal.')
    cursor = None if progress is None else progress.cursor
    if cursor is not None and cursor.is_succeeded:
        if (attempt.provider_io_boundary
                is not kernel_actions.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS
           ):
            raise ValueError('settled provider success requires the '
                             'SUBMITTED_OR_AMBIGUOUS watermark.')
        if type(cursor) is ProviderLaunchProgressV1:
            expected_observation = cursor.success_observation
        else:
            assert type(cursor) is ProviderDownProgressV1
            expected_observation = cursor.absence_observation
        assert expected_observation is not None
        if (result.disposition is not ProviderResultDispositionV1.SUCCEEDED or
                result.observation is None or result.observation.canonical_bytes
                != expected_observation.canonical_bytes):
            raise ValueError('settled provider success differs from its exact '
                             'terminal cursor observation.')
    elif result.disposition is ProviderResultDispositionV1.SUCCEEDED:
        raise ValueError('settled provider success lacks a SUCCEEDED cursor.')
    if (result.disposition is ProviderResultDispositionV1.CANCELLED and
            outcome.supersession_quiescence is None):
        raise ValueError('handler-basis cancelled outcome lacks reducer '
                         'quiescence.')
    if type(cursor) is ProviderLaunchProgressV1:
        _validate_settled_quiescence(action, attempt, context, cursor, outcome)
    elif outcome.supersession_quiescence is not None:
        raise ValueError('down/revision-zero outcome contains launch '
                         'quiescence.')
    _validate_settled_no_io_prefix(action, predecessor, attempt, outcome,
                                   progress)


def _validate_settled_fallback_outcome(
    action: kernel_actions.ActionRecord,
    predecessor: kernel_actions.AttemptRecord | None,
    attempt: kernel_actions.AttemptRecord,
    context: _ActionContext,
) -> None:
    if (attempt.typed_outcome is None or
            attempt.typed_outcome_sha256 != kernel_actions.canonical_sha256(
                attempt.typed_outcome) or attempt.request_terminal_state
            not in ('SUCCEEDED', 'FAILED', 'CANCELLED') or
            attempt.settled_at is None):
        raise ValueError('settled fallback attempt lacks exact terminal '
                         'outcome evidence.')
    outcome = ServeReplicaActionRequestFallbackOutcomeV1.from_value(
        attempt.typed_outcome)
    evidence = outcome.evidence
    if (str(evidence.request_id) != attempt.request_id or
            evidence.attempt != attempt.attempt or
            evidence.request_terminal_state != attempt.request_terminal_state or
            evidence.provider_io_boundary is not attempt.provider_io_boundary or
            evidence.provider_progress_revision
            != attempt.provider_progress_revision or
            evidence.provider_progress_sha256
            != attempt.provider_progress_sha256 or
            evidence.provider_operation_id != attempt.provider_operation_id):
        raise ValueError('settled fallback evidence differs from the retained '
                         'attempt journal.')
    journal_class, parsed = _classify_request_terminal_journal_v1(
        action, predecessor, attempt, context)
    if journal_class != evidence.journal_class:
        raise ValueError('settled fallback journal classification changed.')
    result = outcome.provider_result
    _validate_fallback_provider_tuple(journal_class, result)
    if result.provider_operation_id != attempt.provider_operation_id:
        raise ValueError('settled fallback provider operation ID differs from '
                         'the attempt journal.')
    if journal_class == 'valid_succeeded':
        assert parsed is not None
        cursor = parsed.cursor
        if type(cursor) is ProviderLaunchProgressV1:
            observation = cursor.success_observation
        else:
            assert type(cursor) is ProviderDownProgressV1
            observation = cursor.absence_observation
        assert observation is not None
        if (result.observation is None or result.observation.canonical_bytes
                != observation.canonical_bytes):
            raise ValueError('settled fallback success observation differs '
                             'from its exact cursor checkpoint.')
    _validate_settled_no_io_prefix(action, predecessor, attempt, outcome,
                                   parsed)


def _terminal_progress(
    attempt: kernel_actions.AttemptRecord,
) -> ProviderLifecycleProgressV1 | None:
    if attempt.provider_progress is None:
        return None
    progress = ProviderLifecycleProgressV1.from_value(attempt.provider_progress)
    if (attempt.provider_progress_sha256 != progress.sha256 or
            attempt.provider_progress_revision <= 0):
        raise ValueError('terminal progress hash/revision differs from its '
                         'API006 envelope.')
    return progress


def _classify_request_terminal_journal_v1(
    _action: kernel_actions.ActionRecord,
    predecessor: kernel_actions.AttemptRecord | None,
    attempt: kernel_actions.AttemptRecord,
    context: _ActionContext,
) -> tuple[str, ProviderLifecycleProgressV1 | None]:
    """Classify one outer-bounded raw journal without trusting domain bytes."""

    if (attempt.provider_io_boundary
            is kernel_actions.ProviderIOBoundary.NOT_STARTED and
            attempt.provider_progress is None and
            attempt.provider_progress_sha256 is None and
            attempt.provider_progress_revision == 0 and
            attempt.provider_operation_id is None and
        (attempt.mutation_boundary is kernel_actions.MutationBoundary.SETTLED or
         attempt.mutation_boundary
         is kernel_actions.MutationBoundary.NOT_STARTED)):
        return 'not_started_empty', None
    if attempt.provider_progress is None:
        return 'invalid', None
    try:
        parsed = ProviderLifecycleProgressV1.from_value(
            attempt.provider_progress)
        if (attempt.provider_progress_sha256 != parsed.sha256 or
                attempt.provider_progress_revision <= 0):
            raise ValueError('progress hash/revision is crossed.')
        _validate_progress_attempt_binding(parsed, attempt)
        _validate_progress_operation_ids(parsed, attempt)
        parsed.validate_action_context(context)
        boundary = attempt.provider_io_boundary
        if boundary is kernel_actions.ProviderIOBoundary.NOT_STARTED:
            if (attempt.provider_progress_revision != 1 or
                    parsed.worker_attestation is not None or
                    not _is_exact_inherited_cursor(
                        parsed,
                        predecessor,
                        attempt,
                        allow_bound_attestation=False)):
                raise ValueError('invalid inherited journal.')
        elif boundary is kernel_actions.ProviderIOBoundary.INTENT_COMMITTED:
            if (parsed.worker_attestation is None or
                    not _is_admitted_intent_committed_cursor(
                        parsed, predecessor, attempt)):
                raise ValueError('invalid intent-committed journal.')
        elif boundary is (
                kernel_actions.ProviderIOBoundary.SUBMITTED_OR_AMBIGUOUS):
            if parsed.worker_attestation is None:
                raise ValueError('submitted journal lacks attestation.')
        else:
            raise ValueError('unsupported journal watermark.')
        if attempt.mutation_boundary is not kernel_actions.MutationBoundary.SETTLED:
            if attempt.mutation_boundary is not kernel_actions.MutationBoundary(
                    boundary.value):
                raise ValueError('crossed active journal watermarks.')
        if parsed.is_succeeded:
            if (boundary is not kernel_actions.ProviderIOBoundary.
                    SUBMITTED_OR_AMBIGUOUS or
                    parsed.worker_attestation is None or
                    parsed.worker_attestation.after is None):
                raise ValueError('succeeded cursor lacks post-effect proof.')
            return 'valid_succeeded', parsed
        return 'valid_nonterminal', parsed
    except (AssertionError, TypeError, ValueError):
        return 'invalid', None


def _attestation_can_complete(
    before: ProviderAuthorityWorkerAttemptAttestationV1,
    after: ProviderAuthorityWorkerAttemptAttestationV1,
) -> bool:
    """Return whether ``after`` is the same execution's null->after closure."""

    return (before.request_id == after.request_id and
            before.request_execution_generation
            == after.request_execution_generation and
            before.request_worker_id == after.request_worker_id and
            before.claimed_cursor_sha256 == after.claimed_cursor_sha256 and
            before.before.canonical_bytes == after.before.canonical_bytes and
            (before.after is None or
             (after.after is not None and
              before.after.canonical_bytes == after.after.canonical_bytes)))


def _request_finished_at(request: requests_lib.Request) -> str:
    finished_at = request.finished_at
    if type(finished_at) not in (int, float):
        raise ValueError('terminal request lacks its finish timestamp.')
    assert finished_at is not None
    try:
        value = datetime.datetime.fromtimestamp(float(finished_at),
                                                datetime.timezone.utc)
    except (OverflowError, OSError, TypeError, ValueError) as e:
        raise ValueError('terminal request finish timestamp is invalid.') from e
    return _timestamp_from_datetime(value, name='request.finished_at')


def _validate_terminal_result_binding(
    action: kernel_actions.ActionRecord,
    attempt: kernel_actions.AttemptRecord,
    request: requests_lib.Request,
    request_return: ServeReplicaActionRequestReturnV1,
    progress: ProviderLifecycleProgressV1 | None,
    action_context: _ActionContext,
) -> ServeReplicaActionHandlerTerminalResultV1:
    terminal = request_return.terminal_result
    if request.status is not requests_lib.RequestStatus.SUCCEEDED:
        raise ValueError('handler terminal reduction requires request '
                         'SUCCEEDED.')
    _request_finished_at(request)
    expected_request_id = kernel_actions.request_id_for_attempt(
        action.action_id, attempt.attempt)
    if (type(request.request_id) is not str or
            request.request_id != expected_request_id or
            terminal.action_id != action.action_id or
            terminal.action_kind.value != action.action_type or
            terminal.attempt != attempt.attempt or
            str(terminal.request_id) != expected_request_id or
            terminal.request_input_sha256 != attempt.request_input_sha256):
        raise ValueError('handler terminal result differs from the locked '
                         'action/attempt/request identity.')
    expected_handler = f'serve_resource_action_{terminal.action_kind.value}'
    if (type(request.handler_name) is not str or
            terminal.handler_name != expected_handler or
            request.handler_name != expected_handler):
        raise ValueError('handler terminal result differs from the locked '
                         'private handler.')
    request_generation = _positive_integer(request.execution_generation,
                                           name='request.execution_generation')
    if (terminal.request_execution_generation != request_generation or
            terminal.worker_attestation.request_execution_generation
            != request_generation):
        raise ValueError('handler terminal execution generation differs from '
                         'the locked request.')
    if type(request.worker_instance_id) is not str:
        raise ValueError('handler terminal worker differs from the locked '
                         'request worker.')
    request_worker_id = str(
        _uuid(request.worker_instance_id, name='request.worker_instance_id'))
    if (terminal.worker_attestation.request_worker_id != request_worker_id):
        raise ValueError('handler terminal worker differs from the locked '
                         'request worker.')
    _validate_attestation_cohort(terminal.worker_attestation, action_context)
    expected_progress_hash = (None if progress is None else progress.sha256)
    if terminal.final_provider_progress_sha256 != expected_progress_hash:
        raise ValueError('handler terminal progress hash differs from the '
                         'final API006 envelope.')
    if progress is None:
        if terminal.worker_attestation.claimed_cursor_sha256 is not None:
            raise ValueError('revision-zero handler attestation must claim a '
                             'null cursor.')
    else:
        attestation = progress.worker_attestation
        if attestation is None or not _attestation_can_complete(
                attestation, terminal.worker_attestation):
            raise ValueError('handler terminal worker attestation differs '
                             'from the final API006 execution.')
    return terminal


def _provider_result(
    disposition: ProviderResultDispositionV1,
    certainty: ProviderResultCertaintyV1,
    operation_id: str | None,
    *,
    code: str | None = None,
    retry_class: ProviderResultRetryClassV1 | None = None,
    retry_after_seconds: int | None = None,
    observation: ProviderLifecycleObservationV1 | None = None,
    message: str | None = None,
) -> ServeReplicaActionProviderResultV1:
    return ServeReplicaActionProviderResultV1(
        disposition=disposition,
        certainty=certainty,
        provider_operation_id=operation_id,
        provider_code=code,
        retry_class=retry_class,
        retry_after_seconds=retry_after_seconds,
        observation=observation,
        normalized_message=message)


def _expected_handler_provider_result(
    terminal: ServeReplicaActionHandlerTerminalResultV1,
    progress: ProviderLifecycleProgressV1 | None,
    operation_id: str | None,
    action_context: _ActionContext,
) -> tuple[str, ServeReplicaActionProviderResultV1]:
    cursor = None if progress is None else progress.cursor
    error = terminal.normalized_provider_error
    resolution = terminal.launch_no_effect_resolution
    if terminal.reduction_kind is HandlerReductionKindV1.SUPERSEDE_TO_DOWN:
        if (type(cursor) is not ProviderLaunchProgressV1 or
                cursor.phase is LaunchProgressPhaseV1.SUCCEEDED or
                error is not None):
            raise ValueError('supersede_to_down requires a nonsuccessful '
                             'launch cursor and null provider error.')
        if cursor.is_intent:
            if resolution is None:
                raise ValueError('superseded current intent requires its '
                                 'exact no-effect resolution.')
            resolution.validate_cursor(cursor, terminal.action_id,
                                       terminal.attempt, action_context)
            if (resolution.resolution_origin.request_execution_generation
                    != terminal.request_execution_generation or resolution.
                    resolution_origin.worker_attestation.request_worker_id
                    != terminal.worker_attestation.request_worker_id):
                raise ValueError('no-effect resolver differs from the '
                                 'terminal handler execution.')
            origin_attestation = resolution.resolution_origin.worker_attestation
            if resolution.resolution == 'call_not_entered':
                if (origin_attestation.canonical_bytes
                        != terminal.worker_attestation.canonical_bytes):
                    raise ValueError('call_not_entered requires the byte-equal '
                                     'terminal worker attestation.')
            elif (terminal.worker_attestation.after is None or
                  not _attestation_can_complete(origin_attestation,
                                                terminal.worker_attestation)):
                raise ValueError('definitive no-effect terminal attestation '
                                 'is not the same execution completion.')
        elif resolution is not None:
            raise ValueError('superseded E-only phase rejects a no-effect '
                             'resolution.')
        return ('Q',
                _provider_result(ProviderResultDispositionV1.CANCELLED,
                                 ProviderResultCertaintyV1.OBSERVED,
                                 operation_id))

    if terminal.reduction_kind is not HandlerReductionKindV1.DOMAIN:
        raise ValueError('handler reduction kind is unsupported.')
    if resolution is not None:
        raise ValueError('domain handler result rejects launch no-effect '
                         'resolution.')
    if cursor is not None and cursor.is_succeeded:
        if error is not None:
            raise ValueError('successful provider cursor rejects an error.')
        if terminal.worker_attestation.after is None:
            raise ValueError('successful provider cursor requires terminal '
                             'post-execution worker attestation.')
        if type(cursor) is ProviderLaunchProgressV1:
            observation = cursor.success_observation
        else:
            assert type(cursor) is ProviderDownProgressV1
            observation = cursor.absence_observation
        assert observation is not None
        return ('S',
                _provider_result(ProviderResultDispositionV1.SUCCEEDED,
                                 ProviderResultCertaintyV1.OBSERVED,
                                 operation_id,
                                 observation=observation))
    if error is None:
        raise ValueError('nonsuccessful domain result requires a normalized '
                         'provider error.')

    category = error.category.value
    current_intent = cursor is not None and cursor.is_intent
    if category in ('invalid_request', 'permission', 'conflict'):
        return ('B',
                _provider_result(ProviderResultDispositionV1.TERMINAL_ERROR,
                                 ProviderResultCertaintyV1.UNKNOWN,
                                 operation_id,
                                 code=error.provider_code,
                                 message=error.normalized_message))
    if current_intent or category == 'unknown':
        return ('U',
                _provider_result(
                    ProviderResultDispositionV1.UNCERTAIN,
                    ProviderResultCertaintyV1.UNKNOWN,
                    operation_id,
                    code=error.provider_code,
                    retry_class=(
                        ProviderResultRetryClassV1.OBSERVATION_REQUIRED),
                    retry_after_seconds=60,
                    message=error.normalized_message))
    retry_class = ProviderResultRetryClassV1(category)
    delay = min(
        error.retry_after_seconds
        if error.retry_after_seconds is not None else 60, 3600)
    return ('R',
            _provider_result(ProviderResultDispositionV1.RETRYABLE,
                             ProviderResultCertaintyV1.UNKNOWN,
                             operation_id,
                             code=error.provider_code,
                             retry_class=retry_class,
                             retry_after_seconds=delay,
                             message=error.normalized_message))


def _normalize_terminal_operation_id(
    terminal: ServeReplicaActionHandlerTerminalResultV1,
    attempt: kernel_actions.AttemptRecord,
) -> ServeReplicaActionProviderResultV1:
    handler_id = terminal.provider_result.provider_operation_id
    journal_id = attempt.provider_operation_id
    if handler_id is not None and handler_id != journal_id:
        raise ValueError('handler provider operation ID conflicts with the '
                         'claim-fenced attempt journal.')
    return terminal.provider_result.with_provider_operation_id(journal_id)


def _predecessor_no_io_prefix(
    predecessor: kernel_actions.AttemptRecord | None,
    expected_attempt: int,
) -> ServeLaunchNoIoPrefixV1 | None:
    _resource_action_attempt(expected_attempt, name='expected_attempt')
    if expected_attempt == 1:
        if predecessor is not None:
            raise ValueError('attempt one cannot have a predecessor.')
        return None
    if (predecessor is None or predecessor.attempt != expected_attempt - 1 or
            predecessor.mutation_boundary
            is not kernel_actions.MutationBoundary.SETTLED or
            predecessor.provider_io_boundary
            is not kernel_actions.ProviderIOBoundary.NOT_STARTED or
            predecessor.provider_progress is not None or
            predecessor.provider_progress_sha256 is not None or
            predecessor.provider_progress_revision != 0 or
            predecessor.provider_operation_id is not None or
            predecessor.request_terminal_state not in ('SUCCEEDED', 'FAILED',
                                                       'CANCELLED') or
            predecessor.settled_at is None or
            predecessor.typed_outcome is None or
            predecessor.typed_outcome_sha256 != kernel_actions.canonical_sha256(
                predecessor.typed_outcome)):
        raise ValueError('revision-zero retry lacks its exact settled '
                         'predecessor outcome.')
    outcome = _parse_persisted_outcome(predecessor.typed_outcome)
    if type(outcome) is ServeReplicaActionHandlerOutcomeV1:
        _validate_persisted_provider_result_shape(outcome.provider_result)
        quiescence = outcome.supersession_quiescence
    else:
        assert type(outcome) is ServeReplicaActionRequestFallbackOutcomeV1
        _validate_fallback_provider_tuple(outcome.evidence.journal_class,
                                          outcome.provider_result)
        if outcome.evidence.journal_class != 'not_started_empty':
            raise ValueError('revision-zero retry predecessor fallback class '
                             'is not P0.')
        quiescence = None
    if (outcome.provider_result.disposition
            not in (ProviderResultDispositionV1.RETRYABLE,
                    ProviderResultDispositionV1.UNCERTAIN,
                    ProviderResultDispositionV1.TERMINAL_ERROR) or
            outcome.provider_result.provider_operation_id is not None or
            quiescence is not None or outcome.launch_no_io_prefix is None):
        raise ValueError('revision-zero retry predecessor lacks its no-I/O '
                         'handler outcome.')
    prefix = outcome.launch_no_io_prefix
    current = prefix.current_attempt
    assert current is not None
    if (prefix.count != expected_attempt - 1 or
            current.attempt != predecessor.attempt or
            str(current.request_id) != predecessor.request_id or
            current.request_input_sha256 != predecessor.request_input_sha256 or
            current.request_terminal_state != predecessor.request_terminal_state
            or current.settled_at != _timestamp_from_datetime(
                predecessor.settled_at, name='predecessor.settled_at')):
        raise ValueError('predecessor no-I/O prefix count differs from its '
                         'exact retained attempt projection.')
    return prefix


def _launch_no_io_prefix(
    action: kernel_actions.ActionRecord,
    predecessor: kernel_actions.AttemptRecord | None,
    attempt: kernel_actions.AttemptRecord,
    progress: ProviderLifecycleProgressV1 | None,
    database_now: datetime.datetime,
    request_terminal_state: str,
) -> ServeLaunchNoIoPrefixV1 | None:
    if (action.action_type != kernel_actions.ActionKind.LAUNCH.value or
            progress is not None):
        return None
    if (attempt.provider_io_boundary
            is not kernel_actions.ProviderIOBoundary.NOT_STARTED or
            attempt.provider_progress_revision != 0 or
            attempt.provider_progress_sha256 is not None or
            attempt.provider_operation_id is not None):
        raise ValueError('null launch progress is not the exact revision-zero '
                         'no-I/O journal.')
    previous = _predecessor_no_io_prefix(predecessor, attempt.attempt)
    current = ServeLaunchNoIoAttemptProjectionV1(
        attempt=attempt.attempt,
        request_id=uuid.UUID(attempt.request_id),
        request_input_sha256=attempt.request_input_sha256,
        request_terminal_state=request_terminal_state,
        settled_at=_timestamp_from_datetime(database_now,
                                            name='reduction.database_now'))
    return ServeLaunchNoIoPrefixV1.append(previous, current)


def reduce_handler_terminal_result_v1(
    action: kernel_actions.ActionRecord,
    predecessor: kernel_actions.AttemptRecord | None,
    attempt: kernel_actions.AttemptRecord,
    context: kernel_actions.ReductionContext,
) -> kernel_actions.ActionReduction:
    """Pure exact S/R/U/B/Q reduction of one terminal handler return."""

    ServeProviderProgressContractV1.validate_attempt_snapshot(
        action, predecessor, attempt, None)
    request = context.terminal_request
    request_return = ServeReplicaActionRequestReturnV1.from_value(
        request.return_value)
    progress = _terminal_progress(attempt)
    action_context = _ActionContext.from_record(action)
    terminal = _validate_terminal_result_binding(action, attempt, request,
                                                 request_return, progress,
                                                 action_context)
    normalized_result = _normalize_terminal_operation_id(terminal, attempt)
    symbol, expected_result = _expected_handler_provider_result(
        terminal, progress, attempt.provider_operation_id, action_context)
    if normalized_result.canonical_bytes != expected_result.canonical_bytes:
        raise ValueError('handler provider result is not the exact tuple '
                         f'authorized by {symbol}.')

    quiescence = None
    if symbol == 'Q':
        quiescence = build_launch_supersession_quiescence_v1(
            action,
            attempt,
            request_terminal_state=request.status.value,
            active_claim=False,
            handler_terminal_result_sha256=request_return.
            terminal_result_sha256,
            request_settled_at=_request_finished_at(request),
            launch_no_effect_resolution=(
                None if terminal.launch_no_effect_resolution is None else
                terminal.launch_no_effect_resolution.canonical_value()))
    no_io_prefix = _launch_no_io_prefix(action, predecessor, attempt, progress,
                                        context.database_now,
                                        request.status.value)
    outcome = ServeReplicaActionHandlerOutcomeV1(
        handler_terminal_result_sha256=request_return.terminal_result_sha256,
        provider_result=expected_result,
        supersession_quiescence=quiescence,
        launch_no_io_prefix=no_io_prefix)
    value = outcome.canonical_value()

    if symbol == 'S':
        return kernel_actions.ActionReduction(
            kernel_state=kernel_actions.KernelState.TERMINAL,
            typed_outcome=value,
            result=value,
            terminal_disposition='succeeded')
    if symbol == 'Q':
        return kernel_actions.ActionReduction(
            kernel_state=kernel_actions.KernelState.TERMINAL,
            typed_outcome=value,
            result=value,
            terminal_disposition='SUPERSEDED_TO_DOWN')
    if symbol == 'B' or attempt.attempt == _MAX_RESOURCE_ACTION_ATTEMPT_V1:
        return kernel_actions.ActionReduction(
            kernel_state=kernel_actions.KernelState.BLOCKED,
            typed_outcome=value,
            result=value)
    assert symbol in ('R', 'U')
    assert expected_result.retry_after_seconds is not None
    return kernel_actions.ActionReduction(
        kernel_state=kernel_actions.KernelState.READY,
        typed_outcome=value,
        result=value,
        retry_after_seconds=expected_result.retry_after_seconds)


def reduce_request_terminal_fallback_v1(
    action: kernel_actions.ActionRecord,
    predecessor: kernel_actions.AttemptRecord | None,
    attempt: kernel_actions.AttemptRecord,
    context: kernel_actions.ReductionContext,
) -> kernel_actions.ActionReduction:
    """Pure exact P0/O/S/X reduction for a terminal request fallback."""

    action_context = _ActionContext.from_record(action)
    request = context.terminal_request
    expected_request_id = kernel_actions.request_id_for_attempt(
        action.action_id, attempt.attempt)
    expected_handler = f'serve_resource_action_{action.action_type}'
    if (attempt.action_id != action.action_id or
            attempt.attempt != action.current_attempt or
            attempt.request_id != expected_request_id or
            request.request_id != expected_request_id or
            request.handler_name != expected_handler):
        raise ValueError('fallback request/attempt identity differs from the '
                         'locked action.')
    if request.status not in requests_lib.RequestStatus.finished_status():
        raise ValueError('request-terminal fallback requires a terminal '
                         'request state.')
    if request.claim_token is not None:
        raise ValueError('request-terminal fallback requires no active claim.')
    finished_at = _request_finished_at(request)

    if request.status is requests_lib.RequestStatus.FAILED:
        fallback_reason = 'request_failed'
    elif request.status is requests_lib.RequestStatus.CANCELLED:
        fallback_reason = 'request_cancelled'
    elif request.return_value is None:
        fallback_reason = 'missing_handler_return'
    else:
        try:
            reduce_handler_terminal_result_v1(action, predecessor, attempt,
                                              context)
        except (TypeError, ValueError):
            fallback_reason = 'invalid_handler_return'
        else:
            raise ValueError('a valid handler terminal return must use the '
                             'handler-result reducer.')

    journal_class, parsed_progress = _classify_request_terminal_journal_v1(
        action, predecessor, attempt, action_context)
    evidence = ServeReplicaActionRequestFallbackEvidenceV1(
        request_id=uuid.UUID(attempt.request_id),
        attempt=attempt.attempt,
        fallback_reason=fallback_reason,
        request_terminal_state=request.status.value,
        request_finished_at=finished_at,
        journal_class=journal_class,
        provider_io_boundary=attempt.provider_io_boundary,
        provider_progress_revision=attempt.provider_progress_revision,
        provider_progress_sha256=attempt.provider_progress_sha256,
        provider_operation_id=attempt.provider_operation_id)

    if journal_class == 'not_started_empty':
        result = _provider_result(
            ProviderResultDispositionV1.RETRYABLE,
            ProviderResultCertaintyV1.OBSERVED,
            None,
            retry_class=ProviderResultRetryClassV1.TRANSIENT,
            retry_after_seconds=60)
    elif journal_class == 'valid_nonterminal':
        result = _provider_result(
            ProviderResultDispositionV1.UNCERTAIN,
            ProviderResultCertaintyV1.UNKNOWN,
            attempt.provider_operation_id,
            retry_class=ProviderResultRetryClassV1.OBSERVATION_REQUIRED,
            retry_after_seconds=60)
    elif journal_class == 'valid_succeeded':
        assert parsed_progress is not None
        cursor = parsed_progress.cursor
        if type(cursor) is ProviderLaunchProgressV1:
            observation = cursor.success_observation
        else:
            assert type(cursor) is ProviderDownProgressV1
            observation = cursor.absence_observation
        assert observation is not None
        result = _provider_result(ProviderResultDispositionV1.SUCCEEDED,
                                  ProviderResultCertaintyV1.OBSERVED,
                                  attempt.provider_operation_id,
                                  observation=observation)
    else:
        assert journal_class == 'invalid'
        result = _provider_result(ProviderResultDispositionV1.TERMINAL_ERROR,
                                  ProviderResultCertaintyV1.UNKNOWN,
                                  attempt.provider_operation_id)

    no_io_prefix = None
    if journal_class == 'not_started_empty':
        no_io_prefix = _launch_no_io_prefix(action, predecessor, attempt, None,
                                            context.database_now,
                                            request.status.value)
    outcome = ServeReplicaActionRequestFallbackOutcomeV1(
        evidence=evidence,
        provider_result=result,
        launch_no_io_prefix=no_io_prefix)
    value = outcome.canonical_value()
    if journal_class == 'valid_succeeded':
        return kernel_actions.ActionReduction(
            kernel_state=kernel_actions.KernelState.TERMINAL,
            typed_outcome=value,
            result=value,
            terminal_disposition='succeeded')
    if (journal_class == 'invalid' or
            attempt.attempt == _MAX_RESOURCE_ACTION_ATTEMPT_V1):
        return kernel_actions.ActionReduction(
            kernel_state=kernel_actions.KernelState.BLOCKED,
            typed_outcome=value,
            result=value)
    return kernel_actions.ActionReduction(
        kernel_state=kernel_actions.KernelState.READY,
        typed_outcome=value,
        result=value,
        retry_after_seconds=60)
