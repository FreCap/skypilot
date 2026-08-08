"""Pure, finite representability contracts for provider-authoritative M4.

This module owns the closed API006 size-enumeration boundary.  The checked-in
case inventory is data, never a program: every case is also present in the
sealed code dispatch below, and a caller cannot supply a selector, payload, or
callable.  Live callers provide one exact typed boundary root.  The enumerator
renders both the current and candidate-maximal modes and applies the unchanged
65,536-byte per-value ceiling.

The final V2 top-level artifact inventory and CI golden manifest are a later
one-way edge in the qualification graph.  Until a boundary root carries the
native V2 config-access reference, enumeration fails closed; V1 renderer
references are never accepted as provisional evidence.
"""

from __future__ import annotations

import dataclasses
import enum
import hashlib
import re
from typing import Any, ClassVar, Protocol, TypeAlias
import unicodedata
import uuid

from sky.serve import resource_action_authority as authority
from sky.serve import resource_action_cleanup_v2 as cleanup_v2
from sky.serve import resource_action_preflight_v2 as preflight_v2
from sky.serve import resource_action_progress as progress
from sky.serve import resource_action_renderer_v2 as renderer_v2
from sky.serve import resource_actions as actions
from sky.server.requests import resource_actions as kernel_actions

_MAX_VALUE_CANONICAL_BYTES = 65_536
_ACTION_SPEC_MAX_CANONICAL_BYTES = 60_000
_MAX_POSTGRES_BIGINT = 2**63 - 1
_MAX_RESOURCE_ACTION_ATTEMPT = 2**31 - 1
_CASE_INVENTORY_CONTRACT = (
    'provider_resource_action_representability_case_inventory_index_v2')
_CASE_INVENTORY_SHARD_CONTRACT = (
    'provider_resource_action_representability_case_inventory_shard_v2')
_CASE_INVENTORY_PROFILE = 'pod_cluster_v1'
_CASE_INVENTORY_SHARD_PATHS = (
    'sky/serve/resource_action_artifacts/provider_authority_v2/'
    'representability_case_inventory/000.json',
    'sky/serve/resource_action_artifacts/provider_authority_v2/'
    'representability_case_inventory/001.json',
)
_V2_BINDING_SCHEMA_PATH = (
    'sky/serve/resource_action_artifacts/kubernetes_renderer_v2/'
    'binding_schema.json')
_V2_CONFIG_ACCESS_INVENTORY_PATH = (
    'sky/serve/resource_action_artifacts/kubernetes_renderer_v2/'
    'config_access_inventory.json')
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
_CASE_ID_RE = re.compile(r'^[a-z0-9]+(?:[._-][a-z0-9]+)*$')


class ProviderResourceActionRepresentabilityError(ValueError):
    """One candidate is structurally invalid, unbounded, or oversized."""


class ProviderResourceActionRepresentabilityUnavailableError(
        ProviderResourceActionRepresentabilityError):
    """The exact native-V2 artifact precondition is not installed."""


class ProviderResourceActionRepresentabilityBoundaryV2(str, enum.Enum):
    COMPLETE_PREFLIGHT = 'complete_preflight'
    LINKED_ADMISSION = 'linked_admission'
    CLAIMED_EXECUTION = 'claimed_execution'
    PRE_IO = 'pre_io'
    TERMINALIZATION = 'terminalization'
    SETTLEMENT = 'settlement'
    OWNER_FENCED_TRANSITION = 'owner_fenced_transition'


class ProviderResourceActionRepresentabilityDispatchKindV2(str, enum.Enum):
    AUTHORITATIVE_ACTION = 'authoritative_action'
    SHADOW_CANDIDATE = 'shadow_candidate'


class ProviderResourceActionRepresentabilityPayloadKindV2(str, enum.Enum):
    """Canonical payload kinds covered by representability evidence."""

    PREFLIGHT_REQUEST = 'preflight_request'
    PREFLIGHT_RESPONSE = 'preflight_response'
    COHORT = 'cohort'
    WORKER_IDENTITY = 'worker_identity'
    ATTEMPT_ATTESTATION = 'attempt_attestation'
    RENDERER_INPUT = 'renderer_input'
    RENDERED_BODY = 'rendered_body'
    CLEANUP_TARGET = 'cleanup_target'
    EXECUTION_CAPSULE = 'execution_capsule'
    EXECUTION_CONFIG = 'execution_config'
    INVOCATION = 'invocation'
    PLAN = 'plan'
    ACTION_SPEC = 'action_spec'
    REQUEST_INPUT = 'request_input'
    DISPATCH_MEMBERSHIP = 'dispatch_membership'
    EXECUTION_AUTHORITY = 'execution_authority'
    TERMINAL_AUTHORITY_SELECTOR = 'terminal_authority_selector'
    AUTHORITY_FENCE_OPERATION = 'authority_fence_operation'
    PROGRESS = 'progress'
    NO_EFFECT_RESOLUTION = 'no_effect_resolution'
    REQUEST_RETURN = 'request_return'
    QUIESCENCE = 'quiescence'
    ACTION_OUTCOME = 'action_outcome'
    SHADOW_PROGRESS = 'shadow_progress'
    SHADOW_REQUEST_RETURN = 'shadow_request_return'
    SHADOW_TERMINAL_HISTORY = 'shadow_terminal_history'
    SHADOW_TERMINAL_COMMITMENT = 'shadow_terminal_commitment'
    SHADOW_SETTLEMENT_COMMITMENT = 'shadow_settlement_commitment'
    SHADOW_PROJECTION = 'shadow_projection'
    SHADOW_FALLBACK_EVIDENCE = 'shadow_fallback_evidence'
    SHADOW_OUTCOME = 'shadow_outcome'
    SHADOW_RETRY_DECISION = 'shadow_retry_decision'
    SHADOW_OBSERVATION = 'shadow_observation'
    SHADOW_EFFECT_TRACE = 'shadow_effect_trace'
    SHADOW_PARTIAL_DOWN_BASIS = 'shadow_partial_down_basis'


class ProviderResourceActionRepresentabilityModeV2(str, enum.Enum):
    CURRENT = 'current'
    CANDIDATE_MAXIMAL = 'candidate_maximal'


def _closed_object(value: Any, *, name: str,
                   keys: frozenset[str]) -> dict[str, Any]:
    if type(value) is not dict:
        raise TypeError(f'{name} must be an object.')
    if any(type(key) is not str for key in value):
        raise TypeError(f'{name} keys must be exact text.')
    if set(value) != keys:
        raise ValueError(f'{name} has unknown or missing fields.')
    # The shared serializer rejects floats, subclasses, non-NFC text, cycles,
    # excessive nesting/member counts, and values outside signed int64.
    encoded = actions.canonical_json_bytes(value)
    if not encoded:
        raise ValueError(f'{name} must have a nonempty canonical encoding.')
    return value


def _version_two(value: Any, *, name: str) -> int:
    if type(value) is not int or value != 2:
        raise ValueError(f'{name} must be integer 2.')
    return value


def _nonnegative_integer(value: Any, *, name: str) -> int:
    if type(value) is not int or value < 0 or value > _MAX_POSTGRES_BIGINT:
        raise ValueError(f'{name} must be a nonnegative signed-int64.')
    return value


def _positive_integer(value: Any, *, name: str) -> int:
    parsed = _nonnegative_integer(value, name=name)
    if parsed == 0:
        raise ValueError(f'{name} must be positive.')
    return parsed


def _action_attempt(value: Any, *, name: str) -> int:
    parsed = _positive_integer(value, name=name)
    if parsed > _MAX_RESOURCE_ACTION_ATTEMPT:
        raise ValueError(f'{name} exceeds the closed attempt domain.')
    return parsed


def _sha256(value: Any, *, name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f'{name} must be lowercase SHA-256 hex.')
    return value


def _optional_sha256(value: Any, *, name: str) -> str | None:
    return None if value is None else _sha256(value, name=name)


def _uuid(value: Any, *, name: str) -> uuid.UUID:
    if type(value) is uuid.UUID:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be canonical UUID text.')
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError(f'{name} must be a UUID.') from error
    if str(parsed) != value:
        raise ValueError(f'{name} must be lowercase hyphenated UUID text.')
    return parsed


def _action_kind(value: Any, *, name: str) -> kernel_actions.ActionKind:
    if type(value) is kernel_actions.ActionKind:
        return value
    if type(value) is not str:
        raise TypeError(f'{name} must be exact text.')
    try:
        return kernel_actions.ActionKind(value)
    except ValueError as error:
        raise ValueError(f'{name} must be launch or down.') from error


def _text(value: Any, *, name: str) -> str:
    if type(value) is not str:
        raise TypeError(f'{name} must be exact text.')
    try:
        encoded = value.encode('utf-8')
    except UnicodeEncodeError as error:
        raise ValueError(f'{name} must be valid UTF-8.') from error
    if (not encoded or len(encoded) > 1_024 or '\x00' in value or
            unicodedata.normalize('NFC', value) != value):
        raise ValueError(f'{name} must be bounded canonical text.')
    return value


def _same_bytes(left: Any, right: Any) -> bool:
    return left.canonical_bytes == right.canonical_bytes


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionRepresentabilityCaseV2(authority.CanonicalContract):
    """One fully expanded, code-owned finite representability case."""

    sequence: int
    case_id: str
    dispatch_kind: ProviderResourceActionRepresentabilityDispatchKindV2
    action_kind: kernel_actions.ActionKind
    boundary: ProviderResourceActionRepresentabilityBoundaryV2
    payload_kind: ProviderResourceActionRepresentabilityPayloadKindV2

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'sequence', 'case_id', 'dispatch_kind', 'action_kind', 'boundary',
        'payload_kind'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'sequence',
            _nonnegative_integer(self.sequence,
                                 name='representability case sequence'))
        case_id = _text(self.case_id, name='representability case ID')
        if _CASE_ID_RE.fullmatch(case_id) is None:
            raise ValueError('representability case ID is not a closed token.')
        object.__setattr__(self, 'case_id', case_id)
        try:
            dispatch_kind = (
                self.dispatch_kind if type(self.dispatch_kind)
                is ProviderResourceActionRepresentabilityDispatchKindV2 else
                ProviderResourceActionRepresentabilityDispatchKindV2(
                    self.dispatch_kind))
        except (TypeError, ValueError) as error:
            raise ValueError(
                'representability dispatch kind is unsupported.') from error
        object.__setattr__(self, 'dispatch_kind', dispatch_kind)
        object.__setattr__(
            self, 'action_kind',
            _action_kind(self.action_kind,
                         name='representability case action kind'))
        try:
            boundary = (self.boundary if type(self.boundary)
                        is ProviderResourceActionRepresentabilityBoundaryV2 else
                        ProviderResourceActionRepresentabilityBoundaryV2(
                            self.boundary))
            payload_kind = (
                self.payload_kind if type(self.payload_kind)
                is ProviderResourceActionRepresentabilityPayloadKindV2 else
                ProviderResourceActionRepresentabilityPayloadKindV2(
                    self.payload_kind))
        except (TypeError, ValueError) as error:
            raise ValueError(
                'representability case enum is unsupported.') from error
        object.__setattr__(self, 'boundary', boundary)
        object.__setattr__(self, 'payload_kind', payload_kind)

    @classmethod
    def from_value(cls,
                   value: Any) -> ProviderResourceActionRepresentabilityCaseV2:
        raw = _closed_object(value,
                             name='representability case',
                             keys=cls._KEYS)
        return cls(**raw)

    def canonical_value(self) -> dict[str, Any]:
        return {
            'sequence': self.sequence,
            'case_id': self.case_id,
            'dispatch_kind': self.dispatch_kind.value,
            'action_kind': self.action_kind.value,
            'boundary': self.boundary.value,
            'payload_kind': self.payload_kind.value,
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionRepresentabilityShardDescriptorV2:
    """One content-addressed fixed-path case-inventory shard."""

    ordinal: int
    first_case_sequence: int
    last_case_sequence: int
    case_count: int
    artifact: actions.ProviderRepoArtifactRefV1

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'ordinal', 'first_case_sequence', 'last_case_sequence', 'case_count',
        'artifact'
    })

    def __post_init__(self) -> None:
        ordinal = _nonnegative_integer(self.ordinal,
                                       name='representability shard ordinal')
        if ordinal >= len(_CASE_INVENTORY_SHARD_PATHS):
            raise ValueError('representability shard ordinal is unsupported.')
        object.__setattr__(self, 'ordinal', ordinal)
        if type(self.artifact) is not actions.ProviderRepoArtifactRefV1:
            raise TypeError(
                'representability shard artifact must be an exact reference.')
        if self.artifact.repo_path != _CASE_INVENTORY_SHARD_PATHS[ordinal]:
            raise ValueError('representability shard path is not fixed.')
        first = _nonnegative_integer(
            self.first_case_sequence,
            name='representability shard first sequence')
        last = _nonnegative_integer(self.last_case_sequence,
                                    name='representability shard last sequence')
        count = _positive_integer(self.case_count,
                                  name='representability shard case count')
        if last < first or last - first + 1 != count:
            raise ValueError('representability shard range/count differ.')
        if self.artifact.byte_size > _MAX_VALUE_CANONICAL_BYTES:
            raise ValueError('representability shard exceeds 65536 bytes.')

    @classmethod
    def from_value(
            cls, value: Any
    ) -> ProviderResourceActionRepresentabilityShardDescriptorV2:
        raw = _closed_object(value,
                             name='representability shard descriptor',
                             keys=cls._KEYS)
        return cls(ordinal=raw['ordinal'],
                   first_case_sequence=raw['first_case_sequence'],
                   last_case_sequence=raw['last_case_sequence'],
                   case_count=raw['case_count'],
                   artifact=actions.ProviderRepoArtifactRefV1.from_value(
                       raw['artifact']))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'ordinal': self.ordinal,
            'first_case_sequence': self.first_case_sequence,
            'last_case_sequence': self.last_case_sequence,
            'case_count': self.case_count,
            'artifact': self.artifact.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionRepresentabilityCaseInventoryIndexV2(
        authority.CanonicalContract):
    """The small fixed two-shard case-inventory index."""

    version: int
    contract: str
    profile: str
    shards: tuple[ProviderResourceActionRepresentabilityShardDescriptorV2, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'contract', 'profile', 'shards'})

    def __post_init__(self) -> None:
        _version_two(self.version,
                     name='representability inventory index version')
        if self.contract != _CASE_INVENTORY_CONTRACT:
            raise ValueError('representability inventory contract is invalid.')
        if self.profile != _CASE_INVENTORY_PROFILE:
            raise ValueError('representability inventory profile is invalid.')
        if (type(self.shards) is not tuple or
                len(self.shards) != len(_CASE_INVENTORY_SHARD_PATHS) or any(
                    type(shard) is
                    not ProviderResourceActionRepresentabilityShardDescriptorV2
                    for shard in self.shards)):
            raise TypeError('representability index must contain exactly two '
                            'typed shards.')
        if tuple(shard.ordinal for shard in self.shards) != tuple(
                range(len(self.shards))):
            raise ValueError('representability shard ordinals are not ordered.')
        expected_first = 0
        for shard in self.shards:
            if shard.first_case_sequence != expected_first:
                raise ValueError('representability shard ranges are not '
                                 'contiguous from zero.')
            expected_first = shard.last_case_sequence + 1
        if len({shard.artifact.repo_path for shard in self.shards
               }) != len(self.shards):
            raise ValueError('representability shard paths are not unique.')

    @classmethod
    def from_value(
        cls, value: Any
    ) -> ProviderResourceActionRepresentabilityCaseInventoryIndexV2:
        raw = _closed_object(value,
                             name='representability case inventory index',
                             keys=cls._KEYS)
        if type(raw['shards']) is not list:
            raise TypeError('representability index shards must be a list.')
        return cls(version=raw['version'],
                   contract=raw['contract'],
                   profile=raw['profile'],
                   shards=tuple(
                       ProviderResourceActionRepresentabilityShardDescriptorV2.
                       from_value(item) for item in raw['shards']))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'contract': _CASE_INVENTORY_CONTRACT,
            'profile': _CASE_INVENTORY_PROFILE,
            'shards': [shard.canonical_value() for shard in self.shards],
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionRepresentabilityCaseInventoryShardV2:
    """One independently bounded explicit case-inventory shard."""

    version: int
    contract: str
    profile: str
    ordinal: int
    cases: tuple[ProviderResourceActionRepresentabilityCaseV2, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'contract', 'profile', 'ordinal', 'cases'})

    def __post_init__(self) -> None:
        _version_two(self.version,
                     name='representability inventory shard version')
        if self.contract != _CASE_INVENTORY_SHARD_CONTRACT:
            raise ValueError('representability shard contract is invalid.')
        if self.profile != _CASE_INVENTORY_PROFILE:
            raise ValueError('representability shard profile is invalid.')
        ordinal = _nonnegative_integer(self.ordinal,
                                       name='representability shard ordinal')
        if ordinal >= len(_CASE_INVENTORY_SHARD_PATHS):
            raise ValueError('representability shard ordinal is unsupported.')
        object.__setattr__(self, 'ordinal', ordinal)
        if (type(self.cases) is not tuple or not self.cases or any(
                type(case) is not ProviderResourceActionRepresentabilityCaseV2
                for case in self.cases)):
            raise TypeError('representability shard cases must be a nonempty '
                            'exact typed tuple.')
        sequences = tuple(case.sequence for case in self.cases)
        if sequences != tuple(range(sequences[0], sequences[-1] + 1)):
            raise ValueError('representability shard sequences are not '
                             'contiguous.')
        if len({case.case_id for case in self.cases}) != len(self.cases):
            raise ValueError('representability shard case IDs are not unique.')
        if len(self.canonical_bytes) + 1 > _MAX_VALUE_CANONICAL_BYTES:
            raise ValueError('representability shard exceeds 65536 bytes.')

    @classmethod
    def from_value(
        cls, value: Any
    ) -> ProviderResourceActionRepresentabilityCaseInventoryShardV2:
        raw = _closed_object(value,
                             name='representability case inventory shard',
                             keys=cls._KEYS)
        if type(raw['cases']) is not list:
            raise TypeError('representability shard cases must be a list.')
        return cls(
            version=raw['version'],
            contract=raw['contract'],
            profile=raw['profile'],
            ordinal=raw['ordinal'],
            cases=tuple(
                ProviderResourceActionRepresentabilityCaseV2.from_value(item)
                for item in raw['cases']))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'contract': _CASE_INVENTORY_SHARD_CONTRACT,
            'profile': _CASE_INVENTORY_PROFILE,
            'ordinal': self.ordinal,
            'cases': [case.canonical_value() for case in self.cases],
        }

    @property
    def canonical_bytes(self) -> bytes:
        return actions.canonical_json_bytes(self.canonical_value())


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionRepresentabilityCaseInventoryV2:
    """The sole finite case cardinality and ordering contract."""

    version: int
    contract: str
    profile: str
    cases: tuple[ProviderResourceActionRepresentabilityCaseV2, ...]

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'version', 'contract', 'profile', 'cases'})

    def __post_init__(self) -> None:
        _version_two(self.version, name='representability inventory version')
        if self.contract != _CASE_INVENTORY_CONTRACT:
            raise ValueError('representability inventory contract is invalid.')
        if self.profile != _CASE_INVENTORY_PROFILE:
            raise ValueError('representability inventory profile is invalid.')
        if (type(self.cases) is not tuple or not self.cases or any(
                type(case) is not ProviderResourceActionRepresentabilityCaseV2
                for case in self.cases)):
            raise TypeError('representability inventory cases must be a '
                            'nonempty exact typed tuple.')
        if tuple(case.sequence for case in self.cases) != tuple(
                range(len(self.cases))):
            raise ValueError('representability case sequences are not '
                             'contiguous from zero.')
        case_ids = tuple(case.case_id for case in self.cases)
        if len(case_ids) != len(set(case_ids)):
            raise ValueError('representability case IDs are not unique.')
        # Reject every open-ended artifact shorthand even when embedded in an
        # otherwise token-shaped ID.  The artifact must hold concrete rows.
        forbidden = ('regex', 'range', 'wildcard', 'all_enum', 'cartesian')
        if any(
                any(marker in case.case_id
                    for marker in forbidden)
                for case in self.cases):
            raise ValueError('representability inventory contains an implicit '
                             'case-set placeholder.')
        _ = self.canonical_bytes

    @classmethod
    def from_value(
            cls, value: Any
    ) -> ProviderResourceActionRepresentabilityCaseInventoryV2:
        raw = _closed_object(value,
                             name='representability case inventory',
                             keys=cls._KEYS)
        raw_cases = raw['cases']
        if type(raw_cases) is not list:
            raise TypeError('representability inventory cases must be a list.')
        return cls(
            version=raw['version'],
            contract=raw['contract'],
            profile=raw['profile'],
            cases=tuple(
                ProviderResourceActionRepresentabilityCaseV2.from_value(item)
                for item in raw_cases))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'contract': _CASE_INVENTORY_CONTRACT,
            'profile': _CASE_INVENTORY_PROFILE,
            'cases': [case.canonical_value() for case in self.cases],
        }

    @property
    def canonical_bytes(self) -> bytes:
        return actions.canonical_json_bytes(self.canonical_value())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @property
    def projected_case_tuple(
        self,) -> tuple[tuple[str, str, str, str, str], ...]:
        return tuple(
            (case.case_id, case.dispatch_kind.value, case.action_kind.value,
             case.boundary.value, case.payload_kind.value)
            for case in self.cases)


class _CompositeCanonicalContract:
    """Canonical helpers for bounded-child composites larger than one value."""

    def canonical_value(self) -> dict[str, Any]:
        raise NotImplementedError

    @property
    def canonical_bytes(self) -> bytes:
        return actions.canonical_json_bytes(self.canonical_value())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


def _accepted_membership_from_value(
    value: Any,) -> authority.ProviderAuthorityWorkerAcceptedMembershipV2:
    raw = _closed_object(value,
                         name='accepted authority-worker membership V2',
                         keys=frozenset({
                             'version', 'registration',
                             'registration_set_revision',
                             'registration_set_sha256', 'lease'
                         }))
    return authority.ProviderAuthorityWorkerAcceptedMembershipV2(
        version=raw['version'],
        registration=(
            authority.ProviderAuthorityWorkerRegistrationV2.from_value(
                raw['registration'])),
        registration_set_revision=raw['registration_set_revision'],
        registration_set_sha256=raw['registration_set_sha256'],
        lease=authority.ProviderAuthorityWorkerLeaseV1.from_value(raw['lease']))


def _accepted_memberships(
    value: Any,
) -> tuple[authority.ProviderAuthorityWorkerAcceptedMembershipV2,
           authority.ProviderAuthorityWorkerAcceptedMembershipV2]:
    if type(value) is not tuple or len(value) != 2 or any(
            type(item)
            is not authority.ProviderAuthorityWorkerAcceptedMembershipV2
            for item in value):
        raise TypeError('accepted memberships must be exactly two typed '
                        'members.')
    pod_uids = tuple(str(item.registration.worker.pod_uid) for item in value)
    if pod_uids != tuple(sorted(pod_uids)) or len(set(pod_uids)) != 2:
        raise ValueError('accepted memberships must be unique and Pod-UID '
                         'sorted.')
    if (value[0].registration_set_revision != value[1].registration_set_revision
            or value[0].registration_set_sha256
            != value[1].registration_set_sha256):
        raise ValueError('accepted memberships disagree on their exact set.')
    return value


def _accepted_memberships_from_value(
    value: Any,
) -> tuple[authority.ProviderAuthorityWorkerAcceptedMembershipV2,
           authority.ProviderAuthorityWorkerAcceptedMembershipV2]:
    if type(value) is not list or len(value) != 2:
        raise TypeError('accepted memberships must be an exact two-item list.')
    return _accepted_memberships(
        tuple(_accepted_membership_from_value(item) for item in value))


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionLaunchRepresentabilityConstructionV2(
        _CompositeCanonicalContract):
    """Canonical launch construction used by representability fixtures."""

    action_kind: kernel_actions.ActionKind
    renderer_input: renderer_v2.ProviderKubernetesRendererInputV2
    execution_capsule: actions.ProviderKubernetesExecutionCapsuleV2

    _KEYS: ClassVar[frozenset[str]] = frozenset(
        {'action_kind', 'renderer_input', 'execution_capsule'})

    def __post_init__(self) -> None:
        if _action_kind(self.action_kind,
                        name='launch construction action kind') is not (
                            kernel_actions.ActionKind.LAUNCH):
            raise ValueError('launch construction action kind is not launch.')
        object.__setattr__(self, 'action_kind',
                           kernel_actions.ActionKind.LAUNCH)
        if type(self.renderer_input) is not (
                renderer_v2.ProviderKubernetesRendererInputV2):
            raise TypeError('launch construction renderer input is invalid.')
        if type(self.execution_capsule) is not (
                actions.ProviderKubernetesExecutionCapsuleV2):
            raise TypeError('launch construction capsule is invalid.')
        seed = self.renderer_input.seed
        capsule = self.execution_capsule
        comparisons = (
            (seed.executor_cohort, capsule.executor_cohort),
            (seed.config_projection, capsule.config_projection),
            (seed.scope, capsule.scope),
            (seed.principals, capsule.principals),
            (seed.request_identity, capsule.request_identity),
            (seed.resources, capsule.resources),
            (seed.renderer, capsule.renderer),
            (seed.post_provision, capsule.post_provision),
            (seed.endpoint, capsule.endpoint),
            (seed.scheduling, capsule.scheduling),
            (seed.storage, capsule.storage),
            (seed.metadata, capsule.metadata),
            (seed.security, capsule.security),
            (seed.topology, capsule.topology),
            (seed.mutation_contract, capsule.mutation_contract),
        )
        if any(not _same_bytes(left, right) for left, right in comparisons):
            raise ValueError('launch construction capsule differs from its '
                             'native renderer input.')

    @classmethod
    def from_value(
        cls, value: Any
    ) -> ProviderResourceActionLaunchRepresentabilityConstructionV2:
        raw = _closed_object(value,
                             name='launch representability construction',
                             keys=cls._KEYS)
        return cls(action_kind=raw['action_kind'],
                   renderer_input=(
                       renderer_v2.ProviderKubernetesRendererInputV2.from_value(
                           raw['renderer_input'])),
                   execution_capsule=(
                       actions.ProviderKubernetesExecutionCapsuleV2.from_value(
                           raw['execution_capsule'])))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'action_kind': 'launch',
            'renderer_input': self.renderer_input.canonical_value(),
            'execution_capsule': self.execution_capsule.canonical_value(),
        }


CleanupRederivationInputV2: TypeAlias = (
    cleanup_v2.ProviderKubernetesCleanupRederivationInputV2)


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionDownRepresentabilityConstructionV2(
        _CompositeCanonicalContract):
    """Canonical down construction used by representability fixtures."""

    action_kind: kernel_actions.ActionKind
    capsule_input: renderer_v2.ProviderKubernetesDownExecutionCapsuleInputV2
    cleanup_rederivation_input: CleanupRederivationInputV2
    rederived_cleanup_target: actions.ProviderKubernetesCleanupTargetV1
    execution_capsule: actions.ProviderKubernetesDownExecutionCapsuleV2

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'action_kind', 'capsule_input', 'cleanup_rederivation_input',
        'rederived_cleanup_target', 'execution_capsule'
    })

    def __post_init__(self) -> None:
        if _action_kind(self.action_kind,
                        name='down construction action kind') is not (
                            kernel_actions.ActionKind.DOWN):
            raise ValueError('down construction action kind is not down.')
        object.__setattr__(self, 'action_kind', kernel_actions.ActionKind.DOWN)
        if type(self.capsule_input) is not (
                renderer_v2.ProviderKubernetesDownExecutionCapsuleInputV2):
            raise TypeError('down construction capsule input is invalid.')
        if type(self.cleanup_rederivation_input) not in (
                cleanup_v2.
                ProviderKubernetesCompletedCleanupRederivationInputV2,
                cleanup_v2.ProviderKubernetesPartialCleanupRederivationInputV2):
            raise TypeError('down cleanup-rederivation input is invalid.')
        if type(self.rederived_cleanup_target) is not (
                actions.ProviderKubernetesCleanupTargetV1):
            raise TypeError('down rederived cleanup target is invalid.')
        if type(self.execution_capsule) is not (
                actions.ProviderKubernetesDownExecutionCapsuleV2):
            raise TypeError('down execution capsule is invalid.')
        actual = cleanup_v2.rederive_provider_kubernetes_cleanup_target_v2(
            self.cleanup_rederivation_input)
        if not _same_bytes(actual, self.rederived_cleanup_target):
            raise ValueError('down cleanup target differs from sole '
                             'rederivation output.')
        if not _same_bytes(self.execution_capsule.cleanup_target,
                           self.rederived_cleanup_target):
            raise ValueError('down capsule target differs from rederivation.')
        capsule_input = self.capsule_input
        capsule = self.execution_capsule
        comparisons = (
            (capsule_input.executor_cohort, capsule.executor_cohort),
            (capsule_input.config_projection, capsule.config_projection),
            (capsule_input.scope, capsule.scope),
            (capsule_input.principals, capsule.principals),
            (capsule_input.mutation_contract, capsule.mutation_contract),
        )
        if any(not _same_bytes(left, right) for left, right in comparisons):
            raise ValueError('down capsule differs from its native input.')

    @classmethod
    def from_value(
            cls, value: Any
    ) -> ProviderResourceActionDownRepresentabilityConstructionV2:
        raw = _closed_object(value,
                             name='down representability construction',
                             keys=cls._KEYS)
        rederivation = raw['cleanup_rederivation_input']
        if type(rederivation) is not dict:
            raise TypeError(
                'down cleanup-rederivation input must be an object.')
        source = rederivation.get('source')
        if source == 'completed_launch':
            parsed_rederivation: CleanupRederivationInputV2 = (
                cleanup_v2.ProviderKubernetesCompletedCleanupRederivationInputV2
                .from_value(rederivation))
        elif source == 'partial_launch_cleanup':
            parsed_rederivation = (
                cleanup_v2.ProviderKubernetesPartialCleanupRederivationInputV2.
                from_value(rederivation))
        else:
            raise ValueError('down cleanup-rederivation source is unsupported.')
        return cls(
            action_kind=raw['action_kind'],
            capsule_input=(
                renderer_v2.ProviderKubernetesDownExecutionCapsuleInputV2.
                from_value(raw['capsule_input'])),
            cleanup_rederivation_input=parsed_rederivation,
            rederived_cleanup_target=(
                actions.ProviderKubernetesCleanupTargetV1.from_value(
                    raw['rederived_cleanup_target'])),
            execution_capsule=(
                actions.ProviderKubernetesDownExecutionCapsuleV2.from_value(
                    raw['execution_capsule'])))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'action_kind': 'down',
            'capsule_input': self.capsule_input.canonical_value(),
            'cleanup_rederivation_input':
                self.cleanup_rederivation_input.canonical_value(),
            'rederived_cleanup_target':
                self.rederived_cleanup_target.canonical_value(),
            'execution_capsule': self.execution_capsule.canonical_value(),
        }


ProviderResourceActionRepresentabilityConstructionV2 = (
    ProviderResourceActionLaunchRepresentabilityConstructionV2 |
    ProviderResourceActionDownRepresentabilityConstructionV2)


def _construction_from_value(
    value: Any,) -> ProviderResourceActionRepresentabilityConstructionV2:
    if type(value) is not dict:
        raise TypeError('representability construction must be an object.')
    kind = _action_kind(value.get('action_kind'),
                        name='representability construction action kind')
    if kind is kernel_actions.ActionKind.LAUNCH:
        return (ProviderResourceActionLaunchRepresentabilityConstructionV2.
                from_value(value))
    return ProviderResourceActionDownRepresentabilityConstructionV2.from_value(
        value)


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionRequestTerminalSnapshotV2(
        _CompositeCanonicalContract):
    """Terminal request state captured for deterministic reduction."""

    request_terminal_state: str
    request_finished_at: str
    active_claim: bool
    request_execution_generation: int
    request_worker_id: None
    handler_name: str
    request_return: progress.ServeReplicaActionRequestReturnV1 | None
    request_return_sha256: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'request_terminal_state', 'request_finished_at', 'active_claim',
        'request_execution_generation', 'request_worker_id', 'handler_name',
        'request_return', 'request_return_sha256'
    })

    def __post_init__(self) -> None:
        if self.request_terminal_state not in ('SUCCEEDED', 'FAILED',
                                               'CANCELLED'):
            raise ValueError('terminal snapshot state is unsupported.')
        # Reuse the timestamp validator on an exact existing closed contract.
        if (type(self.request_finished_at) is not str or re.fullmatch(
                r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:'
                r'[0-9]{2}\.[0-9]{6}Z', self.request_finished_at) is None):
            raise ValueError('terminal snapshot timestamp is not canonical.')
        if type(self.active_claim) is not bool or self.active_claim:
            raise ValueError('terminal snapshot active_claim must be false.')
        if (type(self.request_execution_generation) is not int or
                self.request_execution_generation not in (0, 1)):
            raise ValueError('terminal snapshot generation must be 0 or 1.')
        if self.request_worker_id is not None:
            raise ValueError('terminal snapshot request worker must be null.')
        if self.handler_name not in ('serve_resource_action_launch',
                                     'serve_resource_action_down'):
            raise ValueError('terminal snapshot handler is unsupported.')
        if (self.request_return is not None and type(self.request_return)
                is not progress.ServeReplicaActionRequestReturnV1):
            raise TypeError('terminal snapshot request return is invalid.')
        return_hash = _optional_sha256(
            self.request_return_sha256,
            name='terminal snapshot request-return hash')
        object.__setattr__(self, 'request_return_sha256', return_hash)
        if (self.request_return is None) != (return_hash is None):
            raise ValueError('terminal snapshot return and hash presence '
                             'differ.')
        if self.request_return is not None and return_hash != (
                self.request_return.sha256):
            raise ValueError('terminal snapshot return hash differs from its '
                             'complete preimage.')

    @classmethod
    def from_value(
            cls, value: Any) -> ProviderResourceActionRequestTerminalSnapshotV2:
        raw = _closed_object(value,
                             name='request terminal snapshot V2',
                             keys=cls._KEYS)
        return cls(
            request_terminal_state=raw['request_terminal_state'],
            request_finished_at=raw['request_finished_at'],
            active_claim=raw['active_claim'],
            request_execution_generation=raw['request_execution_generation'],
            request_worker_id=raw['request_worker_id'],
            handler_name=raw['handler_name'],
            request_return=(
                None if raw['request_return'] is None else
                progress.ServeReplicaActionRequestReturnV1.from_value(
                    raw['request_return'])),
            request_return_sha256=raw['request_return_sha256'])

    def canonical_value(self) -> dict[str, Any]:
        return {
            'request_terminal_state': self.request_terminal_state,
            'request_finished_at': self.request_finished_at,
            'active_claim': False,
            'request_execution_generation': self.request_execution_generation,
            'request_worker_id': None,
            'handler_name': self.handler_name,
            'request_return': (None if self.request_return is None else
                               self.request_return.canonical_value()),
            'request_return_sha256': self.request_return_sha256,
        }


PersistedActionOutcomeV1: TypeAlias = (
    progress.ServeReplicaActionHandlerOutcomeV1 |
    progress.ServeReplicaActionDirectNoEffectOutcomeV1 |
    progress.ServeReplicaActionRequestFallbackOutcomeV1)


def _persisted_outcome_from_value(value: Any) -> PersistedActionOutcomeV1:
    return progress.serve_replica_action_outcome_from_value_v1(value)


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionReducerAttemptSnapshotV2(_CompositeCanonicalContract
                                                    ):
    """One durable attempt snapshot consumed by the V2 reducer."""

    attempt: int
    request_id: uuid.UUID
    request_terminal_snapshot: (ProviderResourceActionRequestTerminalSnapshotV2
                                | None)
    mutation_boundary: str
    provider_io_boundary: str
    provider_progress_revision: int
    provider_progress: progress.ProviderLifecycleProgressV1 | None
    provider_progress_sha256: str | None
    provider_operation_id: str | None
    typed_outcome: PersistedActionOutcomeV1 | None
    typed_outcome_sha256: str | None
    settled_at: str | None

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'attempt', 'request_id', 'request_terminal_snapshot',
        'mutation_boundary', 'provider_io_boundary',
        'provider_progress_revision', 'provider_progress',
        'provider_progress_sha256', 'provider_operation_id', 'typed_outcome',
        'typed_outcome_sha256', 'settled_at'
    })

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'attempt',
            _action_attempt(self.attempt, name='reducer snapshot attempt'))
        object.__setattr__(
            self, 'request_id',
            _uuid(self.request_id, name='reducer snapshot request ID'))
        if (self.request_terminal_snapshot is not None and
                type(self.request_terminal_snapshot)
                is not ProviderResourceActionRequestTerminalSnapshotV2):
            raise TypeError('reducer request terminal snapshot is invalid.')
        if self.provider_io_boundary not in ('NOT_STARTED', 'INTENT_COMMITTED',
                                             'SUBMITTED_OR_AMBIGUOUS'):
            raise ValueError('reducer provider-I/O boundary is unsupported.')
        if self.mutation_boundary not in ('NOT_STARTED', 'INTENT_COMMITTED',
                                          'SUBMITTED_OR_AMBIGUOUS', 'SETTLED'):
            raise ValueError('reducer mutation boundary is unsupported.')
        if (self.mutation_boundary != 'SETTLED' and
                self.mutation_boundary != self.provider_io_boundary):
            raise ValueError('unsettled mutation/provider-I/O boundaries '
                             'differ.')
        revision = _nonnegative_integer(
            self.provider_progress_revision,
            name='reducer snapshot progress revision')
        object.__setattr__(self, 'provider_progress_revision', revision)
        if (self.provider_progress is not None and type(self.provider_progress)
                is not progress.ProviderLifecycleProgressV1):
            raise TypeError('reducer provider progress is invalid.')
        progress_hash = _optional_sha256(self.provider_progress_sha256,
                                         name='reducer snapshot progress hash')
        object.__setattr__(self, 'provider_progress_sha256', progress_hash)
        if revision == 0:
            if self.provider_progress is not None or progress_hash is not None:
                raise ValueError('revision-zero reducer progress must be null.')
        elif (self.provider_progress is None or
              progress_hash != self.provider_progress.sha256):
            raise ValueError('reducer progress/hash/revision tuple differs.')
        if self.provider_operation_id is not None:
            object.__setattr__(
                self, 'provider_operation_id',
                _text(self.provider_operation_id,
                      name='reducer provider operation ID'))
        if (self.typed_outcome is not None and type(self.typed_outcome)
                not in (progress.ServeReplicaActionHandlerOutcomeV1,
                        progress.ServeReplicaActionDirectNoEffectOutcomeV1,
                        progress.ServeReplicaActionRequestFallbackOutcomeV1)):
            raise TypeError('reducer typed outcome is invalid.')
        outcome_hash = _optional_sha256(self.typed_outcome_sha256,
                                        name='reducer snapshot outcome hash')
        object.__setattr__(self, 'typed_outcome_sha256', outcome_hash)
        if (self.typed_outcome is None) != (outcome_hash is None):
            raise ValueError('reducer outcome/hash presence differs.')
        if self.typed_outcome is not None and outcome_hash != (
                self.typed_outcome.sha256):
            raise ValueError('reducer outcome hash differs from its preimage.')
        if self.settled_at is not None and (type(
                self.settled_at) is not str or re.fullmatch(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2}T'
                    r'[0-9]{2}:[0-9]{2}:[0-9]{2}\.'
                    r'[0-9]{6}Z', self.settled_at) is None):
            raise ValueError('reducer settled timestamp is not canonical.')
        settled = self.mutation_boundary == 'SETTLED'
        if settled != (self.settled_at is not None and
                       self.typed_outcome is not None):
            raise ValueError('reducer settlement/outcome tuple is crossed.')

    @classmethod
    def from_value(
            cls, value: Any) -> ProviderResourceActionReducerAttemptSnapshotV2:
        raw = _closed_object(value,
                             name='reducer attempt snapshot V2',
                             keys=cls._KEYS)
        return cls(
            attempt=raw['attempt'],
            request_id=raw['request_id'],
            request_terminal_snapshot=(
                None if raw['request_terminal_snapshot'] is None else
                ProviderResourceActionRequestTerminalSnapshotV2.from_value(
                    raw['request_terminal_snapshot'])),
            mutation_boundary=raw['mutation_boundary'],
            provider_io_boundary=raw['provider_io_boundary'],
            provider_progress_revision=raw['provider_progress_revision'],
            provider_progress=(None if raw['provider_progress'] is None else
                               progress.ProviderLifecycleProgressV1.from_value(
                                   raw['provider_progress'])),
            provider_progress_sha256=raw['provider_progress_sha256'],
            provider_operation_id=raw['provider_operation_id'],
            typed_outcome=(None if raw['typed_outcome'] is None else
                           _persisted_outcome_from_value(raw['typed_outcome'])),
            typed_outcome_sha256=raw['typed_outcome_sha256'],
            settled_at=raw['settled_at'])

    def canonical_value(self) -> dict[str, Any]:
        return {
            'attempt': self.attempt,
            'request_id': str(self.request_id),
            'request_terminal_snapshot':
                (None if self.request_terminal_snapshot is None else
                 self.request_terminal_snapshot.canonical_value()),
            'mutation_boundary': self.mutation_boundary,
            'provider_io_boundary': self.provider_io_boundary,
            'provider_progress_revision': self.provider_progress_revision,
            'provider_progress': (None if self.provider_progress is None else
                                  self.provider_progress.canonical_value()),
            'provider_progress_sha256': self.provider_progress_sha256,
            'provider_operation_id': self.provider_operation_id,
            'typed_outcome': (None if self.typed_outcome is None else
                              self.typed_outcome.canonical_value()),
            'typed_outcome_sha256': self.typed_outcome_sha256,
            'settled_at': self.settled_at,
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionReducerHistoryProjectionV2(
        _CompositeCanonicalContract):
    """Bounded action history projected into deterministic reducer input."""

    version: int
    action_id: uuid.UUID
    action_kind: kernel_actions.ActionKind
    action_current_attempt: int
    action_last_result: PersistedActionOutcomeV1 | None
    action_last_result_sha256: str | None
    locked_predecessor: ProviderResourceActionReducerAttemptSnapshotV2 | None
    locked_current: ProviderResourceActionReducerAttemptSnapshotV2 | None
    launch_no_io_prefix: progress.ServeLaunchNoIoPrefixV1 | None
    supersession_quiescence: (progress.ProviderLaunchSupersessionQuiescenceV1 |
                              None)

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'action_id', 'action_kind', 'action_current_attempt',
        'action_last_result', 'action_last_result_sha256', 'locked_predecessor',
        'locked_current', 'launch_no_io_prefix', 'supersession_quiescence'
    })

    def __post_init__(self) -> None:
        _version_two(self.version, name='reducer history version')
        object.__setattr__(
            self, 'action_id',
            _uuid(self.action_id, name='reducer history action ID'))
        object.__setattr__(
            self, 'action_kind',
            _action_kind(self.action_kind, name='reducer history action kind'))
        current_attempt = _nonnegative_integer(
            self.action_current_attempt, name='reducer history current attempt')
        if current_attempt > _MAX_RESOURCE_ACTION_ATTEMPT:
            raise ValueError('reducer history current attempt exceeds domain.')
        object.__setattr__(self, 'action_current_attempt', current_attempt)
        if (self.action_last_result is not None and
                type(self.action_last_result)
                not in (progress.ServeReplicaActionHandlerOutcomeV1,
                        progress.ServeReplicaActionDirectNoEffectOutcomeV1,
                        progress.ServeReplicaActionRequestFallbackOutcomeV1)):
            raise TypeError('reducer history last result is invalid.')
        last_hash = _optional_sha256(self.action_last_result_sha256,
                                     name='reducer history last-result hash')
        object.__setattr__(self, 'action_last_result_sha256', last_hash)
        if (self.action_last_result is None) != (last_hash is None):
            raise ValueError('reducer history last-result/hash presence '
                             'differs.')
        if self.action_last_result is not None and last_hash != (
                self.action_last_result.sha256):
            raise ValueError('reducer history last-result hash differs.')
        for field in ('locked_predecessor', 'locked_current'):
            item = getattr(self, field)
            if item is not None and type(item) is not (
                    ProviderResourceActionReducerAttemptSnapshotV2):
                raise TypeError(f'reducer history {field} is invalid.')
        if (self.launch_no_io_prefix is not None and
                type(self.launch_no_io_prefix)
                is not progress.ServeLaunchNoIoPrefixV1):
            raise TypeError('reducer history no-I/O prefix is invalid.')
        if (self.supersession_quiescence is not None and
                type(self.supersession_quiescence)
                is not progress.ProviderLaunchSupersessionQuiescenceV1):
            raise TypeError('reducer history quiescence is invalid.')
        if self.action_kind is kernel_actions.ActionKind.DOWN and (
                self.launch_no_io_prefix is not None or
                self.supersession_quiescence is not None):
            raise ValueError('down reducer history retains launch-only state.')
        if self.locked_current is not None and (self.locked_current.attempt
                                                != current_attempt):
            raise ValueError('reducer current snapshot differs from action '
                             'attempt.')
        expected_predecessor = current_attempt - 1
        if self.locked_predecessor is not None and (
                self.locked_predecessor.attempt != expected_predecessor or
                self.locked_predecessor.settled_at is None):
            raise ValueError('reducer predecessor is not the exact settled '
                             'immediate attempt.')
        if current_attempt <= 1 and self.locked_predecessor is not None:
            raise ValueError('attempt one cannot retain a predecessor.')

    @classmethod
    def from_value(
            cls,
            value: Any) -> ProviderResourceActionReducerHistoryProjectionV2:
        raw = _closed_object(value,
                             name='reducer history projection V2',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            action_id=raw['action_id'],
            action_kind=raw['action_kind'],
            action_current_attempt=raw['action_current_attempt'],
            action_last_result=(None if raw['action_last_result'] is None else
                                _persisted_outcome_from_value(
                                    raw['action_last_result'])),
            action_last_result_sha256=raw['action_last_result_sha256'],
            locked_predecessor=(
                None if raw['locked_predecessor'] is None else
                ProviderResourceActionReducerAttemptSnapshotV2.from_value(
                    raw['locked_predecessor'])),
            locked_current=(
                None if raw['locked_current'] is None else
                ProviderResourceActionReducerAttemptSnapshotV2.from_value(
                    raw['locked_current'])),
            launch_no_io_prefix=(None if raw['launch_no_io_prefix'] is None else
                                 progress.ServeLaunchNoIoPrefixV1.from_value(
                                     raw['launch_no_io_prefix'])),
            supersession_quiescence=(
                None if raw['supersession_quiescence'] is None else
                progress.ProviderLaunchSupersessionQuiescenceV1.from_value(
                    raw['supersession_quiescence'])))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'action_id': str(self.action_id),
            'action_kind': self.action_kind.value,
            'action_current_attempt': self.action_current_attempt,
            'action_last_result': (None if self.action_last_result is None else
                                   self.action_last_result.canonical_value()),
            'action_last_result_sha256': self.action_last_result_sha256,
            'locked_predecessor': (None if self.locked_predecessor is None else
                                   self.locked_predecessor.canonical_value()),
            'locked_current': (None if self.locked_current is None else
                               self.locked_current.canonical_value()),
            'launch_no_io_prefix':
                (None if self.launch_no_io_prefix is None else
                 self.launch_no_io_prefix.canonical_value()),
            'supersession_quiescence':
                (None if self.supersession_quiescence is None else
                 self.supersession_quiescence.canonical_value()),
        }


def _complete_preflight_response(
    value: preflight_v2.ProviderAuthorityPreflightResponseV2,
    *,
    action_kind: kernel_actions.ActionKind,
) -> preflight_v2.ProviderAuthorityPreflightResponseV2:
    expected_type = (preflight_v2.ProviderLaunchAuthorityPreflightResponseV2
                     if action_kind is kernel_actions.ActionKind.LAUNCH else
                     preflight_v2.ProviderDownAuthorityPreflightResponseV2)
    if type(value) is not expected_type:
        raise TypeError('representability response has a crossed kind.')
    if value.disposition is not (
            preflight_v2.ProviderAuthorityPreflightDispositionV2.COMPLETE):
        raise ValueError('representability root requires a complete response.')
    if (value.resolved_cohort is None or value.execution_capsule is None or
            value.worker_identity is None):
        raise ValueError('complete response lacks exact authority evidence.')
    return value


def _validate_native_v2_config_reference(
    capsule: actions.ProviderKubernetesExecutionCapsuleV2 |
    actions.ProviderKubernetesDownExecutionCapsuleV2,
) -> None:
    projection_ref = capsule.config_projection.config_access_inventory
    if projection_ref.repo_path != _V2_CONFIG_ACCESS_INVENTORY_PATH:
        raise ProviderResourceActionRepresentabilityUnavailableError(
            'native V2 config-access inventory is not bound.')
    if type(capsule) is actions.ProviderKubernetesExecutionCapsuleV2:
        renderer = capsule.renderer
        if (renderer.binding_schema.repo_path != _V2_BINDING_SCHEMA_PATH or
                renderer.config_access_inventory.canonical_bytes
                != projection_ref.canonical_bytes):
            raise ProviderResourceActionRepresentabilityUnavailableError(
                'native V2 renderer artifacts are not bound.')


def _validate_memberships_for_cohort(
    memberships: tuple[
        authority.ProviderAuthorityWorkerAcceptedMembershipV2,
        authority.ProviderAuthorityWorkerAcceptedMembershipV2,
    ],
    cohort: authority.ProviderAuthorityWorkerCohortV2,
) -> None:
    for membership in memberships:
        membership.registration.worker.validate_for_cohort(cohort)


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionPreflightRepresentabilityInputV2(
        _CompositeCanonicalContract):
    """Inputs proving complete-preflight wire representability."""

    version: int
    boundary: ProviderResourceActionRepresentabilityBoundaryV2
    dispatch_kind: ProviderResourceActionRepresentabilityDispatchKindV2
    action_kind: kernel_actions.ActionKind
    request: preflight_v2.ProviderAuthorityPreflightRequestV2
    candidate_complete_response: preflight_v2.ProviderAuthorityPreflightResponseV2
    construction: ProviderResourceActionRepresentabilityConstructionV2
    accepted_memberships: tuple[
        authority.ProviderAuthorityWorkerAcceptedMembershipV2,
        authority.ProviderAuthorityWorkerAcceptedMembershipV2,
    ]

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'boundary', 'dispatch_kind', 'action_kind', 'request',
        'candidate_complete_response', 'construction', 'accepted_memberships'
    })

    def __post_init__(self) -> None:
        _version_two(self.version,
                     name='preflight representability input version')
        if self.boundary not in (
                ProviderResourceActionRepresentabilityBoundaryV2.
                COMPLETE_PREFLIGHT,
                ProviderResourceActionRepresentabilityBoundaryV2.
                COMPLETE_PREFLIGHT.value):
            raise ValueError('preflight representability boundary is invalid.')
        object.__setattr__(
            self, 'boundary',
            ProviderResourceActionRepresentabilityBoundaryV2.COMPLETE_PREFLIGHT)
        try:
            dispatch_kind = (
                self.dispatch_kind if type(self.dispatch_kind)
                is ProviderResourceActionRepresentabilityDispatchKindV2 else
                ProviderResourceActionRepresentabilityDispatchKindV2(
                    self.dispatch_kind))
        except (TypeError, ValueError) as error:
            raise ValueError(
                'preflight dispatch kind is unsupported.') from error
        object.__setattr__(self, 'dispatch_kind', dispatch_kind)
        kind = _action_kind(self.action_kind,
                            name='preflight representability action kind')
        object.__setattr__(self, 'action_kind', kind)
        if type(self.request) is not (
                preflight_v2.ProviderAuthorityPreflightRequestV2):
            raise TypeError('preflight representability request is invalid.')
        if self.request.action_kind is not kind:
            raise ValueError('preflight request action kind differs.')
        response = _complete_preflight_response(
            self.candidate_complete_response, action_kind=kind)
        response.validate_request(self.request)
        expected_construction_type = (
            ProviderResourceActionLaunchRepresentabilityConstructionV2
            if kind is kernel_actions.ActionKind.LAUNCH else
            ProviderResourceActionDownRepresentabilityConstructionV2)
        if type(self.construction) is not expected_construction_type:
            raise TypeError('preflight construction has a crossed kind.')
        assert response.execution_capsule is not None
        if not _same_bytes(response.execution_capsule,
                           self.construction.execution_capsule):
            raise ValueError('preflight response capsule differs from native '
                             'construction.')
        memberships = _accepted_memberships(self.accepted_memberships)
        object.__setattr__(self, 'accepted_memberships', memberships)
        assert response.resolved_cohort is not None
        _validate_memberships_for_cohort(memberships, response.resolved_cohort)
        if kind is kernel_actions.ActionKind.LAUNCH:
            assert type(self.construction) is (
                ProviderResourceActionLaunchRepresentabilityConstructionV2)
            renderer_v2.validate_provider_kubernetes_renderer_input_v2(
                self.construction.renderer_input, response.resolved_cohort)
            (renderer_v2.
             validate_provider_kubernetes_execution_capsule_context_v2(
                 self.construction.execution_capsule, response.resolved_cohort))
        else:
            assert type(self.construction) is (
                ProviderResourceActionDownRepresentabilityConstructionV2)
            expected_capsule = (
                renderer_v2.
                construct_provider_kubernetes_down_execution_capsule_v2(
                    self.construction.capsule_input, response.resolved_cohort,
                    self.construction.cleanup_rederivation_input))
            if not _same_bytes(expected_capsule,
                               self.construction.execution_capsule):
                raise ValueError('down construction does not equal the sole '
                                 'native constructor output.')
        _validate_native_v2_config_reference(
            self.construction.execution_capsule)

    @classmethod
    def from_value(
            cls, value: Any
    ) -> ProviderResourceActionPreflightRepresentabilityInputV2:
        raw = _closed_object(value,
                             name='preflight representability input V2',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            boundary=raw['boundary'],
            dispatch_kind=raw['dispatch_kind'],
            action_kind=raw['action_kind'],
            request=(
                preflight_v2.ProviderAuthorityPreflightRequestV2.from_value(
                    raw['request'])),
            candidate_complete_response=(
                preflight_v2.
                provider_authority_preflight_response_from_value_v2(
                    raw['candidate_complete_response'])),
            construction=_construction_from_value(raw['construction']),
            accepted_memberships=_accepted_memberships_from_value(
                raw['accepted_memberships']))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'boundary': self.boundary.value,
            'dispatch_kind': self.dispatch_kind.value,
            'action_kind': self.action_kind.value,
            'request': self.request.canonical_value(),
            'candidate_complete_response':
                self.candidate_complete_response.canonical_value(),
            'construction': self.construction.canonical_value(),
            'accepted_memberships': [
                item.canonical_value() for item in self.accepted_memberships
            ],
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionAdmissionRepresentabilityInputV2(
        _CompositeCanonicalContract):
    """Inputs proving linked-admission wire representability."""

    version: int
    boundary: ProviderResourceActionRepresentabilityBoundaryV2
    dispatch_kind: ProviderResourceActionRepresentabilityDispatchKindV2
    action_kind: kernel_actions.ActionKind
    request: preflight_v2.ProviderAuthorityPreflightRequestV2
    complete_response: preflight_v2.ProviderAuthorityPreflightResponseV2
    action_id: uuid.UUID
    candidate_spec: actions.ServeReplicaActionSpecV2
    accepted_memberships: tuple[
        authority.ProviderAuthorityWorkerAcceptedMembershipV2,
        authority.ProviderAuthorityWorkerAcceptedMembershipV2,
    ]
    next_attempt: int
    deterministic_request_id: uuid.UUID
    reducer_history: ProviderResourceActionReducerHistoryProjectionV2

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'boundary', 'dispatch_kind', 'action_kind', 'request',
        'complete_response', 'action_id', 'candidate_spec',
        'accepted_memberships', 'next_attempt', 'deterministic_request_id',
        'reducer_history'
    })

    def __post_init__(self) -> None:
        _version_two(self.version,
                     name='admission representability input version')
        if self.boundary not in (
                ProviderResourceActionRepresentabilityBoundaryV2.
                LINKED_ADMISSION,
                ProviderResourceActionRepresentabilityBoundaryV2.
                LINKED_ADMISSION.value):
            raise ValueError('admission representability boundary is invalid.')
        object.__setattr__(
            self, 'boundary',
            ProviderResourceActionRepresentabilityBoundaryV2.LINKED_ADMISSION)
        if self.dispatch_kind not in (
                ProviderResourceActionRepresentabilityDispatchKindV2.
                AUTHORITATIVE_ACTION,
                ProviderResourceActionRepresentabilityDispatchKindV2.
                AUTHORITATIVE_ACTION.value):
            raise ValueError('admission dispatch kind must be authoritative.')
        object.__setattr__(
            self, 'dispatch_kind',
            ProviderResourceActionRepresentabilityDispatchKindV2.
            AUTHORITATIVE_ACTION)
        kind = _action_kind(self.action_kind,
                            name='admission representability action kind')
        object.__setattr__(self, 'action_kind', kind)
        if type(self.request) is not (
                preflight_v2.ProviderAuthorityPreflightRequestV2):
            raise TypeError('admission preflight request is invalid.')
        if self.request.action_kind is not kind:
            raise ValueError('admission preflight request kind differs.')
        response = _complete_preflight_response(self.complete_response,
                                                action_kind=kind)
        response.validate_request(self.request)
        action_id = _uuid(self.action_id,
                          name='admission representability action ID')
        object.__setattr__(self, 'action_id', action_id)
        if type(self.candidate_spec) is not actions.ServeReplicaActionSpecV2:
            raise TypeError('admission candidate spec is invalid.')
        if (self.candidate_spec.action_id != action_id or
                self.candidate_spec.invocation.action_kind is not kind):
            raise ValueError('admission candidate spec identity differs.')
        assert response.execution_capsule is not None
        config = (
            self.candidate_spec.invocation.require_launch().execution_config
            if kind is kernel_actions.ActionKind.LAUNCH else
            self.candidate_spec.invocation.require_down().execution_config)
        if not _same_bytes(config.capsule, response.execution_capsule):
            raise ValueError('admission spec capsule differs from response.')
        _validate_native_v2_config_reference(config.capsule)
        memberships = _accepted_memberships(self.accepted_memberships)
        object.__setattr__(self, 'accepted_memberships', memberships)
        assert response.resolved_cohort is not None
        _validate_memberships_for_cohort(memberships, response.resolved_cohort)
        attempt = _action_attempt(self.next_attempt,
                                  name='admission next attempt')
        object.__setattr__(self, 'next_attempt', attempt)
        request_id = _uuid(self.deterministic_request_id,
                           name='admission deterministic request ID')
        object.__setattr__(self, 'deterministic_request_id', request_id)
        expected_request_id = kernel_actions.request_id_for_attempt(
            action_id, attempt)
        if str(request_id) != expected_request_id:
            raise ValueError('admission request ID is not deterministic.')
        if type(self.reducer_history) is not (
                ProviderResourceActionReducerHistoryProjectionV2):
            raise TypeError('admission reducer history is invalid.')
        history = self.reducer_history
        if (history.action_id != action_id or history.action_kind is not kind or
                history.locked_current is not None):
            raise ValueError('admission reducer history identity is crossed.')
        if attempt == 1:
            if (history.action_current_attempt != 0 or
                    history.locked_predecessor is not None or
                    history.action_last_result is not None or
                    history.launch_no_io_prefix is not None or
                    history.supersession_quiescence is not None):
                raise ValueError('first admission history is not empty.')
        elif (history.action_current_attempt != attempt - 1 or
              history.locked_predecessor is None or
              history.locked_predecessor.attempt != attempt - 1 or
              history.locked_predecessor.typed_outcome is None):
            raise ValueError('retry admission lacks exact predecessor.')

    @classmethod
    def from_value(
            cls, value: Any
    ) -> ProviderResourceActionAdmissionRepresentabilityInputV2:
        raw = _closed_object(value,
                             name='admission representability input V2',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            boundary=raw['boundary'],
            dispatch_kind=raw['dispatch_kind'],
            action_kind=raw['action_kind'],
            request=(
                preflight_v2.ProviderAuthorityPreflightRequestV2.from_value(
                    raw['request'])),
            complete_response=(
                preflight_v2.
                provider_authority_preflight_response_from_value_v2(
                    raw['complete_response'])),
            action_id=raw['action_id'],
            candidate_spec=actions.serve_replica_action_spec_from_value_v2(
                raw['candidate_spec']),
            accepted_memberships=_accepted_memberships_from_value(
                raw['accepted_memberships']),
            next_attempt=raw['next_attempt'],
            deterministic_request_id=raw['deterministic_request_id'],
            reducer_history=(
                ProviderResourceActionReducerHistoryProjectionV2.from_value(
                    raw['reducer_history'])))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'boundary': self.boundary.value,
            'dispatch_kind': self.dispatch_kind.value,
            'action_kind': self.action_kind.value,
            'request': self.request.canonical_value(),
            'complete_response': self.complete_response.canonical_value(),
            'action_id': str(self.action_id),
            'candidate_spec': self.candidate_spec.canonical_value(),
            'accepted_memberships': [
                item.canonical_value() for item in self.accepted_memberships
            ],
            'next_attempt': self.next_attempt,
            'deterministic_request_id': str(self.deterministic_request_id),
            'reducer_history': self.reducer_history.canonical_value(),
        }


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionPreIoRepresentabilityInputV2(
        _CompositeCanonicalContract):
    """Inputs proving pre-provider-I/O wire representability."""

    version: int
    boundary: ProviderResourceActionRepresentabilityBoundaryV2
    dispatch_kind: ProviderResourceActionRepresentabilityDispatchKindV2
    action_kind: kernel_actions.ActionKind
    action_id: uuid.UUID
    stored_spec: actions.ServeReplicaActionSpecV2
    accepted_membership: authority.ProviderAuthorityWorkerAcceptedMembershipV2
    attempt: int
    request_id: uuid.UUID
    request_execution_generation: int
    current_progress: progress.ProviderLifecycleProgressV1 | None
    worker_attestation: progress.ProviderAuthorityWorkerAttemptAttestationV1
    reducer_history: ProviderResourceActionReducerHistoryProjectionV2

    _KEYS: ClassVar[frozenset[str]] = frozenset({
        'version', 'boundary', 'dispatch_kind', 'action_kind', 'action_id',
        'stored_spec', 'accepted_membership', 'attempt', 'request_id',
        'request_execution_generation', 'current_progress',
        'worker_attestation', 'reducer_history'
    })

    def __post_init__(self) -> None:
        _version_two(self.version,
                     name='pre-I/O representability input version')
        if self.boundary not in (
                ProviderResourceActionRepresentabilityBoundaryV2.PRE_IO,
                ProviderResourceActionRepresentabilityBoundaryV2.PRE_IO.value):
            raise ValueError('pre-I/O representability boundary is invalid.')
        object.__setattr__(
            self, 'boundary',
            ProviderResourceActionRepresentabilityBoundaryV2.PRE_IO)
        if self.dispatch_kind not in (
                ProviderResourceActionRepresentabilityDispatchKindV2.
                AUTHORITATIVE_ACTION,
                ProviderResourceActionRepresentabilityDispatchKindV2.
                AUTHORITATIVE_ACTION.value):
            raise ValueError('pre-I/O dispatch kind must be authoritative.')
        object.__setattr__(
            self, 'dispatch_kind',
            ProviderResourceActionRepresentabilityDispatchKindV2.
            AUTHORITATIVE_ACTION)
        kind = _action_kind(self.action_kind,
                            name='pre-I/O representability action kind')
        object.__setattr__(self, 'action_kind', kind)
        action_id = _uuid(self.action_id,
                          name='pre-I/O representability action ID')
        object.__setattr__(self, 'action_id', action_id)
        if type(self.stored_spec) is not actions.ServeReplicaActionSpecV2:
            raise TypeError('pre-I/O stored spec is invalid.')
        if (self.stored_spec.action_id != action_id or
                self.stored_spec.invocation.action_kind is not kind):
            raise ValueError('pre-I/O stored spec identity differs.')
        config = (self.stored_spec.invocation.require_launch().execution_config
                  if kind is kernel_actions.ActionKind.LAUNCH else
                  self.stored_spec.invocation.require_down().execution_config)
        _validate_native_v2_config_reference(config.capsule)
        if type(self.accepted_membership) is not (
                authority.ProviderAuthorityWorkerAcceptedMembershipV2):
            raise TypeError('pre-I/O accepted membership is invalid.')
        attempt = _action_attempt(self.attempt, name='pre-I/O attempt')
        object.__setattr__(self, 'attempt', attempt)
        request_id = _uuid(self.request_id, name='pre-I/O request ID')
        object.__setattr__(self, 'request_id', request_id)
        if str(request_id) != kernel_actions.request_id_for_attempt(
                action_id, attempt):
            raise ValueError('pre-I/O request ID is not deterministic.')
        generation = _positive_integer(self.request_execution_generation,
                                       name='pre-I/O execution generation')
        object.__setattr__(self, 'request_execution_generation', generation)
        if (self.current_progress is not None and type(self.current_progress)
                is not progress.ProviderLifecycleProgressV1):
            raise TypeError('pre-I/O current progress is invalid.')
        if type(self.worker_attestation) is not (
                progress.ProviderAuthorityWorkerAttemptAttestationV1):
            raise TypeError('pre-I/O worker attestation is invalid.')
        attestation = self.worker_attestation
        if (attestation.request_id != request_id or
                attestation.request_execution_generation != generation or
                attestation.request_worker_id != str(
                    self.accepted_membership.registration.worker_instance_id)):
            raise ValueError('pre-I/O attestation differs from claim identity.')
        if type(self.reducer_history) is not (
                ProviderResourceActionReducerHistoryProjectionV2):
            raise TypeError('pre-I/O reducer history is invalid.')
        history = self.reducer_history
        current = history.locked_current
        if (history.action_id != action_id or history.action_kind is not kind or
                history.action_current_attempt != attempt or current is None or
                current.attempt != attempt or current.request_id != request_id):
            raise ValueError('pre-I/O reducer history identity differs.')
        expected_progress_bytes = (None if self.current_progress is None else
                                   self.current_progress.canonical_bytes)
        actual_progress_bytes = (None if current.provider_progress is None else
                                 current.provider_progress.canonical_bytes)
        if expected_progress_bytes != actual_progress_bytes:
            raise ValueError('pre-I/O current progress differs from locked '
                             'history.')
        if attempt == 1:
            if history.locked_predecessor is not None:
                raise ValueError('pre-I/O attempt one has a predecessor.')
        elif (history.locked_predecessor is None or
              history.locked_predecessor.attempt != attempt - 1 or
              history.locked_predecessor.typed_outcome is None):
            raise ValueError('pre-I/O retry lacks exact predecessor.')

    @classmethod
    def from_value(
            cls,
            value: Any) -> ProviderResourceActionPreIoRepresentabilityInputV2:
        raw = _closed_object(value,
                             name='pre-I/O representability input V2',
                             keys=cls._KEYS)
        return cls(
            version=raw['version'],
            boundary=raw['boundary'],
            dispatch_kind=raw['dispatch_kind'],
            action_kind=raw['action_kind'],
            action_id=raw['action_id'],
            stored_spec=actions.serve_replica_action_spec_from_value_v2(
                raw['stored_spec']),
            accepted_membership=_accepted_membership_from_value(
                raw['accepted_membership']),
            attempt=raw['attempt'],
            request_id=raw['request_id'],
            request_execution_generation=raw['request_execution_generation'],
            current_progress=(None if raw['current_progress'] is None else
                              progress.ProviderLifecycleProgressV1.from_value(
                                  raw['current_progress'])),
            worker_attestation=(
                progress.ProviderAuthorityWorkerAttemptAttestationV1.from_value(
                    raw['worker_attestation'])),
            reducer_history=(
                ProviderResourceActionReducerHistoryProjectionV2.from_value(
                    raw['reducer_history'])))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'boundary': self.boundary.value,
            'dispatch_kind': self.dispatch_kind.value,
            'action_kind': self.action_kind.value,
            'action_id': str(self.action_id),
            'stored_spec': self.stored_spec.canonical_value(),
            'accepted_membership': self.accepted_membership.canonical_value(),
            'attempt': self.attempt,
            'request_id': str(self.request_id),
            'request_execution_generation': self.request_execution_generation,
            'current_progress': (None if self.current_progress is None else
                                 self.current_progress.canonical_value()),
            'worker_attestation': self.worker_attestation.canonical_value(),
            'reducer_history': self.reducer_history.canonical_value(),
        }


ProviderResourceActionRepresentabilityInputV2 = (
    ProviderResourceActionPreflightRepresentabilityInputV2 |
    ProviderResourceActionAdmissionRepresentabilityInputV2 |
    ProviderResourceActionPreIoRepresentabilityInputV2)


def provider_resource_action_representability_input_from_value_v2(
    value: Any,) -> ProviderResourceActionRepresentabilityInputV2:
    if type(value) is not dict:
        raise TypeError('representability input V2 must be an object.')
    try:
        boundary = ProviderResourceActionRepresentabilityBoundaryV2(
            value.get('boundary'))
    except (TypeError, ValueError) as error:
        raise ValueError(
            'representability input boundary is unsupported.') from error
    if boundary is (ProviderResourceActionRepresentabilityBoundaryV2.
                    COMPLETE_PREFLIGHT):
        return (ProviderResourceActionPreflightRepresentabilityInputV2.
                from_value(value))
    if boundary is (
            ProviderResourceActionRepresentabilityBoundaryV2.LINKED_ADMISSION):
        return (ProviderResourceActionAdmissionRepresentabilityInputV2.
                from_value(value))
    return ProviderResourceActionPreIoRepresentabilityInputV2.from_value(value)


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionRepresentabilityFixtureInputV2(
        _CompositeCanonicalContract):
    """Canonical launch and down inputs for all implemented boundaries."""

    version: int
    launch_complete_preflight: ProviderResourceActionPreflightRepresentabilityInputV2
    launch_linked_admission: ProviderResourceActionAdmissionRepresentabilityInputV2
    launch_pre_io: ProviderResourceActionPreIoRepresentabilityInputV2
    down_complete_preflight: ProviderResourceActionPreflightRepresentabilityInputV2
    down_linked_admission: ProviderResourceActionAdmissionRepresentabilityInputV2
    down_pre_io: ProviderResourceActionPreIoRepresentabilityInputV2

    _KEYS: ClassVar[frozenset[str]] = frozenset({'version', 'launch', 'down'})
    _BOUNDARY_KEYS: ClassVar[frozenset[str]] = frozenset(
        {'complete_preflight', 'linked_admission', 'pre_io'})

    def __post_init__(self) -> None:
        _version_two(self.version,
                     name='representability fixture input version')
        rows = (
            (self.launch_complete_preflight,
             ProviderResourceActionPreflightRepresentabilityInputV2,
             kernel_actions.ActionKind.LAUNCH),
            (self.launch_linked_admission,
             ProviderResourceActionAdmissionRepresentabilityInputV2,
             kernel_actions.ActionKind.LAUNCH),
            (self.launch_pre_io,
             ProviderResourceActionPreIoRepresentabilityInputV2,
             kernel_actions.ActionKind.LAUNCH),
            (self.down_complete_preflight,
             ProviderResourceActionPreflightRepresentabilityInputV2,
             kernel_actions.ActionKind.DOWN),
            (self.down_linked_admission,
             ProviderResourceActionAdmissionRepresentabilityInputV2,
             kernel_actions.ActionKind.DOWN),
            (self.down_pre_io,
             ProviderResourceActionPreIoRepresentabilityInputV2,
             kernel_actions.ActionKind.DOWN),
        )
        if any(
                type(item) is not expected_type or
                item.action_kind is not expected_kind
                for item, expected_type, expected_kind in rows):
            raise TypeError('representability fixture boundary root is '
                            'crossed or invalid.')

    @classmethod
    def from_value(
            cls,
            value: Any) -> ProviderResourceActionRepresentabilityFixtureInputV2:
        raw = _closed_object(value,
                             name='representability fixture input V2',
                             keys=cls._KEYS)
        launch = _closed_object(raw['launch'],
                                name='launch fixture boundary set',
                                keys=cls._BOUNDARY_KEYS)
        down = _closed_object(raw['down'],
                              name='down fixture boundary set',
                              keys=cls._BOUNDARY_KEYS)
        return cls(
            version=raw['version'],
            launch_complete_preflight=(
                ProviderResourceActionPreflightRepresentabilityInputV2.
                from_value(launch['complete_preflight'])),
            launch_linked_admission=(
                ProviderResourceActionAdmissionRepresentabilityInputV2.
                from_value(launch['linked_admission'])),
            launch_pre_io=(
                ProviderResourceActionPreIoRepresentabilityInputV2.from_value(
                    launch['pre_io'])),
            down_complete_preflight=(
                ProviderResourceActionPreflightRepresentabilityInputV2.
                from_value(down['complete_preflight'])),
            down_linked_admission=(
                ProviderResourceActionAdmissionRepresentabilityInputV2.
                from_value(down['linked_admission'])),
            down_pre_io=(
                ProviderResourceActionPreIoRepresentabilityInputV2.from_value(
                    down['pre_io'])))

    def canonical_value(self) -> dict[str, Any]:
        return {
            'version': 2,
            'launch': {
                'complete_preflight':
                    self.launch_complete_preflight.canonical_value(),
                'linked_admission':
                    self.launch_linked_admission.canonical_value(),
                'pre_io': self.launch_pre_io.canonical_value(),
            },
            'down': {
                'complete_preflight':
                    self.down_complete_preflight.canonical_value(),
                'linked_admission':
                    self.down_linked_admission.canonical_value(),
                'pre_io': self.down_pre_io.canonical_value(),
            },
        }

    @property
    def ordered_roots(
        self,) -> tuple[ProviderResourceActionRepresentabilityInputV2, ...]:
        return (self.launch_complete_preflight, self.launch_linked_admission,
                self.launch_pre_io, self.down_complete_preflight,
                self.down_linked_admission, self.down_pre_io)


# This tuple is deliberately fully expanded.  Do not replace any group with a
# range, enum walk, product, regex, or artifact-derived selector.
_CASE_ROW_VALUES: tuple[tuple[str, str, str, str, str], ...] = (
    ("launch.complete_preflight.preflight_request", "authoritative_action",
     "launch", "complete_preflight", "preflight_request"),
    ("launch.complete_preflight.preflight_response.complete",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.request_contract",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.secret_or_tls_material",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.source_mismatch",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.policy_configured_or_mutated",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.managed_secrets",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.multi_task",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.multi_node",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.multi_resource",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.mount_or_storage",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.non_kubernetes",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.spot",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.non_direct_pod_topology",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.port_contract",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.reserved_label_collision",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.mutable_image",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.custom_provider_implementation",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.preflight_unavailable_or_invalid",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.authority_worker_attestation",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.authorization_or_principal_drift",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.prerequisite_or_network_drift",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.admitted_object_contract",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.runtime_or_job_contract",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.unrepresented_execution_config",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.unrepresented_resource",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.unfrozen_placement",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.unfrozen_identity",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.unfrozen_kubernetes_scope",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.preflight_response.not_representable.target_mismatch",
     "authoritative_action", "launch", "complete_preflight",
     "preflight_response"),
    ("launch.complete_preflight.cohort.resolved", "authoritative_action",
     "launch", "complete_preflight", "cohort"),
    ("launch.complete_preflight.worker_identity.eligible_0",
     "authoritative_action", "launch", "complete_preflight", "worker_identity"),
    ("launch.complete_preflight.worker_identity.eligible_1",
     "authoritative_action", "launch", "complete_preflight", "worker_identity"),
    ("launch.complete_preflight.renderer_input.native_v2",
     "authoritative_action", "launch", "complete_preflight", "renderer_input"),
    ("launch.complete_preflight.rendered_body.head_ssh_service",
     "authoritative_action", "launch", "complete_preflight", "rendered_body"),
    ("launch.complete_preflight.rendered_body.head_service",
     "authoritative_action", "launch", "complete_preflight", "rendered_body"),
    ("launch.complete_preflight.rendered_body.head_pod", "authoritative_action",
     "launch", "complete_preflight", "rendered_body"),
    ("launch.complete_preflight.execution_capsule.native_v2",
     "authoritative_action", "launch", "complete_preflight",
     "execution_capsule"),
    ("down.complete_preflight.preflight_request", "authoritative_action",
     "down", "complete_preflight", "preflight_request"),
    ("down.complete_preflight.preflight_response.complete",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.preflight_response.not_representable.request_contract",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.preflight_response.not_representable.prior_launch_basis",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.preflight_response.not_representable.target_mismatch",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.preflight_response.not_representable.preflight_unavailable_or_invalid",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.preflight_response.not_representable.authority_worker_attestation",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.preflight_response.not_representable.authorization_or_principal_drift",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.preflight_response.not_representable.prerequisite_or_network_drift",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.preflight_response.not_representable.policy_configured_or_mutated",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.preflight_response.not_representable.unrepresented_execution_config",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.preflight_response.not_representable.unfrozen_kubernetes_scope",
     "authoritative_action", "down", "complete_preflight",
     "preflight_response"),
    ("down.complete_preflight.cohort.resolved", "authoritative_action", "down",
     "complete_preflight", "cohort"),
    ("down.complete_preflight.worker_identity.eligible_0",
     "authoritative_action", "down", "complete_preflight", "worker_identity"),
    ("down.complete_preflight.worker_identity.eligible_1",
     "authoritative_action", "down", "complete_preflight", "worker_identity"),
    ("down.complete_preflight.cleanup_target.completed_launch",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.create_intent_0_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.objects_partial_1_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.create_intent_1_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.objects_partial_2_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.create_intent_2_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.objects_partial_3_unscheduled_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.objects_exact_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.handle_intent_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.handle_committed_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.handle_committed_exact_handle",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.runtime_ready_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.runtime_ready_exact_handle",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.job_intent_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.job_intent_exact_handle",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.job_committed_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.job_committed_exact_handle",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.job_running_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.job_running_exact_handle",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.endpoint_resolved_not_found",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.cleanup_target.partial.endpoint_resolved_exact_handle",
     "authoritative_action", "down", "complete_preflight", "cleanup_target"),
    ("down.complete_preflight.execution_capsule.native_v2",
     "authoritative_action", "down", "complete_preflight", "execution_capsule"),
    ("launch.linked_admission.preflight_request", "authoritative_action",
     "launch", "linked_admission", "preflight_request"),
    ("launch.linked_admission.preflight_response.complete",
     "authoritative_action", "launch", "linked_admission",
     "preflight_response"),
    ("launch.linked_admission.worker_identity.eligible_0",
     "authoritative_action", "launch", "linked_admission", "worker_identity"),
    ("launch.linked_admission.worker_identity.eligible_1",
     "authoritative_action", "launch", "linked_admission", "worker_identity"),
    ("launch.linked_admission.execution_capsule", "authoritative_action",
     "launch", "linked_admission", "execution_capsule"),
    ("launch.linked_admission.execution_config", "authoritative_action",
     "launch", "linked_admission", "execution_config"),
    ("launch.linked_admission.invocation", "authoritative_action", "launch",
     "linked_admission", "invocation"),
    ("launch.linked_admission.plan", "authoritative_action", "launch",
     "linked_admission", "plan"),
    ("launch.linked_admission.action_spec", "authoritative_action", "launch",
     "linked_admission", "action_spec"),
    ("down.linked_admission.preflight_request", "authoritative_action", "down",
     "linked_admission", "preflight_request"),
    ("down.linked_admission.preflight_response.complete",
     "authoritative_action", "down", "linked_admission", "preflight_response"),
    ("down.linked_admission.worker_identity.eligible_0", "authoritative_action",
     "down", "linked_admission", "worker_identity"),
    ("down.linked_admission.worker_identity.eligible_1", "authoritative_action",
     "down", "linked_admission", "worker_identity"),
    ("down.linked_admission.execution_capsule", "authoritative_action", "down",
     "linked_admission", "execution_capsule"),
    ("down.linked_admission.execution_config", "authoritative_action", "down",
     "linked_admission", "execution_config"),
    ("down.linked_admission.invocation", "authoritative_action", "down",
     "linked_admission", "invocation"),
    ("down.linked_admission.plan", "authoritative_action", "down",
     "linked_admission", "plan"),
    ("down.linked_admission.action_spec", "authoritative_action", "down",
     "linked_admission", "action_spec"),
    ("launch.pre_io.worker_identity", "authoritative_action", "launch",
     "pre_io", "worker_identity"),
    ("launch.pre_io.attempt_attestation", "authoritative_action", "launch",
     "pre_io", "attempt_attestation"),
    ("launch.pre_io.execution_capsule", "authoritative_action", "launch",
     "pre_io", "execution_capsule"),
    ("launch.pre_io.execution_config", "authoritative_action", "launch",
     "pre_io", "execution_config"),
    ("launch.pre_io.invocation", "authoritative_action", "launch", "pre_io",
     "invocation"),
    ("launch.pre_io.plan", "authoritative_action", "launch", "pre_io", "plan"),
    ("launch.pre_io.action_spec", "authoritative_action", "launch", "pre_io",
     "action_spec"),
    ("down.pre_io.worker_identity", "authoritative_action", "down", "pre_io",
     "worker_identity"),
    ("down.pre_io.attempt_attestation", "authoritative_action", "down",
     "pre_io", "attempt_attestation"),
    ("down.pre_io.execution_capsule", "authoritative_action", "down", "pre_io",
     "execution_capsule"),
    ("down.pre_io.execution_config", "authoritative_action", "down", "pre_io",
     "execution_config"),
    ("down.pre_io.invocation", "authoritative_action", "down", "pre_io",
     "invocation"),
    ("down.pre_io.plan", "authoritative_action", "down", "pre_io", "plan"),
    ("down.pre_io.action_spec", "authoritative_action", "down", "pre_io",
     "action_spec"),
    ("launch.pre_io.progress.create_intent.head_ssh_service",
     "authoritative_action", "launch", "pre_io", "progress"),
    ("launch.pre_io.progress.objects_partial.one_slot", "authoritative_action",
     "launch", "pre_io", "progress"),
    ("launch.pre_io.progress.create_intent.head_service",
     "authoritative_action", "launch", "pre_io", "progress"),
    ("launch.pre_io.progress.objects_partial.two_slots", "authoritative_action",
     "launch", "pre_io", "progress"),
    ("launch.pre_io.progress.create_intent.head_pod", "authoritative_action",
     "launch", "pre_io", "progress"),
    ("launch.pre_io.progress.objects_partial.three_slots_unscheduled",
     "authoritative_action", "launch", "pre_io", "progress"),
    ("launch.pre_io.progress.objects_exact.unscheduled", "authoritative_action",
     "launch", "pre_io", "progress"),
    ("launch.pre_io.progress.objects_exact.scheduled", "authoritative_action",
     "launch", "pre_io", "progress"),
    ("launch.pre_io.progress.handle_intent", "authoritative_action", "launch",
     "pre_io", "progress"),
    ("launch.pre_io.progress.handle_committed", "authoritative_action",
     "launch", "pre_io", "progress"),
    ("launch.pre_io.progress.runtime_ready", "authoritative_action", "launch",
     "pre_io", "progress"),
    ("launch.pre_io.progress.job_intent", "authoritative_action", "launch",
     "pre_io", "progress"),
    ("launch.pre_io.progress.job_committed", "authoritative_action", "launch",
     "pre_io", "progress"),
    ("launch.pre_io.progress.job_running", "authoritative_action", "launch",
     "pre_io", "progress"),
    ("launch.pre_io.progress.endpoint_resolved", "authoritative_action",
     "launch", "pre_io", "progress"),
    ("launch.pre_io.progress.succeeded", "authoritative_action", "launch",
     "pre_io", "progress"),
    ("down.pre_io.progress.target_resolved.all_present", "authoritative_action",
     "down", "pre_io", "progress"),
    ("down.pre_io.progress.target_resolved.all_absent", "authoritative_action",
     "down", "pre_io", "progress"),
    ("down.pre_io.progress.delete_intent.head_pod", "authoritative_action",
     "down", "pre_io", "progress"),
    ("down.pre_io.progress.delete_partial.head_pod", "authoritative_action",
     "down", "pre_io", "progress"),
    ("down.pre_io.progress.delete_intent.head_service", "authoritative_action",
     "down", "pre_io", "progress"),
    ("down.pre_io.progress.delete_partial.head_service", "authoritative_action",
     "down", "pre_io", "progress"),
    ("down.pre_io.progress.delete_intent.head_ssh_service",
     "authoritative_action", "down", "pre_io", "progress"),
    ("down.pre_io.progress.delete_partial.head_ssh_service",
     "authoritative_action", "down", "pre_io", "progress"),
    ("down.pre_io.progress.absence_exact", "authoritative_action", "down",
     "pre_io", "progress"),
    ("down.pre_io.progress.handle_remove_intent.exact_handle",
     "authoritative_action", "down", "pre_io", "progress"),
    ("down.pre_io.progress.handle_remove_intent.already_absent",
     "authoritative_action", "down", "pre_io", "progress"),
    ("down.pre_io.progress.handle_removed.removed_exact",
     "authoritative_action", "down", "pre_io", "progress"),
    ("down.pre_io.progress.handle_removed.already_absent",
     "authoritative_action", "down", "pre_io", "progress"),
    ("down.pre_io.progress.succeeded.removed_exact", "authoritative_action",
     "down", "pre_io", "progress"),
    ("down.pre_io.progress.succeeded.already_absent", "authoritative_action",
     "down", "pre_io", "progress"),
    ("launch.pre_io.no_effect.call_not_entered.effect_0",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.call_not_entered.effect_1",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.call_not_entered.effect_2",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.call_not_entered.effect_3",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.call_not_entered.effect_4",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.core_v1_422.effect_0", "authoritative_action",
     "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.core_v1_422.effect_1", "authoritative_action",
     "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.core_v1_422.effect_2", "authoritative_action",
     "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.cluster_record.rolled_back_not_found",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.cluster_record.different_uuid_conflict",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.skylet.schema_rejected_not_found",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.skylet.same_key_conflict.succeeded",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.skylet.same_key_conflict.failed",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.no_effect.skylet.same_key_conflict.blocked",
     "authoritative_action", "launch", "pre_io", "no_effect_resolution"),
    ("launch.pre_io.quiescence.phase.create_intent.head_ssh_service",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.objects_partial.one_slot",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.create_intent.head_service",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.objects_partial.two_slots",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.create_intent.head_pod",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.objects_partial.three_slots_unscheduled",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.objects_exact.unscheduled",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.objects_exact.scheduled",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.handle_intent", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.handle_committed", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.runtime_ready", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.job_intent", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.job_committed", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.job_running", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.phase.endpoint_resolved", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.e_only.effect_0", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.e_only.effect_1", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.e_only.effect_2", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.e_only.effect_3", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.e_only.effect_4", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.e_plus_n.effect_0", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.e_plus_n.effect_1", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.e_plus_n.effect_2", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.e_plus_n.effect_3", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.e_plus_n.effect_4", "authoritative_action",
     "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.same_claim_commit.effect_0",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.same_claim_commit.effect_1",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.same_claim_commit.effect_2",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.same_claim_commit.effect_3",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.same_claim_commit.effect_4",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.same_claim_adoption.effect_0",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.same_claim_adoption.effect_1",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.same_claim_adoption.effect_2",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.same_claim_adoption.effect_3",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.same_claim_adoption.effect_4",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.later_generation_adoption.effect_0",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.later_generation_adoption.effect_1",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.later_generation_adoption.effect_2",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.later_generation_adoption.effect_3",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.later_generation_adoption.effect_4",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.generation_reset.effect_0",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.generation_reset.effect_1",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.generation_reset.effect_2",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.generation_reset.effect_3",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.quiescence.generation_reset.effect_4",
     "authoritative_action", "launch", "pre_io", "quiescence"),
    ("launch.pre_io.request_return.domain_success", "authoritative_action",
     "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_success", "authoritative_action",
     "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.revision_zero.transient",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.revision_zero.transient",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.revision_zero.capacity",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.revision_zero.capacity",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.revision_zero.quota",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.revision_zero.quota",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.revision_zero.rate_limited",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.revision_zero.rate_limited",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.revision_zero.invalid_request",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.revision_zero.invalid_request",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.revision_zero.permission",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.revision_zero.permission",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.revision_zero.conflict",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.revision_zero.conflict",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.revision_zero.unknown",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.revision_zero.unknown",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.nonintent.transient",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.nonintent.transient",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.nonintent.capacity",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.nonintent.capacity",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.nonintent.quota",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.nonintent.quota",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.nonintent.rate_limited",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.nonintent.rate_limited",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.nonintent.invalid_request",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.nonintent.invalid_request",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.nonintent.permission",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.nonintent.permission",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.nonintent.conflict",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.nonintent.conflict",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.nonintent.unknown",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.nonintent.unknown",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.current_intent.transient",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.current_intent.transient",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.current_intent.capacity",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.current_intent.capacity",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.current_intent.quota",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.current_intent.quota",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.current_intent.rate_limited",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.current_intent.rate_limited",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.current_intent.invalid_request",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.current_intent.invalid_request",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.current_intent.permission",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.current_intent.permission",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.current_intent.conflict",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.current_intent.conflict",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.domain_error.current_intent.unknown",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.domain_error.current_intent.unknown",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.p0.succeeded.missing_handler_return",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.p0.succeeded.invalid_handler_return",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.p0.failed.request_failed",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.p0.cancelled.request_cancelled",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.o.succeeded.missing_handler_return",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.o.succeeded.invalid_handler_return",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.o.failed.request_failed",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.o.cancelled.request_cancelled",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.s.succeeded.missing_handler_return",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.s.succeeded.invalid_handler_return",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.s.failed.request_failed",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.s.cancelled.request_cancelled",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.x.succeeded.missing_handler_return",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.x.succeeded.invalid_handler_return",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.x.failed.request_failed",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.fallback.x.cancelled.request_cancelled",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.max_attempt_exhaustion.handler_r",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.max_attempt_exhaustion.handler_u",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.max_attempt_exhaustion.fallback_p0",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.action_outcome.max_attempt_exhaustion.fallback_o",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_success", "authoritative_action",
     "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_success", "authoritative_action",
     "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.revision_zero.transient",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.revision_zero.transient",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.revision_zero.capacity",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.revision_zero.capacity",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.revision_zero.quota",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.revision_zero.quota",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.revision_zero.rate_limited",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.revision_zero.rate_limited",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.revision_zero.invalid_request",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.revision_zero.invalid_request",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.revision_zero.permission",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.revision_zero.permission",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.revision_zero.conflict",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.revision_zero.conflict",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.revision_zero.unknown",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.revision_zero.unknown",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.nonintent.transient",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.nonintent.transient",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.nonintent.capacity",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.nonintent.capacity",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.nonintent.quota",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.nonintent.quota",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.nonintent.rate_limited",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.nonintent.rate_limited",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.nonintent.invalid_request",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.nonintent.invalid_request",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.nonintent.permission",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.nonintent.permission",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.nonintent.conflict",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.nonintent.conflict",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.nonintent.unknown",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.nonintent.unknown",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.current_intent.transient",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.current_intent.transient",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.current_intent.capacity",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.current_intent.capacity",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.current_intent.quota",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.current_intent.quota",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.current_intent.rate_limited",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.current_intent.rate_limited",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.current_intent.invalid_request",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.current_intent.invalid_request",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.current_intent.permission",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.current_intent.permission",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.current_intent.conflict",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.current_intent.conflict",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.request_return.domain_error.current_intent.unknown",
     "authoritative_action", "down", "pre_io", "request_return"),
    ("down.pre_io.action_outcome.domain_error.current_intent.unknown",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.p0.succeeded.missing_handler_return",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.p0.succeeded.invalid_handler_return",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.p0.failed.request_failed",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.p0.cancelled.request_cancelled",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.o.succeeded.missing_handler_return",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.o.succeeded.invalid_handler_return",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.o.failed.request_failed",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.o.cancelled.request_cancelled",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.s.succeeded.missing_handler_return",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.s.succeeded.invalid_handler_return",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.s.failed.request_failed",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.s.cancelled.request_cancelled",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.x.succeeded.missing_handler_return",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.x.succeeded.invalid_handler_return",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.x.failed.request_failed",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.fallback.x.cancelled.request_cancelled",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.max_attempt_exhaustion.handler_r",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.max_attempt_exhaustion.handler_u",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.max_attempt_exhaustion.fallback_p0",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("down.pre_io.action_outcome.max_attempt_exhaustion.fallback_o",
     "authoritative_action", "down", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.supersession.e_only.prefix_1",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.supersession.e_only.prefix_1",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.supersession.e_only.prefix_2",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.supersession.e_only.prefix_2",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.supersession.e_only.prefix_3",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.supersession.e_only.prefix_3",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.supersession.e_only.prefix_4",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.supersession.e_only.prefix_4",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.supersession.e_only.prefix_5",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.supersession.e_only.prefix_5",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.supersession.e_plus_n.effect_0",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.supersession.e_plus_n.effect_0",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.supersession.e_plus_n.effect_1",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.supersession.e_plus_n.effect_1",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.supersession.e_plus_n.effect_2",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.supersession.e_plus_n.effect_2",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.supersession.e_plus_n.effect_3",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.supersession.e_plus_n.effect_3",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.pre_io.request_return.supersession.e_plus_n.effect_4",
     "authoritative_action", "launch", "pre_io", "request_return"),
    ("launch.pre_io.action_outcome.supersession.e_plus_n.effect_4",
     "authoritative_action", "launch", "pre_io", "action_outcome"),
    ("launch.owner_fenced_transition.action_outcome.cancelled_no_effect.unmaterialized",
     "authoritative_action", "launch", "owner_fenced_transition",
     "action_outcome"),
    ("launch.owner_fenced_transition.action_outcome.cancelled_no_effect.terminal_request_unsettled.one_link",
     "authoritative_action", "launch", "owner_fenced_transition",
     "action_outcome"),
    ("launch.owner_fenced_transition.action_outcome.cancelled_no_effect.terminal_request_unsettled.max_count",
     "authoritative_action", "launch", "owner_fenced_transition",
     "action_outcome"),
    ("launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_present.one_link",
     "authoritative_action", "launch", "owner_fenced_transition",
     "action_outcome"),
    ("launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_present.max_count",
     "authoritative_action", "launch", "owner_fenced_transition",
     "action_outcome"),
    ("launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_gc.one_link",
     "authoritative_action", "launch", "owner_fenced_transition",
     "action_outcome"),
    ("launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_gc.max_count",
     "authoritative_action", "launch", "owner_fenced_transition",
     "action_outcome"),
    ("launch.settlement.shadow_outcome.primary", "shadow_candidate", "launch",
     "settlement", "shadow_outcome"),
    ("down.settlement.shadow_outcome.primary", "shadow_candidate", "down",
     "settlement", "shadow_outcome"),
)

PROVIDER_RESOURCE_ACTION_REPRESENTABILITY_CASES_V2 = tuple(
    ProviderResourceActionRepresentabilityCaseV2(
        sequence=sequence,
        case_id=row[0],
        dispatch_kind=(
            ProviderResourceActionRepresentabilityDispatchKindV2(row[1])),
        action_kind=(kernel_actions.ActionKind(row[2])),
        boundary=(ProviderResourceActionRepresentabilityBoundaryV2(row[3])),
        payload_kind=(
            ProviderResourceActionRepresentabilityPayloadKindV2(row[4])))
    for sequence, row in enumerate(_CASE_ROW_VALUES))

PROVIDER_RESOURCE_ACTION_REPRESENTABILITY_CASE_INVENTORY_V2 = (
    ProviderResourceActionRepresentabilityCaseInventoryV2(
        version=2,
        contract=_CASE_INVENTORY_CONTRACT,
        profile=_CASE_INVENTORY_PROFILE,
        cases=PROVIDER_RESOURCE_ACTION_REPRESENTABILITY_CASES_V2))


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionRepresentabilityMeasurementV2:
    """One measured canonical payload in one enumerator-owned mode."""

    case_sequence: int
    case_id: str
    mode: ProviderResourceActionRepresentabilityModeV2
    canonical_byte_count: int
    sha256: str

    def __post_init__(self) -> None:
        _nonnegative_integer(self.case_sequence,
                             name='measurement case sequence')
        _text(self.case_id, name='measurement case ID')
        if type(self.mode) is not ProviderResourceActionRepresentabilityModeV2:
            raise TypeError('measurement mode has an invalid type.')
        byte_count = _positive_integer(self.canonical_byte_count,
                                       name='measurement canonical byte count')
        if byte_count > _MAX_VALUE_CANONICAL_BYTES:
            raise ProviderResourceActionRepresentabilityError(
                'representability payload exceeds 65536 canonical bytes.')
        _sha256(self.sha256, name='measurement SHA-256')


@dataclasses.dataclass(frozen=True)
class ProviderResourceActionRepresentabilityEnumerationV2:
    """Both mandatory modes for the cases at one exact boundary root."""

    current: tuple[ProviderResourceActionRepresentabilityMeasurementV2, ...]
    candidate_maximal: tuple[
        ProviderResourceActionRepresentabilityMeasurementV2, ...]

    def __post_init__(self) -> None:
        for mode, values in (
            (ProviderResourceActionRepresentabilityModeV2.CURRENT,
             self.current),
            (ProviderResourceActionRepresentabilityModeV2.CANDIDATE_MAXIMAL,
             self.candidate_maximal),
        ):
            if type(values) is not tuple or not values or any(
                    type(value)
                    is not ProviderResourceActionRepresentabilityMeasurementV2
                    for value in values):
                raise TypeError('enumeration mode must be a nonempty typed '
                                'tuple.')
            if any(value.mode is not mode for value in values):
                raise ValueError('enumeration measurement mode is crossed.')
            sequences = tuple(value.case_sequence for value in values)
            if sequences != tuple(sorted(sequences)):
                raise ValueError('enumeration measurements are not ordered.')
        if tuple(value.case_sequence for value in self.current) != tuple(
                value.case_sequence for value in self.candidate_maximal):
            raise ValueError('enumeration modes cover different case rows.')


class CanonicalPayloadV2(Protocol):
    """Structural payload accepted by the canonical size enumerator."""

    @property
    def canonical_bytes(self) -> bytes:
        ...


@dataclasses.dataclass(frozen=True)
class _CaseProjectorV2:
    """One code-owned fixed-signature projector in the sealed dispatch."""

    case_id: str

    def __post_init__(self) -> None:
        if _CASE_ID_RE.fullmatch(self.case_id) is None:
            raise ValueError('case projector ID is invalid.')

    def __call__(
        self,
        representability_input: ProviderResourceActionRepresentabilityInputV2,
        mode: ProviderResourceActionRepresentabilityModeV2,
    ) -> CanonicalPayloadV2:
        return _project_provider_resource_action_representability_case_v2(
            representability_input, mode, self.case_id)


def _not_representable_response_v2(
    representability_input:
    ProviderResourceActionPreflightRepresentabilityInputV2,
    reason: str,
) -> preflight_v2.ProviderAuthorityPreflightResponseV2:
    response_value = (
        representability_input.candidate_complete_response.canonical_value())
    response_value['disposition'] = 'not_representable'
    response_value['reason'] = reason
    for field in ('resolved_cohort', 'execution_capsule',
                  'executor_policy_proof', 'worker_identity'):
        response_value[field] = None
    return preflight_v2.provider_authority_preflight_response_from_value_v2(
        response_value)


def _execution_members_v2(
    representability_input: (
        ProviderResourceActionAdmissionRepresentabilityInputV2 |
        ProviderResourceActionPreIoRepresentabilityInputV2),
) -> tuple[actions.ProviderLifecycleInvocationV2,
           actions.ProviderLifecyclePlanV2,
           actions.ProviderKubernetesExecutionConfigV2 |
           actions.ProviderKubernetesDownExecutionConfigV2]:
    if type(representability_input) is (
            ProviderResourceActionAdmissionRepresentabilityInputV2):
        invocation = representability_input.candidate_spec.invocation
        plan = representability_input.candidate_spec.provider_plan
    else:
        assert type(representability_input) is (
            ProviderResourceActionPreIoRepresentabilityInputV2)
        invocation = representability_input.stored_spec.invocation
        plan = representability_input.stored_spec.provider_plan
    config = (invocation.require_launch().execution_config
              if invocation.action_kind is kernel_actions.ActionKind.LAUNCH else
              invocation.require_down().execution_config)
    return invocation, plan, config


def _project_provider_resource_action_representability_case_v2(
    representability_input: ProviderResourceActionRepresentabilityInputV2,
    mode: ProviderResourceActionRepresentabilityModeV2,
    case_id: str,
) -> CanonicalPayloadV2:
    """Project one explicit code-owned case; never consume artifact arguments."""

    if type(mode) is not ProviderResourceActionRepresentabilityModeV2:
        raise TypeError('representability projector mode is invalid.')
    # Known live bytes are invariant across modes.  Candidate substitution is
    # limited to the explicit response/progress/result builders below; it
    # never rewrites a frozen spec, cohort, worker identity, or attestation.
    if type(representability_input) is (
            ProviderResourceActionPreflightRepresentabilityInputV2):
        preflight_root = representability_input
        if case_id.endswith('.preflight_request'):
            return preflight_root.request
        if '.preflight_response.complete' in case_id:
            return preflight_root.candidate_complete_response
        marker = '.preflight_response.not_representable.'
        if marker in case_id:
            return _not_representable_response_v2(preflight_root,
                                                  case_id.split(marker, 1)[1])
        response = preflight_root.candidate_complete_response
        assert response.resolved_cohort is not None
        if '.cohort.resolved' in case_id:
            return response.resolved_cohort
        if '.worker_identity.eligible_' in case_id:
            index = int(case_id.rsplit('_', 1)[1])
            return preflight_root.accepted_memberships[
                index].registration.worker
        if '.renderer_input.native_v2' in case_id:
            if type(preflight_root.construction) is not (
                    ProviderResourceActionLaunchRepresentabilityConstructionV2):
                raise ProviderResourceActionRepresentabilityError(
                    'renderer-input case has a down construction.')
            return preflight_root.construction.renderer_input
        if '.rendered_body.' in case_id:
            if type(preflight_root.construction) is not (
                    ProviderResourceActionLaunchRepresentabilityConstructionV2):
                raise ProviderResourceActionRepresentabilityError(
                    'rendered-body case has a down construction.')
            role = case_id.rsplit('.', 1)[1]
            by_role = {
                item.role.value: item.request_body for item in
                preflight_root.construction.execution_capsule.objects
            }
            return by_role[role]
        if '.cleanup_target.' in case_id:
            if type(preflight_root.construction) is not (
                    ProviderResourceActionDownRepresentabilityConstructionV2):
                raise ProviderResourceActionRepresentabilityError(
                    'cleanup-target case has a launch construction.')
            expected_case = case_id.rsplit('.cleanup_target.', 1)[1]
            source = preflight_root.construction.cleanup_rederivation_input
            if expected_case == 'completed_launch':
                if type(source) is not (
                        cleanup_v2.
                        ProviderKubernetesCompletedCleanupRederivationInputV2):
                    raise ProviderResourceActionRepresentabilityUnavailableError(
                        'fixture does not contain completed cleanup source.')
            else:
                if type(source) is not (
                        cleanup_v2.
                        ProviderKubernetesPartialCleanupRederivationInputV2):
                    raise ProviderResourceActionRepresentabilityUnavailableError(
                        'fixture does not contain partial cleanup source.')
                expected_partial_case = expected_case.removeprefix('partial.')
                source_progress = source.source_progress.cursor
                if type(source_progress
                       ) is not progress.ProviderLaunchProgressV1:
                    raise ProviderResourceActionRepresentabilityError(
                        'partial cleanup source does not contain launch '
                        'progress.')
                actual_partial_case = next(
                    item.case_id
                    for item in actions.
                    enumerate_provider_partial_launch_cleanup_legal_shapes_v1()
                    if item.launch_phase == source_progress.phase.value and
                    item.committed_object_count == len(
                        source_progress.committed_effects) and
                    item.cluster_row_disposition.value ==
                    source.cluster_row.disposition.value)
                if expected_partial_case != actual_partial_case:
                    raise ProviderResourceActionRepresentabilityUnavailableError(
                        'fixture lacks this legal partial-cleanup preimage.')
            return preflight_root.construction.rederived_cleanup_target
        if '.execution_capsule.' in case_id:
            return preflight_root.construction.execution_capsule
    elif type(representability_input) is (
            ProviderResourceActionAdmissionRepresentabilityInputV2):
        admission_root = representability_input
        if case_id.endswith('.preflight_request'):
            return admission_root.request
        if '.preflight_response.complete' in case_id:
            return admission_root.complete_response
        if '.worker_identity.eligible_' in case_id:
            index = int(case_id.rsplit('_', 1)[1])
            return admission_root.accepted_memberships[
                index].registration.worker
        invocation, plan, config = _execution_members_v2(admission_root)
        if case_id.endswith('.execution_capsule'):
            return config.capsule
        if case_id.endswith('.execution_config'):
            return config
        if case_id.endswith('.invocation'):
            return invocation
        if case_id.endswith('.plan'):
            return plan
        if case_id.endswith('.action_spec'):
            return admission_root.candidate_spec
    elif type(representability_input) is (
            ProviderResourceActionPreIoRepresentabilityInputV2):
        pre_io_root = representability_input
        if case_id.endswith('.worker_identity'):
            return pre_io_root.accepted_membership.registration.worker
        if case_id.endswith('.attempt_attestation'):
            return pre_io_root.worker_attestation
        # Progress, no-effect, terminal-return, quiescence, and reducer outcome
        # cases require the exact response-origin/maximal builders.  Keep this
        # an explicit hard gate until those builders and their final fixture
        # preimages land; returning the current cursor for another case would
        # be false representability evidence.
        if any(marker in case_id
               for marker in ('.progress.', '.no_effect.', '.request_return.',
                              '.quiescence.', '.action_outcome.')):
            raise ProviderResourceActionRepresentabilityUnavailableError(
                'final response-origin representability builders are not '
                'installed.')
        invocation, plan, config = _execution_members_v2(pre_io_root)
        if case_id.endswith('.execution_capsule'):
            return config.capsule
        if case_id.endswith('.execution_config'):
            return config
        if case_id.endswith('.invocation'):
            return invocation
        if case_id.endswith('.plan'):
            return plan
        if case_id.endswith('.action_spec'):
            return pre_io_root.stored_spec
    else:
        raise TypeError('representability input has an invalid exact type.')
    raise ProviderResourceActionRepresentabilityError(
        f'case projector {case_id!r} is not applicable to its boundary root.')


def _measure_provider_resource_action_representability_payload_v2(
    case: ProviderResourceActionRepresentabilityCaseV2,
    mode: ProviderResourceActionRepresentabilityModeV2,
    payload: CanonicalPayloadV2,
) -> ProviderResourceActionRepresentabilityMeasurementV2:
    canonical_bytes = payload.canonical_bytes
    byte_count = len(canonical_bytes)
    if byte_count > _MAX_VALUE_CANONICAL_BYTES:
        raise ProviderResourceActionRepresentabilityError(
            f'{case.case_id} exceeds the 65536-byte value budget.')
    if (case.payload_kind
            is ProviderResourceActionRepresentabilityPayloadKindV2.ACTION_SPEC
            and byte_count > _ACTION_SPEC_MAX_CANONICAL_BYTES):
        raise ProviderResourceActionRepresentabilityError(
            f'{case.case_id} exceeds the 60000-byte action-spec budget.')
    return ProviderResourceActionRepresentabilityMeasurementV2(
        case_sequence=case.sequence,
        case_id=case.case_id,
        mode=mode,
        canonical_byte_count=byte_count,
        sha256=hashlib.sha256(canonical_bytes).hexdigest())


# Explicit keys are intentional: repository AST inventory compares this exact
# literal dispatch with the exact ordered artifact rows.
_CASE_PROJECTORS_V2: dict[str, _CaseProjectorV2] = {
    "launch.complete_preflight.preflight_request":
        _CaseProjectorV2("launch.complete_preflight.preflight_request"),
    "launch.complete_preflight.preflight_response.complete": _CaseProjectorV2(
        "launch.complete_preflight.preflight_response.complete"),
    "launch.complete_preflight.preflight_response.not_representable.request_contract":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.request_contract"
        ),
    "launch.complete_preflight.preflight_response.not_representable.secret_or_tls_material":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.secret_or_tls_material"
        ),
    "launch.complete_preflight.preflight_response.not_representable.source_mismatch":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.source_mismatch"
        ),
    "launch.complete_preflight.preflight_response.not_representable.policy_configured_or_mutated":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.policy_configured_or_mutated"
        ),
    "launch.complete_preflight.preflight_response.not_representable.managed_secrets":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.managed_secrets"
        ),
    "launch.complete_preflight.preflight_response.not_representable.multi_task":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.multi_task"
        ),
    "launch.complete_preflight.preflight_response.not_representable.multi_node":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.multi_node"
        ),
    "launch.complete_preflight.preflight_response.not_representable.multi_resource":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.multi_resource"
        ),
    "launch.complete_preflight.preflight_response.not_representable.mount_or_storage":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.mount_or_storage"
        ),
    "launch.complete_preflight.preflight_response.not_representable.non_kubernetes":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.non_kubernetes"
        ),
    "launch.complete_preflight.preflight_response.not_representable.spot":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.spot"
        ),
    "launch.complete_preflight.preflight_response.not_representable.non_direct_pod_topology":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.non_direct_pod_topology"
        ),
    "launch.complete_preflight.preflight_response.not_representable.port_contract":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.port_contract"
        ),
    "launch.complete_preflight.preflight_response.not_representable.reserved_label_collision":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.reserved_label_collision"
        ),
    "launch.complete_preflight.preflight_response.not_representable.mutable_image":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.mutable_image"
        ),
    "launch.complete_preflight.preflight_response.not_representable.custom_provider_implementation":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.custom_provider_implementation"
        ),
    "launch.complete_preflight.preflight_response.not_representable.preflight_unavailable_or_invalid":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.preflight_unavailable_or_invalid"
        ),
    "launch.complete_preflight.preflight_response.not_representable.authority_worker_attestation":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.authority_worker_attestation"
        ),
    "launch.complete_preflight.preflight_response.not_representable.authorization_or_principal_drift":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.authorization_or_principal_drift"
        ),
    "launch.complete_preflight.preflight_response.not_representable.prerequisite_or_network_drift":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.prerequisite_or_network_drift"
        ),
    "launch.complete_preflight.preflight_response.not_representable.admitted_object_contract":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.admitted_object_contract"
        ),
    "launch.complete_preflight.preflight_response.not_representable.runtime_or_job_contract":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.runtime_or_job_contract"
        ),
    "launch.complete_preflight.preflight_response.not_representable.unrepresented_execution_config":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.unrepresented_execution_config"
        ),
    "launch.complete_preflight.preflight_response.not_representable.unrepresented_resource":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.unrepresented_resource"
        ),
    "launch.complete_preflight.preflight_response.not_representable.unfrozen_placement":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.unfrozen_placement"
        ),
    "launch.complete_preflight.preflight_response.not_representable.unfrozen_identity":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.unfrozen_identity"
        ),
    "launch.complete_preflight.preflight_response.not_representable.unfrozen_kubernetes_scope":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.unfrozen_kubernetes_scope"
        ),
    "launch.complete_preflight.preflight_response.not_representable.target_mismatch":
        _CaseProjectorV2(
            "launch.complete_preflight.preflight_response.not_representable.target_mismatch"
        ),
    "launch.complete_preflight.cohort.resolved":
        _CaseProjectorV2("launch.complete_preflight.cohort.resolved"),
    "launch.complete_preflight.worker_identity.eligible_0": _CaseProjectorV2(
        "launch.complete_preflight.worker_identity.eligible_0"),
    "launch.complete_preflight.worker_identity.eligible_1": _CaseProjectorV2(
        "launch.complete_preflight.worker_identity.eligible_1"),
    "launch.complete_preflight.renderer_input.native_v2":
        _CaseProjectorV2("launch.complete_preflight.renderer_input.native_v2"),
    "launch.complete_preflight.rendered_body.head_ssh_service":
        _CaseProjectorV2(
            "launch.complete_preflight.rendered_body.head_ssh_service"),
    "launch.complete_preflight.rendered_body.head_service": _CaseProjectorV2(
        "launch.complete_preflight.rendered_body.head_service"),
    "launch.complete_preflight.rendered_body.head_pod":
        _CaseProjectorV2("launch.complete_preflight.rendered_body.head_pod"),
    "launch.complete_preflight.execution_capsule.native_v2": _CaseProjectorV2(
        "launch.complete_preflight.execution_capsule.native_v2"),
    "down.complete_preflight.preflight_request":
        _CaseProjectorV2("down.complete_preflight.preflight_request"),
    "down.complete_preflight.preflight_response.complete":
        _CaseProjectorV2("down.complete_preflight.preflight_response.complete"),
    "down.complete_preflight.preflight_response.not_representable.request_contract":
        _CaseProjectorV2(
            "down.complete_preflight.preflight_response.not_representable.request_contract"
        ),
    "down.complete_preflight.preflight_response.not_representable.prior_launch_basis":
        _CaseProjectorV2(
            "down.complete_preflight.preflight_response.not_representable.prior_launch_basis"
        ),
    "down.complete_preflight.preflight_response.not_representable.target_mismatch":
        _CaseProjectorV2(
            "down.complete_preflight.preflight_response.not_representable.target_mismatch"
        ),
    "down.complete_preflight.preflight_response.not_representable.preflight_unavailable_or_invalid":
        _CaseProjectorV2(
            "down.complete_preflight.preflight_response.not_representable.preflight_unavailable_or_invalid"
        ),
    "down.complete_preflight.preflight_response.not_representable.authority_worker_attestation":
        _CaseProjectorV2(
            "down.complete_preflight.preflight_response.not_representable.authority_worker_attestation"
        ),
    "down.complete_preflight.preflight_response.not_representable.authorization_or_principal_drift":
        _CaseProjectorV2(
            "down.complete_preflight.preflight_response.not_representable.authorization_or_principal_drift"
        ),
    "down.complete_preflight.preflight_response.not_representable.prerequisite_or_network_drift":
        _CaseProjectorV2(
            "down.complete_preflight.preflight_response.not_representable.prerequisite_or_network_drift"
        ),
    "down.complete_preflight.preflight_response.not_representable.policy_configured_or_mutated":
        _CaseProjectorV2(
            "down.complete_preflight.preflight_response.not_representable.policy_configured_or_mutated"
        ),
    "down.complete_preflight.preflight_response.not_representable.unrepresented_execution_config":
        _CaseProjectorV2(
            "down.complete_preflight.preflight_response.not_representable.unrepresented_execution_config"
        ),
    "down.complete_preflight.preflight_response.not_representable.unfrozen_kubernetes_scope":
        _CaseProjectorV2(
            "down.complete_preflight.preflight_response.not_representable.unfrozen_kubernetes_scope"
        ),
    "down.complete_preflight.cohort.resolved":
        _CaseProjectorV2("down.complete_preflight.cohort.resolved"),
    "down.complete_preflight.worker_identity.eligible_0":
        _CaseProjectorV2("down.complete_preflight.worker_identity.eligible_0"),
    "down.complete_preflight.worker_identity.eligible_1":
        _CaseProjectorV2("down.complete_preflight.worker_identity.eligible_1"),
    "down.complete_preflight.cleanup_target.completed_launch": _CaseProjectorV2(
        "down.complete_preflight.cleanup_target.completed_launch"),
    "down.complete_preflight.cleanup_target.partial.create_intent_0_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.create_intent_0_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.objects_partial_1_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.objects_partial_1_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.create_intent_1_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.create_intent_1_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.objects_partial_2_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.objects_partial_2_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.create_intent_2_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.create_intent_2_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.objects_partial_3_unscheduled_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.objects_partial_3_unscheduled_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.objects_exact_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.objects_exact_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.handle_intent_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.handle_intent_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.handle_committed_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.handle_committed_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.handle_committed_exact_handle":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.handle_committed_exact_handle"
        ),
    "down.complete_preflight.cleanup_target.partial.runtime_ready_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.runtime_ready_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.runtime_ready_exact_handle":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.runtime_ready_exact_handle"
        ),
    "down.complete_preflight.cleanup_target.partial.job_intent_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.job_intent_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.job_intent_exact_handle":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.job_intent_exact_handle"
        ),
    "down.complete_preflight.cleanup_target.partial.job_committed_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.job_committed_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.job_committed_exact_handle":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.job_committed_exact_handle"
        ),
    "down.complete_preflight.cleanup_target.partial.job_running_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.job_running_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.job_running_exact_handle":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.job_running_exact_handle"
        ),
    "down.complete_preflight.cleanup_target.partial.endpoint_resolved_not_found":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.endpoint_resolved_not_found"
        ),
    "down.complete_preflight.cleanup_target.partial.endpoint_resolved_exact_handle":
        _CaseProjectorV2(
            "down.complete_preflight.cleanup_target.partial.endpoint_resolved_exact_handle"
        ),
    "down.complete_preflight.execution_capsule.native_v2":
        _CaseProjectorV2("down.complete_preflight.execution_capsule.native_v2"),
    "launch.linked_admission.preflight_request":
        _CaseProjectorV2("launch.linked_admission.preflight_request"),
    "launch.linked_admission.preflight_response.complete":
        _CaseProjectorV2("launch.linked_admission.preflight_response.complete"),
    "launch.linked_admission.worker_identity.eligible_0":
        _CaseProjectorV2("launch.linked_admission.worker_identity.eligible_0"),
    "launch.linked_admission.worker_identity.eligible_1":
        _CaseProjectorV2("launch.linked_admission.worker_identity.eligible_1"),
    "launch.linked_admission.execution_capsule":
        _CaseProjectorV2("launch.linked_admission.execution_capsule"),
    "launch.linked_admission.execution_config":
        _CaseProjectorV2("launch.linked_admission.execution_config"),
    "launch.linked_admission.invocation":
        _CaseProjectorV2("launch.linked_admission.invocation"),
    "launch.linked_admission.plan":
        _CaseProjectorV2("launch.linked_admission.plan"),
    "launch.linked_admission.action_spec":
        _CaseProjectorV2("launch.linked_admission.action_spec"),
    "down.linked_admission.preflight_request":
        _CaseProjectorV2("down.linked_admission.preflight_request"),
    "down.linked_admission.preflight_response.complete":
        _CaseProjectorV2("down.linked_admission.preflight_response.complete"),
    "down.linked_admission.worker_identity.eligible_0":
        _CaseProjectorV2("down.linked_admission.worker_identity.eligible_0"),
    "down.linked_admission.worker_identity.eligible_1":
        _CaseProjectorV2("down.linked_admission.worker_identity.eligible_1"),
    "down.linked_admission.execution_capsule":
        _CaseProjectorV2("down.linked_admission.execution_capsule"),
    "down.linked_admission.execution_config":
        _CaseProjectorV2("down.linked_admission.execution_config"),
    "down.linked_admission.invocation":
        _CaseProjectorV2("down.linked_admission.invocation"),
    "down.linked_admission.plan":
        _CaseProjectorV2("down.linked_admission.plan"),
    "down.linked_admission.action_spec":
        _CaseProjectorV2("down.linked_admission.action_spec"),
    "launch.pre_io.worker_identity":
        _CaseProjectorV2("launch.pre_io.worker_identity"),
    "launch.pre_io.attempt_attestation":
        _CaseProjectorV2("launch.pre_io.attempt_attestation"),
    "launch.pre_io.execution_capsule":
        _CaseProjectorV2("launch.pre_io.execution_capsule"),
    "launch.pre_io.execution_config":
        _CaseProjectorV2("launch.pre_io.execution_config"),
    "launch.pre_io.invocation": _CaseProjectorV2("launch.pre_io.invocation"),
    "launch.pre_io.plan": _CaseProjectorV2("launch.pre_io.plan"),
    "launch.pre_io.action_spec": _CaseProjectorV2("launch.pre_io.action_spec"),
    "down.pre_io.worker_identity":
        _CaseProjectorV2("down.pre_io.worker_identity"),
    "down.pre_io.attempt_attestation":
        _CaseProjectorV2("down.pre_io.attempt_attestation"),
    "down.pre_io.execution_capsule":
        _CaseProjectorV2("down.pre_io.execution_capsule"),
    "down.pre_io.execution_config":
        _CaseProjectorV2("down.pre_io.execution_config"),
    "down.pre_io.invocation": _CaseProjectorV2("down.pre_io.invocation"),
    "down.pre_io.plan": _CaseProjectorV2("down.pre_io.plan"),
    "down.pre_io.action_spec": _CaseProjectorV2("down.pre_io.action_spec"),
    "launch.pre_io.progress.create_intent.head_ssh_service": _CaseProjectorV2(
        "launch.pre_io.progress.create_intent.head_ssh_service"),
    "launch.pre_io.progress.objects_partial.one_slot":
        _CaseProjectorV2("launch.pre_io.progress.objects_partial.one_slot"),
    "launch.pre_io.progress.create_intent.head_service":
        _CaseProjectorV2("launch.pre_io.progress.create_intent.head_service"),
    "launch.pre_io.progress.objects_partial.two_slots":
        _CaseProjectorV2("launch.pre_io.progress.objects_partial.two_slots"),
    "launch.pre_io.progress.create_intent.head_pod":
        _CaseProjectorV2("launch.pre_io.progress.create_intent.head_pod"),
    "launch.pre_io.progress.objects_partial.three_slots_unscheduled":
        _CaseProjectorV2(
            "launch.pre_io.progress.objects_partial.three_slots_unscheduled"),
    "launch.pre_io.progress.objects_exact.unscheduled":
        _CaseProjectorV2("launch.pre_io.progress.objects_exact.unscheduled"),
    "launch.pre_io.progress.objects_exact.scheduled":
        _CaseProjectorV2("launch.pre_io.progress.objects_exact.scheduled"),
    "launch.pre_io.progress.handle_intent":
        _CaseProjectorV2("launch.pre_io.progress.handle_intent"),
    "launch.pre_io.progress.handle_committed":
        _CaseProjectorV2("launch.pre_io.progress.handle_committed"),
    "launch.pre_io.progress.runtime_ready":
        _CaseProjectorV2("launch.pre_io.progress.runtime_ready"),
    "launch.pre_io.progress.job_intent":
        _CaseProjectorV2("launch.pre_io.progress.job_intent"),
    "launch.pre_io.progress.job_committed":
        _CaseProjectorV2("launch.pre_io.progress.job_committed"),
    "launch.pre_io.progress.job_running":
        _CaseProjectorV2("launch.pre_io.progress.job_running"),
    "launch.pre_io.progress.endpoint_resolved":
        _CaseProjectorV2("launch.pre_io.progress.endpoint_resolved"),
    "launch.pre_io.progress.succeeded":
        _CaseProjectorV2("launch.pre_io.progress.succeeded"),
    "down.pre_io.progress.target_resolved.all_present":
        _CaseProjectorV2("down.pre_io.progress.target_resolved.all_present"),
    "down.pre_io.progress.target_resolved.all_absent":
        _CaseProjectorV2("down.pre_io.progress.target_resolved.all_absent"),
    "down.pre_io.progress.delete_intent.head_pod":
        _CaseProjectorV2("down.pre_io.progress.delete_intent.head_pod"),
    "down.pre_io.progress.delete_partial.head_pod":
        _CaseProjectorV2("down.pre_io.progress.delete_partial.head_pod"),
    "down.pre_io.progress.delete_intent.head_service":
        _CaseProjectorV2("down.pre_io.progress.delete_intent.head_service"),
    "down.pre_io.progress.delete_partial.head_service":
        _CaseProjectorV2("down.pre_io.progress.delete_partial.head_service"),
    "down.pre_io.progress.delete_intent.head_ssh_service":
        _CaseProjectorV2("down.pre_io.progress.delete_intent.head_ssh_service"),
    "down.pre_io.progress.delete_partial.head_ssh_service": _CaseProjectorV2(
        "down.pre_io.progress.delete_partial.head_ssh_service"),
    "down.pre_io.progress.absence_exact":
        _CaseProjectorV2("down.pre_io.progress.absence_exact"),
    "down.pre_io.progress.handle_remove_intent.exact_handle": _CaseProjectorV2(
        "down.pre_io.progress.handle_remove_intent.exact_handle"),
    "down.pre_io.progress.handle_remove_intent.already_absent":
        _CaseProjectorV2(
            "down.pre_io.progress.handle_remove_intent.already_absent"),
    "down.pre_io.progress.handle_removed.removed_exact":
        _CaseProjectorV2("down.pre_io.progress.handle_removed.removed_exact"),
    "down.pre_io.progress.handle_removed.already_absent":
        _CaseProjectorV2("down.pre_io.progress.handle_removed.already_absent"),
    "down.pre_io.progress.succeeded.removed_exact":
        _CaseProjectorV2("down.pre_io.progress.succeeded.removed_exact"),
    "down.pre_io.progress.succeeded.already_absent":
        _CaseProjectorV2("down.pre_io.progress.succeeded.already_absent"),
    "launch.pre_io.no_effect.call_not_entered.effect_0":
        _CaseProjectorV2("launch.pre_io.no_effect.call_not_entered.effect_0"),
    "launch.pre_io.no_effect.call_not_entered.effect_1":
        _CaseProjectorV2("launch.pre_io.no_effect.call_not_entered.effect_1"),
    "launch.pre_io.no_effect.call_not_entered.effect_2":
        _CaseProjectorV2("launch.pre_io.no_effect.call_not_entered.effect_2"),
    "launch.pre_io.no_effect.call_not_entered.effect_3":
        _CaseProjectorV2("launch.pre_io.no_effect.call_not_entered.effect_3"),
    "launch.pre_io.no_effect.call_not_entered.effect_4":
        _CaseProjectorV2("launch.pre_io.no_effect.call_not_entered.effect_4"),
    "launch.pre_io.no_effect.core_v1_422.effect_0":
        _CaseProjectorV2("launch.pre_io.no_effect.core_v1_422.effect_0"),
    "launch.pre_io.no_effect.core_v1_422.effect_1":
        _CaseProjectorV2("launch.pre_io.no_effect.core_v1_422.effect_1"),
    "launch.pre_io.no_effect.core_v1_422.effect_2":
        _CaseProjectorV2("launch.pre_io.no_effect.core_v1_422.effect_2"),
    "launch.pre_io.no_effect.cluster_record.rolled_back_not_found":
        _CaseProjectorV2(
            "launch.pre_io.no_effect.cluster_record.rolled_back_not_found"),
    "launch.pre_io.no_effect.cluster_record.different_uuid_conflict":
        _CaseProjectorV2(
            "launch.pre_io.no_effect.cluster_record.different_uuid_conflict"),
    "launch.pre_io.no_effect.skylet.schema_rejected_not_found":
        _CaseProjectorV2(
            "launch.pre_io.no_effect.skylet.schema_rejected_not_found"),
    "launch.pre_io.no_effect.skylet.same_key_conflict.succeeded":
        _CaseProjectorV2(
            "launch.pre_io.no_effect.skylet.same_key_conflict.succeeded"),
    "launch.pre_io.no_effect.skylet.same_key_conflict.failed": _CaseProjectorV2(
        "launch.pre_io.no_effect.skylet.same_key_conflict.failed"),
    "launch.pre_io.no_effect.skylet.same_key_conflict.blocked":
        _CaseProjectorV2(
            "launch.pre_io.no_effect.skylet.same_key_conflict.blocked"),
    "launch.pre_io.quiescence.phase.create_intent.head_ssh_service":
        _CaseProjectorV2(
            "launch.pre_io.quiescence.phase.create_intent.head_ssh_service"),
    "launch.pre_io.quiescence.phase.objects_partial.one_slot": _CaseProjectorV2(
        "launch.pre_io.quiescence.phase.objects_partial.one_slot"),
    "launch.pre_io.quiescence.phase.create_intent.head_service":
        _CaseProjectorV2(
            "launch.pre_io.quiescence.phase.create_intent.head_service"),
    "launch.pre_io.quiescence.phase.objects_partial.two_slots":
        _CaseProjectorV2(
            "launch.pre_io.quiescence.phase.objects_partial.two_slots"),
    "launch.pre_io.quiescence.phase.create_intent.head_pod": _CaseProjectorV2(
        "launch.pre_io.quiescence.phase.create_intent.head_pod"),
    "launch.pre_io.quiescence.phase.objects_partial.three_slots_unscheduled":
        _CaseProjectorV2(
            "launch.pre_io.quiescence.phase.objects_partial.three_slots_unscheduled"
        ),
    "launch.pre_io.quiescence.phase.objects_exact.unscheduled":
        _CaseProjectorV2(
            "launch.pre_io.quiescence.phase.objects_exact.unscheduled"),
    "launch.pre_io.quiescence.phase.objects_exact.scheduled": _CaseProjectorV2(
        "launch.pre_io.quiescence.phase.objects_exact.scheduled"),
    "launch.pre_io.quiescence.phase.handle_intent":
        _CaseProjectorV2("launch.pre_io.quiescence.phase.handle_intent"),
    "launch.pre_io.quiescence.phase.handle_committed":
        _CaseProjectorV2("launch.pre_io.quiescence.phase.handle_committed"),
    "launch.pre_io.quiescence.phase.runtime_ready":
        _CaseProjectorV2("launch.pre_io.quiescence.phase.runtime_ready"),
    "launch.pre_io.quiescence.phase.job_intent":
        _CaseProjectorV2("launch.pre_io.quiescence.phase.job_intent"),
    "launch.pre_io.quiescence.phase.job_committed":
        _CaseProjectorV2("launch.pre_io.quiescence.phase.job_committed"),
    "launch.pre_io.quiescence.phase.job_running":
        _CaseProjectorV2("launch.pre_io.quiescence.phase.job_running"),
    "launch.pre_io.quiescence.phase.endpoint_resolved":
        _CaseProjectorV2("launch.pre_io.quiescence.phase.endpoint_resolved"),
    "launch.pre_io.quiescence.e_only.effect_0":
        _CaseProjectorV2("launch.pre_io.quiescence.e_only.effect_0"),
    "launch.pre_io.quiescence.e_only.effect_1":
        _CaseProjectorV2("launch.pre_io.quiescence.e_only.effect_1"),
    "launch.pre_io.quiescence.e_only.effect_2":
        _CaseProjectorV2("launch.pre_io.quiescence.e_only.effect_2"),
    "launch.pre_io.quiescence.e_only.effect_3":
        _CaseProjectorV2("launch.pre_io.quiescence.e_only.effect_3"),
    "launch.pre_io.quiescence.e_only.effect_4":
        _CaseProjectorV2("launch.pre_io.quiescence.e_only.effect_4"),
    "launch.pre_io.quiescence.e_plus_n.effect_0":
        _CaseProjectorV2("launch.pre_io.quiescence.e_plus_n.effect_0"),
    "launch.pre_io.quiescence.e_plus_n.effect_1":
        _CaseProjectorV2("launch.pre_io.quiescence.e_plus_n.effect_1"),
    "launch.pre_io.quiescence.e_plus_n.effect_2":
        _CaseProjectorV2("launch.pre_io.quiescence.e_plus_n.effect_2"),
    "launch.pre_io.quiescence.e_plus_n.effect_3":
        _CaseProjectorV2("launch.pre_io.quiescence.e_plus_n.effect_3"),
    "launch.pre_io.quiescence.e_plus_n.effect_4":
        _CaseProjectorV2("launch.pre_io.quiescence.e_plus_n.effect_4"),
    "launch.pre_io.quiescence.same_claim_commit.effect_0":
        _CaseProjectorV2("launch.pre_io.quiescence.same_claim_commit.effect_0"),
    "launch.pre_io.quiescence.same_claim_commit.effect_1":
        _CaseProjectorV2("launch.pre_io.quiescence.same_claim_commit.effect_1"),
    "launch.pre_io.quiescence.same_claim_commit.effect_2":
        _CaseProjectorV2("launch.pre_io.quiescence.same_claim_commit.effect_2"),
    "launch.pre_io.quiescence.same_claim_commit.effect_3":
        _CaseProjectorV2("launch.pre_io.quiescence.same_claim_commit.effect_3"),
    "launch.pre_io.quiescence.same_claim_commit.effect_4":
        _CaseProjectorV2("launch.pre_io.quiescence.same_claim_commit.effect_4"),
    "launch.pre_io.quiescence.same_claim_adoption.effect_0": _CaseProjectorV2(
        "launch.pre_io.quiescence.same_claim_adoption.effect_0"),
    "launch.pre_io.quiescence.same_claim_adoption.effect_1": _CaseProjectorV2(
        "launch.pre_io.quiescence.same_claim_adoption.effect_1"),
    "launch.pre_io.quiescence.same_claim_adoption.effect_2": _CaseProjectorV2(
        "launch.pre_io.quiescence.same_claim_adoption.effect_2"),
    "launch.pre_io.quiescence.same_claim_adoption.effect_3": _CaseProjectorV2(
        "launch.pre_io.quiescence.same_claim_adoption.effect_3"),
    "launch.pre_io.quiescence.same_claim_adoption.effect_4": _CaseProjectorV2(
        "launch.pre_io.quiescence.same_claim_adoption.effect_4"),
    "launch.pre_io.quiescence.later_generation_adoption.effect_0":
        _CaseProjectorV2(
            "launch.pre_io.quiescence.later_generation_adoption.effect_0"),
    "launch.pre_io.quiescence.later_generation_adoption.effect_1":
        _CaseProjectorV2(
            "launch.pre_io.quiescence.later_generation_adoption.effect_1"),
    "launch.pre_io.quiescence.later_generation_adoption.effect_2":
        _CaseProjectorV2(
            "launch.pre_io.quiescence.later_generation_adoption.effect_2"),
    "launch.pre_io.quiescence.later_generation_adoption.effect_3":
        _CaseProjectorV2(
            "launch.pre_io.quiescence.later_generation_adoption.effect_3"),
    "launch.pre_io.quiescence.later_generation_adoption.effect_4":
        _CaseProjectorV2(
            "launch.pre_io.quiescence.later_generation_adoption.effect_4"),
    "launch.pre_io.quiescence.generation_reset.effect_0":
        _CaseProjectorV2("launch.pre_io.quiescence.generation_reset.effect_0"),
    "launch.pre_io.quiescence.generation_reset.effect_1":
        _CaseProjectorV2("launch.pre_io.quiescence.generation_reset.effect_1"),
    "launch.pre_io.quiescence.generation_reset.effect_2":
        _CaseProjectorV2("launch.pre_io.quiescence.generation_reset.effect_2"),
    "launch.pre_io.quiescence.generation_reset.effect_3":
        _CaseProjectorV2("launch.pre_io.quiescence.generation_reset.effect_3"),
    "launch.pre_io.quiescence.generation_reset.effect_4":
        _CaseProjectorV2("launch.pre_io.quiescence.generation_reset.effect_4"),
    "launch.pre_io.request_return.domain_success":
        _CaseProjectorV2("launch.pre_io.request_return.domain_success"),
    "launch.pre_io.action_outcome.domain_success":
        _CaseProjectorV2("launch.pre_io.action_outcome.domain_success"),
    "launch.pre_io.request_return.domain_error.revision_zero.transient":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.revision_zero.transient"
        ),
    "launch.pre_io.action_outcome.domain_error.revision_zero.transient":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.revision_zero.transient"
        ),
    "launch.pre_io.request_return.domain_error.revision_zero.capacity":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.revision_zero.capacity"),
    "launch.pre_io.action_outcome.domain_error.revision_zero.capacity":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.revision_zero.capacity"),
    "launch.pre_io.request_return.domain_error.revision_zero.quota":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.revision_zero.quota"),
    "launch.pre_io.action_outcome.domain_error.revision_zero.quota":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.revision_zero.quota"),
    "launch.pre_io.request_return.domain_error.revision_zero.rate_limited":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.revision_zero.rate_limited"
        ),
    "launch.pre_io.action_outcome.domain_error.revision_zero.rate_limited":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.revision_zero.rate_limited"
        ),
    "launch.pre_io.request_return.domain_error.revision_zero.invalid_request":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.revision_zero.invalid_request"
        ),
    "launch.pre_io.action_outcome.domain_error.revision_zero.invalid_request":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.revision_zero.invalid_request"
        ),
    "launch.pre_io.request_return.domain_error.revision_zero.permission":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.revision_zero.permission"
        ),
    "launch.pre_io.action_outcome.domain_error.revision_zero.permission":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.revision_zero.permission"
        ),
    "launch.pre_io.request_return.domain_error.revision_zero.conflict":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.revision_zero.conflict"),
    "launch.pre_io.action_outcome.domain_error.revision_zero.conflict":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.revision_zero.conflict"),
    "launch.pre_io.request_return.domain_error.revision_zero.unknown":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.revision_zero.unknown"),
    "launch.pre_io.action_outcome.domain_error.revision_zero.unknown":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.revision_zero.unknown"),
    "launch.pre_io.request_return.domain_error.nonintent.transient":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.nonintent.transient"),
    "launch.pre_io.action_outcome.domain_error.nonintent.transient":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.nonintent.transient"),
    "launch.pre_io.request_return.domain_error.nonintent.capacity":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.nonintent.capacity"),
    "launch.pre_io.action_outcome.domain_error.nonintent.capacity":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.nonintent.capacity"),
    "launch.pre_io.request_return.domain_error.nonintent.quota":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.nonintent.quota"),
    "launch.pre_io.action_outcome.domain_error.nonintent.quota":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.nonintent.quota"),
    "launch.pre_io.request_return.domain_error.nonintent.rate_limited":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.nonintent.rate_limited"),
    "launch.pre_io.action_outcome.domain_error.nonintent.rate_limited":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.nonintent.rate_limited"),
    "launch.pre_io.request_return.domain_error.nonintent.invalid_request":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.nonintent.invalid_request"
        ),
    "launch.pre_io.action_outcome.domain_error.nonintent.invalid_request":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.nonintent.invalid_request"
        ),
    "launch.pre_io.request_return.domain_error.nonintent.permission":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.nonintent.permission"),
    "launch.pre_io.action_outcome.domain_error.nonintent.permission":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.nonintent.permission"),
    "launch.pre_io.request_return.domain_error.nonintent.conflict":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.nonintent.conflict"),
    "launch.pre_io.action_outcome.domain_error.nonintent.conflict":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.nonintent.conflict"),
    "launch.pre_io.request_return.domain_error.nonintent.unknown":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.nonintent.unknown"),
    "launch.pre_io.action_outcome.domain_error.nonintent.unknown":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.nonintent.unknown"),
    "launch.pre_io.request_return.domain_error.current_intent.transient":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.current_intent.transient"
        ),
    "launch.pre_io.action_outcome.domain_error.current_intent.transient":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.current_intent.transient"
        ),
    "launch.pre_io.request_return.domain_error.current_intent.capacity":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.current_intent.capacity"
        ),
    "launch.pre_io.action_outcome.domain_error.current_intent.capacity":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.current_intent.capacity"
        ),
    "launch.pre_io.request_return.domain_error.current_intent.quota":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.current_intent.quota"),
    "launch.pre_io.action_outcome.domain_error.current_intent.quota":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.current_intent.quota"),
    "launch.pre_io.request_return.domain_error.current_intent.rate_limited":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.current_intent.rate_limited"
        ),
    "launch.pre_io.action_outcome.domain_error.current_intent.rate_limited":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.current_intent.rate_limited"
        ),
    "launch.pre_io.request_return.domain_error.current_intent.invalid_request":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.current_intent.invalid_request"
        ),
    "launch.pre_io.action_outcome.domain_error.current_intent.invalid_request":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.current_intent.invalid_request"
        ),
    "launch.pre_io.request_return.domain_error.current_intent.permission":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.current_intent.permission"
        ),
    "launch.pre_io.action_outcome.domain_error.current_intent.permission":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.current_intent.permission"
        ),
    "launch.pre_io.request_return.domain_error.current_intent.conflict":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.current_intent.conflict"
        ),
    "launch.pre_io.action_outcome.domain_error.current_intent.conflict":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.current_intent.conflict"
        ),
    "launch.pre_io.request_return.domain_error.current_intent.unknown":
        _CaseProjectorV2(
            "launch.pre_io.request_return.domain_error.current_intent.unknown"),
    "launch.pre_io.action_outcome.domain_error.current_intent.unknown":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.domain_error.current_intent.unknown"),
    "launch.pre_io.action_outcome.fallback.p0.succeeded.missing_handler_return":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.p0.succeeded.missing_handler_return"
        ),
    "launch.pre_io.action_outcome.fallback.p0.succeeded.invalid_handler_return":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.p0.succeeded.invalid_handler_return"
        ),
    "launch.pre_io.action_outcome.fallback.p0.failed.request_failed":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.p0.failed.request_failed"),
    "launch.pre_io.action_outcome.fallback.p0.cancelled.request_cancelled":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.p0.cancelled.request_cancelled"
        ),
    "launch.pre_io.action_outcome.fallback.o.succeeded.missing_handler_return":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.o.succeeded.missing_handler_return"
        ),
    "launch.pre_io.action_outcome.fallback.o.succeeded.invalid_handler_return":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.o.succeeded.invalid_handler_return"
        ),
    "launch.pre_io.action_outcome.fallback.o.failed.request_failed":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.o.failed.request_failed"),
    "launch.pre_io.action_outcome.fallback.o.cancelled.request_cancelled":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.o.cancelled.request_cancelled"
        ),
    "launch.pre_io.action_outcome.fallback.s.succeeded.missing_handler_return":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.s.succeeded.missing_handler_return"
        ),
    "launch.pre_io.action_outcome.fallback.s.succeeded.invalid_handler_return":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.s.succeeded.invalid_handler_return"
        ),
    "launch.pre_io.action_outcome.fallback.s.failed.request_failed":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.s.failed.request_failed"),
    "launch.pre_io.action_outcome.fallback.s.cancelled.request_cancelled":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.s.cancelled.request_cancelled"
        ),
    "launch.pre_io.action_outcome.fallback.x.succeeded.missing_handler_return":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.x.succeeded.missing_handler_return"
        ),
    "launch.pre_io.action_outcome.fallback.x.succeeded.invalid_handler_return":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.x.succeeded.invalid_handler_return"
        ),
    "launch.pre_io.action_outcome.fallback.x.failed.request_failed":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.x.failed.request_failed"),
    "launch.pre_io.action_outcome.fallback.x.cancelled.request_cancelled":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.fallback.x.cancelled.request_cancelled"
        ),
    "launch.pre_io.action_outcome.max_attempt_exhaustion.handler_r":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.max_attempt_exhaustion.handler_r"),
    "launch.pre_io.action_outcome.max_attempt_exhaustion.handler_u":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.max_attempt_exhaustion.handler_u"),
    "launch.pre_io.action_outcome.max_attempt_exhaustion.fallback_p0":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.max_attempt_exhaustion.fallback_p0"),
    "launch.pre_io.action_outcome.max_attempt_exhaustion.fallback_o":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.max_attempt_exhaustion.fallback_o"),
    "down.pre_io.request_return.domain_success":
        _CaseProjectorV2("down.pre_io.request_return.domain_success"),
    "down.pre_io.action_outcome.domain_success":
        _CaseProjectorV2("down.pre_io.action_outcome.domain_success"),
    "down.pre_io.request_return.domain_error.revision_zero.transient":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.revision_zero.transient"),
    "down.pre_io.action_outcome.domain_error.revision_zero.transient":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.revision_zero.transient"),
    "down.pre_io.request_return.domain_error.revision_zero.capacity":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.revision_zero.capacity"),
    "down.pre_io.action_outcome.domain_error.revision_zero.capacity":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.revision_zero.capacity"),
    "down.pre_io.request_return.domain_error.revision_zero.quota":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.revision_zero.quota"),
    "down.pre_io.action_outcome.domain_error.revision_zero.quota":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.revision_zero.quota"),
    "down.pre_io.request_return.domain_error.revision_zero.rate_limited":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.revision_zero.rate_limited"
        ),
    "down.pre_io.action_outcome.domain_error.revision_zero.rate_limited":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.revision_zero.rate_limited"
        ),
    "down.pre_io.request_return.domain_error.revision_zero.invalid_request":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.revision_zero.invalid_request"
        ),
    "down.pre_io.action_outcome.domain_error.revision_zero.invalid_request":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.revision_zero.invalid_request"
        ),
    "down.pre_io.request_return.domain_error.revision_zero.permission":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.revision_zero.permission"),
    "down.pre_io.action_outcome.domain_error.revision_zero.permission":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.revision_zero.permission"),
    "down.pre_io.request_return.domain_error.revision_zero.conflict":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.revision_zero.conflict"),
    "down.pre_io.action_outcome.domain_error.revision_zero.conflict":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.revision_zero.conflict"),
    "down.pre_io.request_return.domain_error.revision_zero.unknown":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.revision_zero.unknown"),
    "down.pre_io.action_outcome.domain_error.revision_zero.unknown":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.revision_zero.unknown"),
    "down.pre_io.request_return.domain_error.nonintent.transient":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.nonintent.transient"),
    "down.pre_io.action_outcome.domain_error.nonintent.transient":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.nonintent.transient"),
    "down.pre_io.request_return.domain_error.nonintent.capacity":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.nonintent.capacity"),
    "down.pre_io.action_outcome.domain_error.nonintent.capacity":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.nonintent.capacity"),
    "down.pre_io.request_return.domain_error.nonintent.quota": _CaseProjectorV2(
        "down.pre_io.request_return.domain_error.nonintent.quota"),
    "down.pre_io.action_outcome.domain_error.nonintent.quota": _CaseProjectorV2(
        "down.pre_io.action_outcome.domain_error.nonintent.quota"),
    "down.pre_io.request_return.domain_error.nonintent.rate_limited":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.nonintent.rate_limited"),
    "down.pre_io.action_outcome.domain_error.nonintent.rate_limited":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.nonintent.rate_limited"),
    "down.pre_io.request_return.domain_error.nonintent.invalid_request":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.nonintent.invalid_request"
        ),
    "down.pre_io.action_outcome.domain_error.nonintent.invalid_request":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.nonintent.invalid_request"
        ),
    "down.pre_io.request_return.domain_error.nonintent.permission":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.nonintent.permission"),
    "down.pre_io.action_outcome.domain_error.nonintent.permission":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.nonintent.permission"),
    "down.pre_io.request_return.domain_error.nonintent.conflict":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.nonintent.conflict"),
    "down.pre_io.action_outcome.domain_error.nonintent.conflict":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.nonintent.conflict"),
    "down.pre_io.request_return.domain_error.nonintent.unknown":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.nonintent.unknown"),
    "down.pre_io.action_outcome.domain_error.nonintent.unknown":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.nonintent.unknown"),
    "down.pre_io.request_return.domain_error.current_intent.transient":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.current_intent.transient"),
    "down.pre_io.action_outcome.domain_error.current_intent.transient":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.current_intent.transient"),
    "down.pre_io.request_return.domain_error.current_intent.capacity":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.current_intent.capacity"),
    "down.pre_io.action_outcome.domain_error.current_intent.capacity":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.current_intent.capacity"),
    "down.pre_io.request_return.domain_error.current_intent.quota":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.current_intent.quota"),
    "down.pre_io.action_outcome.domain_error.current_intent.quota":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.current_intent.quota"),
    "down.pre_io.request_return.domain_error.current_intent.rate_limited":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.current_intent.rate_limited"
        ),
    "down.pre_io.action_outcome.domain_error.current_intent.rate_limited":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.current_intent.rate_limited"
        ),
    "down.pre_io.request_return.domain_error.current_intent.invalid_request":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.current_intent.invalid_request"
        ),
    "down.pre_io.action_outcome.domain_error.current_intent.invalid_request":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.current_intent.invalid_request"
        ),
    "down.pre_io.request_return.domain_error.current_intent.permission":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.current_intent.permission"
        ),
    "down.pre_io.action_outcome.domain_error.current_intent.permission":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.current_intent.permission"
        ),
    "down.pre_io.request_return.domain_error.current_intent.conflict":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.current_intent.conflict"),
    "down.pre_io.action_outcome.domain_error.current_intent.conflict":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.current_intent.conflict"),
    "down.pre_io.request_return.domain_error.current_intent.unknown":
        _CaseProjectorV2(
            "down.pre_io.request_return.domain_error.current_intent.unknown"),
    "down.pre_io.action_outcome.domain_error.current_intent.unknown":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.domain_error.current_intent.unknown"),
    "down.pre_io.action_outcome.fallback.p0.succeeded.missing_handler_return":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.p0.succeeded.missing_handler_return"
        ),
    "down.pre_io.action_outcome.fallback.p0.succeeded.invalid_handler_return":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.p0.succeeded.invalid_handler_return"
        ),
    "down.pre_io.action_outcome.fallback.p0.failed.request_failed":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.p0.failed.request_failed"),
    "down.pre_io.action_outcome.fallback.p0.cancelled.request_cancelled":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.p0.cancelled.request_cancelled"
        ),
    "down.pre_io.action_outcome.fallback.o.succeeded.missing_handler_return":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.o.succeeded.missing_handler_return"
        ),
    "down.pre_io.action_outcome.fallback.o.succeeded.invalid_handler_return":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.o.succeeded.invalid_handler_return"
        ),
    "down.pre_io.action_outcome.fallback.o.failed.request_failed":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.o.failed.request_failed"),
    "down.pre_io.action_outcome.fallback.o.cancelled.request_cancelled":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.o.cancelled.request_cancelled"
        ),
    "down.pre_io.action_outcome.fallback.s.succeeded.missing_handler_return":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.s.succeeded.missing_handler_return"
        ),
    "down.pre_io.action_outcome.fallback.s.succeeded.invalid_handler_return":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.s.succeeded.invalid_handler_return"
        ),
    "down.pre_io.action_outcome.fallback.s.failed.request_failed":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.s.failed.request_failed"),
    "down.pre_io.action_outcome.fallback.s.cancelled.request_cancelled":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.s.cancelled.request_cancelled"
        ),
    "down.pre_io.action_outcome.fallback.x.succeeded.missing_handler_return":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.x.succeeded.missing_handler_return"
        ),
    "down.pre_io.action_outcome.fallback.x.succeeded.invalid_handler_return":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.x.succeeded.invalid_handler_return"
        ),
    "down.pre_io.action_outcome.fallback.x.failed.request_failed":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.x.failed.request_failed"),
    "down.pre_io.action_outcome.fallback.x.cancelled.request_cancelled":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.fallback.x.cancelled.request_cancelled"
        ),
    "down.pre_io.action_outcome.max_attempt_exhaustion.handler_r":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.max_attempt_exhaustion.handler_r"),
    "down.pre_io.action_outcome.max_attempt_exhaustion.handler_u":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.max_attempt_exhaustion.handler_u"),
    "down.pre_io.action_outcome.max_attempt_exhaustion.fallback_p0":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.max_attempt_exhaustion.fallback_p0"),
    "down.pre_io.action_outcome.max_attempt_exhaustion.fallback_o":
        _CaseProjectorV2(
            "down.pre_io.action_outcome.max_attempt_exhaustion.fallback_o"),
    "launch.pre_io.request_return.supersession.e_only.prefix_1":
        _CaseProjectorV2(
            "launch.pre_io.request_return.supersession.e_only.prefix_1"),
    "launch.pre_io.action_outcome.supersession.e_only.prefix_1":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.supersession.e_only.prefix_1"),
    "launch.pre_io.request_return.supersession.e_only.prefix_2":
        _CaseProjectorV2(
            "launch.pre_io.request_return.supersession.e_only.prefix_2"),
    "launch.pre_io.action_outcome.supersession.e_only.prefix_2":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.supersession.e_only.prefix_2"),
    "launch.pre_io.request_return.supersession.e_only.prefix_3":
        _CaseProjectorV2(
            "launch.pre_io.request_return.supersession.e_only.prefix_3"),
    "launch.pre_io.action_outcome.supersession.e_only.prefix_3":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.supersession.e_only.prefix_3"),
    "launch.pre_io.request_return.supersession.e_only.prefix_4":
        _CaseProjectorV2(
            "launch.pre_io.request_return.supersession.e_only.prefix_4"),
    "launch.pre_io.action_outcome.supersession.e_only.prefix_4":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.supersession.e_only.prefix_4"),
    "launch.pre_io.request_return.supersession.e_only.prefix_5":
        _CaseProjectorV2(
            "launch.pre_io.request_return.supersession.e_only.prefix_5"),
    "launch.pre_io.action_outcome.supersession.e_only.prefix_5":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.supersession.e_only.prefix_5"),
    "launch.pre_io.request_return.supersession.e_plus_n.effect_0":
        _CaseProjectorV2(
            "launch.pre_io.request_return.supersession.e_plus_n.effect_0"),
    "launch.pre_io.action_outcome.supersession.e_plus_n.effect_0":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.supersession.e_plus_n.effect_0"),
    "launch.pre_io.request_return.supersession.e_plus_n.effect_1":
        _CaseProjectorV2(
            "launch.pre_io.request_return.supersession.e_plus_n.effect_1"),
    "launch.pre_io.action_outcome.supersession.e_plus_n.effect_1":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.supersession.e_plus_n.effect_1"),
    "launch.pre_io.request_return.supersession.e_plus_n.effect_2":
        _CaseProjectorV2(
            "launch.pre_io.request_return.supersession.e_plus_n.effect_2"),
    "launch.pre_io.action_outcome.supersession.e_plus_n.effect_2":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.supersession.e_plus_n.effect_2"),
    "launch.pre_io.request_return.supersession.e_plus_n.effect_3":
        _CaseProjectorV2(
            "launch.pre_io.request_return.supersession.e_plus_n.effect_3"),
    "launch.pre_io.action_outcome.supersession.e_plus_n.effect_3":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.supersession.e_plus_n.effect_3"),
    "launch.pre_io.request_return.supersession.e_plus_n.effect_4":
        _CaseProjectorV2(
            "launch.pre_io.request_return.supersession.e_plus_n.effect_4"),
    "launch.pre_io.action_outcome.supersession.e_plus_n.effect_4":
        _CaseProjectorV2(
            "launch.pre_io.action_outcome.supersession.e_plus_n.effect_4"),
    "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.unmaterialized":
        _CaseProjectorV2(
            "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.unmaterialized"
        ),
    "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.terminal_request_unsettled.one_link":
        _CaseProjectorV2(
            "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.terminal_request_unsettled.one_link"
        ),
    "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.terminal_request_unsettled.max_count":
        _CaseProjectorV2(
            "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.terminal_request_unsettled.max_count"
        ),
    "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_present.one_link":
        _CaseProjectorV2(
            "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_present.one_link"
        ),
    "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_present.max_count":
        _CaseProjectorV2(
            "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_present.max_count"
        ),
    "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_gc.one_link":
        _CaseProjectorV2(
            "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_gc.one_link"
        ),
    "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_gc.max_count":
        _CaseProjectorV2(
            "launch.owner_fenced_transition.action_outcome.cancelled_no_effect.retained_settled_request_gc.max_count"
        ),
    "launch.settlement.shadow_outcome.primary":
        _CaseProjectorV2("launch.settlement.shadow_outcome.primary"),
    "down.settlement.shadow_outcome.primary":
        _CaseProjectorV2("down.settlement.shadow_outcome.primary"),
}


def _validate_provider_resource_action_representability_dispatch_v2() -> None:
    inventory = PROVIDER_RESOURCE_ACTION_REPRESENTABILITY_CASE_INVENTORY_V2
    expected_ids = tuple(case.case_id for case in inventory.cases)
    if tuple(_CASE_PROJECTORS_V2) != expected_ids:
        raise ProviderResourceActionRepresentabilityError(
            'sealed representability dispatch differs from the case inventory.')
    if tuple(projector.case_id
             for projector in _CASE_PROJECTORS_V2.values()) != expected_ids:
        raise ProviderResourceActionRepresentabilityError(
            'representability projector identities differ from dispatch keys.')
    projected_rows = tuple(
        (case.case_id, case.dispatch_kind.value, case.action_kind.value,
         case.boundary.value, case.payload_kind.value)
        for case in inventory.cases)
    if projected_rows != _CASE_ROW_VALUES:
        raise ProviderResourceActionRepresentabilityError(
            'representability code rows differ from the canonical inventory.')


def enumerate_provider_resource_action_representability_v2(
    representability_input: ProviderResourceActionRepresentabilityInputV2,
) -> ProviderResourceActionRepresentabilityEnumerationV2:
    """Render both finite modes for every case at one exact live boundary.

    This function is pure: it performs no filesystem, database, clock,
    configuration, Kubernetes, provider, or network access.  Its input must
    already be a closed typed root constructed from the owning boundary's
    locked/live values.
    """

    if type(representability_input) not in (
            ProviderResourceActionPreflightRepresentabilityInputV2,
            ProviderResourceActionAdmissionRepresentabilityInputV2,
            ProviderResourceActionPreIoRepresentabilityInputV2):
        raise TypeError('representability enumerator input is invalid.')
    _validate_provider_resource_action_representability_dispatch_v2()
    selected_cases = tuple(
        case for case in
        PROVIDER_RESOURCE_ACTION_REPRESENTABILITY_CASE_INVENTORY_V2.cases
        if case.action_kind is representability_input.action_kind and
        case.boundary is representability_input.boundary)
    if not selected_cases:
        raise ProviderResourceActionRepresentabilityError(
            'representability boundary has no explicit cases.')
    measurements: dict[
        ProviderResourceActionRepresentabilityModeV2,
        tuple[ProviderResourceActionRepresentabilityMeasurementV2, ...],
    ] = {}
    for mode in ProviderResourceActionRepresentabilityModeV2:
        current: list[ProviderResourceActionRepresentabilityMeasurementV2] = []
        for case in selected_cases:
            projector = _CASE_PROJECTORS_V2[case.case_id]
            payload = projector(representability_input, mode)
            current.append(
                _measure_provider_resource_action_representability_payload_v2(
                    case, mode, payload))
        measurements[mode] = tuple(current)
    return ProviderResourceActionRepresentabilityEnumerationV2(
        current=measurements[
            ProviderResourceActionRepresentabilityModeV2.CURRENT],
        candidate_maximal=measurements[
            ProviderResourceActionRepresentabilityModeV2.CANDIDATE_MAXIMAL])


__all__ = [
    'PROVIDER_RESOURCE_ACTION_REPRESENTABILITY_CASES_V2',
    'PROVIDER_RESOURCE_ACTION_REPRESENTABILITY_CASE_INVENTORY_V2',
    'ProviderResourceActionAdmissionRepresentabilityInputV2',
    'ProviderResourceActionDownRepresentabilityConstructionV2',
    'ProviderResourceActionLaunchRepresentabilityConstructionV2',
    'ProviderResourceActionPreIoRepresentabilityInputV2',
    'ProviderResourceActionPreflightRepresentabilityInputV2',
    'ProviderResourceActionReducerAttemptSnapshotV2',
    'ProviderResourceActionReducerHistoryProjectionV2',
    'ProviderResourceActionRepresentabilityCaseInventoryV2',
    'ProviderResourceActionRepresentabilityCaseInventoryIndexV2',
    'ProviderResourceActionRepresentabilityCaseInventoryShardV2',
    'ProviderResourceActionRepresentabilityShardDescriptorV2',
    'ProviderResourceActionRepresentabilityCaseV2',
    'ProviderResourceActionRepresentabilityEnumerationV2',
    'ProviderResourceActionRepresentabilityError',
    'ProviderResourceActionRepresentabilityFixtureInputV2',
    'ProviderResourceActionRepresentabilityMeasurementV2',
    'ProviderResourceActionRepresentabilityUnavailableError',
    'enumerate_provider_resource_action_representability_v2',
    'provider_resource_action_representability_input_from_value_v2',
]
