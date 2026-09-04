"""PostgreSQL state machine for durable ordinary SkyServe launches.

The API request backend owns request serialization, claims, queue delivery,
and retention pins.  This module owns only Serve-side identity, lifecycle
fences, replica association, effect phases, and result projection.  Every
cross-lineage operation accepts an existing SQLAlchemy connection and never
commits it, so the request backend can compose one atomic transaction without
Serve importing request-table definitions.
"""
# pylint: disable=not-callable

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
import contextlib
import contextvars
import dataclasses
import datetime
import enum
import hashlib
import json
import math
import re
from typing import Any, Protocol, TYPE_CHECKING
import uuid

import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.adaptors import common as adaptors_common
from sky.serve import capacity_admission
from sky.serve import capacity_authority
from sky.serve import constants as serve_constants
from sky.serve import kubernetes_identity
from sky.serve import pool_capacity_observation_schema
from sky.serve import route_projection
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import serve_statuses
from sky.utils import common_utils
from sky.utils import locks
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

if TYPE_CHECKING:
    from sky.serve import paid_capacity as paid_capacity_lib

reserved_fill_planner = adaptors_common.LazyImport(
    'sky.serve.reserved_fill_planner')
capacity_policy = adaptors_common.LazyImport('sky.provision.capacity_policy')
paid_capacity = adaptors_common.LazyImport('sky.serve.paid_capacity')
aws_cloud = adaptors_common.LazyImport('sky.clouds.aws')
gcp_cloud = adaptors_common.LazyImport('sky.clouds.gcp')
kueue_lane_lineage = adaptors_common.LazyImport('sky.serve.kueue_lane_lineage')
system_oom_recovery = adaptors_common.LazyImport(
    'sky.serve.system_oom_recovery')
zero_cost_actuation = adaptors_common.LazyImport(
    'sky.serve.zero_cost_actuation')

SUBMISSION_ID_KEY = 'sky_serve_ordinary_launch_submission_id'
ASSOCIATION_ID_KEY = 'sky_serve_ordinary_launch_association_id'
REPLICA_ID_KEY = 'sky_serve_ordinary_launch_replica_id'
REPLICA_RECORD_ID_KEY = 'sky_serve_ordinary_launch_replica_record_id'
LAUNCH_GENERATION_KEY = 'sky_serve_ordinary_launch_generation'
BOUND_REQUEST_ID_KEY = 'sky_serve_ordinary_launch_request_id'
INPUT_DIGEST_KEY = 'sky_serve_ordinary_launch_input_digest'
CONTROLLER_INCARNATION_KEY = (
    serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_INCARNATION_KEY)
CONTROLLER_OWNER_EPOCH_KEY = (
    serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_OWNER_EPOCH_KEY)
OWNER_REVISION_KEY = 'sky_serve_ordinary_launch_owner_revision'
LIFECYCLE_EPOCH_KEY = serve_constants.REPLICA_LAUNCH_FENCE_LIFECYCLE_EPOCH_KEY
BINDING_EPOCH_KEY = 'sky_serve_ordinary_launch_binding_epoch'
BINDING_PROTOCOL_VERSION_KEY = 'sky_serve_non_pool_binding_protocol_version'
PROFILE_KIND_KEY = 'sky_serve_non_pool_profile_kind'
PROFILE_VERSION_KEY = 'sky_serve_non_pool_profile_version'
PROFILE_DIGEST_KEY = 'sky_serve_non_pool_profile_digest'
CAPABILITY_COHORT_EPOCH_KEY = 'sky_serve_non_pool_capability_cohort_epoch'
CAPABILITY_PROFILE_SET_DIGEST_KEY = (
    'sky_serve_non_pool_capability_profile_set_digest')
RECEIPT_PROTOCOL_VERSION_KEY = 'sky_serve_non_pool_receipt_protocol_version'
AUTHORIZATION_KIND_KEY = 'sky_serve_non_pool_authorization_kind'
AUTHORIZATION_REFERENCE_KEY = 'sky_serve_non_pool_authorization_reference'
AUTHORIZATION_GENERATION_KEY = 'sky_serve_non_pool_authorization_generation'
AUTHORIZATION_DIGEST_KEY = 'sky_serve_non_pool_authorization_digest'

# A bound request retains the legacy service/hash/version fields so old
# preconditions recognize that this is controller-originated, while an
# impossible PID/IP makes an old executor fail before provider I/O.
LEGACY_FAIL_CLOSED_CONTROLLER_PID = -1
LEGACY_FAIL_CLOSED_CONTROLLER_IP = 'ordinary-launch-binding.invalid'

DIGEST_VERSION = 'serve-bound-launch.v1'
NON_POOL_BINDING_PROTOCOL_VERSION = 2
NON_POOL_PROFILE_VERSION = 1
NON_POOL_RECEIPT_PROTOCOL_VERSION = 1
NON_POOL_CAPABILITY_COHORT_EPOCH = (
    serve_constants.NON_POOL_CAPABILITY_COHORT_EPOCH)
# Cohort 11 is the first cohort whose ordinary-paid AWS provider effects use
# the immutable association as the EC2 idempotency identity.  This floor must
# remain stable: a pre-floor association may already have called RunInstances
# without a ClientToken and can never acquire provider-absence authority from
# a later tokenized retry.
ORDINARY_PAID_AWS_CLIENT_TOKEN_COHORT_FLOOR = 11
ORDINARY_PAID_AWS_REPLACEMENT_CREATE_COHORT_FLOOR = 13
# Cohort 16 is the first cohort whose fresh ordinary-paid admission commits a
# resource-action identity with the executable request and whose executor uses
# that identity for the global cluster record.  Keep this floor stable: older
# rows may be retained for cleanup but cannot safely acquire a UUID after the
# provider effect has started.
ORDINARY_PAID_RESOURCE_ACTION_IDENTITY_COHORT_FLOOR = 16
ORDINARY_PAID_AWS_ABSENCE_SETTLE_SECONDS = 60
# Cohort 12 is the first cohort whose GCP create timeout preserves the zone
# operation record and whose provider reconciliation reads VM, disk, and
# in-flight create-operation state.  A cohort-12 binary may conservatively
# recover retained cohort-11 rows after the legacy settling horizon, but a
# cohort-11 binary must never advertise this provider-evidence contract.
ORDINARY_PAID_GCP_OPERATION_EVIDENCE_COHORT_FLOOR = 12
ORDINARY_PAID_GCP_ABSENCE_SETTLE_SECONDS = 300
_GCP_PROJECT_ID_RE = re.compile(r'[a-z][a-z0-9-]{4,28}[a-z0-9]')
TOMBSTONE_RETENTION_DAYS = 60
MAX_GC_BATCH_SIZE = 500
_SHA256_RE = re.compile(r'[0-9a-f]{64}')
_ASSOCIATION_NAMESPACE = uuid.UUID('5ab85493-af88-4e82-bdda-8cbe1a8b15ea')
_REQUEST_NAMESPACE = uuid.UUID('f77cfdf5-95c4-4882-a768-30496fd23c97')
_ORDINARY_LAUNCH_SUBMISSION_NAMESPACE = uuid.UUID(
    '58a82cb0-534c-5a5d-bb5d-681759e60469')
_ORDINARY_PAID_REPLICA_INCARNATION_NAMESPACE = uuid.UUID(
    '1fa8f58e-acde-49fb-b545-cd53c6ce5cab')
_ORDINARY_PAID_CLUSTER_RECORD_NAMESPACE = uuid.UUID(
    'e1cffd21-1c9d-4545-962c-95693f0a80b3')
_LEGACY_SCOPE_NAMESPACE = uuid.UUID('85efcb78-8e08-4d18-bc25-c9de88377399')
_LEGACY_EVENT_NAMESPACE = uuid.UUID('1daed865-c0b3-40e0-bd33-e65f752df996')
_FRESH_PAID_RESOURCE_ACTION_COLUMNS = (
    'replica_incarnation',
    'desired_generation',
    'sky_cluster_record_uuid',
)


class EffectPhase(str, enum.Enum):
    """Monotonic external-effect boundary."""

    NOT_STARTED = 'NOT_STARTED'
    PROVIDER_IO = 'PROVIDER_IO'
    SERVICE_JOB_IO = 'SERVICE_JOB_IO'
    SERVICE_JOB_RECORDED = 'SERVICE_JOB_RECORDED'


_PAID_PROVIDER_RECONCILIATION_PHASES = frozenset(
    (EffectPhase.PROVIDER_IO, EffectPhase.SERVICE_JOB_IO))


def is_paid_provider_reconciliation_phase(
        effect_phase: EffectPhase | str) -> bool:
    """Whether paid provider allocation may exist without a job receipt."""
    try:
        phase = EffectPhase(effect_phase)
    except (TypeError, ValueError):
        return False
    return phase in _PAID_PROVIDER_RECONCILIATION_PHASES


class Resolution(str, enum.Enum):
    """Closed association resolution state."""

    BOUND = 'BOUND'
    CANCEL_REQUESTED = 'CANCEL_REQUESTED'
    RESULT_RECORDED = 'RESULT_RECORDED'
    PROJECTED = 'PROJECTED'
    PRE_EFFECT_TERMINAL = 'PRE_EFFECT_TERMINAL'
    AMBIGUOUS = 'AMBIGUOUS'


class BindingMode(str, enum.Enum):
    LEGACY = 'legacy'
    BOUND = 'bound'


class NonPoolLaunchProfileKind(str, enum.Enum):
    """Closed launch-reason profile for the shared non-pool binding."""

    ORDINARY_PAID = 'ORDINARY_PAID'
    ORDINARY_ZERO_COST = 'ORDINARY_ZERO_COST'
    RESERVED_FILL = 'RESERVED_FILL'
    UNKNOWN_CAPACITY_REPLACEMENT = 'UNKNOWN_CAPACITY_REPLACEMENT'
    COST_REBALANCE = 'COST_REBALANCE'
    SYSTEM_OOM_RECOVERY = 'SYSTEM_OOM_RECOVERY'


def is_paid_provider_reconciliation_profile(
        kind: NonPoolLaunchProfileKind) -> bool:
    """Whether a profile may use exact paid-provider reconciliation.

    This is a capability classification, not authority by itself.  Every
    caller must still prove the exact paid claim, request identity, terminal
    executor quiescence, and provider-specific identity.  Both AWS and GCP
    observers support a paid replacement only when its provider-specific
    immutable identity is complete.
    """
    return kind in (NonPoolLaunchProfileKind.ORDINARY_PAID,
                    NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT)


class NonPoolLaunchAuthorizationKind(str, enum.Enum):
    """Planner-owned authority referenced by a non-pool launch profile."""

    PAID_CAPACITY_CLAIM = 'PAID_CAPACITY_CLAIM'
    ZERO_COST_ADMISSION = 'ZERO_COST_ADMISSION'
    RESERVED_FILL_ALLOCATION = 'RESERVED_FILL_ALLOCATION'
    UNKNOWN_CAPACITY_REPLACEMENT = 'UNKNOWN_CAPACITY_REPLACEMENT'
    COST_REBALANCE_DECISION = 'COST_REBALANCE_DECISION'
    SYSTEM_OOM_RECOVERY = 'SYSTEM_OOM_RECOVERY'


class ReconciliationOutcome(str, enum.Enum):
    """Typed disposition of one generalized launch association."""

    ACTIVE_ADOPT = 'ACTIVE_ADOPT'
    RESULT_RECORDED = 'RESULT_RECORDED'
    PROJECTED = 'PROJECTED'
    PRE_EFFECT_TERMINAL = 'PRE_EFFECT_TERMINAL'
    POST_EFFECT_AMBIGUOUS = 'POST_EFFECT_AMBIGUOUS'
    LEGACY_EFFECT_AMBIGUOUS = 'LEGACY_EFFECT_AMBIGUOUS'


class ProviderEvidence(str, enum.Enum):
    """Closed provider readback classification."""

    NOT_QUERIED = 'NOT_QUERIED'
    PRESENT = 'PRESENT'
    ABSENT = 'ABSENT'
    UNKNOWN = 'UNKNOWN'
    REPLACED = 'REPLACED'


class ProviderPresentTeardownPhase(str, enum.Enum):
    """Typed view of the persisted immediate provider-cleanup phase.

    ``ProcessStatus.FAILED`` is deliberately reused for observation pending:
    the prior writer already recognizes it as exact provider-present cleanup
    authority, so rollback remains safe and merely uses its slower combined
    submit/poll path.  Callers reason about the protocol through this adapter
    instead of depending on that compatibility encoding.
    """

    SUBMISSION_SCHEDULED = 'SUBMISSION_SCHEDULED'
    SUBMISSION_RUNNING = 'SUBMISSION_RUNNING'
    ABSENCE_OBSERVATION_PENDING = 'ABSENCE_OBSERVATION_PENDING'
    CLEANUP_SUCCEEDED = 'CLEANUP_SUCCEEDED'


_PROVIDER_PRESENT_TEARDOWN_STATUS_BY_PHASE = {
    ProviderPresentTeardownPhase.SUBMISSION_SCHEDULED:
        common_utils.ProcessStatus.SCHEDULED,
    ProviderPresentTeardownPhase.SUBMISSION_RUNNING:
        common_utils.ProcessStatus.RUNNING,
    ProviderPresentTeardownPhase.ABSENCE_OBSERVATION_PENDING:
        common_utils.ProcessStatus.FAILED,
    ProviderPresentTeardownPhase.CLEANUP_SUCCEEDED:
        common_utils.ProcessStatus.SUCCEEDED,
}
_PROVIDER_PRESENT_TEARDOWN_PHASE_BY_STATUS = {
    status: phase
    for phase, status in _PROVIDER_PRESENT_TEARDOWN_STATUS_BY_PHASE.items()
}


def provider_present_teardown_phase(
        replica_info: Any) -> ProviderPresentTeardownPhase:
    """Return the typed teardown phase for one immediate-cleanup row."""
    status_property = getattr(replica_info, 'status_property', None)
    down_status = getattr(status_property, 'sky_down_status', None)
    if not isinstance(down_status, common_utils.ProcessStatus):
        raise OrdinaryLaunchBindingConflict(
            'Provider-present cleanup has no recognized teardown phase.')
    try:
        return _PROVIDER_PRESENT_TEARDOWN_PHASE_BY_STATUS[down_status]
    except KeyError as error:
        raise OrdinaryLaunchBindingConflict(
            'Provider-present cleanup has no recognized teardown phase.'
        ) from error


def transition_provider_present_teardown_phase(
    replica_info: Any,
    *,
    expected: ProviderPresentTeardownPhase,
    target: ProviderPresentTeardownPhase,
) -> None:
    """Apply one exact in-memory teardown phase transition."""
    if not isinstance(expected, ProviderPresentTeardownPhase):
        raise TypeError('Expected teardown phase has an invalid type.')
    if not isinstance(target, ProviderPresentTeardownPhase):
        raise TypeError('Target teardown phase has an invalid type.')
    actual = provider_present_teardown_phase(replica_info)
    if actual is not expected:
        raise OrdinaryLaunchBindingConflict(
            'Provider-present cleanup teardown phase changed: expected '
            f'{expected.value}, found {actual.value}.')
    replica_info.status_property.sky_down_status = (
        _PROVIDER_PRESENT_TEARDOWN_STATUS_BY_PHASE[target])


class ProviderAllocationDisposition(str, enum.Enum):
    """Result of one exact paid provider-allocation checkpoint."""

    RECORDED = 'RECORDED'
    EXACT_REPLAY = 'EXACT_REPLAY'


@dataclasses.dataclass(frozen=True)
class RetainedAuthorityCensus:
    """Locked provider-safe association history for one service name."""

    association_rows: tuple[Mapping[str, Any], ...]


class _TerminalCensusPolicy(enum.Enum):
    """Supported consumers of the retained terminal graph census."""

    N2_TRANSFER = enum.auto()
    FINAL_DELETION = enum.auto()


class LegacyReconciliationResolution(str, enum.Enum):
    """Monotonic disposition for a scoped historical unbound launch."""

    EFFECT_AMBIGUOUS = 'LEGACY_EFFECT_AMBIGUOUS'
    CLEANUP_AUTHORIZED = 'CLEANUP_AUTHORIZED'
    PROJECTED = 'PROJECTED'


_PROFILE_AUTHORIZATION_KIND = {
    NonPoolLaunchProfileKind.ORDINARY_PAID:
        NonPoolLaunchAuthorizationKind.PAID_CAPACITY_CLAIM,
    NonPoolLaunchProfileKind.ORDINARY_ZERO_COST:
        NonPoolLaunchAuthorizationKind.ZERO_COST_ADMISSION,
    NonPoolLaunchProfileKind.RESERVED_FILL:
        NonPoolLaunchAuthorizationKind.RESERVED_FILL_ALLOCATION,
    NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT:
        NonPoolLaunchAuthorizationKind.UNKNOWN_CAPACITY_REPLACEMENT,
    NonPoolLaunchProfileKind.COST_REBALANCE:
        NonPoolLaunchAuthorizationKind.COST_REBALANCE_DECISION,
    NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY:
        NonPoolLaunchAuthorizationKind.SYSTEM_OOM_RECOVERY,
}


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload,
                           sort_keys=True,
                           separators=(',', ':'),
                           ensure_ascii=False,
                           allow_nan=False).encode('utf-8')
    return hashlib.sha256(canonical).hexdigest()


@dataclasses.dataclass(frozen=True)
class NonPoolLaunchProfile:
    """Immutable profile and planner-authorization reference.

    The profile does not grant launch authority by itself. Admission and the
    pre-I/O fence resolve the reference against the profile planner's locked
    durable state and recompute both digests.
    """

    kind: NonPoolLaunchProfileKind
    version: int
    authorization_kind: NonPoolLaunchAuthorizationKind
    authorization_reference: str
    authorization_generation: int
    authorization_digest: str
    digest: str

    @classmethod
    def create(
        cls,
        kind: NonPoolLaunchProfileKind,
        *,
        authorization_reference: str,
        authorization_generation: int,
        authorization_payload: Mapping[str, Any],
    ) -> NonPoolLaunchProfile:
        """Construct a canonical v1 profile from planner-owned evidence."""
        if not isinstance(kind, NonPoolLaunchProfileKind):
            raise ValueError('kind must be a closed non-pool launch profile.')
        authorization_kind = _PROFILE_AUTHORIZATION_KIND[kind]
        authorization_reference = _nonempty(authorization_reference,
                                            'authorization_reference')
        authorization_generation = _nonnegative_int(authorization_generation,
                                                    'authorization_generation')
        if not isinstance(authorization_payload, Mapping):
            raise ValueError('authorization_payload must be a mapping.')
        authorization_digest = _canonical_sha256({
            'authorization_generation': authorization_generation,
            'authorization_kind': authorization_kind.value,
            'authorization_payload': dict(authorization_payload),
            'authorization_reference': authorization_reference,
        })
        digest = canonical_non_pool_profile_digest(
            kind,
            profile_version=NON_POOL_PROFILE_VERSION,
            authorization_kind=authorization_kind,
            authorization_reference=authorization_reference,
            authorization_generation=authorization_generation,
            authorization_digest=authorization_digest)
        return cls(kind=kind,
                   version=NON_POOL_PROFILE_VERSION,
                   authorization_kind=authorization_kind,
                   authorization_reference=authorization_reference,
                   authorization_generation=authorization_generation,
                   authorization_digest=authorization_digest,
                   digest=digest)

    def validate(self) -> None:
        """Reject a partial, noncanonical, or cross-profile envelope."""
        expected = canonical_non_pool_profile_digest(
            self.kind,
            profile_version=self.version,
            authorization_kind=self.authorization_kind,
            authorization_reference=self.authorization_reference,
            authorization_generation=self.authorization_generation,
            authorization_digest=self.authorization_digest)
        if self.digest != expected:
            raise ValueError('Non-pool profile digest is not canonical.')

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> NonPoolLaunchProfile:
        """Parse the exact profile tuple installed in a bound request."""
        try:
            profile = cls(
                kind=NonPoolLaunchProfileKind(values[PROFILE_KIND_KEY]),
                version=_positive_int(values[PROFILE_VERSION_KEY],
                                      'profile_version'),
                authorization_kind=NonPoolLaunchAuthorizationKind(
                    values[AUTHORIZATION_KIND_KEY]),
                authorization_reference=_nonempty(
                    values[AUTHORIZATION_REFERENCE_KEY],
                    'authorization_reference'),
                authorization_generation=_nonnegative_int(
                    values[AUTHORIZATION_GENERATION_KEY],
                    'authorization_generation'),
                authorization_digest=_nonempty(values[AUTHORIZATION_DIGEST_KEY],
                                               'authorization_digest'),
                digest=_nonempty(values[PROFILE_DIGEST_KEY], 'profile_digest'))
        except (KeyError, ValueError) as error:
            raise ValueError(
                'Non-pool profile envelope is incomplete.') from error
        profile.validate()
        return profile


def canonical_non_pool_profile_digest(
    profile_kind: NonPoolLaunchProfileKind,
    *,
    profile_version: int,
    authorization_kind: NonPoolLaunchAuthorizationKind,
    authorization_reference: str,
    authorization_generation: int,
    authorization_digest: str,
) -> str:
    """Digest the immutable profile and planner-authorization envelope."""
    if not isinstance(profile_kind, NonPoolLaunchProfileKind):
        raise ValueError('profile_kind must be a closed non-pool profile.')
    if profile_version != NON_POOL_PROFILE_VERSION:
        raise ValueError(f'profile_version must be {NON_POOL_PROFILE_VERSION}.')
    if not isinstance(authorization_kind, NonPoolLaunchAuthorizationKind):
        raise ValueError(
            'authorization_kind must be a closed authorization kind.')
    if _PROFILE_AUTHORIZATION_KIND[profile_kind] != authorization_kind:
        raise ValueError('authorization_kind does not match profile_kind.')
    authorization_reference = _nonempty(authorization_reference,
                                        'authorization_reference')
    authorization_generation = _nonnegative_int(authorization_generation,
                                                'authorization_generation')
    if (not isinstance(authorization_digest, str) or
            not _SHA256_RE.fullmatch(authorization_digest)):
        raise ValueError('authorization_digest must be lowercase SHA-256.')
    return _canonical_sha256({
        'authorization': {
            'digest': authorization_digest,
            'generation': authorization_generation,
            'kind': authorization_kind.value,
            'reference': authorization_reference,
        },
        'profile_kind': profile_kind.value,
        'profile_version': profile_version,
    })


def supported_non_pool_profile_set_digest() -> str:
    """Return the exact closed profile set advertised by capable processes."""
    return _canonical_sha256({
        'binding_protocol_version': NON_POOL_BINDING_PROTOCOL_VERSION,
        'profiles': [{
            'authorization_kind': _PROFILE_AUTHORIZATION_KIND[kind].value,
            'kind': kind.value,
            'version': NON_POOL_PROFILE_VERSION,
        } for kind in sorted(NonPoolLaunchProfileKind,
                             key=lambda item: item.value)],
        'receipt_protocol_version': NON_POOL_RECEIPT_PROTOCOL_VERSION,
    })


class AdmissionDisposition(str, enum.Enum):
    """Result of the Serve half of atomic admission."""

    CREATE = 'CREATE'
    EXISTING_EXACT = 'EXISTING_EXACT'


class StartupClassification(str, enum.Enum):
    """Conservative startup disposition for request-layer evidence."""

    ADOPT_ACTIVE = 'ADOPT_ACTIVE'
    WAIT_QUIESCENCE = 'WAIT_QUIESCENCE'
    REDUCE_TERMINAL = 'REDUCE_TERMINAL'
    PRE_EFFECT_TERMINALIZE = 'PRE_EFFECT_TERMINALIZE'
    SETTLED = 'SETTLED'
    AMBIGUOUS = 'AMBIGUOUS'


class PreAdmissionRetirementDisposition(str, enum.Enum):
    """Outcome of retiring planner intent that never became an action."""

    RETIRED = 'RETIRED'
    ABSENT = 'ABSENT'
    ASSOCIATED = 'ASSOCIATED'


@dataclasses.dataclass(frozen=True)
class PreAdmissionRetirement:
    """Exact result of one pointerless generic-intent retirement."""

    disposition: PreAdmissionRetirementDisposition
    profile_kind: NonPoolLaunchProfileKind | None = None


class TerminalStatus(str, enum.Enum):
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


_TERMINAL_STATUS_VALUES = frozenset(value.value for value in TerminalStatus)


def ordinary_paid_provider_terminal_shape_matches(
    status: TerminalStatus | str | Any,
    cause: str | Any,
    paid_capacity_pool_key: str | None,
) -> bool:
    """Whether terminal evidence can safely enter paid-provider cleanup.

    A terminal cause is diagnostic, not cleanup authority.  Any terminal
    request may use an exact v2 GCP or AWS census; callers remain responsible
    for proving the immutable request/binding graph, paid claim, retention pin,
    effect phase, and execution quiescence before provider access.  The two
    narrow legacy arms preserve pre-v2 behavior.
    """
    status_value = getattr(status, 'value', status)
    cause_value = getattr(cause, 'value', cause)
    if (status_value == TerminalStatus.FAILED.value and
            cause_value == 'handler_failed'):
        return True
    if (not isinstance(paid_capacity_pool_key, str) or
            not paid_capacity_pool_key):
        return False
    identity = paid_capacity.pool_key_payload(paid_capacity_pool_key)
    if not isinstance(identity,
                      Mapping) or identity.get('use_spot') is not True:
        return False
    provider_identity = identity.get('provider_identity')
    exact_aws_pool = bool(
        identity.get('cloud') == 'aws' and identity.get('version') == 2 and
        isinstance(provider_identity, Mapping) and
        re.fullmatch(r'[0-9]{12}', str(
            provider_identity.get('aws_account_id'))) is not None)
    exact_gcp_pool = bool(
        identity.get('cloud') == 'gcp' and identity.get('version') == 2 and
        isinstance(provider_identity, Mapping) and _GCP_PROJECT_ID_RE.fullmatch(
            str(provider_identity.get('gcp_project_id'))) is not None)
    if (exact_aws_pool or exact_gcp_pool):
        return bool(status_value in _TERMINAL_STATUS_VALUES and
                    isinstance(cause_value, str) and cause_value)
    if (status_value != TerminalStatus.CANCELLED.value or
            cause_value != 'explicit_cancel'):
        return False
    return bool(identity.get('cloud') == 'gcp' and identity.get('version') == 1)


def paid_provider_reconciliation_pool_shape_matches(
    profile_kind: NonPoolLaunchProfileKind,
    paid_capacity_pool_key: str | None,
) -> bool:
    """Whether one profile/pool pair has an exact provider-census contract."""
    if profile_kind is NonPoolLaunchProfileKind.ORDINARY_PAID:
        return True
    if (profile_kind
            is not NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT or
            not isinstance(paid_capacity_pool_key, str) or
            not paid_capacity_pool_key):
        return False
    identity = paid_capacity.pool_key_payload(paid_capacity_pool_key)
    if not isinstance(identity,
                      Mapping) or identity.get('use_spot') is not True:
        return False
    return bool(
        (identity.get('cloud') == 'gcp' and
         (identity.get('version') == 1 or
          (identity.get('version') == 2 and
           isinstance(identity.get('provider_identity'), Mapping) and
           _GCP_PROJECT_ID_RE.fullmatch(
               str(identity['provider_identity'].get('gcp_project_id')))
           is not None))) or
        (identity.get('cloud') == 'aws' and identity.get('version') == 2 and
         isinstance(identity.get('provider_identity'), Mapping) and
         re.fullmatch(r'[0-9]{12}',
                      str(identity['provider_identity'].get('aws_account_id')))
         is not None))


UNSETTLED_RESOLUTIONS = frozenset({
    Resolution.BOUND,
    Resolution.CANCEL_REQUESTED,
    Resolution.RESULT_RECORDED,
    Resolution.AMBIGUOUS,
})
SETTLED_RESOLUTIONS = frozenset({
    Resolution.PROJECTED,
    Resolution.PRE_EFFECT_TERMINAL,
})

# Transitional aliases retained for callers while the stack is assembled.
STATE_BOUND = Resolution.BOUND.value
STATE_AMBIGUOUS = Resolution.AMBIGUOUS.value
STATE_PROJECTED = Resolution.PROJECTED.value
STATE_TERMINAL_UNPROJECTED = Resolution.RESULT_RECORDED.value
STATE_VALUES = tuple(state.value for state in Resolution)

_EFFECT_PHASE_SQL = ', '.join(f"'{value.value}'" for value in EffectPhase)
_RESOLUTION_SQL = ', '.join(f"'{value.value}'" for value in Resolution)
_TERMINAL_STATUS_SQL = ', '.join(f"'{value.value}'" for value in TerminalStatus)
_UNSETTLED_SQL = ', '.join(
    f"'{value.value}'" for value in UNSETTLED_RESOLUTIONS)
_PROFILE_KIND_SQL = ', '.join(
    f"'{value.value}'" for value in NonPoolLaunchProfileKind)
_AUTHORIZATION_KIND_SQL = ', '.join(
    f"'{value.value}'" for value in NonPoolLaunchAuthorizationKind)
_RECONCILIATION_OUTCOME_SQL = ', '.join(
    f"'{value.value}'" for value in ReconciliationOutcome
    if value != ReconciliationOutcome.LEGACY_EFFECT_AMBIGUOUS)
_PROVIDER_EVIDENCE_SQL = ', '.join(
    f"'{value.value}'" for value in ProviderEvidence)
_LEGACY_RESOLUTION_SQL = ', '.join(
    f"'{value.value}'" for value in LegacyReconciliationResolution)
_ORDINARY_PAID_PROVIDER_TERMINAL_SQL = (
    "(((terminal_status IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND "
    "terminal_cause IS NOT NULL AND terminal_cause <> '') AND "
    "((paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
    "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
    "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
    "'gcp_project_id' ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$') OR "
    "(paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
    "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
    "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
    "'aws_account_id' ~ '^[0-9]{12}$'))) OR "
    "(terminal_status = 'FAILED' AND terminal_cause = 'handler_failed') OR "
    "(terminal_status = 'CANCELLED' AND terminal_cause = 'explicit_cancel' "
    "AND paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
    "paid_capacity_pool_key::jsonb ->> 'version' = '1' AND "
    "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true'))")

metadata = sqlalchemy.MetaData()
ordinary_launch_associations_table = sqlalchemy.Table(
    'serve_ordinary_launch_associations',
    metadata,
    sqlalchemy.Column('association_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('submission_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('tenant_scope', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_lifecycle_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('service_binding_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('service_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('replica_record_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    # Immutable snapshot of the exact paid-capacity claim admitted for this
    # generation.  It lets every effect/reducer transaction lock the global
    # provider pool before the replica without a mutable pre-lock lookup.
    sqlalchemy.Column('paid_capacity_pool_key', sqlalchemy.Text),
    sqlalchemy.Column('launch_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('cluster_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('input_digest', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('digest_version',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default=DIGEST_VERSION),
    sqlalchemy.Column('owner_controller_incarnation',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('owner_controller_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('owner_revision',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='1'),
    sqlalchemy.Column('owner_transferred_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('effect_phase',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default=EffectPhase.NOT_STARTED.value),
    sqlalchemy.Column('effect_phase_changed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('resolution',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default=Resolution.BOUND.value),
    sqlalchemy.Column('cancel_reason', sqlalchemy.Text),
    sqlalchemy.Column('cancel_requested_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('terminal_status', sqlalchemy.Text),
    sqlalchemy.Column('terminal_cause', sqlalchemy.Text),
    sqlalchemy.Column('terminal_execution_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('execution_quiescence_required', sqlalchemy.Boolean),
    sqlalchemy.Column('execution_quiesced_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('execution_quiesced_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('service_job_id', sqlalchemy.BigInteger),
    sqlalchemy.Column('result_recorded_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('ambiguity_code', sqlalchemy.Text),
    sqlalchemy.Column('projected_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('pin_released_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('tombstone_not_before',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    # Serve047 generic envelope. NULL is the inert historical protocol-v1
    # shape; only a complete v2 tuple can authorize the generic handler.
    sqlalchemy.Column('binding_protocol_version', sqlalchemy.Integer),
    sqlalchemy.Column('profile_kind', sqlalchemy.Text),
    sqlalchemy.Column('profile_version', sqlalchemy.Integer),
    sqlalchemy.Column('profile_digest', sqlalchemy.Text),
    sqlalchemy.Column('capability_cohort_epoch', sqlalchemy.BigInteger),
    sqlalchemy.Column('capability_profile_set_digest', sqlalchemy.Text),
    sqlalchemy.Column('receipt_protocol_version', sqlalchemy.Integer),
    sqlalchemy.Column('authorization_kind', sqlalchemy.Text),
    sqlalchemy.Column('authorization_reference', sqlalchemy.Text),
    sqlalchemy.Column('authorization_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('authorization_digest', sqlalchemy.Text),
    sqlalchemy.Column('reconciliation_outcome', sqlalchemy.Text),
    sqlalchemy.Column('provider_evidence', sqlalchemy.Text),
    sqlalchemy.Column('provider_evidence_observed_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('provider_evidence_payload',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('provider_evidence_digest', sqlalchemy.Text),
    sqlalchemy.CheckConstraint('length(tenant_scope) > 0',
                               name='serve_ordinary_binding_tenant_scope'),
    sqlalchemy.CheckConstraint('length(service_name) > 0',
                               name='serve_ordinary_binding_service_name'),
    sqlalchemy.CheckConstraint('length(service_hash) > 0',
                               name='serve_ordinary_binding_service_hash'),
    sqlalchemy.CheckConstraint('length(service_workspace) > 0',
                               name='serve_ordinary_binding_workspace'),
    sqlalchemy.CheckConstraint('service_lifecycle_epoch > 0',
                               name='serve_ordinary_binding_lifecycle_epoch'),
    sqlalchemy.CheckConstraint('service_binding_epoch > 0',
                               name='serve_ordinary_binding_binding_epoch'),
    sqlalchemy.CheckConstraint('service_version > 0',
                               name='serve_ordinary_binding_service_version'),
    sqlalchemy.CheckConstraint('replica_id > 0',
                               name='serve_ordinary_binding_replica_id'),
    sqlalchemy.CheckConstraint(
        'paid_capacity_pool_key IS NULL OR '
        'length(paid_capacity_pool_key) > 0',
        name='serve_ordinary_binding_paid_pool'),
    sqlalchemy.CheckConstraint(
        "CASE WHEN profile_kind IS DISTINCT FROM 'ORDINARY_PAID' AND "
        "profile_kind IS DISTINCT FROM 'UNKNOWN_CAPACITY_REPLACEMENT' "
        "THEN TRUE WHEN capability_cohort_epoch < 11 THEN TRUE "
        "WHEN profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' THEN "
        "COALESCE((paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
        "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
        "((paid_capacity_pool_key::jsonb ->> 'version' = '1') OR "
        "(paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
        "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
        "'gcp_project_id' ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$'))) OR "
        "(paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
        "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
        "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
        "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
        "'aws_account_id' ~ '^[0-9]{12}$'), FALSE) ELSE "
        "COALESCE((paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
        "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
        "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
        "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
        "'aws_account_id' ~ '^[0-9]{12}$') OR "
        "(paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
        "((capability_cohort_epoch < 15 AND "
        "paid_capacity_pool_key::jsonb ->> 'version' = '1' AND NOT "
        "(paid_capacity_pool_key::jsonb ? 'provider_identity')) OR "
        "(paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
        "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
        "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
        "'gcp_project_id' ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$'))), FALSE) "
        "END",
        name='serve059_paid_pool_scope_ck'),
    sqlalchemy.CheckConstraint(
        "CASE WHEN profile_kind IS DISTINCT FROM 'ORDINARY_PAID' AND NOT "
        "(profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' AND "
        "(paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
        "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
        "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
        "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
        "'aws_account_id' ~ '^[0-9]{12}$')) THEN TRUE "
        "WHEN provider_evidence IS DISTINCT FROM 'ABSENT' THEN TRUE ELSE "
        "CASE WHEN capability_cohort_epoch < 11 THEN TRUE "
        "WHEN COALESCE(paid_capacity_pool_key::jsonb ->> 'cloud' <> 'aws', "
        "FALSE) THEN TRUE ELSE "
        "COALESCE((profile_kind = 'ORDINARY_PAID' AND "
        "(provider_evidence_payload #>> "
        "'{receipt,aws_account_id}') = (paid_capacity_pool_key::jsonb #>> "
        "'{provider_identity,aws_account_id}') AND "
        "provider_evidence_payload #>> '{receipt,client_token}' ~ "
        "'^[0-9a-f]{64}$') OR "
        "(provider_evidence_payload ->> 'probe_contract' = "
        "'aws-client-token-instance-presence-v1' AND "
        "provider_evidence_payload ->> 'result' = 'ABSENT' AND "
        "jsonb_typeof(provider_evidence_payload -> 'instances') = 'array' "
        "AND (provider_evidence_payload #>> "
        "'{provider_identity,aws_account_id}') = "
        "(paid_capacity_pool_key::jsonb #>> "
        "'{provider_identity,aws_account_id}') AND "
        "(provider_evidence_payload #>> '{provider_identity,workspace}') = "
        "(paid_capacity_pool_key::jsonb ->> 'workspace') AND "
        "(provider_evidence_payload #>> '{provider_identity,region}') = "
        "(paid_capacity_pool_key::jsonb ->> 'region') AND "
        "(provider_evidence_payload #>> '{provider_identity,zone}') = "
        "(paid_capacity_pool_key::jsonb ->> 'zone') AND "
        "(provider_evidence_payload #>> "
        "'{provider_identity,instance_type}') = "
        "(paid_capacity_pool_key::jsonb ->> 'instance_type') AND "
        "(provider_evidence_payload #> '{provider_identity,num_nodes}') = "
        "(paid_capacity_pool_key::jsonb -> 'num_nodes') AND "
        "(provider_evidence_payload #> '{provider_identity,use_spot}') = "
        "(paid_capacity_pool_key::jsonb -> 'use_spot') AND "
        "provider_evidence_payload #>> '{provider_identity,client_token}' ~ "
        "'^[0-9a-f]{64}$' AND length(provider_evidence_payload #>> "
        "'{provider_identity,cluster_name_on_cloud}') > 0), FALSE) END END",
        name='serve059_paid_receipt_scope_ck'),
    sqlalchemy.CheckConstraint('launch_generation > 0',
                               name='serve_ordinary_binding_generation'),
    sqlalchemy.CheckConstraint('length(cluster_name) > 0',
                               name='serve_ordinary_binding_cluster_name'),
    sqlalchemy.CheckConstraint('length(request_id) > 0',
                               name='serve_ordinary_binding_request_id'),
    sqlalchemy.CheckConstraint("input_digest ~ '^[0-9a-f]{64}$'",
                               name='serve_ordinary_binding_input_digest'),
    sqlalchemy.CheckConstraint(f"digest_version = '{DIGEST_VERSION}'",
                               name='serve_ordinary_binding_digest_version'),
    sqlalchemy.CheckConstraint(
        'num_nonnulls(binding_protocol_version, profile_kind, '
        'profile_version, profile_digest, capability_cohort_epoch, '
        'capability_profile_set_digest, receipt_protocol_version, '
        'authorization_kind, authorization_reference, '
        'authorization_generation, authorization_digest) IN (0, 11)',
        name='serve047_profile_complete_ck'),
    sqlalchemy.CheckConstraint(
        'binding_protocol_version IS NULL OR '
        f'(binding_protocol_version = {NON_POOL_BINDING_PROTOCOL_VERSION} '
        f'AND profile_version = {NON_POOL_PROFILE_VERSION} '
        'AND capability_cohort_epoch > 0 '
        'AND authorization_generation >= 0 '
        'AND length(authorization_reference) > 0 '
        f'AND receipt_protocol_version = {NON_POOL_RECEIPT_PROTOCOL_VERSION} '
        f'AND profile_kind IN ({_PROFILE_KIND_SQL}) '
        f'AND authorization_kind IN ({_AUTHORIZATION_KIND_SQL}))',
        name='serve047_profile_values_ck'),
    sqlalchemy.CheckConstraint(
        'binding_protocol_version IS NULL OR '
        "(profile_digest ~ '^[0-9a-f]{64}$' AND "
        "capability_profile_set_digest ~ '^[0-9a-f]{64}$' AND "
        "authorization_digest ~ '^[0-9a-f]{64}$')",
        name='serve047_profile_digests_ck'),
    sqlalchemy.CheckConstraint(
        "profile_kind IS NULL OR (profile_kind = 'ORDINARY_PAID' AND "
        "authorization_kind = 'PAID_CAPACITY_CLAIM') OR "
        "(profile_kind = 'ORDINARY_ZERO_COST' AND "
        "authorization_kind = 'ZERO_COST_ADMISSION') OR "
        "(profile_kind = 'RESERVED_FILL' AND "
        "authorization_kind = 'RESERVED_FILL_ALLOCATION') OR "
        "(profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' AND "
        "authorization_kind = 'UNKNOWN_CAPACITY_REPLACEMENT') OR "
        "(profile_kind = 'COST_REBALANCE' AND "
        "authorization_kind = 'COST_REBALANCE_DECISION') OR "
        "(profile_kind = 'SYSTEM_OOM_RECOVERY' AND "
        "authorization_kind = 'SYSTEM_OOM_RECOVERY')",
        name='serve047_profile_authorization_ck'),
    sqlalchemy.CheckConstraint(
        'reconciliation_outcome IS NULL OR '
        f'reconciliation_outcome IN ({_RECONCILIATION_OUTCOME_SQL})',
        name='serve047_reconciliation_ck'),
    sqlalchemy.CheckConstraint(
        '(binding_protocol_version IS NULL AND '
        'reconciliation_outcome IS NULL AND provider_evidence IS NULL) OR '
        '(binding_protocol_version = 2 AND '
        'reconciliation_outcome IS NOT NULL AND provider_evidence IS NOT NULL)',
        name='serve047_reconciliation_complete_ck'),
    sqlalchemy.CheckConstraint(
        "binding_protocol_version IS NULL OR "
        "(reconciliation_outcome = 'ACTIVE_ADOPT' AND "
        "resolution IN ('BOUND', 'CANCEL_REQUESTED')) OR "
        "(reconciliation_outcome = 'RESULT_RECORDED' AND "
        "resolution = 'RESULT_RECORDED') OR "
        "(reconciliation_outcome = 'PROJECTED' AND "
        "resolution = 'PROJECTED') OR "
        "(reconciliation_outcome = 'PRE_EFFECT_TERMINAL' AND "
        "resolution = 'PRE_EFFECT_TERMINAL') OR "
        "(reconciliation_outcome = 'POST_EFFECT_AMBIGUOUS' AND "
        "resolution = 'AMBIGUOUS')",
        name='serve047_reconciliation_resolution_ck'),
    sqlalchemy.CheckConstraint(
        'provider_evidence IS NULL OR '
        f'provider_evidence IN ({_PROVIDER_EVIDENCE_SQL})',
        name='serve047_provider_evidence_ck'),
    sqlalchemy.CheckConstraint(
        '(provider_evidence IS NULL AND '
        'provider_evidence_observed_at IS NULL AND '
        'provider_evidence_payload IS NULL AND '
        'provider_evidence_digest IS NULL) OR '
        "(provider_evidence = 'NOT_QUERIED' AND "
        'provider_evidence_observed_at IS NULL AND '
        'provider_evidence_payload IS NULL AND '
        'provider_evidence_digest IS NULL) OR '
        "(provider_evidence IN ('PRESENT', 'ABSENT', 'UNKNOWN', 'REPLACED') "
        'AND provider_evidence_observed_at IS NOT NULL AND '
        "jsonb_typeof(provider_evidence_payload) = 'object' AND "
        "provider_evidence_digest ~ '^[0-9a-f]{64}$')",
        name='serve047_provider_evidence_shape_ck'),
    sqlalchemy.CheckConstraint('owner_controller_epoch > 0',
                               name='serve_ordinary_binding_owner_epoch'),
    sqlalchemy.CheckConstraint('owner_revision > 0',
                               name='serve_ordinary_binding_owner_revision'),
    sqlalchemy.CheckConstraint(f'effect_phase IN ({_EFFECT_PHASE_SQL})',
                               name='serve_ordinary_binding_effect_phase'),
    sqlalchemy.CheckConstraint(f'resolution IN ({_RESOLUTION_SQL})',
                               name='serve_ordinary_binding_resolution'),
    sqlalchemy.CheckConstraint(
        f'terminal_status IS NULL OR terminal_status IN '
        f'({_TERMINAL_STATUS_SQL})',
        name='serve_ordinary_binding_terminal_status'),
    sqlalchemy.CheckConstraint(
        'terminal_execution_generation IS NULL OR '
        'terminal_execution_generation >= 0',
        name='serve_ordinary_binding_terminal_generation'),
    sqlalchemy.CheckConstraint(
        'execution_quiesced_generation IS NULL OR '
        'execution_quiesced_generation >= 0',
        name='serve_ordinary_binding_quiesced_generation'),
    sqlalchemy.CheckConstraint('service_job_id IS NULL OR service_job_id > 0',
                               name='serve_ordinary_binding_service_job_id'),
    sqlalchemy.CheckConstraint(
        "(resolution = 'AMBIGUOUS') = (ambiguity_code IS NOT NULL)",
        name='serve_ordinary_binding_ambiguity'),
    sqlalchemy.CheckConstraint(
        "resolution <> 'CANCEL_REQUESTED' OR "
        '(cancel_reason IS NOT NULL AND cancel_requested_at IS NOT NULL)',
        name='serve_ordinary_binding_cancel'),
    sqlalchemy.CheckConstraint(
        '(cancel_reason IS NULL) = (cancel_requested_at IS NULL)',
        name='serve_ordinary_binding_cancel_pair'),
    sqlalchemy.CheckConstraint(
        "(effect_phase = 'SERVICE_JOB_RECORDED') = "
        '(service_job_id IS NOT NULL)',
        name='serve_ordinary_binding_service_job'),
    sqlalchemy.CheckConstraint(
        "resolution <> 'RESULT_RECORDED' OR "
        "effect_phase = 'SERVICE_JOB_RECORDED'",
        name='serve_ordinary_binding_result_effect'),
    sqlalchemy.CheckConstraint(
        "resolution <> 'PROJECTED' OR effect_phase = 'SERVICE_JOB_RECORDED' "
        "OR (binding_protocol_version = 2 AND profile_kind = 'RESERVED_FILL' "
        "AND reconciliation_outcome = 'PROJECTED' AND "
        "provider_evidence = 'ABSENT' AND "
        "provider_evidence_observed_at >= execution_quiesced_at) OR "
        "(binding_protocol_version = 2 AND "
        "(profile_kind = 'ORDINARY_PAID' OR "
        "(profile_kind = 'UNKNOWN_CAPACITY_REPLACEMENT' AND "
        "((paid_capacity_pool_key::jsonb ->> 'cloud' = 'gcp' AND "
        "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
        "((paid_capacity_pool_key::jsonb ->> 'version' = '1') OR "
        "(paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
        "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
        "'gcp_project_id' ~ '^[a-z][a-z0-9-]{4,28}[a-z0-9]$'))) OR "
        "(paid_capacity_pool_key::jsonb ->> 'cloud' = 'aws' AND "
        "paid_capacity_pool_key::jsonb ->> 'version' = '2' AND "
        "paid_capacity_pool_key::jsonb ->> 'use_spot' = 'true' AND "
        "paid_capacity_pool_key::jsonb -> 'provider_identity' ->> "
        "'aws_account_id' ~ '^[0-9]{12}$')))) AND "
        "reconciliation_outcome = 'PROJECTED' AND "
        "provider_evidence = 'ABSENT' AND "
        "execution_quiesced_at IS NOT NULL AND "
        "provider_evidence_observed_at >= execution_quiesced_at AND "
        "effect_phase IN ('PROVIDER_IO', 'SERVICE_JOB_IO') AND "
        "paid_capacity_pool_key IS NOT NULL AND service_job_id IS NULL AND "
        f'{_ORDINARY_PAID_PROVIDER_TERMINAL_SQL})',
        name='serve047_provider_absence_projection_ck'),
    sqlalchemy.CheckConstraint(
        "resolution NOT IN ('RESULT_RECORDED', 'PROJECTED', "
        "'PRE_EFFECT_TERMINAL') OR "
        '(terminal_status IS NOT NULL AND '
        'terminal_execution_generation IS NOT NULL AND '
        'execution_quiescence_required IS NOT NULL)',
        name='serve_ordinary_binding_terminal_evidence'),
    sqlalchemy.CheckConstraint(
        '(execution_quiescence_required IS DISTINCT FROM TRUE) OR '
        '(execution_quiesced_generation = terminal_execution_generation AND '
        'execution_quiesced_at IS NOT NULL)',
        name='serve_ordinary_binding_quiescence'),
    sqlalchemy.CheckConstraint(
        "resolution <> 'PRE_EFFECT_TERMINAL' OR "
        "effect_phase = 'NOT_STARTED'",
        name='serve_ordinary_binding_pre_effect'),
    sqlalchemy.CheckConstraint(
        "resolution NOT IN ('PROJECTED', 'PRE_EFFECT_TERMINAL') OR "
        '(projected_at IS NOT NULL AND pin_released_at IS NOT NULL AND '
        'tombstone_not_before IS NOT NULL)',
        name='serve_ordinary_binding_projection'),
    sqlalchemy.CheckConstraint(
        "pin_released_at IS NULL OR resolution IN "
        "('PROJECTED', 'PRE_EFFECT_TERMINAL')",
        name='serve_ordinary_binding_pin_release'),
)
sqlalchemy.Index('uq_serve_ordinary_binding_submission',
                 ordinary_launch_associations_table.c.tenant_scope,
                 ordinary_launch_associations_table.c.service_workspace,
                 ordinary_launch_associations_table.c.submission_id,
                 unique=True)
sqlalchemy.Index('uq_serve_ordinary_binding_request',
                 ordinary_launch_associations_table.c.request_id,
                 unique=True)
sqlalchemy.Index('uq_serve_ordinary_binding_generation',
                 ordinary_launch_associations_table.c.service_name,
                 ordinary_launch_associations_table.c.replica_record_id,
                 ordinary_launch_associations_table.c.launch_generation,
                 unique=True)
sqlalchemy.Index(
    'uq_serve_ordinary_binding_unsettled',
    ordinary_launch_associations_table.c.service_name,
    ordinary_launch_associations_table.c.replica_record_id,
    unique=True,
    postgresql_where=ordinary_launch_associations_table.c.resolution.in_(
        tuple(value.value for value in UNSETTLED_RESOLUTIONS)))
sqlalchemy.Index('ix_serve_ordinary_binding_replica',
                 ordinary_launch_associations_table.c.service_name,
                 ordinary_launch_associations_table.c.replica_id,
                 ordinary_launch_associations_table.c.created_at)
sqlalchemy.Index(
    'ix_serve_ordinary_binding_gc',
    ordinary_launch_associations_table.c.tombstone_not_before,
    postgresql_where=ordinary_launch_associations_table.c.resolution.in_(
        tuple(value.value for value in SETTLED_RESOLUTIONS)))

legacy_reconciliation_scopes_table = sqlalchemy.Table(
    'serve_legacy_launch_reconciliation_scopes',
    metadata,
    sqlalchemy.Column('scope_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('scope_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('service_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_lifecycle_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('identity_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('identities', postgresql.JSONB, nullable=False),
    sqlalchemy.Column('identities_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('reviewed_by', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('review_reason', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('reviewed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.CheckConstraint(
        'scope_version = 1 AND service_lifecycle_epoch > 0 AND '
        'identity_count > 0 AND identity_count <= 1000',
        name='serve047_legacy_scope_shape_ck'),
    sqlalchemy.CheckConstraint(
        "jsonb_typeof(identities) = 'array' AND "
        'jsonb_array_length(identities) = identity_count',
        name='serve047_legacy_scope_identities_ck'),
    sqlalchemy.CheckConstraint("identities_sha256 ~ '^[0-9a-f]{64}$'",
                               name='serve047_legacy_scope_digest_ck'),
    sqlalchemy.CheckConstraint(
        'length(service_name) > 0 AND length(service_hash) > 0 AND '
        'length(reviewed_by) > 0 AND length(review_reason) > 0',
        name='serve047_legacy_scope_text_ck'),
)

legacy_reconciliations_table = sqlalchemy.Table(
    'serve_legacy_launch_reconciliations',
    metadata,
    sqlalchemy.Column('event_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('scope_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      sqlalchemy.ForeignKey(
                          'serve_legacy_launch_reconciliation_scopes.scope_id',
                          name='fk_serve047_legacy_reconciliation_scope',
                          ondelete='RESTRICT'),
                      nullable=False),
    sqlalchemy.Column('service_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_lifecycle_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('replica_record_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('replica_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('cluster_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('provider_context', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('provider_physical_resource_uid',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('reconciliation_sequence',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('observed_request_status',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('observed_request_execution_generation',
                      sqlalchemy.BigInteger),
    sqlalchemy.Column('observed_request_queue_present',
                      sqlalchemy.Boolean,
                      nullable=False),
    sqlalchemy.Column('observed_request_claim_present',
                      sqlalchemy.Boolean,
                      nullable=False),
    sqlalchemy.Column('observed_request_result_digest', sqlalchemy.Text),
    sqlalchemy.Column('observed_request_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('observed_request_evidence',
                      postgresql.JSONB,
                      nullable=False),
    sqlalchemy.Column('observed_request_evidence_digest',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('executor_terminated_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('executor_termination_evidence',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('executor_termination_evidence_digest', sqlalchemy.Text),
    sqlalchemy.Column('provider_evidence', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('provider_evidence_observed_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('provider_evidence_payload',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('provider_evidence_digest', sqlalchemy.Text),
    sqlalchemy.Column('cleanup_completed_at',
                      sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('cleanup_completion_evidence',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('cleanup_completion_evidence_digest', sqlalchemy.Text),
    sqlalchemy.Column('resolution', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('actor', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('reason', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.CheckConstraint(
        'service_lifecycle_epoch > 0 AND replica_id > 0 AND '
        'replica_version > 0 AND reconciliation_sequence > 0',
        name='serve047_legacy_positive_identity_ck'),
    sqlalchemy.CheckConstraint(
        'length(service_name) > 0 AND length(service_hash) > 0 AND '
        'length(cluster_name) > 0 AND length(request_id) > 0 AND '
        'length(provider_context) > 0 AND '
        'length(provider_physical_resource_uid) > 0 AND '
        'length(observed_request_status) > 0 AND '
        'length(actor) > 0 AND length(reason) > 0',
        name='serve047_legacy_text_ck'),
    sqlalchemy.CheckConstraint(
        "jsonb_typeof(observed_request_evidence) = 'object' AND "
        "observed_request_evidence_digest ~ '^[0-9a-f]{64}$' AND "
        '(observed_request_result_digest IS NULL OR '
        "observed_request_result_digest ~ '^[0-9a-f]{64}$')",
        name='serve047_legacy_request_evidence_ck'),
    sqlalchemy.CheckConstraint(
        'observed_request_execution_generation IS NULL OR '
        'observed_request_execution_generation >= 0',
        name='serve047_legacy_request_generation_ck'),
    sqlalchemy.CheckConstraint(
        '(executor_terminated_at IS NULL AND '
        'executor_termination_evidence IS NULL AND '
        'executor_termination_evidence_digest IS NULL) OR '
        '(executor_terminated_at IS NOT NULL AND '
        "jsonb_typeof(executor_termination_evidence) = 'object' AND "
        "executor_termination_evidence_digest ~ '^[0-9a-f]{64}$')",
        name='serve047_legacy_executor_evidence_ck'),
    sqlalchemy.CheckConstraint(
        f'provider_evidence IN '
        f'({_PROVIDER_EVIDENCE_SQL})',
        name='serve047_legacy_provider_evidence_ck'),
    sqlalchemy.CheckConstraint(
        "(provider_evidence = 'NOT_QUERIED' AND "
        'provider_evidence_observed_at IS NULL AND '
        'provider_evidence_payload IS NULL AND '
        'provider_evidence_digest IS NULL) OR '
        "(provider_evidence <> 'NOT_QUERIED' AND "
        'provider_evidence_observed_at IS NOT NULL AND '
        "jsonb_typeof(provider_evidence_payload) = 'object' AND "
        "provider_evidence_digest ~ '^[0-9a-f]{64}$')",
        name='serve047_legacy_provider_shape_ck'),
    sqlalchemy.CheckConstraint(f'resolution IN ({_LEGACY_RESOLUTION_SQL})',
                               name='serve047_legacy_resolution_ck'),
    sqlalchemy.CheckConstraint(
        "resolution = 'LEGACY_EFFECT_AMBIGUOUS' OR "
        "(observed_request_status IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND "
        'executor_terminated_at IS NOT NULL AND '
        "provider_evidence = 'ABSENT' AND "
        'provider_evidence_observed_at >= executor_terminated_at)',
        name='serve047_legacy_cleanup_authority_ck'),
    sqlalchemy.CheckConstraint(
        "(resolution <> 'PROJECTED' AND cleanup_completed_at IS NULL AND "
        'cleanup_completion_evidence IS NULL AND '
        'cleanup_completion_evidence_digest IS NULL) OR '
        "(resolution = 'PROJECTED' AND "
        'cleanup_completed_at >= provider_evidence_observed_at AND '
        "jsonb_typeof(cleanup_completion_evidence) = 'object' AND "
        "cleanup_completion_evidence_digest ~ '^[0-9a-f]{64}$')",
        name='serve047_legacy_cleanup_completion_ck'),
)
sqlalchemy.Index('uq_serve047_legacy_identity_sequence',
                 legacy_reconciliations_table.c.scope_id,
                 legacy_reconciliations_table.c.service_name,
                 legacy_reconciliations_table.c.service_hash,
                 legacy_reconciliations_table.c.replica_record_id,
                 legacy_reconciliations_table.c.cluster_name,
                 legacy_reconciliations_table.c.replica_id,
                 legacy_reconciliations_table.c.request_id,
                 legacy_reconciliations_table.c.provider_context,
                 legacy_reconciliations_table.c.provider_physical_resource_uid,
                 legacy_reconciliations_table.c.reconciliation_sequence,
                 unique=True)
sqlalchemy.Index('ix_serve047_legacy_resolution_created',
                 legacy_reconciliations_table.c.resolution,
                 legacy_reconciliations_table.c.created_at)


class OrdinaryLaunchBindingError(RuntimeError):
    """Base error for the closed ordinary-launch binding protocol."""


class OrdinaryLaunchBindingUnavailable(OrdinaryLaunchBindingError):
    """The selected store cannot safely perform the protocol."""


class OrdinaryLaunchBindingConflict(OrdinaryLaunchBindingError):
    """Durable state no longer matches an exact binding identity."""


class OrdinaryLaunchBindingBusy(OrdinaryLaunchBindingError):
    """Exclusive authority is busy; the caller must retry without blocking."""


@dataclasses.dataclass(frozen=True)
class BindingIntent:
    """Validated controller submission before server-derived identity."""

    service_name: str
    service_hash: str
    service_version: int
    replica_id: int
    replica_record_id: uuid.UUID
    lifecycle_epoch: int
    binding_epoch: int
    controller_incarnation: uuid.UUID
    controller_owner_epoch: int
    controller_pid: int | None
    controller_ip: str | None


@dataclasses.dataclass(frozen=True)
class BindingIdentity:
    """Complete immutable identity validated during atomic admission."""

    submission_id: uuid.UUID
    association_id: uuid.UUID
    request_id: str
    tenant_scope: str
    service_name: str
    service_hash: str
    service_workspace: str
    service_lifecycle_epoch: int
    service_binding_epoch: int
    service_version: int
    replica_id: int
    replica_record_id: uuid.UUID
    cluster_name: str
    input_digest: str
    digest_version: str
    controller_incarnation: uuid.UUID
    controller_owner_epoch: int


@dataclasses.dataclass(frozen=True)
class NonPoolBindingIdentity(BindingIdentity):
    """Complete protocol-v2 identity accepted by the generic handler."""

    profile: NonPoolLaunchProfile
    capability_cohort_epoch: int
    capability_profile_set_digest: str
    receipt_protocol_version: int


@dataclasses.dataclass(frozen=True)
class BindingAdmission:
    """Serve-side outcome of exact atomic admission."""

    disposition: AdmissionDisposition
    association_id: str
    request_id: str
    launch_generation: int
    owner_revision: int
    resolution: Resolution
    effect_phase: EffectPhase

    @property
    def created(self) -> bool:
        return self.disposition == AdmissionDisposition.CREATE

    @property
    def expects_active_request(self) -> bool:
        return self.resolution in UNSETTLED_RESOLUTIONS


@dataclasses.dataclass(frozen=True)
class FreshOrdinaryPaidTarget:
    """Exact planner row expected in one newly accepted paid wave."""

    replica_id: int
    replica_record_id: uuid.UUID
    service_version: int
    cluster_name: str


@dataclasses.dataclass(frozen=True)
class PreparedFreshOrdinaryPaidMember:
    """Validated immutable material for one transaction-local batch member."""

    target: FreshOrdinaryPaidTarget
    submission_id: uuid.UUID
    association_id: uuid.UUID
    request_id: str
    paid_capacity_pool_key: str
    profile: NonPoolLaunchProfile
    resource_action_identity: serve_state.ReplicaResourceActionIdentity


@dataclasses.dataclass(frozen=True)
class PreparedFreshOrdinaryPaidBatch:
    """Transaction-scoped proof consumed by the set-based graph writer.

    This is an internal capability for the fused paid-wave caller.  The caller
    may perform pure request construction after preparation, but must execute
    no database statement before passing the proof to the commit half.
    """

    tenant_scope: str
    authority: ControllerBindingAuthority
    members: tuple[PreparedFreshOrdinaryPaidMember, ...]
    _connection: sqlalchemy.engine.Connection = dataclasses.field(repr=False,
                                                                  compare=False)
    _transaction: Any = dataclasses.field(repr=False, compare=False)

    def belongs_to(self, connection: sqlalchemy.engine.Connection) -> bool:
        """Return whether this proof still belongs to the active transaction."""
        return (self._connection is connection and
                self._transaction is connection.get_transaction())


# Compatibility name used by the endpoint while the stack is rebased.
BindingReservation = BindingAdmission


@dataclasses.dataclass(frozen=True)
class BoundLaunchContext:
    association_id: uuid.UUID
    request_id: str
    service_name: str
    replica_id: int
    replica_record_id: uuid.UUID
    launch_generation: int
    input_digest: str


@dataclasses.dataclass(frozen=True)
class BoundNonPoolLaunchContext(BoundLaunchContext):
    """Complete protocol-v2 execution context."""

    profile: NonPoolLaunchProfile
    capability_cohort_epoch: int
    capability_profile_set_digest: str
    receipt_protocol_version: int


class ExecutionClaim(Protocol):
    """Request-layer claim shape without importing the request subsystem."""

    request_id: str
    execution_generation: int
    claim_token: str
    worker_instance_id: str | None


ClaimValidator = Callable[
    [sqlalchemy.engine.Connection, uuid.UUID, ExecutionClaim], bool]
PaidProviderAllocationRequestValidator = Callable[[
    sqlalchemy.engine.Connection, BoundNonPoolLaunchContext, Mapping[
        str, Any], 'paid_capacity_lib.PaidProviderAllocationReceipt'
], bool]
TransitionBarrier = Callable[[sqlalchemy.engine.Connection], bool]


@dataclasses.dataclass(frozen=True)
class EffectAuthorization:
    context: BoundLaunchContext
    claim: ExecutionClaim
    owner_revision: int
    durable_replica_info: Any
    guard: Any | None
    claim_validator: ClaimValidator


@dataclasses.dataclass(frozen=True)
class EffectAuthoritySnapshot:
    """Exact association and replica state proven at an effect boundary."""

    association: Mapping[str, Any]
    durable_replica_info: Any


@dataclasses.dataclass(frozen=True)
class TerminalEvidence:
    status: TerminalStatus
    cause: str
    execution_generation: int
    quiescence_required: bool
    quiesced_generation: int | None
    quiesced_at: datetime.datetime | None


@dataclasses.dataclass(frozen=True)
class LegacyLaunchIdentity:
    """Exact historical unbound launch identity sealed by an operator."""

    service_name: str
    service_hash: str
    service_lifecycle_epoch: int
    replica_id: int
    replica_record_id: uuid.UUID
    replica_version: int
    cluster_name: str
    request_id: str
    provider_context: str
    provider_physical_resource_uid: str

    def canonical_mapping(self) -> dict[str, Any]:
        return {
            'cluster_name': self.cluster_name,
            'provider_context': self.provider_context,
            'provider_physical_resource_uid':
                self.provider_physical_resource_uid,
            'replica_id': self.replica_id,
            'replica_record_id': str(self.replica_record_id),
            'replica_version': self.replica_version,
            'request_id': self.request_id,
        }


@dataclasses.dataclass(frozen=True)
class LegacyReconciliationEvidence:
    """One complete evidence snapshot for a historical unbound launch."""

    observed_request_status: str
    observed_request_execution_generation: int | None
    observed_request_queue_present: bool
    observed_request_claim_present: bool
    observed_request_result_digest: str | None
    observed_request_at: datetime.datetime
    observed_request_evidence: Mapping[str, Any]
    executor_terminated_at: datetime.datetime | None
    executor_termination_evidence: Mapping[str, Any] | None
    provider_evidence: ProviderEvidence
    provider_evidence_observed_at: datetime.datetime | None
    provider_evidence_payload: Mapping[str, Any] | None


@dataclasses.dataclass(frozen=True)
class RequestStartupFacts:
    exists: bool
    status: str | None
    queue_exists: bool
    execution_generation: int | None
    claim_exists: bool
    quiescent: bool


@dataclasses.dataclass(frozen=True)
class ControllerBindingAuthority:
    """Durable authority installed for one controller subprocess."""

    service_name: str
    service_hash: str
    service_workspace: str
    service_lifecycle_epoch: int
    controller_pid: int | None
    controller_ip: str | None
    controller_incarnation: uuid.UUID
    controller_owner_epoch: int
    capable: bool
    binding_mode: BindingMode
    binding_epoch: int
    non_pool_capable: bool = False
    non_pool_binding_protocol_version: int | None = None
    non_pool_profile_set_digest: str | None = None
    non_pool_capability_cohort_epoch: int | None = None
    non_pool_receipt_protocol_version: int | None = None

    @property
    def incarnation_uuid(self) -> uuid.UUID:
        return self.controller_incarnation

    @property
    def owner_epoch(self) -> int:
        return self.controller_owner_epoch

    @property
    def generic_launches_required(self) -> bool:
        """Whether every non-pool launch must use protocol v2."""
        return bool(self.capable is True and
                    self.binding_mode == BindingMode.BOUND and
                    self.non_pool_capable is True and
                    self.non_pool_binding_protocol_version
                    == NON_POOL_BINDING_PROTOCOL_VERSION and
                    self.non_pool_profile_set_digest
                    == supported_non_pool_profile_set_digest() and
                    self.non_pool_capability_cohort_epoch
                    == NON_POOL_CAPABILITY_COHORT_EPOCH and
                    self.non_pool_receipt_protocol_version
                    == NON_POOL_RECEIPT_PROTOCOL_VERSION)

    @property
    def retained_non_pool_settlement_allowed(self) -> bool:
        """Whether this owner may settle current/adjacent-cohort actions.

        A provider-semantic cohort rotation closes new admission immediately
        on the new binary, but the durable service tuple stays on the previous
        cohort until its requests and replicas are drained.  Permit that one
        adjacent cohort to adopt, reconcile, or retire existing actions.  This
        property must never guard request admission or provider-effect start.
        """
        cohort = self.non_pool_capability_cohort_epoch
        return bool(self.capable is True and
                    self.binding_mode == BindingMode.BOUND and
                    self.non_pool_capable is True and
                    self.non_pool_binding_protocol_version
                    == NON_POOL_BINDING_PROTOCOL_VERSION and
                    self.non_pool_profile_set_digest
                    == supported_non_pool_profile_set_digest() and
                    type(cohort) is int and
                    cohort in (NON_POOL_CAPABILITY_COHORT_EPOCH,
                               NON_POOL_CAPABILITY_COHORT_EPOCH - 1) and
                    self.non_pool_receipt_protocol_version
                    == NON_POOL_RECEIPT_PROTOCOL_VERSION)


# Compatibility name while the controller integration is assembled.
ServiceOwner = ControllerBindingAuthority


class ServiceTeardownDisposition(str, enum.Enum):
    """Atomic teardown publication result for one service owner."""

    UNSUPPORTED = 'UNSUPPORTED'
    MARKED_LEGACY = 'MARKED_LEGACY'
    MARKED_BOUND = 'MARKED_BOUND'


@dataclasses.dataclass(frozen=True)
class ServiceTeardownResult:
    """Mode classification committed with the teardown status write."""

    disposition: ServiceTeardownDisposition
    authority: ControllerBindingAuthority | None

    def __post_init__(self) -> None:
        has_authority = self.authority is not None
        if has_authority != (
                self.disposition == ServiceTeardownDisposition.MARKED_BOUND):
            raise ValueError(
                'Only marked-bound teardown may carry controller authority.')


def _canonical_uuid(value: Any, field_name: str) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    if not isinstance(value, str):
        raise ValueError(f'{field_name} must be a canonical UUID string.')
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(
            f'{field_name} must be a canonical UUID string.') from error
    if str(parsed) != value:
        raise ValueError(f'{field_name} must be a canonical UUID string.')
    return parsed


def _positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f'{field_name} must be a positive integer.')
    return value


def _nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f'{field_name} must be a non-negative integer.')
    return value


def _nonempty(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f'{field_name} must be non-empty text.')
    return value


def _non_pool_capability_from_service(
    service: Mapping[str, Any],
) -> tuple[bool, int | None, str | None, int | None, int | None]:
    """Decode the all-or-none generic launch capability tuple."""
    capable = service.get('non_pool_launch_binding_capable', False)
    capability_incarnation = service.get(
        'non_pool_launch_controller_incarnation')
    protocol_version = service.get('non_pool_launch_binding_protocol_version')
    profile_set_digest = service.get(
        'non_pool_launch_capability_profile_set_digest')
    cohort_epoch = service.get('non_pool_launch_capability_cohort_epoch')
    receipt_protocol_version = service.get(
        'non_pool_launch_receipt_protocol_version')
    values = (capability_incarnation, protocol_version, profile_set_digest,
              cohort_epoch, receipt_protocol_version)
    if capable is not True:
        if capable is not False or any(value is not None for value in values):
            raise OrdinaryLaunchBindingConflict(
                'Service generic launch capability tuple is malformed.')
        return False, None, None, None, None
    if (not isinstance(capability_incarnation, uuid.UUID) or
            protocol_version != NON_POOL_BINDING_PROTOCOL_VERSION or
            capability_incarnation != service.get('controller_incarnation') or
            profile_set_digest != supported_non_pool_profile_set_digest() or
            type(cohort_epoch) is not int or cohort_epoch < 1 or
            receipt_protocol_version != NON_POOL_RECEIPT_PROTOCOL_VERSION):
        raise OrdinaryLaunchBindingConflict(
            'Service generic launch capability tuple is unsupported.')
    return (True, protocol_version, profile_set_digest, cohort_epoch,
            receipt_protocol_version)


def _authority_from_service(
    service: Mapping[str, Any],
    *,
    controller_pid: int | None,
    controller_ip: str | None,
    controller_incarnation: uuid.UUID,
    controller_owner_epoch: int,
    capable: bool,
) -> ControllerBindingAuthority:
    """Build one process authority from the exact locked service row."""
    (non_pool_capable, protocol_version, profile_set_digest, cohort_epoch,
     receipt_protocol_version) = _non_pool_capability_from_service(service)
    return ControllerBindingAuthority(
        service_name=str(service['name']),
        service_hash=str(service['hash']),
        service_workspace=str(service['workspace']),
        service_lifecycle_epoch=int(service['lifecycle_epoch']),
        controller_pid=controller_pid,
        controller_ip=controller_ip,
        controller_incarnation=controller_incarnation,
        controller_owner_epoch=controller_owner_epoch,
        capable=capable,
        binding_mode=BindingMode(str(service['ordinary_launch_binding_mode'])),
        binding_epoch=int(service['ordinary_launch_binding_epoch']),
        non_pool_capable=non_pool_capable,
        non_pool_binding_protocol_version=protocol_version,
        non_pool_profile_set_digest=profile_set_digest,
        non_pool_capability_cohort_epoch=cohort_epoch,
        non_pool_receipt_protocol_version=receipt_protocol_version)


def build_paid_launch_fence(
    *,
    service_name: str,
    service_hash: str,
    service_version: int,
    replica_id: int,
    replica_record_id: str,
    service_lifecycle_epoch: int,
    binding_epoch: int,
    controller_incarnation: uuid.UUID,
    controller_owner_epoch: int,
    controller_pid: int,
    controller_ip: str,
) -> dict[str, Any]:
    """Build and validate the complete immutable paid launch fence."""
    fence = {
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: service_name,
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: service_hash,
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: service_version,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: controller_pid,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: controller_ip,
        REPLICA_ID_KEY: replica_id,
        REPLICA_RECORD_ID_KEY: replica_record_id,
        LIFECYCLE_EPOCH_KEY: service_lifecycle_epoch,
        BINDING_EPOCH_KEY: binding_epoch,
        CONTROLLER_INCARNATION_KEY: str(controller_incarnation),
        CONTROLLER_OWNER_EPOCH_KEY: controller_owner_epoch,
    }
    parse_unbound_launch_context(fence)
    return fence


def parse_unbound_launch_context(context: Mapping[str, Any]) -> BindingIntent:
    """Parse a controller submission accepted by the private endpoint."""
    if not isinstance(context, Mapping):
        raise ValueError('Ordinary launch context must be a mapping.')
    server_owned_keys = (
        SUBMISSION_ID_KEY,
        ASSOCIATION_ID_KEY,
        LAUNCH_GENERATION_KEY,
        BOUND_REQUEST_ID_KEY,
        INPUT_DIGEST_KEY,
        OWNER_REVISION_KEY,
        BINDING_PROTOCOL_VERSION_KEY,
        PROFILE_KIND_KEY,
        PROFILE_VERSION_KEY,
        PROFILE_DIGEST_KEY,
        CAPABILITY_COHORT_EPOCH_KEY,
        CAPABILITY_PROFILE_SET_DIGEST_KEY,
        RECEIPT_PROTOCOL_VERSION_KEY,
        AUTHORIZATION_KIND_KEY,
        AUTHORIZATION_REFERENCE_KEY,
        AUTHORIZATION_GENERATION_KEY,
        AUTHORIZATION_DIGEST_KEY,
    )
    if any(key in context for key in server_owned_keys):
        raise ValueError('Ordinary launch context contains server-owned IDs.')
    service_name = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
    service_hash = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY)
    controller_pid = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY)
    controller_ip = context.get(
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY)
    if not (controller_pid is None or
            type(controller_pid) is int and controller_pid > 0):
        raise ValueError('Ordinary launch controller PID is malformed.')
    if not (controller_ip is None or isinstance(controller_ip, str)):
        raise ValueError('Ordinary launch controller IP is malformed.')
    return BindingIntent(
        service_name=_nonempty(service_name, 'service_name'),
        service_hash=_nonempty(service_hash, 'service_hash'),
        service_version=_positive_int(
            context.get(
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY),
            'service_version'),
        replica_id=_positive_int(context.get(REPLICA_ID_KEY), 'replica_id'),
        replica_record_id=_canonical_uuid(context.get(REPLICA_RECORD_ID_KEY),
                                          'replica_record_id'),
        lifecycle_epoch=_positive_int(context.get(LIFECYCLE_EPOCH_KEY),
                                      'lifecycle_epoch'),
        binding_epoch=_positive_int(context.get(BINDING_EPOCH_KEY),
                                    'binding_epoch'),
        controller_incarnation=_canonical_uuid(
            context.get(CONTROLLER_INCARNATION_KEY), 'controller_incarnation'),
        controller_owner_epoch=_positive_int(
            context.get(CONTROLLER_OWNER_EPOCH_KEY), 'controller_owner_epoch'),
        controller_pid=controller_pid,
        controller_ip=controller_ip,
    )


def has_bound_launch_context(context: Mapping[str, Any]) -> bool:
    """Whether any server-owned binding field is present.

    Callers use this as a fail-closed discriminator: no fields selects the
    legacy path, while one or more fields selects bound parsing, which rejects
    partial or malformed context.
    """
    return isinstance(context, Mapping) and any(key in context for key in (
        SUBMISSION_ID_KEY,
        ASSOCIATION_ID_KEY,
        LAUNCH_GENERATION_KEY,
        BOUND_REQUEST_ID_KEY,
        INPUT_DIGEST_KEY,
        OWNER_REVISION_KEY,
        BINDING_PROTOCOL_VERSION_KEY,
        PROFILE_KIND_KEY,
        PROFILE_VERSION_KEY,
        PROFILE_DIGEST_KEY,
        CAPABILITY_COHORT_EPOCH_KEY,
        CAPABILITY_PROFILE_SET_DIGEST_KEY,
        RECEIPT_PROTOCOL_VERSION_KEY,
        AUTHORIZATION_KIND_KEY,
        AUTHORIZATION_REFERENCE_KEY,
        AUTHORIZATION_GENERATION_KEY,
        AUTHORIZATION_DIGEST_KEY,
    ))


def canonical_launch_digest(request_body: Any) -> str:
    """Hash canonical prepared LaunchBody bytes before mutable normalization."""
    try:
        payload = json.loads(request_body.model_dump_json())
        launch_context = payload.get('extra_launch_context')
        if isinstance(launch_context, dict):
            # Server-bound and legacy routing owner fields are mutable and are
            # not part of the stable submission identity.
            for key in (ASSOCIATION_ID_KEY, LAUNCH_GENERATION_KEY,
                        BOUND_REQUEST_ID_KEY, INPUT_DIGEST_KEY,
                        OWNER_REVISION_KEY, CONTROLLER_INCARNATION_KEY,
                        CONTROLLER_OWNER_EPOCH_KEY,
                        BINDING_PROTOCOL_VERSION_KEY, PROFILE_KIND_KEY,
                        PROFILE_VERSION_KEY, PROFILE_DIGEST_KEY,
                        CAPABILITY_COHORT_EPOCH_KEY,
                        CAPABILITY_PROFILE_SET_DIGEST_KEY,
                        RECEIPT_PROTOCOL_VERSION_KEY, AUTHORIZATION_KIND_KEY,
                        AUTHORIZATION_REFERENCE_KEY,
                        AUTHORIZATION_GENERATION_KEY, AUTHORIZATION_DIGEST_KEY,
                        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY,
                        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY):
                launch_context.pop(key, None)
        canonical = json.dumps(payload,
                               sort_keys=True,
                               separators=(',', ':'),
                               ensure_ascii=False,
                               allow_nan=False).encode('utf-8')
    except (AttributeError, TypeError, UnicodeError, ValueError) as error:
        raise ValueError(
            'Ordinary launch body is not canonical JSON.') from error
    return hashlib.sha256(canonical).hexdigest()


def derive_binding_ids(tenant_scope: str, service_workspace: str,
                       submission_id: uuid.UUID) -> tuple[uuid.UUID, str]:
    tenant_scope = _nonempty(tenant_scope, 'tenant_scope')
    service_workspace = _nonempty(service_workspace, 'service_workspace')
    submission_id = _canonical_uuid(submission_id, 'submission_id')
    key = json.dumps([tenant_scope, service_workspace,
                      str(submission_id)],
                     ensure_ascii=True,
                     separators=(',', ':'))
    association_id = uuid.uuid5(_ASSOCIATION_NAMESPACE, key)
    request_id = str(uuid.uuid5(_REQUEST_NAMESPACE, key))
    return association_id, request_id


def derive_ordinary_launch_submission_id(
    service_name: str,
    replica_id: int,
    replica_record_id: uuid.UUID | str,
    launch_generation: int,
) -> uuid.UUID:
    """Derive the established generation-stable ordinary submission UUID."""
    service_name = _nonempty(service_name, 'service_name')
    replica_id = _positive_int(replica_id, 'replica_id')
    replica_record_id = _canonical_uuid(replica_record_id, 'replica_record_id')
    launch_generation = _positive_int(launch_generation, 'launch_generation')
    material = (f'{service_name}\0{replica_id}\0{replica_record_id}\0'
                f'{launch_generation}')
    return uuid.uuid5(_ORDINARY_LAUNCH_SUBMISSION_NAMESPACE, material)


def derive_fresh_ordinary_paid_resource_action_identity(
    *,
    replica_id: int,
    replica_record_id: uuid.UUID | str,
    cluster_name: str,
) -> serve_state.ReplicaResourceActionIdentity:
    """Derive one initial paid replica's action identity without state.

    ``replica_record_id`` is the immutable row incarnation already carried by
    the bound request.  Separate UUID namespaces keep the replica incarnation
    and global cluster-record identity semantically distinct without adding a
    second wire representation that could drift.
    """
    replica_id = _positive_int(replica_id, 'replica_id')
    replica_record_id = _canonical_uuid(replica_record_id, 'replica_record_id')
    cluster_name = _nonempty(cluster_name, 'cluster_name')
    material = str(replica_record_id)
    return serve_state.ReplicaResourceActionIdentity(
        replica_id=replica_id,
        cluster_name=cluster_name,
        replica_incarnation=uuid.uuid5(
            _ORDINARY_PAID_REPLICA_INCARNATION_NAMESPACE, material),
        desired_generation=1,
        sky_cluster_record_uuid=uuid.uuid5(
            _ORDINARY_PAID_CLUSTER_RECORD_NAMESPACE, material))


def fresh_ordinary_paid_resource_action_identity_from_launch_context(
    launch_context: Mapping[str, Any],
    cluster_name: str,
) -> serve_state.ReplicaResourceActionIdentity | None:
    """Decode the cohort-gated action identity used by cluster persistence."""
    if (not isinstance(launch_context, Mapping) or
            BINDING_PROTOCOL_VERSION_KEY not in launch_context):
        return None
    context = parse_bound_non_pool_launch_context(launch_context)
    if context.profile.kind is not NonPoolLaunchProfileKind.ORDINARY_PAID:
        return None
    if (context.capability_cohort_epoch
            < ORDINARY_PAID_RESOURCE_ACTION_IDENTITY_COHORT_FLOOR):
        return None
    if (context.capability_cohort_epoch > NON_POOL_CAPABILITY_COHORT_EPOCH or
            context.launch_generation != 1):
        raise OrdinaryLaunchBindingConflict(
            'Fresh ordinary-paid launch has no supported resource-action '
            'identity cohort or generation.')
    return derive_fresh_ordinary_paid_resource_action_identity(
        replica_id=context.replica_id,
        replica_record_id=context.replica_record_id,
        cluster_name=cluster_name)


def build_binding_identity(
    intent: BindingIntent,
    *,
    submission_id: uuid.UUID,
    tenant_scope: str,
    service_workspace: str,
    cluster_name: str,
    input_digest: str,
) -> BindingIdentity:
    if not isinstance(intent, BindingIntent):
        raise ValueError('intent must be a BindingIntent.')
    submission_id = _canonical_uuid(submission_id, 'submission_id')
    tenant_scope = _nonempty(tenant_scope, 'tenant_scope')
    service_workspace = _nonempty(service_workspace, 'service_workspace')
    cluster_name = _nonempty(cluster_name, 'cluster_name')
    if not isinstance(input_digest,
                      str) or not _SHA256_RE.fullmatch(input_digest):
        raise ValueError('input_digest must be lowercase SHA-256.')
    association_id, request_id = derive_binding_ids(tenant_scope,
                                                    service_workspace,
                                                    submission_id)
    return BindingIdentity(
        submission_id=submission_id,
        association_id=association_id,
        request_id=request_id,
        tenant_scope=tenant_scope,
        service_name=intent.service_name,
        service_hash=intent.service_hash,
        service_workspace=service_workspace,
        service_lifecycle_epoch=intent.lifecycle_epoch,
        service_binding_epoch=intent.binding_epoch,
        service_version=intent.service_version,
        replica_id=intent.replica_id,
        replica_record_id=intent.replica_record_id,
        cluster_name=cluster_name,
        input_digest=input_digest,
        digest_version=DIGEST_VERSION,
        controller_incarnation=intent.controller_incarnation,
        controller_owner_epoch=intent.controller_owner_epoch,
    )


def build_non_pool_binding_identity(
    intent: BindingIntent,
    *,
    submission_id: uuid.UUID,
    tenant_scope: str,
    service_workspace: str,
    cluster_name: str,
    input_digest: str,
    profile: NonPoolLaunchProfile,
    capability_cohort_epoch: int,
    capability_profile_set_digest: str,
    receipt_protocol_version: int,
) -> NonPoolBindingIdentity:
    """Build a complete v2 identity; partial capability tuples are invalid."""
    if not isinstance(profile, NonPoolLaunchProfile):
        raise ValueError('profile must be a NonPoolLaunchProfile.')
    profile.validate()
    capability_cohort_epoch = _positive_int(capability_cohort_epoch,
                                            'capability_cohort_epoch')
    if (not isinstance(capability_profile_set_digest, str) or
            not _SHA256_RE.fullmatch(capability_profile_set_digest)):
        raise ValueError('capability_profile_set_digest must be SHA-256.')
    if (capability_profile_set_digest
            != supported_non_pool_profile_set_digest()):
        raise ValueError('Capability profile set is not locally supported.')
    if receipt_protocol_version != NON_POOL_RECEIPT_PROTOCOL_VERSION:
        raise ValueError('Receipt protocol version is not supported.')
    base = build_binding_identity(intent,
                                  submission_id=submission_id,
                                  tenant_scope=tenant_scope,
                                  service_workspace=service_workspace,
                                  cluster_name=cluster_name,
                                  input_digest=input_digest)
    base_values = {
        field.name: getattr(base, field.name)
        for field in dataclasses.fields(BindingIdentity)
    }
    return NonPoolBindingIdentity(
        **base_values,
        profile=profile,
        capability_cohort_epoch=capability_cohort_epoch,
        capability_profile_set_digest=capability_profile_set_digest,
        receipt_protocol_version=receipt_protocol_version)


def _install_bound_context_base(request_body: Any, identity: BindingIdentity,
                                launch_generation: int) -> None:
    context = dict(request_body.extra_launch_context)
    # These values fenced admission, but authority is mutable.  A controller
    # takeover adopts this exact queued body and resolves the new owner from
    # the locked service/association rows at each effect boundary.
    context.pop(CONTROLLER_INCARNATION_KEY, None)
    context.pop(CONTROLLER_OWNER_EPOCH_KEY, None)
    context.pop(OWNER_REVISION_KEY, None)
    context.update({
        ASSOCIATION_ID_KEY: str(identity.association_id),
        REPLICA_ID_KEY: identity.replica_id,
        REPLICA_RECORD_ID_KEY: str(identity.replica_record_id),
        LAUNCH_GENERATION_KEY: launch_generation,
        BOUND_REQUEST_ID_KEY: identity.request_id,
        INPUT_DIGEST_KEY: identity.input_digest,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: LEGACY_FAIL_CLOSED_CONTROLLER_PID,
        serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: LEGACY_FAIL_CLOSED_CONTROLLER_IP,
    })
    request_body.extra_launch_context = context


def install_bound_context(request_body: Any, identity: BindingIdentity,
                          launch_generation: int) -> None:
    """Install an immutable protocol-v1 ordinary identity."""
    if isinstance(identity, NonPoolBindingIdentity):
        raise ValueError('Use install_bound_non_pool_context for v2 identity.')
    _install_bound_context_base(request_body, identity, launch_generation)


def install_bound_non_pool_context(request_body: Any,
                                   identity: NonPoolBindingIdentity,
                                   launch_generation: int) -> None:
    """Install a complete immutable protocol-v2 identity."""
    if not isinstance(identity, NonPoolBindingIdentity):
        raise ValueError('identity must be a NonPoolBindingIdentity.')
    identity.profile.validate()
    recovery_context: dict[str, Any] | None = None
    has_recovery_context = system_oom_recovery.has_v3_system_oom_recovery_context(
        request_body.extra_launch_context)
    if identity.profile.kind == NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY:
        if not has_recovery_context:
            raise ValueError(
                'System-OOM profile has no recovery execution envelope.')
        recovery_context = system_oom_recovery.extract_unbound_launch_context(
            request_body.extra_launch_context)
        nonce = recovery_context[
            serve_constants.SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY]
        if identity.profile.authorization_reference != f'system-oom:{nonce}':
            raise ValueError(
                'System-OOM profile does not name its execution nonce.')
    elif has_recovery_context:
        raise ValueError(
            'A non-recovery profile contains a system-OOM envelope.')
    _install_bound_context_base(request_body, identity, launch_generation)
    context = dict(request_body.extra_launch_context)
    context.update({
        BINDING_PROTOCOL_VERSION_KEY: NON_POOL_BINDING_PROTOCOL_VERSION,
        PROFILE_KIND_KEY: identity.profile.kind.value,
        PROFILE_VERSION_KEY: identity.profile.version,
        PROFILE_DIGEST_KEY: identity.profile.digest,
        CAPABILITY_COHORT_EPOCH_KEY: identity.capability_cohort_epoch,
        CAPABILITY_PROFILE_SET_DIGEST_KEY:
            identity.capability_profile_set_digest,
        RECEIPT_PROTOCOL_VERSION_KEY: identity.receipt_protocol_version,
        AUTHORIZATION_KIND_KEY: identity.profile.authorization_kind.value,
        AUTHORIZATION_REFERENCE_KEY: identity.profile.authorization_reference,
        AUTHORIZATION_GENERATION_KEY: identity.profile.authorization_generation,
        AUTHORIZATION_DIGEST_KEY: identity.profile.authorization_digest,
    })
    if recovery_context is not None:
        # Generic ownership is association/owner-epoch based. Preserve the
        # legacy recovery matcher inputs, but make its mutable PID/IP fence
        # impossible so an old execution path cannot authorize this request.
        recovery_context[
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY] = (
                LEGACY_FAIL_CLOSED_CONTROLLER_PID)
        recovery_context[
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY] = (
                LEGACY_FAIL_CLOSED_CONTROLLER_IP)
        bound_recovery = system_oom_recovery.bind_launch_context(
            recovery_context, identity.request_id)
        context.pop(serve_constants.SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY, None)
        context.update(bound_recovery)
    request_body.extra_launch_context = context


# Temporary compatibility for stack-local callers; the owner revision was
# intentionally removed because controller takeover must adopt the same body.
_install_bound_context = install_bound_context

_NON_POOL_CONTEXT_KEYS = (
    BINDING_PROTOCOL_VERSION_KEY,
    PROFILE_KIND_KEY,
    PROFILE_VERSION_KEY,
    PROFILE_DIGEST_KEY,
    CAPABILITY_COHORT_EPOCH_KEY,
    CAPABILITY_PROFILE_SET_DIGEST_KEY,
    RECEIPT_PROTOCOL_VERSION_KEY,
    AUTHORIZATION_KIND_KEY,
    AUTHORIZATION_REFERENCE_KEY,
    AUTHORIZATION_GENERATION_KEY,
    AUTHORIZATION_DIGEST_KEY,
)


def _parse_bound_launch_context_base(
        context: Mapping[str, Any]) -> BoundLaunchContext:
    if not isinstance(context, Mapping):
        raise OrdinaryLaunchBindingConflict(
            'Bound ordinary-launch context must be a mapping.')
    return BoundLaunchContext(
        association_id=_canonical_uuid(context.get(ASSOCIATION_ID_KEY),
                                       'association_id'),
        request_id=_nonempty(context.get(BOUND_REQUEST_ID_KEY), 'request_id'),
        service_name=_nonempty(
            context.get(serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY),
            'service_name'),
        replica_id=_positive_int(context.get(REPLICA_ID_KEY), 'replica_id'),
        replica_record_id=_canonical_uuid(context.get(REPLICA_RECORD_ID_KEY),
                                          'replica_record_id'),
        launch_generation=_positive_int(context.get(LAUNCH_GENERATION_KEY),
                                        'launch_generation'),
        input_digest=_nonempty(context.get(INPUT_DIGEST_KEY), 'input_digest'),
    )


def parse_bound_launch_context(
        context: Mapping[str, Any]) -> BoundLaunchContext:
    """Parse only the legacy protocol-v1 ordinary handler context."""
    if isinstance(context, Mapping) and any(
            key in context for key in _NON_POOL_CONTEXT_KEYS):
        raise OrdinaryLaunchBindingConflict(
            'Protocol-v2 context cannot enter the ordinary handler.')
    return _parse_bound_launch_context_base(context)


def parse_bound_non_pool_launch_context(
        context: Mapping[str, Any]) -> BoundNonPoolLaunchContext:
    """Parse a complete context accepted by the generic handler only."""
    if not isinstance(context, Mapping):
        raise OrdinaryLaunchBindingConflict(
            'Bound non-pool launch context must be a mapping.')
    if not all(key in context for key in _NON_POOL_CONTEXT_KEYS):
        raise OrdinaryLaunchBindingConflict(
            'Generic non-pool handler requires a complete profile context.')
    if context.get(
            BINDING_PROTOCOL_VERSION_KEY) != NON_POOL_BINDING_PROTOCOL_VERSION:
        raise OrdinaryLaunchBindingConflict(
            'Bound non-pool launch protocol is unsupported.')
    try:
        profile = NonPoolLaunchProfile.from_mapping(context)
        capability_cohort_epoch = _positive_int(
            context.get(CAPABILITY_COHORT_EPOCH_KEY), 'capability_cohort_epoch')
        capability_profile_set_digest = _nonempty(
            context.get(CAPABILITY_PROFILE_SET_DIGEST_KEY),
            'capability_profile_set_digest')
        if (not _SHA256_RE.fullmatch(capability_profile_set_digest) or
                capability_profile_set_digest
                != supported_non_pool_profile_set_digest()):
            raise ValueError('Capability profile set is unsupported.')
        receipt_protocol_version = _positive_int(
            context.get(RECEIPT_PROTOCOL_VERSION_KEY),
            'receipt_protocol_version')
        if receipt_protocol_version != NON_POOL_RECEIPT_PROTOCOL_VERSION:
            raise ValueError('Receipt protocol version is unsupported.')
    except ValueError as error:
        raise OrdinaryLaunchBindingConflict(
            'Bound non-pool launch profile is invalid.') from error
    base = _parse_bound_launch_context_base(context)
    base_values = {
        field.name: getattr(base, field.name)
        for field in dataclasses.fields(BoundLaunchContext)
    }
    parsed = BoundNonPoolLaunchContext(
        **base_values,
        profile=profile,
        capability_cohort_epoch=capability_cohort_epoch,
        capability_profile_set_digest=capability_profile_set_digest,
        receipt_protocol_version=receipt_protocol_version)
    has_recovery_context = system_oom_recovery.has_v3_system_oom_recovery_context(
        context)
    if profile.kind == NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY:
        if not has_recovery_context:
            raise OrdinaryLaunchBindingConflict(
                'System-OOM profile has no bound recovery envelope.')
        try:
            recovery_context = system_oom_recovery.extract_bound_launch_context(
                dict(context))
        except (TypeError, ValueError) as error:
            raise OrdinaryLaunchBindingConflict(
                'System-OOM bound recovery envelope is malformed.') from error
        if (recovery_context[
                serve_constants.SYSTEM_OOM_RECOVERY_BOUND_REQUEST_ID_KEY]
                != parsed.request_id or recovery_context[
                    serve_constants.SYSTEM_OOM_RECOVERY_REPLICA_ID_KEY]
                != parsed.replica_id or recovery_context[
                    serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY]
                != parsed.service_name):
            raise OrdinaryLaunchBindingConflict(
                'System-OOM bound recovery envelope has a different action '
                'identity.')
    elif has_recovery_context:
        raise OrdinaryLaunchBindingConflict(
            'A non-recovery profile contains a system-OOM envelope.')
    return parsed


def _require_postgres(connection: sqlalchemy.engine.Connection) -> None:
    if connection.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise OrdinaryLaunchBindingUnavailable(
            'Ordinary launch binding requires central PostgreSQL state.')


def lock_legacy_request_admission_in_connection(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
) -> None:
    """Pair one legacy request INSERT with the service transition lock.

    Binding promotion already owns the exclusive service launch-authority
    advisory lock on its transaction.  A valid legacy Serve launch admission
    takes the shared side of that exact lock before inserting its request and
    queue rows.  Therefore an admission that wins first commits and is visible
    to promotion's legacy drain, while an admission that loses waits until the
    bound mode is durable and its legacy pre-effect/provider fences fail closed.

    This transaction-scoped lock deliberately replaces a table ``SHARE`` lock.
    A queue claimant first owns request/queue row locks and then updates those
    tables; a table lock compatible with its initial ``ROW SHARE`` lock can
    otherwise deadlock that later lock upgrade while promotion waits for the
    claimant's row.
    """
    _require_postgres(connection)
    service_name = _nonempty(service_name, 'service_name')
    if not connection.in_transaction():
        raise OrdinaryLaunchBindingUnavailable(
            'Legacy launch admission locking requires an active transaction.')
    # This is the same stable key used by
    # ``service_replica_launch_authority_write_session``.  Keep key derivation
    # single-owned by Serve so request admission cannot silently drift from the
    # transition/provider lock domain.
    lock_id = serve_state._replica_launch_authority_lock_id(  # pylint: disable=protected-access
        service_name, connection.engine)
    connection.execute(
        sqlalchemy.text(
            'SELECT pg_catalog.pg_advisory_xact_lock_shared(:lock_key)'),
        {'lock_key': locks.postgres_lock_key(lock_id)})


def _replica_record_id(row: Mapping[str, Any]) -> str | None:
    replica_state = row.get('replica_state')
    if not isinstance(replica_state, dict):
        return None
    value = replica_state.get('replica_record_id')
    return value if isinstance(value, str) else None


def _replica_snapshot_matches_association(
    replica: Mapping[str, Any],
    association: Mapping[str, Any],
    *,
    require_launch_authorized: bool,
) -> bool:
    """Validate both query columns and the full versioned replica payload."""
    state_version = replica.get('replica_state_version')
    state = replica.get('replica_state')
    if (type(state_version) is not int or not isinstance(state, dict) or
            replica.get('replica_id') != association['replica_id'] or
            replica.get('version') != association['service_version'] or
            replica.get('cluster_name') != association['cluster_name'] or
            replica.get('paid_capacity_pool_key')
            != association.get('paid_capacity_pool_key') or
            _replica_record_id(replica) != str(
                association['replica_record_id'])):
        return False
    try:
        info = serve_state.decode_replica_state_for_authority(
            state_version, state)
    except (AttributeError, KeyError, RuntimeError, TypeError, ValueError):
        return False
    if (info.replica_id != association['replica_id'] or
            info.version != association['service_version'] or
            info.cluster_name != association['cluster_name'] or
            info.replica_record_id != str(association['replica_record_id']) or
            info.paid_capacity_pool_key != replica.get('paid_capacity_pool_key')
            or info.status.value != replica.get('status')):
        return False
    persisted_profile = association.get('profile_kind')
    if persisted_profile is None:
        # Protocol-v1 associations retain the narrow ordinary-only contract.
        if not replica_has_narrow_ordinary_profile(info):
            return False
    else:
        try:
            expected_profile = NonPoolLaunchProfileKind(str(persisted_profile))
        except ValueError:
            return False
        if classify_non_pool_launch_profile(info) != expected_profile:
            return False
    if require_launch_authorized and info.status.value not in ('PENDING',
                                                               'PROVISIONING'):
        return False
    return True


def replica_has_narrow_ordinary_profile(info: Any) -> bool:
    """Whether a decoded replica has no retained special-launch authority."""
    try:
        disposition = info.system_recovery_disposition.value
    except AttributeError:
        return False
    return bool(
        info.reserved_fill is False and info.reserved_fill_pool_key is None and
        info.reserved_fill_service_generation is None and
        info.reserved_fill_physical_cluster_uid is None and
        info.reserved_fill_kubernetes_context is None and
        info.is_zero_cost is False and
        info.unknown_capacity_replacement is False and
        info.cost_rebalance_for_replica_id is None and
        info.system_recovery_launch_intent is None and
        disposition == 'ORDINARY' and info.launch_request_id is None and
        info.service_job_id is None and
        info.candidate_ready_observed_at is None and
        info.ordinary_release_not_before is None and
        info.system_recovery_revision == 0 and info.system_recovery is None and
        info.system_recovery_quarantine is None)


_RESERVED_FILL_PROFILE_FIELDS = (
    'reserved_fill_pool_key',
    'reserved_fill_service_generation',
    'reserved_fill_physical_cluster_uid',
    'reserved_fill_kubernetes_context',
    'reserved_fill_allocation_generation',
    'reserved_fill_allocation_input_sha256',
    'reserved_fill_allocation_claim_generation',
    'reserved_fill_reconciliation_gate_generation',
    'reserved_fill_reclaim_fleet_bundle_sha256',
    'reserved_fill_reclaim_policy_revision',
    'reserved_fill_reclaim_provider_inventory_sha256',
    'reserved_fill_worker_projection_sha256',
    'reserved_fill_observation_generation',
    'reserved_fill_observation_sequence',
    'reserved_fill_intent_idempotency_key',
)


def _classify_non_pool_launch_profile(
    info: Any,
    *,
    allow_uncommitted_reserved_fill: bool,
) -> NonPoolLaunchProfileKind | None:
    """Classify a replica or one exact pre-admission reserved-fill draft.

    Classification is intentionally strict. Missing, partial, contradictory,
    or malformed state returns ``None`` and cannot enter the generic binding.
    The result identifies the planner that must validate the authorization
    envelope; it is not itself an authorization decision.
    """
    try:
        reserved_fill = info.reserved_fill
        is_zero_cost = info.is_zero_cost
        unknown_replacement = info.unknown_capacity_replacement
        rebalance_predecessor = info.cost_rebalance_for_replica_id
        recovery_disposition = info.system_recovery_disposition.value
        recovery_revision = info.system_recovery_revision
        reserved_values = tuple(
            getattr(info, field) for field in _RESERVED_FILL_PROFILE_FIELDS)
        zero_cost_sequences = (info.zero_cost_admission_sequence,
                               info.zero_cost_materialization_sequence)
        system_recovery_values = (
            info.system_recovery_launch_intent,
            info.launch_request_id,
            info.service_job_id,
            info.candidate_ready_observed_at,
            info.ordinary_release_not_before,
            info.system_recovery,
            info.system_recovery_quarantine,
        )
    except AttributeError:
        return None
    if (type(reserved_fill) is not bool or type(is_zero_cost) is not bool or
            type(unknown_replacement) is not bool):
        return None
    if (rebalance_predecessor is not None and
        (isinstance(rebalance_predecessor, bool) or
         not isinstance(rebalance_predecessor, int) or
         rebalance_predecessor < 1)):
        return None
    if (recovery_disposition not in ('ORDINARY', 'CANDIDATE', 'CAPABLE') or
            isinstance(recovery_revision, bool) or
            not isinstance(recovery_revision, int) or recovery_revision < 0):
        return None

    has_system_recovery = bool(
        recovery_disposition != 'ORDINARY' or recovery_revision > 0 or
        any(value is not None for value in system_recovery_values))
    if has_system_recovery:
        if (info.system_recovery_launch_intent is None or
                recovery_revision < 1 or
                info.system_recovery_quarantine is not None):
            return None
        return NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY
    if any(value is not None
           for value in reserved_values) and not reserved_fill:
        return None
    if any(value is not None
           for value in zero_cost_sequences) and not is_zero_cost:
        return None
    if reserved_fill:
        committed_sequence = (type(info.zero_cost_admission_sequence) is int and
                              info.zero_cost_admission_sequence >= 1)
        uncommitted_sequence = bool(
            allow_uncommitted_reserved_fill and
            info.zero_cost_admission_sequence is None and
            info.zero_cost_materialization_sequence is None)
        if (not is_zero_cost or
                any(value is None for value in reserved_values) or
                not (committed_sequence or uncommitted_sequence)):
            return None
        return NonPoolLaunchProfileKind.RESERVED_FILL
    if unknown_replacement:
        return NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT
    if rebalance_predecessor is not None:
        return NonPoolLaunchProfileKind.COST_REBALANCE
    if is_zero_cost:
        if (type(info.zero_cost_admission_sequence) is not int or
                info.zero_cost_admission_sequence < 1 or
            (info.zero_cost_materialization_sequence is not None and
             (type(info.zero_cost_materialization_sequence) is not int or
              info.zero_cost_materialization_sequence < 1))):
            return None
        return NonPoolLaunchProfileKind.ORDINARY_ZERO_COST
    if replica_has_narrow_ordinary_profile(info):
        return NonPoolLaunchProfileKind.ORDINARY_PAID
    return None


def classify_non_pool_launch_profile(
        info: Any) -> NonPoolLaunchProfileKind | None:
    """Classify only a fully persisted non-pool replica profile."""
    return _classify_non_pool_launch_profile(
        info, allow_uncommitted_reserved_fill=False)


def classify_uncommitted_protocol_v2_reserved_fill_profile(
        info: Any, *, protocol_version: int) -> NonPoolLaunchProfileKind | None:
    """Classify only a typed v2 fill before its admission transaction.

    Protocol-v2 fill deliberately freezes its launch thread before atomically
    persisting the replica.  The transaction is the sole owner of the first
    zero-cost admission sequence, so that one field must still be null while
    the thread is constructed.  This classifier accepts only that transient
    v2 shape; every durable reader continues to use the strict classifier.
    """
    if type(protocol_version) is not int or protocol_version != 2:
        return None
    kind = _classify_non_pool_launch_profile(
        info, allow_uncommitted_reserved_fill=True)
    if kind is not NonPoolLaunchProfileKind.RESERVED_FILL:
        return None
    if (info.zero_cost_admission_sequence is not None or
            info.zero_cost_materialization_sequence is not None):
        return None
    return kind


def _locked_replica_info(replica: Mapping[str, Any]) -> Any:
    """Decode one exact row used as profile authority."""
    state_version = replica.get('replica_state_version')
    state = replica.get('replica_state')
    if type(state_version) is not int or not isinstance(state, dict):
        raise OrdinaryLaunchBindingConflict(
            'Non-pool profile authority requires a current replica record.')
    try:
        return serve_state.decode_replica_state_for_authority(
            state_version, state)
    except (AttributeError, KeyError, RuntimeError, TypeError,
            ValueError) as error:
        raise OrdinaryLaunchBindingConflict(
            'Non-pool profile authority could not decode the replica record.'
        ) from error


def _replica_placement_payload(info: Any) -> dict[str, Any]:
    """Return the immutable placement fields already owned by ReplicaInfo."""
    return {
        'cluster_name': info.cluster_name,
        'is_spot': info.is_spot,
        'location': info.location,
        'planned_capacity': info.planned_capacity,
        'resources_override': info.resources_override,
        'service_version': info.version,
    }


def build_replacement_planner_authorization(
    kind: NonPoolLaunchProfileKind,
    authority: ControllerBindingAuthority,
    *,
    predecessor_replica_id: int,
    predecessor_record_id: str,
    predecessor_service_version: int,
    observation_generation: int | None = None,
    observation_service_version: int | None = None,
    target_capacity: int | None = None,
    target_capacity_by_accelerator: Mapping[str, int] | None = None,
    accelerator_shapes: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Build the immutable planner half of a replacement launch intent."""
    if kind not in (NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT,
                    NonPoolLaunchProfileKind.COST_REBALANCE):
        raise ValueError('Only replacement profiles own this authorization.')
    predecessor = {
        'replica_id': _positive_int(predecessor_replica_id,
                                    'predecessor_replica_id'),
        'replica_record_id': str(
            _canonical_uuid(predecessor_record_id, 'predecessor_record_id')),
        'service_version': _positive_int(predecessor_service_version,
                                         'predecessor_service_version'),
    }
    result: dict[str, Any] = {
        'authorization_version': 1,
        'predecessor': predecessor,
        'profile_kind': kind.value,
        'service_binding_epoch': _positive_int(authority.binding_epoch,
                                               'service_binding_epoch'),
        'service_hash': _nonempty(authority.service_hash, 'service_hash'),
        'service_lifecycle_epoch': _positive_int(
            authority.service_lifecycle_epoch, 'service_lifecycle_epoch'),
    }
    if kind == NonPoolLaunchProfileKind.COST_REBALANCE:
        if any(value is not None
               for value in (observation_generation,
                             observation_service_version, target_capacity,
                             target_capacity_by_accelerator,
                             accelerator_shapes)):
            raise ValueError(
                'Cost-rebalance authorization cannot carry an outage '
                'observation.')
        return result
    generation = _positive_int(observation_generation, 'observation_generation')
    service_version = _positive_int(observation_service_version,
                                    'observation_service_version')
    capacity = _nonnegative_int(target_capacity, 'target_capacity')

    def _accelerator_state(values: Mapping[str, int] | None, *,
                           positive: bool) -> list[list[Any]]:
        if values is None:
            return []
        if not isinstance(values, Mapping):
            raise ValueError('Accelerator state must be a mapping.')
        normalized: list[list[Any]] = []
        for raw_card, raw_value in values.items():
            card = _nonempty(raw_card, 'accelerator')
            value = (_positive_int(raw_value, 'accelerator value') if positive
                     else _nonnegative_int(raw_value, 'accelerator value'))
            normalized.append([card, value])
        normalized.sort(key=lambda item: item[0].casefold())
        return normalized

    result['observation'] = {
        'accelerator_shapes': _accelerator_state(accelerator_shapes,
                                                 positive=True),
        'classification': 'UNKNOWN',
        'reconcile_generation': generation,
        'service_version': service_version,
        'target_capacity': capacity,
        'target_capacity_by_accelerator': _accelerator_state(
            target_capacity_by_accelerator, positive=False),
    }
    return result


@dataclasses.dataclass(frozen=True)
class ReplacementPredecessorIdentity:
    """Immutable identity cited by one replacement authorization."""

    replica_id: int
    replica_record_id: str
    service_version: int


def _decode_unknown_capacity_observation(
    raw: Any,) -> tuple[dict[str, Any], int, int, int]:
    expected_keys = {
        'accelerator_shapes', 'classification', 'reconcile_generation',
        'service_version', 'target_capacity', 'target_capacity_by_accelerator'
    }
    if (not isinstance(raw, dict) or set(raw) != expected_keys or
            raw.get('classification') != 'UNKNOWN'):
        raise ValueError('Unknown-capacity observation authority is malformed.')
    generation = _positive_int(raw.get('reconcile_generation'),
                               'reconcile_generation')
    service_version = _positive_int(raw.get('service_version'),
                                    'observation_service_version')
    target_capacity = _nonnegative_int(raw.get('target_capacity'),
                                       'target_capacity')
    for field, positive in (('target_capacity_by_accelerator', False),
                            ('accelerator_shapes', True)):
        state = raw.get(field)
        if not isinstance(state, list):
            raise ValueError(
                'Unknown-capacity accelerator authority is malformed.')
        normalized_cards = []
        for item in state:
            if (not isinstance(item, list) or len(item) != 2 or
                    not isinstance(item[0], str) or not item[0] or
                    isinstance(item[1], bool) or not isinstance(item[1], int) or
                    item[1] < int(positive)):
                raise ValueError(
                    'Unknown-capacity accelerator authority is malformed.')
            normalized_cards.append(item[0].casefold())
        if (normalized_cards != sorted(normalized_cards) or
                len(normalized_cards) != len(set(normalized_cards))):
            raise ValueError(
                'Unknown-capacity accelerator authority is not canonical.')
    return raw, generation, service_version, target_capacity


def decode_replacement_predecessor_authorization(
    raw: Any,
    kind: NonPoolLaunchProfileKind,
    *,
    expected_authority: ControllerBindingAuthority | None = None,
) -> ReplacementPredecessorIdentity:
    """Decode one complete immutable authorization and its predecessor."""
    if kind not in (NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT,
                    NonPoolLaunchProfileKind.COST_REBALANCE):
        raise ValueError('Only replacement profiles cite a predecessor.')
    expected_keys = {
        'authorization_version', 'predecessor', 'profile_kind',
        'service_binding_epoch', 'service_hash', 'service_lifecycle_epoch'
    }
    if kind == NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT:
        expected_keys.add('observation')
    if (not isinstance(raw, dict) or set(raw) != expected_keys or
            raw.get('authorization_version') != 1 or
            raw.get('profile_kind') != kind.value):
        raise ValueError('Replacement planner authorization is malformed.')
    binding_epoch = _positive_int(raw.get('service_binding_epoch'),
                                  'service_binding_epoch')
    service_hash = _nonempty(raw.get('service_hash'), 'service_hash')
    lifecycle_epoch = _positive_int(raw.get('service_lifecycle_epoch'),
                                    'service_lifecycle_epoch')
    if expected_authority is not None:
        if not isinstance(expected_authority, ControllerBindingAuthority):
            raise ValueError('Expected binding authority is malformed.')
        if (binding_epoch != expected_authority.binding_epoch or
                service_hash != expected_authority.service_hash or
                lifecycle_epoch != expected_authority.service_lifecycle_epoch):
            raise ValueError('Replacement planner authorization is stale.')
    if kind == NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT:
        _decode_unknown_capacity_observation(raw.get('observation'))
    predecessor = raw.get('predecessor')
    if not isinstance(predecessor, dict) or set(predecessor) != {
            'replica_id', 'replica_record_id', 'service_version'
    }:
        raise ValueError('Replacement predecessor authority is malformed.')
    return ReplacementPredecessorIdentity(
        replica_id=_positive_int(predecessor.get('replica_id'),
                                 'predecessor_replica_id'),
        replica_record_id=str(
            _canonical_uuid(predecessor.get('replica_record_id'),
                            'predecessor_record_id')),
        service_version=_positive_int(predecessor.get('service_version'),
                                      'predecessor_service_version'))


def _replacement_planner_authorization(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    info: Any,
    kind: NonPoolLaunchProfileKind,
) -> tuple[dict[str, Any], Any]:
    """Validate an immutable replacement intent and its predecessor row."""
    raw = replica.get('non_pool_launch_authorization')
    if not isinstance(raw, dict):
        raise OrdinaryLaunchBindingConflict(
            'Replacement profile has no durable planner authorization.')
    expected_keys = {
        'authorization_version', 'predecessor', 'profile_kind',
        'service_binding_epoch', 'service_hash', 'service_lifecycle_epoch'
    }
    if kind == NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT:
        expected_keys.add('observation')
    if (set(raw) != expected_keys or raw.get('authorization_version') != 1 or
            raw.get('profile_kind') != kind.value or
            raw.get('service_binding_epoch')
            != service.get('ordinary_launch_binding_epoch') or
            raw.get('service_hash') != service.get('hash') or
            raw.get('service_lifecycle_epoch')
            != service.get('lifecycle_epoch')):
        raise OrdinaryLaunchBindingConflict(
            'Replacement planner authorization is malformed or stale.')
    try:
        predecessor = decode_replacement_predecessor_authorization(raw, kind)
    except ValueError as error:
        raise OrdinaryLaunchBindingConflict(
            'Replacement predecessor authority is malformed.') from error
    predecessor_row = connection.execute(
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name == service['name'],
            serve_state_schema.replicas_table.c.replica_id ==
            predecessor.replica_id)).mappings().one_or_none()
    if predecessor_row is None:
        raise OrdinaryLaunchBindingConflict(
            'Replacement predecessor no longer exists.')
    predecessor_info = _locked_replica_info(predecessor_row)
    if (predecessor_info.replica_record_id != predecessor.replica_record_id or
            predecessor_info.version != predecessor.service_version or
            predecessor_info.version != info.version or
            predecessor_info.is_terminal):
        raise OrdinaryLaunchBindingConflict(
            'Replacement predecessor identity or lifecycle changed.')
    return raw, predecessor_info


def _paid_claim_payload_from_rows(
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    info: Any,
    rows: Sequence[Mapping[str, Any]],
) -> tuple[str | None, dict[str, Any] | None]:
    """Validate and project already-read exact paid-claim rows."""
    pool_key = replica.get('paid_capacity_pool_key')
    if pool_key is None:
        if rows:
            raise OrdinaryLaunchBindingConflict(
                'Zero-cost profile retained a paid-capacity claim.')
        return None, None
    if (not isinstance(pool_key, str) or not pool_key or len(rows) != 1 or
            rows[0]['service_hash'] != service['hash'] or
            rows[0]['pool_key'] != pool_key or
            info.paid_capacity_pool_key != pool_key):
        raise OrdinaryLaunchBindingConflict(
            'Non-pool profile lost its exact paid-capacity claim.')
    row = rows[0]
    if (row['capacity_plan_generation'] is not None and
        (not isinstance(info.planned_capacity, int) or
         isinstance(info.planned_capacity, bool) or
         info.planned_capacity != row['capacity_plan_units'])):
        raise OrdinaryLaunchBindingConflict(
            'Paid replica width contradicts its capacity-plan debit.')
    payload = {
        'claimed_at': row['claimed_at'],
        'pool_key': pool_key,
        'priority': row['priority'],
        'service_hash': row['service_hash'],
    }
    if row['capacity_plan_generation'] is not None:
        payload.update({
            'capacity_plan_generation': row['capacity_plan_generation'],
            'capacity_plan_sha256': row['capacity_plan_sha256'],
            'demand_feed_generation': row['demand_feed_generation'],
            'demand_source_epoch': row['demand_source_epoch'],
            'capacity_plan_accelerator': row['capacity_plan_accelerator'],
            'capacity_plan_units': row['capacity_plan_units'],
        })
    return pool_key, payload


def _paid_claim_payload(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    info: Any,
) -> tuple[str | None, dict[str, Any] | None]:
    """Read the exact paid claim, if any, for a locked replica row."""
    rows = connection.execute(
        sqlalchemy.select(serve_state_schema.paid_capacity_claims_table).where(
            serve_state_schema.paid_capacity_claims_table.c.service_name ==
            service['name'],
            serve_state_schema.paid_capacity_claims_table.c.replica_id ==
            replica['replica_id'])).mappings().all()
    return _paid_claim_payload_from_rows(service, replica, info, rows)


def _zero_cost_sequence_payload(
    connection: sqlalchemy.engine.Connection,
    info: Any,
    *,
    require_current_ordinary_high_water: int | None = None,
) -> dict[str, Any]:
    """Validate database-assigned zero-cost sequencing for one row."""
    table = pool_capacity_observation_schema.protocol_state_sequence_table
    row = connection.execute(sqlalchemy.select(table).where(
        table.c.id == 1)).mappings().one_or_none()
    if row is None:
        raise OrdinaryLaunchBindingConflict(
            'Zero-cost profile has no durable protocol sequencer.')
    admission_sequence = info.zero_cost_admission_sequence
    materialization_sequence = info.zero_cost_materialization_sequence
    current_admission = row['zero_cost_admission_sequence']
    current_materialization = row['zero_cost_materialization_sequence']
    # Admission and successful materialization are independent event streams.
    # Authenticate each replica marker only against its own durable high-water;
    # their numeric values have no cross-stream ordering relationship.
    if (type(admission_sequence) is not int or admission_sequence < 1 or
            type(current_admission) is not int or
            current_admission < admission_sequence or
        (materialization_sequence is not None and
         (type(materialization_sequence) is not int or materialization_sequence
          < 1 or type(current_materialization) is not int or
          current_materialization < materialization_sequence))):
        raise OrdinaryLaunchBindingConflict(
            'Zero-cost profile has stale or malformed sequencer authority.')
    current_ordinary = row['ordinary_zero_cost_admission_sequence']
    if (require_current_ordinary_high_water is not None and
            current_ordinary != require_current_ordinary_high_water):
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill allocation was superseded by ordinary zero-cost '
            'admission.')
    return {
        'admission_sequence': admission_sequence,
        'materialization_sequence': materialization_sequence,
        'protocol_version': row['protocol_version'],
        'reconciliation_gate_generation': row['reconciliation_gate_generation'],
    }


def _non_pool_funding_payload(
    connection: sqlalchemy.engine.Connection,
    info: Any,
    paid_claim: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return the exact paid claim or zero-cost admission for a profile."""
    if paid_claim is not None:
        if info.is_zero_cost:
            raise OrdinaryLaunchBindingConflict(
                'Non-pool profile cannot carry paid and zero-cost authority.')
        return {'kind': 'PAID', 'claim': paid_claim}
    if not info.is_zero_cost:
        raise OrdinaryLaunchBindingConflict(
            'Non-pool profile has neither paid nor zero-cost authority.')
    return {
        'kind': 'ZERO_COST',
        'sequence': _zero_cost_sequence_payload(connection, info),
    }


def _reserved_fill_payload(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    info: Any,
    *,
    freeze_reconciliation_gate: bool = False,
) -> dict[str, Any]:
    """Resolve one fill intent against its allocation and observation rows."""
    if (service.get('reserved_fill_actuation_mode') ==
            zero_cost_actuation.ActuationMode.DURABLE_INTENT.value):
        intent = zero_cost_actuation.committed_intent_for_replica_in_connection(
            connection,
            service_name=service['name'],
            service_hash=service['hash'],
            replica_info=info)
        if intent is None:
            raise OrdinaryLaunchBindingConflict(
                'Durable reserved-fill profile lost its exact committed '
                'intent handoff.')
        observations = (pool_capacity_observation_schema.
                        demand_capacity_observations_v2_table)
        observation = connection.execute(
            sqlalchemy.select(observations).where(
                observations.c.pool_key == intent.pool_key,
                observations.c.observation_generation ==
                intent.observation_generation,
                observations.c.observation_sequence ==
                intent.observation_sequence, observations.c.observation_status
                == pool_capacity_observation_schema.SUCCESS)).mappings(
                ).one_or_none()
        if observation is None:
            raise OrdinaryLaunchBindingConflict(
                'Durable reserved-fill profile lost its committed '
                'observation evidence.')
        sequence = _zero_cost_sequence_payload(connection, info)
        if freeze_reconciliation_gate:
            sequence = _freeze_reserved_fill_sequence_gate(
                info, sequence, committed_intent=intent)
        return {
            'allocation_input_sha256': intent.allocation_input_sha256,
            'claim_generation': intent.allocation_claim_generation,
            'intent_idempotency_key': intent.idempotency_key,
            'observation_payload_sha256': observation['payload_sha256'],
            'physical_cluster_uid': intent.physical_cluster_uid,
            'pool_key': intent.pool_key,
            'reclaim_fleet_bundle_sha256': intent.reclaim_fleet_bundle_sha256,
            'reclaim_policy_revision': intent.reclaim_policy_revision,
            'reclaim_provider_inventory_sha256':
                intent.reclaim_provider_inventory_sha256,
            'sequence': sequence,
            'worker_projection_sha256': intent.worker_projection_sha256,
        }

    allocation_table = (
        pool_capacity_observation_schema.reserved_fill_service_allocation_table)
    allocation_row = connection.execute(
        sqlalchemy.select(allocation_table).where(
            allocation_table.c.service_name ==
            service['name'])).mappings().one_or_none()
    if allocation_row is None or type(
            allocation_row['allocation_map']) is not dict:
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill profile has no authenticated allocation map.')
    try:
        allocation = reserved_fill_planner.AuthenticatedAllocationMap.from_mapping(
            allocation_row['allocation_map'])
    except ValueError as error:
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill allocation map failed authentication.') from error
    if (allocation.allocation_generation
            != info.reserved_fill_allocation_generation or
            allocation.allocation_input_sha256
            != info.reserved_fill_allocation_input_sha256 or
            allocation.allocation_claim_generation
            != info.reserved_fill_allocation_claim_generation or
            allocation.reconciliation_gate_generation
            != info.reserved_fill_reconciliation_gate_generation or
            allocation.service_version != info.version or
            allocation_row['allocation_gate_generation']
            != allocation.reconciliation_gate_generation or
            allocation.reclaim_fleet_bundle_sha256
            != info.reserved_fill_reclaim_fleet_bundle_sha256 or
            allocation.reclaim_policy_revision
            != info.reserved_fill_reclaim_policy_revision or
            allocation.reclaim_provider_inventory_sha256
            != info.reserved_fill_reclaim_provider_inventory_sha256):
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill replica no longer matches its allocation map.')

    snapshot = next((candidate for candidate in allocation.pool_snapshots
                     if candidate.pool_key == info.reserved_fill_pool_key),
                    None)
    if snapshot is None:
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill allocation no longer contains the exact pool.')
    projection_digests = dict(snapshot.worker_projection_sha256_by_accelerator)
    location = info.location
    accelerator = None
    if isinstance(location, dict):
        accelerators = location.get('accelerators')
        if isinstance(accelerators, dict) and len(accelerators) == 1:
            accelerator = next(iter(accelerators)).casefold()
    if (snapshot.service_generation != info.reserved_fill_service_generation or
            snapshot.physical_cluster_uid
            != info.reserved_fill_physical_cluster_uid or
            snapshot.observation_generation
            != info.reserved_fill_observation_generation or
            snapshot.observation_sequence
            != info.reserved_fill_observation_sequence or accelerator is None or
            projection_digests.get(accelerator)
            != info.reserved_fill_worker_projection_sha256 or location
            not in tuple(item.to_pickleable() for item in snapshot.locations)):
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill exact pool, card, observation, or placement '
            'authority changed.')

    claim_sets = serve_state_schema.reserved_fill_service_claim_sets_table
    claim_set = connection.execute(
        sqlalchemy.select(claim_sets).where(
            claim_sets.c.service_name ==
            service['name'])).mappings().one_or_none()
    edges = serve_state_schema.reserved_fill_pool_claims_table
    edge = connection.execute(
        sqlalchemy.select(edges).where(
            edges.c.service_name == service['name'], edges.c.pool_key ==
            info.reserved_fill_pool_key)).mappings().one_or_none()
    if (claim_set is None or edge is None or
            claim_set['claim_set_state'] != 'authoritative_v2' or
            claim_set['generation'] != info.reserved_fill_service_generation or
            claim_set['service_version'] != info.version or
            edge['service_generation'] != info.reserved_fill_service_generation
            or edge['physical_cluster_uid']
            != info.reserved_fill_physical_cluster_uid):
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill claim-set authority changed.')

    provenance_table = (
        pool_capacity_observation_schema.reserved_fill_round_observation_table)
    provenance = connection.execute(
        sqlalchemy.select(provenance_table).where(
            provenance_table.c.pool_key ==
            info.reserved_fill_pool_key)).mappings().one_or_none()
    observations = (
        pool_capacity_observation_schema.demand_capacity_observations_v2_table)
    database_epoch = sqlalchemy.func.extract('epoch',
                                             sqlalchemy.func.clock_timestamp())
    observation = connection.execute(
        sqlalchemy.select(observations).where(
            observations.c.pool_key == info.reserved_fill_pool_key,
            observations.c.observation_generation ==
            info.reserved_fill_observation_generation,
            observations.c.observation_sequence ==
            info.reserved_fill_observation_sequence,
            observations.c.observation_status ==
            pool_capacity_observation_schema.SUCCESS, observations.c.valid_until
            >= database_epoch)).mappings().one_or_none()
    if (provenance is None or observation is None or
            provenance['observation_generation']
            != info.reserved_fill_observation_generation or
            provenance['observation_sequence']
            != info.reserved_fill_observation_sequence or
            observation['payload_sha256']
            != provenance['observation_payload_sha256']):
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill observation is stale or no longer exact.')

    sequence = _zero_cost_sequence_payload(
        connection,
        info,
        require_current_ordinary_high_water=(
            allocation.ordinary_zero_cost_admission_sequence_high_water))
    if freeze_reconciliation_gate:
        sequence = _freeze_reserved_fill_sequence_gate(info, sequence)
    return {
        'allocation_input_sha256': allocation.allocation_input_sha256,
        'claim_generation': allocation.allocation_claim_generation,
        'intent_idempotency_key': info.reserved_fill_intent_idempotency_key,
        'observation_payload_sha256': observation['payload_sha256'],
        'physical_cluster_uid': snapshot.physical_cluster_uid,
        'pool_key': snapshot.pool_key,
        'reclaim_fleet_bundle_sha256': allocation.reclaim_fleet_bundle_sha256,
        'reclaim_policy_revision': allocation.reclaim_policy_revision,
        'reclaim_provider_inventory_sha256':
            allocation.reclaim_provider_inventory_sha256,
        'sequence': sequence,
        'worker_projection_sha256': info.reserved_fill_worker_projection_sha256,
    }


def _reserved_fill_cleanup_payload(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    info: Any,
) -> dict[str, Any]:
    """Reconstruct immutable fill evidence for provider-present teardown.

    This is the sole compatibility consumer for a retained Serve055 JSON-only
    intent edge.  It cannot authorize launch, workspace access, materialization,
    or a lease.  New scalar-linked rows take the canonical resolver.
    """
    intent = (zero_cost_actuation.
              cleanup_only_committed_intent_for_replica_in_connection(
                  connection,
                  service_name=service['name'],
                  service_hash=service['hash'],
                  replica_info=info))
    if intent is None:
        raise OrdinaryLaunchBindingConflict(
            'Provider-present cleanup lost its exact committed intent '
            'evidence.')
    observations = (
        pool_capacity_observation_schema.demand_capacity_observations_v2_table)
    observation = connection.execute(
        sqlalchemy.select(observations).where(
            observations.c.pool_key == intent.pool_key,
            observations.c.observation_generation ==
            intent.observation_generation,
            observations.c.observation_sequence == intent.observation_sequence,
            observations.c.observation_status ==
            pool_capacity_observation_schema.SUCCESS)).mappings().one_or_none()
    if observation is None:
        raise OrdinaryLaunchBindingConflict(
            'Provider-present cleanup lost its committed observation '
            'evidence.')
    sequence = _zero_cost_sequence_payload(connection, info)
    # The association profile is frozen by admission before provider launch.
    # A successful projection later stamps ReplicaInfo's independent
    # materialization sequence, but that post-effect receipt cannot rewrite the
    # already-bound profile. Validate the current monotonic receipt above, then
    # reconstruct the admission-time payload with no materialization yet.
    sequence = {**sequence, 'materialization_sequence': None}
    sequence = _freeze_reserved_fill_sequence_gate(info,
                                                   sequence,
                                                   committed_intent=intent)
    return {
        'allocation_input_sha256': intent.allocation_input_sha256,
        'claim_generation': intent.allocation_claim_generation,
        'intent_idempotency_key': intent.idempotency_key,
        'observation_payload_sha256': observation['payload_sha256'],
        'physical_cluster_uid': intent.physical_cluster_uid,
        'pool_key': intent.pool_key,
        'reclaim_fleet_bundle_sha256': intent.reclaim_fleet_bundle_sha256,
        'reclaim_policy_revision': intent.reclaim_policy_revision,
        'reclaim_provider_inventory_sha256':
            intent.reclaim_provider_inventory_sha256,
        'sequence': sequence,
        'worker_projection_sha256': intent.worker_projection_sha256,
    }


def _freeze_reserved_fill_sequence_gate(
    info: Any,
    sequence: Mapping[str, Any],
    *,
    committed_intent: Any | None = None,
) -> dict[str, Any]:
    """Project the admission-time gate without weakening live sequencing."""
    frozen_gate = info.reserved_fill_reconciliation_gate_generation
    frozen_protocol = sequence.get('protocol_version')
    if committed_intent is not None:
        if (getattr(committed_intent, 'reconciliation_gate_generation',
                    None) != frozen_gate or
                getattr(committed_intent, 'idempotency_key',
                        None) != info.reserved_fill_intent_idempotency_key):
            raise OrdinaryLaunchBindingConflict(
                'Reserved-fill cleanup intent lost its frozen gate identity.')
        frozen_protocol = getattr(committed_intent, 'protocol_version', None)
        frozen_gate = getattr(committed_intent,
                              'reconciliation_gate_generation', None)
    current_gate = sequence.get('reconciliation_gate_generation')
    current_protocol = sequence.get('protocol_version')
    if (type(frozen_gate) is not int or frozen_gate < 1 or
            type(frozen_protocol) is not int or frozen_protocol < 1 or
            type(current_gate) is not int or current_gate < frozen_gate or
            type(current_protocol) is not int or
            current_protocol < frozen_protocol):
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill cleanup lost its monotonic gate history.')
    return {
        **sequence,
        'protocol_version': frozen_protocol,
        'reconciliation_gate_generation': frozen_gate,
    }


def _cost_rebalance_payload(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    info: Any,
) -> tuple[dict[str, Any], int]:
    """Resolve a replacement against the persisted stabilization decision."""
    authorization, predecessor_info = _replacement_planner_authorization(
        connection, service, replica, info,
        NonPoolLaunchProfileKind.COST_REBALANCE)
    state = service.get('cost_rebalance_state')
    predecessor = info.cost_rebalance_for_replica_id
    if (not isinstance(state, dict) or state.get('version') != 1 or
            state.get('service_version') != info.version or
            not isinstance(state.get('candidates'), list)):
        raise OrdinaryLaunchBindingConflict(
            'Cost-rebalance profile has no current durable planner state.')
    location = info.location
    matching = [
        item for item in state['candidates']
        if isinstance(item, dict) and item.get('replica_id') == predecessor and
        item.get('location') == location
    ]
    if len(matching) != 1:
        raise OrdinaryLaunchBindingConflict(
            'Cost-rebalance predecessor and target decision are no longer '
            'current.')
    decision = matching[0]
    first_seen_at = decision.get('first_seen_at')
    if (isinstance(first_seen_at, bool) or
            not isinstance(first_seen_at, (int, float)) or
            not math.isfinite(float(first_seen_at)) or first_seen_at < 0):
        raise OrdinaryLaunchBindingConflict(
            'Cost-rebalance decision has no stable generation.')
    decision_generation = int(float(first_seen_at) * 1_000_000)
    return ({
        'authorization': authorization,
        'decision': matching[0],
        'placement': _replica_placement_payload(info),
        'predecessor_replica_id': predecessor,
        'predecessor_record_id': predecessor_info.replica_record_id,
    }, decision_generation)


def _unknown_capacity_replacement_payload(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    info: Any,
) -> tuple[dict[str, Any], int]:
    """Resolve one exact UNKNOWN observation committed by the planner."""
    authorization, predecessor_info = _replacement_planner_authorization(
        connection, service, replica, info,
        NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT)
    try:
        observation, generation, observation_version, target_capacity = (
            _decode_unknown_capacity_observation(
                authorization.get('observation')))
    except ValueError as error:
        raise OrdinaryLaunchBindingConflict(
            'Unknown-capacity observation authority is malformed.') from error
    if observation_version != info.version or target_capacity < int(
            info.planned_capacity):
        raise OrdinaryLaunchBindingConflict(
            'Unknown-capacity observation no longer authorizes this shape.')
    return ({
        'authorization': authorization,
        'funding': None,
        'observation': observation,
        'placement': _replica_placement_payload(info),
        'predecessor_record_id': predecessor_info.replica_record_id,
        'reason': 'logical-capacity-observation-unknown',
    }, generation)


def _resolve_non_pool_launch_profile_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    *,
    protocol_and_service_prelocked: bool = False,
    validate_paid_provider_start: bool = True,
) -> tuple[NonPoolLaunchProfile, datetime.datetime | None]:
    """Recompute one profile from planner-owned durable authority."""
    _require_postgres(connection)
    info = _locked_replica_info(replica)
    kind = classify_non_pool_launch_profile(info)
    if kind is None:
        raise OrdinaryLaunchBindingConflict(
            'Replica does not have one complete non-pool launch profile.')
    pool_key, paid_claim = _paid_claim_payload(connection, service, replica,
                                               info)
    paid_fresh_until = None
    if paid_claim is not None and validate_paid_provider_start:
        paid_fresh_until = capacity_admission.validate_paid_claim_in_connection(
            connection,
            service,
            paid_claim,
            protocol_and_service_prelocked=(protocol_and_service_prelocked))
    placement = _replica_placement_payload(info)
    record_id = _nonempty(str(info.replica_record_id), 'replica_record_id')
    payload: dict[str, Any]

    if kind == NonPoolLaunchProfileKind.ORDINARY_PAID:
        if paid_claim is None:
            raise OrdinaryLaunchBindingConflict(
                'Ordinary paid profile has no paid-capacity claim.')
        reference = f'paid-capacity:{service["hash"]}:{record_id}:{pool_key}'
        generation = 0
        payload = {'claim': paid_claim, 'placement': placement}
    elif kind == NonPoolLaunchProfileKind.ORDINARY_ZERO_COST:
        if paid_claim is not None:
            raise OrdinaryLaunchBindingConflict(
                'Ordinary zero-cost profile retained a paid claim.')
        sequence = _zero_cost_sequence_payload(connection, info)
        reference = f'zero-cost:{record_id}:{info.zero_cost_admission_sequence}'
        generation = info.zero_cost_admission_sequence
        payload = {'placement': placement, 'sequence': sequence}
    elif kind == NonPoolLaunchProfileKind.RESERVED_FILL:
        if paid_claim is not None:
            raise OrdinaryLaunchBindingConflict(
                'Reserved fill must never carry paid-capacity authority.')
        payload = _reserved_fill_payload(connection, service, info)
        reference = f'reserved-fill:{info.reserved_fill_intent_idempotency_key}'
        generation = info.reserved_fill_allocation_generation
    elif kind == NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT:
        payload, generation = _unknown_capacity_replacement_payload(
            connection, service, replica, info)
        payload['funding'] = _non_pool_funding_payload(connection, info,
                                                       paid_claim)
        predecessor = payload['authorization']['predecessor']
        reference = ('unknown-capacity:'
                     f'{predecessor["replica_record_id"]}:{generation}')
        if paid_claim is not None:
            pool_identity = paid_capacity.pool_key_payload(pool_key)
            if isinstance(pool_identity, Mapping):
                if pool_identity.get('cloud') == 'aws':
                    aws_account_id = ordinary_paid_aws_account_id_from_pool_key(
                        pool_key)
                    reference += f':aws-account:{aws_account_id}'
                elif pool_identity.get('cloud') == 'gcp':
                    project_id = ordinary_paid_gcp_project_id_from_pool_key(
                        pool_key)
                    reference += f':gcp-project:{project_id}'
    elif kind == NonPoolLaunchProfileKind.COST_REBALANCE:
        payload, generation = _cost_rebalance_payload(connection, service,
                                                      replica, info)
        payload['funding'] = _non_pool_funding_payload(connection, info,
                                                       paid_claim)
        predecessor = payload['authorization']['predecessor']
        reference = ('cost-rebalance:'
                     f'{predecessor["replica_record_id"]}:{generation}')
    else:
        intent = info.system_recovery_launch_intent
        if intent is None:
            raise OrdinaryLaunchBindingConflict(
                'System-OOM recovery profile lost its launch intent.')
        payload = {
            'funding': _non_pool_funding_payload(connection, info, paid_claim),
            'intent': intent.to_dict(),
            'placement': placement,
            'recovery_disposition': info.system_recovery_disposition.value,
        }
        reference = f'system-oom:{intent.launch_nonce}'
        generation = intent.launch_generation
    return (NonPoolLaunchProfile.create(kind,
                                        authorization_reference=reference,
                                        authorization_generation=generation,
                                        authorization_payload=payload),
            paid_fresh_until)


def resolve_non_pool_launch_profile_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    *,
    protocol_and_service_prelocked: bool = False,
) -> NonPoolLaunchProfile:
    """Recompute one profile from planner-owned durable authority."""
    profile, _ = _resolve_non_pool_launch_profile_in_connection(
        connection,
        service,
        replica,
        protocol_and_service_prelocked=protocol_and_service_prelocked)
    return profile


def resolve_non_pool_launch_profile(
    service_name: str,
    replica_id: int,
    replica_record_id: uuid.UUID | str,
) -> NonPoolLaunchProfile:
    """Read a proposed profile; admission revalidates it under row locks."""
    service_name = _nonempty(service_name, 'service_name')
    replica_id = _positive_int(replica_id, 'replica_id')
    record_id = _canonical_uuid(replica_record_id, 'replica_record_id')
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise OrdinaryLaunchBindingUnavailable(
            'Generic non-pool launch profiles require PostgreSQL.')
    with engine.begin() as connection:
        # Profile resolution may discover a paid claim.  Take the protocol
        # prefix before reading the service so allocation-bound validation
        # never has to invert the writer's canonical lock order.
        serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
            connection)
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                service_name)).mappings().one_or_none()
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                service_name, serve_state_schema.replicas_table.c.replica_id ==
                replica_id)).mappings().one_or_none()
        if (service is None or replica is None or
                _replica_record_id(replica) != str(record_id)):
            raise OrdinaryLaunchBindingConflict(
                'Generic profile target no longer names the exact replica.')
        return resolve_non_pool_launch_profile_in_connection(
            connection, service, replica, protocol_and_service_prelocked=True)


def get_existing_non_pool_launch_profile(
    association_id: uuid.UUID | str,) -> NonPoolLaunchProfile | None:
    """Return the immutable profile for an exact admission retry, if any.

    This is a read hint only.  The admission transaction locks and validates
    the complete association identity before returning it.  Looking up the
    stored profile before recomputing mutable planner authority lets a retry
    whose acknowledgement was lost submit the exact committed bytes even when
    the planner observation has since expired.
    """
    association_uuid = _canonical_uuid(association_id, 'association_id')
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise OrdinaryLaunchBindingUnavailable(
            'Generic non-pool launch profiles require PostgreSQL.')
    with engine.connect() as connection:
        association = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id ==
                association_uuid)).mappings().one_or_none()
    if association is None:
        return None
    profile = _association_profile(association)
    if profile is None:
        raise OrdinaryLaunchBindingConflict(
            'Exact admission retry names a protocol-v1 association.')
    return profile


def validate_non_pool_submission_execution_context_in_connection(
    connection: sqlalchemy.engine.Connection,
    identity: NonPoolBindingIdentity,
    launch_context: Mapping[str, Any],
) -> None:
    """Validate profile-owned request bytes under admission's row locks."""
    _require_postgres(connection)
    if not isinstance(identity, NonPoolBindingIdentity):
        raise ValueError('identity must be a NonPoolBindingIdentity.')
    has_recovery_context = system_oom_recovery.has_v3_system_oom_recovery_context(
        launch_context)
    if identity.profile.kind != NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY:
        if has_recovery_context:
            raise OrdinaryLaunchBindingConflict(
                'A non-recovery profile contains a system-OOM envelope.')
        return
    if not has_recovery_context:
        raise OrdinaryLaunchBindingConflict(
            'System-OOM profile has no recovery execution envelope.')

    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            identity.service_name)).mappings().one_or_none()
    replica = connection.execute(
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            identity.service_name,
            serve_state_schema.replicas_table.c.replica_id ==
            identity.replica_id)).mappings().one_or_none()
    if service is None or replica is None:
        raise OrdinaryLaunchBindingConflict(
            'System-OOM execution envelope lost its locked planner rows.')
    info = _locked_replica_info(replica)
    intent = info.system_recovery_launch_intent
    if intent is None:
        raise OrdinaryLaunchBindingConflict(
            'System-OOM execution envelope lost its recovery intent.')
    expected = system_oom_recovery.create_unbound_launch_context(
        intent,
        service_name=identity.service_name,
        service_version=identity.service_version,
        service_lifecycle_epoch=service['lifecycle_epoch'],
        controller_pid=service['controller_pid'],
        controller_ip=service['controller_ip'],
        controller_incarnation=service['controller_incarnation'],
        controller_owner_epoch=service['controller_owner_epoch'])
    try:
        submitted = system_oom_recovery.extract_unbound_launch_context(
            dict(launch_context))
    except (TypeError, ValueError) as error:
        raise OrdinaryLaunchBindingConflict(
            'System-OOM execution envelope is incomplete or malformed.'
        ) from error
    if (submitted != expected or identity.profile.authorization_reference
            != f'system-oom:{intent.launch_nonce}' or
            identity.profile.authorization_generation
            != intent.launch_generation):
        raise OrdinaryLaunchBindingConflict(
            'System-OOM execution envelope does not match the locked intent.')


def retire_pre_admission_non_pool_launch_intent(
    authority: ControllerBindingAuthority,
    replica_id: int,
    replica_record_id: uuid.UUID | str,
) -> PreAdmissionRetirement:
    """Atomically retire one generic planner intent with no admitted action.

    The replica-intent commit deliberately precedes association/request
    admission.  A controller can therefore crash with a durable planner row
    but no action identity.  Under an exact protocol-v2 service authority,
    absence of both the replica pointer and every unsettled association proves
    that no request -- and consequently no provider effect -- escaped for
    this record.  Delete the intent and its paid claim in the same transaction
    so the current planner can make a fresh decision.

    This is not provider cleanup and must never use provider discovery as
    evidence.  A concurrent admission serializes on the same locked replica
    row: if admission wins, ``ASSOCIATED`` tells the controller to adopt it;
    if retirement wins, the later admission fails before queue visibility.
    """
    if not isinstance(authority, ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    replica_id = _positive_int(replica_id, 'replica_id')
    record_id = _canonical_uuid(replica_record_id, 'replica_record_id')
    if not authority.retained_non_pool_settlement_allowed:
        raise OrdinaryLaunchBindingConflict(
            'Pre-admission retirement requires exact retained generic '
            'settlement authority.')

    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise OrdinaryLaunchBindingUnavailable(
            'Generic pre-admission retirement requires PostgreSQL.')

    # The zero-cost sequencer is a global lock and must not tax ordinary paid
    # retirement.  This is only a routing hint; the transaction below repeats
    # and exact-matches the immutable normalized edge before any deletion.
    with engine.connect() as discovery_connection:
        discovery = discovery_connection.execute(
            sqlalchemy.select(
                serve_state_schema.replicas_table.c.
                reserved_fill_intent_idempotency_key,
                serve_state_schema.replicas_table.c.replica_state).where(
                    serve_state_schema.replicas_table.c.service_name ==
                    authority.service_name,
                    serve_state_schema.replicas_table.c.replica_id ==
                    replica_id)).mappings().one_or_none()
    reserved_fill_hint = bool(
        discovery is not None and
        discovery['reserved_fill_intent_idempotency_key'] is not None and
        isinstance(discovery['replica_state'], Mapping) and
        discovery['replica_state'].get('replica_record_id') == str(record_id))

    with engine.begin() as connection:
        if reserved_fill_hint:
            # Canonical zero-cost order: protocol, lifecycle, service, intent,
            # replica, association, request, queue, pin, admission.
            serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
                connection)
        lifecycle = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table).where(
                    serve_state_schema.service_lifecycle_fences_table.c.name ==
                    authority.service_name).with_for_update()).mappings(
                    ).one_or_none()
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == authority.
                service_name).with_for_update()).mappings().one_or_none()
        if lifecycle is None or service is None:
            raise OrdinaryLaunchBindingConflict(
                'Pre-admission retirement lost service lifecycle authority.')
        current_capability = _non_pool_capability_from_service(service)
        expected_capability = (
            authority.non_pool_capable,
            authority.non_pool_binding_protocol_version,
            authority.non_pool_profile_set_digest,
            authority.non_pool_capability_cohort_epoch,
            authority.non_pool_receipt_protocol_version,
        )
        if (lifecycle['epoch'] != authority.service_lifecycle_epoch or
                service['hash'] != authority.service_hash or
                service['workspace'] != authority.service_workspace or
                service['lifecycle_epoch'] != authority.service_lifecycle_epoch
                or service['pool'] != 0 or
                service['controller_pid'] != authority.controller_pid or
                service['controller_ip'] != authority.controller_ip or
                service['controller_incarnation']
                != authority.controller_incarnation or
                service['controller_owner_epoch']
                != authority.controller_owner_epoch or
                service['ordinary_launch_binding_capable'] is not True or
                service['ordinary_launch_binding_mode']
                != authority.binding_mode.value or
                service['ordinary_launch_binding_epoch']
                != authority.binding_epoch or
                current_capability != expected_capability):
            raise OrdinaryLaunchBindingConflict(
                'Pre-admission retirement authority is no longer current.')

        # A non-locking read is stable behind the service owner lock and lets
        # an already-won association race return ASSOCIATED without inverting
        # the zero-cost intent-before-replica lock order.
        replica_discovery = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                authority.service_name,
                serve_state_schema.replicas_table.c.replica_id ==
                replica_id)).mappings().one_or_none()
        if replica_discovery is None:
            return PreAdmissionRetirement(
                PreAdmissionRetirementDisposition.ABSENT)
        discovered_info = _locked_replica_info(replica_discovery)
        if discovered_info.replica_record_id != str(record_id):
            raise OrdinaryLaunchBindingConflict(
                'Pre-admission retirement replica identity is stale.')

        if reserved_fill_hint:
            intent_key = replica_discovery[
                'reserved_fill_intent_idempotency_key']
            if (not isinstance(intent_key, str) or
                    classify_non_pool_launch_profile(discovered_info)
                    is not NonPoolLaunchProfileKind.RESERVED_FILL):
                raise OrdinaryLaunchBindingConflict(
                    'Pre-admission reserved fill lost its normalized intent '
                    'edge.')
            pointer = replica_discovery['ordinary_launch_association_id']
            if pointer is not None:
                replica = connection.execute(
                    sqlalchemy.select(serve_state_schema.replicas_table).where(
                        serve_state_schema.replicas_table.c.service_name ==
                        authority.service_name,
                        serve_state_schema.replicas_table.c.replica_id ==
                        replica_id).with_for_update()).mappings().one()
                associations = connection.execute(
                    sqlalchemy.select(ordinary_launch_associations_table).where(
                        ordinary_launch_associations_table.c.service_name ==
                        authority.service_name,
                        ordinary_launch_associations_table.c.replica_id ==
                        replica_id,
                        ordinary_launch_associations_table.c.replica_record_id
                        == record_id).order_by(
                            ordinary_launch_associations_table.c.
                            launch_generation).with_for_update()).mappings(
                            ).all()
                unsettled = [
                    row for row in associations if Resolution(
                        str(row['resolution'])) in UNSETTLED_RESOLUTIONS
                ]
                if (len(unsettled) == 1 and
                        replica['ordinary_launch_association_id']
                        == unsettled[0]['association_id']):
                    return PreAdmissionRetirement(
                        PreAdmissionRetirementDisposition.ASSOCIATED)
                raise OrdinaryLaunchBindingConflict(
                    'Replica association pointer and unsettled history '
                    'disagree.')
            repository = kueue_lane_lineage.KueueAdmissionRepository(engine)
            target = kueue_lane_lineage.MaterializedAdmissionRetirementTarget(
                replica_id=replica_id, replica_record_id=record_id)
            try:
                proof = (
                    repository.prelock_pre_admission_retirement_in_connection(
                        connection,
                        service_name=authority.service_name,
                        service_hash=authority.service_hash,
                        service_lifecycle_epoch=(
                            authority.service_lifecycle_epoch),
                        intent_idempotency_key=intent_key,
                        target=target))
                proofs = (proof,)
                repository.delete_materialized_admissions_in_connection(
                    connection, proofs)
                deleted = connection.execute(
                    sqlalchemy.delete(serve_state_schema.replicas_table).where(
                        serve_state_schema.replicas_table.c.service_name ==
                        authority.service_name,
                        serve_state_schema.replicas_table.c.replica_id ==
                        replica_id,
                        serve_state_schema.replicas_table.c.
                        ordinary_launch_association_id.is_(None),
                        serve_state_schema.replicas_table.c.
                        reserved_fill_intent_idempotency_key == intent_key))
                if deleted.rowcount != 1:
                    raise OrdinaryLaunchBindingConflict(
                        'Pre-admission retirement lost the replica delete '
                        'CAS.')
                (repository.
                 finalize_materialized_admission_retirements_in_connection(
                     connection, proofs))
            except kueue_lane_lineage.KueueAdmissionConflict as error:
                raise OrdinaryLaunchBindingConflict(
                    'Reserved-fill pre-admission retirement is not '
                    'provider-free and exact.') from error
            return PreAdmissionRetirement(
                PreAdmissionRetirementDisposition.RETIRED,
                NonPoolLaunchProfileKind.RESERVED_FILL)

        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                authority.service_name,
                serve_state_schema.replicas_table.c.replica_id ==
                replica_id).with_for_update()).mappings().one_or_none()
        if replica is None:
            return PreAdmissionRetirement(
                PreAdmissionRetirementDisposition.ABSENT)
        info = _locked_replica_info(replica)
        if (info.replica_id != replica_id or
                info.replica_record_id != str(record_id) or
                info.version != replica['version'] or
                info.cluster_name != replica['cluster_name'] or
                info.status.value != replica['status'] or
                info.status.value not in ('PENDING', 'PROVISIONING')):
            raise OrdinaryLaunchBindingConflict(
                'Pre-admission retirement replica identity is stale or '
                'not launch-pending.')

        associations = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.service_name ==
                authority.service_name,
                ordinary_launch_associations_table.c.replica_id == replica_id,
                ordinary_launch_associations_table.c.replica_record_id ==
                record_id).order_by(
                    ordinary_launch_associations_table.c.launch_generation).
            with_for_update()).mappings().all()
        unsettled = [
            row for row in associations
            if Resolution(str(row['resolution'])) in UNSETTLED_RESOLUTIONS
        ]
        pointer = replica['ordinary_launch_association_id']
        if unsettled:
            if (len(unsettled) == 1 and
                    pointer == unsettled[0]['association_id']):
                return PreAdmissionRetirement(
                    PreAdmissionRetirementDisposition.ASSOCIATED)
            raise OrdinaryLaunchBindingConflict(
                'Replica association pointer and unsettled history disagree.')
        if pointer is not None:
            raise OrdinaryLaunchBindingConflict(
                'Pointerless retirement found a settled or missing '
                'association pointer.')

        profile_kind = classify_non_pool_launch_profile(info)
        if profile_kind is None:
            raise OrdinaryLaunchBindingConflict(
                'Pre-admission retirement found an incomplete generic '
                'planner profile.')
        if (profile_kind is NonPoolLaunchProfileKind.RESERVED_FILL or
                replica['reserved_fill_intent_idempotency_key'] is not None):
            raise OrdinaryLaunchBindingConflict(
                'Reserved fill retirement requires its zero-cost protocol '
                'prefix and committed-intent proof.')
        if associations:
            if len(associations) != 1:
                raise OrdinaryLaunchBindingConflict(
                    'Generic planner intent has unexpected action history.')
            predecessor = associations[0]
            if (predecessor['resolution']
                    != Resolution.PRE_EFFECT_TERMINAL.value or
                    predecessor.get('binding_protocol_version')
                    != NON_POOL_BINDING_PROTOCOL_VERSION or
                    predecessor.get('profile_kind') != profile_kind.value or
                    predecessor.get('cancel_reason') is not None):
                raise OrdinaryLaunchBindingConflict(
                    'Only an exact non-cancelled pre-effect action may retire '
                    'a generic planner intent.')
        connection.execute(
            sqlalchemy.delete(
                serve_state_schema.paid_capacity_claims_table).where(
                    serve_state_schema.paid_capacity_claims_table.c.service_name
                    == authority.service_name,
                    serve_state_schema.paid_capacity_claims_table.c.service_hash
                    == authority.service_hash,
                    serve_state_schema.paid_capacity_claims_table.c.replica_id
                    == replica_id))
        deleted = connection.execute(
            sqlalchemy.delete(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                authority.service_name,
                serve_state_schema.replicas_table.c.replica_id == replica_id,
                serve_state_schema.replicas_table.c.
                ordinary_launch_association_id.is_(None),
                serve_state_schema.replicas_table.c.
                reserved_fill_intent_idempotency_key.is_(None)))
        if deleted.rowcount != 1:
            raise OrdinaryLaunchBindingConflict(
                'Pre-admission retirement lost the replica delete CAS.')
        return PreAdmissionRetirement(PreAdmissionRetirementDisposition.RETIRED,
                                      profile_kind)


def _identity_values(
        identity: BindingIdentity,
        launch_generation: int,
        *,
        paid_capacity_pool_key: str | None = None) -> dict[str, Any]:
    non_pool_identity = (identity if isinstance(
        identity, NonPoolBindingIdentity) else None)
    profile = (non_pool_identity.profile
               if non_pool_identity is not None else None)
    return {
        'association_id': identity.association_id,
        'submission_id': identity.submission_id,
        'tenant_scope': identity.tenant_scope,
        'service_name': identity.service_name,
        'service_hash': identity.service_hash,
        'service_workspace': identity.service_workspace,
        'service_lifecycle_epoch': identity.service_lifecycle_epoch,
        'service_binding_epoch': identity.service_binding_epoch,
        'service_version': identity.service_version,
        'replica_id': identity.replica_id,
        'replica_record_id': identity.replica_record_id,
        'paid_capacity_pool_key': paid_capacity_pool_key,
        'launch_generation': launch_generation,
        'cluster_name': identity.cluster_name,
        'request_id': identity.request_id,
        'input_digest': identity.input_digest,
        'digest_version': identity.digest_version,
        'binding_protocol_version':
            (NON_POOL_BINDING_PROTOCOL_VERSION if profile is not None else None
            ),
        'profile_kind': profile.kind.value if profile is not None else None,
        'profile_version': profile.version if profile is not None else None,
        'profile_digest': profile.digest if profile is not None else None,
        'capability_cohort_epoch': (non_pool_identity.capability_cohort_epoch
                                    if non_pool_identity is not None else None),
        'capability_profile_set_digest':
            (non_pool_identity.capability_profile_set_digest
             if non_pool_identity is not None else None),
        'receipt_protocol_version':
            (non_pool_identity.receipt_protocol_version
             if non_pool_identity is not None else None),
        'authorization_kind':
            (profile.authorization_kind.value if profile is not None else None),
        'authorization_reference':
            (profile.authorization_reference if profile is not None else None),
        'authorization_generation':
            (profile.authorization_generation if profile is not None else None),
        'authorization_digest':
            (profile.authorization_digest if profile is not None else None),
    }


def _existing_identity_matches(row: Mapping[str, Any],
                               identity: BindingIdentity) -> bool:
    immutable = _identity_values(
        identity,
        int(row['launch_generation']),
        paid_capacity_pool_key=row['paid_capacity_pool_key'])
    return all(row[key] == value for key, value in immutable.items())


def _association_profile(
        association: Mapping[str, Any]) -> NonPoolLaunchProfile | None:
    """Decode an immutable association profile without accepting partials."""
    if association.get('profile_kind') is None:
        generic_fields = (
            association.get('binding_protocol_version'),
            association.get('profile_version'),
            association.get('profile_digest'),
            association.get('capability_cohort_epoch'),
            association.get('capability_profile_set_digest'),
            association.get('receipt_protocol_version'),
            association.get('authorization_kind'),
            association.get('authorization_reference'),
            association.get('authorization_generation'),
            association.get('authorization_digest'),
        )
        if any(value is not None for value in generic_fields):
            raise OrdinaryLaunchBindingConflict(
                'Association retained a partial generic profile envelope.')
        return None
    try:
        profile = NonPoolLaunchProfile(
            kind=NonPoolLaunchProfileKind(str(association['profile_kind'])),
            version=int(association['profile_version']),
            authorization_kind=NonPoolLaunchAuthorizationKind(
                str(association['authorization_kind'])),
            authorization_reference=str(association['authorization_reference']),
            authorization_generation=int(
                association['authorization_generation']),
            authorization_digest=str(association['authorization_digest']),
            digest=str(association['profile_digest']))
        profile.validate()
    except (KeyError, TypeError, ValueError) as error:
        raise OrdinaryLaunchBindingConflict(
            'Association generic profile envelope is malformed.') from error
    return profile


def bound_context_from_association(
        association: Mapping[str, Any]) -> BoundLaunchContext:
    """Build an exact typed execution context from durable association data."""
    association_id = _canonical_uuid(association.get('association_id'),
                                     'association_id')
    request_id = _nonempty(association.get('request_id'), 'request_id')
    service_name = _nonempty(association.get('service_name'), 'service_name')
    replica_id = _positive_int(association.get('replica_id'), 'replica_id')
    replica_record_id = _canonical_uuid(association.get('replica_record_id'),
                                        'replica_record_id')
    launch_generation = _positive_int(association.get('launch_generation'),
                                      'launch_generation')
    input_digest = _nonempty(association.get('input_digest'), 'input_digest')
    profile = _association_profile(association)
    if profile is None:
        return BoundLaunchContext(association_id=association_id,
                                  request_id=request_id,
                                  service_name=service_name,
                                  replica_id=replica_id,
                                  replica_record_id=replica_record_id,
                                  launch_generation=launch_generation,
                                  input_digest=input_digest)
    return BoundNonPoolLaunchContext(
        association_id=association_id,
        request_id=request_id,
        service_name=service_name,
        replica_id=replica_id,
        replica_record_id=replica_record_id,
        launch_generation=launch_generation,
        input_digest=input_digest,
        profile=profile,
        capability_cohort_epoch=_positive_int(
            association.get('capability_cohort_epoch'),
            'capability_cohort_epoch'),
        capability_profile_set_digest=_nonempty(
            association.get('capability_profile_set_digest'),
            'capability_profile_set_digest'),
        receipt_protocol_version=_positive_int(
            association.get('receipt_protocol_version'),
            'receipt_protocol_version'))


def _validate_generic_capability(
    service: Mapping[str, Any],
    *,
    capability_cohort_epoch: int | None,
    capability_profile_set_digest: str | None,
    receipt_protocol_version: int | None,
) -> None:
    """Require one exact per-service protocol-v2 capable cohort."""
    actual = _non_pool_capability_from_service(service)
    if (actual != (True, NON_POOL_BINDING_PROTOCOL_VERSION,
                   capability_profile_set_digest, capability_cohort_epoch,
                   receipt_protocol_version) or capability_profile_set_digest
            != supported_non_pool_profile_set_digest() or
            capability_cohort_epoch != NON_POOL_CAPABILITY_COHORT_EPOCH or
            receipt_protocol_version != NON_POOL_RECEIPT_PROTOCOL_VERSION):
        raise OrdinaryLaunchBindingConflict(
            'Service is not owned by the exact generic launch cohort.')


def _validate_retained_generic_cleanup_capability(
    service: Mapping[str, Any],
    *,
    capability_cohort_epoch: int | None,
    capability_profile_set_digest: str | None,
    receipt_protocol_version: int | None,
) -> None:
    """Accept the exact current or adjacent cohort for cleanup only."""
    actual = _non_pool_capability_from_service(service)
    if (actual != (True, NON_POOL_BINDING_PROTOCOL_VERSION,
                   capability_profile_set_digest, capability_cohort_epoch,
                   receipt_protocol_version) or capability_profile_set_digest
            != supported_non_pool_profile_set_digest() or
            type(capability_cohort_epoch) is not int or capability_cohort_epoch
            not in (NON_POOL_CAPABILITY_COHORT_EPOCH,
                    NON_POOL_CAPABILITY_COHORT_EPOCH - 1) or
            receipt_protocol_version != NON_POOL_RECEIPT_PROTOCOL_VERSION):
        raise OrdinaryLaunchBindingConflict(
            'Service is not owned by a retained generic cleanup cohort.')


def _validate_terminal_generic_cleanup_capability(
    service: Mapping[str, Any],
    *,
    capability_cohort_epoch: int | None,
    capability_profile_set_digest: str | None,
    receipt_protocol_version: int | None,
) -> None:
    """Accept N/N-1/N-2 only after complete terminal cleanup proof.

    This predicate must remain downstream of frozen-profile, zero-paid,
    terminal, execution-quiescence, projected-pin-release, and canonical
    provider-absence validation.  It must never guard admission, adoption,
    provider reconciliation/evidence writes, or provider-effect start.
    """
    actual = _non_pool_capability_from_service(service)
    if (actual != (True, NON_POOL_BINDING_PROTOCOL_VERSION,
                   capability_profile_set_digest, capability_cohort_epoch,
                   receipt_protocol_version) or capability_profile_set_digest
            != supported_non_pool_profile_set_digest() or
            type(capability_cohort_epoch) is not int or capability_cohort_epoch
            not in (NON_POOL_CAPABILITY_COHORT_EPOCH,
                    NON_POOL_CAPABILITY_COHORT_EPOCH - 1,
                    NON_POOL_CAPABILITY_COHORT_EPOCH - 2) or
            receipt_protocol_version != NON_POOL_RECEIPT_PROTOCOL_VERSION):
        raise OrdinaryLaunchBindingConflict(
            'Service is not owned by a terminal generic cleanup cohort.')


def _validate_profile_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    expected: NonPoolLaunchProfile,
    *,
    protocol_and_service_prelocked: bool = False,
    validate_paid_provider_start: bool = True,
) -> datetime.datetime | None:
    """Recompute and exact-match planner authority at a commit boundary."""
    actual, paid_fresh_until = _resolve_non_pool_launch_profile_in_connection(
        connection,
        service,
        replica,
        protocol_and_service_prelocked=protocol_and_service_prelocked,
        validate_paid_provider_start=validate_paid_provider_start)
    if actual != expected:
        raise OrdinaryLaunchBindingConflict(
            'Non-pool planner authorization changed before provider effect.')
    return paid_fresh_until


def _validate_committed_reserved_fill_profile_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    expected: NonPoolLaunchProfile,
) -> None:
    """Validate the frozen Serve056 handoff without mutable planner reads."""
    if (expected.kind is not NonPoolLaunchProfileKind.RESERVED_FILL or
            expected.authorization_kind
            is not NonPoolLaunchAuthorizationKind.RESERVED_FILL_ALLOCATION or
            service.get('reserved_fill_actuation_mode')
            != zero_cost_actuation.ActuationMode.DURABLE_INTENT.value):
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill provider effect has no durable intent profile.')
    info = _locked_replica_info(replica)
    scalar_key = replica.get('reserved_fill_intent_idempotency_key')
    if (not isinstance(scalar_key, str) or
            expected.authorization_reference != f'reserved-fill:{scalar_key}' or
            expected.authorization_generation
            != info.reserved_fill_allocation_generation or
            info.reserved_fill_intent_idempotency_key != scalar_key):
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill profile lost its normalized committed-intent '
            'edge.')
    intent = zero_cost_actuation.committed_intent_for_replica_in_connection(
        connection,
        service_name=str(service['name']),
        service_hash=str(service['hash']),
        replica_info=info)
    if (intent is None or intent.idempotency_key != scalar_key or
            expected.authorization_generation != intent.allocation_generation):
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill provider effect lost its immutable committed '
            'handoff.')


def _validate_reserved_fill_cleanup_profile_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    expected: NonPoolLaunchProfile,
) -> None:
    """Match cleanup to the immutable admission authority, not today's plan."""
    info = _locked_replica_info(replica)
    kind = classify_non_pool_launch_profile(info)
    _, paid_claim = _paid_claim_payload(connection, service, replica, info)
    if (service.get('reserved_fill_actuation_mode')
            != zero_cost_actuation.ActuationMode.DURABLE_INTENT.value or
            kind is not NonPoolLaunchProfileKind.RESERVED_FILL or
            paid_claim is not None):
        raise OrdinaryLaunchBindingConflict(
            'Provider-present cleanup lost its durable reserved zero-cost '
            'profile.')
    payload = _reserved_fill_cleanup_payload(connection, service, info)
    actual = NonPoolLaunchProfile.create(
        NonPoolLaunchProfileKind.RESERVED_FILL,
        authorization_reference=(
            f'reserved-fill:{info.reserved_fill_intent_idempotency_key}'),
        authorization_generation=info.reserved_fill_allocation_generation,
        authorization_payload=payload)
    if actual != expected:
        raise OrdinaryLaunchBindingConflict(
            'Provider-present cleanup no longer matches its frozen admission '
            'authority.')


def validate_reserved_fill_cleanup_association_in_connection(
    connection: sqlalchemy.engine.Connection,
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    association: Mapping[str, Any],
) -> NonPoolLaunchProfile:
    """Authenticate one frozen reserved-fill association for cleanup only.

    This grants no provider or admission authority.  It is the public cleanup
    boundary for consumers that must prove a retained association's complete
    protocol, capability, profile, authorization, and committed-intent tuple
    after its request row may have been garbage-collected.
    """
    _require_postgres(connection)
    if not all(
            isinstance(row, Mapping)
            for row in (service, replica, association)):
        raise TypeError('Cleanup association authority requires mapped rows.')
    try:
        context = bound_context_from_association(association)
    except (OrdinaryLaunchBindingConflict, TypeError, ValueError) as error:
        raise OrdinaryLaunchBindingConflict(
            'Reserved-fill cleanup association is malformed.') from error
    if (not isinstance(context, BoundNonPoolLaunchContext) or
            association.get('binding_protocol_version')
            != NON_POOL_BINDING_PROTOCOL_VERSION or
            context.profile.kind is not NonPoolLaunchProfileKind.RESERVED_FILL):
        raise OrdinaryLaunchBindingConflict(
            'Cleanup association is not an exact reserved-fill profile.')
    try:
        _validate_retained_generic_cleanup_capability(
            service,
            capability_cohort_epoch=context.capability_cohort_epoch,
            capability_profile_set_digest=(
                context.capability_profile_set_digest),
            receipt_protocol_version=context.receipt_protocol_version)
    except OrdinaryLaunchBindingConflict as error:
        # N-2 is deliberately unavailable to the broad settlement paths.  It
        # may pass this cleanup boundary only when the already-projected
        # history independently proves terminal quiescence, released pins,
        # zero paid authority, and canonical post-quiescence ABSENT evidence.
        if (context.capability_cohort_epoch
                != NON_POOL_CAPABILITY_COHORT_EPOCH - 2 or
                service.get('non_pool_launch_capability_cohort_epoch')
                != context.capability_cohort_epoch):
            raise
        canonical_association, _ = (
            projected_provider_absence_retirement_authority_in_connection(
                connection, context.service_name, context.replica_id,
                str(context.replica_record_id)))
        if dict(canonical_association) != dict(association):
            raise OrdinaryLaunchBindingConflict(
                'Terminal N-2 cleanup does not match its locked association.'
            ) from error
        return context.profile
    _validate_reserved_fill_cleanup_profile_in_connection(
        connection, service, replica, context.profile)
    return context.profile


def _validate_profile_execution_context(
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    context: BoundNonPoolLaunchContext,
    launch_context: Mapping[str, Any],
) -> None:
    """Exact-match profile-owned execution bytes to locked planner state."""
    has_recovery_context = system_oom_recovery.has_v3_system_oom_recovery_context(
        launch_context)
    if context.profile.kind != NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY:
        if has_recovery_context:
            raise OrdinaryLaunchBindingConflict(
                'A non-recovery action contains a system-OOM envelope.')
        return
    if not has_recovery_context:
        raise OrdinaryLaunchBindingConflict(
            'System-OOM action lost its bound recovery envelope.')
    info = _locked_replica_info(replica)
    intent = info.system_recovery_launch_intent
    if intent is None:
        raise OrdinaryLaunchBindingConflict(
            'System-OOM action lost its locked recovery intent.')
    expected_unbound = system_oom_recovery.create_unbound_launch_context(
        intent,
        service_name=str(service['name']),
        service_version=info.version,
        service_lifecycle_epoch=service['lifecycle_epoch'],
        controller_pid=LEGACY_FAIL_CLOSED_CONTROLLER_PID,
        controller_ip=LEGACY_FAIL_CLOSED_CONTROLLER_IP,
        controller_incarnation=service['controller_incarnation'],
        controller_owner_epoch=service['controller_owner_epoch'])
    expected = system_oom_recovery.bind_launch_context(expected_unbound,
                                                       context.request_id)
    try:
        actual = system_oom_recovery.extract_bound_launch_context(
            dict(launch_context))
    except (TypeError, ValueError) as error:
        raise OrdinaryLaunchBindingConflict(
            'System-OOM bound recovery envelope is malformed.') from error
    if (actual != expected or context.profile.authorization_reference
            != f'system-oom:{intent.launch_nonce}' or
            context.profile.authorization_generation
            != intent.launch_generation):
        raise OrdinaryLaunchBindingConflict(
            'System-OOM bound recovery envelope no longer matches its intent.')


def _active_paid_capacity_pool_key(
    connection: sqlalchemy.engine.Connection,
    identity: BindingIdentity,
    replica: Mapping[str, Any],
) -> str | None:
    """Read an exact claim while the service and replica are locked."""
    rows = connection.execute(
        sqlalchemy.select(
            serve_state_schema.paid_capacity_claims_table.c.service_hash,
            serve_state_schema.paid_capacity_claims_table.c.pool_key).where(
                serve_state_schema.paid_capacity_claims_table.c.service_name ==
                identity.service_name,
                serve_state_schema.paid_capacity_claims_table.c.replica_id ==
                identity.replica_id)).all()
    if not rows:
        if replica['paid_capacity_pool_key'] is not None:
            raise OrdinaryLaunchBindingConflict(
                'A paid bound replica has no exact capacity claim.')
        return None
    if len(rows) != 1:
        raise OrdinaryLaunchBindingConflict(
            'Replica has multiple paid-capacity claim incarnations.')
    service_hash, pool_key = rows[0]
    if (service_hash != identity.service_hash or
            replica['paid_capacity_pool_key'] != pool_key or
            not isinstance(pool_key, str) or not pool_key):
        raise OrdinaryLaunchBindingConflict(
            'Paid-capacity claim does not match the bound replica identity.')
    return pool_key


def _unsettled_paid_capacity_claim_matches(
    connection: sqlalchemy.engine.Connection,
    association: Mapping[str, Any],
) -> bool:
    """Validate the immutable association-to-claim edge under service lock."""
    rows = connection.execute(
        sqlalchemy.select(
            serve_state_schema.paid_capacity_claims_table.c.service_hash,
            serve_state_schema.paid_capacity_claims_table.c.pool_key).where(
                serve_state_schema.paid_capacity_claims_table.c.service_name ==
                association['service_name'],
                serve_state_schema.paid_capacity_claims_table.c.replica_id ==
                association['replica_id'])).all()
    expected = association['paid_capacity_pool_key']
    if expected is None:
        return not rows
    return (len(rows) == 1 and rows[0][0] == association['service_hash'] and
            rows[0][1] == expected)


def _lock_admission_rows(
    connection: sqlalchemy.engine.Connection,
    identity: BindingIdentity,
) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any],
           Mapping[str, Any] | None]:
    lifecycle = connection.execute(
        sqlalchemy.select(serve_state_schema.service_lifecycle_fences_table).
        where(
            serve_state_schema.service_lifecycle_fences_table.c.name ==
            identity.service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            identity.service_name).with_for_update()).mappings().one_or_none()
    replica = connection.execute(
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            identity.service_name,
            serve_state_schema.replicas_table.c.replica_id ==
            identity.replica_id).with_for_update()).mappings().one_or_none()
    if lifecycle is None or service is None or replica is None:
        raise OrdinaryLaunchBindingConflict(
            'Lifecycle, service, or replica disappeared before binding.')
    pointer = replica['ordinary_launch_association_id']
    current = None
    if pointer is not None:
        current = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id ==
                pointer).with_for_update()).mappings().one_or_none()
        if current is None:
            raise OrdinaryLaunchBindingConflict(
                'Replica points to a missing ordinary-launch association.')
    return lifecycle, service, replica, current


def _validate_admission_target(connection: sqlalchemy.engine.Connection,
                               lifecycle: Mapping[str,
                                                  Any], service: Mapping[str,
                                                                         Any],
                               replica: Mapping[str,
                                                Any], identity: BindingIdentity,
                               *, validate_profile_authority: bool,
                               protocol_and_service_prelocked: bool) -> None:
    derived = derive_binding_ids(identity.tenant_scope,
                                 identity.service_workspace,
                                 identity.submission_id)
    if derived != (identity.association_id, identity.request_id):
        raise OrdinaryLaunchBindingConflict(
            'Association/request IDs are not server-derived identity.')
    if (lifecycle['epoch'] != identity.service_lifecycle_epoch or
            service['lifecycle_epoch'] != identity.service_lifecycle_epoch):
        raise OrdinaryLaunchBindingConflict('Service lifecycle epoch changed.')
    workspace = service['workspace']
    if (service['hash'] != identity.service_hash or
            workspace != identity.service_workspace or service['pool'] != 0 or
            service['ordinary_launch_binding_mode'] != 'bound' or
            service['ordinary_launch_binding_epoch']
            != identity.service_binding_epoch or
            service['ordinary_launch_binding_capable'] is not True or
            service['controller_incarnation'] != identity.controller_incarnation
            or service['controller_owner_epoch']
            != identity.controller_owner_epoch):
        raise OrdinaryLaunchBindingConflict(
            'Service identity, workspace, binding mode, or owner changed.')
    try:
        service_status = serve_statuses.ServiceStatus[str(service['status'])]
    except (KeyError, TypeError) as error:
        raise OrdinaryLaunchBindingConflict(
            'Service status is malformed.') from error
    if service_status in (
            serve_statuses.ServiceStatus.replica_launch_blocking_statuses()):
        raise OrdinaryLaunchBindingConflict(
            'Service no longer authorizes replica launch.')
    identity_snapshot = {
        'replica_id': identity.replica_id,
        'replica_record_id': identity.replica_record_id,
        'service_version': identity.service_version,
        'cluster_name': identity.cluster_name,
        'paid_capacity_pool_key': replica.get('paid_capacity_pool_key'),
        'profile_kind': (identity.profile.kind.value if isinstance(
            identity, NonPoolBindingIdentity) else None),
    }
    if (not _replica_snapshot_matches_association(
            replica, identity_snapshot, require_launch_authorized=True) or
            _elected_recovery_version_in_connection(
                connection, identity.service_name) != identity.service_version):
        raise OrdinaryLaunchBindingConflict(
            'Replica identity, version, state, or cluster changed.')
    if isinstance(identity, NonPoolBindingIdentity):
        _validate_generic_capability(
            service,
            capability_cohort_epoch=identity.capability_cohort_epoch,
            capability_profile_set_digest=(
                identity.capability_profile_set_digest),
            receipt_protocol_version=identity.receipt_protocol_version)
        if validate_profile_authority:
            _require_current_non_pool_worker_projection_in_connection(
                connection, identity.service_name, identity.service_version)
            _validate_profile_authority_in_connection(
                connection,
                service,
                replica,
                identity.profile,
                protocol_and_service_prelocked=(protocol_and_service_prelocked))


def _require_current_non_pool_worker_projection_in_connection(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
    service_version: int,
) -> None:
    """Fence projected non-pool effects to the exact current protocol."""
    versions = serve_state_schema.version_specs_table
    row = connection.execute(
        sqlalchemy.select(versions.c.worker_placement_projections).where(
            versions.c.service_name == service_name,
            versions.c.version == service_version,
            versions.c.yaml_content.isnot(None),
            versions.c.quarantined_at.is_(None),
            versions.c.retired_at.is_(None),
        ).with_for_update(read=True)).mappings().one_or_none()
    if row is None:
        raise OrdinaryLaunchBindingConflict(
            'Non-pool service version is not active and immutable.')
    try:
        kubernetes_identity.validate_worker_placement_projections(
            row['worker_placement_projections'],
            allow_none=True,
            require_protocol_version=(
                kubernetes_identity.PLACEMENT_PROJECTION_PROTOCOL_VERSION))
    except ValueError as error:
        raise OrdinaryLaunchBindingConflict(
            'Projected non-pool admission and provider effects require the '
            'exact current worker projection protocol.') from error


def _admission_from_row(row: Mapping[str, Any],
                        disposition: AdmissionDisposition) -> BindingAdmission:
    return BindingAdmission(
        disposition=disposition,
        association_id=str(row['association_id']),
        request_id=str(row['request_id']),
        launch_generation=int(row['launch_generation']),
        owner_revision=int(row['owner_revision']),
        resolution=Resolution(str(row['resolution'])),
        effect_phase=EffectPhase(str(row['effect_phase'])),
    )


def prepare_fresh_ordinary_paid_batch_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service: Mapping[str, Any],
    authority: ControllerBindingAuthority,
    tenant_scope: str,
    targets: Sequence[FreshOrdinaryPaidTarget],
) -> PreparedFreshOrdinaryPaidBatch:
    """Lock and validate one fresh paid wave with a bounded query set.

    The caller must already own the protocol, lifecycle, and service locks in
    the capacity-admission order.  This is the read half of one
    transaction-local protocol: it validates every member row, claim, history,
    and authority before returning immutable material to the request builder.
    The matching commit function accepts the proof only on this exact
    connection and transaction.  No database statement may run between the
    returned proof and that commit; the production caller performs only pure
    request-body construction in that interval.
    """
    _require_postgres(connection)
    transaction = connection.get_transaction()
    if transaction is None:
        raise OrdinaryLaunchBindingUnavailable(
            'Fresh paid batch preparation requires an active transaction.')
    if (not isinstance(service, Mapping) or
            not isinstance(authority, ControllerBindingAuthority) or
            not authority.generic_launches_required):
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid batch authority is malformed or stale.')
    tenant_scope = _nonempty(tenant_scope, 'tenant_scope')
    targets = tuple(targets)
    if (not targets or len(targets)
            > paid_capacity.MAX_ATOMIC_PAID_ADMISSION_WAVE_MEMBERS or not all(
                isinstance(target, FreshOrdinaryPaidTarget)
                for target in targets)):
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid batch must be nonempty, bounded, and typed.')
    if (len({target.replica_id for target in targets}) != len(targets) or len(
        {target.replica_record_id for target in targets}) != len(targets)):
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid batch member identities must be unique.')
    for target in targets:
        _positive_int(target.replica_id, 'replica_id')
        _canonical_uuid(target.replica_record_id, 'replica_record_id')
        _positive_int(target.service_version, 'service_version')
        _nonempty(target.cluster_name, 'cluster_name')

    if (service.get('name') != authority.service_name or
            service.get('hash') != authority.service_hash or
            service.get('workspace') != authority.service_workspace or
            service.get('lifecycle_epoch') != authority.service_lifecycle_epoch
            or service.get('ordinary_launch_binding_mode')
            != BindingMode.BOUND.value or
            service.get('ordinary_launch_binding_epoch')
            != authority.binding_epoch or
            service.get('ordinary_launch_binding_capable') is not True or
            service.get('controller_incarnation')
            != authority.controller_incarnation or
            service.get('controller_owner_epoch')
            != authority.controller_owner_epoch or service.get('pool') != 0):
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid batch service authority is stale.')
    try:
        service_status = serve_statuses.ServiceStatus[str(service['status'])]
    except (KeyError, TypeError) as error:
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid batch service status is malformed.') from error
    if service_status in (
            serve_statuses.ServiceStatus.replica_launch_blocking_statuses()):
        raise OrdinaryLaunchBindingConflict(
            'Service no longer authorizes fresh paid launches.')
    service_version = targets[0].service_version
    if (any(target.service_version != service_version for target in targets) or
            service.get('current_version') != service_version or
            _elected_recovery_version_in_connection(
                connection, authority.service_name) != service_version):
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid batch does not target the elected service version.')
    _validate_generic_capability(
        service,
        capability_cohort_epoch=authority.non_pool_capability_cohort_epoch,
        capability_profile_set_digest=authority.non_pool_profile_set_digest,
        receipt_protocol_version=authority.non_pool_receipt_protocol_version)
    _require_current_non_pool_worker_projection_in_connection(
        connection, authority.service_name, service_version)

    replica_ids = tuple(sorted(target.replica_id for target in targets))
    replica_rows = connection.execute(
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            authority.service_name,
            serve_state_schema.replicas_table.c.replica_id.in_(
                replica_ids)).order_by(
                    serve_state_schema.replicas_table.c.replica_id).
        with_for_update()).mappings().all()
    replicas_by_id = {int(row['replica_id']): row for row in replica_rows}
    if len(replicas_by_id) != len(targets):
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid batch lost one or more replica rows.')

    claim_rows = connection.execute(
        sqlalchemy.select(serve_state_schema.paid_capacity_claims_table).where(
            serve_state_schema.paid_capacity_claims_table.c.service_name ==
            authority.service_name,
            serve_state_schema.paid_capacity_claims_table.c.replica_id.in_(
                replica_ids)).order_by(
                    serve_state_schema.paid_capacity_claims_table.c.replica_id).
        with_for_update()).mappings().all()
    claims_by_id: dict[int, list[Mapping[str, Any]]] = {}
    for row in claim_rows:
        claims_by_id.setdefault(int(row['replica_id']), []).append(row)

    infos: dict[int, Any] = {}
    claim_payloads: dict[int, tuple[str, dict[str, Any]]] = {}
    validation_by_card: dict[str, list[tuple[Mapping[str, Any], int, str]]] = {}
    plan_authority: tuple[Any, ...] | None = None
    for target in targets:
        replica = replicas_by_id[target.replica_id]
        if replica['ordinary_launch_association_id'] is not None:
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid batch replica already has a launch association.')
        if any(
                replica.get(column) is not None
                for column in _FRESH_PAID_RESOURCE_ACTION_COLUMNS):
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid batch replica already has a resource-action '
                'identity.')
        info = _locked_replica_info(replica)
        snapshot = {
            'replica_id': target.replica_id,
            'replica_record_id': target.replica_record_id,
            'service_version': target.service_version,
            'cluster_name': target.cluster_name,
            'paid_capacity_pool_key': replica.get('paid_capacity_pool_key'),
            'profile_kind': NonPoolLaunchProfileKind.ORDINARY_PAID.value,
        }
        if not _replica_snapshot_matches_association(
                replica, snapshot, require_launch_authorized=True):
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid batch replica identity or state changed.')
        pool_key, claim_payload = _paid_claim_payload_from_rows(
            service, replica, info, claims_by_id.get(target.replica_id, ()))
        if pool_key is None or claim_payload is None:
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid batch member has no exact paid claim.')
        claim = claims_by_id[target.replica_id][0]
        card = claim.get('capacity_plan_accelerator')
        units = claim.get('capacity_plan_units')
        if (not isinstance(card, str) or not card or isinstance(units, bool) or
                not isinstance(units, int) or units < 1):
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid batch claim has a malformed plan debit.')
        validation_claim = dict(claim)
        validation_claim['paid_capacity_pool_key'] = pool_key
        member_plan_authority = tuple(
            claim.get(field) for field in (
                'capacity_plan_generation',
                'capacity_plan_sha256',
                'demand_feed_generation',
                'demand_source_epoch',
            ))
        if any(value is None for value in member_plan_authority):
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid batch has no complete capacity-plan authority.')
        if plan_authority is None:
            plan_authority = member_plan_authority
        elif member_plan_authority != plan_authority:
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid batch spans multiple capacity-plan authorities.')
        validation_by_card.setdefault(card, []).append(
            (validation_claim, units, pool_key))
        infos[target.replica_id] = info
        claim_payloads[target.replica_id] = (pool_key, claim_payload)

    for card in sorted(validation_by_card):
        members = validation_by_card[card]
        aggregate_claim = dict(members[0][0])
        aggregate_claim['capacity_plan_units'] = sum(
            item[1] for item in members)
        capacity_admission.validate_paid_claim_in_connection(
            connection,
            service,
            aggregate_claim,
            prospective=False,
            require_planner=True,
            protocol_and_service_prelocked=True,
            _batch_member_units=tuple(item[1] for item in members),
            _batch_member_pool_keys=tuple(item[2] for item in members))

    prepared_members = []
    for target in targets:
        info = infos[target.replica_id]
        pool_key, claim_payload = claim_payloads[target.replica_id]
        record_id = _nonempty(str(info.replica_record_id), 'replica_record_id')
        profile = NonPoolLaunchProfile.create(
            NonPoolLaunchProfileKind.ORDINARY_PAID,
            authorization_reference=(
                f'paid-capacity:{service["hash"]}:{record_id}:'
                f'{pool_key}'),
            authorization_generation=0,
            authorization_payload={
                'claim': claim_payload,
                'placement': _replica_placement_payload(info),
            })
        submission_id = derive_ordinary_launch_submission_id(
            authority.service_name, target.replica_id, target.replica_record_id,
            1)
        association_id, request_id = derive_binding_ids(
            tenant_scope, authority.service_workspace, submission_id)
        resource_action_identity = (
            derive_fresh_ordinary_paid_resource_action_identity(
                replica_id=target.replica_id,
                replica_record_id=target.replica_record_id,
                cluster_name=target.cluster_name))
        prepared_members.append(
            PreparedFreshOrdinaryPaidMember(
                target=target,
                submission_id=submission_id,
                association_id=association_id,
                request_id=request_id,
                paid_capacity_pool_key=pool_key,
                profile=profile,
                resource_action_identity=(resource_action_identity)))

    association_ids = tuple(
        member.association_id for member in prepared_members)
    replica_keys = tuple(
        (member.target.replica_id, member.target.replica_record_id)
        for member in prepared_members)
    replica_record_ids = tuple(
        member.target.replica_record_id for member in prepared_members)
    collisions = connection.execute(
        sqlalchemy.select(
            ordinary_launch_associations_table.c.association_id).where(
                sqlalchemy.or_(
                    ordinary_launch_associations_table.c.association_id.in_(
                        association_ids),
                    sqlalchemy.and_(
                        ordinary_launch_associations_table.c.service_name ==
                        authority.service_name,
                        sqlalchemy.or_(
                            ordinary_launch_associations_table.c.
                            replica_record_id.in_(replica_record_ids),
                            sqlalchemy.tuple_(
                                ordinary_launch_associations_table.c.replica_id,
                                ordinary_launch_associations_table.c.
                                replica_record_id).in_(
                                    replica_keys))))).with_for_update()).all()
    if collisions:
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid batch collided with existing association history.')

    return PreparedFreshOrdinaryPaidBatch(tenant_scope=tenant_scope,
                                          authority=authority,
                                          members=tuple(prepared_members),
                                          _connection=connection,
                                          _transaction=transaction)


def commit_prepared_fresh_ordinary_paid_batch_in_connection(
    connection: sqlalchemy.engine.Connection,
    prepared: PreparedFreshOrdinaryPaidBatch,
    identities: Sequence[NonPoolBindingIdentity],
) -> tuple[BindingAdmission, ...]:
    """Set-insert associations and replica pointers for a prepared wave."""
    _require_postgres(connection)
    identities = tuple(identities)
    if (not isinstance(prepared, PreparedFreshOrdinaryPaidBatch) or
            not prepared.belongs_to(connection) or
            len(identities) != len(prepared.members)):
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid batch proof is absent or belongs to another '
            'transaction.')

    values = []
    authority = prepared.authority
    for member, identity in zip(prepared.members, identities, strict=True):
        target = member.target
        if not isinstance(identity, NonPoolBindingIdentity):
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid request identity is not a generic binding.')
        expected = build_non_pool_binding_identity(
            BindingIntent(
                service_name=authority.service_name,
                service_hash=authority.service_hash,
                service_version=target.service_version,
                replica_id=target.replica_id,
                replica_record_id=target.replica_record_id,
                lifecycle_epoch=authority.service_lifecycle_epoch,
                binding_epoch=authority.binding_epoch,
                controller_incarnation=authority.controller_incarnation,
                controller_owner_epoch=authority.controller_owner_epoch,
                controller_pid=authority.controller_pid,
                controller_ip=authority.controller_ip),
            submission_id=member.submission_id,
            tenant_scope=prepared.tenant_scope,
            service_workspace=authority.service_workspace,
            cluster_name=target.cluster_name,
            input_digest=identity.input_digest,
            profile=member.profile,
            capability_cohort_epoch=NON_POOL_CAPABILITY_COHORT_EPOCH,
            capability_profile_set_digest=supported_non_pool_profile_set_digest(
            ),
            receipt_protocol_version=NON_POOL_RECEIPT_PROTOCOL_VERSION)
        if identity != expected:
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid request identity differs from its prepared row.')
        expected_resource_identity = (
            derive_fresh_ordinary_paid_resource_action_identity(
                replica_id=target.replica_id,
                replica_record_id=target.replica_record_id,
                cluster_name=target.cluster_name))
        if member.resource_action_identity != expected_resource_identity:
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid resource-action identity differs from its '
                'prepared row.')
        row = _identity_values(
            identity, 1, paid_capacity_pool_key=member.paid_capacity_pool_key)
        row.update({
            'owner_controller_incarnation': identity.controller_incarnation,
            'owner_controller_epoch': identity.controller_owner_epoch,
            'owner_revision': 1,
            'effect_phase': EffectPhase.NOT_STARTED.value,
            'resolution': Resolution.BOUND.value,
            'reconciliation_outcome': ReconciliationOutcome.ACTIVE_ADOPT.value,
            'provider_evidence': ProviderEvidence.NOT_QUERIED.value,
        })
        values.append(row)

    inserted = set(
        connection.execute(
            sqlalchemy.insert(ordinary_launch_associations_table).returning(
                ordinary_launch_associations_table.c.association_id),
            values).scalars())
    expected_association_ids = {
        identity.association_id for identity in identities
    }
    if inserted != expected_association_ids:
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid association set was not inserted exactly once.')

    pointer_values = sqlalchemy.values(
        sqlalchemy.column('replica_id', sqlalchemy.Integer),
        sqlalchemy.column('association_id', sqlalchemy.Uuid(as_uuid=True)),
        sqlalchemy.column('replica_incarnation', sqlalchemy.Uuid(as_uuid=True)),
        sqlalchemy.column('desired_generation', sqlalchemy.Integer),
        sqlalchemy.column('sky_cluster_record_uuid',
                          sqlalchemy.Uuid(as_uuid=True)),
        name='fresh_paid_associations').data([
            (identity.replica_id, identity.association_id,
             member.resource_action_identity.replica_incarnation,
             member.resource_action_identity.desired_generation,
             member.resource_action_identity.sky_cluster_record_uuid)
            for member, identity in zip(
                prepared.members, identities, strict=True)
        ])
    replicas = serve_state_schema.replicas_table
    pointed = connection.execute(
        sqlalchemy.update(replicas).where(
            replicas.c.service_name == authority.service_name,
            replicas.c.replica_id == pointer_values.c.replica_id,
            replicas.c.ordinary_launch_association_id.is_(None),
            replicas.c.replica_incarnation.is_(None),
            replicas.c.desired_generation.is_(None),
            replicas.c.sky_cluster_record_uuid.is_(None)).values(
                ordinary_launch_association_id=pointer_values.c.association_id,
                replica_incarnation=pointer_values.c.replica_incarnation,
                desired_generation=pointer_values.c.desired_generation,
                sky_cluster_record_uuid=(
                    pointer_values.c.sky_cluster_record_uuid)))
    if pointed.rowcount != len(identities):
        raise OrdinaryLaunchBindingConflict(
            'Fresh paid replica pointer set changed during admission.')

    return tuple(
        BindingAdmission(disposition=AdmissionDisposition.CREATE,
                         association_id=str(identity.association_id),
                         request_id=identity.request_id,
                         launch_generation=1,
                         owner_revision=1,
                         resolution=Resolution.BOUND,
                         effect_phase=EffectPhase.NOT_STARTED)
        for identity in identities)


def insert_or_get_locked(
    connection: sqlalchemy.engine.Connection,
    identity: BindingIdentity,
) -> BindingAdmission:
    """Create or validate Serve association state without committing.

    Lock order is lifecycle, service, replica, current association, then
    association history.  The caller may subsequently insert or verify the API
    request, retention pin, and queue row on this same connection.
    """
    _require_postgres(connection)
    if not isinstance(identity, BindingIdentity):
        raise ValueError('identity must be a BindingIdentity.')
    planner_funding_candidate = (
        isinstance(identity, NonPoolBindingIdentity) and
        identity.profile.kind is not NonPoolLaunchProfileKind.RESERVED_FILL)
    if planner_funding_candidate:
        serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
            connection)
    lifecycle, service, replica, current = _lock_admission_rows(
        connection, identity)
    existing = current
    if existing is None or existing['association_id'] != identity.association_id:
        existing = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id == identity.
                association_id).with_for_update()).mappings().one_or_none()
    # A first admission must still be authorized by the live planner under the
    # locked service/replica rows.  An exact retry instead validates the
    # immutable stored profile below; re-resolving mutable observations here
    # would make a committed request lose idempotency after a lost ACK.  The
    # shared pre-provider guard independently revalidates live authority before
    # any external effect.
    _validate_admission_target(
        connection,
        lifecycle,
        service,
        replica,
        identity,
        validate_profile_authority=existing is None,
        protocol_and_service_prelocked=(planner_funding_candidate))
    if existing is not None:
        if not _existing_identity_matches(existing, identity):
            raise OrdinaryLaunchBindingConflict(
                'Submission ID was reused with a different launch intent.')
        resolution = Resolution(str(existing['resolution']))
        if (resolution in UNSETTLED_RESOLUTIONS and
                not _unsettled_paid_capacity_claim_matches(
                    connection, existing)):
            raise OrdinaryLaunchBindingConflict(
                'Unsettled association lost its exact paid-capacity claim.')
        pointer = replica['ordinary_launch_association_id']
        if ((resolution in UNSETTLED_RESOLUTIONS and
             pointer != identity.association_id) or
            (resolution in SETTLED_RESOLUTIONS and pointer is not None)):
            raise OrdinaryLaunchBindingConflict(
                'Exact association and replica pointer disagree.')
        return _admission_from_row(existing,
                                   AdmissionDisposition.EXISTING_EXACT)

    if current is not None:
        raise OrdinaryLaunchBindingConflict(
            'Replica already has a different unsettled association.')
    history = connection.execute(
        sqlalchemy.select(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.service_name ==
            identity.service_name,
            ordinary_launch_associations_table.c.replica_record_id ==
            identity.replica_record_id).order_by(
                ordinary_launch_associations_table.c.launch_generation.desc()).
        with_for_update()).mappings().all()
    if history:
        if isinstance(identity, NonPoolBindingIdentity):
            raise OrdinaryLaunchBindingConflict(
                'A settled planner record cannot admit another launch action; '
                'retire it and create a fresh planner intent.')
        predecessor = history[0]
        if (predecessor['resolution'] != Resolution.PRE_EFFECT_TERMINAL.value or
                predecessor['pin_released_at'] is None or
                predecessor['service_binding_epoch']
                != identity.service_binding_epoch):
            raise OrdinaryLaunchBindingConflict(
                'Predecessor does not prove pre-effect terminal settlement.')
        if predecessor['cancel_reason'] is not None:
            raise OrdinaryLaunchBindingConflict(
                'A cancelled pre-effect predecessor cannot admit a '
                'successor.')
        launch_generation = int(predecessor['launch_generation']) + 1
    else:
        launch_generation = 1
    paid_capacity_pool_key = _active_paid_capacity_pool_key(
        connection, identity, replica)
    values = _identity_values(identity,
                              launch_generation,
                              paid_capacity_pool_key=paid_capacity_pool_key)
    values.update({
        'owner_controller_incarnation': identity.controller_incarnation,
        'owner_controller_epoch': identity.controller_owner_epoch,
        'owner_revision': 1,
        'effect_phase': EffectPhase.NOT_STARTED.value,
        'resolution': Resolution.BOUND.value,
        'updated_at': sqlalchemy.func.clock_timestamp(),
    })
    if isinstance(identity, NonPoolBindingIdentity):
        values.update({
            'reconciliation_outcome': ReconciliationOutcome.ACTIVE_ADOPT.value,
            'provider_evidence': ProviderEvidence.NOT_QUERIED.value,
        })
    connection.execute(
        sqlalchemy.insert(ordinary_launch_associations_table).values(**values))
    pointed = connection.execute(
        sqlalchemy.update(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            identity.service_name,
            serve_state_schema.replicas_table.c.replica_id ==
            identity.replica_id,
            serve_state_schema.replicas_table.c.ordinary_launch_association_id.
            is_(None)).values(
                ordinary_launch_association_id=identity.association_id))
    if pointed.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Replica association pointer changed during admission.')
    row = dict(values)
    row['launch_generation'] = launch_generation
    return _admission_from_row(row, AdmissionDisposition.CREATE)


def get_binding(association_id: str) -> dict[str, Any] | None:
    association_uuid = _canonical_uuid(association_id, 'association_id')
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise OrdinaryLaunchBindingUnavailable(
            'Ordinary launch binding requires central PostgreSQL state.')
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id ==
                association_uuid)).mappings().one_or_none()
    if row is None:
        return None
    values = dict(row)
    for field in ('association_id', 'submission_id', 'replica_record_id',
                  'owner_controller_incarnation'):
        values[field] = str(values[field])
    return values


def _serve042_supported(engine: sqlalchemy.engine.Engine) -> bool:
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    revision = migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)
    return revision is not None and int(revision) >= 42


def _serve055_supported(engine: sqlalchemy.engine.Engine) -> bool:
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return False
    revision = migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)
    return revision is not None and int(revision) >= 55


def binding_mode(service_name: str) -> BindingMode | None:
    """Return the durable service cutover mode, or None before Serve042."""
    service_name = _nonempty(service_name, 'service_name')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        return None
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.
                ordinary_launch_binding_mode, serve_state_schema.services_table.
                c.ordinary_launch_binding_capable).where(
                    serve_state_schema.services_table.c.name ==
                    service_name)).one_or_none()
    if row is None:
        raise OrdinaryLaunchBindingConflict('Service does not exist.')
    mode = BindingMode(str(row[0]))
    if mode == BindingMode.BOUND and row[1] is not True:
        raise OrdinaryLaunchBindingConflict(
            'Bound service is owned by an incapable controller.')
    return mode


def get_unsettled_binding_for_replica(
    service_name: str,
    replica_record_id: str,
) -> dict[str, Any] | None:
    record_uuid = _canonical_uuid(replica_record_id, 'replica_record_id')
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return None
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.service_name ==
                service_name,
                ordinary_launch_associations_table.c.replica_record_id ==
                record_uuid,
                ordinary_launch_associations_table.c.resolution.in_(
                    tuple(value.value for value in UNSETTLED_RESOLUTIONS)))
        ).mappings().one_or_none()
    return None if row is None else dict(row)


def get_for_replica(service_name: str, replica_id: int,
                    replica_record_id: str) -> dict[str, Any] | None:
    """Resolve the exact association named by one replica scalar pointer."""
    service_name = _nonempty(service_name, 'service_name')
    replica_id = _positive_int(replica_id, 'replica_id')
    record_uuid = _canonical_uuid(replica_record_id, 'replica_record_id')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        return None
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).join(
                serve_state_schema.replicas_table,
                sqlalchemy.and_(
                    serve_state_schema.replicas_table.c.service_name ==
                    ordinary_launch_associations_table.c.service_name,
                    serve_state_schema.replicas_table.c.replica_id ==
                    ordinary_launch_associations_table.c.replica_id,
                    serve_state_schema.replicas_table.c.
                    ordinary_launch_association_id ==
                    ordinary_launch_associations_table.c.association_id)).where(
                        ordinary_launch_associations_table.c.service_name ==
                        service_name,
                        ordinary_launch_associations_table.c.replica_id ==
                        replica_id,
                        ordinary_launch_associations_table.c.replica_record_id
                        == record_uuid)).mappings().one_or_none()
    return None if row is None else dict(row)


def list_provider_reconciliation_contexts(
    authority: ControllerBindingAuthority,) -> list[BoundNonPoolLaunchContext]:
    """List exact ambiguous non-pool actions still pinned to replicas.

    Provider reconciliation normally starts when a local launch worker
    finishes.  A controller can instead recover a replica after teardown has
    already persisted ``INTERRUPTED``; that row has no launch worker, but its
    exact ambiguous association still needs the same provider-evidence path.
    This single indexed query finds both shapes without scanning every
    replica or inferring authority from status.
    """
    if not isinstance(authority, ControllerBindingAuthority):
        raise TypeError('authority must be a ControllerBindingAuthority.')
    if not authority.retained_non_pool_settlement_allowed:
        return []
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        return []
    associations = ordinary_launch_associations_table
    replicas = serve_state_schema.replicas_table
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(associations).join(
                replicas,
                sqlalchemy.and_(
                    replicas.c.service_name == associations.c.service_name,
                    replicas.c.replica_id == associations.c.replica_id,
                    replicas.c.ordinary_launch_association_id ==
                    associations.c.association_id)).
            where(
                associations.c.service_name == authority.service_name,
                associations.c.service_hash == authority.service_hash,
                associations.c.service_workspace == authority.service_workspace,
                associations.c.service_lifecycle_epoch ==
                authority.service_lifecycle_epoch,
                associations.c.service_binding_epoch == authority.binding_epoch,
                associations.c.owner_controller_incarnation ==
                authority.controller_incarnation,
                associations.c.owner_controller_epoch ==
                authority.controller_owner_epoch,
                associations.c.resolution == Resolution.AMBIGUOUS.value,
                associations.c.reconciliation_outcome ==
                ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.value,
                associations.c.binding_protocol_version ==
                NON_POOL_BINDING_PROTOCOL_VERSION,
                associations.c.capability_cohort_epoch ==
                authority.non_pool_capability_cohort_epoch,
                associations.c.capability_profile_set_digest ==
                authority.non_pool_profile_set_digest,
                associations.c.receipt_protocol_version ==
                authority.non_pool_receipt_protocol_version,
                associations.c.profile_kind.is_not(None)).order_by(
                    associations.c.replica_id)).mappings().all()
    contexts = []
    for row in rows:
        context = bound_context_from_association(row)
        if not isinstance(context, BoundNonPoolLaunchContext):
            raise OrdinaryLaunchBindingConflict(
                'Provider reconciliation selected a non-generic '
                'association.')
        contexts.append(context)
    return contexts


def _binding_allows_request(
    association_id: str | None,
    request_id: str,
    *,
    reserved_fill_workspace_authority: tuple[str, str] | None = None,
) -> bool:
    """Conservative non-locking pre-claim qualification.

    This read can only reject queue work early.  It never grants provider
    authority; the later shared-guard transaction locks and revalidates the
    complete request/Serve tuple.
    """
    tenant_scope: str | None = None
    service_workspace: str | None = None
    try:
        association_uuid = (None if association_id is None else _canonical_uuid(
            association_id, 'association_id'))
        request_id = _nonempty(request_id, 'request_id')
        if reserved_fill_workspace_authority is not None:
            tenant_scope = _nonempty(reserved_fill_workspace_authority[0],
                                     'tenant_scope')
            service_workspace = _nonempty(reserved_fill_workspace_authority[1],
                                          'service_workspace')
    except ValueError:
        return False
    engine = serve_state.get_database_engine()
    if reserved_fill_workspace_authority is not None:
        if not _serve055_supported(engine):
            return False
    elif not _serve042_supported(engine):
        return False
    association = ordinary_launch_associations_table
    service = serve_state_schema.services_table
    replica = serve_state_schema.replicas_table
    lifecycle = serve_state_schema.service_lifecycle_fences_table
    association_predicates = ([
        association.c.request_id == request_id
    ] if association_uuid is None else [
        association.c.association_id == association_uuid,
        association.c.request_id == request_id,
    ])
    statement = sqlalchemy.select(
        association,
        service.c.hash.label('_current_service_hash'),
        service.c.workspace.label('_current_workspace'),
        service.c.lifecycle_epoch.label('_current_lifecycle_epoch'),
        service.c.ordinary_launch_binding_mode.label('_current_binding_mode'),
        service.c.ordinary_launch_binding_epoch.label('_current_binding_epoch'),
        service.c.ordinary_launch_binding_capable.label('_current_capable'),
        service.c.non_pool_launch_binding_capable.label(
            '_current_non_pool_capable'),
        service.c.non_pool_launch_binding_protocol_version.label(
            '_current_non_pool_protocol'),
        service.c.non_pool_launch_capability_profile_set_digest.label(
            '_current_non_pool_profile_set'),
        service.c.non_pool_launch_capability_cohort_epoch.label(
            '_current_non_pool_cohort'),
        service.c.non_pool_launch_receipt_protocol_version.label(
            '_current_non_pool_receipt'),
        service.c.controller_incarnation.label('_current_incarnation'),
        service.c.controller_owner_epoch.label('_current_owner_epoch'),
        service.c.status.label('_current_service_status'),
        lifecycle.c.epoch.label('_fence_epoch'),
        replica.c.replica_id.label('_replica_id'),
        replica.c.replica_state_version.label('_replica_state_version'),
        replica.c.replica_state.label('_replica_state'),
        replica.c.status.label('_replica_status'),
        replica.c.version.label('_replica_version'),
        replica.c.cluster_name.label('_replica_cluster_name'),
        replica.c.paid_capacity_pool_key.label('_replica_paid_pool_key'),
        replica.c.ordinary_launch_association_id.label('_replica_pointer'),
    ).join(service, service.c.name == association.c.service_name).join(
        lifecycle, lifecycle.c.name == association.c.service_name).join(
            replica,
            sqlalchemy.and_(
                replica.c.service_name == association.c.service_name,
                replica.c.replica_id == association.c.replica_id)).where(
                    *association_predicates,
                    association.c.resolution == Resolution.BOUND.value)
    if reserved_fill_workspace_authority is not None:
        statement = statement.add_columns(
            service.c.owner_user_id.label('_current_owner_user_id'),
            service.c.reserved_fill_actuation_mode.label(
                '_current_reserved_fill_actuation_mode'))
    with engine.connect() as connection:
        row = connection.execute(statement).mappings().one_or_none()
        elected_version = (None if row is None else
                           _elected_recovery_version_in_connection(
                               connection, str(row['service_name'])))
    if row is None:
        return False
    try:
        status = serve_statuses.ServiceStatus[str(
            row['_current_service_status'])]
    except (KeyError, TypeError):
        return False
    replica_snapshot = {
        'replica_id': row['_replica_id'],
        'replica_state_version': row['_replica_state_version'],
        'replica_state': row['_replica_state'],
        'status': row['_replica_status'],
        'version': row['_replica_version'],
        'cluster_name': row['_replica_cluster_name'],
        'paid_capacity_pool_key': row['_replica_paid_pool_key'],
    }
    generic_matches = bool(
        row['binding_protocol_version'] is None or
        (row['_current_non_pool_capable'] is True and
         row['_current_non_pool_protocol'] == row['binding_protocol_version'] ==
         NON_POOL_BINDING_PROTOCOL_VERSION and
         row['_current_non_pool_profile_set']
         == row['capability_profile_set_digest'] and
         row['_current_non_pool_profile_set']
         == supported_non_pool_profile_set_digest() and
         row['_current_non_pool_cohort'] == row['capability_cohort_epoch'] and
         row['_current_non_pool_cohort'] in
         (NON_POOL_CAPABILITY_COHORT_EPOCH, NON_POOL_CAPABILITY_COHORT_EPOCH -
          1) and row['_current_non_pool_receipt'] ==
         row['receipt_protocol_version'] == NON_POOL_RECEIPT_PROTOCOL_VERSION))
    expected_association_id = (row['association_id'] if association_uuid is None
                               else association_uuid)
    authorized = bool(
        generic_matches and
        row['_replica_pointer'] == expected_association_id and
        _replica_snapshot_matches_association(
            replica_snapshot, row, require_launch_authorized=True) and
        elected_version == row['service_version'] and
        row['_fence_epoch'] == row['service_lifecycle_epoch'] and
        row['_current_lifecycle_epoch'] == row['service_lifecycle_epoch'] and
        row['_current_service_hash'] == row['service_hash'] and
        row['_current_workspace'] == row['service_workspace'] and
        row['_current_binding_mode'] == BindingMode.BOUND.value and
        row['_current_binding_epoch'] == row['service_binding_epoch'] and
        row['_current_capable'] is True and
        row['_current_incarnation'] == row['owner_controller_incarnation'] and
        row['_current_owner_epoch'] == row['owner_controller_epoch'] and status
        not in serve_statuses.ServiceStatus.replica_launch_blocking_statuses())
    if not authorized or reserved_fill_workspace_authority is None:
        return authorized
    assert tenant_scope is not None
    assert service_workspace is not None
    try:
        profile = _association_profile(row)
    except OrdinaryLaunchBindingConflict:
        return False
    return bool(profile is not None and
                profile.kind is NonPoolLaunchProfileKind.RESERVED_FILL and
                profile.authorization_kind
                is NonPoolLaunchAuthorizationKind.RESERVED_FILL_ALLOCATION and
                row['tenant_scope'] == tenant_scope and
                row['_current_owner_user_id'] == tenant_scope and
                row['_current_reserved_fill_actuation_mode']
                == zero_cost_actuation.ActuationMode.DURABLE_INTENT.value and
                row['service_workspace'] == service_workspace and
                row['_current_workspace'] == service_workspace)


def binding_allows_request(association_id: str, request_id: str) -> bool:
    """Conservative non-locking pre-claim qualification."""
    return _binding_allows_request(association_id, request_id)


def reserved_fill_binding_authorizes_workspace(
    request_id: str,
    tenant_scope: str,
    service_workspace: str,
) -> bool:
    """Authorize one internal fill against its current durable owner tuple."""
    return _binding_allows_request(
        None,
        request_id,
        reserved_fill_workspace_authority=(tenant_scope, service_workspace))


def _lock_effect_rows(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    *,
    require_paid_claim: bool = True,
) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[
        str, Any]]:
    lifecycle = connection.execute(
        sqlalchemy.select(serve_state_schema.service_lifecycle_fences_table).
        where(serve_state_schema.service_lifecycle_fences_table.c.name ==
              context.service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            context.service_name).with_for_update()).mappings().one_or_none()
    # The pool identity is immutable association data.  Reading it after the
    # service mutex but before its association row lock is safe: every
    # association writer holds this same service row, and the Serve042 trigger
    # rejects identity changes.  This pre-read is what preserves the global
    # service -> paid pool -> replica order used by paid-capacity admission.
    preassociation = connection.execute(
        sqlalchemy.select(
            ordinary_launch_associations_table.c.association_id,
            ordinary_launch_associations_table.c.service_name,
            ordinary_launch_associations_table.c.service_hash,
            ordinary_launch_associations_table.c.replica_id,
            ordinary_launch_associations_table.c.paid_capacity_pool_key,
        ).where(ordinary_launch_associations_table.c.association_id ==
                context.association_id)).mappings().one_or_none()
    if service is None or preassociation is None:
        raise OrdinaryLaunchBindingConflict(
            'Bound effect service or association disappeared.')
    pool_key = preassociation['paid_capacity_pool_key']
    if pool_key is not None:
        pool = connection.execute(
            sqlalchemy.select(
                serve_state_schema.paid_capacity_pools_table.c.pool_key).where(
                    serve_state_schema.paid_capacity_pools_table.c.pool_key ==
                    pool_key).with_for_update()).scalar_one_or_none()
        if pool is None and require_paid_claim:
            raise OrdinaryLaunchBindingConflict(
                'Bound paid-capacity pool disappeared before reduction.')
    replica = connection.execute(
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            context.service_name, serve_state_schema.replicas_table.c.replica_id
            == context.replica_id).with_for_update()).mappings().one_or_none()
    claim_rows = connection.execute(
        sqlalchemy.select(
            serve_state_schema.paid_capacity_claims_table.c.service_hash,
            serve_state_schema.paid_capacity_claims_table.c.pool_key).where(
                serve_state_schema.paid_capacity_claims_table.c.service_name ==
                context.service_name,
                serve_state_schema.paid_capacity_claims_table.c.replica_id ==
                context.replica_id).with_for_update()).all()
    association = connection.execute(
        sqlalchemy.select(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id).with_for_update()).mappings().one_or_none()
    if any(row is None for row in (lifecycle, service, replica, association)):
        raise OrdinaryLaunchBindingConflict(
            'Bound effect authority disappeared before provider I/O.')
    assert lifecycle is not None and service is not None
    assert replica is not None and association is not None
    if any(association[key] != preassociation[key]
           for key in ('association_id', 'service_name', 'service_hash',
                       'replica_id', 'paid_capacity_pool_key')):
        raise OrdinaryLaunchBindingConflict(
            'Paid-capacity association identity changed while locking.')
    expected_claim = association['paid_capacity_pool_key']
    claim_mismatch = (expected_claim is None and claim_rows) or (
        expected_claim is not None and
        (len(claim_rows) != 1 or claim_rows[0][0] != association['service_hash']
         or claim_rows[0][1] != expected_claim or
         replica['paid_capacity_pool_key'] != expected_claim))
    if require_paid_claim and claim_mismatch:
        raise OrdinaryLaunchBindingConflict(
            'Unsettled association does not hold its exact paid-capacity '
            'claim.')
    return lifecycle, service, replica, association


def _validate_effect_rows(
    lifecycle: Mapping[str, Any],
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    association: Mapping[str, Any],
    context: BoundLaunchContext,
    *,
    allowed_resolutions: frozenset[Resolution] = frozenset({Resolution.BOUND}),
    require_launch_authorized: bool = False,
) -> None:
    if (association['request_id'] != context.request_id or
            association['service_name'] != context.service_name or
            association['replica_id'] != context.replica_id or
            association['replica_record_id'] != context.replica_record_id or
            association['launch_generation'] != context.launch_generation or
            association['input_digest'] != context.input_digest or
            association['resolution'] not in tuple(
                resolution.value for resolution in allowed_resolutions)):
        raise OrdinaryLaunchBindingConflict(
            'Bound request identity, revision, or resolution changed.')
    persisted_profile = _association_profile(association)
    context_profile = (context.profile if isinstance(
        context, BoundNonPoolLaunchContext) else None)
    if (persisted_profile != context_profile or
        (persisted_profile is not None and
         isinstance(context, BoundNonPoolLaunchContext) and
         (association['binding_protocol_version']
          != NON_POOL_BINDING_PROTOCOL_VERSION or
          association['capability_cohort_epoch']
          != context.capability_cohort_epoch or
          association['capability_profile_set_digest']
          != context.capability_profile_set_digest or
          association['receipt_protocol_version']
          != context.receipt_protocol_version))):
        raise OrdinaryLaunchBindingConflict(
            'Bound request profile or capability cohort changed.')
    if (isinstance(context, BoundNonPoolLaunchContext) and
            context.profile.kind is NonPoolLaunchProfileKind.ORDINARY_PAID and
            context.capability_cohort_epoch
            >= ORDINARY_PAID_RESOURCE_ACTION_IDENTITY_COHORT_FLOOR):
        if context.launch_generation != 1:
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid resource-action identity has a different launch '
                'generation.')
        try:
            expected_resource_identity = (
                derive_fresh_ordinary_paid_resource_action_identity(
                    replica_id=context.replica_id,
                    replica_record_id=context.replica_record_id,
                    cluster_name=replica.get('cluster_name')))
        except (TypeError, ValueError) as error:
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid resource-action identity is malformed.') from error
        observed_resource_identity = (
            replica.get('replica_incarnation'),
            replica.get('desired_generation'),
            replica.get('sky_cluster_record_uuid'),
        )
        if observed_resource_identity != (
                expected_resource_identity.replica_incarnation,
                expected_resource_identity.desired_generation,
                expected_resource_identity.sky_cluster_record_uuid):
            raise OrdinaryLaunchBindingConflict(
                'Fresh paid resource-action identity changed before provider '
                'I/O.')
    if (replica['ordinary_launch_association_id'] != context.association_id or
            not _replica_snapshot_matches_association(
                replica,
                association,
                require_launch_authorized=require_launch_authorized)):
        raise OrdinaryLaunchBindingConflict(
            'Replica pointer, identity, version, status, or cluster changed.')
    if (lifecycle['epoch'] != association['service_lifecycle_epoch'] or
            service['lifecycle_epoch'] != association['service_lifecycle_epoch']
            or service['hash'] != association['service_hash'] or
            service['workspace'] != association['service_workspace'] or
            service['ordinary_launch_binding_mode'] != 'bound' or
            service['ordinary_launch_binding_epoch']
            != association['service_binding_epoch'] or
            service['ordinary_launch_binding_capable'] is not True or
            service['controller_incarnation']
            != association['owner_controller_incarnation'] or
            service['controller_owner_epoch']
            != association['owner_controller_epoch']):
        raise OrdinaryLaunchBindingConflict(
            'Service lifecycle, binding epoch, or owner changed.')
    if persisted_profile is not None:
        if not isinstance(context, BoundNonPoolLaunchContext):
            raise OrdinaryLaunchBindingConflict(
                'Generic association lost its typed execution context.')
        if require_launch_authorized:
            _validate_generic_capability(
                service,
                capability_cohort_epoch=context.capability_cohort_epoch,
                capability_profile_set_digest=(
                    context.capability_profile_set_digest),
                receipt_protocol_version=context.receipt_protocol_version)
        else:
            _validate_retained_generic_cleanup_capability(
                service,
                capability_cohort_epoch=context.capability_cohort_epoch,
                capability_profile_set_digest=(
                    context.capability_profile_set_digest),
                receipt_protocol_version=context.receipt_protocol_version)
    if require_launch_authorized:
        try:
            service_status = serve_statuses.ServiceStatus[str(
                service['status'])]
        except (KeyError, TypeError) as error:
            raise OrdinaryLaunchBindingConflict(
                'Bound effect encountered an unknown service status.'
            ) from error
        if service_status in (serve_statuses.ServiceStatus.
                              replica_launch_blocking_statuses()):
            raise OrdinaryLaunchBindingConflict(
                'Service no longer authorizes provider effects.')


def retained_reduction_snapshot_matches(
    lifecycle: Mapping[str, Any],
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    association: Mapping[str, Any],
    context: BoundLaunchContext,
) -> bool:
    """Purely validate one non-authorizing retained-reduction snapshot.

    The caller may use this only to avoid an unnecessary mutation reducer pass.
    It grants no provider, projection, cancellation, cleanup, or ownership
    authority, and deliberately accepts only the still-bound resolution.
    """
    try:
        _validate_effect_rows(lifecycle,
                              service,
                              replica,
                              association,
                              context,
                              allowed_resolutions=frozenset({Resolution.BOUND}),
                              require_launch_authorized=False)
    except (KeyError, TypeError, ValueError, OrdinaryLaunchBindingConflict):
        return False
    return True


def lock_reduction_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
) -> Mapping[str, Any]:
    """Lock canonical Serve rows before request result and pin evidence."""
    _require_postgres(connection)
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context, require_paid_claim=False)
    _validate_effect_rows(lifecycle,
                          service,
                          replica,
                          association,
                          context,
                          allowed_resolutions=UNSETTLED_RESOLUTIONS)
    return dict(association)


def validate_effect_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    claim: ExecutionClaim,
    claim_validator: ClaimValidator,
    *,
    launch_context: Mapping[str, Any] | None = None,
) -> EffectAuthoritySnapshot:
    _require_postgres(connection)
    if (getattr(claim, 'request_id', None) != context.request_id or
            getattr(claim, 'execution_generation', 0) < 1 or
            not getattr(claim, 'claim_token', None) or
            not getattr(claim, 'worker_instance_id', None)):
        raise OrdinaryLaunchBindingConflict(
            'The exact API request execution claim is no longer active.')
    legacy_context = not isinstance(context, BoundNonPoolLaunchContext)
    planner_funding_candidate = (
        isinstance(context, BoundNonPoolLaunchContext) and
        context.profile.kind is not NonPoolLaunchProfileKind.RESERVED_FILL)
    if legacy_context or planner_funding_candidate:
        # A retained protocol-v1 association has no typed profile from which
        # to determine funding before lifecycle/service locks.  Conservatively
        # share the protocol prefix; its locked paid claim below determines
        # whether planner revalidation is required.
        serve_state.lock_zero_cost_protocol_for_bound_launch_observation(
            connection)
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context)
    _validate_effect_rows(lifecycle,
                          service,
                          replica,
                          association,
                          context,
                          require_launch_authorized=True)
    # The atomic replica+claim+global-cap debit authorizes exactly one first
    # provider effect. Raw demand telemetry or a demand-derived successor plan
    # cannot revoke it; explicit durable cancellation/retirement may still
    # remove the claim before this boundary. Once PROVIDER_IO is durable, later
    # phase transitions are bookkeeping for that same immutable profile.
    # Current desired state independently owns any compensating teardown.
    provider_start = association[
        'effect_phase'] == EffectPhase.NOT_STARTED.value
    paid_fresh_until = None
    if legacy_context:
        _, legacy_paid_claim = _paid_claim_payload(
            connection, service, replica, _locked_replica_info(replica))
        if legacy_paid_claim is not None and provider_start:
            paid_fresh_until = (
                capacity_admission.validate_paid_claim_in_connection(
                    connection,
                    service,
                    legacy_paid_claim,
                    protocol_and_service_prelocked=True))
    if isinstance(context, BoundNonPoolLaunchContext):
        _require_current_non_pool_worker_projection_in_connection(
            connection, context.service_name,
            int(association['service_version']))
        if context.profile.kind is NonPoolLaunchProfileKind.RESERVED_FILL:
            _validate_committed_reserved_fill_profile_in_connection(
                connection, service, replica, context.profile)
        else:
            paid_fresh_until = _validate_profile_authority_in_connection(
                connection,
                service,
                replica,
                context.profile,
                protocol_and_service_prelocked=(planner_funding_candidate),
                validate_paid_provider_start=provider_start)
        if launch_context is None:
            raise OrdinaryLaunchBindingConflict(
                'Generic provider effect has no immutable launch context.')
        _validate_profile_execution_context(service, replica, context,
                                            launch_context)
    if (_elected_recovery_version_in_connection(connection,
                                                context.service_name)
            != association['service_version']):
        raise OrdinaryLaunchBindingConflict(
            'Bound request service version is no longer elected.')
    if not claim_validator(connection, context.association_id, claim):
        raise OrdinaryLaunchBindingConflict(
            'The exact API request execution claim is no longer active.')
    if paid_fresh_until is not None:
        provider_start_now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        if paid_fresh_until <= provider_start_now:
            raise OrdinaryLaunchBindingConflict(
                'Paid provider authority expired while locking its request.')
    return EffectAuthoritySnapshot(dict(association),
                                   _locked_replica_info(replica))


def _advance_effect_phase(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    claim: ExecutionClaim,
    claim_validator: ClaimValidator,
    expected: EffectPhase,
    target: EffectPhase,
    *,
    service_job_id: int | None = None,
    launch_context: Mapping[str, Any] | None = None,
) -> tuple[int, Any]:
    authority = validate_effect_authority_in_connection(
        connection,
        context,
        claim,
        claim_validator,
        launch_context=launch_context)
    association = authority.association
    if association['effect_phase'] == target.value:
        if (target != EffectPhase.SERVICE_JOB_RECORDED or
                association['service_job_id'] == service_job_id):
            return (int(association['owner_revision']),
                    authority.durable_replica_info)
        raise OrdinaryLaunchBindingConflict(
            'Service-job replay used a different exact job ID.')
    if association['effect_phase'] != expected.value:
        raise OrdinaryLaunchBindingConflict(
            f'Effect phase is {association["effect_phase"]!r}, expected '
            f'{expected.value!r}.')
    next_revision = int(association['owner_revision']) + 1
    values: dict[str, Any] = {
        'effect_phase': target.value,
        'effect_phase_changed_at': sqlalchemy.func.clock_timestamp(),
        'owner_revision': next_revision,
        'updated_at': sqlalchemy.func.clock_timestamp(),
    }
    if target == EffectPhase.SERVICE_JOB_RECORDED:
        values['service_job_id'] = _positive_int(service_job_id,
                                                 'service_job_id')
    current_revision = int(association['owner_revision'])
    updated = connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id,
            ordinary_launch_associations_table.c.owner_revision ==
            current_revision,
            ordinary_launch_associations_table.c.effect_phase == expected.value,
            ordinary_launch_associations_table.c.resolution ==
            Resolution.BOUND.value).values(**values))
    if updated.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Effect phase lost its exact compare-and-swap.')
    return next_revision, authority.durable_replica_info


_ACTIVE_EFFECT_AUTHORIZATION: contextvars.ContextVar[
    EffectAuthorization | None] = contextvars.ContextVar(
        'ordinary_launch_effect_authorization', default=None)


@contextlib.contextmanager
def provider_effect_guard(
    launch_context: Mapping[str, Any],
    claim: ExecutionClaim,
    *,
    claim_validator: ClaimValidator,
) -> Iterator[EffectAuthorization | None]:
    """Fence and record provider I/O, then hold shared authority through it."""
    if not has_bound_launch_context(launch_context):
        yield None
        return
    context = parse_bound_launch_context(launch_context)
    if claim.request_id != context.request_id:
        raise OrdinaryLaunchBindingConflict(
            'Active request claim does not name the bound request.')
    with serve_state.service_replica_launch_authority_guard(
            context.service_name) as guard:
        if not serve_state.service_replica_launch_authority_guard_is_valid(
                guard):
            raise OrdinaryLaunchBindingConflict(
                'Service launch authority guard lost its database session.')
        engine = serve_state.get_database_engine()
        with engine.begin() as connection:
            next_revision, durable_replica_info = _advance_effect_phase(
                connection,
                context,
                claim,
                claim_validator,
                EffectPhase.NOT_STARTED,
                EffectPhase.PROVIDER_IO,
                launch_context=launch_context)
        authorization = EffectAuthorization(context, claim, next_revision,
                                            durable_replica_info, guard,
                                            claim_validator)
        token = _ACTIVE_EFFECT_AUTHORIZATION.set(authorization)
        try:
            yield authorization
        finally:
            _ACTIVE_EFFECT_AUTHORIZATION.reset(token)


# Name requested by the distinct bound request handler.
authorize_provider_io = provider_effect_guard


@contextlib.contextmanager
def non_pool_provider_effect_guard(
    launch_context: Mapping[str, Any],
    claim: ExecutionClaim,
    *,
    claim_validator: ClaimValidator,
) -> Iterator[EffectAuthorization]:
    """Fence a complete protocol-v2 action at the common provider boundary."""
    context = parse_bound_non_pool_launch_context(launch_context)
    if claim.request_id != context.request_id:
        raise OrdinaryLaunchBindingConflict(
            'Active request claim does not name the bound request.')
    if context.profile.kind is NonPoolLaunchProfileKind.RESERVED_FILL:
        # The durable execution claim, claimed queue delivery, retention pin,
        # association effect phase, and guardian-authored quiescence receipt
        # are the in-flight barrier for reserved fill.  Commit that complete
        # boundary before provider I/O instead of retaining a PostgreSQL
        # session/advisory lock across a potentially blocked Kubernetes call.
        engine = serve_state.get_database_engine()
        with engine.begin() as connection:
            next_revision, durable_replica_info = _advance_effect_phase(
                connection,
                context,
                claim,
                claim_validator,
                EffectPhase.NOT_STARTED,
                EffectPhase.PROVIDER_IO,
                launch_context=launch_context)
        authorization = EffectAuthorization(context, claim, next_revision,
                                            durable_replica_info, None,
                                            claim_validator)
        token = _ACTIVE_EFFECT_AUTHORIZATION.set(authorization)
        try:
            yield authorization
            # A provider exception skips this read and remains effect-
            # ambiguous.  A completed call must not publish success after a
            # concurrently committed authority transition.
            current_authorization = _ACTIVE_EFFECT_AUTHORIZATION.get()
            if (current_authorization is None or
                    current_authorization.context != context):
                raise OrdinaryLaunchBindingConflict(
                    'Reserved-fill effect lost its active authorization.')
            _revalidate_lock_free_effect_authorization(current_authorization,
                                                       launch_context)
        finally:
            _ACTIVE_EFFECT_AUTHORIZATION.reset(token)
        return
    with serve_state.service_replica_launch_authority_guard(
            context.service_name) as guard:
        if not serve_state.service_replica_launch_authority_guard_is_valid(
                guard):
            raise OrdinaryLaunchBindingConflict(
                'Service launch authority guard lost its database session.')
        engine = serve_state.get_database_engine()
        with engine.begin() as connection:
            next_revision, durable_replica_info = _advance_effect_phase(
                connection,
                context,
                claim,
                claim_validator,
                EffectPhase.NOT_STARTED,
                EffectPhase.PROVIDER_IO,
                launch_context=launch_context)
        authorization = EffectAuthorization(context, claim, next_revision,
                                            durable_replica_info, guard,
                                            claim_validator)
        token = _ACTIVE_EFFECT_AUTHORIZATION.set(authorization)
        try:
            yield authorization
        finally:
            _ACTIVE_EFFECT_AUTHORIZATION.reset(token)


def _parse_any_bound_launch_context(
        launch_context: Mapping[str, Any]) -> BoundLaunchContext:
    if BINDING_PROTOCOL_VERSION_KEY in launch_context:
        return parse_bound_non_pool_launch_context(launch_context)
    return parse_bound_launch_context(launch_context)


def _active_authorization(
        launch_context: Mapping[str, Any]) -> EffectAuthorization | None:
    if not has_bound_launch_context(launch_context):
        return None
    requested = _parse_any_bound_launch_context(launch_context)
    authorization = _ACTIVE_EFFECT_AUTHORIZATION.get()
    if authorization is None or authorization.context != requested:
        raise OrdinaryLaunchBindingConflict(
            'Service-job I/O requires the active provider authority guard.')
    if authorization.guard is None:
        _revalidate_lock_free_effect_authorization(authorization,
                                                   launch_context)
    elif not serve_state.service_replica_launch_authority_guard_is_valid(
            authorization.guard):
        raise OrdinaryLaunchBindingConflict(
            'Service-job I/O requires the active provider authority guard.')
    return authorization


def _revalidate_lock_free_effect_authorization(
    authorization: EffectAuthorization,
    launch_context: Mapping[str, Any],
) -> None:
    """Freshly validate one durable reserved-fill effect claim.

    This function deliberately owns only a short transaction.  The caller
    invokes it before or after provider I/O; it must never wrap that I/O.
    """
    context = authorization.context
    if (authorization.guard is not None or
            not isinstance(context, BoundNonPoolLaunchContext) or
            context.profile.kind is not NonPoolLaunchProfileKind.RESERVED_FILL
            or parse_bound_non_pool_launch_context(launch_context) != context):
        raise OrdinaryLaunchBindingConflict(
            'Lock-free effect authority is not reserved fill.')
    engine = serve_state.get_database_engine()
    with engine.begin() as connection:
        snapshot = validate_effect_authority_in_connection(
            connection,
            context,
            authorization.claim,
            authorization.claim_validator,
            launch_context=launch_context)
        if int(snapshot.association['owner_revision']) != (
                authorization.owner_revision):
            raise OrdinaryLaunchBindingConflict(
                'Reserved-fill effect claim owner revision changed.')


def require_active_provider_effect_authorization(
        launch_context: Mapping[str, Any]) -> EffectAuthorization:
    """Prove the exact bound request still owns its outer provider guard.

    Cloud backends retain a legacy per-provider guard for ordinary Serve
    requests.  A bound request must bypass that PID/IP fence because controller
    takeover deliberately replaces those values with fail-closed sentinels,
    but only while this exact association is inside ``provider_effect_guard``.
    """
    authorization = _active_authorization(launch_context)
    if authorization is None:
        raise OrdinaryLaunchBindingConflict(
            'Bound provider I/O has no active association authorization.')
    return authorization


def record_paid_provider_allocation(
    launch_context: Mapping[str, Any],
    receipt: paid_capacity_lib.PaidProviderAllocationReceipt,
    *,
    request_validator: PaidProviderAllocationRequestValidator,
) -> ProviderAllocationDisposition:
    """Atomically checkpoint one full-fresh running paid allocation.

    This is economic pool feedback, not provider cleanup evidence. It runs on
    the exact shared launch-authority PostgreSQL session while the bound API
    request still owns its execution claim and provider effect.
    """
    authorization = _active_authorization(launch_context)
    if authorization is None:
        raise OrdinaryLaunchBindingConflict(
            'Paid provider allocation has no active effect authorization.')
    context = authorization.context
    if (not isinstance(context, BoundNonPoolLaunchContext) or
            context.profile.kind is not NonPoolLaunchProfileKind.ORDINARY_PAID
            or authorization.guard is None or
            not isinstance(receipt, paid_capacity.PaidProviderAllocationReceipt)
            or receipt.association_id != str(context.association_id) or
            receipt.replica_record_id != str(context.replica_record_id)):
        raise OrdinaryLaunchBindingConflict(
            'Paid provider-allocation receipt has no exact ordinary-paid '
            'authority.')

    def _record(connection: sqlalchemy.engine.Connection) -> bool:
        snapshot = validate_effect_authority_in_connection(
            connection,
            context,
            authorization.claim,
            authorization.claim_validator,
            launch_context=launch_context)
        association = snapshot.association
        if (association['resolution'] != Resolution.BOUND.value or
                association['reconciliation_outcome']
                != ReconciliationOutcome.ACTIVE_ADOPT.value or
                association['effect_phase'] != EffectPhase.PROVIDER_IO.value or
                int(association['owner_revision'])
                != authorization.owner_revision):
            raise OrdinaryLaunchBindingConflict(
                'Paid provider allocation lost its active provider phase.')
        if not request_validator(connection, context, association, receipt):
            raise OrdinaryLaunchBindingConflict(
                'Paid provider allocation contradicts its immutable '
                'request authority.')
        pool_key = association['paid_capacity_pool_key']
        if not isinstance(pool_key, str) or not pool_key:
            raise OrdinaryLaunchBindingConflict(
                'Paid provider allocation has no exact pool identity.')
        try:
            receipt.validate_pool_key(pool_key)
            receipt_sha256 = receipt.sha256(
                pool_key=pool_key, profile_digest=context.profile.digest)
        except ValueError as error:
            raise OrdinaryLaunchBindingConflict(
                'Paid provider-allocation receipt contradicts its bound '
                'pool.') from error
        return serve_state.record_paid_provider_allocation_in_transaction(
            connection,
            service_name=context.service_name,
            service_hash=str(association['service_hash']),
            replica_id=context.replica_id,
            pool_key=pool_key,
            receipt_sha256=receipt_sha256)

    recorded = serve_state.run_service_replica_launch_authority_transaction(
        authorization.guard, _record)
    return (ProviderAllocationDisposition.RECORDED
            if recorded else ProviderAllocationDisposition.EXACT_REPLAY)


def begin_service_job_io(launch_context: Mapping[str, Any]) -> int | None:
    authorization = _active_authorization(launch_context)
    if authorization is None:
        return None
    engine = serve_state.get_database_engine()
    with engine.begin() as connection:
        next_revision, durable_replica_info = _advance_effect_phase(
            connection,
            authorization.context,
            authorization.claim,
            authorization.claim_validator,
            EffectPhase.PROVIDER_IO,
            EffectPhase.SERVICE_JOB_IO,
            launch_context=launch_context)
    _ACTIVE_EFFECT_AUTHORIZATION.set(
        dataclasses.replace(authorization,
                            owner_revision=next_revision,
                            durable_replica_info=durable_replica_info))
    return next_revision


def record_service_job(launch_context: Mapping[str, Any],
                       job_id: int) -> int | None:
    authorization = _active_authorization(launch_context)
    if authorization is None:
        return None
    engine = serve_state.get_database_engine()
    with engine.begin() as connection:
        next_revision, durable_replica_info = _advance_effect_phase(
            connection,
            authorization.context,
            authorization.claim,
            authorization.claim_validator,
            EffectPhase.SERVICE_JOB_IO,
            EffectPhase.SERVICE_JOB_RECORDED,
            service_job_id=job_id,
            launch_context=launch_context)
    _ACTIVE_EFFECT_AUTHORIZATION.set(
        dataclasses.replace(authorization,
                            owner_revision=next_revision,
                            durable_replica_info=durable_replica_info))
    return next_revision


def transfer_service_owner_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    expected_incarnation: uuid.UUID,
    expected_owner_epoch: int,
    new_incarnation: uuid.UUID,
    new_controller_pid: int | None,
    new_controller_ip: str | None,
    capable: bool,
) -> ControllerBindingAuthority:
    """Transfer service and only associations in the mutable cohort window."""
    _require_postgres(connection)
    expected_incarnation = _canonical_uuid(expected_incarnation,
                                           'expected_incarnation')
    new_incarnation = _canonical_uuid(new_incarnation, 'new_incarnation')
    if new_incarnation == expected_incarnation:
        raise ValueError('A controller takeover requires a fresh incarnation.')
    expected_owner_epoch = _positive_int(expected_owner_epoch,
                                         'expected_owner_epoch')
    lifecycle = connection.execute(
        sqlalchemy.select(
            serve_state_schema.service_lifecycle_fences_table).where(
                serve_state_schema.service_lifecycle_fences_table.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            service_name).with_for_update()).mappings().one_or_none()
    if (lifecycle is None or service is None or
            lifecycle['epoch'] != service['lifecycle_epoch'] or
            service['controller_incarnation'] != expected_incarnation or
            service['controller_owner_epoch'] != expected_owner_epoch):
        raise OrdinaryLaunchBindingConflict(
            'Service owner changed before controller transfer.')
    if service['ordinary_launch_binding_mode'] == 'bound' and not capable:
        raise OrdinaryLaunchBindingConflict(
            'An incapable controller cannot own a bound service.')
    capability = _non_pool_capability_from_service(service)
    exact_capability_prefix = (
        True,
        NON_POOL_BINDING_PROTOCOL_VERSION,
        supported_non_pool_profile_set_digest(),
    )
    exact_capability_suffix = (NON_POOL_RECEIPT_PROTOCOL_VERSION,)
    supported_capabilities = {
        (*exact_capability_prefix, NON_POOL_CAPABILITY_COHORT_EPOCH,
         *exact_capability_suffix),
        (*exact_capability_prefix, NON_POOL_CAPABILITY_COHORT_EPOCH - 1,
         *exact_capability_suffix),
        (*exact_capability_prefix, NON_POOL_CAPABILITY_COHORT_EPOCH - 2,
         *exact_capability_suffix),
    }
    if capability[0] is True and capability not in supported_capabilities:
        raise OrdinaryLaunchBindingConflict(
            'Controller transfer encountered an unsupported generic cohort.')
    terminal_n2_takeover = capability == (
        *exact_capability_prefix,
        NON_POOL_CAPABILITY_COHORT_EPOCH - 2,
        *exact_capability_suffix,
    )
    if terminal_n2_takeover:
        if not _retained_graphs_have_terminal_absence_authority(
                connection, lifecycle, service):
            raise OrdinaryLaunchBindingConflict(
                'N-2 controller transfer requires exact terminal provider '
                'absence for every retained association-backed replica.')
    new_epoch = expected_owner_epoch + 1
    updated = connection.execute(
        sqlalchemy.update(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name == service_name,
            serve_state_schema.services_table.c.controller_pid ==
            service['controller_pid'],
            serve_state_schema.services_table.c.controller_ip ==
            service['controller_ip'],
            serve_state_schema.services_table.c.controller_incarnation ==
            expected_incarnation,
            serve_state_schema.services_table.c.controller_owner_epoch ==
            expected_owner_epoch).values(
                controller_incarnation=new_incarnation,
                controller_owner_epoch=new_epoch,
                ordinary_launch_binding_capable=capable,
                non_pool_launch_controller_incarnation=(
                    new_incarnation
                    if service.get('non_pool_launch_binding_capable') is True
                    else None),
                controller_pid=new_controller_pid,
                controller_ip=new_controller_ip,
                controller_port=None))
    if updated.rowcount != 1:
        raise OrdinaryLaunchBindingConflict('Service owner transfer lost CAS.')
    if not terminal_n2_takeover:
        try:
            capacity_authority.rebind_service_after_controller_takeover_in_connection(
                connection,
                service_name=service_name,
                controller_incarnation=new_incarnation,
                controller_owner_epoch=new_epoch)
        except capacity_admission.CapacityAdmissionError as error:
            raise OrdinaryLaunchBindingConflict(
                'Controller transfer could not preserve capacity authority.'
            ) from error
        route_projection.revoke_service_leases_in_session(
            connection, service_name, 'controller_owner_changed')
        connection.execute(
            sqlalchemy.update(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.service_name ==
                service_name,
                ordinary_launch_associations_table.c.resolution.in_(
                    tuple(value.value for value in UNSETTLED_RESOLUTIONS)),
                sqlalchemy.or_(
                    ordinary_launch_associations_table.c.
                    binding_protocol_version.is_(None),
                    sqlalchemy.and_(
                        ordinary_launch_associations_table.c.
                        binding_protocol_version ==
                        NON_POOL_BINDING_PROTOCOL_VERSION,
                        ordinary_launch_associations_table.c.
                        capability_cohort_epoch.in_((
                            NON_POOL_CAPABILITY_COHORT_EPOCH,
                            NON_POOL_CAPABILITY_COHORT_EPOCH - 1,
                        )), ordinary_launch_associations_table.c.
                        capability_profile_set_digest ==
                        supported_non_pool_profile_set_digest(),
                        ordinary_launch_associations_table.c.
                        receipt_protocol_version ==
                        NON_POOL_RECEIPT_PROTOCOL_VERSION))).
            values(owner_controller_incarnation=new_incarnation,
                   owner_controller_epoch=new_epoch,
                   owner_revision=(
                       ordinary_launch_associations_table.c.owner_revision + 1),
                   owner_transferred_at=(sqlalchemy.func.clock_timestamp()),
                   updated_at=sqlalchemy.func.clock_timestamp()))
    return _authority_from_service(service,
                                   controller_pid=new_controller_pid,
                                   controller_ip=new_controller_ip,
                                   controller_incarnation=new_incarnation,
                                   controller_owner_epoch=new_epoch,
                                   capable=capable)


def _controller_owner_pair(
    value: tuple[int | None, str | None],
    field_name: str,
) -> tuple[int | None, str | None]:
    if not isinstance(value, tuple) or len(value) != 2:
        raise ValueError(f'{field_name} must be a PID/IP pair.')
    controller_pid, controller_ip = value
    if (controller_pid is not None and
        (type(controller_pid) is not int or controller_pid < 1)):
        raise ValueError(f'{field_name} PID must be positive or None.')
    if controller_ip is not None and not isinstance(controller_ip, str):
        raise ValueError(f'{field_name} IP must be text or None.')
    return controller_pid, controller_ip


def _elected_recovery_version_in_connection(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
) -> int | None:
    versions = serve_state_schema.version_specs_table
    rows = connection.execute(
        sqlalchemy.select(versions.c.version, versions.c.quarantined_at,
                          versions.c.controller_applied_at).where(
                              versions.c.service_name == service_name,
                              versions.c.yaml_content.is_not(None)).order_by(
                                  versions.c.version.desc())).mappings().all()
    latest_applicable = next(
        (int(row['version']) for row in rows if row['quarantined_at'] is None),
        None)
    latest_quarantined = next((int(row['version'])
                               for row in rows
                               if row['quarantined_at'] is not None), None)
    latest_applied_applicable = next((int(row['version'])
                                      for row in rows
                                      if row['quarantined_at'] is None and
                                      row['controller_applied_at'] is not None),
                                     None)
    if (latest_quarantined is not None and
        (latest_applicable is None or latest_applicable < latest_quarantined)):
        return latest_applied_applicable
    return latest_applicable


def begin_service_teardown_if_owner(
    service_name: str,
    expected_service_hash: str,
    expected_parent_owner: tuple[int | None, str | None],
) -> ServiceTeardownResult:
    """Atomically classify binding mode and publish terminal admission.

    Generic provider effects own the shared launch-authority advisory guard for
    their retry loop. Reserved fill instead owns a durable execution/effect
    claim and requires exact guardian quiescence. Teardown must publish its
    terminal intent and deliver request cancellation before waiting for either
    exclusion proof, or the cancellation needed to end the provider loop is
    unreachable. This transaction uses the canonical lifecycle/service row
    locks to classify the exact binding epoch and close new admissions in one
    commit. A concurrent promotion therefore orders entirely before this
    transaction (and returns bound authority) or entirely after it (and is
    rejected by terminal status).

    Serve042 legacy mode is marked in the same transaction and needs no second
    status CAS. Stores without Serve042 return ``UNSUPPORTED`` without writing;
    callers retain their established legacy status transition there. A marked
    bound result grants the exact old authority needed to cancel and reduce
    associations before taking any exclusive advisory authority.
    """
    service_name = _nonempty(service_name, 'service_name')
    expected_service_hash = _nonempty(expected_service_hash,
                                      'expected_service_hash')
    expected_parent_owner = _controller_owner_pair(expected_parent_owner,
                                                   'expected_parent_owner')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        return ServiceTeardownResult(ServiceTeardownDisposition.UNSUPPORTED,
                                     None)
    with engine.begin() as connection:
        lifecycle = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table).where(
                    serve_state_schema.service_lifecycle_fences_table.c.name ==
                    service_name).with_for_update()).mappings().one_or_none()
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
        if lifecycle is None or service is None:
            raise OrdinaryLaunchBindingConflict(
                'Teardown lost the service lifecycle authority.')
        if (service['hash'] != expected_service_hash or
                lifecycle['epoch'] != service['lifecycle_epoch'] or
            (service['controller_pid'], service['controller_ip'])
                != expected_parent_owner):
            raise OrdinaryLaunchBindingConflict(
                'Teardown does not match the parent-owned service.')
        try:
            mode = BindingMode(str(service['ordinary_launch_binding_mode']))
        except ValueError as error:
            raise OrdinaryLaunchBindingConflict(
                'Teardown encountered an unknown binding mode.') from error
        if (mode == BindingMode.BOUND and
            (service['pool'] != 0 or
             service['ordinary_launch_binding_capable'] is not True)):
            raise OrdinaryLaunchBindingConflict(
                'A bound teardown requires a capable non-pool service.')
        binding_epoch = _nonnegative_int(
            service['ordinary_launch_binding_epoch'],
            'ordinary_launch_binding_epoch')
        updated = connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == service_name,
                serve_state_schema.services_table.c.hash ==
                expected_service_hash,
                serve_state_schema.services_table.c.controller_pid ==
                expected_parent_owner[0],
                serve_state_schema.services_table.c.controller_ip ==
                expected_parent_owner[1],
                serve_state_schema.services_table.c.lifecycle_epoch ==
                lifecycle['epoch'],
                serve_state_schema.services_table.c.ordinary_launch_binding_mode
                == mode.value, serve_state_schema.services_table.c.
                ordinary_launch_binding_epoch == binding_epoch).values(
                    status=serve_statuses.ServiceStatus.SHUTTING_DOWN.value))
        if updated.rowcount != 1:
            raise OrdinaryLaunchBindingConflict(
                'Teardown status transition lost its owner or binding CAS.')
        if mode == BindingMode.LEGACY:
            return ServiceTeardownResult(
                ServiceTeardownDisposition.MARKED_LEGACY, None)
        incarnation = _canonical_uuid(service['controller_incarnation'],
                                      'controller_incarnation')
        owner_epoch = _positive_int(service['controller_owner_epoch'],
                                    'controller_owner_epoch')
        authority = _authority_from_service(
            service,
            controller_pid=expected_parent_owner[0],
            controller_ip=expected_parent_owner[1],
            controller_incarnation=incarnation,
            controller_owner_epoch=owner_epoch,
            capable=True)
        return ServiceTeardownResult(ServiceTeardownDisposition.MARKED_BOUND,
                                     authority)


def claim_controller_incarnation(
    service_name: str,
    expected_service_hash: str,
    expected_parent_owner: tuple[int | None, str | None],
    incarnation_uuid: uuid.UUID | str,
    *,
    new_parent_owner: tuple[int | None, str | None] | None = None,
    expected_lifecycle_epoch: int | None = None,
    expected_status: serve_statuses.ServiceStatus | None = None,
    expected_recovery_version: int | None = None,
    wait_for_authority: bool = True,
) -> ControllerBindingAuthority | None:
    """Claim a fresh capable controller incarnation under exclusive authority.

    Local SQLite and pre-Serve042 stores retain the legacy PID/IP protocol and
    return ``None``.  A Serve042 PostgreSQL row either performs the exact
    service-plus-unresolved-association transfer or raises closed.
    """
    service_name = _nonempty(service_name, 'service_name')
    expected_service_hash = _nonempty(expected_service_hash,
                                      'expected_service_hash')
    expected_parent_owner = _controller_owner_pair(expected_parent_owner,
                                                   'expected_parent_owner')
    if new_parent_owner is None:
        new_parent_owner = expected_parent_owner
    else:
        new_parent_owner = _controller_owner_pair(new_parent_owner,
                                                  'new_parent_owner')
    if expected_lifecycle_epoch is not None:
        expected_lifecycle_epoch = _positive_int(expected_lifecycle_epoch,
                                                 'expected_lifecycle_epoch')
    if (expected_status is not None and
            not isinstance(expected_status, serve_statuses.ServiceStatus)):
        raise TypeError('expected_status must be a ServiceStatus or None.')
    if expected_recovery_version is not None:
        expected_recovery_version = _positive_int(expected_recovery_version,
                                                  'expected_recovery_version')
    if not isinstance(wait_for_authority, bool):
        raise TypeError('wait_for_authority must be bool.')
    incarnation = _canonical_uuid(incarnation_uuid, 'incarnation_uuid')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        return None
    authority_session = (
        serve_state.service_replica_launch_authority_write_session
        if wait_for_authority else
        serve_state.try_service_replica_launch_authority_write_session)
    with authority_session(service_name) as locked_session:
        if locked_session is None:
            raise OrdinaryLaunchBindingBusy(
                'Controller claim is waiting behind active provider work.')
        _, session = locked_session
        connection = session.connection()
        lifecycle = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table).where(
                    serve_state_schema.service_lifecycle_fences_table.c.name ==
                    service_name).with_for_update()).mappings().one_or_none()
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
        if lifecycle is None or service is None:
            raise OrdinaryLaunchBindingConflict(
                'Controller claim lost the service lifecycle authority.')
        if (service['hash'] != expected_service_hash or
                lifecycle['epoch'] != service['lifecycle_epoch'] or
            (service['controller_pid'], service['controller_ip'])
                != expected_parent_owner):
            raise OrdinaryLaunchBindingConflict(
                'Controller claim does not match the parent-owned service.')
        if (expected_lifecycle_epoch is not None and
                service['lifecycle_epoch'] != expected_lifecycle_epoch):
            raise OrdinaryLaunchBindingConflict(
                'Controller claim lifecycle fence changed.')
        if (expected_status is not None and
                service['status'] != expected_status.value):
            raise OrdinaryLaunchBindingConflict(
                'Controller claim status fence changed.')
        try:
            current_status = serve_statuses.ServiceStatus[str(
                service['status'])]
        except (KeyError, TypeError) as error:
            raise OrdinaryLaunchBindingConflict(
                'Controller claim encountered an unknown service status.'
            ) from error
        if (current_status in serve_statuses.ServiceStatus.
                replica_launch_blocking_statuses() and
                expected_status != current_status):
            raise OrdinaryLaunchBindingConflict(
                'Controller claim is blocked by terminal service status.')
        if (expected_recovery_version is not None and
                _elected_recovery_version_in_connection(
                    connection, service_name) != expected_recovery_version):
            raise OrdinaryLaunchBindingConflict(
                'Controller claim recovery-version fence changed.')
        mode = BindingMode(str(service['ordinary_launch_binding_mode']))
        if mode == BindingMode.BOUND and service[
                'ordinary_launch_binding_capable'] is not True:
            raise OrdinaryLaunchBindingConflict(
                'An incapable controller cannot own a bound service.')
        old_incarnation = service['controller_incarnation']
        old_epoch = int(service['controller_owner_epoch'])
        if incarnation == old_incarnation:
            raise OrdinaryLaunchBindingConflict(
                'Every controller startup requires a fresh incarnation UUID.')
        authority = transfer_service_owner_in_connection(
            connection,
            service_name=service_name,
            expected_incarnation=old_incarnation,
            expected_owner_epoch=old_epoch,
            new_incarnation=incarnation,
            new_controller_pid=new_parent_owner[0],
            new_controller_ip=new_parent_owner[1],
            capable=True)
        session.commit()
        return authority


def validate_controller_authority(
    authority: ControllerBindingAuthority | None,
    *,
    service_name: str,
    service_hash: str | None,
    controller_pid: int | None,
    controller_ip: str | None,
) -> ControllerBindingAuthority | None:
    """Validate the parent's exact claim before manager construction."""
    service_name = _nonempty(service_name, 'service_name')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        if authority is not None:
            raise OrdinaryLaunchBindingConflict(
                'Controller authority cannot be verified by this store.')
        return None
    if authority is None:
        raise OrdinaryLaunchBindingConflict(
            'Serve042 controller startup has no claimed incarnation.')
    if not isinstance(authority, ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority or None.')
    if (authority.service_name != service_name or
            authority.service_hash != service_hash or
            authority.controller_pid != controller_pid or
            authority.controller_ip != controller_ip or
            authority.capable is not True):
        raise OrdinaryLaunchBindingConflict(
            'Controller authority does not match its startup arguments.')
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table,
                serve_state_schema.service_lifecycle_fences_table.c.epoch.label(
                    '_fence_epoch')).join(
                        serve_state_schema.service_lifecycle_fences_table,
                        serve_state_schema.service_lifecycle_fences_table.c.name
                        == serve_state_schema.services_table.c.name).where(
                            serve_state_schema.services_table.c.name ==
                            service_name)).mappings().one_or_none()
    if row is None:
        raise OrdinaryLaunchBindingConflict(
            'Claimed controller service no longer exists.')
    try:
        current_mode = BindingMode(str(row['ordinary_launch_binding_mode']))
        current_status = serve_statuses.ServiceStatus[str(row['status'])]
        current_non_pool = _non_pool_capability_from_service(row)
    except (KeyError, TypeError, ValueError) as error:
        raise OrdinaryLaunchBindingConflict(
            'Claimed controller state is malformed.') from error
    if (row['hash'] != authority.service_hash or
            row['workspace'] != authority.service_workspace or
            row['lifecycle_epoch'] != authority.service_lifecycle_epoch or
            row['_fence_epoch'] != authority.service_lifecycle_epoch or
            row['controller_pid'] != authority.controller_pid or
            row['controller_ip'] != authority.controller_ip or
            row['controller_incarnation'] != authority.controller_incarnation or
            row['controller_owner_epoch'] != authority.controller_owner_epoch or
            row['ordinary_launch_binding_capable'] is not True or
            current_mode != authority.binding_mode or
            row['ordinary_launch_binding_epoch'] != authority.binding_epoch or
            current_non_pool != (authority.non_pool_capable,
                                 authority.non_pool_binding_protocol_version,
                                 authority.non_pool_profile_set_digest,
                                 authority.non_pool_capability_cohort_epoch,
                                 authority.non_pool_receipt_protocol_version) or
            current_status
            in serve_statuses.ServiceStatus.replica_launch_blocking_statuses()):
        raise OrdinaryLaunchBindingConflict(
            'Claimed controller authority is no longer current.')
    return authority


@contextlib.contextmanager
def refresh_controller_authority(
    previous_authority: ControllerBindingAuthority,
) -> Iterator[ControllerBindingAuthority]:
    """Refresh a live controller's binding mode under shared authority.

    Binding promotion and demotion intentionally advance the binding epoch
    without replacing the controller process.  Every other authority field is
    immutable for that process.  Keep the shared launch-authority guard held
    through the caller's use of the refreshed value so an exclusive owner or
    binding transition cannot race the caller's row persistence and thread
    registration.
    """
    if not isinstance(previous_authority, ControllerBindingAuthority):
        raise TypeError(
            'previous_authority must be ControllerBindingAuthority.')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        raise OrdinaryLaunchBindingUnavailable(
            'Controller authority cannot be refreshed by this store.')
    with serve_state.service_replica_launch_authority_guard(
            previous_authority.service_name) as guard:
        if not serve_state.service_replica_launch_authority_guard_is_valid(
                guard):
            raise OrdinaryLaunchBindingConflict(
                'Controller authority guard lost its database session.')
        with engine.connect() as connection:
            row = connection.execute(
                sqlalchemy.select(
                    serve_state_schema.services_table,
                    serve_state_schema.service_lifecycle_fences_table.c.epoch.
                    label('_fence_epoch')).join(
                        serve_state_schema.service_lifecycle_fences_table,
                        serve_state_schema.service_lifecycle_fences_table.c.name
                        == serve_state_schema.services_table.c.name).where(
                            serve_state_schema.services_table.c.name ==
                            previous_authority.service_name)).mappings(
                            ).one_or_none()
        if row is None:
            raise OrdinaryLaunchBindingConflict(
                'Controller authority service no longer exists.')
        try:
            current_mode = BindingMode(str(row['ordinary_launch_binding_mode']))
            current_epoch = int(row['ordinary_launch_binding_epoch'])
            current_status = serve_statuses.ServiceStatus[str(row['status'])]
            current_non_pool = _non_pool_capability_from_service(row)
        except (KeyError, TypeError, ValueError) as error:
            raise OrdinaryLaunchBindingConflict(
                'Controller authority state is malformed.') from error
        if (row['hash'] != previous_authority.service_hash or
                row['workspace'] != previous_authority.service_workspace or
                row['lifecycle_epoch']
                != previous_authority.service_lifecycle_epoch or
                row['_fence_epoch']
                != previous_authority.service_lifecycle_epoch or
                row['controller_pid'] != previous_authority.controller_pid or
                row['controller_ip'] != previous_authority.controller_ip or
                row['controller_incarnation']
                != previous_authority.controller_incarnation or
                row['controller_owner_epoch']
                != previous_authority.controller_owner_epoch or
                previous_authority.capable is not True or
                row['ordinary_launch_binding_capable'] is not True or
                current_epoch < previous_authority.binding_epoch or
            (current_epoch == previous_authority.binding_epoch and
             (current_mode != previous_authority.binding_mode or
              current_non_pool !=
              (previous_authority.non_pool_capable,
               previous_authority.non_pool_binding_protocol_version,
               previous_authority.non_pool_profile_set_digest,
               previous_authority.non_pool_capability_cohort_epoch,
               previous_authority.non_pool_receipt_protocol_version))) or
                current_status in serve_statuses.ServiceStatus.
                replica_launch_blocking_statuses()):
            raise OrdinaryLaunchBindingConflict(
                'Controller authority changed outside a binding transition.')
        refreshed = dataclasses.replace(
            previous_authority,
            binding_mode=current_mode,
            binding_epoch=current_epoch,
            non_pool_capable=current_non_pool[0],
            non_pool_binding_protocol_version=current_non_pool[1],
            non_pool_profile_set_digest=current_non_pool[2],
            non_pool_capability_cohort_epoch=current_non_pool[3],
            non_pool_receipt_protocol_version=current_non_pool[4])
        yield refreshed
        if not serve_state.service_replica_launch_authority_guard_is_valid(
                guard):
            raise OrdinaryLaunchBindingConflict(
                'Controller authority guard was lost during guarded work.')


def publish_controller_port_if_authority(
    authority: ControllerBindingAuthority,
    controller_port: int,
) -> bool:
    """Publish a ready port under an exact incarnation without advancing it."""
    if not isinstance(authority, ControllerBindingAuthority):
        raise TypeError('authority must be ControllerBindingAuthority.')
    controller_port = _positive_int(controller_port, 'controller_port')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        raise OrdinaryLaunchBindingUnavailable(
            'Controller authority cannot be published by this store.')
    with serve_state.service_replica_launch_authority_write_session(
            authority.service_name) as (_, session):
        connection = session.connection()
        lifecycle = connection.execute(
            sqlalchemy.select(
                serve_state_schema.service_lifecycle_fences_table).where(
                    serve_state_schema.service_lifecycle_fences_table.c.name ==
                    authority.service_name).with_for_update()).mappings(
                    ).one_or_none()
        result = connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                authority.service_name, serve_state_schema.services_table.c.hash
                == authority.service_hash,
                serve_state_schema.services_table.c.workspace ==
                authority.service_workspace,
                serve_state_schema.services_table.c.lifecycle_epoch ==
                authority.service_lifecycle_epoch,
                serve_state_schema.services_table.c.controller_pid ==
                authority.controller_pid,
                serve_state_schema.services_table.c.controller_ip ==
                authority.controller_ip,
                serve_state_schema.services_table.c.controller_incarnation ==
                authority.controller_incarnation,
                serve_state_schema.services_table.c.controller_owner_epoch ==
                authority.controller_owner_epoch,
                serve_state_schema.services_table.c.
                ordinary_launch_binding_capable.is_(True),
                serve_state_schema.services_table.c.
                non_pool_launch_binding_capable.is_(
                    authority.non_pool_capable), serve_state_schema.
                services_table.c.non_pool_launch_controller_incarnation == (
                    authority.controller_incarnation if
                    authority.non_pool_capable else None), serve_state_schema.
                services_table.c.non_pool_launch_binding_protocol_version ==
                authority.non_pool_binding_protocol_version, serve_state_schema.
                services_table.c.non_pool_launch_capability_profile_set_digest
                == authority.non_pool_profile_set_digest, serve_state_schema.
                services_table.c.non_pool_launch_capability_cohort_epoch ==
                authority.non_pool_capability_cohort_epoch, serve_state_schema.
                services_table.c.non_pool_launch_receipt_protocol_version ==
                authority.non_pool_receipt_protocol_version,
                serve_state_schema.services_table.c.ordinary_launch_binding_mode
                == authority.binding_mode.value,
                serve_state_schema.services_table.c.
                ordinary_launch_binding_epoch == authority.binding_epoch,
                serve_state_schema.services_table.c.status.not_in(
                    tuple(status.value
                          for status in serve_statuses.ServiceStatus.
                          replica_launch_blocking_statuses()))).values(
                              controller_port=controller_port))
        lifecycle_matches = (lifecycle is not None and lifecycle['epoch']
                             == authority.service_lifecycle_epoch)
        if result.rowcount != 1 or not lifecycle_matches:
            session.rollback()
            return False
        session.commit()
        return True


def _lock_transition_rows(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
) -> tuple[Mapping[str, Any], Mapping[str, Any], list[Mapping[str, Any]],
           list[Mapping[str, Any]]]:
    """Lock the complete Serve transition surface in canonical order."""
    lifecycle = connection.execute(
        sqlalchemy.select(
            serve_state_schema.service_lifecycle_fences_table).where(
                serve_state_schema.service_lifecycle_fences_table.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            service_name).with_for_update()).mappings().one_or_none()
    if lifecycle is None or service is None:
        raise OrdinaryLaunchBindingConflict('Service disappeared.')
    if lifecycle['epoch'] != service['lifecycle_epoch']:
        raise OrdinaryLaunchBindingConflict(
            'Service lifecycle changed before binding transition.')
    replicas = list(
        connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                service_name).order_by(
                    serve_state_schema.replicas_table.c.replica_id).
            with_for_update()).mappings())
    associations = list(
        connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.service_name ==
                service_name).order_by(
                    ordinary_launch_associations_table.c.association_id).
            with_for_update()).mappings())

    return lifecycle, service, replicas, associations


def _transition_barrier_passes(
    connection: sqlalchemy.engine.Connection,
    barrier: TransitionBarrier | bool,
    description: str,
) -> bool:
    """Evaluate a request-side barrier only after canonical Serve locks."""
    if callable(barrier):
        return barrier(connection) is True
    if barrier is False:
        return False
    if barrier is True:
        raise OrdinaryLaunchBindingUnavailable(
            f'A precomputed passing {description} cannot authorize a '
            'transition; provide a transaction-local callback.')
    raise TypeError(f'{description} must be a boolean or callable.')


def _has_no_retained_association_graphs(
    replicas: Sequence[Mapping[str, Any]],
    associations: Sequence[Mapping[str, Any]],
) -> bool:
    """Require every association-backed replica to retire before rotation.

    The immutable association keeps its origin cohort, while the service tuple
    changes during rotation.  Carrying even a terminal PROJECTED/ABSENT graph
    across that change would make its exact cleanup authority disagree with
    the service and strand the replica.  Historical associations are safe only
    after their exact replica record has been physically retired.
    """
    replica_records: set[tuple[int, str]] = set()
    try:
        for replica in replicas:
            info = _locked_replica_info(replica)
            replica_records.add(
                (int(replica['replica_id']), str(info.replica_record_id)))
    except (KeyError, TypeError, ValueError):
        return False
    try:
        association_records = {(int(association['replica_id']),
                                str(association['replica_record_id']))
                               for association in associations}
    except (KeyError, TypeError, ValueError):
        return False
    return replica_records.isdisjoint(association_records)


def _neutral_pre_effect_unknown_observation_is_inert(
    association: Mapping[str, Any],
    profile: NonPoolLaunchProfile,
) -> bool:
    """Validate a row-bound UNKNOWN observation after pre-effect quiescence.

    A provider observation cannot weaken a definitive NOT_STARTED effect
    boundary.  Older reconcilers nevertheless recorded UNKNOWN while
    adjudicating some already-quiesced pre-effect launches.  Treat that
    observation as neutral only when its canonical envelope is bound to this
    exact association and was recorded after executor quiescence.
    """
    observed_at = association.get('provider_evidence_observed_at')
    quiesced_at = association.get('execution_quiesced_at')
    payload = association.get('provider_evidence_payload')
    digest = association.get('provider_evidence_digest')
    if (not isinstance(observed_at, datetime.datetime) or
            not isinstance(quiesced_at, datetime.datetime) or
            observed_at < quiesced_at or not isinstance(payload, Mapping) or
            not isinstance(digest, str) or
            _SHA256_RE.fullmatch(digest) is None):
        return False
    expected_payload = {
        'association_id': str(association.get('association_id')),
        'cluster_name': association.get('cluster_name'),
        'probe_contract': 'immutable-provider-identity-v1',
        'profile_kind': profile.kind.value,
        'reason': 'profile-has-no-durable-provider-uid',
        'replica_record_id': str(association.get('replica_record_id')),
    }
    if dict(payload) != expected_payload:
        return False
    expected_digest = _canonical_sha256({
        'association_id': expected_payload['association_id'],
        'evidence': ProviderEvidence.UNKNOWN.value,
        'payload': expected_payload,
        'profile_digest': profile.digest,
    })
    return digest == expected_digest


def _replica_free_association_is_inert(association: Mapping[str, Any],) -> bool:
    """Validate history that no longer has a matching replica record.

    N-2 takeover deliberately leaves association ownership byte-stable.  A
    replica-free row is therefore safe to carry only after its effect is
    terminal, its projection and pin release are durable, and its closed
    protocol shape is internally consistent.  Normal PostgreSQL constraints
    enforce the same shapes for current writes; these checks also fail closed
    for malformed history that predates those constraints.
    """
    try:
        resolution = Resolution(str(association['resolution']))
        effect_phase = EffectPhase(str(association['effect_phase']))
        terminal_status = TerminalStatus(str(association['terminal_status']))
        execution_generation = association['terminal_execution_generation']
        quiescence_required = association['execution_quiescence_required']
        profile = _association_profile(association)
    except (KeyError, TypeError, ValueError, OrdinaryLaunchBindingConflict):
        return False
    terminal_cause = association.get('terminal_cause')
    projection_times = (
        association.get('projected_at'),
        association.get('pin_released_at'),
        association.get('tombstone_not_before'),
    )
    if (resolution not in SETTLED_RESOLUTIONS or
            not isinstance(terminal_cause, str) or not terminal_cause or
            len(terminal_cause) > 256 or
            type(execution_generation) is not int or execution_generation < 0 or
            type(quiescence_required) is not bool or
            any(not isinstance(value, datetime.datetime)
                for value in projection_times)):
        return False
    if quiescence_required:
        quiesced_generation = association.get('execution_quiesced_generation')
        quiesced_at = association.get('execution_quiesced_at')
        if (type(quiesced_generation) is not int or
                quiesced_generation != execution_generation or
                not isinstance(quiesced_at, datetime.datetime)):
            return False

    if resolution is Resolution.PRE_EFFECT_TERMINAL:
        cancel_reason = association.get('cancel_reason')
        cancel_requested_at = association.get('cancel_requested_at')
        cancel_shape_valid = bool(
            (cancel_reason is None and cancel_requested_at is None) or
            (isinstance(cancel_reason, str) and bool(cancel_reason) and
             len(cancel_reason) <= 128 and
             isinstance(cancel_requested_at, datetime.datetime)))
        if (effect_phase is not EffectPhase.NOT_STARTED or
                association.get('service_job_id') is not None or
                association.get('result_recorded_at') is not None or
                association.get('ambiguity_code') is not None or
                quiescence_required is not True or not cancel_shape_valid):
            return False
    elif effect_phase is EffectPhase.SERVICE_JOB_RECORDED:
        service_job_id = association.get('service_job_id')
        if type(service_job_id) is not int or service_job_id < 1:
            return False
    else:
        # Provider-absence projection is the only settled path without a
        # recorded service job.  It must retain exact post-quiescence ABSENT
        # evidence even though its replica record has already retired.
        observed_at = association.get('provider_evidence_observed_at')
        quiesced_at = association.get('execution_quiesced_at')
        if (profile is None or association.get('binding_protocol_version')
                != NON_POOL_BINDING_PROTOCOL_VERSION or
                association.get('reconciliation_outcome')
                != ReconciliationOutcome.PROJECTED.value or
                association.get('provider_evidence')
                != ProviderEvidence.ABSENT.value or
                association.get('service_job_id') is not None or
                quiescence_required is not True or execution_generation < 1 or
                not isinstance(observed_at, datetime.datetime) or
                not isinstance(quiesced_at, datetime.datetime) or
                observed_at < quiesced_at):
            return False
        if profile.kind is NonPoolLaunchProfileKind.RESERVED_FILL:
            if (effect_phase
                    not in (EffectPhase.NOT_STARTED, EffectPhase.PROVIDER_IO,
                            EffectPhase.SERVICE_JOB_IO) or
                    association.get('paid_capacity_pool_key') is not None):
                return False
        elif is_paid_provider_reconciliation_profile(profile.kind):
            pool_key = association.get('paid_capacity_pool_key')
            replacement_shape_matches = (
                paid_provider_reconciliation_pool_shape_matches(
                    profile.kind, pool_key))
            if (not replacement_shape_matches or
                    not is_paid_provider_reconciliation_phase(effect_phase) or
                    not ordinary_paid_provider_terminal_shape_matches(
                        terminal_status, terminal_cause, pool_key) or
                    not isinstance(pool_key, str) or not pool_key):
                return False
        else:
            return False

    if profile is None:
        return all(
            association.get(field) is None for field in (
                'binding_protocol_version',
                'reconciliation_outcome',
                'provider_evidence',
                'provider_evidence_observed_at',
                'provider_evidence_payload',
                'provider_evidence_digest',
            ))

    try:
        reconciliation_outcome = ReconciliationOutcome(
            str(association['reconciliation_outcome']))
        provider_evidence = ProviderEvidence(
            str(association['provider_evidence']))
    except (KeyError, TypeError, ValueError):
        return False
    expected_outcome = (ReconciliationOutcome.PRE_EFFECT_TERMINAL
                        if resolution is Resolution.PRE_EFFECT_TERMINAL else
                        ReconciliationOutcome.PROJECTED)
    capability_cohort = association.get('capability_cohort_epoch')
    capability_digest = association.get('capability_profile_set_digest')
    if (association.get('binding_protocol_version')
            != NON_POOL_BINDING_PROTOCOL_VERSION or
            type(capability_cohort) is not int or capability_cohort < 1 or
            not isinstance(capability_digest, str) or
            _SHA256_RE.fullmatch(capability_digest) is None or
            association.get('receipt_protocol_version')
            != NON_POOL_RECEIPT_PROTOCOL_VERSION or
            reconciliation_outcome is not expected_outcome):
        return False
    observed_at = association.get('provider_evidence_observed_at')
    evidence_payload = association.get('provider_evidence_payload')
    evidence_digest = association.get('provider_evidence_digest')
    if provider_evidence is ProviderEvidence.NOT_QUERIED:
        return (observed_at is None and evidence_payload is None and
                evidence_digest is None)
    if resolution is Resolution.PRE_EFFECT_TERMINAL:
        return bool(provider_evidence is ProviderEvidence.UNKNOWN and
                    _neutral_pre_effect_unknown_observation_is_inert(
                        association, profile))
    if not (isinstance(observed_at, datetime.datetime) and
            isinstance(evidence_payload, Mapping) and bool(evidence_payload) and
            isinstance(evidence_digest, str) and
            _SHA256_RE.fullmatch(evidence_digest) is not None):
        return False
    if (is_paid_provider_reconciliation_profile(profile.kind) and
            provider_evidence is ProviderEvidence.ABSENT):
        try:
            expected_payload, expected_digest = (
                _ordinary_paid_provider_evidence(
                    association, str(association['cluster_name']),
                    ProviderEvidence.ABSENT))
        except (KeyError, TypeError, ValueError, OrdinaryLaunchBindingConflict):
            return False
        return (evidence_payload == expected_payload and
                evidence_digest == expected_digest)
    return True


def settled_association_proves_execution_quiescence(
        association: Mapping[str, Any]) -> bool:
    """Return whether retained association history is safe for teardown.

    This is the shared read-side authority for both replica-free history and a
    pointerless current replica whose exact cancellation projection has
    already released its association pointer.
    """
    return _replica_free_association_is_inert(association)


def _final_deletion_graph_context(
    connection: sqlalchemy.engine.Connection,
    lifecycle: Mapping[str, Any],
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    association: Mapping[str, Any],
) -> BoundNonPoolLaunchContext | None:
    """Authenticate one current generic graph before final deletion."""
    try:
        current_epoch = service['lifecycle_epoch']
        if (type(current_epoch) is not int or
                lifecycle['epoch'] != current_epoch or
                association['service_lifecycle_epoch'] != current_epoch or
                service['name'] != association['service_name'] or
                service['hash'] != association['service_hash'] or
                service['workspace'] != association['service_workspace'] or
                service['ordinary_launch_binding_epoch']
                != association['service_binding_epoch'] or
                service['pool'] != 0 or
                replica['ordinary_launch_association_id'] is not None or
                not _replica_snapshot_matches_association(
                    replica, association, require_launch_authorized=False)):
            return None
        context = bound_context_from_association(association)
        if not isinstance(context, BoundNonPoolLaunchContext):
            return None
        if context.profile.kind is NonPoolLaunchProfileKind.RESERVED_FILL:
            _validate_reserved_fill_cleanup_profile_in_connection(
                connection, service, replica, context.profile)
        _validate_terminal_generic_cleanup_capability(
            service,
            capability_cohort_epoch=context.capability_cohort_epoch,
            capability_profile_set_digest=(
                context.capability_profile_set_digest),
            receipt_protocol_version=context.receipt_protocol_version)
    except (KeyError, TypeError, ValueError, OrdinaryLaunchBindingConflict):
        return None
    return context


def _pre_effect_terminal_retirement_matches_locked_rows(
    connection: sqlalchemy.engine.Connection,
    lifecycle: Mapping[str, Any],
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    association: Mapping[str, Any],
) -> bool:
    """Prove an inert no-effect receipt belongs to the locked replica."""
    return bool(
        _final_deletion_graph_context(connection, lifecycle, service, replica,
                                      association) and
        _replica_free_association_is_inert(association))


def _provider_clean_graph_matches_locked_rows(
    connection: sqlalchemy.engine.Connection,
    lifecycle: Mapping[str, Any],
    service: Mapping[str, Any],
    replica: Mapping[str, Any],
    association: Mapping[str, Any],
    proof: capacity_admission.FinalDeletionProviderCleanGraph,
) -> bool:
    """Validate one exact same-transaction Kueue retirement proof."""
    try:
        context = _final_deletion_graph_context(connection, lifecycle, service,
                                                replica, association)
        if not (isinstance(
                proof, capacity_admission.FinalDeletionProviderCleanGraph) and
                type(proof.transaction_id) is int and
                isinstance(context, BoundNonPoolLaunchContext) and
                context.profile.kind is NonPoolLaunchProfileKind.RESERVED_FILL
                and proof.service_name == service['name'] and
                proof.service_hash == service['hash'] and
                proof.service_lifecycle_epoch == service['lifecycle_epoch'] and
                proof.replica_id == association['replica_id'] and
                proof.replica_record_id == association['replica_record_id'] and
                proof.association_id == association['association_id'] and
                proof.association_updated_at == association['updated_at']):
            return False
    except (KeyError, TypeError, ValueError, OrdinaryLaunchBindingConflict):
        return False
    return True


def _lock_and_validate_retained_terminal_absence_authority(
    connection: sqlalchemy.engine.Connection,
    lifecycle: Mapping[str, Any],
    service: Mapping[str, Any],
    *,
    policy: _TerminalCensusPolicy,
    provider_clean_graphs: tuple[
        capacity_admission.FinalDeletionProviderCleanGraph, ...] = (),
) -> RetainedAuthorityCensus | None:
    """Lock and validate every retained graph for terminal N-2 takeover.

    The caller already owns lifecycle and service locks.  This census acquires
    every remaining row class once in canonical order: replicas, paid claims,
    then associations.  Validation consumes those locked mappings directly;
    it must not perform a later claim lock behind an association lock.
    """
    service_name = str(service['name'])
    replicas = list(
        connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                service_name).order_by(
                    serve_state_schema.replicas_table.c.replica_id).
            with_for_update()).mappings())
    claims = list(
        serve_state.lock_paid_capacity_claims_in_connection(
            connection, service_name=service_name))
    associations = list(
        connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.service_name ==
                service_name).order_by(
                    ordinary_launch_associations_table.c.association_id).
            with_for_update()).mappings())

    # The N-2 branch deliberately preserves capacity authority byte-for-byte.
    # It is therefore available only to a completely zero-live-claim terminal
    # census, including replicas whose association history is missing.
    if claims:
        return None
    if (not isinstance(provider_clean_graphs, tuple) or
        (policy is _TerminalCensusPolicy.N2_TRANSFER and
         provider_clean_graphs)):
        return None
    provider_clean_by_replica: dict[tuple[
        int, str], capacity_admission.FinalDeletionProviderCleanGraph] = {
            (proof.replica_id, str(proof.replica_record_id)): proof
            for proof in provider_clean_graphs
            if isinstance(proof,
                          capacity_admission.FinalDeletionProviderCleanGraph)
        }
    if len(provider_clean_by_replica) != len(provider_clean_graphs):
        return None
    if provider_clean_graphs:
        transaction_id = int(
            connection.execute(sqlalchemy.select(
                sqlalchemy.func.txid_current())).scalar_one())
        if any(proof.transaction_id != transaction_id
               for proof in provider_clean_graphs):
            return None

    try:
        replica_records: dict[tuple[int, str], Mapping[str, Any]] = {}
        for replica in replicas:
            # PROJECTED/ABSENT clears this pointer atomically.  A dangling,
            # mismatched, or merely unresolved pointer must never be mistaken
            # for the permitted no-graph shape.
            if replica['ordinary_launch_association_id'] is not None:
                return None
            info = _locked_replica_info(replica)
            key = (int(replica['replica_id']), str(info.replica_record_id))
            if key in replica_records:
                return None
            replica_records[key] = replica
        retained_associations: dict[tuple[int, str], list[Mapping[str,
                                                                  Any]]] = {}
        replica_free_associations: list[Mapping[str, Any]] = []
        for association in associations:
            key = (int(association['replica_id']),
                   str(association['replica_record_id']))
            if key in replica_records:
                retained_associations.setdefault(key, []).append(association)
            else:
                replica_free_associations.append(association)
    except (KeyError, TypeError, ValueError):
        return None

    # An empty service is the sole legitimate no-graph shape.  Every retained
    # replica row must otherwise resolve to exactly one current-record graph;
    # an unassociated or historically mismatched row may still need ordinary
    # takeover/recovery and cannot enter the non-mutating N-2 branch.
    if set(retained_associations) != set(replica_records):
        return None
    if any(not _replica_free_association_is_inert(association)
           for association in replica_free_associations):
        return None

    for key, candidates in retained_associations.items():
        if (len(candidates) != 1 or
                candidates[0].get('binding_protocol_version')
                != NON_POOL_BINDING_PROTOCOL_VERSION):
            return None
        candidate = candidates[0]
        provider_clean = provider_clean_by_replica.pop(key, None)
        if provider_clean is not None:
            if (policy is not _TerminalCensusPolicy.FINAL_DELETION or
                    not _provider_clean_graph_matches_locked_rows(
                        connection, lifecycle, service, replica_records[key],
                        candidate, provider_clean)):
                return None
            continue
        if (policy is _TerminalCensusPolicy.FINAL_DELETION and
                candidate.get('resolution')
                == Resolution.PRE_EFFECT_TERMINAL.value and
                _pre_effect_terminal_retirement_matches_locked_rows(
                    connection, lifecycle, service, replica_records[key],
                    candidate)):
            # No provider-effect boundary was crossed.  The replica is only a
            # retained logical record after Kueue/request retirement and can
            # be deleted without manufacturing an ABSENT provider receipt.
            continue
        try:
            projected, info = (
                _validate_projected_provider_absence_retirement_locked_rows(
                    connection, lifecycle, service, replica_records[key], (),
                    (candidate,)))
        except (OrdinaryLaunchBindingConflict, TypeError, ValueError):
            return None
        if (projected['association_id'] != candidate['association_id'] or
                not replica_has_projected_provider_absence_cleanup_marker(info)
           ):
            return None
    if provider_clean_by_replica:
        return None
    return RetainedAuthorityCensus(tuple(associations))


def _retained_graphs_have_terminal_absence_authority(
    connection: sqlalchemy.engine.Connection,
    lifecycle: Mapping[str, Any],
    service: Mapping[str, Any],
) -> bool:
    """Return whether complete retained name history is provider-safe."""
    return _lock_and_validate_retained_terminal_absence_authority(
        connection,
        lifecycle,
        service,
        policy=_TerminalCensusPolicy.N2_TRANSFER) is not None


def lock_retained_terminal_absence_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    lifecycle: Mapping[str, Any],
    service: Mapping[str, Any],
    *,
    provider_clean_graphs: tuple[
        capacity_admission.FinalDeletionProviderCleanGraph, ...] = (),
) -> RetainedAuthorityCensus | None:
    """Lock complete retained provider authority for final deletion.

    Deletion cannot assume a format-6 genesis receipt exists, so it repeats the
    complete same-name census that the next clean genesis would require.
    """
    return _lock_and_validate_retained_terminal_absence_authority(
        connection,
        lifecycle,
        service,
        policy=_TerminalCensusPolicy.FINAL_DELETION,
        provider_clean_graphs=provider_clean_graphs)


def promote_service_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    controller_incarnation: uuid.UUID,
    controller_owner_epoch: int,
    expected_binding_epoch: int,
    participant_barrier_passed: TransitionBarrier | bool,
    legacy_requests_drained: TransitionBarrier | bool,
) -> int:
    """Promote one non-pool service only after both external barriers pass."""
    _require_postgres(connection)
    controller_incarnation = _canonical_uuid(controller_incarnation,
                                             'controller_incarnation')
    # Serve042 installs every legacy service at epoch zero.  The first
    # promotion, including its exact lost-response retry, must therefore
    # accept zero as the source epoch.
    expected_binding_epoch = _nonnegative_int(expected_binding_epoch,
                                              'expected_binding_epoch')
    _, service, replicas, _ = _lock_transition_rows(connection, service_name)
    try:
        service_status = serve_statuses.ServiceStatus(str(service['status']))
    except ValueError as error:
        raise OrdinaryLaunchBindingConflict(
            'Binding promotion encountered an unknown service status.'
        ) from error
    if (service_status
            in serve_statuses.ServiceStatus.replica_launch_blocking_statuses()):
        raise OrdinaryLaunchBindingConflict(
            'Binding promotion is blocked by terminal service status.')
    if service['ordinary_launch_binding_mode'] == 'bound':
        if (service['ordinary_launch_binding_capable'] is not True or
                service['controller_incarnation'] != controller_incarnation or
                service['controller_owner_epoch'] != controller_owner_epoch or
                service['ordinary_launch_binding_epoch']
                != expected_binding_epoch + 1):
            raise OrdinaryLaunchBindingConflict(
                'Already-bound service belongs to different controller '
                'authority or binding epoch.')
        return int(service['ordinary_launch_binding_epoch'])
    if service['ordinary_launch_binding_epoch'] != expected_binding_epoch:
        raise OrdinaryLaunchBindingConflict(
            'Binding promotion source epoch changed before transition.')
    if (not _transition_barrier_passes(connection, participant_barrier_passed,
                                       'participant capability barrier') or
            not _transition_barrier_passes(connection, legacy_requests_drained,
                                           'legacy-request drain barrier')):
        raise OrdinaryLaunchBindingUnavailable(
            'Promotion requires participant capability and legacy drain.')
    pending = sum(replica['status'] in ('PENDING', 'PROVISIONING')
                  for replica in replicas)
    if (service['pool'] != 0 or
            service['ordinary_launch_binding_capable'] is not True or
            service['controller_incarnation'] != controller_incarnation or
            service['controller_owner_epoch'] != controller_owner_epoch or
            pending != 0):
        raise OrdinaryLaunchBindingConflict(
            'Service is not eligible for ordinary-launch binding promotion.')
    next_epoch = int(service['ordinary_launch_binding_epoch']) + 1
    connection.execute(
        sqlalchemy.update(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name == service_name).values(
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=next_epoch))
    return next_epoch


def promote_non_pool_launch_service_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    controller_incarnation: uuid.UUID,
    controller_owner_epoch: int,
    expected_binding_epoch: int,
    participant_barrier_passed: TransitionBarrier | bool,
    legacy_requests_drained: TransitionBarrier | bool,
) -> int:
    """Promote one bound service to the single protocol-v2 launch path.

    Promotion is deliberately per service. It advances the existing binding
    epoch, so every previously prepared v1 admission is fenced, and installs
    the complete generic capability tuple in the same transaction.
    """
    _require_postgres(connection)
    controller_incarnation = _canonical_uuid(controller_incarnation,
                                             'controller_incarnation')
    controller_owner_epoch = _positive_int(controller_owner_epoch,
                                           'controller_owner_epoch')
    expected_binding_epoch = _positive_int(expected_binding_epoch,
                                           'expected_binding_epoch')
    _, service, replicas, associations = _lock_transition_rows(
        connection, service_name)
    try:
        service_status = serve_statuses.ServiceStatus(str(service['status']))
    except ValueError as error:
        raise OrdinaryLaunchBindingConflict(
            'Generic launch promotion encountered an unknown service status.'
        ) from error
    if (service_status
            in serve_statuses.ServiceStatus.replica_launch_blocking_statuses()):
        raise OrdinaryLaunchBindingConflict(
            'Generic launch promotion is blocked by terminal service status.')

    if service['non_pool_launch_binding_capable'] is True:
        capability = _non_pool_capability_from_service(service)
        expected_capability = (True, NON_POOL_BINDING_PROTOCOL_VERSION,
                               supported_non_pool_profile_set_digest(),
                               NON_POOL_CAPABILITY_COHORT_EPOCH,
                               NON_POOL_RECEIPT_PROTOCOL_VERSION)
        if (service['controller_incarnation'] != controller_incarnation or
                service['controller_owner_epoch'] != controller_owner_epoch):
            raise OrdinaryLaunchBindingConflict(
                'Already-generic service belongs to different controller '
                'authority.')
        if capability == expected_capability:
            if (service['ordinary_launch_binding_epoch']
                    != expected_binding_epoch + 1):
                raise OrdinaryLaunchBindingConflict(
                    'Already-generic service belongs to a different binding '
                    'epoch.')
            return int(service['ordinary_launch_binding_epoch'])

        # A provider-effect semantic change rotates the complete advertised
        # cohort.  Keep the old tuple durable until all new participants have
        # survived the heartbeat/quiescence horizon and every old association
        # has settled.  The adjacent binding-epoch CAS fences prepared old
        # requests from the first new admission.
        previous_capability = (True, NON_POOL_BINDING_PROTOCOL_VERSION,
                               supported_non_pool_profile_set_digest(),
                               NON_POOL_CAPABILITY_COHORT_EPOCH - 1,
                               NON_POOL_RECEIPT_PROTOCOL_VERSION)
        if (capability != previous_capability or
                service['ordinary_launch_binding_mode']
                != BindingMode.BOUND.value or
                service['ordinary_launch_binding_epoch']
                != expected_binding_epoch):
            raise OrdinaryLaunchBindingConflict(
                'Generic launch capability rotation is not exact and '
                'adjacent.')
        if (not _transition_barrier_passes(
                connection, participant_barrier_passed,
                'generic participant capability rotation barrier') or
                not _transition_barrier_passes(
                    connection, legacy_requests_drained,
                    'prior-cohort request drain barrier')):
            raise OrdinaryLaunchBindingUnavailable(
                'Generic capability rotation requires the exact new fleet '
                'cohort and complete prior-cohort drain.')
        pending = [
            int(replica['replica_id'])
            for replica in replicas
            if replica['status'] in ('PENDING', 'PROVISIONING')
        ]
        unsettled = [
            str(association['association_id'])
            for association in associations
            if association['resolution'] in tuple(
                value.value for value in UNSETTLED_RESOLUTIONS)
        ]
        no_retained_graphs = _has_no_retained_association_graphs(
            replicas, associations)
        if (service['pool'] != 0 or pending or unsettled or
                not no_retained_graphs):
            raise OrdinaryLaunchBindingConflict(
                'Generic capability rotation requires no pending replicas or '
                'retained prior-cohort association graphs.')
        result = connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == service_name,
                serve_state_schema.services_table.c.controller_incarnation ==
                controller_incarnation,
                serve_state_schema.services_table.c.controller_owner_epoch ==
                controller_owner_epoch, serve_state_schema.services_table.c.
                ordinary_launch_binding_epoch == expected_binding_epoch,
                serve_state_schema.services_table.c.
                non_pool_launch_binding_capable.is_(True), serve_state_schema.
                services_table.c.non_pool_launch_capability_cohort_epoch ==
                NON_POOL_CAPABILITY_COHORT_EPOCH - 1).values(
                    ordinary_launch_binding_epoch=expected_binding_epoch + 1,
                    non_pool_launch_capability_cohort_epoch=
                    NON_POOL_CAPABILITY_COHORT_EPOCH))
        if result.rowcount != 1:
            raise OrdinaryLaunchBindingConflict(
                'Generic capability rotation lost its exact service CAS.')
        return expected_binding_epoch + 1

    _non_pool_capability_from_service(service)
    if (service['ordinary_launch_binding_mode'] != BindingMode.BOUND.value or
            service['ordinary_launch_binding_capable'] is not True or
            service['controller_incarnation'] != controller_incarnation or
            service['controller_owner_epoch'] != controller_owner_epoch or
            service['ordinary_launch_binding_epoch'] != expected_binding_epoch):
        raise OrdinaryLaunchBindingConflict(
            'Service is not under exact bound controller authority.')
    if (not _transition_barrier_passes(
            connection, participant_barrier_passed,
            'generic participant capability barrier') or
            not _transition_barrier_passes(
                connection, legacy_requests_drained,
                'protocol-v1 request drain barrier')):
        raise OrdinaryLaunchBindingUnavailable(
            'Generic promotion requires exact fleet capability and a '
            'protocol-v1 drain.')

    pending = [
        int(replica['replica_id'])
        for replica in replicas
        if replica['status'] in ('PENDING', 'PROVISIONING')
    ]
    active_v1 = [
        str(association['association_id'])
        for association in associations
        if association.get('binding_protocol_version') is None and
        association['resolution'] in tuple(
            value.value for value in UNSETTLED_RESOLUTIONS)
    ]
    unexpected_v2 = [
        str(association['association_id'])
        for association in associations
        if association.get('binding_protocol_version') is not None
    ]
    if service['pool'] != 0 or pending or active_v1 or unexpected_v2:
        raise OrdinaryLaunchBindingConflict(
            'Generic launch promotion requires a non-pool service with no '
            'pending replicas or active/mismatched binding associations.')

    next_epoch = int(service['ordinary_launch_binding_epoch']) + 1
    result = connection.execute(
        sqlalchemy.update(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name == service_name,
            serve_state_schema.services_table.c.controller_incarnation ==
            controller_incarnation,
            serve_state_schema.services_table.c.controller_owner_epoch ==
            controller_owner_epoch,
            serve_state_schema.services_table.c.ordinary_launch_binding_epoch ==
            expected_binding_epoch,
            serve_state_schema.services_table.c.non_pool_launch_binding_capable.
            is_(False)).values(
                ordinary_launch_binding_epoch=next_epoch,
                non_pool_launch_binding_capable=True,
                non_pool_launch_controller_incarnation=controller_incarnation,
                non_pool_launch_binding_protocol_version=
                NON_POOL_BINDING_PROTOCOL_VERSION,
                non_pool_launch_capability_profile_set_digest=
                supported_non_pool_profile_set_digest(),
                non_pool_launch_capability_cohort_epoch=
                NON_POOL_CAPABILITY_COHORT_EPOCH,
                non_pool_launch_receipt_protocol_version=
                NON_POOL_RECEIPT_PROTOCOL_VERSION))
    if result.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Generic launch promotion lost its exact service CAS.')
    return next_epoch


def demote_service_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    controller_incarnation: uuid.UUID,
    controller_owner_epoch: int,
    expected_binding_epoch: int,
    request_barrier_clear: TransitionBarrier | bool,
) -> int:
    """Demote only after every association is settled, unpinned, and clear.

    An exact already-legacy observation is a successful retry.  This matters
    when the demotion transaction committed but the controller died or lost the
    HTTP response before installing/refetching its process-local authority.
    """
    _require_postgres(connection)
    controller_incarnation = _canonical_uuid(controller_incarnation,
                                             'controller_incarnation')
    controller_owner_epoch = _positive_int(controller_owner_epoch,
                                           'controller_owner_epoch')
    expected_binding_epoch = _positive_int(expected_binding_epoch,
                                           'expected_binding_epoch')
    _, service, replicas, associations = _lock_transition_rows(
        connection, service_name)
    if (service['ordinary_launch_binding_capable'] is not True or
            service['controller_incarnation'] != controller_incarnation or
            service['controller_owner_epoch'] != controller_owner_epoch):
        raise OrdinaryLaunchBindingConflict(
            'Binding demotion belongs to different controller authority.')
    if service['ordinary_launch_binding_mode'] == 'legacy':
        if (service['ordinary_launch_binding_epoch']
                != expected_binding_epoch + 1):
            raise OrdinaryLaunchBindingConflict(
                'Binding demotion retry observed a different binding epoch.')
        return int(service['ordinary_launch_binding_epoch'])
    if service['ordinary_launch_binding_epoch'] != expected_binding_epoch:
        raise OrdinaryLaunchBindingConflict(
            'Binding demotion source epoch changed before transition.')
    if not _transition_barrier_passes(connection, request_barrier_clear,
                                      'request/pin quiescence barrier'):
        raise OrdinaryLaunchBindingUnavailable(
            'Demotion requires the request/pin quiescence barrier.')
    unresolved = sum(association['resolution'] in tuple(
        value.value
        for value in UNSETTLED_RESOLUTIONS) or
                     association['pin_released_at'] is None
                     for association in associations)
    pointers = sum(replica['ordinary_launch_association_id'] is not None
                   for replica in replicas)
    no_retained_graphs = _has_no_retained_association_graphs(
        replicas, associations)
    if unresolved or pointers or not no_retained_graphs:
        raise OrdinaryLaunchBindingConflict(
            'Bound associations remain active, unprojected, pinned, or '
            'backed by a retained replica.')
    next_epoch = int(service['ordinary_launch_binding_epoch']) + 1
    connection.execute(
        sqlalchemy.update(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name == service_name).values(
                ordinary_launch_binding_mode='legacy',
                ordinary_launch_binding_epoch=next_epoch,
                non_pool_launch_binding_capable=False,
                non_pool_launch_controller_incarnation=None,
                non_pool_launch_binding_protocol_version=None,
                non_pool_launch_capability_profile_set_digest=None,
                non_pool_launch_capability_cohort_epoch=None,
                non_pool_launch_receipt_protocol_version=None))
    return next_epoch


def demote_non_pool_launch_service_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    controller_incarnation: uuid.UUID,
    controller_owner_epoch: int,
    expected_binding_epoch: int,
    request_barrier_clear: TransitionBarrier | bool,
) -> int:
    """Rollback protocol v2 to the retained bound protocol-v1 path."""
    _require_postgres(connection)
    controller_incarnation = _canonical_uuid(controller_incarnation,
                                             'controller_incarnation')
    controller_owner_epoch = _positive_int(controller_owner_epoch,
                                           'controller_owner_epoch')
    expected_binding_epoch = _positive_int(expected_binding_epoch,
                                           'expected_binding_epoch')
    _, service, replicas, associations = _lock_transition_rows(
        connection, service_name)
    if (service['ordinary_launch_binding_mode'] != BindingMode.BOUND.value or
            service['ordinary_launch_binding_capable'] is not True or
            service['controller_incarnation'] != controller_incarnation or
            service['controller_owner_epoch'] != controller_owner_epoch):
        raise OrdinaryLaunchBindingConflict(
            'Generic rollback belongs to different controller authority.')
    if service['non_pool_launch_binding_capable'] is not True:
        _non_pool_capability_from_service(service)
        if service[
                'ordinary_launch_binding_epoch'] != expected_binding_epoch + 1:
            raise OrdinaryLaunchBindingConflict(
                'Generic rollback retry observed a different binding epoch.')
        return int(service['ordinary_launch_binding_epoch'])
    _non_pool_capability_from_service(service)
    if service['ordinary_launch_binding_epoch'] != expected_binding_epoch:
        raise OrdinaryLaunchBindingConflict(
            'Generic rollback source epoch changed before transition.')
    if not _transition_barrier_passes(connection, request_barrier_clear,
                                      'generic request/pin quiescence barrier'):
        raise OrdinaryLaunchBindingUnavailable(
            'Generic rollback requires request and pin quiescence.')
    unresolved = sum(
        association.get('binding_protocol_version') is not None and
        (association['resolution'] in tuple(value.value
                                            for value in UNSETTLED_RESOLUTIONS)
         or association['pin_released_at'] is None)
        for association in associations)
    pointers = sum(replica['ordinary_launch_association_id'] is not None
                   for replica in replicas)
    no_retained_graphs = _has_no_retained_association_graphs(
        replicas, associations)
    if unresolved or pointers or not no_retained_graphs:
        raise OrdinaryLaunchBindingConflict(
            'Generic associations remain active, unprojected, pinned, or '
            'backed by a retained replica.')
    next_epoch = int(service['ordinary_launch_binding_epoch']) + 1
    result = connection.execute(
        sqlalchemy.update(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name == service_name,
            serve_state_schema.services_table.c.controller_incarnation ==
            controller_incarnation,
            serve_state_schema.services_table.c.controller_owner_epoch ==
            controller_owner_epoch,
            serve_state_schema.services_table.c.ordinary_launch_binding_epoch ==
            expected_binding_epoch,
            serve_state_schema.services_table.c.non_pool_launch_binding_capable.
            is_(True)).values(
                ordinary_launch_binding_epoch=next_epoch,
                non_pool_launch_binding_capable=False,
                non_pool_launch_controller_incarnation=None,
                non_pool_launch_binding_protocol_version=None,
                non_pool_launch_capability_profile_set_digest=None,
                non_pool_launch_capability_cohort_epoch=None,
                non_pool_launch_receipt_protocol_version=None))
    if result.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Generic launch rollback lost its exact service CAS.')
    return next_epoch


def request_cancel_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    reason: str,
) -> int:
    reason = _nonempty(reason, 'cancel_reason')
    if len(reason) > 128:
        raise ValueError('cancel_reason must be at most 128 characters.')
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context)
    _validate_effect_rows(lifecycle,
                          service,
                          replica,
                          association,
                          context,
                          allowed_resolutions=frozenset(
                              {Resolution.BOUND, Resolution.CANCEL_REQUESTED}))
    if association['resolution'] == Resolution.CANCEL_REQUESTED.value:
        if association['cancel_reason'] != reason:
            raise OrdinaryLaunchBindingConflict(
                'Cancel intent replay used a different exact reason.')
        return int(association['owner_revision'])
    next_revision = int(association['owner_revision']) + 1
    result = connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id,
            ordinary_launch_associations_table.c.resolution ==
            Resolution.BOUND.value,
            ordinary_launch_associations_table.c.owner_revision == int(
                association['owner_revision'])).values(
                    resolution=Resolution.CANCEL_REQUESTED.value,
                    cancel_reason=reason,
                    cancel_requested_at=sqlalchemy.func.clock_timestamp(),
                    owner_revision=next_revision,
                    updated_at=sqlalchemy.func.clock_timestamp()))
    if result.rowcount != 1:
        raise OrdinaryLaunchBindingConflict('Cancel intent lost its CAS.')
    return next_revision


def commit_cancel_intent(context: BoundLaunchContext | Mapping[str, Any],
                         reason: str) -> int:
    """Commit an exact owner-fenced cancellation intent before API cancel."""
    if isinstance(context, Mapping):
        context = _parse_any_bound_launch_context(context)
    if not isinstance(context, BoundLaunchContext):
        raise ValueError('context must be a bound non-pool launch context.')
    engine = serve_state.get_database_engine()
    if not _serve042_supported(engine):
        raise OrdinaryLaunchBindingUnavailable(
            'Ordinary launch cancellation requires Serve042 PostgreSQL.')
    with serve_state.service_replica_launch_authority_write_session(
            context.service_name) as (_, session):
        revision = request_cancel_in_connection(session.connection(), context,
                                                reason)
        session.commit()
        return revision


def _terminal_values(evidence: TerminalEvidence) -> dict[str, Any]:
    if not isinstance(evidence, TerminalEvidence):
        raise ValueError('evidence must be TerminalEvidence.')
    if not isinstance(evidence.status, TerminalStatus):
        raise ValueError('terminal status must be closed.')
    cause = _nonempty(evidence.cause, 'terminal_cause')
    if len(cause) > 256:
        raise ValueError('terminal_cause must be at most 256 characters.')
    generation = _nonnegative_int(evidence.execution_generation,
                                  'execution_generation')
    if evidence.quiescence_required:
        if (evidence.quiesced_generation != generation or
                evidence.quiesced_at is None):
            raise ValueError('Required exact quiescence is not proven.')
    return {
        'terminal_status': evidence.status.value,
        'terminal_cause': cause,
        'terminal_execution_generation': generation,
        'execution_quiescence_required': evidence.quiescence_required,
        'execution_quiesced_generation': evidence.quiesced_generation,
        'execution_quiesced_at': evidence.quiesced_at,
    }


def record_terminal_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    evidence: TerminalEvidence,
) -> StartupClassification:
    """Copy immutable terminal evidence and classify the safe next action."""
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context)
    # Terminal reduction is permitted after a committed cancel intent too.
    if association['resolution'] == Resolution.CANCEL_REQUESTED.value:
        adjusted = dict(association)
        adjusted['resolution'] = Resolution.BOUND.value
        _validate_effect_rows(lifecycle, service, replica, adjusted, context)
    else:
        _validate_effect_rows(lifecycle, service, replica, association, context)
    values = _terminal_values(evidence)
    existing_terminal = association['terminal_status']
    if existing_terminal is not None:
        if any(association[key] != value for key, value in values.items()):
            raise OrdinaryLaunchBindingConflict(
                'Terminal evidence replay does not match the copied result.')
        if association['resolution'] == Resolution.AMBIGUOUS.value:
            return StartupClassification.AMBIGUOUS
        if association['resolution'] in (Resolution.PROJECTED.value,
                                         Resolution.PRE_EFFECT_TERMINAL.value):
            return StartupClassification.SETTLED
    phase = EffectPhase(str(association['effect_phase']))
    next_revision = int(association['owner_revision']) + 1
    values.update({
        'owner_revision': next_revision,
        'updated_at': sqlalchemy.func.clock_timestamp(),
    })
    if phase == EffectPhase.NOT_STARTED:
        # Projection and request-pin release must happen in the caller's one
        # cross-layer transaction before this becomes a settled state.
        connection.execute(
            sqlalchemy.update(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id ==
                context.association_id).values(**values))
        return StartupClassification.PRE_EFFECT_TERMINALIZE
    if phase == EffectPhase.SERVICE_JOB_RECORDED:
        values.update({
            'resolution': Resolution.RESULT_RECORDED.value,
            'result_recorded_at': sqlalchemy.func.clock_timestamp(),
        })
        if isinstance(context, BoundNonPoolLaunchContext):
            values['reconciliation_outcome'] = (
                ReconciliationOutcome.RESULT_RECORDED.value)
        connection.execute(
            sqlalchemy.update(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.association_id ==
                context.association_id).values(**values))
        return StartupClassification.REDUCE_TERMINAL
    values.update({
        'resolution': Resolution.AMBIGUOUS.value,
        'ambiguity_code': 'terminal-after-unrecorded-effect',
    })
    if isinstance(context, BoundNonPoolLaunchContext):
        values['reconciliation_outcome'] = (
            ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.value)
    connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id).values(**values))
    return StartupClassification.AMBIGUOUS


def mark_ambiguous_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    ambiguity_code: str,
) -> bool:
    ambiguity_code = _nonempty(ambiguity_code, 'ambiguity_code')
    if len(ambiguity_code) > 128:
        raise ValueError('ambiguity_code must be at most 128 characters.')
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context, require_paid_claim=False)
    _validate_effect_rows(lifecycle,
                          service,
                          replica,
                          association,
                          context,
                          allowed_resolutions=frozenset({
                              Resolution.BOUND, Resolution.CANCEL_REQUESTED,
                              Resolution.AMBIGUOUS
                          }))
    if association['resolution'] == Resolution.AMBIGUOUS.value:
        if association['ambiguity_code'] != ambiguity_code:
            raise OrdinaryLaunchBindingConflict(
                'Ambiguity replay used a different exact reason.')
        return False
    values: dict[str, Any] = {
        'resolution': Resolution.AMBIGUOUS.value,
        'ambiguity_code': ambiguity_code,
        'owner_revision': int(association['owner_revision']) + 1,
        'updated_at': sqlalchemy.func.clock_timestamp(),
    }
    if isinstance(context, BoundNonPoolLaunchContext):
        values['reconciliation_outcome'] = (
            ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.value)
    result = connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id,
            ordinary_launch_associations_table.c.resolution.in_(
                (Resolution.BOUND.value,
                 Resolution.CANCEL_REQUESTED.value))).values(**values))
    return result.rowcount == 1


def record_non_pool_provider_evidence(
    connection: sqlalchemy.engine.Connection,
    context: BoundNonPoolLaunchContext,
    authority: ControllerBindingAuthority,
    evidence: ProviderEvidence,
    payload: Mapping[str, Any],
    request_quiescence_validator: Callable[
        [sqlalchemy.engine.Connection, BoundNonPoolLaunchContext],
        TerminalEvidence | None],
) -> bool:
    """Record one fresh typed provider read for an ambiguous v2 action.

    The provider call happens before this transaction and under no manager or
    database lock.  This owner-fenced write never settles the association or
    authorizes cleanup by itself; it only makes the exact row's quarantine
    actionable and observable.
    """
    if not isinstance(context, BoundNonPoolLaunchContext):
        raise TypeError('context must be a BoundNonPoolLaunchContext.')
    if not isinstance(authority, ControllerBindingAuthority):
        raise TypeError('authority must be a ControllerBindingAuthority.')
    if (not isinstance(evidence, ProviderEvidence) or
            evidence == ProviderEvidence.NOT_QUERIED):
        raise ValueError('evidence must be a queried provider classification.')
    if not isinstance(payload, Mapping):
        raise TypeError('payload must be a mapping.')
    canonical_payload = dict(payload)
    evidence_digest = _canonical_sha256({
        'association_id': str(context.association_id),
        'evidence': evidence.value,
        'payload': canonical_payload,
        'profile_digest': context.profile.digest,
    })
    _require_postgres(connection)
    if not callable(request_quiescence_validator):
        raise TypeError('request_quiescence_validator must be callable.')
    association = lock_reduction_authority_in_connection(connection, context)
    if (association['resolution'] != Resolution.AMBIGUOUS.value or
            association['reconciliation_outcome']
            != ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.value):
        raise OrdinaryLaunchBindingConflict(
            'Provider evidence requires one post-effect ambiguous action.')
    expected_authority = (
        authority.service_name == association['service_name'] and
        authority.service_hash == association['service_hash'] and
        authority.service_workspace == association['service_workspace'] and
        authority.service_lifecycle_epoch
        == association['service_lifecycle_epoch'] and
        authority.binding_epoch == association['service_binding_epoch'] and
        authority.controller_incarnation
        == association['owner_controller_incarnation'] and
        authority.controller_owner_epoch
        == association['owner_controller_epoch'] and
        authority.retained_non_pool_settlement_allowed and
        association['binding_protocol_version']
        == authority.non_pool_binding_protocol_version and
        association['capability_cohort_epoch']
        == authority.non_pool_capability_cohort_epoch and
        association['capability_profile_set_digest']
        == authority.non_pool_profile_set_digest and
        association['receipt_protocol_version']
        == authority.non_pool_receipt_protocol_version)
    if not expected_authority:
        raise OrdinaryLaunchBindingConflict(
            'Provider evidence writer no longer owns this association.')
    # The provider read happened before this transaction. Requiring the exact
    # request generation to be terminal and quiescent now proves no admitted
    # executor can create a resource after an ABSENT observation is recorded.
    terminal_evidence = request_quiescence_validator(connection, context)
    if terminal_evidence is None:
        raise OrdinaryLaunchBindingConflict(
            'Provider evidence requires exact request quiescence.')
    terminal_values = _terminal_values(terminal_evidence)
    if (association['terminal_status'] is not None and
            any(association[key] != value
                for key, value in terminal_values.items())):
        raise OrdinaryLaunchBindingConflict(
            'Provider evidence conflicts with copied terminal evidence.')

    previous = ProviderEvidence(str(association['provider_evidence']))
    # Do not let a failed later read erase a stronger observation. Exact
    # absence and a retargeted physical identity are terminal evidence
    # classifications for this association; contradictions remain quarantined
    # for operator review rather than rewriting history.
    if previous in (ProviderEvidence.ABSENT, ProviderEvidence.REPLACED):
        existing_digest = association['provider_evidence_digest']
        if evidence != previous or existing_digest != evidence_digest:
            raise OrdinaryLaunchBindingConflict(
                'Provider evidence contradicts a terminal classification.')
        return False
    if (previous == ProviderEvidence.PRESENT and
            evidence == ProviderEvidence.UNKNOWN):
        return False
    values = {
        'provider_evidence': evidence.value,
        'provider_evidence_observed_at': sqlalchemy.func.clock_timestamp(),
        'provider_evidence_payload': canonical_payload,
        'provider_evidence_digest': evidence_digest,
        'owner_revision': int(association['owner_revision']) + 1,
        'updated_at': sqlalchemy.func.clock_timestamp(),
    }
    # Only the request layer can produce this callback. Copy its locked exact
    # receipt before any provider classification becomes projectable, so a
    # later Serve-only caller can compare evidence but never fabricate it.
    values.update(terminal_values)
    changed = connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id,
            ordinary_launch_associations_table.c.owner_revision ==
            association['owner_revision']).values(**values))
    if changed.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Provider evidence update lost its association CAS.')
    return True


def provider_absence_projection_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundNonPoolLaunchContext,
    terminal_evidence: TerminalEvidence,
    *,
    expected_provider_evidence_payload: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], Any]:
    """Validate one exact typed provider-absence settlement authority."""
    if not isinstance(context, BoundNonPoolLaunchContext):
        raise TypeError('context must be a BoundNonPoolLaunchContext.')
    values = _terminal_values(terminal_evidence)
    profile_kind = context.profile.kind
    lifecycle, service, replica, association = _lock_effect_rows(
        connection,
        context,
        require_paid_claim=is_paid_provider_reconciliation_profile(
            profile_kind))
    _validate_effect_rows(lifecycle,
                          service,
                          replica,
                          association,
                          context,
                          allowed_resolutions=frozenset({Resolution.AMBIGUOUS}))
    if (association['reconciliation_outcome']
            != ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.value or
            association['provider_evidence'] != ProviderEvidence.ABSENT.value):
        raise OrdinaryLaunchBindingConflict(
            'Provider absence cannot settle this launch profile or phase.')
    if profile_kind is NonPoolLaunchProfileKind.RESERVED_FILL:
        shape_matches = bool(
            association['effect_phase']
            in (EffectPhase.NOT_STARTED.value, EffectPhase.PROVIDER_IO.value,
                EffectPhase.SERVICE_JOB_IO.value) and
            association['paid_capacity_pool_key'] is None)
    elif is_paid_provider_reconciliation_profile(profile_kind):
        pool_key = association['paid_capacity_pool_key']
        replacement_shape_matches = (
            paid_provider_reconciliation_pool_shape_matches(
                profile_kind, pool_key))
        shape_matches = bool(replacement_shape_matches and
                             is_paid_provider_reconciliation_phase(
                                 association['effect_phase']) and
                             isinstance(pool_key, str) and pool_key and
                             association['service_job_id'] is None and
                             ordinary_paid_provider_terminal_shape_matches(
                                 association['terminal_status'],
                                 association['terminal_cause'], pool_key))
    else:
        shape_matches = False
    if not shape_matches:
        raise OrdinaryLaunchBindingConflict(
            'Provider absence cannot settle this launch profile or phase.')
    existing_terminal = association['terminal_status']
    if existing_terminal is None or any(
            association[key] != value for key, value in values.items()):
        raise OrdinaryLaunchBindingConflict(
            'Provider absence lacks the exact copied terminal evidence.')

    observed_at = association['provider_evidence_observed_at']
    if (observed_at is None or terminal_evidence.quiesced_at is None or
            observed_at < terminal_evidence.quiesced_at):
        raise OrdinaryLaunchBindingConflict(
            'Provider absence predates exact executor quiescence.')
    info = _locked_replica_info(replica)
    if profile_kind is NonPoolLaunchProfileKind.RESERVED_FILL:
        expected_payload, expected_digest = _reserved_fill_provider_evidence(
            association, info, ProviderEvidence.ABSENT)
    else:
        if (info.service_job_id is not None or info.is_spot is not True or
                info.is_zero_cost is not False or
                info.reserved_fill is not False or info.paid_capacity_pool_key
                != association['paid_capacity_pool_key']):
            raise OrdinaryLaunchBindingConflict(
                'Paid-provider absence lost its exact Spot profile.')
        if expected_provider_evidence_payload is None:
            raise OrdinaryLaunchBindingConflict(
                'Paid-provider absence requires a freshly extracted '
                'locked-request receipt.')
        # A replacement's predecessor/observation is mutable planner state and
        # may legitimately change after provider I/O.  Cleanup instead relies
        # on the immutable admitted association/profile, exact replica snapshot,
        # paid claim, retained request, pin, and quiescence receipt validated by
        # this transaction.  Ordinary paid has no predecessor and requires
        # byte-exact immutable admission-profile revalidation.
        if profile_kind is NonPoolLaunchProfileKind.ORDINARY_PAID:
            _validate_profile_authority_in_connection(
                connection,
                service,
                replica,
                context.profile,
                validate_paid_provider_start=False)
        expected_payload, expected_digest = _ordinary_paid_provider_evidence(
            association,
            info.cluster_name,
            ProviderEvidence.ABSENT,
            evidence_payload=expected_provider_evidence_payload)
    if association['provider_evidence_payload'] != expected_payload:
        raise OrdinaryLaunchBindingConflict(
            'Provider absence does not name the exact physical replica.')
    if association['provider_evidence_digest'] != expected_digest:
        raise OrdinaryLaunchBindingConflict(
            'Provider absence evidence digest is not canonical.')
    return dict(association), info


def _reserved_fill_provider_evidence(
    association: Mapping[str, Any],
    info: Any,
    evidence: ProviderEvidence,
) -> tuple[dict[str, Any], str]:
    """Build the single canonical physical-replica observation envelope."""
    payload = {
        'association_id': str(association['association_id']),
        'cluster_name': info.cluster_name,
        'kubernetes_context': info.reserved_fill_kubernetes_context,
        'physical_cluster_uid': info.reserved_fill_physical_cluster_uid,
        'probe_contract': 'kubernetes-physical-replica-presence-v1',
        'profile_kind': NonPoolLaunchProfileKind.RESERVED_FILL.value,
        'replica_record_id': str(association['replica_record_id']),
        'result': evidence.value,
    }
    digest = _canonical_sha256({
        'association_id': str(association['association_id']),
        'evidence': evidence.value,
        'payload': payload,
        'profile_digest': association['profile_digest'],
    })
    return payload, digest


def ordinary_paid_cluster_name_on_cloud(association: Mapping[str, Any]) -> str:
    """Derive the immutable AWS cluster name used by the bound request."""
    cluster_name = _nonempty(association.get('cluster_name'), 'cluster_name')
    tenant_scope = _nonempty(association.get('tenant_scope'), 'tenant_scope')
    return common_utils.make_cluster_name_on_cloud_for_user(
        cluster_name,
        max_length=aws_cloud.AWS.max_cluster_name_length(),
        cluster_name_hash_length=aws_cloud.AWS.cluster_name_hash_length(),
        user_hash=tenant_scope)


def ordinary_paid_gcp_provider_identity(
    association: Mapping[str, Any],
    *,
    project_id: str | None = None,
) -> dict[str, Any]:
    """Build the exact GCP allocation identity retained by a paid request.

    Fresh v2 pools freeze project and placement together.  ``project_id`` is
    accepted only to settle retained v1 rows whose immutable request predates
    project-scoped pools; it cannot override a v2 identity.
    """
    pool_key = association.get('paid_capacity_pool_key')
    if not isinstance(pool_key, str) or not pool_key:
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid GCP launch has no exact paid pool identity.')
    identity = paid_capacity.pool_key_payload(pool_key)
    provider_scope = (identity.get('provider_identity') if isinstance(
        identity, Mapping) else None)
    frozen_project_id = (provider_scope.get('gcp_project_id') if isinstance(
        provider_scope, Mapping) else None)
    if isinstance(identity, Mapping) and identity.get('version') == 2:
        if (not isinstance(frozen_project_id, str) or
                _GCP_PROJECT_ID_RE.fullmatch(frozen_project_id) is None or
                project_id is not None and project_id != frozen_project_id):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid GCP launch has no exact pool project ID.')
        project_id = frozen_project_id
    if (not isinstance(project_id, str) or
            _GCP_PROJECT_ID_RE.fullmatch(project_id) is None):
        raise OrdinaryLaunchBindingConflict(
            'Legacy ordinary-paid GCP launch has no retained project ID.')
    if (not isinstance(identity, Mapping) or identity.get('cloud') != 'gcp' or
            identity.get('version') not in (1, 2) or
            identity.get('use_spot') is not True or
            not isinstance(identity.get('workspace'), str) or
            not identity['workspace'] or
            not isinstance(identity.get('region'), str) or
            not identity['region'] or
            not isinstance(identity.get('zone'), str) or not identity['zone'] or
            not isinstance(identity.get('instance_type'), str) or
            not identity['instance_type'] or
            type(identity.get('num_nodes')) is not int or  # pylint: disable=unidiomatic-typecheck
            identity['num_nodes'] < 1):
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid GCP launch has no exact Spot placement.')
    cluster_name = _nonempty(association.get('cluster_name'), 'cluster_name')
    tenant_scope = _nonempty(association.get('tenant_scope'), 'tenant_scope')
    cluster_name_on_cloud = common_utils.make_cluster_name_on_cloud_for_user(
        cluster_name,
        max_length=gcp_cloud.GCP.max_cluster_name_length(),
        cluster_name_hash_length=gcp_cloud.GCP.cluster_name_hash_length(),
        user_hash=tenant_scope)
    return {
        'cluster_name_on_cloud': cluster_name_on_cloud,
        'instance_type': identity['instance_type'],
        'num_nodes': identity['num_nodes'],
        'project_id': project_id,
        'region': identity['region'],
        'use_spot': True,
        'workspace': identity['workspace'],
        'zone': identity['zone'],
    }


def ordinary_paid_gcp_project_id_from_pool_key(pool_key: object) -> str:
    """Decode the immutable project scope required for a fresh GCP effect."""
    if not isinstance(pool_key, str) or not pool_key:
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid GCP launch has no exact paid pool identity.')
    identity = paid_capacity.pool_key_payload(pool_key)
    provider_scope = (identity.get('provider_identity') if isinstance(
        identity, Mapping) else None)
    project_id = (provider_scope.get('gcp_project_id') if isinstance(
        provider_scope, Mapping) else None)
    if (not isinstance(identity, Mapping) or identity.get('cloud') != 'gcp' or
            identity.get('version') != 2 or not isinstance(project_id, str) or
            _GCP_PROJECT_ID_RE.fullmatch(project_id) is None):
        raise OrdinaryLaunchBindingConflict(
            'Fresh ordinary-paid GCP launch has no immutable project scope.')
    return project_id


def ordinary_paid_gcp_project_id(context: BoundNonPoolLaunchContext) -> str:
    """Decode the project frozen into one fresh paid GCP profile."""
    if (not isinstance(context, BoundNonPoolLaunchContext) or
            context.capability_cohort_epoch
            != NON_POOL_CAPABILITY_COHORT_EPOCH):
        raise OrdinaryLaunchBindingConflict(
            'Launch has no current paid GCP project authority.')
    reference = context.profile.authorization_reference
    if context.profile.kind is NonPoolLaunchProfileKind.ORDINARY_PAID:
        prefix = 'paid-capacity:'
        if not reference.startswith(prefix):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid authorization reference is malformed.')
        parts = reference[len(prefix):].split(':', 2)
        if len(parts) != 3 or parts[1] != str(context.replica_record_id):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid authorization names a different replica.')
        return ordinary_paid_gcp_project_id_from_pool_key(parts[2])
    if (context.profile.kind
            is not NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT):
        raise OrdinaryLaunchBindingConflict(
            'Profile has no paid GCP project authority.')
    match = re.fullmatch(
        r'unknown-capacity:[0-9a-f-]{36}:[0-9]+:gcp-project:'
        r'([a-z][a-z0-9-]{4,28}[a-z0-9])', reference)
    if match is None:
        raise OrdinaryLaunchBindingConflict(
            'Paid GCP replacement has no immutable project authority.')
    return match.group(1)


def ordinary_paid_gcp_resource_name_matches(
    provider_identity: Mapping[str, Any],
    resource_name: object,
) -> bool:
    """Whether one VM/disk name is in the exact generated cluster namespace."""
    cluster_name = provider_identity.get('cluster_name_on_cloud')
    if not isinstance(cluster_name, str) or not isinstance(resource_name, str):
        return False
    pattern = re.compile(
        rf'{re.escape(cluster_name)}-(?:head|worker)-[a-z0-9]{{8}}-compute')
    return pattern.fullmatch(resource_name) is not None


def ordinary_paid_aws_client_token(context: BoundNonPoolLaunchContext) -> str:
    """Return the versioned EC2 idempotency token for one paid association."""
    if not isinstance(context, BoundNonPoolLaunchContext):
        raise TypeError('context must be a bound non-pool launch context.')
    if not is_paid_provider_reconciliation_profile(context.profile.kind):
        raise OrdinaryLaunchBindingConflict(
            'Only paid reconciliation launches have an AWS create token.')
    if (context.capability_cohort_epoch
            < ORDINARY_PAID_AWS_CLIENT_TOKEN_COHORT_FLOOR or
            context.capability_cohort_epoch > NON_POOL_CAPABILITY_COHORT_EPOCH):
        raise OrdinaryLaunchBindingConflict(
            'Paid launch cohort has no supported AWS create token.')
    # EC2 ClientToken accepts at most 64 ASCII characters.  Hashing the stable
    # domain and canonical association UUID produces exactly 64 lowercase
    # ASCII characters and remains identical across executor generations.
    material = f'skypilot-serve-paid:{context.association_id}'.encode('ascii')
    return hashlib.sha256(material).hexdigest()


def ordinary_paid_aws_account_id_from_pool_key(pool_key: object) -> str:
    """Decode the server-observed AWS account frozen in a paid pool v2."""
    if not isinstance(pool_key, str) or not pool_key:
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid launch has no exact paid pool identity.')
    identity = paid_capacity.pool_key_payload(pool_key)
    provider_identity = (identity.get('provider_identity') if isinstance(
        identity, Mapping) else None)
    account_id = (provider_identity.get('aws_account_id') if isinstance(
        provider_identity, Mapping) else None)
    if (not isinstance(identity, Mapping) or identity.get('version') != 2 or
            identity.get('cloud') != 'aws' or not isinstance(account_id, str) or
            re.fullmatch(r'[0-9]{12}', account_id) is None):
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid AWS launch has no immutable account scope.')
    return account_id


def ordinary_paid_aws_account_id(context: BoundNonPoolLaunchContext) -> str:
    """Decode the account frozen into one bound paid authorization."""
    # This also enforces the token-capable cohort floor before the provider
    # scope is allowed to influence a new AWS effect.
    ordinary_paid_aws_client_token(context)
    reference = context.profile.authorization_reference
    if context.profile.kind is NonPoolLaunchProfileKind.ORDINARY_PAID:
        prefix = 'paid-capacity:'
        if not reference.startswith(prefix):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid authorization reference is malformed.')
        parts = reference[len(prefix):].split(':', 2)
        if len(parts) != 3 or parts[1] != str(context.replica_record_id):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid authorization names a different replica.')
        return ordinary_paid_aws_account_id_from_pool_key(parts[2])
    if context.profile.kind is not NonPoolLaunchProfileKind.UNKNOWN_CAPACITY_REPLACEMENT:
        raise OrdinaryLaunchBindingConflict(
            'Profile has no paid AWS account authority.')
    match = re.fullmatch(
        r'unknown-capacity:[0-9a-f-]{36}:[0-9]+:aws-account:([0-9]{12})',
        reference)
    if match is None:
        raise OrdinaryLaunchBindingConflict(
            'Paid AWS replacement has no immutable account authority.')
    return match.group(1)


def ordinary_paid_aws_provider_identity(
    association: Mapping[str, Any],
    *,
    credential_profile: str | None,
) -> dict[str, Any]:
    """Build the exact AWS census identity retained by a paid request."""
    if credential_profile is not None and (
            not isinstance(credential_profile, str) or not credential_profile):
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid AWS launch has an invalid credential profile.')
    pool_key = association.get('paid_capacity_pool_key')
    if not isinstance(pool_key, str) or not pool_key:
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid AWS launch has no exact paid pool identity.')
    pool_identity = paid_capacity.pool_key_payload(pool_key)
    provider_identity = (pool_identity.get('provider_identity') if isinstance(
        pool_identity, Mapping) else None)
    aws_account_id = (provider_identity.get('aws_account_id') if isinstance(
        provider_identity, Mapping) else None)
    if (not isinstance(pool_identity, Mapping) or
            pool_identity.get('version') != 2 or
            pool_identity.get('cloud') != 'aws' or
            pool_identity.get('use_spot') is not True or
            not isinstance(pool_identity.get('workspace'), str) or
            not pool_identity['workspace'] or
            not isinstance(pool_identity.get('region'), str) or
            not pool_identity['region'] or
            not isinstance(pool_identity.get('zone'), str) or
            not pool_identity['zone'] or
            not isinstance(pool_identity.get('instance_type'), str) or
            not pool_identity['instance_type'] or
            type(pool_identity.get('num_nodes')) is not int or  # pylint: disable=unidiomatic-typecheck
            pool_identity['num_nodes'] < 1
            or not isinstance(aws_account_id, str)
            or re.fullmatch(r'[0-9]{12}', aws_account_id) is None):
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid AWS launch has no exact Spot placement.')
    context = bound_context_from_association(association)
    if not isinstance(context, BoundNonPoolLaunchContext):
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid AWS launch lost its typed association.')
    return {
        'aws_account_id': aws_account_id,
        'client_token': ordinary_paid_aws_client_token(context),
        'cluster_name_on_cloud':
            ordinary_paid_cluster_name_on_cloud(association),
        'credential_profile': credential_profile,
        'instance_type': pool_identity['instance_type'],
        'num_nodes': pool_identity['num_nodes'],
        'region': pool_identity['region'],
        'use_spot': True,
        'workspace': pool_identity['workspace'],
        'zone': pool_identity['zone'],
    }


def _ordinary_paid_provider_evidence(
    association: Mapping[str, Any],
    cluster_name: str,
    evidence: ProviderEvidence,
    *,
    evidence_payload: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], str]:
    """Validate one canonical exact paid-provider evidence envelope."""
    try:
        profile_kind = NonPoolLaunchProfileKind(
            str(association.get('profile_kind')))
    except ValueError as error:
        raise OrdinaryLaunchBindingConflict(
            'Paid-provider evidence has an unknown profile kind.') from error
    if not is_paid_provider_reconciliation_profile(profile_kind):
        raise OrdinaryLaunchBindingConflict(
            'Profile does not authorize paid-provider evidence.')
    candidate_payload = (association.get('provider_evidence_payload')
                         if evidence_payload is None else evidence_payload)
    if not isinstance(candidate_payload, Mapping):
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid provider absence has no typed evidence envelope.')
    probe_contract = candidate_payload.get('probe_contract')
    if probe_contract == 'aws-client-token-instance-presence-v1':
        if evidence not in (ProviderEvidence.ABSENT, ProviderEvidence.PRESENT):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid AWS evidence must prove presence or absence.')
        expected_keys = {
            'association_id', 'cluster_name', 'instances', 'probe_contract',
            'profile_kind', 'provider_identity', 'replica_record_id', 'result'
        }
        provider_identity = candidate_payload.get('provider_identity')
        credential_profile = (provider_identity.get('credential_profile') if
                              isinstance(provider_identity, Mapping) else None)
        expected_identity = ordinary_paid_aws_provider_identity(
            association, credential_profile=credential_profile)
        instances = candidate_payload.get('instances')
        if not isinstance(instances, list):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid AWS provider evidence has no instance list.')
        allowed_states = {
            'pending', 'running', 'shutting-down', 'terminated', 'stopping',
            'stopped'
        }
        instance_keys = {
            'availability_zone', 'client_token', 'cluster_name_on_cloud',
            'instance_id', 'instance_type', 'market', 'state'
        }
        canonical_instances = True
        for instance in instances:
            if (not isinstance(instance, Mapping) or
                    set(instance) != instance_keys or
                    instance.get('availability_zone')
                    != expected_identity['zone'] or instance.get('client_token')
                    != expected_identity['client_token'] or
                    instance.get('cluster_name_on_cloud')
                    != expected_identity['cluster_name_on_cloud'] or
                    not isinstance(instance.get('instance_id'), str) or
                    not instance['instance_id'] or instance.get('instance_type')
                    != expected_identity['instance_type'] or
                    instance.get('market') != 'spot' or
                    instance.get('state') not in allowed_states):
                canonical_instances = False
                break
        live_instances = ([
            instance for instance in instances
            if instance['state'] != 'terminated'
        ] if canonical_instances else [])
        if (set(candidate_payload) != expected_keys or
                not isinstance(provider_identity, Mapping) or
                dict(provider_identity) != expected_identity or
                not canonical_instances or
                len(instances) > expected_identity['num_nodes'] or
                instances != sorted(instances,
                                    key=lambda item: item['instance_id']) or
                len({instance['instance_id'] for instance in instances
                    }) != len(instances) or
            (evidence is ProviderEvidence.ABSENT and live_instances) or
            (evidence is ProviderEvidence.PRESENT and not live_instances)):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid AWS provider evidence is not canonical.')
        payload = {
            'association_id': str(association['association_id']),
            'cluster_name': cluster_name,
            'instances': [dict(instance) for instance in instances],
            'probe_contract': 'aws-client-token-instance-presence-v1',
            'profile_kind': profile_kind.value,
            'provider_identity': expected_identity,
            'replica_record_id': str(association['replica_record_id']),
            'result': evidence.value,
        }
        if dict(candidate_payload) != payload:
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid AWS provider evidence changed its allocation.')
        digest = _canonical_sha256({
            'association_id': str(association['association_id']),
            'evidence': evidence.value,
            'payload': payload,
            'profile_digest': association['profile_digest'],
        })
        return payload, digest

    if probe_contract == 'gcp-vm-disk-operation-presence-v1':
        if evidence not in (ProviderEvidence.ABSENT, ProviderEvidence.PRESENT):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid GCP evidence must prove presence or absence.')
        expected_keys = {
            'association_id', 'cluster_name', 'create_operation_targets',
            'disk_ids', 'instance_ids', 'probe_contract', 'profile_kind',
            'provider_identity', 'replica_record_id', 'result'
        }
        provider_identity = candidate_payload.get('provider_identity')
        if not isinstance(provider_identity, Mapping):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid GCP provider evidence has no identity.')
        project_id = provider_identity.get('project_id')
        if not isinstance(project_id, str):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid GCP provider evidence has no project identity.')
        expected_identity = ordinary_paid_gcp_provider_identity(
            association, project_id=project_id)
        instance_ids = candidate_payload.get('instance_ids')
        disk_ids = candidate_payload.get('disk_ids')
        create_targets = candidate_payload.get('create_operation_targets')
        if (not isinstance(create_targets, Mapping) or
                set(create_targets) != {'failed', 'inflight', 'succeeded'}):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid GCP provider evidence has no canonical create '
                'operation targets.')
        operation_target_lists = tuple(
            create_targets.get(key)
            for key in ('failed', 'inflight', 'succeeded'))
        if (set(candidate_payload) != expected_keys or
                dict(provider_identity) != expected_identity or
                not isinstance(instance_ids, list) or
                any(not ordinary_paid_gcp_resource_name_matches(
                    expected_identity, instance_id)
                    for instance_id in instance_ids) or
                instance_ids != sorted(set(instance_ids)) or
                not isinstance(disk_ids, list) or
                any(not ordinary_paid_gcp_resource_name_matches(
                    expected_identity, disk_id) for disk_id in disk_ids) or
                disk_ids != sorted(set(disk_ids)) or
                any(not isinstance(targets, list)
                    for targets in operation_target_lists) or
                any(not ordinary_paid_gcp_resource_name_matches(
                    expected_identity, target)
                    for targets in operation_target_lists
                    for target in (targets or [])) or
                any(targets != sorted(set(targets or []))
                    for targets in operation_target_lists) or
                len(set().union(*(set(targets or [])
                                  for targets in operation_target_lists)))
                != sum(
                    len(targets or []) for targets in operation_target_lists) or
            (evidence is ProviderEvidence.ABSENT and
             (instance_ids or disk_ids or operation_target_lists[1])) or
            (evidence is ProviderEvidence.PRESENT and
             not (instance_ids or disk_ids))):
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid GCP provider evidence is not canonical.')
        payload = {
            'association_id': str(association['association_id']),
            'cluster_name': cluster_name,
            'create_operation_targets': dict(create_targets),
            'disk_ids': disk_ids,
            'instance_ids': instance_ids,
            'probe_contract': 'gcp-vm-disk-operation-presence-v1',
            'profile_kind': profile_kind.value,
            'provider_identity': expected_identity,
            'replica_record_id': str(association['replica_record_id']),
            'result': evidence.value,
        }
        if dict(candidate_payload) != payload:
            raise OrdinaryLaunchBindingConflict(
                'Ordinary-paid GCP provider evidence changed its allocation.')
        digest = _canonical_sha256({
            'association_id': str(association['association_id']),
            'evidence': evidence.value,
            'payload': payload,
            'profile_digest': association['profile_digest'],
        })
        return payload, digest

    if (profile_kind is not NonPoolLaunchProfileKind.ORDINARY_PAID or
            evidence is not ProviderEvidence.ABSENT):
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid negative acknowledgement must prove absence.')
    receipt = candidate_payload.get('receipt')
    expected_cluster_name_on_cloud = ordinary_paid_cluster_name_on_cloud(
        association)
    context = bound_context_from_association(association)
    if not isinstance(context, BoundNonPoolLaunchContext):
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid provider absence lost its typed association.')
    expected_client_token = ordinary_paid_aws_client_token(context)
    expected_aws_account_id = ordinary_paid_aws_account_id_from_pool_key(
        association.get('paid_capacity_pool_key'))
    canonical_receipt = capacity_policy.validate_provider_negative_ack(
        receipt,
        cluster_name=expected_cluster_name_on_cloud,
        client_token=expected_client_token,
        expected_aws_account_id=expected_aws_account_id)
    if canonical_receipt is None or receipt != canonical_receipt:
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid provider absence receipt is not canonical.')
    pool_identity = paid_capacity.pool_key_payload(
        str(association.get('paid_capacity_pool_key')))
    if (pool_identity is None or pool_identity['cloud'] != 'aws' or
            pool_identity['use_spot'] is not True or
            pool_identity['region'] != canonical_receipt['region'] or
            pool_identity['zone'] != canonical_receipt['availability_zone'] or
            pool_identity['instance_type'] != canonical_receipt['instance_type']
            or
            pool_identity['num_nodes'] != canonical_receipt['requested_count']):
        raise OrdinaryLaunchBindingConflict(
            'Ordinary-paid provider absence changed its paid placement.')
    payload = {
        'association_id': str(association['association_id']),
        'cluster_name': cluster_name,
        'probe_contract': 'aws-run-instances-negative-ack-v1',
        'profile_kind': NonPoolLaunchProfileKind.ORDINARY_PAID.value,
        'receipt': canonical_receipt,
        'replica_record_id': str(association['replica_record_id']),
        'result': ProviderEvidence.ABSENT.value,
    }
    digest = _canonical_sha256({
        'association_id': str(association['association_id']),
        'evidence': evidence.value,
        'payload': payload,
        'profile_digest': association['profile_digest'],
    })
    return payload, digest


def _replica_has_provider_cleanup_marker(
    replica_info: Any,
    allowed_down_statuses: set[common_utils.ProcessStatus],
) -> bool:
    """Whether one ReplicaInfo has the exact immediate-cleanup shape."""
    status = getattr(replica_info, 'status_property', None)
    if status is None:
        return False
    reserved_shape = bool(
        getattr(replica_info, 'reserved_fill', None) is True and
        getattr(replica_info, 'is_zero_cost', None) is True and
        getattr(replica_info, 'paid_capacity_pool_key', None) is None)
    pool_key = getattr(replica_info, 'paid_capacity_pool_key', None)
    paid_provider_shape = bool(
        getattr(replica_info, 'reserved_fill', None) is False and
        getattr(replica_info, 'is_zero_cost', None) is False and
        getattr(replica_info, 'is_spot', None) is True and
        isinstance(pool_key, str) and bool(pool_key) and
        (paid_capacity.pool_key_payload(pool_key) or
         {}).get('cloud') in ('aws', 'gcp'))
    return bool(
        (reserved_shape or paid_provider_shape) and
        getattr(replica_info, 'service_job_id', None) is None and
        getattr(replica_info, 'zero_cost_materialization_sequence',
                None) is None and
        status.sky_launch_status == common_utils.ProcessStatus.INTERRUPTED and
        status.sky_down_status in allowed_down_statuses and
        status.service_ready_now is False and status.is_scale_down is True and
        status.preempted is False and status.purged is False and
        status.failed_spot_availability is False and
        status.wait_for_idle_before_termination is False and
        status.drain_cap_seconds == 0 and status.drain_started_at is None and
        status.logical_retirement_version is None and
        status.logical_retirement_controller_epoch is None and
        status.logical_retirement_generation is None and
        status.logical_retirement_target_capacity is None and
        status.logical_retirement_confirmed_generation is None and
        status.logical_retirement_bounded_deadline is False and
        status.logical_retirement_committed is False)


def replica_has_provider_present_cleanup_marker(
    replica_info: Any,
    *,
    require_scheduled: bool = False,
) -> bool:
    """Whether one ReplicaInfo is the closed immediate-cleanup marker."""
    allowed_down_statuses = ({common_utils.ProcessStatus.SCHEDULED}
                             if require_scheduled else {
                                 common_utils.ProcessStatus.SCHEDULED,
                                 common_utils.ProcessStatus.RUNNING,
                                 common_utils.ProcessStatus.FAILED,
                             })
    return _replica_has_provider_cleanup_marker(replica_info,
                                                allowed_down_statuses)


def replica_has_projected_provider_absence_cleanup_marker(
        replica_info: Any) -> bool:
    """Whether a row is the closed DB-only post-ABSENT retirement marker."""
    if _replica_has_provider_cleanup_marker(
            replica_info, {
                common_utils.ProcessStatus.SCHEDULED,
                common_utils.ProcessStatus.RUNNING,
                common_utils.ProcessStatus.FAILED,
                common_utils.ProcessStatus.SUCCEEDED,
            }):
        # Reserved fill uses this same immediate-cleanup marker before and
        # after projection; the settled association distinguishes the latter.
        # A purge may persist SUCCEEDED after the exact ABSENT projection and
        # before final row deletion.  Provider-present consumers deliberately
        # keep rejecting that completed state.
        return True
    status = getattr(replica_info, 'status_property', None)
    pool_key = getattr(replica_info, 'paid_capacity_pool_key', None)
    if status is None:
        return False
    reserved_1516_absence_candidate = bool(
        getattr(replica_info, 'reserved_fill', None) is True and
        getattr(replica_info, 'is_zero_cost', None) is True and
        pool_key is None and
        getattr(replica_info, 'service_job_id', None) is None and
        getattr(replica_info, 'zero_cost_materialization_sequence',
                None) is None and
        status.sky_launch_status == common_utils.ProcessStatus.FAILED and
        status.sky_down_status == common_utils.ProcessStatus.FAILED and
        status.user_app_failed is False and
        status.service_ready_now is False and
        status.first_ready_time is None and status.is_scale_down is False and
        status.preempted is False and status.purged is False and
        status.failed_spot_availability is False and
        status.wait_for_idle_before_termination is False and
        status.drain_cap_seconds is None and status.drain_started_at is None and
        status.logical_retirement_version is None and
        status.logical_retirement_controller_epoch is None and
        status.logical_retirement_generation is None and
        status.logical_retirement_target_capacity is None and
        status.logical_retirement_confirmed_generation is None and
        status.logical_retirement_bounded_deadline is False and
        status.logical_retirement_committed is False)
    if reserved_1516_absence_candidate:
        # Release 1.1.1516 preserved this exact FAILED/FAILED shape after
        # projection. Current writers use the single marker above. This N-1
        # candidate alone authorizes no deletion or provider action: every
        # consumer must revalidate the settled exact association and
        # post-quiescence ABSENT evidence before retirement. The removal path
        # independently revalidates its owner, record, and Kueue fences.
        return True
    # A later generic purge can persist SUCCEEDED after this provider-ABSENT
    # shape committed. Consumers still revalidate the exact settled evidence.
    paid_absence_down_status = status.sky_down_status in (
        None, common_utils.ProcessStatus.SUCCEEDED)
    return bool(
        getattr(replica_info, 'reserved_fill', None) is False and
        getattr(replica_info, 'is_zero_cost', None) is False and
        getattr(replica_info, 'is_spot', None) is True and
        getattr(replica_info, 'service_job_id', None) is None and
        isinstance(pool_key, str) and bool(pool_key) and
        status.sky_launch_status == common_utils.ProcessStatus.FAILED and
        paid_absence_down_status and status.service_ready_now is False and
        status.is_scale_down is False and status.preempted is False and
        status.purged is False and status.failed_spot_availability is True and
        status.wait_for_idle_before_termination is False and
        status.drain_cap_seconds is None and status.drain_started_at is None and
        status.logical_retirement_version is None and
        status.logical_retirement_controller_epoch is None and
        status.logical_retirement_generation is None and
        status.logical_retirement_target_capacity is None and
        status.logical_retirement_confirmed_generation is None and
        status.logical_retirement_bounded_deadline is False and
        status.logical_retirement_committed is False)


def provider_presence_cleanup_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundNonPoolLaunchContext,
    terminal_evidence: TerminalEvidence,
) -> tuple[dict[str, Any], Any]:
    """Validate exact PRESENT evidence before reserved-fill teardown.

    PRESENT is not settlement evidence: the association, retention pin, and
    replica pointer remain intact while the existing fenced down worker owns
    cleanup.  This validator therefore grants only the request layer's atomic
    transition into that durable cleanup shape.
    """
    if not isinstance(context, BoundNonPoolLaunchContext):
        raise TypeError('context must be a BoundNonPoolLaunchContext.')
    values = _terminal_values(terminal_evidence)
    profile_kind = context.profile.kind
    lifecycle, service, replica, association = _lock_effect_rows(
        connection,
        context,
        require_paid_claim=is_paid_provider_reconciliation_profile(
            profile_kind))
    _validate_effect_rows(lifecycle,
                          service,
                          replica,
                          association,
                          context,
                          allowed_resolutions=frozenset({Resolution.AMBIGUOUS}))
    if (association['reconciliation_outcome']
            != ReconciliationOutcome.POST_EFFECT_AMBIGUOUS.value or
            association['provider_evidence'] != ProviderEvidence.PRESENT.value
            or association['service_job_id'] is not None):
        raise OrdinaryLaunchBindingConflict(
            'Provider presence cannot authorize cleanup for this launch '
            'profile or phase.')
    if profile_kind is NonPoolLaunchProfileKind.RESERVED_FILL:
        shape_matches = bool(association['effect_phase']
                             in (EffectPhase.PROVIDER_IO.value,
                                 EffectPhase.SERVICE_JOB_IO.value) and
                             association['paid_capacity_pool_key'] is None)
    elif is_paid_provider_reconciliation_profile(profile_kind):
        pool_key = association['paid_capacity_pool_key']
        pool_identity = (paid_capacity.pool_key_payload(pool_key) if isinstance(
            pool_key, str) else None)
        shape_matches = bool(
            is_paid_provider_reconciliation_phase(association['effect_phase'])
            and isinstance(pool_identity, Mapping) and
            pool_identity.get('cloud') in ('aws', 'gcp') and
            pool_identity.get('use_spot') is True and
            paid_provider_reconciliation_pool_shape_matches(
                profile_kind, pool_key) and
            ordinary_paid_provider_terminal_shape_matches(
                association['terminal_status'], association['terminal_cause'],
                pool_key))
    else:
        shape_matches = False
    if not shape_matches:
        raise OrdinaryLaunchBindingConflict(
            'Provider presence cannot authorize cleanup for this launch '
            'profile or phase.')
    existing_terminal = association['terminal_status']
    if existing_terminal is None or any(
            association[key] != value for key, value in values.items()):
        raise OrdinaryLaunchBindingConflict(
            'Provider presence lacks the exact copied terminal evidence.')

    observed_at = association['provider_evidence_observed_at']
    if (observed_at is None or terminal_evidence.quiesced_at is None or
            observed_at < terminal_evidence.quiesced_at):
        raise OrdinaryLaunchBindingConflict(
            'Provider presence predates exact executor quiescence.')
    info = _locked_replica_info(replica)
    if profile_kind is NonPoolLaunchProfileKind.RESERVED_FILL:
        if (info.service_job_id is not None or
                info.paid_capacity_pool_key is not None or
                info.is_zero_cost is not True or
                info.zero_cost_materialization_sequence is not None):
            raise OrdinaryLaunchBindingConflict(
                'Provider-present cleanup requires an unmaterialized '
                'zero-cost replica with no service job or paid identity.')
        # The provider effect has already happened. Current allocation and gate
        # generations may advance while its exact physical object is being
        # reconciled, so validate the immutable admission-time profile.
        _validate_reserved_fill_cleanup_profile_in_connection(
            connection, service, replica, context.profile)
        expected_payload, expected_digest = _reserved_fill_provider_evidence(
            association, info, ProviderEvidence.PRESENT)
    else:
        if (info.service_job_id is not None or info.is_spot is not True or
                info.is_zero_cost is not False or
                info.reserved_fill is not False or info.paid_capacity_pool_key
                != association['paid_capacity_pool_key']):
            raise OrdinaryLaunchBindingConflict(
                'Provider-present cleanup lost its exact paid GCP profile.')
        # Do not re-consult a replacement predecessor after provider I/O; the
        # frozen cleanup graph above is the non-authorizing settlement proof.
        if profile_kind is NonPoolLaunchProfileKind.ORDINARY_PAID:
            _validate_profile_authority_in_connection(
                connection,
                service,
                replica,
                context.profile,
                validate_paid_provider_start=False)
        expected_payload, expected_digest = _ordinary_paid_provider_evidence(
            association, info.cluster_name, ProviderEvidence.PRESENT)
    if association['provider_evidence_payload'] != expected_payload:
        raise OrdinaryLaunchBindingConflict(
            'Provider presence does not name the exact physical replica.')
    if association['provider_evidence_digest'] != expected_digest:
        raise OrdinaryLaunchBindingConflict(
            'Provider presence evidence digest is not canonical.')
    return dict(association), info


def _validate_projected_provider_absence_retirement_locked_rows(
    connection: sqlalchemy.engine.Connection,
    lifecycle: Mapping[str, Any] | None,
    service: Mapping[str, Any] | None,
    replica: Mapping[str, Any] | None,
    claim_rows: Sequence[Any],
    associations: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], Any]:
    """Validate ABSENT retirement after canonical rows are already locked."""
    if (lifecycle is None or service is None or replica is None or
            len(associations) != 1 or claim_rows):
        raise OrdinaryLaunchBindingConflict(
            'Projected provider absence lost its exact service, replica, '
            'association, or released paid-claim shape.')
    association = associations[0]
    profile = _association_profile(association)
    current_epoch = service['lifecycle_epoch']
    origin_epoch = association['service_lifecycle_epoch']
    if (type(current_epoch) is not int or type(origin_epoch) is not int or
            lifecycle['epoch'] != current_epoch or
            origin_epoch != current_epoch or
            service['hash'] != association['service_hash'] or
            service['workspace'] != association['service_workspace'] or
            replica['ordinary_launch_association_id'] is not None or
            association['resolution'] != Resolution.PROJECTED.value or
            association['reconciliation_outcome']
            != ReconciliationOutcome.PROJECTED.value or
            association['provider_evidence'] != ProviderEvidence.ABSENT.value or
            association['service_job_id'] is not None or
            association['terminal_status'] is None or
            type(association['terminal_execution_generation']) is not int or
            association['terminal_execution_generation'] < 1 or
            association['execution_quiescence_required'] is not True or
            association['execution_quiesced_generation']
            != association['terminal_execution_generation'] or
            association['execution_quiesced_at'] is None or
            association['provider_evidence_observed_at'] is None or
            association['provider_evidence_observed_at']
            < association['execution_quiesced_at'] or
            association['projected_at'] is None or
            association['pin_released_at'] is None or
            association['tombstone_not_before'] is None or profile is None or
            not _replica_snapshot_matches_association(
                replica, association, require_launch_authorized=False)):
        raise OrdinaryLaunchBindingConflict(
            'Projected provider absence history is not exact and complete.')
    info = _locked_replica_info(replica)
    if profile.kind is NonPoolLaunchProfileKind.RESERVED_FILL:
        if (association['effect_phase']
                not in (EffectPhase.NOT_STARTED.value,
                        EffectPhase.PROVIDER_IO.value,
                        EffectPhase.SERVICE_JOB_IO.value) or
                association['paid_capacity_pool_key'] is not None):
            raise OrdinaryLaunchBindingConflict(
                'Projected reserved-fill absence history is not exact.')
        expected_payload, expected_digest = _reserved_fill_provider_evidence(
            association, info, ProviderEvidence.ABSENT)
        _validate_reserved_fill_cleanup_profile_in_connection(
            connection, service, replica, profile)
    elif is_paid_provider_reconciliation_profile(profile.kind):
        pool_key = association['paid_capacity_pool_key']
        replacement_shape_matches = (
            paid_provider_reconciliation_pool_shape_matches(
                profile.kind, pool_key))
        if (not replacement_shape_matches or
                not is_paid_provider_reconciliation_phase(
                    association['effect_phase']) or
                not isinstance(pool_key, str) or not pool_key or
                not ordinary_paid_provider_terminal_shape_matches(
                    association['terminal_status'],
                    association['terminal_cause'], pool_key) or
                info.paid_capacity_pool_key != pool_key or
                not replica_has_projected_provider_absence_cleanup_marker(info)
           ):
            raise OrdinaryLaunchBindingConflict(
                'Projected ordinary-paid absence history is not exact.')
        expected_payload, expected_digest = _ordinary_paid_provider_evidence(
            association, info.cluster_name, ProviderEvidence.ABSENT)
    else:
        raise OrdinaryLaunchBindingConflict(
            'Projected provider absence profile is not retireable.')
    if (association['provider_evidence_payload'] != expected_payload or
            association['provider_evidence_digest'] != expected_digest):
        raise OrdinaryLaunchBindingConflict(
            'Projected provider absence evidence is not canonical.')
    # This is the sole N-2 operational boundary. Every check above proves a
    # frozen typed profile, terminal execution quiescence, released pin and
    # paid claim, and canonical post-quiescence provider absence before the
    # wider terminal-only cohort window is consulted.
    _validate_terminal_generic_cleanup_capability(
        service,
        capability_cohort_epoch=association['capability_cohort_epoch'],
        capability_profile_set_digest=association[
            'capability_profile_set_digest'],
        receipt_protocol_version=association['receipt_protocol_version'])
    return dict(association), info


def projected_provider_absence_retirement_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
    replica_id: int,
    replica_record_id: str,
) -> tuple[dict[str, Any], Any]:
    """Validate the shared restart-safe ABSENT retirement authority.

    The association is settled and its replica pointer is deliberately gone,
    so the live reduction validator cannot address it.  This history readback
    accepts only a protocol-v2 reserved-fill physical-absence tombstone or an
    ordinary-paid exact create-rejection tombstone whose canonical provider
    ABSENT proof postdates executor quiescence. By itself it grants only
    provider-free replica-row retirement. A caller may derive the distinct
    current-cohort paid auxiliary authority only by additionally proving the
    deterministic resource-action identity and closed provider scope in the
    same locked transaction.
    """
    _require_postgres(connection)
    service_name = _nonempty(service_name, 'service_name')
    replica_id = _positive_int(replica_id, 'replica_id')
    record_uuid = _canonical_uuid(replica_record_id, 'replica_record_id')
    lifecycle = connection.execute(
        sqlalchemy.select(
            serve_state_schema.service_lifecycle_fences_table).where(
                serve_state_schema.service_lifecycle_fences_table.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            service_name).with_for_update()).mappings().one_or_none()
    replica = connection.execute(
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name == service_name,
            serve_state_schema.replicas_table.c.replica_id ==
            replica_id).with_for_update()).mappings().one_or_none()
    claim_rows = connection.execute(
        sqlalchemy.select(
            serve_state_schema.paid_capacity_claims_table.c.pool_key).where(
                serve_state_schema.paid_capacity_claims_table.c.service_name ==
                service_name,
                serve_state_schema.paid_capacity_claims_table.c.replica_id ==
                replica_id).with_for_update()).all()
    associations = list(
        connection.execute(
            sqlalchemy.select(ordinary_launch_associations_table).where(
                ordinary_launch_associations_table.c.service_name ==
                service_name,
                ordinary_launch_associations_table.c.replica_id == replica_id,
                ordinary_launch_associations_table.c.replica_record_id ==
                record_uuid,
                ordinary_launch_associations_table.c.binding_protocol_version ==
                NON_POOL_BINDING_PROTOCOL_VERSION).order_by(
                    ordinary_launch_associations_table.c.launch_generation).
            with_for_update()).mappings())
    return _validate_projected_provider_absence_retirement_locked_rows(
        connection, lifecycle, service, replica, claim_rows, associations)


def projected_provider_absence_cleanup_authority_in_connection(
    connection: sqlalchemy.engine.Connection,
    service_name: str,
    replica_id: int,
    replica_record_id: str,
) -> tuple[dict[str, Any], Any]:
    """Validate restart-safe immediate cleanup after committed ABSENT."""
    association, info = (
        projected_provider_absence_retirement_authority_in_connection(
            connection, service_name, replica_id, replica_record_id))
    if not replica_has_projected_provider_absence_cleanup_marker(info):
        raise OrdinaryLaunchBindingConflict(
            'Projected provider absence lost its durable cleanup marker.')
    return association, info


def project_provider_absence_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundNonPoolLaunchContext,
    terminal_evidence: TerminalEvidence,
    *,
    expected_provider_evidence_payload: Mapping[str, Any] | None = None,
) -> bool:
    """Settle one typed phantom after exact provider absence proof."""
    association, _ = provider_absence_projection_authority_in_connection(
        connection,
        context,
        terminal_evidence,
        expected_provider_evidence_payload=expected_provider_evidence_payload)
    cleared = connection.execute(
        sqlalchemy.update(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            context.service_name, serve_state_schema.replicas_table.c.replica_id
            == context.replica_id,
            serve_state_schema.replicas_table.c.ordinary_launch_association_id
            == context.association_id).values(
                ordinary_launch_association_id=None))
    if cleared.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Provider absence could not clear the exact replica pointer.')
    now = sqlalchemy.func.clock_timestamp()
    values = {
        'resolution': Resolution.PROJECTED.value,
        'reconciliation_outcome': ReconciliationOutcome.PROJECTED.value,
        'ambiguity_code': None,
        'projected_at': now,
        'pin_released_at': now,
        'tombstone_not_before':
            sqlalchemy.text("transaction_timestamp() + INTERVAL '60 days'"),
        'owner_revision': int(association['owner_revision']) + 1,
        'updated_at': now,
    }
    updated = connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id,
            ordinary_launch_associations_table.c.owner_revision ==
            association['owner_revision']).values(**values))
    if updated.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Provider absence projection lost its association CAS.')
    return True


def project_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    *,
    pre_effect_terminal: bool,
    service_job_id: int | None,
) -> bool:
    """Settle Serve association/pointer inside a cross-layer transaction.

    The request layer must delete the exact active retention pin later in this
    same transaction.  Any failure rolls back these Serve writes too.
    """
    lifecycle, service, replica, association = _lock_effect_rows(
        connection, context)
    adjusted = dict(association)
    adjusted['resolution'] = Resolution.BOUND.value
    _validate_effect_rows(lifecycle, service, replica, adjusted, context)
    if pre_effect_terminal:
        if (association['effect_phase'] != EffectPhase.NOT_STARTED.value or
                association['terminal_status'] is None or
                association['resolution']
                not in (Resolution.BOUND.value,
                        Resolution.CANCEL_REQUESTED.value)):
            return False
        target = Resolution.PRE_EFFECT_TERMINAL
    else:
        if (association['resolution'] != Resolution.RESULT_RECORDED.value or
                association['effect_phase']
                != EffectPhase.SERVICE_JOB_RECORDED.value or
                association['service_job_id'] != service_job_id):
            return False
        target = Resolution.PROJECTED
    cleared = connection.execute(
        sqlalchemy.update(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            context.service_name, serve_state_schema.replicas_table.c.replica_id
            == context.replica_id,
            serve_state_schema.replicas_table.c.ordinary_launch_association_id
            == context.association_id).values(
                ordinary_launch_association_id=None))
    if cleared.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Projection could not clear the exact replica pointer.')
    now = sqlalchemy.func.clock_timestamp()
    values = {
        'resolution': target.value,
        'projected_at': now,
        'pin_released_at': now,
        'tombstone_not_before':
            sqlalchemy.text("transaction_timestamp() + INTERVAL '60 days'"),
        'owner_revision': int(association['owner_revision']) + 1,
        'updated_at': now,
    }
    if isinstance(context, BoundNonPoolLaunchContext):
        values['reconciliation_outcome'] = (
            ReconciliationOutcome.PRE_EFFECT_TERMINAL.value
            if pre_effect_terminal else ReconciliationOutcome.PROJECTED.value)
    updated = connection.execute(
        sqlalchemy.update(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id).values(**values))
    return updated.rowcount == 1


def project_from_request(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
    *,
    pre_effect_terminal: bool,
    service_job_id: int | None,
    release_pin: Callable[[sqlalchemy.engine.Connection, str, uuid.UUID], bool],
) -> bool:
    """Project Serve state and delete its exact request pin atomically."""
    changed = project_in_connection(connection,
                                    context,
                                    pre_effect_terminal=pre_effect_terminal,
                                    service_job_id=service_job_id)
    if changed and not release_pin(connection, context.request_id,
                                   context.association_id):
        raise OrdinaryLaunchBindingConflict(
            'Projection could not release the exact request retention pin.')
    return changed


def release_projected_paid_capacity_claim_in_connection(
    connection: sqlalchemy.engine.Connection,
    context: BoundLaunchContext,
) -> bool:
    """Delete the exact paid claim after projection in the same transaction.

    The caller must already hold the canonical lifecycle, service, paid-pool,
    replica, and association locks through ``project_from_request``.  Keeping
    this as a post-projection step lets the final project revalidation still
    observe the claim; all writes remain invisible until the shared transaction
    commits.
    """
    service = None
    if isinstance(context, BoundNonPoolLaunchContext):
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == context.service_name
            ).with_for_update()).mappings().one_or_none()
    association = connection.execute(
        sqlalchemy.select(ordinary_launch_associations_table).where(
            ordinary_launch_associations_table.c.association_id ==
            context.association_id).with_for_update()).mappings().one_or_none()
    if (association is None or
            association['request_id'] != context.request_id or
            association['service_name'] != context.service_name or
            association['replica_id'] != context.replica_id or
            association['replica_record_id'] != context.replica_record_id or
            association['launch_generation'] != context.launch_generation or
            association['input_digest'] != context.input_digest or
            association['resolution'] not in tuple(
                value.value for value in SETTLED_RESOLUTIONS)):
        raise OrdinaryLaunchBindingConflict(
            'Paid-capacity release lost the exact projected association.')
    if isinstance(context, BoundNonPoolLaunchContext):
        if service is None:
            raise OrdinaryLaunchBindingConflict(
                'Paid-capacity release lost its generic service tuple.')
        _validate_retained_generic_cleanup_capability(
            service,
            capability_cohort_epoch=context.capability_cohort_epoch,
            capability_profile_set_digest=(
                context.capability_profile_set_digest),
            receipt_protocol_version=context.receipt_protocol_version)
    claims = serve_state_schema.paid_capacity_claims_table
    predicates = (
        claims.c.service_name == association['service_name'],
        claims.c.replica_id == association['replica_id'],
    )
    pool_key = association['paid_capacity_pool_key']
    if pool_key is None:
        remaining = connection.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()).select_from(claims).where(
                    *predicates)).scalar_one()
        return int(remaining) == 0
    result = connection.execute(
        sqlalchemy.delete(claims).where(
            *predicates, claims.c.service_hash == association['service_hash'],
            claims.c.pool_key == pool_key))
    return result.rowcount == 1


# Compatibility name for the reducer integration while the stack is assembled.
mark_projected_in_connection = project_in_connection


def classify_startup(
    association: Mapping[str, Any],
    request: RequestStartupFacts,
) -> StartupClassification:
    resolution = Resolution(str(association['resolution']))
    if resolution in SETTLED_RESOLUTIONS:
        return StartupClassification.SETTLED
    if resolution == Resolution.AMBIGUOUS:
        return StartupClassification.AMBIGUOUS
    phase = EffectPhase(str(association['effect_phase']))
    if not request.exists:
        return StartupClassification.AMBIGUOUS
    if request.status in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
        return (StartupClassification.REDUCE_TERMINAL
                if request.quiescent else StartupClassification.WAIT_QUIESCENCE)
    if request.status not in ('PENDING', 'WAITING', 'RUNNING'):
        return StartupClassification.AMBIGUOUS
    if request.queue_exists:
        return StartupClassification.ADOPT_ACTIVE
    if (request.execution_generation == 0 and not request.claim_exists and
            phase == EffectPhase.NOT_STARTED):
        return StartupClassification.PRE_EFFECT_TERMINALIZE
    return StartupClassification.AMBIGUOUS


def _utc_timestamp(value: datetime.datetime,
                   field_name: str) -> datetime.datetime:
    if not isinstance(value, datetime.datetime) or value.tzinfo is None:
        raise ValueError(f'{field_name} must be timezone-aware.')
    return value.astimezone(datetime.timezone.utc)


def _canonical_json_object(value: Mapping[str, Any],
                           field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f'{field_name} must be a non-empty mapping.')
    try:
        canonical = json.loads(
            json.dumps(dict(value),
                       sort_keys=True,
                       separators=(',', ':'),
                       ensure_ascii=False,
                       allow_nan=False))
    except (TypeError, ValueError) as error:
        raise ValueError(
            f'{field_name} must contain canonical JSON values.') from error
    if not isinstance(canonical, dict) or not canonical:
        raise ValueError(f'{field_name} must be a non-empty JSON object.')
    return canonical


def _validate_legacy_identity(
        identity: LegacyLaunchIdentity) -> LegacyLaunchIdentity:
    if not isinstance(identity, LegacyLaunchIdentity):
        raise TypeError('identity must be LegacyLaunchIdentity.')
    _nonempty(identity.service_name, 'service_name')
    _nonempty(identity.service_hash, 'service_hash')
    _positive_int(identity.service_lifecycle_epoch, 'service_lifecycle_epoch')
    _positive_int(identity.replica_id, 'replica_id')
    _canonical_uuid(identity.replica_record_id, 'replica_record_id')
    _positive_int(identity.replica_version, 'replica_version')
    _nonempty(identity.cluster_name, 'cluster_name')
    _nonempty(identity.request_id, 'request_id')
    _nonempty(identity.provider_context, 'provider_context')
    _nonempty(identity.provider_physical_resource_uid,
              'provider_physical_resource_uid')
    return identity


def _legacy_identity_sort_key(identity: LegacyLaunchIdentity) -> str:
    return json.dumps(identity.canonical_mapping(),
                      sort_keys=True,
                      separators=(',', ':'),
                      ensure_ascii=False,
                      allow_nan=False)


def _legacy_scope_digest(
    identity: LegacyLaunchIdentity,
    canonical_identities: list[dict[str, Any]],
) -> str:
    return _canonical_sha256({
        'identities': canonical_identities,
        'scope_version': 1,
        'service_hash': identity.service_hash,
        'service_lifecycle_epoch': identity.service_lifecycle_epoch,
        'service_name': identity.service_name,
    })


def create_legacy_reconciliation_scope_in_connection(
    connection: sqlalchemy.engine.Connection,
    identities: list[LegacyLaunchIdentity] | tuple[LegacyLaunchIdentity, ...],
    *,
    reviewed_by: str,
    review_reason: str,
) -> uuid.UUID:
    """Seal one exact reviewed set of historical unbound replica rows."""
    _require_postgres(connection)
    if not connection.in_transaction():
        raise OrdinaryLaunchBindingUnavailable(
            'Legacy scope creation requires an active transaction.')
    reviewed_by = _nonempty(reviewed_by, 'reviewed_by')
    review_reason = _nonempty(review_reason, 'review_reason')
    if not isinstance(identities, (list, tuple)) or not identities:
        raise ValueError('identities must contain at least one exact row.')
    if len(identities) > 1000:
        raise ValueError('A legacy reconciliation scope is limited to 1000.')
    checked = [_validate_legacy_identity(identity) for identity in identities]
    first = checked[0]
    if any((identity.service_name, identity.service_hash,
            identity.service_lifecycle_epoch) != (first.service_name,
                                                  first.service_hash,
                                                  first.service_lifecycle_epoch)
           for identity in checked):
        raise ValueError('A legacy scope must name one service incarnation.')
    ordered = sorted(checked, key=_legacy_identity_sort_key)
    canonical_identities = [
        identity.canonical_mapping() for identity in ordered
    ]
    if len({_legacy_identity_sort_key(identity) for identity in ordered
           }) != len(ordered):
        raise ValueError('A legacy scope cannot contain duplicate identities.')
    identities_sha256 = _legacy_scope_digest(first, canonical_identities)
    scope_id = uuid.uuid5(_LEGACY_SCOPE_NAMESPACE, identities_sha256)

    lifecycle = connection.execute(
        sqlalchemy.select(
            serve_state_schema.service_lifecycle_fences_table).where(
                serve_state_schema.service_lifecycle_fences_table.c.name ==
                first.service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            first.service_name).with_for_update()).mappings().one_or_none()
    if (lifecycle is None or service is None or
            lifecycle['epoch'] != first.service_lifecycle_epoch or
            service['hash'] != first.service_hash or
            service['lifecycle_epoch'] != first.service_lifecycle_epoch or
            service['pool'] != 0):
        raise OrdinaryLaunchBindingConflict(
            'Legacy scope service incarnation is no longer exact.')

    for identity in ordered:
        replica = connection.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                identity.service_name,
                serve_state_schema.replicas_table.c.replica_id == identity.
                replica_id).with_for_update()).mappings().one_or_none()
        if (replica is None or replica['version'] != identity.replica_version or
                replica['cluster_name'] != identity.cluster_name or
                _replica_record_id(replica) != str(identity.replica_record_id)
                or replica['ordinary_launch_association_id'] is not None):
            raise OrdinaryLaunchBindingConflict(
                'Legacy scope replica identity is no longer exact or is '
                'already bound.')
        association_exists = connection.execute(
            sqlalchemy.select(sqlalchemy.exists().where(
                ordinary_launch_associations_table.c.service_name ==
                identity.service_name,
                ordinary_launch_associations_table.c.replica_record_id ==
                identity.replica_record_id))).scalar_one()
        if association_exists:
            raise OrdinaryLaunchBindingConflict(
                'Legacy scope cannot include an associated launch.')
        info = _locked_replica_info(replica)
        if (info.replica_id != identity.replica_id or
                info.version != identity.replica_version or
                info.cluster_name != identity.cluster_name or
                info.replica_record_id != str(identity.replica_record_id)):
            raise OrdinaryLaunchBindingConflict(
                'Legacy scope replica payload does not match its row.')
        retained_context = getattr(info, 'reserved_fill_kubernetes_context',
                                   None)
        retained_uid = getattr(info, 'reserved_fill_physical_cluster_uid', None)
        if ((retained_context is not None and
             retained_context != identity.provider_context) or
            (retained_uid is not None and
             retained_uid != identity.provider_physical_resource_uid)):
            raise OrdinaryLaunchBindingConflict(
                'Legacy scope provider identity conflicts with retained '
                'replica evidence.')

    values = {
        'scope_id': scope_id,
        'scope_version': 1,
        'service_name': first.service_name,
        'service_hash': first.service_hash,
        'service_lifecycle_epoch': first.service_lifecycle_epoch,
        'identity_count': len(ordered),
        'identities': canonical_identities,
        'identities_sha256': identities_sha256,
        'reviewed_by': reviewed_by,
        'review_reason': review_reason,
    }
    inserted = connection.execute(
        postgresql.insert(legacy_reconciliation_scopes_table).values(
            **values).on_conflict_do_nothing(
                index_elements=[legacy_reconciliation_scopes_table.c.scope_id]))
    if inserted.rowcount == 1:
        return scope_id
    existing = connection.execute(
        sqlalchemy.select(legacy_reconciliation_scopes_table).where(
            legacy_reconciliation_scopes_table.c.scope_id ==
            scope_id).with_for_update()).mappings().one_or_none()
    if existing is None or any(
            existing[key] != value for key, value in values.items()):
        raise OrdinaryLaunchBindingConflict(
            'Legacy reconciliation scope replay is not exact.')
    return scope_id


def create_legacy_reconciliation_scope(
    identities: list[LegacyLaunchIdentity] | tuple[LegacyLaunchIdentity, ...],
    *,
    reviewed_by: str,
    review_reason: str,
) -> uuid.UUID:
    """Seal a legacy scope under the service launch-authority lock."""
    if not identities:
        raise ValueError('identities must contain at least one exact row.')
    service_name = _validate_legacy_identity(identities[0]).service_name
    with serve_state.service_replica_launch_authority_write_session(
            service_name) as (_, session):
        scope_id = create_legacy_reconciliation_scope_in_connection(
            session.connection(),
            identities,
            reviewed_by=reviewed_by,
            review_reason=review_reason)
        session.commit()
        return scope_id


def _legacy_evidence_values(
    evidence: LegacyReconciliationEvidence,) -> dict[str, Any]:
    if not isinstance(evidence, LegacyReconciliationEvidence):
        raise TypeError('evidence must be LegacyReconciliationEvidence.')
    status = _nonempty(evidence.observed_request_status,
                       'observed_request_status')
    generation = evidence.observed_request_execution_generation
    if generation is not None:
        generation = _nonnegative_int(generation,
                                      'observed_request_execution_generation')
    if (type(evidence.observed_request_queue_present) is not bool or
            type(evidence.observed_request_claim_present) is not bool):
        raise ValueError('Observed request queue/claim facts must be booleans.')
    result_digest = evidence.observed_request_result_digest
    if result_digest is not None and (not isinstance(result_digest, str) or
                                      not _SHA256_RE.fullmatch(result_digest)):
        raise ValueError('observed_request_result_digest must be SHA-256.')
    request_at = _utc_timestamp(evidence.observed_request_at,
                                'observed_request_at')
    request_payload = _canonical_json_object(evidence.observed_request_evidence,
                                             'observed_request_evidence')
    request_digest_payload = {
        'claim_present': evidence.observed_request_claim_present,
        'evidence': request_payload,
        'execution_generation': generation,
        'observed_at': request_at.isoformat(),
        'queue_present': evidence.observed_request_queue_present,
        'result_digest': result_digest,
        'status': status,
    }

    executor_at = evidence.executor_terminated_at
    executor_payload = evidence.executor_termination_evidence
    if (executor_at is None) != (executor_payload is None):
        raise ValueError(
            'Executor termination timestamp and evidence must be paired.')
    executor_digest = None
    if executor_at is not None:
        executor_at = _utc_timestamp(executor_at, 'executor_terminated_at')
        assert executor_payload is not None
        executor_payload = _canonical_json_object(
            executor_payload, 'executor_termination_evidence')
        executor_digest = _canonical_sha256({
            'evidence': executor_payload,
            'terminated_at': executor_at.isoformat(),
        })

    provider = evidence.provider_evidence
    if not isinstance(provider, ProviderEvidence):
        raise ValueError('provider_evidence must be a closed classification.')
    provider_at = evidence.provider_evidence_observed_at
    provider_payload = evidence.provider_evidence_payload
    provider_digest = None
    if provider == ProviderEvidence.NOT_QUERIED:
        if provider_at is not None or provider_payload is not None:
            raise ValueError('NOT_QUERIED cannot carry provider evidence.')
    else:
        if provider_at is None or provider_payload is None:
            raise ValueError(
                'A provider classification requires timestamped evidence.')
        provider_at = _utc_timestamp(provider_at,
                                     'provider_evidence_observed_at')
        provider_payload = _canonical_json_object(provider_payload,
                                                  'provider_evidence_payload')
        provider_digest = _canonical_sha256({
            'classification': provider.value,
            'evidence': provider_payload,
            'observed_at': provider_at.isoformat(),
        })
    return {
        'observed_request_status': status,
        'observed_request_execution_generation': generation,
        'observed_request_queue_present':
            evidence.observed_request_queue_present,
        'observed_request_claim_present':
            evidence.observed_request_claim_present,
        'observed_request_result_digest': result_digest,
        'observed_request_at': request_at,
        'observed_request_evidence': request_payload,
        'observed_request_evidence_digest':
            _canonical_sha256(request_digest_payload),
        'executor_terminated_at': executor_at,
        'executor_termination_evidence': executor_payload,
        'executor_termination_evidence_digest': executor_digest,
        'provider_evidence': provider.value,
        'provider_evidence_observed_at': provider_at,
        'provider_evidence_payload': provider_payload,
        'provider_evidence_digest': provider_digest,
    }


def _lock_legacy_scope(
    connection: sqlalchemy.engine.Connection,
    scope_id: uuid.UUID,
    identity: LegacyLaunchIdentity,
) -> Mapping[str, Any]:
    scope = connection.execute(
        sqlalchemy.select(legacy_reconciliation_scopes_table).where(
            legacy_reconciliation_scopes_table.c.scope_id ==
            scope_id).with_for_update()).mappings().one_or_none()
    if (scope is None or scope['service_name'] != identity.service_name or
            scope['service_hash'] != identity.service_hash or
            scope['service_lifecycle_epoch'] != identity.service_lifecycle_epoch
            or identity.canonical_mapping() not in scope['identities']):
        raise OrdinaryLaunchBindingConflict(
            'Legacy reconciliation identity is outside its sealed scope.')
    return scope


def _latest_legacy_event(
    connection: sqlalchemy.engine.Connection,
    scope_id: uuid.UUID,
    identity: LegacyLaunchIdentity,
) -> Mapping[str, Any] | None:
    return connection.execute(
        sqlalchemy.select(legacy_reconciliations_table).where(
            legacy_reconciliations_table.c.scope_id == scope_id,
            legacy_reconciliations_table.c.service_name ==
            identity.service_name, legacy_reconciliations_table.c.service_hash
            == identity.service_hash,
            legacy_reconciliations_table.c.replica_record_id ==
            identity.replica_record_id,
            legacy_reconciliations_table.c.cluster_name ==
            identity.cluster_name,
            legacy_reconciliations_table.c.replica_id == identity.replica_id,
            legacy_reconciliations_table.c.request_id == identity.request_id,
            legacy_reconciliations_table.c.provider_context ==
            identity.provider_context,
            legacy_reconciliations_table.c.provider_physical_resource_uid ==
            identity.provider_physical_resource_uid).order_by(
                legacy_reconciliations_table.c.reconciliation_sequence.desc()).
        limit(1).with_for_update()).mappings().one_or_none()


def get_latest_legacy_reconciliation(
    scope_id: uuid.UUID | str,
    identity: LegacyLaunchIdentity,
) -> Mapping[str, Any] | None:
    """Read the latest append-only disposition for one scoped identity."""
    scope_uuid = _canonical_uuid(scope_id, 'scope_id')
    identity = _validate_legacy_identity(identity)
    engine = serve_state.get_database_engine()
    if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise OrdinaryLaunchBindingUnavailable(
            'Legacy reconciliation requires central PostgreSQL state.')
    with engine.connect() as connection:
        scope = connection.execute(
            sqlalchemy.select(
                legacy_reconciliation_scopes_table.c.scope_id).where(
                    legacy_reconciliation_scopes_table.c.scope_id ==
                    scope_uuid)).scalar_one_or_none()
        if scope is None:
            raise OrdinaryLaunchBindingConflict(
                'Legacy reconciliation scope does not exist.')
        return connection.execute(
            sqlalchemy.select(legacy_reconciliations_table).where(
                legacy_reconciliations_table.c.scope_id == scope_uuid,
                legacy_reconciliations_table.c.service_name ==
                identity.service_name,
                legacy_reconciliations_table.c.service_hash ==
                identity.service_hash,
                legacy_reconciliations_table.c.replica_record_id ==
                identity.replica_record_id,
                legacy_reconciliations_table.c.cluster_name ==
                identity.cluster_name, legacy_reconciliations_table.c.replica_id
                == identity.replica_id,
                legacy_reconciliations_table.c.request_id ==
                identity.request_id,
                legacy_reconciliations_table.c.provider_context ==
                identity.provider_context,
                legacy_reconciliations_table.c.provider_physical_resource_uid ==
                identity.provider_physical_resource_uid).order_by(
                    legacy_reconciliations_table.c.reconciliation_sequence.desc(
                    )).limit(1)).mappings().one_or_none()


def append_legacy_reconciliation_in_connection(
    connection: sqlalchemy.engine.Connection,
    scope_id: uuid.UUID | str,
    identity: LegacyLaunchIdentity,
    resolution: LegacyReconciliationResolution,
    evidence: LegacyReconciliationEvidence,
    *,
    actor: str,
    reason: str,
    cleanup_completed_at: datetime.datetime | None = None,
    cleanup_completion_evidence: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    """Append or exactly replay one monotonic legacy evidence event."""
    _require_postgres(connection)
    if not connection.in_transaction():
        raise OrdinaryLaunchBindingUnavailable(
            'Legacy reconciliation append requires an active transaction.')
    scope_uuid = _canonical_uuid(scope_id, 'scope_id')
    identity = _validate_legacy_identity(identity)
    if not isinstance(resolution, LegacyReconciliationResolution):
        raise ValueError('resolution must be a closed legacy resolution.')
    actor = _nonempty(actor, 'actor')
    reason = _nonempty(reason, 'reason')
    _lock_legacy_scope(connection, scope_uuid, identity)
    previous = _latest_legacy_event(connection, scope_uuid, identity)
    evidence_values = _legacy_evidence_values(evidence)

    if resolution == LegacyReconciliationResolution.EFFECT_AMBIGUOUS:
        if cleanup_completed_at is not None or cleanup_completion_evidence is not None:
            raise ValueError('Ambiguous evidence cannot claim cleanup.')
    else:
        executor_at = evidence_values['executor_terminated_at']
        provider_at = evidence_values['provider_evidence_observed_at']
        if (evidence_values['observed_request_status']
                not in ('SUCCEEDED', 'FAILED', 'CANCELLED') or
                executor_at is None or evidence_values['provider_evidence']
                != ProviderEvidence.ABSENT.value or provider_at is None or
                provider_at < executor_at):
            raise OrdinaryLaunchBindingConflict(
                'Cleanup requires provider absence observed after exact '
                'executor termination.')

    cleanup_payload = None
    cleanup_digest = None
    if resolution == LegacyReconciliationResolution.PROJECTED:
        if cleanup_completed_at is None or cleanup_completion_evidence is None:
            raise ValueError('Projection requires exact cleanup evidence.')
        cleanup_completed_at = _utc_timestamp(cleanup_completed_at,
                                              'cleanup_completed_at')
        provider_at = evidence_values['provider_evidence_observed_at']
        assert provider_at is not None
        if cleanup_completed_at < provider_at:
            raise ValueError('Cleanup cannot predate provider absence.')
        cleanup_payload = _canonical_json_object(cleanup_completion_evidence,
                                                 'cleanup_completion_evidence')
        cleanup_digest = _canonical_sha256({
            'completed_at': cleanup_completed_at.isoformat(),
            'evidence': cleanup_payload,
        })
    elif (cleanup_completed_at is not None or
          cleanup_completion_evidence is not None):
        raise ValueError('Only projection can carry cleanup evidence.')

    previous_sequence = (0 if previous is None else int(
        previous['reconciliation_sequence']))
    values = {
        'scope_id': scope_uuid,
        'service_name': identity.service_name,
        'service_hash': identity.service_hash,
        'service_lifecycle_epoch': identity.service_lifecycle_epoch,
        'replica_id': identity.replica_id,
        'replica_record_id': identity.replica_record_id,
        'replica_version': identity.replica_version,
        'cluster_name': identity.cluster_name,
        'request_id': identity.request_id,
        'provider_context': identity.provider_context,
        'provider_physical_resource_uid':
            identity.provider_physical_resource_uid,
        'reconciliation_sequence': previous_sequence + 1,
        **evidence_values,
        'cleanup_completed_at': cleanup_completed_at,
        'cleanup_completion_evidence': cleanup_payload,
        'cleanup_completion_evidence_digest': cleanup_digest,
        'resolution': resolution.value,
        'actor': actor,
        'reason': reason,
    }
    replay_fields = tuple(values)
    if previous is not None and all(previous[key] == value
                                    for key, value in values.items()
                                    if key != 'reconciliation_sequence'):
        return previous

    ranks = {
        LegacyReconciliationResolution.EFFECT_AMBIGUOUS: 1,
        LegacyReconciliationResolution.CLEANUP_AUTHORIZED: 2,
        LegacyReconciliationResolution.PROJECTED: 3,
    }
    if previous is None:
        if resolution != LegacyReconciliationResolution.EFFECT_AMBIGUOUS:
            raise OrdinaryLaunchBindingConflict(
                'Legacy reconciliation must begin effect-ambiguous.')
    else:
        previous_resolution = LegacyReconciliationResolution(
            previous['resolution'])
        if (previous_resolution == LegacyReconciliationResolution.PROJECTED or
                ranks[resolution] < ranks[previous_resolution] or
                ranks[resolution] > ranks[previous_resolution] + 1):
            raise OrdinaryLaunchBindingConflict(
                'Legacy reconciliation transition is not monotonic.')

    event_digest = _canonical_sha256({
        key: (value.isoformat() if isinstance(value, datetime.datetime) else
              str(value) if isinstance(value, uuid.UUID) else value
             ) for key, value in values.items()
    })
    values['event_id'] = uuid.uuid5(_LEGACY_EVENT_NAMESPACE, event_digest)
    connection.execute(
        sqlalchemy.insert(legacy_reconciliations_table).values(**values))
    return {key: values[key] for key in (*replay_fields, 'event_id')}


def append_legacy_reconciliation(
    scope_id: uuid.UUID | str,
    identity: LegacyLaunchIdentity,
    resolution: LegacyReconciliationResolution,
    evidence: LegacyReconciliationEvidence,
    *,
    actor: str,
    reason: str,
) -> Mapping[str, Any]:
    """Append legacy evidence under the service launch-authority lock."""
    identity = _validate_legacy_identity(identity)
    with serve_state.service_replica_launch_authority_write_session(
            identity.service_name) as (_, session):
        result = append_legacy_reconciliation_in_connection(
            session.connection(),
            scope_id,
            identity,
            resolution,
            evidence,
            actor=actor,
            reason=reason)
        session.commit()
        return result


def project_legacy_replica_cleanup_in_connection(
    connection: sqlalchemy.engine.Connection,
    scope_id: uuid.UUID | str,
    identity: LegacyLaunchIdentity,
    *,
    actor: str,
    reason: str,
    cleanup_completion_evidence: Mapping[str, Any],
) -> bool:
    """Delete one exact absent-provider row and record it in one transaction."""
    _require_postgres(connection)
    scope_uuid = _canonical_uuid(scope_id, 'scope_id')
    identity = _validate_legacy_identity(identity)
    _lock_legacy_scope(connection, scope_uuid, identity)
    previous = _latest_legacy_event(connection, scope_uuid, identity)
    if previous is None:
        raise OrdinaryLaunchBindingConflict(
            'Legacy cleanup has no reconciliation evidence.')
    if previous['resolution'] == LegacyReconciliationResolution.PROJECTED.value:
        return False
    if previous['resolution'] != (
            LegacyReconciliationResolution.CLEANUP_AUTHORIZED.value):
        raise OrdinaryLaunchBindingConflict(
            'Legacy cleanup is not authorized by exact absence evidence.')

    lifecycle = connection.execute(
        sqlalchemy.select(serve_state_schema.service_lifecycle_fences_table).
        where(
            serve_state_schema.service_lifecycle_fences_table.c.name ==
            identity.service_name).with_for_update()).mappings().one_or_none()
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            identity.service_name).with_for_update()).mappings().one_or_none()
    replica = connection.execute(
        sqlalchemy.select(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            identity.service_name,
            serve_state_schema.replicas_table.c.replica_id ==
            identity.replica_id).with_for_update()).mappings().one_or_none()
    if (lifecycle is None or service is None or replica is None or
            lifecycle['epoch'] != identity.service_lifecycle_epoch or
            service['hash'] != identity.service_hash or
            service['lifecycle_epoch'] != identity.service_lifecycle_epoch or
            replica['version'] != identity.replica_version or
            replica['cluster_name'] != identity.cluster_name or
            _replica_record_id(replica) != str(identity.replica_record_id) or
            replica['ordinary_launch_association_id'] is not None):
        raise OrdinaryLaunchBindingConflict(
            'Legacy cleanup target no longer has its exact unbound identity.')
    association_exists = connection.execute(
        sqlalchemy.select(sqlalchemy.exists().where(
            ordinary_launch_associations_table.c.service_name ==
            identity.service_name,
            ordinary_launch_associations_table.c.replica_record_id ==
            identity.replica_record_id))).scalar_one()
    if association_exists:
        raise OrdinaryLaunchBindingConflict(
            'Legacy cleanup target unexpectedly acquired an association.')

    connection.execute(
        sqlalchemy.delete(serve_state_schema.paid_capacity_claims_table).where(
            serve_state_schema.paid_capacity_claims_table.c.service_name ==
            identity.service_name,
            serve_state_schema.paid_capacity_claims_table.c.service_hash ==
            identity.service_hash,
            serve_state_schema.paid_capacity_claims_table.c.replica_id ==
            identity.replica_id))
    deleted = connection.execute(
        sqlalchemy.delete(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name ==
            identity.service_name,
            serve_state_schema.replicas_table.c.replica_id ==
            identity.replica_id,
            serve_state_schema.replicas_table.c.ordinary_launch_association_id.
            is_(None)))
    if deleted.rowcount != 1:
        raise OrdinaryLaunchBindingConflict(
            'Legacy cleanup lost its exact replica delete fence.')
    completed_at = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    copied_evidence = LegacyReconciliationEvidence(
        observed_request_status=previous['observed_request_status'],
        observed_request_execution_generation=previous[
            'observed_request_execution_generation'],
        observed_request_queue_present=previous[
            'observed_request_queue_present'],
        observed_request_claim_present=previous[
            'observed_request_claim_present'],
        observed_request_result_digest=previous[
            'observed_request_result_digest'],
        observed_request_at=previous['observed_request_at'],
        observed_request_evidence=previous['observed_request_evidence'],
        executor_terminated_at=previous['executor_terminated_at'],
        executor_termination_evidence=previous['executor_termination_evidence'],
        provider_evidence=ProviderEvidence(previous['provider_evidence']),
        provider_evidence_observed_at=previous['provider_evidence_observed_at'],
        provider_evidence_payload=previous['provider_evidence_payload'])
    append_legacy_reconciliation_in_connection(
        connection,
        scope_uuid,
        identity,
        LegacyReconciliationResolution.PROJECTED,
        copied_evidence,
        actor=actor,
        reason=reason,
        cleanup_completed_at=completed_at,
        cleanup_completion_evidence=cleanup_completion_evidence)
    return True


def project_legacy_replica_cleanup(
    scope_id: uuid.UUID | str,
    identity: LegacyLaunchIdentity,
    *,
    actor: str,
    reason: str,
    cleanup_completion_evidence: Mapping[str, Any],
) -> bool:
    """Project one authorized legacy cleanup under launch authority."""
    identity = _validate_legacy_identity(identity)
    with serve_state.service_replica_launch_authority_write_session(
            identity.service_name) as (_, session):
        changed = project_legacy_replica_cleanup_in_connection(
            session.connection(),
            scope_id,
            identity,
            actor=actor,
            reason=reason,
            cleanup_completion_evidence=cleanup_completion_evidence)
        session.commit()
        return changed


def authorize_and_project_legacy_replica_cleanup(
    scope_id: uuid.UUID | str,
    identity: LegacyLaunchIdentity,
    evidence: LegacyReconciliationEvidence,
    *,
    actor: str,
    authorization_reason: str,
    projection_reason: str,
    cleanup_completion_evidence: Mapping[str, Any],
) -> bool:
    """Commit absence authority and exact row projection atomically."""
    identity = _validate_legacy_identity(identity)
    with serve_state.service_replica_launch_authority_write_session(
            identity.service_name) as (_, session):
        connection = session.connection()
        scope_uuid = _canonical_uuid(scope_id, 'scope_id')
        _lock_legacy_scope(connection, scope_uuid, identity)
        previous = _latest_legacy_event(connection, scope_uuid, identity)
        if (previous is not None and previous['resolution']
                == LegacyReconciliationResolution.PROJECTED.value):
            session.commit()
            return False
        append_legacy_reconciliation_in_connection(
            connection,
            scope_uuid,
            identity,
            LegacyReconciliationResolution.CLEANUP_AUTHORIZED,
            evidence,
            actor=actor,
            reason=authorization_reason)
        changed = project_legacy_replica_cleanup_in_connection(
            connection,
            scope_uuid,
            identity,
            actor=actor,
            reason=projection_reason,
            cleanup_completion_evidence=cleanup_completion_evidence)
        session.commit()
        return changed
