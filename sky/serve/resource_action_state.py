"""PostgreSQL-only typed store for SkyServe resource-action shadow evidence.

The shadow journal observes the existing legacy mutation path.  It never
creates an API resource action, an API request, or a second provider mutation.
Callers that already own a Serve transaction can borrow it through the
``*_in_session`` methods so replica intent and shadow evidence commit together.
"""

from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import datetime
import json
from typing import Any
import unicodedata
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky.serve import resource_action_state_schema as state_schema
from sky.serve import resource_actions as actions
from sky.serve import serve_state_schema
from sky.serve.serve_statuses import ReplicaStatus
from sky.serve.serve_statuses import ServiceStatus
from sky.server.requests import postgres as request_postgres
from sky.server.requests import resource_actions as kernel_actions

_TERMINAL_ATTEMPT_PHASES = frozenset({
    actions.ShadowAttemptPhase.COMPLETE,
    actions.ShadowAttemptPhase.ABANDONED_PRE_SUBMIT,
    actions.ShadowAttemptPhase.REQUEST_ASSOCIATION_UNKNOWN,
})
_PRIMARY_ROLES = frozenset({
    actions.ShadowRequestRole.PRIMARY_LAUNCH,
    actions.ShadowRequestRole.PRIMARY_DOWN,
})
_DIVERGENCE_PARITIES = frozenset(
    value.value for value in actions.ShadowDivergenceClass)
_MINIMUM_PROMOTION_WINDOW = datetime.timedelta(hours=24)
_MAX_ACTIVATION_EVIDENCE_AGE = datetime.timedelta(minutes=5)
_MAX_WORKER_REGISTRATION_AGE = datetime.timedelta(minutes=5)
_MAX_PROMOTION_INVENTORY_DECISIONS = 10_000
_MAX_PROMOTION_INVENTORY_ATTEMPTS_AND_LINKS = 100_000


def _profile_is_authoritative(
        immutable_spec: actions.ServeReplicaActionSpecV1) -> bool:
    """Return whether a checked-in provider profile has cleared every gate."""
    plan = immutable_spec.provider_plan
    invocation = immutable_spec.invocation
    if (plan.profile is not actions.ProviderProfile.POD_CLUSTER_V1 or
            not plan.requested_target.is_authoritative_pod_locator):
        return False
    if plan.action_kind is kernel_actions.ActionKind.LAUNCH:
        launch = invocation.launch
        if (launch is None or launch.file_mounts_blob_id is not None or
                launch.tls_material_ref is not None):
            return False
    elif plan.prior_resolved_target is None:
        return False
    # TODO(fcapponi): Return true only after Global028 propagation and the
    # label-qualified Kubernetes write/read/delete fixtures pass end to end.
    return False


@dataclasses.dataclass(frozen=True)
class NewShadowSample:
    """Immutable values admitted for one logical shadow sample."""

    service_name: str
    immutable_spec: actions.ServeReplicaActionSpecV1
    provider_plan: actions.ProviderLifecyclePlanV1
    profile_eligibility: actions.ProfileEligibility

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'service_name',
            _bounded_text(self.service_name,
                          name='service_name',
                          maximum_bytes=256))
        if not isinstance(self.immutable_spec,
                          actions.ServeReplicaActionSpecV1):
            raise TypeError('immutable_spec has an invalid type.')
        if not isinstance(self.provider_plan, actions.ProviderLifecyclePlanV1):
            raise TypeError('provider_plan has an invalid type.')
        eligibility = (self.profile_eligibility if isinstance(
            self.profile_eligibility, actions.ProfileEligibility) else
                       actions.ProfileEligibility(self.profile_eligibility))
        object.__setattr__(self, 'profile_eligibility', eligibility)
        self.immutable_spec.validate_parent_provider_plan(self.provider_plan)
        if self.immutable_spec.action_id != self.provider_plan.action_id:
            raise ValueError('immutable action spec and provider plan action '
                             'IDs differ.')
        if (eligibility is actions.ProfileEligibility.ELIGIBLE and
                not _profile_is_authoritative(self.immutable_spec)):
            raise ValueError('provider profile has not cleared the '
                             'authoritative eligibility gate.')
        launch = self.immutable_spec.invocation.launch
        if launch is not None and launch.source.service_name != self.service_name:
            raise ValueError('launch source service name differs from the '
                             'shadow sample service name.')
        normalized_action = kernel_actions.NewResourceAction(
            self.provider_plan.resource_identity.action_identity(
                self.provider_plan.action_kind),
            self.immutable_spec.canonical_value())
        if normalized_action.action_id != self.provider_plan.action_id:
            raise ValueError('provider plan and immutable action identities '
                             'differ.')
        if (actions.canonical_json_bytes(normalized_action.immutable_spec)
                != self.immutable_spec.canonical_bytes):
            raise ValueError('immutable action spec normalization changed its '
                             'canonical bytes.')

    @property
    def action_id(self) -> uuid.UUID:
        return self.provider_plan.action_id

    @property
    def immutable_spec_sha256(self) -> str:
        return self.immutable_spec.sha256

    @property
    def provider_plan_sha256(self) -> str:
        return self.provider_plan.sha256

    @property
    def resource_identity(self) -> str:
        return self.provider_plan.resource_identity.action_identity(
            self.provider_plan.action_kind).resource_identity


@dataclasses.dataclass(frozen=True)
class ShadowSampleRecord:
    """Fully revalidated logical shadow row."""

    action_id: uuid.UUID
    service_name: str
    immutable_spec: actions.ServeReplicaActionSpecV1
    immutable_spec_sha256: str
    provider_plan: actions.ProviderLifecyclePlanV1
    provider_plan_sha256: str
    profile_eligibility: actions.ProfileEligibility
    phase: actions.ShadowParentPhase
    legacy_projection: actions.ServeShadowProjectionV1 | None
    proposed_projection: actions.ServeShadowProjectionV1 | None
    parity_class: actions.ShadowParityClass
    revision: int
    created_at: datetime.datetime
    updated_at: datetime.datetime
    completed_at: datetime.datetime | None

    @property
    def resource_identity(self) -> actions.ProviderResourceIdentityV1:
        return self.provider_plan.resource_identity

    @property
    def action_kind(self) -> kernel_actions.ActionKind:
        return self.provider_plan.action_kind


@dataclasses.dataclass(frozen=True)
class ShadowAttemptRecord:
    """Fully revalidated per-mutation shadow row."""

    action_id: uuid.UUID
    request_sequence: int
    logical_attempt: int
    request_role: actions.ShadowRequestRole
    planned_execution_kind: actions.PlannedExecutionKind
    phase: actions.ShadowAttemptPhase
    legacy_request_id: str | None
    invocation: actions.ProviderLifecycleInvocationV1
    provider_operation_id: str | None
    actual_outcome: actions.ServeReplicaActionOutcomeV1 | None
    proposed_outcome: actions.ServeReplicaActionOutcomeV1 | None
    retry_decision: actions.ServeShadowRetryDecisionV1 | None
    pre_observation: actions.ProviderLifecycleObservationV1 | None
    post_observation: actions.ProviderLifecycleObservationV1 | None
    divergence_class: actions.ShadowDivergenceClass | None
    admitted_at: datetime.datetime
    request_bound_at: datetime.datetime | None
    completed_at: datetime.datetime | None
    updated_at: datetime.datetime


@dataclasses.dataclass(frozen=True)
class PreparedShadowAttempt:
    sample: ShadowSampleRecord
    attempt: ShadowAttemptRecord
    adopted: bool = False


@dataclasses.dataclass(frozen=True)
class WorkerCohortRecord:
    """Fully revalidated immutable worker cohort and lifecycle state."""

    cohort_identity: actions.WorkerCohortIdentityV1
    registration_attestations: actions.WorkerCohortRegistrationSetV1
    lifecycle_state: actions.WorkerCohortLifecycleState
    revision: int
    created_at: datetime.datetime
    state_changed_at: datetime.datetime
    retired_at: datetime.datetime | None

    @property
    def cohort_id(self) -> str:
        return self.cohort_identity.manifest.cohort_id

    @property
    def deployment_uid(self) -> str:
        return self.cohort_identity.deployment_uid


@dataclasses.dataclass(frozen=True)
class WorkerCohortTransition:
    record: WorkerCohortRecord
    adopted: bool = False


@dataclasses.dataclass(frozen=True)
class WorkerCohortReferenceRecord:
    """One nonexecuting retention reference for a prepared decision."""

    reference: actions.WorkerCohortReferenceInputV1
    reference_state: actions.WorkerCohortReferenceState
    revision: int
    created_at: datetime.datetime
    bound_at: datetime.datetime | None
    released_at: datetime.datetime | None

    @property
    def decision_id(self) -> uuid.UUID:
        return self.reference.decision_id

    @property
    def cohort_id(self) -> str:
        return self.reference.cohort_id


@dataclasses.dataclass(frozen=True)
class WorkerCohortReferenceTransition:
    record: WorkerCohortReferenceRecord
    adopted: bool = False


@dataclasses.dataclass(frozen=True)
class NewShadowCoverage:
    """Immutable decision coverage before its PostgreSQL admission time."""

    service_name: str
    identity: actions.CoverageDecisionIdentityV1
    normalization_outcome: actions.NormalizationOutcome
    not_representable_reason: (actions.ProviderLaunchNotRepresentableReasonV1 |
                               actions.ProviderDownNotRepresentableReasonV1 |
                               None)
    worker_cohort_ref_id: uuid.UUID | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, 'service_name',
            _bounded_text(self.service_name,
                          name='service_name',
                          maximum_bytes=256))
        if not isinstance(self.identity, actions.CoverageDecisionIdentityV1):
            raise TypeError('identity has an invalid type.')
        outcome = (self.normalization_outcome if isinstance(
            self.normalization_outcome, actions.NormalizationOutcome) else
                   actions.NormalizationOutcome(self.normalization_outcome))
        object.__setattr__(self, 'normalization_outcome', outcome)
        reason = self.not_representable_reason
        if outcome is actions.NormalizationOutcome.REPRESENTABLE:
            if reason is not None:
                raise ValueError(
                    'Representable coverage cannot carry a rejection reason.')
        else:
            expected_reason_type = (
                actions.ProviderLaunchNotRepresentableReasonV1
                if self.identity.action_type is kernel_actions.ActionKind.LAUNCH
                else actions.ProviderDownNotRepresentableReasonV1)
            if not isinstance(reason, expected_reason_type):
                raise TypeError('Coverage rejection reason has the wrong '
                                'action-kind type.')
        if self.worker_cohort_ref_id is not None:
            object.__setattr__(
                self, 'worker_cohort_ref_id',
                _canonical_uuid(self.worker_cohort_ref_id,
                                name='worker_cohort_ref_id'))
            if self.worker_cohort_ref_id != self.identity.decision_id:
                raise ValueError('worker_cohort_ref_id must equal the '
                                 'deterministic decision ID.')

    @property
    def decision_id(self) -> uuid.UUID:
        return self.identity.decision_id


@dataclasses.dataclass(frozen=True)
class ShadowCoverageAdmission:
    record: actions.CoverageDecisionV1
    adopted: bool = False


@dataclasses.dataclass(frozen=True)
class CoverageAttemptTransition:
    record: actions.CoverageAttemptV1
    adopted: bool = False


@dataclasses.dataclass(frozen=True)
class PromotionBlockerReport:
    """Typed evidence for one service's current shadow candidate window."""

    service_name: str
    service_hash: str
    candidate_since: datetime.datetime
    candidate_sample_count: int
    clean_launch_samples: int
    clean_down_samples: int
    blocking_sample_ids: tuple[uuid.UUID, ...]
    coverage_inventory_sha256: str
    reasons: tuple[str, ...]

    @property
    def clean(self) -> bool:
        return not self.reasons


@dataclasses.dataclass(frozen=True)
class ServiceModeRecord:
    service_name: str
    service_hash: str
    mode: actions.ResourceActionMode
    changed_at: datetime.datetime | None


@dataclasses.dataclass(frozen=True)
class ServiceModeTransition:
    record: ServiceModeRecord
    adopted: bool = False
    promotion_report: PromotionBlockerReport | None = None


@dataclasses.dataclass(frozen=True)
class ShadowRetentionResult:
    removed_action_ids: tuple[uuid.UUID, ...]
    protected_action_ids: tuple[uuid.UUID, ...]
    deferred_action_ids: tuple[uuid.UUID, ...] = ()


@dataclasses.dataclass(frozen=True)
class ActivationGateEvidenceV1:
    """Bounded externally collected rollout inventory presented to the store.

    The store cannot discover process images or handler inventories itself.
    Requiring named facts and immutable fingerprints keeps the transition API
    from turning one unreviewable ``gates_verified=True`` assertion into
    mutation authority.
    """

    version: int
    service_name: str
    service_hash: str
    lifecycle_epoch: int
    candidate_since: datetime.datetime | None
    old_controller_processes_drained: bool
    all_processes_on_approved_image: bool
    approved_image_digest: str
    api_schema_revision: str
    serve_schema_revision: str
    global_user_state_schema_revision: str
    handler_registered_everywhere: bool
    image_inventory_sha256: str
    handler_inventory_sha256: str
    provider_profiles_eligible: bool
    profile_inventory_sha256: str
    shadow_coverage_complete: bool
    coverage_inventory_sha256: str
    crash_injection_complete: bool
    verified_at: datetime.datetime

    def __post_init__(self) -> None:
        if self.version != 1 or isinstance(self.version, bool):
            raise ValueError('activation gate evidence version must be 1.')
        object.__setattr__(
            self, 'service_name',
            _bounded_text(self.service_name,
                          name='service_name',
                          maximum_bytes=256))
        service_hash = _canonical_uuid(self.service_hash, name='service_hash')
        object.__setattr__(self, 'service_hash', str(service_hash))
        object.__setattr__(
            self, 'lifecycle_epoch',
            _positive_integer(self.lifecycle_epoch, name='lifecycle_epoch'))
        object.__setattr__(
            self, 'candidate_since',
            _optional_timestamp(self.candidate_since, name='candidate_since'))
        for field in ('old_controller_processes_drained',
                      'all_processes_on_approved_image',
                      'handler_registered_everywhere',
                      'provider_profiles_eligible', 'shadow_coverage_complete',
                      'crash_injection_complete'):
            if not isinstance(getattr(self, field), bool):
                raise TypeError(f'{field} must be Boolean.')
        digest = _bounded_text(self.approved_image_digest,
                               name='approved_image_digest',
                               maximum_bytes=71)
        if (not digest.startswith('sha256:') or len(digest) != 71 or
                any(character not in '0123456789abcdef'
                    for character in digest[7:])):
            raise ValueError('approved_image_digest must be sha256:<64 hex>.')
        object.__setattr__(self, 'approved_image_digest', digest)
        if self.api_schema_revision not in ('005', '006'):
            raise ValueError('activation requires API schema revision 005 or '
                             '006.')
        if self.serve_schema_revision != '033':
            raise ValueError('activation requires Serve schema revision 033.')
        if self.global_user_state_schema_revision != '028':
            raise ValueError('activation requires global-user-state schema '
                             'revision 028.')
        for field in ('image_inventory_sha256', 'handler_inventory_sha256',
                      'profile_inventory_sha256', 'coverage_inventory_sha256'):
            value = getattr(self, field)
            if (not isinstance(value, str) or len(value) != 64 or
                    any(character not in '0123456789abcdef'
                        for character in value)):
                raise ValueError(f'{field} must be lowercase SHA-256 text.')
        object.__setattr__(self, 'verified_at',
                           _timestamp(self.verified_at, name='verified_at'))

    @property
    def shadow_ready(self) -> bool:
        return (self.old_controller_processes_drained and
                self.all_processes_on_approved_image and
                self.handler_registered_everywhere)

    @property
    def authority_ready(self) -> bool:
        return (self.shadow_ready and self.provider_profiles_eligible and
                self.shadow_coverage_complete and self.crash_injection_complete)


def _validate_activation_evidence_time(evidence: ActivationGateEvidenceV1,
                                       database_now: datetime.datetime) -> None:
    if evidence.verified_at > database_now:
        raise kernel_actions.ActionConflict(
            'Activation evidence timestamp is in the database future.')
    if database_now - evidence.verified_at > _MAX_ACTIVATION_EVIDENCE_AGE:
        raise kernel_actions.ActionConflict(
            'Activation evidence is older than five minutes.')


def _bounded_text(value: Any, *, name: str, maximum_bytes: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f'{name} must be text.')
    normalized = unicodedata.normalize('NFC', value)
    size = len(normalized.encode('utf-8'))
    if size == 0 or size > maximum_bytes:
        raise ValueError(f'{name} must be 1..{maximum_bytes} UTF-8 bytes.')
    return normalized


def _canonical_uuid(value: Any, *, name: str) -> uuid.UUID:
    try:
        parsed = value if isinstance(value, uuid.UUID) else uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as e:
        raise ValueError(f'{name} must be a UUID.') from e
    if isinstance(value, str) and str(parsed) != value:
        raise ValueError(f'{name} must be canonical UUID text.')
    return parsed


def _positive_integer(value: Any, *, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f'{name} must be a positive integer.')
    return value


def _timestamp(value: Any, *, name: str) -> datetime.datetime:
    if (not isinstance(value, datetime.datetime) or value.tzinfo is None or
            value.utcoffset() is None):
        raise ValueError(f'{name} must be timezone-aware.')
    return value


def _optional_timestamp(value: Any, *, name: str) -> datetime.datetime | None:
    if value is None:
        return None
    return _timestamp(value, name=name)


def _json_object(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f'{name} must be a JSON object.')
    encoded = actions.canonical_json_bytes(value)
    if len(encoded) > 65_536:
        raise ValueError(f'{name} exceeds 65536 canonical bytes.')
    normalized = json.loads(encoded.decode('utf-8'))
    if not isinstance(normalized, dict):
        raise ValueError(f'{name} must be a JSON object.')
    return normalized


def _hash_matches(value: Mapping[str, Any], digest: Any, *, name: str) -> str:
    if not isinstance(digest, str) or actions.canonical_sha256(value) != digest:
        raise ValueError(f'{name} hash does not match canonical bytes.')
    return digest


def _typed_pair(value: Any, digest: Any, *, name: str,
                reader: Any) -> Any | None:
    if (value is None) != (digest is None):
        raise ValueError(f'{name} value/hash pair is incomplete.')
    if value is None:
        return None
    normalized = _json_object(value, name=name)
    _hash_matches(normalized, digest, name=name)
    return reader(normalized)


def _canonical_equal(left: Any, right: Any) -> bool:
    return actions.canonical_json_bytes(left) == actions.canonical_json_bytes(
        right)


def _utc_timestamp_text(value: Any, *, name: str) -> str:
    timestamp = _timestamp(value, name=name).astimezone(datetime.timezone.utc)
    return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def _required_typed_pair(value: Any, digest: Any, *, name: str,
                         reader: Any) -> Any:
    parsed = _typed_pair(value, digest, name=name, reader=reader)
    if parsed is None:
        raise ValueError(f'{name} is required.')
    return parsed


def _canonical_timestamp_datetime(value: Any, *,
                                  name: str) -> datetime.datetime:
    if not isinstance(value, str):
        raise TypeError(f'{name} must be canonical timestamp text.')
    try:
        parsed = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError as e:
        raise ValueError(f'{name} is not a canonical UTC timestamp.') from e
    if parsed.strftime('%Y-%m-%dT%H:%M:%S.%fZ') != value:
        raise ValueError(f'{name} is not a canonical UTC timestamp.')
    return parsed.replace(tzinfo=datetime.timezone.utc)


def _validate_current_worker_registrations(
    cohort: actions.WorkerCohortIdentityV1,
    registrations: actions.WorkerCohortRegistrationSetV1,
    database_now: datetime.datetime,
    *,
    require_two: bool,
) -> None:
    registrations.validate_for_cohort(cohort, require_two=require_two)
    now = _timestamp(database_now,
                     name='database_now').astimezone(datetime.timezone.utc)
    for index, registration in enumerate(
            registrations.canonical_value()['workers']):
        registered_at = _canonical_timestamp_datetime(
            registration['registered_at'],
            name=f'registration[{index}].registered_at')
        observed_at = _canonical_timestamp_datetime(
            registration['worker']['observed_at'],
            name=f'registration[{index}].worker.observed_at')
        for name, timestamp in (('registered_at', registered_at),
                                ('observed_at', observed_at)):
            if timestamp > now:
                raise kernel_actions.ActionConflict(
                    f'Worker {name} is in the database future.')
            if now - timestamp > _MAX_WORKER_REGISTRATION_AGE:
                raise kernel_actions.ActionConflict(
                    f'Worker {name} is older than five minutes.')


def _worker_cohort_record(row: Mapping[str, Any]) -> WorkerCohortRecord:
    try:
        identity = _required_typed_pair(
            row['cohort_identity'],
            row['cohort_identity_sha256'],
            name='cohort_identity',
            reader=actions.WorkerCohortIdentityV1.from_value)
        registrations = _required_typed_pair(
            row['registration_attestations'],
            row['registration_attestations_sha256'],
            name='registration_attestations',
            reader=actions.WorkerCohortRegistrationSetV1.from_value)
        registrations.validate_for_cohort(identity)
        cohort_id = _bounded_text(row['cohort_id'],
                                  name='cohort_id',
                                  maximum_bytes=1024)
        deployment_uid = _bounded_text(row['deployment_uid'],
                                       name='deployment_uid',
                                       maximum_bytes=1024)
        if (cohort_id != identity.manifest.cohort_id or
                deployment_uid != identity.deployment_uid):
            raise ValueError('cohort key columns differ from the typed '
                             'identity.')
        state = actions.WorkerCohortLifecycleState(row['lifecycle_state'])
        revision = _positive_integer(row['revision'], name='revision')
        created_at = _timestamp(row['created_at'], name='created_at')
        changed_at = _timestamp(row['state_changed_at'],
                                name='state_changed_at')
        retired_at = _optional_timestamp(row.get('retired_at'),
                                         name='retired_at')
        if changed_at < created_at or (retired_at is not None and
                                       retired_at < changed_at):
            raise ValueError('cohort timestamps are out of order.')
        if ((state is actions.WorkerCohortLifecycleState.RETIRED)
                != (retired_at is not None)):
            raise ValueError('cohort retirement timestamp has an invalid '
                             'lifecycle shape.')
        if (state is actions.WorkerCohortLifecycleState.ACCEPTING and
                registrations.count != 2):
            raise ValueError('an accepting cohort requires two workers.')
        return WorkerCohortRecord(identity, registrations, state, revision,
                                  created_at, changed_at, retired_at)
    except (KeyError, TypeError, ValueError) as e:
        raise kernel_actions.InvariantViolation(
            f'Invalid Serve worker cohort row: {e}') from e


def _worker_cohort_reference_record(
        row: Mapping[str, Any]) -> WorkerCohortReferenceRecord:
    try:
        reference = actions.WorkerCohortReferenceInputV1.from_value({
            'version': 1,
            'decision_id': str(
                _canonical_uuid(row['decision_id'], name='decision_id')),
            'cohort_id': row['cohort_id'],
            'service_hash': row['service_hash'],
            'replica_incarnation': str(
                _canonical_uuid(row['replica_incarnation'],
                                name='replica_incarnation')),
            'desired_generation': row['desired_generation'],
            'action_type': row['action_type'],
            'controller_owner_fence': row['controller_owner_fence'],
            'lifecycle_epoch': row['lifecycle_epoch'],
            'preparation_capability_sha256':
                row['preparation_capability_sha256'],
        })
        state = actions.WorkerCohortReferenceState(row['reference_state'])
        revision = _positive_integer(row['revision'], name='revision')
        created_at = _timestamp(row['created_at'], name='created_at')
        bound_at = _optional_timestamp(row.get('bound_at'), name='bound_at')
        released_at = _optional_timestamp(row.get('released_at'),
                                          name='released_at')
        if ((bound_at is not None and bound_at < created_at) or
            (released_at is not None and released_at < created_at)):
            raise ValueError('cohort-reference timestamps are out of order.')
        if state is actions.WorkerCohortReferenceState.PREPARING:
            if bound_at is not None or released_at is not None:
                raise ValueError('preparing reference has later timestamps.')
        elif state in (actions.WorkerCohortReferenceState.SHADOW_ACTIVE,
                       actions.WorkerCohortReferenceState.ACTION_ACTIVE):
            if bound_at is None or released_at is not None:
                raise ValueError('active reference has invalid timestamps.')
        elif released_at is None:
            raise ValueError('released reference lacks a release timestamp.')
        return WorkerCohortReferenceRecord(reference, state, revision,
                                           created_at, bound_at, released_at)
    except (KeyError, TypeError, ValueError) as e:
        raise kernel_actions.InvariantViolation(
            f'Invalid Serve worker cohort reference row: {e}') from e


def _shadow_coverage_record(
        row: Mapping[str, Any]) -> actions.CoverageDecisionV1:
    try:
        return actions.CoverageDecisionV1.from_value({
            'decision_id': str(
                _canonical_uuid(row['decision_id'], name='decision_id')),
            'service_name': row['service_name'],
            'service_hash': row['service_hash'],
            'service_incarnation': str(
                _canonical_uuid(row['service_incarnation'],
                                name='service_incarnation')),
            'replica_id': row['replica_id'],
            'replica_incarnation': str(
                _canonical_uuid(row['replica_incarnation'],
                                name='replica_incarnation')),
            'desired_generation': row['desired_generation'],
            'action_type': row['action_type'],
            'normalizer_contract_version': row['normalizer_contract_version'],
            'normalization_outcome': row['normalization_outcome'],
            'not_representable_reason': row['not_representable_reason'],
            'worker_cohort_ref_id':
                (None if row['worker_cohort_ref_id'] is None else str(
                    _canonical_uuid(row['worker_cohort_ref_id'],
                                    name='worker_cohort_ref_id'))),
            'admitted_at': _utc_timestamp_text(row['admitted_at'],
                                               name='admitted_at'),
        })
    except (KeyError, TypeError, ValueError) as e:
        raise kernel_actions.InvariantViolation(
            f'Invalid Serve shadow coverage row: {e}') from e


def _coverage_attempt_record(
        row: Mapping[str, Any]) -> actions.CoverageAttemptV1:
    try:
        return actions.CoverageAttemptV1.from_value({
            'decision_id': str(
                _canonical_uuid(row['decision_id'], name='decision_id')),
            'request_sequence': row['request_sequence'],
            'logical_attempt': row['logical_attempt'],
            'request_role': row['request_role'],
            'phase': row['phase'],
            'legacy_request_id': row['legacy_request_id'],
            'terminal_request_status': row['terminal_request_status'],
            'retry_disposition': row['retry_disposition'],
            'admitted_at': _utc_timestamp_text(row['admitted_at'],
                                               name='admitted_at'),
            'request_bound_at': (None if row['request_bound_at'] is None else
                                 _utc_timestamp_text(row['request_bound_at'],
                                                     name='request_bound_at')),
            'completed_at':
                (None if row['completed_at'] is None else _utc_timestamp_text(
                    row['completed_at'], name='completed_at')),
            'updated_at': _utc_timestamp_text(row['updated_at'],
                                              name='updated_at'),
        })
    except (KeyError, TypeError, ValueError) as e:
        raise kernel_actions.InvariantViolation(
            f'Invalid Serve shadow coverage attempt row: {e}') from e


def _sample_record(row: Mapping[str, Any]) -> ShadowSampleRecord:
    try:
        plan_value = _json_object(row['provider_plan'], name='provider_plan')
        plan = actions.ProviderLifecyclePlanV1.from_value(plan_value)
        _hash_matches(plan_value,
                      row['provider_plan_sha256'],
                      name='provider_plan')
        immutable_spec_value = _json_object(row['immutable_spec'],
                                            name='immutable_spec')
        immutable_spec = actions.ServeReplicaActionSpecV1.from_value(
            immutable_spec_value)
        _hash_matches(immutable_spec_value,
                      row['immutable_spec_sha256'],
                      name='immutable_spec')
        eligibility = actions.ProfileEligibility(row['profile_eligibility'])
        expected = NewShadowSample(service_name=row['service_name'],
                                   immutable_spec=immutable_spec,
                                   provider_plan=plan,
                                   profile_eligibility=eligibility)
        action_id = _canonical_uuid(row['would_be_action_id'],
                                    name='would_be_action_id')
        identity = plan.resource_identity
        if (action_id != expected.action_id or
                row['service_hash'] != identity.service_hash or _canonical_uuid(
                    row['service_incarnation'], name='service_incarnation')
                != identity.service_incarnation or
                row['replica_id'] != identity.replica_id or _canonical_uuid(
                    row['replica_incarnation'], name='replica_incarnation')
                != identity.replica_incarnation or
                row['desired_generation'] != identity.desired_generation or
                row['action_type'] != plan.action_kind.value or
                row['resource_identity'] != expected.resource_identity or
                row['immutable_spec_sha256'] != expected.immutable_spec_sha256):
            raise ValueError('logical identity/spec commitment differs from '
                             'its deterministic preimage.')
        phase = actions.ShadowParentPhase(row['phase'])
        parity = actions.ShadowParityClass(row['parity_class'])
        legacy = _typed_pair(row.get('legacy_projection'),
                             row.get('legacy_projection_sha256'),
                             name='legacy_projection',
                             reader=actions.ServeShadowProjectionV1.from_value)
        proposed = _typed_pair(
            row.get('proposed_projection'),
            row.get('proposed_projection_sha256'),
            name='proposed_projection',
            reader=actions.ServeShadowProjectionV1.from_value)
        revision = _positive_integer(row['revision'], name='revision')
        created_at = _timestamp(row['created_at'], name='created_at')
        updated_at = _timestamp(row['updated_at'], name='updated_at')
        completed_at = _optional_timestamp(row.get('completed_at'),
                                           name='completed_at')
        if updated_at < created_at or (completed_at is not None and
                                       completed_at < created_at):
            raise ValueError('sample timestamps are out of order.')
        if phase in (actions.ShadowParentPhase.PENDING,
                     actions.ShadowParentPhase.RUNNING):
            if (parity is not actions.ShadowParityClass.PENDING or
                    completed_at is not None):
                raise ValueError('nonterminal sample has terminal shape.')
        elif phase is actions.ShadowParentPhase.COMPLETE:
            if (legacy is None or proposed is None or completed_at is None or
                    parity in (actions.ShadowParityClass.PENDING,
                               actions.ShadowParityClass.ABANDONED,
                               actions.ShadowParityClass.AMBIGUOUS)):
                raise ValueError('complete sample has incomplete projections.')
        elif phase is actions.ShadowParentPhase.ABANDONED_PRE_SUBMIT:
            if (parity is not actions.ShadowParityClass.ABANDONED or
                    completed_at is None):
                raise ValueError('abandoned sample has an invalid shape.')
        elif (parity is not actions.ShadowParityClass.AMBIGUOUS or
              completed_at is None):
            raise ValueError('ambiguous sample has an invalid shape.')
        return ShadowSampleRecord(
            action_id=action_id,
            service_name=expected.service_name,
            immutable_spec=expected.immutable_spec,
            immutable_spec_sha256=expected.immutable_spec_sha256,
            provider_plan=plan,
            provider_plan_sha256=expected.provider_plan_sha256,
            profile_eligibility=eligibility,
            phase=phase,
            legacy_projection=legacy,
            proposed_projection=proposed,
            parity_class=parity,
            revision=revision,
            created_at=created_at,
            updated_at=updated_at,
            completed_at=completed_at)
    except (KeyError, TypeError, ValueError) as e:
        raise kernel_actions.InvariantViolation(
            f'Invalid Serve shadow sample row: {e}') from e


def _validate_invocation(
        sample: ShadowSampleRecord, role: actions.ShadowRequestRole,
        invocation: actions.ProviderLifecycleInvocationV1) -> None:
    sample.immutable_spec.validate_shadow_child_invocation(role, invocation)


def _attempt_record(row: Mapping[str, Any],
                    sample: ShadowSampleRecord) -> ShadowAttemptRecord:
    try:
        action_id = _canonical_uuid(row['would_be_action_id'],
                                    name='would_be_action_id')
        if action_id != sample.action_id:
            raise ValueError('attempt belongs to a different parent.')
        sequence = _positive_integer(row['request_sequence'],
                                     name='request_sequence')
        logical_attempt = _positive_integer(row['logical_attempt'],
                                            name='logical_attempt')
        role = actions.ShadowRequestRole(row['request_role'])
        execution = actions.PlannedExecutionKind(row['planned_execution_kind'])
        phase = actions.ShadowAttemptPhase(row['phase'])
        request_id = row.get('legacy_request_id')
        if request_id is not None:
            request_id = _bounded_text(request_id,
                                       name='legacy_request_id',
                                       maximum_bytes=128)
        provider_operation_id = row.get('provider_operation_id')
        if provider_operation_id is not None:
            provider_operation_id = _bounded_text(provider_operation_id,
                                                  name='provider_operation_id',
                                                  maximum_bytes=1024)
        invocation_value = _json_object(row['invocation'], name='invocation')
        invocation = actions.ProviderLifecycleInvocationV1.from_value(
            invocation_value)
        _hash_matches(invocation_value,
                      row['invocation_sha256'],
                      name='invocation')
        _validate_invocation(sample, role, invocation)
        actual = _typed_pair(
            row.get('actual_outcome'),
            row.get('actual_outcome_sha256'),
            name='actual_outcome',
            reader=actions.ServeReplicaActionOutcomeV1.from_value)
        proposed = _typed_pair(
            row.get('proposed_outcome'),
            row.get('proposed_outcome_sha256'),
            name='proposed_outcome',
            reader=actions.ServeReplicaActionOutcomeV1.from_value)
        retry = _typed_pair(
            row.get('retry_decision'),
            row.get('retry_decision_sha256'),
            name='retry_decision',
            reader=actions.ServeShadowRetryDecisionV1.from_value)
        pre_observation = _typed_pair(
            row.get('pre_observation'),
            row.get('pre_observation_sha256'),
            name='pre_observation',
            reader=actions.ProviderLifecycleObservationV1.from_value)
        post_observation = _typed_pair(
            row.get('post_observation'),
            row.get('post_observation_sha256'),
            name='post_observation',
            reader=actions.ProviderLifecycleObservationV1.from_value)
        divergence_value = row.get('divergence_class')
        divergence = (None if divergence_value is None else
                      actions.ShadowDivergenceClass(divergence_value))
        admitted_at = _timestamp(row['admitted_at'], name='admitted_at')
        bound_at = _optional_timestamp(row.get('request_bound_at'),
                                       name='request_bound_at')
        completed_at = _optional_timestamp(row.get('completed_at'),
                                           name='completed_at')
        updated_at = _timestamp(row['updated_at'], name='updated_at')
        if (updated_at < admitted_at or
            (bound_at is not None and bound_at < admitted_at) or
            (completed_at is not None and completed_at < admitted_at)):
            raise ValueError('attempt timestamps are out of order.')
        if retry is not None and retry.logical_attempt != logical_attempt:
            raise ValueError('retry decision logical attempt differs.')
        for observation in (pre_observation, post_observation):
            if observation is not None:
                observation.validate_target(
                    sample.provider_plan.requested_target)
        for outcome in (actual, proposed):
            if outcome is not None:
                outcome.validate_for_invocation(invocation)
        evidence = (actual, proposed, retry, pre_observation, post_observation,
                    divergence)
        if phase is actions.ShadowAttemptPhase.PRE_SUBMIT:
            if (request_id is not None or bound_at is not None or
                    completed_at is not None or
                    any(value is not None for value in evidence)):
                raise ValueError('pre-submit attempt has later evidence.')
        elif phase is actions.ShadowAttemptPhase.REQUEST_BOUND:
            if (execution is not actions.PlannedExecutionKind.API_REQUEST or
                    request_id is None or bound_at is None or
                    completed_at is not None or
                    any(value is not None for value in evidence)):
                raise ValueError('request-bound attempt has invalid evidence.')
        elif phase is actions.ShadowAttemptPhase.COMPLETE:
            if (completed_at is None or actual is None or proposed is None or
                    retry is None or
                (execution is actions.PlannedExecutionKind.API_REQUEST and
                 (request_id is None or bound_at is None))):
                raise ValueError('complete attempt lacks typed evidence.')
            if (execution is actions.PlannedExecutionKind.LEGACY_DIRECT_DOWN and
                    divergence is None):
                raise ValueError('direct down must remain divergent.')
        elif phase is actions.ShadowAttemptPhase.ABANDONED_PRE_SUBMIT:
            if (request_id is not None or bound_at is not None or
                    provider_operation_id is not None or completed_at is None or
                    any(value is not None for value in evidence)):
                raise ValueError('abandoned attempt has mutation evidence.')
        elif (execution is not actions.PlannedExecutionKind.API_REQUEST or
              request_id is not None or bound_at is not None or
              completed_at is None or
              any(value is not None for value in evidence)):
            raise ValueError('unknown request association has invalid shape.')
        operation_ids = _operation_ids(provider_operation_id, actual, proposed,
                                       pre_observation, post_observation)
        if len(operation_ids) > 1:
            raise ValueError('provider operation evidence conflicts.')
        if phase is actions.ShadowAttemptPhase.COMPLETE:
            assert actual is not None and proposed is not None
            if (actual.provider_operation_id != provider_operation_id or
                    proposed.provider_operation_id != provider_operation_id):
                raise ValueError('typed outcomes do not exactly carry the '
                                 'attempt provider operation ID.')
            if (divergence is None and
                    actual.canonical_bytes != proposed.canonical_bytes):
                raise ValueError(
                    'nondivergent actual/proposed outcomes differ.')
        return ShadowAttemptRecord(action_id=action_id,
                                   request_sequence=sequence,
                                   logical_attempt=logical_attempt,
                                   request_role=role,
                                   planned_execution_kind=execution,
                                   phase=phase,
                                   legacy_request_id=request_id,
                                   invocation=invocation,
                                   provider_operation_id=provider_operation_id,
                                   actual_outcome=actual,
                                   proposed_outcome=proposed,
                                   retry_decision=retry,
                                   pre_observation=pre_observation,
                                   post_observation=post_observation,
                                   divergence_class=divergence,
                                   admitted_at=admitted_at,
                                   request_bound_at=bound_at,
                                   completed_at=completed_at,
                                   updated_at=updated_at)
    except (KeyError, TypeError, ValueError) as e:
        raise kernel_actions.InvariantViolation(
            f'Invalid Serve shadow attempt row: {e}') from e


def _operation_ids(
    provider_operation_id: str | None,
    actual: actions.ServeReplicaActionOutcomeV1 | None,
    proposed: actions.ServeReplicaActionOutcomeV1 | None,
    pre_observation: actions.ProviderLifecycleObservationV1 | None,
    post_observation: actions.ProviderLifecycleObservationV1 | None,
) -> set[str]:
    values: list[str | None] = [provider_operation_id]
    for outcome in (actual, proposed):
        if outcome is not None:
            values.append(outcome.provider_operation_id)
            if outcome.observation is not None:
                values.append(
                    outcome.observation.observed_provider_operation_id)
    for observation in (pre_observation, post_observation):
        if observation is not None:
            values.append(observation.observed_provider_operation_id)
    return {value for value in values if value is not None}


def _outcome_with_provider_operation_id(
    outcome: actions.ServeReplicaActionOutcomeV1,
    provider_operation_id: str | None,
) -> actions.ServeReplicaActionOutcomeV1:
    if provider_operation_id is None:
        return outcome
    if (outcome.provider_operation_id is not None and
            outcome.provider_operation_id != provider_operation_id):
        raise ValueError('typed outcome carries a different provider '
                         'operation ID.')
    if outcome.provider_operation_id is not None:
        return outcome
    value = outcome.canonical_value()
    value['provider_operation_id'] = provider_operation_id
    return actions.ServeReplicaActionOutcomeV1.from_value(value)


def _validate_child_graph(
    sample: ShadowSampleRecord,
    attempts: list[ShadowAttemptRecord],
    *,
    require_closed: bool = False,
) -> list[str]:
    problems: list[str] = []
    sequences = [attempt.request_sequence for attempt in attempts]
    if sequences != list(range(1, len(attempts) + 1)):
        problems.append('noncontiguous_request_sequence')
    logical_attempts = sorted({attempt.logical_attempt for attempt in attempts})
    if logical_attempts and logical_attempts != list(
            range(1, logical_attempts[-1] + 1)):
        problems.append('noncontiguous_logical_attempt')
    for logical_attempt in logical_attempts:
        grouped = [
            attempt for attempt in attempts
            if attempt.logical_attempt == logical_attempt
        ]
        primary_rows = [
            attempt for attempt in grouped
            if attempt.request_role in _PRIMARY_ROLES
        ]
        if len(primary_rows) != 1:
            problems.append(f'logical_attempt_{logical_attempt}_primary_count')
    expected_role = (actions.ShadowRequestRole.PRIMARY_LAUNCH
                     if sample.action_kind is kernel_actions.ActionKind.LAUNCH
                     else actions.ShadowRequestRole.PRIMARY_DOWN)
    if any(attempt.request_role in _PRIMARY_ROLES and
           attempt.request_role is not expected_role for attempt in attempts):
        problems.append('primary_role_action_mismatch')
    for attempt in attempts[:-1]:
        if attempt.phase is not actions.ShadowAttemptPhase.COMPLETE:
            problems.append(
                f'noncomplete_attempt_{attempt.request_sequence}_has_successor')
    for logical_attempt in logical_attempts:
        grouped = [
            attempt for attempt in attempts
            if attempt.logical_attempt == logical_attempt
        ]
        if not grouped:
            continue
        if grouped[0].request_role not in _PRIMARY_ROLES:
            problems.append(
                f'logical_attempt_{logical_attempt}_primary_not_first')
            continue
        primary = grouped[0]
        cleanups = [
            attempt for attempt in grouped if attempt.request_role is
            actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN
        ]
        if cleanups and (primary.phase
                         is not actions.ShadowAttemptPhase.COMPLETE or
                         primary.actual_outcome is None or
                         primary.actual_outcome.disposition
                         is actions.ServeActionDisposition.SUCCEEDED):
            problems.append(
                f'logical_attempt_{logical_attempt}_cleanup_without_failure')
        for cleanup_index in range(1, len(cleanups)):
            previous = cleanups[cleanup_index - 1]
            retry = previous.retry_decision
            if (previous.phase is not actions.ShadowAttemptPhase.COMPLETE or
                    retry is None or retry.decision
                    is not actions.ShadowRetryDecision.RETRY_SAME_PLAN):
                problems.append(
                    f'logical_attempt_{logical_attempt}_cleanup_retry_fence')
        if logical_attempt == logical_attempts[-1]:
            continue
        retry = primary.retry_decision
        if (primary.phase is not actions.ShadowAttemptPhase.COMPLETE or
                retry is None or retry.decision
                is not actions.ShadowRetryDecision.RETRY_SAME_PLAN):
            problems.append(
                f'logical_attempt_{logical_attempt}_primary_retry_fence')
        if cleanups:
            latest = cleanups[-1]
            latest_retry = latest.retry_decision
            latest_outcome = latest.actual_outcome
            if (latest.phase is not actions.ShadowAttemptPhase.COMPLETE or
                    latest_retry is None or latest_retry.decision
                    is not actions.ShadowRetryDecision.TERMINAL or
                    latest_outcome is None or latest_outcome.disposition
                    is not actions.ServeActionDisposition.SUCCEEDED):
                problems.append(
                    f'logical_attempt_{logical_attempt}_cleanup_terminal_fence')
        else:
            outcome = primary.actual_outcome
            observation = None if outcome is None else outcome.observation
            if (observation is None or observation.state
                    is not actions.ProviderObservationState.ABSENT or
                    observation.certainty
                    is not actions.ProviderObservationCertainty.AUTHORITATIVE):
                problems.append(
                    f'logical_attempt_{logical_attempt}_safe_relaunch_proof')
    if require_closed:
        for attempt in attempts:
            retry = attempt.retry_decision
            if (retry is None or retry.decision
                    is not actions.ShadowRetryDecision.RETRY_SAME_PLAN):
                continue
            if attempt.request_role in _PRIMARY_ROLES:
                has_successor = any(
                    candidate.request_role in _PRIMARY_ROLES and
                    candidate.logical_attempt == attempt.logical_attempt + 1
                    for candidate in attempts)
                successor_kind = 'primary'
            else:
                has_successor = any(
                    candidate.request_role is
                    actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN and
                    candidate.logical_attempt == attempt.logical_attempt and
                    candidate.request_sequence > attempt.request_sequence
                    for candidate in attempts)
                successor_kind = 'cleanup'
            if not has_successor:
                problems.append(f'attempt_{attempt.request_sequence}_missing_'
                                f'{successor_kind}_retry_successor')
    return problems


def _validate_coverage_attempt_graph(
    coverage: actions.CoverageDecisionV1,
    attempts: list[actions.CoverageAttemptV1],
) -> list[str]:
    """Validate the ordered coverage-only retry graph for promotion."""
    problems: list[str] = []
    sequences = [attempt.request_sequence for attempt in attempts]
    if sequences != list(range(1, len(attempts) + 1)):
        problems.append('noncontiguous_request_sequence')
    logical_attempts = sorted({attempt.logical_attempt for attempt in attempts})
    if logical_attempts and logical_attempts != list(
            range(1, logical_attempts[-1] + 1)):
        problems.append('noncontiguous_logical_attempt')
    expected_primary = (actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH if
                        coverage.action_type is kernel_actions.ActionKind.LAUNCH
                        else actions.CoverageAttemptRequestRole.PRIMARY_DOWN)
    for logical_attempt in logical_attempts:
        grouped = [
            attempt for attempt in attempts
            if attempt.logical_attempt == logical_attempt
        ]
        primaries = [
            attempt for attempt in grouped
            if attempt.request_role in _PRIMARY_ROLES
        ]
        if len(primaries) != 1:
            problems.append(f'logical_attempt_{logical_attempt}_primary_count')
        elif primaries[0].request_role is not expected_primary:
            problems.append('primary_role_action_mismatch')
        if grouped and grouped[0].request_role not in _PRIMARY_ROLES:
            problems.append(
                f'logical_attempt_{logical_attempt}_primary_not_first')
        if (coverage.action_type is kernel_actions.ActionKind.DOWN and
                any(attempt.request_role is
                    actions.CoverageAttemptRequestRole.LAUNCH_CLEANUP_DOWN
                    for attempt in grouped)):
            problems.append('down_decision_has_cleanup')
    for attempt in attempts[:-1]:
        if (attempt.phase is not actions.CoverageAttemptPhase.COMPLETE or
                attempt.retry_disposition is not actions.
                CoverageAttemptRetryDisposition.RETRY_SAME_DECISION):
            problems.append(
                f'attempt_{attempt.request_sequence}_invalid_successor_fence')
    for attempt in attempts:
        if attempt.phase is not actions.CoverageAttemptPhase.COMPLETE:
            problems.append(
                f'attempt:{attempt.request_sequence}:phase:{attempt.phase.value}'
            )
        if (attempt.retry_disposition
                is actions.CoverageAttemptRetryDisposition.RETRY_SAME_DECISION):
            if attempt.request_role in _PRIMARY_ROLES:
                has_successor = any(
                    candidate.request_role in _PRIMARY_ROLES and
                    candidate.logical_attempt == attempt.logical_attempt + 1
                    for candidate in attempts)
                successor_kind = 'primary'
            else:
                has_successor = any(
                    candidate.request_role is
                    actions.CoverageAttemptRequestRole.LAUNCH_CLEANUP_DOWN and
                    candidate.logical_attempt == attempt.logical_attempt and
                    candidate.request_sequence > attempt.request_sequence
                    for candidate in attempts)
                successor_kind = 'cleanup'
            if not has_successor:
                problems.append(f'attempt_{attempt.request_sequence}_missing_'
                                f'{successor_kind}_retry_successor')
    return problems


def _validate_terminal_coverage_attempt_graph(
    coverage: actions.CoverageDecisionV1,
    attempts: list[actions.CoverageAttemptV1],
) -> list[str]:
    """Validate retention-terminal coverage, including final abandonment."""
    problems = _validate_coverage_attempt_graph(coverage, attempts)
    abandoned_phase_problems = {
        f'attempt:{attempt.request_sequence}:phase:'
        f'{actions.CoverageAttemptPhase.ABANDONED_PRE_SUBMIT.value}'
        for attempt in attempts
        if attempt.phase is actions.CoverageAttemptPhase.ABANDONED_PRE_SUBMIT
    }
    return [
        problem for problem in problems
        if problem not in abandoned_phase_problems
    ]


def _validate_match_evidence(
    sample: ShadowSampleRecord,
    attempts: list[ShadowAttemptRecord],
    legacy_projection: actions.ServeShadowProjectionV1,
    proposed_projection: actions.ServeShadowProjectionV1,
) -> None:
    """Recompute the evidence required for a promotion-eligible MATCH."""
    if sample.profile_eligibility is not actions.ProfileEligibility.ELIGIBLE:
        raise ValueError('provider profile is not authoritative-eligible.')
    if legacy_projection.canonical_bytes != proposed_projection.canonical_bytes:
        raise ValueError('final projections are not byte-equal.')
    if not attempts:
        raise ValueError('MATCH requires at least one child attempt.')
    for attempt in attempts:
        if (attempt.phase is not actions.ShadowAttemptPhase.COMPLETE or
                attempt.divergence_class is not None or
                attempt.planned_execution_kind
                is not actions.PlannedExecutionKind.API_REQUEST or
                attempt.actual_outcome is None or
                attempt.proposed_outcome is None or
                attempt.actual_outcome.canonical_bytes
                != attempt.proposed_outcome.canonical_bytes):
            raise ValueError('child evidence is incomplete or divergent.')
    primary_attempts = [
        attempt for attempt in attempts
        if attempt.request_role in _PRIMARY_ROLES
    ]
    final_primary = max(primary_attempts,
                        key=lambda value: value.logical_attempt)
    assert final_primary.actual_outcome is not None
    outcome = final_primary.actual_outcome
    if legacy_projection.action_disposition is not outcome.disposition:
        raise ValueError('final projection disposition differs from the final '
                         'primary outcome.')
    if outcome.disposition is actions.ServeActionDisposition.SUCCEEDED:
        observation = outcome.observation
        assert observation is not None
        if sample.action_kind is kernel_actions.ActionKind.LAUNCH:
            if (legacy_projection.row_disposition
                    is not actions.ShadowRowDisposition.RETAINED or
                    legacy_projection.replica_status
                    is not actions.ReplicaStatusValue.READY or
                    legacy_projection.capacity_outcome
                    is not actions.ShadowCapacityOutcome.SUCCESS or
                    legacy_projection.resolved_target is None or
                    observation.resolved_target is None or
                    legacy_projection.resolved_target.canonical_bytes
                    != observation.resolved_target.canonical_bytes):
                raise ValueError('launch success projection must retain a '
                                 'ready replica with successful capacity and '
                                 'the exact resolved target.')
        elif (legacy_projection.row_disposition
              is not actions.ShadowRowDisposition.REMOVED or
              legacy_projection.resolved_target is not None):
            raise ValueError('down success projection must remove the replica '
                             'without a resolved target.')


class PostgresServeResourceActionStateStore:
    """Typed PostgreSQL shadow journal; no provider or API-action side effects."""

    def __init__(self, engine: sqlalchemy.engine.Engine | None = None) -> None:
        self._engine = engine
        if engine is not None:
            self._require_postgres(engine)

    @staticmethod
    def _require_postgres(bind: Any) -> None:
        dialect = bind.dialect
        if dialect.name != 'postgresql':
            raise RuntimeError('SkyServe resource-action state requires '
                               'PostgreSQL; refusing non-PostgreSQL access.')

    def _database(self) -> sqlalchemy.engine.Engine:
        engine = self._engine or serve_state_schema.get_database_engine()
        self._require_postgres(engine)
        return engine

    def _require_session(self, session: orm.Session) -> None:
        self._require_postgres(session.get_bind())

    @staticmethod
    def _locked_worker_cohort(session: orm.Session,
                              cohort_id: str) -> Mapping[str, Any] | None:
        return session.execute(
            sqlalchemy.select(state_schema.WORKER_COHORTS).where(
                state_schema.WORKER_COHORTS.c.cohort_id ==
                cohort_id).with_for_update()).mappings().first()

    def register_worker_cohort_in_session(
        self,
        session: orm.Session,
        cohort_identity: actions.WorkerCohortIdentityV1,
        registration_attestations: actions.WorkerCohortRegistrationSetV1,
    ) -> WorkerCohortTransition:
        """Insert or exactly adopt one immutable REGISTERING cohort."""
        self._require_session(session)
        if not isinstance(cohort_identity, actions.WorkerCohortIdentityV1):
            raise TypeError('cohort_identity has an invalid type.')
        if not isinstance(registration_attestations,
                          actions.WorkerCohortRegistrationSetV1):
            raise TypeError('registration_attestations has an invalid type.')
        registration_attestations.validate_for_cohort(cohort_identity,
                                                      require_two=False)
        if registration_attestations.count not in (1, 2):
            raise ValueError('A registering cohort requires one or two '
                             'worker attestations.')
        database_now = session.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        _validate_current_worker_registrations(cohort_identity,
                                               registration_attestations,
                                               database_now,
                                               require_two=False)
        cohort_id = _bounded_text(cohort_identity.manifest.cohort_id,
                                  name='cohort_id',
                                  maximum_bytes=1024)
        deployment_uid = _bounded_text(cohort_identity.deployment_uid,
                                       name='deployment_uid',
                                       maximum_bytes=1024)
        table = state_schema.WORKER_COHORTS
        inserted = session.execute(
            postgresql.insert(table).values(
                cohort_id=cohort_id,
                deployment_uid=deployment_uid,
                cohort_identity=cohort_identity.canonical_value(),
                cohort_identity_sha256=cohort_identity.sha256,
                registration_attestations=(
                    registration_attestations.canonical_value()),
                registration_attestations_sha256=(
                    registration_attestations.sha256),
                lifecycle_state=(
                    actions.WorkerCohortLifecycleState.REGISTERING.value),
                revision=1,
                created_at=sqlalchemy.func.clock_timestamp(),
                state_changed_at=sqlalchemy.func.clock_timestamp(),
                retired_at=None).on_conflict_do_nothing().returning(
                    table.c.cohort_id)).scalar_one_or_none()
        row = self._locked_worker_cohort(session, cohort_id)
        if row is None:
            raise kernel_actions.ActionConflict(
                'Worker cohort deployment UID belongs to another cohort.')
        record = _worker_cohort_record(row)
        if (record.cohort_identity.canonical_bytes
                != cohort_identity.canonical_bytes or
                record.registration_attestations.canonical_bytes
                != registration_attestations.canonical_bytes or
                record.lifecycle_state
                is not actions.WorkerCohortLifecycleState.REGISTERING or
                record.revision != 1):
            raise kernel_actions.ActionConflict(
                'Worker cohort ID already has different immutable bytes or '
                'registration state.')
        return WorkerCohortTransition(record, adopted=inserted is None)

    def register_worker_cohort(
        self,
        cohort_identity: actions.WorkerCohortIdentityV1,
        registration_attestations: actions.WorkerCohortRegistrationSetV1,
    ) -> WorkerCohortTransition:
        with orm.Session(self._database()) as session, session.begin():
            return self.register_worker_cohort_in_session(
                session, cohort_identity, registration_attestations)

    def get_worker_cohort(self, cohort_id: str) -> WorkerCohortRecord | None:
        parsed_id = _bounded_text(cohort_id,
                                  name='cohort_id',
                                  maximum_bytes=1024)
        with orm.Session(self._database()) as session:
            row = session.execute(
                sqlalchemy.select(state_schema.WORKER_COHORTS).where(
                    state_schema.WORKER_COHORTS.c.cohort_id ==
                    parsed_id)).mappings().first()
        return None if row is None else _worker_cohort_record(row)

    def transition_worker_cohort_in_session(
        self,
        session: orm.Session,
        cohort_id: str,
        expected_revision: int,
        expected_state: actions.WorkerCohortLifecycleState,
        new_state: actions.WorkerCohortLifecycleState,
        *,
        registration_attestations: (actions.WorkerCohortRegistrationSetV1 |
                                    None) = None,
    ) -> WorkerCohortTransition:
        """Apply only transitions whose complete evidence exists in Serve033."""
        self._require_session(session)
        parsed_id = _bounded_text(cohort_id,
                                  name='cohort_id',
                                  maximum_bytes=1024)
        expected_revision = _positive_integer(expected_revision,
                                              name='expected_revision')
        old_state = (expected_state if isinstance(
            expected_state, actions.WorkerCohortLifecycleState) else
                     actions.WorkerCohortLifecycleState(expected_state))
        target_state = (new_state if isinstance(
            new_state, actions.WorkerCohortLifecycleState) else
                        actions.WorkerCohortLifecycleState(new_state))
        if registration_attestations is not None and not isinstance(
                registration_attestations,
                actions.WorkerCohortRegistrationSetV1):
            raise TypeError('registration_attestations has an invalid type.')
        row = self._locked_worker_cohort(session, parsed_id)
        if row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown worker cohort {parsed_id!r}.')
        current = _worker_cohort_record(row)
        replacement = (current.registration_attestations
                       if registration_attestations is None else
                       registration_attestations)
        replacement.validate_for_cohort(current.cohort_identity,
                                        require_two=False)
        same_registration = (replacement.canonical_bytes ==
                             current.registration_attestations.canonical_bytes)
        transition = (old_state, target_state)
        allowed = {
            (actions.WorkerCohortLifecycleState.REGISTERING,
             actions.WorkerCohortLifecycleState.REGISTERING),
            (actions.WorkerCohortLifecycleState.REGISTERING,
             actions.WorkerCohortLifecycleState.ACCEPTING),
            (actions.WorkerCohortLifecycleState.ACCEPTING,
             actions.WorkerCohortLifecycleState.DRAINING),
            (actions.WorkerCohortLifecycleState.DRAINING,
             actions.WorkerCohortLifecycleState.ACCEPTING),
        }
        if transition not in allowed:
            raise ValueError(
                'Cohort transition lacks the required reviewed evidence path.')
        if (current.lifecycle_state is target_state and same_registration and
                current.revision == expected_revision + 1):
            return WorkerCohortTransition(current, adopted=True)
        if (current.lifecycle_state is not old_state or
                current.revision != expected_revision):
            raise kernel_actions.StaleRevision(
                'Worker cohort lifecycle revision/state changed.')
        database_now = session.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        if transition == (actions.WorkerCohortLifecycleState.REGISTERING,
                          actions.WorkerCohortLifecycleState.REGISTERING):
            if registration_attestations is None or same_registration:
                raise ValueError('REGISTERING evidence update must append a '
                                 'new worker attestation.')
            old_workers = {
                item['worker']['pod_uid']: actions.canonical_json_bytes(item)
                for item in current.registration_attestations.canonical_value()
                ['workers']
            }
            new_workers = {
                item['worker']['pod_uid']: actions.canonical_json_bytes(item)
                for item in replacement.canonical_value()['workers']
            }
            if (len(new_workers) <= len(old_workers) or any(
                    new_workers.get(key) != value
                    for key, value in old_workers.items())):
                raise kernel_actions.ActionConflict(
                    'REGISTERING attestations must append without replacing '
                    'existing worker evidence.')
            _validate_current_worker_registrations(current.cohort_identity,
                                                   replacement,
                                                   database_now,
                                                   require_two=False)
        elif target_state is actions.WorkerCohortLifecycleState.ACCEPTING:
            if (old_state is actions.WorkerCohortLifecycleState.DRAINING and
                    registration_attestations is None):
                raise ValueError('DRAINING rollback requires replacement '
                                 'two-worker evidence.')
            _validate_current_worker_registrations(current.cohort_identity,
                                                   replacement,
                                                   database_now,
                                                   require_two=True)
        elif registration_attestations is not None and not same_registration:
            raise ValueError('ACCEPTING -> DRAINING cannot rewrite worker '
                             'registration evidence.')
        values: dict[str, Any] = {
            'registration_attestations': replacement.canonical_value(),
            'registration_attestations_sha256': replacement.sha256,
            'lifecycle_state': target_state.value,
            'revision': expected_revision + 1,
        }
        if target_state is not old_state:
            values['state_changed_at'] = sqlalchemy.func.clock_timestamp()
        updated = session.execute(
            sqlalchemy.update(state_schema.WORKER_COHORTS).where(
                state_schema.WORKER_COHORTS.c.cohort_id == parsed_id,
                state_schema.WORKER_COHORTS.c.lifecycle_state ==
                old_state.value, state_schema.WORKER_COHORTS.c.revision ==
                expected_revision).values(**values))
        if updated.rowcount != 1:
            raise kernel_actions.StaleRevision(
                'Worker cohort changed during lifecycle transition.')
        updated_row = self._locked_worker_cohort(session, parsed_id)
        assert updated_row is not None
        return WorkerCohortTransition(_worker_cohort_record(updated_row))

    def transition_worker_cohort(
        self,
        cohort_id: str,
        expected_revision: int,
        expected_state: actions.WorkerCohortLifecycleState,
        new_state: actions.WorkerCohortLifecycleState,
        *,
        registration_attestations: (actions.WorkerCohortRegistrationSetV1 |
                                    None) = None,
    ) -> WorkerCohortTransition:
        with orm.Session(self._database()) as session, session.begin():
            return self.transition_worker_cohort_in_session(
                session,
                cohort_id,
                expected_revision,
                expected_state,
                new_state,
                registration_attestations=registration_attestations)

    @staticmethod
    def _locked_worker_cohort_reference(
            session: orm.Session,
            decision_id: uuid.UUID) -> Mapping[str, Any] | None:
        return session.execute(
            sqlalchemy.select(state_schema.WORKER_COHORT_REFS).where(
                state_schema.WORKER_COHORT_REFS.c.decision_id ==
                decision_id).with_for_update()).mappings().first()

    def prepare_worker_cohort_reference_in_session(
        self,
        session: orm.Session,
        reference: actions.WorkerCohortReferenceInputV1,
    ) -> WorkerCohortReferenceTransition:
        """Insert or exactly adopt a nonauthorizing PREPARING reference."""
        self._require_session(session)
        if not isinstance(reference, actions.WorkerCohortReferenceInputV1):
            raise TypeError('reference has an invalid type.')
        cohort_row = self._locked_worker_cohort(session, reference.cohort_id)
        if cohort_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown worker cohort {reference.cohort_id!r}.')
        cohort = _worker_cohort_record(cohort_row)
        if cohort.lifecycle_state is not actions.WorkerCohortLifecycleState.ACCEPTING:
            raise kernel_actions.ClaimLost(
                'New preparation references require an accepting cohort.')
        table = state_schema.WORKER_COHORT_REFS
        inserted = session.execute(
            postgresql.insert(table).values(
                decision_id=reference.decision_id,
                cohort_id=reference.cohort_id,
                service_hash=reference.service_hash,
                replica_incarnation=reference.replica_incarnation,
                desired_generation=reference.desired_generation,
                action_type=reference.action_type.value,
                controller_owner_fence=reference.controller_owner_fence,
                lifecycle_epoch=reference.lifecycle_epoch,
                preparation_capability_sha256=(
                    reference.preparation_capability_sha256),
                reference_state=(
                    actions.WorkerCohortReferenceState.PREPARING.value),
                revision=1,
                created_at=sqlalchemy.func.clock_timestamp(),
                bound_at=None,
                released_at=None).on_conflict_do_nothing().returning(
                    table.c.decision_id)).scalar_one_or_none()
        row = self._locked_worker_cohort_reference(session,
                                                   reference.decision_id)
        if row is None:
            raise kernel_actions.ActionConflict(
                'Preparation reference conflicted without an adoptable row.')
        record = _worker_cohort_reference_record(row)
        if (record.reference != reference or record.reference_state
                is not actions.WorkerCohortReferenceState.PREPARING or
                record.revision != 1):
            raise kernel_actions.ActionConflict(
                'Decision already has different preparation reference bytes '
                'or lifecycle state.')
        return WorkerCohortReferenceTransition(record, adopted=inserted is None)

    def prepare_worker_cohort_reference(
        self,
        reference: actions.WorkerCohortReferenceInputV1,
    ) -> WorkerCohortReferenceTransition:
        with orm.Session(self._database()) as session, session.begin():
            return self.prepare_worker_cohort_reference_in_session(
                session, reference)

    def get_worker_cohort_reference(
            self, decision_id: uuid.UUID) -> WorkerCohortReferenceRecord | None:
        parsed = _canonical_uuid(decision_id, name='decision_id')
        with orm.Session(self._database()) as session:
            row = session.execute(
                sqlalchemy.select(state_schema.WORKER_COHORT_REFS).where(
                    state_schema.WORKER_COHORT_REFS.c.decision_id ==
                    parsed)).mappings().first()
        return None if row is None else _worker_cohort_reference_record(row)

    def bind_worker_cohort_reference_in_session(
        self,
        session: orm.Session,
        reference: actions.WorkerCohortReferenceInputV1,
        expected_revision: int,
        new_state: actions.WorkerCohortReferenceState,
    ) -> WorkerCohortReferenceTransition:
        """Bind PREPARING to one active state under its immutable fences."""
        self._require_session(session)
        if not isinstance(reference, actions.WorkerCohortReferenceInputV1):
            raise TypeError('reference has an invalid type.')
        expected_revision = _positive_integer(expected_revision,
                                              name='expected_revision')
        target = (new_state
                  if isinstance(new_state, actions.WorkerCohortReferenceState)
                  else actions.WorkerCohortReferenceState(new_state))
        if target not in (actions.WorkerCohortReferenceState.SHADOW_ACTIVE,
                          actions.WorkerCohortReferenceState.ACTION_ACTIVE):
            raise ValueError('A preparation can bind only to an active state.')
        cohort_row = self._locked_worker_cohort(session, reference.cohort_id)
        if cohort_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown worker cohort {reference.cohort_id!r}.')
        cohort = _worker_cohort_record(cohort_row)
        if cohort.lifecycle_state not in (
                actions.WorkerCohortLifecycleState.ACCEPTING,
                actions.WorkerCohortLifecycleState.DRAINING):
            raise kernel_actions.ClaimLost(
                'Prepared work cannot bind to this cohort lifecycle state.')
        row = self._locked_worker_cohort_reference(session,
                                                   reference.decision_id)
        if row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown cohort reference {reference.decision_id}.')
        current = _worker_cohort_reference_record(row)
        if current.reference != reference:
            raise kernel_actions.ClaimLost(
                'Preparation owner/lifecycle or decision identity changed.')
        if (current.reference_state is target and
                current.revision == expected_revision + 1):
            return WorkerCohortReferenceTransition(current, adopted=True)
        if (current.reference_state
                is not actions.WorkerCohortReferenceState.PREPARING or
                current.revision != expected_revision):
            raise kernel_actions.StaleRevision(
                'Preparation reference is no longer the expected revision.')
        updated = session.execute(
            sqlalchemy.update(state_schema.WORKER_COHORT_REFS).where(
                state_schema.WORKER_COHORT_REFS.c.decision_id ==
                reference.decision_id,
                state_schema.WORKER_COHORT_REFS.c.reference_state ==
                actions.WorkerCohortReferenceState.PREPARING.value,
                state_schema.WORKER_COHORT_REFS.c.revision ==
                expected_revision).values(
                    reference_state=target.value,
                    revision=expected_revision + 1,
                    bound_at=sqlalchemy.func.clock_timestamp()))
        if updated.rowcount != 1:
            raise kernel_actions.StaleRevision(
                'Preparation reference changed during binding.')
        updated_row = self._locked_worker_cohort_reference(
            session, reference.decision_id)
        assert updated_row is not None
        return WorkerCohortReferenceTransition(
            _worker_cohort_reference_record(updated_row))

    @staticmethod
    def _locked_shadow_coverage(
            session: orm.Session,
            decision_id: uuid.UUID) -> Mapping[str, Any] | None:
        return session.execute(
            sqlalchemy.select(state_schema.SHADOW_COVERAGE).where(
                state_schema.SHADOW_COVERAGE.c.decision_id ==
                decision_id).with_for_update()).mappings().first()

    @staticmethod
    def _locked_coverage_attempts(
            session: orm.Session,
            decision_id: uuid.UUID) -> list[actions.CoverageAttemptV1]:
        rows = session.execute(
            sqlalchemy.select(state_schema.SHADOW_COVERAGE_ATTEMPTS).where(
                state_schema.SHADOW_COVERAGE_ATTEMPTS.c.decision_id ==
                decision_id).order_by(
                    state_schema.SHADOW_COVERAGE_ATTEMPTS.c.request_sequence).
            with_for_update()).mappings().all()
        return [_coverage_attempt_record(row) for row in rows]

    def admit_shadow_coverage_in_session(
        self,
        session: orm.Session,
        new_coverage: NewShadowCoverage,
    ) -> ShadowCoverageAdmission:
        """Insert or exactly adopt one immutable deterministic decision."""
        self._require_session(session)
        if not isinstance(new_coverage, NewShadowCoverage):
            raise TypeError('new_coverage has an invalid type.')
        reference_id = new_coverage.worker_cohort_ref_id
        if reference_id is not None:
            optimistic_reference = session.execute(
                sqlalchemy.select(
                    state_schema.WORKER_COHORT_REFS.c.cohort_id).where(
                        state_schema.WORKER_COHORT_REFS.c.decision_id ==
                        reference_id)).scalar_one_or_none()
            if optimistic_reference is None:
                raise kernel_actions.InvariantViolation(
                    'Coverage references an unknown worker cohort fence.')
            cohort_row = self._locked_worker_cohort(session,
                                                    optimistic_reference)
            if cohort_row is None:
                raise kernel_actions.InvariantViolation(
                    'Coverage references an unknown worker cohort.')
            cohort = _worker_cohort_record(cohort_row)
            if cohort.lifecycle_state not in (
                    actions.WorkerCohortLifecycleState.ACCEPTING,
                    actions.WorkerCohortLifecycleState.DRAINING):
                raise kernel_actions.ClaimLost(
                    'Coverage cannot bind to this cohort lifecycle state.')
            reference_row = self._locked_worker_cohort_reference(
                session, reference_id)
            if reference_row is None:
                raise kernel_actions.InvariantViolation(
                    'Coverage reference disappeared during admission.')
            reference = _worker_cohort_reference_record(reference_row)
            identity = new_coverage.identity
            if (reference.reference_state
                    is not actions.WorkerCohortReferenceState.SHADOW_ACTIVE or
                    reference.cohort_id != cohort.cohort_id or
                    reference.decision_id != new_coverage.decision_id or
                    reference.reference.service_hash != identity.service_hash or
                    reference.reference.replica_incarnation
                    != identity.replica_incarnation or
                    reference.reference.desired_generation
                    != identity.desired_generation or
                    reference.reference.action_type
                    is not identity.action_type):
                raise kernel_actions.ClaimLost(
                    'Coverage differs from its active preparation reference.')
        admitted_at = session.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        return self._insert_or_adopt_shadow_coverage_in_session(
            session, new_coverage, admitted_at)

    def _insert_or_adopt_shadow_coverage_in_session(
        self,
        session: orm.Session,
        new_coverage: NewShadowCoverage,
        admitted_at: datetime.datetime,
    ) -> ShadowCoverageAdmission:
        """Insert/adopt coverage at a DB timestamp already sampled by caller."""
        admitted_at = _timestamp(admitted_at, name='admitted_at')
        reference_id = new_coverage.worker_cohort_ref_id
        identity = new_coverage.identity
        reason = new_coverage.not_representable_reason
        table = state_schema.SHADOW_COVERAGE
        inserted = session.execute(
            postgresql.insert(table).values(
                decision_id=new_coverage.decision_id,
                service_name=new_coverage.service_name,
                service_hash=identity.service_hash,
                service_incarnation=identity.service_incarnation,
                replica_id=identity.replica_id,
                replica_incarnation=identity.replica_incarnation,
                desired_generation=identity.desired_generation,
                action_type=identity.action_type.value,
                normalizer_contract_version=1,
                normalization_outcome=(
                    new_coverage.normalization_outcome.value),
                not_representable_reason=(None
                                          if reason is None else reason.value),
                worker_cohort_ref_id=reference_id,
                admitted_at=admitted_at).on_conflict_do_nothing().returning(
                    table.c.decision_id)).scalar_one_or_none()
        row = self._locked_shadow_coverage(session, new_coverage.decision_id)
        if row is None:
            natural_row = session.execute(
                sqlalchemy.select(table.c.decision_id).where(
                    table.c.service_hash == identity.service_hash,
                    table.c.service_incarnation == identity.service_incarnation,
                    table.c.replica_id == identity.replica_id,
                    table.c.replica_incarnation == identity.replica_incarnation,
                    table.c.desired_generation == identity.desired_generation,
                    table.c.action_type ==
                    identity.action_type.value).with_for_update()).first()
            detail = ('identity belongs to another decision ID' if natural_row
                      is not None else 'conflicted without an adoptable row')
            raise kernel_actions.ActionConflict(f'Shadow coverage {detail}.')
        record = _shadow_coverage_record(row)
        if (record.service_name != new_coverage.service_name or
                record.identity != identity or record.normalization_outcome
                is not new_coverage.normalization_outcome or
                record.not_representable_reason is not reason or
                record.worker_cohort_ref_id != reference_id):
            raise kernel_actions.ActionConflict(
                'Decision ID already has different immutable coverage bytes.')
        return ShadowCoverageAdmission(record, adopted=inserted is None)

    def admit_shadow_coverage(
            self, new_coverage: NewShadowCoverage) -> ShadowCoverageAdmission:
        with orm.Session(self._database()) as session, session.begin():
            return self.admit_shadow_coverage_in_session(session, new_coverage)

    def get_shadow_coverage(
            self, decision_id: uuid.UUID) -> actions.CoverageDecisionV1 | None:
        parsed = _canonical_uuid(decision_id, name='decision_id')
        with orm.Session(self._database()) as session:
            row = session.execute(
                sqlalchemy.select(state_schema.SHADOW_COVERAGE).where(
                    state_schema.SHADOW_COVERAGE.c.decision_id ==
                    parsed)).mappings().first()
        return None if row is None else _shadow_coverage_record(row)

    def list_coverage_attempts(
            self, decision_id: uuid.UUID) -> list[actions.CoverageAttemptV1]:
        parsed = _canonical_uuid(decision_id, name='decision_id')
        with orm.Session(self._database()) as session:
            coverage_row = session.execute(
                sqlalchemy.select(state_schema.SHADOW_COVERAGE).where(
                    state_schema.SHADOW_COVERAGE.c.decision_id ==
                    parsed)).mappings().first()
            if coverage_row is None:
                raise kernel_actions.InvariantViolation(
                    f'Unknown shadow coverage {parsed}.')
            _shadow_coverage_record(coverage_row)
            rows = session.execute(
                sqlalchemy.select(state_schema.SHADOW_COVERAGE_ATTEMPTS).where(
                    state_schema.SHADOW_COVERAGE_ATTEMPTS.c.decision_id ==
                    parsed).order_by(state_schema.SHADOW_COVERAGE_ATTEMPTS.c.
                                     request_sequence)).mappings().all()
        return [_coverage_attempt_record(row) for row in rows]

    def _insert_coverage_attempt_after_external_fences_in_session(
        self,
        session: orm.Session,
        decision_id: uuid.UUID,
        request_sequence: int,
        logical_attempt: int,
        request_role: actions.CoverageAttemptRequestRole,
    ) -> CoverageAttemptTransition:
        """Low-level PRE_SUBMIT insert after externally established fences.

        This is deliberately private and has no transaction-owning wrapper. It
        is not a provider-submission gate: the future manager integration must
        first lock and validate the service owner, replica coverage link,
        cohort/reference, one-use authorization, and cancellation fences in
        the reviewed global order, then call this helper in that transaction.
        """
        self._require_session(session)
        parsed = _canonical_uuid(decision_id, name='decision_id')
        sequence = _positive_integer(request_sequence, name='request_sequence')
        logical = _positive_integer(logical_attempt, name='logical_attempt')
        role = (request_role
                if isinstance(request_role, actions.CoverageAttemptRequestRole)
                else actions.CoverageAttemptRequestRole(request_role))
        coverage_row = self._locked_shadow_coverage(session, parsed)
        if coverage_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown shadow coverage {parsed}.')
        coverage = _shadow_coverage_record(coverage_row)
        if coverage.normalization_outcome is not actions.NormalizationOutcome.NOT_REPRESENTABLE:
            raise kernel_actions.ActionConflict(
                'Representable coverage must use a typed shadow parent.')
        attempts = self._locked_coverage_attempts(session, parsed)
        existing = next((attempt for attempt in attempts
                         if attempt.request_sequence == sequence), None)
        if existing is not None:
            if (existing.logical_attempt != logical or
                    existing.request_role is not role or existing.phase
                    is not actions.CoverageAttemptPhase.PRE_SUBMIT):
                raise kernel_actions.ActionConflict(
                    'Coverage attempt key already has different bytes or '
                    'lifecycle state.')
            return CoverageAttemptTransition(existing, adopted=True)
        if sequence != len(attempts) + 1:
            raise kernel_actions.ActionConflict(
                'Coverage request_sequence must be contiguous.')
        if attempts:
            previous = attempts[-1]
            if (previous.phase is not actions.CoverageAttemptPhase.COMPLETE or
                    previous.retry_disposition is not actions.
                    CoverageAttemptRetryDisposition.RETRY_SAME_DECISION):
                raise kernel_actions.ActionConflict(
                    'Another coverage attempt requires an exact retry '
                    'decision from the preceding terminal attempt.')
            if (previous.request_role
                    is actions.CoverageAttemptRequestRole.LAUNCH_CLEANUP_DOWN
                    and role is not actions.CoverageAttemptRequestRole.
                    LAUNCH_CLEANUP_DOWN):
                raise kernel_actions.ActionConflict(
                    'Coverage-only cleanup evidence cannot prove safe '
                    'relaunch under the same decision.')
        primary_role = (actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH if
                        coverage.action_type is kernel_actions.ActionKind.LAUNCH
                        else actions.CoverageAttemptRequestRole.PRIMARY_DOWN)
        primary_attempts = [
            attempt for attempt in attempts if attempt.request_role in (
                actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH,
                actions.CoverageAttemptRequestRole.PRIMARY_DOWN)
        ]
        if role in (actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH,
                    actions.CoverageAttemptRequestRole.PRIMARY_DOWN):
            if role is not primary_role or logical != len(primary_attempts) + 1:
                raise kernel_actions.ActionConflict(
                    'Coverage primary role/logical attempt differs from its '
                    'decision identity.')
        elif (coverage.action_type is not kernel_actions.ActionKind.LAUNCH or
              not primary_attempts or
              logical != primary_attempts[-1].logical_attempt):
            raise kernel_actions.ActionConflict(
                'Coverage cleanup must follow the current launch primary.')
        now = sqlalchemy.func.clock_timestamp()
        table = state_schema.SHADOW_COVERAGE_ATTEMPTS
        session.execute(
            sqlalchemy.insert(table).values(
                decision_id=parsed,
                request_sequence=sequence,
                logical_attempt=logical,
                request_role=role.value,
                phase=actions.CoverageAttemptPhase.PRE_SUBMIT.value,
                legacy_request_id=None,
                terminal_request_status=None,
                retry_disposition=None,
                admitted_at=now,
                request_bound_at=None,
                completed_at=None,
                updated_at=now))
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.decision_id == parsed, table.c.request_sequence ==
                sequence).with_for_update()).mappings().one()
        return CoverageAttemptTransition(_coverage_attempt_record(row))

    @staticmethod
    def _expected_legacy_request_name(
            role: actions.CoverageAttemptRequestRole) -> str:
        if role is actions.CoverageAttemptRequestRole.PRIMARY_LAUNCH:
            return 'sky.launch'
        return 'sky.down'

    @staticmethod
    def _lock_and_validate_legacy_request(
        session: orm.Session,
        request_id: str,
        expected_name: str,
    ) -> None:
        """Serialize both shadow ledgers on the exact central request row."""
        request_row = session.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id ==
                request_id).with_for_update()).mappings().first()
        if request_row is None:
            raise kernel_actions.ActionConflict(
                f'Cannot bind missing API request {request_id!r}.')
        if (request_row['name'] != expected_name or
                request_row['resource_action_id'] is not None or
                request_row['resource_action_attempt'] is not None):
            raise kernel_actions.ActionConflict(
                'API request kind/correlation differs from shadow attempt.')

    @staticmethod
    def _shadow_request_owners(
        session: orm.Session,
        request_id: str,
    ) -> tuple[tuple[Any, Any] | None, tuple[Any, Any] | None]:
        """Read both ledgers while the caller holds the request-row lock."""
        represented_owner = session.execute(
            sqlalchemy.select(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence).where(
                    state_schema.SHADOW_ATTEMPTS.c.legacy_request_id ==
                    request_id)).first()
        coverage_owner = session.execute(
            sqlalchemy.select(
                state_schema.SHADOW_COVERAGE_ATTEMPTS.c.decision_id,
                state_schema.SHADOW_COVERAGE_ATTEMPTS.c.request_sequence).where(
                    state_schema.SHADOW_COVERAGE_ATTEMPTS.c.legacy_request_id ==
                    request_id)).first()
        return represented_owner, coverage_owner

    def _lock_coverage_and_attempt(
        self,
        session: orm.Session,
        decision_id: uuid.UUID,
        request_sequence: int,
    ) -> tuple[actions.CoverageDecisionV1, actions.CoverageAttemptV1]:
        coverage_row = self._locked_shadow_coverage(session, decision_id)
        if coverage_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown shadow coverage {decision_id}.')
        coverage = _shadow_coverage_record(coverage_row)
        attempt_row = session.execute(
            sqlalchemy.select(state_schema.SHADOW_COVERAGE_ATTEMPTS).where(
                state_schema.SHADOW_COVERAGE_ATTEMPTS.c.decision_id ==
                decision_id,
                state_schema.SHADOW_COVERAGE_ATTEMPTS.c.request_sequence ==
                request_sequence).with_for_update()).mappings().first()
        if attempt_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown coverage attempt {decision_id}/{request_sequence}.')
        return coverage, _coverage_attempt_record(attempt_row)

    def bind_coverage_attempt_request_in_session(
        self,
        session: orm.Session,
        decision_id: uuid.UUID,
        request_sequence: int,
        legacy_request_id: str,
    ) -> CoverageAttemptTransition:
        """Write-once bind after locking the exact central request row."""
        self._require_session(session)
        parsed = _canonical_uuid(decision_id, name='decision_id')
        sequence = _positive_integer(request_sequence, name='request_sequence')
        request_id = _bounded_text(legacy_request_id,
                                   name='legacy_request_id',
                                   maximum_bytes=128)
        attempt_row = session.execute(
            sqlalchemy.select(state_schema.SHADOW_COVERAGE_ATTEMPTS).where(
                state_schema.SHADOW_COVERAGE_ATTEMPTS.c.decision_id == parsed,
                state_schema.SHADOW_COVERAGE_ATTEMPTS.c.request_sequence ==
                sequence).with_for_update()).mappings().first()
        if attempt_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown coverage attempt {parsed}/{sequence}.')
        attempt = _coverage_attempt_record(attempt_row)
        expected_name = self._expected_legacy_request_name(attempt.request_role)
        self._lock_and_validate_legacy_request(session, request_id,
                                               expected_name)
        represented_owner, coverage_owner = self._shadow_request_owners(
            session, request_id)
        if represented_owner is not None or (
                coverage_owner is not None and
            (coverage_owner[0], coverage_owner[1]) != (parsed, sequence)):
            raise kernel_actions.ActionConflict(
                f'Request {request_id!r} belongs to another shadow attempt.')
        if attempt.legacy_request_id is not None:
            if attempt.legacy_request_id != request_id:
                raise kernel_actions.ActionConflict(
                    'Coverage attempt already has a different request ID.')
            return CoverageAttemptTransition(attempt, adopted=True)
        if attempt.phase is not actions.CoverageAttemptPhase.PRE_SUBMIT:
            raise kernel_actions.StaleRevision(
                'Only PRE_SUBMIT coverage may bind a request.')
        now = sqlalchemy.func.clock_timestamp()
        table = state_schema.SHADOW_COVERAGE_ATTEMPTS
        try:
            with session.begin_nested():
                session.execute(
                    sqlalchemy.update(table).where(
                        table.c.decision_id == parsed,
                        table.c.request_sequence == sequence,
                        table.c.phase == actions.CoverageAttemptPhase.
                        PRE_SUBMIT.value).values(phase=(
                            actions.CoverageAttemptPhase.REQUEST_BOUND.value),
                                                 legacy_request_id=request_id,
                                                 request_bound_at=now,
                                                 updated_at=now))
        except sqlalchemy.exc.IntegrityError as e:
            raise kernel_actions.ActionConflict(
                f'Request {request_id!r} raced with another coverage attempt.'
            ) from e
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.decision_id == parsed, table.c.request_sequence ==
                sequence).with_for_update()).mappings().one()
        return CoverageAttemptTransition(_coverage_attempt_record(row))

    def bind_coverage_attempt_request(
        self,
        decision_id: uuid.UUID,
        request_sequence: int,
        legacy_request_id: str,
    ) -> CoverageAttemptTransition:
        with orm.Session(self._database()) as session, session.begin():
            return self.bind_coverage_attempt_request_in_session(
                session, decision_id, request_sequence, legacy_request_id)

    def complete_coverage_attempt_in_session(
        self,
        session: orm.Session,
        decision_id: uuid.UUID,
        request_sequence: int,
        terminal_request_status: actions.CoverageAttemptTerminalStatus,
        retry_disposition: actions.CoverageAttemptRetryDisposition,
    ) -> CoverageAttemptTransition:
        """Snapshot one bound request's exact terminal status and decision."""
        self._require_session(session)
        parsed = _canonical_uuid(decision_id, name='decision_id')
        sequence = _positive_integer(request_sequence, name='request_sequence')
        terminal = (
            terminal_request_status if isinstance(
                terminal_request_status, actions.CoverageAttemptTerminalStatus)
            else actions.CoverageAttemptTerminalStatus(terminal_request_status))
        retry = (retry_disposition if isinstance(
            retry_disposition, actions.CoverageAttemptRetryDisposition) else
                 actions.CoverageAttemptRetryDisposition(retry_disposition))
        _, attempt = self._lock_coverage_and_attempt(session, parsed, sequence)
        if attempt.phase is actions.CoverageAttemptPhase.COMPLETE:
            if (attempt.terminal_request_status is not terminal or
                    attempt.retry_disposition is not retry):
                raise kernel_actions.ActionConflict(
                    'Completed coverage attempt has different terminal '
                    'evidence.')
            return CoverageAttemptTransition(attempt, adopted=True)
        if (attempt.phase is not actions.CoverageAttemptPhase.REQUEST_BOUND or
                attempt.legacy_request_id is None):
            raise kernel_actions.StaleRevision(
                'Only REQUEST_BOUND coverage may complete.')
        request_row = session.execute(
            sqlalchemy.select(request_postgres.REQUESTS).where(
                request_postgres.REQUESTS.c.request_id == attempt.
                legacy_request_id).with_for_update()).mappings().first()
        expected_name = self._expected_legacy_request_name(attempt.request_role)
        if (request_row is None or request_row['name'] != expected_name or
                request_row['status'] != terminal.value or
                request_row['finished_at'] is None or
                request_row['resource_action_id'] is not None or
                request_row['resource_action_attempt'] is not None):
            raise kernel_actions.ActionConflict(
                'Bound API request lacks the exact terminal coverage shape.')
        now = sqlalchemy.func.clock_timestamp()
        table = state_schema.SHADOW_COVERAGE_ATTEMPTS
        updated = session.execute(
            sqlalchemy.update(table).where(
                table.c.decision_id == parsed,
                table.c.request_sequence == sequence, table.c.phase ==
                actions.CoverageAttemptPhase.REQUEST_BOUND.value,
                table.c.legacy_request_id == attempt.legacy_request_id).values(
                    phase=actions.CoverageAttemptPhase.COMPLETE.value,
                    terminal_request_status=terminal.value,
                    retry_disposition=retry.value,
                    completed_at=now,
                    updated_at=now))
        if updated.rowcount != 1:
            raise kernel_actions.StaleRevision(
                'Coverage attempt changed during terminalization.')
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.decision_id == parsed, table.c.request_sequence ==
                sequence).with_for_update()).mappings().one()
        return CoverageAttemptTransition(_coverage_attempt_record(row))

    def complete_coverage_attempt(
        self,
        decision_id: uuid.UUID,
        request_sequence: int,
        terminal_request_status: actions.CoverageAttemptTerminalStatus,
        retry_disposition: actions.CoverageAttemptRetryDisposition,
    ) -> CoverageAttemptTransition:
        with orm.Session(self._database()) as session, session.begin():
            return self.complete_coverage_attempt_in_session(
                session, decision_id, request_sequence, terminal_request_status,
                retry_disposition)

    def mark_coverage_request_association_unknown_in_session(
        self,
        session: orm.Session,
        decision_id: uuid.UUID,
        request_sequence: int,
    ) -> CoverageAttemptTransition:
        """Close the call/request-ID gap conservatively and permanently."""
        self._require_session(session)
        parsed = _canonical_uuid(decision_id, name='decision_id')
        sequence = _positive_integer(request_sequence, name='request_sequence')
        _, attempt = self._lock_coverage_and_attempt(session, parsed, sequence)
        if attempt.phase is actions.CoverageAttemptPhase.REQUEST_ASSOCIATION_UNKNOWN:
            return CoverageAttemptTransition(attempt, adopted=True)
        if attempt.phase is not actions.CoverageAttemptPhase.PRE_SUBMIT:
            raise kernel_actions.StaleRevision(
                'Request association can become unknown only from PRE_SUBMIT.')
        now = sqlalchemy.func.clock_timestamp()
        table = state_schema.SHADOW_COVERAGE_ATTEMPTS
        session.execute(
            sqlalchemy.update(table).where(
                table.c.decision_id == parsed,
                table.c.request_sequence == sequence,
                table.c.phase == actions.CoverageAttemptPhase.PRE_SUBMIT.value).
            values(phase=(
                actions.CoverageAttemptPhase.REQUEST_ASSOCIATION_UNKNOWN.value),
                   completed_at=now,
                   updated_at=now))
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.decision_id == parsed, table.c.request_sequence ==
                sequence).with_for_update()).mappings().one()
        return CoverageAttemptTransition(_coverage_attempt_record(row))

    def mark_coverage_request_association_unknown(
        self,
        decision_id: uuid.UUID,
        request_sequence: int,
    ) -> CoverageAttemptTransition:
        with orm.Session(self._database()) as session, session.begin():
            return self.mark_coverage_request_association_unknown_in_session(
                session, decision_id, request_sequence)

    def _shadow_reference_is_releasable(self, session: orm.Session,
                                        decision_id: uuid.UUID) -> bool:
        coverage_row = self._locked_shadow_coverage(session, decision_id)
        if coverage_row is None:
            return False
        coverage = _shadow_coverage_record(coverage_row)
        if (coverage.normalization_outcome
                is actions.NormalizationOutcome.NOT_REPRESENTABLE):
            coverage_attempts = self._locked_coverage_attempts(
                session, decision_id)
            if any(attempt.phase is not actions.CoverageAttemptPhase.COMPLETE
                   for attempt in coverage_attempts):
                return False
            return (bool(coverage_attempts) and
                    coverage_attempts[-1].retry_disposition is not actions.
                    CoverageAttemptRetryDisposition.RETRY_SAME_DECISION)
        sample_row = self._locked_sample(session, decision_id)
        if sample_row is None:
            return False
        sample = _sample_record(sample_row)
        if sample.phase not in (actions.ShadowParentPhase.COMPLETE,
                                actions.ShadowParentPhase.ABANDONED_PRE_SUBMIT):
            return False
        represented_attempts = self._locked_attempts(session, sample)
        if any(attempt.phase not in (
                actions.ShadowAttemptPhase.COMPLETE,
                actions.ShadowAttemptPhase.ABANDONED_PRE_SUBMIT)
               for attempt in represented_attempts):
            return False
        return not _validate_child_graph(
            sample, represented_attempts, require_closed=True)

    def release_worker_cohort_reference_in_session(
        self,
        session: orm.Session,
        reference: actions.WorkerCohortReferenceInputV1,
        expected_revision: int,
    ) -> WorkerCohortReferenceTransition:
        """Release only fully reduced SHADOW_ACTIVE evidence.

        PREPARING owner recovery and ACTION_ACTIVE reduction need evidence from
        orchestration/API stores not yet represented by a reviewed typed input;
        this method deliberately keeps both states closed.
        """
        self._require_session(session)
        if not isinstance(reference, actions.WorkerCohortReferenceInputV1):
            raise TypeError('reference has an invalid type.')
        expected_revision = _positive_integer(expected_revision,
                                              name='expected_revision')
        cohort_row = self._locked_worker_cohort(session, reference.cohort_id)
        if cohort_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown worker cohort {reference.cohort_id!r}.')
        _worker_cohort_record(cohort_row)
        row = self._locked_worker_cohort_reference(session,
                                                   reference.decision_id)
        if row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown cohort reference {reference.decision_id}.')
        current = _worker_cohort_reference_record(row)
        if current.reference != reference:
            raise kernel_actions.ClaimLost(
                'Reference owner/lifecycle or decision identity changed.')
        if (current.reference_state
                is actions.WorkerCohortReferenceState.RELEASED and
                current.revision == expected_revision + 1):
            return WorkerCohortReferenceTransition(current, adopted=True)
        if (current.reference_state
                is not actions.WorkerCohortReferenceState.SHADOW_ACTIVE or
                current.revision != expected_revision):
            raise kernel_actions.StaleRevision(
                'Only the expected SHADOW_ACTIVE revision can be released by '
                'the Serve033 store.')
        if not self._shadow_reference_is_releasable(session,
                                                    reference.decision_id):
            raise kernel_actions.ActionConflict(
                'Shadow evidence is absent, nonterminal, ambiguous, or still '
                'requires retry.')
        updated = session.execute(
            sqlalchemy.update(state_schema.WORKER_COHORT_REFS).where(
                state_schema.WORKER_COHORT_REFS.c.decision_id ==
                reference.decision_id,
                state_schema.WORKER_COHORT_REFS.c.reference_state ==
                actions.WorkerCohortReferenceState.SHADOW_ACTIVE.value,
                state_schema.WORKER_COHORT_REFS.c.revision ==
                expected_revision).values(
                    reference_state=(
                        actions.WorkerCohortReferenceState.RELEASED.value),
                    revision=expected_revision + 1,
                    released_at=sqlalchemy.func.clock_timestamp()))
        if updated.rowcount != 1:
            raise kernel_actions.StaleRevision(
                'Cohort reference changed during release.')
        updated_row = self._locked_worker_cohort_reference(
            session, reference.decision_id)
        assert updated_row is not None
        return WorkerCohortReferenceTransition(
            _worker_cohort_reference_record(updated_row))

    def release_worker_cohort_reference(
        self,
        reference: actions.WorkerCohortReferenceInputV1,
        expected_revision: int,
    ) -> WorkerCohortReferenceTransition:
        with orm.Session(self._database()) as session, session.begin():
            return self.release_worker_cohort_reference_in_session(
                session, reference, expected_revision)

    @staticmethod
    def _locked_sample(session: orm.Session,
                       action_id: uuid.UUID) -> Mapping[str, Any] | None:
        return session.execute(
            sqlalchemy.select(state_schema.SHADOW_SAMPLES).where(
                state_schema.SHADOW_SAMPLES.c.would_be_action_id ==
                action_id).with_for_update()).mappings().first()

    @staticmethod
    def _locked_attempts(
            session: orm.Session,
            sample: ShadowSampleRecord) -> list[ShadowAttemptRecord]:
        rows = session.execute(
            sqlalchemy.select(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id ==
                sample.action_id).order_by(
                    state_schema.SHADOW_ATTEMPTS.c.request_sequence).
            with_for_update()).mappings().all()
        return [_attempt_record(row, sample) for row in rows]

    @staticmethod
    def _locked_shadow_service(
        session: orm.Session,
        sample: NewShadowSample,
        expected_controller_owner: tuple[int | None, str | None],
        expected_lifecycle_epoch: int,
        *,
        require_launch_allowed: bool = False,
    ) -> Mapping[str, Any]:
        if (not isinstance(expected_controller_owner, tuple) or
                len(expected_controller_owner) != 2):
            raise TypeError('expected_controller_owner must be (pid, ip).')
        lifecycle_epoch = _positive_integer(expected_lifecycle_epoch,
                                            name='expected_lifecycle_epoch')
        row = session.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                sample.service_name).with_for_update()).mappings().first()
        if row is None:
            raise kernel_actions.ClaimLost(
                f'Service {sample.service_name!r} no longer exists.')
        if (row['hash'] != sample.provider_plan.resource_identity.service_hash
                or (row['controller_pid'],
                    row['controller_ip']) != expected_controller_owner or
                row['lifecycle_epoch'] != lifecycle_epoch or
                row['resource_action_mode']
                != actions.ResourceActionMode.SHADOW.value or
                row['resource_action_mode_changed_at'] is None):
            raise kernel_actions.ClaimLost(
                'Service hash/owner/lifecycle/shadow fence no longer matches.')
        if require_launch_allowed:
            try:
                service_status = ServiceStatus(row['status'])
            except (TypeError, ValueError) as e:
                raise kernel_actions.InvariantViolation(
                    'Shadow launch admission found an invalid service status.'
                ) from e
            if service_status in ServiceStatus.replica_launch_blocking_statuses(
            ):
                raise kernel_actions.ClaimLost(
                    'Service status now blocks replica launch admission.')
        return row

    @staticmethod
    def _validate_prepared_reference_service_fence(
        reference: actions.WorkerCohortReferenceInputV1,
        service_row: Mapping[str, Any],
    ) -> None:
        controller_pid = service_row['controller_pid']
        controller_ip = service_row['controller_ip']
        if controller_pid is None or controller_ip is None:
            raise kernel_actions.ClaimLost(
                'Prepared work requires a nonnull locked controller owner.')
        expected_owner_fence = f'{controller_pid}:{controller_ip}'
        if reference.controller_owner_fence != expected_owner_fence:
            raise kernel_actions.ClaimLost(
                'Preparation reference controller-owner fence no longer '
                'matches.')
        if reference.lifecycle_epoch != service_row['lifecycle_epoch']:
            raise kernel_actions.ClaimLost(
                'Preparation reference lifecycle fence no longer matches.')

    def _admit_after_service_lock_in_session(
        self,
        session: orm.Session,
        new_sample: NewShadowSample,
        prepared_reference: actions.WorkerCohortReferenceInputV1 | None = None,
    ) -> ShadowSampleRecord:
        """Insert/adopt a represented decision after service/replica locks.

        A prepared reference binds under the canonical cohort -> reference ->
        coverage -> parent order.  ``None`` intentionally retains the
        incomplete-foundation path used by legacy tests; promotion treats that
        unlinked coverage as a blocker.
        """
        if prepared_reference is not None and not isinstance(
                prepared_reference, actions.WorkerCohortReferenceInputV1):
            raise TypeError('prepared_reference has an invalid type.')
        plan = new_sample.provider_plan
        identity = plan.resource_identity
        coverage_identity = actions.CoverageDecisionIdentityV1(
            version=1,
            service_hash=identity.service_hash,
            service_incarnation=identity.service_incarnation,
            replica_id=identity.replica_id,
            replica_incarnation=identity.replica_incarnation,
            desired_generation=identity.desired_generation,
            action_type=plan.action_kind)
        if coverage_identity.decision_id != new_sample.action_id:
            raise kernel_actions.InvariantViolation(
                'Represented sample and coverage identities differ.')
        reference_id = None
        if prepared_reference is not None:
            if (prepared_reference.decision_id != new_sample.action_id or
                    prepared_reference.service_hash != identity.service_hash or
                    prepared_reference.replica_incarnation
                    != identity.replica_incarnation or
                    prepared_reference.desired_generation
                    != identity.desired_generation or
                    prepared_reference.action_type is not plan.action_kind):
                raise ValueError('Prepared worker cohort reference does not '
                                 'match the represented sample.')
            binding = self.bind_worker_cohort_reference_in_session(
                session, prepared_reference, 1,
                actions.WorkerCohortReferenceState.SHADOW_ACTIVE)
            reference_id = prepared_reference.decision_id
            existing_coverage = self._locked_shadow_coverage(
                session, new_sample.action_id)
            existing_coverage_attempts = session.execute(
                sqlalchemy.select(state_schema.SHADOW_COVERAGE_ATTEMPTS).where(
                    state_schema.SHADOW_COVERAGE_ATTEMPTS.c.decision_id ==
                    new_sample.action_id).order_by(
                        state_schema.SHADOW_COVERAGE_ATTEMPTS.c.request_sequence
                    ).with_for_update()).mappings().all()
            existing_parent = session.execute(
                sqlalchemy.select(state_schema.SHADOW_SAMPLES).where(
                    state_schema.SHADOW_SAMPLES.c.would_be_action_id ==
                    new_sample.action_id).with_for_update()).mappings().first()
            existing_children = session.execute(
                sqlalchemy.select(state_schema.SHADOW_ATTEMPTS).where(
                    state_schema.SHADOW_ATTEMPTS.c.would_be_action_id ==
                    new_sample.action_id).order_by(
                        state_schema.SHADOW_ATTEMPTS.c.request_sequence).
                with_for_update()).mappings().all()
            graph_exists = (existing_coverage is not None, existing_parent
                            is not None)
            if binding.adopted and graph_exists != (True, True):
                raise kernel_actions.ActionConflict(
                    'Active preparation reference lacks a complete represented '
                    'shadow graph for exact replay.')
            if binding.adopted:
                assert existing_parent is not None
                replay_parent = _sample_record(existing_parent)
                if (replay_parent.phase is not actions.ShadowParentPhase.PENDING
                        or replay_parent.revision != 1 or
                        existing_coverage_attempts or existing_children):
                    raise kernel_actions.ActionConflict(
                        'Represented shadow graph has advanced beyond the exact '
                        'pre-submit replay boundary.')
            elif (graph_exists != (False, False) or
                  existing_coverage_attempts or existing_children):
                raise kernel_actions.ActionConflict(
                    'Preparing reference has preexisting represented shadow '
                    'state; admission cannot repair a partial graph.')
        database_now = session.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        coverage = self._insert_or_adopt_shadow_coverage_in_session(
            session,
            NewShadowCoverage(service_name=new_sample.service_name,
                              identity=coverage_identity,
                              normalization_outcome=actions.
                              NormalizationOutcome.REPRESENTABLE,
                              not_representable_reason=None,
                              worker_cohort_ref_id=reference_id), database_now)
        now = _canonical_timestamp_datetime(coverage.record.admitted_at,
                                            name='coverage.admitted_at')
        values = {
            'would_be_action_id': new_sample.action_id,
            'service_name': new_sample.service_name,
            'service_hash': identity.service_hash,
            'service_incarnation': identity.service_incarnation,
            'replica_id': identity.replica_id,
            'replica_incarnation': identity.replica_incarnation,
            'desired_generation': identity.desired_generation,
            'action_type': plan.action_kind.value,
            'resource_identity': new_sample.resource_identity,
            'immutable_spec': new_sample.immutable_spec.canonical_value(),
            'immutable_spec_sha256': new_sample.immutable_spec_sha256,
            'provider_plan': plan.canonical_value(),
            'provider_plan_sha256': new_sample.provider_plan_sha256,
            'profile_eligibility': new_sample.profile_eligibility.value,
            'phase': actions.ShadowParentPhase.PENDING.value,
            'legacy_projection': None,
            'legacy_projection_sha256': None,
            'proposed_projection': None,
            'proposed_projection_sha256': None,
            'parity_class': actions.ShadowParityClass.PENDING.value,
            'revision': 1,
            'created_at': now,
            'updated_at': now,
            'completed_at': None,
        }
        session.execute(
            postgresql.insert(state_schema.SHADOW_SAMPLES).values(
                **values).on_conflict_do_nothing())
        table = state_schema.SHADOW_SAMPLES
        rows = session.execute(
            sqlalchemy.select(table).where(
                sqlalchemy.or_(
                    table.c.would_be_action_id == new_sample.action_id,
                    sqlalchemy.and_(
                        table.c.service_hash == identity.service_hash,
                        table.c.service_incarnation ==
                        identity.service_incarnation,
                        table.c.replica_id == identity.replica_id,
                        table.c.replica_incarnation ==
                        identity.replica_incarnation, table.c.desired_generation
                        == identity.desired_generation,
                        table.c.action_type == plan.action_kind.value))).
            order_by(
                table.c.would_be_action_id).with_for_update()).mappings().all()
        if len(rows) != 1:
            if not rows:
                raise kernel_actions.InvariantViolation(
                    'Sample insert conflicted without a durable row.')
            raise kernel_actions.ActionConflict(
                'Sample UUID and natural identity resolve to different rows.')
        record = _sample_record(rows[0])
        if record.created_at != now:
            raise kernel_actions.InvariantViolation(
                'Represented coverage and parent admission timestamps differ.')
        if (record.action_id != new_sample.action_id or
                record.service_name != new_sample.service_name or
                record.immutable_spec.canonical_bytes
                != new_sample.immutable_spec.canonical_bytes or
                record.immutable_spec_sha256 != new_sample.immutable_spec_sha256
                or record.provider_plan != new_sample.provider_plan or
                record.provider_plan_sha256 != new_sample.provider_plan_sha256
                or record.profile_eligibility
                is not new_sample.profile_eligibility):
            raise kernel_actions.ActionConflict(
                f'Shadow sample {new_sample.action_id} already exists with '
                'different immutable bytes.')
        return record

    def admit_launch_replica_in_session(
        self,
        session: orm.Session,
        new_sample: NewShadowSample,
        replica_values: Mapping[str, Any],
        expected_controller_owner: tuple[int | None, str | None],
        expected_lifecycle_epoch: int,
        *,
        prepared_reference: actions.WorkerCohortReferenceInputV1 | None = None,
    ) -> ShadowSampleRecord:
        """Commit a new launch replica intent, parent, and link atomically.

        This initial-launch primitive owns the service -> replica -> shadow
        lock order.  Existing rows are accepted only as exact lost-ack replay;
        in particular, a name-only legacy row is never assigned a fabricated
        action identity.
        """
        self._require_session(session)
        if not isinstance(new_sample, NewShadowSample):
            raise TypeError('new_sample has an invalid type.')
        if prepared_reference is not None and not isinstance(
                prepared_reference, actions.WorkerCohortReferenceInputV1):
            raise TypeError('prepared_reference has an invalid type.')
        if prepared_reference is not None:
            raise kernel_actions.ActionConflict(
                'Linked represented admission requires the immutable '
                'invocation execution_config.capsule.executor_cohort; the '
                'current flattened spec cannot commit that cohort.')
        plan = new_sample.provider_plan
        identity = plan.resource_identity
        if (plan.action_kind is not kernel_actions.ActionKind.LAUNCH or
                identity.desired_generation != 1):
            raise ValueError(
                'Initial replica admission requires generation-one launch.')
        values = dict(replica_values)
        action_columns = {
            'replica_incarnation', 'desired_generation',
            'sky_cluster_record_uuid', 'launch_action_id', 'down_action_id',
            'launch_shadow_coverage_id', 'down_shadow_coverage_id',
            'launch_shadow_sample_id', 'down_shadow_sample_id'
        }
        if action_columns.intersection(values):
            raise ValueError(
                'Legacy replica values must omit action-owned columns.')
        if (values.get('service_name') != new_sample.service_name or
                values.get('replica_id') != identity.replica_id or
                values.get('cluster_name')
                != plan.requested_target.sky_cluster_name):
            raise ValueError(
                'Replica intent does not match the immutable launch sample.')

        service_row = self._locked_shadow_service(session,
                                                  new_sample,
                                                  expected_controller_owner,
                                                  expected_lifecycle_epoch,
                                                  require_launch_allowed=True)
        if bool(service_row['pool']):
            raise kernel_actions.ClaimLost(
                'Resource-action launch admission excludes pool services.')
        if prepared_reference is not None:
            self._validate_prepared_reference_service_fence(
                prepared_reference, service_row)
        table = serve_state_schema.replicas_table
        row = session.execute(
            sqlalchemy.select(table).where(
                table.c.service_name == new_sample.service_name,
                table.c.replica_id ==
                identity.replica_id).with_for_update()).mappings().first()
        action_values = {
            'replica_incarnation': identity.replica_incarnation,
            'desired_generation': identity.desired_generation,
            'sky_cluster_record_uuid':
                plan.requested_target.sky_cluster_record_uuid,
            'launch_action_id': None,
            'down_action_id': None,
            'launch_shadow_coverage_id': new_sample.action_id,
            'down_shadow_coverage_id': None,
            'launch_shadow_sample_id': new_sample.action_id,
            'down_shadow_sample_id': None,
        }
        if row is None:
            session.execute(
                postgresql.insert(table).values(**values, **action_values))
        else:
            if (row['cluster_name'] != values['cluster_name'] or
                    any(row[name] != value
                        for name, value in action_values.items())):
                raise kernel_actions.ActionConflict(
                    'Replica row already has a different or name-only action '
                    'identity.')
        return self._admit_after_service_lock_in_session(
            session, new_sample, prepared_reference)

    def admit_in_session(
        self,
        session: orm.Session,
        new_sample: NewShadowSample,
        expected_controller_owner: tuple[int | None, str | None],
        expected_lifecycle_epoch: int,
        *,
        prepared_reference: actions.WorkerCohortReferenceInputV1 | None = None,
    ) -> ShadowSampleRecord:
        """Insert or exactly adopt a sample in the caller's transaction."""
        self._require_session(session)
        if not isinstance(new_sample, NewShadowSample):
            raise TypeError('new_sample has an invalid type.')
        if prepared_reference is not None and not isinstance(
                prepared_reference, actions.WorkerCohortReferenceInputV1):
            raise TypeError('prepared_reference has an invalid type.')
        if prepared_reference is not None:
            raise kernel_actions.ActionConflict(
                'Linked represented admission requires the immutable '
                'invocation execution_config.capsule.executor_cohort; the '
                'current flattened spec cannot commit that cohort.')
        service_row = self._locked_shadow_service(session, new_sample,
                                                  expected_controller_owner,
                                                  expected_lifecycle_epoch)
        if prepared_reference is not None:
            self._validate_prepared_reference_service_fence(
                prepared_reference, service_row)
        return self._admit_after_service_lock_in_session(
            session, new_sample, prepared_reference)

    def admit(
        self,
        new_sample: NewShadowSample,
        expected_controller_owner: tuple[int | None, str | None],
        expected_lifecycle_epoch: int,
        *,
        prepared_reference: actions.WorkerCohortReferenceInputV1 | None = None,
    ) -> ShadowSampleRecord:
        with orm.Session(self._database()) as session, session.begin():
            return self.admit_in_session(session,
                                         new_sample,
                                         expected_controller_owner,
                                         expected_lifecycle_epoch,
                                         prepared_reference=prepared_reference)

    def get_sample(self, action_id: uuid.UUID) -> ShadowSampleRecord | None:
        parsed = _canonical_uuid(action_id, name='action_id')
        with orm.Session(self._database()) as session:
            row = session.execute(
                sqlalchemy.select(state_schema.SHADOW_SAMPLES).where(
                    state_schema.SHADOW_SAMPLES.c.would_be_action_id ==
                    parsed)).mappings().first()
        return None if row is None else _sample_record(row)

    def list_attempts(self, action_id: uuid.UUID) -> list[ShadowAttemptRecord]:
        parsed = _canonical_uuid(action_id, name='action_id')
        with orm.Session(self._database()) as session:
            sample_row = session.execute(
                sqlalchemy.select(state_schema.SHADOW_SAMPLES).where(
                    state_schema.SHADOW_SAMPLES.c.would_be_action_id ==
                    parsed)).mappings().first()
            if sample_row is None:
                raise kernel_actions.InvariantViolation(
                    f'Unknown shadow sample {parsed}.')
            sample = _sample_record(sample_row)
            rows = session.execute(
                sqlalchemy.select(state_schema.SHADOW_ATTEMPTS).where(
                    state_schema.SHADOW_ATTEMPTS.c.would_be_action_id ==
                    parsed).order_by(state_schema.SHADOW_ATTEMPTS.c.
                                     request_sequence)).mappings().all()
        return [_attempt_record(row, sample) for row in rows]

    def prepare_attempt_in_session(
        self,
        session: orm.Session,
        action_id: uuid.UUID,
        expected_revision: int,
        request_sequence: int,
        logical_attempt: int,
        request_role: actions.ShadowRequestRole,
        planned_execution_kind: actions.PlannedExecutionKind,
        invocation: actions.ProviderLifecycleInvocationV1,
    ) -> PreparedShadowAttempt:
        """Commit the next PRE_SUBMIT child before entering an SDK call."""
        self._require_session(session)
        parsed = _canonical_uuid(action_id, name='action_id')
        expected_revision = _positive_integer(expected_revision,
                                              name='expected_revision')
        sequence = _positive_integer(request_sequence, name='request_sequence')
        logical = _positive_integer(logical_attempt, name='logical_attempt')
        role = (request_role
                if isinstance(request_role, actions.ShadowRequestRole) else
                actions.ShadowRequestRole(request_role))
        execution = (planned_execution_kind if isinstance(
            planned_execution_kind, actions.PlannedExecutionKind) else
                     actions.PlannedExecutionKind(planned_execution_kind))
        if not isinstance(invocation, actions.ProviderLifecycleInvocationV1):
            raise TypeError('invocation has an invalid type.')
        parent_row = self._locked_sample(session, parsed)
        if parent_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown shadow sample {parsed}.')
        sample = _sample_record(parent_row)
        _validate_invocation(sample, role, invocation)
        attempts = self._locked_attempts(session, sample)
        graph_problems = _validate_child_graph(sample, attempts)
        if graph_problems:
            raise kernel_actions.InvariantViolation(
                f'Shadow attempt graph is invalid: {graph_problems[0]}.')
        existing = next((attempt for attempt in attempts
                         if attempt.request_sequence == sequence), None)
        if existing is not None:
            if (sample.revision != expected_revision + 1 or
                    existing.logical_attempt != logical or
                    existing.request_role is not role or
                    existing.planned_execution_kind is not execution or
                    existing.invocation != invocation):
                raise kernel_actions.StaleRevision(
                    'Prepared shadow attempt replay does not match the '
                    'committed sequence/revision.')
            return PreparedShadowAttempt(sample, existing, adopted=True)
        if any(attempt.phase is not actions.ShadowAttemptPhase.COMPLETE
               for attempt in attempts):
            raise kernel_actions.ActionConflict(
                'A new shadow child requires every earlier child to be '
                'complete.')
        if (sample.revision != expected_revision or
                sample.phase not in (actions.ShadowParentPhase.PENDING,
                                     actions.ShadowParentPhase.RUNNING)):
            raise kernel_actions.StaleRevision(
                f'Shadow sample {parsed} is not the expected open revision.')
        if sequence != len(attempts) + 1:
            raise kernel_actions.ActionConflict(
                'Shadow request_sequence must be contiguous.')
        primary_attempts = sorted({
            attempt.logical_attempt
            for attempt in attempts
            if attempt.request_role in _PRIMARY_ROLES
        })
        if role in _PRIMARY_ROLES:
            expected_logical = len(primary_attempts) + 1
            if logical != expected_logical:
                raise kernel_actions.ActionConflict(
                    'Primary logical_attempt must be contiguous.')
            if primary_attempts:
                previous_logical = primary_attempts[-1]
                previous_primary = next(
                    attempt for attempt in attempts
                    if attempt.logical_attempt == previous_logical and
                    attempt.request_role in _PRIMARY_ROLES)
                retry = previous_primary.retry_decision
                if (retry is None or retry.decision
                        is not actions.ShadowRetryDecision.RETRY_SAME_PLAN):
                    raise kernel_actions.ActionConflict(
                        'Another primary requires retry_same_plan from the '
                        'preceding primary.')
                cleanups = [
                    attempt for attempt in attempts
                    if attempt.logical_attempt == previous_logical and
                    attempt.request_role is
                    actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN
                ]
                if cleanups:
                    latest_cleanup = cleanups[-1]
                    cleanup_outcome = latest_cleanup.actual_outcome
                    cleanup_retry = latest_cleanup.retry_decision
                    if (cleanup_outcome is None or cleanup_retry is None or
                            cleanup_outcome.disposition
                            is not actions.ServeActionDisposition.SUCCEEDED or
                            cleanup_retry.decision
                            is not actions.ShadowRetryDecision.TERMINAL):
                        raise kernel_actions.ActionConflict(
                            'Another primary requires the cleanup chain to '
                            'terminalize successfully.')
                else:
                    outcome = previous_primary.actual_outcome
                    observation = (None
                                   if outcome is None else outcome.observation)
                    if (observation is None or observation.state
                            is not actions.ProviderObservationState.ABSENT or
                            observation.certainty is not actions.
                            ProviderObservationCertainty.AUTHORITATIVE):
                        raise kernel_actions.ActionConflict(
                            'Another primary without cleanup requires exact '
                            'authoritative absence proof.')
        elif (logical not in primary_attempts or
              logical != primary_attempts[-1]):
            raise kernel_actions.ActionConflict(
                'Cleanup must follow the current logical attempt primary.')
        else:
            grouped = [
                attempt for attempt in attempts
                if attempt.logical_attempt == logical
            ]
            primary = next(attempt for attempt in grouped
                           if attempt.request_role in _PRIMARY_ROLES)
            if (primary.actual_outcome is None or
                    primary.actual_outcome.disposition
                    is actions.ServeActionDisposition.SUCCEEDED):
                raise kernel_actions.ActionConflict(
                    'Cleanup requires a completed failed launch primary.')
            cleanups = [
                attempt for attempt in grouped if attempt.request_role is
                actions.ShadowRequestRole.LAUNCH_CLEANUP_DOWN
            ]
            if cleanups:
                retry = cleanups[-1].retry_decision
                if (retry is None or retry.decision
                        is not actions.ShadowRetryDecision.RETRY_SAME_PLAN):
                    raise kernel_actions.ActionConflict(
                        'Another cleanup requires retry_same_plan from the '
                        'preceding cleanup.')
        now = sqlalchemy.func.clock_timestamp()
        session.execute(
            sqlalchemy.insert(state_schema.SHADOW_ATTEMPTS).values(
                would_be_action_id=parsed,
                request_sequence=sequence,
                logical_attempt=logical,
                request_role=role.value,
                planned_execution_kind=execution.value,
                phase=actions.ShadowAttemptPhase.PRE_SUBMIT.value,
                legacy_request_id=None,
                invocation=invocation.canonical_value(),
                invocation_sha256=invocation.sha256,
                provider_operation_id=None,
                admitted_at=now,
                updated_at=now))
        updated = session.execute(
            sqlalchemy.update(state_schema.SHADOW_SAMPLES).where(
                state_schema.SHADOW_SAMPLES.c.would_be_action_id == parsed,
                state_schema.SHADOW_SAMPLES.c.revision ==
                expected_revision).values(
                    phase=actions.ShadowParentPhase.RUNNING.value,
                    revision=expected_revision + 1,
                    updated_at=now))
        if updated.rowcount != 1:
            raise kernel_actions.StaleRevision(
                f'Shadow sample {parsed} changed during attempt preparation.')
        parent_row = self._locked_sample(session, parsed)
        assert parent_row is not None
        sample = _sample_record(parent_row)
        attempt_row = session.execute(
            sqlalchemy.select(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == parsed,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence ==
                sequence).with_for_update()).mappings().one()
        return PreparedShadowAttempt(sample,
                                     _attempt_record(attempt_row, sample))

    def prepare_attempt(
        self,
        action_id: uuid.UUID,
        expected_revision: int,
        request_sequence: int,
        logical_attempt: int,
        request_role: actions.ShadowRequestRole,
        planned_execution_kind: actions.PlannedExecutionKind,
        invocation: actions.ProviderLifecycleInvocationV1,
    ) -> PreparedShadowAttempt:
        with orm.Session(self._database()) as session, session.begin():
            return self.prepare_attempt_in_session(
                session, action_id, expected_revision, request_sequence,
                logical_attempt, request_role, planned_execution_kind,
                invocation)

    def _lock_parent_and_attempt(
        self,
        session: orm.Session,
        action_id: uuid.UUID,
        request_sequence: int,
    ) -> tuple[ShadowSampleRecord, ShadowAttemptRecord]:
        parent_row = self._locked_sample(session, action_id)
        if parent_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown shadow sample {action_id}.')
        sample = _sample_record(parent_row)
        attempt_row = session.execute(
            sqlalchemy.select(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == action_id,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence ==
                request_sequence).with_for_update()).mappings().first()
        if attempt_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown shadow attempt {action_id}/{request_sequence}.')
        return sample, _attempt_record(attempt_row, sample)

    def bind_request_in_session(self, session: orm.Session,
                                action_id: uuid.UUID, request_sequence: int,
                                legacy_request_id: str) -> ShadowAttemptRecord:
        """Write-once bind the real request ID returned by SDK admission."""
        self._require_session(session)
        parsed = _canonical_uuid(action_id, name='action_id')
        sequence = _positive_integer(request_sequence, name='request_sequence')
        request_id = _bounded_text(legacy_request_id,
                                   name='legacy_request_id',
                                   maximum_bytes=128)
        sample, attempt = self._lock_parent_and_attempt(session, parsed,
                                                        sequence)
        self._lock_and_validate_legacy_request(
            session, request_id,
            self._expected_legacy_request_name(attempt.request_role))
        represented_owner, coverage_owner = self._shadow_request_owners(
            session, request_id)
        if ((represented_owner is not None and
             (represented_owner[0], represented_owner[1]) != (parsed, sequence))
                or coverage_owner is not None):
            raise kernel_actions.ActionConflict(
                f'Request {request_id!r} belongs to another shadow attempt.')
        if attempt.legacy_request_id is not None:
            if attempt.legacy_request_id != request_id:
                raise kernel_actions.ActionConflict(
                    'Shadow attempt already has a different request ID.')
            return attempt
        if (attempt.phase is not actions.ShadowAttemptPhase.PRE_SUBMIT or
                attempt.planned_execution_kind
                is not actions.PlannedExecutionKind.API_REQUEST):
            raise kernel_actions.StaleRevision(
                'Only a PRE_SUBMIT API-request attempt may bind a request.')
        now = sqlalchemy.func.clock_timestamp()
        try:
            with session.begin_nested():
                session.execute(
                    sqlalchemy.update(state_schema.SHADOW_ATTEMPTS).where(
                        state_schema.SHADOW_ATTEMPTS.c.would_be_action_id ==
                        parsed, state_schema.SHADOW_ATTEMPTS.c.request_sequence
                        == sequence).values(phase=actions.ShadowAttemptPhase.
                                            REQUEST_BOUND.value,
                                            legacy_request_id=request_id,
                                            request_bound_at=now,
                                            updated_at=now))
        except sqlalchemy.exc.IntegrityError as e:
            raise kernel_actions.ActionConflict(
                f'Request {request_id} raced with another shadow attempt.'
            ) from e
        row = session.execute(
            sqlalchemy.select(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == parsed,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence ==
                sequence).with_for_update()).mappings().one()
        return _attempt_record(row, sample)

    def bind_request(self, action_id: uuid.UUID, request_sequence: int,
                     legacy_request_id: str) -> ShadowAttemptRecord:
        with orm.Session(self._database()) as session, session.begin():
            return self.bind_request_in_session(session, action_id,
                                                request_sequence,
                                                legacy_request_id)

    def mark_request_association_unknown_in_session(
            self, session: orm.Session, action_id: uuid.UUID,
            request_sequence: int) -> ShadowAttemptRecord:
        """Close the call/ID-bind gap without inventing a request ID."""
        self._require_session(session)
        parsed = _canonical_uuid(action_id, name='action_id')
        sequence = _positive_integer(request_sequence, name='request_sequence')
        sample, attempt = self._lock_parent_and_attempt(session, parsed,
                                                        sequence)
        if attempt.phase is actions.ShadowAttemptPhase.REQUEST_ASSOCIATION_UNKNOWN:
            return attempt
        if (attempt.phase is not actions.ShadowAttemptPhase.PRE_SUBMIT or
                attempt.planned_execution_kind
                is not actions.PlannedExecutionKind.API_REQUEST):
            raise kernel_actions.StaleRevision(
                'Request association can become unknown only from PRE_SUBMIT.')
        now = sqlalchemy.func.clock_timestamp()
        session.execute(
            sqlalchemy.update(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == parsed,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence == sequence).
            values(phase=(
                actions.ShadowAttemptPhase.REQUEST_ASSOCIATION_UNKNOWN.value),
                   completed_at=now,
                   updated_at=now))
        row = session.execute(
            sqlalchemy.select(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == parsed,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence ==
                sequence).with_for_update()).mappings().one()
        return _attempt_record(row, sample)

    def mark_request_association_unknown(
            self, action_id: uuid.UUID,
            request_sequence: int) -> ShadowAttemptRecord:
        with orm.Session(self._database()) as session, session.begin():
            return self.mark_request_association_unknown_in_session(
                session, action_id, request_sequence)

    def abandon_pre_submit_in_session(
            self, session: orm.Session, action_id: uuid.UUID,
            request_sequence: int, *,
            mutation_function_was_never_entered: bool) -> ShadowAttemptRecord:
        """Abandon only with an explicit proof the mutation call was not entered."""
        if mutation_function_was_never_entered is not True:
            raise ValueError('Pre-submit abandonment requires proof that the '
                             'mutation function was never entered.')
        self._require_session(session)
        parsed = _canonical_uuid(action_id, name='action_id')
        sequence = _positive_integer(request_sequence, name='request_sequence')
        sample, attempt = self._lock_parent_and_attempt(session, parsed,
                                                        sequence)
        if attempt.phase is actions.ShadowAttemptPhase.ABANDONED_PRE_SUBMIT:
            return attempt
        if attempt.phase is not actions.ShadowAttemptPhase.PRE_SUBMIT:
            raise kernel_actions.StaleRevision(
                'Only PRE_SUBMIT evidence can be proven abandoned.')
        now = sqlalchemy.func.clock_timestamp()
        session.execute(
            sqlalchemy.update(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == parsed,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence == sequence).
            values(
                phase=(actions.ShadowAttemptPhase.ABANDONED_PRE_SUBMIT.value),
                completed_at=now,
                updated_at=now))
        row = session.execute(
            sqlalchemy.select(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == parsed,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence ==
                sequence).with_for_update()).mappings().one()
        return _attempt_record(row, sample)

    def abandon_pre_submit(
            self, action_id: uuid.UUID, request_sequence: int, *,
            mutation_function_was_never_entered: bool) -> ShadowAttemptRecord:
        with orm.Session(self._database()) as session, session.begin():
            return self.abandon_pre_submit_in_session(
                session,
                action_id,
                request_sequence,
                mutation_function_was_never_entered=
                mutation_function_was_never_entered)

    def record_provider_operation_id_in_session(
            self, session: orm.Session, action_id: uuid.UUID,
            request_sequence: int,
            provider_operation_id: str) -> ShadowAttemptRecord:
        """Persist one optional provider ID without permitting replacement."""
        self._require_session(session)
        parsed = _canonical_uuid(action_id, name='action_id')
        sequence = _positive_integer(request_sequence, name='request_sequence')
        operation_id = _bounded_text(provider_operation_id,
                                     name='provider_operation_id',
                                     maximum_bytes=1024)
        sample, attempt = self._lock_parent_and_attempt(session, parsed,
                                                        sequence)
        if attempt.provider_operation_id is not None:
            if attempt.provider_operation_id != operation_id:
                raise kernel_actions.ActionConflict(
                    'Shadow attempt already has a different provider '
                    'operation ID.')
            return attempt
        if attempt.phase not in (
                actions.ShadowAttemptPhase.REQUEST_BOUND,
                actions.ShadowAttemptPhase.REQUEST_ASSOCIATION_UNKNOWN
        ) and not (attempt.phase is actions.ShadowAttemptPhase.PRE_SUBMIT and
                   attempt.planned_execution_kind
                   is actions.PlannedExecutionKind.LEGACY_DIRECT_DOWN):
            raise kernel_actions.StaleRevision(
                'Provider operation ID is not writable in this phase.')
        session.execute(
            sqlalchemy.update(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == parsed,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence ==
                sequence).values(provider_operation_id=operation_id,
                                 updated_at=sqlalchemy.func.clock_timestamp()))
        row = session.execute(
            sqlalchemy.select(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == parsed,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence ==
                sequence).with_for_update()).mappings().one()
        return _attempt_record(row, sample)

    def record_provider_operation_id(
            self, action_id: uuid.UUID, request_sequence: int,
            provider_operation_id: str) -> ShadowAttemptRecord:
        with orm.Session(self._database()) as session, session.begin():
            return self.record_provider_operation_id_in_session(
                session, action_id, request_sequence, provider_operation_id)

    def complete_attempt_in_session(
        self,
        session: orm.Session,
        action_id: uuid.UUID,
        request_sequence: int,
        actual_outcome: actions.ServeReplicaActionOutcomeV1,
        proposed_outcome: actions.ServeReplicaActionOutcomeV1,
        retry_decision: actions.ServeShadowRetryDecisionV1,
        *,
        pre_observation: actions.ProviderLifecycleObservationV1 | None = None,
        post_observation: actions.ProviderLifecycleObservationV1 | None = None,
        divergence_class: actions.ShadowDivergenceClass | None = None,
    ) -> ShadowAttemptRecord:
        """Complete one child with bounded typed actual/proposed evidence."""
        self._require_session(session)
        parsed = _canonical_uuid(action_id, name='action_id')
        sequence = _positive_integer(request_sequence, name='request_sequence')
        for name, value, expected_type in (
            ('actual_outcome', actual_outcome,
             actions.ServeReplicaActionOutcomeV1),
            ('proposed_outcome', proposed_outcome,
             actions.ServeReplicaActionOutcomeV1),
            ('retry_decision', retry_decision,
             actions.ServeShadowRetryDecisionV1)):
            if not isinstance(value, expected_type):
                raise TypeError(f'{name} has an invalid type.')
        for name, observation in (('pre_observation', pre_observation),
                                  ('post_observation', post_observation)):
            if observation is not None and not isinstance(
                    observation, actions.ProviderLifecycleObservationV1):
                raise TypeError(f'{name} has an invalid type.')
        divergence = (divergence_class if isinstance(
            divergence_class, actions.ShadowDivergenceClass) else
                      (None if divergence_class is None else
                       actions.ShadowDivergenceClass(divergence_class)))
        sample, attempt = self._lock_parent_and_attempt(session, parsed,
                                                        sequence)
        if retry_decision.logical_attempt != attempt.logical_attempt:
            raise kernel_actions.ActionConflict(
                'Retry decision belongs to another logical attempt.')
        for observation in (pre_observation, post_observation):
            if observation is not None:
                observation.validate_target(
                    sample.provider_plan.requested_target)
        if (attempt.planned_execution_kind
                is actions.PlannedExecutionKind.LEGACY_DIRECT_DOWN and
                divergence is None):
            raise kernel_actions.ActionConflict(
                'legacy_direct_down completion must be divergent.')
        operation_ids = _operation_ids(attempt.provider_operation_id,
                                       actual_outcome, proposed_outcome,
                                       pre_observation, post_observation)
        if len(operation_ids) > 1:
            raise kernel_actions.ActionConflict(
                'Provider operation evidence is not write-once consistent.')
        operation_id = next(iter(operation_ids), None)
        try:
            actual_outcome = _outcome_with_provider_operation_id(
                actual_outcome, operation_id)
            proposed_outcome = _outcome_with_provider_operation_id(
                proposed_outcome, operation_id)
            actual_outcome.validate_for_invocation(attempt.invocation)
            proposed_outcome.validate_for_invocation(attempt.invocation)
        except ValueError as e:
            raise kernel_actions.ActionConflict(
                f'Shadow outcome evidence is invalid: {e}') from e
        if (divergence is None and actual_outcome.canonical_bytes
                != proposed_outcome.canonical_bytes):
            raise kernel_actions.ActionConflict(
                'Nondivergent actual/proposed outcomes must be byte-equal.')
        values = {
            'provider_operation_id': operation_id,
            'actual_outcome': actual_outcome.canonical_value(),
            'actual_outcome_sha256': actual_outcome.sha256,
            'proposed_outcome': proposed_outcome.canonical_value(),
            'proposed_outcome_sha256': proposed_outcome.sha256,
            'retry_decision': retry_decision.canonical_value(),
            'retry_decision_sha256': retry_decision.sha256,
            'pre_observation': (None if pre_observation is None else
                                pre_observation.canonical_value()),
            'pre_observation_sha256':
                (None if pre_observation is None else pre_observation.sha256),
            'post_observation': (None if post_observation is None else
                                 post_observation.canonical_value()),
            'post_observation_sha256':
                (None if post_observation is None else post_observation.sha256),
            'divergence_class':
                (None if divergence is None else divergence.value),
        }
        if attempt.phase is actions.ShadowAttemptPhase.COMPLETE:
            stored_values = {
                'provider_operation_id': attempt.provider_operation_id,
                'actual_outcome': attempt.actual_outcome.canonical_value() if
                                  attempt.actual_outcome is not None else None,
                'proposed_outcome': attempt.proposed_outcome.canonical_value()
                                    if attempt.proposed_outcome is not None else
                                    None,
                'retry_decision': attempt.retry_decision.canonical_value() if
                                  attempt.retry_decision is not None else None,
                'pre_observation': attempt.pre_observation.canonical_value()
                                   if attempt.pre_observation is not None else
                                   None,
                'post_observation': attempt.post_observation.canonical_value()
                                    if attempt.post_observation is not None else
                                    None,
                'divergence_class': (None if attempt.divergence_class is None
                                     else attempt.divergence_class.value),
            }
            replay_values = {
                key: value
                for key, value in values.items()
                if not key.endswith('_sha256')
            }
            if not _canonical_equal(stored_values, replay_values):
                raise kernel_actions.ActionConflict(
                    'Completed shadow attempt replay has different evidence.')
            return attempt
        allowed = (attempt.phase is actions.ShadowAttemptPhase.REQUEST_BOUND and
                   attempt.planned_execution_kind
                   is actions.PlannedExecutionKind.API_REQUEST) or (
                       attempt.phase is actions.ShadowAttemptPhase.PRE_SUBMIT
                       and attempt.planned_execution_kind
                       is actions.PlannedExecutionKind.LEGACY_DIRECT_DOWN)
        if not allowed:
            raise kernel_actions.StaleRevision(
                'Shadow attempt is not in a completable phase.')
        now = sqlalchemy.func.clock_timestamp()
        values.update({
            'phase': actions.ShadowAttemptPhase.COMPLETE.value,
            'completed_at': now,
            'updated_at': now,
        })
        session.execute(
            sqlalchemy.update(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == parsed,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence ==
                sequence).values(**values))
        row = session.execute(
            sqlalchemy.select(state_schema.SHADOW_ATTEMPTS).where(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id == parsed,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence ==
                sequence).with_for_update()).mappings().one()
        return _attempt_record(row, sample)

    def complete_attempt(
        self,
        action_id: uuid.UUID,
        request_sequence: int,
        actual_outcome: actions.ServeReplicaActionOutcomeV1,
        proposed_outcome: actions.ServeReplicaActionOutcomeV1,
        retry_decision: actions.ServeShadowRetryDecisionV1,
        *,
        pre_observation: actions.ProviderLifecycleObservationV1 | None = None,
        post_observation: actions.ProviderLifecycleObservationV1 | None = None,
        divergence_class: actions.ShadowDivergenceClass | None = None,
    ) -> ShadowAttemptRecord:
        with orm.Session(self._database()) as session, session.begin():
            return self.complete_attempt_in_session(
                session,
                action_id,
                request_sequence,
                actual_outcome,
                proposed_outcome,
                retry_decision,
                pre_observation=pre_observation,
                post_observation=post_observation,
                divergence_class=divergence_class)

    def finalize_parent_in_session(
        self,
        session: orm.Session,
        action_id: uuid.UUID,
        expected_revision: int,
        legacy_projection: actions.ServeShadowProjectionV1 | None,
        proposed_projection: actions.ServeShadowProjectionV1 | None,
        parity_class: actions.ShadowParityClass,
    ) -> ShadowSampleRecord:
        """Atomically finalize projections after every child is terminal."""
        self._require_session(session)
        parsed = _canonical_uuid(action_id, name='action_id')
        expected = _positive_integer(expected_revision,
                                     name='expected_revision')
        parity = (parity_class
                  if isinstance(parity_class, actions.ShadowParityClass) else
                  actions.ShadowParityClass(parity_class))
        for name, projection in (('legacy_projection', legacy_projection),
                                 ('proposed_projection', proposed_projection)):
            if projection is not None and not isinstance(
                    projection, actions.ServeShadowProjectionV1):
                raise TypeError(f'{name} has an invalid type.')
        parent_row = self._locked_sample(session, parsed)
        if parent_row is None:
            raise kernel_actions.InvariantViolation(
                f'Unknown shadow sample {parsed}.')
        sample = _sample_record(parent_row)
        attempts = self._locked_attempts(session, sample)
        if not attempts:
            raise kernel_actions.ActionConflict(
                'Shadow parent cannot finalize without a child attempt.')
        if any(attempt.phase not in _TERMINAL_ATTEMPT_PHASES
               for attempt in attempts):
            raise kernel_actions.StaleRevision(
                'Shadow parent cannot finalize with a nonterminal child.')
        graph_problems = _validate_child_graph(sample,
                                               attempts,
                                               require_closed=True)
        if graph_problems:
            raise kernel_actions.InvariantViolation(
                f'Shadow attempt graph is invalid: {graph_problems[0]}.')
        if parity is actions.ShadowParityClass.PENDING:
            raise ValueError('A terminal parent cannot retain PENDING parity.')
        if parity in (actions.ShadowParityClass.ABANDONED,
                      actions.ShadowParityClass.AMBIGUOUS):
            phase = (actions.ShadowParentPhase.ABANDONED_PRE_SUBMIT
                     if parity is actions.ShadowParityClass.ABANDONED else
                     actions.ShadowParentPhase.AMBIGUOUS)
        else:
            phase = actions.ShadowParentPhase.COMPLETE
            if legacy_projection is None or proposed_projection is None:
                raise ValueError('Complete parity requires both projections.')
        for projection in (legacy_projection, proposed_projection):
            if (projection is not None and
                    projection.action_kind is not sample.action_kind):
                raise kernel_actions.ActionConflict(
                    'Projection action kind differs from its parent.')
        if parity is actions.ShadowParityClass.MATCH:
            if legacy_projection is None or proposed_projection is None:
                raise kernel_actions.ActionConflict(
                    'MATCH parity requires two typed projections.')
            try:
                _validate_match_evidence(sample, attempts, legacy_projection,
                                         proposed_projection)
            except ValueError as e:
                raise kernel_actions.ActionConflict(
                    'MATCH parity is not supported by complete eligible '
                    f'byte-equal evidence: {e}') from e
        if (sample.profile_eligibility is actions.ProfileEligibility.UNSUPPORTED
                and parity
                is not actions.ShadowParityClass.UNSUPPORTED_PROVIDER_PROFILE):
            raise kernel_actions.ActionConflict(
                'Unsupported provider profile requires its dedicated parity.')
        if (parity.value in _DIVERGENCE_PARITIES and
                not any(attempt.divergence_class is not None and
                        attempt.divergence_class.value == parity.value
                        for attempt in attempts)):
            raise kernel_actions.ActionConflict(
                'Divergent parent parity lacks a matching child divergence.')
        if (parity is actions.ShadowParityClass.ABANDONED and attempts and
                not any(attempt.phase is
                        actions.ShadowAttemptPhase.ABANDONED_PRE_SUBMIT
                        for attempt in attempts)):
            raise kernel_actions.ActionConflict(
                'ABANDONED parity lacks abandoned child evidence.')
        if (parity is actions.ShadowParityClass.AMBIGUOUS and attempts and
                not any(attempt.phase is actions.ShadowAttemptPhase.
                        REQUEST_ASSOCIATION_UNKNOWN or
                        (attempt.actual_outcome is not None and
                         attempt.actual_outcome.disposition is
                         actions.ServeActionDisposition.UNCERTAIN)
                        for attempt in attempts)):
            raise kernel_actions.ActionConflict(
                'AMBIGUOUS parity lacks uncertainty evidence.')
        legacy_value = (None if legacy_projection is None else
                        legacy_projection.canonical_value())
        proposed_value = (None if proposed_projection is None else
                          proposed_projection.canonical_value())
        exact_terminal = (
            sample.phase is phase and sample.parity_class is parity and
            ((sample.legacy_projection is None and legacy_value is None) or
             (sample.legacy_projection is not None and legacy_value is not None
              and _canonical_equal(sample.legacy_projection.canonical_value(),
                                   legacy_value))) and
            ((sample.proposed_projection is None and proposed_value is None) or
             (sample.proposed_projection is not None and
              proposed_value is not None and
              _canonical_equal(sample.proposed_projection.canonical_value(),
                               proposed_value))))
        if sample.phase in (actions.ShadowParentPhase.COMPLETE,
                            actions.ShadowParentPhase.ABANDONED_PRE_SUBMIT,
                            actions.ShadowParentPhase.AMBIGUOUS):
            if sample.revision != expected + 1 or not exact_terminal:
                raise kernel_actions.StaleRevision(
                    'Finalized parent replay differs or has advanced.')
            return sample
        if sample.revision != expected:
            raise kernel_actions.StaleRevision(
                f'Shadow sample {parsed} is not expected revision {expected}.')
        now = sqlalchemy.func.clock_timestamp()
        session.execute(
            sqlalchemy.update(state_schema.SHADOW_SAMPLES).where(
                state_schema.SHADOW_SAMPLES.c.would_be_action_id == parsed,
                state_schema.SHADOW_SAMPLES.c.revision == expected).values(
                    phase=phase.value,
                    legacy_projection=legacy_value,
                    legacy_projection_sha256=(None if legacy_projection is None
                                              else legacy_projection.sha256),
                    proposed_projection=proposed_value,
                    proposed_projection_sha256=(None if proposed_projection
                                                is None else
                                                proposed_projection.sha256),
                    parity_class=parity.value,
                    revision=expected + 1,
                    completed_at=now,
                    updated_at=now))
        row = self._locked_sample(session, parsed)
        assert row is not None
        return _sample_record(row)

    def finalize_parent(
        self,
        action_id: uuid.UUID,
        expected_revision: int,
        legacy_projection: actions.ServeShadowProjectionV1 | None,
        proposed_projection: actions.ServeShadowProjectionV1 | None,
        parity_class: actions.ShadowParityClass,
    ) -> ShadowSampleRecord:
        with orm.Session(self._database()) as session, session.begin():
            return self.finalize_parent_in_session(session, action_id,
                                                   expected_revision,
                                                   legacy_projection,
                                                   proposed_projection,
                                                   parity_class)

    @staticmethod
    def _mode_record(row: Mapping[str, Any]) -> ServiceModeRecord:
        try:
            service_hash = str(row['hash'])
            service_incarnation = _canonical_uuid(service_hash,
                                                  name='services.hash')
            if str(service_incarnation) != service_hash:
                raise ValueError('services.hash is not canonical UUID text.')
            return ServiceModeRecord(
                service_name=str(row['name']),
                service_hash=service_hash,
                mode=actions.ResourceActionMode(row['resource_action_mode']),
                changed_at=_optional_timestamp(
                    row.get('resource_action_mode_changed_at'),
                    name='resource_action_mode_changed_at'))
        except (KeyError, TypeError, ValueError) as e:
            raise kernel_actions.InvariantViolation(
                f'Invalid resource-action service mode row: {e}') from e

    def _promotion_report_in_session(
        self,
        session: orm.Session,
        service_name: str,
        service_hash: str,
        candidate_since: datetime.datetime,
        minimum_launch_samples: int,
        minimum_down_samples: int,
        *,
        lock_rows: bool,
    ) -> PromotionBlockerReport:
        candidate_since = _timestamp(candidate_since, name='candidate_since')
        decision_reasons: dict[uuid.UUID, list[str]] = {}

        def add_reason(decision_id: uuid.UUID, reason: str) -> None:
            bucket = decision_reasons.setdefault(decision_id, [])
            if reason not in bucket:
                bucket.append(reason)

        # The service/owner row is already locked by the caller.  Preserve the
        # reviewed global order: live replicas, cleanup intents, references,
        # coverage, coverage-only attempts, parents, then represented children.
        replica_statement = sqlalchemy.select(
            serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                service_name).order_by(
                    serve_state_schema.replicas_table.c.replica_id)
        if lock_rows:
            replica_statement = replica_statement.with_for_update()
        replica_rows = session.execute(replica_statement).mappings().all()

        cleanup_statement = sqlalchemy.select(
            serve_state_schema.ephemeral_storage_cleanup_intents_table).where(
                serve_state_schema.ephemeral_storage_cleanup_intents_table.c.
                service_name == service_name).order_by(
                    serve_state_schema.ephemeral_storage_cleanup_intents_table.
                    c.resource_scope,
                    serve_state_schema.ephemeral_storage_cleanup_intents_table.
                    c.storage_generation)
        if lock_rows:
            cleanup_statement = cleanup_statement.with_for_update()
        session.execute(cleanup_statement).mappings().all()

        expected_ids: set[uuid.UUID] = set()
        replica_links: dict[uuid.UUID,
                            list[tuple[Mapping[str,
                                               Any], kernel_actions.ActionKind,
                                       uuid.UUID | None,
                                       uuid.UUID | None]]] = {}
        replica_link_count = 0
        for row in replica_rows:
            try:
                replica_id = row['replica_id']
                if (not isinstance(replica_id, int) or
                        isinstance(replica_id, bool) or replica_id < 0):
                    raise ValueError('replica_id must be nonnegative.')
                identity_values = (row['replica_incarnation'],
                                   row['desired_generation'],
                                   row['sky_cluster_record_uuid'])
                if any(value is not None
                       for value in identity_values) and not all(
                           value is not None for value in identity_values):
                    raise ValueError(
                        'replica action identity triple must be all-null or '
                        'fully populated.')
                fully_identified = all(
                    value is not None for value in identity_values)
                live = False
                if fully_identified:
                    _canonical_uuid(row['replica_incarnation'],
                                    name='replicas.replica_incarnation')
                    _positive_integer(row['desired_generation'],
                                      name='replicas.desired_generation')
                    _canonical_uuid(row['sky_cluster_record_uuid'],
                                    name='replicas.sky_cluster_record_uuid')
                    replica_status = ReplicaStatus(row['status'])
                    live = replica_status not in ReplicaStatus.terminal_statuses(
                    )
                for action_kind, coverage_column, sample_column in (
                    (kernel_actions.ActionKind.LAUNCH,
                     'launch_shadow_coverage_id', 'launch_shadow_sample_id'),
                    (kernel_actions.ActionKind.DOWN, 'down_shadow_coverage_id',
                     'down_shadow_sample_id'),
                ):
                    coverage_id = (None if row[coverage_column] is None else
                                   _canonical_uuid(row[coverage_column],
                                                   name=coverage_column))
                    sample_id = (None if row[sample_column] is None else
                                 _canonical_uuid(row[sample_column],
                                                 name=sample_column))
                    if (action_kind is kernel_actions.ActionKind.LAUNCH and
                            live and coverage_id is None and sample_id is None):
                        missing_identity = actions.CoverageDecisionIdentityV1(
                            version=1,
                            service_hash=service_hash,
                            service_incarnation=_canonical_uuid(
                                service_hash, name='service_hash'),
                            replica_id=replica_id,
                            replica_incarnation=_canonical_uuid(
                                row['replica_incarnation'],
                                name='replicas.replica_incarnation'),
                            desired_generation=_positive_integer(
                                row['desired_generation'],
                                name='replicas.desired_generation'),
                            action_type=kernel_actions.ActionKind.LAUNCH)
                        missing_id = missing_identity.decision_id
                        expected_ids.add(missing_id)
                        add_reason(missing_id,
                                   'live_replica_missing_launch_coverage')
                        replica_links.setdefault(missing_id, []).append(
                            (row, action_kind, None, None))
                        replica_link_count += 1
                    if coverage_id is not None or sample_id is not None:
                        replica_link_count += 1
                    if (replica_link_count
                            > _MAX_PROMOTION_INVENTORY_ATTEMPTS_AND_LINKS):
                        raise kernel_actions.ActionConflict(
                            'Shadow promotion coverage inventory exceeds the '
                            f'{_MAX_PROMOTION_INVENTORY_ATTEMPTS_AND_LINKS} '
                            'combined attempt/link row cap.')
                    if coverage_id is not None:
                        expected_ids.add(coverage_id)
                    if sample_id is not None:
                        expected_ids.add(sample_id)
                        if coverage_id != sample_id:
                            add_reason(sample_id,
                                       'replica_sample_coverage_link_mismatch')
                            if coverage_id is not None:
                                add_reason(
                                    coverage_id,
                                    'replica_sample_coverage_link_mismatch')
                    for linked_id in {coverage_id, sample_id} - {None}:
                        assert linked_id is not None
                        replica_links.setdefault(linked_id, []).append(
                            (row, action_kind, coverage_id, sample_id))
                    if len(expected_ids) > _MAX_PROMOTION_INVENTORY_DECISIONS:
                        raise kernel_actions.ActionConflict(
                            'Shadow promotion coverage inventory exceeds the '
                            f'{_MAX_PROMOTION_INVENTORY_DECISIONS} decision '
                            'row cap.')
            except (KeyError, TypeError, ValueError) as e:
                raise kernel_actions.InvariantViolation(
                    f'Invalid promotion replica link row: {e}') from e

        coverage_window_predicate = sqlalchemy.and_(
            state_schema.SHADOW_COVERAGE.c.service_name == service_name,
            state_schema.SHADOW_COVERAGE.c.service_hash == service_hash,
            state_schema.SHADOW_COVERAGE.c.admitted_at >= candidate_since)
        discovered_coverage_ids = {
            _canonical_uuid(value, name='decision_id')
            for value in session.execute(
                sqlalchemy.select(state_schema.SHADOW_COVERAGE.c.decision_id).
                where(coverage_window_predicate).limit(
                    _MAX_PROMOTION_INVENTORY_DECISIONS + 1)).scalars()
        }
        parent_window_predicate = sqlalchemy.and_(
            state_schema.SHADOW_SAMPLES.c.service_name == service_name,
            state_schema.SHADOW_SAMPLES.c.service_hash == service_hash,
            state_schema.SHADOW_SAMPLES.c.created_at >= candidate_since)
        discovered_parent_ids = {
            _canonical_uuid(value, name='would_be_action_id')
            for value in session.execute(
                sqlalchemy.select(
                    state_schema.SHADOW_SAMPLES.c.would_be_action_id).where(
                        parent_window_predicate).limit(
                            _MAX_PROMOTION_INVENTORY_DECISIONS + 1)).scalars()
        }
        expected_ids.update(discovered_coverage_ids)
        expected_ids.update(discovered_parent_ids)
        if len(expected_ids) > _MAX_PROMOTION_INVENTORY_DECISIONS:
            raise kernel_actions.ActionConflict(
                'Shadow promotion coverage inventory exceeds the '
                f'{_MAX_PROMOTION_INVENTORY_DECISIONS} decision row cap.')

        reference_predicate = sqlalchemy.and_(
            state_schema.WORKER_COHORT_REFS.c.service_hash == service_hash,
            state_schema.WORKER_COHORT_REFS.c.reference_state
            != actions.WorkerCohortReferenceState.RELEASED.value)
        if expected_ids:
            reference_predicate = sqlalchemy.or_(
                reference_predicate,
                state_schema.WORKER_COHORT_REFS.c.decision_id.in_(expected_ids))
        reference_statement = sqlalchemy.select(
            state_schema.WORKER_COHORT_REFS).where(
                reference_predicate).order_by(
                    state_schema.WORKER_COHORT_REFS.c.decision_id).limit(
                        _MAX_PROMOTION_INVENTORY_DECISIONS + 1)
        if lock_rows:
            reference_statement = reference_statement.with_for_update()
        reference_rows = session.execute(reference_statement).mappings().all()
        references = [
            _worker_cohort_reference_record(row) for row in reference_rows
        ]
        reference_by_id = {
            reference.decision_id: reference for reference in references
        }
        for candidate_reference in references:
            if candidate_reference.reference.service_hash != service_hash:
                continue
            if (candidate_reference.reference_state
                    is not actions.WorkerCohortReferenceState.RELEASED):
                expected_ids.add(candidate_reference.decision_id)
            if (candidate_reference.reference_state
                    is actions.WorkerCohortReferenceState.PREPARING):
                add_reason(candidate_reference.decision_id,
                           'worker_cohort_reference:PREPARING')
            elif (candidate_reference.reference_state
                  is actions.WorkerCohortReferenceState.ACTION_ACTIVE):
                add_reason(candidate_reference.decision_id,
                           'worker_cohort_reference:ACTION_ACTIVE')
        if len(expected_ids) > _MAX_PROMOTION_INVENTORY_DECISIONS:
            raise kernel_actions.ActionConflict(
                'Shadow promotion coverage inventory exceeds the '
                f'{_MAX_PROMOTION_INVENTORY_DECISIONS} decision row cap.')

        coverage_predicate = coverage_window_predicate
        if expected_ids:
            coverage_predicate = sqlalchemy.or_(
                coverage_predicate,
                state_schema.SHADOW_COVERAGE.c.decision_id.in_(expected_ids))
        coverage_statement = sqlalchemy.select(
            state_schema.SHADOW_COVERAGE).where(coverage_predicate).order_by(
                state_schema.SHADOW_COVERAGE.c.decision_id).limit(
                    _MAX_PROMOTION_INVENTORY_DECISIONS + 1)
        if lock_rows:
            coverage_statement = coverage_statement.with_for_update()
        coverage_rows = session.execute(coverage_statement).mappings().all()
        coverages = [_shadow_coverage_record(row) for row in coverage_rows]
        coverage_by_id = {
            coverage.decision_id: coverage for coverage in coverages
        }
        candidate_ids = expected_ids | set(coverage_by_id)
        if len(candidate_ids) > _MAX_PROMOTION_INVENTORY_DECISIONS:
            raise kernel_actions.ActionConflict(
                'Shadow promotion coverage inventory exceeds the '
                f'{_MAX_PROMOTION_INVENTORY_DECISIONS} decision row cap.')

        coverage_attempt_rows: list[Mapping[str, Any]] = []
        if coverage_by_id:
            coverage_attempt_statement = sqlalchemy.select(
                state_schema.SHADOW_COVERAGE_ATTEMPTS).where(
                    state_schema.SHADOW_COVERAGE_ATTEMPTS.c.decision_id.
                    in_(coverage_by_id)).order_by(
                        state_schema.SHADOW_COVERAGE_ATTEMPTS.c.decision_id,
                        state_schema.SHADOW_COVERAGE_ATTEMPTS.c.request_sequence
                    ).limit(_MAX_PROMOTION_INVENTORY_ATTEMPTS_AND_LINKS -
                            replica_link_count + 1)
            if lock_rows:
                coverage_attempt_statement = (
                    coverage_attempt_statement.with_for_update())
            coverage_attempt_rows = list(
                session.execute(coverage_attempt_statement).mappings().all())
        if (replica_link_count + len(coverage_attempt_rows)
                > _MAX_PROMOTION_INVENTORY_ATTEMPTS_AND_LINKS):
            raise kernel_actions.ActionConflict(
                'Shadow promotion coverage inventory exceeds the '
                f'{_MAX_PROMOTION_INVENTORY_ATTEMPTS_AND_LINKS} combined '
                'attempt/link row cap.')
        coverage_attempts_by_id: dict[uuid.UUID,
                                      list[actions.CoverageAttemptV1]] = {
                                          decision_id: []
                                          for decision_id in coverage_by_id
                                      }
        for row in coverage_attempt_rows:
            attempt = _coverage_attempt_record(row)
            coverage_attempts_by_id[attempt.decision_id].append(attempt)

        parent_predicate = parent_window_predicate
        if candidate_ids:
            parent_predicate = sqlalchemy.or_(
                parent_predicate,
                state_schema.SHADOW_SAMPLES.c.would_be_action_id.in_(
                    candidate_ids))
        parent_statement = sqlalchemy.select(
            state_schema.SHADOW_SAMPLES).where(parent_predicate).order_by(
                state_schema.SHADOW_SAMPLES.c.would_be_action_id).limit(
                    _MAX_PROMOTION_INVENTORY_DECISIONS + 1)
        if lock_rows:
            parent_statement = parent_statement.with_for_update()
        parent_rows = session.execute(parent_statement).mappings().all()
        samples = [_sample_record(row) for row in parent_rows]
        sample_by_id = {sample.action_id: sample for sample in samples}
        candidate_ids.update(sample_by_id)
        if len(candidate_ids) > _MAX_PROMOTION_INVENTORY_DECISIONS:
            raise kernel_actions.ActionConflict(
                'Shadow promotion coverage inventory exceeds the '
                f'{_MAX_PROMOTION_INVENTORY_DECISIONS} decision row cap.')

        attempt_rows: list[Mapping[str, Any]] = []
        if sample_by_id:
            child_statement = sqlalchemy.select(
                state_schema.SHADOW_ATTEMPTS).where(
                    state_schema.SHADOW_ATTEMPTS.c.would_be_action_id.in_(
                        sample_by_id)).order_by(
                            state_schema.SHADOW_ATTEMPTS.c.would_be_action_id,
                            state_schema.SHADOW_ATTEMPTS.c.request_sequence)
            if lock_rows:
                child_statement = child_statement.with_for_update()
            attempt_rows = list(
                session.execute(child_statement).mappings().all())
        attempts_by_id: dict[uuid.UUID, list[ShadowAttemptRecord]] = {
            action_id: [] for action_id in sample_by_id
        }
        for row in attempt_rows:
            action_id = _canonical_uuid(row['would_be_action_id'],
                                        name='would_be_action_id')
            attempts_by_id[action_id].append(
                _attempt_record(row, sample_by_id[action_id]))

        inventory_decisions: list[dict[str, Any]] = []
        for decision_id in sorted(candidate_ids, key=lambda value: value.bytes):
            coverage = coverage_by_id.get(decision_id)
            reference_record = reference_by_id.get(decision_id)
            sample = sample_by_id.get(decision_id)
            try:
                inventory_replica_links = []
                for (replica_row, action_kind, coverage_link_id,
                     sample_link_id) in sorted(
                         replica_links.get(decision_id, []),
                         key=lambda link:
                         (link[0]['replica_id'], link[1].value)):
                    inventory_replica_links.append({
                        'replica_id': replica_row['replica_id'],
                        'replica_incarnation': str(
                            _canonical_uuid(replica_row['replica_incarnation'],
                                            name='replicas.replica_incarnation')
                        ),
                        'desired_generation': _positive_integer(
                            replica_row['desired_generation'],
                            name='replicas.desired_generation'),
                        'action_type': action_kind.value,
                        'coverage_id': (None if coverage_link_id is None else
                                        str(coverage_link_id)),
                        'represented_sample_id': (None if sample_link_id is None
                                                  else str(sample_link_id)),
                    })
                reference_value = None
                if reference_record is not None:
                    reference_value = {
                        'reference':
                            reference_record.reference.canonical_value(),
                        'reference_state':
                            reference_record.reference_state.value,
                        'revision': reference_record.revision,
                        'created_at': _utc_timestamp_text(
                            reference_record.created_at,
                            name='cohort_reference.created_at'),
                        'bound_at': (None if reference_record.bound_at is None
                                     else _utc_timestamp_text(
                                         reference_record.bound_at,
                                         name='cohort_reference.bound_at')),
                        'released_at':
                            (None if reference_record.released_at is None else
                             _utc_timestamp_text(
                                 reference_record.released_at,
                                 name='cohort_reference.released_at')),
                    }
                parent_value = None
                if sample is not None:
                    parent_value = {
                        'would_be_action_id': str(sample.action_id),
                        'immutable_spec_sha256': sample.immutable_spec_sha256,
                        'provider_plan_sha256': sample.provider_plan_sha256,
                        'phase': sample.phase.value,
                        'parity_class': sample.parity_class.value,
                        'revision': sample.revision,
                        'created_at': _utc_timestamp_text(
                            sample.created_at, name='sample.created_at'),
                        'updated_at': _utc_timestamp_text(
                            sample.updated_at, name='sample.updated_at'),
                        'completed_at':
                            (None if sample.completed_at is None else
                             _utc_timestamp_text(sample.completed_at,
                                                 name='sample.completed_at')),
                    }
                inventory_decisions.append({
                    'decision_id': str(decision_id),
                    'coverage': (None if coverage is None else
                                 coverage.canonical_value()),
                    'cohort_reference': reference_value,
                    'replica_links': inventory_replica_links,
                    'represented_parent': parent_value,
                    'coverage_attempts': [
                        attempt.canonical_value()
                        for attempt in coverage_attempts_by_id.get(
                            decision_id, [])
                    ],
                })
            except (KeyError, TypeError, ValueError) as e:
                raise kernel_actions.InvariantViolation(
                    f'Invalid promotion coverage inventory row: {e}') from e
        coverage_inventory_sha256 = actions.canonical_sha256({
            'version': 1,
            'service_name': service_name,
            'service_hash': service_hash,
            'candidate_since': _utc_timestamp_text(candidate_since,
                                                   name='candidate_since'),
            'decisions': inventory_decisions,
        })

        for decision_id in sorted(candidate_ids, key=lambda value: value.bytes):
            coverage = coverage_by_id.get(decision_id)
            sample = sample_by_id.get(decision_id)
            reference_record = reference_by_id.get(decision_id)
            if coverage is None:
                add_reason(decision_id, 'missing_candidate_coverage')
                if sample is not None:
                    add_reason(decision_id, 'parent_without_coverage')
                if (reference_record is not None and
                        reference_record.reference_state
                        is not actions.WorkerCohortReferenceState.RELEASED):
                    add_reason(decision_id, 'active_reference_without_coverage')
                continue
            admitted_at = _canonical_timestamp_datetime(
                coverage.admitted_at, name='coverage.admitted_at')
            if (coverage.service_name != service_name or
                    coverage.service_hash != service_hash or
                    admitted_at < candidate_since):
                add_reason(decision_id, 'coverage_outside_candidate_window')
            if coverage.worker_cohort_ref_id is None:
                add_reason(decision_id, 'coverage_without_cohort_reference')
            elif reference_record is None:
                add_reason(decision_id, 'coverage_reference_missing')
            else:
                try:
                    reference_record.reference.validate_coverage(coverage)
                except ValueError:
                    add_reason(decision_id, 'coverage_reference_mismatch')
                if reference_record.reference_state not in (
                        actions.WorkerCohortReferenceState.SHADOW_ACTIVE,
                        actions.WorkerCohortReferenceState.RELEASED):
                    add_reason(
                        decision_id, 'coverage_reference_state:'
                        f'{reference_record.reference_state.value}')
            for (replica_row, action_kind, coverage_link_id,
                 sample_link_id) in replica_links.get(decision_id, []):
                try:
                    replica_incarnation = _canonical_uuid(
                        replica_row['replica_incarnation'],
                        name='replicas.replica_incarnation')
                    desired_generation = _positive_integer(
                        replica_row['desired_generation'],
                        name='replicas.desired_generation')
                    if (coverage.replica_id != replica_row['replica_id'] or
                            coverage.replica_incarnation != replica_incarnation
                            or coverage.action_type is not action_kind or
                        (action_kind is kernel_actions.ActionKind.DOWN and
                         coverage.desired_generation != desired_generation) or
                        (action_kind is kernel_actions.ActionKind.LAUNCH and
                         coverage.desired_generation > desired_generation)):
                        add_reason(decision_id,
                                   'replica_coverage_identity_mismatch')
                    represented = (
                        coverage.normalization_outcome
                        is actions.NormalizationOutcome.REPRESENTABLE)
                    if (coverage_link_id != decision_id or
                        (represented and sample_link_id != decision_id) or
                        (not represented and sample_link_id is not None)):
                        add_reason(decision_id,
                                   'replica_sample_coverage_link_mismatch')
                except (KeyError, TypeError, ValueError) as e:
                    raise kernel_actions.InvariantViolation(
                        f'Invalid promotion replica identity row: {e}') from e
            coverage_attempts = coverage_attempts_by_id.get(decision_id, [])
            if (coverage.normalization_outcome
                    is actions.NormalizationOutcome.NOT_REPRESENTABLE):
                add_reason(decision_id, 'coverage:NOT_REPRESENTABLE')
                if sample is not None:
                    add_reason(decision_id,
                               'not_representable_coverage_has_parent')
                if not coverage_attempts:
                    add_reason(decision_id, 'missing_coverage_attempt')
                for problem in _validate_coverage_attempt_graph(
                        coverage, coverage_attempts):
                    add_reason(decision_id, f'coverage_attempt_graph:{problem}')
                continue
            if coverage_attempts:
                add_reason(decision_id,
                           'representable_coverage_has_coverage_attempt')
            if sample is None:
                add_reason(decision_id, 'representable_coverage_missing_parent')
                continue
            identity = coverage.identity
            if (sample.service_name != coverage.service_name or
                    sample.provider_plan.resource_identity
                    != identity.resource_identity or
                    sample.action_kind is not coverage.action_type or
                    sample.created_at != admitted_at):
                add_reason(decision_id, 'coverage_parent_mismatch')

        clean_launch = 0
        clean_down = 0
        for decision_id in sorted(candidate_ids, key=lambda value: value.bytes):
            coverage = coverage_by_id.get(decision_id)
            sample = sample_by_id.get(decision_id)
            if (coverage is None or sample is None or
                    coverage.normalization_outcome
                    is not actions.NormalizationOutcome.REPRESENTABLE):
                continue
            sample_reasons: list[str] = []
            represented_attempts = attempts_by_id[sample.action_id]
            if sample.phase is not actions.ShadowParentPhase.COMPLETE:
                sample_reasons.append(f'phase:{sample.phase.value}')
            if sample.parity_class is not actions.ShadowParityClass.MATCH:
                sample_reasons.append(f'parity:{sample.parity_class.value}')
            if sample.profile_eligibility is not actions.ProfileEligibility.ELIGIBLE:
                sample_reasons.append('profile:UNSUPPORTED')
            sample_reasons.extend(
                _validate_child_graph(sample,
                                      represented_attempts,
                                      require_closed=True))
            if not represented_attempts:
                sample_reasons.append('missing_attempt')
            for represented_attempt in represented_attempts:
                prefix = f'attempt:{represented_attempt.request_sequence}'
                if (represented_attempt.phase
                        is not actions.ShadowAttemptPhase.COMPLETE):
                    sample_reasons.append(
                        f'{prefix}:phase:{represented_attempt.phase.value}')
                if (represented_attempt.planned_execution_kind
                        is not actions.PlannedExecutionKind.API_REQUEST):
                    sample_reasons.append(f'{prefix}:direct_down')
                if represented_attempt.legacy_request_id is None:
                    sample_reasons.append(f'{prefix}:request_unbound')
                if represented_attempt.divergence_class is not None:
                    sample_reasons.append(
                        f'{prefix}:divergence:'
                        f'{represented_attempt.divergence_class.value}')
            if (sample.parity_class is actions.ShadowParityClass.MATCH and
                    sample.legacy_projection is not None and
                    sample.proposed_projection is not None):
                try:
                    _validate_match_evidence(sample, represented_attempts,
                                             sample.legacy_projection,
                                             sample.proposed_projection)
                except ValueError as e:
                    sample_reasons.append(f'match_evidence:{e}')
            for reason in sample_reasons:
                add_reason(decision_id, reason)
            if not decision_reasons.get(decision_id):
                assert sample.legacy_projection is not None
                if (sample.legacy_projection.action_disposition
                        is actions.ServeActionDisposition.SUCCEEDED):
                    if sample.action_kind is kernel_actions.ActionKind.LAUNCH:
                        clean_launch += 1
                    else:
                        clean_down += 1
        reasons = [
            f'decision:{decision_id}:{reason}' for decision_id in sorted(
                decision_reasons, key=lambda value: value.bytes)
            for reason in decision_reasons[decision_id]
        ]
        if clean_launch < minimum_launch_samples:
            reasons.append(f'minimum_launch_samples:{clean_launch}/'
                           f'{minimum_launch_samples}')
        if clean_down < minimum_down_samples:
            reasons.append(
                f'minimum_down_samples:{clean_down}/{minimum_down_samples}')
        window_sample_count = sum(
            sample.service_name == service_name and
            sample.provider_plan.resource_identity.service_hash == service_hash
            and sample.created_at >= candidate_since for sample in samples)
        return PromotionBlockerReport(
            service_name=service_name,
            service_hash=service_hash,
            candidate_since=candidate_since,
            candidate_sample_count=window_sample_count,
            clean_launch_samples=clean_launch,
            clean_down_samples=clean_down,
            blocking_sample_ids=tuple(
                sorted(decision_reasons, key=lambda value: value.bytes)),
            coverage_inventory_sha256=coverage_inventory_sha256,
            reasons=tuple(reasons))

    def promotion_blocker_report(
            self,
            service_name: str,
            service_hash: str,
            *,
            minimum_launch_samples: int = 1,
            minimum_down_samples: int = 1) -> PromotionBlockerReport:
        minimum_launch_samples = _positive_integer(
            minimum_launch_samples, name='minimum_launch_samples')
        minimum_down_samples = _positive_integer(minimum_down_samples,
                                                 name='minimum_down_samples')
        with orm.Session(self._database()) as session, session.begin():
            row = session.execute(
                sqlalchemy.select(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    service_name).with_for_update()).mappings().first()
            if row is None or row['hash'] != service_hash:
                raise kernel_actions.ClaimLost(
                    'Service incarnation changed before promotion audit.')
            mode = self._mode_record(row)
            if (mode.mode is not actions.ResourceActionMode.SHADOW or
                    mode.changed_at is None):
                raise kernel_actions.StaleRevision(
                    'Promotion audit requires an active shadow window.')
            return self._promotion_report_in_session(session,
                                                     service_name,
                                                     service_hash,
                                                     mode.changed_at,
                                                     minimum_launch_samples,
                                                     minimum_down_samples,
                                                     lock_rows=True)

    def transition_service_mode_in_session(
        self,
        session: orm.Session,
        service_name: str,
        expected_service_hash: str,
        expected_controller_owner: tuple[int | None, str | None],
        expected_mode: actions.ResourceActionMode,
        new_mode: actions.ResourceActionMode,
        *,
        gate_evidence: ActivationGateEvidenceV1,
        expected_lifecycle_epoch: int,
        minimum_window: datetime.timedelta = _MINIMUM_PROMOTION_WINDOW,
        minimum_launch_samples: int = 1,
        minimum_down_samples: int = 1,
    ) -> ServiceModeTransition:
        """Perform a fenced monotonic mode transition under the service lock."""
        self._require_session(session)
        name = _bounded_text(service_name,
                             name='service_name',
                             maximum_bytes=256)
        parsed_hash = _canonical_uuid(expected_service_hash,
                                      name='expected_service_hash')
        if str(parsed_hash) != expected_service_hash:
            raise ValueError(
                'expected_service_hash must be canonical UUID text.')
        if (not isinstance(expected_controller_owner, tuple) or
                len(expected_controller_owner) != 2):
            raise TypeError('expected_controller_owner must be (pid, ip).')
        lifecycle_epoch = _positive_integer(expected_lifecycle_epoch,
                                            name='expected_lifecycle_epoch')
        old_mode = (expected_mode
                    if isinstance(expected_mode, actions.ResourceActionMode)
                    else actions.ResourceActionMode(expected_mode))
        target_mode = (new_mode
                       if isinstance(new_mode, actions.ResourceActionMode) else
                       actions.ResourceActionMode(new_mode))
        if (old_mode, target_mode) not in (
            (actions.ResourceActionMode.LEGACY,
             actions.ResourceActionMode.SHADOW),
            (actions.ResourceActionMode.SHADOW,
             actions.ResourceActionMode.AUTHORITATIVE),
        ):
            raise ValueError(
                'Resource-action mode transition is not monotonic.')
        if not isinstance(gate_evidence, ActivationGateEvidenceV1):
            raise TypeError('gate_evidence has an invalid type.')
        if (gate_evidence.service_name != name or
                gate_evidence.service_hash != expected_service_hash or
                gate_evidence.lifecycle_epoch != lifecycle_epoch):
            raise kernel_actions.ActionConflict(
                'Activation evidence belongs to another service fence.')
        if not gate_evidence.shadow_ready:
            raise kernel_actions.ActionConflict(
                'Drain/image/head/handler inventory is not ready for shadow.')
        if (target_mode is actions.ResourceActionMode.AUTHORITATIVE and
                not gate_evidence.authority_ready):
            raise kernel_actions.ActionConflict(
                'Profile/coverage/crash evidence is not authority-ready.')
        row = session.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                name).with_for_update()).mappings().first()
        if row is None:
            raise kernel_actions.ClaimLost(
                f'Service {name!r} no longer exists.')
        if (row['hash'] != expected_service_hash or
            (row['controller_pid'], row['controller_ip'])
                != expected_controller_owner or
                row['lifecycle_epoch'] != lifecycle_epoch):
            raise kernel_actions.ClaimLost(
                'Service hash/owner/lifecycle fence no longer matches.')
        current = self._mode_record(row)
        database_now = session.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        _validate_activation_evidence_time(gate_evidence, database_now)
        if target_mode is actions.ResourceActionMode.SHADOW:
            if gate_evidence.api_schema_revision != '005':
                raise kernel_actions.ActionConflict(
                    'Legacy-to-shadow activation requires API revision 005.')
            if gate_evidence.candidate_since is not None:
                raise kernel_actions.ActionConflict(
                    'Legacy-to-shadow evidence must not bind a candidate '
                    'window.')
        else:
            if gate_evidence.api_schema_revision != '006':
                raise kernel_actions.ActionConflict(
                    'Shadow-to-authority activation requires API revision '
                    '006.')
            if (current.mode is old_mode and
                (current.changed_at is None or
                 gate_evidence.candidate_since != current.changed_at)):
                raise kernel_actions.ActionConflict(
                    'Authority evidence does not bind the current shadow '
                    'window.')
        if (current.mode is target_mode and
                target_mode is actions.ResourceActionMode.SHADOW):
            return ServiceModeTransition(current, adopted=True)
        if current.mode is not old_mode:
            raise kernel_actions.StaleRevision(
                f'Service mode is {current.mode.value}, expected '
                f'{old_mode.value}.')
        report = None
        if target_mode is actions.ResourceActionMode.AUTHORITATIVE:
            if (not isinstance(minimum_window, datetime.timedelta) or
                    minimum_window < _MINIMUM_PROMOTION_WINDOW):
                raise ValueError('minimum_window cannot be less than 24 hours.')
            minimum_launch_samples = _positive_integer(
                minimum_launch_samples, name='minimum_launch_samples')
            minimum_down_samples = _positive_integer(
                minimum_down_samples, name='minimum_down_samples')
            if current.changed_at is None:
                raise kernel_actions.InvariantViolation(
                    'Shadow mode lacks its candidate-window timestamp.')
            if database_now - current.changed_at < minimum_window:
                raise kernel_actions.ActionConflict(
                    'Shadow candidate window has not reached its minimum age.')
            report = self._promotion_report_in_session(session,
                                                       name,
                                                       expected_service_hash,
                                                       current.changed_at,
                                                       minimum_launch_samples,
                                                       minimum_down_samples,
                                                       lock_rows=True)
            if not report.clean:
                raise kernel_actions.ActionConflict(
                    f'Shadow promotion is blocked: {report.reasons[0]}')
            if (report.coverage_inventory_sha256
                    != gate_evidence.coverage_inventory_sha256):
                raise kernel_actions.ActionConflict(
                    'Authority evidence coverage inventory hash does not '
                    'match the locked candidate graph.')
            database_now = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            _validate_activation_evidence_time(gate_evidence, database_now)
        now = sqlalchemy.func.clock_timestamp()
        evidence_expires_at = (gate_evidence.verified_at +
                               _MAX_ACTIVATION_EVIDENCE_AGE)
        updated = session.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name == name,
                serve_state_schema.services_table.c.hash ==
                expected_service_hash,
                serve_state_schema.services_table.c.resource_action_mode ==
                old_mode.value,
                sqlalchemy.func.clock_timestamp() >= gate_evidence.verified_at,
                sqlalchemy.func.clock_timestamp()
                <= evidence_expires_at).values(
                    resource_action_mode=target_mode.value,
                    resource_action_mode_changed_at=now))
        if updated.rowcount != 1:
            final_database_now = session.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            _validate_activation_evidence_time(gate_evidence,
                                               final_database_now)
            raise kernel_actions.ClaimLost(
                'Service mode fence changed during transition.')
        updated_row = session.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                name).with_for_update()).mappings().one()
        return ServiceModeTransition(self._mode_record(updated_row),
                                     promotion_report=report)

    def transition_service_mode(
        self,
        service_name: str,
        expected_service_hash: str,
        expected_controller_owner: tuple[int | None, str | None],
        expected_mode: actions.ResourceActionMode,
        new_mode: actions.ResourceActionMode,
        *,
        gate_evidence: ActivationGateEvidenceV1,
        expected_lifecycle_epoch: int,
        minimum_window: datetime.timedelta = _MINIMUM_PROMOTION_WINDOW,
        minimum_launch_samples: int = 1,
        minimum_down_samples: int = 1,
    ) -> ServiceModeTransition:
        with orm.Session(self._database()) as session, session.begin():
            return self.transition_service_mode_in_session(
                session,
                service_name,
                expected_service_hash,
                expected_controller_owner,
                expected_mode,
                new_mode,
                gate_evidence=gate_evidence,
                expected_lifecycle_epoch=expected_lifecycle_epoch,
                minimum_window=minimum_window,
                minimum_launch_samples=minimum_launch_samples,
                minimum_down_samples=minimum_down_samples)

    def _purge_retention_candidate_in_session(
        self,
        session: orm.Session,
        decision_id: uuid.UUID,
        cutoff: datetime.datetime,
    ) -> str:
        """Apply the Serve033 per-decision lock/release/delete protocol."""
        self._require_session(session)
        coverage_hint = session.execute(
            sqlalchemy.select(
                state_schema.SHADOW_COVERAGE.c.service_name,
                state_schema.SHADOW_COVERAGE.c.service_hash,
                state_schema.SHADOW_COVERAGE.c.worker_cohort_ref_id).where(
                    state_schema.SHADOW_COVERAGE.c.decision_id ==
                    decision_id)).mappings().first()
        if coverage_hint is None:
            return 'deferred'
        service_name = _bounded_text(coverage_hint['service_name'],
                                     name='coverage.service_name',
                                     maximum_bytes=256)
        service_hash = str(
            _canonical_uuid(coverage_hint['service_hash'],
                            name='coverage.service_hash'))

        service_row = session.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                service_name).with_for_update()).mappings().first()
        if service_row is None or service_row['hash'] != service_hash:
            # Deleted incarnations need a future durable tombstone protocol;
            # a same-name successor must never authorize their reclamation.
            return 'deferred'
        mode = self._mode_record(service_row)

        replica_rows = session.execute(
            sqlalchemy.select(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name ==
                service_name).order_by(
                    serve_state_schema.replicas_table.c.replica_id).
            with_for_update()).mappings().all()
        cleanup_rows = session.execute(
            sqlalchemy.select(
                serve_state_schema.ephemeral_storage_cleanup_intents_table).
            where(
                serve_state_schema.ephemeral_storage_cleanup_intents_table.c.
                service_name == service_name).order_by(
                    serve_state_schema.ephemeral_storage_cleanup_intents_table.
                    c.resource_scope,
                    serve_state_schema.ephemeral_storage_cleanup_intents_table.
                    c.storage_generation).with_for_update()).mappings().all()

        reference_row = None
        cohort_row = None
        worker_reference_id = coverage_hint['worker_cohort_ref_id']
        if worker_reference_id is not None:
            parsed_reference_id = _canonical_uuid(
                worker_reference_id, name='coverage.worker_cohort_ref_id')
            reference_hint = session.execute(
                sqlalchemy.select(
                    state_schema.WORKER_COHORT_REFS.c.cohort_id).where(
                        state_schema.WORKER_COHORT_REFS.c.decision_id ==
                        parsed_reference_id)).first()
            if reference_hint is not None:
                cohort_row = self._locked_worker_cohort(session,
                                                        reference_hint[0])
                reference_row = self._locked_worker_cohort_reference(
                    session, parsed_reference_id)

        coverage_row = self._locked_shadow_coverage(session, decision_id)
        if coverage_row is None:
            return 'deferred'
        coverage = _shadow_coverage_record(coverage_row)
        coverage_attempts = self._locked_coverage_attempts(session, decision_id)
        parent_row = session.execute(
            sqlalchemy.select(state_schema.SHADOW_SAMPLES).where(
                state_schema.SHADOW_SAMPLES.c.would_be_action_id == decision_id
            ).with_for_update(skip_locked=True)).mappings().first()
        sample = None if parent_row is None else _sample_record(parent_row)
        represented_attempts = ([] if sample is None else self._locked_attempts(
            session, sample))

        if (coverage.service_name != service_name or
                coverage.service_hash != service_hash):
            raise kernel_actions.InvariantViolation(
                'Retention coverage changed after immutable candidate '
                'discovery.')
        replica_references = {
            _canonical_uuid(value, name='replica.shadow_evidence_id')
            for row in replica_rows
            for value in (row['launch_shadow_coverage_id'],
                          row['down_shadow_coverage_id'],
                          row['launch_shadow_sample_id'],
                          row['down_shadow_sample_id'])
            if value is not None
        }
        if decision_id in replica_references or cleanup_rows:
            return 'protected'
        admitted_at = _canonical_timestamp_datetime(coverage.admitted_at,
                                                    name='coverage.admitted_at')
        if mode.mode is actions.ResourceActionMode.SHADOW:
            if (coverage.normalization_outcome
                    is actions.NormalizationOutcome.NOT_REPRESENTABLE or
                    mode.changed_at is None or admitted_at >= mode.changed_at):
                return 'protected'

        represented = (coverage.normalization_outcome
                       is actions.NormalizationOutcome.REPRESENTABLE)
        if represented and sample is None:
            return 'deferred'
        if (coverage.worker_cohort_ref_id is None or reference_row is None or
                cohort_row is None):
            return 'protected'
        cohort = _worker_cohort_record(cohort_row)
        reference = _worker_cohort_reference_record(reference_row)
        if cohort.cohort_id != reference.cohort_id:
            raise kernel_actions.InvariantViolation(
                'Retention cohort reference has a mismatched cohort row.')
        try:
            reference.reference.validate_coverage(coverage)
        except ValueError as e:
            raise kernel_actions.InvariantViolation(
                f'Retention coverage reference is invalid: {e}') from e
        owner_fence = (None if service_row['controller_pid'] is None or
                       service_row['controller_ip'] is None else
                       f'{service_row["controller_pid"]}:'
                       f'{service_row["controller_ip"]}')
        if (owner_fence != reference.reference.controller_owner_fence or
                service_row['lifecycle_epoch']
                != reference.reference.lifecycle_epoch):
            return 'deferred'
        if reference.reference_state in (
                actions.WorkerCohortReferenceState.PREPARING,
                actions.WorkerCohortReferenceState.ACTION_ACTIVE):
            return 'protected'

        if represented:
            assert sample is not None
            if coverage_attempts:
                raise kernel_actions.InvariantViolation(
                    'Represented retention evidence has coverage-only '
                    'attempts.')
            if (sample.action_id != coverage.decision_id or
                    sample.service_name != coverage.service_name or
                    sample.provider_plan.resource_identity
                    != coverage.identity.resource_identity or
                    sample.action_kind is not coverage.action_type or
                    sample.created_at != admitted_at):
                raise kernel_actions.InvariantViolation(
                    'Represented retention parent differs from coverage.')
            if sample.completed_at is None or sample.completed_at >= cutoff:
                return 'protected'
            if sample.phase not in (
                    actions.ShadowParentPhase.COMPLETE,
                    actions.ShadowParentPhase.ABANDONED_PRE_SUBMIT):
                return 'protected'
            if any(attempt.phase not in (
                    actions.ShadowAttemptPhase.COMPLETE,
                    actions.ShadowAttemptPhase.ABANDONED_PRE_SUBMIT)
                   for attempt in represented_attempts):
                return 'protected'
            graph_problems = _validate_child_graph(sample,
                                                   represented_attempts,
                                                   require_closed=True)
            if not represented_attempts or graph_problems:
                reason = ('missing_attempt'
                          if not represented_attempts else graph_problems[0])
                raise kernel_actions.InvariantViolation(
                    'Cannot retain-delete invalid represented evidence: '
                    f'{decision_id}:{reason}.')
        else:
            if sample is not None:
                raise kernel_actions.InvariantViolation(
                    'Coverage-only retention evidence unexpectedly has a '
                    'represented parent.')
            if not coverage_attempts:
                raise kernel_actions.InvariantViolation(
                    'Cannot retain-delete coverage-only evidence without an '
                    'attempt.')
            if any(attempt.phase not in (
                    actions.CoverageAttemptPhase.COMPLETE,
                    actions.CoverageAttemptPhase.ABANDONED_PRE_SUBMIT)
                   for attempt in coverage_attempts):
                return 'protected'
            graph_problems = _validate_terminal_coverage_attempt_graph(
                coverage, coverage_attempts)
            if graph_problems:
                raise kernel_actions.InvariantViolation(
                    'Cannot retain-delete invalid coverage-only evidence: '
                    f'{decision_id}:{graph_problems[0]}.')
            completed_values = [(None if attempt.completed_at is None else
                                 _canonical_timestamp_datetime(
                                     attempt.completed_at,
                                     name='coverage_attempt.completed_at'))
                                for attempt in coverage_attempts]
            if (any(value is None for value in completed_values) or max(
                    value for value in completed_values if value is not None)
                    >= cutoff):
                return 'protected'

        if (reference.reference_state
                is actions.WorkerCohortReferenceState.SHADOW_ACTIVE):
            released = session.execute(
                sqlalchemy.update(state_schema.WORKER_COHORT_REFS).where(
                    state_schema.WORKER_COHORT_REFS.c.decision_id ==
                    decision_id,
                    state_schema.WORKER_COHORT_REFS.c.reference_state ==
                    actions.WorkerCohortReferenceState.SHADOW_ACTIVE.value,
                    state_schema.WORKER_COHORT_REFS.c.revision ==
                    reference.revision).values(
                        reference_state=actions.WorkerCohortReferenceState.
                        RELEASED.value,
                        revision=reference.revision + 1,
                        released_at=sqlalchemy.func.clock_timestamp()))
            if released.rowcount != 1:
                raise kernel_actions.ClaimLost(
                    'Retention lost the exact cohort-reference release fence.')

        if represented:
            deleted_children = session.execute(
                sqlalchemy.delete(state_schema.SHADOW_ATTEMPTS).where(
                    state_schema.SHADOW_ATTEMPTS.c.would_be_action_id ==
                    decision_id))
            if deleted_children.rowcount != len(represented_attempts):
                raise kernel_actions.ClaimLost(
                    'Represented children changed during retention deletion.')
            deleted_parent = session.execute(
                sqlalchemy.delete(state_schema.SHADOW_SAMPLES).where(
                    state_schema.SHADOW_SAMPLES.c.would_be_action_id ==
                    decision_id))
            if deleted_parent.rowcount != 1:
                raise kernel_actions.ClaimLost(
                    'Represented parent changed during retention deletion.')
        else:
            deleted_attempts = session.execute(
                sqlalchemy.delete(state_schema.SHADOW_COVERAGE_ATTEMPTS).where(
                    state_schema.SHADOW_COVERAGE_ATTEMPTS.c.decision_id ==
                    decision_id))
            if deleted_attempts.rowcount != len(coverage_attempts):
                raise kernel_actions.ClaimLost(
                    'Coverage attempts changed during retention deletion.')
        deleted_coverage = session.execute(
            sqlalchemy.delete(state_schema.SHADOW_COVERAGE).where(
                state_schema.SHADOW_COVERAGE.c.decision_id == decision_id))
        if deleted_coverage.rowcount != 1:
            raise kernel_actions.ClaimLost(
                'Coverage changed during retention deletion.')
        return 'removed'

    def purge_completed_before(self,
                               cutoff: datetime.datetime,
                               limit: int = 100) -> ShadowRetentionResult:
        """Reclaim represented and coverage-only terminal Serve033 evidence."""
        cutoff = _timestamp(cutoff, name='cutoff')
        limit = _positive_integer(limit, name='limit')
        coverage_table = state_schema.SHADOW_COVERAGE
        coverage_attempt_table = state_schema.SHADOW_COVERAGE_ATTEMPTS
        parent_table = state_schema.SHADOW_SAMPLES
        with orm.Session(self._database()) as discovery_session:
            represented_rows = discovery_session.execute(
                sqlalchemy.select(parent_table.c.would_be_action_id,
                                  parent_table.c.completed_at).where(
                                      parent_table.c.completed_at.is_not(None),
                                      parent_table.c.completed_at < cutoff).
                order_by(parent_table.c.completed_at,
                         parent_table.c.would_be_action_id).limit(limit)).all()
            coverage_completed_at = sqlalchemy.func.max(
                coverage_attempt_table.c.completed_at)
            coverage_only_rows = discovery_session.execute(
                sqlalchemy.select(
                    coverage_table.c.decision_id,
                    coverage_completed_at.label('completed_at')).select_from(
                        coverage_table.join(
                            coverage_attempt_table,
                            coverage_attempt_table.c.decision_id ==
                            coverage_table.c.decision_id).outerjoin(
                                parent_table, parent_table.c.would_be_action_id
                                == coverage_table.c.decision_id)
                    ).where(
                        parent_table.c.would_be_action_id.is_(None),
                        coverage_table.c.normalization_outcome ==
                        actions.NormalizationOutcome.NOT_REPRESENTABLE.value).
                group_by(coverage_table.c.decision_id).having(
                    sqlalchemy.func.bool_and(
                        coverage_attempt_table.c.phase.in_(
                            (actions.CoverageAttemptPhase.COMPLETE.value,
                             actions.CoverageAttemptPhase.ABANDONED_PRE_SUBMIT.
                             value))), coverage_completed_at.is_not(None),
                    coverage_completed_at < cutoff).order_by(
                        coverage_completed_at,
                        coverage_table.c.decision_id).limit(limit)).all()

        candidates: dict[uuid.UUID, datetime.datetime] = {}
        for raw_id, raw_completed_at in (*represented_rows,
                                         *coverage_only_rows):
            candidate_id = _canonical_uuid(raw_id, name='retention.decision_id')
            completed_at = _timestamp(raw_completed_at,
                                      name='retention.completed_at')
            previous = candidates.get(candidate_id)
            if previous is None or completed_at < previous:
                candidates[candidate_id] = completed_at
        ordered_candidates = sorted(candidates,
                                    key=lambda value:
                                    (candidates[value], value.bytes))[:limit]
        removed: list[uuid.UUID] = []
        protected: list[uuid.UUID] = []
        deferred: list[uuid.UUID] = []
        for decision_id in ordered_candidates:
            with orm.Session(self._database()) as session, session.begin():
                outcome = self._purge_retention_candidate_in_session(
                    session, decision_id, cutoff)
            if outcome == 'removed':
                removed.append(decision_id)
            elif outcome == 'protected':
                protected.append(decision_id)
            else:
                assert outcome == 'deferred'
                deferred.append(decision_id)
        return ShadowRetentionResult(tuple(removed), tuple(protected),
                                     tuple(deferred))
