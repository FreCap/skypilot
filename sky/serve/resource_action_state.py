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
class PromotionBlockerReport:
    """Typed evidence for one service's current shadow candidate window."""

    service_name: str
    service_hash: str
    candidate_since: datetime.datetime
    candidate_sample_count: int
    clean_launch_samples: int
    clean_down_samples: int
    blocking_sample_ids: tuple[uuid.UUID, ...]
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
        if self.api_schema_revision != '005':
            raise ValueError('activation requires API schema revision 005.')
        if self.serve_schema_revision != '032':
            raise ValueError('activation requires Serve schema revision 032.')
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
        return row

    def admit_in_session(
        self,
        session: orm.Session,
        new_sample: NewShadowSample,
        expected_controller_owner: tuple[int | None, str | None],
        expected_lifecycle_epoch: int,
    ) -> ShadowSampleRecord:
        """Insert or exactly adopt a sample in the caller's transaction."""
        self._require_session(session)
        if not isinstance(new_sample, NewShadowSample):
            raise TypeError('new_sample has an invalid type.')
        self._locked_shadow_service(session, new_sample,
                                    expected_controller_owner,
                                    expected_lifecycle_epoch)
        plan = new_sample.provider_plan
        identity = plan.resource_identity
        now = sqlalchemy.func.clock_timestamp()
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

    def admit(
        self,
        new_sample: NewShadowSample,
        expected_controller_owner: tuple[int | None, str | None],
        expected_lifecycle_epoch: int,
    ) -> ShadowSampleRecord:
        with orm.Session(self._database()) as session, session.begin():
            return self.admit_in_session(session, new_sample,
                                         expected_controller_owner,
                                         expected_lifecycle_epoch)

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
        owner = session.execute(
            sqlalchemy.select(
                state_schema.SHADOW_ATTEMPTS.c.would_be_action_id,
                state_schema.SHADOW_ATTEMPTS.c.request_sequence).where(
                    state_schema.SHADOW_ATTEMPTS.c.legacy_request_id ==
                    request_id)).first()
        if owner is not None and (owner[0] != parsed or owner[1] != sequence):
            raise kernel_actions.ActionConflict(
                f'Request {request_id} already belongs to another shadow '
                'attempt.')
        sample, attempt = self._lock_parent_and_attempt(session, parsed,
                                                        sequence)
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
        statement = sqlalchemy.select(state_schema.SHADOW_SAMPLES).where(
            state_schema.SHADOW_SAMPLES.c.service_name == service_name,
            state_schema.SHADOW_SAMPLES.c.service_hash == service_hash,
            state_schema.SHADOW_SAMPLES.c.created_at
            >= candidate_since).order_by(
                state_schema.SHADOW_SAMPLES.c.would_be_action_id)
        if lock_rows:
            statement = statement.with_for_update()
        rows = session.execute(statement).mappings().all()
        samples = [_sample_record(row) for row in rows]
        ids = [sample.action_id for sample in samples]
        attempt_rows: list[Mapping[str, Any]] = []
        if ids:
            child_statement = sqlalchemy.select(
                state_schema.SHADOW_ATTEMPTS).where(
                    state_schema.SHADOW_ATTEMPTS.c.would_be_action_id.in_(
                        ids)).order_by(
                            state_schema.SHADOW_ATTEMPTS.c.would_be_action_id,
                            state_schema.SHADOW_ATTEMPTS.c.request_sequence)
            if lock_rows:
                child_statement = child_statement.with_for_update()
            attempt_rows = list(
                session.execute(child_statement).mappings().all())
        attempts_by_id: dict[uuid.UUID, list[ShadowAttemptRecord]] = {
            action_id: [] for action_id in ids
        }
        sample_by_id = {sample.action_id: sample for sample in samples}
        for row in attempt_rows:
            action_id = _canonical_uuid(row['would_be_action_id'],
                                        name='would_be_action_id')
            attempts_by_id[action_id].append(
                _attempt_record(row, sample_by_id[action_id]))
        reasons: list[str] = []
        blocking: list[uuid.UUID] = []
        clean_launch = 0
        clean_down = 0
        for sample in samples:
            sample_reasons: list[str] = []
            attempts = attempts_by_id[sample.action_id]
            if sample.phase is not actions.ShadowParentPhase.COMPLETE:
                sample_reasons.append(f'phase:{sample.phase.value}')
            if sample.parity_class is not actions.ShadowParityClass.MATCH:
                sample_reasons.append(f'parity:{sample.parity_class.value}')
            if sample.profile_eligibility is not actions.ProfileEligibility.ELIGIBLE:
                sample_reasons.append('profile:UNSUPPORTED')
            sample_reasons.extend(
                _validate_child_graph(sample, attempts, require_closed=True))
            if not attempts:
                sample_reasons.append('missing_attempt')
            for attempt in attempts:
                prefix = f'attempt:{attempt.request_sequence}'
                if attempt.phase is not actions.ShadowAttemptPhase.COMPLETE:
                    sample_reasons.append(
                        f'{prefix}:phase:{attempt.phase.value}')
                if (attempt.planned_execution_kind
                        is not actions.PlannedExecutionKind.API_REQUEST):
                    sample_reasons.append(f'{prefix}:direct_down')
                if attempt.legacy_request_id is None:
                    sample_reasons.append(f'{prefix}:request_unbound')
                if attempt.divergence_class is not None:
                    sample_reasons.append(
                        f'{prefix}:divergence:{attempt.divergence_class.value}')
            if (sample.parity_class is actions.ShadowParityClass.MATCH and
                    sample.legacy_projection is not None and
                    sample.proposed_projection is not None):
                try:
                    _validate_match_evidence(sample, attempts,
                                             sample.legacy_projection,
                                             sample.proposed_projection)
                except ValueError as e:
                    sample_reasons.append(f'match_evidence:{e}')
            if sample_reasons:
                blocking.append(sample.action_id)
                reasons.extend(f'sample:{sample.action_id}:{reason}'
                               for reason in sample_reasons)
            else:
                assert sample.legacy_projection is not None
                if (sample.legacy_projection.action_disposition
                        is actions.ServeActionDisposition.SUCCEEDED):
                    if sample.action_kind is kernel_actions.ActionKind.LAUNCH:
                        clean_launch += 1
                    else:
                        clean_down += 1
        if clean_launch < minimum_launch_samples:
            reasons.append(f'minimum_launch_samples:{clean_launch}/'
                           f'{minimum_launch_samples}')
        if clean_down < minimum_down_samples:
            reasons.append(
                f'minimum_down_samples:{clean_down}/{minimum_down_samples}')
        return PromotionBlockerReport(service_name=service_name,
                                      service_hash=service_hash,
                                      candidate_since=candidate_since,
                                      candidate_sample_count=len(samples),
                                      clean_launch_samples=clean_launch,
                                      clean_down_samples=clean_down,
                                      blocking_sample_ids=tuple(blocking),
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
            if gate_evidence.candidate_since is not None:
                raise kernel_actions.ActionConflict(
                    'Legacy-to-shadow evidence must not bind a candidate '
                    'window.')
        elif (current.mode is old_mode and
              (current.changed_at is None or
               gate_evidence.candidate_since != current.changed_at)):
            raise kernel_actions.ActionConflict(
                'Authority evidence does not bind the current shadow window.')
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

    def purge_completed_before(self,
                               cutoff: datetime.datetime,
                               limit: int = 100) -> ShadowRetentionResult:
        """Delete old terminal evidence after locking every earlier owner class."""
        cutoff = _timestamp(cutoff, name='cutoff')
        limit = _positive_integer(limit, name='limit')
        table = state_schema.SHADOW_SAMPLES
        with orm.Session(self._database()) as session, session.begin():
            candidate_rows = session.execute(
                sqlalchemy.select(
                    table.c.would_be_action_id, table.c.service_name).where(
                        table.c.completed_at.is_not(None),
                        table.c.completed_at < cutoff).order_by(
                            table.c.completed_at,
                            table.c.would_be_action_id).limit(limit)).all()
            if not candidate_rows:
                return ShadowRetentionResult((), ())
            ids = sorted((row[0] for row in candidate_rows), key=str)
            service_names = sorted({row[1] for row in candidate_rows})
            service_rows = session.execute(
                sqlalchemy.select(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name.in_(
                        service_names)).order_by(
                            serve_state_schema.services_table.c.name).
                with_for_update()).mappings().all()
            replica_rows = session.execute(
                sqlalchemy.select(
                    serve_state_schema.replicas_table.c.service_name,
                    serve_state_schema.replicas_table.c.replica_id,
                    serve_state_schema.replicas_table.c.launch_shadow_sample_id,
                    serve_state_schema.replicas_table.c.down_shadow_sample_id).
                where(
                    serve_state_schema.replicas_table.c.service_name.in_(
                        service_names)).order_by(
                            serve_state_schema.replicas_table.c.service_name,
                            serve_state_schema.replicas_table.c.replica_id).
                with_for_update()).mappings().all()
            cleanup_rows = session.execute(
                sqlalchemy.select(
                    serve_state_schema.ephemeral_storage_cleanup_intents_table.
                    c.service_name).
                where(
                    serve_state_schema.ephemeral_storage_cleanup_intents_table.
                    c.service_name.in_(service_names)).order_by(
                        serve_state_schema.
                        ephemeral_storage_cleanup_intents_table.c.service_name).
                with_for_update()).all()
            parent_rows = session.execute(
                sqlalchemy.select(table).where(
                    table.c.would_be_action_id.in_(ids)).order_by(
                        table.c.would_be_action_id).with_for_update(
                            skip_locked=True)).mappings().all()
            locked_ids = {
                _canonical_uuid(row['would_be_action_id'],
                                name='would_be_action_id')
                for row in parent_rows
            }
            deferred = [
                action_id for action_id in ids if action_id not in locked_ids
            ]
            references = {
                value for row in replica_rows
                for value in (row['launch_shadow_sample_id'],
                              row['down_shadow_sample_id']) if value is not None
            }
            cleanup_services = {row[0] for row in cleanup_rows}
            shadow_windows = {
                row['name']:
                    (row['hash'], row['resource_action_mode_changed_at'])
                for row in service_rows
                if row['resource_action_mode'] ==
                actions.ResourceActionMode.SHADOW.value and
                row['resource_action_mode_changed_at'] is not None
            }
            removed: list[uuid.UUID] = []
            protected: list[uuid.UUID] = []
            for row in parent_rows:
                sample = _sample_record(row)
                attempts = self._locked_attempts(session, sample)
                graph_problems = _validate_child_graph(sample,
                                                       attempts,
                                                       require_closed=True)
                if (not attempts or
                        any(attempt.phase not in _TERMINAL_ATTEMPT_PHASES
                            for attempt in attempts) or graph_problems):
                    reason = ('missing_attempt' if not attempts else (
                        'nonterminal_attempt' if any(
                            attempt.phase not in _TERMINAL_ATTEMPT_PHASES
                            for attempt in attempts) else graph_problems[0]))
                    raise kernel_actions.InvariantViolation(
                        'Cannot retain-delete invalid shadow evidence: '
                        f'{sample.action_id}:{reason}.')
                window = shadow_windows.get(sample.service_name)
                is_candidate = (window is not None and window[0]
                                == sample.resource_identity.service_hash and
                                sample.created_at >= window[1])
                if (sample.action_id in references or
                        sample.service_name in cleanup_services or
                        is_candidate):
                    protected.append(sample.action_id)
                    continue
                deleted = session.execute(
                    sqlalchemy.delete(table).where(
                        table.c.would_be_action_id == sample.action_id,
                        table.c.completed_at.is_not(None), table.c.completed_at
                        < cutoff))
                if deleted.rowcount == 1:
                    removed.append(sample.action_id)
            return ShadowRetentionResult(tuple(removed), tuple(protected),
                                         tuple(deferred))
