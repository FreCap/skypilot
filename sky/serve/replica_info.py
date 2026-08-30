"""Versioned state and behavior for one SkyServe replica."""
import dataclasses
import math
import re
import time
import typing
from typing import Any
import uuid

import colorama

from sky import backends
from sky import estimated_spend
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky.backends import backend_utils
from sky.serve import constants as serve_constants
from sky.serve import provider_phase
from sky.serve import replica_tls
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.serve import system_recovery_state
from sky.skylet import job_lib
from sky.utils import common_utils
from sky.utils import env_options
from sky.utils import resources_utils

logger = sky_logging.init_logger(__name__)

# Sentinel for to_info_dict's pre-fetched cluster_record parameters. None is a
# legitimate value meaning that the cluster row is absent.
_NOT_PROVIDED: Any = object()

# This tuple is the stable mutable recovery subdocument copied by the
# PostgreSQL row-locked patch path. The immutable record identity is validated
# separately and is never copied from one in-memory record to another.
SYSTEM_RECOVERY_STORAGE_FIELDS = (
    'system_recovery_launch_intent',
    'system_recovery_disposition',
    'launch_request_id',
    'service_job_id',
    'candidate_ready_observed_at',
    'ordinary_release_not_before',
    'system_recovery_revision',
    'system_recovery',
    'system_recovery_quarantine',
)
# Exact set added by v13. Every v13 row must contain all ten keys; any missing
# key, including a completely absent bundle, is quarantined.
V13_ADDITIVE_STORAGE_FIELDS = ('replica_record_id',
                               *SYSTEM_RECOVERY_STORAGE_FIELDS)
_REPLICA_INFO_VERSION = 18
V17_COLLISION_OPTIONAL_STORAGE_FIELDS = (
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
    'zero_cost_admission_sequence',
    'zero_cost_materialization_sequence',
)
_SHA256_PATTERN = re.compile(r'^[0-9a-f]{64}$')

# A fixed namespace makes the v12 transition identity
# reproducible across processes and across JSON/pickle readers.  New v13 rows
# never use this namespace: they receive an independent random UUID4.
_REPLICA_RECORD_ID_TRANSITION_NAMESPACE = uuid.UUID(
    '3b448973-9e2f-58aa-a640-27fb7c6a8884')


def _canonical_replica_record_id(value: object) -> str:
    if not isinstance(value, str):
        raise system_recovery_state.RecoveryStateError(
            'replica_record_id must be a canonical UUID string.')
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as e:
        raise system_recovery_state.RecoveryStateError(
            'replica_record_id must be a canonical UUID string.') from e
    if str(parsed) != value:
        raise system_recovery_state.RecoveryStateError(
            'replica_record_id must be a canonical UUID string.')
    return value


def _derive_transition_replica_record_id(replica: Any) -> str:
    persisted = vars(replica)
    replica_id = persisted.get('replica_id')
    cluster_name = persisted.get('cluster_name')
    created_at = persisted.get('created_at')
    if (isinstance(replica_id, bool) or not isinstance(replica_id, int) or
            replica_id < 0):
        raise system_recovery_state.RecoveryStateError(
            'Transition replica identity requires a nonnegative replica ID.')
    if not isinstance(cluster_name, str) or not cluster_name:
        raise system_recovery_state.RecoveryStateError(
            'Transition replica identity requires a nonempty cluster name.')
    if created_at is None:
        created_at_token = 'none'
    elif (isinstance(created_at, bool) or
          not isinstance(created_at, (int, float))):
        raise system_recovery_state.RecoveryStateError(
            'Transition replica identity requires a finite creation timestamp.')
    else:
        try:
            normalized_created_at = float(created_at)
        except (OverflowError, TypeError, ValueError) as e:
            raise system_recovery_state.RecoveryStateError(
                'Transition replica identity requires a finite creation '
                'timestamp.') from e
        if not math.isfinite(normalized_created_at):
            raise system_recovery_state.RecoveryStateError(
                'Transition replica identity requires a finite creation '
                'timestamp.')
        created_at_token = normalized_created_at.hex()
    identity_material = (f'{replica_id}:{len(cluster_name)}:{cluster_name}:'
                         f'{created_at_token}')
    return str(
        uuid.uuid5(_REPLICA_RECORD_ID_TRANSITION_NAMESPACE, identity_material))


def _set_transition_replica_record_id(replica: Any) -> None:
    replica.replica_record_id = _derive_transition_replica_record_id(replica)


def _ensure_replica_record_id(replica: Any) -> None:
    try:
        _canonical_replica_record_id(vars(replica).get('replica_record_id'))
    except system_recovery_state.RecoveryStateError:
        _set_transition_replica_record_id(replica)


def _positive_timestamp(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise system_recovery_state.RecoveryStateError(
            f'{name} must be a positive finite timestamp.')
    try:
        timestamp = float(value)
    except (OverflowError, TypeError, ValueError) as e:
        raise system_recovery_state.RecoveryStateError(
            f'{name} must be a positive finite timestamp.') from e
    if not math.isfinite(timestamp) or timestamp <= 0:
        raise system_recovery_state.RecoveryStateError(
            f'{name} must be a positive finite timestamp.')
    return timestamp


def _optional_positive_timestamp(value: object, name: str) -> float | None:
    if value is None:
        return None
    return _positive_timestamp(value, name)


def _is_valid_drain_started_at(value: Any) -> bool:
    return (not isinstance(value, bool) and isinstance(value, (int, float)) and
            math.isfinite(value) and value > 0)


@dataclasses.dataclass
class ReplicaStatusProperty:
    """Some properties that determine replica status.

    Attributes:
        sky_launch_status: Process status of sky.launch.
        user_app_failed: Whether the service job failed.
        service_ready_now: Latest readiness probe result.
        first_ready_time: The first time the service is ready.
        sky_down_status: Process status of sky.down.
    """
    # sky.launch will always be scheduled on creation of ReplicaStatusProperty.
    sky_launch_status: common_utils.ProcessStatus = (
        common_utils.ProcessStatus.SCHEDULED)
    user_app_failed: bool = False
    service_ready_now: bool = False
    # None means readiness probe is not succeeded yet;
    # -1 means the initial delay seconds is exceeded.
    first_ready_time: float | None = None
    # None means sky.down is not called yet.
    sky_down_status: common_utils.ProcessStatus | None = None
    # Whether the termination is caused by autoscaler's decision
    is_scale_down: bool = False
    # The replica's underlying capacity was interrupted. This includes spot
    # preemption and reclamation of low-priority zero-cost Kubernetes pods.
    preempted: bool = False
    # Whether the replica is purged.
    purged: bool = False
    # Whether the replica failed to launch due to spot availability.
    # This is only possible when spot placer is enabled, so the retry until up
    # is set to True and it can fail immediately due to spot availability.
    failed_spot_availability: bool = False
    # [boltz fork] The graceful-drain cap resolved when this replica's
    # retirement was scheduled, persisted so a recovery re-drive reuses
    # it exactly instead of re-resolving (the spec lookup can fail after
    # a crash and silently substitute the 120s default). None on purge
    # and failure teardowns, and on rows written before this field existed.
    # Legacy decoders materialize that missing field explicitly.
    drain_cap_seconds: int | None = None
    # Wall-clock epoch seconds at which the bounded drain first became
    # durable. Unlike time.monotonic(), this survives controller restarts and
    # prevents repeated recovery from restarting the full drain cap. None for
    # unbounded/immediate cleanup and rows written before this field existed.
    drain_started_at: float | None = None
    # Economic replacement is fail-closed: persist the off-route retirement
    # intent, but do not admit sky.down until a fresh LB report proves zero
    # occupancy. Legacy decoders materialize False for rows predating this
    # field.
    wait_for_idle_before_termination: bool = False
    # Logical autoscaling retirement fence. None on physical services and
    # destructive purge/failure cleanup.
    logical_retirement_version: int | None = None
    logical_retirement_controller_epoch: str | None = None
    logical_retirement_generation: int | None = None
    logical_retirement_target_capacity: int | None = None
    logical_retirement_confirmed_generation: int | None = None
    # True only after an outdated backend consumed the full configured drain
    # deadline without an explicit idle proof and replacement capacity was
    # revalidated.  Persisted so down-thread admission can distinguish that
    # bounded rolling-update completion from an ordinary idle confirmation.
    logical_retirement_bounded_deadline: bool = False
    # Persisted at down admission immediately before the worker starts. A
    # SCHEDULED row without this bit is queued but unadmitted and may still be
    # safely aborted after a controller restart. RUNNING/FAILED rows predate
    # the bit but are already unambiguously committed cleanup.
    logical_retirement_committed: bool | None = False

    def unrecoverable_failure(self) -> bool:
        """Whether the replica fails and cannot be recovered.

        Autoscaler should stop scaling if any of the replica has unrecoverable
        failure, e.g., the user app fails before the service endpoint being
        ready for the current version.
        """
        replica_status = self.to_replica_status()
        if replica_status not in serve_state.ReplicaStatus.terminal_statuses():
            return False
        if self.first_ready_time is not None:
            if self.first_ready_time >= 0:
                # If the service is ever up, we assume there is no bug in the
                # user code and the scale down is successful, thus enabling the
                # controller to remove the replica from the replica table and
                # auto restart the replica.
                # For replica with a failed sky.launch, it is likely due to some
                # misconfigured resources, so we don't want to auto restart it.
                # For replica with a failed sky.down, we cannot restart it since
                # otherwise we will have a resource leak.
                return False
            else:
                # If the initial delay exceeded, it is likely the service is not
                # recoverable.
                return True
        if self.user_app_failed:
            return True
        # TODO(zhwu): launch failures not related to resource unavailability
        # should be considered as unrecoverable failure. (refer to
        # `spot.recovery_strategy.StrategyExecutor::_launch`)
        return False

    def should_track_service_status(self) -> bool:
        """Should we track the status of the replica.

        This includes:
            (1) Job status;
            (2) Readiness probe.
        """
        if self.sky_launch_status != common_utils.ProcessStatus.SUCCEEDED:
            return False
        if self.sky_down_status is not None:
            return False
        if self.user_app_failed:
            return False
        if self.preempted:
            return False
        if self.purged:
            return False
        return True

    def to_replica_status(self) -> serve_state.ReplicaStatus:
        """Convert status property to human-readable replica status."""
        # Backward compatibility. Before we introduce ProcessStatus.SCHEDULED,
        # we use None to represent sky.launch is not called yet.
        if (self.sky_launch_status is None or
                self.sky_launch_status == common_utils.ProcessStatus.SCHEDULED):
            # Pending to launch
            return serve_state.ReplicaStatus.PENDING
        if self.sky_launch_status == common_utils.ProcessStatus.RUNNING:
            if self.sky_down_status == common_utils.ProcessStatus.FAILED:
                return serve_state.ReplicaStatus.FAILED_CLEANUP
            if self.sky_down_status == common_utils.ProcessStatus.SUCCEEDED:
                # This indicate it is a scale_down with correct teardown.
                # Should have been cleaned from the replica table.
                return serve_state.ReplicaStatus.UNKNOWN
            # Still launching
            return serve_state.ReplicaStatus.PROVISIONING
        if self.sky_launch_status == common_utils.ProcessStatus.INTERRUPTED:
            # sky.down is running and a scale down interrupted sky.launch
            return serve_state.ReplicaStatus.SHUTTING_DOWN
        if self.sky_down_status is not None:
            if self.preempted:
                # The replica's underlying capacity was interrupted.
                return serve_state.ReplicaStatus.PREEMPTED
            if self.sky_down_status == common_utils.ProcessStatus.SCHEDULED:
                # sky.down is scheduled to run, but not started yet.
                return serve_state.ReplicaStatus.SHUTTING_DOWN
            if self.sky_down_status == common_utils.ProcessStatus.RUNNING:
                # sky.down is running
                return serve_state.ReplicaStatus.SHUTTING_DOWN
            if self.sky_down_status == common_utils.ProcessStatus.FAILED:
                # sky.down failed
                return serve_state.ReplicaStatus.FAILED_CLEANUP
            if self.user_app_failed:
                # Failed on user setup/run
                return serve_state.ReplicaStatus.FAILED
            if self.sky_launch_status == common_utils.ProcessStatus.FAILED:
                # sky.launch failed
                return serve_state.ReplicaStatus.FAILED_PROVISION
            if self.first_ready_time is None:
                # readiness probe is not executed yet, but a scale down is
                # triggered.
                return serve_state.ReplicaStatus.SHUTTING_DOWN
            if self.first_ready_time == -1:
                # initial delay seconds exceeded
                return serve_state.ReplicaStatus.FAILED_INITIAL_DELAY
            if not self.service_ready_now:
                # Max continuous failure exceeded
                return serve_state.ReplicaStatus.FAILED_PROBING
            # This indicate it is a scale_down with correct teardown.
            # Should have been cleaned from the replica table.
            return serve_state.ReplicaStatus.UNKNOWN
        if self.sky_launch_status == common_utils.ProcessStatus.FAILED:
            # sky.launch failed
            # The down thread has not been started if it reaches here,
            # due to the `if self.sky_down_status is not None`` check above.
            # However, it should have been started by _refresh_thread_pool.
            # If not started, this means some bug prevent sky.down from
            # executing. It is also a potential resource leak, so we mark
            # it as FAILED_CLEANUP.
            return serve_state.ReplicaStatus.FAILED_CLEANUP
        if self.user_app_failed:
            # Failed on user setup/run
            # Same as above, the down thread should have been started.
            return serve_state.ReplicaStatus.FAILED_CLEANUP
        if self.service_ready_now:
            # Service is ready
            return serve_state.ReplicaStatus.READY
        if self.first_ready_time is not None and self.first_ready_time >= 0.0:
            # Service was ready before but not now
            return serve_state.ReplicaStatus.NOT_READY
        else:
            # No readiness probe passed and sky.launch finished
            return serve_state.ReplicaStatus.STARTING


_REPLICA_STATUS_PROPERTY_FIELDS = tuple(
    field.name for field in dataclasses.fields(ReplicaStatusProperty))
_REPLICA_STATUS_PROPERTY_LEGACY_DEFAULTS = dict(vars(ReplicaStatusProperty()))
# Absence predates the down-admission commit bit. It is not equivalent to the
# current default False: a missing bit makes a SCHEDULED teardown ambiguous.
_REPLICA_STATUS_PROPERTY_LEGACY_DEFAULTS['logical_retirement_committed'] = None


def _materialize_legacy_status_property_fields(
        status_property: ReplicaStatusProperty) -> None:
    """Own every status field after decoding a pre-v14 replica record."""
    status_state = vars(status_property)
    for field, default in _REPLICA_STATUS_PROPERTY_LEGACY_DEFAULTS.items():
        status_state.setdefault(field, default)


def _require_status_property_fields(status_property: ReplicaStatusProperty, *,
                                    owner: str) -> None:
    """Reject a current record whose status object is only partially owned."""
    status_state = vars(status_property)
    missing = [
        field for field in _REPLICA_STATUS_PROPERTY_FIELDS
        if field not in status_state
    ]
    if missing:
        raise AttributeError(
            f'{owner} is missing required ReplicaStatusProperty fields: '
            f'{", ".join(missing)}')


_REPLICA_INFO_OWNED_FIELDS = (
    'replica_id',
    'cluster_name',
    'version',
    'replica_port',
    'created_at',
    'replica_record_id',
    'first_not_ready_time',
    'first_consecutive_failure_time',
    'status_property',
    'is_spot',
    'location',
    'resources_override',
    'planned_capacity',
    'unknown_capacity_replacement',
    'logical_bridge_capacity_verified',
    'reserved_fill',
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
    'zero_cost_admission_sequence',
    'zero_cost_materialization_sequence',
    'is_zero_cost',
    'cost_rebalance_for_replica_id',
    'paid_capacity_pool_key',
    'system_recovery_launch_intent',
    'system_recovery_disposition',
    'launch_request_id',
    'service_job_id',
    'candidate_ready_observed_at',
    'ordinary_release_not_before',
    'system_recovery_revision',
    'system_recovery',
    'system_recovery_quarantine',
)
# Process-local fields are part of the complete in-memory interface, but are
# deliberately excluded from the versioned JSON storage contract.
_REPLICA_INFO_TRANSIENT_FIELDS = ('non_pool_launch_authorization',)
_REPLICA_INFO_STORAGE_FIELDS = ('replica_info_version',
                                *_REPLICA_INFO_OWNED_FIELDS)
_SUPPORTED_LEGACY_JSON_VERSIONS = frozenset((3, 6, 7, 12, 13, 14))
_LEGACY_JSON_BASE_STORAGE_FIELDS = frozenset((
    'replica_info_version',
    'replica_id',
    'cluster_name',
    'version',
    'replica_port',
    'created_at',
    'first_not_ready_time',
    'first_consecutive_failure_time',
    'status_property',
    'is_spot',
    'location',
    'resources_override',
    'reserved_fill',
    'cost_rebalance_for_replica_id',
))
_LEGACY_JSON_CAPACITY_STORAGE_FIELDS = frozenset((
    'planned_capacity',
    'unknown_capacity_replacement',
    'logical_bridge_capacity_verified',
))
_LEGACY_JSON_ECONOMIC_STORAGE_FIELDS = frozenset((
    'is_zero_cost',
    'paid_capacity_pool_key',
))
_LEGACY_JSON_RECOVERY_STORAGE_FIELDS = frozenset(V13_ADDITIVE_STORAGE_FIELDS)
_LEGACY_JSON_RESERVED_FILL_IDENTITY_V13_FIELDS = frozenset((
    'reserved_fill_pool_key',
    'reserved_fill_service_generation',
    'reserved_fill_physical_cluster_uid',
))
_LEGACY_JSON_RESERVED_FILL_IDENTITY_V14_FIELDS = frozenset((
    *_LEGACY_JSON_RESERVED_FILL_IDENTITY_V13_FIELDS,
    'reserved_fill_kubernetes_context',
))
_LEGACY_JSON_CAPACITY_SHAPE = (_LEGACY_JSON_BASE_STORAGE_FIELDS |
                               _LEGACY_JSON_CAPACITY_STORAGE_FIELDS)
_LEGACY_JSON_ECONOMIC_SHAPE = (_LEGACY_JSON_CAPACITY_SHAPE |
                               _LEGACY_JSON_ECONOMIC_STORAGE_FIELDS)
_LEGACY_JSON_RECOVERY_SHAPE = (_LEGACY_JSON_ECONOMIC_SHAPE |
                               _LEGACY_JSON_RECOVERY_STORAGE_FIELDS)
_LEGACY_JSON_STORAGE_SHAPES = {
    3: (_LEGACY_JSON_BASE_STORAGE_FIELDS,),
    6: (_LEGACY_JSON_CAPACITY_SHAPE,),
    7: (_LEGACY_JSON_BASE_STORAGE_FIELDS,),
    12: (_LEGACY_JSON_ECONOMIC_SHAPE,),
    13: (
        _LEGACY_JSON_RECOVERY_SHAPE,
        _LEGACY_JSON_RECOVERY_SHAPE |
        _LEGACY_JSON_RESERVED_FILL_IDENTITY_V13_FIELDS,
        _LEGACY_JSON_RECOVERY_SHAPE |
        _LEGACY_JSON_RESERVED_FILL_IDENTITY_V14_FIELDS,
    ),
    14: (_LEGACY_JSON_RECOVERY_SHAPE |
         _LEGACY_JSON_RESERVED_FILL_IDENTITY_V14_FIELDS,),
}
_LEGACY_JSON_STATUS_BASE_FIELDS = frozenset((
    'sky_launch_status',
    'user_app_failed',
    'service_ready_now',
    'first_ready_time',
    'sky_down_status',
    'is_scale_down',
    'preempted',
    'purged',
    'failed_spot_availability',
    'drain_cap_seconds',
    'wait_for_idle_before_termination',
))
_LEGACY_JSON_STATUS_CURRENT_FIELDS = frozenset(_REPLICA_STATUS_PROPERTY_FIELDS)
_LEGACY_JSON_STATUS_SHAPES = {
    3: _LEGACY_JSON_STATUS_BASE_FIELDS,
    6: _LEGACY_JSON_STATUS_CURRENT_FIELDS,
    7: _LEGACY_JSON_STATUS_BASE_FIELDS,
    12: _LEGACY_JSON_STATUS_CURRENT_FIELDS,
    13: _LEGACY_JSON_STATUS_CURRENT_FIELDS,
    14: _LEGACY_JSON_STATUS_CURRENT_FIELDS,
}
_REPLICA_INFO_LEGACY_DEFAULTS = {
    'created_at': None,
    'first_not_ready_time': None,
    'first_consecutive_failure_time': None,
    'is_spot': False,
    'location': None,
    'resources_override': None,
    'planned_capacity': 1,
    'unknown_capacity_replacement': False,
    'logical_bridge_capacity_verified': False,
    'reserved_fill': False,
    'reserved_fill_pool_key': None,
    'reserved_fill_service_generation': None,
    'reserved_fill_physical_cluster_uid': None,
    'reserved_fill_kubernetes_context': None,
    'reserved_fill_allocation_generation': None,
    'reserved_fill_allocation_input_sha256': None,
    'reserved_fill_allocation_claim_generation': None,
    'reserved_fill_reconciliation_gate_generation': None,
    'reserved_fill_reclaim_fleet_bundle_sha256': None,
    'reserved_fill_reclaim_policy_revision': None,
    'reserved_fill_reclaim_provider_inventory_sha256': None,
    'reserved_fill_worker_projection_sha256': None,
    'reserved_fill_observation_generation': None,
    'reserved_fill_observation_sequence': None,
    'reserved_fill_intent_idempotency_key': None,
    'zero_cost_admission_sequence': None,
    'zero_cost_materialization_sequence': None,
    'is_zero_cost': False,
    'cost_rebalance_for_replica_id': None,
    'paid_capacity_pool_key': None,
}


def _materialize_legacy_replica_info_fields(replica: Any) -> None:
    """Own additive ReplicaInfo fields after decoding a pre-v18 record."""
    replica_state = vars(replica)
    for field, default in _REPLICA_INFO_LEGACY_DEFAULTS.items():
        replica_state.setdefault(field, default)
    status_property = replica.status_property
    _materialize_legacy_status_property_fields(status_property)


def _require_replica_info_fields(replica: Any, *, owner: str) -> None:
    """Reject a current in-memory record with an incomplete interface."""
    replica_state = vars(replica)
    missing = [
        field for field in _REPLICA_INFO_OWNED_FIELDS
        if field not in replica_state
    ]
    if missing:
        raise AttributeError(f'{owner} is missing required ReplicaInfo fields: '
                             f'{", ".join(missing)}')
    _require_status_property_fields(replica.status_property, owner=owner)


def _require_exact_storage_fields(
    state: dict[str, Any], *, owner: str, optional: tuple[str,
                                                          ...] = ()) -> None:
    """Require one closed top-level and status-property storage shape."""
    expected = set(_REPLICA_INFO_STORAGE_FIELDS)
    observed = set(state)
    missing = sorted(expected - observed - set(optional))
    unexpected = sorted(observed - expected)
    if missing or unexpected:
        details = []
        if missing:
            details.append(f'missing fields: {", ".join(missing)}')
        if unexpected:
            details.append(f'unexpected fields: {", ".join(unexpected)}')
        raise ValueError(f'{owner} has an invalid top-level shape ('
                         f'{"; ".join(details)}).')
    status_state = state['status_property']
    if not isinstance(status_state, dict):
        raise ValueError(f'{owner} status_property must be a dict.')
    expected_status = set(_REPLICA_STATUS_PROPERTY_FIELDS)
    observed_status = set(status_state)
    missing_status = sorted(expected_status - observed_status)
    unexpected_status = sorted(observed_status - expected_status)
    if missing_status or unexpected_status:
        details = []
        if missing_status:
            details.append(f'missing fields: {", ".join(missing_status)}')
        if unexpected_status:
            details.append(f'unexpected fields: {", ".join(unexpected_status)}')
        raise ValueError(f'{owner} has an invalid status_property shape ('
                         f'{"; ".join(details)}).')


def _require_current_storage_fields(state: dict[str, Any]) -> None:
    """Reject anything except the exact v18 JSON record shape."""
    _require_exact_storage_fields(state, owner='ReplicaInfo v18')


def _require_v17_collision_storage_fields(state: dict[str, Any]) -> None:
    """Accept only the observed v17 collision, never generic legacy JSON."""
    attribution = set(V17_COLLISION_OPTIONAL_STORAGE_FIELDS)
    present_attribution = attribution.intersection(state)
    if present_attribution not in (set(), attribution):
        missing = sorted(attribution - present_attribution)
        raise ValueError('ReplicaInfo v17 collision has a partially missing '
                         'attribution bundle: '
                         f'{", ".join(missing)}')
    _require_exact_storage_fields(
        state,
        owner='ReplicaInfo v17 collision',
        optional=(V17_COLLISION_OPTIONAL_STORAGE_FIELDS
                  if not present_attribution else ()))


def _require_legacy_json_storage_fields(state: dict[str, Any],
                                        version: int) -> None:
    """Accept only the closed pre-v17 shapes observed in the live census."""
    allowed_shapes = _LEGACY_JSON_STORAGE_SHAPES[version]
    observed = frozenset(state)
    if observed not in allowed_shapes:
        nearest = min(
            allowed_shapes,
            key=lambda shape: len(shape.symmetric_difference(observed)))
        missing = sorted(nearest - observed)
        unexpected = sorted(observed - nearest)
        details = []
        if missing:
            details.append(f'missing fields: {", ".join(missing)}')
        if unexpected:
            details.append(f'unexpected fields: {", ".join(unexpected)}')
        raise ValueError(f'Legacy ReplicaInfo v{version} has an invalid '
                         f'top-level shape ({"; ".join(details)}).')

    status_state = state['status_property']
    if not isinstance(status_state, dict):
        raise ValueError(
            f'Legacy ReplicaInfo v{version} status_property must be a dict.')
    expected_status = _LEGACY_JSON_STATUS_SHAPES[version]
    observed_status = frozenset(status_state)
    missing_status = sorted(expected_status - observed_status)
    unexpected_status = sorted(observed_status - expected_status)
    if missing_status or unexpected_status:
        details = []
        if missing_status:
            details.append(f'missing fields: {", ".join(missing_status)}')
        if unexpected_status:
            details.append(f'unexpected fields: {", ".join(unexpected_status)}')
        raise ValueError(f'Legacy ReplicaInfo v{version} has an invalid '
                         f'status_property shape ({"; ".join(details)}).')


def _require_current_pickle_fields(state: dict[str, Any], version: int) -> None:
    """Reject a v18+ pickle whose owned in-memory interface is partial."""
    missing = [
        field for field in _REPLICA_INFO_OWNED_FIELDS if field not in state
    ]
    owner = f'ReplicaInfo v{version} pickle'
    if missing:
        raise AttributeError(f'{owner} is missing required ReplicaInfo fields: '
                             f'{", ".join(missing)}')
    _require_status_property_fields(state['status_property'], owner=owner)


def _encode_replica_resource_state(
        state: dict[str, Any] | None) -> dict[str, Any] | None:
    """Makes a location/resources override lossless in a JSON object.

    ``Resources.image_id`` is keyed by a region or by ``None`` for a
    region-independent image. JSON object keys cannot represent ``None``:
    PostgreSQL JSONB reads it back as the string ``"null"``. Store this one
    nested mapping as key/value pairs so its key types survive the round trip.
    """
    return spot_placer.encode_resources_override(state)


def _decode_replica_resource_state(
        state: dict[str, Any] | None) -> dict[str, Any] | None:
    """Restores the internal location/resources override representation."""
    return spot_placer.decode_resources_override(state)


def _exact_reserved_fill_marker(value: Any) -> bool:
    """Return one durable fill marker without truthiness coercion."""
    if type(value) is not bool:  # pylint: disable=unidiomatic-typecheck
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Stored reserved_fill marker must be a boolean.')
    return value


_LEGACY_RESERVED_FILL_ALLOCATION_ATTRIBUTION_FIELDS = (
    'reserved_fill_allocation_generation',
    'reserved_fill_allocation_input_sha256',
    'reserved_fill_allocation_claim_generation',
    'reserved_fill_observation_generation',
    'reserved_fill_observation_sequence',
    'reserved_fill_intent_idempotency_key',
)
_RESERVED_FILL_RECLAIM_POLICY_ATTRIBUTION_FIELDS = (
    'reserved_fill_reconciliation_gate_generation',
    'reserved_fill_reclaim_fleet_bundle_sha256',
    'reserved_fill_reclaim_policy_revision',
    'reserved_fill_reclaim_provider_inventory_sha256',
    'reserved_fill_worker_projection_sha256',
)
_RESERVED_FILL_ALLOCATION_ATTRIBUTION_FIELDS = (
    *_LEGACY_RESERVED_FILL_ALLOCATION_ATTRIBUTION_FIELDS,
    *_RESERVED_FILL_RECLAIM_POLICY_ATTRIBUTION_FIELDS,
)


def validate_reserved_fill_allocation_attribution(
    replica: Any,
    *,
    require_policy_bound_admission: bool = False,
) -> None:
    """Validate one complete typed allocation identity or its legacy absence.

    Protocol-v2 rows written before the typed planner legitimately have no
    allocation attribution.  Serve044 rows may carry the complete historical
    allocation tuple without the Serve045 policy identity.  Both remain
    readable, but neither can pass the identity-bound launch fence after the
    one-way gate activates.  New typed rows persist the complete tuple;
    accepting a partial tuple would let unrelated authorities be conflated.
    """
    state = vars(replica)
    if type(state.get('is_zero_cost')) is not bool:
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'is_zero_cost must be an exact boolean.')
    values = {
        field: state.get(field)
        for field in _RESERVED_FILL_ALLOCATION_ATTRIBUTION_FIELDS
    }
    admission_sequence = state.get('zero_cost_admission_sequence')
    if admission_sequence is not None:
        if (type(admission_sequence) is not int or  # pylint: disable=unidiomatic-typecheck
                admission_sequence < 1):
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'zero_cost_admission_sequence must be a positive integer.')
        if state.get('is_zero_cost') is not True:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'zero_cost_admission_sequence requires a zero-cost row.')
    materialization_sequence = state.get('zero_cost_materialization_sequence')
    if materialization_sequence is not None:
        if (type(materialization_sequence) is not int or  # pylint: disable=unidiomatic-typecheck
                materialization_sequence < 1):
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'zero_cost_materialization_sequence must be a positive '
                'integer.')
        if state.get('is_zero_cost') is not True:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'zero_cost_materialization_sequence requires a zero-cost '
                'row.')
        # This marker is immutable historical evidence of first successful
        # materialization. Teardown and cleanup legitimately move the current
        # process status to INTERRUPTED or FAILED after that success; first
        # assignment is guarded by the transactional Serve state writer.
    legacy_values = {
        field: values[field]
        for field in _LEGACY_RESERVED_FILL_ALLOCATION_ATTRIBUTION_FIELDS
    }
    policy_values = {
        field: values[field]
        for field in _RESERVED_FILL_RECLAIM_POLICY_ATTRIBUTION_FIELDS
    }
    if all(value is None for value in legacy_values.values()):
        if any(value is not None for value in policy_values.values()):
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'Reserved-fill reclaim policy attribution requires legacy '
                'allocation attribution.')
        # Ordinary zero-cost rows intentionally carry only the global commit
        # sequence. The independent ordinary admission high-water invalidates
        # stale allocation maps; this total sequence remains durable row
        # attribution without pretending the row was fill-authorized.
        if admission_sequence is not None and replica.reserved_fill is True:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'A sequenced reserved-fill row requires complete allocation '
                'attribution.')
        if require_policy_bound_admission:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'Policy-bound reserved-fill launch requires complete '
                'allocation and reclaim-policy attribution.')
        return
    if any(value is None for value in legacy_values.values()):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill allocation attribution must be complete.')
    if (any(value is not None for value in policy_values.values()) and
            any(value is None for value in policy_values.values())):
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill reclaim policy attribution must be complete.')
    if replica.reserved_fill is not True:
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill allocation attribution requires a fill row.')

    for field in ('reserved_fill_pool_key',
                  'reserved_fill_physical_cluster_uid',
                  'reserved_fill_kubernetes_context'):
        value = state.get(field)
        if type(value) is not str or not value:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'Reserved-fill allocation attribution requires a complete '
                'protocol-v2 pool identity.')
    service_generation = state.get('reserved_fill_service_generation')
    if type(service_generation) is not int or service_generation < 1:
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'Reserved-fill allocation attribution requires a positive '
            'service generation.')

    for field in ('reserved_fill_allocation_generation',
                  'reserved_fill_allocation_claim_generation',
                  'reserved_fill_observation_generation'):
        value = values[field]
        if type(value) is not int or value < 1:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                f'{field} must be a positive integer.')
    observation_sequence = values['reserved_fill_observation_sequence']
    if type(observation_sequence) is not int or observation_sequence < 0:
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'reserved_fill_observation_sequence must be a nonnegative '
            'integer.')
    for field in ('reserved_fill_allocation_input_sha256',
                  'reserved_fill_intent_idempotency_key'):
        value = values[field]
        if (type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None):
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                f'{field} must be a lowercase SHA-256 digest.')
    if all(value is None for value in policy_values.values()):
        if require_policy_bound_admission:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'Policy-bound reserved-fill launch requires complete '
                'reclaim-policy attribution.')
        return
    gate_generation = values['reserved_fill_reconciliation_gate_generation']
    if type(gate_generation) is not int or gate_generation < 1:
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'reserved_fill_reconciliation_gate_generation must be a positive '
            'integer.')
    for field in ('reserved_fill_reclaim_fleet_bundle_sha256',
                  'reserved_fill_reclaim_provider_inventory_sha256',
                  'reserved_fill_worker_projection_sha256'):
        value = values[field]
        if (type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None):
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                f'{field} must be a lowercase SHA-256 digest.')
    policy_revision = values['reserved_fill_reclaim_policy_revision']
    if type(policy_revision) is not str or not policy_revision:
        raise exceptions.KubernetesPhysicalClusterIdentityError(
            'reserved_fill_reclaim_policy_revision must be nonempty text.')
    if require_policy_bound_admission:
        if (getattr(replica, '_version', None) != _REPLICA_INFO_VERSION or
                getattr(replica, '_VERSION', None) != _REPLICA_INFO_VERSION):
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'Policy-bound reserved-fill launch requires a current '
                f'ReplicaInfo v{_REPLICA_INFO_VERSION} row.')
        if (admission_sequence is None or
                values['reserved_fill_allocation_claim_generation']
                != service_generation):
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'Policy-bound reserved-fill launch requires complete admitted '
                'allocation provenance.')


# Historical private import identity retained while the public validator is
# the single serialization, decoding, and terminal-launch contract.
_validate_reserved_fill_allocation_attribution = (
    validate_reserved_fill_allocation_attribution)


def _set_ordinary_system_recovery_defaults(replica: Any) -> None:
    replica.system_recovery_launch_intent = None
    replica.system_recovery_disposition = (
        system_recovery_state.SystemRecoveryDisposition.ORDINARY)
    replica.launch_request_id = None
    replica.service_job_id = None
    replica.candidate_ready_observed_at = None
    replica.ordinary_release_not_before = None
    replica.system_recovery_revision = 0
    replica.system_recovery = None
    replica.system_recovery_quarantine = None


def _quarantine_system_recovery(
        replica: Any,
        reason: system_recovery_state.RecoveryQuarantineReason) -> None:
    """Replace an unsafe bundle with a reason-only, absorbing marker."""
    _ensure_replica_record_id(replica)
    _set_ordinary_system_recovery_defaults(replica)
    replica.system_recovery_quarantine = (
        system_recovery_state.SystemRecoveryQuarantine(reason=reason))


def _validate_system_recovery_fields(replica: Any) -> None:
    """Validate the complete in-memory v13 recovery subdocument."""
    _canonical_replica_record_id(replica.replica_record_id)
    intent = replica.system_recovery_launch_intent
    disposition = replica.system_recovery_disposition
    launch_request_id = replica.launch_request_id
    service_job_id = replica.service_job_id
    ready_at = replica.candidate_ready_observed_at
    release_at = replica.ordinary_release_not_before
    revision = replica.system_recovery_revision
    recovery = replica.system_recovery
    quarantine = replica.system_recovery_quarantine

    if intent is not None:
        if not isinstance(intent,
                          system_recovery_state.SystemRecoveryLaunchIntent):
            raise system_recovery_state.RecoveryStateError(
                'system_recovery_launch_intent has an invalid type.')
        if intent.replica_id != replica.replica_id:
            raise system_recovery_state.RecoveryStateError(
                'Recovery launch intent does not match replica ID.')
    if not isinstance(disposition,
                      system_recovery_state.SystemRecoveryDisposition):
        raise system_recovery_state.RecoveryStateError(
            'system_recovery_disposition has an invalid type.')
    if (launch_request_id is not None and
        (not isinstance(launch_request_id, str) or not launch_request_id)):
        raise system_recovery_state.RecoveryStateError(
            'launch_request_id must be a nonempty string.')
    if service_job_id is not None:
        if (isinstance(service_job_id, bool) or
                not isinstance(service_job_id, int) or service_job_id < 1):
            raise system_recovery_state.RecoveryStateError(
                'service_job_id must be a positive integer.')
        if launch_request_id is None:
            raise system_recovery_state.RecoveryStateError(
                'service_job_id requires launch_request_id.')
    ready_at = _optional_positive_timestamp(ready_at,
                                            'candidate_ready_observed_at')
    release_at = _optional_positive_timestamp(release_at,
                                              'ordinary_release_not_before')
    if (ready_at is None) != (release_at is None):
        raise system_recovery_state.RecoveryStateError(
            'Candidate freshness anchors must be both set or both absent.')
    if ready_at is not None:
        assert release_at is not None
        if not math.isclose(
                release_at - ready_at,
                system_recovery_state.CANDIDATE_RELEASE_GUARD_SECONDS,
                rel_tol=0,
                abs_tol=1e-9):
            raise system_recovery_state.RecoveryStateError(
                'Candidate freshness anchors disagree with the fixed guard.')
    if (isinstance(revision, bool) or not isinstance(revision, int) or
            revision < 0):
        raise system_recovery_state.RecoveryStateError(
            'system_recovery_revision must be a nonnegative integer.')
    if recovery is not None:
        if not isinstance(recovery,
                          system_recovery_state.ReplicaSystemRecovery):
            raise system_recovery_state.RecoveryStateError(
                'system_recovery has an invalid type.')
        if service_job_id is None or recovery.job_id != service_job_id:
            raise system_recovery_state.RecoveryStateError(
                'system_recovery must match the exact service job ID.')
        if intent is None:
            raise system_recovery_state.RecoveryStateError(
                'system_recovery requires a launch intent.')
        if (recovery.capability != intent.expected_runtime_capability or
                recovery.profile_version != intent.runtime_profile_version):
            raise system_recovery_state.RecoveryStateError(
                'system_recovery capability does not match launch intent.')
    if quarantine is not None and not isinstance(
            quarantine, system_recovery_state.SystemRecoveryQuarantine):
        raise system_recovery_state.RecoveryStateError(
            'system_recovery_quarantine has an invalid type.')

    if quarantine is not None:
        # Only already-validated typed fields can accompany the reason-only
        # marker. Malformed decoder input is sanitized before it reaches here.
        # Returning now makes quarantine absorbing without requiring a
        # disposition transition that could accidentally grant authority.
        return

    if disposition in (
            system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
            system_recovery_state.SystemRecoveryDisposition.CAPABLE):
        if intent is None:
            raise system_recovery_state.RecoveryStateError(
                'Recovery disposition requires a launch intent.')
    if (disposition == system_recovery_state.SystemRecoveryDisposition.CANDIDATE
            and recovery is not None):
        raise system_recovery_state.RecoveryStateError(
            'CANDIDATE cannot contain capable recovery state.')
    if (disposition == system_recovery_state.SystemRecoveryDisposition.ORDINARY
            and recovery is not None):
        raise system_recovery_state.RecoveryStateError(
            'ORDINARY cannot contain capable recovery state.')
    if disposition == system_recovery_state.SystemRecoveryDisposition.CAPABLE:
        if (launch_request_id is None or service_job_id is None or
                recovery is None):
            raise system_recovery_state.RecoveryStateError(
                'CAPABLE requires exact request, job, and recovery state.')
    if intent is None and any(value is not None
                              for value in (launch_request_id, service_job_id,
                                            ready_at, release_at, recovery)):
        raise system_recovery_state.RecoveryStateError(
            'Recovery associations require a historical launch intent.')


def copy_system_recovery_fields(source: Any,
                                destination: Any,
                                *,
                                increment_revision: bool = False) -> None:
    """Copy only the v13 recovery subdocument into a locked latest record.

    With ``increment_revision=True``, ``source`` must have been reduced from
    the same revision as ``destination``.  The destination is advanced exactly
    once; a stale source is rejected instead of being replayed.
    """
    _validate_system_recovery_fields(source)
    source_revision = source.system_recovery_revision
    destination_state = vars(destination)
    destination_revision = destination_state.get('system_recovery_revision', 0)
    source_record_id = source.replica_record_id
    destination_record_id = destination_state.get('replica_record_id')
    if (isinstance(destination_revision, bool) or
            not isinstance(destination_revision, int) or
            destination_revision < 0):
        raise system_recovery_state.RecoveryStateError(
            'Locked recovery revision is invalid.')
    if increment_revision and source_revision != destination_revision:
        raise system_recovery_state.RecoveryStateError(
            'Recovery subdocument revision changed.')
    if source_record_id != destination_record_id:
        raise system_recovery_state.RecoveryStateError(
            'Replica record identity changed.')
    for field in SYSTEM_RECOVERY_STORAGE_FIELDS:
        if field == 'system_recovery_revision':
            continue
        setattr(destination, field, vars(source)[field])
    destination.system_recovery_revision = (
        system_recovery_state.next_recovery_revision(destination_revision)
        if increment_revision else source_revision)
    setattr(destination, '_version',
            max(destination_state.get('_version', 0), _REPLICA_INFO_VERSION))
    _validate_system_recovery_fields(destination)


def is_recoverable_uncommitted_logical_retirement(info: Any) -> bool:
    """Whether an exact logical retirement is durable but reversible."""
    status = info.status_property
    retirement_version = status.logical_retirement_version
    controller_epoch = status.logical_retirement_controller_epoch
    selection_generation = status.logical_retirement_generation
    selection_target = status.logical_retirement_target_capacity
    confirmed_generation = status.logical_retirement_confirmed_generation
    bounded_deadline = status.logical_retirement_bounded_deadline
    generation_valid = (type(selection_generation) is int and
                        selection_generation >= 0)
    confirmation_valid = (
        confirmed_generation is None or
        (type(confirmed_generation) is int and generation_valid and
         confirmed_generation >= typing.cast(int, selection_generation)))
    strict_idle_wait = (status.wait_for_idle_before_termination is True and
                        confirmation_valid)
    bounded_precommit = (status.wait_for_idle_before_termination is False and
                         bounded_deadline is True and
                         type(confirmed_generation) is int and
                         generation_valid and
                         confirmed_generation >= typing.cast(
                             int, selection_generation) and
                         type(info.version) is int and
                         type(retirement_version) is int and
                         info.version <= retirement_version)
    return bool(
        status.sky_launch_status == common_utils.ProcessStatus.SUCCEEDED and
        status.sky_down_status == common_utils.ProcessStatus.SCHEDULED and
        status.is_scale_down is True and status.preempted is False and
        status.purged is False and (strict_idle_wait or bounded_precommit) and
        status.logical_retirement_committed is False and
        type(info.version) is int and type(retirement_version) is int and
        info.version <= retirement_version and
        isinstance(controller_epoch, str) and bool(controller_epoch) and
        generation_valid and type(selection_target) is int and
        selection_target >= 0 and type(bounded_deadline) is bool)


def is_uncommitted_logical_retirement_admission(info: Any) -> bool:
    """Whether exact readback proves down admission did not commit."""
    status = info.status_property
    retirement_version = status.logical_retirement_version
    controller_epoch = status.logical_retirement_controller_epoch
    selection_generation = status.logical_retirement_generation
    selection_target = status.logical_retirement_target_capacity
    confirmed_generation = status.logical_retirement_confirmed_generation
    return bool(
        status.sky_launch_status == common_utils.ProcessStatus.SUCCEEDED and
        status.sky_down_status == common_utils.ProcessStatus.SCHEDULED and
        status.is_scale_down is True and status.preempted is False and
        status.purged is False and
        status.wait_for_idle_before_termination is False and
        status.logical_retirement_committed is False and
        type(info.version) is int and type(retirement_version) is int and
        info.version <= retirement_version and
        isinstance(controller_epoch, str) and bool(controller_epoch) and
        type(selection_generation) is int and selection_generation >= 0 and
        type(selection_target) is int and selection_target >= 0 and
        type(confirmed_generation) is int and
        selection_generation <= confirmed_generation and
        type(status.logical_retirement_bounded_deadline) is bool)


def is_restart_recoverable_logical_retirement(info: Any) -> bool:
    """Whether controller restart may safely re-fence or reactivate a row."""
    return bool(
        is_recoverable_uncommitted_logical_retirement(info) or
        is_uncommitted_logical_retirement_admission(info))


class ReplicaInfo:
    """Replica info for each replica."""

    # Version 6 is also a worker-runtime compatibility marker for immutable
    # Sky Batch attempt outputs. New Batch clients reject older pool replicas
    # so an incompatible worker fails before dispatch rather than mid-run.
    # Version 7 replaces the consecutive_failure_times list with the single
    # first_consecutive_failure_time timestamp.
    # Version 8 persists the immutable logical slot width selected for this
    # physical backend. Version 9 marks bounded unknown-capacity replacement
    # rows so a persistent telemetry outage cannot recursively replace them.
    # Version 10 records that a pre-activation physical bridge has published
    # a load-balancer-verified logical width.
    # Version 11 persists placement-cost provenance independently from the
    # reserved_fill launch reason. Demand launches can also land on free
    # reserved capacity and must receive the same routing preference.
    # Version 12 stores the exact global paid-capacity pool claim associated
    # with an unresolved fresh demand launch. Version 13 adds the closed
    # system-recovery launch disposition, exact associations, freshness
    # anchors, monotonic subdocument revision, nested state, and quarantine.
    # Version 14 makes every field owned by ReplicaInfo and
    # ReplicaStatusProperty explicit after the storage/pickle decode boundary.
    # Version 15 persists the exact typed allocation publication, observation,
    # intent, and database-assigned admission identities for replay-safe
    # reserved-fill debits.
    # Version 16 binds reserved-fill attribution to the reconciliation-gate
    # generation and immutable deployment reclaim-policy identity.
    # Version 18 repairs the v17 record-label collision: some v17 writers
    # omitted the 13 sequenced-attribution keys added during Serve046. A
    # pre-finalization normalizer rewrites every retained row through this
    # serializer, adding explicit nulls only where that attribution is absent.
    _VERSION = _REPLICA_INFO_VERSION

    def __init__(self,
                 replica_id: int,
                 cluster_name: str,
                 replica_port: str,
                 is_spot: bool,
                 location: spot_placer.Location | None,
                 version: int,
                 resources_override: dict[str, Any] | None,
                 planned_capacity: int = 1,
                 unknown_capacity_replacement: bool = False) -> None:
        self._version = self._VERSION
        self.replica_id: int = replica_id
        self.cluster_name: str = cluster_name
        self.version: int = version
        self.replica_port: str = replica_port
        # Row creation time, set the moment the row object is built (before
        # the row is persisted or any launch/pod exists), so it is present
        # for every nonterminal status including PROVISIONING. The
        # reserved-capacity fill overlay compares it against its free-slot
        # snapshot time to debit replicas that landed on the zero-cost tier
        # after the snapshot was taken (see
        # Autoscaler._fill_row_occupies_free_slot).
        self.created_at: float | None = time.time()
        # This fences a physical database record, independently from the
        # reusable numeric replica id and from resource-action identity.
        self.replica_record_id: str = str(uuid.uuid4())
        self.first_not_ready_time: float | None = None
        # Start of the current run of consecutive failed readiness probes
        # after the replica was once READY; None while the replica is
        # passing probes. The failure window is measured against the
        # current probe time, so only the first failure needs to be kept.
        self.first_consecutive_failure_time: float | None = None
        self.status_property: ReplicaStatusProperty = ReplicaStatusProperty()
        self.is_spot: bool = is_spot
        self.location: dict[str, Any] | None = (location.to_pickleable() if
                                                location is not None else None)
        self.resources_override: dict[str, Any] | None = resources_override
        if (isinstance(planned_capacity, bool) or
                not isinstance(planned_capacity, int) or planned_capacity < 1):
            raise ValueError('planned_capacity must be a positive integer. '
                             f'Got: {planned_capacity!r}')
        self.planned_capacity: int = planned_capacity
        self.unknown_capacity_replacement = bool(unknown_capacity_replacement)
        # A physical row created before implicit logical replicas starts at
        # width one. It becomes part of the logical capacity contract only
        # after the live LB probes the local router and the controller clamps
        # that observation to the backend's launched GPU count.
        self.logical_bridge_capacity_verified: bool = False
        # Launch-origin attribution: True only for sentinel (fill)
        # launches; set by _launch_replica before the row is persisted.
        # The broker's holdings split and the grant ceiling's demand
        # exemption both key on it. A controller restart never re-drives an
        # interrupted fill row because its one-shot broker authority was
        # consumed; recovery tears it down and lets a fresh round refill.
        self.reserved_fill: bool = False
        # Protocol-v2 origin fences for reserved fill. These were additive
        # through v13; the v14 decode boundary makes them explicit attributes.
        self.reserved_fill_pool_key: str | None = None
        self.reserved_fill_service_generation: int | None = None
        self.reserved_fill_physical_cluster_uid: str | None = None
        self.reserved_fill_kubernetes_context: str | None = None
        # Exact publication identity for typed protocol-v2 admission. Legacy
        # fill rows keep the complete tuple absent and are not replay debits.
        self.reserved_fill_allocation_generation: int | None = None
        self.reserved_fill_allocation_input_sha256: str | None = None
        self.reserved_fill_allocation_claim_generation: int | None = None
        self.reserved_fill_reconciliation_gate_generation: int | None = None
        self.reserved_fill_reclaim_fleet_bundle_sha256: str | None = None
        self.reserved_fill_reclaim_policy_revision: str | None = None
        self.reserved_fill_reclaim_provider_inventory_sha256: str | None = None
        self.reserved_fill_worker_projection_sha256: str | None = None
        self.reserved_fill_observation_generation: int | None = None
        self.reserved_fill_observation_sequence: int | None = None
        self.reserved_fill_intent_idempotency_key: str | None = None
        # Assigned in the same database transaction that increments the
        # protocol's global zero-cost admission sequence. It is intentionally
        # not trusted from the planner or transitional override.
        self.zero_cost_admission_sequence: int | None = None
        # Assigned atomically with the first persisted successful sky.launch
        # transition. It orders provider-visible materialization independently
        # from row admission, closing the observe-while-binding race.
        self.zero_cost_materialization_sequence: int | None = None
        # Placement-cost provenance, not launch intent. True means the
        # replica occupies capacity the placer classifies as zero cost.
        self.is_zero_cost: bool = False
        # Incumbent id this replica was launched to replace economically.
        # None for ordinary demand/fill launches.
        self.cost_rebalance_for_replica_id: int | None = None
        # Exact provider capacity pool whose unresolved-launch allowance this
        # row consumes. None for zero-cost, recovery-only, and pre-v12 rows.
        self.paid_capacity_pool_key: str | None = None
        # Initial-insert-only planner authority is stored in its own
        # PostgreSQL column, not in the versioned ReplicaInfo JSON. This
        # process-local field carries it only across planner construction and
        # the atomic row insert.
        self.non_pool_launch_authorization: dict[str, Any] | None = None
        self.system_recovery_launch_intent: (
            system_recovery_state.SystemRecoveryLaunchIntent | None) = None
        self.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.ORDINARY)
        self.launch_request_id: str | None = None
        self.service_job_id: int | None = None
        self.candidate_ready_observed_at: float | None = None
        self.ordinary_release_not_before: float | None = None
        self.system_recovery_revision: int = 0
        self.system_recovery: (system_recovery_state.ReplicaSystemRecovery |
                               None) = None
        self.system_recovery_quarantine: (
            system_recovery_state.SystemRecoveryQuarantine | None) = None

    def to_storage_dict(self) -> dict[str, Any]:
        """Serialize control-plane state into the versioned JSON contract."""
        replica_state = vars(self)
        record_version = replica_state.get('_version')
        if (isinstance(record_version, bool) or
                not isinstance(record_version, int)):
            raise AttributeError(
                'ReplicaInfo is missing a valid record version.')
        if record_version not in (17, self._VERSION):
            raise ValueError('Only ReplicaInfo v17 collision records and '
                             f'v{self._VERSION} records are writable; got '
                             f'v{record_version}.')
        _require_replica_info_fields(self,
                                     owner=f'ReplicaInfo v{record_version}')

        # The precursor writer accepts only a complete v17 collision object or
        # an exact v18 object. Both versions own the complete recovery bundle.
        present_recovery_fields = {
            field for field in V13_ADDITIVE_STORAGE_FIELDS
            if field in vars(self)
        }
        if present_recovery_fields != set(V13_ADDITIVE_STORAGE_FIELDS):
            raise system_recovery_state.RecoveryStateError(
                f'ReplicaInfo v{record_version} has a partial recovery bundle.')
        if record_version == 17:
            self._version = self._VERSION
        _require_replica_info_fields(self,
                                     owner=f'ReplicaInfo v{self._version}')
        _validate_system_recovery_fields(self)

        status_property = self.status_property
        logical_retirement_committed = (
            status_property.logical_retirement_committed)
        if type(logical_retirement_committed) is not bool:
            logical_retirement_committed = None
        drain_started_at = status_property.drain_started_at
        if not _is_valid_drain_started_at(drain_started_at):
            drain_started_at = None
        location = _encode_replica_resource_state(self.location)
        resources_override = _encode_replica_resource_state(
            self.resources_override)
        if resources_override is not None:
            cloud = resources_override.get('cloud')
            if cloud is not None and not isinstance(cloud, str):
                # Placer-pinned overrides carry a Cloud instance. The recovery
                # path accepts its registry name and reconstructs the object.
                resources_override['cloud'] = str(cloud)

        launch_intent = self.system_recovery_launch_intent
        disposition = self.system_recovery_disposition
        recovery = self.system_recovery
        quarantine = self.system_recovery_quarantine
        reserved_fill = _exact_reserved_fill_marker(self.reserved_fill)
        _validate_reserved_fill_allocation_attribution(self)

        def _process_status_value(
            status: common_utils.ProcessStatus | None,) -> str | None:
            return status.value if status is not None else None

        return {
            'replica_info_version': self._version,
            'replica_id': self.replica_id,
            'cluster_name': self.cluster_name,
            'version': self.version,
            'replica_port': self.replica_port,
            'created_at': self.created_at,
            'first_not_ready_time': self.first_not_ready_time,
            'first_consecutive_failure_time':
                self.first_consecutive_failure_time,
            'is_spot': self.is_spot,
            'location': location,
            'resources_override': resources_override,
            'planned_capacity': int(self.planned_capacity),
            'unknown_capacity_replacement': bool(
                self.unknown_capacity_replacement),
            'logical_bridge_capacity_verified': bool(
                self.logical_bridge_capacity_verified),
            'reserved_fill': reserved_fill,
            'reserved_fill_pool_key': self.reserved_fill_pool_key,
            'reserved_fill_service_generation':
                self.reserved_fill_service_generation,
            'reserved_fill_physical_cluster_uid':
                self.reserved_fill_physical_cluster_uid,
            'reserved_fill_kubernetes_context':
                self.reserved_fill_kubernetes_context,
            'reserved_fill_allocation_generation':
                self.reserved_fill_allocation_generation,
            'reserved_fill_allocation_input_sha256':
                self.reserved_fill_allocation_input_sha256,
            'reserved_fill_allocation_claim_generation':
                self.reserved_fill_allocation_claim_generation,
            'reserved_fill_reconciliation_gate_generation':
                self.reserved_fill_reconciliation_gate_generation,
            'reserved_fill_reclaim_fleet_bundle_sha256':
                self.reserved_fill_reclaim_fleet_bundle_sha256,
            'reserved_fill_reclaim_policy_revision':
                self.reserved_fill_reclaim_policy_revision,
            'reserved_fill_reclaim_provider_inventory_sha256':
                self.reserved_fill_reclaim_provider_inventory_sha256,
            'reserved_fill_worker_projection_sha256':
                self.reserved_fill_worker_projection_sha256,
            'reserved_fill_observation_generation':
                self.reserved_fill_observation_generation,
            'reserved_fill_observation_sequence':
                self.reserved_fill_observation_sequence,
            'reserved_fill_intent_idempotency_key':
                self.reserved_fill_intent_idempotency_key,
            'zero_cost_admission_sequence': self.zero_cost_admission_sequence,
            'zero_cost_materialization_sequence':
                self.zero_cost_materialization_sequence,
            'is_zero_cost': self.is_zero_cost,
            'cost_rebalance_for_replica_id': self.cost_rebalance_for_replica_id,
            'paid_capacity_pool_key': self.paid_capacity_pool_key,
            'replica_record_id': self.replica_record_id,
            'system_recovery_launch_intent':
                (launch_intent.to_dict() if launch_intent is not None else None
                ),
            'system_recovery_disposition': disposition.value,
            'launch_request_id': self.launch_request_id,
            'service_job_id': self.service_job_id,
            'candidate_ready_observed_at': self.candidate_ready_observed_at,
            'ordinary_release_not_before': self.ordinary_release_not_before,
            'system_recovery_revision': self.system_recovery_revision,
            'system_recovery':
                (recovery.to_dict() if recovery is not None else None),
            'system_recovery_quarantine':
                (quarantine.to_dict() if quarantine is not None else None),
            'status_property': {
                'sky_launch_status': _process_status_value(
                    status_property.sky_launch_status),
                'user_app_failed': status_property.user_app_failed,
                'service_ready_now': status_property.service_ready_now,
                'first_ready_time': status_property.first_ready_time,
                'sky_down_status': _process_status_value(
                    status_property.sky_down_status),
                'is_scale_down': status_property.is_scale_down,
                'preempted': status_property.preempted,
                'purged': status_property.purged,
                'failed_spot_availability':
                    status_property.failed_spot_availability,
                'drain_cap_seconds': status_property.drain_cap_seconds,
                'drain_started_at': drain_started_at,
                'wait_for_idle_before_termination': bool(
                    status_property.wait_for_idle_before_termination),
                'logical_retirement_version':
                    status_property.logical_retirement_version,
                'logical_retirement_controller_epoch':
                    status_property.logical_retirement_controller_epoch,
                'logical_retirement_generation':
                    status_property.logical_retirement_generation,
                'logical_retirement_target_capacity':
                    status_property.logical_retirement_target_capacity,
                'logical_retirement_confirmed_generation':
                    status_property.logical_retirement_confirmed_generation,
                'logical_retirement_bounded_deadline':
                    (status_property.logical_retirement_bounded_deadline
                     is True),
                'logical_retirement_committed': logical_retirement_committed,
            },
        }

    @classmethod
    def from_storage_dict(cls, state: dict[str, Any]) -> 'ReplicaInfo':
        """Reconstruct a replica from the JSON storage contract."""
        if not isinstance(state, dict):
            raise ValueError('ReplicaInfo storage record must be a dict.')
        record_version = state.get('replica_info_version')
        if type(record_version) is not int:
            raise ValueError('ReplicaInfo storage version must be an integer.')
        if record_version == cls._VERSION:
            _require_current_storage_fields(state)
        elif record_version == 17:
            _require_v17_collision_storage_fields(state)
        elif record_version in _SUPPORTED_LEGACY_JSON_VERSIONS:
            _require_legacy_json_storage_fields(state, record_version)
        else:
            raise ValueError(
                'Unsupported ReplicaInfo storage version; readable versions '
                f'are {sorted(_SUPPORTED_LEGACY_JSON_VERSIONS)}, 17, and '
                f'{cls._VERSION}; got {record_version!r}.')
        status_state = state['status_property']

        def _status_value(field: str) -> Any:
            if record_version in (17, cls._VERSION):
                return status_state[field]
            return status_state.get(
                field, _REPLICA_STATUS_PROPERTY_LEGACY_DEFAULTS[field])

        def _process_status(
            value: common_utils.ProcessStatus | str | None,
        ) -> common_utils.ProcessStatus | None:
            if value is None or isinstance(value, common_utils.ProcessStatus):
                return value
            return common_utils.ProcessStatus(value)

        replica = cls.__new__(cls)
        replica._version = record_version
        replica.replica_id = int(state['replica_id'])
        replica.cluster_name = str(state['cluster_name'])
        replica.version = int(state['version'])
        replica.replica_port = str(state['replica_port'])
        replica.created_at = state.get('created_at')
        replica.first_not_ready_time = state.get('first_not_ready_time')
        replica.first_consecutive_failure_time = state.get(
            'first_consecutive_failure_time')
        replica.is_spot = bool(state['is_spot'])
        replica.location = _decode_replica_resource_state(state.get('location'))
        replica.resources_override = _decode_replica_resource_state(
            state.get('resources_override'))
        planned_capacity = state.get('planned_capacity', 1)
        if (isinstance(planned_capacity, bool) or
                not isinstance(planned_capacity, int) or planned_capacity < 1):
            raise ValueError('Stored planned_capacity must be a positive '
                             f'integer. Got: {planned_capacity!r}')
        replica.planned_capacity = planned_capacity
        replica.unknown_capacity_replacement = bool(
            state.get('unknown_capacity_replacement', False))
        replica.logical_bridge_capacity_verified = bool(
            state.get('logical_bridge_capacity_verified', False))
        replica.reserved_fill = _exact_reserved_fill_marker(
            state.get('reserved_fill', False))
        replica.reserved_fill_pool_key = state.get('reserved_fill_pool_key')
        replica.reserved_fill_service_generation = state.get(
            'reserved_fill_service_generation')
        replica.reserved_fill_physical_cluster_uid = state.get(
            'reserved_fill_physical_cluster_uid')
        replica.reserved_fill_kubernetes_context = state.get(
            'reserved_fill_kubernetes_context')
        replica.reserved_fill_allocation_generation = state.get(
            'reserved_fill_allocation_generation')
        replica.reserved_fill_allocation_input_sha256 = state.get(
            'reserved_fill_allocation_input_sha256')
        replica.reserved_fill_allocation_claim_generation = state.get(
            'reserved_fill_allocation_claim_generation')
        replica.reserved_fill_reconciliation_gate_generation = state.get(
            'reserved_fill_reconciliation_gate_generation')
        replica.reserved_fill_reclaim_fleet_bundle_sha256 = state.get(
            'reserved_fill_reclaim_fleet_bundle_sha256')
        replica.reserved_fill_reclaim_policy_revision = state.get(
            'reserved_fill_reclaim_policy_revision')
        replica.reserved_fill_reclaim_provider_inventory_sha256 = state.get(
            'reserved_fill_reclaim_provider_inventory_sha256')
        replica.reserved_fill_worker_projection_sha256 = state.get(
            'reserved_fill_worker_projection_sha256')
        replica.reserved_fill_observation_generation = state.get(
            'reserved_fill_observation_generation')
        replica.reserved_fill_observation_sequence = state.get(
            'reserved_fill_observation_sequence')
        replica.reserved_fill_intent_idempotency_key = state.get(
            'reserved_fill_intent_idempotency_key')
        replica.zero_cost_admission_sequence = state.get(
            'zero_cost_admission_sequence')
        replica.zero_cost_materialization_sequence = state.get(
            'zero_cost_materialization_sequence')
        raw_is_zero_cost = state.get('is_zero_cost', False)
        if type(raw_is_zero_cost) is not bool:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'Stored is_zero_cost must be an exact boolean.')
        replica.is_zero_cost = raw_is_zero_cost
        replica.cost_rebalance_for_replica_id = state.get(
            'cost_rebalance_for_replica_id')
        replica.paid_capacity_pool_key = state.get('paid_capacity_pool_key')
        if record_version < 13:
            _set_ordinary_system_recovery_defaults(replica)
            _set_transition_replica_record_id(replica)
        else:
            _set_ordinary_system_recovery_defaults(replica)
            try:
                replica.replica_record_id = state['replica_record_id']
                intent_state = state['system_recovery_launch_intent']
                replica.system_recovery_launch_intent = (
                    system_recovery_state.SystemRecoveryLaunchIntent.from_dict(
                        intent_state) if intent_state is not None else None)
                replica.system_recovery_disposition = (
                    system_recovery_state.SystemRecoveryDisposition(
                        state['system_recovery_disposition']))
                replica.launch_request_id = state['launch_request_id']
                replica.service_job_id = state['service_job_id']
                replica.candidate_ready_observed_at = state[
                    'candidate_ready_observed_at']
                replica.ordinary_release_not_before = state[
                    'ordinary_release_not_before']
                replica.system_recovery_revision = state[
                    'system_recovery_revision']
                nested_state = state['system_recovery']
                replica.system_recovery = (
                    system_recovery_state.ReplicaSystemRecovery.from_dict(
                        nested_state) if nested_state is not None else None)
                quarantine_state = state['system_recovery_quarantine']
                replica.system_recovery_quarantine = (
                    system_recovery_state.SystemRecoveryQuarantine.from_dict(
                        quarantine_state)
                    if quarantine_state is not None else None)
            except (system_recovery_state.RecoveryStateError, TypeError,
                    ValueError):
                _quarantine_system_recovery(
                    replica, system_recovery_state.RecoveryQuarantineReason.
                    MALFORMED_V13_BUNDLE)
            else:
                try:
                    _validate_system_recovery_fields(replica)
                except system_recovery_state.RecoveryStateError:
                    _quarantine_system_recovery(
                        replica, system_recovery_state.RecoveryQuarantineReason.
                        INCONSISTENT_V13_BUNDLE)
        drain_started_at = _status_value('drain_started_at')
        logical_retirement_committed = _status_value(
            'logical_retirement_committed')
        replica.status_property = ReplicaStatusProperty(
            sky_launch_status=typing.cast(
                common_utils.ProcessStatus,
                _process_status(_status_value('sky_launch_status'))),
            user_app_failed=bool(_status_value('user_app_failed')),
            service_ready_now=bool(_status_value('service_ready_now')),
            first_ready_time=_status_value('first_ready_time'),
            sky_down_status=_process_status(_status_value('sky_down_status')),
            is_scale_down=bool(_status_value('is_scale_down')),
            preempted=bool(_status_value('preempted')),
            purged=bool(_status_value('purged')),
            failed_spot_availability=bool(
                _status_value('failed_spot_availability')),
            drain_cap_seconds=_status_value('drain_cap_seconds'),
            drain_started_at=(drain_started_at
                              if _is_valid_drain_started_at(drain_started_at)
                              else None),
            wait_for_idle_before_termination=bool(
                _status_value('wait_for_idle_before_termination')),
            logical_retirement_version=_status_value(
                'logical_retirement_version'),
            logical_retirement_controller_epoch=_status_value(
                'logical_retirement_controller_epoch'),
            logical_retirement_generation=_status_value(
                'logical_retirement_generation'),
            logical_retirement_target_capacity=_status_value(
                'logical_retirement_target_capacity'),
            logical_retirement_confirmed_generation=_status_value(
                'logical_retirement_confirmed_generation'),
            logical_retirement_bounded_deadline=(
                _status_value('logical_retirement_bounded_deadline') is True),
            logical_retirement_committed=(logical_retirement_committed
                                          if type(logical_retirement_committed)
                                          is bool else None),
        )
        quarantine = replica.system_recovery_quarantine
        if quarantine is not None:
            # Isolate only this row. Logging is intentionally owned by the
            # runtime storage boundary; this decoder is also used by secret-
            # safe maintenance operations and must remain side-effect free.
            replica.status_property.service_ready_now = False
            replica.first_consecutive_failure_time = None
            if replica.status_property.sky_down_status is None:
                replica.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.SUCCEEDED)
                replica.status_property.user_app_failed = True
        if record_version in _SUPPORTED_LEGACY_JSON_VERSIONS:
            _materialize_legacy_replica_info_fields(replica)
            replica._version = cls._VERSION
        replica.non_pool_launch_authorization = None
        _require_replica_info_fields(
            replica, owner=f'decoded ReplicaInfo v{record_version}')
        _validate_reserved_fill_allocation_attribution(replica)
        return replica

    def get_spot_location(self) -> spot_placer.Location | None:
        return spot_placer.Location.from_pickleable(self.location)

    def handle(
        self,
        cluster_record: dict[str, Any] | None = None
    ) -> backends.CloudVmRayResourceHandle | None:
        """Get the handle of the cluster.

        Args:
            cluster_record: The cluster record in the cluster table. If not
                provided, will fetch the cluster record from the cluster table
                based on the cluster name.
        """
        if cluster_record is None:
            handle = global_user_state.get_handle_from_cluster_name(
                self.cluster_name)
        else:
            handle = cluster_record['handle']
        if handle is None:
            return None
        assert isinstance(handle, backends.CloudVmRayResourceHandle)
        return handle

    @property
    def is_terminal(self) -> bool:
        return self.status in serve_state.ReplicaStatus.terminal_statuses()

    @property
    def is_ready(self) -> bool:
        return (not self._system_recovery_forces_off_route() and
                self.status == serve_state.ReplicaStatus.READY)

    def _system_recovery_forces_off_route(self) -> bool:
        if self.system_recovery_quarantine is not None:
            return True
        disposition = self.system_recovery_disposition
        if disposition == system_recovery_state.SystemRecoveryDisposition.CANDIDATE:
            return True
        recovery = self.system_recovery
        return (
            disposition
            == system_recovery_state.SystemRecoveryDisposition.CAPABLE and
            recovery is not None and recovery.state
            in (system_recovery_state.ControllerRecoveryState.RECOVERING,
                system_recovery_state.ControllerRecoveryState.RETRY_SUBMITTED,
                system_recovery_state.ControllerRecoveryState.EXHAUSTED))

    def _resolve_url(
        self,
        cluster_record: Any = _NOT_PROVIDED,
        handle: backends.CloudVmRayResourceHandle | None = None,
        provider_config: dict[str, Any] | None = None,
    ) -> str | None:
        # Imported lazily to break replica_info -> reserved_capacity ->
        # serve_state -> replica_info during module initialization. Endpoint
        # resolution is a provider operation and must reconstruct its physical
        # authority from the durable row, never from caller-supplied context.
        # pylint: disable-next=import-outside-toplevel
        from sky.serve import reserved_capacity

        cleanup_fence = reserved_capacity.parse_protocol_v2_cleanup_fence(self)
        if handle is None:
            if cluster_record is _NOT_PROVIDED:
                handle = global_user_state.get_handle_from_cluster_name(
                    self.cluster_name)
            elif cluster_record is None:
                handle = None
            else:
                if cleanup_fence is not None:
                    if not isinstance(cluster_record, dict):
                        raise exceptions.KubernetesPhysicalClusterIdentityError(
                            'The durable replica cluster record is malformed.')
                    record_name = cluster_record.get('name')
                    if (record_name is not None and
                            record_name != self.cluster_name):
                        raise exceptions.KubernetesPhysicalClusterIdentityError(
                            'The durable replica cluster record was replaced.')
                    handle = cluster_record.get('handle')
                else:
                    handle = self.handle(cluster_record)
        provider_fence = reserved_capacity.protocol_v2_provider_fence(
            self, handle)
        if handle is None:
            return None
        if self.replica_port == '-':
            # This is a pool replica so there is no endpoint and it's filled
            # with this dummy value. We return None here so that we can
            # get the active ready replicas and perform autoscaling. Otherwise,
            # would error out when trying to get the endpoint.
            return None
        replica_port_int = int(self.replica_port)
        endpoint_kwargs = {}
        if (cluster_record is not _NOT_PROVIDED and cluster_record is not None):
            endpoint_kwargs['cluster_record'] = cluster_record
        if provider_config is not None:
            endpoint_kwargs['provider_config'] = provider_config
        try:
            with provider_fence:
                endpoint_dict = backend_utils.get_endpoints(
                    self.cluster_name, replica_port_int, **endpoint_kwargs)
        except exceptions.ClusterNotUpError:
            return None
        endpoint = endpoint_dict.get(replica_port_int, None)
        if not endpoint:
            return None
        assert isinstance(endpoint, str), endpoint
        # If replica doesn't start with http or https, add the configured
        # scheme. The LB reaches replicas over public IPs across clouds and
        # regions, so this hop is https whenever replica TLS is enabled.
        if not endpoint.startswith('http'):
            scheme = ('https' if serve_utils.replica_tls_mode()
                      != serve_constants.REPLICA_TLS_MODE_OFF else 'http')
            endpoint = f'{scheme}://{endpoint}'
        return endpoint

    @property
    def url(self) -> str | None:
        return self._resolve_url()

    @property
    def status(self) -> serve_state.ReplicaStatus:
        replica_status = self.status_property.to_replica_status()
        if (self._system_recovery_forces_off_route() and
                replica_status == serve_state.ReplicaStatus.READY):
            if self.status_property.first_ready_time is None:
                return serve_state.ReplicaStatus.STARTING
            return serve_state.ReplicaStatus.NOT_READY
        return replica_status

    def to_info_dict(
            self,
            with_handle: bool,
            with_url: bool = True,
            cluster_record: Any = _NOT_PROVIDED,
            rate_cache: dict[str, float] | None = None) -> dict[str, Any]:
        """Build the dashboard/CLI view dict for this replica.

        Args:
            with_handle: include the (pickled) ResourceHandle and derived
                cloud/region/resources_str fields.
            with_url: resolve the replica endpoint via ``self.url`` (does a
                cluster lookup itself). Off for pool views.
            cluster_record: optional pre-fetched record from
                ``global_user_state.get_cluster_from_name`` /
                ``get_clusters_from_names``. Pass to avoid the per-replica
                DB round-trip when iterating many replicas. Use
                ``_NOT_PROVIDED`` (the default) to fall back to the
                self-fetch path for backward compatibility (e.g. ``__repr__``
                still works without changes).
            rate_cache: optional per-status-request pricing cache shared by
                replicas with identical launched resources.
        """
        # Local import avoids the replica_info -> reserved_capacity ->
        # serve_state import cycle. Presentation must isolate a stale physical
        # target to this row and must never publish a replacement handle's
        # endpoint, cost, or provider metadata.
        # pylint: disable-next=import-outside-toplevel
        from sky.serve import reserved_capacity

        provider_identity_uncertain = False
        try:
            cleanup_fence = (
                reserved_capacity.parse_protocol_v2_cleanup_fence(self))
        except exceptions.KubernetesPhysicalClusterIdentityError:
            cleanup_fence = None
            provider_identity_uncertain = True
        if cluster_record is _NOT_PROVIDED:
            cluster_record = global_user_state.get_cluster_from_name(
                self.cluster_name,
                include_user_info=False,
                summary_response=True)
        # Resolve the handle once. When the cluster row is missing, the
        # handle is also missing (they live in the same row), so
        # short-circuit to avoid an extra DB lookup.
        if cluster_record is None or provider_identity_uncertain:
            handle = None
            if cleanup_fence is not None:
                provider_identity_uncertain = True
        elif cleanup_fence is not None:
            handle = (cluster_record.get('handle') if isinstance(
                cluster_record, dict) else None)
            try:
                reserved_capacity.protocol_v2_provider_fence(self, handle)
            except exceptions.KubernetesPhysicalClusterIdentityError:
                provider_identity_uncertain = True
                cluster_record = None
                handle = None
        else:
            handle = self.handle(cluster_record)
        created_at = self.created_at
        ready_at = self.status_property.first_ready_time
        # ``-1`` is the persisted sentinel for an exhausted initial-delay
        # window, not a successful readiness probe.
        if ready_at is not None and ready_at < 0:
            ready_at = None
        time_to_ready_seconds = None
        if (created_at is not None and ready_at is not None and
                ready_at >= created_at):
            # End-to-end launch latency: replica row creation -> first
            # successful readiness probe. This includes placement queueing,
            # cloud provisioning, setup, and application startup.
            time_to_ready_seconds = ready_at - created_at
        endpoint = None
        if with_url and not provider_identity_uncertain:
            try:
                endpoint = self._resolve_url(cluster_record=cluster_record,
                                             handle=handle)
            except exceptions.KubernetesPhysicalClusterIdentityError:
                provider_identity_uncertain = True
                cluster_record = None
                handle = None
        info_dict = {
            'replica_id': self.replica_id,
            'name': self.cluster_name,
            'status': (serve_state.ReplicaStatus.UNKNOWN
                       if provider_identity_uncertain else self.status),
            'version': self.version,
            'replica_info_version': self._version,
            # Immutable logical width selected when this physical backend was
            # placed. It is one for ordinary and legacy physical replicas.
            'planned_capacity': int(self.planned_capacity),
            'endpoint': endpoint,
            'is_spot': self.is_spot,
            'launched_at': (cluster_record['launched_at']
                            if cluster_record is not None else None),
            'ready_at': ready_at,
            'time_to_ready_seconds': time_to_ready_seconds,
            # Additive evidence for status consumers: UNKNOWN here means the
            # durable physical pool identity could not be proved, not an
            # ordinary application/readiness failure.
            'provider_identity_uncertain': provider_identity_uncertain,
        }
        # Always populate the small derived strings — new clients read
        # these instead of touching the handle, and the cost is just a
        # dict lookup + isinstance on a cluster_record we already have.
        if handle is not None and handle.launched_resources is not None:
            info_dict['cloud'] = repr(handle.launched_resources.cloud)
            info_dict['region'] = handle.launched_resources.region
            hourly_cost, exclusion_reason = (
                estimated_spend.estimate_hourly_cost(handle.launched_resources,
                                                     handle.launched_nodes,
                                                     rate_cache))
            info_dict['hourly_cost'] = hourly_cost
            info_dict['hourly_cost_exclusion_reason'] = exclusion_reason
            simple, full = resources_utils.get_readable_resources_repr(
                handle, simplified_only=False)
            info_dict['resources_str'] = simple
            info_dict['resources_str_full'] = (full
                                               if full is not None else simple)
            info_dict['infra'] = handle.launched_resources.infra.formatted_str()
        else:
            # A placer-selected location exists before the replica has a
            # cluster handle, including while it is PENDING or early
            # PROVISIONING. Publish it through the existing placement fields
            # so status consumers can account for every replica by
            # cloud/region. Avoid reconstructing it for launched replicas,
            # whose resources above are authoritative.
            location = self.get_spot_location()
            if location is not None:
                cloud = repr(location.cloud)
                info_dict['cloud'] = cloud
                info_dict['region'] = location.region
                info_dict['infra'] = f'{cloud} ({location.region})'
        if with_handle:
            info_dict['handle'] = handle
        return info_dict

    def __repr__(self) -> str:
        show_details = env_options.Options.SHOW_DEBUG_INFO.get()
        info_dict = self.to_info_dict(with_handle=show_details,
                                      with_url=show_details)
        handle_str = ''
        if 'handle' in info_dict:
            handle_str = f', handle={info_dict["handle"]}'
        info = (f'ReplicaInfo(replica_id={self.replica_id}, '
                f'cluster_name={self.cluster_name}, '
                f'version={self.version}, '
                f'replica_port={self.replica_port}, '
                f'is_spot={self.is_spot}, '
                f'location={self.location}, '
                f'status={self.status}, '
                f'launched_at={info_dict["launched_at"]}{handle_str})')
        return info

    def probe_pool(
        self,
        *,
        provider_phase_admission: provider_phase.ProviderPhaseAdmission |
        None = None,
    ) -> tuple['ReplicaInfo', bool, float]:
        """Probe the replica for pool management.

        This function will check the first job status of the cluster, which is a
        dummy job that only echoes "setup done". The success of this job means
        the setup command is done and the replica is ready to be used. Check
        sky/serve/server/core.py::up for more details.

        Returns:
            Tuple of (self, is_ready, probe_time).
        """
        probe_time = time.time()
        try:
            # See _resolve_url for the import-cycle rationale.
            # pylint: disable-next=import-outside-toplevel
            from sky.serve import reserved_capacity
            durable_handle = global_user_state.get_handle_from_cluster_name(
                self.cluster_name)
            with reserved_capacity.protocol_v2_provider_fence(
                    self, durable_handle,
                    phase_admission=provider_phase_admission):
                handle = backend_utils.check_cluster_available(
                    self.cluster_name, operation='probing pool')
                if handle is None:
                    return self, False, probe_time
                with reserved_capacity.protocol_v2_provider_fence(
                        self, handle, phase_admission=provider_phase_admission):
                    backend = backend_utils.get_backend_from_handle(handle)
                    statuses = backend.get_job_status(handle, [1],
                                                      stream_logs=False)
            if statuses[1] == job_lib.JobStatus.SUCCEEDED:
                return self, True, probe_time
            return self, False, probe_time
        except (exceptions.KubernetesPhysicalClusterIdentityError,
                exceptions.ProviderPhaseError):
            # Provider-phase admission failure is no readiness observation.
            # The replica manager defers this exact row and keeps healthy
            # peers progressing instead of converting contention into a
            # negative probe sample.
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Error when probing pool of {self.cluster_name}: '
                         f'{common_utils.format_exception(e)}.')
            return self, False, probe_time

    def probe(
        self,
        readiness_path: str,
        post_data: dict[str, Any] | None,
        timeout: int,
        headers: dict[str, str] | None,
        resolved_url: Any = _NOT_PROVIDED,
        request_started_callback: typing.Callable[[float], None] | None = None,
    ) -> tuple['ReplicaInfo', bool, float]:
        """Probe the readiness of the replica.

        Returns:
            Tuple of (self, is_ready, probe_time).
        """
        url = self.url if resolved_url is _NOT_PROVIDED else resolved_url
        assert url is None or isinstance(url, str), url
        replica_identity = f'replica {self.replica_id} with url {url}'
        # TODO(tian): This requiring the clock on each replica to be aligned,
        # which may not be true when the GCP VMs have run for a long time. We
        # should have a better way to do this. See #2539 for more information.
        probe_time = time.time()
        try:
            msg = ''
            if url is None:
                logger.info(f'Error when probing {replica_identity}: '
                            'Cannot get the endpoint.')
                return self, False, probe_time
            readiness_path = (f'{url}{readiness_path}')
            logger.info(f'Probing {replica_identity} with {readiness_path}.')
            # This probe is a second, independent client on the same hop as the
            # load balancer's proxy. It decides readiness, so if it cannot
            # complete the TLS handshake every healthy replica is marked
            # NOT_READY and the controller tears down live capacity. It must
            # therefore trust exactly what the proxy trusts.
            # With TLS off this IS the `requests` module, so the default
            # path is unchanged; under TLS it is a session carrying the same
            # trust the proxy uses.
            client = replica_tls.probe_client()
            # Recovery route issuance needs the exact local start of the HTTP
            # request, not the submitting thread's queue time or wall clock.
            # Keep the ordinary three-value return contract unchanged and
            # expose this only through the manager-owned callback.
            request_started_at = time.monotonic()
            if request_started_callback is not None:
                request_started_callback(request_started_at)
            if post_data is not None:
                msg += 'POST'
                response = client.post(readiness_path,
                                       json=post_data,
                                       headers=headers,
                                       timeout=timeout)
            else:
                msg += 'GET'
                response = client.get(readiness_path,
                                      headers=headers,
                                      timeout=timeout)
            msg += (f' request to {replica_identity} returned status '
                    f'code {response.status_code}')
            if response.status_code == 200:
                msg += '.'
                log_method = logger.info
            else:
                msg += f' and response {response.text}.'
                msg = f'{colorama.Fore.YELLOW}{msg}{colorama.Style.RESET_ALL}'
                log_method = logger.error
            log_method(msg)
            if response.status_code == 200:
                logger.debug(f'{replica_identity.capitalize()} is ready.')
                return self, True, probe_time
        except Exception as e:  # pylint: disable=broad-except
            # Catch all errors, not just RequestException: probe inputs
            # (readiness path/headers/post data) come from user YAML and can
            # make the HTTP stack raise e.g. UnicodeEncodeError or ValueError.
            # An escaping exception aborts the whole probe round when the
            # prober drains futures, stalling status updates for every
            # replica on each tick.
            logger.error(
                f'{colorama.Fore.YELLOW}Error when probing {replica_identity}:'
                f' {common_utils.format_exception(e)}.'
                f'{colorama.Style.RESET_ALL}')
        return self, False, probe_time

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Set state from pickled state, for backward compatibility."""
        version = state.pop('_version', None)
        # Handle old version(s) here.
        if version is None:
            version = -1
        if isinstance(version, bool) or not isinstance(version, int):
            raise ValueError('Pickled ReplicaInfo version must be an integer.')
        if version == self._VERSION:
            _require_current_pickle_fields(state, version)

        if version < 0:
            # It will be handled with RequestRateAutoscaler.
            # Treated similar to on-demand instances.
            self.is_spot = False

        if version < 1:
            self.location = None

        if version < 2:
            self.resources_override = None

        if version < 4:
            # Pre-upgrade rows carry no creation time. None deliberately
            # reads as "older than any fill snapshot" in
            # Autoscaler._fill_row_occupies_free_slot: these rows predate
            # the build, their bound pods are already excluded by fresh
            # polls, and treating them as new would debit free slots for
            # their whole lifetime.
            self.created_at = None

        if version < 5:
            # Pre-broker rows carry no launch-origin flag. False reads as
            # demand-placed: they keep their scale-down shelter and stay
            # exempt from the broker's grant ceiling until natural churn
            # replaces them with flagged rows -- the conservative
            # direction for a live fleet crossing the upgrade.
            self.reserved_fill = False

        if version < 11:
            # Old rows do not contain authoritative cost provenance. False is
            # conservative: it preserves correctness and only forgoes the new
            # economic tie-break until natural replacement.
            self.is_zero_cost = False

        if version < self._VERSION:
            state.setdefault('cost_rebalance_for_replica_id', None)
            state.setdefault('paid_capacity_pool_key', None)
            state.setdefault('reserved_fill_pool_key', None)
            state.setdefault('reserved_fill_service_generation', None)
            state.setdefault('reserved_fill_physical_cluster_uid', None)
            state.setdefault('reserved_fill_kubernetes_context', None)
            state.setdefault('reserved_fill_allocation_generation', None)
            state.setdefault('reserved_fill_allocation_input_sha256', None)
            state.setdefault('reserved_fill_allocation_claim_generation', None)
            state.setdefault('reserved_fill_reconciliation_gate_generation',
                             None)
            state.setdefault('reserved_fill_reclaim_fleet_bundle_sha256', None)
            state.setdefault('reserved_fill_reclaim_policy_revision', None)
            state.setdefault('reserved_fill_reclaim_provider_inventory_sha256',
                             None)
            state.setdefault('reserved_fill_worker_projection_sha256', None)
            state.setdefault('reserved_fill_observation_generation', None)
            state.setdefault('reserved_fill_observation_sequence', None)
            state.setdefault('reserved_fill_intent_idempotency_key', None)
            state.setdefault('zero_cost_admission_sequence', None)
            state.setdefault('zero_cost_materialization_sequence', None)

        if version < 7:
            # Rows written before version 7 carry the full list of failed
            # probe timestamps; only its first entry was ever read (the
            # window is first-failure -> current probe time), so migrate
            # to the single timestamp.
            failure_times = state.pop('consecutive_failure_times', [])
            self.first_consecutive_failure_time = (failure_times[0]
                                                   if failure_times else None)

        if version < 8:
            # Historical rows represent one physical replica. They are never
            # inferred into logical mode during activation; the rolling bridge
            # launches a new logical service version instead.
            self.planned_capacity = 1

        self.__dict__.update(state)
        self.non_pool_launch_authorization = state.get(
            'non_pool_launch_authorization')
        self._version = version if version >= 0 else 0
        if version < self._VERSION:
            _materialize_legacy_replica_info_fields(self)
        elif version == self._VERSION:
            _require_replica_info_fields(self,
                                         owner=f'ReplicaInfo v{version} pickle')

        recovery_fields = set(V13_ADDITIVE_STORAGE_FIELDS)
        present_recovery_fields = recovery_fields.intersection(state)
        if version < 13:
            _set_ordinary_system_recovery_defaults(self)
            _set_transition_replica_record_id(self)
        elif version > self._VERSION:
            _quarantine_system_recovery(
                self, system_recovery_state.RecoveryQuarantineReason.
                INCONSISTENT_V13_BUNDLE)
        elif present_recovery_fields != recovery_fields:
            _quarantine_system_recovery(
                self, system_recovery_state.RecoveryQuarantineReason.
                PARTIAL_V13_BUNDLE)
        else:
            try:
                _validate_system_recovery_fields(self)
            except system_recovery_state.RecoveryStateError:
                _quarantine_system_recovery(
                    self, system_recovery_state.RecoveryQuarantineReason.
                    INCONSISTENT_V13_BUNDLE)
        _require_replica_info_fields(
            self, owner=f'decoded ReplicaInfo v{version} pickle')
        _validate_reserved_fill_allocation_attribution(self)
        if self.system_recovery_quarantine is not None:
            self.status_property.service_ready_now = False
            self.first_consecutive_failure_time = None
            if self.status_property.sky_down_status is None:
                self.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.SUCCEEDED)
                self.status_property.user_app_failed = True


# Keep historical import and pickle identities even when this implementation
# module is imported before the replica_managers facade.
for _public_symbol in (
        _is_valid_drain_started_at,
        ReplicaStatusProperty,
        _encode_replica_resource_state,
        _decode_replica_resource_state,
        ReplicaInfo,
):
    _public_symbol.__module__ = 'sky.serve.replica_managers'
del _public_symbol
