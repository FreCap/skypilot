"""Versioned state and behavior for one SkyServe replica."""
import dataclasses
import math
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
# Exact set added by v13. A row missing all ten keys is the one supported
# rollback shape; any nonempty proper subset is quarantined.
V13_ADDITIVE_STORAGE_FIELDS = ('replica_record_id',
                               *SYSTEM_RECOVERY_STORAGE_FIELDS)

# A fixed namespace makes the one supported v12/rollback transition identity
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
    replica_id = getattr(replica, 'replica_id', None)
    cluster_name = getattr(replica, 'cluster_name', None)
    created_at = getattr(replica, 'created_at', None)
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
        _canonical_replica_record_id(getattr(replica, 'replica_record_id',
                                             None))
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
    # and failure teardowns, and on rows written before this field
    # existed (read via getattr for unpickle back-compat).
    drain_cap_seconds: int | None = None
    # Wall-clock epoch seconds at which the bounded drain first became
    # durable. Unlike time.monotonic(), this survives controller restarts and
    # prevents repeated recovery from restarting the full drain cap. None for
    # unbounded/immediate cleanup and rows written before this field existed.
    drain_started_at: float | None = None
    # Economic replacement is fail-closed: persist the off-route retirement
    # intent, but do not admit sky.down until a fresh LB report proves zero
    # occupancy.  getattr is used for rows predating this field.
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


def _encode_replica_resource_state(
        state: dict[str, Any] | None) -> dict[str, Any] | None:
    """Makes a location/resources override lossless in a JSON object.

    ``Resources.image_id`` is keyed by a region or by ``None`` for a
    region-independent image. JSON object keys cannot represent ``None``:
    PostgreSQL JSONB reads it back as the string ``"null"``. Store this one
    nested mapping as key/value pairs so its key types survive the round trip.
    """
    if state is None:
        return None
    encoded = dict(state)
    image_id = encoded.get('image_id')
    if isinstance(image_id, dict):
        encoded['image_id'] = [
            [region, image] for region, image in image_id.items()
        ]
    return encoded


def _decode_replica_resource_state(
        state: dict[str, Any] | None) -> dict[str, Any] | None:
    """Restores the internal location/resources override representation."""
    if state is None:
        return None
    decoded = dict(state)
    image_id = decoded.get('image_id')
    if isinstance(image_id, list):
        restored_image_id = {}
        for item in image_id:
            if not isinstance(item, list) or len(item) != 2:
                raise ValueError('Invalid replica image_id storage state: '
                                 f'{image_id!r}')
            restored_image_id[item[0]] = item[1]
        decoded['image_id'] = restored_image_id
    elif isinstance(image_id, dict) and 'null' in image_id:
        # Compatibility for version-1 rows written before image_id mappings
        # used a lossless representation. The JSON encoder coerced a None key
        # to the literal string "null".
        decoded['image_id'] = {
            None if region == 'null' else region: image
            for region, image in image_id.items()
        }
    return decoded


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
    destination_revision = getattr(destination, 'system_recovery_revision', 0)
    source_record_id = source.replica_record_id
    destination_record_id = getattr(destination, 'replica_record_id', None)
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
        setattr(destination, field, getattr(source, field))
    destination.system_recovery_revision = (
        system_recovery_state.next_recovery_revision(destination_revision)
        if increment_revision else source_revision)
    setattr(destination, '_version', max(getattr(destination, '_version', 0),
                                         13))
    _validate_system_recovery_fields(destination)


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
    _VERSION = 13

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
        self.location: dict[str, str | None] | None = (
            location.to_pickleable() if location is not None else None)
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
        # exemption both key on it. A fill row re-driven after a
        # controller crash mid-PENDING keeps the flag: the sentinel was
        # consumed at original emission, so the recovery path carries the
        # prior row's attribution into _launch_replica explicitly
        # (prior_reserved_fill) -- otherwise the replacement row would
        # read as demand-placed and stay ceiling-exempt for its lifetime.
        self.reserved_fill: bool = False
        # Placement-cost provenance, not launch intent. True means the
        # replica occupies capacity the placer classifies as zero cost.
        self.is_zero_cost: bool = False
        # Incumbent id this replica was launched to replace economically.
        # None for ordinary demand/fill launches.
        self.cost_rebalance_for_replica_id: int | None = None
        # Exact provider capacity pool whose unresolved-launch allowance this
        # row consumes. None for zero-cost, recovery-only, and pre-v12 rows.
        self.paid_capacity_pool_key: str | None = None
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
        status_property = self.status_property
        # getattr() is insufficient for old pickles. If a pre-field dataclass
        # instance lacks this key, attribute lookup falls through to the new
        # class-level default False and destroys the missing-vs-uncommitted
        # distinction needed to recover an ambiguous SCHEDULED teardown.
        logical_retirement_committed = vars(status_property).get(
            'logical_retirement_committed')
        if type(logical_retirement_committed) is not bool:
            logical_retirement_committed = None
        drain_started_at = getattr(status_property, 'drain_started_at', None)
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

        # Old pickles have no v13 attributes and are ordinary by contract.
        # Any other partial in-memory bundle is isolated rather than silently
        # becoming routable ordinary state.
        present_recovery_fields = {
            field for field in V13_ADDITIVE_STORAGE_FIELDS
            if hasattr(self, field)
        }
        if getattr(self, '_version', 0) < 13:
            _set_ordinary_system_recovery_defaults(self)
            _set_transition_replica_record_id(self)
            self._version = 13
        elif (not present_recovery_fields and
              getattr(self, '_version', 0) == 13):
            _set_ordinary_system_recovery_defaults(self)
            _set_transition_replica_record_id(self)
        elif present_recovery_fields != set(V13_ADDITIVE_STORAGE_FIELDS):
            _quarantine_system_recovery(
                self, system_recovery_state.RecoveryQuarantineReason.
                PARTIAL_V13_BUNDLE)
        _validate_system_recovery_fields(self)
        launch_intent = self.system_recovery_launch_intent
        disposition = self.system_recovery_disposition
        recovery = self.system_recovery
        quarantine = self.system_recovery_quarantine

        def _process_status_value(
            status: common_utils.ProcessStatus | None,) -> str | None:
            return status.value if status is not None else None

        return {
            'replica_info_version': self._version,
            'replica_id': self.replica_id,
            'cluster_name': self.cluster_name,
            'version': self.version,
            'replica_port': self.replica_port,
            'created_at': getattr(self, 'created_at', None),
            'first_not_ready_time': getattr(self, 'first_not_ready_time', None),
            'first_consecutive_failure_time': getattr(
                self, 'first_consecutive_failure_time', None),
            'is_spot': self.is_spot,
            'location': location,
            'resources_override': resources_override,
            'planned_capacity': int(getattr(self, 'planned_capacity', 1)),
            'unknown_capacity_replacement': bool(
                getattr(self, 'unknown_capacity_replacement', False)),
            'logical_bridge_capacity_verified': bool(
                getattr(self, 'logical_bridge_capacity_verified', False)),
            'reserved_fill': bool(getattr(self, 'reserved_fill', False)),
            'is_zero_cost': bool(getattr(self, 'is_zero_cost', False)),
            'cost_rebalance_for_replica_id': getattr(
                self, 'cost_rebalance_for_replica_id', None),
            'paid_capacity_pool_key': getattr(self, 'paid_capacity_pool_key',
                                              None),
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
                'drain_cap_seconds': getattr(status_property,
                                             'drain_cap_seconds', None),
                'drain_started_at': drain_started_at,
                'wait_for_idle_before_termination': bool(
                    getattr(status_property, 'wait_for_idle_before_termination',
                            False)),
                'logical_retirement_version': getattr(
                    status_property, 'logical_retirement_version', None),
                'logical_retirement_controller_epoch': getattr(
                    status_property, 'logical_retirement_controller_epoch',
                    None),
                'logical_retirement_generation': getattr(
                    status_property, 'logical_retirement_generation', None),
                'logical_retirement_target_capacity': getattr(
                    status_property, 'logical_retirement_target_capacity',
                    None),
                'logical_retirement_confirmed_generation': getattr(
                    status_property, 'logical_retirement_confirmed_generation',
                    None),
                'logical_retirement_bounded_deadline':
                    (getattr(status_property,
                             'logical_retirement_bounded_deadline', False)
                     is True),
                'logical_retirement_committed': logical_retirement_committed,
            },
        }

    @classmethod
    def from_storage_dict(cls, state: dict[str, Any]) -> 'ReplicaInfo':
        """Reconstruct a replica from the JSON storage contract."""
        status_state = state['status_property']

        def _process_status(
            value: str | None,) -> common_utils.ProcessStatus | None:
            return (common_utils.ProcessStatus(value)
                    if value is not None else None)

        replica = cls.__new__(cls)
        replica._version = int(state['replica_info_version'])
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
        replica.reserved_fill = bool(state.get('reserved_fill', False))
        replica.is_zero_cost = bool(state.get('is_zero_cost', False))
        replica.cost_rebalance_for_replica_id = state.get(
            'cost_rebalance_for_replica_id')
        replica.paid_capacity_pool_key = state.get('paid_capacity_pool_key')
        recovery_keys = set(V13_ADDITIVE_STORAGE_FIELDS)
        present_recovery_keys = recovery_keys.intersection(state)
        if replica._version < 13:
            _set_ordinary_system_recovery_defaults(replica)
            # Old writers cannot choose or preserve the v13 fence. Ignore an
            # untrusted additive key on an old-labelled row.
            _set_transition_replica_record_id(replica)
        elif replica._version > 13:
            _quarantine_system_recovery(
                replica, system_recovery_state.RecoveryQuarantineReason.
                INCONSISTENT_V13_BUNDLE)
        elif not present_recovery_keys:
            # Exact rollback compatibility: a v12 writer can retain the v13
            # version label while erasing the *entire* additive bundle.
            _set_ordinary_system_recovery_defaults(replica)
            _set_transition_replica_record_id(replica)
        elif present_recovery_keys != recovery_keys:
            _quarantine_system_recovery(
                replica, system_recovery_state.RecoveryQuarantineReason.
                PARTIAL_V13_BUNDLE)
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
        replica.status_property = ReplicaStatusProperty(
            sky_launch_status=typing.cast(
                common_utils.ProcessStatus,
                _process_status(status_state['sky_launch_status'])),
            user_app_failed=bool(status_state['user_app_failed']),
            service_ready_now=bool(status_state['service_ready_now']),
            first_ready_time=status_state.get('first_ready_time'),
            sky_down_status=_process_status(
                status_state.get('sky_down_status')),
            is_scale_down=bool(status_state['is_scale_down']),
            preempted=bool(status_state['preempted']),
            purged=bool(status_state['purged']),
            failed_spot_availability=bool(
                status_state['failed_spot_availability']),
            drain_cap_seconds=status_state.get('drain_cap_seconds'),
            drain_started_at=(status_state.get('drain_started_at')
                              if _is_valid_drain_started_at(
                                  status_state.get('drain_started_at')) else
                              None),
            wait_for_idle_before_termination=bool(
                status_state.get('wait_for_idle_before_termination', False)),
            logical_retirement_version=status_state.get(
                'logical_retirement_version'),
            logical_retirement_controller_epoch=status_state.get(
                'logical_retirement_controller_epoch'),
            logical_retirement_generation=status_state.get(
                'logical_retirement_generation'),
            logical_retirement_target_capacity=status_state.get(
                'logical_retirement_target_capacity'),
            logical_retirement_confirmed_generation=status_state.get(
                'logical_retirement_confirmed_generation'),
            logical_retirement_bounded_deadline=(status_state.get(
                'logical_retirement_bounded_deadline', False) is True),
            logical_retirement_committed=(
                status_state.get('logical_retirement_committed') if type(
                    status_state.get('logical_retirement_committed')) is bool
                else None),
        )
        quarantine = replica.system_recovery_quarantine
        if quarantine is not None:
            # Isolate only this row.  Never include its raw storage payload in
            # the audit record or exception text.
            replica.status_property.service_ready_now = False
            replica.first_consecutive_failure_time = None
            if replica.status_property.sky_down_status is None:
                replica.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.SUCCEEDED)
                replica.status_property.user_app_failed = True
            logger.warning(
                'Quarantined system recovery state for replica %s (%s); '
                'the row remains off-route pending legacy cleanup.',
                replica.replica_id, quarantine.reason.value)
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
        if getattr(self, 'system_recovery_quarantine', None) is not None:
            return True
        disposition = getattr(
            self, 'system_recovery_disposition',
            system_recovery_state.SystemRecoveryDisposition.ORDINARY)
        if disposition == system_recovery_state.SystemRecoveryDisposition.CANDIDATE:
            return True
        recovery = getattr(self, 'system_recovery', None)
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
        if handle is None:
            if cluster_record is _NOT_PROVIDED:
                handle = self.handle()
            elif cluster_record is None:
                return None
            else:
                handle = self.handle(cluster_record)
        if handle is None:
            return None
        if self.replica_port == '-':
            # This is a pool replica so there is no endpoint and it's filled
            # with this dummy value. We return None here so that we can
            # get the active ready replicas and perform autoscaling. Otherwise,
            # would error out when trying to get the endpoint.
            return None
        replica_port_int = int(self.replica_port)
        try:
            endpoint_kwargs = {}
            if (cluster_record is not _NOT_PROVIDED and
                    cluster_record is not None):
                endpoint_kwargs['cluster_record'] = cluster_record
            if provider_config is not None:
                endpoint_kwargs['provider_config'] = provider_config
            endpoint_dict = backend_utils.get_endpoints(self.cluster_name,
                                                        replica_port_int,
                                                        **endpoint_kwargs)
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
        if replica_status == serve_state.ReplicaStatus.UNKNOWN:
            logger.error('Detecting UNKNOWN replica status for '
                         f'replica {self.replica_id}.')
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
        if cluster_record is _NOT_PROVIDED:
            cluster_record = global_user_state.get_cluster_from_name(
                self.cluster_name,
                include_user_info=False,
                summary_response=True)
        # Resolve the handle once. When the cluster row is missing, the
        # handle is also missing (they live in the same row), so
        # short-circuit to avoid an extra DB lookup.
        if cluster_record is None:
            handle = None
        else:
            handle = self.handle(cluster_record)
        created_at = getattr(self, 'created_at', None)
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
        info_dict = {
            'replica_id': self.replica_id,
            'name': self.cluster_name,
            'status': self.status,
            'version': self.version,
            'replica_info_version': self._version,
            # Immutable logical width selected when this physical backend was
            # placed. It is one for ordinary and legacy physical replicas.
            'planned_capacity': int(getattr(self, 'planned_capacity', 1)),
            'endpoint':
                (self._resolve_url(cluster_record=cluster_record, handle=handle)
                 if with_url else None),
            'is_spot': self.is_spot,
            'launched_at': (cluster_record['launched_at']
                            if cluster_record is not None else None),
            'ready_at': ready_at,
            'time_to_ready_seconds': time_to_ready_seconds,
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

    def probe_pool(self) -> tuple['ReplicaInfo', bool, float]:
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
            handle = backend_utils.check_cluster_available(
                self.cluster_name, operation='probing pool')
            if handle is None:
                return self, False, probe_time
            backend = backend_utils.get_backend_from_handle(handle)
            statuses = backend.get_job_status(handle, [1], stream_logs=False)
            if statuses[1] == job_lib.JobStatus.SUCCEEDED:
                return self, True, probe_time
            return self, False, probe_time
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

    def __setstate__(self, state):
        """Set state from pickled state, for backward compatibility."""
        version = state.pop('_version', None)
        # Handle old version(s) here.
        if version is None:
            version = -1

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

        state.setdefault('cost_rebalance_for_replica_id', None)
        state.setdefault('paid_capacity_pool_key', None)

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
        self._version = version if version >= 0 else 0

        recovery_fields = set(V13_ADDITIVE_STORAGE_FIELDS)
        present_recovery_fields = recovery_fields.intersection(state)
        if version < 13:
            _set_ordinary_system_recovery_defaults(self)
            _set_transition_replica_record_id(self)
        elif version > 13:
            _quarantine_system_recovery(
                self, system_recovery_state.RecoveryQuarantineReason.
                INCONSISTENT_V13_BUNDLE)
        elif not present_recovery_fields:
            _set_ordinary_system_recovery_defaults(self)
            _set_transition_replica_record_id(self)
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
