"""ReplicaManager: handles the creation and deletion of endpoint replicas."""
import asyncio
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
import concurrent.futures
import contextlib
import copy
import dataclasses
import enum
import functools
import hashlib
import math
from multiprocessing import pool as mp_pool
import os
import pathlib
import queue
import threading
import time
import traceback
import typing
from typing import Any, Optional
import uuid

import aiohttp

from sky import backends
from sky import clouds
from sky import estimated_spend
from sky import exceptions
from sky import global_user_state
from sky import resources as resources_lib
from sky import sky_logging
from sky import skypilot_config
from sky import task as task_lib
from sky.adaptors import common as adaptors_common
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_backend
from sky.client import sdk
from sky.serve import capacity_admission
from sky.serve import constants as serve_constants
from sky.serve import drain_observability
from sky.serve import non_pool_launch_reconciliation
from sky.serve import ordinary_launch_handoff
from sky.serve import paid_capacity
from sky.serve import paid_retirement
from sky.serve import provider_phase
from sky.serve import replica_info as replica_info_lib
from sky.serve import replica_tls
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_allocation
from sky.serve import reserved_fill_planner
from sky.serve import route_projection
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.serve import system_oom_recovery
from sky.serve import system_oom_recovery_observability
from sky.serve import system_recovery_route_lease
from sky.serve import system_recovery_state
from sky.serve import zero_cost_actuation
from sky.server import common as server_common
from sky.skylet import constants
from sky.skylet import job_lib
from sky.usage import usage_lib
from sky.utils import common_utils
from sky.utils import context
from sky.utils import controller_utils
from sky.utils import env_options
from sky.utils import locks
from sky.utils import resources_utils
from sky.utils import status_lib
from sky.utils import subprocess_utils
from sky.utils import thread_utils
from sky.utils import ux_utils
from sky.utils import yaml_utils

if typing.TYPE_CHECKING:

    from sky.serve import demand_state
    from sky.serve import service_spec
    from sky.serve.ordinary_launch_binding import BoundLaunchContext
    from sky.serve.ordinary_launch_binding import BoundNonPoolLaunchContext
    from sky.serve.ordinary_launch_binding import ControllerBindingAuthority
    from sky.serve.replica_info import ReplicaInfo
    from sky.serve.replica_info import ReplicaStatusProperty
    SpotPlacerType: typing.TypeAlias = spot_placer.SpotPlacer
else:
    ReplicaStatusProperty = replica_info_lib.ReplicaStatusProperty
    ReplicaInfo = replica_info_lib.ReplicaInfo

logger = sky_logging.init_logger(__name__)
ordinary_launch_binding = adaptors_common.LazyImport(
    'sky.serve.ordinary_launch_binding')
request_postgres = adaptors_common.LazyImport('sky.server.requests.postgres')
reserved_fill_admission = adaptors_common.LazyImport(
    'sky.server.requests.reserved_fill_admission')
kueue_lane_observer = adaptors_common.LazyImport(
    'sky.serve.kueue_lane_observer')
requests = adaptors_common.LazyImport('requests')


def _required_controller_admin_auth_tokens() -> tuple[str, ...]:
    """Read the live token ring for controller-to-API launch operations."""
    return serve_utils.get_controller_admin_auth_tokens(required=True)


# Keep the established replica_managers import and pickle identities while the
# versioned record implementation lives in its own low-state module.
replica_info_lib.logger = logger
replica_info_lib.estimated_spend = estimated_spend
replica_info_lib.env_options = env_options
colorama = replica_info_lib.colorama
# pylint: disable=protected-access
_NOT_PROVIDED = replica_info_lib._NOT_PROVIDED
_is_valid_drain_started_at = replica_info_lib._is_valid_drain_started_at
_encode_replica_resource_state = (
    replica_info_lib._encode_replica_resource_state)
_decode_replica_resource_state = (
    replica_info_lib._decode_replica_resource_state)
# pylint: enable=protected-access

_JOB_STATUS_FETCH_INTERVAL = 30
_PROCESS_POOL_REFRESH_INTERVAL = 20
_RETRY_INIT_GAP_SECONDS = 60
# Default number of launch attempts for launch_cluster. Spot replicas with a
# spot placer cap resource availability (capacity) failures at one attempt so
# the placer can fail over to a different location immediately instead of
# re-hammering the same exhausted zone (see _launch_replica).
_DEFAULT_LAUNCH_MAX_RETRY = 3
_DEFAULT_DRAIN_SECONDS = 120
# Poll cadence for the in-flight-aware drain wait during replica retirement.
_DRAIN_POLL_SECONDS = 2


@dataclasses.dataclass(frozen=True)
class ProbeRouteResult:
    """Provider observations belonging to one completed readiness round."""

    replica_infos: list['ReplicaInfo']
    resolved_routes: dict[int, route_projection.ResolvedRouteMaterial]
    identity_verified_replica_ids: set[int]
    complete: bool


class _PreemptionPrefilterDisposition(enum.Enum):
    """Conservative result of one status-lock-free provider probe."""

    LIVE_OR_UNPROVEN = 'LIVE_OR_UNPROVEN'
    INTERRUPTED = 'INTERRUPTED'
    EXACT_KUBERNETES_ABSENT = 'EXACT_KUBERNETES_ABSENT'
    IDENTITY_UNCERTAIN = 'IDENTITY_UNCERTAIN'


@dataclasses.dataclass(frozen=True)
class _ExactKubernetesAbsenceProof:
    """Pre-quiescence identity proof used only for interruption reduction."""

    cleanup_fence: reserved_capacity.ProtocolV2CleanupFence
    cluster_name: str
    replica_record_id: str


@dataclasses.dataclass(frozen=True)
class _PreemptionPrefilterResult:
    disposition: _PreemptionPrefilterDisposition
    exact_absence: _ExactKubernetesAbsenceProof | None = None

    def __post_init__(self) -> None:
        carries_absence = self.exact_absence is not None
        expects_absence = self.disposition is (
            _PreemptionPrefilterDisposition.EXACT_KUBERNETES_ABSENT)
        if carries_absence != expects_absence:
            raise ValueError(
                'Exact Kubernetes absence disposition and proof disagree.')


# Wall-clock persistence is the only way to carry a drain deadline across
# process restarts. Accept ordinary NTP skew, but rewrite timestamps farther in
# the future before admitting teardown so one corrupted row cannot postpone
# cleanup indefinitely. A large forward clock jump remains indistinguishable
# from real elapsed time and is part of the host clock's trust boundary.
_DRAIN_WALL_CLOCK_FUTURE_SKEW_SECONDS = 5 * 60
# An LB in-flight report older than this is considered stale (LB dead or
# not reporting): the drain wait then falls back to the full cap.
_IN_FLIGHT_REPORT_STALENESS_SECONDS = (
    3 * serve_constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
# A controller and its external LB commonly roll together. Give the new pair
# multiple sync opportunities to publish one matching target/capacity proof
# before emitting a diagnostic and renewing the wait. The deadline never grants
# route-admission authority without that fresh proof.
_LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS = (
    6 * serve_constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
# A recovered controller may need several generations to prove and repair a
# real capacity shortfall.  Bound each reactivation wave like the logical
# rolling-update bridge instead of returning the complete old fleet to routing.
_LOGICAL_RETIREMENT_RECOVERY_MAX_REACTIVATIONS_PER_GENERATION = 20
_SERVICE_OWNER_WATCH_INTERVAL_SECONDS = 1
_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS = 0.5
# Rate limit for the "fill launch skipped" log (see _log_fill_skip).
_FILL_SKIP_LOG_INTERVAL_SECONDS = 60
_WAIT_LAUNCH_THREAD_TIMEOUT_SECONDS = 15
# Cleanup is a durable invariant, not a finite-attempt operation.  Failed
# teardown rows remain in the replica table and are retried forever; these
# bounds only rate-limit provider calls while an outage persists.
_FAILED_CLEANUP_RETRY_BASE_SECONDS = 60
_FAILED_CLEANUP_RETRY_MAX_SECONDS = 15 * 60
# Kubernetes accepts a force-delete before its object necessarily disappears
# from subsequent reads.  Protocol-v2 teardown must bridge that propagation
# window before publishing exact provider absence, without holding the provider
# phase gate while it waits.
_POST_TEARDOWN_ABSENCE_TIMEOUT_SECONDS = 90
_POST_TEARDOWN_ABSENCE_POLL_SECONDS = 1
_NON_POOL_RECONCILIATION_RETRY_BASE_SECONDS = 30
_NON_POOL_RECONCILIATION_RETRY_MAX_SECONDS = 15 * 60
_MAX_CONCURRENT_NON_POOL_RECONCILIATIONS_PER_SERVICE = 16
# A service can queue an arbitrarily large durable teardown wave. Keep the
# queued intent, but bound live worker threads so one controller process cannot
# exhaust its memory or refresh-loop CPU while the global budget is spacious.
MAX_CONCURRENT_DOWNS_PER_SERVICE = 64
_CHANGED_ONLY_READINESS_PERSISTENCE_ENV_VAR = (
    'SKYPILOT_SERVE_CHANGED_ONLY_READINESS_PERSISTENCE')
# An autoscaler tick can place a full wave before any sky.launch result benches
# an unavailable location. Without a bound, a zero-cost-first placer can pin
# hundreds of replicas to one full Kubernetes pool. Demand placement consumes
# a shared, asynchronously refreshed free-GPU observation. During a startup or
# measurement blackout, keep only a few probes per ACTIVE zero-cost shape.
# Four matches SkyServe's historical per-service launch parallelism.
_ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION = 4
# Physical-cluster capture has its own bounded 30-second provider deadline.
# This slightly larger coordination bound is one absolute deadline for the
# complete parallel batch, not a fresh allowance for every context.  Provider
# I/O remains outside the manager mutex.
_RESERVED_FILL_PHYSICAL_PREFLIGHT_TIMEOUT_SECONDS = 45
_RESERVED_FILL_PHYSICAL_PREFLIGHT_RELEASE_TIMEOUT_SECONDS = 1
_ZERO_COST_ACTUATION_LEASE_SECONDS = 90
_ZERO_COST_ACTUATION_POLL_SECONDS = 1
# Lease only the work one lane is about to materialize.  The repository may
# retain a wider provider-free pending window, but leasing that complete window
# makes its tail age behind controller-local work and prevents a successor
# allocation from refreshing it.  Four matches SkyServe's historical
# per-service launch parallelism and bounds consecutive manager-mutex work.
_ZERO_COST_ACTUATION_QUANTUM = 4
# The current version plus two recently used recovery versions. Older
# versions remain recoverable from their immutable PostgreSQL YAML/spec rows;
# bounding parsed templates prevents a long-lived service from accumulating a
# Task object for every historical update.
_SERVICE_VERSION_TASK_TEMPLATE_CACHE_SIZE = 3
# Sentinel for drain registration's optional pre-resolved replica URL. ``None``
# is a real batched result: the cluster has no resolvable endpoint and the
# bounded deadline must remain the only completion path.
_REPLICA_URL_NOT_PROVIDED: Any = object()


def replica_may_consume_physical_capacity(info: Any) -> bool:
    """Whether a durable row may still own its provider capacity.

    Terminal status is a routing/lifecycle property, not cleanup evidence.
    Keep every row in capacity accounting until teardown has durably
    succeeded; deleting the row is the other proof and naturally removes it
    from the caller's snapshot.  Missing or malformed teardown state therefore
    fails closed.
    """
    status_property = getattr(info, 'status_property', None)
    return (status_property is None or
            getattr(status_property, 'sky_down_status',
                    None) != common_utils.ProcessStatus.SUCCEEDED)


def _changed_only_readiness_persistence_enabled() -> bool:
    value = os.environ.get(_CHANGED_ONLY_READINESS_PERSISTENCE_ENV_VAR)
    if value is None or value.lower() == 'false':
        return False
    if value.lower() == 'true':
        return True
    logger.warning(
        f'Invalid {_CHANGED_ONLY_READINESS_PERSISTENCE_ENV_VAR} value '
        f'{value!r}; changed-only readiness persistence remains disabled.')
    return False


def load_task_with_service_spec(
    yaml_content: str,
    authoritative_service_spec: 'service_spec.SkyServiceSpec | None' = None,
) -> task_lib.Task:
    """Load task resources while preserving a committed service policy.

    Service-policy validation evolves. Re-parsing an old committed YAML can
    therefore reject it or silently activate new hidden defaults. When an
    immutable pickled spec is available, parse the rest of the task without
    the service/pool section and bind that authoritative spec afterwards.
    """
    if authoritative_service_spec is None:
        return task_lib.Task.from_yaml_str(yaml_content)
    config = yaml_utils.safe_load(yaml_content)
    if not isinstance(config, dict):
        raise ValueError('Service task YAML must contain a mapping.')
    config.pop('service', None)
    config.pop('pool', None)
    task = task_lib.Task.from_yaml_config(config)
    task.set_service(authoritative_service_spec)
    return task


@dataclasses.dataclass
class _ZeroCostDemandBudget:
    """One scale-up batch's raw-GPU budget by (K8s context, accelerator)."""

    remaining_by_pool: dict[tuple[str, str], int]
    measured_by_pool: dict[tuple[str, str], int | None]
    selected_launches_by_pool: dict[tuple[str, str], int] = dataclasses.field(
        default_factory=dict)


class _ReplicaLaunchFunding(enum.Enum):
    """Funding provenance of one durably accepted replica launch."""

    PAID = 'paid'
    ZERO_COST = 'zero_cost'


@dataclasses.dataclass(frozen=True)
class _ReplicaLaunchResult:
    """Immutable accounting facts emitted by launch-wave admission."""

    replica_id: int
    planned_capacity: int
    funding: _ReplicaLaunchFunding


@dataclasses.dataclass(frozen=True)
class _PreparedPaidLaunch:
    """One side-effect-free paid launch awaiting atomic Phase-A admission."""

    candidate: 'paid_capacity.PaidClaimCandidate'
    launch_result: _ReplicaLaunchResult
    launch_thread: '_ReplicaLaunchThread'


@dataclasses.dataclass(frozen=True)
class _AmbiguousPaidPhaseAIdentity:
    """Exact staged replica whose Phase-A commit outcome is unknown."""

    replica_id: int
    replica_record_id: str


@dataclasses.dataclass
class _AmbiguousPaidPhaseARecovery:
    """Process-local retry state for one exact ambiguous Phase-A identity."""

    attempts: int = 0
    retry_at: float = 0.0


@dataclasses.dataclass
class _ReservedFillPhysicalPreflight:
    """One concurrently initialized physical-identity capture."""

    kubernetes_context: str
    physical_cluster_uid: str
    ready: threading.Event = dataclasses.field(default_factory=threading.Event)
    release: threading.Event = dataclasses.field(
        default_factory=threading.Event)
    cancellation: threading.Event = dataclasses.field(
        default_factory=threading.Event)
    error: BaseException | None = None


@dataclasses.dataclass(frozen=True)
class _ReservedFillPhysicalPreflightBatch:
    """Shared deadline and independently cancelable preflight holders."""

    preflights: dict[tuple[str, str], _ReservedFillPhysicalPreflight]
    threads: tuple[threading.Thread, ...]
    deadline_monotonic: float


class _ReplicaLaunchThread(thread_utils.SafeThread):
    """Launch worker that publishes a joinable completion notification."""

    def __init__(self,
                 *args: Any,
                 replica_id: int,
                 replica_record_id: str,
                 service_hash: str | None,
                 controller_owner: tuple[int | None, str | None] | None,
                 teardown_requested: threading.Event,
                 completion_queue: 'queue.SimpleQueue[_ReplicaLaunchThread]',
                 completion_event: threading.Event,
                 bound_ordinary_launch: bool = False,
                 adopts_existing_bound_request: bool = False,
                 ordinary_legacy_launch: bool = False,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.replica_id = replica_id
        self.replica_record_id = replica_record_id
        self.service_hash = service_hash
        self.controller_owner = controller_owner
        self.teardown_requested = teardown_requested
        self._completion_queue = completion_queue
        self._completion_event = completion_event
        self.bound_ordinary_launch = bound_ordinary_launch
        self.adopts_existing_bound_request = adopts_existing_bound_request
        self.ordinary_legacy_launch = ordinary_legacy_launch

    def run(self) -> None:
        try:
            super().run()
        finally:
            # This callback runs just before Thread.run returns, so the receiver
            # joins the notified worker before relying on is_alive(). The queue
            # preserves completion across Event coalescing and clear races.
            self._completion_queue.put(self)
            self._completion_event.set()


class _ReplicaDownThread(thread_utils.SafeThread):
    """Provider cleanup worker bound to one replica record and owner."""

    def __init__(self, *args: Any, replica_id: int, replica_record_id: str,
                 service_hash: str | None,
                 controller_owner: tuple[int | None, str | None] | None,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.replica_id = replica_id
        self.replica_record_id = replica_record_id
        self.service_hash = service_hash
        self.controller_owner = controller_owner


@dataclasses.dataclass(frozen=True)
class LogicalReconcileSnapshot:
    """One immutable LB capacity and occupancy generation."""

    version: int
    generation: int
    observed_slots_by_replica_id: dict[int, int]
    in_flight_by_replica_id: dict[int, int]
    unknown_replica_ids: frozenset[int]
    received_at: float
    authority: 'demand_state.DurableReconcileAuthority | None' = None


LogicalAcceleratorState = tuple[tuple[str, int], ...]
LogicalTargetState = (tuple[int, int, int] |
                      tuple[int, int, int, LogicalAcceleratorState,
                            LogicalAcceleratorState])


@dataclasses.dataclass(frozen=True)
class _LogicalReconcileState:
    """One atomically observed actuation, retirement, and capacity state."""

    target: LogicalTargetState | None
    snapshot: LogicalReconcileSnapshot | None
    retirement_floor: LogicalTargetState | None = None
    retirement_shelter: (reserved_fill_planner.SequencedRetirementShelter |
                         None) = None


@dataclasses.dataclass(frozen=True)
class _LogicalPendingLaunchAdmission:
    """One exact-card pending-launch admission calculation."""

    applicable: bool
    target_fence: LogicalTargetState | None
    authorized_ids: frozenset[int]
    reason: str
    details: str = ''


def _logical_retirement_target(
        state: _LogicalReconcileState) -> LogicalTargetState | None:
    """Return the destructive floor, with legacy target compatibility."""
    if state.retirement_shelter is not None:
        # A sequenced publication uses ``None`` as an explicit fail-closed
        # destructive state when exact-card shelter composition is incomplete.
        # It must never silently inherit the demand-only actuation target.
        return state.retirement_floor
    return (state.retirement_floor
            if state.retirement_floor is not None else state.target)


def _logical_target_state_components(
    state: LogicalTargetState | None,
) -> tuple[int, int, int, LogicalAcceleratorState,
           LogicalAcceleratorState] | None:
    """Validate and expand legacy or exact-card logical target state."""
    if not isinstance(state, tuple) or len(state) not in (3, 5):
        return None
    version, generation, target_capacity = state[:3]
    if (type(version) is not int or type(generation) is not int or
            generation < 0 or type(target_capacity) is not int or
            target_capacity < 0):
        return None
    if len(state) == 3:
        return version, generation, target_capacity, (), ()

    def _validated_items(raw: Any, *,
                         allow_zero: bool) -> LogicalAcceleratorState | None:
        if not isinstance(raw, tuple):
            return None
        normalized: list[tuple[str, int]] = []
        seen: set[str] = set()
        for item in raw:
            if (not isinstance(item, tuple) or len(item) != 2 or
                    not isinstance(item[0], str) or not item[0] or
                    type(item[1]) is not int or
                    item[1] < (0 if allow_zero else 1)):
                return None
            folded = item[0].casefold()
            if folded in seen:
                return None
            seen.add(folded)
            if item[1] > 0 or not allow_zero:
                normalized.append((item[0], item[1]))
        return tuple(normalized)

    target_by_card = _validated_items(state[3], allow_zero=True)
    shapes = _validated_items(state[4], allow_zero=False)
    if target_by_card is None or shapes is None:
        return None
    target_names = {card.casefold() for card, _ in target_by_card}
    shape_names = {card.casefold() for card, _ in shapes}
    if (sum(value for _, value in target_by_card) != target_capacity or
            target_names - shape_names or
        (target_capacity > 0 and not target_by_card)):
        return None
    return version, generation, target_capacity, target_by_card, shapes


def _logical_target_intent_preserved(
    current: LogicalTargetState | None,
    previous: LogicalTargetState | None,
) -> bool:
    """Whether a newer target preserves an earlier fence's exact intent."""
    current_components = _logical_target_state_components(current)
    previous_components = _logical_target_state_components(previous)
    if (current_components is None or previous_components is None or
            current is None or previous is None or
            len(current) != len(previous)):
        return False
    (current_version, current_generation, current_target, current_by_card,
     current_shapes) = current_components
    (previous_version, previous_generation, previous_target, previous_by_card,
     previous_shapes) = previous_components
    return (current_generation >= previous_generation and
            (current_version, current_target, current_by_card, current_shapes)
            == (previous_version, previous_target, previous_by_card,
                previous_shapes))


def _remove_nonmaterial_replica_config_metadata(config: dict[str, Any]) -> None:
    """Ignore generated metadata that cannot affect a running replica.

    Source provenance changes whenever a task is submitted from another Git
    commit, but it does not alter the running process. Every service update
    also gets a fresh ephemeral-storage generation, even when the task has no
    Sky-managed storage. Treating either generated identity as a replica
    config change turns a service-policy-only update into a full rolling
    replacement. An empty owned-mount list makes the storage identity
    operationally irrelevant; the actual secrets, file mounts, and volumes
    remain in the config comparison and still prevent unsafe replica reuse.
    """
    metadata = config.get('_metadata')
    if not isinstance(metadata, dict):
        return
    metadata.pop('git_commit', None)
    scope = metadata.get(serve_constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY)
    if isinstance(scope, dict) and scope.get('storage_mounts') == []:
        metadata.pop(serve_constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY)
    if not metadata:
        config.pop('_metadata')


def _normalize_empty_file_mounts_for_replica_reuse(
        config: dict[str, Any]) -> bool:
    """Canonicalize configurations that do not mount files.

    YAML omission, explicit null, and an empty mapping all describe the same
    runtime: no file mounts.  Normalizing those representations lets a
    service-policy-only update reuse its existing replicas.  Any other value
    remains material and requires replacement.
    """
    if ('file_mounts' not in config or config['file_mounts'] is None or
            config['file_mounts'] == {}):
        config.pop('file_mounts', None)
        return True
    return False


# TODO(tian): Backward compatibility. Remove this after 3 minor release, i.e.
# 0.13.0. We move the ProcessStatus to common_utils.ProcessStatus in #6666, but
# old ReplicaInfo in database will still tries to unpickle using ProcessStatus
# in replica_managers. We set this alias to avoid breaking changes. See #6729
# for more details.
ProcessStatus = common_utils.ProcessStatus


class _ReplicaLaunchOwnershipLostError(RuntimeError):
    """The controller lost authority while a replica launch was in flight."""


class _ReplicaLaunchSupersededError(RuntimeError):
    """A queued logical launch lost its exact-card target authority."""


class _BoundOrdinaryLaunchUnresolvedError(RuntimeError):
    """A bound request cannot safely be replaced or projected yet."""


class _BoundOrdinaryLaunchTerminalError(RuntimeError):
    """An exactly projected bound request finished without a launch."""


class _BoundOrdinaryLaunchPreEffectTerminalError(
        _BoundOrdinaryLaunchTerminalError):
    """A bound request failed before either external-effect boundary."""


def _bound_submission_may_have_committed(error: BaseException) -> bool:
    """Whether submission failed at a boundary that can lose a committed ACK.

    A deterministic 4xx response proves that the server rejected this exact
    payload.  Inspecting and adopting an older replica pointer in that case
    would turn a digest/identity conflict into success.  Network failures,
    server failures, and response-decoding failures rooted in a truncated
    transport remain ambiguous and may be resolved through the exact pointer.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, requests.exceptions.HTTPError):
            response = getattr(current, 'response', None)
            status_code = getattr(response, 'status_code', None)
            return (not isinstance(status_code, int) or status_code >= 500)
        if isinstance(current, requests.exceptions.RequestException):
            return True
        if isinstance(current, (exceptions.RequestInterruptedError,
                                exceptions.ServerTemporarilyUnavailableError)):
            return True
        current = current.__cause__ or current.__context__
    return False


class _ReplicaLaunchCapacityError(RuntimeError):
    """A pinned provider launch failed with typed availability evidence."""

    def __init__(self, message: str, reason: str) -> None:
        super().__init__(message)
        if reason not in ('capacity', 'quota'):
            raise ValueError(f'Invalid availability failure reason: {reason}')
        self.reason = reason


class _UnfencedExternalLbLaunchError(RuntimeError):
    """A legacy controller cannot satisfy the API replica-launch fence."""


class _SystemRecoveryLaunchCaptureError(RuntimeError):
    """A recovery-bearing request could not be associated exactly."""


def _scope_security_group_to_service(task: 'task_lib.Task',
                                     service_name: str | None) -> None:
    """Pins a service's replicas to ONE security group instead of one each.

    A cluster that declares ports gets its own group, named after the cluster.
    For a service that is one group per replica, and this fleet replaces spot
    replicas continuously across 20 AWS regions, so the count grows without
    bound: measured at ~3000 groups against a 2500-per-VPC quota, ~99% of them
    referenced by no network interface because teardown cannot wait long enough
    for the interface to detach.

    Naming the group after the SERVICE collapses that to one per service, and
    is the tightest scope that still bounds growth. It is not the same as
    sharing a group across services: the group carries a self-referencing rule
    that grants ALL protocols and ports between its members, and skylet listens
    on an unauthenticated port, so members of one group can execute code on each
    other. Replicas of a single service already share an image, a spec and their
    secrets, so that is within an existing trust boundary; two different
    services are not, and must never share.

    Implemented through ``aws.security_group_name``, which additionally gives
    exactly the lifecycle we need for free: SkyPilot marks a caller-specified
    group as not-managed-by-SkyPilot, so a single replica's teardown will not
    delete a group its siblings are still using, while ``open_ports`` still
    reconciles the service's ports onto it.

    Only applies when the caller did not already pin a group, so an explicit
    operator override always wins.
    """
    if not service_name:
        return
    scoped = f'sky-sg-{service_name}'
    new_resources = []
    for resource in task.resources:
        existing = dict(resource.cluster_config_overrides or {})
        aws_overrides = dict(existing.get('aws', {}))
        if aws_overrides.get('security_group_name'):
            # An operator pinned a group explicitly; do not second-guess it.
            return
        aws_overrides['security_group_name'] = scoped
        existing['aws'] = aws_overrides
        new_resources.append(resource.copy(_cluster_config_overrides=existing))
    task.set_resources(type(task.resources)(new_resources))


def _inject_replica_tls_material(task: 'task_lib.Task') -> None:
    """Hand a replica the TLS material its proxy needs, if TLS is enabled.

    The certificate is public and travels as an ordinary env var; the private
    key travels as a task SECRET so it is redacted from task YAML dumps and
    logs rather than sitting in plain text next to it.

    Only the material is delivered here. Terminating TLS is the task's job:
    its setup is expected to start a proxy on the service port that forwards
    to the model server on loopback. A task that ignores the material keeps
    serving plaintext, which the load balancer will then fail to reach over
    https -- deliberately visible rather than silently unencrypted.
    """
    mode = serve_utils.replica_tls_mode()
    if mode != serve_constants.REPLICA_TLS_MODE_PINNED:
        # 'unverified' intentionally ships no material: it exists for
        # deployments that terminate TLS with their own certificate.
        return
    certificate_pem = os.environ.get(serve_constants.REPLICA_TLS_CERT_ENV_VAR,
                                     '')
    private_key_pem = os.environ.get(
        serve_constants.REPLICA_TLS_KEY_SECRET_ENV_VAR, '')
    if not certificate_pem or not private_key_pem:
        raise RuntimeError(
            f'{serve_constants.REPLICA_TLS_MODE_ENV_VAR}='
            f'{serve_constants.REPLICA_TLS_MODE_PINNED} requires both '
            f'{serve_constants.REPLICA_TLS_CERT_ENV_VAR} and '
            f'{serve_constants.REPLICA_TLS_KEY_SECRET_ENV_VAR} in the '
            'controller environment.')
    task.update_envs(
        {serve_constants.REPLICA_TLS_CERT_ENV_VAR: certificate_pem})
    task.update_secrets(
        {serve_constants.REPLICA_TLS_KEY_SECRET_ENV_VAR: private_key_pem})


def _build_replica_launch_task(
    yaml_content: str,
    replica_id: int,
    resources_override: dict[str, Any] | None,
    *,
    exact_resources_override: bool,
    authoritative_service_spec: 'service_spec.SkyServiceSpec | None',
    service_name: str | None,
    task_template: task_lib.Task | None = None,
) -> task_lib.Task:
    """Build the exact pre-policy task submitted by a replica launch.

    Candidate authorization and the launch worker must hash/submit the same
    task. Keeping their construction in one helper also makes a later
    controller-side environment or security-group change fail closed through
    the backend's post-policy rematch instead of silently widening recovery.
    """
    task = (copy.deepcopy(task_template)
            if task_template is not None else load_task_with_service_spec(
                yaml_content, authoritative_service_spec))
    # The original user YAML is retained in the immutable service-version row
    # for status/display. Embedding the same text in every executable replica
    # request duplicates a potentially large payload and has no execution
    # semantics; keep only the structured task fields in controller launches.
    task._user_specified_yaml = None  # pylint: disable=protected-access
    if resources_override is not None:
        resources = task.resources
        if exact_resources_override:
            # Placement already selected the complete location and shape.
            resource = next(iter(resources)).copy(**resources_override)
            task.set_resources(resource)
        else:
            overridden_resources = [
                resource.copy(**resources_override) for resource in resources
            ]
            task.set_resources(type(resources)(overridden_resources))
    task.update_envs({serve_constants.REPLICA_ID_ENV_VAR: str(replica_id)})
    _inject_replica_tls_material(task)
    _scope_security_group_to_service(task, service_name)
    return task


def _task_is_known_non_aws(task: task_lib.Task) -> bool:
    """Whether every pre-policy alternative names a non-AWS provider."""
    providers = {
        repr(resource.cloud).lower()
        for resource in task.resources
        if resource.cloud is not None
    }
    return bool(providers) and 'aws' not in providers


def _system_recovery_launch_result_job_id(result: Any,
                                          cluster_name: str) -> int:
    """Validate the exact ordinary launch result used by recovery."""
    if (not isinstance(result, tuple) or len(result) != 2 or
            isinstance(result[0], bool) or not isinstance(result[0], int) or
            result[0] < 1 or
            not isinstance(result[1], backends.CloudVmRayResourceHandle) or
            result[1].cluster_name != cluster_name):
        raise _SystemRecoveryLaunchCaptureError(
            'Recovery-bearing launch returned a malformed or mismatched '
            '(job_id, handle) result.')
    return result[0]


@context.contextual
def adopt_system_recovery_launch(
    replica_id: int,
    cluster_name: str,
    log_file: str,
    request_id: str,
    persist_system_recovery_job_id: Callable[[str, int], bool],
    teardown_requested: threading.Event,
) -> None:
    """Adopt one already-bound launch request after controller restart."""
    ctx = context.get()
    assert ctx is not None, 'Context is not initialized'
    ctx.redirect_log(pathlib.Path(log_file))
    launch_request_id = server_common.RequestId[tuple[int | None,
                                                      backends.ResourceHandle |
                                                      None]](request_id)
    stop_watchdog = threading.Event()

    def _watch_teardown() -> None:
        while not stop_watchdog.wait(_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS):
            if not teardown_requested.is_set():
                continue
            try:
                sdk.api_cancel(launch_request_id)
            except Exception as error:  # pylint: disable=broad-except
                logger.warning(
                    'Failed to cancel recovered launch for replica %s; '
                    'retrying: %s', replica_id,
                    common_utils.format_exception(error))
                continue
            return

    watchdog = threading.Thread(target=_watch_teardown,
                                name=f'replica-{replica_id}-recovery-cancel',
                                daemon=True)
    watchdog.start()
    try:
        result = sdk.get(launch_request_id)
    finally:
        stop_watchdog.set()
        watchdog.join(timeout=1)
    job_id = _system_recovery_launch_result_job_id(result, cluster_name)
    if not persist_system_recovery_job_id(request_id, job_id):
        raise _SystemRecoveryLaunchCaptureError(
            f'Failed to persist exact service job {job_id} for recovery '
            f'candidate replica {replica_id}.')


def _bound_projection_classification(projection: Any) -> str:
    classification = getattr(projection, 'disposition', None)
    if classification is None:
        classification = getattr(projection, 'classification', None)
    if isinstance(classification, enum.Enum):
        classification = classification.value
    if not isinstance(classification, str) or not classification:
        raise _BoundOrdinaryLaunchUnresolvedError(
            'The exact ordinary-launch reducer returned no classification.')
    return classification


def _bound_reduction_request_id(reduction: Any) -> str:
    request = getattr(reduction, 'request', None)
    request_id = getattr(request, 'request_id', None)
    if request_id is None:
        request_id = getattr(reduction, 'request_id', None)
    if not isinstance(request_id, str) or not request_id:
        raise _BoundOrdinaryLaunchUnresolvedError(
            'The exact ordinary-launch association returned no request ID.')
    return request_id


def _wait_for_bound_ordinary_launch(
    replica_id: int,
    cluster_name: str,
    request_id: str,
    stream_logs: bool,
    launch_cloud: clouds.Cloud | None,
    reduce_exact: Callable[[Any, BaseException | None], Any],
    cancel_exact: Callable[[str], Any],
    teardown_requested: threading.Event,
    continue_guard: Callable[[], bool] | None = None,
    supersession_guard: Callable[[], bool | tuple[bool, str]] | None = None,
    api_auth_token_provider: Callable[[], tuple[str, ...]] | None = None,
    durable_store_only: bool = False,
) -> None:
    """Adopt, quiesce, and reduce one exact durable request binding."""
    cancel_reason: str | None = None
    cancel_committed = False
    cancellation_failures = 0
    cancel_projection: Any = None

    def _still_owned() -> bool:
        if continue_guard is not None:
            try:
                owned = continue_guard()
            except Exception as error:  # pylint: disable=broad-except
                logger.warning(
                    'Failed to verify bound ordinary-launch ownership; '
                    'detaching: %s', common_utils.format_exception(error))
                return False
            return owned
        return True

    def _raise_if_owner_lost() -> None:
        if _still_owned():
            return
        # Controller replacement transfers the durable association. It is not
        # a cancellation decision, and the successor adopts the exact pointer.
        raise _ReplicaLaunchOwnershipLostError(
            f'Detaching bound ordinary launch for replica {replica_id} after '
            'controller ownership loss.')

    def _observe_cancel_reason() -> str | None:
        nonlocal cancel_reason

        if cancel_reason is None and teardown_requested.is_set():
            cancel_reason = 'replica-teardown'
        if cancel_reason is None and supersession_guard is not None:
            try:
                decision = supersession_guard()
            except Exception as error:  # pylint: disable=broad-except
                decision = (False, f'guard-error-{type(error).__name__}')
            if isinstance(decision, bool):
                allowed = decision
                reason = 'guard-rejected'
            elif (isinstance(decision, tuple) and len(decision) == 2 and
                  isinstance(decision[0], bool) and
                  isinstance(decision[1], str) and decision[1]):
                allowed, reason = decision
            else:
                allowed, reason = False, 'invalid-guard-result'
            if not allowed:
                cancel_reason = reason[:128]
        return cancel_reason

    def _commit_cancel_if_needed() -> None:
        nonlocal cancel_committed, cancellation_failures, cancel_projection
        reason = _observe_cancel_reason()
        if reason is not None and not cancel_committed:
            try:
                cancel_result = cancel_exact(reason)
                if cancel_result is None or cancel_result is False:
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Exact ordinary-launch cancellation found no binding.')
                cancellation_failures = 0
                cancel_committed = True
                cancel_projection = cancel_result
            except Exception as error:  # pylint: disable=broad-except
                cancellation_failures += 1
                if (cancellation_failures == 1 or
                        cancellation_failures % 10 == 0):
                    logger.warning(
                        'Exact cancellation for bound replica %s is not yet '
                        'committed (attempt %s): %s', replica_id,
                        cancellation_failures,
                        common_utils.format_exception(error))
                raise

    def _request_status(projection: Any) -> str | None:
        request = getattr(projection, 'request', None)
        status = getattr(request, 'status', None)
        if isinstance(status, enum.Enum):
            status = status.value
        return status if isinstance(status, str) else None

    def _request_error(projection: Any) -> BaseException | None:
        request = getattr(projection, 'request', None)
        error = getattr(request, 'error', None)
        return non_pool_launch_reconciliation.decoded_request_error(error)

    def _finish_projection(projection: Any,
                           waiter_error: BaseException | None = None) -> None:
        classification = _bound_projection_classification(projection)
        if classification == 'AMBIGUOUS':
            raise _BoundOrdinaryLaunchUnresolvedError(
                f'Bound ordinary launch for replica {replica_id} is '
                'durably ambiguous; refusing a successor or cleanup.')
        if not (getattr(projection, 'projected', False) or classification
                in ('PROJECTED', 'PRE_EFFECT_TERMINAL', 'SETTLED')):
            raise _BoundOrdinaryLaunchUnresolvedError(
                f'Bound ordinary launch for replica {replica_id} returned '
                f'nonterminal classification {classification!r}.')

        status = _request_status(projection)
        request_error = _request_error(projection)
        exact_error = request_error or waiter_error
        persisted_cancel_reason = getattr(projection, 'cancel_reason', None)
        if (persisted_cancel_reason is not None and
            (not isinstance(persisted_cancel_reason, str) or
             not persisted_cancel_reason)):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Bound ordinary launch returned a malformed durable cancel '
                'reason.')
        if (cancel_reason is not None and
                persisted_cancel_reason is not None and
                cancel_reason != persisted_cancel_reason):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Bound ordinary launch local and durable cancellation '
                'reasons disagree.')
        effective_cancel_reason = persisted_cancel_reason or cancel_reason
        if effective_cancel_reason is not None:
            if effective_cancel_reason == 'replica-teardown':
                # Teardown owns the next action.  Exact projection has cleared
                # the request pointer, so its caller may now proceed to down.
                return
            raise _ReplicaLaunchSupersededError(
                f'Bound ordinary launch for replica {replica_id} was '
                'cancelled after supersession: '
                f'reason={effective_cancel_reason}.')
        if classification == 'PRE_EFFECT_TERMINAL':
            error = _BoundOrdinaryLaunchPreEffectTerminalError(
                f'Bound ordinary launch for replica {replica_id} terminated '
                'before provider or service-job I/O; the projected failure '
                'is safe for a later retry generation.')
            if exact_error is not None:
                raise error from exact_error
            raise error
        if status == 'SUCCEEDED':
            if exact_error is not None:
                raise _BoundOrdinaryLaunchUnresolvedError(
                    f'Bound ordinary launch for replica {replica_id} has a '
                    'successful durable result but its exact waiter failed.'
                ) from exact_error
            return
        if (isinstance(exact_error, exceptions.ResourcesUnavailableError) and
                launch_cloud is not None):
            reason = cloud_vm_ray_backend.classify_resources_unavailable_error(
                launch_cloud, exact_error)
            if reason is not None:
                raise _ReplicaLaunchCapacityError(
                    f'Bound ordinary launch for replica {replica_id} failed '
                    f'due to provider {reason}.',
                    reason=reason) from exact_error
        if exact_error is not None:
            raise exact_error
        raise _BoundOrdinaryLaunchTerminalError(
            f'Bound ordinary launch for replica {replica_id} projected '
            f'terminal status {status!r} without a launch result.')

    reduction_failures = 0

    def _reduce_until_wait_or_terminal(
            waiter_result: Any = None,
            waiter_error: BaseException | None = None) -> str:
        """Drive the reducer and return only when an active wait is needed."""
        nonlocal reduction_failures, cancel_projection, cancel_reason
        nonlocal cancel_committed
        while True:
            _raise_if_owner_lost()
            cancel_delivery_failed = False
            try:
                _commit_cancel_if_needed()
            except Exception:  # pylint: disable=broad-except
                # Cancellation deliberately rejects an already-ambiguous
                # association. The reducer is still a safe, non-authorizing
                # read/transition and must observe that durable ambiguity so
                # this process-local launch owner can detach and let the
                # provider-evidence reconciler take over. Transient failures
                # remain retryable below if reduction is also unavailable or
                # still reports an active request.
                cancel_delivery_failed = True
            try:
                if cancel_projection is not None:
                    projection = cancel_projection
                    cancel_projection = None
                else:
                    projection = reduce_exact(waiter_result, waiter_error)
            except Exception as error:  # pylint: disable=broad-except
                reduction_failures += 1
                if reduction_failures == 1 or reduction_failures % 10 == 0:
                    logger.warning(
                        'Exact ordinary-launch reduction for replica %s is '
                        'temporarily unavailable (attempt %s): %s',
                        replica_id, reduction_failures,
                        common_utils.format_exception(error))
                time.sleep(_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS)
                continue
            reduction_failures = 0
            if projection is None:
                raise _BoundOrdinaryLaunchUnresolvedError(
                    f'Bound ordinary launch for replica {replica_id} lost '
                    'its exact durable pointer.')
            classification = _bound_projection_classification(projection)
            if (getattr(projection, 'projected', False) or
                    classification in ('PROJECTED', 'PRE_EFFECT_TERMINAL',
                                       'SETTLED', 'AMBIGUOUS')):
                # A reducer may atomically finish a recovered CANCEL_REQUESTED
                # row (including expired NOT_STARTED ownership). Its pointer
                # and pin are already cleared, so attempting cancel redelivery
                # after projection would target a deliberately settled row.
                _finish_projection(projection, waiter_error)
                return 'TERMINAL'
            if cancel_delivery_failed:
                time.sleep(_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS)
                continue
            durable_cancel_reason = getattr(projection, 'cancel_reason', None)
            if durable_cancel_reason is not None:
                if (not isinstance(durable_cancel_reason, str) or
                        not durable_cancel_reason):
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Bound ordinary launch returned a malformed durable '
                        'cancel reason.')
                if (cancel_reason is not None and
                        cancel_reason != durable_cancel_reason):
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Bound ordinary launch local and durable '
                        'cancellation reasons disagree.')
                if cancel_reason is None:
                    # A replacement controller recovered a previously
                    # committed cancel intent.  Redeliver the exact API cancel
                    # before waiting; association CAS makes this idempotent.
                    cancel_reason = durable_cancel_reason
                    cancel_committed = False
                if not cancel_committed:
                    continue
            if classification == 'ADOPT_ACTIVE':
                return classification
            if classification not in ('WAIT_QUIESCENCE', 'REDUCE_TERMINAL',
                                      'PRE_EFFECT_TERMINALIZE'):
                raise _BoundOrdinaryLaunchUnresolvedError(
                    f'Bound ordinary launch for replica {replica_id} returned '
                    f'unknown classification {classification!r}.')
            # A terminal request can precede its executor's exact-generation
            # acknowledgement.  It cannot be replaced or cleaned up yet; poll
            # the one reducer transaction rather than guessing from sdk.get().
            time.sleep(_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS)

    # Classify before blocking in sdk.get().  In particular this closes the
    # generation-zero terminalization case: an active request whose queue row
    # vanished has no executor that could ever wake an SDK waiter, but its
    # NOT_STARTED effect phase makes exact terminalization safe.
    if _reduce_until_wait_or_terminal() == 'TERMINAL':
        return
    if durable_store_only:
        while True:
            time.sleep(_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS)
            if _reduce_until_wait_or_terminal() == 'TERMINAL':
                return

    launch_request_id = server_common.RequestId[tuple[int | None,
                                                      backends.ResourceHandle |
                                                      None]](request_id)
    result_box: list[Any] = []
    parent_context = context.get()

    def _wait_exact_request() -> None:
        api_auth_context: contextlib.AbstractContextManager = (
            server_common.serve_controller_api_auth(api_auth_token_provider) if
            api_auth_token_provider is not None else contextlib.nullcontext())
        with context.initialize(parent_context), api_auth_context:
            if stream_logs:
                result_box.append(sdk.stream_and_get(launch_request_id))
            else:
                result_box.append(sdk.get(launch_request_id))

    # The exact SDK wait preserves typed results/errors and the normal log
    # stream. Keeping it in a daemon child lets controller replacement detach
    # promptly without cancelling the durable request it just handed off.
    exact_waiter = thread_utils.SafeThread(
        target=_wait_exact_request,
        name=f'replica-{replica_id}-bound-request-wait',
        daemon=True)
    exact_waiter.start()
    while exact_waiter.is_alive():
        _raise_if_owner_lost()
        # Drive claim expiry and terminal/quiescence even while the SDK waiter
        # is still blocked. Bound requests are intentionally excluded from the
        # generic queue reaper because only this association-aware reducer can
        # distinguish NOT_STARTED settlement from post-effect ambiguity.
        if _reduce_until_wait_or_terminal() == 'TERMINAL':
            return
        exact_waiter.join(timeout=_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS)
    exact_waiter.join()
    exact_error = exact_waiter.exception
    launch_result = result_box[0] if result_box else None
    if exact_error is None:
        result_is_exact = False
        if isinstance(launch_result, tuple) and len(launch_result) == 2:
            service_job_id, handle = launch_result  # pylint: disable=unpacking-non-sequence
            result_is_exact = bool(
                not isinstance(service_job_id, bool) and
                isinstance(service_job_id, int) and service_job_id > 0 and
                isinstance(handle, backends.CloudVmRayResourceHandle) and
                handle.cluster_name == cluster_name)
        if not result_is_exact:
            exact_error = _BoundOrdinaryLaunchUnresolvedError(
                f'Bound request {request_id} returned a malformed or '
                f'mismatched result for replica {replica_id}.')
    _reduce_until_wait_or_terminal(launch_result, exact_error)


@context.contextual
def adopt_bound_ordinary_launch(
    replica_id: int,
    cluster_name: str,
    log_file: str,
    request_id: str,
    launch_cloud: clouds.Cloud | None,
    reduce_exact: Callable[[Any, BaseException | None], Any],
    cancel_exact: Callable[[str], Any],
    teardown_requested: threading.Event,
    continue_guard: Callable[[], bool] | None = None,
    supersession_guard: Callable[[], bool | tuple[bool, str]] | None = None,
    durable_store_only: bool = False,
) -> None:
    """Adopt only the association named by the exact replica pointer."""
    ctx = context.get()
    assert ctx is not None, 'Context is not initialized'
    if not durable_store_only:
        # Reserved-fill adoption is a PostgreSQL-only reducer loop.  Opening
        # the legacy shared launch log here would reintroduce an RWX storage
        # dependency before the first durable request observation.
        ctx.redirect_log(pathlib.Path(log_file))
    _wait_for_bound_ordinary_launch(
        replica_id,
        cluster_name,
        request_id,
        False,
        launch_cloud,
        reduce_exact,
        cancel_exact,
        teardown_requested,
        continue_guard=continue_guard,
        supersession_guard=supersession_guard,
        api_auth_token_provider=(None if durable_store_only else
                                 _required_controller_admin_auth_tokens),
        durable_store_only=durable_store_only)


# TODO(tian): Combine this with
# sky/spot/recovery_strategy.py::StrategyExecutor::launch
# Use context.contextual to enable per-launch output redirection.
@context.contextual
def launch_cluster(
    replica_id: int,
    yaml_content: str,
    cluster_name: str,
    log_file: str,
    replica_to_request_id: thread_utils.ThreadSafeDict[int, str],
    resources_override: dict[str, Any] | None = None,
    retry_until_up: bool = True,
    max_retry: int = _DEFAULT_LAUNCH_MAX_RETRY,
    availability_max_retry: int | None = None,
    exact_resources_override: bool = False,
    pre_launch_guard: Callable[[], bool] | None = None,
    cloud_launch_guard: Callable[[], bool | tuple[bool, str]] | None = None,
    supersession_guard: Callable[[], bool | tuple[bool, str]] | None = None,
    continue_guard: Callable[[], bool] | None = None,
    cleanup_continue_guard: Callable[[], bool] | None = None,
    launch_fence: dict[str, Any] | None = None,
    service_spec: 'service_spec.SkyServiceSpec | None' = None,
    task_template: task_lib.Task | None = None,
    workspace: str | None = None,
    service_name: str | None = None,
    system_recovery_launch_context: dict[str, Any] | None = None,
    get_bound_system_recovery_request_id: Callable[[], str | None] |
    None = None,
    persist_system_recovery_job_id: Callable[[str, int], bool] | None = None,
    demote_system_recovery_candidate: Callable[[], bool] | None = None,
    ordinary_launch_handoff_context: dict[str, Any] | None = None,
    ordinary_launch_event: Callable[[
        ordinary_launch_handoff.EventKind, str | None, int |
        None, ordinary_launch_handoff.TerminalStatus | None
    ], None] | None = None,
    ordinary_launch_submission_uuid: str | None = None,
    non_pool_launch_profile_kind: str | None = None,
    inspect_bound_ordinary_launch: Callable[[], Any] | None = None,
    reduce_bound_ordinary_launch: Callable[[Any, BaseException | None], Any] |
    None = None,
    cancel_bound_ordinary_launch: Callable[[str], Any] | None = None,
    teardown_requested: threading.Event | None = None,
) -> None:
    """Launch a sky serve replica cluster.

    This function will not wait for the job starts running. It will return
    immediately after the job is submitted.

    Launch failures are retried in place with backoff, up to max_retry
    attempts. Failures caused by resource availability (capacity) are capped
    separately by availability_max_retry, defaulting to max_retry. Spot
    replicas with a spot placer pass availability_max_retry=1 so a capacity
    failure at the placer-pinned location propagates immediately and the
    placer fails over to a different location, while other (transient) errors
    keep the max_retry in-place attempts.

    Raises:
        RuntimeError: If failed to launch the cluster after the allowed
            attempts, or some error happened before provisioning and will
            happen again if retry.
    """
    ctx = context.get()
    assert ctx is not None, 'Context is not initialized'
    ctx.redirect_log(pathlib.Path(log_file))
    if teardown_requested is None:
        teardown_requested = threading.Event()

    if resources_override is not None:
        logger.info(f'Scaling up replica (id: {replica_id}) cluster '
                    f'{cluster_name} with resources override: '
                    f'{resources_override}')
    try:
        task = _build_replica_launch_task(
            yaml_content,
            replica_id,
            resources_override,
            exact_resources_override=exact_resources_override,
            authoritative_service_spec=service_spec,
            service_name=service_name,
            task_template=task_template)

        logger.info(f'Launching replica (id: {replica_id}) cluster '
                    f'{cluster_name} with resources: {task.resources}')
    except Exception as e:  # pylint: disable=broad-except
        logger.error('Failed to construct task object from yaml file with '
                     f'error {common_utils.format_exception(e)}')
        raise RuntimeError(
            f'Failed to launch the sky serve replica cluster {cluster_name} '
            'due to failing to initialize sky.Task from yaml file.') from e

    try:
        protocol_v2_fence = reserved_capacity.parse_protocol_v2_launch_fence(
            launch_fence or {})
    except ValueError as error:
        raise reserved_capacity.ReservedFillLaunchFenceError(
            'Reserved-fill launch cleanup authority is malformed.') from error
    if protocol_v2_fence is not None:
        raise _BoundOrdinaryLaunchUnresolvedError(
            'Protocol-v2 reserved fill must adopt its atomically admitted '
            'PostgreSQL request; direct controller submission is unsupported.')
    controller_api_auth_token_provider: Callable[[], tuple[str,
                                                           ...]] | None = None

    def _emit_ordinary_launch_event(
        event_kind: ordinary_launch_handoff.EventKind,
        request_id: str | None = None,
        service_job_id: int | None = None,
        terminal_status: ordinary_launch_handoff.TerminalStatus | None = None
    ) -> None:
        if ordinary_launch_event is None:
            return
        try:
            ordinary_launch_event(event_kind, request_id, service_job_id,
                                  terminal_status)
        except Exception as error:  # pylint: disable=broad-except
            # Telemetry is never part of launch correctness or availability.
            logger.debug('Ordinary-launch telemetry callback failed: %s', error)

    def _lookup_terminal_status(request_id: str) -> str | None:
        api_auth_context: contextlib.AbstractContextManager = (
            server_common.serve_controller_api_auth(
                controller_api_auth_token_provider)
            if controller_api_auth_token_provider is not None else
            contextlib.nullcontext())
        with api_auth_context:
            request_payloads = sdk.api_status(
                request_ids=[request_id],
                fields=['request_id', 'status'],
                _exact_request_ids=True,
                _use_body=True,
                _request_timeout_seconds=(
                    ordinary_launch_handoff.
                    TERMINAL_STATUS_LOOKUP_TIMEOUT_SECONDS),
                _retry_on_server_unavailable=False)
        exact_matches = [
            request for request in request_payloads
            if request.request_id == request_id
        ]
        if len(exact_matches) != 1:
            return None
        status = exact_matches[0].status
        return status if isinstance(status, str) else None

    def _observe_terminal_nonblocking(request_id: str) -> None:
        if ordinary_launch_event is None:
            return

        def _emit_terminal(
                terminal_status: ordinary_launch_handoff.TerminalStatus
        ) -> None:
            _emit_ordinary_launch_event(
                ordinary_launch_handoff.EventKind.API_TERMINAL,
                request_id,
                terminal_status=terminal_status)

        ordinary_launch_handoff.observe_terminal_nonblocking(
            request_id, lookup=_lookup_terminal_status, emit=_emit_terminal)

    def _check_is_cancelled() -> bool:
        if teardown_requested.is_set():
            logger.info(f'Replica {replica_id} launch cancelled.')
            return True
        return False

    ownership_lost = threading.Event()
    launch_superseded = threading.Event()
    supersession_reason = ['unknown']
    supersession_cancel_failures = 0
    ownership_loss_cancel_event_lock = threading.Lock()
    ownership_loss_cancel_event_request_ids: set[str] = set()

    def _emit_owner_loss_cancel_request_once(request_id: str) -> None:
        """Record local cancellation intent once; it is not terminal proof."""
        if recovery_request_attempted:
            return
        with ownership_loss_cancel_event_lock:
            if request_id in ownership_loss_cancel_event_request_ids:
                return
            ownership_loss_cancel_event_request_ids.add(request_id)
        _emit_ordinary_launch_event(
            ordinary_launch_handoff.EventKind.OWNER_LOSS_CANCEL_REQUESTED,
            request_id)

    def _guard_allows(guard: Callable[[], bool] | None) -> bool:
        if guard is None:
            return True
        try:
            return guard()
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to verify replica launch ownership; '
                           'failing closed: '
                           f'{common_utils.format_exception(e)}')
            return False

    def _cloud_guard_decision() -> tuple[bool, str]:
        """Return a bounded rejection reason across the launch-thread boundary."""
        if cloud_launch_guard is None:
            return True, 'not-configured'
        try:
            result = cloud_launch_guard()
        except Exception as e:  # pylint: disable=broad-except
            reason = f'guard-error-{type(e).__name__}'
            logger.warning('Failed to verify cloud launch authority; '
                           f'failing closed: reason={reason}.')
            return False, reason
        if isinstance(result, bool):
            return result, 'authorized' if result else 'guard-rejected'
        if (isinstance(result, tuple) and len(result) == 2 and
                isinstance(result[0], bool) and isinstance(result[1], str) and
                result[1] and len(result[1]) <= 128):
            return result
        logger.warning('Cloud launch guard returned an invalid result; '
                       'failing closed: reason=invalid-guard-result.')
        return False, 'invalid-guard-result'

    def _supersession_guard_decision() -> tuple[bool, str]:
        """Return whether this worker still belongs to the current version."""
        if supersession_guard is None:
            return True, 'not-configured'
        try:
            result = supersession_guard()
        except Exception as e:  # pylint: disable=broad-except
            reason = f'guard-error-{type(e).__name__}'
            logger.warning('Failed to verify replica launch generation; '
                           f'failing closed: reason={reason}.')
            return False, reason
        if isinstance(result, bool):
            return result, 'authorized' if result else 'guard-rejected'
        if (isinstance(result, tuple) and len(result) == 2 and
                isinstance(result[0], bool) and isinstance(result[1], str) and
                result[1] and len(result[1]) <= 128):
            return result
        logger.warning('Replica launch generation guard returned an invalid '
                       'result; failing closed: reason=invalid-guard-result.')
        return False, 'invalid-guard-result'

    def _cancel_request_for_ownership_loss() -> None:
        ownership_lost.set()
        if ordinary_launch_submission_uuid is not None:
            # A capable successor atomically transfers this association and
            # adopts the same API request.  Process-local owner loss is never a
            # durable cancellation decision for a bound launch.
            return
        request_id = replica_to_request_id.get(replica_id)
        if request_id is None:
            return
        _emit_owner_loss_cancel_request_once(request_id)
        try:
            sdk.api_cancel(request_id)
        except Exception as e:  # pylint: disable=broad-except
            # The successor still owns the durable replica row and can
            # recover/garbage-collect the incarnation-scoped cluster. Never
            # let a cancellation transport error authorize more stale work.
            logger.warning(f'Failed to cancel stale replica {replica_id} '
                           f'launch request {request_id}: '
                           f'{common_utils.format_exception(e)}')

    def _cancel_request_for_supersession(reason: str) -> bool:
        """Cancel only this worker; a normal update retains manager health."""
        nonlocal supersession_cancel_failures
        supersession_reason[0] = reason
        launch_superseded.set()
        request_id = replica_to_request_id.get(replica_id)
        if request_id is None:
            return True
        try:
            if ordinary_launch_submission_uuid is not None:
                if cancel_bound_ordinary_launch is None:
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Bound launch has no exact cancellation callback.')
                if not cancel_bound_ordinary_launch(reason):
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Exact bound launch cancellation was not committed.')
            else:
                sdk.api_cancel(request_id)
        except Exception as e:  # pylint: disable=broad-except
            supersession_cancel_failures += 1
            if (supersession_cancel_failures == 1 or
                    supersession_cancel_failures % 10 == 0):
                logger.warning(
                    f'Failed to cancel superseded replica {replica_id} '
                    f'launch request {request_id}; retrying '
                    f'(failure {supersession_cancel_failures}): '
                    f'{common_utils.format_exception(e)}')
            return False
        return True

    def _assert_launch_not_superseded() -> None:
        allowed, reason = _supersession_guard_decision()
        if launch_superseded.is_set() or not allowed:
            if not launch_superseded.is_set():
                _cancel_request_for_supersession(reason)
            raise _ReplicaLaunchSupersededError(
                f'Refusing superseded cloud launch for replica {replica_id}: '
                f'reason={supersession_reason[0]}.')

    def _assert_launch_authorized() -> None:
        if (ownership_lost.is_set() or not _guard_allows(pre_launch_guard) or
                not _guard_allows(continue_guard)):
            _cancel_request_for_ownership_loss()
            raise _ReplicaLaunchOwnershipLostError(
                f'Refusing to launch replica {replica_id} after service '
                'controller ownership was lost.')

    def _stream_with_launch_watchdogs(request_id: Any) -> Any:
        """Cancel an async launch on teardown, owner loss, or supersession."""
        stop_watchdog = threading.Event()

        def _watch_launch_authority() -> None:
            while not stop_watchdog.wait(_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS):
                if teardown_requested.is_set():
                    try:
                        sdk.api_cancel(request_id)
                    except Exception as error:  # pylint: disable=broad-except
                        logger.warning(
                            'Failed to cancel teardown launch request for '
                            'replica %s; retrying: %s', replica_id,
                            common_utils.format_exception(error))
                        continue
                    return
                if not _guard_allows(continue_guard):
                    logger.warning(
                        f'Cancelling replica {replica_id} launch after '
                        'controller ownership loss.')
                    _cancel_request_for_ownership_loss()
                    return
                allowed, reason = _supersession_guard_decision()
                if not allowed:
                    logger.info(
                        f'Cancelling replica {replica_id} launch after its '
                        f'generation was superseded: reason={reason}.')
                    if _cancel_request_for_supersession(reason):
                        return

        watchdog = threading.Thread(target=_watch_launch_authority,
                                    name=f'replica-{replica_id}-launch-owner',
                                    daemon=True)
        watchdog.start()
        result: Any = None
        stream_error: Exception | None = None
        try:
            try:
                result = sdk.stream_and_get(request_id)
            except Exception as e:  # pylint: disable=broad-except
                stream_error = e
        finally:
            stop_watchdog.set()
            watchdog.join(timeout=1)
        if launch_superseded.is_set():
            message = (f'Replica {replica_id} launch was cancelled after its '
                       'generation was superseded' if stream_error is not None
                       else f'Replica {replica_id} launch completed after its '
                       'generation was superseded')
            raise _ReplicaLaunchSupersededError(
                f'{message}: reason={supersession_reason[0]}.'
            ) from stream_error
        if stream_error is not None:
            raise stream_error
        return result

    recovery_callbacks_configured = all(callback is not None for callback in (
        get_bound_system_recovery_request_id,
        persist_system_recovery_job_id,
        demote_system_recovery_candidate,
    ))
    recovery_context_available = (system_recovery_launch_context is not None and
                                  recovery_callbacks_configured)

    def _bound_recovery_request_id() -> str | None:
        assert get_bound_system_recovery_request_id is not None
        request_id = get_bound_system_recovery_request_id()
        if request_id is not None and (not isinstance(request_id, str) or
                                       not request_id):
            raise _SystemRecoveryLaunchCaptureError(
                'Recovery request association returned an invalid request ID.')
        return request_id

    def _demote_recovery_candidate() -> None:
        nonlocal recovery_context_available
        if not recovery_context_available:
            return
        assert demote_system_recovery_candidate is not None
        if not demote_system_recovery_candidate():
            raise _SystemRecoveryLaunchCaptureError(
                f'Failed to demote recovery candidate for replica '
                f'{replica_id}; refusing a second launch request.')
        recovery_context_available = False

    def _capture_recovery_result(request_id: str, result: Any) -> None:
        job_id = _system_recovery_launch_result_job_id(result, cluster_name)
        assert persist_system_recovery_job_id is not None
        if not persist_system_recovery_job_id(request_id, job_id):
            raise _SystemRecoveryLaunchCaptureError(
                f'Failed to persist exact service job {job_id} for '
                f'recovery candidate replica {replica_id}.')

    if availability_max_retry is None:
        availability_max_retry = max_retry

    if non_pool_launch_profile_kind is not None:
        try:
            generic_profile_kind = (
                ordinary_launch_binding.NonPoolLaunchProfileKind(
                    non_pool_launch_profile_kind))
        except (TypeError, ValueError) as error:
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Generic launch has an unsupported profile kind.') from error
        if ordinary_launch_submission_uuid is None:
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Generic launch has no stable submission UUID.')
    else:
        generic_profile_kind = None

    if ordinary_launch_submission_uuid is not None:
        if (generic_profile_kind is None and
            (recovery_context_available or protocol_v2_fence is not None)):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Special recovery and reserved-fill launches cannot enter the '
                'ordinary binding path.')
        if (generic_profile_kind is
                ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Reserved-fill requests are admitted before worker creation.')
        if (generic_profile_kind == ordinary_launch_binding.
                NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY and
                not recovery_context_available):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'System-OOM profile lost its exact recovery context.')
        if (launch_fence is None or inspect_bound_ordinary_launch is None or
                reduce_bound_ordinary_launch is None or
                cancel_bound_ordinary_launch is None):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Bound ordinary launch requires complete context and exact '
                'inspection, reduction, and cancellation authority.')
        controller_api_auth_token_provider = (
            _required_controller_admin_auth_tokens)
        bound_workspace_ctx: contextlib.AbstractContextManager = (
            skypilot_config.local_active_workspace_ctx(workspace)
            if workspace is not None else contextlib.nullcontext())
        usage_lib.messages.usage.set_internal()
        with bound_workspace_ctx, server_common.serve_controller_api_auth(
                controller_api_auth_token_provider):
            # Freeze once. Every transport retry below submits these exact
            # bytes with the same controller-generated UUID.
            prepared_request = sdk.prepare_launch_request(
                task,
                cluster_name,
                retry_until_up=retry_until_up,
                _is_launched_by_sky_serve_controller=True,
                _extra_launch_context=launch_fence)
        expected_input_digest = ordinary_launch_binding.canonical_launch_digest(
            prepared_request.body)
        submit_backoff = common_utils.Backoff(_RETRY_INIT_GAP_SECONDS)
        request_id = None
        adopted_after_lost_ack = False
        for submit_attempt in range(1, max_retry + 1):
            if _check_is_cancelled():
                return
            _assert_launch_not_superseded()
            cloud_launch_allowed, cloud_launch_reason = _cloud_guard_decision()
            if not cloud_launch_allowed:
                raise _ReplicaLaunchSupersededError(
                    f'Refusing superseded cloud launch for replica '
                    f'{replica_id}: reason={cloud_launch_reason}.')
            _assert_launch_authorized()
            try:
                with (skypilot_config.local_active_workspace_ctx(workspace)
                      if workspace is not None else contextlib.nullcontext()), \
                     server_common.serve_controller_api_auth(
                         controller_api_auth_token_provider):
                    if generic_profile_kind is None:
                        request_id = (
                            sdk.submit_prepared_ordinary_launch_request(
                                prepared_request,
                                ordinary_launch_submission_uuid))
                    else:
                        request_id = (
                            sdk.submit_prepared_non_pool_launch_request(
                                prepared_request,
                                ordinary_launch_submission_uuid,
                                generic_profile_kind.value))
                break
            except Exception as error:  # pylint: disable=broad-except
                if not _bound_submission_may_have_committed(error):
                    # A deterministic rejection (most importantly a 4xx digest
                    # or identity conflict) cannot be converted into a lost-ACK
                    # adoption of some older exact replica pointer.
                    raise
                # A response may be lost after the atomic transaction commits.
                # Resolve only through this replica record's exact durable
                # pointer; request history/latest inference is forbidden.
                try:
                    snapshot = inspect_bound_ordinary_launch()
                except Exception as inspect_error:  # pylint: disable=broad-except
                    logger.warning(
                        'Could not inspect exact bound admission for replica '
                        '%s after transport failure: %s', replica_id,
                        common_utils.format_exception(inspect_error))
                else:
                    if snapshot is not None:
                        snapshot_context = getattr(snapshot, 'context', None)
                        snapshot_digest = getattr(snapshot_context,
                                                  'input_digest', None)
                        if snapshot_digest != expected_input_digest:
                            raise _BoundOrdinaryLaunchUnresolvedError(
                                'Lost-ACK inspection found a bound request with '
                                'a different canonical launch digest.'
                            ) from error
                        exact_request_id = _bound_reduction_request_id(snapshot)
                        request_id = server_common.RequestId[tuple[
                            int | None,
                            backends.ResourceHandle | None]](exact_request_id)
                        adopted_after_lost_ack = True
                        logger.warning(
                            'Adopting exact bound request %s for replica %s '
                            'after its admission acknowledgement was lost.',
                            request_id, replica_id)
                        break
                if submit_attempt >= max_retry:
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        f'Could not resolve bound ordinary launch admission for '
                        f'replica {replica_id} after {submit_attempt} exact '
                        'submission attempt(s).') from error
                delay = submit_backoff.current_backoff()
                logger.warning(
                    f'Bound ordinary launch admission for replica {replica_id} '
                    f'did not acknowledge attempt {submit_attempt}; retrying '
                    f'the same submission UUID in {delay:.1f} seconds.')
                deadline = time.monotonic() + delay
                while time.monotonic() < deadline:
                    if _check_is_cancelled():
                        return
                    time.sleep(min(0.1, deadline - time.monotonic()))
        assert request_id is not None
        logger.info(f'Replica cluster {cluster_name} bound launch requested '
                    f'with request_id: {request_id}.')
        replica_to_request_id[replica_id] = request_id
        _emit_ordinary_launch_event(
            ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED, request_id)
        try:
            _wait_for_bound_ordinary_launch(
                replica_id,
                cluster_name,
                str(request_id),
                not adopted_after_lost_ack,
                next(iter(task.resources)).cloud,
                reduce_bound_ordinary_launch,
                cancel_bound_ordinary_launch,
                teardown_requested,
                continue_guard=continue_guard,
                supersession_guard=supersession_guard,
                api_auth_token_provider=controller_api_auth_token_provider)
        except Exception:
            _observe_terminal_nonblocking(request_id)
            raise
        _observe_terminal_nonblocking(request_id)
        logger.info(f'Replica cluster {cluster_name} launch was exactly '
                    'projected.')
        return

    # This remains the current launch retry/request owner.  A future bounded
    # request-binding change may make the exact request association durable,
    # but the retired action-authority design does not deprecate this loop.
    retry_cnt = 0
    availability_retry_cnt = 0
    backoff = common_utils.Backoff(_RETRY_INIT_GAP_SECONDS)
    while True:
        retry_cnt += 1
        capacity_error: exceptions.ResourcesUnavailableError | None = None
        availability_reason: str | None = None
        recovery_request_attempted = False
        try:
            if _check_is_cancelled():
                return
            _assert_launch_not_superseded()
            cloud_launch_allowed, cloud_launch_reason = (
                _cloud_guard_decision())
            if not cloud_launch_allowed:
                raise _ReplicaLaunchSupersededError(
                    f'Refusing superseded cloud launch for replica '
                    f'{replica_id}: reason={cloud_launch_reason}.')
            # This is the authoritative DB-backed check immediately before
            # every cloud mutation. The shared watchdog event is a second,
            # cheap fence for an already-running request.
            _assert_launch_authorized()
            if (launch_fence is None and
                    serve_utils.is_external_load_balancer_mode()):
                # The API rejects every controller-originated launch without
                # the durable service-owner tuple. This occurs for pre-fence
                # legacy rows recovered after external-LB mode is enabled.
                # Retrying the same HTTP 409 can never repair the missing
                # lifecycle fence; fail once with a typed error so the manager
                # records one unrecoverable replica instead of appending
                # failed rows forever.
                raise _UnfencedExternalLbLaunchError(
                    f'Refusing to launch replica {replica_id} for legacy '
                    'service or pool without a durable owner fence. Purge '
                    'and recreate it to establish a current lifecycle fence.')
            usage_lib.messages.usage.set_internal()
            launch_kwargs: dict[str, Any] = {}
            launch_context = (system_recovery_launch_context
                              if recovery_context_available else launch_fence)
            if (ordinary_launch_handoff_context is not None and
                    not recovery_context_available):
                # Reuse the existing internal launch context transport.  This
                # metadata is diagnostic only and remains separate from every
                # provider-authority fence inside one nested versioned value.
                # The initial recovery context is an exact-key contract; add
                # this value only after durable demotion makes a retry ordinary.
                launch_context = dict(launch_context or {})
                launch_context[
                    serve_constants.ORDINARY_LAUNCH_HANDOFF_CONTEXT_KEY] = (
                        dict(ordinary_launch_handoff_context))
            if launch_context is not None:
                launch_kwargs['_extra_launch_context'] = launch_context
            workspace_ctx: contextlib.AbstractContextManager = (
                skypilot_config.local_active_workspace_ctx(workspace)
                if workspace is not None else contextlib.nullcontext())
            try:
                with workspace_ctx:
                    recovery_request_attempted = recovery_context_available
                    request_id = sdk.launch(
                        task,
                        cluster_name,
                        retry_until_up=retry_until_up,
                        _is_launched_by_sky_serve_controller=True,
                        **launch_kwargs)
            except Exception:  # pylint: disable=broad-except
                if not recovery_request_attempted:
                    raise
                # The response can be lost after /launch durably bound the
                # server-known request ID. Adopt only that exact association;
                # never search request history or infer the latest request.
                bound_request_id = _bound_recovery_request_id()
                if bound_request_id is None:
                    _demote_recovery_candidate()
                    raise
                request_id = server_common.RequestId[
                    tuple[int | None,
                          backends.ResourceHandle | None]](bound_request_id)
                logger.warning(
                    f'Adopting bound recovery launch request {request_id} for '
                    f'replica {replica_id} after sdk.launch did not return it.')
            if recovery_request_attempted:
                bound_request_id = _bound_recovery_request_id()
                if request_id != bound_request_id:
                    raise _SystemRecoveryLaunchCaptureError(
                        f'Recovery launch request mismatch for replica '
                        f'{replica_id}: returned={request_id!r}, '
                        f'bound={bound_request_id!r}.')
            logger.info(f'Replica cluster {cluster_name} launch requested '
                        f'with request_id: {request_id}.')
            replica_to_request_id[replica_id] = request_id
            if not recovery_request_attempted:
                _emit_ordinary_launch_event(
                    ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
                    request_id)
            try:
                launch_result = _stream_with_launch_watchdogs(request_id)
            except Exception:  # pylint: disable=broad-except
                if not recovery_request_attempted:
                    _observe_terminal_nonblocking(request_id)
                raise
            if not recovery_request_attempted:
                _observe_terminal_nonblocking(request_id)
            service_job_id = (launch_result[0] if isinstance(
                launch_result, tuple) and len(launch_result) == 2 and
                              isinstance(launch_result[0], int) and
                              not isinstance(launch_result[0], bool) and
                              launch_result[0] > 0 else None)
            if service_job_id is not None and not recovery_request_attempted:
                _emit_ordinary_launch_event(
                    ordinary_launch_handoff.EventKind.SERVICE_JOB_OBSERVED,
                    request_id, service_job_id)
            _assert_launch_not_superseded()
            _assert_launch_authorized()
            if recovery_request_attempted:
                _capture_recovery_result(request_id, launch_result)
            logger.info(f'Replica cluster {cluster_name} launched.')
        except _ReplicaLaunchOwnershipLostError:
            # The in-memory request entry belongs to this stale manager only.
            # Keep the durable replica row for the successor to re-drive or
            # garbage-collect; discard local bookkeeping.
            replica_to_request_id.pop(replica_id)
            raise
        except _ReplicaLaunchSupersededError:
            raise
        except _UnfencedExternalLbLaunchError:
            raise
        except reserved_capacity.ReservedFillLaunchFenceError:
            # The executor/provisioner has proved that this durable request
            # no longer matches its exact pool generation, Kubernetes
            # context, physical UID, or accelerator pin. Retrying the same
            # request and cleaning through that rejected authority can never
            # repair it and may cross a retargeted alias.
            raise
        except exceptions.KubernetesPhysicalClusterIdentityError:
            # Immediate cleanup through a retargeted alias would be the unsafe
            # action.  Preserve the typed result for the manager, whose durable
            # row drives only identity-fenced cleanup retries.
            raise
        except exceptions.ServeReplicaLaunchFenceError:
            # A provider-side durable owner/generation fence is terminal for
            # this API request. Re-evaluate the local guards so an ordinary
            # version transition keeps its typed supersession handling. The
            # exact durable replica row remains available to the current
            # manager for reconciliation; do not retry or clean up through the
            # stale request's authority.
            _assert_launch_not_superseded()
            _assert_launch_authorized()
            raise
        except (exceptions.InvalidClusterNameError,
                exceptions.NoCloudAccessError,
                exceptions.ResourcesMismatchError) as e:
            if recovery_request_attempted and recovery_context_available:
                _demote_recovery_candidate()
            logger.error('Failure happened before provisioning. '
                         f'{common_utils.format_exception(e)}')
            raise RuntimeError('Failed to launch the sky serve replica '
                               f'cluster {cluster_name}.') from e
        except exceptions.ResourcesUnavailableError as e:
            if ownership_lost.is_set():
                replica_to_request_id.pop(replica_id)
                raise _ReplicaLaunchOwnershipLostError(
                    f'Replica {replica_id} launch was cancelled after '
                    'controller ownership loss.') from e
            launch_cloud = next(iter(task.resources)).cloud
            if launch_cloud is not None:
                availability_reason = (
                    cloud_vm_ray_backend.classify_resources_unavailable_error(
                        launch_cloud, e))
            if availability_reason is not None:
                capacity_error = e
                availability_retry_cnt += 1
            logger.info('Failed to launch the sky serve replica cluster with '
                        f'error: {common_utils.format_exception(e)})')
        except Exception as e:  # pylint: disable=broad-except
            if ownership_lost.is_set():
                replica_to_request_id.pop(replica_id)
                raise _ReplicaLaunchOwnershipLostError(
                    f'Replica {replica_id} launch was cancelled after '
                    'controller ownership loss.') from e
            logger.info('Failed to launch the sky serve replica cluster with '
                        f'error: {common_utils.format_exception(e)})')
            with ux_utils.enable_traceback():
                logger.info(f'  Traceback: {traceback.format_exc()}')
        else:  # No exception, the launch succeeds.
            return

        if recovery_request_attempted and recovery_context_available:
            # At most the first outer request carries recovery authority. A
            # later request is allowed only after this generation is durably
            # and irreversibly ordinary; its historical association remains.
            _demote_recovery_candidate()

        # Cleanup the request id and the failed cluster.
        replica_to_request_id.pop(replica_id)
        # If it is cancelled, no need to terminate the cluster. It will be
        # handled by the termination thread.
        if _check_is_cancelled():
            return

        terminal = (retry_cnt >= max_retry or
                    availability_retry_cnt >= availability_max_retry)
        if terminal and capacity_error is not None:
            # A typed availability error reaches this layer only after the
            # backend's failover cleanup succeeded (or proved no nodes were
            # created). Let the manager persist that feedback before its
            # idempotent replica cleanup; waiting for another controller-side
            # down here delays the next exact-pool decision.
            raise _ReplicaLaunchCapacityError(
                'Failed to launch the sky serve replica cluster '
                f'{cluster_name} due to provider {availability_reason} '
                f'after {retry_cnt} attempt(s).',
                reason=typing.cast(str,
                                   availability_reason)) from capacity_error

        terminate_cluster(cluster_name, continue_guard=cleanup_continue_guard)

        if terminal:
            raise RuntimeError('Failed to launch the sky serve replica cluster '
                               f'{cluster_name} after {retry_cnt} attempt(s).')

        gap_seconds = backoff.current_backoff()
        logger.info('Retrying to launch the sky serve replica cluster '
                    f'in {gap_seconds:.1f} seconds.')
        backoff_deadline = time.monotonic() + gap_seconds
        # Check if it is cancelled every 0.1 seconds.
        while True:
            if _check_is_cancelled():
                return
            remaining = backoff_deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.1, remaining))


@context.contextual
def launch_cluster_with_frozen_controller_config(
        *args: Any, frozen_controller_config: Any,
        frozen_controller_config_path: str | None, **kwargs: Any) -> None:
    """Run a launch worker under its construction-time config generation."""
    ctx = context.get()
    assert ctx is not None, 'Context is not initialized'
    if frozen_controller_config_path is None:
        os.environ.pop(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, None)
    else:
        ctx.override_envs({
            skypilot_config.ENV_VAR_SKYPILOT_CONFIG: frozen_controller_config_path,
        })
    with skypilot_config.replace_skypilot_config_in_memory(
            frozen_controller_config):
        launch_cluster(*args, **kwargs)


def _wait_for_drain(drain_deadline: float,
                    drain_complete: Callable[[], bool] | None) -> None:
    """Wait for a retiring replica to drain, bounded by the deadline.

    The deadline is anchored at the moment the replica's SHUTTING_DOWN
    status was persisted (not at thread start), so time spent queued in
    the down-thread admission pass counts toward the drain budget instead
    of extending it. Without a `drain_complete` predicate this is a plain
    bounded sleep. With one, poll it and proceed as soon as it reports the
    replica drained; a predicate that never fires (no fresh LB report,
    requests still in flight) degrades to the deadline. A predicate
    failure must never break the teardown: it is treated as not-drained
    and the wait continues.
    """
    if drain_complete is None:
        time.sleep(max(drain_deadline - time.monotonic(), 0))
        return
    start = time.monotonic()
    check_failures = 0
    while time.monotonic() < drain_deadline:
        try:
            if drain_complete():
                logger.info('Replica reported drained after '
                            f'{time.monotonic() - start:.0f}s of waiting.')
                return
        except Exception as e:  # pylint: disable=broad-except
            # First failure at WARNING, the rest at DEBUG: a persistently
            # raising check would otherwise emit one warning per poll for
            # the whole drain window.
            log = logger.warning if check_failures == 0 else logger.debug
            check_failures += 1
            log('Drain check failed; continuing to wait: '
                f'{common_utils.format_exception(e)}')
        time.sleep(
            min(_DRAIN_POLL_SECONDS, max(drain_deadline - time.monotonic(), 0)))
    logger.info('Drain deadline reached; proceeding with termination.')


def _ensure_drain_started_at(status: 'ReplicaStatusProperty',
                             drain_cap_seconds: int | None) -> float | None:
    """Return the durable wall-clock start for one bounded drain.

    Monotonic timestamps cannot survive a controller restart. Persisting the
    wall-clock start keeps the configured cap bounded across recovery while a
    fresh monotonic tracker is still used for each controller incarnation's LB
    reports. Legacy or malformed rows get one conservative full-cap window.
    """
    if drain_cap_seconds is None or drain_cap_seconds <= 0:
        status.drain_started_at = None
        return None
    now = time.time()
    started_at = status.drain_started_at
    if not _is_valid_drain_started_at(started_at):
        started_at = now
        status.drain_started_at = started_at
    else:
        assert isinstance(started_at, (int, float))
        if started_at > now + _DRAIN_WALL_CLOCK_FUTURE_SKEW_SECONDS:
            started_at = now
            status.drain_started_at = started_at
    assert isinstance(started_at, (int, float))
    return float(started_at)


def _remaining_drain_seconds(started_at: float,
                             drain_cap_seconds: int) -> float:
    """Return a restart-safe, fail-closed remainder for a bounded drain."""
    now = time.time()
    # A small future timestamp can result from bounded clock skew. Treat it as
    # zero elapsed until the wall clock catches up.
    elapsed = max(now - started_at, 0.0)
    return max(float(drain_cap_seconds) - elapsed, 0.0)


def _classify_abort_reason(reason: str) -> str:
    """Map a free-text abort reason onto the bounded counter key set."""
    lowered = reason.lower()
    if 'covers the target' in lowered or 'coverage' in lowered:
        return drain_observability.ABORT_REASON_TARGET_COVERAGE
    if 'idle proof' in lowered:
        return drain_observability.ABORT_REASON_IDLE_PROOF_TIMEOUT
    if 'fence' in lowered or 'target or controller' in lowered:
        return drain_observability.ABORT_REASON_FENCE_CHANGED
    return drain_observability.ABORT_REASON_OTHER


class _ReplicaDrainTracker:
    """Stateful drain-complete predicate for one retiring replica.

    'Drained' requires SEEN-THEN-CLEAN: a fresh authoritative report at
    retirement selection, or received after the drain began, must have
    acknowledged the replica's url (in the routing view, the in-flight gauge,
    the unknown set, or the draining set) before a later fresh report showing
    it absent-and-idle is trusted -- and both must come from the SAME LB
    incarnation (the LB ships a per-process session id): a cold LB (restarted
    mid-drain, its
    draining and occupancy overlays lost) ships empty sets and must not
    'prove' any replica drained, with or without an older incarnation's
    acknowledgement. An explicit idle entry (gauge zero, or a
    post-retirement occupancy zero) is both seen and clean at once. A url
    ever reported occupancy-UNKNOWN in this incarnation is tainted: only
    an explicit idle entry (never absence, which may just be the LB's
    off-ready retention expiring) can complete its drain.
    Matching by the replica's own url -- known from its record, stable
    for the cluster's lifetime -- needs no id translation, so translation
    gaps (cold cache after a controller restart, urls of already-removed
    replicas lingering in the LB's retention) can neither mask the target
    nor block unrelated drains. No report, an old LB that ships no
    routing view, and stale reports (LB dead / not reporting) all answer
    False, degrading the wait to its deadline.
    """

    def __init__(self, manager: 'ReplicaManager', replica_url: str,
                 drain_started: float) -> None:
        self._manager = manager
        self._replica_url = replica_url
        self._drain_started = drain_started
        self._seen = False
        self._unknown_tainted = False
        self._session: str | None = None
        self._seed_from_existing_report()

    def _seed_from_existing_report(self) -> None:
        """Carry a fresh pre-retirement LB acknowledgement into the drain.

        Route removal is applied in the response to a sync. An idle client can
        disappear before the next sync, so requiring the newly constructed
        tracker to observe the url again would force the full drain deadline.
        The prior report is only an acknowledgement, never a clean proof: a
        later report from the same LB session must still show the url idle.
        """
        report = self._manager._lb_in_flight_report  # pylint: disable=protected-access
        if report is None:
            return
        (received_at, in_flight, routing_urls, unknown_urls, draining_urls,
         session) = report
        if routing_urls is None or not isinstance(session, str) or not session:
            return
        if (time.monotonic() - received_at
                > _IN_FLIGHT_REPORT_STALENESS_SECONDS):
            return
        url = self._replica_url
        if (url not in routing_urls and url not in unknown_urls and
                url not in draining_urls and url not in in_flight):
            return
        self._session = session
        self._seen = True
        self._unknown_tainted = url in unknown_urls

    def __call__(self) -> bool:
        report = self._manager._lb_in_flight_report  # pylint: disable=protected-access
        if report is None:
            return False
        (received_at, in_flight, routing_urls, unknown_urls, draining_urls,
         session) = report
        if received_at < self._drain_started:
            return False
        if routing_urls is None:
            return False
        if (time.monotonic() - received_at
                > _IN_FLIGHT_REPORT_STALENESS_SECONDS):
            return False
        url = self._replica_url
        if session != self._session:
            # A different LB incarnation: its overlays started empty, so
            # acknowledgements from the previous one prove nothing about
            # its reports.
            self._session = session
            self._seen = False
            self._unknown_tainted = False
        if (url in routing_urls or url in unknown_urls or
                url in draining_urls or url in in_flight):
            self._seen = True
        if url in unknown_urls:
            # Async occupancy was unproven at least once this incarnation:
            # a later ABSENCE cannot prove idleness (the LB's off-ready
            # retention may simply have expired). Only an explicit idle
            # entry clears the taint.
            self._unknown_tainted = True
        explicit_idle = url in in_flight and in_flight[url] == 0
        if explicit_idle and url not in unknown_urls:
            # Defensive ordering: an inconsistent report listing the url
            # both unknown and explicitly idle must not clear the taint.
            self._unknown_tainted = False
        blocked = (url in routing_urls or url in unknown_urls or
                   in_flight.get(url, 0) != 0 or
                   (self._unknown_tainted and not explicit_idle))
        return self._seen and not blocked


@dataclasses.dataclass(frozen=True)
class _WaitForIdleState:
    """Exact drain wait whose optional URL evidence is resolved lock-free."""

    replica_record_id: str
    deadline: float
    tracker: _ReplicaDrainTracker | None = None
    needs_url_resolution: bool = True


# TODO(tian): Combine this with
# sky/spot/recovery_strategy.py::terminate_cluster
def _wait_for_post_teardown_physical_absence(
    cluster_name: str,
    cleanup_fence: reserved_capacity.ProtocolV2CleanupFence,
    continue_guard: Callable[[], bool] | None,
    cluster_name_on_cloud: str | None = None,
    observed_after: float | None = None,
) -> reserved_capacity.ProtocolV2PhysicalAbsenceReceipt:
    """Wait for one successful protocol-v2 down to become observable.

    Kubernetes deletion is asynchronous even with a zero grace period.  Each
    causally fresh provider read gets its own short provider phase and
    immutable physical-cluster fence; the gate is released before the next
    poll sleeps.
    Only ABSENT completes teardown.  PRESENT, an unreadable/retargeted context,
    or ownership loss remains fail closed.
    """
    deadline = time.monotonic() + _POST_TEARDOWN_ABSENCE_TIMEOUT_SECONDS
    last_presence = reserved_capacity.PhysicalReplicaPresence.UNPROVEN
    while True:
        if continue_guard is not None and not continue_guard():
            raise RuntimeError(
                f'Refusing to confirm termination of {cluster_name!r} after '
                'service lifecycle ownership was lost.')
        remaining = max(0.0, deadline - time.monotonic())
        try:
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.V2_FENCED,
                    timeout_seconds=remaining):
                probe_kwargs: dict[str, Any] = {}
                if observed_after is not None:
                    probe_kwargs['observed_after'] = observed_after
                if cluster_name_on_cloud is not None:
                    probe_kwargs['cluster_name_on_cloud'] = (
                        cluster_name_on_cloud)
                last_presence = reserved_capacity.probe_physical_replica_presence(
                    cleanup_fence, cluster_name, **probe_kwargs)
        except exceptions.ProviderPhaseTimeoutError as error:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                f'Cannot prove protocol-v2 cleanup for {cluster_name!r}: '
                'timed out waiting to observe post-teardown provider '
                'absence.') from error

        # Ownership can change during a provider read.  Recheck before
        # accepting its result as cleanup authority.
        if continue_guard is not None and not continue_guard():
            raise RuntimeError(
                f'Refusing to confirm termination of {cluster_name!r} after '
                'service lifecycle ownership was lost.')
        if last_presence is reserved_capacity.PhysicalReplicaPresence.ABSENT:
            return reserved_capacity.ProtocolV2PhysicalAbsenceReceipt(
                cleanup_fence=cleanup_fence, cluster_name=cluster_name)

        # PRESENT and UNPROVEN must not pin the next poll to a cached provider
        # result.  Advance the causal floor after observing this result; the
        # next poll can share only a read that starts after this boundary.
        observed_after = time.monotonic()

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                f'Cannot prove protocol-v2 cleanup for {cluster_name!r}: '
                'post-teardown provider presence remained '
                f'{last_presence.value.lower()} for '
                f'{_POST_TEARDOWN_ABSENCE_TIMEOUT_SECONDS} seconds.')
        time.sleep(min(_POST_TEARDOWN_ABSENCE_POLL_SECONDS, remaining))


@context.contextual_without_log
def terminate_cluster(
    cluster_name: str,
    replica_drain_delay_seconds: int = 0,
    max_retry: int = 3,
    drain_deadline: float | None = None,
    drain_complete: Callable[[], bool] | None = None,
    continue_guard: Callable[[], bool] | None = None,
    expected_cluster_record_uuid: str | None = None,
    cleanup_fence: reserved_capacity.ProtocolV2CleanupFence | None = None
) -> reserved_capacity.ProtocolV2PhysicalAbsenceReceipt | None:
    """Terminate the sky serve replica cluster."""
    from sky import core  # pylint: disable=import-outside-toplevel

    logger.info(f'Terminating replica cluster {cluster_name} with '
                f'replica_drain_delay_seconds: {replica_drain_delay_seconds}, '
                f'drain_deadline: {drain_deadline}')
    if drain_deadline is not None:
        _wait_for_drain(drain_deadline, drain_complete)
    else:
        time.sleep(replica_drain_delay_seconds)

    # This remains the current down retry owner.  Cleanup intent is durable;
    # the retired action-authority design does not replace this loop.
    retry_cnt = 0
    backoff = common_utils.Backoff()
    cleanup_cluster_name_on_cloud: str | None = None
    teardown_completed_at: float | None = None
    while True:
        if continue_guard is not None and not continue_guard():
            raise RuntimeError(
                f'Refusing to retry termination of {cluster_name!r} after '
                'service lifecycle ownership was lost.')
        retry_cnt += 1
        try:
            usage_lib.messages.usage.set_internal()
            logger.info(f'Sending down request to cluster {cluster_name}')
            expected_cluster_record_handle = None
            if cleanup_fence is None:
                (phase_mode, expected_cluster_record_handle) = (
                    _ordinary_cleanup_phase_authority(
                        cluster_name, expected_cluster_record_uuid))
            else:
                phase_mode = provider_phase.ProviderPhaseMode.V2_FENCED
            phase_context: contextlib.AbstractContextManager[Any] = (
                contextlib.nullcontext() if phase_mode is None else
                provider_phase.provider_phase(phase_mode))
            with phase_context:
                # Every retry re-reads the durable handle, generation, and
                # workspace only after phase admission. A transient failure
                # cannot reuse authority captured by an earlier attempt.
                cluster_record = global_user_state.get_cluster_from_name(
                    cluster_name)
                if cluster_record is None and cleanup_fence is not None:
                    # The provider may have completed deletion before the
                    # central cluster row was observed.  The immutable cleanup
                    # fence remains sufficient only to prove physical absence;
                    # it never authorizes another name-only down.  Leave this
                    # phase and obtain that exact uncached proof below.
                    logger.info(
                        f'Replica cluster {cluster_name!r} has no durable '
                        'cluster record; proving fenced physical absence.')
                    teardown_completed_at = time.monotonic()
                    break
                expected_cluster_hash = None
                if cleanup_fence is not None:
                    assert cluster_record is not None
                    handle = cluster_record.get('handle')
                    if not isinstance(
                            handle,
                            cloud_vm_ray_backend.CloudVmRayResourceHandle):
                        raise exceptions.KubernetesPhysicalClusterIdentityError(
                            f'Cannot prove protocol-v2 cleanup for '
                            f'{cluster_name!r}: its durable cluster handle '
                            'does not match the fenced Kubernetes context.')
                    launched_resources = handle.launched_resources
                    if (handle.cluster_name != cluster_name or
                            launched_resources is None or not isinstance(
                                launched_resources.cloud, clouds.Kubernetes) or
                            launched_resources.region
                            != cleanup_fence.kubernetes_context):
                        raise exceptions.KubernetesPhysicalClusterIdentityError(
                            f'Cannot prove protocol-v2 cleanup for '
                            f'{cluster_name!r}: its durable cluster handle '
                            'does not match the fenced Kubernetes context.')
                    observed_name_on_cloud = handle.cluster_name_on_cloud
                    cleanup_cluster_name_on_cloud = (
                        observed_name_on_cloud
                        if isinstance(observed_name_on_cloud, str) and
                        observed_name_on_cloud else None)
                    expected_cluster_hash = cluster_record.get('cluster_hash')
                    if (not isinstance(expected_cluster_hash, str) or
                            not expected_cluster_hash):
                        raise exceptions.KubernetesPhysicalClusterIdentityError(
                            f'Cannot prove protocol-v2 cleanup for '
                            f'{cluster_name!r}: its durable cluster generation '
                            'hash is absent.')
                    if expected_cluster_record_uuid is not None:
                        exact_snapshot = (global_user_state.
                                          get_cluster_record_identity_snapshot(
                                              cluster_name,
                                              expected_cluster_record_uuid))
                        if exact_snapshot is None:
                            raise global_user_state.ClusterRecordIdentityConflictError(
                                f'Cluster {cluster_name!r} disappeared before '
                                'protocol-v2 cleanup could validate its exact '
                                'record UUID.')
                cluster_workspace = (cluster_record.get('workspace')
                                     if cluster_record is not None else None)
                workspace_ctx: contextlib.AbstractContextManager = (
                    skypilot_config.local_active_workspace_ctx(
                        cluster_workspace)
                    if cluster_workspace else contextlib.nullcontext())
                physical_fence: contextlib.AbstractContextManager[None] = (
                    contextlib.nullcontext() if cleanup_fence is None else
                    kubernetes_adaptor.physical_cluster_uid_fence(
                        cleanup_fence.kubernetes_context,
                        cleanup_fence.physical_cluster_uid))
                # Workspace selection owns kubeconfig/environment resolution;
                # enter it before creating this attempt's immutable capture.
                with workspace_ctx, physical_fence:
                    if cleanup_fence is None:
                        core.down(cluster_name,
                                  _expected_cluster_record_uuid=(
                                      expected_cluster_record_uuid),
                                  _expected_cluster_record_handle=(
                                      expected_cluster_record_handle))
                    elif expected_cluster_record_uuid is not None:
                        core.down(cluster_name,
                                  _expected_cluster_record_uuid=(
                                      expected_cluster_record_uuid),
                                  _continue_guard=continue_guard)
                    else:
                        core.down(cluster_name,
                                  _expected_cluster_hash=expected_cluster_hash,
                                  _continue_guard=continue_guard)
                teardown_completed_at = time.monotonic()
            break
        except exceptions.ClusterDoesNotExist:
            if cleanup_fence is not None:
                # ``core.down`` may race the provider or another exact cleanup
                # owner after validating the durable record.  Do not infer
                # absence from this exception and do not retry by name.  The
                # fenced uncached provider observation below is the only
                # authority that can complete this cleanup.
                logger.info(
                    f'Replica cluster {cluster_name!r} disappeared during '
                    'teardown; proving fenced physical absence.')
                teardown_completed_at = time.monotonic()
                break
            # The cluster is already terminated.
            logger.info(
                f'Replica cluster {cluster_name} is already terminated.')
            return None
        except global_user_state.ClusterRecordHandleChangedError as error:
            # A same-generation handle update is retryable: the next attempt
            # reclassifies the fresh provider before selecting its phase.
            if retry_cnt >= max_retry:
                raise RuntimeError('Failed to terminate the sky serve replica '
                                   f'cluster {cluster_name}.') from error
            gap_seconds = backoff.current_backoff()
            logger.info(f'Cluster {cluster_name!r} handle changed during '
                        'teardown classification. Retrying after '
                        f'{gap_seconds} seconds.')
            time.sleep(gap_seconds)
        except global_user_state.ClusterRecordIdentityConflictError:
            # A different/null durable identity is not a transient provider
            # failure. Never turn the exact action fence into repeated
            # name-only teardown attempts.
            raise
        except exceptions.KubernetesPhysicalClusterIdentityError:
            # A replacement target is not a transient provider failure.  The
            # durable row remains cleanup-uncertain and a later reconciliation
            # constructs a fresh capture after identity is restored.
            raise
        except Exception as e:  # pylint: disable=broad-except
            if retry_cnt >= max_retry:
                raise RuntimeError('Failed to terminate the sky serve replica '
                                   f'cluster {cluster_name}.') from e
            gap_seconds = backoff.current_backoff()
            logger.error(
                'Failed to terminate the sky serve replica cluster '
                f'{cluster_name}. Retrying after {gap_seconds} seconds.'
                f'Details: {common_utils.format_exception(e)}')
            logger.error(f'  Traceback: {traceback.format_exc()}')
            time.sleep(gap_seconds)

    absence_receipt = None
    if cleanup_fence is not None:
        if teardown_completed_at is None:
            raise RuntimeError('Protocol-v2 cleanup has no causal teardown '
                               'completion boundary.')
        absence_receipt = _wait_for_post_teardown_physical_absence(
            cluster_name,
            cleanup_fence,
            continue_guard,
            cluster_name_on_cloud=cleanup_cluster_name_on_cloud,
            observed_after=teardown_completed_at)
    logger.info(f'Replica cluster {cluster_name} terminated.')
    return absence_receipt


def terminate_bound_non_pool_provider_present_cluster(
    binding_context: 'BoundNonPoolLaunchContext',
    replica_info: ReplicaInfo,
    authority: 'ControllerBindingAuthority',
    project_replica_result: Callable[..., bool],
    cluster_name: str,
    replica_drain_delay_seconds: int = 0,
    **terminate_kwargs: Any,
) -> None:
    """Down one exact PRESENT allocation, then project fresh ABSENT proof."""
    if ordinary_launch_binding.is_paid_provider_reconciliation_profile(
            binding_context.profile.kind):
        expected_cluster_record_uuid = terminate_kwargs.get(
            'expected_cluster_record_uuid')
        if expected_cluster_record_uuid is None:
            if terminate_kwargs.get('cleanup_fence') is not None:
                raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                    'Paid GCP cleanup acquired a reserved-fill fence.')
            (non_pool_launch_reconciliation.
             terminate_gcp_paid_provider_allocation)(
                 binding_context,
                 replica_info,
                 authority,
                 project_replica_result,
                 continue_guard=terminate_kwargs.get('continue_guard'))
            return
        # A live exact SkyPilot cluster row retains the complete backend
        # teardown context. Use it to remove the provider object and cluster
        # metadata, then independently prove the frozen GCP label is absent.
        terminate_cluster(cluster_name, replica_drain_delay_seconds,
                          **terminate_kwargs)
        observation = (
            non_pool_launch_reconciliation.
            terminate_gcp_paid_provider_allocation)(
                binding_context,
                replica_info,
                authority,
                project_replica_result,
                continue_guard=terminate_kwargs.get('continue_guard'))
        assert (observation.evidence
                is ordinary_launch_binding.ProviderEvidence.ABSENT)
        return
    absence_receipt = terminate_cluster(cluster_name,
                                        replica_drain_delay_seconds,
                                        **terminate_kwargs)
    if absence_receipt is None:
        raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'Fenced provider cleanup returned no exact physical ABSENT '
            'receipt for the reserved-fill allocation.')
    non_pool_launch_reconciliation.reconcile_post_teardown_absence(
        binding_context, replica_info, authority, project_replica_result,
        absence_receipt)


def terminate_cluster_with_kueue_absence_receipt(
    service_name: str,
    replica_id: int,
    replica_record_id: str,
    cluster_name: str,
    replica_drain_delay_seconds: int = 0,
    **terminate_kwargs: Any,
) -> None:
    """Down one cluster and record exact admitted-Pod absence when applicable."""
    if replica_drain_delay_seconds:
        terminate_kwargs['replica_drain_delay_seconds'] = (
            replica_drain_delay_seconds)
    terminate_cluster(cluster_name, **terminate_kwargs)
    kueue_lane_observer.project_exact_pod_absence_after_teardown(
        service_name, replica_id, replica_record_id)


def _get_resources_ports(
    yaml_content: str,
    service_spec: 'service_spec.SkyServiceSpec | None' = None,
    task_template: task_lib.Task | None = None,
) -> str:
    """Get the replica ingress port from the service or its resources."""
    task = (task_template if task_template is not None else
            load_task_with_service_spec(yaml_content, service_spec))
    # Already checked all ports are valid in sky.serve.core.up
    assert task.resources, task
    assert task.service is not None, task
    return serve_utils.resolve_replica_ingress_port(task,
                                                    pool=task.service.pool)


def _load_spot_placer(
    service_name: str,
    version: int,
    service_spec: 'service_spec.SkyServiceSpec',
    task: 'task_lib.Task',
    workspace: str | None = None,
) -> spot_placer.SpotPlacer | None:
    """Load one version's durable catalog without provider resolution."""
    if not service_spec.placement_contract.enabled:
        return None
    catalog_data = serve_state.get_placement_catalog(service_name, version)
    if catalog_data is None:
        raise RuntimeError(
            f'Placement catalog is missing for {service_name!r} version '
            f'{version}. The service parent must backfill legacy versions '
            'before starting the controller.')
    return spot_placer.SpotPlacer.from_task(service_spec,
                                            task,
                                            placement_catalog=catalog_data,
                                            workspace=workspace)


def validate_service_update_preflight(
    service_name: str,
    version: int,
    service_spec: 'service_spec.SkyServiceSpec',
    workspace: str | None = None,
) -> spot_placer.SpotPlacer | None:
    """Run immutable candidate calculations needed before replica launch."""
    yaml_content = serve_state.get_yaml_content(service_name, version)
    if yaml_content is None:
        raise ValueError(
            f'YAML content not found for {service_name} version {version}.')
    task = load_task_with_service_spec(yaml_content, service_spec)
    if task.service is None:
        raise ValueError(
            f'Service spec not found for {service_name} version {version}.')
    serve_utils.resolve_replica_ingress_port(task, pool=task.service.pool)

    uses_logical_replicas = service_spec.uses_logical_replicas is True
    default_planned_capacity = _uniform_whole_gpu_capacity(task.resources)
    if uses_logical_replicas:
        _exact_accelerator_shapes(task.resources)
    candidate_placer = None
    if service_spec.placement_contract.enabled:
        candidate_placer = _load_spot_placer(service_name, version,
                                             service_spec, task, workspace)
    if uses_logical_replicas:
        _validate_logical_capacity_sources(default_planned_capacity,
                                           candidate_placer, task.num_nodes)
    return candidate_placer


def _should_use_spot(
    yaml_content: str,
    resource_override: dict[str, Any] | None,
    service_spec: 'service_spec.SkyServiceSpec | None' = None,
    task_template: task_lib.Task | None = None,
) -> bool:
    """Get whether the task should use spot."""
    if resource_override is not None:
        use_spot_override = resource_override.get('use_spot')
        if use_spot_override is not None:
            assert isinstance(use_spot_override, bool)
            return use_spot_override
    task = (task_template if task_template is not None else
            load_task_with_service_spec(yaml_content, service_spec))
    spot_use_resources = [
        resources for resources in task.resources if resources.use_spot
    ]
    # Heterogeneous any_of sets may mix spot cloud entries with
    # non-spot reserved-capacity entries (e.g. a Kubernetes pool, which
    # cannot use spot). The task counts as spot-managed when ANY entry
    # is spot; the placer then pins each launch's actual use_spot via
    # its location's resources_override.
    return len(spot_use_resources) > 0


def _placer_has_only_non_spot_kubernetes_gpu_locations(
        placer: spot_placer.SpotPlacer | None) -> bool:
    """Whether every placer location is a budgetable non-spot K8s GPU."""
    if placer is None:
        return False
    location_statuses = getattr(placer, 'location2status', None)
    if not isinstance(location_statuses, dict) or not location_statuses:
        return False
    return all(
        not location.use_spot and str(location.cloud).lower() == 'kubernetes'
        and _whole_gpu_capacity(location.accelerators) is not None
        for location in location_statuses)


def _whole_gpu_capacity(
        accelerators: Mapping[str, int | float] | None) -> int | None:
    """Return the v1 logical slot width for one exact GPU shape."""
    if accelerators is None or len(accelerators) != 1:
        return None
    count = next(iter(accelerators.values()))
    if (isinstance(count, bool) or not isinstance(count, (int, float)) or
            count < 1 or not float(count).is_integer()):
        return None
    return int(count)


def _kubernetes_context_has_configured_autoscaler(
        kubernetes_context: str) -> bool:
    """Whether a Kubernetes context is configured to scale from zero."""
    autoscaler = skypilot_config.get_effective_region_config(
        cloud='kubernetes',
        region=kubernetes_context,
        keys=('autoscaler',),
        default_value=None)
    return isinstance(autoscaler, str) and bool(autoscaler.strip())


def _is_protocol_v2_fill_override(
        resources_override: Mapping[str, Any] | None) -> bool:
    """Classify a carried v2 fill override without provider access."""
    if resources_override is None:
        return False
    protocol = resources_override.get(
        serve_constants.RESERVED_FILL_PROTOCOL_VERSION_OVERRIDE_KEY)
    return (serve_constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY
            in resources_override and type(protocol) is int and
            protocol == reserved_capacity_broker.PROTOCOL_V2)


def _is_lowercase_sha256(value: Any) -> bool:
    return (type(value) is str and len(value) == 64 and
            all(character in '0123456789abcdef' for character in value))


def _protocol_v2_fill_cloud_launch_guard(
    pool_key: str,
    service_generation: int,
    physical_cluster_uid: str,
    kube_context: str,
    accelerator: str,
    accelerator_count: int,
    resources_override: Mapping[str, Any],
) -> tuple[bool, str]:
    """Validate one protocol-v2 queued pin without provider access.

    The durable request tuple is the asynchronous launch's authority. Every
    executor attempt proves its physical UID under a fresh provider phase and
    physical fence; this early guard only rejects an in-memory queued tuple
    whose exact context or shape was mutated before request submission.
    """
    if (isinstance(service_generation, bool) or
            not isinstance(service_generation, int) or service_generation < 1):
        return False, 'invalid-fill-service-generation'
    if not isinstance(physical_cluster_uid, str) or not physical_cluster_uid:
        return False, 'invalid-fill-physical-cluster-uid'
    try:
        identity = reserved_capacity_broker.parse_pool_identity(pool_key)
    except (TypeError, ValueError):
        return False, 'invalid-fill-pool-key'
    if (identity.protocol_version != reserved_capacity_broker.PROTOCOL_V2 or
            identity.physical_cluster_uid != physical_cluster_uid):
        return False, 'fill-pool-identity-mismatch'
    if (not isinstance(kube_context, str) or not kube_context or
            not isinstance(accelerator, str) or not accelerator or
            accelerator.casefold() not in identity.gpu_names or
            isinstance(accelerator_count, bool) or
            not isinstance(accelerator_count, int) or accelerator_count < 1):
        return False, 'invalid-fill-expected-pin'
    queued_context = resources_override.get('region')
    queued_accelerators = resources_override.get('accelerators')
    if (str(resources_override.get('cloud')).lower() != 'kubernetes' or
            queued_context != kube_context):
        return False, 'invalid-fill-kubernetes-context'
    if (not isinstance(queued_accelerators, Mapping) or
            len(queued_accelerators) != 1):
        return False, 'invalid-fill-accelerator-shape'
    queued_accelerator, queued_count = next(iter(queued_accelerators.items()))
    if (not isinstance(queued_accelerator, str) or
            queued_accelerator.casefold() != accelerator.casefold() or
            queued_count != accelerator_count or
            _whole_gpu_capacity(queued_accelerators) is None):
        return False, 'fill-accelerator-shape-mismatch'
    return True, 'authorized'


def _interrupted_reserved_fill_protocol(info: 'ReplicaInfo') -> int:
    """Classify a durable interrupted fill row without guessing corruption."""
    try:
        cleanup_fence = reserved_capacity.parse_protocol_v2_cleanup_fence(info)
    except exceptions.KubernetesPhysicalClusterIdentityError as error:
        raise ValueError(
            'partial or contradictory protocol-v2 fill identity') from error
    return (reserved_capacity_broker.PROTOCOL_V1
            if cleanup_fence is None else reserved_capacity_broker.PROTOCOL_V2)


def _provider_cleanup_phase_order(info: 'ReplicaInfo') -> int:
    """Order provider cleanup without consulting mutable provider state.

    Protocol-v2 authority (including a malformed v2-shaped row that must fail
    closed) always precedes ambient/legacy work.  This prevents an ordinary
    provider call from reacquiring the ambient phase between v2 owners in a
    recovery or refresher wave.
    """
    try:
        cleanup_fence = reserved_capacity.parse_protocol_v2_cleanup_fence(info)
    except exceptions.KubernetesPhysicalClusterIdentityError:
        return 0
    return 0 if cleanup_fence is not None else 1


def _ordinary_cleanup_phase_authority(
    cluster_name: str,
    expected_cluster_record_uuid: str | None,
) -> tuple[provider_phase.ProviderPhaseMode | None, bytes | None]:
    """Return ambient admission only when cleanup can touch Kubernetes.

    The phase gate protects mutable Kubernetes context authority; AWS, GCP,
    and other providers do not consult it.  Bypass the gate only from an
    action-aware, UUID-predicated durable handle.  Missing, legacy, malformed,
    or Kubernetes handles remain conservative ambient work.
    """
    if expected_cluster_record_uuid is None:
        return provider_phase.ProviderPhaseMode.AMBIENT_LEGACY, None
    snapshot = global_user_state.get_cluster_record_identity_snapshot(
        cluster_name, expected_cluster_record_uuid)
    if snapshot is None:
        return provider_phase.ProviderPhaseMode.AMBIENT_LEGACY, None
    phase_mode = reserved_capacity.ordinary_provider_phase_mode(
        snapshot.handle, cluster_name)
    serialized_handle = (snapshot.serialized_handle
                         if phase_mode is None else None)
    return phase_mode, serialized_handle


def _zero_cost_pool_key(
        location: spot_placer.Location) -> tuple[str, str] | None:
    """Return the shared demand pool identity for one exact GPU shape."""
    if (str(location.cloud).lower() != 'kubernetes' or
            _whole_gpu_capacity(location.accelerators) is None or
            location.accelerators is None):
        return None
    gpu_name = next(iter(location.accelerators))
    return location.region, gpu_name.lower()


def _uniform_whole_gpu_capacity(
        resources: typing.Iterable[resources_lib.Resources]) -> int | None:
    """Return one shared width only when every resource is an exact GPU."""
    capacities = [
        _whole_gpu_capacity(resource.accelerators) for resource in resources
    ]
    if not capacities or any(capacity is None for capacity in capacities):
        return None
    unique_capacities = set(typing.cast(list[int], capacities))
    return (next(iter(unique_capacities))
            if len(unique_capacities) == 1 else None)


def _exact_accelerator_shapes(
        resources: typing.Iterable[resources_lib.Resources]) -> dict[str, int]:
    """Return the distinct exact-card catalog, or empty for legacy shapes."""
    shapes: dict[str, int] = {}
    canonical_by_name: dict[str, str] = {}
    saw_resource = False
    for resource in resources:
        saw_resource = True
        accelerators = resource.accelerators
        width = _whole_gpu_capacity(accelerators)
        if width is None or accelerators is None:
            return {}
        card = str(next(iter(accelerators)))
        folded = card.casefold()
        canonical = canonical_by_name.setdefault(folded, card)
        previous = shapes.get(canonical)
        if previous is not None and previous != width:
            return {}
        shapes[canonical] = width
    return shapes if saw_resource else {}


def _validate_logical_capacity_sources(default_capacity: int | None,
                                       placer: spot_placer.SpotPlacer | None,
                                       num_nodes: int) -> None:
    """Require every launch path to prove an integer logical width."""
    if num_nodes != 1:
        raise ValueError(
            'dynamic_fallback_per_gpu currently supports only single-node '
            'services. Multi-node replica routing does not yet define a safe '
            'logical capacity contract.')
    if placer is None and default_capacity is None:
        raise ValueError(
            'Logical replicas require every launch to have one exact '
            'whole-GPU width, or a shape-aware spot placer that pins the '
            'selected resources.')
    if placer is not None and any(
            _whole_gpu_capacity(location.accelerators) is None
            for location in placer.active_locations()):
        raise ValueError(
            'Logical replicas require every eligible spot-placement '
            'candidate to have one exact whole-GPU shape.')


# Every function that calls serve_state.add_or_update_replica should acquire
# this lock. It is to prevent race condition when the replica status is updated
# by multiple threads at the same time. The modification of replica info is
# 2 database calls: read the whole replica info object, unpickle it, and modify
# corresponding fields. Then it is write back to the database. We need to ensure
# the read-modify-write operation is atomic.
def with_lock(func):

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.lock:
            return func(self, *args, **kwargs)

    return wrapper


class ReplicaManager:
    """Each replica manager monitors one service."""

    # The controller consumes this through a ReplicaManager-typed reference.
    # Concrete managers must bind it before they can launch a replica.
    yaml_content: str

    def __new__(cls, *args: Any, **kwargs: Any) -> 'ReplicaManager':
        """Allocate a manager with its complete process-local interface.

        A few embedders and focused tests suppress the I/O-bearing ``__init__``
        while exercising lifecycle logic.  Keep that supported construction
        path explicit: allocation establishes every base runtime field, so
        ordinary methods never have to guess whether their own attributes
        exist.
        """
        del args, kwargs
        manager = super().__new__(cls)
        ReplicaManager._initialize_process_state(manager)
        return manager

    def _initialize_process_state(self) -> None:
        """Initialize base process-local state without external I/O."""
        self.lock = threading.Lock()
        self._next_replica_id: int = 1
        self._changed_only_readiness_persistence = (
            _changed_only_readiness_persistence_enabled())
        self._workspace = constants.SKYPILOT_DEFAULT_WORKSPACE
        self._resource_action_mode = 'legacy'
        self._resource_scope: str | None = None
        self._service_hash: str | None = None
        self._controller_owner: tuple[int | None, str | None] | None = None
        self._enforce_launch_fence = True
        self._uptime: float | None = None
        self._update_mode = serve_utils.DEFAULT_UPDATE_MODE
        self._is_pool = False
        self._spot_placer: spot_placer.SpotPlacer | None = None
        self._lb_in_flight_report: tuple[float, dict[str, int], set[str] | None,
                                         set[str], set[str],
                                         str | None] | None = None
        self._logical_state_lock = threading.RLock()
        self._logical_reconcile_state = _LogicalReconcileState(target=None,
                                                               snapshot=None)
        self._logical_controller_epoch = uuid.uuid4().hex
        self._unknown_capacity_replacement_ids: set[int] = set()
        self._superseded_prune_pending = True
        self._target_num_replicas_lock = threading.Lock()
        self._target_num_replicas: int | None = None
        self._target_num_replicas_generation = 0
        self._status_epoch_lock = threading.Lock()
        self._status_epoch_generation = 0
        self._update_recovery_required = False
        self._pending_version: int | None = None
        self._drain_proof_stats_value = drain_observability.DrainProofStats()
        self._last_probe_route_result: ProbeRouteResult | None = None
        self._route_projection_publisher: Callable[[ProbeRouteResult],
                                                   None] | None = None
        self._route_material_writer: Callable[
            [list[tuple[ReplicaInfo, route_projection.RouteLeaseMaterial]]],
            None] | None = None

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int,
                 resource_scope: str | None = None,
                 service_hash: str | None = None,
                 controller_pid: int | None = None,
                 controller_ip: str | None = None,
                 enforce_launch_fence: bool = True) -> None:
        self._service_name: str = service_name
        service_record = serve_state.get_service_from_name(service_name)
        if service_record is not None:
            self._workspace = serve_utils.resolve_service_workspace(
                service_name, service_record,
                skypilot_config.get_active_workspace())
            resource_action_mode = service_record.get('resource_action_mode',
                                                      'legacy')
        else:
            self._workspace = (skypilot_config.get_active_workspace() or
                               constants.SKYPILOT_DEFAULT_WORKSPACE)
            resource_action_mode = 'legacy'
        if resource_action_mode not in ('legacy', 'shadow', 'authoritative'):
            raise RuntimeError(
                f'Service {service_name!r} has an invalid resource-action '
                'mode.')
        self._resource_action_mode = resource_action_mode
        self._resource_scope = resource_scope
        self._service_hash = service_hash
        self._controller_owner = ((controller_pid,
                                   controller_ip) if service_hash is not None or
                                  controller_pid is not None or
                                  controller_ip is not None else None)
        self._enforce_launch_fence = enforce_launch_fence
        self._is_pool = spec.pool
        # Freshest (received_at, {url: in_flight}, routing_urls,
        # unknown_urls, draining_urls, lb_session_id) report from the LB,
        # published raw (url-keyed) by the controller's
        # load_balancer_sync handler. All sets are sampled by the LB
        # atomically with the gauge; _ReplicaDrainTracker combines them
        # to prove 'not routed and nothing in flight' for a retiring
        # replica's url. Written by whole-tuple replace and read without
        # a lock (atomic in CPython); None until the first report (old
        # LB / pool: never).
        # Degraded replacements are protected from recursively replacing one
        # another only until they produce a real capacity sample. The durable
        # marker and this recovered index survive controller restarts; the
        # thread-pool refresher clears both after authoritative recovery.
        # Published by the autoscaler tick after it consumes a report. The
        # target remains authoritative while newer LB capacity reports arrive;
        # only a capacity report older than the target publication blocks
        # logical actuation.
        header_keys = None
        if spec.readiness_headers is not None:
            header_keys = list(spec.readiness_headers.keys())
        logger.info(f'Readiness probe path: {spec.readiness_path}\n'
                    f'Initial delay seconds: {spec.initial_delay_seconds}\n'
                    'Endpoint probe interval seconds: '
                    f'{spec.endpoint_probe_interval_seconds}\n'
                    f'Post data: {spec.post_data}\n'
                    f'Readiness header keys: {header_keys}')

        # Newest version among the currently provisioned and launched replicas
        self.latest_version: int = version

    def _get_target_num_replicas_lock(self) -> threading.Lock:
        return self._target_num_replicas_lock

    def _get_status_epoch_lock(self) -> threading.Lock:
        return self._status_epoch_lock

    def publish_target_num_replicas(self, target_num_replicas: int | None,
                                    expected_version: int) -> bool:
        """Publish autoscaler intent for target-aware status aggregation."""
        if (target_num_replicas is not None and
            (type(target_num_replicas) is not int or  # pylint: disable=unidiomatic-typecheck
             target_num_replicas < 0)):
            raise ValueError(
                'target_num_replicas must be a nonnegative integer '
                'or None.')
        with self._get_target_num_replicas_lock():
            if (self._update_recovery_required or
                    expected_version != self.latest_version):
                return False
            if target_num_replicas != self._target_num_replicas:
                self._target_num_replicas_generation = self._target_num_replicas_generation + 1
            self._target_num_replicas = target_num_replicas
            return True

    def get_target_num_replicas(self) -> int | None:
        """Return the latest version-fenced authoritative autoscaler target."""
        with self._get_target_num_replicas_lock():
            return self._target_num_replicas

    def _transition_status_epoch_for_version(
            self, version: int, update_mode: serve_utils.UpdateMode) -> None:
        """Atomically advance status aggregation to a new applied version."""
        with self._get_status_epoch_lock():
            with self._get_target_num_replicas_lock():
                self._target_num_replicas = None
                self._target_num_replicas_generation = self._target_num_replicas_generation + 1
                self.latest_version = version
                self._update_mode = update_mode
                self._status_epoch_generation = self._status_epoch_generation + 1
                # Failed records retained under the version just superseded
                # are now stale; let the refresher collect them.
                self._superseded_prune_pending = True

    def update_lb_in_flight(self,
                            in_flight_by_url: dict[str, int] | None,
                            routing_urls: list[str] | None = None,
                            unknown_urls: list[str] | None = None,
                            draining_urls: list[str] | None = None,
                            lb_session_id: str | None = None) -> None:
        """Publish the LB's url-keyed in-flight gauge from a sync.

        A None gauge means the LB sent none (old LB version, or a policy
        that cannot track in-flight) -- keep the previous report so its
        staleness, not a blind overwrite, decides the drain fallback.
        `routing_urls` is the LB's routing view sampled atomically with
        the gauge (None: old LB without the field); `unknown_urls` are
        occupancy-capable urls whose async work is unknown this round;
        `draining_urls` are urls whose pruned clients are still open.
        """
        if in_flight_by_url is None:
            return
        # time.monotonic() throughout the drain machinery: deadlines and
        # freshness must be immune to wall-clock jumps (an NTP step forward
        # would otherwise expire a drain instantly and kill in-flight work).
        self._lb_in_flight_report = (time.monotonic(), in_flight_by_url,
                                     set(routing_urls)
                                     if routing_urls is not None else None,
                                     set(unknown_urls or
                                         ()), set(draining_urls or
                                                  ()), lb_session_id)

    @property
    def _logical_target(self) -> LogicalTargetState | None:
        """Compatibility view of the atomically published logical state."""
        return self._logical_reconcile_state.target

    @_logical_target.setter
    def _logical_target(self, target: LogicalTargetState | None) -> None:
        # Focused manager tests and recovery fixtures historically construct
        # the two inputs independently. Each assignment still publishes one
        # immutable pair; production readers capture the pair exactly once.
        state = self._logical_reconcile_state
        self._logical_reconcile_state = _LogicalReconcileState(
            target=target,
            snapshot=state.snapshot,
            retirement_floor=target,
            retirement_shelter=None)

    @property
    def _logical_reconcile_snapshot(self) -> LogicalReconcileSnapshot | None:
        """Compatibility view of the atomically published logical state."""
        return self._logical_reconcile_state.snapshot

    @_logical_reconcile_snapshot.setter
    def _logical_reconcile_snapshot(
            self, snapshot: LogicalReconcileSnapshot | None) -> None:
        state = self._logical_reconcile_state
        self._logical_reconcile_state = _LogicalReconcileState(
            target=state.target,
            snapshot=snapshot,
            retirement_floor=state.retirement_floor,
            retirement_shelter=state.retirement_shelter)

    def update_logical_reconcile_snapshot(
        self,
        version: int,
        generation: int,
        observed_slots_by_replica_id: dict[int, int],
        in_flight_by_replica_id: dict[int, int],
        unknown_replica_ids: set[int],
    ) -> None:
        """Publish one advancing legacy logical-capacity observation."""
        with self._logical_state_lock:
            state = self._logical_reconcile_state
            current_snapshot = state.snapshot
            if (current_snapshot is not None and
                    generation <= current_snapshot.generation):
                # The legacy LB source increments its controller-local
                # generation for every accepted report. A duplicate or older
                # standalone half-publication is therefore impossible for a
                # valid producer and could pair replayed capacity with a
                # same-generation target computed from different inputs.
                logger.warning(
                    'Discarding non-advancing legacy logical capacity '
                    f'snapshot generation {generation}; current generation is '
                    f'{current_snapshot.generation}.')
                return
            snapshot = LogicalReconcileSnapshot(
                version=version,
                generation=generation,
                observed_slots_by_replica_id=dict(observed_slots_by_replica_id),
                in_flight_by_replica_id=dict(in_flight_by_replica_id),
                unknown_replica_ids=frozenset(unknown_replica_ids),
                received_at=time.monotonic())
            self._logical_reconcile_state = _LogicalReconcileState(
                target=state.target,
                snapshot=snapshot,
                retirement_floor=state.retirement_floor,
                retirement_shelter=state.retirement_shelter)

    def publish_logical_target(
        self,
        version: int,
        generation: int,
        target_capacity: int,
        target_capacity_by_accelerator: LogicalAcceleratorState = (),
        accelerator_shapes: LogicalAcceleratorState = (),
        *,
        retirement_floor: LogicalTargetState | None = None,
        retirement_shelter: reserved_fill_planner.SequencedRetirementShelter |
        None = None,
    ) -> None:
        """Publish one non-regressing legacy logical target."""
        with self._logical_state_lock:
            if self._update_recovery_required:
                return
            state = self._logical_reconcile_state
            candidate: LogicalTargetState
            if target_capacity_by_accelerator or accelerator_shapes:
                candidate = (version, generation, target_capacity,
                             target_capacity_by_accelerator, accelerator_shapes)
            else:
                candidate = (version, generation, target_capacity)
            if _logical_target_state_components(candidate) is None:
                logger.warning('Discarding malformed published logical target '
                               f'{candidate!r}.')
                self._logical_reconcile_state = _LogicalReconcileState(
                    target=None,
                    snapshot=state.snapshot,
                    retirement_floor=None,
                    retirement_shelter=None)
                return
            if retirement_floor is None and retirement_shelter is None:
                retirement_floor = candidate
            if retirement_floor is not None:
                floor_components = _logical_target_state_components(
                    retirement_floor)
                if (floor_components is None or
                        floor_components[:2] != (version, generation)):
                    logger.warning('Discarding malformed logical retirement '
                                   f'floor {retirement_floor!r}.')
                    return
            if (retirement_shelter is not None and
                    retirement_shelter.service_version != version):
                logger.warning('Discarding mismatched logical retirement '
                               'shelter service version.')
                return
            current_components = _logical_target_state_components(state.target)
            if (current_components is not None and
                    generation < current_components[1]):
                logger.warning(
                    'Discarding regressed legacy logical target generation '
                    f'{generation}; current generation is '
                    f'{current_components[1]}.')
                return
            self._logical_reconcile_state = _LogicalReconcileState(
                target=candidate,
                snapshot=state.snapshot,
                retirement_floor=retirement_floor,
                retirement_shelter=retirement_shelter)

    def publish_logical_reconcile_state(
        self,
        target: LogicalTargetState,
        snapshot: LogicalReconcileSnapshot,
        retirement_floor: LogicalTargetState | None = None,
        retirement_shelter: reserved_fill_planner.SequencedRetirementShelter |
        None = None
    ) -> bool:
        """Atomically publish one coherent durable target/capacity pair."""
        with self._logical_state_lock:
            if self._update_recovery_required:
                return False
            components = _logical_target_state_components(target)
            if components is None:
                logger.warning('Discarding malformed logical reconcile target '
                               f'{target!r}.')
                return False
            target_version, target_generation, _, _, _ = components
            if retirement_floor is None and retirement_shelter is None:
                retirement_floor = target
            if retirement_floor is not None:
                floor_components = _logical_target_state_components(
                    retirement_floor)
                if (floor_components is None or floor_components[:2]
                        != (target_version, target_generation)):
                    logger.warning('Discarding incoherent logical retirement '
                                   f'floor {retirement_floor!r}.')
                    return False
            if (retirement_shelter is not None and
                    retirement_shelter.service_version != target_version):
                logger.warning('Discarding mismatched logical retirement '
                               'shelter service version.')
                return False
            if (snapshot.version, snapshot.generation) != (target_version,
                                                           target_generation):
                logger.warning(
                    'Discarding incoherent logical reconcile state: target '
                    f'version/generation={(target_version, target_generation)!r}, '
                    'snapshot version/generation='
                    f'{(snapshot.version, snapshot.generation)!r}.')
                return False
            published_snapshot = LogicalReconcileSnapshot(
                version=snapshot.version,
                generation=snapshot.generation,
                observed_slots_by_replica_id=dict(
                    snapshot.observed_slots_by_replica_id),
                in_flight_by_replica_id=dict(snapshot.in_flight_by_replica_id),
                unknown_replica_ids=frozenset(snapshot.unknown_replica_ids),
                received_at=snapshot.received_at,
                authority=snapshot.authority)
            if not self._logical_snapshot_has_scale_up_authority(
                    published_snapshot):
                logger.warning(
                    'Discarding expired logical reconcile authority for '
                    f'generation {snapshot.generation}.')
                return False
            # One reference assignment is this paired publication's boundary.
            # Readers capture the immutable pair once, so a same-generation
            # replay or generation regression cannot expose halves from two
            # different paired publications.
            self._logical_reconcile_state = _LogicalReconcileState(
                target=target,
                snapshot=published_snapshot,
                retirement_floor=retirement_floor,
                retirement_shelter=retirement_shelter)
            return True

    def invalidate_logical_target(self) -> None:
        """Revoke authority for pending logical-capacity retirements."""
        with self._logical_state_lock:
            if self._update_recovery_required:
                return
            state = self._logical_reconcile_state
            self._logical_reconcile_state = _LogicalReconcileState(
                target=None,
                snapshot=state.snapshot,
                retirement_floor=None,
                retirement_shelter=None)

    def invalidate_logical_reconcile_state(self) -> None:
        """Revoke both halves of durable logical reconcile authority."""
        with self._logical_state_lock:
            self._logical_reconcile_state = _LogicalReconcileState(
                target=None, snapshot=None)

    def _logical_target_fence_holds(
            self,
            version: int,
            decision_generation: int,
            target_capacity: int,
            target_capacity_by_accelerator: LogicalAcceleratorState |
        None = None,
            accelerator_shapes: LogicalAcceleratorState | None = None,
            require_exact_generation: bool = False,
            require_fresh_occupancy: bool = True,
            logical_state: _LogicalReconcileState | None = None) -> bool:
        """Whether a logical target intent is still authorized.

        Capacity reports may advance while the autoscaler waits for the
        replica-manager lock on a large fleet. A newer snapshot is stronger
        capacity evidence, not a superseding demand decision. The separately
        published target remains stamped with its producer generation and is
        the authority that invalidates the intent when the autoscaler takes a
        newer decision tick.
        """
        if logical_state is None:
            logical_state = self._logical_reconcile_state
        snapshot = logical_state.snapshot
        target_state = _logical_target_state_components(logical_state.target)
        if target_state is None:
            return False
        (target_version, target_generation, current_target,
         current_target_by_card, current_shapes) = target_state
        expected_target_by_card = target_capacity_by_accelerator or ()
        expected_shapes = accelerator_shapes or ()
        if ((current_target_by_card or current_shapes) and
            (target_capacity_by_accelerator is None or
             accelerator_shapes is None)):
            # An aggregate-only caller cannot act on an exact-card target.
            return False
        pending_version = self._pending_version
        generation_matches = (snapshot is not None and
                              (snapshot.generation == decision_generation
                               if require_exact_generation else
                               snapshot.generation >= decision_generation))
        snapshot_is_fresh = bool(
            snapshot is not None and
            (self._logical_snapshot_is_fresh(snapshot)
             if require_fresh_occupancy else
             self._logical_snapshot_has_scale_up_authority(snapshot)))
        return (not self._update_recovery_required and snapshot is not None and
                snapshot.version == version and generation_matches and
                snapshot_is_fresh and self.latest_version == version and
                (pending_version is None or pending_version <= version) and
                (target_version, target_generation, current_target)
                == (version, decision_generation, target_capacity) and
                current_target_by_card == expected_target_by_card and
                current_shapes == expected_shapes)

    def _logical_reconcile_fence_holds(
        self,
        fence: LogicalTargetState,
        *,
        require_exact_generation: bool = False,
        require_fresh_occupancy: bool = True,
        logical_state: _LogicalReconcileState | None = None,
    ) -> bool:
        components = _logical_target_state_components(fence)
        if components is None:
            return False
        version, generation, target, target_by_card, shapes = components
        exact = len(fence) == 5
        return self._logical_target_fence_holds(
            version,
            generation,
            target,
            target_by_card if exact else None,
            shapes if exact else None,
            require_exact_generation=require_exact_generation,
            require_fresh_occupancy=require_fresh_occupancy,
            logical_state=logical_state)

    @staticmethod
    def _logical_snapshot_is_fresh(snapshot: LogicalReconcileSnapshot) -> bool:
        if snapshot.authority is not None:
            return time.monotonic() < snapshot.authority.deadline_monotonic
        return (time.monotonic() - snapshot.received_at
                <= 3 * serve_constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)

    @staticmethod
    def _logical_snapshot_has_scale_up_authority(
            snapshot: LogicalReconcileSnapshot) -> bool:
        """Return whether additive work retains fresh demand/route authority.

        Occupancy freshness remains mandatory for every destructive consumer
        through ``_logical_snapshot_is_fresh``. A selected backend occupancy
        sample expiring must not revoke an independently fresh exact-card
        scale-up decision. Older in-process authorities without the split
        conservatively retain the stricter deadline.
        """
        if snapshot.authority is not None:
            deadline = getattr(snapshot.authority,
                               'scale_up_deadline_monotonic',
                               snapshot.authority.deadline_monotonic)
            return time.monotonic() < deadline
        return (time.monotonic() - snapshot.received_at
                <= 3 * serve_constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)

    @property
    def workspace(self) -> str:
        """Durable workspace used for replica placement and launches."""
        return self._workspace

    @property
    def spot_placer(self) -> Optional['SpotPlacerType']:
        """The placer, if this manager kind carries one (else None).

        Public accessor for the controller's fill machinery, which needs
        the placer's zero-cost location set; only
        SkyPilotReplicaManager actually builds one.
        """
        return self._spot_placer

    def scale_up(self,
                 resources_override: dict[str, Any] | None = None) -> None:
        """Scale up the service by 1 replica with resources_override.
        resources_override is of the same format with resources section
        in skypilot task yaml
        """
        raise NotImplementedError

    def scale_up_batch(
        self,
        resources_overrides: list[dict[str, Any] | None],
        expected_version: int | None = None,
        launch_priority: int = (serve_constants.LB_REQUEST_PRIORITY_MIN),
        paid_launch_authority: capacity_admission.PaidLaunchAuthority |
        None = None,
        paid_launch_allowed: bool = True,
    ) -> list[_ReplicaLaunchResult]:
        """Scale up by len(resources_overrides) replicas in one batch.

        Subclasses may override to amortize per-call synchronization; the
        default just loops over `scale_up`.
        """
        del launch_priority, paid_launch_authority, paid_launch_allowed
        if (self._update_recovery_required or
            (expected_version is not None and
             expected_version != self.latest_version)):
            return []
        accepted: list[_ReplicaLaunchResult] = []
        for resources_override in resources_overrides:
            self.scale_up(resources_override)
        return accepted

    def accept_reserved_fill(
        self, plan: reserved_fill_planner.FillPlan
    ) -> reserved_fill_planner.FillCommitResult:
        """Durably admit a typed protocol-v2 reserved-fill plan."""
        del plan
        raise NotImplementedError

    def pending_reserved_fill_snapshot(
        self,
        allocation: reserved_fill_planner.AuthenticatedAllocationMap,
        capacity_unit: reserved_fill_planner.FillCapacityUnit,
    ) -> zero_cost_actuation.PendingFillSnapshot:
        """Return service-wide headroom plus exact-map pending debits."""
        del allocation, capacity_unit
        raise NotImplementedError

    def install_durable_zero_cost_actuation(self) -> None:
        """Install a committed durable reserved-fill authority."""
        raise NotImplementedError

    def scale_up_to_logical_capacity(
        self,
        target_capacity: int,
        version: int,
        reconcile_generation: int,
        replace_unknown_replica_ids: tuple[int, ...] = (),
        target_capacity_by_accelerator: dict[str, int] | None = None,
        accelerator_shapes: dict[str, int] | None = None,
        launch_budget: int | None = None,
        launch_priority: int = serve_constants.LB_REQUEST_PRIORITY_MIN,
        launch_priority_by_accelerator: dict[str, int] | None = None,
        cold_launch_authority_by_accelerator: dict[str, int] | None = None,
        paid_launch_authority: capacity_admission.PaidLaunchAuthority |
        None = None,
        paid_launch_allowed: bool = True,
    ) -> list[_ReplicaLaunchResult]:
        """Persist complete backend shapes until target capacity is covered."""
        raise NotImplementedError

    def confirm_logical_bridge_capacities(
            self, verified_capacities: dict[int, int]) -> dict[int, int]:
        """Confirm LB-proven widths when this manager owns logical bridges.

        Managers without logical bridge persistence have no widths to adopt.
        The no-op default keeps the controller on the public manager contract;
        SkyPilotReplicaManager overrides it with the durable implementation.
        """
        del verified_capacities
        return {}

    def notify_version_pending(self, version: int) -> None:
        """Notify long manager operations that a newer version is waiting."""

    def clear_pending_version(self, version: int) -> None:
        """Clear a previously announced pending version."""

    def fence_launches_for_update_recovery(self) -> None:
        """Irreversibly stop autoscaler actuation until manager rebuild."""
        self._update_recovery_required = True

    def clear_scale_reconciliation_signal(self) -> None:
        """Prepare an ordinary autoscaler tick to consume prior feedback."""

    def wait_for_scale_reconciliation(self, timeout_seconds: float) -> bool:
        """Wait for feedback or the ordinary autoscaler interval."""
        time.sleep(timeout_seconds)
        return False

    def set_scale_reconcile_notifier(
            self, notify_reconcile: Callable[[], int]) -> None:
        """Install an optional controller wakeup sink.

        Non-SkyPilot managers have no typed provider feedback to publish.
        """
        del notify_reconcile

    def scale_down(self,
                   replica_id: int,
                   purge: bool = False,
                   wait_for_idle: bool = False,
                   expected_version: int | None = None) -> None:
        """Scale down replica with replica_id."""
        raise NotImplementedError

    def reconcile_fresh_zero_paid_retirements(
        self,
        authority: paid_retirement.FreshZeroAuthority,
        replica_infos: list['ReplicaInfo'],
    ) -> bool:
        """Off-route paid replicas under exact fresh-zero authority."""
        del authority, replica_infos
        return False

    def cancel_uncommitted_paid_retirements(
        self,
        service_hash: str,
        positive_demand_generation: int,
    ) -> bool:
        """Readmit paid replicas fenced by newer positive demand."""
        del service_hash, positive_demand_generation
        return False

    def scale_down_logically(
        self,
        replica_id: int,
        target_capacity: int,
        version: int,
        reconcile_generation: int,
        target_capacity_by_accelerator: LogicalAcceleratorState = (),
        accelerator_shapes: LogicalAcceleratorState = ()
    ) -> None:
        """Retire one backend only if the logical coverage fence still holds."""
        raise NotImplementedError

    def scale_down_logically_batch(
        self,
        replica_ids: list[int],
        target_capacity: int,
        version: int,
        reconcile_generation: int,
        target_capacity_by_accelerator: LogicalAcceleratorState = (),
        accelerator_shapes: LogicalAcceleratorState = ()
    ) -> None:
        """Retire logical backends selected from one reconcile generation.

        Subclasses may override to amortize synchronization and fleet reads.
        The compatibility path preserves the singleton behavior.
        """
        for replica_id in replica_ids:
            self.scale_down_logically(replica_id, target_capacity, version,
                                      reconcile_generation,
                                      target_capacity_by_accelerator,
                                      accelerator_shapes)

    def update_version(
        self,
        version: int,
        spec: 'service_spec.SkyServiceSpec',
        update_mode: serve_utils.UpdateMode,
        new_spot_placer: 'SpotPlacerType | None' = None,
        install_config: Callable[[], None] | None = None,
    ) -> None:
        raise NotImplementedError

    def get_active_replica_urls(self) -> list[str]:
        """Get the urls of the active replicas."""
        raise NotImplementedError

    def set_route_projection_publisher(
            self, publisher: Callable[[ProbeRouteResult], None] | None) -> None:
        """Install the post-readiness publisher for complete probe rounds."""
        self._route_projection_publisher = publisher

    def set_route_material_writer(
        self,
        writer: Callable[
            [list[tuple['ReplicaInfo',
                        route_projection.RouteLeaseMaterial]]], None] | None,
    ) -> None:
        """Install the provider-result writer with no publication authority."""
        self._route_material_writer = writer

    def system_recovery_allows_routing(self, info: 'ReplicaInfo') -> bool:
        """Whether recovery state permits a READY row to route."""
        del info
        return True

    def system_recovery_route_marker(
            self, info: 'ReplicaInfo',
            route_url: str) -> 'system_recovery_route_lease.RouteMarker | None':
        """Return a closed capable-route marker, or no usable marker."""
        del info, route_url
        return None

    def system_recovery_route_lease_snapshot(self) -> dict[str, Any]:
        """Return the closed v1 heartbeat payload for external LBs."""
        return {
            'version':
                serve_constants.SYSTEM_RECOVERY_ROUTE_LEASE_PROTOCOL_VERSION,
            'entries': [],
        }

    def retire_system_recovery_route(self, info: 'ReplicaInfo') -> None:
        """Permanently retire one exact row after route ambiguity."""
        del info

    @property
    def _drain_proof_stats(self) -> 'drain_observability.DrainProofStats':
        """Process-local drain/retirement counters."""
        return self._drain_proof_stats_value

    def drain_proof_stats_snapshot(self) -> dict[str, Any]:
        """Drain and retirement counters for /autoscaler/info."""
        return self._drain_proof_stats.snapshot()


@dataclasses.dataclass
class _ReplicaMutationRuntime:
    """Process-local owner for current replica mutation workers and queues.

    PostgreSQL owns durable intent and request associations. This object owns
    only the current controller process's worker handles, completion signals,
    compatibility patch points, and retry clocks. Keeping that ephemeral state
    behind one owner prevents independent aliases from drifting.
    """

    launch_completion_queue: queue.SimpleQueue[
        _ReplicaLaunchThread] = dataclasses.field(
            default_factory=queue.SimpleQueue)
    launch_completion_event: threading.Event = dataclasses.field(
        default_factory=threading.Event)
    launch_thread_pool: thread_utils.ThreadSafeDict[
        int, thread_utils.SafeThread] = dataclasses.field(
            default_factory=thread_utils.ThreadSafeDict)
    replica_to_request_id: thread_utils.ThreadSafeDict[
        int,
        str] = dataclasses.field(default_factory=thread_utils.ThreadSafeDict)
    replica_to_logical_launch_fence: thread_utils.ThreadSafeDict[
        int, LogicalTargetState] = dataclasses.field(
            default_factory=thread_utils.ThreadSafeDict)
    down_thread_pool: thread_utils.ThreadSafeDict[
        int, thread_utils.SafeThread] = dataclasses.field(
            default_factory=thread_utils.ThreadSafeDict)
    never_started_launch_reservations: dict[int,
                                            tuple[str,
                                                  int]] = (dataclasses.field(
                                                      default_factory=dict))
    failed_cleanup_retry_attempts: dict[int, int] = dataclasses.field(
        default_factory=dict)
    failed_cleanup_retry_at: dict[int, float] = dataclasses.field(
        default_factory=dict)

    def recover(self, recover: Callable[[], None]) -> None:
        """Run status-inference recovery through the process-local owner."""
        recover()

    def refresh(self, refresh: Callable[[], None]) -> None:
        """Run thread completion through the process-local owner."""
        refresh()

    def clear_failed_cleanup_retry(self, replica_id: int) -> None:
        """Forget process-local cleanup rate limiting after success."""
        self.failed_cleanup_retry_attempts.pop(replica_id, None)
        self.failed_cleanup_retry_at.pop(replica_id, None)

    def schedule_failed_cleanup_retry(self, replica_id: int,
                                      now: float) -> tuple[int, float]:
        """Record one process-local retry and return attempt and delay."""
        attempt = self.failed_cleanup_retry_attempts.get(replica_id, 0) + 1
        self.failed_cleanup_retry_attempts[replica_id] = attempt
        exponential_step = min(attempt - 1, 30)
        delay_seconds = min(
            _FAILED_CLEANUP_RETRY_BASE_SECONDS * 2**exponential_step,
            _FAILED_CLEANUP_RETRY_MAX_SECONDS)
        self.failed_cleanup_retry_at[replica_id] = now + delay_seconds
        return attempt, delay_seconds


class SkyPilotReplicaManager(ReplicaManager):
    """Replica Manager for SkyPilot clusters.

    It will run three daemon to monitor the status of the replicas:
        (1) _thread_pool_refresher: Refresh the launch/down thread pool
            to monitor the progress of the launch/down thread.
        (2) _job_status_fetcher: Fetch the job status of the service to
            monitor the status of the service jobs.
        (3) _replica_prober: Do readiness probe to the replicas to monitor
            whether it is still responding to requests.
    """

    _scale_reconciliation_event: threading.Event
    _LEGACY_MUTATION_RUNTIME_INIT_LOCK = threading.Lock()

    _LEGACY_MUTATION_FIELD_MAP = {
        '_launch_completion_queue': 'launch_completion_queue',
        '_launch_completion_event': 'launch_completion_event',
        '_launch_thread_pool': 'launch_thread_pool',
        '_replica_to_request_id': 'replica_to_request_id',
        '_replica_to_logical_launch_fence': 'replica_to_logical_launch_fence',
        '_down_thread_pool': 'down_thread_pool',
        '_failed_cleanup_retry_attempts': 'failed_cleanup_retry_attempts',
        '_failed_cleanup_retry_at': 'failed_cleanup_retry_at',
    }

    def __new__(cls, *args: Any, **kwargs: Any) -> 'SkyPilotReplicaManager':
        manager = typing.cast('SkyPilotReplicaManager',
                              super().__new__(cls, *args, **kwargs))
        manager._initialize_skypilot_process_state()
        return manager

    def _initialize_skypilot_process_state(self) -> None:
        """Initialize SkyPilot-specific process state without external I/O."""
        self._version_specs: dict[int, service_spec.SkyServiceSpec] = {}
        # Service versions are immutable. Parse each version once, then clone
        # its Task for launch-specific resource/env mutation.
        self._version_task_templates: dict[int, task_lib.Task] = {}
        self._uses_logical_replicas = False
        self._default_planned_capacity: int | None = None
        self._logical_exact_accelerator_shapes: dict[str, int] = {}
        self._spot_placement_state_restored = False
        self._fill_skip_last_log_time = 0.0
        # Process-local adapters and caches for the current Serve mutation
        # implementation.  The retired resource-action authority proposal no
        # longer makes this runtime a deprecated removal target.  Its logical
        # launch fence is assigned when a queued thread is admitted and
        # rechecked immediately before sdk.launch().
        self._publish_legacy_mutation_runtime_state(_ReplicaMutationRuntime())
        # Ownership loss and update-recovery are distinct terminal signals:
        # the parent can retain its durable owner while replacing this child.
        self._ownership_lost = threading.Event()
        self._manager_daemon_stop = threading.Event()
        self._scale_reconciliation_event = threading.Event()
        self._readiness_executor_lock = threading.Lock()
        self._readiness_executor: concurrent.futures.ThreadPoolExecutor | None = (
            None)
        # The controller installs its single generation coordinator after the
        # manager is constructed.  Keep the legacy event as a compatibility
        # signal for direct embedders, but route every committed feedback wake
        # to that coordinator when it is present.
        self._scale_reconcile_notifier: Callable[[], int] | None = None
        self._system_recovery_route_epoch = str(uuid.uuid4())
        self._ordinary_launch_handoff_route_epoch = str(uuid.uuid4())
        self._system_recovery_route_registry = (
            system_recovery_route_lease.ManagerRouteLeaseRegistry())
        # Durable wall-clock anchors are restored separately. These monotonic
        # guards intentionally start fresh after controller replacement.
        self._candidate_release_monotonic_deadlines: dict[int, float] = {}
        self._system_recovery_status_initialized: set[int] = set()
        self._wait_for_idle_trackers: dict[int, _WaitForIdleState] = {}
        self._legacy_uncertain_logical_retirement_ids: set[int] = set()
        self._ambiguous_logical_retirement_commit_ids: set[int] = set()
        self._recovering_logical_retirement_ids: set[int] = set()
        self._logical_retirement_recovery_deadline: float | None = None
        self._logical_retirement_reactivation_generation: int | None = None
        self._tick_version_spec_cache: dict[int,
                                            service_spec.SkyServiceSpec] = {}
        self._provider_identity_uncertain_ids: set[int] = set()
        self._non_pool_reconciliation_threads: thread_utils.ThreadSafeDict[
            int, thread_utils.SafeThread] = thread_utils.ThreadSafeDict()
        self._non_pool_reconciliation_attempts: dict[int, int] = {}
        self._non_pool_reconciliation_retry_at: dict[int, float] = {}
        self._ambiguous_paid_phase_a_lock = threading.Lock()
        self._ambiguous_paid_phase_a_recoveries: dict[
            _AmbiguousPaidPhaseAIdentity, _AmbiguousPaidPhaseARecovery] = {}
        self._ordinary_launch_binding_authority: (ControllerBindingAuthority |
                                                  None) = None
        self._ordinary_launch_binding_transition_lock = threading.Lock()
        self._ordinary_launch_binding_transition_in_progress = (
            threading.Event())
        # Real controllers replace this from PostgreSQL during __init__.  The
        # direct default preserves lightweight manager test doubles that do
        # not own a central database.
        self._reserved_fill_actuation_mode: (
            zero_cost_actuation.ActuationMode |
            None) = zero_cost_actuation.ActuationMode.DIRECT_REPLICA
        self._zero_cost_actuation_repository = (
            zero_cost_actuation.ZeroCostActuationRepository())
        self._zero_cost_actuation_executor_id = uuid.uuid4()
        self._zero_cost_actuation_event = threading.Event()
        self._zero_cost_actuation_lane_lock = threading.Lock()
        self._zero_cost_actuation_lanes: dict[str, threading.Thread] = {}

    def _publish_legacy_mutation_runtime_state(
            self, runtime: _ReplicaMutationRuntime) -> None:
        """Publish one runtime and synchronized compatibility aliases."""
        # Data-descriptor properties below remain the only read owner. Keeping
        # identity-matched instance entries makes unittest.mock treat legacy
        # instance patch points as local, so context teardown restores the
        # captured value through the setter without retaining old worker pools.
        self.__dict__.update({
            '_launch_completion_queue': runtime.launch_completion_queue,
            '_launch_completion_event': runtime.launch_completion_event,
            '_launch_thread_pool': runtime.launch_thread_pool,
            '_replica_to_request_id': runtime.replica_to_request_id,
            '_replica_to_logical_launch_fence':
                runtime.replica_to_logical_launch_fence,
            '_down_thread_pool': runtime.down_thread_pool,
            '_failed_cleanup_retry_attempts':
                runtime.failed_cleanup_retry_attempts,
            '_failed_cleanup_retry_at': runtime.failed_cleanup_retry_at,
        })
        # Publish last. A caller that observes the runtime also observes every
        # compatibility alias from the same critical section.
        self.__dict__['_legacy_mutation_runtime'] = runtime

    def _set_legacy_mutation_compat_field(self, legacy_name: str,
                                          runtime_name: str,
                                          value: Any) -> None:
        """Keep a temporary instance patch point identical to its owner."""
        runtime = self._legacy_mutation_runtime_state()
        setattr(runtime, runtime_name, value)
        self.__dict__[legacy_name] = value

    def _reset_legacy_mutation_compat_field(
            self, legacy_name: str, runtime_name: str,
            default_factory: Callable[[], Any]) -> None:
        """Recreate a deleted compatibility field with its historical type."""
        self._set_legacy_mutation_compat_field(legacy_name, runtime_name,
                                               default_factory())

    def _legacy_mutation_runtime_state(self) -> _ReplicaMutationRuntime:
        """Return the current owner, adopting legacy instance patch points."""
        runtime = self.__dict__.get('_legacy_mutation_runtime')
        if runtime is not None:
            if '_launch_completion_queue' not in self.__dict__:
                # Lightweight tests and embedders may inject only the runtime.
                # Repair their patch metadata once under the publication lock.
                with self._LEGACY_MUTATION_RUNTIME_INIT_LOCK:
                    runtime = self.__dict__['_legacy_mutation_runtime']
                    self._publish_legacy_mutation_runtime_state(runtime)
            return runtime
        # Compatibility managers reconstructed without the current __init__
        # can first touch this accessor from multiple daemon threads. Adopt and
        # publish their old fields exactly once so a losing initializer cannot
        # overwrite live queues, events, or workers with fresh defaults.
        with self._LEGACY_MUTATION_RUNTIME_INIT_LOCK:
            runtime = self.__dict__.get('_legacy_mutation_runtime')
            if runtime is not None:
                return runtime
            runtime = _ReplicaMutationRuntime()
            for legacy_name, runtime_name in (
                    self._LEGACY_MUTATION_FIELD_MAP.items()):
                legacy_value = self.__dict__.get(legacy_name)
                if legacy_value is not None:
                    setattr(runtime, runtime_name, legacy_value)
            self._publish_legacy_mutation_runtime_state(runtime)
            return runtime

    @property
    def _launch_completion_queue(
            self) -> queue.SimpleQueue[_ReplicaLaunchThread]:
        return self._legacy_mutation_runtime_state().launch_completion_queue

    @_launch_completion_queue.setter
    def _launch_completion_queue(
            self, value: queue.SimpleQueue[_ReplicaLaunchThread]) -> None:
        self._set_legacy_mutation_compat_field('_launch_completion_queue',
                                               'launch_completion_queue', value)

    @_launch_completion_queue.deleter
    def _launch_completion_queue(self) -> None:
        self._reset_legacy_mutation_compat_field('_launch_completion_queue',
                                                 'launch_completion_queue',
                                                 queue.SimpleQueue)

    @property
    def _launch_completion_event(self) -> threading.Event:
        return self._legacy_mutation_runtime_state().launch_completion_event

    @_launch_completion_event.setter
    def _launch_completion_event(self, value: threading.Event) -> None:
        self._set_legacy_mutation_compat_field('_launch_completion_event',
                                               'launch_completion_event', value)

    @_launch_completion_event.deleter
    def _launch_completion_event(self) -> None:
        self._reset_legacy_mutation_compat_field('_launch_completion_event',
                                                 'launch_completion_event',
                                                 threading.Event)

    @property
    def _launch_thread_pool(
            self) -> thread_utils.ThreadSafeDict[int, thread_utils.SafeThread]:
        return self._legacy_mutation_runtime_state().launch_thread_pool

    @_launch_thread_pool.setter
    def _launch_thread_pool(
        self,
        value: thread_utils.ThreadSafeDict[int,
                                           thread_utils.SafeThread]) -> None:
        self._set_legacy_mutation_compat_field('_launch_thread_pool',
                                               'launch_thread_pool', value)

    @_launch_thread_pool.deleter
    def _launch_thread_pool(self) -> None:
        self._reset_legacy_mutation_compat_field('_launch_thread_pool',
                                                 'launch_thread_pool',
                                                 thread_utils.ThreadSafeDict)

    @property
    def _replica_to_request_id(self) -> thread_utils.ThreadSafeDict[int, str]:
        return self._legacy_mutation_runtime_state().replica_to_request_id

    @_replica_to_request_id.setter
    def _replica_to_request_id(
            self, value: thread_utils.ThreadSafeDict[int, str]) -> None:
        self._set_legacy_mutation_compat_field('_replica_to_request_id',
                                               'replica_to_request_id', value)

    @_replica_to_request_id.deleter
    def _replica_to_request_id(self) -> None:
        self._reset_legacy_mutation_compat_field('_replica_to_request_id',
                                                 'replica_to_request_id',
                                                 thread_utils.ThreadSafeDict)

    @property
    def _replica_to_logical_launch_fence(
            self) -> thread_utils.ThreadSafeDict[int, LogicalTargetState]:
        return (self._legacy_mutation_runtime_state().
                replica_to_logical_launch_fence)

    @_replica_to_logical_launch_fence.setter
    def _replica_to_logical_launch_fence(
            self,
            value: thread_utils.ThreadSafeDict[int,
                                               LogicalTargetState]) -> None:
        self._set_legacy_mutation_compat_field(
            '_replica_to_logical_launch_fence',
            'replica_to_logical_launch_fence', value)

    @_replica_to_logical_launch_fence.deleter
    def _replica_to_logical_launch_fence(self) -> None:
        self._reset_legacy_mutation_compat_field(
            '_replica_to_logical_launch_fence',
            'replica_to_logical_launch_fence', thread_utils.ThreadSafeDict)

    @property
    def _down_thread_pool(
            self) -> thread_utils.ThreadSafeDict[int, thread_utils.SafeThread]:
        return self._legacy_mutation_runtime_state().down_thread_pool

    @_down_thread_pool.setter
    def _down_thread_pool(
        self,
        value: thread_utils.ThreadSafeDict[int,
                                           thread_utils.SafeThread]) -> None:
        self._set_legacy_mutation_compat_field('_down_thread_pool',
                                               'down_thread_pool', value)

    @_down_thread_pool.deleter
    def _down_thread_pool(self) -> None:
        self._reset_legacy_mutation_compat_field('_down_thread_pool',
                                                 'down_thread_pool',
                                                 thread_utils.ThreadSafeDict)

    @property
    def _failed_cleanup_retry_attempts(self) -> dict[int, int]:
        return (
            self._legacy_mutation_runtime_state().failed_cleanup_retry_attempts)

    @_failed_cleanup_retry_attempts.setter
    def _failed_cleanup_retry_attempts(self, value: dict[int, int]) -> None:
        self._set_legacy_mutation_compat_field('_failed_cleanup_retry_attempts',
                                               'failed_cleanup_retry_attempts',
                                               value)

    @_failed_cleanup_retry_attempts.deleter
    def _failed_cleanup_retry_attempts(self) -> None:
        self._reset_legacy_mutation_compat_field(
            '_failed_cleanup_retry_attempts', 'failed_cleanup_retry_attempts',
            dict)

    @property
    def _failed_cleanup_retry_at(self) -> dict[int, float]:
        return self._legacy_mutation_runtime_state().failed_cleanup_retry_at

    @_failed_cleanup_retry_at.setter
    def _failed_cleanup_retry_at(self, value: dict[int, float]) -> None:
        self._set_legacy_mutation_compat_field('_failed_cleanup_retry_at',
                                               'failed_cleanup_retry_at', value)

    @_failed_cleanup_retry_at.deleter
    def _failed_cleanup_retry_at(self) -> None:
        self._reset_legacy_mutation_compat_field('_failed_cleanup_retry_at',
                                                 'failed_cleanup_retry_at',
                                                 dict)

    _candidate_release_monotonic_deadlines: dict[int, float]
    _system_recovery_status_initialized: set[int]

    def _restore_spot_placement_state(self) -> None:
        """Restore durable exact-location benches once per manager process."""
        if self._spot_placement_state_restored:
            return
        placer = self._spot_placer
        if placer is not None:
            states = serve_state.get_service_placement_policy_states(
                self._service_name)
            placer.load_retry_state(None if states is
                                    None else states['spot_placement_state'])
        self._spot_placement_state_restored = True

    def _persist_spot_placement_state_if_dirty(self) -> None:
        """Fence and persist placer evidence before dependent replica rows."""
        placer = self._spot_placer
        if placer is None or not placer.retry_state_dirty:
            return
        service_hash = self._service_hash
        if service_hash is None:
            placer.mark_retry_state_persisted()
            return
        persisted = serve_state.set_service_spot_placement_state(
            self._service_name, service_hash, self._controller_owner,
            placer.dump_retry_state())
        if not persisted:
            raise RuntimeError(
                f'Service {self._service_name!r} controller ownership changed '
                'while persisting placement retry state.')
        placer.mark_retry_state_persisted()

    def _release_unstarted_location_retry(
            self, location: 'spot_placer.Location | None') -> None:
        """Return a consumed bench probe when no provider launch will start."""
        placer = self._spot_placer
        if placer is None or location is None:
            return
        placer.release_retry(location)
        self._persist_spot_placement_state_if_dirty()

    def _launch_completion_state(
        self,
    ) -> tuple['queue.SimpleQueue[_ReplicaLaunchThread]', threading.Event]:
        """Return lazily compatible completion state for launch workers."""
        runtime = self._legacy_mutation_runtime_state()
        return (runtime.launch_completion_queue,
                runtime.launch_completion_event)

    def _join_notified_launch_workers(self) -> None:
        """Join completion callbacks before the reducer checks is_alive()."""
        completion_queue, _ = self._launch_completion_state()
        while True:
            try:
                worker = completion_queue.get_nowait()
            except queue.Empty:
                return
            if worker is not threading.current_thread():
                worker.join()

    def clear_scale_reconciliation_signal(self) -> None:
        """Clear feedback before a tick that will read durable state."""
        self._scale_reconciliation_event.clear()

    def wait_for_scale_reconciliation(self, timeout_seconds: float) -> bool:
        """Wait interruptibly for committed typed provider feedback."""
        return self._scale_reconciliation_event.wait(timeout_seconds)

    def set_scale_reconcile_notifier(
            self, notify_reconcile: Callable[[], int]) -> None:
        """Bind committed manager feedback to the controller coordinator."""
        if not callable(notify_reconcile):
            raise TypeError('notify_reconcile must be callable.')
        self._scale_reconcile_notifier = notify_reconcile

    def _notify_scale_reconciliation(self) -> None:
        """Wake compatibility consumers and the canonical controller loop."""
        self._scale_reconciliation_event.set()
        notifier = getattr(self, '_scale_reconcile_notifier', None)
        if notifier is not None:
            notifier()

    def _db_fence_kwargs(self) -> dict[str, Any]:
        """Exact owner predicates, omitted for legacy/direct test managers."""
        kwargs: dict[str, Any] = {}
        service_hash = self._service_hash
        if service_hash is not None:
            kwargs['expected_service_hash'] = service_hash
        controller_owner = self._controller_owner
        if controller_owner is not None:
            kwargs['expected_controller_owner'] = controller_owner
        return kwargs

    def _resource_action_fence_kwargs(self) -> dict[str, Any] | None:
        """Snapshot the current fence for a later locked action admission.

        A lifecycle epoch fences one API lifecycle operation; it is not a
        stable controller credential and legitimately advances during updates.
        The action store revalidates this optimistic snapshot under the
        service-row lock, so a concurrent advance safely rejects admission.
        """
        service_hash = self._service_hash
        controller_owner = self._controller_owner
        if service_hash is None or controller_owner is None:
            return None
        try:
            owner = serve_state.get_service_controller_owner(self._service_name)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to snapshot the current resource-action '
                           f'fence: {common_utils.format_exception(e)}')
            return None
        if owner is None:
            return None
        service_status = owner.get('status')
        if (owner.get('hash') != service_hash or
            (owner.get('controller_pid'),
             owner.get('controller_ip')) != controller_owner or
                not isinstance(service_status, serve_state.ServiceStatus) or
                service_status in
                serve_state.ServiceStatus.replica_launch_blocking_statuses()):
            return None
        lifecycle_epoch = owner.get('lifecycle_epoch')
        if (type(lifecycle_epoch) is not int or lifecycle_epoch <= 0):
            return None
        return {
            'expected_controller_owner': controller_owner,
            'expected_lifecycle_epoch': lifecycle_epoch,
        }

    def _service_launch_authorization(self) -> bool | None:
        """Return True/False for proven authority/loss, None if unverifiable."""
        if self._update_recovery_required:
            # A controller-config transition may have published a new policy
            # before the remaining manager/autoscaler epoch was installed.
            # Once that happens only a fresh controller reconstruction can
            # prove a coherent launch generation.  This process-local fence is
            # deliberately independent of service ownership: the parent keeps
            # the same durable owner tuple while it respawns this child.
            return False
        service_hash = self._service_hash
        if service_hash is None:
            # Compatibility for direct/legacy managers without durable owner
            # identity. New controllers always supply the full tuple.
            return True
        ownership_lost = self._ownership_lost
        if ownership_lost.is_set():
            return False
        controller_owner = self._controller_owner
        try:
            owner = serve_state.get_service_controller_owner(self._service_name)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to verify controller ownership before '
                           f'launching a replica: '
                           f'{common_utils.format_exception(e)}')
            # A transient DB error is not evidence that another controller
            # owns the row. Fail this attempt closed, but do not trip the
            # permanent loss event: queued replicas and the owner watchdog
            # must retry once the DB is reachable again.
            return None
        authorized = (
            owner is not None and owner.get('hash') == service_hash and
            (owner.get('controller_pid'), owner.get('controller_ip'))
            == controller_owner and owner.get('status')
            not in serve_state.ServiceStatus.replica_launch_blocking_statuses())
        if not authorized:
            ownership_lost.set()
            self._manager_daemon_stop.set()
            self._launch_completion_event.set()
        return authorized

    def _service_is_launch_authorized(self) -> bool:
        """Fail one launch closed unless ownership is currently proven."""
        return self._service_launch_authorization() is True

    def _launch_owner_watchdog_allows_continue(self) -> bool:
        """Cheap shared fence polled by every in-flight launch request."""
        return (not self._update_recovery_required and
                not self._ownership_lost.is_set())

    def _service_is_cleanup_authorized(self) -> bool:
        """Fail cleanup closed unless this exact controller still owns it.

        Cleanup deliberately ignores launch-blocking lifecycle statuses:
        SHUTTING_DOWN is the normal state while a down worker is running. The
        immutable service hash and controller PID/IP are the authority, so a
        stale daemon worker cannot retry after controller replacement.
        """
        service_hash = self._service_hash
        if service_hash is None:
            # Compatibility for direct/legacy managers without durable owner
            # identity. New controllers always supply the full tuple.
            return True
        controller_owner = self._controller_owner
        if controller_owner is None:
            return False
        try:
            owner = serve_state.get_service_controller_owner(self._service_name)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning('Failed to verify controller ownership before '
                           'cleaning up a replica: '
                           f'{common_utils.format_exception(e)}')
            return False
        return (owner is not None and owner.get('hash') == service_hash and
                (owner.get('controller_pid'), owner.get('controller_ip'))
                == controller_owner)

    def _replica_launch_fence_context(self,
                                      service_version: int | None = None
                                     ) -> dict[str, Any] | None:
        """Owner tuple validated by the API executor before provisioning."""
        if not self._enforce_launch_fence:
            # A legacy/non-consolidated controller owns a different Serve DB;
            # the API server cannot validate that tuple against its local DB.
            return None
        service_hash = self._service_hash
        controller_owner = self._controller_owner
        if service_hash is None or controller_owner is None:
            return None
        controller_pid, controller_ip = controller_owner
        service_name_key = (
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY)
        service_hash_key = (
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY)
        controller_pid_key = (
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY)
        controller_ip_key = (
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY)
        fence_context = {
            service_name_key: self._service_name,
            service_hash_key: service_hash,
            controller_pid_key: controller_pid,
            controller_ip_key: controller_ip,
        }
        if service_version is not None:
            fence_context[
                serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY] = (
                    service_version)
        return fence_context

    def _is_ordinary_launch_binding_profile(
            self, info: ReplicaInfo,
            recovery_launch_kwargs: Mapping[str, Any]) -> bool:
        """Whether one worker belongs to the narrow ordinary profile."""
        if self._is_pool or recovery_launch_kwargs:
            return False
        # These paths retain their existing identity, retry, and cleanup
        # contracts.  In particular, a promoted service must never silently
        # reinterpret a system-OOM or physical-capacity operation as ordinary.
        return ordinary_launch_binding.replica_has_narrow_ordinary_profile(info)

    def _bound_ordinary_launch_is_eligible(
            self, info: ReplicaInfo,
            recovery_launch_kwargs: Mapping[str, Any]) -> bool:
        """Select only the explicitly promoted ordinary launch profile."""
        authority = self._ordinary_launch_binding_authority
        return bool(
            self._is_ordinary_launch_binding_profile(
                info, recovery_launch_kwargs) and authority is not None and
            authority.capable is True and
            authority.binding_mode == ordinary_launch_binding.BindingMode.BOUND)

    def _ordinary_binding_profile_launch_is_authorized(self) -> bool:
        """Close eligible process admission during a binding transition."""
        return bool(
            not self._ordinary_launch_binding_transition_in_progress.is_set()
            and self._service_is_launch_authorized())

    def _bound_ordinary_launch_fence_context(
        self,
        info: ReplicaInfo,
        service_version: int,
        base_launch_fence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build the complete immutable admission fence for one replica."""
        authority = self._ordinary_launch_binding_authority
        if (authority is None or authority.capable is not True or
                authority.binding_mode
                != ordinary_launch_binding.BindingMode.BOUND):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Bound ordinary launch has no promoted controller authority.')
        fence = (self._replica_launch_fence_context(service_version)
                 if base_launch_fence is None else dict(base_launch_fence))
        if fence is None:
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Bound ordinary launch has no durable service-owner fence.')
        fence = dict(fence)
        fence.update({
            ordinary_launch_binding.REPLICA_ID_KEY: info.replica_id,
            ordinary_launch_binding.REPLICA_RECORD_ID_KEY:
                info.replica_record_id,
            ordinary_launch_binding.LIFECYCLE_EPOCH_KEY:
                authority.service_lifecycle_epoch,
            ordinary_launch_binding.BINDING_EPOCH_KEY: authority.binding_epoch,
            ordinary_launch_binding.CONTROLLER_INCARNATION_KEY: str(
                authority.controller_incarnation),
            ordinary_launch_binding.CONTROLLER_OWNER_EPOCH_KEY:
                authority.controller_owner_epoch,
        })
        # Parse locally before publishing the request. This keeps a partially
        # assembled context from becoming a durable admission ambiguity.
        ordinary_launch_binding.parse_unbound_launch_context(fence)
        return fence

    @staticmethod
    def _binding_excluded_launch_fence_context(
        info: ReplicaInfo,
        launch_fence: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Bind a legacy special-profile request to its persisted replica."""
        if launch_fence is None:
            return None
        excluded = dict(launch_fence)
        excluded.update({
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
                (serve_constants.
                 ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE),
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY:
                info.replica_id,
            serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
                info.replica_record_id,
        })
        # Keep manager emission and API/executor parsing on one closed shape.
        serve_state.normalize_binding_excluded_launch_context(excluded)
        return excluded

    @staticmethod
    def _bound_launch_capacity_reason(launch_cloud: clouds.Cloud | None,
                                      error: Any) -> str | None:
        error = non_pool_launch_reconciliation.decoded_request_error(error)
        if (launch_cloud is None or
                not isinstance(error, exceptions.ResourcesUnavailableError)):
            return None
        return cloud_vm_ray_backend.classify_resources_unavailable_error(
            launch_cloud, error)

    def _project_bound_ordinary_launch(self, launch_cloud: clouds.Cloud | None,
                                       connection: Any,
                                       projection: Any) -> bool:
        """Project exact request evidence and paid feedback atomically."""
        info = projection.locked_replica_info
        request_error = projection.request.error
        reason = self._bound_launch_capacity_reason(launch_cloud, request_error)
        status = projection.status.value
        provider_absent = (getattr(
            projection, 'provider_evidence',
            None) == ordinary_launch_binding.ProviderEvidence.ABSENT)
        provider_present_cleanup = (getattr(
            projection, 'provider_evidence',
            None) == ordinary_launch_binding.ProviderEvidence.PRESENT)
        provider_absence_projection = None
        if provider_absent:
            provider_absence_projection = (
                non_pool_launch_reconciliation.
                apply_exact_provider_absence_replica_projection(projection))
            if provider_absence_projection is None:
                return False
        if provider_present_cleanup:
            cleanup_context = projection.context
            if (not isinstance(
                    cleanup_context,
                    ordinary_launch_binding.BoundNonPoolLaunchContext) or
                    projection.pre_effect_terminal or
                    projection.service_job_id is not None or
                    info.service_job_id is not None or
                    info.zero_cost_materialization_sequence is not None):
                return False
            if (cleanup_context.profile.kind == ordinary_launch_binding.
                    NonPoolLaunchProfileKind.RESERVED_FILL):
                shape_matches = bool(
                    projection.paid_capacity_pool_key is None and
                    info.paid_capacity_pool_key is None and
                    info.is_zero_cost is True)
            elif ordinary_launch_binding.is_paid_provider_reconciliation_profile(
                    cleanup_context.profile.kind):
                pool_key = projection.paid_capacity_pool_key
                pool_identity = (paid_capacity.pool_key_payload(pool_key)
                                 if isinstance(pool_key, str) else None)
                shape_matches = bool(
                    isinstance(pool_identity, Mapping) and
                    pool_identity.get('cloud') == 'gcp' and
                    pool_identity.get('use_spot') is True and
                    info.paid_capacity_pool_key == pool_key and
                    info.is_spot is True and info.is_zero_cost is False and
                    info.reserved_fill is False)
            else:
                shape_matches = False
            if not shape_matches:
                return False
            status_property = info.status_property
            status_property.sky_launch_status = (
                common_utils.ProcessStatus.INTERRUPTED)
            if (status_property.sky_down_status
                    != common_utils.ProcessStatus.RUNNING):
                status_property.sky_down_status = (
                    common_utils.ProcessStatus.SCHEDULED)
            status_property.service_ready_now = False
            status_property.is_scale_down = True
            status_property.preempted = False
            status_property.purged = False
            status_property.failed_spot_availability = False
            status_property.drain_cap_seconds = 0
            status_property.drain_started_at = None
            status_property.wait_for_idle_before_termination = False
            status_property.logical_retirement_version = None
            status_property.logical_retirement_controller_epoch = None
            status_property.logical_retirement_generation = None
            status_property.logical_retirement_target_capacity = None
            status_property.logical_retirement_confirmed_generation = None
            status_property.logical_retirement_bounded_deadline = False
            status_property.logical_retirement_committed = False
        paid_outcome: paid_capacity.LaunchOutcome | None
        if projection.pre_effect_terminal:
            # No provider or service-job effect occurred.  Leave this exact
            # replica record pending so the same demand can publish a fresh
            # association generation.  An already-durable teardown remains
            # absorbing; its exact cancellation will release the claim and
            # the down worker owns the next action.
            if getattr(projection, 'cancel_reason', None) is not None:
                info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.INTERRUPTED)
            elif (info.status_property.sky_launch_status
                  != common_utils.ProcessStatus.INTERRUPTED):
                info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.SCHEDULED)
            # OTHER_FAILURE deliberately makes no paid-pool feedback change.
            # The reducer, which can see the association's cancel reason,
            # decides whether the exact claim is retained for the successor.
            paid_outcome = paid_capacity.LaunchOutcome.OTHER_FAILURE
        elif provider_absent:
            # The request result describes the handler; this independent exact
            # provider read describes usable capacity.  Once the immutable
            # physical resource is absent, never publish the replica as ready
            # or stamp a zero-cost materialization even if the handler had
            # reported success before disappearing.
            assert provider_absence_projection is not None
            paid_outcome = provider_absence_projection.paid_capacity_outcome
        elif provider_present_cleanup:
            # PRESENT proves the exact physical allocation exists, not that
            # launch succeeded.  Preserve the association and request pin;
            # the existing UID-fenced down worker must obtain fresh ABSENT
            # evidence before either can be settled.
            # OTHER_FAILURE is the paid-pool reducer's neutral outcome: it
            # persists this cleanup marker without resizing the pool or
            # releasing the still-live exact claim.
            paid_outcome = paid_capacity.LaunchOutcome.OTHER_FAILURE
        elif status == 'SUCCEEDED':
            # Teardown writes INTERRUPTED before exact cancellation.  A request
            # may race that cancel and finish successfully, but its result must
            # not erase the only durable cleanup intent before the down worker
            # records SCHEDULED.
            if (info.status_property.sky_launch_status
                    != common_utils.ProcessStatus.INTERRUPTED):
                info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.SUCCEEDED)
            paid_outcome = paid_capacity.LaunchOutcome.SUCCESS
        else:
            # A teardown persists INTERRUPTED before exact cancellation. Do
            # not let the reducer turn that durable cleanup intent back into a
            # generic failed launch. Post-effect terminal outcomes remain
            # failures; they must never look successful merely because
            # projection itself committed.
            if (info.status_property.sky_launch_status
                    != common_utils.ProcessStatus.INTERRUPTED):
                info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.FAILED)
            if reason == 'quota':
                paid_outcome = paid_capacity.LaunchOutcome.QUOTA_FAILURE
                info.status_property.failed_spot_availability = True
            elif reason == 'capacity':
                paid_outcome = paid_capacity.LaunchOutcome.CAPACITY_FAILURE
                info.status_property.failed_spot_availability = True
            else:
                paid_outcome = paid_capacity.LaunchOutcome.OTHER_FAILURE
        binding_context = projection.context
        if (isinstance(binding_context,
                       ordinary_launch_binding.BoundNonPoolLaunchContext) and
                binding_context.profile.kind == ordinary_launch_binding.
                NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY):
            intent = info.system_recovery_launch_intent
            if (intent is None or info.system_recovery_quarantine is not None or
                    info.system_recovery_disposition
                    != system_recovery_state.SystemRecoveryDisposition.CANDIDATE
                    or binding_context.profile.authorization_reference
                    != f'system-oom:{intent.launch_nonce}' or
                    binding_context.profile.authorization_generation
                    != intent.launch_generation):
                return False
            if not projection.pre_effect_terminal:
                job_id = projection.service_job_id
                if (isinstance(job_id, bool) or not isinstance(job_id, int) or
                        job_id < 1):
                    return False
                existing = (info.launch_request_id, info.service_job_id)
                expected = (binding_context.request_id, job_id)
                if existing == (None, None):
                    revision = info.system_recovery_revision
                    if (isinstance(revision, bool) or
                            not isinstance(revision, int) or revision < 1):
                        return False
                    info.launch_request_id = binding_context.request_id
                    info.service_job_id = job_id
                    info.system_recovery_revision = revision + 1
                elif existing != expected:
                    return False
        if projection.paid_capacity_pool_key is None:
            paid_outcome = None
        paid_capacity_pool_key = (
            provider_absence_projection.paid_capacity_pool_key
            if provider_absence_projection is not None else
            projection.paid_capacity_pool_key)
        authority = self._ordinary_launch_binding_authority
        if authority is None:
            return False
        return serve_state.update_replica_for_bound_ordinary_launch_in_transaction(
            connection,
            self._service_name,
            authority.service_hash,
            info.replica_id,
            info.replica_record_id,
            projection.context.association_id,
            info,
            provider_launch_succeeded=(not projection.pre_effect_terminal and
                                       not provider_absent and
                                       not provider_present_cleanup and
                                       status == 'SUCCEEDED'),
            paid_capacity_pool_key=paid_capacity_pool_key,
            paid_capacity_outcome=paid_outcome)

    def _bound_ordinary_launch_callbacks(
        self,
        info: ReplicaInfo,
        launch_cloud: clouds.Cloud | None,
        *,
        initial_context: 'BoundLaunchContext | None' = None,
    ) -> tuple[Callable[[], Any], Callable[[Any, BaseException | None], Any],
               Callable[[str], Any]]:
        """Close exact inspect/reduce/cancel calls over one record identity."""
        authority = self._ordinary_launch_binding_authority
        if authority is None:
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Bound ordinary launch has no controller authority.')
        context_box: list[Any] = []
        if initial_context is not None:
            context_box.append(initial_context)

        def _inspect() -> Any:
            reduction = request_postgres.inspect_bound_ordinary_launch(
                self._service_name, info.replica_id, info.replica_record_id)
            if reduction is not None:
                if context_box and context_box[0] != reduction.context:
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Replica pointer resolved to a different bound '
                        'ordinary-launch generation.')
                if not context_box:
                    context_box.append(reduction.context)
            return reduction

        def _context() -> Any:
            if not context_box:
                reduction = _inspect()
                if reduction is None:
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Replica has no exact bound ordinary-launch pointer.')
            return context_box[0]

        projector = functools.partial(self._project_bound_ordinary_launch,
                                      launch_cloud)

        def _reduce(_result: Any, _error: BaseException | None) -> Any:
            del _result, _error
            # An active request has no projectable result. One conservative
            # non-authorizing SELECT keeps its periodic adopter poll off every
            # mutation lock. A stale result can only defer terminal reduction
            # until the next poll; every false/error shape enters the canonical
            # locked reducer below.
            launch_context = _context()
            try:
                active = (
                    request_postgres.read_bound_reserved_fill_active_snapshot(
                        launch_context, authority))
            except Exception:  # pylint: disable=broad-except
                active = None
            if active is not None:
                return active
            return request_postgres.reduce_bound_ordinary_launch(
                launch_context, authority, project_replica_result=projector)

        def _cancel(reason: str) -> Any:
            return request_postgres.cancel_bound_ordinary_launch_request(
                _context(), authority, reason, project_replica_result=projector)

        return _inspect, _reduce, _cancel

    def _register_bound_launch_adopter(
        self,
        info: ReplicaInfo,
        request_id: str,
        launch_thread: '_ReplicaLaunchThread',
        existing_replica_infos: list[ReplicaInfo] | None = None,
    ) -> None:
        """Publish one adopter locally, rolling back a partial publication."""
        runtime = self._legacy_mutation_runtime_state()
        replica_id = info.replica_id
        try:
            if existing_replica_infos is not None:
                existing_replica_infos.append(info)
            runtime.replica_to_request_id[replica_id] = request_id
            runtime.launch_thread_pool[replica_id] = launch_thread
        except BaseException as error:
            try:
                if runtime.launch_thread_pool.get(replica_id) is launch_thread:
                    runtime.launch_thread_pool.pop(replica_id)
                if runtime.replica_to_request_id.get(replica_id) == request_id:
                    runtime.replica_to_request_id.pop(replica_id)
                if existing_replica_infos is not None:
                    for index in range(len(existing_replica_infos) - 1, -1, -1):
                        if existing_replica_infos[index] is info:
                            del existing_replica_infos[index]
                            break
            except BaseException as cleanup_error:
                if not isinstance(error, Exception):
                    raise error from cleanup_error
                if not isinstance(cleanup_error, Exception):
                    raise
                raise error from cleanup_error
            raise

    def _build_bound_launch_adopter(
        self,
        info: ReplicaInfo,
        bound_context: 'BoundLaunchContext',
        *,
        yaml_content: str | None = None,
        spec: 'service_spec.SkyServiceSpec | None' = None,
    ) -> '_ReplicaLaunchThread':
        """Reconstruct the exact durable-request adopter for one replica."""
        if yaml_content is None:
            yaml_content = (
                self.yaml_content if info.version == self.latest_version else
                serve_state.get_yaml_content(self._service_name, info.version))
        if yaml_content is None:
            raise ValueError('yaml content not found for bound launch '
                             f'recovery of version {info.version}')
        if spec is None:
            spec = self._version_specs.get(info.version)
        if spec is None:
            spec = serve_state.get_spec(self._service_name, info.version)
        if spec is None:
            raise ValueError('service spec not found for bound launch '
                             f'recovery of version {info.version}')
        task_template = self._task_template_for_version(info.version,
                                                        yaml_content, spec)
        recovery_task = _build_replica_launch_task(
            yaml_content,
            info.replica_id,
            info.resources_override,
            exact_resources_override=info.get_spot_location() is not None,
            authoritative_service_spec=spec,
            service_name=self._service_name,
            task_template=task_template)
        recovery_cloud = next(iter(recovery_task.resources)).cloud
        _, reduce_bound, cancel_bound = self._bound_ordinary_launch_callbacks(
            info, recovery_cloud, initial_context=bound_context)
        log_file = serve_utils.generate_replica_launch_log_file_name(
            self._service_name, info.replica_id, self._resource_scope)
        completion_queue, completion_event = self._launch_completion_state()
        teardown_requested = threading.Event()
        launch_context = bound_context
        return _ReplicaLaunchThread(
            target=adopt_bound_ordinary_launch,
            replica_id=info.replica_id,
            replica_record_id=info.replica_record_id,
            service_hash=self._service_hash,
            controller_owner=self._controller_owner,
            teardown_requested=teardown_requested,
            completion_queue=completion_queue,
            completion_event=completion_event,
            bound_ordinary_launch=True,
            adopts_existing_bound_request=True,
            args=(info.replica_id, info.cluster_name, log_file,
                  launch_context.request_id, recovery_cloud, reduce_bound,
                  cancel_bound, teardown_requested),
            kwargs={
                'continue_guard': self._launch_owner_watchdog_allows_continue,
                'supersession_guard': functools.partial(
                    self._queued_launch_generation_decision, info.version),
                'durable_store_only': bool(
                    isinstance(
                        launch_context,
                        ordinary_launch_binding.BoundNonPoolLaunchContext) and
                    launch_context.profile.kind is ordinary_launch_binding.
                    NonPoolLaunchProfileKind.RESERVED_FILL),
            })

    def _install_bound_launch_adopter(
        self,
        info: ReplicaInfo,
        bound_context: 'BoundLaunchContext',
        *,
        start: bool,
        yaml_content: str | None = None,
        spec: 'service_spec.SkyServiceSpec | None' = None,
        existing_replica_infos: list[ReplicaInfo] | None = None,
    ) -> bool:
        """Install one exact adopter if no local worker already owns it."""
        runtime = self._legacy_mutation_runtime_state()
        if info.replica_id in runtime.launch_thread_pool:
            return False
        launch_thread = self._build_bound_launch_adopter(
            info, bound_context, yaml_content=yaml_content, spec=spec)
        request_id = bound_context.request_id
        self._register_bound_launch_adopter(info, request_id, launch_thread,
                                            existing_replica_infos)
        if start:
            reserved_here = False
            if (info.status_property.sky_launch_status ==
                    common_utils.ProcessStatus.SCHEDULED):
                reserved_info = (
                    serve_state.reserve_replica_launch_running_if_capacity(
                        self._service_name,
                        info.replica_id,
                        info.replica_record_id,
                        launch_limit=controller_utils.get_serve_launch_limit(
                            self._is_pool),
                        require_bound_association=True,
                        **self._db_fence_kwargs()))
                if reserved_info is None:
                    # Keep the exact adopter registered but queued.  The
                    # ordinary refresher batches it with the next available P
                    # slot; no executable observation starts before RUNNING.
                    return True
                info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.RUNNING)
                reserved_here = True
                runtime.never_started_launch_reservations[info.replica_id] = (
                    info.replica_record_id, id(launch_thread))
            try:
                launch_thread.start()
            except BaseException as error:
                # Thread.start() has no atomic start acknowledgement.  An
                # operator interrupt can arrive after the native thread starts
                # but before ``ident`` is published, so its durable adopter
                # registration must remain intact for reconciliation.
                if not isinstance(error, Exception):
                    # An asynchronous interruption cannot prove no native
                    # worker exists.  Revoke the process-local rollback proof
                    # so the next tick treats RUNNING as inherited/adoptable.
                    runtime.never_started_launch_reservations.pop(
                        info.replica_id, None)
                    raise
                if launch_thread.ident is not None:
                    runtime.never_started_launch_reservations.pop(
                        info.replica_id, None)
                if launch_thread.ident is None:
                    if (reserved_here and
                            not launch_thread.adopts_existing_bound_request):
                        try:
                            restored = (
                                self._restore_never_started_launch_to_scheduled(
                                    info.replica_id, info.replica_record_id))
                        except Exception as recovery_error:  # pylint: disable=broad-except
                            logger.warning(
                                'Failed to release never-started bound launch '
                                'reservation for replica %s: %s',
                                info.replica_id,
                                common_utils.format_exception(recovery_error))
                        else:
                            if restored is not None:
                                runtime.never_started_launch_reservations.pop(
                                    info.replica_id, None)
                    else:
                        # An adopter's immutable request may already own or
                        # have completed provider I/O.  Thread.start failure is
                        # not permission to demote that durable generation.
                        runtime.never_started_launch_reservations.pop(
                            info.replica_id, None)
                    try:
                        if (runtime.launch_thread_pool.get(info.replica_id)
                                is launch_thread):
                            runtime.launch_thread_pool.pop(info.replica_id)
                            runtime.replica_to_request_id.pop(info.replica_id)
                        if existing_replica_infos is not None:
                            for index in range(
                                    len(existing_replica_infos) - 1, -1, -1):
                                if existing_replica_infos[index] is info:
                                    del existing_replica_infos[index]
                                    break
                    except BaseException as cleanup_error:
                        if not isinstance(cleanup_error, Exception):
                            raise
                        raise error from cleanup_error
                raise
            runtime.never_started_launch_reservations.pop(info.replica_id, None)
        logger.info('Adopting exact bound ordinary launch %s for replica %s.',
                    request_id, info.replica_id)
        return True

    def _schedule_non_pool_provider_reconciliation(
        self,
        info: ReplicaInfo,
        binding_context: Any,
    ) -> None:
        """Schedule one bounded provider read without blocking this manager."""
        if not isinstance(binding_context,
                          ordinary_launch_binding.BoundNonPoolLaunchContext):
            return
        authority = self._ordinary_launch_binding_authority
        if (authority is None or
                not authority.retained_non_pool_settlement_allowed):
            return
        replica_id = info.replica_id
        existing = self._non_pool_reconciliation_threads.get(replica_id)
        if existing is not None:
            if existing.is_alive():
                return
            self._non_pool_reconciliation_threads.pop(replica_id)
            if existing.exception is None:
                self._non_pool_reconciliation_attempts.pop(replica_id, None)
                self._non_pool_reconciliation_retry_at[replica_id] = (
                    time.monotonic() +
                    _NON_POOL_RECONCILIATION_RETRY_BASE_SECONDS)
                return
            else:
                attempt = self._non_pool_reconciliation_attempts.get(
                    replica_id, 0) + 1
                self._non_pool_reconciliation_attempts[replica_id] = attempt
                delay = min(
                    _NON_POOL_RECONCILIATION_RETRY_BASE_SECONDS *
                    2**min(attempt - 1, 30),
                    _NON_POOL_RECONCILIATION_RETRY_MAX_SECONDS)
                self._non_pool_reconciliation_retry_at[
                    replica_id] = time.monotonic() + delay
                logger.warning(
                    'Provider reconciliation for replica %s failed; retrying '
                    'in %.1f seconds: %s', replica_id, delay,
                    existing.format_exc or repr(existing.exception))
                return
        if time.monotonic() < self._non_pool_reconciliation_retry_at.get(
                replica_id, 0):
            return
        active_workers = sum(
            worker.is_alive()
            for worker in self._non_pool_reconciliation_threads.values())
        if active_workers >= (
                _MAX_CONCURRENT_NON_POOL_RECONCILIATIONS_PER_SERVICE):
            return
        worker = thread_utils.SafeThread(
            target=non_pool_launch_reconciliation.reconcile,
            name=f'replica-{replica_id}-provider-reconciliation',
            daemon=True,
            args=(binding_context, info, authority,
                  functools.partial(self._project_bound_ordinary_launch, None)))
        self._non_pool_reconciliation_threads[replica_id] = worker
        worker.start()

    def _finalize_projected_provider_absence_cleanup(self,
                                                     replica_id: int) -> bool:
        """Remove an immediate-cleanup row after exact ABSENT projected."""
        runtime = self._legacy_mutation_runtime_state()
        if (replica_id in runtime.launch_thread_pool or
                replica_id in runtime.down_thread_pool):
            return False
        info = serve_state.get_replica_info_from_id(self._service_name,
                                                    replica_id)
        if (info is None or not ordinary_launch_binding.
                replica_has_projected_provider_absence_cleanup_marker(info)):
            return False
        # The selected transaction independently proves settled association
        # history, canonical ABSENT evidence, released pin, null pointer, and
        # no paid claim after any process restart. Ordinary paid rejection has
        # no provider object to tear down, so retire its exact row directly.
        if info.reserved_fill is True:
            if not (request_postgres.
                    bound_non_pool_projected_provider_absence_is_authorized(
                        self._service_name, replica_id,
                        info.replica_record_id)):
                return False
            self._handle_sky_down_finish(info, format_exc=None)
        else:
            if not (request_postgres.
                    retire_bound_non_pool_projected_paid_provider_absence(
                        self._service_name, replica_id,
                        info.replica_record_id)):
                return False
            logger.info(
                'Replica %s removed after exact paid provider '
                'negative acknowledgement.', info.replica_id)
        return True

    def _reconcile_unowned_bound_non_pool_launches(
            self, replica_infos: list[ReplicaInfo]) -> None:
        """Adopt active requests and reconcile effects from durable state."""
        authority = self._ordinary_launch_binding_authority
        if (authority is None or
                not authority.retained_non_pool_settlement_allowed):
            return
        runtime = self._legacy_mutation_runtime_state()
        for info in replica_infos:
            if (info.status not in (serve_state.ReplicaStatus.PENDING,
                                    serve_state.ReplicaStatus.PROVISIONING) or
                    info.replica_id in runtime.launch_thread_pool or
                    info.replica_id in runtime.down_thread_pool or
                    self._ambiguous_paid_phase_a_is_pending(info)):
                continue
            try:
                reduction = request_postgres.inspect_bound_ordinary_launch(
                    self._service_name, info.replica_id, info.replica_record_id)
                if (reduction is None and ordinary_launch_binding.
                        classify_non_pool_launch_profile(info) is not None):
                    retirement = (ordinary_launch_binding.
                                  retire_pre_admission_non_pool_launch_intent(
                                      authority, info.replica_id,
                                      info.replica_record_id))
                    if retirement.disposition in (
                            ordinary_launch_binding.
                            PreAdmissionRetirementDisposition.RETIRED,
                            ordinary_launch_binding.
                            PreAdmissionRetirementDisposition.ABSENT):
                        self._notify_scale_reconciliation()
                        continue
                    if retirement.disposition is (
                            ordinary_launch_binding.
                            PreAdmissionRetirementDisposition.ASSOCIATED):
                        # Admission won the service-row race after the stale
                        # snapshot.  Re-read its durable identity and adopt it
                        # instead of waiting for another controller tick.
                        reduction = (
                            request_postgres.inspect_bound_ordinary_launch(
                                self._service_name, info.replica_id,
                                info.replica_record_id))
                        if reduction is None:
                            raise RuntimeError(
                                'Associated launch lost its durable request '
                                'projection.')
                if (reduction is not None and
                        _bound_projection_classification(reduction)
                        in ('ADOPT_ACTIVE', 'WAIT_QUIESCENCE')):
                    self._install_bound_launch_adopter(info,
                                                       reduction.context,
                                                       start=True)
            except Exception as error:  # pylint: disable=broad-except
                logger.warning(
                    'Unable to adopt durable launch for replica %s: %s',
                    info.replica_id, common_utils.format_exception(error))
        try:
            contexts = (ordinary_launch_binding.
                        list_provider_reconciliation_contexts(authority))
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Unable to list bound non-pool provider reconciliations: %s',
                common_utils.format_exception(error))
            return
        active_ids = {
            binding_context.replica_id for binding_context in contexts
        }
        infos = {
            (info.replica_id, info.replica_record_id): info
            for info in replica_infos
        }
        for binding_context in contexts:
            if (binding_context.replica_id in runtime.launch_thread_pool or
                    binding_context.replica_id in runtime.down_thread_pool):
                # Finished launch workers already own the established
                # scheduling path, including its retry and logging behavior.
                continue
            context_info = infos.get((binding_context.replica_id,
                                      str(binding_context.replica_record_id)))
            if context_info is None:
                continue
            if (self._provider_present_cleanup_marker_shape(context_info) and
                    global_user_state.cluster_with_name_exists(
                        context_info.cluster_name) and not request_postgres.
                    bound_non_pool_provider_absence_is_recorded(
                        binding_context, authority)):
                # The existing committed-cleanup reconciler consumes this
                # restart shape below.  Do not race its down worker with an
                # independent provider read while evidence is still PRESENT.
                # A crash after ABSENT was recorded but before its projection
                # must instead consume that immutable evidence immediately,
                # even if a stale SkyPilot cluster record still exists.
                continue
            self._schedule_non_pool_provider_reconciliation(
                context_info, binding_context)

        # A successful projection clears the replica pointer, so the context
        # disappears from the next query. Retire its completed process-local
        # bookkeeping without disturbing a worker still finishing that
        # transaction.
        for replica_id, worker in list(
                self._non_pool_reconciliation_threads.items()):
            if replica_id in active_ids or worker.is_alive():
                continue
            self._non_pool_reconciliation_threads.pop(replica_id)
            self._non_pool_reconciliation_attempts.pop(replica_id, None)
            self._non_pool_reconciliation_retry_at.pop(replica_id, None)

    def _redrive_bound_ordinary_launch_after_pre_effect(
            self, info: ReplicaInfo) -> bool:
        """Re-enqueue one settled pre-effect row with its exact paid claim."""
        authority = self._ordinary_launch_binding_authority
        if (authority is not None and
                authority.retained_non_pool_settlement_allowed):
            retirement = (ordinary_launch_binding.
                          retire_pre_admission_non_pool_launch_intent(
                              authority, info.replica_id,
                              info.replica_record_id))
            if retirement.disposition in (
                    ordinary_launch_binding.PreAdmissionRetirementDisposition.
                    RETIRED, ordinary_launch_binding.
                    PreAdmissionRetirementDisposition.ABSENT):
                self._notify_scale_reconciliation()
                return True
            return False
        prior_planned_capacity = info.planned_capacity
        if (isinstance(prior_planned_capacity, bool) or
                not isinstance(prior_planned_capacity, int) or
                prior_planned_capacity < 1):
            prior_planned_capacity = 1
        prior_yaml_content: str | None
        if info.version == self.latest_version:
            prior_yaml_content = self.yaml_content
        else:
            prior_yaml_content = serve_state.get_yaml_content(
                self._service_name, info.version)
        if prior_yaml_content is None:
            raise ValueError('yaml content not found for pre-effect retry '
                             f'of {self._service_name} version '
                             f'{info.version}')
        result = self._launch_replica(
            info.replica_id,
            resources_override=info.resources_override,
            recovering_existing_replica=True,
            prior_is_zero_cost=info.is_zero_cost,
            prior_planned_capacity=prior_planned_capacity,
            prior_unknown_capacity_replacement=bool(
                info.unknown_capacity_replacement),
            prior_replica_record_id=info.replica_record_id,
            prior_created_at=info.created_at,
            prior_version=info.version,
            prior_yaml_content=prior_yaml_content,
            prior_paid_capacity_pool_key=(
                info.paid_capacity_pool_key if isinstance(
                    info.paid_capacity_pool_key, str) else None))
        return result is not None

    def _bound_non_pool_provider_present_cleanup_context(
        self,
        info: ReplicaInfo,
        target: Any = None,
    ) -> 'BoundNonPoolLaunchContext | None':
        """Return one fully revalidated durable PRESENT cleanup marker."""
        authority = self._ordinary_launch_binding_authority
        if (authority is None or authority.capable is not True or
                authority.binding_mode
                != ordinary_launch_binding.BindingMode.BOUND or
                not authority.retained_non_pool_settlement_allowed):
            return None
        if target is None:
            target = request_postgres.lookup_bound_ordinary_launch_cancel_target(
                self._service_name, info.replica_id, info.replica_record_id)
        if target is None:
            return None
        binding_context = target.context
        if (not isinstance(binding_context,
                           ordinary_launch_binding.BoundNonPoolLaunchContext) or
                binding_context.profile.kind != ordinary_launch_binding.
                NonPoolLaunchProfileKind.RESERVED_FILL and
                not ordinary_launch_binding.
                is_paid_provider_reconciliation_profile(
                    binding_context.profile.kind)):
            return None
        if not (request_postgres.
                bound_non_pool_provider_present_cleanup_is_authorized(
                    binding_context, authority)):
            return None
        return binding_context

    @staticmethod
    def _provider_present_cleanup_marker_shape(info: ReplicaInfo) -> bool:
        """Match the durable marker through its down-worker lifecycle."""
        return ordinary_launch_binding.replica_has_provider_present_cleanup_marker(
            info)

    def _settle_bound_ordinary_launch_for_teardown(
        self,
        info: ReplicaInfo,
    ) -> 'BoundNonPoolLaunchContext | None':
        """Cancel, quiesce, and project an exact request before provider down."""
        authority = self._ordinary_launch_binding_authority
        if (authority is None or authority.capable is not True or
                authority.binding_mode
                != ordinary_launch_binding.BindingMode.BOUND):
            return None
        initial = request_postgres.lookup_bound_ordinary_launch_cancel_target(
            self._service_name, info.replica_id, info.replica_record_id)
        if initial is None:
            return None
        provider_present_cleanup = (
            self._bound_non_pool_provider_present_cleanup_context(
                info, initial))
        if provider_present_cleanup is not None:
            return provider_present_cleanup
        _, reduce_exact, cancel_exact = (self._bound_ordinary_launch_callbacks(
            info, None, initial_context=initial.context))
        durable_reason = getattr(initial, 'cancel_reason', None)
        if durable_reason is not None and (not isinstance(durable_reason, str)
                                           or not durable_reason):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Bound teardown found a malformed durable cancel reason.')
        projection = cancel_exact(durable_reason or 'replica-teardown')
        attempts = 0
        while True:
            if projection is None:
                raise _BoundOrdinaryLaunchUnresolvedError(
                    f'Bound teardown for replica {info.replica_id} lost its '
                    'exact association.')
            classification = _bound_projection_classification(projection)
            if classification == 'AMBIGUOUS':
                raise _BoundOrdinaryLaunchUnresolvedError(
                    f'Bound teardown for replica {info.replica_id} is '
                    'durably ambiguous; refusing provider cleanup.')
            if (getattr(projection, 'projected', False) or classification
                    in ('PROJECTED', 'PRE_EFFECT_TERMINAL', 'SETTLED')):
                return None
            if classification not in ('ADOPT_ACTIVE', 'WAIT_QUIESCENCE',
                                      'REDUCE_TERMINAL',
                                      'PRE_EFFECT_TERMINALIZE'):
                raise _BoundOrdinaryLaunchUnresolvedError(
                    f'Bound teardown for replica {info.replica_id} returned '
                    f'unknown classification {classification!r}.')
            attempts += 1
            if attempts == 1 or attempts % 20 == 0:
                logger.info(
                    'Waiting for exact ordinary-launch quiescence before '
                    'tearing down replica %s (classification=%s).',
                    info.replica_id, classification)
            time.sleep(_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS)
            projection = reduce_exact(None, None)

    def _request_bound_ordinary_launch_cancel_for_teardown(
            self, info: ReplicaInfo) -> None:
        """Deliver exact cancellation without waiting for provider authority."""
        authority = self._ordinary_launch_binding_authority
        if (authority is None or authority.capable is not True or
                authority.binding_mode
                != ordinary_launch_binding.BindingMode.BOUND):
            return
        target = request_postgres.lookup_bound_ordinary_launch_cancel_target(
            self._service_name, info.replica_id, info.replica_record_id)
        if target is None:
            return
        durable_reason = target.cancel_reason
        request_postgres.request_bound_ordinary_launch_cancel(
            target.context, authority, durable_reason or 'replica-teardown')

    @contextlib.contextmanager
    def ordinary_launch_binding_transition(
        self,) -> Iterator[Callable[[Any], None]]:
        """Fence process admission while the controller changes binding mode.

        The controller must already hold its actuation-epoch lock. This method
        then establishes the manager side of the lock order: transition lock,
        followed by ``self.lock``. The yielded installer is called only after
        the PostgreSQL promotion/demotion transaction commits and its returned
        authority has been refreshed. Assignment occurs while both process
        locks and the admission gate remain held.
        """
        with self._ordinary_launch_binding_transition_lock:
            self._ordinary_launch_binding_transition_in_progress.set()
            try:
                with self.lock:
                    eligible_workers = [
                        replica_id for replica_id, worker in
                        self._legacy_mutation_runtime_state(
                        ).launch_thread_pool.items()
                        if isinstance(worker, _ReplicaLaunchThread)
                    ]
                    if eligible_workers:
                        raise _BoundOrdinaryLaunchUnresolvedError(
                            'Launch-binding transition found local eligible '
                            'workers '
                            'that have not crossed the '
                            f'completion barrier: {sorted(eligible_workers)}.')

                    installed = False

                    def _install(authority: Any) -> None:
                        nonlocal installed
                        if installed:
                            raise RuntimeError(
                                'Binding transition authority was installed '
                                'more than once.')
                        if not isinstance(
                                authority, ordinary_launch_binding.
                                ControllerBindingAuthority):
                            raise ValueError(
                                'Binding transition returned malformed '
                                'controller authority.')
                        previous = self._ordinary_launch_binding_authority
                        if previous is None:
                            raise _BoundOrdinaryLaunchUnresolvedError(
                                'Manager has no prior controller binding '
                                'authority.')
                        immutable_identity = (
                            authority.service_name == self._service_name and
                            authority.service_hash == previous.service_hash and
                            authority.service_workspace
                            == previous.service_workspace and
                            authority.service_lifecycle_epoch
                            == previous.service_lifecycle_epoch and
                            authority.controller_incarnation
                            == previous.controller_incarnation and
                            authority.controller_owner_epoch
                            == previous.controller_owner_epoch and
                            authority.controller_pid == previous.controller_pid
                            and
                            authority.controller_ip == previous.controller_ip)
                        if not immutable_identity or authority.capable is not True:
                            raise _BoundOrdinaryLaunchUnresolvedError(
                                'Binding transition changed controller or '
                                'service identity.')
                        self._ordinary_launch_binding_authority = authority
                        installed = True

                    yield _install
            finally:
                self._ordinary_launch_binding_transition_in_progress.clear()

    def _queued_launch_generation_decision(
            self, expected_manager_version: int) -> tuple[bool, str]:
        """Fence every queued worker to its construction-time manager epoch."""
        if self._update_recovery_required:
            return False, 'controller-update-recovery-required'
        current_version = self.latest_version
        if current_version != expected_manager_version:
            return False, 'manager-version-changed'
        pending_version = self._pending_version
        if (pending_version is not None and
                pending_version > expected_manager_version):
            return False, 'newer-version-pending'
        return True, 'authorized'

    def fence_launches_for_update_recovery(self) -> None:
        """Irreversibly stop all manager actuation until child respawn."""
        super().fence_launches_for_update_recovery()
        self._manager_daemon_stop.set()
        self._launch_completion_event.set()

    def _manager_daemon_should_stop(self) -> bool:
        """Whether background manager duties must perform no more work."""
        should_stop = (self._update_recovery_required or
                       self._ownership_lost.is_set())
        if should_stop:
            # Propagate direct ownership-event writes used by embedders/tests
            # so the supervisor observes the same terminal duty state.
            self._manager_daemon_stop.set()
        return should_stop or self._manager_daemon_stop.is_set()

    def _wait_for_manager_daemon_stop(self, timeout_seconds: float) -> bool:
        """Interrupt one daemon interval on either terminal stop reason."""
        if self._manager_daemon_should_stop():
            return True
        if self._manager_daemon_stop.wait(timeout_seconds):
            return True
        return self._manager_daemon_should_stop()

    def _service_owner_watchdog(self) -> None:
        """Trip one shared launch-cancellation fence on ownership loss."""
        if self._service_hash is None:
            return
        ownership_lost = self._ownership_lost
        while not ownership_lost.wait(_SERVICE_OWNER_WATCH_INTERVAL_SECONDS):
            authorization = self._service_launch_authorization()
            if authorization is False:
                logger.warning(
                    f'Service {self._service_name!r} controller ownership '
                    'was lost; cancelling in-flight replica launches and '
                    'refusing queued launches.')
                return

    @staticmethod
    def _has_system_recovery_teardown_intent(info: ReplicaInfo) -> bool:
        status = info.status_property
        return bool(info.is_terminal or status.preempted or status.purged or
                    status.is_scale_down or status.sky_down_status is not None)

    def _initialize_system_recovery_process_guards(
            self, infos: list[ReplicaInfo]) -> list[ReplicaInfo]:
        """Rebuild conservative monotonic/startup barriers after restart."""
        monotonic_now = time.monotonic()
        wall_now = time.time()
        self._prune_system_recovery_process_guards(infos)
        deadlines = self._candidate_release_monotonic_deadlines
        changed = False
        for info in infos:
            if info.system_recovery_quarantine is not None:
                continue
            disposition = info.system_recovery_disposition
            if (disposition
                    == system_recovery_state.SystemRecoveryDisposition.CANDIDATE
                    and info.candidate_ready_observed_at is not None):
                # A process replacement can never prove continuity with the
                # old monotonic clock, so every unresolved anchored candidate
                # waits a complete fresh guard.
                deadlines[info.replica_id] = (
                    monotonic_now +
                    system_recovery_state.CANDIDATE_RELEASE_GUARD_SECONDS)
            if (disposition
                    == system_recovery_state.SystemRecoveryDisposition.CANDIDATE
                    and info.status_property.service_ready_now):
                info.status_property.service_ready_now = False
                self._persist_replica(info.replica_id, info)
                changed = True
            if (disposition
                    != system_recovery_state.SystemRecoveryDisposition.CAPABLE
                    or info.system_recovery is None or
                    info.system_recovery.state
                    == system_recovery_state.ControllerRecoveryState.EXHAUSTED
                    or self._has_system_recovery_teardown_intent(info)):
                continue

            def _start_barrier(fresh: ReplicaInfo) -> bool:
                if (fresh.system_recovery_disposition != system_recovery_state.
                        SystemRecoveryDisposition.CAPABLE or
                        fresh.system_recovery is None or
                        fresh.system_recovery.state == system_recovery_state.
                        ControllerRecoveryState.EXHAUSTED or
                        self._has_system_recovery_teardown_intent(fresh)):
                    return False
                fresh.system_recovery = dataclasses.replace(
                    fresh.system_recovery, status_barrier_started_at=wall_now)
                return True

            updated = self._patch_system_recovery_with_latest(
                info.replica_id, _start_barrier)
            if updated is not None:
                if updated.status_property.service_ready_now:
                    updated.status_property.service_ready_now = False
                    self._persist_replica(updated.replica_id, updated)
                changed = True
        if changed:
            return serve_state.get_replica_infos(self._service_name)
        return infos

    def _prune_system_recovery_process_guards(self,
                                              infos: list[ReplicaInfo]) -> None:
        """Bound process-local recovery guards to their live durable rows."""
        candidate_ids = {
            info.replica_id
            for info in infos
            if (info.system_recovery_quarantine is None and
                info.system_recovery_disposition ==
                system_recovery_state.SystemRecoveryDisposition.CANDIDATE and
                not self._has_system_recovery_teardown_intent(info))
        }
        deadlines = self._candidate_release_monotonic_deadlines
        self._candidate_release_monotonic_deadlines = {
            replica_id: deadline
            for replica_id, deadline in deadlines.items()
            if replica_id in candidate_ids
        }

        capable_ids = {
            info.replica_id
            for info in infos
            if (info.system_recovery_quarantine is None and
                info.system_recovery_disposition ==
                system_recovery_state.SystemRecoveryDisposition.CAPABLE and
                info.system_recovery is not None and info.system_recovery.state
                != system_recovery_state.ControllerRecoveryState.EXHAUSTED and
                not self._has_system_recovery_teardown_intent(info))
        }
        self._system_recovery_status_initialized.intersection_update(
            capable_ids)

    def _suspend_system_recovery_route_if_unroutable(
        self, info: ReplicaInfo
    ) -> system_recovery_route_lease.RouteSuspension | None:
        """Reversibly omit one exact row around an off-route DB mutation."""
        if (info.system_recovery_disposition
                != system_recovery_state.SystemRecoveryDisposition.CAPABLE):
            return None
        if (info.status == serve_state.ReplicaStatus.READY and
                self.system_recovery_allows_routing(info)):
            return None
        return self._route_lease_registry().suspend_record(
            info.replica_id, info.replica_record_id)

    def _commit_system_recovery_route_suspensions(
            self, suspensions: list[system_recovery_route_lease.RouteSuspension]
    ) -> None:
        for suspension in suspensions:
            self._route_lease_registry().commit_suspension(suspension)

    def _rollback_system_recovery_route_suspensions(
            self, suspensions: list[system_recovery_route_lease.RouteSuspension]
    ) -> None:
        for suspension in suspensions:
            self._route_lease_registry().rollback_suspension(suspension)

    def _resolve_ambiguous_system_recovery_route_suspensions(
            self, suspensions: list[system_recovery_route_lease.RouteSuspension]
    ) -> None:
        """Resolve holds after a DB exception without reviving stale routes.

        An exception can be raised before commit or after a successful commit.
        Rollback is therefore allowed only when a fresh durable read proves
        that this controller still owns the service and every exact suspended
        generation remains a routable row.  Every unproven hold is retired.
        """
        if not suspensions:
            return
        expected_service_hash = self._service_hash
        expected_controller_owner = self._controller_owner
        if (not isinstance(expected_service_hash, str) or
                not expected_service_hash or
                not isinstance(expected_controller_owner, tuple) or
                len(expected_controller_owner) != 2):
            self._commit_system_recovery_route_suspensions(suspensions)
            return
        try:
            owner = serve_state.get_service_controller_owner(self._service_name)
            if (not isinstance(owner, Mapping) or
                    owner.get('hash') != expected_service_hash or
                (owner.get('controller_pid'), owner.get('controller_ip'))
                    != expected_controller_owner):
                self._commit_system_recovery_route_suspensions(suspensions)
                return
            fresh_infos = serve_state.get_replica_infos_from_ids(
                self._service_name,
                sorted({suspension.replica_id for suspension in suspensions}))
            if not isinstance(fresh_infos, Mapping):
                self._commit_system_recovery_route_suspensions(suspensions)
                return
        except asyncio.CancelledError:
            # A cancelled reconciliation must not strand a reversible hold or
            # make an old generation routable again. Retire every suspended
            # route, then preserve task cancellation semantics.
            self._commit_system_recovery_route_suspensions(suspensions)
            raise
        except BaseException:  # pylint: disable=broad-exception-caught
            self._commit_system_recovery_route_suspensions(suspensions)
            return

        for suspension in suspensions:
            try:
                fresh = fresh_infos.get(suspension.replica_id)
                generation = (None if fresh is None else
                              self._system_recovery_route_generation(fresh))
                can_restore = (
                    fresh is not None and
                    fresh.replica_id == suspension.replica_id and
                    fresh.replica_record_id
                    == suspension.generation.replica_record_id and
                    generation == suspension.generation and
                    fresh.status == serve_state.ReplicaStatus.READY and
                    self.system_recovery_allows_routing(fresh) and
                    not self._has_system_recovery_teardown_intent(fresh))
            except asyncio.CancelledError:
                self._commit_system_recovery_route_suspensions(suspensions)
                raise
            except BaseException:  # pylint: disable=broad-exception-caught
                can_restore = False
            if can_restore:
                self._route_lease_registry().rollback_suspension(suspension)
            else:
                self._route_lease_registry().commit_suspension(suspension)

    def _persist_replica(self, replica_id: int, info: ReplicaInfo) -> None:
        if self._update_recovery_required:
            return
        suspension = self._suspend_system_recovery_route_if_unroutable(info)
        if self._update_recovery_required:
            if suspension is not None:
                self._route_lease_registry().rollback_suspension(suspension)
            return
        try:
            persisted = serve_state.add_or_update_replica(
                self._service_name,
                replica_id,
                info,
                **self._db_fence_kwargs(),
                expected_replica_exists=True,
                guard_launch_exclusion=(
                    serve_state.replica_info_has_binding_excluded_profile(info)
                ))
        except BaseException:
            if suspension is not None:
                self._resolve_ambiguous_system_recovery_route_suspensions(
                    [suspension])
            raise
        if persisted is False:
            if suspension is not None:
                self._route_lease_registry().commit_suspension(suspension)
            raise RuntimeError(
                f'Service {self._service_name!r} ownership changed or replica '
                f'{replica_id} disappeared while persisting bookkeeping.')
        if suspension is not None:
            self._route_lease_registry().commit_suspension(suspension)

    def _restore_never_started_launch_to_scheduled(
            self, replica_id: int,
            replica_record_id: str) -> ReplicaInfo | None:
        """Recover one durable launch reservation with no local worker.

        Exact record identity and controller ownership are re-read in the same
        transaction that owns the cross-pod mutation guard before reversing
        only a RUNNING reservation to SCHEDULED. Any other state is left
        untouched so an ambiguous Thread.start acknowledgement remains
        conservative.
        """
        return serve_state.restore_never_started_replica_launch_to_scheduled(
            self._service_name, replica_id, replica_record_id,
            **self._db_fence_kwargs())

    def _persist_new_replica(self, replica_id: int, info: ReplicaInfo) -> None:
        """Persist an explicitly admitted initial replica row."""
        persisted = serve_state.add_or_update_replica(
            self._service_name,
            replica_id,
            info,
            **self._db_fence_kwargs(),
            guard_launch_exclusion=(
                serve_state.replica_info_has_binding_excluded_profile(info)))
        if persisted is False:
            raise RuntimeError(
                f'Service {self._service_name!r} incarnation changed while '
                f'admitting replica {replica_id}.')
        # A successfully inserted recreation is now the live row even if a
        # delayed callback still carries the same numeric replica ID.
        self._route_lease_registry().observe_record_identity(
            replica_id, info.replica_record_id)

    def _emit_ordinary_launch_handoff_event(
        self,
        info: ReplicaInfo,
        event_kind: ordinary_launch_handoff.EventKind,
        ordinary_request_id: str | None = None,
        service_job_id: int | None = None,
        terminal_status: ordinary_launch_handoff.TerminalStatus | None = None,
        *,
        input_digest: str | None = None,
        allow_demoted_candidate: bool = False,
    ) -> None:
        """Emit diagnostic evidence without changing replica behavior."""
        if self._is_pool or info.reserved_fill:
            return
        disposition = info.system_recovery_disposition
        # The launch thread captures the candidate object before durable
        # demotion.  Its callback may use that stale value only on attempts
        # that launch_cluster has already classified as ordinary; the bound
        # recovery attempt is gated before every callback invocation.
        if (disposition
                != system_recovery_state.SystemRecoveryDisposition.ORDINARY and
                not (allow_demoted_candidate and
                     disposition == system_recovery_state.
                     SystemRecoveryDisposition.CANDIDATE)):
            return
        ordinary_launch_handoff.emit_event(
            event_kind=event_kind,
            service_name=self._service_name,
            service_version=info.version,
            replica_id=info.replica_id,
            replica_record_id=info.replica_record_id,
            controller_route_epoch=self._ordinary_launch_handoff_route_epoch,
            ordinary_request_id=ordinary_request_id,
            service_job_id=service_job_id,
            terminal_status=terminal_status,
            input_digest=input_digest)

    def _persist_replicas(
        self,
        replica_infos: list[tuple[int, ReplicaInfo]],
        *,
        route_suspensions: list[system_recovery_route_lease.RouteSuspension] |
        None = None,
    ) -> None:
        """Persist a batch and resolve every route hold with its outcome.

        ``route_suspensions`` transfers ownership of holds acquired before
        this call.  In particular, the readiness probe uses them to omit a
        route at the off-route decision instead of waiting until the later
        batched write.  Do not acquire a nested hold for a transferred
        replica: suspensions intentionally have no unique hold identifier, so
        double resolution could consume another concurrent owner's hold.
        """
        suspensions = list(route_suspensions or ())
        if self._update_recovery_required:
            self._rollback_system_recovery_route_suspensions(suspensions)
            return
        suspended_replica_ids = {
            suspension.replica_id for suspension in suspensions
        }
        try:
            for _, info in replica_infos:
                if info.replica_id in suspended_replica_ids:
                    continue
                suspension = (
                    self._suspend_system_recovery_route_if_unroutable(info))
                if suspension is not None:
                    suspensions.append(suspension)
                    suspended_replica_ids.add(suspension.replica_id)
        except BaseException:
            self._rollback_system_recovery_route_suspensions(suspensions)
            raise
        if self._update_recovery_required:
            self._rollback_system_recovery_route_suspensions(suspensions)
            return
        try:
            fence_kwargs = self._db_fence_kwargs()
            if not replica_infos and suspensions:
                fence_kwargs['validate_fence_on_empty'] = True
            persisted = serve_state.add_or_update_replicas(
                self._service_name,
                replica_infos,
                **fence_kwargs,
                expected_replica_exists=True,
                guard_launch_exclusion=any(
                    serve_state.replica_info_has_binding_excluded_profile(info)
                    for _, info in replica_infos))
        except BaseException:
            self._resolve_ambiguous_system_recovery_route_suspensions(
                suspensions)
            raise
        if persisted is False:
            self._commit_system_recovery_route_suspensions(suspensions)
            raise RuntimeError(
                f'Service {self._service_name!r} ownership changed or an '
                'expected replica disappeared while persisting a batch.')
        self._commit_system_recovery_route_suspensions(suspensions)

    def _system_recovery_mutation_fence(self) -> dict[str, Any] | None:
        """Snapshot the exact owner/lifecycle tuple for one recovery CAS."""
        service_hash = self._service_hash
        controller_owner = self._controller_owner
        if (not isinstance(service_hash, str) or not service_hash or
                controller_owner is None):
            return None
        owner = serve_state.get_service_controller_owner(self._service_name)
        if (owner is None or owner.get('hash') != service_hash or
            (owner.get('controller_pid'), owner.get('controller_ip'))
                != controller_owner):
            return None
        lifecycle_epoch = owner.get('lifecycle_epoch')
        if (isinstance(lifecycle_epoch, bool) or
                not isinstance(lifecycle_epoch, int) or lifecycle_epoch < 1):
            return None
        return {
            'expected_service_hash': service_hash,
            'expected_lifecycle_epoch': lifecycle_epoch,
            'expected_controller_owner': controller_owner,
        }

    def _patch_system_recovery_with_latest(
        self,
        replica_id: int,
        transition: Callable[[ReplicaInfo], bool],
    ) -> ReplicaInfo | None:
        """Refresh and rerun a recovery transition after revision conflicts."""
        for _ in range(8):
            if self._update_recovery_required:
                return None
            fresh = serve_state.get_replica_info_from_id(
                self._service_name, replica_id)
            fence = self._system_recovery_mutation_fence()
            if fresh is None or fence is None:
                return None
            if self._update_recovery_required:
                return None
            if not transition(fresh):
                return fresh
            suspension = self._suspend_system_recovery_route_if_unroutable(
                fresh)
            try:
                if self._update_recovery_required:
                    if suspension is not None:
                        self._route_lease_registry().rollback_suspension(
                            suspension)
                    return None
                updated = serve_state.patch_replica_system_recovery(
                    self._service_name,
                    replica_id,
                    fresh,
                    expected_revision=fresh.system_recovery_revision,
                    **fence)
            except serve_state.ReplicaSystemRecoveryRevisionConflict:
                if suspension is not None:
                    self._resolve_ambiguous_system_recovery_route_suspensions(
                        [suspension])
                continue
            except serve_state.ReplicaSystemRecoveryStateError as e:
                if suspension is not None:
                    self._resolve_ambiguous_system_recovery_route_suspensions(
                        [suspension])
                logger.warning(
                    f'Recovery-state patch was rejected for replica '
                    f'{replica_id}: {common_utils.format_exception(e)}')
                return None
            except BaseException:
                if suspension is not None:
                    self._resolve_ambiguous_system_recovery_route_suspensions(
                        [suspension])
                raise
            if suspension is not None:
                self._route_lease_registry().commit_suspension(suspension)
            return updated
        logger.warning(f'Recovery-state patch for replica {replica_id} '
                       'remained conflicted after repeated refreshes.')
        return None

    def _create_system_recovery_candidate(
        self, replica_id: int,
        intent: system_recovery_state.SystemRecoveryLaunchIntent
    ) -> ReplicaInfo | None:
        """Transition a freshly persisted ordinary row to CANDIDATE."""
        for _ in range(8):
            fresh = serve_state.get_replica_info_from_id(
                self._service_name, replica_id)
            fence = self._system_recovery_mutation_fence()
            if fresh is None or fence is None:
                return None
            if (fresh.system_recovery_quarantine is not None or
                    fresh.system_recovery_launch_intent is not None or
                    fresh.system_recovery_disposition !=
                    system_recovery_state.SystemRecoveryDisposition.ORDINARY):
                return None
            fresh.system_recovery_launch_intent = intent
            fresh.system_recovery_disposition = (
                system_recovery_state.SystemRecoveryDisposition.CANDIDATE)
            try:
                created = serve_state.create_replica_system_recovery_candidate(
                    self._service_name,
                    replica_id,
                    fresh,
                    expected_revision=fresh.system_recovery_revision,
                    **fence)
                system_oom_recovery_observability.record_for_replica(
                    'authorization_v3_candidate', created)
                return created
            except serve_state.ReplicaSystemRecoveryRevisionConflict:
                continue
            except serve_state.ReplicaSystemRecoveryStateError as e:
                logger.warning(
                    f'Recovery candidacy was rejected for replica '
                    f'{replica_id}: {common_utils.format_exception(e)}')
                return None
        logger.warning(f'Recovery candidacy for replica {replica_id} kept '
                       'conflicting; leaving the launch ordinary.')
        return None

    def _get_bound_system_recovery_request_id(
            self, replica_id: int,
            intent: system_recovery_state.SystemRecoveryLaunchIntent
    ) -> str | None:
        """Read only the exact durable request association for one intent."""
        fresh = serve_state.get_replica_info_from_id(self._service_name,
                                                     replica_id)
        if (fresh is None or fresh.system_recovery_quarantine is not None or
                fresh.system_recovery_launch_intent != intent or
                fresh.system_recovery_disposition
                != system_recovery_state.SystemRecoveryDisposition.CANDIDATE):
            return None
        request_id = fresh.launch_request_id
        return request_id if isinstance(request_id,
                                        str) and request_id else None

    def _persist_system_recovery_job_id(
            self, replica_id: int,
            intent: system_recovery_state.SystemRecoveryLaunchIntent,
            request_id: str, service_job_id: int) -> bool:
        """Persist one exact request result without taking the manager lock."""
        for _ in range(8):
            fresh = serve_state.get_replica_info_from_id(
                self._service_name, replica_id)
            fence = self._system_recovery_mutation_fence()
            if fresh is None or fence is None:
                return False
            if (fresh.system_recovery_quarantine is not None or
                    fresh.system_recovery_launch_intent != intent or
                    fresh.system_recovery_disposition
                    != system_recovery_state.SystemRecoveryDisposition.CANDIDATE
                    or fresh.launch_request_id != request_id):
                return False
            if fresh.service_job_id == service_job_id:
                return True
            if fresh.service_job_id is not None:
                return False
            try:
                serve_state.set_replica_system_recovery_job_id(
                    self._service_name,
                    replica_id,
                    service_job_id,
                    expected_launch_request_id=request_id,
                    expected_revision=fresh.system_recovery_revision,
                    **fence)
                return True
            except serve_state.ReplicaSystemRecoveryRevisionConflict:
                continue
            except serve_state.ReplicaSystemRecoveryStateError as e:
                logger.warning(
                    f'Exact recovery job association was rejected for '
                    f'replica {replica_id}: '
                    f'{common_utils.format_exception(e)}')
                return False
        return False

    def _demote_system_recovery_candidate(
            self, replica_id: int,
            intent: system_recovery_state.SystemRecoveryLaunchIntent) -> bool:
        """Irreversibly demote one failed first request before any retry."""
        for _ in range(8):
            fresh = serve_state.get_replica_info_from_id(
                self._service_name, replica_id)
            fence = self._system_recovery_mutation_fence()
            if fresh is None or fence is None:
                return False
            if (fresh.system_recovery_quarantine is not None or
                    fresh.system_recovery_launch_intent != intent):
                return False
            if (fresh.system_recovery_disposition ==
                    system_recovery_state.SystemRecoveryDisposition.ORDINARY):
                return True
            if (fresh.system_recovery_disposition !=
                    system_recovery_state.SystemRecoveryDisposition.CANDIDATE):
                return False
            fresh.system_recovery_disposition = (
                system_recovery_state.SystemRecoveryDisposition.ORDINARY)
            try:
                demoted = serve_state.demote_replica_system_recovery_to_ordinary(
                    self._service_name,
                    replica_id,
                    fresh,
                    expected_revision=fresh.system_recovery_revision,
                    **fence)
                system_oom_recovery_observability.record_for_replica(
                    'authorization_v3_ordinary', demoted)
                return True
            except serve_state.ReplicaSystemRecoveryRevisionConflict:
                continue
            except serve_state.ReplicaSystemRecoveryStateError as e:
                logger.warning(
                    f'Recovery demotion was rejected for replica '
                    f'{replica_id}: {common_utils.format_exception(e)}')
                return False
        return False

    @with_lock
    def confirm_logical_bridge_capacities(
            self, verified_capacities: dict[int, int]) -> dict[int, int]:
        """Durably adopt fresh LB-proven widths for physical bridge rows.

        The caller has already bounded each observation by the launched GPU
        count. Re-read under the manager lock before updating so an older LB
        sync snapshot cannot overwrite a concurrent readiness or teardown
        transition. This path runs only for a bridge's first proof or a later
        monotonic width increase, not on every LB heartbeat.
        """
        if not self._uses_logical_replicas or not verified_capacities:
            return {}
        fresh_infos = {
            info.replica_id: info
            for info in serve_state.get_replica_infos(self._service_name)
        }
        updates: list[tuple[int, ReplicaInfo]] = []
        confirmed: dict[int, int] = {}
        for replica_id, verified_capacity in verified_capacities.items():
            info = fresh_infos.get(replica_id)
            if (info is None or info.is_terminal or
                    isinstance(verified_capacity, bool) or
                    not isinstance(verified_capacity, int) or
                    verified_capacity < 1):
                continue
            current_capacity = int(info.planned_capacity)
            adopted_capacity = max(current_capacity, verified_capacity)
            already_verified = bool(info.logical_bridge_capacity_verified)
            if not already_verified or adopted_capacity != current_capacity:
                info._version = ReplicaInfo._VERSION  # pylint: disable=protected-access
                info.planned_capacity = adopted_capacity
                info.logical_bridge_capacity_verified = True
                updates.append((replica_id, info))
            confirmed[replica_id] = adopted_capacity
        if updates:
            self._persist_replicas(updates)
        return confirmed

    def _remove_replica(
        self,
        replica_id: int,
        replica_record_id: str,
        *,
        allow_active_provider_free_pre_job: bool = False,
    ) -> None:
        suspension = self._route_lease_registry().suspend_record(
            replica_id, replica_record_id)
        try:
            removed = serve_state.remove_replica(
                self._service_name,
                replica_id,
                **self._db_fence_kwargs(),
                expected_replica_record_id=replica_record_id,
                allow_active_provider_free_pre_job=(
                    allow_active_provider_free_pre_job))
        except BaseException:
            if suspension is not None:
                self._resolve_ambiguous_system_recovery_route_suspensions(
                    [suspension])
            raise
        if removed is False:
            if suspension is not None:
                self._route_lease_registry().commit_suspension(suspension)
            raise RuntimeError(
                f'Service {self._service_name!r} incarnation changed while '
                f'removing replica {replica_id}.')
        if suspension is not None:
            self._route_lease_registry().commit_suspension(suspension)

    def _remove_replicas(self, replica_infos: list[ReplicaInfo]) -> None:
        """Remove one replica wave under a single durable owner fence."""
        if not replica_infos:
            return
        replica_ids = [info.replica_id for info in replica_infos]
        if len(set(replica_ids)) != len(replica_ids):
            raise ValueError('Replica cleanup wave contains duplicate IDs.')
        expected_record_ids = {
            info.replica_id: info.replica_record_id for info in replica_infos
        }
        service_hash = self._service_hash
        if service_hash is None:
            # Legacy/direct managers do not have the durable incarnation
            # identity required by the batch-delete API.
            for info in replica_infos:
                self._remove_replica(info.replica_id, info.replica_record_id)
            return
        suspensions = []
        try:
            for info in replica_infos:
                suspension = self._route_lease_registry().suspend_record(
                    info.replica_id, info.replica_record_id)
                if suspension is not None:
                    suspensions.append(suspension)
        except BaseException:
            self._rollback_system_recovery_route_suspensions(suspensions)
            raise
        try:
            removed = serve_state.remove_replicas(
                self._service_name,
                replica_ids,
                service_hash,
                expected_controller_owner=self._controller_owner,
                expected_replica_record_ids=(expected_record_ids))
        except BaseException:
            self._resolve_ambiguous_system_recovery_route_suspensions(
                suspensions)
            raise
        if removed is False:
            self._commit_system_recovery_route_suspensions(suspensions)
            raise RuntimeError(
                f'Service {self._service_name!r} incarnation changed while '
                f'removing {len(replica_ids)} replicas.')
        self._commit_system_recovery_route_suspensions(suspensions)

    def _failed_cleanup_retry_state(
            self) -> tuple[dict[int, int], dict[int, float]]:
        """Return retry maps, tolerating managers built before the runtime.

        Normal construction initializes the legacy runtime in ``__init__``.
        Its accessor also adopts old instance fields, protecting lightweight
        embedders, tests, and upgrade/recovery paths that reconstruct a manager
        without replaying the newest initializer in full.
        """
        # These process-local clocks rate-limit a durable cleanup intent.  A
        # restart may reset backoff but cannot forget the cleanup.
        runtime = self._legacy_mutation_runtime_state()
        return (runtime.failed_cleanup_retry_attempts,
                runtime.failed_cleanup_retry_at)

    def _clear_failed_cleanup_retry(self, replica_id: int) -> None:
        """Forget in-memory cleanup rate limiting after confirmed success."""
        self._legacy_mutation_runtime_state().clear_failed_cleanup_retry(
            replica_id)

    def _schedule_failed_cleanup_retry(self, replica_id: int) -> None:
        """Rate-limit, but never give up on, a durable cleanup failure."""
        attempt, delay_seconds = self._legacy_mutation_runtime_state(
        ).schedule_failed_cleanup_retry(replica_id, time.monotonic())
        logger.warning(f'Replica {replica_id} cleanup will retry in '
                       f'{delay_seconds}s (attempt {attempt}).')

    def _record_cleanup_uncertain(self, info: ReplicaInfo,
                                  message: str) -> None:
        """Retain one row whose exact provider cleanup cannot be proven."""
        logger.error(f'Replica {info.replica_id} cleanup is uncertain: '
                     f'{message}')
        if info.status_property.sky_launch_status in (
                None, common_utils.ProcessStatus.SCHEDULED,
                common_utils.ProcessStatus.INTERRUPTED):
            # ReplicaStatusProperty otherwise reports PENDING/SHUTTING_DOWN
            # before consulting sky_down_status, hiding this durable cleanup
            # failure from reconciliation and operators.
            info.status_property.sky_launch_status = (
                common_utils.ProcessStatus.FAILED)
        info.status_property.service_ready_now = False
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
        self._persist_replica(info.replica_id, info)
        self._schedule_failed_cleanup_retry(info.replica_id)

    def _provider_identity_uncertain_replica_ids(self) -> set[int]:
        """Return process-local rows awaiting a fresh identity proof."""
        return self._provider_identity_uncertain_ids

    def _record_provider_identity_uncertain(self, info: ReplicaInfo,
                                            message: str) -> None:
        """Keep a replica off-route without misclassifying provider state."""
        logger.error(
            f'Replica {info.replica_id} provider identity is uncertain: '
            f'{message}')
        info.status_property.service_ready_now = False
        uncertain_ids = self._provider_identity_uncertain_replica_ids()
        uncertain_ids.add(info.replica_id)
        replica_record_id = info.replica_record_id
        if isinstance(replica_record_id, str):
            self._route_lease_registry().deactivate_record(
                info.replica_id, replica_record_id)
        self._persist_replica(info.replica_id, info)

    def __init__(
        self,
        service_name: str,
        spec: 'service_spec.SkyServiceSpec',
        version: int,
        resource_scope: str | None = None,
        service_hash: str | None = None,
        controller_pid: int | None = None,
        controller_ip: str | None = None,
        enforce_launch_fence: bool = True,
        controller_binding_authority: (
            'ControllerBindingAuthority | None') = None
    ) -> None:
        # Keep the historical three-argument base-init call for embedders that
        # replace it, then restore the scope it initializes to the legacy
        # default.  Setting this before super() would be silently overwritten.
        super().__init__(service_name, spec, version)
        self._resource_scope = resource_scope
        self._service_hash = service_hash
        self._controller_owner = ((controller_pid,
                                   controller_ip) if service_hash is not None or
                                  controller_pid is not None or
                                  controller_ip is not None else None)
        self._enforce_launch_fence = enforce_launch_fence
        self._ordinary_launch_binding_authority = controller_binding_authority
        if controller_binding_authority is None:
            self._reserved_fill_actuation_mode = (
                zero_cost_actuation.get_service_mode(service_name))
        else:
            try:
                self._reserved_fill_actuation_mode = (
                    zero_cost_actuation.advertise_capability(
                        service_name,
                        controller_binding_authority.controller_incarnation))
            except zero_cost_actuation.ZeroCostActuationError as error:
                self._reserved_fill_actuation_mode = None
                logger.warning(
                    'Zero-cost actuation capability could not be installed: '
                    '%s', common_utils.format_exception(error))
        yaml_content = serve_state.get_yaml_content(service_name, version)
        assert yaml_content is not None, (
            f'yaml content not found for {service_name} version {version}')
        self.yaml_content: str = yaml_content
        task = load_task_with_service_spec(self.yaml_content, spec)
        self._version_specs = {version: spec}
        self._version_task_templates = {version: task}
        self._uses_logical_replicas = spec.uses_logical_replicas is True
        self._default_planned_capacity = _uniform_whole_gpu_capacity(
            task.resources)
        self._logical_exact_accelerator_shapes = (_exact_accelerator_shapes(
            task.resources) if self._uses_logical_replicas else {})
        self._spot_placer = _load_spot_placer(service_name, version, spec, task,
                                              self._workspace)
        if self._uses_logical_replicas:
            _validate_logical_capacity_sources(self._default_planned_capacity,
                                               self._spot_placer,
                                               task.num_nodes)
        # Run recovery in its own thread, but only start the daemon threads
        # once recovery HOLDS the manager lock. Two hazards shaped this:
        #
        # 1. (original) If a daemon grabs `self.lock` before recovery —
        #    `_job_status_fetcher` SSHes every replica under it — recovery
        #    (and formerly __init__) blocks for the daemon's full walk.
        #    The lock-acquired handshake below preserves the guarantee that
        #    recovery gets the lock FIRST, without recovery having to finish
        #    before __init__ returns.
        # 2. (fleet-scale) Recovery itself is O(interrupted operations): at
        #    a measured ~860-row fleet with ~520 interrupted launches it
        #    runs for minutes. When it ran synchronously here, uvicorn never
        #    bound within _start's 60s readiness window
        #    (SERVICE_REGISTER_TIMEOUT_SECONDS) → _bail_on_boot_failure →
        #    os._exit(1) → daemon respawn → recovery restarted from scratch,
        #    forever: a controller crash-loop that froze the whole service.
        #    With the thread, uvicorn binds within seconds. Each recovery pass
        #    proceeds under the lock, but a failed pass releases it before
        #    backoff so probes/scaling are not frozen by one unresolved row.
        recovery_lock_acquired = threading.Event()

        def _recover_with_lock() -> None:
            try:
                # Retry a failed recovery pass instead of dying silently. Each
                # pass owns the manager lock, but the backoff deliberately
                # does not: one malformed or ambiguous association must not
                # freeze probes, routes, scaling, or healthy sibling actions
                # while its exact recovery is retried. Re-entry is idempotent;
                # already reconstructed workers remain in their pools.
                backoff_seconds = 30
                while not self._manager_daemon_should_stop():
                    with self.lock:
                        recovery_lock_acquired.set()
                        try:
                            self._recover_replica_operations()
                        except Exception as e:  # pylint: disable=broad-except
                            logger.error(
                                'Replica recovery pass failed; retrying in '
                                f'{backoff_seconds}s: '
                                f'{common_utils.format_exception(e)}')
                            with ux_utils.enable_traceback():
                                logger.error(
                                    f'  Traceback: {traceback.format_exc()}')
                        else:
                            return
                    if self._manager_daemon_stop.wait(backoff_seconds):
                        return
            finally:
                # Failsafe: never leave __init__ waiting if this thread dies
                # before signaling (nothing before the `with` can normally
                # raise, but a stuck boot here would resurrect the exact
                # crash-loop this design removes).
                recovery_lock_acquired.set()

        threading.Thread(target=_recover_with_lock,
                         name='replica-recovery',
                         daemon=True).start()
        recovery_lock_acquired.wait()

        threading.Thread(target=self._service_owner_watchdog,
                         name='replica-service-owner-watchdog',
                         daemon=True).start()

        # Supervised so a BaseException escaping any of these loops (or the
        # loop returning) does not silently freeze replica reconciliation /
        # job-status reaping / readiness probing while the controller keeps
        # serving HTTP -- they are restarted instead.
        thread_utils.start_supervised_thread(
            self._thread_pool_refresher,
            'replica-thread-pool-refresher',
            stop_event=self._manager_daemon_stop)
        thread_utils.start_supervised_thread(
            self._job_status_fetcher,
            'replica-job-status-fetcher',
            stop_event=self._manager_daemon_stop)
        thread_utils.start_supervised_thread(
            self._replica_prober,
            'replica-prober',
            stop_event=self._manager_daemon_stop)
        thread_utils.start_supervised_thread(
            self._system_recovery_route_prober,
            'replica-system-recovery-route-prober',
            stop_event=self._manager_daemon_stop)
        thread_utils.start_supervised_thread(
            self._zero_cost_actuation_dispatcher,
            'replica-zero-cost-actuation-dispatcher',
            stop_event=self._manager_daemon_stop)

    def _recover_replica_operations(self):
        """Route restart inference through the current mutation runtime."""
        self._legacy_mutation_runtime_state().recover(
            self._recover_legacy_replica_operations)

    def _recover_legacy_replica_operations(self) -> None:
        """Re-drive interrupted replica operations from durable state.

        Runs in the dedicated recovery thread started by __init__, which
        holds the manager lock for one pass but releases it between failed
        retries (see __init__ for the lock-ordering handshake with daemon
        threads)."""
        # This remains the current launch/down restart-recovery owner.  A
        # future bounded ordinary-launch association may replace only its
        # duplicate-request inference after independent qualification.
        legacy_runtime = self._legacy_mutation_runtime_state()
        if (legacy_runtime.launch_thread_pool or
                legacy_runtime.down_thread_pool):
            # Only possible on a RETRY of a partially-completed recovery
            # pass: the per-replica enqueues below skip anything already in
            # the pools, so re-running is safe.
            logger.warning(
                'Recovery pass re-entered with '
                f'{len(legacy_runtime.launch_thread_pool)} launch / '
                f'{len(legacy_runtime.down_thread_pool)} down threads '
                'already enqueued; continuing idempotently.')

        # Seed the replica-id allocator from durable state. A fresh
        # ReplicaManager initializes `self._next_replica_id` to 1 (see
        # `ReplicaManager.__init__`). On a controller respawn -- a
        # consolidation-mode API-server pod restart re-running `_start`
        # (is_recovery=True), or the in-place controller-respawn path -- a
        # brand-new ReplicaManager is constructed, resetting the allocator to
        # 1 even though replicas 1..N survived in the DB. The next `scale_up`
        # would then reuse an id a live replica still owns: `_launch_replica`
        # upserts a fresh `ReplicaInfo` over the survivor's persisted row
        # (`add_or_update_replica` is keyed on (service_name, replica_id)),
        # destroying its status/version/failure history, and re-runs
        # `sky.launch` against its live serving cluster. Advance the allocator
        # past every persisted id so new replicas always get a fresh id. On a
        # first run there are no replicas, so this stays at 1 (no-op).
        all_replica_infos = serve_state.get_replica_infos(self._service_name)
        all_replica_infos = self._initialize_system_recovery_process_guards(
            all_replica_infos)
        recovery_teardowns = [
            info for info in all_replica_infos
            if (info.system_recovery_quarantine is not None or
                (info.system_recovery is not None and info.system_recovery.state
                 == system_recovery_state.ControllerRecoveryState.EXHAUSTED)
               ) and not self._has_system_recovery_teardown_intent(info)
        ]
        for info in recovery_teardowns:
            logger.warning(
                f'Replica {info.replica_id} has terminal or quarantined '
                'system-recovery state; adopting legacy teardown.')
            self._terminate_replica(info.replica_id,
                                    replica_drain_delay_seconds=0)
        if recovery_teardowns:
            all_replica_infos = serve_state.get_replica_infos(
                self._service_name)
        self._restore_spot_placement_state()
        if not paid_capacity.adopt_existing_claims(
                service_name=self._service_name,
                service_hash=self._service_hash,
                controller_owner=self._controller_owner,
                workspace=self._workspace,
                placer=self._spot_placer,
                replica_infos=all_replica_infos,
                priority=serve_constants.LB_REQUEST_PRIORITY_MIN):
            raise RuntimeError(
                f'Service {self._service_name!r} controller ownership changed '
                'while adopting paid-capacity claims.')
        with self._logical_state_lock:
            self._unknown_capacity_replacement_ids.update(
                info.replica_id
                for info in all_replica_infos
                if info.unknown_capacity_replacement is True)
        existing_replica_ids = [info.replica_id for info in all_replica_infos]
        intent_replica_id_high_water = 0
        if (self._reserved_fill_actuation_mode
                is zero_cost_actuation.ActuationMode.DURABLE_INTENT):
            # A committed durable intent retains its replica-ID association
            # after the replica row is cleaned.  Reusing that historical ID
            # would violate the intent ledger's uniqueness constraint and
            # prevent every later atomic handoff from committing.
            intent_replica_id_high_water = (self._zero_cost_actuation_repository
                                            .committed_replica_id_high_water(
                                                self._service_name))
        self._next_replica_id = max(
            [intent_replica_id_high_water, *existing_replica_ids]) + 1

        # There is a FIFO queue with capacity _MAX_NUM_LAUNCH for
        # _launch_replica.
        # We prioritize PROVISIONING replicas since they were previously
        # launched but may have been interrupted and need to be restarted.
        # This is why we handle PENDING replicas only after PROVISIONING
        # replicas.
        # Filter the snapshot in-memory rather than re-querying per status:
        # each `get_replicas_at_status` re-read and re-unpickled the whole
        # replica table, and separate reads could also diverge from the
        # snapshot the allocator seed and `existing_replica_infos` use.
        to_up_replicas = [
            info for info in all_replica_infos
            if info.status == serve_state.ReplicaStatus.PROVISIONING
        ]
        to_up_replicas.extend(
            info for info in all_replica_infos
            if info.status == serve_state.ReplicaStatus.PENDING)

        # Validate the complete cleanup-authority shape for every row before
        # deciding whether it is a fill launch. In particular, a false or
        # malformed marker carrying v2 fields must never fall through to the
        # ordinary launch re-drive path.
        try:
            for info in to_up_replicas:
                reserved_capacity.parse_protocol_v2_cleanup_fence(info)
        except exceptions.KubernetesPhysicalClusterIdentityError as error:
            raise RuntimeError(
                'Could not validate interrupted reserved-fill identity; '
                'retaining every durable row for recovery retry.') from error

        # A fill launch is opportunistic and its original broker authority is
        # intentionally one-shot.  Re-driving its sentinel-stripped row would
        # bypass the current protocol/generation/UID fences and could mutate a
        # Kubernetes context that was retargeted while this controller was
        # down.  Tear every interrupted fill down before considering the
        # ordinary recovery wave; a fresh broker round can refill the slot.
        binding_authority = self._ordinary_launch_binding_authority
        generic_binding_active = bool(
            not self._is_pool and binding_authority is not None and
            binding_authority.generic_launches_required)
        generic_settlement_active = bool(
            not self._is_pool and binding_authority is not None and
            binding_authority.retained_non_pool_settlement_allowed)
        interrupted_fill_replicas = ([] if generic_binding_active else [
            info for info in to_up_replicas if info.reserved_fill is True
        ])
        interrupted_fill_replicas.sort(key=_provider_cleanup_phase_order)
        if interrupted_fill_replicas:
            # The old controller may have submitted sdk.launch before it died
            # without receiving the durable request ID or before the cluster
            # table row appeared.  This recovery pass holds the new manager's
            # lock, so no current producer can enqueue these replicas. Cancel
            # and prove execution-level quiescence for the whole wave in one
            # API barrier. On uncertainty, retain every row and retry the
            # recovery pass instead of creating an unowned cluster.
            legacy_fill_replicas = []
            protocol_v2_fill_replicas = []
            resource_scope = self._resource_scope
            service_hash = self._service_hash
            try:
                for info in interrupted_fill_replicas:
                    protocol = _interrupted_reserved_fill_protocol(info)
                    if protocol == reserved_capacity_broker.PROTOCOL_V1:
                        legacy_fill_replicas.append(info)
                        continue
                    if (not isinstance(resource_scope, str) or
                            not resource_scope or
                            resource_scope != service_hash):
                        raise ValueError(
                            'protocol-v2 row has no service incarnation scope')
                    expected_cluster_name = (
                        serve_utils.generate_replica_cluster_name(
                            self._service_name, info.replica_id,
                            resource_scope))
                    if info.cluster_name != expected_cluster_name:
                        raise ValueError(
                            'protocol-v2 row has a non-incarnation-scoped '
                            'cluster name')
                    protocol_v2_fill_replicas.append(info)
            except ValueError as e:
                raise RuntimeError(
                    'Could not validate interrupted reserved-fill identity; '
                    'retaining every durable row for recovery retry.') from e

            barrier_partitions = ((protocol_v2_fill_replicas, True),
                                  (legacy_fill_replicas, False))
            for replicas, include_terminal_history in barrier_partitions:
                if not replicas:
                    continue
                if not serve_utils.quiesce_service_replica_launch_requests(
                        self._service_name,
                        replicas,
                        continue_guard=self._service_is_launch_authorized,
                        include_terminal_history=include_terminal_history):
                    raise RuntimeError(
                        'Could not quiesce interrupted reserved-fill launches; '
                        'retaining their durable rows for recovery retry.')
        for info in interrupted_fill_replicas:
            logger.warning(
                f'Replica {info.replica_id} is an interrupted reserved-fill '
                'launch; scheduling immediate teardown instead of recovery '
                're-drive.')
            self._terminate_replica(info.replica_id,
                                    replica_drain_delay_seconds=0,
                                    is_scale_down=True,
                                    in_flight_drain_cap_seconds=0)
        if interrupted_fill_replicas:
            interrupted_fill_ids = {
                info.replica_id for info in interrupted_fill_replicas
            }
            to_up_replicas = [
                info for info in to_up_replicas
                if info.replica_id not in interrupted_fill_ids
            ]

        recovery_versions = sorted({
            info.version
            for info in to_up_replicas
            if info.version != self.latest_version
        })
        recovery_yaml_contents = serve_state.get_yaml_contents(
            self._service_name, recovery_versions)

        bound_recovery_errors: list[tuple[int, Exception]] = []
        for replica_info in to_up_replicas:
            generic_reduction = None
            if generic_settlement_active:
                try:
                    generic_reduction = (
                        request_postgres.inspect_bound_ordinary_launch(
                            self._service_name, replica_info.replica_id,
                            replica_info.replica_record_id))
                    if generic_reduction is None:
                        # Generic promotion has a zero-pending barrier, so this
                        # retained row was committed after cutover. Without an
                        # association no API request became visible and no
                        # provider effect could escape. Retire the planner
                        # intent atomically instead of reconstructing it with a
                        # partial set of profile fields; current planners will
                        # make a fresh decision.
                        authority = self._ordinary_launch_binding_authority
                        assert authority is not None
                        retirement = (
                            ordinary_launch_binding.
                            retire_pre_admission_non_pool_launch_intent(
                                authority, replica_info.replica_id,
                                replica_info.replica_record_id))
                        if retirement.disposition in (
                                ordinary_launch_binding.
                                PreAdmissionRetirementDisposition.RETIRED,
                                ordinary_launch_binding.
                                PreAdmissionRetirementDisposition.ABSENT):
                            profile = (None if retirement.profile_kind is None
                                       else retirement.profile_kind.value)
                            logger.info(
                                'Retired pre-admission generic launch intent '
                                'for replica %s after controller restart '
                                '(profile=%s, disposition=%s).',
                                replica_info.replica_id, profile,
                                retirement.disposition.value)
                            continue
                        # Admission won the row-lock race. Re-read its exact
                        # request/association snapshot and enter normal
                        # adoption.
                        generic_reduction = (
                            request_postgres.inspect_bound_ordinary_launch(
                                self._service_name, replica_info.replica_id,
                                replica_info.replica_record_id))
                        if generic_reduction is None:
                            raise ordinary_launch_binding.OrdinaryLaunchBindingConflict(
                                'Generic admission became associated without '
                                'an adoptable request snapshot.')
                except Exception as e:  # pylint: disable=broad-except
                    logger.error(
                        'Failed to recover generic launch identity for replica '
                        f'{replica_info.replica_id}: '
                        f'{common_utils.format_exception(e)}')
                    bound_recovery_errors.append((replica_info.replica_id, e))
                    continue
            pending_version = self._pending_version
            if (pending_version is not None and
                    pending_version > replica_info.version):
                authority = self._ordinary_launch_binding_authority
                bound_reduction = generic_reduction
                if (bound_reduction is None and authority is not None and
                        authority.capable is True and authority.binding_mode
                        == ordinary_launch_binding.BindingMode.BOUND):
                    bound_reduction = (
                        request_postgres.inspect_bound_ordinary_launch(
                            self._service_name, replica_info.replica_id,
                            replica_info.replica_record_id))
                if bound_reduction is not None:
                    logger.info(
                        'Cancelling exact bound launch for replica %s at '
                        'version %s because version %s is waiting to be '
                        'applied.', replica_info.replica_id,
                        replica_info.version, pending_version)
                    self._terminate_replica(replica_info.replica_id,
                                            replica_drain_delay_seconds=0,
                                            is_scale_down=True,
                                            in_flight_drain_cap_seconds=0)
                else:
                    logger.info(
                        'Deferring legacy pointerless recovery re-drive for '
                        'replica '
                        '%s at version %s because version %s is waiting to be '
                        'applied.', replica_info.replica_id,
                        replica_info.version, pending_version)
                continue
            if replica_info.version != self.latest_version:
                logger.info(
                    'Retiring recovered replica %s at superseded version %s; '
                    'the current manager version is %s.',
                    replica_info.replica_id, replica_info.version,
                    self.latest_version)
                # Generic pointerless pre-admission rows were retired above.
                # For remaining legacy rows, _terminate_replica first settles
                # any exact bound pointer and otherwise enters legacy cleanup.
                self._terminate_replica(replica_info.replica_id,
                                        replica_drain_delay_seconds=0,
                                        is_scale_down=True,
                                        in_flight_drain_cap_seconds=0)
                continue
            if (not generic_binding_active and
                    replica_info.system_recovery_quarantine is not None):
                logger.warning(
                    f'Replica {replica_info.replica_id} has quarantined '
                    'system-recovery state; scheduling legacy teardown.')
                self._terminate_replica(replica_info.replica_id,
                                        replica_drain_delay_seconds=0)
                continue
            disposition = replica_info.system_recovery_disposition
            if (not generic_binding_active and disposition
                    == system_recovery_state.SystemRecoveryDisposition.CAPABLE):
                # Exact job capture proves the original launch completed. A
                # controller crash before the ordinary launch-status write
                # must not submit another request for the same generation.
                replica_info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.SUCCEEDED)
                self._persist_replica(replica_info.replica_id, replica_info)
                continue
            if (not generic_binding_active and disposition ==
                    system_recovery_state.SystemRecoveryDisposition.CANDIDATE):
                intent = replica_info.system_recovery_launch_intent
                assert intent is not None
                request_id = replica_info.launch_request_id
                service_job_id = replica_info.service_job_id
                if service_job_id is not None:
                    # The callback committed the exact result before the old
                    # controller died; only the ordinary status write remains.
                    replica_info.status_property.sky_launch_status = (
                        common_utils.ProcessStatus.SUCCEEDED)
                    self._persist_replica(replica_info.replica_id, replica_info)
                    continue
                if request_id is not None:
                    if (replica_info.replica_id
                            in legacy_runtime.launch_thread_pool):
                        # A recovery-pass retry must retain the already-started
                        # exact-request adopter; replacing it would orphan the
                        # only worker that can capture this generation's job.
                        continue
                    log_file_name = (
                        serve_utils.generate_replica_launch_log_file_name(
                            self._service_name, replica_info.replica_id,
                            self._resource_scope))
                    teardown_requested = threading.Event()
                    launch_thread = _ReplicaLaunchThread(
                        target=adopt_system_recovery_launch,
                        replica_id=replica_info.replica_id,
                        replica_record_id=replica_info.replica_record_id,
                        service_hash=self._service_hash,
                        controller_owner=self._controller_owner,
                        teardown_requested=teardown_requested,
                        completion_queue=(
                            legacy_runtime.launch_completion_queue),
                        completion_event=(
                            legacy_runtime.launch_completion_event),
                        args=(
                            replica_info.replica_id,
                            replica_info.cluster_name,
                            log_file_name,
                            request_id,
                            functools.partial(
                                self._persist_system_recovery_job_id,
                                replica_info.replica_id, intent),
                            teardown_requested,
                        ))
                    # The cloud request was already admitted before the old
                    # controller died, so this waiter consumes no new launch
                    # budget.  Restore its exact request association before
                    # starting it: teardown can then cancel sdk.get() instead
                    # of joining an untracked retry-until-up request forever.
                    legacy_runtime.replica_to_request_id[
                        replica_info.replica_id] = request_id
                    legacy_runtime.launch_thread_pool[
                        replica_info.replica_id] = launch_thread
                    try:
                        launch_thread.start()
                    except Exception:
                        if (legacy_runtime.launch_thread_pool.get(
                                replica_info.replica_id) is launch_thread):
                            legacy_runtime.launch_thread_pool.pop(
                                replica_info.replica_id)
                            legacy_runtime.replica_to_request_id.pop(
                                replica_info.replica_id)
                        raise
                    continue
                # The endpoint never consumed the nonce, so no request could
                # have been scheduled. Demote first, then let the established
                # recovery re-drive issue one ordinary request.
                if not self._demote_system_recovery_candidate(
                        replica_info.replica_id, intent):
                    self._terminate_replica(replica_info.replica_id,
                                            replica_drain_delay_seconds=0)
                    continue
                refreshed = serve_state.get_replica_info_from_id(
                    self._service_name, replica_info.replica_id)
                if refreshed is None:
                    continue
                replica_info = refreshed
            # It should be robust enough for `execution.launch` to handle cases
            # where the provisioning is partially done.
            # So we mock the original request based on all call sites,
            # including SkyServeController._run_autoscaler.
            # One shared snapshot: per-launch fresh scans made recovery
            # O(pending x total rows) — tens of minutes at fleet scale.
            # Per-replica isolation: one bad row must not strand the rest
            # un-redriven.
            authority = self._ordinary_launch_binding_authority
            bound_profile_recovery = bool(
                not self._is_pool and authority is not None and
                authority.capable is True and authority.binding_mode
                == ordinary_launch_binding.BindingMode.BOUND and
                (authority.retained_non_pool_settlement_allowed or
                 ordinary_launch_binding.replica_has_narrow_ordinary_profile(
                     replica_info)))
            try:
                prior_planned_capacity = replica_info.planned_capacity
                if (isinstance(prior_planned_capacity, bool) or
                        not isinstance(prior_planned_capacity, int) or
                        prior_planned_capacity < 1):
                    prior_planned_capacity = 1
                if replica_info.version == self.latest_version:
                    prior_yaml_content = self.yaml_content
                else:
                    recovered_yaml_content = recovery_yaml_contents.get(
                        replica_info.version)
                    if recovered_yaml_content is None:
                        raise ValueError(
                            'yaml content not found for recovery of '
                            f'{self._service_name} version '
                            f'{replica_info.version}')
                    prior_yaml_content = recovered_yaml_content
                authority = self._ordinary_launch_binding_authority
                if (authority is not None and authority.capable is True and
                        authority.binding_mode
                        == ordinary_launch_binding.BindingMode.BOUND):
                    bound_reduction = generic_reduction
                    if bound_reduction is None:
                        bound_reduction = (
                            request_postgres.inspect_bound_ordinary_launch(
                                self._service_name, replica_info.replica_id,
                                replica_info.replica_record_id))
                    if bound_reduction is not None:
                        if (replica_info.replica_id
                                in legacy_runtime.launch_thread_pool):
                            continue
                        recovery_spec = self._version_specs.get(
                            replica_info.version)
                        if recovery_spec is None:
                            recovery_spec = serve_state.get_spec(
                                self._service_name, replica_info.version)
                        self._install_bound_launch_adopter(
                            replica_info,
                            bound_reduction.context,
                            start=True,
                            yaml_content=prior_yaml_content,
                            spec=recovery_spec)
                        continue
                # A retained pointerless row cannot prove whether an older
                # controller crossed provider I/O before it died.  Never
                # rewrite RUNNING to SCHEDULED or submit a fresh effect from
                # that ambiguity.  Exact bound-request/system-recovery paths
                # above may observe and settle their immutable generation;
                # legacy rows remain counted/off-route for evidence-backed
                # cleanup or clean service recreation.
                logger.warning(
                    'Retaining legacy pointerless launch for replica %s '
                    'without recovery re-drive; no exact executable request '
                    'authority is available.', replica_info.replica_id)
                continue
            except Exception as e:  # pylint: disable=broad-except
                logger.error('Failed to re-drive launch of replica '
                             f'{replica_info.replica_id}: '
                             f'{common_utils.format_exception(e)}')
                if bound_profile_recovery:
                    # A bound row has no safe inference fallback.  Preserve
                    # per-row isolation for this wave, but fail the recovery
                    # pass after teardown reconstruction so the supervised
                    # loop retries exact inspection/adoption in this same
                    # live controller.  Otherwise a transient read, spec
                    # reconstruction, or Thread.start failure leaves no local
                    # owner and the normal refresher can never discover it.
                    bound_recovery_errors.append((replica_info.replica_id, e))

        # A forced status refresh can remove an interrupted cluster from global
        # state before the replica row records the interruption. If the
        # controller crashes between those writes, the next prober cannot
        # classify the replica because its handle is already gone. Detect that
        # earliest crash window in one bulk lookup. Only active, successfully
        # launched interruptible rows qualify; pending launches and retained
        # failure records can legitimately lack a handle and keep their
        # existing recovery semantics.
        active_statuses = {
            serve_state.ReplicaStatus.STARTING,
            serve_state.ReplicaStatus.READY,
            serve_state.ReplicaStatus.NOT_READY,
        }
        active_interruptible_replicas = [
            info for info in all_replica_infos
            if (self._is_interruptible_replica(info) and not info.
                status_property.preempted and info.status in active_statuses)
        ]
        active_interruptible_cluster_names = [
            info.cluster_name for info in active_interruptible_replicas
        ]
        active_interruptible_status_fields = (
            global_user_state.get_cluster_status_fields(
                active_interruptible_cluster_names))
        orphaned_interruptible_clusters = {
            info.cluster_name
            for info in active_interruptible_replicas
            if info.cluster_name not in active_interruptible_status_fields
        }

        # Inspect the raw durable rows instead of querying only the derived
        # SHUTTING_DOWN status.  A preemption is persisted before teardown is
        # scheduled; a controller crash in that window leaves
        # ``preempted=True`` with no ``sky_down_status``, so the derived status
        # is not PREEMPTED or SHUTTING_DOWN yet.  Once teardown is scheduled,
        # PREEMPTED also deliberately wins status derivation.  Both shapes
        # must be re-driven or the cluster cleanup and replica row are
        # stranded indefinitely.
        to_down_replicas = [
            info for info in all_replica_infos
            if (info.status == serve_state.ReplicaStatus.SHUTTING_DOWN or
                info.status_property.preempted or
                info.cluster_name in orphaned_interruptible_clusters)
        ]
        to_down_replicas.sort(key=_provider_cleanup_phase_order)
        waiting_replicas = [
            info for info in to_down_replicas
            if (not self._is_legacy_uncertain_logical_retirement(info) and
                (self._is_restart_recoverable_logical_retirement(info) or
                 info.status_property.wait_for_idle_before_termination is True))
        ]
        recovery_wait_urls: dict[int, str | None] = {}
        try:
            recovered_paid_retirements = paid_retirement.list_for_service(
                self._service_name)
            paid_retirement_read_failed = False
        except Exception as error:  # pylint: disable=broad-except
            logger.warning('Unable to read paid-retirement state during '
                           'recovery; exact-idle rows remain blocked: '
                           f'{common_utils.format_exception(error)}')
            recovered_paid_retirements = {}
            paid_retirement_read_failed = True
        if waiting_replicas and not self._is_pool:
            for info in waiting_replicas:
                record = recovered_paid_retirements.get(info.replica_id)
                matching_paid_retirement = bool(
                    record is not None and str(record['replica_record_id'])
                    == info.replica_record_id and record['state'] in {
                        paid_retirement.PaidRetirementState.ACTIVE.value,
                        paid_retirement.PaidRetirementState.COMMITTED.value,
                    })
                if (matching_paid_retirement or
                    (paid_retirement_read_failed and
                     info.status_property.wait_for_idle_before_termination
                     is True and
                     info.status_property.drain_cap_seconds is None)):
                    recovery_wait_urls[info.replica_id] = (
                        None if record is None else record.get('route_url'))
                else:
                    # Endpoint evidence is an early-drain optimization. The
                    # lock-free refresher resolves every unresolved exact row.
                    recovery_wait_urls[info.replica_id] = None

        legacy_uncertain_ids = self._legacy_uncertain_logical_retirement_ids
        recovering_logical_ids = self._recovering_logical_retirement_ids
        for replica_info in to_down_replicas:
            try:
                paid_record = recovered_paid_retirements.get(
                    replica_info.replica_id)
                if (paid_record is not None and
                        str(paid_record['replica_record_id'])
                        == replica_info.replica_record_id and
                        paid_record['state']
                        == paid_retirement.PaidRetirementState.COMMITTED.value):
                    # Destructive authority was already committed under the
                    # exact demand/route/plan transaction. Recovery must not
                    # reinterpret the intentionally absent drain cap as the
                    # legacy bounded fallback.
                    self._terminate_replica(replica_info.replica_id,
                                            replica_drain_delay_seconds=0,
                                            is_scale_down=True,
                                            in_flight_drain_cap_seconds=0)
                    continue
                if self._is_legacy_uncertain_logical_retirement(replica_info):
                    logger.warning(
                        f'Keeping legacy logical retirement for replica '
                        f'{replica_info.replica_id} off-route until fresh '
                        'replacement capacity is confirmed.')
                    legacy_uncertain_ids.add(replica_info.replica_id)
                    continue
                if self._is_restart_recoverable_logical_retirement(
                        replica_info):
                    # Register both strict idle waits and bounded precommit
                    # drains before rebuilding any teardown worker. The latter
                    # already consumed their idle deadline, but the persisted
                    # wall-clock deadline lets the tracker resume at zero and
                    # fresh recovery evidence remains the admission authority.
                    replica_url = recovery_wait_urls.get(
                        replica_info.replica_id, _REPLICA_URL_NOT_PROVIDED)
                    self._register_wait_for_idle(replica_info,
                                                 replica_url=replica_url)
                    if (replica_info.status_property.
                            logical_retirement_controller_epoch
                            != self._logical_controller_epoch):
                        recovering_logical_ids.add(replica_info.replica_id)
                    continue
                if (replica_info.status_property.
                        wait_for_idle_before_termination is True):
                    replica_url = recovery_wait_urls.get(
                        replica_info.replica_id, _REPLICA_URL_NOT_PROVIDED)
                    self._register_wait_for_idle(replica_info,
                                                 replica_url=replica_url)
                    continue
                # A scale-down retirement interrupted by a controller restart
                # re-enters the remaining bounded drain. The cap and wall-clock
                # start are persisted with the off-route row, so recovery never
                # kills early and repeated restarts cannot restart the cost
                # window. Purged and failure teardowns keep the immediate
                # re-drive. Preempted replicas also re-drive immediately: their
                # cloud instance is already gone (or partially gone), and the
                # persisted preempted bit is itself sufficient to classify this
                # as scale-down cleanup even if the crash preceded the
                # is_scale_down write.
                drain_cap: int | None = None
                status_property = replica_info.status_property
                if replica_info.cluster_name in orphaned_interruptible_clusters:
                    logger.warning(
                        f'Recovering interrupted replica '
                        f'{replica_info.replica_id}: cluster '
                        f'{replica_info.cluster_name!r} was removed before '
                        'the interruption intent was persisted.')
                    status_property.preempted = True
                    # Persist the recovered intent before scheduling cleanup;
                    # another crash is then caught by the raw preempted scan.
                    self._persist_replica(replica_info.replica_id, replica_info)
                is_preempted = status_property.preempted
                # SpotPlacer is reconstructed with every location ACTIVE on a
                # controller restart. Rebuild only real spot capacity benches.
                # A reclaimed research pod proves that pod was evicted, not
                # that the zero-cost pool is unavailable.
                if (is_preempted and replica_info.is_spot and
                        self._spot_placer is not None):
                    spot_location = replica_info.get_spot_location()
                    if spot_location is not None:
                        self._spot_placer.set_preemptive(spot_location)
                is_scale_down = status_property.is_scale_down or is_preempted
                if (is_scale_down and not status_property.purged and
                        not is_preempted):
                    # Prefer the cap persisted when the retirement was
                    # scheduled (exact reuse across the restart); legacy
                    # rows predating the field re-resolve after record-boundary
                    # normalization supplies the explicit None default.
                    drain_cap = status_property.drain_cap_seconds
                    if drain_cap is None:
                        drain_cap = self._resolve_drain_cap_seconds(
                            replica_info.replica_id, replica_info)
                self._terminate_replica(replica_info.replica_id,
                                        replica_drain_delay_seconds=0,
                                        purge=status_property.purged,
                                        is_scale_down=is_scale_down,
                                        in_flight_drain_cap_seconds=drain_cap)
            except Exception as e:  # pylint: disable=broad-except
                logger.error('Failed to re-drive termination of replica '
                             f'{replica_info.replica_id}: '
                             f'{common_utils.format_exception(e)}')
        if (recovering_logical_ids and
                self._logical_retirement_recovery_deadline is None):
            self._logical_retirement_recovery_deadline = (
                time.monotonic() + _LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS)
            logger.info(
                f'Recovered {len(recovering_logical_ids)} uncommitted logical '
                'retirements; keeping them off-route until current capacity '
                'is revalidated.')
        if bound_recovery_errors:
            failed_ids = [replica_id for replica_id, _ in bound_recovery_errors]
            raise RuntimeError(
                'Exact bound non-pool launch recovery remains incomplete for '
                f'replicas {failed_ids!r}; retrying the recovery pass.') from (
                    bound_recovery_errors[0][1])

    ################################
    # Replica management functions #
    ################################

    def _task_template_for_version(
        self,
        version: int,
        yaml_content: str,
        spec: 'service_spec.SkyServiceSpec',
    ) -> task_lib.Task:
        """Return the immutable parsed task template for one version.

        The caller holds ``self.lock``. Service-version YAML and specs are
        immutable, so recovery can safely populate an old version lazily and
        every executable task can deepcopy this shared parse result.
        """
        task = self._version_task_templates.get(version)
        if task is None:
            task = load_task_with_service_spec(yaml_content, spec)
        self._cache_task_template(version, task)
        return task

    def _max_live_paid_gpu_units_for_version(self, version: int) -> int | None:
        """Return the immutable paid cap for one committed service version."""
        spec = self._version_specs.get(version)
        if spec is None:
            spec = serve_state.get_spec(self._service_name, version)
            if spec is None:
                raise ValueError(f'Version {version} not found.')
            self._version_specs[version] = spec
        return spec.max_live_paid_gpu_units

    @property
    def max_live_paid_gpu_units(self) -> int | None:
        """Return the paid cap for the manager's active service version."""
        return self._max_live_paid_gpu_units_for_version(self.latest_version)

    def _cache_task_template(self, version: int, task: task_lib.Task) -> None:
        """Retain a bounded current-plus-recovery Task parse cache."""
        self._version_task_templates.pop(version, None)
        self._version_task_templates[version] = task
        while (len(self._version_task_templates)
               > _SERVICE_VERSION_TASK_TEMPLATE_CACHE_SIZE):
            for cached_version in tuple(self._version_task_templates):
                if cached_version != self.latest_version:
                    self._version_task_templates.pop(cached_version)
                    break

    # We don't need to add lock here since every caller of this function
    # will acquire the lock.
    def _launch_replica(
        self,
        replica_id: int,
        resources_override: dict[str, Any] | None = None,
        existing_replica_infos: list['ReplicaInfo'] | None = None,
        prior_is_zero_cost: bool = False,
        prior_cost_rebalance_for_replica_id: int | None = None,
        prior_paid_capacity_pool_key: str | None = None,
        prior_replica_record_id: str | None = None,
        prior_created_at: float | None = None,
        prior_planned_capacity: int | None = None,
        prior_unknown_capacity_replacement: bool = False,
        unknown_capacity_replacement_authorization: dict[str, Any] |
        None = None,
        prior_version: int | None = None,
        prior_yaml_content: str | None = None,
        zero_cost_demand_budget: _ZeroCostDemandBudget | None = None,
        paid_location_launch_budget: paid_capacity.LaunchBudget | None = None,
        paid_launch_authority: capacity_admission.PaidLaunchAuthority |
        None = None,
        paid_launch_allowed: bool = True,
        launch_priority: int = serve_constants.LB_REQUEST_PRIORITY_MIN,
        recovering_existing_replica: bool = False,
        logical_reconcile_fence: LogicalTargetState | None = None,
        logical_reconcile_fence_requires_exact_generation: bool = False,
        provider_phase_admission: (provider_phase.ProviderPhaseAdmission |
                                   None) = None,
        try_provider_phase_admission: bool = False,
        require_preinitialized_physical_fence: bool = False,
        zero_cost_actuation_lease: (zero_cost_actuation.IntentLease |
                                    None) = None,
        prepared_paid_launches: list[_PreparedPaidLaunch] | None = None,
    ) -> _ReplicaLaunchResult | None:
        """Enqueue one replica launch.

        Returns immutable accounting facts after a launch is durably accepted
        or staged for its caller-owned atomic paid wave, and None when no
        launch is accepted. A zero-cost-only fill launch is skipped when no
        zero-cost location is ACTIVE, and the skip must leak nothing -- no
        replica row, no launch thread.

        prior_is_zero_cost: placement-cost provenance of a recovery row. The
        recovered exact pin remains on the same capacity, so preserve it even
        if the current placer snapshot is temporarily unavailable.

        prior_paid_capacity_pool_key: exact global claim retained by a
        recovery-pinned unresolved paid row. Recovery does not acquire a new
        claim, but its ordinary replica upsert must not erase the adopted one.

        prior_replica_record_id: immutable database-record identity retained
        by a recovery re-drive. Numeric replica IDs can be reused after a
        terminal delete, so an update must carry this exact fence.

        prior_created_at: original durable row creation timestamp retained by
        a recovery re-drive. It is also an input to the deterministic identity
        used by v12 transition rows.

        recovering_existing_replica: the replica already has a durable row
        and cluster identity. Reuse an exact persisted placement instead of
        asking the spot placer to select a new location.

        prior_planned_capacity: immutable logical width of a recovery row.

        prior_unknown_capacity_replacement: preserve the bounded degradation
        attribution of a recovery row.

        prior_version: immutable service version of a recovery row.

        prior_yaml_content: exact launch YAML of a recovery row. Recovery must
        not relabel or relaunch an old-version backend with the latest config.

        logical_reconcile_fence_requires_exact_generation: keep bounded
        unknown-capacity replacement tied to the exact outage observation.

        paid_location_launch_budget: advisory shared allowance for fresh paid
        placement. Recovery bypasses new admission; fresh cost-rebalance
        replacements bypass selection but still require exact-pool admission.

        paid_launch_allowed: fail-closed acquisition fence for a demand-owned
        exact-card target. False still permits an ACTIVE zero-cost placement,
        but it must never fall through to a paid location and does not relabel
        the resulting demand row as reserved fill.

        launch_priority: highest queued demand priority represented by this
        fresh launch. It gates new global claims only and never preempts an
        existing launch.

        provider_phase_admission: blocking v2 admission acquired by the public
        single-scale entrypoint before it takes ``self.lock``.

        try_provider_phase_admission: batch-only mode that takes a zero-wait
        v2 admission at the exact physical-capture/persist seam. Contention
        defers this one launch without writing a row or registering a thread.

        require_preinitialized_physical_fence: join a physical-identity
        capture prepared outside the manager lock and never initialize one at
        the persistence seam.

        prepared_paid_launches: optional wave-owned staging sink for fresh
        globally managed paid launches. Eligible launches freeze their exact
        replica, location, and worker here without writing or registering the
        worker. The caller admits the complete sink in one PostgreSQL
        transaction and publishes only its committed members.
        """
        if self._update_recovery_required:
            logger.info(
                'Refusing to enqueue replica %s because the '
                'controller update requires supervised recovery.', replica_id)
            return None
        protocol_v2_fill = _is_protocol_v2_fill_override(resources_override)
        if protocol_v2_fill:
            mode = self._reserved_fill_actuation_mode
            if (mode is not zero_cost_actuation.ActuationMode.DURABLE_INTENT or
                    zero_cost_actuation_lease is None):
                self._log_fill_skip(
                    'protocol-v2 fill requires durable atomic admission')
                return None
        elif zero_cost_actuation_lease is not None:
            raise ValueError('A zero-cost actuation lease requires a v2 fill.')
        if try_provider_phase_admission and (
                not protocol_v2_fill or provider_phase_admission is not None):
            raise exceptions.ProviderPhaseMisuseError(
                'Try-only provider admission requires one protocol-v2 fill '
                'without an existing admission.')
        if require_preinitialized_physical_fence and (
                not protocol_v2_fill or try_provider_phase_admission or
                provider_phase_admission is None):
            raise exceptions.ProviderPhaseMisuseError(
                'A preinitialized physical fence requires one protocol-v2 '
                'fill with an existing provider admission.')
        if (protocol_v2_fill and not try_provider_phase_admission and
            (provider_phase_admission is None or provider_phase_admission.mode
             != provider_phase.ProviderPhaseMode.V2_FENCED)):
            raise exceptions.ProviderPhaseMisuseError(
                'A protocol-v2 fill launch requires fenced admission.')
        legacy_runtime = self._legacy_mutation_runtime_state()
        if replica_id in legacy_runtime.launch_thread_pool:
            logger.warning(f'Launch thread for replica {replica_id} '
                           'already exists. Skipping.')
            return None
        # [boltz fork] Reserved-capacity fill scale-ups carry a sentinel
        # override key restricting the launch to zero-cost locations (plus,
        # under the broker, the grant epoch the decision was emitted
        # under). Pop them on a COPY: the caller may reuse the dict, and
        # the popped copy is what gets persisted on the ReplicaInfo row.
        # Interrupted fill rows are torn down rather than recovery re-driven;
        # only a new broker decision may enter this selection path again.
        zero_cost_only = False
        fill_grant_epoch: int | None = None
        fill_pool_key: str | None = None
        fill_protocol_version: int | None = None
        fill_service_generation: int | None = None
        fill_service_version: int | None = None
        fill_physical_cluster_uid: str | None = None
        fill_allowed_location_keys: list[dict[str, Any]] | None = None
        fill_allocation_generation: int | None = None
        fill_allocation_input_sha256: str | None = None
        fill_allocation_claim_generation: int | None = None
        fill_gate_generation: int | None = None
        fill_reclaim_fleet_bundle_sha256: str | None = None
        fill_reclaim_policy_revision: str | None = None
        fill_reclaim_provider_inventory_sha256: str | None = None
        fill_worker_projection_sha256: str | None = None
        fill_observation_generation: int | None = None
        fill_observation_sequence: int | None = None
        fill_ordinary_admission_sequence: int | None = None
        fill_intent_idempotency_key: str | None = None
        fill_pool_identity: reserved_capacity_broker.PoolIdentity | None = None
        fill_exact_accelerator_shape: tuple[str, int] | None = None
        fill_launch_context: str | None = None
        fill_launch_accelerator_shape: tuple[str, int] | None = None
        fill_cloud_launch_guard: (Callable[[], bool | tuple[bool, str]] |
                                  None) = None
        cost_rebalance_for_replica_id = (prior_cost_rebalance_for_replica_id)
        non_pool_launch_authorization = (
            unknown_capacity_replacement_authorization)
        if (resources_override is not None and
                serve_constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY
                in resources_override):
            resources_override = dict(resources_override)
            resources_override.pop(
                serve_constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY)
            fill_grant_epoch = resources_override.pop(
                serve_constants.RESERVED_FILL_GRANT_EPOCH_OVERRIDE_KEY, None)
            fill_pool_key = resources_override.pop(
                serve_constants.RESERVED_FILL_POOL_KEY_OVERRIDE_KEY, None)
            fill_protocol_version = resources_override.pop(
                serve_constants.RESERVED_FILL_PROTOCOL_VERSION_OVERRIDE_KEY,
                None)
            fill_service_generation = resources_override.pop(
                serve_constants.RESERVED_FILL_SERVICE_GENERATION_OVERRIDE_KEY,
                None)
            fill_service_version = resources_override.pop(
                serve_constants.RESERVED_FILL_SERVICE_VERSION_OVERRIDE_KEY,
                None)
            fill_physical_cluster_uid = resources_override.pop(
                serve_constants.RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY,
                None)
            raw_allowed_locations = resources_override.pop(
                serve_constants.RESERVED_FILL_ALLOWED_LOCATIONS_OVERRIDE_KEY,
                None)
            fill_allocation_generation = resources_override.pop(
                serve_constants.
                RESERVED_FILL_ALLOCATION_GENERATION_OVERRIDE_KEY, None)
            fill_allocation_input_sha256 = resources_override.pop(
                serve_constants.
                RESERVED_FILL_ALLOCATION_INPUT_SHA256_OVERRIDE_KEY, None)
            fill_allocation_claim_generation = resources_override.pop(
                serve_constants.
                RESERVED_FILL_ALLOCATION_CLAIM_GENERATION_OVERRIDE_KEY, None)
            fill_gate_generation = resources_override.pop(
                serve_constants.RESERVED_FILL_GATE_GENERATION_OVERRIDE_KEY,
                None)
            fill_reclaim_fleet_bundle_sha256 = resources_override.pop(
                serve_constants.
                RESERVED_FILL_RECLAIM_FLEET_BUNDLE_SHA256_OVERRIDE_KEY, None)
            fill_reclaim_policy_revision = resources_override.pop(
                serve_constants.
                RESERVED_FILL_RECLAIM_POLICY_REVISION_OVERRIDE_KEY, None)
            fill_reclaim_provider_inventory_sha256 = resources_override.pop(
                serve_constants.
                RESERVED_FILL_RECLAIM_PROVIDER_INVENTORY_SHA256_OVERRIDE_KEY,
                None)
            fill_worker_projection_sha256 = resources_override.pop(
                serve_constants.
                RESERVED_FILL_WORKER_PROJECTION_SHA256_OVERRIDE_KEY, None)
            fill_observation_generation = resources_override.pop(
                serve_constants.
                RESERVED_FILL_OBSERVATION_GENERATION_OVERRIDE_KEY, None)
            fill_observation_sequence = resources_override.pop(
                serve_constants.RESERVED_FILL_OBSERVATION_SEQUENCE_OVERRIDE_KEY,
                None)
            fill_ordinary_admission_sequence = resources_override.pop(
                serve_constants.
                RESERVED_FILL_ORDINARY_ADMISSION_SEQUENCE_OVERRIDE_KEY, None)
            fill_intent_idempotency_key = resources_override.pop(
                serve_constants.
                RESERVED_FILL_INTENT_IDEMPOTENCY_KEY_OVERRIDE_KEY, None)
            if raw_allowed_locations is not None:
                if not isinstance(raw_allowed_locations, list):
                    self._log_fill_skip('malformed pool location fence')
                    return None
                fill_allowed_location_keys = raw_allowed_locations
            zero_cost_only = True
        if (resources_override is not None and
                serve_constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY
                in resources_override):
            if non_pool_launch_authorization is not None:
                raise ValueError('One replica intent cannot be both an '
                                 'unknown-capacity and cost-rebalance action.')
            resources_override = dict(resources_override)
            cost_rebalance_for_replica_id = int(
                resources_override.pop(
                    serve_constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY))
        logger.info(f'Launching replica {replica_id}...')
        cluster_name = serve_utils.generate_replica_cluster_name(
            self._service_name, replica_id, self._resource_scope)
        log_file_name = serve_utils.generate_replica_launch_log_file_name(
            self._service_name, replica_id, self._resource_scope)
        if recovering_existing_replica:
            if prior_version is None or prior_yaml_content is None:
                raise ValueError('Recovery launch requires its persisted '
                                 'version and exact launch YAML.')
            launch_version = prior_version
            launch_yaml_content = prior_yaml_content
        else:
            launch_version = self.latest_version
            launch_yaml_content = self.yaml_content
        version_specs = self._version_specs
        launch_spec = version_specs.get(launch_version)
        if launch_spec is None:
            launch_spec = serve_state.get_spec(self._service_name,
                                               launch_version)
            if launch_spec is None:
                raise ValueError(f'Version {launch_version} not found.')
            version_specs[launch_version] = launch_spec
        launch_task_template = self._task_template_for_version(
            launch_version, launch_yaml_content, launch_spec)
        use_spot = _should_use_spot(launch_yaml_content, resources_override,
                                    launch_spec, launch_task_template)
        retry_until_up = True
        location = None
        placer_owns_kubernetes_fallback = (
            _placer_has_only_non_spot_kubernetes_gpu_locations(
                self._spot_placer))
        debit_paid_location_launch_budget = False
        recovered_location = None
        if recovering_existing_replica and self._spot_placer is not None:
            # A persisted row already owns a cluster name and may own live
            # infrastructure.  Its exact location is immutable during a
            # controller re-drive: choosing a new spot location would run the
            # same cluster name against different resources and overwrite the
            # only durable identity needed for cleanup.
            recovered_location = spot_placer.Location.from_resources_override(
                resources_override)
        if (zero_cost_only or
                not paid_launch_allowed) and self._spot_placer is None:
            # Defensive: fill decisions are only emitted while the capacity
            # poller runs, and an authority-limited demand launch also needs a
            # placer to prove that its selected location is free. Without one
            # there is no way to guarantee zero-cost placement -- skip rather
            # than risk a paid launch.
            if zero_cost_only:
                self._log_fill_skip('no spot placer available to pin a '
                                    'zero-cost-only fill launch')
            else:
                logger.info('Deferring demand launch without paid authority: '
                            'no placer can prove a zero-cost location.')
            return None
        # A fill launch must reach the placer even though zero-cost k8s
        # entries are use_spot=False (the _should_use_spot gate above keys
        # on the task/override spot-ness, which says nothing about fill).
        if cost_rebalance_for_replica_id is not None:
            if self._spot_placer is None:
                logger.warning('Skipping cost-rebalance launch: no spot '
                               'placer is available.')
                self._clean_up_skipped_cost_rebalance_redrive(
                    replica_id, prior_cost_rebalance_for_replica_id)
                return None
            pinned_location = spot_placer.Location.from_resources_override(
                resources_override)
            if pinned_location is None:
                logger.warning('Skipping cost-rebalance launch: candidate '
                               'location could not be reconstructed.')
                self._clean_up_skipped_cost_rebalance_redrive(
                    replica_id, prior_cost_rebalance_for_replica_id)
                return None
            location = self._spot_placer.resolve_location(pinned_location)
            if (location is None or
                    not self._spot_placer.is_active_location(location)):
                logger.info('Skipping cost-rebalance launch: candidate '
                            f'{pinned_location} is no longer active.')
                self._clean_up_skipped_cost_rebalance_redrive(
                    replica_id, prior_cost_rebalance_for_replica_id)
                return None
            if not recovering_existing_replica:
                if existing_replica_infos is None:
                    existing_replica_infos = serve_state.get_replica_infos(
                        self._service_name)
                predecessor_info = next(
                    (candidate for candidate in existing_replica_infos
                     if candidate.replica_id == cost_rebalance_for_replica_id
                     and candidate.version == launch_version and
                     not candidate.is_terminal), None)
                binding_authority = self._ordinary_launch_binding_authority
                if (binding_authority is not None and
                        binding_authority.generic_launches_required):
                    if predecessor_info is None:
                        raise _BoundOrdinaryLaunchUnresolvedError(
                            'Cost-rebalance planner lost its exact predecessor '
                            'before replica-intent admission.')
                    non_pool_launch_authorization = (
                        ordinary_launch_binding.
                        build_replacement_planner_authorization(
                            ordinary_launch_binding.NonPoolLaunchProfileKind.
                            COST_REBALANCE,
                            binding_authority,
                            predecessor_replica_id=(
                                predecessor_info.replica_id),
                            predecessor_record_id=(
                                predecessor_info.replica_record_id),
                            predecessor_service_version=(
                                predecessor_info.version)))
                if paid_location_launch_budget is None:
                    paid_location_launch_budget = (
                        paid_capacity.build_launch_budget(
                            self._spot_placer,
                            workspace=self._workspace,
                            existing_replica_infos=existing_replica_infos,
                            globally_managed=(self._service_hash is not None),
                            service_name=self._service_name,
                            service_hash=self._service_hash,
                            max_live_paid_gpu_units=(
                                launch_spec.max_live_paid_gpu_units),
                            requested_frontier_keys={
                                paid_capacity.frontier_key(location)
                            }))
                if location in (
                        paid_location_launch_budget.remaining_by_location):
                    if (paid_location_launch_budget.
                            remaining_by_location[location] <= 0 or
                            paid_capacity.service_exhausted(
                                paid_location_launch_budget)):
                        logger.info(
                            'Deferring cost-rebalance replacement because '
                            f'paid admission is closed at {location}.')
                        self._clean_up_skipped_cost_rebalance_redrive(
                            replica_id, prior_cost_rebalance_for_replica_id)
                        return None
                    debit_paid_location_launch_budget = True
                self._spot_placer.reserve_retry(location)
            resources_override = location.to_dict()
            use_spot = location.use_spot
            retry_until_up = False
        elif (self._spot_placer is not None and
              (use_spot or zero_cost_only or not paid_launch_allowed or
               placer_owns_kubernetes_fallback) and recovered_location is None):
            # For spot placer, we don't retry until up so any launch failed
            # due to availability issue will be handled by the placer.
            retry_until_up = False
            # TODO(tian): Currently, the resources_override can only be
            # `use_spot=True/False`, which will not cause any conflict with
            # spot placer's cloud, region & zone. When we add more resources
            # to the resources_override, we need to make sure they won't
            # conflict with the spot placer's selection.
            if resources_override is None:
                resources_override = {}
            else:
                # Location pinning below is persisted on ReplicaInfo, but the
                # autoscaler's batch decision is caller-owned and may be
                # reused by later entries in the same wave.
                resources_override = dict(resources_override)
            allowed_locations = self._locations_for_accelerator_override(
                resources_override)
            if fill_allowed_location_keys is not None:
                try:
                    carried_locations = [
                        location for location in (
                            spot_placer.Location.from_pickleable(key)
                            for key in fill_allowed_location_keys)
                        if location is not None
                    ]
                except (AssertionError, KeyError, TypeError, ValueError):
                    self._log_fill_skip('malformed pool location fence')
                    return None
                if len(carried_locations) != len(fill_allowed_location_keys):
                    self._log_fill_skip('malformed pool location fence')
                    return None
                active = set(self._spot_placer.active_locations())
                pool_allowed = {
                    candidate for candidate in active if any(
                        spot_placer.locations_match_placement(
                            candidate, carried)
                        for carried in carried_locations)
                }
                allowed_locations = (
                    pool_allowed if allowed_locations is None else
                    allowed_locations.intersection(pool_allowed))
            allowed_location_kwargs: dict[str, Any] = (
                {} if allowed_locations is None else {
                    'allowed_locations': allowed_locations
                })
            if (resources_override.get('accelerators') and
                    not allowed_locations):
                if zero_cost_only:
                    self._log_fill_skip(
                        'no pool location matches the carried exact '
                        f'accelerator shape '
                        f'{resources_override["accelerators"]!r}')
                    return None
                if not paid_launch_allowed:
                    logger.info(
                        'Deferring demand launch without paid authority: no '
                        'ACTIVE zero-cost location matches exact accelerator '
                        f'override {resources_override["accelerators"]!r}.')
                    return None
                raise ValueError(
                    'No active placement location matches exact accelerator '
                    f'override {resources_override["accelerators"]!r}.')
            if existing_replica_infos is None:
                existing_replica_infos = serve_state.get_replica_infos(
                    self._service_name)
            if not zero_cost_only:
                saturated_locations = (
                    self._demand_saturated_zero_cost_locations(
                        existing_replica_infos))
                if saturated_locations:
                    candidate_locations = (
                        set(self._spot_placer.active_locations()) if
                        allowed_locations is None else set(allowed_locations))
                    allowed_locations = (candidate_locations -
                                         saturated_locations)
                    allowed_location_kwargs = {
                        'allowed_locations': allowed_locations
                    }
                    if not allowed_locations:
                        logger.info(
                            'Deferring demand launch because every active '
                            'location belongs to a saturated reserved-fill '
                            'pool.')
                        return None
            if zero_cost_only:
                if fill_protocol_version is not None:
                    if (isinstance(fill_protocol_version, bool) or
                            not isinstance(fill_protocol_version, int) or
                            fill_protocol_version not in (1, 2)):
                        self._log_fill_skip('invalid reserved-fill protocol '
                                            'version')
                        return None
                    if (not isinstance(fill_pool_key, str) or
                            not fill_pool_key or
                            isinstance(fill_grant_epoch, bool) or
                            not isinstance(fill_grant_epoch, int) or
                            fill_grant_epoch < 1):
                        self._log_fill_skip('incomplete broker epoch/pool '
                                            'fence')
                        return None
                if fill_protocol_version == 2:
                    resource_scope = self._resource_scope
                    service_hash = self._service_hash
                    if (not isinstance(resource_scope, str) or
                            not resource_scope or
                            resource_scope != service_hash):
                        self._log_fill_skip(
                            'protocol-v2 fill requires a durable service '
                            'incarnation scope')
                        return None
                    if (isinstance(fill_service_generation, bool) or
                            not isinstance(fill_service_generation, int) or
                            fill_service_generation < 1 or
                            not isinstance(fill_physical_cluster_uid, str) or
                            not fill_physical_cluster_uid or
                            fill_allowed_location_keys is None):
                        self._log_fill_skip('incomplete protocol-v2 pool '
                                            'fence')
                        return None
                    allocation_attribution = (
                        fill_allocation_generation,
                        fill_allocation_input_sha256,
                        fill_allocation_claim_generation,
                        fill_service_version,
                        fill_gate_generation,
                        fill_reclaim_fleet_bundle_sha256,
                        fill_reclaim_policy_revision,
                        fill_reclaim_provider_inventory_sha256,
                        fill_worker_projection_sha256,
                        fill_observation_generation,
                        fill_observation_sequence,
                        fill_ordinary_admission_sequence,
                        fill_intent_idempotency_key,
                    )
                    if any(value is not None
                           for value in allocation_attribution):
                        if (type(fill_allocation_generation) is not int or
                                fill_allocation_generation < 1 or
                                not _is_lowercase_sha256(
                                    fill_allocation_input_sha256) or
                                type(fill_allocation_claim_generation)
                                is not int or
                                fill_allocation_claim_generation < 1 or
                                type(fill_service_version) is not int or
                                fill_service_version < 1 or
                                fill_service_version != launch_version or
                                type(fill_gate_generation) is not int or
                                fill_gate_generation < 1 or
                                not _is_lowercase_sha256(
                                    fill_reclaim_fleet_bundle_sha256) or
                                not isinstance(fill_reclaim_policy_revision,
                                               str) or
                                not fill_reclaim_policy_revision or
                                not _is_lowercase_sha256(
                                    fill_reclaim_provider_inventory_sha256) or
                                not _is_lowercase_sha256(
                                    fill_worker_projection_sha256) or
                                type(fill_observation_generation) is not int or
                                fill_observation_generation < 1 or
                                type(fill_observation_sequence) is not int or
                                fill_observation_sequence < 0 or
                                type(fill_ordinary_admission_sequence)
                                is not int or
                                fill_ordinary_admission_sequence < 0 or
                                fill_ordinary_admission_sequence
                                > fill_observation_sequence or
                                not _is_lowercase_sha256(
                                    fill_intent_idempotency_key)):
                            self._log_fill_skip(
                                'incomplete typed allocation attribution')
                            return None
                    assert isinstance(fill_pool_key, str)
                    try:
                        fill_pool_identity = (
                            reserved_capacity_broker.parse_pool_identity(
                                fill_pool_key))
                    except (TypeError, ValueError):
                        self._log_fill_skip('malformed protocol-v2 pool key')
                        return None
                    if (fill_pool_identity.protocol_version != 2 or
                            fill_pool_identity.physical_cluster_uid
                            != fill_physical_cluster_uid):
                        self._log_fill_skip('protocol-v2 pool identity does '
                                            'not match its physical UID')
                        return None
                    raw_exact_shape = resources_override.get('accelerators')
                    if raw_exact_shape is not None:
                        if (not isinstance(raw_exact_shape, dict) or
                                len(raw_exact_shape) != 1):
                            self._log_fill_skip('malformed protocol-v2 exact '
                                                'accelerator shape')
                            return None
                        raw_card, raw_count = next(iter(
                            raw_exact_shape.items()))
                        if (not isinstance(raw_card, str) or not raw_card or
                                isinstance(raw_count, bool) or
                                not isinstance(raw_count, int) or
                                raw_count < 1 or raw_card.casefold()
                                not in fill_pool_identity.gpu_names):
                            self._log_fill_skip('protocol-v2 exact accelerator '
                                                'shape is outside its pool')
                            return None
                        fill_exact_accelerator_shape = (raw_card.casefold(),
                                                        raw_count)
                # Historical protocol-v1 decisions retain a cheap pre-location
                # epoch check; their authoritative recheck remains atomic with
                # standalone persistence below. Protocol v2 has a durable
                # intent lease and deliberately performs no current-epoch
                # pre-read: its round, intent, replica, association, request,
                # queue, and pin fences commit in one atomic admission.
                if (zero_cost_actuation_lease is None and
                        fill_grant_epoch is not None and
                        fill_pool_key is not None):
                    broker_epoch = reserved_capacity_broker.current_epoch(
                        fill_pool_key)
                    if (broker_epoch is not None and
                            broker_epoch != fill_grant_epoch):
                        self._log_fill_skip(
                            f'grant epoch {fill_grant_epoch} superseded '
                            f'(current {broker_epoch})')
                        return None
                # The no-spill guarantee: a fill launch either lands on a
                # zero-cost ACTIVE location or does not happen at all --
                # checked BEFORE any replica row is persisted, so an
                # aborted fill launch leaks nothing and the autoscaler
                # simply retries on a later tick as capacity frees.
                zero_cost_location = (
                    self._spot_placer.select_next_zero_cost_location(
                        allowed_locations=allowed_locations))
                if zero_cost_location is None:
                    self._log_fill_skip(
                        'no ACTIVE zero-cost location available')
                    return None
                location = zero_cost_location
                if fill_protocol_version == 2:
                    assert fill_pool_identity is not None
                    kube_context = location.region
                    selected_shape = location.accelerators
                    if (str(location.cloud).lower() != 'kubernetes' or
                            not isinstance(kube_context, str) or
                            not kube_context or
                            not isinstance(selected_shape, dict) or
                            len(selected_shape) != 1):
                        self._release_unstarted_location_retry(location)
                        self._log_fill_skip('selected pool location has no '
                                            'exact Kubernetes accelerator '
                                            'shape')
                        return None
                    selected_card, selected_count = next(
                        iter(selected_shape.items()))
                    selected_capacity = _whole_gpu_capacity(selected_shape)
                    if (not isinstance(selected_card, str) or
                            not selected_card or selected_capacity is None or
                            selected_card.casefold()
                            not in fill_pool_identity.gpu_names):
                        self._release_unstarted_location_retry(location)
                        self._log_fill_skip('selected pool location has no '
                                            'matching Kubernetes accelerator '
                                            'identity')
                        return None
                    if fill_exact_accelerator_shape is not None:
                        if ((str(selected_card).casefold(), selected_count)
                                != fill_exact_accelerator_shape):
                            self._release_unstarted_location_retry(location)
                            self._log_fill_skip(
                                'selected pool location changed the carried '
                                'exact accelerator shape')
                            return None
                    fill_launch_context = kube_context
                    fill_launch_accelerator_shape = (selected_card.casefold(),
                                                     selected_capacity)
            elif not paid_launch_allowed:
                if zero_cost_demand_budget is not None:
                    location = self._select_budgeted_zero_cost_location(
                        zero_cost_demand_budget, allowed_locations)
                else:
                    location = self._spot_placer.select_next_zero_cost_location(
                        allowed_locations=allowed_locations)
                if location is None:
                    logger.info(
                        'Deferring demand launch without paid authority: no '
                        'ACTIVE budgeted zero-cost location is available.')
                    return None
            else:
                if paid_location_launch_budget is None:
                    paid_location_launch_budget = (
                        paid_capacity.build_launch_budget(
                            self._spot_placer,
                            workspace=self._workspace,
                            existing_replica_infos=existing_replica_infos,
                            globally_managed=(self._service_hash is not None),
                            service_name=self._service_name,
                            service_hash=self._service_hash,
                            max_live_paid_gpu_units=(
                                launch_spec.max_live_paid_gpu_units),
                            requested_frontier_keys=(
                                None if allowed_locations is None else {
                                    paid_capacity.frontier_key(candidate)
                                    for candidate in allowed_locations
                                })))
                assert paid_location_launch_budget is not None
                if self._demand_should_skip_zero_cost(existing_replica_infos):
                    # The broker grant or speculative-probe budget says this
                    # demand launch should compete on paid capacity instead of
                    # preferring the zero-cost tier.  The placer falls back to
                    # zero-cost when no paid candidate exists.
                    location = paid_capacity.select_location(
                        self._spot_placer,
                        paid_location_launch_budget,
                        skip_zero_cost_preference=True,
                        **allowed_location_kwargs)
                    if location is None:
                        logger.info(
                            'Deferring demand launch because every active paid '
                            'location is awaiting launch feedback.')
                        return None
                    if (zero_cost_demand_budget is not None and location
                            in self._spot_placer.zero_cost_locations()):
                        budgeted_location = (
                            self._select_budgeted_zero_cost_location(
                                zero_cost_demand_budget, allowed_locations))
                        if budgeted_location is None:
                            logger.info(
                                'Deferring demand launch because the shared '
                                'zero-cost GPU budget is exhausted and no '
                                'paid location is active.')
                            return None
                        location = budgeted_location
                elif zero_cost_demand_budget is not None:
                    location = self._select_budgeted_zero_cost_location(
                        zero_cost_demand_budget, allowed_locations)
                    if location is None:
                        location = paid_capacity.select_location(
                            self._spot_placer,
                            paid_location_launch_budget,
                            skip_zero_cost_preference=True,
                            **allowed_location_kwargs)
                        if location is None:
                            logger.info(
                                'Deferring demand launch because every active '
                                'paid location is awaiting launch feedback.')
                            return None
                        if location in self._spot_placer.zero_cost_locations():
                            # A successful zero (or an exhausted speculative
                            # allowance) is authoritative. If no paid candidate
                            # is active, defer instead of falling through into
                            # the same saturated research pool.
                            logger.info(
                                'Deferring demand launch because the shared '
                                'zero-cost GPU budget is exhausted and no '
                                'paid location is active.')
                            return None
                elif self._demand_should_skip_saturated_zero_cost(
                        existing_replica_infos):
                    location = paid_capacity.select_location(
                        self._spot_placer,
                        paid_location_launch_budget,
                        skip_zero_cost_preference=True,
                        **allowed_location_kwargs)
                else:
                    location = paid_capacity.select_location(
                        self._spot_placer, paid_location_launch_budget,
                        **allowed_location_kwargs)
            if location is None:
                logger.info('Deferring demand launch because every active paid '
                            'location is awaiting launch feedback.')
                return None
            debit_paid_location_launch_budget = (
                paid_location_launch_budget is not None and
                location in paid_location_launch_budget.remaining_by_location)
            resources_override.update(location.to_dict())
            if fill_exact_accelerator_shape is not None:
                persisted_shape = resources_override.get('accelerators')
                assert isinstance(persisted_shape, dict)
                persisted_card, persisted_count = next(
                    iter(persisted_shape.items()))
                if ((str(persisted_card).casefold(), persisted_count)
                        != fill_exact_accelerator_shape):
                    self._release_unstarted_location_retry(location)
                    self._log_fill_skip('persisted pool location changed the '
                                        'carried exact accelerator shape')
                    return None
            if fill_protocol_version == 2:
                assert isinstance(fill_pool_key, str)
                assert isinstance(fill_service_generation, int)
                assert isinstance(fill_physical_cluster_uid, str)
                assert fill_launch_context is not None
                assert fill_launch_accelerator_shape is not None
                fill_card, fill_count = fill_launch_accelerator_shape
                fill_cloud_launch_guard = functools.partial(
                    _protocol_v2_fill_cloud_launch_guard,
                    fill_pool_key,
                    fill_service_generation,
                    fill_physical_cluster_uid,
                    fill_launch_context,
                    fill_card,
                    fill_count,
                    resources_override,
                )
                guard_allowed, guard_reason = fill_cloud_launch_guard()
                if not guard_allowed:
                    self._release_unstarted_location_retry(location)
                    self._log_fill_skip(
                        'selected protocol-v2 pool pin failed its pre-launch '
                        f'guard: {guard_reason}')
                    return None
            # The location dictates the actual spot-ness of THIS launch
            # (a zero-cost reserved location is non-spot even though the
            # task as a whole is spot-managed).
            use_spot = location.use_spot
        elif self._spot_placer is not None:
            # Pinned launch: the persisted override already carries the
            # placer's inlined location fields. A recovered spot pin skips
            # fresh selection explicitly; a non-spot pin skips it via the
            # use_spot gate after its fill sentinel was consumed at original
            # emission. Recover the location from the override so the
            # upserted replica row keeps it -- location=None would permanently
            # drop the replica from zero-cost fill accounting (no scale-down
            # shelter, undercounted fill baseline).
            location = recovered_location
            if location is None:
                location = spot_placer.Location.from_resources_override(
                    resources_override)
            if location is not None:
                use_spot = location.use_spot
                # Same fail-fast contract as the selection path above: a
                # recovered pin targets ONE location, so a capacity
                # failure there must surface immediately (the placer
                # benches it and the next launch picks elsewhere).
                # Leaving retry_until_up=True would spin inside
                # sky.launch forever on a full zero-cost tier, occupying
                # a bounded launch-pool slot and starving demand
                # launches -- while availability_max_retry=1 (armed by
                # this location below) never sees the error.
                retry_until_up = False
        self._persist_spot_placement_state_if_dirty()
        if logical_reconcile_fence is not None:
            if not self._logical_reconcile_fence_holds(
                    logical_reconcile_fence,
                    require_exact_generation=(
                        logical_reconcile_fence_requires_exact_generation),
                    require_fresh_occupancy=False):
                logger.info('Logical launch selection was superseded before '
                            'row persistence; dropping the unpersisted pin.')
                return None
        # When the spot placer owns failover (use_spot + placer above sets
        # retry_until_up=False), the launch is pinned to ONE location, so a
        # capacity failure there must propagate immediately for the placer to
        # mark the location preemptive and pick a different one on the next
        # launch. Retrying the same exhausted zone in place with the default
        # attempts + 60s exponential backoff burns minutes before failing
        # over. Other (transient) launch errors say nothing about the
        # location's capacity, so they keep the default in-place retries.
        availability_max_retry = (1 if location is not None else None)
        cloud_launch_guard = fill_cloud_launch_guard
        if (self._uses_logical_replicas and
                bool(self._logical_exact_accelerator_shapes) and
                not zero_cost_only and cost_rebalance_for_replica_id is None and
                not prior_unknown_capacity_replacement):
            assert cloud_launch_guard is None
            cloud_launch_guard = lambda: (
                self._queued_logical_launch_fence_decision(replica_id)[:2])
        expected_manager_version = launch_version
        existing_cloud_launch_guard = cloud_launch_guard

        def _versioned_cloud_launch_guard() -> bool | tuple[bool, str]:
            generation_decision = self._queued_launch_generation_decision(
                expected_manager_version)
            if not generation_decision[0]:
                return generation_decision
            if existing_cloud_launch_guard is None:
                return generation_decision
            return existing_cloud_launch_guard()

        cloud_launch_guard = _versioned_cloud_launch_guard
        launch_fence = self._replica_launch_fence_context(launch_version)
        if fill_protocol_version == reserved_capacity_broker.PROTOCOL_V2:
            assert isinstance(fill_pool_key, str)
            assert isinstance(fill_service_generation, int)
            assert isinstance(fill_physical_cluster_uid, str)
            assert fill_launch_context is not None
            assert fill_launch_accelerator_shape is not None
            if launch_fence is None:
                self._release_unstarted_location_retry(location)
                self._log_fill_skip(
                    'protocol-v2 fill requires a durable service-owner '
                    'launch fence')
                return None
            fill_card, fill_count = fill_launch_accelerator_shape
            launch_fence = dict(launch_fence)
            launch_fence.update(
                reserved_capacity.make_protocol_v2_launch_fence(
                    pool_key=fill_pool_key,
                    service_generation=fill_service_generation,
                    service_version=launch_version,
                    physical_cluster_uid=fill_physical_cluster_uid,
                    kubernetes_context=fill_launch_context,
                    accelerator=fill_card,
                    accelerator_count=fill_count,
                    reconciliation_gate_generation=fill_gate_generation,
                    reclaim_fleet_bundle_sha256=(
                        fill_reclaim_fleet_bundle_sha256),
                    reclaim_policy_revision=fill_reclaim_policy_revision,
                    reclaim_provider_inventory_sha256=(
                        fill_reclaim_provider_inventory_sha256),
                    worker_projection_sha256=(fill_worker_projection_sha256)))
        recovery_intent: (system_recovery_state.SystemRecoveryLaunchIntent |
                          None) = None
        recovery_launch_context: dict[str, Any] | None = None
        candidate_prerequisites = (
            not recovering_existing_replica and not zero_cost_only and
            not self._is_pool and self._resource_action_mode == 'legacy' and
            self._enforce_launch_fence and launch_fence is not None and
            launch_spec is not None and
            serve_state.system_recovery_persistence_available() and
            launch_spec.endpoint_probe_interval_seconds <= serve_constants.
            SYSTEM_RECOVERY_MAX_ELIGIBLE_PROBE_INTERVAL_SECONDS and
            launch_spec.readiness_timeout_seconds <= serve_constants.
            SYSTEM_RECOVERY_MAX_ELIGIBLE_READINESS_TIMEOUT_SECONDS)
        if candidate_prerequisites:
            try:
                candidate_task = _build_replica_launch_task(
                    launch_yaml_content,
                    replica_id,
                    resources_override,
                    exact_resources_override=location is not None,
                    authoritative_service_spec=launch_spec,
                    service_name=self._service_name,
                    task_template=launch_task_template)
                if not _task_is_known_non_aws(candidate_task):
                    workspace = self._workspace
                    with skypilot_config.local_active_workspace_ctx(workspace):
                        requested_authorization = (
                            system_oom_recovery.
                            resolve_requested_authorization_v3(
                                candidate_task,
                                service_name=self._service_name,
                                service_hash=self._service_hash))
                    if requested_authorization is not None:
                        assert isinstance(self._service_hash, str)
                        recovery_intent = (
                            system_recovery_state.SystemRecoveryLaunchIntent(
                                version=1,
                                controller_contract_version=
                                (serve_constants.
                                 SYSTEM_OOM_RECOVERY_CONTROLLER_CONTRACT_VERSION
                                ),
                                recovery_authorization_version=(
                                    requested_authorization.
                                    authorization_version),
                                recovery_authorization_profile_id=(
                                    requested_authorization.profile_id),
                                recovery_authorization_sha256=(
                                    requested_authorization.authorization_sha256
                                ),
                                runtime_profile_version=(
                                    requested_authorization.
                                    runtime_profile_version),
                                expected_runtime_capability=(
                                    requested_authorization.
                                    expected_runtime_capability),
                                service_hash=self._service_hash,
                                replica_id=replica_id,
                                launch_generation=replica_id,
                                launch_nonce=(
                                    system_oom_recovery.new_launch_nonce()),
                                workspace=requested_authorization.workspace,
                                resource_envelope_sha256=(
                                    requested_authorization.
                                    resource_envelope_sha256),
                                task_sha256=(
                                    requested_authorization.task_sha256),
                                runtime_image_digest=(requested_authorization.
                                                      runtime_image_digest),
                                owned_container_spec_sha256=(
                                    requested_authorization.
                                    owned_container_spec_sha256),
                                execution_envelope_sha256=(
                                    requested_authorization.
                                    execution_envelope_sha256)))
                        assert self._controller_owner is not None
                        controller_pid, controller_ip = self._controller_owner
                        recovery_launch_context = (
                            system_oom_recovery.create_unbound_launch_context(
                                recovery_intent,
                                service_name=self._service_name,
                                service_version=launch_version,
                                controller_pid=controller_pid,
                                controller_ip=controller_ip))
            except Exception as e:  # pylint: disable=broad-except
                # Candidate authorization is additive. A malformed/missing
                # operator document or local pre-policy mismatch must leave
                # the existing replica launch ordinary, not fail capacity.
                recovery_intent = None
                recovery_launch_context = None
                logger.warning(
                    'System-OOM recovery candidate resolution failed closed; '
                    f'launching replica {replica_id} ordinarily: '
                    f'{common_utils.format_exception(e)}')
        replica_port = _get_resources_ports(launch_yaml_content, launch_spec,
                                            launch_task_template)

        planned_capacity = 1
        if self._uses_logical_replicas:
            resolved_capacity = (prior_planned_capacity
                                 if recovering_existing_replica else None)
            if resolved_capacity is None:
                accelerators = (location.accelerators
                                if location is not None else None)
                if accelerators is None and resources_override is not None:
                    accelerators = resources_override.get('accelerators')
                resolved_capacity = _whole_gpu_capacity(accelerators)
                if resolved_capacity is None:
                    resolved_capacity = self._default_planned_capacity
            if resolved_capacity is None:
                raise RuntimeError(
                    'Logical replica launch requires an exact whole-GPU '
                    'shape before the replica row is persisted.')
            planned_capacity = resolved_capacity
        info = ReplicaInfo(
            replica_id,
            cluster_name,
            replica_port,
            use_spot,
            location,
            launch_version,
            resources_override,
            planned_capacity=planned_capacity,
            unknown_capacity_replacement=prior_unknown_capacity_replacement)
        binding_authority = self._ordinary_launch_binding_authority
        if (binding_authority is not None and
                binding_authority.generic_launches_required and
                prior_unknown_capacity_replacement and
                non_pool_launch_authorization is None):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Unknown-capacity replacement has no exact durable '
                'observation authorization.')
        if non_pool_launch_authorization is not None:
            info.non_pool_launch_authorization = (non_pool_launch_authorization)
        if recovering_existing_replica and prior_replica_record_id is not None:
            info.replica_record_id = prior_replica_record_id
            info.created_at = prior_created_at
        # Persisted launch-origin attribution: the broker's holdings split
        # and the ceiling's demand exemption key on this flag. Interrupted
        # fill rows are torn down during recovery instead of re-driven, so
        # only a fresh sentinel-authorized launch may set these fields.
        info.reserved_fill = bool(zero_cost_only)
        info.reserved_fill_pool_key = (fill_pool_key
                                       if zero_cost_only else None)
        info.reserved_fill_service_generation = (fill_service_generation
                                                 if zero_cost_only else None)
        info.reserved_fill_physical_cluster_uid = (fill_physical_cluster_uid
                                                   if zero_cost_only else None)
        info.reserved_fill_kubernetes_context = (fill_launch_context
                                                 if zero_cost_only else None)
        info.reserved_fill_allocation_generation = (fill_allocation_generation
                                                    if zero_cost_only else None)
        info.reserved_fill_allocation_input_sha256 = (
            fill_allocation_input_sha256 if zero_cost_only else None)
        info.reserved_fill_allocation_claim_generation = (
            fill_allocation_claim_generation if zero_cost_only else None)
        info.reserved_fill_reconciliation_gate_generation = (
            fill_gate_generation if zero_cost_only else None)
        info.reserved_fill_reclaim_fleet_bundle_sha256 = (
            fill_reclaim_fleet_bundle_sha256 if zero_cost_only else None)
        info.reserved_fill_reclaim_policy_revision = (
            fill_reclaim_policy_revision if zero_cost_only else None)
        info.reserved_fill_reclaim_provider_inventory_sha256 = (
            fill_reclaim_provider_inventory_sha256 if zero_cost_only else None)
        info.reserved_fill_worker_projection_sha256 = (
            fill_worker_projection_sha256 if zero_cost_only else None)
        info.reserved_fill_observation_generation = (
            fill_observation_generation if zero_cost_only else None)
        info.reserved_fill_observation_sequence = (fill_observation_sequence
                                                   if zero_cost_only else None)
        info.reserved_fill_intent_idempotency_key = (
            fill_intent_idempotency_key if zero_cost_only else None)
        is_zero_cost = bool(prior_is_zero_cost or zero_cost_only)
        if not is_zero_cost and self._spot_placer is not None:
            is_zero_cost = self._spot_placer.is_zero_cost_location(location)
        info.is_zero_cost = is_zero_cost
        launch_result = _ReplicaLaunchResult(
            replica_id=replica_id,
            planned_capacity=planned_capacity,
            funding=(_ReplicaLaunchFunding.ZERO_COST
                     if is_zero_cost else _ReplicaLaunchFunding.PAID))
        info.cost_rebalance_for_replica_id = (cost_rebalance_for_replica_id)
        info.paid_capacity_pool_key = prior_paid_capacity_pool_key
        atomic_admission_spec: Any = None

        def _make_launch_thread(
            recovery_launch_kwargs: dict[str,
                                         Any],) -> _ReplicaLaunchThread | None:
            nonlocal atomic_admission_spec
            completion_queue, completion_event = self._launch_completion_state()
            teardown_requested = threading.Event()
            frozen_controller_config = skypilot_config.to_dict()
            frozen_controller_config_path = os.environ.get(
                skypilot_config.ENV_VAR_SKYPILOT_CONFIG)
            ordinary_binding_profile = (
                self._is_ordinary_launch_binding_profile(
                    info, recovery_launch_kwargs))
            authority = self._ordinary_launch_binding_authority
            generic_profile_kind = (
                None if self._is_pool else
                ordinary_launch_binding.classify_non_pool_launch_profile(info))
            if generic_profile_kind is None and protocol_v2_fill:
                # A typed protocol-v2 fill owns every field except the
                # global zero-cost admission sequence. PostgreSQL allocates
                # that sequence with the replica/request transaction.
                generic_profile_kind = (
                    ordinary_launch_binding.
                    classify_uncommitted_protocol_v2_reserved_fill_profile(
                        info, protocol_version=fill_protocol_version))
            generic_launches_required = bool(
                authority is not None and authority.generic_launches_required)
            if generic_launches_required and generic_profile_kind is None:
                raise _BoundOrdinaryLaunchUnresolvedError(
                    'Promoted generic service produced an incomplete non-pool '
                    f'launch profile for replica {info.replica_id}.')
            bound_non_pool_launch = bool(generic_launches_required and
                                         generic_profile_kind is not None)
            bound_ordinary_launch = bool(
                bound_non_pool_launch or
                (ordinary_binding_profile and
                 self._bound_ordinary_launch_is_eligible(
                     info, recovery_launch_kwargs)))
            ordinary_legacy_launch = bool(ordinary_binding_profile and
                                          not bound_ordinary_launch)
            effective_launch_fence = launch_fence
            if (not bound_non_pool_launch and not ordinary_binding_profile and
                    not self._is_pool):
                # Emit this while still in legacy mode too.  A queued special
                # launch can then cross an immediately following promotion
                # without being mistaken for an unbound ordinary request.
                effective_launch_fence = (
                    self._binding_excluded_launch_fence_context(
                        info, effective_launch_fence))
            launch_thread_kwargs: dict[str, Any] = {
                'availability_max_retry': availability_max_retry,
                'exact_resources_override': location is not None,
                'pre_launch_guard':
                    (self._ordinary_binding_profile_launch_is_authorized
                     if bound_ordinary_launch or ordinary_binding_profile else
                     self._service_is_launch_authorized),
                'cloud_launch_guard': cloud_launch_guard,
                'supersession_guard': functools.partial(
                    self._queued_launch_generation_decision,
                    expected_manager_version),
                'continue_guard': self._launch_owner_watchdog_allows_continue,
                'cleanup_continue_guard': self._service_is_cleanup_authorized,
                'launch_fence': effective_launch_fence,
                'service_spec': launch_spec,
                'task_template': launch_task_template,
                'service_name': self._service_name,
                'workspace': self._workspace,
                'frozen_controller_config': frozen_controller_config,
                'frozen_controller_config_path': frozen_controller_config_path,
                **recovery_launch_kwargs,
            }
            if bound_ordinary_launch:
                # Build the same launch task once locally so typed provider
                # failures can update the exact paid-capacity pool in the
                # reducer transaction. This construction is side-effect free.
                bound_task = _build_replica_launch_task(
                    launch_yaml_content,
                    replica_id,
                    resources_override,
                    exact_resources_override=location is not None,
                    authoritative_service_spec=launch_spec,
                    service_name=self._service_name,
                    task_template=launch_task_template)
                bound_cloud = next(iter(bound_task.resources)).cloud
                base_launch_fence = launch_fence
                if (generic_profile_kind == ordinary_launch_binding.
                        NonPoolLaunchProfileKind.SYSTEM_OOM_RECOVERY):
                    recovery_context = recovery_launch_kwargs.get(
                        'system_recovery_launch_context')
                    if not isinstance(recovery_context, dict):
                        raise _BoundOrdinaryLaunchUnresolvedError(
                            'System-OOM profile has no recovery execution '
                            'envelope.')
                    base_launch_fence = recovery_context
                effective_launch_fence = (
                    self._bound_ordinary_launch_fence_context(
                        info,
                        launch_version,
                        base_launch_fence=(base_launch_fence
                                           if bound_non_pool_launch else None)))
                bound_profile_kind = (None if generic_profile_kind is None else
                                      generic_profile_kind.value)
                submission_id = (
                    request_postgres.stable_bound_ordinary_launch_submission_id(
                        self._service_name, info.replica_id,
                        info.replica_record_id))
                if (protocol_v2_fill and
                        zero_cost_actuation_lease is not None and
                        generic_profile_kind is ordinary_launch_binding.
                        NonPoolLaunchProfileKind.RESERVED_FILL):
                    assert authority is not None
                    assert fill_grant_epoch is not None
                    assert fill_ordinary_admission_sequence is not None
                    prepared = sdk.prepare_launch_request_for_server_controller(
                        bound_task,
                        cluster_name,
                        workspace=self._workspace,
                        retry_until_up=retry_until_up,
                        extra_launch_context=effective_launch_fence)
                    admission_info = copy.deepcopy(info)
                    admission_info.status_property.sky_launch_status = (
                        common_utils.ProcessStatus.RUNNING)
                    atomic_admission_spec = (
                        reserved_fill_admission.AdmissionSpec(
                            prepared_request=prepared,
                            submission_id=uuid.UUID(submission_id),
                            authority=authority,
                            replica_info=admission_info,
                            actuation_lease=zero_cost_actuation_lease,
                            launch_limit=(
                                controller_utils.get_serve_launch_limit(
                                    self._is_pool))))
                    return None
                inspect_bound, reduce_bound, cancel_bound = (
                    self._bound_ordinary_launch_callbacks(info, bound_cloud))
                launch_thread_kwargs.update({
                    'launch_fence': effective_launch_fence,
                    'ordinary_launch_submission_uuid': submission_id,
                    'non_pool_launch_profile_kind': bound_profile_kind,
                    'inspect_bound_ordinary_launch': inspect_bound,
                    'reduce_bound_ordinary_launch': reduce_bound,
                    'cancel_bound_ordinary_launch': cancel_bound,
                })
            if not self._is_pool and not zero_cost_only:
                input_digest = (ordinary_launch_handoff.redacted_input_digest(
                    launch_yaml_content, resources_override))
                if input_digest is not None:
                    launch_thread_kwargs['ordinary_launch_handoff_context'] = {
                        'context_version':
                            (serve_constants.
                             ORDINARY_LAUNCH_HANDOFF_CONTEXT_VERSION),
                        'service_name': self._service_name,
                        'service_version': launch_version,
                        'replica_id': replica_id,
                        'replica_record_id': info.replica_record_id,
                        'controller_route_epoch':
                            (self._ordinary_launch_handoff_route_epoch),
                        'input_digest': input_digest,
                    }
                    launch_thread_kwargs['ordinary_launch_event'] = (
                        functools.partial(
                            self._emit_ordinary_launch_handoff_event,
                            info,
                            input_digest=input_digest,
                            allow_demoted_candidate=bool(
                                recovery_launch_kwargs)))
            return _ReplicaLaunchThread(
                target=launch_cluster_with_frozen_controller_config,
                replica_id=replica_id,
                replica_record_id=info.replica_record_id,
                service_hash=self._service_hash,
                controller_owner=self._controller_owner,
                teardown_requested=teardown_requested,
                completion_queue=completion_queue,
                completion_event=completion_event,
                bound_ordinary_launch=bound_ordinary_launch,
                ordinary_legacy_launch=ordinary_legacy_launch,
                args=(replica_id, launch_yaml_content, cluster_name,
                      log_file_name, legacy_runtime.replica_to_request_id,
                      resources_override, retry_until_up),
                kwargs={
                    **launch_thread_kwargs,
                    'teardown_requested': teardown_requested,
                },
            )

        if fill_protocol_version == reserved_capacity_broker.PROTOCOL_V2:
            # Single-scale callers acquire blocking admission before self.lock.
            # A batch already owns self.lock, so each item instead attempts a
            # zero-wait admission here. FIFO prevents later items from barging
            # once an ambient waiter queues, and a busy item leaks no durable
            # row or launch thread. Retain the admitted phase and exact UID
            # authority continuously through the atomic broker persist and
            # launch-thread argument freeze. The asynchronous request starts
            # later and proves its durable tuple afresh in the executor.
            assert fill_launch_context is not None
            assert isinstance(fill_physical_cluster_uid, str)
            assert isinstance(fill_service_generation, int)
            assert fill_grant_epoch is not None
            assert fill_pool_key is not None
            phase_context: contextlib.AbstractContextManager[
                provider_phase.ProviderPhaseAdmission]
            if provider_phase_admission is None:
                assert try_provider_phase_admission
                phase_context = provider_phase.try_provider_phase(
                    provider_phase.ProviderPhaseMode.V2_FENCED)
            else:
                phase_context = contextlib.nullcontext(provider_phase_admission)
            admission_receipt: Any = None
            try:
                with phase_context as effective_admission:
                    with provider_phase.join_provider_phase(
                            effective_admission):
                        if try_provider_phase_admission:
                            physical_context = (
                                kubernetes_adaptor.physical_cluster_uid_fence(
                                    fill_launch_context,
                                    fill_physical_cluster_uid,
                                    wait_for_initializer=False))
                        elif require_preinitialized_physical_fence:
                            physical_context = (
                                kubernetes_adaptor.physical_cluster_uid_fence(
                                    fill_launch_context,
                                    fill_physical_cluster_uid,
                                    require_existing=True))
                        else:
                            physical_context = (
                                kubernetes_adaptor.physical_cluster_uid_fence(
                                    fill_launch_context,
                                    fill_physical_cluster_uid))
                        with physical_context:
                            # Freeze the complete request tuple before its
                            # durable reservation. Construction is side-effect
                            # free; doing it first prevents an allocation row
                            # from being left without local worker ownership if
                            # construction raises.
                            try:
                                _make_launch_thread({})
                            except BaseException as error:
                                try:
                                    self._release_unstarted_location_retry(
                                        location)
                                except BaseException as cleanup_error:
                                    if not isinstance(error, Exception):
                                        raise error from cleanup_error
                                    if not isinstance(cleanup_error, Exception):
                                        raise
                                    raise error from cleanup_error
                                raise
                            needs_logical_state_guard = (
                                logical_reconcile_fence is not None or
                                require_preinitialized_physical_fence)
                            logical_state_guard = (self._logical_state_lock
                                                   if needs_logical_state_guard
                                                   else
                                                   contextlib.nullcontext())
                            with logical_state_guard:
                                pending_version = self._pending_version
                                if (require_preinitialized_physical_fence and
                                        pending_version is not None and
                                        pending_version > launch_version):
                                    self._release_unstarted_location_retry(
                                        location)
                                    logger.info(
                                        'Reserved-fill launch was superseded '
                                        'by a pending service version at its '
                                        'final row-persistence fence.')
                                    return None
                                if (logical_reconcile_fence is not None and
                                        not self._logical_reconcile_fence_holds(
                                            logical_reconcile_fence,
                                            require_exact_generation=
                                            (logical_reconcile_fence_requires_exact_generation
                                            ),
                                            require_fresh_occupancy=False)):
                                    logger.info(
                                        'Logical launch was superseded at its '
                                        'final row-persistence fence.')
                                    self._release_unstarted_location_retry(
                                        location)
                                    return None
                                if atomic_admission_spec is None:
                                    raise _BoundOrdinaryLaunchUnresolvedError(
                                        'protocol-v2 fill has no atomic '
                                        'admission specification')
                                admission_result = (
                                    reserved_fill_admission.admit(
                                        atomic_admission_spec))
                                if (admission_result.disposition
                                        is reserved_fill_admission.
                                        AdmissionDisposition.AMBIGUOUS):
                                    raise (reserved_fill_admission.
                                           AdmissionAmbiguousError)(
                                               admission_result.detail or
                                               'atomic admission is ambiguous')
                                if (admission_result.disposition
                                        is not reserved_fill_admission.
                                        AdmissionDisposition.COMMITTED):
                                    self._release_unstarted_location_retry(
                                        location)
                                    self._log_fill_skip(
                                        admission_result.detail or
                                        'atomic admission was rejected')
                                    return None
                                admission_receipt = admission_result.receipt
                                if (admission_receipt is None or
                                        admission_receipt.replica_id
                                        != replica_id or
                                        admission_receipt.replica_record_id
                                        != info.replica_record_id):
                                    raise (reserved_fill_admission.
                                           AdmissionAmbiguousError)(
                                               'atomic admission returned a '
                                               'mismatched durable receipt')
                assert admission_receipt is not None
                # Atomic admission has already committed this exact launch as
                # RUNNING under P.  The adopter is observation-only and starts
                # after that transaction released every admission lock.
                info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.RUNNING)
                bound_context = admission_receipt.context
                if (not isinstance(
                        bound_context,
                        ordinary_launch_binding.BoundNonPoolLaunchContext) or
                        bound_context.profile.kind
                        is not ordinary_launch_binding.NonPoolLaunchProfileKind.
                        RESERVED_FILL or str(bound_context.association_id)
                        != admission_receipt.association_id or
                        bound_context.request_id != admission_receipt.request_id
                        or bound_context.service_name != self._service_name or
                        bound_context.replica_id != replica_id or
                        str(bound_context.replica_record_id)
                        != info.replica_record_id or
                        bound_context.launch_generation
                        != admission_receipt.launch_generation):
                    raise reserved_fill_admission.AdmissionAmbiguousError(
                        'committed atomic admission returned an inconsistent '
                        'bound launch context')
                try:
                    self._install_bound_launch_adopter(
                        info,
                        bound_context,
                        start=True,
                        yaml_content=launch_yaml_content,
                        spec=launch_spec,
                        existing_replica_infos=existing_replica_infos)
                except BaseException as error:
                    if not isinstance(error, Exception):
                        raise
                    raise reserved_fill_admission.AdmissionAmbiguousError(
                        'committed atomic admission could not register its '
                        'adopter') from error
                return launch_result
            except BaseException as error:
                if (admission_receipt is not None or isinstance(
                        error,
                        reserved_fill_admission.AdmissionAmbiguousError)):
                    try:
                        self._launch_completion_event.set()
                    except BaseException as signal_error:
                        if not isinstance(error, Exception):
                            raise error from signal_error
                        if not isinstance(signal_error, Exception):
                            raise
                        if isinstance(
                                error, reserved_fill_admission.
                                AdmissionAmbiguousError):
                            raise error from signal_error
                        raise reserved_fill_admission.AdmissionAmbiguousError(
                            'committed atomic admission could not signal '
                            'reconciliation') from signal_error
                    if not isinstance(error, Exception):
                        raise
                    if isinstance(
                            error,
                            reserved_fill_admission.AdmissionAmbiguousError):
                        raise
                    raise reserved_fill_admission.AdmissionAmbiguousError(
                        'committed atomic admission failed during '
                        'postcommit finalization') from error
                if isinstance(
                        error,
                    (exceptions.ProviderPhaseBusyError,
                     exceptions.KubernetesPhysicalClusterFenceBusyError)):
                    self._release_unstarted_location_retry(location)
                    self._log_fill_skip(
                        'provider or physical-cluster phase is busy; deferring '
                        'this launch without reserving capacity')
                    if require_preinitialized_physical_fence:
                        raise
                    assert try_provider_phase_admission
                    raise exceptions.ProviderPhaseBusyError(
                        'Protocol-v2 batch item deferred at its persist seam.'
                    ) from error
                if isinstance(
                        error,
                        exceptions.KubernetesPhysicalClusterIdentityError):
                    self._release_unstarted_location_retry(location)
                    self._log_fill_skip(
                        'selected protocol-v2 pool physical identity could not '
                        f'be proved: {common_utils.format_exception(error)}')
                    if require_preinitialized_physical_fence:
                        raise
                    return None
                raise

        capacity_plan_claim: Mapping[str, Any] | None = None
        if debit_paid_location_launch_budget:
            assert location is not None
            try:
                capacity_plan_claim = (
                    None if paid_launch_authority is None else
                    paid_launch_authority.claim_values(
                        (str(next(iter(location.accelerators))).casefold()
                         if location.accelerators and len(location.accelerators)
                         == 1 else capacity_admission.AGGREGATE_ACCELERATOR),
                        planned_capacity))
            except capacity_admission.CapacityAdmissionError as error:
                self._release_unstarted_location_retry(location)
                logger.info(
                    'Deferring paid demand launch because its ordered '
                    'capacity authority changed: %s',
                    common_utils.format_exception(error))
                return None
        stage_paid_launch = bool(
            prepared_paid_launches is not None and
            debit_paid_location_launch_budget and
            paid_launch_authority is not None and
            paid_location_launch_budget is not None and
            paid_location_launch_budget.globally_managed and
            not recovering_existing_replica and
            cost_rebalance_for_replica_id is None and
            not prior_unknown_capacity_replacement and recovery_intent is None)
        if stage_paid_launch:
            assert location is not None
            assert paid_location_launch_budget is not None
            # Generic ORDINARY_PAID classification happens while the worker
            # is frozen, before Phase A. Carry the exact candidate pool on the
            # in-memory row now; only the batch transaction can persist it.
            info.paid_capacity_pool_key = (
                paid_location_launch_budget.pool_key_by_location[location])

        logical_state_guard = (self._logical_state_lock
                               if logical_reconcile_fence is not None else
                               contextlib.nullcontext())
        with logical_state_guard:
            if logical_reconcile_fence is not None:
                if not self._logical_reconcile_fence_holds(
                        logical_reconcile_fence,
                        require_exact_generation=(
                            logical_reconcile_fence_requires_exact_generation),
                        require_fresh_occupancy=False):
                    logger.info('Logical launch was superseded at its final '
                                'row-persistence fence.')
                    return None
            if (zero_cost_only and fill_grant_epoch is not None and
                    fill_pool_key is not None):
                if fill_protocol_version not in (
                        None, reserved_capacity_broker.PROTOCOL_V1):
                    raise ValueError(
                        'Only protocol-v1 fill may use standalone replica '
                        'persistence.')
                # Broker epoch fence, authoritative leg: the pre-check above
                # is TOCTOU (a round can publish a new epoch between it and
                # this persist, after capacity was already fed to a peer), so
                # the final recheck and the row upsert are ONE transaction.
                if not reserved_capacity_broker.persist_fill_replica(
                        self._service_name,
                        replica_id,
                        info,
                        pool_key=fill_pool_key,
                        expected_epoch=fill_grant_epoch,
                        **self._db_fence_kwargs()):
                    # No row was written and the launch thread was never
                    # registered/started: same leak-nothing contract as the
                    # pre-check fence.
                    self._release_unstarted_location_retry(location)
                    self._log_fill_skip(
                        f'grant epoch {fill_grant_epoch} superseded or round '
                        'in flight at persist')
                    return None
            else:
                if debit_paid_location_launch_budget:
                    assert location is not None
                    assert paid_location_launch_budget is not None
                    if stage_paid_launch:
                        # The wave caller owns Phase A. Keep this exact
                        # identity unpersisted and unregistered until every
                        # candidate has been frozen and one atomic transaction
                        # returns its authoritative sparse result.
                        pass
                    else:
                        try:
                            claim_result = paid_capacity.try_persist_claim(
                                service_name=self._service_name,
                                service_hash=self._service_hash,
                                controller_owner=self._controller_owner,
                                replica_id=replica_id,
                                replica_info=info,
                                location=location,
                                budget=paid_location_launch_budget,
                                priority=launch_priority,
                                capacity_plan_claim=capacity_plan_claim)
                        except capacity_admission.CapacityAdmissionError as error:
                            self._release_unstarted_location_retry(location)
                            logger.info(
                                'Deferring paid demand launch because its '
                                'ordered capacity authority changed: %s',
                                common_utils.format_exception(error))
                            return None
                        if claim_result not in (
                                paid_capacity.ClaimResult.ACQUIRED,
                                paid_capacity.ClaimResult.LEGACY_LOCAL):
                            # Selection consumes an expired bench's one-probe
                            # reservation. An admission rejection never reached
                            # the provider, so release that reservation instead
                            # of silently extending the durable cooldown.
                            assert self._spot_placer is not None
                            self._spot_placer.release_retry(location)
                            self._persist_spot_placement_state_if_dirty()
                        if (claim_result ==
                                paid_capacity.ClaimResult.FEEDBACK_PENDING):
                            paid_capacity.defer_for_feedback(
                                paid_location_launch_budget, location)
                            logger.info('Deferring paid demand launch at '
                                        f'{location}: {claim_result.value}.')
                            return None
                        if claim_result == paid_capacity.ClaimResult.SATURATED:
                            paid_capacity.exhaust(paid_location_launch_budget,
                                                  location)
                            logger.info('Deferring paid demand launch at '
                                        f'{location}: {claim_result.value}.')
                            return None
                        if (claim_result ==
                                paid_capacity.ClaimResult.SERVICE_SATURATED):
                            paid_capacity.exhaust_service(
                                paid_location_launch_budget)
                            logger.info(
                                'Deferring paid demand launch because the '
                                'service paid-capacity envelope is full.')
                            return None
                        if (claim_result == paid_capacity.ClaimResult.
                                HIGHER_PRIORITY_WAITING):
                            paid_capacity.defer_for_priority(
                                paid_location_launch_budget, location)
                            logger.info('Deferring paid demand launch at '
                                        f'{location}: {claim_result.value}.')
                            return None
                        if claim_result == paid_capacity.ClaimResult.OWNERSHIP_LOST:
                            raise RuntimeError(
                                f'Service {self._service_name!r} controller '
                                'ownership changed while claiming paid '
                                'capacity.')
                        if claim_result == paid_capacity.ClaimResult.LEGACY_LOCAL:
                            if recovering_existing_replica:
                                self._persist_replica(replica_id, info)
                            else:
                                self._persist_new_replica(replica_id, info)
                else:
                    if recovering_existing_replica:
                        self._persist_replica(replica_id, info)
                    else:
                        self._persist_new_replica(replica_id, info)
            if debit_paid_location_launch_budget:
                paid_capacity.debit(paid_location_launch_budget, location)
            if info.unknown_capacity_replacement:
                self._unknown_capacity_replacement_ids.add(replica_id)

        if recovery_intent is not None:
            candidate_info = self._create_system_recovery_candidate(
                replica_id, recovery_intent)
            if candidate_info is None:
                recovery_intent = None
                recovery_launch_context = None
            else:
                info = candidate_info

        recovery_launch_kwargs: dict[str, Any] = {}
        if recovery_intent is not None:
            recovery_launch_kwargs = {
                'system_recovery_launch_context': recovery_launch_context,
                'get_bound_system_recovery_request_id': functools.partial(
                    self._get_bound_system_recovery_request_id, replica_id,
                    recovery_intent),
                'persist_system_recovery_job_id': functools.partial(
                    self._persist_system_recovery_job_id, replica_id,
                    recovery_intent),
                'demote_system_recovery_candidate': functools.partial(
                    self._demote_system_recovery_candidate, replica_id,
                    recovery_intent),
            }
        t = _make_launch_thread(recovery_launch_kwargs)
        assert t is not None
        if existing_replica_infos is not None:
            # Bulk callers (recovery re-drive) reuse one snapshot across a
            # whole wave of launches. Append each accepted replica so shared
            # zero-cost capacity accounting sees the in-wave reservations.
            existing_replica_infos.append(info)
        if stage_paid_launch:
            assert prepared_paid_launches is not None
            assert location is not None
            prepared_paid_launches.append(
                _PreparedPaidLaunch(candidate=paid_capacity.PaidClaimCandidate(
                    replica_id=replica_id,
                    replica_info=info,
                    location=location,
                    priority=launch_priority,
                    capacity_plan_claim=capacity_plan_claim),
                                    launch_result=launch_result,
                                    launch_thread=t))
            return launch_result
        # Don't start right now; _refresh_thread_pool owns the shared launch
        # limit and final ownership/target fences.  Wake it immediately,
        # though: a planner-bound paid claim can be invalidated by the next
        # five-second demand report, while the periodic fallback is twenty
        # seconds.  Waiting for that fallback made a continuously reporting
        # load balancer starve every cold paid launch before provider I/O.
        legacy_runtime.launch_thread_pool[replica_id] = t
        legacy_runtime.launch_completion_event.set()
        return launch_result

    def _demand_should_skip_zero_cost(
            self, existing_replica_infos: list['ReplicaInfo'] | None) -> bool:
        """Broker demand-placement gate: stop NEW squatting at the grant.

        When this service already holds at least its broker grant on the
        zero-cost tier, its DEMAND launches stop preferring that tier and
        select paid capacity when it is available. Reads ONLY the poller's
        in-process grant cache -- no DB on the launch path, and with no broker
        grant (single service, broker disabled, or unit tests) the gate is
        inert and behavior is exactly pre-broker.
        Demand replicas already ON the pool are untouched: the gate
        prevents new squatting only; existing rows are demand-protected
        until their traffic recedes (v1 semantics per the design doc).
        """
        # Protocol v2 is filtered pool-by-pool by
        # _demand_saturated_zero_cost_locations(). Do not collapse those
        # independent grants back into this protocol-v1 global switch.
        if reserved_capacity_broker.get_cached_pool_grants(
                self._service_name,
                max_age_seconds=2 * reserved_capacity.poll_interval_seconds()):
            return False
        grant = reserved_capacity_broker.get_cached_grant(
            self._service_name,
            # The poller refreshes the cache every poll interval; 2x
            # tolerates scheduling jitter without letting the gate flap
            # open between polls. A poller outage past that reopens the
            # zero-cost preference -- the safe direction (pre-broker
            # behavior), and the broker's ceiling still bounds the fill
            # fleet itself.
            max_age_seconds=2 * reserved_capacity.poll_interval_seconds())
        if grant is None:
            return False
        if self._spot_placer is None:
            return False
        zero_cost = self._spot_placer.zero_cost_locations()
        if not zero_cost:
            return False
        holdings = 0
        for info in existing_replica_infos or []:
            if info.is_terminal:
                continue
            replica_location = info.get_spot_location()
            if replica_location is None:
                continue
            if any(
                    spot_placer.locations_match_placement(replica_location, zc)
                    for zc in zero_cost):
                holdings += 1
        return holdings >= grant

    def _demand_saturated_zero_cost_locations(
        self,
        existing_replica_infos: list['ReplicaInfo'] | None,
    ) -> set[spot_placer.Location]:
        """Return only protocol-v2 pools whose fresh grant is occupied."""
        if self._spot_placer is None:
            return set()
        grants = reserved_capacity_broker.get_cached_pool_grants(
            self._service_name,
            max_age_seconds=2 * reserved_capacity.poll_interval_seconds())
        if not grants:
            return set()
        zero_cost_locations = self._spot_placer.zero_cost_locations()
        saturated: set[spot_placer.Location] = set()
        for grant in grants.values():
            pool_locations = [
                location for location in zero_cost_locations
                if (location.region == grant.access_context and
                    location.accelerators and any(
                        str(accelerator).lower() in grant.accelerator_names
                        for accelerator in location.accelerators))
            ]
            holdings = 0
            for info in existing_replica_infos or []:
                if info.is_terminal:
                    continue
                replica_location = info.get_spot_location()
                if (replica_location is not None and any(
                        spot_placer.locations_match_placement(
                            replica_location, candidate)
                        for candidate in pool_locations)):
                    holdings += 1
            if holdings >= grant.grant:
                saturated.update(pool_locations)
        return saturated

    def _select_budgeted_zero_cost_location(
        self,
        budget: _ZeroCostDemandBudget,
        allowed_locations: set[spot_placer.Location] | None = None,
    ) -> spot_placer.Location | None:
        """Reserve one location while balancing a multi-pool launch wave.

        Selection uses remaining backend attempts, rather than raw GPU count,
        as the balance unit. This keeps different replica widths comparable
        and ensures equal unknown-capacity probe budgets alternate across
        contexts instead of letting one indefinitely pending Kubernetes pool
        consume the whole provider-launch admission window.
        """
        if self._spot_placer is None:
            return None
        available_pool_keys: dict[spot_placer.Location, tuple[str, str]] = {}
        for location in self._spot_placer.zero_cost_locations():
            if (allowed_locations is not None and
                    location not in allowed_locations):
                continue
            pool_key = _zero_cost_pool_key(location)
            if pool_key is None:
                continue
            remaining = budget.remaining_by_pool.get(pool_key, 0)
            measured = budget.measured_by_pool.get(pool_key)
            width = _whole_gpu_capacity(location.accelerators)
            if remaining <= 0 or width is None:
                continue
            # A successful measurement is expressed in GPU slots. During a
            # measurement blackout, the fallback is deliberately expressed
            # in bounded speculative backend attempts instead.
            if measured is None or width <= remaining:
                available_pool_keys[location] = pool_key
        if not available_pool_keys:
            return None
        fewest_selected = min(
            budget.selected_launches_by_pool.get(pool_key, 0)
            for pool_key in available_pool_keys.values())
        allowed = {
            location for location, pool_key in available_pool_keys.items() if
            budget.selected_launches_by_pool.get(pool_key, 0) == fewest_selected
        }
        location = self._spot_placer.select_next_zero_cost_location(
            allowed_locations=allowed)
        if location is None:
            return None
        pool_key = _zero_cost_pool_key(location)
        assert pool_key is not None, location
        remaining = budget.remaining_by_pool[pool_key]
        assert remaining > 0, (location, budget)
        width = _whole_gpu_capacity(location.accelerators)
        assert width is not None, location
        debit = (width
                 if budget.measured_by_pool.get(pool_key) is not None else 1)
        assert remaining >= debit, (location, budget)
        budget.remaining_by_pool[pool_key] = remaining - debit
        budget.selected_launches_by_pool[pool_key] = (
            budget.selected_launches_by_pool.get(pool_key, 0) + 1)
        return location

    def _locations_for_accelerator_override(
        self,
        resources_override: dict[str, Any],
    ) -> set[spot_placer.Location] | None:
        """Restrict a targeted launch to one exact accelerator shape."""
        if self._spot_placer is None:
            return None
        requested = resources_override.get('accelerators')
        if not isinstance(requested, dict) or not requested:
            return None
        requested_shape = {
            str(name).casefold(): count for name, count in requested.items()
        }
        return {
            location for location in self._spot_placer.active_locations()
            if isinstance(location.accelerators, dict) and {
                str(name).casefold(): count
                for name, count in location.accelerators.items()
            } == requested_shape
        }

    def _requested_paid_frontier_keys(
        self, resources_overrides: Iterable[dict[str, Any] | None]
    ) -> set[paid_capacity.FrontierKey] | None:
        """Return exact cards targeted by a batch, or None for task defaults."""
        requested_frontiers: set[paid_capacity.FrontierKey] = set()
        for resources_override in resources_overrides:
            if resources_override is None:
                return None
            allowed = self._locations_for_accelerator_override(
                resources_override)
            if allowed is None:
                return None
            requested_frontiers.update(
                paid_capacity.frontier_key(location) for location in allowed)
        return requested_frontiers

    def _build_zero_cost_demand_budget(
        self,
        existing_replica_infos: list['ReplicaInfo'],
        resources_overrides: list[dict[str, Any] | None],
        demand_count_override: int | None = None,
        capacity_replica_infos: list['ReplicaInfo'] | None = None,
    ) -> _ZeroCostDemandBudget | None:
        """Build a nonblocking shared free-GPU budget for one demand wave.

        The gate-selected observation is either the legacy context cache or an
        exact-current-claim sequenced physical-pool record. Rows across every
        service that may not be represented in that snapshot are debited under
        the cross-process reservation lock before this budget is returned;
        sequenced observations use their durable admission/materialization
        high-waters instead of application time. Only legacy unknown/zero
        observations may receive bounded speculative probes. Sequenced or
        unavailable UNKNOWN grants zero, while the controller's authenticated
        allocation fence independently withholds the later paid pass.
        """
        if self._spot_placer is None:
            return None
        demand_count = (
            demand_count_override if demand_count_override is not None else sum(
                resources_override is None or serve_constants.
                RESERVED_CAPACITY_FILL_OVERRIDE_KEY not in resources_override
                for resources_override in resources_overrides))
        active = set(self._spot_placer.active_locations())
        all_zero_cost = self._spot_placer.zero_cost_locations()
        zero_cost = [
            location for location in all_zero_cost if location in active and
            str(location.cloud).lower() == 'kubernetes'
        ]
        if not zero_cost:
            return None
        observations = reserved_capacity.get_cached_free_gpus_by_pool(
            zero_cost,
            service_name=self._service_name,
            service_version=self.latest_version)
        kubernetes_only_placement = (
            _placer_has_only_non_spot_kubernetes_gpu_locations(
                self._spot_placer))
        measured = {
            key: observation.free_gpus
            for key, observation in observations.items()
        }
        authority_by_pool = {
            key: observation.authority
            for key, observation in observations.items()
        }
        for measured_pool_key, free_gpus in measured.items():
            if (free_gpus == 0 and authority_by_pool.get(measured_pool_key) is
                    reserved_capacity.FreeGpuObservationAuthority.LEGACY_AMBIENT
                    and (kubernetes_only_placement or
                         _kubernetes_context_has_configured_autoscaler(
                             measured_pool_key[0]))):
                measured[measured_pool_key] = None
        active_count_by_pool: dict[tuple[str, str], int] = {}
        for location in zero_cost:
            pool_key = _zero_cost_pool_key(location)
            if pool_key is not None:
                active_count_by_pool[pool_key] = (
                    active_count_by_pool.get(pool_key, 0) + 1)

        capacity_infos = (existing_replica_infos if capacity_replica_infos
                          is None else capacity_replica_infos)
        unobserved_gpus_by_pool: dict[tuple[str, str], int] = {}
        unresolved_backends_by_pool: dict[tuple[str, str], int] = {}
        for info in capacity_infos:
            if info.is_terminal:
                continue
            replica_location = info.get_spot_location()
            if replica_location is None:
                continue
            pool_key = _zero_cost_pool_key(replica_location)
            if pool_key not in active_count_by_pool:
                continue
            observation = observations.get(pool_key)
            snapshot_time = (None if observation is None else
                             observation.snapshot_time)
            observation_admission_sequence = (None if observation is None else
                                              observation.admission_sequence)
            observation_materialization_sequence = (
                None if observation is None else
                observation.materialization_sequence)
            observation_authority = (
                reserved_capacity.FreeGpuObservationAuthority.UNAVAILABLE
                if observation is None else observation.authority)
            created_at = info.created_at
            status_property = info.status_property
            if (observation_authority is reserved_capacity.
                    FreeGpuObservationAuthority.SEQUENCED_GATE and
                    observation_admission_sequence is not None and
                    observation_materialization_sequence is not None):
                admission_sequence = info.zero_cost_admission_sequence
                materialization_sequence = (
                    info.zero_cost_materialization_sequence)
                unobserved = (isinstance(admission_sequence, bool) or
                              not isinstance(admission_sequence, int) or
                              admission_sequence
                              > observation_admission_sequence or
                              isinstance(materialization_sequence, bool) or
                              not isinstance(materialization_sequence, int) or
                              materialization_sequence
                              > observation_materialization_sequence)
                if not info.is_ready:
                    unresolved_backends_by_pool[pool_key] = (
                        unresolved_backends_by_pool.get(pool_key, 0) + 1)
            elif (observation_authority is not reserved_capacity.
                  FreeGpuObservationAuthority.LEGACY_AMBIENT):
                # No sequenced high-water means no consumable capacity. Keep
                # the row conservatively unobserved, but never reinterpret the
                # blackout through legacy timestamps or speculative probes.
                unobserved = True
                if not info.is_ready:
                    unresolved_backends_by_pool[pool_key] = (
                        unresolved_backends_by_pool.get(pool_key, 0) + 1)
            elif info.is_ready:
                first_ready_time = status_property.first_ready_time
                if not isinstance(first_ready_time, (int, float)):
                    first_ready_time = None
                # The query timestamp is captured before the Kubernetes pod
                # list. A row that only became READY afterwards may represent
                # a pod created during the query and must be debited once.
                unobserved = (snapshot_time is not None and
                              (first_ready_time is None or
                               first_ready_time > snapshot_time))
            else:
                unresolved_backends_by_pool[pool_key] = (
                    unresolved_backends_by_pool.get(pool_key, 0) + 1)
                launch_status = status_property.sky_launch_status
                unobserved = (
                    info.status == serve_state.ReplicaStatus.PENDING or
                    launch_status != common_utils.ProcessStatus.SUCCEEDED or
                    (snapshot_time is not None and created_at is not None and
                     created_at > snapshot_time))
            if unobserved:
                width = (_whole_gpu_capacity(replica_location.accelerators) or
                         int(info.planned_capacity))
                unobserved_gpus_by_pool[pool_key] = (
                    unobserved_gpus_by_pool.get(pool_key, 0) + width)

        remaining: dict[tuple[str, str], int] = {}
        for pool_key, location_count in active_count_by_pool.items():
            free_gpus = measured.get(pool_key)
            if free_gpus is None:
                if authority_by_pool.get(pool_key) is (
                        reserved_capacity.FreeGpuObservationAuthority.
                        LEGACY_AMBIENT):
                    allowance = (_ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION *
                                 location_count)
                    remaining[pool_key] = max(
                        0, allowance -
                        unresolved_backends_by_pool.get(pool_key, 0))
                else:
                    remaining[pool_key] = 0
            else:
                remaining[pool_key] = max(
                    0, free_gpus - unobserved_gpus_by_pool.get(pool_key, 0))
        logger.info('Zero-cost demand capacity snapshot: measured='
                    f'{measured}, unobserved_gpus='
                    f'{unobserved_gpus_by_pool}, '
                    f'authority={authority_by_pool}, '
                    f'batch_budget={remaining}, demand={demand_count}.')
        return _ZeroCostDemandBudget(remaining, measured)

    def _uses_shared_zero_cost_demand_budget(self) -> bool:
        """Whether demand placement can consume a shared free GPU pool."""
        if self._spot_placer is None:
            return False
        active = set(self._spot_placer.active_locations())
        all_zero_cost = self._spot_placer.zero_cost_locations()
        zero_cost = [
            location for location in all_zero_cost
            if location in active and _zero_cost_pool_key(location) is not None
        ]
        return bool(zero_cost)

    def _demand_should_skip_saturated_zero_cost(
            self, existing_replica_infos: list['ReplicaInfo'] | None) -> bool:
        """Bound speculative demand launches into zero-cost locations.

        Placement for one autoscaler tick happens before any launch outcome is
        available.  Count nonterminal, not-yet-READY rows already pinned to
        ACTIVE zero-cost locations and stop preferring the free tier once the
        per-location probe budget is full.  ``select_next_location(...,
        skip_zero_cost_preference=True)`` still falls back to zero-cost when no
        paid candidate exists, so this pacing never makes a zero-cost-only
        service unavailable.

        READY rows deliberately do not consume the probe budget: they prove
        capacity exists.  Rows on a benched location also do not consume the
        budget for a different active shape; the placer's normal bench logic
        already excludes the failed location.
        """
        if self._spot_placer is None:
            return False
        active_locations = self._spot_placer.active_locations()
        active_zero_cost = [
            location for location in self._spot_placer.zero_cost_locations()
            if location in active_locations
        ]
        if not active_zero_cost:
            return False
        speculative = 0
        for info in existing_replica_infos or []:
            if info.is_terminal or info.is_ready:
                continue
            replica_location = info.get_spot_location()
            if replica_location is None:
                continue
            if any(
                    spot_placer.locations_match_placement(
                        replica_location, zero_cost)
                    for zero_cost in active_zero_cost):
                speculative += 1
        limit = (_ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION *
                 len(active_zero_cost))
        if speculative < limit:
            return False
        logger.debug('Zero-cost demand probe budget is full '
                     f'({speculative}/{limit} across '
                     f'{len(active_zero_cost)} active location(s)); '
                     'placing additional demand on paid candidates.')
        return True

    def _log_fill_skip(self, reason: str) -> None:
        """Rate-limited skip log for aborted reserved-capacity fill launches.

        A tick can carry a whole batch of fill scale-ups; when the
        zero-cost tier is benched every one of them skips with the same
        reason, every tick, until capacity frees -- log at INFO once per
        window and DEBUG otherwise.
        """
        now = time.time()
        if now - self._fill_skip_last_log_time >= (
                _FILL_SKIP_LOG_INTERVAL_SECONDS):
            self._fill_skip_last_log_time = now
            logger.info(f'Reserved-capacity fill launch skipped: {reason}.')
        else:
            logger.debug(f'Reserved-capacity fill launch skipped: {reason}.')

    def _scale_up_one_locked(
        self,
        resources_override: dict[str, Any] | None,
        used_replica_ids: set[int],
        existing_replica_infos: list['ReplicaInfo'] | None = None,
        zero_cost_demand_budget: _ZeroCostDemandBudget | None = None,
        paid_location_launch_budget: paid_capacity.LaunchBudget | None = None,
        paid_launch_authority: capacity_admission.PaidLaunchAuthority |
        None = None,
        logical_reconcile_fence: LogicalTargetState | None = None,
        logical_reconcile_fence_requires_exact_generation: bool = False,
        unknown_capacity_replacement: bool = False,
        unknown_capacity_replacement_authorization: dict[str, Any] |
        None = None,
        launch_priority: int = serve_constants.LB_REQUEST_PRIORITY_MIN,
        paid_launch_allowed: bool = True,
        provider_phase_admission: (provider_phase.ProviderPhaseAdmission |
                                   None) = None,
        require_preinitialized_physical_fence: bool = False,
        zero_cost_actuation_lease: (zero_cost_actuation.IntentLease |
                                    None) = None,
        prepared_paid_launches: list[_PreparedPaidLaunch] | None = None,
    ) -> _ReplicaLaunchResult | None:
        """Allocate an id and enqueue one replica launch. Lock must be held.

        `used_replica_ids` is the set of ids with a durable replica row,
        snapshotted once per lock acquisition. Ids are handed out
        monotonically while the lock is held, so the snapshot stays valid
        for the whole batch without a per-replica DB read.
        """
        # Defensive: never hand `_launch_replica` an id that still has a
        # durable replica row. `add_or_update_replica` is an upsert keyed on
        # (service_name, replica_id), so reusing a live id would overwrite a
        # surviving replica's persisted state and re-launch its cluster. With
        # the allocator seeded from durable state in
        # `_recover_replica_operations` this should never fire, but guard
        # against any drift so id allocation can never clobber a live replica.
        while self._next_replica_id in used_replica_ids:
            logger.warning(f'Replica id {self._next_replica_id} still has a '
                           'durable replica row; skipping it to avoid '
                           'clobbering a live replica.')
            self._next_replica_id += 1
        # An aborted launch (zero-cost-only fill with no ACTIVE zero-cost
        # location) consumed nothing: keep the id free for the next
        # scale-up.
        if (unknown_capacity_replacement_authorization is not None and
                not unknown_capacity_replacement):
            raise ValueError('Unknown-capacity authorization requires its '
                             'replacement marker.')
        if existing_replica_infos is None:
            direct_launch_kwargs: dict[str, Any] = {}
            if unknown_capacity_replacement:
                direct_launch_kwargs['prior_unknown_capacity_replacement'] = (
                    True)
                if unknown_capacity_replacement_authorization is not None:
                    direct_launch_kwargs[
                        'unknown_capacity_replacement_authorization'] = (
                            unknown_capacity_replacement_authorization)
            if launch_priority != serve_constants.LB_REQUEST_PRIORITY_MIN:
                direct_launch_kwargs['launch_priority'] = launch_priority
            if paid_launch_authority is not None:
                direct_launch_kwargs['paid_launch_authority'] = (
                    paid_launch_authority)
            if not paid_launch_allowed:
                direct_launch_kwargs['paid_launch_allowed'] = False
            if provider_phase_admission is not None:
                direct_launch_kwargs['provider_phase_admission'] = (
                    provider_phase_admission)
                if require_preinitialized_physical_fence:
                    direct_launch_kwargs[
                        'require_preinitialized_physical_fence'] = True
            elif _is_protocol_v2_fill_override(resources_override):
                direct_launch_kwargs['try_provider_phase_admission'] = True
            if zero_cost_actuation_lease is not None:
                direct_launch_kwargs['zero_cost_actuation_lease'] = (
                    zero_cost_actuation_lease)
            if prepared_paid_launches is not None:
                direct_launch_kwargs['prepared_paid_launches'] = (
                    prepared_paid_launches)
            launch_result = self._launch_replica(self._next_replica_id,
                                                 resources_override,
                                                 **direct_launch_kwargs)
        else:
            launch_kwargs: dict[str, Any] = {
                'existing_replica_infos': existing_replica_infos
            }
            if zero_cost_demand_budget is not None:
                launch_kwargs['zero_cost_demand_budget'] = (
                    zero_cost_demand_budget)
            if paid_location_launch_budget is not None:
                launch_kwargs['paid_location_launch_budget'] = (
                    paid_location_launch_budget)
            if paid_launch_authority is not None:
                launch_kwargs['paid_launch_authority'] = paid_launch_authority
            if logical_reconcile_fence is not None:
                launch_kwargs['logical_reconcile_fence'] = (
                    logical_reconcile_fence)
                launch_kwargs[
                    'logical_reconcile_fence_requires_exact_generation'] = (
                        logical_reconcile_fence_requires_exact_generation)
            if unknown_capacity_replacement:
                launch_kwargs['prior_unknown_capacity_replacement'] = True
                if unknown_capacity_replacement_authorization is not None:
                    launch_kwargs[
                        'unknown_capacity_replacement_authorization'] = (
                            unknown_capacity_replacement_authorization)
            if launch_priority != serve_constants.LB_REQUEST_PRIORITY_MIN:
                launch_kwargs['launch_priority'] = launch_priority
            if not paid_launch_allowed:
                launch_kwargs['paid_launch_allowed'] = False
            if provider_phase_admission is not None:
                launch_kwargs['provider_phase_admission'] = (
                    provider_phase_admission)
                if require_preinitialized_physical_fence:
                    launch_kwargs[
                        'require_preinitialized_physical_fence'] = True
            elif _is_protocol_v2_fill_override(resources_override):
                launch_kwargs['try_provider_phase_admission'] = True
            if zero_cost_actuation_lease is not None:
                launch_kwargs['zero_cost_actuation_lease'] = (
                    zero_cost_actuation_lease)
            if prepared_paid_launches is not None:
                launch_kwargs['prepared_paid_launches'] = (
                    prepared_paid_launches)
            launch_result = self._launch_replica(self._next_replica_id,
                                                 resources_override,
                                                 **launch_kwargs)
        if launch_result is not None:
            try:
                assert launch_result.replica_id == self._next_replica_id
                self._next_replica_id += 1
            except BaseException as error:
                if zero_cost_actuation_lease is not None:
                    if not isinstance(error, Exception):
                        raise
                    raise reserved_fill_admission.AdmissionAmbiguousError(
                        'committed materialization could not publish its '
                        'replica id') from error
                raise
        return launch_result

    @staticmethod
    def _reserved_fill_commit_result(
        plan: reserved_fill_planner.FillPlan,
        accepted: list[reserved_fill_planner.AcceptedFillIntent],
        deferred: list[reserved_fill_planner.DeferredFillIntent],
        *,
        authority_current: bool,
    ) -> reserved_fill_planner.FillCommitResult:
        """Build and self-check one complete, potentially sparse receipt."""
        receipt = reserved_fill_planner.FillCommitResult(
            accepted=tuple(accepted),
            deferred=tuple(deferred),
            authority_current=authority_current)
        receipt.validate_for_plan(plan)
        return receipt

    @staticmethod
    def _reserved_fill_deferred_tail(
        plan: reserved_fill_planner.FillPlan,
        deferred_from: int,
        reason: reserved_fill_planner.DeferredFillReason,
        detail: str,
    ) -> list[reserved_fill_planner.DeferredFillIntent]:
        """Build one typed tail for a batch-global admission failure."""
        return [
            reserved_fill_planner.DeferredFillIntent(intent, reason, detail)
            for intent in plan.intents[deferred_from:]
        ]

    def _reserved_fill_manager_authority_failure(
        self, plan: reserved_fill_planner.FillPlan
    ) -> tuple[reserved_fill_planner.DeferredFillReason, str] | None:
        """Check the manager-local and durable owner fence without providers."""
        if not plan.intents:
            return None
        first = plan.intents[0]
        expected_capacity_unit = (
            reserved_fill_planner.FillCapacityUnit.LOGICAL
            if self._uses_logical_replicas else
            reserved_fill_planner.FillCapacityUnit.PHYSICAL)
        if plan.capacity_unit is not expected_capacity_unit:
            return (reserved_fill_planner.DeferredFillReason.SUPERSEDED_POLICY,
                    'the manager replica unit no longer matches the plan')
        if self._is_pool:
            return (reserved_fill_planner.DeferredFillReason.SUPERSEDED_POLICY,
                    'worker pools cannot accept service reserved-fill plans')
        if self._reserved_fill_has_newer_pending_version(plan):
            return (reserved_fill_planner.DeferredFillReason.SUPERSEDED_POLICY,
                    'a newer service version is pending application')
        if (self._update_recovery_required or
                first.service_version != self.latest_version):
            return (reserved_fill_planner.DeferredFillReason.SUPERSEDED_POLICY,
                    'the manager service version no longer matches the plan')
        binding_authority = self._ordinary_launch_binding_authority
        if (binding_authority is not None and
                not binding_authority.generic_launches_required):
            return (
                reserved_fill_planner.DeferredFillReason.LOST_OWNER,
                'the current generic launch capability cohort is unavailable')
        service_hash = self._service_hash
        controller_owner = self._controller_owner
        if (not isinstance(service_hash, str) or not service_hash or
                self._resource_scope != service_hash or
                first.service_incarnation != service_hash or
                controller_owner is None or not self._enforce_launch_fence or
                self._ownership_lost.is_set()):
            return (reserved_fill_planner.DeferredFillReason.LOST_OWNER,
                    'the manager does not hold the plan service incarnation')
        try:
            owner = serve_state.get_service_controller_owner(self._service_name)
        except Exception as error:  # pylint: disable=broad-except
            logger.warning('Reserved-fill admission could not read controller '
                           'ownership: '
                           f'{common_utils.format_exception(error)}')
            return (reserved_fill_planner.DeferredFillReason.LOST_OWNER,
                    'controller ownership could not be proven')
        if (owner is None or owner.get('hash') != service_hash or
            (owner.get('controller_pid'), owner.get('controller_ip'))
                != controller_owner or owner.get('status') in
                serve_state.ServiceStatus.replica_launch_blocking_statuses()):
            return (
                reserved_fill_planner.DeferredFillReason.LOST_OWNER,
                'the durable controller owner no longer matches the manager')
        try:
            owner_fingerprint = serve_utils.make_controller_owner_fingerprint(
                service_hash, owner.get('controller_pid'),
                owner.get('controller_ip'), owner.get('controller_port'))
        except Exception as error:  # pylint: disable=broad-except
            logger.warning('Reserved-fill admission found an invalid durable '
                           'controller owner: '
                           f'{common_utils.format_exception(error)}')
            return (reserved_fill_planner.DeferredFillReason.LOST_OWNER,
                    'the durable controller owner is incomplete')
        if first.controller_owner != owner_fingerprint:
            return (reserved_fill_planner.DeferredFillReason.LOST_OWNER,
                    'the plan controller owner is no longer current')

        uids_by_context: dict[str, set[str]] = {}
        for intent in plan.intents:
            context_name = intent.allowed_locations[0].region
            uids_by_context.setdefault(context_name,
                                       set()).add(intent.physical_cluster_uid)
        if any(len(uids) != 1 for uids in uids_by_context.values()):
            return (reserved_fill_planner.DeferredFillReason.
                    PHYSICAL_CLUSTER_UID_MISMATCH,
                    'one Kubernetes context carries conflicting physical UIDs')
        return None

    def _reserved_fill_has_newer_pending_version(
            self, plan: reserved_fill_planner.FillPlan) -> bool:
        """Return the lock-free version signal checked at each persist seam."""
        if not plan.intents:
            return False
        pending_version = self._pending_version
        return (pending_version is not None and
                pending_version > plan.intents[0].service_version)

    def _reserved_fill_max_capacity_locked(self) -> int:
        """Return the exact current service ceiling in the plan's unit."""
        spec = self._version_specs.get(self.latest_version)
        if spec is None:
            spec = serve_state.get_spec(self._service_name, self.latest_version)
        if spec is None:
            raise ValueError('the current service specification is missing')
        maximum = (spec.max_replicas
                   if spec.max_replicas is not None else spec.min_replicas)
        if type(maximum) is not int or maximum < 0:
            raise ValueError('the current service maximum is malformed')
        return maximum

    def pending_reserved_fill_snapshot(
        self,
        allocation: reserved_fill_planner.AuthenticatedAllocationMap,
        capacity_unit: reserved_fill_planner.FillCapacityUnit,
    ) -> zero_cost_actuation.PendingFillSnapshot:
        """Return one bounded planning read of every live durable grant."""
        allocation.__post_init__()
        mode = self._reserved_fill_actuation_mode
        if mode is not zero_cost_actuation.ActuationMode.DURABLE_INTENT:
            raise zero_cost_actuation.ZeroCostActuationUnavailable(
                'Durable reserved-fill actuation is unavailable.')
        service_hash = self._service_hash
        if not isinstance(service_hash, str) or not service_hash:
            raise zero_cost_actuation.ZeroCostActuationConflict(
                'Reserved-fill manager has no service incarnation.')
        return self._zero_cost_actuation_repository.pending_fill_snapshot(
            service_name=self._service_name,
            service_hash=service_hash,
            allocation_generation=allocation.allocation_generation,
            allocation_input_sha256=allocation.allocation_input_sha256,
            allocation_claim_generation=(
                allocation.allocation_claim_generation),
            capacity_unit=capacity_unit)

    def install_durable_zero_cost_actuation(self) -> None:
        """Publish a committed one-way promotion to manager workers."""
        with self.lock:
            self._reserved_fill_actuation_mode = (
                zero_cost_actuation.ActuationMode.DURABLE_INTENT)
            self._zero_cost_actuation_event.set()

    @staticmethod
    def _reserved_fill_override(
            intent: reserved_fill_planner.FillIntent) -> dict[str, Any]:
        """Translate one typed intent into the sole transitional v2 seam."""
        return {
            serve_constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY: True,
            serve_constants.RESERVED_FILL_GRANT_EPOCH_OVERRIDE_KEY:
                intent.pool_epoch,
            serve_constants.RESERVED_FILL_POOL_KEY_OVERRIDE_KEY:
                intent.pool_key,
            serve_constants.RESERVED_FILL_PROTOCOL_VERSION_OVERRIDE_KEY:
                intent.protocol_version,
            serve_constants.RESERVED_FILL_SERVICE_GENERATION_OVERRIDE_KEY:
                intent.service_generation,
            serve_constants.RESERVED_FILL_SERVICE_VERSION_OVERRIDE_KEY:
                intent.service_version,
            serve_constants.RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY:
                intent.physical_cluster_uid,
            serve_constants.RESERVED_FILL_ALLOWED_LOCATIONS_OVERRIDE_KEY: list(
                intent.allowed_location_keys()),
            serve_constants.RESERVED_FILL_ALLOCATION_GENERATION_OVERRIDE_KEY:
                intent.allocation_generation,
            serve_constants.RESERVED_FILL_ALLOCATION_INPUT_SHA256_OVERRIDE_KEY:
                intent.allocation_input_sha256,
            serve_constants.RESERVED_FILL_ALLOCATION_CLAIM_GENERATION_OVERRIDE_KEY:
                intent.allocation_claim_generation,
            serve_constants.RESERVED_FILL_GATE_GENERATION_OVERRIDE_KEY:
                intent.reconciliation_gate_generation,
            serve_constants.RESERVED_FILL_RECLAIM_FLEET_BUNDLE_SHA256_OVERRIDE_KEY:
                intent.reclaim_fleet_bundle_sha256,
            serve_constants.RESERVED_FILL_RECLAIM_POLICY_REVISION_OVERRIDE_KEY:
                intent.reclaim_policy_revision,
            serve_constants.RESERVED_FILL_RECLAIM_PROVIDER_INVENTORY_SHA256_OVERRIDE_KEY:
                intent.reclaim_provider_inventory_sha256,
            serve_constants.RESERVED_FILL_WORKER_PROJECTION_SHA256_OVERRIDE_KEY:
                intent.worker_projection_sha256,
            serve_constants.RESERVED_FILL_OBSERVATION_GENERATION_OVERRIDE_KEY:
                intent.observation_generation,
            serve_constants.RESERVED_FILL_OBSERVATION_SEQUENCE_OVERRIDE_KEY:
                intent.observation_sequence,
            serve_constants.RESERVED_FILL_ORDINARY_ADMISSION_SEQUENCE_OVERRIDE_KEY:
                intent.ordinary_zero_cost_admission_sequence,
            serve_constants.RESERVED_FILL_INTENT_IDEMPOTENCY_KEY_OVERRIDE_KEY:
                intent.idempotency_key,
            'accelerators': {
                intent.accelerator: intent.accelerator_count
            },
        }

    @staticmethod
    def _start_reserved_fill_physical_preflights(
        intents: tuple[reserved_fill_planner.FillIntent, ...],
        admission: provider_phase.ProviderPhaseAdmission,
        workspace: str,
    ) -> _ReservedFillPhysicalPreflightBatch:
        """Initialize every distinct pool capture under one batch deadline."""
        preflights: dict[tuple[str, str], _ReservedFillPhysicalPreflight] = {}
        for intent in intents:
            target_key = (intent.allowed_locations[0].region,
                          intent.physical_cluster_uid)
            preflights.setdefault(target_key,
                                  _ReservedFillPhysicalPreflight(*target_key))
        deadline = (time.monotonic() +
                    _RESERVED_FILL_PHYSICAL_PREFLIGHT_TIMEOUT_SECONDS)

        def _hold_preflight(preflight: _ReservedFillPhysicalPreflight) -> None:
            physical_context: contextlib.AbstractContextManager[None] | None = (
                None)
            physical_context_entered = False
            try:
                with kubernetes_adaptor.api_call_deadline(
                        deadline, preflight.cancellation):
                    with skypilot_config.local_active_workspace_ctx(
                            workspace), provider_phase.join_provider_phase(
                                admission,
                                timeout_seconds=max(0.0, deadline -
                                                    time.monotonic())):
                        physical_context = (
                            kubernetes_adaptor.physical_cluster_uid_fence(
                                preflight.kubernetes_context,
                                preflight.physical_cluster_uid,
                                wait_for_initializer=False))
                        physical_context.__enter__()
                        physical_context_entered = True
                # The absolute deadline bounds provider initialization, not
                # ownership of a verified capture. Keep that capture alive
                # until the manager has completed every in-lock join.
                preflight.ready.set()
                preflight.release.wait()
            # This closure runs only in a dedicated synchronous preflight
            # thread, where every provider/context failure is result data.
            except BaseException as error:  # noqa: ASYNC103  # pylint: disable=broad-except
                if preflight.error is None:
                    preflight.error = error
                preflight.ready.set()
            finally:
                if physical_context_entered:
                    assert physical_context is not None
                    try:
                        physical_context.__exit__(None, None, None)
                    except BaseException as error:  # noqa: ASYNC103  # pylint: disable=broad-except
                        if preflight.error is None:
                            preflight.error = error
                        preflight.ready.set()

        threads: list[threading.Thread] = []
        for index, preflight in enumerate(preflights.values()):
            worker = threading.Thread(
                target=_hold_preflight,
                args=(preflight,),
                name=f'skyserve-fill-physical-preflight-{index}',
                daemon=True)
            worker.start()
            threads.append(worker)
        for preflight in preflights.values():
            remaining = max(0.0, deadline - time.monotonic())
            if not preflight.ready.wait(remaining):
                preflight.cancellation.set()
                preflight.release.set()
                if preflight.error is None:
                    preflight.error = TimeoutError(
                        'physical-cluster preflight exceeded its deadline')
        return _ReservedFillPhysicalPreflightBatch(preflights=preflights,
                                                   threads=tuple(threads),
                                                   deadline_monotonic=deadline)

    @staticmethod
    def _release_reserved_fill_physical_preflights(
        batch: _ReservedFillPhysicalPreflightBatch,) -> None:
        """Release every capture holder without waiting on a late provider."""
        for preflight in batch.preflights.values():
            preflight.cancellation.set()
            preflight.release.set()
        deadline = (time.monotonic() +
                    _RESERVED_FILL_PHYSICAL_PREFLIGHT_RELEASE_TIMEOUT_SECONDS)
        for worker in batch.threads:
            worker.join(timeout=max(0.0, deadline - time.monotonic()))

    def _zero_cost_actuation_authority_current(
            self, intent: reserved_fill_planner.FillIntent) -> bool:
        """Cheap in-process fence before a leased intent touches a provider."""
        binding_authority = self._ordinary_launch_binding_authority
        # An adjacent capability cohort may settle old actions only. Keep a
        # pre-row intent retryable until rotation installs launch authority.
        if (binding_authority is None or
                not binding_authority.generic_launches_required or
                self._reserved_fill_actuation_mode
                is not zero_cost_actuation.ActuationMode.DURABLE_INTENT or
                self._update_recovery_required or
                self._ownership_lost.is_set() or self._is_pool or
                intent.service_version != self.latest_version or
                intent.service_incarnation != self._service_hash or
                self._resource_scope != self._service_hash or
                self._controller_owner is None or
                not self._enforce_launch_fence):
            return False
        try:
            owner = serve_state.get_service_controller_owner(self._service_name)
            if owner is None:
                return False
            owner_fingerprint = serve_utils.make_controller_owner_fingerprint(
                owner.get('hash'), owner.get('controller_pid'),
                owner.get('controller_ip'), owner.get('controller_port'))
        except Exception:  # pylint: disable=broad-except
            return False
        return bool(
            owner.get('hash') == self._service_hash and
            (owner.get('controller_pid'), owner.get('controller_ip'))
            == self._controller_owner and owner.get('status')
            not in serve_state.ServiceStatus.replica_launch_blocking_statuses()
            and owner_fingerprint == intent.controller_owner)

    def _actuate_zero_cost_pool(self, pool_key: str) -> None:
        """Stage one JIT quantum in an independent physical-pool lane."""
        leases: tuple[zero_cost_actuation.IntentLease, ...] = ()
        handled_intent_keys: set[str] = set()
        preflights: _ReservedFillPhysicalPreflightBatch | None = None
        materialization_committed = False
        window_released = False
        ambiguous_error: Exception | None = None
        actuation_error: BaseException | None = None
        try:
            leases = self._zero_cost_actuation_repository.lease_batch(
                service_name=self._service_name,
                pool_key=pool_key,
                owner=self._zero_cost_actuation_executor_id,
                lease_seconds=_ZERO_COST_ACTUATION_LEASE_SECONDS,
                max_leases=_ZERO_COST_ACTUATION_QUANTUM)
            if not leases:
                return
            actionable: list[zero_cost_actuation.IntentLease] = []
            for lease in leases:
                if self._zero_cost_actuation_authority_current(lease.intent):
                    actionable.append(lease)
                    continue
                self._zero_cost_actuation_repository.release_retryable(
                    lease, 'controller_authority_unavailable')
                handled_intent_keys.add(lease.intent.idempotency_key)
            if not actionable:
                return
            with provider_phase.try_provider_phase(
                    provider_phase.ProviderPhaseMode.V2_FENCED) as admission:
                preflights = self._start_reserved_fill_physical_preflights(
                    tuple(lease.intent for lease in actionable), admission,
                    self._workspace)
                launch_error: BaseException | None = None
                try:
                    for lease in actionable:
                        intent = lease.intent
                        preflight = preflights.preflights[(
                            intent.allowed_locations[0].region,
                            intent.physical_cluster_uid)]
                        if preflight.error is not None:
                            raise preflight.error
                    for chunk_start in range(0, len(actionable),
                                             _ZERO_COST_ACTUATION_QUANTUM):
                        chunk = actionable[chunk_start:chunk_start +
                                           _ZERO_COST_ACTUATION_QUANTUM]
                        with self.lock:
                            # Refresh both the durable graph and replica-ID set
                            # immediately before this JIT quantum commits.
                            infos = serve_state.get_replica_infos(
                                self._service_name)
                            used_replica_ids = {
                                info.replica_id for info in infos
                            }
                            for lease in chunk:
                                intent = lease.intent
                                intent_key = intent.idempotency_key
                                if not self._zero_cost_actuation_authority_current(
                                        intent):
                                    (self._zero_cost_actuation_repository.
                                     release_retryable(
                                         lease, 'controller_authority_changed'))
                                    handled_intent_keys.add(intent_key)
                                    continue
                                result = None
                                try:
                                    result = self._scale_up_one_locked(
                                        self._reserved_fill_override(intent),
                                        used_replica_ids,
                                        infos,
                                        paid_launch_allowed=False,
                                        provider_phase_admission=admission,
                                        require_preinitialized_physical_fence=
                                        True,
                                        zero_cost_actuation_lease=lease)
                                except (reserved_fill_admission.
                                        AdmissionAmbiguousError) as error:
                                    # Preserve only this exact maybe-committed
                                    # graph. Later quantum members remain
                                    # independently actionable.
                                    materialization_committed = True
                                    window_released = True
                                    handled_intent_keys.add(intent_key)
                                    if ambiguous_error is None:
                                        ambiguous_error = error
                                    logger.warning(
                                        'Zero-cost admission for pool %s intent '
                                        '%s is ambiguous; preserving it and '
                                        'continuing the JIT quantum: %s',
                                        pool_key, intent_key,
                                        common_utils.format_exception(error))
                                    continue
                                except (exceptions.ProviderPhaseBusyError,
                                        exceptions.ProviderPhaseTimeoutError,
                                        exceptions.
                                        KubernetesPhysicalClusterFenceBusyError,
                                        TimeoutError) as error:
                                    (self._zero_cost_actuation_repository.
                                     release_retryable(lease,
                                                       type(error).__name__))
                                    handled_intent_keys.add(intent_key)
                                    continue
                                except (exceptions.
                                        KubernetesPhysicalClusterIdentityError
                                       ) as error:
                                    transitioned = (
                                        self._zero_cost_actuation_repository.
                                        terminate(
                                            lease,
                                            'physical_cluster_identity_changed')
                                    )
                                    window_released |= transitioned
                                    handled_intent_keys.add(intent_key)
                                    logger.info(
                                        'Terminalized zero-cost actuation for '
                                        'pool %s intent %s: %s', pool_key,
                                        intent_key,
                                        common_utils.format_exception(error))
                                    continue
                                except BaseException as error:  # noqa: ASYNC103  # pylint: disable=broad-except
                                    if not isinstance(error, Exception):
                                        raise
                                    try:
                                        (self._zero_cost_actuation_repository.
                                         release_retryable(
                                             lease,
                                             type(error).__name__))
                                        handled_intent_keys.add(intent_key)
                                    except Exception:  # pylint: disable=broad-except
                                        logger.exception(
                                            'Could not release zero-cost '
                                            'actuation lease for pool %s intent '
                                            '%s.', pool_key, intent_key)
                                    logger.exception(
                                        'Zero-cost actuation failed for pool %s '
                                        'intent %s: %s', pool_key, intent_key,
                                        common_utils.format_exception(error))
                                    continue
                                if result is None:
                                    (self._zero_cost_actuation_repository.
                                     release_retryable(
                                         lease, 'replica_commit_deferred'))
                                    handled_intent_keys.add(intent_key)
                                    continue
                                materialization_committed = True
                                window_released = True
                                handled_intent_keys.add(intent_key)
                                used_replica_ids.add(result.replica_id)
                except BaseException as error:
                    launch_error = error
                    raise
                finally:
                    try:
                        self._release_reserved_fill_physical_preflights(
                            preflights)
                    except BaseException as release_error:
                        if (launch_error is not None and
                                not isinstance(launch_error, Exception)):
                            raise launch_error from release_error
                        raise
                    preflights = None
            if ambiguous_error is not None:
                # Finish every independently actionable quantum member, then
                # enter the established ambiguity path exactly once. It owns
                # the sole reconciliation signal and preserves BaseException
                # precedence without retrying the provider effect.
                raise ambiguous_error
            if window_released:
                self._notify_scale_reconciliation()
        except BaseException as error:  # pylint: disable=broad-except
            actuation_error = error
            unresolved = tuple(
                lease for lease in leases
                if lease.intent.idempotency_key not in handled_intent_keys)
            if materialization_committed:
                # Preserve possibly committed evidence for restart hydration.
                formatted_error = (common_utils.format_exception(error)
                                   if isinstance(error, (Exception, SystemExit,
                                                         KeyboardInterrupt))
                                   else repr(error))
                logger.warning(
                    'Zero-cost admission for pool %s is ambiguous; '
                    'preserving its intent without cleanup: %s', pool_key,
                    formatted_error)
                try:
                    self._notify_scale_reconciliation()
                except BaseException as signal_error:  # pylint: disable=broad-except
                    if not isinstance(error, Exception):
                        raise error from signal_error
                    if not isinstance(signal_error, Exception):
                        raise
                    logger.exception(
                        'Could not signal reconciliation after '
                        'committed admission for pool %s.', pool_key)
                if not isinstance(error, Exception):
                    raise
            elif isinstance(error,
                            (exceptions.ProviderPhaseBusyError,
                             exceptions.ProviderPhaseTimeoutError,
                             exceptions.KubernetesPhysicalClusterFenceBusyError,
                             TimeoutError)):
                for lease in unresolved:
                    self._zero_cost_actuation_repository.release_retryable(
                        lease,
                        type(error).__name__)
            elif isinstance(error,
                            exceptions.KubernetesPhysicalClusterIdentityError):
                for lease in unresolved:
                    transitioned = (
                        self._zero_cost_actuation_repository.terminate(
                            lease, 'physical_cluster_identity_changed'))
                    window_released |= transitioned
                logger.info('Terminalized zero-cost actuation for pool %s: %s',
                            pool_key, common_utils.format_exception(error))
            elif not isinstance(error, Exception):
                raise
            else:
                for lease in unresolved:
                    try:
                        self._zero_cost_actuation_repository.release_retryable(
                            lease,
                            type(error).__name__)
                    except Exception:  # pylint: disable=broad-except
                        logger.exception(
                            'Could not release zero-cost actuation '
                            'lease for pool %s.', pool_key)
                logger.exception('Zero-cost actuation failed for pool %s: %s',
                                 pool_key, common_utils.format_exception(error))
            if window_released and not materialization_committed:
                self._notify_scale_reconciliation()
        finally:
            if preflights is not None:
                try:
                    self._release_reserved_fill_physical_preflights(preflights)
                except BaseException as error:  # pylint: disable=broad-except
                    if (actuation_error is not None and
                            not isinstance(actuation_error, Exception)):
                        raise actuation_error from error
                    if not isinstance(error, Exception):
                        raise
                    if not materialization_committed:
                        raise
                    logger.warning(
                        'Committed zero-cost admission for pool %s is ambiguous '
                        'after physical-preflight release failed: %s', pool_key,
                        common_utils.format_exception(error))
            # Release this lane before waking the dispatcher.  Signalling while
            # the current SafeThread remains registered races with the
            # dispatcher's is_alive() filter: it can consume the wakeup, retain
            # this almost-finished thread, and impose the one-second poll delay
            # before the next JIT quantum. All provider and graph work is
            # complete at this point, so removing only our exact thread hands
            # actuation ownership to the next quantum without overlapping work.
            if (window_released and
                    len(leases) == _ZERO_COST_ACTUATION_QUANTUM):
                current_thread = threading.current_thread()
                with self._zero_cost_actuation_lane_lock:
                    if (self._zero_cost_actuation_lanes.get(pool_key)
                            is current_thread):
                        del self._zero_cost_actuation_lanes[pool_key]
                self._zero_cost_actuation_event.set()

    def _zero_cost_actuation_dispatcher(self) -> None:
        """Supervise one independent executor lane per physical pool."""
        while not self._manager_daemon_should_stop():
            # Clear before the durable scan.  A publication before this point
            # is visible to the scan; a publication or completed batch after
            # this point leaves the event set for the wait below.  Clearing
            # after the scan would lose that wakeup and impose a poll delay.
            self._zero_cost_actuation_event.clear()
            mode = zero_cost_actuation.get_service_mode(self._service_name)
            self._reserved_fill_actuation_mode = mode
            pool_keys: tuple[str, ...] = ()
            if mode is zero_cost_actuation.ActuationMode.DURABLE_INTENT:
                pool_keys = (
                    self._zero_cost_actuation_repository.actionable_pool_keys(
                        service_name=self._service_name))
            with self._zero_cost_actuation_lane_lock:
                self._zero_cost_actuation_lanes = {
                    key: worker
                    for key, worker in self._zero_cost_actuation_lanes.items()
                    if worker.is_alive()
                }
                for pool_key in pool_keys:
                    if pool_key in self._zero_cost_actuation_lanes:
                        continue
                    worker = thread_utils.SafeThread(
                        target=self._actuate_zero_cost_pool,
                        args=(pool_key,),
                        name=
                        ('replica-zero-cost-actuation-'
                         f'{hashlib.sha256(pool_key.encode()).hexdigest()[:8]}'
                        ),
                        daemon=True)
                    self._zero_cost_actuation_lanes[pool_key] = worker
                    worker.start()
            self._zero_cost_actuation_event.wait(
                _ZERO_COST_ACTUATION_POLL_SECONDS)

    def accept_reserved_fill(
        self, plan: reserved_fill_planner.FillPlan
    ) -> reserved_fill_planner.FillCommitResult:
        """Commit a typed v2 fill plan to the durable actuation queue."""
        if not isinstance(plan, reserved_fill_planner.FillPlan):
            raise ValueError('Reserved-fill admission requires a FillPlan.')
        # Frozen dataclasses prevent ordinary mutation. Re-run their validators
        # at this trust boundary as defense against object.__setattr__ callers.
        for intent in plan.intents:
            intent.__post_init__()
        plan.__post_init__()
        if not plan.intents:
            return reserved_fill_planner.FillCommitResult(
                accepted=(), deferred=(), authority_current=True)

        actuation_mode = self._reserved_fill_actuation_mode
        if actuation_mode is not zero_cost_actuation.ActuationMode.DURABLE_INTENT:
            return self._reserved_fill_commit_result(
                plan, [],
                self._reserved_fill_deferred_tail(
                    plan, 0,
                    reserved_fill_planner.DeferredFillReason.LOST_OWNER,
                    'durable reserved-fill actuation is unavailable'),
                authority_current=False)
        # Publication owns no provider phase, physical-cluster call,
        # replica ID, request, or worker thread.  PostgreSQL serializes
        # the service ceiling and records every accepted grant first.
        with self.lock:
            authority_failure = (
                self._reserved_fill_manager_authority_failure(plan))
            if authority_failure is not None:
                reason, detail = authority_failure
                return self._reserved_fill_commit_result(
                    plan, [],
                    self._reserved_fill_deferred_tail(plan, 0, reason, detail),
                    authority_current=False)
            controller_authority = (self._ordinary_launch_binding_authority)
            if controller_authority is None:
                return self._reserved_fill_commit_result(
                    plan, [],
                    self._reserved_fill_deferred_tail(
                        plan, 0,
                        reserved_fill_planner.DeferredFillReason.LOST_OWNER,
                        'durable controller authority is unavailable'),
                    authority_current=False)
            controller_incarnation = (
                controller_authority.controller_incarnation)
            controller_owner_epoch = (
                controller_authority.controller_owner_epoch)
            service_hash = self._service_hash
            controller_owner = self._controller_owner
            assert isinstance(service_hash, str) and service_hash
            assert controller_owner is not None
            try:
                maximum = self._reserved_fill_max_capacity_locked()
            except Exception as error:  # pylint: disable=broad-except
                logger.warning(
                    'Durable reserved-fill admission could not establish '
                    'the service ceiling: %s',
                    common_utils.format_exception(error))
                return self._reserved_fill_commit_result(
                    plan, [],
                    self._reserved_fill_deferred_tail(
                        plan, 0, reserved_fill_planner.DeferredFillReason.
                        SUPERSEDED_POLICY,
                        'the service ceiling could not be established'),
                    authority_current=False)
        try:
            # Durable intent insertion is the physical-slot admission
            # boundary. Serialize it with broker debit scans so a new grant
            # is either visible to the next round or lands after that round;
            # it cannot disappear into the scan-to-publish window.
            broker_lock = locks.get_lock(
                serve_constants.RESERVED_FILL_BROKER_LOCK_ID)
            with broker_lock.acquire(blocking=True):
                current_allocation = (
                    reserved_fill_allocation.ReservedFillAllocationRepository(
                    ).read_current(self._service_name, service_hash,
                                   controller_owner))
                if (current_allocation is None or
                        current_allocation.allocation_generation
                        != plan.allocation_generation or
                        current_allocation.allocation_input_sha256
                        != plan.allocation_input_sha256 or
                        current_allocation.allocation_claim_generation
                        != plan.allocation_claim_generation):
                    return self._reserved_fill_commit_result(
                        plan, [],
                        self._reserved_fill_deferred_tail(
                            plan, 0, reserved_fill_planner.DeferredFillReason.
                            CHANGED_EPOCH,
                            'the authenticated allocation changed before '
                            'durable grant admission'),
                        authority_current=False)
                receipt = self._zero_cost_actuation_repository.grant_plan(
                    self._service_name,
                    plan,
                    max_capacity=maximum,
                    expected_controller_incarnation=(controller_incarnation),
                    expected_controller_owner_epoch=controller_owner_epoch)
            if receipt.accepted:
                self._zero_cost_actuation_event.set()
            return receipt
        except (zero_cost_actuation.ZeroCostActuationError,
                reserved_fill_allocation.ReservedFillAllocationError,
                ValueError) as error:
            logger.warning('Durable reserved-fill grant failed closed: %s',
                           common_utils.format_exception(error))
            return self._reserved_fill_commit_result(
                plan, [],
                self._reserved_fill_deferred_tail(
                    plan, 0,
                    reserved_fill_planner.DeferredFillReason.LOST_OWNER,
                    'durable actuation authority changed before grant'),
                authority_current=False)

    def scale_up(self,
                 resources_override: dict[str, Any] | None = None) -> None:
        if _is_protocol_v2_fill_override(resources_override):
            # Typed protocol-v2 fills are admitted only through
            # ``accept_reserved_fill()``.  Keeping their complete plan and
            # receipt boundary intact prevents this compatibility method from
            # bypassing ordinary-demand serialization.
            raise ValueError('Protocol-v2 fill requires typed plan admission.')
        self.scale_up_batch([resources_override])

    def _enqueue_ambiguous_paid_phase_a_recovery(
            self,
            prepared_paid_launches: Iterable[_PreparedPaidLaunch]) -> None:
        """Coalesce exact unknown Phase-A outcomes without database I/O."""
        identities = tuple(
            _AmbiguousPaidPhaseAIdentity(
                prepared.candidate.replica_id,
                prepared.candidate.replica_info.replica_record_id)
            for prepared in prepared_paid_launches)
        if not identities:
            return
        with self._ambiguous_paid_phase_a_lock:
            for identity in identities:
                self._ambiguous_paid_phase_a_recoveries.setdefault(
                    identity, _AmbiguousPaidPhaseARecovery())
        # The supervised refresher drains this queue only after its locked
        # refresh pass returns. Waking it here never performs database or
        # provider I/O under the scale-up manager lock.
        self._legacy_mutation_runtime_state().launch_completion_event.set()

    def _ambiguous_paid_phase_a_is_pending(self, info: ReplicaInfo) -> bool:
        identity = _AmbiguousPaidPhaseAIdentity(info.replica_id,
                                                info.replica_record_id)
        with self._ambiguous_paid_phase_a_lock:
            return identity in self._ambiguous_paid_phase_a_recoveries

    def _resolve_ambiguous_paid_phase_a(
            self, identity: _AmbiguousPaidPhaseAIdentity) -> None:
        with self._ambiguous_paid_phase_a_lock:
            self._ambiguous_paid_phase_a_recoveries.pop(identity, None)

    def _retry_ambiguous_paid_phase_a(self,
                                      identity: _AmbiguousPaidPhaseAIdentity,
                                      error: Exception) -> None:
        with self._ambiguous_paid_phase_a_lock:
            recovery = self._ambiguous_paid_phase_a_recoveries.get(identity)
            if recovery is None:
                return
            recovery.attempts += 1
            delay = min(
                _NON_POOL_RECONCILIATION_RETRY_BASE_SECONDS *
                2**min(recovery.attempts - 1, 30),
                _NON_POOL_RECONCILIATION_RETRY_MAX_SECONDS)
            recovery.retry_at = time.monotonic() + delay
        logger.warning(
            'Exact ambiguous paid Phase-A recovery for replica %s failed; '
            'retrying in %.1f seconds: %s', identity.replica_id, delay,
            common_utils.format_exception(error))

    def _reconcile_ambiguous_paid_phase_a_outcomes(self) -> None:
        """Resolve exact unknown Phase-A commits outside the manager lock."""
        now = time.monotonic()
        with self._ambiguous_paid_phase_a_lock:
            identities = tuple(identity for identity, recovery in
                               self._ambiguous_paid_phase_a_recoveries.items()
                               if recovery.retry_at <= now)
        for identity in identities:
            try:
                authority = self._ordinary_launch_binding_authority
                if (authority is None or
                        not authority.retained_non_pool_settlement_allowed):
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Exact ambiguous paid Phase-A recovery has no retained '
                        'generic settlement authority.')
                retirement = (ordinary_launch_binding.
                              retire_pre_admission_non_pool_launch_intent(
                                  authority, identity.replica_id,
                                  identity.replica_record_id))
                if retirement.disposition in (
                        ordinary_launch_binding.
                        PreAdmissionRetirementDisposition.RETIRED,
                        ordinary_launch_binding.
                        PreAdmissionRetirementDisposition.ABSENT):
                    self._resolve_ambiguous_paid_phase_a(identity)
                    self._notify_scale_reconciliation()
                    continue
                if retirement.disposition is not (
                        ordinary_launch_binding.
                        PreAdmissionRetirementDisposition.ASSOCIATED):
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Exact ambiguous paid Phase-A retirement returned an '
                        'unknown disposition.')

                # Admission won the exact row race. It is no longer safe to
                # retire or create a successor. Adopt only its durable request
                # identity; any incomplete projection stays on this supervised
                # fail-closed retry lane indefinitely.
                reduction = request_postgres.inspect_bound_ordinary_launch(
                    self._service_name, identity.replica_id,
                    identity.replica_record_id)
                info = serve_state.get_replica_info_from_id(
                    self._service_name, identity.replica_id)
                if (reduction is None or info is None or
                        info.replica_record_id != identity.replica_record_id):
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Associated ambiguous paid Phase-A identity lost its '
                        'exact durable projection.')
                bound_context = reduction.context
                if (not isinstance(
                        bound_context,
                        ordinary_launch_binding.BoundNonPoolLaunchContext) or
                        bound_context.service_name != self._service_name or
                        bound_context.replica_id != identity.replica_id or
                        str(bound_context.replica_record_id)
                        != identity.replica_record_id):
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Associated ambiguous paid Phase-A projection returned '
                        'a mismatched request identity.')
                classification = _bound_projection_classification(reduction)
                if classification not in ('ADOPT_ACTIVE', 'WAIT_QUIESCENCE'):
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        'Associated ambiguous paid Phase-A request requires '
                        'supervised fail-closed recovery: '
                        f'{classification!r}.')

                # The durable decisions above happen without self.lock. Hold
                # it only for process-local exact-worker publication. The
                # ordinary refresher reserves P and starts this adopter later.
                with self.lock:
                    runtime = self._legacy_mutation_runtime_state()
                    existing = runtime.launch_thread_pool.get(
                        identity.replica_id)
                    if existing is not None:
                        if (getattr(existing, 'replica_record_id', None)
                                != identity.replica_record_id):
                            raise _BoundOrdinaryLaunchUnresolvedError(
                                'Associated ambiguous paid Phase-A identity '
                                'collided with a different local worker.')
                    elif not self._install_bound_launch_adopter(
                            info, bound_context, start=False):
                        raise _BoundOrdinaryLaunchUnresolvedError(
                            'Associated ambiguous paid Phase-A adopter could '
                            'not be registered.')
                self._resolve_ambiguous_paid_phase_a(identity)
                self._legacy_mutation_runtime_state(
                ).launch_completion_event.set()
            except Exception as error:  # pylint: disable=broad-except
                self._retry_ambiguous_paid_phase_a(identity, error)

    @staticmethod
    def _remove_prepared_paid_info(existing_replica_infos: list['ReplicaInfo'] |
                                   None, prepared: _PreparedPaidLaunch) -> None:
        """Remove one uncommitted staged row from the mutable wave snapshot."""
        if existing_replica_infos is None:
            return
        candidate = prepared.candidate
        existing_replica_infos[:] = [
            info for info in existing_replica_infos if not (
                info.replica_id == candidate.replica_id and info.
                replica_record_id == candidate.replica_info.replica_record_id)
        ]

    def _finalize_prepared_paid_launches(
        self,
        prepared_paid_launches: list[_PreparedPaidLaunch],
        paid_location_launch_budget: paid_capacity.LaunchBudget | None,
        existing_replica_infos: list['ReplicaInfo'] | None,
    ) -> list[_ReplicaLaunchResult]:
        """Atomically admit and then publish one frozen paid launch wave."""
        if not prepared_paid_launches:
            return []
        if paid_location_launch_budget is None:
            raise RuntimeError('Prepared paid launches require one frozen '
                               'launch budget.')

        def _release(prepared: _PreparedPaidLaunch) -> None:
            placer = self._spot_placer
            if placer is not None:
                placer.release_retry(prepared.candidate.location)
            self._remove_prepared_paid_info(existing_replica_infos, prepared)

        def _discard_ambiguous() -> None:
            # Queue exact identities before dropping the only frozen local
            # workers. The post-lock supervised reconciler proves whether
            # Phase A committed; this stack must never infer from the error.
            self._enqueue_ambiguous_paid_phase_a_recovery(
                prepared_paid_launches)
            for prepared in prepared_paid_launches:
                _release(prepared)
            self._persist_spot_placement_state_if_dirty()

        candidates = tuple(
            prepared.candidate for prepared in prepared_paid_launches)
        try:
            batch_result = paid_capacity.try_persist_claim_batch(
                service_name=self._service_name,
                service_hash=self._service_hash,
                controller_owner=self._controller_owner,
                candidates=candidates,
                budget=paid_location_launch_budget)
        except capacity_admission.CapacityAdmissionError as error:
            for prepared in prepared_paid_launches:
                _release(prepared)
            self._persist_spot_placement_state_if_dirty()
            logger.info(
                'Deferring paid demand launch wave because its ordered '
                'capacity authority changed: %s',
                common_utils.format_exception(error))
            return []
        except BaseException:  # pylint: disable=broad-exception-caught
            _discard_ambiguous()
            raise

        expected_identities = tuple(
            (candidate.replica_id, candidate.replica_info.replica_record_id)
            for candidate in candidates)
        try:
            members = tuple(batch_result.members)
            result_identities = tuple(
                (member.replica_id, member.replica_record_id)
                for member in members)
        except BaseException as error:  # pylint: disable=broad-exception-caught
            _discard_ambiguous()
            if not isinstance(error, Exception):
                raise
            raise RuntimeError(
                'Atomic paid launch admission returned an invalid result.'
            ) from error
        if result_identities != expected_identities:
            _discard_ambiguous()
            raise RuntimeError('Atomic paid launch admission returned a '
                               'mismatched member sequence.')

        try:
            claim_results = tuple(member.claim_result for member in members)
        except BaseException as error:  # pylint: disable=broad-exception-caught
            _discard_ambiguous()
            if not isinstance(error, Exception):
                raise
            raise RuntimeError(
                'Atomic paid launch admission returned an invalid result.'
            ) from error

        valid_results = frozenset({
            paid_capacity.ClaimResult.ACQUIRED,
            paid_capacity.ClaimResult.SATURATED,
            paid_capacity.ClaimResult.SERVICE_SATURATED,
            paid_capacity.ClaimResult.FEEDBACK_PENDING,
            paid_capacity.ClaimResult.HIGHER_PRIORITY_WAITING,
            paid_capacity.ClaimResult.OWNERSHIP_LOST,
        })
        legacy_runtime = self._legacy_mutation_runtime_state()
        invalid_members = tuple(
            (member, claim_result)
            for member, claim_result in zip(members, claim_results, strict=True)
            if (not isinstance(claim_result, paid_capacity.ClaimResult) or
                claim_result not in valid_results))
        ownership_lost_count = sum(
            claim_result is paid_capacity.ClaimResult.OWNERSHIP_LOST
            for claim_result in claim_results)
        collided_replica_id = next((
            member.replica_id
            for member, claim_result in zip(members, claim_results, strict=True)
            if claim_result is paid_capacity.ClaimResult.ACQUIRED and
            member.replica_id in legacy_runtime.launch_thread_pool), None)
        if invalid_members:
            _discard_ambiguous()
            raise RuntimeError(
                'Globally managed paid launch batch returned an invalid '
                f'claim result: {invalid_members[0][1]!r}.')
        if 0 < ownership_lost_count < len(members):
            # The state transaction returns OWNERSHIP_LOST uniformly before
            # admitting any member. A mixed receipt cannot be interpreted as
            # a sparse commit, so reconcile every exact candidate before
            # publishing even an apparently ACQUIRED worker.
            _discard_ambiguous()
            raise RuntimeError(
                'Atomic paid launch admission returned a mixed ownership-lost '
                'receipt.')
        if collided_replica_id is not None:
            # Validate the complete durable receipt and every local target
            # before publishing the first worker. A late collision can
            # otherwise strand an earlier ACQUIRED member in a half-published
            # process-local wave.
            _discard_ambiguous()
            raise RuntimeError(
                'Committed paid launch collided with an existing worker for '
                f'replica {collided_replica_id}.')

        committed: list[_ReplicaLaunchResult] = []
        rejected = False
        ownership_lost = False
        try:
            for prepared, claim_result in zip(prepared_paid_launches,
                                              claim_results,
                                              strict=True):
                location = prepared.candidate.location
                if claim_result is paid_capacity.ClaimResult.ACQUIRED:
                    replica_id = prepared.candidate.replica_id
                    legacy_runtime.launch_thread_pool[replica_id] = (
                        prepared.launch_thread)
                    committed.append(prepared.launch_result)
                    continue

                rejected = True
                _release(prepared)
                if claim_result is paid_capacity.ClaimResult.FEEDBACK_PENDING:
                    paid_capacity.defer_for_feedback(
                        paid_location_launch_budget, location)
                elif claim_result is paid_capacity.ClaimResult.SATURATED:
                    paid_capacity.exhaust(paid_location_launch_budget, location)
                elif (claim_result
                      is paid_capacity.ClaimResult.SERVICE_SATURATED):
                    paid_capacity.exhaust_service(paid_location_launch_budget)
                elif (claim_result
                      is paid_capacity.ClaimResult.HIGHER_PRIORITY_WAITING):
                    paid_capacity.defer_for_priority(
                        paid_location_launch_budget, location)
                elif claim_result is paid_capacity.ClaimResult.OWNERSHIP_LOST:
                    ownership_lost = True

            if rejected:
                self._persist_spot_placement_state_if_dirty()
            if committed:
                # Registration intentionally follows the all-or-subset durable
                # commit. The shared refresher remains the sole owner of starts.
                legacy_runtime.launch_completion_event.set()
            if ownership_lost:
                raise RuntimeError(
                    f'Service {self._service_name!r} controller ownership '
                    'changed while claiming paid capacity.')
        except BaseException:  # pylint: disable=broad-exception-caught
            # The complete receipt was validated, so Phase A is no longer
            # ambiguous and must not be retired. If publication is interrupted,
            # exact workers already registered remain valid; after self.lock
            # unwinds, the ordinary durable-row adopter publishes every missing
            # committed member. Process death uses the same startup recovery.
            legacy_runtime.launch_completion_event.set()
            raise
        return committed

    def scale_up_batch(
        self,
        resources_overrides: list[dict[str, Any] | None],
        expected_version: int | None = None,
        launch_priority: int = (serve_constants.LB_REQUEST_PRIORITY_MIN),
        paid_launch_authority: capacity_admission.PaidLaunchAuthority |
        None = None,
        paid_launch_allowed: bool = True,
    ) -> list[_ReplicaLaunchResult]:
        """Enqueue a batch of replica launches under one manager lock.

        The manager lock is held by the readiness-probe round for tens of
        seconds per round on large fleets, so per-replica `scale_up` calls
        (one lock acquisition each) trickle through the short gaps between
        rounds: measured live at a 1000-target / ~340-replica fleet, launch
        enqueueing was the scaling bottleneck at ~100 replicas per several
        minutes while the launch budget sat idle.

        Shared zero-cost placement reuses one replica snapshot across the wave.
        The launch path appends each successfully enqueued replica so later
        decisions observe in-wave reservations without querying and unpickling
        all existing rows once per launch. Protocol-v2 reserved fill is not a
        batch dictionary: its sole public admission is the typed, immutable
        ``accept_reserved_fill()`` plan and receipt boundary.
        """
        untyped_v2_count = sum(
            _is_protocol_v2_fill_override(resources_override)
            for resources_override in resources_overrides)
        if untyped_v2_count:
            noun = 'entry' if untyped_v2_count == 1 else 'entries'
            verb = 'requires' if untyped_v2_count == 1 else 'require'
            self._log_fill_skip(
                f'{untyped_v2_count} protocol-v2 batch {noun} {verb} typed '
                'plan admission')
            resources_overrides = [
                resources_override for resources_override in resources_overrides
                if not _is_protocol_v2_fill_override(resources_override)
            ]
            if not resources_overrides:
                return []
        with self.lock:
            if self._update_recovery_required:
                return []
            if self._spot_placer is not None:
                self._spot_placer.refresh_workspace_policy()
            needs_reservation = (
                self._batch_needs_placement_snapshot(resources_overrides) and
                self._uses_shared_zero_cost_demand_budget())
            batch_kwargs: dict[str, Any] = {}
            if launch_priority != serve_constants.LB_REQUEST_PRIORITY_MIN:
                batch_kwargs['launch_priority'] = launch_priority
            if paid_launch_authority is not None:
                batch_kwargs['paid_launch_authority'] = paid_launch_authority
            if not paid_launch_allowed:
                batch_kwargs['paid_launch_allowed'] = False
            if not needs_reservation:
                return self._scale_up_batch_locked(resources_overrides,
                                                   expected_version,
                                                   **batch_kwargs)
            try:
                lock = locks.get_lock(
                    serve_constants.DEMAND_CAPACITY_RESERVATION_LOCK_ID)
                with lock.acquire(blocking=False):
                    return self._scale_up_batch_locked(resources_overrides,
                                                       expected_version,
                                                       **batch_kwargs)
            except locks.LockTimeout:
                logger.info(
                    'Deferring demand scale-up because another service is '
                    'reserving shared zero-cost capacity.')
                return []

    def _scale_up_batch_locked(
        self,
        resources_overrides: list[dict[str, Any] | None],
        expected_version: int | None = None,
        launch_priority: int = (serve_constants.LB_REQUEST_PRIORITY_MIN),
        paid_launch_authority: capacity_admission.PaidLaunchAuthority |
        None = None,
        paid_launch_allowed: bool = True,
    ) -> list[_ReplicaLaunchResult]:
        """Persist one physical batch while any shared demand lock is held."""
        if self._update_recovery_required:
            return []
        batch_version = self.latest_version
        if (expected_version is not None and expected_version != batch_version):
            logger.info('Discarding stale physical scale-up batch for '
                        f'version {expected_version}; manager is at version '
                        f'{batch_version}.')
            return []
        existing_replica_infos = None
        infos_by_service = None
        needs_placement_snapshot = self._batch_needs_placement_snapshot(
            resources_overrides)
        uses_shared_capacity = (needs_placement_snapshot and
                                self._uses_shared_zero_cost_demand_budget())
        if uses_shared_capacity:
            infos_by_service = serve_state.get_replica_infos_grouped()
            existing_replica_infos = infos_by_service.get(
                self._service_name, [])
        elif needs_placement_snapshot:
            existing_replica_infos = serve_state.get_replica_infos(
                self._service_name)
        # One id-only snapshot for the whole batch: the collision guard in
        # `_scale_up_one_locked` used to point-read (and unpickle) one row
        # per replica launched, K reads per wave that never fire in
        # steady-state.
        if existing_replica_infos is not None:
            used_replica_ids = {
                info.replica_id for info in existing_replica_infos
            }
        else:
            used_replica_ids = serve_state.get_replica_ids(self._service_name)
        zero_cost_demand_budget = None
        if existing_replica_infos is not None and infos_by_service is not None:
            capacity_replica_infos = [
                info for infos in infos_by_service.values() for info in infos
            ]
            zero_cost_demand_budget = self._build_zero_cost_demand_budget(
                existing_replica_infos,
                resources_overrides,
                capacity_replica_infos=capacity_replica_infos)
        paid_location_launch_budget = None
        if (existing_replica_infos is not None and
                self._spot_placer is not None):
            paid_location_launch_budget = (paid_capacity.build_launch_budget(
                self._spot_placer,
                workspace=self._workspace,
                existing_replica_infos=existing_replica_infos,
                globally_managed=(self._service_hash is not None),
                service_name=self._service_name,
                service_hash=self._service_hash,
                max_live_paid_gpu_units=(
                    self._max_live_paid_gpu_units_for_version(batch_version)),
                requested_frontier_keys=self._requested_paid_frontier_keys(
                    resources_overrides)))
        deferred_paid_overrides: list[dict[str, Any] | None] = []
        accepted: list[_ReplicaLaunchResult] = []
        prepared_paid_launches: list[_PreparedPaidLaunch] = []
        for resources_override in resources_overrides:
            pending_version = self._pending_version
            if (pending_version is not None and
                    pending_version > batch_version):
                logger.info('Stopping version '
                            f'{batch_version} scale-up batch because version '
                            f'{pending_version} is waiting to be applied.')
                break
            if any(resources_override == deferred
                   for deferred in deferred_paid_overrides):
                continue
            scale_up_kwargs: dict[str, Any] = {}
            if paid_location_launch_budget is not None:
                scale_up_kwargs['paid_location_launch_budget'] = (
                    paid_location_launch_budget)
            if launch_priority != serve_constants.LB_REQUEST_PRIORITY_MIN:
                scale_up_kwargs['launch_priority'] = launch_priority
            if paid_launch_authority is not None:
                scale_up_kwargs['paid_launch_authority'] = paid_launch_authority
            if not paid_launch_allowed:
                scale_up_kwargs['paid_launch_allowed'] = False
            if paid_launch_authority is not None:
                scale_up_kwargs['prepared_paid_launches'] = (
                    prepared_paid_launches)
            stop_sequence_before = (paid_location_launch_budget.stop_sequence
                                    if paid_location_launch_budget is not None
                                    else 0)
            service_remaining_before = (
                paid_location_launch_budget.service_remaining
                if paid_location_launch_budget is not None else None)
            override_before = (None if resources_override is None else
                               dict(resources_override))
            prepared_count_before = len(prepared_paid_launches)
            launch_result = self._scale_up_one_locked(resources_override,
                                                      used_replica_ids,
                                                      existing_replica_infos,
                                                      zero_cost_demand_budget,
                                                      **scale_up_kwargs)
            if (launch_result is not None and
                    len(prepared_paid_launches) == prepared_count_before):
                accepted.append(launch_result)
            if paid_location_launch_budget is None:
                continue
            paid_selection_stopped = (paid_location_launch_budget.stop_sequence
                                      != stop_sequence_before)
            service_exhausted = (service_remaining_before is not None and
                                 service_remaining_before > 0 and
                                 paid_location_launch_budget.service_remaining
                                 == 0)
            if ((launch_result is None and paid_selection_stopped) or
                    service_exhausted):
                # Only suppress later equivalent fresh-paid decisions. A
                # complete pass must still examine different accelerator
                # cards plus reserved-fill and pinned-rebalance overrides.
                deferred_paid_overrides.append(override_before)
        accepted.extend(
            self._finalize_prepared_paid_launches(prepared_paid_launches,
                                                  paid_location_launch_budget,
                                                  existing_replica_infos))
        return accepted

    @with_lock
    def scale_up_to_logical_capacity(
        self,
        target_capacity: int,
        version: int,
        reconcile_generation: int,
        replace_unknown_replica_ids: tuple[int, ...] = (),
        target_capacity_by_accelerator: dict[str, int] | None = None,
        accelerator_shapes: dict[str, int] | None = None,
        launch_budget: int | None = None,
        launch_priority: int = serve_constants.LB_REQUEST_PRIORITY_MIN,
        launch_priority_by_accelerator: dict[str, int] | None = None,
        cold_launch_authority_by_accelerator: dict[str, int] | None = None,
        paid_launch_authority: capacity_admission.PaidLaunchAuthority |
        None = None,
        paid_launch_allowed: bool = True,
    ) -> list[_ReplicaLaunchResult]:
        """Plan and persist complete backend shapes up to a logical target.

        Selection and row persistence share the manager lock and one mutable
        fleet snapshot. Each persisted backend immediately participates in the
        next placement decision, so a single 8-slot choice removes eight slots
        from the shortfall instead of causing eight physical launches.
        """
        if self._update_recovery_required:
            return []
        if self._spot_placer is not None:
            self._spot_placer.refresh_workspace_policy()
        if not self._uses_logical_replicas:
            raise RuntimeError('Logical scale target sent to a physical '
                               'replica service.')
        target_by_accelerator_state = (tuple(
            (str(card), int(value))
            for card, value in target_capacity_by_accelerator.items())
                                       if target_capacity_by_accelerator
                                       is not None else None)
        accelerator_shape_state = (tuple(
            (str(card), int(value))
            for card, value in accelerator_shapes.items())
                                   if accelerator_shapes is not None else None)
        logical_state = self._logical_reconcile_state
        if not self._logical_target_fence_holds(version,
                                                reconcile_generation,
                                                target_capacity,
                                                target_by_accelerator_state,
                                                accelerator_shape_state,
                                                require_fresh_occupancy=False,
                                                logical_state=logical_state):
            logger.info('Discarding stale logical scale-up intent for '
                        f'version {version}, generation '
                        f'{reconcile_generation}.')
            return []
        if launch_budget is not None and launch_budget < 0:
            logger.warning('Discarding logical scale-up with negative launch '
                           f'budget {launch_budget}.')
            return []
        if launch_budget == 0:
            logger.info('Deferring logical scale-up until the current launch '
                        'wave has remaining authority.')
            return []
        snapshot = logical_state.snapshot
        assert snapshot is not None
        # An unknown backend may have recovered while this decision waited for
        # the manager lock. A newer snapshot can safely narrow the bounded
        # replacement set even though the still-current target remains valid.
        if snapshot.generation != reconcile_generation:
            replace_unknown_replica_ids = ()
        else:
            replace_unknown_replica_ids = tuple(
                replica_id for replica_id in replace_unknown_replica_ids
                if replica_id in snapshot.unknown_replica_ids)

        launch_kwargs: dict[str, Any] = {}
        if launch_budget is not None:
            launch_kwargs['launch_budget'] = launch_budget
        if launch_priority != serve_constants.LB_REQUEST_PRIORITY_MIN:
            launch_kwargs['launch_priority'] = launch_priority
        if launch_priority_by_accelerator:
            launch_kwargs['launch_priority_by_accelerator'] = dict(
                launch_priority_by_accelerator)
        if cold_launch_authority_by_accelerator is not None:
            launch_kwargs['cold_launch_authority_by_accelerator'] = dict(
                cold_launch_authority_by_accelerator)
        if paid_launch_authority is not None:
            launch_kwargs['paid_launch_authority'] = paid_launch_authority
        if not paid_launch_allowed:
            launch_kwargs['paid_launch_allowed'] = False
        if not self._uses_shared_zero_cost_demand_budget():
            if target_capacity_by_accelerator is None:
                return self._scale_up_to_logical_capacity_locked(
                    target_capacity, version, reconcile_generation, snapshot,
                    replace_unknown_replica_ids, **launch_kwargs)
            else:
                return self._scale_up_to_logical_capacity_locked(
                    target_capacity, version, reconcile_generation, snapshot,
                    replace_unknown_replica_ids, target_capacity_by_accelerator,
                    accelerator_shapes, **launch_kwargs)
            return
        try:
            lock = locks.get_lock(
                serve_constants.DEMAND_CAPACITY_RESERVATION_LOCK_ID)
            with lock.acquire(blocking=False):
                if target_capacity_by_accelerator is None:
                    return self._scale_up_to_logical_capacity_locked(
                        target_capacity, version, reconcile_generation,
                        snapshot, replace_unknown_replica_ids, **launch_kwargs)
                else:
                    return self._scale_up_to_logical_capacity_locked(
                        target_capacity, version, reconcile_generation,
                        snapshot, replace_unknown_replica_ids,
                        target_capacity_by_accelerator, accelerator_shapes,
                        **launch_kwargs)
        except locks.LockTimeout:
            logger.info('Deferring logical scale-up because another service '
                        'is reserving shared zero-cost capacity.')
            return []

    def _scale_up_to_logical_capacity_locked(
        self,
        target_capacity: int,
        version: int,
        reconcile_generation: int,
        snapshot: LogicalReconcileSnapshot,
        replace_unknown_replica_ids: tuple[int, ...],
        target_capacity_by_accelerator: dict[str, int] | None = None,
        accelerator_shapes: dict[str, int] | None = None,
        launch_budget: int | None = None,
        launch_priority: int = (serve_constants.LB_REQUEST_PRIORITY_MIN),
        launch_priority_by_accelerator: dict[str, int] | None = None,
        cold_launch_authority_by_accelerator: dict[str, int] | None = None,
        paid_launch_authority: capacity_admission.PaidLaunchAuthority |
        None = None,
        paid_launch_allowed: bool = True,
    ) -> list[_ReplicaLaunchResult]:
        """Persist complete shapes while the global demand lock is held."""

        uses_shared_capacity = self._uses_shared_zero_cost_demand_budget()
        infos_by_service = None
        if uses_shared_capacity:
            infos_by_service = serve_state.get_replica_infos_grouped()
            existing_replica_infos = infos_by_service.get(
                self._service_name, [])
        else:
            existing_replica_infos = serve_state.get_replica_infos(
                self._service_name)
        used_replica_ids = {info.replica_id for info in existing_replica_infos}
        card_targets = dict(target_capacity_by_accelerator or {})
        shapes = dict(accelerator_shapes or {})
        card_target_state: LogicalAcceleratorState | None = (
            tuple(
                (str(card), int(value)) for card, value in card_targets.items())
            if target_capacity_by_accelerator is not None else None)
        shape_state: LogicalAcceleratorState | None = (tuple(
            (str(card), int(value)) for card, value in shapes.items())
                                                       if accelerator_shapes
                                                       is not None else None)
        if card_targets:
            if (sum(card_targets.values()) != target_capacity or
                    set(card_targets) - set(shapes)):
                logger.warning('Discarding malformed logical exact-card '
                               f'target: total={target_capacity}, '
                               f'by_card={card_targets}, shapes={shapes}.')
                return []
            canonical_by_name = {card.casefold(): card for card in card_targets}
        else:
            canonical_by_name = {}
        paid_authority_left: dict[str, int] | None = None
        if cold_launch_authority_by_accelerator is not None:
            if any(card not in card_targets or isinstance(raw_count, bool) or
                   not isinstance(raw_count, int) or raw_count < 0 or
                   raw_count > card_targets.get(card, 0) for card, raw_count in
                   cold_launch_authority_by_accelerator.items()):
                logger.warning(
                    'Discarding malformed logical paid cold-launch authority: '
                    f'target={card_targets}, authority='
                    f'{cold_launch_authority_by_accelerator}.')
                return []
            paid_authority_left = {
                card: int(cold_launch_authority_by_accelerator.get(card, 0))
                for card in card_targets
            }

        def _replica_card(info: ReplicaInfo) -> str | None:
            # Logical rows persist their exact placement on the replica record.
            # ReplicaInfo.handle() is an explicit cluster-table lookup, not
            # cached record state, and this fleet loop must remain provider-free.
            accelerators = None
            location = info.get_spot_location()
            if location is not None:
                accelerators = location.accelerators
            if not accelerators:
                accelerators = (info.resources_override or
                                {}).get('accelerators')
            if not isinstance(accelerators, dict) or len(accelerators) != 1:
                return None
            raw_card = next(iter(accelerators))
            return canonical_by_name.get(str(raw_card).casefold())

        unknown_predecessors = {
            info.replica_id: info
            for info in existing_replica_infos
            if info.replica_id in replace_unknown_replica_ids and
            info.replica_id in snapshot.unknown_replica_ids and
            info.version == version and not info.is_terminal
        }
        active_replacement_infos = [
            info for info in existing_replica_infos
            if info.version == version and not info.is_terminal and
            info.unknown_capacity_replacement is True
        ]
        replacement_authorizations = (
            serve_state.get_replica_non_pool_launch_authorizations(
                self._service_name,
                [info.replica_id for info in active_replacement_infos]))
        paired_unknown_predecessor_ids: set[int] = set()
        for replacement in active_replacement_infos:
            authorization = replacement_authorizations.get(
                (replacement.replica_id, replacement.replica_record_id))
            try:
                predecessor = (
                    ordinary_launch_binding.
                    decode_replacement_predecessor_authorization(
                        authorization,
                        ordinary_launch_binding.NonPoolLaunchProfileKind.
                        UNKNOWN_CAPACITY_REPLACEMENT,
                        expected_authority=(
                            self._ordinary_launch_binding_authority)))
            except ValueError:
                # Malformed or legacy rows cannot prove that they cover an
                # exact predecessor. They remain committed capacity, but must
                # not suppress a correctly attributed successor.
                continue
            candidate = unknown_predecessors.get(predecessor.replica_id)
            if (candidate is None or candidate.replica_record_id
                    != predecessor.replica_record_id or
                    candidate.version != predecessor.service_version or
                    replacement.planned_capacity != candidate.planned_capacity
                    or _replica_card(replacement) != _replica_card(candidate)):
                continue
            paired_unknown_predecessor_ids.add(candidate.replica_id)
        unpaired_unknown_predecessor_ids = (set(unknown_predecessors) -
                                            paired_unknown_predecessor_ids)

        def _committed_capacity(
                capacity_snapshot: LogicalReconcileSnapshot) -> int:
            committed = 0
            for info in existing_replica_infos:
                if info.is_terminal or info.version != version:
                    continue
                if info.status_property.is_scale_down is True:
                    continue
                planned = int(info.planned_capacity)
                if info.replica_id in replace_unknown_replica_ids:
                    # A bounded degraded-recovery decision explicitly
                    # overlaps this uncertain backend without terminating it.
                    continue
                observed = capacity_snapshot.observed_slots_by_replica_id.get(
                    info.replica_id)
                if (info.is_ready and observed is not None and info.replica_id
                        not in capacity_snapshot.unknown_replica_ids):
                    if (observed <= 0 and info.unknown_capacity_replacement):
                        # A durably attributed replacement is the one bounded
                        # overlap wave for this degradation incident. Keep its
                        # planned pin until it proves positive capacity; zero
                        # must not recursively authorize another full wave.
                        committed += planned
                    else:
                        committed += min(planned, max(0, observed))
                else:
                    # Pending and temporarily unknown rows keep their durable
                    # pin for duplicate-launch suppression.
                    committed += planned
            return committed

        def _committed_by_card(
                capacity_snapshot: LogicalReconcileSnapshot) -> dict[str, int]:
            if not card_targets:
                return {}
            committed_by_card = {card: 0 for card in card_targets}
            for info in existing_replica_infos:
                if (info.is_terminal or info.version != version or
                        info.status_property.is_scale_down is True or
                        info.replica_id in replace_unknown_replica_ids):
                    continue
                card = _replica_card(info)
                if card is None:
                    continue
                planned = int(info.planned_capacity)
                observed = capacity_snapshot.observed_slots_by_replica_id.get(
                    info.replica_id)
                if (info.is_ready and observed is not None and info.replica_id
                        not in capacity_snapshot.unknown_replica_ids):
                    if (observed <= 0 and info.unknown_capacity_replacement):
                        width = planned
                    else:
                        width = min(planned, max(0, observed))
                else:
                    width = planned
                committed_by_card[card] += width
            return committed_by_card

        committed = _committed_capacity(snapshot)
        committed_by_card = _committed_by_card(snapshot)
        zero_cost_demand_budget = None
        if infos_by_service is not None:
            capacity_replica_infos = [
                info for infos in infos_by_service.values() for info in infos
            ]
            required_capacity = max(
                target_capacity - committed,
                sum(
                    max(0, card_target - committed_by_card.get(card, 0))
                    for card, card_target in card_targets.items()))
            if launch_budget is not None:
                required_capacity = min(required_capacity, launch_budget)
            zero_cost_demand_budget = self._build_zero_cost_demand_budget(
                existing_replica_infos, [None],
                demand_count_override=required_capacity,
                capacity_replica_infos=capacity_replica_infos)
        paid_cards = {
            card for card, target in card_targets.items()
            if committed_by_card.get(card, 0) < target and
            (paid_authority_left is None or paid_authority_left.get(card, 0) > 0
            )
        }
        should_build_paid_budget = (self._spot_placer is not None and
                                    (not card_targets or
                                     paid_authority_left is None or paid_cards))
        paid_location_launch_budget = None
        if should_build_paid_budget:
            assert self._spot_placer is not None
            paid_location_launch_budget = paid_capacity.build_launch_budget(
                self._spot_placer,
                workspace=self._workspace,
                existing_replica_infos=existing_replica_infos,
                globally_managed=(self._service_hash is not None),
                service_name=self._service_name,
                service_hash=self._service_hash,
                max_live_paid_gpu_units=(
                    self._max_live_paid_gpu_units_for_version(version)),
                requested_frontier_keys=(None if not card_targets else {
                    (str(card).casefold(),) for card in paid_cards
                }))
        deferred_cards: set[str] = set()
        launched_capacity = 0
        accepted: list[_ReplicaLaunchResult] = []
        prepared_paid_launches: list[_PreparedPaidLaunch] = []
        while True:
            logical_state = self._logical_reconcile_state
            if not self._logical_target_fence_holds(
                    version,
                    reconcile_generation,
                    target_capacity,
                    card_target_state,
                    shape_state,
                    require_exact_generation=bool(replace_unknown_replica_ids),
                    require_fresh_occupancy=False,
                    logical_state=logical_state):
                logger.info('Stopping logical scale-up batch after its '
                            'reconciliation fence advanced.')
                break
            current_snapshot = logical_state.snapshot
            assert current_snapshot is not None
            committed = _committed_capacity(current_snapshot)
            committed_by_card = _committed_by_card(current_snapshot)
            if (launch_budget is not None and
                    launched_capacity >= launch_budget):
                break
            selected_card = None
            if card_targets:
                selected_card = next(
                    (card for card, card_target in card_targets.items()
                     if card not in deferred_cards and
                     committed_by_card.get(card, 0) < card_target), None)
                if selected_card is None:
                    break
            elif committed >= target_capacity:
                break
            resources_override = None
            if selected_card is not None:
                resources_override = {
                    'accelerators': {
                        selected_card: shapes[selected_card]
                    }
                }
            item_paid_launch_allowed = (
                paid_launch_allowed and
                (paid_authority_left is None or selected_card is None or
                 paid_authority_left.get(selected_card, 0) > 0))
            if (item_paid_launch_allowed and
                    self._paid_service_envelope_blocks_launch(
                        paid_location_launch_budget, resources_override)):
                if selected_card is not None:
                    deferred_cards.add(selected_card)
                    continue
                break
            launch_kwargs: dict[str, Any] = {}
            if (item_paid_launch_allowed and
                    paid_location_launch_budget is not None):
                launch_kwargs['paid_location_launch_budget'] = (
                    paid_location_launch_budget)
            if paid_authority_left is not None:
                launch_kwargs['paid_launch_allowed'] = item_paid_launch_allowed
            elif not item_paid_launch_allowed:
                launch_kwargs['paid_launch_allowed'] = False
            selected_launch_priority = launch_priority
            if (selected_card is not None and
                    launch_priority_by_accelerator is not None):
                selected_launch_priority = (launch_priority_by_accelerator.get(
                    selected_card, serve_constants.LB_REQUEST_PRIORITY_MIN))
            selected_launch_priority = max(
                serve_constants.LB_REQUEST_PRIORITY_MIN,
                min(serve_constants.LB_REQUEST_PRIORITY_MAX,
                    selected_launch_priority))
            if (selected_launch_priority
                    != serve_constants.LB_REQUEST_PRIORITY_MIN):
                launch_kwargs['launch_priority'] = selected_launch_priority
            unknown_predecessor = next(
                (unknown_predecessors[replica_id]
                 for replica_id in sorted(unpaired_unknown_predecessor_ids)
                 if selected_card is None or _replica_card(
                     unknown_predecessors[replica_id]) == selected_card), None)
            if unknown_predecessor is not None:
                launch_kwargs['unknown_capacity_replacement'] = True
                launch_kwargs[
                    'logical_reconcile_fence_requires_exact_generation'] = True
                binding_authority = self._ordinary_launch_binding_authority
                if (binding_authority is not None and
                        binding_authority.generic_launches_required):
                    launch_kwargs[
                        'unknown_capacity_replacement_authorization'] = (
                            ordinary_launch_binding.
                            build_replacement_planner_authorization(
                                ordinary_launch_binding.NonPoolLaunchProfileKind
                                .UNKNOWN_CAPACITY_REPLACEMENT,
                                binding_authority,
                                predecessor_replica_id=(
                                    unknown_predecessor.replica_id),
                                predecessor_record_id=(
                                    unknown_predecessor.replica_record_id),
                                predecessor_service_version=(
                                    unknown_predecessor.version),
                                observation_generation=reconcile_generation,
                                observation_service_version=version,
                                target_capacity=target_capacity,
                                target_capacity_by_accelerator=(
                                    target_capacity_by_accelerator),
                                accelerator_shapes=accelerator_shapes))
            if paid_launch_authority is not None:
                # Replacement attribution proves which uncertain backend may
                # be overlapped; it grants no purchase authority. Every paid
                # row in a mixed wave debits the same immutable demand plan so
                # provider admission and the next row see one exact inventory.
                launch_kwargs['paid_launch_authority'] = paid_launch_authority
                launch_kwargs['prepared_paid_launches'] = (
                    prepared_paid_launches)
            prepared_count_before = len(prepared_paid_launches)
            launch_result = self._scale_up_one_locked(
                resources_override,
                used_replica_ids,
                existing_replica_infos,
                zero_cost_demand_budget,
                logical_reconcile_fence=((version, reconcile_generation,
                                          target_capacity, card_target_state or
                                          (), shape_state or ())
                                         if card_target_state is not None or
                                         shape_state is not None else
                                         (version, reconcile_generation,
                                          target_capacity)),
                **launch_kwargs)
            if launch_result is None:
                if selected_card is not None:
                    deferred_cards.add(selected_card)
                    logger.info('Deferring logical exact-card target '
                                f'{selected_card} after no placement progress; '
                                'continuing with other card targets in this '
                                'tick.')
                    continue
                logger.info('Logical scale-up made no placement progress; '
                            'retrying on the next reconciliation tick.')
                break
            if len(prepared_paid_launches) == prepared_count_before:
                accepted.append(launch_result)
            if unknown_predecessor is not None:
                unpaired_unknown_predecessor_ids.discard(
                    unknown_predecessor.replica_id)
            launched_capacity += launch_result.planned_capacity
            if (paid_authority_left is not None and
                    selected_card is not None and
                    launch_result.funding is _ReplicaLaunchFunding.PAID):
                paid_authority_left[selected_card] = max(
                    0,
                    paid_authority_left.get(selected_card, 0) -
                    launch_result.planned_capacity)
        accepted.extend(
            self._finalize_prepared_paid_launches(prepared_paid_launches,
                                                  paid_location_launch_budget,
                                                  existing_replica_infos))
        return accepted

    def notify_version_pending(self, version: int) -> None:
        with self._logical_state_lock:
            pending_version = self._pending_version
            if pending_version is None or version > pending_version:
                self._pending_version = version

    def _handoff_logical_retirements_for_version_update(
            self, replica_infos: list[ReplicaInfo]) -> set[int]:
        """Keep uncommitted drains off route across an in-process update.

        A pending version freezes old-version retirement admission. Once the
        update is applied, those selections no longer match the manager's
        latest version. Treat them like controller-recovery selections: rotate
        the authority epoch, retain their durable drain deadlines, and re-fence
        them only after the new version publishes a fresh target and capacity
        snapshot. Committed teardowns are already irreversible and continue in
        the existing down-thread pool independently of this handoff.
        """
        retiring_ids = {
            info.replica_id
            for info in replica_infos
            if self._is_recoverable_uncommitted_logical_retirement(info)
        }
        if not retiring_ids:
            return set()
        with self._logical_state_lock:
            self._logical_controller_epoch = uuid.uuid4().hex
            self._recovering_logical_retirement_ids.update(retiring_ids)
            # Start a fresh bounded recovery window for the new version. The
            # old selection's original drain deadline remains on each row.
            self._logical_retirement_recovery_deadline = None
            self._logical_retirement_reactivation_generation = None
        logger.info(
            'Handing off %s uncommitted logical retirements to version-update '
            'recovery without returning them to routing.', len(retiring_ids))
        return retiring_ids

    def clear_pending_version(self, version: int) -> None:
        with self._logical_state_lock:
            if self._pending_version == version:
                self._pending_version = None

    def _batch_needs_placement_snapshot(
            self, resources_overrides: list[dict[str, Any] | None]) -> bool:
        """Whether any launch in a batch will ask the placer for a location."""
        if self._spot_placer is None or not resources_overrides:
            return False
        if _placer_has_only_non_spot_kubernetes_gpu_locations(
                self._spot_placer):
            return True
        uses_task_default = False
        for resources_override in resources_overrides:
            if (resources_override is not None and
                    serve_constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY
                    in resources_override):
                return True
            if (resources_override is not None and
                    serve_constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY
                    in resources_override):
                return True
            use_spot_override = (resources_override or {}).get('use_spot')
            if use_spot_override is None:
                uses_task_default = True
            elif use_spot_override:
                return True
        return (uses_task_default and _should_use_spot(
            self.yaml_content,
            resource_override=None,
            service_spec=self._version_specs.get(self.latest_version),
            task_template=self._version_task_templates.get(
                self.latest_version)))

    def _paid_service_envelope_blocks_launch(
            self, budget: paid_capacity.LaunchBudget | None,
            resources_override: dict[str, Any] | None) -> bool:
        """Whether this launch can only use an exhausted paid envelope."""
        if not paid_capacity.service_exhausted(budget):
            return False
        override = resources_override or {}
        if (serve_constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY in override or
                serve_constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY
                in override or override.get('use_spot') is False):
            return False
        if self._spot_placer is None:
            return True
        allowed_locations = self._locations_for_accelerator_override(override)
        if allowed_locations is not None and not allowed_locations:
            # Preserve the normal exact-shape validation error.
            return False
        active_locations = set(self._spot_placer.active_locations())
        return not any(
            location in active_locations and
            (allowed_locations is None or location in allowed_locations)
            for location in self._spot_placer.zero_cost_locations())

    def _handle_sky_down_finish(self, info: ReplicaInfo,
                                format_exc: str | None) -> None:
        # This is the current down-result projection owner.  It is not
        # deprecated by the retired action-authority proposal.
        provider_free_failed_launch = bool(
            format_exc is None and info.reserved_fill is True and
            info.zero_cost_materialization_sequence is None and
            info.service_job_id is None and info.status in {
                serve_state.ReplicaStatus.SHUTTING_DOWN,
                serve_state.ReplicaStatus.FAILED_CLEANUP,
            })
        if format_exc is not None:
            logger.error(f'Down thread for replica {info.replica_id} '
                         f'exited abnormally with exception {format_exc}.')
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.FAILED)
            # A failed provider cleanup is never evidence that the resource
            # is gone.  Keep the durable row regardless of scale-down, purge,
            # preemption, or version state, then retry with capped backoff.
            self._persist_replica(info.replica_id, info)
            self._schedule_failed_cleanup_retry(info.replica_id)
            return
        else:
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.SUCCEEDED)
            self._clear_failed_cleanup_retry(info.replica_id)
        # Teardown writes INTERRUPTED before joining/cancelling an in-flight
        # launch.  If that cleanup succeeds but this was not an autoscaler,
        # purge, or preemption removal, the row is intentionally retained as
        # the current version's failure record.  Leaving INTERRUPTED in that
        # retained row derives SHUTTING_DOWN forever, so every controller
        # restart replays cleanup for capacity already proven gone.  Settle
        # the launch side as failed; FAILED_PROVISION preserves the diagnostic
        # row while SUCCEEDED remains the durable provider-cleanup evidence.
        if (info.status_property.sky_launch_status
                == common_utils.ProcessStatus.INTERRUPTED and
                not info.status_property.is_scale_down and
                not info.status_property.preempted and
                not info.status_property.purged):
            info.status_property.sky_launch_status = (
                common_utils.ProcessStatus.FAILED)
        # Failed replica still count as a replica. In our current design, we
        # want to fail early if user code have any error. This will prevent
        # infinite loop of teardown and re-provision. However, there is a
        # special case that if the replica is UP for longer than
        # initial_delay_seconds, we assume it is just some random failure and
        # we should restart the replica. Please refer to the implementation of
        # `is_scale_down_succeeded` for more details.
        # TODO(tian): Currently, restart replicas that failed within
        # initial_delay_seconds is not supported. We should add it
        # later when we support `sky serve update`.
        removal_reason = None
        if provider_free_failed_launch:
            # The exact removal transaction revalidates the terminal,
            # execution-quiesced request and association, canonical
            # post-receipt provider ABSENT evidence, and the absence of any
            # queue, pin, or paid claim.  Retaining this current-version
            # diagnostic row after that proof would debit the Kueue slot
            # forever and prevent the same allocation from replacing it.
            removal_reason = 'for a provider-free failed launch replacement'
        elif info.status_property.is_scale_down:
            # This means the cluster is deleted due to an autoscaler
            # decision or the cluster is recovering from preemption.
            # Delete the replica info so it won't count as a replica.
            if info.status_property.preempted:
                removal_reason = 'for preemption recovery'
            else:
                removal_reason = 'normally'
        # Don't keep failed record for version mismatch replicas,
        # since user should fixed the error before update.
        elif info.version != self.latest_version:
            removal_reason = 'for version outdated'
        elif info.status_property.purged:
            removal_reason = 'for purge'
        elif info.status_property.failed_spot_availability:
            removal_reason = 'for spot availability failure'
        elif info.status == serve_state.ReplicaStatus.UNKNOWN:
            # Both UNKNOWN arms of `to_replica_status` describe a replica
            # whose teardown already succeeded and that, in their own words,
            # "should have been cleaned from the replica table" -- a launch
            # still marked RUNNING when the teardown landed, or a probe that
            # never resolved. The record is kept here only because no other
            # reason matched, and it then reports nothing an operator can act
            # on: no endpoint, no resources, and a status that names no
            # failure. Retaining it strands one row per interrupted teardown.
            removal_reason = 'for a teardown that left no reportable state'
        else:
            logger.info(f'Termination of replica {info.replica_id} '
                        'finished. Replica info is kept since some '
                        'failure detected.')
            self._persist_replica(info.replica_id, info)
        if removal_reason is not None:
            self._remove_replica(info.replica_id,
                                 info.replica_record_id,
                                 allow_active_provider_free_pre_job=(
                                     provider_free_failed_launch))
            logger.info(f'Replica {info.replica_id} removed from the '
                        f'replica table {removal_reason}.')

    # Every caller holds ``self.lock``. Launch workers own cancellation and
    # quiescence; this method only commits their teardown signal.
    def _terminate_replica(
            self,
            replica_id: int,
            replica_drain_delay_seconds: int,
            is_scale_down: bool = False,
            purge: bool = False,
            in_flight_drain_cap_seconds: int | None = None) -> None:
        # Every caller holds ``self.lock``.  The controller transition owns
        # that same mutex, so this check is the linearization point that keeps
        # an in-flight health/probe result from scheduling teardown after a
        # partial config/runtime transition has been fenced.
        if self._update_recovery_required:
            logger.info(
                'Refusing to terminate replica %s because the controller '
                'update requires supervised recovery.', replica_id)
            return
        # This is the current down scheduler.  The bounded ordinary-launch
        # request-binding design does not replace it.
        legacy_runtime = self._legacy_mutation_runtime_state()
        provider_present_cleanup_context: BoundNonPoolLaunchContext | None = (
            None)
        projected_provider_absence_info: ReplicaInfo | None = None
        teardown_snapshot = (
            serve_state.get_replica_info_with_resource_action_identity(
                self._service_name, replica_id))
        assert teardown_snapshot is not None
        info, resource_action_identity = teardown_snapshot
        launch_thread = legacy_runtime.launch_thread_pool.get(replica_id)
        if (launch_thread is not None and
            (not isinstance(launch_thread, _ReplicaLaunchThread) or
             launch_thread.replica_record_id != info.replica_record_id or
             launch_thread.service_hash != self._service_hash or
             launch_thread.controller_owner != self._controller_owner)):
            logger.warning('Discarding stale launch worker for replica %s.',
                           replica_id)
            if legacy_runtime.launch_thread_pool.get(
                    replica_id) is launch_thread:
                legacy_runtime.launch_thread_pool.pop(replica_id)
                legacy_runtime.replica_to_request_id.pop(replica_id)
                legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)
            launch_thread = None
        if launch_thread is not None:
            if self._provider_present_cleanup_marker_shape(info):
                provider_present_cleanup_context = (
                    self._bound_non_pool_provider_present_cleanup_context(info))
            if provider_present_cleanup_context is not None:
                replica_drain_delay_seconds = 0
                is_scale_down = True
                purge = False
                in_flight_drain_cap_seconds = 0
            info.status_property.sky_launch_status = (
                common_utils.ProcessStatus.INTERRUPTED)
            # Persist the teardown flags in the SAME write as INTERRUPTED:
            # a crash between them would leave a SHUTTING_DOWN-deriving row
            # whose flags recovery misreads (a flagless scale-down re-drives
            # as a left-in-record failure teardown and strands the row).
            info.status_property.is_scale_down = is_scale_down
            info.status_property.purged = purge
            # The drain cap too: this INTERRUPTED row already derives
            # SHUTTING_DOWN, so a crash before the SCHEDULED write below
            # must leave recovery the resolved cap, not the resolver.
            info.status_property.drain_cap_seconds = (
                in_flight_drain_cap_seconds)
            _ensure_drain_started_at(info.status_property,
                                     in_flight_drain_cap_seconds)
            info.status_property.wait_for_idle_before_termination = False
            self._route_lease_registry().deactivate_record(
                replica_id, info.replica_record_id)
            self._persist_replica(replica_id, info)
            assert isinstance(launch_thread, _ReplicaLaunchThread)
            # The exact launch worker owns API cancellation and quiescence. It
            # publishes itself on completion, so the refresher never joins a
            # replacement that reused the numeric replica ID.
            launch_thread.teardown_requested.set()
            legacy_runtime.launch_completion_event.set()
            return

        # Recovery may observe a durable SHUTTING_DOWN row before rebuilding a
        # local launch waiter. Resolve its exact association before any
        # provider operation; direct down/delete is forbidden while the API
        # execution generation is active or ambiguous.
        binding_authority = self._ordinary_launch_binding_authority
        if (binding_authority is not None and binding_authority.binding_mode
                == ordinary_launch_binding.BindingMode.BOUND):
            if info is not None:
                if self._provider_present_cleanup_marker_shape(info):
                    provider_present_cleanup_context = (
                        self._bound_non_pool_provider_present_cleanup_context(
                            info))
                if provider_present_cleanup_context is not None:
                    replica_drain_delay_seconds = 0
                    is_scale_down = True
                    purge = False
                    in_flight_drain_cap_seconds = 0
                elif (request_postgres.
                      bound_non_pool_projected_provider_absence_is_authorized(
                          self._service_name, replica_id,
                          info.replica_record_id)):
                    projected_provider_absence_info = info
                else:
                    cancel_target = (request_postgres.
                                     lookup_bound_ordinary_launch_cancel_target(
                                         self._service_name, replica_id,
                                         info.replica_record_id))
                    if cancel_target is not None:
                        status = info.status_property
                        status.sky_launch_status = (
                            common_utils.ProcessStatus.INTERRUPTED)
                        status.service_ready_now = False
                        status.is_scale_down = is_scale_down
                        status.purged = purge
                        status.drain_cap_seconds = (in_flight_drain_cap_seconds)
                        _ensure_drain_started_at(status,
                                                 in_flight_drain_cap_seconds)
                        status.wait_for_idle_before_termination = False
                        self._route_lease_registry().deactivate_record(
                            replica_id, info.replica_record_id)
                        self._persist_replica(replica_id, info)
                        # Recovery may have lost the local waiter. Re-adopt the
                        # exact durable request and let that worker perform
                        # cancellation/quiescence outside the manager lock.
                        self._install_bound_launch_adopter(
                            info, cancel_target.context, start=True)
                        adopter = legacy_runtime.launch_thread_pool.get(
                            replica_id)
                        if not isinstance(adopter, _ReplicaLaunchThread):
                            raise RuntimeError(
                                'Bound teardown adopter was not installed.')
                        adopter.teardown_requested.set()
                        legacy_runtime.launch_completion_event.set()
                        return

        if replica_id in legacy_runtime.down_thread_pool:
            logger.warning(f'Terminate thread for replica {replica_id} '
                           'already exists. Skipping.')
            return

        if projected_provider_absence_info is not None:
            # Provider ABSENT and the released association/pin were committed
            # before a prior controller died. No provider call remains: route
            # this restart shape directly through the exact record-fenced
            # down-result remover.
            if not self._finalize_projected_provider_absence_cleanup(
                    projected_provider_absence_info.replica_id):
                raise RuntimeError(
                    'Projected provider absence lost exact row-removal '
                    'authority.')
            return

        logger.info(f'Terminating replica {replica_id}...')
        info.status_property.is_scale_down = is_scale_down
        info.status_property.purged = purge
        info.status_property.wait_for_idle_before_termination = False
        # Revoke the exact process-local route before recovery terminalization,
        # drain bookkeeping, or provider cleanup can block.  A
        # row recreation with the same numeric ID first retires only the prior
        # record and can never be revoked through its stale identity.  A
        # still-launching row has never owned a route and is handled above.
        registry = self._route_lease_registry()
        registry.observe_record_identity(replica_id, info.replica_record_id)
        registry.deactivate_record(replica_id, info.replica_record_id)
        expected_cluster_record_uuid = (
            str(resource_action_identity.sky_cluster_record_uuid)
            if resource_action_identity is not None else None)
        try:
            cleanup_fence = (
                reserved_capacity.parse_protocol_v2_cleanup_fence(info))
        except exceptions.KubernetesPhysicalClusterIdentityError as error:
            self._record_cleanup_uncertain(info,
                                           common_utils.format_exception(error))
            return

        if info.system_recovery is not None:

            def _terminalize_recovery(fresh: ReplicaInfo) -> bool:
                terminal = system_recovery_state.terminalize_for_teardown(
                    fresh.system_recovery, now=time.time())
                if terminal == fresh.system_recovery:
                    return False
                fresh.system_recovery = terminal
                return True

            recovery_info = self._patch_system_recovery_with_latest(
                replica_id, _terminalize_recovery)
            if recovery_info is not None:
                info = recovery_info

        # A recovery patch returns a fresh row, so reapply this teardown's
        # durable intent before any later uncertainty is recorded.
        info.status_property.is_scale_down = is_scale_down
        info.status_property.purged = purge
        info.status_property.wait_for_idle_before_termination = False

        # A controller restart loses the in-memory down worker.  Once a
        # logical retirement crossed the durable teardown boundary, its
        # prior worker may already have started even if the last persisted
        # process status is only SCHEDULED.  Re-evaluating the old selection
        # epoch would then be unsafe: aborting could re-advertise a backend
        # whose cloud resources are disappearing.  Detach the obsolete
        # optimization fence; the SCHEDULED write below persists that
        # detachment atomically before installing the idempotent cleanup
        # worker.  The strict pre-commit shape (wait_for_idle=True) does not
        # qualify and keeps the normal epoch/target/coverage abort behavior.
        if self._is_committed_logical_retirement(info):
            self._detach_committed_logical_retirement(info)

        logger.info(f'preempted: {info.status_property.preempted}, '
                    f'replica_id: {replica_id}')
        # A missing cluster-table row is not provider absence.  Exact PRESENT
        # evidence takes precedence and must run provider-native cleanup; its
        # immutable identity does not depend on the cluster table.  A reserved
        # cleanup fence likewise still requires a provider absence proof.  We
        # may finish inline only when neither authority exists.
        if not global_user_state.cluster_with_name_exists(info.cluster_name):
            if provider_present_cleanup_context is not None:
                # Continue into the exact down worker below.  Ordinary-paid
                # GCP cleanup uses the immutable request identity directly;
                # reserved fill uses its physical cleanup fence.
                pass
            elif cleanup_fence is not None:
                # Protocol-v2 absence is a provider observation.  Route this
                # through the normal down worker even when the cluster-table
                # row is already gone; terminate_cluster is idempotent and the
                # wrapper obtains the exact post-teardown Pod receipt without
                # holding the fleet mutex.
                pass
            else:
                # There is no provider identity to prove. Finish the
                # provider-free legacy row inline.
                self._handle_sky_down_finish(info, format_exc=None)
                return

        # Otherwise, schedule the thread to terminate the cluster. The
        # SHUTTING_DOWN status (sky_down_status set) is persisted FIRST:
        # the drain deadline and predicate are anchored to a moment at
        # which the controller provably stops advertising the replica to
        # the LB, and the deadline is anchored here (not at thread start)
        # so time queued in the admission pass counts toward the drain
        # budget instead of extending the terminate-slot hold.
        # Preserve an inherited RUNNING teardown.  It already consumes one D
        # slot and provider termination is idempotent, so the reconstructed
        # local worker below adopts it directly.  Rewriting it to SCHEDULED
        # would temporarily erase the global debit and admit excess cleanup.
        if (info.status_property.sky_down_status
                != common_utils.ProcessStatus.RUNNING):
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.SCHEDULED)
        info.status_property.drain_cap_seconds = in_flight_drain_cap_seconds
        drain_started_at = _ensure_drain_started_at(
            info.status_property, in_flight_drain_cap_seconds)
        self._persist_replica(replica_id, info)
        drain_deadline: float | None = None
        if (in_flight_drain_cap_seconds is not None and
                in_flight_drain_cap_seconds > 0):
            assert drain_started_at is not None
            drain_deadline = time.monotonic() + _remaining_drain_seconds(
                drain_started_at, in_flight_drain_cap_seconds)
        cleanup_record_id = info.replica_record_id
        cleanup_service_hash = self._service_hash
        cleanup_controller_owner = self._controller_owner

        def _exact_cleanup_authorized() -> bool:
            if (cleanup_service_hash != self._service_hash or
                    cleanup_controller_owner != self._controller_owner or
                    not self._service_is_cleanup_authorized()):
                return False
            current = serve_state.get_replica_info_from_id(
                self._service_name, replica_id)
            return (current is not None and
                    current.replica_record_id == cleanup_record_id and
                    current.status_property.sky_down_status in {
                        common_utils.ProcessStatus.SCHEDULED,
                        common_utils.ProcessStatus.RUNNING,
                    })

        terminate_kwargs = {
            'drain_deadline': drain_deadline,
            'drain_complete': None,
            'expected_cluster_record_uuid': expected_cluster_record_uuid,
            'cleanup_fence': cleanup_fence,
            'continue_guard': _exact_cleanup_authorized,
        }
        target: Callable[..., None]
        target_args: tuple[Any, ...]
        if provider_present_cleanup_context is None:
            target = terminate_cluster_with_kueue_absence_receipt
            target_args = (self._service_name, replica_id,
                           info.replica_record_id, info.cluster_name,
                           replica_drain_delay_seconds)
        else:
            assert binding_authority is not None
            target = terminate_bound_non_pool_provider_present_cluster
            target_args = (provider_present_cleanup_context, info,
                           binding_authority,
                           functools.partial(
                               self._project_bound_ordinary_launch, None),
                           info.cluster_name, replica_drain_delay_seconds)

        def _run_teardown(**worker_metadata: Any) -> None:
            # Provider termination is cost-critical.  Endpoint discovery is
            # optional diagnostic/latency evidence and may itself wait on a
            # stalled provider phase.  Consume only the already-anchored
            # bounded drain here, then invoke the exact provider teardown.
            target(*target_args, **worker_metadata)

        # Keep the cleanup metadata visible on the SafeThread for existing
        # admission/recovery introspection; the wrapper deliberately performs
        # no provider or diagnostic I/O before invoking the real target.
        t = _ReplicaDownThread(target=_run_teardown,
                               replica_id=replica_id,
                               replica_record_id=info.replica_record_id,
                               service_hash=self._service_hash,
                               controller_owner=self._controller_owner,
                               kwargs=terminate_kwargs)
        legacy_runtime.down_thread_pool[replica_id] = t

    def _reconcile_failed_cleanup(self,
                                  replica_infos: list[ReplicaInfo]) -> None:
        """Re-drive every durable cleanup failure until absence is proven."""
        # This is the current cleanup retry scheduler.  Its intent and target
        # are durable even though the backoff clock is process-local.
        legacy_runtime = self._legacy_mutation_runtime_state()
        now = time.monotonic()
        _, retry_at_by_replica = self._failed_cleanup_retry_state()
        for info in sorted(replica_infos, key=_provider_cleanup_phase_order):
            # A crash after the ABSENT transaction but before local down-result
            # handling loses every process-local worker. Consume that exact
            # durable history before process-local worker status.  The
            # transaction below still requires the association's exact
            # current lifecycle, owner, incarnation, request, and provider
            # identity; no historical lifecycle is cleanup authority.
            if (ordinary_launch_binding.
                    replica_has_projected_provider_absence_cleanup_marker(
                        info)):
                try:
                    if self._finalize_projected_provider_absence_cleanup(
                            info.replica_id):
                        reconciliation_threads = getattr(
                            self, '_non_pool_reconciliation_threads', None)
                        if reconciliation_threads is not None:
                            try:
                                reconciliation_threads.pop(info.replica_id)
                            except KeyError:
                                pass
                        getattr(self, '_non_pool_reconciliation_attempts',
                                {}).pop(info.replica_id, None)
                        getattr(self, '_non_pool_reconciliation_retry_at',
                                {}).pop(info.replica_id, None)
                        continue
                except Exception as error:  # pylint: disable=broad-except
                    logger.warning(
                        'Unable to finalize projected provider absence for '
                        'replica %s: %s', info.replica_id,
                        common_utils.format_exception(error))
            down_status = info.status_property.sky_down_status
            # Exact retirement owns successful teardown tombstones.  Ignore
            # even a stale process-local retry deadline left by an earlier
            # attempt so provider cleanup can never be repeated after success.
            if down_status == common_utils.ProcessStatus.SUCCEEDED:
                continue
            down_failed = down_status == common_utils.ProcessStatus.FAILED
            retry_pending = info.replica_id in retry_at_by_replica
            # SCHEDULED/RUNNING and the pre-scheduling ``None`` state are
            # durable cleanup intent, but their process-local down worker is
            # lost on controller restart.  Re-drive them through this same
            # bounded cleanup path.  A SUCCEEDED tombstone must never re-enter
            # provider cleanup; exact retirement owns that separate terminal
            # state.
            logical_retirement_present = (
                info.status_property.logical_retirement_version is not None)
            unfinished_teardown = (
                info.status in (serve_state.ReplicaStatus.SHUTTING_DOWN,
                                serve_state.ReplicaStatus.PREEMPTED) and
                info.status_property.wait_for_idle_before_termination is False
                and (not logical_retirement_present or
                     self._is_committed_logical_retirement(info)))
            if (info.status != serve_state.ReplicaStatus.FAILED_CLEANUP and
                    not down_failed and not retry_pending and
                    not unfinished_teardown):
                continue
            replica_id = info.replica_id
            if (replica_id in legacy_runtime.down_thread_pool or
                    replica_id in legacy_runtime.launch_thread_pool):
                continue
            retry_at = retry_at_by_replica.get(replica_id, 0)
            if now < retry_at:
                continue

            status_property = info.status_property
            is_scale_down = (status_property.is_scale_down or
                             status_property.preempted)
            purge = status_property.purged
            # A provider cleanup failure means the drain worker already ran;
            # retry cleanup immediately. A worker-start failure never entered
            # the drain wait and remains SCHEDULED, so it must reuse the
            # original cap/start pair and consume only the remaining window.
            # Pre-field FAILED rows are ambiguous because older controllers
            # also used FAILED for Thread.start() failures; without a durable
            # timestamp, grant one conservative bounded drain on upgrade.
            drain_cap: int | None = 0
            ambiguous_legacy_failure = (down_failed and is_scale_down and
                                        not purge and
                                        not status_property.preempted and
                                        not _is_valid_drain_started_at(
                                            status_property.drain_started_at))
            if ((not down_failed or ambiguous_legacy_failure) and
                    is_scale_down and not purge and
                    not status_property.preempted):
                drain_cap = status_property.drain_cap_seconds
                if drain_cap is None:
                    drain_cap = self._resolve_drain_cap_seconds(
                        replica_id, info)
            # Once an attempt is admitted, its durable SCHEDULED/RUNNING state
            # prevents duplicate reconciliation.  Remove the old deadline;
            # a failure records the next one in _handle_sky_down_finish.
            retry_at_by_replica.pop(replica_id, None)
            try:
                self._terminate_replica(replica_id,
                                        replica_drain_delay_seconds=0,
                                        is_scale_down=is_scale_down,
                                        purge=purge,
                                        in_flight_drain_cap_seconds=drain_cap)
            except Exception as e:  # pylint: disable=broad-except
                logger.error(
                    f'Failed to reconcile cleanup for replica {replica_id}: '
                    f'{common_utils.format_exception(e)}')
                self._schedule_failed_cleanup_retry(replica_id)

    def _resolve_drain_cap_seconds(self,
                                   replica_id: int,
                                   info: 'ReplicaInfo | None' = None) -> int:
        """Drain cap for retiring this replica, per its own version spec.

        An outdated replica retired by a rolling update drains per the
        spec it was serving under. Spec lookup failures fall back to the
        default cap -- a drain regression must never block a teardown.

        Callers that already hold the replica's ``ReplicaInfo`` pass it in
        to skip a redundant full-row read (and unpickle) of the same row.
        """
        try:
            if info is None:
                info = serve_state.get_replica_info_from_id(
                    self._service_name, replica_id)
            if info is not None:
                spec_drain = self._get_version_spec(
                    info.version).graceful_drain_seconds
                if spec_drain is not None:
                    return spec_drain
        except Exception as e:  # pylint: disable=broad-except
            logger.warning(
                f'Failed to resolve graceful_drain_seconds for replica '
                f'{replica_id}; using the default '
                f'({_DEFAULT_DRAIN_SECONDS}s): '
                f'{common_utils.format_exception(e)}')
        return _DEFAULT_DRAIN_SECONDS

    def _clean_up_skipped_cost_rebalance_redrive(
            self, replica_id: int,
            prior_cost_rebalance_for_replica_id: int | None) -> None:
        """Retire a persisted replacement that recovery cannot re-launch."""
        if prior_cost_rebalance_for_replica_id is None:
            # Fresh cost-rebalance emissions have not persisted a row yet.
            return
        logger.warning(
            f'Retiring cost-rebalance replacement {replica_id} after its '
            'recovery re-drive could not be enqueued.')
        self._terminate_replica(replica_id,
                                replica_drain_delay_seconds=0,
                                is_scale_down=True,
                                in_flight_drain_cap_seconds=0)

    def _register_wait_for_idle(
            self,
            info: ReplicaInfo,
            deadline: float | None = None,
            replica_url: Any = _REPLICA_URL_NOT_PROVIDED) -> None:
        """Register exact drain state without resolving a provider URL."""
        existing = self._wait_for_idle_trackers.get(info.replica_id)
        if (existing is not None and
                existing.replica_record_id == info.replica_record_id):
            return
        if existing is not None:
            self._wait_for_idle_trackers.pop(info.replica_id, None)
        drain_cap = info.status_property.drain_cap_seconds
        exact_retirement = None
        if (info.status_property.wait_for_idle_before_termination is True and
                drain_cap is None and self._service_hash is not None):
            try:
                exact_retirement = paid_retirement.get_for_replica(
                    self._service_name, info.replica_id)
            except Exception as error:  # pylint: disable=broad-except
                # A missing authority read is never permission to reinterpret
                # an unbounded exact-idle retirement as a bounded legacy drain.
                logger.warning(
                    'Unable to read exact paid-retirement authority for '
                    f'replica {info.replica_id}; keeping teardown blocked: '
                    f'{common_utils.format_exception(error)}')
                self._wait_for_idle_trackers[info.replica_id] = (
                    _WaitForIdleState(info.replica_record_id, math.inf))
                return
        if (exact_retirement is not None and
                str(exact_retirement['replica_record_id'])
                == info.replica_record_id and exact_retirement['state'] in {
                    paid_retirement.PaidRetirementState.ACTIVE.value,
                    paid_retirement.PaidRetirementState.COMMITTED.value,
                }):
            deadline = math.inf
            if exact_retirement['state'] == (
                    paid_retirement.PaidRetirementState.ACTIVE.value):
                replica_url = exact_retirement['route_url']
        needs_persist = False
        if drain_cap is None and exact_retirement is None:
            drain_cap = self._resolve_drain_cap_seconds(info.replica_id, info)
            info.status_property.drain_cap_seconds = drain_cap
            needs_persist = True
        prior_started_at = info.status_property.drain_started_at
        drain_started_at = (None if exact_retirement is not None
                            else _ensure_drain_started_at(
                                info.status_property, drain_cap))
        if drain_started_at != prior_started_at:
            needs_persist = True
        if needs_persist:
            self._persist_replica(info.replica_id, info)
        drain_started = time.monotonic()
        if deadline is None:
            assert drain_cap is not None
            remaining = (0.0 if drain_started_at is None else
                         _remaining_drain_seconds(drain_started_at, drain_cap))
            deadline = drain_started + remaining
        tracker: _ReplicaDrainTracker | None = None
        if (replica_url is not _REPLICA_URL_NOT_PROVIDED and
                replica_url is not None and not self._is_pool):
            assert isinstance(replica_url, str), replica_url
            tracker = _ReplicaDrainTracker(self, replica_url, drain_started)
        self._wait_for_idle_trackers[info.replica_id] = _WaitForIdleState(
            replica_record_id=info.replica_record_id,
            deadline=deadline,
            tracker=tracker,
            needs_url_resolution=(tracker is None and not self._is_pool))

    def _resolve_wait_for_idle_urls(self) -> bool:
        """Resolve strict-drain endpoints outside the fleet mutex."""
        with self.lock:
            if self._update_recovery_required:
                return False
            owner_snapshot = (self._service_hash, self._controller_owner)
            pending_states = {
                replica_id: state
                for replica_id, state in self._wait_for_idle_trackers.items()
                if state.needs_url_resolution
            }
            infos = serve_state.get_replica_infos_from_ids(
                self._service_name, list(pending_states))
            pending_infos = [
                info for replica_id, info in infos.items()
                if (replica_id in pending_states and pending_states[replica_id].
                    replica_record_id == info.replica_record_id)
            ]
        if not pending_infos:
            return False

        deferred_ids: set[int] = set()
        identity_rejected_ids: set[int] = set()
        urls: dict[int, str | None] = {}
        fenced_infos: list[ReplicaInfo] = []
        ordinary_infos: list[ReplicaInfo] = []
        for info in pending_infos:
            destination = (fenced_infos if _provider_cleanup_phase_order(info)
                           == 0 else ordinary_infos)
            destination.append(info)

        def _try_resolve_partition(
                partition: list[ReplicaInfo],
                mode: provider_phase.ProviderPhaseMode) -> None:
            if not partition:
                return
            try:
                with provider_phase.try_provider_phase(mode) as admission:
                    urls.update(
                        self._resolve_probe_urls(
                            partition,
                            phase_admission=admission,
                            deferred_replica_ids=deferred_ids,
                            identity_rejected_replica_ids=(
                                identity_rejected_ids)))
            except exceptions.ProviderPhaseBusyError:
                deferred_ids.update(info.replica_id for info in partition)
            except Exception as phase_error:  # pylint: disable=broad-except
                # Phase acquisition/retirement failure is scoped to this
                # partition. The opposite provider phase must still run.
                deferred_ids.update(info.replica_id for info in partition)
                logger.warning(
                    'Strict-drain endpoint phase %s failed; retaining %s '
                    'replicas for retry: %s', mode.value, len(partition),
                    common_utils.format_exception(phase_error))

        # This optimization never queues behind provider work. Complete the
        # exact-identity partition first, then opportunistically try ordinary
        # endpoints; unresolved rows remain durable and retry next pass.
        _try_resolve_partition(fenced_infos,
                               provider_phase.ProviderPhaseMode.V2_FENCED)
        _try_resolve_partition(ordinary_infos,
                               provider_phase.ProviderPhaseMode.AMBIENT_LEGACY)

        changed = False
        with self.lock:
            if (self._update_recovery_required or owner_snapshot
                    != (self._service_hash, self._controller_owner)):
                return False
            current_infos = serve_state.get_replica_infos_from_ids(
                self._service_name, [info.replica_id for info in pending_infos])
            for opening_info in pending_infos:
                replica_id = opening_info.replica_id
                opening_state = pending_states[replica_id]
                if self._wait_for_idle_trackers.get(
                        replica_id) is not opening_state:
                    continue
                current_info = current_infos.get(replica_id)
                if (current_info is None or current_info.replica_record_id
                        != opening_state.replica_record_id):
                    continue
                if replica_id in identity_rejected_ids:
                    self._record_provider_identity_uncertain(
                        current_info,
                        'strict-drain endpoint identity was fenced off')
                    continue
                if replica_id in deferred_ids:
                    continue
                url = urls.get(replica_id)
                if url is None:
                    # A transiently absent endpoint is no evidence. Retain the
                    # retry bit: exact paid retirement has no deadline and
                    # must not be stranded by one failed lookup; bounded
                    # drains may still expire in the pure reducer.
                    continue
                self._provider_identity_uncertain_replica_ids().discard(
                    replica_id)
                self._wait_for_idle_trackers[replica_id] = dataclasses.replace(
                    opening_state,
                    tracker=_ReplicaDrainTracker(self, url, time.monotonic()),
                    needs_url_resolution=False)
                changed = True
        return changed

    def _defer_scale_down_until_idle(
            self,
            replica_id: int,
            logical_retirement: tuple[int, int, int] | None = None,
            *,
            replica_info: ReplicaInfo | None = None,
            replica_url: Any = _REPLICA_URL_NOT_PROVIDED) -> None:
        """Persist off-route state without admitting termination yet."""
        info = replica_info
        if info is None:
            info = serve_state.get_replica_info_from_id(self._service_name,
                                                        replica_id)
        if info is None:
            return
        if info.status_property.wait_for_idle_before_termination is True:
            self._register_wait_for_idle(info, replica_url=replica_url)
            return
        identity_uncertain = (
            replica_id in self._provider_identity_uncertain_replica_ids())
        if (not identity_uncertain and
                not global_user_state.cluster_with_name_exists(
                    info.cluster_name)):
            self._terminate_replica(replica_id,
                                    replica_drain_delay_seconds=0,
                                    is_scale_down=True,
                                    in_flight_drain_cap_seconds=0)
            return
        info.status_property.is_scale_down = True
        info.status_property.purged = False
        if (info.status_property.sky_down_status
                != common_utils.ProcessStatus.RUNNING):
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.SCHEDULED)
        info.status_property.drain_cap_seconds = (
            self._resolve_drain_cap_seconds(replica_id, info))
        _ensure_drain_started_at(info.status_property,
                                 info.status_property.drain_cap_seconds)
        info.status_property.wait_for_idle_before_termination = True
        if logical_retirement is not None:
            version, generation, target_capacity = logical_retirement
            info.status_property.logical_retirement_version = version
            info.status_property.logical_retirement_controller_epoch = (
                self._logical_controller_epoch)
            info.status_property.logical_retirement_generation = generation
            info.status_property.logical_retirement_target_capacity = (
                target_capacity)
        info.status_property.logical_retirement_confirmed_generation = (None)
        info.status_property.logical_retirement_bounded_deadline = False
        info.status_property.logical_retirement_committed = False
        self._persist_replica(replica_id, info)
        self._register_wait_for_idle(info, replica_url=replica_url)

    def _logical_retirement_state(
            self,
            info: ReplicaInfo,
            *,
            require_victim_idle: bool = True,
            replica_infos: list[ReplicaInfo] | None = None) -> str:
        """Return safe, wait, or abort for one off-route logical backend.

        ``require_victim_idle=False`` is reserved for an outdated backend
        that has already consumed its full configured drain window.  It still
        requires a fresh current-epoch/current-target replacement-capacity
        proof; only the retiring backend's otherwise-unprovable idle state is
        omitted from that bounded rolling-update completion check.

        ``replica_infos`` lets a caller revalidate multiple retirements from
        one coherent ready-capacity read.  The fallback preserves
        single-retirement callers; batch paths must pass their shared snapshot
        so retained replica history is not repeatedly decoded while the
        controller is starting.
        """
        status = info.status_property
        version = status.logical_retirement_version
        controller_epoch = status.logical_retirement_controller_epoch
        selection_generation = status.logical_retirement_generation
        selection_target = status.logical_retirement_target_capacity
        if (type(version) is not int or not isinstance(controller_epoch, str) or
                not controller_epoch or type(selection_generation) is not int or
                selection_generation < 0 or type(selection_target) is not int or
                selection_target < 0):
            return 'abort'
        if controller_epoch != self._logical_controller_epoch:
            return 'abort'
        logical_state = self._logical_reconcile_state
        snapshot = logical_state.snapshot
        target_state = _logical_target_state_components(
            _logical_retirement_target(logical_state))
        if (snapshot is None or snapshot.generation <= selection_generation or
                target_state is None):
            return 'wait'
        if not self._logical_snapshot_is_fresh(snapshot):
            return 'wait'
        if (snapshot.version != version or self.latest_version != version):
            return 'abort'
        pending_version = self._pending_version
        if pending_version is not None and pending_version > version:
            # The committed update may wait on the manager lock for minutes at
            # fleet scale. Keep an already off-route victim frozen until the
            # update can hand it to the new version's recovery fence; aborting
            # here would advertise every pending retirement again.
            return 'wait'
        (target_version, target_generation, current_target,
         target_by_accelerator, accelerator_shapes) = target_state
        if target_version != version:
            return 'abort'
        if snapshot.generation < target_generation:
            return 'wait'

        # A same-version demand rebound does not invalidate every accepted
        # retirement. Recompute against the current target instead. Since
        # _logical_ready_capacity excludes all off-route rows, callers abort
        # and reactivate only enough victims to cover a real shortfall; the
        # remainder can continue draining without fleet-wide churn. Check
        # route coverage before the victim's idle proof: idleness gates
        # destructive teardown, not re-advertising a still-running backend.
        if replica_infos is None:
            replica_infos = serve_state.get_replica_infos(self._service_name)
        excluded_ids = {info.replica_id}
        ready_capacity = self._logical_ready_capacity(
            replica_infos,
            snapshot,
            version,
            excluded_ids,
            stats=self._drain_proof_stats)
        ready_by_accelerator = (self._logical_ready_capacity_by_accelerator(
            replica_infos, snapshot, version, excluded_ids, accelerator_shapes)
                                if accelerator_shapes else {})
        ready_covers_target = (ready_capacity >= current_target and
                               self._logical_card_capacity_covers(
                                   ready_by_accelerator, target_by_accelerator))
        if not ready_covers_target:
            return 'abort'
        if (require_victim_idle and
                not self._logical_retirement_victim_is_idle(info, snapshot)):
            return 'wait'
        return 'safe'

    @staticmethod
    def _logical_ready_capacity(
            replica_infos: list[ReplicaInfo],
            snapshot: LogicalReconcileSnapshot,
            version: int,
            excluded_replica_ids: set[int] | frozenset[int],
            stats: 'drain_observability.DrainProofStats | None' = None) -> int:
        """Return freshly observed ready capacity from one fleet snapshot."""
        ready_capacity = 0
        blind_skipped = 0
        for candidate in replica_infos:
            if (candidate.replica_id in excluded_replica_ids or
                    candidate.is_terminal or not candidate.is_ready or
                    candidate.status_property.is_scale_down is True):
                continue
            if candidate.version < version:
                ready_capacity += 1
                continue
            if candidate.version != version:
                continue
            observed = snapshot.observed_slots_by_replica_id.get(
                candidate.replica_id)
            if (observed is None or
                    candidate.replica_id in snapshot.unknown_replica_ids):
                # Unobserved or explicitly unknown: contributes nothing. A
                # restarted load balancer makes this true for EVERY replica
                # for its first sync or two, which reads downstream as a
                # capacity shortfall and aborts the whole wave.
                blind_skipped += 1
                continue
            ready_capacity += min(int(candidate.planned_capacity), observed)
        if stats is not None:
            stats.record_blind_ready_capacity(blind_skipped)
        return ready_capacity

    @staticmethod
    def _logical_replica_accelerator(
        info: ReplicaInfo,
        accelerator_shapes: LogicalAcceleratorState,
        *,
        require_configured_shape: bool,
    ) -> str | None:
        """Resolve one exact configured card without family matching."""
        canonical = {
            card.casefold(): (card, count) for card, count in accelerator_shapes
        }
        accelerators = None
        location = info.get_spot_location()
        if location is not None:
            accelerators = location.accelerators
        if not accelerators:
            accelerators = (info.resources_override or {}).get('accelerators')
        if not isinstance(accelerators, dict) or len(accelerators) != 1:
            return None
        raw_card, raw_count = next(iter(accelerators.items()))
        configured = canonical.get(str(raw_card).casefold())
        if configured is None:
            return None
        card, configured_count = configured
        if require_configured_shape:
            try:
                count = int(raw_count)
                planned = int(info.planned_capacity)
            except (TypeError, ValueError):
                return None
            if count != configured_count or planned != configured_count:
                return None
        return card

    @classmethod
    def _logical_ready_capacity_by_accelerator(
        cls,
        replica_infos: list[ReplicaInfo],
        snapshot: LogicalReconcileSnapshot,
        version: int,
        excluded_replica_ids: set[int] | frozenset[int],
        accelerator_shapes: LogicalAcceleratorState,
    ) -> dict[str, int]:
        """Return fresh ready capacity grouped by exact configured card."""
        ready = {card: 0 for card, _ in accelerator_shapes}
        for candidate in replica_infos:
            if (candidate.replica_id in excluded_replica_ids or
                    candidate.is_terminal or not candidate.is_ready or
                    candidate.status_property.is_scale_down is True or
                    candidate.version > version):
                continue
            card = cls._logical_replica_accelerator(
                candidate,
                accelerator_shapes,
                require_configured_shape=(candidate.version == version))
            if card is None:
                continue
            if candidate.version < version:
                ready[card] += 1
                continue
            observed = snapshot.observed_slots_by_replica_id.get(
                candidate.replica_id)
            if (observed is None or
                    candidate.replica_id in snapshot.unknown_replica_ids):
                continue
            ready[card] += min(int(candidate.planned_capacity), observed)
        return ready

    @staticmethod
    def _logical_card_capacity_covers(
            capacity: dict[str, int],
            target_by_accelerator: LogicalAcceleratorState) -> bool:
        return all(
            capacity.get(card, 0) >= target
            for card, target in target_by_accelerator)

    def _logical_retirement_victim_is_idle(
            self, info: ReplicaInfo,
            snapshot: LogicalReconcileSnapshot) -> bool:
        """Use the raw URL proof when the ID translation is already pruned."""
        tracked = self._wait_for_idle_trackers.get(info.replica_id)
        if tracked is not None:
            tracker = tracked.tracker
            if tracker is not None and tracker():
                return True
        return (info.replica_id not in snapshot.unknown_replica_ids and
                snapshot.in_flight_by_replica_id.get(info.replica_id) == 0)

    @staticmethod
    def _is_committed_logical_retirement(info: ReplicaInfo) -> bool:
        """Whether a persisted logical retirement must finish cleanup.

        Down admission persists ``logical_retirement_committed`` immediately
        before starting the worker. That disambiguates a budget-delayed
        SCHEDULED row from the crash window in which ``sky.down`` may already
        have started before RUNNING is persisted. Legacy RUNNING/FAILED rows
        are intrinsically committed. Exact type/value checks keep malformed
        state fail-closed.
        """
        status = info.status_property
        retirement_version = status.logical_retirement_version
        controller_epoch = status.logical_retirement_controller_epoch
        selection_generation = status.logical_retirement_generation
        selection_target = status.logical_retirement_target_capacity
        confirmed_generation = status.logical_retirement_confirmed_generation
        bounded_deadline = status.logical_retirement_bounded_deadline
        committed = status.logical_retirement_committed
        info_version = info.version
        committed_statuses = (
            common_utils.ProcessStatus.SCHEDULED,
            common_utils.ProcessStatus.RUNNING,
            common_utils.ProcessStatus.FAILED,
        )
        return (status.is_scale_down is True and
                status.wait_for_idle_before_termination is False and
                status.sky_down_status in committed_statuses and
                (committed is None or type(committed) is bool) and
                (committed is True or
                 (status.sky_down_status
                  in (common_utils.ProcessStatus.RUNNING,
                      common_utils.ProcessStatus.FAILED))) and
                type(info_version) is int and
                type(retirement_version) is int and
                info_version <= retirement_version and
                isinstance(controller_epoch, str) and bool(controller_epoch) and
                type(selection_generation) is int and
                selection_generation >= 0 and type(selection_target) is int and
                selection_target >= 0 and type(confirmed_generation) is int and
                confirmed_generation >= selection_generation and
                type(bounded_deadline) is bool)

    @staticmethod
    def _is_recoverable_uncommitted_logical_retirement(
            info: ReplicaInfo) -> bool:
        """Whether an old-epoch precommit retirement can be re-fenced.

        This includes both a strict idle-wait victim and an outdated victim
        whose bounded deadline was confirmed but whose teardown is still
        waiting for shared-budget admission. Neither has crossed the durable
        RUNNING boundary, and both must stay off route while fresh authority
        decides whether to adopt or selectively reactivate them.
        """
        return replica_info_lib.is_recoverable_uncommitted_logical_retirement(
            info)

    @staticmethod
    def _is_uncommitted_logical_retirement_admission(info: ReplicaInfo) -> bool:
        """Whether exact readback proves a destructive commit did not land.

        The ordinary idle path clears the durable wait bit when it constructs
        an unstarted down worker.  If the later PostgreSQL commit call loses
        its acknowledgement *before* committing, readback therefore sees this
        narrower admission-precommit shape rather than the earlier strict-wait
        shape.  It is reversible and may be requeued, but it still needs fresh
        N+1 authority and ``commit_logical_retirement`` before worker start.
        """
        return replica_info_lib.is_uncommitted_logical_retirement_admission(
            info)

    @classmethod
    def _is_restart_recoverable_logical_retirement(cls,
                                                   info: ReplicaInfo) -> bool:
        """Whether controller-start recovery owns an exact precommit row.

        Version-update handoff deliberately excludes ordinary queued
        admission precommits so it can abort and reselect them in-process.
        After a controller crash there is no process-local worker or
        ambiguous-ID owner left, so startup must additionally adopt that
        exact reversible shape behind fresh N+1 authority.
        """
        del cls
        return replica_info_lib.is_restart_recoverable_logical_retirement(info)

    @staticmethod
    def _is_legacy_uncertain_logical_retirement(info: ReplicaInfo) -> bool:
        """Whether a pre-commit-bit SCHEDULED retirement is ambiguous."""
        status = info.status_property
        retirement_version = status.logical_retirement_version
        controller_epoch = status.logical_retirement_controller_epoch
        selection_generation = status.logical_retirement_generation
        selection_target = status.logical_retirement_target_capacity
        confirmed_generation = status.logical_retirement_confirmed_generation
        bounded_deadline = status.logical_retirement_bounded_deadline
        committed = status.logical_retirement_committed
        info_version = info.version
        return (status.is_scale_down is True and
                status.wait_for_idle_before_termination is False and
                status.sky_down_status == common_utils.ProcessStatus.SCHEDULED
                and committed is None and type(info_version) is int and
                type(retirement_version) is int and
                info_version <= retirement_version and
                isinstance(controller_epoch, str) and bool(controller_epoch) and
                type(selection_generation) is int and
                selection_generation >= 0 and type(selection_target) is int and
                selection_target >= 0 and type(confirmed_generation) is int and
                confirmed_generation >= selection_generation and
                type(bounded_deadline) is bool)

    def _reconcile_legacy_uncertain_logical_retirements(self) -> None:
        """Adopt legacy rows as reversible precommits for a later N+1."""
        uncertain_ids = self._legacy_uncertain_logical_retirement_ids
        if not uncertain_ids:
            return

        infos = serve_state.get_replica_infos_from_ids(self._service_name,
                                                       sorted(uncertain_ids))
        for replica_id in list(uncertain_ids):
            info = infos.get(replica_id)
            if info is None:
                uncertain_ids.discard(replica_id)
                continue
            if self._is_committed_logical_retirement(info):
                try:
                    self._terminate_replica(replica_id,
                                            replica_drain_delay_seconds=0,
                                            is_scale_down=True,
                                            in_flight_drain_cap_seconds=0)
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(f'Failed to re-drive adopted legacy logical '
                                   f'retirement for replica {replica_id}: '
                                   f'{common_utils.format_exception(e)}')
                    continue
                uncertain_ids.discard(replica_id)
                continue
            if (self._is_recoverable_uncommitted_logical_retirement(info) and
                    info.status_property.logical_retirement_version
                    == self.latest_version and
                    info.status_property.logical_retirement_controller_epoch
                    == self._logical_controller_epoch):
                # The normalization write may have committed even if its
                # acknowledgement was lost.  Exact durable readback of the
                # current-format reversible row is sufficient to adopt the
                # wait; it still needs a strict later N+1 and the PostgreSQL
                # destructive commit seam before any worker can start.
                self._register_wait_for_idle(info, replica_url=None)
                uncertain_ids.discard(replica_id)
                continue
            if not self._is_legacy_uncertain_logical_retirement(info):
                # Once classified as ambiguous, never reactivate the backend
                # merely because its durable state becomes malformed. Keep the
                # safe off-route state for operator inspection.
                continue

            with self._logical_state_lock:
                logical_state = self._logical_reconcile_state
                snapshot = logical_state.snapshot
                target_state = _logical_target_state_components(
                    _logical_retirement_target(logical_state))
                if (snapshot is None or target_state is None or
                        not self._logical_snapshot_is_fresh(snapshot) or
                        snapshot.authority is None or
                        snapshot.version != self.latest_version or
                        snapshot.generation <= 0):
                    continue
                (target_version, target_generation, current_target, _,
                 _) = target_state
                if (target_version != self.latest_version or
                        snapshot.generation < target_generation):
                    continue
                pending_version = self._pending_version
                if (pending_version is not None and
                        pending_version > self.latest_version):
                    continue

                status = info.status_property
                old_selection = (
                    status.logical_retirement_version,
                    status.logical_retirement_controller_epoch,
                    status.logical_retirement_generation,
                    status.logical_retirement_target_capacity,
                    status.logical_retirement_confirmed_generation,
                    status.logical_retirement_bounded_deadline,
                    status.wait_for_idle_before_termination,
                    status.logical_retirement_committed,
                )
                status.logical_retirement_version = self.latest_version
                status.logical_retirement_controller_epoch = (
                    self._logical_controller_epoch)
                status.logical_retirement_generation = snapshot.generation
                status.logical_retirement_target_capacity = current_target
                status.logical_retirement_confirmed_generation = None
                status.logical_retirement_bounded_deadline = False
                status.wait_for_idle_before_termination = True
                status.logical_retirement_committed = False
                try:
                    self._persist_replica(replica_id, info)
                except Exception as e:  # pylint: disable=broad-except
                    (status.logical_retirement_version,
                     status.logical_retirement_controller_epoch,
                     status.logical_retirement_generation,
                     status.logical_retirement_target_capacity,
                     status.logical_retirement_confirmed_generation,
                     status.logical_retirement_bounded_deadline,
                     status.wait_for_idle_before_termination,
                     status.logical_retirement_committed) = old_selection
                    logger.warning(
                        f'Failed to persist adoption of legacy logical '
                        f'retirement for replica {replica_id}: '
                        f'{common_utils.format_exception(e)}')
                    continue
                # Generation N only establishes a current-format reversible
                # precommit. The normal idle path must observe N+1 and then
                # use commit_logical_retirement before any worker can start.
                self._register_wait_for_idle(info, replica_url=None)
                uncertain_ids.discard(replica_id)

    def _reconcile_ambiguous_logical_retirement_commits(self) -> None:
        """Read back a lost commit acknowledgement before reconstructing."""
        ambiguous_ids = self._ambiguous_logical_retirement_commit_ids
        if not ambiguous_ids:
            return
        try:
            infos = serve_state.get_replica_infos_from_ids(
                self._service_name, sorted(ambiguous_ids))
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(
                'Unable to read back ambiguous logical-retirement commits: '
                f'{common_utils.format_exception(error)}')
            return
        for replica_id in sorted(list(ambiguous_ids)):
            info = infos.get(replica_id)
            if info is None:
                ambiguous_ids.discard(replica_id)
                continue
            if (not self._is_committed_logical_retirement(info) and
                    not self._is_uncommitted_logical_retirement_admission(info)
               ):
                # A malformed or externally changed row is not evidence that
                # the commit failed. Keep it off-route for inspection.
                continue
            try:
                self._terminate_replica(replica_id,
                                        replica_drain_delay_seconds=0,
                                        is_scale_down=True,
                                        in_flight_drain_cap_seconds=0)
            except Exception as error:  # pylint: disable=broad-except
                logger.warning(
                    'Failed to reconstruct logical retirement after commit '
                    f'readback for replica {replica_id}: '
                    f'{common_utils.format_exception(error)}')
                continue
            ambiguous_ids.discard(replica_id)

    def _logical_retirement_recovery_timed_out(self) -> bool:
        deadline = self._logical_retirement_recovery_deadline
        return deadline is not None and time.monotonic() >= deadline

    @staticmethod
    def _logical_planned_capacity(info: ReplicaInfo) -> int:
        planned = info.planned_capacity
        if (isinstance(planned, bool) or not isinstance(planned, int) or
                planned < 1):
            return 1
        return planned

    def _clear_logical_retirement_recovery_if_done(self) -> None:
        recovering_ids: set[int] = self._recovering_logical_retirement_ids
        if recovering_ids:
            return
        self._logical_retirement_recovery_deadline = None
        self._logical_retirement_reactivation_generation = None

    def _reconcile_recovering_logical_retirements(self) -> None:
        """Adopt safe old-epoch drains without advertising the whole fleet."""
        recovering_ids = self._recovering_logical_retirement_ids
        if not recovering_ids:
            return
        if self._logical_retirement_recovery_deadline is None:
            self._logical_retirement_recovery_deadline = (
                time.monotonic() + _LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS)

        # One fleet read per pass. Calling _logical_retirement_state() for each
        # candidate would rescan the whole table O(candidates * fleet).
        replica_infos = serve_state.get_replica_infos(self._service_name)
        infos_by_id = {info.replica_id: info for info in replica_infos}
        candidates: list[ReplicaInfo] = []
        for replica_id in sorted(list(recovering_ids)):
            info = infos_by_id.get(replica_id)
            if info is None:
                recovering_ids.discard(replica_id)
                self._wait_for_idle_trackers.pop(replica_id, None)
                continue
            if self._is_committed_logical_retirement(info):
                recovering_ids.discard(replica_id)
                continue
            if not self._is_restart_recoverable_logical_retirement(info):
                # The normal refresh path retains authority over malformed or
                # otherwise changed state; the recovery-only epoch gate must
                # not hide it.
                recovering_ids.discard(replica_id)
                continue
            candidates.append(info)

        if not candidates:
            self._clear_logical_retirement_recovery_if_done()
            return

        with self._logical_state_lock:
            logical_state = self._logical_reconcile_state
            snapshot = logical_state.snapshot
            target_state = _logical_target_state_components(
                _logical_retirement_target(logical_state))
            if (snapshot is None or target_state is None or
                    not self._logical_snapshot_is_fresh(snapshot) or
                    snapshot.version != self.latest_version):
                if self._logical_retirement_recovery_timed_out():
                    logger.warning(
                        'Logical retirement recovery evidence remained '
                        f'unavailable after '
                        f'{_LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS}s; '
                        f'keeping {len(candidates)} uncommitted replicas '
                        'off-route until a fresh target and capacity snapshot '
                        'arrives.')
                    self._logical_retirement_recovery_deadline = (
                        time.monotonic() +
                        _LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS)
                return
            (target_version, target_generation, current_target,
             target_by_accelerator, accelerator_shapes) = target_state
            if (target_version != self.latest_version or
                    snapshot.generation < target_generation):
                if self._logical_retirement_recovery_timed_out():
                    logger.warning(
                        'Logical retirement recovery target and capacity '
                        'generations remained incoherent after '
                        f'{_LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS}s; '
                        f'keeping {len(candidates)} uncommitted replicas '
                        'off-route until coherent evidence arrives.')
                    self._logical_retirement_recovery_deadline = (
                        time.monotonic() +
                        _LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS)
                return
            pending_version = self._pending_version
            if (pending_version is not None and
                    pending_version > self.latest_version):
                return
            reactivation_generation = self._logical_retirement_reactivation_generation
            if (reactivation_generation is not None and
                    snapshot.generation <= reactivation_generation):
                return
            self._logical_retirement_reactivation_generation = None

            old_epoch_candidates = []
            for info in candidates:
                status = info.status_property
                if (status.logical_retirement_controller_epoch
                        != self._logical_controller_epoch):
                    old_epoch_candidates.append(info)
                    continue
                # Adoption changes durable shutdown authority. Do not admit
                # that shutdown from the same LB generation whose pre-adoption
                # view authorized the re-fence. A strictly newer matching
                # capacity snapshot releases it to the normal admission path
                # while the separately published target remains authoritative.
                selection_generation = (status.logical_retirement_generation)
                assert type(selection_generation) is int
                if snapshot.generation > selection_generation:
                    recovering_ids.discard(info.replica_id)
            candidates = old_epoch_candidates
            if not candidates:
                self._clear_logical_retirement_recovery_if_done()
                return

            ready_capacity = self._logical_ready_capacity(
                replica_infos,
                snapshot,
                self.latest_version,
                frozenset(recovering_ids),
                stats=self._drain_proof_stats)
            ready_by_accelerator = (self._logical_ready_capacity_by_accelerator(
                replica_infos, snapshot, self.latest_version,
                frozenset(recovering_ids), accelerator_shapes)
                                    if accelerator_shapes else {})
            shortfall_by_accelerator = {
                card: max(0, target - ready_by_accelerator.get(card, 0))
                for card, target in target_by_accelerator
            }
            if (ready_capacity < current_target or
                    any(shortfall_by_accelerator.values())):
                shortfall = max(0, current_target - ready_capacity)
                reactivated_capacity = 0
                reactivated_count = 0
                # Prefer current-version victims because their immutable
                # logical width is authoritative.  Old-version victims remain
                # a valid availability bridge, but count as one conservative
                # slot each, matching _logical_ready_capacity.
                ordered_candidates = sorted(
                    candidates,
                    key=lambda candidate:
                    (candidate.version != self.latest_version, candidate.
                     replica_id))
                # Consume this generation before the first write. A partial or
                # commit-ambiguous failure must not let the next refresh reuse
                # the same evidence to exceed the per-generation wave bound.
                self._logical_retirement_reactivation_generation = (
                    snapshot.generation)
                for info in ordered_candidates:
                    candidate_capacity = (self._logical_planned_capacity(info)
                                          if info.version == self.latest_version
                                          else 1)
                    card: str | None = None
                    if target_by_accelerator:
                        card = self._logical_replica_accelerator(
                            info,
                            accelerator_shapes,
                            require_configured_shape=(
                                info.version == self.latest_version))
                        if (card is None or
                                shortfall_by_accelerator.get(card, 0) <= 0):
                            continue
                    self._abort_logical_retirement(
                        info,
                        'current ready capacity is below the recovered target')
                    recovering_ids.discard(info.replica_id)
                    reactivated_capacity += candidate_capacity
                    if target_by_accelerator:
                        assert card is not None
                        shortfall_by_accelerator[card] = max(
                            0,
                            shortfall_by_accelerator[card] - candidate_capacity)
                    reactivated_count += 1
                    aggregate_covered = (ready_capacity + reactivated_capacity
                                         >= current_target)
                    cards_covered = not any(shortfall_by_accelerator.values())
                    if ((aggregate_covered and cards_covered) or
                            reactivated_count >=
                            _LOGICAL_RETIREMENT_RECOVERY_MAX_REACTIVATIONS_PER_GENERATION
                       ):
                        break
                if reactivated_count:
                    logger.info(
                        f'Reactivated {reactivated_count} recovered logical '
                        f'retirements ({reactivated_capacity} conservative '
                        'slots) '
                        f'to cover a {shortfall}-slot aggregate and '
                        f'exact-card ready-capacity shortfalls '
                        f'{shortfall_by_accelerator}; '
                        'waiting for a newer observed-capacity generation.')
                self._clear_logical_retirement_recovery_if_done()
                return

            adopted = 0
            for info in candidates:
                status = info.status_property
                bounded_precommit = (
                    status.wait_for_idle_before_termination is False and
                    status.logical_retirement_bounded_deadline is True)
                admission_precommit = (
                    self._is_uncommitted_logical_retirement_admission(info) and
                    not self._is_recoverable_uncommitted_logical_retirement(
                        info))
                old_selection = (
                    status.logical_retirement_version,
                    status.logical_retirement_controller_epoch,
                    status.logical_retirement_generation,
                    status.logical_retirement_target_capacity,
                    status.logical_retirement_confirmed_generation,
                    status.logical_retirement_bounded_deadline,
                    status.wait_for_idle_before_termination,
                    status.logical_retirement_committed,
                )
                status.logical_retirement_version = self.latest_version
                status.logical_retirement_controller_epoch = (
                    self._logical_controller_epoch)
                status.logical_retirement_generation = snapshot.generation
                status.logical_retirement_target_capacity = current_target
                # Adoption only refreshes the selection fence. Idle proof and
                # the irreversible teardown commit remain in the existing
                # _finish_logical_retirement path. Normalize a recovered
                # admission precommit back to the canonical strict-idle shape;
                # this confines recognition of that ambiguous crash shape to
                # startup while making a genuine N+1 and fresh idle proof
                # mandatory before worker requeue.
                status.logical_retirement_confirmed_generation = (
                    snapshot.generation if bounded_precommit else None)
                status.logical_retirement_bounded_deadline = bounded_precommit
                if admission_precommit:
                    status.wait_for_idle_before_termination = True
                status.logical_retirement_committed = False
                try:
                    self._persist_replica(info.replica_id, info)
                except Exception as e:  # pylint: disable=broad-except
                    (status.logical_retirement_version,
                     status.logical_retirement_controller_epoch,
                     status.logical_retirement_generation,
                     status.logical_retirement_target_capacity,
                     status.logical_retirement_confirmed_generation,
                     status.logical_retirement_bounded_deadline,
                     status.wait_for_idle_before_termination,
                     status.logical_retirement_committed) = old_selection
                    logger.warning(
                        f'Failed to re-fence recovered logical retirement '
                        f'{info.replica_id}; keeping it off-route for retry: '
                        f'{common_utils.format_exception(e)}')
                    continue
                adopted += 1
            if adopted:
                logger.info(
                    f'Adopted {adopted} recovered logical retirements under '
                    'the current controller fence; keeping them off-route '
                    'until a newer capacity generation revalidates admission '
                    'while preserving their durable drain deadlines.')
        self._clear_logical_retirement_recovery_if_done()

    def _detach_committed_logical_retirement(self, info: ReplicaInfo) -> None:
        """Make an irreversible teardown independent of its selection."""
        assert self._is_committed_logical_retirement(info)
        status = info.status_property
        logger.info(
            f'Recovering committed logical retirement of replica '
            f'{info.replica_id}; continuing teardown independently of its '
            'obsolete controller selection epoch.')
        status.logical_retirement_version = None
        status.logical_retirement_controller_epoch = None
        status.logical_retirement_generation = None
        status.logical_retirement_target_capacity = None
        status.logical_retirement_confirmed_generation = None
        status.logical_retirement_bounded_deadline = False
        status.logical_retirement_committed = False

    def _abort_logical_retirement(self, info: ReplicaInfo, reason: str) -> None:
        """Cancel an optimization retirement and make a healthy backend live.

        An abort discards the victim's elapsed drain and returns it to
        routing, so a wave that keeps aborting makes no progress no matter
        how long it runs. Classify here rather than at the call sites so a
        new caller cannot add an uncounted abort.
        """
        logger.info(f'Aborting logical retirement of replica '
                    f'{info.replica_id}: {reason}.')
        self._drain_proof_stats.record_logical_abort(
            _classify_abort_reason(reason))
        down_thread_pool = self._legacy_mutation_runtime_state(
        ).down_thread_pool
        queued_down = down_thread_pool.get(info.replica_id)
        if (queued_down is not None and not queued_down.is_alive() and
                info.status_property.sky_down_status
                == common_utils.ProcessStatus.SCHEDULED):
            if down_thread_pool.get(info.replica_id) is queued_down:
                down_thread_pool.pop(info.replica_id)
        status = info.status_property
        status.sky_down_status = None
        status.is_scale_down = False
        status.purged = False
        status.drain_cap_seconds = None
        status.drain_started_at = None
        status.wait_for_idle_before_termination = False
        status.logical_retirement_version = None
        status.logical_retirement_controller_epoch = None
        status.logical_retirement_generation = None
        status.logical_retirement_target_capacity = None
        status.logical_retirement_confirmed_generation = None
        status.logical_retirement_bounded_deadline = False
        status.logical_retirement_committed = False
        self._persist_replica(info.replica_id, info)
        self._wait_for_idle_trackers.pop(info.replica_id, None)

    def _finish_logical_retirement(
            self,
            replica_id: int,
            info: ReplicaInfo,
            *,
            require_victim_idle: bool = True,
            replica_infos: list[ReplicaInfo] | None = None) -> None:
        """Recheck and schedule one fenced logical retirement atomically."""
        with self._logical_state_lock:
            retirement_state = self._logical_retirement_state(
                info,
                require_victim_idle=require_victim_idle,
                replica_infos=replica_infos)
            if retirement_state == 'wait':
                return
            if retirement_state == 'abort':
                self._abort_logical_retirement(
                    info, 'the current target or coverage fence changed')
                return
            snapshot = self._logical_reconcile_state.snapshot
            assert snapshot is not None
            info.status_property.logical_retirement_confirmed_generation = (
                snapshot.generation)
            info.status_property.logical_retirement_bounded_deadline = (
                not require_victim_idle)
            self._persist_replica(replica_id, info)
            # The state lock prevents a later sync from invalidating the
            # confirmation between this final proof and shutdown scheduling.
            if self._logical_retirement_state(
                    info,
                    require_victim_idle=require_victim_idle,
                    replica_infos=replica_infos) != 'safe':
                return
            # _terminate_replica atomically clears the durable idle-wait bit
            # with its SCHEDULED down state before installing the worker. Keep
            # both the bit and tracker until that succeeds, so a transient DB
            # failure is retried on the next refresh instead of stranding an
            # off-route SHUTTING_DOWN row until controller restart.
            try:
                self._terminate_replica(replica_id,
                                        replica_drain_delay_seconds=0,
                                        is_scale_down=True,
                                        in_flight_drain_cap_seconds=0)
            except Exception:  # pylint: disable=broad-except
                info.status_property.wait_for_idle_before_termination = True
                try:
                    self._persist_replica(replica_id, info)
                except Exception as restore_error:  # pylint: disable=broad-except
                    logger.warning(
                        'Failed to restore logical retirement retry state for '
                        f'replica {replica_id}: '
                        f'{common_utils.format_exception(restore_error)}')
                raise
            if not require_victim_idle:
                # Bounded completion no longer needs victim-idle evidence.
                # Ordinary completion retains the original deadline until
                # the queued down worker actually starts, so late occupancy
                # cannot strand a budget-delayed retirement forever.
                self._wait_for_idle_trackers.pop(replica_id, None)

    def _refresh_wait_for_idle(self) -> None:
        """Admit strict drains only after fresh LB zero-occupancy proof."""
        down_thread_pool = self._legacy_mutation_runtime_state(
        ).down_thread_pool
        tracker_items = list(self._wait_for_idle_trackers.items())
        if not tracker_items:
            return
        try:
            paid_retirements = paid_retirement.list_for_service(
                self._service_name)
        except Exception as error:  # pylint: disable=broad-except
            # Exact-idle authority is PostgreSQL-owned. A read failure blocks
            # this entire pass; it can never degrade into deadline authority.
            logger.warning('Unable to refresh paid-retirement authority; '
                           'keeping strict teardown blocked: '
                           f'{common_utils.format_exception(error)}')
            return

        replica_infos = serve_state.get_replica_infos_from_ids(
            self._service_name, [replica_id for replica_id, _ in tracker_items])
        tracked_infos: dict[int, ReplicaInfo] = {}
        queued_logical_ids: set[int] = set()
        for replica_id, _ in tracker_items:
            info = replica_infos.get(replica_id)
            if info is None:
                self._wait_for_idle_trackers.pop(replica_id, None)
                continue
            status = info.status_property
            waiting_for_idle = (status.wait_for_idle_before_termination is True)
            down_thread = down_thread_pool.get(replica_id)
            queued_logical = (status.logical_retirement_version is not None and
                              status.is_scale_down is True and
                              status.sky_down_status
                              == common_utils.ProcessStatus.SCHEDULED and
                              down_thread is not None and
                              not down_thread.is_alive())
            # A bounded precommit retirement recovered across a controller
            # restart has wait_for_idle False and no rebuilt down worker, yet
            # its tracker is the only path that resumes teardown once recovery
            # releases the row; evicting it would strand the replica off route
            # with its cluster still up.
            recoverable_logical = (
                down_thread is None and
                (replica_id in self._recovering_logical_retirement_ids or
                 self._is_recoverable_uncommitted_logical_retirement(info)))
            if (not waiting_for_idle and not queued_logical and
                    not recoverable_logical):
                self._wait_for_idle_trackers.pop(replica_id, None)
                continue
            if queued_logical:
                queued_logical_ids.add(replica_id)
            tracked_infos[replica_id] = info

        # Revalidate every logical retirement in this pass from one coherent
        # ready-capacity snapshot.  A service can retain thousands of terminal
        # history rows; decoding that history once per retiring replica starves
        # the controller during restart even though terminal rows can never
        # satisfy a replacement-capacity proof.
        logical_retirement_infos: list[ReplicaInfo] | None = None
        if any(info.status_property.logical_retirement_version is not None
               for info in tracked_infos.values()):
            logical_retirement_infos = serve_state.get_ready_replica_infos(
                self._service_name)

        committed_paid_ids = {
            replica_id for replica_id, info in tracked_infos.items()
            if ((retirement := paid_retirements.get(replica_id)) is not None and
                str(retirement['replica_record_id']) == info.replica_record_id
                and retirement['state'] ==
                paid_retirement.PaidRetirementState.COMMITTED.value)
        }
        # A COMMITTED paid retirement is an irreversible PostgreSQL decision;
        # it needs neither endpoint nor cluster-presence evidence. Excluding
        # it here also prevents a restart-created unbounded tracker from
        # waiting forever on URL discovery before cleanup is re-driven.
        cluster_names = list(
            dict.fromkeys(info.cluster_name
                          for replica_id, info in tracked_infos.items()
                          if replica_id not in committed_paid_ids))
        cluster_status_fields = (
            global_user_state.get_cluster_status_fields(cluster_names)
            if cluster_names else {})
        tracker_items.sort(key=lambda item: (_provider_cleanup_phase_order(
            tracked_infos[item[0]]) if item[0] in tracked_infos else 0))
        for replica_id, opening_state in tracker_items:
            info = tracked_infos.get(replica_id)
            if info is None:
                continue
            state = self._wait_for_idle_trackers.get(replica_id)
            if state is not opening_state:
                continue
            if state.replica_record_id != info.replica_record_id:
                self._wait_for_idle_trackers.pop(replica_id, None)
                continue
            retirement = paid_retirements.get(replica_id)
            exact_paid_retirement = bool(
                retirement is not None and str(retirement['replica_record_id'])
                == info.replica_record_id and retirement['state'] in {
                    paid_retirement.PaidRetirementState.ACTIVE.value,
                    paid_retirement.PaidRetirementState.COMMITTED.value,
                })
            if replica_id in committed_paid_ids:
                self._wait_for_idle_trackers.pop(replica_id, None)
                try:
                    self._terminate_replica(replica_id,
                                            replica_drain_delay_seconds=0,
                                            is_scale_down=True,
                                            in_flight_drain_cap_seconds=0)
                except _BoundOrdinaryLaunchUnresolvedError as error:
                    # The bound-launch reducer remains the sole authority for
                    # this ambiguous row. Its durable paid-retirement commit
                    # keeps it off route without blocking independent peers.
                    logger.warning(
                        'Deferring committed paid retirement for replica %s '
                        'while its bound ordinary launch remains durably '
                        'ambiguous: %s', replica_id,
                        common_utils.format_exception(error))
                continue
            if (exact_paid_retirement and retirement is not None and
                    retirement['state']
                    == paid_retirement.PaidRetirementState.ACTIVE.value and
                    retirement['route_url'] is not None and
                    state.tracker is None):
                state = dataclasses.replace(state,
                                            tracker=_ReplicaDrainTracker(
                                                self, retirement['route_url'],
                                                time.monotonic()),
                                            needs_url_resolution=False)
                self._wait_for_idle_trackers[replica_id] = state
            deadline_expired = time.monotonic() >= state.deadline
            if info.cluster_name not in cluster_status_fields:
                drained = True
            elif state.needs_url_resolution and not deadline_expired:
                # Provider endpoint discovery is owned by the lock-free
                # resolver. Contention or identity uncertainty contributes no
                # drain evidence. A bounded deadline remains an independent
                # cleanup authority, but an unbounded exact-idle retirement
                # keeps retrying until it gets real evidence.
                continue
            else:
                drained = state.tracker is not None and state.tracker()
            logical_retirement = (
                info.status_property.logical_retirement_version is not None)
            if logical_retirement:
                if self._is_committed_logical_retirement(info):
                    # Admission persisted the irreversible bit before starting
                    # the worker. A version change must not return this backend
                    # to routing while the shared termination budget delays the
                    # already-authorized cleanup.
                    continue
                recovering_ids: set[
                    int] = self._recovering_logical_retirement_ids
                if replica_id in recovering_ids:
                    # Recovery reconciliation exclusively owns route
                    # readmission. Its deadline is diagnostic and renews when
                    # evidence is unavailable, so this tracker path must never
                    # convert elapsed wall time into authority to advertise the
                    # victim again.
                    continue
                with self._logical_state_lock:
                    retirement_state = self._logical_retirement_state(
                        info, replica_infos=logical_retirement_infos)
                    if retirement_state == 'abort':
                        self._abort_logical_retirement(
                            info, 'the current target or controller fence '
                            'changed')
                        continue
                if deadline_expired and not drained:
                    retirement_version = (
                        info.status_property.logical_retirement_version)
                    info_version = info.version
                    outdated_backend = (type(retirement_version) is int and
                                        type(info_version) is int and
                                        info_version < retirement_version)
                    if outdated_backend:
                        with self._logical_state_lock:
                            bounded_state = self._logical_retirement_state(
                                info,
                                require_victim_idle=False,
                                replica_infos=logical_retirement_infos)
                            if bounded_state == 'abort':
                                self._abort_logical_retirement(
                                    info, 'the bounded rolling-update '
                                    'coverage fence changed')
                                continue
                        if bounded_state == 'wait':
                            continue
                        logger.warning(
                            f'Outdated replica {replica_id} reached its '
                            'post-routing idle-proof deadline; replacement '
                            'capacity still covers the target, so completing '
                            'the bounded rolling-update retirement.')
                        self._drain_proof_stats.record_bounded_completion()
                        self._finish_logical_retirement(
                            replica_id,
                            info,
                            require_victim_idle=False,
                            replica_infos=logical_retirement_infos)
                        continue
                    with self._logical_state_lock:
                        self._abort_logical_retirement(
                            info, 'post-routing idle proof timed out')
                    continue
                if not drained:
                    continue
                if replica_id in queued_logical_ids:
                    # Keep revalidating the original deadline until the
                    # resource budget admits this already-scheduled worker.
                    # Admission below performs the final strict state proof.
                    continue
                self._finish_logical_retirement(
                    replica_id, info, replica_infos=logical_retirement_infos)
                continue
            if exact_paid_retirement:
                assert retirement is not None
                if not drained:
                    continue
                authority = paid_retirement.FreshZeroAuthority(
                    service_hash=retirement['service_hash'],
                    demand_source_epoch=int(retirement['demand_source_epoch']),
                    demand_feed_generation=int(
                        retirement['demand_feed_generation']),
                    capacity_plan_generation=int(
                        retirement['capacity_plan_generation']),
                    capacity_plan_sha256=retirement['capacity_plan_sha256'],
                    route_generation=int(retirement['route_generation']))
                info.status_property.wait_for_idle_before_termination = False
                if (self._service_hash is None or
                        self._controller_owner is None or
                        not serve_state.commit_paid_retirement(
                            self._service_name,
                            replica_id,
                            info,
                            authority,
                            expected_service_hash=self._service_hash,
                            expected_controller_owner=self._controller_owner)):
                    info.status_property.wait_for_idle_before_termination = True
                    continue
                self._drain_proof_stats.record_proved_drained()
                self._wait_for_idle_trackers.pop(replica_id, None)
                try:
                    self._terminate_replica(replica_id,
                                            replica_drain_delay_seconds=0,
                                            is_scale_down=True,
                                            in_flight_drain_cap_seconds=0)
                except _BoundOrdinaryLaunchUnresolvedError as error:
                    # Idle proof and the PostgreSQL commit are already
                    # irreversible.  Preserve that exact row for bound-launch
                    # reconciliation, but continue retiring independent peers.
                    logger.warning(
                        'Deferring proved-idle paid retirement for replica %s '
                        'while its bound ordinary launch remains durably '
                        'ambiguous: %s', replica_id,
                        common_utils.format_exception(error))
                continue
            if not drained and not deadline_expired:
                continue
            drain_cap: int | None = 0
            if drained:
                self._drain_proof_stats.record_proved_drained()
            else:
                self._drain_proof_stats.record_deadline_expiry_without_proof()
                drain_cap = info.status_property.drain_cap_seconds
                if drain_cap is None:
                    drain_cap = self._resolve_drain_cap_seconds(
                        replica_id, info)
                logger.warning(
                    f'Strict idle wait for replica {replica_id} reached its '
                    f'{drain_cap}s deadline without fresh zero-occupancy '
                    'proof; falling back to a bounded graceful drain.')
            info.status_property.wait_for_idle_before_termination = False
            self._persist_replica(replica_id, info)
            self._wait_for_idle_trackers.pop(replica_id, None)
            self._terminate_replica(replica_id,
                                    replica_drain_delay_seconds=0,
                                    is_scale_down=True,
                                    in_flight_drain_cap_seconds=drain_cap)

    def _clear_known_unknown_capacity_replacements(self) -> None:
        """End degraded-wave attribution after a real capacity sample.

        Called under the manager lock. Holding the logical-state lock across
        the small fenced persistence prevents a concurrent LB generation from
        changing the backend back to unknown between proof and marker clear.
        """
        replacement_ids: set[int] = self._unknown_capacity_replacement_ids
        if not replacement_ids:
            return
        with self._logical_state_lock:
            snapshot = self._logical_reconcile_state.snapshot
            if snapshot is None or not self._logical_snapshot_is_fresh(
                    snapshot):
                return
            known_ids = {
                replica_id for replica_id in replacement_ids
                if replica_id not in snapshot.unknown_replica_ids and
                snapshot.observed_slots_by_replica_id.get(replica_id, 0) > 0
            }
            if not known_ids:
                return
            infos = serve_state.get_replica_infos_from_ids(
                self._service_name, sorted(known_ids))
            for replica_id in sorted(known_ids):
                info = infos.get(replica_id)
                if (info is not None and
                        info.unknown_capacity_replacement is True):
                    info.unknown_capacity_replacement = False
                    self._persist_replica(replica_id, info)
                replacement_ids.discard(replica_id)

    @with_lock
    def reconcile_fresh_zero_paid_retirements(
        self,
        authority: paid_retirement.FreshZeroAuthority,
        replica_infos: list[ReplicaInfo],
    ) -> bool:
        """Persist exact-idle-only retirement for every live paid replica."""
        if (self._update_recovery_required or self._service_hash is None or
                self._controller_owner is None or
                authority.service_hash != self._service_hash):
            return False
        changed = False
        for original in sorted(replica_infos, key=lambda item: item.replica_id):
            if (original.is_terminal or original.is_zero_cost is True or
                    original.status_property.is_scale_down or
                    original.status_property.sky_down_status is not None):
                continue
            info = serve_state.get_replica_info_from_id(self._service_name,
                                                        original.replica_id)
            if (info is None or
                    info.replica_record_id != original.replica_record_id or
                    info.is_terminal or info.is_zero_cost is True or
                    info.status_property.is_scale_down or
                    info.status_property.sky_down_status is not None):
                continue
            requires_idle_proof = bool(
                info.is_ready or
                info.status == serve_state.ReplicaStatus.NOT_READY or
                (isinstance(info.status_property.first_ready_time,
                            (int, float)) and
                 not isinstance(info.status_property.first_ready_time, bool) and
                 info.status_property.first_ready_time >= 0))
            status = info.status_property
            status.is_scale_down = True
            status.purged = False
            status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
            # An economic zero retirement has no elapsed-time authority.
            # The separate PostgreSQL intent is the durable discriminator;
            # these legacy bounded-drain fields remain intentionally empty.
            status.drain_cap_seconds = None
            status.drain_started_at = None
            status.wait_for_idle_before_termination = requires_idle_proof
            record = serve_state.admit_paid_retirement(
                self._service_name,
                info.replica_id,
                info,
                authority,
                requires_idle_proof=requires_idle_proof,
                expected_service_hash=self._service_hash,
                expected_controller_owner=self._controller_owner)
            if record is None:
                # Every item in this wave shares one fresh-zero authority.
                # Contention or a stale authority on any item should yield to
                # provider progress and a newer demand reconcile, rather than
                # repeatedly attempting the same service-wide writer guard.
                return changed
            changed = True
            if requires_idle_proof:
                self._wait_for_idle_trackers.pop(info.replica_id, None)
                self._register_wait_for_idle(info,
                                             deadline=math.inf,
                                             replica_url=record['route_url'])
            else:
                try:
                    self._terminate_replica(info.replica_id,
                                            replica_drain_delay_seconds=0,
                                            is_scale_down=True,
                                            in_flight_drain_cap_seconds=0)
                except _BoundOrdinaryLaunchUnresolvedError as error:
                    # This row was committed without an idle wait because it
                    # never served traffic.  Keep its ambiguous bound launch
                    # for exact reconciliation without aborting the rest of
                    # the fresh-zero retirement wave.
                    logger.warning(
                        'Deferring immediate paid retirement for replica %s '
                        'while its bound ordinary launch remains durably '
                        'ambiguous: %s', info.replica_id,
                        common_utils.format_exception(error))
        return changed

    @with_lock
    def cancel_uncommitted_paid_retirements(
        self,
        service_hash: str,
        positive_demand_generation: int,
    ) -> bool:
        """Cancel only pre-teardown intents under newer positive demand."""
        if (self._update_recovery_required or self._service_hash is None or
                self._controller_owner is None or
                service_hash != self._service_hash):
            return False
        changed = False
        records = paid_retirement.list_for_service(self._service_name)
        active_ids = [
            replica_id for replica_id, record in records.items() if
            record['state'] == paid_retirement.PaidRetirementState.ACTIVE.value
        ]
        infos = serve_state.get_replica_infos_from_ids(self._service_name,
                                                       active_ids)
        for replica_id in sorted(active_ids):
            record = records[replica_id]
            info = infos.get(replica_id)
            if (info is None or
                    str(record['replica_record_id']) != info.replica_record_id):
                continue
            status = info.status_property
            status.sky_down_status = None
            status.is_scale_down = False
            status.purged = False
            status.drain_cap_seconds = None
            status.drain_started_at = None
            status.wait_for_idle_before_termination = False
            if not serve_state.cancel_paid_retirement(
                    self._service_name,
                    replica_id,
                    info,
                    positive_demand_generation,
                    expected_service_hash=self._service_hash,
                    expected_controller_owner=self._controller_owner):
                continue
            self._wait_for_idle_trackers.pop(replica_id, None)
            changed = True
        return changed

    @with_lock
    def scale_down(self,
                   replica_id: int,
                   purge: bool = False,
                   wait_for_idle: bool = False,
                   expected_version: int | None = None) -> None:
        if self._update_recovery_required:
            return
        if (expected_version is not None and
                expected_version != self.latest_version):
            logger.info('Discarding stale physical scale-down for replica '
                        f'{replica_id} from version {expected_version}; '
                        f'manager is at version {self.latest_version}.')
            return
        # Retirement drain: bounded by the replica's version spec,
        # completing early once the LB provably stopped routing to the
        # replica and reports zero in-flight for it. A purge is a forceful
        # cleanup of an already-failed replica: nothing routable is being
        # retired, so it must not wait out a graceful-drain cap.
        if wait_for_idle and not purge:
            self._defer_scale_down_until_idle(replica_id)
            return
        drain_cap = (None
                     if purge else self._resolve_drain_cap_seconds(replica_id))
        self._terminate_replica(replica_id,
                                replica_drain_delay_seconds=0,
                                is_scale_down=True,
                                purge=purge,
                                in_flight_drain_cap_seconds=drain_cap)

    def scale_down_logically(
        self,
        replica_id: int,
        target_capacity: int,
        version: int,
        reconcile_generation: int,
        target_capacity_by_accelerator: LogicalAcceleratorState = (),
        accelerator_shapes: LogicalAcceleratorState = ()
    ) -> None:
        self.scale_down_logically_batch([replica_id], target_capacity, version,
                                        reconcile_generation,
                                        target_capacity_by_accelerator,
                                        accelerator_shapes)

    @with_lock
    def scale_down_logically_batch(
        self,
        replica_ids: list[int],
        target_capacity: int,
        version: int,
        reconcile_generation: int,
        target_capacity_by_accelerator: LogicalAcceleratorState = (),
        accelerator_shapes: LogicalAcceleratorState = ()
    ) -> None:
        """Accept one logical retirement wave from one fleet snapshot."""
        if (self._update_recovery_required or not replica_ids):
            return
        if not self._uses_logical_replicas:
            raise RuntimeError('Logical scale-down sent to a physical '
                               'replica service.')
        with self._logical_state_lock:
            logical_state = self._logical_reconcile_state
            if not self._logical_target_fence_holds(
                    version,
                    reconcile_generation,
                    target_capacity,
                    target_capacity_by_accelerator,
                    accelerator_shapes,
                    logical_state=logical_state):
                logger.info(
                    'Discarding stale logical scale-down batch for version '
                    f'{version}, generation {reconcile_generation}, target '
                    f'{target_capacity} with {len(replica_ids)} victim(s).')
                return
            retirement_shelter = logical_state.retirement_shelter
            if (retirement_shelter is not None and
                    not retirement_shelter.authority_current):
                logger.info('Holding logical scale-down batch because the '
                            'sequenced reserved-fill allocation is '
                            'unavailable.')
                return
            retirement_components = _logical_target_state_components(
                _logical_retirement_target(logical_state))
            if (retirement_components is None or retirement_components[:2]
                    != (version, reconcile_generation)):
                logger.info('Discarding logical scale-down batch without a '
                            'coherent same-generation retirement floor.')
                return
            (_, _, retirement_target_capacity, retirement_target_by_accelerator,
             retirement_accelerator_shapes) = retirement_components
            snapshot = logical_state.snapshot
            assert snapshot is not None

            # This is the only fleet read for the whole wave. Resolve victims
            # from the same durable snapshot used for capacity accounting so a
            # concurrent row transition cannot mix two proofs.
            replica_infos = serve_state.get_replica_infos(self._service_name)
            infos_by_id = {info.replica_id: info for info in replica_infos}
            # Failed logical launches commonly leave a large wave of durable
            # replica rows whose clusters were never created. The old
            # per-victim teardown path re-read both databases and deleted one
            # row per transaction while holding the manager lock. Snapshot
            # cluster existence once so eligible finished/never-started
            # victims can use one fenced delete below. A live launch or any
            # down worker retains the existing cancellation/cleanup path.
            bulk_absent_replica_ids: set[int] = set()
            if self._service_hash is not None:
                legacy_runtime = self._legacy_mutation_runtime_state()
                launch_pool = legacy_runtime.launch_thread_pool
                down_pool = legacy_runtime.down_thread_pool
                absence_candidates: list[ReplicaInfo] = []
                seen_candidate_ids: set[int] = set()
                for replica_id in replica_ids:
                    if replica_id in seen_candidate_ids:
                        continue
                    seen_candidate_ids.add(replica_id)
                    candidate = infos_by_id.get(replica_id)
                    if (candidate is None or candidate.is_terminal or
                            candidate.status_property.is_scale_down is True or
                            candidate.status_property.first_ready_time
                            is not None or replica_id in down_pool):
                        continue
                    launch_thread = launch_pool.get(replica_id)
                    if (launch_thread is not None and launch_thread.is_alive()):
                        continue
                    try:
                        cleanup_fence = (
                            reserved_capacity.parse_protocol_v2_cleanup_fence(
                                candidate))
                    except exceptions.KubernetesPhysicalClusterIdentityError:
                        # A malformed physical-identity row must flow through
                        # _terminate_replica, which records FAILED_CLEANUP.
                        continue
                    if cleanup_fence is not None:
                        # Cluster-table absence is not proof that protocol-v2
                        # provider resources are absent. Never batch-delete
                        # its durable cleanup authority.
                        continue
                    absence_candidates.append(candidate)
                existing_cluster_names = (
                    serve_utils.get_existing_replica_cluster_names(
                        absence_candidates))
                bulk_absent_replica_ids = {
                    info.replica_id
                    for info in absence_candidates
                    if info.cluster_name not in existing_cluster_names
                }
            ready_capacity = 0
            committed_capacity = 0
            ready_by_accelerator = {
                card: 0 for card, _ in retirement_accelerator_shapes
            }
            committed_by_accelerator = {
                card: 0 for card, _ in retirement_accelerator_shapes
            }
            capacity_by_id: dict[int, tuple[int, int, str | None]] = {}
            for candidate in replica_infos:
                committed_width = 0
                ready_width = 0
                contributes = (not candidate.is_terminal and
                               candidate.status_property.is_scale_down
                               is not True)
                if contributes and candidate.version == version:
                    planned = int(candidate.planned_capacity)
                    observed = snapshot.observed_slots_by_replica_id.get(
                        candidate.replica_id)
                    if (candidate.is_ready and observed is not None and
                            candidate.replica_id
                            not in snapshot.unknown_replica_ids):
                        ready_width = min(planned, observed)
                        committed_width = ready_width
                    else:
                        committed_width = planned
                elif (contributes and candidate.version < version and
                      candidate.is_ready):
                    # Historical physical rows do not carry authoritative
                    # logical widths, but every READY old backend provides at
                    # least one serving slot. Match the rolling bridge's
                    # conservative coverage floor at the final manager fence.
                    committed_width = 1
                    ready_width = 1
                card = None
                if contributes and retirement_accelerator_shapes:
                    card = self._logical_replica_accelerator(
                        candidate,
                        retirement_accelerator_shapes,
                        require_configured_shape=(candidate.version == version))
                ready_capacity += ready_width
                committed_capacity += committed_width
                if card is not None:
                    ready_by_accelerator[card] += ready_width
                    committed_by_accelerator[card] += committed_width
                capacity_by_id[candidate.replica_id] = (committed_width,
                                                        ready_width, card)

            accepted = 0
            absent_finished_launch_infos: list[ReplicaInfo] = []
            immediate_teardown_infos: list[ReplicaInfo] = []
            logical_drain_infos: list[ReplicaInfo] = []
            seen_ids: set[int] = set()
            for replica_id in replica_ids:
                if replica_id in seen_ids:
                    continue
                seen_ids.add(replica_id)
                info = infos_by_id.get(replica_id)
                if (info is None or info.is_terminal or
                        info.status_property.is_scale_down is True):
                    continue

                committed_width, ready_width, card = capacity_by_id.get(
                    replica_id, (0, 0, None))
                if (retirement_accelerator_shapes and card is None and
                        info.version >= version):
                    continue
                has_served = (info.status_property.first_ready_time is not None
                              and info.status_property.first_ready_time >= 0)
                if not has_served:
                    victim_width = committed_width
                    committed_after = committed_capacity - victim_width
                    card_committed_after = dict(committed_by_accelerator)
                    if card is not None:
                        card_committed_after[card] -= committed_width
                    if (committed_after < retirement_target_capacity or
                            not self._logical_card_capacity_covers(
                                card_committed_after,
                                retirement_target_by_accelerator)):
                        continue
                    if replica_id in bulk_absent_replica_ids:
                        absent_finished_launch_infos.append(info)
                    else:
                        immediate_teardown_infos.append(info)
                else:
                    if (replica_id in snapshot.unknown_replica_ids or
                            snapshot.in_flight_by_replica_id.get(replica_id)
                            != 0):
                        continue
                    victim_ready_width = ready_width
                    if info.version == version and victim_ready_width == 0:
                        observed = snapshot.observed_slots_by_replica_id.get(
                            replica_id)
                        if observed is None:
                            continue
                    ready_after = ready_capacity - victim_ready_width
                    card_ready_after = dict(ready_by_accelerator)
                    if card is not None:
                        card_ready_after[card] -= victim_ready_width
                    if (ready_after < retirement_target_capacity or
                            not self._logical_card_capacity_covers(
                                card_ready_after,
                                retirement_target_by_accelerator)):
                        continue
                    logical_drain_infos.append(info)

                # Mutate the in-memory proof only after accepting this victim
                # into the provider-free wave. Cleanup scheduling below is
                # v2-first; if it raises, the remainder aborts and the next
                # autoscaler tick retries under a fresh fence.
                committed_capacity -= committed_width
                ready_capacity -= ready_width
                if card is not None:
                    committed_by_accelerator[card] -= committed_width
                    ready_by_accelerator[card] -= ready_width
                accepted += 1

            # Scheduling is provider-free while holding the manager lock.
            # Preserve v2-first ordering for worker admission; each worker or
            # the lock-free drain resolver acquires its own provider phase.
            scheduled_infos = [
                (info, False) for info in immediate_teardown_infos
            ]
            scheduled_infos.extend((info, True) for info in logical_drain_infos)
            scheduled_infos.sort(
                key=lambda item: _provider_cleanup_phase_order(item[0]))
            for info, requires_idle in scheduled_infos:
                if requires_idle:
                    self._defer_scale_down_until_idle(
                        info.replica_id,
                        logical_retirement=(version, reconcile_generation,
                                            retirement_target_capacity),
                        replica_info=info)
                else:
                    self._terminate_replica(info.replica_id,
                                            replica_drain_delay_seconds=0,
                                            is_scale_down=True,
                                            in_flight_drain_cap_seconds=0)

            if absent_finished_launch_infos:
                self._remove_replicas(absent_finished_launch_infos)
                for info in absent_finished_launch_infos:
                    replica_id = info.replica_id
                    # Delete local worker bookkeeping only after the durable
                    # fenced delete succeeds. A failed transaction therefore
                    # leaves the entire wave retryable on the next tick.
                    legacy_runtime = self._legacy_mutation_runtime_state()
                    for mapping in (
                            legacy_runtime.launch_thread_pool,
                            legacy_runtime.replica_to_request_id,
                            legacy_runtime.replica_to_logical_launch_fence):
                        if replica_id in mapping:
                            mapping.pop(replica_id)
                    self._clear_failed_cleanup_retry(replica_id)
                logger.info(
                    f'Removed {len(absent_finished_launch_infos)} replicas from '
                    'the replica table in one batch (clusters were never '
                    'created).')

            logger.info(
                'Logical scale-down batch completed for version '
                f'{version}, generation {reconcile_generation}, action target '
                f'{target_capacity}, retirement floor '
                f'{retirement_target_capacity}: requested={len(replica_ids)}, '
                f'accepted={accepted}, skipped={len(replica_ids) - accepted}.')

    # We don't need to add lock here since every caller of this function
    # will acquire the lock.
    # Thread-pool bound for the per-probe-round parallel cloud pre-filter
    # over failed-probe spot replicas (see _cloud_instance_looks_alive).
    _PREEMPTION_PREFILTER_PARALLELISM = 16
    _PROBE_ROUND_MAX_PARALLELISM = 256

    def _get_readiness_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        """Return the manager's bounded, reusable readiness I/O executor."""
        with self._readiness_executor_lock:
            executor = self._readiness_executor
            if executor is None:
                executor = subprocess_utils.ContextThreadPoolExecutor(
                    max_workers=self._PROBE_ROUND_MAX_PARALLELISM,
                    thread_name_prefix='serve-readiness')
                self._readiness_executor = executor
            return executor

    def _shutdown_readiness_executor(self) -> None:
        """Release readiness workers after this manager is terminal."""
        with self._readiness_executor_lock:
            executor = self._readiness_executor
            self._readiness_executor = None
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    def _cloud_instance_looks_alive(
        self,
        info: ReplicaInfo,
        *,
        phase_admission: provider_phase.ProviderPhaseAdmission | None = None,
        handle: Any = _NOT_PROVIDED,
    ) -> _PreemptionPrefilterResult:
        """Classify exact-handle liveness without mutating cluster state.

        This is the only provider-side interruption classifier used by Serve:
        one provider read, no SSH probe, cluster-table refresh, manager lock,
        or database write. During a fleet cold start every not-yet-listening
        replica fails its endpoint probe by design; live provider evidence
        prevents those failures from being mistaken for interruptions.

        Alive requires EVERY launched node to be reported UP, mirroring the
        full refresh's partial-cluster semantics ("some nodes UP" is
        abnormal: the cluster is partially preempted or terminated). Any
        shortfall — fewer instances than launched_nodes, or any non-UP
        instance — is interruption evidence. The caller must revalidate the
        opening replica lifecycle before applying it.

        Exact protocol-v2 Kubernetes ABSENT carries a private typed proof for
        the ordered reducer. It is not post-teardown evidence: the normal down
        worker must still quiesce the request and prove a fresh absence before
        deleting durable state.

        Errors count as live/unproven: a transient provider/API error must not
        stampede a whole cold-starting fleet into teardown; a genuinely dead
        instance keeps failing its probe and is re-checked next round. For an
        already-launched trackable replica, absence of its exact opening
        cluster handle is interruption evidence, still subject to the same
        lifecycle revalidation by the reducer.
        """
        try:
            if handle is _NOT_PROVIDED:
                handle = global_user_state.get_handle_from_cluster_name(
                    info.cluster_name)
            provider_fence = reserved_capacity.protocol_v2_provider_fence(
                info,
                handle,
                phase_admission=phase_admission,
                wait_for_initializer=phase_admission is None)
            if handle is None:
                return _PreemptionPrefilterResult(
                    _PreemptionPrefilterDisposition.INTERRUPTED)
            assert isinstance(handle, backends.CloudVmRayResourceHandle)
            observation_boundary = time.monotonic()
            with provider_fence:
                cleanup_fence = (
                    reserved_capacity.parse_protocol_v2_cleanup_fence(info))
                launched_resources = handle.launched_resources
                if (cleanup_fence is not None and
                        launched_resources is not None and isinstance(
                            launched_resources.cloud, clouds.Kubernetes)):
                    cluster_name_on_cloud = handle.cluster_name_on_cloud
                    if (not isinstance(cluster_name_on_cloud, str) or
                            not cluster_name_on_cloud):
                        return _PreemptionPrefilterResult(
                            _PreemptionPrefilterDisposition.LIVE_OR_UNPROVEN)
                    presence = (
                        reserved_capacity.probe_physical_replica_presence(
                            cleanup_fence,
                            info.cluster_name,
                            observed_after=observation_boundary,
                            cluster_name_on_cloud=cluster_name_on_cloud))
                    if (presence is
                            reserved_capacity.PhysicalReplicaPresence.ABSENT):
                        return _PreemptionPrefilterResult(
                            _PreemptionPrefilterDisposition.
                            EXACT_KUBERNETES_ABSENT,
                            _ExactKubernetesAbsenceProof(
                                cleanup_fence=cleanup_fence,
                                cluster_name=info.cluster_name,
                                replica_record_id=info.replica_record_id))
                    # PRESENT or an API uncertainty is not evidence of
                    # interruption. Retry next round without stampeding the
                    # full refresh path.
                    return _PreemptionPrefilterResult(
                        _PreemptionPrefilterDisposition.LIVE_OR_UNPROVEN)
                statuses = backend_utils.query_cluster_instance_statuses(handle)
            if len(statuses) < handle.launched_nodes:
                return _PreemptionPrefilterResult(
                    _PreemptionPrefilterDisposition.INTERRUPTED)
            if all(status == status_lib.ClusterStatus.UP
                   for status, _ in statuses.values()):
                return _PreemptionPrefilterResult(
                    _PreemptionPrefilterDisposition.LIVE_OR_UNPROVEN)
            return _PreemptionPrefilterResult(
                _PreemptionPrefilterDisposition.INTERRUPTED)
        except exceptions.KubernetesPhysicalClusterIdentityError as error:
            logger.error(
                f'Preemption pre-filter has unknown provider identity for '
                f'replica {info.replica_id} ({info.cluster_name}): '
                f'{common_utils.format_exception(error)}')
            return _PreemptionPrefilterResult(
                _PreemptionPrefilterDisposition.IDENTITY_UNCERTAIN)
        except exceptions.ProviderPhaseError:
            # Contention is not liveness evidence. Let the caller defer this
            # exact lifecycle without advancing its readiness failure window.
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f'Preemption pre-filter failed for replica '
                         f'{info.replica_id} ({info.cluster_name}); treating '
                         f'as alive: {common_utils.format_exception(e)}')
            return _PreemptionPrefilterResult(
                _PreemptionPrefilterDisposition.LIVE_OR_UNPROVEN)

    def _is_reclaimable_zero_cost_kubernetes(self, info: ReplicaInfo) -> bool:
        """Whether a non-spot replica runs as reclaimable research capacity."""
        placer = self._spot_placer
        if info.is_spot or placer is None:
            return False
        replica_location = info.get_spot_location()
        if (replica_location is None or
                str(replica_location.cloud).lower() != 'kubernetes'):
            return False
        return any(
            not location.use_spot and
            str(location.cloud).lower() == 'kubernetes' and
            spot_placer.locations_match_placement(replica_location, location)
            for location in placer.zero_cost_locations())

    def _is_interruptible_replica(self, info: ReplicaInfo) -> bool:
        """Whether infrastructure loss should enter replacement lifecycle."""
        return (info.is_spot or self._is_reclaimable_zero_cost_kubernetes(info))

    def _apply_confirmed_preemption(self,
                                    info: ReplicaInfo,
                                    cluster_status: status_lib.ClusterStatus |
                                    None,
                                    *,
                                    persist_placement: bool = True) -> None:
        """Apply already-confirmed interruption evidence without provider I/O."""
        cluster_status_str = ('' if cluster_status is None else
                              f' (status: {cluster_status.value})')
        interruption = ('spot-preempted'
                        if info.is_spot else 'reclaimed from zero-cost '
                        'Kubernetes capacity')
        logger.info(f'Replica {info.replica_id} was {interruption}'
                    f'{cluster_status_str}.')
        if info.system_recovery_disposition in (
                system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
                system_recovery_state.SystemRecoveryDisposition.CAPABLE):
            system_oom_recovery_observability.record_for_replica(
                'preemption_observed', info)
        info.status_property.preempted = True
        if info.is_spot and self._spot_placer is not None:
            spot_location = info.get_spot_location()
            assert spot_location is not None
            self._spot_placer.set_preemptive(spot_location, reason='preempted')
            if persist_placement:
                self._persist_spot_placement_state_if_dirty()

    #################################
    # ReplicaManager Daemon Threads #
    #################################

    @staticmethod
    def _replica_card_for_catalog(
            info: ReplicaInfo, canonical_by_name: dict[str, str]) -> str | None:
        """Resolve one replica's persisted card without cluster-table I/O."""
        accelerators = None
        location = info.get_spot_location()
        if location is not None:
            accelerators = location.accelerators
        if not accelerators:
            accelerators = (info.resources_override or {}).get('accelerators')
        if not accelerators and len(canonical_by_name) == 1:
            # An ordinary single-resource service has no placer-selected
            # resources_override before its first cloud mutation. Its exact
            # card is still deterministic when the complete service catalog
            # contains one card. Preserve fail-closed behavior for a
            # multi-card catalog, where an unpinned optimizer launch has no
            # authoritative pre-launch card identity.
            return next(iter(canonical_by_name.values()))
        if not isinstance(accelerators, dict) or len(accelerators) != 1:
            return None
        return canonical_by_name.get(str(next(iter(accelerators))).casefold())

    def _logical_pending_launch_admission_decision(
        self,
        candidate_replica_id: int | None = None,
    ) -> _LogicalPendingLaunchAdmission:
        """Calculate exact-card authority for not-yet-started demand launches.

        The replica row and local launch thread can outlive the autoscaler tick
        that created them, including across controller recovery. Only a fresh,
        complete exact-card target may turn such a row into a cloud mutation.
        READY/STARTING/PROVISIONING supply is counted first. Reserved-fill and
        special replacement rows keep their independent fences; ordinary
        zero-cost demand rows win the remaining demand budget before paid rows.

        ``reason`` and ``details`` are bounded, secret-free diagnostics for the
        final pre-cloud guard. They distinguish a stale target from a candidate
        rejected by current supply without weakening either fail-closed path.
        """
        if (not self._uses_logical_replicas or
                not self._logical_exact_accelerator_shapes):
            return _LogicalPendingLaunchAdmission(applicable=False,
                                                  target_fence=None,
                                                  authorized_ids=frozenset(),
                                                  reason='not-applicable')

        with self._logical_state_lock:
            logical_state = self._logical_reconcile_state
            target_fence = logical_state.target
            target_state = _logical_target_state_components(target_fence)
            if (target_state is None or target_fence is None or
                    len(target_fence) != 5):
                return _LogicalPendingLaunchAdmission(
                    applicable=True,
                    target_fence=None,
                    authorized_ids=frozenset(),
                    reason='target-missing-or-malformed',
                    details=f'target={target_fence!r}')
            if not self._logical_reconcile_fence_holds(
                    target_fence,
                    require_fresh_occupancy=False,
                    logical_state=logical_state):
                snapshot = logical_state.snapshot
                snapshot_summary = (None if snapshot is None else
                                    (snapshot.version, snapshot.generation,
                                     round(
                                         time.monotonic() -
                                         snapshot.received_at, 3)))
                return _LogicalPendingLaunchAdmission(
                    applicable=True,
                    target_fence=None,
                    authorized_ids=frozenset(),
                    reason='target-not-authoritative',
                    details=(f'target={target_fence!r}, '
                             f'snapshot_version_generation_age='
                             f'{snapshot_summary!r}, '
                             f'latest_version={self.latest_version!r}, '
                             f'pending_version='
                             f'{self._pending_version!r}'))
            (version, _, _, target_by_accelerator,
             accelerator_shapes) = target_state
            configured = {
                str(card).casefold()
                for card in self._logical_exact_accelerator_shapes
            }
            published = {str(card).casefold() for card, _ in accelerator_shapes}
            if configured != published:
                return _LogicalPendingLaunchAdmission(
                    applicable=True,
                    target_fence=None,
                    authorized_ids=frozenset(),
                    reason='accelerator-catalog-mismatch',
                    details=(f'configured={sorted(configured)!r}, '
                             f'published={sorted(published)!r}'))

        canonical_by_name = {
            card.casefold(): card for card, _ in accelerator_shapes
        }
        targets = {card: 0 for card, _ in accelerator_shapes}
        targets.update(dict(target_by_accelerator))
        baseline = {card: 0 for card in targets}
        candidates: dict[str, list[ReplicaInfo]] = {card: [] for card in targets}
        authorized_ids: set[int] = set()
        candidate_summary: tuple[Any, ...] | None = None
        replica_infos = serve_state.get_replica_infos(self._service_name)
        for info in replica_infos:
            if (info.is_terminal or info.version != version or
                    info.status_property.is_scale_down is True or
                    info.status_property.preempted is True):
                continue
            card = self._replica_card_for_catalog(info, canonical_by_name)
            if card is None:
                if info.replica_id == candidate_replica_id:
                    candidate_summary = (info.replica_id, info.status.value,
                                         info.version, None,
                                         info.planned_capacity)
                continue
            planned = int(info.planned_capacity)
            is_pending = ((info.status == serve_state.ReplicaStatus.PENDING and
                           info.status_property.sky_launch_status
                           in (None, common_utils.ProcessStatus.SCHEDULED)) or
                          info.replica_id == candidate_replica_id)
            special_pending = bool(
                info.reserved_fill or info.unknown_capacity_replacement or
                type(info.cost_rebalance_for_replica_id) is int)
            if info.replica_id == candidate_replica_id:
                candidate_summary = (info.replica_id, info.status.value,
                                     info.version, card, planned, is_pending,
                                     special_pending)
            if is_pending and not special_pending:
                candidates[card].append(info)
            else:
                baseline[card] += planned
                if is_pending:
                    authorized_ids.add(info.replica_id)

        for card, card_candidates in candidates.items():
            remaining = max(0, targets[card] - baseline[card])

            def _candidate_key(info: ReplicaInfo) -> tuple[bool, float, int]:
                created_at = info.created_at
                if not isinstance(created_at, (int, float)):
                    created_at = float('-inf')
                return (info.is_zero_cost
                        is not True, float(created_at), info.replica_id)

            for info in sorted(card_candidates, key=_candidate_key):
                planned = int(info.planned_capacity)
                if planned > remaining:
                    continue
                authorized_ids.add(info.replica_id)
                remaining -= planned

        # A newer autoscaler tick may publish while the fleet read above is in
        # flight. Never let an authorization set cross that target boundary.
        with self._logical_state_lock:
            current_state = self._logical_reconcile_state
            if (current_state.target != target_fence or
                    not self._logical_reconcile_fence_holds(
                        target_fence,
                        require_fresh_occupancy=False,
                        logical_state=current_state)):
                return _LogicalPendingLaunchAdmission(
                    applicable=True,
                    target_fence=None,
                    authorized_ids=frozenset(),
                    reason='target-changed-during-replica-read',
                    details=(f'previous_target={target_fence!r}, '
                             f'current_target={current_state.target!r}'))
        candidate_ids_first_16 = {
            card: [info.replica_id for info in card_candidates[:16]]
            for card, card_candidates in candidates.items()
        }
        candidate_counts = {
            card: len(card_candidates)
            for card, card_candidates in candidates.items()
        }
        return _LogicalPendingLaunchAdmission(
            applicable=True,
            target_fence=target_fence,
            authorized_ids=frozenset(authorized_ids),
            reason='ready',
            details=(f'targets={targets!r}, baseline={baseline!r}, '
                     f'candidate_counts={candidate_counts!r}, '
                     f'candidate_ids_first_16={candidate_ids_first_16!r}, '
                     f'candidate={candidate_summary!r}'))

    def _logical_pending_launch_admission(
        self,
        candidate_replica_id: int | None = None,
    ) -> tuple[bool, LogicalTargetState | None, set[int]]:
        """Return the compatibility tuple for pending-launch callers."""
        decision = self._logical_pending_launch_admission_decision(
            candidate_replica_id=candidate_replica_id)
        return (decision.applicable, decision.target_fence,
                set(decision.authorized_ids))

    def _queued_logical_launch_fence_decision(
        self, replica_id: int
    ) -> tuple[bool, str, _LogicalPendingLaunchAdmission | None]:
        """Return the final cloud-launch decision and a stable reason code."""
        fence_map = (self._legacy_mutation_runtime_state().
                     replica_to_logical_launch_fence)
        fence = fence_map.get(replica_id)
        if fence is None:
            return False, 'replica-fence-missing', None
        admission = self._logical_pending_launch_admission_decision(
            candidate_replica_id=replica_id)
        if not admission.applicable:
            return False, f'admission-{admission.reason}', admission
        if admission.target_fence is None:
            return False, admission.reason, admission
        if not _logical_target_intent_preserved(admission.target_fence, fence):
            return False, 'target-intent-changed', admission
        if replica_id not in admission.authorized_ids:
            return False, 'replica-not-authorized', admission
        return True, 'authorized', admission

    def _queued_logical_launch_fence_holds(self, replica_id: int) -> bool:
        """Revalidate target and current supply before every sdk.launch()."""
        allowed, reason, admission = (
            self._queued_logical_launch_fence_decision(replica_id))
        if not allowed:
            fence_map = (self._legacy_mutation_runtime_state().
                         replica_to_logical_launch_fence)
            stored_fence = fence_map.get(replica_id)
            authorized_ids = ([] if admission is None else sorted(
                admission.authorized_ids))
            logger.info(
                f'Rejecting final logical cloud launch for replica '
                f'{replica_id}: reason={reason}; '
                f'stored_target={stored_fence!r}; '
                f'current_target='
                f'{None if admission is None else admission.target_fence!r}; '
                f'authorized_count={len(authorized_ids)}; '
                f'authorized_ids_first_16={authorized_ids[:16]!r}; '
                f'details={None if admission is None else admission.details}.')
        return allowed

    # A failed replica is kept so the operator sees why its version failed.
    # Only a teardown that already SUCCEEDED proves the row holds nothing;
    # FAILED_CLEANUP and UNKNOWN are exactly the unresolved-cleanup rows the
    # provider fences retain on purpose, so they are never pruned here.
    _PRUNABLE_SUPERSEDED_STATUSES = frozenset({
        serve_state.ReplicaStatus.FAILED,
        serve_state.ReplicaStatus.FAILED_INITIAL_DELAY,
        serve_state.ReplicaStatus.FAILED_PROBING,
        serve_state.ReplicaStatus.FAILED_PROVISION,
    })

    def _prune_superseded_failed_replicas(self) -> None:
        """Drop failed records whose version the service has moved past.

        ``_handle_sky_down_finish`` already refuses to keep a failed record
        for a version mismatch, but it only decides once, as that replica's
        teardown finishes. A replica that failed while its version was still
        the latest is therefore retained forever, and nothing re-examines it
        when the service moves on. Across dozens of versions those records
        accumulate without bound and bury the current version's real failures.

        Re-apply the same policy whenever the applied version advances, and
        only to rows that already proved their teardown succeeded. Scanning
        on that edge (rather than every tick) keeps the refresher's budgeted
        per-tick scans untouched.
        """
        if not self._superseded_prune_pending:
            return
        latest_version = self.latest_version
        prunable_infos: list[ReplicaInfo] = []
        for info in serve_state.get_replica_infos(self._service_name):
            if info.version == latest_version:
                continue
            if (info.status_property.sky_down_status
                    != common_utils.ProcessStatus.SUCCEEDED):
                continue
            if (info.status not in self._PRUNABLE_SUPERSEDED_STATUSES):
                continue
            prunable_infos.append(info)
        if not prunable_infos:
            self._superseded_prune_pending = False
            return
        prunable_infos.sort(key=lambda info: info.replica_id)
        self._remove_replicas(prunable_infos)
        self._superseded_prune_pending = False
        superseded_versions = sorted({info.version for info in prunable_infos})
        logger.info(
            f'Removed {len(prunable_infos)} superseded failed replicas from '
            f'the replica table in one batch (versions '
            f'{superseded_versions!r} superseded by {latest_version}).')

    @with_lock
    def _refresh_thread_pool(self) -> None:
        """Route mutation completion through the current mutation runtime."""
        if self._update_recovery_required:
            return
        self._legacy_mutation_runtime_state().refresh(
            self._refresh_legacy_mutation_runtime)
        self._prune_superseded_failed_replicas()

    def _refresh_legacy_mutation_runtime(self) -> None:
        """Refresh the launch/down thread pool.

        This function will checks all sky.launch and sky.down thread on
        the fly. If any of them finished, it will update the status of the
        corresponding replica.
        """
        # This remains the current launch/down completion owner.  It is not
        # deprecated by the retired action-authority proposal.
        legacy_runtime = self._legacy_mutation_runtime_state()
        # A lost PostgreSQL commit acknowledgement is resolved from the exact
        # row before any replacement worker is constructed.
        self._reconcile_ambiguous_logical_retirement_commits()
        # A pre-field SCHEDULED retirement stays off-route across an upgrade
        # until current replacement capacity proves it can be re-driven.
        self._reconcile_legacy_uncertain_logical_retirements()
        # Current-format uncommitted retirements retain their original drain
        # deadlines and are either adopted under a fresh controller fence or
        # selectively reactivated for current capacity.
        self._reconcile_recovering_logical_retirements()
        # Economic retirements are persisted off-route first and enter the
        # normal termination pool only after the LB proves they are idle.
        self._refresh_wait_for_idle()
        self._clear_known_unknown_capacity_replacements()
        # To avoid `dictionary changed size during iteration` error.
        launch_thread_pool_snapshot = list(
            legacy_runtime.launch_thread_pool.items())
        # Process finished launch threads BEFORE taking the cross-process
        # resources lock: this pass performs per-replica DB writes and, for a
        # failed launch, durable teardown scheduling. Provider cleanup runs
        # later on the down worker. None of the completion reduction needs the
        # cross-process resources lock; only the admission pass below does.
        launch_to_admit: list[tuple[int, thread_utils.SafeThread,
                                    ReplicaInfo]] = []
        running_launches_to_start: list[tuple[int, _ReplicaLaunchThread,
                                              ReplicaInfo]] = []
        pending_launches: list[tuple[int, thread_utils.SafeThread,
                                     ReplicaInfo]] = []
        successful_spot_locations: dict[spot_placer.Location, float] = {}
        failed_spot_locations: dict[spot_placer.Location, str] = {}
        generic_failed_spot_locations: set[spot_placer.Location] = set()
        # One query for every finished launch thread; walking the pool with
        # per-replica reads makes queued PENDING launches re-hit the DB every
        # tick until they are admitted.
        finished_launches = [(replica_id, t)
                             for replica_id, t in launch_thread_pool_snapshot
                             if not t.is_alive()]
        unfenced_launch_failures = {
            replica_id for replica_id, t in finished_launches
            if isinstance(t.exception, _UnfencedExternalLbLaunchError)
        }
        superseded_launches = {
            replica_id for replica_id, t in finished_launches
            if isinstance(t.exception, _ReplicaLaunchSupersededError)
        }
        launch_infos = serve_state.get_replica_infos_from_ids(
            self._service_name,
            [replica_id for replica_id, _ in finished_launches])
        # A completed local worker can outlive its durable replica row. This
        # happens when another reconciliation path removes the row before the
        # worker result is observed. Treat durable absence as terminal local
        # cleanup, not as a controller-wide assertion failure: the latter
        # aborts this refresh before unrelated teardown workers are handled.
        # Partition stale workers before spot-placement evidence is collected,
        # since that pass also dereferences every finished launch row.
        stale_finished_launches = [(replica_id, t)
                                   for replica_id, t in finished_launches
                                   if replica_id not in launch_infos]
        for replica_id, t in stale_finished_launches:
            logger.warning(
                f'Discarding completed launch worker for replica '
                f'{replica_id}: its durable replica row no longer exists.')
            if legacy_runtime.launch_thread_pool.get(replica_id) is t:
                legacy_runtime.launch_thread_pool.pop(replica_id)
                legacy_runtime.replica_to_request_id.pop(replica_id)
                legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)
        if stale_finished_launches:
            stale_replica_ids = {
                replica_id for replica_id, _ in stale_finished_launches
            }
            finished_launches = [(replica_id, t)
                                 for replica_id, t in finished_launches
                                 if replica_id not in stale_replica_ids]
        stale_identity_launches = [
            (replica_id, t)
            for replica_id, t in finished_launches
            if (not isinstance(t, _ReplicaLaunchThread) or t.replica_record_id
                != launch_infos[replica_id].replica_record_id or
                t.service_hash != self._service_hash or
                t.controller_owner != self._controller_owner)
        ]
        for replica_id, t in stale_identity_launches:
            logger.warning('Discarding stale launch worker for replica %s.',
                           replica_id)
            if legacy_runtime.launch_thread_pool.get(replica_id) is t:
                legacy_runtime.launch_thread_pool.pop(replica_id)
                legacy_runtime.replica_to_request_id.pop(replica_id)
                legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)
        if stale_identity_launches:
            stale_workers = {id(t) for _, t in stale_identity_launches}
            finished_launches = [(replica_id, t)
                                 for replica_id, t in finished_launches
                                 if id(t) not in stale_workers]
        # A same-ID worker can be replaced after the opening pool snapshot.
        # Identity-filter the process-local slot before any placer or paid
        # accounting effect is derived from its completion.
        finished_launches = [
            (replica_id, t)
            for replica_id, t in finished_launches
            if legacy_runtime.launch_thread_pool.get(replica_id) is t
        ]
        # A failed/ambiguous Thread.start can leave an exact durable RUNNING
        # reservation with a worker that provably never obtained a native
        # thread identity.  Never reduce that shape as launch success.  Under
        # the same cross-pod gate, restore it to SCHEDULED for ordinary
        # admission; if the lock or exact row is unavailable, this row emits
        # no placement/economic evidence and remains conservatively counted.
        running_without_native_thread = [
            (replica_id, t)
            for replica_id, t in finished_launches
            if (isinstance(t, _ReplicaLaunchThread) and t.ident is None and
                launch_infos[replica_id].status_property.sky_launch_status ==
                common_utils.ProcessStatus.RUNNING)
        ]
        never_started_reservations = [
            (replica_id, t)
            for replica_id, t in running_without_native_thread
            if legacy_runtime.never_started_launch_reservations.get(
                replica_id) == (t.replica_record_id, id(t))
        ]
        if running_without_native_thread:
            never_started_ids = {
                replica_id for replica_id, _ in running_without_native_thread
            }
            finished_launches = [(replica_id, t)
                                 for replica_id, t in finished_launches
                                 if replica_id not in never_started_ids]
            proven_never_started_ids: set[int] = set()
            for replica_id, t in never_started_reservations:
                try:
                    fresh = (self._restore_never_started_launch_to_scheduled(
                        replica_id, t.replica_record_id))
                except Exception as error:  # pylint: disable=broad-except
                    logger.warning(
                        'Unable to recover never-started launch reservation '
                        'for replica %s: %s', replica_id,
                        common_utils.format_exception(error))
                    continue
                if fresh is not None:
                    legacy_runtime.never_started_launch_reservations.pop(
                        replica_id, None)
                    launch_infos[replica_id] = fresh
                    pending_launches.append((replica_id, t, fresh))
                    proven_never_started_ids.add(replica_id)
            # Every other local identity-less RUNNING worker is either an
            # exact bound-request adopter reconstructed after restart or the
            # candidate whose reservation commit acknowledgement was lost.
            # RUNNING already consumes P. Start that existing worker directly;
            # never demote it based on a freshly constructed Thread.ident.
            for replica_id, t in running_without_native_thread:
                if replica_id in proven_never_started_ids:
                    continue
                if t.bound_ordinary_launch:
                    # A failed/ambiguous restore is not proof that the
                    # provider generation never started. Revoke local
                    # rollback authority before adopting the immutable bound
                    # operation as already charged RUNNING work.
                    legacy_runtime.never_started_launch_reservations.pop(
                        replica_id, None)
                    running_launches_to_start.append(
                        (replica_id, t, launch_infos[replica_id]))
                else:
                    # A pointerless legacy RUNNING row has no immutable
                    # executable generation to adopt.  Preserve its P debit
                    # and let provider/quiescence reconciliation adjudicate
                    # it; starting this reconstructed callable could duplicate
                    # an unacknowledged provider effect.
                    logger.warning(
                        'Retaining ambiguous legacy RUNNING launch for '
                        'replica %s without provider re-drive.', replica_id)
        # A teardown signal is durably persisted before it is delivered to
        # the worker. Exclude those completions from all placement/economic
        # evidence: cancellation is neither a successful provider launch nor
        # a provider capacity failure. The normal completion reducer below
        # still routes the exact worker into cleanup.
        placement_finished_launches = [
            (replica_id, t)
            for replica_id, t in finished_launches
            if (launch_infos[replica_id].status_property.sky_launch_status !=
                common_utils.ProcessStatus.INTERRUPTED and not (isinstance(
                    t, _ReplicaLaunchThread) and t.teardown_requested.is_set()))
        ]
        finished_spot_locations: dict[int, spot_placer.Location] = {}
        if self._spot_placer is not None:
            for replica_id, t in placement_finished_launches:
                info = launch_infos.get(replica_id)
                assert info is not None, replica_id
                if info.status == serve_state.ReplicaStatus.PENDING:
                    continue
                location = info.get_spot_location()
                if location is None:
                    continue
                resolved_location = self._spot_placer.resolve_location(location)
                if resolved_location is not None:
                    location = resolved_location
                finished_spot_locations[replica_id] = location
                launch_error = t.exception
                if isinstance(launch_error, _ReplicaLaunchCapacityError):
                    previous_reason = failed_spot_locations.get(location)
                    failed_spot_locations[location] = (
                        'quota' if launch_error.reason == 'quota' or
                        previous_reason == 'quota' else 'capacity')
                elif t.format_exc is not None:
                    generic_failed_spot_locations.add(location)
                else:
                    selected_at = info.created_at
                    if t.format_exc is None and selected_at is not None:
                        successful_spot_locations[location] = max(
                            selected_at,
                            successful_spot_locations.get(
                                location, selected_at))

            # Commit the placement evidence before the per-replica durable
            # writes and teardown preparation below. Either can fail, but a
            # failed launch must still bench its location so queued siblings
            # cannot be admitted on the next refresh.
            for location, selected_at in successful_spot_locations.items():
                if location not in failed_spot_locations:
                    self._spot_placer.set_active(location,
                                                 selected_at=selected_at)
            for location, reason in failed_spot_locations.items():
                if reason == 'quota':
                    self._spot_placer.set_quota_limited(location)
                else:
                    self._spot_placer.set_preemptive(location,
                                                     reason='capacity')
            for location in (generic_failed_spot_locations -
                             failed_spot_locations.keys()):
                self._spot_placer.release_retry(location)
            self._persist_spot_placement_state_if_dirty()

        completed_launches: list[tuple[int, ReplicaInfo, bool]] = []
        bound_completed_launches: list[tuple[int, ReplicaInfo, bool, bool]] = []
        bound_pre_effect_retries: list[tuple[int, ReplicaInfo,
                                             _ReplicaLaunchThread]] = []
        superseded_launch_infos: list[tuple[int, ReplicaInfo]] = []
        capacity_launch_failures: set[int] = set()
        quota_launch_failures: set[int] = set()
        bound_capacity_launch_failures: set[int] = set()
        bound_quota_launch_failures: set[int] = set()
        for replica_id, t in finished_launches:
            if legacy_runtime.launch_thread_pool.get(replica_id) is not t:
                continue
            info = launch_infos.get(replica_id)
            assert info is not None, replica_id
            bound_ordinary_launch = bool(
                isinstance(t, _ReplicaLaunchThread) and t.bound_ordinary_launch)
            teardown_requested = (isinstance(t, _ReplicaLaunchThread) and
                                  t.teardown_requested.is_set())
            interrupted = (info.status_property.sky_launch_status ==
                           common_utils.ProcessStatus.INTERRUPTED)
            if bound_ordinary_launch and (interrupted or teardown_requested):
                # Teardown normally owns completion first, but an AMBIGUOUS
                # association cannot authorize cancellation or provider
                # cleanup. Inspect it before the generic teardown branch;
                # otherwise _terminate_replica installs another adopter for
                # the same rejected cancel and permanently excludes this row
                # from provider reconciliation.
                try:
                    teardown_projection = (
                        request_postgres.inspect_bound_ordinary_launch(
                            self._service_name, replica_id,
                            info.replica_record_id))
                    teardown_is_ambiguous = bool(
                        teardown_projection is not None and
                        _bound_projection_classification(teardown_projection)
                        == 'AMBIGUOUS')
                    provider_reconcilable = bool(
                        teardown_is_ambiguous and isinstance(
                            teardown_projection.context,
                            ordinary_launch_binding.BoundNonPoolLaunchContext))
                    provider_present_context = (
                        self._bound_non_pool_provider_present_cleanup_context(
                            info, teardown_projection)
                        if provider_reconcilable else None)
                except Exception as error:  # pylint: disable=broad-except
                    logger.warning(
                        'Unable to inspect finished bound teardown for '
                        'replica %s; retaining its local owner: %s', replica_id,
                        common_utils.format_exception(error))
                    continue
                if (provider_reconcilable and provider_present_context is None):
                    # Durable association identity, not this finished thread,
                    # is the retry source. Detach only process-local
                    # bookkeeping and let the exact provider observer commit
                    # PRESENT/ABSENT before any cleanup path can proceed.
                    legacy_runtime.launch_thread_pool.pop(replica_id)
                    legacy_runtime.replica_to_request_id.pop(replica_id)
                    legacy_runtime.replica_to_logical_launch_fence.pop(
                        replica_id)
                    self._schedule_non_pool_provider_reconciliation(
                        info, teardown_projection.context)
                    logger.error(
                        'Finished bound teardown for replica %s is durably '
                        'ambiguous; detached its local launch owner and '
                        'scheduled exact provider reconciliation.', replica_id)
                    continue
            if interrupted or teardown_requested:
                if not self._service_is_cleanup_authorized():
                    continue
                legacy_runtime.launch_thread_pool.pop(replica_id)
                legacy_runtime.replica_to_request_id.pop(replica_id)
                legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)
                status = info.status_property
                is_scale_down = (status.is_scale_down or status.preempted)
                purge = status.purged
                self._terminate_replica(
                    replica_id,
                    replica_drain_delay_seconds=0,
                    is_scale_down=is_scale_down,
                    purge=purge,
                    in_flight_drain_cap_seconds=status.drain_cap_seconds)
                continue
            if (info.status == serve_state.ReplicaStatus.PENDING and
                (not bound_ordinary_launch or t.ident is None)):
                # A thread is not alive before its first ``start()``.  Route
                # every never-started durable PENDING row through admission
                # before looking at bound-request completion; otherwise a
                # freshly queued bound launch is mistaken for a finished
                # worker and can be reduced or discarded without ever
                # reaching the provider.  A started bound worker may project
                # a retry back to PENDING, so its non-null thread identity
                # must still reach bound completion handling below.
                pending_launches.append((replica_id, t, info))
                continue
            if bound_ordinary_launch:
                ownership_lost = isinstance(t.exception,
                                            _ReplicaLaunchOwnershipLostError)
                remaining = request_postgres.inspect_bound_ordinary_launch(
                    self._service_name, replica_id, info.replica_record_id)
                unresolved = bool(
                    isinstance(t.exception, _BoundOrdinaryLaunchUnresolvedError)
                    or remaining is not None)
                if ownership_lost:
                    logger.info(
                        'Discarding bound ordinary-launch worker for replica '
                        '%s after controller ownership loss.', replica_id)
                    legacy_runtime.launch_thread_pool.pop(replica_id)
                    legacy_runtime.replica_to_request_id.pop(replica_id)
                    legacy_runtime.replica_to_logical_launch_fence.pop(
                        replica_id)
                    continue
                if unresolved:
                    if (remaining is not None and
                            _bound_projection_classification(remaining)
                            == 'AMBIGUOUS'):
                        cleanup_context = (
                            self.
                            _bound_non_pool_provider_present_cleanup_context(
                                info, remaining))
                        if cleanup_context is not None:
                            logger.info(
                                'Scheduling exact immediate cleanup for '
                                'provider-present reserved-fill replica %s.',
                                replica_id)
                            try:
                                self._terminate_replica(
                                    replica_id,
                                    replica_drain_delay_seconds=0,
                                    is_scale_down=True,
                                    in_flight_drain_cap_seconds=0)
                            except Exception as error:  # pylint: disable=broad-except
                                logger.warning(
                                    'Could not schedule provider-present '
                                    'cleanup for replica %s: %s', replica_id,
                                    common_utils.format_exception(error))
                                self._schedule_failed_cleanup_retry(replica_id)
                            continue
                        self._schedule_non_pool_provider_reconciliation(
                            info, remaining.context)
                        logger.error(
                            'Retaining finished bound ordinary-launch worker '
                            'for replica %s: its exact association is durably '
                            'ambiguous (%s).', replica_id, t.exception)
                        continue
                    # A transport failure can happen before admission commits,
                    # or after a commit whose response was lost.  Replace the
                    # finished worker while this controller is still live:
                    # the stable submission ID adopts an existing pointer or
                    # safely performs the never-committed first admission.
                    retry_request_id = (
                        legacy_runtime.replica_to_request_id.get(replica_id))
                    legacy_runtime.launch_thread_pool.pop(replica_id)
                    legacy_runtime.replica_to_request_id.pop(replica_id)
                    legacy_runtime.replica_to_logical_launch_fence.pop(
                        replica_id)
                    if info.status in (serve_state.ReplicaStatus.PENDING,
                                       serve_state.ReplicaStatus.PROVISIONING):
                        try:
                            redriven = (
                                self.
                                _redrive_bound_ordinary_launch_after_pre_effect(
                                    info))
                        except Exception as error:  # pylint: disable=broad-except
                            logger.warning(
                                'Could not locally re-drive unresolved bound '
                                'launch for replica %s: %s', replica_id,
                                common_utils.format_exception(error))
                            redriven = False
                        if not redriven:
                            # Retain a retry owner.  A later refresh repeats
                            # the exact stable admission without waiting for a
                            # controller restart.
                            legacy_runtime.launch_thread_pool[replica_id] = t
                    elif (info.status == serve_state.ReplicaStatus.SHUTTING_DOWN
                         ):
                        # Teardown can persist INTERRUPTED and then lose the
                        # final admission acknowledgement while joining this
                        # worker.  The finished unresolved marker is no longer
                        # an executable owner, but dropping it without
                        # rebuilding teardown would strand the durable row: a
                        # live controller does not rerun startup recovery.
                        status = info.status_property
                        is_scale_down = (status.is_scale_down or
                                         status.preempted)
                        purge = status.purged
                        try:
                            self._terminate_replica(
                                replica_id,
                                replica_drain_delay_seconds=0,
                                is_scale_down=is_scale_down,
                                purge=purge,
                                in_flight_drain_cap_seconds=(
                                    status.drain_cap_seconds))
                        except Exception as error:  # pylint: disable=broad-except
                            logger.warning(
                                'Could not locally re-drive teardown after '
                                'unresolved bound launch for replica %s; '
                                'retaining its retry owner: %s', replica_id,
                                common_utils.format_exception(error))
                            legacy_runtime.launch_thread_pool[replica_id] = t
                            if retry_request_id is not None:
                                legacy_runtime.replica_to_request_id[
                                    replica_id] = retry_request_id
                    continue
                if isinstance(t.exception,
                              _BoundOrdinaryLaunchPreEffectTerminalError):
                    assert isinstance(t, _ReplicaLaunchThread)
                    bound_pre_effect_retries.append((replica_id, info, t))
                    continue
                superseded = isinstance(t.exception,
                                        _ReplicaLaunchSupersededError)
                error_in_sky_launch = t.format_exc is not None
                if isinstance(t.exception, _ReplicaLaunchCapacityError):
                    if t.exception.reason == 'quota':
                        bound_quota_launch_failures.add(replica_id)
                    else:
                        bound_capacity_launch_failures.add(replica_id)
                bound_completed_launches.append(
                    (replica_id, info, error_in_sky_launch, superseded))
                continue
            if replica_id in superseded_launches:
                superseded_launch_infos.append((replica_id, info))
                continue
            # sky.launch finished
            # TODO(tian): Try-catch in thread, and have an enum return
            # value to indicate which type of failure happened.
            # Currently we only have user code failure since the
            # retry_until_up flag is set to True, but it will be helpful
            # when we enable user choose whether to retry or not.
            logger.info(f'Launch thread for replica {replica_id} finished.')
            error_in_sky_launch = False
            if t.format_exc is not None:
                is_capacity_failure = isinstance(t.exception,
                                                 _ReplicaLaunchCapacityError)
                if is_capacity_failure:
                    if t.exception.reason == 'quota':
                        quota_launch_failures.add(replica_id)
                    else:
                        capacity_launch_failures.add(replica_id)
                else:
                    logger.warning(f'Launch thread for replica {replica_id} '
                                   f'exited abnormally with exception '
                                   f'{t.format_exc}. Terminating...')
                info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.FAILED)
                if replica_id in unfenced_launch_failures:
                    # The current API requires a durable launch fence in
                    # external-LB mode. A legacy controller cannot acquire
                    # one by retrying a replica; make this failure
                    # unrecoverable so the autoscaler stops creating rows
                    # until the operator purges/recreates the service.
                    info.status_property.user_app_failed = True
                error_in_sky_launch = True
            else:
                info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.SUCCEEDED)
            if replica_id in finished_spot_locations:
                if replica_id in (capacity_launch_failures |
                                  quota_launch_failures):
                    info.status_property.failed_spot_availability = True
            completed_launches.append((replica_id, info, error_in_sky_launch))

        availability_launch_failures = (capacity_launch_failures |
                                        quota_launch_failures)
        affected_pool_keys: set[str] = set()
        if availability_launch_failures:
            affected_pool_keys = {
                info.paid_capacity_pool_key
                for replica_id, info, _ in completed_launches
                if replica_id in availability_launch_failures and
                isinstance(info.paid_capacity_pool_key, str) and
                paid_capacity.frontier_key_from_pool_key(
                    info.paid_capacity_pool_key) is not None
            }
            pool_count = len(affected_pool_keys)
            pool_count_text = str(pool_count) if pool_count else 'unknown'
            logger.warning(
                'Provider-availability launch failure wave: '
                f'capacity_failures={len(capacity_launch_failures)}, '
                f'quota_failures={len(quota_launch_failures)}, '
                f'exact_pools={pool_count_text}; '
                'shared paid-capacity admission will close the affected '
                'pools. Per-replica tracebacks remain in replica logs.')

        # Persist one completed launch wave in one transaction while holding
        # the manager lock. A per-replica transaction here delays admission of
        # already-selected teardown workers behind O(wave size) PostgreSQL
        # round trips. Keep local worker tracking intact until the batch commit
        # succeeds so a transient write failure is retried on the next tick.
        completed_replica_infos = [
            (replica_id, info) for replica_id, info, _ in completed_launches
        ]
        if completed_launches:
            outcomes = {}
            for replica_id, info, error_in_sky_launch in completed_launches:
                if not error_in_sky_launch:
                    outcome = paid_capacity.LaunchOutcome.SUCCESS
                elif replica_id in capacity_launch_failures:
                    outcome = paid_capacity.LaunchOutcome.CAPACITY_FAILURE
                elif replica_id in quota_launch_failures:
                    outcome = paid_capacity.LaunchOutcome.QUOTA_FAILURE
                else:
                    outcome = paid_capacity.LaunchOutcome.OTHER_FAILURE
                outcomes[replica_id] = outcome
            paid_outcome_persisted = paid_capacity.persist_completed_launches(
                service_name=self._service_name,
                service_hash=self._service_hash,
                controller_owner=self._controller_owner,
                replica_infos=completed_replica_infos,
                outcomes=outcomes)
            if paid_outcome_persisted is None:
                self._persist_replicas(completed_replica_infos)
            elif not paid_outcome_persisted.ownership_valid:
                raise RuntimeError(
                    f'Service {self._service_name!r} controller ownership '
                    'changed while persisting paid-capacity launch outcomes.')
            elif (availability_launch_failures and affected_pool_keys &
                  set(paid_outcome_persisted.applied_pool_keys)):
                # The PostgreSQL transaction above is the authorization
                # boundary. Wake only after it applied the typed failure to a
                # matching exact paid pool and released that claim; the
                # controller then performs one ordinary target-fenced
                # autoscaler tick.
                self._notify_scale_reconciliation()
            for replica_id, info, _ in completed_launches:
                self._emit_ordinary_launch_handoff_event(
                    info,
                    ordinary_launch_handoff.EventKind.SERVE_RESULT_PROJECTED,
                    legacy_runtime.replica_to_request_id.get(replica_id))

        # Bound reducers have already committed ReplicaInfo, exact paid-pool
        # feedback, pointer clearing, and retention-pin release in one
        # transaction. They release claims for terminal outcomes, while a
        # non-cancelled PRE_EFFECT terminal keeps its exact claim for the next
        # generation. Re-running the legacy batch here would apply the same
        # economic outcome twice and could overwrite the typed projection with
        # a stale pre-reducer snapshot.
        if bound_completed_launches or bound_pre_effect_retries:
            if (bound_capacity_launch_failures or bound_quota_launch_failures):
                self._notify_scale_reconciliation()
            for replica_id, info, _, _ in bound_completed_launches:
                self._emit_ordinary_launch_handoff_event(
                    info,
                    ordinary_launch_handoff.EventKind.SERVE_RESULT_PROJECTED,
                    legacy_runtime.replica_to_request_id.get(replica_id))
            for replica_id, info, _ in bound_pre_effect_retries:
                self._emit_ordinary_launch_handoff_event(
                    info,
                    ordinary_launch_handoff.EventKind.SERVE_RESULT_PROJECTED,
                    legacy_runtime.replica_to_request_id.get(replica_id))

        # Projection made these exact rows PENDING and cleared their old
        # association pointer. Replace the completed local worker with a fresh
        # recovery-style admission: the stable submission identity plus the
        # settled predecessor makes the request transaction allocate
        # generation+1, and the retained paid claim is reused exactly. If
        # local reconstruction is temporarily unavailable, keep the completed
        # marker so the next refresh retries without teardown.
        for replica_id, info, old_thread in bound_pre_effect_retries:
            legacy_runtime.launch_thread_pool.pop(replica_id)
            legacy_runtime.replica_to_request_id.pop(replica_id)
            legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)
            try:
                retry_enqueued = (
                    self._redrive_bound_ordinary_launch_after_pre_effect(info))
            except Exception as error:  # pylint: disable=broad-except
                logger.warning(
                    'Could not enqueue generation+1 bound ordinary launch '
                    'for replica %s; retaining the pending retry marker: %s',
                    replica_id, common_utils.format_exception(error))
                retry_enqueued = False
            if not retry_enqueued:
                # _launch_replica either installs a complete fresh worker or
                # installs none. Preserve a retry owner if admission deferred.
                if replica_id not in legacy_runtime.launch_thread_pool:
                    legacy_runtime.launch_thread_pool[replica_id] = old_thread
                continue
            logger.info(
                'Completed bound launch recovery for replica %s after exact '
                'pre-effect settlement.', replica_id)

        # Retire v2 failures before any ordinary drain/provider work. The
        # worker remains registered until its row outcome has been persisted;
        # _terminate_replica consumes try-only phase contention and always
        # preserves independently retryable cleanup ownership.
        cleanup_launches = [
            (replica_id, info, error_in_sky_launch, False)
            for replica_id, info, error_in_sky_launch in completed_launches
        ]
        cleanup_launches.extend((replica_id, info, False, True)
                                for replica_id, info in superseded_launch_infos)
        for replica_id, info, error_in_sky_launch, superseded in sorted(
                cleanup_launches,
                key=lambda item: _provider_cleanup_phase_order(item[1])):
            if superseded:
                launch_thread = legacy_runtime.launch_thread_pool[replica_id]
                rejection = launch_thread.exception
                logger.info(
                    f'Cleaning up replica {replica_id}: its cloud launch '
                    'authority was superseded before the next cloud mutation '
                    f'({rejection}).')
                # Keep the completed worker registered until termination so
                # _terminate_replica durably marks the interrupted launch and
                # clears all of its launch bookkeeping atomically with the
                # teardown intent.
                self._terminate_replica(replica_id,
                                        replica_drain_delay_seconds=0,
                                        is_scale_down=True,
                                        in_flight_drain_cap_seconds=0)
                continue
            legacy_runtime.launch_thread_pool.pop(replica_id)
            legacy_runtime.replica_to_request_id.pop(replica_id)
            legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)
            if error_in_sky_launch:
                # Teardown after update replica info since
                # _terminate_replica will update the replica info too.
                self._terminate_replica(replica_id,
                                        replica_drain_delay_seconds=0)

        for replica_id, info, error_in_sky_launch, superseded in (
                bound_completed_launches):
            if error_in_sky_launch:
                # Keep the finished worker registered until teardown proves
                # the exact request pointer is clear. Supersession is a
                # scale-down decision; a launch failure keeps the historical
                # failure record under existing Serve semantics.
                self._terminate_replica(
                    replica_id,
                    replica_drain_delay_seconds=0,
                    is_scale_down=superseded,
                    in_flight_drain_cap_seconds=(0 if superseded else None))
            else:
                legacy_runtime.launch_thread_pool.pop(replica_id)
                legacy_runtime.replica_to_request_id.pop(replica_id)
                legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)

        if pending_launches:
            if self._spot_placer is not None:
                # Workspace policy is centrally mutable while this controller
                # is long-lived. Reload exactly once per queued admission wave
                # so the final pre-thread fence cannot use a startup snapshot;
                # subsequent replanning also sees the refreshed policy.
                self._spot_placer.refresh_workspace_policy()
            # Queued launches for one service share the same controller-owner
            # proof; re-checking it per replica only burns DB work and log
            # budget without changing the admission decision for this tick.
            authorization = self._service_launch_authorization()
            (logical_admission_applies, logical_target_fence,
             logical_authorized_ids) = (
                 self._logical_pending_launch_admission())
            for replica_id, t, info in pending_launches:
                if authorization is None:
                    logger.warning(
                        f'Deferring queued launch for replica {replica_id}: '
                        'controller ownership is temporarily unverifiable.')
                    continue
                if not authorization:
                    # Do not delete the durable PENDING row: the successor
                    # owns it and recovery will either re-drive its
                    # incarnation-scoped cluster or garbage-collect it. Only
                    # discard this stale manager's never-started thread.
                    logger.warning(
                        f'Discarding queued launch for replica {replica_id} '
                        'after controller ownership loss.')
                    legacy_runtime.launch_thread_pool.pop(replica_id)
                    legacy_runtime.replica_to_request_id.pop(replica_id)
                    legacy_runtime.replica_to_logical_launch_fence.pop(
                        replica_id)
                    continue
                special_logical_launch = bool(
                    info.reserved_fill or info.unknown_capacity_replacement or
                    type(info.cost_rebalance_for_replica_id) is int)
                if logical_admission_applies and not special_logical_launch:
                    if logical_target_fence is None:
                        logger.info(
                            f'Deferring queued logical launch for replica '
                            f'{replica_id}: no fresh complete exact-card '
                            'target is authoritative.')
                        continue
                    if replica_id not in logical_authorized_ids:
                        # Recheck and commit the supersession while target
                        # publication is excluded. The thread has never
                        # started, so this cannot preempt serving work.
                        with self._logical_state_lock:
                            if not self._logical_reconcile_fence_holds(
                                    logical_target_fence,
                                    require_fresh_occupancy=False):
                                continue
                            logger.info(
                                f'Superseding queued logical launch for '
                                f'replica {replica_id}: current exact-card '
                                'capacity already covers its target budget.')
                            self._terminate_replica(
                                replica_id,
                                replica_drain_delay_seconds=0,
                                is_scale_down=True,
                                in_flight_drain_cap_seconds=0)
                        continue
                if self._spot_placer is not None:
                    location = info.get_spot_location()
                    resolved_location = (
                        self._spot_placer.resolve_location(location)
                        if location is not None else None)
                    if resolved_location is not None:
                        location = resolved_location
                    if (location is not None and
                        (location in failed_spot_locations or
                         not self._spot_placer.is_launch_admissible(
                             location, selected_at=info.created_at))):
                        # This exact placement failed after the batch was
                        # queued but before this thread was admitted. Drop the
                        # never-started row so the autoscaler replans it on the
                        # next tick against the next-cheapest active location.
                        logger.info(
                            f'Discarding queued launch for replica '
                            f'{replica_id}: placement {location} is benched.')
                        self._remove_replica(replica_id, info.replica_record_id)
                        legacy_runtime.launch_thread_pool.pop(replica_id)
                        legacy_runtime.replica_to_request_id.pop(replica_id)
                        legacy_runtime.replica_to_logical_launch_fence.pop(
                            replica_id)
                        continue
                # sky.launch not started yet; admitted below under the
                # resources lock.
                if (logical_target_fence is not None and
                        not special_logical_launch):
                    legacy_runtime.replica_to_logical_launch_fence[
                        replica_id] = logical_target_fence
                launch_to_admit.append((replica_id, t, info))

        # Snapshot AFTER the finished-launch pass so down threads it scheduled
        # (via _terminate_replica for failed launches) are admitted this tick.
        down_thread_pool_snapshot = list(
            legacy_runtime.down_thread_pool.items())
        concurrent_downs = sum(
            1 for _, t in down_thread_pool_snapshot if t.is_alive())
        down_to_admit: list[tuple[int, thread_utils.SafeThread,
                                  ReplicaInfo]] = []
        running_downs_to_start: list[tuple[int, _ReplicaDownThread,
                                           ReplicaInfo]] = []
        finished_downs = [(replica_id, t)
                          for replica_id, t in down_thread_pool_snapshot
                          if not t.is_alive()]
        down_infos = serve_state.get_replica_infos_from_ids(
            self._service_name,
            [replica_id for replica_id, _ in finished_downs])
        for replica_id, t in finished_downs:
            info = down_infos.get(replica_id)
            if (info is None or not isinstance(t, _ReplicaDownThread) or
                    t.replica_record_id != info.replica_record_id or
                    t.service_hash != self._service_hash or
                    t.controller_owner != self._controller_owner):
                logger.warning(
                    'Discarding stale teardown worker for replica %s.',
                    replica_id)
                if legacy_runtime.down_thread_pool.get(replica_id) is t:
                    legacy_runtime.down_thread_pool.pop(replica_id)
                continue
            if not self._service_is_cleanup_authorized():
                continue
            if (info.status_property.sky_down_status ==
                    common_utils.ProcessStatus.SCHEDULED):
                # sky.down not started yet; admitted below under the
                # resources lock.
                down_to_admit.append((replica_id, t, info))
                continue
            if (t.ident is None and info.status_property.sky_down_status
                    == common_utils.ProcessStatus.RUNNING):
                # The prior process (or an admission with a lost commit ACK)
                # already charged this row to D, but no native worker belongs
                # to this reconstructed SafeThread.  Provider teardown is
                # exact and idempotent, so adopt it below without reserving a
                # second slot.  Never treat ident=None as successful cleanup.
                running_downs_to_start.append((replica_id, t, info))
                continue
            logger.info(f'Terminate thread for replica {replica_id} finished.')
            self._handle_sky_down_finish(info, format_exc=t.format_exc)
            # Pop only after the durable completion update succeeds.  If a DB
            # write fails, retaining the finished worker makes the next tick
            # retry the handler instead of stranding a RUNNING down status.
            if legacy_runtime.down_thread_pool.get(replica_id) is t:
                legacy_runtime.down_thread_pool.pop(replica_id)

        # Prepare exact candidates without owning the global mutation gate.
        # The canonical batch transactions below acquire the transaction-
        # scoped gate, count, and persist RUNNING atomically; workers start
        # only after those transactions have committed and released it.
        if (launch_to_admit or down_to_admit or running_launches_to_start or
                running_downs_to_start):
            down_candidates: list[tuple[int, _ReplicaDownThread,
                                        ReplicaInfo]] = []
            logical_down_receipts: dict[int, tuple[Any, ...]] = {}
            with contextlib.nullcontext():
                available_down_starts = max(
                    0, MAX_CONCURRENT_DOWNS_PER_SERVICE - concurrent_downs)
                running_downs_to_start = running_downs_to_start[:
                                                                available_down_starts]
                for replica_id, t, info in down_to_admit:
                    if (concurrent_downs + len(running_downs_to_start) +
                            len(down_candidates)
                            >= MAX_CONCURRENT_DOWNS_PER_SERVICE):
                        break
                    if legacy_runtime.down_thread_pool.get(replica_id) is not t:
                        continue
                    current = serve_state.get_replica_info_from_id(
                        self._service_name, replica_id)
                    if (current is None or
                            not isinstance(t, _ReplicaDownThread) or
                            current.replica_record_id != t.replica_record_id or
                            t.service_hash != self._service_hash or
                            t.controller_owner != self._controller_owner):
                        if (legacy_runtime.down_thread_pool.get(replica_id)
                                is t):
                            legacy_runtime.down_thread_pool.pop(replica_id)
                        continue
                    info = current
                    logical_retirement = (
                        info.status_property.logical_retirement_version
                        is not None)
                    logical_state_guard = (self._logical_state_lock
                                           if logical_retirement else
                                           contextlib.nullcontext())
                    snapshot: LogicalReconcileSnapshot | None = None
                    with logical_state_guard:
                        if logical_retirement:
                            recovering_ids: set[
                                int] = self._recovering_logical_retirement_ids
                            if replica_id in recovering_ids:
                                # Recovery owns this durable SCHEDULED row.
                                # Evaluating its pre-restart fence here would
                                # abort and advertise the victim before the
                                # recovery pass can adopt it from fresh
                                # capacity evidence.
                                continue
                        if logical_retirement:
                            status = info.status_property
                            retirement_version = (
                                status.logical_retirement_version)
                            confirmed_generation = (
                                status.logical_retirement_confirmed_generation)
                            bounded_deadline = (
                                status.logical_retirement_bounded_deadline)
                            info_version = info.version
                            bounded_outdated_retirement = (
                                type(retirement_version) is int and
                                type(info_version) is int and
                                info_version < retirement_version and
                                type(confirmed_generation) is int and
                                bounded_deadline is True)
                            retirement_state = self._logical_retirement_state(
                                info,
                                require_victim_idle=
                                not bounded_outdated_retirement)
                            if retirement_state == 'wait':
                                continue
                            if retirement_state == 'abort':
                                self._abort_logical_retirement(
                                    info, 'shutdown admission fence changed')
                                continue
                            snapshot = self._logical_reconcile_state.snapshot
                            assert snapshot is not None
                            authority = snapshot.authority
                            if authority is not None:
                                retirement_shelter = (
                                    self._logical_reconcile_state.
                                    retirement_shelter)
                                if (retirement_shelter is not None and
                                        not retirement_shelter.authority_current
                                   ):
                                    continue
                                if (self._service_hash is None or
                                        self._controller_owner is None):
                                    self.invalidate_logical_reconcile_state()
                                    continue
                                try:
                                    commit_result = (
                                        serve_state.commit_logical_retirement(
                                            self._service_name,
                                            replica_id,
                                            info,
                                            authority,
                                            expected_service_hash=(
                                                self._service_hash),
                                            expected_controller_owner=(
                                                self._controller_owner),
                                            expected_logical_controller_epoch=(
                                                self._logical_controller_epoch),
                                            expected_reserved_fill_allocation_identity
                                            =(None if retirement_shelter is None
                                              else retirement_shelter.
                                              allocation_identity)))
                                except Exception as error:  # pylint: disable=broad-except
                                    logger.warning(
                                        'Revoking logical-retirement '
                                        'authority because its commit could '
                                        'not be evaluated: '
                                        f'{common_utils.format_exception(error)}'
                                    )
                                    self.invalidate_logical_reconcile_state()
                                    continue
                                if commit_result.state is (
                                        serve_state.
                                        LogicalRetirementCommitState.REJECTED):
                                    self.invalidate_logical_reconcile_state()
                                    continue
                                if commit_result.state is (
                                        serve_state.
                                        LogicalRetirementCommitState.AMBIGUOUS):
                                    # The original worker has never started.
                                    # Only exact readback may reconstruct it.
                                    if (legacy_runtime.down_thread_pool.get(
                                            replica_id) is t):
                                        legacy_runtime.down_thread_pool.pop(
                                            replica_id)
                                    (self.
                                     _ambiguous_logical_retirement_commit_ids.
                                     add(replica_id))
                                    self.invalidate_logical_reconcile_state()
                                    continue
                                if commit_result.state is not (
                                        serve_state.
                                        LogicalRetirementCommitState.COMMITTED):
                                    logger.error(
                                        'Revoking logical-retirement '
                                        'authority after an unknown commit '
                                        f'outcome: {commit_result.state!r}')
                                    self.invalidate_logical_reconcile_state()
                                    continue
                                assert commit_result.replica_info is not None
                                info = commit_result.replica_info
                            else:
                                # Transitional direct-source services keep the
                                # established local admission boundary. The
                                # durable path above is the steady-state path.
                                info.status_property.logical_retirement_confirmed_generation = (
                                    snapshot.generation)
                                info.status_property.logical_retirement_committed = (
                                    True)
                        direct_logical_commit = bool(logical_retirement and
                                                     snapshot is not None and
                                                     snapshot.authority is None)
                        if direct_logical_commit:
                            try:
                                # Persist only the logical selection here.
                                # The row remains SCHEDULED until the batch
                                # admission transaction reserves D.
                                self._persist_replica(replica_id, info)
                            except Exception as error:  # pylint: disable=broad-except
                                logger.warning(
                                    'Deferring logical teardown for replica '
                                    '%s because its SCHEDULED commitment '
                                    'could not be persisted: %s', replica_id,
                                    common_utils.format_exception(error))
                                continue
                        down_candidates.append((replica_id, t, info))
                        logical_receipt = (
                            serve_state.logical_retirement_commit_identity(info)
                        )
                        if logical_receipt is not None:
                            logical_down_receipts[replica_id] = logical_receipt
                reserved_down_infos = {}
                if down_candidates:
                    try:
                        reserved_down_infos = (
                            serve_state.
                            reserve_replica_teardowns_running_if_capacity(
                                self._service_name,
                                [(replica_id, info.replica_record_id)
                                 for replica_id, _, info in down_candidates],
                                termination_limit=(controller_utils.
                                                   get_serve_termination_limit(
                                                       self._is_pool)),
                                expected_logical_retirement_commits=(
                                    logical_down_receipts),
                                **self._db_fence_kwargs()))
                    except Exception as error:  # pylint: disable=broad-except
                        logger.warning(
                            'Deferring teardown admission because its atomic '
                            'reservation could not be evaluated: %s',
                            common_utils.format_exception(error))

                launch_candidates: list[tuple[int, _ReplicaLaunchThread,
                                              ReplicaInfo]] = []
                for replica_id, t, info in launch_to_admit:
                    if (legacy_runtime.launch_thread_pool.get(replica_id)
                            is not t or
                            not isinstance(t, _ReplicaLaunchThread)):
                        continue
                    logical_fence = self._replica_to_logical_launch_fence.get(
                        replica_id)
                    if logical_fence is not None:
                        with self._logical_state_lock:
                            if not self._logical_reconcile_fence_holds(
                                    logical_fence,
                                    require_fresh_occupancy=False):
                                continue
                    launch_candidates.append((replica_id, t, info))

                reserved_launch_infos = {}
                if launch_candidates:
                    try:
                        reserved_launch_infos = (
                            serve_state.
                            reserve_replica_launches_running_if_capacity(
                                self._service_name,
                                [(replica_id, info.replica_record_id,
                                  t.adopts_existing_bound_request)
                                 for replica_id, t, info in launch_candidates],
                                launch_limit=(
                                    controller_utils.get_serve_launch_limit(
                                        self._is_pool)),
                                **self._db_fence_kwargs()))
                    except Exception as error:  # pylint: disable=broad-except
                        # A lost commit acknowledgement may have reserved some
                        # exact rows. Start none: the next tick's current-
                        # process never-started ledger resolves only those
                        # rows.
                        logger.warning(
                            'Deferring launch admission because its atomic '
                            'reservation could not be evaluated: %s',
                            common_utils.format_exception(error))

                for replica_id, t, _ in launch_candidates:
                    info = reserved_launch_infos.get(replica_id)
                    if (info is not None and
                            not t.adopts_existing_bound_request):
                        legacy_runtime.never_started_launch_reservations[
                            replica_id] = (info.replica_record_id, id(t))

                # Provider teardowns remain first, but neither direction held
                # the other's budget or any database lock while starting.
                for replica_id, t, _ in running_downs_to_start:
                    if legacy_runtime.down_thread_pool.get(replica_id) is not t:
                        continue
                    try:
                        t.start()
                    except BaseException as error:  # pylint: disable=broad-except
                        # This RUNNING receipt predates this Thread.start call;
                        # it may belong to a predecessor process or an
                        # admission whose commit acknowledgement was lost.
                        # Retain the D debit and retry exact idempotent
                        # adoption; there is no proof permitting demotion.
                        if not isinstance(error, Exception):
                            raise
                        logger.warning(
                            'Could not adopt durable RUNNING teardown worker '
                            '%s; retaining its reservation: %s', replica_id,
                            common_utils.format_exception(error))
                        continue
                    self._wait_for_idle_trackers.pop(replica_id, None)

                for replica_id, t, _ in down_candidates:
                    info = reserved_down_infos.get(replica_id)
                    if info is None:
                        continue
                    if legacy_runtime.down_thread_pool.get(replica_id) is not t:
                        # RUNNING stays conservative; recovery adopts it.
                        continue
                    try:
                        t.start()
                    except BaseException as error:  # pylint: disable=broad-except
                        # KeyboardInterrupt/SystemExit can arrive after native
                        # start but before ident publication.  Preserve the
                        # durable debit for every asynchronous interruption.
                        if not isinstance(error, Exception):
                            raise
                        if t.ident is not None:
                            logger.warning(
                                'Teardown worker %s returned a start error '
                                'after obtaining a native identity; retaining '
                                'its durable RUNNING reservation: %s',
                                replica_id,
                                common_utils.format_exception(error))
                        else:
                            try:
                                serve_state.restore_never_started_replica_teardown_to_scheduled(
                                    self._service_name, replica_id,
                                    info.replica_record_id,
                                    **self._db_fence_kwargs())
                            except Exception as recovery_error:  # pylint: disable=broad-except
                                logger.warning(
                                    'Failed to release never-started teardown '
                                    'reservation for replica %s; retaining '
                                    'conservative RUNNING evidence: %s',
                                    replica_id,
                                    common_utils.format_exception(
                                        recovery_error))
                            if (legacy_runtime.down_thread_pool.get(replica_id)
                                    is t):
                                legacy_runtime.down_thread_pool.pop(replica_id)
                            self._wait_for_idle_trackers.pop(replica_id, None)
                            self._schedule_failed_cleanup_retry(replica_id)
                        continue
                    self._wait_for_idle_trackers.pop(replica_id, None)

                for replica_id, t, _ in running_launches_to_start:
                    if (legacy_runtime.launch_thread_pool.get(replica_id)
                            is not t):
                        continue
                    try:
                        t.start()
                    except BaseException as error:  # pylint: disable=broad-except
                        # As for inherited D, no process-local reservation
                        # receipt authorizes RUNNING -> SCHEDULED here.  The
                        # exact bound/current worker remains registered and
                        # charged for the next adoption attempt.
                        if not isinstance(error, Exception):
                            raise
                        logger.warning(
                            'Could not adopt durable RUNNING launch worker '
                            '%s; retaining its reservation: %s', replica_id,
                            common_utils.format_exception(error))
                        continue

                for replica_id, t, _ in launch_candidates:
                    info = reserved_launch_infos.get(replica_id)
                    if info is None:
                        continue
                    if (legacy_runtime.launch_thread_pool.get(replica_id)
                            is not t):
                        # RUNNING stays conservative; the next owner resolves
                        # the exact durable request/provider evidence.
                        legacy_runtime.never_started_launch_reservations.pop(
                            replica_id, None)
                        continue
                    try:
                        t.start()
                    except BaseException as error:  # pylint: disable=broad-except
                        if not isinstance(error, Exception):
                            (legacy_runtime.never_started_launch_reservations.
                             pop(replica_id, None))
                            raise
                        if t.ident is not None:
                            (legacy_runtime.never_started_launch_reservations.
                             pop(replica_id, None))
                            logger.warning(
                                'Launch worker %s returned a start error after '
                                'obtaining a native identity; retaining its '
                                'durable RUNNING reservation: %s', replica_id,
                                common_utils.format_exception(error))
                        else:
                            if t.adopts_existing_bound_request:
                                # Its request generation predates this local
                                # worker and can already own provider effect.
                                # Keep RUNNING and revoke rollback authority.
                                (legacy_runtime.
                                 never_started_launch_reservations.pop(
                                     replica_id, None))
                            else:
                                try:
                                    restored = (
                                        self.
                                        _restore_never_started_launch_to_scheduled(
                                            replica_id, info.replica_record_id))
                                except Exception as recovery_error:  # pylint: disable=broad-except
                                    logger.warning(
                                        'Failed to release never-started '
                                        'launch reservation for replica %s; '
                                        'retaining conservative RUNNING '
                                        'evidence: %s', replica_id,
                                        common_utils.format_exception(
                                            recovery_error))
                                else:
                                    if restored is not None:
                                        (legacy_runtime.
                                         never_started_launch_reservations.pop(
                                             replica_id, None))
                        continue
                    legacy_runtime.never_started_launch_reservations.pop(
                        replica_id, None)

        # Reconcile provider cleanup, but retain immutable version metadata.
        # Historical specs power admin comparison and rollback, while full
        # service teardown remains responsible for deleting all version rows.
        replica_infos = serve_state.get_replica_infos(self._service_name)
        self._reconcile_unowned_bound_non_pool_launches(replica_infos)
        self._reconcile_failed_cleanup(replica_infos)

    def _thread_pool_refresher(self) -> None:
        """Periodically refresh the launch/down thread pool."""
        while not self._manager_daemon_should_stop():
            _, completion_event = self._launch_completion_state()
            # Clear before draining the durable-in-process queue. A completion
            # racing after this clear is either drained now or leaves the event
            # set so the wait below returns immediately.
            completion_event.clear()
            if self._manager_daemon_should_stop():
                return
            self._join_notified_launch_workers()
            if self._manager_daemon_should_stop():
                return
            logger.debug('Refreshing thread pool.')
            try:
                # Admit already-durable launch/down work before optional
                # endpoint discovery. A slow provider lookup must never hold
                # the sole admission loop in front of a queued worker.
                self._refresh_thread_pool()
                if self._manager_daemon_should_stop():
                    return
                # `_refresh_thread_pool()` has released the manager lock here.
                # Unknown paid Phase-A outcomes require PostgreSQL settlement
                # before any exact worker can be published or retired.
                self._reconcile_ambiguous_paid_phase_a_outcomes()
                if self._manager_daemon_should_stop():
                    return
                wait_state_changed = self._resolve_wait_for_idle_urls()
                # Re-enter through the loop head so launch completions that
                # raced with URL I/O are joined before the next reducer pass.
                # Setting the event also admits newly resolved drains without
                # the ordinary refresh interval.
                if wait_state_changed:
                    completion_event.set()
            except Exception as e:  # pylint: disable=broad-except
                # No matter what error happens, we should keep the
                # thread pool refresher running.
                logger.error('Error in thread pool refresher: '
                             f'{common_utils.format_exception(e)}')
                with ux_utils.enable_traceback():
                    logger.error(f'  Traceback: {traceback.format_exc()}')
            if self._manager_daemon_should_stop():
                return
            completion_event.wait(_PROCESS_POOL_REFRESH_INTERVAL)

    def _system_recovery_status_initialized_ids(self) -> set[int]:
        return self._system_recovery_status_initialized

    def _system_recovery_status_barrier_expired(self,
                                                info: ReplicaInfo) -> bool:
        if (info.system_recovery_disposition
                != system_recovery_state.SystemRecoveryDisposition.CAPABLE or
                info.replica_id
                in self._system_recovery_status_initialized_ids()):
            return False
        recovery = info.system_recovery
        if recovery is None:
            return True
        anchor = recovery.status_barrier_started_at
        if not isinstance(anchor, (int, float)):
            return True
        return (time.time() - float(anchor)
                >= system_recovery_state.CANDIDATE_RELEASE_GUARD_SECONDS)

    def _system_recovery_controller_grace_seconds(
            self, info: ReplicaInfo,
            observation: system_recovery_state.RecoveryObservation) -> float:
        initial_delay = max(0, self._get_initial_delay_seconds(info.version))
        remaining_local = 120.0
        if observation.deadline_at is not None:
            remaining_local = max(0.0, observation.deadline_at - time.time())
        return max(1.0, min(900.0, remaining_local + initial_delay))

    def _reconcile_system_recovery_status(
        self,
        snapshot: ReplicaInfo,
        job_status: job_lib.JobStatus | None,
        detail: job_lib.JobSystemRecoveryInfo | None,
        detail_status: job_lib.JobSystemRecoveryDetailStatus,
        *,
        deferred_teardowns: dict[int, ReplicaInfo] | None = None,
        reconciled_infos: dict[int, ReplicaInfo] | None = None,
        stale_replica_ids: set[int] | None = None,
    ) -> bool:
        """Reduce one exact-job observation and schedule legacy teardown.

        A readiness round supplies ``deferred_teardowns`` because it owns a
        short bulk-reduction critical section.  The durable teardown intent is
        then included in that round's batch instead of entering the potentially
        blocking legacy termination scheduler while the fleet mutex is held.
        """
        if self._update_recovery_required:
            return False
        outcome: dict[str, Any] = {
            'off_route': True,
            'clear_probe': False,
            'teardown': False,
            'valid_present': False,
            'events': set(),
            'stale': False,
        }

        def _reduce(fresh: ReplicaInfo) -> bool:
            # A revision conflict reruns this closure on a newer row.  No
            # decision derived from the stale attempt may survive into that
            # retry, including side effects emitted after the CAS succeeds.
            outcome.update({
                'off_route': True,
                'clear_probe': False,
                'teardown': False,
                'valid_present': False,
                'events': set(),
                'stale': False,
            })
            if not self._probe_snapshot_matches_current(snapshot, fresh):
                outcome['stale'] = True
                return False
            disposition = fresh.system_recovery_disposition
            if disposition not in (
                    system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
                    system_recovery_state.SystemRecoveryDisposition.CAPABLE):
                outcome['off_route'] = False
                return False
            if (fresh.system_recovery_quarantine is not None or
                    self._has_system_recovery_teardown_intent(fresh)):
                outcome['teardown'] = True
                return False
            intent = fresh.system_recovery_launch_intent
            exact_job_id = fresh.service_job_id
            if (intent is None or not isinstance(exact_job_id, int) or
                    isinstance(exact_job_id, bool) or exact_job_id < 1 or
                    exact_job_id != snapshot.service_job_id or
                    fresh.launch_request_id != snapshot.launch_request_id or
                    not isinstance(job_status, job_lib.JobStatus)):
                outcome['teardown'] = True
                outcome['events'].add('evidence_lost')
                return False

            observation = None
            if detail_status == job_lib.JobSystemRecoveryDetailStatus.PRESENT:
                try:
                    if detail is None:
                        raise system_recovery_state.RecoveryStateError(
                            'Recovery detail is missing.')
                    observation = (system_recovery_state.RecoveryObservation.
                                   from_job_system_recovery_info(
                                       exact_job_id, detail))
                except system_recovery_state.RecoveryStateError as e:
                    logger.warning(
                        f'Replica {fresh.replica_id} returned malformed '
                        f'recovery detail for exact job {exact_job_id}: '
                        f'{common_utils.format_exception(e)}')
                if (observation is None or observation.capability
                        != intent.expected_runtime_capability or
                        observation.profile_version
                        != intent.runtime_profile_version):
                    outcome['teardown'] = True
                    outcome['events'].add('evidence_lost')
                    observation = None
                else:
                    outcome['valid_present'] = True
            elif (detail is not None or detail_status
                  not in (job_lib.JobSystemRecoveryDetailStatus.ABSENT,
                          job_lib.JobSystemRecoveryDetailStatus.MALFORMED)):
                outcome['teardown'] = True
                outcome['events'].add('evidence_lost')
            if (detail_status == job_lib.JobSystemRecoveryDetailStatus.MALFORMED
               ):
                outcome['events'].add('evidence_lost')

            previous_recovery = fresh.system_recovery
            if disposition == (
                    system_recovery_state.SystemRecoveryDisposition.CANDIDATE):
                if observation is None:
                    # ABSENT is a valid unresolved-candidate sample until the
                    # guarded release protocol sees it in the same cycle as a
                    # fresh successful probe. Every other shape fails closed.
                    if (detail_status
                            != job_lib.JobSystemRecoveryDetailStatus.ABSENT or
                            job_status.is_terminal()):
                        outcome['teardown'] = True
                    return False
                reduction = system_recovery_state.reduce_remote_observation(
                    None,
                    observation,
                    now=time.time(),
                    controller_grace_seconds=(
                        self._system_recovery_controller_grace_seconds(
                            fresh, observation)),
                    job_terminal=job_status.is_terminal(),
                    teardown_intent=False)
                fresh.system_recovery_disposition = (
                    system_recovery_state.SystemRecoveryDisposition.CAPABLE)
                outcome['events'].add('authorization_v3_capable')
            else:
                if observation is None:
                    outcome['teardown'] = True
                    reduction = (
                        system_recovery_state.reduce_remote_observation(
                            fresh.system_recovery,
                            None,
                            now=time.time(),
                            controller_grace_seconds=max(
                                1.0,
                                min(
                                    900.0,
                                    120.0 + self._get_initial_delay_seconds(
                                        fresh.version))),
                            job_terminal=job_status.is_terminal(),
                            teardown_intent=True))
                else:
                    reduction = (
                        system_recovery_state.reduce_remote_observation(
                            fresh.system_recovery,
                            observation,
                            now=time.time(),
                            controller_grace_seconds=(
                                self._system_recovery_controller_grace_seconds(
                                    fresh, observation)),
                            job_terminal=job_status.is_terminal(),
                            teardown_intent=False))

            fresh.system_recovery = reduction.state
            previous_state = (None if previous_recovery is None else
                              previous_recovery.state)
            updated_state = (None if reduction.state is None else
                             reduction.state.state)
            route_generation = self._system_recovery_route_generation(fresh)
            if (route_generation is not None and
                    self._route_lease_registry().is_retired(
                        fresh.replica_id, route_generation)):
                fresh.system_recovery = (
                    system_recovery_state.terminalize_for_teardown(
                        fresh.system_recovery, now=time.time()))
                updated_state = (None if fresh.system_recovery is None else
                                 fresh.system_recovery.state)
                outcome['teardown'] = True
            if (updated_state in
                (system_recovery_state.ControllerRecoveryState.RECOVERING,
                 system_recovery_state.ControllerRecoveryState.RETRY_SUBMITTED)
                    and previous_state
                    in (None,
                        system_recovery_state.ControllerRecoveryState.ARMED)):
                outcome['events'].add('recovery_started')
            if (updated_state
                    == system_recovery_state.ControllerRecoveryState.EXHAUSTED
                    and previous_state != updated_state):
                outcome['events'].add('recovery_exhausted')
            if (outcome['valid_present'] and
                    fresh.system_recovery is not None and
                    fresh.system_recovery.status_barrier_started_at
                    is not None):
                fresh.system_recovery = dataclasses.replace(
                    fresh.system_recovery, status_barrier_started_at=None)
            outcome['off_route'] = reduction.force_off_route
            outcome['clear_probe'] = reduction.clear_probe_failure_window
            outcome['teardown'] = (outcome['teardown'] or
                                   reduction.schedule_legacy_teardown)
            return True

        updated = self._patch_system_recovery_with_latest(
            snapshot.replica_id, _reduce)
        if self._update_recovery_required:
            return False
        if updated is None:
            # None means the exact CAS/lifecycle authority was unavailable.
            # It is not evidence about the observed replica. In particular,
            # never re-read by numeric ID and reinterpret a same-ID successor
            # as a teardown target.
            if stale_replica_ids is not None:
                stale_replica_ids.add(snapshot.replica_id)
            return False
        if outcome.get('stale'):
            if stale_replica_ids is not None:
                stale_replica_ids.add(snapshot.replica_id)
            return False
        if reconciled_infos is not None:
            reconciled_infos[updated.replica_id] = updated
        if (updated.system_recovery_disposition
                != system_recovery_state.SystemRecoveryDisposition.CANDIDATE):
            self._candidate_release_monotonic_deadlines.pop(
                updated.replica_id, None)
        status_changed = False
        if outcome['off_route'] and updated.status_property.service_ready_now:
            updated.status_property.service_ready_now = False
            status_changed = True
        if (outcome['clear_probe'] and
                updated.first_consecutive_failure_time is not None):
            updated.first_consecutive_failure_time = None
            status_changed = True
        if (outcome['valid_present'] and not outcome['teardown'] and
                updated.system_recovery_disposition
                == system_recovery_state.SystemRecoveryDisposition.CAPABLE and
                updated.system_recovery is not None and
                updated.system_recovery.state
                != system_recovery_state.ControllerRecoveryState.EXHAUSTED):
            self._system_recovery_status_initialized_ids().add(
                updated.replica_id)
        elif outcome['teardown']:
            self._system_recovery_status_initialized_ids().discard(
                updated.replica_id)
        for event in outcome['events']:
            system_oom_recovery_observability.record_for_replica(event, updated)
        if status_changed and reconciled_infos is None:
            self._persist_replica(updated.replica_id, updated)
        if outcome['teardown']:
            logger.warning(
                f'System recovery for replica {updated.replica_id} cannot '
                'continue safely; scheduling legacy teardown.')
            if deferred_teardowns is not None:
                deferred_teardowns[updated.replica_id] = updated
            else:
                self._terminate_replica(updated.replica_id,
                                        replica_drain_delay_seconds=0)
            return True
        return False

    def _fetch_job_status(self) -> None:
        """Fetch the service job status of all replicas.

        This function monitors replicas whose backend or service contract
        requires exact remote job evidence. If one of those jobs fails, it
        terminates the replica.

        Ordinary Kubernetes non-pool workers deliberately do not enter this
        path. Their Pod lifecycle and application endpoint probe are the
        canonical liveness owners; ``kubectl exec`` job-table polling would
        duplicate those owners and create one child process per replica every
        round. Pools, system-recovery rows, and non-Kubernetes backends retain
        exact polling because their semantics require evidence the ordinary
        endpoint/provider contract does not provide.

        NOTE: this does NOT hold ``self.lock`` across the per-replica
        ``get_job_status`` SSH walk. An unreachable (e.g. preempted spot)
        replica's SSH connect hangs at the kernel TCP timeout (tens of seconds
        to minutes); holding the lock across the walk would block the
        refresher / prober / scaler -- which all take ``self.lock`` -- for the
        whole walk, stalling autoscaling exactly when the fleet is churning.
        Provider classification is also completed before taking the manager
        lock. The lock is acquired only for an exact lifecycle check and the
        provider-free reducer/write/teardown scheduling.

        The remaining remote fetches run in a thread pool (like
        ``_probe_all_replicas``):
        a serial walk lets one hung replica delay failure detection for every
        replica after it, scaling the round as O(N * per-replica SSH). The
        failure-handling branches still run serially, consuming results in
        latest-version-first order, and each re-reads fresh state under
        ``self.lock`` before acting.
        """
        if self._manager_daemon_should_stop():
            return
        infos = serve_state.get_replica_infos(self._service_name)
        # Snapshot every replica's cluster record in one batched read; the
        # per-replica ``info.handle()`` fallback would issue one cluster-table
        # read per replica per fetch round.
        cluster_records = global_user_state.get_clusters_from_names(
            [info.cluster_name for info in infos])
        # A setup error in a new version is commonly version-wide. Consume one
        # trackable latest-version replica's result before the rest of the
        # fleet so a bad rollout is stopped without waiting behind every old
        # replica.
        for index, info in enumerate(infos):
            if (info.version == self.latest_version and
                    info.status_property.should_track_service_status()):
                infos.insert(0, infos.pop(index))
                break
        # We use backend API to avoid usage collection in the
        # sdk.job_status. The backend object is stateless; construct it
        # once for the whole walk.
        backend = backends.CloudVmRayBackend()
        ordinary_fetches: list[tuple[ReplicaInfo, Any, list[int] | None,
                                     bool]] = []
        unphased_fetches: list[tuple[ReplicaInfo, Any, list[int] | None,
                                     bool]] = []
        fenced_fetches: list[tuple[ReplicaInfo, Any, list[int] | None,
                                   bool]] = []
        invalid_recovery_infos: dict[
            provider_phase.ProviderPhaseMode, list[ReplicaInfo]] = {
                provider_phase.ProviderPhaseMode.V2_FENCED: [],
                provider_phase.ProviderPhaseMode.AMBIENT_LEGACY: [],
            }
        identity_uncertainties: list[tuple[ReplicaInfo, str]] = []
        fence_representatives: dict[tuple[str, str], tuple[ReplicaInfo,
                                                           Any]] = {}
        fence_group_infos: dict[tuple[str, str], list[ReplicaInfo]] = {}
        for info in infos:
            if not info.status_property.should_track_service_status():
                continue
            cluster_record = cluster_records.get(info.cluster_name)
            try:
                cleanup_fence = (
                    reserved_capacity.parse_protocol_v2_cleanup_fence(info))
                if cleanup_fence is None:
                    handle = (None if cluster_record is None else
                              info.handle(cluster_record))
                else:
                    handle = (cluster_record.get('handle') if isinstance(
                        cluster_record, dict) else None)
                reserved_capacity.protocol_v2_provider_fence(info, handle)
            except exceptions.KubernetesPhysicalClusterIdentityError as error:
                identity_uncertainties.append(
                    (info, common_utils.format_exception(error)))
                continue
            if handle is None:
                # The walk runs lock-free, so the replica's cluster record can
                # vanish mid-walk (a scale-down or preemption cleanup
                # completing after the snapshot was taken). Skip it; the next
                # round re-snapshots.
                if (not self._is_pool and info.system_recovery_disposition in
                    (system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
                     system_recovery_state.SystemRecoveryDisposition.CAPABLE)):
                    mode = (provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                            if cleanup_fence is None else
                            provider_phase.ProviderPhaseMode.V2_FENCED)
                    invalid_recovery_infos[mode].append(info)
                continue
            with_recovery = (
                not self._is_pool and info.system_recovery_disposition
                in (system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
                    system_recovery_state.SystemRecoveryDisposition.CAPABLE))
            requires_exact_job_evidence = self._is_pool or with_recovery
            if (not requires_exact_job_evidence and
                    backend.serve_replica_job_status_source(handle) is
                    backends.ServeReplicaJobStatusSource.PROVIDER_AND_ENDPOINT):
                # The ordinary Kubernetes Serve contract has one happy path:
                # provider lifecycle plus application readiness. Do not enter
                # a provider phase, start a worker thread, or exec into the Pod
                # merely to duplicate that evidence.
                continue
            if cleanup_fence is not None:
                # Register only rows that still require exact remote evidence.
                # Invalid recovery rows may schedule teardown, so their
                # reduction needs the same batch physical owner as a normal
                # status result.
                key = (cleanup_fence.kubernetes_context,
                       cleanup_fence.physical_cluster_uid)
                fence_representatives.setdefault(key, (info, handle))
                fence_group_infos.setdefault(key, []).append(info)
            if with_recovery:
                service_job_id = info.service_job_id
                if (isinstance(service_job_id, bool) or
                        not isinstance(service_job_id, int) or
                        service_job_id < 1):
                    mode = (provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                            if cleanup_fence is None else
                            provider_phase.ProviderPhaseMode.V2_FENCED)
                    invalid_recovery_infos[mode].append(info)
                    continue
                job_ids: list[int] | None = [service_job_id]
            else:
                job_ids = [1] if self._is_pool else None
            fetch = (info, handle, job_ids, with_recovery)
            if cleanup_fence is not None:
                fenced_fetches.append(fetch)
            elif reserved_capacity.ordinary_provider_phase_mode(
                    handle, info.cluster_name) is None:
                unphased_fetches.append(fetch)
            else:
                ordinary_fetches.append(fetch)

        def _current_exact(snapshot: ReplicaInfo) -> ReplicaInfo | None:
            fresh = serve_state.get_replica_info_from_id(
                self._service_name, snapshot.replica_id)
            if (fresh is None or
                    not self._probe_snapshot_matches_current(snapshot, fresh)):
                return None
            return fresh

        def _record_identity_uncertainty(snapshot: ReplicaInfo,
                                         message: str) -> None:
            with self.lock:
                if self._update_recovery_required:
                    return
                fresh = _current_exact(snapshot)
                if fresh is None:
                    return
                self._record_provider_identity_uncertain(
                    fresh, f'job-status lookup was fenced off: {message}')

        for snapshot, message in identity_uncertainties:
            _record_identity_uncertainty(snapshot, message)

        def _terminate_invalid_recovery_rows(
                snapshots: list[ReplicaInfo]) -> None:
            for snapshot in snapshots:
                with self.lock:
                    if self._update_recovery_required:
                        return
                    fresh = _current_exact(snapshot)
                    if (fresh is None or fresh.system_recovery_disposition
                            not in (system_recovery_state.
                                    SystemRecoveryDisposition.CANDIDATE,
                                    system_recovery_state.
                                    SystemRecoveryDisposition.CAPABLE)):
                        continue
                    logger.warning(
                        f'Recovery candidate/capable replica '
                        f'{fresh.replica_id} lacks its exact cluster handle '
                        'or service job association.')
                    self._terminate_replica(fresh.replica_id,
                                            replica_drain_delay_seconds=0)

        def _get_job_status(info, handle, job_ids, with_recovery,
                            phase_admission):
            # SSH into the replica's head node -- intentionally OUTSIDE
            # self.lock so an unreachable replica cannot wedge the round.
            if phase_admission is None:
                # The batched durable snapshot supplied this exact ordinary
                # non-Kubernetes handle; the backend consumes it directly.
                if (reserved_capacity.parse_protocol_v2_cleanup_fence(info)
                        is not None or
                        reserved_capacity.ordinary_provider_phase_mode(
                            handle, info.cluster_name) is not None):
                    raise exceptions.ProviderPhaseMisuseError(
                        'Unphased job status requires an exact ordinary '
                        'non-Kubernetes handle.')
                provider_context: contextlib.AbstractContextManager[Any] = (
                    contextlib.nullcontext())
            else:
                provider_context = (
                    reserved_capacity.protocol_v2_provider_fence(
                        info, handle, phase_admission=phase_admission))
            with provider_context:
                if with_recovery:
                    return backend.get_job_status_with_system_recovery(
                        handle, job_ids, stream_logs=False)
                return (backend.get_job_status(handle,
                                               job_ids,
                                               stream_logs=False), {}, {})

        def _run_fetches(
            fetches: list[tuple[ReplicaInfo, Any, list[int] | None, bool]],
            phase_admission: provider_phase.ProviderPhaseAdmission | None,
        ) -> list[tuple[ReplicaInfo, Any, Any]]:
            if not fetches or self._manager_daemon_should_stop():
                return []
            # The fetches are pure I/O and explicitly join the caller's phase.
            # Wait for every worker while the provider fence is held, but
            # classify/reduce only after the caller releases that fence. This
            # prevents provider-phase -> manager-lock inversion.
            num_fetch_threads = min(len(fetches),
                                    self._PROBE_ROUND_MAX_PARALLELISM)
            with mp_pool.ThreadPool(num_fetch_threads) as pool:
                fetch_results = [
                    (info, handle,
                     pool.apply_async(_get_job_status,
                                      (info, handle, job_ids, with_recovery,
                                       phase_admission)))
                    for info, handle, job_ids, with_recovery in fetches
                ]
                for _, _, result in fetch_results:
                    result.wait()
                return fetch_results

        fenced_invalid_infos = invalid_recovery_infos[
            provider_phase.ProviderPhaseMode.V2_FENCED]
        if fenced_fetches or fenced_invalid_infos:
            # Blocking admission is outside self.lock, so one unreachable SSH
            # worker does not block probe/refresher admission on the manager
            # mutex. Result reduction happens only after owners retire.
            fenced_identity_uncertainties: list[tuple[ReplicaInfo, str]] = []
            fenced_results: list[tuple[ReplicaInfo, Any, Any]] = []
            failed_replica_ids: set[int] = set()
            try:
                with provider_phase.provider_phase(
                        provider_phase.ProviderPhaseMode.V2_FENCED
                ) as phase_admission:
                    with reserved_capacity.protocol_v2_provider_batch_fences(
                            fence_representatives,
                            phase_admission=phase_admission) as fence_failures:
                        for key, error in fence_failures.items():
                            if not isinstance(error, Exception):
                                raise error
                            group_infos = fence_group_infos[key]
                            failed_replica_ids.update(
                                info.replica_id for info in group_infos)
                            if isinstance(
                                    error, exceptions.
                                    KubernetesPhysicalClusterIdentityError):
                                message = (
                                    'job-status batch identity was fenced off: '
                                    f'{common_utils.format_exception(error)}')
                                fenced_identity_uncertainties.extend(
                                    (info, message) for info in group_infos)
                            else:
                                # A failed physical group contributes no job
                                # evidence. Other v2 groups and later provider
                                # partitions remain independently consumable.
                                logger.warning(
                                    'Deferring protocol-v2 job status for '
                                    'replica IDs %s after a group fence '
                                    'failure: %s',
                                    [info.replica_id for info in group_infos],
                                    common_utils.format_exception(error))
                        admitted_fetches = [
                            item for item in fenced_fetches
                            if item[0].replica_id not in failed_replica_ids
                        ]
                        fenced_results = _run_fetches(admitted_fetches,
                                                      phase_admission)
            except exceptions.ProviderPhaseError as error:
                # This partition produced no evidence. Continue the ambient
                # and unphased partitions so an unrelated provider owner
                # cannot convoy the whole fleet.
                logger.info(
                    'Deferring the protocol-v2 job-status partition because '
                    'provider admission was unavailable: %s',
                    common_utils.format_exception(error))
            else:
                for snapshot, message in fenced_identity_uncertainties:
                    _record_identity_uncertainty(snapshot, message)
                _terminate_invalid_recovery_rows([
                    info for info in fenced_invalid_infos
                    if info.replica_id not in failed_replica_ids
                ])
                self._handle_job_status_results(
                    fenced_results,
                    provider_error_phase_mode=(
                        provider_phase.ProviderPhaseMode.V2_FENCED))

        ordinary_invalid_infos = invalid_recovery_infos[
            provider_phase.ProviderPhaseMode.AMBIENT_LEGACY]
        if ordinary_fetches or ordinary_invalid_infos:
            # Genuine ordinary rows run only after all physical owners retire.
            try:
                with provider_phase.provider_phase(
                        provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                ) as phase_admission:
                    ordinary_results = _run_fetches(ordinary_fetches,
                                                    phase_admission)
            except exceptions.ProviderPhaseError as error:
                logger.info(
                    'Deferring the ambient job-status partition because '
                    'provider admission was unavailable: %s',
                    common_utils.format_exception(error))
            else:
                _terminate_invalid_recovery_rows(ordinary_invalid_infos)
                self._handle_job_status_results(
                    ordinary_results,
                    provider_error_phase_mode=(
                        provider_phase.ProviderPhaseMode.AMBIENT_LEGACY))

        # Healthy exact non-Kubernetes SSH can be arbitrarily slow without
        # owning Kubernetes authority. If it fails, result reduction takes a
        # fresh ambient phase before the manager lock.
        unphased_results = _run_fetches(unphased_fetches, None)
        if unphased_results:
            self._handle_job_status_results(
                unphased_results,
                provider_error_phase_mode=(
                    provider_phase.ProviderPhaseMode.AMBIENT_LEGACY))

    def _handle_job_status_results(
        self,
        fetch_results: list[tuple[ReplicaInfo, Any, Any]],
        *,
        provider_error_phase_mode: provider_phase.ProviderPhaseMode,
    ) -> None:
        """Isolate every exact result before consuming the next row."""
        for snapshot, handle, result in fetch_results:
            if self._manager_daemon_should_stop():
                return
            try:
                self._handle_job_status_result_unisolated(
                    [(snapshot, handle, result)],
                    provider_error_phase_mode=provider_error_phase_mode)
            except Exception as error:  # pylint: disable=broad-except
                # This boundary includes decode, identity/liveness
                # classification, recovery reconciliation, and durable row
                # reduction. An exact row with malformed behavior is no
                # evidence about any later independently completed row.
                logger.warning(
                    'Ignoring unexpected job-status processing failure for '
                    'replica %s (cluster %s): %s', snapshot.replica_id,
                    snapshot.cluster_name, common_utils.format_exception(error))

    def _handle_job_status_result_unisolated(
        self,
        fetch_results: list[tuple[ReplicaInfo, Any, Any]],
        *,
        provider_error_phase_mode: provider_phase.ProviderPhaseMode,
    ) -> None:
        """Consume one pre-isolated job-status result."""
        assert len(fetch_results) == 1
        for snapshot, handle, result in fetch_results:
            if self._manager_daemon_should_stop():
                return
            try:
                result_payload = result.get()
                # The SSH result may arrive after a partial update has fenced
                # this child.  Never reduce stale health evidence into a
                # replica write or teardown in that process.
                if self._manager_daemon_should_stop():
                    return
                if (not isinstance(result_payload, tuple) or
                        len(result_payload) != 3):
                    raise ValueError(
                        'Job-status worker returned a non-canonical payload.')
                (job_statuses, recovery_infos,
                 recovery_detail_statuses) = result_payload
                if not all(
                        isinstance(payload, dict)
                        for payload in (job_statuses, recovery_infos,
                                        recovery_detail_statuses)):
                    raise ValueError(
                        'Job-status worker payload maps are malformed.')
            except exceptions.KubernetesPhysicalClusterIdentityError as error:
                with self.lock:
                    if self._update_recovery_required:
                        return
                    fresh = serve_state.get_replica_info_from_id(
                        self._service_name, snapshot.replica_id)
                    if (fresh is None or
                            not self._probe_snapshot_matches_current(
                                snapshot, fresh)):
                        continue
                    self._record_provider_identity_uncertain(
                        fresh, 'job-status lookup was fenced off: '
                        f'{common_utils.format_exception(error)}')
                continue
            except exceptions.ProviderPhaseError as error:
                # Admission failure is no job/liveness evidence for this row.
                logger.info(
                    'Deferring job-status result for replica %s because '
                    'provider admission was unavailable: %s',
                    snapshot.replica_id, common_utils.format_exception(error))
                continue
            except exceptions.CommandError:
                # Classify against the exact opening handle before taking the
                # manager lock. This is one provider read with no cluster
                # table mutation; stale evidence is rejected below before any
                # route, placement, row, or teardown effect.
                try:
                    with provider_phase.provider_phase(
                            provider_error_phase_mode) as phase_admission:
                        liveness = self._cloud_instance_looks_alive(
                            snapshot,
                            phase_admission=phase_admission,
                            handle=handle)
                except exceptions.ProviderPhaseError as error:
                    logger.info(
                        'Deferring failed job-status classification for '
                        'replica %s because provider admission was '
                        'unavailable: %s', snapshot.replica_id,
                        common_utils.format_exception(error))
                    continue
                is_preempted = False
                with self.lock:
                    if self._update_recovery_required:
                        return
                    fresh = serve_state.get_replica_info_from_id(
                        self._service_name, snapshot.replica_id)
                    if (fresh is None or
                            not self._probe_snapshot_matches_current(
                                snapshot, fresh)):
                        continue
                    if liveness.disposition is (
                            _PreemptionPrefilterDisposition.IDENTITY_UNCERTAIN):
                        self._record_provider_identity_uncertain(
                            fresh, 'preemption classification was fenced off')
                        continue
                    if liveness.disposition in (
                            _PreemptionPrefilterDisposition.INTERRUPTED,
                            _PreemptionPrefilterDisposition.
                            EXACT_KUBERNETES_ABSENT):
                        self._apply_confirmed_preemption(fresh, None)
                        self._persist_replica(fresh.replica_id, fresh)
                        self._terminate_replica(fresh.replica_id,
                                                replica_drain_delay_seconds=0,
                                                is_scale_down=True)
                        is_preempted = True
                    elif (fresh.system_recovery_disposition
                          == system_recovery_state.SystemRecoveryDisposition.
                          CAPABLE and
                          self._system_recovery_status_barrier_expired(fresh)):
                        self._terminate_replica(fresh.replica_id,
                                                replica_drain_delay_seconds=0)
                        is_preempted = True
                # Whether preempted or not, move on to the next replica: a
                # replica whose job status cannot be fetched (e.g. a
                # persistently broken but reachable node) must not abort the
                # walk and starve failure detection for every replica after
                # it. The outer fetcher used to swallow the re-raise anyway,
                # so skipping here loses nothing.
                if not is_preempted:
                    logger.error('Failed to fetch job status for replica '
                                 f'{snapshot.replica_id} (cluster '
                                 f'{snapshot.cluster_name}); '
                                 'skipping it this round.')
                continue
            except Exception as error:  # pylint: disable=broad-except
                # One malformed/failed future must not suppress later exact
                # rows in this deterministic submission-order walk.
                logger.warning(
                    'Ignoring unexpected job-status result for replica %s '
                    '(cluster %s): %s', snapshot.replica_id,
                    snapshot.cluster_name, common_utils.format_exception(error))
                continue
            with self.lock:
                if self._update_recovery_required:
                    return
                current = serve_state.get_replica_info_from_id(
                    self._service_name, snapshot.replica_id)
                if (current is None or not self._probe_snapshot_matches_current(
                        snapshot, current)):
                    continue
                self._provider_identity_uncertain_replica_ids().discard(
                    current.replica_id)
            info = snapshot
            if self._is_pool:
                job_status = job_statuses.get(1)
            elif info.system_recovery_disposition in (
                    system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
                    system_recovery_state.SystemRecoveryDisposition.CAPABLE):
                service_job_id = info.service_job_id
                if (isinstance(service_job_id, bool) or
                        not isinstance(service_job_id, int) or
                        service_job_id < 1):
                    job_status = None
                    recovery_detail = None
                    recovery_detail_status = (
                        job_lib.JobSystemRecoveryDetailStatus.MALFORMED)
                else:
                    job_status = job_statuses.get(service_job_id)
                    recovery_detail = recovery_infos.get(service_job_id)
                    recovery_detail_status = recovery_detail_statuses.get(
                        service_job_id,
                        job_lib.JobSystemRecoveryDetailStatus.MALFORMED)
                with self.lock:
                    if self._update_recovery_required:
                        return
                    reconciled = self._reconcile_system_recovery_status(
                        info, job_status, recovery_detail,
                        recovery_detail_status)
                    if self._update_recovery_required:
                        return
                    if reconciled:
                        continue
            else:
                job_status = next(iter(job_statuses.values()), None)
            if job_status is None:
                # No job record on the replica (e.g. the job table was
                # wiped by a recovery, or the job has not been submitted
                # yet). Nothing to conclude; re-check next round.
                continue
            if job_status in job_lib.JobStatus.user_code_failure_states():
                with self.lock:
                    if self._update_recovery_required:
                        return
                    # Re-read under the lock: another thread (e.g. scale_down)
                    # may have terminated or mutated this replica while we
                    # were SSHing without the lock.
                    fresh = serve_state.get_replica_info_from_id(
                        self._service_name, snapshot.replica_id)
                    if (fresh is None or
                            not self._probe_snapshot_matches_current(
                                snapshot, fresh)):
                        continue
                    fresh.status_property.user_app_failed = True
                    self._persist_replica(fresh.replica_id, fresh)
                    logger.warning(
                        f'Service job for replica {fresh.replica_id} FAILED. '
                        'Terminating...')
                    self._terminate_replica(fresh.replica_id,
                                            replica_drain_delay_seconds=0)

    def _job_status_fetcher(self) -> None:
        """Periodically fetch the service job status of all replicas."""
        while not self._manager_daemon_should_stop():
            logger.debug('Refreshing job status.')
            try:
                self._fetch_job_status()
            except Exception as e:  # pylint: disable=broad-except
                # No matter what error happens, we should keep the
                # job status fetcher running.
                logger.error('Error in job status fetcher: '
                             f'{common_utils.format_exception(e)}')
                with ux_utils.enable_traceback():
                    logger.error(f'  Traceback: {traceback.format_exc()}')
            if self._wait_for_manager_daemon_stop(_JOB_STATUS_FETCH_INTERVAL):
                return

    def _resolve_probe_urls(
        self,
        infos: list[ReplicaInfo],
        *,
        phase_admission: provider_phase.ProviderPhaseAdmission | None = None,
        deferred_replica_ids: set[int] | None = None,
        identity_rejected_replica_ids: set[int] | None = None,
        resolved_route_material: dict[int,
                                      route_projection.ResolvedRouteMaterial] |
        None = None,
        resolved_handles: dict[int, backends.CloudVmRayResourceHandle] |
        None = None,
    ) -> dict[int, str | None]:
        """Resolve one endpoint per replica from batched cluster state.

        Endpoint resolution normally loads permissions and one cluster record
        per call.  Kubernetes PodIP mode also reads the Pod unless the handle's
        already-recorded head IP is reused.  A probe round historically called
        ``ReplicaInfo.url`` once for logging and twice inside ``probe``, turning
        an N-replica fleet into 3N permission, database, and Kubernetes API
        reads while the replica-manager lock was held.

        Snapshot the cluster records and provider configs once.  The returned
        URL is passed through to ``probe`` so each replica has one consistent
        endpoint for the whole round.
        """
        cluster_records = global_user_state.get_clusters_from_names(
            [info.cluster_name for info in infos])
        urls: dict[int, str | None] = {}
        handles: dict[int, backends.CloudVmRayResourceHandle] = {}
        ordinary_infos: list[ReplicaInfo] = []
        fenced_groups: dict[tuple[str, str], list[ReplicaInfo]] = {}

        def _retain_identity_uncertainty(
                info: ReplicaInfo,
                error: exceptions.KubernetesPhysicalClusterIdentityError
        ) -> None:
            urls[info.replica_id] = None
            if identity_rejected_replica_ids is not None:
                identity_rejected_replica_ids.add(info.replica_id)
            else:
                self._record_provider_identity_uncertain(
                    info, 'endpoint resolution was fenced off: '
                    f'{common_utils.format_exception(error)}')

        def _defer_phase(infos_to_defer: typing.Iterable[ReplicaInfo],
                         error: exceptions.ProviderPhaseError) -> None:
            deferred_infos = list(infos_to_defer)
            if deferred_replica_ids is not None:
                deferred_replica_ids.update(
                    info.replica_id for info in deferred_infos)
            for info in deferred_infos:
                urls[info.replica_id] = None
                if resolved_route_material is not None:
                    resolved_route_material.pop(info.replica_id, None)
            logger.info(
                'Deferring endpoint resolution for replica IDs %s because '
                'provider admission was unavailable: %s',
                [info.replica_id for info in deferred_infos],
                common_utils.format_exception(error))

        def _defer_resolution_failure(
                infos_to_defer: typing.Iterable[ReplicaInfo],
                error: Exception) -> None:
            """Treat one failed provider URL group as exact zero evidence."""
            deferred_infos = list(infos_to_defer)
            if deferred_replica_ids is not None:
                deferred_replica_ids.update(
                    info.replica_id for info in deferred_infos)
            for info in deferred_infos:
                urls[info.replica_id] = None
                if resolved_route_material is not None:
                    resolved_route_material.pop(info.replica_id, None)
            logger.warning(
                'Deferring endpoint resolution for replica IDs %s after an '
                'unexpected provider URL failure: %s',
                [info.replica_id for info in deferred_infos],
                common_utils.format_exception(error))

        for info in infos:
            cluster_record = cluster_records.get(info.cluster_name)
            try:
                cleanup_fence = (
                    reserved_capacity.parse_protocol_v2_cleanup_fence(info))
            except exceptions.KubernetesPhysicalClusterIdentityError as error:
                _retain_identity_uncertainty(info, error)
                continue
            if cleanup_fence is None:
                ordinary_infos.append(info)
                if cluster_record is None:
                    continue
                try:
                    handle = info.handle(cluster_record)
                except Exception as error:  # pylint: disable=broad-except
                    _defer_resolution_failure([info], error)
                    continue
                if handle is not None:
                    handles[info.replica_id] = handle
                continue
            raw_handle = (cluster_record.get('handle') if isinstance(
                cluster_record, dict) else None)
            # Validate every member before entering a shared fence. This is a
            # pure durable-state check; construction does not contact the
            # provider. A missing/replaced handle aborts the batch without an
            # unfenced endpoint read.
            try:
                reserved_capacity.protocol_v2_provider_fence(info, raw_handle)
            except exceptions.KubernetesPhysicalClusterIdentityError as error:
                _retain_identity_uncertainty(info, error)
                continue
            assert isinstance(raw_handle, backends.CloudVmRayResourceHandle)
            handles[info.replica_id] = raw_handle
            group_key = (cleanup_fence.kubernetes_context,
                         cleanup_fence.physical_cluster_uid)
            fenced_groups.setdefault(group_key, []).append(info)

        uids_by_context: dict[str, set[str]] = {}
        for kube_context, physical_cluster_uid in fenced_groups:
            uids_by_context.setdefault(kube_context,
                                       set()).add(physical_cluster_uid)
        conflicting_contexts = {
            kube_context
            for kube_context, physical_uids in uids_by_context.items()
            if len(physical_uids) > 1
        }
        if conflicting_contexts:
            conflict_error = (exceptions.KubernetesPhysicalClusterIdentityError(
                'One Kubernetes context has conflicting physical-cluster '
                'UIDs in the same endpoint-resolution batch.'))
            for key in list(fenced_groups):
                if key[0] not in conflicting_contexts:
                    continue
                group_infos = fenced_groups.pop(key)
                for info in group_infos:
                    handles.pop(info.replica_id, None)
                    _retain_identity_uncertainty(info, conflict_error)
        provider_config_failed_ids: set[int] = set()
        provider_configs = serve_utils.get_provider_configs_for_handles(
            handles, failed_keys=provider_config_failed_ids)
        infos_by_id = {info.replica_id: info for info in infos}
        for replica_id in provider_config_failed_ids:
            failed_info = infos_by_id.get(replica_id)
            if failed_info is not None:
                _defer_resolution_failure(
                    [failed_info],
                    ValueError('cluster provider configuration is invalid'))

        def _resolve(info: ReplicaInfo) -> None:
            if info.replica_id in provider_config_failed_ids:
                urls[info.replica_id] = None
                return
            cluster_record = cluster_records.get(info.cluster_name)
            handle = handles.get(info.replica_id)
            if cluster_record is None or handle is None:
                urls[info.replica_id] = None
                return
            resolved_url = info._resolve_url(  # pylint: disable=protected-access
                cluster_record=cluster_record,
                handle=handle,
                provider_config=provider_configs.get(info.replica_id),
            )
            urls[info.replica_id] = resolved_url
            if resolved_url is not None and resolved_route_material is not None:
                try:
                    normalized_url = (system_recovery_route_lease.
                                      normalize_route_url(resolved_url))
                except system_recovery_route_lease.RouteLeaseError:
                    normalized_url = None
                if normalized_url is not None:
                    gpu_type = 'unknown'
                    gpu_count = 1
                    try:
                        accelerators = handle.launched_resources.accelerators
                        if accelerators:
                            gpu_type = next(iter(accelerators))
                            try:
                                gpu_count = max(1, int(accelerators[gpu_type]))
                            except (TypeError, ValueError):
                                gpu_count = 1
                    except Exception as error:  # pylint: disable=broad-except
                        # Accelerator projection is row metadata, not a
                        # physical-fence fact. A corrupt peer in one V2 pool
                        # must not revoke healthy peers resolved under the
                        # same fence.
                        _defer_resolution_failure([info], error)
                        return
                    resolved_route_material[info.replica_id] = (
                        route_projection.ResolvedRouteMaterial(
                            normalized_url, gpu_type, gpu_count))
            if identity_rejected_replica_ids is None:
                self._provider_identity_uncertain_replica_ids().discard(
                    info.replica_id)

        def _resolve_fenced_groups(
                admission: provider_phase.ProviderPhaseAdmission, *,
                wait_for_initializer: bool) -> None:
            for group_infos in fenced_groups.values():
                representative = group_infos[0]
                try:
                    with reserved_capacity.protocol_v2_provider_fence(
                            representative,
                            handles[representative.replica_id],
                            phase_admission=admission,
                            wait_for_initializer=wait_for_initializer):
                        for info in group_infos:
                            _resolve(info)
                except exceptions.KubernetesPhysicalClusterFenceBusyError:
                    # Locked callers must treat initializer contention as a
                    # zero-evidence deferral, not physical uncertainty.
                    if deferred_replica_ids is not None:
                        deferred_replica_ids.update(
                            info.replica_id for info in group_infos)
                    continue
                except exceptions.ProviderPhaseError as error:
                    _defer_phase(group_infos, error)
                    continue
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    for info in group_infos:
                        _retain_identity_uncertainty(info, error)
                except Exception as error:  # pylint: disable=broad-except
                    _defer_resolution_failure(group_infos, error)

        def _resolve_ordinary(
                admission: provider_phase.ProviderPhaseAdmission) -> None:
            for info in ordinary_infos:
                # Joining here also covers provider-bearing URL resolution for
                # ordinary rows without inventing a physical identity.
                try:
                    with reserved_capacity.protocol_v2_provider_fence(
                            info,
                            handles.get(info.replica_id),
                            phase_admission=admission):
                        _resolve(info)
                except exceptions.ProviderPhaseError as error:
                    _defer_phase([info], error)
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    _retain_identity_uncertainty(info, error)
                except Exception as error:  # pylint: disable=broad-except
                    _defer_resolution_failure([info], error)

        if phase_admission is not None:
            if fenced_groups:
                if ordinary_infos:
                    raise exceptions.ProviderPhaseMisuseError(
                        'A joined URL-resolution partition must be homogeneous.'
                    )
                _resolve_fenced_groups(phase_admission,
                                       wait_for_initializer=False)
            elif ordinary_infos:
                _resolve_ordinary(phase_admission)
            for info in infos:
                urls.setdefault(info.replica_id, None)
            if resolved_handles is not None:
                resolved_handles.update(handles)
            return urls

        # Standalone active-URL reads establish the process phase themselves,
        # always completing exact v2 groups before ambient rows.
        if fenced_groups:
            try:
                with provider_phase.provider_phase(
                        provider_phase.ProviderPhaseMode.V2_FENCED
                ) as admission:
                    _resolve_fenced_groups(admission, wait_for_initializer=True)
            except exceptions.ProviderPhaseError as error:
                _defer_phase((
                    info for group in fenced_groups.values() for info in group),
                             error)
        if ordinary_infos:
            try:
                with provider_phase.provider_phase(
                        provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                ) as admission:
                    _resolve_ordinary(admission)
            except exceptions.ProviderPhaseError as error:
                _defer_phase(ordinary_infos, error)
        for info in infos:
            urls.setdefault(info.replica_id, None)
        if resolved_handles is not None:
            resolved_handles.update(handles)
        return urls

    def _write_resolved_route_materials(
        self,
        infos: list[ReplicaInfo],
        resolved_routes: dict[int, route_projection.ResolvedRouteMaterial],
    ) -> None:
        """Persist this provider-fenced partition without publishing it."""
        writer = self._route_material_writer
        if writer is None:
            return
        entries = []
        for info in infos:
            resolved = resolved_routes.get(info.replica_id)
            if resolved is None:
                continue
            try:
                spec = self._get_version_spec(info.version)
                material = route_projection.RouteLeaseMaterial(
                    route=resolved,
                    readiness_path=spec.readiness_path,
                    probe_timeout_seconds=spec.readiness_timeout_seconds,
                    post_data=spec.post_data,
                    headers=spec.readiness_headers,
                    async_occupancy=(spec.graceful_drain_async_occupancy),
                    uses_logical_replicas=(spec.uses_logical_replicas is True),
                    is_zero_cost=info.is_zero_cost,
                    planned_capacity=info.planned_capacity,
                    route_allowed=self.system_recovery_allows_routing(info),
                    requires_route_marker=(info.system_recovery_disposition ==
                                           system_recovery_state.
                                           SystemRecoveryDisposition.CAPABLE),
                    route_marker=self.system_recovery_route_marker(
                        info, resolved.url))
            except (route_projection.RouteProjectionValidationError,
                    ValueError) as error:
                logger.warning(
                    'Skipping invalid incremental route material for replica '
                    f'{info.replica_id}: '
                    f'{common_utils.format_exception(error)}')
                continue
            entries.append((info, material))
        if not entries:
            return
        try:
            writer(entries)
        except Exception as error:  # pylint: disable=broad-except
            # Route authority remains fail-closed on its own lease expiry. A
            # PostgreSQL publication outage must not suppress ordinary
            # lifecycle/readiness bookkeeping in this provider-owned round.
            logger.error('Incremental route material persistence failed: '
                         f'{common_utils.format_exception(error)}')

    def _reduce_candidate_probe(
        self,
        info: ReplicaInfo,
        *,
        succeeded: bool,
        probe_started_at: float,
        probe_monotonic_started_at: float,
        exact_job_nonterminal: bool,
        exact_detail_absent: bool,
    ) -> tuple[ReplicaInfo, bool, bool, bool]:
        """Apply the candidate readiness/ABSENT release protocol."""
        if self._update_recovery_required:
            return info, True, False, True
        outcome: dict[str, Any] = {
            'off_route': True,
            'teardown': False,
            'released': False,
            'stale': False,
            'new_deadline': None,
        }
        deadlines = self._candidate_release_monotonic_deadlines

        def _reduce(fresh: ReplicaInfo) -> bool:
            outcome.update({
                'off_route': True,
                'teardown': False,
                'released': False,
                'stale': False,
                'new_deadline': None,
            })
            if not self._probe_snapshot_matches_current(info, fresh):
                outcome['stale'] = True
                return False
            if (fresh.system_recovery_quarantine is not None or
                    self._has_system_recovery_teardown_intent(fresh)):
                outcome['teardown'] = True
                return False
            if (fresh.system_recovery_disposition !=
                    system_recovery_state.SystemRecoveryDisposition.CANDIDATE):
                outcome['off_route'] = (
                    fresh.system_recovery_disposition ==
                    system_recovery_state.SystemRecoveryDisposition.CAPABLE)
                return False
            deadline = deadlines.get(fresh.replica_id)
            if deadline is None and (
                    succeeded or fresh.candidate_ready_observed_at is not None):
                deadline = (
                    time.monotonic() +
                    system_recovery_state.CANDIDATE_RELEASE_GUARD_SECONDS)
                outcome['new_deadline'] = deadline
            reduction = system_recovery_state.reduce_candidate_readiness(
                fresh.system_recovery_disposition,
                fresh.candidate_ready_observed_at,
                fresh.ordinary_release_not_before,
                succeeded=succeeded,
                probe_started_at=probe_started_at,
                now=time.time(),
                monotonic_guard_satisfied=(deadline is not None and
                                           probe_monotonic_started_at
                                           > deadline),
                exact_job_nonterminal=exact_job_nonterminal,
                exact_detail_absent=exact_detail_absent,
                teardown_intent=self._has_system_recovery_teardown_intent(
                    fresh),
                quarantined=False)
            fresh.system_recovery_disposition = reduction.disposition
            fresh.candidate_ready_observed_at = (
                reduction.candidate_ready_observed_at)
            fresh.ordinary_release_not_before = (
                reduction.ordinary_release_not_before)
            outcome['off_route'] = reduction.force_off_route
            outcome['teardown'] = reduction.schedule_legacy_teardown
            outcome['released'] = (
                reduction.disposition ==
                system_recovery_state.SystemRecoveryDisposition.ORDINARY)
            return reduction.changed

        updated = self._patch_system_recovery_with_latest(
            info.replica_id, _reduce)
        if self._update_recovery_required:
            return info, True, False, True
        if updated is None:
            # Lost CAS/owner authority carries no evidence about the opening
            # lifecycle. Defer it exactly like an identity mismatch; never
            # manufacture teardown intent from the stale in-memory row.
            return info, True, False, True
        if outcome['stale']:
            return updated, True, False, True
        new_deadline = outcome['new_deadline']
        if new_deadline is not None:
            deadlines.setdefault(updated.replica_id, new_deadline)
        if (updated.system_recovery_disposition
                != system_recovery_state.SystemRecoveryDisposition.CANDIDATE):
            deadlines.pop(updated.replica_id, None)
        if (updated.system_recovery_disposition ==
                system_recovery_state.SystemRecoveryDisposition.ORDINARY):
            if outcome['released']:
                system_oom_recovery_observability.record_for_replica(
                    'authorization_v3_ordinary', updated)
        return (updated, bool(outcome['off_route']), bool(outcome['teardown']),
                False)

    def _reduce_capable_probe(
        self,
        info: ReplicaInfo,
        *,
        succeeded: bool,
        probe_started_at: float,
    ) -> tuple[ReplicaInfo, system_recovery_state.RecoveryReduction | None,
               bool]:
        """Reduce a capable replica probe against the latest revision."""
        if self._update_recovery_required:
            return info, None, True
        outcome: dict[str, system_recovery_state.RecoveryReduction | None] = {
            'reduction': None
        }
        stale = False
        recovery_events: set[str] = set()

        def _reduce(fresh: ReplicaInfo) -> bool:
            nonlocal stale
            outcome['reduction'] = None
            recovery_events.clear()
            stale = False
            if not self._probe_snapshot_matches_current(info, fresh):
                stale = True
                return False
            if (fresh.system_recovery_disposition
                    != system_recovery_state.SystemRecoveryDisposition.CAPABLE):
                return False
            recovery = fresh.system_recovery
            if recovery is None:
                return False
            reduction = system_recovery_state.reduce_probe_result(
                recovery,
                succeeded=succeeded,
                probe_started_at=probe_started_at,
                now=time.time(),
                was_ready=(fresh.status_property.first_ready_time is not None
                           and fresh.status_property.first_ready_time >= 0),
                detection_window_seconds=(
                    system_recovery_state.CANDIDATE_RELEASE_GUARD_SECONDS),
                teardown_intent=(
                    self._has_system_recovery_teardown_intent(fresh) or
                    self._system_recovery_status_barrier_expired(fresh)),
                quarantined=(fresh.system_recovery_quarantine is not None))
            previous_state = recovery.state
            updated_state = (None if reduction.state is None else
                             reduction.state.state)
            if (updated_state
                    == system_recovery_state.ControllerRecoveryState.RECOVERED
                    and previous_state != updated_state):
                recovery_events.add('recovery_succeeded')
            if (updated_state
                    == system_recovery_state.ControllerRecoveryState.EXHAUSTED
                    and previous_state != updated_state):
                recovery_events.add('recovery_exhausted')
            fresh.system_recovery = reduction.state
            outcome['reduction'] = reduction
            return reduction.changed

        updated = self._patch_system_recovery_with_latest(
            info.replica_id, _reduce)
        if self._update_recovery_required:
            return info, None, True
        if updated is None:
            # CAS/lifecycle unavailability is a stale observation, not
            # negative recovery evidence for a same-ID successor.
            return info, None, True
        if stale:
            return updated, None, True
        for event in recovery_events:
            system_oom_recovery_observability.record_for_replica(event, updated)
        reduction = outcome['reduction']
        if (reduction is None or reduction.schedule_legacy_teardown or
                updated.system_recovery is None or updated.system_recovery.state
                == system_recovery_state.ControllerRecoveryState.EXHAUSTED):
            self._system_recovery_status_initialized_ids().discard(
                updated.replica_id)
        if (reduction is not None and updated.replica_id
                not in self._system_recovery_status_initialized_ids()):
            reduction = dataclasses.replace(reduction,
                                            force_off_route=True,
                                            mark_ready=False)
        return updated, reduction, False

    @staticmethod
    def _readiness_persistence_fingerprint(
        info: ReplicaInfo,
    ) -> tuple[bool, float | None, float | None, float | None]:
        """Return exactly the probe-mutated ordinary readiness fields."""
        return (info.status_property.service_ready_now,
                info.status_property.first_ready_time,
                info.first_not_ready_time, info.first_consecutive_failure_time)

    @staticmethod
    def _is_changed_only_readiness_persistence_eligible(
            info: ReplicaInfo) -> bool:
        """Whether recovery state permits readiness-only write filtering."""
        return (info.system_recovery_disposition
                == system_recovery_state.SystemRecoveryDisposition.ORDINARY and
                info.system_recovery is None and
                info.system_recovery_quarantine is None)

    def _probe_snapshot_matches_current(self, snapshot: ReplicaInfo,
                                        current: ReplicaInfo) -> bool:
        """Whether remote evidence still names this exact live lifecycle."""
        return (self._probe_lifecycle_fingerprint(snapshot)
                == self._probe_lifecycle_fingerprint(current) and
                current.status_property.should_track_service_status() and
                not self._has_system_recovery_teardown_intent(current))

    @staticmethod
    def _probe_lifecycle_fingerprint(info: ReplicaInfo) -> tuple[str, int, int]:
        """Identity whose remote observations may be reduced together."""
        return (info.replica_record_id, info.version,
                info.system_recovery_revision)

    def _prepare_probe_teardown_intent(self,
                                       info: ReplicaInfo,
                                       *,
                                       preempted: bool = False) -> None:
        """Make one latest probe row durably off-route and cleanup-recoverable.

        This is deliberately provider- and thread-free.  The normal cleanup
        refresher re-reads the durable row and constructs the exact down worker
        after this short probe reduction releases ``self.lock``.
        """
        status = info.status_property
        status.service_ready_now = False
        status.is_scale_down = bool(status.is_scale_down or preempted)
        status.purged = False
        if status.sky_down_status != common_utils.ProcessStatus.RUNNING:
            status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.wait_for_idle_before_termination = False
        if preempted:
            status.preempted = True
            status.drain_cap_seconds = 0
            _ensure_drain_started_at(status, 0)
        self._route_lease_registry().deactivate_record(info.replica_id,
                                                       info.replica_record_id)

    def _probe_all_replicas(self) -> list[ReplicaInfo]:
        """Probe without holding the fleet mutation lock over remote I/O."""
        with self.lock:
            self._last_probe_route_result = None
            infos = serve_state.get_replica_infos(self._service_name)
            if self._update_recovery_required:
                return infos
            # Snapshot and prune atomically with manager mutations.  A stale
            # opening snapshot must not retire a route or process guard that a
            # concurrent scale admission just created.
            self._tick_version_spec_cache = {}
            self._prune_system_recovery_process_guards(infos)
            self._route_lease_registry().prune({
                info.replica_id: info.replica_record_id
                for info in infos
                if (info.system_recovery_quarantine is None and
                    info.system_recovery_disposition == system_recovery_state.
                    SystemRecoveryDisposition.CAPABLE and info.system_recovery
                    is not None and info.system_recovery.state !=
                    system_recovery_state.ControllerRecoveryState.EXHAUSTED and
                    not self._has_system_recovery_teardown_intent(info))
            })
        tracked_infos = [
            info for info in infos
            if info.status_property.should_track_service_status()
        ]
        participated_replica_ids: set[int] = set()
        accepted_probe_fingerprints: dict[int, tuple[str, int, int]] = {}
        resolved_routes: dict[int, route_projection.ResolvedRouteMaterial] = {}
        deferred_route_ids: set[int] = set()
        identity_rejected_route_ids: set[int] = set()
        projection_complete = True
        try:
            self._probe_all_replicas_with_snapshot(
                tracked_infos,
                phase_admission=None,
                resolved_route_material=resolved_routes,
                deferred_route_ids=deferred_route_ids,
                identity_rejected_route_ids=identity_rejected_route_ids,
                accepted_probe_fingerprints=accepted_probe_fingerprints)
        except exceptions.ProviderPhaseError:
            # Provider ownership contention produces no evidence this round.
            # It does not hold the fleet lock or publish a partial projection.
            projection_complete = False
        else:
            if self._update_recovery_required:
                return infos
            participated_replica_ids.update(
                info.replica_id for info in tracked_infos)

        # One final complete read is the publication snapshot. It includes
        # replicas admitted while HTTP was in flight and excludes rows deleted
        # during the round; neither can be reconstructed from the opening
        # partition lists without reviving or hiding lifecycle state.
        with self.lock:
            snapshot = serve_state.get_replica_infos(self._service_name)
            current_by_id = {info.replica_id: info for info in snapshot}
            identity_current_ids = {
                replica_id for replica_id, fingerprint in
                accepted_probe_fingerprints.items()
                if replica_id in participated_replica_ids and
                replica_id in current_by_id and
                self._probe_lifecycle_fingerprint(current_by_id[replica_id]) ==
                fingerprint and current_by_id[replica_id].status_property.
                should_track_service_status() and not self.
                _has_system_recovery_teardown_intent(current_by_id[replica_id])
            }
            final_stale_ids = (set(accepted_probe_fingerprints) -
                               identity_current_ids)
            resolved_routes = {
                replica_id: material
                for replica_id, material in resolved_routes.items()
                if replica_id in identity_current_ids
            }
            self._last_probe_route_result = ProbeRouteResult(
                replica_infos=snapshot,
                resolved_routes=resolved_routes,
                identity_verified_replica_ids=(identity_current_ids -
                                               identity_rejected_route_ids),
                complete=(projection_complete and not deferred_route_ids and
                          not final_stale_ids))
        return snapshot

    def _probe_all_replicas_with_snapshot(
        self,
        infos: list[ReplicaInfo],
        *,
        phase_admission: provider_phase.ProviderPhaseAdmission | None,
        resolved_route_material: dict[int,
                                      route_projection.ResolvedRouteMaterial] |
        None = None,
        deferred_route_ids: set[int] | None = None,
        identity_rejected_route_ids: set[int] | None = None,
        accepted_probe_fingerprints: dict[int, tuple[str, int, int]] |
        None = None,
    ) -> list[ReplicaInfo]:
        """Readiness probe replicas.

        This function will probe all replicas to make sure the service is
        ready. It will keep track of:
            (1) the initial delay for each replica;
            (2) the start of the current consecutive-failure window.
        The replica will be terminated if any of the thresholds exceeded.

        Returns:
            The end-of-round fleet snapshot: every replica row as of the end
            of this probe round, with rows mutated by teardown/preemption
            re-read from the DB. Callers can derive the service status from
            it without re-deserializing the whole fleet.
        """
        if self._update_recovery_required:
            return infos
        if identity_rejected_route_ids is None:
            identity_rejected_route_ids = set()
        probe_futures = []
        replica_to_probe = []
        infos_to_probe = [
            info for info in infos
            if info.status_property.should_track_service_status()
        ]
        if not infos_to_probe:
            return infos
        probe_handles: dict[int, backends.CloudVmRayResourceHandle] = {}
        provider_identity_errors: dict[int, str] = {}
        provider_identity_errors_lock = threading.Lock()
        provider_phase_deferred_replica_ids: set[int] = set()

        def _defer_probe_error(info: ReplicaInfo, error: Exception,
                               reason: str) -> None:
            """Keep one failed worker's result out of every reducer."""
            with provider_identity_errors_lock:
                provider_phase_deferred_replica_ids.add(info.replica_id)
                if deferred_route_ids is not None:
                    deferred_route_ids.add(info.replica_id)
                if resolved_route_material is not None:
                    resolved_route_material.pop(info.replica_id, None)
            logger.warning(
                'Deferring probe evidence for replica %s after %s: %s',
                info.replica_id, reason, common_utils.format_exception(error))

        def _defer_provider_phase(info: ReplicaInfo,
                                  error: exceptions.ProviderPhaseError) -> None:
            # This helper may run on any readiness worker. A failed admission
            # carries no readiness, liveness, route, or recovery evidence.
            _defer_probe_error(info, error, 'provider admission failure')

        if not self._is_pool:
            versions = {info.version for info in infos_to_probe}
            failed_spec_versions: set[int] = set()
            try:
                raw_specs = serve_state.get_specs(self._service_name,
                                                  sorted(versions))
            except Exception as batch_error:  # pylint: disable=broad-except
                # The healthy path remains one query.  A corrupt/incompatible
                # retained pickle must not black out every other immutable
                # version, so only a failed batch falls back to isolated
                # version reads and records the exact bad versions.
                logger.warning(
                    'Batched immutable-spec decode failed; isolating service '
                    'versions: %s', common_utils.format_exception(batch_error))
                raw_specs = {}
                for version in sorted(versions):
                    try:
                        raw_specs[version] = serve_state.get_spec(
                            self._service_name, version)
                    except Exception as error:  # pylint: disable=broad-except
                        failed_spec_versions.add(version)
                        logger.warning(
                            'Immutable service version %s could not be '
                            'decoded; deferring only matching replicas: %s',
                            version, common_utils.format_exception(error))
            specs = {
                version: spec
                for version, spec in raw_specs.items()
                if spec is not None
            }
            missing_versions = versions - specs.keys()
            if missing_versions:
                missing_infos = [
                    info for info in infos_to_probe
                    if info.version in missing_versions
                ]
                for info in missing_infos:
                    _defer_probe_error(
                        info, ValueError(f'Version {info.version} not found.'),
                        ('invalid immutable service version'
                         if info.version in failed_spec_versions else
                         'missing immutable service version'))
                infos_to_probe = [
                    info for info in infos_to_probe
                    if info.version not in missing_versions
                ]
                if not infos_to_probe:
                    return infos
            self._tick_version_spec_cache.update(specs)
            deferred_before_url_resolution = set(deferred_route_ids or ())
            probe_urls = self._resolve_probe_urls(
                infos_to_probe,
                phase_admission=phase_admission,
                deferred_replica_ids=deferred_route_ids,
                identity_rejected_replica_ids=identity_rejected_route_ids,
                resolved_route_material=resolved_route_material,
                resolved_handles=probe_handles)
            if deferred_route_ids is not None:
                provider_phase_deferred_replica_ids.update(
                    deferred_route_ids - deferred_before_url_resolution)
            if self._update_recovery_required:
                return infos
        else:
            probe_urls = {}
        candidate_status_inputs: list[tuple[ReplicaInfo, Any]] = []
        route_issue_inputs: dict[
            int, tuple[Any, system_recovery_route_lease.RouteGeneration, bool,
                       float | None, str, str, dict[str, Any] | None,
                       dict[str, str] | None, int]] = {}
        if not self._is_pool:
            candidates = [
                info for info in infos_to_probe
                if info.system_recovery_disposition ==
                system_recovery_state.SystemRecoveryDisposition.CANDIDATE
            ]
            initialized_ids = set(
                self._system_recovery_status_initialized_ids())
            route_issue_candidates: dict[int, tuple[
                ReplicaInfo, system_recovery_route_lease.RouteGeneration, bool,
                float | None, str, str, dict[str, Any] | None,
                dict[str, str] | None, int]] = {}
            for info in infos_to_probe:
                route_url = probe_urls.get(info.replica_id)
                if (route_url is None or
                        info.replica_id not in initialized_ids):
                    continue
                exact_generation = self._system_recovery_route_generation(info)
                status_generation = exact_generation
                if status_generation is None:
                    status_generation = self._system_recovery_route_generation(
                        info, allow_retry_submitted=True)
                if status_generation is None:
                    continue
                try:
                    needs_issuance = self._route_lease_registry(
                    ).needs_issuance(info.replica_id, status_generation,
                                     route_url)
                except system_recovery_route_lease.RouteLeaseError:
                    needs_issuance = True
                if not needs_issuance:
                    continue
                job_id = info.service_job_id
                if (not isinstance(job_id, int) or isinstance(job_id, bool) or
                        job_id < 1):
                    # Preserve a closed MALFORMED result below so the parent
                    # reducer cannot treat missing exact-job identity as an
                    # ordinary readiness success.
                    job_id = -1
                recovery = info.system_recovery
                retry_submitted_adopted_at = (
                    None if recovery is None else
                    recovery.retry_submitted_adopted_at)
                route_issue_candidates[info.replica_id] = (
                    info, status_generation, exact_generation
                    is None, retry_submitted_adopted_at, route_url,
                    self._get_readiness_path(info.version),
                    self._get_post_data(info.version),
                    self._get_readiness_headers(info.version), job_id)

            # Resolve every handle before scheduling the workers.  Besides
            # avoiding per-thread database reads, this freezes the exact
            # row/job/URL/spec inputs used by the ordered evidence chain.
            status_infos = candidates + [
                candidate[0] for candidate in route_issue_candidates.values()
            ]
            status_cluster_records = (global_user_state.get_clusters_from_names(
                [info.cluster_name for info in status_infos])
                                      if status_infos else {})
            if self._update_recovery_required:
                return infos

            def _status_handle(info: ReplicaInfo, record: Any) -> Any:
                if (info.replica_id
                        in self._provider_identity_uncertain_replica_ids()):
                    return None
                try:
                    cleanup_fence = (
                        reserved_capacity.parse_protocol_v2_cleanup_fence(info))
                    if cleanup_fence is None:
                        return None if record is None else info.handle(record)
                    handle = (record.get('handle')
                              if isinstance(record, dict) else None)
                    reserved_capacity.protocol_v2_provider_fence(info, handle)
                    return handle
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    if self._update_recovery_required:
                        return None
                    provider_identity_errors[info.replica_id] = (
                        'exact status handle was fenced off: '
                        f'{common_utils.format_exception(error)}')
                    return None
                except Exception as error:  # pylint: disable=broad-except
                    _defer_probe_error(info, error,
                                       'status handle resolution failure')
                    return None

            for info in candidates:
                if info.replica_id in provider_phase_deferred_replica_ids:
                    continue
                record = status_cluster_records.get(info.cluster_name)
                handle = _status_handle(info, record)
                if info.replica_id in provider_phase_deferred_replica_ids:
                    continue
                if (info.replica_id
                        in self._provider_identity_uncertain_replica_ids()):
                    continue
                candidate_status_inputs.append((info, handle))
            for replica_id, candidate in route_issue_candidates.items():
                (info, generation, predicted_generation,
                 retry_submitted_adopted_at, route_url, readiness_path,
                 post_data, readiness_headers, job_id) = candidate
                if replica_id in provider_phase_deferred_replica_ids:
                    continue
                record = status_cluster_records.get(info.cluster_name)
                handle = _status_handle(info, record)
                if info.replica_id in provider_phase_deferred_replica_ids:
                    continue
                if (info.replica_id
                        in self._provider_identity_uncertain_replica_ids()):
                    continue
                route_issue_inputs[replica_id] = (handle, generation,
                                                  predicted_generation,
                                                  retry_submitted_adopted_at,
                                                  route_url, readiness_path,
                                                  post_data, readiness_headers,
                                                  job_id)

        recovery_backend = backends.CloudVmRayBackend()
        # Probes are pure I/O (HTTP GET/POST with a several-second timeout).
        # Reuse one bounded executor across ticks: constructing and retiring
        # up to 256 native threads every ten seconds retains allocator arenas
        # and amplifies transient provider-response memory at fleet scale.
        executor = self._get_readiness_executor()
        with contextlib.ExitStack() as route_suspension_rollback:
            pending_route_suspensions: list[
                system_recovery_route_lease.RouteSuspension] = []

            def _rollback_pending_route_suspensions() -> None:
                for suspension in pending_route_suspensions:
                    self._route_lease_registry().rollback_suspension(suspension)

            # Any exception before the batch helper takes ownership must
            # restore exact unchanged/unexpired routes.  Ownership is
            # transferred below by copying and then clearing this list.
            route_suspension_rollback.callback(
                _rollback_pending_route_suspensions)

            def _ordered_route_status(
                info: ReplicaInfo,
                handle: Any,
                job_id: int,
            ) -> tuple[job_lib.JobStatus | None, job_lib.JobSystemRecoveryInfo |
                       None, job_lib.JobSystemRecoveryDetailStatus]:
                if handle is None or job_id < 1:
                    return (None, None,
                            job_lib.JobSystemRecoveryDetailStatus.MALFORMED)
                try:
                    with reserved_capacity.protocol_v2_provider_fence(
                            info,
                            handle,
                            phase_admission=phase_admission,
                            wait_for_initializer=False):
                        status_payload = (recovery_backend.
                                          get_job_status_with_system_recovery(
                                              handle, [job_id],
                                              stream_logs=False))
                    if status_payload is None:
                        raise ValueError('exact status payload is missing')
                    statuses, recovery_infos, detail_statuses = status_payload
                    return (
                        statuses.get(job_id), recovery_infos.get(job_id),
                        detail_statuses.get(
                            job_id,
                            job_lib.JobSystemRecoveryDetailStatus.MALFORMED))
                # pylint: disable-next=try-except-raise
                except (exceptions.KubernetesPhysicalClusterIdentityError,
                        exceptions.ProviderPhaseError):
                    # Provider contention is no route-status evidence. The
                    # caller defers the exact lifecycle without reducing the
                    # successful HTTP sample or a fabricated MALFORMED row.
                    raise
                except Exception:  # pylint: disable=broad-except,try-except-raise
                    # A missing/malformed provider result is no route or job
                    # evidence. The exact future boundary defers this row and
                    # continues independently completed peers.
                    raise

            def _probe_nonpool(
                info: ReplicaInfo,
                readiness_path: str,
                post_data: dict[str, Any] | None,
                timeout: int,
                readiness_headers: dict[str, str] | None,
                route_url: str | None,
            ) -> tuple[ReplicaInfo, bool, float, float, tuple[
                    job_lib.JobStatus | None, job_lib.JobSystemRecoveryInfo |
                    None, job_lib.JobSystemRecoveryDetailStatus] | None, bool]:
                # The fallback timestamp is used only for ordinary candidate
                # guard bookkeeping. Route issuance fails closed unless the
                # production HTTP probe invokes the exact-start callback.
                worker_started_at = time.monotonic()
                request_started_at: float | None = None

                def _capture_request_start(started_at: float) -> None:
                    nonlocal request_started_at
                    if request_started_at is None:
                        request_started_at = started_at

                result_info, succeeded, probe_time = info.probe(
                    readiness_path,
                    post_data,
                    timeout,
                    readiness_headers,
                    route_url,
                    request_started_callback=_capture_request_start)
                route_input = route_issue_inputs.get(info.replica_id)
                if (not succeeded or request_started_at is None or
                        route_input is None):
                    return (result_info, succeeded, probe_time,
                            (worker_started_at if request_started_at is None
                             else request_started_at), None, False)

                (handle, _, predicted_generation, retry_submitted_adopted_at,
                 *_, job_id) = route_input
                try:
                    evidence = _ordered_route_status(result_info, handle,
                                                     job_id)
                except exceptions.ProviderPhaseError as error:
                    _defer_provider_phase(result_info, error)
                    return (result_info, succeeded, probe_time,
                            request_started_at, None, False)
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    with provider_identity_errors_lock:
                        provider_identity_errors[result_info.replica_id] = (
                            'ordered route-status lookup was fenced off: '
                            f'{common_utils.format_exception(error)}')
                    return (result_info, succeeded, probe_time,
                            request_started_at, None, False)
                # A RETRY_SUBMITTED row may already carry a durable adoption
                # fence from an earlier round. A probe at or before that fence
                # requires a later readiness request. Route issuance is
                # deferred until the locked reducer revalidates the exact
                # latest row/recovery revision.
                probe_started_after_adoption = (
                    isinstance(retry_submitted_adopted_at, (int, float)) and
                    not isinstance(retry_submitted_adopted_at, bool) and
                    probe_time > float(retry_submitted_adopted_at))
                requires_next_probe = (predicted_generation and
                                       not probe_started_after_adoption)
                return (result_info, succeeded, probe_time, request_started_at,
                        evidence, requires_next_probe)

            def _probe_pool(
                info: ReplicaInfo,
            ) -> tuple[ReplicaInfo, bool, float, float, tuple[
                    job_lib.JobStatus | None, job_lib.JobSystemRecoveryInfo |
                    None, job_lib.JobSystemRecoveryDetailStatus] | None, bool]:
                request_started_at = time.monotonic()
                try:
                    result_info, succeeded, probe_time = info.probe_pool(
                        provider_phase_admission=phase_admission)
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    with provider_identity_errors_lock:
                        provider_identity_errors[info.replica_id] = (
                            'pool probe was fenced off: '
                            f'{common_utils.format_exception(error)}')
                    result_info, succeeded, probe_time = (info, False,
                                                          time.time())
                except exceptions.ProviderPhaseError as error:
                    _defer_provider_phase(info, error)
                    result_info, succeeded, probe_time = (info, False,
                                                          time.time())
                except Exception as error:  # pylint: disable=broad-except
                    _defer_probe_error(info, error,
                                       'unexpected readiness worker failure')
                    result_info, succeeded, probe_time = (info, False,
                                                          time.time())
                return (result_info, succeeded, probe_time, request_started_at,
                        None, False)

            for info in infos_to_probe:
                if self._is_pool:
                    replica_to_probe.append(f'replica_{info.replica_id}(cluster'
                                            f'_name={info.cluster_name})')
                    probe_futures.append(executor.submit(_probe_pool, info))
                else:
                    resolved_url = probe_urls[info.replica_id]
                    readiness_path = self._get_readiness_path(info.version)
                    post_data = self._get_post_data(info.version)
                    timeout = self._get_readiness_timeout_seconds(info.version)
                    readiness_headers = self._get_readiness_headers(
                        info.version)
                    replica_to_probe.append(
                        f'replica_{info.replica_id}(url={resolved_url})')
                    probe_futures.append(
                        executor.submit(
                            _probe_nonpool,
                            info,
                            readiness_path,
                            post_data,
                            timeout,
                            readiness_headers,
                            resolved_url,
                        ),)
            logger.info(f'Replicas to probe: {", ".join(replica_to_probe)}')

            # Draining in submission order does not serialize endpoint I/O:
            # every bounded worker is already running. Route issuance itself
            # remains in the exact-current reducer below so a stale opening
            # lifecycle can never publish a token.
            probe_results: list[tuple[
                ReplicaInfo, bool, float, float,
                tuple[job_lib.JobStatus | None, job_lib.JobSystemRecoveryInfo |
                      None, job_lib.JobSystemRecoveryDetailStatus] | None,
                bool]] = []
            for info, future in zip(infos_to_probe, probe_futures):
                try:
                    probe_results.append(future.result())
                except exceptions.ProviderPhaseError as error:
                    # A worker must normally classify this itself. Keep the
                    # collection boundary defensive so one failed pool never
                    # suppresses independently completed peers.
                    _defer_provider_phase(info, error)
                except Exception as error:  # pylint: disable=broad-except
                    _defer_probe_error(info, error,
                                       'unexpected readiness worker failure')
            # A config/runtime transition can fail while this locked probe is
            # waiting on HTTP.  Treat every completed result as stale before
            # any route, recovery, uptime, replica, or teardown reduction.
            if self._update_recovery_required:
                return infos

            # Candidate release requires ABSENT + nonterminal status from the
            # exact job in the same reconciliation cycle as the fresh probe.
            # These few short-lived candidates share the probe pool; capable
            # steady-state replicas remain on the normal job-status cadence.
            def _candidate_status(info: ReplicaInfo, handle: Any) -> Any:
                if (handle is None or isinstance(info.service_job_id, bool) or
                        not isinstance(info.service_job_id, int) or
                        info.service_job_id < 1):
                    return None
                with reserved_capacity.protocol_v2_provider_fence(
                        info,
                        handle,
                        phase_admission=phase_admission,
                        wait_for_initializer=False):
                    return (
                        recovery_backend.get_job_status_with_system_recovery(
                            handle, [info.service_job_id], stream_logs=False))

            candidate_status_futures = {
                info.replica_id:
                    (info, executor.submit(_candidate_status, info, handle))
                for info, handle in candidate_status_inputs
            }
            candidate_cycle_evidence: dict[int, tuple[bool, bool]] = {}
            candidate_status_evidence: dict[int, tuple[
                job_lib.JobStatus | None, job_lib.JobSystemRecoveryInfo | None,
                job_lib.JobSystemRecoveryDetailStatus]] = {}
            for replica_id, (candidate_info,
                             status_future) in candidate_status_futures.items():
                if self._update_recovery_required:
                    return infos
                try:
                    status_payload = status_future.result()
                    if self._update_recovery_required:
                        return infos
                    if (status_payload is not None and
                        (not isinstance(status_payload, tuple) or
                         len(status_payload) != 3 or not all(
                             isinstance(payload, dict)
                             for payload in status_payload))):
                        raise ValueError(
                            'Candidate status worker returned a malformed '
                            'payload.')
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    if self._update_recovery_required:
                        return infos
                    provider_identity_errors[candidate_info.replica_id] = (
                        'candidate status lookup was fenced off: '
                        f'{common_utils.format_exception(error)}')
                    candidate_cycle_evidence[replica_id] = (False, False)
                    continue
                except exceptions.ProviderPhaseError as error:
                    _defer_provider_phase(candidate_info, error)
                    continue
                except Exception as e:  # pylint: disable=broad-except
                    _defer_probe_error(candidate_info, e,
                                       'candidate status worker failure')
                    continue
                if status_payload is None:
                    statuses: dict[int | None, job_lib.JobStatus | None] = {}
                    recovery_infos: dict[int,
                                         job_lib.JobSystemRecoveryInfo] = {}
                    detail_statuses: dict[
                        int, job_lib.JobSystemRecoveryDetailStatus] = {}
                else:
                    statuses, recovery_infos, detail_statuses = status_payload
                job_id = candidate_info.service_job_id
                valid_job_id = (isinstance(job_id, int) and
                                not isinstance(job_id, bool) and job_id > 0)
                if valid_job_id:
                    assert isinstance(job_id, int)
                    job_status = statuses.get(job_id)
                    detail = recovery_infos.get(job_id)
                    detail_status = detail_statuses.get(
                        job_id, job_lib.JobSystemRecoveryDetailStatus.MALFORMED)
                else:
                    job_status = None
                    detail = None
                    detail_status = (
                        job_lib.JobSystemRecoveryDetailStatus.MALFORMED)
                candidate_status_evidence[replica_id] = (job_status, detail,
                                                         detail_status)
                candidate_cycle_evidence[replica_id] = (
                    isinstance(job_status, job_lib.JobStatus) and
                    not job_status.is_terminal(), detail_status
                    == job_lib.JobSystemRecoveryDetailStatus.ABSENT and
                    detail is None)

            ordered_route_evidence: dict[int, tuple[
                job_lib.JobStatus | None, job_lib.JobSystemRecoveryInfo | None,
                job_lib.JobSystemRecoveryDetailStatus]] = {}
            route_requires_next_probe_ids: set[int] = set()
            for (route_info, _, _, _, evidence,
                 requires_next_probe) in probe_results:
                if evidence is not None:
                    ordered_route_evidence[route_info.replica_id] = evidence
                if requires_next_probe:
                    route_requires_next_probe_ids.add(route_info.replica_id)

            # Confirm interruptions with one cloud-only read per failed
            # interruptible replica. This exact-handle evidence is final: a
            # second name-based status refresh both duplicated provider work
            # and could mutate a same-name replacement cluster before the
            # replica lifecycle fence below rejected the stale result.
            failed_interruptible_infos = [
                info for info, probe_succeeded, _, _, _, _ in probe_results
                if (not probe_succeeded and
                    self._is_interruptible_replica(info) and
                    info.replica_id not in provider_phase_deferred_replica_ids)
            ]
            possibly_preempted_ids: set[int] = set()
            if failed_interruptible_infos:

                def _preemption_liveness(
                        failed_info: ReplicaInfo) -> _PreemptionPrefilterResult:
                    handle = probe_handles.get(failed_info.replica_id,
                                               _NOT_PROVIDED)
                    if handle is _NOT_PROVIDED:
                        return self._cloud_instance_looks_alive(
                            failed_info, phase_admission=phase_admission)
                    return self._cloud_instance_looks_alive(
                        failed_info,
                        phase_admission=phase_admission,
                        handle=handle)

                liveness_results: list[_PreemptionPrefilterResult] = []
                for offset in range(0, len(failed_interruptible_infos),
                                    self._PREEMPTION_PREFILTER_PARALLELISM):
                    batch = failed_interruptible_infos[
                        offset:offset + self._PREEMPTION_PREFILTER_PARALLELISM]
                    futures = [(failed_info,
                                executor.submit(_preemption_liveness,
                                                failed_info))
                               for failed_info in batch]
                    for failed_info, liveness_future in futures:
                        try:
                            liveness_results.append(liveness_future.result())
                        except exceptions.ProviderPhaseError as error:
                            _defer_provider_phase(failed_info, error)
                            liveness_results.append(
                                _PreemptionPrefilterResult(
                                    _PreemptionPrefilterDisposition.
                                    LIVE_OR_UNPROVEN))
                        except Exception as error:  # pylint: disable=broad-except
                            _defer_probe_error(
                                failed_info, error,
                                'unexpected preemption-liveness worker failure')
                            # Preserve exact submission-order alignment. The
                            # sentinel is zero interruption evidence.
                            liveness_results.append(
                                _PreemptionPrefilterResult(
                                    _PreemptionPrefilterDisposition.
                                    LIVE_OR_UNPROVEN))
                if self._update_recovery_required:
                    return infos
                for failed_info, liveness in zip(failed_interruptible_infos,
                                                 liveness_results):
                    if self._update_recovery_required:
                        return infos
                    if liveness.disposition is (
                            _PreemptionPrefilterDisposition.IDENTITY_UNCERTAIN):
                        provider_identity_errors[failed_info.replica_id] = (
                            'cloud liveness could not prove the physical '
                            'Kubernetes identity')
                possibly_preempted_ids = {
                    failed_info.replica_id
                    for failed_info, liveness in zip(failed_interruptible_infos,
                                                     liveness_results)
                    if liveness.disposition in (
                        _PreemptionPrefilterDisposition.INTERRUPTED,
                        _PreemptionPrefilterDisposition.EXACT_KUBERNETES_ABSENT)
                }
            if self._update_recovery_required:
                return infos

            blocked_identity_ids = (set(provider_identity_errors) |
                                    set(identity_rejected_route_ids))
            # All provider/HTTP evidence is complete before this boundary.
            # Re-enter the fleet mutex once, reload the current rows in one
            # query, and reduce only observations whose immutable lifecycle
            # identity still matches.  The remaining work is provider-free
            # in-memory reduction plus one batched persistence transaction.
            route_suspension_rollback.enter_context(self.lock)
            if self._update_recovery_required:
                return infos
            latest_by_id = serve_state.get_replica_infos_from_ids(
                self._service_name,
                sorted(info.replica_id for info in infos_to_probe))
            current_probe_results = []
            identity_pending_writes: dict[int, ReplicaInfo] = {}

            def _defer_stale_result(replica_id: int) -> None:
                if deferred_route_ids is not None:
                    deferred_route_ids.add(replica_id)
                if resolved_route_material is not None:
                    resolved_route_material.pop(replica_id, None)
                if accepted_probe_fingerprints is not None:
                    accepted_probe_fingerprints.pop(replica_id, None)

            for result in probe_results:
                snapshot_info = result[0]
                current_info = latest_by_id.get(snapshot_info.replica_id)
                if (current_info is None or
                        not self._probe_snapshot_matches_current(
                            snapshot_info, current_info)):
                    _defer_stale_result(snapshot_info.replica_id)
                    continue
                if (snapshot_info.replica_id
                        in provider_phase_deferred_replica_ids):
                    _defer_stale_result(snapshot_info.replica_id)
                    continue
                if snapshot_info.replica_id in blocked_identity_ids:
                    message = provider_identity_errors.get(
                        snapshot_info.replica_id,
                        'endpoint resolution was fenced off by the exact '
                        'physical-cluster identity')
                    logger.error(
                        f'Replica {current_info.replica_id} provider identity '
                        f'is uncertain: {message}')
                    current_info.status_property.service_ready_now = False
                    self._provider_identity_uncertain_replica_ids().add(
                        current_info.replica_id)
                    self._route_lease_registry().deactivate_record(
                        current_info.replica_id, current_info.replica_record_id)
                    identity_pending_writes[
                        current_info.replica_id] = current_info
                    continue
                self._provider_identity_uncertain_replica_ids().discard(
                    snapshot_info.replica_id)
                current_probe_results.append((current_info, *result[1:]))
            probe_results = current_probe_results
            # Return current rows even when a stale observation was skipped.
            # A replacement with the same numeric id must never disappear from
            # the service snapshot merely because its predecessor was probed.
            infos = [
                latest_by_id[info.replica_id]
                for info in infos
                if info.replica_id in latest_by_id
            ]

            changed_only_readiness_persistence = self._changed_only_readiness_persistence
            pending_writes: list[tuple[int, ReplicaInfo]] = []
            teardown_intents: dict[int, ReplicaInfo] = {}
            preempted_replica_ids: set[int] = set()
            deferred_recovery_teardowns: dict[int, ReplicaInfo] = {}
            accepted_infos_by_id: dict[int, ReplicaInfo] = {}
            for future_result in probe_results:
                if self._update_recovery_required:
                    return infos
                (info, probe_succeeded, probe_time, probe_monotonic_started_at,
                 route_evidence, _) = future_result
                if (info.replica_id
                        in self._provider_identity_uncertain_replica_ids()):
                    # Provider identity is UNKNOWN, not a negative readiness,
                    # preemption, or job-health observation. Keep the row
                    # durably off-route and retry identity next round.
                    info.status_property.service_ready_now = False
                    continue
                readiness_fingerprint_before = None
                changed_only_eligible_before = False
                if changed_only_readiness_persistence:
                    readiness_fingerprint_before = (
                        self._readiness_persistence_fingerprint(info))
                    changed_only_eligible_before = (
                        self._is_changed_only_readiness_persistence_eligible(
                            info))
                should_teardown = False
                if (not probe_succeeded and
                        info.replica_id in possibly_preempted_ids):
                    # The provider evidence was collected before taking this
                    # mutex.  Exact lifecycle revalidation above is the sole
                    # authority to apply it; only provider-free state mutation
                    # and durable placement evidence remain here.
                    self._apply_confirmed_preemption(info,
                                                     None,
                                                     persist_placement=False)
                    self._prepare_probe_teardown_intent(info, preempted=True)
                    teardown_intents[info.replica_id] = info
                    preempted_replica_ids.add(info.replica_id)
                    continue

                candidate_evidence = candidate_status_evidence.get(
                    info.replica_id)
                if candidate_evidence is not None:
                    reconciled_infos: dict[int, ReplicaInfo] = {}
                    stale_reconciliations: set[int] = set()
                    if self._reconcile_system_recovery_status(
                            info,
                            *candidate_evidence,
                            deferred_teardowns=deferred_recovery_teardowns,
                            reconciled_infos=reconciled_infos,
                            stale_replica_ids=stale_reconciliations):
                        info = reconciled_infos.get(info.replica_id, info)
                        self._route_lease_registry().deactivate_record(
                            info.replica_id, info.replica_record_id)
                        teardown_intents[info.replica_id] = info
                        continue
                    if info.replica_id in stale_reconciliations:
                        _defer_stale_result(info.replica_id)
                        continue
                    info = reconciled_infos.get(info.replica_id, info)

                force_off_route = False
                should_teardown = False
                if info.system_recovery_quarantine is not None:
                    force_off_route = True
                    should_teardown = True
                elif (info.system_recovery_disposition == system_recovery_state.
                      SystemRecoveryDisposition.CANDIDATE):
                    exact_nonterminal, exact_absent = (
                        candidate_cycle_evidence.get(info.replica_id,
                                                     (False, False)))
                    (info, force_off_route, candidate_teardown,
                     candidate_stale) = (self._reduce_candidate_probe(
                         info,
                         succeeded=probe_succeeded,
                         probe_started_at=probe_time,
                         probe_monotonic_started_at=(
                             probe_monotonic_started_at),
                         exact_job_nonterminal=exact_nonterminal,
                         exact_detail_absent=exact_absent))
                    if candidate_stale:
                        _defer_stale_result(info.replica_id)
                        continue
                    should_teardown = candidate_teardown

                if route_evidence is not None:
                    reconciled_infos = {}
                    stale_reconciliations = set()
                    if self._reconcile_system_recovery_status(
                            info,
                            *route_evidence,
                            deferred_teardowns=deferred_recovery_teardowns,
                            reconciled_infos=reconciled_infos,
                            stale_replica_ids=stale_reconciliations):
                        info = reconciled_infos.get(info.replica_id, info)
                        self._route_lease_registry().deactivate_record(
                            info.replica_id, info.replica_record_id)
                        teardown_intents[info.replica_id] = info
                        continue
                    if info.replica_id in stale_reconciliations:
                        _defer_stale_result(info.replica_id)
                        continue
                    info = reconciled_infos.get(info.replica_id, info)

                recovery_holds_failure = False
                if (info.system_recovery_disposition == system_recovery_state.
                        SystemRecoveryDisposition.CAPABLE):
                    (info, recovery_reduction,
                     capable_stale) = self._reduce_capable_probe(
                         info,
                         succeeded=probe_succeeded,
                         probe_started_at=probe_time)
                    if capable_stale:
                        _defer_stale_result(info.replica_id)
                        continue
                    if recovery_reduction is None:
                        force_off_route = True
                        should_teardown = True
                    else:
                        force_off_route = (force_off_route or
                                           recovery_reduction.force_off_route)
                        should_teardown = (
                            should_teardown or
                            recovery_reduction.schedule_legacy_teardown)
                        if recovery_reduction.clear_probe_failure_window:
                            info.first_consecutive_failure_time = None
                        recovery = info.system_recovery
                        has_served = (info.status_property.first_ready_time
                                      is not None and
                                      info.status_property.first_ready_time
                                      >= 0)
                        recovery_holds_failure = has_served and (
                            recovery is None or recovery.state
                            in (system_recovery_state.ControllerRecoveryState.
                                ARMED, system_recovery_state.
                                ControllerRecoveryState.RECOVERING,
                                system_recovery_state.ControllerRecoveryState.
                                RETRY_SUBMITTED) or info.replica_id not in
                            self._system_recovery_status_initialized_ids())

                info.status_property.service_ready_now = (probe_succeeded and
                                                          not force_off_route)
                if not info.status_property.service_ready_now:
                    suspension = (
                        self._suspend_system_recovery_route_if_unroutable(info))
                    if suspension is not None:
                        pending_route_suspensions.append(suspension)
                if probe_succeeded:
                    if (self._uptime is None and
                            info.status_property.service_ready_now):
                        if self._update_recovery_required:
                            return infos
                        self._uptime = probe_time
                        logger.info(
                            f'Replica {info.replica_id} is the first ready '
                            f'replica. Setting uptime to {self._uptime}.')
                        persisted = serve_state.set_service_uptime(
                            self._service_name, int(self._uptime),
                            **self._db_fence_kwargs())
                        if persisted is False:
                            raise RuntimeError(
                                f'Service {self._service_name!r} incarnation '
                                'changed while publishing uptime.')
                    info.first_consecutive_failure_time = None
                    if info.status_property.first_ready_time is None:
                        info.status_property.first_ready_time = probe_time
                else:
                    if info.first_not_ready_time is None:
                        info.first_not_ready_time = probe_time
                    if recovery_holds_failure:
                        logger.info(
                            f'Replica {info.replica_id} is held off-route by '
                            'bounded system recovery; ordinary probe-failure '
                            'teardown is deferred to its recovery deadline.')
                    elif info.status_property.first_ready_time is not None:
                        if info.first_consecutive_failure_time is None:
                            info.first_consecutive_failure_time = probe_time
                        consecutive_failure_time = (
                            probe_time - info.first_consecutive_failure_time)
                        failure_threshold = (
                            self._consecutive_failure_threshold_timeout())
                        if consecutive_failure_time >= failure_threshold:
                            logger.info(
                                f'Replica {info.replica_id} is not ready for '
                                'too long and exceeding consecutive failure '
                                'threshold. Terminating the replica...')
                            should_teardown = True
                        else:
                            logger.info(
                                f'Replica {info.replica_id} is not ready '
                                'but within consecutive failure threshold '
                                f'({consecutive_failure_time}s / '
                                f'{failure_threshold}s). Skipping.')
                    else:
                        initial_delay_seconds = self._get_initial_delay_seconds(
                            info.version)
                        current_delay_seconds = (probe_time -
                                                 info.first_not_ready_time)
                        if current_delay_seconds > initial_delay_seconds:
                            logger.info(
                                f'Replica {info.replica_id} is not ready and '
                                'exceeding initial delay seconds. Terminating '
                                'the replica...')
                            should_teardown = True
                            info.status_property.first_ready_time = -1.0
                        else:
                            current_delay_seconds = int(current_delay_seconds)
                            logger.info(f'Replica {info.replica_id} is not '
                                        'ready but within initial delay '
                                        f'seconds ({current_delay_seconds}s '
                                        f'/ {initial_delay_seconds}s). '
                                        'Skipping.')

                if (info.status_property.service_ready_now and
                        info.system_recovery_disposition
                        == system_recovery_state.SystemRecoveryDisposition.
                        CAPABLE):
                    route_url = probe_urls.get(info.replica_id)
                    route_generation = (
                        self._system_recovery_route_generation(info))
                    route_ready = False
                    if (route_url is not None and route_generation is not None):
                        try:
                            needs_issuance = self._route_lease_registry(
                            ).needs_issuance(info.replica_id, route_generation,
                                             route_url)
                        except system_recovery_route_lease.RouteLeaseError:
                            needs_issuance = True
                        route_ready = (
                            not needs_issuance or
                            info.replica_id not in route_requires_next_probe_ids
                            and not self._update_recovery_required and
                            self._issue_system_recovery_route(
                                info,
                                route_url,
                                probe_monotonic_started_at,
                                route_evidence,
                                deferred_teardowns=deferred_recovery_teardowns))
                    if not route_ready:
                        info.status_property.service_ready_now = False
                        force_off_route = True
                        suspension = (
                            self._suspend_system_recovery_route_if_unroutable(
                                info))
                        if suspension is not None:
                            pending_route_suspensions.append(suspension)
                should_persist_readiness = (
                    not changed_only_readiness_persistence)
                if changed_only_readiness_persistence:
                    changed_only_eligible_after = (
                        self._is_changed_only_readiness_persistence_eligible(
                            info))
                    readiness_fingerprint_after = (
                        self._readiness_persistence_fingerprint(info))
                    should_persist_readiness = (
                        not changed_only_eligible_before or
                        not changed_only_eligible_after or
                        readiness_fingerprint_before
                        != readiness_fingerprint_after)
                if should_persist_readiness:
                    pending_writes.append((info.replica_id, info))
                if should_teardown:
                    teardown_intents[info.replica_id] = info
                else:
                    accepted_infos_by_id[info.replica_id] = info

            # Every teardown source above returns a durable intent instead of
            # entering the legacy scheduler here.  In particular, nested
            # recovery/route reducers and dead-Spot handling must not perform
            # provider I/O, log sync, launch cancellation, or a worker join
            # while this fleet mutex excludes scale reconciliation.
            teardown_intents.update(deferred_recovery_teardowns)
            for replica_id in teardown_intents:
                accepted_infos_by_id.pop(replica_id, None)
            if preempted_replica_ids:
                # Persist the complete placement bench wave once, before the
                # dependent replica intents, rather than once per dead VM.
                self._persist_spot_placement_state_if_dirty()
            for replica_id, teardown_info in teardown_intents.items():
                self._prepare_probe_teardown_intent(teardown_info,
                                                    preempted=replica_id
                                                    in preempted_replica_ids)

            # One multi-row upsert is the durable handoff to the canonical
            # cleanup refresher.  Replace any earlier readiness copy with its
            # final teardown copy so one row is written at most once.
            pending_writes_by_id = dict(identity_pending_writes)
            pending_writes_by_id.update(pending_writes)
            pending_writes_by_id.update(teardown_intents)
            pending_writes = list(pending_writes_by_id.items())
            if self._update_recovery_required:
                return infos
            if pending_route_suspensions:
                transferred_route_suspensions = list(pending_route_suspensions)
                pending_route_suspensions.clear()
                self._persist_replicas(
                    pending_writes,
                    route_suspensions=transferred_route_suspensions)
            elif pending_writes or not changed_only_readiness_persistence:
                # Preserve the ordinary call shape for paths that do not
                # participate in the route-lease protocol.
                self._persist_replicas(pending_writes)

            # Route material is a side effect of the exact current recovery
            # lifecycle, not of the opening URL snapshot.  Persist it only
            # after revision/teardown validation and the readiness batch.  A
            # stale same-record result therefore cannot reactivate a revoked
            # recovery marker through the repository's weaker scalar READY
            # predicate.
            current_resolved_routes = {
                replica_id: material
                for replica_id, material in (
                    resolved_route_material or {}).items()
                if replica_id in accepted_infos_by_id
            }
            if current_resolved_routes:
                self._write_resolved_route_materials([
                    accepted_infos_by_id[replica_id]
                    for replica_id in sorted(current_resolved_routes)
                ], current_resolved_routes)
            if accepted_probe_fingerprints is not None:
                accepted_probe_fingerprints.update({
                    replica_id: self._probe_lifecycle_fingerprint(info)
                    for replica_id, info in accepted_infos_by_id.items()
                })

        if teardown_intents:
            # Wake the existing durable cleanup owner after releasing the
            # fleet mutex.  It re-reads each exact row and never consumes this
            # probe's stale in-memory object as teardown authority.
            self._launch_completion_state()[1].set()

        # The top-level round performs one complete publication read after all
        # partitions finish.  Direct test callers retain the exact-current
        # partition objects reduced above.
        return infos

    def _set_service_status_from_replica_infos(
            self,
            replica_infos: list[ReplicaInfo],
            expected_status_epoch: int | None = None) -> None:
        """Write status from a stable target snapshot without blocking scale."""
        with self._get_status_epoch_lock():
            if self._update_recovery_required:
                return
            if (expected_status_epoch is not None and
                    expected_status_epoch != self._status_epoch_generation):
                return
            for _ in range(2):
                with self._get_target_num_replicas_lock():
                    target_num_replicas = self._target_num_replicas
                    update_mode = self._update_mode
                    target_generation = self._target_num_replicas_generation
                if self._update_recovery_required:
                    return
                serve_utils.set_service_status_and_active_versions_from_replica(
                    self._service_name,
                    replica_infos,
                    update_mode,
                    target_num_replicas=target_num_replicas,
                    **self._db_fence_kwargs())
                with self._get_target_num_replicas_lock():
                    if target_generation == self._target_num_replicas_generation:
                        return
                ownership_lost = self._ownership_lost
                if ownership_lost is not None and ownership_lost.is_set():
                    return
            # Repeated target churn outpaced both optimistic writes. Serialize
            # one final write with publication so the last persisted status
            # cannot already be stale when this probe round returns. This
            # blocking fallback is restricted to the contended path; the
            # common stable-target path above never holds this lock over I/O.
            with self._get_target_num_replicas_lock():
                if self._update_recovery_required:
                    return
                ownership_lost = self._ownership_lost
                if ownership_lost is not None and ownership_lost.is_set():
                    return
                serve_utils.set_service_status_and_active_versions_from_replica(
                    self._service_name,
                    replica_infos,
                    self._update_mode,
                    target_num_replicas=self._target_num_replicas,
                    **self._db_fence_kwargs())

    async def _probe_system_recovery_route_target(
            self, session: aiohttp.ClientSession,
            target: system_recovery_route_lease.RouteProbeTarget) -> None:
        """Publish one dedicated route-probe result immediately."""
        request_started_at = time.monotonic()
        succeeded = False
        try:
            request_kwargs: dict[str, Any] = {
                'headers': target.headers,
                'timeout': aiohttp.ClientTimeout(
                    total=serve_constants.
                    SYSTEM_RECOVERY_ROUTE_PROBE_TIMEOUT_SECONDS),
                'ssl': replica_tls.aiohttp_ssl_setting(),
            }
            if target.method == 'POST':
                request_kwargs['json'] = target.post_data
            async with session.request(target.method, target.probe_url,
                                       **request_kwargs) as response:
                succeeded = response.status == 200
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            succeeded = False
        finally:
            if not self._manager_daemon_should_stop():
                self._route_lease_registry().record_probe_result(
                    target,
                    request_started_at=request_started_at,
                    succeeded=succeeded)

    async def _system_recovery_route_probe_loop(self) -> None:
        """Run nonoverlapping route-probe rounds on a fixed monotonic grid."""
        loop = asyncio.get_running_loop()
        interval = float(
            serve_constants.SYSTEM_RECOVERY_ROUTE_PROBE_INTERVAL_SECONDS)
        next_start = loop.time()
        connector = aiohttp.TCPConnector(
            limit=serve_constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS)
        async with aiohttp.ClientSession(connector=connector) as session:
            while not self._manager_daemon_should_stop():
                delay = next_start - loop.time()
                if delay > 0:
                    await asyncio.sleep(delay)
                    if self._manager_daemon_should_stop():
                        return
                targets = self._route_lease_registry().probe_targets()
                if targets:
                    await asyncio.gather(
                        *(self._probe_system_recovery_route_target(
                            session, target) for target in targets))
                next_start += interval
                # A 15-second hard timeout can intentionally skip 5-second
                # grid slots.  Never overlap rounds or drift the grid.
                now = loop.time()
                while next_start <= now:
                    next_start += interval

    def _system_recovery_route_prober(self) -> None:
        """Supervised-thread entry point for the independent async prober."""
        asyncio.run(self._system_recovery_route_probe_loop())

    def _replica_prober(self) -> None:
        """Periodically probe replicas."""
        try:
            while not self._manager_daemon_should_stop():
                logger.debug('Running replica prober.')
                try:
                    with self._get_status_epoch_lock():
                        status_epoch = self._status_epoch_generation
                    # Reuse the probe round's end-of-round snapshot instead of
                    # re-reading (and re-deserializing) the whole fleet from the
                    # DB a second time per tick.
                    replica_infos = self._probe_all_replicas()
                    if self._manager_daemon_should_stop():
                        return
                    # TODO(zhwu): when there are multiple load balancers, we
                    # need to make sure the active_versions are the union of
                    # all versions of all load balancers.
                    self._set_service_status_from_replica_infos(
                        replica_infos, expected_status_epoch=status_epoch)
                    route_result = self._last_probe_route_result
                    publisher = self._route_projection_publisher
                    if (publisher is not None and route_result is not None and
                            route_result.replica_infos is replica_infos and
                            route_result.complete and
                            not self._manager_daemon_should_stop()):
                        publisher(route_result)

                except Exception as e:  # pylint: disable=broad-except
                    # No matter what error happens, we should keep the
                    # replica prober running.
                    logger.error('Error in replica prober: '
                                 f'{common_utils.format_exception(e)}')
                    with ux_utils.enable_traceback():
                        logger.error(f'  Traceback: {traceback.format_exc()}')
                finally:
                    # The per-version spec memo is valid only for the probe
                    # round that just finished; drop it so the probe-interval
                    # read below (and next round) re-reads every spec fresh.
                    self._tick_version_spec_cache = {}
                # TODO(MaoZiming): Probe cloud for early preemption warning.
                if self._wait_for_manager_daemon_stop(
                        self._get_endpoint_probe_interval_seconds()):
                    return
        finally:
            if self._manager_daemon_stop.is_set():
                self._shutdown_readiness_executor()

    def get_active_replica_urls(self) -> list[str]:
        """Get the urls of all active replicas."""
        record = serve_state.get_service_from_name(self._service_name)
        assert record is not None, (f'{self._service_name} not found on '
                                    'controller records.')
        active_versions = set(record['active_versions'])
        ready_infos = [
            info for info in serve_state.get_replica_infos(self._service_name)
            if (info.status == serve_state.ReplicaStatus.READY and
                info.version in active_versions and
                self.system_recovery_allows_routing(info))
        ]
        resolved_urls = self._resolve_probe_urls(ready_infos)
        return [
            url for info in ready_infos
            if (url := resolved_urls.get(info.replica_id)) is not None
        ]

    def _route_lease_registry(
            self) -> system_recovery_route_lease.ManagerRouteLeaseRegistry:
        return self._system_recovery_route_registry

    def _system_recovery_route_generation(
        self,
        info: ReplicaInfo,
        *,
        allow_retry_submitted: bool = False,
    ) -> system_recovery_route_lease.RouteGeneration | None:
        """Build the exact routable process/row/attempt generation."""
        if (info.system_recovery_disposition
                != system_recovery_state.SystemRecoveryDisposition.CAPABLE or
                info.system_recovery is None):
            return None
        recovery = info.system_recovery
        state = recovery.state
        attempt_id: str | None
        event_id: str | None
        if state == system_recovery_state.ControllerRecoveryState.ARMED:
            attempt_id = recovery.original_attempt_id
            event_id = None
            state_value = 'ARMED'
        elif state == system_recovery_state.ControllerRecoveryState.RECOVERED:
            attempt_id = recovery.replacement_attempt_id
            event_id = recovery.event_id
            state_value = 'RECOVERED'
        elif (allow_retry_submitted and state
              == system_recovery_state.ControllerRecoveryState.RETRY_SUBMITTED):
            # This predicts only the identity that a later post-adoption probe
            # may make RECOVERED.  It is used to decide whether an ordered
            # post-probe remote read is needed; it never grants routing.
            attempt_id = recovery.replacement_attempt_id
            event_id = recovery.event_id
            state_value = 'RECOVERED'
        else:
            return None
        if not isinstance(attempt_id, str):
            return None
        try:
            return system_recovery_route_lease.RouteGeneration(
                controller_epoch=self._system_recovery_route_epoch,
                replica_record_id=info.replica_record_id,
                event_id=event_id,
                attempt_id=attempt_id,
                recovery_state=state_value)
        except system_recovery_route_lease.RouteLeaseError:
            return None

    def _system_recovery_route_evidence_matches(
        self,
        info: ReplicaInfo,
        job_status: job_lib.JobStatus | None,
        detail: job_lib.JobSystemRecoveryInfo | None,
        detail_status: job_lib.JobSystemRecoveryDetailStatus,
        *,
        allow_retry_submitted: bool = False,
    ) -> bool:
        """Whether ordered post-probe evidence names the routable attempt."""
        generation = self._system_recovery_route_generation(
            info, allow_retry_submitted=allow_retry_submitted)
        recovery = info.system_recovery
        if (generation is None or recovery is None or
                not isinstance(job_status, job_lib.JobStatus) or
                job_status.is_terminal() or
                detail_status != job_lib.JobSystemRecoveryDetailStatus.PRESENT):
            return False
        try:
            if detail is None:
                raise system_recovery_state.RecoveryStateError(
                    'Recovery detail is missing.')
            observation = (system_recovery_state.RecoveryObservation.
                           from_job_system_recovery_info(
                               recovery.job_id, detail))
        except system_recovery_state.RecoveryStateError:
            return False
        if (observation.job_id != info.service_job_id or
                observation.capability != recovery.capability or
                observation.node_boot_id != recovery.node_boot_id or
                observation.original_attempt_id
                != recovery.original_attempt_id):
            return False
        if generation.recovery_state == 'ARMED':
            return (observation.phase
                    == system_recovery_state.RemoteRecoveryPhase.ARMED and
                    observation.event_id is None and
                    observation.replacement_attempt_id is None and
                    observation.original_attempt_id == generation.attempt_id)
        return (observation.phase
                == system_recovery_state.RemoteRecoveryPhase.RETRY_SUBMITTED and
                observation.event_id == generation.event_id and
                observation.replacement_attempt_id == generation.attempt_id)

    def _exhaust_retired_route_generation(
        self,
        info: ReplicaInfo,
        *,
        deferred_teardowns: dict[int, ReplicaInfo] | None = None,
    ) -> None:
        """Persist the conservative legacy outcome for a stale generation."""

        def _terminalize(fresh: ReplicaInfo) -> bool:
            generation = self._system_recovery_route_generation(fresh)
            if (generation is None or
                    not self._route_lease_registry().is_retired(
                        fresh.replica_id, generation) or
                    fresh.system_recovery is None):
                return False
            terminal = system_recovery_state.terminalize_for_teardown(
                fresh.system_recovery, now=time.time())
            if terminal == fresh.system_recovery:
                return False
            fresh.system_recovery = terminal
            return True

        updated = self._patch_system_recovery_with_latest(
            info.replica_id, _terminalize)
        if (updated is None or updated.system_recovery is None or
                updated.system_recovery.state
                != system_recovery_state.ControllerRecoveryState.EXHAUSTED):
            return
        self._system_recovery_status_initialized_ids().discard(info.replica_id)
        system_oom_recovery_observability.record_for_replica(
            'recovery_exhausted', updated)
        if deferred_teardowns is not None:
            deferred_teardowns[updated.replica_id] = updated
        else:
            self._terminate_replica(updated.replica_id,
                                    replica_drain_delay_seconds=0)

    def _issue_system_recovery_route(
        self,
        info: ReplicaInfo,
        route_url: str,
        normal_probe_started_at: float,
        evidence: tuple[job_lib.JobStatus | None,
                        job_lib.JobSystemRecoveryInfo | None,
                        job_lib.JobSystemRecoveryDetailStatus] | None,
        *,
        deferred_teardowns: dict[int, ReplicaInfo] | None = None,
    ) -> bool:
        """Issue only after an ordered exact post-readiness remote read."""
        if self._update_recovery_required:
            return False
        generation = self._system_recovery_route_generation(info)
        if (generation is None or evidence is None or
                not self._system_recovery_route_evidence_matches(
                    info, *evidence)):
            return False
        spec = self._get_version_spec(info.version)
        try:
            registry = self._route_lease_registry()
            issued = registry.issue(info.replica_id, generation, route_url,
                                    spec.readiness_path, spec.post_data,
                                    spec.readiness_headers,
                                    normal_probe_started_at)
            if self._update_recovery_required:
                registry.deactivate_record(info.replica_id,
                                           info.replica_record_id)
                return False
        except system_recovery_route_lease.RouteLeaseError:
            issued = False
        if not issued and self._route_lease_registry().is_retired(
                info.replica_id, generation):
            self._exhaust_retired_route_generation(
                info, deferred_teardowns=deferred_teardowns)
        return issued

    def system_recovery_route_marker(
            self, info: ReplicaInfo,
            route_url: str) -> system_recovery_route_lease.RouteMarker | None:
        if not self.system_recovery_allows_routing(info):
            return None
        generation = self._system_recovery_route_generation(info)
        if generation is None:
            return None
        try:
            return self._route_lease_registry().marker(info.replica_id,
                                                       generation, route_url)
        except system_recovery_route_lease.RouteLeaseError:
            return None

    def system_recovery_route_lease_snapshot(self) -> dict[str, Any]:
        return self._route_lease_registry().heartbeat_payload()

    def retire_system_recovery_route(self, info: ReplicaInfo) -> None:
        self._route_lease_registry().deactivate_record(info.replica_id,
                                                       info.replica_record_id)

    def system_recovery_allows_routing(self, info: ReplicaInfo) -> bool:
        """Whether durable recovery state permits this READY row to route."""
        if info.system_recovery_quarantine is not None:
            return False
        disposition = info.system_recovery_disposition
        if (disposition ==
                system_recovery_state.SystemRecoveryDisposition.ORDINARY):
            return True
        if (disposition
                != system_recovery_state.SystemRecoveryDisposition.CAPABLE or
                info.replica_id
                not in self._system_recovery_status_initialized_ids()):
            return False
        recovery = info.system_recovery
        if recovery is None:
            return False
        if recovery.state == system_recovery_state.ControllerRecoveryState.ARMED:
            return recovery.detection_deadline is None
        return (recovery.state ==
                system_recovery_state.ControllerRecoveryState.RECOVERED)

    ###########################################
    # SkyServe Update and replica versioning. #
    ###########################################

    # Runs on the controller's HTTP-handler thread while the autoscaler /
    # prober / refresher daemon threads hold `self.lock` for their own
    # read-modify-write cycles. Without the lock, `scale_up` can read a torn
    # (latest_version, yaml_content) pair — recording a replica at the new
    # version but launching it with the old yaml, which a rolling update then
    # never replaces — and the replica-row upsert below can clobber (or be
    # clobbered by) a concurrent prober status write. See the with_lock
    # invariant note above ReplicaStatusProperty.
    @with_lock
    def update_version(
        self,
        version: int,
        spec: 'service_spec.SkyServiceSpec',
        update_mode: serve_utils.UpdateMode,
        new_spot_placer: 'SpotPlacerType | None' = None,
        install_config: Callable[[], None] | None = None,
    ) -> None:
        if version <= self.latest_version:
            logger.error(f'Invalid version: {version}, '
                         f'latest version: {self.latest_version}')
            return
        # This callback promotes and publishes the exact immutable config
        # generation while the same mutex that fences every replica launch is
        # held. No old-version launch can therefore observe the new policy,
        # and no newer update can overtake this manager transition.
        if install_config is not None:
            install_config()
        new_yaml_content = serve_state.get_yaml_content(self._service_name,
                                                        version)
        assert new_yaml_content is not None, (
            f'yaml content not found for {self._service_name} version {version}'
        )
        new_uses_logical_replicas = spec.uses_logical_replicas is True
        if self._uses_logical_replicas and not new_uses_logical_replicas:
            raise ValueError(
                'Cannot change a logical per-GPU service back to physical '
                'backend replica semantics in place.')
        new_task = load_task_with_service_spec(new_yaml_content, spec)
        new_default_planned_capacity = _uniform_whole_gpu_capacity(
            new_task.resources)
        if new_uses_logical_replicas and new_task.num_nodes != 1:
            _validate_logical_capacity_sources(new_default_planned_capacity,
                                               None, new_task.num_nodes)
        new_logical_exact_accelerator_shapes = (_exact_accelerator_shapes(
            new_task.resources) if new_uses_logical_replicas else {})
        # A service update may change the placement policy or any_of shape
        # set. Use the preflight placer when provided; otherwise load the
        # committed version's centralized catalog. Neither path rebuilds
        # provider candidates in the controller child.
        if (spec.placement_contract.enabled and new_spot_placer is None):
            new_spot_placer = _load_spot_placer(self._service_name, version,
                                                spec, new_task, self._workspace)
        old_spot_placer = self._spot_placer
        if new_spot_placer is not None and old_spot_placer is not None:
            new_spot_placer.inherit_preemption_state(old_spot_placer)
        elif new_spot_placer is not None:
            # A service may disable the placer for one version and later
            # re-enable it without restarting the controller. Recover still-
            # live exact benches from the service row instead of treating that
            # update as a fresh availability epoch.
            placement_states = (serve_state.get_service_placement_policy_states(
                self._service_name))
            new_spot_placer.load_retry_state(
                None if placement_states is
                None else placement_states['spot_placement_state'])
        if new_uses_logical_replicas:
            _validate_logical_capacity_sources(new_default_planned_capacity,
                                               new_spot_placer,
                                               new_task.num_nodes)

        replica_infos = serve_state.get_replica_infos(self._service_name)
        handed_off_retirement_ids: set[int] = set()
        if self._uses_logical_replicas and new_uses_logical_replicas:
            handed_off_retirement_ids = (
                self._handoff_logical_retirements_for_version_update(
                    replica_infos))

        # The previous version's target is not authoritative for the new
        # policy. Keep status derivation conservative until the new autoscaler
        # completes a decision tick and publishes its version-fenced target.
        self._transition_status_epoch_for_version(version, update_mode)
        self.yaml_content = new_yaml_content
        self._uses_logical_replicas = new_uses_logical_replicas
        self._version_specs[version] = spec
        self._cache_task_template(version, new_task)
        self._default_planned_capacity = new_default_planned_capacity
        self._logical_exact_accelerator_shapes = (
            new_logical_exact_accelerator_shapes)
        self._spot_placer = new_spot_placer
        self._persist_spot_placement_state_if_dirty()

        # Reuse all replicas that have the same config as the new version
        # (except for the `service` field) by directly setting the version to be
        # the latest version. This can significantly improve the speed
        # for updating an existing service with only config changes to the
        # service specs, e.g. scale down the service.
        new_config = yaml_utils.safe_load(new_yaml_content)
        # Always create new replicas and scale down old ones when file_mounts
        # are not empty.  Omitted, null, and {} are equivalent no-mount
        # representations and are removed before comparing runtime configs.
        if not _normalize_empty_file_mounts_for_replica_reuse(new_config):
            return
        for key in ['service', 'pool', '_user_specified_yaml']:
            new_config.pop(key, None)
        _remove_nonmaterial_replica_config_metadata(new_config)
        new_config_any_of = (resources_utils.normalize_any_of_resources_config(
            new_config.get('resources', {}).pop('any_of', [])))

        prior_versions = sorted({
            info.version for info in replica_infos if info.version < version and
            (not info.is_terminal or
             info.replica_id in handed_off_retirement_ids)
        })
        prior_yaml_contents = (serve_state.get_yaml_contents(
            self._service_name, prior_versions) if prior_versions else {})
        prior_specs = (serve_state.get_specs(self._service_name, prior_versions)
                       if prior_versions else {})
        if not isinstance(prior_specs, dict):
            prior_specs = {}
        prior_configs = {}
        prior_any_of = {}
        for prior_version in prior_versions:
            yaml_content = prior_yaml_contents.get(prior_version)
            if yaml_content is None:
                raise ValueError('yaml content not found for '
                                 f'{self._service_name} version '
                                 f'{prior_version}')
            old_config = yaml_utils.safe_load(yaml_content)
            _normalize_empty_file_mounts_for_replica_reuse(old_config)
            for key in ['service', 'pool', '_user_specified_yaml']:
                old_config.pop(key, None)
            _remove_nonmaterial_replica_config_metadata(old_config)
            prior_configs[prior_version] = old_config
            prior_any_of[prior_version] = (
                resources_utils.normalize_any_of_resources_config(
                    old_config.get('resources', {}).pop('any_of', [])))
        for info in replica_infos:
            reusable_retirement = (info.replica_id in handed_off_retirement_ids)
            if (info.version < version and
                (not info.is_terminal or reusable_retirement)):
                prior_spec = prior_specs.get(info.version)
                if prior_spec is None:
                    logger.warning(
                        'Service spec for version %s is unavailable; replica %s '
                        'cannot be safely reused for version %s.', info.version,
                        info.replica_id, version)
                    continue
                prior_is_logical = prior_spec.uses_logical_replicas is True
                if prior_is_logical != self._uses_logical_replicas:
                    # A unit change is a one-way rolling bridge. Never relabel
                    # an existing physical row as logical merely because the
                    # model task itself is byte-for-byte identical.
                    logger.info(
                        f'Replica unit changed for replica {info.replica_id}; '
                        'launching replacement capacity instead of reusing '
                        'the backend row.')
                    continue
                old_config = prior_configs[info.version]
                # Bump replica version if all fields except for service are
                # the same.
                # Here, we manually convert the any_of field to a set to avoid
                # only the difference in the random order of the any_of fields.
                old_config_any_of = prior_any_of[info.version]

                if old_config_any_of != new_config_any_of:
                    logger.info(
                        'Replica %s config changed (any_of) from version %s '
                        'to %s; launching replacement capacity.',
                        info.replica_id, info.version, version)
                    continue
                # File mounts should both be empty, as updates always create
                # new buckets if they are not empty.
                if old_config == new_config:
                    if (info.system_recovery_disposition ==
                            system_recovery_state.SystemRecoveryDisposition.
                            CAPABLE):
                        # A route target binds the prior version's closed
                        # readiness request. Keep this row on that version
                        # until the ordinary rolling-update lifecycle replaces
                        # it; relabeling would let the old request renew a token
                        # projected as the new version.
                        logger.info(
                            'Recovery-capable replica %s remains on version '
                            '%s; launching version %s replacement capacity '
                            'instead of reusing its backend row.',
                            info.replica_id, info.version, version)
                        continue
                    logger.info(
                        'Updating replica %s from version %s to version %s '
                        'because its runtime config is unchanged.',
                        info.replica_id, info.version, version)
                    info.version = version
                    if reusable_retirement:
                        # Keep the recovery shape internally consistent while
                        # the old authority epoch prevents admission. Fresh
                        # vNext evidence will either re-fence this victim or
                        # reactivate only the capacity now required.
                        info.status_property.logical_retirement_version = (
                            version)
                    self._persist_replica(info.replica_id, info)
                else:
                    logger.info(
                        'Replica %s runtime config changed from version %s '
                        'to %s; launching replacement capacity.',
                        info.replica_id, info.version, version)

    def _get_version_spec(self, version: int) -> 'service_spec.SkyServiceSpec':
        cached = self._tick_version_spec_cache.get(version)
        if cached is not None:
            return cached
        spec = serve_state.get_spec(self._service_name, version)
        if spec is None:
            raise ValueError(f'Version {version} not found.')
        self._tick_version_spec_cache[version] = spec
        return spec

    def _get_readiness_path(self, version: int) -> str:
        return self._get_version_spec(version).readiness_path

    def _get_post_data(self, version: int) -> dict[str, Any] | None:
        return self._get_version_spec(version).post_data

    def _get_readiness_headers(self, version: int) -> dict[str, str] | None:
        return self._get_version_spec(version).readiness_headers

    def _get_initial_delay_seconds(self, version: int) -> int:
        return self._get_version_spec(version).initial_delay_seconds

    def _get_readiness_timeout_seconds(self, version: int) -> int:
        return self._get_version_spec(version).readiness_timeout_seconds

    def _get_endpoint_probe_interval_seconds(self) -> int:
        return self._get_version_spec(
            self.latest_version).endpoint_probe_interval_seconds

    def _consecutive_failure_threshold_timeout(self) -> int:
        """The timeout for the consecutive failure threshold in seconds.

        If not set by the user, we utilize 180 seconds as the default.
        If it is a pool, we reduce the timeout to 10 seconds to make the
        pool more responsive to the failure.
        """
        spec_timeout = self._get_version_spec(
            self.latest_version).consecutive_failure_threshold_timeout
        if spec_timeout is not None:
            return spec_timeout
        return 10 if self._is_pool else 180
