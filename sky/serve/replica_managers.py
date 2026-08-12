"""ReplicaManager: handles the creation and deletion of endpoint replicas."""
import asyncio
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
import contextlib
import dataclasses
import enum
import functools
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
import filelock

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
from sky.serve import constants as serve_constants
from sky.serve import drain_observability
from sky.serve import ordinary_launch_handoff
from sky.serve import paid_capacity
from sky.serve import provider_phase
from sky.serve import replica_info as replica_info_lib
from sky.serve import replica_tls
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.serve import system_oom_recovery
from sky.serve import system_oom_recovery_observability
from sky.serve import system_recovery_route_lease
from sky.serve import system_recovery_state
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
from sky.utils import thread_utils
from sky.utils import ux_utils
from sky.utils import yaml_utils

if typing.TYPE_CHECKING:

    from sky.serve import service_spec
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
requests = adaptors_common.LazyImport('requests')

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
# A service can queue an arbitrarily large durable teardown wave. Keep the
# queued intent, but bound live worker threads so one controller process cannot
# exhaust its memory or refresh-loop CPU while the global budget is spacious.
_MAX_CONCURRENT_DOWNS_PER_SERVICE = 64
_CHANGED_ONLY_READINESS_PERSISTENCE_ENV_VAR = (
    'SKYPILOT_SERVE_CHANGED_ONLY_READINESS_PERSISTENCE')
# An autoscaler tick can place a full wave before any sky.launch result benches
# an unavailable location. Without a bound, a zero-cost-first placer can pin
# hundreds of replicas to one full Kubernetes pool. Demand placement consumes
# a shared, asynchronously refreshed free-GPU observation. During a startup or
# measurement blackout, keep only a few probes per ACTIVE zero-cost shape.
# Four matches SkyServe's historical per-service launch parallelism.
_ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION = 4
# Sentinel for drain registration's optional pre-resolved replica URL. ``None``
# is a real batched result: the cluster has no resolvable endpoint and the
# bounded deadline must remain the only completion path.
_REPLICA_URL_NOT_PROVIDED: Any = object()


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
    """Immutable accounting facts emitted by the launch persistence seam."""

    replica_id: int
    planned_capacity: int
    funding: _ReplicaLaunchFunding


class _ReplicaLaunchThread(thread_utils.SafeThread):
    """Launch worker that publishes a joinable completion notification."""

    def __init__(self,
                 *args: Any,
                 replica_id: int,
                 completion_queue: 'queue.SimpleQueue[int]',
                 completion_event: threading.Event,
                 bound_ordinary_launch: bool = False,
                 ordinary_legacy_launch: bool = False,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._completion_replica_id = replica_id
        self._completion_queue = completion_queue
        self._completion_event = completion_event
        self.bound_ordinary_launch = bound_ordinary_launch
        self.ordinary_legacy_launch = ordinary_legacy_launch

    def run(self) -> None:
        try:
            super().run()
        finally:
            # This callback runs just before Thread.run returns, so the receiver
            # joins the notified worker before relying on is_alive(). The queue
            # preserves completion across Event coalescing and clear races.
            self._completion_queue.put(self._completion_replica_id)
            self._completion_event.set()


@dataclasses.dataclass(frozen=True)
class LogicalReconcileSnapshot:
    """One immutable LB capacity and occupancy generation."""

    version: int
    generation: int
    observed_slots_by_replica_id: dict[int, int]
    in_flight_by_replica_id: dict[int, int]
    unknown_replica_ids: frozenset[int]
    received_at: float


LogicalAcceleratorState = tuple[tuple[str, int], ...]
LogicalTargetState = (tuple[int, int, int] |
                      tuple[int, int, int, LogicalAcceleratorState,
                            LogicalAcceleratorState])


@dataclasses.dataclass(frozen=True)
class _LogicalPendingLaunchAdmission:
    """One exact-card pending-launch admission calculation."""

    applicable: bool
    target_fence: LogicalTargetState | None
    authorized_ids: frozenset[int]
    reason: str
    details: str = ''


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
) -> task_lib.Task:
    """Build the exact pre-policy task submitted by a replica launch.

    Candidate authorization and the launch worker must hash/submit the same
    task. Keeping their construction in one helper also makes a later
    controller-side environment or security-group change fail closed through
    the backend's post-policy rematch instead of silently widening recovery.
    """
    task = load_task_with_service_spec(yaml_content, authoritative_service_spec)
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
) -> None:
    """Adopt one already-bound launch request after controller restart."""
    ctx = context.get()
    assert ctx is not None, 'Context is not initialized'
    ctx.redirect_log(pathlib.Path(log_file))
    launch_request_id = server_common.RequestId[tuple[int | None,
                                                      backends.ResourceHandle |
                                                      None]](request_id)
    result = sdk.get(launch_request_id)
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


def _decoded_bound_request_error(error: Any) -> BaseException | None:
    """Extract the exception from the exact durable request error shape."""
    if isinstance(error, BaseException):
        return error
    if not isinstance(error, dict):
        return None
    error_object = error.get('object')
    error_type = error.get('type')
    error_message = error.get('message')
    if (set(error) != {'object', 'type', 'message'} or
            not isinstance(error_object, BaseException) or
            error_type != type(error_object).__name__ or
            error_message != str(error_object)):
        return None
    return error_object


def _wait_for_bound_ordinary_launch(
    replica_id: int,
    cluster_name: str,
    request_id: str,
    stream_logs: bool,
    launch_cloud: clouds.Cloud | None,
    reduce_exact: Callable[[Any, BaseException | None], Any],
    cancel_exact: Callable[[str], Any],
    replica_to_launch_cancelled: thread_utils.ThreadSafeDict[int, bool],
    continue_guard: Callable[[], bool] | None = None,
    supersession_guard: Callable[[], bool | tuple[bool, str]] | None = None,
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

        if cancel_reason is None and replica_to_launch_cancelled.get(
                replica_id, False):
            replica_to_launch_cancelled.pop(replica_id)
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
        return _decoded_bound_request_error(error)

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
            try:
                _commit_cancel_if_needed()
            except Exception:  # pylint: disable=broad-except
                time.sleep(_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS)
                continue
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

    launch_request_id = server_common.RequestId[tuple[int | None,
                                                      backends.ResourceHandle |
                                                      None]](request_id)
    result_box: list[Any] = []
    parent_context = context.get()

    def _wait_exact_request() -> None:
        with context.initialize(parent_context):
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
    replica_to_launch_cancelled: thread_utils.ThreadSafeDict[int, bool],
    continue_guard: Callable[[], bool] | None = None,
    supersession_guard: Callable[[], bool | tuple[bool, str]] | None = None,
) -> None:
    """Adopt only the association named by the exact replica pointer."""
    ctx = context.get()
    assert ctx is not None, 'Context is not initialized'
    ctx.redirect_log(pathlib.Path(log_file))
    _wait_for_bound_ordinary_launch(replica_id,
                                    cluster_name,
                                    request_id,
                                    False,
                                    launch_cloud,
                                    reduce_exact,
                                    cancel_exact,
                                    replica_to_launch_cancelled,
                                    continue_guard=continue_guard,
                                    supersession_guard=supersession_guard)


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
    replica_to_launch_cancelled: thread_utils.ThreadSafeDict[int, bool],
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
    inspect_bound_ordinary_launch: Callable[[], Any] | None = None,
    reduce_bound_ordinary_launch: Callable[[Any, BaseException | None], Any] |
    None = None,
    cancel_bound_ordinary_launch: Callable[[str], Any] | None = None,
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
            service_name=service_name)

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
    cleanup_fence = (
        None if protocol_v2_fence is None else
        reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context=protocol_v2_fence.kubernetes_context,
            physical_cluster_uid=protocol_v2_fence.physical_cluster_uid))

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
        request_payloads = sdk.api_status(
            request_ids=[request_id],
            fields=['request_id', 'status'],
            _exact_request_ids=True,
            _use_body=True,
            _request_timeout_seconds=(
                ordinary_launch_handoff.TERMINAL_STATUS_LOOKUP_TIMEOUT_SECONDS),
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
        is_cancelled = replica_to_launch_cancelled.get(replica_id, False)
        if is_cancelled:
            logger.info(f'Replica {replica_id} launch cancelled.')
            # Pop the value to indicate that the signal was received.
            replica_to_launch_cancelled.pop(replica_id)
        return is_cancelled

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
        replica_to_launch_cancelled[replica_id] = True
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
        """Cancel an async launch on owner loss or version supersession."""
        if continue_guard is None and supersession_guard is None:
            return sdk.stream_and_get(request_id)
        stop_watchdog = threading.Event()

        def _watch_launch_authority() -> None:
            while not stop_watchdog.wait(_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS):
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

    if ordinary_launch_submission_uuid is not None:
        if recovery_context_available or protocol_v2_fence is not None:
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Special recovery and reserved-fill launches cannot enter the '
                'ordinary binding path.')
        if (launch_fence is None or inspect_bound_ordinary_launch is None or
                reduce_bound_ordinary_launch is None or
                cancel_bound_ordinary_launch is None):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Bound ordinary launch requires complete context and exact '
                'inspection, reduction, and cancellation authority.')
        bound_workspace_ctx: contextlib.AbstractContextManager = (
            skypilot_config.local_active_workspace_ctx(workspace)
            if workspace is not None else contextlib.nullcontext())
        usage_lib.messages.usage.set_internal()
        with bound_workspace_ctx:
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
                      if workspace is not None else contextlib.nullcontext()):
                    request_id = (sdk.submit_prepared_ordinary_launch_request(
                        prepared_request, ordinary_launch_submission_uuid))
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
                replica_to_launch_cancelled,
                continue_guard=continue_guard,
                supersession_guard=supersession_guard)
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
            # In-memory request/cancellation entries belong to this stale
            # manager only. Keep the durable replica row for the successor to
            # re-drive or garbage-collect; discard local bookkeeping.
            replica_to_request_id.pop(replica_id)
            replica_to_launch_cancelled.pop(replica_id)
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
                replica_to_launch_cancelled.pop(replica_id)
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
                replica_to_launch_cancelled.pop(replica_id)
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

        terminate_cluster(cluster_name,
                          log_file=log_file,
                          continue_guard=cleanup_continue_guard,
                          cleanup_fence=cleanup_fence)

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


# TODO(tian): Combine this with
# sky/spot/recovery_strategy.py::terminate_cluster
@context.contextual
def terminate_cluster(
    cluster_name: str,
    log_file: str,
    replica_drain_delay_seconds: int = 0,
    max_retry: int = 3,
    drain_deadline: float | None = None,
    drain_complete: Callable[[], bool] | None = None,
    continue_guard: Callable[[], bool] | None = None,
    expected_cluster_record_uuid: str | None = None,
    cleanup_fence: reserved_capacity.ProtocolV2CleanupFence | None = None
) -> None:
    """Terminate the sky serve replica cluster."""
    from sky import core  # pylint: disable=import-outside-toplevel

    # Setup logging redirection.
    ctx = context.get()
    assert ctx is not None, 'Context is not initialized'
    ctx.redirect_log(pathlib.Path(log_file))

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
                    raise exceptions.KubernetesPhysicalClusterIdentityError(
                        f'Cannot prove protocol-v2 cleanup for {cluster_name!r}: '
                        'its durable cluster record is absent.')
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
            logger.info(f'Replica cluster {cluster_name} terminated.')
            return
        except exceptions.ClusterDoesNotExist as error:
            if cleanup_fence is not None:
                raise exceptions.KubernetesPhysicalClusterIdentityError(
                    f'Cannot prove protocol-v2 cleanup for {cluster_name!r}: '
                    'the exact cluster disappeared during teardown.') from error
            # The cluster is already terminated.
            logger.info(
                f'Replica cluster {cluster_name} is already terminated.')
            return
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


def _get_resources_ports(
    yaml_content: str,
    service_spec: 'service_spec.SkyServiceSpec | None' = None,
) -> str:
    """Get the replica ingress port from the service or its resources."""
    task = load_task_with_service_spec(yaml_content, service_spec)
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
        service_spec: 'service_spec.SkyServiceSpec | None' = None) -> bool:
    """Get whether the task should use spot."""
    if resource_override is not None:
        use_spot_override = resource_override.get('use_spot')
        if use_spot_override is not None:
            assert isinstance(use_spot_override, bool)
            return use_spot_override
    task = load_task_with_service_spec(yaml_content, service_spec)
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


def _conflicting_protocol_v2_fill_override_indexes(
    resources_overrides: typing.Sequence[Mapping[str, Any] | None],
) -> set[int]:
    """Return every v2 batch entry in a conflicting context/UID group."""
    targets: list[tuple[int, set[str], str]] = []
    uids_by_context: dict[str, set[str]] = {}
    for index, resources_override in enumerate(resources_overrides):
        if not _is_protocol_v2_fill_override(resources_override):
            continue
        assert resources_override is not None
        physical_uid = resources_override.get(
            serve_constants.RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY)
        raw_locations = resources_override.get(
            serve_constants.RESERVED_FILL_ALLOWED_LOCATIONS_OVERRIDE_KEY)
        if (not isinstance(physical_uid, str) or not physical_uid or
                not isinstance(raw_locations, list)):
            # The per-launch validator rejects malformed entries. Only
            # complete provider-free identities participate in batch conflict
            # detection, so unrelated valid entries remain launchable.
            continue
        contexts: set[str] = set()
        malformed = False
        for raw_location in raw_locations:
            if not isinstance(raw_location, Mapping):
                malformed = True
                break
            kube_context = raw_location.get('region')
            if not isinstance(kube_context, str) or not kube_context:
                malformed = True
                break
            contexts.add(kube_context)
        if malformed or not contexts:
            continue
        targets.append((index, contexts, physical_uid))
        for kube_context in contexts:
            uids_by_context.setdefault(kube_context, set()).add(physical_uid)
    conflicting_contexts = {
        context for context, physical_uids in uids_by_context.items()
        if len(physical_uids) > 1
    }
    return {
        index for index, contexts, _ in targets
        if contexts & conflicting_contexts
    }


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
        self._logical_reconcile_snapshot: LogicalReconcileSnapshot | None = (
            None)
        self._logical_state_lock = threading.RLock()
        self._logical_controller_epoch = uuid.uuid4().hex
        self._unknown_capacity_replacement_ids: set[int] = set()
        self._logical_target: LogicalTargetState | None = None
        self._superseded_prune_pending = True
        self._target_num_replicas_lock = threading.Lock()
        self._target_num_replicas: int | None = None
        self._target_num_replicas_generation = 0
        self._status_epoch_lock = threading.Lock()
        self._status_epoch_generation = 0
        self._update_recovery_required = False
        self._pending_version: int | None = None
        self._drain_proof_stats_value = drain_observability.DrainProofStats()

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

    def update_logical_reconcile_snapshot(
        self,
        version: int,
        generation: int,
        observed_slots_by_replica_id: dict[int, int],
        in_flight_by_replica_id: dict[int, int],
        unknown_replica_ids: set[int],
    ) -> None:
        """Atomically publish one complete logical-capacity observation."""
        with self._logical_state_lock:
            self._logical_reconcile_snapshot = LogicalReconcileSnapshot(
                version=version,
                generation=generation,
                observed_slots_by_replica_id=dict(observed_slots_by_replica_id),
                in_flight_by_replica_id=dict(in_flight_by_replica_id),
                unknown_replica_ids=frozenset(unknown_replica_ids),
                received_at=time.monotonic())

    def publish_logical_target(
            self,
            version: int,
            generation: int,
            target_capacity: int,
            target_capacity_by_accelerator: LogicalAcceleratorState = (),
            accelerator_shapes: LogicalAcceleratorState = (),
    ) -> None:
        """Publish the target computed from an exact reconcile generation."""
        with self._logical_state_lock:
            if self._update_recovery_required:
                return
            candidate: LogicalTargetState
            if target_capacity_by_accelerator or accelerator_shapes:
                candidate = (version, generation, target_capacity,
                             target_capacity_by_accelerator, accelerator_shapes)
            else:
                candidate = (version, generation, target_capacity)
            if _logical_target_state_components(candidate) is None:
                logger.warning('Discarding malformed published logical target '
                               f'{candidate!r}.')
                self._logical_target = None
                return
            self._logical_target = candidate

    def invalidate_logical_target(self) -> None:
        """Revoke authority for pending logical-capacity retirements."""
        with self._logical_state_lock:
            if self._update_recovery_required:
                return
            self._logical_target = None

    def _logical_target_fence_holds(
            self,
            version: int,
            decision_generation: int,
            target_capacity: int,
            target_capacity_by_accelerator: LogicalAcceleratorState |
        None = None,
            accelerator_shapes: LogicalAcceleratorState | None = None,
            require_exact_generation: bool = False) -> bool:
        """Whether a logical target intent is still authorized.

        Capacity reports may advance while the autoscaler waits for the
        replica-manager lock on a large fleet. A newer snapshot is stronger
        capacity evidence, not a superseding demand decision. The separately
        published target remains stamped with its producer generation and is
        the authority that invalidates the intent when the autoscaler takes a
        newer decision tick.
        """
        snapshot = self._logical_reconcile_snapshot
        target_state = _logical_target_state_components(self._logical_target)
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
        return (not self._update_recovery_required and snapshot is not None and
                snapshot.version == version and generation_matches and
                self._logical_snapshot_is_fresh(snapshot) and
                self.latest_version == version and
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
            require_exact_generation=require_exact_generation)

    @staticmethod
    def _logical_snapshot_is_fresh(snapshot: LogicalReconcileSnapshot) -> bool:
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
        launch_priority: int = (serve_constants.LB_REQUEST_PRIORITY_MIN)
    ) -> None:
        """Scale up by len(resources_overrides) replicas in one batch.

        Subclasses may override to amortize per-call synchronization; the
        default just loops over `scale_up`.
        """
        del launch_priority
        if (self._update_recovery_required or
            (expected_version is not None and
             expected_version != self.latest_version)):
            return
        for resources_override in resources_overrides:
            self.scale_up(resources_override)

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
    ) -> None:
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

    def scale_down(self,
                   replica_id: int,
                   purge: bool = False,
                   wait_for_idle: bool = False,
                   expected_version: int | None = None) -> None:
        """Scale down replica with replica_id."""
        raise NotImplementedError

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
class _LegacyReplicaMutationRuntime:
    """Process-local owner for the legacy/shadow replica mutation path.

    This is a behavior-preserving removal seam, not the durable action runtime.
    Authoritative launch/down cannot use these pools, request associations, or
    retry clocks once M4 exists. Keeping them behind one object lets the M5
    cleanup delete a named runtime instead of rediscovering state spread across
    ``SkyPilotReplicaManager``.
    """

    launch_completion_queue: queue.SimpleQueue[int] = dataclasses.field(
        default_factory=queue.SimpleQueue)
    launch_completion_event: threading.Event = dataclasses.field(
        default_factory=threading.Event)
    launch_thread_pool: thread_utils.ThreadSafeDict[
        int, thread_utils.SafeThread] = dataclasses.field(
            default_factory=thread_utils.ThreadSafeDict)
    replica_to_request_id: thread_utils.ThreadSafeDict[
        int,
        str] = dataclasses.field(default_factory=thread_utils.ThreadSafeDict)
    replica_to_launch_cancelled: thread_utils.ThreadSafeDict[
        int,
        bool] = dataclasses.field(default_factory=thread_utils.ThreadSafeDict)
    replica_to_logical_launch_fence: thread_utils.ThreadSafeDict[
        int, LogicalTargetState] = dataclasses.field(
            default_factory=thread_utils.ThreadSafeDict)
    down_thread_pool: thread_utils.ThreadSafeDict[
        int, thread_utils.SafeThread] = dataclasses.field(
            default_factory=thread_utils.ThreadSafeDict)
    failed_cleanup_retry_attempts: dict[int, int] = dataclasses.field(
        default_factory=dict)
    failed_cleanup_retry_at: dict[int, float] = dataclasses.field(
        default_factory=dict)

    def recover(self, recover: Callable[[], None]) -> None:
        """Run legacy status-inference recovery through the removal seam."""
        recover()

    def refresh(self, refresh: Callable[[], None]) -> None:
        """Run legacy thread completion through the removal seam."""
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
        '_replica_to_launch_cancelled': 'replica_to_launch_cancelled',
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
        self._publish_legacy_mutation_runtime_state(
            _LegacyReplicaMutationRuntime())
        # Ownership loss and update-recovery are distinct terminal signals:
        # the parent can retain its durable owner while replacing this child.
        self._ownership_lost = threading.Event()
        self._manager_daemon_stop = threading.Event()
        self._scale_reconciliation_event = threading.Event()
        self._system_recovery_route_epoch = str(uuid.uuid4())
        self._ordinary_launch_handoff_route_epoch = str(uuid.uuid4())
        self._system_recovery_route_registry = (
            system_recovery_route_lease.ManagerRouteLeaseRegistry())
        # Durable wall-clock anchors are restored separately. These monotonic
        # guards intentionally start fresh after controller replacement.
        self._candidate_release_monotonic_deadlines: dict[int, float] = {}
        self._system_recovery_status_initialized: set[int] = set()
        self._wait_for_idle_trackers: dict[int,
                                           tuple[_ReplicaDrainTracker | None,
                                                 float]] = {}
        self._legacy_uncertain_logical_retirement_ids: set[int] = set()
        self._recovering_logical_retirement_ids: set[int] = set()
        self._logical_retirement_recovery_deadline: float | None = None
        self._logical_retirement_reactivation_generation: int | None = None
        self._tick_version_spec_cache: dict[int,
                                            service_spec.SkyServiceSpec] = {}
        self._provider_identity_uncertain_ids: set[int] = set()
        self._ordinary_launch_binding_authority: (ControllerBindingAuthority |
                                                  None) = None
        self._ordinary_launch_binding_transition_lock = threading.Lock()
        self._ordinary_launch_binding_transition_in_progress = (
            threading.Event())

    def _publish_legacy_mutation_runtime_state(
            self, runtime: _LegacyReplicaMutationRuntime) -> None:
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
            '_replica_to_launch_cancelled': runtime.replica_to_launch_cancelled,
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

    def _legacy_mutation_runtime_state(self) -> _LegacyReplicaMutationRuntime:
        """Return the legacy owner, adopting pre-refactor instance fields."""
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
            runtime = _LegacyReplicaMutationRuntime()
            for legacy_name, runtime_name in (
                    self._LEGACY_MUTATION_FIELD_MAP.items()):
                legacy_value = self.__dict__.get(legacy_name)
                if legacy_value is not None:
                    setattr(runtime, runtime_name, legacy_value)
            self._publish_legacy_mutation_runtime_state(runtime)
            return runtime

    @property
    def _launch_completion_queue(self) -> queue.SimpleQueue[int]:
        return self._legacy_mutation_runtime_state().launch_completion_queue

    @_launch_completion_queue.setter
    def _launch_completion_queue(self, value: queue.SimpleQueue[int]) -> None:
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
    def _replica_to_launch_cancelled(
            self) -> thread_utils.ThreadSafeDict[int, bool]:
        return self._legacy_mutation_runtime_state().replica_to_launch_cancelled

    @_replica_to_launch_cancelled.setter
    def _replica_to_launch_cancelled(
            self, value: thread_utils.ThreadSafeDict[int, bool]) -> None:
        self._set_legacy_mutation_compat_field('_replica_to_launch_cancelled',
                                               'replica_to_launch_cancelled',
                                               value)

    @_replica_to_launch_cancelled.deleter
    def _replica_to_launch_cancelled(self) -> None:
        self._reset_legacy_mutation_compat_field('_replica_to_launch_cancelled',
                                                 'replica_to_launch_cancelled',
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
        self,) -> tuple['queue.SimpleQueue[int]', threading.Event]:
        """Return lazily compatible completion state for launch workers."""
        runtime = self._legacy_mutation_runtime_state()
        return (runtime.launch_completion_queue,
                runtime.launch_completion_event)

    def _join_notified_launch_workers(self) -> None:
        """Join completion callbacks before the reducer checks is_alive()."""
        completion_queue, _ = self._launch_completion_state()
        while True:
            try:
                replica_id = completion_queue.get_nowait()
            except queue.Empty:
                return
            worker = self._launch_thread_pool.get(replica_id)
            if worker is not None and worker is not threading.current_thread():
                worker.join()

    def clear_scale_reconciliation_signal(self) -> None:
        """Clear feedback before a tick that will read durable state."""
        self._scale_reconciliation_event.clear()

    def wait_for_scale_reconciliation(self, timeout_seconds: float) -> bool:
        """Wait interruptibly for committed typed provider feedback."""
        return self._scale_reconciliation_event.wait(timeout_seconds)

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
            self, info: ReplicaInfo, service_version: int) -> dict[str, Any]:
        """Build the complete immutable admission fence for one replica."""
        authority = self._ordinary_launch_binding_authority
        if (authority is None or authority.capable is not True or
                authority.binding_mode
                != ordinary_launch_binding.BindingMode.BOUND):
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Bound ordinary launch has no promoted controller authority.')
        fence = self._replica_launch_fence_context(service_version)
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
        error = _decoded_bound_request_error(error)
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
        if projection.paid_capacity_pool_key is None:
            paid_outcome = None
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
            paid_capacity_pool_key=projection.paid_capacity_pool_key,
            paid_capacity_outcome=paid_outcome)

    def _bound_ordinary_launch_callbacks(
        self,
        info: ReplicaInfo,
        launch_cloud: clouds.Cloud | None,
        *,
        initial_reduction: Any = None,
    ) -> tuple[Callable[[], Any], Callable[[Any, BaseException | None], Any],
               Callable[[str], Any]]:
        """Close exact inspect/reduce/cancel calls over one record identity."""
        authority = self._ordinary_launch_binding_authority
        if authority is None:
            raise _BoundOrdinaryLaunchUnresolvedError(
                'Bound ordinary launch has no controller authority.')
        context_box: list[Any] = []
        if initial_reduction is not None:
            context_box.append(initial_reduction.context)

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
            return request_postgres.reduce_bound_ordinary_launch(
                _context(), authority, project_replica_result=projector)

        def _cancel(reason: str) -> Any:
            return request_postgres.cancel_bound_ordinary_launch_request(
                _context(), authority, reason, project_replica_result=projector)

        return _inspect, _reduce, _cancel

    def _redrive_bound_ordinary_launch_after_pre_effect(
            self, info: ReplicaInfo) -> bool:
        """Re-enqueue one settled pre-effect row with its exact paid claim."""
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

    def _settle_bound_ordinary_launch_for_teardown(self,
                                                   info: ReplicaInfo) -> None:
        """Cancel, quiesce, and project an exact request before provider down."""
        authority = self._ordinary_launch_binding_authority
        if (authority is None or authority.capable is not True or
                authority.binding_mode
                != ordinary_launch_binding.BindingMode.BOUND):
            return
        initial = request_postgres.lookup_bound_ordinary_launch_cancel_target(
            self._service_name, info.replica_id, info.replica_record_id)
        if initial is None:
            return
        _, reduce_exact, cancel_exact = (self._bound_ordinary_launch_callbacks(
            info, None, initial_reduction=initial))
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
                return
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
                        if (isinstance(worker, _ReplicaLaunchThread) and
                            (worker.ordinary_legacy_launch or
                             worker.bound_ordinary_launch))
                    ]
                    if eligible_workers:
                        raise _BoundOrdinaryLaunchUnresolvedError(
                            'Ordinary-launch binding transition found local '
                            'eligible workers that have not crossed the '
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

    def _remove_replica(self, replica_id: int, replica_record_id: str) -> None:
        suspension = self._route_lease_registry().suspend_record(
            replica_id, replica_record_id)
        try:
            removed = serve_state.remove_replica(
                self._service_name,
                replica_id,
                **self._db_fence_kwargs(),
                expected_replica_record_id=replica_record_id)
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

    def _prove_cleanup_complete(self, info: ReplicaInfo, message: str) -> bool:
        """Settle an unprovable cleanup by reading the physical cluster.

        The durable record is not the only evidence available. A replica
        retired before provisioning ever created one -- or one already
        reclaimed by a status refresh -- owns no Pod, so its cleanup is
        complete rather than uncertain. Retaining those rows forever grows
        the replica table without bound and re-drives teardown for capacity
        that never existed, so prove the negative before giving up.
        """
        try:
            cleanup_fence = reserved_capacity.parse_protocol_v2_cleanup_fence(
                info)
        except exceptions.KubernetesPhysicalClusterIdentityError:
            # A malformed identity is exactly the row that must be retained.
            return False
        if cleanup_fence is None:
            return False
        presence = reserved_capacity.probe_physical_replica_presence(
            cleanup_fence, info.cluster_name)
        if presence is not reserved_capacity.PhysicalReplicaPresence.ABSENT:
            return False
        logger.info(
            f'Replica {info.replica_id} owns no Pod on '
            f'{cleanup_fence.kubernetes_context!r}: treating cleanup as '
            f'complete despite the unprovable step ({message}).')
        self._handle_sky_down_finish(info, format_exc=None)
        return True

    def _record_cleanup_uncertain(self, info: ReplicaInfo,
                                  message: str) -> None:
        """Retain one row whose exact provider cleanup cannot be proven."""
        if self._prove_cleanup_complete(info, message):
            return
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
        yaml_content = serve_state.get_yaml_content(service_name, version)
        assert yaml_content is not None, (
            f'yaml content not found for {service_name} version {version}')
        self.yaml_content: str = yaml_content
        task = load_task_with_service_spec(self.yaml_content, spec)
        self._version_specs = {version: spec}
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
        #    With the thread, uvicorn binds within seconds while recovery
        #    proceeds under the lock; probes/scaling naturally wait on the
        #    lock exactly as they would during any long locked operation.
        recovery_lock_acquired = threading.Event()

        def _recover_with_lock() -> None:
            try:
                with self.lock:
                    recovery_lock_acquired.set()
                    # Retry a failed recovery pass instead of dying silently:
                    # in the previous synchronous design a recovery exception
                    # failed the controller boot and the HA daemon retried
                    # via respawn; a thread that just died would instead
                    # leave interrupted replicas un-redriven forever while
                    # the controller kept serving. Re-running is idempotent:
                    # _launch_replica/_terminate_replica skip replicas whose
                    # threads are already enqueued. The lock is deliberately
                    # held across the backoff — until recovery completes the
                    # daemons must not act on half-redriven state (matching
                    # the pre-existing recovery-holds-lock-first semantics).
                    backoff_seconds = 30
                    while True:
                        try:
                            self._recover_replica_operations()
                            break
                        except Exception as e:  # pylint: disable=broad-except
                            logger.error(
                                'Replica recovery pass failed; retrying in '
                                f'{backoff_seconds}s: '
                                f'{common_utils.format_exception(e)}')
                            with ux_utils.enable_traceback():
                                logger.error(
                                    f'  Traceback: {traceback.format_exc()}')
                            time.sleep(backoff_seconds)
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

    def _recover_replica_operations(self):
        """Route restart inference through the current mutation runtime."""
        self._legacy_mutation_runtime_state().recover(
            self._recover_legacy_replica_operations)

    def _recover_legacy_replica_operations(self) -> None:
        """Re-drive interrupted replica operations from durable state.

        Runs in the dedicated recovery thread started by __init__, which
        holds the manager lock for the whole pass (see __init__ for the
        lock-ordering handshake with the daemon threads)."""
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
                                    sync_down_logs=True,
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
        self._next_replica_id = max(existing_replica_ids, default=0) + 1

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
        interrupted_fill_replicas = [
            info for info in to_up_replicas if info.reserved_fill is True
        ]
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
                                    sync_down_logs=False,
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
            pending_version = self._pending_version
            if (pending_version is not None and
                    pending_version > replica_info.version):
                authority = self._ordinary_launch_binding_authority
                bound_reduction = None
                if (authority is not None and authority.capable is True and
                        authority.binding_mode
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
                                            sync_down_logs=False,
                                            replica_drain_delay_seconds=0,
                                            is_scale_down=True,
                                            in_flight_drain_cap_seconds=0)
                else:
                    logger.info(
                        'Deferring pointerless recovery re-drive for replica '
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
                # _terminate_replica first settles any exact bound pointer;
                # pointerless pre-admission rows proceed directly to cleanup.
                self._terminate_replica(replica_info.replica_id,
                                        sync_down_logs=False,
                                        replica_drain_delay_seconds=0,
                                        is_scale_down=True,
                                        in_flight_drain_cap_seconds=0)
                continue
            if replica_info.system_recovery_quarantine is not None:
                logger.warning(
                    f'Replica {replica_info.replica_id} has quarantined '
                    'system-recovery state; scheduling legacy teardown.')
                self._terminate_replica(replica_info.replica_id,
                                        sync_down_logs=True,
                                        replica_drain_delay_seconds=0)
                continue
            disposition = replica_info.system_recovery_disposition
            if (disposition ==
                    system_recovery_state.SystemRecoveryDisposition.CAPABLE):
                # Exact job capture proves the original launch completed. A
                # controller crash before the ordinary launch-status write
                # must not submit another request for the same generation.
                replica_info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.SUCCEEDED)
                self._persist_replica(replica_info.replica_id, replica_info)
                continue
            if (disposition ==
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
                    launch_thread = _ReplicaLaunchThread(
                        target=adopt_system_recovery_launch,
                        replica_id=replica_info.replica_id,
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
                                            sync_down_logs=True,
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
                ordinary_launch_binding.replica_has_narrow_ordinary_profile(
                    replica_info))
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
                        if recovery_spec is None:
                            raise ValueError(
                                'service spec not found for bound launch '
                                f'recovery of version {replica_info.version}')
                        recovery_task = _build_replica_launch_task(
                            prior_yaml_content,
                            replica_info.replica_id,
                            replica_info.resources_override,
                            exact_resources_override=(
                                replica_info.get_spot_location() is not None),
                            authoritative_service_spec=recovery_spec,
                            service_name=self._service_name)
                        recovery_cloud = next(iter(
                            recovery_task.resources)).cloud
                        _, reduce_bound, cancel_bound = (
                            self._bound_ordinary_launch_callbacks(
                                replica_info,
                                recovery_cloud,
                                initial_reduction=bound_reduction))
                        log_file_name = (
                            serve_utils.generate_replica_launch_log_file_name(
                                self._service_name, replica_info.replica_id,
                                self._resource_scope))
                        completion_queue, completion_event = (
                            self._launch_completion_state())
                        launch_thread = _ReplicaLaunchThread(
                            target=adopt_bound_ordinary_launch,
                            replica_id=replica_info.replica_id,
                            completion_queue=completion_queue,
                            completion_event=completion_event,
                            bound_ordinary_launch=True,
                            args=(
                                replica_info.replica_id,
                                replica_info.cluster_name,
                                log_file_name,
                                bound_reduction.context.request_id,
                                recovery_cloud,
                                reduce_bound,
                                cancel_bound,
                                legacy_runtime.replica_to_launch_cancelled,
                            ),
                            kwargs={
                                'continue_guard':
                                    self._launch_owner_watchdog_allows_continue,
                                'supersession_guard': functools.partial(
                                    self._queued_launch_generation_decision,
                                    replica_info.version),
                            })
                        legacy_runtime.replica_to_request_id[
                            replica_info.replica_id] = (
                                bound_reduction.context.request_id)
                        legacy_runtime.launch_thread_pool[
                            replica_info.replica_id] = launch_thread
                        try:
                            launch_thread.start()
                        except Exception:
                            legacy_runtime.launch_thread_pool.pop(
                                replica_info.replica_id)
                            legacy_runtime.replica_to_request_id.pop(
                                replica_info.replica_id)
                            raise
                        logger.info(
                            'Adopting exact bound ordinary launch %s for '
                            'replica %s after controller restart.',
                            bound_reduction.context.request_id,
                            replica_info.replica_id)
                        continue
                launch_kwargs: dict[str, Any] = {
                    'resources_override': replica_info.resources_override,
                    'existing_replica_infos': all_replica_infos,
                    'recovering_existing_replica': True,
                    'prior_is_zero_cost': replica_info.is_zero_cost,
                    'prior_planned_capacity': prior_planned_capacity,
                    'prior_unknown_capacity_replacement': bool(
                        replica_info.unknown_capacity_replacement),
                    'prior_replica_record_id': replica_info.replica_record_id,
                    'prior_created_at': replica_info.created_at,
                    'prior_version': replica_info.version,
                    'prior_yaml_content': prior_yaml_content,
                }
                prior_rebalance_id = (
                    replica_info.cost_rebalance_for_replica_id)
                # Only forward a real ID so existing launch wrappers retain
                # their compatible signature.
                if (isinstance(prior_rebalance_id, int) and
                        not isinstance(prior_rebalance_id, bool)):
                    launch_kwargs['prior_cost_rebalance_for_replica_id'] = (
                        prior_rebalance_id)
                prior_paid_pool_key = replica_info.paid_capacity_pool_key
                if isinstance(prior_paid_pool_key, str):
                    launch_kwargs['prior_paid_capacity_pool_key'] = (
                        prior_paid_pool_key)
                input_digest = ordinary_launch_handoff.redacted_input_digest(
                    prior_yaml_content, replica_info.resources_override)
                if input_digest is not None:
                    self._emit_ordinary_launch_handoff_event(
                        replica_info,
                        ordinary_launch_handoff.EventKind.
                        CONTROLLER_START_NONTERMINAL,
                        input_digest=input_digest)
                    self._emit_ordinary_launch_handoff_event(
                        replica_info,
                        ordinary_launch_handoff.EventKind.RESTART_REDRIVE,
                        input_digest=input_digest)
                self._launch_replica(replica_info.replica_id, **launch_kwargs)
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
                (self._is_recoverable_uncommitted_logical_retirement(info) or
                 info.status_property.wait_for_idle_before_termination is True))
        ]
        recovery_wait_urls: dict[int, str | None] = {}
        malformed_waiting_rows: dict[int, str] = {}
        ordinary_waiting_replicas: list[ReplicaInfo] = []
        if waiting_replicas and not self._is_pool:
            for info in waiting_replicas:
                try:
                    cleanup_fence = (
                        reserved_capacity.parse_protocol_v2_cleanup_fence(info))
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    malformed_waiting_rows[info.replica_id] = (
                        common_utils.format_exception(error))
                    continue
                if cleanup_fence is None:
                    ordinary_waiting_replicas.append(info)
                else:
                    # Endpoint evidence is only an early-drain optimization.
                    # A recovered v2 row can safely consume its persisted
                    # bounded deadline without any provider lookup here; exact
                    # UID validation remains mandatory before teardown.
                    recovery_wait_urls[info.replica_id] = None

        def _resolve_ordinary_recovery_wait_urls() -> None:
            if not ordinary_waiting_replicas:
                return
            try:
                # One cluster/config snapshot for the whole recovery wave.
                # Resolving ``info.url`` independently repeats cluster-record
                # and provider-config reads around each endpoint lookup while
                # the manager lock blocks every probe and autoscaler tick.
                # Recovery already owns self.lock, so it must never queue
                # behind an opposite provider phase. A busy phase merely
                # removes the early-drain optimization; the durable bounded
                # drain deadline remains authoritative and is retried by the
                # refresher on its next pass.
                with provider_phase.try_provider_phase(
                        provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                ) as phase_admission:
                    recovery_wait_urls.update(
                        self._resolve_probe_urls(
                            ordinary_waiting_replicas,
                            phase_admission=phase_admission))
            except exceptions.ProviderPhaseBusyError:
                recovery_wait_urls.update({
                    info.replica_id: None for info in ordinary_waiting_replicas
                })
                logger.debug(
                    'Deferring recovered drain endpoint resolution because '
                    'the ambient provider phase is busy.')
            except Exception as e:  # pylint: disable=broad-except
                # URL resolution only enables early drain completion. Keep the
                # bounded per-replica fallback rather than failing recovery.
                logger.warning(
                    'Failed to batch-resolve recovered drain endpoints; '
                    'falling back to per-replica resolution: '
                    f'{common_utils.format_exception(e)}')

        legacy_uncertain_ids = self._legacy_uncertain_logical_retirement_ids
        recovering_logical_ids = self._recovering_logical_retirement_ids
        ordinary_wait_urls_resolved = False
        for replica_info in to_down_replicas:
            if (_provider_cleanup_phase_order(replica_info) == 1 and
                    not ordinary_wait_urls_resolved):
                # The wave is v2-first. Do not perform even the ordinary URL
                # optimization until every preceding fenced teardown/wait has
                # completed its inline provider work.
                ordinary_wait_urls_resolved = True
                _resolve_ordinary_recovery_wait_urls()
            try:
                malformed_identity = malformed_waiting_rows.get(
                    replica_info.replica_id)
                if malformed_identity is not None:
                    self._record_cleanup_uncertain(
                        replica_info,
                        'the recovered strict-drain row has malformed '
                        f'physical identity: {malformed_identity}')
                    continue
                if self._is_legacy_uncertain_logical_retirement(replica_info):
                    logger.warning(
                        f'Keeping legacy logical retirement for replica '
                        f'{replica_info.replica_id} off-route until fresh '
                        'replacement capacity is confirmed.')
                    legacy_uncertain_ids.add(replica_info.replica_id)
                    continue
                if self._is_recoverable_uncommitted_logical_retirement(
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
                # Failure teardowns stay in the record
                # (left_in_record=True), and _terminate_replica asserts
                # such rows sync logs down for debuggability -- re-driving
                # them with sync_down_logs=False would trip that assert
                # into this except and leave the replica SHUTTING_DOWN
                # forever. Re-syncing after a restart is harmless
                # (idempotent download).
                left_in_record = not (is_scale_down or status_property.purged)
                self._terminate_replica(replica_info.replica_id,
                                        sync_down_logs=left_in_record,
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
                'Exact bound ordinary-launch recovery remains incomplete for '
                f'replicas {failed_ids!r}; retrying the recovery pass.') from (
                    bound_recovery_errors[0][1])

    ################################
    # Replica management functions #
    ################################

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
        prior_version: int | None = None,
        prior_yaml_content: str | None = None,
        zero_cost_demand_budget: _ZeroCostDemandBudget | None = None,
        paid_location_launch_budget: paid_capacity.LaunchBudget | None = None,
        paid_launch_allowed: bool = True,
        launch_priority: int = serve_constants.LB_REQUEST_PRIORITY_MIN,
        recovering_existing_replica: bool = False,
        logical_reconcile_fence: LogicalTargetState | None = None,
        logical_reconcile_fence_requires_exact_generation: bool = False,
        provider_phase_admission: (provider_phase.ProviderPhaseAdmission |
                                   None) = None,
        try_provider_phase_admission: bool = False,
    ) -> _ReplicaLaunchResult | None:
        """Enqueue one replica launch.

        Returns immutable accounting facts after a launch is durably accepted,
        or None when no launch is accepted. A zero-cost-only fill launch is
        skipped when no zero-cost location is ACTIVE, and the skip must leak
        nothing -- no replica row, no launch thread.

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
        """
        if self._update_recovery_required:
            logger.info(
                'Refusing to enqueue replica %s because the '
                'controller update requires supervised recovery.', replica_id)
            return None
        protocol_v2_fill = _is_protocol_v2_fill_override(resources_override)
        if try_provider_phase_admission and (
                not protocol_v2_fill or provider_phase_admission is not None):
            raise exceptions.ProviderPhaseMisuseError(
                'Try-only provider admission requires one protocol-v2 fill '
                'without an existing admission.')
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
        fill_physical_cluster_uid: str | None = None
        fill_allowed_location_keys: list[dict[str, Any]] | None = None
        fill_pool_identity: reserved_capacity_broker.PoolIdentity | None = None
        fill_exact_accelerator_shape: tuple[str, int] | None = None
        fill_launch_context: str | None = None
        fill_launch_accelerator_shape: tuple[str, int] | None = None
        fill_cloud_launch_guard: (Callable[[], bool | tuple[bool, str]] |
                                  None) = None
        cost_rebalance_for_replica_id = (prior_cost_rebalance_for_replica_id)
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
            fill_physical_cluster_uid = resources_override.pop(
                serve_constants.RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY,
                None)
            raw_allowed_locations = resources_override.pop(
                serve_constants.RESERVED_FILL_ALLOWED_LOCATIONS_OVERRIDE_KEY,
                None)
            if raw_allowed_locations is not None:
                if not isinstance(raw_allowed_locations, list):
                    self._log_fill_skip('malformed pool location fence')
                    return None
                fill_allowed_location_keys = raw_allowed_locations
            zero_cost_only = True
        if (resources_override is not None and
                serve_constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY
                in resources_override):
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
        use_spot = _should_use_spot(launch_yaml_content, resources_override,
                                    launch_spec)
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
                if paid_location_launch_budget is None:
                    paid_location_launch_budget = (
                        paid_capacity.build_launch_budget(
                            self._spot_placer,
                            workspace=self._workspace,
                            existing_replica_infos=existing_replica_infos,
                            globally_managed=(self._service_hash is not None),
                            service_name=self._service_name,
                            service_hash=self._service_hash,
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
                # Broker epoch fence: a fill decision computed from a
                # superseded allocation round must never launch against
                # capacity that may have been re-granted to a peer. Provider
                # snapshot and replica creation timestamps do not establish
                # which row commits the round's debit scan included, so even a
                # same-generation/same-UID v2 allocation cannot safely
                # re-authorize a stale decision.
                # Compared against the POOL's round epoch (stamped
                # alongside the carried epoch): rounds are per-pool, so a
                # grant change on an unrelated pool must not fence this
                # launch. Checked BEFORE any replica row is persisted, so
                # a fenced launch leaks nothing (same contract as the
                # benched skip below); the next decision tick re-emits
                # under the fresh epoch. Only broker-stamped decisions
                # carry an epoch + pool key: without a broker round this
                # is a no-op (single-service identity), and a missing
                # round row (current None) fails open -- there is no
                # newer allocation to defer to. A pool with a pending
                # dead-gap fence marker fails CLOSED instead:
                # current_epoch returns a sentinel no launch ever
                # carries, so the comparison below skips until an
                # epoch-bumping publish clears the marker. This read is
                # only the cheap EARLY-OUT before location selection: a
                # round can still publish between it and the row persist,
                # so the authoritative recheck runs atomically WITH the
                # persist below (persist_fill_replica).
                if fill_grant_epoch is not None and fill_pool_key is not None:
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
                        logical_reconcile_fence_requires_exact_generation)):
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
                    physical_cluster_uid=fill_physical_cluster_uid,
                    kubernetes_context=fill_launch_context,
                    accelerator=fill_card,
                    accelerator_count=fill_count))
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
                    service_name=self._service_name)
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
        replica_port = _get_resources_ports(launch_yaml_content, launch_spec)

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

        def _make_launch_thread(
            recovery_launch_kwargs: dict[str, Any],) -> _ReplicaLaunchThread:
            completion_queue, completion_event = self._launch_completion_state()
            frozen_controller_config = skypilot_config.to_dict()
            frozen_controller_config_path = os.environ.get(
                skypilot_config.ENV_VAR_SKYPILOT_CONFIG)
            ordinary_binding_profile = (
                self._is_ordinary_launch_binding_profile(
                    info, recovery_launch_kwargs))
            bound_ordinary_launch = bool(
                ordinary_binding_profile and
                self._bound_ordinary_launch_is_eligible(info,
                                                        recovery_launch_kwargs))
            ordinary_legacy_launch = bool(ordinary_binding_profile and
                                          not bound_ordinary_launch)
            effective_launch_fence = launch_fence
            if not ordinary_binding_profile and not self._is_pool:
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
                     if ordinary_binding_profile else
                     self._service_is_launch_authorized),
                'cloud_launch_guard': cloud_launch_guard,
                'supersession_guard': functools.partial(
                    self._queued_launch_generation_decision,
                    expected_manager_version),
                'continue_guard': self._launch_owner_watchdog_allows_continue,
                'cleanup_continue_guard': self._service_is_cleanup_authorized,
                'launch_fence': effective_launch_fence,
                'service_spec': launch_spec,
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
                    service_name=self._service_name)
                bound_cloud = next(iter(bound_task.resources)).cloud
                effective_launch_fence = (
                    self._bound_ordinary_launch_fence_context(
                        info, launch_version))
                inspect_bound, reduce_bound, cancel_bound = (
                    self._bound_ordinary_launch_callbacks(info, bound_cloud))
                launch_thread_kwargs.update({
                    'launch_fence': effective_launch_fence,
                    'ordinary_launch_submission_uuid':
                        request_postgres.
                        stable_bound_ordinary_launch_submission_id(
                            self._service_name, info.replica_id,
                            info.replica_record_id),
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
                completion_queue=completion_queue,
                completion_event=completion_event,
                bound_ordinary_launch=bound_ordinary_launch,
                ordinary_legacy_launch=ordinary_legacy_launch,
                args=(replica_id, launch_yaml_content, cluster_name,
                      log_file_name, legacy_runtime.replica_to_request_id,
                      legacy_runtime.replica_to_launch_cancelled,
                      resources_override, retry_until_up),
                kwargs=launch_thread_kwargs,
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
            try:
                with phase_context as effective_admission:
                    with provider_phase.join_provider_phase(
                            effective_admission):
                        physical_context = (
                            kubernetes_adaptor.physical_cluster_uid_fence(
                                fill_launch_context,
                                fill_physical_cluster_uid,
                                wait_for_initializer=False)
                            if try_provider_phase_admission else
                            kubernetes_adaptor.physical_cluster_uid_fence(
                                fill_launch_context, fill_physical_cluster_uid))
                        with physical_context:
                            # Freeze the complete request tuple before its
                            # durable reservation. Construction is side-effect
                            # free; doing it first prevents an allocation row
                            # from being left without local worker ownership if
                            # construction raises.
                            try:
                                launch_thread = _make_launch_thread({})
                            except BaseException:
                                self._release_unstarted_location_retry(location)
                                raise
                            logical_state_guard = (self._logical_state_lock
                                                   if logical_reconcile_fence
                                                   is not None else
                                                   contextlib.nullcontext())
                            with logical_state_guard:
                                if (logical_reconcile_fence is not None and
                                        not self._logical_reconcile_fence_holds(
                                            logical_reconcile_fence,
                                            require_exact_generation=
                                            (logical_reconcile_fence_requires_exact_generation
                                            ))):
                                    logger.info(
                                        'Logical launch was superseded at its '
                                        'final row-persistence fence.')
                                    self._release_unstarted_location_retry(
                                        location)
                                    return None
                                if not reserved_capacity_broker.persist_fill_replica(
                                        self._service_name,
                                        replica_id,
                                        info,
                                        pool_key=fill_pool_key,
                                        expected_epoch=fill_grant_epoch,
                                        expected_protocol_version=(
                                            reserved_capacity_broker.PROTOCOL_V2
                                        ),
                                        expected_service_generation=(
                                            fill_service_generation),
                                        expected_physical_cluster_uid=(
                                            fill_physical_cluster_uid),
                                        **self._db_fence_kwargs()):
                                    self._release_unstarted_location_retry(
                                        location)
                                    self._log_fill_skip(
                                        f'grant epoch {fill_grant_epoch} '
                                        'superseded or round in flight at '
                                        'persist')
                                    return None
                            legacy_runtime.launch_thread_pool[
                                replica_id] = launch_thread
                            if existing_replica_infos is not None:
                                existing_replica_infos.append(info)
                    return launch_result
            except (exceptions.ProviderPhaseBusyError,
                    exceptions.KubernetesPhysicalClusterFenceBusyError
                   ) as error:
                self._release_unstarted_location_retry(location)
                self._log_fill_skip(
                    'provider or physical-cluster phase is busy; deferring '
                    'this launch without reserving capacity')
                # A batch holds the manager lock. Continuing to churn later
                # items after an opposite root is admitted would retain the
                # lock that root needs and recreate phase-to-manager HOL.
                assert try_provider_phase_admission
                raise exceptions.ProviderPhaseBusyError(
                    'Protocol-v2 batch item deferred at its persist seam.'
                ) from error
            except exceptions.KubernetesPhysicalClusterIdentityError as error:
                self._release_unstarted_location_retry(location)
                self._log_fill_skip(
                    'selected protocol-v2 pool physical identity could not be '
                    f'proved: {common_utils.format_exception(error)}')
                return None

        logical_state_guard = (self._logical_state_lock
                               if logical_reconcile_fence is not None else
                               contextlib.nullcontext())
        with logical_state_guard:
            if logical_reconcile_fence is not None:
                if not self._logical_reconcile_fence_holds(
                        logical_reconcile_fence,
                        require_exact_generation=(
                            logical_reconcile_fence_requires_exact_generation)):
                    logger.info('Logical launch was superseded at its final '
                                'row-persistence fence.')
                    return None
            if (zero_cost_only and fill_grant_epoch is not None and
                    fill_pool_key is not None):
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
                        expected_protocol_version=(1 if fill_protocol_version
                                                   is None else
                                                   fill_protocol_version),
                        expected_service_generation=(
                            0 if fill_service_generation is None else
                            fill_service_generation),
                        expected_physical_cluster_uid=(
                            fill_physical_cluster_uid),
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
                    claim_result = paid_capacity.try_persist_claim(
                        service_name=self._service_name,
                        service_hash=self._service_hash,
                        controller_owner=self._controller_owner,
                        replica_id=replica_id,
                        replica_info=info,
                        location=location,
                        budget=paid_location_launch_budget,
                        priority=launch_priority)
                    if claim_result not in (
                            paid_capacity.ClaimResult.ACQUIRED,
                            paid_capacity.ClaimResult.LEGACY_LOCAL):
                        # Selection consumes an expired bench's one-probe
                        # reservation. An admission rejection never reached
                        # the provider, so release that reservation instead of
                        # silently extending the durable capacity cooldown.
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
                        logger.info('Deferring paid demand launch because the '
                                    'service paid-capacity envelope is full.')
                        return None
                    if (claim_result ==
                            paid_capacity.ClaimResult.HIGHER_PRIORITY_WAITING):
                        paid_capacity.defer_for_priority(
                            paid_location_launch_budget, location)
                        logger.info('Deferring paid demand launch at '
                                    f'{location}: {claim_result.value}.')
                        return None
                    if claim_result == paid_capacity.ClaimResult.OWNERSHIP_LOST:
                        raise RuntimeError(
                            f'Service {self._service_name!r} controller '
                            'ownership changed while claiming paid capacity.')
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
        if existing_replica_infos is not None:
            # Bulk callers (recovery re-drive) reuse one snapshot across a
            # whole wave of launches. Append each accepted replica so shared
            # zero-cost capacity accounting sees the in-wave reservations.
            existing_replica_infos.append(info)
        # Don't start right now; we will start it later in _refresh_thread_pool
        # to avoid too many sky.launch running at the same time.
        legacy_runtime.launch_thread_pool[replica_id] = t
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

        The observation is a database-cached, background-refreshed raw GPU
        count. Rows across every service that may not be represented in that
        snapshot are debited under the cross-process reservation lock before
        this budget is returned. A missing/failed observation falls back to a
        bounded number of backend probes. A successful zero is authoritative
        for fixed pools in mixed fallback, while all-Kubernetes placement and
        configured autoscalers receive bounded probes so a pod can trigger
        scheduler preemption or scale-up from zero.
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
        observations = reserved_capacity.get_cached_free_gpus_by_pool(zero_cost)
        kubernetes_only_placement = (
            _placer_has_only_non_spot_kubernetes_gpu_locations(
                self._spot_placer))
        measured = {
            key: observation.free_gpus
            for key, observation in observations.items()
        }
        for measured_pool_key, free_gpus in measured.items():
            if (free_gpus == 0 and
                (kubernetes_only_placement or
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
            created_at = info.created_at
            status_property = info.status_property
            if info.is_ready:
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
                allowance = (_ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION *
                             location_count)
                remaining[pool_key] = max(
                    0, allowance - unresolved_backends_by_pool.get(pool_key, 0))
            else:
                remaining[pool_key] = max(
                    0, free_gpus - unobserved_gpus_by_pool.get(pool_key, 0))
        logger.info('Zero-cost demand capacity snapshot: measured='
                    f'{measured}, unobserved_gpus='
                    f'{unobserved_gpus_by_pool}, '
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
        logical_reconcile_fence: LogicalTargetState | None = None,
        logical_reconcile_fence_requires_exact_generation: bool = False,
        unknown_capacity_replacement: bool = False,
        launch_priority: int = serve_constants.LB_REQUEST_PRIORITY_MIN,
        paid_launch_allowed: bool = True,
        provider_phase_admission: (provider_phase.ProviderPhaseAdmission |
                                   None) = None,
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
        if existing_replica_infos is None:
            direct_launch_kwargs: dict[str, Any] = {}
            if launch_priority != serve_constants.LB_REQUEST_PRIORITY_MIN:
                direct_launch_kwargs['launch_priority'] = launch_priority
            if not paid_launch_allowed:
                direct_launch_kwargs['paid_launch_allowed'] = False
            if provider_phase_admission is not None:
                direct_launch_kwargs['provider_phase_admission'] = (
                    provider_phase_admission)
            elif _is_protocol_v2_fill_override(resources_override):
                direct_launch_kwargs['try_provider_phase_admission'] = True
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
            if logical_reconcile_fence is not None:
                launch_kwargs['logical_reconcile_fence'] = (
                    logical_reconcile_fence)
                launch_kwargs[
                    'logical_reconcile_fence_requires_exact_generation'] = (
                        logical_reconcile_fence_requires_exact_generation)
            if unknown_capacity_replacement:
                launch_kwargs['prior_unknown_capacity_replacement'] = True
            if launch_priority != serve_constants.LB_REQUEST_PRIORITY_MIN:
                launch_kwargs['launch_priority'] = launch_priority
            if not paid_launch_allowed:
                launch_kwargs['paid_launch_allowed'] = False
            if provider_phase_admission is not None:
                launch_kwargs['provider_phase_admission'] = (
                    provider_phase_admission)
            elif _is_protocol_v2_fill_override(resources_override):
                launch_kwargs['try_provider_phase_admission'] = True
            launch_result = self._launch_replica(self._next_replica_id,
                                                 resources_override,
                                                 **launch_kwargs)
        if launch_result is not None:
            assert launch_result.replica_id == self._next_replica_id
            self._next_replica_id += 1
        return launch_result

    def scale_up(self,
                 resources_override: dict[str, Any] | None = None) -> None:
        phase_context: contextlib.AbstractContextManager[
            provider_phase.ProviderPhaseAdmission | None]
        if _is_protocol_v2_fill_override(resources_override):
            phase_context = provider_phase.provider_phase(
                provider_phase.ProviderPhaseMode.V2_FENCED)
        else:
            phase_context = contextlib.nullcontext(None)
        # Blocking provider admission must precede the manager mutex. The
        # selected exact physical capture is lower in this hierarchy and is
        # acquired only after the v2 location has been chosen.
        with phase_context as phase_admission:
            with self.lock:
                if self._update_recovery_required:
                    return
                if self._spot_placer is not None:
                    self._spot_placer.refresh_workspace_policy()
                launch_kwargs: dict[str, Any] = {}
                if phase_admission is not None:
                    launch_kwargs['provider_phase_admission'] = phase_admission
                self._scale_up_one_locked(
                    resources_override,
                    serve_state.get_replica_ids(self._service_name),
                    **launch_kwargs)

    def scale_up_batch(
        self,
        resources_overrides: list[dict[str, Any] | None],
        expected_version: int | None = None,
        launch_priority: int = (serve_constants.LB_REQUEST_PRIORITY_MIN)
    ) -> None:
        """Enqueue a batch of replica launches under one manager lock.

        The manager lock is held by the readiness-probe round for tens of
        seconds per round on large fleets, so per-replica `scale_up` calls
        (one lock acquisition each) trickle through the short gaps between
        rounds: measured live at a 1000-target / ~340-replica fleet, launch
        enqueueing was the scaling bottleneck at ~100 replicas per several
        minutes while the launch budget sat idle. Protocol-v2 items take a
        zero-wait provider phase only at each physical UID proof and durable
        reservation. This keeps the O(1) manager-lock acquisition while
        allowing a queued ambient caller its FIFO turn between replicas.

        Shared zero-cost placement reuses one replica snapshot across the wave.
        The launch path appends each successfully enqueued replica so later
        decisions observe in-wave reservations without querying and unpickling
        all existing rows once per launch.
        """
        conflicting_v2_indexes = (
            _conflicting_protocol_v2_fill_override_indexes(resources_overrides))
        if conflicting_v2_indexes:
            for _ in sorted(conflicting_v2_indexes):
                self._log_fill_skip(
                    'one batch carries conflicting physical-cluster UIDs for '
                    'the same Kubernetes context')
            resources_overrides = [
                resources_override
                for index, resources_override in enumerate(resources_overrides)
                if index not in conflicting_v2_indexes
            ]
            if not resources_overrides:
                return
        try:
            with self.lock:
                if self._update_recovery_required:
                    return
                if self._spot_placer is not None:
                    self._spot_placer.refresh_workspace_policy()
                needs_reservation = (
                    self._batch_needs_placement_snapshot(resources_overrides)
                    and self._uses_shared_zero_cost_demand_budget())
                batch_kwargs: dict[str, Any] = {}
                if launch_priority != serve_constants.LB_REQUEST_PRIORITY_MIN:
                    batch_kwargs['launch_priority'] = launch_priority
                if not needs_reservation:
                    self._scale_up_batch_locked(resources_overrides,
                                                expected_version,
                                                **batch_kwargs)
                    return
                try:
                    lock = locks.get_lock(
                        serve_constants.DEMAND_CAPACITY_RESERVATION_LOCK_ID)
                    with lock.acquire(blocking=False):
                        self._scale_up_batch_locked(resources_overrides,
                                                    expected_version,
                                                    **batch_kwargs)
                except locks.LockTimeout:
                    logger.info(
                        'Deferring demand scale-up because another service is '
                        'reserving shared zero-cost capacity.')
        except exceptions.ProviderPhaseBusyError:
            # The failed item already rolled back its location reservation and
            # wrote no row/thread. Stop the wave so this lock is released to
            # the phase owner; every remaining decision is retried next tick.
            logger.info('Stopping protocol-v2 scale-up batch at a busy '
                        'provider/physical phase boundary.')

    def _scale_up_batch_locked(
        self,
        resources_overrides: list[dict[str, Any] | None],
        expected_version: int | None = None,
        launch_priority: int = (serve_constants.LB_REQUEST_PRIORITY_MIN),
    ) -> None:
        """Persist one physical batch while any shared demand lock is held."""
        if self._update_recovery_required:
            return
        batch_version = self.latest_version
        if (expected_version is not None and expected_version != batch_version):
            logger.info('Discarding stale physical scale-up batch for '
                        f'version {expected_version}; manager is at version '
                        f'{batch_version}.')
            return
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
                requested_frontier_keys=self._requested_paid_frontier_keys(
                    resources_overrides)))
        deferred_paid_overrides: list[dict[str, Any] | None] = []
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
            stop_sequence_before = (paid_location_launch_budget.stop_sequence
                                    if paid_location_launch_budget is not None
                                    else 0)
            service_remaining_before = (
                paid_location_launch_budget.service_remaining
                if paid_location_launch_budget is not None else None)
            override_before = (None if resources_override is None else
                               dict(resources_override))
            launch_result = self._scale_up_one_locked(resources_override,
                                                      used_replica_ids,
                                                      existing_replica_infos,
                                                      zero_cost_demand_budget,
                                                      **scale_up_kwargs)
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
    ) -> None:
        """Plan and persist complete backend shapes up to a logical target.

        Selection and row persistence share the manager lock and one mutable
        fleet snapshot. Each persisted backend immediately participates in the
        next placement decision, so a single 8-slot choice removes eight slots
        from the shortfall instead of causing eight physical launches.
        """
        if self._update_recovery_required:
            return
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
        if not self._logical_target_fence_holds(
                version, reconcile_generation, target_capacity,
                target_by_accelerator_state, accelerator_shape_state):
            logger.info('Discarding stale logical scale-up intent for '
                        f'version {version}, generation '
                        f'{reconcile_generation}.')
            return
        if launch_budget is not None and launch_budget < 0:
            logger.warning('Discarding logical scale-up with negative launch '
                           f'budget {launch_budget}.')
            return
        if launch_budget == 0:
            logger.info('Deferring logical scale-up until the current launch '
                        'wave has remaining authority.')
            return
        snapshot = self._logical_reconcile_snapshot
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
        if not self._uses_shared_zero_cost_demand_budget():
            if target_capacity_by_accelerator is None:
                self._scale_up_to_logical_capacity_locked(
                    target_capacity, version, reconcile_generation, snapshot,
                    replace_unknown_replica_ids, **launch_kwargs)
            else:
                self._scale_up_to_logical_capacity_locked(
                    target_capacity, version, reconcile_generation, snapshot,
                    replace_unknown_replica_ids, target_capacity_by_accelerator,
                    accelerator_shapes, **launch_kwargs)
            return
        try:
            lock = locks.get_lock(
                serve_constants.DEMAND_CAPACITY_RESERVATION_LOCK_ID)
            with lock.acquire(blocking=False):
                if target_capacity_by_accelerator is None:
                    self._scale_up_to_logical_capacity_locked(
                        target_capacity, version, reconcile_generation,
                        snapshot, replace_unknown_replica_ids, **launch_kwargs)
                else:
                    self._scale_up_to_logical_capacity_locked(
                        target_capacity, version, reconcile_generation,
                        snapshot, replace_unknown_replica_ids,
                        target_capacity_by_accelerator, accelerator_shapes,
                        **launch_kwargs)
        except locks.LockTimeout:
            logger.info('Deferring logical scale-up because another service '
                        'is reserving shared zero-cost capacity.')

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
    ) -> None:
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
                return
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
                return
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
                requested_frontier_keys=(None if not card_targets else {
                    (str(card).casefold(),) for card in paid_cards
                }))
        deferred_cards: set[str] = set()
        launched_capacity = 0
        while True:
            if not self._logical_target_fence_holds(
                    version,
                    reconcile_generation,
                    target_capacity,
                    card_target_state,
                    shape_state,
                    require_exact_generation=bool(replace_unknown_replica_ids)):
                logger.info('Stopping logical scale-up batch after its '
                            'reconciliation fence advanced.')
                break
            current_snapshot = self._logical_reconcile_snapshot
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
            paid_launch_allowed = (paid_authority_left is None or
                                   selected_card is None or
                                   paid_authority_left.get(selected_card,
                                                           0) > 0)
            if (paid_launch_allowed and
                    self._paid_service_envelope_blocks_launch(
                        paid_location_launch_budget, resources_override)):
                if selected_card is not None:
                    deferred_cards.add(selected_card)
                    continue
                break
            launch_kwargs: dict[str, Any] = {}
            if (paid_launch_allowed and
                    paid_location_launch_budget is not None):
                launch_kwargs['paid_location_launch_budget'] = (
                    paid_location_launch_budget)
            if paid_authority_left is not None:
                launch_kwargs['paid_launch_allowed'] = paid_launch_allowed
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
            if replace_unknown_replica_ids:
                launch_kwargs['unknown_capacity_replacement'] = True
                launch_kwargs[
                    'logical_reconcile_fence_requires_exact_generation'] = True
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
            launched_capacity += launch_result.planned_capacity
            if (paid_authority_left is not None and
                    selected_card is not None and
                    launch_result.funding is _ReplicaLaunchFunding.PAID):
                paid_authority_left[selected_card] = max(
                    0,
                    paid_authority_left.get(selected_card, 0) -
                    launch_result.planned_capacity)

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
            service_spec=self._version_specs.get(self.latest_version)))

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
        if info.status_property.is_scale_down:
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
            self._remove_replica(info.replica_id, info.replica_record_id)
            logger.info(f'Replica {info.replica_id} removed from the '
                        f'replica table {removal_reason}.')

    # We don't need to add lock here since every caller of this function
    # will acquire the lock.
    def _terminate_replica(
            self,
            replica_id: int,
            sync_down_logs: bool,
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
        left_in_record = not (is_scale_down or purge)
        if left_in_record:
            assert sync_down_logs, (
                'For the replica left in the record, '
                'the logs should always be synced down. '
                'So that the user can see the logs to debug.')

        if replica_id in legacy_runtime.launch_thread_pool:
            info = serve_state.get_replica_info_from_id(self._service_name,
                                                        replica_id)
            assert info is not None
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
            self._persist_replica(replica_id, info)
            launch_thread = legacy_runtime.launch_thread_pool[replica_id]
            bound_ordinary_launch = bool(
                isinstance(launch_thread, _ReplicaLaunchThread) and
                launch_thread.bound_ordinary_launch)
            if launch_thread.is_alive():
                legacy_runtime.replica_to_launch_cancelled[replica_id] = True
                if bound_ordinary_launch:
                    # Deliver cancellation from the manager thread before
                    # joining. The waiter may still be inside the provider's
                    # shared guard; waiting for it before this direct
                    # row-locked cancel would make the provider and join
                    # depend cyclically on each other.
                    self._request_bound_ordinary_launch_cancel_for_teardown(
                        info)
                    launch_thread.join()
                else:
                    wait_deadline = (time.monotonic() +
                                     _WAIT_LAUNCH_THREAD_TIMEOUT_SECONDS)
                    timeout_reached = False
                    while True:
                        # Launch request id found. cancel it.
                        if replica_id in legacy_runtime.replica_to_request_id:
                            request_id = legacy_runtime.replica_to_request_id[
                                replica_id]
                            sdk.api_cancel(request_id)
                            break
                        if (replica_id not in
                                legacy_runtime.replica_to_launch_cancelled):
                            # Indicates that the cancellation was received.
                            break
                        if not launch_thread.is_alive():
                            # The launch may finish between the map checks.
                            break
                        remaining = wait_deadline - time.monotonic()
                        if remaining <= 0:
                            timeout_reached = True
                            break
                        time.sleep(min(0.1, remaining))
                    if timeout_reached:
                        logger.warning(
                            'Failed to cancel launch request for replica '
                            f'{replica_id} after '
                            f'{_WAIT_LAUNCH_THREAD_TIMEOUT_SECONDS} seconds. '
                            'Force waiting the launch thread to finish.')
                    else:
                        logger.info('Interrupted launch thread for replica '
                                    f'{replica_id} and deleted the cluster.')
                    launch_thread.join()
            else:
                logger.info(f'Launch thread for replica {replica_id} '
                            'already finished. Delete the cluster now.')
            if bound_ordinary_launch:
                if isinstance(launch_thread.exception,
                              _ReplicaLaunchOwnershipLostError):
                    raise launch_thread.exception
                if isinstance(launch_thread.exception,
                              _BoundOrdinaryLaunchUnresolvedError):
                    raise launch_thread.exception
                # Handles a controller crash after the local worker completed
                # but before its caller observed projection, and is a no-op
                # after the normal exact reducer cleared the pointer.
                fresh_info = serve_state.get_replica_info_from_id(
                    self._service_name, replica_id)
                if fresh_info is None:
                    raise _BoundOrdinaryLaunchUnresolvedError(
                        f'Bound teardown lost replica row {replica_id}.')
                self._settle_bound_ordinary_launch_for_teardown(fresh_info)
            legacy_runtime.launch_thread_pool.pop(replica_id)
            legacy_runtime.replica_to_request_id.pop(replica_id)
            legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)

        # Recovery may observe a durable SHUTTING_DOWN row before rebuilding a
        # local launch waiter. Resolve its exact association before any log or
        # provider operation; direct down/delete is forbidden while the API
        # execution generation is active or ambiguous.
        binding_authority = self._ordinary_launch_binding_authority
        if (binding_authority is not None and binding_authority.binding_mode
                == ordinary_launch_binding.BindingMode.BOUND):
            bound_teardown_info = serve_state.get_replica_info_from_id(
                self._service_name, replica_id)
            if bound_teardown_info is not None:
                self._settle_bound_ordinary_launch_for_teardown(
                    bound_teardown_info)

        if replica_id in legacy_runtime.down_thread_pool:
            logger.warning(f'Terminate thread for replica {replica_id} '
                           'already exists. Skipping.')
            return

        log_file_name = serve_utils.generate_replica_log_file_name(
            self._service_name, replica_id, self._resource_scope)

        def _download_and_stream_logs(
                info: ReplicaInfo,
                phase_admission: provider_phase.ProviderPhaseAdmission) -> None:
            launch_log_file_name = (
                serve_utils.generate_replica_launch_log_file_name(
                    self._service_name, replica_id, self._resource_scope))
            # Write launch log to replica log file. Tolerate a missing
            # launch log: a recovery re-drive re-enters this after a prior
            # pass already consumed it, and crashing here would strand the
            # replica in SHUTTING_DOWN (the recovery loop catches and moves
            # on without enqueueing the down thread). The replica log from
            # the prior pass is preserved in that case.
            if os.path.exists(launch_log_file_name):
                with open(log_file_name, 'w',
                          encoding='utf-8') as replica_log_file, open(
                              launch_log_file_name,
                              encoding='utf-8') as launch_file:
                    replica_log_file.write(launch_file.read())
                with contextlib.suppress(FileNotFoundError):
                    os.remove(launch_log_file_name)
            else:
                logger.info(f'Launch log for replica {replica_id} already '
                            'consumed (recovery re-drive); keeping the '
                            'existing replica log.')

            logger.info(f'Syncing down logs for replica {replica_id}...')
            backend = backends.CloudVmRayBackend()
            handle = global_user_state.get_handle_from_cluster_name(
                info.cluster_name)
            provider_fence = reserved_capacity.protocol_v2_provider_fence(
                info,
                handle,
                phase_admission=phase_admission,
                wait_for_initializer=False)
            if handle is None:
                logger.error(f'Cannot find cluster {info.cluster_name} for '
                             f'replica {replica_id} in the cluster table. '
                             'Skipping syncing down job logs.')
                return
            assert isinstance(handle, backends.CloudVmRayResourceHandle)
            replica_job_logs_dir = os.path.join(constants.SKY_LOGS_DIRECTORY,
                                                'replica_jobs')
            if self._is_pool:
                job_ids = ['1']
            elif info.system_recovery_disposition in (
                    system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
                    system_recovery_state.SystemRecoveryDisposition.CAPABLE):
                service_job_id = info.service_job_id
                if (isinstance(service_job_id, bool) or
                        not isinstance(service_job_id, int) or
                        service_job_id < 1):
                    logger.warning(
                        f'Replica {replica_id} has no exact recovery service '
                        'job ID; refusing a latest-job log lookup.')
                    return
                job_ids = [str(service_job_id)]
            else:
                job_ids = None
            with provider_fence:
                job_log_file_name = (
                    controller_utils.download_and_stream_job_log(
                        backend, handle, replica_job_logs_dir, job_ids))
            if job_log_file_name is not None:
                logger.info(f'\n== End of logs (Replica: {replica_id}) ==')
                with open(log_file_name, 'a',
                          encoding='utf-8') as replica_log_file, open(
                              os.path.expanduser(job_log_file_name),
                              encoding='utf-8') as job_file:
                    replica_log_file.write(job_file.read())
            else:
                with open(log_file_name, 'a',
                          encoding='utf-8') as replica_log_file:
                    replica_log_file.write(
                        f'Failed to sync down job logs from replica'
                        f' {replica_id}.\n')

        logger.info(f'Terminating replica {replica_id}...')
        teardown_snapshot = (
            serve_state.get_replica_info_with_resource_action_identity(
                self._service_name, replica_id))
        assert teardown_snapshot is not None
        info, resource_action_identity = teardown_snapshot
        info.status_property.is_scale_down = is_scale_down
        info.status_property.purged = purge
        info.status_property.wait_for_idle_before_termination = False
        # Revoke the exact process-local route before recovery terminalization,
        # log download, drain bookkeeping, or provider cleanup can block.  A
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

        if sync_down_logs:
            try:
                mode = (provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                        if cleanup_fence is None else
                        provider_phase.ProviderPhaseMode.V2_FENCED)
                with provider_phase.try_provider_phase(mode) as admission:
                    _download_and_stream_logs(info, admission)
            except (exceptions.ProviderPhaseBusyError,
                    exceptions.KubernetesPhysicalClusterFenceBusyError):
                # Log sync is best-effort. A locked refresher never waits for a
                # phase or physical initializer and cleanup remains independently
                # retryable under the down worker's blocking fence.
                logger.info(
                    f'Deferring log sync for replica {replica_id}: provider '
                    'authority is busy; continuing with fenced cleanup.')
            except exceptions.KubernetesPhysicalClusterIdentityError as e:
                # Log sync is credential delivery through
                # KubernetesCommandRunner, so an identity mismatch must never
                # be downgraded to the historical best-effort logging path:
                # skip the logs entirely. It must not abort the teardown
                # either. Returning here would leave a replica that may still
                # hold Pods running forever, which is the very leak this fence
                # exists to prevent. Cleanup below re-proves identity under its
                # own fence and records uncertainty only if it truly cannot act.
                logger.warning(
                    f'Skipping log sync for replica {replica_id}: the physical '
                    'Kubernetes identity could not be proved; continuing with '
                    f'fenced cleanup: {common_utils.format_exception(e)}')
            except Exception as e:  # pylint: disable=broad-except
                # Logs aid diagnosis, but cannot be a prerequisite for
                # stopping potentially billable infrastructure.
                logger.warning(
                    f'Failed to sync down logs for replica {replica_id}; '
                    'continuing with cleanup: '
                    f'{common_utils.format_exception(e)}')

        logger.info(f'preempted: {info.status_property.preempted}, '
                    f'replica_id: {replica_id}')
        # If the cluster does not exist, it means either the cluster never
        # exists (e.g., the cluster is scaled down before it gets a chance to
        # provision) or the cluster is preempted and cleaned up by the status
        # refresh. In this case, we skip spawning a new down thread to save
        # controller resources.
        if not global_user_state.cluster_with_name_exists(info.cluster_name):
            if cleanup_fence is not None:
                self._record_cleanup_uncertain(
                    info, 'the durable cluster record is absent, so cleanup '
                    'of partial resources cannot be proven')
                return
            self._handle_sky_down_finish(info, format_exc=None)
            return

        # Otherwise, schedule the thread to terminate the cluster. The
        # SHUTTING_DOWN status (sky_down_status set) is persisted FIRST:
        # the drain deadline and predicate are anchored to a moment at
        # which the controller provably stops advertising the replica to
        # the LB, and the deadline is anchored here (not at thread start)
        # so time queued in the admission pass counts toward the drain
        # budget instead of extending the terminate-slot hold.
        info.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        info.status_property.drain_cap_seconds = in_flight_drain_cap_seconds
        drain_started_at = _ensure_drain_started_at(
            info.status_property, in_flight_drain_cap_seconds)
        self._persist_replica(replica_id, info)
        drain_deadline: float | None = None
        drain_complete: Callable[[], bool] | None = None
        if (in_flight_drain_cap_seconds is not None and
                in_flight_drain_cap_seconds > 0):
            assert drain_started_at is not None
            drain_started = time.monotonic()
            drain_deadline = drain_started + _remaining_drain_seconds(
                drain_started_at, in_flight_drain_cap_seconds)
            try:
                # Live endpoint resolution (one DB/provider lookup); a
                # failure must never block the teardown -- it only costs
                # the early-exit (bounded sleep to the deadline instead).
                mode = (provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                        if cleanup_fence is None else
                        provider_phase.ProviderPhaseMode.V2_FENCED)
                with provider_phase.try_provider_phase(mode) as admission:
                    replica_url = self._resolve_probe_urls(
                        [info], phase_admission=admission).get(replica_id)
            except (exceptions.ProviderPhaseBusyError,
                    exceptions.KubernetesPhysicalClusterFenceBusyError):
                # URL evidence only shortens a bounded drain. Never wait while
                # holding self.lock and never turn contention into absence.
                replica_url = None
            except exceptions.KubernetesPhysicalClusterIdentityError:
                self._record_cleanup_uncertain(
                    info, 'the physical Kubernetes identity could not be '
                    'proved while resolving the drain endpoint')
                return
            except Exception as e:  # pylint: disable=broad-except
                logger.warning(
                    f'Failed to resolve the url of replica {replica_id} for '
                    f'the drain wait: {common_utils.format_exception(e)}')
                replica_url = None
            if not self._is_pool and replica_url is not None:
                # Pools have no LB (no gauge, nothing routed), and a
                # replica without a resolvable url never served: both keep
                # the plain bounded sleep semantics via a None predicate.
                drain_complete = _ReplicaDrainTracker(self, replica_url,
                                                      drain_started)
        t = thread_utils.SafeThread(
            target=terminate_cluster,
            args=(info.cluster_name, log_file_name,
                  replica_drain_delay_seconds),
            kwargs={
                'drain_deadline': drain_deadline,
                'drain_complete': drain_complete,
                'expected_cluster_record_uuid': expected_cluster_record_uuid,
                'cleanup_fence': cleanup_fence,
                'continue_guard': self._service_is_cleanup_authorized,
            },
        )
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
            down_failed = (info.status_property.sky_down_status ==
                           common_utils.ProcessStatus.FAILED)
            retry_pending = info.replica_id in retry_at_by_replica
            if (info.status != serve_state.ReplicaStatus.FAILED_CLEANUP and
                    not down_failed and not retry_pending):
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
                self._terminate_replica(
                    replica_id,
                    sync_down_logs=not (is_scale_down or purge),
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
                                sync_down_logs=False,
                                replica_drain_delay_seconds=0,
                                is_scale_down=True,
                                in_flight_drain_cap_seconds=0)

    def _register_wait_for_idle(
            self,
            info: ReplicaInfo,
            deadline: float | None = None,
            replica_url: Any = _REPLICA_URL_NOT_PROVIDED) -> None:
        """Register or conservatively retry a strict economic drain."""
        if info.replica_id in self._wait_for_idle_trackers:
            return
        drain_cap = info.status_property.drain_cap_seconds
        needs_persist = False
        if drain_cap is None:
            drain_cap = self._resolve_drain_cap_seconds(info.replica_id, info)
            info.status_property.drain_cap_seconds = drain_cap
            needs_persist = True
        prior_started_at = info.status_property.drain_started_at
        drain_started_at = _ensure_drain_started_at(info.status_property,
                                                    drain_cap)
        if drain_started_at != prior_started_at:
            needs_persist = True
        if needs_persist:
            self._persist_replica(info.replica_id, info)
        drain_started = time.monotonic()
        if deadline is None:
            remaining = (0.0 if drain_started_at is None else
                         _remaining_drain_seconds(drain_started_at, drain_cap))
            deadline = drain_started + remaining
        tracker = None
        if replica_url is _REPLICA_URL_NOT_PROVIDED:
            try:
                cleanup_fence = (
                    reserved_capacity.parse_protocol_v2_cleanup_fence(info))
                mode = (provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                        if cleanup_fence is None else
                        provider_phase.ProviderPhaseMode.V2_FENCED)
                deferred_ids: set[int] = set()
                with provider_phase.try_provider_phase(mode) as admission:
                    replica_url = self._resolve_probe_urls(
                        [info],
                        phase_admission=admission,
                        deferred_replica_ids=deferred_ids).get(info.replica_id)
                if info.replica_id in deferred_ids:
                    self._wait_for_idle_trackers[info.replica_id] = (None,
                                                                     deadline)
                    return
            except exceptions.ProviderPhaseBusyError:
                # Registration remains retryable; contention is not endpoint
                # absence or physical-identity evidence.
                self._wait_for_idle_trackers[info.replica_id] = (None, deadline)
                return
            except exceptions.KubernetesPhysicalClusterIdentityError as error:
                self._record_provider_identity_uncertain(
                    info, 'the physical Kubernetes identity could not be '
                    'proved while resolving the strict-drain endpoint: '
                    f'{common_utils.format_exception(error)}')
                self._wait_for_idle_trackers[info.replica_id] = (None, deadline)
                return
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Unable to resolve replica '
                               f'{info.replica_id} url for strict drain: '
                               f'{common_utils.format_exception(e)}')
                replica_url = None
            else:
                self._provider_identity_uncertain_replica_ids().discard(
                    info.replica_id)
        if replica_url is not None and not self._is_pool:
            assert isinstance(replica_url, str), replica_url
            tracker = _ReplicaDrainTracker(self, replica_url, drain_started)
        self._wait_for_idle_trackers[info.replica_id] = (tracker, deadline)

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
                                    sync_down_logs=False,
                                    replica_drain_delay_seconds=0,
                                    is_scale_down=True,
                                    in_flight_drain_cap_seconds=0)
            return
        info.status_property.is_scale_down = True
        info.status_property.purged = False
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

    def _logical_retirement_state(self,
                                  info: ReplicaInfo,
                                  *,
                                  require_victim_idle: bool = True) -> str:
        """Return safe, wait, or abort for one off-route logical backend.

        ``require_victim_idle=False`` is reserved for an outdated backend
        that has already consumed its full configured drain window.  It still
        requires a fresh current-epoch/current-target replacement-capacity
        proof; only the retiring backend's otherwise-unprovable idle state is
        omitted from that bounded rolling-update completion check.
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
        snapshot = self._logical_reconcile_snapshot
        target_state = _logical_target_state_components(self._logical_target)
        if (snapshot is None or snapshot.generation < selection_generation or
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
            tracker, _ = tracked
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
        status = info.status_property
        retirement_version = status.logical_retirement_version
        controller_epoch = status.logical_retirement_controller_epoch
        selection_generation = status.logical_retirement_generation
        selection_target = status.logical_retirement_target_capacity
        confirmed_generation = status.logical_retirement_confirmed_generation
        bounded_deadline = status.logical_retirement_bounded_deadline
        committed = status.logical_retirement_committed
        info_version = info.version
        generation_valid = (type(selection_generation) is int and
                            selection_generation >= 0)
        confirmation_valid = (
            confirmed_generation is None or
            (type(confirmed_generation) is int and generation_valid and
             confirmed_generation >= typing.cast(int, selection_generation)))
        strict_idle_wait = (status.wait_for_idle_before_termination is True and
                            confirmation_valid)
        bounded_precommit = (
            status.wait_for_idle_before_termination is False and
            bounded_deadline is True and type(confirmed_generation) is int and
            generation_valid and
            confirmed_generation >= typing.cast(int, selection_generation) and
            type(info_version) is int and type(retirement_version) is int and
            info_version <= retirement_version)
        return (
            status.sky_launch_status == common_utils.ProcessStatus.SUCCEEDED and
            status.is_scale_down is True and status.preempted is False and
            status.purged is False and
            (strict_idle_wait or bounded_precommit) and
            status.sky_down_status == common_utils.ProcessStatus.SCHEDULED and
            committed is False and type(info_version) is int and
            type(retirement_version) is int and
            info_version <= retirement_version and
            isinstance(controller_epoch, str) and bool(controller_epoch) and
            generation_valid and type(selection_target) is int and
            selection_target >= 0 and type(bounded_deadline) is bool)

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
        """Safely adopt and re-drive ambiguous pre-commit-bit retirements."""
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
                                            sync_down_logs=False,
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
            if not self._is_legacy_uncertain_logical_retirement(info):
                # Once classified as ambiguous, never reactivate the backend
                # merely because its durable state becomes malformed. Keep the
                # safe off-route state for operator inspection.
                continue

            with self._logical_state_lock:
                snapshot = self._logical_reconcile_snapshot
                target_state = _logical_target_state_components(
                    self._logical_target)
                if (snapshot is None or target_state is None or
                        not self._logical_snapshot_is_fresh(snapshot) or
                        snapshot.version != self.latest_version):
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
                )
                status.logical_retirement_version = self.latest_version
                status.logical_retirement_controller_epoch = (
                    self._logical_controller_epoch)
                status.logical_retirement_generation = snapshot.generation
                status.logical_retirement_target_capacity = current_target
                status.logical_retirement_confirmed_generation = (
                    snapshot.generation)
                status.logical_retirement_bounded_deadline = False
                if self._logical_retirement_state(
                        info, require_victim_idle=False) != 'safe':
                    (status.logical_retirement_version,
                     status.logical_retirement_controller_epoch,
                     status.logical_retirement_generation,
                     status.logical_retirement_target_capacity,
                     status.logical_retirement_confirmed_generation,
                     status.logical_retirement_bounded_deadline) = old_selection
                    continue

                status.logical_retirement_committed = True
                try:
                    self._persist_replica(replica_id, info)
                except Exception as e:  # pylint: disable=broad-except
                    # The write may have committed server-side. A fresh read on
                    # the next tick distinguishes committed from still-legacy
                    # state without ever advertising the backend again.
                    logger.warning(
                        f'Failed to confirm adoption of legacy logical '
                        f'retirement for replica {replica_id}: '
                        f'{common_utils.format_exception(e)}')
                    continue
                try:
                    self._terminate_replica(replica_id,
                                            sync_down_logs=False,
                                            replica_drain_delay_seconds=0,
                                            is_scale_down=True,
                                            in_flight_drain_cap_seconds=0)
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(f'Failed to schedule adopted legacy logical '
                                   f'retirement for replica {replica_id}: '
                                   f'{common_utils.format_exception(e)}')
                    continue
                uncertain_ids.discard(replica_id)

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
            if not self._is_recoverable_uncommitted_logical_retirement(info):
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
            snapshot = self._logical_reconcile_snapshot
            target_state = _logical_target_state_components(
                self._logical_target)
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
                old_selection = (
                    status.logical_retirement_version,
                    status.logical_retirement_controller_epoch,
                    status.logical_retirement_generation,
                    status.logical_retirement_target_capacity,
                    status.logical_retirement_confirmed_generation,
                    status.logical_retirement_bounded_deadline,
                    status.logical_retirement_committed,
                )
                status.logical_retirement_version = self.latest_version
                status.logical_retirement_controller_epoch = (
                    self._logical_controller_epoch)
                status.logical_retirement_generation = snapshot.generation
                status.logical_retirement_target_capacity = current_target
                # Adoption only refreshes the selection fence. Idle proof and
                # the irreversible teardown commit remain in the existing
                # _finish_logical_retirement path.
                status.logical_retirement_confirmed_generation = (
                    snapshot.generation if bounded_precommit else None)
                status.logical_retirement_bounded_deadline = bounded_precommit
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

    def _finish_logical_retirement(self,
                                   replica_id: int,
                                   info: ReplicaInfo,
                                   *,
                                   require_victim_idle: bool = True) -> None:
        """Recheck and schedule one fenced logical retirement atomically."""
        with self._logical_state_lock:
            retirement_state = self._logical_retirement_state(
                info, require_victim_idle=require_victim_idle)
            if retirement_state == 'wait':
                return
            if retirement_state == 'abort':
                self._abort_logical_retirement(
                    info, 'the current target or coverage fence changed')
                return
            snapshot = self._logical_reconcile_snapshot
            assert snapshot is not None
            info.status_property.logical_retirement_confirmed_generation = (
                snapshot.generation)
            info.status_property.logical_retirement_bounded_deadline = (
                not require_victim_idle)
            self._persist_replica(replica_id, info)
            # The state lock prevents a later sync from invalidating the
            # confirmation between this final proof and shutdown scheduling.
            if self._logical_retirement_state(
                    info, require_victim_idle=require_victim_idle) != 'safe':
                return
            # _terminate_replica atomically clears the durable idle-wait bit
            # with its SCHEDULED down state before installing the worker. Keep
            # both the bit and tracker until that succeeds, so a transient DB
            # failure is retried on the next refresh instead of stranding an
            # off-route SHUTTING_DOWN row until controller restart.
            try:
                self._terminate_replica(replica_id,
                                        sync_down_logs=False,
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

        cluster_names = list(
            dict.fromkeys(info.cluster_name for info in tracked_infos.values()))
        cluster_status_fields = global_user_state.get_cluster_status_fields(
            cluster_names)
        retry_url_infos = [
            tracked_infos[replica_id]
            for replica_id, (tracker, _) in tracker_items
            if (tracker is None and replica_id in tracked_infos and
                tracked_infos[replica_id].cluster_name in cluster_status_fields)
        ]
        retry_urls: dict[int, str | None] = {}
        deferred_url_ids: set[int] = set()
        fenced_retry_infos: list[ReplicaInfo] = []
        ordinary_retry_infos: list[ReplicaInfo] = []
        for info in retry_url_infos:
            try:
                cleanup_fence = (
                    reserved_capacity.parse_protocol_v2_cleanup_fence(info))
            except exceptions.KubernetesPhysicalClusterIdentityError as error:
                self._record_provider_identity_uncertain(
                    info, 'strict-drain endpoint identity is malformed: '
                    f'{common_utils.format_exception(error)}')
                deferred_url_ids.add(info.replica_id)
                continue
            if cleanup_fence is None:
                ordinary_retry_infos.append(info)
            else:
                fenced_retry_infos.append(info)

        def _try_resolve_urls(partition: list[ReplicaInfo],
                              mode: provider_phase.ProviderPhaseMode) -> None:
            if not partition:
                return
            try:
                with provider_phase.try_provider_phase(mode) as admission:
                    retry_urls.update(
                        self._resolve_probe_urls(
                            partition,
                            phase_admission=admission,
                            deferred_replica_ids=deferred_url_ids))
            except exceptions.ProviderPhaseBusyError:
                deferred_url_ids.update(info.replica_id for info in partition)

        # Resolve and reduce the complete fenced subset before attempting any
        # ambient work. A raw tracker order of ordinary then v2 must not let an
        # ordinary URL lookup or teardown run before a later fenced owner.
        _try_resolve_urls(fenced_retry_infos,
                          provider_phase.ProviderPhaseMode.V2_FENCED)
        tracker_items.sort(key=lambda item: (_provider_cleanup_phase_order(
            tracked_infos[item[0]]) if item[0] in tracked_infos else 0))
        ordinary_urls_resolved = False
        for replica_id, tracked in tracker_items:
            tracker, deadline = tracked
            info = tracked_infos.get(replica_id)
            if info is None:
                continue
            if (_provider_cleanup_phase_order(info) == 1 and
                    not ordinary_urls_resolved):
                ordinary_urls_resolved = True
                _try_resolve_urls(
                    ordinary_retry_infos,
                    provider_phase.ProviderPhaseMode.AMBIENT_LEGACY)
            if info.cluster_name not in cluster_status_fields:
                drained = True
            else:
                if tracker is None:
                    if replica_id in deferred_url_ids:
                        # A busy phase/initializer contributes no endpoint or
                        # drain evidence and leaves the tracker untouched.
                        continue
                    # Endpoint discovery can fail transiently during recovery.
                    self._wait_for_idle_trackers.pop(replica_id, None)
                    self._register_wait_for_idle(
                        info,
                        deadline=deadline,
                        replica_url=retry_urls.get(replica_id))
                    retried = self._wait_for_idle_trackers.get(replica_id)
                    if retried is None:
                        # A physical-identity failure is durably retained by
                        # registration; do not turn the same pass into cleanup
                        # admission or another provider lookup.
                        continue
                    tracker = retried[0]
                    if (replica_id
                            in self._provider_identity_uncertain_replica_ids()):
                        continue
                drained = tracker is not None and tracker()
            deadline_expired = time.monotonic() >= deadline
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
                    retirement_state = self._logical_retirement_state(info)
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
                                info, require_victim_idle=False)
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
                            replica_id, info, require_victim_idle=False)
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
                self._finish_logical_retirement(replica_id, info)
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
                                    sync_down_logs=False,
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
            snapshot = self._logical_reconcile_snapshot
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
                                sync_down_logs=False,
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
            if not self._logical_target_fence_holds(
                    version, reconcile_generation, target_capacity,
                    target_capacity_by_accelerator, accelerator_shapes):
                logger.info(
                    'Discarding stale logical scale-down batch for version '
                    f'{version}, generation {reconcile_generation}, target '
                    f'{target_capacity} with {len(replica_ids)} victim(s).')
                return
            snapshot = self._logical_reconcile_snapshot
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
            ready_by_accelerator = {card: 0 for card, _ in accelerator_shapes}
            committed_by_accelerator = {
                card: 0 for card, _ in accelerator_shapes
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
                if contributes and accelerator_shapes:
                    card = self._logical_replica_accelerator(
                        candidate,
                        accelerator_shapes,
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
                if (accelerator_shapes and card is None and
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
                    if (committed_after < target_capacity or
                            not self._logical_card_capacity_covers(
                                card_committed_after,
                                target_capacity_by_accelerator)):
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
                    if (ready_after < target_capacity or
                            not self._logical_card_capacity_covers(
                                card_ready_after,
                                target_capacity_by_accelerator)):
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

            # Resolve the accepted victim wave as one provider batch. Passing
            # the pre-resolved URL into drain registration prevents a second
            # per-victim UID proof. A failed group remains durably off-route
            # and retries identity; it never commits teardown through a
            # replacement alias.
            ordinary_immediate_infos: list[ReplicaInfo] = []
            identity_fenced_immediate_infos: list[ReplicaInfo] = []
            ordinary_drain_infos: list[ReplicaInfo] = []
            identity_fenced_drain_infos: list[ReplicaInfo] = []
            for info in immediate_teardown_infos:
                destination = (identity_fenced_immediate_infos
                               if _provider_cleanup_phase_order(info) == 0 else
                               ordinary_immediate_infos)
                destination.append(info)
            for info in logical_drain_infos:
                destination = (identity_fenced_drain_infos
                               if _provider_cleanup_phase_order(info) == 0 else
                               ordinary_drain_infos)
                destination.append(info)

            # Every provider-bearing v2 action in the accepted wave completes
            # before the first ambient action, including never-served victims
            # that skip endpoint draining altogether.
            for info in identity_fenced_immediate_infos:
                self._terminate_replica(info.replica_id,
                                        sync_down_logs=False,
                                        replica_drain_delay_seconds=0,
                                        is_scale_down=True,
                                        in_flight_drain_cap_seconds=0)
            if identity_fenced_drain_infos:
                deferred_drain_ids: set[int] = set()
                try:
                    with provider_phase.try_provider_phase(
                            provider_phase.ProviderPhaseMode.V2_FENCED
                    ) as admission:
                        logical_drain_urls = self._resolve_probe_urls(
                            identity_fenced_drain_infos,
                            phase_admission=admission,
                            deferred_replica_ids=deferred_drain_ids)
                except exceptions.ProviderPhaseBusyError:
                    logical_drain_urls = {}
                    deferred_drain_ids.update(
                        info.replica_id for info in identity_fenced_drain_infos)
                for info in identity_fenced_drain_infos:
                    if info.replica_id in deferred_drain_ids:
                        continue
                    self._defer_scale_down_until_idle(
                        info.replica_id,
                        logical_retirement=(version, reconcile_generation,
                                            target_capacity),
                        replica_info=info,
                        replica_url=logical_drain_urls.get(info.replica_id))

            for info in ordinary_immediate_infos:
                self._terminate_replica(info.replica_id,
                                        sync_down_logs=False,
                                        replica_drain_delay_seconds=0,
                                        is_scale_down=True,
                                        in_flight_drain_cap_seconds=0)
            # Genuine ordinary rows may use ambient authority only after every
            # v2 physical owner above has retired. Keep the endpoint wave in
            # one try-admitted ambient phase so a waiting v2 caller cannot
            # interleave between individual victims.
            if ordinary_drain_infos:
                try:
                    with provider_phase.try_provider_phase(
                            provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                    ) as admission:
                        ordinary_drain_urls_by_id = self._resolve_probe_urls(
                            ordinary_drain_infos, phase_admission=admission)
                except exceptions.ProviderPhaseBusyError:
                    ordinary_drain_urls_by_id = None
                if ordinary_drain_urls_by_id is not None:
                    for info in ordinary_drain_infos:
                        self._defer_scale_down_until_idle(
                            info.replica_id,
                            logical_retirement=(version, reconcile_generation,
                                                target_capacity),
                            replica_info=info,
                            replica_url=ordinary_drain_urls_by_id.get(
                                info.replica_id))

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
                            legacy_runtime.replica_to_launch_cancelled,
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
                f'{version}, generation {reconcile_generation}, target '
                f'{target_capacity}: requested={len(replica_ids)}, '
                f'accepted={accepted}, skipped={len(replica_ids) - accepted}.')

    # We don't need to add lock here since every caller of this function
    # will acquire the lock.
    # Thread-pool bound for the per-probe-round parallel cloud pre-filter
    # over failed-probe spot replicas (see _cloud_instance_looks_alive).
    _PREEMPTION_PREFILTER_PARALLELISM = 16
    _PROBE_ROUND_MAX_PARALLELISM = 256

    def _cloud_instance_looks_alive(
        self,
        info: ReplicaInfo,
        *,
        phase_admission: provider_phase.ProviderPhaseAdmission | None = None,
    ) -> bool | None:
        """Whether the cloud still reports this replica's instance(s) as UP.

        Cloud-API-only (one provider call, no SSH probe, no status lock, no
        DB writes): this is the cheap pre-filter that decides whether a
        failed readiness probe warrants the full `_handle_preemption` path
        (which does a forced, serial, lock-holding cluster refresh). During
        a fleet cold start EVERY not-yet-listening replica fails its probe
        by design; the pre-filter confirms their instances are running and
        skips the expensive path for them.

        Alive requires EVERY launched node to be reported UP, mirroring the
        full refresh's partial-cluster semantics ("some nodes UP" is
        abnormal: the cluster is partially preempted or terminated). Any
        shortfall — fewer instances than launched_nodes, or any non-UP
        instance — routes to the full path, which stays the authority on
        classification.

        Errors count as alive: a transient provider/API error must not
        stampede a whole cold-starting fleet into forced refreshes — a
        genuinely dead instance keeps failing its probe and is re-checked
        next round. A missing handle counts as NOT alive so the full path
        (which logs and handles that case) runs.
        """
        try:
            handle = global_user_state.get_handle_from_cluster_name(
                info.cluster_name)
            provider_fence = reserved_capacity.protocol_v2_provider_fence(
                info,
                handle,
                phase_admission=phase_admission,
                wait_for_initializer=phase_admission is None)
            if handle is None:
                return False
            assert isinstance(handle, backends.CloudVmRayResourceHandle)
            with provider_fence:
                statuses = backend_utils.query_cluster_instance_statuses(handle)
            if len(statuses) < handle.launched_nodes:
                return False
            return all(status == status_lib.ClusterStatus.UP
                       for status, _ in statuses.values())
        except exceptions.KubernetesPhysicalClusterIdentityError as error:
            logger.error(
                f'Preemption pre-filter has unknown provider identity for '
                f'replica {info.replica_id} ({info.cluster_name}): '
                f'{common_utils.format_exception(error)}')
            return None
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f'Preemption pre-filter failed for replica '
                         f'{info.replica_id} ({info.cluster_name}); treating '
                         f'as alive: {common_utils.format_exception(e)}')
            return True

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

    def _handle_preemption(self, info: ReplicaInfo) -> bool:
        """Handle an infrastructure interruption after a replica error.

        Returns:
            bool: Whether the replica's capacity was interrupted.
        """
        if (self._update_recovery_required or
                not self._is_interruptible_replica(info)):
            return False

        # Get cluster handle first for zone information. The following
        # backend_utils.refresh_cluster_status_handle might delete the
        # cluster record from the cluster table.
        handle = global_user_state.get_handle_from_cluster_name(
            info.cluster_name)
        if self._update_recovery_required:
            return False
        provider_fence = reserved_capacity.protocol_v2_provider_fence(
            info, handle)
        if handle is None:
            # A missing global-state row after a failed probe is conclusive for
            # a successfully launched interruptible replica: forced status
            # refresh removes terminated clusters before this handler can run.
            logger.warning(f'Cannot find cluster {info.cluster_name} for '
                           f'replica {info.replica_id} in the cluster table; '
                           'treating it as interrupted.')
            cluster_status = None
        else:
            assert isinstance(handle, backends.CloudVmRayResourceHandle)
            # Pull the actual cluster status from the provider to distinguish
            # an infrastructure interruption from an application readiness
            # failure.
            with provider_fence:
                cluster_status, _ = (
                    backend_utils.refresh_cluster_status_handle(
                        info.cluster_name,
                        force_refresh_statuses=set(status_lib.ClusterStatus)))

            if self._update_recovery_required:
                return False

            if cluster_status in (status_lib.ClusterStatus.UP,
                                  status_lib.ClusterStatus.AUTOSTOPPING):
                return False
        # The cluster is partially or fully interrupted. It can be down, INIT
        # or STOPPED, based on the provider's interruption behavior.
        if self._update_recovery_required:
            return False
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
            self._persist_spot_placement_state_if_dirty()
        self._persist_replica(info.replica_id, info)
        self._terminate_replica(info.replica_id,
                                sync_down_logs=False,
                                replica_drain_delay_seconds=0,
                                is_scale_down=True)
        return True

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
            target_fence = self._logical_target
            target_state = _logical_target_state_components(target_fence)
            if (target_state is None or target_fence is None or
                    len(target_fence) != 5):
                return _LogicalPendingLaunchAdmission(
                    applicable=True,
                    target_fence=None,
                    authorized_ids=frozenset(),
                    reason='target-missing-or-malformed',
                    details=f'target={target_fence!r}')
            if not self._logical_reconcile_fence_holds(target_fence):
                snapshot = self._logical_reconcile_snapshot
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
            if (self._logical_target != target_fence or
                    not self._logical_reconcile_fence_holds(target_fence)):
                return _LogicalPendingLaunchAdmission(
                    applicable=True,
                    target_fence=None,
                    authorized_ids=frozenset(),
                    reason='target-changed-during-replica-read',
                    details=(f'previous_target={target_fence!r}, '
                             f'current_target={self._logical_target!r}'))
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
        self._superseded_prune_pending = False
        latest_version = self.latest_version
        for info in serve_state.get_replica_infos(self._service_name):
            if info.version == latest_version:
                continue
            if (info.status_property.sky_down_status
                    != common_utils.ProcessStatus.SUCCEEDED):
                continue
            if (info.status not in self._PRUNABLE_SUPERSEDED_STATUSES):
                continue
            self._remove_replica(info.replica_id, info.replica_record_id)
            logger.info(
                f'Replica {info.replica_id} removed from the replica table '
                f'for version outdated (version {info.version} superseded by '
                f'{latest_version}).')

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
        # failed launch, an inline log sync that may SSH into the replica.
        # None of that needs the lock -- holding it here would stall every
        # other service's admission pass (and `sky serve up`) for the whole
        # walk. Only the admission pass below needs the lock.
        launch_to_admit: list[tuple[int, thread_utils.SafeThread,
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
        stale_finished_launches = [
            replica_id for replica_id, _ in finished_launches
            if replica_id not in launch_infos
        ]
        for replica_id in stale_finished_launches:
            logger.warning(
                f'Discarding completed launch worker for replica '
                f'{replica_id}: its durable replica row no longer exists.')
            legacy_runtime.launch_thread_pool.pop(replica_id)
            legacy_runtime.replica_to_request_id.pop(replica_id)
            legacy_runtime.replica_to_launch_cancelled.pop(replica_id)
            legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)
        if stale_finished_launches:
            stale_replica_ids = set(stale_finished_launches)
            finished_launches = [(replica_id, t)
                                 for replica_id, t in finished_launches
                                 if replica_id not in stale_replica_ids]
        finished_spot_locations: dict[int, spot_placer.Location] = {}
        if self._spot_placer is not None:
            for replica_id, t in finished_launches:
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
            info = launch_infos.get(replica_id)
            assert info is not None, replica_id
            bound_ordinary_launch = bool(
                isinstance(t, _ReplicaLaunchThread) and t.bound_ordinary_launch)
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
                    legacy_runtime.replica_to_launch_cancelled.pop(replica_id)
                    legacy_runtime.replica_to_logical_launch_fence.pop(
                        replica_id)
                    continue
                if unresolved:
                    if (remaining is not None and
                            _bound_projection_classification(remaining)
                            == 'AMBIGUOUS'):
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
                    legacy_runtime.replica_to_launch_cancelled.pop(replica_id)
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
                                sync_down_logs=not (is_scale_down or purge),
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
                self._scale_reconciliation_event.set()
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
                self._scale_reconciliation_event.set()
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
            legacy_runtime.replica_to_launch_cancelled.pop(replica_id)
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
                'Queued generation+1 bound ordinary launch for replica %s '
                'after exact pre-effect settlement.', replica_id)

        # Retire v2 failures before any ordinary log/drain provider work. The
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
                                        sync_down_logs=False,
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
                                        sync_down_logs=True,
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
                    sync_down_logs=not superseded,
                    replica_drain_delay_seconds=0,
                    is_scale_down=superseded,
                    in_flight_drain_cap_seconds=(0 if superseded else None))
            else:
                legacy_runtime.launch_thread_pool.pop(replica_id)
                legacy_runtime.replica_to_request_id.pop(replica_id)
                legacy_runtime.replica_to_launch_cancelled.pop(replica_id)
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
                    legacy_runtime.replica_to_launch_cancelled.pop(replica_id)
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
                                    logical_target_fence):
                                continue
                            logger.info(
                                f'Superseding queued logical launch for '
                                f'replica {replica_id}: current exact-card '
                                'capacity already covers its target budget.')
                            self._terminate_replica(
                                replica_id,
                                sync_down_logs=False,
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
                        legacy_runtime.replica_to_launch_cancelled.pop(
                            replica_id)
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
        finished_downs = [(replica_id, t)
                          for replica_id, t in down_thread_pool_snapshot
                          if not t.is_alive()]
        down_infos = serve_state.get_replica_infos_from_ids(
            self._service_name,
            [replica_id for replica_id, _ in finished_downs])
        for replica_id, t in finished_downs:
            info = down_infos.get(replica_id)
            assert info is not None, replica_id
            if (info.status_property.sky_down_status ==
                    common_utils.ProcessStatus.SCHEDULED):
                # sky.down not started yet; admitted below under the
                # resources lock.
                down_to_admit.append((replica_id, t, info))
                continue
            logger.info(f'Terminate thread for replica {replica_id} finished.')
            self._handle_sky_down_finish(info, format_exc=t.format_exc)
            # Pop only after the durable completion update succeeds.  If a DB
            # write fails, retaining the finished worker makes the next tick
            # retry the handler instead of stranding a RUNNING down status.
            legacy_runtime.down_thread_pool.pop(replica_id)

        # Admission pass: read the launch budget ONCE per tick, under the
        # cross-process resources lock held across ALL admission decisions
        # (launch and down -- both draw on the same weighted budget). Reading
        # it outside the lock would let a concurrent service manager admit
        # against the same stale count and oversubscribe the launch cap.
        # Tracking the delta locally avoids the O(K*N) per-replica re-scan
        # this read used to incur -- can_provision/can_terminate otherwise
        # unpickle the ENTIRE replica table per launching/terminating replica
        # (measured ~1.7s/tick at N=2000, K=140; grows with fleet size). When
        # there is nothing to admit, skip the lock and the scan entirely.
        if launch_to_admit or down_to_admit:
            with filelock.FileLock(controller_utils.get_resources_lock_path()):
                in_flight = controller_utils.in_flight_launch_count()
                for replica_id, t, info in launch_to_admit:
                    if not controller_utils.can_provision(self._is_pool,
                                                          in_flight=in_flight):
                        continue
                    logical_fence = self._replica_to_logical_launch_fence.get(
                        replica_id)
                    if logical_fence is not None:
                        with self._logical_state_lock:
                            if not self._logical_reconcile_fence_holds(
                                    logical_fence):
                                continue
                    t.start()
                    # This replica is now provisioning; reflect it locally
                    # instead of re-scanning the DB on the next replica.
                    in_flight += 1
                    if (isinstance(t, _ReplicaLaunchThread) and
                            t.bound_ordinary_launch):
                        # The child may admit and project before this parent
                        # bookkeeping write.  Update only the latest locked
                        # SCHEDULED row while its scalar pointer is still
                        # active; a completed projection wins permanently.
                        serve_state.mark_bound_replica_launch_running_if_active(
                            self._service_name, replica_id,
                            info.replica_record_id)
                    else:
                        info.status_property.sky_launch_status = (
                            common_utils.ProcessStatus.RUNNING)
                        self._persist_replica(replica_id, info)
                for replica_id, t, info in down_to_admit:
                    if concurrent_downs >= _MAX_CONCURRENT_DOWNS_PER_SERVICE:
                        break
                    logical_retirement = (
                        info.status_property.logical_retirement_version
                        is not None)
                    logical_state_guard = (self._logical_state_lock
                                           if logical_retirement else
                                           contextlib.nullcontext())
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
                        if not controller_utils.can_terminate(
                                self._is_pool, in_flight=in_flight):
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
                            snapshot = self._logical_reconcile_snapshot
                            assert snapshot is not None
                            info.status_property.logical_retirement_confirmed_generation = (
                                snapshot.generation)
                            # The RUNNING write below is the durable admission
                            # boundary. After it, a crash may happen before
                            # t.start(), so recovery must finish cleanup.
                            # Before it, a budget-delayed SCHEDULED retirement
                            # remains safe to abort and reselect.
                            info.status_property.logical_retirement_committed = (
                                True)
                        info.status_property.sky_down_status = (
                            common_utils.ProcessStatus.RUNNING)
                        try:
                            self._persist_replica(replica_id, info)
                        except Exception:
                            # The database may have committed RUNNING even if
                            # the client observed an error. Remove the never-
                            # started worker and immediately re-read/re-drive:
                            # committed readback detaches into ordinary cleanup;
                            # uncommitted readback recreates the fenced worker.
                            legacy_runtime.down_thread_pool.pop(replica_id)
                            status = info.status_property
                            is_scale_down = (status.is_scale_down or
                                             status.preempted)
                            purge = status.purged
                            try:
                                self._terminate_replica(
                                    replica_id,
                                    sync_down_logs=not (is_scale_down or purge),
                                    replica_drain_delay_seconds=0,
                                    is_scale_down=is_scale_down,
                                    purge=purge,
                                    in_flight_drain_cap_seconds=(
                                        status.drain_cap_seconds))
                            except Exception as redrive_error:  # pylint: disable=broad-except
                                logger.warning(
                                    f'Failed to re-drive replica {replica_id} '
                                    'after ambiguous down-admission write: '
                                    f'{common_utils.format_exception(redrive_error)}'
                                )
                                self._schedule_failed_cleanup_retry(replica_id)
                            raise
                        try:
                            t.start()
                        except Exception as e:  # pylint: disable=broad-except
                            # RUNNING and (for logical retirement) the
                            # commitment bit were persisted before start, so
                            # a crash here is safely recoverable. In-process,
                            # convert failed admission into the ordinary
                            # retry loop with a fresh worker. Keep SCHEDULED:
                            # unlike a provider cleanup failure, Thread.start()
                            # never entered the drain wait, so the retry must
                            # consume the remaining original deadline.
                            logger.error(
                                f'Failed to start terminate worker for '
                                f'replica {replica_id}: '
                                f'{common_utils.format_exception(e)}')
                            legacy_runtime.down_thread_pool.pop(replica_id)
                            self._wait_for_idle_trackers.pop(replica_id, None)
                            info.status_property.sky_down_status = (
                                common_utils.ProcessStatus.SCHEDULED)
                            try:
                                self._persist_replica(replica_id, info)
                            finally:
                                self._schedule_failed_cleanup_retry(replica_id)
                            continue
                        self._wait_for_idle_trackers.pop(replica_id, None)
                        concurrent_downs += 1
                        # This replica is now terminating; reflect it locally
                        # (weighted like in_flight_launch_count) instead of
                        # re-scanning the DB on the next replica.
                        in_flight += 1.0 / controller_utils.SERVE_LAUNCH_RATIO

        # Reconcile provider cleanup, but retain immutable version metadata.
        # Historical specs power admin comparison and rollback, while full
        # service teardown remains responsible for deleting all version rows.
        replica_infos = serve_state.get_replica_infos(self._service_name)
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
                self._refresh_thread_pool()
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
    ) -> bool:
        """Reduce one exact-job observation and schedule legacy teardown."""
        if self._update_recovery_required:
            return False
        outcome: dict[str, Any] = {
            'off_route': True,
            'clear_probe': False,
            'teardown': False,
            'valid_present': False,
            'events': set(),
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
            })
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
            outcome['teardown'] = True
            updated = serve_state.get_replica_info_from_id(
                self._service_name, snapshot.replica_id)
        if updated is None:
            return True
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
        if status_changed:
            self._persist_replica(updated.replica_id, updated)
        if outcome['teardown']:
            logger.warning(
                f'System recovery for replica {updated.replica_id} cannot '
                'continue safely; scheduling legacy teardown.')
            self._terminate_replica(updated.replica_id,
                                    sync_down_logs=True,
                                    replica_drain_delay_seconds=0)
            return True
        return False

    def _fetch_job_status(self) -> None:
        """Fetch the service job status of all replicas.

        This function will monitor the job status of all replicas
        to make sure the service is running correctly. If any of the
        replicas failed, it will terminate the replica.

        It is still needed even if we already keep probing the replicas,
        since the replica job might launch the API server in the background
        (using &), and the readiness probe will not detect the worker failure.

        NOTE: this does NOT hold ``self.lock`` across the per-replica
        ``get_job_status`` SSH walk. An unreachable (e.g. preempted spot)
        replica's SSH connect hangs at the kernel TCP timeout (tens of seconds
        to minutes); holding the lock across the walk would block the
        refresher / prober / scaler -- which all take ``self.lock`` -- for the
        whole walk, stalling autoscaling exactly when the fleet is churning.
        The lock is re-acquired only on the failure-handling paths (preemption
        and user-code failure); those paths may still run a cloud status
        refresh or a log sync while holding it.

        The SSH fetches run in a thread pool (like ``_probe_all_replicas``):
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
        invalid_recovery_ids: dict[
            provider_phase.ProviderPhaseMode, list[int]] = {
                provider_phase.ProviderPhaseMode.V2_FENCED: [],
                provider_phase.ProviderPhaseMode.AMBIENT_LEGACY: [],
            }
        identity_uncertainties: list[tuple[int, str]] = []
        fence_representatives: dict[tuple[str, str], tuple[ReplicaInfo,
                                                           Any]] = {}
        fence_group_replica_ids: dict[tuple[str, str], set[int]] = {}
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
                    (info.replica_id, common_utils.format_exception(error)))
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
                    invalid_recovery_ids[mode].append(info.replica_id)
                continue
            if cleanup_fence is not None:
                # Register every valid-handle v2 row before validating its job
                # association. Invalid recovery rows may still sync logs and
                # schedule teardown, so their reduction needs the same batch
                # physical owner as a normal status result.
                key = (cleanup_fence.kubernetes_context,
                       cleanup_fence.physical_cluster_uid)
                fence_representatives.setdefault(key, (info, handle))
                fence_group_replica_ids.setdefault(key,
                                                   set()).add(info.replica_id)
            with_recovery = (
                not self._is_pool and info.system_recovery_disposition
                in (system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
                    system_recovery_state.SystemRecoveryDisposition.CAPABLE))
            if with_recovery:
                service_job_id = info.service_job_id
                if (isinstance(service_job_id, bool) or
                        not isinstance(service_job_id, int) or
                        service_job_id < 1):
                    mode = (provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                            if cleanup_fence is None else
                            provider_phase.ProviderPhaseMode.V2_FENCED)
                    invalid_recovery_ids[mode].append(info.replica_id)
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
        for replica_id, message in identity_uncertainties:
            with self.lock:
                if self._update_recovery_required:
                    return
                fresh = serve_state.get_replica_info_from_id(
                    self._service_name, replica_id)
                if (fresh is None or
                        not fresh.status_property.should_track_service_status()
                   ):
                    continue
                self._record_provider_identity_uncertain(
                    fresh, f'job-status lookup was fenced off: {message}')

        def _terminate_invalid_recovery_rows(replica_ids: list[int]) -> None:
            for replica_id in replica_ids:
                with self.lock:
                    if self._update_recovery_required:
                        return
                    fresh = serve_state.get_replica_info_from_id(
                        self._service_name, replica_id)
                    if (fresh is None or not fresh.status_property.
                            should_track_service_status() or
                            fresh.system_recovery_disposition
                            not in (system_recovery_state.
                                    SystemRecoveryDisposition.CANDIDATE,
                                    system_recovery_state.
                                    SystemRecoveryDisposition.CAPABLE)):
                        continue
                    logger.warning(
                        f'Recovery candidate/capable replica {replica_id} lacks '
                        'its exact cluster handle or service job association.')
                    self._terminate_replica(replica_id,
                                            sync_down_logs=True,
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
            *,
            provider_error_phase_mode: (provider_phase.ProviderPhaseMode |
                                        None) = None,
        ) -> None:
            if not fetches or self._manager_daemon_should_stop():
                return
            # The fetches are pure I/O and explicitly join the caller's phase.
            # Result classification stays inside that phase as it may perform
            # preemption refresh, log sync, or teardown provider work.
            num_fetch_threads = min(len(fetches),
                                    self._PROBE_ROUND_MAX_PARALLELISM)
            with mp_pool.ThreadPool(num_fetch_threads) as pool:
                fetch_results = [
                    (info,
                     pool.apply_async(_get_job_status,
                                      (info, handle, job_ids, with_recovery,
                                       phase_admission)))
                    for info, handle, job_ids, with_recovery in fetches
                ]
                if provider_error_phase_mode is None:
                    self._handle_job_status_results(fetch_results)
                else:
                    self._handle_job_status_results(
                        fetch_results,
                        provider_error_phase_mode=provider_error_phase_mode)

        fenced_invalid_ids = invalid_recovery_ids[
            provider_phase.ProviderPhaseMode.V2_FENCED]
        if fenced_fetches or fenced_invalid_ids:
            # Blocking admission is outside self.lock, so one unreachable SSH
            # worker does not block probe/refresher admission on the manager
            # mutex. Every v2 result is fully reduced before owners retire.
            with provider_phase.provider_phase(provider_phase.ProviderPhaseMode.
                                               V2_FENCED) as phase_admission:
                with reserved_capacity.protocol_v2_provider_batch_fences(
                        fence_representatives,
                        phase_admission=phase_admission) as fence_failures:
                    failed_replica_ids: set[int] = set()
                    for key, error in fence_failures.items():
                        if not isinstance(
                                error, exceptions.
                                KubernetesPhysicalClusterIdentityError):
                            raise error
                        failed_replica_ids.update(fence_group_replica_ids[key])
                        for replica_id in fence_group_replica_ids[key]:
                            with self.lock:
                                fresh = serve_state.get_replica_info_from_id(
                                    self._service_name, replica_id)
                                if (fresh is None or not fresh.status_property.
                                        should_track_service_status()):
                                    continue
                                self._record_provider_identity_uncertain(
                                    fresh,
                                    'job-status batch identity was fenced off: '
                                    f'{common_utils.format_exception(error)}')
                    _terminate_invalid_recovery_rows([
                        replica_id for replica_id in fenced_invalid_ids
                        if replica_id not in failed_replica_ids
                    ])
                    admitted_fetches = [
                        item for item in fenced_fetches
                        if item[0].replica_id not in failed_replica_ids
                    ]
                    _run_fetches(admitted_fetches, phase_admission)

        ordinary_invalid_ids = invalid_recovery_ids[
            provider_phase.ProviderPhaseMode.AMBIENT_LEGACY]
        if ordinary_fetches or ordinary_invalid_ids:
            # Genuine ordinary rows run only after all physical owners retire.
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
            ) as phase_admission:
                _terminate_invalid_recovery_rows(ordinary_invalid_ids)
                _run_fetches(ordinary_fetches, phase_admission)

        # Healthy exact non-Kubernetes SSH can be arbitrarily slow without
        # owning Kubernetes authority. If it fails, result reduction takes a
        # fresh ambient phase before the manager lock and provider refresh.
        _run_fetches(unphased_fetches,
                     None,
                     provider_error_phase_mode=(
                         provider_phase.ProviderPhaseMode.AMBIENT_LEGACY))

    def _handle_job_status_results(
        self,
        fetch_results: list[tuple[ReplicaInfo, Any]],
        *,
        provider_error_phase_mode: provider_phase.ProviderPhaseMode |
        None = None,
    ) -> None:
        """Consume the parallel job-status fetches, in submission order."""
        for info, result in fetch_results:
            if self._manager_daemon_should_stop():
                return
            try:
                result_payload = result.get()
                # The SSH result may arrive after a partial update has fenced
                # this child.  Never reduce stale health evidence into a
                # replica write or teardown in that process.
                if self._manager_daemon_should_stop():
                    return
                if (isinstance(result_payload, tuple) and
                        len(result_payload) == 3):
                    (job_statuses, recovery_infos,
                     recovery_detail_statuses) = result_payload
                else:
                    # Compatibility for focused tests and old backend shims.
                    job_statuses = result_payload
                    recovery_infos = {}
                    recovery_detail_statuses = {}
                self._provider_identity_uncertain_replica_ids().discard(
                    info.replica_id)
            except exceptions.KubernetesPhysicalClusterIdentityError as error:
                with self.lock:
                    if self._update_recovery_required:
                        return
                    fresh = serve_state.get_replica_info_from_id(
                        self._service_name, info.replica_id)
                    if (fresh is None or not fresh.status_property.
                            should_track_service_status()):
                        continue
                    self._record_provider_identity_uncertain(
                        fresh, 'job-status lookup was fenced off: '
                        f'{common_utils.format_exception(error)}')
                continue
            except exceptions.CommandError:
                # If the job status fetch failed, it is likely that the
                # cluster is preempted.
                error_phase_context: contextlib.AbstractContextManager[Any] = (
                    contextlib.nullcontext()
                    if provider_error_phase_mode is None else
                    provider_phase.provider_phase(provider_error_phase_mode))
                with error_phase_context:
                    with self.lock:
                        if self._update_recovery_required:
                            return
                        # Re-read only after any blocking phase admission:
                        # another thread may have mutated/purged/scheduled-down
                        # this replica while its SSH ran lock-free.
                        fresh = serve_state.get_replica_info_from_id(
                            self._service_name, info.replica_id)
                        if fresh is None:
                            continue
                        if not fresh.status_property.should_track_service_status(
                        ):
                            continue
                        try:
                            fresh_cleanup_fence = (
                                reserved_capacity.
                                parse_protocol_v2_cleanup_fence(fresh))
                            if (provider_error_phase_mode == provider_phase.
                                    ProviderPhaseMode.AMBIENT_LEGACY and
                                    fresh_cleanup_fence is not None):
                                # The row changed authority while SSH was in
                                # flight. Leave it unchanged for the next
                                # strict-v2 round; never cross modes under lock.
                                continue
                            is_preempted = self._handle_preemption(fresh)
                        except exceptions.KubernetesPhysicalClusterIdentityError as error:
                            if self._update_recovery_required:
                                return
                            self._record_provider_identity_uncertain(
                                fresh,
                                'preemption classification was fenced off: '
                                f'{common_utils.format_exception(error)}')
                            continue
                        if self._update_recovery_required:
                            return
                        if (not is_preempted and
                                fresh.system_recovery_disposition
                                == system_recovery_state.
                                SystemRecoveryDisposition.CAPABLE and
                                self._system_recovery_status_barrier_expired(
                                    fresh)):
                            self._terminate_replica(
                                fresh.replica_id,
                                sync_down_logs=True,
                                replica_drain_delay_seconds=0)
                            is_preempted = True
                # Whether preempted or not, move on to the next replica: a
                # replica whose job status cannot be fetched (e.g. a
                # persistently broken but reachable node) must not abort the
                # walk and starve failure detection for every replica after
                # it. The outer fetcher used to swallow the re-raise anyway,
                # so skipping here loses nothing.
                if not is_preempted:
                    logger.error(
                        'Failed to fetch job status for replica '
                        f'{info.replica_id} (cluster {info.cluster_name}); '
                        'skipping it this round.')
                continue
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
                        self._service_name, info.replica_id)
                    if fresh is None:
                        continue
                    if not fresh.status_property.should_track_service_status():
                        continue
                    fresh.status_property.user_app_failed = True
                    self._persist_replica(fresh.replica_id, fresh)
                    logger.warning(
                        f'Service job for replica {fresh.replica_id} FAILED. '
                        'Terminating...')
                    self._terminate_replica(fresh.replica_id,
                                            sync_down_logs=True,
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
            self._record_provider_identity_uncertain(
                info, 'endpoint resolution was fenced off: '
                f'{common_utils.format_exception(error)}')

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
                handle = info.handle(cluster_record)
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
        provider_configs = serve_utils.get_provider_configs_for_handles(handles)

        def _resolve(info: ReplicaInfo) -> None:
            cluster_record = cluster_records.get(info.cluster_name)
            handle = handles.get(info.replica_id)
            if cluster_record is None or handle is None:
                urls[info.replica_id] = None
                return
            urls[info.replica_id] = info._resolve_url(  # pylint: disable=protected-access
                cluster_record=cluster_record,
                handle=handle,
                provider_config=provider_configs.get(info.replica_id),
            )
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
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    for info in group_infos:
                        _retain_identity_uncertainty(info, error)

        def _resolve_ordinary(
                admission: provider_phase.ProviderPhaseAdmission) -> None:
            for info in ordinary_infos:
                # Joining here also covers provider-bearing URL resolution for
                # ordinary rows without inventing a physical identity.
                with reserved_capacity.protocol_v2_provider_fence(
                        info,
                        handles.get(info.replica_id),
                        phase_admission=admission):
                    _resolve(info)

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
            return urls

        # Standalone active-URL reads establish the process phase themselves,
        # always completing exact v2 groups before ambient rows.
        if fenced_groups:
            with provider_phase.provider_phase(
                    provider_phase.ProviderPhaseMode.V2_FENCED) as admission:
                _resolve_fenced_groups(admission, wait_for_initializer=True)
        if ordinary_infos:
            with provider_phase.provider_phase(provider_phase.ProviderPhaseMode.
                                               AMBIENT_LEGACY) as admission:
                _resolve_ordinary(admission)
        return urls

    def _reduce_candidate_probe(
        self,
        info: ReplicaInfo,
        *,
        succeeded: bool,
        probe_started_at: float,
        probe_monotonic_started_at: float,
        exact_job_nonterminal: bool,
        exact_detail_absent: bool,
    ) -> tuple[ReplicaInfo, bool, bool]:
        """Apply the candidate readiness/ABSENT release protocol."""
        if self._update_recovery_required:
            return info, True, False
        outcome = {'off_route': True, 'teardown': False, 'released': False}
        deadlines = self._candidate_release_monotonic_deadlines

        def _reduce(fresh: ReplicaInfo) -> bool:
            outcome.update({
                'off_route': True,
                'teardown': False,
                'released': False,
            })
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
                deadlines[fresh.replica_id] = deadline
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
            return info, True, False
        if updated is None:
            outcome['teardown'] = True
            return info, True, True
        if (updated.system_recovery_disposition
                != system_recovery_state.SystemRecoveryDisposition.CANDIDATE):
            deadlines.pop(updated.replica_id, None)
        if (updated.system_recovery_disposition ==
                system_recovery_state.SystemRecoveryDisposition.ORDINARY):
            if outcome['released']:
                system_oom_recovery_observability.record_for_replica(
                    'authorization_v3_ordinary', updated)
        return updated, bool(outcome['off_route']), bool(outcome['teardown'])

    def _reduce_capable_probe(
        self,
        info: ReplicaInfo,
        *,
        succeeded: bool,
        probe_started_at: float,
    ) -> tuple[ReplicaInfo, system_recovery_state.RecoveryReduction | None]:
        """Reduce a capable replica probe against the latest revision."""
        if self._update_recovery_required:
            return info, None
        outcome: dict[str, system_recovery_state.RecoveryReduction | None] = {
            'reduction': None
        }
        recovery_events: set[str] = set()

        def _reduce(fresh: ReplicaInfo) -> bool:
            outcome['reduction'] = None
            recovery_events.clear()
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
            return info, None
        if updated is None:
            return info, system_recovery_state.RecoveryReduction(
                state=info.system_recovery,
                changed=False,
                force_off_route=True,
                clear_probe_failure_window=False,
                mark_ready=False,
                schedule_legacy_teardown=True)
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
        return updated, reduction

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

    @with_lock
    def _probe_all_replicas(self) -> list[ReplicaInfo]:
        """Run one probe round under one physical UID proof per pool."""
        infos = serve_state.get_replica_infos(self._service_name)
        if self._update_recovery_required:
            return infos
        # Provider-free per-tick state is prepared exactly once even though the
        # provider-bearing work below is split into two authority phases.
        self._tick_version_spec_cache = {}
        self._prune_system_recovery_process_guards(infos)
        self._route_lease_registry().prune({
            info.replica_id: info.replica_record_id
            for info in infos
            if (info.system_recovery_quarantine is None and
                info.system_recovery_disposition ==
                system_recovery_state.SystemRecoveryDisposition.CAPABLE and
                info.system_recovery is not None and info.system_recovery.state
                != system_recovery_state.ControllerRecoveryState.EXHAUSTED and
                not self._has_system_recovery_teardown_intent(info))
        })
        tracked_infos = [
            info for info in infos
            if info.status_property.should_track_service_status()
        ]
        cluster_records = global_user_state.get_clusters_from_names(
            [info.cluster_name for info in tracked_infos])
        if self._update_recovery_required:
            return infos
        representatives: dict[tuple[str, str], tuple[ReplicaInfo, Any]] = {}
        grouped_infos: dict[tuple[str, str], list[ReplicaInfo]] = {}
        ordinary_infos: list[ReplicaInfo] = []
        for info in tracked_infos:
            try:
                cleanup_fence = (
                    reserved_capacity.parse_protocol_v2_cleanup_fence(info))
                if cleanup_fence is None:
                    self._provider_identity_uncertain_replica_ids().discard(
                        info.replica_id)
                    ordinary_infos.append(info)
                    continue
                cluster_record = cluster_records.get(info.cluster_name)
                handle = (cluster_record.get('handle') if isinstance(
                    cluster_record, dict) else None)
                # Validate durable row/handle agreement before any owner
                # thread is allowed to contact the provider.
                reserved_capacity.protocol_v2_provider_fence(info, handle)
            except exceptions.KubernetesPhysicalClusterIdentityError as error:
                if self._update_recovery_required:
                    return infos
                self._record_provider_identity_uncertain(
                    info, 'probe-round physical identity was fenced off: '
                    f'{common_utils.format_exception(error)}')
                continue
            key = (cleanup_fence.kubernetes_context,
                   cleanup_fence.physical_cluster_uid)
            representatives.setdefault(key, (info, handle))
            grouped_infos.setdefault(key, []).append(info)

        fenced_infos = [
            info for group_infos in grouped_infos.values()
            for info in group_infos
        ]
        phased_snapshots: list[ReplicaInfo] = []
        participated_replica_ids: set[int] = set()
        if fenced_infos:
            try:
                with provider_phase.try_provider_phase(
                        provider_phase.ProviderPhaseMode.V2_FENCED
                ) as phase_admission:
                    with reserved_capacity.protocol_v2_provider_batch_fences(
                            representatives,
                            phase_admission=phase_admission,
                            wait_for_initializer=False) as fence_failures:
                        if self._update_recovery_required:
                            return infos
                        failed_replica_ids: set[int] = set()
                        for key, error in fence_failures.items():
                            if self._update_recovery_required:
                                return infos
                            group = grouped_infos[key]
                            failed_replica_ids.update(
                                info.replica_id for info in group)
                            if isinstance(
                                    error, exceptions.
                                    KubernetesPhysicalClusterFenceBusyError):
                                # An existing initializer cannot be joined while
                                # self.lock is held. Preserve every row and retry
                                # the complete group next tick with no evidence.
                                continue
                            if not isinstance(
                                    error, exceptions.
                                    KubernetesPhysicalClusterIdentityError):
                                raise error
                            for info in group:
                                self._record_provider_identity_uncertain(
                                    info,
                                    'probe-round physical identity was fenced '
                                    'off: '
                                    f'{common_utils.format_exception(error)}')
                        admitted_infos = [
                            info for info in fenced_infos
                            if info.replica_id not in failed_replica_ids
                        ]
                        for info in admitted_infos:
                            self._provider_identity_uncertain_replica_ids(
                            ).discard(info.replica_id)
                        if admitted_infos:
                            phased_snapshots.extend(
                                self._probe_all_replicas_with_snapshot(
                                    admitted_infos,
                                    phase_admission=phase_admission))
                            if self._update_recovery_required:
                                return infos
                            participated_replica_ids.update(
                                info.replica_id for info in admitted_infos)
            except exceptions.ProviderPhaseBusyError:
                pass

        if ordinary_infos:
            try:
                # The continuous manager lock permits only a zero-time try.
                # Ordinary work begins after every v2 owner above has retired.
                with provider_phase.try_provider_phase(
                        provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
                ) as phase_admission:
                    phased_snapshots.extend(
                        self._probe_all_replicas_with_snapshot(
                            ordinary_infos, phase_admission=phase_admission))
                    if self._update_recovery_required:
                        return infos
                    participated_replica_ids.update(
                        info.replica_id for info in ordinary_infos)
            except exceptions.ProviderPhaseBusyError:
                pass

        snapshot_by_id = {info.replica_id: info for info in phased_snapshots}
        snapshot: list[ReplicaInfo] = []
        for info in infos:
            if info.replica_id in participated_replica_ids:
                refreshed = snapshot_by_id.get(info.replica_id)
                # A participating row missing from its phase snapshot was
                # durably removed by inline teardown.
                if refreshed is not None:
                    snapshot.append(refreshed)
                continue
            # Untracked, malformed, and admission-denied rows are unchanged.
            snapshot.append(info)
        return snapshot

    def _probe_all_replicas_with_snapshot(
        self,
        infos: list[ReplicaInfo],
        *,
        phase_admission: provider_phase.ProviderPhaseAdmission,
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
        probe_futures = []
        replica_to_probe = []
        infos_to_probe = [
            info for info in infos
            if (info.status_property.should_track_service_status() and
                info.replica_id not in
                self._provider_identity_uncertain_replica_ids())
        ]
        if not infos_to_probe:
            return infos
        if not self._is_pool:
            versions = {info.version for info in infos_to_probe}
            specs = {
                version: spec
                for version, spec in serve_state.get_specs(
                    self._service_name, sorted(versions)).items()
                if spec is not None
            }
            missing_versions = versions - specs.keys()
            if missing_versions:
                missing_versions_str = ', '.join(
                    str(version) for version in sorted(missing_versions))
                version_label = ('Version'
                                 if len(missing_versions) == 1 else 'Versions')
                raise ValueError(
                    f'{version_label} {missing_versions_str} not found.')
            self._tick_version_spec_cache.update(specs)
            probe_urls = self._resolve_probe_urls(
                infos_to_probe, phase_admission=phase_admission)
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
                    self._record_provider_identity_uncertain(
                        info, 'exact status handle was fenced off: '
                        f'{common_utils.format_exception(error)}')
                    return None

            for info in candidates:
                record = status_cluster_records.get(info.cluster_name)
                handle = _status_handle(info, record)
                if (info.replica_id
                        in self._provider_identity_uncertain_replica_ids()):
                    continue
                candidate_status_inputs.append((info, handle))
            for replica_id, candidate in route_issue_candidates.items():
                (info, generation, predicted_generation,
                 retry_submitted_adopted_at, route_url, readiness_path,
                 post_data, readiness_headers, job_id) = candidate
                record = status_cluster_records.get(info.cluster_name)
                handle = _status_handle(info, record)
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
        # Probes are pure I/O (HTTP GET/POST with a several-second timeout):
        # the default ThreadPool size (cpu_count) turns a large fleet into
        # dozens of sequential probe waves and the round overruns its 10s
        # period. Size the pool to the fleet, capped to bound thread cost.
        num_probe_threads = min(len(infos_to_probe),
                                self._PROBE_ROUND_MAX_PARALLELISM)
        with mp_pool.ThreadPool(num_probe_threads) as pool, \
             contextlib.ExitStack() as route_suspension_rollback:
            provider_identity_errors: dict[int, str] = {}
            provider_identity_errors_lock = threading.Lock()
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
                except exceptions.KubernetesPhysicalClusterIdentityError:
                    raise
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(
                        f'Ordered route-issuance status fetch failed for '
                        f'exact job {job_id}: '
                        f'{common_utils.format_exception(e)}')
                    return (None, None,
                            job_lib.JobSystemRecoveryDetailStatus.MALFORMED)

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
                with provider_phase.join_provider_phase(phase_admission):
                    return _probe_nonpool_admitted(info, readiness_path,
                                                   post_data, timeout,
                                                   readiness_headers, route_url)

            def _probe_nonpool_admitted(
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

                (handle, generation, predicted_generation,
                 retry_submitted_adopted_at, exact_route_url,
                 exact_readiness_path, exact_post_data, exact_headers,
                 job_id) = route_input
                try:
                    evidence = _ordered_route_status(result_info, handle,
                                                     job_id)
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    with provider_identity_errors_lock:
                        provider_identity_errors[result_info.replica_id] = (
                            'ordered route-status lookup was fenced off: '
                            f'{common_utils.format_exception(error)}')
                    return (result_info, succeeded, probe_time,
                            request_started_at, None, False)
                # A RETRY_SUBMITTED row may already carry a durable adoption
                # fence from an earlier round. A probe that starts after that
                # fence can register the predicted RECOVERED target here;
                # only the parent can complete the durable READY reduction.
                # A probe at or before the adoption fence requires a later
                # readiness request.
                probe_started_after_adoption = (
                    isinstance(retry_submitted_adopted_at, (int, float)) and
                    not isinstance(retry_submitted_adopted_at, bool) and
                    probe_time > float(retry_submitted_adopted_at))
                requires_next_probe = (predicted_generation and
                                       not probe_started_after_adoption)
                if (not requires_next_probe and
                        not self._update_recovery_required and
                        self._system_recovery_route_evidence_matches(
                            result_info,
                            *evidence,
                            allow_retry_submitted=predicted_generation)):
                    try:
                        # Process-local issuance is intentionally the sole
                        # worker side effect. Persistence/teardown remains in
                        # the parent thread after it revalidates current state.
                        if not self._update_recovery_required:
                            registry = self._route_lease_registry()
                            registry.issue(result_info.replica_id, generation,
                                           exact_route_url,
                                           exact_readiness_path,
                                           exact_post_data, exact_headers,
                                           request_started_at)
                            if self._update_recovery_required:
                                registry.deactivate(result_info.replica_id)
                    except system_recovery_route_lease.RouteLeaseError:
                        pass
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
                    if not self._update_recovery_required:
                        self._provider_identity_uncertain_replica_ids().discard(
                            info.replica_id)
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    with provider_identity_errors_lock:
                        provider_identity_errors[info.replica_id] = (
                            'pool probe was fenced off: '
                            f'{common_utils.format_exception(error)}')
                    result_info, succeeded, probe_time = (info, False,
                                                          time.time())
                return (result_info, succeeded, probe_time, request_started_at,
                        None, False)

            for info in infos_to_probe:
                if self._is_pool:
                    replica_to_probe.append(f'replica_{info.replica_id}(cluster'
                                            f'_name={info.cluster_name})')
                    probe_futures.append(pool.apply_async(_probe_pool, (info,)))
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
                        pool.apply_async(
                            _probe_nonpool,
                            (
                                info,
                                readiness_path,
                                post_data,
                                timeout,
                                readiness_headers,
                                resolved_url,
                            ),
                        ),)
            logger.info(f'Replicas to probe: {", ".join(replica_to_probe)}')

            # Draining in submission order is safe because first issuance is
            # completed by the composite worker itself. A slow earlier future
            # cannot delay a later worker's readiness -> exact-status -> token
            # chain or consume the later worker's lease lifetime.
            probe_results: list[tuple[
                ReplicaInfo, bool, float, float,
                tuple[job_lib.JobStatus | None, job_lib.JobSystemRecoveryInfo |
                      None, job_lib.JobSystemRecoveryDetailStatus] | None,
                bool]] = [future.get() for future in probe_futures]
            # A config/runtime transition can fail while this locked probe is
            # waiting on HTTP.  Treat every completed result as stale before
            # any route, recovery, uptime, replica, or teardown reduction.
            if self._update_recovery_required:
                return infos

            if provider_identity_errors:
                infos_by_id = {info.replica_id: info for info in infos_to_probe}
                for replica_id, message in provider_identity_errors.items():
                    if self._update_recovery_required:
                        return infos
                    identity_info = infos_by_id.get(replica_id)
                    if identity_info is not None:
                        self._record_provider_identity_uncertain(
                            identity_info, message)

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
                    (info, pool.apply_async(_candidate_status, (info, handle)))
                for info, handle in candidate_status_inputs
            }
            candidate_cycle_evidence: dict[int, tuple[bool, bool]] = {}
            candidate_refreshed: dict[int, ReplicaInfo] = {}
            terminal_candidate_ids: set[int] = set()
            for replica_id, (candidate_info,
                             status_future) in candidate_status_futures.items():
                if self._update_recovery_required:
                    return infos
                try:
                    status_payload = status_future.get()
                    if self._update_recovery_required:
                        return infos
                except exceptions.KubernetesPhysicalClusterIdentityError as error:
                    if self._update_recovery_required:
                        return infos
                    self._record_provider_identity_uncertain(
                        candidate_info,
                        'candidate status lookup was fenced off: '
                        f'{common_utils.format_exception(error)}')
                    candidate_cycle_evidence[replica_id] = (False, False)
                    continue
                except Exception as e:  # pylint: disable=broad-except
                    logger.warning(
                        f'Exact candidate status fetch failed for replica '
                        f'{replica_id}: {common_utils.format_exception(e)}')
                    status_payload = None
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
                if self._reconcile_system_recovery_status(
                        candidate_info, job_status, detail, detail_status):
                    # Status reconciliation already scheduled (or completed)
                    # the absorbing legacy teardown.  The readiness probe was
                    # launched from an older snapshot; never let its result
                    # reduce or whole-row-upsert this replica afterward.
                    terminal_candidate_ids.add(replica_id)
                if self._update_recovery_required:
                    return infos
                fresh_candidate = serve_state.get_replica_info_from_id(
                    self._service_name, replica_id)
                if fresh_candidate is not None:
                    candidate_refreshed[replica_id] = fresh_candidate
                candidate_cycle_evidence[replica_id] = (
                    isinstance(job_status, job_lib.JobStatus) and
                    not job_status.is_terminal(), detail_status
                    == job_lib.JobSystemRecoveryDetailStatus.ABSENT and
                    detail is None)

            if candidate_refreshed:
                probe_results = [
                    (candidate_refreshed.get(info.replica_id,
                                             info), succeeded, probe_time,
                     monotonic_started_at, route_evidence, requires_next_probe)
                    for info, succeeded, probe_time, monotonic_started_at,
                    route_evidence, requires_next_probe in probe_results
                ]
            if terminal_candidate_ids:
                probe_results = [
                    result for result in probe_results
                    if result[0].replica_id not in terminal_candidate_ids
                ]

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

            # Parallel cloud-only pre-filter for interruption handling. The
            # full _handle_preemption does a forced cluster refresh (cloud
            # API + an SSH ray-status probe) serially under the manager
            # lock; running it for every failed probe made cold-start probe
            # rounds take minutes instead of the 10s cadence on a
            # 129-replica spot fleet (starving scale-up on the same lock
            # and leaving the LB's ready-set stale -> 503s with READY
            # replicas). Instead, confirm instance liveness with one cheap
            # provider call per failed interruptible replica, in parallel;
            # only
            # cloud-confirmed-dead (or handle-less) replicas take the full
            # preemption path, which is rare and worth its cost. Detection
            # latency is unchanged: every failed probe is still checked
            # against the cloud every round.
            failed_interruptible_infos = [
                info for info, probe_succeeded, _, _, _, _ in probe_results
                if (not probe_succeeded and self._is_interruptible_replica(info)
                   )
            ]
            possibly_preempted_ids: set[int] = set()
            if failed_interruptible_infos:
                num_workers = min(self._PREEMPTION_PREFILTER_PARALLELISM,
                                  len(failed_interruptible_infos))
                with mp_pool.ThreadPool(num_workers) as prefilter_pool:
                    alive_flags = prefilter_pool.map(
                        functools.partial(self._cloud_instance_looks_alive,
                                          phase_admission=phase_admission),
                        failed_interruptible_infos)
                if self._update_recovery_required:
                    return infos
                for failed_info, alive in zip(failed_interruptible_infos,
                                              alive_flags):
                    if self._update_recovery_required:
                        return infos
                    if alive is None:
                        self._record_provider_identity_uncertain(
                            failed_info,
                            'cloud liveness could not prove the physical '
                            'Kubernetes identity')
                possibly_preempted_ids = {
                    failed_info.replica_id
                    for failed_info, alive in zip(failed_interruptible_infos,
                                                  alive_flags)
                    if alive is False
                }

            changed_only_readiness_persistence = self._changed_only_readiness_persistence
            pending_writes: list[tuple[int, ReplicaInfo]] = []
            replicas_to_teardown: list[int] = []
            preempted_replica_ids: set[int] = set()
            terminal_route_ids: set[int] = set()
            for future_result in probe_results:
                if self._update_recovery_required:
                    return infos
                (info, probe_succeeded, probe_time, probe_monotonic_started_at,
                 _, _) = future_result
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
                    # Durable legacy interruption/down intent wins before any
                    # OOM observation or probe reduction can revive routing.
                    try:
                        is_preempted = self._handle_preemption(info)
                    except exceptions.KubernetesPhysicalClusterIdentityError as error:
                        self._record_provider_identity_uncertain(
                            info, 'forced preemption refresh was fenced off: '
                            f'{common_utils.format_exception(error)}')
                        continue
                    if self._update_recovery_required:
                        return infos
                    if is_preempted:
                        preempted_replica_ids.add(info.replica_id)
                        continue

                force_off_route = False
                recovery_holds_failure = False
                if info.system_recovery_quarantine is not None:
                    force_off_route = True
                    should_teardown = True
                elif (info.system_recovery_disposition == system_recovery_state.
                      SystemRecoveryDisposition.CANDIDATE):
                    exact_nonterminal, exact_absent = (
                        candidate_cycle_evidence.get(info.replica_id,
                                                     (False, False)))
                    info, force_off_route, candidate_teardown = (
                        self._reduce_candidate_probe(
                            info,
                            succeeded=probe_succeeded,
                            probe_started_at=probe_time,
                            probe_monotonic_started_at=(
                                probe_monotonic_started_at),
                            exact_job_nonterminal=exact_nonterminal,
                            exact_detail_absent=exact_absent))
                    if self._update_recovery_required:
                        return infos
                    should_teardown = (should_teardown or candidate_teardown)

                route_evidence = ordered_route_evidence.get(info.replica_id)
                if route_evidence is not None:
                    if self._reconcile_system_recovery_status(
                            info, *route_evidence):
                        terminal_route_ids.add(info.replica_id)
                        self._route_lease_registry().deactivate(info.replica_id)
                        continue
                    if self._update_recovery_required:
                        return infos
                    fresh_route_info = serve_state.get_replica_info_from_id(
                        self._service_name, info.replica_id)
                    if fresh_route_info is None:
                        terminal_route_ids.add(info.replica_id)
                        self._route_lease_registry().deactivate(info.replica_id)
                        continue
                    info = fresh_route_info

                recovery_reduction = None
                if (info.system_recovery_disposition == system_recovery_state.
                        SystemRecoveryDisposition.CAPABLE):
                    info, recovery_reduction = self._reduce_capable_probe(
                        info,
                        succeeded=probe_succeeded,
                        probe_started_at=probe_time)
                    if self._update_recovery_required:
                        return infos
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
                                info, route_url, probe_monotonic_started_at,
                                route_evidence))
                    if not route_ready:
                        info.status_property.service_ready_now = False
                        force_off_route = True
                        fresh_after_issue = (
                            serve_state.get_replica_info_from_id(
                                self._service_name, info.replica_id))
                        if (fresh_after_issue is None or
                                fresh_after_issue.system_recovery is not None
                                and fresh_after_issue.system_recovery.state
                                == system_recovery_state.
                                ControllerRecoveryState.EXHAUSTED):
                            terminal_route_ids.add(info.replica_id)
                            continue
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
                    replicas_to_teardown.append(info.replica_id)

            # One multi-row upsert for the whole round's bookkeeping instead
            # of a DB round-trip per replica (all under the manager lock
            # either way, so batching changes no interleaving — it only
            # shortens how long the round holds the lock). Flushed BEFORE
            # the teardowns: _terminate_replica re-reads the replica row,
            # and the probe mutations (e.g. first_ready_time=-1.0, which
            # drives the failure classification) must be visible to it.
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
            for replica_id in replicas_to_teardown:
                self._terminate_replica(replica_id,
                                        sync_down_logs=True,
                                        replica_drain_delay_seconds=0)

        # The round mutated (and persisted) the in-memory `infos` objects, so
        # they ARE the fresh fleet state -- except the few rows the teardown
        # paths rewrote through their own DB re-read (_terminate_replica /
        # _handle_preemption). Re-read only those by id so the returned
        # snapshot matches a post-round full read without re-deserializing
        # the whole fleet (a second full unpickle per 10s round is a real
        # cost at ~1k replicas).
        mutated_ids = (set(replicas_to_teardown) | preempted_replica_ids |
                       terminal_candidate_ids | terminal_route_ids)
        if not mutated_ids:
            return infos
        refreshed = serve_state.get_replica_infos_from_ids(
            self._service_name, sorted(mutated_ids))
        snapshot = []
        for info in infos:
            if info.replica_id not in mutated_ids:
                snapshot.append(info)
                continue
            refreshed_info = refreshed.get(info.replica_id)
            # A missing row means the teardown path already removed the
            # replica record; a post-round full read would not see it either.
            if refreshed_info is not None:
                snapshot.append(refreshed_info)
        return snapshot

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
                # TODO(zhwu): when there are multiple load balancers, we need
                # to make sure the active_versions are the union of all
                # versions of all load balancers.
                self._set_service_status_from_replica_infos(
                    replica_infos, expected_status_epoch=status_epoch)

            except Exception as e:  # pylint: disable=broad-except
                # No matter what error happens, we should keep the
                # replica prober running.
                logger.error('Error in replica prober: '
                             f'{common_utils.format_exception(e)}')
                with ux_utils.enable_traceback():
                    logger.error(f'  Traceback: {traceback.format_exc()}')
            finally:
                # The per-version spec memo is valid only for the probe round
                # that just finished; drop it so the probe-interval read below
                # (and the next round) re-reads each spec fresh and never reuses
                # one across ticks.
                self._tick_version_spec_cache = {}
            # TODO(MaoZiming): Probe cloud for early preemption warning.
            if self._wait_for_manager_daemon_stop(
                    self._get_endpoint_probe_interval_seconds()):
                return

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

    def _exhaust_retired_route_generation(self, info: ReplicaInfo) -> None:
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
        self._terminate_replica(updated.replica_id,
                                sync_down_logs=True,
                                replica_drain_delay_seconds=0)

    def _issue_system_recovery_route(
        self,
        info: ReplicaInfo,
        route_url: str,
        normal_probe_started_at: float,
        evidence: tuple[job_lib.JobStatus | None,
                        job_lib.JobSystemRecoveryInfo | None,
                        job_lib.JobSystemRecoveryDetailStatus] | None,
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
                registry.deactivate(info.replica_id)
                return False
        except system_recovery_route_lease.RouteLeaseError:
            issued = False
        if not issued and self._route_lease_registry().is_retired(
                info.replica_id, generation):
            self._exhaust_retired_route_generation(info)
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
        # are not empty.
        if new_config.get('file_mounts', None) != {}:
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
                # File mounts should both be empty, as update always
                # create new buckets if they are not empty.
                if (old_config == new_config and
                        old_config.get('file_mounts', None) == {}):
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
