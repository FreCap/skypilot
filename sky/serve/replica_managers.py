"""ReplicaManager: handles the creation and deletion of endpoint replicas."""
import asyncio
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
import contextlib
import dataclasses
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
from sky import estimated_spend
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky import task as task_lib
from sky.backends import backend_utils
from sky.backends import cloud_vm_ray_backend
from sky.client import sdk
from sky.serve import constants as serve_constants
from sky.serve import drain_observability
from sky.serve import paid_capacity
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
    from sky.serve.replica_info import ReplicaInfo
    from sky.serve.replica_info import ReplicaStatusProperty
    SpotPlacerType: typing.TypeAlias = spot_placer.SpotPlacer
else:
    ReplicaStatusProperty = replica_info_lib.ReplicaStatusProperty
    ReplicaInfo = replica_info_lib.ReplicaInfo

logger = sky_logging.init_logger(__name__)

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


class _ReplicaLaunchThread(thread_utils.SafeThread):
    """Launch worker that publishes a joinable completion notification."""

    def __init__(self, *args: Any, replica_id: int,
                 completion_queue: 'queue.SimpleQueue[int]',
                 completion_event: threading.Event, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._completion_replica_id = replica_id
        self._completion_queue = completion_queue
        self._completion_event = completion_event

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
    continue_guard: Callable[[], bool] | None = None,
    launch_fence: dict[str, Any] | None = None,
    service_spec: 'service_spec.SkyServiceSpec | None' = None,
    workspace: str | None = None,
    service_name: str | None = None,
    system_recovery_launch_context: dict[str, Any] | None = None,
    get_bound_system_recovery_request_id: Callable[[], str | None] |
    None = None,
    persist_system_recovery_job_id: Callable[[str, int], bool] | None = None,
    demote_system_recovery_candidate: Callable[[], bool] | None = None,
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

    def _check_is_cancelled() -> bool:
        is_cancelled = replica_to_launch_cancelled.get(replica_id, False)
        if is_cancelled:
            logger.info(f'Replica {replica_id} launch cancelled.')
            # Pop the value to indicate that the signal was received.
            replica_to_launch_cancelled.pop(replica_id)
        return is_cancelled

    ownership_lost = threading.Event()

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
            logger.warning('Failed to verify logical cloud launch authority; '
                           f'failing closed: reason={reason}.')
            return False, reason
        if isinstance(result, bool):
            return result, 'authorized' if result else 'guard-rejected'
        if (isinstance(result, tuple) and len(result) == 2 and
                isinstance(result[0], bool) and isinstance(result[1], str) and
                result[1] and len(result[1]) <= 128):
            return result
        logger.warning('Logical cloud launch guard returned an invalid result; '
                       'failing closed: reason=invalid-guard-result.')
        return False, 'invalid-guard-result'

    def _cancel_request_for_ownership_loss() -> None:
        ownership_lost.set()
        replica_to_launch_cancelled[replica_id] = True
        request_id = replica_to_request_id.get(replica_id)
        if request_id is None:
            return
        try:
            sdk.api_cancel(request_id)
        except Exception as e:  # pylint: disable=broad-except
            # The successor still owns the durable replica row and can
            # recover/garbage-collect the incarnation-scoped cluster. Never
            # let a cancellation transport error authorize more stale work.
            logger.warning(f'Failed to cancel stale replica {replica_id} '
                           f'launch request {request_id}: '
                           f'{common_utils.format_exception(e)}')

    def _assert_launch_authorized() -> None:
        if (ownership_lost.is_set() or not _guard_allows(pre_launch_guard) or
                not _guard_allows(continue_guard)):
            _cancel_request_for_ownership_loss()
            raise _ReplicaLaunchOwnershipLostError(
                f'Refusing to launch replica {replica_id} after service '
                'controller ownership was lost.')

    def _stream_with_owner_watchdog(request_id: Any) -> Any:
        """Cancel an async launch promptly when the shared owner fence trips."""
        if continue_guard is None:
            return sdk.stream_and_get(request_id)
        stop_watchdog = threading.Event()

        def _watch_ownership() -> None:
            while not stop_watchdog.wait(_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS):
                if not _guard_allows(continue_guard):
                    logger.warning(
                        f'Cancelling replica {replica_id} launch after '
                        'controller ownership loss.')
                    _cancel_request_for_ownership_loss()
                    return

        watchdog = threading.Thread(target=_watch_ownership,
                                    name=f'replica-{replica_id}-launch-owner',
                                    daemon=True)
        watchdog.start()
        try:
            return sdk.stream_and_get(request_id)
        finally:
            stop_watchdog.set()
            watchdog.join(timeout=1)

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
    # TODO(fcapponi): DEPRECATED resource-action retry/request association
    # owner. Remove at M5 after action-only launch proves its rollback gate;
    # never use this loop for an eligible authoritative service.
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
            cloud_launch_allowed, cloud_launch_reason = (
                _cloud_guard_decision())
            if not cloud_launch_allowed:
                raise _ReplicaLaunchSupersededError(
                    f'Refusing superseded logical cloud launch for replica '
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
            launch_result = _stream_with_owner_watchdog(request_id)
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

        terminate_cluster(cluster_name, log_file=log_file)

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
    started_at = getattr(status, 'drain_started_at', None)
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
def terminate_cluster(cluster_name: str,
                      log_file: str,
                      replica_drain_delay_seconds: int = 0,
                      max_retry: int = 3,
                      drain_deadline: float | None = None,
                      drain_complete: Callable[[], bool] | None = None,
                      continue_guard: Callable[[], bool] | None = None,
                      expected_cluster_record_uuid: str | None = None) -> None:
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

    # Controller-side teardown runs in the API server's workspace context,
    # which may differ from the replica cluster's recorded workspace. Pin each
    # down request to the durable cluster identity so failed-service purge can
    # clean up replicas after their controller is gone.
    cluster_record = global_user_state.get_cluster_from_name(cluster_name)
    cluster_workspace = (cluster_record.get('workspace')
                         if cluster_record is not None else None)
    # TODO(fcapponi): DEPRECATED resource-action retry owner. Remove at M5
    # after action-only down proves its rollback gate; never use this loop for
    # an eligible authoritative service.
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
            workspace_ctx: contextlib.AbstractContextManager = (
                skypilot_config.local_active_workspace_ctx(cluster_workspace)
                if cluster_workspace else contextlib.nullcontext())
            with workspace_ctx:
                core.down(
                    cluster_name,
                    _expected_cluster_record_uuid=expected_cluster_record_uuid)
            logger.info(f'Replica cluster {cluster_name} terminated.')
            return
        except exceptions.ClusterDoesNotExist:
            # The cluster is already terminated.
            logger.info(
                f'Replica cluster {cluster_name} is already terminated.')
            return
        except global_user_state.ClusterRecordIdentityConflictError:
            # A different/null durable identity is not a transient provider
            # failure. Never turn the exact action fence into repeated
            # name-only teardown attempts.
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
    if service_spec.spot_placer is None:
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

    uses_logical_replicas = (getattr(service_spec, 'uses_logical_replicas',
                                     False) is True)
    default_planned_capacity = _uniform_whole_gpu_capacity(task.resources)
    if uses_logical_replicas:
        _exact_accelerator_shapes(task.resources)
    placer_name = getattr(service_spec, 'spot_placer', None)
    candidate_placer = None
    if uses_logical_replicas or isinstance(placer_name, str):
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


def _zero_cost_pool_key(
        location: spot_placer.Location) -> tuple[str, str] | None:
    """Return the shared demand pool identity for one exact GPU shape."""
    if (str(location.cloud).lower() != 'kubernetes' or
            _whole_gpu_capacity(location.accelerators) is None or
            location.accelerators is None):
        return None
    gpu_name = next(iter(location.accelerators))
    return location.region, gpu_name.lower()


def _uniform_whole_gpu_capacity(resources: typing.Iterable[Any]) -> int | None:
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
        resources: typing.Iterable[Any]) -> dict[str, int]:
    """Return the distinct exact-card catalog, or empty for legacy shapes."""
    shapes: dict[str, int] = {}
    canonical_by_name: dict[str, str] = {}
    saw_resource = False
    for resource in resources:
        saw_resource = True
        accelerators = getattr(resource, 'accelerators', None)
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

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int,
                 resource_scope: str | None = None,
                 service_hash: str | None = None,
                 controller_pid: int | None = None,
                 controller_ip: str | None = None,
                 enforce_launch_fence: bool = True) -> None:
        self.lock = threading.Lock()
        self._next_replica_id: int = 1
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
        self._uptime: float | None = None
        self._update_mode = serve_utils.DEFAULT_UPDATE_MODE
        self._is_pool: bool = spec.pool
        # Freshest (received_at, {url: in_flight}, routing_urls,
        # unknown_urls, draining_urls, lb_session_id) report from the LB,
        # published raw (url-keyed) by the controller's
        # load_balancer_sync handler. All sets are sampled by the LB
        # atomically with the gauge; _ReplicaDrainTracker combines them
        # to prove 'not routed and nothing in flight' for a retiring
        # replica's url. Written by whole-tuple replace and read without
        # a lock (atomic in CPython); None until the first report (old
        # LB / pool: never).
        self._lb_in_flight_report: tuple[float, dict[str, int], set[str] | None,
                                         set[str], set[str],
                                         str | None] | None = None
        self._logical_reconcile_snapshot: LogicalReconcileSnapshot | None = (
            None)
        self._logical_state_lock = threading.RLock()
        self._logical_controller_epoch = uuid.uuid4().hex
        # Degraded replacements are protected from recursively replacing one
        # another only until they produce a real capacity sample. The durable
        # marker and this recovered index survive controller restarts; the
        # thread-pool refresher clears both after authoritative recovery.
        self._unknown_capacity_replacement_ids: set[int] = set()
        # Published by the autoscaler tick after it consumes a report. The
        # target remains authoritative while newer LB capacity reports arrive;
        # only a capacity report older than the target publication blocks
        # logical actuation.
        self._logical_target: LogicalTargetState | None = None
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
        # Published only after an autoscaler decision tick has produced an
        # authoritative target. None preserves failure reporting across
        # controller startup and version transitions until that first tick.
        self._target_num_replicas_lock = threading.Lock()
        self._target_num_replicas: int | None = None
        self._target_num_replicas_generation = 0
        # Status writes are serialized only with applied-version transitions,
        # not with autoscaler target publication. This keeps scaling responsive
        # while preventing an old probe from committing across an update.
        # Lock order is self.lock -> status epoch -> target; never reverse it.
        self._status_epoch_lock = threading.Lock()
        self._status_epoch_generation = 0

    def _get_target_num_replicas_lock(self):
        lock = getattr(self, '_target_num_replicas_lock', None)
        if lock is None:
            # Compatibility for embedders and tests that construct a manager
            # without running the current base constructor.
            lock = threading.Lock()
            self._target_num_replicas_lock = lock
        return lock

    def _get_status_epoch_lock(self):
        lock = getattr(self, '_status_epoch_lock', None)
        if lock is None:
            # Compatibility for embedders and tests that construct a manager
            # without running the current base constructor.
            lock = threading.Lock()
            self._status_epoch_lock = lock
        return lock

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
            if expected_version != self.latest_version:
                return False
            if target_num_replicas != getattr(self, '_target_num_replicas',
                                              None):
                self._target_num_replicas_generation = getattr(
                    self, '_target_num_replicas_generation', 0) + 1
            self._target_num_replicas = target_num_replicas
            return True

    def get_target_num_replicas(self) -> int | None:
        """Return the latest version-fenced authoritative autoscaler target."""
        with self._get_target_num_replicas_lock():
            return getattr(self, '_target_num_replicas', None)

    def _transition_status_epoch_for_version(
            self, version: int, update_mode: serve_utils.UpdateMode) -> None:
        """Atomically advance status aggregation to a new applied version."""
        with self._get_status_epoch_lock():
            with self._get_target_num_replicas_lock():
                self._target_num_replicas = None
                self._target_num_replicas_generation = getattr(
                    self, '_target_num_replicas_generation', 0) + 1
                self.latest_version = version
                self._update_mode = update_mode
                self._status_epoch_generation = getattr(
                    self, '_status_epoch_generation', 0) + 1

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
        pending_version = getattr(self, '_pending_version', None)
        generation_matches = (snapshot is not None and
                              (snapshot.generation == decision_generation
                               if require_exact_generation else
                               snapshot.generation >= decision_generation))
        return (snapshot is not None and snapshot.version == version and
                generation_matches and
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
        return getattr(self, '_spot_placer', None)

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
        if (expected_version is not None and
                expected_version != self.latest_version):
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
    ) -> None:
        """Persist complete backend shapes until target capacity is covered."""
        raise NotImplementedError

    def notify_version_pending(self, version: int) -> None:
        """Notify long manager operations that a newer version is waiting."""

    def clear_pending_version(self, version: int) -> None:
        """Clear a previously announced pending version."""

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
        """Process-local drain/retirement counters, lazily created.

        Deliberately NOT assigned in __init__: recovery paths and tests build
        managers without running the current constructor, which is the same
        reason _wait_for_idle_trackers is rebuilt defensively at its use site.
        A counter that raises AttributeError on those paths would turn
        observability into an outage. On the base class so the controller's
        typed reference resolves, even though only the SkyPilot manager
        increments them. See
        docs/designs/serve-drain-proof-across-lb-restarts.md, Milestone 0.
        """
        stats = getattr(self, '_drain_proof_stats_value', None)
        if stats is None:
            stats = drain_observability.DrainProofStats()
            self._drain_proof_stats_value = stats
        return stats

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

    def _publish_legacy_mutation_runtime_state(
            self, runtime: _LegacyReplicaMutationRuntime) -> None:
        """Publish one runtime and synchronized compatibility aliases."""
        # Data-descriptor properties below remain the only read owner. Keeping
        # identity-matched instance entries makes unittest.mock treat legacy
        # instance patch points as local, so context teardown restores the
        # captured value through the setter without retaining old worker pools.
        for legacy_name, runtime_name in self._LEGACY_MUTATION_FIELD_MAP.items(
        ):
            self.__dict__[legacy_name] = getattr(runtime, runtime_name)
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
        if getattr(self, '_spot_placement_state_restored', False):
            return
        placer = getattr(self, '_spot_placer', None)
        if placer is not None:
            states = serve_state.get_service_placement_policy_states(
                self._service_name)
            placer.load_retry_state(None if states is
                                    None else states['spot_placement_state'])
        self._spot_placement_state_restored = True

    def _persist_spot_placement_state_if_dirty(self) -> None:
        """Fence and persist placer evidence before dependent replica rows."""
        placer = getattr(self, '_spot_placer', None)
        if placer is None or not placer.retry_state_dirty:
            return
        service_hash = getattr(self, '_service_hash', None)
        if service_hash is None:
            placer.mark_retry_state_persisted()
            return
        persisted = serve_state.set_service_spot_placement_state(
            self._service_name, service_hash,
            getattr(self, '_controller_owner', None), placer.dump_retry_state())
        if not persisted:
            raise RuntimeError(
                f'Service {self._service_name!r} controller ownership changed '
                'while persisting placement retry state.')
        placer.mark_retry_state_persisted()

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
        event = getattr(self, '_scale_reconciliation_event', None)
        if event is not None:
            event.clear()

    def wait_for_scale_reconciliation(self, timeout_seconds: float) -> bool:
        """Wait interruptibly for committed typed provider feedback."""
        event = getattr(self, '_scale_reconciliation_event', None)
        if event is None:
            time.sleep(timeout_seconds)
            return False
        return event.wait(timeout_seconds)

    def _db_fence_kwargs(self) -> dict[str, Any]:
        """Exact owner predicates, omitted for legacy/direct test managers."""
        kwargs: dict[str, Any] = {}
        service_hash = getattr(self, '_service_hash', None)
        if service_hash is not None:
            kwargs['expected_service_hash'] = service_hash
        controller_owner = getattr(self, '_controller_owner', None)
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
        service_hash = getattr(self, '_service_hash', None)
        controller_owner = getattr(self, '_controller_owner', None)
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
        service_hash = getattr(self, '_service_hash', None)
        if service_hash is None:
            # Compatibility for direct/legacy managers without durable owner
            # identity. New controllers always supply the full tuple.
            return True
        ownership_lost = getattr(self, '_ownership_lost', None)
        if ownership_lost is not None and ownership_lost.is_set():
            return False
        controller_owner = getattr(self, '_controller_owner', None)
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
        if not authorized and ownership_lost is not None:
            ownership_lost.set()
            completion_event = getattr(self, '_launch_completion_event', None)
            if completion_event is not None:
                completion_event.set()
        return authorized

    def _service_is_launch_authorized(self) -> bool:
        """Fail one launch closed unless ownership is currently proven."""
        return self._service_launch_authorization() is True

    def _launch_owner_watchdog_allows_continue(self) -> bool:
        """Cheap shared fence polled by every in-flight launch request."""
        ownership_lost = getattr(self, '_ownership_lost', None)
        return ownership_lost is None or not ownership_lost.is_set()

    def _replica_launch_fence_context(self,
                                      service_version: int | None = None
                                     ) -> dict[str, Any] | None:
        """Owner tuple validated by the API executor before provisioning."""
        if not getattr(self, '_enforce_launch_fence', True):
            # A legacy/non-consolidated controller owns a different Serve DB;
            # the API server cannot validate that tuple against its local DB.
            return None
        service_hash = getattr(self, '_service_hash', None)
        controller_owner = getattr(self, '_controller_owner', None)
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

    def _service_owner_watchdog(self) -> None:
        """Trip one shared launch-cancellation fence on ownership loss."""
        if getattr(self, '_service_hash', None) is None:
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
        deadlines = getattr(self, '_candidate_release_monotonic_deadlines',
                            None)
        if deadlines is None:
            deadlines = {}
            self._candidate_release_monotonic_deadlines = deadlines
        if getattr(self, '_system_recovery_status_initialized', None) is None:
            self._system_recovery_status_initialized = set()
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
        deadlines = getattr(self, '_candidate_release_monotonic_deadlines',
                            None)
        if deadlines is None:
            self._candidate_release_monotonic_deadlines = {}
        else:
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
        initialized = getattr(self, '_system_recovery_status_initialized', None)
        if initialized is None:
            self._system_recovery_status_initialized = set()
        else:
            initialized.intersection_update(capable_ids)

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
        expected_service_hash = getattr(self, '_service_hash', None)
        expected_controller_owner = getattr(self, '_controller_owner', None)
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
        suspension = self._suspend_system_recovery_route_if_unroutable(info)
        try:
            persisted = serve_state.add_or_update_replica(
                self._service_name,
                replica_id,
                info,
                **self._db_fence_kwargs(),
                expected_replica_exists=True)
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
        persisted = serve_state.add_or_update_replica(self._service_name,
                                                      replica_id, info,
                                                      **self._db_fence_kwargs())
        if persisted is False:
            raise RuntimeError(
                f'Service {self._service_name!r} incarnation changed while '
                f'admitting replica {replica_id}.')
        # A successfully inserted recreation is now the live row even if a
        # delayed callback still carries the same numeric replica ID.
        self._route_lease_registry().observe_record_identity(
            replica_id, info.replica_record_id)

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
        try:
            persisted = serve_state.add_or_update_replicas(
                self._service_name,
                replica_infos,
                **self._db_fence_kwargs(),
                expected_replica_exists=True)
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
        service_hash = getattr(self, '_service_hash', None)
        controller_owner = getattr(self, '_controller_owner', None)
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
            fresh = serve_state.get_replica_info_from_id(
                self._service_name, replica_id)
            fence = self._system_recovery_mutation_fence()
            if fresh is None or fence is None:
                return None
            if not transition(fresh):
                return fresh
            suspension = self._suspend_system_recovery_route_if_unroutable(
                fresh)
            try:
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
            current_capacity = int(getattr(info, 'planned_capacity', 1))
            adopted_capacity = max(current_capacity, verified_capacity)
            already_verified = bool(
                getattr(info, 'logical_bridge_capacity_verified', False))
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
        service_hash = getattr(self, '_service_hash', None)
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
                expected_controller_owner=getattr(self, '_controller_owner',
                                                  None),
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
        # TODO(fcapponi): DEPRECATED resource-action retry-clock owner. Remove
        # at M5 after action-only down proves its rollback gate.
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

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int,
                 resource_scope: str | None = None,
                 service_hash: str | None = None,
                 controller_pid: int | None = None,
                 controller_ip: str | None = None,
                 enforce_launch_fence: bool = True) -> None:
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
        yaml_content = serve_state.get_yaml_content(service_name, version)
        assert yaml_content is not None, (
            f'yaml content not found for {service_name} version {version}')
        self.yaml_content: str = yaml_content
        task = load_task_with_service_spec(self.yaml_content, spec)
        self._version_specs: dict[int, service_spec.SkyServiceSpec] = {
            version: spec
        }
        self._uses_logical_replicas = (getattr(spec, 'uses_logical_replicas',
                                               False) is True)
        self._default_planned_capacity = _uniform_whole_gpu_capacity(
            task.resources)
        self._logical_exact_accelerator_shapes = (_exact_accelerator_shapes(
            task.resources) if self._uses_logical_replicas else {})
        self._spot_placer = _load_spot_placer(service_name, version, spec, task,
                                              getattr(self, '_workspace', None))
        self._spot_placement_state_restored = False
        if self._uses_logical_replicas:
            _validate_logical_capacity_sources(self._default_planned_capacity,
                                               self._spot_placer,
                                               task.num_nodes)
        self._fill_skip_last_log_time: float = 0.0
        # TODO(fcapponi): DEPRECATED resource-action owner. Remove this whole
        # legacy/shadow runtime at M5 after action-only launch/down proves its
        # rollback gate; never select it for an eligible authoritative service.
        self._publish_legacy_mutation_runtime_state(
            _LegacyReplicaMutationRuntime())
        # Exact-card authority is assigned when a queued thread is admitted,
        # then checked by that thread immediately before sdk.launch(). The
        # runtime-owned map lets recovered PENDING rows use the same current
        # target fence without rewriting their durable replica format.
        # update_service persists a version before waiting for the manager
        # lock.  A large placer-backed scale-up batch can hold that lock for
        # minutes while it assigns hundreds of replicas.  Publish the waiting
        # version outside the manager lock so that the stale batch can stop
        # enqueueing work and let the update take over.  Controller updates
        # are serialized, and integer assignment is atomic in CPython, so a
        # separate lock would only recreate the lock inversion this signal is
        # designed to break.
        self._pending_version: int | None = None
        # One DB-backed owner watcher per manager fans out through this event;
        # individual launch watchdogs poll the event rather than multiplying
        # ownership queries by the number of in-flight replicas.
        self._ownership_lost = threading.Event()
        self._scale_reconciliation_event = threading.Event()
        self._system_recovery_route_epoch = str(uuid.uuid4())
        self._system_recovery_route_registry = (
            system_recovery_route_lease.ManagerRouteLeaseRegistry())
        # Wall-clock anchors are durable; these process-monotonic guards are
        # intentionally rebuilt in full after every controller replacement.
        self._candidate_release_monotonic_deadlines = {}
        self._system_recovery_status_initialized = set()
        self._wait_for_idle_trackers: dict[int,
                                           tuple[_ReplicaDrainTracker | None,
                                                 float]] = {}
        # A pre-commit-bit SCHEDULED row is ambiguous across an upgrade: the
        # old controller may have started sky.down before persisting RUNNING.
        # Keep it off-route until a fresh current-generation replacement proof
        # lets us safely re-drive the idempotent teardown.
        self._legacy_uncertain_logical_retirement_ids: set[int] = set()
        # Current-format pre-commit logical retirements are safe to re-fence
        # after a controller restart. Until a fresh target/capacity proof is
        # available, keep them off-route instead of aborting every old-epoch
        # selection back to READY.
        self._recovering_logical_retirement_ids: set[int] = set()
        self._logical_retirement_recovery_deadline: float | None = None
        self._logical_retirement_reactivation_generation: int | None = None

        # Tick-scoped memo of per-version specs, reset at the start of every
        # probe round (see _probe_all_replicas). Within a single readiness probe
        # the prober resolves the spec for every replica 4 times (readiness
        # path, post data, headers, timeout); memoizing by version collapses
        # those 4*N DB reads + pickle.loads into one read per distinct version
        # per tick. Scoping it to a tick keeps it single-threaded (only the
        # prober thread touches it) and never reuses a spec across ticks, so it
        # cannot go stale even if a version's spec row is later rewritten.
        self._tick_version_spec_cache: dict[int,
                                            service_spec.SkyServiceSpec] = {}

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
        thread_utils.start_supervised_thread(self._thread_pool_refresher,
                                             'replica-thread-pool-refresher',
                                             stop_event=self._ownership_lost)
        thread_utils.start_supervised_thread(self._job_status_fetcher,
                                             'replica-job-status-fetcher',
                                             stop_event=self._ownership_lost)
        thread_utils.start_supervised_thread(self._replica_prober,
                                             'replica-prober',
                                             stop_event=self._ownership_lost)
        thread_utils.start_supervised_thread(
            self._system_recovery_route_prober,
            'replica-system-recovery-route-prober',
            stop_event=self._ownership_lost)

    def _recover_replica_operations(self):
        """Route restart inference through the removable legacy runtime."""
        self._legacy_mutation_runtime_state().recover(
            self._recover_legacy_replica_operations)

    def _recover_legacy_replica_operations(self) -> None:
        """Re-drive interrupted replica operations from durable state.

        Runs in the dedicated recovery thread started by __init__, which
        holds the manager lock for the whole pass (see __init__ for the
        lock-ordering handshake with the daemon threads)."""
        # TODO(fcapponi): DEPRECATED status-inference owner. Remove the
        # launch/down reconstruction branches at M5 after durable action links
        # become the sole recovery source for eligible authoritative services.
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
        # A rollback to the last compatible v12 writer can erase the complete
        # recovery bundle while retaining the v13 replica version label.  The
        # v13 reader accepts only that exact all-fields-absent shape, and the
        # first rewritten controller that owns the service must immediately
        # canonicalize it again.  Do this before any recovery decisions so a
        # later generic whole-row write cannot perpetuate the temporary shape.
        recovery_fence = self._system_recovery_mutation_fence()
        if (recovery_fence is not None and
                not getattr(self, '_is_pool', False) and
                getattr(self, '_resource_action_mode', 'legacy') == 'legacy' and
                getattr(self, '_enforce_launch_fence', True) and
                serve_state.system_recovery_persistence_available()):
            rewritten = (
                serve_state.rewrite_rollback_replica_system_recovery_state(
                    self._service_name, **recovery_fence))
            if rewritten:
                logger.info(
                    f'Rewrote {rewritten} rollback-shaped system-recovery '
                    'replica row(s) into the complete v13 representation.')

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
                service_hash=getattr(self, '_service_hash', None),
                controller_owner=getattr(self, '_controller_owner', None),
                workspace=getattr(self, '_workspace',
                                  constants.SKYPILOT_DEFAULT_WORKSPACE),
                placer=getattr(self, '_spot_placer', None),
                replica_infos=all_replica_infos,
                priority=serve_constants.LB_REQUEST_PRIORITY_MIN):
            raise RuntimeError(
                f'Service {self._service_name!r} controller ownership changed '
                'while adopting paid-capacity claims.')
        replacement_ids = getattr(self, '_unknown_capacity_replacement_ids',
                                  None)
        if replacement_ids is None:
            replacement_ids = set()
            self._unknown_capacity_replacement_ids = replacement_ids
        logical_state_lock = getattr(self, '_logical_state_lock', None)
        logical_state_guard = (logical_state_lock if logical_state_lock
                               is not None else contextlib.nullcontext())
        with logical_state_guard:
            replacement_ids.update(
                info.replica_id
                for info in all_replica_infos
                if getattr(info, 'unknown_capacity_replacement', False) is True)
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

        recovery_versions = sorted({
            info.version
            for info in to_up_replicas
            if info.version != self.latest_version
        })
        recovery_yaml_contents = serve_state.get_yaml_contents(
            self._service_name, recovery_versions)

        for replica_info in to_up_replicas:
            pending_version = getattr(self, '_pending_version', None)
            if (pending_version is not None and
                    pending_version > replica_info.version):
                logger.info('Stopping recovery re-drive for version '
                            f'{replica_info.version} because version '
                            f'{pending_version} is waiting to be applied.')
                break
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
                            getattr(self, '_resource_scope', None)))
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
            try:
                prior_planned_capacity = getattr(replica_info,
                                                 'planned_capacity', 1)
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
                # Carry the prior row's launch-origin attribution: the fill
                # sentinel was consumed at original emission, so without it
                # the replacement row would flip a fill replica to
                # demand-placed (permanently ceiling-exempt).
                launch_kwargs: dict[str, Any] = {
                    'resources_override': replica_info.resources_override,
                    'existing_replica_infos': all_replica_infos,
                    'recovering_existing_replica': True,
                    'prior_reserved_fill': bool(
                        getattr(replica_info, 'reserved_fill', False)),
                    'prior_is_zero_cost': bool(
                        getattr(replica_info, 'is_zero_cost', False)),
                    'prior_planned_capacity': prior_planned_capacity,
                    'prior_unknown_capacity_replacement': bool(
                        getattr(replica_info, 'unknown_capacity_replacement',
                                False)),
                    'prior_replica_record_id': replica_info.replica_record_id,
                    'prior_created_at': getattr(replica_info, 'created_at',
                                                None),
                    'prior_version': replica_info.version,
                    'prior_yaml_content': prior_yaml_content,
                }
                prior_rebalance_id = getattr(replica_info,
                                             'cost_rebalance_for_replica_id',
                                             None)
                # Older persisted rows, as well as lightweight test doubles,
                # may not carry the pairing field. Only forward a real ID so
                # existing launch wrappers retain their compatible signature.
                if (isinstance(prior_rebalance_id, int) and
                        not isinstance(prior_rebalance_id, bool)):
                    launch_kwargs['prior_cost_rebalance_for_replica_id'] = (
                        prior_rebalance_id)
                prior_paid_pool_key = getattr(replica_info,
                                              'paid_capacity_pool_key', None)
                if isinstance(prior_paid_pool_key, str):
                    launch_kwargs['prior_paid_capacity_pool_key'] = (
                        prior_paid_pool_key)
                self._launch_replica(replica_info.replica_id, **launch_kwargs)
            except Exception as e:  # pylint: disable=broad-except
                logger.error('Failed to re-drive launch of replica '
                             f'{replica_info.replica_id}: '
                             f'{common_utils.format_exception(e)}')

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
        waiting_replicas = [
            info for info in to_down_replicas
            if (not self._is_legacy_uncertain_logical_retirement(info) and
                (self._is_recoverable_uncommitted_logical_retirement(info) or
                 getattr(info.status_property,
                         'wait_for_idle_before_termination', False) is True))
        ]
        recovery_wait_urls: dict[int, str | None] = {}
        if waiting_replicas and not self._is_pool:
            try:
                # One cluster/config snapshot for the whole recovery wave.
                # Resolving ``info.url`` independently repeats cluster-record
                # and provider-config reads around each endpoint lookup while
                # the manager lock blocks every probe and autoscaler tick.
                recovery_wait_urls = self._resolve_probe_urls(waiting_replicas)
            except Exception as e:  # pylint: disable=broad-except
                # URL resolution only enables early drain completion. Keep the
                # bounded per-replica fallback rather than failing recovery.
                logger.warning(
                    'Failed to batch-resolve recovered drain endpoints; '
                    'falling back to per-replica resolution: '
                    f'{common_utils.format_exception(e)}')
        legacy_uncertain_ids = getattr(
            self, '_legacy_uncertain_logical_retirement_ids', None)
        if legacy_uncertain_ids is None:
            legacy_uncertain_ids = set()
            self._legacy_uncertain_logical_retirement_ids = (
                legacy_uncertain_ids)
        recovering_logical_ids = getattr(self,
                                         '_recovering_logical_retirement_ids',
                                         None)
        if recovering_logical_ids is None:
            recovering_logical_ids = set()
            self._recovering_logical_retirement_ids = recovering_logical_ids
        for replica_info in to_down_replicas:
            try:
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
                    if (getattr(replica_info.status_property,
                                'logical_retirement_controller_epoch', None)
                            != self._logical_controller_epoch):
                        recovering_logical_ids.add(replica_info.replica_id)
                    continue
                if (getattr(replica_info.status_property,
                            'wait_for_idle_before_termination', False) is True):
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
                    # rows predating the field re-resolve. getattr: the
                    # row may be an unpickled pre-field instance.
                    drain_cap = getattr(status_property, 'drain_cap_seconds',
                                        None)
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
        if (recovering_logical_ids and getattr(
                self, '_logical_retirement_recovery_deadline', None) is None):
            self._logical_retirement_recovery_deadline = (
                time.monotonic() + _LOGICAL_RETIREMENT_RECOVERY_TIMEOUT_SECONDS)
            logger.info(
                f'Recovered {len(recovering_logical_ids)} uncommitted logical '
                'retirements; keeping them off-route until current capacity '
                'is revalidated.')

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
        prior_reserved_fill: bool = False,
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
        launch_priority: int = serve_constants.LB_REQUEST_PRIORITY_MIN,
        recovering_existing_replica: bool = False,
        logical_reconcile_fence: LogicalTargetState | None = None,
        logical_reconcile_fence_requires_exact_generation: bool = False,
    ) -> bool:
        """Enqueue one replica launch.

        Returns whether a launch was actually enqueued: a zero-cost-only
        fill launch is skipped when no zero-cost location is ACTIVE, and
        the skip must leak nothing -- no replica row, no launch thread.

        prior_reserved_fill: launch-origin attribution of the replica row
        this launch replaces (recovery re-drive). The fill sentinel is
        consumed at original emission, so a re-drive cannot re-derive it
        from the persisted override; OR-ing the prior flag in keeps a
        recovered fill replica counted as arbitrated (ceiling-governed)
        capacity instead of silently converting it to demand.

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
        used by v12/rollback-shaped transition rows.

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

        launch_priority: highest queued demand priority represented by this
        fresh launch. It gates new global claims only and never preempts an
        existing launch.
        """
        legacy_runtime = self._legacy_mutation_runtime_state()
        if replica_id in legacy_runtime.launch_thread_pool:
            logger.warning(f'Launch thread for replica {replica_id} '
                           'already exists. Skipping.')
            return False
        # [boltz fork] Reserved-capacity fill scale-ups carry a sentinel
        # override key restricting the launch to zero-cost locations (plus,
        # under the broker, the grant epoch the decision was emitted
        # under). Pop them on a COPY: the caller may reuse the dict, and
        # the popped copy is what gets persisted on the ReplicaInfo row --
        # the recovery re-drive relaunches with the recorded
        # (location-pinned) override and must not re-enter the
        # fill-selection path (nor re-run the epoch fence: its round is
        # long gone and the row already exists).
        zero_cost_only = False
        fill_grant_epoch: int | None = None
        fill_pool_key: str | None = None
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
            self._service_name, replica_id,
            getattr(self, '_resource_scope', None))
        log_file_name = serve_utils.generate_replica_launch_log_file_name(
            self._service_name, replica_id,
            getattr(self, '_resource_scope', None))
        if recovering_existing_replica:
            if prior_version is None or prior_yaml_content is None:
                raise ValueError('Recovery launch requires its persisted '
                                 'version and exact launch YAML.')
            launch_version = prior_version
            launch_yaml_content = prior_yaml_content
        else:
            launch_version = self.latest_version
            launch_yaml_content = self.yaml_content
        version_specs = getattr(self, '_version_specs', None)
        launch_spec = None
        if version_specs is not None:
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
        if zero_cost_only and self._spot_placer is None:
            # Defensive: fill decisions are only emitted while the capacity
            # poller runs, which requires a placer. Without one there is no
            # way to guarantee zero-cost placement -- skip rather than risk
            # a paid launch.
            self._log_fill_skip('no spot placer available to pin a '
                                'zero-cost-only fill launch')
            return False
        # A fill launch must reach the placer even though zero-cost k8s
        # entries are use_spot=False (the _should_use_spot gate above keys
        # on the task/override spot-ness, which says nothing about fill).
        if cost_rebalance_for_replica_id is not None:
            if self._spot_placer is None:
                logger.warning('Skipping cost-rebalance launch: no spot '
                               'placer is available.')
                self._clean_up_skipped_cost_rebalance_redrive(
                    replica_id, prior_cost_rebalance_for_replica_id)
                return False
            pinned_location = spot_placer.Location.from_resources_override(
                resources_override)
            if pinned_location is None:
                logger.warning('Skipping cost-rebalance launch: candidate '
                               'location could not be reconstructed.')
                self._clean_up_skipped_cost_rebalance_redrive(
                    replica_id, prior_cost_rebalance_for_replica_id)
                return False
            location = self._spot_placer.resolve_location(pinned_location)
            if (location is None or
                    not self._spot_placer.is_active_location(location)):
                logger.info('Skipping cost-rebalance launch: candidate '
                            f'{pinned_location} is no longer active.')
                self._clean_up_skipped_cost_rebalance_redrive(
                    replica_id, prior_cost_rebalance_for_replica_id)
                return False
            if not recovering_existing_replica:
                if existing_replica_infos is None:
                    existing_replica_infos = serve_state.get_replica_infos(
                        self._service_name)
                if paid_location_launch_budget is None:
                    paid_location_launch_budget = (
                        paid_capacity.build_launch_budget(
                            self._spot_placer,
                            workspace=getattr(
                                self, '_workspace',
                                constants.SKYPILOT_DEFAULT_WORKSPACE),
                            existing_replica_infos=existing_replica_infos,
                            globally_managed=(getattr(self, '_service_hash',
                                                      None) is not None),
                            service_name=self._service_name,
                            service_hash=getattr(self, '_service_hash', None),
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
                        return False
                    debit_paid_location_launch_budget = True
                self._spot_placer.reserve_retry(location)
            resources_override = location.to_dict()
            use_spot = location.use_spot
            retry_until_up = False
        elif (self._spot_placer is not None and (use_spot or zero_cost_only) and
              recovered_location is None):
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
            allowed_location_kwargs: dict[str, Any] = (
                {} if allowed_locations is None else {
                    'allowed_locations': allowed_locations
                })
            if (resources_override.get('accelerators') and
                    not allowed_locations):
                raise ValueError(
                    'No active placement location matches exact accelerator '
                    f'override {resources_override["accelerators"]!r}.')
            if existing_replica_infos is None:
                existing_replica_infos = serve_state.get_replica_infos(
                    self._service_name)
            if zero_cost_only:
                # Broker epoch fence: a fill decision computed from a
                # superseded allocation round (the pool's epoch moved
                # because its grants changed, or after a lease-dead gap)
                # must not launch against capacity re-granted to a peer.
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
                        return False
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
                    return False
                location = zero_cost_location
            else:
                if paid_location_launch_budget is None:
                    paid_location_launch_budget = (
                        paid_capacity.build_launch_budget(
                            self._spot_placer,
                            workspace=getattr(
                                self, '_workspace',
                                constants.SKYPILOT_DEFAULT_WORKSPACE),
                            existing_replica_infos=existing_replica_infos,
                            globally_managed=(getattr(self, '_service_hash',
                                                      None) is not None),
                            service_name=self._service_name,
                            service_hash=getattr(self, '_service_hash', None),
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
                        return False
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
                            return False
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
                            return False
                        if location in self._spot_placer.zero_cost_locations():
                            # A successful zero (or an exhausted speculative
                            # allowance) is authoritative. If no paid candidate
                            # is active, defer instead of falling through into
                            # the same saturated research pool.
                            logger.info(
                                'Deferring demand launch because the shared '
                                'zero-cost GPU budget is exhausted and no '
                                'paid location is active.')
                            return False
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
                return False
            debit_paid_location_launch_budget = (
                paid_location_launch_budget is not None and
                location in paid_location_launch_budget.remaining_by_location)
            resources_override.update(location.to_dict())
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
                return False
        # When the spot placer owns failover (use_spot + placer above sets
        # retry_until_up=False), the launch is pinned to ONE location, so a
        # capacity failure there must propagate immediately for the placer to
        # mark the location preemptive and pick a different one on the next
        # launch. Retrying the same exhausted zone in place with the default
        # attempts + 60s exponential backoff burns minutes before failing
        # over. Other (transient) launch errors say nothing about the
        # location's capacity, so they keep the default in-place retries.
        availability_max_retry = (1 if location is not None else None)
        logical_cloud_launch_guard: (Callable[[], bool | tuple[bool, str]] |
                                     None) = None
        if (getattr(self, '_uses_logical_replicas', False) and
                bool(getattr(self, '_logical_exact_accelerator_shapes', {})) and
                not zero_cost_only and not prior_reserved_fill and
                cost_rebalance_for_replica_id is None and
                not prior_unknown_capacity_replacement):
            logical_cloud_launch_guard = lambda: (
                self._queued_logical_launch_fence_decision(replica_id)[:2])
        launch_fence = self._replica_launch_fence_context(launch_version)
        recovery_intent: (system_recovery_state.SystemRecoveryLaunchIntent |
                          None) = None
        recovery_launch_context: dict[str, Any] | None = None
        candidate_prerequisites = (
            not recovering_existing_replica and
            not getattr(self, '_is_pool', False) and
            getattr(self, '_resource_action_mode', 'legacy') == 'legacy' and
            getattr(self, '_enforce_launch_fence', True) and
            launch_fence is not None and launch_spec is not None and
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
                    workspace = getattr(
                        self, '_workspace',
                        skypilot_config.get_active_workspace() or
                        constants.SKYPILOT_DEFAULT_WORKSPACE)
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
        if getattr(self, '_uses_logical_replicas', False):
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
        # and the ceiling's demand exemption key on this flag. OR in the
        # replaced row's attribution on recovery re-drives (the sentinel
        # only exists at original emission).
        info.reserved_fill = bool(zero_cost_only or prior_reserved_fill)
        is_zero_cost = bool(prior_is_zero_cost or zero_cost_only)
        if not is_zero_cost and self._spot_placer is not None:
            candidates = self._spot_placer.zero_cost_locations()
            if isinstance(candidates, (list, tuple, set, frozenset)):
                is_zero_cost = location in candidates
        info.is_zero_cost = is_zero_cost
        info.cost_rebalance_for_replica_id = (cost_rebalance_for_replica_id)
        info.paid_capacity_pool_key = prior_paid_capacity_pool_key
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
                    return False
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
                        **self._db_fence_kwargs()):
                    # No row was written and the launch thread was never
                    # registered/started: same leak-nothing contract as the
                    # pre-check fence.
                    self._log_fill_skip(
                        f'grant epoch {fill_grant_epoch} superseded or round '
                        'in flight at persist')
                    return False
            else:
                if debit_paid_location_launch_budget:
                    assert location is not None
                    assert paid_location_launch_budget is not None
                    claim_result = paid_capacity.try_persist_claim(
                        service_name=self._service_name,
                        service_hash=getattr(self, '_service_hash', None),
                        controller_owner=getattr(self, '_controller_owner',
                                                 None),
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
                        return False
                    if claim_result == paid_capacity.ClaimResult.SATURATED:
                        paid_capacity.exhaust(paid_location_launch_budget,
                                              location)
                        logger.info('Deferring paid demand launch at '
                                    f'{location}: {claim_result.value}.')
                        return False
                    if (claim_result ==
                            paid_capacity.ClaimResult.SERVICE_SATURATED):
                        paid_capacity.exhaust_service(
                            paid_location_launch_budget)
                        logger.info('Deferring paid demand launch because the '
                                    'service paid-capacity envelope is full.')
                        return False
                    if (claim_result ==
                            paid_capacity.ClaimResult.HIGHER_PRIORITY_WAITING):
                        paid_capacity.defer_for_priority(
                            paid_location_launch_budget, location)
                        logger.info('Deferring paid demand launch at '
                                    f'{location}: {claim_result.value}.')
                        return False
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
                replacement_ids = getattr(self,
                                          '_unknown_capacity_replacement_ids',
                                          None)
                if replacement_ids is None:
                    replacement_ids = set()
                    self._unknown_capacity_replacement_ids = replacement_ids
                replacement_ids.add(replica_id)

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
        completion_queue, completion_event = self._launch_completion_state()
        t = _ReplicaLaunchThread(
            target=launch_cluster,
            replica_id=replica_id,
            completion_queue=completion_queue,
            completion_event=completion_event,
            args=(replica_id, launch_yaml_content, cluster_name, log_file_name,
                  legacy_runtime.replica_to_request_id,
                  legacy_runtime.replica_to_launch_cancelled,
                  resources_override, retry_until_up),
            kwargs={
                'availability_max_retry': availability_max_retry,
                'exact_resources_override': location is not None,
                'pre_launch_guard': self._service_is_launch_authorized,
                'cloud_launch_guard': logical_cloud_launch_guard,
                'continue_guard': self._launch_owner_watchdog_allows_continue,
                'launch_fence': launch_fence,
                'service_spec': launch_spec,
                'service_name': self._service_name,
                'workspace': getattr(
                    self, '_workspace',
                    skypilot_config.get_active_workspace() or
                    constants.SKYPILOT_DEFAULT_WORKSPACE),
                **recovery_launch_kwargs,
            },
        )
        if existing_replica_infos is not None:
            # Bulk callers (recovery re-drive) reuse one snapshot across a
            # whole wave of launches. Append each accepted replica so shared
            # zero-cost capacity accounting sees the in-wave reservations.
            existing_replica_infos.append(info)
        # Don't start right now; we will start it later in _refresh_thread_pool
        # to avoid too many sky.launch running at the same time.
        legacy_runtime.launch_thread_pool[replica_id] = t
        return True

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

    def _select_budgeted_zero_cost_location(
        self,
        budget: _ZeroCostDemandBudget,
        allowed_locations: set[spot_placer.Location] | None = None,
    ) -> spot_placer.Location | None:
        """Reserve and select one location from a measured batch budget."""
        if self._spot_placer is None:
            return None
        allowed = set()
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
                allowed.add(location)
        if not allowed:
            return None
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
        bounded number of backend probes; a successful zero is authoritative.
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
        measured = {
            key: observation.free_gpus
            for key, observation in observations.items()
        }
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
            created_at = getattr(info, 'created_at', None)
            status_property = getattr(info, 'status_property', None)
            if info.is_ready:
                first_ready_time = getattr(status_property, 'first_ready_time',
                                           None)
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
                launch_status = getattr(status_property, 'sky_launch_status',
                                        None)
                unobserved = (
                    info.status == serve_state.ReplicaStatus.PENDING or
                    launch_status != common_utils.ProcessStatus.SUCCEEDED or
                    (snapshot_time is not None and created_at is not None and
                     created_at > snapshot_time))
            if unobserved:
                width = (_whole_gpu_capacity(replica_location.accelerators) or
                         int(getattr(info, 'planned_capacity', 1)))
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
    ) -> bool:
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
            launched = self._launch_replica(self._next_replica_id,
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
            launched = self._launch_replica(self._next_replica_id,
                                            resources_override, **launch_kwargs)
        if launched:
            self._next_replica_id += 1
        return launched

    @with_lock
    def scale_up(self,
                 resources_override: dict[str, Any] | None = None) -> None:
        if self._spot_placer is not None:
            self._spot_placer.refresh_workspace_policy()
        self._scale_up_one_locked(
            resources_override, serve_state.get_replica_ids(self._service_name))

    @with_lock
    def scale_up_batch(
        self,
        resources_overrides: list[dict[str, Any] | None],
        expected_version: int | None = None,
        launch_priority: int = (serve_constants.LB_REQUEST_PRIORITY_MIN)
    ) -> None:
        """Enqueue a batch of replica launches under ONE lock acquisition.

        The manager lock is held by the readiness-probe round for tens of
        seconds per round on large fleets, so per-replica `scale_up` calls
        (one lock acquisition each) trickle through the short gaps between
        rounds: measured live at a 1000-target / ~340-replica fleet, launch
        enqueueing was the scaling bottleneck at ~100 replicas per several
        minutes while the launch budget sat idle. Batching the whole
        autoscaler tick into one acquisition makes the enqueue O(1) lock
        waits per tick; the launch budget in `_refresh_thread_pool` then
        paces actual `sky.launch` concurrency as intended.

        Shared zero-cost placement also reuses one replica snapshot across the
        wave. The launch path appends each successfully enqueued replica so
        later decisions observe in-wave reservations without querying and
        unpickling all existing rows once per launch.
        """
        if self._spot_placer is not None:
            self._spot_placer.refresh_workspace_policy()
        needs_reservation = (
            self._batch_needs_placement_snapshot(resources_overrides) and
            self._uses_shared_zero_cost_demand_budget())
        batch_kwargs: dict[str, Any] = {}
        if launch_priority != serve_constants.LB_REQUEST_PRIORITY_MIN:
            batch_kwargs['launch_priority'] = launch_priority
        if not needs_reservation:
            self._scale_up_batch_locked(resources_overrides, expected_version,
                                        **batch_kwargs)
            return
        try:
            lock = locks.get_lock(
                serve_constants.DEMAND_CAPACITY_RESERVATION_LOCK_ID)
            with lock.acquire(blocking=False):
                self._scale_up_batch_locked(resources_overrides,
                                            expected_version, **batch_kwargs)
        except locks.LockTimeout:
            logger.info('Deferring demand scale-up because another service '
                        'is reserving shared zero-cost capacity.')

    def _scale_up_batch_locked(
        self,
        resources_overrides: list[dict[str, Any] | None],
        expected_version: int | None = None,
        launch_priority: int = (serve_constants.LB_REQUEST_PRIORITY_MIN)
    ) -> None:
        """Persist one physical batch while any shared demand lock is held."""
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
                workspace=getattr(self, '_workspace',
                                  constants.SKYPILOT_DEFAULT_WORKSPACE),
                existing_replica_infos=existing_replica_infos,
                globally_managed=(getattr(self, '_service_hash', None)
                                  is not None),
                service_name=self._service_name,
                service_hash=getattr(self, '_service_hash', None),
                requested_frontier_keys=self._requested_paid_frontier_keys(
                    resources_overrides)))
        deferred_paid_overrides: list[dict[str, Any] | None] = []
        for resources_override in resources_overrides:
            pending_version = getattr(self, '_pending_version', None)
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
            launched = self._scale_up_one_locked(resources_override,
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
            if ((not launched and paid_selection_stopped) or service_exhausted):
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
    ) -> None:
        """Plan and persist complete backend shapes up to a logical target.

        Selection and row persistence share the manager lock and one mutable
        fleet snapshot. Each persisted backend immediately participates in the
        next placement decision, so a single 8-slot choice removes eight slots
        from the shortfall instead of causing eight physical launches.
        """
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

        def _replica_card(info: ReplicaInfo) -> str | None:
            accelerators = None
            location = info.get_spot_location()
            if location is not None:
                accelerators = location.accelerators
            if not accelerators:
                accelerators = (getattr(info, 'resources_override', None) or
                                {}).get('accelerators')
            if not accelerators:
                resources = getattr(getattr(info, 'handle', None),
                                    'launched_resources', None)
                accelerators = getattr(resources, 'accelerators', None)
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
                if (getattr(info.status_property, 'is_scale_down', False)
                        is True):
                    continue
                planned = int(getattr(info, 'planned_capacity', 1))
                if info.replica_id in replace_unknown_replica_ids:
                    # A bounded degraded-recovery decision explicitly
                    # overlaps this uncertain backend without terminating it.
                    continue
                observed = capacity_snapshot.observed_slots_by_replica_id.get(
                    info.replica_id)
                if (info.is_ready and observed is not None and info.replica_id
                        not in capacity_snapshot.unknown_replica_ids):
                    if (observed <= 0 and getattr(
                            info, 'unknown_capacity_replacement', False)):
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
                if (info.is_terminal or info.version != version or getattr(
                        info.status_property, 'is_scale_down', False) is True or
                        info.replica_id in replace_unknown_replica_ids):
                    continue
                card = _replica_card(info)
                if card is None:
                    continue
                planned = int(getattr(info, 'planned_capacity', 1))
                observed = capacity_snapshot.observed_slots_by_replica_id.get(
                    info.replica_id)
                if (info.is_ready and observed is not None and info.replica_id
                        not in capacity_snapshot.unknown_replica_ids):
                    if (observed <= 0 and getattr(
                            info, 'unknown_capacity_replacement', False)):
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
        paid_location_launch_budget = (paid_capacity.build_launch_budget(
            self._spot_placer,
            workspace=getattr(self, '_workspace',
                              constants.SKYPILOT_DEFAULT_WORKSPACE),
            existing_replica_infos=existing_replica_infos,
            globally_managed=(getattr(self, '_service_hash', None) is not None),
            service_name=self._service_name,
            service_hash=getattr(self, '_service_hash', None),
            requested_frontier_keys=(None if not card_targets else {
                (str(card).casefold(),)
                for card, target in card_targets.items()
                if committed_by_card.get(card, 0) < target
            })) if self._spot_placer is not None else None)
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
            if self._paid_service_envelope_blocks_launch(
                    paid_location_launch_budget, resources_override):
                if selected_card is not None:
                    deferred_cards.add(selected_card)
                    continue
                break
            before = len(existing_replica_infos)
            launch_kwargs: dict[str, Any] = {}
            if paid_location_launch_budget is not None:
                launch_kwargs['paid_location_launch_budget'] = (
                    paid_location_launch_budget)
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
            launched = self._scale_up_one_locked(
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
            if not launched or len(existing_replica_infos) == before:
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
            launched_capacity += sum(
                max(1, int(getattr(info, 'planned_capacity', 1)))
                for info in existing_replica_infos[before:])

    def notify_version_pending(self, version: int) -> None:
        with self._logical_state_lock:
            pending_version = getattr(self, '_pending_version', None)
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
            recovering_ids = getattr(self, '_recovering_logical_retirement_ids',
                                     None)
            if recovering_ids is None:
                recovering_ids = set()
                self._recovering_logical_retirement_ids = recovering_ids
            recovering_ids.update(retiring_ids)
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
            if getattr(self, '_pending_version', None) == version:
                self._pending_version = None

    def _batch_needs_placement_snapshot(
            self, resources_overrides: list[dict[str, Any] | None]) -> bool:
        """Whether any launch in a batch will ask the placer for a location."""
        if self._spot_placer is None or not resources_overrides:
            return False
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
            service_spec=getattr(self, '_version_specs', {}).get(
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
        # TODO(fcapponi): DEPRECATED resource-action result reducer. Remove at
        # M5 for eligible authoritative services after the durable reducer
        # owns this projection.
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
        # TODO(fcapponi): DEPRECATED resource-action scheduler. Remove at M5
        # for eligible authoritative services after durable down admission
        # owns scheduling and retry.
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
            if launch_thread.is_alive():
                legacy_runtime.replica_to_launch_cancelled[replica_id] = True
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
                    if replica_id not in legacy_runtime.replica_to_launch_cancelled:
                        # Indicates that the cancellation was received.
                        break
                    if not launch_thread.is_alive():
                        # It's possible that the launch thread immediately
                        # finished after we check. Exit the loop now.
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
            legacy_runtime.launch_thread_pool.pop(replica_id)
            legacy_runtime.replica_to_request_id.pop(replica_id)
            legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)

        if replica_id in legacy_runtime.down_thread_pool:
            logger.warning(f'Terminate thread for replica {replica_id} '
                           'already exists. Skipping.')
            return

        log_file_name = serve_utils.generate_replica_log_file_name(
            self._service_name, replica_id,
            getattr(self, '_resource_scope', None))

        def _download_and_stream_logs(info: ReplicaInfo):
            launch_log_file_name = (
                serve_utils.generate_replica_launch_log_file_name(
                    self._service_name, replica_id,
                    getattr(self, '_resource_scope', None)))
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
            job_log_file_name = controller_utils.download_and_stream_job_log(
                backend, handle, replica_job_logs_dir, job_ids)
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
        if hasattr(self, '_resource_action_mode'):
            teardown_snapshot = (
                serve_state.get_replica_info_with_resource_action_identity(
                    self._service_name, replica_id))
            assert teardown_snapshot is not None
            info, resource_action_identity = teardown_snapshot
        else:
            # Compatibility for lightweight embedders/tests constructed with
            # ``__new__``. Every normally initialized manager takes the
            # atomic action-aware snapshot above.
            info = serve_state.get_replica_info_from_id(self._service_name,
                                                        replica_id)
            assert info is not None
            resource_action_identity = None
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
                _download_and_stream_logs(info)
            except Exception as e:  # pylint: disable=broad-except
                # Logs aid diagnosis, but cannot be a prerequisite for
                # stopping potentially billable infrastructure.
                logger.warning(
                    f'Failed to sync down logs for replica {replica_id}; '
                    'continuing with cleanup: '
                    f'{common_utils.format_exception(e)}')

        logger.info(f'preempted: {info.status_property.preempted}, '
                    f'replica_id: {replica_id}')
        info.status_property.is_scale_down = is_scale_down
        info.status_property.purged = purge
        info.status_property.wait_for_idle_before_termination = False

        # If the cluster does not exist, it means either the cluster never
        # exists (e.g., the cluster is scaled down before it gets a chance to
        # provision) or the cluster is preempted and cleaned up by the status
        # refresh. In this case, we skip spawning a new down thread to save
        # controller resources.
        if not global_user_state.cluster_with_name_exists(info.cluster_name):
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
                replica_url = info.url
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
            },
        )
        legacy_runtime.down_thread_pool[replica_id] = t

    def _reconcile_failed_cleanup(self,
                                  replica_infos: list[ReplicaInfo]) -> None:
        """Re-drive every durable cleanup failure until absence is proven."""
        # TODO(fcapponi): DEPRECATED resource-action retry scheduler. Remove at
        # M5 for eligible authoritative services after database-clock action
        # retries own cleanup.
        legacy_runtime = self._legacy_mutation_runtime_state()
        now = time.monotonic()
        _, retry_at_by_replica = self._failed_cleanup_retry_state()
        for info in replica_infos:
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
            ambiguous_legacy_failure = (
                down_failed and is_scale_down and not purge and
                not status_property.preempted and
                not _is_valid_drain_started_at(
                    getattr(status_property, 'drain_started_at', None)))
            if ((not down_failed or ambiguous_legacy_failure) and
                    is_scale_down and not purge and
                    not status_property.preempted):
                drain_cap = getattr(status_property, 'drain_cap_seconds', None)
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
        drain_cap = getattr(info.status_property, 'drain_cap_seconds', None)
        needs_persist = False
        if drain_cap is None:
            drain_cap = self._resolve_drain_cap_seconds(info.replica_id, info)
            info.status_property.drain_cap_seconds = drain_cap
            needs_persist = True
        prior_started_at = getattr(info.status_property, 'drain_started_at',
                                   None)
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
                replica_url = info.url
            except Exception as e:  # pylint: disable=broad-except
                logger.warning('Unable to resolve replica '
                               f'{info.replica_id} url for strict drain: '
                               f'{common_utils.format_exception(e)}')
                replica_url = None
        if replica_url is not None and not self._is_pool:
            assert isinstance(replica_url, str), replica_url
            tracker = _ReplicaDrainTracker(self, replica_url, drain_started)
        self._wait_for_idle_trackers[info.replica_id] = (tracker, deadline)

    def _defer_scale_down_until_idle(
            self,
            replica_id: int,
            logical_retirement: tuple[int, int, int] | None = None,
            *,
            replica_info: ReplicaInfo | None = None) -> None:
        """Persist off-route state without admitting termination yet."""
        info = replica_info
        if info is None:
            info = serve_state.get_replica_info_from_id(self._service_name,
                                                        replica_id)
        if info is None:
            return
        if (getattr(info.status_property, 'wait_for_idle_before_termination',
                    False) is True):
            self._register_wait_for_idle(info)
            return
        if not global_user_state.cluster_with_name_exists(info.cluster_name):
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
        self._register_wait_for_idle(info)

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
        version = getattr(status, 'logical_retirement_version', None)
        controller_epoch = getattr(status,
                                   'logical_retirement_controller_epoch', None)
        selection_generation = getattr(status, 'logical_retirement_generation',
                                       None)
        selection_target = getattr(status, 'logical_retirement_target_capacity',
                                   None)
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
        pending_version = getattr(self, '_pending_version', None)
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
                    getattr(candidate.status_property, 'is_scale_down',
                            False) is True):
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
            ready_capacity += min(
                int(getattr(candidate, 'planned_capacity', 1)), observed)
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
            accelerators = (getattr(info, 'resources_override', None) or
                            {}).get('accelerators')
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
                planned = int(getattr(info, 'planned_capacity', 1))
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
                    getattr(candidate.status_property, 'is_scale_down',
                            False) is True or candidate.version > version):
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
            ready[card] += min(int(getattr(candidate, 'planned_capacity', 1)),
                               observed)
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
        tracked = getattr(self, '_wait_for_idle_trackers',
                          {}).get(info.replica_id)
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
        retirement_version = getattr(status, 'logical_retirement_version', None)
        controller_epoch = getattr(status,
                                   'logical_retirement_controller_epoch', None)
        selection_generation = getattr(status, 'logical_retirement_generation',
                                       None)
        selection_target = getattr(status, 'logical_retirement_target_capacity',
                                   None)
        confirmed_generation = getattr(
            status, 'logical_retirement_confirmed_generation', None)
        bounded_deadline = getattr(status,
                                   'logical_retirement_bounded_deadline', False)
        committed = getattr(status, 'logical_retirement_committed', None)
        info_version = getattr(info, 'version', None)
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
        retirement_version = getattr(status, 'logical_retirement_version', None)
        controller_epoch = getattr(status,
                                   'logical_retirement_controller_epoch', None)
        selection_generation = getattr(status, 'logical_retirement_generation',
                                       None)
        selection_target = getattr(status, 'logical_retirement_target_capacity',
                                   None)
        confirmed_generation = getattr(
            status, 'logical_retirement_confirmed_generation', None)
        bounded_deadline = getattr(status,
                                   'logical_retirement_bounded_deadline', False)
        committed = getattr(status, 'logical_retirement_committed', None)
        info_version = getattr(info, 'version', None)
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
        retirement_version = getattr(status, 'logical_retirement_version', None)
        controller_epoch = getattr(status,
                                   'logical_retirement_controller_epoch', None)
        selection_generation = getattr(status, 'logical_retirement_generation',
                                       None)
        selection_target = getattr(status, 'logical_retirement_target_capacity',
                                   None)
        confirmed_generation = getattr(
            status, 'logical_retirement_confirmed_generation', None)
        bounded_deadline = getattr(status,
                                   'logical_retirement_bounded_deadline', False)
        committed = getattr(status, 'logical_retirement_committed', None)
        info_version = getattr(info, 'version', None)
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
        uncertain_ids = getattr(self,
                                '_legacy_uncertain_logical_retirement_ids',
                                None)
        if uncertain_ids is None:
            uncertain_ids = set()
            self._legacy_uncertain_logical_retirement_ids = uncertain_ids
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
                pending_version = getattr(self, '_pending_version', None)
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
        deadline = getattr(self, '_logical_retirement_recovery_deadline', None)
        return deadline is not None and time.monotonic() >= deadline

    @staticmethod
    def _logical_planned_capacity(info: ReplicaInfo) -> int:
        planned = getattr(info, 'planned_capacity', 1)
        if (isinstance(planned, bool) or not isinstance(planned, int) or
                planned < 1):
            return 1
        return planned

    def _clear_logical_retirement_recovery_if_done(self) -> None:
        recovering_ids: set[int] = getattr(
            self, '_recovering_logical_retirement_ids', set())
        if recovering_ids:
            return
        self._logical_retirement_recovery_deadline = None
        self._logical_retirement_reactivation_generation = None

    def _reconcile_recovering_logical_retirements(self) -> None:
        """Adopt safe old-epoch drains without advertising the whole fleet."""
        recovering_ids = getattr(self, '_recovering_logical_retirement_ids',
                                 None)
        if not recovering_ids:
            return
        if getattr(self, '_logical_retirement_recovery_deadline', None) is None:
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
            pending_version = getattr(self, '_pending_version', None)
            if (pending_version is not None and
                    pending_version > self.latest_version):
                return
            reactivation_generation = getattr(
                self, '_logical_retirement_reactivation_generation', None)
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
        # Some recovery/unit-test construction paths predate this field and
        # instantiate the manager without running the current constructor.
        if not hasattr(self, '_wait_for_idle_trackers'):
            self._wait_for_idle_trackers = {}
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
            waiting_for_idle = getattr(status,
                                       'wait_for_idle_before_termination',
                                       False) is True
            down_thread = down_thread_pool.get(replica_id)
            queued_logical = (getattr(status, 'logical_retirement_version',
                                      None) is not None and
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
                (replica_id in getattr(
                    self, '_recovering_logical_retirement_ids', set()) or
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
        for replica_id, tracked in tracker_items:
            tracker, deadline = tracked
            info = tracked_infos.get(replica_id)
            if info is None:
                continue
            if info.cluster_name not in cluster_status_fields:
                drained = True
            else:
                if tracker is None:
                    # Endpoint discovery can fail transiently during recovery.
                    self._wait_for_idle_trackers.pop(replica_id, None)
                    self._register_wait_for_idle(info, deadline=deadline)
                    retried = self._wait_for_idle_trackers.get(replica_id)
                    tracker = retried[0] if retried is not None else None
                drained = tracker is not None and tracker()
            deadline_expired = time.monotonic() >= deadline
            logical_retirement = getattr(info.status_property,
                                         'logical_retirement_version',
                                         None) is not None
            if logical_retirement:
                if self._is_committed_logical_retirement(info):
                    # Admission persisted the irreversible bit before starting
                    # the worker. A version change must not return this backend
                    # to routing while the shared termination budget delays the
                    # already-authorized cleanup.
                    continue
                recovering_ids: set[int] = getattr(
                    self, '_recovering_logical_retirement_ids', set())
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
                    retirement_version = getattr(info.status_property,
                                                 'logical_retirement_version',
                                                 None)
                    info_version = getattr(info, 'version', None)
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
                drain_cap = getattr(info.status_property, 'drain_cap_seconds',
                                    None)
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
        replacement_ids: set[int] = getattr(
            self, '_unknown_capacity_replacement_ids', set())
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
                if info is not None and getattr(
                        info, 'unknown_capacity_replacement', False) is True:
                    info.unknown_capacity_replacement = False
                    self._persist_replica(replica_id, info)
                replacement_ids.discard(replica_id)

    @with_lock
    def scale_down(self,
                   replica_id: int,
                   purge: bool = False,
                   wait_for_idle: bool = False,
                   expected_version: int | None = None) -> None:
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
        if not replica_ids:
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
            if getattr(self, '_service_hash', None) is not None:
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
                            getattr(candidate.status_property, 'is_scale_down',
                                    False) is True or
                            candidate.status_property.first_ready_time
                            is not None or replica_id in down_pool):
                        continue
                    launch_thread = launch_pool.get(replica_id)
                    if (launch_thread is not None and launch_thread.is_alive()):
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
                               getattr(candidate.status_property,
                                       'is_scale_down', False) is not True)
                if contributes and candidate.version == version:
                    planned = int(getattr(candidate, 'planned_capacity', 1))
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
            seen_ids: set[int] = set()
            for replica_id in replica_ids:
                if replica_id in seen_ids:
                    continue
                seen_ids.add(replica_id)
                info = infos_by_id.get(replica_id)
                if (info is None or info.is_terminal or getattr(
                        info.status_property, 'is_scale_down', False) is True):
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
                        self._terminate_replica(replica_id,
                                                sync_down_logs=False,
                                                replica_drain_delay_seconds=0,
                                                is_scale_down=True,
                                                in_flight_drain_cap_seconds=0)
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
                    self._defer_scale_down_until_idle(
                        replica_id,
                        logical_retirement=(version, reconcile_generation,
                                            target_capacity),
                        replica_info=info)

                # Only mutate the in-memory proof after durable acceptance.
                # If persistence raises, the exception aborts the remainder and
                # the next autoscaler tick retries under a fresh fence.
                committed_capacity -= committed_width
                ready_capacity -= ready_width
                if card is not None:
                    committed_by_accelerator[card] -= committed_width
                    ready_by_accelerator[card] -= ready_width
                accepted += 1

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

    def _cloud_instance_looks_alive(self, info: ReplicaInfo) -> bool:
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
            if handle is None:
                return False
            assert isinstance(handle, backends.CloudVmRayResourceHandle)
            statuses = backend_utils.query_cluster_instance_statuses(handle)
            if len(statuses) < handle.launched_nodes:
                return False
            return all(status == status_lib.ClusterStatus.UP
                       for status, _ in statuses.values())
        except Exception as e:  # pylint: disable=broad-except
            logger.debug(f'Preemption pre-filter failed for replica '
                         f'{info.replica_id} ({info.cluster_name}); treating '
                         f'as alive: {common_utils.format_exception(e)}')
            return True

    def _is_reclaimable_zero_cost_kubernetes(self, info: ReplicaInfo) -> bool:
        """Whether a non-spot replica runs as reclaimable research capacity."""
        placer = getattr(self, '_spot_placer', None)
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
        if not self._is_interruptible_replica(info):
            return False

        # Get cluster handle first for zone information. The following
        # backend_utils.refresh_cluster_status_handle might delete the
        # cluster record from the cluster table.
        handle = global_user_state.get_handle_from_cluster_name(
            info.cluster_name)
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
            cluster_status, _ = backend_utils.refresh_cluster_status_handle(
                info.cluster_name,
                force_refresh_statuses=set(status_lib.ClusterStatus))

            if cluster_status in (status_lib.ClusterStatus.UP,
                                  status_lib.ClusterStatus.AUTOSTOPPING):
                return False
        # The cluster is partially or fully interrupted. It can be down, INIT
        # or STOPPED, based on the provider's interruption behavior.
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
        """Resolve one replica's exact card without folding card variants."""
        accelerators = None
        location = info.get_spot_location()
        if location is not None:
            accelerators = location.accelerators
        if not accelerators:
            accelerators = (getattr(info, 'resources_override', None) or
                            {}).get('accelerators')
        if not accelerators:
            resources = getattr(getattr(info, 'handle', None),
                                'launched_resources', None)
            accelerators = getattr(resources, 'accelerators', None)
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
        if (not getattr(self, '_uses_logical_replicas', False) or
                not getattr(self, '_logical_exact_accelerator_shapes', {})):
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
                             f'{getattr(self, "_pending_version", None)!r}'))
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
            if (info.is_terminal or info.version != version or getattr(
                    info.status_property, 'is_scale_down', False) is True or
                    getattr(info.status_property, 'preempted', False) is True):
                continue
            card = self._replica_card_for_catalog(info, canonical_by_name)
            if card is None:
                if info.replica_id == candidate_replica_id:
                    candidate_summary = (info.replica_id, info.status.value,
                                         info.version, None,
                                         getattr(info, 'planned_capacity',
                                                 None))
                continue
            planned = int(getattr(info, 'planned_capacity', 1))
            is_pending = (
                (info.status == serve_state.ReplicaStatus.PENDING and
                 getattr(info.status_property, 'sky_launch_status', None)
                 in (None, common_utils.ProcessStatus.SCHEDULED)) or
                info.replica_id == candidate_replica_id)
            special_pending = bool(
                getattr(info, 'reserved_fill', False) or
                getattr(info, 'unknown_capacity_replacement', False) or
                type(getattr(info, 'cost_rebalance_for_replica_id',
                             None)) is int)
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
                created_at = getattr(info, 'created_at', None)
                if not isinstance(created_at, (int, float)):
                    created_at = float('-inf')
                return (not bool(getattr(info, 'is_zero_cost', False)),
                        float(created_at), info.replica_id)

            for info in sorted(card_candidates, key=_candidate_key):
                planned = int(getattr(info, 'planned_capacity', 1))
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

    @with_lock
    def _refresh_thread_pool(self) -> None:
        """Route mutation completion through the removable legacy runtime."""
        self._legacy_mutation_runtime_state().refresh(
            self._refresh_legacy_mutation_runtime)

    def _refresh_legacy_mutation_runtime(self) -> None:
        """Refresh the launch/down thread pool.

        This function will checks all sky.launch and sky.down thread on
        the fly. If any of them finished, it will update the status of the
        corresponding replica.
        """
        # TODO(fcapponi): DEPRECATED launch/down mutation owner. Remove its
        # eligible authoritative branches at M5 after action-only execution
        # and the compatible rollback gate are proven.
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
            replica_id for replica_id, t in finished_launches if isinstance(
                getattr(t, 'exception', None), _UnfencedExternalLbLaunchError)
        }
        superseded_launches = {
            replica_id for replica_id, t in finished_launches if isinstance(
                getattr(t, 'exception', None), _ReplicaLaunchSupersededError)
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
                launch_error = getattr(t, 'exception', None)
                if isinstance(launch_error, _ReplicaLaunchCapacityError):
                    previous_reason = failed_spot_locations.get(location)
                    failed_spot_locations[location] = (
                        'quota' if launch_error.reason == 'quota' or
                        previous_reason == 'quota' else 'capacity')
                elif t.format_exc is not None:
                    generic_failed_spot_locations.add(location)
                else:
                    selected_at = getattr(info, 'created_at', None)
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
        capacity_launch_failures: set[int] = set()
        quota_launch_failures: set[int] = set()
        for replica_id, t in finished_launches:
            info = launch_infos.get(replica_id)
            assert info is not None, replica_id
            if info.status == serve_state.ReplicaStatus.PENDING:
                pending_launches.append((replica_id, t, info))
                continue
            if replica_id in superseded_launches:
                rejection = getattr(t, 'exception', None)
                logger.info(
                    f'Cleaning up logical replica {replica_id}: its exact-card '
                    'target was superseded before the first cloud mutation '
                    f'({rejection}).')
                self._terminate_replica(replica_id,
                                        sync_down_logs=False,
                                        replica_drain_delay_seconds=0,
                                        is_scale_down=True,
                                        in_flight_drain_cap_seconds=0)
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
                service_hash=getattr(self, '_service_hash', None),
                controller_owner=getattr(self, '_controller_owner', None),
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
                reconciliation_event = getattr(self,
                                               '_scale_reconciliation_event',
                                               None)
                if reconciliation_event is not None:
                    reconciliation_event.set()
        for replica_id, info, error_in_sky_launch in completed_launches:
            legacy_runtime.launch_thread_pool.pop(replica_id)
            legacy_runtime.replica_to_request_id.pop(replica_id)
            legacy_runtime.replica_to_logical_launch_fence.pop(replica_id)
            if error_in_sky_launch:
                # Teardown after update replica info since
                # _terminate_replica will update the replica info too.
                self._terminate_replica(replica_id,
                                        sync_down_logs=True,
                                        replica_drain_delay_seconds=0)

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
                    getattr(info, 'reserved_fill', False) or
                    getattr(info, 'unknown_capacity_replacement', False) or
                    type(getattr(info, 'cost_rebalance_for_replica_id',
                                 None)) is int)
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
                             location,
                             selected_at=getattr(info, 'created_at', None)))):
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
                    fence_map = getattr(self,
                                        '_replica_to_logical_launch_fence',
                                        None)
                    logical_fence = (None if fence_map is None else
                                     fence_map.get(replica_id))
                    if logical_fence is not None:
                        with self._logical_state_lock:
                            if not self._logical_reconcile_fence_holds(
                                    logical_fence):
                                continue
                    t.start()
                    # This replica is now provisioning; reflect it locally
                    # instead of re-scanning the DB on the next replica.
                    in_flight += 1
                    info.status_property.sky_launch_status = (
                        common_utils.ProcessStatus.RUNNING)
                    self._persist_replica(replica_id, info)
                for replica_id, t, info in down_to_admit:
                    if concurrent_downs >= _MAX_CONCURRENT_DOWNS_PER_SERVICE:
                        break
                    logical_retirement = getattr(info.status_property,
                                                 'logical_retirement_version',
                                                 None) is not None
                    logical_state_guard = (self._logical_state_lock
                                           if logical_retirement else
                                           contextlib.nullcontext())
                    with logical_state_guard:
                        if logical_retirement:
                            recovering_ids: set[int] = getattr(
                                self, '_recovering_logical_retirement_ids',
                                set())
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
                            retirement_version = getattr(
                                status, 'logical_retirement_version', None)
                            confirmed_generation = getattr(
                                status,
                                'logical_retirement_confirmed_generation', None)
                            bounded_deadline = getattr(
                                status, 'logical_retirement_bounded_deadline',
                                False)
                            info_version = getattr(info, 'version', None)
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
                                    in_flight_drain_cap_seconds=getattr(
                                        status, 'drain_cap_seconds', None))
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
        while not self._ownership_lost.is_set():
            _, completion_event = self._launch_completion_state()
            # Clear before draining the durable-in-process queue. A completion
            # racing after this clear is either drained now or leaves the event
            # set so the wait below returns immediately.
            completion_event.clear()
            self._join_notified_launch_workers()
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
            if self._ownership_lost.is_set():
                return
            completion_event.wait(_PROCESS_POOL_REFRESH_INTERVAL)

    def _system_recovery_status_initialized_ids(self) -> set[int]:
        initialized = getattr(self, '_system_recovery_status_initialized', None)
        if initialized is None:
            initialized = set()
            self._system_recovery_status_initialized = initialized
        return initialized

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
                if observation is not None and observation.profile_version == 1:
                    outcome['events'].add('runtime_capability_v1_observed')
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
                          job_lib.JobSystemRecoveryDetailStatus.UNSPECIFIED,
                          job_lib.JobSystemRecoveryDetailStatus.MALFORMED)):
                outcome['teardown'] = True
                outcome['events'].add('evidence_lost')
            if (detail_status ==
                    job_lib.JobSystemRecoveryDetailStatus.UNSPECIFIED):
                outcome['events'].add('status_only_read')
                outcome['events'].add('evidence_lost')
            elif (detail_status ==
                  job_lib.JobSystemRecoveryDetailStatus.MALFORMED):
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
        if updated is None:
            outcome['teardown'] = True
            updated = serve_state.get_replica_info_from_id(
                self._service_name, snapshot.replica_id)
        if updated is None:
            return True
        if (updated.system_recovery_disposition
                != system_recovery_state.SystemRecoveryDisposition.CANDIDATE):
            deadlines = getattr(self, '_candidate_release_monotonic_deadlines',
                                None)
            if deadlines is not None:
                deadlines.pop(updated.replica_id, None)
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
        to_fetch: list[tuple[ReplicaInfo, Any, list[int] | None, bool]] = []
        invalid_recovery_ids: list[int] = []
        for info in infos:
            if not info.status_property.should_track_service_status():
                continue
            cluster_record = cluster_records.get(info.cluster_name)
            handle = None if cluster_record is None else info.handle(
                cluster_record)
            if handle is None:
                # The walk runs lock-free, so the replica's cluster record can
                # vanish mid-walk (a scale-down or preemption cleanup
                # completing after the snapshot was taken). Skip it; the next
                # round re-snapshots.
                if (not self._is_pool and info.system_recovery_disposition in
                    (system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
                     system_recovery_state.SystemRecoveryDisposition.CAPABLE)):
                    invalid_recovery_ids.append(info.replica_id)
                continue
            with_recovery = (
                not self._is_pool and info.system_recovery_disposition
                in (system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
                    system_recovery_state.SystemRecoveryDisposition.CAPABLE))
            if with_recovery:
                service_job_id = info.service_job_id
                if (isinstance(service_job_id, bool) or
                        not isinstance(service_job_id, int) or
                        service_job_id < 1):
                    invalid_recovery_ids.append(info.replica_id)
                    continue
                job_ids: list[int] | None = [service_job_id]
            else:
                job_ids = [1] if self._is_pool else None
            to_fetch.append((info, handle, job_ids, with_recovery))
        for replica_id in invalid_recovery_ids:
            with self.lock:
                fresh = serve_state.get_replica_info_from_id(
                    self._service_name, replica_id)
                if (fresh is None or
                        not fresh.status_property.should_track_service_status()
                        or fresh.system_recovery_disposition not in
                    (system_recovery_state.SystemRecoveryDisposition.CANDIDATE,
                     system_recovery_state.SystemRecoveryDisposition.CAPABLE)):
                    continue
                logger.warning(
                    f'Recovery candidate/capable replica {replica_id} lacks '
                    'its exact cluster handle or service job association.')
                self._terminate_replica(replica_id,
                                        sync_down_logs=True,
                                        replica_drain_delay_seconds=0)
        if not to_fetch:
            return

        def _get_job_status(handle, job_ids, with_recovery):
            # SSH into the replica's head node -- intentionally OUTSIDE
            # self.lock so an unreachable replica cannot wedge the round.
            if with_recovery:
                return backend.get_job_status_with_system_recovery(
                    handle, job_ids, stream_logs=False)
            return (backend.get_job_status(handle, job_ids,
                                           stream_logs=False), {}, {})

        # The fetches are pure I/O; run them in parallel so one hung SSH
        # (preempted spot) delays only its own replica's result, not the
        # whole fleet's failure detection.
        num_fetch_threads = min(len(to_fetch),
                                self._PROBE_ROUND_MAX_PARALLELISM)
        with mp_pool.ThreadPool(num_fetch_threads) as pool:
            fetch_results = [
                (info,
                 pool.apply_async(_get_job_status,
                                  (handle, job_ids, with_recovery)))
                for info, handle, job_ids, with_recovery in to_fetch
            ]
            self._handle_job_status_results(fetch_results)

    def _handle_job_status_results(
            self, fetch_results: list[tuple[ReplicaInfo, Any]]) -> None:
        """Consume the parallel job-status fetches, in submission order."""
        for info, result in fetch_results:
            try:
                result_payload = result.get()
                if (isinstance(result_payload, tuple) and
                        len(result_payload) == 3):
                    (job_statuses, recovery_infos,
                     recovery_detail_statuses) = result_payload
                else:
                    # Compatibility for focused tests and old backend shims.
                    job_statuses = result_payload
                    recovery_infos = {}
                    recovery_detail_statuses = {}
            except exceptions.CommandError:
                # If the job status fetch failed, it is likely that the
                # cluster is preempted.
                with self.lock:
                    # Re-read under the lock: another thread may have
                    # mutated/purged/scheduled-down this replica while we SSHed
                    # lock-free; acting on the stale snapshot could clobber the
                    # newer state or double-terminate.
                    fresh = serve_state.get_replica_info_from_id(
                        self._service_name, info.replica_id)
                    if fresh is None:
                        continue
                    if not fresh.status_property.should_track_service_status():
                        continue
                    is_preempted = self._handle_preemption(fresh)
                    if (not is_preempted and fresh.system_recovery_disposition
                            == system_recovery_state.SystemRecoveryDisposition.
                            CAPABLE and
                            self._system_recovery_status_barrier_expired(fresh)
                       ):
                        self._terminate_replica(fresh.replica_id,
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
                        job_lib.JobSystemRecoveryDetailStatus.UNSPECIFIED)
                with self.lock:
                    if self._reconcile_system_recovery_status(
                            info, job_status, recovery_detail,
                            recovery_detail_status):
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
        while not self._ownership_lost.is_set():
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
            if self._ownership_lost.wait(_JOB_STATUS_FETCH_INTERVAL):
                return

    def _resolve_probe_urls(self,
                            infos: list[ReplicaInfo]) -> dict[int, str | None]:
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
        handles: dict[int, backends.CloudVmRayResourceHandle] = {}
        for info in infos:
            cluster_record = cluster_records.get(info.cluster_name)
            if cluster_record is None:
                continue
            handle = info.handle(cluster_record)
            if handle is None:
                continue
            handles[info.replica_id] = handle
        provider_configs = serve_utils.get_provider_configs_for_handles(handles)

        urls: dict[int, str | None] = {}
        for info in infos:
            cluster_record = cluster_records.get(info.cluster_name)
            handle = handles.get(info.replica_id)
            if cluster_record is None or handle is None:
                urls[info.replica_id] = None
                continue
            urls[info.replica_id] = info._resolve_url(  # pylint: disable=protected-access
                cluster_record=cluster_record,
                handle=handle,
                provider_config=provider_configs.get(info.replica_id),
            )
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
        outcome = {'off_route': True, 'teardown': False, 'released': False}
        deadlines = getattr(self, '_candidate_release_monotonic_deadlines',
                            None)
        if deadlines is None:
            deadlines = {}
            self._candidate_release_monotonic_deadlines = deadlines

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

    @with_lock
    def _probe_all_replicas(self) -> list[ReplicaInfo]:
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
        # Reset the per-tick spec memo so this probe round reads each version's
        # spec from the DB at most once and never reuses a spec across ticks.
        self._tick_version_spec_cache = {}
        probe_futures = []
        replica_to_probe = []
        infos = serve_state.get_replica_infos(self._service_name)
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
        infos_to_probe = [
            info for info in infos
            if info.status_property.should_track_service_status()
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
            probe_urls = self._resolve_probe_urls(infos_to_probe)
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
            for info in candidates:
                record = status_cluster_records.get(info.cluster_name)
                handle = None if record is None else info.handle(record)
                candidate_status_inputs.append((info, handle))
            for replica_id, candidate in route_issue_candidates.items():
                (info, generation, predicted_generation,
                 retry_submitted_adopted_at, route_url, readiness_path,
                 post_data, readiness_headers, job_id) = candidate
                record = status_cluster_records.get(info.cluster_name)
                handle = None if record is None else info.handle(record)
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
                handle: Any,
                job_id: int,
            ) -> tuple[job_lib.JobStatus | None, job_lib.JobSystemRecoveryInfo |
                       None, job_lib.JobSystemRecoveryDetailStatus]:
                if handle is None or job_id < 1:
                    return (None, None,
                            job_lib.JobSystemRecoveryDetailStatus.MALFORMED)
                try:
                    status_payload = (
                        recovery_backend.get_job_status_with_system_recovery(
                            handle, [job_id], stream_logs=False))
                    if status_payload is None:
                        raise ValueError('exact status payload is missing')
                    statuses, recovery_infos, detail_statuses = status_payload
                    return (
                        statuses.get(job_id), recovery_infos.get(job_id),
                        detail_statuses.get(
                            job_id,
                            job_lib.JobSystemRecoveryDetailStatus.MALFORMED))
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
                evidence = _ordered_route_status(handle, job_id)
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
                        self._system_recovery_route_evidence_matches(
                            result_info,
                            *evidence,
                            allow_retry_submitted=predicted_generation)):
                    try:
                        # Process-local issuance is intentionally the sole
                        # worker side effect. Persistence/teardown remains in
                        # the parent thread after it revalidates current state.
                        self._route_lease_registry().issue(
                            result_info.replica_id, generation, exact_route_url,
                            exact_readiness_path, exact_post_data,
                            exact_headers, request_started_at)
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
                result_info, succeeded, probe_time = info.probe_pool()
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

            # Candidate release requires ABSENT + nonterminal status from the
            # exact job in the same reconciliation cycle as the fresh probe.
            # These few short-lived candidates share the probe pool; capable
            # steady-state replicas remain on the normal job-status cadence.
            def _candidate_status(info: ReplicaInfo, handle: Any) -> Any:
                if (handle is None or isinstance(info.service_job_id, bool) or
                        not isinstance(info.service_job_id, int) or
                        info.service_job_id < 1):
                    return None
                return recovery_backend.get_job_status_with_system_recovery(
                    handle, [info.service_job_id], stream_logs=False)

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
                try:
                    status_payload = status_future.get()
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
                        job_id,
                        job_lib.JobSystemRecoveryDetailStatus.UNSPECIFIED)
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
                        self._cloud_instance_looks_alive,
                        failed_interruptible_infos)
                possibly_preempted_ids = {
                    failed_info.replica_id for failed_info, alive in zip(
                        failed_interruptible_infos, alive_flags) if not alive
                }

            pending_writes: list[tuple[int, ReplicaInfo]] = []
            replicas_to_teardown: list[int] = []
            preempted_replica_ids: set[int] = set()
            terminal_route_ids: set[int] = set()
            for future_result in probe_results:
                (info, probe_succeeded, probe_time, probe_monotonic_started_at,
                 _, _) = future_result
                should_teardown = False
                if (not probe_succeeded and
                        info.replica_id in possibly_preempted_ids):
                    # Durable legacy interruption/down intent wins before any
                    # OOM observation or probe reduction can revive routing.
                    is_preempted = self._handle_preemption(info)
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
                    should_teardown = (should_teardown or candidate_teardown)

                route_evidence = ordered_route_evidence.get(info.replica_id)
                if route_evidence is not None:
                    if self._reconcile_system_recovery_status(
                            info, *route_evidence):
                        terminal_route_ids.add(info.replica_id)
                        self._route_lease_registry().deactivate(info.replica_id)
                        continue
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
                            and self._issue_system_recovery_route(
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
            if pending_route_suspensions:
                transferred_route_suspensions = list(pending_route_suspensions)
                pending_route_suspensions.clear()
                self._persist_replicas(
                    pending_writes,
                    route_suspensions=transferred_route_suspensions)
            else:
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
            if (expected_status_epoch is not None and
                    expected_status_epoch != getattr(
                        self, '_status_epoch_generation', 0)):
                return
            for _ in range(2):
                with self._get_target_num_replicas_lock():
                    target_num_replicas = getattr(self, '_target_num_replicas',
                                                  None)
                    update_mode = self._update_mode
                    target_generation = getattr(
                        self, '_target_num_replicas_generation', 0)
                serve_utils.set_service_status_and_active_versions_from_replica(
                    self._service_name,
                    replica_infos,
                    update_mode,
                    target_num_replicas=target_num_replicas,
                    **self._db_fence_kwargs())
                with self._get_target_num_replicas_lock():
                    if target_generation == getattr(
                            self, '_target_num_replicas_generation', 0):
                        return
                ownership_lost = getattr(self, '_ownership_lost', None)
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
            while not self._ownership_lost.is_set():
                delay = next_start - loop.time()
                if delay > 0:
                    await asyncio.sleep(delay)
                    if self._ownership_lost.is_set():
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
        while not self._ownership_lost.is_set():
            logger.debug('Running replica prober.')
            try:
                with self._get_status_epoch_lock():
                    status_epoch = getattr(self, '_status_epoch_generation', 0)
                # Reuse the probe round's end-of-round snapshot instead of
                # re-reading (and re-deserializing) the whole fleet from the
                # DB a second time per tick.
                replica_infos = self._probe_all_replicas()
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
            if self._ownership_lost.wait(
                    self._get_endpoint_probe_interval_seconds()):
                return

    def get_active_replica_urls(self) -> list[str]:
        """Get the urls of all active replicas."""
        record = serve_state.get_service_from_name(self._service_name)
        assert record is not None, (f'{self._service_name} not found on '
                                    'controller records.')
        ready_replica_urls = []
        active_versions = set(record['active_versions'])
        for info in serve_state.get_replica_infos(self._service_name):
            if (info.status == serve_state.ReplicaStatus.READY and
                    info.version in active_versions and
                    self.system_recovery_allows_routing(info)):
                assert info.url is not None, info
                ready_replica_urls.append(info.url)
        return ready_replica_urls

    def _route_lease_registry(
            self) -> system_recovery_route_lease.ManagerRouteLeaseRegistry:
        registry = getattr(self, '_system_recovery_route_registry', None)
        if registry is None:
            registry = (system_recovery_route_lease.ManagerRouteLeaseRegistry())
            self._system_recovery_route_registry = registry
        return registry

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
        epoch = getattr(self, '_system_recovery_route_epoch', None)
        if not isinstance(epoch, str):
            epoch = str(uuid.uuid4())
            self._system_recovery_route_epoch = epoch
        try:
            return system_recovery_route_lease.RouteGeneration(
                controller_epoch=epoch,
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
        generation = self._system_recovery_route_generation(info)
        if (generation is None or evidence is None or
                not self._system_recovery_route_evidence_matches(
                    info, *evidence)):
            return False
        spec = self._get_version_spec(info.version)
        try:
            issued = self._route_lease_registry().issue(
                info.replica_id, generation, route_url, spec.readiness_path,
                spec.post_data, spec.readiness_headers, normal_probe_started_at)
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
    ) -> None:
        if version <= self.latest_version:
            logger.error(f'Invalid version: {version}, '
                         f'latest version: {self.latest_version}')
            return
        new_yaml_content = serve_state.get_yaml_content(self._service_name,
                                                        version)
        assert new_yaml_content is not None, (
            f'yaml content not found for {self._service_name} version {version}'
        )
        new_uses_logical_replicas = (getattr(spec, 'uses_logical_replicas',
                                             False) is True)
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
        new_placer_name = getattr(spec, 'spot_placer', None)
        if ((new_uses_logical_replicas or isinstance(new_placer_name, str)) and
                new_spot_placer is None):
            new_spot_placer = _load_spot_placer(
                self._service_name, version, spec, new_task,
                getattr(self, '_workspace', None))
        old_spot_placer = getattr(self, '_spot_placer', None)
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
        version_specs = getattr(self, '_version_specs', None)
        if version_specs is None:
            # Compatibility for embedders and legacy tests that construct a
            # manager without running the current constructor.
            version_specs = {}
            self._version_specs = version_specs
        version_specs[version] = spec
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
                prior_is_logical = (getattr(prior_spec, 'uses_logical_replicas',
                                            False) is True)
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
