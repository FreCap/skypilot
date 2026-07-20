"""ReplicaManager: handles the creation and deletion of endpoint replicas."""
from collections.abc import Callable
from collections.abc import Mapping
import contextlib
import dataclasses
import functools
import math
from multiprocessing import pool as mp_pool
import os
import pathlib
import threading
import time
import traceback
import typing
from typing import Any, Optional
import uuid

import colorama
import filelock
import requests

from sky import backends
from sky import estimated_spend
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky import skypilot_config
from sky import task as task_lib
from sky.backends import backend_utils
from sky.client import sdk
from sky.serve import constants as serve_constants
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
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

logger = sky_logging.init_logger(__name__)

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

# Sentinel for to_info_dict's pre-fetched cluster_record
# parameters. We can't use None because None is a legitimate value (it means
# "no cluster row" / "no handle"). The sentinel lets callers opt in to the
# batched fetch path while preserving the existing self-fetch behavior for
# back-compat callers like ReplicaInfo.__repr__.
_NOT_PROVIDED: Any = object()
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


@dataclasses.dataclass(frozen=True)
class LogicalReconcileSnapshot:
    """One immutable LB capacity and occupancy generation."""

    version: int
    generation: int
    observed_slots_by_replica_id: dict[int, int]
    in_flight_by_replica_id: dict[int, int]
    unknown_replica_ids: frozenset[int]
    received_at: float


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


class _UnfencedExternalLbLaunchError(RuntimeError):
    """A legacy controller cannot satisfy the API replica-launch fence."""


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
        continue_guard: Callable[[], bool] | None = None,
        launch_fence: dict[str, Any] | None = None,
        service_spec: 'service_spec.SkyServiceSpec | None' = None) -> None:
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
        task = load_task_with_service_spec(yaml_content, service_spec)
        if resources_override is not None:
            resources = task.resources
            if exact_resources_override:
                # Spot placement has already selected the complete launch
                # location and shape. All entries have the same non-location
                # fields (validated by SpotPlacer), so one is sufficient.
                # Keeping the whole any_of set here would turn N entries into
                # N identical pinned candidates and make sky.launch retry the
                # same unavailable zone N times before reporting failure.
                resource = next(iter(resources)).copy(**resources_override)
                task.set_resources(resource)
            else:
                overrided_resources = [
                    r.copy(**resources_override) for r in resources
                ]
                task.set_resources(type(resources)(overrided_resources))
        task.update_envs({serve_constants.REPLICA_ID_ENV_VAR: str(replica_id)})

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

    def _stream_with_owner_watchdog(request_id: Any) -> None:
        """Cancel an async launch promptly when the shared owner fence trips."""
        if continue_guard is None:
            sdk.stream_and_get(request_id)
            return
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
            sdk.stream_and_get(request_id)
        finally:
            stop_watchdog.set()
            watchdog.join(timeout=1)

    if availability_max_retry is None:
        availability_max_retry = max_retry
    retry_cnt = 0
    availability_retry_cnt = 0
    backoff = common_utils.Backoff(_RETRY_INIT_GAP_SECONDS)
    while True:
        retry_cnt += 1
        try:
            if _check_is_cancelled():
                return
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
            if launch_fence is not None:
                launch_kwargs['_extra_launch_context'] = launch_fence
            request_id = sdk.launch(task,
                                    cluster_name,
                                    retry_until_up=retry_until_up,
                                    _is_launched_by_sky_serve_controller=True,
                                    **launch_kwargs)
            logger.info(f'Replica cluster {cluster_name} launch requested '
                        f'with request_id: {request_id}.')
            replica_to_request_id[replica_id] = request_id
            _stream_with_owner_watchdog(request_id)
            _assert_launch_authorized()
            logger.info(f'Replica cluster {cluster_name} launched.')
        except _ReplicaLaunchOwnershipLostError:
            # In-memory request/cancellation entries belong to this stale
            # manager only. Keep the durable replica row for the successor to
            # re-drive or garbage-collect; discard local bookkeeping.
            replica_to_request_id.pop(replica_id)
            replica_to_launch_cancelled.pop(replica_id)
            raise
        except _UnfencedExternalLbLaunchError:
            raise
        except (exceptions.InvalidClusterNameError,
                exceptions.NoCloudAccessError,
                exceptions.ResourcesMismatchError) as e:
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
            if not any(
                    isinstance(err, exceptions.ResourcesUnavailableError)
                    for err in e.failover_history):
                raise RuntimeError('Failed to launch the sky serve replica '
                                   f'cluster {cluster_name}.') from e
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

        # Cleanup the request id and the failed cluster.
        replica_to_request_id.pop(replica_id)
        # If it is cancelled, no need to terminate the cluster. It will be
        # handled by the termination thread.
        if _check_is_cancelled():
            return
        terminate_cluster(cluster_name, log_file=log_file)

        if (retry_cnt >= max_retry or
                availability_retry_cnt >= availability_max_retry):
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


def _is_valid_drain_started_at(value: Any) -> bool:
    return (not isinstance(value, bool) and isinstance(value, (int, float)) and
            math.isfinite(value) and value > 0)


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
                      continue_guard: Callable[[], bool] | None = None) -> None:
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
                core.down(cluster_name)
            logger.info(f'Replica cluster {cluster_name} terminated.')
            return
        except exceptions.ClusterDoesNotExist:
            # The cluster is already terminated.
            logger.info(
                f'Replica cluster {cluster_name} is already terminated.')
            return
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
    """Get the resources ports used by the task."""
    task = load_task_with_service_spec(yaml_content, service_spec)
    # Already checked all ports are valid in sky.serve.core.up
    assert task.resources, task
    assert task.service is not None, task
    if task.service.pool:
        return '-'
    assert task.service.ports is not None, task
    return task.service.ports


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
    _VERSION = 10

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
        # Incumbent id this replica was launched to replace economically.
        # None for ordinary demand/fill launches.
        self.cost_rebalance_for_replica_id: int | None = None

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
            'cost_rebalance_for_replica_id': getattr(
                self, 'cost_rebalance_for_replica_id', None),
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
        replica.cost_rebalance_for_replica_id = state.get(
            'cost_rebalance_for_replica_id')
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
        return self.status == serve_state.ReplicaStatus.READY

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
        # If replica doesn't start with http or https, add http://
        if not endpoint.startswith('http'):
            endpoint = 'http://' + endpoint
        return endpoint

    @property
    def url(self) -> str | None:
        return self._resolve_url()

    @property
    def status(self) -> serve_state.ReplicaStatus:
        replica_status = self.status_property.to_replica_status()
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
            # TODO(tian): Support HTTPS in the future.
            if url is None:
                logger.info(f'Error when probing {replica_identity}: '
                            'Cannot get the endpoint.')
                return self, False, probe_time
            readiness_path = (f'{url}{readiness_path}')
            logger.info(f'Probing {replica_identity} with {readiness_path}.')
            if post_data is not None:
                msg += 'POST'
                response = requests.post(readiness_path,
                                         json=post_data,
                                         headers=headers,
                                         timeout=timeout)
            else:
                msg += 'GET'
                response = requests.get(readiness_path,
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
        except requests.exceptions.RequestException as e:
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

        state.setdefault('cost_rebalance_for_replica_id', None)

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
        self._logical_target: tuple[int, int, int] | None = None
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

    def publish_logical_target(self, version: int, generation: int,
                               target_capacity: int) -> None:
        """Publish the target computed from an exact reconcile generation."""
        with self._logical_state_lock:
            self._logical_target = (version, generation, target_capacity)

    def _logical_target_fence_holds(
            self,
            version: int,
            decision_generation: int,
            target_capacity: int,
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
                self._logical_target == (version, decision_generation,
                                         target_capacity))

    @staticmethod
    def _logical_snapshot_is_fresh(snapshot: LogicalReconcileSnapshot) -> bool:
        return (time.monotonic() - snapshot.received_at
                <= 3 * serve_constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)

    @property
    def spot_placer(self) -> Optional['spot_placer.SpotPlacer']:
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

    def scale_up_batch(self,
                       resources_overrides: list[dict[str, Any] | None],
                       expected_version: int | None = None) -> None:
        """Scale up by len(resources_overrides) replicas in one batch.

        Subclasses may override to amortize per-call synchronization; the
        default just loops over `scale_up`.
        """
        if (expected_version is not None and
                expected_version != self.latest_version):
            return
        for resources_override in resources_overrides:
            self.scale_up(resources_override)

    def scale_up_to_logical_capacity(self, target_capacity: int, version: int,
                                     reconcile_generation: int) -> None:
        """Persist complete backend shapes until target capacity is covered."""
        raise NotImplementedError

    def notify_version_pending(self, version: int) -> None:
        """Notify long manager operations that a newer version is waiting."""

    def clear_pending_version(self, version: int) -> None:
        """Clear a previously announced pending version."""

    def scale_down(self,
                   replica_id: int,
                   purge: bool = False,
                   wait_for_idle: bool = False,
                   expected_version: int | None = None) -> None:
        """Scale down replica with replica_id."""
        raise NotImplementedError

    def scale_down_logically(self, replica_id: int, target_capacity: int,
                             version: int, reconcile_generation: int) -> None:
        """Retire one backend only if the logical coverage fence still holds."""
        raise NotImplementedError

    def scale_down_logically_batch(self, replica_ids: list[int],
                                   target_capacity: int, version: int,
                                   reconcile_generation: int) -> None:
        """Retire logical backends selected from one reconcile generation.

        Subclasses may override to amortize synchronization and fleet reads.
        The compatibility path preserves the singleton behavior.
        """
        for replica_id in replica_ids:
            self.scale_down_logically(replica_id, target_capacity, version,
                                      reconcile_generation)

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        raise NotImplementedError

    def get_active_replica_urls(self) -> list[str]:
        """Get the urls of the active replicas."""
        raise NotImplementedError


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
        return authorized

    def _service_is_launch_authorized(self) -> bool:
        """Fail one launch closed unless ownership is currently proven."""
        return self._service_launch_authorization() is True

    def _launch_owner_watchdog_allows_continue(self) -> bool:
        """Cheap shared fence polled by every in-flight launch request."""
        ownership_lost = getattr(self, '_ownership_lost', None)
        return ownership_lost is None or not ownership_lost.is_set()

    def _replica_launch_fence_context(self) -> dict[str, Any] | None:
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
        return {
            service_name_key: self._service_name,
            service_hash_key: service_hash,
            controller_pid_key: controller_pid,
            controller_ip_key: controller_ip,
        }

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

    def _persist_replica(self, replica_id: int, info: ReplicaInfo) -> None:
        persisted = serve_state.add_or_update_replica(self._service_name,
                                                      replica_id, info,
                                                      **self._db_fence_kwargs())
        if persisted is False:
            raise RuntimeError(
                f'Service {self._service_name!r} incarnation changed while '
                f'persisting replica {replica_id}.')

    def _persist_replicas(self,
                          replica_infos: list[tuple[int, ReplicaInfo]]) -> None:
        persisted = serve_state.add_or_update_replicas(
            self._service_name, replica_infos, **self._db_fence_kwargs())
        if persisted is False:
            raise RuntimeError(
                f'Service {self._service_name!r} incarnation changed while '
                'persisting a replica batch.')

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

    def _remove_replica(self, replica_id: int) -> None:
        removed = serve_state.remove_replica(self._service_name, replica_id,
                                             **self._db_fence_kwargs())
        if removed is False:
            raise RuntimeError(
                f'Service {self._service_name!r} incarnation changed while '
                f'removing replica {replica_id}.')

    def _failed_cleanup_retry_state(
            self) -> tuple[dict[int, int], dict[int, float]]:
        """Return retry maps, tolerating managers built before these fields.

        Normal construction initializes both maps in ``__init__``.  Keeping
        this accessor backward-compatible also protects lightweight embedders,
        tests, and upgrade/recovery paths that reconstruct a manager without
        replaying the newest initializer in full.
        """
        attempts: dict[int, int] | None = getattr(
            self, '_failed_cleanup_retry_attempts', None)
        retry_at: dict[int, float] | None = getattr(self,
                                                    '_failed_cleanup_retry_at',
                                                    None)
        if attempts is None:
            attempts = {}
            self._failed_cleanup_retry_attempts = attempts
        if retry_at is None:
            retry_at = {}
            self._failed_cleanup_retry_at = retry_at
        return attempts, retry_at

    def _clear_failed_cleanup_retry(self, replica_id: int) -> None:
        """Forget in-memory cleanup rate limiting after confirmed success."""
        attempts, retry_at = self._failed_cleanup_retry_state()
        attempts.pop(replica_id, None)
        retry_at.pop(replica_id, None)

    def _schedule_failed_cleanup_retry(self, replica_id: int) -> None:
        """Rate-limit, but never give up on, a durable cleanup failure."""
        attempts, retry_at = self._failed_cleanup_retry_state()
        attempt = attempts.get(replica_id, 0) + 1
        attempts[replica_id] = attempt
        exponential_step = min(attempt - 1, 30)
        delay_seconds = min(
            _FAILED_CLEANUP_RETRY_BASE_SECONDS * 2**exponential_step,
            _FAILED_CLEANUP_RETRY_MAX_SECONDS)
        retry_at[replica_id] = time.monotonic() + delay_seconds
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
        self._spot_placer: spot_placer.SpotPlacer | None = (
            spot_placer.SpotPlacer.from_task(spec, task))
        if self._uses_logical_replicas:
            _validate_logical_capacity_sources(self._default_planned_capacity,
                                               self._spot_placer,
                                               task.num_nodes)
        self._fill_skip_last_log_time: float = 0.0
        # TODO(tian): Store launch/down request id in the replica table, to make
        # the manager more persistent.
        self._launch_thread_pool: thread_utils.ThreadSafeDict[
            int, thread_utils.SafeThread] = thread_utils.ThreadSafeDict()
        self._replica_to_request_id: thread_utils.ThreadSafeDict[
            int, str] = thread_utils.ThreadSafeDict()
        self._replica_to_launch_cancelled: thread_utils.ThreadSafeDict[
            int, bool] = thread_utils.ThreadSafeDict()
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
        self._down_thread_pool: thread_utils.ThreadSafeDict[
            int, thread_utils.SafeThread] = thread_utils.ThreadSafeDict()
        self._failed_cleanup_retry_attempts = {}
        self._failed_cleanup_retry_at = {}
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
                                             'replica-thread-pool-refresher')
        thread_utils.start_supervised_thread(self._job_status_fetcher,
                                             'replica-job-status-fetcher')
        thread_utils.start_supervised_thread(self._replica_prober,
                                             'replica-prober')

    def _recover_replica_operations(self):
        """Re-drive interrupted replica operations from durable state.

        Runs in the dedicated recovery thread started by __init__, which
        holds the manager lock for the whole pass (see __init__ for the
        lock-ordering handshake with the daemon threads)."""
        if self._launch_thread_pool or self._down_thread_pool:
            # Only possible on a RETRY of a partially-completed recovery
            # pass: the per-replica enqueues below skip anything already in
            # the pools, so re-running is safe.
            logger.warning('Recovery pass re-entered with '
                           f'{len(self._launch_thread_pool)} launch / '
                           f'{len(self._down_thread_pool)} down threads '
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
                    'prior_planned_capacity': prior_planned_capacity,
                    'prior_unknown_capacity_replacement': bool(
                        getattr(replica_info, 'unknown_capacity_replacement',
                                False)),
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
                            replica_info.replica_id)
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
        prior_cost_rebalance_for_replica_id: int | None = None,
        prior_planned_capacity: int | None = None,
        prior_unknown_capacity_replacement: bool = False,
        prior_version: int | None = None,
        prior_yaml_content: str | None = None,
        zero_cost_demand_budget: _ZeroCostDemandBudget | None = None,
        recovering_existing_replica: bool = False,
        logical_reconcile_fence: tuple[int, int, int] | None = None,
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
        """
        if replica_id in self._launch_thread_pool:
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
                    self._spot_placer.select_next_zero_cost_location())
                if zero_cost_location is None:
                    self._log_fill_skip(
                        'no ACTIVE zero-cost location available')
                    return False
                location = zero_cost_location
            elif self._demand_should_skip_zero_cost(existing_replica_infos):
                # The broker grant or speculative-probe budget says this
                # demand launch should compete on paid capacity instead of
                # preferring the zero-cost tier.  The placer falls back to
                # zero-cost when no paid candidate exists.
                location = self._spot_placer.select_next_location(
                    skip_zero_cost_preference=True)
                if (zero_cost_demand_budget is not None and
                        location in self._spot_placer.zero_cost_locations()):
                    budgeted_location = self._select_budgeted_zero_cost_location(
                        zero_cost_demand_budget)
                    if budgeted_location is None:
                        logger.info('Deferring demand launch because the '
                                    'shared zero-cost GPU budget is exhausted '
                                    'and no paid location is active.')
                        return False
                    location = budgeted_location
            elif zero_cost_demand_budget is not None:
                location = self._select_budgeted_zero_cost_location(
                    zero_cost_demand_budget)
                if location is None:
                    location = self._spot_placer.select_next_location(
                        skip_zero_cost_preference=True)
                    if location in self._spot_placer.zero_cost_locations():
                        # A successful zero (or an exhausted speculative
                        # allowance) is authoritative. If no paid candidate is
                        # active, defer instead of falling through into the
                        # same saturated research pool.
                        logger.info('Deferring demand launch because the '
                                    'shared zero-cost GPU budget is exhausted '
                                    'and no paid location is active.')
                        return False
            elif self._demand_should_skip_saturated_zero_cost(
                    existing_replica_infos):
                location = self._spot_placer.select_next_location(
                    skip_zero_cost_preference=True)
            else:
                location = self._spot_placer.select_next_location()
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
        if logical_reconcile_fence is not None:
            fence_version, fence_generation, fence_target = (
                logical_reconcile_fence)
            if not self._logical_target_fence_holds(
                    fence_version,
                    fence_generation,
                    fence_target,
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
        t = thread_utils.SafeThread(
            target=launch_cluster,
            args=(replica_id, launch_yaml_content, cluster_name, log_file_name,
                  self._replica_to_request_id,
                  self._replica_to_launch_cancelled, resources_override,
                  retry_until_up),
            kwargs={
                'availability_max_retry': availability_max_retry,
                'exact_resources_override': location is not None,
                'pre_launch_guard': self._service_is_launch_authorized,
                'continue_guard': self._launch_owner_watchdog_allows_continue,
                'launch_fence': self._replica_launch_fence_context(),
                'service_spec': launch_spec,
            },
        )
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
        # Persisted launch-origin attribution: the broker's holdings split
        # and the ceiling's demand exemption key on this flag. OR in the
        # replaced row's attribution on recovery re-drives (the sentinel
        # only exists at original emission).
        info.reserved_fill = bool(zero_cost_only or prior_reserved_fill)
        info.cost_rebalance_for_replica_id = (cost_rebalance_for_replica_id)
        logical_state_guard = (self._logical_state_lock
                               if logical_reconcile_fence is not None else
                               contextlib.nullcontext())
        with logical_state_guard:
            if logical_reconcile_fence is not None:
                fence_version, fence_generation, fence_target = (
                    logical_reconcile_fence)
                if not self._logical_target_fence_holds(
                        fence_version,
                        fence_generation,
                        fence_target,
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
                self._persist_replica(replica_id, info)
            if info.unknown_capacity_replacement:
                replacement_ids = getattr(self,
                                          '_unknown_capacity_replacement_ids',
                                          None)
                if replacement_ids is None:
                    replacement_ids = set()
                    self._unknown_capacity_replacement_ids = replacement_ids
                replacement_ids.add(replica_id)
        if existing_replica_infos is not None:
            # Bulk callers (recovery re-drive) reuse one snapshot across a
            # whole wave of launches. Append each accepted replica so shared
            # zero-cost capacity accounting sees the in-wave reservations.
            existing_replica_infos.append(info)
        # Don't start right now; we will start it later in _refresh_thread_pool
        # to avoid too many sky.launch running at the same time.
        self._launch_thread_pool[replica_id] = t
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
            self, budget: _ZeroCostDemandBudget) -> spot_placer.Location | None:
        """Reserve and select one location from a measured batch budget."""
        if self._spot_placer is None:
            return None
        allowed = set()
        for location in self._spot_placer.zero_cost_locations():
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
        logical_reconcile_fence: tuple[int, int, int] | None = None,
        logical_reconcile_fence_requires_exact_generation: bool = False,
        unknown_capacity_replacement: bool = False,
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
            launched = self._launch_replica(self._next_replica_id,
                                            resources_override)
        else:
            launch_kwargs: dict[str, Any] = {
                'existing_replica_infos': existing_replica_infos
            }
            if zero_cost_demand_budget is not None:
                launch_kwargs['zero_cost_demand_budget'] = (
                    zero_cost_demand_budget)
            if logical_reconcile_fence is not None:
                launch_kwargs['logical_reconcile_fence'] = (
                    logical_reconcile_fence)
                launch_kwargs[
                    'logical_reconcile_fence_requires_exact_generation'] = (
                        logical_reconcile_fence_requires_exact_generation)
            if unknown_capacity_replacement:
                launch_kwargs['prior_unknown_capacity_replacement'] = True
            launched = self._launch_replica(self._next_replica_id,
                                            resources_override, **launch_kwargs)
        if launched:
            self._next_replica_id += 1
        return launched

    @with_lock
    def scale_up(self,
                 resources_override: dict[str, Any] | None = None) -> None:
        self._scale_up_one_locked(
            resources_override, serve_state.get_replica_ids(self._service_name))

    @with_lock
    def scale_up_batch(self,
                       resources_overrides: list[dict[str, Any] | None],
                       expected_version: int | None = None) -> None:
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
        needs_reservation = (
            self._batch_needs_placement_snapshot(resources_overrides) and
            self._uses_shared_zero_cost_demand_budget())
        if not needs_reservation:
            self._scale_up_batch_locked(resources_overrides, expected_version)
            return
        try:
            lock = locks.get_lock(
                serve_constants.DEMAND_CAPACITY_RESERVATION_LOCK_ID)
            with lock.acquire(blocking=False):
                self._scale_up_batch_locked(resources_overrides,
                                            expected_version)
        except locks.LockTimeout:
            logger.info('Deferring demand scale-up because another service '
                        'is reserving shared zero-cost capacity.')

    def _scale_up_batch_locked(self,
                               resources_overrides: list[dict[str, Any] | None],
                               expected_version: int | None = None) -> None:
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
        for resources_override in resources_overrides:
            pending_version = getattr(self, '_pending_version', None)
            if (pending_version is not None and
                    pending_version > batch_version):
                logger.info('Stopping version '
                            f'{batch_version} scale-up batch because version '
                            f'{pending_version} is waiting to be applied.')
                break
            self._scale_up_one_locked(resources_override, used_replica_ids,
                                      existing_replica_infos,
                                      zero_cost_demand_budget)

    @with_lock
    def scale_up_to_logical_capacity(
        self,
        target_capacity: int,
        version: int,
        reconcile_generation: int,
        replace_unknown_replica_ids: tuple[int, ...] = ()
    ) -> None:
        """Plan and persist complete backend shapes up to a logical target.

        Selection and row persistence share the manager lock and one mutable
        fleet snapshot. Each persisted backend immediately participates in the
        next placement decision, so a single 8-slot choice removes eight slots
        from the shortfall instead of causing eight physical launches.
        """
        if not self._uses_logical_replicas:
            raise RuntimeError('Logical scale target sent to a physical '
                               'replica service.')
        if not self._logical_target_fence_holds(version, reconcile_generation,
                                                target_capacity):
            logger.info('Discarding stale logical scale-up intent for '
                        f'version {version}, generation '
                        f'{reconcile_generation}.')
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

        if not self._uses_shared_zero_cost_demand_budget():
            self._scale_up_to_logical_capacity_locked(
                target_capacity, version, reconcile_generation, snapshot,
                replace_unknown_replica_ids)
            return
        try:
            lock = locks.get_lock(
                serve_constants.DEMAND_CAPACITY_RESERVATION_LOCK_ID)
            with lock.acquire(blocking=False):
                self._scale_up_to_logical_capacity_locked(
                    target_capacity, version, reconcile_generation, snapshot,
                    replace_unknown_replica_ids)
        except locks.LockTimeout:
            logger.info('Deferring logical scale-up because another service '
                        'is reserving shared zero-cost capacity.')

    def _scale_up_to_logical_capacity_locked(
            self, target_capacity: int, version: int, reconcile_generation: int,
            snapshot: LogicalReconcileSnapshot,
            replace_unknown_replica_ids: tuple[int, ...]) -> None:
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

        committed = _committed_capacity(snapshot)
        zero_cost_demand_budget = None
        if infos_by_service is not None:
            capacity_replica_infos = [
                info for infos in infos_by_service.values() for info in infos
            ]
            zero_cost_demand_budget = self._build_zero_cost_demand_budget(
                existing_replica_infos, [None],
                demand_count_override=target_capacity - committed,
                capacity_replica_infos=capacity_replica_infos)
        while True:
            if not self._logical_target_fence_holds(
                    version,
                    reconcile_generation,
                    target_capacity,
                    require_exact_generation=bool(replace_unknown_replica_ids)):
                logger.info('Stopping logical scale-up batch after its '
                            'reconciliation fence advanced.')
                break
            current_snapshot = self._logical_reconcile_snapshot
            assert current_snapshot is not None
            committed = _committed_capacity(current_snapshot)
            if committed >= target_capacity:
                break
            before = len(existing_replica_infos)
            launch_kwargs: dict[str, Any] = {}
            if replace_unknown_replica_ids:
                launch_kwargs['unknown_capacity_replacement'] = True
                launch_kwargs[
                    'logical_reconcile_fence_requires_exact_generation'] = True
            launched = self._scale_up_one_locked(
                None,
                used_replica_ids,
                existing_replica_infos,
                zero_cost_demand_budget,
                logical_reconcile_fence=(version, reconcile_generation,
                                         target_capacity),
                **launch_kwargs)
            if not launched or len(existing_replica_infos) == before:
                logger.info('Logical scale-up made no placement progress; '
                            'retrying on the next reconciliation tick.')
                break

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

    def _handle_sky_down_finish(self, info: ReplicaInfo,
                                format_exc: str | None) -> None:
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
            self._remove_replica(info.replica_id)
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
        left_in_record = not (is_scale_down or purge)
        if left_in_record:
            assert sync_down_logs, (
                'For the replica left in the record, '
                'the logs should always be synced down. '
                'So that the user can see the logs to debug.')

        if replica_id in self._launch_thread_pool:
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
            launch_thread = self._launch_thread_pool[replica_id]
            if launch_thread.is_alive():
                self._replica_to_launch_cancelled[replica_id] = True
                wait_deadline = (time.monotonic() +
                                 _WAIT_LAUNCH_THREAD_TIMEOUT_SECONDS)
                timeout_reached = False
                while True:
                    # Launch request id found. cancel it.
                    if replica_id in self._replica_to_request_id:
                        request_id = self._replica_to_request_id[replica_id]
                        sdk.api_cancel(request_id)
                        break
                    if replica_id not in self._replica_to_launch_cancelled:
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
            self._launch_thread_pool.pop(replica_id)
            self._replica_to_request_id.pop(replica_id)

        if replica_id in self._down_thread_pool:
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
            job_ids = ['1'] if self._is_pool else None
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
        info = serve_state.get_replica_info_from_id(self._service_name,
                                                    replica_id)
        assert info is not None

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
            },
        )
        self._down_thread_pool[replica_id] = t

    def _reconcile_failed_cleanup(self,
                                  replica_infos: list[ReplicaInfo]) -> None:
        """Re-drive every durable cleanup failure until absence is proven."""
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
            if (replica_id in self._down_thread_pool or
                    replica_id in self._launch_thread_pool):
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
                    drain_cap = self._resolve_drain_cap_seconds(replica_id)
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

    def _resolve_drain_cap_seconds(self, replica_id: int) -> int:
        """Drain cap for retiring this replica, per its own version spec.

        An outdated replica retired by a rolling update drains per the
        spec it was serving under. Spec lookup failures fall back to the
        default cap -- a drain regression must never block a teardown.
        """
        try:
            info = serve_state.get_replica_info_from_id(self._service_name,
                                                        replica_id)
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
            drain_cap = self._resolve_drain_cap_seconds(info.replica_id)
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
            self._resolve_drain_cap_seconds(replica_id))
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
        target_state = self._logical_target
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
        target_version, target_generation, current_target = target_state
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
        ready_capacity = self._logical_ready_capacity(replica_infos, snapshot,
                                                      version, excluded_ids)
        if ready_capacity < current_target:
            return 'abort'
        if (require_victim_idle and
                not self._logical_retirement_victim_is_idle(info, snapshot)):
            return 'wait'
        return 'safe'

    @staticmethod
    def _logical_ready_capacity(
            replica_infos: list[ReplicaInfo],
            snapshot: LogicalReconcileSnapshot, version: int,
            excluded_replica_ids: set[int] | frozenset[int]) -> int:
        """Return freshly observed ready capacity from one fleet snapshot."""
        ready_capacity = 0
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
                continue
            ready_capacity += min(
                int(getattr(candidate, 'planned_capacity', 1)), observed)
        return ready_capacity

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
                target_state = self._logical_target
                if (snapshot is None or target_state is None or
                        not self._logical_snapshot_is_fresh(snapshot) or
                        snapshot.version != self.latest_version):
                    continue
                target_version, target_generation, current_target = target_state
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
            target_state = self._logical_target
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
            target_version, target_generation, current_target = target_state
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
                replica_infos, snapshot, self.latest_version,
                frozenset(recovering_ids))
            if ready_capacity < current_target:
                shortfall = current_target - ready_capacity
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
                    self._abort_logical_retirement(
                        info,
                        'current ready capacity is below the recovered target')
                    recovering_ids.discard(info.replica_id)
                    reactivated_capacity += (
                        self._logical_planned_capacity(info)
                        if info.version == self.latest_version else 1)
                    reactivated_count += 1
                    if (reactivated_capacity >= shortfall or
                            reactivated_count >=
                            _LOGICAL_RETIREMENT_RECOVERY_MAX_REACTIVATIONS_PER_GENERATION
                       ):
                        break
                if reactivated_count:
                    logger.info(
                        f'Reactivated {reactivated_count} recovered logical '
                        f'retirements ({reactivated_capacity} conservative '
                        'slots) '
                        f'to cover a {shortfall}-slot ready-capacity shortfall; '
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
        """Cancel an optimization retirement and make a healthy backend live."""
        logger.info(f'Aborting logical retirement of replica '
                    f'{info.replica_id}: {reason}.')
        down_thread_pool = getattr(self, '_down_thread_pool', {})
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
        down_thread_pool = getattr(self, '_down_thread_pool', {})
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
            if not drained:
                drain_cap = getattr(info.status_property, 'drain_cap_seconds',
                                    None)
                if drain_cap is None:
                    drain_cap = self._resolve_drain_cap_seconds(replica_id)
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

    def scale_down_logically(self, replica_id: int, target_capacity: int,
                             version: int, reconcile_generation: int) -> None:
        self.scale_down_logically_batch([replica_id], target_capacity, version,
                                        reconcile_generation)

    @with_lock
    def scale_down_logically_batch(self, replica_ids: list[int],
                                   target_capacity: int, version: int,
                                   reconcile_generation: int) -> None:
        """Accept one logical retirement wave from one fleet snapshot."""
        if not replica_ids:
            return
        if not self._uses_logical_replicas:
            raise RuntimeError('Logical scale-down sent to a physical '
                               'replica service.')
        with self._logical_state_lock:
            if not self._logical_target_fence_holds(
                    version, reconcile_generation, target_capacity):
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
            ready_capacity = 0
            committed_capacity = 0
            capacity_by_id: dict[int, tuple[int, int]] = {}
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
                ready_capacity += ready_width
                committed_capacity += committed_width
                capacity_by_id[candidate.replica_id] = (committed_width,
                                                        ready_width)

            accepted = 0
            seen_ids: set[int] = set()
            for replica_id in replica_ids:
                if replica_id in seen_ids:
                    continue
                seen_ids.add(replica_id)
                info = infos_by_id.get(replica_id)
                if (info is None or info.is_terminal or getattr(
                        info.status_property, 'is_scale_down', False) is True):
                    continue

                committed_width, ready_width = capacity_by_id.get(
                    replica_id, (0, 0))
                has_served = (info.status_property.first_ready_time is not None
                              and info.status_property.first_ready_time >= 0)
                if not has_served:
                    victim_width = committed_width
                    if committed_capacity - victim_width < target_capacity:
                        continue
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
                    if ready_capacity - victim_ready_width < target_capacity:
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
                accepted += 1

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
        info.status_property.preempted = True
        if info.is_spot and self._spot_placer is not None:
            spot_location = info.get_spot_location()
            assert spot_location is not None
            self._spot_placer.set_preemptive(spot_location)
        self._persist_replica(info.replica_id, info)
        self._terminate_replica(info.replica_id,
                                sync_down_logs=False,
                                replica_drain_delay_seconds=0,
                                is_scale_down=True)
        return True

    #################################
    # ReplicaManager Daemon Threads #
    #################################

    @with_lock
    def _refresh_thread_pool(self) -> None:
        """Refresh the launch/down thread pool.

        This function will checks all sky.launch and sky.down thread on
        the fly. If any of them finished, it will update the status of the
        corresponding replica.
        """
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
        launch_thread_pool_snapshot = list(self._launch_thread_pool.items())
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
        failed_spot_locations: set[spot_placer.Location] = set()
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
        launch_infos = serve_state.get_replica_infos_from_ids(
            self._service_name,
            [replica_id for replica_id, _ in finished_launches])
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
                if t.format_exc is not None:
                    if replica_id not in unfenced_launch_failures:
                        failed_spot_locations.add(location)
                else:
                    selected_at = getattr(info, 'created_at', None)
                    if selected_at is not None:
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
            for location in failed_spot_locations:
                self._spot_placer.set_preemptive(location)

        completed_launches: list[tuple[int, ReplicaInfo, bool]] = []
        for replica_id, t in finished_launches:
            info = launch_infos.get(replica_id)
            assert info is not None, replica_id
            if info.status == serve_state.ReplicaStatus.PENDING:
                pending_launches.append((replica_id, t, info))
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
                # TODO(tian): Currently, we set the location to
                # preemptive if the launch thread failed. This is
                # because if the error is not related to the
                # availability of the location, then all locations
                # should failed for same reason. So it does not matter
                # which location is preemptive or not, instead, all
                # locations would fail. We should implement a log parser
                # to detect if the error is actually related to the
                # availability of the location later.
                if (t.format_exc is not None and
                        replica_id not in unfenced_launch_failures):
                    info.status_property.failed_spot_availability = True
            completed_launches.append((replica_id, info, error_in_sky_launch))

        # Persist one completed launch wave in one transaction while holding
        # the manager lock. A per-replica transaction here delays admission of
        # already-selected teardown workers behind O(wave size) PostgreSQL
        # round trips. Keep local worker tracking intact until the batch commit
        # succeeds so a transient write failure is retried on the next tick.
        self._persist_replicas([
            (replica_id, info) for replica_id, info, _ in completed_launches
        ])
        for replica_id, info, error_in_sky_launch in completed_launches:
            self._launch_thread_pool.pop(replica_id)
            self._replica_to_request_id.pop(replica_id)
            if error_in_sky_launch:
                # Teardown after update replica info since
                # _terminate_replica will update the replica info too.
                self._terminate_replica(replica_id,
                                        sync_down_logs=True,
                                        replica_drain_delay_seconds=0)

        if pending_launches:
            # Queued launches for one service share the same controller-owner
            # proof; re-checking it per replica only burns DB work and log
            # budget without changing the admission decision for this tick.
            authorization = self._service_launch_authorization()
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
                    self._launch_thread_pool.pop(replica_id)
                    self._replica_to_request_id.pop(replica_id)
                    self._replica_to_launch_cancelled.pop(replica_id)
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
                         not self._spot_placer.is_active_location(location))):
                        # This exact placement failed after the batch was
                        # queued but before this thread was admitted. Drop the
                        # never-started row so the autoscaler replans it on the
                        # next tick against the next-cheapest active location.
                        logger.info(
                            f'Discarding queued launch for replica '
                            f'{replica_id}: placement {location} is benched.')
                        self._remove_replica(replica_id)
                        self._launch_thread_pool.pop(replica_id)
                        self._replica_to_request_id.pop(replica_id)
                        self._replica_to_launch_cancelled.pop(replica_id)
                        continue
                # sky.launch not started yet; admitted below under the
                # resources lock.
                launch_to_admit.append((replica_id, t, info))

        # Snapshot AFTER the finished-launch pass so down threads it scheduled
        # (via _terminate_replica for failed launches) are admitted this tick.
        down_thread_pool_snapshot = list(self._down_thread_pool.items())
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
            self._down_thread_pool.pop(replica_id)

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
                            self._down_thread_pool.pop(replica_id)
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
                            self._down_thread_pool.pop(replica_id)
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
        while True:
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
            time.sleep(_PROCESS_POOL_REFRESH_INTERVAL)

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
        to_fetch: list[tuple[ReplicaInfo, Any]] = []
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
                continue
            to_fetch.append((info, handle))
        if not to_fetch:
            return
        # Use None to fetch latest job, which stands for user task job
        job_ids = [1] if self._is_pool else None

        def _get_job_status(handle):
            # SSH into the replica's head node -- intentionally OUTSIDE
            # self.lock so an unreachable replica cannot wedge the round.
            return backend.get_job_status(handle, job_ids, stream_logs=False)

        # The fetches are pure I/O; run them in parallel so one hung SSH
        # (preempted spot) delays only its own replica's result, not the
        # whole fleet's failure detection.
        num_fetch_threads = min(len(to_fetch),
                                self._PROBE_ROUND_MAX_PARALLELISM)
        with mp_pool.ThreadPool(num_fetch_threads) as pool:
            fetch_results = [(info, pool.apply_async(_get_job_status,
                                                     (handle,)))
                             for info, handle in to_fetch]
            self._handle_job_status_results(fetch_results)

    def _handle_job_status_results(
            self, fetch_results: list[tuple[ReplicaInfo, Any]]) -> None:
        """Consume the parallel job-status fetches, in submission order."""
        for info, result in fetch_results:
            try:
                job_statuses = result.get()
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
        while True:
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
            time.sleep(_JOB_STATUS_FETCH_INTERVAL)

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
        yaml_replica_ids: list[int] = []
        yaml_paths: list[str] = []
        for info in infos:
            cluster_record = cluster_records.get(info.cluster_name)
            if cluster_record is None:
                continue
            handle = info.handle(cluster_record)
            if handle is None:
                continue
            handles[info.replica_id] = handle
            cluster_yaml = getattr(handle, 'cluster_yaml', None)
            if cluster_yaml is not None:
                yaml_replica_ids.append(info.replica_id)
                yaml_paths.append(cluster_yaml)

        provider_configs: dict[int, dict[str, Any]] = {}
        if yaml_paths:
            yaml_configs = global_user_state.get_cluster_yaml_dict_multiple(
                yaml_paths)
            provider_configs = {
                replica_id: config['provider'] for replica_id, config in zip(
                    yaml_replica_ids, yaml_configs, strict=True)
            }

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
        # Probes are pure I/O (HTTP GET/POST with a several-second timeout):
        # the default ThreadPool size (cpu_count) turns a large fleet into
        # dozens of sequential probe waves and the round overruns its 10s
        # period. Size the pool to the fleet, capped to bound thread cost.
        num_probe_threads = min(len(infos_to_probe),
                                self._PROBE_ROUND_MAX_PARALLELISM)
        with mp_pool.ThreadPool(num_probe_threads) as pool:
            for info in infos_to_probe:
                if self._is_pool:
                    replica_to_probe.append(f'replica_{info.replica_id}(cluster'
                                            f'_name={info.cluster_name})')
                    probe_futures.append(pool.apply_async(info.probe_pool))
                else:
                    resolved_url = probe_urls[info.replica_id]
                    replica_to_probe.append(
                        f'replica_{info.replica_id}(url={resolved_url})')
                    probe_futures.append(
                        pool.apply_async(
                            info.probe,
                            (
                                self._get_readiness_path(info.version),
                                self._get_post_data(info.version),
                                self._get_readiness_timeout_seconds(
                                    info.version),
                                self._get_readiness_headers(info.version),
                                resolved_url,
                            ),
                        ),)
            logger.info(f'Replicas to probe: {", ".join(replica_to_probe)}')

            # Since futures.as_completed will return futures in the order of
            # completion, we need the info.probe function to return the info
            # object as well, so that we could update the info object in the
            # same order.
            probe_results: list[tuple[ReplicaInfo, bool, float]] = [
                future.get() for future in probe_futures
            ]

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
                info for info, probe_succeeded, _ in probe_results
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
            for future_result in probe_results:
                info, probe_succeeded, probe_time = future_result
                info.status_property.service_ready_now = probe_succeeded
                should_teardown = False
                if probe_succeeded:
                    if self._uptime is None:
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
                    is_preempted = False
                    if info.replica_id in possibly_preempted_ids:
                        # Cloud pre-filter above says the instance is gone:
                        # run the full preemption path (forced refresh with
                        # its record-cleanup side effects + spot placer
                        # preemptive marking + teardown).
                        is_preempted = self._handle_preemption(info)
                    if is_preempted:
                        preempted_replica_ids.add(info.replica_id)
                        continue

                    if info.first_not_ready_time is None:
                        info.first_not_ready_time = probe_time
                    if info.status_property.first_ready_time is not None:
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
        mutated_ids = set(replicas_to_teardown) | preempted_replica_ids
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

    def _replica_prober(self) -> None:
        """Periodically probe replicas."""
        while True:
            logger.debug('Running replica prober.')
            try:
                # Reuse the probe round's end-of-round snapshot instead of
                # re-reading (and re-deserializing) the whole fleet from the
                # DB a second time per tick.
                replica_infos = self._probe_all_replicas()
                # TODO(zhwu): when there are multiple load balancers, we need
                # to make sure the active_versions are the union of all
                # versions of all load balancers.
                serve_utils.set_service_status_and_active_versions_from_replica(
                    self._service_name, replica_infos, self._update_mode,
                    **self._db_fence_kwargs())

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
            time.sleep(self._get_endpoint_probe_interval_seconds())

    def get_active_replica_urls(self) -> list[str]:
        """Get the urls of all active replicas."""
        record = serve_state.get_service_from_name(self._service_name)
        assert record is not None, (f'{self._service_name} not found on '
                                    'controller records.')
        ready_replica_urls = []
        active_versions = set(record['active_versions'])
        for info in serve_state.get_replica_infos(self._service_name):
            if (info.status == serve_state.ReplicaStatus.READY and
                    info.version in active_versions):
                assert info.url is not None, info
                ready_replica_urls.append(info.url)
        return ready_replica_urls

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
    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
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
        # A service update may change the placement policy or any_of shape
        # set. Rebuild it before mutating manager version state so neither
        # logical nor physical versions retain candidates from the prior spec.
        new_placer_name = getattr(spec, 'spot_placer', None)
        new_spot_placer = None
        if new_uses_logical_replicas or isinstance(new_placer_name, str):
            new_spot_placer = spot_placer.SpotPlacer.from_task(spec, new_task)
        old_spot_placer = getattr(self, '_spot_placer', None)
        if new_spot_placer is not None and old_spot_placer is not None:
            new_spot_placer.inherit_preemption_state(old_spot_placer)
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

        self.latest_version = version
        self.yaml_content = new_yaml_content
        self._update_mode = update_mode
        self._uses_logical_replicas = new_uses_logical_replicas
        version_specs = getattr(self, '_version_specs', None)
        if version_specs is None:
            # Compatibility for embedders and legacy tests that construct a
            # manager without running the current constructor.
            version_specs = {}
            self._version_specs = version_specs
        version_specs[version] = spec
        self._default_planned_capacity = new_default_planned_capacity
        self._spot_placer = new_spot_placer

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
