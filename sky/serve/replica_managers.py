"""ReplicaManager: handles the creation and deletion of endpoint replicas."""
import contextlib
import dataclasses
import functools
from multiprocessing import pool as mp_pool
import os
import pathlib
import threading
import time
import traceback
import typing
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import colorama
import filelock
import requests

from sky import backends
from sky import exceptions
from sky import global_user_state
from sky import sky_logging
from sky import task as task_lib
from sky.backends import backend_utils
from sky.client import sdk
from sky.serve import constants as serve_constants
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import service
from sky.serve import spot_placer
from sky.skylet import constants
from sky.skylet import job_lib
from sky.usage import usage_lib
from sky.utils import common_utils
from sky.utils import context
from sky.utils import controller_utils
from sky.utils import env_options
from sky.utils import resources_utils
from sky.utils import status_lib
from sky.utils import thread_utils
from sky.utils import ux_utils
from sky.utils import yaml_utils

if typing.TYPE_CHECKING:
    import logging

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
# An LB in-flight report older than this is considered stale (LB dead or
# not reporting): the drain wait then falls back to the full cap.
_IN_FLIGHT_REPORT_STALENESS_SECONDS = (
    3 * serve_constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
# Rate limit for the "fill launch skipped" log (see _log_fill_skip).
_FILL_SKIP_LOG_INTERVAL_SECONDS = 60
_WAIT_LAUNCH_THREAD_TIMEOUT_SECONDS = 15

# Sentinel for to_info_dict's pre-fetched cluster_record
# parameters. We can't use None because None is a legitimate value (it means
# "no cluster row" / "no handle"). The sentinel lets callers opt in to the
# batched fetch path while preserving the existing self-fetch behavior for
# back-compat callers like ReplicaInfo.__repr__.
_NOT_PROVIDED: Any = object()

# TODO(tian): Backward compatibility. Remove this after 3 minor release, i.e.
# 0.13.0. We move the ProcessStatus to common_utils.ProcessStatus in #6666, but
# old ReplicaInfo in database will still tries to unpickle using ProcessStatus
# in replica_managers. We set this alias to avoid breaking changes. See #6729
# for more details.
ProcessStatus = common_utils.ProcessStatus


# TODO(tian): Combine this with
# sky/spot/recovery_strategy.py::StrategyExecutor::launch
# Use context.contextual to enable per-launch output redirection.
@context.contextual
def launch_cluster(replica_id: int,
                   yaml_content: str,
                   cluster_name: str,
                   log_file: str,
                   replica_to_request_id: thread_utils.ThreadSafeDict[int, str],
                   replica_to_launch_cancelled: thread_utils.ThreadSafeDict[
                       int, bool],
                   resources_override: Optional[Dict[str, Any]] = None,
                   retry_until_up: bool = True,
                   max_retry: int = _DEFAULT_LAUNCH_MAX_RETRY,
                   availability_max_retry: Optional[int] = None) -> None:
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
        task = task_lib.Task.from_yaml_str(yaml_content)
        if resources_override is not None:
            resources = task.resources
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
            usage_lib.messages.usage.set_internal()
            request_id = sdk.launch(task,
                                    cluster_name,
                                    retry_until_up=retry_until_up,
                                    _is_launched_by_sky_serve_controller=True)
            logger.info(f'Replica cluster {cluster_name} launch requested '
                        f'with request_id: {request_id}.')
            replica_to_request_id[replica_id] = request_id
            sdk.stream_and_get(request_id)
            logger.info(f'Replica cluster {cluster_name} launched.')
        except (exceptions.InvalidClusterNameError,
                exceptions.NoCloudAccessError,
                exceptions.ResourcesMismatchError) as e:
            logger.error('Failure happened before provisioning. '
                         f'{common_utils.format_exception(e)}')
            raise RuntimeError('Failed to launch the sky serve replica '
                               f'cluster {cluster_name}.') from e
        except exceptions.ResourcesUnavailableError as e:
            if not any(
                    isinstance(err, exceptions.ResourcesUnavailableError)
                    for err in e.failover_history):
                raise RuntimeError('Failed to launch the sky serve replica '
                                   f'cluster {cluster_name}.') from e
            availability_retry_cnt += 1
            logger.info('Failed to launch the sky serve replica cluster with '
                        f'error: {common_utils.format_exception(e)})')
        except Exception as e:  # pylint: disable=broad-except
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
        start_backoff = time.time()
        # Check if it is cancelled every 0.1 seconds.
        while time.time() - start_backoff < gap_seconds:
            if _check_is_cancelled():
                return
            time.sleep(0.1)


def _wait_for_drain(drain_deadline: float,
                    drain_complete: Optional[Callable[[], bool]]) -> None:
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


class _ReplicaDrainTracker:
    """Stateful drain-complete predicate for one retiring replica.

    'Drained' requires SEEN-THEN-CLEAN: some fresh report received after
    the drain began must have acknowledged the replica's url (in the
    routing view, the in-flight gauge, the unknown set, or the draining
    set) before a later fresh report showing it absent-and-idle is
    trusted -- and both must come from the SAME LB incarnation (the LB
    ships a per-process session id): a cold LB (restarted mid-drain, its
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
        self._session: Optional[str] = None

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
        if (time.monotonic() - received_at >
                _IN_FLIGHT_REPORT_STALENESS_SECONDS):
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
        drain_deadline: Optional[float] = None,
        drain_complete: Optional[Callable[[], bool]] = None,
        continue_guard: Optional[Callable[[], bool]] = None) -> None:
    """Terminate the sky serve replica cluster."""
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
            request_id = sdk.down(cluster_name)
            sdk.stream_and_get(request_id)
            logger.info(f'Replica cluster {cluster_name} terminated.')
            return
        except ValueError:
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


def _get_resources_ports(yaml_content: str) -> str:
    """Get the resources ports used by the task."""
    task = task_lib.Task.from_yaml_str(yaml_content)
    # Already checked all ports are valid in sky.serve.core.up
    assert task.resources, task
    assert task.service is not None, task
    if task.service.pool:
        return '-'
    assert task.service.ports is not None, task
    return task.service.ports


def _should_use_spot(yaml_content: str,
                     resource_override: Optional[Dict[str, Any]]) -> bool:
    """Get whether the task should use spot."""
    if resource_override is not None:
        use_spot_override = resource_override.get('use_spot')
        if use_spot_override is not None:
            assert isinstance(use_spot_override, bool)
            return use_spot_override
    task = task_lib.Task.from_yaml_str(yaml_content)
    spot_use_resources = [
        resources for resources in task.resources if resources.use_spot
    ]
    # Heterogeneous any_of sets may mix spot cloud entries with
    # non-spot reserved-capacity entries (e.g. a Kubernetes pool, which
    # cannot use spot). The task counts as spot-managed when ANY entry
    # is spot; the placer then pins each launch's actual use_spot via
    # its location's resources_override.
    return len(spot_use_resources) > 0


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
    first_ready_time: Optional[float] = None
    # None means sky.down is not called yet.
    sky_down_status: Optional[common_utils.ProcessStatus] = None
    # Whether the termination is caused by autoscaler's decision
    is_scale_down: bool = False
    # The replica's spot instance was preempted.
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
    drain_cap_seconds: Optional[int] = None

    def unrecoverable_failure(self) -> bool:
        """Whether the replica fails and cannot be recovered.

        Autoscaler should stop scaling if any of the replica has unrecoverable
        failure, e.g., the user app fails before the service endpoint being
        ready for the current version.
        """
        replica_status = self.to_replica_status()
        logger.info(
            'Check replica unrecorverable: first_ready_time '
            f'{self.first_ready_time}, user_app_failed {self.user_app_failed}, '
            f'status {replica_status}')
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
                # Replica (spot) is preempted
                return serve_state.ReplicaStatus.PREEMPTED
            if self.sky_down_status == common_utils.ProcessStatus.SCHEDULED:
                # sky.down is scheduled to run, but not started yet.
                return serve_state.ReplicaStatus.SHUTTING_DOWN
            if self.sky_down_status == common_utils.ProcessStatus.RUNNING:
                # sky.down is running
                return serve_state.ReplicaStatus.SHUTTING_DOWN
            if self.sky_launch_status == common_utils.ProcessStatus.INTERRUPTED:
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


class ReplicaInfo:
    """Replica info for each replica."""

    # Version 6 is also a worker-runtime compatibility marker for immutable
    # Sky Batch attempt outputs. New Batch clients reject older pool replicas
    # so an incompatible worker fails before dispatch rather than mid-run.
    _VERSION = 6

    def __init__(self, replica_id: int, cluster_name: str, replica_port: str,
                 is_spot: bool, location: Optional[spot_placer.Location],
                 version: int, resources_override: Optional[Dict[str,
                                                                 Any]]) -> None:
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
        self.created_at: Optional[float] = time.time()
        self.first_not_ready_time: Optional[float] = None
        self.consecutive_failure_times: List[float] = []
        self.status_property: ReplicaStatusProperty = ReplicaStatusProperty()
        self.is_spot: bool = is_spot
        self.location: Optional[Dict[str, Optional[str]]] = (
            location.to_pickleable() if location is not None else None)
        self.resources_override: Optional[Dict[str, Any]] = resources_override
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

    def get_spot_location(self) -> Optional[spot_placer.Location]:
        return spot_placer.Location.from_pickleable(self.location)

    def handle(
        self,
        cluster_record: Optional[Dict[str, Any]] = None
    ) -> Optional[backends.CloudVmRayResourceHandle]:
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

    @property
    def url(self) -> Optional[str]:
        handle = self.handle()
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
            endpoint_dict = backend_utils.get_endpoints(handle.cluster_name,
                                                        replica_port_int)
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
    def status(self) -> serve_state.ReplicaStatus:
        replica_status = self.status_property.to_replica_status()
        if replica_status == serve_state.ReplicaStatus.UNKNOWN:
            logger.error('Detecting UNKNOWN replica status for '
                         f'replica {self.replica_id}.')
        return replica_status

    def to_info_dict(self,
                     with_handle: bool,
                     with_url: bool = True,
                     cluster_record: Any = _NOT_PROVIDED) -> Dict[str, Any]:
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
        """
        if cluster_record is _NOT_PROVIDED:
            cluster_record = global_user_state.get_cluster_from_name(
                self.cluster_name,
                include_user_info=False,
                summary_response=True)
        info_dict = {
            'replica_id': self.replica_id,
            'name': self.cluster_name,
            'status': self.status,
            'version': self.version,
            'replica_info_version': self._version,
            'endpoint': self.url if with_url else None,
            'is_spot': self.is_spot,
            'launched_at': (cluster_record['launched_at']
                            if cluster_record is not None else None),
        }
        # Resolve the handle once. When the cluster row is missing, the
        # handle is also missing (they live in the same row), so
        # short-circuit to avoid an extra DB lookup.
        if cluster_record is None:
            handle = None
        else:
            handle = self.handle(cluster_record)
        # Always populate the small derived strings — new clients read
        # these instead of touching the handle, and the cost is just a
        # dict lookup + isinstance on a cluster_record we already have.
        if handle is not None and handle.launched_resources is not None:
            info_dict['cloud'] = repr(handle.launched_resources.cloud)
            info_dict['region'] = handle.launched_resources.region
            simple, full = resources_utils.get_readable_resources_repr(
                handle, simplified_only=False)
            info_dict['resources_str'] = simple
            info_dict['resources_str_full'] = (full
                                               if full is not None else simple)
            info_dict['infra'] = handle.launched_resources.infra.formatted_str()
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

    def probe_pool(self) -> Tuple['ReplicaInfo', bool, float]:
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
        post_data: Optional[Dict[str, Any]],
        timeout: int,
        headers: Optional[Dict[str, str]],
    ) -> Tuple['ReplicaInfo', bool, float]:
        """Probe the readiness of the replica.

        Returns:
            Tuple of (self, is_ready, probe_time).
        """
        replica_identity = f'replica {self.replica_id} with url {self.url}'
        # TODO(tian): This requiring the clock on each replica to be aligned,
        # which may not be true when the GCP VMs have run for a long time. We
        # should have a better way to do this. See #2539 for more information.
        probe_time = time.time()
        try:
            msg = ''
            # TODO(tian): Support HTTPS in the future.
            url = self.url
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

        self.__dict__.update(state)
        self._version = version if version >= 0 else 0


class ReplicaManager:
    """Each replica manager monitors one service."""

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int,
                 resource_scope: Optional[str] = None,
                 service_hash: Optional[str] = None,
                 controller_pid: Optional[int] = None,
                 controller_ip: Optional[str] = None) -> None:
        self.lock = threading.Lock()
        self._next_replica_id: int = 1
        self._service_name: str = service_name
        self._resource_scope = resource_scope
        self._service_hash = service_hash
        self._controller_owner = ((controller_pid,
                                   controller_ip) if service_hash is not None or
                                  controller_pid is not None or
                                  controller_ip is not None else None)
        self._uptime: Optional[float] = None
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
        self._lb_in_flight_report: Optional[Tuple[float, Dict[str, int],
                                                  Optional[Set[str]], Set[str],
                                                  Set[str],
                                                  Optional[str]]] = None
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
        # Oldest version among the currently provisioned and launched replicas
        self.least_recent_version: int = version

    def update_lb_in_flight(self,
                            in_flight_by_url: Optional[Dict[str, int]],
                            routing_urls: Optional[List[str]] = None,
                            unknown_urls: Optional[List[str]] = None,
                            draining_urls: Optional[List[str]] = None,
                            lb_session_id: Optional[str] = None) -> None:
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
    def spot_placer(self) -> Optional['spot_placer.SpotPlacer']:
        """The placer, if this manager kind carries one (else None).

        Public accessor for the controller's fill machinery, which needs
        the placer's zero-cost location set; only
        SkyPilotReplicaManager actually builds one.
        """
        return getattr(self, '_spot_placer', None)

    def scale_up(self,
                 resources_override: Optional[Dict[str, Any]] = None) -> None:
        """Scale up the service by 1 replica with resources_override.
        resources_override is of the same format with resources section
        in skypilot task yaml
        """
        raise NotImplementedError

    def scale_up_batch(
            self, resources_overrides: List[Optional[Dict[str, Any]]]) -> None:
        """Scale up by len(resources_overrides) replicas in one batch.

        Subclasses may override to amortize per-call synchronization; the
        default just loops over `scale_up`.
        """
        for resources_override in resources_overrides:
            self.scale_up(resources_override)

    def scale_down(self, replica_id: int, purge: bool = False) -> None:
        """Scale down replica with replica_id."""
        raise NotImplementedError

    def update_version(self, version: int, spec: 'service_spec.SkyServiceSpec',
                       update_mode: serve_utils.UpdateMode) -> None:
        raise NotImplementedError

    def get_active_replica_urls(self) -> List[str]:
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

    def _db_fence_kwargs(self) -> Dict[str, Any]:
        """Exact owner predicates, omitted for legacy/direct test managers."""
        kwargs: Dict[str, Any] = {}
        service_hash = getattr(self, '_service_hash', None)
        if service_hash is not None:
            kwargs['expected_service_hash'] = service_hash
        controller_owner = getattr(self, '_controller_owner', None)
        if controller_owner is not None:
            kwargs['expected_controller_owner'] = controller_owner
        return kwargs

    def _persist_replica(self, replica_id: int, info: ReplicaInfo) -> None:
        persisted = serve_state.add_or_update_replica(self._service_name,
                                                      replica_id, info,
                                                      **self._db_fence_kwargs())
        if persisted is False:
            raise RuntimeError(
                f'Service {self._service_name!r} incarnation changed while '
                f'persisting replica {replica_id}.')

    def _persist_replicas(self,
                          replica_infos: List[Tuple[int, ReplicaInfo]]) -> None:
        persisted = serve_state.add_or_update_replicas(
            self._service_name, replica_infos, **self._db_fence_kwargs())
        if persisted is False:
            raise RuntimeError(
                f'Service {self._service_name!r} incarnation changed while '
                'persisting replica probe results.')

    def _remove_replica(self, replica_id: int) -> None:
        removed = serve_state.remove_replica(self._service_name, replica_id,
                                             **self._db_fence_kwargs())
        if removed is False:
            raise RuntimeError(
                f'Service {self._service_name!r} incarnation changed while '
                f'removing replica {replica_id}.')

    def __init__(self,
                 service_name: str,
                 spec: 'service_spec.SkyServiceSpec',
                 version: int,
                 resource_scope: Optional[str] = None,
                 service_hash: Optional[str] = None,
                 controller_pid: Optional[int] = None,
                 controller_ip: Optional[str] = None) -> None:
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
        yaml_content = serve_state.get_yaml_content(service_name, version)
        assert yaml_content is not None, (
            f'yaml content not found for {service_name} version {version}')
        self.yaml_content: str = yaml_content
        task = task_lib.Task.from_yaml_str(self.yaml_content)
        self._spot_placer: Optional[spot_placer.SpotPlacer] = (
            spot_placer.SpotPlacer.from_task(spec, task))
        self._fill_skip_last_log_time: float = 0.0
        # TODO(tian): Store launch/down request id in the replica table, to make
        # the manager more persistent.
        self._launch_thread_pool: thread_utils.ThreadSafeDict[
            int, thread_utils.SafeThread] = thread_utils.ThreadSafeDict()
        self._replica_to_request_id: thread_utils.ThreadSafeDict[
            int, str] = thread_utils.ThreadSafeDict()
        self._replica_to_launch_cancelled: thread_utils.ThreadSafeDict[
            int, bool] = thread_utils.ThreadSafeDict()
        self._down_thread_pool: thread_utils.ThreadSafeDict[
            int, thread_utils.SafeThread] = thread_utils.ThreadSafeDict()

        # Tick-scoped memo of per-version specs, reset at the start of every
        # probe round (see _probe_all_replicas). Within a single readiness probe
        # the prober resolves the spec for every replica 4 times (readiness
        # path, post data, headers, timeout); memoizing by version collapses
        # those 4*N DB reads + pickle.loads into one read per distinct version
        # per tick. Scoping it to a tick keeps it single-threaded (only the
        # prober thread touches it) and never reuses a spec across ticks, so it
        # cannot go stale even if a version's spec row is later rewritten.
        self._tick_version_spec_cache: Dict[int,
                                            'service_spec.SkyServiceSpec'] = {}

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
        existing_replica_ids = [info.replica_id for info in all_replica_infos]
        self._next_replica_id = max(existing_replica_ids, default=0) + 1

        # There is a FIFO queue with capacity _MAX_NUM_LAUNCH for
        # _launch_replica.
        # We prioritize PROVISIONING replicas since they were previously
        # launched but may have been interrupted and need to be restarted.
        # This is why we handle PENDING replicas only after PROVISIONING
        # replicas.
        to_up_replicas = serve_state.get_replicas_at_status(
            self._service_name, serve_state.ReplicaStatus.PROVISIONING)
        to_up_replicas.extend(
            serve_state.get_replicas_at_status(
                self._service_name, serve_state.ReplicaStatus.PENDING))

        for replica_info in to_up_replicas:
            # It should be robust enough for `execution.launch` to handle cases
            # where the provisioning is partially done.
            # So we mock the original request based on all call sites,
            # including SkyServeController._run_autoscaler.
            # One shared snapshot: per-launch fresh scans made recovery
            # O(pending x total rows) — tens of minutes at fleet scale.
            # Per-replica isolation: one bad row must not strand the rest
            # un-redriven.
            try:
                # Carry the prior row's launch-origin attribution: the fill
                # sentinel was consumed at original emission, so without it
                # the replacement row would flip a fill replica to
                # demand-placed (permanently ceiling-exempt).
                self._launch_replica(
                    replica_info.replica_id,
                    resources_override=replica_info.resources_override,
                    existing_replica_infos=all_replica_infos,
                    prior_reserved_fill=bool(
                        getattr(replica_info, 'reserved_fill', False)))
            except Exception as e:  # pylint: disable=broad-except
                logger.error('Failed to re-drive launch of replica '
                             f'{replica_info.replica_id}: '
                             f'{common_utils.format_exception(e)}')

        # A forced status refresh can remove a preempted spot cluster from
        # global state before the replica row is marked preempted. If the
        # controller crashes between those writes, the next prober cannot
        # classify the replica (its handle is already gone). Detect that
        # earliest crash window in one bulk lookup. Only active, successfully
        # launched spot rows qualify; pending launches and retained failure
        # records can legitimately lack a handle and must keep their existing
        # recovery semantics.
        active_statuses = {
            serve_state.ReplicaStatus.STARTING,
            serve_state.ReplicaStatus.READY,
            serve_state.ReplicaStatus.NOT_READY,
        }
        active_spot_replicas = [
            info for info in all_replica_infos
            if (info.is_spot and not info.status_property.preempted and
                info.status in active_statuses)
        ]
        active_spot_cluster_names = [
            info.cluster_name for info in active_spot_replicas
        ]
        active_spot_status_fields = global_user_state.get_cluster_status_fields(
            active_spot_cluster_names)
        orphaned_spot_clusters = {
            info.cluster_name
            for info in active_spot_replicas
            if info.cluster_name not in active_spot_status_fields
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
                info.cluster_name in orphaned_spot_clusters)
        ]
        for replica_info in to_down_replicas:
            try:
                # A scale-down retirement interrupted by a controller
                # restart re-enters a FULL bounded drain: its pre-crash
                # drain progress is unknowable without persisted drain
                # state, and re-driving with no drain (as before) would
                # kill whatever is still in flight. Worst case the total
                # drain doubles; it never kills early. Purged and failure
                # teardowns keep the immediate re-drive. Preempted replicas
                # also re-drive immediately: their cloud instance is already
                # gone (or partially gone), and the persisted preempted bit
                # is itself sufficient to classify this as scale-down cleanup
                # even if the crash preceded the is_scale_down write.
                drain_cap: Optional[int] = None
                status_property = replica_info.status_property
                if replica_info.cluster_name in orphaned_spot_clusters:
                    logger.warning(
                        f'Recovering preempted replica '
                        f'{replica_info.replica_id}: cluster '
                        f'{replica_info.cluster_name!r} was removed before '
                        'the preemption intent was persisted.')
                    status_property.preempted = True
                    # Persist the recovered intent before scheduling cleanup;
                    # another crash is then caught by the raw preempted scan.
                    serve_state.add_or_update_replica(self._service_name,
                                                      replica_info.replica_id,
                                                      replica_info)
                is_preempted = status_property.preempted
                # SpotPlacer is reconstructed with every location ACTIVE on a
                # controller restart. Rebuild the preemption bench before the
                # durable replica row (and its location evidence) is removed,
                # including when the preempted intent was already persisted.
                if is_preempted and self._spot_placer is not None:
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

    ################################
    # Replica management functions #
    ################################

    # We don't need to add lock here since every caller of this function
    # will acquire the lock.
    def _launch_replica(
        self,
        replica_id: int,
        resources_override: Optional[Dict[str, Any]] = None,
        existing_replica_infos: Optional[List['ReplicaInfo']] = None,
        prior_reserved_fill: bool = False,
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
        fill_grant_epoch: Optional[int] = None
        fill_pool_key: Optional[str] = None
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
        logger.info(f'Launching replica {replica_id}...')
        cluster_name = serve_utils.generate_replica_cluster_name(
            self._service_name, replica_id,
            getattr(self, '_resource_scope', None))
        log_file_name = serve_utils.generate_replica_launch_log_file_name(
            self._service_name, replica_id,
            getattr(self, '_resource_scope', None))
        use_spot = _should_use_spot(self.yaml_content, resources_override)
        retry_until_up = True
        location = None
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
        if self._spot_placer is not None and (use_spot or zero_cost_only):
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
            current_spot_locations: List[spot_placer.Location] = []
            # The per-launch replica scan is O(total rows) of unpickling;
            # bulk callers (recovery re-enqueueing hundreds of launches)
            # pass one snapshot instead of paying it per launch.
            if existing_replica_infos is None:
                existing_replica_infos = serve_state.get_replica_infos(
                    self._service_name)
            for info in existing_replica_infos:
                # Every placer-placed replica counts toward location load,
                # including non-spot reserved-capacity ones.
                spot_location = info.get_spot_location()
                if spot_location is not None:
                    current_spot_locations.append(spot_location)
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
                        current_spot_locations))
                if zero_cost_location is None:
                    self._log_fill_skip(
                        'no ACTIVE zero-cost location available')
                    return False
                location = zero_cost_location
            elif self._demand_should_skip_zero_cost(existing_replica_infos):
                # Broker demand-placement gate engaged: this service
                # already holds its zero-cost grant, so new DEMAND
                # placements compete normally instead of squatting on a
                # peer's entitlement. Passed only when engaged so the
                # ungated call shape stays byte-identical.
                location = self._spot_placer.select_next_location(
                    current_spot_locations, skip_zero_cost_preference=True)
            else:
                location = self._spot_placer.select_next_location(
                    current_spot_locations)
            resources_override.update(location.to_dict())
            # The location dictates the actual spot-ness of THIS launch
            # (a zero-cost reserved location is non-spot even though the
            # task as a whole is spot-managed).
            use_spot = location.use_spot
        elif self._spot_placer is not None:
            # Re-driven pinned launch (controller crash mid-PENDING): the
            # persisted override already carries the placer's inlined
            # location fields, but skipped the selection path above (a
            # non-spot pin fails the use_spot gate and the fill sentinel
            # was consumed at original emission). Recover the location
            # from the override so the upserted replica row keeps it --
            # location=None would permanently drop the replica from the
            # placer's load counting and from zero-cost fill accounting
            # (no scale-down shelter, undercounted fill baseline).
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
            args=(replica_id, self.yaml_content, cluster_name, log_file_name,
                  self._replica_to_request_id,
                  self._replica_to_launch_cancelled, resources_override,
                  retry_until_up),
            kwargs={'availability_max_retry': availability_max_retry},
        )
        replica_port = _get_resources_ports(self.yaml_content)

        info = ReplicaInfo(replica_id, cluster_name, replica_port, use_spot,
                           location, self.latest_version, resources_override)
        # Persisted launch-origin attribution: the broker's holdings split
        # and the ceiling's demand exemption key on this flag. OR in the
        # replaced row's attribution on recovery re-drives (the sentinel
        # only exists at original emission).
        info.reserved_fill = bool(zero_cost_only or prior_reserved_fill)
        if (zero_cost_only and fill_grant_epoch is not None and
                fill_pool_key is not None):
            # Broker epoch fence, authoritative leg: the pre-check above
            # is TOCTOU (a round can publish a new epoch between it and
            # this persist, after capacity was already fed to a peer), so
            # the final recheck and the row upsert are ONE transaction,
            # and the persist additionally takes the cross-process broker
            # round lock (non-blocking): a row must land either before a
            # round's debit scan (counted) or after its publish (fenced)
            # -- never inside the scan->publish window where the epoch is
            # still current but the completed scan missed the row (see
            # reserved_capacity_broker.persist_fill_replica). Contention
            # with an in-flight round reads as a fence-skip: retried next
            # tick. The lock/transaction spans only this persist -- never
            # the launch thread (built above, started later by
            # _refresh_thread_pool).
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
        if existing_replica_infos is not None:
            # Bulk callers (recovery re-drive) reuse one snapshot across a
            # whole wave of launches. Append the replica we just placed so
            # the spot placer sees in-wave assignments and spreads the wave
            # across locations instead of pinning every launch to the
            # least-loaded location of the frozen snapshot.
            existing_replica_infos.append(info)
        # Don't start right now; we will start it later in _refresh_thread_pool
        # to avoid too many sky.launch running at the same time.
        self._launch_thread_pool[replica_id] = t
        return True

    def _demand_should_skip_zero_cost(
            self,
            existing_replica_infos: Optional[List['ReplicaInfo']]) -> bool:
        """Broker demand-placement gate: stop NEW squatting at the grant.

        When this service already holds at least its broker grant on the
        zero-cost tier, its DEMAND launches stop preferring that tier and
        compete normally (in practice: go to paid capacity, since the
        grant-full tier carries more load). Reads ONLY the poller's
        in-process grant cache -- no DB on the launch path, and with no
        broker grant (single service, broker disabled, or unit tests)
        the gate is inert and behavior is exactly pre-broker.
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
            self, resources_override: Optional[Dict[str, Any]]) -> None:
        """Allocate an id and enqueue one replica launch. Lock must be held."""
        # Defensive: never hand `_launch_replica` an id that still has a
        # durable replica row. `add_or_update_replica` is an upsert keyed on
        # (service_name, replica_id), so reusing a live id would overwrite a
        # surviving replica's persisted state and re-launch its cluster. With
        # the allocator seeded from durable state in
        # `_recover_replica_operations` this should never fire, but guard
        # against any drift so id allocation can never clobber a live replica.
        while serve_state.get_replica_info_from_id(
                self._service_name, self._next_replica_id) is not None:
            logger.warning(f'Replica id {self._next_replica_id} still has a '
                           'durable replica row; skipping it to avoid '
                           'clobbering a live replica.')
            self._next_replica_id += 1
        # An aborted launch (zero-cost-only fill with no ACTIVE zero-cost
        # location) consumed nothing: keep the id free for the next
        # scale-up.
        if self._launch_replica(self._next_replica_id, resources_override):
            self._next_replica_id += 1

    @with_lock
    def scale_up(self,
                 resources_override: Optional[Dict[str, Any]] = None) -> None:
        self._scale_up_one_locked(resources_override)

    @with_lock
    def scale_up_batch(
            self, resources_overrides: List[Optional[Dict[str, Any]]]) -> None:
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
        """
        for resources_override in resources_overrides:
            self._scale_up_one_locked(resources_override)

    def _handle_sky_down_finish(self, info: ReplicaInfo,
                                format_exc: Optional[str]) -> None:
        if format_exc is not None:
            logger.error(f'Down thread for replica {info.replica_id} '
                         f'exited abnormally with exception {format_exc}.')
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.FAILED)
        else:
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.SUCCEEDED)
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
            in_flight_drain_cap_seconds: Optional[int] = None) -> None:
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
            self._persist_replica(replica_id, info)
            launch_thread = self._launch_thread_pool[replica_id]
            if launch_thread.is_alive():
                self._replica_to_launch_cancelled[replica_id] = True
                start_wait_time = time.time()
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
                    if (time.time() - start_wait_time >
                            _WAIT_LAUNCH_THREAD_TIMEOUT_SECONDS):
                        timeout_reached = True
                        break
                    time.sleep(0.1)
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
                              launch_log_file_name, 'r',
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
                              'r',
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

        if sync_down_logs:
            _download_and_stream_logs(info)

        logger.info(f'preempted: {info.status_property.preempted}, '
                    f'replica_id: {replica_id}')
        info.status_property.is_scale_down = is_scale_down
        info.status_property.purged = purge

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
        self._persist_replica(replica_id, info)
        drain_deadline: Optional[float] = None
        drain_complete: Optional[Callable[[], bool]] = None
        if (in_flight_drain_cap_seconds is not None and
                in_flight_drain_cap_seconds > 0):
            drain_started = time.monotonic()
            drain_deadline = drain_started + in_flight_drain_cap_seconds
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

    @with_lock
    def scale_down(self, replica_id: int, purge: bool = False) -> None:
        # Retirement drain: bounded by the replica's version spec,
        # completing early once the LB provably stopped routing to the
        # replica and reports zero in-flight for it. A purge is a forceful
        # cleanup of an already-failed replica: nothing routable is being
        # retired, so it must not wait out a graceful-drain cap.
        drain_cap = (None
                     if purge else self._resolve_drain_cap_seconds(replica_id))
        self._terminate_replica(replica_id,
                                sync_down_logs=False,
                                replica_drain_delay_seconds=0,
                                is_scale_down=True,
                                purge=purge,
                                in_flight_drain_cap_seconds=drain_cap)

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

    def _handle_preemption(self, info: ReplicaInfo) -> bool:
        """Handle preemption of the replica if any error happened.

        Returns:
            bool: Whether the replica is preempted.
        """
        if not info.is_spot:
            return False

        # Get cluster handle first for zone information. The following
        # backend_utils.refresh_cluster_status_handle might delete the
        # cluster record from the cluster table.
        handle = global_user_state.get_handle_from_cluster_name(
            info.cluster_name)
        if handle is None:
            logger.error(f'Cannot find cluster {info.cluster_name} for '
                         f'replica {info.replica_id} in the cluster table. '
                         'Skipping preemption handling.')
            return False
        assert isinstance(handle, backends.CloudVmRayResourceHandle)
        # Pull the actual cluster status from the cloud provider to
        # determine whether the cluster is preempted.
        cluster_status, _ = backend_utils.refresh_cluster_status_handle(
            info.cluster_name,
            force_refresh_statuses=set(status_lib.ClusterStatus))

        if cluster_status in (status_lib.ClusterStatus.UP,
                              status_lib.ClusterStatus.AUTOSTOPPING):
            return False
        # The cluster is (partially) preempted. It can be down, INIT or STOPPED,
        # based on the interruption behavior of the cloud.
        cluster_status_str = ('' if cluster_status is None else
                              f' (status: {cluster_status.value})')
        logger.info(
            f'Replica {info.replica_id} is preempted{cluster_status_str}.')
        info.status_property.preempted = True
        if self._spot_placer is not None:
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
        # To avoid `dictionary changed size during iteration` error.
        launch_thread_pool_snapshot = list(self._launch_thread_pool.items())
        # Process finished launch threads BEFORE taking the cross-process
        # resources lock: this pass performs per-replica DB writes and, for a
        # failed launch, an inline log sync that may SSH into the replica.
        # None of that needs the lock -- holding it here would stall every
        # other service's admission pass (and `sky serve up`) for the whole
        # walk. Only the admission pass below needs the lock.
        launch_to_admit: List[Tuple[int, thread_utils.SafeThread,
                                    ReplicaInfo]] = []
        for replica_id, t in launch_thread_pool_snapshot:
            if t.is_alive():
                continue
            info = serve_state.get_replica_info_from_id(self._service_name,
                                                        replica_id)
            assert info is not None, replica_id
            if info.status == serve_state.ReplicaStatus.PENDING:
                # sky.launch not started yet; admitted below under the
                # resources lock.
                launch_to_admit.append((replica_id, t, info))
                continue
            # sky.launch finished
            # TODO(tian): Try-catch in thread, and have an enum return
            # value to indicate which type of failure happened.
            # Currently we only have user code failure since the
            # retry_until_up flag is set to True, but it will be helpful
            # when we enable user choose whether to retry or not.
            logger.info(f'Launch thread for replica {replica_id} finished.')
            self._launch_thread_pool.pop(replica_id)
            self._replica_to_request_id.pop(replica_id)
            error_in_sky_launch = False
            if t.format_exc is not None:
                logger.warning(f'Launch thread for replica {replica_id} '
                               f'exited abnormally with exception '
                               f'{t.format_exc}. Terminating...')
                info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.FAILED)
                error_in_sky_launch = True
            else:
                info.status_property.sky_launch_status = (
                    common_utils.ProcessStatus.SUCCEEDED)
            if (self._spot_placer is not None and
                    info.get_spot_location() is not None):
                # TODO(tian): Currently, we set the location to
                # preemptive if the launch thread failed. This is
                # because if the error is not related to the
                # availability of the location, then all locations
                # should failed for same reason. So it does not matter
                # which location is preemptive or not, instead, all
                # locations would fail. We should implement a log parser
                # to detect if the error is actually related to the
                # availability of the location later.
                location = info.get_spot_location()
                assert location is not None
                if t.format_exc is not None:
                    self._spot_placer.set_preemptive(location)
                    info.status_property.failed_spot_availability = True
                else:
                    self._spot_placer.set_active(location)
            self._persist_replica(replica_id, info)
            if error_in_sky_launch:
                # Teardown after update replica info since
                # _terminate_replica will update the replica info too.
                self._terminate_replica(replica_id,
                                        sync_down_logs=True,
                                        replica_drain_delay_seconds=0)

        # Snapshot AFTER the finished-launch pass so down threads it scheduled
        # (via _terminate_replica for failed launches) are admitted this tick.
        down_thread_pool_snapshot = list(self._down_thread_pool.items())
        down_to_admit: List[Tuple[int, thread_utils.SafeThread,
                                  ReplicaInfo]] = []
        for replica_id, t in down_thread_pool_snapshot:
            if t.is_alive():
                continue
            info = serve_state.get_replica_info_from_id(self._service_name,
                                                        replica_id)
            assert info is not None, replica_id
            if (info.status_property.sky_down_status ==
                    common_utils.ProcessStatus.SCHEDULED):
                # sky.down not started yet; admitted below under the
                # resources lock.
                down_to_admit.append((replica_id, t, info))
                continue
            logger.info(f'Terminate thread for replica {replica_id} finished.')
            self._down_thread_pool.pop(replica_id)
            self._handle_sky_down_finish(info, format_exc=t.format_exc)

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
                    if not controller_utils.can_terminate(self._is_pool,
                                                          in_flight=in_flight):
                        continue
                    t.start()
                    # This replica is now terminating; reflect it locally
                    # (weighted like in_flight_launch_count) instead of
                    # re-scanning the DB on the next replica.
                    in_flight += 1.0 / controller_utils.SERVE_LAUNCH_RATIO
                    info.status_property.sky_down_status = (
                        common_utils.ProcessStatus.RUNNING)
                    self._persist_replica(replica_id, info)

        # Clean old version
        replica_infos = serve_state.get_replica_infos(self._service_name)
        current_least_recent_version = min([
            info.version for info in replica_infos
        ]) if replica_infos else self.least_recent_version
        if self.least_recent_version < current_least_recent_version:
            for version in range(self.least_recent_version,
                                 current_least_recent_version):
                yaml_content = serve_utils.get_yaml_content(
                    self._service_name, version)
                # Delete old version metadata.
                deleted = serve_state.delete_version(self._service_name,
                                                     version,
                                                     **self._db_fence_kwargs())
                if deleted is False:
                    raise RuntimeError(
                        f'Service {self._service_name!r} incarnation changed '
                        f'while deleting version {version}.')
                # Delete storage buckets of older versions.
                if not self._is_pool:
                    # For pools, we don't clean up the storage, because the
                    # storage is shared between all replicas. We clean up the
                    # storage in sky/serve/service.py when the pool is
                    # terminated so storage will not be leaked.
                    service.cleanup_storage(yaml_content)
            # newest version will be cleaned in serve down
            self.least_recent_version = current_least_recent_version

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
        """
        infos = serve_state.get_replica_infos(self._service_name)
        for info in infos:
            if not info.status_property.should_track_service_status():
                continue
            # We use backend API to avoid usage collection in the
            # sdk.job_status.
            backend = backends.CloudVmRayBackend()
            handle = info.handle()
            if handle is None:
                # The walk runs lock-free, so the replica's cluster record can
                # vanish mid-walk (a scale-down or preemption cleanup
                # completing after the snapshot was taken). Skip it; the next
                # round re-snapshots.
                continue
            # Use None to fetch latest job, which stands for user task job
            job_ids = [1] if self._is_pool else None
            try:
                # SSH into the replica's head node -- intentionally OUTSIDE
                # self.lock so an unreachable replica cannot wedge the loop.
                job_statuses = backend.get_job_status(handle,
                                                      job_ids,
                                                      stream_logs=False)
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
                if is_preempted:
                    continue
                # Re-raise the exception if it is not preempted.
                raise
            job_status = job_statuses[1] if self._is_pool else list(
                job_statuses.values())[0]
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

    @with_lock
    def _probe_all_replicas(self) -> None:
        """Readiness probe replicas.

        This function will probe all replicas to make sure the service is
        ready. It will keep track of:
            (1) the initial delay for each replica;
            (2) the consecutive failure times.
        The replica will be terminated if any of the thresholds exceeded.
        """
        # Reset the per-tick spec memo so this probe round reads each version's
        # spec from the DB at most once and never reuses a spec across ticks.
        self._tick_version_spec_cache = {}
        probe_futures = []
        replica_to_probe = []
        infos = serve_state.get_replica_infos(self._service_name)
        # Probes are pure I/O (HTTP GET/POST with a several-second timeout):
        # the default ThreadPool size (cpu_count) turns a large fleet into
        # dozens of sequential probe waves and the round overruns its 10s
        # period. Size the pool to the fleet, capped to bound thread cost.
        num_probe_threads = min(max(len(infos), 1),
                                self._PROBE_ROUND_MAX_PARALLELISM)
        with mp_pool.ThreadPool(num_probe_threads) as pool:
            for info in infos:
                if not info.status_property.should_track_service_status():
                    continue
                if self._is_pool:
                    replica_to_probe.append(f'replica_{info.replica_id}(cluster'
                                            f'_name={info.cluster_name})')
                    probe_futures.append(pool.apply_async(info.probe_pool))
                else:
                    replica_to_probe.append(
                        f'replica_{info.replica_id}(url={info.url})')
                    probe_futures.append(
                        pool.apply_async(
                            info.probe,
                            (
                                self._get_readiness_path(info.version),
                                self._get_post_data(info.version),
                                self._get_readiness_timeout_seconds(
                                    info.version),
                                self._get_readiness_headers(info.version),
                            ),
                        ),)
            logger.info(f'Replicas to probe: {", ".join(replica_to_probe)}')

            # Since futures.as_completed will return futures in the order of
            # completion, we need the info.probe function to return the info
            # object as well, so that we could update the info object in the
            # same order.
            probe_results: List[Tuple[ReplicaInfo, bool, float]] = [
                future.get() for future in probe_futures
            ]

            # Parallel cloud-only pre-filter for preemption handling. The
            # full _handle_preemption does a forced cluster refresh (cloud
            # API + an SSH ray-status probe) serially under the manager
            # lock; running it for every failed probe made cold-start probe
            # rounds take minutes instead of the 10s cadence on a
            # 129-replica spot fleet (starving scale-up on the same lock
            # and leaving the LB's ready-set stale -> 503s with READY
            # replicas). Instead, confirm instance liveness with one cheap
            # provider call per failed spot replica, in parallel; only
            # cloud-confirmed-dead (or handle-less) replicas take the full
            # preemption path, which is rare and worth its cost. Detection
            # latency is unchanged: every failed probe is still checked
            # against the cloud every round.
            failed_spot_infos = [
                info for info, probe_succeeded, _ in probe_results
                if not probe_succeeded and info.is_spot
            ]
            possibly_preempted_ids: Set[int] = set()
            if failed_spot_infos:
                num_workers = min(self._PREEMPTION_PREFILTER_PARALLELISM,
                                  len(failed_spot_infos))
                with mp_pool.ThreadPool(num_workers) as prefilter_pool:
                    alive_flags = prefilter_pool.map(
                        self._cloud_instance_looks_alive, failed_spot_infos)
                possibly_preempted_ids = {
                    failed_info.replica_id for failed_info, alive in zip(
                        failed_spot_infos, alive_flags) if not alive
                }

            pending_writes: List[Tuple[int, ReplicaInfo]] = []
            replicas_to_teardown: List[int] = []
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
                    info.consecutive_failure_times.clear()
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
                        continue

                    if info.first_not_ready_time is None:
                        info.first_not_ready_time = probe_time
                    if info.status_property.first_ready_time is not None:
                        info.consecutive_failure_times.append(probe_time)
                        consecutive_failure_time = (
                            info.consecutive_failure_times[-1] -
                            info.consecutive_failure_times[0])
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

    def _replica_prober(self) -> None:
        """Periodically probe replicas."""
        while True:
            logger.debug('Running replica prober.')
            try:
                self._probe_all_replicas()
                replica_infos = serve_state.get_replica_infos(
                    self._service_name)
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

    def get_active_replica_urls(self) -> List[str]:
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
        self.latest_version = version
        self.yaml_content = new_yaml_content
        self._update_mode = update_mode

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
        new_config_any_of = new_config.get('resources', {}).pop('any_of', [])

        replica_infos = serve_state.get_replica_infos(self._service_name)
        for info in replica_infos:
            if info.version < version and not info.is_terminal:
                # Assume user does not change the yaml file on the controller.
                old_yaml_content = serve_state.get_yaml_content(
                    self._service_name, info.version)
                old_config = yaml_utils.safe_load(old_yaml_content)
                for key in ['service', 'pool', '_user_specified_yaml']:
                    old_config.pop(key, None)
                # Bump replica version if all fields except for service are
                # the same.
                # Here, we manually convert the any_of field to a set to avoid
                # only the difference in the random order of the any_of fields.
                old_config_any_of = old_config.get('resources',
                                                   {}).pop('any_of', [])

                if (resources_utils.normalize_any_of_resources_config(
                        old_config_any_of) != resources_utils.
                        normalize_any_of_resources_config(new_config_any_of)):
                    logger.info('Replica config changed (any_of), skipping. '
                                f'old: {old_config_any_of}, '
                                f'new: {new_config_any_of}')
                    continue
                # File mounts should both be empty, as update always
                # create new buckets if they are not empty.
                if (old_config == new_config and
                        old_config.get('file_mounts', None) == {}):
                    logger.info(
                        f'Updating replica {info.replica_id} to version '
                        f'{version}. Replica {info.replica_id}\'s config '
                        f'{old_config} is the same as '
                        f'latest version\'s {new_config}.')
                    info.version = version
                    self._persist_replica(info.replica_id, info)
                else:
                    logger.info('Replica config changed (rest), skipping. '
                                f'old: {old_config}, '
                                f'new: {new_config}')

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

    def _get_post_data(self, version: int) -> Optional[Dict[str, Any]]:
        return self._get_version_spec(version).post_data

    def _get_readiness_headers(self, version: int) -> Optional[Dict[str, str]]:
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
