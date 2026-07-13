"""Tests for sky/serve/replica_managers.py.

Covers:
- `SkyPilotReplicaManager.__init__` startup ordering: the daemon threads
  (especially `_job_status_fetcher`) must NOT race the main thread for
  `self.lock` before `_recover_replica_operations` runs.
- `launch_cluster` retry scoping: resource availability (capacity) failures
  are capped by `availability_max_retry` while other errors keep the
  `max_retry` in-place attempts.
- `_launch_replica` passing `availability_max_retry=1` only for spot
  replicas managed by a spot placer.
"""
# pylint: disable=protected-access,import-outside-toplevel,reimported
# pylint: disable=unused-argument,invalid-name,line-too-long
# pylint: disable=missing-class-docstring,unnecessary-dunder-call
import threading
from unittest import mock

import pytest

from sky import exceptions
from sky.serve import replica_managers
from sky.serve import serve_utils
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import thread_utils


class TestSkyPilotReplicaManagerInitOrdering:
    """`SkyPilotReplicaManager.__init__` must (1) hand the manager lock to
    the recovery pass BEFORE any daemon thread can grab it — otherwise
    `_job_status_fetcher`'s per-replica SSH walk can starve recovery — and
    (2) NOT block on recovery finishing: at fleet scale recovery re-drives
    hundreds of interrupted launches and runs for minutes, and a blocking
    __init__ kept uvicorn from binding within _start's 60s readiness window
    (`_bail_on_boot_failure` -> os._exit -> daemon respawn -> recovery from
    scratch: a controller crash-loop, observed live at ~860 rows / ~520
    interrupted launches)."""

    def _build(self, recovery_body, started_records, resource_scope=None):
        import threading as threading_mod

        with mock.patch.object(
                replica_managers.ReplicaManager, '__init__',
                lambda self_, service_name, spec, version: None), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_yaml_content',
                 return_value='dummy: yaml'), \
             mock.patch(
                 'sky.serve.replica_managers.task_lib.Task.from_yaml_str',
                 return_value=mock.MagicMock()), \
             mock.patch(
                 'sky.serve.replica_managers.spot_placer.SpotPlacer.from_task',
                 return_value=None), \
             mock.patch.object(
                 replica_managers.SkyPilotReplicaManager,
                 '_recover_replica_operations', recovery_body), \
             mock.patch(
                 'sky.serve.replica_managers.thread_utils.'
                 'start_supervised_thread') as mock_supervised:

            def _record(target, *_args, **_kwargs):
                started_records.append((getattr(target, '__name__',
                                                repr(target))))
                return mock.Mock()

            mock_supervised.side_effect = _record

            # Base __init__ is stubbed; provide the attrs it would set.
            def _patched_base_init(self_, service_name, spec, version):
                self_.lock = threading_mod.Lock()
                self_._service_name = service_name
                self_._next_replica_id = 1
                self_._uptime = None
                self_.latest_version = version
                self_._update_mode = None
                self_._is_pool = False
                self_._resource_scope = None

            with mock.patch.object(replica_managers.ReplicaManager, '__init__',
                                   _patched_base_init):
                mgr = replica_managers.SkyPilotReplicaManager(
                    service_name='svc',
                    spec=mock.MagicMock(),
                    version=1,
                    resource_scope=resource_scope)
            return mgr

    def test_incarnation_scope_survives_base_initialization(self):
        mgr = self._build(lambda self_: None, [], 'incarnation-a')
        assert mgr._resource_scope == 'incarnation-a'

    def test_lock_is_held_by_recovery_when_daemons_start(self):
        import threading as threading_mod
        release = threading_mod.Event()
        lock_state_at_daemon_start = []

        def _slow_recovery(self_):
            release.wait(timeout=10)

        started = []
        mgr = self._build(_slow_recovery, started)
        # __init__ returned while recovery is still running (non-blocking
        # boot), and the manager lock was already held when it returned —
        # so any daemon started afterwards cannot win it.
        assert mgr.lock.locked() is True
        assert len(started) == 3
        lock_state_at_daemon_start.append(mgr.lock.locked())
        release.set()
        # Recovery finishes and releases the lock.
        for _ in range(100):
            if not mgr.lock.locked():
                break
            import time as time_mod
            time_mod.sleep(0.05)
        assert mgr.lock.locked() is False

    def test_all_three_daemon_threads_are_started(self):
        started = []
        self._build(lambda self_: None, started)
        assert '_thread_pool_refresher' in started
        assert '_job_status_fetcher' in started
        assert '_replica_prober' in started


def _make_manager(service_name='svc', next_replica_id=1):
    """Build a bare SkyPilotReplicaManager with only the attributes the
    recovery / scale-up id-allocator and version-spec lookup paths touch,
    skipping the heavy __init__ (yaml parse, spot placer, daemon threads)."""
    mgr = object.__new__(replica_managers.SkyPilotReplicaManager)
    mgr.lock = threading.RLock()
    mgr._service_name = service_name
    mgr._next_replica_id = next_replica_id
    mgr._launch_thread_pool = {}
    mgr._down_thread_pool = {}
    mgr._tick_version_spec_cache = {}
    mgr._spot_placer = None
    return mgr


def _fake_replica_info(replica_id, status=None):
    info = mock.Mock()
    info.replica_id = replica_id
    # Explicit, inert lifecycle fields: a bare Mock attribute is truthy and
    # would accidentally route the fake into the spot-orphan / re-drive
    # teardown scans of `_recover_replica_operations`.
    info.status = status
    info.is_spot = False
    info.status_property.preempted = False
    info.status_property.is_scale_down = False
    info.status_property.purged = False
    return info


def _record_launch(launched):
    """A _launch_replica side_effect that records the allocated replica id.

    Returns True per the production contract ("launch enqueued"): the id
    allocator only advances past an id whose launch was actually enqueued.
    """

    def _side_effect(replica_id, _resources_override):
        launched.append(replica_id)
        return True

    return _side_effect


class TestReplicaIdSeededOnRecovery:
    """`_recover_replica_operations` must advance `_next_replica_id` past every
    persisted replica id.

    A fresh ReplicaManager starts `_next_replica_id` at 1. On a controller
    respawn (consolidation-mode pod restart re-running `_start`, or the
    in-place controller-respawn path) a brand-new ReplicaManager is built,
    resetting the allocator to 1 while replicas 1..N survive in the DB. The
    next `scale_up` would then reuse a live id, and `add_or_update_replica`
    (upsert on (service_name, replica_id)) would overwrite the surviving
    replica's persisted ReplicaInfo and re-launch its live serving cluster.
    Seeding the allocator from durable state prevents the collision.
    """

    def test_seeds_past_max_existing_id(self):
        mgr = _make_manager(next_replica_id=1)
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=[
                    _fake_replica_info(1),
                    _fake_replica_info(2),
                    _fake_replica_info(5),
                ]):
            mgr._recover_replica_operations()
        # max existing id is 5 -> next must be 6, NOT 1 (the reset value).
        assert mgr._next_replica_id == 6

    def test_first_run_keeps_id_at_one(self):
        # No replicas yet (first `up`, not a recovery) -> allocator unchanged.
        mgr = _make_manager(next_replica_id=1)
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=[]):
            mgr._recover_replica_operations()
        assert mgr._next_replica_id == 1


class TestScaleUpDoesNotClobberLiveReplica:
    """Defensive guard: `scale_up` must never allocate an id that still has a
    durable replica row, even if the allocator somehow drifted."""

    def test_allocates_fresh_id_normally(self):
        mgr = _make_manager(next_replica_id=6)
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_info_from_id',
                return_value=None), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up()
        assert launched == [6]
        assert mgr._next_replica_id == 7

    def test_skips_ids_with_existing_rows(self):
        # _next_replica_id points at 6, but 6 and 7 still have live rows;
        # 8 is free. scale_up must skip 6 and 7 and launch 8.
        mgr = _make_manager(next_replica_id=6)
        launched = []
        existing = {6, 7}

        def _get(_service_name, replica_id):
            return mock.Mock() if replica_id in existing else None

        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_info_from_id',
                side_effect=_get), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up()
        assert launched == [8]
        assert mgr._next_replica_id == 9


class TestVersionSpecMemoizedPerProbeRound:
    """`_get_version_spec` reads each version's spec from the DB at most once
    per probe round.

    The readiness prober resolves the spec for every replica 4x per tick
    (path / post_data / headers / timeout), each a `serve_state.get_spec`
    (SQL SELECT + pickle.loads). The tick-scoped `_tick_version_spec_cache`
    collapses those 4*N reads into one per distinct version, and is reset
    each probe round so a rewritten spec is never served stale across rounds.
    """

    def test_memoizes_within_a_round_and_rereads_after_reset(self):
        mgr = _make_manager()
        calls = []

        def _get_spec(_service_name, version):
            calls.append(version)
            return mock.Mock()

        with mock.patch('sky.serve.replica_managers.serve_state.get_spec',
                        side_effect=_get_spec):
            # One round: 4 lookups for v1 + 2 for v2 -> 1 DB read per version.
            for _ in range(4):
                mgr._get_version_spec(1)
            for _ in range(2):
                mgr._get_version_spec(2)
            assert calls == [1, 2]
            # New round: the cache is reset (as _probe_all_replicas /
            # _replica_prober do) -> the version is re-read from the DB.
            mgr._tick_version_spec_cache = {}
            mgr._get_version_spec(1)
            assert calls == [1, 2, 1]

    def test_raises_when_version_missing(self):
        mgr = _make_manager()
        with mock.patch('sky.serve.replica_managers.serve_state.get_spec',
                        return_value=None):
            with pytest.raises(ValueError):
                mgr._get_version_spec(99)
        # A missing version must not be cached as a hit.
        assert 99 not in mgr._tick_version_spec_cache


def _capacity_error() -> exceptions.ResourcesUnavailableError:
    return exceptions.ResourcesUnavailableError(
        'no capacity',
        failover_history=[
            exceptions.ResourcesUnavailableError('zone exhausted')
        ])


class TestLaunchClusterRetry:
    """`launch_cluster` must fail fast ONLY on resource availability
    (capacity) failures when `availability_max_retry` caps them; other
    (transient) errors must keep the `max_retry` in-place attempts."""

    def _run_launch_cluster(self, tmp_path, stream_side_effects, **kwargs):
        """Run launch_cluster with a mocked SDK.

        Each element of stream_side_effects is one launch attempt: an
        exception to raise from sdk.stream_and_get, or None for success.
        Returns (mock_sdk, mock_terminate, raised RuntimeError or None).
        """
        raised = None
        with mock.patch(
                'sky.serve.replica_managers.task_lib.Task.from_yaml_str',
                return_value=mock.MagicMock()), \
             mock.patch('sky.serve.replica_managers.usage_lib'), \
             mock.patch('sky.serve.replica_managers.sdk') as mock_sdk, \
             mock.patch('sky.serve.replica_managers.terminate_cluster'
                       ) as mock_terminate, \
             mock.patch('sky.serve.replica_managers.common_utils.Backoff'
                       ) as mock_backoff:
            # Skip the (up to 60s) backoff between attempts.
            mock_backoff.return_value.current_backoff.return_value = 0
            mock_sdk.launch.return_value = 'request-id'
            mock_sdk.stream_and_get.side_effect = stream_side_effects
            try:
                replica_managers.launch_cluster(
                    replica_id=1,
                    yaml_content='dummy: yaml',
                    cluster_name='svc-1',
                    log_file=str(tmp_path / 'launch.log'),
                    replica_to_request_id=thread_utils.ThreadSafeDict(),
                    replica_to_launch_cancelled=thread_utils.ThreadSafeDict(),
                    **kwargs)
            except RuntimeError as e:
                raised = e
        return mock_sdk, mock_terminate, raised

    def test_capacity_failure_fails_fast_with_availability_max_retry(
            self, tmp_path):
        """One capacity failure with availability_max_retry=1 must raise
        immediately (no in-place retry of the same exhausted location)."""
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [_capacity_error()] * 3, availability_max_retry=1)
        assert raised is not None
        assert mock_sdk.launch.call_count == 1
        assert mock_terminate.call_count == 1

    def test_transient_failures_keep_in_place_retries(self, tmp_path):
        """Transient (non-availability) errors must still be retried in
        place even when availability_max_retry=1, so a one-off blip does
        not poison the placer location."""
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path,
            [RuntimeError('transient'),
             RuntimeError('transient'), None],
            availability_max_retry=1)
        assert raised is None
        assert mock_sdk.launch.call_count == 3
        assert mock_sdk.stream_and_get.call_count == 3
        assert mock_terminate.call_count == 2

    def test_capacity_failures_default_to_max_retry(self, tmp_path):
        """Without availability_max_retry, capacity failures keep the
        default max_retry in-place attempts."""
        mock_sdk, _, raised = self._run_launch_cluster(tmp_path,
                                                       [_capacity_error()] * 3)
        assert raised is not None
        assert mock_sdk.launch.call_count == 3

    def test_authoritative_prelaunch_guard_rejects_cloud_mutation(
            self, tmp_path):
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [None], pre_launch_guard=lambda: False)
        assert raised is not None
        assert 'ownership was lost' in str(raised)
        mock_sdk.launch.assert_not_called()
        mock_terminate.assert_not_called()

    def test_inflight_owner_watchdog_cancels_request(self, tmp_path):
        allowed = threading.Event()
        watchdog_observed_loss = threading.Event()
        allowed.set()

        def _continue_guard():
            if allowed.is_set():
                return True
            watchdog_observed_loss.set()
            return False

        def _block_while_watchdog_runs(_request_id):
            allowed.clear()
            assert watchdog_observed_loss.wait(timeout=5)
            raise RuntimeError('request cancelled')

        with mock.patch(
                'sky.serve.replica_managers.'
                '_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS', 0.01):
            mock_sdk, _, raised = self._run_launch_cluster(
                tmp_path,
                _block_while_watchdog_runs,
                continue_guard=_continue_guard)

        assert raised is not None
        assert 'ownership loss' in str(raised)
        mock_sdk.api_cancel.assert_called_once_with('request-id')


class TestLaunchReplicaAvailabilityMaxRetry:
    """`_launch_replica` must cap availability failures at one attempt only
    for spot replicas managed by a spot placer."""

    def _launch_replica(self, use_spot: bool, with_placer: bool):
        # pylint: disable=protected-access
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager.yaml_content = 'dummy: yaml'
        manager.latest_version = 1
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        placer = None
        if with_placer:
            placer = mock.Mock()
            location = mock.Mock()
            location.to_dict.return_value = {'zone': 'z'}
            placer.select_next_location.return_value = location
        manager._spot_placer = placer

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=use_spot), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos',
                 return_value=[]), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state'
                 '.add_or_update_replica'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers.ReplicaInfo'), \
             mock.patch('sky.serve.replica_managers.thread_utils.SafeThread'
                       ) as mock_thread:
            manager._launch_replica(replica_id=1)
        return mock_thread.call_args

    def test_spot_with_placer_fails_fast_on_availability(self):
        call = self._launch_replica(use_spot=True, with_placer=True)
        assert call.kwargs['kwargs']['availability_max_retry'] == 1
        assert callable(call.kwargs['kwargs']['pre_launch_guard'])
        assert callable(call.kwargs['kwargs']['continue_guard'])
        # retry_until_up must be False: failover is owned by the placer.
        assert call.kwargs['args'][-1] is False

    def test_spot_without_placer_keeps_default_retries(self):
        call = self._launch_replica(use_spot=True, with_placer=False)
        assert call.kwargs['kwargs']['availability_max_retry'] is None
        assert call.kwargs['args'][-1] is True

    def test_non_spot_with_placer_keeps_default_retries(self):
        """A non-spot (on-demand fallback) replica keeps the default
        retries even when the service has a spot placer."""
        call = self._launch_replica(use_spot=False, with_placer=True)
        assert call.kwargs['kwargs']['availability_max_retry'] is None
        assert call.kwargs['args'][-1] is True


class TestUpdateVersionHoldsManagerLock:
    """`update_version` must serialize on the manager lock.

    It runs on the controller's HTTP-handler thread while the autoscaler /
    prober daemon threads hold `self.lock` for their own read-modify-write
    cycles; without the lock a concurrent `scale_up` can read a torn
    (latest_version, yaml_content) pair and replica-row upserts can be lost.
    """

    def test_update_version_blocks_until_lock_released(self):
        mgr = _make_manager()
        mgr.lock = threading.Lock()
        mgr.latest_version = 5
        entered = threading.Event()
        done = threading.Event()

        def _call():
            entered.set()
            # version <= latest_version returns right after the lock is
            # acquired, so completion is a proxy for lock acquisition.
            mgr.update_version(1,
                               mock.Mock(),
                               update_mode=serve_utils.UpdateMode.ROLLING)
            done.set()

        thread = threading.Thread(target=_call, daemon=True)
        with mgr.lock:  # simulate a daemon thread mid read-modify-write
            thread.start()
            assert entered.wait(timeout=5)
            assert not done.wait(timeout=0.5)
        assert done.wait(timeout=5)
        thread.join(timeout=5)


class TestLaunchOwnershipFence:
    """A stale manager must never start work that was only queued locally."""

    @staticmethod
    def _pending_info():
        info = mock.Mock()
        info.status = replica_managers.serve_state.ReplicaStatus.PENDING
        info.status_property = mock.Mock()
        return info

    @staticmethod
    def _owned_manager():
        mgr = _make_manager()
        mgr._service_hash = 'incarnation-a'
        mgr._controller_owner = (101, '10.0.0.1')
        mgr._ownership_lost = threading.Event()
        return mgr

    @classmethod
    def _queued_manager(cls, replica_ids):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr.least_recent_version = 1
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()

        infos = {}
        for replica_id in replica_ids:
            thread = mock.Mock()
            thread.is_alive.return_value = False
            thread.format_exc = None
            mgr._launch_thread_pool[replica_id] = thread
            mgr._replica_to_request_id[replica_id] = f'req-{replica_id}'
            mgr._replica_to_launch_cancelled[replica_id] = False
            infos[replica_id] = cls._pending_info()
        return mgr, infos

    def test_recovering_exact_owner_may_launch_from_controller_failed(self):
        mgr = self._owned_manager()
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'status':
                replica_managers.serve_state.ServiceStatus.CONTROLLER_FAILED,
        }
        with mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=owner):
            assert mgr._service_is_launch_authorized()
        assert not mgr._ownership_lost.is_set()

    def test_shutting_down_exact_owner_is_fenced(self):
        mgr = self._owned_manager()
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'status': replica_managers.serve_state.ServiceStatus.SHUTTING_DOWN,
        }
        with mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=owner):
            assert not mgr._service_is_launch_authorized()
        assert mgr._ownership_lost.is_set()

    def test_nonconsolidated_controller_omits_api_local_fence(self):
        mgr = self._owned_manager()
        mgr._enforce_launch_fence = False
        assert mgr._replica_launch_fence_context() is None

    def test_transient_owner_lookup_fails_attempt_without_latching_loss(self):
        mgr = self._owned_manager()
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'status': replica_managers.serve_state.ServiceStatus.READY,
        }
        with mock.patch.object(
                replica_managers.serve_state,
                'get_service_controller_owner',
                side_effect=[RuntimeError('database restarting'), owner]):
            # The current launch fails closed, but the manager remains able to
            # prove ownership and recover on the next check.
            assert not mgr._service_is_launch_authorized()
            assert not mgr._ownership_lost.is_set()
            assert mgr._service_is_launch_authorized()
        assert not mgr._ownership_lost.is_set()

    def test_owner_watchdog_retries_transient_lookup(self):
        mgr = self._owned_manager()
        current_owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'status': replica_managers.serve_state.ServiceStatus.READY,
        }
        replacement_owner = {
            **current_owner,
            'controller_pid': 202,
        }
        with mock.patch.object(
                replica_managers.serve_state,
                'get_service_controller_owner',
                side_effect=[
                    RuntimeError('database restarting'), current_owner,
                    replacement_owner
                ]) as get_owner, \
             mock.patch.object(
                 replica_managers,
                 '_SERVICE_OWNER_WATCH_INTERVAL_SECONDS',
                 0):
            mgr._service_owner_watchdog()

        assert get_owner.call_count == 3
        assert mgr._ownership_lost.is_set()

    def test_transient_lookup_defers_queued_launch_instead_of_discarding(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr.least_recent_version = 1
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = False
        mgr._launch_thread_pool[1] = launch_thread
        info = self._pending_info()

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=None), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        launch_thread.start.assert_not_called()
        assert mgr._launch_thread_pool[1] is launch_thread
        persist.assert_not_called()

    def test_transient_lookup_is_shared_across_queued_launches(self):
        mgr, infos = self._queued_manager([1, 2, 3])

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=None) as authorize, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               side_effect=lambda _svc, rid: infos[rid]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        assert authorize.call_count == 1
        for launch_thread in mgr._launch_thread_pool.values():
            launch_thread.start.assert_not_called()
        assert len(mgr._launch_thread_pool) == 3
        for replica_id in (1, 2, 3):
            assert replica_id in mgr._launch_thread_pool
        persist.assert_not_called()

    def test_stale_queued_launch_is_discarded_without_deleting_row(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr.least_recent_version = 1
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = False
        mgr._launch_thread_pool[1] = launch_thread
        info = self._pending_info()

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=False), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        launch_thread.start.assert_not_called()
        assert 1 not in mgr._launch_thread_pool
        persist.assert_not_called()

    def test_stale_lookup_is_shared_across_queued_launches(self):
        mgr, infos = self._queued_manager([1, 2, 3])

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=False) as authorize, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               side_effect=lambda _svc, rid: infos[rid]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        assert authorize.call_count == 1
        assert len(mgr._launch_thread_pool) == 0
        assert len(mgr._replica_to_request_id) == 0
        assert len(mgr._replica_to_launch_cancelled) == 0
        persist.assert_not_called()

    def test_authorized_lookup_is_shared_across_queued_launches(self, tmp_path):
        mgr, infos = self._queued_manager([1, 2, 3])

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=True) as authorize, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               side_effect=lambda _svc, rid: infos[rid]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_provision',
                               return_value=True), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        assert authorize.call_count == 1
        for info in infos.values():
            assert (info.status_property.sky_launch_status ==
                    common_utils.ProcessStatus.RUNNING)
        for launch_thread in mgr._launch_thread_pool.values():
            launch_thread.start.assert_called_once_with()
        assert persist.call_count == 3

    def test_old_version_retirement_leaves_storage_to_durable_intents(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr.least_recent_version = 1
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        info = mock.Mock(version=2)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_service_versions',
                               return_value=[1, 2]), \
             mock.patch.object(replica_managers.serve_utils,
                               'get_yaml_content',
                               ) as get_yaml_content, \
             mock.patch.object(replica_managers.serve_state,
                               'delete_version',
                               return_value=True) as delete_version:
            mgr._refresh_thread_pool()

        delete_version.assert_called_once_with('svc', 1)
        get_yaml_content.assert_not_called()

    def test_old_version_scoped_storage_is_not_deleted_during_retirement(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._resource_scope = 'incarnation-a'
        mgr.least_recent_version = 1
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        info = mock.Mock(version=2)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos', return_value=[info]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_service_versions', return_value=[1, 2]), \
             mock.patch.object(replica_managers.serve_utils,
                               'get_yaml_content') as get_yaml_content, \
             mock.patch.object(replica_managers.serve_state,
                               'delete_version', return_value=True) as delete:
            mgr._refresh_thread_pool()

        get_yaml_content.assert_not_called()
        delete.assert_called_once_with('svc', 1)
        assert mgr.least_recent_version == 2

    def test_old_version_metadata_delete_failure_keeps_retry_pointer(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._resource_scope = 'incarnation-a'
        mgr.least_recent_version = 1
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        info = mock.Mock(version=2)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos', return_value=[info]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_service_versions', return_value=[1, 2]), \
             mock.patch.object(replica_managers.serve_state,
                               'delete_version', return_value=False) as delete, \
             pytest.raises(RuntimeError, match='incarnation changed'):
            mgr._refresh_thread_pool()

        delete.assert_called_once_with('svc', 1)
        assert mgr.least_recent_version == 1


class TestCloudInstanceLooksAlive:
    """The spot-preemption pre-filter must be cloud-API-only and
    conservative: it decides whether a failed readiness probe warrants the
    expensive full `_handle_preemption` path (forced refresh under the
    manager lock). During a fleet cold start every not-yet-listening spot
    replica fails its probe, so the common case must be one cheap provider
    call confirming the instance is up."""

    @staticmethod
    def _spot_info():
        info = mock.Mock()
        info.is_spot = True
        info.cluster_name = 'svc-1'
        info.replica_id = 1
        return info

    def _run(self, handle, statuses=None, side_effect=None):
        mgr = _make_manager()
        with mock.patch(
                'sky.serve.replica_managers.global_user_state.'
                'get_handle_from_cluster_name',
                return_value=handle), \
             mock.patch(
                 'sky.serve.replica_managers.backend_utils.'
                 'query_cluster_instance_statuses',
                 return_value=statuses,
                 side_effect=side_effect) as query:
            result = mgr._cloud_instance_looks_alive(self._spot_info())
        return result, query

    @staticmethod
    def _handle(launched_nodes=1):
        handle = mock.Mock(
            spec=replica_managers.backends.CloudVmRayResourceHandle)
        handle.launched_nodes = launched_nodes
        return handle

    def test_running_instance_counts_as_alive(self):
        from sky.utils import status_lib
        result, query = self._run(
            self._handle(),
            statuses={'i-1': (status_lib.ClusterStatus.UP, None)})
        assert result is True
        query.assert_called_once()

    def test_partially_up_multinode_counts_as_dead(self):
        # Mirrors the full refresh's partial-cluster semantics: a 2-node
        # replica with only 1 instance UP is abnormal, not alive.
        from sky.utils import status_lib
        result, _ = self._run(
            self._handle(launched_nodes=2),
            statuses={'i-1': (status_lib.ClusterStatus.UP, None)})
        assert result is False

    def test_multinode_with_stopped_member_counts_as_dead(self):
        from sky.utils import status_lib
        result, _ = self._run(self._handle(launched_nodes=2),
                              statuses={
                                  'i-1': (status_lib.ClusterStatus.UP, None),
                                  'i-2': (status_lib.ClusterStatus.STOPPED,
                                          'preempted'),
                              })
        assert result is False

    def test_no_instances_counts_as_dead(self):
        result, _ = self._run(self._handle(), statuses={})
        assert result is False

    def test_stopped_instance_counts_as_dead(self):
        from sky.utils import status_lib
        result, _ = self._run(
            self._handle(),
            statuses={'i-1': (status_lib.ClusterStatus.STOPPED, 'preempted')})
        assert result is False

    def test_provider_error_counts_as_alive(self):
        # A transient provider error must not stampede a cold-starting
        # fleet into forced refreshes.
        result, _ = self._run(self._handle(),
                              side_effect=RuntimeError('throttled'))
        assert result is True

    def test_missing_handle_routes_to_full_path(self):
        # No handle -> NOT alive, so the full _handle_preemption (which
        # logs and handles the missing-handle case) runs.
        result, query = self._run(handle=None)
        assert result is False
        query.assert_not_called()


class TestScaleUpBatch:
    """A batch of scale-ups must run under ONE manager-lock acquisition:
    the probe round holds the lock for tens of seconds per round on large
    fleets, so per-replica acquisitions trickle through the gaps and
    become the fleet-scale launch bottleneck (measured live at a
    1000-target fleet)."""

    class _CountingLock:

        def __init__(self):
            self.acquisitions = 0

        def __enter__(self):
            self.acquisitions += 1
            return self

        def __exit__(self, *args):
            return False

    def test_batch_launches_all_with_one_lock_acquisition(self):
        mgr = _make_manager(next_replica_id=1)
        lock = self._CountingLock()
        mgr.lock = lock
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_info_from_id',
                return_value=None), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up_batch([None, {'use_spot': True}, None])
        assert launched == [1, 2, 3]
        assert mgr._next_replica_id == 4
        assert lock.acquisitions == 1

    def test_single_scale_up_unchanged(self):
        mgr = _make_manager(next_replica_id=7)
        lock = self._CountingLock()
        mgr.lock = lock
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_info_from_id',
                return_value=None), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up()
        assert launched == [7]
        assert lock.acquisitions == 1

    def test_batch_skips_ids_with_existing_rows(self):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        launched = []
        existing = {2}

        def _get(_service_name, replica_id):
            return mock.Mock() if replica_id in existing else None

        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_info_from_id',
                side_effect=_get), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up_batch([None, None])
        assert launched == [1, 3]

    def test_spot_batch_reuses_one_replica_snapshot(self):
        """K placer launches must scan/unpickle the N-row table once.

        The shared list must also accumulate each newly enqueued replica so
        later placements in the wave see the same in-wave load that the old
        per-launch committed DB scans exposed.
        """
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr._spot_placer = mock.Mock()
        mgr.yaml_content = 'dummy: yaml'
        initial = [_fake_replica_info(40), _fake_replica_info(41)]
        snapshots = []

        def _launch(replica_id,
                    _resources_override,
                    existing_replica_infos=None):
            assert existing_replica_infos is not None
            snapshots.append(
                (existing_replica_infos, len(existing_replica_infos)))
            existing_replica_infos.append(_fake_replica_info(replica_id))
            return True

        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_info_from_id', return_value=None), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos',
                 return_value=list(initial)) as scan, \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr.scale_up_batch([{'use_spot': True}] * 3)

        scan.assert_called_once_with('svc')
        assert [size for _, size in snapshots] == [2, 3, 4]
        assert all(snapshot is snapshots[0][0] for snapshot, _ in snapshots)

    def test_on_demand_batch_does_not_add_replica_scan(self):
        """Explicit on-demand pins do not ask the placer for a location."""
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr._spot_placer = mock.Mock()
        mgr.yaml_content = 'dummy: yaml'
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_info_from_id', return_value=None), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos'
             ) as scan, \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up_batch([{'use_spot': False}] * 3)
        assert launched == [1, 2, 3]
        scan.assert_not_called()


class TestLaunchReplicaSnapshotAccumulation:
    """Bulk launches sharing one snapshot must see in-wave placements.

    Recovery re-drive passes a single existing_replica_infos snapshot
    across a whole wave of launches; without appending each newly placed
    replica, every launch in the wave computes identical load counts and
    the spot placer pins the entire wave to one location.
    """

    def _make_manager(self, placer):
        # pylint: disable=protected-access
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager.yaml_content = 'dummy: yaml'
        manager.latest_version = 1
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        manager._spot_placer = placer
        return manager

    def test_wave_launches_see_prior_in_wave_placements(self):
        # pylint: disable=protected-access
        placer = mock.Mock()
        seen_current_locations = []

        def _select(current_locations):
            seen_current_locations.append(list(current_locations))
            location = mock.Mock()
            location.to_dict.return_value = {'zone': 'z'}
            return location

        placer.select_next_location.side_effect = _select
        manager = self._make_manager(placer)
        shared_snapshot = []

        def _fake_replica_info_ctor(replica_id, *_args, **_kwargs):
            info = mock.Mock()
            info.replica_id = replica_id
            info.is_spot = True
            return info

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers.ReplicaInfo',
                        side_effect=_fake_replica_info_ctor), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state'
                 '.add_or_update_replica'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers.thread_utils.SafeThread'):
            manager._launch_replica(replica_id=1,
                                    existing_replica_infos=shared_snapshot)
            manager._launch_replica(replica_id=2,
                                    existing_replica_infos=shared_snapshot)

        # The snapshot accumulated both newly placed replicas...
        assert len(shared_snapshot) == 2
        assert [info.replica_id for info in shared_snapshot] == [1, 2]
        # ...and the second placement saw the first replica's location.
        assert seen_current_locations[0] == []
        assert len(seen_current_locations[1]) == 1

    def test_fresh_scan_path_does_not_leak_appends(self):
        # pylint: disable=protected-access
        placer = mock.Mock()
        location = mock.Mock()
        location.to_dict.return_value = {'zone': 'z'}
        placer.select_next_location.return_value = location
        manager = self._make_manager(placer)

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos',
                 return_value=[]) as mock_scan, \
             mock.patch(
                 'sky.serve.replica_managers.serve_state'
                 '.add_or_update_replica'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers.thread_utils.SafeThread'):
            manager._launch_replica(replica_id=1)
        # Without a caller-provided snapshot each launch scans fresh state.
        assert mock_scan.call_count == 1


class TestRecoveryRetryAndIsolation:
    """A failed recovery pass must retry (previously a recovery exception
    failed the boot and the HA daemon retried via respawn; the recovery
    thread must not die silently and strand un-redriven replicas), and one
    bad replica must not abort re-driving the rest."""

    def test_orphaned_spot_intent_persist_is_owner_fenced(self):
        mgr = _make_manager()
        mgr._service_hash = 'incarnation-a'
        mgr._controller_owner = (123, '10.0.0.1')
        mgr._spot_placer = None

        info = mock.MagicMock()
        info.replica_id = 1
        info.cluster_name = 'svc-1-incarnation'
        info.is_spot = True
        info.status = replica_managers.serve_state.ReplicaStatus.READY
        info.status_property.preempted = False
        info.status_property.is_scale_down = False
        info.status_property.purged = False
        info.get_spot_location.return_value = None

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'add_or_update_replica',
                 return_value=True) as persist, \
             mock.patch.object(mgr, '_terminate_replica'):
            mgr._recover_replica_operations()

        persist.assert_called_once_with('svc',
                                        1,
                                        info,
                                        expected_service_hash='incarnation-a',
                                        expected_controller_owner=(123,
                                                                   '10.0.0.1'))

    def test_one_bad_launch_does_not_strand_the_rest(self):
        mgr = _make_manager(next_replica_id=1)
        launched = []

        def _launch(replica_id,
                    resources_override=None,
                    existing_replica_infos=None,
                    prior_reserved_fill=False):
            del resources_override, existing_replica_infos
            del prior_reserved_fill
            if replica_id == 2:
                raise RuntimeError('boom')
            launched.append(replica_id)

        infos = [
            _fake_replica_info(
                i,
                status=replica_managers.serve_state.ReplicaStatus.PROVISIONING)
            for i in (1, 2, 3)
        ]
        for info in infos:
            info.resources_override = None
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr._recover_replica_operations()
        # Replica 2 failed; 1 and 3 still re-driven.
        assert launched == [1, 3]

    def test_redrive_preserves_reserved_fill_attribution(self):
        # A fill replica surviving a controller respawn is re-driven with
        # its persisted (sentinel-stripped) override; the replacement row
        # must keep reserved_fill=True or the replica silently converts
        # to ceiling-exempt "demand" and can starve peers forever. Demand
        # rows must stay False.
        mgr = _make_manager()
        mgr.yaml_content = 'dummy: yaml'
        mgr.latest_version = 1
        mgr._spot_placer = None
        mgr._replica_to_request_id = {}
        mgr._replica_to_launch_cancelled = {}
        provisioning = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        fill_row = _fake_replica_info(1, status=provisioning)
        fill_row.resources_override = None
        fill_row.reserved_fill = True
        demand_row = _fake_replica_info(2, status=provisioning)
        demand_row.resources_override = None
        demand_row.reserved_fill = False
        persisted: dict = {}
        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=False), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils.'
                 'generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos',
                 return_value=[fill_row, demand_row]), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.'
                 'add_or_update_replica',
                 side_effect=lambda _svc, rid, info: persisted.__setitem__(
                     rid, info)), \
             mock.patch('sky.serve.replica_managers.thread_utils.SafeThread'):
            mgr._recover_replica_operations()
        assert persisted[1].reserved_fill is True
        assert persisted[2].reserved_fill is False

    def test_reentry_with_enqueued_threads_is_tolerated(self):
        mgr = _make_manager(next_replica_id=1)
        mgr._launch_thread_pool = {7: mock.Mock()}
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=[]):
            # Previously an assert; on a retry pass this must not raise.
            mgr._recover_replica_operations()


class TestRecoverySingleSnapshot:
    """The recovery pass must read the replica table exactly once.

    It previously fetched (and unpickled) the whole table three times: once
    for the snapshot and once per `get_replicas_at_status(PROVISIONING /
    PENDING)` call. Beyond the wasted O(3 x rows) work at fleet scale, the
    reads could diverge: the re-drive list, the id-allocator seed, and the
    `existing_replica_infos` snapshot passed to `_launch_replica` must all
    describe the same durable state.
    """

    @staticmethod
    def _statuses(*statuses):
        return [
            _fake_replica_info(i + 1, status=status)
            for i, status in enumerate(statuses)
        ]

    def test_replica_table_is_read_exactly_once(self):
        mgr = _make_manager()
        infos = self._statuses(
            replica_managers.serve_state.ReplicaStatus.PROVISIONING,
            replica_managers.serve_state.ReplicaStatus.PENDING,
            replica_managers.serve_state.ReplicaStatus.READY,
        )
        for info in infos:
            info.resources_override = None
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos) as scan, \
             mock.patch.object(mgr, '_launch_replica'):
            mgr._recover_replica_operations()
        assert scan.call_count == 1

    def test_provisioning_redriven_before_pending(self):
        # PROVISIONING replicas were previously launched and may hold live
        # cloud resources; they must win the bounded launch queue over
        # PENDING ones regardless of row order.
        mgr = _make_manager()
        infos = self._statuses(
            replica_managers.serve_state.ReplicaStatus.PENDING,
            replica_managers.serve_state.ReplicaStatus.PROVISIONING,
            replica_managers.serve_state.ReplicaStatus.PENDING,
            replica_managers.serve_state.ReplicaStatus.PROVISIONING,
        )
        for info in infos:
            info.resources_override = None
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos), \
             mock.patch.object(
                 mgr, '_launch_replica',
                 side_effect=lambda replica_id, **_: launched.append(
                     replica_id)):
            mgr._recover_replica_operations()
        assert launched == [2, 4, 1, 3]

    def test_launch_redrive_reuses_the_snapshot(self):
        # `existing_replica_infos` handed to each re-driven launch must be
        # the same object as the recovery snapshot (no per-launch re-scan).
        mgr = _make_manager()
        infos = self._statuses(
            replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        infos[0].resources_override = None
        seen_snapshots = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos), \
             mock.patch.object(
                 mgr, '_launch_replica',
                 side_effect=lambda replica_id, existing_replica_infos=None,
                 **_: seen_snapshots.append(existing_replica_infos)):
            mgr._recover_replica_operations()
        assert seen_snapshots == [infos]
        assert seen_snapshots[0] is infos
