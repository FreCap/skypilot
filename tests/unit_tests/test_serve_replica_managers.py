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
import dataclasses
import logging
import threading
import types
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
                 'sky.serve.replica_managers.load_task_with_service_spec',
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

    def test_legacy_per_gpu_yaml_uses_persisted_physical_semantics(self):
        legacy_yaml = """
resources:
  cpus: 1
  ports: 8080
  accelerators: A100:1
  use_spot: true
service:
  readiness_probe: /health
  replica_policy:
    min_replicas: 1
    max_replicas: 8
    target_concurrency_per_replica: 2
    spot_placer: dynamic_fallback_per_gpu
run: echo hi
"""
        # This committed YAML predates implicit logical replicas and is
        # intentionally invalid under the current service policy because it
        # lacks the required async-occupancy signal. Recovery must load
        # resources around the persisted physical spec rather than applying
        # today's hidden default to historical state.
        with pytest.raises(ValueError,
                           match='graceful_drain_async_occupancy: true'):
            replica_managers.task_lib.Task.from_yaml_str(legacy_yaml)

        persisted_spec = mock.MagicMock()
        persisted_spec.pool = False
        persisted_spec.uses_logical_replicas = False
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_yaml_content',
                return_value=legacy_yaml), \
             mock.patch(
                 'sky.serve.replica_managers.task_lib.Task.from_yaml_str',
                 side_effect=AssertionError('must not reparse service')), \
             mock.patch(
                 'sky.serve.replica_managers.spot_placer.SpotPlacer.from_task',
                 return_value=None), \
             mock.patch.object(
                 replica_managers.SkyPilotReplicaManager,
                 '_recover_replica_operations'), \
             mock.patch(
                 'sky.serve.replica_managers.thread_utils.'
                 'start_supervised_thread'):
            manager = replica_managers.SkyPilotReplicaManager(
                service_name='svc', spec=persisted_spec, version=7)

        assert manager._uses_logical_replicas is False
        assert manager._version_specs == {7: persisted_spec}
        assert manager._default_planned_capacity == 1


def _make_manager(service_name='svc', next_replica_id=1):
    """Build a bare SkyPilotReplicaManager with only the attributes the
    recovery / scale-up id-allocator and version-spec lookup paths touch,
    skipping the heavy __init__ (yaml parse, spot placer, daemon threads)."""
    mgr = object.__new__(replica_managers.SkyPilotReplicaManager)
    mgr.lock = threading.RLock()
    mgr._service_name = service_name
    mgr._next_replica_id = next_replica_id
    mgr.latest_version = 1
    mgr.yaml_content = 'resources: {}'
    mgr._launch_thread_pool = {}
    mgr._down_thread_pool = {}
    mgr._failed_cleanup_retry_attempts = {}
    mgr._failed_cleanup_retry_at = {}
    mgr._tick_version_spec_cache = {}
    mgr._spot_placer = None
    mgr._pending_version = None
    mgr._uses_logical_replicas = False
    mgr._logical_reconcile_snapshot = None
    mgr._logical_target = None
    mgr._logical_state_lock = threading.RLock()
    mgr._logical_controller_epoch = 'test-controller-epoch'
    mgr._wait_for_idle_trackers = {}
    mgr._recovering_logical_retirement_ids = set()
    mgr._logical_retirement_recovery_deadline = None
    mgr._logical_retirement_reactivation_generation = None
    return mgr


def _fake_replica_info(replica_id, status=None):
    info = mock.Mock()
    info.replica_id = replica_id
    info.version = 1
    # Explicit, inert lifecycle fields: a bare Mock attribute is truthy and
    # would accidentally route the fake into the spot-orphan / re-drive
    # teardown scans of `_recover_replica_operations`.
    info.status = status
    info.is_spot = False
    info.status_property.preempted = False
    info.status_property.is_scale_down = False
    info.status_property.purged = False
    return info


class TestLaunchCancellationWait:

    @staticmethod
    def _run(monkeypatch,
             launch_thread,
             *,
             on_sleep=None,
             forbid_wall_clock=False):
        manager = _make_manager()
        manager._is_pool = False
        manager._resource_scope = None
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        manager._launch_thread_pool[1] = launch_thread
        # Stop after the launch wait. Down-thread creation is exercised by
        # the cleanup tests below and would only obscure these timing checks.
        manager._down_thread_pool[1] = mock.Mock()
        manager._persist_replica = mock.Mock()
        info = replica_managers.ReplicaInfo(1, 'svc-1', '8080', False, None, 1,
                                            None)

        now = [0.0]
        sleeps = []

        def _sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds
            if on_sleep is not None:
                on_sleep(manager, len(sleeps))

        fake_time = mock.Mock(wraps=replica_managers.time)
        fake_time.monotonic.side_effect = lambda: now[0]
        fake_time.sleep.side_effect = _sleep
        if forbid_wall_clock:
            fake_time.time.side_effect = AssertionError('wall clock consulted')
        monkeypatch.setattr(replica_managers, 'time', fake_time)
        monkeypatch.setattr(replica_managers,
                            '_WAIT_LAUNCH_THREAD_TIMEOUT_SECONDS', 0.15)
        get_info = mock.Mock(return_value=info)
        monkeypatch.setattr(replica_managers.serve_state,
                            'get_replica_info_from_id', get_info)
        cancel = mock.Mock()
        monkeypatch.setattr(replica_managers.sdk, 'api_cancel', cancel)

        manager._terminate_replica(1,
                                   sync_down_logs=False,
                                   replica_drain_delay_seconds=0,
                                   is_scale_down=True)
        manager._persist_replica.assert_called_once_with(1, info)
        get_info.assert_called_once_with('svc', 1)
        launch_thread.join.assert_called_once_with()
        return manager, sleeps, cancel, fake_time

    def test_wait_uses_monotonic_clock_and_clamps_final_sleep(
            self, monkeypatch):
        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = True

        _, sleeps, cancel, fake_time = self._run(monkeypatch,
                                                 launch_thread,
                                                 forbid_wall_clock=True)

        assert sleeps == pytest.approx([0.1, 0.05])
        assert fake_time.monotonic.call_count == 4
        cancel.assert_not_called()

    def test_request_published_at_deadline_is_cancelled(self, monkeypatch):
        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = True

        def _publish_request(manager, sleep_count):
            if sleep_count == 2:
                manager._replica_to_request_id[1] = 'request-1'

        _, sleeps, cancel, fake_time = self._run(monkeypatch,
                                                 launch_thread,
                                                 on_sleep=_publish_request)

        assert sleeps == pytest.approx([0.1, 0.05])
        assert fake_time.monotonic.call_count == 3
        cancel.assert_called_once_with('request-1')

    def test_cancellation_acknowledgement_stops_wait(self, monkeypatch):
        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = True

        def _acknowledge(manager, _sleep_count):
            manager._replica_to_launch_cancelled.pop(1)

        _, sleeps, cancel, fake_time = self._run(monkeypatch,
                                                 launch_thread,
                                                 on_sleep=_acknowledge)

        assert sleeps == [0.1]
        assert fake_time.monotonic.call_count == 2
        cancel.assert_not_called()

    def test_launch_thread_completion_stops_without_sleep(self, monkeypatch):
        launch_thread = mock.Mock()
        launch_thread.is_alive.side_effect = [True, False]

        _, sleeps, cancel, fake_time = self._run(monkeypatch, launch_thread)

        assert not sleeps
        assert fake_time.monotonic.call_count == 1
        cancel.assert_not_called()


def _record_launch(launched):
    """A _launch_replica side_effect that records the allocated replica id.

    Returns True per the production contract ("launch enqueued"): the id
    allocator only advances past an id whose launch was actually enqueued.
    """

    def _side_effect(replica_id, _resources_override):
        launched.append(replica_id)
        return True

    return _side_effect


def test_confirm_logical_bridge_capacity_is_durable_and_monotonic():
    mgr = _make_manager()
    mgr._uses_logical_replicas = True
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='8080',
                                        is_spot=True,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    persisted = []
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[info]), \
         mock.patch.object(mgr,
                           '_persist_replicas',
                           side_effect=persisted.extend):
        confirmed = mgr.confirm_logical_bridge_capacities({1: 8})

    assert confirmed == {1: 8}
    assert info.to_storage_dict()['replica_info_version'] == 11
    assert info.planned_capacity == 8
    assert info.logical_bridge_capacity_verified is True
    assert persisted == [(1, info)]

    # A later smaller runtime observation must affect ready capacity only. It
    # cannot shrink the durable upper bound or cause another DB write.
    persisted.clear()
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[info]), \
         mock.patch.object(mgr,
                           '_persist_replicas',
                           side_effect=persisted.extend):
        confirmed = mgr.confirm_logical_bridge_capacities({1: 4})

    assert confirmed == {1: 8}
    assert info.planned_capacity == 8
    assert not persisted


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
                'get_replica_ids',
                return_value=set()), \
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

        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids',
                return_value={6, 7}), \
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

    def _run_launch_cluster(self,
                            tmp_path,
                            stream_side_effects,
                            *,
                            backoff_seconds=0,
                            replica_to_launch_cancelled=None,
                            **kwargs):
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
            mock_backoff.return_value.current_backoff.return_value = (
                backoff_seconds)
            mock_sdk.launch.return_value = 'request-id'
            mock_sdk.stream_and_get.side_effect = stream_side_effects
            if replica_to_launch_cancelled is None:
                replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
            try:
                replica_managers.launch_cluster(
                    replica_id=1,
                    yaml_content='dummy: yaml',
                    cluster_name='svc-1',
                    log_file=str(tmp_path / 'launch.log'),
                    replica_to_request_id=thread_utils.ThreadSafeDict(),
                    replica_to_launch_cancelled=replica_to_launch_cancelled,
                    **kwargs)
            except RuntimeError as e:
                raised = e
        return mock_sdk, mock_terminate, raised

    def test_retry_backoff_uses_monotonic_bounded_sleeps(self, tmp_path):
        now = 0.0
        sleeps = []

        def _monotonic():
            return now

        def _sleep(seconds):
            nonlocal now
            sleeps.append(seconds)
            now += seconds

        fake_time = mock.Mock(wraps=replica_managers.time)
        fake_time.time.side_effect = AssertionError('wall clock used')
        fake_time.monotonic.side_effect = _monotonic
        fake_time.sleep.side_effect = _sleep
        with mock.patch('sky.serve.replica_managers.time', fake_time):
            mock_sdk, _, raised = self._run_launch_cluster(
                tmp_path, [RuntimeError('transient'), None],
                backoff_seconds=0.15)

        assert raised is None
        assert mock_sdk.launch.call_count == 2
        assert sleeps == pytest.approx([0.1, 0.05])
        assert now == pytest.approx(0.15)

    def test_retry_backoff_stops_on_cancellation(self, tmp_path):
        now = 0.0
        sleeps = []
        cancelled = thread_utils.ThreadSafeDict()

        def _monotonic():
            return now

        def _sleep(seconds):
            nonlocal now
            sleeps.append(seconds)
            now += seconds
            cancelled[1] = True

        fake_time = mock.Mock(wraps=replica_managers.time)
        fake_time.monotonic.side_effect = _monotonic
        fake_time.sleep.side_effect = _sleep
        with mock.patch('sky.serve.replica_managers.time', fake_time):
            mock_sdk, mock_terminate, raised = self._run_launch_cluster(
                tmp_path, [RuntimeError('transient')],
                backoff_seconds=1,
                replica_to_launch_cancelled=cancelled)

        assert raised is None
        assert mock_sdk.launch.call_count == 1
        assert mock_terminate.call_count == 1
        assert sleeps == [0.1]
        assert 1 not in cancelled

    def test_capacity_failure_fails_fast_with_availability_max_retry(
            self, tmp_path):
        """One capacity failure with availability_max_retry=1 must raise
        immediately (no in-place retry of the same exhausted location)."""
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [_capacity_error()] * 3, availability_max_retry=1)
        assert raised is not None
        assert mock_sdk.launch.call_count == 1
        assert mock_terminate.call_count == 1

    def test_legacy_service_policy_does_not_block_recovered_launch(
            self, tmp_path):
        legacy_yaml = """
resources:
  cpus: 1
  ports: 8080
  accelerators: A100:1
  use_spot: true
service:
  readiness_probe: /health
  replica_policy:
    min_replicas: 1
    max_replicas: 8
    target_concurrency_per_replica: 2
    spot_placer: dynamic_fallback_per_gpu
run: echo hi
"""
        persisted_spec = mock.MagicMock()
        with mock.patch('sky.serve.replica_managers.usage_lib'), \
             mock.patch('sky.serve.replica_managers.sdk') as mock_sdk:
            mock_sdk.launch.return_value = 'request-id'
            mock_sdk.stream_and_get.return_value = None
            replica_managers.launch_cluster(
                replica_id=1,
                yaml_content=legacy_yaml,
                cluster_name='svc-1',
                log_file=str(tmp_path / 'launch.log'),
                replica_to_request_id=thread_utils.ThreadSafeDict(),
                replica_to_launch_cancelled=thread_utils.ThreadSafeDict(),
                service_spec=persisted_spec)

        mock_sdk.launch.assert_called_once()
        launched_task = mock_sdk.launch.call_args.args[0]
        assert launched_task.service is persisted_spec

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

    def test_exact_override_collapses_any_of_to_one_resource(self, tmp_path):
        task = mock.MagicMock()
        resources = [mock.Mock(name='first'), mock.Mock(name='second')]
        task.resources = resources
        pinned = mock.Mock(name='pinned')
        resources[0].copy.return_value = pinned
        persisted_spec = mock.sentinel.persisted_spec

        with mock.patch(
                'sky.serve.replica_managers.load_task_with_service_spec',
                return_value=task) as load_task, \
             mock.patch('sky.serve.replica_managers.usage_lib'), \
             mock.patch('sky.serve.replica_managers.sdk') as mock_sdk:
            mock_sdk.launch.return_value = 'request-id'
            replica_managers.launch_cluster(
                replica_id=1,
                yaml_content='dummy: yaml',
                cluster_name='svc-1',
                log_file=str(tmp_path / 'launch.log'),
                replica_to_request_id=thread_utils.ThreadSafeDict(),
                replica_to_launch_cancelled=thread_utils.ThreadSafeDict(),
                resources_override={'region': 'us-east-1'},
                exact_resources_override=True,
                service_spec=persisted_spec)

        load_task.assert_called_once_with('dummy: yaml', persisted_spec)
        resources[0].copy.assert_called_once_with(region='us-east-1')
        resources[1].copy.assert_not_called()
        task.set_resources.assert_called_once_with(pinned)
        mock_sdk.launch.assert_called_once()

    def test_authoritative_prelaunch_guard_rejects_cloud_mutation(
            self, tmp_path):
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [None], pre_launch_guard=lambda: False)
        assert raised is not None
        assert 'ownership was lost' in str(raised)
        mock_sdk.launch.assert_not_called()
        mock_terminate.assert_not_called()

    def test_unfenced_external_lb_fails_once_before_api_request(self, tmp_path):
        with mock.patch.object(replica_managers.serve_utils,
                               'is_external_load_balancer_mode',
                               return_value=True):
            mock_sdk, mock_terminate, raised = self._run_launch_cluster(
                tmp_path, [None], launch_fence=None)

        assert isinstance(raised,
                          replica_managers._UnfencedExternalLbLaunchError)
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
            placer.active_locations.return_value = []
            placer.zero_cost_locations.return_value = []
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
        assert call.kwargs['kwargs']['exact_resources_override'] is True
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


class TestUpdateVersionBatchesPriorVersionYamls:
    """`update_version` should reuse old YAMLs per distinct version."""

    def test_refreshes_spot_placer_from_new_task(self):
        mgr = _make_manager()
        old_placer = mock.Mock(name='old_placer')
        new_placer = mock.Mock(name='new_placer')
        mgr._spot_placer = old_placer
        spec = mock.Mock(spot_placer='dynamic_fallback_per_gpu')
        new_task = mock.Mock(name='new_task', resources=[])
        new_yaml = ('resources: {accelerators: L4:1}\n'
                    'file_mounts: {}\n'
                    'service: {readiness_probe: /}\n')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=new_yaml), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[]), \
             mock.patch.object(
                 replica_managers,
                 'load_task_with_service_spec',
                 return_value=new_task) as parse_task, \
             mock.patch.object(
                 replica_managers.spot_placer.SpotPlacer,
                 'from_task',
                 return_value=new_placer) as build_placer:
            mgr.update_version(2,
                               spec,
                               update_mode=serve_utils.UpdateMode.ROLLING)

        parse_task.assert_called_once_with(new_yaml, spec)
        build_placer.assert_called_once_with(spec, new_task)
        assert mgr._spot_placer is new_placer

    def test_reuses_distinct_old_version_yamls(self):
        mgr = _make_manager()
        mgr.latest_version = 2
        mgr.yaml_content = 'old: yaml'
        mgr._update_mode = None
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))

        info1 = mock.Mock(replica_id=1, version=1, is_terminal=False)
        info2 = mock.Mock(replica_id=2, version=1, is_terminal=False)
        info3 = mock.Mock(replica_id=3, version=2, is_terminal=False)
        terminal = mock.Mock(replica_id=4, version=1, is_terminal=True)
        replica_infos = [info1, info2, info3, terminal]

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=('resources: {}\n'
                              'file_mounts: {}\n'
                              'service: {readiness_probe: /}\n')
        ) as get_new_yaml, \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={
                     1: ('resources: {}\n'
                         'file_mounts: {}\n'
                         'service: {readiness_probe: /}\n'),
                     2: ('resources: {cpus: 2}\n'
                         'file_mounts: {}\n'
                         'service: {readiness_probe: /}\n'),
                 }) as get_old_yamls, \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=replica_infos):
            mgr.update_version(3,
                               mock.Mock(spot_placer=None),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        get_new_yaml.assert_called_once_with('svc', 3)
        get_old_yamls.assert_called_once_with('svc', [1, 2])
        assert persisted == [(1, 3), (2, 3)]
        assert info1.version == 3
        assert info2.version == 3
        assert info3.version == 2
        assert terminal.version == 1

    def test_missing_old_version_yaml_fails_before_persisting(self):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        mgr._update_mode = None
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))
        replica_infos = [
            mock.Mock(replica_id=1, version=1, is_terminal=False),
            mock.Mock(replica_id=2, version=2, is_terminal=False),
        ]

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=('resources: {}\n'
                              'file_mounts: {}\n'
                              'service: {readiness_probe: /}\n')), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={
                     1: ('resources: {}\n'
                         'file_mounts: {}\n'
                         'service: {readiness_probe: /}\n'),
                 }), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=replica_infos):
            with pytest.raises(
                    ValueError,
                    match='yaml content not found for svc version 2'):
                mgr.update_version(3,
                                   mock.Mock(spot_placer=None),
                                   update_mode=serve_utils.UpdateMode.ROLLING)

        assert not persisted
        assert [info.version for info in replica_infos] == [1, 2]

    def test_no_prior_nonterminal_versions_skips_yaml_lookup(self):
        for replica_infos in (
            [],
            [mock.Mock(replica_id=1, version=1, is_terminal=True)],
        ):
            mgr = _make_manager()
            mgr.latest_version = 1
            mgr.yaml_content = 'old: yaml'
            mgr._update_mode = None

            with mock.patch.object(
                    replica_managers.serve_state,
                    'get_yaml_content',
                    return_value=('resources: {}\n'
                                  'file_mounts: {}\n'
                                  'service: {readiness_probe: /}\n')), \
                 mock.patch.object(
                     replica_managers.serve_state,
                     'get_yaml_contents') as get_old_yamls, \
                 mock.patch.object(
                     replica_managers.serve_state,
                     'get_replica_infos',
                     return_value=replica_infos):
                mgr.update_version(2,
                                   mock.Mock(spot_placer=None),
                                   update_mode=serve_utils.UpdateMode.ROLLING)

            get_old_yamls.assert_not_called()

    @pytest.mark.parametrize('old_scope_id,new_scope_id', [
        ('old-scope', 'new-scope'),
        ('old-scope', None),
    ])
    def test_reuses_replica_when_only_empty_storage_scope_changes(
            self, old_scope_id, new_scope_id):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        mgr._update_mode = None
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))
        info = mock.Mock(replica_id=1, version=1, is_terminal=False)

        def _yaml(scope_id):
            metadata = ''
            if scope_id is not None:
                metadata = ('_metadata:\n'
                            '  sky_serve_ephemeral_storage_scope:\n'
                            '    resource_scope: incarnation\n'
                            f'    scope_id: {scope_id}\n'
                            f'    storage_generation: {scope_id}-generation\n'
                            '    storage_mounts: []\n')
            return ('resources: {}\n'
                    'file_mounts: {}\n'
                    'volumes: {}\n'
                    'service: {readiness_probe: /}\n' + metadata)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=_yaml(new_scope_id)), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={1: _yaml(old_scope_id)}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]):
            mgr.update_version(2,
                               mock.Mock(spot_placer=None),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert persisted == [(1, 2)]
        assert info.version == 2

    def test_reuses_replica_when_only_git_commit_changes(self, caplog):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))
        info = mock.Mock(replica_id=1, version=1, is_terminal=False)

        def _yaml(git_commit):
            return ('resources: {}\n'
                    'file_mounts: {}\n'
                    'secrets: {TOKEN: stable-secret}\n'
                    'service: {readiness_probe: /}\n'
                    f'_metadata: {{git_commit: {git_commit}}}\n')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=_yaml('new-commit')), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={1: _yaml('old-commit')}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]), \
             caplog.at_level(logging.INFO):
            mgr.update_version(2,
                               mock.Mock(spot_placer=None),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert persisted == [(1, 2)]
        assert info.version == 2
        assert 'stable-secret' not in caplog.text

    def test_secret_change_forces_replacement_without_logging_values(
            self, caplog):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))
        info = mock.Mock(replica_id=1, version=1, is_terminal=False)

        def _yaml(secret):
            return ('resources: {}\n'
                    'file_mounts: {}\n'
                    f'secrets: {{TOKEN: {secret}}}\n'
                    'service: {readiness_probe: /}\n')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=_yaml('new-secret')), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={1: _yaml('old-secret')}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]), \
             caplog.at_level(logging.INFO):
            mgr.update_version(2,
                               mock.Mock(spot_placer=None),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert not persisted
        assert info.version == 1
        assert 'runtime config changed' in caplog.text
        assert 'old-secret' not in caplog.text
        assert 'new-secret' not in caplog.text

    def test_storage_scope_with_owned_mount_still_forces_replacement(self):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        mgr._update_mode = None
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))
        info = mock.Mock(replica_id=1, version=1, is_terminal=False)

        def _yaml(scope_id):
            return ('resources: {}\n'
                    'file_mounts: {}\n'
                    'volumes: {}\n'
                    'service: {readiness_probe: /}\n'
                    '_metadata:\n'
                    '  sky_serve_ephemeral_storage_scope:\n'
                    '    resource_scope: incarnation\n'
                    f'    scope_id: {scope_id}\n'
                    f'    storage_generation: {scope_id}-generation\n'
                    '    storage_mounts: [/data]\n')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=_yaml('new-scope')), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={1: _yaml('old-scope')}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]):
            mgr.update_version(2,
                               mock.Mock(spot_placer=None),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert not persisted
        assert info.version == 1

    def test_logical_update_rebuilds_shape_placer_from_new_version(self):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        old_placer = mock.Mock(name='old_placer')
        mgr._spot_placer = old_placer
        new_location = types.SimpleNamespace(accelerators={'L4': 4})
        new_placer = mock.Mock(name='new_placer')
        new_placer.active_locations.return_value = [new_location]
        new_task = types.SimpleNamespace(resources=[new_location], num_nodes=1)
        spec = types.SimpleNamespace(uses_logical_replicas=True)
        yaml_content = ('resources: {}\n'
                        'file_mounts: {}\n'
                        'service: {readiness_probe: /}\n')

        with mock.patch.object(replica_managers.serve_state,
                               'get_yaml_content',
                               return_value=yaml_content), \
             mock.patch.object(replica_managers,
                               'load_task_with_service_spec',
                               return_value=new_task), \
             mock.patch.object(replica_managers.spot_placer.SpotPlacer,
                               'from_task',
                               return_value=new_placer) as build_placer, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]):
            mgr.update_version(2,
                               spec,
                               update_mode=serve_utils.UpdateMode.ROLLING)

        build_placer.assert_called_once_with(spec, new_task)
        assert mgr._spot_placer is new_placer
        assert mgr._default_planned_capacity == 4
        assert mgr.latest_version == 2

    def test_logical_update_rejects_multi_node_service_before_mutation(self):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        old_placer = mock.Mock(name='old_placer')
        mgr._spot_placer = old_placer
        location = types.SimpleNamespace(accelerators={'A100': 8})
        new_task = types.SimpleNamespace(resources=[location], num_nodes=2)
        new_placer = mock.Mock(name='new_placer')
        new_placer.active_locations.return_value = [location]
        spec = types.SimpleNamespace(uses_logical_replicas=True)

        with mock.patch.object(replica_managers.serve_state,
                               'get_yaml_content',
                               return_value='new: yaml'), \
             mock.patch.object(replica_managers,
                               'load_task_with_service_spec',
                               return_value=new_task), \
             mock.patch.object(replica_managers.spot_placer.SpotPlacer,
                               'from_task',
                               return_value=new_placer), \
             pytest.raises(ValueError, match='only single-node services'):
            mgr.update_version(2,
                               spec,
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert mgr.latest_version == 1
        assert mgr.yaml_content == 'old: yaml'
        assert mgr._spot_placer is old_placer

    def test_logical_manager_rejects_physical_update_before_mutation(self):
        mgr = _make_manager()
        mgr.latest_version = 2
        mgr.yaml_content = 'logical: yaml'
        mgr._uses_logical_replicas = True
        physical = types.SimpleNamespace(uses_logical_replicas=False)

        with mock.patch.object(replica_managers.serve_state,
                               'get_yaml_content',
                               return_value='physical: yaml'), \
             pytest.raises(ValueError, match='back to physical'):
            mgr.update_version(3,
                               physical,
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert mgr.latest_version == 2
        assert mgr.yaml_content == 'logical: yaml'


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
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: info for rid in ids}), \
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
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
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
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: info for rid in ids}), \
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
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
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

    def test_unfenced_external_lb_failure_stops_replica_churn(self):
        mgr, infos = self._queued_manager([1])
        info = infos[1]
        info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        launch_thread = mgr._launch_thread_pool[1]
        launch_thread.format_exc = 'missing durable owner fence'
        launch_thread.exception = (
            replica_managers._UnfencedExternalLbLaunchError('unfenced'))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value=infos), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]), \
             mock.patch.object(mgr, '_persist_replica') as persist, \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        assert info.status_property.user_app_failed is True
        assert (info.status_property.sky_launch_status ==
                common_utils.ProcessStatus.FAILED)
        persist.assert_called_once_with(1, info)
        terminate.assert_called_once_with(1,
                                          sync_down_logs=True,
                                          replica_drain_delay_seconds=0)

        terminal = replica_managers.ReplicaStatusProperty(
            sky_launch_status=common_utils.ProcessStatus.FAILED,
            sky_down_status=common_utils.ProcessStatus.SUCCEEDED,
            user_app_failed=True)
        assert (terminal.to_replica_status() ==
                replica_managers.serve_state.ReplicaStatus.FAILED)
        assert terminal.unrecoverable_failure() is True

    def test_unfenced_external_lb_failure_does_not_bench_spot_location(self):
        mgr, infos = self._queued_manager([1])
        info = infos[1]
        info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        info.status_property.failed_spot_availability = False
        location = mock.Mock()
        info.get_spot_location.return_value = location
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        mgr._spot_placer = placer
        launch_thread = mgr._launch_thread_pool[1]
        launch_thread.format_exc = 'missing durable owner fence'
        launch_thread.exception = (
            replica_managers._UnfencedExternalLbLaunchError('unfenced'))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value=infos), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]), \
             mock.patch.object(mgr, '_persist_replica'), \
             mock.patch.object(mgr, '_terminate_replica'), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        assert info.status_property.user_app_failed is True
        assert info.status_property.failed_spot_availability is False
        placer.set_active.assert_not_called()
        placer.set_preemptive.assert_not_called()

    def test_unrecoverable_failure_check_does_not_log_per_replica(self):
        status = replica_managers.ReplicaStatusProperty(
            sky_launch_status=common_utils.ProcessStatus.FAILED,
            sky_down_status=common_utils.ProcessStatus.SUCCEEDED,
            user_app_failed=True)

        with mock.patch.object(replica_managers, 'logger') as logger:
            results = [status.unrecoverable_failure() for _ in range(2_159)]

        assert all(results)
        assert logger.mock_calls == []

    def test_safe_thread_exposes_captured_exception(self):
        error = RuntimeError('typed failure')

        def fail():
            raise error

        launch_thread = thread_utils.SafeThread(target=fail)
        launch_thread.run()

        assert launch_thread.exception is error
        assert launch_thread.format_exc is not None

    def test_authorized_lookup_is_shared_across_queued_launches(self, tmp_path):
        mgr, infos = self._queued_manager([1, 2, 3])
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        placer.is_active_location.return_value = True
        mgr._spot_placer = placer
        for info in infos.values():
            info.get_spot_location.return_value = location

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=True) as authorize, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
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
        placer.is_active_location.assert_has_calls([mock.call(location)] * 3)
        assert persist.call_count == 3

    def test_benched_placement_discards_queued_wave(self):
        mgr, infos = self._queued_manager([1, 2, 3])
        launch_threads = list(mgr._launch_thread_pool.values())
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        placer.is_active_location.return_value = False
        mgr._spot_placer = placer
        for info in infos.values():
            info.get_spot_location.return_value = location

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=True), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_remove_replica') as remove, \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        placer.is_active_location.assert_has_calls([mock.call(location)] * 3)
        assert remove.call_args_list == [
            mock.call(1), mock.call(2),
            mock.call(3)
        ]
        for launch_thread in launch_threads:
            launch_thread.start.assert_not_called()
        assert len(mgr._launch_thread_pool) == 0
        assert len(mgr._replica_to_request_id) == 0
        assert len(mgr._replica_to_launch_cancelled) == 0
        persist.assert_not_called()

    def test_failure_overrides_sibling_success_before_queue_admission(self):
        mgr, infos = self._queued_manager([1, 2, 3])
        launch_threads = list(mgr._launch_thread_pool.values())
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        mgr._spot_placer = placer
        for info in infos.values():
            info.get_spot_location.return_value = location
            info.created_at = 123.0
        infos[
            1].status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        infos[
            2].status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        launch_threads[0].format_exc = 'no capacity'
        launch_threads[1].format_exc = None

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=True), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_remove_replica') as remove, \
             mock.patch.object(mgr, '_persist_replica'), \
             mock.patch.object(mgr, '_terminate_replica'):
            mgr._refresh_thread_pool()

        placer.set_active.assert_not_called()
        placer.set_preemptive.assert_called_once_with(location)
        remove.assert_called_once_with(3)
        launch_threads[2].start.assert_not_called()
        assert len(mgr._launch_thread_pool) == 0

    def test_success_reactivation_uses_launch_selection_time(self):
        mgr, infos = self._queued_manager([1])
        launch_thread = mgr._launch_thread_pool[1]
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        mgr._spot_placer = placer
        info = infos[1]
        info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        info.created_at = 123.0
        info.get_spot_location.return_value = location
        launch_thread.format_exc = None

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value=infos), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_persist_replica'), \
             mock.patch.object(mgr, '_terminate_replica'):
            mgr._refresh_thread_pool()

        placer.set_active.assert_called_once_with(location, selected_at=123.0)
        placer.set_preemptive.assert_not_called()

    @pytest.mark.parametrize('failure_stage', ('persist', 'cleanup'))
    def test_launch_failure_benches_before_fallible_cleanup(
            self, failure_stage):
        mgr, infos = self._queued_manager([1, 2])
        failed_thread = mgr._launch_thread_pool[1]
        failed_thread.format_exc = 'no capacity'
        infos[
            1].status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        mgr._spot_placer = placer
        for info in infos.values():
            info.get_spot_location.return_value = location

        events = []
        placer.set_preemptive.side_effect = lambda _location: events.append(
            'bench')

        def _persist(*_args, **_kwargs):
            events.append('persist')
            if failure_stage == 'persist':
                raise RuntimeError('persist unavailable')

        def _fail_cleanup(*_args, **_kwargs):
            events.append('cleanup')
            if failure_stage == 'cleanup':
                raise RuntimeError('cleanup unavailable')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                side_effect=lambda _svc, ids: {
                    rid: infos[rid]
                    for rid in ids
                }), \
             mock.patch.object(mgr,
                               '_persist_replica',
                               side_effect=_persist), \
             mock.patch.object(mgr,
                               '_terminate_replica',
                               side_effect=_fail_cleanup), \
             pytest.raises(RuntimeError,
                           match=f'{failure_stage} unavailable'):
            mgr._refresh_thread_pool()

        expected = ['bench', 'persist']
        if failure_stage == 'cleanup':
            expected.append('cleanup')
        assert events == expected
        placer.set_preemptive.assert_called_once_with(location)
        assert 2 in mgr._launch_thread_pool
        mgr._launch_thread_pool[2].start.assert_not_called()

    def test_old_version_metadata_is_retained(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        info = mock.Mock(version=2)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup') as reconcile:
            mgr._refresh_thread_pool()

        reconcile.assert_called_once_with([info])


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


class TestInfrastructureInterruptionRecovery:
    """Research Kubernetes pods use the recoverable spot lifecycle.

    Every service pod on a configured zero-cost research location is
    low-priority and reclaimable, including ordinary demand pods. Reclamation
    must replace the backend without benching the still-healthy research pool.
    """

    @staticmethod
    def _location(*, cloud='Kubernetes', region='research-ctx', use_spot=False):
        return replica_managers.spot_placer.Location.from_pickleable({
            'cloud': cloud,
            'region': region,
            'zone': None,
            'accelerators': {
                'A100' if cloud == 'Kubernetes' else 'L4': 1
            },
            'use_spot': use_spot,
        })

    @staticmethod
    def _info(location, *, is_spot=False, ready=False):
        info = replica_managers.ReplicaInfo(replica_id=1,
                                            cluster_name='svc-1',
                                            replica_port='8080',
                                            is_spot=is_spot,
                                            location=location,
                                            version=1,
                                            resources_override=None)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.reserved_fill = False
        if ready:
            info.status_property.first_ready_time = 1.0
            info.status_property.service_ready_now = True
        return info

    @staticmethod
    def _handle():
        return mock.Mock(
            spec=replica_managers.backends.CloudVmRayResourceHandle)

    def _manager(self, zero_cost):
        manager = _make_manager()
        manager._spot_placer = mock.Mock()
        manager._spot_placer.zero_cost_locations.return_value = [zero_cost]
        return manager

    def test_non_fill_research_replica_is_interruptible(self):
        research = self._location()
        manager = self._manager(research)
        info = self._info(research)

        assert info.reserved_fill is False
        assert manager._is_interruptible_replica(info) is True

    def test_unrelated_nonspot_replica_is_not_interruptible(self):
        research = self._location()
        unrelated = self._location(cloud='AWS', region='us-east-1')
        manager = self._manager(research)

        assert manager._is_interruptible_replica(self._info(unrelated)) is False

    def test_reclaimed_research_replica_reuses_recoverable_lifecycle(self):
        research = self._location()
        manager = self._manager(research)
        info = self._info(research)

        with mock.patch.object(
                replica_managers.global_user_state,
                'get_handle_from_cluster_name',
                return_value=self._handle()), \
             mock.patch.object(
                 replica_managers.backend_utils,
                 'refresh_cluster_status_handle',
                 return_value=(None, None)), \
             mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_terminate_replica') as terminate:
            assert manager._handle_preemption(info) is True

        assert info.status_property.preempted is True
        persist.assert_called_once_with(1, info)
        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True)
        manager._spot_placer.set_preemptive.assert_not_called()

        # A reclaimed backend before first readiness must not brick the
        # version as an unrecoverable application failure.
        info.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        assert info.status_property.unrecoverable_failure() is False

    def test_running_research_replica_is_not_reclaimed(self):
        research = self._location()
        manager = self._manager(research)
        info = self._info(research)
        from sky.utils import status_lib

        with mock.patch.object(
                replica_managers.global_user_state,
                'get_handle_from_cluster_name',
                return_value=self._handle()), \
             mock.patch.object(
                 replica_managers.backend_utils,
                 'refresh_cluster_status_handle',
                 return_value=(status_lib.ClusterStatus.UP, None)), \
             mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_terminate_replica') as terminate:
            assert manager._handle_preemption(info) is False

        persist.assert_not_called()
        terminate.assert_not_called()

    @pytest.mark.parametrize('is_spot', [False, True])
    def test_missing_handle_recovers_interrupted_replica(self, is_spot):
        research = self._location()
        manager = self._manager(research)
        location = (self._location(cloud='AWS',
                                   region='us-east-1',
                                   use_spot=True) if is_spot else research)
        info = self._info(location, is_spot=is_spot, ready=True)

        with mock.patch.object(
                replica_managers.global_user_state,
                'get_handle_from_cluster_name',
                return_value=None), \
             mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_terminate_replica') as terminate:
            assert manager._handle_preemption(info) is True

        assert info.status_property.preempted is True
        persist.assert_called_once_with(1, info)
        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True)
        if is_spot:
            manager._spot_placer.set_preemptive.assert_called_once_with(
                location)
        else:
            manager._spot_placer.set_preemptive.assert_not_called()

    def test_spot_interruption_still_benches_location(self):
        research = self._location()
        spot = self._location(cloud='AWS', region='us-east-1', use_spot=True)
        manager = self._manager(research)
        info = self._info(spot, is_spot=True)

        with mock.patch.object(
                replica_managers.global_user_state,
                'get_handle_from_cluster_name',
                return_value=self._handle()), \
             mock.patch.object(
                 replica_managers.backend_utils,
                 'refresh_cluster_status_handle',
                 return_value=(None, None)), \
             mock.patch.object(manager, '_persist_replica'), \
             mock.patch.object(manager, '_terminate_replica'):
            assert manager._handle_preemption(info) is True

        manager._spot_placer.set_preemptive.assert_called_once_with(spot)

    def test_failed_research_probe_enters_interruption_prefilter(self):
        research = self._location()
        manager = self._manager(research)
        info = self._info(research)
        info.probe = mock.Mock(return_value=(info, False, 100.0))
        manager._is_pool = False
        manager._uptime = 1.0
        manager._tick_version_spec_cache = {}
        manager._resolve_probe_urls = mock.Mock(
            return_value={1: 'http://10.0.0.1:8080'})
        manager._get_readiness_path = mock.Mock(return_value='/health')
        manager._get_post_data = mock.Mock(return_value=None)
        manager._get_readiness_timeout_seconds = mock.Mock(return_value=15)
        manager._get_readiness_headers = mock.Mock(return_value=None)
        manager._cloud_instance_looks_alive = mock.Mock(return_value=False)
        manager._handle_preemption = mock.Mock(return_value=True)
        manager._persist_replicas = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_specs',
                               return_value={1: mock.Mock()}):
            manager._probe_all_replicas()

        manager._cloud_instance_looks_alive.assert_called_once_with(info)
        manager._handle_preemption.assert_called_once_with(info)
        manager._persist_replicas.assert_called_once_with([])

    @pytest.mark.parametrize('persisted_intent', [False, True])
    def test_recovery_redrives_reclaimed_research_replica_without_bench(
            self, persisted_intent):
        research = self._location()
        manager = self._manager(research)
        info = self._info(research, ready=True)
        info.status_property.preempted = persisted_intent

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={}), \
             mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_terminate_replica') as terminate:
            manager._recover_replica_operations()

        assert info.status_property.preempted is True
        if persisted_intent:
            persist.assert_not_called()
        else:
            persist.assert_called_once_with(1, info)
        terminate.assert_called_once()
        manager._spot_placer.set_preemptive.assert_not_called()


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
                'get_replica_ids',
                return_value=set()) as id_scan, \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up_batch([None, {'use_spot': True}, None])
        assert launched == [1, 2, 3]
        assert mgr._next_replica_id == 4
        assert lock.acquisitions == 1
        # The collision guard reads the id set ONCE per batch, not once per
        # replica launched.
        assert id_scan.call_count == 1

    def test_single_scale_up_unchanged(self):
        mgr = _make_manager(next_replica_id=7)
        lock = self._CountingLock()
        mgr.lock = lock
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids',
                return_value=set()), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up()
        assert launched == [7]
        assert lock.acquisitions == 1

    def test_batch_skips_ids_with_existing_rows(self):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        launched = []

        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids',
                return_value={2}), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up_batch([None, None])
        assert launched == [1, 3]

    def test_spot_batch_reuses_one_replica_snapshot(self):
        """K placer launches must scan/unpickle the N-row table once.

        The shared list must also accumulate each newly enqueued replica so
        reserved-capacity accounting sees in-wave reservations. Cost-first
        placement must not scan the N existing rows for location load. The
        current service must come from the same global snapshot used for
        cross-service capacity; combining a separate local read with a later
        global read can mix two database states in one placement decision.
        """
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr._spot_placer = mock.Mock()
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=True)
        mgr.yaml_content = 'dummy: yaml'
        initial = [_fake_replica_info(40), _fake_replica_info(41)]
        stale_local = [_fake_replica_info(99)]
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
                'get_replica_ids') as id_scan, \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos',
                 return_value=stale_local) as local_scan, \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.'
                 'get_replica_infos_grouped',
                 return_value={'svc': list(initial)}) as grouped_scan, \
             mock.patch.object(mgr,
                               '_build_zero_cost_demand_budget',
                               return_value=None), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr.scale_up_batch([{'use_spot': True}] * 3)

        local_scan.assert_not_called()
        grouped_scan.assert_called_once_with()
        # The id set is derived from the placement snapshot; no second query.
        id_scan.assert_not_called()
        assert [size for _, size in snapshots] == [2, 3, 4]
        assert all(snapshot is snapshots[0][0] for snapshot, _ in snapshots)
        for info in initial:
            info.get_spot_location.assert_not_called()

    def test_spot_batch_defers_when_shared_reservation_lock_is_busy(self):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr._spot_placer = mock.Mock()
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=True)
        mgr.yaml_content = 'resources:\n  use_spot: true\n'
        reservation_lock = mock.Mock()
        reservation_lock.acquire.side_effect = replica_managers.locks.LockTimeout(
            'busy')

        with mock.patch.object(replica_managers.locks,
                               'get_lock',
                               return_value=reservation_lock) as get_lock, \
             mock.patch.object(mgr, '_scale_up_batch_locked') as scale_locked:
            mgr.scale_up_batch([{'use_spot': True}] * 3)

        get_lock.assert_called_once_with(replica_managers.serve_constants.
                                         DEMAND_CAPACITY_RESERVATION_LOCK_ID)
        reservation_lock.acquire.assert_called_once_with(blocking=False)
        scale_locked.assert_not_called()

    def test_paid_only_spot_batch_does_not_take_shared_capacity_lock(self):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr._spot_placer = mock.Mock()
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        mgr.yaml_content = 'resources:\n  use_spot: true\n'

        with mock.patch.object(replica_managers.locks,
                               'get_lock') as get_lock, \
             mock.patch.object(mgr, '_scale_up_batch_locked') as scale_locked:
            mgr.scale_up_batch([{'use_spot': True}] * 3)

        get_lock.assert_not_called()
        scale_locked.assert_called_once_with([{'use_spot': True}] * 3, None)

    @pytest.mark.parametrize('active_paid', [False, True])
    def test_zero_cost_batch_uses_shared_capacity_lock_even_without_active_paid(
            self, active_paid):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        zero = replica_managers.spot_placer.Location.from_pickleable({
            'cloud': 'Kubernetes',
            'region': 'research-ctx',
            'zone': None,
            'accelerators': {
                'A100': 8
            },
            'use_spot': False,
        })
        paid = replica_managers.spot_placer.Location.from_pickleable({
            'cloud': 'AWS',
            'region': 'us-east-1',
            'zone': None,
            'accelerators': {
                'A100': 8
            },
            'use_spot': True,
        })
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [zero]
        placer.active_locations.return_value = ([zero, paid]
                                                if active_paid else [zero])
        mgr._spot_placer = placer
        mgr.yaml_content = 'resources:\n  use_spot: true\n'
        reservation_lock = mock.MagicMock()

        assert mgr._uses_shared_zero_cost_demand_budget()
        with mock.patch.object(replica_managers.locks,
                               'get_lock',
                               return_value=reservation_lock) as get_lock, \
             mock.patch.object(mgr, '_scale_up_batch_locked') as scale_locked:
            mgr.scale_up_batch([{'use_spot': True}] * 100)

        get_lock.assert_called_once_with(replica_managers.serve_constants.
                                         DEMAND_CAPACITY_RESERVATION_LOCK_ID)
        reservation_lock.acquire.assert_called_once_with(blocking=False)
        scale_locked.assert_called_once()

    def test_on_demand_batch_does_not_add_replica_scan(self):
        """Explicit on-demand pins do not ask the placer for a location."""
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr._spot_placer = mock.Mock()
        mgr.yaml_content = 'dummy: yaml'
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids', return_value=set()), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos'
             ) as scan, \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up_batch([{'use_spot': False}] * 3)
        assert launched == [1, 2, 3]
        scan.assert_not_called()

    def test_batch_yields_to_newer_pending_version(self):
        """A committed update must not wait behind the rest of a huge wave."""
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr.latest_version = 4
        mgr._pending_version = None
        launched = []

        def _launch(replica_id,
                    resources_override,
                    existing_replica_infos=None):
            del resources_override, existing_replica_infos
            launched.append(replica_id)
            if replica_id == 2:
                mgr.notify_version_pending(6)
            return True

        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids', return_value=set()), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr.scale_up_batch([None] * 500)

        assert launched == [1, 2]
        assert mgr._next_replica_id == 3
        assert mgr._pending_version == 6

    def test_stale_physical_batch_cannot_cross_logical_update(self):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr.latest_version = 5
        launched = []

        with mock.patch.object(mgr,
                               '_launch_replica',
                               side_effect=_record_launch(launched)), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_ids') \
                     as id_scan:
            mgr.scale_up_batch([None] * 100, expected_version=4)

        assert not launched
        id_scan.assert_not_called()

    def test_stale_physical_scale_down_cannot_cross_logical_update(self):
        mgr = _make_manager()
        mgr.latest_version = 5
        mgr._terminate_replica = mock.Mock()

        mgr.scale_down(1, expected_version=4)

        mgr._terminate_replica.assert_not_called()

    def test_pending_version_signal_clears_only_matching_update(self):
        mgr = _make_manager()
        mgr._pending_version = None
        mgr.notify_version_pending(6)
        mgr.notify_version_pending(7)
        mgr.clear_pending_version(6)
        assert mgr._pending_version == 7
        mgr.clear_pending_version(7)
        assert mgr._pending_version is None


class TestLogicalCapacityPlanning:
    """One manager operation packs whole backend shapes to a slot target."""

    @pytest.mark.parametrize('accelerators,expected', [
        ({
            'L4': 1
        }, 1),
        ({
            'A100': 4
        }, 4),
        ({
            'A100-80GB': 8
        }, 8),
        ({
            'L4': 0.5
        }, None),
        ({
            'L4': 1,
            'A100': 1
        }, None),
        (None, None),
    ])
    def test_v1_capacity_requires_one_whole_gpu_shape(self, accelerators,
                                                      expected):
        assert replica_managers._whole_gpu_capacity(accelerators) == expected

    @pytest.mark.parametrize('accelerators,expected', [
        ([{
            'A100': 8
        }, {
            'A100': 8
        }], 8),
        ([{
            'A100': 8
        }, None], None),
        ([{
            'A100': 8
        }, {
            'L4': 4
        }], None),
    ])
    def test_default_capacity_requires_every_resource_to_share_one_width(
            self, accelerators, expected):
        resources = [
            types.SimpleNamespace(accelerators=value) for value in accelerators
        ]
        assert replica_managers._uniform_whole_gpu_capacity(
            resources) == expected

    def test_v1_logical_capacity_rejects_multi_node_service(self):
        with pytest.raises(ValueError, match='only single-node services'):
            replica_managers._validate_logical_capacity_sources(
                default_capacity=8, placer=None, num_nodes=2)

    def test_plans_complete_shapes_until_target_is_covered(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 7, 9)
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=True)
        widths = iter([8, 4])
        planned = []

        def _append_shape(_override, _used_ids, existing, _budget,
                          logical_reconcile_fence):
            assert logical_reconcile_fence == (1, 7, 9)
            width = next(widths)
            info = mock.Mock(replica_id=len(existing) + 1,
                             is_terminal=False,
                             is_ready=False,
                             version=1,
                             planned_capacity=width)
            existing.append(info)
            planned.append(width)
            return True

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_grouped',
                               return_value={}), \
             mock.patch.object(
                 mgr, '_build_zero_cost_demand_budget',
                 return_value=None) as build_zero_cost_budget, \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_append_shape):
            mgr.scale_up_to_logical_capacity(target_capacity=9,
                                             version=1,
                                             reconcile_generation=7)

        assert planned == [8, 4]
        assert build_zero_cost_budget.call_args.kwargs[
            'demand_count_override'] == 9

    def test_paid_only_logical_scale_up_skips_shared_capacity_lock(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 3, 8)
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)

        with mock.patch.object(replica_managers.locks,
                               'get_lock') as get_lock, \
             mock.patch.object(
                 mgr, '_scale_up_to_logical_capacity_locked') as scale_locked:
            mgr.scale_up_to_logical_capacity(target_capacity=8,
                                             version=1,
                                             reconcile_generation=3)

        get_lock.assert_not_called()
        scale_locked.assert_called_once_with(8, 1, 3,
                                             mgr._logical_reconcile_snapshot,
                                             ())

    def test_unknown_capacity_replacement_launch_is_durably_attributed(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        original = self._ready_backend(1, 8)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=9,
                observed_slots_by_replica_id={1: 0},
                in_flight_by_replica_id={1: 0},
                unknown_replica_ids=frozenset({1}),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 9, 8)
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=True)
        launches = []
        stale_replacement = self._ready_backend(2, 8)
        stale_replacement.unknown_capacity_replacement = True

        def _append_replacement(_override,
                                _used_ids,
                                existing,
                                _budget,
                                logical_reconcile_fence,
                                unknown_capacity_replacement=False):
            launches.append(unknown_capacity_replacement)
            existing.append(
                types.SimpleNamespace(replica_id=2,
                                      is_terminal=False,
                                      is_ready=False,
                                      version=1,
                                      planned_capacity=8))
            return True

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[original, stale_replacement
                                            ]) as local_scan, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_grouped',
                               return_value={'svc': [original]
                                            }) as grouped_scan, \
             mock.patch.object(mgr,
                               '_build_zero_cost_demand_budget',
                               return_value=None), \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_append_replacement):
            mgr.scale_up_to_logical_capacity(target_capacity=8,
                                             version=1,
                                             reconcile_generation=9,
                                             replace_unknown_replica_ids=(1,))

        assert launches == [True]
        local_scan.assert_not_called()
        grouped_scan.assert_called_once_with()

    def test_existing_zero_capacity_replacement_prevents_recursive_launch(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        original = self._ready_backend(1, 8)
        replacement = self._ready_backend(2, 8)
        replacement.unknown_capacity_replacement = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=9,
                observed_slots_by_replica_id={
                    1: 0,
                    2: 0
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 9, 8)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[original, replacement]), \
             mock.patch.object(mgr, '_scale_up_one_locked') as launch:
            mgr.scale_up_to_logical_capacity(target_capacity=8,
                                             version=1,
                                             reconcile_generation=9,
                                             replace_unknown_replica_ids=(1,))

        launch.assert_not_called()

    def test_known_capacity_clears_replacement_incident_marker(self):
        mgr = _make_manager()
        replacement = self._ready_backend(2, 8)
        replacement.unknown_capacity_replacement = True
        mgr._unknown_capacity_replacement_ids = {2}
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=10,
                observed_slots_by_replica_id={2: 8},
                in_flight_by_replica_id={2: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._persist_replica = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={2: replacement}):
            mgr._clear_known_unknown_capacity_replacements()

        assert replacement.unknown_capacity_replacement is False
        assert not mgr._unknown_capacity_replacement_ids
        mgr._persist_replica.assert_called_once_with(2, replacement)

    def test_zero_capacity_keeps_replacement_incident_marker(self):
        mgr = _make_manager()
        replacement = self._ready_backend(2, 8)
        replacement.unknown_capacity_replacement = True
        mgr._unknown_capacity_replacement_ids = {2}
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=10,
                observed_slots_by_replica_id={2: 0},
                in_flight_by_replica_id={2: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._persist_replica = mock.Mock()

        mgr._clear_known_unknown_capacity_replacements()

        assert replacement.unknown_capacity_replacement is True
        assert mgr._unknown_capacity_replacement_ids == {2}
        mgr._persist_replica.assert_not_called()

    def test_stale_generation_persists_no_backend_prefix(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=8,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 8, 9)

        with mock.patch.object(mgr, '_scale_up_one_locked') as launch:
            mgr.scale_up_to_logical_capacity(target_capacity=9,
                                             version=1,
                                             reconcile_generation=7)

        launch.assert_not_called()

    @staticmethod
    def _ready_backend(replica_id, width):
        info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                            cluster_name=f'svc-{replica_id}',
                                            replica_port='8080',
                                            is_spot=True,
                                            location=None,
                                            version=1,
                                            resources_override=None,
                                            planned_capacity=width)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.service_ready_now = True
        info.status_property.first_ready_time = 1.0
        return info

    def _pending_logical_retirement(self, retiring_version=9):
        mgr = _make_manager()
        mgr.latest_version = 10
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._wait_for_idle_trackers = {}
        mgr._lb_in_flight_report = None
        retiring = self._ready_backend(9, 1)
        retiring.version = retiring_version
        survivor = self._ready_backend(10, 1)
        survivor.version = 10
        status = retiring.status_property
        status.is_scale_down = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.wait_for_idle_before_termination = True
        status.logical_retirement_version = 10
        status.logical_retirement_controller_epoch = 'test-controller-epoch'
        status.logical_retirement_generation = 4
        status.logical_retirement_target_capacity = 1
        status.logical_retirement_confirmed_generation = None
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=10,
                generation=5,
                observed_slots_by_replica_id={10: 1},
                in_flight_by_replica_id={
                    9: 0,
                    10: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (10, 5, 1)
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()
        return mgr, retiring, survivor

    def _recoverable_logical_retirement(self,
                                        replica_id,
                                        width=1,
                                        confirmed_generation=None):
        info = self._ready_backend(replica_id, width)
        status = info.status_property
        status.is_scale_down = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.drain_cap_seconds = 3900
        status.drain_started_at = replica_managers.time.time() - 100
        status.wait_for_idle_before_termination = True
        status.logical_retirement_version = 1
        status.logical_retirement_controller_epoch = 'old-controller-epoch'
        status.logical_retirement_generation = 4
        status.logical_retirement_target_capacity = 1
        status.logical_retirement_confirmed_generation = confirmed_generation
        status.logical_retirement_bounded_deadline = False
        status.logical_retirement_committed = False
        return info

    def _logical_recovery_manager(self, candidates, survivor, target=1):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._recovering_logical_retirement_ids = {
            info.replica_id for info in candidates
        }
        mgr._logical_retirement_recovery_deadline = (
            replica_managers.time.monotonic() + 120)
        mgr._wait_for_idle_trackers = {
            info.replica_id: (mock.Mock(return_value=False),
                              replica_managers.time.monotonic() + 300
                             ) for info in candidates
        }
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=5,
                observed_slots_by_replica_id={
                    survivor.replica_id: survivor.planned_capacity
                },
                in_flight_by_replica_id={
                    **{
                        info.replica_id: 0 for info in candidates
                    },
                    survivor.replica_id: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 5, target)
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()
        return mgr

    def test_recovery_gate_keeps_old_epoch_retirement_off_route_without_proof(
            self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        mgr._logical_reconcile_snapshot = None
        mgr._logical_target = None

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        assert (retiring.status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert retiring.status_property.logical_retirement_controller_epoch == (
            'old-controller-epoch')
        assert 1 in mgr._recovering_logical_retirement_ids
        mgr._persist_replica.assert_not_called()
        mgr._terminate_replica.assert_not_called()

    def test_recovery_pass_indexes_valid_uncommitted_retirement(self):
        retiring = self._recoverable_logical_retirement(1)
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._register_wait_for_idle = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_yaml_contents',
                               return_value={}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={}):
            mgr._recover_replica_operations()

        mgr._register_wait_for_idle.assert_called_once_with(retiring)
        assert mgr._recovering_logical_retirement_ids == {1}
        assert mgr._logical_retirement_recovery_deadline is not None

    @pytest.mark.parametrize('confirmed_generation', [None, 4])
    def test_recovery_adopts_old_epoch_retirement_and_preserves_deadline(
            self, confirmed_generation):
        retiring = self._recoverable_logical_retirement(
            1, confirmed_generation=confirmed_generation)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        drain_started_at = retiring.status_property.drain_started_at
        tracker_deadline = mgr._wait_for_idle_trackers[1][1]

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]) as fleet_read:
            mgr._reconcile_recovering_logical_retirements()

        fleet_read.assert_called_once_with('svc')
        status = retiring.status_property
        assert (retiring.status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert status.logical_retirement_version == 1
        assert status.logical_retirement_controller_epoch == (
            'test-controller-epoch')
        assert status.logical_retirement_generation == 5
        assert status.logical_retirement_target_capacity == 1
        assert status.logical_retirement_confirmed_generation is None
        assert status.logical_retirement_committed is False
        assert status.drain_started_at == drain_started_at
        assert mgr._wait_for_idle_trackers[1][1] == tracker_deadline
        assert not mgr._recovering_logical_retirement_ids
        mgr._persist_replica.assert_called_once_with(1, retiring)
        mgr._terminate_replica.assert_not_called()

    def test_recovery_reactivates_only_target_shortfall_then_adopts_remainder(
            self):
        candidates = [
            self._recoverable_logical_retirement(replica_id)
            for replica_id in (1, 2, 3)
        ]
        survivor = self._ready_backend(10, 1)
        mgr = self._logical_recovery_manager(candidates, survivor, target=3)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert candidates[0].is_ready
        assert candidates[1].is_ready
        assert (candidates[2].status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert mgr._recovering_logical_retirement_ids == {3}
        assert mgr._logical_retirement_reactivation_generation == 5

        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=6,
                observed_slots_by_replica_id={
                    1: 1,
                    2: 1,
                    10: 1,
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0,
                    10: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 6, 3)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert (candidates[2].status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert candidates[2].status_property.logical_retirement_generation == 6
        assert not mgr._recovering_logical_retirement_ids

    def test_recovery_reactivates_more_after_prior_candidate_stays_unobserved(
            self):
        candidates = [
            self._recoverable_logical_retirement(replica_id)
            for replica_id in (1, 2)
        ]
        survivor = self._ready_backend(10, 1)
        mgr = self._logical_recovery_manager(candidates, survivor, target=2)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()
        assert candidates[0].is_ready
        assert mgr._recovering_logical_retirement_ids == {2}

        # A newer generation still cannot observe the first reactivation.
        # Recompute the shortfall and release one more candidate rather than
        # leaving the service under-covered until the timeout.
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=6,
                observed_slots_by_replica_id={10: 1},
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    10: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 6, 2)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert candidates[1].is_ready
        assert not mgr._recovering_logical_retirement_ids

    def test_recovery_timeout_reactivates_uncommitted_retirement(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        mgr._logical_retirement_recovery_deadline = (
            replica_managers.time.monotonic() - 1)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert retiring.is_ready
        assert retiring.status_property.drain_started_at is None
        assert not mgr._recovering_logical_retirement_ids
        mgr._persist_replica.assert_called_once_with(1, retiring)

    def test_recovery_adoption_persist_failure_stays_off_route_for_retry(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        mgr._persist_replica.side_effect = RuntimeError('database unavailable')

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert (retiring.status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert retiring.status_property.logical_retirement_controller_epoch == (
            'old-controller-epoch')
        assert 1 in mgr._recovering_logical_retirement_ids
        mgr._terminate_replica.assert_not_called()

    def test_recovery_bulk_adoption_reads_fleet_once(self):
        candidates = [
            self._recoverable_logical_retirement(replica_id)
            for replica_id in range(1, 201)
        ]
        survivor = self._ready_backend(1000, 1)
        mgr = self._logical_recovery_manager(candidates, survivor)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]) as scan:
            mgr._reconcile_recovering_logical_retirements()

        scan.assert_called_once_with('svc')
        assert mgr._persist_replica.call_count == 200
        assert not mgr._recovering_logical_retirement_ids

    def test_adopted_expired_same_version_retirement_reactivates_safely(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()
        tracker, _ = mgr._wait_for_idle_trackers[1]
        mgr._wait_for_idle_trackers[1] = (tracker,
                                          replica_managers.time.monotonic() - 1)
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            in_flight_by_replica_id={
                1: 1,
                2: 0,
            })

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: retiring}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        assert retiring.is_ready
        mgr._terminate_replica.assert_not_called()

    def test_manager_rechecks_ready_coverage_before_accepting_retirement(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        eight = self._ready_backend(1, 8)
        four = self._ready_backend(2, 4)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={
                    1: 8,
                    2: 4
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=four), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[eight, four]):
            mgr._logical_target = (1, 3, 9)
            mgr.scale_down_logically(2, 9, 1, 3)
            defer.assert_not_called()

            mgr._logical_target = (1, 3, 8)
            mgr.scale_down_logically(2, 8, 1, 3)

        defer.assert_called_once_with(2,
                                      logical_retirement=(1, 3, 8),
                                      replica_info=four)

    @pytest.mark.parametrize('victim_still_present', [True, False])
    def test_logical_retirement_uses_one_fleet_snapshot(self,
                                                        victim_still_present):
        """Victim resolution and capacity proof use the same fleet snapshot."""
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        terminal_victim = self._ready_backend(1, 4)
        terminal_victim.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        peer = self._ready_backend(2, 8)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={
                    1: 4,
                    2: 8
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 3, 4)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_info_from_id') as point_read, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=([terminal_victim, peer]
                                             if victim_still_present else
                                             [peer])) as scan:
            mgr.scale_down_logically(1, 4, 1, 3)

        point_read.assert_not_called()
        scan.assert_called_once_with('svc')
        defer.assert_not_called()

    @pytest.mark.parametrize('victim_missing', [True, False])
    def test_terminal_or_missing_retirement_uses_one_fleet_scan(
            self, victim_missing):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        terminal_victim = self._ready_backend(1, 4)
        terminal_victim.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={1: 4},
                in_flight_by_replica_id={1: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 3, 0)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_info_from_id') as point_read, mock.patch.object(
                    replica_managers.serve_state,
                    'get_replica_infos',
                    return_value=([] if victim_missing else [terminal_victim
                                                            ])) as scan:
            mgr.scale_down_logically(1, 0, 1, 3)

        point_read.assert_not_called()
        scan.assert_called_once_with('svc')

    def test_stale_logical_scale_down_batch_reads_no_fleet(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 0)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos') as scan, \
             mock.patch.object(mgr, '_defer_scale_down_until_idle') as defer, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr.scale_down_logically_batch([1, 2, 3], 0, 1, 3)

        scan.assert_not_called()
        defer.assert_not_called()
        terminate.assert_not_called()

    def test_pending_version_rejects_logical_scale_down_batch(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._pending_version = 2
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 0)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos') as scan, \
             mock.patch.object(mgr, '_defer_scale_down_until_idle') as defer, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr.scale_down_logically_batch([1], 0, 1, 4)

        scan.assert_not_called()
        defer.assert_not_called()
        terminate.assert_not_called()

    def test_logical_scale_down_batch_scans_once_and_stops_at_target(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        backends = [
            self._ready_backend(replica_id, 4) for replica_id in (1, 2, 3)
        ]
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={
                    1: 4,
                    2: 4,
                    3: 4
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 4)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=backends) as scan:
            mgr.scale_down_logically_batch([1, 2, 3], 4, 1, 4)

        scan.assert_called_once_with('svc')
        assert defer.call_args_list == [
            mock.call(1, logical_retirement=(1, 4, 4),
                      replica_info=backends[0]),
            mock.call(2, logical_retirement=(1, 4, 4),
                      replica_info=backends[1]),
        ]

    def test_logical_scale_down_batch_uses_exact_observed_contribution(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        degraded = self._ready_backend(1, 8)
        survivor = self._ready_backend(2, 8)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={
                    1: 4,
                    2: 8
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 8)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[degraded, survivor]):
            mgr.scale_down_logically_batch([1, 2], 8, 1, 4)

        defer.assert_called_once_with(1,
                                      logical_retirement=(1, 4, 8),
                                      replica_info=degraded)

    def test_logical_scale_down_batch_skips_duplicates_and_retiring_rows(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        first = self._ready_backend(1, 1)
        already_retiring = self._ready_backend(2, 1)
        already_retiring.status_property.is_scale_down = True
        survivor = self._ready_backend(3, 1)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={
                    1: 1,
                    2: 1,
                    3: 1
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 1)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[first, already_retiring,
                                             survivor]):
            mgr.scale_down_logically_batch([1, 1, 2, 3], 1, 1, 4)

        defer.assert_called_once_with(1,
                                      logical_retirement=(1, 4, 1),
                                      replica_info=first)

    def test_logical_scale_down_batch_aborts_after_acceptance_error(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        backends = [
            self._ready_backend(replica_id, 1) for replica_id in (1, 2, 3)
        ]
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={
                    1: 1,
                    2: 1,
                    3: 1
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 0)
        defer = mock.Mock(side_effect=[None, RuntimeError('persist failed')])
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=backends) as scan, \
             pytest.raises(RuntimeError, match='persist failed'):
            mgr.scale_down_logically_batch([1, 2, 3], 0, 1, 4)

        scan.assert_called_once_with('svc')
        assert defer.call_args_list == [
            mock.call(1, logical_retirement=(1, 4, 0),
                      replica_info=backends[0]),
            mock.call(2, logical_retirement=(1, 4, 0),
                      replica_info=backends[1]),
        ]

    def test_logical_scale_down_batch_handles_unserved_and_outdated_victims(
            self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        unserved = self._ready_backend(1, 4)
        unserved.status_property.service_ready_now = False
        unserved.status_property.first_ready_time = None
        survivor = self._ready_backend(2, 4)
        outdated = self._ready_backend(3, 8)
        outdated.version = 0
        outdated.status_property.service_ready_now = False
        outdated.status_property.first_ready_time = None
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={2: 4},
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 4)
        terminate = mock.Mock()
        mgr._terminate_replica = terminate

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[unserved, survivor, outdated]):
            mgr.scale_down_logically_batch([1, 3], 4, 1, 4)

        assert [call.args[0] for call in terminate.call_args_list] == [1, 3]

    def test_logical_scale_down_batch_matches_sequential_singletons(self):

        def _run(batch: bool):
            mgr = _make_manager()
            mgr._uses_logical_replicas = True
            backends = {
                replica_id: self._ready_backend(replica_id, width)
                for replica_id, width in ((1, 2), (2, 1), (3, 1), (4, 1),
                                          (5, 1), (6, 8), (7, 4))
            }
            backends[6].version = 0
            mgr._logical_reconcile_snapshot = (
                replica_managers.LogicalReconcileSnapshot(
                    version=1,
                    generation=4,
                    observed_slots_by_replica_id={
                        1: 2,
                        2: 1,
                        3: 1,
                        5: 0,
                        6: 8,
                        7: 4,
                    },
                    in_flight_by_replica_id={
                        1: 0,
                        2: 1,
                        3: 0,
                        4: 0,
                        5: 0,
                        6: 0,
                        7: 0,
                    },
                    unknown_replica_ids=frozenset({3}),
                    received_at=replica_managers.time.monotonic()))
            mgr._logical_target = (1, 4, 4)
            accepted = []

            def _defer(replica_id, logical_retirement, *, replica_info):
                assert logical_retirement == (1, 4, 4)
                assert replica_info is backends[replica_id]
                accepted.append(replica_id)
                backends[replica_id].status_property.is_scale_down = True

            mgr._defer_scale_down_until_idle = _defer
            victim_ids = [2, 3, 4, 5, 6, 1, 7]
            with mock.patch.object(
                    replica_managers.serve_state,
                    'get_replica_infos',
                    side_effect=lambda _service: list(backends.values())):
                if batch:
                    mgr.scale_down_logically_batch(victim_ids, 4, 1, 4)
                else:
                    for replica_id in victim_ids:
                        mgr.scale_down_logically(replica_id, 4, 1, 4)
            return accepted

        assert _run(batch=True) == _run(batch=False) == [5, 6, 1]

    def test_zero_capacity_rebalance_replacement_cannot_retire_incumbent(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        victim = self._ready_backend(1, 8)
        replacement = self._ready_backend(2, 8)
        snapshot = replica_managers.LogicalReconcileSnapshot(
            version=1,
            generation=3,
            observed_slots_by_replica_id={
                1: 8,
                2: 0
            },
            in_flight_by_replica_id={
                1: 0,
                2: 0
            },
            unknown_replica_ids=frozenset(),
            received_at=replica_managers.time.monotonic())
        mgr._logical_reconcile_snapshot = snapshot
        mgr._logical_target = (1, 3, 8)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=victim), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[victim, replacement]):
            mgr.scale_down_logically(1, 8, 1, 3)
            defer.assert_not_called()

            snapshot.observed_slots_by_replica_id[2] = 8
            mgr.scale_down_logically(1, 8, 1, 3)

        defer.assert_called_once_with(1,
                                      logical_retirement=(1, 3, 8),
                                      replica_info=victim)

    def test_controller_restart_aborts_persisted_retirement(self):
        mgr = _make_manager()
        retiring = self._ready_backend(1, 8)
        retiring.status_property.logical_retirement_version = 1
        retiring.status_property.logical_retirement_controller_epoch = (
            'prior-controller-epoch')
        retiring.status_property.logical_retirement_generation = 3
        retiring.status_property.logical_retirement_target_capacity = 0

        assert mgr._logical_retirement_state(retiring) == 'abort'

    def test_successful_logical_retirement_persists_confirmation_before_down(
            self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        retiring = self._ready_backend(1, 4)
        survivor = self._ready_backend(2, 8)
        status = retiring.status_property
        status.is_scale_down = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.wait_for_idle_before_termination = True
        status.logical_retirement_version = 1
        status.logical_retirement_controller_epoch = 'test-controller-epoch'
        status.logical_retirement_generation = 4
        status.logical_retirement_target_capacity = 8
        status.logical_retirement_confirmed_generation = None
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=5,
                # The controller removes SHUTTING_DOWN replicas from its
                # URL-to-ID translation before this post-retirement report.
                observed_slots_by_replica_id={2: 8},
                in_flight_by_replica_id={2: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 5, 8)
        mgr._wait_for_idle_trackers = {
            1: (mock.Mock(return_value=True),
                replica_managers.time.monotonic() + 60)
        }
        persisted = []
        mgr._persist_replica = mock.Mock(
            side_effect=lambda _rid, info: persisted.append(
                (info.status_property.logical_retirement_confirmed_generation,
                 info.status_property.wait_for_idle_before_termination)))
        mgr._terminate_replica = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: retiring}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={'svc-1': ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        assert persisted[-1:] == [(5, True)]
        mgr._terminate_replica.assert_called_once_with(
            1,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)
        assert 1 in mgr._wait_for_idle_trackers

    @pytest.mark.parametrize('retiring_version,should_terminate', [(9, True),
                                                                   (10, False)])
    def test_unknown_then_absent_logical_retirement_deadline_is_bounded_only_for_outdated_backend(
            self, monkeypatch, retiring_version, should_terminate):
        now = [100.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        mgr, retiring, survivor = self._pending_logical_retirement(
            retiring_version=retiring_version)
        tracker = replica_managers._ReplicaDrainTracker(mgr,
                                                        'http://old-backend',
                                                        drain_started=now[0])
        mgr._wait_for_idle_trackers = {9: (tracker, 160.0)}

        def _refresh():
            with mock.patch.object(replica_managers.serve_state,
                                   'get_replica_infos_from_ids',
                                   return_value={9: retiring}), \
                 mock.patch.object(replica_managers.serve_state,
                                   'get_replica_infos',
                                   return_value=[retiring, survivor]), \
                 mock.patch.object(
                     replica_managers.global_user_state,
                     'get_cluster_status_fields',
                     return_value={'svc-9': ('UP', 1)}):
                mgr._refresh_wait_for_idle()

        # Reproduce the production migration: the old nginx backend is first
        # occupancy-UNKNOWN, then disappears from every LB overlay. Absence
        # cannot clear the tracker's UNKNOWN taint before the deadline.
        now[0] = 110.0
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, received_at=now[0])
        mgr._lb_in_flight_report = (now[0], {
            'http://old-backend': 0
        }, set(), {'http://old-backend'}, set(), 'lb-session')
        _refresh()
        mgr._terminate_replica.assert_not_called()

        now[0] = 120.0
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, received_at=now[0])
        mgr._lb_in_flight_report = (now[0], {}, set(), set(), set(),
                                    'lb-session')
        _refresh()
        mgr._terminate_replica.assert_not_called()

        now[0] = 160.0
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, received_at=now[0])
        mgr._lb_in_flight_report = (now[0], {}, set(), set(), set(),
                                    'lb-session')
        _refresh()

        if should_terminate:
            mgr._terminate_replica.assert_called_once_with(
                9,
                sync_down_logs=False,
                replica_drain_delay_seconds=0,
                is_scale_down=True,
                in_flight_drain_cap_seconds=0)
            assert retiring.status_property.is_scale_down
        else:
            mgr._terminate_replica.assert_not_called()
            assert not retiring.status_property.is_scale_down
        assert (retiring.status_property.logical_retirement_bounded_deadline
                is should_terminate)
        assert 9 not in mgr._wait_for_idle_trackers

    @pytest.mark.parametrize('guard', [
        'stale_snapshot',
        'pending_update',
        'target_growth',
        'unknown_replacement',
        'insufficient_replacement',
    ])
    def test_outdated_deadline_never_bypasses_logical_coverage_fences(
            self, monkeypatch, guard):
        now = [200.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        mgr, retiring, survivor = self._pending_logical_retirement()
        snapshot = mgr._logical_reconcile_snapshot
        if guard == 'stale_snapshot':
            snapshot = dataclasses.replace(snapshot, received_at=0.0)
        elif guard == 'pending_update':
            mgr._pending_version = 11
        elif guard == 'target_growth':
            mgr._logical_target = (10, 5, 2)
        elif guard == 'unknown_replacement':
            snapshot = dataclasses.replace(snapshot,
                                           unknown_replica_ids=frozenset({10}))
        elif guard == 'insufficient_replacement':
            snapshot.observed_slots_by_replica_id[10] = 0
        mgr._logical_reconcile_snapshot = snapshot
        retiring.status_property.drain_started_at = 1234.5
        mgr._lb_in_flight_report = (now[0], {}, set(), set(), set(),
                                    'lb-session')
        mgr._wait_for_idle_trackers = {
            9: (mock.Mock(return_value=False), now[0])
        }

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={9: retiring}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={'svc-9': ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        mgr._terminate_replica.assert_not_called()
        if guard == 'stale_snapshot':
            assert retiring.status_property.wait_for_idle_before_termination
            assert 9 in mgr._wait_for_idle_trackers
        else:
            assert not retiring.status_property.is_scale_down
            assert retiring.status_property.drain_started_at is None
            assert 9 not in mgr._wait_for_idle_trackers

    def test_outdated_bounded_retirement_retries_scheduling_failure(
            self, monkeypatch):
        now = [200.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        mgr, retiring, survivor = self._pending_logical_retirement()
        tracker = mock.Mock(return_value=False)
        mgr._wait_for_idle_trackers = {9: (tracker, now[0])}
        mgr._terminate_replica = mock.Mock(
            side_effect=[RuntimeError('database unavailable'), None])

        def _refresh():
            with mock.patch.object(replica_managers.serve_state,
                                   'get_replica_infos_from_ids',
                                   return_value={9: retiring}), \
                 mock.patch.object(replica_managers.serve_state,
                                   'get_replica_infos',
                                   return_value=[retiring, survivor]), \
                 mock.patch.object(
                     replica_managers.global_user_state,
                     'get_cluster_status_fields',
                     return_value={'svc-9': ('UP', 1)}):
                mgr._refresh_wait_for_idle()

        with pytest.raises(RuntimeError, match='database unavailable'):
            _refresh()
        assert retiring.status_property.wait_for_idle_before_termination
        assert retiring.status_property.logical_retirement_bounded_deadline
        assert 9 in mgr._wait_for_idle_trackers

        _refresh()
        assert mgr._terminate_replica.call_count == 2
        assert 9 not in mgr._wait_for_idle_trackers

    @pytest.mark.parametrize(
        'bounded_deadline,confirmed_generation,should_start', [
            (False, 5, False),
            (True, 5, True),
            (True, None, False),
            (True, True, False),
            (True, '5', False),
        ])
    def test_only_bounded_outdated_confirmation_bypasses_late_victim_occupancy(
            self, monkeypatch, tmp_path, bounded_deadline, confirmed_generation,
            should_start):
        now = [200.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        mgr, retiring, survivor = self._pending_logical_retirement()
        retiring.status_property.wait_for_idle_before_termination = False
        retiring.status_property.logical_retirement_confirmed_generation = (
            confirmed_generation)
        retiring.status_property.logical_retirement_bounded_deadline = (
            bounded_deadline)
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            generation=6,
            in_flight_by_replica_id={
                9: 1,
                10: 0
            },
            received_at=now[0])
        mgr._logical_target = (10, 6, 1)
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        mgr._down_thread_pool[9] = down_thread

        with mock.patch.object(mgr, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 mgr, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               ({9: retiring} if ids else {})), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        if should_start:
            down_thread.start.assert_called_once_with()
            assert (retiring.status_property.sky_down_status ==
                    common_utils.ProcessStatus.RUNNING)
            assert retiring.status_property.logical_retirement_committed
            persist.assert_called_once_with(9, retiring)
        else:
            down_thread.start.assert_not_called()
            assert (retiring.status_property.sky_down_status ==
                    common_utils.ProcessStatus.SCHEDULED)
            persist.assert_not_called()

    @pytest.mark.parametrize('scenario', [
        'outdated_bounded_start',
        'same_version_abort',
        'target_growth_abort',
    ])
    def test_budget_delayed_logical_admission_retains_original_deadline(
            self, monkeypatch, tmp_path, scenario):
        now = [100.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        retiring_version = 10 if scenario == 'same_version_abort' else 9
        mgr, retiring, survivor = self._pending_logical_retirement(
            retiring_version=retiring_version)
        mgr._terminate_replica = types.MethodType(
            replica_managers.SkyPilotReplicaManager._terminate_replica, mgr)
        mgr._resource_scope = None
        tracker = replica_managers._ReplicaDrainTracker(mgr,
                                                        'http://old-backend',
                                                        drain_started=90.0)
        mgr._wait_for_idle_trackers = {9: (tracker, 160.0)}
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        down_thread.format_exc = None
        mgr._persist_replica = mock.Mock()

        def _infos_from_ids(_service_name, replica_ids):
            infos = {9: retiring, 10: survivor}
            return {
                replica_id: infos[replica_id]
                for replica_id in replica_ids
                if replica_id in infos
            }

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=retiring), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=_infos_from_ids), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={
                     retiring.cluster_name: ('UP', 1),
                     survivor.cluster_name: ('UP', 1)
                 }), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'cluster_with_name_exists',
                 return_value=True), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value=str(tmp_path / 'replica.log')), \
             mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=down_thread), \
             mock.patch.object(
                 mgr, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(
                 controller_utils,
                 'can_terminate',
                 side_effect=lambda *_args, **_kwargs: now[0] >= 200.0):
            # Explicit idle proves the victim drained, but the global
            # terminate budget cannot admit its already-scheduled worker.
            mgr._lb_in_flight_report = (now[0], {
                'http://old-backend': 0
            }, set(), set(), {'http://old-backend'}, 'lb-session')
            mgr._refresh_thread_pool()
            down_thread.start.assert_not_called()
            assert 9 in mgr._down_thread_pool
            assert 9 in mgr._wait_for_idle_trackers
            assert not retiring.status_property.wait_for_idle_before_termination
            assert not (
                retiring.status_property.logical_retirement_bounded_deadline)
            assert not retiring.status_property.logical_retirement_committed

            # A late busy report invalidates the ordinary idle proof. The
            # original deadline must still promote only an outdated backend;
            # same-version or grown-target retirement is cancelled.
            now[0] = 200.0
            mgr._logical_reconcile_snapshot = dataclasses.replace(
                mgr._logical_reconcile_snapshot,
                in_flight_by_replica_id={
                    9: 1,
                    10: 0
                },
                received_at=now[0])
            mgr._lb_in_flight_report = (now[0], {
                'http://old-backend': 1
            }, set(), set(), {'http://old-backend'}, 'lb-session')
            if scenario == 'target_growth_abort':
                mgr._logical_target = (10, 5, 2)
            mgr._refresh_thread_pool()

        if scenario == 'outdated_bounded_start':
            down_thread.start.assert_called_once_with()
            assert retiring.status_property.is_scale_down
            assert (retiring.status_property.sky_down_status ==
                    common_utils.ProcessStatus.RUNNING)
            assert (
                retiring.status_property.logical_retirement_bounded_deadline)
            assert retiring.status_property.logical_retirement_committed
        else:
            down_thread.start.assert_not_called()
            assert not retiring.status_property.is_scale_down
            assert retiring.status_property.sky_down_status is None
            assert 9 not in mgr._down_thread_pool
        assert 9 not in mgr._wait_for_idle_trackers

    def _recover_logical_teardown(self,
                                  tmp_path,
                                  down_status,
                                  bounded_deadline,
                                  committed=True,
                                  current_target=1):
        mgr, retiring, survivor = self._pending_logical_retirement()
        mgr._terminate_replica = types.MethodType(
            replica_managers.SkyPilotReplicaManager._terminate_replica, mgr)
        mgr._logical_controller_epoch = 'new-controller-epoch'
        mgr._logical_target = (10, 5, current_target)
        mgr._resource_scope = None
        status = retiring.status_property
        status.logical_retirement_controller_epoch = 'old-controller-epoch'
        status.wait_for_idle_before_termination = False
        status.logical_retirement_confirmed_generation = 5
        status.logical_retirement_bounded_deadline = bounded_deadline
        status.logical_retirement_committed = committed
        status.sky_down_status = down_status
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        down_thread.format_exc = None
        mgr._persist_replica = mock.Mock()

        def _infos_from_ids(_service_name, replica_ids):
            infos = {9: retiring, 10: survivor}
            return {
                replica_id: infos[replica_id]
                for replica_id in replica_ids
                if replica_id in infos
            }

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=retiring), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=_infos_from_ids), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={
                     retiring.cluster_name: ('UP', 1),
                     survivor.cluster_name: ('UP', 1)
                 }), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'cluster_with_name_exists',
                 return_value=True), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value=str(tmp_path / 'replica.log')), \
             mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=down_thread), \
             mock.patch.object(mgr, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 mgr, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True):
            mgr._recover_replica_operations()
            # FAILED cleanup is reconciled at the end of the first refresh
            # and admitted on the next one. SCHEDULED/RUNNING recovery has a
            # worker ready for admission immediately.
            if down_status == common_utils.ProcessStatus.FAILED:
                mgr._refresh_thread_pool()
            mgr._refresh_thread_pool()

        return mgr, retiring, down_thread

    @pytest.mark.parametrize('bounded_deadline', [False, True])
    @pytest.mark.parametrize('down_status', [
        common_utils.ProcessStatus.SCHEDULED,
        common_utils.ProcessStatus.RUNNING,
        common_utils.ProcessStatus.FAILED,
    ])
    def test_recovery_never_reactivates_committed_logical_teardown(
            self, tmp_path, bounded_deadline, down_status):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path, down_status, bounded_deadline)

        down_thread.start.assert_called_once_with()
        assert retiring.status_property.is_scale_down
        assert (retiring.status_property.sky_down_status ==
                common_utils.ProcessStatus.RUNNING)
        assert retiring.status_property.logical_retirement_version is None
        assert (retiring.status_property.logical_retirement_controller_epoch
                is None)
        assert (retiring.status_property.logical_retirement_generation is None)
        assert (retiring.status_property.logical_retirement_target_capacity
                is None)
        assert (retiring.status_property.logical_retirement_confirmed_generation
                is None)
        assert not retiring.status_property.logical_retirement_bounded_deadline
        assert not retiring.status_property.logical_retirement_committed
        assert 9 in mgr._down_thread_pool

    def test_recovery_aborts_valid_unadmitted_scheduled_logical_retirement(
            self, tmp_path):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path,
            common_utils.ProcessStatus.SCHEDULED,
            bounded_deadline=False,
            committed=False)

        down_thread.start.assert_not_called()
        assert not retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status is None
        assert retiring.status_property.logical_retirement_version is None
        assert not retiring.status_property.logical_retirement_committed
        assert 9 not in mgr._down_thread_pool

    def test_recovery_adopts_legacy_ambiguous_scheduled_retirement(
            self, tmp_path):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path,
            common_utils.ProcessStatus.SCHEDULED,
            bounded_deadline=False,
            committed=None)

        down_thread.start.assert_called_once_with()
        assert retiring.status_property.is_scale_down
        assert (retiring.status_property.sky_down_status ==
                common_utils.ProcessStatus.RUNNING)
        assert retiring.status_property.logical_retirement_version is None
        assert 9 not in mgr._legacy_uncertain_logical_retirement_ids
        assert 9 in mgr._down_thread_pool

    @pytest.mark.parametrize('down_status', [
        common_utils.ProcessStatus.RUNNING,
        common_utils.ProcessStatus.FAILED,
    ])
    def test_recovery_finishes_intrinsically_committed_legacy_teardown(
            self, tmp_path, down_status):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path, down_status, bounded_deadline=False, committed=None)

        down_thread.start.assert_called_once_with()
        assert retiring.status_property.is_scale_down
        assert (retiring.status_property.sky_down_status ==
                common_utils.ProcessStatus.RUNNING)
        assert retiring.status_property.logical_retirement_version is None
        assert 9 in mgr._down_thread_pool

    def test_recovery_keeps_legacy_ambiguous_retirement_off_route_until_covered(
            self, tmp_path):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path,
            common_utils.ProcessStatus.SCHEDULED,
            bounded_deadline=False,
            committed=None,
            current_target=2)

        down_thread.start.assert_not_called()
        assert retiring.status_property.is_scale_down
        assert (retiring.status_property.sky_down_status ==
                common_utils.ProcessStatus.SCHEDULED)
        assert (retiring.status_property.logical_retirement_committed is None)
        assert 9 in mgr._legacy_uncertain_logical_retirement_ids
        assert 9 not in mgr._down_thread_pool

    def test_recovery_revalidates_unadmitted_bounded_retirement_after_growth(
            self, tmp_path):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path,
            common_utils.ProcessStatus.SCHEDULED,
            bounded_deadline=True,
            committed=False,
            current_target=2)

        down_thread.start.assert_not_called()
        assert not retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status is None
        assert retiring.status_property.logical_retirement_version is None
        assert not retiring.status_property.logical_retirement_committed
        assert 9 not in mgr._down_thread_pool

    @pytest.mark.parametrize('malformed_field,malformed_value', [
        ('logical_retirement_confirmed_generation', None),
        ('logical_retirement_confirmed_generation', True),
        ('logical_retirement_confirmed_generation', '5'),
        ('logical_retirement_bounded_deadline', 'true'),
        ('logical_retirement_committed', 'true'),
        ('logical_retirement_version', True),
        ('logical_retirement_controller_epoch', ''),
    ])
    def test_recovery_keeps_unconfirmed_or_malformed_teardown_fail_closed(
            self, tmp_path, malformed_field, malformed_value):
        mgr, retiring, survivor = self._pending_logical_retirement()
        mgr._terminate_replica = types.MethodType(
            replica_managers.SkyPilotReplicaManager._terminate_replica, mgr)
        mgr._logical_controller_epoch = 'new-controller-epoch'
        mgr._resource_scope = None
        status = retiring.status_property
        status.logical_retirement_controller_epoch = 'old-controller-epoch'
        status.wait_for_idle_before_termination = False
        status.logical_retirement_confirmed_generation = 5
        status.logical_retirement_bounded_deadline = False
        status.logical_retirement_committed = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        setattr(status, malformed_field, malformed_value)
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        down_thread.format_exc = None
        mgr._persist_replica = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=retiring), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={9: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={
                     retiring.cluster_name: ('UP', 1),
                     survivor.cluster_name: ('UP', 1)
                 }), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'cluster_with_name_exists',
                 return_value=True), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value=str(tmp_path / 'replica.log')), \
             mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=down_thread), \
             mock.patch.object(mgr, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 mgr, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True):
            mgr._recover_replica_operations()
            mgr._refresh_thread_pool()

        down_thread.start.assert_not_called()
        assert not retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status is None
        assert 9 not in mgr._down_thread_pool

    def test_logical_retirement_retries_termination_scheduling_failure(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        retiring = self._ready_backend(1, 4)
        survivor = self._ready_backend(2, 8)
        status = retiring.status_property
        status.is_scale_down = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.wait_for_idle_before_termination = True
        status.logical_retirement_version = 1
        status.logical_retirement_controller_epoch = 'test-controller-epoch'
        status.logical_retirement_generation = 4
        status.logical_retirement_target_capacity = 8
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=5,
                observed_slots_by_replica_id={
                    1: 0,
                    2: 8
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 5, 8)
        tracker = mock.Mock(return_value=True)
        mgr._wait_for_idle_trackers = {
            1: (tracker, replica_managers.time.monotonic() + 60)
        }
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock(
            side_effect=[RuntimeError('database unavailable'), None])

        def _refresh():
            with mock.patch.object(replica_managers.serve_state,
                                   'get_replica_infos_from_ids',
                                   return_value={1: retiring}), \
                 mock.patch.object(replica_managers.serve_state,
                                   'get_replica_infos',
                                   return_value=[retiring, survivor]), \
                 mock.patch.object(
                     replica_managers.global_user_state,
                     'get_cluster_status_fields',
                     return_value={'svc-1': ('UP', 1)}):
                mgr._refresh_wait_for_idle()

        with pytest.raises(RuntimeError, match='database unavailable'):
            _refresh()

        assert status.wait_for_idle_before_termination
        assert 1 in mgr._wait_for_idle_trackers
        _refresh()
        assert mgr._terminate_replica.call_count == 2
        assert 1 in mgr._wait_for_idle_trackers

    @pytest.mark.parametrize('confirmed_generation', [None, 5])
    def test_restart_before_or_after_confirmation_never_terminates_stale_intent(
            self, confirmed_generation):
        mgr = _make_manager()
        mgr._logical_controller_epoch = 'new-controller-epoch'
        mgr._is_pool = False
        retiring = self._ready_backend(1, 4)
        status = retiring.status_property
        status.is_scale_down = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.wait_for_idle_before_termination = True
        status.logical_retirement_version = 1
        status.logical_retirement_controller_epoch = 'old-controller-epoch'
        status.logical_retirement_generation = 4
        status.logical_retirement_target_capacity = 8
        status.logical_retirement_confirmed_generation = confirmed_generation
        mgr._wait_for_idle_trackers = {
            1: (mock.Mock(return_value=True),
                replica_managers.time.monotonic() + 60)
        }
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={'svc-1': ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        mgr._terminate_replica.assert_not_called()
        assert not status.is_scale_down
        assert status.logical_retirement_confirmed_generation is None
        assert 1 not in mgr._wait_for_idle_trackers

    def test_pending_retirements_reserve_capacity_sequentially(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        backends = [
            replica_managers.ReplicaInfo(replica_id=replica_id,
                                         cluster_name=f'svc-{replica_id}',
                                         replica_port='8080',
                                         is_spot=True,
                                         location=None,
                                         version=1,
                                         resources_override=None,
                                         planned_capacity=4)
            for replica_id in (1, 2, 3)
        ]
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 4)
        terminated = []

        def _terminate(replica_id, **_kwargs):
            terminated.append(replica_id)
            info = backends[replica_id - 1]
            info.status_property.is_scale_down = True
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.SCHEDULED)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_info_from_id',
                side_effect=lambda _service, replica_id: backends[
                    replica_id - 1]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=backends), \
             mock.patch.object(mgr,
                               '_terminate_replica',
                               side_effect=_terminate):
            for replica_id in (1, 2, 3):
                mgr.scale_down_logically(replica_id, 4, 1, 4)

        assert terminated == [1, 2]


class TestLaunchReplicaSnapshotAccumulation:
    """Bulk launches must preserve in-wave reserved-capacity accounting.

    Recovery re-drive passes a single existing_replica_infos snapshot
    across a whole wave of launches; without appending each newly placed
    replica, later launches can overbook the same zero-cost capacity.
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
        placer.active_locations.return_value = []
        placer.zero_cost_locations.return_value = []
        manager._spot_placer = placer
        return manager

    def test_wave_launches_do_not_feed_load_to_placer(self):
        # pylint: disable=protected-access
        placer = mock.Mock()
        location = mock.Mock()
        location.to_dict.return_value = {'zone': 'z'}
        placer.select_next_location.return_value = location
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
        # Placement is cheapest-first and independent of fleet load.
        assert placer.select_next_location.call_count == 2
        assert all(not call.args
                   for call in placer.select_next_location.call_args_list)

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

    def test_recovery_preserves_exact_spot_location(self):
        # A recovered spot row already owns its cluster name. Selecting a new
        # location would create a resource mismatch and overwrite the only
        # durable identity available to cleanup.
        placer = mock.Mock()
        manager = self._make_manager(placer)
        resources_override = {
            'cloud': 'AWS',
            'region': 'ap-northeast-1',
            'zone': 'ap-northeast-1a',
            'accelerators': {
                'L4': 1
            },
            'use_spot': True,
        }
        persisted = []

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch.object(manager,
                               '_persist_replica',
                               side_effect=lambda _rid, info: persisted.append(
                                   info)), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers.thread_utils.SafeThread'):
            manager._launch_replica(replica_id=1463,
                                    resources_override=resources_override,
                                    existing_replica_infos=[],
                                    recovering_existing_replica=True,
                                    prior_version=1,
                                    prior_yaml_content='resources: {}')

        placer.select_next_location.assert_not_called()
        placer.select_next_zero_cost_location.assert_not_called()
        assert len(persisted) == 1
        assert persisted[0].resources_override == resources_override
        assert persisted[0].get_spot_location() == (
            replica_managers.spot_placer.Location.from_resources_override(
                resources_override))

    def test_logical_recovery_preserves_persisted_capacity(self):
        placer = mock.Mock()
        manager = self._make_manager(placer)
        manager._uses_logical_replicas = True
        manager._default_planned_capacity = 1
        resources_override = {
            'cloud': 'AWS',
            'region': 'us-east-1',
            'zone': 'us-east-1a',
            'accelerators': {
                'L4': 1
            },
            'use_spot': True,
        }
        persisted = []

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch.object(
                 manager,
                 '_persist_replica',
                 side_effect=lambda _rid, info: persisted.append(info)), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers.thread_utils.SafeThread'):
            manager._launch_replica(replica_id=1,
                                    resources_override=resources_override,
                                    existing_replica_infos=[],
                                    recovering_existing_replica=True,
                                    prior_planned_capacity=8,
                                    prior_version=7,
                                    prior_yaml_content='resources: {}')

        assert len(persisted) == 1
        assert persisted[0].planned_capacity == 8
        assert persisted[0].version == 7

    def test_recovery_uses_original_version_yaml_during_unit_transition(self):
        placer = mock.Mock()
        manager = self._make_manager(placer)
        manager.latest_version = 8
        manager.yaml_content = 'resources:\n  accelerators: A100:8\n'
        manager._uses_logical_replicas = True
        persisted = []
        thread = mock.Mock()

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=False), \
             mock.patch.object(
                 manager,
                 '_persist_replica',
                 side_effect=lambda _rid, info: persisted.append(info)), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080') as get_ports, \
             mock.patch('sky.serve.replica_managers.thread_utils.SafeThread',
                        return_value=thread) as safe_thread:
            manager._launch_replica(
                replica_id=1,
                resources_override={'accelerators': {
                    'L4': 1
                }},
                existing_replica_infos=[],
                recovering_existing_replica=True,
                prior_planned_capacity=1,
                prior_version=7,
                prior_yaml_content='resources:\n  accelerators: L4:1\n')

        assert len(persisted) == 1
        assert persisted[0].version == 7
        assert persisted[0].planned_capacity == 1
        assert safe_thread.call_args.kwargs['args'][1] == (
            'resources:\n  accelerators: L4:1\n')
        get_ports.assert_called_once_with('resources:\n  accelerators: L4:1\n',
                                          None)


class TestFailedCleanupReconciliation:

    @staticmethod
    def _info(replica_id=1, version=1):
        info = replica_managers.ReplicaInfo(replica_id, f'svc-{replica_id}',
                                            '8080', False, None, version, None)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        return info

    @pytest.mark.parametrize('terminal_kind', [
        'scale_down',
        'purge',
        'preempted',
        'outdated',
        'spot_availability',
    ])
    def test_failed_down_never_removes_durable_row(self, terminal_kind):
        manager = _make_manager()
        info = self._info(version=0 if terminal_kind == 'outdated' else 1)
        if terminal_kind == 'scale_down':
            info.status_property.is_scale_down = True
        elif terminal_kind == 'purge':
            info.status_property.purged = True
        elif terminal_kind == 'preempted':
            info.status_property.preempted = True
        elif terminal_kind == 'spot_availability':
            info.status_property.failed_spot_availability = True

        with mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_remove_replica') as remove, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        return_value=100):
            manager._handle_sky_down_finish(info, 'provider error')

        persist.assert_called_once_with(1, info)
        remove.assert_not_called()
        assert (info.status_property.sky_down_status ==
                common_utils.ProcessStatus.FAILED)
        assert manager._failed_cleanup_retry_attempts == {1: 1}
        assert manager._failed_cleanup_retry_at == {1: 160}

    def test_raw_preempted_down_failure_is_reconciled(self):
        # PREEMPTED intentionally wins derived status, so retry eligibility
        # must also inspect the raw failed-down field.
        manager = _make_manager()
        info = self._info()
        info.status_property.preempted = True
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
        assert info.status == replica_managers.serve_state.ReplicaStatus.PREEMPTED

        with mock.patch.object(manager, '_terminate_replica') as terminate, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        return_value=100):
            manager._reconcile_failed_cleanup([info])

        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True,
                                          purge=False,
                                          in_flight_drain_cap_seconds=0)

    def test_provider_failure_does_not_repeat_consumed_drain(self):
        manager = _make_manager()
        info = self._info()
        info.status_property.is_scale_down = True
        info.status_property.drain_cap_seconds = 600
        info.status_property.drain_started_at = 10.0
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED

        with mock.patch.object(manager, '_terminate_replica') as terminate, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        return_value=100):
            manager._reconcile_failed_cleanup([info])

        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True,
                                          purge=False,
                                          in_flight_drain_cap_seconds=0)

    def test_legacy_failed_row_gets_one_conservative_bounded_drain(self):
        manager = _make_manager()
        info = self._info()
        info.status_property.is_scale_down = True
        info.status_property.drain_cap_seconds = 600
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
        legacy_state = info.to_storage_dict()
        legacy_state['status_property'].pop('drain_started_at')
        legacy_info = replica_managers.ReplicaInfo.from_storage_dict(
            legacy_state)
        assert legacy_info.status_property.drain_started_at is None

        with mock.patch.object(manager, '_terminate_replica') as terminate, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        return_value=100):
            manager._reconcile_failed_cleanup([legacy_info])

        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True,
                                          purge=False,
                                          in_flight_drain_cap_seconds=600)

    def test_cleanup_retry_respects_capped_backoff_deadline(self):
        manager = _make_manager()
        info = self._info()
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
        manager._failed_cleanup_retry_at[1] = 200

        with mock.patch.object(manager, '_terminate_replica') as terminate, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        side_effect=[199, 200]):
            manager._reconcile_failed_cleanup([info])
            terminate.assert_not_called()
            manager._reconcile_failed_cleanup([info])

        terminate.assert_called_once()

    def test_cleanup_retry_delay_is_capped(self):
        manager = _make_manager()
        manager._failed_cleanup_retry_attempts[1] = 100

        with mock.patch('sky.serve.replica_managers.time.monotonic',
                        return_value=100):
            manager._schedule_failed_cleanup_retry(1)

        assert manager._failed_cleanup_retry_attempts == {1: 101}
        assert manager._failed_cleanup_retry_at == {
            1: 100 + replica_managers._FAILED_CLEANUP_RETRY_MAX_SECONDS
        }

    def test_successful_absent_cleanup_clears_retry_and_removes_old_row(self):
        manager = _make_manager()
        info = self._info(version=0)
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
        manager._failed_cleanup_retry_attempts[1] = 3
        manager._failed_cleanup_retry_at[1] = 500

        with mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_remove_replica') as remove:
            manager._handle_sky_down_finish(info, format_exc=None)

        remove.assert_called_once_with(1)
        persist.assert_not_called()
        assert not manager._failed_cleanup_retry_attempts
        assert not manager._failed_cleanup_retry_at

    def test_synchronous_reconcile_error_is_backed_off(self):
        manager = _make_manager()
        info = self._info()
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED

        with mock.patch.object(manager,
                               '_terminate_replica',
                               side_effect=RuntimeError('database error')), \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        side_effect=[100, 100]):
            manager._reconcile_failed_cleanup([info])

        assert manager._failed_cleanup_retry_attempts == {1: 1}
        assert manager._failed_cleanup_retry_at == {1: 160}

    def test_finished_down_worker_survives_completion_persist_error(self):
        manager = _make_manager()
        manager._is_pool = False
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        down_thread.format_exc = 'provider error'
        manager._down_thread_pool[1] = down_thread
        info = self._info()
        info.status_property.sky_down_status = common_utils.ProcessStatus.RUNNING

        with mock.patch.object(manager, '_refresh_wait_for_idle'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _service, ids:
                               ({1: info} if ids else {})), \
             mock.patch.object(manager,
                               '_persist_replica',
                               side_effect=RuntimeError('database error')), \
             pytest.raises(RuntimeError, match='database error'):
            manager._refresh_thread_pool()

        assert manager._down_thread_pool[1] is down_thread

    @pytest.mark.parametrize('server_committed', [False, True])
    def test_ambiguous_down_admission_write_replaces_unstarted_worker(
            self, tmp_path, server_committed):
        manager, retiring, survivor = (
            TestLogicalCapacityPlanning()._pending_logical_retirement())
        manager._terminate_replica = types.MethodType(
            replica_managers.SkyPilotReplicaManager._terminate_replica, manager)
        manager._resource_scope = None
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        retiring.status_property.wait_for_idle_before_termination = False
        retiring.status_property.logical_retirement_confirmed_generation = 5
        original_thread = mock.Mock()
        original_thread.is_alive.return_value = False
        original_thread.format_exc = None
        fresh_thread = mock.Mock()
        fresh_thread.is_alive.return_value = False
        fresh_thread.format_exc = None
        manager._down_thread_pool[9] = original_thread
        durable = {
            9: replica_managers.ReplicaInfo.from_storage_dict(
                retiring.to_storage_dict())
        }
        persist_calls = [0]

        def _persist(replica_id, info):
            persist_calls[0] += 1
            if persist_calls[0] == 1:
                if server_committed:
                    durable[replica_id] = (
                        replica_managers.ReplicaInfo.from_storage_dict(
                            info.to_storage_dict()))
                raise RuntimeError('ambiguous database write')
            durable[replica_id] = (
                replica_managers.ReplicaInfo.from_storage_dict(
                    info.to_storage_dict()))

        with mock.patch.object(manager, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 manager, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _service, ids:
                               ({9: retiring} if ids else {})), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               side_effect=lambda _service, replica_id:
                               durable.get(replica_id)), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value=str(tmp_path / 'replica.log')), \
             mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=fresh_thread), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True), \
             mock.patch.object(manager,
                               '_persist_replica',
                               side_effect=_persist), \
             pytest.raises(RuntimeError, match='ambiguous database write'):
            manager._refresh_thread_pool()

        original_thread.start.assert_not_called()
        assert manager._down_thread_pool[9] is fresh_thread
        assert (durable[9].status_property.sky_down_status ==
                common_utils.ProcessStatus.SCHEDULED)
        if server_committed:
            assert durable[9].status_property.logical_retirement_version is None
        else:
            assert durable[9].status_property.logical_retirement_version == 10
            assert not durable[9].status_property.logical_retirement_committed

    @pytest.mark.parametrize('failed_state_persist_raises', [False, True])
    def test_down_worker_start_failure_retries_committed_cleanup(
            self, tmp_path, failed_state_persist_raises):
        manager, retiring, survivor = (
            TestLogicalCapacityPlanning()._pending_logical_retirement())
        manager._terminate_replica = types.MethodType(
            replica_managers.SkyPilotReplicaManager._terminate_replica, manager)
        manager._resource_scope = None
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        retiring.status_property.wait_for_idle_before_termination = False
        retiring.status_property.logical_retirement_confirmed_generation = 5
        retiring.status_property.drain_cap_seconds = 600
        retiring.status_property.drain_started_at = 10.0
        original_thread = mock.Mock()
        original_thread.is_alive.return_value = False
        original_thread.format_exc = None
        original_thread.start.side_effect = RuntimeError('thread start failed')
        fresh_thread = mock.Mock()
        fresh_thread.is_alive.return_value = False
        fresh_thread.format_exc = None
        manager._down_thread_pool[9] = original_thread
        manager._wait_for_idle_trackers[9] = (None, 999)
        durable = {
            9: replica_managers.ReplicaInfo.from_storage_dict(
                retiring.to_storage_dict())
        }
        clock = [100]
        failed_persist_attempted = [False]

        def _clone(info):
            return replica_managers.ReplicaInfo.from_storage_dict(
                info.to_storage_dict())

        def _persist(replica_id, info):
            if (failed_state_persist_raises and
                    not failed_persist_attempted[0] and
                    info.status_property.sky_down_status
                    == common_utils.ProcessStatus.SCHEDULED):
                failed_persist_attempted[0] = True
                raise RuntimeError('failed-state database write')
            durable[replica_id] = _clone(info)

        def _read_many(_service, replica_ids):
            return {
                replica_id: _clone(durable[replica_id])
                for replica_id in replica_ids
                if replica_id in durable
            }

        def _read_all(_service):
            return [_clone(durable[9]), _clone(survivor)]

        with mock.patch.object(
                manager, '_reconcile_legacy_uncertain_logical_retirements'), \
             mock.patch.object(manager, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 manager, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=_read_many), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               side_effect=lambda _service, replica_id:
                               _clone(durable[replica_id])), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               side_effect=_read_all), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value=str(tmp_path / 'replica.log')), \
             mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=fresh_thread) as safe_thread_factory, \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils,
                               'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True), \
             mock.patch.object(manager,
                               '_persist_replica',
                               side_effect=_persist), \
             mock.patch.object(manager, '_remove_replica') as remove, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        side_effect=lambda: clock[0]), \
             mock.patch('sky.serve.replica_managers.time.time',
                        side_effect=lambda: clock[0]):
            if failed_state_persist_raises:
                with pytest.raises(RuntimeError,
                                   match='failed-state database write'):
                    manager._refresh_thread_pool()
            else:
                manager._refresh_thread_pool()

            original_thread.start.assert_called_once_with()
            assert 9 not in manager._down_thread_pool
            assert 9 not in manager._wait_for_idle_trackers
            assert manager._failed_cleanup_retry_attempts == {9: 1}
            assert manager._failed_cleanup_retry_at == {9: 160}
            expected_durable_status = (common_utils.ProcessStatus.RUNNING
                                       if failed_state_persist_raises else
                                       common_utils.ProcessStatus.SCHEDULED)
            assert (durable[9].status_property.sky_down_status ==
                    expected_durable_status)
            assert durable[9].status_property.logical_retirement_committed
            assert not durable[9].is_ready
            remove.assert_not_called()

            # Once the retry deadline arrives, the durable commitment is
            # detached from the obsolete selection epoch and a new,
            # idempotent cleanup worker is installed. It is admitted on the
            # next tick without ever making the backend READY again.
            clock[0] = 160
            manager._refresh_thread_pool()
            assert manager._down_thread_pool[9] is fresh_thread
            assert (durable[9].status_property.sky_down_status ==
                    common_utils.ProcessStatus.SCHEDULED)
            assert (durable[9].status_property.logical_retirement_version
                    is None)
            assert not durable[9].is_ready
            assert manager._down_thread_pool[9] is fresh_thread
            assert (safe_thread_factory.call_args.kwargs['kwargs']
                    ['drain_deadline'] == 610)

            manager._refresh_thread_pool()

        fresh_thread.start.assert_called_once_with()
        assert (durable[9].status_property.sky_down_status ==
                common_utils.ProcessStatus.RUNNING)
        assert not durable[9].is_ready
        assert manager._down_thread_pool[9] is fresh_thread
        remove.assert_not_called()

    def test_log_sync_failure_does_not_block_cleanup(self):
        manager = _make_manager()
        manager._is_pool = False
        manager._resource_scope = None
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        info = self._info()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value='/tmp/replica.log'), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_launch_log_file_name',
                               return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers.os.path.exists',
                        return_value=True), \
             mock.patch('builtins.open', side_effect=OSError('disk error')), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch('sky.serve.replica_managers.thread_utils.SafeThread',
                        return_value=mock.Mock()) as safe_thread:
            manager._terminate_replica(1,
                                       sync_down_logs=True,
                                       replica_drain_delay_seconds=0)

        persist.assert_called_once_with(1, info)
        safe_thread.assert_called_once()
        assert 1 in manager._down_thread_pool
        assert (info.status_property.sky_down_status ==
                common_utils.ProcessStatus.SCHEDULED)


class TestZeroCostDemandProbeBudget:

    @staticmethod
    def _manager(zero_cost, active):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = zero_cost
        placer.active_locations.return_value = active
        manager._spot_placer = placer
        return manager

    @staticmethod
    def _info(location, *, ready=False, terminal=False):
        info = mock.Mock()
        info.is_ready = ready
        info.is_terminal = terminal
        info.get_spot_location.return_value = location
        return info

    def test_spills_after_each_active_shape_fills_probe_budget(self):
        zero_a = object()
        zero_b = object()
        paid = object()
        manager = self._manager([zero_a, zero_b], [zero_a, zero_b, paid])
        per_location = (
            replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION)
        infos = ([self._info(zero_a) for _ in range(per_location)] +
                 [self._info(zero_b) for _ in range(per_location)])

        with mock.patch(
                'sky.serve.replica_managers.spot_placer.'
                'locations_match_placement',
                side_effect=lambda a, b: a is b):
            assert manager._demand_should_skip_saturated_zero_cost(infos)
            assert not manager._demand_should_skip_saturated_zero_cost(
                infos[:-1])

    def test_ready_terminal_and_benched_rows_do_not_consume_budget(self):
        active_zero = object()
        benched_zero = object()
        paid = object()
        manager = self._manager([active_zero, benched_zero],
                                [active_zero, paid])
        per_location = (
            replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION)
        infos = [
            self._info(active_zero, ready=True),
            self._info(active_zero, terminal=True),
            *[self._info(benched_zero) for _ in range(per_location)],
            *[self._info(active_zero) for _ in range(per_location - 1)],
        ]

        with mock.patch(
                'sky.serve.replica_managers.spot_placer.'
                'locations_match_placement',
                side_effect=lambda a, b: a is b):
            assert not manager._demand_should_skip_saturated_zero_cost(infos)
            infos.append(self._info(active_zero))
            assert manager._demand_should_skip_saturated_zero_cost(infos)

    @staticmethod
    def _location(cloud, region, gpu, *, use_spot, count=1):
        return replica_managers.spot_placer.Location.from_pickleable({
            'cloud': cloud,
            'region': region,
            'zone': None,
            'accelerators': {
                gpu: count
            },
            'use_spot': use_spot,
        })

    @staticmethod
    def _observations(values):
        return {
            key: replica_managers.reserved_capacity.FreeGpuObservation(
                value, 100.0
                if value is not None else None) for key, value in values.items()
        }

    def test_large_batch_uses_all_223_measured_gpus_then_spills(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        manager._spot_placer.select_next_zero_cost_location.return_value = zero

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 223
                               })) as query:
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 223}
        for _ in range(27):
            assert manager._select_budgeted_zero_cost_location(budget) == zero
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 7}
        assert manager._select_budgeted_zero_cost_location(budget) is None
        query.assert_called_once_with([zero])

    def test_pending_rows_are_debited_from_measured_free_slots(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        pending = [self._info(zero) for _ in range(3)]
        for info in pending:
            info.status = replica_managers.serve_state.ReplicaStatus.PENDING

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 223
                               })):
            budget = manager._build_zero_cost_demand_budget(
                pending, [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 220}

    def test_measured_gpu_budget_debits_complete_backend_widths(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        manager._spot_placer.select_next_zero_cost_location.return_value = zero

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 23
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        assert manager._select_budgeted_zero_cost_location(budget) == zero
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 15}
        assert manager._select_budgeted_zero_cost_location(budget) == zero
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 7}
        assert manager._select_budgeted_zero_cost_location(budget) is None

    def test_pending_multi_gpu_rows_debit_measured_gpu_slots(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        pending = [self._info(zero) for _ in range(2)]
        for info in pending:
            info.status = replica_managers.serve_state.ReplicaStatus.PENDING
            info.planned_capacity = 8

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 23
                               })):
            budget = manager._build_zero_cost_demand_budget(
                pending, [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 7}

    def test_pending_rows_from_peer_service_share_the_same_gpu_budget(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        peer_pending = self._info(zero)
        peer_pending.status = replica_managers.serve_state.ReplicaStatus.PENDING
        peer_pending.planned_capacity = 8

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 223
                               })):
            budget = manager._build_zero_cost_demand_budget(
                [], [None] * 500, capacity_replica_infos=[peer_pending])

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 215}

    def test_peer_becoming_ready_after_snapshot_is_still_debited(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        peer = self._info(zero, ready=True)
        peer.created_at = 50.0
        peer.planned_capacity = 8
        peer.status_property.first_ready_time = 101.0

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 223
                               })):
            budget = manager._build_zero_cost_demand_budget(
                [], [None] * 500, capacity_replica_infos=[peer])

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 215}

    def test_blackout_budget_remains_bounded_in_backend_attempts(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        manager._spot_placer.select_next_zero_cost_location.return_value = zero

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): None
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        attempts = replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION
        for _ in range(attempts):
            assert manager._select_budgeted_zero_cost_location(budget) == zero
        assert manager._select_budgeted_zero_cost_location(budget) is None

    def test_measurement_blackout_falls_back_to_probe_budget(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        unresolved = [self._info(zero) for _ in range(2)]
        for info in unresolved:
            info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): None
                               })):
            budget = manager._build_zero_cost_demand_budget(
                unresolved, [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 2}

    def test_mixed_measured_and_blackout_contexts_budget_independently(self):
        # One context measures successfully (measured slots minus PENDING
        # debits); the other fails (None) and falls back to the bounded
        # per-location probe allowance minus unresolved rows. The two
        # budgets are computed independently in the same snapshot.
        zero_a = self._location('Kubernetes', 'ctx-a', 'A100', use_spot=False)
        zero_b = self._location('Kubernetes', 'ctx-b', 'A100', use_spot=False)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero_a, zero_b], [zero_a, zero_b, paid])
        pending_a = [self._info(zero_a) for _ in range(2)]
        for info in pending_a:
            info.status = replica_managers.serve_state.ReplicaStatus.PENDING
        unresolved_b = [self._info(zero_b)]
        for info in unresolved_b:
            info.status = (
                replica_managers.serve_state.ReplicaStatus.PROVISIONING)

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('ctx-a', 'a100'): 7,
                                   ('ctx-b', 'a100'): None
                               })):
            budget = manager._build_zero_cost_demand_budget(
                pending_a + unresolved_b, [None] * 500)

        assert budget is not None
        per_location = (
            replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION)
        assert budget.remaining_by_pool == {
            ('ctx-a', 'a100'): 5,
            ('ctx-b', 'a100'): per_location - 1,
        }
        assert budget.measured_by_pool == {
            ('ctx-a', 'a100'): 7,
            ('ctx-b', 'a100'): None,
        }

    def test_accelerators_in_same_context_have_independent_gpu_budgets(self):
        a100 = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        h100 = self._location('Kubernetes',
                              'research-ctx',
                              'H100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([a100, h100], [a100, h100, paid])

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 223,
                                   ('research-ctx', 'h100'): 16,
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {
            ('research-ctx', 'a100'): 223,
            ('research-ctx', 'h100'): 16,
        }

    def test_targeted_zero_cost_selection_keeps_a100_variants_exact(self):
        a100 = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False)
        a100_80gb = self._location('Kubernetes',
                                   'research-ctx',
                                   'A100-80GB',
                                   use_spot=False)
        manager = self._manager([a100, a100_80gb], [a100, a100_80gb])
        manager._spot_placer.select_next_zero_cost_location.side_effect = (
            lambda *, allowed_locations: next(iter(allowed_locations)))
        budget = replica_managers._ZeroCostDemandBudget(
            remaining_by_pool={
                ('research-ctx', 'a100'): 1,
                ('research-ctx', 'a100-80gb'): 1,
            },
            measured_by_pool={
                ('research-ctx', 'a100'): 1,
                ('research-ctx', 'a100-80gb'): 1,
            })

        selected = manager._select_budgeted_zero_cost_location(
            budget, {a100_80gb})

        assert selected == a100_80gb
        assert budget.remaining_by_pool == {
            ('research-ctx', 'a100'): 1,
            ('research-ctx', 'a100-80gb'): 0,
        }

    def test_successful_zero_snapshot_does_not_speculate(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 0
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 0}
        assert manager._select_budgeted_zero_cost_location(budget) is None

    def test_zero_cost_only_pool_still_builds_authoritative_budget(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        manager = self._manager([zero], [zero])

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 0
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 0}


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
                    prior_reserved_fill=False,
                    recovering_existing_replica=False,
                    **_kwargs):
            del resources_override, existing_replica_infos
            del prior_reserved_fill
            assert recovering_existing_replica
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

    def test_newer_pending_version_stops_stale_recovery_wave(self):
        mgr = _make_manager(next_replica_id=1)
        launched = []
        infos = [
            _fake_replica_info(
                i,
                status=replica_managers.serve_state.ReplicaStatus.PROVISIONING)
            for i in (1, 2, 3)
        ]
        for info in infos:
            info.version = 1
            info.resources_override = None

        def _launch(replica_id, **_kwargs):
            launched.append(replica_id)
            mgr.notify_version_pending(2)

        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr._recover_replica_operations()

        assert launched == [1]

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
        seen_recovery_modes = []

        def _launch(_replica_id,
                    existing_replica_infos=None,
                    recovering_existing_replica=False,
                    **_kwargs):
            seen_snapshots.append(existing_replica_infos)
            seen_recovery_modes.append(recovering_existing_replica)

        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr._recover_replica_operations()
        assert seen_snapshots == [infos]
        assert seen_snapshots[0] is infos
        assert seen_recovery_modes == [True]


class TestRefreshThreadPoolUnfencedLaunch:
    """`_refresh_thread_pool` must convert an unfenced external-LB launch
    failure into one unrecoverable replica and keep that control-plane
    failure out of spot-placement evidence.

    This guards the manager-side half of the fix in PR #524: the client-side
    pre-check raises `_UnfencedExternalLbLaunchError`, but only this pass turns
    it into `user_app_failed` (so the autoscaler stops appending rows) and
    excludes it from `failed_spot_locations` / `failed_spot_availability` (so a
    missing owner fence does not bench an otherwise-usable location). A generic
    launch failure must still behave the old way.
    """

    def _run(self, thread_exception):
        replica_id = 7
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._is_pool = False
        manager.lock = threading.Lock()
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()

        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = False
        launch_thread.format_exc = 'boom traceback'
        launch_thread.exception = thread_exception
        manager._launch_thread_pool[replica_id] = launch_thread
        manager._replica_to_request_id[replica_id] = 'req'

        location = mock.Mock(name='location')
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        manager._spot_placer = placer

        info = mock.Mock()
        info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        info.status_property = replica_managers.ReplicaStatusProperty()
        info.get_spot_location.return_value = location
        info.created_at = 100.0

        persisted = []
        terminated = []
        with mock.patch.object(
                manager, '_reconcile_legacy_uncertain_logical_retirements'), \
             mock.patch.object(
                 manager, '_reconcile_recovering_logical_retirements'), \
             mock.patch.object(manager, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 manager, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(
                 manager, '_persist_replica',
                 side_effect=lambda rid, i: persisted.append((rid, i))), \
             mock.patch.object(
                 manager, '_terminate_replica',
                 side_effect=lambda rid, **_k: terminated.append(rid)), \
             mock.patch.object(manager, '_reconcile_failed_cleanup'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos',
                 return_value=[]), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state'
                 '.get_replica_infos_from_ids',
                 return_value={replica_id: info}):
            manager._refresh_thread_pool()

        return info, placer, terminated

    def test_unfenced_failure_is_unrecoverable_and_not_benched(self):
        info, placer, terminated = self._run(
            replica_managers._UnfencedExternalLbLaunchError('no fence'))
        # Unrecoverable so the autoscaler stops recreating replica rows.
        assert info.status_property.user_app_failed is True
        assert info.status_property.unrecoverable_failure() is True
        # A missing owner fence is a control-plane failure, not a location
        # problem: the location must not be benched.
        placer.set_preemptive.assert_not_called()
        assert info.status_property.failed_spot_availability is False
        assert terminated == [7]

    def test_generic_failure_still_benches_location_and_stays_recoverable(self):
        info, placer, terminated = self._run(RuntimeError('transient'))
        # An ordinary launch failure keeps the historical behavior: the
        # location is benched and the replica remains recoverable.
        assert info.status_property.user_app_failed is False
        placer.set_preemptive.assert_called_once_with(mock.ANY)
        assert info.status_property.failed_spot_availability is True
        assert terminated == [7]
