"""Tests for sky/serve/replica_managers.py.

Currently focused on `SkyPilotReplicaManager.__init__` startup ordering:
the daemon threads (especially `_job_status_fetcher`) must NOT race the
main thread for `self.lock` before `_recover_replica_operations` runs.
"""
import threading
from unittest import mock

import pytest

from sky.serve import replica_managers


class TestSkyPilotReplicaManagerInitOrdering:
    """`SkyPilotReplicaManager.__init__` must run `_recover_replica_operations`
    BEFORE starting the `_job_status_fetcher` / `_thread_pool_refresher` /
    `_replica_prober` daemon threads.

    If the daemon threads start first, `_job_status_fetcher` will acquire
    `self.lock` (via the `@with_lock` decorator on `_fetch_job_status`)
    and perform a per-replica SSH/gRPC call to query job status. When a
    replica's head node is unreachable (pod / VM gone), each SSH connect
    hangs at the kernel TCP timeout (tens of seconds to minutes). The
    main thread then blocks on `_recover_replica_operations`'s
    `with self.lock:` for the full hang duration, never returns from
    `SkyPilotReplicaManager.__init__`, and `uvicorn.run` is never called.

    With HA recovery changes, `_wait_for_controller_ready`
    then times out (60s) → `_bail_on_boot_failure` → `os._exit(1)` →
    daemon retries → same race → infinite recovery loop.

    The fix: recovery first, daemon threads after.
    """

    def test_recover_called_before_threads_start(self):
        """Verify the call order: `_recover_replica_operations` first,
        then each daemon thread's `.start()`."""
        call_order = []

        def _record(name):

            def _fn(*_args, **_kwargs):
                call_order.append(name)

            return _fn

        # Patch the heavy deps so __init__ doesn't actually do work.
        # We only care about the call order.
        with mock.patch.object(
                replica_managers.ReplicaManager, '__init__',
                return_value=None), \
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
                 '_recover_replica_operations',
                 _record('recover')), \
             mock.patch(
                 'sky.serve.replica_managers.thread_utils'
                 '.start_supervised_thread') as mock_supervised:
            # start_supervised_thread starts the supervisor thread
            # immediately, so its call time IS the thread start time.
            def _start_supervised(target, *_args, **_kwargs):
                target_name = getattr(target, '__name__', repr(target))
                call_order.append(f'thread_start:{target_name}')
                return mock.Mock()

            mock_supervised.side_effect = _start_supervised

            spec = mock.MagicMock()
            replica_managers.SkyPilotReplicaManager(service_name='svc',
                                                    spec=spec,
                                                    version=1)

        # `recover` must come before any `thread_start:*` entry. The
        # daemon threads themselves may be created in any order relative
        # to each other (we don't constrain that), but ALL of them must
        # appear after `recover`.
        assert 'recover' in call_order, (
            f'_recover_replica_operations was never called; '
            f'call_order={call_order}')
        recover_idx = call_order.index('recover')
        for i, name in enumerate(call_order):
            if name.startswith('thread_start:'):
                assert i > recover_idx, (
                    f'{name} happened at index {i} before recover at '
                    f'index {recover_idx}; call_order={call_order}. '
                    f'Daemon threads must NOT start until '
                    f'_recover_replica_operations has finished — '
                    f'see the docstring of '
                    f'TestSkyPilotReplicaManagerInitOrdering.')

    def test_all_three_daemon_threads_are_started(self):
        """Sanity: regardless of ordering, the three control-loop threads
        (_thread_pool_refresher / _job_status_fetcher / _replica_prober)
        still all start (supervised)."""
        started_targets = []

        # The three control loops are launched via
        # thread_utils.start_supervised_thread(target, name), not
        # threading.Thread directly, so capture the supervised target's name
        # from there. Patching threading.Thread would only ever see the
        # supervisor wrapper (_supervise), not the real methods.
        with mock.patch.object(
                replica_managers.ReplicaManager, '__init__',
                return_value=None), \
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
                 '_recover_replica_operations'), \
             mock.patch(
                 'sky.serve.replica_managers.thread_utils.'
                 'start_supervised_thread') as mock_start:

            def _record(target, *_args, **_kwargs):
                started_targets.append(getattr(target, '__name__', None))
                return mock.Mock()

            mock_start.side_effect = _record

            spec = mock.MagicMock()
            replica_managers.SkyPilotReplicaManager(service_name='svc',
                                                    spec=spec,
                                                    version=1)

        # Bound methods on the instance — verify by name.
        assert '_thread_pool_refresher' in started_targets
        assert '_job_status_fetcher' in started_targets
        assert '_replica_prober' in started_targets


def _make_manager(service_name='svc', next_replica_id=1):
    """Build a bare SkyPilotReplicaManager with only the attributes the
    recovery / scale-up id-allocator paths touch, skipping the heavy
    __init__ (yaml parse, spot placer, daemon threads)."""
    mgr = object.__new__(replica_managers.SkyPilotReplicaManager)
    mgr.lock = threading.RLock()
    mgr._service_name = service_name
    mgr._next_replica_id = next_replica_id
    mgr._launch_thread_pool = {}
    mgr._down_thread_pool = {}
    mgr._tick_version_spec_cache = {}
    return mgr


def _fake_replica_info(replica_id):
    info = mock.Mock()
    info.replica_id = replica_id
    return info


def _record_launch(launched):
    """A _launch_replica side_effect that records the allocated replica id."""

    def _side_effect(replica_id, _resources_override):
        launched.append(replica_id)

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
                'sky.serve.replica_managers.serve_state.get_replicas_at_status',
                return_value=[]), \
             mock.patch(
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
                'sky.serve.replica_managers.serve_state.get_replicas_at_status',
                return_value=[]), \
             mock.patch(
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
