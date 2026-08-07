"""Regression tests for the SkyServe control-loop launch-budget scan cost.

Before the fix, ``ReplicaManager._refresh_thread_pool`` evaluated the launch
budget by calling ``controller_utils.can_provision`` / ``can_terminate`` once
*per* launching/terminating replica, and each call scanned and unpickled the
ENTIRE replica table. That is O(K*N) ``pickle.loads`` per refresh tick
(measured ~1.7s at N=2000, K=140; grows with fleet size), burning the refresh
loop's CPU budget on bookkeeping instead of starting launches and drains.

The fix hoists the budget read ONCE per tick via
``controller_utils.in_flight_launch_count`` and tracks the delta locally, so the
predicate accepts a pre-computed ``in_flight`` and does not re-scan. The two
counts needed by that one read are also computed from one shared table scan.

These tests fail on the pre-fix code (the per-replica predicate has no
``in_flight`` parameter and the loop scans K times) and pass after it.
"""
# pylint: disable=protected-access,unnecessary-lambda
import threading
from unittest import mock

from sky.serve import replica_managers
from sky.serve import serve_state
from sky.utils import common_utils
from sky.utils import controller_utils


class _NotStartedThread:
    """Stands in for a queued-but-not-started launch SafeThread."""
    format_exc = None
    exception = None

    def __init__(self):
        self.started = False

    def is_alive(self) -> bool:
        return self.started

    def start(self) -> None:
        self.started = True

    def complete(self) -> None:
        self.started = False


def _pending_replica(replica_id: int) -> replica_managers.ReplicaInfo:
    # Default ReplicaStatusProperty -> sky_launch_status SCHEDULED -> PENDING.
    return replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'c{replica_id}',
                                        replica_port='8080',
                                        is_spot=True,
                                        location=None,
                                        version=1,
                                        resources_override=None)


def _existing_replica_writer(replicas):

    def _write(_service_name, replica_id, info, *, expected_replica_exists,
               **_fence_kwargs):
        assert expected_replica_exists is True
        replicas[replica_id] = info
        return True

    return _write


def _build_manager(num_launching: int):
    # Bypass __init__: it spawns the refresher/prober/job-status daemon threads.
    mgr = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    mgr.lock = threading.Lock()
    mgr._service_name = 'svc'
    mgr.latest_version = 1
    mgr._is_pool = False
    mgr._spot_placer = None
    mgr._launch_thread_pool = {
        rid: _NotStartedThread() for rid in range(num_launching)
    }
    mgr._down_thread_pool = {}
    mgr._replica_to_request_id = {}
    mgr._replica_to_launch_cancelled = {}
    return mgr


def test_refresh_thread_pool_scans_budget_once_per_tick(monkeypatch, tmp_path):
    """With K launching replicas, the budget table is scanned O(1), not O(K)."""
    num_launching = 50
    replicas = {rid: _pending_replica(rid) for rid in range(num_launching)}
    scans = {'budget': 0}

    def _count_budget():
        scans['budget'] += 1
        provisioning = sum(
            1 for info in replicas.values()
            if info.status == serve_state.ReplicaStatus.PROVISIONING)
        return provisioning, 0

    monkeypatch.setattr(serve_state, 'get_replica_launch_budget_counts',
                        _count_budget)
    monkeypatch.setattr(serve_state, 'get_replica_infos_from_ids',
                        lambda svc, rids: {rid: replicas[rid] for rid in rids})
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas.values()))
    monkeypatch.setattr(serve_state, 'add_or_update_replica',
                        _existing_replica_writer(replicas))
    # Ample budget so every pending replica is allowed to launch; also avoids
    # the memory-probing path inside _get_request_parallelism.
    monkeypatch.setattr(controller_utils, '_get_request_parallelism',
                        lambda pool: 10_000)
    monkeypatch.setattr(controller_utils, 'get_resources_lock_path',
                        lambda: str(tmp_path / 'resources.lock'))

    mgr = _build_manager(num_launching)
    mgr._refresh_thread_pool()

    # Correctness preserved: every pending replica got launched...
    assert all(t.started for t in mgr._launch_thread_pool.values())
    assert all(info.status_property.sky_launch_status ==
               common_utils.ProcessStatus.RUNNING for info in replicas.values())
    # ...and the whole-table budget scan happened at most ONCE for the tick,
    # not once per launching replica (the O(K*N) bug -> would be num_launching).
    assert scans['budget'] <= 1, (
        f'budget table scanned {scans["budget"]}x for {num_launching} '
        'launching replicas; expected a single hoisted scan per tick')


def test_down_admission_is_capped_and_uses_hoisted_budget(
        monkeypatch, tmp_path):
    """One service caps live down workers and backfills without rescanning."""
    num_terminating = 7
    per_service_cap = 3
    replicas = {}
    for rid in range(num_terminating):
        info = _pending_replica(rid)
        info.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        replicas[rid] = info
    scans = {'n': 0}

    def _scan():
        scans['n'] += 1
        return 0, 0

    monkeypatch.setattr(serve_state, 'get_replica_launch_budget_counts', _scan)
    monkeypatch.setattr(serve_state, 'get_replica_infos_from_ids',
                        lambda svc, rids: {rid: replicas[rid] for rid in rids})
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas.values()))
    monkeypatch.setattr(serve_state, 'add_or_update_replica',
                        _existing_replica_writer(replicas))
    monkeypatch.setattr(controller_utils, '_get_request_parallelism',
                        lambda pool: 10_000)
    monkeypatch.setattr(replica_managers, '_MAX_CONCURRENT_DOWNS_PER_SERVICE',
                        per_service_cap)
    monkeypatch.setattr(controller_utils, 'get_resources_lock_path',
                        lambda: str(tmp_path / 'resources.lock'))

    mgr = _build_manager(num_launching=0)
    mgr._down_thread_pool = {
        rid: _NotStartedThread() for rid in range(num_terminating)
    }
    mgr._refresh_thread_pool()

    assert sum(t.started for t in mgr._down_thread_pool.values()) == 3
    assert [
        replicas[rid].status_property.sky_down_status
        for rid in range(num_terminating)
    ] == ([common_utils.ProcessStatus.RUNNING] * 3 +
          [common_utils.ProcessStatus.SCHEDULED] * 4)
    assert scans['n'] <= 1

    # A completed worker is handled and one queued row fills the freed local
    # slot on the next refresh. The other two live workers count against the
    # cap even though the global budget remains effectively unlimited.
    mgr._down_thread_pool[0].complete()
    mgr._handle_sky_down_finish = lambda info, format_exc: None
    mgr._refresh_thread_pool()

    assert 0 not in mgr._down_thread_pool
    assert sum(t.started for t in mgr._down_thread_pool.values()) == 3
    assert [
        replicas[rid].status_property.sky_down_status
        for rid in range(1, num_terminating)
    ] == ([common_utils.ProcessStatus.RUNNING] * 3 +
          [common_utils.ProcessStatus.SCHEDULED] * 3)
    assert scans['n'] <= 2


def test_refresh_thread_pool_batches_replica_row_reads(monkeypatch, tmp_path):
    """Finished launch/down threads hydrate with one batch read per pool.

    Before the fix, every finished (or still-PENDING queued) thread issued its
    own ``get_replica_info_from_id`` DB read on every tick, so K queued
    launches cost K reads per tick until admitted.
    """
    num_launching = 25
    replicas = {rid: _pending_replica(rid) for rid in range(num_launching)}
    reads = {'batch': 0}

    def _batch(svc, rids):
        del svc
        reads['batch'] += 1
        return {rid: replicas[rid] for rid in rids}

    def _single(svc, rid):
        raise AssertionError(
            f'per-replica DB read for {svc}/{rid} in refresh tick')

    monkeypatch.setattr(serve_state, 'get_replica_infos_from_ids', _batch)
    monkeypatch.setattr(serve_state, 'get_replica_info_from_id', _single)
    monkeypatch.setattr(serve_state, 'get_replica_launch_budget_counts', lambda:
                        (0, 0))
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas.values()))
    monkeypatch.setattr(serve_state, 'add_or_update_replica',
                        _existing_replica_writer(replicas))
    monkeypatch.setattr(controller_utils, '_get_request_parallelism',
                        lambda pool: 10_000)
    monkeypatch.setattr(controller_utils, 'get_resources_lock_path',
                        lambda: str(tmp_path / 'resources.lock'))

    mgr = _build_manager(num_launching)
    mgr._refresh_thread_pool()

    # Correctness preserved: every pending replica got launched...
    assert all(t.started for t in mgr._launch_thread_pool.values())
    # ...via one batch hydration per pool (launch + down), not one read per
    # queued replica per tick.
    assert reads['batch'] <= 2, (
        f'{reads["batch"]} replica-row reads for {num_launching} queued '
        'launches; expected at most one batch read per thread pool')


def test_stale_completed_launch_does_not_block_worker_reconciliation(
        monkeypatch):
    """A missing launch row is discarded before spot evidence is read."""
    stale_replica_id = 1
    live_replica_id = 2
    down_replica_id = 3
    live_info = _pending_replica(live_replica_id)
    live_info.status_property.sky_launch_status = (
        common_utils.ProcessStatus.RUNNING)
    down_info = _pending_replica(down_replica_id)
    down_info.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    down_info.status_property.sky_down_status = (
        common_utils.ProcessStatus.RUNNING)
    location = mock.sentinel.location
    live_info.get_spot_location = mock.Mock(return_value=location)

    mgr = _build_manager(num_launching=0)
    mgr._launch_thread_pool = {
        stale_replica_id: _NotStartedThread(),
        live_replica_id: _NotStartedThread(),
    }
    mgr._down_thread_pool = {down_replica_id: _NotStartedThread()}
    mgr._replica_to_request_id = {
        stale_replica_id: 'stale-request',
        live_replica_id: 'live-request',
    }
    mgr._replica_to_launch_cancelled = {stale_replica_id: False}
    mgr._spot_placer = mock.Mock()
    mgr._spot_placer.resolve_location.return_value = location
    infos = {
        live_replica_id: live_info,
        down_replica_id: down_info,
    }

    def _batch(_service_name, replica_ids):
        return {
            replica_id: infos[replica_id]
            for replica_id in replica_ids
            if replica_id in infos
        }

    persisted = []
    finished_downs = []
    monkeypatch.setattr(serve_state, 'get_replica_infos_from_ids', _batch)
    monkeypatch.setattr(serve_state, 'get_replica_infos', lambda _svc: [])
    monkeypatch.setattr(mgr, '_persist_replicas',
                        lambda updates: persisted.extend(updates))
    monkeypatch.setattr(
        mgr, '_handle_sky_down_finish',
        lambda info, format_exc: finished_downs.append(
            (info.replica_id, format_exc)))
    monkeypatch.setattr(mgr, '_reconcile_failed_cleanup', mock.Mock())

    mgr._refresh_thread_pool()

    assert stale_replica_id not in mgr._launch_thread_pool
    assert stale_replica_id not in mgr._replica_to_request_id
    assert stale_replica_id not in mgr._replica_to_launch_cancelled
    assert persisted == [(live_replica_id, live_info)]
    assert live_replica_id not in mgr._launch_thread_pool
    assert mgr._spot_placer.set_active.call_args_list == [
        mock.call(location, selected_at=live_info.created_at)
    ]
    assert finished_downs == [(down_replica_id, None)]
    assert down_replica_id not in mgr._down_thread_pool


def test_idle_tick_performs_no_budget_scan(monkeypatch):
    """A tick with nothing to admit must not scan the budget tables."""
    scans = {'n': 0}

    def _scan():
        scans['n'] += 1
        return 0, 0

    monkeypatch.setattr(serve_state, 'get_replica_launch_budget_counts', _scan)
    monkeypatch.setattr(serve_state, 'get_replica_infos', lambda svc: [])

    mgr = _build_manager(num_launching=0)
    mgr._refresh_thread_pool()

    assert scans['n'] == 0


def test_can_provision_with_precomputed_in_flight_skips_db_scan(monkeypatch):
    """can_provision/can_terminate must honor a pre-computed in_flight count
    without touching the (expensive) whole-table scan functions."""
    scanned = {'n': 0}

    def _boom():
        scanned['n'] += 1
        return 0, 0

    monkeypatch.setattr(serve_state, 'get_replica_launch_budget_counts', _boom)
    monkeypatch.setattr(controller_utils, '_get_request_parallelism',
                        lambda pool: 100)

    assert controller_utils.can_provision(False, in_flight=3) is True
    assert controller_utils.can_terminate(False, in_flight=3) is True
    assert scanned['n'] == 0

    # And when in_flight is NOT supplied it falls back to one combined scan.
    assert controller_utils.can_terminate(False) is True
    assert scanned['n'] == 1
