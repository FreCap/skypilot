"""Pruning failed replica records the service has already moved past.

`_handle_sky_down_finish` refuses to keep a failed record for a version
mismatch, but decides only once, as that replica's teardown finishes. A
replica that failed while its version was still the latest is retained
forever and never re-examined, so records pile up across every later version
and bury the current version's real failures.
"""
# pylint: disable=protected-access
import types

import pytest

from sky.serve import replica_managers
from sky.serve import serve_state
from sky.utils import common_utils

Status = serve_state.ReplicaStatus
Process = common_utils.ProcessStatus


def _info(replica_id, version, status, down_status=Process.SUCCEEDED):
    return types.SimpleNamespace(
        replica_id=replica_id,
        replica_record_id=f'record-{replica_id}',
        version=version,
        status=status,
        status_property=types.SimpleNamespace(sky_down_status=down_status),
    )


@pytest.fixture
def manager(monkeypatch):
    """A bare manager exposing only what the prune touches."""
    instance = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    instance._service_name = 'boltz-l4-fleet'
    instance.latest_version = 56
    instance._superseded_prune_pending = True
    removed = []
    instance._remove_replica = (
        lambda replica_id, record_id: removed.append(replica_id))
    instance.removed = removed
    return instance


def _run(manager, monkeypatch, infos):
    monkeypatch.setattr(serve_state, 'get_replica_infos', lambda name: infos)
    manager._prune_superseded_failed_replicas()
    return manager.removed


@pytest.mark.parametrize('status', [
    Status.FAILED,
    Status.FAILED_INITIAL_DELAY,
    Status.FAILED_PROBING,
    Status.FAILED_PROVISION,
])
def test_superseded_failed_records_are_pruned(manager, monkeypatch, status):
    """The 419-row backlog: failures from versions the service left behind."""
    assert _run(manager, monkeypatch, [_info(1, 40, status)]) == [1]


def test_current_version_failures_are_kept(manager, monkeypatch):
    """The operator must still see why the version they run is failing."""
    assert _run(manager, monkeypatch,
                [_info(1, 56, Status.FAILED_PROVISION)]) == []


@pytest.mark.parametrize('status', [Status.FAILED_CLEANUP, Status.UNKNOWN])
def test_unresolved_cleanup_rows_are_never_pruned(manager, monkeypatch, status):
    """These are exactly the rows the provider fences retain on purpose."""
    assert _run(manager, monkeypatch, [_info(1, 40, status)]) == []


@pytest.mark.parametrize('down_status', [
    None,
    Process.FAILED,
    Process.RUNNING,
    Process.SCHEDULED,
])
def test_rows_without_a_succeeded_teardown_are_kept(manager, monkeypatch,
                                                    down_status):
    """Only a teardown that SUCCEEDED proves the row holds no capacity."""
    assert _run(manager, monkeypatch,
                [_info(1, 40, Status.FAILED_PROVISION, down_status)]) == []


def test_healthy_replicas_are_never_pruned(manager, monkeypatch):
    """An old-version replica still serving traffic must survive."""
    assert _run(manager, monkeypatch,
                [_info(1, 40, Status.READY, Process.SUCCEEDED)]) == []


def test_only_the_superseded_failures_are_removed(manager, monkeypatch):
    """One pass over a realistic mix keeps every row that still matters."""
    infos = [
        _info(1, 14, Status.FAILED_PROVISION),
        _info(2, 40, Status.FAILED_INITIAL_DELAY),
        _info(3, 55, Status.FAILED_PROBING),
        _info(4, 56, Status.FAILED_PROVISION),
        _info(5, 40, Status.UNKNOWN),
        _info(6, 40, Status.FAILED_PROVISION, Process.FAILED),
        _info(7, 56, Status.READY),
        _info(8, 40, Status.READY),
    ]
    assert _run(manager, monkeypatch, infos) == [1, 2, 3]


def test_the_sweep_does_not_rescan_on_every_tick(manager, monkeypatch):
    """The refresher's per-tick scan budget must stay untouched when idle."""
    scans = {'n': 0}

    def _count(name):
        scans['n'] += 1
        return [_info(1, 40, Status.FAILED_PROVISION)]

    monkeypatch.setattr(serve_state, 'get_replica_infos', _count)
    manager._prune_superseded_failed_replicas()
    manager._prune_superseded_failed_replicas()
    manager._prune_superseded_failed_replicas()
    assert scans['n'] == 1
    assert manager.removed == [1]


def test_a_version_transition_rearms_the_sweep(manager, monkeypatch):
    """Records superseded by the new version must be collected next tick."""
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda name: [_info(1, 40, Status.FAILED_PROVISION)])
    manager._prune_superseded_failed_replicas()
    assert manager.removed == [1]

    replica_managers.SkyPilotReplicaManager._transition_status_epoch_for_version(
        manager, 57, replica_managers.serve_utils.UpdateMode.ROLLING)
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda name: [_info(2, 56, Status.FAILED_PROVISION)])
    manager._prune_superseded_failed_replicas()
    assert manager.removed == [1, 2]
