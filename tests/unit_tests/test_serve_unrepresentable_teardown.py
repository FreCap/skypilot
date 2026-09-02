"""Removing rows a succeeded teardown left in an unreportable state.

`to_replica_status` returns UNKNOWN for a replica whose teardown succeeded
but whose row still exists, annotating both arms with "should have been
cleaned from the replica table". `_handle_sky_down_finish` nonetheless kept
those rows whenever no other removal reason matched, stranding one per
interrupted teardown: no endpoint, no resources, and a status naming no
failure.
"""
# pylint: disable=protected-access
import types

import pytest

from sky.serve import replica_managers
from sky.serve import serve_state
from sky.utils import common_utils

Status = serve_state.ReplicaStatus
Process = common_utils.ProcessStatus


class _Manager:
    """A manager exposing only what the down-finish reducer touches."""

    latest_version = 57

    def __init__(self):
        self.removed = []
        self.persisted = []

    def _remove_replica(self,
                        replica_id,
                        record_id,
                        allow_active_provider_free_pre_job=False):
        del record_id, allow_active_provider_free_pre_job
        self.removed.append(replica_id)

    def _persist_replica(self, replica_id, info):
        self.persisted.append(replica_id)

    def _clear_failed_cleanup_retry(self, replica_id):
        pass


def _info(status,
          *,
          version=57,
          is_scale_down=False,
          purged=False,
          failed_spot=False):
    status_property = types.SimpleNamespace(
        is_scale_down=is_scale_down,
        preempted=False,
        purged=purged,
        failed_spot_availability=failed_spot,
        sky_launch_status=Process.FAILED,
        sky_down_status=None,
    )
    return types.SimpleNamespace(
        replica_id=39310,
        replica_record_id='record-39310',
        version=version,
        status=status,
        status_property=status_property,
        # The down-result projection reads the provider-free reserved-fill
        # markers before it looks at the status property (#1720).
        reserved_fill=False,
        zero_cost_materialization_sequence=None,
        service_job_id=None,
    )


def _finish(manager, info):
    replica_managers.SkyPilotReplicaManager._handle_sky_down_finish(
        manager, info, None)


def test_unknown_after_a_succeeded_teardown_is_removed():
    """The stranded row: teardown landed, nothing left to report."""
    manager = _Manager()
    _finish(manager, _info(Status.UNKNOWN))
    assert manager.removed == [39310]
    assert manager.persisted == []


def test_the_teardown_is_still_recorded_as_succeeded():
    manager = _Manager()
    info = _info(Status.UNKNOWN)
    _finish(manager, info)
    assert info.status_property.sky_down_status is Process.SUCCEEDED


@pytest.mark.parametrize('status', [
    Status.FAILED,
    Status.FAILED_PROVISION,
    Status.FAILED_INITIAL_DELAY,
    Status.FAILED_PROBING,
])
def test_real_failures_at_the_current_version_are_still_kept(status):
    """The operator must still see why the version they run is failing."""
    manager = _Manager()
    _finish(manager, _info(status))
    assert manager.removed == []
    assert manager.persisted == [39310]


def test_failed_cleanup_is_never_treated_as_unreportable():
    """An unresolved cleanup keeps its row; it may still hold capacity."""
    manager = _Manager()
    _finish(manager, _info(Status.FAILED_CLEANUP))
    assert manager.removed == []
    assert manager.persisted == [39310]


def test_scale_down_keeps_its_own_removal_reason():
    manager = _Manager()
    _finish(manager, _info(Status.UNKNOWN, is_scale_down=True))
    assert manager.removed == [39310]


def test_outdated_version_keeps_its_own_removal_reason():
    manager = _Manager()
    _finish(manager, _info(Status.UNKNOWN, version=40))
    assert manager.removed == [39310]
