"""Grace-anchor regression tests for Kubernetes autodown reconciliation.

The reconciler in ``backend_utils`` falls back to the AUTOSTOPPING transition
timestamp when no durable Kubernetes Event is available. These tests drive the
real ``global_user_state`` event table instead of mocking the accessor, because
the defect they pin lives in the interaction between the event writer
(``add_cluster_event(nop_if_duplicate=True)``) and the anchor reader.
"""
# pylint: disable=protected-access
import time
from unittest import mock

from sky import backends
from sky import clouds
from sky import global_user_state
from sky.backends import backend_utils
from sky.skylet import constants
from sky.utils import status_lib
from sky.utils.db import db_utils

# Reasons emitted verbatim by ``_update_cluster_status``. The AUTOSTOPPING one
# is what ``_handle_autostopping_cluster`` writes; the UP one is what the
# health-probe branch writes when ``is_definitely_autostopping`` returns False.
_AUTODOWN_REASON = 'Cluster is autodowning.'
_UP_REASON = 'All nodes up; SkyPilot runtime healthy.'


def _fresh_db(tmp_path, monkeypatch):
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    monkeypatch.setattr(
        global_user_state,
        '_db_manager',
        db_utils.DatabaseManager(
            'state',
            global_user_state.create_table,
            post_init_fn=lambda _: global_user_state._sqlite_supports_returning(
            ),
        ),
    )


class _MinimalHandle:
    launched_resources = None


def _add_cluster(name: str) -> str:
    global_user_state.add_or_update_cluster(
        cluster_name=name,
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=False,
    )
    return global_user_state._get_hash_for_existing_cluster(name)


def _handle():
    handle = mock.Mock(spec=backends.CloudVmRayResourceHandle)
    handle.cluster_name = 'c1'
    handle.cluster_name_on_cloud = 'c1-abcd1234'
    handle.launched_nodes = 1
    handle.launched_resources = mock.Mock(unsafe=True)
    handle.launched_resources.cloud = clouds.Kubernetes()
    handle.launched_resources.hooks = None
    handle.launched_resources.use_spot = False
    handle.provision_runtime_metadata = mock.Mock(has_ray=False)
    return handle


def _record(cluster_hash: str, launched_at: int):
    return {
        'status': status_lib.ClusterStatus.AUTOSTOPPING,
        'autostop': 10,
        'to_down': True,
        'cluster_hash': cluster_hash,
        'launched_at': launched_at,
        'status_updated_at': int(time.time()),
    }


def _live_pods():
    return {'c1-abcd1234-head': (status_lib.ClusterStatus.UP, None)}


def _autodown_event(transitioned_at: int) -> None:
    """Write exactly what ``_handle_autostopping_cluster`` writes."""
    global_user_state.add_cluster_event(
        'c1',
        status_lib.ClusterStatus.AUTOSTOPPING,
        _AUTODOWN_REASON,
        global_user_state.ClusterEventType.STATUS_CHANGE,
        nop_if_duplicate=True,
        transitioned_at=transitioned_at,
    )


def _up_probe_blip_event(transitioned_at: int) -> None:
    """Write what the health-probe branch writes on a non-definitive probe."""
    global_user_state.add_cluster_event(
        'c1',
        status_lib.ClusterStatus.UP,
        _UP_REASON,
        global_user_state.ClusterEventType.STATUS_CHANGE,
        nop_if_duplicate=True,
        transitioned_at=transitioned_at,
    )


def _reconcile(handle, record, now: float) -> bool:
    """Run the reconciler with the durable-Event fast path unavailable."""
    with mock.patch.object(backend_utils.k8s_instance,
                           'get_cluster_autostop_event',
                           return_value=None), \
         mock.patch.object(backend_utils.time, 'time', return_value=now), \
         mock.patch.object(backend_utils.provision_lib,
                           'terminate_instances') as terminate:
        reconciled = backend_utils._maybe_reconcile_stalled_kubernetes_autodown(
            handle, record, _live_pods(), lambda: {'provider': {}})
    assert reconciled == (terminate.call_count == 1), (reconciled,
                                                       terminate.call_count)
    return reconciled


def test_probe_blip_does_not_suppress_a_second_autostopping_row(
        tmp_path, monkeypatch):
    """The duplicate guard only compares against the *previous* event.

    An interleaved UP row therefore lets a second AUTOSTOPPING row through,
    which is the mechanism that can move the reconciliation anchor.
    """
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c1')
    t0 = int(time.time()) - 10_000

    _autodown_event(t0)
    # Same reason back-to-back: suppressed, anchor cannot move.
    _autodown_event(t0 + 60)
    assert global_user_state.get_last_status_change_times(
        {cluster_hash}, status_lib.ClusterStatus.AUTOSTOPPING) == {
            cluster_hash: t0
        }

    # A probe blip writes an UP row, breaking the back-to-back comparison.
    _up_probe_blip_event(t0 + 120)
    _autodown_event(t0 + 180)
    assert global_user_state.get_last_status_change_times(
        {cluster_hash}, status_lib.ClusterStatus.AUTOSTOPPING) == {
            cluster_hash: t0 + 180
        }


def test_grace_anchors_on_the_original_transition_despite_probe_blips(
        tmp_path, monkeypatch):
    """A stalled autodown must still be reconciled after a probe blip.

    ``is_definitely_autostopping`` documents that it returns False on transient
    transport errors. Such a blip flips the cluster to UP for one sweep and
    re-enters AUTOSTOPPING with a fresh timestamp. The hook-aware grace must
    keep measuring from the original transition, otherwise the stalled autodown
    this reconciler exists to finish is never re-driven and the pods leak.
    """
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c1')
    handle = _handle()
    launched_at = int(time.time()) - 100_000
    grace = backend_utils._kubernetes_autodown_reconciliation_grace_seconds(
        handle)

    t0 = launched_at + 1_000
    _autodown_event(t0)
    # One transient probe failure, then the cluster is seen autodowning again.
    _up_probe_blip_event(t0 + 60)
    _autodown_event(t0 + 120)

    record = _record(cluster_hash, launched_at)
    # Well past the grace measured from the original transition.
    assert _reconcile(handle, record, now=t0 + grace + 1)


def test_repeated_probe_blips_do_not_defer_reconciliation_forever(
        tmp_path, monkeypatch):
    """Sequential adversarial cadence: a blip arriving faster than the grace.

    The status sweep runs about once a minute. One blip per hour is enough to
    keep re-anchoring a grace of just over an hour, so the reconciler would
    never fire for the entire life of the cluster.
    """
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c1')
    handle = _handle()
    launched_at = int(time.time()) - 200_000
    grace = backend_utils._kubernetes_autodown_reconciliation_grace_seconds(
        handle)

    t0 = launched_at + 1_000
    _autodown_event(t0)
    now = t0
    # Blip every (grace - 60)s for a full day of wall clock.
    step = grace - 60
    for i in range(1, int(86_400 // step) + 1):
        now = t0 + i * step
        _up_probe_blip_event(now)
        _autodown_event(now + 1)

    record = _record(cluster_hash, launched_at)
    assert _reconcile(handle, record, now=now + 2)


def test_transitions_from_a_previous_launch_do_not_shorten_the_grace(
        tmp_path, monkeypatch):
    """Safety direction: never terminate ahead of the hook-aware grace.

    ``launched_at`` is rewritten by every launch, including ``sky start``, so
    AUTOSTOPPING rows older than it belong to a finished cycle on a different
    machine boot. Anchoring on one of those would terminate a freshly
    autodowning cluster on the first sweep, killing in-flight down hooks.
    """
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c1')
    handle = _handle()
    grace = backend_utils._kubernetes_autodown_reconciliation_grace_seconds(
        handle)

    now = int(time.time())
    # A completed autostop cycle from a previous generation, long expired.
    _autodown_event(now - 10 * grace)
    _up_probe_blip_event(now - 9 * grace)
    # The cluster was stopped and started again; launch rewrites launched_at.
    launched_at = now - 120
    _autodown_event(now - 60)

    record = _record(cluster_hash, launched_at)
    assert not _reconcile(handle, record, now=now)


def test_fresh_transition_still_waits_for_the_grace(tmp_path, monkeypatch):
    """The grace itself must survive: no anchor change may short-circuit it."""
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c1')
    handle = _handle()
    launched_at = int(time.time()) - 10_000
    grace = backend_utils._kubernetes_autodown_reconciliation_grace_seconds(
        handle)

    t0 = launched_at + 1_000
    _autodown_event(t0)

    record = _record(cluster_hash, launched_at)
    assert not _reconcile(handle, record, now=t0 + grace - 1)
    assert _reconcile(handle, record, now=t0 + grace)


def test_no_autostopping_row_leaves_the_cluster_alone(tmp_path, monkeypatch):
    """No anchor at all must stay conservative rather than terminate."""
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c1')
    handle = _handle()
    launched_at = int(time.time()) - 10_000

    _up_probe_blip_event(launched_at + 10)

    record = _record(cluster_hash, launched_at)
    assert not _reconcile(handle, record, now=launched_at + 100_000)
