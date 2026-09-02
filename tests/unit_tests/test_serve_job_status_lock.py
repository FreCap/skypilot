"""Regression test: _fetch_exact_status must not hold self.lock during SSH.

``ReplicaManager._fetch_exact_status`` SSHes into every tracked replica's head
node to query job status. It used to be decorated ``@with_lock``, so the global
``self.lock`` was held across the entire serial SSH walk. When a replica is
unreachable (e.g. a preempted spot VM), its SSH connect hangs at the kernel TCP
timeout (tens of seconds to minutes) -- and during that hang the refresher /
prober / scaler (which all take ``self.lock``) are blocked, stalling autoscaling
exactly when the fleet is churning hardest.

The fix does the SSH walk WITHOUT the lock and re-takes it only on the
failure-handling paths. This test pins that: while a replica's
``get_job_status`` is blocked, ``self.lock`` must still be acquirable. It is
deterministic (uses Events, not sleeps).
"""
# pylint: disable=protected-access,unnecessary-lambda,unused-argument
# pylint: disable=use-implicit-booleaness-not-comparison
import copy
import threading
import types
from unittest import mock

import pytest

from sky import backends
from sky import clouds
from sky import exceptions
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.skylet import job_lib
from sky.utils import common_utils


def _tracked_replica(replica_id: int,
                     version: int = 1) -> replica_managers.ReplicaInfo:
    info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'c{replica_id}',
                                        replica_port='8080',
                                        is_spot=True,
                                        location=None,
                                        version=version,
                                        resources_override=None)
    # SUCCEEDED + no down -> should_track_service_status() is True.
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    return info


@pytest.fixture(autouse=True)
def _batched_cluster_records(monkeypatch):
    """The walk resolves handles from one batched cluster-record read."""
    monkeypatch.setattr(
        replica_managers.global_user_state, 'get_clusters_from_names',
        lambda names: {name: {
            'handle': object()
        } for name in names})

    def _prepare(handles, *, command_timeout_seconds):
        preparations = []
        for handle in handles:
            runner = mock.MagicMock(enable_interactive_auth=False)
            transport = backends.ServeJobStatusTransport(
                source_handle=handle,
                head_runner=runner,
                command_timeout_seconds=command_timeout_seconds)
            preparations.append(
                backends.ServeJobStatusTransportPreparation(
                    source_handle=handle, transport=transport))
        return preparations

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'build_serve_job_status_transports',
                        staticmethod(_prepare))


def _build_manager():
    mgr = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    mgr.lock = threading.Lock()
    mgr._service_name = 'svc'
    mgr._is_pool = False
    mgr.latest_version = 1
    return mgr


@pytest.mark.parametrize(
    ('cloud', 'expected'),
    [(clouds.Kubernetes(),
      backends.ServeReplicaJobStatusSource.PROVIDER_AND_ENDPOINT),
     (clouds.GCP(), backends.ServeReplicaJobStatusSource.
      STARTUP_SENTINEL_AND_PROVIDER_ENDPOINT)],
)
def test_backend_selects_one_ordinary_serve_liveness_source(cloud, expected):
    handle = backends.CloudVmRayResourceHandle.__new__(
        backends.CloudVmRayResourceHandle)
    handle.launched_resources = type('LaunchedResources', (),
                                     {'cloud': cloud})()

    assert (backends.CloudVmRayBackend().serve_replica_job_status_source(handle)
            is expected)


def test_backend_liveness_source_fails_closed_for_malformed_handle():
    backend = backends.CloudVmRayBackend()
    handle = backends.CloudVmRayResourceHandle.__new__(
        backends.CloudVmRayResourceHandle)

    # Missing launched_resources, a malformed resource object, and a missing
    # cloud must all preserve exact remote status polling instead of raising or
    # silently selecting the endpoint-only path.
    assert (backend.serve_replica_job_status_source(handle)
            is backends.ServeReplicaJobStatusSource.REMOTE_JOB)
    handle.launched_resources = object()
    assert (backend.serve_replica_job_status_source(handle)
            is backends.ServeReplicaJobStatusSource.REMOTE_JOB)
    handle.launched_resources = type('LaunchedResources', (), {'cloud': None})()
    assert (backend.serve_replica_job_status_source(handle)
            is backends.ServeReplicaJobStatusSource.REMOTE_JOB)


def _vm_handle(cluster_name: str):
    handle = backends.CloudVmRayResourceHandle.__new__(
        backends.CloudVmRayResourceHandle)
    handle.cluster_name = cluster_name
    handle.launched_resources = types.SimpleNamespace(cloud=clouds.GCP())
    return handle


def test_never_ready_vm_version_uses_one_exact_bound_startup_sentinel(
        monkeypatch):
    replicas = [_tracked_replica(replica_id) for replica_id in range(1, 4)]
    handles = {
        info.cluster_name: _vm_handle(info.cluster_name) for info in replicas
    }
    by_id = {info.replica_id: info for info in replicas}
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda _service: list(replicas))
    monkeypatch.setattr(serve_state, 'get_replica_info_from_id',
                        lambda _service, replica_id: by_id.get(replica_id))
    monkeypatch.setattr(
        replica_managers.global_user_state, 'get_clusters_from_names',
        lambda names: {name: {
            'handle': handles[name]
        } for name in names})
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, _record=None: handles[self.cluster_name])
    read_job_ids = mock.Mock(
        side_effect=lambda _service, identities: {identities[0]: 42})
    monkeypatch.setattr(replica_managers.request_postgres,
                        'read_bound_ordinary_launch_status_job_ids',
                        read_job_ids)
    fetched = []

    def _status(_self, _handle, job_ids, **_kwargs):
        fetched.append(job_ids)
        return {42: job_lib.JobStatus.RUNNING}

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _status)
    manager = _build_manager()
    manager._fetch_exact_status()

    assert fetched == [[42]]
    read_job_ids.assert_called_once()
    service_name, identities = read_job_ids.call_args.args
    assert service_name == 'svc'
    assert [(identity.replica_id, identity.replica_record_id)
            for identity in identities
           ] == [(info.replica_id, info.replica_record_id) for info in replicas]


def test_startup_sentinel_skips_bad_cached_handle_without_blinding_version(
        monkeypatch):
    replicas = [_tracked_replica(replica_id) for replica_id in range(1, 3)]
    handles = {
        info.cluster_name: _vm_handle(info.cluster_name) for info in replicas
    }
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda _service: list(replicas))
    monkeypatch.setattr(
        replica_managers.global_user_state, 'get_clusters_from_names',
        lambda names: {name: {
            'handle': handles[name]
        } for name in names})
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, _record=None: handles[self.cluster_name])
    read_job_ids = mock.Mock(
        side_effect=lambda _service, identities:
        {identity: 100 + identity.replica_id for identity in identities})
    monkeypatch.setattr(replica_managers.request_postgres,
                        'read_bound_ordinary_launch_status_job_ids',
                        read_job_ids)

    good_handle = handles[replicas[1].cluster_name]
    good_transport = backends.ServeJobStatusTransport(
        source_handle=good_handle,
        head_runner=mock.Mock(enable_interactive_auth=False),
        command_timeout_seconds=10)

    def _prepare(candidate_handles, *, command_timeout_seconds):
        assert command_timeout_seconds == 10
        assert candidate_handles == [
            handles[replicas[0].cluster_name], handles[replicas[1].cluster_name]
        ]
        return [
            backends.ServeJobStatusTransportPreparation(
                source_handle=candidate_handles[0],
                error=ValueError('corrupt retained connection state')),
            backends.ServeJobStatusTransportPreparation(
                source_handle=candidate_handles[1], transport=good_transport),
        ]

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'build_serve_job_status_transports',
                        staticmethod(_prepare))
    fetched = []

    def _status(_self, handle, job_ids, **kwargs):
        fetched.append((handle, job_ids, kwargs['serve_transport']))
        return {job_ids[0]: job_lib.JobStatus.RUNNING}

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _status)

    _build_manager()._fetch_exact_status()

    assert fetched == [(handles[replicas[1].cluster_name], [102],
                        good_transport)]
    read_job_ids.assert_called_once()


def test_ever_ready_vm_version_performs_zero_remote_status_reads(monkeypatch):
    replicas = [_tracked_replica(replica_id) for replica_id in range(800)]
    replicas[0].status_property.first_ready_time = 1.0
    handles = {
        info.cluster_name: _vm_handle(info.cluster_name) for info in replicas
    }
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda _service: list(replicas))
    monkeypatch.setattr(
        replica_managers.global_user_state, 'get_clusters_from_names',
        lambda names: {name: {
            'handle': handles[name]
        } for name in names})
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, _record=None: handles[self.cluster_name])
    status = mock.Mock(side_effect=AssertionError('unexpected remote status'))
    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', status)
    read_job_ids = mock.Mock()
    monkeypatch.setattr(replica_managers.request_postgres,
                        'read_bound_ordinary_launch_status_job_ids',
                        read_job_ids)

    _build_manager()._fetch_exact_status()

    status.assert_not_called()
    read_job_ids.assert_not_called()


def test_fetch_exact_status_samples_latest_version_first(monkeypatch):
    """The latest-version replica's result is consumed (acted on) first.

    The SSH fetches run in parallel, so no ordering is asserted on the
    fetches themselves -- only that every replica is fetched and that the
    failure-handling consumption starts with the latest-version replica,
    so a version-wide bad rollout is stopped without waiting behind every
    old replica.
    """
    old = [_tracked_replica(1), _tracked_replica(2)]
    latest = _tracked_replica(3, version=2)
    replicas = old + [latest]
    by_id = {info.replica_id: info for info in replicas}
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas))
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: self.replica_id)

    probed = []

    def _get_job_status(self, handle, job_ids, stream_logs=False, **_kwargs):
        probed.append(handle)
        return {1: job_lib.JobStatus.FAILED}

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _get_job_status)
    monkeypatch.setattr(serve_state, 'get_replica_info_from_id',
                        lambda svc, rid: by_id.get(rid))

    terminated = []
    mgr = _build_manager()
    mgr.latest_version = 2
    mgr._persist_replica = lambda rid, info: None
    mgr._terminate_replica = (
        lambda rid, replica_drain_delay_seconds: terminated.append(rid))
    mgr._fetch_exact_status()

    assert sorted(probed) == [1, 2, 3]
    assert terminated == [3, 1, 2]


def test_latest_startup_failure_reduces_before_old_status_finishes(monkeypatch):
    """A bad new version cannot wait behind a blocked old-version read."""
    old = _tracked_replica(1, version=1)
    latest = _tracked_replica(2, version=2)
    replicas = [old, latest]
    handles = {
        info.replica_id: _vm_handle(info.cluster_name) for info in replicas
    }
    by_id = {info.replica_id: info for info in replicas}
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda _service: list(replicas))
    monkeypatch.setattr(serve_state, 'get_replica_info_from_id',
                        lambda _service, replica_id: by_id.get(replica_id))
    monkeypatch.setattr(
        replica_managers.global_user_state, 'get_clusters_from_names',
        lambda names: {
            info.cluster_name: {
                'handle': handles[info.replica_id]
            } for info in replicas if info.cluster_name in names
        })
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, _record=None: handles[self.replica_id])
    monkeypatch.setattr(
        replica_managers.request_postgres,
        'read_bound_ordinary_launch_status_job_ids',
        lambda _service, identities:
        {identity: 100 + identity.replica_id for identity in identities})

    old_started = threading.Event()
    release_old = threading.Event()
    latest_terminated = threading.Event()

    def _status(_self, handle, job_ids, **_kwargs):
        if handle is handles[old.replica_id]:
            old_started.set()
            assert release_old.wait(timeout=5)
            return {job_ids[0]: job_lib.JobStatus.RUNNING}
        assert handle is handles[latest.replica_id]
        return {job_ids[0]: job_lib.JobStatus.FAILED}

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _status)
    manager = _build_manager()
    manager.latest_version = latest.version
    manager._persist_replica = lambda *_args, **_kwargs: None
    manager._terminate_replica = (
        lambda replica_id, **_kwargs: latest_terminated.set()
        if replica_id == latest.replica_id else None)

    fetch = threading.Thread(target=manager._fetch_exact_status)
    fetch.start()
    try:
        assert old_started.wait(timeout=5)
        assert latest_terminated.wait(timeout=2), (
            'latest-version failure waited behind blocked old status')
    finally:
        release_old.set()
        fetch.join(timeout=5)
        manager._shutdown_remote_io_executor()

    assert not fetch.is_alive()


def test_fetch_exact_status_walk_is_parallel(monkeypatch):
    """One hung replica must not serialize the whole fleet's SSH walk.

    Every replica's ``get_job_status`` blocks until all replicas have
    entered it. A serial walk deadlocks (the first SSH never returns while
    the others never start); the parallel walk proceeds. Deterministic:
    uses a barrier, not sleeps.
    """
    num_replicas = 3
    replicas = [_tracked_replica(i) for i in range(num_replicas)]
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas))
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: object())

    all_entered = threading.Barrier(num_replicas)

    transport_kwargs = []

    def _blocking_get_job_status(self,
                                 handle,
                                 job_ids,
                                 stream_logs=False,
                                 **kwargs):
        transport_kwargs.append(kwargs)
        # Times out (raising BrokenBarrierError -> test failure) if the
        # walk is serial and the other fetches never start.
        all_entered.wait(timeout=5)
        return {1: job_lib.JobStatus.RUNNING}

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _blocking_get_job_status)

    mgr = _build_manager()
    lanes = []
    submit = mgr._submit_remote_io

    def _record_submit(fn, *args, lane, **kwargs):
        lanes.append(lane)
        return submit(fn, *args, lane=lane, **kwargs)

    monkeypatch.setattr(mgr, '_submit_remote_io', _record_submit)
    fetch = threading.Thread(target=mgr._fetch_exact_status)
    fetch.start()
    fetch.join(timeout=10)
    assert not fetch.is_alive(), 'parallel job-status walk did not complete'
    assert not all_entered.broken
    assert lanes == [replica_managers._ReplicaRemoteIOLane.STATUS] * 3
    assert len(transport_kwargs) == 3
    assert all(
        set(kwargs) == {'serve_transport'} for kwargs in transport_kwargs)
    assert all(
        isinstance(kwargs['serve_transport'], backends.ServeJobStatusTransport)
        for kwargs in transport_kwargs)


def test_fetch_exact_status_releases_lock_during_ssh(monkeypatch):
    replicas = [_tracked_replica(i) for i in range(3)]
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas))
    # Avoid the real cluster lookup / handle assertion.
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: object())

    ssh_entered = threading.Event()
    ssh_release = threading.Event()

    def _blocking_get_job_status(self,
                                 handle,
                                 job_ids,
                                 stream_logs=False,
                                 **_kwargs):
        ssh_entered.set()
        # Simulate an unreachable replica's hung SSH connect.
        assert ssh_release.wait(timeout=5)
        return {1: job_lib.JobStatus.RUNNING}

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _blocking_get_job_status)

    mgr = _build_manager()
    fetch = threading.Thread(target=mgr._fetch_exact_status)
    fetch.start()
    acquired = False
    try:
        assert ssh_entered.wait(2), 'job-status SSH walk did not start'
        # While the SSH is blocked, the lock MUST be acquirable. Pre-fix
        # (_fetch_exact_status was @with_lock) the lock is held for the whole
        # walk and this times out -> the control loop would be wedged behind
        # an unreachable replica.
        acquired = mgr.lock.acquire(timeout=1.0)
        if acquired:
            mgr.lock.release()
    finally:
        ssh_release.set()
        fetch.join(timeout=5)

    assert acquired, (
        'self.lock was held during the get_job_status SSH walk; an unreachable '
        'replica would stall the refresher/prober/scaler')


def _raise_command_error(self, handle, job_ids, stream_logs=False, **_kwargs):
    raise exceptions.CommandError(returncode=255,
                                  command='get_job_status',
                                  error_msg='ssh failed',
                                  detailed_reason=None)


def test_preemption_path_acts_on_fresh_replica_not_stale_snapshot(monkeypatch):
    """On CommandError, preemption handling must re-read the replica under the
    lock and act on the FRESH state, not the pre-SSH snapshot (which another
    thread may have mutated while we SSHed lock-free)."""
    stale = _tracked_replica(7)
    fresh = copy.deepcopy(stale)
    monkeypatch.setattr(serve_state, 'get_replica_infos', lambda svc: [stale])
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: object())
    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _raise_command_error)
    monkeypatch.setattr(serve_state, 'get_replica_info_from_id',
                        lambda svc, rid: fresh)

    persisted = []
    terminated = []
    mgr = _build_manager()
    mgr._cloud_instance_looks_alive = lambda *_args, **_kwargs: (
        replica_managers._PreemptionPrefilterResult(
            replica_managers._PreemptionPrefilterDisposition.INTERRUPTED))
    mgr._persist_replica = lambda rid, info: persisted.append((rid, info))
    mgr._terminate_replica = lambda rid, **kwargs: terminated.append(rid)
    mgr._fetch_exact_status()

    assert persisted == [(7, fresh)]
    assert terminated == [7]
    assert fresh.status_property.preempted


def test_preemption_path_skips_vanished_replica(monkeypatch):
    """If the replica is gone (re-read returns None) after a CommandError, the
    preemption path must skip it, not crash or act on the stale snapshot."""
    stale = _tracked_replica(9)
    monkeypatch.setattr(serve_state, 'get_replica_infos', lambda svc: [stale])
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: object())
    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _raise_command_error)
    monkeypatch.setattr(serve_state, 'get_replica_info_from_id',
                        lambda svc, rid: None)

    called = []
    mgr = _build_manager()
    mgr._cloud_instance_looks_alive = lambda *_args, **_kwargs: (
        replica_managers._PreemptionPrefilterResult(
            replica_managers._PreemptionPrefilterDisposition.INTERRUPTED))
    mgr._persist_replica = lambda *_args, **_kwargs: called.append('persist')
    mgr._terminate_replica = lambda *_args, **_kwargs: called.append('terminate'
                                                                    )
    mgr._fetch_exact_status()  # must not raise

    assert called == [], 'vanished replica must be skipped, not handled'


def test_walk_skips_replica_whose_cluster_record_vanished(monkeypatch):
    """A replica whose cluster record vanished mid-walk (handle() is None)
    must be skipped without aborting the walk for the remaining replicas."""
    replicas = [_tracked_replica(1), _tracked_replica(2)]
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas))
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: None
                        if self.replica_id == 1 else object())

    probed = []

    def _get_job_status(self, handle, job_ids, stream_logs=False, **_kwargs):
        probed.append(handle)
        return {1: job_lib.JobStatus.RUNNING}

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _get_job_status)

    mgr = _build_manager()
    mgr._fetch_exact_status()  # must not raise

    assert len(probed) == 1, 'the remaining replica must still be checked'


def test_user_failure_path_skips_replica_scheduled_down(monkeypatch):
    """A FAILED job status must not mark user_app_failed on (or re-terminate)
    a replica that was scheduled for scale-down while the SSH ran lock-free."""
    stale = _tracked_replica(3)
    fresh = _tracked_replica(3)  # distinct object, same id
    fresh.status_property.sky_down_status = common_utils.ProcessStatus.SCHEDULED
    monkeypatch.setattr(serve_state, 'get_replica_infos', lambda svc: [stale])
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: object())
    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status',
                        lambda self, handle, job_ids, stream_logs=False, **
                        _kwargs: {1: job_lib.JobStatus.FAILED})
    monkeypatch.setattr(serve_state, 'get_replica_info_from_id',
                        lambda svc, rid: fresh)

    def _unexpected_persist(*_args, **_kwargs):
        pytest.fail('scheduled-down replica must not be persisted')

    monkeypatch.setattr(serve_state, 'add_or_update_replica',
                        _unexpected_persist)

    terminated = []
    mgr = _build_manager()
    mgr._terminate_replica = lambda rid, **kwargs: terminated.append(rid)
    mgr._fetch_exact_status()

    assert not fresh.status_property.user_app_failed
    assert terminated == []


def test_command_error_on_one_replica_does_not_starve_the_rest(monkeypatch):
    """A non-preemption CommandError on one replica must not abort the walk:
    a FAILED user job on a later replica must still be detected and the
    replica terminated in the same round."""
    broken = _tracked_replica(1)
    failed = _tracked_replica(2)
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: [broken, failed])
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: self.replica_id)

    def _get_job_status(self, handle, job_ids, stream_logs=False, **_kwargs):
        if handle == 1:
            raise exceptions.CommandError(returncode=255,
                                          command='get_job_status',
                                          error_msg='ssh failed',
                                          detailed_reason=None)
        return {1: job_lib.JobStatus.FAILED}

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _get_job_status)
    monkeypatch.setattr(serve_state, 'get_replica_info_from_id',
                        lambda svc, rid: broken if rid == 1 else failed)
    writes = []

    def _persist_existing(_service_name, replica_id, info, *,
                          expected_replica_exists, **_fence_kwargs):
        assert expected_replica_exists is True
        writes.append((replica_id, info))
        return True

    monkeypatch.setattr(serve_state, 'add_or_update_replica', _persist_existing)

    terminated = []
    mgr = _build_manager()
    # Provider still reports the backend live: this is a command failure, not
    # interruption evidence.
    mgr._cloud_instance_looks_alive = lambda *_args, **_kwargs: (
        replica_managers._PreemptionPrefilterResult(
            replica_managers._PreemptionPrefilterDisposition.LIVE_OR_UNPROVEN))
    mgr._terminate_replica = lambda rid, **kwargs: terminated.append(rid)
    mgr._fetch_exact_status()  # must not raise

    assert terminated == [
        2
    ], ('the failed replica after the broken one must still be terminated')
    assert failed.status_property.user_app_failed
    assert writes == [(2, failed)]


def test_empty_job_statuses_skipped_without_aborting_walk(monkeypatch):
    """An empty job-status result on one replica (e.g. wiped job table) must
    be skipped, not raise IndexError and abort the walk."""
    empty = _tracked_replica(1)
    healthy = _tracked_replica(2)
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: [empty, healthy])
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: self.replica_id)

    probed = []

    def _get_job_status(self, handle, job_ids, stream_logs=False, **_kwargs):
        probed.append(handle)
        if handle == 1:
            return {}
        return {1: job_lib.JobStatus.RUNNING}

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _get_job_status)

    mgr = _build_manager()
    mgr._fetch_exact_status()  # must not raise

    assert sorted(probed) == [1, 2]


def test_pool_status_is_owned_by_probe_not_exact_status_daemon(monkeypatch):
    """Pool dummy-job status is queried once by the readiness probe."""
    missing = _tracked_replica(1)
    healthy = _tracked_replica(2)
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: [missing, healthy])
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: self.replica_id)

    probed = []

    def _get_job_status(self, handle, job_ids, stream_logs=False, **_kwargs):
        probed.append(handle)
        if handle == 1:
            return {}
        return {1: job_lib.JobStatus.RUNNING}

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status', _get_job_status)

    mgr = _build_manager()
    mgr._is_pool = True
    mgr._fetch_exact_status()  # must not raise

    assert probed == []


def test_walk_constructs_backend_once(monkeypatch):
    """The stateless backend must be constructed once per walk, not once per
    replica."""
    replicas = [_tracked_replica(i) for i in (1, 2, 3)]
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas))
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: object())

    constructed = []
    real_backend = replica_managers.backends.CloudVmRayBackend

    class _CountingBackend(real_backend):

        def __init__(self, *args, **kwargs):
            constructed.append(1)
            super().__init__(*args, **kwargs)

        def get_job_status(self, handle, job_ids, stream_logs=False, **_kwargs):
            return {1: job_lib.JobStatus.RUNNING}

    monkeypatch.setattr(replica_managers.backends, 'CloudVmRayBackend',
                        _CountingBackend)

    mgr = _build_manager()
    mgr._fetch_exact_status()

    assert len(constructed) == 1


def test_walk_batches_cluster_records_into_one_read(monkeypatch):
    """The walk must resolve every replica's handle from ONE batched
    cluster-record read; a per-replica cluster-table fallback re-introduces
    N serialized DB reads per fetch round."""
    replicas = [_tracked_replica(i) for i in (1, 2, 3)]
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas))
    batch_calls = []

    def _get_clusters_from_names(names):
        batch_calls.append(list(names))
        return {name: {'handle': object()} for name in names}

    monkeypatch.setattr(replica_managers.global_user_state,
                        'get_clusters_from_names', _get_clusters_from_names)
    monkeypatch.setattr(
        replica_managers.global_user_state, 'get_handle_from_cluster_name',
        lambda name: pytest.fail(
            'the walk must not read cluster records one at a time'))

    seen_records = []

    def _handle(self, cluster_record=None):
        seen_records.append(cluster_record)
        return object()

    monkeypatch.setattr(replica_managers.ReplicaInfo, 'handle', _handle)

    monkeypatch.setattr(replica_managers.backends.CloudVmRayBackend,
                        'get_job_status',
                        lambda self, handle, job_ids, stream_logs=False, **
                        _kwargs: {1: job_lib.JobStatus.RUNNING})

    mgr = _build_manager()
    mgr._fetch_exact_status()

    assert batch_calls == [['c1', 'c2', 'c3']]
    assert len(seen_records) == 3
    assert all(record is not None for record in seen_records)


def test_walk_skips_replica_missing_from_batched_records(monkeypatch):
    """A replica whose cluster record is absent from the batched snapshot
    (row deleted between snapshot and walk) is skipped without a fallback
    per-name read and without aborting the walk."""
    replicas = [_tracked_replica(1), _tracked_replica(2)]
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas))
    monkeypatch.setattr(replica_managers.global_user_state,
                        'get_clusters_from_names', lambda names: {
                            'c1': None,
                            'c2': {
                                'handle': object()
                            }
                        })
    monkeypatch.setattr(
        replica_managers.global_user_state, 'get_handle_from_cluster_name',
        lambda name: pytest.fail(
            'missing record must not trigger a per-name fallback read'))
    monkeypatch.setattr(replica_managers.ReplicaInfo,
                        'handle',
                        lambda self, cluster_record=None: object())

    probed = []
    monkeypatch.setattr(
        replica_managers.backends.CloudVmRayBackend,
        'get_job_status',
        lambda self, handle, job_ids, stream_logs=False, **_kwargs:
        (probed.append(handle) or {
            1: job_lib.JobStatus.RUNNING
        }))

    mgr = _build_manager()
    mgr._fetch_exact_status()  # must not raise

    assert len(probed) == 1, 'the replica with a record must still be checked'
