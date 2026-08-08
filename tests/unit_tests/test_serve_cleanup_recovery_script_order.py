"""Regression tests for owner-fenced teardown finalization.

``sky/serve/service.py::_cleanup`` used to delete the
``serve_ha_recovery_script`` row on its very first line, *before* the
(seconds-to-minutes) replica teardown.
If the controller pod was then killed mid-teardown (HA pod move / node drain),
the durable service row survived but its recovery script was gone, so
``ha_recovery_for_consolidation_mode`` logged 'recovery script does not exist.
Skipping recovery' forever and stranded the service with replicas still
consuming resources.

The recovery script must outlive replica teardown so a crash partway through
leaves recovery able to respawn the controller and re-run cleanup. A successful
finalizer removes it atomically with the exact service row; a failed finalizer
first publishes ``FAILED_CLEANUP`` and only then removes the script to avoid a
persistent recovery loop.
"""
# pylint: disable=protected-access
import types
from unittest import mock

import pytest

from sky import exceptions
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import service
from sky.utils import controller_utils


def _replica(replica_id: int) -> replica_managers.ReplicaInfo:
    return replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'c{replica_id}',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)


def _patch_common(monkeypatch, events, replicas):
    """Wire up _cleanup's collaborators to record an ordered event log."""
    monkeypatch.setattr(service.time, 'sleep', lambda *_a, **_k: None)
    monkeypatch.setattr(serve_state, 'get_service_from_name', lambda svc: None)
    monkeypatch.setattr(serve_state, 'service_owner_matches',
                        lambda *args, **kwargs: True)
    monkeypatch.setattr(service.serve_utils, 'lifecycle_lock_is_valid',
                        lambda lock: True)
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda svc: list(replicas))
    monkeypatch.setattr(
        serve_state, 'get_replica_resource_action_identities', lambda svc,
        replica_ids: {replica_id: None for replica_id in replica_ids})
    cluster_names = {replica.cluster_name for replica in replicas}
    monkeypatch.setattr(
        service.serve_utils, 'get_existing_replica_cluster_names',
        lambda replica_infos: cluster_names.intersection(
            replica.cluster_name for replica in replica_infos))
    monkeypatch.setattr(serve_state, 'add_or_update_replica',
                        lambda *a, **k: None)
    monkeypatch.setattr(serve_state, 'remove_replica', lambda *a, **k: None)
    monkeypatch.setattr(serve_state, 'get_service_versions', lambda svc: [])
    monkeypatch.setattr(controller_utils,
                        'can_terminate',
                        lambda pool, in_flight=None: True)
    monkeypatch.setattr(serve_state, 'remove_ha_recovery_script',
                        lambda svc: events.append('remove_recovery_script'))


def test_cleanup_preserves_recovery_script_through_replica_teardown(
        monkeypatch):
    """The recovery script remains durable throughout replica teardown."""
    events = []

    def _terminate(cluster_name,
                   unused_log_file_name,
                   continue_guard=None,
                   expected_cluster_record_uuid=None):
        assert continue_guard is not None
        assert continue_guard()
        assert expected_cluster_record_uuid is None
        events.append(f'teardown:{cluster_name}')

    monkeypatch.setattr(replica_managers, 'terminate_cluster', _terminate)
    _patch_common(monkeypatch, events, [_replica(1)])

    failed = service._cleanup('svc', False, 'incarnation-a', 123, None,
                              mock.Mock())

    assert failed is False
    assert events == ['teardown:c1']


def test_cleanup_uses_exact_scoped_cluster_identity_for_long_name(monkeypatch):
    """Truncating a scoped cluster prefix must not make cleanup miss it."""
    events = []
    service_name = 's' * 63
    info = _replica(1)
    info.cluster_name = serve_utils.generate_replica_cluster_name(
        service_name, 1, 'incarnation-a')
    assert not info.cluster_name.startswith(service_name)

    def _terminate(cluster_name,
                   unused_log_file_name,
                   continue_guard=None,
                   expected_cluster_record_uuid=None):
        assert continue_guard is not None and continue_guard()
        assert expected_cluster_record_uuid is None
        events.append(f'teardown:{cluster_name}')

    monkeypatch.setattr(replica_managers, 'terminate_cluster', _terminate)
    _patch_common(monkeypatch, events, [info])

    failed = service._cleanup(service_name,
                              False,
                              'incarnation-a',
                              123,
                              None,
                              mock.Mock(),
                              resource_scope='incarnation-a')

    assert failed is False
    assert events == [f'teardown:{info.cluster_name}']


# --- recovery must resume teardown, not resurrect a torn-down service ---


def _svc(status):
    return {'status': status}


def test_should_resume_teardown():
    """Teardown is resumed only on a recovery run of a service left in a
    teardown status; a healthy (e.g. READY) service is recovered normally and
    a fresh run never resumes teardown."""
    assert service._should_resume_teardown(
        True, _svc(serve_state.ServiceStatus.SHUTTING_DOWN)) is True
    assert service._should_resume_teardown(
        True, _svc(serve_state.ServiceStatus.FAILED_CLEANUP)) is True
    assert service._should_resume_teardown(
        True, _svc(serve_state.ServiceStatus.READY)) is False
    assert service._should_resume_teardown(False, None) is False
    assert service._should_resume_teardown(
        False, _svc(serve_state.ServiceStatus.SHUTTING_DOWN)) is False


def _patch_finalize(monkeypatch, calls):
    monkeypatch.setattr(serve_state, 'get_replica_infos', lambda _svc: [])
    monkeypatch.setattr(
        service.serve_utils, 'quiesce_service_replica_launch_requests',
        lambda *a, **k: calls.append(('quiesce_launches', a[0])) or True)
    monkeypatch.setattr(
        serve_state, 'acknowledge_service_controller_teardown_if_owner',
        lambda *a, **k: calls.append(('begin_teardown', a[0])) or True)
    monkeypatch.setattr(service.serve_utils, 'get_service_lifecycle_lock',
                        lambda name: mock.MagicMock())
    monkeypatch.setattr(service.serve_utils, 'lifecycle_lock_is_valid',
                        lambda lock: True)
    monkeypatch.setattr(serve_state, 'service_owner_matches',
                        lambda *a, **k: True)
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        lambda *a, **k: calls.append(('status', a[4])) or True)
    monkeypatch.setattr(serve_state, 'remove_service_completely',
                        lambda *a, **k: calls.append(('removed', a[0])) or True)
    monkeypatch.setattr(
        serve_state, 'remove_ha_recovery_script_if_owner',
        lambda *a, **k: calls.append(('remove_script', a[0])) or True)
    monkeypatch.setattr(service.lb_k8s, 'get_api_deployment_owner_uid',
                        lambda **_kwargs: 'api-deployment-uid')
    monkeypatch.setattr(service.lb_k8s, 'delete_lb_objects',
                        lambda *a, **k: calls.append(('delete_lb', a[0])))
    monkeypatch.setattr(service, '_cleanup_task_run_script', lambda jid: None)


def test_finalize_removes_service_on_clean_teardown(monkeypatch):
    calls = []
    monkeypatch.setattr(service, '_cleanup', lambda *a, **k: False)
    _patch_finalize(monkeypatch, calls)

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert ('removed', 'svc') in calls
    assert ('status', serve_state.ServiceStatus.FAILED_CLEANUP) not in calls
    # The clean finalizer removes the script atomically with the service row,
    # not through a separate name-keyed delete.
    assert not any(c[0] == 'remove_script' for c in calls)
    assert calls.index(
        ('status', serve_state.ServiceStatus.SHUTTING_DOWN)) < (calls.index(
            ('quiesce_launches', 'svc')))
    assert calls.index(('quiesce_launches', 'svc')) < calls.index(
        ('begin_teardown', 'svc'))
    assert calls.index(('begin_teardown', 'svc')) < calls.index(
        ('delete_lb', 'svc'))


def test_finalize_does_not_ack_or_delete_until_launches_quiesce(monkeypatch):
    calls = []
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda _svc: [_replica(1)])
    monkeypatch.setattr(
        service.serve_utils, 'quiesce_service_replica_launch_requests',
        lambda *a, **k: calls.append(('quiesce_failed', a[0])) or False)
    monkeypatch.setattr(
        serve_state, 'acknowledge_service_controller_teardown_if_owner',
        lambda *a, **k: calls.append(('begin_teardown', a[0])) or True)
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        lambda *a, **k: calls.append(('status', a[4])) or True)
    monkeypatch.setattr(service, '_cleanup', lambda *a, **k: calls.append(
        ('cleanup', a[0])))

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert calls == [('status', serve_state.ServiceStatus.SHUTTING_DOWN),
                     ('quiesce_failed', 'svc')]


def test_finalize_marks_failed_cleanup_when_teardown_fails(monkeypatch):
    calls = []
    monkeypatch.setattr(service, '_cleanup', lambda *a, **k: True)
    _patch_finalize(monkeypatch, calls)

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert ('status', serve_state.ServiceStatus.FAILED_CLEANUP) in calls
    assert not any(c[0] == 'removed' for c in calls)
    # FAILED_CLEANUP is published first, then the recovery script is removed
    # so a persistent cleanup failure cannot loop forever.
    assert ('remove_script', 'svc') in calls


def test_finalize_contains_cleanup_exception_and_breaks_recovery_loop(
        monkeypatch):
    """A _cleanup that RAISES must be contained, leave the service
    FAILED_CLEANUP, AND remove the HA recovery script -- otherwise a persistent
    cleanup error loops forever (FAILED_CLEANUP is a resume status and the
    script was never reached for removal inside _cleanup)."""
    calls = []

    def _boom(*args, **kwargs):
        raise RuntimeError('cleanup blew up')

    monkeypatch.setattr(service, '_cleanup', _boom)
    _patch_finalize(monkeypatch, calls)

    service._run_cleanup_and_finalize('svc', types.SimpleNamespace(pool=False),
                                      '/tmp/svc', 1, 'incarnation-a', 123, None)

    assert ('status', serve_state.ServiceStatus.FAILED_CLEANUP) in calls
    assert ('remove_script', 'svc') in calls, (
        'a caught cleanup exception must remove the HA script to avoid a '
        'recovery loop')


def test_handle_signal_persists_shutting_down_before_consuming_signal(
        monkeypatch, tmp_path):
    """The terminate signal must not be consumed before SHUTTING_DOWN is durably
    set: otherwise a crash in that window loses the teardown intent and HA
    recovery would bring the (user-downed) service back up serving."""
    sig = tmp_path / 'svc.signal'
    sig.write_text('terminate')
    monkeypatch.setattr(service.constants, 'SIGNAL_FILE_PATH',
                        str(tmp_path / '{}.signal'))
    observed = []

    def _record_status(unused_name, unused_hash, unused_pid, unused_ip, status):
        # The signal file must still exist when we persist SHUTTING_DOWN.
        observed.append((status, sig.exists()))

    monkeypatch.setattr(
        serve_state, 'set_service_status_and_active_versions_if_owner',
        lambda *args, **kwargs: _record_status(*args[:5]) or True)
    owner_match = mock.Mock(
        side_effect=AssertionError('redundant owner read must not run'))
    monkeypatch.setattr(serve_state, 'service_owner_matches', owner_match)

    with pytest.raises(exceptions.ServeUserTerminatedError):
        service._handle_signal('svc', 'incarnation-a', 123, None)

    assert observed, 'SHUTTING_DOWN must be persisted on a terminate signal'
    status, signal_existed_at_status_time = observed[0]
    assert status == serve_state.ServiceStatus.SHUTTING_DOWN
    assert signal_existed_at_status_time is True, (
        'status must be set BEFORE the signal file is consumed')
    assert not sig.exists(), 'signal file is consumed after status is persisted'
    owner_match.assert_not_called()


def test_handle_signal_retries_status_cas_db_error_without_cleanup(
        monkeypatch, tmp_path):
    sig = tmp_path / 'svc.signal'
    sig.write_text('terminate')
    monkeypatch.setattr(service.constants, 'SIGNAL_FILE_PATH',
                        str(tmp_path / '{}.signal'))
    monkeypatch.setattr(
        serve_state, 'service_owner_matches', lambda *args, **kwargs:
        (_ for _ in
         ()).throw(AssertionError('redundant owner read must not run')))
    persist = mock.Mock(side_effect=[RuntimeError('db unavailable'), True])
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        persist)

    # The DB error is contained and the wakeup remains durable; _start keeps
    # supervising instead of falling into unexpected-exception cleanup.
    assert service._handle_signal('svc', 'incarnation-a', 123, None)
    assert sig.exists()

    with pytest.raises(exceptions.ServeUserTerminatedError):
        service._handle_signal('svc', 'incarnation-a', 123, None)
    assert not sig.exists()
    assert persist.call_count == 2


def test_scoped_successor_discards_legacy_name_only_terminate(
        monkeypatch, tmp_path):
    sig = tmp_path / 'svc.signal'
    sig.write_text('terminate')
    monkeypatch.setattr(service.constants, 'SIGNAL_FILE_PATH',
                        str(tmp_path / '{}.signal'))
    set_status = mock.Mock()
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        set_status)

    assert service._handle_signal('svc',
                                  'incarnation-b',
                                  123,
                                  None,
                                  resource_scope='incarnation-b')
    assert not sig.exists()
    set_status.assert_not_called()


@pytest.mark.parametrize('malformed', ['not-a-signal', '{'])
def test_handle_signal_ignores_malformed_legacy_payload(monkeypatch, tmp_path,
                                                        malformed):
    sig = tmp_path / 'svc.signal'
    sig.write_text(malformed)
    monkeypatch.setattr(service.constants, 'SIGNAL_FILE_PATH',
                        str(tmp_path / '{}.signal'))
    set_status = mock.Mock()
    monkeypatch.setattr(serve_state,
                        'set_service_status_and_active_versions_if_owner',
                        set_status)

    assert service._handle_signal('svc', 'incarnation-a', 123, None)
    assert not sig.exists()
    set_status.assert_not_called()
