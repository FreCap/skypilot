"""Regression tests for managed-job controller refresh ownership.

Fixed-slot controller processes are supervised by the controller runtime.  A
status refresh cannot use their Pod-local PIDs as a death proof and must not
perform provider cleanup.  Incomplete pre-cutover rows retain only the narrow
legacy PID terminalization/recovery behavior needed during deployment.
"""

import pytest

from sky.jobs import state as managed_job_state
from sky.jobs import utils
from sky.skylet import job_lib


def _make_status_check_info():
    """Return the slim per-job shape consumed by the refresh sweep."""
    return {
        1: {
            'schedule_state': managed_job_state.ManagedJobScheduleState.ALIVE,
            'controller_pid': 123,
            'controller_pid_started_at': None,
            'controller_instance_id': None,
            'controller_generation': None,
            'controller_slot_id': None,
            'controller_slot_attempt': None,
            'controller_slot_quiescing': False,
            'pool': None,
            'tasks': [{
                'task_id': 0,
                'status': managed_job_state.ManagedJobStatus.RUNNING,
                'task_name': 'job',
                'submitted_at': 100.0,
                'start_at': 110.0,
                'last_recovered_at': 110.0,
            }],
        }
    }


def _make_job_status_check_state(schedule_state,
                                 pid=123,
                                 started_at=None,
                                 all_tasks_terminal=False):
    return {
        'schedule_state': schedule_state,
        'controller_pid': pid,
        'controller_pid_started_at': started_at,
        'controller_instance_id': None,
        'controller_generation': None,
        'controller_slot_id': None,
        'controller_slot_attempt': None,
        'controller_slot_quiescing': False,
        'all_tasks_terminal': all_tasks_terminal,
    }


def _unexpected(label):

    def _raise(*args, **kwargs):
        del args, kwargs
        raise AssertionError(f'unexpected refresh action: {label}')

    return _raise


def _forbid_split_snapshot_helpers(monkeypatch):
    monkeypatch.setattr(managed_job_state, 'get_jobs_to_check_status',
                        _unexpected('split status-id query'))
    monkeypatch.setattr(managed_job_state, 'get_jobs_status_check_info',
                        _unexpected('second status-detail query'))


def _forbid_provider_cleanup(monkeypatch):
    monkeypatch.setattr(utils, 'terminate_cluster',
                        _unexpected('cluster termination'))
    monkeypatch.setattr(utils, 'generate_managed_job_cluster_name',
                        _unexpected('cluster-name discovery'))
    monkeypatch.setattr(utils.global_user_state,
                        'get_managed_job_cluster_cleanup_candidates',
                        _unexpected('global cluster-row discovery'))


def test_global_sweep_does_not_discover_done_cluster_rows(monkeypatch):
    """Terminal resource adoption is scheduler-owned, not refresh-owned."""
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)
    monkeypatch.setattr(managed_job_state, 'get_jobs_to_check_status_info',
                        lambda job_ids: {})
    _forbid_provider_cleanup(monkeypatch)

    utils.update_managed_jobs_statuses()


@pytest.mark.parametrize(('recorded_owner', 'terminal'), [
    (('current-instance', 7), False),
    (('stale-instance', 6), True),
])
def test_complete_fixed_slot_row_is_observational_only(monkeypatch,
                                                       recorded_owner,
                                                       terminal):
    """A complete slot tuple forbids PID verdicts and provider effects."""
    info = _make_status_check_info()
    info[1].update({
        'controller_instance_id': recorded_owner[0],
        'controller_generation': recorded_owner[1],
        'controller_slot_id': 3,
        'controller_slot_attempt': '9c2252b5-f40b-4922-8d4a-282d2d43d7ae',
    })
    if terminal:
        info[1]['tasks'][0]['status'] = (
            managed_job_state.ManagedJobStatus.SUCCEEDED)

    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: ('current-instance', 7))
    monkeypatch.setattr(utils, 'controller_process_alive',
                        _unexpected('fixed-slot PID inspection'))
    monkeypatch.setattr(managed_job_state,
                        'set_failed_controller_if_current_snapshot',
                        _unexpected('fixed-slot PID terminalization'))
    monkeypatch.setattr(managed_job_state, 'reset_job_for_recovery_if_stale',
                        _unexpected('fixed-slot refresh recovery'))
    monkeypatch.setattr(managed_job_state, 'get_job_status_check_state',
                        _unexpected('fixed-slot destructive reread'))
    monkeypatch.setattr(job_lib, 'get_status',
                        _unexpected('fixed-slot skylet status read'))
    _forbid_provider_cleanup(monkeypatch)

    utils.update_managed_jobs_statuses(job_ids=[1], jobs_info=info)


def _wire_legacy_dead_controller(monkeypatch,
                                 set_failed_calls,
                                 fresh_state=None,
                                 set_failed_return=managed_job_state.
                                 ControllerFailureDecision.TERMINALIZED):
    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_ids=None: _make_status_check_info())
    if fresh_state is None:
        fresh_state = _make_job_status_check_state(
            managed_job_state.ManagedJobScheduleState.ALIVE)
    monkeypatch.setattr(managed_job_state, 'get_job_status_check_state',
                        lambda job_id: fresh_state)
    monkeypatch.setattr(utils, 'controller_process_alive', lambda record: False)

    def _set_failed(*args, **kwargs):
        set_failed_calls.append((args, kwargs))
        if callable(set_failed_return):
            return set_failed_return(*args, **kwargs)
        if set_failed_return is True:
            return managed_job_state.ControllerFailureDecision.TERMINALIZED
        if set_failed_return is False:
            return managed_job_state.ControllerFailureDecision.STALE
        return set_failed_return

    monkeypatch.setattr(managed_job_state,
                        'set_failed_controller_if_current_snapshot',
                        _set_failed)
    _forbid_provider_cleanup(monkeypatch)


def test_defers_legacy_failed_controller_when_restart_begins_midcycle(
        monkeypatch):
    set_failed_calls = []
    _wire_legacy_dead_controller(monkeypatch, set_failed_calls)
    sequence = iter([False, True])
    monkeypatch.setattr(utils, '_controller_is_restarting',
                        lambda: next(sequence))

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert not set_failed_calls


def test_legacy_dead_controller_terminalizes_without_provider_effects(
        monkeypatch):
    """The deployment-only legacy path writes state but never cleans up."""
    set_failed_calls = []
    _wire_legacy_dead_controller(monkeypatch, set_failed_calls)
    monkeypatch.setattr(managed_job_state, 'get_job_status_check_state',
                        _unexpected('successful terminalization status reread'))
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert len(set_failed_calls) == 1


def test_legacy_terminal_tasks_wait_for_cleanup_adoption(monkeypatch):
    """Incomplete terminal rows stay visible to the canonical adopter."""
    info = _make_status_check_info()
    info[1]['tasks'][0]['status'] = (
        managed_job_state.ManagedJobStatus.SUCCEEDED)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: ('current-instance', 7))
    monkeypatch.setattr(utils, 'controller_process_alive',
                        _unexpected('terminal legacy PID inspection'))
    monkeypatch.setattr(managed_job_state,
                        'set_failed_controller_if_current_snapshot',
                        _unexpected('terminal legacy status rewrite'))
    monkeypatch.setattr(managed_job_state, 'reset_job_for_recovery_if_stale',
                        _unexpected('terminal legacy recovery reset'))
    monkeypatch.setattr(managed_job_state, 'get_job_status_check_state',
                        _unexpected('terminal legacy destructive reread'))
    _forbid_provider_cleanup(monkeypatch)

    utils.update_managed_jobs_statuses(job_ids=[1], jobs_info=info)


def test_defers_when_legacy_job_reset_for_recovery_midcycle(monkeypatch):
    """A declined legacy CAS rechecks state and performs no provider action."""
    set_failed_calls = []
    _wire_legacy_dead_controller(
        monkeypatch,
        set_failed_calls,
        fresh_state=_make_job_status_check_state(
            managed_job_state.ManagedJobScheduleState.WAITING, pid=None),
        set_failed_return=False)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert len(set_failed_calls) == 1


def test_incomplete_stale_outer_generation_is_reset_for_recovery(monkeypatch):
    """Keep the deployment-only stale-row reconciliation path."""
    info = _make_status_check_info()
    info[1]['controller_instance_id'] = 'old-instance'
    info[1]['controller_generation'] = 12
    current_owner = ('new-instance', 13)
    resets = []

    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_ids=None: info)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: current_owner)
    monkeypatch.setattr(
        managed_job_state, 'reset_job_for_recovery_if_stale',
        lambda job_id, owner: resets.append((job_id, owner)) or True)
    monkeypatch.setattr(utils, 'controller_process_alive',
                        _unexpected('foreign PID inspection'))
    monkeypatch.setattr(managed_job_state,
                        'set_failed_controller_if_current_snapshot',
                        _unexpected('stale-row terminalization'))
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    _forbid_provider_cleanup(monkeypatch)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert resets == [(1, current_owner)]


def _make_pending_status_check_info(schedule_state):
    """A pending job whose legacy controller process has not started."""
    info = _make_status_check_info()
    info[1].update({
        'schedule_state': schedule_state,
        'controller_pid': None,
        'controller_pid_started_at': None,
    })
    info[1]['tasks'][0]['status'] = managed_job_state.ManagedJobStatus.PENDING
    return info


@pytest.mark.parametrize('schedule_state', [
    managed_job_state.ManagedJobScheduleState.INACTIVE,
    managed_job_state.ManagedJobScheduleState.WAITING,
])
def test_pending_legacy_job_skips_controller_status_read(
        monkeypatch, schedule_state):
    get_status_calls = []
    set_failed_calls = []

    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)
    monkeypatch.setattr(
        managed_job_state,
        'get_jobs_to_check_status_info',
        lambda job_ids=None: _make_pending_status_check_info(schedule_state))
    monkeypatch.setattr(
        job_lib, 'get_status', lambda job_id: get_status_calls.append(job_id) or
        job_lib.JobStatus.FAILED_SETUP)
    monkeypatch.setattr(
        managed_job_state, 'set_failed_controller_if_current_snapshot',
        lambda *args, **kwargs: set_failed_calls.append((args, kwargs)))
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    _forbid_provider_cleanup(monkeypatch)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert not get_status_calls
    assert not set_failed_calls
