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
    monkeypatch.setattr(managed_job_state, 'get_jobs_to_check_status_summary',
                        _unexpected('summary refresh query'))


def _forbid_provider_cleanup(monkeypatch):
    monkeypatch.setattr(utils, 'terminate_cluster',
                        _unexpected('cluster termination'))
    monkeypatch.setattr(utils, 'generate_managed_job_cluster_name',
                        _unexpected('cluster-name discovery'))
    monkeypatch.setattr(utils.global_user_state,
                        'get_managed_job_cluster_cleanup_candidates',
                        _unexpected('global cluster-row discovery'))


def _record_call(calls: list[tuple[tuple[object, ...], dict[str, object]]],
                 *args, **kwargs) -> None:
    calls.append((args, kwargs))


def test_global_sweep_does_not_discover_done_cluster_rows(monkeypatch):
    """Terminal resource adoption is scheduler-owned, not refresh-owned."""
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)
    monkeypatch.setattr(managed_job_state, 'get_jobs_to_check_status_summary',
                        lambda job_ids: {})
    _forbid_provider_cleanup(monkeypatch)

    utils.update_managed_jobs_statuses()


def test_recovery_requeues_only_exact_terminal_done_cluster_owners(monkeypatch):
    """Recovery nominates orphans; the scheduler still owns cleanup."""
    cluster_candidates = {
        'job-6036': '6036',
        'legacy-task-7': None,
        'wrong-name-8': '8',
        'nonterminal-9': '9',
        'pool-task-10': '10',
        'never-launched-11': '11',
        'malformed': 'not-a-job-id',
        # A legacy row whose numeric suffix collides with another job id
        # (production shape: a pre-attribution serve replica 'cf-repro-1').
        'cf-repro-12': None,
    }
    monkeypatch.setattr(utils.global_user_state,
                        'get_managed_job_cluster_cleanup_candidates',
                        lambda: cluster_candidates)

    def _task(task_name, status, launched=True):
        return {
            'task_id': 0,
            'task_name': task_name,
            'status': status,
            'submitted_at': 100.0 if launched else None,
            'start_at': None,
            'last_recovered_at': None,
        }

    snapshots = {
        6036: {
            'schedule_state': managed_job_state.ManagedJobScheduleState.DONE,
            'pool': None,
            'tasks': [
                _task('job', managed_job_state.ManagedJobStatus.SUCCEEDED)
            ],
        },
        7: {
            'schedule_state': managed_job_state.ManagedJobScheduleState.DONE,
            'pool': None,
            'tasks': [
                _task('legacy-task', managed_job_state.ManagedJobStatus.FAILED)
            ],
        },
        8: {
            'schedule_state': managed_job_state.ManagedJobScheduleState.DONE,
            'pool': None,
            'tasks': [
                _task('expected-name',
                      managed_job_state.ManagedJobStatus.FAILED)
            ],
        },
        9: {
            'schedule_state': managed_job_state.ManagedJobScheduleState.DONE,
            'pool': None,
            'tasks': [
                _task('nonterminal', managed_job_state.ManagedJobStatus.RUNNING)
            ],
        },
        10: {
            'schedule_state': managed_job_state.ManagedJobScheduleState.DONE,
            'pool': 'shared-pool',
            'tasks': [
                _task('pool-task', managed_job_state.ManagedJobStatus.SUCCEEDED)
            ],
        },
        11: {
            'schedule_state': managed_job_state.ManagedJobScheduleState.DONE,
            'pool': None,
            'tasks': [
                _task('never-launched',
                      managed_job_state.ManagedJobStatus.CANCELLED,
                      launched=False)
            ],
        },
        12: {
            'schedule_state': managed_job_state.ManagedJobScheduleState.DONE,
            'pool': None,
            'tasks': [
                _task('mmp-chembl-100',
                      managed_job_state.ManagedJobStatus.CANCELLED)
            ],
        },
    }
    monkeypatch.setattr(
        managed_job_state, 'get_jobs_status_check_info', lambda job_ids:
        {job_id: snapshots[job_id] for job_id in job_ids if job_id in snapshots})
    requeued = []
    monkeypatch.setattr(
        managed_job_state, 'requeue_terminal_done_jobs_for_cleanup',
        lambda job_ids: requeued.extend(job_ids) or len(job_ids))

    assert utils.requeue_terminal_done_jobs_with_live_clusters() == 2
    assert requeued == [6036, 7]


def test_done_nonterminal_without_workspace_terminalizes(monkeypatch):
    """Inconsistent rows must not depend on cancellation workspace routing."""
    info = _make_status_check_info()
    info[1]['schedule_state'] = managed_job_state.ManagedJobScheduleState.DONE
    terminalized = []

    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)

    def _set_failed(*args, **kwargs):
        terminalized.append((args, kwargs))
        return managed_job_state.ControllerFailureDecision.TERMINALIZED

    monkeypatch.setattr(managed_job_state,
                        'set_failed_controller_if_current_snapshot',
                        _set_failed)
    monkeypatch.setattr(managed_job_state, 'get_job_status_check_state',
                        _unexpected('successful terminalization status reread'))
    monkeypatch.setattr(utils, 'controller_process_alive',
                        _unexpected('DONE row PID inspection'))
    _forbid_provider_cleanup(monkeypatch)

    utils.update_managed_jobs_statuses(job_ids=[1], jobs_info=info)

    assert len(terminalized) == 1


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
                                 set_failed_return=managed_job_state.
                                 ControllerFailureDecision.TERMINALIZED):
    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_summary',
                        lambda job_ids=None: _make_status_check_info())
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
    set_failed_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    _wire_legacy_dead_controller(monkeypatch, set_failed_calls)
    sequence = iter([False, True])
    monkeypatch.setattr(utils, '_controller_is_restarting',
                        lambda: next(sequence))

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert not set_failed_calls


def test_legacy_dead_controller_terminalizes_without_provider_effects(
        monkeypatch):
    """The deployment-only legacy path writes state but never cleans up."""
    set_failed_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
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
    """A declined legacy CAS performs no provider action or point reread."""
    set_failed_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    _wire_legacy_dead_controller(monkeypatch,
                                 set_failed_calls,
                                 set_failed_return=False)
    monkeypatch.setattr(managed_job_state, 'get_job_status_check_state',
                        _unexpected('declined-CAS point reread'))
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert len(set_failed_calls) == 1


def test_incomplete_stale_outer_generation_is_reset_for_recovery(monkeypatch):
    """Keep the deployment-only stale-row reconciliation path."""
    info = _make_status_check_info()
    info[1]['controller_instance_id'] = 'old-instance'
    info[1]['controller_generation'] = 12
    current_owner = ('new-instance', 13)
    resets: list[tuple[int, tuple[str, int]]] = []

    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_summary',
                        lambda job_ids=None: info)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: current_owner)

    def _reset_job_for_recovery_if_stale(job_id, owner) -> bool:
        resets.append((job_id, owner))
        return True

    monkeypatch.setattr(managed_job_state, 'reset_job_for_recovery_if_stale',
                        _reset_job_for_recovery_if_stale)
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
    get_status_calls: list[int] = []
    set_failed_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)
    monkeypatch.setattr(
        managed_job_state,
        'get_jobs_to_check_status_summary',
        lambda job_ids=None: _make_pending_status_check_info(schedule_state))

    def _get_status(job_id):
        get_status_calls.append(job_id)
        return job_lib.JobStatus.FAILED_SETUP

    def _set_failed(*args, **kwargs):
        _record_call(set_failed_calls, *args, **kwargs)
        return None

    monkeypatch.setattr(job_lib, 'get_status', _get_status)
    monkeypatch.setattr(managed_job_state,
                        'set_failed_controller_if_current_snapshot',
                        _set_failed)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    _forbid_provider_cleanup(monkeypatch)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert not get_status_calls
    assert not set_failed_calls


def test_shared_controller_pid_is_probed_once_per_refresh_sweep(monkeypatch):
    info = {
        1: _make_status_check_info()[1],
        2: {
            **_make_status_check_info()[1],
            'tasks': [{
                **_make_status_check_info()[1]['tasks'][0],
                'task_name': 'job-2',
            }],
        },
    }
    info[1]['controller_pid_started_at'] = 1000.0
    info[2]['controller_pid_started_at'] = 1000.0
    calls: list[managed_job_state.ControllerPidRecord] = []

    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_summary',
                        lambda job_ids=None: info)

    def _controller_process_alive(record) -> bool:
        calls.append(record)
        return True

    monkeypatch.setattr(utils, 'controller_process_alive',
                        _controller_process_alive)
    monkeypatch.setattr(managed_job_state,
                        'set_failed_controller_if_current_snapshot',
                        _unexpected('shared live controller failure write'))
    monkeypatch.setattr(managed_job_state, 'get_job_status_check_state',
                        _unexpected('shared live controller fresh reread'))
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    _forbid_provider_cleanup(monkeypatch)

    utils.update_managed_jobs_statuses(job_ids=[1, 2])

    assert calls == [
        managed_job_state.ControllerPidRecord(pid=123, started_at=1000.0)
    ]


def test_same_pid_different_start_times_do_not_share_probe_result(monkeypatch):
    info = {
        1: _make_status_check_info()[1],
        2: {
            **_make_status_check_info()[1],
            'tasks': [{
                **_make_status_check_info()[1]['tasks'][0],
                'task_name': 'job-2',
            }],
        },
    }
    info[1]['controller_pid_started_at'] = 1000.0
    info[2]['controller_pid_started_at'] = 2000.0
    calls: list[managed_job_state.ControllerPidRecord] = []

    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_summary',
                        lambda job_ids=None: info)

    def _controller_process_alive(record) -> bool:
        calls.append(record)
        return True

    monkeypatch.setattr(utils, 'controller_process_alive',
                        _controller_process_alive)
    monkeypatch.setattr(managed_job_state,
                        'set_failed_controller_if_current_snapshot',
                        _unexpected('distinct controller incarnation failure'))
    monkeypatch.setattr(managed_job_state, 'get_job_status_check_state',
                        _unexpected('distinct controller incarnation reread'))
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    _forbid_provider_cleanup(monkeypatch)

    utils.update_managed_jobs_statuses(job_ids=[1, 2])

    assert calls == [
        managed_job_state.ControllerPidRecord(pid=123, started_at=1000.0),
        managed_job_state.ControllerPidRecord(pid=123, started_at=2000.0),
    ]
