"""Regression tests for controller-restart races in managed-job refresh.

``update_managed_jobs_statuses`` checks the controller-restart signal file once
at the TOP, then iterates jobs and, for any whose controller pid is dead,
terminates the job's cluster and marks it FAILED_CONTROLLER. Marking many jobs
takes time, so a controller restart that begins in that window (creating the
signal file) races the loop: a job that is actually being restarted under it
gets its cluster torn down and is failed terminally instead of resuming.

The fix re-checks the restart signal immediately before the destructive action.
These tests pin that: a restart that appears mid-cycle defers the job's
FAILED_CONTROLLER, while a genuinely dead controller (no restart) still fails.
"""
import threading

import pytest

from sky.jobs import state as managed_job_state
from sky.jobs import utils
from sky.skylet import job_lib


def _make_status_check_info():
    """The slim per-job shape update_managed_jobs_statuses now reads."""
    return {
        1: {
            'schedule_state': managed_job_state.ManagedJobScheduleState.ALIVE,
            'controller_pid': 123,
            'controller_pid_started_at': None,
            'controller_instance_id': None,
            'controller_generation': None,
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
        'all_tasks_terminal': all_tasks_terminal,
    }


def _make_done_terminal_status_check_info(task_name='job'):
    info = _make_status_check_info()
    info[1]['schedule_state'] = managed_job_state.ManagedJobScheduleState.DONE
    info[1]['tasks'][0]['status'] = (
        managed_job_state.ManagedJobStatus.FAILED_CONTROLLER)
    info[1]['tasks'][0]['task_name'] = task_name
    return info


def _forbid_split_snapshot_helpers(monkeypatch):
    monkeypatch.setattr(
        managed_job_state, 'get_jobs_to_check_status', lambda *a, **k:
        (_ for _ in ()).throw(
            AssertionError('refresh must use get_jobs_to_check_status_info')))
    monkeypatch.setattr(
        managed_job_state, 'get_jobs_status_check_info', lambda *a, **k:
        (_ for _ in ()).throw(
            AssertionError('refresh must use get_jobs_to_check_status_info')))


def test_global_sweep_reconciles_done_terminal_legacy_cluster(monkeypatch):
    info = _make_done_terminal_status_check_info(task_name='legacy-task')
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)
    monkeypatch.setattr(managed_job_state, 'get_jobs_to_check_status_info',
                        lambda job_ids: {})
    monkeypatch.setattr(utils.global_user_state,
                        'get_managed_job_cluster_cleanup_candidates',
                        lambda: {'legacy-task-1': None})
    monkeypatch.setattr(managed_job_state, 'get_jobs_status_check_info',
                        lambda job_ids: info if job_ids == [1] else {})
    monkeypatch.setattr(
        managed_job_state, 'get_job_status_check_state',
        lambda job_id: _make_job_status_check_state(
            managed_job_state.ManagedJobScheduleState.DONE,
            all_tasks_terminal=True))
    terminated = []
    finished = []
    monkeypatch.setattr(utils, 'terminate_cluster', terminated.append)
    monkeypatch.setattr(managed_job_state,
                        'finish_controller_cleanup_if_current_snapshot',
                        lambda *a, **k: finished.append((a, k)) or True)

    utils.update_managed_jobs_statuses()

    assert terminated == ['legacy-task-1']
    assert len(finished) == 1


def test_global_sweep_ignores_unproven_legacy_cluster_name(monkeypatch):
    info = _make_done_terminal_status_check_info(task_name='actual-task')
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: None)
    monkeypatch.setattr(managed_job_state, 'get_jobs_to_check_status_info',
                        lambda job_ids: {})
    monkeypatch.setattr(utils.global_user_state,
                        'get_managed_job_cluster_cleanup_candidates',
                        lambda: {'unrelated-cluster-1': None})
    monkeypatch.setattr(managed_job_state, 'get_jobs_status_check_info',
                        lambda job_ids: info if job_ids == [1] else {})
    monkeypatch.setattr(
        managed_job_state, 'get_job_status_check_state', lambda job_id:
        (_ for _ in ()).throw(AssertionError('unproven row must be ignored')))
    monkeypatch.setattr(
        utils, 'terminate_cluster', lambda name:
        (_ for _ in ()).throw(AssertionError('unproven row must be ignored')))

    utils.update_managed_jobs_statuses()


def _wire_dead_controller(monkeypatch,
                          set_failed_calls,
                          job_done_calls,
                          fresh_state=None,
                          set_failed_return=managed_job_state.
                          ControllerFailureDecision.TERMINALIZED):
    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: _make_status_check_info())
    if fresh_state is None:
        fresh_state = _make_job_status_check_state(
            managed_job_state.ManagedJobScheduleState.ALIVE)
    monkeypatch.setattr(managed_job_state, 'get_job_status_check_state',
                        lambda job_id: fresh_state)
    monkeypatch.setattr(utils, 'controller_process_alive', lambda record: False)
    monkeypatch.setattr(utils, 'terminate_cluster', lambda name: None)

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
    monkeypatch.setattr(managed_job_state,
                        'finish_controller_cleanup_if_current_snapshot',
                        lambda *a, **k: job_done_calls.append((a, k)) or True)


def test_defers_failed_controller_when_restart_begins_midcycle(monkeypatch):
    set_failed_calls, job_done_calls = [], []
    _wire_dead_controller(monkeypatch, set_failed_calls, job_done_calls)
    # Restart signal: absent at the top-of-function check, present at the
    # re-check just before the destructive action.
    seq = iter([False, True])
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: next(seq))

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert not set_failed_calls, (
        'a job must not be FAILED_CONTROLLER while its controller is restarting'
    )
    assert not job_done_calls


def test_marks_failed_controller_when_no_restart(monkeypatch):
    set_failed_calls, job_done_calls = [], []
    _wire_dead_controller(monkeypatch, set_failed_calls, job_done_calls)
    monkeypatch.setattr(
        managed_job_state, 'get_managed_job_tasks', lambda job_id:
        (_ for _ in ()).throw(
            AssertionError('must reuse the sweep snapshot, not refetch tasks')))
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert len(set_failed_calls) == 1, (
        'a genuinely dead controller (no restart) must still fail the job')
    assert len(job_done_calls) == 1


def test_dead_controller_common_path_skips_fresh_status_reread(monkeypatch):
    """The common dead-controller path should not pay for a fallback reread."""
    set_failed_calls, job_done_calls = [], []
    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: _make_status_check_info())
    monkeypatch.setattr(
        managed_job_state, 'get_job_status_check_state', lambda job_id:
        (_ for _ in ()).throw(
            AssertionError('successful CAS must not reread point status')))
    monkeypatch.setattr(utils, 'controller_process_alive', lambda record: False)
    monkeypatch.setattr(utils, 'terminate_cluster', lambda name: None)
    monkeypatch.setattr(
        managed_job_state, 'set_failed_controller_if_current_snapshot',
        lambda *a, **k: set_failed_calls.append(
            (a, k)) or managed_job_state.ControllerFailureDecision.TERMINALIZED)
    monkeypatch.setattr(managed_job_state,
                        'finish_controller_cleanup_if_current_snapshot',
                        lambda *a, **k: job_done_calls.append((a, k)) or True)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert len(set_failed_calls) == 1
    assert len(job_done_calls) == 1


def test_terminal_decision_precedes_provider_cleanup_and_done(monkeypatch):
    """The no-recovery decision linearizes before destructive provider work."""
    order = []
    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: _make_status_check_info())
    monkeypatch.setattr(
        managed_job_state, 'get_job_status_check_state',
        lambda job_id: _make_job_status_check_state(
            managed_job_state.ManagedJobScheduleState.ALIVE))
    monkeypatch.setattr(utils, 'controller_process_alive', lambda record: False)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    monkeypatch.setattr(
        managed_job_state, 'set_failed_controller_if_current_snapshot',
        lambda *a, **k: order.append('terminalize') or managed_job_state.
        ControllerFailureDecision.TERMINALIZED)
    monkeypatch.setattr(
        utils, 'terminate_cluster',
        lambda cluster_name: order.append(f'terminate:{cluster_name}'))
    monkeypatch.setattr(managed_job_state,
                        'finish_controller_cleanup_if_current_snapshot',
                        lambda *a, **k: order.append('done') or True)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert order == ['terminalize', 'terminate:job-1', 'done']


def test_cleanup_dedupes_duplicate_task_cluster_teardowns(monkeypatch):
    """Duplicate snapshot rows must not fan out duplicate teardowns."""
    set_failed_calls, job_done_calls = [], []
    _wire_dead_controller(monkeypatch, set_failed_calls, job_done_calls)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    info = _make_status_check_info()
    info[1]['tasks'] = [{
        'task_id': 0,
        'status': managed_job_state.ManagedJobStatus.RUNNING,
        'task_name': 'dup-task',
        'submitted_at': 100.0,
        'start_at': 110.0,
        'last_recovered_at': 110.0,
    }, {
        'task_id': 0,
        'status': managed_job_state.ManagedJobStatus.RUNNING,
        'task_name': 'dup-task',
        'submitted_at': 100.0,
        'start_at': 110.0,
        'last_recovered_at': 110.0,
    }, {
        'task_id': 1,
        'status': managed_job_state.ManagedJobStatus.RUNNING,
        'task_name': 'other-task',
        'submitted_at': 101.0,
        'start_at': 111.0,
        'last_recovered_at': 111.0,
    }]
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: info)
    monkeypatch.setattr(utils, 'generate_managed_job_cluster_name',
                        lambda task_name, job_id: f'{task_name}-{job_id}')
    terminated_clusters = []
    monkeypatch.setattr(utils, 'terminate_cluster', terminated_clusters.append)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert terminated_clusters == ['dup-task-1', 'other-task-1']
    assert len(set_failed_calls) == 1
    assert len(job_done_calls) == 1


def test_cleanup_reports_every_failed_cluster_termination(monkeypatch, caplog):
    """All termination failures are attempted and reported after terminalizing.

    A multi-task job tears down one cluster per task. If terminations for
    several tasks fail, each failed cluster must be attempted and logged. The
    FAILED_CONTROLLER decision is intentionally persisted before teardown, so
    cleanup errors cannot be part of that earlier linearized reason.
    """
    set_failed_calls, job_done_calls = [], []
    _wire_dead_controller(monkeypatch, set_failed_calls, job_done_calls)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    info = _make_status_check_info()
    info[1]['tasks'] = [{
        'task_id': i,
        'status': managed_job_state.ManagedJobStatus.RUNNING,
        'task_name': name,
        'submitted_at': 100.0 + i,
        'start_at': 110.0 + i,
        'last_recovered_at': 110.0 + i,
    } for i, name in enumerate(['task-a', 'task-b', 'task-c'])]
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: info)
    monkeypatch.setattr(utils, 'generate_managed_job_cluster_name',
                        lambda task_name, job_id: f'{task_name}-{job_id}')
    handle_prechecks = []
    monkeypatch.setattr(utils.global_user_state, 'get_handle_from_cluster_name',
                        lambda name: handle_prechecks.append(name) or object())
    attempted = []

    def _terminate(cluster_name):
        attempted.append(cluster_name)
        if cluster_name != 'task-b-1':
            raise RuntimeError(f'teardown boom for {cluster_name}')

    monkeypatch.setattr(utils, 'terminate_cluster', _terminate)

    utils.update_managed_jobs_statuses(job_ids=[1])

    # A failure on one cluster must not stop teardown of the others. The
    # terminations run in parallel, so only the attempted set is defined.
    assert sorted(attempted) == ['task-a-1', 'task-b-1', 'task-c-1']
    assert not handle_prechecks, (
        'terminate_cluster owns the authoritative cluster-row lookup')
    assert len(set_failed_calls) == 1
    failure_reason = set_failed_calls[0][1]['failure_reason']
    assert 'task-a-1' not in failure_reason
    assert 'task-c-1' not in failure_reason
    assert 'task-a-1' in caplog.text
    assert 'task-c-1' in caplog.text
    assert not job_done_calls, (
        'failed cleanup must remain eligible for a later retry')


def test_cleanup_skips_never_launched_followup_tasks(monkeypatch):
    """Cleanup should not invent clusters for untouched later pipeline stages."""
    set_failed_calls, job_done_calls = [], []
    _wire_dead_controller(monkeypatch, set_failed_calls, job_done_calls)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    info = _make_status_check_info()
    info[1]['tasks'] = [{
        'task_id': 0,
        'status': managed_job_state.ManagedJobStatus.FAILED,
        'task_name': 'launched-task',
        'submitted_at': 100.0,
        'start_at': 110.0,
        'last_recovered_at': 110.0,
    }, {
        'task_id': 1,
        'status': managed_job_state.ManagedJobStatus.PENDING,
        'task_name': 'never-launched-task',
        'submitted_at': None,
        'start_at': None,
        'last_recovered_at': None,
    }]
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: info)
    monkeypatch.setattr(utils, 'generate_managed_job_cluster_name',
                        lambda task_name, job_id: f'{task_name}-{job_id}')
    terminated_clusters = []
    monkeypatch.setattr(utils, 'terminate_cluster', terminated_clusters.append)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert terminated_clusters == ['launched-task-1']
    assert len(set_failed_calls) == 1
    assert len(job_done_calls) == 1


def test_cleanup_keeps_backoff_pending_task_with_launch_attempt(monkeypatch):
    """A retry-backoff ``PENDING`` task still owns a cluster to tear down."""
    set_failed_calls, job_done_calls = [], []
    _wire_dead_controller(monkeypatch, set_failed_calls, job_done_calls)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    info = _make_status_check_info()
    info[1]['tasks'] = [{
        'task_id': 0,
        'status': managed_job_state.ManagedJobStatus.PENDING,
        'task_name': 'retrying-task',
        'submitted_at': 100.0,
        'start_at': None,
        'last_recovered_at': 95.0,
    }]
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: info)
    monkeypatch.setattr(utils, 'generate_managed_job_cluster_name',
                        lambda task_name, job_id: f'{task_name}-{job_id}')
    terminated_clusters = []
    monkeypatch.setattr(utils, 'terminate_cluster', terminated_clusters.append)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert terminated_clusters == ['retrying-task-1']
    assert len(set_failed_calls) == 1
    assert len(job_done_calls) == 1


def test_terminal_job_preserves_status_when_controller_dies_during_cleanup(
        monkeypatch):
    """A terminal job should be finalized, not rewritten to FAILED_CONTROLLER.

    Controllers can die during post-terminal cleanup (log streaming, teardown,
    etc.). The refresh loop must preserve the already-terminal task outcome and
    only finalize the scheduler state.
    """
    set_failed_calls, job_done_calls = [], []
    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: _make_status_check_info())
    monkeypatch.setattr(
        managed_job_state, 'get_job_status_check_state', lambda job_id:
        (_ for _ in ()).throw(
            AssertionError('already-terminal decision must not reread point '
                           'status')))
    monkeypatch.setattr(utils, 'controller_process_alive', lambda record: False)
    monkeypatch.setattr(
        managed_job_state, 'set_failed_controller_if_current_snapshot',
        lambda *a, **k: set_failed_calls.append((a, k)) or managed_job_state.
        ControllerFailureDecision.ALREADY_TERMINAL)
    monkeypatch.setattr(managed_job_state,
                        'finish_controller_cleanup_if_current_snapshot',
                        lambda *a, **k: job_done_calls.append((a, k)) or True)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    cleanup_reads = []

    def _record_cluster_name(task_name, job_id):
        cleanup_reads.append((task_name, job_id))
        return f'{task_name}-{job_id}'

    monkeypatch.setattr(utils, 'generate_managed_job_cluster_name',
                        _record_cluster_name)
    terminated_clusters = []

    def _record_termination(cluster_name):
        terminated_clusters.append(cluster_name)

    monkeypatch.setattr(utils, 'terminate_cluster', _record_termination)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert cleanup_reads == [('job', 1)]
    assert terminated_clusters == ['job-1']
    assert len(set_failed_calls) == 1, (
        'the refresh loop should use the exact terminal decision from the '
        'atomic state recheck once')
    assert len(job_done_calls) == 1


def test_restart_after_terminal_decision_defers_cleanup(monkeypatch):
    """A restart after terminalization defers cleanup but keeps the decision.

    The restart signal is absent at the top-of-function check and at the
    pre-terminalization re-check, then appears before cleanup. The durable
    FAILED_CONTROLLER write remains valid and prevents recovery; cleanup and
    DONE are retried by the replacement refresh loop.
    """
    set_failed_calls, job_done_calls = [], []
    _wire_dead_controller(monkeypatch, set_failed_calls, job_done_calls)
    seq = iter([False, False, True])
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: next(seq))
    cleanup_reads = []
    monkeypatch.setattr(
        utils, 'generate_managed_job_cluster_name',
        lambda task_name, job_id: cleanup_reads.append(
            (task_name, job_id)) or f'{task_name}-{job_id}')

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert not cleanup_reads, 'cleanup should defer once restart begins'
    assert len(set_failed_calls) == 1
    assert not job_done_calls


def test_defers_when_job_reset_for_recovery_midcycle(monkeypatch):
    """A job reset after the snapshot (WAITING, pid cleared) must be deferred.

    The batched sweep snapshot judges the job dead (ALIVE, dead pid), but the
    fresh per-job re-read taken just before the destructive action shows it was
    reset for recovery. Neither the cluster teardown nor the terminal
    set_failed may run; the next sweep re-judges the job from fresh state.
    """
    set_failed_calls, job_done_calls = [], []
    _wire_dead_controller(monkeypatch,
                          set_failed_calls,
                          job_done_calls,
                          fresh_state=_make_job_status_check_state(
                              managed_job_state.ManagedJobScheduleState.WAITING,
                              pid=None),
                          set_failed_return=False)
    cleanup_reads = []
    monkeypatch.setattr(
        utils, 'generate_managed_job_cluster_name',
        lambda task_name, job_id: cleanup_reads.append(
            (task_name, job_id)) or f'{task_name}-{job_id}')
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert not cleanup_reads, (
        'cluster cleanup must not start for a job reset for recovery')
    assert len(set_failed_calls) == 1, (
        'the refresh loop may attempt the atomic CAS once before the fallback '
        'snapshot proves the job was reset for recovery')
    assert not job_done_calls


def test_stale_outer_generation_is_recovered_not_failed(monkeypatch):
    """A PID from another controller pod is never a local crash verdict."""
    info = _make_status_check_info()
    info[1]['controller_instance_id'] = 'old-instance'
    info[1]['controller_generation'] = 12
    current_owner = ('new-instance', 13)
    resets = []
    set_failed_calls = []
    job_done_calls = []

    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: info)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: current_owner)
    monkeypatch.setattr(
        managed_job_state, 'reset_job_for_recovery_if_stale',
        lambda job_id, owner: resets.append((job_id, owner)) or True)
    monkeypatch.setattr(
        utils, 'controller_process_alive', lambda record:
        (_ for _ in
         ()).throw(AssertionError('a foreign pod PID must never be inspected')))
    monkeypatch.setattr(
        utils, 'terminate_cluster', lambda name: (_ for _ in ()).throw(
            AssertionError('stale ownership must not tear down the workload')))
    monkeypatch.setattr(
        managed_job_state, 'set_failed_controller_if_current_snapshot',
        lambda *a, **k: set_failed_calls.append(
            (a, k)) or managed_job_state.ControllerFailureDecision.TERMINALIZED)
    monkeypatch.setattr(managed_job_state,
                        'finish_controller_cleanup_if_current_snapshot',
                        lambda *a, **k: job_done_calls.append((a, k)) or True)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert resets == [(1, current_owner)]
    assert not set_failed_calls
    assert not job_done_calls


def test_terminal_stale_generation_is_cleanup_not_recovery(monkeypatch):
    """A replacement adopts terminal cleanup instead of resetting the job."""
    info = _make_status_check_info()
    info[1]['controller_instance_id'] = 'old-instance'
    info[1]['controller_generation'] = 12
    info[1]['tasks'][0]['status'] = (
        managed_job_state.ManagedJobStatus.SUCCEEDED)
    fresh = _make_job_status_check_state(
        managed_job_state.ManagedJobScheduleState.ALIVE,
        all_tasks_terminal=True)
    fresh['controller_instance_id'] = 'old-instance'
    fresh['controller_generation'] = 12
    current_owner = ('new-instance', 13)
    terminated = []
    finished = []

    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: info)
    monkeypatch.setattr(managed_job_state, 'get_job_status_check_state',
                        lambda job_id: fresh)
    monkeypatch.setattr(managed_job_state, 'get_current_controller_owner',
                        lambda: current_owner)
    monkeypatch.setattr(
        managed_job_state, 'reset_job_for_recovery_if_stale', lambda *a, **k:
        (_ for _ in ()).throw(
            AssertionError('terminal work must not be reset for recovery')))
    monkeypatch.setattr(
        utils, 'controller_process_alive', lambda record: (_ for _ in ()).throw(
            AssertionError('a terminal foreign PID must not be inspected')))
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    monkeypatch.setattr(utils, 'terminate_cluster', terminated.append)
    monkeypatch.setattr(managed_job_state,
                        'finish_controller_cleanup_if_current_snapshot',
                        lambda *a, **k: finished.append((a, k)) or True)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert terminated == ['job-1']
    assert len(finished) == 1
    assert finished[0][1]['controller_instance_id'] == 'old-instance'
    assert finished[0][1]['controller_generation'] == 12


def _make_pending_status_check_info(schedule_state):
    """A pending job whose controller process has not started (pid is None)."""
    return {
        1: {
            'schedule_state': schedule_state,
            'controller_pid': None,
            'controller_pid_started_at': None,
            'pool': None,
            'tasks': [{
                'task_id': 0,
                'status': managed_job_state.ManagedJobStatus.PENDING,
                'task_name': 'job',
            }],
        }
    }


@pytest.mark.parametrize('schedule_state', [
    managed_job_state.ManagedJobScheduleState.INACTIVE,
    managed_job_state.ManagedJobScheduleState.WAITING,
])
def test_pending_job_skips_controller_status_read(monkeypatch, schedule_state):
    """A pid-None INACTIVE/WAITING job is skipped before the skylet controller
    status is read.

    Two things are pinned: (1) the per-job filelock + SQLite read in
    ``job_lib.get_status`` is avoided for the pending backlog, and (2) a stale
    skylet ``FAILED_SETUP`` row cannot strand a recovering job — a WAITING job
    (e.g. one reset by ``reset_jobs_for_recovery``) must be left for the
    scheduler to relaunch, not marked FAILED_CONTROLLER.
    """
    get_status_calls = []
    set_failed_calls = []

    def _record_get_status(job_id):
        get_status_calls.append(job_id)
        return job_lib.JobStatus.FAILED_SETUP

    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(
        managed_job_state,
        'get_jobs_to_check_status_info',
        lambda job_id=None: _make_pending_status_check_info(schedule_state))
    monkeypatch.setattr(job_lib, 'get_status', _record_get_status)
    monkeypatch.setattr(
        managed_job_state, 'set_failed_controller_if_current_snapshot',
        lambda *a, **k: set_failed_calls.append(
            (a, k)) or managed_job_state.ControllerFailureDecision.TERMINALIZED)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert not get_status_calls, (
        'controller status must not be read for a pid-None pending job')
    assert not set_failed_calls, (
        'a pending (pre-controller) job must not be FAILED_CONTROLLER, even if '
        'a stale skylet status would read FAILED_SETUP')


def test_cleanup_uses_task_name_identity_for_multi_task_jobs(monkeypatch):
    """Cleanup must derive cluster names from task_name, not public job_name."""
    set_failed_calls, job_done_calls = [], []
    snapshot = {
        1: {
            'schedule_state': managed_job_state.ManagedJobScheduleState.ALIVE,
            'controller_pid': 123,
            'controller_pid_started_at': None,
            'pool': None,
            'tasks': [{
                'task_id': 0,
                'status': managed_job_state.ManagedJobStatus.SUCCEEDED,
                'task_name': 'extract',
                'submitted_at': 100.0,
                'start_at': 110.0,
                'last_recovered_at': 110.0,
            }, {
                'task_id': 1,
                'status': managed_job_state.ManagedJobStatus.RUNNING,
                'task_name': 'transform',
                'submitted_at': 120.0,
                'start_at': 130.0,
                'last_recovered_at': 130.0,
            }],
        }
    }
    _forbid_split_snapshot_helpers(monkeypatch)
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: snapshot)
    monkeypatch.setattr(
        managed_job_state, 'get_job_status_check_state',
        lambda job_id: _make_job_status_check_state(
            managed_job_state.ManagedJobScheduleState.ALIVE))
    monkeypatch.setattr(
        managed_job_state, 'get_managed_job_tasks', lambda job_id: [{
            'job_name': 'pipeline-job',
            'task_name': 'extract',
            'pool': None,
        }, {
            'job_name': 'pipeline-job',
            'task_name': 'transform',
            'pool': None,
        }])
    monkeypatch.setattr(utils, 'controller_process_alive', lambda record: False)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    seen_task_names = []
    monkeypatch.setattr(
        utils, 'generate_managed_job_cluster_name', lambda task_name, job_id:
        seen_task_names.append(task_name) or f'{task_name}-{job_id}')
    monkeypatch.setattr(utils, 'terminate_cluster', lambda cluster_name: None)
    monkeypatch.setattr(
        managed_job_state, 'set_failed_controller_if_current_snapshot',
        lambda *a, **k: set_failed_calls.append(
            (a, k)) or managed_job_state.ControllerFailureDecision.TERMINALIZED)
    monkeypatch.setattr(managed_job_state,
                        'finish_controller_cleanup_if_current_snapshot',
                        lambda *a, **k: job_done_calls.append((a, k)) or True)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert seen_task_names == ['extract', 'transform']
    assert len(set_failed_calls) == 1
    assert len(job_done_calls) == 1


def test_cleanup_terminates_task_clusters_in_parallel(monkeypatch):
    """Multi-task cluster teardowns must overlap, not run serially.

    A single teardown can take minutes; a serial walk over a JobGroup's task
    clusters holds the refresh tick for the sum of all teardowns and widens
    the window in which the batched status snapshot goes stale. Each task has
    a distinct cluster, so the terminations are independent.
    """
    set_failed_calls, job_done_calls = [], []
    _wire_dead_controller(monkeypatch, set_failed_calls, job_done_calls)
    monkeypatch.setattr(utils, '_controller_is_restarting', lambda: False)
    info = _make_status_check_info()
    info[1]['tasks'] = [{
        'task_id': i,
        'status': managed_job_state.ManagedJobStatus.RUNNING,
        'task_name': name,
        'submitted_at': 100.0 + i,
        'start_at': 110.0 + i,
        'last_recovered_at': 110.0 + i,
    } for i, name in enumerate(['task-a', 'task-b', 'task-c'])]
    monkeypatch.setattr(managed_job_state,
                        'get_jobs_to_check_status_info',
                        lambda job_id=None: info)
    monkeypatch.setattr(utils, 'generate_managed_job_cluster_name',
                        lambda task_name, job_id: f'{task_name}-{job_id}')

    barrier = threading.Barrier(3, timeout=10)
    terminated = []

    def _terminate(cluster_name):
        # Every teardown blocks until all three are in flight at once. With
        # the old serial loop the first call would wait on the barrier alone
        # and time out.
        barrier.wait()
        terminated.append(cluster_name)

    monkeypatch.setattr(utils, 'terminate_cluster', _terminate)

    utils.update_managed_jobs_statuses(job_ids=[1])

    assert sorted(terminated) == ['task-a-1', 'task-b-1', 'task-c-1']
    assert len(set_failed_calls) == 1
    assert 'cleanup failed' not in set_failed_calls[0][1]['failure_reason']
    assert len(job_done_calls) == 1
