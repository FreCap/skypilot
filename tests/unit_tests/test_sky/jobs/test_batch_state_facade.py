"""Characterization tests for the historical Batch state facade."""

# pylint: disable=protected-access

import inspect

from sky.jobs import batch_state
from sky.jobs import state
from sky.jobs import state_storage

_EXPECTED_SIGNATURES = {
    'save_batch_states': "(job_id: int, batches: list[list[int]], owner_token: str) -> bool",
    'is_batch_job': "(job_id: int) -> bool",
    'acquire_batch_coordinator': "(job_id: int, owner_token: str) -> str | None",
    'is_batch_coordinator_owner': "(job_id: int, owner_token: str) -> bool",
    'get_batch_states': "(job_id: int) -> list[dict[str, typing.Any]]",
    'register_batch_worker_launch':
        ("(job_id: int, owner_token: str, worker_cluster: str, "
         "worker_job_name: str) -> bool"),
    'record_batch_worker_launch_request':
        ("(job_id: int, owner_token: str, worker_cluster: str, "
         "request_id: str) -> bool"),
    'record_batch_worker_job_id':
        ("(job_id: int, owner_token: str, worker_cluster: str, "
         "worker_job_id: int) -> bool"),
    'get_batch_worker_records':
        ("(job_id: int, owner_token: str | None = None) -> "
         "list[dict[str, typing.Any]]"),
    'remove_batch_worker_record':
        ("(job_id: int, owner_token: str, worker_cluster: str, "
         "worker_job_id: int | None = None) -> bool"),
    'claim_batch': ("(job_id: int, batch_idx: int, owner_token: str, "
                    "worker_cluster: str, lease_duration: float, "
                    "now: float | None = None) -> tuple[int, int] | None"),
    'renew_batch_lease':
        ("(job_id: int, batch_idx: int, attempt_id: int, owner_token: str, "
         "lease_duration: float, now: float | None = None) -> bool"),
    'set_batch_attempt_status':
        ("(job_id: int, batch_idx: int, attempt_id: int, owner_token: str, "
         "status: str, retry_count: int | None = None, "
         "next_retry_at: float | None = None, "
         "now: float | None = None) -> bool"),
    'requeue_expired_batch_attempts':
        ("(job_id: int, owner_token: str, "
         "now: float | None = None) -> list[int]"),
    'set_batch_winding_down':
        ("(job_id: int, task_id: int, owner_token: str) -> "
         "BatchLifecycleTransition"),
    'set_batch_succeeded':
        ("(job_id: int, task_id: int, owner_token: str, end_time: float) -> "
         "BatchLifecycleTransition"),
    'set_batch_failed': ("(job_id: int, task_id: int, owner_token: str, "
                         "failure_reason: str) -> "
                         "BatchLifecycleTransition"),
}


def test_batch_state_facade_signatures():
    actual = {
        name: str(inspect.signature(getattr(state, name))).replace(
            'sky.jobs.batch_state.BatchLifecycleTransition',
            'BatchLifecycleTransition').replace(
                'sky.jobs.state.BatchLifecycleTransition',
                'BatchLifecycleTransition') for name in _EXPECTED_SIGNATURES
    }
    assert actual == _EXPECTED_SIGNATURES


def test_batch_state_facade_uses_direct_aliases_and_shared_storage():
    for name in _EXPECTED_SIGNATURES:
        assert getattr(state, name) is getattr(batch_state, name)

    assert state.create_table is state_storage.create_table
    assert state._db_manager is state_storage.db_manager
    assert state.migration_utils is state_storage.migration_utils
    assert state.BatchLifecycleTransition is batch_state.BatchLifecycleTransition
    assert batch_state._db_manager is state_storage.db_manager
    assert batch_state.logger.name == state.logger.name == 'sky.jobs.state'
    assert batch_state.batch_state_table is state.batch_state_table
    assert batch_state.batch_worker_table is state.state_schema.batch_worker_table
