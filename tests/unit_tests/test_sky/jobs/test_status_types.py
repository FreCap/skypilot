"""Characterization tests for managed-job status domain types."""

import pickle

from sky import jobs
from sky.jobs import state
from sky.jobs import status_types
from sky.schemas.generated import managed_jobsv1_pb2


def test_status_types_preserve_historical_module_and_pickle_identity():
    samples = [
        state.ManagedJobStatus.PENDING,
        state.BatchLifecycleTransition.APPLIED,
        state.ManagedJobScheduleState.INACTIVE,
        state.ControllerPidRecord(pid=123, started_at=1.5),
        state.JobCancellationState(status=state.ManagedJobStatus.RUNNING,
                                   workspace='workspace'),
    ]

    for sample in samples:
        assert type(sample).__module__ == 'sky.jobs.state'
        assert pickle.loads(pickle.dumps(sample)) == sample

    assert state.ManagedJobStatus is status_types.ManagedJobStatus
    assert jobs.ManagedJobStatus is state.ManagedJobStatus
    assert (state.BatchLifecycleTransition
            is status_types.BatchLifecycleTransition)
    assert state.ManagedJobScheduleState is status_types.ManagedJobScheduleState
    assert state.ControllerPidRecord is status_types.ControllerPidRecord
    assert state.JobCancellationState is status_types.JobCancellationState


def test_managed_job_status_contract():
    assert state.ManagedJobStatus.terminal_statuses() == [
        state.ManagedJobStatus.SUCCEEDED,
        state.ManagedJobStatus.FAILED,
        state.ManagedJobStatus.FAILED_SETUP,
        state.ManagedJobStatus.FAILED_PRECHECKS,
        state.ManagedJobStatus.FAILED_NO_RESOURCE,
        state.ManagedJobStatus.FAILED_CONTROLLER,
        state.ManagedJobStatus.CANCELLED,
    ]
    assert state.ManagedJobStatus.failure_statuses() == [
        state.ManagedJobStatus.FAILED,
        state.ManagedJobStatus.FAILED_SETUP,
        state.ManagedJobStatus.FAILED_PRECHECKS,
        state.ManagedJobStatus.FAILED_NO_RESOURCE,
        state.ManagedJobStatus.FAILED_CONTROLLER,
    ]
    assert state.ManagedJobStatus.processing_statuses() == [
        state.ManagedJobStatus.PENDING,
        state.ManagedJobStatus.STARTING,
        state.ManagedJobStatus.RUNNING,
        state.ManagedJobStatus.WINDING_DOWN,
        state.ManagedJobStatus.RECOVERING,
    ]

    for status in state.ManagedJobStatus:
        assert state.ManagedJobStatus.from_protobuf(
            status.to_protobuf()) is status
    assert state.ManagedJobStatus.from_protobuf(
        managed_jobsv1_pb2.MANAGED_JOB_STATUS_UNSPECIFIED) is None


def test_managed_job_schedule_state_protobuf_contract():
    for schedule_state in state.ManagedJobScheduleState:
        assert state.ManagedJobScheduleState.from_protobuf(
            schedule_state.to_protobuf()) is schedule_state
    assert state.ManagedJobScheduleState.from_protobuf(
        managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_UNSPECIFIED) is None
    assert state.ManagedJobScheduleState.from_protobuf(
        managed_jobsv1_pb2.DEPRECATED_MANAGED_JOB_SCHEDULE_STATE_INVALID
    ) is None


def test_status_snapshot_shapes():
    controller = state.ControllerPidRecord(pid=123, started_at=1.5)
    assert controller._fields == ('pid', 'started_at')

    cancellation = state.JobCancellationState(
        status=state.ManagedJobStatus.RUNNING, workspace='workspace')
    assert cancellation._fields == ('status', 'workspace')
