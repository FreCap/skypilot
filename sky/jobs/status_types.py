"""Managed-job lifecycle domain types."""

import collections
import enum
import typing
from typing import Optional

import colorama

from sky.adaptors import common as adaptors_common

if typing.TYPE_CHECKING:
    from sky.schemas.generated import managed_jobsv1_pb2
else:
    managed_jobsv1_pb2 = adaptors_common.LazyImport(
        'sky.schemas.generated.managed_jobsv1_pb2')


class ManagedJobStatus(enum.Enum):
    """Managed job status, designed to be in serverless style.

    The ManagedJobStatus is a higher level status than the JobStatus.
    Each managed job submitted to a cluster will have a JobStatus
    on that cluster:
        JobStatus = [INIT, SETTING_UP, PENDING, RUNNING, ...]
    Whenever the cluster is preempted and recovered, the JobStatus
    will go through the statuses above again.
    That means during the lifetime of a managed job, its JobsStatus could be
    reset to INIT or SETTING_UP multiple times (depending on the preemptions).

    However, a managed job only has one ManagedJobStatus on the jobs controller.
        ManagedJobStatus = [PENDING, STARTING, RUNNING, ...]
    Mapping from JobStatus to ManagedJobStatus:
        INIT            ->  STARTING/RECOVERING
        SETTING_UP      ->  RUNNING
        PENDING         ->  RUNNING
        RUNNING         ->  RUNNING
        SUCCEEDED       ->  SUCCEEDED
        FAILED          ->  FAILED
        FAILED_SETUP    ->  FAILED_SETUP
    Not all statuses are in this list, since some ManagedJobStatuses are only
    possible while the cluster is INIT/STOPPED/not yet UP.
    Note that the JobStatus will not be stuck in PENDING, because each cluster
    is dedicated to a managed job, i.e. there should always be enough resource
    to run the job and the job will be immediately transitioned to RUNNING.

    You can see a state diagram for ManagedJobStatus in sky/jobs/README.md.
    """
    # PENDING: Waiting for the jobs controller to have a slot to run the
    # controller process.
    PENDING = 'PENDING'
    # SUBMITTED: This state used to be briefly set before immediately changing
    # to STARTING. Its use was removed in #5682. We keep it for backwards
    # compatibility, so we can still parse old jobs databases that may have jobs
    # in this state.
    # TODO(cooperc): remove this in v0.12.0
    DEPRECATED_SUBMITTED = 'SUBMITTED'
    # The submitted_at timestamp of the managed job in the 'spot' table will be
    # set to the time when the job controller begins running.
    # STARTING: The controller process is launching the cluster for the managed
    # job.
    STARTING = 'STARTING'
    # RUNNING: The job is submitted to the cluster, and is setting up or
    # running.
    # The start_at timestamp of the managed job in the 'spot' table will be set
    # to the time when the job is firstly transitioned to RUNNING.
    RUNNING = 'RUNNING'
    # WINDING_DOWN: All batches are done; the coordinator is waiting for
    # worker threads to finish and merging per-batch output files.
    WINDING_DOWN = 'WINDING_DOWN'
    # RECOVERING: The cluster is preempted, and the controller process is
    # recovering the cluster (relaunching/failover).
    RECOVERING = 'RECOVERING'
    # CANCELLING: The job is requested to be cancelled by the user, and the
    # controller is cleaning up the cluster.
    CANCELLING = 'CANCELLING'
    # Terminal statuses
    # SUCCEEDED: The job is finished successfully.
    SUCCEEDED = 'SUCCEEDED'
    # CANCELLED: The job is cancelled by the user. When the managed job is in
    # CANCELLED status, the cluster has been cleaned up.
    CANCELLED = 'CANCELLED'
    # FAILED: The job is finished with failure from the user's program.
    FAILED = 'FAILED'
    # FAILED_SETUP: The job is finished with failure during setup -- either the
    # user's setup script, or a deterministic cluster/runtime setup failure such
    # as the job's pod being OOMKilled before the job started.
    FAILED_SETUP = 'FAILED_SETUP'
    # FAILED_PRECHECKS: the underlying `sky.launch` fails due to precheck
    # errors only. I.e., none of the failover exceptions, if any, is due to
    # resources unavailability. This exception includes the following cases:
    # 1. The optimizer cannot find a feasible solution.
    # 2. Precheck errors: invalid cluster name, failure in getting cloud user
    #    identity, or unsupported feature.
    FAILED_PRECHECKS = 'FAILED_PRECHECKS'
    # FAILED_NO_RESOURCE: The job is finished with failure because there is no
    # resource available in the cloud provider(s) to launch the cluster.
    FAILED_NO_RESOURCE = 'FAILED_NO_RESOURCE'
    # FAILED_CONTROLLER: The job is finished with failure because of unexpected
    # error in the controller process.
    FAILED_CONTROLLER = 'FAILED_CONTROLLER'

    def is_terminal(self) -> bool:
        return self in self.terminal_statuses()

    def is_failed(self) -> bool:
        return self in self.failure_statuses()

    def colored_str(self) -> str:
        color = _SPOT_STATUS_TO_COLOR[self]
        return f'{color}{self.value}{colorama.Style.RESET_ALL}'

    def __lt__(self, other) -> bool:
        status_list = list(ManagedJobStatus)
        return status_list.index(self) < status_list.index(other)

    @classmethod
    def terminal_statuses(cls) -> list['ManagedJobStatus']:
        return [
            cls.SUCCEEDED,
            cls.FAILED,
            cls.FAILED_SETUP,
            cls.FAILED_PRECHECKS,
            cls.FAILED_NO_RESOURCE,
            cls.FAILED_CONTROLLER,
            cls.CANCELLED,
        ]

    @classmethod
    def failure_statuses(cls) -> list['ManagedJobStatus']:
        return [
            cls.FAILED, cls.FAILED_SETUP, cls.FAILED_PRECHECKS,
            cls.FAILED_NO_RESOURCE, cls.FAILED_CONTROLLER
        ]

    @classmethod
    def processing_statuses(cls) -> list['ManagedJobStatus']:
        # Any status that is not terminal and is not CANCELLING.
        return [
            cls.PENDING,
            cls.STARTING,
            cls.RUNNING,
            cls.WINDING_DOWN,
            cls.RECOVERING,
        ]

    @classmethod
    def from_protobuf(
        cls, protobuf_value: 'managed_jobsv1_pb2.ManagedJobStatus'
    ) -> Optional['ManagedJobStatus']:
        """Convert protobuf ManagedJobStatus enum to Python enum value."""
        protobuf_to_enum = {
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_UNSPECIFIED: None,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_PENDING: cls.PENDING,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_SUBMITTED:
                cls.DEPRECATED_SUBMITTED,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_STARTING: cls.STARTING,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_RUNNING: cls.RUNNING,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_SUCCEEDED: cls.SUCCEEDED,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_FAILED: cls.FAILED,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_FAILED_CONTROLLER:
                cls.FAILED_CONTROLLER,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_FAILED_SETUP:
                cls.FAILED_SETUP,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_CANCELLED: cls.CANCELLED,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_RECOVERING: cls.RECOVERING,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_CANCELLING: cls.CANCELLING,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_FAILED_PRECHECKS:
                cls.FAILED_PRECHECKS,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_FAILED_NO_RESOURCE:
                cls.FAILED_NO_RESOURCE,
            managed_jobsv1_pb2.MANAGED_JOB_STATUS_WINDING_DOWN:
                cls.WINDING_DOWN,
        }

        if protobuf_value not in protobuf_to_enum:
            raise ValueError(
                f'Unknown protobuf ManagedJobStatus value: {protobuf_value}')

        return protobuf_to_enum[protobuf_value]

    def to_protobuf(self) -> 'managed_jobsv1_pb2.ManagedJobStatus':
        """Convert this Python enum value to protobuf enum value."""
        enum_to_protobuf = {
            ManagedJobStatus.PENDING:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_PENDING,
            ManagedJobStatus.DEPRECATED_SUBMITTED:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_SUBMITTED,
            ManagedJobStatus.STARTING:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_STARTING,
            ManagedJobStatus.RUNNING:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_RUNNING,
            ManagedJobStatus.SUCCEEDED:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_SUCCEEDED,
            ManagedJobStatus.FAILED:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_FAILED,
            ManagedJobStatus.FAILED_CONTROLLER:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_FAILED_CONTROLLER,
            ManagedJobStatus.FAILED_SETUP:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_FAILED_SETUP,
            ManagedJobStatus.CANCELLED:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_CANCELLED,
            ManagedJobStatus.RECOVERING:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_RECOVERING,
            ManagedJobStatus.CANCELLING:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_CANCELLING,
            ManagedJobStatus.FAILED_PRECHECKS:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_FAILED_PRECHECKS,
            ManagedJobStatus.FAILED_NO_RESOURCE:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_FAILED_NO_RESOURCE,
            ManagedJobStatus.WINDING_DOWN:
                managed_jobsv1_pb2.MANAGED_JOB_STATUS_WINDING_DOWN,
        }

        if self not in enum_to_protobuf:
            raise ValueError(f'Unknown ManagedJobStatus value: {self}')

        return enum_to_protobuf[self]


class BatchLifecycleTransition(enum.Enum):
    """Outcome of an owner-fenced Batch lifecycle transition."""

    APPLIED = 'APPLIED'
    ALREADY_TARGET = 'ALREADY_TARGET'
    OWNER_LOST = 'OWNER_LOST'
    INVALID_STATE = 'INVALID_STATE'


_SPOT_STATUS_TO_COLOR = {
    ManagedJobStatus.PENDING: colorama.Fore.BLUE,
    ManagedJobStatus.STARTING: colorama.Fore.BLUE,
    ManagedJobStatus.RUNNING: colorama.Fore.GREEN,
    ManagedJobStatus.WINDING_DOWN: colorama.Fore.CYAN,
    ManagedJobStatus.RECOVERING: colorama.Fore.CYAN,
    ManagedJobStatus.SUCCEEDED: colorama.Fore.GREEN,
    ManagedJobStatus.FAILED: colorama.Fore.RED,
    ManagedJobStatus.FAILED_PRECHECKS: colorama.Fore.RED,
    ManagedJobStatus.FAILED_SETUP: colorama.Fore.RED,
    ManagedJobStatus.FAILED_NO_RESOURCE: colorama.Fore.RED,
    ManagedJobStatus.FAILED_CONTROLLER: colorama.Fore.RED,
    ManagedJobStatus.CANCELLING: colorama.Fore.YELLOW,
    ManagedJobStatus.CANCELLED: colorama.Fore.YELLOW,
    # TODO(cooperc): backwards compatibility, remove this in v0.12.0
    ManagedJobStatus.DEPRECATED_SUBMITTED: colorama.Fore.BLUE,
}


class ManagedJobScheduleState(enum.Enum):
    """Captures the state of the job from the scheduler's perspective.

    A newly created job will be INACTIVE.  The following transitions are valid:
    - INACTIVE -> WAITING: The job is "submitted" to the scheduler, and its job
      controller can be started.
    - WAITING -> LAUNCHING: The job controller is starting by the scheduler and
      may proceed to sky.launch.
    - LAUNCHING -> ALIVE: The launch attempt was completed. It may have
      succeeded or failed. The job controller is not allowed to sky.launch again
      without transitioning to ALIVE_WAITING and then LAUNCHING.
    - LAUNCHING -> ALIVE_BACKOFF: The launch failed to find resources, and is
      in backoff waiting for resources.
    - ALIVE -> ALIVE_WAITING: The job controller wants to sky.launch again,
      either for recovery or to launch a subsequent task.
    - ALIVE_BACKOFF -> ALIVE_WAITING: The backoff period has ended, and the job
      controller wants to try to launch again.
    - ALIVE_WAITING -> LAUNCHING: The scheduler has determined that the job
      controller may launch again.
    - LAUNCHING, ALIVE, or ALIVE_WAITING -> DONE: The job controller is exiting
      and the job is in some terminal status. In the future it may be possible
      to transition directly from WAITING or even INACTIVE to DONE if the job is
      cancelled.

    You can see a state diagram in sky/jobs/README.md.

    There is no well-defined mapping from the managed job status to schedule
    state or vice versa. (In fact, schedule state is defined on the job and
    status on the task.)
    - INACTIVE or WAITING should only be seen when a job is PENDING.
    - ALIVE_BACKOFF should only be seen when a job is STARTING.
    - ALIVE_WAITING should only be seen when a job is RECOVERING, has multiple
      tasks, or needs to retry launching.
    - LAUNCHING and ALIVE can be seen in many different statuses.
    - DONE should only be seen when a job is in a terminal status.
    Since state and status transitions are not atomic, it may be possible to
    briefly observe inconsistent states, like a job that just finished but
    hasn't yet transitioned to DONE.
    """
    # TODO(luca): the only states we need are INACTIVE, WAITING, ALIVE, and
    # DONE. ALIVE = old LAUNCHING + ALIVE + ALIVE_BACKOFF + ALIVE_WAITING and
    # will represent jobs that are claimed by a controller. Delete the rest
    # in v0.13.0
    # The job should be ignored by the scheduler.
    INACTIVE = 'INACTIVE'
    # The job is waiting to transition to LAUNCHING for the first time. The
    # scheduler should try to transition it, and when it does, it should start
    # the job controller.
    WAITING = 'WAITING'
    # The job is already alive, but wants to transition back to LAUNCHING,
    # e.g. for recovery, or launching later tasks in the DAG. The scheduler
    # should try to transition it to LAUNCHING.
    ALIVE_WAITING = 'ALIVE_WAITING'
    # The job is running sky.launch, or soon will, using a limited number of
    # allowed launch slots.
    LAUNCHING = 'LAUNCHING'
    # The job is alive, but is in backoff waiting for resources - a special case
    # of ALIVE.
    ALIVE_BACKOFF = 'ALIVE_BACKOFF'
    # The controller for the job is running, but it's not currently launching.
    ALIVE = 'ALIVE'
    # The job is in a terminal state. (Not necessarily SUCCEEDED.)
    DONE = 'DONE'

    @classmethod
    def from_protobuf(
        cls, protobuf_value: 'managed_jobsv1_pb2.ManagedJobScheduleState'
    ) -> Optional['ManagedJobScheduleState']:
        """Convert protobuf ManagedJobScheduleState enum to Python enum value.
        """
        protobuf_to_enum = {
            managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_UNSPECIFIED: None,
            # TODO(cooperc): remove this in v0.13.0. See #8105.
            managed_jobsv1_pb2.DEPRECATED_MANAGED_JOB_SCHEDULE_STATE_INVALID:
                (None),
            managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_INACTIVE:
                cls.INACTIVE,
            managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_WAITING: cls.WAITING,
            managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_ALIVE_WAITING:
                cls.ALIVE_WAITING,
            managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_LAUNCHING:
                cls.LAUNCHING,
            managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_ALIVE_BACKOFF:
                cls.ALIVE_BACKOFF,
            managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_ALIVE: cls.ALIVE,
            managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_DONE: cls.DONE,
        }

        if protobuf_value not in protobuf_to_enum:
            raise ValueError('Unknown protobuf ManagedJobScheduleState value: '
                             f'{protobuf_value}')

        return protobuf_to_enum[protobuf_value]

    def to_protobuf(self) -> 'managed_jobsv1_pb2.ManagedJobScheduleState':
        """Convert this Python enum value to protobuf enum value."""
        enum_to_protobuf = {
            ManagedJobScheduleState.INACTIVE:
                managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_INACTIVE,
            ManagedJobScheduleState.WAITING:
                managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_WAITING,
            ManagedJobScheduleState.ALIVE_WAITING:
                managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_ALIVE_WAITING,
            ManagedJobScheduleState.LAUNCHING:
                managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_LAUNCHING,
            ManagedJobScheduleState.ALIVE_BACKOFF:
                managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_ALIVE_BACKOFF,
            ManagedJobScheduleState.ALIVE:
                managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_ALIVE,
            ManagedJobScheduleState.DONE:
                managed_jobsv1_pb2.MANAGED_JOB_SCHEDULE_STATE_DONE,
        }

        if self not in enum_to_protobuf:
            raise ValueError(f'Unknown ManagedJobScheduleState value: {self}')

        return enum_to_protobuf[self]


ControllerPidRecord = collections.namedtuple('ControllerPidRecord', [
    'pid',
    'started_at',
])


class JobCancellationState(typing.NamedTuple):
    """State needed to authorize and route a managed-job cancellation."""
    status: ManagedJobStatus
    workspace: str


class JobLogStreamSnapshot(typing.NamedTuple):
    """Latest-task status and routing fields for one log-follow poll."""
    task_id: int | None
    status: ManagedJobStatus | None
    pool: str | None
    cluster_name: str | None
    job_id_on_pool_cluster: int | None
    task_name: str | None


# These types were historically defined in sky.jobs.state. Keep their module
# identity stable for existing imports and serialized values while state.py
# remains the public facade.
for _status_type in (ManagedJobStatus, BatchLifecycleTransition,
                     ManagedJobScheduleState, ControllerPidRecord,
                     JobCancellationState, JobLogStreamSnapshot):
    _status_type.__module__ = 'sky.jobs.state'
