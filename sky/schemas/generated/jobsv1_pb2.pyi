from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class JobStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    JOB_STATUS_UNSPECIFIED: _ClassVar[JobStatus]
    JOB_STATUS_INIT: _ClassVar[JobStatus]
    JOB_STATUS_PENDING: _ClassVar[JobStatus]
    JOB_STATUS_SETTING_UP: _ClassVar[JobStatus]
    JOB_STATUS_RUNNING: _ClassVar[JobStatus]
    JOB_STATUS_FAILED_DRIVER: _ClassVar[JobStatus]
    JOB_STATUS_SUCCEEDED: _ClassVar[JobStatus]
    JOB_STATUS_FAILED: _ClassVar[JobStatus]
    JOB_STATUS_FAILED_SETUP: _ClassVar[JobStatus]
    JOB_STATUS_CANCELLED: _ClassVar[JobStatus]

class JobSystemRecoveryPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    JOB_SYSTEM_RECOVERY_PHASE_UNSPECIFIED: _ClassVar[JobSystemRecoveryPhase]
    JOB_SYSTEM_RECOVERY_PHASE_ARMED: _ClassVar[JobSystemRecoveryPhase]
    JOB_SYSTEM_RECOVERY_PHASE_WAITING_CLEANUP: _ClassVar[JobSystemRecoveryPhase]
    JOB_SYSTEM_RECOVERY_PHASE_WAITING_MEMORY: _ClassVar[JobSystemRecoveryPhase]
    JOB_SYSTEM_RECOVERY_PHASE_RESUBMITTING: _ClassVar[JobSystemRecoveryPhase]
    JOB_SYSTEM_RECOVERY_PHASE_RETRY_SUBMITTED: _ClassVar[JobSystemRecoveryPhase]
    JOB_SYSTEM_RECOVERY_PHASE_EXHAUSTED: _ClassVar[JobSystemRecoveryPhase]

class JobSystemRecoveryDetailStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    JOB_SYSTEM_RECOVERY_DETAIL_STATUS_UNSPECIFIED: _ClassVar[JobSystemRecoveryDetailStatus]
    JOB_SYSTEM_RECOVERY_DETAIL_STATUS_ABSENT: _ClassVar[JobSystemRecoveryDetailStatus]
    JOB_SYSTEM_RECOVERY_DETAIL_STATUS_PRESENT: _ClassVar[JobSystemRecoveryDetailStatus]
    JOB_SYSTEM_RECOVERY_DETAIL_STATUS_MALFORMED: _ClassVar[JobSystemRecoveryDetailStatus]
JOB_STATUS_UNSPECIFIED: JobStatus
JOB_STATUS_INIT: JobStatus
JOB_STATUS_PENDING: JobStatus
JOB_STATUS_SETTING_UP: JobStatus
JOB_STATUS_RUNNING: JobStatus
JOB_STATUS_FAILED_DRIVER: JobStatus
JOB_STATUS_SUCCEEDED: JobStatus
JOB_STATUS_FAILED: JobStatus
JOB_STATUS_FAILED_SETUP: JobStatus
JOB_STATUS_CANCELLED: JobStatus
JOB_SYSTEM_RECOVERY_PHASE_UNSPECIFIED: JobSystemRecoveryPhase
JOB_SYSTEM_RECOVERY_PHASE_ARMED: JobSystemRecoveryPhase
JOB_SYSTEM_RECOVERY_PHASE_WAITING_CLEANUP: JobSystemRecoveryPhase
JOB_SYSTEM_RECOVERY_PHASE_WAITING_MEMORY: JobSystemRecoveryPhase
JOB_SYSTEM_RECOVERY_PHASE_RESUBMITTING: JobSystemRecoveryPhase
JOB_SYSTEM_RECOVERY_PHASE_RETRY_SUBMITTED: JobSystemRecoveryPhase
JOB_SYSTEM_RECOVERY_PHASE_EXHAUSTED: JobSystemRecoveryPhase
JOB_SYSTEM_RECOVERY_DETAIL_STATUS_UNSPECIFIED: JobSystemRecoveryDetailStatus
JOB_SYSTEM_RECOVERY_DETAIL_STATUS_ABSENT: JobSystemRecoveryDetailStatus
JOB_SYSTEM_RECOVERY_DETAIL_STATUS_PRESENT: JobSystemRecoveryDetailStatus
JOB_SYSTEM_RECOVERY_DETAIL_STATUS_MALFORMED: JobSystemRecoveryDetailStatus

class AddJobRequest(_message.Message):
    __slots__ = ("job_name", "username", "run_timestamp", "resources_str", "metadata")
    JOB_NAME_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    RUN_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_STR_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    job_name: str
    username: str
    run_timestamp: str
    resources_str: str
    metadata: str
    def __init__(self, job_name: _Optional[str] = ..., username: _Optional[str] = ..., run_timestamp: _Optional[str] = ..., resources_str: _Optional[str] = ..., metadata: _Optional[str] = ...) -> None: ...

class AddJobResponse(_message.Message):
    __slots__ = ("job_id", "log_dir")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    LOG_DIR_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    log_dir: str
    def __init__(self, job_id: _Optional[int] = ..., log_dir: _Optional[str] = ...) -> None: ...

class QueueJobRequest(_message.Message):
    __slots__ = ("job_id", "codegen", "script_path", "remote_log_dir", "managed_job")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    CODEGEN_FIELD_NUMBER: _ClassVar[int]
    SCRIPT_PATH_FIELD_NUMBER: _ClassVar[int]
    REMOTE_LOG_DIR_FIELD_NUMBER: _ClassVar[int]
    MANAGED_JOB_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    codegen: str
    script_path: str
    remote_log_dir: str
    managed_job: ManagedJobInfo
    def __init__(self, job_id: _Optional[int] = ..., codegen: _Optional[str] = ..., script_path: _Optional[str] = ..., remote_log_dir: _Optional[str] = ..., managed_job: _Optional[_Union[ManagedJobInfo, _Mapping]] = ...) -> None: ...

class ManagedJobInfo(_message.Message):
    __slots__ = ("name", "pool", "workspace", "entrypoint", "tasks", "user_id", "execution")
    NAME_FIELD_NUMBER: _ClassVar[int]
    POOL_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    ENTRYPOINT_FIELD_NUMBER: _ClassVar[int]
    TASKS_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_FIELD_NUMBER: _ClassVar[int]
    name: str
    pool: str
    workspace: str
    entrypoint: str
    tasks: _containers.RepeatedCompositeFieldContainer[ManagedJobTask]
    user_id: str
    execution: str
    def __init__(self, name: _Optional[str] = ..., pool: _Optional[str] = ..., workspace: _Optional[str] = ..., entrypoint: _Optional[str] = ..., tasks: _Optional[_Iterable[_Union[ManagedJobTask, _Mapping]]] = ..., user_id: _Optional[str] = ..., execution: _Optional[str] = ...) -> None: ...

class ManagedJobTask(_message.Message):
    __slots__ = ("task_id", "name", "resources_str", "metadata_json", "is_primary_in_job_group")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_STR_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    IS_PRIMARY_IN_JOB_GROUP_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    name: str
    resources_str: str
    metadata_json: str
    is_primary_in_job_group: bool
    def __init__(self, task_id: _Optional[int] = ..., name: _Optional[str] = ..., resources_str: _Optional[str] = ..., metadata_json: _Optional[str] = ..., is_primary_in_job_group: bool = ...) -> None: ...

class QueueJobResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UpdateStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UpdateStatusResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetJobQueueRequest(_message.Message):
    __slots__ = ("user_hash", "all_jobs")
    USER_HASH_FIELD_NUMBER: _ClassVar[int]
    ALL_JOBS_FIELD_NUMBER: _ClassVar[int]
    user_hash: str
    all_jobs: bool
    def __init__(self, user_hash: _Optional[str] = ..., all_jobs: bool = ...) -> None: ...

class JobInfo(_message.Message):
    __slots__ = ("job_id", "job_name", "username", "submitted_at", "status", "run_timestamp", "start_at", "end_at", "resources", "pid", "log_path", "metadata")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_NAME_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    SUBMITTED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RUN_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    START_AT_FIELD_NUMBER: _ClassVar[int]
    END_AT_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    PID_FIELD_NUMBER: _ClassVar[int]
    LOG_PATH_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    job_name: str
    username: str
    submitted_at: float
    status: JobStatus
    run_timestamp: str
    start_at: float
    end_at: float
    resources: str
    pid: int
    log_path: str
    metadata: str
    def __init__(self, job_id: _Optional[int] = ..., job_name: _Optional[str] = ..., username: _Optional[str] = ..., submitted_at: _Optional[float] = ..., status: _Optional[_Union[JobStatus, str]] = ..., run_timestamp: _Optional[str] = ..., start_at: _Optional[float] = ..., end_at: _Optional[float] = ..., resources: _Optional[str] = ..., pid: _Optional[int] = ..., log_path: _Optional[str] = ..., metadata: _Optional[str] = ...) -> None: ...

class GetJobQueueResponse(_message.Message):
    __slots__ = ("jobs",)
    JOBS_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.RepeatedCompositeFieldContainer[JobInfo]
    def __init__(self, jobs: _Optional[_Iterable[_Union[JobInfo, _Mapping]]] = ...) -> None: ...

class CancelJobsRequest(_message.Message):
    __slots__ = ("job_ids", "cancel_all", "user_hash")
    JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    CANCEL_ALL_FIELD_NUMBER: _ClassVar[int]
    USER_HASH_FIELD_NUMBER: _ClassVar[int]
    job_ids: _containers.RepeatedScalarFieldContainer[int]
    cancel_all: bool
    user_hash: str
    def __init__(self, job_ids: _Optional[_Iterable[int]] = ..., cancel_all: bool = ..., user_hash: _Optional[str] = ...) -> None: ...

class CancelJobsResponse(_message.Message):
    __slots__ = ("cancelled_job_ids",)
    CANCELLED_JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    cancelled_job_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, cancelled_job_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class FailAllInProgressJobsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FailAllInProgressJobsResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TailLogsRequest(_message.Message):
    __slots__ = ("job_id", "managed_job_id", "follow", "tail", "tail_offset")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    MANAGED_JOB_ID_FIELD_NUMBER: _ClassVar[int]
    FOLLOW_FIELD_NUMBER: _ClassVar[int]
    TAIL_FIELD_NUMBER: _ClassVar[int]
    TAIL_OFFSET_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    managed_job_id: int
    follow: bool
    tail: int
    tail_offset: int
    def __init__(self, job_id: _Optional[int] = ..., managed_job_id: _Optional[int] = ..., follow: bool = ..., tail: _Optional[int] = ..., tail_offset: _Optional[int] = ...) -> None: ...

class TailLogsResponse(_message.Message):
    __slots__ = ("log_line", "exit_code")
    LOG_LINE_FIELD_NUMBER: _ClassVar[int]
    EXIT_CODE_FIELD_NUMBER: _ClassVar[int]
    log_line: str
    exit_code: int
    def __init__(self, log_line: _Optional[str] = ..., exit_code: _Optional[int] = ...) -> None: ...

class GetJobStatusRequest(_message.Message):
    __slots__ = ("job_ids",)
    JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    job_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, job_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class JobSystemRecoveryInfo(_message.Message):
    __slots__ = ("capability", "phase", "event_id", "original_attempt_id", "task_index", "replacement_attempt_id", "node_boot_id", "reason", "occurrence_count", "armed_at", "occurred_at", "updated_at", "deadline_at", "summary")
    CAPABILITY_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_INDEX_FIELD_NUMBER: _ClassVar[int]
    REPLACEMENT_ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_BOOT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_COUNT_FIELD_NUMBER: _ClassVar[int]
    ARMED_AT_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_AT_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    capability: str
    phase: JobSystemRecoveryPhase
    event_id: str
    original_attempt_id: str
    task_index: int
    replacement_attempt_id: str
    node_boot_id: str
    reason: str
    occurrence_count: int
    armed_at: float
    occurred_at: float
    updated_at: float
    deadline_at: float
    summary: str
    def __init__(self, capability: _Optional[str] = ..., phase: _Optional[_Union[JobSystemRecoveryPhase, str]] = ..., event_id: _Optional[str] = ..., original_attempt_id: _Optional[str] = ..., task_index: _Optional[int] = ..., replacement_attempt_id: _Optional[str] = ..., node_boot_id: _Optional[str] = ..., reason: _Optional[str] = ..., occurrence_count: _Optional[int] = ..., armed_at: _Optional[float] = ..., occurred_at: _Optional[float] = ..., updated_at: _Optional[float] = ..., deadline_at: _Optional[float] = ..., summary: _Optional[str] = ...) -> None: ...

class GetJobStatusResponse(_message.Message):
    __slots__ = ("job_statuses", "system_recovery_infos", "system_recovery_detail_statuses")
    class JobStatusesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: JobStatus
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[JobStatus, str]] = ...) -> None: ...
    class SystemRecoveryInfosEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: JobSystemRecoveryInfo
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[JobSystemRecoveryInfo, _Mapping]] = ...) -> None: ...
    class SystemRecoveryDetailStatusesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: JobSystemRecoveryDetailStatus
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[JobSystemRecoveryDetailStatus, str]] = ...) -> None: ...
    JOB_STATUSES_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_RECOVERY_INFOS_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_RECOVERY_DETAIL_STATUSES_FIELD_NUMBER: _ClassVar[int]
    job_statuses: _containers.ScalarMap[int, JobStatus]
    system_recovery_infos: _containers.MessageMap[int, JobSystemRecoveryInfo]
    system_recovery_detail_statuses: _containers.ScalarMap[int, JobSystemRecoveryDetailStatus]
    def __init__(self, job_statuses: _Optional[_Mapping[int, JobStatus]] = ..., system_recovery_infos: _Optional[_Mapping[int, JobSystemRecoveryInfo]] = ..., system_recovery_detail_statuses: _Optional[_Mapping[int, JobSystemRecoveryDetailStatus]] = ...) -> None: ...

class GetJobSubmittedTimestampRequest(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    def __init__(self, job_id: _Optional[int] = ...) -> None: ...

class GetJobSubmittedTimestampResponse(_message.Message):
    __slots__ = ("timestamp",)
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    timestamp: float
    def __init__(self, timestamp: _Optional[float] = ...) -> None: ...

class GetJobEndedTimestampRequest(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    def __init__(self, job_id: _Optional[int] = ...) -> None: ...

class GetJobEndedTimestampResponse(_message.Message):
    __slots__ = ("timestamp",)
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    timestamp: float
    def __init__(self, timestamp: _Optional[float] = ...) -> None: ...

class GetLogDirsForJobsRequest(_message.Message):
    __slots__ = ("job_ids",)
    JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    job_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, job_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class GetLogDirsForJobsResponse(_message.Message):
    __slots__ = ("job_log_dirs",)
    class JobLogDirsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: str
        def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...
    JOB_LOG_DIRS_FIELD_NUMBER: _ClassVar[int]
    job_log_dirs: _containers.ScalarMap[int, str]
    def __init__(self, job_log_dirs: _Optional[_Mapping[int, str]] = ...) -> None: ...

class GetJobExitCodesRequest(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: int
    def __init__(self, job_id: _Optional[int] = ...) -> None: ...

class GetJobExitCodesResponse(_message.Message):
    __slots__ = ("exit_codes",)
    EXIT_CODES_FIELD_NUMBER: _ClassVar[int]
    exit_codes: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, exit_codes: _Optional[_Iterable[int]] = ...) -> None: ...

class OptionalBool(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bool
    def __init__(self, value: bool = ...) -> None: ...

class SetJobInfoWithoutJobIdRequest(_message.Message):
    __slots__ = ("name", "workspace", "entrypoint", "pool", "pool_hash", "user_hash", "task_ids", "task_names", "resources_str", "metadata_jsons", "num_jobs", "execution", "is_primary_in_job_groups", "is_primary_in_job_groups_v2")
    NAME_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    ENTRYPOINT_FIELD_NUMBER: _ClassVar[int]
    POOL_FIELD_NUMBER: _ClassVar[int]
    POOL_HASH_FIELD_NUMBER: _ClassVar[int]
    USER_HASH_FIELD_NUMBER: _ClassVar[int]
    TASK_IDS_FIELD_NUMBER: _ClassVar[int]
    TASK_NAMES_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_STR_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSONS_FIELD_NUMBER: _ClassVar[int]
    NUM_JOBS_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_FIELD_NUMBER: _ClassVar[int]
    IS_PRIMARY_IN_JOB_GROUPS_FIELD_NUMBER: _ClassVar[int]
    IS_PRIMARY_IN_JOB_GROUPS_V2_FIELD_NUMBER: _ClassVar[int]
    name: str
    workspace: str
    entrypoint: str
    pool: str
    pool_hash: str
    user_hash: str
    task_ids: _containers.RepeatedScalarFieldContainer[int]
    task_names: _containers.RepeatedScalarFieldContainer[str]
    resources_str: str
    metadata_jsons: _containers.RepeatedScalarFieldContainer[str]
    num_jobs: int
    execution: str
    is_primary_in_job_groups: _containers.RepeatedScalarFieldContainer[bool]
    is_primary_in_job_groups_v2: _containers.RepeatedCompositeFieldContainer[OptionalBool]
    def __init__(self, name: _Optional[str] = ..., workspace: _Optional[str] = ..., entrypoint: _Optional[str] = ..., pool: _Optional[str] = ..., pool_hash: _Optional[str] = ..., user_hash: _Optional[str] = ..., task_ids: _Optional[_Iterable[int]] = ..., task_names: _Optional[_Iterable[str]] = ..., resources_str: _Optional[str] = ..., metadata_jsons: _Optional[_Iterable[str]] = ..., num_jobs: _Optional[int] = ..., execution: _Optional[str] = ..., is_primary_in_job_groups: _Optional[_Iterable[bool]] = ..., is_primary_in_job_groups_v2: _Optional[_Iterable[_Union[OptionalBool, _Mapping]]] = ...) -> None: ...

class SetJobInfoWithoutJobIdResponse(_message.Message):
    __slots__ = ("job_ids",)
    JOB_IDS_FIELD_NUMBER: _ClassVar[int]
    job_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, job_ids: _Optional[_Iterable[int]] = ...) -> None: ...
