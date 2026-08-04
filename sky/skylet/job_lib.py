"""Utilities for jobs on a remote cluster, backed by a sqlite database.

This is a remote utility module that provides job queue functionality.
"""
from collections.abc import Iterator
from collections.abc import Sequence
import contextlib
import dataclasses
import enum
import functools
import getpass
import json
import math
import os
import pathlib
import signal
import sqlite3
import threading
import time
import typing
from typing import Any, Optional

import colorama
import filelock

from sky import global_user_state
from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.skylet import constants
from sky.skylet import job_lib_codegen
from sky.skylet import keyed_submission_state
from sky.skylet import runtime_utils
from sky.utils import common_utils
from sky.utils import message_utils
from sky.utils import subprocess_utils
from sky.utils.db import db_utils

if typing.TYPE_CHECKING:
    import psutil

    from sky.schemas.generated import jobsv1_pb2
else:
    psutil = adaptors_common.LazyImport('psutil')
    jobsv1_pb2 = adaptors_common.LazyImport('sky.schemas.generated.jobsv1_pb2')

logger = sky_logging.init_logger(__name__)

_JOB_STATUS_LOCK = '~/.sky/locks/.job_{}.lock'
JOB_SYSTEM_RECOVERY_API_VERSION = 1
_JOB_SYSTEM_RECOVERY_SCHEMA_VERSION = 1
JOB_SYSTEM_RECOVERY_SUMMARY_MAX_CHARS = 8192
_JOB_STATUS_SYSTEM_RECOVERY_PAYLOAD_VERSION = 1
# JOB_CMD_IDENTIFIER is used for identifying the process retrieved
# with pid is the same driver process to guard against the case where
# the same pid is reused by a different process.
JOB_CMD_IDENTIFIER = 'echo "SKYPILOT_JOB_ID <{}>"'


def _get_lock_path(job_id: int) -> str:
    lock_path = os.path.expanduser(_JOB_STATUS_LOCK.format(job_id))
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    return lock_path


@contextlib.contextmanager
def job_status_lock(job_id: int) -> Iterator[None]:
    """Acquires the lock shared by a job's status and recovery state."""
    # TODO(mraheja): remove pylint disabling when filelock version updated.
    # pylint: disable=abstract-class-instantiated
    with filelock.FileLock(_get_lock_path(job_id)):
        yield


class JobInfoLoc(enum.IntEnum):
    """Job Info's Location in the DB record"""
    JOB_ID = 0
    JOB_NAME = 1
    USERNAME = 2
    SUBMITTED_AT = 3
    STATUS = 4
    RUN_TIMESTAMP = 5
    START_AT = 6
    END_AT = 7
    RESOURCES = 8
    PID = 9
    LOG_PATH = 10
    METADATA = 11
    EXIT_CODES = 12


def create_table(cursor, conn):
    # Enable WAL mode to avoid locking issues.
    # See: issue #3863, #1441 and PR #1509
    # https://github.com/microsoft/WSL/issues/2395
    # TODO(romilb): We do not enable WAL for WSL because of known issue in WSL.
    #  This may cause the database locked problem from WSL issue #1441.
    if not common_utils.is_wsl():
        try:
            cursor.execute('PRAGMA journal_mode=WAL')
        except sqlite3.OperationalError as e:
            if 'database is locked' not in str(e):
                raise
            # If the database is locked, it is OK to continue, as the WAL mode
            # is not critical and is likely to be enabled by other processes.

    # Pid column is used for keeping track of the driver process of a job. It
    # can be in two states:
    # 0: The job driver process has never been started. When adding a job with
    #    INIT state, the pid will be set to 0.
    # >=0: The job has been started. The pid is the driver process's pid.
    #      The driver can be actually running or finished.
    # TODO(SKY-1213): username is actually user hash, should rename.
    cursor.execute("""\
        CREATE TABLE IF NOT EXISTS jobs (
        job_id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_name TEXT,
        username TEXT,
        submitted_at FLOAT,
        status TEXT,
        run_timestamp TEXT CANDIDATE KEY,
        start_at FLOAT DEFAULT -1,
        end_at FLOAT DEFAULT NULL,
        resources TEXT DEFAULT NULL,
        pid INTEGER DEFAULT -1,
        log_dir TEXT DEFAULT NULL,
        metadata TEXT DEFAULT '{}')""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS pending_jobs(
        job_id INTEGER,
        run_cmd TEXT,
        submit INTEGER,
        created_time INTEGER
    )""")

    # Keep system recovery state in a companion table instead of adding a
    # column to ``jobs``. Older skylets use positional INSERTs into ``jobs``;
    # preserving its exact layout makes a runtime rollback safe.
    cursor.execute("""CREATE TABLE IF NOT EXISTS job_system_recovery(
        job_id INTEGER PRIMARY KEY,
        info_json TEXT NOT NULL
    )""")

    db_utils.add_column_to_table(cursor, conn, 'jobs', 'end_at', 'FLOAT')
    db_utils.add_column_to_table(cursor, conn, 'jobs', 'resources', 'TEXT')
    db_utils.add_column_to_table(cursor, conn, 'jobs', 'pid',
                                 'INTEGER DEFAULT -1')
    db_utils.add_column_to_table(cursor, conn, 'jobs', 'log_dir',
                                 'TEXT DEFAULT NULL')
    db_utils.add_column_to_table(cursor,
                                 conn,
                                 'jobs',
                                 'metadata',
                                 'TEXT DEFAULT \'{}\'',
                                 value_to_replace_existing_entries='{}')
    db_utils.add_column_to_table(cursor, conn, 'jobs', 'exit_codes',
                                 'TEXT DEFAULT NULL')
    if not keyed_submission_state.create_tables(cursor, conn):
        logger.warning('Keyed submission schema is unavailable; continuing '
                       'with the legacy job schema.')
    conn.commit()


_DB = None
_db_init_lock = threading.Lock()


def init_db(func):
    """Initialize the database."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        global _DB
        if _DB is not None:
            return func(*args, **kwargs)

        with _db_init_lock:
            if _DB is None:
                db_path = runtime_utils.get_runtime_dir_path('.sky/jobs.db')
                os.makedirs(pathlib.Path(db_path).parents[0], exist_ok=True)
                _DB = db_utils.SQLiteConn(db_path, create_table)
        return func(*args, **kwargs)

    return wrapper


class JobStatus(enum.Enum):
    """Job status enum."""

    # 3 in-flux states: each can transition to any state below it.
    # The `job_id` has been generated, but the generated ray program has
    # not started yet. skylet can transit the state from INIT to FAILED
    # directly, if the ray program fails to start.
    # In the 'jobs' table, the `submitted_at` column will be set to the current
    # time, when the job is firstly created (in the INIT state).
    INIT = 'INIT'
    """The job has been submitted, but not started yet."""
    # The job is waiting for the required resources. (`ray job status`
    # shows RUNNING as the generated ray program has started, but blocked
    # by the placement constraints.)
    PENDING = 'PENDING'
    """The job is waiting for required resources."""
    # Running the user's setup script.
    # Our update_job_status() can temporarily (for a short period) set
    # the status to SETTING_UP, if the generated ray program has not set
    # the status to PENDING or RUNNING yet.
    SETTING_UP = 'SETTING_UP'
    """The job is running the user's setup script."""
    # The job is running.
    # In the 'jobs' table, the `start_at` column will be set to the current
    # time, when the job is firstly transitioned to RUNNING.
    RUNNING = 'RUNNING'
    """The job is running."""
    # The job driver process failed. This happens when the job driver process
    # finishes when the status in job table is still not set to terminal state.
    # We should keep this state before the SUCCEEDED, as our job status update
    # relies on the order of the statuses to keep the latest status.
    FAILED_DRIVER = 'FAILED_DRIVER'
    """The job driver process failed."""
    # 3 terminal states below: once reached, they do not transition.
    # The job finished successfully.
    SUCCEEDED = 'SUCCEEDED'
    """The job finished successfully."""
    # The job fails due to the user code or a system restart.
    FAILED = 'FAILED'
    """The job fails due to the user code."""
    # The job setup failed. It needs to be placed after the `FAILED` state,
    # so that the status set by our generated ray program will not be
    # overwritten by ray's job status (FAILED). This is for a better UX, so
    # that the user can find out the reason of the failure quickly.
    FAILED_SETUP = 'FAILED_SETUP'
    """The job setup failed."""
    # The job is cancelled by the user.
    CANCELLED = 'CANCELLED'
    """The job is cancelled by the user."""

    @classmethod
    def nonterminal_statuses(cls) -> list['JobStatus']:
        return [cls.INIT, cls.SETTING_UP, cls.PENDING, cls.RUNNING]

    def is_terminal(self) -> bool:
        return self not in self.nonterminal_statuses()

    @classmethod
    def user_code_failure_states(cls) -> Sequence['JobStatus']:
        return (cls.FAILED, cls.FAILED_SETUP)

    def __lt__(self, other: 'JobStatus') -> bool:
        return list(JobStatus).index(self) < list(JobStatus).index(other)

    def colored_str(self) -> str:
        color = _JOB_STATUS_TO_COLOR[self]
        return f'{color}{self.value}{colorama.Style.RESET_ALL}'

    @classmethod
    def from_protobuf(
            cls,
            protobuf_value: 'jobsv1_pb2.JobStatus') -> Optional['JobStatus']:
        """Convert protobuf JobStatus enum to Python enum value."""
        protobuf_to_enum = {
            jobsv1_pb2.JOB_STATUS_INIT: cls.INIT,
            jobsv1_pb2.JOB_STATUS_PENDING: cls.PENDING,
            jobsv1_pb2.JOB_STATUS_SETTING_UP: cls.SETTING_UP,
            jobsv1_pb2.JOB_STATUS_RUNNING: cls.RUNNING,
            jobsv1_pb2.JOB_STATUS_FAILED_DRIVER: cls.FAILED_DRIVER,
            jobsv1_pb2.JOB_STATUS_SUCCEEDED: cls.SUCCEEDED,
            jobsv1_pb2.JOB_STATUS_FAILED: cls.FAILED,
            jobsv1_pb2.JOB_STATUS_FAILED_SETUP: cls.FAILED_SETUP,
            jobsv1_pb2.JOB_STATUS_CANCELLED: cls.CANCELLED,
            jobsv1_pb2.JOB_STATUS_UNSPECIFIED: None,
        }
        if protobuf_value not in protobuf_to_enum:
            raise ValueError(
                f'Unknown protobuf JobStatus value: {protobuf_value}')
        return protobuf_to_enum[protobuf_value]

    def to_protobuf(self) -> 'jobsv1_pb2.JobStatus':
        """Convert this Python enum value to protobuf enum value."""
        enum_to_protobuf = {
            JobStatus.INIT: jobsv1_pb2.JOB_STATUS_INIT,
            JobStatus.PENDING: jobsv1_pb2.JOB_STATUS_PENDING,
            JobStatus.SETTING_UP: jobsv1_pb2.JOB_STATUS_SETTING_UP,
            JobStatus.RUNNING: jobsv1_pb2.JOB_STATUS_RUNNING,
            JobStatus.FAILED_DRIVER: jobsv1_pb2.JOB_STATUS_FAILED_DRIVER,
            JobStatus.SUCCEEDED: jobsv1_pb2.JOB_STATUS_SUCCEEDED,
            JobStatus.FAILED: jobsv1_pb2.JOB_STATUS_FAILED,
            JobStatus.FAILED_SETUP: jobsv1_pb2.JOB_STATUS_FAILED_SETUP,
            JobStatus.CANCELLED: jobsv1_pb2.JOB_STATUS_CANCELLED,
        }
        if self not in enum_to_protobuf:
            raise ValueError(f'Unknown JobStatus value: {self}')
        return enum_to_protobuf[self]


class JobSystemRecoveryPhase(enum.Enum):
    """Monotonic phases for a driver-owned system recovery attempt."""

    ARMED = 'ARMED'
    WAITING_CLEANUP = 'WAITING_CLEANUP'
    WAITING_MEMORY = 'WAITING_MEMORY'
    RESUBMITTING = 'RESUBMITTING'
    RETRY_SUBMITTED = 'RETRY_SUBMITTED'
    EXHAUSTED = 'EXHAUSTED'

    @classmethod
    def from_protobuf(
        cls,
        protobuf_value: 'jobsv1_pb2.JobSystemRecoveryPhase',
    ) -> Optional['JobSystemRecoveryPhase']:
        """Convert a protobuf enum value to a recovery phase."""
        protobuf_to_enum = {
            jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_ARMED: cls.ARMED,
            jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_WAITING_CLEANUP:
                cls.WAITING_CLEANUP,
            jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_WAITING_MEMORY:
                cls.WAITING_MEMORY,
            jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_RESUBMITTING: cls.RESUBMITTING,
            jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_RETRY_SUBMITTED:
                cls.RETRY_SUBMITTED,
            jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_EXHAUSTED: cls.EXHAUSTED,
        }
        return protobuf_to_enum.get(protobuf_value)

    def to_protobuf(self) -> 'jobsv1_pb2.JobSystemRecoveryPhase':
        """Convert to a protobuf enum value."""
        enum_to_protobuf = {
            JobSystemRecoveryPhase.ARMED:
                jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_ARMED,
            JobSystemRecoveryPhase.WAITING_CLEANUP:
                jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_WAITING_CLEANUP,
            JobSystemRecoveryPhase.WAITING_MEMORY:
                jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_WAITING_MEMORY,
            JobSystemRecoveryPhase.RESUBMITTING:
                jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_RESUBMITTING,
            JobSystemRecoveryPhase.RETRY_SUBMITTED:
                jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_RETRY_SUBMITTED,
            JobSystemRecoveryPhase.EXHAUSTED:
                jobsv1_pb2.JOB_SYSTEM_RECOVERY_PHASE_EXHAUSTED,
        }
        return enum_to_protobuf[self]


class JobSystemRecoveryDetailStatus(enum.Enum):
    """Validity/presence of one optional recovery detail record."""

    UNSPECIFIED = 'UNSPECIFIED'
    ABSENT = 'ABSENT'
    PRESENT = 'PRESENT'
    MALFORMED = 'MALFORMED'

    @classmethod
    def from_protobuf(
        cls,
        value: 'jobsv1_pb2.JobSystemRecoveryDetailStatus',
    ) -> 'JobSystemRecoveryDetailStatus':
        mapping = {
            jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_ABSENT: cls.ABSENT,
            jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_PRESENT: cls.PRESENT,
            jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_MALFORMED:
                cls.MALFORMED,
        }
        if value == jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_UNSPECIFIED:
            return cls.UNSPECIFIED
        return mapping.get(value, cls.MALFORMED)

    def to_protobuf(self) -> 'jobsv1_pb2.JobSystemRecoveryDetailStatus':
        mapping = {
            self.UNSPECIFIED:
                jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_UNSPECIFIED,
            self.ABSENT: jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_ABSENT,
            self.PRESENT: jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_PRESENT,
            self.MALFORMED:
                jobsv1_pb2.JOB_SYSTEM_RECOVERY_DETAIL_STATUS_MALFORMED,
        }
        return mapping[self]


@dataclasses.dataclass(frozen=True)
class JobSystemRecoveryInfo:
    """Persisted state for one driver-owned system recovery sequence."""

    capability: str
    phase: JobSystemRecoveryPhase
    original_attempt_id: str
    replacement_attempt_id: str | None
    task_index: int
    node_boot_id: str
    occurrence_count: int
    armed_at: float
    updated_at: float
    event_id: str | None = None
    reason: str | None = None
    occurred_at: float | None = None
    deadline_at: float | None = None
    summary: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.capability, str) or not self.capability:
            raise ValueError('capability must be non-empty')
        if not isinstance(self.phase, JobSystemRecoveryPhase):
            raise ValueError('phase must be a JobSystemRecoveryPhase')
        if (not isinstance(self.original_attempt_id, str) or
                not self.original_attempt_id):
            raise ValueError('original_attempt_id must be non-empty')
        if (self.replacement_attempt_id is not None and
            (not isinstance(self.replacement_attempt_id, str) or
             not self.replacement_attempt_id)):
            raise ValueError('replacement_attempt_id must be non-empty')
        if self.replacement_attempt_id == self.original_attempt_id:
            raise ValueError('replacement attempt must differ from original')
        if not isinstance(self.node_boot_id, str) or not self.node_boot_id:
            raise ValueError('node_boot_id must be non-empty')
        for name, value in (('event_id', self.event_id),
                            ('reason', self.reason), ('summary', self.summary)):
            if value is not None and not isinstance(value, str):
                raise ValueError(f'{name} must be a string')
        for name, value in (
            ('task_index', self.task_index),
            ('occurrence_count', self.occurrence_count),
        ):
            if (isinstance(value, bool) or not isinstance(value, int) or
                    value < 0):
                raise ValueError(f'{name} must be a nonnegative integer')
        for name, value in (
            ('armed_at', self.armed_at),
            ('updated_at', self.updated_at),
            ('occurred_at', self.occurred_at),
            ('deadline_at', self.deadline_at),
        ):
            if value is None:
                continue
            try:
                finite = (not isinstance(value, bool) and
                          isinstance(value,
                                     (int, float)) and math.isfinite(value))
            except OverflowError:
                finite = False
            if not finite:
                raise ValueError(f'{name} must be a finite timestamp')
        if self.updated_at < self.armed_at:
            raise ValueError('updated_at must not precede armed_at')
        if (self.summary is not None and
                len(self.summary) > JOB_SYSTEM_RECOVERY_SUMMARY_MAX_CHARS):
            raise ValueError(
                'summary exceeds '
                f'{JOB_SYSTEM_RECOVERY_SUMMARY_MAX_CHARS} characters')

    def to_dict(self) -> dict[str, Any]:
        """Convert to the versioned JSON representation."""
        return {
            'schema_version': _JOB_SYSTEM_RECOVERY_SCHEMA_VERSION,
            'capability': self.capability,
            'phase': self.phase.value,
            'original_attempt_id': self.original_attempt_id,
            'replacement_attempt_id': self.replacement_attempt_id,
            'task_index': self.task_index,
            'node_boot_id': self.node_boot_id,
            'occurrence_count': self.occurrence_count,
            'armed_at': self.armed_at,
            'updated_at': self.updated_at,
            'event_id': self.event_id,
            'reason': self.reason,
            'occurred_at': self.occurred_at,
            'deadline_at': self.deadline_at,
            'summary': self.summary,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> 'JobSystemRecoveryInfo':
        """Parse and validate the versioned JSON representation."""
        expected_fields = {
            'schema_version', 'capability', 'phase', 'original_attempt_id',
            'replacement_attempt_id', 'task_index', 'node_boot_id',
            'occurrence_count', 'armed_at', 'updated_at', 'event_id', 'reason',
            'occurred_at', 'deadline_at', 'summary'
        }
        if not isinstance(value, dict):
            raise ValueError('Recovery info must be a JSON object')
        if set(value) != expected_fields:
            raise ValueError('Recovery info has invalid fields')
        schema_version = value.get('schema_version')
        if (type(schema_version) is not int or  # pylint: disable=unidiomatic-typecheck
                schema_version != _JOB_SYSTEM_RECOVERY_SCHEMA_VERSION):
            raise ValueError('Unsupported job system recovery schema version: '
                             f'{schema_version!r}')
        try:
            phase = JobSystemRecoveryPhase(value['phase'])
            return cls(
                capability=value['capability'],
                phase=phase,
                original_attempt_id=value['original_attempt_id'],
                replacement_attempt_id=value.get('replacement_attempt_id'),
                task_index=value['task_index'],
                node_boot_id=value['node_boot_id'],
                occurrence_count=value['occurrence_count'],
                armed_at=value['armed_at'],
                updated_at=value['updated_at'],
                event_id=value.get('event_id'),
                reason=value.get('reason'),
                occurred_at=value.get('occurred_at'),
                deadline_at=value.get('deadline_at'),
                summary=value.get('summary'),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f'Invalid job system recovery info: {e}') from e

    def to_protobuf(self) -> 'jobsv1_pb2.JobSystemRecoveryInfo':
        """Convert to the gRPC representation."""
        kwargs: dict[str, Any] = {
            'capability': self.capability,
            'phase': self.phase.to_protobuf(),
            'original_attempt_id': self.original_attempt_id,
            'task_index': self.task_index,
            'node_boot_id': self.node_boot_id,
            'occurrence_count': self.occurrence_count,
            'armed_at': self.armed_at,
            'updated_at': self.updated_at,
        }
        for name in ('event_id', 'reason', 'occurred_at', 'deadline_at',
                     'summary', 'replacement_attempt_id'):
            value = getattr(self, name)
            if value is not None:
                kwargs[name] = value
        return jobsv1_pb2.JobSystemRecoveryInfo(**kwargs)

    @classmethod
    def from_protobuf(
        cls,
        value: 'jobsv1_pb2.JobSystemRecoveryInfo',
    ) -> 'JobSystemRecoveryInfo':
        """Convert from the gRPC representation."""
        phase = JobSystemRecoveryPhase.from_protobuf(value.phase)
        if phase is None:
            raise ValueError(
                f'Unknown job system recovery phase: {value.phase}')

        def _optional(name: str) -> Any | None:
            return getattr(value, name) if value.HasField(name) else None

        return cls(
            capability=value.capability,
            phase=phase,
            original_attempt_id=value.original_attempt_id,
            replacement_attempt_id=_optional('replacement_attempt_id'),
            task_index=value.task_index,
            node_boot_id=value.node_boot_id,
            occurrence_count=value.occurrence_count,
            armed_at=value.armed_at,
            updated_at=value.updated_at,
            event_id=_optional('event_id'),
            reason=_optional('reason'),
            occurred_at=_optional('occurred_at'),
            deadline_at=_optional('deadline_at'),
            summary=_optional('summary'),
        )


# We have two steps for job submissions:
# 1. Client reserve a job id from the job table by adding a INIT state job.
# 2. Client updates the job status to PENDING by actually submitting the job's
#    command to the scheduler.
# In normal cases, the two steps happens very close to each other through two
# consecutive SSH connections.
# We should update status for INIT job that has been staying in INIT state for
# a while (60 seconds), which likely fails to reach step 2.
# TODO(zhwu): This number should be tuned based on heuristics.
_INIT_SUBMIT_GRACE_PERIOD = 60

_PRE_RESOURCE_STATUSES = [JobStatus.PENDING]


class JobScheduler:
    """Base class for job scheduler"""

    @init_db
    def queue(self, job_id: int, cmd: str) -> None:
        assert _DB is not None
        _DB.cursor.execute('INSERT INTO pending_jobs VALUES (?,?,?,?)',
                           (job_id, cmd, 0, int(time.time())))
        _DB.conn.commit()
        set_status(job_id, JobStatus.PENDING)
        self.schedule_step()

    @init_db
    def remove_job_no_lock(self, job_id: int) -> None:
        assert _DB is not None
        _DB.cursor.execute(f'DELETE FROM pending_jobs WHERE job_id={job_id!r}')
        _DB.conn.commit()

    @init_db
    def _run_job(self, job_id: int, run_cmd: str):
        assert _DB is not None
        _DB.cursor.execute(f'UPDATE pending_jobs SET submit={int(time.time())} '
                           f'WHERE job_id={job_id!r}')
        _DB.conn.commit()
        pid = subprocess_utils.launch_new_process_tree(run_cmd)

        _DB.cursor.execute(f'UPDATE jobs SET pid={pid} '
                           f'WHERE job_id={job_id!r}')
        _DB.conn.commit()

    def schedule_step(self, force_update_jobs: bool = False) -> None:
        if force_update_jobs:
            update_status()
        pending_job_ids = self._get_pending_job_ids()
        # TODO(zhwu, mraheja): One optimization can be allowing more than one
        # job staying in the pending state after ray job submit, so that to be
        # faster to schedule a large amount of jobs.
        for job_id in pending_job_ids:
            with filelock.FileLock(_get_lock_path(job_id)):
                pending_job = _get_pending_job(job_id)
                if pending_job is None:
                    # Pending job can be removed by another thread, due to the
                    # job being scheduled already.
                    continue
                run_cmd = pending_job['run_cmd']
                submit = pending_job['submit']
                created_time = pending_job['created_time']
                # We don't have to refresh the job status before checking, as
                # the job status will only be stale in rare cases where ray job
                # crashes; or the job stays in INIT state for a long time.
                # In those cases, the periodic JobSchedulerEvent event will
                # update the job status every 300 seconds.
                status = get_status_no_lock(job_id)
                if (status not in _PRE_RESOURCE_STATUSES or
                        created_time < psutil.boot_time()):
                    # Job doesn't exist, is running/cancelled, or created
                    # before the last reboot.
                    self.remove_job_no_lock(job_id)
                    continue
                if submit:
                    # Next job waiting for resources
                    return
                self._run_job(job_id, run_cmd)
                return

    def _get_pending_job_ids(self) -> list[int]:
        """Returns the job ids in the pending jobs table

        The information contains job_id, run command, submit time,
        creation time.
        """
        raise NotImplementedError


class FIFOScheduler(JobScheduler):
    """First in first out job scheduler"""

    @init_db
    def _get_pending_job_ids(self) -> list[int]:
        assert _DB is not None
        rows = _DB.cursor.execute(
            'SELECT job_id FROM pending_jobs ORDER BY job_id').fetchall()
        return [row[0] for row in rows]


scheduler = FIFOScheduler()

_JOB_STATUS_TO_COLOR = {
    JobStatus.INIT: colorama.Fore.BLUE,
    JobStatus.SETTING_UP: colorama.Fore.BLUE,
    JobStatus.PENDING: colorama.Fore.BLUE,
    JobStatus.RUNNING: colorama.Fore.GREEN,
    JobStatus.FAILED_DRIVER: colorama.Fore.RED,
    JobStatus.SUCCEEDED: colorama.Fore.GREEN,
    JobStatus.FAILED: colorama.Fore.RED,
    JobStatus.FAILED_SETUP: colorama.Fore.RED,
    JobStatus.CANCELLED: colorama.Fore.YELLOW,
}


def make_job_command_with_user_switching(username: str,
                                         command: str) -> list[str]:
    return ['sudo', '-H', 'su', '--login', username, '-c', command]


@init_db
def add_job(job_name: str,
            username: str,
            run_timestamp: str,
            resources_str: str,
            metadata: str = '{}') -> tuple[int, str]:
    """Atomically reserve the next available job id for the user."""
    assert _DB is not None
    job_submitted_at = time.time()
    # job_id will autoincrement with the null value
    if int(constants.SKYLET_VERSION) >= 28:
        insert_cursor = _DB.cursor.execute(
            'INSERT INTO jobs VALUES (null, ?, ?, ?, ?, ?, ?, null, ?, 0, null, ?, null)',  # pylint: disable=line-too-long
            (job_name, username, job_submitted_at, JobStatus.INIT.value,
             run_timestamp, None, resources_str, metadata))
    else:
        insert_cursor = _DB.cursor.execute(
            'INSERT INTO jobs VALUES (null, ?, ?, ?, ?, ?, ?, null, ?, 0, null, ?)',  # pylint: disable=line-too-long
            (job_name, username, job_submitted_at, JobStatus.INIT.value,
             run_timestamp, None, resources_str, metadata))
    job_id = insert_cursor.lastrowid
    if job_id is None:
        raise RuntimeError('Failed to read the newly inserted job ID.')
    _DB.conn.commit()
    log_dir = os.path.join(constants.SKY_LOGS_DIRECTORY, f'{job_id}-{job_name}')
    set_log_dir_no_lock(job_id, log_dir)
    return job_id, log_dir


@init_db
def set_log_dir_no_lock(job_id: int, log_dir: str) -> None:
    """Set the log directory for the job.

    We persist the log directory for the job to allow changing the log directory
    generation logic over versions.

    Args:
        job_id: The ID of the job.
        log_dir: The log directory for the job.
    """
    assert _DB is not None
    _DB.cursor.execute('UPDATE jobs SET log_dir=(?) WHERE job_id=(?)',
                       (log_dir, job_id))
    _DB.conn.commit()


@init_db
def get_log_dir_for_job(job_id: int) -> str | None:
    """Get the log directory for the job.

    Args:
        job_id: The ID of the job.
    """
    assert _DB is not None
    rows = _DB.cursor.execute('SELECT log_dir FROM jobs WHERE job_id=(?)',
                              (job_id,))
    for row in rows:
        return row[0]
    return None


@init_db
def _set_status_no_lock(job_id: int, status: JobStatus) -> None:
    """Setting the status of the job in the database."""
    assert _DB is not None
    assert status != JobStatus.RUNNING, (
        'Please use set_job_started() to set job status to RUNNING')
    if status.is_terminal():
        end_at = time.time()
        # status does not need to be set if the end_at is not null, since
        # the job must be in a terminal state already.
        # Don't check the end_at for FAILED_SETUP, so that the generated
        # ray program can overwrite the status.
        check_end_at_str = ' AND end_at IS NULL'
        if status != JobStatus.FAILED_SETUP:
            check_end_at_str = ''
        _DB.cursor.execute(
            'UPDATE jobs SET status=(?), end_at=(?) '
            f'WHERE job_id=(?) {check_end_at_str}',
            (status.value, end_at, job_id))
    else:
        _DB.cursor.execute(
            'UPDATE jobs SET status=(?), end_at=NULL '
            'WHERE job_id=(?)', (status.value, job_id))
    _DB.conn.commit()


def set_status(job_id: int, status: JobStatus) -> None:
    # TODO(mraheja): remove pylint disabling when filelock version updated
    # pylint: disable=abstract-class-instantiated
    with filelock.FileLock(_get_lock_path(job_id)):
        _set_status_no_lock(job_id, status)


@init_db
def set_exit_codes(job_id: int, exit_codes: list[int]) -> None:
    """Set exit codes for a job as comma-separated string.

    Args:
        job_id: The job ID to update.
        exit_codes: A list of exit codes to store.
    """
    assert _DB is not None
    exit_codes_str = ','.join(str(code) for code in exit_codes)
    with filelock.FileLock(_get_lock_path(job_id)):
        _DB.cursor.execute('UPDATE jobs SET exit_codes=(?) WHERE job_id=(?)',
                           (exit_codes_str, job_id))
        _DB.conn.commit()


@init_db
def get_exit_codes(job_id: int) -> list[int] | None:
    """Get exit codes for a job from comma-separated string.

    Args:
        job_id: The job ID to retrieve exit codes for.

    Returns:
        A list of exit codes, or None if not found.
    """
    assert _DB is not None
    rows = _DB.cursor.execute('SELECT exit_codes FROM jobs WHERE job_id=(?)',
                              (job_id,))
    row = rows.fetchone()
    if row is None or row[0] is None:
        return None
    return [int(code) for code in row[0].split(',')]


@init_db
def set_job_started(job_id: int) -> None:
    # TODO(mraheja): remove pylint disabling when filelock version updated.
    # pylint: disable=abstract-class-instantiated
    assert _DB is not None
    with filelock.FileLock(_get_lock_path(job_id)):
        _DB.cursor.execute(
            'UPDATE jobs SET status=(?), start_at=(?), end_at=NULL '
            'WHERE job_id=(?)', (JobStatus.RUNNING.value, time.time(), job_id))
        _DB.conn.commit()


@init_db
def get_status_no_lock(job_id: int) -> JobStatus | None:
    """Get the status of the job with the given id.

    This function can return a stale status if there is a concurrent update.
    Make sure the caller will not be affected by the stale status, e.g. getting
    the status in a while loop as in `log_lib._follow_job_logs`. Otherwise, use
    `get_status`.
    """
    assert _DB is not None
    rows = _DB.cursor.execute('SELECT status FROM jobs WHERE job_id=(?)',
                              (job_id,))
    for (status,) in rows:
        if status is None:
            return None
        return JobStatus(status)
    return None


def get_status(job_id: int) -> JobStatus | None:
    # TODO(mraheja): remove pylint disabling when filelock version updated.
    # pylint: disable=abstract-class-instantiated
    with filelock.FileLock(_get_lock_path(job_id)):
        return get_status_no_lock(job_id)


def wait_for_job_completion(job_id: int, poll_interval: float = 1.0) -> None:
    """Wait for a job to reach a terminal state.

    Args:
        job_id: The job ID to wait for.
        poll_interval: How often to poll the job status in seconds.
    """
    while True:
        status = get_status(job_id)
        if status is None or status.is_terminal():
            break
        time.sleep(poll_interval)


@init_db
def get_statuses_payload(job_ids: list[int | None]) -> str:
    return message_utils.encode_payload(get_statuses(job_ids))


@init_db
def get_statuses(job_ids: list[int | None]) -> dict[int | None, str | None]:
    assert _DB is not None
    # Per-job lock is not required here, since the staled job status will not
    # affect the caller.
    query_str = ','.join(['?'] * len(job_ids))
    rows = _DB.cursor.execute(
        f'SELECT job_id, status FROM jobs WHERE job_id IN ({query_str})',
        job_ids)
    statuses: dict[int | None, str | None] = {
        job_id: None for job_id in job_ids
    }
    for (job_id, status) in rows:
        statuses[job_id] = status
    return statuses


def _serialize_job_system_recovery_info(info: JobSystemRecoveryInfo) -> str:
    return json.dumps(info.to_dict(), sort_keys=True, separators=(',', ':'))


def _deserialize_job_system_recovery_info(
        info_json: str) -> JobSystemRecoveryInfo:
    try:
        value = json.loads(info_json)
    except (TypeError, json.JSONDecodeError) as e:
        raise ValueError(f'Invalid job system recovery JSON: {e}') from e
    return JobSystemRecoveryInfo.from_dict(value)


def _get_job_system_recovery_info_no_lock(
        job_id: int) -> JobSystemRecoveryInfo | None:
    assert _DB is not None
    row = _DB.cursor.execute(
        'SELECT info_json FROM job_system_recovery WHERE job_id=(?)',
        (job_id,)).fetchone()
    if row is None:
        return None
    return _deserialize_job_system_recovery_info(row[0])


def _validate_armed_job_system_recovery_info(
        info: JobSystemRecoveryInfo) -> None:
    if info.phase != JobSystemRecoveryPhase.ARMED:
        raise ValueError('Initial recovery phase must be ARMED')
    if info.event_id is not None or info.reason is not None:
        raise ValueError('ARMED recovery must not have an event or reason')
    if info.occurrence_count != 0 or info.occurred_at is not None:
        raise ValueError('ARMED recovery must not have an occurrence')
    if info.deadline_at is not None:
        raise ValueError('ARMED recovery must not have a deadline')
    if info.replacement_attempt_id is not None:
        raise ValueError('ARMED recovery must not have a replacement attempt')


_JOB_SYSTEM_RECOVERY_ALLOWED_TRANSITIONS = {
    JobSystemRecoveryPhase.ARMED: {
        JobSystemRecoveryPhase.WAITING_CLEANUP,
        JobSystemRecoveryPhase.EXHAUSTED,
    },
    JobSystemRecoveryPhase.WAITING_CLEANUP: {
        JobSystemRecoveryPhase.WAITING_MEMORY,
        JobSystemRecoveryPhase.EXHAUSTED,
    },
    JobSystemRecoveryPhase.WAITING_MEMORY: {
        JobSystemRecoveryPhase.RESUBMITTING,
        JobSystemRecoveryPhase.EXHAUSTED,
    },
    JobSystemRecoveryPhase.RESUBMITTING: {
        JobSystemRecoveryPhase.RETRY_SUBMITTED,
        JobSystemRecoveryPhase.EXHAUSTED,
    },
    JobSystemRecoveryPhase.RETRY_SUBMITTED: {JobSystemRecoveryPhase.EXHAUSTED,},
    JobSystemRecoveryPhase.EXHAUSTED: set(),
}


def _validate_job_system_recovery_transition(
        current: JobSystemRecoveryInfo, updated: JobSystemRecoveryInfo) -> None:
    if updated.phase not in _JOB_SYSTEM_RECOVERY_ALLOWED_TRANSITIONS[
            current.phase]:
        raise ValueError(f'Invalid recovery phase transition: '
                         f'{current.phase.value} -> {updated.phase.value}')
    for name in ('capability', 'original_attempt_id', 'task_index',
                 'node_boot_id', 'armed_at'):
        if getattr(current, name) != getattr(updated, name):
            raise ValueError(f'Recovery identity field changed: {name}')
    if current.event_id is not None and updated.event_id != current.event_id:
        raise ValueError('Recovery event_id changed')
    if (current.event_id is None and
            updated.phase != JobSystemRecoveryPhase.EXHAUSTED and
            updated.event_id is None):
        raise ValueError('Recovery event_id must be set after ARMED')
    if current.reason is not None and updated.reason != current.reason:
        raise ValueError('Recovery reason changed')
    if current.occurred_at is not None and updated.occurred_at != current.occurred_at:
        raise ValueError('Recovery occurred_at changed')
    if current.deadline_at is not None and updated.deadline_at != current.deadline_at:
        raise ValueError('Recovery deadline_at changed')
    if updated.updated_at < current.updated_at:
        raise ValueError('Recovery updated_at moved backwards')

    is_resubmission = (current.phase == JobSystemRecoveryPhase.WAITING_MEMORY
                       and updated.phase == JobSystemRecoveryPhase.RESUBMITTING)
    if is_resubmission:
        if (current.replacement_attempt_id is not None or
                updated.replacement_attempt_id is None):
            raise ValueError('RESUBMITTING must allocate one replacement')
    elif updated.replacement_attempt_id != current.replacement_attempt_id:
        raise ValueError('Recovery replacement identity changed unexpectedly')

    if (current.phase == JobSystemRecoveryPhase.ARMED and
            updated.phase == JobSystemRecoveryPhase.WAITING_CLEANUP):
        if (updated.occurrence_count != 1 or updated.event_id is None or
                updated.reason is None or updated.occurred_at is None or
                updated.deadline_at is None):
            raise ValueError('WAITING_CLEANUP must record the first event')
    elif updated.phase == JobSystemRecoveryPhase.EXHAUSTED:
        if updated.occurrence_count not in (current.occurrence_count,
                                            current.occurrence_count + 1):
            raise ValueError('EXHAUSTED occurrence_count advanced invalidly')
    elif updated.occurrence_count != current.occurrence_count:
        raise ValueError('Recovery occurrence_count changed unexpectedly')


@init_db
def arm_job_system_recovery_no_lock(job_id: int,
                                    info: JobSystemRecoveryInfo) -> bool:
    """Persist ARMED while the caller holds ``job_status_lock``."""
    assert _DB is not None
    _validate_armed_job_system_recovery_info(info)
    if get_status_no_lock(job_id) != JobStatus.RUNNING:
        return False
    current = _get_job_system_recovery_info_no_lock(job_id)
    if current is not None:
        return current == info
    try:
        _DB.cursor.execute(
            'INSERT INTO job_system_recovery(job_id, info_json) VALUES (?, ?)',
            (job_id, _serialize_job_system_recovery_info(info)))
        _DB.conn.commit()
    except Exception:
        _DB.conn.rollback()
        raise
    return True


@init_db
def arm_job_system_recovery(job_id: int, info: JobSystemRecoveryInfo) -> bool:
    """Persist an eligible recovery capability for a running job."""
    with job_status_lock(job_id):
        return arm_job_system_recovery_no_lock(job_id, info)


@init_db
def transition_job_system_recovery_no_lock(
    job_id: int,
    expected_phase: JobSystemRecoveryPhase,
    info: JobSystemRecoveryInfo,
) -> bool:
    """CAS a recovery phase while the caller holds ``job_status_lock``."""
    assert _DB is not None
    if get_status_no_lock(job_id) != JobStatus.RUNNING:
        return False
    current = _get_job_system_recovery_info_no_lock(job_id)
    if current is None:
        return False
    if current == info:
        return True
    if current.phase != expected_phase:
        return False
    _validate_job_system_recovery_transition(current, info)
    try:
        update_cursor = _DB.cursor.execute(
            'UPDATE job_system_recovery SET info_json=(?) WHERE job_id=(?)',
            (_serialize_job_system_recovery_info(info), job_id))
        if update_cursor.rowcount != 1:
            _DB.conn.rollback()
            return False
        _DB.conn.commit()
    except Exception:
        _DB.conn.rollback()
        raise
    return True


def transition_job_system_recovery(
    job_id: int,
    expected_phase: JobSystemRecoveryPhase,
    info: JobSystemRecoveryInfo,
) -> bool:
    """Atomically compare and advance a running job's recovery phase."""
    with job_status_lock(job_id):
        return transition_job_system_recovery_no_lock(job_id, expected_phase,
                                                      info)


@init_db
def exhaust_job_system_recovery_no_lock(
    job_id: int,
    expected_phase: JobSystemRecoveryPhase,
    info: JobSystemRecoveryInfo,
) -> bool:
    """Atomically persist EXHAUSTED and terminal FAILED.

    The caller must hold ``job_status_lock``. A concurrent cancellation that
    reached the database first wins and this method returns False.
    """
    assert _DB is not None
    if info.phase != JobSystemRecoveryPhase.EXHAUSTED:
        raise ValueError('Exhausted recovery info must use EXHAUSTED phase')
    _DB.cursor.execute('BEGIN IMMEDIATE')
    try:
        status_row = _DB.cursor.execute(
            'SELECT status FROM jobs WHERE job_id=(?)', (job_id,)).fetchone()
        current = _get_job_system_recovery_info_no_lock(job_id)
        if current == info and status_row is not None and status_row[
                0] == JobStatus.FAILED.value:
            _DB.conn.commit()
            return True
        if (status_row is None or status_row[0] != JobStatus.RUNNING.value or
                current is None or current.phase != expected_phase):
            _DB.conn.rollback()
            return False
        _validate_job_system_recovery_transition(current, info)
        _DB.cursor.execute(
            'UPDATE job_system_recovery SET info_json=(?) WHERE job_id=(?)',
            (_serialize_job_system_recovery_info(info), job_id))
        status_cursor = _DB.cursor.execute(
            'UPDATE jobs SET status=(?), end_at=(?) '
            'WHERE job_id=(?) AND status=(?)',
            (JobStatus.FAILED.value, time.time(), job_id,
             JobStatus.RUNNING.value))
        if status_cursor.rowcount != 1:
            _DB.conn.rollback()
            return False
        _DB.conn.commit()
        return True
    except Exception:
        _DB.conn.rollback()
        raise


def exhaust_job_system_recovery(
    job_id: int,
    expected_phase: JobSystemRecoveryPhase,
    info: JobSystemRecoveryInfo,
) -> bool:
    """Persist terminal recovery state under the per-job lock."""
    with job_status_lock(job_id):
        return exhaust_job_system_recovery_no_lock(job_id, expected_phase, info)


def fail_job_system_recovery_no_lock(job_id: int) -> None:
    """Fail a still-running job while the caller holds its status lock."""
    if get_status_no_lock(job_id) == JobStatus.RUNNING:
        _set_status_no_lock(job_id, JobStatus.FAILED)


@init_db
def get_job_system_recovery_info(job_id: int) -> JobSystemRecoveryInfo | None:
    """Return optional recovery state without affecting ordinary status."""
    try:
        return _get_job_system_recovery_info_no_lock(job_id)
    except ValueError as e:
        logger.warning('Ignoring malformed system recovery state for job '
                       f'{job_id}: {e}')
        return None


@init_db
def get_job_system_recovery_details(
    job_ids: Sequence[int],
) -> tuple[dict[int, JobSystemRecoveryInfo], dict[
        int, JobSystemRecoveryDetailStatus]]:
    """Return valid details plus explicit per-job validity/presence."""
    assert _DB is not None
    if not job_ids:
        return {}, {}
    requested_job_ids = set(job_ids)
    rows = _DB.cursor.execute(
        'SELECT job_id, info_json FROM job_system_recovery '
        f'WHERE job_id IN ({",".join(["?"] * len(job_ids))})', list(job_ids))
    infos: dict[int, JobSystemRecoveryInfo] = {}
    detail_statuses = {
        job_id: JobSystemRecoveryDetailStatus.ABSENT
        for job_id in requested_job_ids
    }
    for job_id, info_json in rows:
        try:
            infos[job_id] = _deserialize_job_system_recovery_info(info_json)
            detail_statuses[job_id] = JobSystemRecoveryDetailStatus.PRESENT
        except ValueError as e:
            detail_statuses[job_id] = JobSystemRecoveryDetailStatus.MALFORMED
            logger.warning('Ignoring malformed system recovery state for job '
                           f'{job_id}: {e}')
    return infos, detail_statuses


def get_job_system_recovery_infos(
        job_ids: Sequence[int]) -> dict[int, JobSystemRecoveryInfo]:
    """Return valid recovery records for compatibility callers."""
    return get_job_system_recovery_details(job_ids)[0]


def get_statuses_with_system_recovery_payload(job_ids: list[int | None]) -> str:
    """Encode statuses and optional recovery details in one SSH payload."""
    statuses = get_statuses(job_ids)
    concrete_job_ids = [job_id for job_id in job_ids if job_id is not None]
    try:
        recovery_infos, detail_statuses = get_job_system_recovery_details(
            concrete_job_ids)
    except Exception as e:  # pylint: disable=broad-except
        logger.warning('Ignoring optional system recovery details while '
                       f'encoding job statuses: {e}')
        recovery_infos = {}
        detail_statuses = {
            job_id: JobSystemRecoveryDetailStatus.MALFORMED
            for job_id in concrete_job_ids
        }
    return message_utils.encode_payload({
        'version': _JOB_STATUS_SYSTEM_RECOVERY_PAYLOAD_VERSION,
        'job_statuses': statuses,
        'system_recovery_infos': {
            job_id: info.to_dict() for job_id, info in recovery_infos.items()
        },
        'system_recovery_detail_statuses': {
            job_id: status.value for job_id, status in detail_statuses.items()
        },
    })


@init_db
def get_jobs_info(user_hash: str | None = None,
                  all_jobs: bool = False) -> list['jobsv1_pb2.JobInfo']:
    """Get detailed job information.

    Similar to dump_job_queue but returns structured protobuf objects instead
    of encoded strings.

    Args:
        user_hash: The user hash to show jobs for. Show all the users if None.
        all_jobs: Whether to show all jobs, not just the pending/running ones.
    """
    assert _DB is not None

    status_list: list[JobStatus] | None = [
        JobStatus.SETTING_UP, JobStatus.PENDING, JobStatus.RUNNING
    ]
    if all_jobs:
        status_list = None

    jobs = _get_jobs(user_hash, status_list=status_list)
    jobs_info = []
    for job in jobs:
        jobs_info.append(
            jobsv1_pb2.JobInfo(job_id=job['job_id'],
                               job_name=job['job_name'],
                               username=job['username'],
                               submitted_at=job['submitted_at'],
                               status=job['status'].to_protobuf(),
                               run_timestamp=job['run_timestamp'],
                               start_at=job['start_at'],
                               end_at=job['end_at'],
                               resources=job['resources'],
                               pid=job['pid'],
                               log_path=os.path.join(
                                   constants.SKY_LOGS_DIRECTORY,
                                   job['run_timestamp']),
                               metadata=json.dumps(job['metadata'])))
    return jobs_info


def _load_statuses_dict(
    original_statuses: dict[str, str | None],
) -> dict[int | None, JobStatus | None]:
    statuses: dict[int | None, JobStatus | None] = {}
    for job_id, status in original_statuses.items():
        # json.dumps will convert all keys to strings. Integers will
        # become string representations of integers, e.g. "1" instead of 1;
        # `None` will become "null" instead of None. Here we use
        # json.loads to convert them back to their original values.
        # See docstr of core::job_status for the meaning of `statuses`.
        statuses[json.loads(job_id)] = (JobStatus(status)
                                        if status is not None else None)
    return statuses


def load_statuses_payload(
        statuses_payload: str) -> dict[int | None, JobStatus | None]:
    original_statuses = message_utils.decode_payload(statuses_payload)
    return _load_statuses_dict(original_statuses)


def load_statuses_with_system_recovery_payload(
    payload: str,
) -> tuple[dict[int | None, JobStatus | None], dict[int, JobSystemRecoveryInfo],
           dict[int, JobSystemRecoveryDetailStatus]]:
    """Decode the new SSH envelope or a legacy status-only payload."""
    decoded = message_utils.decode_payload(payload)
    if not isinstance(decoded, dict) or 'job_statuses' not in decoded:
        statuses = _load_statuses_dict(decoded)
        return statuses, {}, {
            job_id: JobSystemRecoveryDetailStatus.UNSPECIFIED
            for job_id in statuses
            if isinstance(job_id, int)
        }

    statuses = _load_statuses_dict(decoded['job_statuses'])
    detail_statuses = {
        job_id: JobSystemRecoveryDetailStatus.UNSPECIFIED
        for job_id in statuses
        if isinstance(job_id, int)
    }
    payload_version = decoded.get('version')
    if (type(payload_version) is not int or  # pylint: disable=unidiomatic-typecheck
            payload_version != _JOB_STATUS_SYSTEM_RECOVERY_PAYLOAD_VERSION):
        logger.warning('Ignoring unsupported job status recovery payload '
                       f'version: {payload_version!r}')
        return statuses, {}, {
            job_id: JobSystemRecoveryDetailStatus.MALFORMED
            for job_id in detail_statuses
        }

    serialized_statuses = decoded.get('system_recovery_detail_statuses')
    if not isinstance(serialized_statuses, dict):
        logger.warning('Recovery detail status map is missing or malformed')
        return statuses, {}, {
            job_id: JobSystemRecoveryDetailStatus.MALFORMED
            for job_id in detail_statuses
        }
    for raw_job_id, serialized_status in serialized_statuses.items():
        job_id: object = None
        try:
            job_id = json.loads(raw_job_id)
            if isinstance(job_id, bool) or not isinstance(job_id, int):
                raise ValueError(f'invalid job ID {job_id!r}')
            detail_statuses[job_id] = JobSystemRecoveryDetailStatus(
                serialized_status)
        except (TypeError, ValueError, json.JSONDecodeError) as e:
            logger.warning('Ignoring malformed recovery detail status entry '
                           f'for job {raw_job_id!r}: {e}')
            if isinstance(job_id, int):
                detail_statuses[job_id] = (
                    JobSystemRecoveryDetailStatus.MALFORMED)

    recovery_infos: dict[int, JobSystemRecoveryInfo] = {}
    serialized_infos = decoded.get('system_recovery_infos', {})
    if not isinstance(serialized_infos, dict):
        logger.warning('Ignoring malformed system recovery info map')
        return statuses, recovery_infos, {
            job_id: JobSystemRecoveryDetailStatus.MALFORMED
            for job_id in detail_statuses
        }
    for raw_job_id, serialized_info in serialized_infos.items():
        job_id = None
        try:
            job_id = json.loads(raw_job_id)
            if isinstance(job_id, bool) or not isinstance(job_id, int):
                raise ValueError(f'invalid job ID {job_id!r}')
            recovery_infos[job_id] = JobSystemRecoveryInfo.from_dict(
                serialized_info)
        except (TypeError, ValueError, json.JSONDecodeError) as e:
            logger.warning('Ignoring malformed system recovery payload entry '
                           f'for job {raw_job_id!r}: {e}')
            if isinstance(job_id, int):
                detail_statuses[job_id] = (
                    JobSystemRecoveryDetailStatus.MALFORMED)
    for job_id in tuple(detail_statuses):
        detail_status = detail_statuses[job_id]
        if ((detail_status == JobSystemRecoveryDetailStatus.PRESENT)
                != (job_id in recovery_infos)):
            detail_statuses[job_id] = JobSystemRecoveryDetailStatus.MALFORMED
            recovery_infos.pop(job_id, None)
    return statuses, recovery_infos, detail_statuses


@init_db
def get_latest_job_id() -> int | None:
    assert _DB is not None
    rows = _DB.cursor.execute(
        'SELECT job_id FROM jobs ORDER BY job_id DESC LIMIT 1')
    for (job_id,) in rows:
        return job_id
    return None


@init_db
def get_job_submitted_or_ended_timestamp_payload(job_id: int,
                                                 get_ended_time: bool) -> str:
    """Get the job submitted/ended timestamp.

    This function should only be called by the jobs controller, which is ok to
    use `submitted_at` instead of `start_at`, because the managed job duration
    need to include both setup and running time and the job will not stay in
    PENDING state.

    The normal job duration will use `start_at` instead of `submitted_at` (in
    `table_utils.format_job_queue()`), because the job may stay in PENDING if
    the cluster is busy.
    """
    return message_utils.encode_payload(
        get_job_submitted_or_ended_timestamp(job_id, get_ended_time))


@init_db
def get_job_submitted_or_ended_timestamp(job_id: int,
                                         get_ended_time: bool) -> float | None:
    """Get the job submitted timestamp.

    Returns the raw timestamp or None if job doesn't exist.
    """
    assert _DB is not None
    field = 'end_at' if get_ended_time else 'submitted_at'
    rows = _DB.cursor.execute(f'SELECT {field} FROM jobs WHERE job_id=(?)',
                              (job_id,))
    for (timestamp,) in rows:
        return timestamp
    return None


def get_ray_port():
    """Get the port Skypilot-internal Ray cluster uses.

    If the port file does not exist, the cluster was launched before #1790,
    return the default port.
    """
    port_path = runtime_utils.get_runtime_dir_path(
        constants.SKY_REMOTE_RAY_PORT_FILE)
    if not os.path.exists(port_path):
        return 6379
    port = json.load(open(port_path, encoding='utf-8'))['ray_port']
    return port


def get_job_submission_port():
    """Get the dashboard port Skypilot-internal Ray cluster uses.

    If the port file does not exist, the cluster was launched before #1790,
    return the default port.
    """
    port_path = runtime_utils.get_runtime_dir_path(
        constants.SKY_REMOTE_RAY_PORT_FILE)
    if not os.path.exists(port_path):
        return 8265
    port = json.load(open(port_path, encoding='utf-8'))['ray_dashboard_port']
    return port


def _get_records_from_rows(rows) -> list[dict[str, Any]]:
    records = []
    for row in rows:
        if row[0] is None:
            break
        # TODO: use namedtuple instead of dict
        records.append({
            'job_id': row[JobInfoLoc.JOB_ID.value],
            'job_name': row[JobInfoLoc.JOB_NAME.value],
            'username': row[JobInfoLoc.USERNAME.value],
            'submitted_at': row[JobInfoLoc.SUBMITTED_AT.value],
            'status': JobStatus(row[JobInfoLoc.STATUS.value]),
            'run_timestamp': row[JobInfoLoc.RUN_TIMESTAMP.value],
            'start_at': row[JobInfoLoc.START_AT.value],
            'end_at': row[JobInfoLoc.END_AT.value],
            'resources': row[JobInfoLoc.RESOURCES.value],
            'pid': row[JobInfoLoc.PID.value],
            'metadata': json.loads(row[JobInfoLoc.METADATA.value]),
        })
        if int(constants.SKYLET_VERSION) >= 28:
            exit_code_str = row[JobInfoLoc.EXIT_CODES.value]
            if not isinstance(exit_code_str, str):
                records[-1]['exit_codes'] = None
            else:
                records[-1]['exit_codes'] = ([
                    int(code) for code in exit_code_str.split(',')
                ])
    return records


@init_db
def _get_jobs(
        user_hash: str | None,
        status_list: list[JobStatus] | None = None) -> list[dict[str, Any]]:
    """Returns jobs with the given fields, sorted by job_id, descending."""
    assert _DB is not None
    if status_list is None:
        status_list = list(JobStatus)
    status_str_list = [repr(status.value) for status in status_list]
    filter_str = f'WHERE status IN ({",".join(status_str_list)})'
    params = []
    if user_hash is not None:
        # We use the old username field for compatibility.
        filter_str += ' AND username=(?)'
        params.append(user_hash)
    rows = _DB.cursor.execute(
        f'SELECT * FROM jobs {filter_str} ORDER BY job_id DESC', params)
    records = _get_records_from_rows(rows)
    return records


@init_db
def _get_jobs_by_ids(job_ids: list[int]) -> list[dict[str, Any]]:
    assert _DB is not None
    rows = _DB.cursor.execute(
        f"""\
        SELECT * FROM jobs
        WHERE job_id IN ({','.join(['?'] * len(job_ids))})
        ORDER BY job_id DESC""",
        (*job_ids,),
    )
    records = _get_records_from_rows(rows)
    return records


@init_db
def _get_pending_job(job_id: int) -> dict[str, Any] | None:
    assert _DB is not None
    rows = _DB.cursor.execute(
        'SELECT created_time, submit, run_cmd FROM pending_jobs '
        f'WHERE job_id={job_id!r}')
    for row in rows:
        created_time, submit, run_cmd = row
        return {
            'created_time': created_time,
            'submit': submit,
            'run_cmd': run_cmd
        }
    return None


def _is_job_driver_process_running(job_pid: int, job_id: int) -> bool:
    """Check if the job driver process is running.

    We check the cmdline to avoid the case where the same pid is reused by a
    different process.
    """
    if job_pid <= 0:
        return False
    try:
        job_process = psutil.Process(job_pid)
        return job_process.is_running() and any(
            JOB_CMD_IDENTIFIER.format(job_id) in line
            for line in job_process.cmdline())
    except psutil.NoSuchProcess:
        return False


def update_job_status(job_ids: list[int],
                      silent: bool = False) -> list[JobStatus]:
    """Updates and returns the job statuses matching our `JobStatus` semantics.

    This function queries `ray job status` and processes those results to match
    our semantics.

    Though we update job status actively in the generated ray program and
    during job cancelling, we still need this to handle the staleness problem,
    caused by instance restarting and other corner cases (if any).

    This function should only be run on the remote instance with ray>=2.4.0.
    """
    echo = logger.info if not silent else logger.debug
    if not job_ids:
        return []

    statuses = []
    for job_id in job_ids:
        # Per-job status lock is required because between the job status
        # query and the job status update, the job status in the database
        # can be modified by the generated ray program.
        with filelock.FileLock(_get_lock_path(job_id)):
            status = None
            job_record = _get_jobs_by_ids([job_id])[0]
            original_status = job_record['status']
            job_submitted_at = job_record['submitted_at']
            job_pid = job_record['pid']

            pid_query_time = time.time()
            failed_driver_transition_message = None
            if original_status == JobStatus.INIT:
                if (job_submitted_at >= psutil.boot_time() and job_submitted_at
                        >= pid_query_time - _INIT_SUBMIT_GRACE_PERIOD):
                    # The job id is reserved, but the job is not submitted yet.
                    # We should keep it in INIT.
                    status = JobStatus.INIT
                else:
                    # We always immediately submit job after the job id is
                    # allocated, i.e. INIT -> PENDING, if a job stays in INIT
                    # for too long, it is likely the job submission process
                    # was killed before the job is submitted. We should set it
                    # to FAILED then. Note, if ray job indicates the job is
                    # running, we will change status to PENDING below.
                    failed_driver_transition_message = (
                        f'INIT job {job_id} is stale, setting to FAILED_DRIVER')
                    status = JobStatus.FAILED_DRIVER

            # job_pid is 0 if the job is not submitted yet.
            # job_pid is -1 if the job is submitted with SkyPilot older than
            # #4318, using ray job submit. We skip the checking for those
            # jobs.
            if job_pid > 0:
                if _is_job_driver_process_running(job_pid, job_id):
                    status = JobStatus.PENDING
                else:
                    # By default, if the job driver process does not exist,
                    # the actual SkyPilot job is one of the following:
                    # 1. Still pending to be submitted.
                    # 2. Submitted and finished.
                    # 3. Driver failed without correctly setting the job
                    #    status in the job table.
                    # Although we set the status to FAILED_DRIVER, it can be
                    # overridden to PENDING if the job is not submitted, or
                    # any other terminal status if the job driver process
                    # finished correctly.
                    failed_driver_transition_message = (
                        f'Job {job_id} driver process is not running, but '
                        'the job state is not in terminal states, setting '
                        'it to FAILED_DRIVER')
                    status = JobStatus.FAILED_DRIVER

            pending_job = _get_pending_job(job_id)
            if pending_job is not None:
                if pending_job['created_time'] < psutil.boot_time():
                    failed_driver_transition_message = (
                        f'Job {job_id} is stale, setting to FAILED_DRIVER: '
                        f'created_time={pending_job["created_time"]}, '
                        f'boot_time={psutil.boot_time()}')
                    # The job is stale as it is created before the instance
                    # is booted, e.g. the instance is rebooted.
                    status = JobStatus.FAILED_DRIVER
                elif pending_job['submit'] <= 0:
                    # The job is not submitted (submit <= 0), we set it to
                    # PENDING.
                    # For submitted jobs, the driver should have been started,
                    # because the job_lib.JobScheduler.schedule_step() have
                    # the submit field and driver process pid set in the same
                    # job lock.
                    # The job process check in the above section should
                    # correctly figured out the status and we don't overwrite
                    # it here. (Note: the FAILED_DRIVER status will be
                    # overridden by the actual job terminal status in the table
                    # if the job driver process finished correctly.)
                    status = JobStatus.PENDING

            assert original_status is not None, (job_id, status)
            if status is None:
                # The job is submitted but the job driver process pid is not
                # set in the database. This is guarding against the case where
                # the schedule_step() function is interrupted (e.g., VM stop)
                # at the middle of starting a new process and setting the pid.
                status = original_status
                if (original_status is not None and
                        not original_status.is_terminal()):
                    echo(f'Job {job_id} status is None, setting it to '
                         'FAILED_DRIVER.')
                    # The job may be stale, when the instance is restarted. We
                    # need to reset the job status to FAILED_DRIVER if its
                    # original status is in nonterminal_statuses.
                    echo(f'Job {job_id} is in a unknown state, setting it to '
                         'FAILED_DRIVER')
                    status = JobStatus.FAILED_DRIVER
                    _set_status_no_lock(job_id, status)
            else:
                # Taking max of the status is necessary because:
                # 1. The original status has already been set to later
                #    terminal state by a finished job driver.
                # 2. Job driver process check would map any running job process
                #    to `PENDING`, so we need to take the max to keep it at
                #    later status for jobs actually started in SETTING_UP or
                #    RUNNING.
                status = max(status, original_status)
                assert status is not None, (job_id, status, original_status)
                if status != original_status:  # Prevents redundant update.
                    _set_status_no_lock(job_id, status)
                    echo(f'Updated job {job_id} status to {status}')
                    if (status == JobStatus.FAILED_DRIVER and
                            failed_driver_transition_message is not None):
                        echo(failed_driver_transition_message)
        statuses.append(status)
    return statuses


@init_db
def fail_all_jobs_in_progress() -> None:
    assert _DB is not None
    in_progress_status = [
        status.value for status in JobStatus.nonterminal_statuses()
    ]
    _DB.cursor.execute(
        f"""\
        UPDATE jobs SET status=(?)
        WHERE status IN ({','.join(['?'] * len(in_progress_status))})
        """, (JobStatus.FAILED_DRIVER.value, *in_progress_status))
    _DB.conn.commit()


def update_status() -> None:
    # This signal file suggests that the controller is recovering from a
    # failure. See sky/jobs/utils.py::update_managed_jobs_statuses for more
    # details. When recovering, we should not update the job status to failed
    # driver as they will be recovered later.
    if os.path.exists(
            os.path.expanduser(
                constants.PERSISTENT_RUN_RESTARTING_SIGNAL_FILE)):
        return
    # This will be called periodically by the skylet to update the status
    # of the jobs in the database, to avoid stale job status.
    nonterminal_jobs = _get_jobs(user_hash=None,
                                 status_list=JobStatus.nonterminal_statuses())
    nonterminal_job_ids = [job['job_id'] for job in nonterminal_jobs]

    update_job_status(nonterminal_job_ids)


@init_db
def is_cluster_idle() -> bool:
    """Returns if the cluster is idle (no in-flight jobs)."""
    assert _DB is not None
    in_progress_status = [
        status.value for status in JobStatus.nonterminal_statuses()
    ]
    rows = _DB.cursor.execute(
        f"""\
        SELECT COUNT(*) FROM jobs
        WHERE status IN ({','.join(['?'] * len(in_progress_status))})
        """, in_progress_status)
    for (count,) in rows:
        return count == 0
    assert False, 'Should not reach here'


def dump_job_queue(user_hash: str | None, all_jobs: bool) -> str:
    """Get the job queue in encoded json format.

    Args:
        user_hash: The user hash to show jobs for. Show all the users if None.
        all_jobs: Whether to show all jobs, not just the pending/running ones.
    """
    status_list: list[JobStatus] | None = [
        JobStatus.SETTING_UP, JobStatus.PENDING, JobStatus.RUNNING
    ]
    if all_jobs:
        status_list = None

    jobs = _get_jobs(user_hash, status_list=status_list)
    for job in jobs:
        job['status'] = job['status'].value
        job['log_path'] = os.path.join(constants.SKY_LOGS_DIRECTORY,
                                       job.pop('run_timestamp'))
    return message_utils.encode_payload(jobs)


def load_job_queue(payload: str) -> list[dict[str, Any]]:
    """Load the job queue from encoded json format.

    Args:
        payload: The encoded payload string to load.
    """
    jobs = message_utils.decode_payload(payload)
    for job in jobs:
        job['status'] = JobStatus(job['status'])
    return resolve_job_queue_users(jobs)


def resolve_job_queue_users(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Populate queue user hashes and resolve usernames in one batch."""
    user_ids = set()
    for job in jobs:
        user_hash = job.get('user_hash')
        if user_hash is None:
            user_hash = job.get('username', '')
            job['user_hash'] = user_hash
        if user_hash:
            user_ids.add(user_hash)
    users = global_user_state.get_users(user_ids) if user_ids else {}
    for job in jobs:
        user = users.get(job['user_hash'])
        job['username'] = user.name if user is not None else None
    return jobs


def _make_ray_job_id(sky_job_id: int) -> str:
    return f'{sky_job_id}-{getpass.getuser()}'


def cancel_jobs_encoded_results(jobs: list[int] | None,
                                cancel_all: bool = False,
                                user_hash: str | None = None) -> str:
    """Cancel jobs.

    Args:
        jobs: Job IDs to cancel.
        cancel_all: Whether to cancel all jobs.
        user_hash: If specified, cancels the jobs for the specified user only.
            Otherwise, applies to all users.

    Returns:
        Encoded job IDs that are actually cancelled. Caller should use
        message_utils.decode_payload() to parse.
    """
    return message_utils.encode_payload(cancel_jobs(jobs, cancel_all,
                                                    user_hash))


def cancel_jobs(jobs: list[int] | None,
                cancel_all: bool = False,
                user_hash: str | None = None) -> list[int]:
    job_records = []
    all_status = [JobStatus.PENDING, JobStatus.SETTING_UP, JobStatus.RUNNING]
    if jobs is None and not cancel_all:
        # Cancel the latest (largest job ID) running job from current user.
        job_records = _get_jobs(user_hash, [JobStatus.RUNNING])[:1]
    elif cancel_all:
        job_records = _get_jobs(user_hash, all_status)
    if jobs is not None:
        job_records.extend(_get_jobs_by_ids(jobs))

    cancelled_ids = []
    # Sequentially cancel the jobs to avoid the resource number bug caused by
    # ray cluster (tracked in #1262).
    for job_record in job_records:
        job_id = job_record['job_id']
        # Job is locked to ensure that pending queue does not start it while
        # it is being cancelled
        with filelock.FileLock(_get_lock_path(job_id)):
            job = _get_jobs_by_ids([job_id])[0]
            if _is_job_driver_process_running(job['pid'], job_id):
                # Not use process.terminate() as that will only terminate the
                # process shell process, not the ray driver process
                # under the shell.
                #
                # We don't kill all the children of the process, like
                # subprocess_utils.kill_process_daemon() does, but just the
                # process group here, because the underlying job driver can
                # start other jobs with `schedule_step`, causing the other job
                # driver processes to be children of the current job driver
                # process.
                #
                # Killing the process group is enough as the underlying job
                # should be able to clean itself up correctly by ray driver.
                #
                # The process group pid should be the same as the job pid as we
                # use start_new_session=True, but we use os.getpgid() to be
                # extra cautious.
                job_pgid = os.getpgid(job['pid'])
                os.killpg(job_pgid, signal.SIGTERM)
                # We don't have to start a daemon to forcefully kill the process
                # as our job driver process will clean up the underlying
                # child processes.
            # Get the job status again to avoid race condition.
            job_status = get_status_no_lock(job['job_id'])
            if job_status in [
                    JobStatus.PENDING, JobStatus.SETTING_UP, JobStatus.RUNNING
            ]:
                _set_status_no_lock(job['job_id'], JobStatus.CANCELLED)
                cancelled_ids.append(job['job_id'])

        scheduler.schedule_step()
    return cancelled_ids


@init_db
def get_run_timestamp(job_id: int | None) -> str | None:
    """Returns the relative path to the log file for a job."""
    assert _DB is not None
    _DB.cursor.execute(
        """\
            SELECT * FROM jobs
            WHERE job_id=(?)""", (job_id,))
    row = _DB.cursor.fetchone()
    if row is None:
        return None
    run_timestamp = row[JobInfoLoc.RUN_TIMESTAMP.value]
    return run_timestamp


@init_db
def get_log_dir_for_jobs(job_ids: list[str | None]) -> str:
    """Returns the relative paths to the log files for jobs with globbing,
    encoded."""
    job_to_dir = get_job_log_dirs(job_ids)
    job_to_dir_str: dict[str, str] = {}
    for job_id, log_dir in job_to_dir.items():
        job_to_dir_str[str(job_id)] = log_dir
    return message_utils.encode_payload(job_to_dir_str)


@init_db
def get_job_log_dirs(job_ids: list[int]) -> dict[int, str]:
    """Returns the relative paths to the log files for jobs with globbing."""
    assert _DB is not None
    query_str = ' OR '.join(['job_id GLOB (?)'] * len(job_ids))
    _DB.cursor.execute(
        f"""\
            SELECT * FROM jobs
            WHERE {query_str}""", job_ids)
    rows = _DB.cursor.fetchall()
    job_to_dir: dict[int, str] = {}
    for row in rows:
        job_id = row[JobInfoLoc.JOB_ID.value]
        if row[JobInfoLoc.LOG_PATH.value]:
            job_to_dir[job_id] = row[JobInfoLoc.LOG_PATH.value]
        else:
            run_timestamp = row[JobInfoLoc.RUN_TIMESTAMP.value]
            job_to_dir[job_id] = os.path.join(constants.SKY_LOGS_DIRECTORY,
                                              run_timestamp)
    return job_to_dir


JobLibCodeGen = job_lib_codegen.JobLibCodeGen
# Preserve the long-standing facade identity for introspection and pickle.
JobLibCodeGen.__module__ = __name__
