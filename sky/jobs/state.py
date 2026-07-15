"""The database for managed jobs status."""
# TODO(zhwu): maybe use file based status instead of database, so
# that we can easily switch to a s3-based storage.
import asyncio
import collections
from collections.abc import Awaitable
from collections.abc import Callable
import datetime
import json
import time
import typing
from typing import Any, Optional

import sqlalchemy
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext import asyncio as sql_async
from sqlalchemy.ext import declarative

from sky import exceptions
from sky import resources as resources_lib
from sky import sky_logging
from sky.dag import DagExecution
from sky.jobs.status_types import BatchLifecycleTransition
from sky.jobs.status_types import ControllerPidRecord
from sky.jobs.status_types import JobCancellationState
from sky.jobs.status_types import ManagedJobScheduleState
from sky.jobs.status_types import ManagedJobStatus
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils
from sky.utils.db import retries as db_retries
from sky.utils.plugin_extensions import ExternalClusterFailure

if typing.TYPE_CHECKING:
    from sqlalchemy.engine import row

# Separate callback types for sync and async contexts
SyncCallbackType = Callable[[str], None]
AsyncCallbackType = Callable[[str], Awaitable[Any]]
CallbackType = SyncCallbackType | AsyncCallbackType

logger = sky_logging.init_logger(__name__)

_DB_RETRY_TIMES = 30

# 30 days retention for job events
DEFAULT_JOB_EVENT_RETENTION_HOURS = 30 * 24.0
# Run the job event retention daemon every hour
JOB_EVENT_DAEMON_INTERVAL_SECONDS = 3600
# Bound parameters per token upsert while keeping all chunks in one transaction.
_API_ACCESS_TOKEN_UPSERT_BATCH_SIZE = 1000

Base = declarative.declarative_base()

# === Database schema ===
# `spot` table contains all the finest-grained tasks, including all the
# tasks of a managed job (called spot for legacy reason, as it is generalized
# from the previous managed spot jobs). All tasks of the same job will have the
# same `spot_job_id`.
# The `job_name` column is now deprecated. It now holds the task's name, i.e.,
# the same content as the `task_name` column.
# The `job_id` is now not really a job id, but a only a unique
# identifier/primary key for all the tasks. We will use `spot_job_id`
# to identify the job.
# TODO(zhwu): schema migration may be needed.

spot_table = sqlalchemy.Table(
    'spot',
    Base.metadata,
    sqlalchemy.Column('job_id',
                      sqlalchemy.Integer,
                      primary_key=True,
                      autoincrement=True),
    sqlalchemy.Column('job_name', sqlalchemy.Text),
    sqlalchemy.Column('resources', sqlalchemy.Text),
    sqlalchemy.Column('submitted_at', sqlalchemy.Float),
    # Indexed because non-terminal-status filtering on this column is on the
    # hot path for the pool dashboard (per-pool job listing) and skip_finished
    # queries; without it the filter is a full table scan over all (including
    # finished) tasks.
    sqlalchemy.Column('status', sqlalchemy.Text, index=True),
    sqlalchemy.Column('run_timestamp', sqlalchemy.Text),
    sqlalchemy.Column('start_at', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('end_at', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('last_recovered_at',
                      sqlalchemy.Float,
                      server_default='-1'),
    sqlalchemy.Column('recovery_count', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('job_duration', sqlalchemy.Float, server_default='0'),
    sqlalchemy.Column('failure_reason', sqlalchemy.Text),
    sqlalchemy.Column('spot_job_id', sqlalchemy.Integer, index=True),
    sqlalchemy.Column('task_id', sqlalchemy.Integer, server_default='0'),
    sqlalchemy.Column('task_name', sqlalchemy.Text),
    sqlalchemy.Column('specs', sqlalchemy.Text),
    sqlalchemy.Column('local_log_file', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('metadata', sqlalchemy.Text, server_default='{}'),
    sqlalchemy.Column('links', sqlalchemy.JSON, server_default=None),
    sqlalchemy.Column('logs_cleaned_at', sqlalchemy.Float, server_default=None),
    sqlalchemy.Column('full_resources', sqlalchemy.JSON, server_default=None),
    # Whether this task is a primary task (True) or auxiliary task (False)
    # within a job group. NULL for non-job-group jobs (single jobs/pipelines).
    # Auxiliary tasks are terminated when all primary tasks complete.
    sqlalchemy.Column('is_primary_in_job_group',
                      sqlalchemy.Boolean,
                      server_default=None),
    # Optional plugin-provided override for the user-facing status. The core
    # state machine never reads this column; it always uses `status`. Read
    # paths (status counts, status filter, returned status) may surface this
    # value instead of `status` via the optional `status_expr` seam, so a
    # plugin can present a refined status (e.g. show a still-launching job as
    # PENDING while it waits in an external scheduler queue) without altering
    # the underlying job lifecycle. NULL means "no override".
    sqlalchemy.Column('status_override', sqlalchemy.Text, server_default=None),
)

job_info_table = sqlalchemy.Table(
    'job_info',
    Base.metadata,
    sqlalchemy.Column('spot_job_id',
                      sqlalchemy.Integer,
                      primary_key=True,
                      autoincrement=True),
    sqlalchemy.Column('name', sqlalchemy.Text),
    sqlalchemy.Column('schedule_state', sqlalchemy.Text),
    sqlalchemy.Column('controller_pid', sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('controller_pid_started_at',
                      sqlalchemy.Float,
                      server_default=None),
    sqlalchemy.Column('dag_yaml_path', sqlalchemy.Text),
    sqlalchemy.Column('env_file_path', sqlalchemy.Text),
    sqlalchemy.Column('dag_yaml_content', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('env_file_content', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('config_file_content',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('user_hash', sqlalchemy.Text),
    sqlalchemy.Column('workspace', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('priority',
                      sqlalchemy.Integer,
                      server_default=str(constants.DEFAULT_PRIORITY)),
    sqlalchemy.Column('priority_class', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('entrypoint', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('original_user_yaml_path',
                      sqlalchemy.Text,
                      server_default=None),
    sqlalchemy.Column('original_user_yaml_content',
                      sqlalchemy.Text,
                      server_default=None),
    # Indexed: every per-pool dashboard query and pool_status request filters
    # by this column. Without an index a job_info table with tens of thousands
    # of (mostly finished) rows turns each pool lookup into a full scan.
    sqlalchemy.Column('pool', sqlalchemy.Text, server_default=None, index=True),
    # Indexed: pool_status fetches per-replica used_by lists by filtering on
    # current_cluster_name; the index keeps that fast when many jobs share
    # the same pool.
    sqlalchemy.Column('current_cluster_name',
                      sqlalchemy.Text,
                      server_default=None,
                      index=True),
    sqlalchemy.Column('job_id_on_pool_cluster',
                      sqlalchemy.Integer,
                      server_default=None),
    sqlalchemy.Column('pool_hash', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('controller_logs_cleaned_at',
                      sqlalchemy.Float,
                      server_default=None),
    # DAG execution mode: 'parallel' (job group) or 'serial' (pipeline/single)
    sqlalchemy.Column('execution',
                      sqlalchemy.Text,
                      server_default=DagExecution.SERIAL.value),
    # Infrastructure columns for efficient filtering/sorting
    sqlalchemy.Column('cloud', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('region', sqlalchemy.Text, server_default=None),
    sqlalchemy.Column('zone', sqlalchemy.Text, server_default=None),
    # Whether this job is a batch coordinator (ds.map()).  Batch jobs are
    # serialized one-at-a-time per pool by the scheduler.
    sqlalchemy.Column('is_batch',
                      sqlalchemy.Boolean,
                      server_default=sqlalchemy.sql.expression.false()),
    # Durable fencing token for the coordinator incarnation that currently
    # owns this Batch job.  Every attempt mutation checks this value so a
    # replacement controller immediately fences its predecessor.
    sqlalchemy.Column('batch_coordinator_token',
                      sqlalchemy.Text,
                      server_default=None),
    # Node names for dashboard display (comma-separated)
    sqlalchemy.Column('node_names', sqlalchemy.Text, server_default=None),
    # In consolidation mode, managed jobs shares the filemount blob managed
    # by API server. This id is a reference to the blob.
    sqlalchemy.Column('file_mounts_blob_id',
                      sqlalchemy.Text,
                      server_default=None),
)

# Separate table for API access token IDs associated with managed jobs.
# Maps job_id -> token_id for cleanup when the job completes.
api_access_token_table = sqlalchemy.Table(
    'api_access_tokens',
    Base.metadata,
    sqlalchemy.Column('job_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('token_id', sqlalchemy.Text, nullable=False),
)

# TODO(cooperc): drop the table in a migration
ha_recovery_script_table = sqlalchemy.Table(
    'ha_recovery_script',
    Base.metadata,
    sqlalchemy.Column('job_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('script', sqlalchemy.Text),
)

job_events_table = sqlalchemy.Table(
    'job_events',
    Base.metadata,
    sqlalchemy.Column('id',
                      sqlalchemy.Integer,
                      primary_key=True,
                      autoincrement=True),
    # See comment above for explanation of the legacy spot_job_id and
    # task_id columns.
    sqlalchemy.Column('spot_job_id', sqlalchemy.Integer, index=True),
    sqlalchemy.Column('task_id', sqlalchemy.Integer, index=True),
    sqlalchemy.Column('new_status', sqlalchemy.Text),
    sqlalchemy.Column('code', sqlalchemy.Text),
    sqlalchemy.Column('reason', sqlalchemy.Text),
    sqlalchemy.Column('timestamp',
                      sqlalchemy.DateTime(timezone=True),
                      index=True),
)

batch_state_table = sqlalchemy.Table(
    'batch_state',
    Base.metadata,
    sqlalchemy.Column('job_id', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('batch_idx', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('start_idx', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('end_idx', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('status',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='PENDING'),
    sqlalchemy.Column('worker_cluster', sqlalchemy.Text),
    sqlalchemy.Column('retry_count',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    # Monotonically increasing fencing token.  Every successful claim gets a
    # new value; state transitions from an older controller incarnation are
    # rejected once a newer attempt has claimed the batch.
    sqlalchemy.Column('attempt_id',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='0'),
    # Coordinator incarnation that claimed the current attempt.  This remains
    # set after the attempt leaves DISPATCHED so replacement coordinators can
    # identify exactly which token-scoped worker services may be stale.
    sqlalchemy.Column('attempt_owner_token', sqlalchemy.Text),
    sqlalchemy.Column('lease_expires_at', sqlalchemy.Float),
    # Earliest wall-clock time at which a failed batch may be claimed again.
    # Persisting this makes retry backoff survive controller restarts.
    sqlalchemy.Column('next_retry_at', sqlalchemy.Float),
    sqlalchemy.Column('updated_at', sqlalchemy.Float),
    sqlalchemy.PrimaryKeyConstraint('job_id', 'batch_idx'),
)

# Durable launch intents for long-running Batch worker services.  The row is
# inserted before the external ``sdk.exec`` call, then filled with the request
# ID and exact worker job ID as they become available.  This bridges worker
# launches that happen before any batch attempt is claimed and lets a later
# coordinator clean only the exact external job created by an older one.
batch_worker_table = sqlalchemy.Table(
    'batch_worker',
    Base.metadata,
    sqlalchemy.Column('job_id', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('coordinator_token', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('worker_cluster', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('worker_job_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('launch_request_id', sqlalchemy.Text),
    sqlalchemy.Column('worker_job_id', sqlalchemy.Integer),
    sqlalchemy.Column('updated_at', sqlalchemy.Float),
    sqlalchemy.PrimaryKeyConstraint('job_id', 'coordinator_token',
                                    'worker_cluster'),
)

# Subquery that aggregates batch_state into per-job progress counts.
# Used by jobs-queue queries to supply batch_total_batches and
# batch_completed_batches without denormalized columns on job_info.
_batch_progress_subquery = sqlalchemy.select(
    batch_state_table.c.job_id,
    sqlalchemy.func.count().label(  # pylint: disable=not-callable
        'batch_total_batches'),
    sqlalchemy.func.count(  # pylint: disable=not-callable
        sqlalchemy.case((batch_state_table.c.status
                         == 'COMPLETED', 1),)).label('batch_completed_batches'),
).group_by(batch_state_table.c.job_id).subquery('batch_progress')


def create_table(engine: sqlalchemy.engine.Engine):
    # Enable WAL mode to avoid locking issues.
    # See: issue #3863, #1441 and PR #1509
    # https://github.com/microsoft/WSL/issues/2395
    # TODO(romilb): We do not enable WAL for WSL because of known issue in WSL.
    #  This may cause the database locked problem from WSL issue #1441.
    if (engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value and
            not common_utils.is_wsl()):
        try:
            with orm.Session(engine) as session:
                session.execute(sqlalchemy.text('PRAGMA journal_mode=WAL'))
                session.execute(sqlalchemy.text('PRAGMA synchronous=1'))
                session.commit()
        except sqlalchemy_exc.OperationalError as e:
            if 'database is locked' not in str(e):
                raise
            # If the database is locked, it is OK to continue, as the WAL mode
            # is not critical and is likely to be enabled by other processes.

    migration_utils.safe_alembic_upgrade(engine,
                                         migration_utils.SPOT_JOBS_DB_NAME,
                                         migration_utils.SPOT_JOBS_VERSION)


_db_manager = db_utils.DatabaseManager('spot_jobs', create_table)


def _supports_update_returning(engine: sqlalchemy.engine.Engine) -> bool:
    """Whether UPDATE ... RETURNING is supported on the active dialect."""
    return bool(getattr(engine.dialect, 'update_returning', False))


async def _retry_session(operation):
    """Run `operation(session)` in a fresh async session with retry on
    transient DB errors. Use when a function has non-DB side effects
    (event logs, callbacks) that must run exactly once; wrap only the
    session block with this helper. For pure-leaf DB functions, prefer
    the `@db_retries.retry_async` decorator on the function itself.
    """

    async def _do(attempt):  # pylint: disable=unused-argument
        del attempt
        engine = await _db_manager.get_async_engine()
        async with sql_async.AsyncSession(engine) as session:
            return await operation(session)

    return await db_retries.with_db_retries_async(_do)


async def _describe_task_transition_failure(session: sql_async.AsyncSession,
                                            job_id: int, task_id: int) -> str:
    """Return a human-readable description when a task transition fails."""
    details = 'Couldn\'t fetch the task details.'
    try:
        debug_result = await session.execute(
            sqlalchemy.select(spot_table.c.status, spot_table.c.end_at).where(
                sqlalchemy.and_(spot_table.c.spot_job_id == job_id,
                                spot_table.c.task_id == task_id)))
        rows = debug_result.mappings().all()
        details = (f'{len(rows)} rows matched job {job_id} and task '
                   f'{task_id}.')
        for row in rows:
            status = row['status']
            end_at = row['end_at']
            details += f' Status: {status}, End time: {end_at}.'
    except Exception as exc:  # pylint: disable=broad-except
        details += f' Error fetching task details: {exc}'
    return details


async def _retry_task_status_update(
    job_id: int,
    task_id: int,
    target_status: 'ManagedJobStatus',
    update: Callable[[sql_async.AsyncSession], Awaitable[int]],
    failure_prefix: str,
) -> None:
    """Run a one-row task status update with commit-lost-safe retry."""
    prior_update_matched = False

    async def _op(attempt: int) -> None:
        nonlocal prior_update_matched
        engine = await _db_manager.get_async_engine()
        async with sql_async.AsyncSession(engine) as session:
            count = await update(session)
            if count == 1:
                prior_update_matched = True
            await session.commit()
            if count == 1:
                return
            if count == 0 and attempt > 0 and prior_update_matched:
                current = await session.execute(
                    sqlalchemy.select(spot_table.c.status).where(
                        sqlalchemy.and_(spot_table.c.spot_job_id == job_id,
                                        spot_table.c.task_id == task_id)))
                row = current.fetchone()
                if row is not None and row[0] == target_status.value:
                    return
            details = await _describe_task_transition_failure(
                session, job_id, task_id)
            message = f'{failure_prefix} ({count} rows updated. {details})'
            logger.error(message)
            raise exceptions.ManagedJobStatusError(message)

    await db_retries.with_db_retries_async(_op)


async def _retry_schedule_state_update(
    job_id: int,
    target_state: 'ManagedJobScheduleState',
    update: Callable[[sql_async.AsyncSession], Awaitable[int]],
    idempotent: bool = False,
) -> None:
    """Run a one-row schedule-state update with commit-lost-safe retry."""
    prior_update_matched = False

    async def _op(attempt: int) -> None:
        nonlocal prior_update_matched
        engine = await _db_manager.get_async_engine()
        async with sql_async.AsyncSession(engine) as session:
            count = await update(session)
            if count == 1:
                prior_update_matched = True
            await session.commit()
            if count == 1 or idempotent:
                return
            assert count == 0, (job_id, count)
            if count == 0 and attempt > 0 and prior_update_matched:
                current = await session.execute(
                    sqlalchemy.select(job_info_table.c.schedule_state).where(
                        job_info_table.c.spot_job_id == job_id))
                row = current.fetchone()
                if row is not None and row[0] == target_state.value:
                    return
            assert False, (job_id, count)

    await db_retries.with_db_retries_async(_op)


# job_duration is the time a job actually runs (including the
# setup duration) before last_recover, excluding the provision
# and recovery time.
# If the job is not finished:
# total_job_duration = now() - last_recovered_at + job_duration
# If the job is not finished:
# total_job_duration = end_at - last_recovered_at + job_duration
#
# Column names to be used in the jobs dict returned to the caller,
# e.g., via sky jobs queue. These may not correspond to actual
# column names in the DB and it corresponds to the combined view
# by joining the spot and job_info tables.
def _get_jobs_dict(r: 'row.RowMapping') -> dict[str, Any]:
    # WARNING: If you update these you may also need to update GetJobTable in
    # the skylet ManagedJobsServiceImpl.
    return {
        '_job_id': r.get('job_id'),  # from spot table
        '_task_name': r.get('job_name'),  # deprecated, from spot table
        'resources': r.get('resources'),
        'submitted_at': r.get('submitted_at'),
        'status': r.get('status'),
        'run_timestamp': r.get('run_timestamp'),
        'start_at': r.get('start_at'),
        'end_at': r.get('end_at'),
        'last_recovered_at': r.get('last_recovered_at'),
        'recovery_count': r.get('recovery_count'),
        'job_duration': r.get('job_duration'),
        'failure_reason': r.get('failure_reason'),
        'job_id': r.get(spot_table.c.spot_job_id
                       ),  # ambiguous, use table.column
        'task_id': r.get('task_id'),
        'task_name': r.get('task_name'),
        'specs': r.get('specs'),
        'local_log_file': r.get('local_log_file'),
        'metadata': r.get('metadata'),
        'links': r.get('links'),  # SQLAlchemy JSON type, already parsed
        # columns from job_info table (some may be None for legacy jobs)
        '_job_info_job_id': r.get(job_info_table.c.spot_job_id
                                 ),  # ambiguous, use table.column
        'job_name': r.get('name'),  # from job_info table
        'schedule_state': r.get('schedule_state'),
        'controller_pid': r.get('controller_pid'),
        'controller_pid_started_at': r.get('controller_pid_started_at'),
        # the _path columns are for backwards compatibility, use the _content
        # columns instead
        'dag_yaml_path': r.get('dag_yaml_path'),
        'env_file_path': r.get('env_file_path'),
        'dag_yaml_content': r.get('dag_yaml_content'),
        'env_file_content': r.get('env_file_content'),
        'config_file_content': r.get('config_file_content'),
        'user_hash': r.get('user_hash'),
        'workspace': r.get('workspace'),
        'priority': r.get('priority'),
        'priority_class': r.get('priority_class'),
        'entrypoint': r.get('entrypoint'),
        'original_user_yaml_path': r.get('original_user_yaml_path'),
        'original_user_yaml_content': r.get('original_user_yaml_content'),
        'pool': r.get('pool'),
        'current_cluster_name': r.get('current_cluster_name'),
        'job_id_on_pool_cluster': r.get('job_id_on_pool_cluster'),
        'pool_hash': r.get('pool_hash'),
        # Whether this task is primary (True) or auxiliary (False) in a job
        # group. NULL for non-job-group jobs.
        'is_primary_in_job_group': r.get('is_primary_in_job_group'),
        # Execution mode: 'parallel' (job group) or 'serial' (pipeline/single)
        'execution': r.get('execution'),
        # Infrastructure columns for filtering/sorting
        'cloud': r.get('cloud'),
        'region': r.get('region'),
        'zone': r.get('zone'),
        # Batch progress columns
        'is_batch': r.get('is_batch'),
        'batch_total_batches': r.get('batch_total_batches'),
        'batch_completed_batches': r.get('batch_completed_batches'),
        'node_names': common_utils.get_display_node_names(r.get('node_names')),
    }


# === Status transition functions ===
def set_job_info_without_job_id(name: str,
                                workspace: str,
                                entrypoint: str,
                                pool: str | None,
                                pool_hash: str | None,
                                user_hash: str | None,
                                execution: str | None = None,
                                is_batch: bool = False,
                                file_mounts_blob_id: str | None = None) -> int:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')

        insert_stmt = insert_func(job_info_table).values(
            name=name,
            schedule_state=ManagedJobScheduleState.INACTIVE.value,
            workspace=workspace,
            entrypoint=entrypoint,
            pool=pool,
            pool_hash=pool_hash,
            user_hash=user_hash,
            execution=execution,
            is_batch=is_batch,
            file_mounts_blob_id=file_mounts_blob_id,
        )

        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            result = session.execute(insert_stmt)
            ret = result.lastrowid
            session.commit()
            return ret
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            result = session.execute(
                insert_stmt.returning(job_info_table.c.spot_job_id))
            ret = result.scalar()
            session.commit()
            return ret
        else:
            raise ValueError('Unsupported database dialect')


def set_pending(
    job_id: int,
    task_id: int,
    task_name: str,
    resources_str: str,
    metadata: str,
    is_primary_in_job_group: bool | None = None,
):
    """Set the task to pending state."""
    add_job_event(job_id, task_id, ManagedJobStatus.PENDING,
                  'Job submitted to queue')

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.insert(spot_table).values(
                spot_job_id=job_id,
                task_id=task_id,
                task_name=task_name,
                resources=resources_str,
                metadata=metadata,
                status=ManagedJobStatus.PENDING.value,
                is_primary_in_job_group=is_primary_in_job_group,
            ))
        session.commit()


async def set_backoff_pending_async(job_id: int, task_id: int):
    """Set the task to PENDING state if it is in backoff.

    This should only be used to transition from STARTING or RECOVERING back to
    PENDING.
    """
    await add_job_event_async(job_id, task_id, ManagedJobStatus.PENDING,
                              'Job is in backoff')

    async def _op(session: sql_async.AsyncSession) -> int:
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.status.in_([
                        ManagedJobStatus.STARTING.value,
                        ManagedJobStatus.RECOVERING.value
                    ]),
                    spot_table.c.end_at.is_(None),
                )).values({spot_table.c.status: ManagedJobStatus.PENDING.value})
        )
        return result.rowcount

    await _retry_task_status_update(job_id, task_id, ManagedJobStatus.PENDING,
                                    _op,
                                    'Failed to set the task back to pending.')
    # Do not call callback_func here, as we don't use the callback for PENDING.


async def set_restarting_async(job_id: int, task_id: int, recovering: bool):
    """Set the task back to STARTING or RECOVERING from PENDING.

    This should not be used for the initial transition from PENDING to STARTING.
    In that case, use set_starting instead. This function should only be used
    after using set_backoff_pending to transition back to PENDING during
    launch retry backoff.
    """
    target_status = ManagedJobStatus.STARTING
    if recovering:
        target_status = ManagedJobStatus.RECOVERING

    await add_job_event_async(job_id, task_id, target_status,
                              'Job is restarting')

    async def _op(session):
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.end_at.is_(None),
                )).values({spot_table.c.status: target_status.value}))
        count = result.rowcount
        await session.commit()
        logger.debug(f'back to {target_status}')
        if count != 1:
            details = await _describe_task_transition_failure(
                session, job_id, task_id)
            message = (f'Failed to set the task back to {target_status}. '
                       f'({count} rows updated. {details})')
            logger.error(message)
            raise exceptions.ManagedJobStatusError(message)

    await _retry_session(_op)
    # Do not call callback_func here, as it should only be invoked for the
    # initial (pre-`set_backoff_pending`) transition to STARTING or RECOVERING.


def set_failed(
    job_id: int,
    task_id: int | None,
    failure_type: ManagedJobStatus,
    failure_reason: str,
    callback_func: CallbackType | None = None,
    end_time: float | None = None,
    override_terminal: bool = False,
):
    """Set an entire job or task to failed.

    By default, don't override tasks that are already terminal (that is, for
    which end_at is already set).

    Args:
        job_id: The job id.
        task_id: The task id. If None, all non-finished tasks of the job will
            be set to failed.
        failure_type: The failure type. One of ManagedJobStatus.FAILED_*.
        failure_reason: The failure reason.
        end_time: The end time. If None, the current time will be used.
        override_terminal: If True, override the current status even if end_at
            is already set.
    """
    engine = _db_manager.get_engine()
    assert failure_type.is_failed(), failure_type
    end_time = time.time() if end_time is None else end_time

    fields_to_set: dict[str, Any] = {
        spot_table.c.status: failure_type.value,
        spot_table.c.failure_reason: failure_reason,
    }
    updated = False
    with orm.Session(engine) as session:
        # Get previous status
        previous_status = session.execute(
            sqlalchemy.select(spot_table.c.status).where(
                spot_table.c.spot_job_id == job_id)).fetchone()[0]
        previous_status = ManagedJobStatus(previous_status)
        if previous_status == ManagedJobStatus.RECOVERING:
            # If the job is recovering, we should set the last_recovered_at to
            # the end_time, so that the end_at - last_recovered_at will not be
            # affect the job duration calculation.
            fields_to_set[spot_table.c.last_recovered_at] = end_time
        where_conditions = [spot_table.c.spot_job_id == job_id]
        if task_id is not None:
            where_conditions.append(spot_table.c.task_id == task_id)

        # Handle failure_reason prepending when override_terminal is True
        if override_terminal:
            # Get existing failure_reason with row lock to prevent race
            # conditions
            existing_reason_result = session.execute(
                sqlalchemy.select(spot_table.c.failure_reason).where(
                    sqlalchemy.and_(*where_conditions)).with_for_update())
            existing_reason_row = existing_reason_result.fetchone()
            if existing_reason_row and existing_reason_row[0]:
                # Prepend new failure reason to existing one
                fields_to_set[spot_table.c.failure_reason] = (
                    failure_reason + '. Previously: ' + existing_reason_row[0])
            # Use COALESCE for end_at to avoid overriding the existing end_at if
            # it's already set.
            fields_to_set[spot_table.c.end_at] = sqlalchemy.func.coalesce(
                spot_table.c.end_at, end_time)
        else:
            fields_to_set[spot_table.c.end_at] = end_time
            where_conditions.append(spot_table.c.end_at.is_(None))
        count = session.query(spot_table).filter(
            sqlalchemy.and_(*where_conditions)).update(fields_to_set)
        session.commit()
        updated = count > 0
    if callback_func and updated:
        callback_func('FAILED')
    logger.info(failure_reason)


def set_pending_cancelled(job_id: int):
    """Set the job as cancelled, if it is PENDING and WAITING/INACTIVE.

    This may fail if the job is not PENDING, e.g. another process has changed
    its state in the meantime.

    Returns:
        True if the job was cancelled, False otherwise.
    """
    add_job_event(job_id, None, ManagedJobStatus.CANCELLED,
                  'Job has been cancelled')
    engine = _db_manager.get_engine()
    count = 0
    with orm.Session(engine) as session:
        # Subquery to get the spot_job_ids that match the joined condition.
        # Build it as a select() construct (rather than Query.subquery()) so it
        # can be passed directly to in_() without SQLAlchemy emitting a
        # "Coercing Subquery object into a select()" warning.
        subquery = sqlalchemy.select(spot_table.c.job_id).select_from(
            spot_table.join(
                job_info_table,
                spot_table.c.spot_job_id == job_info_table.c.spot_job_id)
        ).where(
            spot_table.c.spot_job_id == job_id,
            spot_table.c.status == ManagedJobStatus.PENDING.value,
            # Note: it's possible that a WAITING job actually needs to be
            # cleaned up, if we are in the middle of an upgrade/recovery and
            # the job is waiting to be reclaimed by a new controller. But,
            # in this case the status will not be PENDING.
            sqlalchemy.or_(
                job_info_table.c.schedule_state ==
                ManagedJobScheduleState.WAITING.value,
                job_info_table.c.schedule_state ==
                ManagedJobScheduleState.INACTIVE.value,
            ),
        )

        count = session.query(spot_table).filter(
            spot_table.c.job_id.in_(subquery)).update(
                {spot_table.c.status: ManagedJobStatus.CANCELLED.value},
                synchronize_session=False)
        session.commit()
        return count > 0


@db_retries.retry
def set_local_log_file(job_id: int, task_id: int | None, local_log_file: str):
    """Set the local log file for a job."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        where_conditions = [spot_table.c.spot_job_id == job_id]
        if task_id is not None:
            where_conditions.append(spot_table.c.task_id == task_id)
        session.query(spot_table).filter(
            sqlalchemy.and_(*where_conditions)).update(
                {spot_table.c.local_log_file: local_log_file})
        session.commit()


# ======== utility functions ========
def get_nonterminal_job_ids_by_name(name: str | None,
                                    user_hash: str | None = None,
                                    all_users: bool = False) -> list[int]:
    """Get non-terminal job ids by name.

    If name is None:
    1. if all_users is False, get for the given user_hash
    2. otherwise, get for all users
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        # Build the query using SQLAlchemy core
        query = sqlalchemy.select(
            spot_table.c.spot_job_id.distinct()).select_from(
                spot_table.outerjoin(
                    job_info_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
                ))
        where_conditions = [
            ~spot_table.c.status.in_([
                status.value for status in ManagedJobStatus.terminal_statuses()
            ])
        ]
        if name is None and not all_users:
            if user_hash is None:
                # For backwards compatibility. With codegen, USER_ID_ENV_VAR
                # was set to the correct value by the jobs controller, as
                # part of ManagedJobCodeGen._build(). This is no longer the
                # case for the Skylet gRPC server, which is why we need to
                # pass it explicitly through the request body.
                logger.debug('user_hash is None, using current user hash')
                user_hash = common_utils.get_user_hash()
            where_conditions.append(job_info_table.c.user_hash == user_hash)
        if name is not None:
            # We match the job name from `job_info` for the jobs submitted after
            # #1982, and from `spot` for the jobs submitted before #1982, whose
            # job_info is not available.
            where_conditions.append(
                sqlalchemy.or_(
                    job_info_table.c.name == name,
                    sqlalchemy.and_(job_info_table.c.name.is_(None),
                                    spot_table.c.task_name == name),
                ))
        query = query.where(sqlalchemy.and_(*where_conditions)).order_by(
            spot_table.c.spot_job_id.desc())
        rows = session.execute(query).fetchall()
        job_ids = [row[0] for row in rows if row[0] is not None]
        return job_ids


def get_jobs_to_check_status(job_id: int | None = None) -> list[int]:
    """Get jobs that need controller process checking.

    Args:
        job_id: Optional job ID to check. If None, checks all jobs.

    Returns a list of job_ids, including the following:
    - Jobs that have a schedule_state that is not DONE
    - Jobs have schedule_state DONE but are in a non-terminal status
    - Legacy jobs (that is, no schedule state) that are in non-terminal status
    """
    where_condition = _get_jobs_to_check_status_condition(job_id)
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            spot_table.c.spot_job_id.distinct()).select_from(
                spot_table.outerjoin(
                    job_info_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id))
        query = query.where(where_condition).order_by(
            spot_table.c.spot_job_id.desc())

        rows = session.execute(query).fetchall()
        return [row[0] for row in rows if row[0] is not None]


def _get_all_task_ids_statuses(
        job_id: int) -> list[tuple[int, ManagedJobStatus]]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        id_statuses = session.execute(
            sqlalchemy.select(
                spot_table.c.task_id,
                spot_table.c.status,
            ).where(spot_table.c.spot_job_id == job_id).order_by(
                spot_table.c.task_id.asc())).fetchall()
        return [(row[0], ManagedJobStatus(row[1])) for row in id_statuses]


def get_all_task_ids_names_statuses_logs(
        job_id: int
) -> list[tuple[int, str, ManagedJobStatus, str, float | None]]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        id_names = session.execute(
            sqlalchemy.select(
                spot_table.c.task_id,
                spot_table.c.task_name,
                spot_table.c.status,
                spot_table.c.local_log_file,
                spot_table.c.logs_cleaned_at,
            ).where(spot_table.c.spot_job_id == job_id).order_by(
                spot_table.c.task_id.asc())).fetchall()
        return [(row[0], row[1], ManagedJobStatus(row[2]), row[3], row[4])
                for row in id_names]


def get_num_tasks(job_id: int) -> int:
    return len(_get_all_task_ids_statuses(job_id))


def get_latest_task_id_from_statuses(
    id_statuses: list[tuple[int, ManagedJobStatus]]
) -> tuple[int | None, ManagedJobStatus | None]:
    """Returns the (task_id, status) of the latest non-terminal task.

    If all tasks are terminal, returns the last task. If the list is empty,
    returns (None, None).
    """
    if not id_statuses:
        return None, None
    task_id, status = next(
        ((tid, st) for tid, st in id_statuses if not st.is_terminal()),
        id_statuses[-1],
    )
    return task_id, status


def get_latest_task_id_status(
        job_id: int) -> tuple[int | None, ManagedJobStatus | None]:
    """Returns the (task id, status) of the latest task of a job.

    The latest means the task that is currently being executed or to be started
    by the controller process. For example, in a managed job with 3 tasks, the
    first task is succeeded, and the second task is being executed. This will
    return (1, ManagedJobStatus.RUNNING).

    If the job_id does not exist, (None, None) will be returned.
    """
    id_statuses = _get_all_task_ids_statuses(job_id)
    return get_latest_task_id_from_statuses(id_statuses)


def get_job_controller_processes(
        job_ids: list[int]) -> dict[int, ControllerPidRecord]:
    """Return controller process records for the requested jobs."""
    if not job_ids:
        return {}

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                job_info_table.c.spot_job_id, job_info_table.c.controller_pid,
                job_info_table.c.controller_pid_started_at).where(
                    job_info_table.c.spot_job_id.in_(job_ids))).fetchall()

    records: dict[int, ControllerPidRecord] = {}
    for job_id, pid, started_at in rows:
        if pid is None:
            continue
        if pid < 0:
            # Between #7051 and #7847, the controller pid was negative to
            # indicate a controller process that can handle multiple jobs.
            pid = -pid
        records[job_id] = ControllerPidRecord(pid=pid, started_at=started_at)
    return records


def get_job_controller_process(job_id: int) -> ControllerPidRecord | None:
    return get_job_controller_processes([job_id]).get(job_id)


def _is_legacy_controller_record(pid: int | None,
                                 started_at: float | None) -> bool:
    if pid is None:
        # Job is from before #4485, so controller_pid is not set.
        return True
    if started_at is not None:
        # controller_pid_started_at is only set after #7847.
        return False
    # Between #7051 and #7847, a negative pid identified the consolidated
    # controller. Positive pids without a start time belong to legacy
    # single-job controllers.
    return pid >= 0


def is_legacy_controller_process(job_id: int) -> bool:
    """Check if the controller process is a legacy single-job controller process

    After #7051, the controller process pid is negative to indicate a new
    multi-job controller process.
    After #7847, the controller process pid is changed back to positive, but
    controller_pid_started_at will also be set.
    """
    # TODO(cooperc): Remove this function for 0.13.0
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                job_info_table.c.controller_pid,
                job_info_table.c.controller_pid_started_at).where(
                    job_info_table.c.spot_job_id == job_id)).fetchone()
        if row is None:
            raise ValueError(f'Job {job_id} not found')
        return _is_legacy_controller_record(row[0], row[1])


def get_status(job_id: int) -> ManagedJobStatus | None:
    _, status = get_latest_task_id_status(job_id)
    return status


def get_failure_reason(job_id: int) -> str | None:
    """Get the failure reason of a job.

    If the job has multiple tasks, we return the first failure reason.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        reason = session.execute(
            sqlalchemy.select(spot_table.c.failure_reason).where(
                spot_table.c.spot_job_id == job_id).order_by(
                    spot_table.c.task_id.asc())).fetchall()
        reason = [r[0] for r in reason if r[0] is not None]
        if not reason:
            return None
        return reason[0]


def get_managed_job_tasks(job_id: int) -> list[dict[str, Any]]:
    """Get managed job tasks for a specific managed job id from the database."""
    engine = _db_manager.get_engine()

    # Join spot and job_info tables to get the job name for each task.
    # We use LEFT OUTER JOIN mainly for backward compatibility, as for an
    # existing controller before #1982, the job_info table may not exist,
    # and all the managed jobs created before will not present in the
    # job_info.
    # Note: we will get the user_hash here, but don't try to call
    # global_user_state.get_user() on it. This runs on the controller, which may
    # not have the user info. Prefer to do it on the API server side.
    query = sqlalchemy.select(
        spot_table,
        job_info_table,
        _batch_progress_subquery.c.batch_total_batches,
        _batch_progress_subquery.c.batch_completed_batches,
    ).select_from(
        spot_table.outerjoin(
            job_info_table,
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id).outerjoin(
                _batch_progress_subquery,
                spot_table.c.spot_job_id == _batch_progress_subquery.c.job_id))
    query = query.where(spot_table.c.spot_job_id == job_id)
    query = query.order_by(spot_table.c.task_id.asc())
    rows = None
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    jobs = []
    for row in rows:
        job_dict = _get_jobs_dict(row._mapping)  # pylint: disable=protected-access
        # WARNING: Keep this decode (enum conversion + job_name fallback) in
        # sync with get_jobs_status_check_info.
        job_dict['status'] = ManagedJobStatus(job_dict['status'])
        job_dict['schedule_state'] = ManagedJobScheduleState(
            job_dict['schedule_state'])
        if job_dict['job_name'] is None:
            job_dict['job_name'] = job_dict['task_name']
        job_dict['metadata'] = json.loads(job_dict['metadata'])

        # Add user YAML content for managed jobs.
        job_dict['user_yaml'] = job_dict.get('original_user_yaml_content')
        if job_dict['user_yaml'] is None:
            # Backwards compatibility - try to read from file path
            yaml_path = job_dict.get('original_user_yaml_path')
            if yaml_path:
                try:
                    with open(yaml_path, encoding='utf-8') as f:
                        job_dict['user_yaml'] = f.read()
                except (FileNotFoundError, OSError) as e:
                    logger.debug('Failed to read original user YAML for job '
                                 f'{job_id} from {yaml_path}: {e}')

        jobs.append(job_dict)
    return jobs


# Cap the ids per ``IN (...)`` so a large refresh never overflows the DB
# bind-parameter limit (SQLite's default is ~999); see
# get_jobs_status_check_info.
_STATUS_CHECK_JOB_ID_CHUNK = 500


def _get_jobs_to_check_status_condition(job_id: int | None = None):
    """Build the filter for jobs that need controller-process checking."""
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]

    # Get jobs that are either:
    # 1. Have schedule state that is not DONE, or
    # 2. Have schedule state DONE AND are in non-terminal status (unexpected
    #    inconsistent state), or
    # 3. Have no schedule state (legacy) AND are in non-terminal status
    condition1 = sqlalchemy.and_(
        job_info_table.c.schedule_state.is_not(None),
        job_info_table.c.schedule_state != ManagedJobScheduleState.DONE.value)
    condition2 = sqlalchemy.and_(
        sqlalchemy.or_(
            job_info_table.c.schedule_state.is_(None),
            job_info_table.c.schedule_state ==
            ManagedJobScheduleState.DONE.value),
        ~spot_table.c.status.in_(terminal_status_values),
    )
    where_condition = sqlalchemy.or_(condition1, condition2)
    if job_id is not None:
        where_condition = sqlalchemy.and_(where_condition,
                                          spot_table.c.spot_job_id == job_id)
    return where_condition


def _merge_jobs_status_check_rows(result: dict[int, dict[str, Any]],
                                  rows: list[Any]) -> None:
    """Decode slim status-check rows into the per-job refresh snapshot."""
    for row in rows:
        mapping = row._mapping  # pylint: disable=protected-access
        job_id = mapping['spot_job_id']
        info = result.get(job_id)
        # WARNING: Keep this decode (enum conversion + job_name fallback)
        # in sync with get_managed_job_tasks.
        if info is None:
            info = {
                'schedule_state': ManagedJobScheduleState(
                    mapping['schedule_state']),
                'controller_pid': mapping['controller_pid'],
                'controller_pid_started_at':
                    mapping['controller_pid_started_at'],
                'pool': mapping['pool'],
                'tasks': [],
            }
            result[job_id] = info
        job_name = mapping['job_info_name']
        if job_name is None:
            job_name = mapping['task_name']
        info['tasks'].append({
            'task_id': mapping['task_id'],
            'status': ManagedJobStatus(mapping['status']),
            'job_name': job_name,
            'task_name': mapping['task_name'],
        })


def get_jobs_to_check_status_info(
        job_id: int | None = None) -> dict[int, dict[str, Any]]:
    """One-query slim snapshot for jobs needing controller-process checking.

    The status-refresh sweep needs two things from the same tables:
    1. identify which jobs require checking; and
    2. fetch the slim per-task fields it actually consumes.

    The old path did those as two round trips. This helper keeps the same
    per-job/per-task shape as ``get_jobs_status_check_info`` but does the
    "which jobs?" filter in a subquery and returns the full slim snapshot in
    one SQL statement.
    """
    engine = _db_manager.get_engine()
    jobs_to_check = sqlalchemy.select(
        spot_table.c.spot_job_id.label('spot_job_id')).select_from(
            spot_table.outerjoin(
                job_info_table,
                spot_table.c.spot_job_id == job_info_table.c.spot_job_id)
        ).where(
            _get_jobs_to_check_status_condition(job_id)).distinct().subquery()
    query = sqlalchemy.select(
        spot_table.c.spot_job_id,
        spot_table.c.task_id,
        spot_table.c.status,
        spot_table.c.task_name,
        job_info_table.c.name.label('job_info_name'),
        job_info_table.c.schedule_state,
        job_info_table.c.controller_pid,
        job_info_table.c.controller_pid_started_at,
        job_info_table.c.pool,
    ).select_from(
        spot_table.outerjoin(
            job_info_table,
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id).join(
                jobs_to_check, spot_table.c.spot_job_id ==
                jobs_to_check.c.spot_job_id)).order_by(
                    spot_table.c.spot_job_id.desc(), spot_table.c.task_id.asc())
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    result: dict[int, dict[str, Any]] = {}
    _merge_jobs_status_check_rows(result, rows)
    return result


def get_jobs_status_check_info(job_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Batched, slim fetch of the fields the status-refresh tick needs.

    ``update_managed_jobs_statuses`` only consumes a handful of small scalar
    columns per job (the per-job ``schedule_state`` / ``controller_pid`` /
    ``controller_pid_started_at`` / ``pool`` from ``job_info``, plus each
    task's ``status`` and identity from ``spot``). The old path calls
    ``get_managed_job_tasks`` once per job -- N heavyweight ``SELECT *`` 3-way
    joins that also pull large text blobs (``dag_yaml_content`` etc.),
    ``json.loads`` the metadata, and may read YAML files, all per refresh tick.
    This collapses that ``1 + N`` pattern into a single slim
    ``WHERE spot_job_id IN (...)`` query (chunked).

    Returns a mapping ``job_id -> {schedule_state, controller_pid,
    controller_pid_started_at, pool, tasks: [{task_id, status, job_name,
    task_name}]}``
    with ``tasks`` ordered by ``task_id``. Job ids with no task rows are absent
    from the result.

    ``job_name`` mirrors ``get_managed_job_tasks`` (public display name with a
    fallback to ``task_name``); ``task_name`` preserves the controller launch
    identity for teardown/recovery logic.
    """
    if not job_ids:
        return {}
    engine = _db_manager.get_engine()
    result: dict[int, dict[str, Any]] = {}
    for start in range(0, len(job_ids), _STATUS_CHECK_JOB_ID_CHUNK):
        chunk = job_ids[start:start + _STATUS_CHECK_JOB_ID_CHUNK]
        query = sqlalchemy.select(
            spot_table.c.spot_job_id,
            spot_table.c.task_id,
            spot_table.c.status,
            spot_table.c.task_name,
            job_info_table.c.name.label('job_info_name'),
            job_info_table.c.schedule_state,
            job_info_table.c.controller_pid,
            job_info_table.c.controller_pid_started_at,
            job_info_table.c.pool,
        ).select_from(
            spot_table.outerjoin(
                job_info_table, spot_table.c.spot_job_id ==
                job_info_table.c.spot_job_id)).where(
                    spot_table.c.spot_job_id.in_(chunk)).order_by(
                        spot_table.c.spot_job_id.asc(),
                        spot_table.c.task_id.asc())
        with orm.Session(engine) as session:
            rows = session.execute(query).fetchall()
        _merge_jobs_status_check_rows(result, rows)
    return result


def get_job_status_check_state(job_id: int) -> dict[str, Any] | None:
    """Fetch the minimal fresh job/task state for a destructive recheck.

    ``update_managed_jobs_statuses`` reuses its sweep-wide task snapshot for
    cleanup, but before any destructive action it must confirm the job still
    has the same controller ownership fields. It also needs to know if every
    task is already terminal, so a controller crash during post-terminal
    cleanup can preserve the task outcome instead of overwriting it with
    ``FAILED_CONTROLLER``. The recheck stays slim: one grouped join on the
    small scalar fields plus a non-terminal task count.
    """
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                job_info_table.c.schedule_state,
                job_info_table.c.controller_pid,
                job_info_table.c.controller_pid_started_at,
                sqlalchemy.func.count(  # pylint: disable=not-callable
                    spot_table.c.task_id).label('task_count'),
                sqlalchemy.func.sum(
                    sqlalchemy.case(
                        (~spot_table.c.status.in_(terminal_status_values), 1),
                        else_=0)).label('nonterminal_task_count'),
            ).select_from(
                job_info_table.outerjoin(
                    spot_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id)).
            where(job_info_table.c.spot_job_id == job_id).group_by(
                job_info_table.c.schedule_state,
                job_info_table.c.controller_pid,
                job_info_table.c.controller_pid_started_at)).fetchone()
    if row is None or row[0] is None:
        return None
    task_count = int(row[3] or 0)
    nonterminal_task_count = int(row[4] or 0)
    return {
        'schedule_state': ManagedJobScheduleState(row[0]),
        'controller_pid': row[1],
        'controller_pid_started_at': row[2],
        'all_tasks_terminal': task_count > 0 and nonterminal_task_count == 0,
    }


def get_job_cancellation_states(
        job_ids: list[int]) -> dict[int, JobCancellationState]:
    """Return slim, batched snapshots for managed-job cancellation.

    Cancellation needs the currently executable task's status together with
    workspace authorization and the controller generation used to choose the
    signal path. Reading those fields together avoids three point queries per
    job and prevents decisions assembled from different lifecycle snapshots.
    """
    if not job_ids:
        return {}

    unique_job_ids = list(dict.fromkeys(job_ids))
    engine = _db_manager.get_engine()
    task_states: dict[int, list[tuple[int, ManagedJobStatus]]] = (
        collections.defaultdict(list))
    job_metadata: dict[int, tuple[str | None, int | None, float | None]] = {}
    for start in range(0, len(unique_job_ids), _STATUS_CHECK_JOB_ID_CHUNK):
        chunk = unique_job_ids[start:start + _STATUS_CHECK_JOB_ID_CHUNK]
        query = sqlalchemy.select(
            spot_table.c.spot_job_id,
            spot_table.c.task_id,
            spot_table.c.status,
            job_info_table.c.workspace,
            job_info_table.c.controller_pid,
            job_info_table.c.controller_pid_started_at,
        ).select_from(
            spot_table.outerjoin(
                job_info_table, spot_table.c.spot_job_id ==
                job_info_table.c.spot_job_id)).where(
                    spot_table.c.spot_job_id.in_(chunk)).order_by(
                        spot_table.c.spot_job_id.asc(),
                        spot_table.c.task_id.asc())
        with orm.Session(engine) as session:
            rows = session.execute(query).fetchall()
        for job_id, task_id, status, workspace, pid, started_at in rows:
            task_states[job_id].append((task_id, ManagedJobStatus(status)))
            job_metadata[job_id] = (workspace, pid, started_at)

    snapshots: dict[int, JobCancellationState] = {}
    for job_id, statuses in task_states.items():
        _, status = get_latest_task_id_from_statuses(statuses)
        assert status is not None, job_id
        workspace, pid, started_at = job_metadata[job_id]
        snapshots[job_id] = JobCancellationState(
            status=status,
            workspace=(constants.SKYPILOT_DEFAULT_WORKSPACE
                       if workspace is None else workspace),
            is_legacy_controller=_is_legacy_controller_record(pid, started_at),
        )
    return snapshots


def has_jobs_requiring_recovery_grace_wait() -> bool:
    """Whether HA leader handoff should pause before managed-job recovery.

    The post-acquire grace wait only matters when a prior leader may still have
    detached controllers alive long enough to race recovery. That requires a
    job which is already claimed by a controller (``controller_pid`` set) or is
    otherwise beyond the pure backlog states (``INACTIVE``/``WAITING``).

    Empty, terminal, or pending-only backlogs can recover immediately without
    paying the fixed sleep.
    """
    engine = _db_manager.get_engine()
    pending_only_states = [
        ManagedJobScheduleState.INACTIVE.value,
        ManagedJobScheduleState.WAITING.value,
    ]
    query = sqlalchemy.select(sqlalchemy.literal(True)).where(
        sqlalchemy.and_(
            job_info_table.c.schedule_state.is_not(None),
            job_info_table.c.schedule_state
            != ManagedJobScheduleState.DONE.value,
            sqlalchemy.or_(
                job_info_table.c.controller_pid.is_not(None),
                sqlalchemy.not_(
                    job_info_table.c.schedule_state.in_(pending_only_states)),
            ),
        )).limit(1)
    with orm.Session(engine) as session:
        return session.execute(query).first() is not None


def _map_response_field_to_db_column(field: str):
    """Map the response field name to an actual SQLAlchemy ColumnElement.

    This ensures we never pass plain strings to SQLAlchemy 2.0 APIs like
    Select.with_only_columns().
    """
    # Explicit aliases differing from actual DB column names
    alias_mapping = {
        '_job_id': spot_table.c.job_id,  # spot.job_id
        '_task_name': spot_table.c.job_name,  # deprecated, from spot table
        'job_id': spot_table.c.spot_job_id,  # public job id -> spot.spot_job_id
        '_job_info_job_id': job_info_table.c.spot_job_id,
        'job_name': job_info_table.c.name,  # public job name -> job_info.name
        # Batch progress from batch_state aggregation subquery
        'batch_total_batches': _batch_progress_subquery.c.batch_total_batches,
        'batch_completed_batches':
            _batch_progress_subquery.c.batch_completed_batches,
    }
    if field in alias_mapping:
        return alias_mapping[field]

    # Try direct match on the `spot` table columns
    if field in spot_table.c:
        return spot_table.c[field]

    # Try direct match on the `job_info` table columns
    if field in job_info_table.c:
        return job_info_table.c[field]

    raise ValueError(f'Unknown field: {field}')


def get_managed_jobs_total() -> int:
    """Get the total number of managed jobs."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(spot_table)).fetchone()
        return result[0] if result else 0


def get_active_file_mounts_blob_ids() -> set[str]:
    """Return blob ids referenced by jobs still in a non-terminal state.

    Used by the API server's blob GC so that a blob is not reclaimed while
    any managed job (including queued / recovering / winding-down jobs) still
    needs its contents.
    """
    engine = _db_manager.get_engine()
    terminal_status_values = [
        s.value for s in ManagedJobStatus.terminal_statuses()
    ]
    non_terminal_job_ids_subquery = (sqlalchemy.select(
        spot_table.c.spot_job_id).where(
            sqlalchemy.or_(
                spot_table.c.status.is_(None),
                sqlalchemy.not_(
                    spot_table.c.status.in_(terminal_status_values)),
            )).distinct())
    query = sqlalchemy.select(
        job_info_table.c.file_mounts_blob_id.distinct()).where(
            sqlalchemy.and_(
                job_info_table.c.file_mounts_blob_id.is_not(None),
                job_info_table.c.spot_job_id.in_(non_terminal_job_ids_subquery),
            ))
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    return {row[0] for row in rows if row[0] is not None}


def get_managed_jobs_highest_priority() -> int:
    """Get the highest priority of the managed jobs."""
    engine = _db_manager.get_engine()
    query = sqlalchemy.select(sqlalchemy.func.max(
        job_info_table.c.priority)).where(
            sqlalchemy.and_(
                job_info_table.c.schedule_state.in_([
                    ManagedJobScheduleState.LAUNCHING.value,
                    ManagedJobScheduleState.ALIVE_BACKOFF.value,
                    ManagedJobScheduleState.WAITING.value,
                    ManagedJobScheduleState.ALIVE_WAITING.value,
                ]),
                job_info_table.c.priority.is_not(None),
            ))
    with orm.Session(engine) as session:
        priority = session.execute(query).fetchone()
        return priority[0] if priority and priority[
            0] is not None else constants.MIN_PRIORITY


def build_managed_jobs_with_filters_no_status_query(
    fields: list[str] | None = None,
    job_ids: list[int] | None = None,
    accessible_workspaces: list[str] | None = None,
    workspace_match: str | None = None,
    name_match: str | None = None,
    pool_match: str | None = None,
    user_hashes: list[str | None] | None = None,
    skip_finished: bool = False,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
    count_only: bool = False,
    count_unique_jobs: bool = False,
    status_count: bool = False,
    status_expr: Optional['sqlalchemy.ColumnElement'] = None,
) -> sqlalchemy.Select:
    """Build a query to get managed jobs from the database with filters.

    status_expr is an optional SQLAlchemy expression used in place of the raw
    ``spot.status`` column whenever a user-facing status is needed (the
    status-count grouping column). It lets a caller surface a refined status
    (e.g. a plugin override) without changing the underlying column. When None,
    the raw ``spot.status`` column is used.

    submitted_after / submitted_before are epoch seconds (matching the
    ``submitted_at`` column) and restrict the result to jobs submitted within
    the inclusive window. A still-active job that hasn't been submitted yet
    (NULL ``submitted_at``) is treated as submitted now, so it is kept or
    dropped by the window like a job submitted at the current moment; a
    terminal job that never got a ``submitted_at`` is excluded from the window.
    """
    # Join spot and job_info tables to get the job name for each task.
    # We use LEFT OUTER JOIN mainly for backward compatibility, as for an
    # existing controller before #1982, the job_info table may not exist,
    # and all the managed jobs created before will not present in the
    # job_info.
    # Note: we will get the user_hash here, but don't try to call
    # global_user_state.get_user() on it. This runs on the controller, which may
    # not have the user info. Prefer to do it on the API server side.
    if count_unique_jobs:
        # Count unique jobs (by spot_job_id), not tasks
        query = sqlalchemy.select(
            sqlalchemy.func.count(  # pylint: disable=not-callable
                sqlalchemy.distinct(spot_table.c.spot_job_id)).label('count'))
    elif count_only:
        query = sqlalchemy.select(sqlalchemy.func.count().label('count'))  # pylint: disable=not-callable
    elif status_count:
        status_col = (status_expr
                      if status_expr is not None else spot_table.c.status)
        query = sqlalchemy.select(status_col.label('status'),
                                  sqlalchemy.func.count().label('count'))  # pylint: disable=not-callable
    else:
        query = sqlalchemy.select(
            spot_table,
            job_info_table,
            _batch_progress_subquery.c.batch_total_batches,
            _batch_progress_subquery.c.batch_completed_batches,
        )
    query = query.select_from(
        spot_table.outerjoin(
            job_info_table,
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id).outerjoin(
                _batch_progress_subquery,
                spot_table.c.spot_job_id == _batch_progress_subquery.c.job_id))
    if skip_finished:
        # Filter out finished jobs at the DB level. If a multi-task job is
        # partially finished, include all its tasks. We do this by first
        # selecting job_ids that have at least one non-terminal task, then
        # restricting the main query to those job_ids.
        terminal_status_values = [
            s.value for s in ManagedJobStatus.terminal_statuses()
        ]
        non_terminal_job_ids_subquery = (sqlalchemy.select(
            spot_table.c.spot_job_id).where(
                sqlalchemy.or_(
                    spot_table.c.status.is_(None),
                    sqlalchemy.not_(
                        spot_table.c.status.in_(terminal_status_values)),
                )).distinct())
        query = query.where(
            spot_table.c.spot_job_id.in_(non_terminal_job_ids_subquery))
    if not count_only and not status_count and fields:
        # Resolve requested field names to explicit ColumnElements from
        # the joined tables.
        selected_columns = [_map_response_field_to_db_column(f) for f in fields]
        query = query.with_only_columns(*selected_columns)
    if job_ids is not None:
        query = query.where(spot_table.c.spot_job_id.in_(job_ids))
    if accessible_workspaces is not None:
        query = query.where(
            job_info_table.c.workspace.in_(accessible_workspaces))
    if workspace_match is not None:
        query = query.where(
            job_info_table.c.workspace.like(f'%{workspace_match}%'))
    if name_match is not None:
        query = query.where(job_info_table.c.name.like(f'%{name_match}%'))
    if pool_match is not None:
        query = query.where(job_info_table.c.pool.like(f'%{pool_match}%'))
    if user_hashes is not None:
        query = query.where(job_info_table.c.user_hash.in_(user_hashes))
    if submitted_after is not None or submitted_before is not None:
        # submitted_at is NULL until a job leaves PENDING (it is set at
        # STARTING). For a still-active job that just means "not submitted
        # yet", so treat it as submitted "now". A terminal job with no
        # submitted_at never started (cancelled/failed before STARTING) and
        # has no submission time, so leave it NULL to exclude it from the
        # window rather than letting it masquerade as "now".
        terminal_values = [
            s.value for s in ManagedJobStatus.terminal_statuses()
        ]
        effective_submitted_at = sqlalchemy.case(
            (spot_table.c.submitted_at.is_not(None), spot_table.c.submitted_at),
            (sqlalchemy.or_(
                spot_table.c.status.is_(None),
                ~spot_table.c.status.in_(terminal_values)), time.time()),
        )
        if submitted_after is not None:
            query = query.where(effective_submitted_at >= submitted_after)
        if submitted_before is not None:
            query = query.where(effective_submitted_at <= submitted_before)
    return query


def build_managed_jobs_with_filters_query(
    fields: list[str] | None = None,
    job_ids: list[int] | None = None,
    accessible_workspaces: list[str] | None = None,
    workspace_match: str | None = None,
    name_match: str | None = None,
    pool_match: str | None = None,
    user_hashes: list[str | None] | None = None,
    statuses: list[str] | None = None,
    skip_finished: bool = False,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
    count_only: bool = False,
    count_unique_jobs: bool = False,
    status_expr: Optional['sqlalchemy.ColumnElement'] = None,
) -> sqlalchemy.Select:
    """Build a query to get managed jobs from the database with filters.

    See build_managed_jobs_with_filters_no_status_query for the meaning of
    status_expr; here it is also used to match the ``statuses`` filter against
    the refined status instead of the raw ``spot.status`` column.
    """
    query = build_managed_jobs_with_filters_no_status_query(
        fields=fields,
        job_ids=job_ids,
        accessible_workspaces=accessible_workspaces,
        workspace_match=workspace_match,
        name_match=name_match,
        pool_match=pool_match,
        user_hashes=user_hashes,
        skip_finished=skip_finished,
        submitted_after=submitted_after,
        submitted_before=submitted_before,
        count_only=count_only,
        count_unique_jobs=count_unique_jobs,
        status_expr=status_expr,
    )
    if statuses is not None:
        status_col = (status_expr
                      if status_expr is not None else spot_table.c.status)
        query = query.where(status_col.in_(statuses))
    return query


def get_status_count_with_filters(
    fields: list[str] | None = None,
    job_ids: list[int] | None = None,
    accessible_workspaces: list[str] | None = None,
    workspace_match: str | None = None,
    name_match: str | None = None,
    pool_match: str | None = None,
    user_hashes: list[str | None] | None = None,
    skip_finished: bool = False,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
    status_expr: Optional['sqlalchemy.ColumnElement'] = None,
) -> dict[str, int]:
    """Get the status count of the managed jobs with filters.

    status_expr, when provided, replaces the raw ``spot.status`` column as the
    grouping key, so counts are bucketed by the refined user-facing status.
    """
    query = build_managed_jobs_with_filters_no_status_query(
        fields=fields,
        job_ids=job_ids,
        accessible_workspaces=accessible_workspaces,
        workspace_match=workspace_match,
        name_match=name_match,
        pool_match=pool_match,
        user_hashes=user_hashes,
        skip_finished=skip_finished,
        submitted_after=submitted_after,
        submitted_before=submitted_before,
        status_count=True,
        status_expr=status_expr,
    )
    status_col = (status_expr
                  if status_expr is not None else spot_table.c.status)
    query = query.group_by(status_col)
    results: dict[str, int] = {}
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
        for status_value, count in rows:
            # status_value is already a string (enum value)
            results[str(status_value)] = int(count)
    return results


def get_status_counts() -> dict[str, int]:
    """Get count of tasks grouped by ManagedJobStatus.

    This is used by the Prometheus ManagedJobsCollector.
    """
    query = sqlalchemy.select(
        spot_table.c.status,
        sqlalchemy.func.count().label('cnt'),  # pylint: disable=not-callable
    ).group_by(spot_table.c.status)

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    results: dict[str, int] = {}
    for status_value, count in rows:
        results[str(status_value)] = int(count)
    return results


def get_status_counts_by_workspace_user_cloud(
) -> list[tuple[str | None, str | None, str | None, str, int]]:
    """Return task counts grouped by workspace/user/cloud/status.

    Each tuple is (workspace, user_hash, cloud, status, count). NULL values
    are returned as None. Used by the Prometheus collector to emit
    per-workspace/user/cloud labeled gauges. Includes both active and
    terminal statuses — terminal counts on a gauge grow monotonically as
    the DB accumulates rows, which is awkward (a Counter incremented at
    state-transition would be more semantically correct), but operators
    explicitly want success/failure visibility and `delta(...)` over a
    window approximates the per-period rate.

    The join is on (spot, job_info) — spot rows whose job_info parent has
    been deleted are skipped, but spot rows whose job_info has NULL
    workspace/user_hash/cloud (PENDING jobs, pre-workspaces rows) are
    kept with None labels.
    """
    query = sqlalchemy.select(
        job_info_table.c.workspace,
        job_info_table.c.user_hash,
        job_info_table.c.cloud,
        spot_table.c.status,
        sqlalchemy.func.count().label('cnt'),  # pylint: disable=not-callable
    ).select_from(
        spot_table.join(
            job_info_table,
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
        )).group_by(
            job_info_table.c.workspace,
            job_info_table.c.user_hash,
            job_info_table.c.cloud,
            spot_table.c.status,
        )

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    return [(row[0], row[1], row[2], str(row[3]), int(row[4])) for row in rows]


def get_managed_jobs_with_filters(
    fields: list[str] | None = None,
    job_ids: list[int] | None = None,
    accessible_workspaces: list[str] | None = None,
    workspace_match: str | None = None,
    name_match: str | None = None,
    pool_match: str | None = None,
    user_hashes: list[str | None] | None = None,
    statuses: list[str] | None = None,
    skip_finished: bool = False,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
    page: int | None = None,
    limit: int | None = None,
    sort_by: str | None = None,
    sort_order: str | None = None,
    status_expr: Optional['sqlalchemy.ColumnElement'] = None,
) -> tuple[list[dict[str, Any]], int]:
    """Get managed jobs from the database with filters.

    status_expr, when provided, is used to match the ``statuses`` filter
    against a refined user-facing status instead of the raw ``spot.status``
    column (see build_managed_jobs_with_filters_no_status_query). The returned
    rows still carry the raw ``status``; callers that want the refined value in
    the result should surface it separately.

    Pagination is by unique jobs (spot_job_id), not by tasks. This means
    if you request page 1 with limit 10, you get all tasks for 10 unique jobs.

    Args:
        sort_by: Field to sort by. Valid values: 'job_id', 'id', 'job_name',
            'name', 'submitted_at', 'status', 'job_duration', 'duration',
            'recovery_count', 'recoveries', 'resources', 'user_hash', 'user',
            'cloud', 'infra'.
        sort_order: Sort direction, 'asc' or 'desc'. Defaults to 'desc'.

    Returns:
        A tuple containing
         - the list of managed jobs (all tasks for the paginated jobs)
         - the total number of unique jobs (not tasks)
    """
    # Column mapping for sorting
    sort_field_map = {
        'job_id': spot_table.c.spot_job_id,
        'id': spot_table.c.spot_job_id,
        'job_name': spot_table.c.job_name,
        'name': spot_table.c.job_name,
        'submitted_at': spot_table.c.submitted_at,
        # Sort by the refined status (status_expr) when provided, so the order
        # matches the displayed/grouped status instead of the raw column.
        'status':
            (status_expr if status_expr is not None else spot_table.c.status),
        'job_duration': spot_table.c.job_duration,
        'duration': spot_table.c.job_duration,
        'recovery_count': spot_table.c.recovery_count,
        'recoveries': spot_table.c.recovery_count,
        'resources': spot_table.c.resources,
        'user_hash': job_info_table.c.user_hash,
        'user': job_info_table.c.user_hash,
        'cloud': job_info_table.c.cloud,
        'infra': job_info_table.c.cloud,  # Sort by cloud for infra
    }

    engine = _db_manager.get_engine()

    # Count unique jobs (by spot_job_id), not tasks
    count_query = build_managed_jobs_with_filters_query(
        fields=None,
        job_ids=job_ids,
        accessible_workspaces=accessible_workspaces,
        workspace_match=workspace_match,
        name_match=name_match,
        pool_match=pool_match,
        user_hashes=user_hashes,
        statuses=statuses,
        skip_finished=skip_finished,
        submitted_after=submitted_after,
        submitted_before=submitted_before,
        count_unique_jobs=True,
        status_expr=status_expr,
    )
    with orm.Session(engine) as session:
        total = session.execute(count_query).fetchone()[0]

    # For pagination, first get the unique job_ids for the current page,
    # then fetch all tasks for those jobs
    if page is not None and limit is not None:
        # Get paginated unique job IDs with ordering
        # Use GROUP BY instead of DISTINCT to allow ORDER BY on different
        # columns (PostgreSQL requires ORDER BY columns to be in SELECT list
        # when using DISTINCT).
        job_ids_subquery = build_managed_jobs_with_filters_query(
            fields=None,
            job_ids=job_ids,
            accessible_workspaces=accessible_workspaces,
            workspace_match=workspace_match,
            name_match=name_match,
            pool_match=pool_match,
            user_hashes=user_hashes,
            statuses=statuses,
            skip_finished=skip_finished,
            submitted_after=submitted_after,
            submitted_before=submitted_before,
            status_expr=status_expr,
        ).with_only_columns(spot_table.c.spot_job_id).group_by(
            spot_table.c.spot_job_id)

        # Apply sorting to pagination query - this determines which jobs appear
        # on each page. Use MAX aggregate for columns not in GROUP BY to ensure
        # PostgreSQL compatibility.
        if sort_by and sort_by in sort_field_map:
            sort_column = sort_field_map[sort_by]
            # Use MAX aggregate for columns that aren't the grouped column
            if sort_column != spot_table.c.spot_job_id:
                sort_column = sqlalchemy.func.max(sort_column)
            if sort_order == 'asc':
                job_ids_subquery = job_ids_subquery.order_by(sort_column.asc())
            else:
                job_ids_subquery = job_ids_subquery.order_by(sort_column.desc())
        else:
            # Default sort: job_id desc (newest first)
            job_ids_subquery = job_ids_subquery.order_by(
                spot_table.c.spot_job_id.desc())

        job_ids_subquery = job_ids_subquery.offset(
            (page - 1) * limit).limit(limit)

        with orm.Session(engine) as session:
            paginated_job_ids = [
                row[0] for row in session.execute(job_ids_subquery).fetchall()
            ]

        if not paginated_job_ids:
            return [], total

        # Now get all tasks for those job IDs
        query = build_managed_jobs_with_filters_query(
            fields=fields,
            job_ids=paginated_job_ids,  # Filter to only paginated jobs
            accessible_workspaces=accessible_workspaces,
            workspace_match=workspace_match,
            name_match=name_match,
            pool_match=pool_match,
            user_hashes=user_hashes,
            statuses=statuses,
            skip_finished=skip_finished,
            status_expr=status_expr,
        )
    else:
        # No pagination - get all jobs
        query = build_managed_jobs_with_filters_query(
            fields=fields,
            job_ids=job_ids,
            accessible_workspaces=accessible_workspaces,
            workspace_match=workspace_match,
            name_match=name_match,
            pool_match=pool_match,
            user_hashes=user_hashes,
            statuses=statuses,
            skip_finished=skip_finished,
            submitted_after=submitted_after,
            submitted_before=submitted_before,
            status_expr=status_expr,
        )

    # Apply sorting
    if sort_by and sort_by in sort_field_map:
        sort_column = sort_field_map[sort_by]
        if sort_order == 'asc':
            query = query.order_by(sort_column.asc(),
                                   spot_table.c.task_id.asc())
        else:
            query = query.order_by(sort_column.desc(),
                                   spot_table.c.task_id.asc())
    else:
        # Default sort: job_id desc, task_id asc
        query = query.order_by(spot_table.c.spot_job_id.desc(),
                               spot_table.c.task_id.asc())
    rows = None
    with orm.Session(engine) as session:
        rows = session.execute(query).fetchall()
    jobs = []
    for row in rows:
        job_dict = _get_jobs_dict(row._mapping)  # pylint: disable=protected-access
        if job_dict.get('status') is not None:
            job_dict['status'] = ManagedJobStatus(job_dict['status'])
        if job_dict.get('schedule_state') is not None:
            job_dict['schedule_state'] = ManagedJobScheduleState(
                job_dict['schedule_state'])
        if job_dict.get('job_name') is None:
            job_dict['job_name'] = job_dict.get('task_name')
        if job_dict.get('metadata') is not None:
            job_dict['metadata'] = json.loads(job_dict['metadata'])

        # Add user YAML content for managed jobs.
        job_dict['user_yaml'] = job_dict.get('original_user_yaml_content')
        if job_dict['user_yaml'] is None:
            # Backwards compatibility - try to read from file path
            yaml_path = job_dict.get('original_user_yaml_path')
            if yaml_path:
                try:
                    with open(yaml_path, encoding='utf-8') as f:
                        job_dict['user_yaml'] = f.read()
                except (FileNotFoundError, OSError) as e:
                    job_id = job_dict.get('job_id')
                    if job_id is not None:
                        logger.debug('Failed to read original user YAML for '
                                     f'job {job_id} from {yaml_path}: {e}')
                    else:
                        logger.debug('Failed to read original user YAML from '
                                     f'{yaml_path}: {e}')

        jobs.append(job_dict)
    return jobs, total


def get_task_name(job_id: int, task_id: int) -> str:
    """Get the task name of a job."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        task_name = session.execute(
            sqlalchemy.select(spot_table.c.task_name).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                ))).fetchone()
        return task_name[0]


def get_latest_job_id() -> int | None:
    """Get the latest job id."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        job_id = session.execute(
            sqlalchemy.select(spot_table.c.spot_job_id).where(
                spot_table.c.task_id == 0).order_by(
                    spot_table.c.submitted_at.desc()).limit(1)).fetchone()
        return job_id[0] if job_id else None


def get_task_specs(job_id: int, task_id: int) -> dict[str, Any]:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        task_specs = session.execute(
            sqlalchemy.select(spot_table.c.specs).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                ))).fetchone()
        return json.loads(task_specs[0])


def scheduler_set_waiting(job_ids: list[int],
                          dag_yaml_content: str,
                          original_user_yaml_content: str,
                          env_file_content: str,
                          config_file_content: str | None,
                          priority: int,
                          priority_class: str | None = None) -> None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        updates = {
            job_info_table.c.schedule_state:
                ManagedJobScheduleState.WAITING.value,
            job_info_table.c.dag_yaml_content: dag_yaml_content,
            job_info_table.c.original_user_yaml_content:
                (original_user_yaml_content),
            job_info_table.c.env_file_content: env_file_content,
            job_info_table.c.config_file_content: config_file_content,
            job_info_table.c.priority: priority,
        }
        if priority_class is not None:
            updates[job_info_table.c.priority_class] = priority_class
        updated_count = session.query(job_info_table).filter(
            sqlalchemy.and_(
                job_info_table.c.spot_job_id.in_(job_ids),)).update(updates)
        session.commit()
        assert updated_count == len(job_ids), (job_ids, updated_count)


@db_retries.retry
def set_job_dag_yaml_content(job_id: int,
                             dag_yaml_content: str,
                             priority: int | None = None,
                             priority_class: str | None = None) -> None:
    """Overwrite a managed job's persisted DAG YAML (and optional priority).

    Lets the persisted job spec be updated out of band after submission. A
    running controller picks the new spec up on its next recovery (it
    re-reads the DAG before each recovery attempt); a brand-new controller or
    a fresh launch reads it directly.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        updates: dict[Any, Any] = {
            job_info_table.c.dag_yaml_content: dag_yaml_content,
        }
        if priority is not None:
            updates[job_info_table.c.priority] = priority
        if priority_class is not None:
            updates[job_info_table.c.priority_class] = priority_class
        session.query(job_info_table).filter(
            job_info_table.c.spot_job_id == job_id).update(updates)
        session.commit()


def get_job_file_contents(job_id: int) -> dict[str, str | None]:
    """Return file information and stored contents for a managed job."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                job_info_table.c.dag_yaml_path,
                job_info_table.c.env_file_path,
                job_info_table.c.dag_yaml_content,
                job_info_table.c.env_file_content,
                job_info_table.c.config_file_content,
            ).where(job_info_table.c.spot_job_id == job_id)).fetchone()

    if row is None:
        return {
            'dag_yaml_path': None,
            'env_file_path': None,
            'dag_yaml_content': None,
            'env_file_content': None,
            'config_file_content': None,
        }

    return {
        'dag_yaml_path': row[0],
        'env_file_path': row[1],
        'dag_yaml_content': row[2],
        'env_file_content': row[3],
        'config_file_content': row[4],
    }


@db_retries.retry
def get_pool_from_job_id(job_id: int) -> str | None:
    """Get the pool from the job id."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        pool = session.execute(
            sqlalchemy.select(job_info_table.c.pool).where(
                job_info_table.c.spot_job_id == job_id)).fetchone()
        return pool[0] if pool else None


@db_retries.retry
def get_execution_from_job_id(job_id: int) -> str | None:
    """Get the DAG execution mode ('parallel'/'serial') from the job id.

    Returns None when the job is unknown or its row has no recorded execution
    mode (writers may store an explicit NULL, e.g. legacy code paths that
    predate the column). Callers can use this to decide JobGroup-ness without
    fetching and re-parsing the full DAG YAML.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        execution = session.execute(
            sqlalchemy.select(job_info_table.c.execution).where(
                job_info_table.c.spot_job_id == job_id)).fetchone()
        return execution[0] if execution else None


def set_current_cluster_name(job_id: int, current_cluster_name: str) -> None:
    """Set the current cluster name for a job."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(job_info_table).filter(
            job_info_table.c.spot_job_id == job_id).update(
                {job_info_table.c.current_cluster_name: current_cluster_name})
        session.commit()


@db_retries.retry
def set_job_infra(job_id: int,
                  cloud: str | None = None,
                  region: str | None = None,
                  zone: str | None = None,
                  current_node_names: list[str] | None = None) -> None:
    """Update the infrastructure info for a job.

    This is called after a job is launched to record the cloud/region/zone
    and node names for sorting, filtering, and dashboard display purposes.

    Args:
        job_id: The job ID to update.
        cloud: The cloud provider (e.g., 'GCP', 'AWS').
        region: The region (e.g., 'us-central1').
        zone: The zone (e.g., 'us-central1-a').
        current_node_names: List of current node names (head first) to merge
            into the existing lineage.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        update_values: dict[Any, Any] = {}
        if cloud is not None:
            update_values[job_info_table.c.cloud] = cloud
        if region is not None:
            update_values[job_info_table.c.region] = region
        if zone is not None:
            update_values[job_info_table.c.zone] = zone
        if current_node_names is not None:
            row = session.query(job_info_table.c.node_names).filter(
                job_info_table.c.spot_job_id ==
                job_id).with_for_update().first()
            existing_json = row.node_names if row else None
            node_names = common_utils.merge_node_names_lineage(
                existing_json, current_node_names)
            update_values[job_info_table.c.node_names] = node_names
        if update_values:
            session.query(job_info_table).filter(
                job_info_table.c.spot_job_id == job_id).update(update_values)
            session.commit()


@db_retries.retry
def save_batch_states(job_id: int, batches: list[list[int]],
                      owner_token: str) -> bool:
    """Bulk insert all batch records (atomic).

    Args:
        job_id: Managed job ID.
        batches: List of [start_idx, end_idx] pairs, indexed by batch_idx.
    """
    engine = _db_manager.get_engine()
    now = time.time()
    rows = [{
        'job_id': job_id,
        'batch_idx': idx,
        'start_idx': b[0],
        'end_idx': b[1],
        'status': 'PENDING',
        'retry_count': 0,
        'updated_at': now,
    } for idx, b in enumerate(batches)]
    with orm.Session(engine) as session:
        if not _lock_batch_coordinator_owner(session, job_id, owner_token):
            session.rollback()
            return False
        session.execute(batch_state_table.insert(), rows)
        session.commit()
        return True


def is_batch_job(job_id: int) -> bool:
    """Check if a job is a batch coordinator job."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(job_info_table.c.is_batch).where(
                job_info_table.c.spot_job_id == job_id))
        row = result.one_or_none()
        return row is not None and bool(row[0])


def _lock_batch_coordinator_row(session: orm.Session,
                                job_id: int) -> tuple[bool, str | None]:
    """Lock and return a Batch job's current coordinator token.

    PostgreSQL uses a row-level ``FOR UPDATE`` lock.  SQLite ignores that
    clause, so a no-op UPDATE first acquires its database write lock.  Every
    owner-gated mutation uses this helper, establishing one serialization
    point with coordinator takeover on both backends.
    """
    bind = session.get_bind()
    if bind.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
        session.execute(
            sqlalchemy.update(job_info_table).where(
                job_info_table.c.spot_job_id == job_id).values(
                    batch_coordinator_token=job_info_table.c.
                    batch_coordinator_token))
    row = session.execute(
        sqlalchemy.select(job_info_table.c.batch_coordinator_token).where(
            job_info_table.c.spot_job_id ==
            job_id).with_for_update()).one_or_none()
    if row is None:
        return False, None
    return True, row.batch_coordinator_token


def _lock_batch_coordinator_owner(session: orm.Session, job_id: int,
                                  owner_token: str) -> bool:
    """Lock the coordinator row and verify the expected owner token."""
    exists, current_token = _lock_batch_coordinator_row(session, job_id)
    return exists and current_token == owner_token


@db_retries.retry
def acquire_batch_coordinator(job_id: int, owner_token: str) -> str | None:
    """Atomically replace and return a Batch job's coordinator owner.

    Once this transaction commits, every attempt mutation from the previous
    owner is rejected by :func:`_batch_coordinator_owner_predicate`.
    """
    if not owner_token:
        raise ValueError('owner_token must be non-empty')

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        exists, previous_token = _lock_batch_coordinator_row(session, job_id)
        if not exists:
            session.rollback()
            raise RuntimeError(f'Managed job {job_id} does not exist')
        result = session.execute(
            sqlalchemy.update(job_info_table).where(
                job_info_table.c.spot_job_id == job_id).values(
                    batch_coordinator_token=owner_token))
        if result.rowcount != 1:
            session.rollback()
            raise RuntimeError(
                f'Failed to acquire Batch coordinator ownership for {job_id}')
        session.commit()
        return previous_token


@db_retries.retry
def is_batch_coordinator_owner(job_id: int, owner_token: str) -> bool:
    """Return whether ``owner_token`` is the current durable owner."""
    if not owner_token:
        return False
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        return bool(
            session.execute(
                sqlalchemy.select(job_info_table.c.spot_job_id).where(
                    sqlalchemy.and_(
                        job_info_table.c.spot_job_id == job_id,
                        job_info_table.c.batch_coordinator_token ==
                        owner_token))).one_or_none())


@db_retries.retry
def get_batch_states(job_id: int) -> list[dict[str, Any]]:
    """Read all batch records ordered by batch_idx.

    Returns:
        List of dicts with keys: batch_idx, start_idx, end_idx, status,
        worker_cluster, retry_count, attempt_id, attempt_owner_token,
        lease_expires_at, next_retry_at, updated_at.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(batch_state_table).where(
                batch_state_table.c.job_id == job_id).order_by(
                    batch_state_table.c.batch_idx))
        rows = result.mappings().all()
        return [dict(r) for r in rows]


@db_retries.retry
def register_batch_worker_launch(job_id: int, owner_token: str,
                                 worker_cluster: str,
                                 worker_job_name: str) -> bool:
    """Persist a worker launch intent before making the external API call.

    The insert serializes with coordinator takeover on ``job_info``.  A
    successful return therefore proves that either the launch intent committed
    before takeover or no external launch may begin for this owner.
    """
    if not owner_token or not worker_cluster or not worker_job_name:
        raise ValueError('worker launch identity fields must be non-empty')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if not _lock_batch_coordinator_owner(session, job_id, owner_token):
            session.rollback()
            return False
        session.execute(batch_worker_table.insert().values(
            job_id=job_id,
            coordinator_token=owner_token,
            worker_cluster=worker_cluster,
            worker_job_name=worker_job_name,
            updated_at=time.time()))
        session.commit()
        return True


def _get_batch_worker_row_for_update(
        session: orm.Session, job_id: int, owner_token: str,
        worker_cluster: str) -> sqlalchemy.engine.Row | None:
    return session.execute(
        sqlalchemy.select(batch_worker_table).where(
            sqlalchemy.and_(
                batch_worker_table.c.job_id == job_id,
                batch_worker_table.c.coordinator_token == owner_token,
                batch_worker_table.c.worker_cluster ==
                worker_cluster)).with_for_update()).one_or_none()


@db_retries.retry
def record_batch_worker_launch_request(job_id: int, owner_token: str,
                                       worker_cluster: str,
                                       request_id: str) -> bool:
    """Record the API request ID for an already-persisted launch intent.

    This fact may be recorded after takeover: it describes an external launch
    already initiated by ``owner_token`` and enables its exact cleanup rather
    than authorizing any new job-state mutation.
    """
    if not request_id:
        raise ValueError('request_id must be non-empty')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = _get_batch_worker_row_for_update(session, job_id, owner_token,
                                               worker_cluster)
        if row is None:
            session.rollback()
            return False
        existing = row.launch_request_id
        if existing is not None and existing != request_id:
            session.rollback()
            raise RuntimeError('Batch worker launch request ID changed for '
                               f'{job_id}/{owner_token}/{worker_cluster}')
        session.execute(
            sqlalchemy.update(batch_worker_table).where(
                sqlalchemy.and_(
                    batch_worker_table.c.job_id == job_id,
                    batch_worker_table.c.coordinator_token == owner_token,
                    batch_worker_table.c.worker_cluster ==
                    worker_cluster)).values(launch_request_id=request_id,
                                            updated_at=time.time()))
        session.commit()
        return True


@db_retries.retry
def record_batch_worker_job_id(job_id: int, owner_token: str,
                               worker_cluster: str, worker_job_id: int) -> bool:
    """Durably attach the exact external job ID to a launch intent."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = _get_batch_worker_row_for_update(session, job_id, owner_token,
                                               worker_cluster)
        if row is None:
            session.rollback()
            return False
        existing = row.worker_job_id
        if existing is not None and int(existing) != worker_job_id:
            session.rollback()
            raise RuntimeError('Batch worker job ID changed for '
                               f'{job_id}/{owner_token}/{worker_cluster}')
        session.execute(
            sqlalchemy.update(batch_worker_table).where(
                sqlalchemy.and_(
                    batch_worker_table.c.job_id == job_id,
                    batch_worker_table.c.coordinator_token == owner_token,
                    batch_worker_table.c.worker_cluster ==
                    worker_cluster)).values(worker_job_id=worker_job_id,
                                            updated_at=time.time()))
        session.commit()
        return True


@db_retries.retry
def get_batch_worker_records(job_id: int) -> list[dict[str, Any]]:
    """Return every durable worker launch generation for a Batch job."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(batch_worker_table).where(
                batch_worker_table.c.job_id == job_id).order_by(
                    batch_worker_table.c.coordinator_token,
                    batch_worker_table.c.worker_cluster)).mappings().all()
        return [dict(row) for row in rows]


@db_retries.retry
def remove_batch_worker_record(job_id: int,
                               owner_token: str,
                               worker_cluster: str,
                               worker_job_id: int | None = None) -> bool:
    """Forget a launch only after its exact external job was cleaned up."""
    predicates = [
        batch_worker_table.c.job_id == job_id,
        batch_worker_table.c.coordinator_token == owner_token,
        batch_worker_table.c.worker_cluster == worker_cluster,
    ]
    if worker_job_id is not None:
        predicates.append(batch_worker_table.c.worker_job_id == worker_job_id)
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.delete(batch_worker_table).where(
                sqlalchemy.and_(*predicates)))
        session.commit()
        return result.rowcount == 1


@db_retries.retry
def claim_batch(job_id: int,
                batch_idx: int,
                owner_token: str,
                worker_cluster: str,
                lease_duration: float,
                now: float | None = None) -> tuple[int, int] | None:
    """Atomically claim an eligible PENDING batch.

    Returns ``(attempt_id, retry_count)`` from the claimed row, or ``None`` if
    another dispatcher owns the batch or its retry backoff has not elapsed.
    Returning the durable retry count with the attempt keeps dispatchers from
    relying on a stale in-memory mirror after a controller replacement.
    """
    if not owner_token:
        raise ValueError('owner_token must be non-empty')
    if lease_duration <= 0:
        raise ValueError('lease_duration must be positive')
    if now is None:
        now = time.time()

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if not _lock_batch_coordinator_owner(session, job_id, owner_token):
            session.rollback()
            return None
        update_stmt = sqlalchemy.update(batch_state_table).where(
            sqlalchemy.and_(
                batch_state_table.c.job_id == job_id,
                batch_state_table.c.batch_idx == batch_idx,
                batch_state_table.c.status == 'PENDING',
                sqlalchemy.or_(batch_state_table.c.next_retry_at.is_(None),
                               batch_state_table.c.next_retry_at <= now),
            )).values(status='DISPATCHED',
                      worker_cluster=worker_cluster,
                      attempt_id=batch_state_table.c.attempt_id + 1,
                      attempt_owner_token=owner_token,
                      lease_expires_at=now + lease_duration,
                      next_retry_at=None,
                      updated_at=now)
        if _supports_update_returning(engine):
            claimed = session.execute(
                update_stmt.returning(
                    batch_state_table.c.attempt_id,
                    batch_state_table.c.retry_count)).one_or_none()
            session.commit()
            if claimed is None:
                return None
            return int(claimed.attempt_id), int(claimed.retry_count)

        result = session.execute(update_stmt)
        if result.rowcount != 1:
            session.commit()
            return None
        claimed = session.execute(
            sqlalchemy.select(
                batch_state_table.c.attempt_id,
                batch_state_table.c.retry_count).where(
                    sqlalchemy.and_(
                        batch_state_table.c.job_id == job_id,
                        batch_state_table.c.batch_idx == batch_idx))).one()
        session.commit()
        return int(claimed.attempt_id), int(claimed.retry_count)


@db_retries.retry
def renew_batch_lease(job_id: int,
                      batch_idx: int,
                      attempt_id: int,
                      owner_token: str,
                      lease_duration: float,
                      now: float | None = None) -> bool:
    """Extend a lease only for the current coordinator-owned attempt."""
    if not owner_token:
        raise ValueError('owner_token must be non-empty')
    if lease_duration <= 0:
        raise ValueError('lease_duration must be positive')
    if now is None:
        now = time.time()

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if not _lock_batch_coordinator_owner(session, job_id, owner_token):
            session.rollback()
            return False
        result = session.execute(
            sqlalchemy.update(batch_state_table).where(
                sqlalchemy.and_(
                    batch_state_table.c.job_id == job_id,
                    batch_state_table.c.batch_idx == batch_idx,
                    batch_state_table.c.status == 'DISPATCHED',
                    batch_state_table.c.attempt_id == attempt_id,
                    batch_state_table.c.attempt_owner_token == owner_token,
                )).values(lease_expires_at=now + lease_duration,
                          updated_at=now))
        session.commit()
        return result.rowcount == 1


@db_retries.retry
def set_batch_attempt_status(job_id: int,
                             batch_idx: int,
                             attempt_id: int,
                             owner_token: str,
                             status: str,
                             retry_count: int | None = None,
                             next_retry_at: float | None = None,
                             now: float | None = None) -> bool:
    """Transition the currently leased attempt using an attempt-token CAS.

    Stale controllers get ``False`` instead of overwriting a newer attempt.
    Only transitions out of ``DISPATCHED`` are supported here.
    """
    if not owner_token:
        raise ValueError('owner_token must be non-empty')
    if status not in ('PENDING', 'COMPLETED', 'FAILED'):
        raise ValueError(f'Unsupported batch attempt status: {status}')
    if status != 'PENDING' and next_retry_at is not None:
        raise ValueError('next_retry_at is only valid for PENDING batches')
    if now is None:
        now = time.time()

    values: dict[str, Any] = {
        'status': status,
        'worker_cluster': None,
        'lease_expires_at': None,
        'next_retry_at': next_retry_at if status == 'PENDING' else None,
        'updated_at': now,
    }
    if retry_count is not None:
        values['retry_count'] = retry_count

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if not _lock_batch_coordinator_owner(session, job_id, owner_token):
            session.rollback()
            return False
        result = session.execute(
            sqlalchemy.update(batch_state_table).where(
                sqlalchemy.and_(
                    batch_state_table.c.job_id == job_id,
                    batch_state_table.c.batch_idx == batch_idx,
                    batch_state_table.c.status == 'DISPATCHED',
                    batch_state_table.c.attempt_id == attempt_id,
                    batch_state_table.c.attempt_owner_token == owner_token,
                )).values(values))
        session.commit()
        return result.rowcount == 1


@db_retries.retry
def requeue_expired_batch_attempts(job_id: int,
                                   owner_token: str,
                                   now: float | None = None) -> list[int]:
    """Atomically return expired DISPATCHED attempts to PENDING.

    The compare-and-set includes each candidate's attempt ID, so concurrent
    coordinator incarnations cannot both reclaim the same attempt.
    """
    if not owner_token:
        raise ValueError('owner_token must be non-empty')
    if now is None:
        now = time.time()

    engine = _db_manager.get_engine()
    reclaimed: list[int] = []
    with orm.Session(engine) as session:
        if not _lock_batch_coordinator_owner(session, job_id, owner_token):
            session.rollback()
            return reclaimed
        if _supports_update_returning(engine):
            rows = session.execute(
                sqlalchemy.update(batch_state_table).where(
                    sqlalchemy.and_(
                        batch_state_table.c.job_id == job_id,
                        batch_state_table.c.status == 'DISPATCHED',
                        batch_state_table.c.attempt_owner_token.is_not(None),
                        sqlalchemy.or_(
                            batch_state_table.c.lease_expires_at.is_(None),
                            batch_state_table.c.lease_expires_at <= now,
                        ))).values(status='PENDING',
                                   worker_cluster=None,
                                   lease_expires_at=None,
                                   next_retry_at=now,
                                   updated_at=now).returning(
                                       batch_state_table.c.batch_idx)).all()
            session.commit()
            return sorted(int(row.batch_idx) for row in rows)

        candidates = session.execute(
            sqlalchemy.select(
                batch_state_table.c.batch_idx,
                batch_state_table.c.attempt_id).where(
                    sqlalchemy.and_(
                        batch_state_table.c.job_id == job_id,
                        batch_state_table.c.status == 'DISPATCHED',
                        batch_state_table.c.attempt_owner_token.is_not(None),
                        sqlalchemy.or_(
                            batch_state_table.c.lease_expires_at.is_(None),
                            batch_state_table.c.lease_expires_at <= now,
                        ))).order_by(batch_state_table.c.batch_idx)).all()
        for batch_idx, attempt_id in candidates:
            result = session.execute(
                sqlalchemy.update(batch_state_table).where(
                    sqlalchemy.and_(
                        batch_state_table.c.job_id == job_id,
                        batch_state_table.c.batch_idx == batch_idx,
                        batch_state_table.c.status == 'DISPATCHED',
                        batch_state_table.c.attempt_id == attempt_id,
                        batch_state_table.c.attempt_owner_token.is_not(None),
                        sqlalchemy.or_(
                            batch_state_table.c.lease_expires_at.is_(None),
                            batch_state_table.c.lease_expires_at <= now),
                    )).values(status='PENDING',
                              worker_cluster=None,
                              lease_expires_at=None,
                              next_retry_at=now,
                              updated_at=now))
            if result.rowcount == 1:
                reclaimed.append(int(batch_idx))
        session.commit()
    return reclaimed


def update_job_full_resources(job_id: int,
                              full_resources_json: dict[str, Any]) -> None:
    """Update the full_resources column for a job.

    This is called after scheduling to set the specific resource that was
    selected from an any_of or ordered list. The update happens within the
    filelock in get_next_cluster_name to ensure atomicity.

    Args:
        job_id: The spot_job_id to update
        full_resources_json: The resolved resource configuration (single
            resource, not any_of/ordered)
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(spot_table).where(
                spot_table.c.spot_job_id == job_id).values(
                    {spot_table.c.full_resources: full_resources_json}))
        session.commit()


async def set_job_id_on_pool_cluster_async(job_id: int,
                                           job_id_on_pool_cluster: int) -> None:
    """Set the job id on the pool cluster for a job."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        await session.execute(
            sqlalchemy.update(job_info_table).
            where(job_info_table.c.spot_job_id == job_id).values({
                job_info_table.c.job_id_on_pool_cluster: job_id_on_pool_cluster
            }))
        await session.commit()


@db_retries.retry
def get_pool_submit_info(job_id: int) -> tuple[str | None, int | None]:
    """Get the cluster name and job id on the pool from the managed job id."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        info = session.execute(
            sqlalchemy.select(
                job_info_table.c.current_cluster_name,
                job_info_table.c.job_id_on_pool_cluster).where(
                    job_info_table.c.spot_job_id == job_id)).fetchone()
        if info is None:
            return None, None
        return info[0], info[1]


@db_retries.retry_async
async def get_pool_submit_info_async(
        job_id: int) -> tuple[str | None, int | None]:
    """Get the cluster name and job id on the pool from the managed job id."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.select(job_info_table.c.current_cluster_name,
                              job_info_table.c.job_id_on_pool_cluster).where(
                                  job_info_table.c.spot_job_id == job_id))
        info = result.fetchone()
        if info is None:
            return None, None
        return info[0], info[1]


def set_api_access_token_ids(job_ids: list[int], token_id: str) -> None:
    """Store one API access token ID for a batch of managed jobs."""
    unique_job_ids = list(dict.fromkeys(job_ids))
    if not unique_job_ids:
        return

    engine = _db_manager.get_engine()
    dialect_map = {
        db_utils.SQLAlchemyDialect.SQLITE.value: sqlite.insert,
        db_utils.SQLAlchemyDialect.POSTGRESQL.value: postgresql.insert,
    }
    insert_func = dialect_map.get(engine.dialect.name)
    if insert_func is None:
        raise ValueError(f'Unsupported database dialect: {engine.dialect.name}')
    with orm.Session(engine) as session:
        for offset in range(0, len(unique_job_ids),
                            _API_ACCESS_TOKEN_UPSERT_BATCH_SIZE):
            job_id_batch = unique_job_ids[offset:offset +
                                          _API_ACCESS_TOKEN_UPSERT_BATCH_SIZE]
            insert_stmt = insert_func(api_access_token_table).values([{
                'job_id': job_id,
                'token_id': token_id,
            } for job_id in job_id_batch])
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[api_access_token_table.c.job_id],
                set_={
                    api_access_token_table.c.token_id:
                        insert_stmt.excluded.token_id
                })
            session.execute(upsert_stmt)
        session.commit()


@db_retries.retry
def get_api_access_token_id(job_id: int) -> str | None:
    """Get the API access token ID for a managed job."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(api_access_token_table.c.token_id).where(
                api_access_token_table.c.job_id == job_id)).fetchone()
        if result is None:
            return None
        return result[0]


@db_retries.retry_async
async def scheduler_set_launching_async(job_id: int):
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(job_info_table.c.spot_job_id == job_id)).values(
                    {
                        job_info_table.c.schedule_state:
                            ManagedJobScheduleState.LAUNCHING.value
                    }))
        await session.commit()


async def scheduler_set_backoff_async(job_id: int) -> None:
    """Transition a launching job to resource backoff."""

    async def _op(session: sql_async.AsyncSession) -> int:
        result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.spot_job_id == job_id,
                    job_info_table.c.schedule_state ==
                    ManagedJobScheduleState.LAUNCHING.value,
                )).values({
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.ALIVE_BACKOFF.value
                }))
        return result.rowcount

    await _retry_schedule_state_update(job_id,
                                       ManagedJobScheduleState.ALIVE_BACKOFF,
                                       _op)


async def scheduler_set_alive_async(job_id: int) -> None:
    """Do not call without holding the scheduler lock."""

    async def _op(session: sql_async.AsyncSession) -> int:
        result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.spot_job_id == job_id,
                    job_info_table.c.schedule_state ==
                    ManagedJobScheduleState.LAUNCHING.value,
                )).values({
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.ALIVE.value
                }))
        return result.rowcount

    await _retry_schedule_state_update(job_id, ManagedJobScheduleState.ALIVE,
                                       _op)


def scheduler_set_done(job_id: int, idempotent: bool = False) -> None:
    """Do not call without holding the scheduler lock."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        updated_count = session.query(job_info_table).filter(
            sqlalchemy.and_(
                job_info_table.c.spot_job_id == job_id,
                job_info_table.c.schedule_state
                != ManagedJobScheduleState.DONE.value,
            )).update({
                job_info_table.c.schedule_state:
                    ManagedJobScheduleState.DONE.value
            })
        session.commit()
        if not idempotent:
            assert updated_count == 1, (job_id, updated_count)


def get_job_schedule_state(job_id: int) -> ManagedJobScheduleState:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        state = session.execute(
            sqlalchemy.select(job_info_table.c.schedule_state).where(
                job_info_table.c.spot_job_id == job_id)).fetchone()[0]
        return ManagedJobScheduleState(state)


def get_num_launching_jobs() -> int:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        return session.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.schedule_state ==
                    ManagedJobScheduleState.LAUNCHING.value,
                    # We only count jobs that are not in the pool, because the
                    # job in the pool does not actually calling the sky.launch.
                    job_info_table.c.pool.is_(None)))).fetchone()[0]


def get_num_alive_jobs(pool: str | None = None) -> int:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        where_conditions = [
            job_info_table.c.schedule_state.in_([
                ManagedJobScheduleState.ALIVE_WAITING.value,
                ManagedJobScheduleState.LAUNCHING.value,
                ManagedJobScheduleState.ALIVE.value,
                ManagedJobScheduleState.ALIVE_BACKOFF.value,
            ])
        ]

        if pool is not None:
            where_conditions.append(job_info_table.c.pool == pool)

        return session.execute(
            sqlalchemy.select(
                sqlalchemy.func.count()  # pylint: disable=not-callable
            ).select_from(job_info_table).where(
                sqlalchemy.and_(*where_conditions))).fetchone()[0]


def get_pending_jobs_count_by_pool(pool: str) -> int:
    """Get the count of pending jobs in a pool.

    Pending jobs are distinct managed jobs that are waiting for a worker.
    A single job can contribute multiple task rows while it remains queued, so
    the queue length must count unique job IDs instead of raw task rows. Jobs
    already assigned to a replica must not keep contributing queued demand just
    because later task rows are still pending in the task table.

    Args:
        pool: The pool name

    Returns:
        The number of pending jobs in the pool
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        # pylint: disable=not-callable
        query = sqlalchemy.select(
            sqlalchemy.func.count(
                spot_table.c.spot_job_id.distinct())).select_from(
                    job_info_table.join(
                        spot_table, job_info_table.c.spot_job_id ==
                        spot_table.c.spot_job_id)).where(
                            sqlalchemy.and_(
                                spot_table.c.status ==
                                ManagedJobStatus.PENDING.value,
                                job_info_table.c.pool == pool,
                                job_info_table.c.current_cluster_name.is_(None),
                            ))
        result = session.execute(query).fetchone()
        return result[0] if result else 0


def get_nonterminal_job_ids_by_pool(pool: str,
                                    cluster_name: str | None = None
                                   ) -> list[int]:
    """Get nonterminal job ids in a pool."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            spot_table.c.spot_job_id.distinct()).select_from(
                spot_table.outerjoin(
                    job_info_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id))
        and_conditions = [
            ~spot_table.c.status.in_([
                status.value for status in ManagedJobStatus.terminal_statuses()
            ]),
            job_info_table.c.pool == pool,
        ]
        if cluster_name is not None:
            and_conditions.append(
                job_info_table.c.current_cluster_name == cluster_name)
        query = query.where(sqlalchemy.and_(*and_conditions)).order_by(
            spot_table.c.spot_job_id.asc())
        rows = session.execute(query).fetchall()
        job_ids = [row[0] for row in rows if row[0] is not None]
        return job_ids


def get_nonterminal_job_counts_by_pool(pool: str) -> dict[str, int]:
    """Get the number of nonterminal jobs per cluster in a pool.

    Returns a dict mapping cluster_name to the count of nonterminal jobs
    running on that cluster. Uses a single GROUP BY query instead of
    per-cluster queries.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            job_info_table.c.current_cluster_name,
            # pylint: disable=not-callable
            sqlalchemy.func.count(
                spot_table.c.spot_job_id.distinct()
            )).select_from(
                spot_table.outerjoin(
                    job_info_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id)
            ).where(
                sqlalchemy.and_(
                    ~spot_table.c.status.in_([
                        status.value
                        for status in ManagedJobStatus.terminal_statuses()
                    ]),
                    job_info_table.c.pool == pool,
                )).group_by(job_info_table.c.current_cluster_name)
        rows = session.execute(query).fetchall()
        return {row[0]: row[1] for row in rows if row[0] is not None}


def get_nonterminal_job_ids_by_pool_grouped(
        pool: str) -> dict[str | None, list[int]]:
    """Get nonterminal job ids in a pool, grouped by current_cluster_name.

    Equivalent to calling get_nonterminal_job_ids_by_pool once per replica
    (plus once for the pool as a whole), but executed in a single query so
    callers like pool_status avoid the N+1 round-trips that dominate
    dashboard latency when there are many finished jobs.

    Returns:
        A dict mapping current_cluster_name to the list of nonterminal
        spot_job_ids assigned to that cluster. Jobs not yet bound to a
        specific cluster (current_cluster_name IS NULL) are grouped under
        the ``None`` key. Each list is sorted by spot_job_id ascending.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            job_info_table.c.current_cluster_name,
            spot_table.c.spot_job_id,
        ).distinct().select_from(
            spot_table.outerjoin(
                job_info_table, spot_table.c.spot_job_id ==
                job_info_table.c.spot_job_id)).where(
                    sqlalchemy.and_(
                        ~spot_table.c.status.in_([
                            status.value
                            for status in ManagedJobStatus.terminal_statuses()
                        ]),
                        job_info_table.c.pool == pool,
                    )).order_by(spot_table.c.spot_job_id.asc())
        rows = session.execute(query).fetchall()
        result: dict[str | None, list[int]] = {}
        for cluster_name, job_id in rows:
            if job_id is None:
                continue
            result.setdefault(cluster_name, []).append(job_id)
        return result


def _is_any_of_or_ordered(resource_config: dict[str, Any]) -> bool:
    """Check if resource config is heterogeneous (any_of or ordered).

    Args:
        resource_config: Resource configuration dictionary

    Returns:
        True if the config contains 'any_of' or 'ordered' keys, indicating
        heterogeneous resources that haven't been resolved to a specific
        resource yet.
    """
    return 'any_of' in resource_config or 'ordered' in resource_config


def _parse_job_full_resources(
    resource_config: dict[str, Any] | None
) -> Optional['resources_lib.Resources']:
    """Parse one persisted full_resources payload."""
    if resource_config is None:
        return None
    if _is_any_of_or_ordered(resource_config):
        return None
    resources_set = resources_lib.Resources.from_yaml_config(resource_config)
    if len(resources_set) == 0:
        return None
    return next(iter(resources_set))


def get_pool_worker_used_resources(
        job_ids: set[int]) -> Optional['resources_lib.Resources']:
    """Get the total used resources by running jobs.

    Args:
        job_ids: Set of spot_job_id values to check

    Returns:
        Resources object with summed resources from all running jobs, or None
        if we couldn't parse the resources string for any job.
    """
    if not job_ids:
        return None

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        # Count only live task rows for each job. Multi-task managed jobs keep
        # terminal task history in spot_table, and those historical rows can
        # retain older full_resources values that should not contribute to the
        # worker's active resource usage.
        query = sqlalchemy.select(
            spot_table.c.spot_job_id,
            spot_table.c.full_resources,
        ).distinct().where(
            sqlalchemy.and_(
                spot_table.c.spot_job_id.in_(job_ids),
                ~spot_table.c.status.in_([
                    status.value
                    for status in ManagedJobStatus.terminal_statuses()
                ]),
            ))
        rows = session.execute(query).fetchall()

        resource_configs = []
        for row in rows:
            if row[1] is None:
                # We don't have full_resources for this job. We should return
                # none since we can't make any guarantees about what resources
                # are being used.
                return None
            resource_configs.append(row[1])

    # Parse resources dicts into Resources objects and sum them using +.
    # If any job on the worker has an empty resource request, fail closed for
    # resource-aware scheduling by treating the worker as fully occupied.
    total_resources = None
    saw_empty_request = False
    for resource_config in resource_configs:
        parsed = _parse_job_full_resources(resource_config)
        if parsed is None:
            return None
        if parsed.is_empty():
            saw_empty_request = True
            continue
        if total_resources is None:
            total_resources = parsed
        else:
            total_resources = total_resources + parsed
    if saw_empty_request:
        return resources_lib.Resources()
    return total_resources


def get_pool_worker_used_resources_by_cluster(
        pool: str) -> dict[str | None, 'resources_lib.Resources'] | None:
    """Get used resources for all nonterminal jobs in a pool in one query."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            job_info_table.c.current_cluster_name,
            spot_table.c.spot_job_id,
            spot_table.c.full_resources,
        ).distinct().select_from(
            spot_table.outerjoin(
                job_info_table, spot_table.c.spot_job_id ==
                job_info_table.c.spot_job_id)).where(
                    sqlalchemy.and_(
                        ~spot_table.c.status.in_([
                            status.value
                            for status in ManagedJobStatus.terminal_statuses()
                        ]),
                        job_info_table.c.pool == pool,
                    ))
        rows = session.execute(query).fetchall()

    totals: dict[str | None, resources_lib.Resources] = {}
    clusters_with_empty_request: set[str | None] = set()
    for cluster_name, _, resource_config in rows:
        parsed = _parse_job_full_resources(resource_config)
        if parsed is None:
            return None
        if parsed.is_empty():
            clusters_with_empty_request.add(cluster_name)
            continue
        if cluster_name in clusters_with_empty_request:
            continue
        total = totals.get(cluster_name)
        if total is None:
            totals[cluster_name] = parsed
        else:
            combined = total + parsed
            assert combined is not None
            totals[cluster_name] = combined

    for cluster_name in clusters_with_empty_request:
        totals[cluster_name] = resources_lib.Resources()
    return totals


@db_retries.retry_async
async def get_waiting_job_async(pid: int,
                                pid_started_at: float) -> dict[str, Any] | None:
    """Get the next job that should transition to LAUNCHING.

    Selects the highest-priority WAITING or ALIVE_WAITING job and atomically
    transitions it to LAUNCHING state to prevent race conditions.

    Returns the job information if a job was successfully transitioned to
    LAUNCHING, or None if no suitable job was found.

    Backwards compatibility note: jobs submitted before #4485 will have no
    schedule_state and will be ignored by this SQL query.
    """
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        # Subquery: pools that already have an active *batch* job.
        # Batch coordinator jobs (ds.map()) are serialized one-at-a-time
        # per pool, so we skip WAITING batch jobs whose pool already has
        # another active batch job.  Regular (non-batch) pool jobs are
        # unaffected — they can run concurrently on the same pool.
        active_batch_states = [
            ManagedJobScheduleState.LAUNCHING.value,
            ManagedJobScheduleState.ALIVE.value,
            ManagedJobScheduleState.ALIVE_WAITING.value,
            ManagedJobScheduleState.ALIVE_BACKOFF.value,
        ]
        busy_batch_pools_subq = sqlalchemy.select(job_info_table.c.pool,).where(
            sqlalchemy.and_(
                job_info_table.c.pool.isnot(None),
                job_info_table.c.is_batch.is_(True),
                job_info_table.c.schedule_state.in_(active_batch_states),
            )).correlate(None).scalar_subquery()

        # Select the highest priority waiting job for update (locks the row).
        # Batch jobs are skipped when their pool already has an active batch
        # job; non-batch jobs (including regular pool jobs) are always eligible.
        select_query = sqlalchemy.select(
            job_info_table.c.spot_job_id,
            job_info_table.c.schedule_state,
            job_info_table.c.pool,
        ).where(
            sqlalchemy.and_(
                job_info_table.c.schedule_state.in_([
                    ManagedJobScheduleState.WAITING.value,
                ]),
                sqlalchemy.or_(
                    # Non-batch jobs: always eligible.
                    job_info_table.c.is_batch.isnot(True),
                    # Batch jobs: only if pool has no active batch job.
                    ~job_info_table.c.pool.in_(busy_batch_pools_subq),
                ),
            )).order_by(
                job_info_table.c.priority.desc(),
                job_info_table.c.spot_job_id.asc(),
            ).limit(1).with_for_update()

        # Execute the select with row locking
        result = await session.execute(select_query)
        waiting_job_row = result.fetchone()

        if waiting_job_row is None:
            return None

        job_id = waiting_job_row[0]
        current_state = ManagedJobScheduleState(waiting_job_row[1])
        pool = waiting_job_row[2]

        # Update the job state to LAUNCHING
        update_result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.spot_job_id == job_id,
                    job_info_table.c.schedule_state == current_state.value,
                )).values({
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.LAUNCHING.value,
                    job_info_table.c.controller_pid: pid,
                    job_info_table.c.controller_pid_started_at: pid_started_at,
                }))

        if update_result.rowcount != 1:
            # Update failed, rollback and return None
            await session.rollback()
            return None

        # Commit the transaction
        await session.commit()

        return {
            'job_id': job_id,
            'pool': pool,
        }


def get_workspace(job_id: int) -> str:
    """Get the workspace of a job."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        workspace = session.execute(
            sqlalchemy.select(job_info_table.c.workspace).where(
                job_info_table.c.spot_job_id == job_id)).fetchone()
        job_workspace = workspace[0] if workspace else None
        if job_workspace is None:
            return constants.SKYPILOT_DEFAULT_WORKSPACE
        return job_workspace


@db_retries.retry_async
async def get_file_mounts_blob_id_async(job_id: int) -> str | None:
    """Return the file_mounts_blob_id persisted for a job, if any."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        row = (await session.execute(
            sqlalchemy.select(job_info_table.c.file_mounts_blob_id).where(
                job_info_table.c.spot_job_id == job_id))).fetchone()
        if row is None:
            return None
        return row[0]


async def get_latest_task_id_status_async(
        job_id: int) -> tuple[int | None, ManagedJobStatus | None]:
    """Returns the (task id, status) of the latest task of a job."""
    id_statuses = await get_all_task_ids_statuses_async(job_id)
    return get_latest_task_id_from_statuses(id_statuses)


@db_retries.retry_async
async def get_all_task_ids_statuses_async(
        job_id: int) -> list[tuple[int, ManagedJobStatus]]:
    """Returns all (task_id, status) pairs for a job (async version)."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.select(
                spot_table.c.task_id,
                spot_table.c.status,
            ).where(spot_table.c.spot_job_id == job_id).order_by(
                spot_table.c.task_id.asc()))
        return [(row[0], ManagedJobStatus(row[1])) for row in result.fetchall()]


async def set_starting_async(job_id: int,
                             task_id: int,
                             run_timestamp: str,
                             submit_time: float,
                             resources_str: str,
                             specs: dict[str, Any],
                             callback_func: AsyncCallbackType,
                             full_resources_json: dict[str, Any] | None = None):
    """Set the task to starting state."""
    await add_job_event_async(job_id, task_id, ManagedJobStatus.STARTING,
                              'Job is starting')
    logger.info('Launching the spot cluster...')

    async def _op(session: sql_async.AsyncSession) -> int:
        values = {
            spot_table.c.resources: resources_str,
            spot_table.c.submitted_at: submit_time,
            spot_table.c.status: ManagedJobStatus.STARTING.value,
            spot_table.c.run_timestamp: run_timestamp,
            spot_table.c.specs: json.dumps(specs),
        }
        if full_resources_json is not None:
            values[spot_table.c.full_resources] = full_resources_json
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.status == ManagedJobStatus.PENDING.value,
                    spot_table.c.end_at.is_(None),
                )).values(values))
        return result.rowcount

    await _retry_task_status_update(job_id, task_id, ManagedJobStatus.STARTING,
                                    _op, 'Failed to set the task to starting.')
    await callback_func('SUBMITTED')
    await callback_func('STARTING')


async def set_started_async(job_id: int, task_id: int, start_time: float,
                            callback_func: AsyncCallbackType):
    """Set the task to started state."""
    await add_job_event_async(job_id, task_id, ManagedJobStatus.RUNNING,
                              'Job has started')
    logger.info('Job started.')

    async def _op(session: sql_async.AsyncSession) -> int:
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.status.in_([
                        ManagedJobStatus.STARTING.value,
                        ManagedJobStatus.PENDING.value
                    ]),
                    spot_table.c.end_at.is_(None),
                )).values({
                    spot_table.c.status: ManagedJobStatus.RUNNING.value,
                    spot_table.c.start_at: start_time,
                    spot_table.c.last_recovered_at: start_time,
                }))
        return result.rowcount

    await _retry_task_status_update(job_id, task_id, ManagedJobStatus.RUNNING,
                                    _op, 'Failed to set the task to started.')
    await callback_func('STARTED')


def get_job_status_with_task_id(job_id: int,
                                task_id: int) -> ManagedJobStatus | None:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(spot_table.c.status).where(
                sqlalchemy.and_(spot_table.c.spot_job_id == job_id,
                                spot_table.c.task_id == task_id)))
        status = result.fetchone()
        return ManagedJobStatus(status[0]) if status else None


@db_retries.retry_async
async def get_job_status_with_task_id_async(
        job_id: int, task_id: int) -> ManagedJobStatus | None:
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.select(spot_table.c.status).where(
                sqlalchemy.and_(spot_table.c.spot_job_id == job_id,
                                spot_table.c.task_id == task_id)))
        status = result.fetchone()
        return ManagedJobStatus(status[0]) if status else None


async def set_recovering_async(
    job_id: int,
    task_id: int,
    force_transit_to_recovering: bool,
    callback_func: AsyncCallbackType,
    external_failures: list[ExternalClusterFailure] | None = None,
    cluster_event_reason: str | None = None,
):
    """Set the task to recovering state, and update the job duration."""
    # Build code and reason from external failures for the event log.
    # Prefer external_failures over cluster_event_reason to avoid
    # duplicating the same message when a plugin writes the same reason
    # to both cluster events and cluster failures.
    code = None
    if external_failures:
        code = '; '.join(f.code for f in external_failures)
        reason = '; '.join(f.reason for f in external_failures)
    elif cluster_event_reason:
        reason = cluster_event_reason
    else:
        assert code is None, 'Code should be None if there are no reasons.'
        reason = 'Cluster preempted or failed, recovering'

    await add_job_event_async(job_id, task_id, ManagedJobStatus.RECOVERING,
                              reason, code)
    logger.info('=== Recovering... ===')
    current_time = time.time()

    async def _op(session: sql_async.AsyncSession) -> int:
        if force_transit_to_recovering:
            status_condition = spot_table.c.status.in_(
                [s.value for s in ManagedJobStatus.processing_statuses()])
        else:
            status_condition = (
                spot_table.c.status == ManagedJobStatus.RUNNING.value)

        # RUNNING and WINDING_DOWN are the "still doing job work"
        # states (set_succeeded_async treats them equivalently, and
        # `set_winding_down` itself doesn't accumulate). Forced
        # recovery may revisit PENDING/STARTING/RECOVERING rows on
        # resume or commit-lost retry; do not re-accumulate there.
        should_accumulate_duration = sqlalchemy.and_(
            spot_table.c.status.in_([
                ManagedJobStatus.RUNNING.value,
                ManagedJobStatus.WINDING_DOWN.value,
            ]),
            spot_table.c.last_recovered_at >= 0,
        )
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    status_condition,
                    spot_table.c.end_at.is_(None),
                )).values({
                    spot_table.c.status: ManagedJobStatus.RECOVERING.value,
                    spot_table.c.job_duration: sqlalchemy.case(
                        (should_accumulate_duration, spot_table.c.job_duration +
                         current_time - spot_table.c.last_recovered_at),
                        else_=spot_table.c.job_duration),
                    spot_table.c.last_recovered_at: sqlalchemy.case(
                        (spot_table.c.last_recovered_at < 0, current_time),
                        else_=spot_table.c.last_recovered_at),
                }))
        return result.rowcount

    await _retry_task_status_update(
        job_id, task_id, ManagedJobStatus.RECOVERING, _op,
        ('Failed to set the task to recovering with '
         f'force_transit_to_recovering={force_transit_to_recovering}.'))
    await callback_func('RECOVERING')


async def set_recovered_async(job_id: int, task_id: int, recovered_time: float,
                              callback_func: AsyncCallbackType):
    """Set the task to recovered."""
    await add_job_event_async(job_id, task_id, ManagedJobStatus.RUNNING,
                              'Job has recovered')

    async def _op(session: sql_async.AsyncSession) -> int:
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.status == ManagedJobStatus.RECOVERING.value,
                    spot_table.c.end_at.is_(None),
                )).values({
                    spot_table.c.status: ManagedJobStatus.RUNNING.value,
                    spot_table.c.last_recovered_at: recovered_time,
                    spot_table.c.recovery_count: spot_table.c.recovery_count +
                                                 1,
                }))
        return result.rowcount

    await _retry_task_status_update(job_id, task_id, ManagedJobStatus.RUNNING,
                                    _op, 'Failed to set the task to recovered.')
    logger.info('==== Recovered. ====')
    await callback_func('RECOVERED')


def set_winding_down(job_id: int, task_id: int) -> None:
    """Transition task from RUNNING to WINDING_DOWN (sync).

    Called by the batch coordinator (which runs in a thread) before
    merging per-batch output files.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.status == ManagedJobStatus.RUNNING.value,
                    spot_table.c.end_at.is_(None),
                )).values(
                    {spot_table.c.status: ManagedJobStatus.WINDING_DOWN.value}))
        session.commit()
        if result.rowcount != 1:
            logger.warning(f'set_winding_down: expected 1 row updated, '
                           f'got {result.rowcount} for job_id={job_id}, '
                           f'task_id={task_id}')


@db_retries.retry
def set_batch_winding_down(job_id: int, task_id: int,
                           owner_token: str) -> BatchLifecycleTransition:
    """Owner-fenced Batch transition to WINDING_DOWN."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if not _lock_batch_coordinator_owner(session, job_id, owner_token):
            session.rollback()
            return BatchLifecycleTransition.OWNER_LOST
        result = session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.status == ManagedJobStatus.RUNNING.value,
                    spot_table.c.end_at.is_(None),
                )).values(
                    {spot_table.c.status: ManagedJobStatus.WINDING_DOWN.value}))
        if result.rowcount == 1:
            session.commit()
            return BatchLifecycleTransition.APPLIED
        status = session.execute(
            sqlalchemy.select(spot_table.c.status).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id))).scalar_one_or_none()
        session.commit()
        if status == ManagedJobStatus.WINDING_DOWN.value:
            return BatchLifecycleTransition.ALREADY_TARGET
        return BatchLifecycleTransition.INVALID_STATE


@db_retries.retry
def set_batch_succeeded(job_id: int, task_id: int, owner_token: str,
                        end_time: float) -> BatchLifecycleTransition:
    """Owner-fenced Batch transition to SUCCEEDED."""
    engine = _db_manager.get_engine()
    changed = False
    with orm.Session(engine) as session:
        if not _lock_batch_coordinator_owner(session, job_id, owner_token):
            session.rollback()
            return BatchLifecycleTransition.OWNER_LOST
        result = session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.status.in_([
                        ManagedJobStatus.RUNNING.value,
                        ManagedJobStatus.WINDING_DOWN.value,
                    ]),
                    spot_table.c.end_at.is_(None),
                )).values({
                    spot_table.c.status: ManagedJobStatus.SUCCEEDED.value,
                    spot_table.c.end_at: end_time,
                }))
        changed = result.rowcount == 1
        if changed:
            outcome = BatchLifecycleTransition.APPLIED
            session.execute(job_events_table.insert().values(
                spot_job_id=job_id,
                task_id=task_id,
                new_status=ManagedJobStatus.SUCCEEDED.value,
                reason='Job has succeeded',
                timestamp=datetime.datetime.now()))
        else:
            status = session.execute(
                sqlalchemy.select(spot_table.c.status).where(
                    sqlalchemy.and_(
                        spot_table.c.spot_job_id == job_id,
                        spot_table.c.task_id == task_id))).scalar_one_or_none()
            if status == ManagedJobStatus.SUCCEEDED.value:
                outcome = BatchLifecycleTransition.ALREADY_TARGET
            else:
                outcome = BatchLifecycleTransition.INVALID_STATE
        session.commit()
    if changed:
        logger.info('Job succeeded.')
    return outcome


@db_retries.retry
def set_batch_failed(job_id: int, task_id: int, owner_token: str,
                     failure_reason: str) -> BatchLifecycleTransition:
    """Owner-fenced Batch transition to FAILED."""
    engine = _db_manager.get_engine()
    end_time = time.time()
    with orm.Session(engine) as session:
        if not _lock_batch_coordinator_owner(session, job_id, owner_token):
            session.rollback()
            return BatchLifecycleTransition.OWNER_LOST
        nonterminal_statuses = [
            status.value
            for status in ManagedJobStatus
            if not status.is_terminal()
        ]
        result = session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.status.in_(nonterminal_statuses),
                    spot_table.c.end_at.is_(None),
                )).values({
                    spot_table.c.status: ManagedJobStatus.FAILED.value,
                    spot_table.c.failure_reason: failure_reason,
                    spot_table.c.end_at: end_time,
                }))
        changed = result.rowcount == 1
        if changed:
            session.execute(job_events_table.insert().values(
                spot_job_id=job_id,
                task_id=task_id,
                new_status=ManagedJobStatus.FAILED.value,
                reason=f'Job failed: {failure_reason}',
                timestamp=datetime.datetime.now()))
            outcome = BatchLifecycleTransition.APPLIED
        else:
            status = session.execute(
                sqlalchemy.select(spot_table.c.status).where(
                    sqlalchemy.and_(
                        spot_table.c.spot_job_id == job_id,
                        spot_table.c.task_id == task_id))).scalar_one_or_none()
            if status == ManagedJobStatus.FAILED.value:
                outcome = BatchLifecycleTransition.ALREADY_TARGET
            else:
                outcome = BatchLifecycleTransition.INVALID_STATE
        session.commit()
    return outcome


async def set_succeeded_async(job_id: int, task_id: int, end_time: float,
                              callback_func: AsyncCallbackType):
    """Set the task to succeeded, if it is in a non-terminal state."""
    await add_job_event_async(job_id, task_id, ManagedJobStatus.SUCCEEDED,
                              'Job has succeeded')

    async def _op(session: sql_async.AsyncSession) -> int:
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.status.in_([
                        ManagedJobStatus.RUNNING.value,
                        ManagedJobStatus.WINDING_DOWN.value,
                    ]),
                    spot_table.c.end_at.is_(None),
                )).values({
                    spot_table.c.status: ManagedJobStatus.SUCCEEDED.value,
                    spot_table.c.end_at: end_time,
                }))
        return result.rowcount

    await _retry_task_status_update(job_id, task_id, ManagedJobStatus.SUCCEEDED,
                                    _op, 'Failed to set the task to succeeded.')
    await callback_func('SUCCEEDED')
    logger.info('Job succeeded.')


async def set_failed_async(
    job_id: int,
    task_id: int | None,
    failure_type: ManagedJobStatus,
    failure_reason: str,
    callback_func: AsyncCallbackType | None = None,
    end_time: float | None = None,
    override_terminal: bool = False,
):
    """Set an entire job or task to failed."""
    await add_job_event_async(job_id, task_id, failure_type,
                              f'Job failed: {failure_reason}')
    assert failure_type.is_failed(), failure_type
    end_time = time.time() if end_time is None else end_time

    async def _op(session):
        fields_to_set: dict[str, Any] = {
            spot_table.c.status: failure_type.value,
            spot_table.c.failure_reason: failure_reason,
        }
        # Get previous status
        result = await session.execute(
            sqlalchemy.select(
                spot_table.c.status).where(spot_table.c.spot_job_id == job_id))
        previous_status_row = result.fetchone()
        previous_status = ManagedJobStatus(previous_status_row[0])
        if previous_status == ManagedJobStatus.RECOVERING:
            fields_to_set[spot_table.c.last_recovered_at] = end_time
        where_conditions = [spot_table.c.spot_job_id == job_id]
        if task_id is not None:
            where_conditions.append(spot_table.c.task_id == task_id)

        # Handle failure_reason prepending when override_terminal is True
        if override_terminal:
            # Get existing failure_reason with row lock to prevent race
            # conditions
            existing_reason_result = await session.execute(
                sqlalchemy.select(spot_table.c.failure_reason).where(
                    sqlalchemy.and_(*where_conditions)).with_for_update())
            existing_reason_row = existing_reason_result.fetchone()
            if existing_reason_row and existing_reason_row[0]:
                # Prepend new failure reason to existing one
                fields_to_set[spot_table.c.failure_reason] = (
                    failure_reason + '. Previously: ' + existing_reason_row[0])
            fields_to_set[spot_table.c.end_at] = sqlalchemy.func.coalesce(
                spot_table.c.end_at, end_time)
        else:
            fields_to_set[spot_table.c.end_at] = end_time
            where_conditions.append(spot_table.c.end_at.is_(None))
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(*where_conditions)).values(fields_to_set))
        count = result.rowcount
        await session.commit()
        return count > 0

    updated = await _retry_session(_op)
    if callback_func and updated:
        await callback_func('FAILED')
    logger.info(failure_reason)


async def update_links_async(job_id: int, task_id: int,
                             links: dict[str, str]) -> None:
    """Update the links for a managed job task.

    Links are stored as JSON in the database. SQLAlchemy handles
    serialization/deserialization automatically.

    Uses a transaction to ensure atomicity. For PostgreSQL, we use row-level
    locking (SELECT FOR UPDATE). For SQLite, row-level locking is not
    supported, so we rely on SQLite's database-level write locking which
    provides serializable isolation for write transactions.
    """
    engine = await _db_manager.get_async_engine()
    logger.info(f'Updating external links with: {links}')
    async with sql_async.AsyncSession(engine) as session:
        async with session.begin():
            # Build the select query
            select_query = sqlalchemy.select(spot_table.c.links).where(
                sqlalchemy.and_(spot_table.c.spot_job_id == job_id,
                                spot_table.c.task_id == task_id))

            # Use row-level locking for PostgreSQL; SQLite doesn't support
            # SELECT FOR UPDATE but provides database-level write locking
            if (engine.dialect.name ==
                    db_utils.SQLAlchemyDialect.POSTGRESQL.value):
                select_query = select_query.with_for_update()

            result = await session.execute(select_query)
            existing_links_row = result.fetchone()
            existing_links = {}
            if existing_links_row and existing_links_row[0]:
                existing_links = existing_links_row[0]

            # Merge new links into existing
            existing_links.update(links)

            # Update the database (SQLAlchemy JSON type handles serialization)
            await session.execute(
                sqlalchemy.update(spot_table).where(
                    sqlalchemy.and_(spot_table.c.spot_job_id == job_id,
                                    spot_table.c.task_id == task_id)).values({
                                        spot_table.c.links: existing_links,
                                    }))
            # Transaction commits automatically when exiting the context


async def set_cancelling_async(job_id: int, callback_func: AsyncCallbackType):
    """Set tasks in the job as cancelling, if they are in non-terminal
    states."""
    await add_job_event_async(job_id, None, ManagedJobStatus.CANCELLING,
                              'Job is cancelling')

    async def _op(session):
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.end_at.is_(None),
                )).values(
                    {spot_table.c.status: ManagedJobStatus.CANCELLING.value}))
        count = result.rowcount
        await session.commit()
        return count > 0

    updated = await _retry_session(_op)
    if updated:
        logger.info('Cancelling the job...')
        await callback_func('CANCELLING')
    else:
        logger.info('Cancellation skipped, job is already terminal')


async def set_cancelled_async(job_id: int, callback_func: AsyncCallbackType):
    """Set tasks in the job as cancelled, if they are in CANCELLING state."""
    await add_job_event_async(job_id, None, ManagedJobStatus.CANCELLED,
                              'Job has been cancelled')

    async def _op(session):
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.status == ManagedJobStatus.CANCELLING.value,
                )).values({
                    spot_table.c.status: ManagedJobStatus.CANCELLED.value,
                    spot_table.c.end_at: time.time(),
                }))
        count = result.rowcount
        await session.commit()
        return count > 0

    updated = await _retry_session(_op)
    if updated:
        logger.info('Job cancelled.')
        await callback_func('CANCELLED')
    else:
        logger.info('Cancellation skipped, job is not CANCELLING')


@db_retries.retry_async
async def remove_ha_recovery_script_async(job_id: int) -> None:
    """Remove the HA recovery script for a job."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        await session.execute(
            sqlalchemy.delete(ha_recovery_script_table).where(
                ha_recovery_script_table.c.job_id == job_id))
        await session.commit()


async def get_status_async(job_id: int) -> ManagedJobStatus | None:
    _, status = await get_latest_task_id_status_async(job_id)
    return status


async def get_job_schedule_state_async(job_id: int) -> ManagedJobScheduleState:
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.select(job_info_table.c.schedule_state).where(
                job_info_table.c.spot_job_id == job_id))
        state = result.fetchone()[0]
        return ManagedJobScheduleState(state)


async def scheduler_set_done_async(job_id: int,
                                   idempotent: bool = False) -> None:
    """Do not call without holding the scheduler lock."""

    async def _op(session: sql_async.AsyncSession) -> int:
        result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.spot_job_id == job_id,
                    job_info_table.c.schedule_state
                    != ManagedJobScheduleState.DONE.value,
                )).values({
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.DONE.value
                }))
        return result.rowcount

    await _retry_schedule_state_update(job_id, ManagedJobScheduleState.DONE,
                                       _op, idempotent)


# ==== needed for codegen ====
# functions have no use outside of codegen, remove at your own peril


def set_job_info(job_id: int,
                 name: str,
                 workspace: str,
                 entrypoint: str,
                 pool: str | None,
                 pool_hash: str | None,
                 user_hash: str | None = None,
                 execution: str | None = None,
                 is_batch: bool = False):
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if engine.dialect.name == db_utils.SQLAlchemyDialect.SQLITE.value:
            insert_func = sqlite.insert
        elif (engine.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value
             ):
            insert_func = postgresql.insert
        else:
            raise ValueError('Unsupported database dialect')
        insert_stmt = insert_func(job_info_table).values(
            spot_job_id=job_id,
            name=name,
            schedule_state=ManagedJobScheduleState.INACTIVE.value,
            workspace=workspace,
            entrypoint=entrypoint,
            pool=pool,
            pool_hash=pool_hash,
            user_hash=user_hash,
            execution=execution,
            is_batch=is_batch,
        )
        session.execute(insert_stmt)
        session.commit()


def reset_jobs_for_recovery() -> None:
    """Remove controller PIDs for live jobs, allowing them to be recovered."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(job_info_table).filter(
            # PID should be set.
            job_info_table.c.controller_pid.isnot(None),
            # Schedule state should be alive.
            job_info_table.c.schedule_state.isnot(None),
            (job_info_table.c.schedule_state
             != ManagedJobScheduleState.WAITING.value),
            (job_info_table.c.schedule_state
             != ManagedJobScheduleState.DONE.value),
        ).update({
            job_info_table.c.controller_pid: None,
            job_info_table.c.controller_pid_started_at: None,
            job_info_table.c.schedule_state:
                (ManagedJobScheduleState.WAITING.value)
        })
        session.commit()


def reset_job_for_recovery(job_id: int) -> None:
    """Set a job to WAITING and remove PID, allowing it to be recovered."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(job_info_table).filter(
            job_info_table.c.spot_job_id == job_id).update({
                job_info_table.c.controller_pid: None,
                job_info_table.c.controller_pid_started_at: None,
                job_info_table.c.schedule_state:
                    ManagedJobScheduleState.WAITING.value,
            })
        session.commit()


def get_all_job_ids_by_name(name: str | None) -> list[int]:
    """Get all job ids by name."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            spot_table.c.spot_job_id.distinct()).select_from(
                spot_table.outerjoin(
                    job_info_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id))
        if name is not None:
            # We match the job name from `job_info` for the jobs submitted after
            # #1982, and from `spot` for the jobs submitted before #1982, whose
            # job_info is not available.
            name_condition = sqlalchemy.or_(
                job_info_table.c.name == name,
                sqlalchemy.and_(job_info_table.c.name.is_(None),
                                spot_table.c.task_name == name))
            query = query.where(name_condition)
        query = query.order_by(spot_table.c.spot_job_id.desc())
        rows = session.execute(query).fetchall()
        job_ids = [row[0] for row in rows if row[0] is not None]
        return job_ids


def get_task_logs_to_clean(retention_seconds: int,
                           batch_size: int) -> list[dict[str, Any]]:
    """Get the logs of job tasks to clean.

    The logs of a task will only cleaned when:
    - the job schedule state is DONE
    - AND the end time of the task is older than the retention period
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        now = time.time()
        result = session.execute(
            sqlalchemy.select(
                spot_table.c.spot_job_id,
                spot_table.c.task_id,
                spot_table.c.local_log_file,
            ).select_from(
                spot_table.join(
                    job_info_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
                )).
            where(
                sqlalchemy.and_(
                    job_info_table.c.schedule_state.is_(
                        ManagedJobScheduleState.DONE.value),
                    spot_table.c.end_at.isnot(None),
                    spot_table.c.end_at < (now - retention_seconds),
                    spot_table.c.logs_cleaned_at.is_(None),
                    # The local log file is set AFTER the task is finished,
                    # add this condition to ensure the entire log file has
                    # been written.
                    spot_table.c.local_log_file.isnot(None),
                )).limit(batch_size))
        rows = result.fetchall()
        return [{
            'job_id': row[0],
            'task_id': row[1],
            'local_log_file': row[2]
        } for row in rows]


def get_controller_logs_to_clean(retention_seconds: int,
                                 batch_size: int) -> list[dict[str, Any]]:
    """Get the controller logs to clean.

    The controller logs will only cleaned when:
    - the job schedule state is DONE
    - AND the end time of the latest task is older than the retention period
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        now = time.time()
        result = session.execute(
            sqlalchemy.select(job_info_table.c.spot_job_id,).select_from(
                job_info_table.join(
                    spot_table,
                    job_info_table.c.spot_job_id == spot_table.c.spot_job_id,
                )).where(
                    sqlalchemy.and_(
                        job_info_table.c.schedule_state.is_(
                            ManagedJobScheduleState.DONE.value),
                        spot_table.c.local_log_file.isnot(None),
                        job_info_table.c.controller_logs_cleaned_at.is_(None),
                    )).group_by(
                        job_info_table.c.spot_job_id,
                        job_info_table.c.current_cluster_name,
                    ).having(
                        sqlalchemy.func.max(
                            spot_table.c.end_at).isnot(None),).having(
                                sqlalchemy.func.max(spot_table.c.end_at) < (
                                    now - retention_seconds)).limit(batch_size))
        rows = result.fetchall()
        return [{'job_id': row[0]} for row in rows]


def set_task_logs_cleaned(tasks: list[tuple[int, int]], logs_cleaned_at: float):
    """Set the task logs cleaned at."""
    if not tasks:
        return
    task_keys = list(dict.fromkeys(tasks))
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.tuple_(spot_table.c.spot_job_id,
                                  spot_table.c.task_id).in_(task_keys)).values(
                                      logs_cleaned_at=logs_cleaned_at))
        session.commit()


def set_controller_logs_cleaned(job_ids: list[int], logs_cleaned_at: float):
    """Set the controller logs cleaned at."""
    if not job_ids:
        return
    job_ids = list(dict.fromkeys(job_ids))
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(
            sqlalchemy.update(job_info_table).where(
                job_info_table.c.spot_job_id.in_(job_ids)).values(
                    controller_logs_cleaned_at=logs_cleaned_at))
        session.commit()


def add_job_event(job_id: int,
                  task_id: int | None,
                  new_status: ManagedJobStatus,
                  reason: str,
                  timestamp: datetime.datetime | None = None) -> None:
    """Add a job event record to the audit log.

    Args:
        job_id: The spot_job_id of the managed job.
        task_id: The task_id within the managed job. If None, adds a
            job-level event that applies to all tasks.
        new_status: The new status being transitioned to. Can be a
            ManagedJobStatus enum.
        reason: A description of why the event occurred.
        timestamp: The timestamp of the event. If None, uses current time.
    """
    if timestamp is None:
        timestamp = datetime.datetime.now()

    status_value = new_status.value

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.execute(job_events_table.insert().values(
            spot_job_id=job_id,
            task_id=task_id,  # Can be None for job-level events
            new_status=status_value,
            reason=reason,
            timestamp=timestamp,
        ))
        session.commit()


async def _get_all_task_ids_async(job_id: int) -> list[int]:
    """Get all task IDs for a job (async version)."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.select(spot_table.c.task_id).where(
                spot_table.c.spot_job_id == job_id).order_by(
                    spot_table.c.task_id.asc()))
        return [row[0] for row in result.fetchall()]


@db_retries.retry_async
async def add_job_event_async(
        job_id: int,
        task_id: int | None,
        new_status: ManagedJobStatus,
        reason: str,
        code: str | None = None,
        timestamp: datetime.datetime | None = None) -> None:
    """Add a job event record to the audit log (async version).

    Args:
        job_id: The spot_job_id of the managed job.
        task_id: The task_id within the managed job. If None, adds a
            job-level event that applies to all tasks.
        new_status: The new status being transitioned to. Can be a
            ManagedJobStatus enum.
        reason: A description of why the event occurred.
        code: Optional error category code for failures.
        timestamp: The timestamp of the event. If None, uses current time.
    """
    if timestamp is None:
        timestamp = datetime.datetime.now()

    status_value = new_status.value

    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        await session.execute(job_events_table.insert().values(
            spot_job_id=job_id,
            task_id=task_id,  # Can be None for job-level events
            new_status=status_value,
            code=code,
            reason=reason,
            timestamp=timestamp,
        ))
        await session.commit()


def get_job_events(job_id: int,
                   task_id: int | None = None,
                   limit: int | None = None) -> list[dict[str, Any]]:
    """Get task events for a managed job.

    Args:
        job_id: The spot_job_id of the managed job.
        task_id: Optional task_id to filter by. If None, returns events
            for all tasks. If specified, returns events for that task plus
            job-level events (where task_id is None).
        limit: Optional limit on number of events to return. If specified,
            returns the most recent N events.

    Returns:
        List of event records, ordered by timestamp descending
        (most recent first) if limit is specified, otherwise ascending.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            job_events_table.c.spot_job_id,
            job_events_table.c.task_id,
            job_events_table.c.new_status,
            job_events_table.c.code,
            job_events_table.c.reason,
            job_events_table.c.timestamp,
        ).where(job_events_table.c.spot_job_id == job_id)

        if task_id is not None:
            # Include events for the specific task AND job-level events
            # (task_id is None)
            query = query.where(
                sqlalchemy.or_(job_events_table.c.task_id == task_id,
                               job_events_table.c.task_id.is_(None)))

        # Order by timestamp descending to get most recent first
        query = query.order_by(job_events_table.c.timestamp.desc())

        if limit is not None:
            query = query.limit(limit)

        rows = session.execute(query).fetchall()
    return [{
        'spot_job_id': row[0],
        'task_id': row[1],
        'new_status': ManagedJobStatus(row[2]),
        'code': row[3],
        'reason': row[4],
        'timestamp': row[5],
    } for row in rows]


def _get_latest_event_reasons(
    job_ids_by_status: dict[ManagedJobStatus, list[int]]
) -> dict[ManagedJobStatus, dict[int, str]]:
    """Return the latest event reason for each requested status and job."""
    reasons: dict[ManagedJobStatus, dict[int, str]] = {
        status: {} for status in job_ids_by_status
    }
    conditions = [
        sqlalchemy.and_(
            job_events_table.c.new_status == status.value,
            job_events_table.c.spot_job_id.in_(job_ids),
        ) for status, job_ids in job_ids_by_status.items() if job_ids
    ]
    if not conditions:
        return reasons

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        ranked_events = sqlalchemy.select(
            job_events_table.c.spot_job_id.label('spot_job_id'),
            job_events_table.c.new_status.label('new_status'),
            job_events_table.c.reason.label('reason'),
            sqlalchemy.func.row_number().over(
                partition_by=(job_events_table.c.spot_job_id,
                              job_events_table.c.new_status),
                order_by=(
                    job_events_table.c.timestamp.desc(),
                    job_events_table.c.id.desc(),
                ),
            ).label('rank'),
        ).where(sqlalchemy.or_(*conditions)).subquery('ranked_job_events')
        rows = session.execute(
            sqlalchemy.select(
                ranked_events.c.spot_job_id,
                ranked_events.c.new_status,
                ranked_events.c.reason,
            ).where(
                sqlalchemy.and_(
                    ranked_events.c.rank == 1,
                    ranked_events.c.reason.is_not(None),
                    ranked_events.c.reason != '',
                ))).fetchall()
    for spot_job_id, new_status, reason in rows:
        reasons[ManagedJobStatus(new_status)][spot_job_id] = reason
    return reasons


def get_latest_recovery_and_pending_reasons(
        recovering_job_ids: list[int],
        pending_job_ids: list[int]) -> tuple[dict[int, str], dict[int, str]]:
    """Return latest recovery and pending reasons in one database query."""
    reasons = _get_latest_event_reasons({
        ManagedJobStatus.RECOVERING: recovering_job_ids,
        ManagedJobStatus.PENDING: pending_job_ids,
    })
    return (reasons[ManagedJobStatus.RECOVERING],
            reasons[ManagedJobStatus.PENDING])


def get_latest_recovery_reasons(job_ids: list[int]) -> dict[int, str]:
    """Return {job_id: reason} for the latest RECOVERING event per job."""
    recovery_reasons, _ = get_latest_recovery_and_pending_reasons(job_ids, [])
    return recovery_reasons


async def cleanup_job_events_with_retention_async(
        retention_hours: float) -> None:
    """Delete job events older than the retention period.

    Args:
        retention_hours: Number of hours to retain job events.
    """
    engine = await _db_manager.get_async_engine()
    cutoff_time = datetime.datetime.now() - datetime.timedelta(
        hours=retention_hours)

    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.delete(job_events_table).where(
                job_events_table.c.timestamp < cutoff_time))
        count = result.rowcount
        if count > 0:
            logger.debug(f'Deleted {count} job events older than '
                         f'{retention_hours} hours.')
        await session.commit()


async def job_event_retention_daemon():
    """Garbage collect job events periodically."""
    while True:
        logger.info('Running job event retention daemon...')
        try:
            await cleanup_job_events_with_retention_async(
                DEFAULT_JOB_EVENT_RETENTION_HOURS)
        except asyncio.CancelledError:
            logger.info('Job event retention daemon cancelled')
            break
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f'Error running job event retention daemon: {e}')

        await asyncio.sleep(JOB_EVENT_DAEMON_INTERVAL_SECONDS)
