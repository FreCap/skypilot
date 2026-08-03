"""The database for managed jobs status."""
# TODO(zhwu): maybe use file based status instead of database, so
# that we can easily switch to a s3-based storage.
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Collection
import json
import time
from typing import Any, Optional

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext import asyncio as sql_async

from sky import exceptions
from sky import resources as resources_lib
from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.jobs import batch_state
from sky.jobs import state_events
from sky.jobs import state_queries
from sky.jobs import state_schema
from sky.jobs import state_storage
from sky.jobs.state_schema import api_access_token_table
from sky.jobs.state_schema import ha_recovery_script_table
from sky.jobs.state_schema import job_info_table
from sky.jobs.state_schema import spot_table
from sky.jobs.status_types import ControllerLogFollowState
from sky.jobs.status_types import ControllerPidRecord
from sky.jobs.status_types import JobCancellationState
from sky.jobs.status_types import JobLogStreamSnapshot
from sky.jobs.status_types import ManagedJobScheduleState
from sky.jobs.status_types import ManagedJobStatus
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils.db import db_utils
from sky.utils.db import retries as db_retries
from sky.utils.plugin_extensions import ExternalClusterFailure

# Separate callback types for sync and async contexts
SyncCallbackType = Callable[[str], None]
AsyncCallbackType = Callable[[str], Awaitable[Any]]
CallbackType = SyncCallbackType | AsyncCallbackType

logger = sky_logging.init_logger(__name__)

# Importing the PostgreSQL request backend eagerly re-enters SkyServe payload
# registration while ``sky.jobs.state`` is still initializing. Defer it until
# a split-role controller actually evaluates an ownership fence.
request_postgres = adaptors_common.LazyImport('sky.server.requests.postgres')

_DB_RETRY_TIMES = 30

# Bound parameters per token upsert while keeping all chunks in one transaction.
_API_ACCESS_TOKEN_UPSERT_BATCH_SIZE = 1000
_TERMINAL_IDENTITY_QUERY_BATCH_SIZE = 250


class ControllerLeadershipLostError(RuntimeError):
    """Raised when a split-role managed-job write has lost its outer fence."""


def get_current_controller_owner() -> tuple[str, int] | None:
    """Return the active outer controller instance and generation, if any."""
    return request_postgres.controller_owner_from_environment()


def controller_owner_is_current(owner: tuple[str, int]) -> bool:
    """Return whether PostgreSQL still proves this exact outer generation."""
    return request_postgres.controller_leadership_is_current(*owner)


def _controller_owner_values(
    owner: tuple[str, int] | None,) -> dict[sqlalchemy.Column, Any]:
    if owner is None:
        return {
            job_info_table.c.controller_instance_id: None,
            job_info_table.c.controller_generation: None,
        }
    instance_id, generation = owner
    return {
        job_info_table.c.controller_instance_id: instance_id,
        job_info_table.c.controller_generation: generation,
    }


def _controller_owner_matches_columns(
    owner: tuple[str, int],) -> sqlalchemy.ColumnElement[bool]:
    instance_id, generation = owner
    return sqlalchemy.and_(
        job_info_table.c.controller_instance_id == instance_id,
        job_info_table.c.controller_generation == generation,
    )


async def _lock_current_controller_owner_async(session: sql_async.AsyncSession,
                                               owner: tuple[str, int]) -> None:
    result = await session.execute(
        request_postgres.current_controller_leadership_statement(*owner,
                                                                 lock=True))
    if result.scalar_one_or_none() is None:
        raise ControllerLeadershipLostError(
            'Managed-job controller leadership changed before the durable '
            'scheduler write.')


def _lock_current_controller_owner(session: orm.Session,
                                   owner: tuple[str, int]) -> None:
    result = session.execute(
        request_postgres.current_controller_leadership_statement(*owner,
                                                                 lock=True))
    if result.scalar_one_or_none() is None:
        raise ControllerLeadershipLostError(
            'Managed-job controller leadership changed before the durable '
            'recovery write.')


# Keep the historical schema facade for migrations and external callers.
Base = state_schema.Base
batch_state_table = state_schema.batch_state_table
batch_worker_table = state_schema.batch_worker_table
job_events_table = state_schema.job_events_table

# Keep the historical query facade as direct aliases.
# pylint: disable=protected-access
_batch_progress_subquery = state_queries._batch_progress_subquery
# pylint: enable=protected-access

create_table = state_storage.create_table
_db_manager = state_storage.db_manager
migration_utils = state_storage.migration_utils

# Keep the historical job-event facade as direct aliases.
DEFAULT_JOB_EVENT_RETENTION_HOURS = (
    state_events.DEFAULT_JOB_EVENT_RETENTION_HOURS)
JOB_EVENT_DAEMON_INTERVAL_SECONDS = (
    state_events.JOB_EVENT_DAEMON_INTERVAL_SECONDS)
# pylint: disable=protected-access
_get_all_task_ids_async = state_events._get_all_task_ids_async
_get_latest_event_reasons = state_events._get_latest_event_reasons
# pylint: enable=protected-access
add_job_event = state_events.add_job_event
add_job_event_async = state_events.add_job_event_async
get_job_events = state_events.get_job_events
get_latest_recovery_and_pending_reasons = (
    state_events.get_latest_recovery_and_pending_reasons)
get_latest_recovery_reasons = state_events.get_latest_recovery_reasons
cleanup_job_events_with_retention_async = (
    state_events.cleanup_job_events_with_retention_async)
job_event_retention_daemon = state_events.job_event_retention_daemon

# Keep the historical Batch persistence facade as direct aliases.
BatchLifecycleTransition = batch_state.BatchLifecycleTransition
# pylint: disable=protected-access
_supports_update_returning = batch_state._supports_update_returning
_lock_batch_coordinator_row = batch_state._lock_batch_coordinator_row
_lock_batch_coordinator_owner = batch_state._lock_batch_coordinator_owner
_get_batch_worker_row_for_update = batch_state._get_batch_worker_row_for_update
# pylint: enable=protected-access
save_batch_states = batch_state.save_batch_states
is_batch_job = batch_state.is_batch_job
acquire_batch_coordinator = batch_state.acquire_batch_coordinator
is_batch_coordinator_owner = batch_state.is_batch_coordinator_owner
get_batch_states = batch_state.get_batch_states
register_batch_worker_launch = batch_state.register_batch_worker_launch
record_batch_worker_launch_request = batch_state.record_batch_worker_launch_request
record_batch_worker_job_id = batch_state.record_batch_worker_job_id
get_batch_worker_records = batch_state.get_batch_worker_records
remove_batch_worker_record = batch_state.remove_batch_worker_record
claim_batch = batch_state.claim_batch
renew_batch_lease = batch_state.renew_batch_lease
set_batch_attempt_status = batch_state.set_batch_attempt_status
requeue_expired_batch_attempts = batch_state.requeue_expired_batch_attempts
set_batch_winding_down = batch_state.set_batch_winding_down
set_batch_succeeded = batch_state.set_batch_succeeded
set_batch_failed = batch_state.set_batch_failed


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
            if count != 0:
                raise AssertionError((job_id, count))
            if count == 0 and attempt > 0 and prior_update_matched:
                current = await session.execute(
                    sqlalchemy.select(job_info_table.c.schedule_state).where(
                        job_info_table.c.spot_job_id == job_id))
                row = current.fetchone()
                if row is not None and row[0] == target_state.value:
                    return
            raise AssertionError((job_id, count))

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
# pylint: disable=protected-access
_get_jobs_dict = state_queries._get_jobs_dict

# pylint: enable=protected-access


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
        # For tasks that are RECOVERING, set last_recovered_at to the
        # end_time, so that end_at - last_recovered_at does not inflate the
        # job duration with time spent in the failed recovery attempt. The
        # CASE keeps this per-row: other tasks of a multi-task job keep
        # their own last_recovered_at.
        spot_table.c.last_recovered_at: sqlalchemy.case(
            (spot_table.c.status
             == ManagedJobStatus.RECOVERING.value, end_time),
            else_=spot_table.c.last_recovered_at),
    }
    updated = False
    with orm.Session(engine) as session:
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
    """Finalize a job cancelled before it ever started.

    Only applies if the job is still PENDING and its schedule state is a
    pre-launch backlog state (WAITING/INACTIVE); it may fail if another
    process has changed its state in the meantime.

    Finalization is atomic and complete: the schedule state becomes DONE and
    the pending tasks become CANCELLED with an ``end_at``, in one transaction.
    Leaving the schedule state in a pre-launch state would let
    ``get_waiting_job_async`` re-claim the job and actually run it, and would
    keep it counted by ``get_num_alive_jobs`` (which gates controller
    autostop) and re-scanned by every status sweep. Leaving ``end_at`` unset
    would let the job loop's cleanup rewrite the terminal CANCELLED status
    back to CANCELLING, and would render an ever-growing duration.

    The schedule-state compare-and-set is the serialization point: it targets
    the same column ``get_waiting_job_async`` compare-and-swaps, so exactly
    one of "cancel" and "claim" can win.

    Returns:
        True if the job was cancelled, False otherwise.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        # Note: it's possible that a WAITING job actually needs to be cleaned
        # up, if we are in the middle of an upgrade/recovery and the job is
        # waiting to be reclaimed by a new controller. But, in this case the
        # job will have no PENDING task, so this EXISTS guard will not match.
        has_pending_task = sqlalchemy.exists(
            sqlalchemy.select(spot_table.c.job_id).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.status == ManagedJobStatus.PENDING.value,
                )))

        claimed = session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.spot_job_id == job_id,
                    job_info_table.c.schedule_state.in_([
                        ManagedJobScheduleState.WAITING.value,
                        ManagedJobScheduleState.INACTIVE.value,
                    ]),
                    has_pending_task,
                )).values({
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.DONE.value
                })).rowcount

        if not claimed:
            session.rollback()
            return False

        session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.status == ManagedJobStatus.PENDING.value,
                )).values({
                    spot_table.c.status: ManagedJobStatus.CANCELLED.value,
                    spot_table.c.end_at: time.time(),
                }))
        session.commit()

    # Only record the event once the cancel has actually been applied, so a
    # no-op cancel does not leave a CANCELLED event on a job that kept running.
    add_job_event(job_id, None, ManagedJobStatus.CANCELLED,
                  'Job has been cancelled')
    return True


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


def get_jobs_to_check_status(job_ids: list[int] | None = None) -> list[int]:
    """Get jobs that need controller process checking.

    Args:
        job_ids: Optional job IDs to check. If None, checks all jobs.

    Returns a list of modern job_ids, including the following:
    - Jobs that have a schedule_state that is not DONE
    - Jobs have schedule_state DONE but are in a non-terminal status

    Legacy jobs have no schedule state and are handled by their dedicated
    single-job controller paths.
    """
    where_condition = _get_jobs_to_check_status_condition(job_ids)
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


@db_retries.retry
def get_task_id_name_status_log(
    job_id: int, task_id: int
) -> tuple[int, str, ManagedJobStatus, str | None, float | None] | None:
    """Return one task row used by the terminal log-follow path."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                spot_table.c.task_id,
                spot_table.c.task_name,
                spot_table.c.status,
                spot_table.c.local_log_file,
                spot_table.c.logs_cleaned_at,
            ).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                ))).fetchone()
    if row is None:
        return None
    return row[0], row[1], ManagedJobStatus(row[2]), row[3], row[4]


def get_num_tasks(job_id: int) -> int:
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        task_count = session.execute(
            sqlalchemy.select(
                sqlalchemy.func.count(),  # pylint: disable=not-callable
            ).where(spot_table.c.spot_job_id == job_id)).scalar_one()
        return int(task_count)


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
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            _latest_task_status_query([job_id],
                                      terminal_status_values)).fetchone()
    if row is None:
        return None, None
    return row[1], ManagedJobStatus(row[2])


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
        # sync with _merge_jobs_status_check_rows.
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


def get_job_event_task_contexts(job_id: int) -> list[dict[str, Any]]:
    """Return the slim per-task fields needed for job-event cluster merges.

    The managed-job event timeline only needs the per-task ``task_id`` /
    ``task_name`` plus the job-level ``pool`` marker to reconstruct cluster
    names. Reading the full managed-job task rows here would also decode
    metadata and may read YAML content from disk, which is unnecessary for
    this path.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(
                spot_table.c.task_id,
                spot_table.c.task_name,
                job_info_table.c.pool,
            ).select_from(
                spot_table.outerjoin(
                    job_info_table, spot_table.c.spot_job_id ==
                    job_info_table.c.spot_job_id)).where(
                        spot_table.c.spot_job_id == job_id).order_by(
                            spot_table.c.task_id.asc())).fetchall()
    return [{
        'task_id': row.task_id,
        'task_name': row.task_name,
        'pool': row.pool,
    } for row in rows]


# Cap the ids per ``IN (...)`` so a large refresh never overflows the DB
# bind-parameter limit (SQLite's default is ~999); see
# get_jobs_status_check_info.
_STATUS_CHECK_JOB_ID_CHUNK = 500


def _get_jobs_to_check_status_condition(job_ids: list[int] | None = None):
    """Build the filter for jobs that need controller-process checking."""
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]

    # Get jobs that are either:
    # 1. Have schedule state that is not DONE, or
    # 2. Have schedule state DONE AND are in non-terminal status (unexpected
    #    inconsistent state).
    #
    # Legacy single-job controllers have NULL schedule_state. They are not
    # manageable through the consolidated refresh sweep or cancellation path,
    # which only know how to reason about modern schedule-state rows. Fence
    # them out here instead of crashing while decoding a NULL enum.
    condition1 = sqlalchemy.and_(
        job_info_table.c.schedule_state.is_not(None),
        job_info_table.c.schedule_state != ManagedJobScheduleState.DONE.value)
    condition2 = sqlalchemy.and_(
        job_info_table.c.schedule_state == ManagedJobScheduleState.DONE.value,
        ~spot_table.c.status.in_(terminal_status_values),
    )
    where_condition = sqlalchemy.or_(condition1, condition2)
    if job_ids is not None:
        where_condition = sqlalchemy.and_(where_condition,
                                          spot_table.c.spot_job_id.in_(job_ids))
    return where_condition


def _status_check_select(from_clause) -> 'sqlalchemy.Select':
    """The slim projection shared by the status-check snapshots."""
    return sqlalchemy.select(
        spot_table.c.spot_job_id,
        spot_table.c.task_id,
        spot_table.c.status,
        spot_table.c.task_name,
        spot_table.c.submitted_at,
        spot_table.c.start_at,
        spot_table.c.last_recovered_at,
        job_info_table.c.name.label('job_info_name'),
        job_info_table.c.schedule_state,
        job_info_table.c.controller_pid,
        job_info_table.c.controller_pid_started_at,
        job_info_table.c.controller_instance_id,
        job_info_table.c.controller_generation,
        job_info_table.c.pool,
    ).select_from(from_clause)


def _spot_job_info_outerjoin():
    return spot_table.outerjoin(
        job_info_table,
        spot_table.c.spot_job_id == job_info_table.c.spot_job_id)


def _collect_status_check_snapshot(
    job_ids: list[int] | None, fetch_chunk: Callable[[list[int] | None],
                                                     list[Any]]
) -> dict[int, dict[str, Any]]:
    """Chunk ``job_ids`` and merge the fetched rows into one snapshot.

    Explicit job-id callers may repeat the same job across retry, cancel, or
    refresh selection paths. Deduping before chunking keeps the snapshot
    stable and avoids re-fetching/re-merging the same task rows when a repeated
    id would otherwise spill into a later chunk.
    """
    result: dict[int, dict[str, Any]] = {}
    if job_ids is None:
        _merge_jobs_status_check_rows(result, fetch_chunk(None))
        return result
    unique_job_ids = list(dict.fromkeys(job_ids))
    for start in range(0, len(unique_job_ids), _STATUS_CHECK_JOB_ID_CHUNK):
        chunk = unique_job_ids[start:start + _STATUS_CHECK_JOB_ID_CHUNK]
        _merge_jobs_status_check_rows(result, fetch_chunk(chunk))
    return result


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
                'controller_instance_id': mapping['controller_instance_id'],
                'controller_generation': mapping['controller_generation'],
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
            'submitted_at': mapping['submitted_at'],
            'start_at': mapping['start_at'],
            'last_recovered_at': mapping['last_recovered_at'],
        })


def get_jobs_to_check_status_info(
        job_ids: list[int] | None = None) -> dict[int, dict[str, Any]]:
    """One-query slim snapshot for jobs needing controller-process checking.

    The status-refresh sweep needs two things from the same tables:
    1. identify which jobs require checking; and
    2. fetch the slim per-task fields it actually consumes.

    The old path did those as two round trips. This helper keeps the same
    per-job/per-task shape as ``get_jobs_status_check_info`` but does the
    "which jobs?" filter in a subquery and returns the full slim snapshot in
    one SQL statement.

    When ``job_ids`` is given, the filter is chunked so a large batch (e.g.
    ``sky jobs cancel --all``) never overflows the DB bind-parameter limit.
    """
    engine = _db_manager.get_engine()

    def _fetch_chunk(chunk: list[int] | None) -> list[Any]:
        jobs_to_check = sqlalchemy.select(
            spot_table.c.spot_job_id.label('spot_job_id')).select_from(
                _spot_job_info_outerjoin()).where(
                    _get_jobs_to_check_status_condition(
                        chunk)).distinct().subquery()
        query = _status_check_select(_spot_job_info_outerjoin().join(
            jobs_to_check,
            spot_table.c.spot_job_id == jobs_to_check.c.spot_job_id)).order_by(
                spot_table.c.spot_job_id.desc(), spot_table.c.task_id.asc())
        with orm.Session(engine) as session:
            return session.execute(query).fetchall()

    return _collect_status_check_snapshot(job_ids, _fetch_chunk)


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
    task_name, submitted_at, start_at, last_recovered_at}]}``
    with ``tasks`` ordered by ``task_id``. Job ids with no task rows are absent
    from the result.

    ``job_name`` mirrors ``get_managed_job_tasks`` (public display name with a
    fallback to ``task_name``); ``task_name`` preserves the controller launch
    identity for teardown/recovery logic. The launch-attempt timestamps keep
    cleanup on the slim snapshot path while distinguishing untouched backlog
    tasks from tasks that already started launching and later fell back to
    ``PENDING`` during recovery.
    """
    if not job_ids:
        return {}
    engine = _db_manager.get_engine()

    def _fetch_chunk(chunk: list[int] | None) -> list[Any]:
        assert chunk is not None
        query = _status_check_select(_spot_job_info_outerjoin()).where(
            spot_table.c.spot_job_id.in_(chunk)).order_by(
                spot_table.c.spot_job_id.asc(), spot_table.c.task_id.asc())
        with orm.Session(engine) as session:
            return session.execute(query).fetchall()

    return _collect_status_check_snapshot(job_ids, _fetch_chunk)


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
                job_info_table.c.controller_instance_id,
                job_info_table.c.controller_generation,
                sqlalchemy.func.count(  # pylint: disable=not-callable
                    spot_table.c.task_id).label('task_count'),
                sqlalchemy.func.sum(
                    sqlalchemy.case(
                        (~spot_table.c.status.in_(terminal_status_values), 1),
                        else_=0)).label('nonterminal_task_count'),
            ).select_from(
                job_info_table.outerjoin(
                    spot_table, spot_table.c.spot_job_id ==
                    job_info_table.c.spot_job_id)).where(
                        job_info_table.c.spot_job_id == job_id).group_by(
                            job_info_table.c.schedule_state,
                            job_info_table.c.controller_pid,
                            job_info_table.c.controller_pid_started_at,
                            job_info_table.c.controller_instance_id,
                            job_info_table.c.controller_generation)).fetchone()
    if row is None or row[0] is None:
        return None
    task_count = int(row[5] or 0)
    nonterminal_task_count = int(row[6] or 0)
    return {
        'schedule_state': ManagedJobScheduleState(row[0]),
        'controller_pid': row[1],
        'controller_pid_started_at': row[2],
        'controller_instance_id': row[3],
        'controller_generation': row[4],
        'all_tasks_terminal': task_count > 0 and nonterminal_task_count == 0,
    }


def _controller_snapshot_conditions(
    job_id: int,
    schedule_state: ManagedJobScheduleState,
    controller_pid: int | None,
    controller_pid_started_at: float | None,
    controller_instance_id: str | None,
    controller_generation: int | None,
) -> list[sqlalchemy.ColumnElement[bool]]:
    """Build the exact job-info snapshot used by destructive refresh writes."""
    return [
        job_info_table.c.spot_job_id == job_id,
        job_info_table.c.schedule_state == schedule_state.value,
        job_info_table.c.controller_pid == controller_pid,
        job_info_table.c.controller_pid_started_at == controller_pid_started_at,
        job_info_table.c.controller_instance_id == controller_instance_id,
        job_info_table.c.controller_generation == controller_generation,
    ]


def set_failed_controller_if_current_snapshot(
    job_id: int,
    *,
    schedule_state: ManagedJobScheduleState,
    controller_pid: int | None,
    controller_pid_started_at: float | None,
    controller_instance_id: str | None,
    controller_generation: int | None,
    failure_reason: str,
) -> bool:
    """Terminalize an exact dead-controller snapshot before provider cleanup.

    The job's schedule state intentionally remains non-DONE. If this process
    exits during cleanup, a replacement leader sees terminal tasks, retries the
    idempotent teardown, and completes the schedule state instead of recovering
    the workload.
    """
    owner = get_current_controller_owner()
    recorded_owner = (controller_instance_id, controller_generation)
    if owner is not None and recorded_owner != owner:
        return False

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if owner is not None:
            _lock_current_controller_owner(session, owner)
        conditions = _controller_snapshot_conditions(job_id, schedule_state,
                                                     controller_pid,
                                                     controller_pid_started_at,
                                                     controller_instance_id,
                                                     controller_generation)
        job_row = session.execute(
            sqlalchemy.select(job_info_table.c.spot_job_id).where(
                sqlalchemy.and_(*conditions)).with_for_update()).first()
        if job_row is None:
            session.rollback()
            return False

        task_rows = session.execute(
            sqlalchemy.select(spot_table.c.status, spot_table.c.failure_reason).
            where(spot_table.c.spot_job_id == job_id).with_for_update()).all()
        if not task_rows or all(
                ManagedJobStatus(row.status).is_terminal()
                for row in task_rows):
            session.rollback()
            return False

        existing_reason = next(
            (row.failure_reason for row in task_rows if row.failure_reason),
            None)
        persisted_reason = failure_reason
        if existing_reason:
            persisted_reason += f'. Previously: {existing_reason}'
        end_time = time.time()
        result = session.execute(
            sqlalchemy.update(spot_table).where(
                spot_table.c.spot_job_id == job_id).values({
                    spot_table.c.status:
                        ManagedJobStatus.FAILED_CONTROLLER.value,
                    spot_table.c.failure_reason: persisted_reason,
                    spot_table.c.last_recovered_at: sqlalchemy.case(
                        (spot_table.c.status
                         == ManagedJobStatus.RECOVERING.value, end_time),
                        else_=spot_table.c.last_recovered_at),
                    spot_table.c.end_at: sqlalchemy.func.coalesce(
                        spot_table.c.end_at, end_time),
                }))
        session.commit()
        return result.rowcount > 0


def finish_controller_cleanup_if_current_snapshot(
    job_id: int,
    *,
    schedule_state: ManagedJobScheduleState,
    controller_pid: int | None,
    controller_pid_started_at: float | None,
    controller_instance_id: str | None,
    controller_generation: int | None,
) -> bool:
    """Mark an exactly rechecked terminal job DONE after provider cleanup.

    A current leader may finish terminal cleanup left by a stale generation.
    It proves its own outer fence, compare-and-swaps the recorded snapshot, and
    stamps the completing generation on the durable row.
    """
    owner = get_current_controller_owner()
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if owner is not None:
            _lock_current_controller_owner(session, owner)
        conditions = _controller_snapshot_conditions(job_id, schedule_state,
                                                     controller_pid,
                                                     controller_pid_started_at,
                                                     controller_instance_id,
                                                     controller_generation)
        job_row = session.execute(
            sqlalchemy.select(job_info_table.c.spot_job_id).where(
                sqlalchemy.and_(*conditions)).with_for_update()).first()
        if job_row is None:
            session.rollback()
            return False
        task_statuses = session.execute(
            sqlalchemy.select(spot_table.c.status).where(
                spot_table.c.spot_job_id ==
                job_id).with_for_update()).scalars().all()
        if (not task_statuses or any(not ManagedJobStatus(status).is_terminal()
                                     for status in task_statuses)):
            session.rollback()
            return False

        values: dict[sqlalchemy.Column, Any] = {
            job_info_table.c.schedule_state: ManagedJobScheduleState.DONE.value,
        }
        values.update(_controller_owner_values(owner))
        result = session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(*conditions)).values(values))
        session.commit()
        return result.rowcount == 1


def get_job_cancellation_states(
        job_ids: list[int]) -> dict[int, JobCancellationState]:
    """Return slim, batched snapshots for managed-job cancellation.

    Cancellation needs the currently executable task's status together with
    workspace authorization. Reading those fields together avoids separate
    point queries and prevents decisions assembled from different lifecycle
    snapshots. Only the latest task row per job matters for cancellation, so
    the query keeps the row volume bounded by job count instead of total task
    count.

    Only jobs with durable modern lifecycle fields (workspace and
    schedule_state) participate in this snapshot. Legacy single-job
    controllers used a separate signal transport; without backward-compatibility
    support, modern cancellation excludes those rows instead of carrying a
    second delivery path.
    """
    if not job_ids:
        return {}

    snapshots: dict[int, JobCancellationState] = {}
    for job_id, _, status, workspace in _fetch_job_cancellation_state_rows(
            job_ids):
        snapshots[job_id] = JobCancellationState(
            status=ManagedJobStatus(status),
            workspace=workspace,
        )
    return snapshots


def _latest_task_ids_subquery(
        job_ids: list[int],
        terminal_status_values: list[str]) -> sqlalchemy.sql.Selectable:
    """Select one latest-task identity per requested job.

    This follows ``get_latest_task_id_from_statuses`` exactly: choose the first
    non-terminal task if one exists, otherwise the last terminal task.
    """
    return sqlalchemy.select(
        spot_table.c.spot_job_id.label('spot_job_id'),
        sqlalchemy.func.coalesce(  # pylint: disable=not-callable
            sqlalchemy.func.min(
                sqlalchemy.case(
                    (~spot_table.c.status.in_(terminal_status_values),
                     spot_table.c.task_id),
                    else_=None)),
            sqlalchemy.func.max(spot_table.c.task_id),
        ).label('task_id')).where(
            spot_table.c.spot_job_id.in_(job_ids)).group_by(
                spot_table.c.spot_job_id).subquery()


def _latest_task_status_query(
        job_ids: list[int],
        terminal_status_values: list[str]) -> sqlalchemy.sql.Selectable:
    """Select the latest-task status row for each requested job."""
    latest_task_ids = _latest_task_ids_subquery(job_ids, terminal_status_values)
    selected_status = sqlalchemy.func.coalesce(  # pylint: disable=not-callable
        sqlalchemy.func.min(
            sqlalchemy.case((~spot_table.c.status.in_(terminal_status_values),
                             spot_table.c.status),
                            else_=None)),
        sqlalchemy.func.max(spot_table.c.status),
    ).label('status')
    return sqlalchemy.select(
        latest_task_ids.c.spot_job_id,
        latest_task_ids.c.task_id,
        selected_status,
    ).select_from(
        latest_task_ids.join(
            spot_table,
            sqlalchemy.and_(
                spot_table.c.spot_job_id == latest_task_ids.c.spot_job_id,
                spot_table.c.task_id == latest_task_ids.c.task_id))).group_by(
                    latest_task_ids.c.spot_job_id,
                    latest_task_ids.c.task_id).order_by(
                        latest_task_ids.c.spot_job_id.asc())


def _fetch_job_cancellation_state_rows(job_ids: list[int]) -> list[Any]:
    """Fetch one cancellation-driving task row per requested job.

    Cancellation follows ``get_latest_task_id_from_statuses`` semantics:
    pick the first non-terminal task if one exists, otherwise the last
    terminal task. Only jobs with durable modern ``job_info`` fields are
    cancellable through the consolidated signal path, so legacy rows are
    excluded here.
    """
    unique_job_ids = list(dict.fromkeys(job_ids))
    if not unique_job_ids:
        return []

    engine = _db_manager.get_engine()
    rows: list[Any] = []
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    for start in range(0, len(unique_job_ids), _STATUS_CHECK_JOB_ID_CHUNK):
        chunk = unique_job_ids[start:start + _STATUS_CHECK_JOB_ID_CHUNK]
        latest_task_ids = _latest_task_ids_subquery(chunk,
                                                    terminal_status_values)
        query = sqlalchemy.select(
            latest_task_ids.c.spot_job_id,
            latest_task_ids.c.task_id,
            spot_table.c.status,
            job_info_table.c.workspace,
        ).select_from(
            latest_task_ids.join(
                spot_table,
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == latest_task_ids.c.spot_job_id,
                    spot_table.c.task_id == latest_task_ids.c.task_id)).join(
                        job_info_table, latest_task_ids.c.spot_job_id ==
                        job_info_table.c.spot_job_id)).where(
                            job_info_table.c.workspace.is_not(None),
                            job_info_table.c.schedule_state.is_not(
                                None)).order_by(
                                    latest_task_ids.c.spot_job_id.asc())
        with orm.Session(engine) as session:
            rows.extend(session.execute(query).fetchall())
    return rows


def has_jobs_requiring_recovery_grace_wait() -> bool:
    """Whether HA leader handoff should pause before managed-job recovery.

    Any nonterminal scheduler row can race a detached controller from the prior
    image during a mixed-version handoff. In particular, an old scheduler can
    claim a WAITING row after this query if that image predates durable
    generation ownership. Keep the bounded drain for every nonterminal job
    until the compatibility image is outside the rollback window.
    """
    engine = _db_manager.get_engine()
    query = sqlalchemy.select(sqlalchemy.literal(True)).where(
        sqlalchemy.and_(
            job_info_table.c.schedule_state.is_not(None),
            job_info_table.c.schedule_state
            != ManagedJobScheduleState.DONE.value,
        )).limit(1)
    with orm.Session(engine) as session:
        return session.execute(query).first() is not None


# pylint: disable=protected-access
_map_response_field_to_db_column = (
    state_queries._map_response_field_to_db_column)
# pylint: enable=protected-access
get_managed_jobs_total = state_queries.get_managed_jobs_total


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
        job_info_table.c.file_mounts_blob_id).distinct().where(
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


build_managed_jobs_with_filters_no_status_query = (
    state_queries.build_managed_jobs_with_filters_no_status_query)
build_managed_jobs_with_filters_query = (
    state_queries.build_managed_jobs_with_filters_query)
get_status_count_with_filters = state_queries.get_status_count_with_filters
get_status_counts = state_queries.get_status_counts
get_status_counts_by_workspace_user_cloud = (
    state_queries.get_status_counts_by_workspace_user_cloud)
get_managed_jobs_with_filters = state_queries.get_managed_jobs_with_filters


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


@db_retries.retry
def get_log_stream_context(
    job_id: int,
    task_id: int,
) -> tuple[str | None, str | None, int | None, str | None]:
    """Return the launch target fields needed to stream one task's logs.

    Pool membership, the current pool cluster, the job ID on that cluster,
    and the task name must describe one database snapshot. Reading them in
    separate sessions can mix recovery epochs while a log follower is choosing
    its target.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        context = session.execute(
            sqlalchemy.select(
                job_info_table.c.pool,
                job_info_table.c.current_cluster_name,
                job_info_table.c.job_id_on_pool_cluster,
                spot_table.c.task_name,
            ).select_from(
                spot_table.outerjoin(
                    job_info_table, job_info_table.c.spot_job_id ==
                    spot_table.c.spot_job_id)).where(
                        sqlalchemy.and_(
                            spot_table.c.spot_job_id == job_id,
                            spot_table.c.task_id == task_id,
                        ))).fetchone()
    if context is None:
        return None, None, None, None
    return context[0], context[1], context[2], context[3]


@db_retries.retry
def get_task_log_stream_snapshot(job_id: int,
                                 task_id: int) -> JobLogStreamSnapshot:
    """Return one task-specific status and routing snapshot for log following.

    When callers follow a specific task in a JobGroup, the task status and its
    routing context must come from the same database snapshot. Otherwise a
    later task can advance the job-level latest-task status between the two
    reads and make the follower wait on the wrong lifecycle.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                spot_table.c.task_id,
                spot_table.c.status,
                job_info_table.c.pool,
                job_info_table.c.current_cluster_name,
                job_info_table.c.job_id_on_pool_cluster,
                spot_table.c.task_name,
            ).select_from(
                spot_table.outerjoin(
                    job_info_table, job_info_table.c.spot_job_id ==
                    spot_table.c.spot_job_id)).where(
                        sqlalchemy.and_(
                            spot_table.c.spot_job_id == job_id,
                            spot_table.c.task_id == task_id,
                        ))).fetchone()
    if row is None:
        return JobLogStreamSnapshot(None, None, None, None, None, None)
    return JobLogStreamSnapshot(
        row.task_id,
        ManagedJobStatus(row.status),
        row.pool,
        row.current_cluster_name,
        row.job_id_on_pool_cluster,
        row.task_name,
    )


@db_retries.retry
def get_latest_log_stream_snapshot(job_id: int) -> JobLogStreamSnapshot:
    """Return one latest-task status and routing snapshot for log following."""
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    latest_task = _latest_task_status_query([job_id],
                                            terminal_status_values).subquery()
    query = sqlalchemy.select(
        latest_task.c.task_id,
        latest_task.c.status,
        job_info_table.c.pool,
        job_info_table.c.current_cluster_name,
        job_info_table.c.job_id_on_pool_cluster,
        spot_table.c.task_name,
    ).select_from(
        latest_task.join(
            spot_table,
            sqlalchemy.and_(
                spot_table.c.spot_job_id == latest_task.c.spot_job_id,
                spot_table.c.task_id == latest_task.c.task_id,
            )).outerjoin(
                job_info_table,
                job_info_table.c.spot_job_id == latest_task.c.spot_job_id))

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(query).fetchone()
    if row is None:
        return JobLogStreamSnapshot(None, None, None, None, None, None)
    return JobLogStreamSnapshot(
        row.task_id,
        ManagedJobStatus(row.status),
        row.pool,
        row.current_cluster_name,
        row.job_id_on_pool_cluster,
        row.task_name,
    )


@db_retries.retry
def get_controller_log_follow_state(job_id: int) -> ControllerLogFollowState:
    """Return the lifecycle snapshot that drives controller-log following."""
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    latest_task = _latest_task_status_query([job_id],
                                            terminal_status_values).subquery()
    query = sqlalchemy.select(
        latest_task.c.status,
        job_info_table.c.schedule_state,
    ).select_from(
        latest_task.outerjoin(
            job_info_table,
            job_info_table.c.spot_job_id == latest_task.c.spot_job_id))

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(query).fetchone()
    if row is None:
        return ControllerLogFollowState(None, None)
    schedule_state = (None if row.schedule_state is None else
                      ManagedJobScheduleState(row.schedule_state))
    return ControllerLogFollowState(
        ManagedJobStatus(row.status),
        schedule_state,
    )


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
    unique_job_ids = list(dict.fromkeys(job_ids))
    if not unique_job_ids:
        return

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        owner = get_current_controller_owner()
        if owner is not None:
            _lock_current_controller_owner(session, owner)
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
            sqlalchemy.and_(job_info_table.c.spot_job_id.in_(unique_job_ids),
                           )).update(updates)
        session.commit()
        if updated_count != len(unique_job_ids):
            raise AssertionError((unique_job_ids, updated_count))


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
def get_pool_and_current_cluster_name(
        job_id: int) -> tuple[str | None, str | None]:
    """Read the pool binding and current pool worker from one job row."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        info = session.execute(
            sqlalchemy.select(
                job_info_table.c.pool,
                job_info_table.c.current_cluster_name).where(
                    job_info_table.c.spot_job_id == job_id)).fetchone()
        if info is None:
            return None, None
        return info[0], info[1]


@db_retries.retry_async
async def get_pool_and_execution_from_job_id_async(
        job_id: int) -> tuple[str | None, str | None]:
    """Get the pool and DAG execution mode from the job id in one query.

    Both columns are fixed at submission time, so they can always be read
    together. Each is None when the job is unknown or its row has no recorded
    value (writers may store an explicit NULL for execution, e.g. legacy code
    paths that predate the column). Callers use execution to decide
    JobGroup-ness ('parallel' == JobGroup) without fetching and re-parsing the
    full DAG YAML.
    """
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        result = await session.execute(
            sqlalchemy.select(job_info_table.c.pool,
                              job_info_table.c.execution).where(
                                  job_info_table.c.spot_job_id == job_id))
        info = result.fetchone()
        if info is None:
            return None, None
        return info[0], info[1]


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
def get_releasable_api_access_token_id(job_id: int) -> str | None:
    """Return this job's token only when every associated job is terminal."""
    engine = _db_manager.get_engine()
    owner = api_access_token_table.alias('token_owner')
    sibling = api_access_token_table.alias('token_sibling')
    sibling_tasks = sibling.outerjoin(
        spot_table, sibling.c.job_id == spot_table.c.spot_job_id)
    terminal_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    unreleasable_sibling = sqlalchemy.exists(
        sqlalchemy.select(1).select_from(sibling_tasks).where(
            sibling.c.token_id == owner.c.token_id,
            sqlalchemy.or_(spot_table.c.status.is_(None),
                           spot_table.c.status.not_in(terminal_values))))
    query = sqlalchemy.select(owner.c.token_id).where(owner.c.job_id == job_id,
                                                      ~unreleasable_sibling)
    with orm.Session(engine) as session:
        return session.execute(query).scalar_one_or_none()


@db_retries.retry_async
async def scheduler_set_launching_async(job_id: int):
    owner = get_current_controller_owner()
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        if owner is not None:
            await _lock_current_controller_owner_async(session, owner)
        conditions = [job_info_table.c.spot_job_id == job_id]
        if owner is not None:
            conditions.append(_controller_owner_matches_columns(owner))
        result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(*conditions)).values({
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.LAUNCHING.value
                }))
        await session.commit()
        if result.rowcount != 1:
            raise ControllerLeadershipLostError(
                f'Managed job {job_id} is no longer owned by this controller '
                'generation.')


async def scheduler_set_backoff_async(job_id: int) -> None:
    """Transition a launching job to resource backoff."""

    async def _op(session: sql_async.AsyncSession) -> int:
        owner = get_current_controller_owner()
        if owner is not None:
            await _lock_current_controller_owner_async(session, owner)
        conditions = [
            job_info_table.c.spot_job_id == job_id,
            job_info_table.c.schedule_state ==
            ManagedJobScheduleState.LAUNCHING.value,
        ]
        if owner is not None:
            conditions.append(_controller_owner_matches_columns(owner))
        result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(*conditions)).values({
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
        owner = get_current_controller_owner()
        if owner is not None:
            await _lock_current_controller_owner_async(session, owner)
        conditions = [
            job_info_table.c.spot_job_id == job_id,
            job_info_table.c.schedule_state ==
            ManagedJobScheduleState.LAUNCHING.value,
        ]
        if owner is not None:
            conditions.append(_controller_owner_matches_columns(owner))
        result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(*conditions)).values({
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
        owner = get_current_controller_owner()
        if owner is not None:
            _lock_current_controller_owner(session, owner)
        conditions = [
            job_info_table.c.spot_job_id == job_id,
            job_info_table.c.schedule_state
            != ManagedJobScheduleState.DONE.value,
        ]
        if owner is not None:
            conditions.append(_controller_owner_matches_columns(owner))
        updated_count = session.query(job_info_table).filter(
            sqlalchemy.and_(*conditions)).update({
                job_info_table.c.schedule_state:
                    ManagedJobScheduleState.DONE.value
            })
        session.commit()
        if not idempotent and updated_count != 1:
            raise AssertionError((job_id, updated_count))


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
            spot_table.c.spot_job_id).distinct().select_from(
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


def get_nonterminal_job_status_counts_by_pool(pool: str) -> dict[str, int]:
    """Get nonterminal pool queue-row counts grouped by status.

    The pool dashboard badges historically counted the nonterminal task rows
    returned by ``jobs/queue/v2`` for a pool, not distinct job ids. Keep that
    semantics while replacing the dashboard's second full queue fetch with one
    grouped DB query owned by the pool-status snapshot.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        query = sqlalchemy.select(
            spot_table.c.status,
            # pylint: disable=not-callable
            sqlalchemy.func.count(spot_table.c.task_id),
        ).select_from(
            spot_table.outerjoin(
                job_info_table, spot_table.c.spot_job_id ==
                job_info_table.c.spot_job_id)).where(
                    sqlalchemy.and_(
                        ~spot_table.c.status.in_([
                            status.value
                            for status in ManagedJobStatus.terminal_statuses()
                        ]),
                        job_info_table.c.pool == pool,
                    )).group_by(spot_table.c.status)
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


def _ranked_nonterminal_job_resources(
    *,
    job_ids: set[int] | None = None,
    pool: str | None = None,
) -> Any:
    """Return nonterminal task resources ranked within each Managed Job.

    ``full_resources`` is a PostgreSQL ``json`` column, so it cannot
    participate in ``DISTINCT`` or ``GROUP BY``. Rank rows using scalar task
    identity instead, then let callers select rank one.
    """
    columns = [
        spot_table.c.spot_job_id,
        spot_table.c.full_resources,
        sqlalchemy.func.row_number().over(
            partition_by=spot_table.c.spot_job_id,
            order_by=spot_table.c.task_id.asc(),
        ).label('task_rank'),
    ]
    from_clause = spot_table
    conditions = [
        ~spot_table.c.status.in_(
            [status.value for status in ManagedJobStatus.terminal_statuses()])
    ]
    if job_ids is not None:
        conditions.append(spot_table.c.spot_job_id.in_(job_ids))
    if pool is not None:
        columns.insert(0, job_info_table.c.current_cluster_name)
        from_clause = spot_table.join(
            job_info_table,
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
        )
        conditions.append(job_info_table.c.pool == pool)
    return sqlalchemy.select(*columns).select_from(from_clause).where(
        sqlalchemy.and_(*conditions)).subquery()


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
        ranked_resources = _ranked_nonterminal_job_resources(job_ids=job_ids)
        query = sqlalchemy.select(
            ranked_resources.c.spot_job_id,
            ranked_resources.c.full_resources,
        ).where(ranked_resources.c.task_rank == 1)
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
        ranked_resources = _ranked_nonterminal_job_resources(pool=pool)
        query = sqlalchemy.select(
            ranked_resources.c.current_cluster_name,
            ranked_resources.c.spot_job_id,
            ranked_resources.c.full_resources,
        ).where(ranked_resources.c.task_rank == 1)
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
    owner = get_current_controller_owner()
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        if owner is not None:
            # Serialize this claim with outer-controller generation
            # advancement. The leadership row and managed-job tables share the
            # central PostgreSQL database even though they use separate
            # SQLAlchemy engine namespaces.
            await _lock_current_controller_owner_async(session, owner)

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
        update_values = {
            job_info_table.c.schedule_state:
                ManagedJobScheduleState.LAUNCHING.value,
            job_info_table.c.controller_pid: pid,
            job_info_table.c.controller_pid_started_at: pid_started_at,
        }
        update_values.update(_controller_owner_values(owner))
        update_result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.spot_job_id == job_id,
                    job_info_table.c.schedule_state == current_state.value,
                )).values(update_values))

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


@db_retries.retry_async
async def get_latest_task_id_status_async(
        job_id: int) -> tuple[int | None, ManagedJobStatus | None]:
    """Returns the (task id, status) of the latest task of a job."""
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        row = (await session.execute(
            _latest_task_status_query([job_id],
                                      terminal_status_values))).fetchone()
    if row is None:
        return None, None
    return row[1], ManagedJobStatus(row[2])


@db_retries.retry_async
async def get_statuses_async(
        job_ids: list[int]) -> dict[int, ManagedJobStatus | None]:
    """Return latest task statuses for jobs from bounded batch reads."""
    if not job_ids:
        return {}

    unique_job_ids = list(dict.fromkeys(job_ids))
    statuses: dict[int, ManagedJobStatus | None] = {
        job_id: None for job_id in unique_job_ids
    }
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        for start in range(0, len(unique_job_ids), _STATUS_CHECK_JOB_ID_CHUNK):
            chunk = unique_job_ids[start:start + _STATUS_CHECK_JOB_ID_CHUNK]
            result = await session.execute(
                _latest_task_status_query(chunk, terminal_status_values))
            for job_id, _, status in result.fetchall():
                statuses[job_id] = ManagedJobStatus(status)

    return statuses


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


def get_job_task_terminal_states(
    identities: list[tuple[int, int]],) -> dict[tuple[int, int], bool]:
    """Returns terminal state for a bounded set of durable job-task owners."""
    if not identities:
        return {}
    if len(identities) > 1000:
        raise ValueError('Managed-job terminal-state batch is too large.')
    wanted = sorted(set(identities))
    engine = _db_manager.get_engine()
    rows = []
    with orm.Session(engine) as session:
        for start in range(0, len(wanted), _TERMINAL_IDENTITY_QUERY_BATCH_SIZE):
            batch = wanted[start:start + _TERMINAL_IDENTITY_QUERY_BATCH_SIZE]
            rows.extend(
                session.execute(
                    sqlalchemy.select(
                        spot_table.c.spot_job_id, spot_table.c.task_id,
                        spot_table.c.status).where(
                            sqlalchemy.tuple_(
                                spot_table.c.spot_job_id,
                                spot_table.c.task_id).in_(batch))).all())
    return {
        (int(row[0]), int(row[1])): ManagedJobStatus(row[2]).is_terminal()
        for row in rows
    }


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


@db_retries.retry_async
async def get_image_recovery_generation_async(job_id: int, task_id: int) -> int:
    """Returns the durable launch generation for managed image ownership."""
    engine = await _db_manager.get_async_engine()
    async with sql_async.AsyncSession(engine) as session:
        row = (await session.execute(
            sqlalchemy.select(spot_table.c.status,
                              spot_table.c.recovery_count).where(
                                  spot_table.c.spot_job_id == job_id,
                                  spot_table.c.task_id == task_id))).first()
    if row is None:
        raise ValueError('Managed job task does not exist.')
    generation = int(row.recovery_count or 0)
    if ManagedJobStatus(row.status) == ManagedJobStatus.RECOVERING:
        generation += 1
    return generation


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
            # Per-row RECOVERING adjustment; see set_failed for rationale.
            spot_table.c.last_recovered_at: sqlalchemy.case(
                (spot_table.c.status
                 == ManagedJobStatus.RECOVERING.value, end_time),
                else_=spot_table.c.last_recovered_at),
        }
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
        owner = get_current_controller_owner()
        if owner is not None:
            await _lock_current_controller_owner_async(session, owner)
        conditions = [
            job_info_table.c.spot_job_id == job_id,
            job_info_table.c.schedule_state
            != ManagedJobScheduleState.DONE.value,
        ]
        if owner is not None:
            conditions.append(_controller_owner_matches_columns(owner))
        result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(*conditions)).values({
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
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    has_nonterminal_task = sqlalchemy.exists(
        sqlalchemy.select(spot_table.c.task_id).where(
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
            spot_table.c.status.not_in(terminal_status_values)))
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
            has_nonterminal_task,
        ).update({
            job_info_table.c.controller_pid: None,
            job_info_table.c.controller_pid_started_at: None,
            job_info_table.c.controller_instance_id: None,
            job_info_table.c.controller_generation: None,
            job_info_table.c.schedule_state:
                (ManagedJobScheduleState.WAITING.value)
        })
        session.commit()


def reset_stale_jobs_for_current_controller() -> int:
    """Reset jobs owned by another outer controller generation.

    The leadership-row lock serializes this recovery write with generation
    advancement. INACTIVE jobs are excluded because request submission may
    still be populating their durable inputs; DONE jobs need no scheduler.
    """
    owner = get_current_controller_owner()
    if owner is None:
        return 0

    engine = _db_manager.get_engine()
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    has_nonterminal_task = sqlalchemy.exists(
        sqlalchemy.select(spot_table.c.task_id).where(
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
            spot_table.c.status.not_in(terminal_status_values)))
    with orm.Session(engine) as session:
        _lock_current_controller_owner(session, owner)
        stale_owner = sqlalchemy.or_(
            job_info_table.c.controller_instance_id.is_(None),
            job_info_table.c.controller_generation.is_(None),
            sqlalchemy.not_(_controller_owner_matches_columns(owner)),
        )
        result = session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.schedule_state.is_not(None),
                    job_info_table.c.schedule_state.not_in([
                        ManagedJobScheduleState.INACTIVE.value,
                        ManagedJobScheduleState.DONE.value,
                    ]),
                    has_nonterminal_task,
                    stale_owner,
                )).values({
                    job_info_table.c.controller_pid: None,
                    job_info_table.c.controller_pid_started_at: None,
                    job_info_table.c.controller_instance_id: None,
                    job_info_table.c.controller_generation: None,
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.WAITING.value,
                }))
        session.commit()
        return result.rowcount


def reset_job_for_recovery_if_stale(job_id: int, owner: tuple[str,
                                                              int]) -> bool:
    """Reset one stale-generation job without clobbering a fresh reclaim."""
    engine = _db_manager.get_engine()
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    has_nonterminal_task = sqlalchemy.exists(
        sqlalchemy.select(spot_table.c.task_id).where(
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
            spot_table.c.status.not_in(terminal_status_values)))
    with orm.Session(engine) as session:
        _lock_current_controller_owner(session, owner)
        stale_owner = sqlalchemy.or_(
            job_info_table.c.controller_instance_id.is_(None),
            job_info_table.c.controller_generation.is_(None),
            sqlalchemy.not_(_controller_owner_matches_columns(owner)),
        )
        result = session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.spot_job_id == job_id,
                    job_info_table.c.schedule_state.is_not(None),
                    job_info_table.c.schedule_state.not_in([
                        ManagedJobScheduleState.INACTIVE.value,
                        ManagedJobScheduleState.DONE.value,
                    ]),
                    has_nonterminal_task,
                    stale_owner,
                )).values({
                    job_info_table.c.controller_pid: None,
                    job_info_table.c.controller_pid_started_at: None,
                    job_info_table.c.controller_instance_id: None,
                    job_info_table.c.controller_generation: None,
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.WAITING.value,
                }))
        session.commit()
        return result.rowcount == 1


def reset_job_for_recovery(job_id: int) -> None:
    """Set a job to WAITING and remove PID, allowing it to be recovered."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        session.query(job_info_table).filter(
            job_info_table.c.spot_job_id == job_id).update({
                job_info_table.c.controller_pid: None,
                job_info_table.c.controller_pid_started_at: None,
                job_info_table.c.controller_instance_id: None,
                job_info_table.c.controller_generation: None,
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


def get_task_logs_to_clean(
    retention_seconds: int,
    batch_size: int,
    exclude_tasks: Collection[tuple[int, int]] | None = None
) -> list[dict[str, Any]]:
    """Get the logs of job tasks to clean.

    The logs of a task will only cleaned when:
    - the job schedule state is DONE
    - AND the end time of the task is older than the retention period

    Tasks in `exclude_tasks` ((job_id, task_id) pairs) are skipped so a
    caller can page past rows whose cleanup already failed in this pass.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        now = time.time()
        conditions = [
            job_info_table.c.schedule_state ==
            ManagedJobScheduleState.DONE.value,
            spot_table.c.end_at.isnot(None),
            spot_table.c.end_at < (now - retention_seconds),
            spot_table.c.logs_cleaned_at.is_(None),
            # The local log file is set AFTER the task is finished,
            # add this condition to ensure the entire log file has
            # been written.
            spot_table.c.local_log_file.isnot(None),
        ]
        if exclude_tasks:
            conditions.append(
                sqlalchemy.tuple_(spot_table.c.spot_job_id,
                                  spot_table.c.task_id).notin_(
                                      list(exclude_tasks)))
        result = session.execute(
            sqlalchemy.select(
                spot_table.c.spot_job_id,
                spot_table.c.task_id,
                spot_table.c.local_log_file,
            ).select_from(
                spot_table.join(
                    job_info_table,
                    spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
                )).where(sqlalchemy.and_(*conditions)).limit(batch_size))
        rows = result.fetchall()
        return [{
            'job_id': row[0],
            'task_id': row[1],
            'local_log_file': row[2]
        } for row in rows]


def get_controller_logs_to_clean(
        retention_seconds: int,
        batch_size: int,
        exclude_job_ids: Collection[int] | None = None) -> list[dict[str, Any]]:
    """Get the controller logs to clean.

    The controller logs will only cleaned when:
    - the job schedule state is DONE
    - AND the end time of the latest task is older than the retention period

    Jobs in `exclude_job_ids` are skipped so a caller can page past rows
    whose cleanup already failed in this pass.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        now = time.time()
        conditions = [
            job_info_table.c.schedule_state ==
            ManagedJobScheduleState.DONE.value,
            spot_table.c.local_log_file.isnot(None),
            job_info_table.c.controller_logs_cleaned_at.is_(None),
        ]
        if exclude_job_ids:
            conditions.append(
                job_info_table.c.spot_job_id.notin_(list(exclude_job_ids)))
        result = session.execute(
            sqlalchemy.select(job_info_table.c.spot_job_id,).select_from(
                job_info_table.join(
                    spot_table,
                    job_info_table.c.spot_job_id == spot_table.c.spot_job_id,
                )).where(sqlalchemy.and_(*conditions)).group_by(
                    job_info_table.c.spot_job_id,
                    job_info_table.c.current_cluster_name,
                ).having(sqlalchemy.func.max(
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
