"""The database for managed jobs status."""
# TODO(zhwu): maybe use file based status instead of database, so
# that we can easily switch to a s3-based storage.
from collections.abc import Awaitable
from collections.abc import Callable
import dataclasses
import enum
import json
import time
from typing import Any, cast, TypeAlias

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql  # pylint: disable=unused-import
from sqlalchemy.dialects import sqlite  # pylint: disable=unused-import
from sqlalchemy.ext import asyncio as sql_async

from sky import exceptions
from sky import sky_logging
from sky.adaptors import common as adaptors_common
from sky.jobs import batch_state
from sky.jobs import controller_fencing
from sky.jobs import state_api_access_tokens
from sky.jobs import state_events
from sky.jobs import state_file_mount_blobs
from sky.jobs import state_job_registration
from sky.jobs import state_log_cleanup
from sky.jobs import state_pool_execution
from sky.jobs import state_pool_queries
from sky.jobs import state_queries
from sky.jobs import state_schema
from sky.jobs import state_storage
from sky.jobs import state_task_lookups
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

# Preserve the historical dialect helper exports used by state tests and
# external debugging code. Registration implementations live in
# state_job_registration.

# Separate callback types for sync and async contexts
SyncCallbackType = Callable[[str], None]
AsyncCallbackType = Callable[[str], Awaitable[Any]]
CallbackType = SyncCallbackType | AsyncCallbackType

logger = sky_logging.init_logger(__name__)
api_requests = adaptors_common.LazyImport('sky.server.requests.requests')

_DB_RETRY_TIMES = 30

# Preserve the historical schema export used by migrations and callers.
api_access_token_table = state_schema.api_access_token_table
_TERMINAL_IDENTITY_QUERY_BATCH_SIZE = 250

ControllerLeadershipLostError = (
    controller_fencing.ControllerLeadershipLostError)
ControllerSlotIdentity: TypeAlias = controller_fencing.ControllerSlotIdentity


@dataclasses.dataclass(frozen=True)
class StaleControllerRequestQuiescencePlan:
    """Exact request families plus request-free pre-slot jobs to adopt."""

    exact_identities: tuple[ControllerSlotIdentity, ...]
    legacy_job_ids: tuple[int, ...]


class ControllerFailureDecision(enum.Enum):
    """Exact dead-controller terminalization outcome for one snapshot."""
    TERMINALIZED = 'terminalized'
    ALREADY_TERMINAL = 'already_terminal'
    STALE = 'stale'


def get_current_controller_owner() -> tuple[str, int] | None:
    """Return the runtime-published managed-job owner, if any."""
    return controller_fencing.get_current_owner()


def controller_owner_is_current(owner: tuple[str, int]) -> bool:
    """Prove the exact outer owner through its configured authority."""
    return controller_fencing.owner_is_current(owner)


def get_current_controller_slot_identity() -> ControllerSlotIdentity | None:
    """Return this disposable manager's complete durable fencing identity."""
    return controller_fencing.get_current_slot_identity()


def controller_job_attempt_is_current(job_id: int,
                                      identity: ControllerSlotIdentity |
                                      None = None) -> bool:
    """Prove that one target job belongs to one live exact slot attempt."""
    identity = identity or get_current_controller_slot_identity()
    if identity is None or not controller_owner_is_current(identity[:2]):
        return False
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        return session.execute(
            sqlalchemy.select(job_info_table.c.spot_job_id).where(
                sqlalchemy.and_(
                    controller_fencing.job_attempt_predicate(job_id, identity),
                    job_info_table.c.schedule_state.in_([
                        ManagedJobScheduleState.LAUNCHING.value,
                        ManagedJobScheduleState.ALIVE.value,
                        ManagedJobScheduleState.ALIVE_WAITING.value,
                        ManagedJobScheduleState.ALIVE_BACKOFF.value,
                    ]),
                    job_info_table.c.controller_slot_quiescing.is_(False),
                )).limit(1)).first() is not None


def begin_controller_request_quiescence(
    authority_owner: tuple[str, int],
    identity: ControllerSlotIdentity,
) -> list[int]:
    """Close nested-request admission for one exact slot attempt.

    SQLite request storage calls this while holding the shared cross-database
    authority file lock. PostgreSQL request storage performs the equivalent
    locks and mutation in its single database transaction.
    """
    if identity[:2] != authority_owner and not controller_owner_is_current(
            authority_owner):
        raise ControllerLeadershipLostError(
            'Managed-job quiescence authority is no longer current.')
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _lock_current_controller_owner(session, authority_owner)
        rows = session.execute(
            sqlalchemy.select(job_info_table.c.spot_job_id).where(
                job_info_table.c.controller_instance_id == identity[0],
                job_info_table.c.controller_generation == identity[1],
                job_info_table.c.controller_slot_id == identity[2],
                job_info_table.c.controller_slot_attempt == identity[3],
            ).order_by(job_info_table.c.spot_job_id)).all()
        job_ids = [int(row.spot_job_id) for row in rows]
        if job_ids:
            result = session.execute(
                sqlalchemy.update(job_info_table).where(
                    job_info_table.c.spot_job_id.in_(job_ids),
                    job_info_table.c.controller_instance_id == identity[0],
                    job_info_table.c.controller_generation == identity[1],
                    job_info_table.c.controller_slot_id == identity[2],
                    job_info_table.c.controller_slot_attempt == identity[3],
                ).values(controller_slot_quiescing=True))
            if result.rowcount != len(job_ids):
                session.rollback()
                raise ControllerLeadershipLostError(
                    'Managed-job ownership changed while closing nested '
                    'request admission.')
        session.commit()
        return job_ids


def begin_stale_controller_request_quiescence(
    current_owner: tuple[str, int],) -> StaleControllerRequestQuiescencePlan:
    """Close admission on every stale schedulable job under a new owner."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _lock_current_controller_owner(session, current_owner)
        rows = session.execute(
            sqlalchemy.select(job_info_table).where(
                job_info_table.c.schedule_state.is_not(None),
                job_info_table.c.schedule_state.not_in([
                    ManagedJobScheduleState.INACTIVE.value,
                    ManagedJobScheduleState.DONE.value,
                ]),
                sqlalchemy.or_(
                    job_info_table.c.controller_instance_id.is_(None),
                    job_info_table.c.controller_generation.is_(None),
                    job_info_table.c.controller_slot_id.is_(None),
                    job_info_table.c.controller_slot_attempt.is_(None),
                    sqlalchemy.not_(
                        _controller_owner_matches_columns(current_owner)),
                )).order_by(job_info_table.c.spot_job_id)).mappings().all()
        job_ids = [int(row['spot_job_id']) for row in rows]
        identities: set[ControllerSlotIdentity] = set()
        legacy_job_ids: list[int] = []
        for row in rows:
            try:
                identity = controller_fencing.persisted_job_attempt_identity(
                    row, current_owner)
            except ValueError as e:
                session.rollback()
                raise ControllerLeadershipLostError(
                    f'Managed job {row["spot_job_id"]} has unsafe prior '
                    'controller identity.') from e
            if identity is None:
                legacy_job_ids.append(int(row['spot_job_id']))
            else:
                identities.add(identity)
        if job_ids:
            session.execute(
                sqlalchemy.update(job_info_table).where(
                    job_info_table.c.spot_job_id.in_(job_ids)).values(
                        controller_slot_quiescing=True))
        session.commit()
        return StaleControllerRequestQuiescencePlan(
            exact_identities=tuple(sorted(identities)),
            legacy_job_ids=tuple(sorted(legacy_job_ids)))


def _controller_owner_values(
    owner: tuple[str, int] | None,) -> dict[sqlalchemy.Column, Any]:
    if owner is None:
        return {
            job_info_table.c.controller_instance_id: None,
            job_info_table.c.controller_generation: None,
            job_info_table.c.controller_slot_id: None,
            job_info_table.c.controller_slot_attempt: None,
            job_info_table.c.controller_slot_quiescing: False,
        }
    instance_id, generation = owner
    values = {
        job_info_table.c.controller_instance_id: instance_id,
        job_info_table.c.controller_generation: generation,
    }
    current_slot = get_current_controller_slot_identity()
    if current_slot is not None:
        if current_slot[:2] != owner:
            raise ControllerLeadershipLostError(
                'Managed-job slot owner does not match outer generation.')
        values.update({
            job_info_table.c.controller_slot_id: current_slot[2],
            job_info_table.c.controller_slot_attempt: current_slot[3],
            job_info_table.c.controller_slot_quiescing: False,
        })
    return values


def _controller_owner_matches_columns(
    owner: tuple[str, int],) -> sqlalchemy.ColumnElement[bool]:
    return controller_fencing.owner_columns_predicate(owner)


async def _lock_current_controller_owner_async(session: sql_async.AsyncSession,
                                               owner: tuple[str, int]) -> None:
    await controller_fencing.lock_current_owner_async(session, owner)


def _lock_current_controller_owner(session: orm.Session,
                                   owner: tuple[str, int]) -> None:
    controller_fencing.lock_current_owner(session, owner)


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

# Keep the historical task-filtered lookup facade as direct aliases.
TaskLogStreamLookup = state_task_lookups.TaskLogStreamLookup
TaskWaitStatusLookup = state_task_lookups.TaskWaitStatusLookup
get_task_wait_status_lookup = (state_task_lookups.get_task_wait_status_lookup)
get_task_wait_status_lookup_by_name = (
    state_task_lookups.get_task_wait_status_lookup_by_name)
get_task_log_stream_lookup = state_task_lookups.get_task_log_stream_lookup
get_task_log_stream_lookup_by_name = (
    state_task_lookups.get_task_log_stream_lookup_by_name)

# Keep the historical log-cleanup metadata facade as direct aliases.
get_task_logs_to_clean = state_log_cleanup.get_task_logs_to_clean
get_controller_logs_to_clean = state_log_cleanup.get_controller_logs_to_clean
set_task_logs_cleaned = state_log_cleanup.set_task_logs_cleaned
set_controller_logs_cleaned = state_log_cleanup.set_controller_logs_cleaned

# Keep the historical pool-query facade as direct aliases.
get_pending_jobs_count_by_pool = (
    state_pool_queries.get_pending_jobs_count_by_pool)
get_nonterminal_job_ids_by_pool = (
    state_pool_queries.get_nonterminal_job_ids_by_pool)
get_nonterminal_job_counts_by_pool = (
    state_pool_queries.get_nonterminal_job_counts_by_pool)
get_nonterminal_job_status_counts_by_pool = (
    state_pool_queries.get_nonterminal_job_status_counts_by_pool)
get_nonterminal_job_ids_by_pool_grouped = (
    state_pool_queries.get_nonterminal_job_ids_by_pool_grouped)
# pylint: disable=protected-access
_is_any_of_or_ordered = state_pool_queries._is_any_of_or_ordered
_parse_job_full_resources = state_pool_queries._parse_job_full_resources
_ranked_nonterminal_job_resources = (
    state_pool_queries._ranked_nonterminal_job_resources)
# pylint: enable=protected-access
get_pool_worker_used_resources = (
    state_pool_queries.get_pool_worker_used_resources)
get_pool_worker_used_resources_by_cluster = (
    state_pool_queries.get_pool_worker_used_resources_by_cluster)


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


def _lock_controller_job_write(session: orm.Session,
                               job_id: int) -> ControllerSlotIdentity | None:
    """Fence a sync controller mutation to its exact target job attempt."""
    owner = get_current_controller_owner()
    if owner is None:
        return None
    identity = get_current_controller_slot_identity()
    if identity is None:
        _lock_current_controller_owner(session, owner)
        return None
    return controller_fencing.lock_current_job_attempt(session, job_id,
                                                       identity)


async def _lock_controller_job_write_async(
        session: sql_async.AsyncSession,
        job_id: int) -> ControllerSlotIdentity | None:
    """Fence an async controller mutation to its exact target job attempt."""
    owner = get_current_controller_owner()
    if owner is None:
        return None
    identity = get_current_controller_slot_identity()
    if identity is None:
        await _lock_current_controller_owner_async(session, owner)
        return None
    return await controller_fencing.lock_current_job_attempt_async(
        session, job_id, identity)


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
    event: tuple['ManagedJobStatus', str, str | None] | None = None,
) -> None:
    """Run a one-row task status update with commit-lost-safe retry."""
    prior_update_matched = False

    async def _op(attempt: int) -> None:
        nonlocal prior_update_matched
        engine = await _db_manager.get_async_engine()
        async with sql_async.AsyncSession(engine) as session:
            await _lock_controller_job_write_async(session, job_id)
            count = await update(session)
            if count == 1:
                prior_update_matched = True
                if event is not None:
                    status, reason, code = event
                    await session.execute(
                        state_events.job_event_insert_statement(job_id,
                                                                task_id,
                                                                status,
                                                                reason,
                                                                code=code))
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
set_job_info_without_job_id = (
    state_job_registration.set_job_info_without_job_id)


def set_pending(
    job_id: int,
    task_id: int,
    task_name: str,
    resources_str: str,
    metadata: str,
    is_primary_in_job_group: bool | None = None,
):
    """Set the task to pending state."""
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
        session.execute(
            state_events.job_event_insert_statement(job_id, task_id,
                                                    ManagedJobStatus.PENDING,
                                                    'Job submitted to queue'))
        session.commit()


async def set_backoff_pending_async(job_id: int, task_id: int):
    """Set the task to PENDING state if it is in backoff.

    This should only be used to transition from STARTING or RECOVERING back to
    PENDING.
    """

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

    await _retry_task_status_update(job_id,
                                    task_id,
                                    ManagedJobStatus.PENDING,
                                    _op,
                                    'Failed to set the task back to pending.',
                                    event=(ManagedJobStatus.PENDING,
                                           'Job is in backoff', None))
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

    async def _op(session: sql_async.AsyncSession) -> int:
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.task_id == task_id,
                    spot_table.c.end_at.is_(None),
                )).values({spot_table.c.status: target_status.value}))
        logger.debug(f'back to {target_status}')
        return result.rowcount

    await _retry_task_status_update(
        job_id,
        task_id,
        target_status,
        _op,
        f'Failed to set the task back to {target_status}.',
        event=(target_status, 'Job is restarting', None))
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
        _lock_controller_job_write(session, job_id)
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
        session.execute(
            state_events.job_event_insert_statement(job_id, None,
                                                    ManagedJobStatus.CANCELLED,
                                                    'Job has been cancelled'))
        session.commit()
    return True


@db_retries.retry
def set_local_log_file(job_id: int, task_id: int | None, local_log_file: str):
    """Set the local log file for a job."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _lock_controller_job_write(session, job_id)
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


@db_retries.retry
def get_all_task_ids_statuses(
        job_id: int) -> list[tuple[int, ManagedJobStatus]]:
    """Return all task statuses for one job in task_id order."""
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
    # Reuse the gateway's duplicate-collapse policy while keeping this legacy
    # projection free of task-count and routing work.
    matching_task = state_task_lookups._preferred_log_task(  # pylint: disable=protected-access
        job_id, task_id=task_id)
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                matching_task.c.task_id,
                matching_task.c.task_name,
                matching_task.c.status,
                matching_task.c.local_log_file,
                matching_task.c.logs_cleaned_at,
            )).fetchone()
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
    """Return controller process records for the requested jobs.

    Dedupes repeated ids and chunks large batches so scheduler scans and
    multi-job submissions do not depend on one oversized ``IN (...)`` query.
    """
    if not job_ids:
        return {}

    unique_job_ids = list(dict.fromkeys(job_ids))
    engine = _db_manager.get_engine()
    records: dict[int, ControllerPidRecord] = {}
    with orm.Session(engine) as session:
        for start in range(0, len(unique_job_ids), _STATUS_CHECK_JOB_ID_CHUNK):
            chunk = unique_job_ids[start:start + _STATUS_CHECK_JOB_ID_CHUNK]
            rows = session.execute(
                sqlalchemy.select(
                    job_info_table.c.spot_job_id,
                    job_info_table.c.controller_pid,
                    job_info_table.c.controller_pid_started_at).where(
                        job_info_table.c.spot_job_id.in_(chunk))).fetchall()
            for job_id, pid, started_at in rows:
                if pid is None:
                    continue
                if pid < 0:
                    # Between #7051 and #7847, the controller pid was
                    # negative to indicate a controller process that can
                    # handle multiple jobs.
                    pid = -pid
                records[job_id] = ControllerPidRecord(pid=pid,
                                                      started_at=started_at)
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
        job_info_table.c.controller_slot_id,
        job_info_table.c.controller_slot_attempt,
        job_info_table.c.controller_slot_quiescing,
        job_info_table.c.pool,
        job_info_table.c.workspace,
    ).select_from(from_clause)


def _spot_job_info_outerjoin():
    return spot_table.outerjoin(
        job_info_table,
        spot_table.c.spot_job_id == job_info_table.c.spot_job_id)


def _status_check_summary_select(from_clause) -> 'sqlalchemy.Select':
    """The per-job projection shared by refresh/cancel summary snapshots."""
    return sqlalchemy.select(
        from_clause.c.spot_job_id,
        from_clause.c.task_id,
        from_clause.c.status,
        from_clause.c.nonterminal_task_count,
        job_info_table.c.schedule_state,
        job_info_table.c.controller_pid,
        job_info_table.c.controller_pid_started_at,
        job_info_table.c.controller_instance_id,
        job_info_table.c.controller_generation,
        job_info_table.c.controller_slot_id,
        job_info_table.c.controller_slot_attempt,
        job_info_table.c.controller_slot_quiescing,
        job_info_table.c.pool,
        job_info_table.c.workspace,
    ).select_from(
        from_clause.outerjoin(
            job_info_table,
            from_clause.c.spot_job_id == job_info_table.c.spot_job_id))


def _collect_status_check_summary(
    job_ids: list[int] | None,
    fetch_chunk: Callable[[list[int] | None], list[Any]],
) -> dict[int, dict[str, Any]]:
    """Chunk ``job_ids`` and merge one summary row per job."""
    result: dict[int, dict[str, Any]] = {}
    if job_ids is None:
        _merge_jobs_status_check_summary_rows(result, fetch_chunk(None))
        return result
    unique_job_ids = list(dict.fromkeys(job_ids))
    for start in range(0, len(unique_job_ids), _STATUS_CHECK_JOB_ID_CHUNK):
        chunk = unique_job_ids[start:start + _STATUS_CHECK_JOB_ID_CHUNK]
        _merge_jobs_status_check_summary_rows(result, fetch_chunk(chunk))
    return {
        job_id: result[job_id] for job_id in unique_job_ids if job_id in result
    }


def _merge_jobs_status_check_summary_rows(result: dict[int, dict[str, Any]],
                                          rows: list[Any]) -> None:
    """Decode one-row-per-job refresh/cancel summaries."""
    for row in rows:
        mapping = row._mapping  # pylint: disable=protected-access
        schedule_state = mapping['schedule_state']
        if schedule_state is None:
            continue
        latest_status = ManagedJobStatus(mapping['status'])
        result[mapping['spot_job_id']] = {
            'schedule_state': ManagedJobScheduleState(schedule_state),
            'controller_pid': mapping['controller_pid'],
            'controller_pid_started_at': mapping['controller_pid_started_at'],
            'controller_instance_id': mapping['controller_instance_id'],
            'controller_generation': mapping['controller_generation'],
            'controller_slot_id': mapping['controller_slot_id'],
            'controller_slot_attempt': mapping['controller_slot_attempt'],
            'controller_slot_quiescing': mapping['controller_slot_quiescing'],
            'pool': mapping['pool'],
            'workspace': mapping['workspace'],
            '_latest_task_id': mapping['task_id'],
            '_latest_task_status': latest_status,
            '_latest_task_has_nonterminal': int(
                mapping['nonterminal_task_count'] or 0) > 0,
            'all_tasks_terminal': int(mapping['nonterminal_task_count'] or 0) ==
                                  0,
        }


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
    """Decode slim status-check rows into the per-job refresh snapshot.

    Each job is staged atomically. A malformed lifecycle enum excludes that
    whole job, rather than one task or the whole batch, so callers cannot act
    on a partial DAG and one legacy row cannot suppress unrelated work.
    """
    staged: dict[int, dict[str, Any]] = {}
    malformed_job_ids: set[int] = set()
    for row in rows:
        mapping = row._mapping  # pylint: disable=protected-access
        raw_schedule_state = mapping['schedule_state']
        workspace = mapping['workspace']
        # Legacy single-job controllers have a NULL schedule_state. Fence them
        # out here instead of raising while decoding a NULL enum; the same
        # rows are already fenced out of get_jobs_to_check_status_info by
        # _get_jobs_to_check_status_condition.
        #
        # A NULL workspace is deliberately NOT fenced here. This decode is
        # shared by the dead-controller sweep and the DONE cluster-leak
        # repair, and both must keep seeing legacy rows: a swept job that
        # disappears is never terminalized when its controller dies, and a
        # repair that cannot see legacy rows leaks the cluster it exists to
        # reclaim. Cancellation routing does require a workspace, and
        # get_job_cancellation_states_from_status_check_info applies that
        # narrowing itself.
        if raw_schedule_state is None:
            continue
        job_id = mapping['spot_job_id']
        if job_id in malformed_job_ids:
            continue
        raw_status = mapping['status']
        try:
            schedule_state = ManagedJobScheduleState(raw_schedule_state)
            status = ManagedJobStatus(raw_status)
        except (TypeError, ValueError) as e:
            staged.pop(job_id, None)
            malformed_job_ids.add(job_id)
            logger.error(
                f'Excluding managed job {job_id} from the shared status '
                'snapshot because one lifecycle row is malformed '
                f'(schedule_state={raw_schedule_state!r}, '
                f'status={raw_status!r}): {common_utils.format_exception(e)}')
            continue
        info = staged.get(job_id)
        # WARNING: Keep this decode (enum conversion + job_name fallback)
        # in sync with get_managed_job_tasks.
        if info is None:
            info = {
                'schedule_state': schedule_state,
                'controller_pid': mapping['controller_pid'],
                'controller_pid_started_at':
                    mapping['controller_pid_started_at'],
                'controller_instance_id': mapping['controller_instance_id'],
                'controller_generation': mapping['controller_generation'],
                'controller_slot_id': mapping['controller_slot_id'],
                'controller_slot_attempt': mapping['controller_slot_attempt'],
                'controller_slot_quiescing':
                    mapping['controller_slot_quiescing'],
                'pool': mapping['pool'],
                'workspace': workspace,
                '_latest_task_id': None,
                '_latest_task_status': None,
                '_latest_task_has_nonterminal': False,
                'tasks': [],
            }
            staged[job_id] = info
        job_name = mapping['job_info_name']
        if job_name is None:
            job_name = mapping['task_name']
        _merge_latest_task_status(info, mapping['task_id'], status)
        info['tasks'].append({
            'task_id': mapping['task_id'],
            'status': status,
            'job_name': job_name,
            'task_name': mapping['task_name'],
            'submitted_at': mapping['submitted_at'],
            'start_at': mapping['start_at'],
            'last_recovered_at': mapping['last_recovered_at'],
        })
    for job_id in malformed_job_ids:
        result.pop(job_id, None)
    result.update(staged)


def _merge_latest_task_status(info: dict[str, Any], task_id: int,
                              status: ManagedJobStatus) -> None:
    """Cache the cancellation-driving latest-task status during decode.

    This mirrors ``_latest_task_status_query`` exactly so callers that already
    have the shared snapshot do not need to rescan tasks or accept different
    duplicate-row resolution from the dedicated cancellation query.
    """
    latest_task_id = cast(int | None, info['_latest_task_id'])
    latest_status = cast(ManagedJobStatus | None, info['_latest_task_status'])
    if info['_latest_task_has_nonterminal']:
        if task_id != latest_task_id or status.is_terminal():
            return
        if latest_status is None or status.value < latest_status.value:
            info['_latest_task_status'] = status
        return

    if status.is_terminal():
        if latest_task_id is None or task_id > latest_task_id:
            info['_latest_task_id'] = task_id
            info['_latest_task_status'] = status
            return
        if (task_id == latest_task_id and latest_status is not None and
                status.value > latest_status.value):
            info['_latest_task_status'] = status
        return

    info['_latest_task_id'] = task_id
    info['_latest_task_status'] = status
    info['_latest_task_has_nonterminal'] = True


def _get_latest_task_status_from_status_check_info(
        info: dict[str, Any]) -> ManagedJobStatus | None:
    """Return the cancellation-driving status from one shared snapshot.

    ``get_jobs_status_check_info()`` populates a cached latest-task status
    during decode. Keep using that O(1) fast path in production. When callers
    provide only the historical public ``tasks`` shape, derive the same status
    from the already-fetched task rows instead of treating the job as missing.
    """
    cached_status = info.get('_latest_task_status')
    if cached_status is not None:
        return cast(ManagedJobStatus, cached_status)

    tasks = cast(list[dict[str, Any]] | None, info.get('tasks'))
    if not tasks:
        return None

    latest_task_info = {
        '_latest_task_id': None,
        '_latest_task_status': None,
        '_latest_task_has_nonterminal': False,
    }
    for task in tasks:
        status = cast(ManagedJobStatus, task['status'])
        _merge_latest_task_status(latest_task_info, task['task_id'], status)

    latest_status = cast(ManagedJobStatus | None,
                         latest_task_info['_latest_task_status'])
    if latest_status is not None:
        info['_latest_task_id'] = latest_task_info['_latest_task_id']
        info['_latest_task_status'] = latest_status
        info['_latest_task_has_nonterminal'] = latest_task_info[
            '_latest_task_has_nonterminal']
    return latest_status


def _status_check_summary_query(
    chunk: list[int] | None,
    *,
    filter_to_jobs_to_check: bool,
) -> 'sqlalchemy.Select':
    """Build the aggregated one-row-per-job status summary query."""
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    jobs = sqlalchemy.select(
        spot_table.c.spot_job_id.label('spot_job_id')).select_from(
            _spot_job_info_outerjoin())
    if filter_to_jobs_to_check:
        jobs = jobs.where(_get_jobs_to_check_status_condition(chunk))
    elif chunk is not None:
        jobs = jobs.where(spot_table.c.spot_job_id.in_(chunk))
    jobs_to_read = jobs.distinct().subquery()

    latest_task = _latest_task_status_query_from_scope(
        _latest_task_ids_subquery_from_scope(jobs_to_read,
                                             terminal_status_values),
        terminal_status_values).subquery()
    task_counts = sqlalchemy.select(
        jobs_to_read.c.spot_job_id,
        sqlalchemy.func.sum(
            sqlalchemy.case(
                (~spot_table.c.status.in_(terminal_status_values), 1),
                else_=0)).label('nonterminal_task_count'),
    ).select_from(
        jobs_to_read.join(
            spot_table,
            spot_table.c.spot_job_id == jobs_to_read.c.spot_job_id)).group_by(
                jobs_to_read.c.spot_job_id).subquery()
    summary = sqlalchemy.select(
        latest_task.c.spot_job_id,
        latest_task.c.task_id,
        latest_task.c.status,
        task_counts.c.nonterminal_task_count,
    ).select_from(
        latest_task.join(
            task_counts,
            latest_task.c.spot_job_id == task_counts.c.spot_job_id)).subquery()
    return _status_check_summary_select(summary).order_by(
        summary.c.spot_job_id.desc())


def get_job_cancellation_states_from_status_check_info(
        jobs_info: dict[int, dict[str,
                                  Any]]) -> dict[int, JobCancellationState]:
    """Derive cancellation states from a shared status-check snapshot.

    Callers that already fetched ``get_jobs_status_check_info()`` can reuse the
    same latest-task / workspace snapshot for cancellation routing instead of
    paying an extra lifecycle read before refresh.
    """
    snapshots: dict[int, JobCancellationState] = {}
    for job_id, info in jobs_info.items():
        workspace = info.get('workspace')
        schedule_state = info.get('schedule_state')
        if workspace is None or schedule_state is None:
            continue
        status = _get_latest_task_status_from_status_check_info(info)
        if status is None:
            continue
        snapshots[job_id] = JobCancellationState(status=status,
                                                 workspace=workspace)
    return snapshots


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
            return list(session.execute(query).fetchall())

    return _collect_status_check_snapshot(job_ids, _fetch_chunk)


def get_jobs_to_check_status_summary(
        job_ids: list[int] | None = None) -> dict[int, dict[str, Any]]:
    """One-row-per-job refresh summary for controller liveness checks."""
    engine = _db_manager.get_engine()

    def _fetch_chunk(chunk: list[int] | None) -> list[Any]:
        query = _status_check_summary_query(chunk, filter_to_jobs_to_check=True)
        with orm.Session(engine) as session:
            return list(session.execute(query).fetchall())

    return _collect_status_check_summary(job_ids, _fetch_chunk)


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
    with ``tasks`` ordered by ``task_id``. Job ids with no task rows, a NULL
    schedule state, or any undecodable lifecycle enum are absent from the
    result. Malformed jobs are omitted atomically so valid peers in the same
    batch remain available while no caller can act on a partial task list.

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
            return list(session.execute(query).fetchall())

    return _collect_status_check_snapshot(job_ids, _fetch_chunk)


def get_jobs_status_check_summary(
        job_ids: list[int]) -> dict[int, dict[str, Any]]:
    """One-row-per-job lifecycle summary for explicit managed-job ids."""
    if not job_ids:
        return {}
    engine = _db_manager.get_engine()

    def _fetch_chunk(chunk: list[int] | None) -> list[Any]:
        assert chunk is not None
        query = _status_check_summary_query(chunk,
                                            filter_to_jobs_to_check=False)
        with orm.Session(engine) as session:
            return list(session.execute(query).fetchall())

    return _collect_status_check_summary(job_ids, _fetch_chunk)


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
                job_info_table.c.controller_slot_id,
                job_info_table.c.controller_slot_attempt,
                job_info_table.c.controller_slot_quiescing,
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
                job_info_table.c.controller_pid_started_at,
                job_info_table.c.controller_instance_id,
                job_info_table.c.controller_generation,
                job_info_table.c.controller_slot_id,
                job_info_table.c.controller_slot_attempt,
                job_info_table.c.controller_slot_quiescing)).fetchone()
    if row is None or row[0] is None:
        return None
    task_count = int(row[8] or 0)
    nonterminal_task_count = int(row[9] or 0)
    return {
        'schedule_state': ManagedJobScheduleState(row[0]),
        'controller_pid': row[1],
        'controller_pid_started_at': row[2],
        'controller_instance_id': row[3],
        'controller_generation': row[4],
        'controller_slot_id': row[5],
        'controller_slot_attempt': row[6],
        'controller_slot_quiescing': row[7],
        'all_tasks_terminal': task_count > 0 and nonterminal_task_count == 0,
    }


def _controller_snapshot_conditions(
    job_id: int,
    schedule_state: ManagedJobScheduleState,
    controller_pid: int | None,
    controller_pid_started_at: float | None,
    controller_instance_id: str | None,
    controller_generation: int | None,
    controller_slot_id: int | None,
    controller_slot_attempt: str | None,
    controller_slot_quiescing: bool | None,
) -> list[sqlalchemy.ColumnElement[bool]]:
    """Build the exact job-info snapshot used by destructive refresh writes."""
    return [
        job_info_table.c.spot_job_id == job_id,
        job_info_table.c.schedule_state == schedule_state.value,
        job_info_table.c.controller_pid == controller_pid,
        job_info_table.c.controller_pid_started_at == controller_pid_started_at,
        job_info_table.c.controller_instance_id == controller_instance_id,
        job_info_table.c.controller_generation == controller_generation,
        job_info_table.c.controller_slot_id == controller_slot_id,
        job_info_table.c.controller_slot_attempt == controller_slot_attempt,
        job_info_table.c.controller_slot_quiescing == controller_slot_quiescing,
    ]


def _locked_task_recheck_summary(
    session: orm.Session,
    job_id: int,
    terminal_status_values: list[str],
) -> tuple[int, int, str | None]:
    """Return one exact locked task summary for destructive refresh writes."""
    locked_rows = sqlalchemy.select(
        spot_table.c.task_id,
        spot_table.c.status,
        spot_table.c.failure_reason,
    ).where(spot_table.c.spot_job_id == job_id).with_for_update().subquery()
    row = session.execute(
        sqlalchemy.select(
            sqlalchemy.func.count(  # pylint: disable=not-callable
                locked_rows.c.task_id).label('task_count'),
            sqlalchemy.func.sum(
                sqlalchemy.case(
                    (~locked_rows.c.status.in_(terminal_status_values), 1),
                    else_=0)).label('nonterminal_task_count'),
            sqlalchemy.func.max(
                sqlalchemy.case((locked_rows.c.failure_reason.is_not(None),
                                 locked_rows.c.failure_reason),
                                else_=None)).label('existing_failure_reason'),
        )).one()
    return (
        int(row.task_count or 0),
        int(row.nonterminal_task_count or 0),
        row.existing_failure_reason,
    )


def set_failed_controller_if_current_snapshot(
    job_id: int,
    *,
    schedule_state: ManagedJobScheduleState,
    controller_pid: int | None,
    controller_pid_started_at: float | None,
    controller_instance_id: str | None,
    controller_generation: int | None,
    controller_slot_id: int | None,
    controller_slot_attempt: str | None,
    controller_slot_quiescing: bool | None,
    failure_reason: str,
) -> ControllerFailureDecision:
    """Terminalize an exact dead-controller snapshot before provider cleanup.

    The job's schedule state intentionally remains non-DONE. If this process
    exits during cleanup, a replacement leader sees terminal tasks, retries the
    idempotent teardown, and completes the schedule state instead of recovering
    the workload.
    """
    owner = get_current_controller_owner()
    recorded_owner = (controller_instance_id, controller_generation)
    if owner is not None and recorded_owner != owner:
        return ControllerFailureDecision.STALE

    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        if owner is not None:
            _lock_current_controller_owner(session, owner)
        conditions = _controller_snapshot_conditions(
            job_id, schedule_state, controller_pid, controller_pid_started_at,
            controller_instance_id, controller_generation, controller_slot_id,
            controller_slot_attempt, controller_slot_quiescing)
        job_row = session.execute(
            sqlalchemy.select(job_info_table.c.spot_job_id).where(
                sqlalchemy.and_(*conditions)).with_for_update()).first()
        if job_row is None:
            session.rollback()
            return ControllerFailureDecision.STALE

        task_count, nonterminal_task_count, existing_reason = (
            _locked_task_recheck_summary(session, job_id,
                                         terminal_status_values))
        if task_count == 0:
            session.rollback()
            return ControllerFailureDecision.STALE
        if nonterminal_task_count == 0:
            session.rollback()
            return ControllerFailureDecision.ALREADY_TERMINAL

        persisted_reason = failure_reason
        if existing_reason:
            persisted_reason += f'. Previously: {existing_reason}'
        end_time = time.time()
        session.execute(
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
        return ControllerFailureDecision.TERMINALIZED


def get_job_cancellation_states(
        job_ids: list[int]) -> dict[int, JobCancellationState]:
    """Return slim, batched snapshots for managed-job cancellation.

    Cancellation reuses the same one-row-per-job summary snapshot as refresh,
    so both paths share latest-task resolution, duplicate-row collapse, legacy
    row filtering, and chunked query behavior.
    """
    return get_job_cancellation_states_from_status_check_info(
        get_jobs_status_check_summary(job_ids))


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


def _latest_task_ids_subquery_from_scope(
        job_scope: sqlalchemy.sql.Selectable,
        terminal_status_values: list[str]) -> sqlalchemy.sql.Selectable:
    """Select one latest-task identity for the given job-id scope."""
    return sqlalchemy.select(
        job_scope.c.spot_job_id.label('spot_job_id'),
        sqlalchemy.func.coalesce(  # pylint: disable=not-callable
            sqlalchemy.func.min(
                sqlalchemy.case(
                    (~spot_table.c.status.in_(terminal_status_values),
                     spot_table.c.task_id),
                    else_=None)),
            sqlalchemy.func.max(spot_table.c.task_id),
        ).label('task_id')).select_from(
            job_scope.join(
                spot_table,
                spot_table.c.spot_job_id == job_scope.c.spot_job_id)).group_by(
                    job_scope.c.spot_job_id).subquery()


def _latest_task_status_query(
        job_ids: list[int],
        terminal_status_values: list[str]) -> sqlalchemy.sql.Selectable:
    """Select the latest-task status row for each requested job."""
    latest_task_ids = _latest_task_ids_subquery(job_ids, terminal_status_values)
    return _latest_task_status_query_from_scope(latest_task_ids,
                                                terminal_status_values)


def _latest_task_status_query_from_scope(
        latest_task_ids: sqlalchemy.sql.Selectable,
        terminal_status_values: list[str]) -> sqlalchemy.sql.Selectable:
    """Select the latest-task status row for each requested job scope."""
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

get_active_file_mounts_blob_ids = (
    state_file_mount_blobs.get_active_file_mounts_blob_ids)


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
    """Return one task-specific status and routing snapshot for log following."""
    return get_task_log_stream_lookup(job_id, task_id).snapshot


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


get_pool_from_job_id = state_pool_execution.get_pool_from_job_id
get_pool_and_current_cluster_name = (
    state_pool_execution.get_pool_and_current_cluster_name)
get_pool_and_execution_from_job_id_async = (
    state_pool_execution.get_pool_and_execution_from_job_id_async)
set_current_cluster_name = state_pool_execution.set_current_cluster_name
set_job_infra = state_pool_execution.set_job_infra
update_job_full_resources = state_pool_execution.update_job_full_resources
set_job_id_on_pool_cluster_async = (
    state_pool_execution.set_job_id_on_pool_cluster_async)
get_pool_submit_info = state_pool_execution.get_pool_submit_info
get_pool_submit_info_async = state_pool_execution.get_pool_submit_info_async

set_api_access_token_ids = state_api_access_tokens.set_api_access_token_ids
get_releasable_api_access_token_id = (
    state_api_access_tokens.get_releasable_api_access_token_id)


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
            if get_current_controller_slot_identity() is not None:
                conditions.append(
                    job_info_table.c.controller_slot_quiescing.is_(False))
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


@db_retries.retry_async
async def get_waiting_job_async(
    pid: int,
    pid_started_at: float,
    controller_slot_id: int,
    controller_slot_attempt: str,
) -> dict[str, Any] | None:
    """Get the next job that should transition to LAUNCHING.

    Selects one WAITING job in the existing priority order and atomically
    transitions it to LAUNCHING.  A terminal task family is returned as
    cleanup-only work; ordinary nonterminal jobs retain the same claim path.

    Returns the job information if a job was successfully transitioned to
    LAUNCHING, or None if no suitable job was found.

    Backwards compatibility note: jobs submitted before #4485 will have no
    schedule_state and will be ignored by this SQL query.
    """
    owner = get_current_controller_owner()
    slot_identity = get_current_controller_slot_identity()
    if owner is None or slot_identity is None:
        raise ControllerLeadershipLostError(
            'Managed-job claims require a runtime-owned controller slot.')
    if (slot_identity[:2] != owner or slot_identity[2] != controller_slot_id or
            slot_identity[3] != controller_slot_attempt):
        raise ControllerLeadershipLostError(
            'Managed-job claim slot does not match the disposable manager.')
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

        terminal_status_values = [
            status.value for status in ManagedJobStatus.terminal_statuses()
        ]
        has_task = sqlalchemy.exists(
            sqlalchemy.select(spot_table.c.task_id).where(
                spot_table.c.spot_job_id == job_info_table.c.spot_job_id))
        has_nonterminal_task = sqlalchemy.exists(
            sqlalchemy.select(spot_table.c.task_id).where(
                spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
                spot_table.c.status.not_in(terminal_status_values)))
        cleanup_only_expr = sqlalchemy.and_(has_task, ~has_nonterminal_task)

        # Select the highest priority waiting job for update (locks the row).
        # Batch jobs are skipped when their pool already has an active batch
        # job; non-batch jobs (including regular pool jobs) are always eligible.
        select_query = sqlalchemy.select(
            job_info_table.c.spot_job_id,
            job_info_table.c.schedule_state,
            job_info_table.c.pool,
            cleanup_only_expr.label('cleanup_only'),
        ).where(
            sqlalchemy.and_(
                job_info_table.c.schedule_state.in_([
                    ManagedJobScheduleState.WAITING.value,
                ]),
                job_info_table.c.controller_slot_quiescing.is_(False),
                has_task,
                sqlalchemy.or_(
                    # Cleanup-only work does not execute a batch and must not
                    # be blocked by another batch using the same pool.
                    cleanup_only_expr,
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
        selected_cleanup_only = bool(waiting_job_row[3])

        # Lock the complete task-status family before deciding which manager
        # path owns this claim.  The job-info lock above serializes scheduler
        # claims; these row locks serialize the terminal classification with
        # exact-attempt task transitions.
        locked_task_statuses = (await session.execute(
            sqlalchemy.select(spot_table.c.status).where(
                spot_table.c.spot_job_id == job_id).order_by(
                    spot_table.c.task_id).with_for_update())).scalars().all()
        if not locked_task_statuses:
            await session.rollback()
            return None
        cleanup_only = all(
            status in terminal_status_values for status in locked_task_statuses)
        if cleanup_only != selected_cleanup_only:
            logger.debug(
                'Managed job %s terminal classification changed while its '
                'claim was locking; using the locked classification.', job_id)
        classification_condition = (~has_nonterminal_task
                                    if cleanup_only else has_nonterminal_task)
        pool_eligibility_condition = (
            sqlalchemy.true() if cleanup_only else sqlalchemy.or_(
                job_info_table.c.is_batch.isnot(True),
                ~job_info_table.c.pool.in_(busy_batch_pools_subq)))

        # Update the job state to LAUNCHING
        update_values = {
            job_info_table.c.schedule_state:
                ManagedJobScheduleState.LAUNCHING.value,
            job_info_table.c.controller_pid: pid,
            job_info_table.c.controller_pid_started_at: pid_started_at,
            job_info_table.c.controller_slot_id: controller_slot_id,
            job_info_table.c.controller_slot_attempt: controller_slot_attempt,
        }
        update_values.update(_controller_owner_values(owner))
        update_result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.spot_job_id == job_id,
                    job_info_table.c.schedule_state == current_state.value,
                    job_info_table.c.controller_slot_quiescing.is_(False),
                    has_task,
                    classification_condition,
                    pool_eligibility_condition,
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
            'cleanup_only': cleanup_only,
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


get_file_mounts_blob_id_async = (
    state_file_mount_blobs.get_file_mounts_blob_id_async)


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

    await _retry_task_status_update(job_id,
                                    task_id,
                                    ManagedJobStatus.STARTING,
                                    _op,
                                    'Failed to set the task to starting.',
                                    event=(ManagedJobStatus.STARTING,
                                           'Job is starting', None))
    await callback_func('SUBMITTED')
    await callback_func('STARTING')


async def set_started_async(job_id: int, task_id: int, start_time: float,
                            callback_func: AsyncCallbackType):
    """Set the task to started state."""
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

    await _retry_task_status_update(job_id,
                                    task_id,
                                    ManagedJobStatus.RUNNING,
                                    _op,
                                    'Failed to set the task to started.',
                                    event=(ManagedJobStatus.RUNNING,
                                           'Job has started', None))
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
        job_id,
        task_id,
        ManagedJobStatus.RECOVERING,
        _op, ('Failed to set the task to recovering with '
              f'force_transit_to_recovering={force_transit_to_recovering}.'),
        event=(ManagedJobStatus.RECOVERING, reason, code))
    await callback_func('RECOVERING')


async def set_recovered_async(job_id: int, task_id: int, recovered_time: float,
                              callback_func: AsyncCallbackType):
    """Set the task to recovered."""

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

    await _retry_task_status_update(job_id,
                                    task_id,
                                    ManagedJobStatus.RUNNING,
                                    _op,
                                    'Failed to set the task to recovered.',
                                    event=(ManagedJobStatus.RUNNING,
                                           'Job has recovered', None))
    logger.info('==== Recovered. ====')
    await callback_func('RECOVERED')


def set_winding_down(job_id: int, task_id: int) -> None:
    """Transition task from RUNNING to WINDING_DOWN (sync).

    Called by the batch coordinator (which runs in a thread) before
    merging per-batch output files.
    """
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _lock_controller_job_write(session, job_id)
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

    await _retry_task_status_update(job_id,
                                    task_id,
                                    ManagedJobStatus.SUCCEEDED,
                                    _op,
                                    'Failed to set the task to succeeded.',
                                    event=(ManagedJobStatus.SUCCEEDED,
                                           'Job has succeeded', None))
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
    assert failure_type.is_failed(), failure_type
    end_time = time.time() if end_time is None else end_time

    async def _op(session):
        await _lock_controller_job_write_async(session, job_id)
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
        if count > 0:
            await session.execute(
                state_events.job_event_insert_statement(
                    job_id, task_id, failure_type,
                    f'Job failed: {failure_reason}'))
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
            await _lock_controller_job_write_async(session, job_id)
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

    async def _op(session):
        await _lock_controller_job_write_async(session, job_id)
        result = await session.execute(
            sqlalchemy.update(spot_table).where(
                sqlalchemy.and_(
                    spot_table.c.spot_job_id == job_id,
                    spot_table.c.end_at.is_(None),
                )).values(
                    {spot_table.c.status: ManagedJobStatus.CANCELLING.value}))
        count = result.rowcount
        if count > 0:
            await session.execute(
                state_events.job_event_insert_statement(
                    job_id, None, ManagedJobStatus.CANCELLING,
                    'Job is cancelling'))
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

    async def _op(session):
        await _lock_controller_job_write_async(session, job_id)
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
        if count > 0:
            await session.execute(
                state_events.job_event_insert_statement(
                    job_id, None, ManagedJobStatus.CANCELLED,
                    'Job has been cancelled'))
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
        await _lock_controller_job_write_async(session, job_id)
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
            if get_current_controller_slot_identity() is not None:
                conditions.append(
                    job_info_table.c.controller_slot_quiescing.is_(False))
        result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(*conditions)).values({
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.DONE.value
                }))
        return result.rowcount

    await _retry_schedule_state_update(job_id, ManagedJobScheduleState.DONE,
                                       _op, idempotent)


async def scheduler_set_cleanup_done_async(job_id: int) -> None:
    """Finish one cleanup-only claim under its exact disposable attempt.

    Cleanup adoption never widens the ordinary DONE transition: the job must
    still be LAUNCHING under this process's exact slot attempt, admission must
    remain open, and every durable task must be terminal in the same
    transaction.
    """
    identity = get_current_controller_slot_identity()
    if identity is None:
        raise ControllerLeadershipLostError(
            'Managed-job cleanup finalization requires a runtime slot.')
    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    has_task = sqlalchemy.exists(
        sqlalchemy.select(spot_table.c.task_id).where(
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id))
    has_nonterminal_task = sqlalchemy.exists(
        sqlalchemy.select(spot_table.c.task_id).where(
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
            sqlalchemy.or_(spot_table.c.status.is_(None),
                           spot_table.c.status.not_in(terminal_status_values))))

    async def _op(session: sql_async.AsyncSession) -> int:
        await controller_fencing.lock_current_job_attempt_async(
            session, job_id, identity)
        result = await session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    controller_fencing.job_attempt_predicate(job_id, identity),
                    job_info_table.c.schedule_state ==
                    ManagedJobScheduleState.LAUNCHING.value,
                    job_info_table.c.controller_slot_quiescing.is_(False),
                    has_task,
                    ~has_nonterminal_task,
                )).values({
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.DONE.value,
                }))
        return result.rowcount

    try:
        await _retry_schedule_state_update(job_id, ManagedJobScheduleState.DONE,
                                           _op)
    except AssertionError as e:
        raise ControllerLeadershipLostError(
            f'Managed job {job_id} is no longer an exact cleanup-only claim '
            'for this slot attempt.') from e


# ==== needed for codegen ====
# functions have no use outside of codegen, remove at your own peril

set_job_info = state_job_registration.set_job_info


def reset_stale_jobs_for_current_controller() -> int:
    """Reset jobs owned by another outer controller generation.

    The leadership-row lock serializes this recovery write with generation
    advancement. INACTIVE jobs are excluded because request submission may
    still be populating their durable inputs; DONE jobs need no scheduler.
    """
    owner = get_current_controller_owner()
    if owner is None:
        return 0

    # This closes every old nested admission and waits for exact request
    # boundary receipts before any stale row can become WAITING.  The helper
    # also marks the matched rows quiescing, closing the create/reset gap.
    api_requests.quiesce_stale_managed_job_requests(owner)

    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _lock_current_controller_owner(session, owner)
        stale_owner = sqlalchemy.or_(
            job_info_table.c.controller_instance_id.is_(None),
            job_info_table.c.controller_generation.is_(None),
            job_info_table.c.controller_slot_id.is_(None),
            job_info_table.c.controller_slot_attempt.is_(None),
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
                    stale_owner,
                    job_info_table.c.controller_slot_quiescing.is_(True),
                )).values({
                    job_info_table.c.controller_pid: None,
                    job_info_table.c.controller_pid_started_at: None,
                    job_info_table.c.controller_instance_id: None,
                    job_info_table.c.controller_generation: None,
                    job_info_table.c.controller_slot_id: None,
                    job_info_table.c.controller_slot_attempt: None,
                    job_info_table.c.controller_slot_quiescing: False,
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.WAITING.value,
                }))
        session.commit()
        return result.rowcount


@db_retries.retry
def requeue_terminal_done_jobs_for_cleanup(job_ids: list[int]) -> int:
    """Move exact terminal DONE jobs back to cleanup-only admission.

    The caller has already associated these job IDs with current dedicated
    managed-job cluster rows. This compare-and-set repeats the authoritative
    task-state checks in the write transaction so a stale inventory snapshot
    cannot requeue workload execution. ``get_waiting_job_async`` classifies
    the resulting all-terminal WAITING rows as cleanup-only work.
    """
    unique_job_ids = list(dict.fromkeys(job_ids))
    if not unique_job_ids:
        return 0

    terminal_status_values = [
        status.value for status in ManagedJobStatus.terminal_statuses()
    ]
    has_task = sqlalchemy.exists(
        sqlalchemy.select(spot_table.c.task_id).where(
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id))
    has_nonterminal_task = sqlalchemy.exists(
        sqlalchemy.select(spot_table.c.task_id).where(
            spot_table.c.spot_job_id == job_info_table.c.spot_job_id,
            sqlalchemy.or_(spot_table.c.status.is_(None),
                           spot_table.c.status.not_in(terminal_status_values))))

    updated_count = 0
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        for offset in range(0, len(unique_job_ids), _STATUS_CHECK_JOB_ID_CHUNK):
            chunk = unique_job_ids[offset:offset + _STATUS_CHECK_JOB_ID_CHUNK]
            result = session.execute(
                sqlalchemy.update(job_info_table).where(
                    sqlalchemy.and_(
                        job_info_table.c.spot_job_id.in_(chunk),
                        job_info_table.c.schedule_state ==
                        ManagedJobScheduleState.DONE.value,
                        job_info_table.c.pool.is_(None),
                        has_task,
                        ~has_nonterminal_task,
                    )).values({
                        job_info_table.c.controller_pid: None,
                        job_info_table.c.controller_pid_started_at: None,
                        job_info_table.c.controller_instance_id: None,
                        job_info_table.c.controller_generation: None,
                        job_info_table.c.controller_slot_id: None,
                        job_info_table.c.controller_slot_attempt: None,
                        job_info_table.c.controller_slot_quiescing: False,
                        job_info_table.c.schedule_state:
                            ManagedJobScheduleState.WAITING.value,
                    }))
            updated_count += result.rowcount
        session.commit()
    return updated_count


def reset_job_for_recovery_if_stale(job_id: int, owner: tuple[str,
                                                              int]) -> bool:
    """Reset one stale-generation job without clobbering a fresh reclaim."""
    api_requests.quiesce_stale_managed_job_requests(owner)
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _lock_current_controller_owner(session, owner)
        stale_owner = sqlalchemy.or_(
            job_info_table.c.controller_instance_id.is_(None),
            job_info_table.c.controller_generation.is_(None),
            job_info_table.c.controller_slot_id.is_(None),
            job_info_table.c.controller_slot_attempt.is_(None),
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
                    stale_owner,
                    job_info_table.c.controller_slot_quiescing.is_(True),
                )).values({
                    job_info_table.c.controller_pid: None,
                    job_info_table.c.controller_pid_started_at: None,
                    job_info_table.c.controller_instance_id: None,
                    job_info_table.c.controller_generation: None,
                    job_info_table.c.controller_slot_id: None,
                    job_info_table.c.controller_slot_attempt: None,
                    job_info_table.c.controller_slot_quiescing: False,
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.WAITING.value,
                }))
        session.commit()
        return result.rowcount == 1


def reset_jobs_for_controller_slot(identity: ControllerSlotIdentity) -> int:
    """Reset unfinished lifecycle work owned by a proven-dead slot attempt.

    The local slot supervisor calls this only after its two-owner guardian has
    proved the complete process family absent and its exact nested requests
    have quiesced.  Nonterminal tasks resume ordinary execution; terminal tasks
    are claimed from the same WAITING queue as cleanup-only work.  DONE remains
    final and is never reset.  Locking the current outer generation makes the
    reset mutually exclusive with successor takeover; the complete tuple
    prevents a delayed cleanup from touching replacement-attempt work.
    """
    instance_id, generation, slot_id, attempt = identity
    owner = (instance_id, generation)
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        _lock_current_controller_owner(session, owner)
        result = session.execute(
            sqlalchemy.update(job_info_table).where(
                sqlalchemy.and_(
                    job_info_table.c.schedule_state.is_not(None),
                    job_info_table.c.schedule_state.not_in([
                        ManagedJobScheduleState.INACTIVE.value,
                        ManagedJobScheduleState.DONE.value,
                    ]),
                    job_info_table.c.controller_instance_id == instance_id,
                    job_info_table.c.controller_generation == generation,
                    job_info_table.c.controller_slot_id == slot_id,
                    job_info_table.c.controller_slot_attempt == attempt,
                    job_info_table.c.controller_slot_quiescing.is_(True),
                )).values({
                    job_info_table.c.controller_pid: None,
                    job_info_table.c.controller_pid_started_at: None,
                    job_info_table.c.controller_instance_id: None,
                    job_info_table.c.controller_generation: None,
                    job_info_table.c.controller_slot_id: None,
                    job_info_table.c.controller_slot_attempt: None,
                    job_info_table.c.controller_slot_quiescing: False,
                    job_info_table.c.schedule_state:
                        ManagedJobScheduleState.WAITING.value,
                }))
        session.commit()
        return result.rowcount


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
