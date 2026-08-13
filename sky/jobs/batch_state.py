"""Persistence repository for Sky Batch coordinator state."""
import datetime
import time
from typing import Any

import sqlalchemy
from sqlalchemy import orm

from sky import sky_logging
from sky.jobs import controller_fencing
from sky.jobs import state_storage
from sky.jobs.state_schema import batch_state_table
from sky.jobs.state_schema import batch_worker_table
from sky.jobs.state_schema import job_events_table
from sky.jobs.state_schema import job_info_table
from sky.jobs.state_schema import spot_table
from sky.jobs.status_types import BatchLifecycleTransition
from sky.jobs.status_types import ManagedJobStatus
from sky.utils.db import db_utils
from sky.utils.db import retries as db_retries

logger = sky_logging.init_logger('sky.jobs.state')

_db_manager = state_storage.db_manager


def _lock_controller_job_attempt(session: orm.Session, job_id: int) -> bool:
    """Serialize a Batch mutation with the exact ControllerManager attempt."""
    try:
        identity = controller_fencing.get_current_slot_identity()
        if identity is not None:
            controller_fencing.lock_current_job_attempt(session, job_id,
                                                        identity)
            return True
        owner = controller_fencing.get_current_owner()
        if owner is not None:
            controller_fencing.lock_current_owner(session, owner)
        return True
    except controller_fencing.ControllerLeadershipLostError:
        return False


def _supports_update_returning(engine: sqlalchemy.engine.Engine) -> bool:
    """Whether UPDATE ... RETURNING is supported on the active dialect."""
    return bool(getattr(engine.dialect, 'update_returning', False))


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
    if not _lock_controller_job_attempt(session, job_id):
        return False
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
        if not _lock_controller_job_attempt(session, job_id):
            session.rollback()
            raise controller_fencing.ControllerLeadershipLostError(
                f'Managed job {job_id} changed controller attempts.')
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
    """Durably attach the exact external job ID to a launch intent.

    Like the request-ID receipt above, this is a monotonic fact about an
    already-authorized launch.  It remains writable after controller takeover
    so the successor does not lose the only exact cleanup identity.
    """
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
def get_batch_worker_records(job_id: int,
                             owner_token: str | None = None
                            ) -> list[dict[str, Any]]:
    """Return durable worker launch generations for a Batch job."""
    engine = _db_manager.get_engine()
    predicates = [batch_worker_table.c.job_id == job_id]
    if owner_token is not None:
        predicates.append(batch_worker_table.c.coordinator_token == owner_token)
    with orm.Session(engine) as session:
        rows = session.execute(
            sqlalchemy.select(batch_worker_table).where(
                sqlalchemy.and_(*predicates)).order_by(
                    batch_worker_table.c.coordinator_token,
                    batch_worker_table.c.worker_cluster)).mappings().all()
        return [dict(row) for row in rows]


@db_retries.retry
def remove_batch_worker_record(job_id: int,
                               owner_token: str,
                               worker_cluster: str,
                               worker_job_id: int | None = None) -> bool:
    """Forget a launch only after its exact external job was cleaned up.

    Retirement is a receipt for an already-completed exact cleanup, not
    authority to begin another provider effect.  The immutable coordinator
    token, cluster, and optional external job ID are therefore the fence; an
    attempt rotation must not strand a proven-clean record.
    """
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
                timestamp=datetime.datetime.now(datetime.timezone.utc)))
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
                timestamp=datetime.datetime.now(datetime.timezone.utc)))
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
