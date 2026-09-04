"""Cycle-free fencing primitives for runtime-owned managed-job controllers."""

from collections.abc import Mapping
import os
from typing import Any
import uuid

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.ext import asyncio as sql_async

from sky.adaptors import common as adaptors_common
from sky.jobs import constants
from sky.jobs import state_schema
from sky.utils import controller_capability

request_postgres = adaptors_common.LazyImport('sky.server.requests.postgres')

ControllerOwner = tuple[str, int]
ControllerSlotIdentity = tuple[str, int, int, str]

POSTGRES_OWNER_MODE = 'postgres'
LOCAL_OWNER_MODE = 'local'
_OWNER_MODES = frozenset({POSTGRES_OWNER_MODE, LOCAL_OWNER_MODE})


class ControllerLeadershipLostError(RuntimeError):
    """The exact outer generation or disposable slot attempt is stale."""


def persisted_job_attempt_identity(
    values: Mapping[str, Any],
    current_owner: ControllerOwner,
) -> ControllerSlotIdentity | None:
    """Decode an exact attempt, or recognize one pre-slot legacy row.

    ``None`` is returned only for a shape an older schema could have written:
    no slot columns, plus either no outer owner or one complete older outer
    owner.  A row carrying the successor's outer identity without a slot is
    not a handoff artifact, so accepting it would let the current generation
    bypass exact-attempt admission.  Every other partial or malformed shape
    fails closed.
    """
    raw = (
        values.get('controller_instance_id'),
        values.get('controller_generation'),
        values.get('controller_slot_id'),
        values.get('controller_slot_attempt'),
    )
    instance_id, generation, slot_id, attempt = raw
    if all(value is not None for value in raw):
        if (not isinstance(instance_id, str) or isinstance(generation, bool) or
                not isinstance(generation, int) or generation <= 0 or
                isinstance(slot_id, bool) or not isinstance(slot_id, int) or
                slot_id < 0 or not isinstance(attempt, str)):
            raise ValueError('Managed-job slot identity is malformed.')
        try:
            canonical_instance_id = str(uuid.UUID(instance_id))
            canonical_attempt = str(uuid.UUID(attempt))
        except ValueError as e:
            raise ValueError('Managed-job slot identity is malformed.') from e
        if (canonical_instance_id != instance_id or
                canonical_attempt != attempt):
            raise ValueError('Managed-job slot identity is not canonical.')
        return canonical_instance_id, generation, slot_id, canonical_attempt

    # Revision 026 may have recorded the outer pair before revision 028 added
    # the slot pair.  Older rows may have neither pair.  No released schema
    # writes only one member of either pair.
    if slot_id is not None or attempt is not None:
        raise ValueError('Managed-job slot identity is partially populated.')
    if (instance_id is None) != (generation is None):
        raise ValueError('Managed-job outer identity is partially populated.')
    if instance_id is None:
        return None
    if (not isinstance(instance_id, str) or isinstance(generation, bool) or
            not isinstance(generation, int) or generation <= 0):
        raise ValueError('Legacy managed-job outer identity is malformed.')
    try:
        canonical_instance_id = str(uuid.UUID(instance_id))
    except ValueError as e:
        raise ValueError(
            'Legacy managed-job outer identity is malformed.') from e
    if canonical_instance_id != instance_id:
        raise ValueError('Legacy managed-job outer identity is not canonical.')
    if (canonical_instance_id, generation) == current_owner:
        raise ValueError('A pre-slot managed job requires a fresh successor '
                         'outer generation before adoption.')
    return None


def _read_process_start_time_ticks(pid: int) -> int:
    """Read through the same live-process proof as local file authority."""
    return controller_capability.read_live_process_start_time_ticks(pid)


def publish_owner(owner: ControllerOwner, *, mode: str) -> None:
    """Publish one immutable outer identity before any slot is forked."""
    instance_id, generation = owner
    if mode not in _OWNER_MODES:
        raise ValueError(f'Unsupported managed-job owner mode {mode!r}.')
    try:
        canonical_instance_id = str(uuid.UUID(instance_id))
    except ValueError as e:
        raise ValueError('Managed-job owner instance must be a UUID.') from e
    if generation <= 0:
        raise ValueError('Managed-job owner generation must be positive.')
    os.environ[constants.CONTROLLER_OWNER_MODE_ENV_VAR] = mode
    os.environ[constants.CONTROLLER_OWNER_INSTANCE_ID_ENV_VAR] = (
        canonical_instance_id)
    os.environ[constants.CONTROLLER_OWNER_GENERATION_ENV_VAR] = str(generation)
    if mode == LOCAL_OWNER_MODE:
        pid = os.getpid()
        os.environ[constants.CONTROLLER_OWNER_PID_ENV_VAR] = str(pid)
        os.environ[constants.CONTROLLER_OWNER_START_TICKS_ENV_VAR] = str(
            _read_process_start_time_ticks(pid))
    else:
        os.environ.pop(constants.CONTROLLER_OWNER_PID_ENV_VAR, None)
        os.environ.pop(constants.CONTROLLER_OWNER_START_TICKS_ENV_VAR, None)


def clear_owner() -> None:
    """Remove the process-local publication after every family has drained."""
    for name in (constants.CONTROLLER_OWNER_MODE_ENV_VAR,
                 constants.CONTROLLER_OWNER_INSTANCE_ID_ENV_VAR,
                 constants.CONTROLLER_OWNER_GENERATION_ENV_VAR,
                 constants.CONTROLLER_OWNER_PID_ENV_VAR,
                 constants.CONTROLLER_OWNER_START_TICKS_ENV_VAR):
        os.environ.pop(name, None)


def get_current_owner() -> ControllerOwner | None:
    """Decode the explicit runtime owner inherited by refresh and slot work."""
    mode = os.environ.get(constants.CONTROLLER_OWNER_MODE_ENV_VAR)
    instance_id = os.environ.get(constants.CONTROLLER_OWNER_INSTANCE_ID_ENV_VAR)
    generation = os.environ.get(constants.CONTROLLER_OWNER_GENERATION_ENV_VAR)
    if mode is None and instance_id is None and generation is None:
        return None
    if mode not in _OWNER_MODES or instance_id is None or generation is None:
        raise ControllerLeadershipLostError(
            'Managed-job outer owner identity is incomplete.')
    try:
        canonical_instance_id = str(uuid.UUID(instance_id))
        parsed_generation = int(generation)
    except (TypeError, ValueError) as e:
        raise ControllerLeadershipLostError(
            'Managed-job outer owner identity is malformed.') from e
    if canonical_instance_id != instance_id or parsed_generation <= 0:
        raise ControllerLeadershipLostError(
            'Managed-job outer owner identity is not canonical.')
    return canonical_instance_id, parsed_generation


def owner_mode() -> str | None:
    """Return the validated mode for the currently published owner."""
    owner = get_current_owner()
    if owner is None:
        return None
    mode = os.environ[constants.CONTROLLER_OWNER_MODE_ENV_VAR]
    assert mode in _OWNER_MODES, mode
    return mode


def owner_is_current(owner: ControllerOwner) -> bool:
    """Prove one outer owner through its canonical authority."""
    try:
        published_owner = get_current_owner()
        # API-only processes validate authenticated controller-origin metadata
        # but intentionally do not publish a local managed-job owner.  A
        # persisted origin can therefore be revalidated by either canonical
        # authority: the private same-host file + exact process birth, or the
        # shared PostgreSQL leadership row.
        if published_owner is None:
            if controller_capability.local_authority_owner_is_current(*owner):
                return True
            return request_postgres.controller_leadership_is_current(*owner)
        if published_owner != owner:
            return False
        mode = owner_mode()
        if mode == POSTGRES_OWNER_MODE:
            return request_postgres.controller_leadership_is_current(*owner)
        if mode != LOCAL_OWNER_MODE:
            return False
        pid = int(os.environ[constants.CONTROLLER_OWNER_PID_ENV_VAR])
        expected_ticks = int(
            os.environ[constants.CONTROLLER_OWNER_START_TICKS_ENV_VAR])
        return _read_process_start_time_ticks(pid) == expected_ticks
    except Exception:  # pylint: disable=broad-except
        return False


def get_current_slot_identity() -> ControllerSlotIdentity | None:
    """Return this disposable manager's complete immutable identity."""
    raw_slot_id = os.environ.get(constants.CONTROLLER_SLOT_ID_ENV_VAR)
    raw_attempt = os.environ.get(constants.CONTROLLER_SLOT_ATTEMPT_ENV_VAR)
    if raw_slot_id is None and raw_attempt is None:
        return None
    owner = get_current_owner()
    if owner is None or raw_slot_id is None or raw_attempt is None:
        raise ControllerLeadershipLostError(
            'Managed-job controller slot identity is incomplete.')
    try:
        slot_id = int(raw_slot_id)
        attempt = str(uuid.UUID(raw_attempt))
    except (TypeError, ValueError) as e:
        raise ControllerLeadershipLostError(
            'Managed-job controller slot identity is malformed.') from e
    if slot_id < 0 or attempt != raw_attempt:
        raise ControllerLeadershipLostError(
            'Managed-job controller slot identity is not canonical.')
    return owner[0], owner[1], slot_id, attempt


def job_attempt_predicate(
    job_id: int,
    identity: ControllerSlotIdentity,
) -> sqlalchemy.ColumnElement[bool]:
    """Return the one canonical exact-attempt predicate for a job row."""
    instance_id, generation, slot_id, attempt = identity
    job_info = state_schema.job_info_table
    return sqlalchemy.and_(
        job_info.c.spot_job_id == job_id,
        job_info.c.controller_instance_id == instance_id,
        job_info.c.controller_generation == generation,
        job_info.c.controller_slot_id == slot_id,
        job_info.c.controller_slot_attempt == attempt,
    )


def owner_columns_predicate(
    owner: ControllerOwner,) -> sqlalchemy.ColumnElement[bool]:
    """Match the outer owner and, inside a slot, its exact attempt."""
    job_info = state_schema.job_info_table
    conditions = [
        job_info.c.controller_instance_id == owner[0],
        job_info.c.controller_generation == owner[1],
    ]
    slot = get_current_slot_identity()
    if slot is not None:
        if slot[:2] != owner:
            raise ControllerLeadershipLostError(
                'Managed-job slot owner does not match outer generation.')
        conditions.extend([
            job_info.c.controller_slot_id == slot[2],
            job_info.c.controller_slot_attempt == slot[3],
        ])
    return sqlalchemy.and_(*conditions)


def _lock_local_owner(owner: ControllerOwner) -> None:
    if not owner_is_current(owner):
        raise ControllerLeadershipLostError(
            'Managed-job local runtime owner is no longer current.')


def lock_current_owner(session: orm.Session, owner: ControllerOwner) -> None:
    """Serialize a write with PG takeover, or prove the local file-lock owner."""
    mode = owner_mode()
    if (mode == LOCAL_OWNER_MODE or
        (mode is None and
         controller_capability.local_authority_owner_is_current(*owner))):
        _lock_local_owner(owner)
        return
    result = session.execute(
        request_postgres.current_controller_leadership_statement(*owner,
                                                                 lock=True))
    if result.scalar_one_or_none() is None:
        raise ControllerLeadershipLostError(
            'Managed-job controller leadership changed before its write.')


async def lock_current_owner_async(session: sql_async.AsyncSession,
                                   owner: ControllerOwner) -> None:
    """Async counterpart of :func:`lock_current_owner`."""
    mode = owner_mode()
    if (mode == LOCAL_OWNER_MODE or
        (mode is None and
         controller_capability.local_authority_owner_is_current(*owner))):
        _lock_local_owner(owner)
        return
    result = await session.execute(
        request_postgres.current_controller_leadership_statement(*owner,
                                                                 lock=True))
    if result.scalar_one_or_none() is None:
        raise ControllerLeadershipLostError(
            'Managed-job controller leadership changed before its write.')


def lock_current_job_attempt(
    session: orm.Session,
    job_id: int,
    identity: ControllerSlotIdentity | None = None,
) -> ControllerSlotIdentity:
    """Lock and prove that one target job belongs to this exact attempt."""
    identity = identity or get_current_slot_identity()
    if identity is None:
        raise ControllerLeadershipLostError(
            'A managed-job mutation requires a slot attempt.')
    lock_current_owner(session, identity[:2])
    if session.get_bind().dialect.name == 'sqlite':
        matched = session.execute(
            sqlalchemy.update(state_schema.job_info_table).where(
                job_attempt_predicate(job_id, identity)).values({
                    state_schema.job_info_table.c.controller_slot_attempt:
                        identity[3],
                })).rowcount
    else:
        matched = int(
            session.execute(
                sqlalchemy.select(state_schema.job_info_table.c.spot_job_id).
                where(job_attempt_predicate(
                    job_id, identity)).with_for_update()).first() is not None)
    if matched != 1:
        raise ControllerLeadershipLostError(
            f'Managed job {job_id} is no longer owned by this slot attempt.')
    return identity


async def lock_current_job_attempt_async(
    session: sql_async.AsyncSession,
    job_id: int,
    identity: ControllerSlotIdentity | None = None,
) -> ControllerSlotIdentity:
    """Async counterpart of :func:`lock_current_job_attempt`."""
    identity = identity or get_current_slot_identity()
    if identity is None:
        raise ControllerLeadershipLostError(
            'A managed-job mutation requires a slot attempt.')
    await lock_current_owner_async(session, identity[:2])
    if session.get_bind().dialect.name == 'sqlite':
        matched = (await session.execute(
            sqlalchemy.update(state_schema.job_info_table).where(
                job_attempt_predicate(job_id, identity)).values({
                    state_schema.job_info_table.c.controller_slot_attempt:
                        identity[3],
                }))).rowcount
    else:
        matched = int((await session.execute(
            sqlalchemy.select(state_schema.job_info_table.c.spot_job_id).where(
                job_attempt_predicate(job_id, identity)).with_for_update())
                      ).first() is not None)
    if matched != 1:
        raise ControllerLeadershipLostError(
            f'Managed job {job_id} is no longer owned by this slot attempt.')
    return identity
