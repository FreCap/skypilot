"""Durable repository for external load balancer cutover state."""

from collections.abc import Iterator
from collections.abc import Mapping
import contextlib
import json
import time
from typing import Any

import sqlalchemy
from sqlalchemy import orm

from sky.serve import lb_ha
from sky.serve import serve_state_schema
from sky.utils.db import db_utils

services_table = serve_state_schema.services_table
_db_manager = serve_state_schema._db_manager  # pylint: disable=protected-access


def _require_postgresql_lb_cutover(engine: sqlalchemy.engine.Engine) -> None:
    if (engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        raise RuntimeError('External load balancer HA cutover state is '
                           'supported only on PostgreSQL.')


def parse_lb_cutover_state_record(
        record: Mapping[str, Any]) -> lb_ha.LbCutoverState:
    """Validate cutover state already read with its controller owner row."""
    enabled = bool(record['lb_ha_enabled'])
    active_slot = lb_ha.parse_slot(record['lb_active_slot'])
    pending_slot = lb_ha.parse_slot(record['lb_pending_slot'])
    phase = lb_ha.parse_phase(record['lb_cutover_phase'])
    generation = record['lb_cutover_generation']
    if (phase is None or not isinstance(generation, int) or generation < 0 or
        (enabled and (active_slot is None or generation < 1)) or
        (not enabled and
         (active_slot is not None or generation != 0 or pending_slot is not None
          or phase is not lb_ha.LbCutoverPhase.STABLE)) or
        (phase is lb_ha.LbCutoverPhase.PREPARING and pending_slot is None) or
        (phase is lb_ha.LbCutoverPhase.DRAINING and pending_slot is None)):
        raise RuntimeError('Malformed LB cutover state.')
    return lb_ha.LbCutoverState(enabled=enabled,
                                active_slot=active_slot,
                                generation=generation,
                                pending_slot=pending_slot,
                                phase=phase,
                                lifecycle_epoch=record['lifecycle_epoch'],
                                drain_started_at=record['lb_drain_started_at'])


def get_lb_cutover_state(service_name: str) -> lb_ha.LbCutoverState | None:
    """Read and validate one service's durable LB authority state."""
    engine = _db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                services_table.c.lb_ha_enabled,
                services_table.c.lb_active_slot,
                services_table.c.lb_cutover_generation,
                services_table.c.lb_pending_slot,
                services_table.c.lb_cutover_phase,
                services_table.c.lb_drain_started_at,
                services_table.c.lifecycle_epoch,
            ).where(services_table.c.name == service_name)).fetchone()
    if row is None:
        return None
    enabled = bool(row.lb_ha_enabled)
    if enabled:
        _require_postgresql_lb_cutover(engine)
    active_slot = lb_ha.parse_slot(row.lb_active_slot)
    pending_slot = lb_ha.parse_slot(row.lb_pending_slot)
    phase = lb_ha.parse_phase(row.lb_cutover_phase)
    generation = row.lb_cutover_generation
    if (phase is None or not isinstance(generation, int) or generation < 0 or
        (enabled and (active_slot is None or generation < 1)) or
        (not enabled and
         (active_slot is not None or generation != 0 or pending_slot is not None
          or phase is not lb_ha.LbCutoverPhase.STABLE)) or
        (phase is lb_ha.LbCutoverPhase.PREPARING and pending_slot is None) or
        (phase is lb_ha.LbCutoverPhase.DRAINING and pending_slot is None)):
        raise RuntimeError(f'Malformed LB cutover state for {service_name!r}.')
    return lb_ha.LbCutoverState(enabled=enabled,
                                active_slot=active_slot,
                                generation=generation,
                                pending_slot=pending_slot,
                                phase=phase,
                                lifecycle_epoch=row.lifecycle_epoch,
                                drain_started_at=row.lb_drain_started_at)


def _lb_cutover_owner_predicates(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
) -> list[Any]:
    return [
        services_table.c.name == service_name,
        services_table.c.hash == expected_service_hash,
        services_table.c.controller_pid == expected_controller_owner[0],
        services_table.c.controller_ip == expected_controller_owner[1],
        services_table.c.lifecycle_epoch == expected_lifecycle_epoch,
        services_table.c.lb_ha_enabled == 1,
    ]


def begin_lb_ha_migration(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
) -> bool:
    """Durably enter legacy-to-two-slot migration without moving traffic."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(
            services_table.c.name == service_name,
            services_table.c.hash == expected_service_hash,
            services_table.c.controller_pid == expected_controller_owner[0],
            services_table.c.controller_ip == expected_controller_owner[1],
            services_table.c.lifecycle_epoch == expected_lifecycle_epoch,
            services_table.c.lb_ha_enabled == 0,
            services_table.c.lb_active_slot.is_(None),
            services_table.c.lb_cutover_generation == 0,
            services_table.c.lb_pending_slot.is_(None),
            services_table.c.lb_cutover_phase ==
            lb_ha.LbCutoverPhase.STABLE.value,
        ).update({
            services_table.c.lb_ha_enabled: 1,
            services_table.c.lb_active_slot: lb_ha.LbSlot.A.value,
            services_table.c.lb_cutover_generation: 1,
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.MIGRATING.value,
        })
        session.commit()
    return count == 1


def finish_lb_ha_migration(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
) -> bool:
    """Commit slot A after the stable Service selector has moved to it."""
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == lb_ha.LbSlot.A.value,
        services_table.c.lb_cutover_generation == 1,
        services_table.c.lb_pending_slot.is_(None),
        services_table.c.lb_cutover_phase ==
        lb_ha.LbCutoverPhase.MIGRATING.value,
    ])
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.STABLE.value,
        })
        session.commit()
    return count == 1


def begin_lb_ha_rollback(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """Durably enter two-slot-to-legacy rollback without moving traffic."""
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == active_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_pending_slot.is_(None),
        services_table.c.lb_cutover_phase == lb_ha.LbCutoverPhase.STABLE.value,
    ])
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.ROLLING_BACK.value,
        })
        session.commit()
    return count == 1


def finish_lb_ha_rollback(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """Disable HA after the stable Service selector has moved to legacy."""
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == active_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_pending_slot.is_(None),
        services_table.c.lb_cutover_phase ==
        lb_ha.LbCutoverPhase.ROLLING_BACK.value,
    ])
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_ha_enabled: 0,
            services_table.c.lb_active_slot: None,
            services_table.c.lb_cutover_generation: 0,
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.STABLE.value,
            services_table.c.lb_drain_started_at: None,
            services_table.c.lb_demand_handoff_generation: None,
            services_table.c.lb_demand_handoff_snapshot: None,
            services_table.c.lb_demand_handoff_complete_at: None,
            services_table.c.lb_last_demand_snapshot: None,
        })
        session.commit()
    return count == 1


def begin_lb_cutover(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    expected_active_slot: lb_ha.LbSlot,
    expected_generation: int,
    target_slot: lb_ha.LbSlot,
    demand_snapshot: lb_ha.DemandSnapshot | None = None,
) -> lb_ha.LbCutoverState | None:
    """CAS STABLE N to PREPARING N+1 for the opposite slot."""
    if target_slot is not expected_active_slot.other:
        raise ValueError('LB cutover target must be the opposite slot.')
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    next_generation = expected_generation + 1
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == expected_active_slot.value,
        services_table.c.lb_cutover_generation == expected_generation,
        services_table.c.lb_pending_slot.is_(None),
        services_table.c.lb_cutover_phase == lb_ha.LbCutoverPhase.STABLE.value,
    ])
    with orm.Session(engine) as session:
        serialized_snapshot = (json.dumps(demand_snapshot.to_dict())
                               if demand_snapshot is not None else
                               services_table.c.lb_last_demand_snapshot)
        row = session.execute(
            sqlalchemy.update(services_table).where(*predicates).values(
                lb_pending_slot=target_slot.value,
                lb_cutover_generation=next_generation,
                lb_cutover_phase=lb_ha.LbCutoverPhase.PREPARING.value,
                lb_demand_handoff_generation=next_generation,
                lb_demand_handoff_snapshot=serialized_snapshot,
                lb_demand_handoff_complete_at=None).returning(
                    services_table.c.lifecycle_epoch)).fetchone()
        session.commit()
    if row is None:
        return None
    return lb_ha.LbCutoverState(enabled=True,
                                active_slot=expected_active_slot,
                                generation=next_generation,
                                pending_slot=target_slot,
                                phase=lb_ha.LbCutoverPhase.PREPARING,
                                lifecycle_epoch=row.lifecycle_epoch)


def record_lb_active_demand_snapshot(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    generation: int,
    demand_snapshot: lb_ha.DemandSnapshot,
) -> bool:
    """Persist demand only while the reporter remains the selected ACTIVE."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == active_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_cutover_phase.in_((
            lb_ha.LbCutoverPhase.STABLE.value,
            lb_ha.LbCutoverPhase.DRAINING.value,
        )),
    ])
    serialized_snapshot = json.dumps(demand_snapshot.to_dict())
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_last_demand_snapshot: serialized_snapshot,
        })
        session.commit()
    return count == 1


def get_lb_last_demand_snapshot(
        service_name: str) -> lb_ha.DemandSnapshot | None:
    """Read the restart-safe latest demand from the selected ACTIVE slot."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(services_table.c.lb_last_demand_snapshot).where(
                services_table.c.name == service_name)).fetchone()
    if row is None or row.lb_last_demand_snapshot is None:
        return None
    try:
        return lb_ha.DemandSnapshot.from_dict(
            json.loads(row.lb_last_demand_snapshot))
    except (TypeError, ValueError, json.JSONDecodeError) as e:
        raise RuntimeError('Malformed durable LB demand snapshot.') from e


def commit_lb_cutover(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    previous_slot: lb_ha.LbSlot,
    target_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """Commit a selector-switched target and retain the old slot as DRAINING."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == previous_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_pending_slot == target_slot.value,
        services_table.c.lb_cutover_phase ==
        lb_ha.LbCutoverPhase.PREPARING.value,
    ])
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_active_slot: target_slot.value,
            services_table.c.lb_pending_slot: previous_slot.value,
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.DRAINING.value,
            services_table.c.lb_drain_started_at: time.time(),
        })
        session.commit()
    return count == 1


def finish_lb_cutover_drain(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    draining_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """CAS DRAINING to STABLE after every former stream owner is clean/gone."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == active_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_pending_slot == draining_slot.value,
        services_table.c.lb_cutover_phase ==
        lb_ha.LbCutoverPhase.DRAINING.value,
    ])
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_pending_slot: None,
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.STABLE.value,
            services_table.c.lb_drain_started_at: None,
        })
        session.commit()
    return count == 1


def get_lb_demand_handoff(
    service_name: str,
) -> tuple[int | None, lb_ha.DemandSnapshot | None, float | None]:
    """Read the restart-safe demand floor for the current promotion."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(
                services_table.c.lb_demand_handoff_generation,
                services_table.c.lb_demand_handoff_snapshot,
                services_table.c.lb_demand_handoff_complete_at,
            ).where(services_table.c.name == service_name)).fetchone()
    if row is None:
        return None, None, None
    snapshot = None
    if row.lb_demand_handoff_snapshot is not None:
        try:
            snapshot = lb_ha.DemandSnapshot.from_dict(
                json.loads(row.lb_demand_handoff_snapshot))
        except (TypeError, ValueError, json.JSONDecodeError) as e:
            raise RuntimeError('Malformed durable LB demand handoff.') from e
    return (row.lb_demand_handoff_generation, snapshot,
            row.lb_demand_handoff_complete_at)


def mark_lb_demand_handoff_complete(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    generation: int,
) -> float | None:
    """Record the promoted active's first complete demand-gauge report."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_demand_handoff_generation == generation,
    ])
    completed_at = time.time()
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.update(services_table).where(
                *predicates,
                services_table.c.lb_demand_handoff_complete_at.is_(None)).
            values(lb_demand_handoff_complete_at=completed_at).returning(
                services_table.c.lb_demand_handoff_complete_at)).fetchone()
        if row is None:
            existing = session.execute(
                sqlalchemy.select(
                    services_table.c.lb_demand_handoff_complete_at).where(
                        *predicates)).scalar_one_or_none()
            session.rollback()
            return existing
        session.commit()
    return float(row.lb_demand_handoff_complete_at)


def clear_lb_demand_handoff(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    generation: int,
) -> bool:
    """Clear an expired demand floor without touching cutover authority."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.append(
        services_table.c.lb_demand_handoff_generation == generation)
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_demand_handoff_generation: None,
            services_table.c.lb_demand_handoff_snapshot: None,
            services_table.c.lb_demand_handoff_complete_at: None,
        })
        session.commit()
    return count == 1


@contextlib.contextmanager
def lb_cutover_kubernetes_guard(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    expected_active_slot: lb_ha.LbSlot,
    expected_generation: int,
    expected_phase: lb_ha.LbCutoverPhase,
    expected_pending_slot: lb_ha.LbSlot | None,
) -> Iterator[bool]:
    """Hold the service row lock across one external Kubernetes mutation.

    Controller ownership updates write the same PostgreSQL row and therefore
    wait for this transaction. This closes the otherwise unavoidable window
    in which a stale controller could pass a DB check and patch the Service
    selector after its successor took ownership.
    """
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == expected_active_slot.value,
        services_table.c.lb_cutover_generation == expected_generation,
        services_table.c.lb_cutover_phase == expected_phase.value,
        (services_table.c.lb_pending_slot.is_(None)
         if expected_pending_slot is None else services_table.c.lb_pending_slot
         == expected_pending_slot.value),
    ])
    with orm.Session(engine) as session:
        row = session.execute(
            sqlalchemy.select(services_table.c.name).where(
                *predicates).with_for_update()).fetchone()
        try:
            yield row is not None
        finally:
            session.rollback()


def abort_lb_cutover_preparation(
    service_name: str,
    expected_service_hash: str,
    expected_controller_owner: tuple[int | None, str | None],
    expected_lifecycle_epoch: int,
    active_slot: lb_ha.LbSlot,
    target_slot: lb_ha.LbSlot,
    generation: int,
) -> bool:
    """Abort an unselected armed target without reusing its generation."""
    engine = _db_manager.get_engine()
    _require_postgresql_lb_cutover(engine)
    predicates = _lb_cutover_owner_predicates(service_name,
                                              expected_service_hash,
                                              expected_controller_owner,
                                              expected_lifecycle_epoch)
    predicates.extend([
        services_table.c.lb_active_slot == active_slot.value,
        services_table.c.lb_cutover_generation == generation,
        services_table.c.lb_pending_slot == target_slot.value,
        services_table.c.lb_cutover_phase ==
        lb_ha.LbCutoverPhase.PREPARING.value,
    ])
    with orm.Session(engine) as session:
        count = session.query(services_table).filter(*predicates).update({
            services_table.c.lb_pending_slot: None,
            services_table.c.lb_cutover_phase:
                lb_ha.LbCutoverPhase.STABLE.value,
            services_table.c.lb_demand_handoff_generation: None,
            services_table.c.lb_demand_handoff_snapshot: None,
            services_table.c.lb_demand_handoff_complete_at: None,
        })
        session.commit()
    return count == 1


# Keep historical import identities even when this implementation module is
# imported before the serve_state facade.
for _moved_symbol in (
        _require_postgresql_lb_cutover,
        get_lb_cutover_state,
        _lb_cutover_owner_predicates,
        begin_lb_ha_migration,
        finish_lb_ha_migration,
        begin_lb_ha_rollback,
        finish_lb_ha_rollback,
        begin_lb_cutover,
        record_lb_active_demand_snapshot,
        get_lb_last_demand_snapshot,
        commit_lb_cutover,
        finish_lb_cutover_drain,
        get_lb_demand_handoff,
        mark_lb_demand_handoff_complete,
        clear_lb_demand_handoff,
        lb_cutover_kubernetes_guard,
        abort_lb_cutover_preparation,
):
    _moved_symbol.__module__ = 'sky.serve.serve_state'
    if hasattr(_moved_symbol, '__wrapped__'):
        _moved_symbol.__wrapped__.__module__ = 'sky.serve.serve_state'
del _moved_symbol
