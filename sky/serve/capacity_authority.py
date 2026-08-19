"""Atomic activation of SkyServe demand and zero-cost actuation authority."""
from __future__ import annotations

from collections.abc import Callable
import dataclasses
import uuid

import sqlalchemy

from sky.serve import capacity_admission
from sky.serve import serve_state_schema
from sky.serve import zero_cost_actuation
from sky.serve import zero_cost_actuation_schema


@dataclasses.dataclass(frozen=True)
class CapacityAuthorityEpochs:
    """The adjacent epochs committed by one capacity-authority cutover."""

    demand_source_epoch: int
    zero_cost_actuation_epoch: int


def rebind_service_after_controller_takeover_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    controller_incarnation: uuid.UUID,
    controller_owner_epoch: int,
) -> bool:
    """Rebind an already-promoted capacity pair to a replacement controller.

    Promotion is one way: a controller takeover must not demote durable demand
    or advance either source epoch.  Instead, the takeover transaction moves
    the demand and zero-cost capability advertisements together to the exact
    new controller incarnation.  Route ownership is deliberately not moved
    here.  The existing takeover path revokes its leases, so both old demand
    reports and old capacity plans remain fail-closed until the replacement
    controller publishes a new route and receives fresh complete reports for
    it.

    Pending grant-before-row intents belong to the pre-takeover reconciliation
    lineage.  Retire them in this same transaction so a reused PID/IP/port
    fingerprint cannot actuate stale demand after the route/report barrier
    becomes fresh.  Committed intents already own replica rows and are not
    changed.

    Returns whether durable demand was rebound.  Legacy services are left
    untouched and keep advertising zero-cost capability during ordinary
    manager construction.
    """
    if (not isinstance(service_name, str) or not service_name or
            not isinstance(controller_incarnation, uuid.UUID) or
            isinstance(controller_owner_epoch, bool) or
            not isinstance(controller_owner_epoch, int) or
            controller_owner_epoch < 1):
        raise ValueError('Capacity takeover requires an exact service owner.')
    if connection.dialect.name != 'postgresql':
        raise capacity_admission.CapacityAdmissionUnavailable(
            'Capacity-authority takeover requires PostgreSQL.')

    services = serve_state_schema.services_table
    service = connection.execute(
        sqlalchemy.select(services).where(services.c.name == service_name).
        with_for_update()).mappings().one_or_none()
    if (service is None or
            service['controller_incarnation'] != controller_incarnation or
            service['controller_owner_epoch'] != controller_owner_epoch):
        raise capacity_admission.CapacityAdmissionConflict(
            'Capacity-authority takeover lost the current controller fence.')
    try:
        demand_mode = capacity_admission.DemandSourceMode(
            service['demand_source_mode'])
        actuation_mode = zero_cost_actuation.ActuationMode(
            service['reserved_fill_actuation_mode'])
        demand_epoch = int(service['demand_source_epoch'])
        actuation_epoch = int(service['reserved_fill_actuation_epoch'])
    except (TypeError, ValueError) as error:
        raise capacity_admission.CapacityAdmissionConflict(
            'Capacity-authority takeover found malformed service state.'
        ) from error
    legacy_pair = (
        demand_mode is capacity_admission.DemandSourceMode.LEGACY_CONTROLLER and
        actuation_mode is zero_cost_actuation.ActuationMode.DIRECT_REPLICA)
    durable_pair = (
        demand_mode is capacity_admission.DemandSourceMode.DURABLE_FEED and
        actuation_mode is zero_cost_actuation.ActuationMode.DURABLE_INTENT)
    if not (legacy_pair or durable_pair):
        raise capacity_admission.CapacityAdmissionConflict(
            'Controller takeover requires a complete capacity-authority pair.')
    if legacy_pair:
        return False
    if (service['pool'] not in (0, False) or demand_epoch < 1 or
            service['demand_authority_capable'] is not True or
            service['demand_authority_protocol_version']
            != capacity_admission.PROTOCOL_VERSION or actuation_epoch < 1 or
            service['reserved_fill_actuation_capable'] is not True or
            service['reserved_fill_actuation_protocol_version']
            != zero_cost_actuation.PROTOCOL_VERSION):
        raise capacity_admission.CapacityAdmissionConflict(
            'Promoted capacity authority is not safe to rebind.')

    result = connection.execute(
        sqlalchemy.update(services).where(
            services.c.name == service_name,
            services.c.controller_incarnation == controller_incarnation,
            services.c.controller_owner_epoch == controller_owner_epoch,
            services.c.demand_source_mode ==
            capacity_admission.DemandSourceMode.DURABLE_FEED.value,
            services.c.demand_source_epoch == demand_epoch,
            services.c.reserved_fill_actuation_mode == actuation_mode.value,
            services.c.reserved_fill_actuation_epoch == actuation_epoch).values(
                demand_authority_capable=True,
                demand_authority_controller_incarnation=(
                    controller_incarnation),
                demand_authority_protocol_version=(
                    capacity_admission.PROTOCOL_VERSION),
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=(
                    controller_incarnation),
                reserved_fill_actuation_protocol_version=(
                    zero_cost_actuation.PROTOCOL_VERSION)))
    if result.rowcount != 1:
        raise capacity_admission.CapacityAdmissionConflict(
            'Capacity-authority takeover lost its compare-and-swap.')

    intents = (
        zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table)
    now = connection.execute(
        sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
    connection.execute(
        sqlalchemy.update(intents).where(
            intents.c.service_name == service_name,
            intents.c.service_hash == service['hash'],
            intents.c.service_lifecycle_epoch == service['lifecycle_epoch'],
            intents.c.actuation_epoch == actuation_epoch,
            intents.c.state.in_(
                tuple(state.value for state in (
                    zero_cost_actuation.IntentState.GRANTED,
                    zero_cost_actuation.IntentState.ACTUATING,
                    zero_cost_actuation.IntentState.RETRYABLE,
                )))).values(
                    state=zero_cost_actuation.IntentState.TERMINAL.value,
                    lease_owner=None,
                    lease_expires_at=None,
                    last_error='controller_owner_changed',
                    updated_at=now,
                    terminal_at=now))
    return True


def promote_service_in_connection(
    connection: sqlalchemy.engine.Connection,
    *,
    service_name: str,
    controller_incarnation: uuid.UUID,
    expected_demand_source_epoch: int,
    expected_zero_cost_actuation_epoch: int,
    participant_barrier_passed: Callable[[sqlalchemy.engine.Connection], bool],
) -> CapacityAuthorityEpochs:
    """Atomically make durable demand and intent actuation the sole path.

    The caller owns the PostgreSQL transaction and the fleet-participant table
    lock.  Both underlying transitions therefore either commit together or
    roll back together.  A retained ``DURABLE_FEED``/``DIRECT_REPLICA`` pair
    from the deprecated two-call rollout is accepted only as a fix-forward
    repair; the inverse pair is never a valid authority state.
    """
    if (not isinstance(controller_incarnation, uuid.UUID) or
            isinstance(expected_demand_source_epoch, bool) or
            not isinstance(expected_demand_source_epoch, int) or
            expected_demand_source_epoch < 0 or
            isinstance(expected_zero_cost_actuation_epoch, bool) or
            not isinstance(expected_zero_cost_actuation_epoch, int) or
            expected_zero_cost_actuation_epoch < 0 or
            not callable(participant_barrier_passed)):
        raise ValueError('Atomic capacity promotion requires exact epochs, '
                         'controller identity, and a live fleet barrier.')

    services = serve_state_schema.services_table
    service = connection.execute(
        sqlalchemy.select(
            services.c.demand_source_mode,
            services.c.demand_source_epoch,
            services.c.reserved_fill_actuation_mode,
            services.c.reserved_fill_actuation_epoch,
        ).where(services.c.name ==
                service_name).with_for_update()).mappings().one_or_none()
    if service is None:
        raise capacity_admission.CapacityAdmissionConflict(
            'Service no longer exists.')

    try:
        demand_mode = capacity_admission.DemandSourceMode(
            service['demand_source_mode'])
        actuation_mode = zero_cost_actuation.ActuationMode(
            service['reserved_fill_actuation_mode'])
        demand_epoch = int(service['demand_source_epoch'])
        actuation_epoch = int(service['reserved_fill_actuation_epoch'])
    except (TypeError, ValueError) as error:
        raise capacity_admission.CapacityAdmissionConflict(
            'Capacity promotion found malformed service authority.') from error
    demand_source_epoch_valid = (
        demand_epoch == expected_demand_source_epoch
        if demand_mode is capacity_admission.DemandSourceMode.LEGACY_CONTROLLER
        else demand_epoch == expected_demand_source_epoch + 1)
    actuation_epoch_valid = (
        actuation_epoch == expected_zero_cost_actuation_epoch
        if actuation_mode is zero_cost_actuation.ActuationMode.DIRECT_REPLICA
        else actuation_epoch == expected_zero_cost_actuation_epoch + 1)
    if not demand_source_epoch_valid or not actuation_epoch_valid:
        raise capacity_admission.CapacityAdmissionConflict(
            'Capacity authority epoch changed before atomic promotion.')
    if (demand_mode is capacity_admission.DemandSourceMode.LEGACY_CONTROLLER and
            actuation_mode is zero_cost_actuation.ActuationMode.DURABLE_INTENT):
        raise capacity_admission.CapacityAdmissionConflict(
            'Durable actuation cannot precede durable demand authority.')

    promoted_demand_epoch = capacity_admission.promote_service_in_connection(
        connection,
        service_name=service_name,
        controller_incarnation=controller_incarnation,
        participant_barrier_passed=participant_barrier_passed)
    promoted_actuation_epoch = (
        zero_cost_actuation.promote_service_in_connection(
            connection,
            service_name=service_name,
            controller_incarnation=controller_incarnation,
            expected_actuation_epoch=expected_zero_cost_actuation_epoch,
            participant_barrier_passed=participant_barrier_passed(connection)))
    expected_demand_epoch = expected_demand_source_epoch + 1
    expected_actuation_epoch = expected_zero_cost_actuation_epoch + 1
    if (promoted_demand_epoch != expected_demand_epoch or
            promoted_actuation_epoch != expected_actuation_epoch):
        raise capacity_admission.CapacityAdmissionConflict(
            'Atomic capacity promotion did not advance adjacent epochs.')
    return CapacityAuthorityEpochs(
        demand_source_epoch=promoted_demand_epoch,
        zero_cost_actuation_epoch=promoted_actuation_epoch)
