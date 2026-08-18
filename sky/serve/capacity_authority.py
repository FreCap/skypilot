"""Atomic activation of SkyServe demand and zero-cost actuation authority."""
from __future__ import annotations

from collections.abc import Callable
import dataclasses
import uuid

import sqlalchemy

from sky.serve import capacity_admission
from sky.serve import serve_state_schema
from sky.serve import zero_cost_actuation


@dataclasses.dataclass(frozen=True)
class CapacityAuthorityEpochs:
    """The adjacent epochs committed by one capacity-authority cutover."""

    demand_source_epoch: int
    zero_cost_actuation_epoch: int


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
