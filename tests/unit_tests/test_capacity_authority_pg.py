"""PostgreSQL contracts for atomic SkyServe capacity activation."""
# pylint: disable=not-callable,redefined-outer-name,unused-import

import pytest
import sqlalchemy
from test_serve_capacity_admission_pg import capacity_database
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import capacity_admission
from sky.serve import capacity_authority
from sky.serve import pool_capacity_observation_schema
from sky.serve import serve_state_schema
from sky.serve import zero_cost_actuation

pytestmark = pytest.mark.xdist_group(
    name='serve_capacity_authority_schema_052_pg')


def _prepare_legacy_pair(database):
    engine, incarnation, _ = database
    services = serve_state_schema.services_table
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                demand_source_mode='LEGACY_CONTROLLER',
                demand_source_epoch=0,
                demand_authority_capable=False,
                demand_authority_controller_incarnation=None,
                demand_authority_protocol_version=None,
                reserved_fill_actuation_mode='DIRECT_REPLICA',
                reserved_fill_actuation_epoch=0,
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=incarnation,
                reserved_fill_actuation_protocol_version=1))
        connection.execute(
            sqlalchemy.update(
                serve_state_schema.reserved_fill_protocol_state_table).where(
                    serve_state_schema.reserved_fill_protocol_state_table.c.id
                    == 1).values(protocol_version=2,
                                 image_digest='sha256:' + '1' * 64,
                                 deployment_generation='deployment-1',
                                 deployment_uid='deployment-uid-1',
                                 pod_inventory_count=1,
                                 pod_inventory_sha256='2' * 64))
        connection.execute(
            sqlalchemy.update(
                pool_capacity_observation_schema.protocol_state_sequence_table).
            where(pool_capacity_observation_schema.
                  protocol_state_sequence_table.c.id == 1).values(
                      protocol_version=2,
                      image_digest='sha256:' + '1' * 64,
                      deployment_generation='deployment-1',
                      deployment_uid='deployment-uid-1',
                      pod_inventory_count=1,
                      pod_inventory_sha256='2' * 64,
                      reconciliation_gate_state='SEQUENCED_ACTIVE',
                      reconciliation_gate_generation=1,
                      reclaim_fleet_bundle_sha256='3' * 64,
                      reclaim_policy_revision='reclaim-v1',
                      reclaim_provider_inventory_sha256='4' * 64,
                      reclaim_claim_scope_count=0,
                      reclaim_claim_scope_sha256='5' * 64,
                      reclaim_evidence_sha256='6' * 64,
                      reclaim_authorized_at=1.0))
    return engine, incarnation


def test_atomic_promotion_commits_both_adjacent_epochs(
        capacity_database) -> None:
    engine, incarnation = _prepare_legacy_pair(capacity_database)

    with engine.begin() as connection:
        epochs = capacity_authority.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            expected_demand_source_epoch=0,
            expected_zero_cost_actuation_epoch=0,
            participant_barrier_passed=lambda _connection: True)

    assert epochs == capacity_authority.CapacityAuthorityEpochs(1, 1)
    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc')).mappings().one()
    assert service['demand_source_mode'] == 'DURABLE_FEED'
    assert service['demand_source_epoch'] == 1
    assert service['reserved_fill_actuation_mode'] == 'DURABLE_INTENT'
    assert service['reserved_fill_actuation_epoch'] == 1

    # A lost response retries from the exact source epochs and performs no
    # second transition.
    with engine.begin() as connection:
        assert capacity_authority.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            expected_demand_source_epoch=0,
            expected_zero_cost_actuation_epoch=0,
            participant_barrier_passed=lambda _connection: False) == epochs


def test_actuation_rejection_rolls_back_demand_promotion(
        capacity_database) -> None:
    engine, incarnation = _prepare_legacy_pair(capacity_database)
    barrier_results = iter((True, False))

    with pytest.raises(zero_cost_actuation.ZeroCostActuationUnavailable):
        with engine.begin() as connection:
            capacity_authority.promote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=incarnation,
                expected_demand_source_epoch=0,
                expected_zero_cost_actuation_epoch=0,
                participant_barrier_passed=lambda _connection: next(
                    barrier_results))

    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc')).mappings().one()
    assert service['demand_source_mode'] == 'LEGACY_CONTROLLER'
    assert service['demand_source_epoch'] == 0
    assert service['reserved_fill_actuation_mode'] == 'DIRECT_REPLICA'
    assert service['reserved_fill_actuation_epoch'] == 0


def test_inverse_partial_pair_is_never_repaired(capacity_database) -> None:
    engine, incarnation = _prepare_legacy_pair(capacity_database)
    services = serve_state_schema.services_table
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                reserved_fill_actuation_mode='DURABLE_INTENT',
                reserved_fill_actuation_epoch=1))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='cannot precede'):
        with engine.begin() as connection:
            capacity_authority.promote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=incarnation,
                expected_demand_source_epoch=0,
                expected_zero_cost_actuation_epoch=0,
                participant_barrier_passed=lambda _connection: True)
