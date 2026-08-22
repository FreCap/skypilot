"""PostgreSQL contracts for grant-before-row zero-cost actuation."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import datetime
import time
import uuid

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky import clouds
from sky.serve import capacity_admission
from sky.serve import constants as serve_constants
from sky.serve import kubernetes_identity
from sky.serve import kueue_lane_lineage_schema
from sky.serve import ordinary_launch_binding
from sky.serve import pool_capacity_observation
from sky.serve import pool_capacity_observation_schema
from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import reserved_fill_reclaim_attestation
from sky.serve import reserved_fill_reclaim_proof_schema
from sky.serve import reserved_fill_reclaim_proofs
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import serve_utils
from sky.serve import service_spec
from sky.serve import spot_placer
from sky.serve import zero_cost_actuation
from sky.serve import zero_cost_actuation_schema
from sky.server.requests import postgres_schema as request_postgres_schema
from sky.utils import common_utils
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_zero_cost_actuation_schema_052_pg')

_SERVICE_HASH = 'service-incarnation'
_CONTROLLER_PID = 41
_CONTROLLER_IP = '10.0.0.7'
_CONTROLLER_PORT = 8123
_OWNER = serve_utils.make_controller_owner_fingerprint(_SERVICE_HASH,
                                                       _CONTROLLER_PID,
                                                       _CONTROLLER_IP,
                                                       _CONTROLLER_PORT)


def _worker_projection(context: str,
                       accelerator_count: int,
                       *,
                       kueue: bool = False) -> dict[str, object]:
    context_ordinal = {
        'context-a': 0,
        'east': 1,
        'west': 2,
        'phx': 3,
    }[context]
    candidate_ordinal = context_ordinal * 2 + int(accelerator_count == 8)
    return {
        'projection_version': 2,
        'candidate_id': f'kubernetes-{candidate_ordinal:04d}',
        'kubernetes_context': context,
        'namespace': 'default',
        'service_account_name': 'skyserve-worker',
        'scheduler_name': 'default-scheduler',
        'priority_class_name': 'skyserve-preemptible',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'kueue_admission': ({
            'local_queue_name': 'be',
            'workload_priority_class_name': 'be-ls',
        } if kueue else None),
        'pod_identity_role_arn': None,
        'accelerator_name': 'L4',
        'accelerator_count': accelerator_count,
        'accelerator_scheduling': {
            'label_key': 'nvidia.com/gpu.product',
            'label_values': ['NVIDIA-L4'],
            'resource_key': 'nvidia.com/gpu',
        },
        'cache': {
            'kind': 'none',
        },
    }


_WORKER_PROJECTIONS = [
    _worker_projection(context, count)
    for context in ('context-a', 'east', 'west')
    for count in (1, 8)
] + [_worker_projection('phx', count, kueue=True) for count in (1, 8)]


def _plan(
    *,
    free_slots: int = 2,
    service_version: int = 19,
    accelerator_count: int = 1,
    context: str = 'context-a',
    physical_uid: str = 'uid-a',
    kueue: bool = False,
    valid_until: float | None = None,
    capacity_unit: reserved_fill_planner.FillCapacityUnit = (
        reserved_fill_planner.FillCapacityUnit.PHYSICAL)
) -> reserved_fill_planner.FillPlan:
    projection = _worker_projection(context, accelerator_count, kueue=kueue)
    location = spot_placer.Location(cloud=clouds.Kubernetes(),
                                    region=context,
                                    zone=None,
                                    accelerators={'L4': accelerator_count},
                                    use_spot=False)
    pool_key = reserved_capacity_broker.make_pool_key(
        context,
        'L4',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=physical_uid)
    snapshot = reserved_fill_planner.PoolFillSnapshot.from_mapping({
        'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
        'pool_key': pool_key,
        'physical_cluster_uid': physical_uid,
        'service_generation': 7,
        'worker_projection_sha256_by_accelerator': {
            'l4': kubernetes_identity.worker_projection_sha256(projection),
        },
        'edge_cap': free_slots,
        'broker_slot_width': accelerator_count,
        'free_slots': free_slots,
        'free_slots_by_accelerator': {
            'l4': free_slots,
        },
        'grant': free_slots,
        'grant_epoch': 23 if free_slots else None,
        'observation_generation': 13,
        'observation_sequence': 17,
        'ordinary_zero_cost_admission_sequence': 17,
        'valid_until':
            (time.time() + 60 if valid_until is None else valid_until),
        'zero_cost_location_keys': [location.to_pickleable()],
    })
    allocation = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=service_version,
        ordinary_zero_cost_admission_sequence_high_water=17,
        reconciliation_gate_generation=1,
        reclaim_fleet_bundle_sha256='c' * 64,
        reclaim_policy_revision='reclaim-v1',
        reclaim_provider_inventory_sha256='d' * 64,
        pool_snapshots=(snapshot,))
    return reserved_fill_planner.ReservedFillPlanner.plan(
        policy_revision=2,
        reconcile_generation=3,
        allocation_map=allocation,
        service_incarnation=_SERVICE_HASH,
        service_version=service_version,
        controller_owner=_OWNER,
        max_replicas=100,
        planned_replicas=0,
        capacity_unit=capacity_unit)


@pytest.fixture
def actuation_database(empty_postgres, monkeypatch):
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    # Exercise retained pre-Serve056 rows through the current canonical
    # Serve057 schema. Older revisions cannot represent the admission graph
    # now locked by every grant, including non-Kueue grants.
    alembic_command.upgrade(config, '057')
    request_postgres_schema.REQUESTS.create(empty_postgres, checkfirst=True)
    request_postgres_schema.QUEUE.create(empty_postgres, checkfirst=True)
    request_postgres_schema.REQUEST_RETENTION_PINS.create(empty_postgres,
                                                          checkfirst=True)
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        empty_postgres)
    incarnation = uuid.uuid4()
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name='svc', epoch=3))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name='svc',
                workspace='workspace-a',
                status='READY',
                hash=_SERVICE_HASH,
                resource_scope=_SERVICE_HASH,
                current_version=19,
                active_versions='[19]',
                pool=0,
                lifecycle_epoch=3,
                controller_incarnation=incarnation,
                controller_owner_epoch=4,
                controller_pid=_CONTROLLER_PID,
                controller_ip=_CONTROLLER_IP,
                controller_port=_CONTROLLER_PORT,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=1,
                reserved_fill_actuation_mode='DURABLE_INTENT',
                reserved_fill_actuation_epoch=1,
                reserved_fill_actuation_capable=True,
                reserved_fill_actuation_controller_incarnation=incarnation,
                reserved_fill_actuation_protocol_version=1))
        ordinary_launch_binding.promote_non_pool_launch_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            controller_owner_epoch=4,
            expected_binding_epoch=1,
            participant_barrier_passed=lambda _: True,
            legacy_requests_drained=lambda _: True)
        connection.execute(
            sqlalchemy.insert(serve_state_schema.version_specs_table).values(
                service_name='svc',
                version=19,
                yaml_content='service: {}',
                placement_catalog={
                    'schema_version': 1,
                    'entries': [],
                    'num_nodes': 1,
                },
                worker_placement_projections=_WORKER_PROJECTIONS))
    return empty_postgres


def _grant_plan(
    repository: zero_cost_actuation.ZeroCostActuationRepository,
    plan: reserved_fill_planner.FillPlan,
    *,
    max_capacity: int,
) -> reserved_fill_planner.FillCommitResult:
    with repository.engine.connect() as connection:
        controller = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.controller_incarnation,
                serve_state_schema.services_table.c.controller_owner_epoch).
            where(serve_state_schema.services_table.c.name ==
                  'svc')).mappings().one()
    return repository.grant_plan(
        'svc',
        plan,
        max_capacity=max_capacity,
        expected_controller_incarnation=controller['controller_incarnation'],
        expected_controller_owner_epoch=controller['controller_owner_epoch'])


def _install_fresh_provider_proofs(
    engine: sqlalchemy.engine.Engine,
    intents: tuple[reserved_fill_planner.FillIntent, ...],
) -> None:
    """Activate the exact test gate and publish provider-free proof receipts."""
    assert intents
    first = intents[0]
    identity = reserved_fill_reclaim_attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256=first.reclaim_fleet_bundle_sha256,
        policy_revision=first.reclaim_policy_revision,
        provider_inventory_sha256=first.reclaim_provider_inventory_sha256)
    gate = pool_capacity_observation.PoolCapacityObservationRepository(
        engine).read_reconciliation_gate()
    if not gate.sequenced_active:
        evidence = reserved_fill_reclaim_attestation.ReclaimEnforcementEvidence(
            contract=(
                reserved_fill_reclaim_attestation.ReclaimEnforcementContract.
                GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2),
            fleet_bundle_sha256=identity.fleet_bundle_sha256,
            policy_revision=identity.policy_revision,
            provider_inventory_sha256=identity.provider_inventory_sha256,
            claimed_contexts=(),
            completed_monotonic=time.monotonic())
        receipt = reserved_fill_reclaim_attestation.activation_receipt(
            evidence,
            writer_image_digest='sha256:' + 'e' * 64,
            writer_deployment_generation='proof-ready-test',
            writer_deployment_uid='proof-ready-test-uid',
            writer_pod_inventory_count=1,
            writer_pod_inventory_sha256='f' * 64)
        assert serve_state.set_reserved_fill_protocol_version(
            serve_state.RESERVED_FILL_PROTOCOL_V2,
            expected_protocol_version=(serve_state.RESERVED_FILL_PROTOCOL_V1),
            image_digest='sha256:' + 'e' * 64,
            deployment_generation='proof-ready-test',
            deployment_uid='proof-ready-test-uid',
            pod_inventory_count=1,
            pod_inventory_sha256='f' * 64)
        activated = pool_capacity_observation.PoolCapacityObservationRepository(
            engine).authorize_sequenced_reconciliation(expected_generation=0,
                                                       receipt=receipt)
        assert activated.gate.generation == first.reconciliation_gate_generation
    else:
        assert gate.generation == first.reconciliation_gate_generation
        assert gate.reclaim_policy_identity == identity

    repository = reserved_fill_reclaim_proofs.ReclaimProviderProofRepository(
        engine)
    try:
        authorities = {(intent.allowed_locations[0].region,
                        intent.physical_cluster_uid) for intent in intents}
        for context, physical_uid in sorted(authorities):
            payload = {
                'aws': {},
                'kubernetes': {
                    'physical_cluster_uid': physical_uid,
                },
            }
            repository.renew(
                identity=identity,
                gate_generation=first.reconciliation_gate_generation,
                kubernetes_context=context,
                deadline_monotonic=(time.monotonic() +
                                    reserved_fill_reclaim_attestation.
                                    PROVIDER_PROOF_REFRESH_TIMEOUT_SECONDS),
                prove=lambda payload=payload: reserved_fill_reclaim_proofs.
                ReclaimProviderProofCandidate(proof_payload=payload,
                                              oldest_completed_monotonic=time.
                                              monotonic()),
                validate=lambda _payload: True,
                minimum_remaining_seconds=(
                    reserved_fill_reclaim_attestation.
                    PROVIDER_PROOF_RENEW_MIN_REMAINING_SECONDS))
    finally:
        repository._proof_engine.dispose()


def _commit_test_service_version(
        version: int) -> serve_state.VersionCommitResult:
    spec = service_spec.SkyServiceSpec(readiness_path='/health',
                                       initial_delay_seconds=0,
                                       readiness_timeout_seconds=5,
                                       endpoint_probe_interval_seconds=1,
                                       lb_stream_timeout_seconds=10,
                                       min_replicas=0,
                                       max_replicas=2,
                                       target_qps_per_replica=1,
                                       lb_high_availability=False)
    return serve_state.add_or_update_version(
        'svc',
        version,
        spec,
        'service: {}',
        expected_service_hash=_SERVICE_HASH,
        expected_controller_owner=(_CONTROLLER_PID, _CONTROLLER_IP))


def _paid_replica_for_shape(
    intent: reserved_fill_planner.FillIntent,
    replica_id: int,
) -> replica_managers.ReplicaInfo:
    """Build an ordinary paid replica with the intent's exact shape."""
    location = intent.allowed_locations[0].to_location()
    info = replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=f'svc-{replica_id}',
        replica_port='8080',
        is_spot=True,
        location=location,
        version=intent.service_version,
        resources_override=location.to_dict(),
        planned_capacity=intent.capacity_unit.intent_cost(
            intent.accelerator_count))
    info.is_zero_cost = False
    return info


def _insert_paid_replica_for_shape(
    engine: sqlalchemy.engine.Engine,
    intent: reserved_fill_planner.FillIntent,
    *,
    replica_id: int = 900,
) -> None:
    """Install one cleanup-unproven paid row with the intent's exact shape."""
    info = _paid_replica_for_shape(intent, replica_id)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **serve_state._replica_row_values('svc', replica_id, info)))


def test_serve056_lineage_and_postgresql_only() -> None:
    sqlite = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    revision = scripts.get_revision('052')
    assert scripts.get_heads() == ['057']
    assert scripts.get_revision('056').down_revision == '055'
    assert revision.down_revision == '051'
    assert migration_utils.SERVE_VERSION == '057'
    assert migration_utils.serve_target_version(sqlite) == '037'
    with pytest.raises(RuntimeError, match='PostgreSQL-only'):
        alembic_command.upgrade(config, '056')


def test_grant_is_idempotent_and_allocates_no_replica(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=2)

    first = _grant_plan(repository, plan, max_capacity=2)
    second = _grant_plan(repository, plan, max_capacity=2)

    assert len(first.accepted) == 2
    assert [item.replica_id for item in first.accepted] == [None, None]
    assert second.accepted == first.accepted
    with actuation_database.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table)).mappings().all()
        replica_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one()
    assert len(rows) == 2
    assert {row['state'] for row in rows} == {'GRANTED'}
    assert replica_count == 0


def test_kueue_grant_accepts_full_authenticated_same_domain_batch(
        actuation_database) -> None:
    config = migration_utils.get_alembic_config(actuation_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=90,
                 context='phx',
                 physical_uid='uid-phx',
                 kueue=True)

    first = _grant_plan(repository, plan, max_capacity=90)
    replay = _grant_plan(repository, plan, max_capacity=90)

    assert [item.intent_idempotency_key for item in first.accepted
           ] == [intent.idempotency_key for intent in plan.intents]
    assert not first.deferred
    assert replay == first
    with actuation_database.connect() as connection:
        intent_rows = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table)).mappings().all()
        lane_rows = connection.execute(
            sqlalchemy.select(kueue_lane_lineage_schema.
                              serve_kueue_admissions_table)).mappings().all()
    assert len(intent_rows) == 90
    assert len(lane_rows) == 90
    assert {row['intent_idempotency_key'] for row in lane_rows
           } == {intent.idempotency_key for intent in plan.intents}
    assert {row['state'] for row in lane_rows} == {'INTENT_PENDING'}


@pytest.mark.parametrize(('kueue', 'expected_result'), [
    (True, serve_state.VersionCommitResult.KUEUE_ADMISSION_HOLD),
    (False, serve_state.VersionCommitResult.COMMITTED),
],
                         ids=('waiting-for-kueue', 'non-kueue'))
def test_version_election_holds_only_outgoing_kueue_admission(
        actuation_database, kueue: bool,
        expected_result: serve_state.VersionCommitResult) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1,
                 context='phx' if kueue else 'context-a',
                 physical_uid='uid-phx' if kueue else 'uid-a',
                 kueue=kueue)
    assert len(_grant_plan(repository, plan, max_capacity=2).accepted) == 1

    result = _commit_test_service_version(20)

    assert result is expected_result
    with actuation_database.connect() as connection:
        elected = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.current_version).where(
                    serve_state_schema.services_table.c.name ==
                    'svc')).scalar_one()
        committed = connection.execute(
            sqlalchemy.select(
                serve_state_schema.version_specs_table.c.yaml_content).where(
                    serve_state_schema.version_specs_table.c.service_name ==
                    'svc', serve_state_schema.version_specs_table.c.version ==
                    20)).scalar_one_or_none()
    assert elected == (19 if kueue else 20)
    if kueue:
        assert committed is None
    else:
        assert committed == 'service: {}'


def test_stale_version_precedes_older_kueue_admission_hold(
        actuation_database) -> None:
    specs = serve_state_schema.version_specs_table
    services = serve_state_schema.services_table
    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(specs).where(specs.c.service_name == 'svc',
                                           specs.c.version == 19))
        connection.execute(
            sqlalchemy.insert(specs).values(
                service_name='svc',
                version=18,
                yaml_content='service: old',
                placement_catalog={
                    'schema_version': 1,
                    'entries': [],
                    'num_nodes': 1,
                },
                worker_placement_projections=_WORKER_PROJECTIONS))
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                current_version=18, active_versions='[18]'))

    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1,
                 service_version=18,
                 context='phx',
                 physical_uid='uid-phx',
                 kueue=True)
    assert len(_grant_plan(repository, plan, max_capacity=2).accepted) == 1

    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(specs).values(service_name='svc',
                                            version=20,
                                            yaml_content='service: high'))
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                current_version=20, active_versions='[20]'))

    result = _commit_test_service_version(19)

    assert result is serve_state.VersionCommitResult.STALE_VERSION
    with actuation_database.connect() as connection:
        current_version = connection.execute(
            sqlalchemy.select(services.c.current_version).where(
                services.c.name == 'svc')).scalar_one()
        candidate = connection.execute(
            sqlalchemy.select(specs.c.yaml_content).where(
                specs.c.service_name == 'svc',
                specs.c.version == 19)).scalar_one_or_none()
    assert current_version == 20
    assert candidate is None


def test_kueue_same_domain_batch_leases_in_three_bounded_waves(
        actuation_database) -> None:
    config = migration_utils.get_alembic_config(actuation_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=90,
                 context='phx',
                 physical_uid='uid-phx',
                 kueue=True)
    assert len(_grant_plan(repository, plan, max_capacity=90).accepted) == 90
    assert not repository.actionable_pool_keys(service_name='svc')
    assert not repository.lease_batch(service_name='svc',
                                      pool_key=plan.intents[0].pool_key,
                                      owner=uuid.uuid4(),
                                      lease_seconds=30)
    with actuation_database.connect() as connection:
        assert set(
            connection.execute(
                sqlalchemy.select(zero_cost_actuation_schema.
                                  serve_zero_cost_actuation_intents_table.c.
                                  state)).scalars()) == {'GRANTED'}
        assert set(
            connection.execute(
                sqlalchemy.select(
                    kueue_lane_lineage_schema.serve_kueue_admissions_table.c.
                    state)).scalars()) == {'INTENT_PENDING'}
    _install_fresh_provider_proofs(actuation_database, plan.intents)
    assert repository.actionable_pool_keys(
        service_name='svc') == (plan.intents[0].pool_key,)
    owner = uuid.uuid4()

    waves = tuple(
        repository.lease_batch(service_name='svc',
                               pool_key=plan.intents[0].pool_key,
                               owner=owner,
                               lease_seconds=30) for _ in range(3))

    assert tuple(len(wave) for wave in waves) == (32, 32, 26)
    leases = tuple(lease for wave in waves for lease in wave)
    assert len({lease.intent.idempotency_key for lease in leases}) == 90
    assert {lease.generation for lease in leases} == {1}
    assert [lease.intent.idempotency_key for lease in leases
           ] == sorted(intent.idempotency_key for intent in plan.intents)
    assert not repository.lease_batch(service_name='svc',
                                      pool_key=plan.intents[0].pool_key,
                                      owner=owner,
                                      lease_seconds=30)


def test_kueue_grant_rejects_multi_node_immutable_catalog(
        actuation_database) -> None:
    config = migration_utils.get_alembic_config(actuation_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.version_specs_table).where(
                serve_state_schema.version_specs_table.c.service_name == 'svc',
                serve_state_schema.version_specs_table.c.version == 19).values(
                    placement_catalog={
                        'schema_version': 1,
                        'entries': [],
                        'num_nodes': 2,
                    }))
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1,
                 context='phx',
                 physical_uid='uid-phx',
                 kueue=True)

    with pytest.raises(zero_cost_actuation.ZeroCostActuationConflict,
                       match='num_nodes == 1'):
        _grant_plan(repository, plan, max_capacity=1)

    with actuation_database.connect() as connection:
        intent_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table)).scalar_one()
        lane_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                kueue_lane_lineage_schema.serve_kueue_admissions_table)
        ).scalar_one()
    assert intent_count == 0
    assert lane_count == 0


@pytest.mark.parametrize('accelerator_count', [1, 8])
def test_kueue_grant_allows_one_exact_shape_physical_replacement_surge(
        actuation_database, accelerator_count) -> None:
    config = migration_utils.get_alembic_config(actuation_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1,
                 accelerator_count=accelerator_count,
                 context='phx',
                 physical_uid='uid-phx',
                 kueue=True)
    _insert_paid_replica_for_shape(actuation_database, plan.intents[0])

    receipt = _grant_plan(repository, plan, max_capacity=1)

    assert len(receipt.accepted) == 1
    assert not receipt.deferred
    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    with actuation_database.connect() as connection:
        row = connection.execute(sqlalchemy.select(admissions)).mappings().one()
    # The exception is one physical Pod even when that Pod exposes eight GPUs.
    assert row['replacement_surge_units'] == 1
    assert row['replacement_compatibility_sha256'] is not None


def test_kueue_replacement_surge_rejects_cross_shape_paid_capacity(
        actuation_database) -> None:
    config = migration_utils.get_alembic_config(actuation_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    paid_shape = _plan(free_slots=1,
                       accelerator_count=1,
                       context='phx',
                       physical_uid='uid-paid',
                       kueue=True)
    candidate = _plan(free_slots=1,
                      accelerator_count=8,
                      context='phx',
                      physical_uid='uid-reserved',
                      kueue=True)
    _insert_paid_replica_for_shape(actuation_database, paid_shape.intents[0])

    receipt = _grant_plan(repository, candidate, max_capacity=1)

    assert not receipt.accepted
    assert len(receipt.deferred) == 1
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)


def test_kueue_replacement_surge_is_service_wide_and_cannot_chain(
        actuation_database) -> None:
    config = migration_utils.get_alembic_config(actuation_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    first = _plan(free_slots=1,
                  context='phx',
                  physical_uid='uid-phx-a',
                  kueue=True)
    second = _plan(free_slots=1,
                   context='phx',
                   physical_uid='uid-phx-b',
                   kueue=True)
    _insert_paid_replica_for_shape(actuation_database, first.intents[0])
    assert len(_grant_plan(repository, first, max_capacity=1).accepted) == 1

    receipt = _grant_plan(repository, second, max_capacity=1)

    assert not receipt.accepted
    assert len(receipt.deferred) == 1
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)


def test_kueue_replacement_surge_releases_only_after_provider_clean_evidence(
        actuation_database) -> None:
    config = migration_utils.get_alembic_config(actuation_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    first_repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1,
                 context='phx',
                 physical_uid='uid-phx',
                 kueue=True)
    _insert_paid_replica_for_shape(actuation_database, plan.intents[0])
    assert len(_grant_plan(first_repository, plan,
                           max_capacity=1).accepted) == 1
    admissions = kueue_lane_lineage_schema.serve_kueue_admissions_table
    replicas = serve_state_schema.replicas_table

    # Lifecycle intent is not cleanup evidence; the durable lease survives.
    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(replicas).where(
                replicas.c.service_name == 'svc',
                replicas.c.replica_id == 900).values(status='SHUTTING_DOWN'))
    restarted = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    _grant_plan(restarted, plan, max_capacity=1)
    with actuation_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(
                admissions.c.replacement_surge_units)).scalar_one() == 1

    # A scalar successful sky.down status is not an evidence-clean graph.
    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(replicas).where(
                replicas.c.service_name == 'svc',
                replicas.c.replica_id == 900).values(
                    sky_down_status=common_utils.ProcessStatus.SUCCEEDED.value))
    _grant_plan(restarted, plan, max_capacity=1)
    with actuation_database.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(
                admissions.c.replacement_surge_units)).scalar_one() == 1

    # Exact removal of the paid victim row is the provider-clean boundary.
    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(replicas).where(replicas.c.service_name == 'svc',
                                              replicas.c.replica_id == 900))
    _grant_plan(restarted, plan, max_capacity=1)
    with actuation_database.connect() as connection:
        row = connection.execute(sqlalchemy.select(admissions)).mappings().one()
    assert row['replacement_surge_units'] == 0
    assert row['replacement_compatibility_sha256'] is None


def test_kueue_grant_atomically_replaces_provider_free_expired_sentinel(
        actuation_database) -> None:
    config = migration_utils.get_alembic_config(actuation_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    predecessor = _plan(free_slots=1,
                        context='phx',
                        physical_uid='uid-phx',
                        kueue=True,
                        valid_until=time.time() + 0.3)
    first = _grant_plan(repository, predecessor, max_capacity=1)
    assert len(first.accepted) == 1
    time.sleep(0.4)
    successor = _plan(free_slots=1,
                      context='phx',
                      physical_uid='uid-phx',
                      kueue=True,
                      valid_until=time.time() + 60)

    second = _grant_plan(repository, successor, max_capacity=1)

    assert [item.intent_idempotency_key for item in second.accepted
           ] == [successor.intents[0].idempotency_key]
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    lineages = kueue_lane_lineage_schema.serve_kueue_admissions_table
    with actuation_database.connect() as connection:
        intent_rows = connection.execute(
            sqlalchemy.select(intents).order_by(
                intents.c.intent_idempotency_key)).mappings().all()
        lineage_rows = connection.execute(
            sqlalchemy.select(lineages)).mappings().all()
    assert len(intent_rows) == 2
    assert {row['state'] for row in intent_rows} == {'GRANTED', 'TERMINAL'}
    assert len(lineage_rows) == 1
    assert lineage_rows[0]['intent_idempotency_key'] == (
        successor.intents[0].idempotency_key)
    assert lineage_rows[0]['state'] == 'INTENT_PENDING'


def test_kueue_expired_replay_garbage_collects_provider_free_admission(
        actuation_database) -> None:
    config = migration_utils.get_alembic_config(actuation_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1,
                 context='phx',
                 physical_uid='uid-phx',
                 kueue=True,
                 valid_until=time.time() + 0.3)
    _grant_plan(repository, plan, max_capacity=1)
    time.sleep(0.4)

    replay = _grant_plan(repository, plan, max_capacity=1)

    assert not replay.accepted
    assert len(replay.deferred) == 1
    lineages = kueue_lane_lineage_schema.serve_kueue_admissions_table
    with actuation_database.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(lineages)).mappings().one_or_none()
    assert row is None


def test_kueue_expired_association_witness_retains_capacity_debit(
        actuation_database) -> None:
    config = migration_utils.get_alembic_config(actuation_database,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '057')
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    predecessor = _plan(free_slots=1,
                        context='phx',
                        physical_uid='uid-phx',
                        kueue=True,
                        valid_until=time.time() + 0.3)
    _grant_plan(repository, predecessor, max_capacity=2)
    with actuation_database.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        controller = connection.execute(
            sqlalchemy.select(
                serve_state_schema.services_table.c.controller_incarnation,
                serve_state_schema.services_table.c.controller_owner_epoch).
            where(serve_state_schema.services_table.c.name ==
                  'svc')).mappings().one()
        association_table = (
            ordinary_launch_binding.ordinary_launch_associations_table)
        # This test isolates provider-free lineage GC.  The generic binding
        # trigger suite is covered separately; disabling it lets us install
        # the exact association witness whose mere existence must fail GC.
        connection.exec_driver_sql(
            f'ALTER TABLE {association_table.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.insert(association_table).values(
                association_id=uuid.uuid4(),
                submission_id=uuid.uuid4(),
                tenant_scope='tenant-a',
                service_name='svc',
                service_hash=_SERVICE_HASH,
                service_workspace='workspace-a',
                service_lifecycle_epoch=3,
                service_binding_epoch=1,
                service_version=19,
                replica_id=1,
                replica_record_id=uuid.uuid4(),
                launch_generation=1,
                cluster_name='svc-1',
                request_id=f'request-{uuid.uuid4()}',
                input_digest='a' * 64,
                owner_controller_incarnation=(
                    controller['controller_incarnation']),
                owner_controller_epoch=(controller['controller_owner_epoch']),
                effect_phase='NOT_STARTED',
                effect_phase_changed_at=now,
                resolution='BOUND',
                created_at=now,
                updated_at=now,
                binding_protocol_version=2,
                profile_kind='RESERVED_FILL',
                profile_version=1,
                profile_digest='b' * 64,
                capability_cohort_epoch=1,
                capability_profile_set_digest='c' * 64,
                receipt_protocol_version=1,
                authorization_kind=('RESERVED_FILL_ALLOCATION'),
                authorization_reference=(
                    'reserved-fill:' + predecessor.intents[0].idempotency_key),
                authorization_generation=1,
                authorization_digest='d' * 64,
                reconciliation_outcome='ACTIVE_ADOPT',
                provider_evidence='NOT_QUERIED'))
        connection.exec_driver_sql(
            f'ALTER TABLE {association_table.name} ENABLE TRIGGER USER')
    time.sleep(0.4)
    successor = _plan(free_slots=1,
                      context='phx',
                      physical_uid='uid-phx',
                      kueue=True,
                      valid_until=time.time() + 60)

    blocked = _grant_plan(repository, successor, max_capacity=1)

    assert not blocked.accepted
    assert len(blocked.deferred) == 1
    assert blocked.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with actuation_database.connect() as connection:
        predecessor_state = connection.execute(
            sqlalchemy.select(intents.c.state).where(
                intents.c.intent_idempotency_key ==
                predecessor.intents[0].idempotency_key)).scalar_one()
    assert predecessor_state == 'TERMINAL'

    second = _grant_plan(repository, successor, max_capacity=2)

    assert [item.intent_idempotency_key for item in second.accepted
           ] == [successor.intents[0].idempotency_key]
    assert not second.deferred
    lineages = kueue_lane_lineage_schema.serve_kueue_admissions_table
    with actuation_database.connect() as connection:
        rows = connection.execute(sqlalchemy.select(lineages)).mappings().all()
    assert {row['intent_idempotency_key'] for row in rows} == {
        predecessor.intents[0].idempotency_key,
        successor.intents[0].idempotency_key,
    }
    assert {row['state'] for row in rows} == {'INTENT_PENDING'}


def test_grant_locks_exclude_terminal_intents_but_retain_replica_rows(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    _grant_plan(repository, plan, max_capacity=1)
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    replicas = serve_state_schema.replicas_table
    with actuation_database.begin() as connection:
        base = dict(
            connection.execute(sqlalchemy.select(intents)).mappings().one())
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        terminal_rows = []
        used_keys = {base['intent_idempotency_key']}
        for ordinal in range(256):
            key = f'{ordinal + 1:064x}'
            while key in used_keys:
                key = f'{int(key, 16) + 256:064x}'
            used_keys.add(key)
            row = dict(base)
            row.update(intent_idempotency_key=key,
                       state='TERMINAL',
                       last_error='retained_history',
                       updated_at=now,
                       terminal_at=now)
            terminal_rows.append(row)
        connection.execute(sqlalchemy.insert(intents), terminal_rows)
        for replica_id in range(1, 33):
            cleaned = _paid_replica_for_shape(plan.intents[0], replica_id)
            cleaned.status_property.sky_down_status = (
                common_utils.ProcessStatus.SUCCEEDED)
            cleaned.status_property.is_scale_down = True
            connection.execute(
                sqlalchemy.insert(replicas).values(
                    **serve_state._replica_row_values('svc', replica_id,
                                                      cleaned)))

    selected_terminal_key = terminal_rows[0]['intent_idempotency_key']
    excluded_terminal_key = terminal_rows[-1]['intent_idempotency_key']
    with actuation_database.connect() as locking_connection:
        transaction = locking_connection.begin()
        try:
            rows = zero_cost_actuation._locked_grant_intent_rows(
                locking_connection,
                service_name='svc',
                plan_keys=(selected_terminal_key,))
            assert {row['intent_idempotency_key'] for row in rows
                   } == {base['intent_idempotency_key'], selected_terminal_key}
            locked_replica_capacity = getattr(zero_cost_actuation,
                                              '_locked_replica_capacity')
            # pylint: disable-next=missing-kwoa
            replica_capacity = locked_replica_capacity(
                locking_connection,
                service_name='svc',
                capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)
            assert replica_capacity.total == 32
            assert replica_capacity.nonretiring_paid_by_shape == {}
            # Unrelated terminal intent history remains unlocked, but a
            # retained replica row is still provider-possible even when its
            # scalar sky.down status says SUCCEEDED.
            with actuation_database.connect() as contender:
                contender_transaction = contender.begin()
                contender.execute(
                    sqlalchemy.select(intents.c.intent_idempotency_key).where(
                        intents.c.intent_idempotency_key ==
                        excluded_terminal_key).with_for_update(
                            nowait=True)).one()
                with pytest.raises(sqlalchemy.exc.OperationalError):
                    contender.execute(
                        sqlalchemy.select(replicas.c.replica_id).where(
                            replicas.c.service_name == 'svc',
                            replicas.c.replica_id == 32).with_for_update(
                                nowait=True)).one()
                contender_transaction.rollback()
        finally:
            transaction.rollback()


def test_status_summary_keeps_intents_separate_from_replicas(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=2)
    _grant_plan(repository, plan, max_capacity=2)

    summary = zero_cost_actuation.get_status_summary('svc',
                                                     _SERVICE_HASH,
                                                     engine=actuation_database)

    assert summary == {
        'zero_cost_actuation_status': 'available',
        'zero_cost_actuation_reason': 'complete',
        'zero_cost_actuation_mode': 'DURABLE_INTENT',
        'zero_cost_actuation_epoch': 1,
        'zero_cost_actuation_state_counts': {
            'GRANTED': 2,
            'ACTUATING': 0,
            'COMMITTED': 0,
            'RETRYABLE': 0,
            'TERMINAL': 0,
        },
        'pending_zero_cost_actuation_count': 2,
    }
    assert zero_cost_actuation.get_status_summary(
        'svc', 'stale-hash', engine=actuation_database
    ) == zero_cost_actuation.unavailable_status_summary('service_hash_mismatch')


def test_capability_advertisement_does_not_promote_service(
        actuation_database) -> None:
    services = serve_state_schema.services_table
    with actuation_database.begin() as connection:
        incarnation = connection.execute(
            sqlalchemy.select(services.c.controller_incarnation).where(
                services.c.name == 'svc')).scalar_one()
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                reserved_fill_actuation_mode='DIRECT_REPLICA',
                reserved_fill_actuation_epoch=0,
                reserved_fill_actuation_capable=False,
                reserved_fill_actuation_controller_incarnation=None,
                reserved_fill_actuation_protocol_version=None))

    mode = zero_cost_actuation.advertise_capability('svc',
                                                    incarnation,
                                                    engine=actuation_database)

    with actuation_database.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == 'svc')).mappings().one()
    assert mode is zero_cost_actuation.ActuationMode.DIRECT_REPLICA
    assert service['reserved_fill_actuation_mode'] == 'DIRECT_REPLICA'
    assert service['reserved_fill_actuation_epoch'] == 0
    assert service['reserved_fill_actuation_capable'] is True
    assert (service['reserved_fill_actuation_controller_incarnation'] ==
            incarnation)
    assert service['reserved_fill_actuation_protocol_version'] == 1


def test_promotion_requires_fleet_barrier_and_is_one_way(
        actuation_database) -> None:
    services = serve_state_schema.services_table
    with actuation_database.begin() as connection:
        incarnation = connection.execute(
            sqlalchemy.select(services.c.controller_incarnation).where(
                services.c.name == 'svc')).scalar_one()
        connection.execute(
            sqlalchemy.update(services).where(services.c.name == 'svc').values(
                reserved_fill_actuation_mode='DIRECT_REPLICA',
                reserved_fill_actuation_epoch=0,
                route_source_mode='DURABLE_PROJECTED',
                route_source_epoch=1,
                route_projection_capable=True,
                route_projection_controller_incarnation=incarnation,
                route_projection_protocol_version=2,
                demand_source_mode='DURABLE_FEED',
                demand_source_epoch=1,
                demand_authority_capable=True,
                demand_authority_controller_incarnation=incarnation,
                demand_authority_protocol_version=1))
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

    with pytest.raises(zero_cost_actuation.ZeroCostActuationUnavailable):
        with actuation_database.begin() as connection:
            zero_cost_actuation.promote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=incarnation,
                expected_actuation_epoch=0,
                participant_barrier_passed=False)
    with actuation_database.begin() as connection:
        epoch = zero_cost_actuation.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            expected_actuation_epoch=0,
            participant_barrier_passed=True)
    assert epoch == 1
    with actuation_database.connect() as connection:
        service = connection.execute(
            sqlalchemy.select(services).where(
                services.c.name == 'svc')).mappings().one()
    assert service['reserved_fill_actuation_mode'] == 'DURABLE_INTENT'
    assert service['reserved_fill_actuation_epoch'] == 1
    with actuation_database.begin() as connection:
        assert zero_cost_actuation.promote_service_in_connection(
            connection,
            service_name='svc',
            controller_incarnation=incarnation,
            expected_actuation_epoch=0,
            participant_barrier_passed=False) == 1
    with pytest.raises(zero_cost_actuation.ZeroCostActuationConflict):
        with actuation_database.begin() as connection:
            zero_cost_actuation.promote_service_in_connection(
                connection,
                service_name='svc',
                controller_incarnation=incarnation,
                expected_actuation_epoch=1,
                participant_barrier_passed=True)


def test_pending_grants_enforce_headroom_and_debit_paid_residual(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=2)
    receipt = _grant_plan(repository, plan, max_capacity=1)
    assert len(receipt.accepted) == 1
    assert len(receipt.deferred) == 1
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)

    with actuation_database.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        pending = zero_cost_actuation.pending_capacity_in_connection(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            service_version=19,
            accounting_cards={'l4'},
            now=now)
    assert pending == {'l4': 1}
    assert capacity_admission._paid_residual({'l4': 2}, {'l4': 0}, pending,
                                             {'l4': 0}) == {
                                                 'l4': 1
                                             }


def test_pending_fill_snapshot_is_global_unit_normalized_and_exact(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1,
                 accelerator_count=8,
                 capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)
    receipt = _grant_plan(repository, plan, max_capacity=8)
    assert len(receipt.accepted) == 1
    with actuation_database.begin() as connection:
        row = dict(
            connection.execute(
                sqlalchemy.select(
                    zero_cost_actuation_schema.
                    serve_zero_cost_actuation_intents_table)).mappings().one())
        row.update(
            intent_idempotency_key='f' * 64,
            service_hash='older-service-hash',
            allocation_generation=plan.allocation_generation + 1,
            allocation_input_sha256='f' * 64,
            allocation_claim_generation=plan.allocation_claim_generation + 1,
        )
        connection.execute(
            sqlalchemy.insert(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table).values(**row))

    logical = repository.pending_fill_snapshot(
        service_name='svc',
        service_hash=_SERVICE_HASH,
        allocation_generation=plan.allocation_generation,
        allocation_input_sha256=plan.allocation_input_sha256,
        allocation_claim_generation=plan.allocation_claim_generation,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)
    physical = repository.pending_fill_snapshot(
        service_name='svc',
        service_hash=_SERVICE_HASH,
        allocation_generation=plan.allocation_generation,
        allocation_input_sha256=plan.allocation_input_sha256,
        allocation_claim_generation=plan.allocation_claim_generation,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)

    # Both current and old-incarnation grants consume service-global headroom,
    # but only the exact current allocation becomes a planner replay debit.
    assert logical.capacity == 16
    assert physical.capacity == 2
    assert logical.debits == physical.debits
    assert len(logical.debits) == 1
    assert logical.debits[0].replica_slots == 1


def test_grant_ceiling_counts_materialized_old_version(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    old = _paid_replica_for_shape(plan.intents[0], 1)
    old.version = plan.intents[0].service_version - 1
    old.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **serve_state._replica_row_values('svc', 1, old)))

    receipt = _grant_plan(repository, plan, max_capacity=1)

    assert not receipt.accepted
    assert len(receipt.deferred) == 1
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)


def test_logical_grant_ceiling_projects_old_physical_row_from_exact_shape(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    physical_plan = _plan(
        free_slots=1,
        accelerator_count=8,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)
    old = _paid_replica_for_shape(physical_plan.intents[0], 1)
    old.version = physical_plan.intents[0].service_version - 1
    assert old.planned_capacity == 1
    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **serve_state._replica_row_values('svc', 1, old)))

    logical_plan = _plan(
        free_slots=1,
        accelerator_count=8,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)
    receipt = _grant_plan(repository, logical_plan, max_capacity=8)

    assert not receipt.accepted
    assert len(receipt.deferred) == 1
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)


def test_logical_grant_ceiling_rejects_conflicting_persisted_shapes(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    physical_plan = _plan(
        free_slots=1,
        accelerator_count=8,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.PHYSICAL)
    old = _paid_replica_for_shape(physical_plan.intents[0], 1)
    assert old.resources_override is not None
    old.resources_override['accelerators'] = {'A100': 8}
    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                **serve_state._replica_row_values('svc', 1, old)))

    logical_plan = _plan(
        free_slots=1,
        accelerator_count=8,
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)
    with pytest.raises(zero_cost_actuation.ZeroCostActuationConflict,
                       match='conflicting accelerator shapes'):
        _grant_plan(repository, logical_plan, max_capacity=16)


@pytest.mark.parametrize(('storage_version', 'replica_info_version'), [(2, 18),
                                                                       (1, 17)])
def test_logical_capacity_rejects_noncanonical_replica_state_version(
        storage_version, replica_info_version) -> None:
    state = {
        'replica_info_version': replica_info_version,
        'resources_override': {
            'accelerators': {
                'H200': 8
            }
        },
        'location': None,
    }

    with pytest.raises(zero_cost_actuation.ZeroCostActuationConflict,
                       match='replica state is malformed'):
        zero_cost_actuation._replica_capacity_for_unit(
            storage_version, state,
            reserved_fill_planner.FillCapacityUnit.LOGICAL)


def test_grant_ceiling_retains_status_only_cleanup_row_until_removed(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    cleaned = _paid_replica_for_shape(plan.intents[0], 1)
    cleaned.version = plan.intents[0].service_version - 1
    cleaned.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    cleaned.status_property.sky_down_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    cleaned.status_property.is_scale_down = True
    with actuation_database.begin() as connection:
        values = serve_state._replica_row_values('svc', 1, cleaned)
        assert values['sky_down_status'] == 'SUCCEEDED'
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**values))

    retained = _grant_plan(repository, plan, max_capacity=1)

    assert not retained.accepted
    assert len(retained.deferred) == 1
    assert retained.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)

    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.delete(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 1))
    receipt = _grant_plan(repository, plan, max_capacity=1)

    assert len(receipt.accepted) == 1
    assert not receipt.deferred


def test_grant_ceiling_counts_uncommitted_shutting_down_capacity(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    retiring = _paid_replica_for_shape(plan.intents[0], 1)
    retiring.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    retiring.status_property.sky_down_status = (
        common_utils.ProcessStatus.SCHEDULED)
    retiring.status_property.is_scale_down = True
    retiring.status_property.wait_for_idle_before_termination = True
    retiring.status_property.logical_retirement_version = retiring.version
    retiring.status_property.logical_retirement_controller_epoch = 'epoch'
    retiring.status_property.logical_retirement_generation = 7
    retiring.status_property.logical_retirement_target_capacity = 0
    retiring.status_property.logical_retirement_confirmed_generation = 7
    retiring.status_property.logical_retirement_bounded_deadline = False
    retiring.status_property.logical_retirement_committed = False
    with actuation_database.begin() as connection:
        values = serve_state._replica_row_values('svc', 1, retiring)
        assert values['status'] == 'SHUTTING_DOWN'
        assert values['sky_down_status'] == 'SCHEDULED'
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**values))

    receipt = _grant_plan(repository, plan, max_capacity=1)

    assert not receipt.accepted
    assert len(receipt.deferred) == 1
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)


@pytest.mark.parametrize(('utilization_gate', 'expected_intents'), [(False, 2),
                                                                    (True, 0)],
                         ids=('full-backfill', 'idle-gated'))
def test_idle_gate_controls_width_adjusted_durable_intents_without_paid_spill(
        actuation_database, monkeypatch, utilization_gate: bool,
        expected_intents: int) -> None:
    spec = service_spec.SkyServiceSpec(readiness_path='/health',
                                       initial_delay_seconds=0,
                                       readiness_timeout_seconds=5,
                                       endpoint_probe_interval_seconds=1,
                                       lb_stream_timeout_seconds=10,
                                       min_replicas=0,
                                       max_replicas=16,
                                       target_concurrency_per_replica=1,
                                       reserved_capacity_fill={
                                           'floor_replicas': 0,
                                           'weight': 100,
                                           'utilization_gate': utilization_gate,
                                       })
    rendered_spec = spec.to_yaml_config()
    assert rendered_spec['replica_policy']['min_replicas'] == 0
    assert rendered_spec['replica_policy']['reserved_capacity_fill'] == {
        'weight': 100.0,
        'utilization_gate': utilization_gate,
    }
    spec = service_spec.SkyServiceSpec.from_yaml_config(rendered_spec)
    assert spec.min_replicas == 0
    assert spec.reserved_fill_floor_replicas == 0
    assert spec.reserved_fill_utilization_gate is utilization_gate

    raw_capacity = pool_capacity_observation.PoolCapacitySuccess.from_counts(
        16, {'L4': 16})
    slots_by_accelerator = dict(raw_capacity.slot_counts(8))
    assert slots_by_accelerator == {'l4': 2}
    available_slots = sum(slots_by_accelerator.values())

    claims = {
        'svc': reserved_capacity_broker.ClaimInput(
            floor=spec.reserved_fill_floor_replicas,
            weight=spec.reserved_fill_weight,
            holdings_fill=0,
            launchable=True,
            effective_cap=available_slots)
    }
    monkeypatch.delenv(serve_constants.RESERVED_FILL_UTILIZATION_GATE_ENV_VAR,
                       raising=False)
    gated_claims, _ = reserved_capacity_broker._apply_utilization_gate(
        claims, {
            'svc': reserved_capacity_broker.ActivityInput(
                armed=spec.reserved_fill_utilization_gate,
                demonstrated_need=0,
                boot_hold=False,
                blind=not spec.reserved_fill_utilization_gate)
        }, {}, 1000.0)
    entitlement = reserved_capacity_broker.compute_entitlements(
        available_slots, gated_claims)['svc']
    assert entitlement == expected_intents

    plan = _plan(free_slots=entitlement, accelerator_count=8)
    assert len(plan.intents) == expected_intents
    assert all(intent.accelerator_count == 8 for intent in plan.intents)
    assert all(
        location.cloud.casefold() == 'kubernetes' and not location.use_spot
        for intent in plan.intents
        for location in intent.allowed_locations)

    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    receipt = _grant_plan(repository, plan, max_capacity=2)
    assert len(receipt.accepted) == expected_intents
    assert not receipt.deferred

    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with actuation_database.begin() as connection:
        intent_rows = connection.execute(
            sqlalchemy.select(intents)).mappings().all()
        replica_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one()
        paid_claim_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.paid_capacity_claims_table)).scalar_one()
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        pending = zero_cost_actuation.pending_capacity_in_connection(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            service_version=19,
            accounting_cards={'l4'},
            now=now)

    assert len(intent_rows) == expected_intents
    assert {row['state'] for row in intent_rows
           } == ({'GRANTED'} if expected_intents else set())
    assert replica_count == 0
    assert paid_claim_count == 0
    assert pending == {'l4': expected_intents}
    assert capacity_admission._paid_residual({'l4': expected_intents},
                                             {'l4': 0}, pending,
                                             {'l4': 0}) == {}
    assert capacity_admission._paid_residual({'l4': 0}, {'l4': 0}, pending,
                                             {'l4': 0}) == {}


def test_pool_leases_are_independent_and_retryable(actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    east = _plan(free_slots=1, context='east', physical_uid='uid-east')
    west = _plan(free_slots=1, context='west', physical_uid='uid-west')
    _grant_plan(repository, east, max_capacity=2)
    _grant_plan(repository, west, max_capacity=2)
    _install_fresh_provider_proofs(actuation_database,
                                   east.intents + west.intents)
    owner = uuid.uuid4()

    east_lease = repository.lease_next(service_name='svc',
                                       pool_key=east.intents[0].pool_key,
                                       owner=owner,
                                       lease_seconds=30)
    west_lease = repository.lease_next(service_name='svc',
                                       pool_key=west.intents[0].pool_key,
                                       owner=owner,
                                       lease_seconds=30)

    assert east_lease is not None
    assert west_lease is not None
    assert repository.lease_next(service_name='svc',
                                 pool_key=east.intents[0].pool_key,
                                 owner=owner,
                                 lease_seconds=30) is None
    assert repository.release_retryable(east_lease, 'provider_busy')
    retried = repository.lease_next(service_name='svc',
                                    pool_key=east.intents[0].pool_key,
                                    owner=owner,
                                    lease_seconds=30)
    assert retried is not None
    assert retried.generation == east_lease.generation + 1


def test_proof_blackout_parks_only_its_exact_pool_and_resumes(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    east = _plan(free_slots=1, context='east', physical_uid='uid-east')
    west = _plan(free_slots=1, context='west', physical_uid='uid-west')
    _grant_plan(repository, east, max_capacity=2)
    _grant_plan(repository, west, max_capacity=2)
    _install_fresh_provider_proofs(actuation_database,
                                   east.intents + west.intents)
    proof_table = (reserved_fill_reclaim_proof_schema.
                   serve_reserved_fill_reclaim_provider_proofs_table)
    with actuation_database.begin() as connection:
        connection.execute(
            sqlalchemy.update(proof_table).where(
                proof_table.c.kubernetes_context == 'west').values(
                    completed_at=(sqlalchemy.func.clock_timestamp() -
                                  datetime.timedelta(seconds=11))))

    assert repository.actionable_pool_keys(
        service_name='svc') == (east.intents[0].pool_key,)
    assert repository.lease_next(service_name='svc',
                                 pool_key=west.intents[0].pool_key,
                                 owner=uuid.uuid4(),
                                 lease_seconds=30) is None
    with actuation_database.connect() as connection:
        west_row = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table).where(
                    zero_cost_actuation_schema.
                    serve_zero_cost_actuation_intents_table.c.kubernetes_context
                    == 'west')).mappings().one()
    assert west_row['state'] == 'GRANTED'
    assert west_row['lease_generation'] == 0

    _install_fresh_provider_proofs(actuation_database, west.intents)
    assert set(repository.actionable_pool_keys(service_name='svc')) == {
        east.intents[0].pool_key, west.intents[0].pool_key
    }
    west_lease = repository.lease_next(service_name='svc',
                                       pool_key=west.intents[0].pool_key,
                                       owner=uuid.uuid4(),
                                       lease_seconds=30)
    assert west_lease is not None


def _replica_for_intent(intent: reserved_fill_planner.FillIntent,
                        replica_id: int) -> replica_managers.ReplicaInfo:
    location = intent.allowed_locations[0].to_location()
    info = replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=f'svc-{replica_id}',
        replica_port='8080',
        is_spot=False,
        location=location,
        version=intent.service_version,
        resources_override=location.to_dict(),
        planned_capacity=intent.capacity_unit.intent_cost(
            intent.accelerator_count))
    info.reserved_fill = True
    info.is_zero_cost = True
    info.reserved_fill_pool_key = intent.pool_key
    info.reserved_fill_service_generation = intent.service_generation
    info.reserved_fill_physical_cluster_uid = intent.physical_cluster_uid
    info.reserved_fill_kubernetes_context = intent.allowed_locations[0].region
    info.reserved_fill_allocation_generation = intent.allocation_generation
    info.reserved_fill_allocation_input_sha256 = intent.allocation_input_sha256
    info.reserved_fill_allocation_claim_generation = (
        intent.allocation_claim_generation)
    info.reserved_fill_reconciliation_gate_generation = (
        intent.reconciliation_gate_generation)
    info.reserved_fill_reclaim_fleet_bundle_sha256 = (
        intent.reclaim_fleet_bundle_sha256)
    info.reserved_fill_reclaim_policy_revision = intent.reclaim_policy_revision
    info.reserved_fill_reclaim_provider_inventory_sha256 = (
        intent.reclaim_provider_inventory_sha256)
    info.reserved_fill_worker_projection_sha256 = (
        intent.worker_projection_sha256)
    info.reserved_fill_observation_generation = intent.observation_generation
    info.reserved_fill_observation_sequence = intent.observation_sequence
    info.reserved_fill_intent_idempotency_key = intent.idempotency_key
    return info


def _commit_and_insert_replica(
    connection: sqlalchemy.engine.Connection,
    lease: zero_cost_actuation.IntentLease,
    info: replica_managers.ReplicaInfo,
) -> None:
    record_id = uuid.UUID(info.replica_record_id)
    zero_cost_actuation.commit_lease_in_connection(connection,
                                                   lease,
                                                   service_name='svc',
                                                   replica_id=info.replica_id,
                                                   replica_record_id=record_id,
                                                   replica_info=info)
    values = serve_state._reserved_fill_replica_row_values(
        'svc',
        info.replica_id,
        info,
        pool_key=lease.intent.pool_key,
        expected_protocol_version=reserved_capacity_broker.PROTOCOL_V2)
    assert values is not None
    connection.execute(
        sqlalchemy.insert(serve_state_schema.replicas_table).values(**values))
    service = connection.execute(
        sqlalchemy.select(serve_state_schema.services_table).where(
            serve_state_schema.services_table.c.name ==
            'svc')).mappings().one()
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
        authorization_reference=(
            f'reserved-fill:{lease.intent.idempotency_key}'),
        authorization_generation=lease.intent.allocation_generation,
        authorization_payload={
            'intent_idempotency_key': lease.intent.idempotency_key,
        })
    intent = ordinary_launch_binding.BindingIntent(
        service_name='svc',
        service_hash=_SERVICE_HASH,
        service_version=info.version,
        replica_id=info.replica_id,
        replica_record_id=record_id,
        lifecycle_epoch=3,
        binding_epoch=service['ordinary_launch_binding_epoch'],
        controller_incarnation=service['controller_incarnation'],
        controller_owner_epoch=service['controller_owner_epoch'],
        controller_pid=_CONTROLLER_PID,
        controller_ip=_CONTROLLER_IP)
    identity = ordinary_launch_binding.build_non_pool_binding_identity(
        intent,
        submission_id=uuid.uuid5(uuid.NAMESPACE_URL,
                                 lease.intent.idempotency_key),
        tenant_scope='tenant-a',
        service_workspace='workspace-a',
        cluster_name=info.cluster_name,
        input_digest='a' * 64,
        profile=profile,
        capability_cohort_epoch=(
            service['non_pool_launch_capability_cohort_epoch']),
        capability_profile_set_digest=(
            service['non_pool_launch_capability_profile_set_digest']),
        receipt_protocol_version=(
            service['non_pool_launch_receipt_protocol_version']))
    association_values = ordinary_launch_binding._identity_values(
        identity, 1, paid_capacity_pool_key=None)
    association_values.update({
        'owner_controller_incarnation': service['controller_incarnation'],
        'owner_controller_epoch': service['controller_owner_epoch'],
        'owner_revision': 1,
        'effect_phase': ordinary_launch_binding.EffectPhase.NOT_STARTED.value,
        'resolution': ordinary_launch_binding.Resolution.BOUND.value,
        'updated_at': sqlalchemy.func.clock_timestamp(),
        'reconciliation_outcome':
            (ordinary_launch_binding.ReconciliationOutcome.ACTIVE_ADOPT.value),
        'provider_evidence':
            (ordinary_launch_binding.ProviderEvidence.NOT_QUERIED.value),
    })
    connection.execute(
        sqlalchemy.insert(
            ordinary_launch_binding.ordinary_launch_associations_table).values(
                **association_values))
    connection.execute(
        sqlalchemy.update(serve_state_schema.replicas_table).where(
            serve_state_schema.replicas_table.c.service_name == 'svc',
            serve_state_schema.replicas_table.c.replica_id == info.replica_id).
        values(ordinary_launch_association_id=identity.association_id))


def _retain_as_pre_serve056_json_only_replica(
    connection: sqlalchemy.engine.Connection,
    *,
    replica_id: int,
) -> None:
    """Project a committed graph into the exact retained legacy shape."""
    replicas = serve_state_schema.replicas_table
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    association_id = connection.execute(
        sqlalchemy.select(replicas.c.ordinary_launch_association_id).where(
            replicas.c.service_name == 'svc',
            replicas.c.replica_id == replica_id)).scalar_one()
    # Serve056 deliberately does not normalize historical JSON-only rows.
    # Install that migration input explicitly; fresh rows are covered by the
    # canonical graph helper above and must never bypass these triggers.
    for table in (replicas, associations):
        connection.exec_driver_sql(
            f'ALTER TABLE {table.name} DISABLE TRIGGER USER')
    connection.execute(
        sqlalchemy.update(replicas).where(
            replicas.c.service_name == 'svc',
            replicas.c.replica_id == replica_id).values(
                ordinary_launch_association_id=None,
                reserved_fill_intent_idempotency_key=None))
    connection.execute(
        sqlalchemy.delete(associations).where(
            associations.c.association_id == association_id))
    for table in (replicas, associations):
        connection.exec_driver_sql(
            f'ALTER TABLE {table.name} ENABLE TRIGGER USER')


def test_replica_and_intent_commit_in_one_transaction(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    _grant_plan(repository, plan, max_capacity=1)
    _install_fresh_provider_proofs(actuation_database, plan.intents)
    lease = repository.lease_next(service_name='svc',
                                  pool_key=plan.intents[0].pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _replica_for_intent(lease.intent, 1)
    record_id = uuid.UUID(info.replica_record_id)

    with actuation_database.begin() as connection:
        _commit_and_insert_replica(connection, lease, info)

    replay = _grant_plan(repository, plan, max_capacity=1)

    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with actuation_database.connect() as connection:
        row = connection.execute(sqlalchemy.select(intents)).mappings().one()
    assert replay.accepted == (reserved_fill_planner.AcceptedFillIntent(
        plan.intents[0].idempotency_key, 1),)
    assert not replay.deferred
    assert row['state'] == 'COMMITTED'
    assert row['replica_id'] == 1
    assert row['replica_record_id'] == record_id


def test_pre_serve056_json_only_replica_is_cleanup_only(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    _grant_plan(repository, plan, max_capacity=1)
    _install_fresh_provider_proofs(actuation_database, plan.intents)
    lease = repository.lease_next(service_name='svc',
                                  pool_key=plan.intents[0].pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _replica_for_intent(lease.intent, 1)
    with actuation_database.begin() as connection:
        _commit_and_insert_replica(connection, lease, info)
    with actuation_database.begin() as connection:
        _retain_as_pre_serve056_json_only_replica(connection, replica_id=1)

    with actuation_database.connect() as connection:
        assert zero_cost_actuation.committed_intent_for_replica_in_connection(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            replica_info=info) is None
        cleanup_intent = (
            zero_cost_actuation.
            cleanup_only_committed_intent_for_replica_in_connection(
                connection,
                service_name='svc',
                service_hash=_SERVICE_HASH,
                replica_info=info))
        assert cleanup_intent == lease.intent
        info.reserved_fill_physical_cluster_uid = 'other-uid'
        assert zero_cost_actuation.committed_intent_for_replica_in_connection(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            replica_info=info) is None
        assert (zero_cost_actuation.
                cleanup_only_committed_intent_for_replica_in_connection(
                    connection,
                    service_name='svc',
                    service_hash=_SERVICE_HASH,
                    replica_info=info) is None)


def test_committed_replica_id_high_water_survives_replica_cleanup(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    _grant_plan(repository, plan, max_capacity=1)
    _install_fresh_provider_proofs(actuation_database, plan.intents)
    lease = repository.lease_next(service_name='svc',
                                  pool_key=plan.intents[0].pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _replica_for_intent(lease.intent, 9)

    with actuation_database.begin() as connection:
        _commit_and_insert_replica(connection, lease, info)
    with actuation_database.begin() as connection:
        replicas = serve_state_schema.replicas_table
        associations = (
            ordinary_launch_binding.ordinary_launch_associations_table)
        association_id = connection.execute(
            sqlalchemy.select(replicas.c.ordinary_launch_association_id).where(
                replicas.c.service_name == 'svc',
                replicas.c.replica_id == 9)).scalar_one()
        # Historical cleanup predates Serve056's immutable handoff graph.  A
        # fresh linked row cannot be deleted; explicitly model only that
        # retained migration input here.
        for table in (replicas, associations):
            connection.exec_driver_sql(
                f'ALTER TABLE {table.name} DISABLE TRIGGER USER')
        connection.execute(
            sqlalchemy.delete(replicas).where(replicas.c.service_name == 'svc',
                                              replicas.c.replica_id == 9))
        connection.execute(
            sqlalchemy.delete(associations).where(
                associations.c.association_id == association_id))
        for table in (replicas, associations):
            connection.exec_driver_sql(
                f'ALTER TABLE {table.name} ENABLE TRIGGER USER')

    assert repository.committed_replica_id_high_water('svc') == 9
    assert repository.committed_replica_id_high_water('other') == 0


def test_intent_mismatch_rolls_back_replica_insert(actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1)
    _grant_plan(repository, plan, max_capacity=1)
    _install_fresh_provider_proofs(actuation_database, plan.intents)
    lease = repository.lease_next(service_name='svc',
                                  pool_key=plan.intents[0].pool_key,
                                  owner=uuid.uuid4(),
                                  lease_seconds=30)
    assert lease is not None
    info = _replica_for_intent(lease.intent, 1)
    info.reserved_fill_physical_cluster_uid = 'different-uid'

    with pytest.raises(zero_cost_actuation.ZeroCostActuationConflict):
        with actuation_database.begin() as connection:
            zero_cost_actuation.commit_lease_in_connection(
                connection,
                lease,
                service_name='svc',
                replica_id=1,
                replica_record_id=uuid.UUID(info.replica_record_id),
                replica_info=info)

    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with actuation_database.connect() as connection:
        replica_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.replicas_table)).scalar_one()
        state = connection.execute(sqlalchemy.select(
            intents.c.state)).scalar_one()
    assert replica_count == 0
    assert state == 'ACTUATING'


def test_expired_retryable_grant_releases_paid_debit(
        actuation_database) -> None:
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    plan = _plan(free_slots=1, valid_until=time.time() + 0.3)
    _grant_plan(repository, plan, max_capacity=1)
    time.sleep(0.4)

    with actuation_database.begin() as connection:
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        pending = zero_cost_actuation.pending_capacity_in_connection(
            connection,
            service_name='svc',
            service_hash=_SERVICE_HASH,
            service_version=19,
            accounting_cards={'l4'},
            now=now)
        state = connection.execute(
            sqlalchemy.select(
                zero_cost_actuation_schema.
                serve_zero_cost_actuation_intents_table.c.state)).scalar_one()
    assert pending == {'l4': 0}
    assert state == 'TERMINAL'


def test_schema_rejects_invalid_state_shape(actuation_database) -> None:
    plan = _plan(free_slots=1)
    repository = zero_cost_actuation.ZeroCostActuationRepository(
        actuation_database)
    _grant_plan(repository, plan, max_capacity=1)
    intents = zero_cost_actuation_schema.serve_zero_cost_actuation_intents_table
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with actuation_database.begin() as connection:
            connection.execute(
                sqlalchemy.update(intents).values(
                    state='COMMITTED',
                    replica_id=1,
                    committed_at=datetime.datetime.now(datetime.timezone.utc)))
