"""Real-PostgreSQL contracts for Serve physical-pool observations."""

# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import
import concurrent.futures
import dataclasses
import json
import os
import subprocess
import threading
import types
import uuid

from alembic import command as alembic_command
import psycopg2
import pytest
import sqlalchemy
from test_reserved_fill_broker_pg import _create_database
from test_reserved_fill_broker_pg import pg_server as _broker_pg_server

from sky import clouds
from sky import exceptions
from sky.serve import ordinary_launch_binding
from sky.serve import pool_capacity_observation as observation
from sky.serve import pool_capacity_observation_schema as observation_schema
from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_reclaim_attestation as reclaim_attestation
from sky.serve import serve_state
from sky.serve import spot_placer
from sky.utils import common_utils
from sky.utils.db import migration_utils

_POSTGRES_REQUIRED = os.environ.get('SKYPILOT_REQUIRE_SERVE_POSTGRES') == '1'
_DOCKER_AVAILABLE = subprocess.run(['docker', 'info'],
                                   check=False,
                                   stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL).returncode == 0
try:
    with psycopg2.connect(dbname='postgres'):
        _LOCAL_POSTGRES_AVAILABLE = True
except psycopg2.Error:
    _LOCAL_POSTGRES_AVAILABLE = False
pytestmark = pytest.mark.skipif(not _LOCAL_POSTGRES_AVAILABLE and
                                not _DOCKER_AVAILABLE and
                                not _POSTGRES_REQUIRED,
                                reason=('Docker unavailable; skipping real-'
                                        'PostgreSQL observation tests'))


@pytest.fixture(scope='session')
def pg_server():
    """Expose the broker module's PostgreSQL server fixture locally."""
    yield from _broker_pg_server.__wrapped__()


def _pool_key(*names: str) -> str:
    encoded_names: str | list[str] = (names[0]
                                      if len(names) == 1 else list(names))
    return json.dumps(['v2', 'physical-uid', encoded_names])


def _isolated_engine(request, prefix: str) -> sqlalchemy.engine.Engine:
    database_name = f'{prefix}_{uuid.uuid4().hex[:10]}'
    if _LOCAL_POSTGRES_AVAILABLE:
        admin = psycopg2.connect(dbname='postgres')
        try:
            admin.autocommit = True
            with admin.cursor() as cursor:
                cursor.execute(f'CREATE DATABASE "{database_name}"')
        finally:
            admin.close()
        url = f'postgresql:///{database_name}'
    else:
        pg_server = request.getfixturevalue('pg_server')
        url = _create_database(pg_server, database_name)
    engine = sqlalchemy.create_engine(url)

    def cleanup() -> None:
        engine.dispose()
        if _LOCAL_POSTGRES_AVAILABLE:
            admin = psycopg2.connect(dbname='postgres')
            try:
                admin.autocommit = True
                with admin.cursor() as cursor:
                    cursor.execute(f'DROP DATABASE "{database_name}"')
            finally:
                admin.close()

    request.addfinalizer(cleanup)
    return engine


@pytest.fixture
def observation_engine(request):
    engine = _isolated_engine(request, 'pool_observation')
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    # Exercise the production upgrade shape: the deployed database is already
    # at Serve042, so the upstream projection migration and both new authority
    # migrations must arrive as linear successors rather than by rewriting an
    # applied revision.
    alembic_command.upgrade(config, '042')
    assert 'reconciliation_gate_state' not in {
        column['name'] for column in sqlalchemy.inspect(engine).get_columns(
            'reserved_fill_protocol_state')
    }
    alembic_command.upgrade(config, '043')
    assert 'reconciliation_gate_state' not in {
        column['name'] for column in sqlalchemy.inspect(engine).get_columns(
            'reserved_fill_protocol_state')
    }
    alembic_command.upgrade(config, '044')
    assert {
        'reclaim_fleet_bundle_sha256',
        'reclaim_policy_revision',
        'reclaim_provider_inventory_sha256',
    }.isdisjoint({
        column['name'] for column in sqlalchemy.inspect(engine).get_columns(
            'reserved_fill_protocol_state')
    })
    alembic_command.upgrade(config, '045')
    alembic_command.upgrade(config, '046')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE reserved_fill_protocol_state
                SET protocol_version = 2,
                    image_digest = :image_digest,
                    deployment_generation = '1',
                    deployment_uid = 'deployment-uid',
                    pod_inventory_count = 1,
                    pod_inventory_sha256 = :inventory_sha256
                WHERE id = 1
            """), {
                'image_digest': f"sha256:{'a' * 64}",
                'inventory_sha256': 'b' * 64,
            })
    yield engine


@pytest.fixture
def serve044_engine(request):
    engine = _isolated_engine(request, 'pool_observation_044')
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '044')
    yield engine


@pytest.fixture
def serve045_engine(request):
    engine = _isolated_engine(request, 'pool_observation_045')
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '045')
    # Revision 001 intentionally bootstraps current Base metadata on a fresh
    # test database.  Remove Serve046's runtime-head fields to reproduce an
    # actually deployed 045 catalog and exercise the additive successor DDL.
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('ALTER TABLE reserved_fill_service_claim_sets '
                            'DROP CONSTRAINT IF EXISTS '
                            'ck_reserved_fill_claim_set_service_version'))
        connection.execute(
            sqlalchemy.text('ALTER TABLE reserved_fill_service_claim_sets '
                            'DROP COLUMN IF EXISTS service_version'))
        connection.execute(
            sqlalchemy.text('ALTER TABLE reserved_fill_pool_claims '
                            'DROP COLUMN IF EXISTS '
                            'worker_projection_sha256_by_accelerator'))
    yield engine


def _repository(
    engine: sqlalchemy.engine.Engine,
) -> observation.PoolCapacityObservationRepository:
    return observation.PoolCapacityObservationRepository(
        engine,
        token_factory=lambda: uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'),
    )


def _reclaim_identity(
    policy_revision: str = 'policy-v1'
) -> reclaim_attestation.ReclaimPolicyIdentity:
    return reclaim_attestation.ReclaimPolicyIdentity(
        fleet_bundle_sha256='c' * 64,
        policy_revision=policy_revision,
        provider_inventory_sha256='d' * 64)


def _reclaim_receipt(
    identity: reclaim_attestation.ReclaimPolicyIdentity | None = None,
    claimed_contexts: tuple[reclaim_attestation.ReservedContextClaim, ...] = (),
) -> reclaim_attestation.ReclaimActivationReceipt:
    identity = _reclaim_identity() if identity is None else identity
    evidence = reclaim_attestation.ReclaimEnforcementEvidence(
        contract=(reclaim_attestation.ReclaimEnforcementContract.
                  GLOBAL_FLEET_CLAIM_ADMISSION_AND_LAUNCH_FENCES_V2),
        fleet_bundle_sha256=identity.fleet_bundle_sha256,
        policy_revision=identity.policy_revision,
        provider_inventory_sha256=identity.provider_inventory_sha256,
        claimed_contexts=claimed_contexts,
        completed_monotonic=1.0)
    return reclaim_attestation.activation_receipt(
        evidence,
        writer_image_digest='sha256:' + 'a' * 64,
        writer_deployment_generation='1',
        writer_deployment_uid='deployment-uid',
        writer_pod_inventory_count=1,
        writer_pod_inventory_sha256='b' * 64)


def _activate(
    repository: observation.PoolCapacityObservationRepository,
    *,
    identity: reclaim_attestation.ReclaimPolicyIdentity | None = None,
) -> observation.ReconciliationGate:
    result = repository.authorize_sequenced_reconciliation(
        expected_generation=0, receipt=_reclaim_receipt(identity))
    assert result.changed
    return result.gate


def _begin(
    repository: observation.PoolCapacityObservationRepository,
    *,
    lease_duration_seconds: float = 60,
    minimum_refresh_interval_seconds: float = 0,
) -> observation.PoolCapacityObservationLease:
    return repository.begin_observation(
        pool_key=_pool_key('a100-80gb', 'h200'),
        physical_cluster_uid='physical-uid',
        accelerator_names=('h200', 'A100-80GB'),
        access_context='research-east',
        lease_duration_seconds=lease_duration_seconds,
        minimum_refresh_interval_seconds=minimum_refresh_interval_seconds,
    )


def _success(free_gpus: int = 3) -> observation.PoolCapacitySuccess:
    return observation.PoolCapacitySuccess.from_counts(free_gpus, {
        'a100-80gb': min(free_gpus, 1),
        'h200': max(0, free_gpus - 1),
    })


def test_serve045_catalog_is_additive_and_legacy_rows_remain_inert(
        observation_engine) -> None:
    inspector = sqlalchemy.inspect(observation_engine)
    protocol_columns = {
        column['name']
        for column in inspector.get_columns('reserved_fill_protocol_state')
    }
    observation_columns = {
        column['name']
        for column in inspector.get_columns('demand_capacity_observations')
    }
    round_columns = {
        column['name']
        for column in inspector.get_columns('reserved_fill_rounds')
    }
    claim_set_columns = {
        column['name']
        for column in inspector.get_columns('reserved_fill_service_claim_sets')
    }
    assert {
        'zero_cost_admission_sequence',
        'ordinary_zero_cost_admission_sequence',
        'zero_cost_materialization_sequence',
        'reconciliation_gate_state',
        'reconciliation_gate_generation',
        'reclaim_fleet_bundle_sha256',
        'reclaim_policy_revision',
        'reclaim_provider_inventory_sha256',
    } <= protocol_columns
    assert {
        'pool_key',
        'physical_cluster_uid',
        'accelerator_names',
        'access_context',
        'observation_generation',
        'lease_token',
        'lease_expires_at',
        'observation_sequence',
        'ordinary_admission_sequence',
        'materialization_sequence',
        'observation_status',
        'payload',
        'payload_sha256',
        'observed_at',
        'valid_until',
        'published_at',
    } <= observation_columns
    assert {
        'observation_generation',
        'observation_sequence',
        'observation_materialization_sequence',
        'observation_payload_sha256',
    } <= round_columns
    assert {
        'allocation_generation',
        'allocation_input_sha256',
        'allocation_claim_generation',
        'allocation_map',
        'allocation_published_at',
        'allocation_gate_generation',
    } <= claim_set_columns

    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO demand_capacity_observations
                    (context, snapshot_time, completed_at, availability)
                VALUES ('legacy-context', 1, 2, '{"a100": 4}')
            """))
        legacy = connection.execute(
            sqlalchemy.text("""
                SELECT pool_key, observation_generation, payload_sha256
                FROM demand_capacity_observations
                WHERE context = 'legacy-context'
            """)).one()
    assert tuple(legacy) == (None, None, None)


def test_round_provenance_and_allocation_publication_are_closed(
        observation_engine) -> None:
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with observation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO reserved_fill_rounds
                        (pool_key, observation_generation)
                    VALUES ('round-partial', 1)
                """))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with observation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO reserved_fill_rounds
                        (pool_key, observation_generation,
                         observation_sequence, observation_payload_sha256)
                    VALUES ('round-missing-materialization', 1, 2, :digest)
                """), {'digest': 'a' * 64})
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                    INSERT INTO reserved_fill_rounds
                        (pool_key, observation_generation,
                     observation_sequence,
                     observation_materialization_sequence,
                     observation_payload_sha256)
                VALUES ('round-complete', 1, 2, 3, :digest)
            """), {'digest': 'a' * 64})
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO reserved_fill_service_claim_sets (service_name)
                VALUES ('legacy-service')
            """))
        legacy = connection.execute(
            sqlalchemy.text("""
                SELECT allocation_generation, allocation_input_sha256,
                       allocation_map
                FROM reserved_fill_service_claim_sets
                WHERE service_name = 'legacy-service'
            """)).one()
    assert tuple(legacy) == (0, None, None)

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with observation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    UPDATE reserved_fill_service_claim_sets
                    SET allocation_generation = 1
                    WHERE service_name = 'legacy-service'
                """))
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE reserved_fill_service_claim_sets
                SET allocation_generation = 1,
                    allocation_input_sha256 = :digest,
                    allocation_claim_generation = 3,
                    allocation_map = CAST(:allocation_map AS jsonb),
                    allocation_published_at = 10,
                    allocation_gate_generation = 4
                WHERE service_name = 'legacy-service'
            """), {
                'digest': 'b' * 64,
                'allocation_map': '{"pool": 2}',
            })


def test_fresh_serve_lineage_reaches_canonical_046(observation_engine) -> None:
    config = migration_utils.get_alembic_config(observation_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '046')

    assert migration_utils.get_current_alembic_revision(
        observation_engine, migration_utils.SERVE_DB_NAME) == '046'
    inspector = sqlalchemy.inspect(observation_engine)
    assert inspector.has_table('serve_ordinary_launch_associations')
    assert 'observation_generation' in {
        column['name']
        for column in inspector.get_columns('reserved_fill_rounds')
    }
    with observation_engine.connect() as connection:
        trigger_modes = {
            row[0]: row[1] for row in connection.execute(
                sqlalchemy.text("""
                SELECT tgname, tgenabled
                FROM pg_trigger
                WHERE tgrelid =
                          'reserved_fill_protocol_state'::regclass
                  AND NOT tgisinternal
            """))
        }
    assert trigger_modes['skyserve045_reconciliation_gate_guard'] == 'A'
    assert trigger_modes[
        'skyserve045_reconciliation_gate_truncate_guard'] == 'A'
    assert 'skyserve044_reconciliation_gate_guard' not in trigger_modes
    assert {
        'service_version',
    } <= {
        column['name']
        for column in inspector.get_columns('reserved_fill_service_claim_sets')
    }
    assert 'worker_projection_sha256_by_accelerator' in {
        column['name']
        for column in inspector.get_columns('reserved_fill_pool_claims')
    }


def test_serve046_is_additive_and_rejects_malformed_projection_authority(
        serve045_engine) -> None:
    inspector = sqlalchemy.inspect(serve045_engine)
    assert 'service_version' not in {
        column['name']
        for column in inspector.get_columns('reserved_fill_service_claim_sets')
    }
    config = migration_utils.get_alembic_config(serve045_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '046')
    assert migration_utils.get_current_alembic_revision(
        serve045_engine, migration_utils.SERVE_DB_NAME) == '046'
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with serve045_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO reserved_fill_service_claim_sets
                        (service_name, claim_set_state, generation, edge_count,
                         service_version, heartbeat_ts)
                    VALUES ('bad-version', 'migration_shadow', 0, 0, 0, 0)
                """))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with serve045_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    INSERT INTO reserved_fill_pool_claims
                        (service_name, pool_key, legacy_pool_key, pool_position,
                         service_generation,
                         worker_projection_sha256_by_accelerator, heartbeat_ts)
                    VALUES ('bad-map', 'pool', 'legacy', 0, 0,
                            '{}'::jsonb, 0)
                """))


def test_serve045_rejects_an_already_sequenced_serve044_gate(
        serve044_engine) -> None:
    with serve044_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                UPDATE reserved_fill_protocol_state
                SET reconciliation_gate_state = 'SEQUENCED_ACTIVE',
                    reconciliation_gate_generation = 1
                WHERE id = 1
            """))
    config = migration_utils.get_alembic_config(serve044_engine,
                                                migration_utils.SERVE_DB_NAME)

    with pytest.raises(RuntimeError, match='already-sequenced Serve044'):
        alembic_command.upgrade(config, '045')

    assert migration_utils.get_current_alembic_revision(
        serve044_engine, migration_utils.SERVE_DB_NAME) == '044'
    assert {
        'reclaim_fleet_bundle_sha256',
        'reclaim_policy_revision',
        'reclaim_provider_inventory_sha256',
    }.isdisjoint({
        column['name'] for column in sqlalchemy.inspect(
            serve044_engine).get_columns('reserved_fill_protocol_state')
    })


def test_failed_serve045_ddl_rolls_back_and_is_retryable(
        serve044_engine) -> None:
    with serve044_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                CREATE FUNCTION skyserve045_guard_reconciliation_gate()
                RETURNS integer
                LANGUAGE sql
                AS 'SELECT 1'
            """))
    config = migration_utils.get_alembic_config(serve044_engine,
                                                migration_utils.SERVE_DB_NAME)

    with pytest.raises(sqlalchemy.exc.DBAPIError):
        alembic_command.upgrade(config, '045')

    assert migration_utils.get_current_alembic_revision(
        serve044_engine, migration_utils.SERVE_DB_NAME) == '044'
    assert {
        'reclaim_fleet_bundle_sha256',
        'reclaim_policy_revision',
        'reclaim_provider_inventory_sha256',
    }.isdisjoint({
        column['name'] for column in sqlalchemy.inspect(
            serve044_engine).get_columns('reserved_fill_protocol_state')
    })
    with serve044_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                DROP FUNCTION skyserve045_guard_reconciliation_gate()
            """))
    alembic_command.upgrade(config, '045')
    assert migration_utils.get_current_alembic_revision(
        serve044_engine, migration_utils.SERVE_DB_NAME) == '045'


@pytest.mark.parametrize('statement', [
    'DELETE FROM reserved_fill_protocol_state WHERE id = 1',
    'TRUNCATE reserved_fill_protocol_state',
])
def test_serve045_guard_preserves_protocol_singleton(observation_engine,
                                                     statement) -> None:
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='singleton cannot be removed'):
        with observation_engine.begin() as connection:
            connection.execute(sqlalchemy.text(statement))

    with observation_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.text("""
                SELECT count(*)
                FROM reserved_fill_protocol_state
                WHERE id = 1
            """)).scalar_one() == 1


@pytest.mark.parametrize(
    'state, generation, fleet_bundle, policy_revision, provider_inventory',
    [
        ('LEGACY_ACTIVE', 0, 'a' * 64, None, None),
        ('SEQUENCED_ACTIVE', 0, 'a' * 64, 'policy-v1', 'b' * 64),
        ('SEQUENCED_ACTIVE', 2, 'a' * 64, 'policy-v1', 'b' * 64),
        ('SEQUENCED_ACTIVE', 1, 'a' * 64, None, 'b' * 64),
        ('SEQUENCED_ACTIVE', 1, 'not-a-digest', 'policy-v1', 'b' * 64),
    ],
)
def test_serve045_gate_rejects_every_non_exact_successor(
        observation_engine, state, generation, fleet_bundle, policy_revision,
        provider_inventory) -> None:
    with pytest.raises(sqlalchemy.exc.DBAPIError):
        with observation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    UPDATE reserved_fill_protocol_state
                    SET reconciliation_gate_state = :state,
                        reconciliation_gate_generation = :generation,
                        reclaim_fleet_bundle_sha256 = :fleet_bundle,
                        reclaim_policy_revision = :policy_revision,
                        reclaim_provider_inventory_sha256 =
                            :provider_inventory
                    WHERE id = 1
                """), {
                    'state': state,
                    'generation': generation,
                    'fleet_bundle': fleet_bundle,
                    'policy_revision': policy_revision,
                    'provider_inventory': provider_inventory,
                })

    assert _repository(observation_engine).read_reconciliation_gate() == (
        observation.ReconciliationGate(
            state=observation.ReconciliationGateState.LEGACY_ACTIVE,
            generation=0))


def test_reconciliation_gate_is_generation_fenced_and_reauthorizable(
        observation_engine) -> None:
    repository = _repository(observation_engine)
    receipt = _reclaim_receipt()
    assert repository.read_reconciliation_gate() == (
        observation.ReconciliationGate(
            state=observation.ReconciliationGateState.LEGACY_ACTIVE,
            generation=0))

    with pytest.raises(observation.ReconciliationGateConflictError):
        repository.authorize_sequenced_reconciliation(expected_generation=1,
                                                      receipt=receipt)

    result = repository.authorize_sequenced_reconciliation(
        expected_generation=0, receipt=receipt)
    assert result.changed
    activated = result.gate
    assert activated.state == observation.ReconciliationGateState.SEQUENCED_ACTIVE
    assert activated.generation == 1
    assert activated.reclaim_policy_identity == _reclaim_identity()
    assert activated.reclaim_activation_receipt == receipt
    assert activated.reclaim_authorized_at is not None
    assert repository.read_reconciliation_gate() == activated

    # A lost-response retry is a complete no-op, including its timestamp.
    retry = repository.authorize_sequenced_reconciliation(expected_generation=0,
                                                          receipt=receipt)
    assert not retry.changed
    assert retry.gate == activated

    rotated_receipt = _reclaim_receipt(_reclaim_identity('policy-v2'))
    rotated = repository.authorize_sequenced_reconciliation(
        expected_generation=1, receipt=rotated_receipt)
    assert rotated.changed
    assert rotated.gate.generation == 2
    assert rotated.gate.reclaim_activation_receipt == rotated_receipt

    current_retry = repository.authorize_sequenced_reconciliation(
        expected_generation=2, receipt=rotated_receipt)
    assert not current_retry.changed
    assert current_retry.gate == rotated.gate
    with pytest.raises(observation.ReconciliationGateConflictError,
                       match='unrelated expected generation'):
        repository.authorize_sequenced_reconciliation(expected_generation=0,
                                                      receipt=rotated_receipt)

    with pytest.raises(observation.ReconciliationGateConflictError):
        repository.authorize_sequenced_reconciliation(
            expected_generation=1,
            receipt=_reclaim_receipt(_reclaim_identity('policy-v3')))


def _activation_worker_projection() -> dict[str, object]:
    return {
        'projection_version': 2,
        'candidate_id': 'kubernetes-0000',
        'kubernetes_context': 'research-east',
        'namespace': 'default',
        'service_account_name': 'skyserve-worker',
        'scheduler_name': 'default-scheduler',
        'priority_class_name': 'skyserve-preemptible',
        'priority_value': -1000,
        'preemption_policy': 'Never',
        'kueue_admission': {
            'local_queue_name': 'skyserve-reserved',
            'workload_priority_class_name': 'skyserve-preemptible',
        },
        'pod_identity_role_arn': 'arn:aws:iam::123456789012:role/skyserve-worker',
        'accelerator_name': 'A100-80GB',
        'accelerator_count': 1,
        'accelerator_scheduling': {
            'label_key': 'nvidia.com/gpu.product',
            'label_values': ['NVIDIA-A100-80GB'],
            'resource_key': 'nvidia.com/gpu',
        },
        'cache': {
            'kind': 'none',
        },
    }


def _install_activation_claim_authority(
    engine: sqlalchemy.engine.Engine,
) -> tuple[reclaim_attestation.ReservedContextClaim, str]:
    projection = _activation_worker_projection()
    projected_admissions = (
        serve_state.reserved_fill_reclaim_projected_admissions(
            [projection],
            access_context='research-east',
            accelerator_names=('a100-80gb',),
            accelerator_count=1))
    claim = reclaim_attestation.ReservedContextClaim(
        service_name='queued',
        service_version=1,
        service_generation=11,
        pool_key=_pool_key('a100-80gb'),
        access_context='research-east',
        physical_cluster_uid='physical-uid',
        accelerator_names=('a100-80gb',),
        projected_admissions=projected_admissions)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state.services_table).values(
                name='queued', current_version=1))
        connection.execute(
            sqlalchemy.insert(serve_state.version_specs_table).values(
                service_name='queued',
                version=1,
                yaml_content='service: v1\n',
                worker_placement_projections=[projection]))
        connection.execute(
            sqlalchemy.update(
                serve_state.reserved_fill_protocol_state_table).where(
                    serve_state.reserved_fill_protocol_state_table.c.id ==
                    1).values(claim_generation=11))
        connection.execute(
            sqlalchemy.insert(
                serve_state.reserved_fill_service_claim_sets_table).values(
                    service_name='queued',
                    claim_set_state='authoritative_v2',
                    generation=11,
                    edge_count=1,
                    semantic_hash='semantic-v1',
                    service_version=None,
                    heartbeat_ts=1))
        connection.execute(
            sqlalchemy.insert(
                serve_state.reserved_fill_pool_claims_table).values(
                    service_name='queued',
                    pool_key=claim.pool_key,
                    legacy_pool_key=json.dumps(['research-east', 'a100-80gb']),
                    pool_position=0,
                    access_context='research-east',
                    physical_cluster_uid='physical-uid',
                    accelerator_names=json.dumps(['a100-80gb']),
                    worker_projection_sha256_by_accelerator=None,
                    service_generation=11,
                    weight=1000,
                    floor_replicas=0,
                    gpus_per_replica=1,
                    holdings_fill=0,
                    effective_cap=8,
                    launchable=1,
                    heartbeat_ts=1))
    return claim, projected_admissions[0].worker_projection_sha256


def _successor_queued_replica_row(
    identity: reclaim_attestation.ReclaimPolicyIdentity,
    worker_projection_sha256: str,
    *,
    service_version: int = 1,
    record_version: int | None = None,
    admission_sequence: int | None = 13,
) -> dict[str, object]:
    location = spot_placer.Location(cloud=clouds.Kubernetes(),
                                    region='research-east',
                                    zone=None,
                                    accelerators={'A100-80GB': 1},
                                    use_spot=False)
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='queued-1',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=location,
                                        version=service_version,
                                        resources_override={
                                            'cloud': clouds.Kubernetes(),
                                            'region': 'research-east',
                                            'zone': None,
                                            'accelerators': {
                                                'A100-80GB': 1,
                                            },
                                            'use_spot': False,
                                        })
    info.reserved_fill = True
    info.is_zero_cost = True
    info.reserved_fill_pool_key = _pool_key('a100-80gb')
    info.reserved_fill_service_generation = 11
    info.reserved_fill_physical_cluster_uid = 'physical-uid'
    info.reserved_fill_kubernetes_context = 'research-east'
    info.reserved_fill_allocation_generation = 3
    info.reserved_fill_allocation_input_sha256 = 'e' * 64
    info.reserved_fill_allocation_claim_generation = 11
    info.reserved_fill_reconciliation_gate_generation = 1
    info.reserved_fill_reclaim_fleet_bundle_sha256 = (
        identity.fleet_bundle_sha256)
    info.reserved_fill_reclaim_policy_revision = identity.policy_revision
    info.reserved_fill_reclaim_provider_inventory_sha256 = (
        identity.provider_inventory_sha256)
    info.reserved_fill_worker_projection_sha256 = worker_projection_sha256
    info.reserved_fill_observation_generation = 5
    info.reserved_fill_observation_sequence = 0
    info.reserved_fill_intent_idempotency_key = 'f' * 64
    # Serialize a valid current record, then optionally emulate a corrupted
    # durable payload that bypassed the normal writer boundary.
    info.zero_cost_admission_sequence = 13
    row = serve_state._replica_row_values('queued', 1, info)
    if admission_sequence != 13:
        state = dict(row['replica_state'])
        state['zero_cost_admission_sequence'] = admission_sequence
        row['replica_state'] = state
    if record_version is not None:
        state = dict(row['replica_state'])
        state['replica_info_version'] = record_version
        row['replica_state'] = state
    return row


@pytest.mark.parametrize('authority_mismatch', (
    'replica-info-v16',
    'worker-projection',
    'service-version',
    'null-admission-sequence',
    'zero-admission-sequence',
))
def test_initial_activation_rejects_noncurrent_queued_launch_authority(
        observation_engine, authority_mismatch) -> None:
    repository = _repository(observation_engine)
    identity = _reclaim_identity()
    claim, projection_sha256 = _install_activation_claim_authority(
        observation_engine)
    record_version = 16 if authority_mismatch == 'replica-info-v16' else None
    service_version = 2 if authority_mismatch == 'service-version' else 1
    if authority_mismatch == 'worker-projection':
        projection_sha256 = '0' * 64
    admission_sequence: int | None = 13
    if authority_mismatch == 'null-admission-sequence':
        admission_sequence = None
    elif authority_mismatch == 'zero-admission-sequence':
        admission_sequence = 0
    row = _successor_queued_replica_row(identity,
                                        projection_sha256,
                                        service_version=service_version,
                                        record_version=record_version,
                                        admission_sequence=admission_sequence)
    with observation_engine.begin() as connection:
        connection.execute(sqlalchemy.insert(serve_state.replicas_table), [row])

    with pytest.raises(observation.ReconciliationGateConflictError,
                       match='blocked rows'):
        repository.authorize_sequenced_reconciliation(expected_generation=0,
                                                      receipt=_reclaim_receipt(
                                                          identity, (claim,)))
    assert repository.read_reconciliation_gate().state == (
        observation.ReconciliationGateState.LEGACY_ACTIVE)


def test_activation_transaction_accepts_exact_current_queue_under_composite_guard(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    identity = _reclaim_identity()
    claim, projection_sha256 = _install_activation_claim_authority(
        observation_engine)
    receipt = _reclaim_receipt(identity, (claim,))
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    legacy_state = {
        'replica_info_version': 16,
        'reserved_fill': True,
    }
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state.replicas_table),
            [_successor_queued_replica_row(identity, projection_sha256)])
        connection.execute(sqlalchemy.insert(serve_state.replicas_table), [{
            'service_name': 'ready',
            'replica_id': 2,
            'status': 'READY',
            'replica_state_version': 1,
            'replica_state': legacy_state,
        }])

    with serve_state.reserved_fill_reclaim_gate_authority_guard(
            shared=False) as guard:

        def activate(connection):
            before = repository.lock_reconciliation_gate_for_activation(
                connection)
            assert len(
                serve_state.
                get_authoritative_reserved_fill_claims_in_connection(
                    connection)) == 1
            advisory_count = connection.execute(
                sqlalchemy.text("""
                    SELECT count(*)
                    FROM pg_locks
                    WHERE pid = pg_backend_pid()
                      AND locktype = 'advisory'
                      AND granted
                """)).scalar_one()
            assert advisory_count >= 2
            return repository.authorize_sequenced_reconciliation(
                expected_generation=before.generation,
                receipt=receipt,
                connection=connection)

        activated = (
            serve_state.run_reserved_fill_reclaim_activation_transaction(
                guard, activate))

    assert activated.changed
    gate = activated.gate
    assert gate.state == observation.ReconciliationGateState.SEQUENCED_ACTIVE
    assert gate.reclaim_policy_identity == identity
    assert gate.reclaim_activation_receipt == receipt
    assert repository.read_reconciliation_gate() == gate
    # A lost-response retry returns the already-committed successor.
    retry = repository.authorize_sequenced_reconciliation(expected_generation=0,
                                                          receipt=receipt)
    assert not retry.changed
    assert retry.gate == gate

    with pytest.raises(observation.ReconciliationGateConflictError):
        repository.authorize_sequenced_reconciliation(
            expected_generation=0,
            receipt=_reclaim_receipt(_reclaim_identity('different-policy')))
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='requires one exact successor generation'):
        with observation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    UPDATE reserved_fill_protocol_state
                    SET reconciliation_gate_state = 'LEGACY_ACTIVE',
                        reconciliation_gate_generation = 2
                    WHERE id = 1
                """))
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='requires one exact successor generation'):
        with observation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    UPDATE reserved_fill_protocol_state
                    SET reclaim_policy_revision = 'mutated-policy'
                    WHERE id = 1
                """))
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='requires one exact successor generation'):
        with observation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    UPDATE reserved_fill_protocol_state
                    SET reconciliation_gate_generation = 2
                    WHERE id = 1
                """))
    assert repository.read_reconciliation_gate() == gate


def test_sequenced_ordinary_zero_cost_inserts_share_observation_sequence(
        observation_engine) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    first = types.SimpleNamespace(is_zero_cost=True,
                                  reserved_fill=False,
                                  zero_cost_admission_sequence=None,
                                  zero_cost_materialization_sequence=None)
    second = types.SimpleNamespace(is_zero_cost=True,
                                   reserved_fill=False,
                                   zero_cost_admission_sequence=None,
                                   zero_cost_materialization_sequence=None)
    infos = [(9, first), (2, second)]
    with sqlalchemy.orm.Session(observation_engine) as session:
        assignments = (
            serve_state._stamp_new_zero_cost_replica_admissions_in_session(  # pylint: disable=protected-access
                session, observation_engine, infos))
        assert first.zero_cost_admission_sequence is None
        assert second.zero_cost_admission_sequence is None
        serve_state._apply_zero_cost_sequence_assignments(  # pylint: disable=protected-access
            infos, admissions=assignments)
        session.commit()
    # Assignment is stable by replica ID, independent of caller order.
    assert second.zero_cost_admission_sequence == 1
    assert first.zero_cost_admission_sequence == 2

    lease = _begin(repository)
    assert lease.observation_sequence == 2
    assert lease.ordinary_admission_sequence == 2
    assert lease.materialization_sequence == 0

    third = types.SimpleNamespace(is_zero_cost=True,
                                  reserved_fill=False,
                                  zero_cost_admission_sequence=None,
                                  zero_cost_materialization_sequence=None)
    third_infos = [(10, third)]
    with sqlalchemy.orm.Session(observation_engine) as session:
        assignments = (
            serve_state._stamp_new_zero_cost_replica_admissions_in_session(  # pylint: disable=protected-access
                session, observation_engine, third_infos))
        serve_state._apply_zero_cost_sequence_assignments(  # pylint: disable=protected-access
            third_infos,
            admissions=assignments)
        session.commit()
    assert third.zero_cost_admission_sequence == 3
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='generation cannot decrease'):
        with observation_engine.begin() as connection:
            connection.execute(
                sqlalchemy.text("""
                    UPDATE reserved_fill_protocol_state
                    SET reconciliation_gate_generation = 0
                    WHERE id = 1
                """))


def _zero_cost_replica(
        replica_id: int,
        *,
        context: str = 'ordinary-alias') -> replica_managers.ReplicaInfo:
    info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'ordinary-{replica_id}',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    info.is_zero_cost = True
    info.location = {
        'cloud': 'Kubernetes',
        'region': context,
        'zone': None,
        'accelerators': {
            'A100-80GB': 1,
        },
    }
    return info


def _install_bound_zero_cost_replica(
    engine: sqlalchemy.engine.Engine,
) -> tuple[uuid.UUID, replica_managers.ReplicaInfo]:
    """Install one exact unresolved binding through the production guards."""
    controller_incarnation = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state.service_lifecycle_fences_table).values(
                    name='ordinary', epoch=1))
        connection.execute(
            sqlalchemy.insert(serve_state.services_table).values(
                name='ordinary',
                workspace='workspace-a',
                status='READY',
                hash='service-hash',
                current_version=1,
                active_versions='[1]',
                pool=0,
                lifecycle_epoch=1,
                controller_incarnation=controller_incarnation,
                controller_owner_epoch=1,
                ordinary_launch_binding_capable=True,
                ordinary_launch_binding_mode='bound',
                ordinary_launch_binding_epoch=1))
    assert serve_state.add_or_update_replica('ordinary', 1,
                                             _zero_cost_replica(1))
    info = serve_state.get_replica_info_from_id('ordinary', 1)
    assert info is not None
    association_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                ordinary_launch_binding.ordinary_launch_associations_table).
            values(association_id=association_id,
                   submission_id=uuid.uuid4(),
                   tenant_scope='tenant-a',
                   service_name='ordinary',
                   service_hash='service-hash',
                   service_workspace='workspace-a',
                   service_lifecycle_epoch=1,
                   service_binding_epoch=1,
                   service_version=1,
                   replica_id=1,
                   replica_record_id=uuid.UUID(info.replica_record_id),
                   launch_generation=1,
                   cluster_name=info.cluster_name,
                   request_id=f'bound-{association_id}',
                   input_digest='a' * 64,
                   owner_controller_incarnation=controller_incarnation,
                   owner_controller_epoch=1))
        connection.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == 'ordinary',
                serve_state.replicas_table.c.replica_id == 1).values(
                    ordinary_launch_association_id=association_id))
    return association_id, info


def _single_pool_key() -> str:
    return _pool_key('a100-80gb')


def _protocol_sequences(
    engine: sqlalchemy.engine.Engine,) -> tuple[int, int, int]:
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.text("""
                SELECT zero_cost_admission_sequence,
                       ordinary_zero_cost_admission_sequence,
                       zero_cost_materialization_sequence
                FROM reserved_fill_protocol_state
                WHERE id = 1
            """)).one()
    return tuple(row)


def _begin_single_pool(
    repository: observation.PoolCapacityObservationRepository,
) -> observation.PoolCapacityObservationLease:
    return repository.begin_observation(
        pool_key=_single_pool_key(),
        physical_cluster_uid='physical-uid',
        accelerator_names=('a100-80gb',),
        access_context='claim-alias',
        lease_duration_seconds=60,
    )


def _sequenced_debit(
    lease: observation.PoolCapacityObservationLease,
) -> tuple[int, int, dict[str, int], dict[str, int], int]:
    return reserved_capacity_broker._occupying_debit(  # pylint: disable=protected-access
        ['claimant'],
        lease.pool_key,
        lease.observed_at,
        access_contexts=('claim-alias',),
        physical_cluster_uid='physical-uid',
        claim_generations={'claimant': 1},
        pool_gpus_per_replica=1,
        observation_admission_sequence=lease.observation_sequence,
        observation_materialization_sequence=lease.materialization_sequence,
    )


def test_protocol_first_replica_admission_does_not_deadlock_service_lock(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state.services_table).values(name='svc'))
        connection.execute(
            sqlalchemy.insert(
                serve_state.service_lifecycle_fences_table).values(name='svc',
                                                                   epoch=1))

    writer_entered_protocol = threading.Event()
    original_lock = serve_state._lock_zero_cost_protocol_sequence_for_update

    def signaled_lock(executor):
        writer_entered_protocol.set()
        return original_lock(executor)

    monkeypatch.setattr(serve_state,
                        '_lock_zero_cost_protocol_sequence_for_update',
                        signaled_lock)
    info = _zero_cost_replica(1)
    protocol = observation_schema.protocol_state_sequence_table
    with observation_engine.connect() as holder:
        transaction = holder.begin()
        holder.execute(
            sqlalchemy.select(protocol).where(
                protocol.c.id == 1).with_for_update()).one()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(serve_state.add_or_update_replica, 'svc',
                                     1, info)
            assert writer_entered_protocol.wait(timeout=5)
            # The writer is waiting on protocol and therefore owns neither
            # lifecycle nor service. This protocol->service participant can
            # take both immediately instead of forming the former cycle.
            holder.execute(
                sqlalchemy.select(
                    serve_state.service_lifecycle_fences_table).where(
                        serve_state.service_lifecycle_fences_table.c.name ==
                        'svc').with_for_update()).one()
            holder.execute(
                sqlalchemy.select(serve_state.services_table.c.name).where(
                    serve_state.services_table.c.name ==
                    'svc').with_for_update()).one()
            transaction.commit()
            assert future.result(timeout=5) is True


def test_failed_zero_cost_insert_keeps_caller_and_sequences_unassigned(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    info = _zero_cost_replica(1)
    original_row_values = serve_state._replica_row_values

    def fail_after_encoding(*args, **kwargs):
        original_row_values(*args, **kwargs)
        raise RuntimeError('injected post-stamp failure')

    with monkeypatch.context() as patch:
        patch.setattr(serve_state, '_replica_row_values', fail_after_encoding)
        with pytest.raises(RuntimeError, match='post-stamp failure'):
            serve_state.add_or_update_replica('ordinary', 1, info)

    assert info.zero_cost_admission_sequence is None
    assert info.zero_cost_materialization_sequence is None
    assert _protocol_sequences(observation_engine) == (0, 0, 0)
    with observation_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(  # pylint: disable=not-callable
                serve_state.replicas_table)).scalar_one() == 0

    assert serve_state.add_or_update_replica('ordinary', 1, info)
    assert info.zero_cost_admission_sequence == 1
    assert info.zero_cost_materialization_sequence is None
    assert _protocol_sequences(observation_engine) == (1, 1, 0)


def test_duplicate_zero_cost_insert_rolls_back_candidate_sequence(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    assert serve_state.add_or_update_replica('ordinary', 1,
                                             _zero_cost_replica(1))
    duplicate = _zero_cost_replica(1)

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.add_or_update_replica('ordinary', 1, duplicate)

    assert duplicate.zero_cost_admission_sequence is None
    assert duplicate.zero_cost_materialization_sequence is None
    assert _protocol_sequences(observation_engine) == (1, 1, 0)


@pytest.mark.parametrize('writer', ('single', 'batch', 'launch-shadow'))
def test_generic_insert_writers_reject_reserved_fill_authority(
        observation_engine, monkeypatch, writer) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    info = _zero_cost_replica(1)
    info.reserved_fill = True

    with pytest.raises(ValueError, match='typed.*persistence path'):
        if writer == 'single':
            serve_state.add_or_update_replica('ordinary', 1, info)
        elif writer == 'batch':
            serve_state.add_or_update_replicas('ordinary', [(1, info)])
        else:
            serve_state.add_or_update_replica_with_launch_shadow(
                'ordinary',
                1,
                info,
                None,  # Rejection precedes launch-shadow decoding.
                expected_controller_owner=(1, '127.0.0.1'),
                expected_lifecycle_epoch=1)

    assert _protocol_sequences(observation_engine) == (0, 0, 0)


def test_exact_boolean_zero_cost_marker_is_required_before_persistence(
        observation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    info = _zero_cost_replica(1)
    info.is_zero_cost = 1

    with pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='exact boolean'):
        serve_state.add_or_update_replica('ordinary', 1, info)

    assert _protocol_sequences(observation_engine) == (0, 0, 0)


def test_generic_zero_cost_insert_rejects_preassigned_materialization(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    info = _zero_cost_replica(1)
    info.zero_cost_materialization_sequence = 7

    with pytest.raises(ValueError, match='assigned by PostgreSQL'):
        serve_state.add_or_update_replica('ordinary', 1, info)

    assert info.zero_cost_admission_sequence is None
    assert info.zero_cost_materialization_sequence == 7
    assert _protocol_sequences(observation_engine) == (0, 0, 0)


@pytest.mark.parametrize('field', (
    'zero_cost_admission_sequence',
    'zero_cost_materialization_sequence',
))
def test_typed_fill_rejects_caller_assigned_event_sequence(
        observation_engine, monkeypatch, field) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    info = _zero_cost_replica(1)
    info.reserved_fill = True
    setattr(info, field, 7)

    with pytest.raises(ValueError, match='assigned by PostgreSQL'):
        serve_state.add_replica_if_round_epoch('claimant',
                                               1,
                                               info,
                                               pool_key=_single_pool_key(),
                                               expected_epoch=1,
                                               expected_protocol_version=2,
                                               expected_lease_token=1)

    assert _protocol_sequences(observation_engine) == (0, 0, 0)


def test_paid_claim_path_rejects_zero_cost_authority_without_side_effects(
        observation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    info = _zero_cost_replica(1)

    with pytest.raises(ValueError, match='cannot enter.*paid-capacity'):
        serve_state.try_add_replica_with_paid_capacity_claim(
            'ordinary',
            'hash',
            1,
            info,
            pool_key='paid-pool',
            priority=10,
            base_limit=1,
            max_limit=1,
            now=1,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=None)

    with observation_engine.connect() as connection:
        replica_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(  # pylint: disable=not-callable
                serve_state.replicas_table)).scalar_one()
        claim_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(  # pylint: disable=not-callable
                serve_state.paid_capacity_claims_table)).scalar_one()
    assert (replica_count, claim_count) == (0, 0)
    assert _protocol_sequences(observation_engine) == (0, 0, 0)


def test_paid_claim_failure_does_not_publish_pool_key(observation_engine,
                                                      monkeypatch) -> None:
    # This test crosses the current paid-admission runtime boundary.  Keep the
    # shared observation fixture at its canonical Serve046 characterization
    # point, but advance this isolated database through the additive tail.
    config = migration_utils.get_alembic_config(observation_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, migration_utils.SERVE_VERSION)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state.services_table).values(
                name='paid', hash='paid-hash', status='READY'))
    info = _zero_cost_replica(1)
    info.is_zero_cost = False
    original_row_values = serve_state._replica_row_values

    def fail_after_encoding(*args, **kwargs):
        original_row_values(*args, **kwargs)
        raise RuntimeError('injected paid-claim rollback')

    monkeypatch.setattr(serve_state, '_replica_row_values', fail_after_encoding)
    with pytest.raises(RuntimeError, match='paid-claim rollback'):
        serve_state.try_add_replica_with_paid_capacity_claim(
            'paid',
            'paid-hash',
            1,
            info,
            pool_key='paid-pool',
            priority=10,
            base_limit=1,
            max_limit=1,
            now=1,
            success_ttl_seconds=60,
            waiter_ttl_seconds=30,
            expected_controller_owner=None)

    assert info.paid_capacity_pool_key is None
    with observation_engine.connect() as connection:
        replica_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state.replicas_table)).scalar_one()
        claim_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state.paid_capacity_claims_table)).scalar_one()
    assert (replica_count, claim_count) == (0, 0)
    assert _protocol_sequences(observation_engine) == (0, 0, 0)


def test_paid_claim_adoption_rejects_persisted_zero_cost_authority(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state.services_table).values(
                name='ordinary', hash='ordinary-hash', status='READY'))
    persisted = _zero_cost_replica(1)
    assert serve_state.add_or_update_replica('ordinary', 1, persisted)

    # A stale migration snapshot can predate cost classification and both
    # database-assigned event identities. The locked durable row remains the
    # authority and must keep this replica out of paid-capacity accounting.
    stale = _zero_cost_replica(1)
    stale.replica_record_id = persisted.replica_record_id
    stale.is_zero_cost = False
    stale.zero_cost_admission_sequence = None
    with pytest.raises(ValueError, match='persisted zero-cost'):
        serve_state.adopt_paid_capacity_claims('ordinary',
                                               'ordinary-hash',
                                               [(1, 'paid-pool', 10, stale)],
                                               base_limit=1,
                                               now=1,
                                               expected_controller_owner=None)

    assert stale.paid_capacity_pool_key is None
    restored = serve_state.get_replica_info_from_id('ordinary', 1)
    assert restored is not None
    assert restored.is_zero_cost is True
    assert restored.paid_capacity_pool_key is None
    with observation_engine.connect() as connection:
        claim_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state.paid_capacity_claims_table)).scalar_one()
    assert claim_count == 0
    assert _protocol_sequences(observation_engine) == (1, 1, 0)


def test_pre_observation_unbound_ordinary_nonclaimant_debits_fill(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    assert serve_state.add_or_update_replica('ordinary', 1,
                                             _zero_cost_replica(1))
    lease = _begin_single_pool(repository)

    feed, entitlement, by_accelerator, live_fill, unclaimed = (
        _sequenced_debit(lease))

    assert (feed, entitlement) == (1, 1)
    assert by_accelerator == {'a100-80gb': 1}
    assert live_fill == {'claimant': 0}
    assert unclaimed == 0


def test_materialization_during_observation_remains_debited(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    assert serve_state.add_or_update_replica('ordinary', 1,
                                             _zero_cost_replica(1))
    lease = _begin_single_pool(repository)
    info = serve_state.get_replica_info_from_id('ordinary', 1)
    assert info is not None
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    info.status_property.service_ready_now = True
    info.status_property.first_ready_time = 1.0
    assert serve_state.add_or_update_replica('ordinary',
                                             1,
                                             info,
                                             expected_replica_exists=True)
    materialized = serve_state.get_replica_info_from_id('ordinary', 1)
    assert materialized is not None
    assert materialized.zero_cost_materialization_sequence == 1

    feed, entitlement, _, _, _ = _sequenced_debit(lease)

    assert (feed, entitlement) == (1, 1)


def test_bound_late_success_materializes_without_reviving_teardown(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    association_id, info = _install_bound_zero_cost_replica(observation_engine)
    lease = _begin_single_pool(repository)
    info.status_property.sky_launch_status = (
        common_utils.ProcessStatus.INTERRUPTED)
    with observation_engine.begin() as connection:
        assert serve_state.update_replica_for_bound_ordinary_launch_in_transaction(
            connection,
            'ordinary',
            'service-hash',
            1,
            info.replica_record_id,
            association_id,
            info,
            provider_launch_succeeded=True,
            paid_capacity_pool_key=None,
            paid_capacity_outcome=None)

    persisted = serve_state.get_replica_info_from_id('ordinary', 1)
    assert persisted is not None
    assert (persisted.status_property.sky_launch_status ==
            common_utils.ProcessStatus.INTERRUPTED)
    assert persisted.zero_cost_materialization_sequence == 1
    assert _protocol_sequences(observation_engine) == (1, 1, 1)
    feed, entitlement, _, _, _ = _sequenced_debit(lease)
    assert (feed, entitlement) == (1, 1)


def test_bound_pre_effect_cancellation_does_not_materialize(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    association_id, info = _install_bound_zero_cost_replica(observation_engine)
    info.status_property.sky_launch_status = (
        common_utils.ProcessStatus.INTERRUPTED)
    with observation_engine.begin() as connection:
        assert serve_state.update_replica_for_bound_ordinary_launch_in_transaction(
            connection,
            'ordinary',
            'service-hash',
            1,
            info.replica_record_id,
            association_id,
            info,
            provider_launch_succeeded=False,
            paid_capacity_pool_key=None,
            paid_capacity_outcome=None)

    persisted = serve_state.get_replica_info_from_id('ordinary', 1)
    assert persisted is not None
    assert (persisted.status_property.sky_launch_status ==
            common_utils.ProcessStatus.INTERRUPTED)
    assert persisted.zero_cost_materialization_sequence is None
    assert _protocol_sequences(observation_engine) == (1, 1, 0)


def test_first_success_is_stamped_once_and_stale_updates_preserve_it(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    assert serve_state.add_or_update_replica('ordinary', 1,
                                             _zero_cost_replica(1))
    pending = serve_state.get_replica_info_from_id('ordinary', 1)
    assert pending is not None
    stale = dataclasses.replace(pending.status_property)
    first = serve_state.get_replica_info_from_id('ordinary', 1)
    assert first is not None
    first.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED

    assert serve_state.add_or_update_replica('ordinary',
                                             1,
                                             first,
                                             expected_replica_exists=True)
    assert first.zero_cost_materialization_sequence == 1
    assert _protocol_sequences(observation_engine) == (1, 1, 1)

    replay = serve_state.get_replica_info_from_id('ordinary', 1)
    assert replay is not None
    replay.status_property = stale
    replay.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    replay.zero_cost_materialization_sequence = None
    assert serve_state.add_or_update_replica('ordinary',
                                             1,
                                             replay,
                                             expected_replica_exists=True)
    assert replay.zero_cost_materialization_sequence == 1
    assert _protocol_sequences(observation_engine) == (1, 1, 1)

    replay.status_property.sky_launch_status = (
        common_utils.ProcessStatus.INTERRUPTED)
    assert serve_state.add_or_update_replica('ordinary',
                                             1,
                                             replay,
                                             expected_replica_exists=True)
    persisted = serve_state.get_replica_info_from_id('ordinary', 1)
    assert persisted is not None
    assert persisted.zero_cost_materialization_sequence == 1
    assert _protocol_sequences(observation_engine) == (1, 1, 1)


def test_failed_first_success_rolls_back_marker_and_retry_reuses_counter(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    assert serve_state.add_or_update_replica('ordinary', 1,
                                             _zero_cost_replica(1))
    info = serve_state.get_replica_info_from_id('ordinary', 1)
    assert info is not None
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    original_row_values = serve_state._replica_row_values

    def fail_after_encoding(*args, **kwargs):
        original_row_values(*args, **kwargs)
        raise RuntimeError('injected materialization rollback')

    with monkeypatch.context() as patch:
        patch.setattr(serve_state, '_replica_row_values', fail_after_encoding)
        with pytest.raises(RuntimeError, match='materialization rollback'):
            serve_state.add_or_update_replica('ordinary',
                                              1,
                                              info,
                                              expected_replica_exists=True)

    assert info.zero_cost_materialization_sequence is None
    assert _protocol_sequences(observation_engine) == (1, 1, 0)
    persisted = serve_state.get_replica_info_from_id('ordinary', 1)
    assert persisted is not None
    assert persisted.zero_cost_materialization_sequence is None

    assert serve_state.add_or_update_replica('ordinary',
                                             1,
                                             info,
                                             expected_replica_exists=True)
    assert info.zero_cost_materialization_sequence == 1
    assert _protocol_sequences(observation_engine) == (1, 1, 1)


def test_legacy_gate_and_paid_success_do_not_consume_materialization(
        observation_engine, monkeypatch) -> None:
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    legacy = _zero_cost_replica(1)
    legacy.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    assert serve_state.add_or_update_replica('legacy', 1, legacy)
    assert legacy.zero_cost_admission_sequence is None
    assert legacy.zero_cost_materialization_sequence is None
    assert _protocol_sequences(observation_engine) == (0, 0, 0)

    repository = _repository(observation_engine)
    _activate(repository)
    paid = _zero_cost_replica(2)
    paid.is_zero_cost = False
    paid.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    assert serve_state.add_or_update_replica('paid', 2, paid)
    assert paid.zero_cost_admission_sequence is None
    assert paid.zero_cost_materialization_sequence is None
    assert _protocol_sequences(observation_engine) == (0, 0, 0)


def test_missing_existing_success_returns_false_without_counter_or_caller_leak(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    info = _zero_cost_replica(1)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED

    assert not serve_state.add_or_update_replica(
        'missing', 1, info, expected_replica_exists=True)
    assert info.zero_cost_admission_sequence is None
    assert info.zero_cost_materialization_sequence is None
    assert _protocol_sequences(observation_engine) == (0, 0, 0)


def test_existing_success_prelocks_protocol_even_with_stale_cost_provenance(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state.services_table).values(name='svc'))
        connection.execute(
            sqlalchemy.insert(
                serve_state.service_lifecycle_fences_table).values(name='svc',
                                                                   epoch=1))
    assert serve_state.add_or_update_replica('svc', 1, _zero_cost_replica(1))
    stale = serve_state.get_replica_info_from_id('svc', 1)
    assert stale is not None
    # Model a manager snapshot taken before the database assigned either
    # sequence.  Clearing the markers keeps the stale object internally valid;
    # the locked row remains the authority that restores all three fields.
    stale.is_zero_cost = False
    stale.zero_cost_admission_sequence = None
    stale.zero_cost_materialization_sequence = None
    stale.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED

    writer_entered_protocol = threading.Event()
    original_lock = serve_state._lock_zero_cost_protocol_sequence_for_update

    def signaled_lock(executor):
        writer_entered_protocol.set()
        return original_lock(executor)

    monkeypatch.setattr(serve_state,
                        '_lock_zero_cost_protocol_sequence_for_update',
                        signaled_lock)
    protocol = observation_schema.protocol_state_sequence_table
    with observation_engine.connect() as holder:
        transaction = holder.begin()
        holder.execute(
            sqlalchemy.select(protocol).where(
                protocol.c.id == 1).with_for_update()).one()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(serve_state.add_or_update_replica,
                                     'svc',
                                     1,
                                     stale,
                                     expected_replica_exists=True)
            assert writer_entered_protocol.wait(timeout=5)
            holder.execute(
                sqlalchemy.select(
                    serve_state.service_lifecycle_fences_table).where(
                        serve_state.service_lifecycle_fences_table.c.name ==
                        'svc').with_for_update()).one()
            holder.execute(
                sqlalchemy.select(serve_state.services_table.c.name).where(
                    serve_state.services_table.c.name ==
                    'svc').with_for_update()).one()
            transaction.commit()
            assert future.result(timeout=5) is True

    persisted = serve_state.get_replica_info_from_id('svc', 1)
    assert persisted is not None
    assert persisted.is_zero_cost is True
    assert persisted.zero_cost_materialization_sequence == 1
    assert _protocol_sequences(observation_engine) == (1, 1, 1)


def test_materialized_before_observation_is_not_double_debited(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    info = _zero_cost_replica(1)
    assert serve_state.add_or_update_replica('ordinary', 1, info)
    persisted = serve_state.get_replica_info_from_id('ordinary', 1)
    assert persisted is not None
    persisted.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    persisted.status_property.service_ready_now = True
    persisted.status_property.first_ready_time = 1.0
    assert serve_state.add_or_update_replica('ordinary',
                                             1,
                                             persisted,
                                             expected_replica_exists=True)
    lease = _begin_single_pool(repository)
    assert lease.materialization_sequence == 1

    feed, entitlement, _, _, _ = _sequenced_debit(lease)

    assert (feed, entitlement) == (0, 0)


def test_fill_admitted_after_observation_uses_sequence_not_created_at(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    _activate(repository)
    monkeypatch.setattr(serve_state._db_manager, '_engine', observation_engine)
    lease = _begin_single_pool(repository)
    info = _zero_cost_replica(1, context='claim-alias')
    info.created_at = lease.observed_at - 100
    info.reserved_fill = True
    info.reserved_fill_pool_key = lease.pool_key
    info.reserved_fill_service_generation = 1
    info.reserved_fill_physical_cluster_uid = 'physical-uid'
    info.reserved_fill_kubernetes_context = 'claim-alias'
    info.reserved_fill_allocation_generation = 1
    info.reserved_fill_allocation_input_sha256 = 'a' * 64
    info.reserved_fill_allocation_claim_generation = 1
    info.reserved_fill_observation_generation = lease.observation_generation
    info.reserved_fill_observation_sequence = lease.observation_sequence
    info.reserved_fill_intent_idempotency_key = 'b' * 64
    info.zero_cost_admission_sequence = 1
    protocol = observation_schema.protocol_state_sequence_table
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(protocol).where(
                protocol.c.id == 1,
                protocol.c.zero_cost_admission_sequence == 0).values(
                    zero_cost_admission_sequence=1))
        connection.execute(
            sqlalchemy.insert(serve_state.replicas_table).values(
                **serve_state._replica_row_values('claimant', 1, info)))

    feed, entitlement, _, live_fill, _ = _sequenced_debit(lease)

    assert (feed, entitlement) == (1, 1)
    assert live_fill == {'claimant': 1}


def test_sequenced_occupancy_scan_failure_is_not_spendable(
        observation_engine, monkeypatch) -> None:
    lease = _begin_single_pool(_repository(observation_engine))
    monkeypatch.setattr(
        serve_state, 'get_replica_infos_grouped', lambda:
        (_ for _ in ()).throw(RuntimeError('decode failed')))

    with pytest.raises(
            reserved_capacity_broker.IncompleteReplicaOccupancySnapshotError,
            match='decode failed'):
        _sequenced_debit(lease)


def test_begin_completion_and_exact_read_are_sequence_fenced(
        observation_engine) -> None:
    repository = _repository(observation_engine)
    lease = _begin(repository)
    assert lease.observation_generation == 1
    assert lease.observation_sequence == 0
    assert lease.ordinary_admission_sequence == 0
    assert lease.materialization_sequence == 0
    assert lease.observed_at < lease.lease_expires_at
    assert lease.observed_at < lease.valid_until

    with pytest.raises(observation.ObservationLeaseBusyError):
        _begin(repository)
    with observation_engine.connect() as connection:
        # The rejected begin rolls back without consuming commit order.
        sequence = connection.execute(
            sqlalchemy.text("""
                SELECT zero_cost_admission_sequence,
                       ordinary_zero_cost_admission_sequence
                FROM reserved_fill_protocol_state WHERE id = 1
            """)).one()
    assert tuple(sequence) == (0, 0)

    completed = repository.complete_success(lease,
                                            _success(),
                                            access_context=lease.access_context)
    assert completed.observation_generation == 1
    assert completed.observation_sequence == 0
    assert completed.ordinary_admission_sequence == 0
    assert completed.materialization_sequence == 0
    assert completed.payload.free_gpus == 3
    assert completed.payload_sha256
    assert repository.read_exact_completed(lease.pool_key, 1) == completed
    assert repository.read_latest_authoritative(lease.pool_key) == completed

    # Lost-response retry is idempotent and does not republish a timestamp.
    assert repository.complete_success(
        lease, _success(), access_context=lease.access_context) == completed


def test_concurrent_begin_has_one_durable_per_pool_lease(
        observation_engine) -> None:

    def begin(token: str) -> object:
        repository = observation.PoolCapacityObservationRepository(
            observation_engine,
            token_factory=lambda: uuid.UUID(token),
        )
        try:
            return _begin(repository)
        except observation.ObservationLeaseBusyError as error:
            return error

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(begin, (
                'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
                'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
            )))

    leases = [
        result for result in results
        if isinstance(result, observation.PoolCapacityObservationLease)
    ]
    busy = [
        result for result in results
        if isinstance(result, observation.ObservationLeaseBusyError)
    ]
    assert len(leases) == 1
    assert len(busy) == 1
    assert leases[0].observation_generation == 1
    assert leases[0].observation_sequence == 0
    assert leases[0].ordinary_admission_sequence == 0
    assert leases[0].materialization_sequence == 0
    with observation_engine.connect() as connection:
        counts = connection.execute(
            sqlalchemy.text("""
                SELECT count(*), max(observation_generation),
                       max(observation_sequence)
                FROM demand_capacity_observations
                WHERE pool_key = :pool_key
            """), {
                'pool_key': leases[0].pool_key,
            }).one()
    assert tuple(counts) == (1, 1, 0)


def test_cohort_skips_busy_pool_and_acquires_healthy_sibling(
        observation_engine) -> None:
    repository = _repository(observation_engine)
    busy = observation.PoolCapacityObservationRequest(
        pool_key=_pool_key('a100'),
        physical_cluster_uid='physical-uid',
        accelerator_names=('a100',),
        access_contexts=('east-context',))
    healthy = observation.PoolCapacityObservationRequest(
        pool_key=_pool_key('h200'),
        physical_cluster_uid='physical-uid',
        accelerator_names=('h200',),
        access_contexts=('west-context',))
    busy_lease = repository.begin_observations((busy,),
                                               lease_duration_seconds=60)[0]

    acquired = repository.begin_observations((busy, healthy),
                                             lease_duration_seconds=60)

    assert [lease.pool_key for lease in acquired] == [healthy.pool_key]
    assert acquired[0].observation_sequence == busy_lease.observation_sequence
    assert (acquired[0].ordinary_admission_sequence ==
            busy_lease.ordinary_admission_sequence)
    with observation_engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.text("""
                SELECT pool_key, count(*)
                FROM demand_capacity_observations
                WHERE pool_key IN (:busy, :healthy)
                GROUP BY pool_key ORDER BY pool_key
            """), {
                'busy': busy.pool_key,
                'healthy': healthy.pool_key,
            }).all()
    assert dict(rows) == {busy.pool_key: 1, healthy.pool_key: 1}


def test_cohort_captures_one_event_prefix_for_every_acquired_pool(
        observation_engine) -> None:
    repository = _repository(observation_engine)
    east = observation.PoolCapacityObservationRequest(
        pool_key=_pool_key('a100'),
        physical_cluster_uid='physical-uid',
        accelerator_names=('A100',),
        access_contexts=('east-context',))
    west = observation.PoolCapacityObservationRequest(
        pool_key=_pool_key('h200'),
        physical_cluster_uid='physical-uid',
        accelerator_names=('H200',),
        access_contexts=('west-context',))

    leases = repository.begin_observations((east, west),
                                           lease_duration_seconds=60)

    assert [lease.pool_key for lease in leases
           ] == [east.pool_key, west.pool_key]
    assert len({lease.observation_sequence for lease in leases}) == 1
    assert len({lease.ordinary_admission_sequence for lease in leases}) == 1
    assert len({lease.materialization_sequence for lease in leases}) == 1
    assert len({lease.observed_at for lease in leases}) == 1
    assert len({lease.valid_until for lease in leases}) == 1


def test_completed_observation_atomically_suppresses_duplicate_refresh(
        observation_engine) -> None:
    repository = _repository(observation_engine)
    lease = _begin(repository)
    repository.complete_success(lease,
                                _success(),
                                access_context=lease.access_context)

    with pytest.raises(observation.ObservationLeaseBusyError):
        _begin(repository, minimum_refresh_interval_seconds=60)

    with observation_engine.connect() as connection:
        sequence = connection.execute(
            sqlalchemy.text("""
                SELECT zero_cost_admission_sequence,
                       ordinary_zero_cost_admission_sequence
                FROM reserved_fill_protocol_state WHERE id = 1
            """)).one()
        generations = connection.execute(
            sqlalchemy.text("""
                SELECT count(*) FROM demand_capacity_observations
                WHERE pool_key = :pool_key
            """), {
                'pool_key': lease.pool_key,
            }).scalar_one()
    assert tuple(sequence) == (0, 0)
    assert generations == 1


def test_in_progress_successor_preserves_previous_completed_generation(
        observation_engine) -> None:
    repository = _repository(observation_engine)
    first_lease = _begin(repository)
    first = repository.complete_success(
        first_lease, _success(), access_context=first_lease.access_context)

    second_lease = _begin(repository)
    assert second_lease.observation_generation == 2
    assert second_lease.observation_sequence == 0
    assert second_lease.ordinary_admission_sequence == 0
    assert second_lease.materialization_sequence == 0
    assert repository.read_exact_completed(second_lease.pool_key, 2) is None
    assert repository.read_latest_authoritative(second_lease.pool_key) == first

    blackout = repository.complete_blackout(
        second_lease,
        observation.PoolCapacityBlackout(
            observation.PoolCapacityBlackoutReason.PERMISSION_DENIED,
            'pods/list forbidden'),
    )
    assert isinstance(blackout.payload, observation.PoolCapacityBlackout)
    assert repository.read_exact_completed(second_lease.pool_key, 2) == blackout
    assert repository.read_latest_completed(second_lease.pool_key) == blackout
    # The newer completed blackout invalidates the older success; no fallback.
    assert repository.read_latest_authoritative(second_lease.pool_key) is None


def test_success_persists_the_authenticated_winning_alias(
        observation_engine) -> None:
    repository = _repository(observation_engine)
    request = observation.PoolCapacityObservationRequest(
        pool_key=_pool_key('a100-80gb', 'h200'),
        physical_cluster_uid='physical-uid',
        accelerator_names=('a100-80gb', 'h200'),
        access_contexts=('research-primary', 'research-alias'))
    lease = repository.begin_observations((request,),
                                          lease_duration_seconds=60)[0]

    completed = repository.complete_success(lease,
                                            _success(),
                                            access_context='research-alias')

    assert completed.access_context == 'research-alias'
    assert repository.read_latest_authoritative(
        request.pool_key).access_context == 'research-alias'


def test_success_rejects_a_route_outside_the_acquired_alias_set(
        observation_engine) -> None:
    repository = _repository(observation_engine)
    request = observation.PoolCapacityObservationRequest(
        pool_key=_pool_key('a100-80gb', 'h200'),
        physical_cluster_uid='physical-uid',
        accelerator_names=('a100-80gb', 'h200'),
        access_contexts=('research-primary', 'research-alias'))
    lease = repository.begin_observations((request,),
                                          lease_duration_seconds=60)[0]

    with pytest.raises(ValueError, match='was not authorized'):
        repository.complete_success(lease,
                                    _success(),
                                    access_context='untrusted-context')

    assert repository.read_latest_authoritative(request.pool_key) is None


def test_expired_writer_cannot_overwrite_successor(observation_engine) -> None:
    repository = _repository(observation_engine)
    expired = _begin(repository)
    table = observation_schema.demand_capacity_observations_v2_table
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(table).where(
                table.c.context == expired.row_key).values(
                    lease_expires_at=expired.observed_at))

    successor = _begin(repository)
    assert successor.observation_generation == 2
    with pytest.raises(observation.StaleObservationWriterError):
        repository.complete_success(expired,
                                    _success(),
                                    access_context=expired.access_context)
    completed = repository.complete_success(
        successor, _success(), access_context=successor.access_context)
    assert completed.observation_generation == 2
    assert repository.read_latest_authoritative(successor.pool_key) == completed


def test_identity_change_and_legacy_projection_tamper_fail_closed(
        observation_engine) -> None:
    repository = _repository(observation_engine)
    lease = _begin(repository)

    retargeted = dataclasses.replace(lease,
                                     physical_cluster_uid='different-uid')
    with pytest.raises(observation.StaleObservationWriterError):
        repository.complete_success(retargeted,
                                    _success(),
                                    access_context=retargeted.access_context)

    completed = repository.complete_success(lease,
                                            _success(),
                                            access_context=lease.access_context)
    table = observation_schema.demand_capacity_observations_v2_table
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(table).where(
                table.c.context == lease.row_key).values(
                    availability='{"a100-80gb": 999}'))

    assert repository.read_exact_completed(lease.pool_key, 1) is None
    assert repository.read_latest_authoritative(lease.pool_key) is None
    assert completed.payload_sha256 is not None


def test_completion_accepts_database_json_array_identity(
        observation_engine) -> None:
    """PostgreSQL JSONB decodes accelerator_names as a mutable list."""
    repository = _repository(observation_engine)
    lease = _begin(repository)

    completed = repository.complete_success(lease,
                                            _success(),
                                            access_context=lease.access_context)

    assert completed.accelerator_names == ('a100-80gb', 'h200')


def test_sequenced_round_publication_requires_exact_fresh_provenance(
        observation_engine, monkeypatch) -> None:
    repository = _repository(observation_engine)
    lease = _begin(repository)
    completed = repository.complete_success(lease,
                                            _success(),
                                            access_context=lease.access_context)
    assert isinstance(completed.payload, observation.PoolCapacitySuccess)
    _activate(repository)
    with observation_engine.begin() as connection:
        connection.execute(
            sqlalchemy.text("""
                INSERT INTO reserved_fill_lease (id, epoch, expires_at)
                VALUES (1, 7, NULL)
            """))
    monkeypatch.setattr(
        serve_state._db_manager,
        '_engine',  # pylint: disable=protected-access
        observation_engine)

    common = {
        'round_id': 1,
        'snapshot_time': completed.observed_at,
        'epoch': 1,
        'grants': '{}',
        'feeds': '{}',
        'raw_grants': '{}',
        'feed_state': '{}',
        'sum_holdings': 0,
        'last_observed_free': completed.payload.free_gpus,
        'last_observed_free_ts': completed.observed_at,
        'phantom_streak': 0,
        'shrink_baseline': None,
        'lease_token': 7,
        'lease_expires_at': completed.valid_until,
        'protocol_version': 2,
        'claim_generations': {},
    }
    assert not serve_state.publish_reserved_fill_round(completed.pool_key, **
                                                       common)
    with pytest.raises(ValueError, match='all present or all absent'):
        serve_state.publish_reserved_fill_round(
            completed.pool_key,
            observation_generation=completed.observation_generation,
            observation_sequence=completed.observation_sequence,
            observation_payload_sha256='f' * 64,
            **common)
    assert not serve_state.publish_reserved_fill_round(
        completed.pool_key,
        observation_generation=completed.observation_generation,
        observation_sequence=completed.observation_sequence,
        observation_materialization_sequence=(
            completed.materialization_sequence),
        observation_payload_sha256='f' * 64,
        **common)
    assert serve_state.publish_reserved_fill_round(
        completed.pool_key,
        observation_generation=completed.observation_generation,
        observation_sequence=completed.observation_sequence,
        observation_materialization_sequence=(
            completed.materialization_sequence),
        observation_payload_sha256=completed.payload_sha256,
        **common)

    round_row = serve_state.get_reserved_fill_round(completed.pool_key)
    assert round_row is not None
    assert round_row['snapshot_time'] == completed.observed_at
    assert (
        round_row['observation_generation'] == completed.observation_generation)
    assert round_row['observation_sequence'] == completed.observation_sequence
    assert (round_row['observation_materialization_sequence'] ==
            completed.materialization_sequence)
    assert (round_row['observation_payload_sha256'] == completed.payload_sha256)
