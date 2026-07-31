"""Tests for the revision-001 physical-capacity transaction repository."""
# pylint: disable=protected-access,redefined-outer-name

import concurrent.futures
import copy
import os
from unittest import mock
import uuid

import pytest
import sqlalchemy
from sqlalchemy import orm

from sky.physical_capacity import canonical
from sky.physical_capacity import models
from sky.physical_capacity import schema
from sky.physical_capacity import state
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(name='physical_capacity_schema_pg')


def test_repository_is_lazy_and_shares_default_engine_namespace():
    assert state._db_manager._engine_namespace is None
    sentinel = mock.Mock(spec=sqlalchemy.engine.Engine)
    with mock.patch.object(state._db_manager,
                           'get_engine',
                           return_value=sentinel) as get_engine:
        assert state.initialize_and_get_db() is sentinel
    get_engine.assert_called_once_with()


def test_initialize_schema_rejects_sqlite_before_alembic():
    sqlite_engine = sqlalchemy.create_engine('sqlite://')
    with mock.patch.object(migration_utils, 'safe_alembic_upgrade') as upgrade:
        with pytest.raises(RuntimeError, match='requires PostgreSQL'):
            state.initialize_schema(sqlite_engine, mode='verify')
    upgrade.assert_not_called()
    sqlite_engine.dispose()


@pytest.mark.parametrize('mode', ['auto', 'upgrade', 'bootstrap', 'verify'])
def test_initialize_schema_forwards_explicit_mode(mode):
    engine = mock.Mock()
    engine.dialect.name = 'postgresql'
    with mock.patch.object(migration_utils, 'safe_alembic_upgrade') as upgrade:
        state.initialize_schema(engine, mode=mode)
    upgrade.assert_called_once_with(
        engine,
        migration_utils.CAPACITY_STATE_DB_NAME,
        migration_utils.CAPACITY_STATE_VERSION,
        mode=mode,
    )


def test_initialize_schema_forwards_configured_mode():
    engine = mock.Mock()
    engine.dialect.name = 'postgresql'
    with mock.patch.object(
            migration_utils, 'configured_migration_mode',
            return_value='bootstrap') as configured, mock.patch.object(
                migration_utils, 'safe_alembic_upgrade') as upgrade:
        state.initialize_schema(engine)
    configured.assert_called_once_with()
    upgrade.assert_called_once_with(
        engine,
        migration_utils.CAPACITY_STATE_DB_NAME,
        migration_utils.CAPACITY_STATE_VERSION,
        mode='bootstrap',
    )


def test_transaction_uses_caller_executor_without_nested_initialization():
    caller_executor = object()
    with mock.patch.object(state, 'initialize_and_get_db') as initialize:
        with state.transaction(caller_executor) as yielded:
            assert yielded is caller_executor
    initialize.assert_not_called()


@pytest.fixture(scope='module')
def postgres_engine():
    postgres_uri = os.environ.get('SKYPILOT_TEST_POSTGRES_URI')
    if postgres_uri is not None:
        engine = sqlalchemy.create_engine(postgres_uri,
                                          pool_size=8,
                                          max_overflow=0)
        try:
            yield engine
        finally:
            engine.dispose()
        return

    testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
    container = None
    try:
        container = testcontainers_postgres.PostgresContainer('postgres:16')
        container.start()
    except Exception as e:  # pylint: disable=broad-except
        # Host Docker availability is outside the test's control.
        pytest.skip(f'could not start postgres container: {e}')
    assert container is not None
    engine = sqlalchemy.create_engine(container.get_connection_url(),
                                      pool_size=8,
                                      max_overflow=0)
    try:
        yield engine
    finally:
        engine.dispose()
        container.stop()


@pytest.fixture
def capacity_database(postgres_engine, monkeypatch):
    schema.METADATA.drop_all(postgres_engine, checkfirst=True)
    schema.METADATA.create_all(postgres_engine)
    monkeypatch.setattr(state._db_manager, '_engine', postgres_engine)
    yield postgres_engine
    schema.METADATA.drop_all(postgres_engine, checkfirst=True)


def _hash(label: str, domain: canonical.CanonicalDomain) -> str:
    return canonical.canonical_hash({'fixture': label}, domain=domain)


def _fixture_rows(label: str = 'a'):
    group_id = uuid.uuid5(uuid.NAMESPACE_DNS, f'group-{label}')
    allocation_id = uuid.uuid5(uuid.NAMESPACE_DNS, f'allocation-{label}')
    placement = {'fixture': f'placement-{label}'}
    topology = {'fixture': 'single-allocation'}
    group = {
        'group_id': group_id,
        'workspace': 'default',
        'owner_kind': models.OwnerKind.SERVICE,
        'owner_id': f'service-{label}',
        'owner_incarnation': f'incarnation-{label}',
        'writer_fence_kind': models.WriterFenceKind.LEGACY,
        'writer_controller_fingerprint': None,
        'writer_instance_id': None,
        'writer_fence_epoch': None,
        'source_kind': models.ProjectionSourceKind.SERVE_SERVICE,
        'source_key': f'service-{label}',
        'source_incarnation_hash': _hash(
            f'group-source-{label}',
            canonical.CanonicalDomain.SOURCE_INCARNATION),
        'projection_confidence': models.ProjectionConfidence.LEGACY,
        'current_intent_generation': 1,
        'lifecycle_state': models.GroupLifecycleState.ACTIVE,
        'created_by_actor_id': 'fixture',
        'updated_by_actor_id': 'fixture',
        'created_by_actor_type': models.ActorType.SYSTEM,
        'updated_by_actor_type': models.ActorType.SYSTEM,
    }
    intent = {
        'group_id': group_id,
        'workspace': 'default',
        'intent_generation': 1,
        'schema_version': 1,
        'placement_contract': placement,
        'placement_contract_hash': canonical.canonical_hash(
            placement, domain=canonical.CanonicalDomain.PLACEMENT_CONTRACT),
        'desired_count': 1,
        'topology': topology,
        'intent_hash': _hash(f'intent-{label}',
                             canonical.CanonicalDomain.INTENT),
        'source_fingerprint': _hash(
            f'fingerprint-{label}',
            canonical.CanonicalDomain.SOURCE_FINGERPRINT),
        'created_by_actor_id': 'fixture',
        'created_by_actor_type': models.ActorType.SYSTEM,
    }
    allocation = {
        'allocation_id': allocation_id,
        'group_id': group_id,
        'workspace': 'default',
        'created_by_intent_generation': 1,
        'source_kind': models.AllocationSourceKind.SERVE_REPLICA,
        'source_key': f'replica-{label}',
        'source_incarnation_hash': _hash(
            f'allocation-source-{label}',
            canonical.CanonicalDomain.SOURCE_INCARNATION),
        'identity_confidence': models.AllocationIdentityConfidence.LEGACY,
        'cluster_name': f'cluster-{label}',
        'cluster_hash': f'cluster-generation-{label}',
        'lifecycle_state': models.GroupLifecycleState.ACTIVE,
        'projection_state': models.AllocationProjectionState.CURRENT,
        'observed_state': models.AllocationObservedState.UNKNOWN,
        'observation_certainty': models.ObservationCertainty.LEGACY,
    }
    desire = {
        'group_id': group_id,
        'workspace': 'default',
        'intent_generation': 1,
        'allocation_id': allocation_id,
        'ordinal': 0,
        'desired_state': models.DesiredState.PRESENT,
        'release_gate': models.ReleaseGate.BLOCKED,
        'reason_code': models.DesireReasonCode.PROJECTION,
    }
    return group, intent, allocation, desire


def _publish(rows):
    group, intent, allocation, desire = rows
    return state.publish_initial_projection_for_test(
        group=group,
        intent=intent,
        allocations=[allocation],
        desires=[desire],
    )


def _row_count(engine, table: sqlalchemy.Table) -> int:
    with engine.connect() as connection:
        return connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()  # pylint: disable=not-callable
                             ).select_from(table)).scalar_one()


def test_initial_projection_is_atomic_and_idempotent(capacity_database):
    rows = _fixture_rows()
    first = _publish(rows)
    second = _publish(copy.deepcopy(rows))

    assert first['group_id'] == second['group_id']
    assert _row_count(capacity_database, schema.GROUPS) == 1
    assert _row_count(capacity_database, schema.GROUP_INTENTS) == 1
    assert _row_count(capacity_database, schema.ALLOCATIONS) == 1
    assert _row_count(capacity_database, schema.ALLOCATION_DESIRES) == 1


def test_idempotency_collision_rejects_changed_immutable_value(
        capacity_database):
    rows = _fixture_rows()
    _publish(rows)
    changed = copy.deepcopy(rows)
    changed[0]['created_by_actor_id'] = 'different-actor'

    with pytest.raises(state.ImmutableRowConflictError,
                       match='created_by_actor_id'):
        _publish(changed)
    assert _row_count(capacity_database, schema.GROUPS) == 1


def test_immutable_intent_and_desire_collisions_are_rejected(capacity_database):
    group, intent, allocation, desire = _fixture_rows()
    state.publish_initial_projection_for_test(
        group=group,
        intent=intent,
        allocations=[allocation],
        desires=[desire],
    )

    changed_intent = copy.deepcopy(intent)
    changed_intent['desired_count'] = 2
    with pytest.raises(state.ImmutableRowConflictError, match='desired_count'):
        state.insert_intent_for_test(changed_intent)

    changed_desire = copy.deepcopy(desire)
    changed_desire['reason_code'] = models.DesireReasonCode.CARRY_FORWARD
    with pytest.raises(state.ImmutableRowConflictError, match='reason_code'):
        state.insert_desire_for_test(changed_desire)
    assert _row_count(capacity_database, schema.GROUP_INTENTS) == 1
    assert _row_count(capacity_database, schema.ALLOCATION_DESIRES) == 1


@pytest.mark.parametrize(('table', 'version_field'), [
    (schema.PROJECTION_SCANS, 'cursor_schema_version'),
    (schema.GROUP_INTENTS, 'schema_version'),
    (schema.ALLOCATIONS, 'spec_schema_version'),
])
def test_revision_001_fixture_writes_reject_future_schema_versions(
        table: sqlalchemy.Table, version_field: str) -> None:
    with pytest.raises(ValueError, match=f'{version_field}=1'):
        state._prepare_values(table, {version_field: 2})


def test_concurrent_identical_projection_publishes_once(capacity_database):
    rows = _fixture_rows()

    def publish_one(_):
        return _publish(copy.deepcopy(rows))['intent_generation']

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        generations = list(executor.map(publish_one, range(4)))

    assert generations == [1, 1, 1, 1]
    assert _row_count(capacity_database, schema.GROUPS) == 1
    assert _row_count(capacity_database, schema.GROUP_INTENTS) == 1
    assert _row_count(capacity_database, schema.ALLOCATIONS) == 1
    assert _row_count(capacity_database, schema.ALLOCATION_DESIRES) == 1


def test_caller_connection_owns_rollback(capacity_database):
    rows = _fixture_rows()
    with pytest.raises(RuntimeError, match='rollback fixture'):
        with capacity_database.begin() as connection:
            group, intent, allocation, desire = rows
            state.publish_initial_projection_for_test(
                group=group,
                intent=intent,
                allocations=[allocation],
                desires=[desire],
                executor=connection,
            )
            raise RuntimeError('rollback fixture')

    assert _row_count(capacity_database, schema.GROUPS) == 0
    assert _row_count(capacity_database, schema.GROUP_INTENTS) == 0


def test_caller_session_owns_commit(capacity_database):
    rows = _fixture_rows()
    with orm.Session(capacity_database) as session:
        with session.begin():
            group, intent, allocation, desire = rows
            state.publish_initial_projection_for_test(
                group=group,
                intent=intent,
                allocations=[allocation],
                desires=[desire],
                executor=session,
            )

    assert _row_count(capacity_database, schema.GROUPS) == 1
    assert _row_count(capacity_database, schema.GROUP_INTENTS) == 1


def test_intent_advance_is_monotonic_for_a_to_b_to_a(capacity_database):
    group, first_intent, allocation, desire = _fixture_rows()
    state.publish_initial_projection_for_test(
        group=group,
        intent=first_intent,
        allocations=[allocation],
        desires=[desire],
    )

    second_intent = copy.deepcopy(first_intent)
    second_intent.update({
        'intent_generation': 2,
        'intent_hash': _hash('intent-b', canonical.CanonicalDomain.INTENT),
        'source_fingerprint': _hash(
            'fingerprint-b', canonical.CanonicalDomain.SOURCE_FINGERPRINT),
    })
    carried_desire = {
        'allocation_id': allocation['allocation_id'],
        'ordinal': 0,
        'desired_state': models.DesiredState.PRESENT,
        'release_gate': models.ReleaseGate.BLOCKED,
        'reason_code': models.DesireReasonCode.CARRY_FORWARD,
    }
    second = state.advance_intent_for_test(
        group_id=group['group_id'],
        workspace=group['workspace'],
        intent=second_intent,
        desires=[carried_desire],
    )

    third_intent = copy.deepcopy(first_intent)
    third_intent.update({
        'intent_generation': 3,
        'source_fingerprint': _hash(
            'fingerprint-a-again',
            canonical.CanonicalDomain.SOURCE_FINGERPRINT),
    })
    third = state.advance_intent_for_test(
        group_id=group['group_id'],
        workspace=group['workspace'],
        intent=third_intent,
        desires=[carried_desire],
    )
    repeated = state.advance_intent_for_test(
        group_id=group['group_id'],
        workspace=group['workspace'],
        intent=third_intent,
        desires=[carried_desire],
    )

    assert second['intent_generation'] == 2
    assert third['intent_generation'] == 3
    assert repeated['intent_generation'] == 3
    assert _row_count(capacity_database, schema.GROUP_INTENTS) == 3
    with capacity_database.connect() as connection:
        current_generation = connection.execute(
            sqlalchemy.select(
                schema.GROUPS.c.current_intent_generation)).scalar_one()
    assert current_generation == 3


def test_last_seen_scan_must_match_workspace_and_source_kind(capacity_database):
    service_scan_id = uuid.uuid4()
    service_scan = {
        'scan_id': service_scan_id,
        'workspace': 'default',
        'source_kind': models.ProjectionSourceKind.SERVE_SERVICE,
        'source_partition_hash': _hash(
            'service-partition', canonical.CanonicalDomain.SOURCE_PARTITION),
        'cursor': {},
        'state': models.ProjectionScanState.RUNNING,
    }
    state.insert_scan_for_test(service_scan)
    group, intent, allocation, desire = _fixture_rows()
    group['last_seen_scan_id'] = service_scan_id
    allocation['last_seen_scan_id'] = service_scan_id
    state.publish_initial_projection_for_test(
        group=group,
        intent=intent,
        allocations=[allocation],
        desires=[desire],
    )

    pool_scan_id = uuid.uuid4()
    pool_scan = {
        **service_scan,
        'scan_id': pool_scan_id,
        'source_kind': models.ProjectionSourceKind.SERVE_POOL,
        'source_partition_hash': _hash(
            'pool-partition', canonical.CanonicalDomain.SOURCE_PARTITION),
    }
    state.insert_scan_for_test(pool_scan)
    mismatched_group, _, _, _ = _fixture_rows('mismatched')
    mismatched_group['last_seen_scan_id'] = pool_scan_id
    with pytest.raises(ValueError, match='workspace and source kind'):
        with capacity_database.begin() as connection:
            state.insert_group_for_test(mismatched_group, executor=connection)

    _, _, mismatched_allocation, _ = _fixture_rows('mismatched-allocation')
    mismatched_allocation.update({
        'group_id': group['group_id'],
        'workspace': group['workspace'],
        'created_by_intent_generation': 1,
        'last_seen_scan_id': pool_scan_id,
    })
    with pytest.raises(ValueError, match='workspace and source kind'):
        state.insert_allocation_for_test(mismatched_allocation)


def test_scan_insert_validates_bounds_and_is_idempotent(capacity_database):
    scan_id = uuid.uuid4()
    cursor = {'after': None}
    scan = {
        'scan_id': scan_id,
        'workspace': 'default',
        'source_kind': models.ProjectionSourceKind.SERVE_SERVICE,
        'source_partition_hash': canonical.canonical_hash(
            {'partition': 'fixture'},
            domain=canonical.CanonicalDomain.SOURCE_PARTITION),
        'cursor_schema_version': 1,
        'cursor': cursor,
        'state': models.ProjectionScanState.RUNNING,
        'finding_counts': {},
    }
    first = state.insert_scan_for_test(scan)
    second = state.insert_scan_for_test(copy.deepcopy(scan))
    assert first['scan_id'] == second['scan_id']
    assert _row_count(capacity_database, schema.PROJECTION_SCANS) == 1

    changed = copy.deepcopy(scan)
    changed['cursor'] = {'after': 'different'}
    with pytest.raises(state.ImmutableRowConflictError, match='cursor'):
        state.insert_scan_for_test(changed)

    oversized = copy.deepcopy(scan)
    oversized['scan_id'] = uuid.uuid4()
    oversized['workspace'] = 'x' * (canonical.MAX_WORKSPACE_IDENTIFIER_BYTES +
                                    1)
    with pytest.raises(ValueError, match='workspace'):
        state.insert_scan_for_test(oversized)
