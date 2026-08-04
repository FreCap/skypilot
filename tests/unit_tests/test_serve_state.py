"""Tests for serve_state.

Focused on the new controller_ip column + atomic update introduced for HA
leader-aware routing.
"""
# Pytest fixture name collides with pylint's "private name" rule (leading
# underscore is the standard convention for fixtures injected for side
# effects). Disable for the file.
# pylint: disable=invalid-name,protected-access
import contextlib
import json
import pickle
import sqlite3
import types
import uuid

import pytest
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sky import clouds
from sky.serve import constants as serve_constants
from sky.serve import paid_capacity
from sky.serve import replica_info as replica_info_lib
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import system_oom_recovery
from sky.serve import system_recovery_state as recovery_state
from sky.utils import common_utils
from sky.utils.db import migration_utils


def _replica(replica_id: int,
             cluster_name: str | None = None,
             version: int = 1) -> replica_managers.ReplicaInfo:
    return replica_managers.ReplicaInfo(
        replica_id=replica_id,
        cluster_name=cluster_name or f'svc-{replica_id}',
        replica_port='8080',
        is_spot=False,
        location=None,
        version=version,
        resources_override=None,
    )


class _FakeSpec:

    def __init__(self, policy: str, load_balancing_policy: str):
        self._policy = policy
        self.load_balancing_policy = load_balancing_policy

    def autoscaling_policy_str(self):
        return self._policy


def _read_row(engine, name):
    """Read raw services row directly (bypassing get_service_from_name which
    does an INNER JOIN with version_specs and would skip rows without a
    version registered)."""
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(serve_state.services_table).where(
                serve_state.services_table.c.name == name)).fetchone()
    return None if result is None else dict(result._mapping)  # pylint: disable=protected-access


def _read_version_row(engine, name, version):
    with orm.Session(engine) as session:
        result = session.execute(
            sqlalchemy.select(serve_state.version_specs_table).where(
                serve_state.version_specs_table.c.service_name == name,
                serve_state.version_specs_table.c.version ==
                version)).fetchone()
    return None if result is None else dict(result._mapping)  # pylint: disable=protected-access


@contextlib.contextmanager
def _count_sql_statements(engine):
    counts = {'n': 0}

    def _count(*args, **kwargs):
        del args, kwargs
        counts['n'] += 1

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _count)
    try:
        yield counts
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute', _count)


@pytest.fixture
def _mock_serve_db(tmp_path, monkeypatch):
    """Point serve_state at a fresh sqlite DB for the duration of one test."""
    db_path = tmp_path / 'serve_state_testing.db'
    engine = create_engine(f'sqlite:///{db_path}')

    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    # `metadata.create_all` only creates tables that don't already exist; this
    # is enough since we're starting from a brand-new DB and the alembic
    # upgrade step in `create_table` would otherwise need a full env.
    serve_state.Base.metadata.create_all(engine)
    yield engine


def _add_minimal_service(name: str,
                         controller_ip=None,
                         controller_pid=12345,
                         service_hash=None,
                         lifecycle_epoch=None,
                         resource_scope=None,
                         workspace=None,
                         yaml_content='yaml: v1',
                         pool=False,
                         spec=None,
                         created_by=None,
                         submitted_yaml_content=None,
                         placement_catalog=None):
    """Add a service row with all-required-args defaults so individual tests
    only need to specify what they care about."""
    return serve_state.add_service(
        name=name,
        controller_job_id=1,
        policy='policy',
        requested_resources_str='1x[CPU:1+]',
        load_balancing_policy='round_robin',
        status=serve_state.ServiceStatus.CONTROLLER_INIT,
        tls_encrypted=False,
        pool=pool,
        controller_pid=controller_pid,
        entrypoint='entry',
        # A None spec is stored as pickled None (like `add_version` does), so
        # the read path (`_get_service_from_row`) skips the spec-dependent
        # fields instead of calling SkyServiceSpec methods on it.
        spec=spec,
        yaml_content=yaml_content,
        workspace=workspace,
        controller_ip=controller_ip,
        service_hash=service_hash,
        lifecycle_epoch=lifecycle_epoch,
        resource_scope=resource_scope,
        created_by=created_by,
        submitted_yaml_content=submitted_yaml_content,
        placement_catalog=placement_catalog,
    )


def _insert_orphan_service_row(engine, name: str, pool: bool = False):
    """Insert a `services` row with no `version_specs` row.

    Simulates the debris stranded by the pre-atomic registration path (or an
    interrupted teardown) on an older controller; `add_service` can no longer
    produce this state."""
    with orm.Session(engine) as session:
        session.execute(serve_state.services_table.insert().values(
            name=name,
            controller_job_id=1,
            status=serve_state.ServiceStatus.CONTROLLER_INIT.value,
            requested_resources_str='1x[CPU:1+]',
            pool=int(pool),
            controller_pid=12345,
            hash='orphan',
            entrypoint='entry'))
        session.commit()


def test_system_recovery_persistence_fails_closed_on_sqlite(_mock_serve_db):
    assert not serve_state.system_recovery_persistence_available()
    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='PostgreSQL'):
        serve_state.patch_replica_system_recovery(
            'svc',
            1,
            _replica(1),
            expected_service_hash='hash',
            expected_lifecycle_epoch=1,
            expected_controller_owner=(123, '10.0.0.1'),
            expected_revision=0)


def test_system_recovery_transaction_advances_revision_once(
        _mock_serve_db, monkeypatch):
    """Exercise the reducer transaction independent of PostgreSQL syntax."""
    engine = _mock_serve_db
    owner = (123, '10.0.0.1')
    with engine.begin() as connection:
        connection.execute(
            serve_state.service_lifecycle_fences_table.insert().values(
                name='svc', epoch=1))
    assert _add_minimal_service('svc',
                                controller_pid=owner[0],
                                controller_ip=owner[1],
                                service_hash='service-hash',
                                lifecycle_epoch=1,
                                workspace='default')
    ordinary = _replica(7, version=3)
    assert serve_state.add_or_update_replica(
        'svc',
        7,
        ordinary,
        expected_service_hash='service-hash',
        expected_lifecycle_epoch=1,
        expected_controller_owner=owner)

    # PostgreSQL row-lock behavior is covered in the real-PG companion suite.
    # Bypass only the dialect admission guard here to keep transition logic
    # executable on every developer machine.
    monkeypatch.setattr(serve_state, '_require_system_recovery_postgres',
                        lambda: engine)
    digest = 'a' * 64
    intent = recovery_state.SystemRecoveryLaunchIntent(
        version=1,
        controller_contract_version=2,
        recovery_authorization_version=3,
        recovery_authorization_profile_id='boltz-l4-v3',
        recovery_authorization_sha256=digest,
        runtime_profile_version=2,
        expected_runtime_capability=recovery_state.SYSTEM_RECOVERY_CAPABILITY,
        service_hash='service-hash',
        replica_id=7,
        launch_generation=7,
        launch_nonce='b' * 64,
        workspace='default',
        resource_envelope_sha256=digest,
        task_sha256=digest,
        runtime_image_digest=f'sha256:{digest}',
        owned_container_spec_sha256=digest,
        execution_envelope_sha256=digest)
    desired = serve_state.get_replica_info_from_id('svc', 7)
    assert desired is not None
    desired.system_recovery_launch_intent = intent
    desired.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.CANDIDATE)
    candidate = serve_state.create_replica_system_recovery_candidate(
        'svc',
        7,
        desired,
        expected_service_hash='service-hash',
        expected_lifecycle_epoch=1,
        expected_controller_owner=owner,
        expected_revision=0)
    assert candidate.system_recovery_revision == 1

    unbound = system_oom_recovery.create_unbound_launch_context(
        intent,
        service_name='svc',
        service_version=3,
        controller_pid=owner[0],
        controller_ip=owner[1])
    bound = serve_state.bind_replica_system_recovery_launch_request(
        unbound, 'request-1')
    assert bound.launch_request_id == 'request-1'
    assert bound.system_recovery_revision == 2


def test_placement_policy_state_is_separate_and_owner_fenced(_mock_serve_db):
    owner = (123, '10.0.0.1')
    _add_minimal_service('svc',
                         controller_pid=owner[0],
                         controller_ip=owner[1],
                         service_hash='incarnation-a')

    spot_state = {'version': 1, 'benches': [{'reason': 'capacity'}]}
    rebalance_state = {'version': 1, 'candidates': [{'replica_id': 7}]}
    assert serve_state.set_service_spot_placement_state('svc', 'incarnation-a',
                                                        owner, spot_state)
    assert not serve_state.set_service_cost_rebalance_state(
        'svc', 'incarnation-a', (999, '10.0.0.9'), rebalance_state)
    assert serve_state.set_service_cost_rebalance_state('svc', 'incarnation-a',
                                                        owner, rebalance_state)

    assert serve_state.get_service_placement_policy_states('svc') == {
        'spot_placement_state': spot_state,
        'cost_rebalance_state': rebalance_state,
    }
    assert not serve_state.set_service_spot_placement_state(
        'svc', 'stale-incarnation', owner, {
            'version': 1,
            'benches': []
        })
    assert serve_state.get_service_placement_policy_states('missing') is None


def test_launch_budget_counts_share_one_replica_scan(_mock_serve_db,
                                                     monkeypatch):
    """Provisioning and termination occupancy are counted in one SQL query.

    A row can satisfy both predicates while a launched replica is being
    terminated; preserve the historical independent-count semantics.
    """
    provisioning = replica_managers.ReplicaInfo(replica_id=1,
                                                cluster_name='svc-1',
                                                replica_port='8080',
                                                is_spot=False,
                                                location=None,
                                                version=1,
                                                resources_override=None)
    provisioning.status_property.sky_launch_status = (
        common_utils.ProcessStatus.RUNNING)
    terminating = replica_managers.ReplicaInfo(replica_id=2,
                                               cluster_name='svc-2',
                                               replica_port='8080',
                                               is_spot=False,
                                               location=None,
                                               version=1,
                                               resources_override=None)
    terminating.status_property.sky_launch_status = (
        common_utils.ProcessStatus.RUNNING)
    terminating.status_property.sky_down_status = (
        common_utils.ProcessStatus.RUNNING)
    serve_state.add_or_update_replica('svc', 1, provisioning)
    serve_state.add_or_update_replica('svc', 2, terminating)

    original_loads = serve_state.pickle.loads
    unpickles = 0

    def _counting_loads(value):
        nonlocal unpickles
        unpickles += 1
        return original_loads(value)

    monkeypatch.setattr(serve_state.pickle, 'loads', _counting_loads)
    with _count_sql_statements(_mock_serve_db) as counts:
        assert serve_state.get_replica_launch_budget_counts() == (2, 1)
    assert counts['n'] == 1
    assert unpickles == 0


def test_replica_json_storage_round_trip_preserves_lifecycle_state():
    info = _replica(7, version=3)
    info.planned_capacity = 8
    info.unknown_capacity_replacement = True
    info.logical_bridge_capacity_verified = True
    info.created_at = 123.5
    info.first_not_ready_time = 124.5
    info.first_consecutive_failure_time = 125.5
    info.reserved_fill = True
    info.cost_rebalance_for_replica_id = 4
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    info.status_property.service_ready_now = True
    info.status_property.first_ready_time = 126.5
    info.status_property.sky_down_status = common_utils.ProcessStatus.SCHEDULED
    info.status_property.is_scale_down = True
    info.status_property.drain_cap_seconds = 30
    info.status_property.drain_started_at = 1234.5
    info.status_property.wait_for_idle_before_termination = True
    info.status_property.logical_retirement_version = 3
    info.status_property.logical_retirement_controller_epoch = 'epoch-a'
    info.status_property.logical_retirement_generation = 10
    info.status_property.logical_retirement_target_capacity = 17
    info.status_property.logical_retirement_confirmed_generation = 11
    info.status_property.logical_retirement_bounded_deadline = True
    info.status_property.logical_retirement_committed = True

    restored = replica_managers.ReplicaInfo.from_storage_dict(
        info.to_storage_dict())

    assert restored.to_storage_dict() == info.to_storage_dict()
    assert restored.unknown_capacity_replacement is True
    assert restored.logical_bridge_capacity_verified is True
    assert restored.status == info.status

    legacy_state = info.to_storage_dict()
    legacy_state['status_property'].pop('logical_retirement_bounded_deadline')
    legacy_restored = replica_managers.ReplicaInfo.from_storage_dict(
        legacy_state)
    assert not legacy_restored.status_property.logical_retirement_bounded_deadline

    malformed_state = info.to_storage_dict()
    malformed_state['status_property'][
        'logical_retirement_bounded_deadline'] = 'true'
    malformed_restored = replica_managers.ReplicaInfo.from_storage_dict(
        malformed_state)
    assert not malformed_restored.status_property.logical_retirement_bounded_deadline

    legacy_commit_state = info.to_storage_dict()
    legacy_commit_state['status_property'].pop('logical_retirement_committed')
    legacy_commit_restored = replica_managers.ReplicaInfo.from_storage_dict(
        legacy_commit_state)
    assert (legacy_commit_restored.status_property.logical_retirement_committed
            is None)

    malformed_commit_state = info.to_storage_dict()
    malformed_commit_state['status_property'][
        'logical_retirement_committed'] = 'true'
    malformed_commit_restored = replica_managers.ReplicaInfo.from_storage_dict(
        malformed_commit_state)
    assert (
        malformed_commit_restored.status_property.logical_retirement_committed
        is None)

    legacy_drain_state = info.to_storage_dict()
    legacy_drain_state['status_property'].pop('drain_started_at')
    legacy_drain_restored = replica_managers.ReplicaInfo.from_storage_dict(
        legacy_drain_state)
    assert legacy_drain_restored.status_property.drain_started_at is None

    for malformed_started_at in (True, 0, -1, float('inf'), '1234.5'):
        malformed_drain_state = info.to_storage_dict()
        malformed_drain_state['status_property'][
            'drain_started_at'] = malformed_started_at
        malformed_drain_restored = (replica_managers.ReplicaInfo.
                                    from_storage_dict(malformed_drain_state))
        assert malformed_drain_restored.status_property.drain_started_at is None


def test_replica_json_storage_preserves_region_independent_image_id():
    image = 'docker:example.invalid/boltz:model'
    resource_state = {
        'cloud': 'Kubernetes',
        'region': 'prod_research_cluster_eks',
        'zone': None,
        'accelerators': {
            'A100-80GB': 1,
        },
        'use_spot': False,
        'image_id': {
            None: image,
        },
        'disk_tier': None,
        'ephemeral_storage': 20,
    }
    info = _replica(9)
    info.location = dict(resource_state)
    info.resources_override = dict(resource_state)
    info.resources_override['cloud'] = clouds.Kubernetes()

    storage_state = info.to_storage_dict()
    assert storage_state['location']['image_id'] == [[None, image]]
    assert storage_state['resources_override']['image_id'] == [[None, image]]

    # Exercise the same JSON object-key conversion as PostgreSQL JSONB.
    restored = replica_managers.ReplicaInfo.from_storage_dict(
        json.loads(json.dumps(storage_state)))

    assert restored.location['image_id'] == {None: image}
    assert restored.resources_override['image_id'] == {None: image}
    assert restored.resources_override['cloud'] == 'Kubernetes'
    assert restored.get_spot_location().image_id == {None: image}
    assert restored.to_storage_dict() == storage_state


def test_replica_json_storage_reads_legacy_null_image_id_key():
    info = _replica(1)
    state = info.to_storage_dict()
    state['resources_override'] = {
        'cloud': 'Kubernetes',
        'region': 'prod_research_cluster_eks',
        'image_id': {
            'null': 'docker:example.invalid/boltz:model',
        },
    }

    restored = replica_managers.ReplicaInfo.from_storage_dict(state)

    assert restored.resources_override['image_id'] == {
        None: 'docker:example.invalid/boltz:model'
    }


@pytest.mark.parametrize('planned_capacity', [0, -1, True, 1.5, '8'])
def test_replica_rejects_invalid_planned_capacity(planned_capacity):
    with pytest.raises(ValueError, match='positive integer'):
        replica_managers.ReplicaInfo(replica_id=1,
                                     cluster_name='svc-1',
                                     replica_port='8080',
                                     is_spot=True,
                                     location=None,
                                     version=1,
                                     resources_override=None,
                                     planned_capacity=planned_capacity)


def test_replica_rejects_invalid_stored_planned_capacity():
    state = _replica(1).to_storage_dict()
    state['planned_capacity'] = 0
    with pytest.raises(ValueError, match='Stored planned_capacity'):
        replica_managers.ReplicaInfo.from_storage_dict(state)


def test_replica_state_uses_jsonb_on_postgres():
    state_type = serve_state.replicas_table.c.replica_state.type
    assert isinstance(state_type.dialect_impl(postgresql.dialect()),
                      postgresql.JSONB)


def test_resource_action_common_columns_default_to_inert(_mock_serve_db):
    _add_minimal_service('svc', service_hash='incarnation-a')
    assert serve_state.add_or_update_replica('svc', 1, _replica(1))

    with orm.Session(_mock_serve_db) as session:
        service = session.execute(
            sqlalchemy.select(serve_state.services_table).where(
                serve_state.services_table.c.name == 'svc')).mappings().one()
        replica = session.execute(
            sqlalchemy.select(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == 'svc',
                serve_state.replicas_table.c.replica_id == 1)).mappings().one()

    assert service['resource_action_mode'] == 'legacy'
    assert service['resource_action_mode_changed_at'] is None
    assert all(replica[name] is None
               for name in serve_state._ACTION_OWNED_REPLICA_COLUMNS)


def test_replica_teardown_identity_snapshot_distinguishes_legacy_and_action(
        _mock_serve_db):
    _add_minimal_service('svc', service_hash='incarnation-a')
    replica = _replica(1)
    assert serve_state.add_or_update_replica('svc', 1, replica)

    assert serve_state.get_replica_resource_action_identities('svc',
                                                              [1, 2]) == {
                                                                  1: None
                                                              }
    snapshot = serve_state.get_replica_info_with_resource_action_identity(
        'svc', 1)
    assert snapshot is not None
    legacy_info, legacy_identity = snapshot
    assert legacy_info.cluster_name == 'svc-1'
    assert legacy_identity is None

    replica_incarnation = uuid.uuid4()
    cluster_record_uuid = uuid.uuid4()
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == 'svc',
                serve_state.replicas_table.c.replica_id == 1).values(
                    replica_incarnation=replica_incarnation,
                    desired_generation=3,
                    sky_cluster_record_uuid=cluster_record_uuid))
        session.commit()

    action_identity = serve_state.get_replica_resource_action_identity('svc', 1)
    assert action_identity == serve_state.ReplicaResourceActionIdentity(
        replica_id=1,
        cluster_name='svc-1',
        replica_incarnation=replica_incarnation,
        desired_generation=3,
        sky_cluster_record_uuid=cluster_record_uuid)
    action_snapshot = (
        serve_state.get_replica_info_with_resource_action_identity('svc', 1))
    assert action_snapshot is not None
    assert action_snapshot[0].cluster_name == 'svc-1'
    assert action_snapshot[1] == action_identity


@pytest.mark.parametrize('invalid_values, message', [
    ({
        'replica_incarnation': uuid.uuid4()
    }, 'partial or invalid'),
    ({
        'launch_action_id': uuid.uuid4()
    }, 'legacy replica row has resource-action links'),
    ({
        'replica_incarnation': uuid.uuid4(),
        'desired_generation': 1,
        'sky_cluster_record_uuid': uuid.uuid4(),
        'launch_action_id': uuid.uuid4(),
        'launch_shadow_coverage_id': uuid.uuid4(),
    }, 'competing launch action owners'),
])
def test_replica_teardown_identity_snapshot_rejects_invalid_rows(
        _mock_serve_db, invalid_values, message):
    _add_minimal_service('svc', service_hash='incarnation-a')
    assert serve_state.add_or_update_replica('svc', 1, _replica(1))
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == 'svc',
                serve_state.replicas_table.c.replica_id == 1).values(
                    **invalid_values))
        session.commit()

    with pytest.raises(serve_state.MalformedReplicaResourceActionIdentityError,
                       match=message):
        serve_state.get_replica_resource_action_identities('svc', [1])


def test_replica_teardown_snapshot_rejects_divergent_physical_column(
        _mock_serve_db):
    _add_minimal_service('svc', service_hash='incarnation-a')
    assert serve_state.add_or_update_replica('svc', 1, _replica(1))
    with orm.Session(_mock_serve_db) as session:
        session.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == 'svc',
                serve_state.replicas_table.c.replica_id == 1).values(
                    cluster_name='replacement-cluster'))
        session.commit()

    with pytest.raises(serve_state.MalformedReplicaResourceActionIdentityError,
                       match='JSON state differs'):
        serve_state.get_replica_info_with_resource_action_identity('svc', 1)


def test_replica_updates_and_insert_conflicts_preserve_action_owned_columns(
        _mock_serve_db):
    service_hash = 'incarnation-a'
    _add_minimal_service('svc', service_hash=service_hash)
    expected_by_replica = {}

    for replica_id in range(1, 5):
        assert serve_state.add_or_update_replica('svc', replica_id,
                                                 _replica(replica_id))
        launch_shadow_coverage_id = (None if replica_id %
                                     2 else uuid.UUID(int=replica_id * 100 + 5))
        down_shadow_coverage_id = (None if replica_id %
                                   2 else uuid.UUID(int=replica_id * 100 + 6))
        action_values = {
            'replica_incarnation': uuid.UUID(int=replica_id * 100 + 1),
            'desired_generation': replica_id,
            'sky_cluster_record_uuid': uuid.UUID(int=replica_id * 100 + 2),
            'launch_action_id':
                (uuid.UUID(int=replica_id * 100 + 3) if replica_id % 2 else None
                ),
            'down_action_id':
                (uuid.UUID(int=replica_id * 100 + 4) if replica_id % 2 else None
                ),
            'launch_shadow_coverage_id': launch_shadow_coverage_id,
            'down_shadow_coverage_id': down_shadow_coverage_id,
            'launch_shadow_sample_id': launch_shadow_coverage_id,
            'down_shadow_sample_id': down_shadow_coverage_id,
        }
        expected_by_replica[replica_id] = action_values
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id ==
                    replica_id).values(**action_values))
            session.commit()

    # Ordinary and batch bookkeeping use different UPDATE builders.
    first = serve_state.get_replica_info_from_id('svc', 1)
    second = serve_state.get_replica_info_from_id('svc', 2)
    assert first is not None and second is not None
    first.version = 2
    second.version = 2
    assert serve_state.add_or_update_replica('svc',
                                             1,
                                             first,
                                             expected_replica_exists=True)
    assert serve_state.add_or_update_replicas('svc', [(2, second)],
                                              expected_replica_exists=True)

    # Paid-capacity admission is INSERT-only and cannot adopt an action row.
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.try_add_replica_with_paid_capacity_claim(
            'svc',
            service_hash,
            3,
            _replica(3, version=2),
            pool_key='test-paid-pool',
            priority=1,
            base_limit=1,
            max_limit=2,
            now=100.0,
            success_ttl_seconds=60.0,
            waiter_ttl_seconds=60.0,
            expected_controller_owner=None)

    # Reserved-fill admission is also INSERT-only. This exercises SQLite's
    # conditional INSERT ... SELECT path against a conflicting key.
    fill_pool_key = json.dumps(['test-context', 'a100'])
    with orm.Session(_mock_serve_db) as session:
        session.execute(serve_state.reserved_fill_claims_table.insert().values(
            service_name='svc',
            pool_key=fill_pool_key,
            weight=1,
            floor_replicas=1,
            gpus_per_replica=1,
            holdings_fill=1,
            heartbeat_ts=100.0))
        session.commit()
    fill_replica = _replica(4, version=2)
    fill_replica.reserved_fill = True
    fill_replica.reserved_fill_pool_key = fill_pool_key
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.add_replica_if_round_epoch('svc',
                                               4,
                                               fill_replica,
                                               pool_key=fill_pool_key,
                                               expected_epoch=1)

    with orm.Session(_mock_serve_db) as session:
        rows = session.execute(
            sqlalchemy.select(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name ==
                'svc')).mappings().all()
    actual_by_replica = {row['replica_id']: row for row in rows}
    for replica_id, expected in expected_by_replica.items():
        assert {
            name: actual_by_replica[replica_id][name]
            for name in serve_state._ACTION_OWNED_REPLICA_COLUMNS
        } == expected


def test_replica_reads_do_not_use_pickle(_mock_serve_db, monkeypatch):
    info = _replica(1)
    info.resources_override = {
        'cloud': clouds.AWS(),
        'region': 'us-east-1',
        'use_spot': True,
    }
    serve_state.add_or_update_replica('svc', 1, info)
    monkeypatch.setattr(
        serve_state.pickle, 'loads',
        lambda _: pytest.fail('replica read attempted pickle fallback'))

    restored = serve_state.get_replica_info_from_id('svc', 1)

    assert restored is not None
    assert restored.to_storage_dict() == info.to_storage_dict()
    assert restored.resources_override['cloud'] == 'AWS'


def test_replica_status_counts_are_grouped_in_sql(_mock_serve_db):
    ready = _replica(1)
    ready.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    ready.status_property.service_ready_now = True
    serve_state.add_or_update_replicas('svc', [(1, ready), (2, _replica(2))])

    with _count_sql_statements(_mock_serve_db) as counts:
        status_counts = serve_state.get_replica_status_counts('svc')

    assert counts['n'] == 1
    assert status_counts == {'PENDING': 1, 'READY': 1}


def test_replica_status_and_capacity_counts_use_compact_json(_mock_serve_db):
    ready = _replica(1)
    ready.planned_capacity = 8
    ready.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    ready.status_property.service_ready_now = True
    pending = _replica(2)
    pending.planned_capacity = 4
    serve_state.add_or_update_replicas('svc', [(1, ready), (2, pending)])

    with _count_sql_statements(_mock_serve_db) as counts:
        status_counts, capacity_counts = (
            serve_state.get_replica_status_and_capacity_counts('svc'))

    assert counts['n'] == 1
    assert status_counts == {'PENDING': 1, 'READY': 1}
    assert capacity_counts == {'PENDING': 4, 'READY': 8}


def test_replica_json_migration_backfills_legacy_pickle(tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path / "legacy-serve.db"}')
    legacy_metadata = sqlalchemy.MetaData()
    legacy_replicas = sqlalchemy.Table(
        'replicas', legacy_metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('replica_info', sqlalchemy.LargeBinary))
    version_table = sqlalchemy.Table(
        'alembic_version_serve_state_db', legacy_metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True))
    legacy_metadata.create_all(engine)
    info = _replica(3, cluster_name='legacy-cluster', version=2)
    info._version = 6
    del info.first_consecutive_failure_time
    info.consecutive_failure_times = [42.0, 43.0]
    info.location = {
        'cloud': 'Kubernetes',
        'region': 'prod_research_cluster_eks',
        'zone': None,
        'accelerators': {
            'A100-80GB': 1,
        },
        'use_spot': False,
        'image_id': {
            None: 'docker:example.invalid/boltz:model',
        },
        'disk_tier': None,
        'ephemeral_storage': 20,
    }
    info.resources_override = dict(info.location)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    info.status_property.service_ready_now = True
    legacy_blob = pickle.dumps(info)
    expected_state = pickle.loads(legacy_blob).to_storage_dict()
    uncertain = _replica(4, cluster_name='legacy-uncertain-cluster', version=1)
    uncertain.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    uncertain.status_property.service_ready_now = True
    uncertain_status = uncertain.status_property
    uncertain_status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
    uncertain_status.is_scale_down = True
    uncertain_status.wait_for_idle_before_termination = False
    uncertain_status.logical_retirement_version = 2
    uncertain_status.logical_retirement_controller_epoch = 'legacy-epoch'
    uncertain_status.logical_retirement_generation = 3
    uncertain_status.logical_retirement_target_capacity = 1
    uncertain_status.logical_retirement_confirmed_generation = 4
    # Model a real pre-field pickle: deleting the instance key still makes
    # getattr() see the new dataclass class default False after unpickling.
    vars(uncertain_status).pop('logical_retirement_committed')
    uncertain_blob = pickle.dumps(uncertain)
    loaded_uncertain = pickle.loads(uncertain_blob)
    assert ('logical_retirement_committed'
            not in vars(loaded_uncertain.status_property))
    assert getattr(loaded_uncertain.status_property,
                   'logical_retirement_committed') is False
    assert (loaded_uncertain.to_storage_dict()['status_property']
            ['logical_retirement_committed'] is None)
    with engine.begin() as connection:
        connection.execute(legacy_replicas.insert(), [{
            'service_name': 'svc',
            'replica_id': 3,
            'replica_info': legacy_blob,
        }, {
            'service_name': 'svc',
            'replica_id': 4,
            'replica_info': uncertain_blob,
        }])
        connection.execute(version_table.insert().values(version_num='009'))

    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         '010')
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)

    restored = serve_state.get_replica_info_from_id('svc', 3)
    assert restored is not None
    assert restored.to_storage_dict() == expected_state
    assert restored.first_consecutive_failure_time == 42.0
    restored_uncertain = serve_state.get_replica_info_from_id('svc', 4)
    assert restored_uncertain is not None
    assert (restored_uncertain.status_property.logical_retirement_committed
            is None)
    assert (replica_managers.SkyPilotReplicaManager.
            _is_legacy_uncertain_logical_retirement(restored_uncertain))
    assert serve_state.get_replica_status_counts('svc') == {
        'READY': 1,
        'SHUTTING_DOWN': 1,
    }


def test_replica_json_migration_handles_fresh_database(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "fresh-serve.db"}')

    serve_state.create_table(engine)

    inspector = sqlalchemy.inspect(engine)
    columns = {column['name'] for column in inspector.get_columns('replicas')}
    assert {'replica_state_version', 'status', 'replica_state'} <= columns
    service_columns = {
        column['name'] for column in inspector.get_columns('services')
    }
    assert 'logical_replica_semantics' in service_columns
    assert 'demand_capacity_observations' in inspector.get_table_names()
    indexes = {
        index['name']: index['column_names']
        for index in inspector.get_indexes('replicas')
    }
    assert indexes['replicas_service_version_idx'] == [
        'service_name', 'version'
    ]


def test_replica_index_migration_converges_predecessor_stamped_legacy_state(
        tmp_path):
    """Revision 026 repairs previews stamped past replica JSON revision 010."""
    engine = create_engine(f'sqlite:///{tmp_path / "preview-serve.db"}')
    metadata = sqlalchemy.MetaData()
    legacy_replicas = sqlalchemy.Table(
        'replicas',
        metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('replica_info', sqlalchemy.LargeBinary),
    )
    version_table = sqlalchemy.Table(
        'alembic_version_serve_state_db',
        metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True),
    )
    metadata.create_all(engine)
    legacy = _replica(1, cluster_name='svc-1', version=3)
    with engine.begin() as connection:
        connection.execute(
            legacy_replicas.insert().values(service_name='svc',
                                            replica_id=1,
                                            replica_info=pickle.dumps(legacy)))
        connection.execute(version_table.insert().values(version_num='025'))

    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         '026')

    inspector = sqlalchemy.inspect(engine)
    columns = {column['name'] for column in inspector.get_columns('replicas')}
    assert {
        'replica_state_version',
        'status',
        'sky_down_status',
        'version',
        'cluster_name',
        'created_at',
        'is_spot',
        'replica_state',
    } <= columns
    indexes = {
        index['name']: index['column_names']
        for index in inspector.get_indexes('replicas')
    }
    assert indexes['replicas_service_status_idx'] == ['service_name', 'status']
    assert indexes['replicas_service_version_idx'] == [
        'service_name', 'version'
    ]
    replicas = sqlalchemy.Table('replicas',
                                sqlalchemy.MetaData(),
                                autoload_with=engine)
    with engine.connect() as connection:
        row = connection.execute(sqlalchemy.select(replicas)).one()._mapping
        revision = connection.execute(
            sqlalchemy.select(version_table.c.version_num)).scalar_one()
    assert row['version'] == 3
    assert row['cluster_name'] == 'svc-1'
    assert row['status'] == serve_state.ReplicaStatus.PENDING.value
    assert row['replica_state_version'] == 1
    assert row['replica_state'] is not None
    assert revision == '026'


def test_replica_index_migration_fails_closed_without_reconstructable_state(
        tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "invalid-preview.db"}')
    metadata = sqlalchemy.MetaData()
    replicas = sqlalchemy.Table(
        'replicas',
        metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer),
    )
    version_table = sqlalchemy.Table(
        'alembic_version_serve_state_db',
        metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True),
    )
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(replicas.insert().values(service_name='svc',
                                                    replica_id=1,
                                                    version=3))
        connection.execute(version_table.insert().values(version_num='025'))

    with pytest.raises(RuntimeError, match='without legacy replica_info'):
        migration_utils.safe_alembic_upgrade(engine,
                                             migration_utils.SERVE_DB_NAME,
                                             '026')

    with engine.connect() as connection:
        revision = connection.execute(
            sqlalchemy.select(version_table.c.version_num)).scalar_one()
    assert revision == '025'


def test_replica_index_migration_rejects_same_name_with_wrong_columns(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path / "wrong-index.db"}')
    metadata = sqlalchemy.MetaData()
    replicas = sqlalchemy.Table(
        'replicas',
        metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
    )
    version_table = sqlalchemy.Table(
        'alembic_version_serve_state_db',
        metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True),
    )
    sqlalchemy.Index('replicas_service_version_idx', replicas.c.service_name,
                     replicas.c.replica_id)
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(version_table.insert().values(version_num='025'))

    with pytest.raises(RuntimeError, match='unexpected shape'):
        migration_utils.safe_alembic_upgrade(engine,
                                             migration_utils.SERVE_DB_NAME,
                                             '026')

    with engine.connect() as connection:
        revision = connection.execute(
            sqlalchemy.select(version_table.c.version_num)).scalar_one()
    assert revision == '025'


def test_elected_version_migration_backfills_latest_committed_version(
        tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path / "old-serve.db"}')
    monkeypatch.setattr(migration_utils, 'SERVE_VERSION', '013')
    serve_state.create_table(engine)
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc', current_version=1))
        connection.execute(serve_state.version_specs_table.insert(), [{
            'service_name': 'svc',
            'version': 1,
            'spec': pickle.dumps(None),
            'yaml_content': 'yaml: v1',
        }, {
            'service_name': 'svc',
            'version': 2,
            'spec': pickle.dumps(None),
            'yaml_content': 'yaml: v2',
        }, {
            'service_name': 'svc',
            'version': 3,
            'spec': pickle.dumps(None),
            'yaml_content': None,
        }])

    monkeypatch.setattr(migration_utils, 'SERVE_VERSION', '014')
    serve_state.create_table(engine)

    assert _read_row(engine, 'svc')['current_version'] == 2


def test_version_provenance_migration_adds_nullable_columns(
        tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path / "old-serve.db"}')
    legacy_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'version_specs',
        legacy_metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
        sqlalchemy.Column('yaml_content', sqlalchemy.Text),
    )
    legacy_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('CREATE TABLE alembic_version_serve_state_db '
                            '(version_num VARCHAR(32) NOT NULL)'))
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO alembic_version_serve_state_db VALUES ('014')"))

    monkeypatch.setattr(migration_utils, 'SERVE_VERSION', '015')
    serve_state.create_table(engine)

    columns = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('version_specs')
    }
    assert {'created_at', 'created_by'} <= columns


def test_submitted_version_yaml_migration_adds_nullable_column(
        tmp_path, monkeypatch):
    engine = create_engine(f'sqlite:///{tmp_path / "old-serve.db"}')
    legacy_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'version_specs',
        legacy_metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
        sqlalchemy.Column('yaml_content', sqlalchemy.Text),
        sqlalchemy.Column('created_at', sqlalchemy.Float),
        sqlalchemy.Column('created_by', sqlalchemy.Text),
    )
    legacy_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('CREATE TABLE alembic_version_serve_state_db '
                            '(version_num VARCHAR(32) NOT NULL)'))
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO alembic_version_serve_state_db VALUES ('016')"))

    monkeypatch.setattr(migration_utils, 'SERVE_VERSION', '017')
    serve_state.create_table(engine)

    columns = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('version_specs')
    }
    assert 'submitted_yaml_content' in columns


def test_placement_catalog_migration_adds_nullable_column(
        tmp_path, monkeypatch):
    # Regression guard for PR #906: migration 028 adds the nullable
    # `placement_catalog` column via ALTER TABLE. The durable round-trip tests
    # (test_version_placement_catalog_persists_and_backfills_once) build the
    # schema with `create_all`, which already includes the column, so they
    # never exercise the upgrade path on a database that predates it. This
    # pins that an on-disk service DB stamped at 027 gains the column and that
    # a pre-existing (legacy) row reads back as SQL NULL, matching 016->017.
    engine = create_engine(f'sqlite:///{tmp_path / "old-serve.db"}')
    legacy_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'version_specs',
        legacy_metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
        sqlalchemy.Column('yaml_content', sqlalchemy.Text),
    )
    legacy_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text('CREATE TABLE alembic_version_serve_state_db '
                            '(version_num VARCHAR(32) NOT NULL)'))
        connection.execute(
            sqlalchemy.text(
                "INSERT INTO alembic_version_serve_state_db VALUES ('027')"))
        connection.execute(
            sqlalchemy.text('INSERT INTO version_specs '
                            '(service_name, version, yaml_content) '
                            "VALUES ('legacy-svc', 1, 'yaml: v1')"))

    monkeypatch.setattr(migration_utils, 'SERVE_VERSION', '028')
    serve_state.create_table(engine)

    columns = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('version_specs')
    }
    assert 'placement_catalog' in columns
    with engine.begin() as connection:
        legacy_catalog = connection.execute(
            sqlalchemy.text('SELECT placement_catalog FROM version_specs '
                            'WHERE service_name = :name'), {
                                'name': 'legacy-svc'
                            }).scalar()
    assert legacy_catalog is None


def test_service_version_terminal_lookup_uses_only_exact_history_keys(
        _mock_serve_db):
    engine = _mock_serve_db
    assert _add_minimal_service('svc-terminal',
                                service_hash='incarnation-a') is True
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.insert(), [{
            'service_name': 'svc-terminal',
            'version': version,
            'spec': pickle.dumps(None),
            'yaml_content': f'yaml: v{version}',
        } for version in range(2, 2001)])
        connection.execute(serve_state.replicas_table.insert(), [{
            'service_name': 'svc-terminal',
            'replica_id': replica,
            'replica_info': b'opaque',
            'version': replica + 1,
        } for replica in range(1, 2000)])
    statements = []

    def record(_connection, _cursor, statement, _parameters, _context,
               _executemany):
        statements.append(statement)

    sqlalchemy.event.listen(engine, 'before_cursor_execute', record)
    try:
        result = serve_state.get_service_version_terminal_states([
            ('svc-terminal', 2000, 'incarnation-a'),
            ('missing-service', 1, 'missing-incarnation'),
        ])
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute', record)

    assert result == {
        ('svc-terminal', 2000, 'incarnation-a'): False,
        ('missing-service', 1, 'missing-incarnation'): True,
    }
    assert len(statements) == 2
    probe_statement = next(statement for statement in statements
                           if 'wanted_service_versions' in statement)
    assert 'WITH wanted_service_versions(service_name, version) AS' in (
        probe_statement)
    assert probe_statement.count('EXISTS (SELECT') == 2
    assert 'FROM version_specs' in probe_statement
    assert 'FROM replicas' in probe_statement
    assert 'SELECT replicas.service_name, replicas.version' not in (
        probe_statement)


def test_get_specs_batches_requested_versions_in_one_query(_mock_serve_db):
    initial_spec = types.SimpleNamespace(graceful_drain_async_occupancy=False)
    assert _add_minimal_service('svc-specs', spec=initial_spec) is True
    serve_state.add_or_update_version(
        'svc-specs',
        1,
        types.SimpleNamespace(graceful_drain_async_occupancy=False),
        'yaml: v1',
    )
    serve_state.add_or_update_version(
        'svc-specs',
        2,
        types.SimpleNamespace(graceful_drain_async_occupancy=True),
        'yaml: v2',
    )

    with _count_sql_statements(_mock_serve_db) as counts:
        specs = serve_state.get_specs('svc-specs', [2, 1, 2, 3])

    assert counts['n'] == 1, counts
    assert set(specs) == {1, 2}
    assert specs[1].graceful_drain_async_occupancy is False
    assert specs[2].graceful_drain_async_occupancy is True


def test_committed_version_content_is_immutable_and_retryable(_mock_serve_db):
    assert _add_minimal_service('svc-immutable') is True
    assert serve_state.add_version('svc-immutable') == 2
    original_spec = types.SimpleNamespace(value='original')
    result = serve_state.add_or_update_version('svc-immutable', 2,
                                               original_spec, 'value: original')
    assert result is serve_state.VersionCommitResult.COMMITTED
    assert _read_row(_mock_serve_db, 'svc-immutable')['current_version'] == 2
    assert serve_state.get_version_yaml_contents('svc-immutable') == {
        1: 'yaml: v1',
        2: 'value: original',
    }
    original_row = _read_version_row(_mock_serve_db, 'svc-immutable', 2)

    # A lost-response retry is idempotent and does not rewrite either stored
    # payload, even if the caller reconstructed a fresh spec object.
    retry_result = serve_state.add_or_update_version(
        'svc-immutable', 2, types.SimpleNamespace(value='original'),
        'value: original')
    assert retry_result is serve_state.VersionCommitResult.IDEMPOTENT_RETRY
    assert _read_version_row(_mock_serve_db, 'svc-immutable', 2) == original_row

    conflict_result = serve_state.add_or_update_version(
        'svc-immutable', 2, types.SimpleNamespace(value='different'),
        'value: different')
    assert conflict_result is serve_state.VersionCommitResult.CONTENT_CONFLICT
    assert _read_version_row(_mock_serve_db, 'svc-immutable', 2) == original_row


def test_version_placement_catalog_persists_and_backfills_once(_mock_serve_db):
    initial_catalog = {'schema_version': 1, 'entries': []}
    assert _add_minimal_service('svc-catalog',
                                placement_catalog=initial_catalog) is True
    assert serve_state.get_placement_catalog('svc-catalog',
                                             1) == (initial_catalog)

    assert serve_state.add_version('svc-catalog') == 2
    update_catalog = {
        'schema_version': 1,
        'entries': [{
            'location': {
                'cloud': 'AWS',
                'region': 'us-east-1',
            },
            'hourly_cost': 0.25,
        }],
    }
    assert (serve_state.add_or_update_version('svc-catalog',
                                              2,
                                              types.SimpleNamespace(value='v2'),
                                              'value: v2',
                                              placement_catalog=update_catalog)
            is serve_state.VersionCommitResult.COMMITTED)
    assert serve_state.get_placement_catalog('svc-catalog', 2) == update_catalog

    assert serve_state.add_version('svc-catalog') == 3
    assert (serve_state.add_or_update_version(
        'svc-catalog', 3, types.SimpleNamespace(value='legacy'),
        'value: legacy') is serve_state.VersionCommitResult.COMMITTED)
    winner = {'schema_version': 1, 'entries': [{'winner': True}]}
    loser = {'schema_version': 1, 'entries': [{'winner': False}]}
    assert serve_state.set_placement_catalog_if_missing('svc-catalog', 3,
                                                        winner)
    assert not serve_state.set_placement_catalog_if_missing(
        'svc-catalog', 3, loser)
    assert serve_state.get_placement_catalog('svc-catalog', 3) == winner


def test_identical_version_retry_only_backfills_missing_catalog(_mock_serve_db):
    assert _add_minimal_service('svc-catalog-retry') is True
    catalog = {'schema_version': 1, 'entries': []}
    assert (serve_state.add_or_update_version(
        'svc-catalog-retry',
        1,
        types.SimpleNamespace(value='ignored'),
        'yaml: v1',
        placement_catalog=catalog)
            is serve_state.VersionCommitResult.IDEMPOTENT_RETRY)
    row = _read_version_row(_mock_serve_db, 'svc-catalog-retry', 1)
    assert row['placement_catalog'] == catalog
    original_spec = row['spec']
    assert (serve_state.add_or_update_version(
        'svc-catalog-retry',
        1,
        types.SimpleNamespace(value='different'),
        'yaml: v1',
        placement_catalog={
            'schema_version': 1,
            'entries': [{
                'other': True
            }]
        }) is serve_state.VersionCommitResult.IDEMPOTENT_RETRY)
    final_row = _read_version_row(_mock_serve_db, 'svc-catalog-retry', 1)
    assert final_row['placement_catalog'] == catalog
    assert final_row['spec'] == original_spec


def test_logical_replica_activation_is_durable_and_one_way(_mock_serve_db):
    physical = types.SimpleNamespace(uses_logical_replicas=False)
    logical = types.SimpleNamespace(uses_logical_replicas=True)
    assert _add_minimal_service('svc-logical', spec=physical) is True
    assert not serve_state.service_uses_logical_replica_semantics('svc-logical')

    assert serve_state.add_version('svc-logical') == 2
    assert (serve_state.add_or_update_version('svc-logical', 2, logical,
                                              'yaml: logical')
            is serve_state.VersionCommitResult.COMMITTED)
    assert serve_state.service_uses_logical_replica_semantics('svc-logical')

    assert serve_state.add_version('svc-logical') == 3
    assert (serve_state.add_or_update_version('svc-logical', 3, physical,
                                              'yaml: physical')
            is serve_state.VersionCommitResult.SEMANTIC_CONFLICT)
    assert serve_state.get_spec('svc-logical', 3) is None

    # A lost-response retry of a physical version committed before activation
    # remains idempotent. The fence only rejects new physical commits.
    assert (serve_state.add_or_update_version('svc-logical', 1, physical,
                                              'yaml: v1')
            is serve_state.VersionCommitResult.IDEMPOTENT_RETRY)


def test_lower_logical_commit_cannot_flip_newer_physical_semantics(
        _mock_serve_db):
    physical = types.SimpleNamespace(uses_logical_replicas=False)
    logical = types.SimpleNamespace(uses_logical_replicas=True)
    assert _add_minimal_service('svc-out-of-order', spec=physical) is True
    assert serve_state.add_version('svc-out-of-order') == 2
    assert serve_state.add_version('svc-out-of-order') == 3

    assert (serve_state.add_or_update_version('svc-out-of-order', 3, physical,
                                              'yaml: physical-v3')
            is serve_state.VersionCommitResult.COMMITTED)
    assert (serve_state.add_or_update_version('svc-out-of-order', 2, logical,
                                              'yaml: logical-v2')
            is serve_state.VersionCommitResult.STALE_VERSION)
    assert not serve_state.service_uses_logical_replica_semantics(
        'svc-out-of-order')
    assert serve_state.get_spec('svc-out-of-order', 2) is None
    assert serve_state.get_spec('svc-out-of-order',
                                3).uses_logical_replicas is False


def test_initial_logical_service_sets_activation_fence(_mock_serve_db):
    logical = types.SimpleNamespace(uses_logical_replicas=True)
    assert _add_minimal_service('svc-initial-logical', spec=logical) is True
    assert serve_state.service_uses_logical_replica_semantics(
        'svc-initial-logical')


def test_demand_capacity_observation_round_trip(_mock_serve_db):
    serve_state.upsert_demand_capacity_observation('research', 123.0, 123.5, {
        'a100': 223,
        'l4': 12,
    })

    observations = serve_state.get_demand_capacity_observations(
        ['research', 'missing'])
    assert set(observations) == {'research'}
    assert observations['research']['snapshot_time'] == 123.0
    assert observations['research']['completed_at'] == 123.5
    assert json.loads(observations['research']['availability']) == {
        'a100': 223,
        'l4': 12,
    }

    # Query failures are durable observations too. They rate-limit provider
    # retries while remaining distinguishable from a successful empty result.
    serve_state.upsert_demand_capacity_observation('research', 124.0, 125.0,
                                                   None)
    observation = serve_state.get_demand_capacity_observations(['research'
                                                               ])['research']
    assert observation['snapshot_time'] == 124.0
    assert observation['completed_at'] == 125.0
    assert observation['availability'] is None


def test_get_replica_infos_from_ids_batches_in_one_query(_mock_serve_db):
    for rid in (1, 2, 3):
        info = replica_managers.ReplicaInfo(replica_id=rid,
                                            cluster_name=f'svc-{rid}',
                                            replica_port='8080',
                                            is_spot=False,
                                            location=None,
                                            version=1,
                                            resources_override=None)
        serve_state.add_or_update_replica('svc', rid, info)

    with _count_sql_statements(_mock_serve_db) as counts:
        infos = serve_state.get_replica_infos_from_ids('svc', [2, 1, 2, 4])

    assert counts['n'] == 1, counts
    assert set(infos) == {1, 2}
    assert infos[1].replica_id == 1
    assert infos[2].replica_id == 2


def test_get_replica_infos_from_ids_empty_skips_query(_mock_serve_db):
    with _count_sql_statements(_mock_serve_db) as counts:
        assert serve_state.get_replica_infos_from_ids('svc', []) == {}

    assert counts['n'] == 0, counts


def test_get_yaml_contents_batches_requested_versions_in_one_query(
        _mock_serve_db):
    assert _add_minimal_service('svc-yamls') is True
    serve_state.add_or_update_version(
        'svc-yamls',
        1,
        types.SimpleNamespace(graceful_drain_async_occupancy=False),
        'yaml: v1',
    )
    serve_state.add_or_update_version(
        'svc-yamls',
        2,
        types.SimpleNamespace(graceful_drain_async_occupancy=True),
        'yaml: v2',
    )

    with _count_sql_statements(_mock_serve_db) as counts:
        yamls = serve_state.get_yaml_contents('svc-yamls', [2, 1, 2, 3])

    assert counts['n'] == 1, counts
    assert yamls == {
        1: 'yaml: v1',
        2: 'yaml: v2',
    }


def test_get_yaml_contents_empty_versions_skips_query(_mock_serve_db):
    with _count_sql_statements(_mock_serve_db) as counts:
        yamls = serve_state.get_yaml_contents('svc-yamls', [])

    assert counts['n'] == 0, counts
    assert not yamls


def test_get_version_yaml_contents_fetches_all_versions_in_one_query(
        _mock_serve_db):
    assert _add_minimal_service('svc-all-yamls') is True
    serve_state.add_or_update_version(
        'svc-all-yamls',
        2,
        types.SimpleNamespace(graceful_drain_async_occupancy=True),
        'yaml: v2',
    )
    serve_state.add_or_update_version(
        'svc-all-yamls',
        1,
        types.SimpleNamespace(graceful_drain_async_occupancy=False),
        'yaml: v1',
    )
    # Interrupted updates leave a NULL-yaml placeholder.  The batched cleanup
    # snapshot must preserve the old per-version reader's skip semantics.
    assert serve_state.add_version('svc-all-yamls') == 3

    with _count_sql_statements(_mock_serve_db) as counts:
        yamls = serve_state.get_version_yaml_contents('svc-all-yamls')

    assert counts['n'] == 1, counts
    assert yamls == {
        1: 'yaml: v1',
        2: 'yaml: v2',
    }
    assert list(yamls) == [1, 2]


def test_get_version_yaml_contents_unknown_service_returns_empty(
        _mock_serve_db):
    assert serve_state.get_version_yaml_contents('svc-missing') == {}


def test_get_service_from_name_uses_joined_spec_in_single_query(_mock_serve_db):
    spec = _FakeSpec('qps=2', 'least_load')
    assert _add_minimal_service('svc-read', spec=spec) is True

    with _count_sql_statements(_mock_serve_db) as counts:
        record = serve_state.get_service_from_name('svc-read')

    assert counts['n'] == 1, counts
    assert record is not None
    assert record['policy'] == 'qps=2'
    assert record['load_balancing_policy'] == 'least_load'


def test_service_workspace_survives_durable_round_trip(_mock_serve_db):
    assert _add_minimal_service('svc-workspace', workspace='research') is True
    record = serve_state.get_service_from_name('svc-workspace')
    assert record is not None
    assert record['workspace'] == 'research'


def test_get_services_uses_single_query_for_multiple_rows(_mock_serve_db):
    assert _add_minimal_service('svc-a') is True
    assert _add_minimal_service('svc-b') is True
    assert _add_minimal_service('svc-c') is True

    with _count_sql_statements(_mock_serve_db) as counts:
        records = serve_state.get_services()

    assert counts['n'] == 1, counts
    assert sorted(record['name'] for record in records) == [
        'svc-a',
        'svc-b',
        'svc-c',
    ]


def test_get_num_services_filters_raw_modes_without_deserializing(
        _mock_serve_db, monkeypatch):
    assert _add_minimal_service('serve-versioned') is True
    assert _add_minimal_service('pool-versioned', pool=True) is True
    _insert_orphan_service_row(_mock_serve_db, 'serve-orphan')
    _insert_orphan_service_row(_mock_serve_db, 'pool-orphan', pool=True)

    def _unexpected_spec_unpickle(_):
        raise AssertionError('service counts must not deserialize specs')

    monkeypatch.setattr(serve_state.pickle, 'loads', _unexpected_spec_unpickle)
    with _count_sql_statements(_mock_serve_db) as counts:
        assert serve_state.get_num_services() == 4
        assert serve_state.get_num_services(pool=False) == 2
        assert serve_state.get_num_services(pool=True) == 2

    assert counts['n'] == 3, counts


def test_get_service_liveness_snapshots_is_one_slim_version_backed_query(
        _mock_serve_db, monkeypatch):
    assert _add_minimal_service('serve-b',
                                controller_ip='10.0.0.2',
                                controller_pid=22,
                                service_hash='hash-b',
                                resource_scope='scope-b') is True
    assert _add_minimal_service('serve-a',
                                controller_ip='10.0.0.1',
                                controller_pid=11,
                                service_hash='hash-a',
                                resource_scope='scope-a') is True
    assert _add_minimal_service('pool-a', pool=True) is True
    _insert_orphan_service_row(_mock_serve_db, 'serve-orphan')
    serve_state.set_service_status_and_active_versions(
        'serve-b', serve_state.ServiceStatus.FAILED_CLEANUP)

    def _unexpected_spec_unpickle(_):
        raise AssertionError('liveness snapshots must not deserialize specs')

    monkeypatch.setattr(serve_state.pickle, 'loads', _unexpected_spec_unpickle)
    with _count_sql_statements(_mock_serve_db) as counts:
        records = serve_state.get_service_liveness_snapshots(pool=False)

    assert counts['n'] == 1, counts
    assert records == [{
        'name': 'serve-a',
        'status': serve_state.ServiceStatus.CONTROLLER_INIT,
        'controller_job_id': 1,
        'controller_pid': 11,
        'controller_ip': '10.0.0.1',
        'hash': 'hash-a',
        'resource_scope': 'scope-a',
        'yaml_content': 'yaml: v1',
    }, {
        'name': 'serve-b',
        'status': serve_state.ServiceStatus.FAILED_CLEANUP,
        'controller_job_id': 1,
        'controller_pid': 22,
        'controller_ip': '10.0.0.2',
        'hash': 'hash-b',
        'resource_scope': 'scope-b',
        'yaml_content': 'yaml: v1',
    }]


def test_get_service_liveness_snapshots_reports_latest_version_yaml(
        _mock_serve_db):
    """The snapshot carries the LATEST version's yaml, including NULL for a
    placeholder version row, so liveness sweeps can retire unbootable rows
    without a per-service joined read."""
    assert _add_minimal_service('svc', yaml_content='yaml: v1') is True
    serve_state.add_or_update_version(
        'svc', 2, types.SimpleNamespace(graceful_drain_async_occupancy=False),
        'yaml: v2')
    assert _add_minimal_service('placeholder', yaml_content=None) is True

    records = serve_state.get_service_liveness_snapshots(pool=False)

    by_name = {record['name']: record for record in records}
    assert by_name['svc']['yaml_content'] == 'yaml: v2'
    assert by_name['placeholder']['yaml_content'] is None


class TestAddServiceWritesControllerIp:
    """`add_service` should persist the new controller_ip column when caller
    provides POD_IP. Older callers that don't pass it must still work
    (column defaults to NULL)."""

    def test_with_controller_ip(self, _mock_serve_db):
        success = _add_minimal_service('svc1', controller_ip='10.0.0.7')
        assert success is True
        record = _read_row(_mock_serve_db, 'svc1')
        assert record is not None
        assert record['controller_ip'] == '10.0.0.7'

    def test_without_controller_ip(self, _mock_serve_db):
        # No controller_ip arg → column stays NULL (used by single-pod /
        # non-K8s deployments where the routing layer falls back to localhost).
        success = _add_minimal_service('svc2')
        assert success is True
        record = _read_row(_mock_serve_db, 'svc2')
        assert record is not None
        assert record['controller_ip'] is None

    def test_returns_false_on_duplicate(self, _mock_serve_db):
        assert _add_minimal_service('svc3', controller_ip='10.0.0.7') is True
        # Adding the same service again must not corrupt — uniqueness violation
        # is converted to False so up() can short-circuit.
        assert _add_minimal_service('svc3', controller_ip='10.0.0.8') is False
        # And the row is unchanged.
        record = _read_row(_mock_serve_db, 'svc3')
        assert record['controller_ip'] == '10.0.0.7'

    def test_persists_caller_generated_incarnation(self, _mock_serve_db):
        assert _add_minimal_service(
            'svc-known-hash', service_hash='caller-generated-hash') is True
        assert _read_row(_mock_serve_db,
                         'svc-known-hash')['hash'] == 'caller-generated-hash'


class TestAddServiceAtomicRegistration:
    """`add_service` must write the `services` row and its initial
    `version_specs` row atomically. The two-write path (add_service then
    add_or_update_version) had a crash window that stranded a `services` row
    with no version row -- invisible to the latest-version INNER JOIN, so it
    could never be recovered, removed, or have its name reused."""

    def test_registration_is_visible_via_join(self, _mock_serve_db):
        # The whole point: after the atomic write, the service is reachable
        # through get_service_from_name (which INNER-JOINs version_specs),
        # with its initial version row in place.
        assert _add_minimal_service('svc-atomic') is True
        assert serve_state.get_service_from_name('svc-atomic') is not None
        assert (serve_state.get_latest_version('svc-atomic') ==
                serve_constants.INITIAL_VERSION)
        assert serve_state.get_yaml_content(
            'svc-atomic', serve_constants.INITIAL_VERSION) == 'yaml: v1'

    def test_duplicate_does_not_write_second_version_row(self, _mock_serve_db):
        assert _add_minimal_service('svc-dup') is True
        # A duplicate name returns False so up() can short-circuit, and must
        # not write a second version row.
        assert _add_minimal_service('svc-dup') is False
        with orm.Session(_mock_serve_db) as session:
            versions = session.execute(
                sqlalchemy.select(
                    serve_state.version_specs_table.c.version).where(
                        serve_state.version_specs_table.c.service_name ==
                        'svc-dup')).fetchall()
        assert len(versions) == 1

    def test_legacy_registration_preserves_stale_version_inventory(
            self, _mock_serve_db):
        # A non-consolidated controller has no API-local lifecycle epoch, but
        # its own Serve DB is still authoritative. Never overwrite the last
        # cleanup inventory merely because this registration is legacy-shaped.
        serve_state.add_or_update_version('svc-stale',
                                          serve_constants.INITIAL_VERSION, None,
                                          'yaml: stale')
        assert _read_row(_mock_serve_db, 'svc-stale') is None  # no svc row

        with pytest.raises(serve_state.OrphanedVersionRecordsError):
            _add_minimal_service('svc-stale')
        assert serve_state.get_service_from_name('svc-stale') is None
        assert serve_state.get_yaml_content(
            'svc-stale', serve_constants.INITIAL_VERSION) == 'yaml: stale'

    def test_fenced_registration_preserves_orphan_version_inventory(
            self, _mock_serve_db):
        # An older interrupted teardown can leave v1 AND v2+ without the
        # parent services row. Replacing v1 alone makes MAX(version)=v2 and
        # leaks the predecessor's spec/yaml into the new incarnation.
        serve_state.add_or_update_version('svc-stale', 1, 'old-spec-1',
                                          'yaml: old-v1')
        serve_state.add_or_update_version('svc-stale', 2, 'old-spec-2',
                                          'yaml: old-v2')
        assert serve_state.set_ha_recovery_script('svc-stale', 'old-script')
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                serve_state.reserved_fill_claims_table.insert().values(
                    service_name='svc-stale',
                    pool_key='old-pool',
                    weight=1,
                    floor_replicas=1,
                    gpus_per_replica=1,
                    holdings_fill=1,
                    heartbeat_ts=1))
            session.commit()

        epoch = serve_state.claim_service_lifecycle_epoch('svc-stale')
        with pytest.raises(serve_state.OrphanedVersionRecordsError,
                           match=r'versions \[1, 2\]'):
            _add_minimal_service('svc-stale',
                                 service_hash='new-incarnation',
                                 lifecycle_epoch=epoch)

        assert _read_row(_mock_serve_db, 'svc-stale') is None
        assert serve_state.get_service_versions('svc-stale') == [1, 2]
        assert serve_state.get_ha_recovery_script('svc-stale') == 'old-script'
        with orm.Session(_mock_serve_db) as session:
            claim = session.execute(
                sqlalchemy.select(
                    serve_state.reserved_fill_claims_table.c.service_name).
                where(serve_state.reserved_fill_claims_table.c.service_name ==
                      'svc-stale')).fetchone()
        assert claim is not None

    def test_fenced_registration_preserves_orphan_replica_inventory(
            self, _mock_serve_db):
        orphan = _replica(9, cluster_name='billable-cluster-9')
        serve_state.add_or_update_replica('svc-stale', 9, orphan)
        epoch = serve_state.claim_service_lifecycle_epoch('svc-stale')

        with pytest.raises(serve_state.OrphanedReplicaRecordsError,
                           match='billable-cluster-9'):
            _add_minimal_service('svc-stale',
                                 service_hash='new-incarnation',
                                 lifecycle_epoch=epoch)

        assert _read_row(_mock_serve_db, 'svc-stale') is None
        replicas = serve_state.get_replica_infos('svc-stale')
        assert len(replicas) == 1
        assert replicas[0].cluster_name == 'billable-cluster-9'

    def test_current_recovery_script_survives_fenced_registration(
            self, _mock_serve_db):
        # Model the consolidation-mode launch ordering: an old script may
        # remain after predecessor cleanup, then the API parent claims the
        # next lifecycle and atomically replaces it before spawning the new
        # controller.  Initial registration must preserve that current script
        # while still clearing stale reserved-fill claims.
        assert serve_state.set_ha_recovery_script('svc-stale', 'old-script')
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                serve_state.reserved_fill_claims_table.insert().values(
                    service_name='svc-stale',
                    pool_key='old-pool',
                    weight=1,
                    floor_replicas=1,
                    gpus_per_replica=1,
                    holdings_fill=1,
                    heartbeat_ts=1))
            session.commit()

        assert 'svc-stale' not in (serve_state.get_orphaned_service_child_names(
            ['svc-stale']))
        epoch = serve_state.claim_service_lifecycle_epoch('svc-stale')
        assert serve_state.set_ha_recovery_script('svc-stale', 'current-script',
                                                  epoch)
        assert _add_minimal_service('svc-stale',
                                    service_hash='new-incarnation',
                                    lifecycle_epoch=epoch)
        assert (
            serve_state.get_ha_recovery_script('svc-stale') == 'current-script')
        with orm.Session(_mock_serve_db) as session:
            claim = session.execute(
                sqlalchemy.select(
                    serve_state.reserved_fill_claims_table.c.service_name).
                where(serve_state.reserved_fill_claims_table.c.service_name ==
                      'svc-stale')).fetchone()
        assert claim is None

    def test_get_service_pool_from_db_sees_orphan_row(self, _mock_serve_db):
        # The raw-pool accessor must read a version-less row (the orphan case)
        # that get_service_from_name's inner join hides -- this is what gates
        # the mode-scoped `down --purge` cleanup of such an orphan.
        _insert_orphan_service_row(_mock_serve_db, 'svc-orphan')
        assert serve_state.get_service_from_name('svc-orphan') is None
        assert serve_state.get_service_pool_from_db('svc-orphan') is False
        assert serve_state.get_service_pool_from_db('never-existed') is None


class TestServiceLifecycleEpoch:
    """A durable name epoch fences work after advisory-session loss."""

    def test_epoch_survives_delete_and_recreate(self, _mock_serve_db):
        epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert epoch_a == 1
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch_a,
                                    resource_scope='incarnation-a')

        epoch_teardown = serve_state.claim_service_lifecycle_epoch('svc')
        assert epoch_teardown == 2
        assert not serve_state.remove_service_completely(
            'svc', 'incarnation-a', expected_lifecycle_epoch=epoch_a)
        assert serve_state.remove_service_completely(
            'svc', 'incarnation-a', expected_lifecycle_epoch=epoch_teardown)

        epoch_b = serve_state.claim_service_lifecycle_epoch('svc')
        assert epoch_b == 3
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-b',
                                    lifecycle_epoch=epoch_b,
                                    resource_scope='incarnation-b')
        row = _read_row(_mock_serve_db, 'svc')
        assert row['hash'] == 'incarnation-b'
        assert row['lifecycle_epoch'] == epoch_b
        assert row['resource_scope'] == 'incarnation-b'

    def test_stale_child_row_mutations_cannot_touch_successor(
            self, _mock_serve_db):
        epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch_a,
                                    resource_scope='incarnation-a')
        replica_a = _replica(1, cluster_name='replica-a')
        assert serve_state.add_or_update_replica(
            'svc',
            1,
            replica_a,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_a)

        epoch_delete = serve_state.claim_service_lifecycle_epoch('svc')
        assert serve_state.remove_service_completely(
            'svc', 'incarnation-a', expected_lifecycle_epoch=epoch_delete)
        epoch_b = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-b',
                                    lifecycle_epoch=epoch_b,
                                    resource_scope='incarnation-b')
        assert serve_state.add_or_update_replica(
            'svc',
            1,
            _replica(1, cluster_name='replica-b'),
            expected_service_hash='incarnation-b',
            expected_lifecycle_epoch=epoch_b)

        assert not serve_state.remove_replica(
            'svc',
            1,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_delete,
            expected_replica_record_id=replica_a.replica_record_id)
        assert not serve_state.remove_replicas(
            'svc', [1],
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_delete,
            expected_replica_record_ids={1: replica_a.replica_record_id})
        replica = serve_state.get_replica_info_from_id('svc', 1)
        assert replica is not None
        assert replica.cluster_name == 'replica-b'

    def test_exact_owner_replica_cleanup_is_idempotent_when_child_is_absent(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch,
                                    resource_scope='incarnation-a')

        assert serve_state.remove_replica('svc',
                                          99,
                                          expected_service_hash='incarnation-a',
                                          expected_lifecycle_epoch=epoch,
                                          expected_replica_record_id=str(
                                              uuid.uuid4()))

    def test_exact_owner_bulk_replica_cleanup_is_atomic_and_idempotent(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        owner = (123, '10.0.0.1')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch,
                                    resource_scope='incarnation-a',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1])
        expected_record_ids = {}
        for replica_id in range(1200):
            info = _replica(replica_id)
            expected_record_ids[replica_id] = info.replica_record_id
            assert serve_state.add_or_update_replica(
                'svc',
                replica_id,
                info,
                expected_service_hash='incarnation-a',
                expected_lifecycle_epoch=epoch)

        assert not serve_state.remove_replicas(
            'svc',
            list(range(1200)),
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch,
            expected_controller_owner=(456, '10.0.0.2'),
            expected_replica_record_ids=expected_record_ids)
        assert len(serve_state.get_replica_infos('svc')) == 1200
        assert serve_state.remove_replicas(
            'svc',
            list(range(1200)),
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch,
            expected_controller_owner=owner,
            expected_replica_record_ids=expected_record_ids)
        assert serve_state.get_replica_infos('svc') == []
        assert _read_row(_mock_serve_db, 'svc') is not None
        assert _read_version_row(_mock_serve_db, 'svc', 1) is not None
        assert serve_state.remove_replicas(
            'svc',
            list(range(1200)),
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch,
            expected_controller_owner=owner,
            expected_replica_record_ids=expected_record_ids)

    def test_stale_recovery_script_and_version_claims_fail(
            self, _mock_serve_db):
        epoch_a = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch_a,
                                    resource_scope='incarnation-a')
        assert serve_state.set_ha_recovery_script('svc', 'script-a', epoch_a)

        epoch_b = serve_state.claim_service_lifecycle_epoch('svc')
        assert not serve_state.set_ha_recovery_script('svc', 'stale-script',
                                                      epoch_a)
        assert serve_state.get_ha_recovery_script('svc') == 'script-a'
        with pytest.raises(RuntimeError, match='lifecycle ownership'):
            serve_state.add_version('svc',
                                    expected_service_hash='incarnation-a',
                                    expected_lifecycle_epoch=epoch_a)
        assert serve_state.add_version('svc',
                                       expected_service_hash='incarnation-a',
                                       expected_lifecycle_epoch=epoch_b) == 2

        assert not serve_state.add_or_update_version(
            'svc',
            2,
            None,
            'stale: yaml',
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch_a)
        assert serve_state.get_yaml_content('svc', 2) is None

    def test_version_cas_checks_authoritative_fence_before_service_row(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch)
        # Model a new lifecycle claimant that advanced the authoritative fence
        # but has not yet stamped services.lifecycle_epoch (it may be blocked
        # on that row behind this update transaction in PostgreSQL).
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(
                    serve_state.service_lifecycle_fences_table).where(
                        serve_state.service_lifecycle_fences_table.c.name ==
                        'svc').values(epoch=epoch + 1))
            session.commit()
        assert _read_row(_mock_serve_db, 'svc')['lifecycle_epoch'] == epoch

        with pytest.raises(RuntimeError, match='lifecycle ownership'):
            serve_state.add_version('svc',
                                    expected_service_hash='incarnation-a',
                                    expected_lifecycle_epoch=epoch)
        assert not serve_state.add_or_update_version(
            'svc',
            2,
            None,
            'yaml: stale',
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch)
        assert serve_state.get_latest_version('svc') == 1

    def test_status_and_teardown_claims_check_authoritative_fence(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch)
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.SHUTTING_DOWN)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                sqlalchemy.update(
                    serve_state.service_lifecycle_fences_table).where(
                        serve_state.service_lifecycle_fences_table.c.name ==
                        'svc').values(epoch=epoch + 1))
            session.commit()

        assert not serve_state.set_service_status_and_active_versions_if_hash(
            'svc',
            'incarnation-a',
            serve_state.ServiceStatus.FAILED_CLEANUP,
            expected_lifecycle_epoch=epoch)
        assert not serve_state.claim_orphaned_service_teardown(
            'svc',
            'incarnation-a',
            200,
            '10.0.0.2',
            999,
            '10.0.0.9',
            expected_lifecycle_epoch=epoch)
        assert not serve_state.claim_unrecoverable_service_teardown(
            'svc',
            'incarnation-a',
            200,
            '10.0.0.2',
            999,
            '10.0.0.9',
            expected_lifecycle_epoch=epoch)
        row = _read_row(_mock_serve_db, 'svc')
        assert row['status'] == serve_state.ServiceStatus.SHUTTING_DOWN.value
        assert row['controller_pid'] == 200

    def test_stale_controller_cannot_write_current_incarnation_children(
            self, _mock_serve_db):
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a')
        stale_owner = (100, '10.0.0.1')
        assert not serve_state.add_or_update_replica(
            'svc',
            1,
            'stale-single',
            expected_service_hash='incarnation-a',
            expected_controller_owner=stale_owner)
        assert not serve_state.add_or_update_replicas(
            'svc', [(1, 'stale-batch')],
            expected_service_hash='incarnation-a',
            expected_controller_owner=stale_owner)
        assert serve_state.get_replica_info_from_id('svc', 1) is None


class TestEphemeralStorageCleanupIntents:
    """Storage ownership is durable before upload and adopted atomically."""

    @staticmethod
    def _yaml(resource_scope: str, generation: str) -> str:
        return f"""\
metadata:
  {serve_constants.EPHEMERAL_STORAGE_SCOPE_METADATA_KEY}:
    resource_scope: {resource_scope}
    scope_id: svscope
    storage_generation: {generation}
    storage_mounts: []
service:
  readiness_probe: /
"""

    def test_initial_registration_adopts_intent_in_same_transaction(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-1')
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-1', yaml_content, False, epoch,
            True)
        assert _add_minimal_service('svc',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch,
                                    resource_scope='incarnation-a',
                                    yaml_content=yaml_content)

        intents = serve_state.get_ephemeral_storage_cleanup_intents('svc')
        assert len(intents) == 1
        assert intents[0]['provisional'] == 0

    def test_version_commit_adopts_new_generation_atomically(
            self, _mock_serve_db):
        first_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=first_epoch,
                                    resource_scope='incarnation-a')
        update_epoch = serve_state.claim_service_lifecycle_epoch('svc')
        yaml_content = self._yaml('incarnation-a', 'generation-2')
        assert serve_state.add_ephemeral_storage_cleanup_intent(
            'svc', 'incarnation-a', 'generation-2', yaml_content, False,
            update_epoch, True)

        assert serve_state.add_or_update_version(
            'svc',
            2,
            None,
            yaml_content,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=update_epoch,
            expected_controller_owner=(200, '10.0.0.2'))
        intent = serve_state.get_ephemeral_storage_cleanup_intents('svc')[0]
        assert intent['storage_generation'] == 'generation-2'
        assert intent['provisional'] == 0


class TestGetServiceFromNameReturnsControllerIp:
    """Joined service reads must surface the persisted controller IP."""

    def _add_with_version(self, service_name, controller_ip):
        # Reading via get_service_from_name requires a version_specs row
        # (it's an INNER JOIN); `add_service` writes the initial version row
        # in the same transaction, so this is enough for the JOIN to fire.
        _add_minimal_service(service_name, controller_ip=controller_ip)

    def test_round_trips_controller_ip(self, _mock_serve_db):
        self._add_with_version('svc-rt', controller_ip='10.4.10.8')
        record = serve_state.get_service_from_name('svc-rt')
        assert record is not None
        # The whole point: the dict KEY must exist with the persisted value.
        assert 'controller_ip' in record, (
            'controller_ip key missing from get_service_from_name() record — '
            'callers will silently fall back to localhost routing')
        assert record['controller_ip'] == '10.4.10.8'

    def test_round_trips_none_controller_ip(self, _mock_serve_db):
        # Even when the row was written without controller_ip (single-pod
        # mode), the dict must contain the key with value None — otherwise
        # `record.get('controller_ip')` and `record['controller_ip']` give
        # inconsistent answers.
        self._add_with_version('svc-rt-none', controller_ip=None)
        record = serve_state.get_service_from_name('svc-rt-none')
        assert record is not None
        assert 'controller_ip' in record
        assert record['controller_ip'] is None

    def test_record_includes_all_persisted_columns(self, _mock_serve_db):
        """Belt and braces: snapshot the keys we expect from the read path.
        If a future PR adds a column to services_table but forgets to update
        `_get_service_from_row`, this test fails loudly."""
        self._add_with_version('svc-rt-keys', controller_ip='10.0.0.1')
        record = serve_state.get_service_from_name('svc-rt-keys')
        assert record is not None
        for key in (
                'name',
                'status',
                'controller_pid',
                'controller_ip',
                'controller_port',
                'pool',
        ):
            assert key in record, f'missing key: {key}'


class TestGetServiceControllerOwner:
    """The proxy hot path reads one narrow services-table record."""

    def test_returns_only_routing_identity_without_loading_spec(
            self, _mock_serve_db, monkeypatch):
        _add_minimal_service('svc-owner', controller_ip='10.4.10.8')
        serve_state.set_service_controller_port('svc-owner', 20007)

        def fail_if_spec_loaded(*args, **kwargs):
            del args, kwargs
            raise AssertionError('owner lookup must not deserialize a spec')

        monkeypatch.setattr(serve_state, 'get_spec', fail_if_spec_loaded)
        record = serve_state.get_service_controller_owner('svc-owner')

        assert record is not None
        assert set(record) == {
            'hash',
            'status',
            'controller_pid',
            'controller_ip',
            'controller_port',
            'lifecycle_epoch',
            'pool',
            'resource_scope',
        }
        assert record['hash']
        assert record['status'] == serve_state.ServiceStatus.CONTROLLER_INIT
        assert record['controller_pid'] == 12345
        assert record['controller_ip'] == '10.4.10.8'
        assert record['controller_port'] == 20007
        assert record['pool'] is False

    def test_missing_row_returns_none(self, _mock_serve_db):
        assert serve_state.get_service_controller_owner('missing') is None

    def test_require_version_rejects_orphan_service_row(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc-orphan')

        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_controller_owner(
                'svc-orphan', require_version=True)

        assert record is None
        assert counts['n'] == 1


class TestGetServiceRuntimeSnapshot:
    """The controller hot paths should avoid the joined latest-spec read."""

    def test_returns_runtime_fields_without_loading_spec(
            self, _mock_serve_db, monkeypatch):
        spec = _FakeSpec('qps=3', 'least_load')
        _add_minimal_service('svc-runtime',
                             controller_ip='10.4.10.9',
                             spec=spec)
        serve_state.set_service_status_and_active_versions(
            'svc-runtime',
            serve_state.ServiceStatus.CONTROLLER_INIT,
            active_versions=[2])

        def fail_if_spec_loaded(*args, **kwargs):
            del args, kwargs
            raise AssertionError(
                'runtime snapshot must not deserialize the latest spec')

        monkeypatch.setattr(serve_state.pickle, 'loads', fail_if_spec_loaded)
        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_runtime_snapshot(
                'svc-runtime', require_version=True)

        assert counts['n'] == 1, counts
        assert record == {
            'hash': _read_row(_mock_serve_db, 'svc-runtime')['hash'],
            'controller_pid': 12345,
            'controller_ip': '10.4.10.9',
            'active_versions': [2],
        }

    def test_require_version_rejects_orphan_service_row(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc-orphan')

        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_runtime_snapshot(
                'svc-orphan', require_version=True)

        assert record is None
        assert counts['n'] == 1


class TestGetServiceStatusSnapshot:
    """Control paths read one slim, version-backed service row."""

    def test_returns_status_fields_without_loading_spec(self, _mock_serve_db,
                                                        monkeypatch):
        _add_minimal_service('svc-status',
                             controller_ip='10.4.10.10',
                             resource_scope='scope-a')
        serve_state.set_service_controller_port('svc-status', 20008)

        def fail_if_spec_loaded(*args, **kwargs):
            del args, kwargs
            raise AssertionError(
                'status snapshot must not deserialize the latest spec')

        monkeypatch.setattr(serve_state.pickle, 'loads', fail_if_spec_loaded)
        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_status_snapshot(
                'svc-status', require_version=True)

        row = _read_row(_mock_serve_db, 'svc-status')
        assert counts['n'] == 1, counts
        assert record == {
            'name': 'svc-status',
            'controller_job_id': 1,
            'controller_port': 20008,
            'load_balancer_port': None,
            'status': serve_state.ServiceStatus.CONTROLLER_INIT,
            'pool': False,
            'controller_pid': 12345,
            'controller_ip': '10.4.10.10',
            'hash': row['hash'],
            'lifecycle_epoch': row['lifecycle_epoch'],
            'resource_scope': 'scope-a',
            'workspace': None,
            'uptime': row['uptime'],
            'policy': 'policy',
            'requested_resources_str': '1x[CPU:1+]',
            'load_balancing_policy': 'round_robin',
            'tls_encrypted': False,
            'version': row['current_version'],
            'elected_version': row['current_version'],
            'active_versions': [],
            'logical_replica_semantics': False,
            'replica_unit': 'physical_backend',
        }

    def test_require_version_rejects_orphan_service_row(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc-orphan')

        with _count_sql_statements(_mock_serve_db) as counts:
            record = serve_state.get_service_status_snapshot(
                'svc-orphan', require_version=True)

        assert record is None
        assert counts['n'] == 1


class TestUpdateServiceControllerPidIpAndPort:
    """The atomic update is the core of the HA-recovery DB flip — it must
    write pid, ip, AND port in a single transaction so clients never
    observe a half-flipped row (e.g. new pid + old ip + stale port that
    points at a different service's listener on the new pod).

    Recovery picks port locally (find_free_port on the recovery pod);
    that new port has to land in DB together with pid/ip, otherwise
    a `_get_controller_url` consumer reading DB between writes could
    route to the new pod with the old port and hit the wrong listener.
    """

    def test_updates_all_three_fields(self, _mock_serve_db):
        _add_minimal_service('svc', controller_ip='10.0.0.7')
        serve_state.set_service_controller_port('svc', 20001)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=99999,
            controller_ip='10.0.0.8',
            controller_port=20007,
            expected_service_hash=service_hash,
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is True

        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 99999
        assert record['controller_ip'] == '10.0.0.8'
        assert record['controller_port'] == 20007

    def test_can_clear_controller_ip(self, _mock_serve_db):
        # If we ever need to write None (e.g. controller is on a non-K8s pod
        # in a hybrid deploy), the column must accept NULL. Port is still
        # required (it's an int column, no NULL).
        _add_minimal_service('svc', controller_ip='10.0.0.7')
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=99999,
            controller_ip=None,
            controller_port=20007,
            expected_service_hash=service_hash,
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is True
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 99999
        assert record['controller_ip'] is None
        assert record['controller_port'] == 20007

    def test_no_op_when_service_missing(self, _mock_serve_db):
        # Should not raise if the row was deleted between read and write
        # (e.g. a `down` raced our recovery).
        assert serve_state.update_service_controller_pid_ip_and_port(
            'never-existed',
            controller_pid=1,
            controller_ip='10.0.0.7',
            controller_port=20001,
            expected_service_hash='missing-incarnation',
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is False
        assert _read_row(_mock_serve_db, 'never-existed') is None

    def test_does_not_touch_other_fields(self, _mock_serve_db):
        # The atomic update must only touch the three specified columns —
        # don't want to clobber e.g. status, load_balancer_port from a
        # concurrent writer.
        _add_minimal_service('svc', controller_ip='10.0.0.7')
        serve_state.set_service_controller_port('svc', 20001)
        serve_state.set_service_load_balancer_port('svc', 30000)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=99999,
            controller_ip='10.0.0.8',
            controller_port=20007,
            expected_service_hash=service_hash,
            expected_controller_pid=12345,
            expected_controller_ip='10.0.0.7') is True

        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 99999
        assert record['controller_ip'] == '10.0.0.8'
        assert record['controller_port'] == 20007
        assert record['load_balancer_port'] == 30000  # untouched

    def test_rejects_purge_and_same_name_successor_with_same_pid(
            self, _mock_serve_db):
        _add_minimal_service('svc',
                             controller_ip='10.0.0.7',
                             controller_pid=777)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', old_hash, 777, '10.0.0.7', 888, '10.0.0.8') is True

        assert serve_state.remove_service_completely('svc', old_hash)
        _add_minimal_service('svc',
                             controller_ip='10.9.0.1',
                             controller_pid=888)
        successor = _read_row(_mock_serve_db, 'svc')
        assert successor['hash'] != old_hash

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=888,
            controller_ip='10.0.0.8',
            controller_port=20007,
            expected_service_hash=old_hash,
            expected_controller_pid=888,
            expected_controller_ip='10.0.0.8') is False
        record = _read_row(_mock_serve_db, 'svc')
        assert record['hash'] == successor['hash']
        assert record['controller_ip'] == '10.9.0.1'
        assert record['controller_port'] is None

    def test_publish_requires_preclaimed_ip_when_pid_collides(
            self, _mock_serve_db):
        _add_minimal_service('svc',
                             controller_ip='10.0.0.1',
                             controller_pid=777,
                             service_hash='same-incarnation')
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', 'same-incarnation', 777, '10.0.0.1', 777, '10.0.0.2') is True

        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=777,
            controller_ip='10.0.0.1',
            controller_port=20001,
            expected_service_hash='same-incarnation',
            expected_controller_pid=777,
            expected_controller_ip='10.0.0.1') is False
        assert serve_state.update_service_controller_pid_ip_and_port(
            'svc',
            controller_pid=777,
            controller_ip='10.0.0.2',
            controller_port=20002,
            expected_service_hash='same-incarnation',
            expected_controller_pid=777,
            expected_controller_ip='10.0.0.2') is True
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_ip'] == '10.0.0.2'
        assert record['controller_port'] == 20002


class TestUpdateServiceControllerPidIfOwner:
    """Recovery preclaim must fence by incarnation, pid, and controller IP."""

    def test_preclaim_requires_original_hash_and_pid(self, _mock_serve_db):
        _add_minimal_service('svc', controller_pid=111)
        serve_state.set_service_controller_port('svc', 20001)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', service_hash, 111, None, 222, '10.0.0.2') is True
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_pid'] == 222
        assert record['controller_ip'] == '10.0.0.2'
        assert record['controller_port'] is None
        # A second recovery that read owner 111 before the first claim loses.
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', service_hash, 111, None, 333, '10.0.0.3') is False
        assert _read_row(_mock_serve_db, 'svc')['controller_pid'] == 222

    def test_preclaim_rejects_same_name_successor(self, _mock_serve_db):
        _add_minimal_service('svc', controller_pid=111)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.remove_service_completely('svc', old_hash)
        # Deliberately reuse the same PID to model distinct Kubernetes pods.
        _add_minimal_service('svc', controller_pid=111)
        successor_hash = _read_row(_mock_serve_db, 'svc')['hash']

        assert serve_state.update_service_controller_pid_if_owner(
            'svc', old_hash, 111, None, 222, '10.0.0.2') is False
        record = _read_row(_mock_serve_db, 'svc')
        assert record['hash'] == successor_hash
        assert record['controller_pid'] == 111

    def test_ip_fences_equal_pids_within_same_incarnation(self, _mock_serve_db):
        _add_minimal_service('svc',
                             controller_pid=111,
                             controller_ip='10.0.0.1',
                             service_hash='same-incarnation')
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', 'same-incarnation', 111, '10.0.0.1', 111, '10.0.0.2') is True
        # A stale parent on the old pod can have the same namespace-local PID,
        # but its old IP is no longer authoritative.
        assert serve_state.update_service_controller_pid_if_owner(
            'svc', 'same-incarnation', 111, '10.0.0.1', 111,
            '10.0.0.3') is False
        assert _read_row(_mock_serve_db, 'svc')['controller_ip'] == '10.0.0.2'


class TestSetServiceControllerPortIfOwner:
    """Compare-and-swap port write for the in-place controller respawn: the
    UPDATE must be filtered on hash/PID/IP so a parent whose row was taken over
    by HA recovery cannot clobber the new owner's port."""

    def test_owner_updates_port(self, _mock_serve_db):
        _add_minimal_service('svc')  # seeds controller_pid=12345
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_controller_port_if_owner(
            'svc', service_hash, 12345, None, 20123) is True
        assert _read_row(_mock_serve_db, 'svc')['controller_port'] == 20123

    def test_non_owner_is_rejected(self, _mock_serve_db):
        _add_minimal_service('svc')
        serve_state.set_service_controller_port('svc', 20123)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_controller_port_if_owner(
            'svc', service_hash, 99999, None, 20999) is False
        assert _read_row(_mock_serve_db, 'svc')['controller_port'] == 20123

    def test_missing_service_returns_false(self, _mock_serve_db):
        assert serve_state.set_service_controller_port_if_owner(
            'never-existed', 'missing-hash', 12345, None, 20123) is False

    def test_same_pid_successor_is_rejected_by_hash(self, _mock_serve_db):
        _add_minimal_service('svc', controller_pid=12345)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.remove_service_completely('svc', old_hash)
        _add_minimal_service('svc', controller_pid=12345)
        assert serve_state.set_service_controller_port_if_owner(
            'svc', old_hash, 12345, None, 20123) is False
        assert _read_row(_mock_serve_db, 'svc')['controller_port'] is None


class TestAcknowledgeControllerTeardown:
    """Owner-only teardown ack must publish the terminal controller port."""

    def test_owner_atomically_publishes_terminal_status_and_ack(
            self, _mock_serve_db):
        _add_minimal_service('svc', service_hash='incarnation-a')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.READY)

        assert serve_state.acknowledge_service_controller_teardown_if_owner(
            'svc', 'incarnation-a', 12345, None)

        row = _read_row(_mock_serve_db, 'svc')
        assert row['status'] == serve_state.ServiceStatus.SHUTTING_DOWN.value
        assert (row['controller_port'] ==
                serve_constants.CONTROLLER_TEARDOWN_ACK_PORT)

    def test_stale_owner_cannot_change_successor_status(self, _mock_serve_db):
        _add_minimal_service('svc', service_hash='incarnation-b')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.READY)

        assert not serve_state.acknowledge_service_controller_teardown_if_owner(
            'svc', 'incarnation-a', 12345, None)
        assert (_read_row(
            _mock_serve_db,
            'svc')['status'] == serve_state.ServiceStatus.READY.value)


class TestSetServiceLoadBalancerPortIfOwner:
    """CAS port write for recovery's external-LB republish: the UPDATE is
    filtered on hash/PID/IP so a stale recovery cannot write to a
    same-name successor's row."""

    def test_owner_updates_port(self, _mock_serve_db):
        _add_minimal_service('svc')  # seeds controller_pid=12345
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', service_hash, 12345, None, 30001) is True
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] == 30001

    def test_owner_updates_null_port(self, _mock_serve_db):
        # NULL -> port must work for the owner: recovery of an up() that
        # crashed before registration is the case this setter exists for.
        _add_minimal_service('svc')
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] is None
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', service_hash, 12345, None, 30001) is True
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] == 30001

    def test_non_owner_is_rejected(self, _mock_serve_db):
        _add_minimal_service('svc')
        serve_state.set_service_load_balancer_port('svc', 30002)
        service_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', service_hash, 99999, None, 30001) is False
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] == 30002

    def test_missing_service_returns_false(self, _mock_serve_db):
        assert serve_state.set_service_load_balancer_port_if_owner(
            'never-existed', 'missing-hash', 12345, None, 30001) is False

    def test_same_pid_successor_is_rejected_by_hash(self, _mock_serve_db):
        _add_minimal_service('svc', controller_pid=12345)
        old_hash = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.remove_service_completely('svc', old_hash)
        _add_minimal_service('svc', controller_pid=12345)
        assert serve_state.set_service_load_balancer_port_if_owner(
            'svc', old_hash, 12345, None, 30001) is False
        assert _read_row(_mock_serve_db, 'svc')['load_balancer_port'] is None


class TestSetServiceControllerIp:
    """Standalone IP setter is rarely called (the atomic version is preferred
    for recovery), but kept for symmetry with set_service_controller_port."""

    def test_basic(self, _mock_serve_db):
        _add_minimal_service('svc', controller_ip=None)
        serve_state.set_service_controller_ip('svc', '10.0.0.9')
        record = _read_row(_mock_serve_db, 'svc')
        assert record['controller_ip'] == '10.0.0.9'
        # pid unchanged
        assert record['controller_pid'] == 12345


class TestRemoveServiceCompletely:
    """`remove_service_completely` deletes services / version_specs /
    serve_ha_recovery_script in one transaction. Sequential deletes had
    a real-world failure mode where the last call
    was the one most likely to be skipped
    when the subprocess died mid-cleanup, leaving the row orphaned across
    many test runs while the other tables stayed clean. This guarantees
    all-or-nothing teardown for service metadata.
    """

    def _populate(self, engine, name):
        # Seed all service tables plus a replica row so the exact incarnation
        # can be removed atomically.
        _add_minimal_service(name, controller_ip='10.0.0.1')
        serve_state.add_version(name)
        serve_state.set_ha_recovery_script(name, 'dummy script')
        # replicas: a minimal pickled ReplicaInfo proxy is hard to
        # construct here, so just write a row directly to the table.
        with orm.Session(engine) as session:
            session.execute(serve_state.replicas_table.insert().values(
                service_name=name, replica_id=1, replica_info=b'fake-pickle'))
            session.commit()

    def test_failed_cleanup_status_retains_metadata_and_reserves_name(
            self, _mock_serve_db):
        self._populate(_mock_serve_db, 'svc-retained')
        service_hash = _read_row(_mock_serve_db, 'svc-retained')['hash']

        assert serve_state.set_service_status_and_active_versions_if_owner(
            'svc-retained',
            service_hash,
            12345,
            '10.0.0.1',
            serve_state.ServiceStatus.FAILED_CLEANUP,
            expected_status=serve_state.ServiceStatus.CONTROLLER_INIT)

        with orm.Session(_mock_serve_db) as session:
            for _, column in [
                (serve_state.services_table, serve_state.services_table.c.name),
                (serve_state.replicas_table,
                 serve_state.replicas_table.c.service_name),
                (serve_state.version_specs_table,
                 serve_state.version_specs_table.c.service_name),
                (serve_state.serve_ha_recovery_script_table,
                 serve_state.serve_ha_recovery_script_table.c.service_name),
            ]:
                assert session.execute(
                    sqlalchemy.select(column).where(
                        column == 'svc-retained')).first() is not None
        # The durable services row keeps same-name H_new from registering.
        assert not _add_minimal_service('svc-retained')

    def test_removes_service_metadata_tables(self, _mock_serve_db):
        self._populate(_mock_serve_db, 'svc-rsc')
        # Sanity: rows are present.
        with orm.Session(_mock_serve_db) as session:
            assert session.execute(
                sqlalchemy.select(serve_state.services_table.c.name).where(
                    serve_state.services_table.c.name ==
                    'svc-rsc')).first() is not None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.serve_ha_recovery_script_table.c.service_name).
                where(serve_state.serve_ha_recovery_script_table.c.service_name
                      == 'svc-rsc')).first() is not None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.service_name).where(
                        serve_state.replicas_table.c.service_name ==
                        'svc-rsc')).first() is not None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.version_specs_table.c.service_name).where(
                        serve_state.version_specs_table.c.service_name ==
                        'svc-rsc')).first() is not None

        service_hash = _read_row(_mock_serve_db, 'svc-rsc')['hash']
        assert serve_state.remove_service_completely('svc-rsc', service_hash)

        # The three metadata tables must be gone.
        with orm.Session(_mock_serve_db) as session:
            assert session.execute(
                sqlalchemy.select(serve_state.services_table.c.name).where(
                    serve_state.services_table.c.name ==
                    'svc-rsc')).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.serve_ha_recovery_script_table.c.service_name).
                where(serve_state.serve_ha_recovery_script_table.c.service_name
                      == 'svc-rsc')).first() is None
            assert session.execute(
                sqlalchemy.select(
                    serve_state.version_specs_table.c.service_name).where(
                        serve_state.version_specs_table.c.service_name ==
                        'svc-rsc')).first() is None
            # Replica rows are children of the incarnation and must be gone in
            # the same transaction, or a stale purge can leave rows that a
            # same-name successor reads as its own.
            assert session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.service_name).where(
                        serve_state.replicas_table.c.service_name ==
                        'svc-rsc')).first() is None

    def test_does_not_touch_other_services(self, _mock_serve_db):
        """Make sure deletion is scoped to the named service."""
        self._populate(_mock_serve_db, 'svc-keep')
        self._populate(_mock_serve_db, 'svc-drop')

        drop_hash = _read_row(_mock_serve_db, 'svc-drop')['hash']
        assert serve_state.remove_service_completely('svc-drop', drop_hash)

        # svc-keep's rows must all survive.
        with orm.Session(_mock_serve_db) as session:
            for tbl_name, tbl, col_name in [
                ('services', serve_state.services_table, 'name'),
                ('version_specs', serve_state.version_specs_table,
                 'service_name'),
                ('serve_ha_recovery_script',
                 serve_state.serve_ha_recovery_script_table, 'service_name'),
                ('replicas', serve_state.replicas_table, 'service_name'),
            ]:
                col = getattr(tbl.c, col_name)
                assert session.execute(
                    sqlalchemy.select(col).where(
                        col == 'svc-keep')).first() is not None, (
                            f'svc-keep was wrongly removed from {tbl_name}')

    def test_no_op_when_nothing_to_delete(self, _mock_serve_db):
        # Should be a silent no-op when no rows exist.
        assert not serve_state.remove_service_completely(
            'never-existed', 'missing-hash')

    def test_stale_a_delete_spares_successor_b_and_all_children(
            self, _mock_serve_db):
        self._populate(_mock_serve_db, 'svc')
        hash_a = _read_row(_mock_serve_db, 'svc')['hash']
        assert serve_state.remove_service_completely('svc', hash_a)

        self._populate(_mock_serve_db, 'svc')
        hash_b = _read_row(_mock_serve_db, 'svc')['hash']
        assert hash_b != hash_a

        assert not serve_state.remove_service_completely('svc', hash_a)
        assert _read_row(_mock_serve_db, 'svc')['hash'] == hash_b
        with orm.Session(_mock_serve_db) as session:
            for table, column in [
                (serve_state.replicas_table,
                 serve_state.replicas_table.c.service_name),
                (serve_state.version_specs_table,
                 serve_state.version_specs_table.c.service_name),
                (serve_state.serve_ha_recovery_script_table,
                 serve_state.serve_ha_recovery_script_table.c.service_name),
            ]:
                assert session.execute(
                    sqlalchemy.select(column).where(
                        column == 'svc')).first() is not None, table.name


class TestTerminalVersionFences:
    """A down/purge winner must prevent both phases of update commit."""

    def test_terminal_status_rejects_placeholder_and_yaml_commit(
            self, _mock_serve_db):
        epoch = serve_state.claim_service_lifecycle_epoch('svc')
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a',
                                    lifecycle_epoch=epoch)
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.SHUTTING_DOWN)

        with pytest.raises(RuntimeError, match='terminal status'):
            serve_state.add_version('svc',
                                    expected_service_hash='incarnation-a',
                                    expected_lifecycle_epoch=epoch)
        assert not serve_state.add_or_update_version(
            'svc',
            2,
            None,
            'yaml: v2',
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=epoch,
            expected_controller_owner=(200, '10.0.0.2'))
        assert serve_state.get_latest_version('svc') == 1


class TestUnrecoverableServiceRows:
    """No-yaml rows make HA recovery impossible and must stay purgeable."""

    def test_ha_retirement_marks_terminal_and_removes_script(
            self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        assert serve_state.set_ha_recovery_script('svc', 'impossible script')

        assert serve_state.mark_unrecoverable_service_for_cleanup('svc',
                                                                  'orphan',
                                                                  pool=False)
        assert _read_row(
            _mock_serve_db,
            'svc')['status'] == serve_state.ServiceStatus.FAILED_CLEANUP.value
        assert serve_state.get_ha_recovery_script('svc') is None

    def test_purge_claim_consumes_impossible_script(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.SHUTTING_DOWN)
        assert serve_state.set_ha_recovery_script('svc', 'impossible script')

        assert serve_state.claim_unrecoverable_service_teardown(
            'svc', 'orphan', 12345, None, 999, '10.0.0.9')
        owner = serve_state.get_service_controller_owner('svc')
        assert owner is not None
        assert owner['controller_pid'] == 999
        assert owner['controller_ip'] == '10.0.0.9'
        assert (owner['controller_port'] ==
                serve_constants.CONTROLLER_TEARDOWN_ACK_PORT)
        assert serve_state.get_ha_recovery_script('svc') is None

    def test_ha_retirement_cannot_overwrite_active_purge(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.SHUTTING_DOWN)
        assert serve_state.set_ha_recovery_script('svc', 'impossible script')

        assert not serve_state.mark_unrecoverable_service_for_cleanup(
            'svc', 'orphan', pool=False)
        assert _read_row(
            _mock_serve_db,
            'svc')['status'] == serve_state.ServiceStatus.SHUTTING_DOWN.value
        assert (
            serve_state.get_ha_recovery_script('svc') == 'impossible script')

    def test_ha_retirement_rechecks_concurrent_committed_version(
            self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        assert serve_state.set_ha_recovery_script('svc', 'bootable script')
        serve_state.add_or_update_version('svc', 1, 'spec-1', 'yaml: v1')

        assert not serve_state.mark_unrecoverable_service_for_cleanup(
            'svc', 'orphan', pool=False)
        assert serve_state.get_ha_recovery_script('svc') == 'bootable script'

    def test_purge_claim_refuses_committed_version(self, _mock_serve_db):
        assert _add_minimal_service('svc', service_hash='incarnation-a')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.SHUTTING_DOWN)
        assert serve_state.set_ha_recovery_script('svc', 'valid script')

        assert not serve_state.claim_unrecoverable_service_teardown(
            'svc', 'incarnation-a', 12345, None, 999, '10.0.0.9')
        assert serve_state.get_ha_recovery_script('svc') == 'valid script'


class TestRecoveryVersionSelection:
    """An interrupted `sky serve update` leaves a NULL-yaml placeholder
    version (written by `add_version`) as MAX(version). Recovery must resume
    the latest version whose yaml was actually committed, otherwise the
    controller crash-loops asserting on the NULL yaml. These tests pin that
    `get_latest_committed_version` skips the placeholder."""

    def test_committed_version_skips_placeholder(self, _mock_serve_db):
        # Two committed versions, then an interrupted update creates a
        # NULL-yaml placeholder v3 as the new MAX.
        serve_state.add_or_update_version('svc', 1, 'spec-1', 'yaml: v1')
        serve_state.add_or_update_version('svc', 2, 'spec-2', 'yaml: v2')
        assert serve_state.add_version('svc') == 3  # placeholder
        # Raw MAX points at the placeholder (the bug)...
        assert serve_state.get_latest_version('svc') == 3
        # ...but recovery must pick the newest committed version, not the
        # placeholder and not the original.
        assert serve_state.get_latest_committed_version('svc') == 2

    def test_committed_version_none_when_only_placeholder(self, _mock_serve_db):
        serve_state.add_version('svc')  # placeholder v1, no committed yaml
        assert serve_state.get_latest_committed_version('svc') is None

    def test_batch_committed_versions_match_single_row_answers(
            self, _mock_serve_db):
        serve_state.add_or_update_version('svc-a', 1, 'spec-a1', 'yaml: a1')
        serve_state.add_or_update_version('svc-a', 2, 'spec-a2', 'yaml: a2')
        serve_state.add_version('svc-a')  # placeholder v3
        serve_state.add_version('svc-b')  # placeholder v1, never committed
        serve_state.add_or_update_version('svc-c', 1, 'spec-c1', 'yaml: c1')

        with _count_sql_statements(_mock_serve_db) as counts:
            committed_versions = serve_state.get_latest_committed_versions(
                ['svc-a', 'svc-b', 'svc-c', 'missing', 'svc-a'])

        assert counts['n'] == 1
        assert committed_versions == {
            'svc-a': 2,
            'svc-c': 1,
        }
        assert committed_versions.get('svc-a') == (
            serve_state.get_latest_committed_version('svc-a'))
        assert committed_versions.get('svc-c') == (
            serve_state.get_latest_committed_version('svc-c'))
        assert 'svc-b' not in committed_versions
        assert 'missing' not in committed_versions
        assert serve_state.get_latest_committed_version('svc-b') is None

    def test_batch_service_mode_and_hashes_match_single_row_answers(
            self, _mock_serve_db):
        assert _add_minimal_service('svc-a', service_hash='hash-a', pool=False)
        assert _add_minimal_service('svc-b', service_hash='hash-b', pool=True)

        with _count_sql_statements(_mock_serve_db) as counts:
            identities = serve_state.get_service_mode_and_hashes(
                ['svc-a', 'svc-b', 'missing', 'svc-a'])

        assert counts['n'] == 1
        assert identities == {
            'svc-a': (False, 'hash-a'),
            'svc-b': (True, 'hash-b'),
        }
        assert identities.get('svc-a') == serve_state.get_service_mode_and_hash(
            'svc-a')
        assert identities.get('svc-b') == serve_state.get_service_mode_and_hash(
            'svc-b')
        assert 'missing' not in identities
        assert serve_state.get_service_mode_and_hash('missing') is None

    @pytest.mark.parametrize('getter_name', [
        'get_latest_committed_versions',
        'get_service_mode_and_hashes',
    ])
    def test_batch_recovery_fallbacks_bound_empty_and_chunked_queries(
            self, _mock_serve_db, getter_name):
        getter = getattr(serve_state, getter_name)
        with _count_sql_statements(_mock_serve_db) as empty_counts:
            assert getter([]) == {}
        assert empty_counts['n'] == 0

        batch_size = serve_state._TERMINAL_IDENTITY_QUERY_BATCH_SIZE
        missing_names = [f'missing-{i}' for i in range(batch_size + 1)]
        with _count_sql_statements(_mock_serve_db) as chunked_counts:
            assert getter(missing_names + [missing_names[0]]) == {}
        assert chunked_counts['n'] == 2

    @pytest.mark.parametrize('getter_name', [
        'get_latest_committed_versions',
        'get_service_mode_and_hashes',
    ])
    def test_batch_recovery_fallbacks_return_second_chunk_values(
            self, _mock_serve_db, getter_name):
        # Regression guard for PR #911: the batched HA-recovery reads
        # accumulate results across `_TERMINAL_IDENTITY_QUERY_BATCH_SIZE`
        # chunks via `rows.extend(...)`. The sibling ...bound_empty_and_chunked
        # test requests only all-missing names, so it asserts the statement
        # count but never that a real row landing in the SECOND chunk survives
        # accumulation. A refactor that dropped later chunks (e.g. `rows =`
        # instead of `rows.extend`, or an early `break`) would still issue two
        # statements yet silently lose the second-chunk service. This pins that
        # a real second-chunk value is returned and equals the single-row read.
        batch_size = serve_state._TERMINAL_IDENTITY_QUERY_BATCH_SIZE
        # The getters read sorted(set(names)); a 'zzz-' name sorts after the
        # `batch_size` fillers, landing at index == batch_size (first row of
        # the second chunk).
        filler_names = [f'chunk-miss-{i:04d}' for i in range(batch_size)]
        real_name = 'zzz-second-chunk-svc'
        getter = getattr(serve_state, getter_name)
        if getter_name == 'get_latest_committed_versions':
            serve_state.add_or_update_version(real_name, 1, 'spec-1',
                                              'yaml: v1')
            serve_state.add_or_update_version(real_name, 2, 'spec-2',
                                              'yaml: v2')
            serve_state.add_version(real_name)  # uncommitted placeholder v3
            expected_value = 2
            single_row_value = serve_state.get_latest_committed_version(
                real_name)
        else:
            assert _add_minimal_service(real_name,
                                        service_hash='hash-z',
                                        pool=True)
            expected_value = (True, 'hash-z')
            single_row_value = serve_state.get_service_mode_and_hash(real_name)

        with _count_sql_statements(_mock_serve_db) as counts:
            result = getter(filler_names + [real_name])

        assert counts['n'] == 2, counts
        assert result == {real_name: expected_value}
        assert result.get(real_name) == single_row_value

    def test_version_records_include_commit_provenance(self, _mock_serve_db,
                                                       monkeypatch):
        timestamps = iter([1001.0, 1002.0])
        monkeypatch.setattr(serve_state.time, 'time', lambda: next(timestamps))
        assert _add_minimal_service('svc',
                                    spec='spec-1',
                                    created_by='alice',
                                    submitted_yaml_content='submitted: v1')
        assert serve_state.add_version('svc', created_by='bob') == 2
        serve_state.add_or_update_version(
            'svc',
            2,
            'spec-2',
            'yaml: v2',
            submitted_yaml_content='submitted: v2')

        assert serve_state.get_version_records('svc') == [{
            'version': 1,
            'spec': 'spec-1',
            'yaml_content': 'yaml: v1',
            'submitted_yaml_content': 'submitted: v1',
            'created_at': 1001.0,
            'created_by': 'alice',
            'quarantined_at': None,
            'quarantine_reason': None,
        }, {
            'version': 2,
            'spec': 'spec-2',
            'yaml_content': 'yaml: v2',
            'submitted_yaml_content': 'submitted: v2',
            'created_at': 1002.0,
            'created_by': 'bob',
            'quarantined_at': None,
            'quarantine_reason': None,
        }]

    def test_quarantine_is_durable_and_applicable_snapshot_skips_it(
            self, _mock_serve_db):
        serve_state.add_or_update_version('svc', 1, 'spec-1', 'yaml: v1')
        serve_state.add_or_update_version('svc', 2, 'spec-2', 'yaml: v2')

        assert serve_state.quarantine_version('svc',
                                              2,
                                              'deterministic port failure',
                                              quarantined_at=123.0)
        assert serve_state.get_latest_committed_version('svc') == 2
        assert serve_state.get_latest_committed_version_spec('svc') == (
            2, 'spec-2')
        assert serve_state.get_latest_applicable_version_spec('svc') == (
            1, 'spec-1')
        assert serve_state.get_latest_quarantined_version('svc') == {
            'version': 2,
            'quarantined_at': 123.0,
            'quarantine_reason': 'deterministic port failure',
        }

        serve_state.add_or_update_version('svc', 3, 'spec-3', 'yaml: v3')
        assert serve_state.get_latest_applicable_version_spec('svc') == (
            3, 'spec-3')

    def test_recovery_prefers_proven_active_version_below_quarantine(
            self, _mock_serve_db):
        assert _add_minimal_service('svc', spec='spec-1')
        serve_state.add_or_update_version('svc', 2, 'spec-2', 'yaml: v2')
        serve_state.add_or_update_version('svc', 3, 'spec-3', 'yaml: v3')
        serve_state.set_service_status_and_active_versions(
            'svc', serve_state.ServiceStatus.READY, active_versions=[1])

        assert serve_state.quarantine_version('svc', 3, 'never ready')
        # Version 2 is committed but never became an active routing version.
        assert serve_state.get_latest_applicable_version_spec('svc') == (
            2, 'spec-2')
        assert serve_state.get_recovery_version_spec('svc') == (1, 'spec-1')

        # A later commit supersedes the quarantine and remains eligible for a
        # fresh rollout on recovery.
        serve_state.add_or_update_version('svc', 4, 'spec-4', 'yaml: v4')
        assert serve_state.get_recovery_version_spec('svc') == (4, 'spec-4')

    def test_quarantine_rejects_placeholder_and_is_idempotent(
            self, _mock_serve_db):
        assert serve_state.add_version('svc') == 1
        assert not serve_state.quarantine_version('svc', 1, 'not committed')
        serve_state.add_or_update_version('svc', 1, 'spec-1', 'yaml: v1')
        assert serve_state.quarantine_version('svc',
                                              1,
                                              'first reason',
                                              quarantined_at=123.0)
        assert serve_state.quarantine_version('svc',
                                              1,
                                              'second reason',
                                              quarantined_at=456.0)
        assert serve_state.get_latest_quarantined_version('svc') == {
            'version': 1,
            'quarantined_at': 123.0,
            'quarantine_reason': 'first reason',
        }

    def test_quarantine_is_fenced_by_controller_ownership(self, _mock_serve_db):
        assert _add_minimal_service('svc-owner',
                                    service_hash='incarnation-a',
                                    controller_pid=123,
                                    controller_ip='10.0.0.1')
        serve_state.add_or_update_version('svc-owner', 2, 'spec-2', 'yaml: v2')

        assert not serve_state.quarantine_version(
            'svc-owner',
            2,
            'stale controller',
            expected_service_hash='incarnation-a',
            expected_controller_owner=(456, '10.0.0.2'))
        assert serve_state.get_latest_quarantined_version('svc-owner') is None

        assert serve_state.quarantine_version(
            'svc-owner',
            2,
            'current controller',
            quarantined_at=123.0,
            expected_service_hash='incarnation-a',
            expected_controller_owner=(123, '10.0.0.1'))
        assert serve_state.get_latest_quarantined_version('svc-owner') == {
            'version': 2,
            'quarantined_at': 123.0,
            'quarantine_reason': 'current controller',
        }

    def test_committed_version_spec_is_one_row_snapshot(self, _mock_serve_db):
        serve_state.add_or_update_version('svc', 1, 'spec-1', 'yaml: v1')
        serve_state.add_or_update_version('svc', 2, 'spec-2', 'yaml: v2')
        serve_state.add_version('svc')  # placeholder v3

        statements = []

        def _record_statement(*args):
            statements.append(args[2])

        sqlalchemy.event.listen(_mock_serve_db, 'before_cursor_execute',
                                _record_statement)
        try:
            snapshot = serve_state.get_latest_committed_version_spec('svc')
        finally:
            sqlalchemy.event.remove(_mock_serve_db, 'before_cursor_execute',
                                    _record_statement)

        assert snapshot == (2, 'spec-2')
        assert len(statements) == 1, statements

    def test_committed_version_spec_none_without_committed_row(
            self, _mock_serve_db):
        serve_state.add_version('svc')
        assert serve_state.get_latest_committed_version_spec('svc') is None

    def test_committed_version_spec_none_for_unusable_spec(
            self, _mock_serve_db):
        serve_state.add_or_update_version('svc', 1, None, 'yaml: v1')
        assert serve_state.get_latest_committed_version_spec('svc') is None


class TestUnrecoverableServiceCleanup:
    """Rows without committed YAML must not wait on impossible recovery."""

    def test_ha_retirement_marks_failed_and_removes_script(
            self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        serve_state.set_ha_recovery_script('svc', 'unbootable script')

        assert serve_state.mark_unrecoverable_service_for_cleanup('svc',
                                                                  'orphan',
                                                                  pool=False)
        assert (_read_row(
            _mock_serve_db,
            'svc')['status'] == serve_state.ServiceStatus.FAILED_CLEANUP.value)
        assert serve_state.get_ha_recovery_script('svc') is None

    def test_purge_claim_consumes_unbootable_script(self, _mock_serve_db):
        _insert_orphan_service_row(_mock_serve_db, 'svc')
        assert serve_state.set_service_status_and_active_versions_if_hash(
            'svc', 'orphan', serve_state.ServiceStatus.SHUTTING_DOWN)
        serve_state.set_ha_recovery_script('svc', 'unbootable script')

        assert serve_state.claim_unrecoverable_service_teardown(
            'svc', 'orphan', 12345, None, 67890, '10.0.0.2')
        row = _read_row(_mock_serve_db, 'svc')
        assert row['controller_pid'] == 67890
        assert row['controller_ip'] == '10.0.0.2'
        assert (row['controller_port'] ==
                serve_constants.CONTROLLER_TEARDOWN_ACK_PORT)
        assert serve_state.get_ha_recovery_script('svc') is None

    def test_committed_version_prevents_unrecoverable_claim(
            self, _mock_serve_db):
        assert _add_minimal_service('svc',
                                    controller_pid=12345,
                                    service_hash='incarnation-a')
        assert serve_state.set_service_status_and_active_versions_if_hash(
            'svc', 'incarnation-a', serve_state.ServiceStatus.SHUTTING_DOWN)
        serve_state.set_ha_recovery_script('svc', 'bootable script')

        assert not serve_state.claim_unrecoverable_service_teardown(
            'svc', 'incarnation-a', 12345, None, 67890, '10.0.0.2')
        assert serve_state.get_ha_recovery_script('svc') == 'bootable script'


class TestTerminalServiceRejectsVersionWrites:
    """Terminal services must reject later version placeholder/YAML writes."""

    def test_down_winner_blocks_placeholder_and_yaml_commit(
            self, _mock_serve_db):
        assert _add_minimal_service('svc',
                                    controller_pid=12345,
                                    service_hash='incarnation-a')
        assert serve_state.set_service_status_and_active_versions_if_hash(
            'svc', 'incarnation-a', serve_state.ServiceStatus.SHUTTING_DOWN)

        with pytest.raises(RuntimeError, match='terminal status'):
            serve_state.add_version('svc')
        assert not serve_state.add_or_update_version('svc', 2, 'spec-2',
                                                     'yaml: v2')
        assert serve_state.get_latest_version('svc') == 1
        assert serve_state.get_yaml_content('svc', 1) == 'yaml: v1'


class TestBatchReplicaUpsert:
    """add_or_update_replicas: one statement for a probe round's writes."""

    def test_batch_insert_then_batch_update(self, _mock_serve_db):
        infos = [(i, _replica(i, version=1)) for i in range(1, 4)]
        serve_state.add_or_update_replicas('svc', infos)
        rows = {
            i: serve_state.get_replica_info_from_id('svc', i)
            for i in range(1, 4)
        }
        assert all(rows[i].version == 1 for i in range(1, 4))

        # Bookkeeping carries each immutable record identity and updates in
        # place without an insert-capable conflict path.
        updated = []
        for replica_id, info in rows.items():
            info.version = 2
            updated.append((replica_id, info))
        serve_state.add_or_update_replicas('svc',
                                           updated,
                                           expected_replica_exists=True)
        assert all(
            serve_state.get_replica_info_from_id('svc', i).version == 2
            for i in range(1, 4))
        assert len(serve_state.get_replica_infos('svc')) == 3

    def test_empty_batch_is_noop(self, _mock_serve_db):
        serve_state.add_or_update_replicas('svc', [])
        assert not serve_state.get_replica_infos('svc')

    def test_empty_batch_fence_requires_and_validates_owner_identity(
            self, _mock_serve_db):
        incomplete_fences = ({}, {
            'expected_service_hash': 'incarnation-a'
        }, {
            'expected_controller_owner': (200, '10.0.0.2')
        }, {
            'expected_service_hash': 'incarnation-a',
            'expected_controller_owner': (None, None)
        })
        for fence in incomplete_fences:
            assert not serve_state.add_or_update_replicas(
                'svc', [], validate_fence_on_empty=True, **fence)
        assert _add_minimal_service('svc',
                                    controller_pid=200,
                                    controller_ip='10.0.0.2',
                                    service_hash='incarnation-a')
        assert serve_state.add_or_update_replicas(
            'svc', [],
            expected_service_hash='incarnation-a',
            expected_controller_owner=(200, '10.0.0.2'),
            expected_replica_exists=True,
            validate_fence_on_empty=True)
        assert not serve_state.add_or_update_replicas(
            'svc', [],
            expected_service_hash='incarnation-a',
            expected_controller_owner=(100, '10.0.0.1'),
            expected_replica_exists=True,
            validate_fence_on_empty=True)

    def test_batch_larger_than_chunk_size(self, _mock_serve_db):
        chunk_size = (serve_state._SQLITE_MAX_BIND_PARAMS //
                      len(serve_state.replicas_table.c))
        n = chunk_size * 2 + 17
        infos = [(i, _replica(i)) for i in range(1, n + 1)]
        serve_state.add_or_update_replicas('svc', infos)
        assert len(serve_state.get_replica_infos('svc')) == n

    def test_batch_respects_legacy_sqlite_bind_limit(self, _mock_serve_db):
        raw_connection = _mock_serve_db.raw_connection()
        try:
            raw_connection.driver_connection.setlimit(
                sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER,
                serve_state._SQLITE_MAX_BIND_PARAMS)
        finally:
            raw_connection.close()

        infos = [(i, _replica(i)) for i in range(1, 91)]
        serve_state.add_or_update_replicas('svc', infos)
        assert len(serve_state.get_replica_infos('svc')) == 90


class TestExpectedExistingReplicaPersistence:
    """Terminal deletes are absorbing for stale bookkeeping snapshots."""

    def test_delete_first_rejects_stale_single_update(self, _mock_serve_db):
        assert serve_state.add_or_update_replica('svc', 1, _replica(1))
        stale = serve_state.get_replica_info_from_id('svc', 1)
        assert stale is not None
        stale.version = 2

        assert serve_state.remove_replica(
            'svc', 1, expected_replica_record_id=stale.replica_record_id)
        assert not serve_state.add_or_update_replica(
            'svc', 1, stale, expected_replica_exists=True)
        assert serve_state.get_replica_info_from_id('svc', 1) is None

    def test_delete_first_rejects_entire_stale_batch(self, _mock_serve_db):
        assert serve_state.add_or_update_replicas('svc', [(1, _replica(1)),
                                                          (2, _replica(2))])
        stale = serve_state.get_replica_info_from_id('svc', 1)
        survivor_update = serve_state.get_replica_info_from_id('svc', 2)
        assert stale is not None and survivor_update is not None
        stale.version = 2
        survivor_update.version = 2
        assert serve_state.remove_replica(
            'svc', 1, expected_replica_record_id=stale.replica_record_id)

        assert not serve_state.add_or_update_replicas(
            'svc', [(1, stale), (2, survivor_update)],
            expected_replica_exists=True)
        assert serve_state.get_replica_info_from_id('svc', 1) is None
        survivor = serve_state.get_replica_info_from_id('svc', 2)
        assert survivor is not None
        assert survivor.version == 1

    def test_write_first_then_delete_leaves_no_row(self, _mock_serve_db):
        assert serve_state.add_or_update_replica('svc', 1, _replica(1))
        update = serve_state.get_replica_info_from_id('svc', 1)
        assert update is not None
        update.version = 2
        assert serve_state.add_or_update_replica('svc',
                                                 1,
                                                 update,
                                                 expected_replica_exists=True)
        assert serve_state.remove_replica(
            'svc', 1, expected_replica_record_id=update.replica_record_id)
        assert serve_state.get_replica_info_from_id('svc', 1) is None

    def test_delete_fence_rejects_malformed_and_inexact_batch_inputs(
            self, _mock_serve_db):
        info = _replica(1)
        assert serve_state.add_or_update_replica('svc', 1, info)

        with pytest.raises(ValueError, match='canonical UUID'):
            serve_state.remove_replica('svc',
                                       1,
                                       expected_replica_record_id='INVALID')
        with pytest.raises(ValueError, match='duplicates'):
            serve_state.remove_replicas(
                'svc', [1, 1],
                expected_service_hash='incarnation-a',
                expected_replica_record_ids={1: info.replica_record_id})
        with pytest.raises(ValueError, match='cover every replica'):
            serve_state.remove_replicas(
                'svc', [],
                expected_service_hash='incarnation-a',
                expected_replica_record_ids={1: info.replica_record_id})
        with pytest.raises(ValueError, match='canonical UUID'):
            serve_state.remove_replicas(
                'svc', [1],
                expected_service_hash='incarnation-a',
                expected_replica_record_ids={1: 'INVALID'})

        persisted = serve_state.get_replica_info_from_id('svc', 1)
        assert persisted is not None
        assert persisted.replica_record_id == info.replica_record_id

    def test_physical_replica_key_must_match_payload_before_writes(
            self, _mock_serve_db):
        with pytest.raises(ValueError, match='row key must match'):
            serve_state.add_or_update_replica('svc', 1, _replica(2))
        with pytest.raises(ValueError, match='row key must match'):
            serve_state.add_or_update_replicas('svc', [(1, _replica(1)),
                                                       (2, _replica(3))])
        assert serve_state.get_replica_infos('svc') == []

        assert _add_minimal_service('svc', service_hash='incarnation-a')
        with pytest.raises(ValueError, match='row key must match'):
            serve_state.try_add_replica_with_paid_capacity_claim(
                'svc',
                'incarnation-a',
                1,
                _replica(2),
                pool_key='paid-pool',
                priority=1,
                base_limit=1,
                max_limit=2,
                now=1.0,
                success_ttl_seconds=60.0,
                waiter_ttl_seconds=60.0,
                expected_controller_owner=None)
        with orm.Session(_mock_serve_db) as session:
            assert session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_pools_table)).first() is None
        assert serve_state.get_replica_infos('svc') == []

    def test_paid_outcomes_cannot_mutate_unfenced_extra_replica(
            self, _mock_serve_db):
        assert _add_minimal_service('svc', service_hash='incarnation-a')
        first = _replica(1)
        second = _replica(2)
        second.paid_capacity_pool_key = 'paid-pool'
        assert serve_state.add_or_update_replicas('svc', [(1, first),
                                                          (2, second)])
        update = serve_state.get_replica_info_from_id('svc', 1)
        assert update is not None
        update.version = 9
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                serve_state.paid_capacity_pools_table.insert().values(
                    pool_key='paid-pool',
                    current_limit=1,
                    successes_since_resize=0,
                    updated_at=1.0))
            session.execute(
                serve_state.paid_capacity_claims_table.insert().values(
                    service_name='svc',
                    service_hash='incarnation-a',
                    replica_id=2,
                    pool_key='paid-pool',
                    priority=1,
                    claimed_at=1.0))
            session.commit()

        with pytest.raises(ValueError, match='outcomes must identify'):
            serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
                'svc',
                'incarnation-a', [(1, update)],
                {2: paid_capacity.LaunchOutcome.SUCCESS},
                base_limit=1,
                max_limit=2,
                now=2.0,
                success_ttl_seconds=60.0,
                expected_controller_owner=None)

        persisted = serve_state.get_replica_info_from_id('svc', 1)
        assert persisted is not None
        assert persisted.version == 1
        with orm.Session(_mock_serve_db) as session:
            claim = session.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table)).first()
        assert claim is not None

    def test_v12_transition_identity_can_fence_terminal_delete(
            self, _mock_serve_db):
        info = _replica(1)
        info.created_at = 123.5
        assert serve_state.add_or_update_replica('svc', 1, info)
        with orm.Session(_mock_serve_db) as session:
            row = session.execute(
                sqlalchemy.select(
                    serve_state.replicas_table.c.replica_state).where(
                        serve_state.replicas_table.c.service_name == 'svc',
                        serve_state.replicas_table.c.replica_id ==
                        1)).scalar_one()
            row['replica_info_version'] = 12
            for field_name in replica_info_lib.V13_ADDITIVE_STORAGE_FIELDS:
                row.pop(field_name)
            session.execute(
                sqlalchemy.update(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id == 1).values(
                        replica_state=row))
            session.commit()

        transitioned = serve_state.get_replica_info_from_id('svc', 1)
        assert transitioned is not None
        assert transitioned.replica_record_id == (
            '5b71cc7f-a36e-5c16-a0c7-de59389ead0e')
        assert serve_state.remove_replica(
            'svc', 1, expected_replica_record_id=transitioned.replica_record_id)
        assert serve_state.get_replica_info_from_id('svc', 1) is None

    def test_recreated_numeric_id_rejects_stale_updates_and_deletes(
            self, _mock_serve_db):
        assert _add_minimal_service('svc', service_hash='incarnation-a')
        assert serve_state.add_or_update_replicas('svc', [(1, _replica(1)),
                                                          (2, _replica(2))])
        stale = serve_state.get_replica_info_from_id('svc', 1)
        survivor_update = serve_state.get_replica_info_from_id('svc', 2)
        assert stale is not None and survivor_update is not None
        assert serve_state.remove_replica(
            'svc', 1, expected_replica_record_id=stale.replica_record_id)

        replacement = _replica(1, version=10)
        assert replacement.replica_record_id != stale.replica_record_id
        assert serve_state.add_or_update_replica('svc', 1, replacement)
        with orm.Session(_mock_serve_db) as session:
            session.execute(
                serve_state.paid_capacity_pools_table.insert().values(
                    pool_key='replacement-pool',
                    current_limit=1,
                    successes_since_resize=0,
                    updated_at=1.0))
            session.execute(
                serve_state.paid_capacity_claims_table.insert().values(
                    service_name='svc',
                    service_hash='incarnation-a',
                    replica_id=1,
                    pool_key='replacement-pool',
                    priority=1,
                    claimed_at=1.0))
            session.commit()
        with orm.Session(_mock_serve_db) as session:
            claim_before = dict(
                session.execute(
                    sqlalchemy.select(serve_state.paid_capacity_claims_table)).
                mappings().one())
        stale.version = 99
        survivor_update.version = 99

        assert not serve_state.add_or_update_replica(
            'svc', 1, stale, expected_replica_exists=True)
        assert not serve_state.add_or_update_replicas(
            'svc', [(1, stale), (2, survivor_update)],
            expected_replica_exists=True)
        assert not serve_state.remove_replica(
            'svc',
            1,
            expected_service_hash='incarnation-a',
            expected_replica_record_id=stale.replica_record_id)
        assert not serve_state.remove_replicas(
            'svc', [1, 2],
            expected_service_hash='incarnation-a',
            expected_replica_record_ids={
                1: stale.replica_record_id,
                2: survivor_update.replica_record_id,
            })
        persisted = serve_state.get_replica_info_from_id('svc', 1)
        survivor = serve_state.get_replica_info_from_id('svc', 2)
        assert persisted is not None and survivor is not None
        assert persisted.replica_record_id == replacement.replica_record_id
        assert persisted.version == 10
        assert survivor.version == 1
        with orm.Session(_mock_serve_db) as session:
            claim_after = dict(
                session.execute(
                    sqlalchemy.select(serve_state.paid_capacity_claims_table)).
                mappings().one())
        assert claim_after == claim_before

        with orm.Session(_mock_serve_db) as session:
            row = session.execute(
                sqlalchemy.select(serve_state.replicas_table).where(
                    serve_state.replicas_table.c.service_name == 'svc',
                    serve_state.replicas_table.c.replica_id ==
                    1)).mappings().one()
        from_json = replica_managers.ReplicaInfo.from_storage_dict(
            row['replica_state'])
        from_pickle = pickle.loads(row['replica_info'])
        assert from_json.replica_record_id == replacement.replica_record_id
        assert from_pickle.replica_record_id == replacement.replica_record_id
        assert from_pickle.to_storage_dict() == from_json.to_storage_dict()

    def test_initial_single_and_batch_insert_conflicts_are_atomic(
            self, _mock_serve_db):
        initial = _replica(1)
        assert serve_state.add_or_update_replica('svc', 1, initial)

        with pytest.raises(sqlalchemy.exc.IntegrityError):
            serve_state.add_or_update_replica('svc', 1, _replica(1, version=2))
        with pytest.raises(sqlalchemy.exc.IntegrityError):
            serve_state.add_or_update_replicas('svc',
                                               [(2, _replica(2)),
                                                (1, _replica(1, version=3))])

        persisted = serve_state.get_replica_info_from_id('svc', 1)
        assert persisted is not None
        assert persisted.replica_record_id == initial.replica_record_id
        assert persisted.version == 1
        assert serve_state.get_replica_info_from_id('svc', 2) is None

    def test_recreated_numeric_id_rejects_paid_capacity_completion(
            self, _mock_serve_db):
        del _mock_serve_db
        owner = (123, '10.0.0.1')
        assert _add_minimal_service('svc',
                                    controller_pid=owner[0],
                                    controller_ip=owner[1],
                                    service_hash='incarnation-a')
        assert serve_state.add_or_update_replica(
            'svc',
            1,
            _replica(1),
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner)
        stale = serve_state.get_replica_info_from_id('svc', 1)
        assert stale is not None
        stale.version = 2
        assert serve_state.remove_replica(
            'svc',
            1,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner,
            expected_replica_record_id=(stale.replica_record_id))
        replacement = _replica(1, version=10)
        assert replacement.replica_record_id != stale.replica_record_id
        assert serve_state.add_or_update_replica(
            'svc',
            1,
            replacement,
            expected_service_hash='incarnation-a',
            expected_controller_owner=owner)

        assert not serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            'svc',
            'incarnation-a', [(1, stale)],
            {1: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=1,
            max_limit=1,
            now=1.0,
            success_ttl_seconds=60.0,
            expected_controller_owner=owner)
        persisted = serve_state.get_replica_info_from_id('svc', 1)
        assert persisted is not None
        assert persisted.replica_record_id == replacement.replica_record_id
        assert persisted.version == 10


class TestGroupedReplicaSnapshot:
    """get_replica_infos_grouped reads the global replica table once."""

    def test_groups_all_services_in_one_statement(self, _mock_serve_db):
        for service_id in range(20):
            service_name = f'svc-{service_id}'
            infos = [(replica_id,
                      _replica(replica_id,
                               cluster_name=f'{service_name}-{replica_id}'))
                     for replica_id in range(3)]
            serve_state.add_or_update_replicas(service_name, infos)

        with _count_sql_statements(_mock_serve_db) as counts:
            grouped = serve_state.get_replica_infos_grouped()

        assert counts['n'] == 1
        assert set(grouped) == {f'svc-{i}' for i in range(20)}
        assert all(len(infos) == 3 for infos in grouped.values())
        assert all(
            info.cluster_name.startswith(f'{service_name}-')
            for service_name, infos in grouped.items()
            for info in infos)

    def test_empty_table_returns_empty_mapping(self, _mock_serve_db):
        assert not serve_state.get_replica_infos_grouped()


class TestReplicaClusterNameSnapshot:
    """Exact owner discovery reads only the scalar cluster-name column."""

    def test_returns_all_exact_cluster_names_in_one_statement(
            self, _mock_serve_db):
        serve_state.add_or_update_replicas(
            'svc-a', [(1, _replica(1, cluster_name='svc-a-r1')),
                      (2, _replica(2, cluster_name='svc-a-r2'))])
        serve_state.add_or_update_replica('svc-b', 1,
                                          _replica(1, cluster_name='svc-b-r1'))

        with _count_sql_statements(_mock_serve_db) as counts:
            cluster_names = serve_state.get_replica_cluster_names()

        assert counts['n'] == 1
        assert cluster_names == {'svc-a-r1', 'svc-a-r2', 'svc-b-r1'}

    def test_empty_table_returns_empty_set(self, _mock_serve_db):
        assert not serve_state.get_replica_cluster_names()
