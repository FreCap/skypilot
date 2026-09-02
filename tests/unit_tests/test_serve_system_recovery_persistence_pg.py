"""Real-PostgreSQL tests for Serve system-recovery subdocuments."""
# pylint: disable=protected-access,redefined-outer-name,unused-import
# pylint: disable=unexpected-keyword-arg

import concurrent.futures
import contextlib
import copy
import dataclasses
import datetime
import pickle
import threading
from typing import Any
from unittest import mock
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_serve_resource_action_state_pg import postgres_engine

from sky import clouds
from sky import exceptions
from sky.serve import constants
from sky.serve import paid_capacity
from sky.serve import replica_info
from sky.serve import replica_managers
from sky.serve import route_projection
from sky.serve import route_projection_schema
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import service_spec
from sky.serve import spot_placer
from sky.serve import system_oom_recovery
from sky.serve import system_recovery_persistence
from sky.serve import system_recovery_state as recovery_state
from sky.skylet import job_lib
from sky.utils import common_utils
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

_SERVICE_NAME = 'svc'
_SERVICE_HASH = 'service-hash'
_OWNER = (123, '10.0.0.1')
_LIFECYCLE_EPOCH = 4
_WORKSPACE = 'default'
_CONTROLLER_INCARNATION = uuid.UUID('33333333-3333-4333-8333-333333333333')
_CONTROLLER_OWNER_EPOCH = 6


@pytest.fixture
def recovery_database(postgres_engine, monkeypatch):  # noqa: F811
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    serve_state_schema.Base.metadata.create_all(postgres_engine)
    config = migration_utils.get_alembic_config(postgres_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.stamp(config, '042')
    # create_all() supplies the current table shape but not the Serve035 data
    # migration that creates the one protocol row.  Seed that historical
    # invariant before exercising the forward-only Serve045 migration.
    alembic_command.upgrade(config, '044')
    with postgres_engine.begin() as connection:
        connection.execute(
            serve_state.reserved_fill_protocol_state_table.insert().values(
                id=1))
    alembic_command.upgrade(config, '045')
    monkeypatch.setattr(serve_state._db_manager, '_engine', postgres_engine)
    with postgres_engine.begin() as connection:
        connection.execute(
            serve_state.service_lifecycle_fences_table.insert().values(
                name=_SERVICE_NAME, epoch=_LIFECYCLE_EPOCH))
        connection.execute(serve_state.services_table.insert().values(
            name=_SERVICE_NAME,
            workspace=_WORKSPACE,
            hash=_SERVICE_HASH,
            status=serve_state.ServiceStatus.READY.value,
            controller_pid=_OWNER[0],
            controller_ip=_OWNER[1],
            controller_incarnation=_CONTROLLER_INCARNATION,
            controller_owner_epoch=_CONTROLLER_OWNER_EPOCH,
            lifecycle_epoch=_LIFECYCLE_EPOCH,
            pool=0,
            resource_action_mode='legacy'))
    assert serve_state.add_or_update_replica(
        _SERVICE_NAME,
        7,
        _replica(7),
        expected_service_hash=_SERVICE_HASH,
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
        expected_controller_owner=_OWNER)
    return postgres_engine


def _replica(replica_id: int) -> replica_info.ReplicaInfo:
    return replica_info.ReplicaInfo(replica_id=replica_id,
                                    cluster_name=f'svc-{replica_id}',
                                    replica_port='8080',
                                    is_spot=False,
                                    location=None,
                                    version=3,
                                    resources_override=None)


def _paid_pool_key() -> str:
    """Return one canonical account-scoped exact paid provider pool."""
    location = spot_placer.Location(cloud=clouds.AWS(),
                                    region='us-east-1',
                                    zone='us-east-1a',
                                    accelerators={'L4': 1},
                                    use_spot=True,
                                    instance_type='g6.xlarge')
    return paid_capacity.pool_key(location,
                                  workspace=_WORKSPACE,
                                  num_nodes=1,
                                  aws_account_id='123456789012')


def _intent(
    replica_id: int,
    *,
    nonce: str = 'b' * 64,
) -> recovery_state.SystemRecoveryLaunchIntent:
    digest = 'a' * 64
    return recovery_state.SystemRecoveryLaunchIntent(
        version=1,
        controller_contract_version=2,
        recovery_authorization_version=3,
        recovery_authorization_profile_id='boltz-l4-v3',
        recovery_authorization_sha256=digest,
        runtime_profile_version=2,
        expected_runtime_capability=recovery_state.SYSTEM_RECOVERY_CAPABILITY,
        service_hash=_SERVICE_HASH,
        replica_id=replica_id,
        launch_generation=replica_id,
        launch_nonce=nonce,
        workspace=_WORKSPACE,
        resource_envelope_sha256=digest,
        task_sha256=digest,
        runtime_image_digest=f'sha256:{digest}',
        owned_container_spec_sha256=digest,
        execution_envelope_sha256=digest)


def _armed_capable(replica_id: int) -> replica_info.ReplicaInfo:
    info = _replica(replica_id)
    info.system_recovery_launch_intent = _intent(replica_id)
    info.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.CAPABLE)
    info.system_recovery = recovery_state.ReplicaSystemRecovery(
        state=recovery_state.ControllerRecoveryState.ARMED,
        job_id=9,
        capability=recovery_state.SYSTEM_RECOVERY_CAPABILITY,
        original_attempt_id='11111111-1111-4111-8111-111111111111',
        replacement_attempt_id=None,
        node_boot_id='boot-id',
        remote_phase=recovery_state.RemoteRecoveryPhase.ARMED,
        occurrence_count=0,
        armed_at=10.0)
    info.launch_request_id = f'launch-request-{replica_id}'
    info.service_job_id = 9
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    info.status_property.first_ready_time = 1.0
    info.status_property.service_ready_now = False
    return info


def _armed_evidence() -> tuple[job_lib.JobStatus, job_lib.JobSystemRecoveryInfo,
                               job_lib.JobSystemRecoveryDetailStatus]:
    return (
        job_lib.JobStatus.RUNNING,
        job_lib.JobSystemRecoveryInfo(
            capability=recovery_state.SYSTEM_RECOVERY_CAPABILITY,
            phase=job_lib.JobSystemRecoveryPhase.ARMED,
            original_attempt_id='11111111-1111-4111-8111-111111111111',
            replacement_attempt_id=None,
            task_index=0,
            node_boot_id='boot-id',
            occurrence_count=0,
            armed_at=10.0,
            updated_at=30.0,
            event_id=None,
            reason=None,
            occurred_at=None,
            deadline_at=None),
        job_lib.JobSystemRecoveryDetailStatus.PRESENT,
    )


def _fence() -> dict[str, Any]:
    return {
        'expected_service_hash': _SERVICE_HASH,
        'expected_lifecycle_epoch': _LIFECYCLE_EPOCH,
        'expected_controller_owner': _OWNER,
    }


def _observer_fence(
    *,
    controller_incarnation: uuid.UUID = _CONTROLLER_INCARNATION,
    controller_owner_epoch: int = _CONTROLLER_OWNER_EPOCH,
) -> system_recovery_persistence.ReplicaObserverOwnerFence:
    return system_recovery_persistence.ReplicaObserverOwnerFence(
        service_name=_SERVICE_NAME,
        service_hash=_SERVICE_HASH,
        service_lifecycle_epoch=_LIFECYCLE_EPOCH,
        controller_pid=_OWNER[0],
        controller_ip=_OWNER[1],
        controller_incarnation=controller_incarnation,
        controller_owner_epoch=controller_owner_epoch)


def _batch_fence() -> dict[str, Any]:
    return {'owner_fence': _observer_fence()}


def _probe_manager() -> replica_managers.SkyPilotReplicaManager:
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = _SERVICE_NAME
    manager._service_hash = _SERVICE_HASH
    manager._controller_owner = _OWNER
    manager._ordinary_launch_binding_authority = None
    manager._replica_observer_owner_fence = (
        system_recovery_persistence.ReplicaObserverOwnerFence(
            service_name=_SERVICE_NAME,
            service_hash=_SERVICE_HASH,
            controller_pid=_OWNER[0],
            controller_ip=_OWNER[1],
            service_lifecycle_epoch=_LIFECYCLE_EPOCH,
            controller_incarnation=_CONTROLLER_INCARNATION,
            controller_owner_epoch=_CONTROLLER_OWNER_EPOCH))
    manager._system_recovery_status_initialized = set()
    manager._candidate_release_monotonic_deadlines = {}
    manager._provider_identity_uncertain_ids = set()
    manager._update_recovery_required = False
    manager._uptime = 1.0
    manager.lock = threading.RLock()
    manager._get_initial_delay_seconds = lambda _version: 1200
    manager._consecutive_failure_threshold_timeout = lambda: 1200
    return manager


def _reduce_ready_observations(
    manager: replica_managers.SkyPilotReplicaManager,
    snapshots: dict[int, replica_info.ReplicaInfo],
) -> list[replica_info.ReplicaInfo]:
    infos = [snapshots[replica_id] for replica_id in sorted(snapshots)]
    results = [
        replica_managers._ReadinessProbeResult(info=info,
                                               succeeded=True,
                                               observed_at=123.0,
                                               request_started_monotonic=122.0)
        for info in infos
    ]
    return manager._reduce_probe_results_batch(
        infos,
        results,
        possibly_preempted_ids=set(),
        blocked_identity_ids=set(),
        provider_identity_errors={},
        provider_phase_deferred_replica_ids=set(),
        candidate_status_evidence={},
        candidate_cycle_evidence={},
        ordered_route_evidence={},
        route_requires_next_probe_ids=set(),
        probe_urls={},
        resolved_route_material={},
        deferred_route_ids=set(),
        accepted_probe_fingerprints={})


def _route_identity() -> route_projection.RoutePublisherIdentity:
    return route_projection.RoutePublisherIdentity(
        service_name=_SERVICE_NAME,
        service_hash=_SERVICE_HASH,
        service_lifecycle_epoch=_LIFECYCLE_EPOCH,
        controller_incarnation=_CONTROLLER_INCARNATION,
        controller_owner_epoch=_CONTROLLER_OWNER_EPOCH,
        controller_pid=_OWNER[0],
        controller_ip=_OWNER[1])


def _seed_ready_route(engine, info: replica_info.ReplicaInfo) -> None:
    route_projection_schema.metadata.create_all(engine)
    repository = route_projection.RouteProjectionRepository(engine)
    material = route_projection.RouteLeaseMaterial(
        route=route_projection.ResolvedRouteMaterial(
            f'http://10.0.0.{info.replica_id}:8000', 'L4', 1),
        readiness_path='/health',
        probe_timeout_seconds=15,
        post_data=None,
        headers=None,
        async_occupancy=True,
        uses_logical_replicas=False,
        is_zero_cost=False,
        planned_capacity=1,
        route_allowed=True,
        requires_route_marker=False)
    repository.upsert_replica_material(_route_identity(), info, material)
    target = repository.list_probe_targets(_route_identity())[0]
    assert repository.record_probe_result(target, True, ttl_seconds=60).accepted


def _route_lease(engine, replica_id: int) -> dict[str, Any]:
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                route_projection_schema.serve_route_replica_leases_table).where(
                    route_projection_schema.serve_route_replica_leases_table.c.
                    service_name == _SERVICE_NAME,
                    route_projection_schema.serve_route_replica_leases_table.c.
                    replica_id == replica_id)).mappings().one()
    return dict(row)


@pytest.mark.parametrize(('field_name', 'invalid_value'), [
    ('user_app_failed', 1),
    ('service_ready_now', 'yes'),
    ('sky_down_status', 'RUNNING'),
    ('drain_cap_seconds', True),
    ('drain_cap_seconds', -1),
    ('first_ready_time', -2.0),
    ('first_ready_time', float('nan')),
    ('drain_started_at', float('inf')),
    ('first_not_ready_time', -0.1),
    ('first_consecutive_failure_time', False),
])
def test_probe_patch_rejects_noncanonical_typed_state(field_name,
                                                      invalid_value) -> None:
    patch = serve_state.ReplicaObservationPatch.from_replica_info(_replica(7))

    with pytest.raises(ValueError):
        dataclasses.replace(patch, **{field_name: invalid_value})


def test_probe_patch_projection_does_not_truthiness_coerce_source_state(
) -> None:
    info = _replica(7)
    info.status_property.service_ready_now = 1

    with pytest.raises(ValueError, match='service_ready_now must be a boolean'):
        serve_state.ReplicaObservationPatch.from_replica_info(info)


def _candidate_reduction(
        info: replica_info.ReplicaInfo) -> replica_info.ReplicaInfo:
    desired = copy.deepcopy(info)
    desired.system_recovery_launch_intent = _intent(desired.replica_id)
    desired.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.CANDIDATE)
    return desired


def _recovery_write(
    desired: replica_info.ReplicaInfo,) -> serve_state.ReplicaObservationWrite:
    return serve_state.ReplicaObservationWrite(
        replica_id=desired.replica_id,
        replica_record_id=desired.replica_record_id,
        service_version=desired.version,
        expected_revision=desired.system_recovery_revision,
        desired_info=desired)


def _probe_write(
    opening: replica_info.ReplicaInfo,
    desired: replica_info.ReplicaInfo,
) -> serve_state.ReplicaObservationWrite:
    patch_type = serve_state.ReplicaObservationPatch
    return serve_state.ReplicaObservationWrite(
        replica_id=opening.replica_id,
        replica_record_id=opening.replica_record_id,
        service_version=opening.version,
        expected_revision=opening.system_recovery_revision,
        desired_info=desired,
        expected_observation_state=patch_type.from_replica_info(opening),
        desired_observation_state=patch_type.from_replica_info(desired))


def _make_candidate(
    replica_id: int,
    *,
    intent: recovery_state.SystemRecoveryLaunchIntent | None = None,
) -> replica_info.ReplicaInfo:
    current = serve_state.get_replica_info_from_id(_SERVICE_NAME, replica_id)
    assert current is not None
    current.system_recovery_launch_intent = intent or _intent(replica_id)
    current.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.CANDIDATE)
    return serve_state.create_replica_system_recovery_candidate(
        _SERVICE_NAME,
        replica_id,
        current,
        expected_revision=current.system_recovery_revision,
        owner_fence=_observer_fence())


def _unbound_context(
    intent: recovery_state.SystemRecoveryLaunchIntent,) -> dict[str, Any]:
    return system_oom_recovery.create_unbound_launch_context(
        intent,
        service_name=_SERVICE_NAME,
        service_version=3,
        service_lifecycle_epoch=_LIFECYCLE_EPOCH,
        controller_pid=_OWNER[0],
        controller_ip=_OWNER[1],
        controller_incarnation=_CONTROLLER_INCARNATION,
        controller_owner_epoch=_CONTROLLER_OWNER_EPOCH)


def _raw_replica_row(engine, replica_id: int) -> dict[str, Any]:
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == _SERVICE_NAME,
                serve_state.replicas_table.c.replica_id ==
                replica_id)).mappings().one()
    return dict(row)


def _seed_raw_replica_state(engine, info: replica_info.ReplicaInfo) -> None:
    """Install an otherwise valid retained lifecycle for a PG fixture."""
    values = serve_state._replica_row_values(_SERVICE_NAME, info.replica_id,
                                             info)
    with engine.begin() as connection:
        result = connection.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == _SERVICE_NAME,
                serve_state.replicas_table.c.replica_id ==
                info.replica_id).values({
                    key: value
                    for key, value in values.items()
                    if key not in ('service_name', 'replica_id')
                }))
    assert result.rowcount == 1


def _assert_json_only_replica_state(engine, replica_id: int) -> None:
    row = _raw_replica_row(engine, replica_id)
    json_info = replica_info.ReplicaInfo.from_storage_dict(row['replica_state'])
    assert row['replica_info'] is None
    assert json_info.to_storage_dict() == row['replica_state']


def test_authorization_snapshot_pairs_quarantine_aware_version_and_incarnation(
        recovery_database) -> None:
    engine = recovery_database
    other_service_name = 'other-svc'
    other_service_hash = 'other-service-hash'
    other_lifecycle_epoch = 1
    stale_pointer_spec = {'identity': 'stale-current-pointer'}
    newer_spec = {'identity': 'newest-applicable'}
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.insert(), [
            {
                'service_name': _SERVICE_NAME,
                'version': 3,
                'spec': pickle.dumps(stale_pointer_spec),
                'yaml_content': 'run: stale-pointer\n',
            },
            {
                'service_name': _SERVICE_NAME,
                'version': 4,
                'spec': pickle.dumps(newer_spec),
                'yaml_content': 'run: newer\n',
            },
        ])
        connection.execute(serve_state.services_table.update().where(
            serve_state.services_table.c.name == _SERVICE_NAME).values(
                current_version=3,
                status=serve_state.ServiceStatus.NO_REPLICA.value))
        connection.execute(serve_state.replicas_table.delete().where(
            serve_state.replicas_table.c.service_name == _SERVICE_NAME))
        connection.execute(
            serve_state.service_lifecycle_fences_table.insert().values(
                name=other_service_name, epoch=other_lifecycle_epoch))
        connection.execute(serve_state.services_table.insert().values(
            name=other_service_name,
            workspace=_WORKSPACE,
            hash=other_service_hash,
            status=serve_state.ServiceStatus.READY.value,
            controller_pid=_OWNER[0],
            controller_ip=_OWNER[1],
            lifecycle_epoch=other_lifecycle_epoch,
            pool=0,
            resource_action_mode='legacy'))
    assert serve_state.add_or_update_replica(
        other_service_name,
        8,
        _replica(8),
        expected_service_hash=other_service_hash,
        expected_lifecycle_epoch=other_lifecycle_epoch,
        expected_controller_owner=_OWNER)

    with _capture_sql(engine) as statements:
        snapshot = serve_state.get_system_recovery_authorization_snapshot(
            _SERVICE_NAME)

    selects = [
        statement for statement in statements
        if statement.lstrip().startswith('select ')
    ]
    assert len(selects) == 1
    assert statements == selects
    assert snapshot == {
        'service_name': _SERVICE_NAME,
        'service_hash': _SERVICE_HASH,
        'workspace': _WORKSPACE,
        'version': 4,
        'status': serve_state.ServiceStatus.NO_REPLICA,
        'pool': False,
        'resource_action_mode': 'legacy',
        'spec': newer_spec,
        'yaml_content': 'run: newer\n',
        'quarantined_at': None,
        'replica_count': 0,
    }


def test_authorization_snapshot_counts_same_service_stale_version_replica(
        recovery_database) -> None:
    engine = recovery_database
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.insert().values(
            service_name=_SERVICE_NAME,
            version=3,
            spec=pickle.dumps({'identity': 'elected'}),
            yaml_content='run: elected\n'))
        connection.execute(serve_state.services_table.update().where(
            serve_state.services_table.c.name == _SERVICE_NAME).values(
                current_version=3,
                status=serve_state.ServiceStatus.NO_REPLICA.value))
        # Replica rows are incarnation-wide, not elected-version-scoped.  A
        # stale version must still make zero-replica bootstrap ineligible.
        connection.execute(serve_state.replicas_table.update().where(
            serve_state.replicas_table.c.service_name == _SERVICE_NAME).values(
                version=2))

    snapshot = serve_state.get_system_recovery_authorization_snapshot(
        _SERVICE_NAME)

    assert snapshot is not None
    assert snapshot['version'] == 3
    assert snapshot['replica_count'] == 1


@contextlib.contextmanager
def _capture_sql(engine):
    statements: list[str] = []

    def _record_statement(_connection, _cursor, statement, _parameters,
                          _context, _executemany):
        statements.append(statement.lower())

    # Replica launch-authority mutations deliberately run on the dedicated
    # PostgreSQL lock engine so the advisory lock and mutation share one
    # transaction without consuming the bounded Serve pool.  Observe both
    # engines; otherwise this lock-order assertion sees an empty trace even
    # though the production transaction took every required row lock.
    engines = (engine, db_utils.get_postgres_lock_engine(engine))
    for observed_engine in engines:
        sqlalchemy.event.listen(observed_engine, 'before_cursor_execute',
                                _record_statement)
    try:
        yield statements
    finally:
        for observed_engine in engines:
            sqlalchemy.event.remove(observed_engine, 'before_cursor_execute',
                                    _record_statement)


def _assert_lifecycle_service_replica_lock_order(statements: list[str]) -> None:
    locked = [
        statement for statement in statements if 'for update' in statement
    ]
    fence_lock = next(index for index, statement in enumerate(locked)
                      if 'service_lifecycle_fences' in statement)
    service_lock = next(index for index, statement in enumerate(locked)
                        if 'from services' in statement)
    replica_lock = next(index for index, statement in enumerate(locked)
                        if 'from replicas' in statement)
    assert fence_lock < service_lock < replica_lock


def _assert_replica_update_only(statements: list[str]) -> None:
    mutations = [statement.lstrip() for statement in statements]
    assert any(
        statement.startswith('update replicas ') for statement in mutations)
    assert not any(
        statement.startswith('insert into replicas ')
        for statement in mutations)


def test_observation_commit_rejects_257_rows_before_sql(
        recovery_database) -> None:
    engine = recovery_database
    limit = system_recovery_persistence.REPLICA_OBSERVATION_COMMIT_MAX_ROWS
    replica_ids = list(range(7, 7 + limit + 1))
    assert serve_state.add_or_update_replicas(
        _SERVICE_NAME,
        [(replica_id, _replica(replica_id)) for replica_id in replica_ids[1:]],
        **_fence())
    snapshots = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)
    desired_by_id = copy.deepcopy(snapshots)
    for desired in desired_by_id.values():
        desired.status_property.service_ready_now = True
        desired.status_property.first_ready_time = 123.0
    writes = [
        _probe_write(snapshots[replica_id], desired_by_id[replica_id])
        for replica_id in replica_ids
    ]

    with _capture_sql(engine) as statements:
        with pytest.raises(ValueError, match='cardinality limit'):
            serve_state.commit_replica_observations_batch(
                _SERVICE_NAME, writes, **_batch_fence())

    assert not any('for update' in statement for statement in statements)
    assert not any(statement.lstrip().startswith('update replicas ')
                   for statement in statements)

    persisted = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)
    assert len(persisted) == limit + 1
    assert all(not info.status_property.service_ready_now
               for info in persisted.values())


def test_manager_windows_800_observations_and_isolates_probe_drift(
        recovery_database) -> None:
    engine = recovery_database
    replica_ids = list(range(7, 807))
    assert serve_state.add_or_update_replicas(
        _SERVICE_NAME,
        [(replica_id, _replica(replica_id)) for replica_id in replica_ids[1:]],
        **_fence())
    snapshots = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)
    # A concurrent readiness owner changes one row without touching recovery
    # revision. The explicit expected probe patch must still reject that row.
    drifted = copy.deepcopy(snapshots[7])
    drifted.first_not_ready_time = 99.0
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             drifted,
                                             expected_replica_exists=True,
                                             **_fence())

    manager = _probe_manager()
    original_commit = manager._commit_probe_row_plans
    with _capture_sql(engine) as statements, mock.patch.object(
            manager, '_commit_probe_row_plans',
            wraps=original_commit) as commit:
        reduced = _reduce_ready_observations(manager, snapshots)

    limit = system_recovery_persistence.REPLICA_OBSERVATION_COMMIT_MAX_ROWS
    assert [len(call.args[0]) for call in commit.call_args_list
           ] == [limit, limit, limit, 800 - 3 * limit]
    assert len(reduced) == 800
    assert not next(info for info in reduced
                    if info.replica_id == 7).status_property.service_ready_now
    assert all(info.status_property.service_ready_now
               for info in reduced
               if info.replica_id != 7)
    locked = [
        statement for statement in statements if 'for update' in statement
    ]
    assert sum(
        'service_lifecycle_fences' in statement for statement in locked) == 4
    assert sum('from services' in statement for statement in locked) == 4
    assert sum('from replicas' in statement for statement in locked) == 4
    updates = [
        statement for statement in statements
        if statement.lstrip().startswith('update replicas ')
    ]
    assert len(updates) == 4

    persisted = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)
    assert persisted[7].first_not_ready_time == 99.0
    assert not persisted[7].status_property.service_ready_now
    assert all(persisted[replica_id].status_property.service_ready_now
               for replica_id in replica_ids[1:])
    assert all(persisted[replica_id].system_recovery_revision == 0
               for replica_id in replica_ids)


def test_manager_windows_800_stable_rows_without_replica_updates(
        recovery_database) -> None:
    engine = recovery_database
    replica_ids = list(range(7, 807))
    ready_infos = {}
    for replica_id in replica_ids:
        info = (_replica(replica_id) if replica_id != 7 else
                serve_state.get_replica_info_from_id(_SERVICE_NAME, 7))
        assert info is not None
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.service_ready_now = True
        info.status_property.first_ready_time = 1.0
        ready_infos[replica_id] = info
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             ready_infos[7],
                                             expected_replica_exists=True,
                                             **_fence())
    assert serve_state.add_or_update_replicas(_SERVICE_NAME, [
        (replica_id, ready_infos[replica_id]) for replica_id in replica_ids[1:]
    ], **_fence())
    snapshots = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)

    manager = _probe_manager()
    original_commit = manager._commit_probe_row_plans
    with _capture_sql(engine) as statements, mock.patch.object(
            manager, '_commit_probe_row_plans',
            wraps=original_commit) as commit:
        reduced = _reduce_ready_observations(manager, snapshots)

    limit = system_recovery_persistence.REPLICA_OBSERVATION_COMMIT_MAX_ROWS
    assert [len(call.args[0]) for call in commit.call_args_list
           ] == [limit, limit, limit, 800 - 3 * limit]
    assert len(reduced) == 800
    assert all(info.status_property.service_ready_now for info in reduced)
    assert sum('from replicas' in statement and 'for update' in statement
               for statement in statements) == 4
    assert not any(statement.lstrip().startswith('update replicas ')
                   for statement in statements)


def test_first_real_pg_window_finishes_teardown_before_second_window_fails(
        recovery_database) -> None:
    limit = system_recovery_persistence.REPLICA_OBSERVATION_COMMIT_MAX_ROWS
    replica_ids = list(range(7, 7 + limit + 1))
    ready_infos = {}
    for replica_id in replica_ids:
        info = (_replica(replica_id) if replica_id != 7 else
                serve_state.get_replica_info_from_id(_SERVICE_NAME, 7))
        assert info is not None
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.service_ready_now = True
        info.status_property.first_ready_time = 1.0
        ready_infos[replica_id] = info
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             ready_infos[7],
                                             expected_replica_exists=True,
                                             **_fence())
    assert serve_state.add_or_update_replicas(_SERVICE_NAME, [
        (replica_id, ready_infos[replica_id]) for replica_id in replica_ids[1:]
    ], **_fence())
    snapshots = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)
    infos = [snapshots[replica_id] for replica_id in replica_ids]
    results = [
        replica_managers._ReadinessProbeResult(info=info,
                                               succeeded=False,
                                               observed_at=123.0,
                                               request_started_monotonic=122.0)
        for info in infos
    ]
    manager = _probe_manager()
    manager._consecutive_failure_threshold_timeout = lambda: 0
    manager._system_recovery_route_registry = (
        replica_managers.system_recovery_route_lease.ManagerRouteLeaseRegistry(
            clock=lambda: 150.0))
    wake = mock.Mock()
    manager._launch_completion_state = mock.Mock(return_value=(mock.Mock(),
                                                               wake))
    original_commit = manager._commit_probe_row_plans
    call_sizes = []

    def _fail_second_window(plans):
        call_sizes.append(len(plans))
        if len(call_sizes) == 2:
            raise RuntimeError('injected second-window failure')
        return original_commit(plans)

    manager._commit_probe_row_plans = _fail_second_window
    with pytest.raises(RuntimeError, match='second-window failure'):
        manager._reduce_probe_results_batch(
            infos,
            results,
            possibly_preempted_ids=set(),
            blocked_identity_ids=set(),
            provider_identity_errors={},
            provider_phase_deferred_replica_ids=set(),
            candidate_status_evidence={},
            candidate_cycle_evidence={},
            ordered_route_evidence={},
            route_requires_next_probe_ids=set(),
            probe_urls={},
            resolved_route_material={},
            deferred_route_ids=set(),
            accepted_probe_fingerprints={})

    assert call_sizes == [limit, 1]
    wake.set.assert_called_once_with()
    persisted = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)
    assert all(persisted[replica_id].status_property.sky_down_status ==
               common_utils.ProcessStatus.SCHEDULED
               for replica_id in replica_ids[:limit])
    assert persisted[replica_ids[-1]].status_property.sky_down_status is None


def test_exact_status_first_pg_window_finishes_before_second_window_failure(
        recovery_database) -> None:
    limit = system_recovery_persistence.REPLICA_OBSERVATION_COMMIT_MAX_ROWS
    replica_ids = list(range(7, 7 + limit + 1))
    assert serve_state.add_or_update_replicas(
        _SERVICE_NAME,
        [(replica_id, _replica(replica_id)) for replica_id in replica_ids[1:]],
        **_fence())
    snapshots = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)
    manager = _probe_manager()
    manager._is_pool = False
    manager._ownership_lost = threading.Event()
    manager._manager_daemon_stop = threading.Event()
    manager._system_recovery_route_registry = (
        replica_managers.system_recovery_route_lease.ManagerRouteLeaseRegistry(
            clock=lambda: 150.0))
    manager._apply_confirmed_preemption = mock.Mock()
    manager._persist_spot_placement_state_if_dirty = mock.Mock()
    wake = mock.Mock()
    manager._launch_completion_state = mock.Mock(return_value=(mock.Mock(),
                                                               wake))
    original_commit = manager._commit_probe_row_plans
    call_windows = []

    def _fail_second_window(plans):
        call_windows.append(
            tuple(plan.opening_info.replica_id for plan in plans))
        if len(call_windows) == 2:
            raise RuntimeError('injected exact-status second-window failure')
        return original_commit(plans)

    manager._commit_probe_row_plans = _fail_second_window
    fetch_results = []
    # Provider/future completion order must not determine window membership.
    for replica_id in reversed(replica_ids):
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        future.set_exception(
            exceptions.CommandError(255, 'status', 'unreachable', None))
        fetch_results.append((snapshots[replica_id], None, future))

    with pytest.raises(RuntimeError, match='exact-status second-window'):
        manager._handle_job_status_results(
            fetch_results,
            provider_error_phase_mode=(replica_managers.provider_phase.
                                       ProviderPhaseMode.AMBIENT_LEGACY))

    assert call_windows == [tuple(replica_ids[:limit]), (replica_ids[-1],)]
    assert manager._apply_confirmed_preemption.call_count == limit
    manager._persist_spot_placement_state_if_dirty.assert_called_once_with()
    wake.set.assert_called_once_with()
    persisted = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)
    assert all(persisted[replica_id].status_property.sky_down_status ==
               common_utils.ProcessStatus.SCHEDULED
               for replica_id in replica_ids[:limit])
    assert persisted[replica_ids[-1]].status_property.sky_down_status is None


@pytest.mark.parametrize('owner_fence', [None, object()])
def test_probe_batch_requires_typed_exact_controller_fence(
        recovery_database, owner_fence) -> None:
    current = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert current is not None

    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='owner identity'):
        serve_state.commit_replica_observations_batch(
            _SERVICE_NAME, [_probe_write(current, current)],
            owner_fence=owner_fence)


@pytest.mark.parametrize(('replacement_incarnation', 'replacement_epoch'), [
    (uuid.UUID('44444444-4444-4444-8444-444444444444'),
     _CONTROLLER_OWNER_EPOCH),
    (_CONTROLLER_INCARNATION, _CONTROLLER_OWNER_EPOCH + 1),
])
def test_probe_batch_rejects_replaced_exact_owner_with_same_pid_ip(
        recovery_database, replacement_incarnation, replacement_epoch) -> None:
    engine = recovery_database
    opening = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert opening is not None
    desired = copy.deepcopy(opening)
    desired.status_property.service_ready_now = True
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == _SERVICE_NAME).values(
                    controller_incarnation=replacement_incarnation,
                    controller_owner_epoch=replacement_epoch))

    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='owner no longer matches'):
        serve_state.commit_replica_observations_batch(
            _SERVICE_NAME, [_probe_write(opening, desired)], **_batch_fence())

    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted is not None
    assert not persisted.status_property.service_ready_now


@pytest.mark.parametrize(('field_name', 'drifted_value'), [
    ('user_app_failed', True),
    ('service_ready_now', True),
    ('first_ready_time', 123.0),
    ('sky_down_status', common_utils.ProcessStatus.SCHEDULED),
    ('is_scale_down', True),
    ('preempted', True),
    ('purged', True),
    ('drain_cap_seconds', 17),
    ('drain_started_at', 123.0),
    ('wait_for_idle_before_termination', True),
    ('first_not_ready_time', 123.0),
    ('first_consecutive_failure_time', 123.0),
])
def test_probe_batch_detects_every_observation_field_drift_without_touching_other_owners(
        recovery_database, field_name: str, drifted_value: object) -> None:
    candidate = _make_candidate(7)
    candidate.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             candidate,
                                             **_fence(),
                                             expected_replica_exists=True)
    opening = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert opening is not None
    concurrent = copy.deepcopy(opening)
    if field_name in ('first_not_ready_time', 'first_consecutive_failure_time'):
        setattr(concurrent, field_name, drifted_value)
    else:
        setattr(concurrent.status_property, field_name, drifted_value)
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             concurrent,
                                             **_fence(),
                                             expected_replica_exists=True)

    desired = copy.deepcopy(opening)
    desired.status_property.service_ready_now = True
    result = serve_state.commit_replica_observations_batch(
        _SERVICE_NAME, [_probe_write(opening, desired)], **_batch_fence())

    assert result.updated_infos == ()
    assert result.unchanged_infos == ()
    assert result.stale_replica_ids == (7,)
    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted is not None
    assert (system_recovery_persistence.ReplicaObservationPatch.
            from_replica_info(persisted) == system_recovery_persistence.
            ReplicaObservationPatch.from_replica_info(concurrent))
    assert (system_recovery_persistence.ReplicaSystemRecoveryPatch.
            from_replica_info(persisted) == system_recovery_persistence.
            ReplicaSystemRecoveryPatch.from_replica_info(opening))
    assert persisted.status_property.sky_launch_status == (
        common_utils.ProcessStatus.SUCCEEDED)
    assert persisted.cluster_name == opening.cluster_name


@pytest.mark.parametrize(('pool', 'action_mode'), [
    (1, 'legacy'),
    (0, 'shadow'),
    (0, 'authoritative'),
    (1, 'authoritative'),
])
def test_probe_only_batch_supports_pool_and_all_resource_action_modes(
        recovery_database, pool, action_mode) -> None:
    engine = recovery_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == _SERVICE_NAME).values(
                    pool=pool, resource_action_mode=action_mode))
    opening = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert opening is not None
    desired = copy.deepcopy(opening)
    desired.status_property.service_ready_now = True

    result = serve_state.commit_replica_observations_batch(
        _SERVICE_NAME, [_probe_write(opening, desired)], **_batch_fence())

    assert result.stale_replica_ids == ()
    assert len(result.updated_infos) == 1
    assert result.updated_infos[0].status_property.service_ready_now


def test_pool_probe_commit_uses_policy_independent_observer_fence(
        recovery_database) -> None:
    engine = recovery_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == _SERVICE_NAME).values(
                    pool=1, resource_action_mode='authoritative'))
    opening = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert opening is not None
    desired = copy.deepcopy(opening)
    desired.status_property.service_ready_now = True
    manager = _probe_manager()
    assert manager._ordinary_launch_binding_authority is None

    accepted, stale = manager._commit_probe_row_plans([
        replica_managers._ProbeRowWritePlan(
            opening_info=opening,
            desired_info=desired,
            effects=replica_managers._ProbeRowPostcommitEffects())
    ])

    assert stale == set()
    assert accepted[7].status_property.service_ready_now


@pytest.mark.parametrize(('pool', 'action_mode'), [
    (1, 'legacy'),
    (0, 'shadow'),
    (0, 'authoritative'),
])
def test_recovery_batch_does_not_broaden_pool_or_action_mode_authority(
        recovery_database, pool, action_mode) -> None:
    engine = recovery_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == _SERVICE_NAME).values(
                    pool=pool, resource_action_mode=action_mode))
    opening = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert opening is not None

    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='requires a legacy non-pool'):
        serve_state.commit_replica_observations_batch(
            _SERVICE_NAME, [_recovery_write(_candidate_reduction(opening))],
            **_batch_fence())

    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted is not None
    assert (persisted.system_recovery_disposition ==
            recovery_state.SystemRecoveryDisposition.ORDINARY)


def test_probe_batch_revokes_active_route_in_same_commit(
        recovery_database) -> None:
    engine = recovery_database
    ready = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert ready is not None
    ready.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    ready.status_property.service_ready_now = True
    ready.status_property.first_ready_time = 1.0
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             ready,
                                             expected_replica_exists=True,
                                             **_fence())
    _seed_ready_route(engine, ready)
    desired = copy.deepcopy(ready)
    desired.status_property.service_ready_now = False

    result = serve_state.commit_replica_observations_batch(
        _SERVICE_NAME, [_probe_write(ready, desired)], **_batch_fence())

    assert len(result.updated_infos) == 1
    lease = _route_lease(engine, 7)
    assert lease['ready'] is False
    assert lease['valid_until'] is None
    assert lease['revoked_at'] is not None
    assert lease['revocation_reason'] == (
        'replica_probe_became_route_ineligible')


def test_probe_batch_revokes_route_for_accepted_stable_offroute_row(
        recovery_database) -> None:
    engine = recovery_database
    ready = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert ready is not None
    ready.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    ready.status_property.service_ready_now = True
    ready.status_property.first_ready_time = 1.0
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             ready,
                                             expected_replica_exists=True,
                                             **_fence())
    _seed_ready_route(engine, ready)
    offroute = copy.deepcopy(ready)
    offroute.status_property.service_ready_now = False
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             offroute,
                                             expected_replica_exists=True,
                                             **_fence())
    # Build the exact inconsistent input this boundary must repair: an accepted
    # stable off-route replica with a still-active durable lease. The ordinary
    # whole-row writer already revokes routes, so reactivate only in fixture SQL
    # rather than accidentally testing that older hook.
    observed_at = datetime.datetime.now(datetime.timezone.utc)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                route_projection_schema.serve_route_replica_leases_table).where(
                    route_projection_schema.serve_route_replica_leases_table.c.
                    service_name == _SERVICE_NAME, route_projection_schema.
                    serve_route_replica_leases_table.c.replica_id == 7).values(
                        ready=True,
                        observed_at=observed_at,
                        valid_until=(observed_at +
                                     datetime.timedelta(seconds=60)),
                        revoked_at=None,
                        revocation_reason=None))
    assert _route_lease(engine, 7)['ready'] is True
    opening = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert opening is not None

    result = serve_state.commit_replica_observations_batch(
        _SERVICE_NAME, [_probe_write(opening, opening)], **_batch_fence())

    assert result.updated_infos == ()
    assert len(result.unchanged_infos) == 1
    lease = _route_lease(engine, 7)
    assert lease['ready'] is False
    assert lease['revoked_at'] is not None


def test_probe_batch_route_revocation_rolls_back_with_replica_write(
        recovery_database, monkeypatch) -> None:
    engine = recovery_database
    ready = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert ready is not None
    ready.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    ready.status_property.service_ready_now = True
    ready.status_property.first_ready_time = 1.0
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             ready,
                                             expected_replica_exists=True,
                                             **_fence())
    _seed_ready_route(engine, ready)
    desired = copy.deepcopy(ready)
    desired.status_property.service_ready_now = False
    revoke_attempted = threading.Event()
    original_revoke = route_projection.revoke_replica_leases_in_session

    def _observe_revoke(*args, **kwargs) -> int:
        revoked = original_revoke(*args, **kwargs)
        assert revoked == 1
        revoke_attempted.set()
        return revoked

    def _fail_replica_write(*_args, **_kwargs) -> None:
        raise RuntimeError('injected replica write failure')

    monkeypatch.setattr(route_projection, 'revoke_replica_leases_in_session',
                        _observe_revoke)
    monkeypatch.setattr(serve_state, '_write_locked_replica_infos_in_session',
                        _fail_replica_write)
    with pytest.raises(RuntimeError, match='injected replica write failure'):
        serve_state.commit_replica_observations_batch(
            _SERVICE_NAME, [_probe_write(ready, desired)], **_batch_fence())

    assert revoke_attempted.is_set()
    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted is not None
    assert persisted.status_property.service_ready_now
    lease = _route_lease(engine, 7)
    assert lease['ready'] is True
    assert lease['revoked_at'] is None


def test_production_probe_requires_commit_then_route_lease_then_ready(
        recovery_database) -> None:
    current = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert current is not None
    capable = _armed_capable(7)
    capable.replica_record_id = current.replica_record_id
    _seed_raw_replica_state(recovery_database, capable)
    manager = _probe_manager()
    manager._is_pool = False
    manager._system_recovery_status_initialized = {7}
    manager._system_recovery_route_epoch = (
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
    manager._system_recovery_route_registry = (
        replica_managers.system_recovery_route_lease.ManagerRouteLeaseRegistry(
            clock=lambda: 150.0))
    manager._get_version_spec = lambda _version: mock.Mock(
        readiness_path='/health', post_data=None, readiness_headers=None)
    route_url = 'http://10.0.0.7:8080'
    evidence = _armed_evidence()

    def _round(opening: replica_info.ReplicaInfo) -> replica_info.ReplicaInfo:
        reduced = manager._reduce_probe_results_batch(
            [opening], [
                replica_managers._ReadinessProbeResult(
                    info=opening,
                    succeeded=True,
                    observed_at=200.0,
                    request_started_monotonic=100.0)
            ],
            possibly_preempted_ids=set(),
            blocked_identity_ids=set(),
            provider_identity_errors={},
            provider_phase_deferred_replica_ids=set(),
            candidate_status_evidence={},
            candidate_cycle_evidence={},
            ordered_route_evidence={7: evidence},
            route_requires_next_probe_ids=set(),
            probe_urls={7: route_url},
            resolved_route_material={},
            deferred_route_ids=set(),
            accepted_probe_fingerprints={})
        return reduced[0]

    first = _round(capable)
    persisted_first = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted_first is not None
    assert not first.status_property.service_ready_now
    assert not persisted_first.status_property.service_ready_now
    targets = manager._route_lease_registry().probe_targets()
    assert len(targets) == 1
    assert targets[0].replica_id == 7

    second = _round(persisted_first)
    persisted_second = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted_second is not None
    assert second.status_property.service_ready_now
    assert persisted_second.status_property.service_ready_now


def test_stale_probe_cannot_issue_route_or_publish_local_readiness(
        recovery_database) -> None:
    current = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert current is not None
    capable = _armed_capable(7)
    capable.replica_record_id = current.replica_record_id
    _seed_raw_replica_state(recovery_database, capable)
    opening = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert opening is not None
    drifted = copy.deepcopy(opening)
    drifted.first_not_ready_time = 99.0
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             drifted,
                                             expected_replica_exists=True,
                                             **_fence())
    manager = _probe_manager()
    manager._is_pool = False
    manager._system_recovery_status_initialized = {7}
    manager._system_recovery_route_epoch = (
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
    manager._system_recovery_route_registry = (
        replica_managers.system_recovery_route_lease.ManagerRouteLeaseRegistry(
            clock=lambda: 150.0))
    manager._get_version_spec = lambda _version: mock.Mock(
        readiness_path='/health', post_data=None, readiness_headers=None)
    deferred_ids: set[int] = set()
    accepted_fingerprints: dict[int, tuple[str, int, int]] = {}

    reduced = manager._reduce_probe_results_batch(
        [opening], [
            replica_managers._ReadinessProbeResult(
                info=opening,
                succeeded=True,
                observed_at=200.0,
                request_started_monotonic=100.0)
        ],
        possibly_preempted_ids=set(),
        blocked_identity_ids=set(),
        provider_identity_errors={},
        provider_phase_deferred_replica_ids=set(),
        candidate_status_evidence={},
        candidate_cycle_evidence={},
        ordered_route_evidence={7: _armed_evidence()},
        route_requires_next_probe_ids=set(),
        probe_urls={7: 'http://10.0.0.7:8080'},
        resolved_route_material={},
        deferred_route_ids=deferred_ids,
        accepted_probe_fingerprints=accepted_fingerprints)

    assert not reduced[0].status_property.service_ready_now
    assert deferred_ids == {7}
    assert accepted_fingerprints == {}
    assert manager._route_lease_registry().probe_targets() == []
    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted is not None
    assert persisted.first_not_ready_time == 99.0
    assert not persisted.status_property.service_ready_now


def test_production_probe_batch_serializes_competing_whole_row_writer(
        recovery_database) -> None:
    engine = recovery_database
    replica_ids = list(range(7, 807))
    assert serve_state.add_or_update_replicas(
        _SERVICE_NAME,
        [(replica_id, _replica(replica_id)) for replica_id in replica_ids[1:]],
        **_fence())
    snapshots = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)
    manager = _probe_manager()
    infos = [snapshots[replica_id] for replica_id in replica_ids]
    probe_results = [
        replica_managers._ReadinessProbeResult(info=info,
                                               succeeded=True,
                                               observed_at=123.0,
                                               request_started_monotonic=10.0)
        for info in infos
    ]
    row_lock_entered = threading.Event()
    release_row_lock = threading.Event()

    def _pause_on_replica_lock(_connection, _cursor, statement, _parameters,
                               _context, _executemany):
        lowered = statement.lower()
        if 'from replicas' in lowered and 'for update' in lowered:
            row_lock_entered.set()
            assert release_row_lock.wait(timeout=10)

    observed_engines = (engine, db_utils.get_postgres_lock_engine(engine))
    for observed_engine in observed_engines:
        sqlalchemy.event.listen(observed_engine, 'before_cursor_execute',
                                _pause_on_replica_lock)
    result_holder = []
    error_holder = []
    writer_attempted = threading.Event()
    writer_acquired_manager_lock = threading.Event()
    writer_finished = threading.Event()

    def _commit() -> None:
        try:
            result_holder.append(
                manager._reduce_probe_results_batch(
                    infos,
                    probe_results,
                    possibly_preempted_ids=set(),
                    blocked_identity_ids=set(),
                    provider_identity_errors={},
                    provider_phase_deferred_replica_ids=set(),
                    candidate_status_evidence={},
                    candidate_cycle_evidence={},
                    ordered_route_evidence={},
                    route_requires_next_probe_ids=set(),
                    probe_urls={},
                    resolved_route_material=None,
                    deferred_route_ids=None,
                    accepted_probe_fingerprints=None))
        except BaseException as error:  # pylint: disable=broad-except
            error_holder.append(error)

    def _competing_writer() -> None:
        writer_attempted.set()
        with manager.lock:
            writer_acquired_manager_lock.set()
            fresh = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
            assert fresh is not None
            fresh.status_property.sky_launch_status = (
                common_utils.ProcessStatus.SUCCEEDED)
            manager._persist_replica(7, fresh)
        writer_finished.set()

    commit_thread = threading.Thread(target=_commit)
    writer_thread = threading.Thread(target=_competing_writer)
    commit_thread.start()
    try:
        assert row_lock_entered.wait(timeout=10)
        writer_thread.start()
        assert writer_attempted.wait(timeout=1)
        assert not writer_acquired_manager_lock.wait(timeout=0.1)
    finally:
        release_row_lock.set()
        commit_thread.join(timeout=20)
        # ``row_lock_entered`` can fail before the writer starts.  Do not mask
        # that useful assertion with ``cannot join thread before it is
        # started`` during cleanup.
        if writer_thread.ident is not None:
            writer_thread.join(timeout=20)
        for observed_engine in observed_engines:
            sqlalchemy.event.remove(observed_engine, 'before_cursor_execute',
                                    _pause_on_replica_lock)

    assert not commit_thread.is_alive()
    assert not writer_thread.is_alive()
    assert error_holder == []
    assert writer_acquired_manager_lock.is_set()
    assert writer_finished.is_set()
    assert len(result_holder[0]) == 800
    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted is not None
    assert persisted.status_property.service_ready_now
    assert (persisted.status_property.sky_launch_status ==
            common_utils.ProcessStatus.SUCCEEDED)


def test_probe_lock_timeout_defers_batch_rolls_back_and_releases_manager_lock(
        recovery_database, monkeypatch) -> None:
    engine = recovery_database
    opening = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert opening is not None
    probe_result = replica_managers._ReadinessProbeResult(
        info=opening,
        succeeded=True,
        observed_at=123.0,
        request_started_monotonic=10.0)
    manager = _probe_manager()
    monkeypatch.setattr(serve_state, '_PROBE_BATCH_LOCK_TIMEOUT_MS', 100)
    monkeypatch.setattr(serve_state, '_PROBE_BATCH_STATEMENT_TIMEOUT_MS', 1000)

    blocker = engine.connect()
    blocker_transaction = blocker.begin()
    blocker.execute(
        sqlalchemy.update(serve_state.replicas_table).where(
            serve_state.replicas_table.c.service_name == _SERVICE_NAME,
            serve_state.replicas_table.c.replica_id == 7).values(
                cluster_name='svc-7'))
    batch_holds_manager_lock = threading.Event()
    allow_batch_commit = threading.Event()
    writer_acquired_manager_lock = threading.Event()
    result_holder = []
    error_holder = []
    original_commit = manager._commit_probe_row_plans

    def _observe_locked_commit(plans):
        batch_holds_manager_lock.set()
        assert allow_batch_commit.wait(timeout=10)
        return original_commit(plans)

    manager._commit_probe_row_plans = _observe_locked_commit

    def _commit() -> None:
        try:
            result_holder.append(
                manager._reduce_probe_results_batch(
                    [opening], [probe_result],
                    possibly_preempted_ids=set(),
                    blocked_identity_ids=set(),
                    provider_identity_errors={},
                    provider_phase_deferred_replica_ids=set(),
                    candidate_status_evidence={},
                    candidate_cycle_evidence={},
                    ordered_route_evidence={},
                    route_requires_next_probe_ids=set(),
                    probe_urls={},
                    resolved_route_material=None,
                    deferred_route_ids=None,
                    accepted_probe_fingerprints=None))
        except BaseException as error:  # pylint: disable=broad-except
            error_holder.append(error)

    def _competing_writer() -> None:
        with manager.lock:
            writer_acquired_manager_lock.set()
            fresh = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
            assert fresh is not None
            fresh.status_property.sky_launch_status = (
                common_utils.ProcessStatus.SUCCEEDED)
            manager._persist_replica(7, fresh)

    commit_thread = threading.Thread(target=_commit)
    writer_thread = threading.Thread(target=_competing_writer)
    commit_thread.start()
    try:
        assert batch_holds_manager_lock.wait(timeout=2)
        writer_thread.start()
        # The competing thread is the portable ownership proof.  A same-thread
        # nonblocking acquire cannot prove this for ``threading.RLock`` because
        # reentrant acquisition is expected to succeed.
        assert not writer_acquired_manager_lock.wait(timeout=0.1)
        allow_batch_commit.set()
        commit_thread.join(timeout=5)
        assert not commit_thread.is_alive()
        assert error_holder == []
        assert len(result_holder) == 1
        assert len(result_holder[0]) == 1
        assert not result_holder[0][0].status_property.service_ready_now
        # The retryable PostgreSQL timeout has unwound the batch transaction and
        # released the process writer mutex. The competing write may now enter,
        # although its row update remains blocked by our explicit PG blocker.
        assert writer_acquired_manager_lock.wait(timeout=2)
    finally:
        allow_batch_commit.set()
        blocker_transaction.rollback()
        blocker.close()
        commit_thread.join(timeout=5)
        if writer_thread.ident is not None:
            writer_thread.join(timeout=5)

    assert not writer_thread.is_alive()
    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted is not None
    assert not persisted.status_property.service_ready_now
    assert (persisted.status_property.sky_launch_status ==
            common_utils.ProcessStatus.SUCCEEDED)


def test_batch_patch_isolates_every_stale_identity_facet(
        recovery_database) -> None:
    engine = recovery_database
    replica_ids = list(range(7, 12))
    assert serve_state.add_or_update_replicas(
        _SERVICE_NAME,
        [(replica_id, _replica(replica_id)) for replica_id in replica_ids[1:]],
        **_fence())
    snapshots = serve_state.get_replica_infos_from_ids(_SERVICE_NAME,
                                                       replica_ids)
    writes = [
        _recovery_write(_candidate_reduction(snapshots[replica_id]))
        for replica_id in replica_ids
    ]

    # Exercise absence, record recreation, service-version drift, and recovery
    # revision drift in a single transaction; replica 11 remains current.
    assert serve_state.remove_replica(
        _SERVICE_NAME,
        7,
        expected_replica_record_id=snapshots[7].replica_record_id,
        **_fence())
    assert serve_state.remove_replica(
        _SERVICE_NAME,
        8,
        expected_replica_record_id=snapshots[8].replica_record_id,
        **_fence())
    assert serve_state.add_or_update_replica(_SERVICE_NAME, 8, _replica(8),
                                             **_fence())
    upgraded = copy.deepcopy(snapshots[9])
    upgraded.version += 1
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             9,
                                             upgraded,
                                             expected_replica_exists=True,
                                             **_fence())
    _make_candidate(10)

    with _capture_sql(engine) as statements:
        result = serve_state.commit_replica_observations_batch(
            _SERVICE_NAME, writes, **_batch_fence())

    assert result.stale_replica_ids == (7, 8, 9, 10)
    assert result.unchanged_infos == ()
    assert tuple(info.replica_id for info in result.updated_infos) == (11,)
    assert result.updated_infos[0].system_recovery_revision == 1
    replica_locks = [
        statement for statement in statements
        if 'for update' in statement and 'from replicas' in statement
    ]
    assert len(replica_locks) == 1
    updates = [
        statement for statement in statements
        if statement.lstrip().startswith('update replicas ')
    ]
    assert len(updates) == 1


def test_batch_patch_reports_noop_without_increment_or_update(
        recovery_database) -> None:
    engine = recovery_database
    current = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)

    with _capture_sql(engine) as statements:
        result = serve_state.commit_replica_observations_batch(
            _SERVICE_NAME, [_recovery_write(current)], **_batch_fence())

    assert result.updated_infos == ()
    assert tuple(info.replica_id for info in result.unchanged_infos) == (7,)
    assert result.stale_replica_ids == ()
    assert not any(statement.lstrip().startswith('update replicas ')
                   for statement in statements)
    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted.system_recovery_revision == 0


def test_batch_patch_rejects_invalid_transition_without_partial_commit(
        recovery_database) -> None:
    assert serve_state.add_or_update_replica(_SERVICE_NAME, 8, _replica(8),
                                             **_fence())
    snapshots = serve_state.get_replica_infos_from_ids(_SERVICE_NAME, [7, 8])
    invalid = copy.deepcopy(snapshots[7])
    invalid.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.CAPABLE)
    valid = _candidate_reduction(snapshots[8])

    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='requires an exact launch intent'):
        serve_state.commit_replica_observations_batch(
            _SERVICE_NAME, [_recovery_write(invalid),
                            _recovery_write(valid)], **_batch_fence())

    persisted = serve_state.get_replica_infos_from_ids(_SERVICE_NAME, [7, 8])
    assert all(
        info.system_recovery_revision == 0 for info in persisted.values())
    assert all(info.system_recovery_disposition ==
               recovery_state.SystemRecoveryDisposition.ORDINARY
               for info in persisted.values())


def test_nonce_bind_is_locked_update_only_and_one_shot(
        recovery_database) -> None:
    engine = recovery_database
    with _capture_sql(engine) as candidate_statements:
        candidate = _make_candidate(7)
    _assert_lifecycle_service_replica_lock_order(candidate_statements)
    _assert_replica_update_only(candidate_statements)
    assert candidate.system_recovery_revision == 1

    intent = candidate.system_recovery_launch_intent
    assert intent is not None
    context = _unbound_context(intent)
    mismatch = dict(context)
    mismatch[constants.SYSTEM_OOM_RECOVERY_LAUNCH_NONCE_KEY] = 'c' * 64
    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='locked intent'):
        serve_state.bind_replica_system_recovery_launch_request(
            mismatch, 'request-mismatch')
    unchanged = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert unchanged is not None
    assert unchanged.system_recovery_revision == 1
    assert unchanged.launch_request_id is None

    with _capture_sql(engine) as bind_statements:
        bound = serve_state.bind_replica_system_recovery_launch_request(
            context, 'request-1')
    _assert_lifecycle_service_replica_lock_order(bind_statements)
    _assert_replica_update_only(bind_statements)
    assert bound.launch_request_id == 'request-1'
    assert bound.system_recovery_revision == 2
    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='already consumed'):
        serve_state.bind_replica_system_recovery_launch_request(
            context, 'request-2')
    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted is not None
    assert persisted.launch_request_id == 'request-1'
    assert persisted.system_recovery_revision == 2
    with _capture_sql(engine) as job_statements:
        associated = serve_state.set_replica_system_recovery_job_id(
            _SERVICE_NAME,
            7,
            9,
            expected_launch_request_id='request-1',
            expected_revision=2,
            owner_fence=_observer_fence())
    _assert_lifecycle_service_replica_lock_order(job_statements)
    _assert_replica_update_only(job_statements)
    assert associated.service_job_id == 9
    assert associated.system_recovery_revision == 3
    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='different service job'):
        serve_state.set_replica_system_recovery_job_id(
            _SERVICE_NAME,
            7,
            10,
            expected_launch_request_id='request-1',
            expected_revision=3,
            owner_fence=_observer_fence())
    _assert_json_only_replica_state(engine, 7)

    missing_context = _unbound_context(_intent(99))
    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='absent'):
        serve_state.bind_replica_system_recovery_launch_request(
            missing_context, 'request-missing')
    with engine.connect() as connection:
        missing_rows = connection.execute(
            sqlalchemy.select(serve_state.replicas_table.c.replica_id).where(
                serve_state.replicas_table.c.service_name == _SERVICE_NAME,
                serve_state.replicas_table.c.replica_id == 99)).fetchall()
    assert missing_rows == []


@pytest.mark.parametrize('mutation', [
    'patch',
    'create',
    'demote',
    'set_job',
    'bind',
])
@pytest.mark.parametrize('replacement', ['incarnation', 'owner_epoch'])
def test_singleton_recovery_mutation_rejects_same_pid_ip_successor(
        recovery_database, mutation: str, replacement: str) -> None:
    """A PID/IP-reusing successor cannot consume its predecessor's work."""
    engine = recovery_database
    old_fence = _observer_fence()
    context = None
    if mutation == 'create':
        opening = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
        assert opening is not None
        desired = _candidate_reduction(opening)
    else:
        opening = _make_candidate(7)
        desired = copy.deepcopy(opening)
        if mutation == 'patch':
            desired.system_recovery_quarantine = (
                recovery_state.SystemRecoveryQuarantine(
                    recovery_state.RecoveryQuarantineReason.
                    INCONSISTENT_V13_BUNDLE))
        elif mutation == 'demote':
            desired.system_recovery_disposition = (
                recovery_state.SystemRecoveryDisposition.ORDINARY)
        elif mutation in ('set_job', 'bind'):
            intent = opening.system_recovery_launch_intent
            assert intent is not None
            context = _unbound_context(intent)
            if mutation == 'set_job':
                opening = (
                    serve_state.bind_replica_system_recovery_launch_request(
                        context, 'request-before-successor'))

    before = copy.deepcopy(_raw_replica_row(engine, 7)['replica_state'])
    successor_incarnation = _CONTROLLER_INCARNATION
    successor_owner_epoch = _CONTROLLER_OWNER_EPOCH
    if replacement == 'incarnation':
        successor_incarnation = uuid.UUID(
            '44444444-4444-4444-8444-444444444444')
    else:
        successor_owner_epoch += 1
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == _SERVICE_NAME).values(
                    controller_incarnation=successor_incarnation,
                    controller_owner_epoch=successor_owner_epoch))

    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='owner no longer matches'):
        if mutation == 'patch':
            serve_state.patch_replica_system_recovery(
                _SERVICE_NAME,
                7,
                desired,
                owner_fence=old_fence,
                expected_revision=opening.system_recovery_revision)
        elif mutation == 'create':
            serve_state.create_replica_system_recovery_candidate(
                _SERVICE_NAME,
                7,
                desired,
                owner_fence=old_fence,
                expected_revision=opening.system_recovery_revision)
        elif mutation == 'demote':
            serve_state.demote_replica_system_recovery_to_ordinary(
                _SERVICE_NAME,
                7,
                desired,
                owner_fence=old_fence,
                expected_revision=opening.system_recovery_revision)
        elif mutation == 'set_job':
            serve_state.set_replica_system_recovery_job_id(
                _SERVICE_NAME,
                7,
                9,
                expected_launch_request_id='request-before-successor',
                owner_fence=old_fence,
                expected_revision=opening.system_recovery_revision)
        else:
            assert context is not None
            serve_state.bind_replica_system_recovery_launch_request(
                context, 'request-after-successor')

    assert _raw_replica_row(engine, 7)['replica_state'] == before
    with engine.connect() as connection:
        successor = connection.execute(
            sqlalchemy.select(
                serve_state.services_table.c.controller_pid,
                serve_state.services_table.c.controller_ip).where(
                    serve_state.services_table.c.name == _SERVICE_NAME)).one()
    assert tuple(successor) == _OWNER


def test_revision_terminal_quarantine_and_demotion_are_absorbing(
        recovery_database) -> None:
    engine = recovery_database
    candidate = _make_candidate(7)
    with pytest.raises(
            serve_state.ReplicaSystemRecoveryRevisionConflict) as exc:
        serve_state.patch_replica_system_recovery(_SERVICE_NAME,
                                                  7,
                                                  copy.deepcopy(candidate),
                                                  expected_revision=0,
                                                  owner_fence=_observer_fence())
    assert exc.value.current_revision == 1

    demotion = copy.deepcopy(candidate)
    demotion.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.ORDINARY)
    demoted = serve_state.demote_replica_system_recovery_to_ordinary(
        _SERVICE_NAME,
        7,
        demotion,
        expected_revision=1,
        owner_fence=_observer_fence())
    assert demoted.system_recovery_revision == 2
    resurrection = copy.deepcopy(demoted)
    resurrection.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.CANDIDATE)
    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='demoted ordinary'):
        serve_state.patch_replica_system_recovery(_SERVICE_NAME,
                                                  7,
                                                  resurrection,
                                                  expected_revision=2,
                                                  owner_fence=_observer_fence())

    assert serve_state.add_or_update_replica(_SERVICE_NAME, 8, _replica(8),
                                             **_fence())
    terminal_candidate = _make_candidate(8)
    terminal_whole_row = copy.deepcopy(terminal_candidate)
    terminal_whole_row.status_property.sky_down_status = (
        common_utils.ProcessStatus.RUNNING)
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             8,
                                             terminal_whole_row,
                                             **_fence(),
                                             expected_replica_exists=True)
    terminal = serve_state.get_replica_info_from_id(_SERVICE_NAME, 8)
    assert terminal is not None
    terminal_demotion = copy.deepcopy(terminal)
    terminal_demotion.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.ORDINARY)
    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='absorbing'):
        serve_state.patch_replica_system_recovery(_SERVICE_NAME,
                                                  8,
                                                  terminal_demotion,
                                                  expected_revision=1,
                                                  owner_fence=_observer_fence())

    assert serve_state.add_or_update_replica(_SERVICE_NAME, 9, _replica(9),
                                             **_fence())
    partial = _raw_replica_row(engine, 9)['replica_state']
    partial['replica_info_version'] = 13
    partial.pop('service_job_id')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == _SERVICE_NAME,
                serve_state.replicas_table.c.replica_id == 9).values(
                    replica_state=partial))
    with pytest.raises(ValueError, match='invalid top-level shape'):
        serve_state.get_replica_info_from_id(_SERVICE_NAME, 9)


def test_stale_generic_whole_row_preserves_locked_recovery_bundle(
        recovery_database) -> None:
    engine = recovery_database
    candidate = _make_candidate(7)
    stale_whole_row = copy.deepcopy(candidate)
    intent = candidate.system_recovery_launch_intent
    assert intent is not None
    serve_state.bind_replica_system_recovery_launch_request(
        _unbound_context(intent), 'request-1')

    stale_whole_row.first_not_ready_time = 42.0
    stale_whole_row.system_recovery_revision = 999
    stale_whole_row.launch_request_id = None
    assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                             7,
                                             stale_whole_row,
                                             **_fence(),
                                             expected_replica_exists=True)

    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted is not None
    assert persisted.first_not_ready_time == 42.0
    assert persisted.launch_request_id == 'request-1'
    assert persisted.system_recovery_revision == 2
    _assert_json_only_replica_state(engine, 7)


def test_delete_first_rejects_stale_single_batch_and_paid_completion(
        recovery_database) -> None:
    engine = recovery_database
    stale = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert stale is not None
    stale.version = 4
    assert serve_state.add_or_update_replica(_SERVICE_NAME, 8, _replica(8),
                                             **_fence())
    survivor_update = serve_state.get_replica_info_from_id(_SERVICE_NAME, 8)
    assert survivor_update is not None
    survivor_update.version = 4
    assert serve_state.remove_replica(
        _SERVICE_NAME,
        7,
        **_fence(),
        expected_replica_record_id=stale.replica_record_id)

    with _capture_sql(engine) as single_statements:
        assert not serve_state.add_or_update_replica(
            _SERVICE_NAME, 7, stale, **_fence(), expected_replica_exists=True)
    _assert_lifecycle_service_replica_lock_order(single_statements)
    assert not any(statement.lstrip().startswith('insert into replicas ')
                   for statement in single_statements)
    assert serve_state.get_replica_info_from_id(_SERVICE_NAME, 7) is None

    with _capture_sql(engine) as batch_statements:
        assert not serve_state.add_or_update_replicas(
            _SERVICE_NAME, [(7, stale), (8, survivor_update)],
            **_fence(),
            expected_replica_exists=True)
    _assert_lifecycle_service_replica_lock_order(batch_statements)
    assert not any(statement.lstrip().startswith('insert into replicas ')
                   for statement in batch_statements)
    survivor = serve_state.get_replica_info_from_id(_SERVICE_NAME, 8)
    assert survivor is not None
    assert survivor.version == 3

    with _capture_sql(engine) as paid_statements:
        assert not serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            _SERVICE_NAME,
            _SERVICE_HASH, [(7, stale)],
            {7: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=1,
            max_limit=1,
            now=1.0,
            success_ttl_seconds=60.0,
            expected_controller_owner=_OWNER)
    _assert_lifecycle_service_replica_lock_order(paid_statements)
    assert not any(statement.lstrip().startswith('insert into replicas ')
                   for statement in paid_statements)
    assert serve_state.get_replica_info_from_id(_SERVICE_NAME, 7) is None


def test_write_first_then_delete_leaves_no_replica_row(
        recovery_database) -> None:
    engine = recovery_database
    updated = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert updated is not None
    updated.version = 4

    with _capture_sql(engine) as update_statements:
        assert serve_state.add_or_update_replica(_SERVICE_NAME,
                                                 7,
                                                 updated,
                                                 **_fence(),
                                                 expected_replica_exists=True)
    _assert_lifecycle_service_replica_lock_order(update_statements)
    _assert_replica_update_only(update_statements)
    assert serve_state.remove_replica(
        _SERVICE_NAME,
        7,
        **_fence(),
        expected_replica_record_id=updated.replica_record_id)
    assert serve_state.get_replica_info_from_id(_SERVICE_NAME, 7) is None


def test_recreated_numeric_id_rejects_stale_single_batch_and_paid_completion(
        recovery_database) -> None:
    engine = recovery_database
    stale = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert stale is not None
    assert serve_state.add_or_update_replica(_SERVICE_NAME, 8, _replica(8),
                                             **_fence())
    survivor_update = serve_state.get_replica_info_from_id(_SERVICE_NAME, 8)
    assert survivor_update is not None
    assert serve_state.remove_replica(
        _SERVICE_NAME,
        7,
        **_fence(),
        expected_replica_record_id=stale.replica_record_id)
    replacement = _replica(7)
    replacement.version = 10
    assert replacement.replica_record_id != stale.replica_record_id
    assert serve_state.add_or_update_replica(_SERVICE_NAME, 7, replacement,
                                             **_fence())
    with engine.begin() as connection:
        connection.execute(
            serve_state.paid_capacity_pools_table.insert().values(
                pool_key='replacement-pool',
                current_limit=1,
                successes_since_resize=0,
                updated_at=1.0))
        connection.execute(
            serve_state.paid_capacity_claims_table.insert().values(
                service_name=_SERVICE_NAME,
                service_hash=_SERVICE_HASH,
                replica_id=7,
                pool_key='replacement-pool',
                priority=1,
                claimed_at=1.0))
    with engine.connect() as connection:
        claim_before = dict(
            connection.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table)).mappings().one())
    stale.version = 99
    survivor_update.version = 99

    with _capture_sql(engine) as single_statements:
        assert not serve_state.add_or_update_replica(
            _SERVICE_NAME, 7, stale, **_fence(), expected_replica_exists=True)
    _assert_lifecycle_service_replica_lock_order(single_statements)
    assert not any(statement.lstrip().startswith('update replicas ')
                   for statement in single_statements)

    with _capture_sql(engine) as batch_statements:
        assert not serve_state.add_or_update_replicas(
            _SERVICE_NAME, [(7, stale), (8, survivor_update)],
            **_fence(),
            expected_replica_exists=True)
    _assert_lifecycle_service_replica_lock_order(batch_statements)
    assert not any(statement.lstrip().startswith('update replicas ')
                   for statement in batch_statements)

    with _capture_sql(engine) as paid_statements:
        assert not serve_state.add_or_update_replicas_with_paid_capacity_outcomes(
            _SERVICE_NAME,
            _SERVICE_HASH, [(7, stale)],
            {7: paid_capacity.LaunchOutcome.SUCCESS},
            base_limit=1,
            max_limit=1,
            now=1.0,
            success_ttl_seconds=60.0,
            expected_controller_owner=_OWNER)
    _assert_lifecycle_service_replica_lock_order(paid_statements)
    assert not any(statement.lstrip().startswith('update replicas ')
                   for statement in paid_statements)

    with _capture_sql(engine) as single_delete_statements:
        assert not serve_state.remove_replica(
            _SERVICE_NAME,
            7,
            **_fence(),
            expected_replica_record_id=stale.replica_record_id)
    _assert_lifecycle_service_replica_lock_order(single_delete_statements)
    with _capture_sql(engine) as batch_delete_statements:
        assert not serve_state.remove_replicas(
            _SERVICE_NAME, [7, 8],
            **_fence(),
            expected_replica_record_ids={
                7: stale.replica_record_id,
                8: survivor_update.replica_record_id,
            })
    _assert_lifecycle_service_replica_lock_order(batch_delete_statements)

    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    survivor = serve_state.get_replica_info_from_id(_SERVICE_NAME, 8)
    assert persisted is not None and survivor is not None
    assert persisted.replica_record_id == replacement.replica_record_id
    assert persisted.version == 10
    assert survivor.version == 3
    with engine.connect() as connection:
        claim_after = dict(
            connection.execute(
                sqlalchemy.select(
                    serve_state.paid_capacity_claims_table)).mappings().one())
    assert claim_after == claim_before
    _assert_json_only_replica_state(engine, 7)


def test_initial_replica_paths_are_insert_only_on_key_conflict(
        recovery_database) -> None:
    engine = recovery_database
    initial = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert initial is not None

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.add_or_update_replica(_SERVICE_NAME, 7, _replica(7),
                                          **_fence())
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.add_or_update_replicas(_SERVICE_NAME, [(8, _replica(8)),
                                                           (7, _replica(7))],
                                           **_fence())
    assert serve_state.get_replica_info_from_id(_SERVICE_NAME, 8) is None

    # Paid admission now reads the elected version's spec, decodes the exact
    # provider pool identity, and fails closed on any live row without paid or
    # zero-cost attribution before it reaches the replica INSERT.  Seed that
    # upstream authority so the INSERT is what conflicts.
    paid_pool_key = _paid_pool_key()
    paid_row_state = _raw_replica_row(engine, 7)['replica_state']
    paid_row_state['paid_capacity_pool_key'] = paid_pool_key
    with engine.begin() as connection:
        connection.execute(serve_state.version_specs_table.insert().values(
            service_name=_SERVICE_NAME,
            version=3,
            spec=pickle.dumps(
                service_spec.SkyServiceSpec.from_yaml_config({'replicas': 1})),
            yaml_content='replicas: 1\n'))
        connection.execute(serve_state.services_table.update().where(
            serve_state.services_table.c.name == _SERVICE_NAME).values(
                current_version=3))
        connection.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == _SERVICE_NAME,
                serve_state.replicas_table.c.replica_id == 7).values(
                    paid_capacity_pool_key=paid_pool_key,
                    replica_state=paid_row_state))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.try_add_replica_with_paid_capacity_claim(
            _SERVICE_NAME,
            _SERVICE_HASH,
            7,
            _replica(7),
            pool_key=paid_pool_key,
            priority=1,
            base_limit=1,
            max_limit=2,
            now=100.0,
            success_ttl_seconds=60.0,
            waiter_ttl_seconds=60.0,
            expected_controller_owner=_OWNER)

    fill_pool_key = '["test-context","a100"]'
    with engine.begin() as connection:
        connection.execute(
            serve_state.reserved_fill_claims_table.insert().values(
                service_name=_SERVICE_NAME,
                pool_key=fill_pool_key,
                weight=1,
                floor_replicas=1,
                gpus_per_replica=1,
                holdings_fill=1,
                heartbeat_ts=100.0))
        connection.execute(
            serve_state.reserved_fill_lease_table.insert().values(id=1,
                                                                  epoch=1))
    duplicate_fill_replica = _replica(7)
    duplicate_fill_replica.reserved_fill = True
    duplicate_fill_replica.reserved_fill_pool_key = fill_pool_key
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.add_replica_if_round_epoch(
            _SERVICE_NAME,
            7,
            duplicate_fill_replica,
            pool_key=fill_pool_key,
            expected_epoch=1,
            expected_service_hash=_SERVICE_HASH,
            expected_controller_owner=_OWNER,
            expected_lease_token=1)

    persisted = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert persisted is not None
    assert persisted.replica_record_id == initial.replica_record_id
    assert persisted.version == initial.version
    with engine.connect() as connection:
        paid_claim = connection.execute(
            sqlalchemy.select(serve_state.paid_capacity_claims_table).where(
                serve_state.paid_capacity_claims_table.c.service_name ==
                _SERVICE_NAME,
                serve_state.paid_capacity_claims_table.c.replica_id ==
                7)).first()
    assert paid_claim is None


def test_all_fields_absent_v13_is_not_a_runtime_read_path(
        recovery_database) -> None:
    engine = recovery_database
    row = _raw_replica_row(engine, 7)
    rollback = row['replica_state']
    rollback['replica_info_version'] = 13
    for field_name in replica_info.V13_ADDITIVE_STORAGE_FIELDS:
        rollback.pop(field_name)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == _SERVICE_NAME,
                serve_state.replicas_table.c.replica_id == 7).values(
                    replica_state=rollback))

    with pytest.raises(ValueError, match='invalid top-level shape'):
        serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    persisted = _raw_replica_row(engine, 7)
    assert not set(replica_info.V13_ADDITIVE_STORAGE_FIELDS).intersection(
        persisted['replica_state'])
