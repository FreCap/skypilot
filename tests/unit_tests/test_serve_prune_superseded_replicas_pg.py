"""PostgreSQL contracts for batched superseded-replica pruning."""
# pylint: disable=not-callable,protected-access,redefined-outer-name
# pylint: disable=unused-import

import datetime
import time
from unittest import mock
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import ordinary_launch_handoff
from sky.serve import replica_managers
from sky.serve import serve_history
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import system_recovery_route_lease
from sky.utils import common_utils
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(name='serve_prune_superseded_replicas_pg')

_SERVICE_NAME = 'boltz-l4-fleet'
_SERVICE_HASH = 'service-hash'
_OWNER = (123, '10.0.0.5')
_LIFECYCLE_EPOCH = 3
_LATEST_VERSION = 56


@pytest.fixture
def prune_database(empty_postgres, monkeypatch):
    config = migration_utils.get_alembic_config(empty_postgres,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, migration_utils.SERVE_VERSION)
    monkeypatch.setattr(serve_state_schema._db_manager, '_engine',
                        empty_postgres)
    controller_incarnation = uuid.uuid4()
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name=_SERVICE_NAME, epoch=_LIFECYCLE_EPOCH))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name=_SERVICE_NAME,
                workspace='default',
                status=serve_state.ServiceStatus.READY.value,
                hash=_SERVICE_HASH,
                current_version=_LATEST_VERSION,
                active_versions=f'[{_LATEST_VERSION}]',
                pool=0,
                lifecycle_epoch=_LIFECYCLE_EPOCH,
                controller_incarnation=controller_incarnation,
                controller_owner_epoch=1,
                controller_pid=_OWNER[0],
                controller_ip=_OWNER[1]))
    return empty_postgres, controller_incarnation


def _failed_replica(
        replica_id: int, version: int,
        status: serve_state.ReplicaStatus) -> replica_managers.ReplicaInfo:
    info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'svc-{replica_id}',
                                        replica_port='8000',
                                        is_spot=False,
                                        location=None,
                                        version=version,
                                        resources_override=None)
    state = info.status_property
    if status == serve_state.ReplicaStatus.FAILED_PROVISION:
        state.sky_launch_status = common_utils.ProcessStatus.FAILED
        state.sky_down_status = common_utils.ProcessStatus.SUCCEEDED
    elif status == serve_state.ReplicaStatus.FAILED_INITIAL_DELAY:
        state.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
        state.first_ready_time = -1
        state.sky_down_status = common_utils.ProcessStatus.SUCCEEDED
    elif status == serve_state.ReplicaStatus.FAILED_PROBING:
        state.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
        state.first_ready_time = time.time()
        state.service_ready_now = False
        state.sky_down_status = common_utils.ProcessStatus.SUCCEEDED
    elif status == serve_state.ReplicaStatus.FAILED_CLEANUP:
        state.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
        state.sky_down_status = common_utils.ProcessStatus.FAILED
    elif status == serve_state.ReplicaStatus.UNKNOWN:
        state.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
        state.first_ready_time = time.time()
        state.service_ready_now = True
        state.sky_down_status = common_utils.ProcessStatus.SUCCEEDED
    else:
        raise ValueError(f'Unsupported test status: {status.value}')
    assert info.status == status
    return info


def _persist(info: replica_managers.ReplicaInfo) -> None:
    assert serve_state.add_or_update_replica(
        _SERVICE_NAME,
        info.replica_id,
        info,
        expected_service_hash=_SERVICE_HASH,
        expected_lifecycle_epoch=_LIFECYCLE_EPOCH,
        expected_controller_owner=_OWNER)


def _manager(owner=_OWNER) -> replica_managers.SkyPilotReplicaManager:
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = _SERVICE_NAME
    manager._service_hash = _SERVICE_HASH
    manager._controller_owner = owner
    manager._system_recovery_route_registry = (
        system_recovery_route_lease.ManagerRouteLeaseRegistry())
    manager.latest_version = _LATEST_VERSION
    manager._superseded_prune_pending = True
    return manager


def _insert_retained_history(engine, info, controller_incarnation):
    now = datetime.datetime.now(datetime.timezone.utc)
    bucket = now.replace(second=0, microsecond=0)
    event_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_history.serve_replica_status_history_table).values(
                    service_name=_SERVICE_NAME,
                    service_hash=_SERVICE_HASH,
                    version=info.version,
                    bucket_start=bucket,
                    observed_at=now,
                    ready_count=0,
                    provisioning_count=0,
                    not_ready_count=0,
                    errored_count=1,
                    preempted_count=0,
                    stopping_count=0,
                    total_count=1))
        connection.execute(
            sqlalchemy.insert(
                ordinary_launch_handoff.
                serve_ordinary_launch_handoff_events_table).values(
                    event_id=event_id,
                    observed_at=now,
                    event_kind=(ordinary_launch_handoff.EventKind.
                                SERVE_RESULT_PROJECTED.value),
                    service_name=_SERVICE_NAME,
                    service_version=info.version,
                    replica_id=info.replica_id,
                    replica_record_id=uuid.UUID(info.replica_record_id),
                    controller_route_epoch=controller_incarnation,
                    ordinary_request_id=str(uuid.uuid4()),
                    service_job_id=1,
                    input_digest='a' * 64))
    return bucket, event_id


def test_prune_uses_one_fenced_batch_and_preserves_history(
        prune_database) -> None:
    engine, controller_incarnation = prune_database
    candidates = [
        _failed_replica(3, 55, serve_state.ReplicaStatus.FAILED_PROBING),
        _failed_replica(1, 14, serve_state.ReplicaStatus.FAILED_PROVISION),
        _failed_replica(2, 40, serve_state.ReplicaStatus.FAILED_INITIAL_DELAY),
    ]
    survivors = [
        _failed_replica(4, _LATEST_VERSION,
                        serve_state.ReplicaStatus.FAILED_PROVISION),
        _failed_replica(5, 40, serve_state.ReplicaStatus.FAILED_CLEANUP),
        _failed_replica(6, 40, serve_state.ReplicaStatus.UNKNOWN),
    ]
    for info in candidates + survivors:
        _persist(info)
    bucket, event_id = _insert_retained_history(engine, candidates[1],
                                                controller_incarnation)

    manager = _manager()
    with mock.patch.object(
            serve_state, 'remove_replicas', wraps=serve_state.remove_replicas
    ) as batch_remove, mock.patch.object(
            serve_state,
            'remove_replica',
            side_effect=AssertionError('single-row delete used')):
        manager._prune_superseded_failed_replicas()

    batch_remove.assert_called_once()
    args, kwargs = batch_remove.call_args
    assert args[:3] == (_SERVICE_NAME, [1, 2, 3], _SERVICE_HASH)
    assert kwargs['expected_controller_owner'] == _OWNER
    assert set(kwargs['expected_replica_record_ids']) == {1, 2, 3}
    assert {
        info.replica_id for info in serve_state.get_replica_infos(_SERVICE_NAME)
    } == {4, 5, 6}
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_history.serve_replica_status_history_table).where(
                    serve_history.serve_replica_status_history_table.c.
                    service_name == _SERVICE_NAME,
                    serve_history.serve_replica_status_history_table.c.
                    bucket_start == bucket)).scalar_one() == 1
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                ordinary_launch_handoff.
                serve_ordinary_launch_handoff_events_table).where(
                    ordinary_launch_handoff.
                    serve_ordinary_launch_handoff_events_table.c.event_id ==
                    event_id)).scalar_one() == 1


def test_prune_rejects_the_entire_batch_for_a_stale_controller_owner(
        prune_database) -> None:
    del prune_database
    candidates = [
        _failed_replica(1, 40, serve_state.ReplicaStatus.FAILED_PROVISION),
        _failed_replica(2, 40, serve_state.ReplicaStatus.FAILED_INITIAL_DELAY),
    ]
    for info in candidates:
        _persist(info)

    manager = _manager(owner=(456, '10.0.0.9'))
    with pytest.raises(RuntimeError, match='incarnation changed'):
        manager._prune_superseded_failed_replicas()

    persisted = serve_state.get_replica_infos(_SERVICE_NAME)
    assert {info.replica_id for info in persisted} == {1, 2}
