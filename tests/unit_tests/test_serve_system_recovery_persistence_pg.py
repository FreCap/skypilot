"""Real-PostgreSQL tests for Serve system-recovery subdocuments."""
# pylint: disable=protected-access,redefined-outer-name,unused-import

import contextlib
import copy
import pickle
from typing import Any

import pytest
import sqlalchemy
from test_serve_resource_action_state_pg import postgres_engine

from sky.serve import constants
from sky.serve import paid_capacity
from sky.serve import replica_info
from sky.serve import serve_state
from sky.serve import serve_state_schema
from sky.serve import system_oom_recovery
from sky.serve import system_recovery_state as recovery_state
from sky.utils import common_utils

_SERVICE_NAME = 'svc'
_SERVICE_HASH = 'service-hash'
_OWNER = (123, '10.0.0.1')
_LIFECYCLE_EPOCH = 4
_WORKSPACE = 'default'


@pytest.fixture
def recovery_database(postgres_engine, monkeypatch):
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    serve_state_schema.Base.metadata.create_all(postgres_engine)
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


def _fence() -> dict[str, Any]:
    return {
        'expected_service_hash': _SERVICE_HASH,
        'expected_lifecycle_epoch': _LIFECYCLE_EPOCH,
        'expected_controller_owner': _OWNER,
    }


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
        **_fence())


def _unbound_context(
    intent: recovery_state.SystemRecoveryLaunchIntent,) -> dict[str, Any]:
    return system_oom_recovery.create_unbound_launch_context(
        intent,
        service_name=_SERVICE_NAME,
        service_version=3,
        controller_pid=_OWNER[0],
        controller_ip=_OWNER[1])


def _raw_replica_row(engine, replica_id: int) -> dict[str, Any]:
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == _SERVICE_NAME,
                serve_state.replicas_table.c.replica_id ==
                replica_id)).mappings().one()
    return dict(row)


def _assert_json_pickle_parity(engine, replica_id: int) -> None:
    row = _raw_replica_row(engine, replica_id)
    json_info = replica_info.ReplicaInfo.from_storage_dict(row['replica_state'])
    pickle_info = pickle.loads(row['replica_info'])
    assert pickle_info.to_storage_dict() == json_info.to_storage_dict()


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

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _record_statement)
    try:
        yield statements
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
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
            **_fence())
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
            **_fence())
    _assert_json_pickle_parity(engine, 7)

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
                                                  **_fence())
    assert exc.value.current_revision == 1

    demotion = copy.deepcopy(candidate)
    demotion.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.ORDINARY)
    demoted = serve_state.demote_replica_system_recovery_to_ordinary(
        _SERVICE_NAME, 7, demotion, expected_revision=1, **_fence())
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
                                                  **_fence())

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
                                                  **_fence())

    assert serve_state.add_or_update_replica(_SERVICE_NAME, 9, _replica(9),
                                             **_fence())
    partial = _raw_replica_row(engine, 9)['replica_state']
    partial.pop('service_job_id')
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == _SERVICE_NAME,
                serve_state.replicas_table.c.replica_id == 9).values(
                    replica_state=partial))
    quarantined = serve_state.get_replica_info_from_id(_SERVICE_NAME, 9)
    assert quarantined is not None
    assert quarantined.system_recovery_quarantine is not None
    clear_quarantine = copy.deepcopy(quarantined)
    clear_quarantine.system_recovery_quarantine = None
    with pytest.raises(serve_state.ReplicaSystemRecoveryMutationRejected,
                       match='absorbing'):
        serve_state.patch_replica_system_recovery(_SERVICE_NAME,
                                                  9,
                                                  clear_quarantine,
                                                  expected_revision=0,
                                                  **_fence())


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
    _assert_json_pickle_parity(engine, 7)


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
    _assert_json_pickle_parity(engine, 7)


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

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        serve_state.try_add_replica_with_paid_capacity_claim(
            _SERVICE_NAME,
            _SERVICE_HASH,
            7,
            _replica(7),
            pool_key='paid-pool',
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


def test_all_fields_absent_v13_rewrite_restores_json_pickle_parity(
        recovery_database) -> None:
    engine = recovery_database
    row = _raw_replica_row(engine, 7)
    rollback = row['replica_state']
    for field_name in replica_info.V13_ADDITIVE_STORAGE_FIELDS:
        rollback.pop(field_name)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == _SERVICE_NAME,
                serve_state.replicas_table.c.replica_id == 7).values(
                    replica_state=rollback))

    rewritten = serve_state.rewrite_rollback_replica_system_recovery_state(
        _SERVICE_NAME, **_fence())
    assert rewritten == 1
    completed = _raw_replica_row(engine, 7)['replica_state']
    assert set(replica_info.V13_ADDITIVE_STORAGE_FIELDS).issubset(completed)
    _assert_json_pickle_parity(engine, 7)


def test_exact_rollback_transition_identity_can_fence_delete(
        recovery_database) -> None:
    engine = recovery_database
    row = _raw_replica_row(engine, 7)
    rollback = row['replica_state']
    for field_name in replica_info.V13_ADDITIVE_STORAGE_FIELDS:
        rollback.pop(field_name)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.replicas_table).where(
                serve_state.replicas_table.c.service_name == _SERVICE_NAME,
                serve_state.replicas_table.c.replica_id == 7).values(
                    replica_state=rollback))

    transitioned = serve_state.get_replica_info_from_id(_SERVICE_NAME, 7)
    assert transitioned is not None
    with _capture_sql(engine) as statements:
        assert serve_state.remove_replica(
            _SERVICE_NAME,
            7,
            **_fence(),
            expected_replica_record_id=transitioned.replica_record_id)
    _assert_lifecycle_service_replica_lock_order(statements)
    assert serve_state.get_replica_info_from_id(_SERVICE_NAME, 7) is None
