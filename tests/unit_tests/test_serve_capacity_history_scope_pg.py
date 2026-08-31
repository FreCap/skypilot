"""PostgreSQL contracts for bounded format-6 authority-history locks."""
# pylint: disable=not-callable,protected-access,redefined-outer-name,unused-import

import datetime
import json
import time
import uuid

import pytest
import sqlalchemy
from test_serve_capacity_admission_pg import _current_decision
from test_serve_capacity_admission_pg import _current_owner_kwargs
from test_serve_capacity_admission_pg import _demand_report
from test_serve_capacity_admission_pg import _enable_durable_intent
from test_serve_capacity_admission_pg import _paid_launch_spec
from test_serve_capacity_admission_pg import _publish_successor_route
from test_serve_capacity_admission_pg import _replica_values
from test_serve_capacity_admission_pg import capacity_database
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import capacity_admission
from sky.serve import capacity_admission_schema
from sky.serve import demand_state
from sky.serve import ordinary_launch_binding
from sky.serve import serve_state_schema

pytestmark = pytest.mark.xdist_group(
    name='serve_capacity_admission_schema_052_pg')

_HISTORY_SIZE = 9000


def _tombstone_values(
    service: dict,
    now: datetime.datetime,
    ordinal: int,
    *,
    replica_record_id: uuid.UUID | None = None,
    resolution: ordinary_launch_binding.Resolution = (
        ordinary_launch_binding.Resolution.PRE_EFFECT_TERMINAL),
) -> dict:
    association_id = uuid.uuid5(uuid.NAMESPACE_URL,
                                f'bounded-history-association-{ordinal}')
    record_id = replica_record_id or uuid.uuid5(
        uuid.NAMESPACE_URL, f'bounded-history-record-{ordinal}')
    values = {
        'association_id': association_id,
        'submission_id': uuid.uuid5(uuid.NAMESPACE_URL,
                                    f'bounded-history-submission-{ordinal}'),
        'tenant_scope': 'tenant-a',
        'service_name': 'svc',
        'service_hash': 'retained-old-hash',
        'service_workspace': 'workspace-a',
        'service_lifecycle_epoch': 2,
        'service_binding_epoch': 2,
        'service_version': 1,
        'replica_id': ordinal + 10000,
        'replica_record_id': record_id,
        'launch_generation': 1,
        'cluster_name': f'svc-old-{ordinal}',
        'request_id': f'old-request-{ordinal}',
        'input_digest': f'{ordinal:064x}',
        'owner_controller_incarnation': service['controller_incarnation'],
        'owner_controller_epoch': service['controller_owner_epoch'],
        'effect_phase': ordinary_launch_binding.EffectPhase.NOT_STARTED.value,
        'resolution': resolution.value,
        'terminal_status': ordinary_launch_binding.TerminalStatus.FAILED.value,
        'terminal_cause': 'request_never_executed',
        'terminal_execution_generation': 0,
        'execution_quiescence_required': True,
        'execution_quiesced_generation': 0,
        'execution_quiesced_at': now,
        'created_at': now,
        'updated_at': now,
    }
    if resolution is ordinary_launch_binding.Resolution.PRE_EFFECT_TERMINAL:
        values.update({
            'projected_at': now,
            'pin_released_at': now,
            'tombstone_not_before': now + datetime.timedelta(days=60),
        })
    elif resolution is ordinary_launch_binding.Resolution.RESULT_RECORDED:
        values.update({
            'effect_phase':
                ordinary_launch_binding.EffectPhase.SERVICE_JOB_RECORDED.value,
            'service_job_id': ordinal + 1,
            'result_recorded_at': now,
        })
    return values


def _insert_old_history(
    engine: sqlalchemy.engine.Engine,
    count: int,
    *,
    start_ordinal: int = 0,
    replica_record_id: uuid.UUID | None = None,
    resolution: ordinary_launch_binding.Resolution = (
        ordinary_launch_binding.Resolution.PRE_EFFECT_TERMINAL),
) -> list[dict]:
    associations = ordinary_launch_binding.ordinary_launch_associations_table
    with engine.begin() as connection:
        service = dict(
            connection.execute(
                sqlalchemy.select(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    'svc')).mappings().one())
        now = connection.execute(
            sqlalchemy.select(sqlalchemy.func.clock_timestamp())).scalar_one()
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        values = [
            _tombstone_values(service,
                              now,
                              ordinal,
                              replica_record_id=replica_record_id,
                              resolution=resolution)
            for ordinal in range(start_ordinal, start_ordinal + count)
        ]
        for offset in range(0, count, 500):
            connection.execute(sqlalchemy.insert(associations),
                               values[offset:offset + 500])
    return values


def _reconcile(engine: sqlalchemy.engine.Engine,
               target: int = 0,
               *,
               prepared_specs=()):
    return capacity_admission.CapacityAdmissionRepository(
        engine).plan_and_admit_current(
            **_current_owner_kwargs(engine),
            service_name='svc',
            service_hash='svc-hash',
            service_lifecycle_epoch=3,
            service_version=1,
            accounting_cards={'l4': 1},
            backend_num_nodes=1,
            sequenced_reserved_fill=False,
            planner=lambda snapshot, supply: _current_decision(
                snapshot, supply, target),
            prepared_paid_launch_specs=prepared_specs)


def _postgres_plan_nodes(document) -> list[dict]:
    if isinstance(document, str):
        document = json.loads(document)
    pending = [document[0]['Plan']]
    nodes = []
    while pending:
        node = pending.pop()
        nodes.append(node)
        pending.extend(node.get('Plans', ()))
    return nodes


def _refresh_authority(engine: sqlalchemy.engine.Engine, incarnation: uuid.UUID,
                       sequence: int) -> None:
    """Refresh the causally paired route and demand leases for a long test."""
    route = _publish_successor_route(engine, incarnation, 9000 + sequence)
    demand_state.ingest_report(
        'svc', 'svc-hash', _demand_report(time.time(), route,
                                          sequence=sequence))


def _rewrite_current_head_as_format_5(engine: sqlalchemy.engine.Engine) -> None:
    """Install an authenticated head emitted by the pre-census format."""
    plans = capacity_admission_schema.serve_capacity_plans_table
    heads = capacity_admission_schema.serve_capacity_plan_heads_table
    with engine.begin() as connection:
        head = connection.execute(
            sqlalchemy.select(heads).where(
                heads.c.service_name == 'svc')).mappings().one()
        plan = connection.execute(
            sqlalchemy.select(plans).where(
                plans.c.service_name == 'svc',
                plans.c.generation == head['generation'])).mappings().one()
        payload = json.loads(json.dumps(plan['payload']))
        payload['planner']['schema_version'] = 5
        digest = capacity_admission.capacity_plan_content_sha256(payload)
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        connection.execute(
            sqlalchemy.update(plans).where(
                plans.c.service_name == 'svc',
                plans.c.generation == head['generation']).values(
                    payload=payload, content_sha256=digest))


def _reset_capacity_history(engine: sqlalchemy.engine.Engine) -> None:
    """Model the authorized exact-zero test-service recreation cutover."""
    plans = capacity_admission_schema.serve_capacity_plans_table
    heads = capacity_admission_schema.serve_capacity_plan_heads_table
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        connection.execute(
            sqlalchemy.delete(heads).where(heads.c.service_name == 'svc'))
        connection.execute(
            sqlalchemy.delete(plans).where(plans.c.service_name == 'svc'))


def test_validated_head_bounds_nine_thousand_tombstone_lock(capacity_database):
    engine, incarnation, _ = capacity_database
    _enable_durable_intent(engine, incarnation, reserved_fill_enabled=False)

    # Format 5 predates the exhaustive genesis census and can never serve as a
    # bounded-history receipt.  Even an otherwise authenticated clean head is
    # scanned exhaustively and then strict-rejected; it is never upgraded in
    # place.  A retained old-incarnation effect is found by that full census.
    _refresh_authority(engine, incarnation, 2)
    _reconcile(engine)
    _rewrite_current_head_as_format_5(engine)
    legacy_scopes = []

    def _capture_legacy_scope(_connection, _cursor, statement, _parameters,
                              _context, _executemany):
        if ('FROM serve_ordinary_launch_associations' in statement and
                'FOR UPDATE' in statement):
            legacy_scopes.append(statement)

    sqlalchemy.event.listen(engine, 'after_cursor_execute',
                            _capture_legacy_scope)
    try:
        _refresh_authority(engine, incarnation, 3)
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='not strict format 6'):
            _reconcile(engine)
        _refresh_authority(engine, incarnation, 4)
        retained_authority = _insert_old_history(
            engine,
            1,
            start_ordinal=_HISTORY_SIZE + 20,
            resolution=ordinary_launch_binding.Resolution.RESULT_RECORDED)[0]
        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='belongs to another service incarnation'):
            _reconcile(engine)
    finally:
        sqlalchemy.event.remove(engine, 'after_cursor_execute',
                                _capture_legacy_scope)
    assert len(legacy_scopes) == 2
    assert all('bounded_association_authority_scope' not in statement
               for statement in legacy_scopes)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        connection.execute(
            sqlalchemy.delete(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == retained_authority['association_id']))
    _reset_capacity_history(engine)

    _insert_old_history(engine, _HISTORY_SIZE)

    # Headless format-6 genesis is the one exhaustive history census.
    _refresh_authority(engine, incarnation, 5)
    _reconcile(engine)
    _refresh_authority(engine, incarnation, 6)

    observed = []

    def _capture(_connection, cursor, statement, parameters, _context,
                 _executemany):
        if ('bounded_association_authority_scope' in statement and
                'FOR UPDATE' in statement):
            observed.append((statement, parameters, cursor.rowcount))

    sqlalchemy.event.listen(engine, 'after_cursor_execute', _capture)
    try:
        _reconcile(engine)
        # Replica numbers are lifecycle-local.  Reusing a number from a settled
        # old incarnation must not collide, while the UUID selector must reach
        # psycopg as a native UUID bind.
        numeric_reuse = _paid_launch_spec(engine, 0, 10000)
        reused = _reconcile(engine, 0, prepared_specs=(numeric_reuse,))
    finally:
        sqlalchemy.event.remove(engine, 'after_cursor_execute', _capture)
    assert not reused.paid_launch_receipt.members

    with engine.connect() as connection:
        retained = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                ordinary_launch_binding.ordinary_launch_associations_table)
        ).scalar_one()
    assert retained == _HISTORY_SIZE
    assert len(observed) == 2
    statement, parameters, locked_count = observed[0]
    assert 'resolution IN' in statement
    assert len(parameters) < 20
    assert locked_count == 0
    with engine.connect() as connection:
        plan_document = connection.exec_driver_sql(
            f'EXPLAIN (FORMAT JSON) {statement}', parameters).scalar_one()
    plan_nodes = _postgres_plan_nodes(plan_document)
    assert not any(
        node.get('Node Type') == 'Seq Scan' and
        node.get('Relation Name') == 'serve_ordinary_launch_associations'
        for node in plan_nodes)
    assert any(
        node.get('Index Name') == 'uq_serve_ordinary_binding_unsettled'
        for node in plan_nodes)
    candidate_statement, candidate_parameters, candidate_locked_count = (
        observed[1])
    assert 'replica_record_id IN' in candidate_statement
    assert candidate_locked_count == 0
    assert uuid.UUID(numeric_reuse.replica_record_id) in set(
        candidate_parameters.values())

    # A settled old row becomes active again only if corrupt retained state
    # attaches it to a live graph.  The scalar pointer is selected even when
    # the replica record ID itself is different.
    tombstone = _insert_old_history(engine, 1, start_ordinal=_HISTORY_SIZE)[0]
    replica = _replica_values(410, zero_cost=True)
    replica['replica_state']['replica_record_id'] = str(uuid.uuid4())
    replica['ordinary_launch_association_id'] = tombstone['association_id']
    with engine.begin() as connection:
        # Install a deliberately corrupt retained graph to verify the read-side
        # fail-closed fence independently of the normal write-side trigger.
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**replica))

    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='association pointer'):
        _reconcile(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        connection.execute(
            sqlalchemy.delete(serve_state_schema.replicas_table).where(
                serve_state_schema.replicas_table.c.service_name == 'svc',
                serve_state_schema.replicas_table.c.replica_id == 410))
        connection.execute(
            sqlalchemy.delete(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == tombstone['association_id']))

    def _assert_corrupt_pointer_rejected(case: str, ordinal: int,
                                         replica_ids: tuple[int, ...]) -> None:
        association = None
        pointer = uuid.uuid5(uuid.NAMESPACE_URL,
                             f'bounded-history-{case}-pointer')
        with engine.begin() as connection:
            service = dict(
                connection.execute(
                    sqlalchemy.select(serve_state_schema.services_table).where(
                        serve_state_schema.services_table.c.name ==
                        'svc')).mappings().one())
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            connection.exec_driver_sql(
                "SET LOCAL session_replication_role = 'replica'")
            if case != 'dangling':
                association = _tombstone_values(service, now, ordinal)
                association.update({
                    'replica_id': replica_ids[0],
                    'service_lifecycle_epoch': 3,
                    'service_binding_epoch': int(
                        service['ordinary_launch_binding_epoch']),
                })
                if case == 'cross-service':
                    association.update({
                        'service_name': 'other-service',
                        'service_hash': 'other-hash',
                    })
                else:
                    association['service_hash'] = 'svc-hash'
                pointer = association['association_id']
                connection.execute(
                    sqlalchemy.insert(
                        ordinary_launch_binding.
                        ordinary_launch_associations_table).values(
                            **association))
            for index, replica_id in enumerate(replica_ids):
                values = _replica_values(replica_id, zero_cost=True)
                if association is not None and (case != 'record-mismatch' or
                                                index > 0):
                    values['replica_state']['replica_record_id'] = str(
                        association['replica_record_id'])
                values['ordinary_launch_association_id'] = pointer
                connection.execute(
                    sqlalchemy.insert(
                        serve_state_schema.replicas_table).values(**values))

        with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                           match='association pointer'):
            _reconcile(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "SET LOCAL session_replication_role = 'replica'")
            connection.execute(
                sqlalchemy.delete(serve_state_schema.replicas_table).where(
                    serve_state_schema.replicas_table.c.service_name == 'svc',
                    serve_state_schema.replicas_table.c.replica_id.in_(
                        replica_ids)))
            if association is not None:
                connection.execute(
                    sqlalchemy.delete(
                        ordinary_launch_binding.
                        ordinary_launch_associations_table).where(
                            ordinary_launch_binding.
                            ordinary_launch_associations_table.c.association_id
                            == pointer))

    _assert_corrupt_pointer_rejected('dangling', _HISTORY_SIZE + 10, (411,))
    _assert_corrupt_pointer_rejected('cross-service', _HISTORY_SIZE + 11,
                                     (412,))
    _assert_corrupt_pointer_rejected('record-mismatch', _HISTORY_SIZE + 12,
                                     (413,))
    _assert_corrupt_pointer_rejected('duplicate', _HISTORY_SIZE + 13,
                                     (414, 415))

    # RESULT_RECORDED is still provider-possible/unsettled authority and must
    # never disappear behind the settled-history optimization.
    _refresh_authority(engine, incarnation, 7)
    result_recorded = _insert_old_history(
        engine,
        1,
        start_ordinal=_HISTORY_SIZE + 1,
        resolution=ordinary_launch_binding.Resolution.RESULT_RECORDED)[0]
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='belongs to another service incarnation'):
        _reconcile(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        connection.execute(
            sqlalchemy.delete(
                ordinary_launch_binding.ordinary_launch_associations_table).
            where(ordinary_launch_binding.ordinary_launch_associations_table.c.
                  association_id == result_recorded['association_id']))

    # Exact UUID collision scope is cross-incarnation; lifecycle-local numeric
    # replica IDs are intentionally not.
    _refresh_authority(engine, incarnation, 8)
    spec = _paid_launch_spec(engine, 0, 420)
    _insert_old_history(engine,
                        1,
                        start_ordinal=_HISTORY_SIZE + 2,
                        replica_record_id=uuid.UUID(spec.replica_record_id))
    with pytest.raises(capacity_admission.CapacityAdmissionConflict,
                       match='belongs to another service incarnation'):
        _reconcile(engine, 1, prepared_specs=(spec,))

    # Every supported association/request-root creator first acquires the same
    # service row that the planner holds.  Prove the real insert entry point
    # cannot cross that mutex; request/queue/pin insertion follows it on the same
    # transaction in the production wrapper.
    writer_replica = _replica_values(777, zero_cost=True)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.replicas_table).values(**writer_replica))
        service = dict(
            connection.execute(
                sqlalchemy.select(serve_state_schema.services_table).where(
                    serve_state_schema.services_table.c.name ==
                    'svc')).mappings().one())
    record_id = uuid.UUID(writer_replica['replica_state']['replica_record_id'])
    intent = ordinary_launch_binding.BindingIntent(
        service_name='svc',
        service_hash='svc-hash',
        service_version=1,
        replica_id=777,
        replica_record_id=record_id,
        lifecycle_epoch=3,
        binding_epoch=int(service['ordinary_launch_binding_epoch']),
        controller_incarnation=service['controller_incarnation'],
        controller_owner_epoch=int(service['controller_owner_epoch']),
        controller_pid=service['controller_pid'],
        controller_ip=service['controller_ip'])
    identity = ordinary_launch_binding.build_binding_identity(
        intent,
        submission_id=uuid.uuid4(),
        tenant_scope='tenant-a',
        service_workspace='workspace-a',
        cluster_name=writer_replica['cluster_name'],
        input_digest='f' * 64)
    blocker = engine.connect()
    contender = engine.connect()
    blocker_transaction = blocker.begin()
    contender_transaction = contender.begin()
    try:
        blocker.execute(
            sqlalchemy.select(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                'svc').with_for_update()).one()
        contender.exec_driver_sql("SET LOCAL lock_timeout = '100ms'")
        with pytest.raises(sqlalchemy.exc.OperationalError,
                           match='lock timeout'):
            ordinary_launch_binding.insert_or_get_locked(contender, identity)
    finally:
        contender_transaction.rollback()
        blocker_transaction.rollback()
        contender.close()
        blocker.close()
