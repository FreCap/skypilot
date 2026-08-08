"""PostgreSQL coverage for placement-normalization load receipts."""

# pylint: disable=protected-access,redefined-outer-name,unused-import
import uuid

import sqlalchemy
from sqlalchemy import event
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

import sky
from sky.serve import placement_contract_normalization
from sky.serve import placement_policy
from sky.serve import serve_state
from sky.serve import service_spec
from sky.utils.db import db_utils


def _fieldless_service_spec() -> bytes:
    spec = service_spec.SkyServiceSpec(readiness_path='/ready',
                                       initial_delay_seconds=1,
                                       readiness_timeout_seconds=2,
                                       endpoint_probe_interval_seconds=3,
                                       lb_stream_timeout_seconds=4,
                                       min_replicas=0)
    state = dict(spec.__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        state.pop(field)
    return placement_contract_normalization._serialize_raw_state(state,
                                                                 protocol=4)


def _api_pod_identity() -> placement_contract_normalization._ApiPodIdentity:
    return placement_contract_normalization._canonical_api_pod_identity(
        'pod-a', uuid.UUID('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'))


def _loaded_receipt(engine: sqlalchemy.engine.Engine,
                    service_name: str) -> tuple[object, ...]:
    table = serve_state.services_table
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.select(
                table.c.placement_normalization_loaded_run_id,
                table.c.placement_normalization_loaded_image_commit,
                table.c.placement_normalization_loaded_controller_pid,
                table.c.placement_normalization_loaded_controller_ip,
                table.c.placement_normalization_loaded_boot_id,
                table.c.placement_normalization_loaded_at,
            ).where(table.c.name == service_name)).one()
    return tuple(row)


def test_postgres_normalization_receipt_locks_and_cas_exact_owner(
        empty_postgres, monkeypatch):
    """A real pending ledger can be acknowledged through its PostgreSQL lock."""
    engine = empty_postgres
    serve_state.Base.metadata.create_all(engine)
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    monkeypatch.setattr(sky, '__commit__', 'a' * 40)

    service_name = 'svc-normalization-receipt-pg'
    service_hash = 'incarnation-a'
    lifecycle_epoch = 7
    owner = (123, '10.0.0.1')
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name=service_name,
            workspace='workspace',
            status=serve_state.ServiceStatus.READY.value,
            current_version=1,
            active_versions='[1]',
            pool=0,
            hash=service_hash,
            lifecycle_epoch=lifecycle_epoch,
            resource_scope=service_hash,
            controller_pid=owner[0],
            controller_ip=owner[1]))
        connection.execute(serve_state.version_specs_table.insert().values(
            service_name=service_name,
            version=1,
            spec=_fieldless_service_spec(),
            yaml_content='service: {}',
            created_at=1.0,
            created_by='test'))

    no_requests = placement_contract_normalization._ExternalEvidence(
        count=0, digest='0' * 64)
    normalization = placement_contract_normalization.run_operator(
        engine=engine,
        mode=placement_contract_normalization.ApplyMode.SUPPORTED,
        row_bound=10,
        freeze_evidence_sha256='f' * 64,
        request_evidence_getter=lambda _engine: no_requests,
        api_pod_checker=lambda _engine: _api_pod_identity())
    assert normalization.run_id is not None
    assert normalization.changed_rows == 1
    run_id = uuid.UUID(normalization.run_id)
    with engine.connect() as connection:
        manifest_completed_at = float(
            connection.execute(
                sqlalchemy.select(
                    serve_state.placement_normalization_runs_table.c.
                    completed_at).where(
                        serve_state.placement_normalization_runs_table.c.run_id
                        == run_id)).scalar_one())
    loaded_at = manifest_completed_at + 1.0

    request = serve_state.get_placement_normalization_request(
        service_name,
        recovery_version=1,
        current_version=1,
        expected_service_hash=service_hash,
        expected_controller_owner=owner)
    assert request == serve_state.PlacementNormalizationRequest(
        run_id=run_id,
        recovery_version=1,
        current_version=1,
        lifecycle_epoch=lifecycle_epoch)

    lock_engine = db_utils.get_postgres_lock_engine(engine)
    executed_statements: list[str] = []

    def capture_statement(_connection, _cursor, statement, _parameters,
                          _context, _executemany):
        executed_statements.append(statement)

    event.listen(lock_engine, 'before_cursor_execute', capture_statement)
    try:
        assert not serve_state.acknowledge_placement_normalization_loaded(
            service_name,
            request,
            expected_service_hash=service_hash,
            expected_controller_owner=(owner[0] + 1, owner[1]),
            image_commit='stale-owner',
            child_controller_pid=456,
            boot_id='b' * 32,
            loaded_at=loaded_at)
        assert _loaded_receipt(engine, service_name) == (None,) * 6

        stale_epoch_request = serve_state.PlacementNormalizationRequest(
            run_id=request.run_id,
            recovery_version=request.recovery_version,
            current_version=request.current_version,
            lifecycle_epoch=lifecycle_epoch + 1)
        assert not serve_state.acknowledge_placement_normalization_loaded(
            service_name,
            stale_epoch_request,
            expected_service_hash=service_hash,
            expected_controller_owner=owner,
            image_commit='stale-epoch',
            child_controller_pid=456,
            boot_id='c' * 32,
            loaded_at=loaded_at)
        assert _loaded_receipt(engine, service_name) == (None,) * 6

        assert serve_state.acknowledge_placement_normalization_loaded(
            service_name,
            request,
            expected_service_hash=service_hash,
            expected_controller_owner=owner,
            image_commit='commit-a',
            child_controller_pid=456,
            boot_id='d' * 32,
            loaded_at=loaded_at)
        first_postimage = _loaded_receipt(engine, service_name)
        assert not serve_state.acknowledge_placement_normalization_loaded(
            service_name,
            request,
            expected_service_hash=service_hash,
            expected_controller_owner=owner,
            image_commit='commit-b',
            child_controller_pid=789,
            boot_id='e' * 32,
            loaded_at=loaded_at + 10.0)
        assert _loaded_receipt(engine, service_name) == first_postimage
    finally:
        event.remove(lock_engine, 'before_cursor_execute', capture_statement)

    assert any('FOR UPDATE OF services' in ' '.join(statement.split())
               for statement in executed_statements)
    receipt_updates = [
        ' '.join(statement.split())
        for statement in executed_statements
        if statement.startswith('UPDATE services SET '
                                'placement_normalization_loaded_run_id=')
    ]
    assert len(receipt_updates) == 1
    for column in ('placement_normalization_loaded_run_id',
                   'placement_normalization_loaded_image_commit',
                   'placement_normalization_loaded_controller_pid',
                   'placement_normalization_loaded_controller_ip',
                   'placement_normalization_loaded_boot_id',
                   'placement_normalization_loaded_at'):
        assert f'{column} IS NULL' in receipt_updates[0]
    assert _loaded_receipt(engine, service_name) == (
        run_id,
        'commit-a',
        456,
        owner[1],
        'd' * 32,
        loaded_at,
    )
    assert serve_state.get_placement_normalization_request(
        service_name,
        recovery_version=1,
        current_version=1,
        expected_service_hash=service_hash,
        expected_controller_owner=owner) is None
