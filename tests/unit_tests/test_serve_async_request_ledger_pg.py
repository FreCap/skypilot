"""PostgreSQL contracts for the exact asynchronous request ledger."""
# pylint: disable=not-callable,protected-access,redefined-outer-name
# pylint: disable=unused-import

import concurrent.futures
import hashlib
import time
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky import global_user_state_schema
from sky.serve import async_request_ledger
from sky.serve import async_request_ledger_schema
from sky.serve import placement_normalization_authority
from sky.serve import route_projection
from sky.serve import serve_state_schema
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(name='serve_async_request_ledger_058_pg')

_SERVICE_NAME = 'svc'
_SERVICE_HASH = 'svc-hash'
_REQUEST_ID = 'durable-request-1'
_INTENT = 'a' * 64
_CONTROLLER_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_RECORD_ID = uuid.UUID('22222222-2222-4222-8222-222222222222')
_NEW_RECORD_ID = uuid.UUID('33333333-3333-4333-8333-333333333333')
_ROUTE_URL = 'http://10.0.0.10:8080'
_NEW_ROUTE_URL = 'http://10.0.0.11:8080'


def _publisher_identity() -> route_projection.RoutePublisherIdentity:
    return route_projection.RoutePublisherIdentity(
        service_name=_SERVICE_NAME,
        service_hash=_SERVICE_HASH,
        service_lifecycle_epoch=4,
        controller_incarnation=_CONTROLLER_ID,
        controller_owner_epoch=6,
        controller_pid=123,
        controller_ip='10.0.0.2')


def _route_response() -> dict:
    return {
        'replica_info': {
            _ROUTE_URL: {
                'gpu_type': 'L4',
                'gpu_count': '1',
                'is_zero_cost': 'false',
            }
        },
        'num_ready_replicas': 1,
        'routing_spec': {
            'load_balancing_policy_name': 'round_robin'
        },
        'capacity_hint': {
            'replica_unit': 'physical_backend'
        },
        'request_history_accepted': False,
        'request_classification_history_accepted': False,
        'response_time_history_accepted': False,
        'prediction_time_history_accepted': False,
        'queued_compatibility_demand_supported': True,
        'service_version': 1,
    }


@pytest.fixture
def ledger_database(empty_postgres):
    serve_config = migration_utils.get_alembic_config(
        empty_postgres, migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(serve_config, migration_utils.SERVE_VERSION)
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.insert(global_user_state_schema.user_table).values(
                id='owner-a', name='Owner A', created_at=int(time.time())))
        connection.execute(
            sqlalchemy.insert(
                serve_state_schema.service_lifecycle_fences_table).values(
                    name=_SERVICE_NAME, epoch=4))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.services_table).values(
                name=_SERVICE_NAME,
                workspace='workspace-a',
                status='READY',
                hash=_SERVICE_HASH,
                current_version=1,
                active_versions='[1]',
                pool=0,
                controller_pid=123,
                controller_ip='10.0.0.2',
                lifecycle_epoch=4,
                controller_incarnation=_CONTROLLER_ID,
                controller_owner_epoch=6,
                owner_user_id='owner-a',
                owner_user_name='Owner A'))
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name=_SERVICE_NAME,
                replica_id=1,
                replica_state_version=18,
                status='READY',
                version=1,
                cluster_name='svc-1',
                is_spot=False,
                replica_state={
                    'replica_record_id': str(_RECORD_ID),
                    'is_zero_cost': False,
                    'location': {
                        'cloud': 'aws',
                        'region': 'us-east-1',
                        'zone': 'us-east-1a',
                    },
                }))
    publication = route_projection.RouteProjectionRepository(
        empty_postgres).publish(_publisher_identity(),
                                1,
                                _route_response(), {
                                    _ROUTE_URL: {
                                        'replica_id': 1,
                                        'replica_record_id': str(_RECORD_ID),
                                        'service_version': 1,
                                        'gpu_type': 'L4',
                                        'gpu_count': 1,
                                        'advertised': True,
                                        'alias_expires_at': None,
                                    }
                                }, {str(_RECORD_ID)},
                                ttl_seconds=300)
    with empty_postgres.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE_NAME).values(
                    route_source_mode='DURABLE_PROJECTED',
                    route_source_epoch=1,
                    route_projection_capable=True,
                    route_projection_controller_incarnation=_CONTROLLER_ID,
                    route_projection_protocol_version=1))
    return empty_postgres, publication


def _bind_payload(publication,
                  *,
                  request_id=_REQUEST_ID,
                  route_contract_service_version=1,
                  selected_route_url=_ROUTE_URL) -> dict:
    return {
        'protocol_version': 1,
        'request_id': request_id,
        'intent_sha256': _INTENT,
        'route_contract_service_version': route_contract_service_version,
        'route_projection_generation': publication.generation,
        'route_projection_sha256': publication.content_sha256,
        'route_source_epoch': 1,
        'selected_route_url': selected_route_url,
        'allow_new_attempt': True,
    }


def _transition_payload(receipt, operation: str, **extra) -> dict:
    return {
        'protocol_version': 1,
        'operation': operation,
        'request_id': _REQUEST_ID,
        'intent_sha256': _INTENT,
        'attempt_id': receipt.attempt_id,
        'attempt_no': receipt.attempt_no,
        'expected_revision': receipt.revision,
        **extra,
    }


def test_migration_upgrades_released_057_directly_to_058(
        empty_postgres) -> None:
    serve_config = migration_utils.get_alembic_config(
        empty_postgres, migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(serve_config, '057')
    with empty_postgres.connect() as connection:
        assert connection.execute(
            sqlalchemy.text(
                'SELECT version_num FROM alembic_version_serve_state_db')
        ).scalar_one() == '057'
    inspector = sqlalchemy.inspect(empty_postgres)
    assert not inspector.has_table('serve_async_requests')
    assert not inspector.has_table('serve_async_request_attempts')

    alembic_command.upgrade(serve_config, '058')
    with empty_postgres.connect() as connection:
        assert connection.execute(
            sqlalchemy.text(
                'SELECT version_num FROM alembic_version_serve_state_db')
        ).scalar_one() == '058'
    inspector = sqlalchemy.inspect(empty_postgres)
    assert inspector.has_table('serve_async_requests')
    assert inspector.has_table('serve_async_request_attempts')


def test_migration_058_has_normalized_deferred_attempt_identity(
        ledger_database) -> None:
    engine, _ = ledger_database
    inspector = sqlalchemy.inspect(engine)
    with engine.connect() as connection:
        revision = connection.execute(
            sqlalchemy.text(
                'SELECT version_num FROM alembic_version_serve_state_db')
        ).scalar_one()
    assert revision == '058'
    assert inspector.has_table('serve_async_requests')
    assert inspector.has_table('serve_async_request_attempts')
    attempt_fks = inspector.get_foreign_keys('serve_async_request_attempts')
    request_fks = inspector.get_foreign_keys('serve_async_requests')
    assert any(foreign_key['referred_table'] == 'serve_async_requests'
               for foreign_key in attempt_fks)
    assert any(foreign_key['referred_table'] == 'serve_async_request_attempts'
               for foreign_key in request_fks)


def test_migration_058_remains_additive_to_frozen_placement_authority(
        ledger_database) -> None:
    engine, _ = ledger_database
    with engine.begin() as connection:
        authority = (placement_normalization_authority.
                     assert_reader_database_authority(connection))
    assert authority.schema == 'public'


def test_bind_is_exact_private_and_duplicate_never_authorizes_dispatch(
        ledger_database) -> None:
    engine, publication = ledger_database
    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)

    first = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                            _bind_payload(publication))
    duplicate = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                                _bind_payload(publication))

    assert first.dispatch_authorized is True
    assert first.duplicate is False
    assert duplicate.dispatch_authorized is False
    assert duplicate.duplicate is True
    assert duplicate.attempt_id == first.attempt_id
    with engine.connect() as connection:
        request_row = connection.execute(
            sqlalchemy.select(async_request_ledger_schema.
                              serve_async_requests_table)).mappings().one()
        attempt_row = connection.execute(
            sqlalchemy.select(
                async_request_ledger_schema.serve_async_request_attempts_table)
        ).mappings().one()
    assert request_row['request_key_sha256'] == hashlib.sha256(
        _REQUEST_ID.encode()).hexdigest()
    assert _REQUEST_ID not in str(dict(request_row))
    assert _ROUTE_URL not in str(dict(attempt_row))
    assert attempt_row['dispatch_binding']['replica_record_id'] == str(
        _RECORD_ID)
    assert attempt_row['dispatch_binding']['projected_accelerator'] == 'L4'
    assert attempt_row['dispatch_binding'][
        'route_contract_service_version'] == 1
    assert attempt_row['dispatch_binding'][
        'selected_worker_service_version'] == 1


def test_current_projection_binds_each_active_worker_version(
        ledger_database) -> None:
    engine, _ = ledger_database
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.insert(serve_state_schema.replicas_table).values(
                service_name=_SERVICE_NAME,
                replica_id=2,
                replica_state_version=18,
                status='READY',
                version=2,
                cluster_name='svc-2',
                is_spot=False,
                replica_state={
                    'replica_record_id': str(_NEW_RECORD_ID),
                    'is_zero_cost': False,
                    'location': {
                        'cloud': 'aws',
                        'region': 'us-east-1',
                        'zone': 'us-east-1a',
                    },
                }))
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE_NAME).values(current_version=2,
                                      active_versions='[1, 2]'))
    response = _route_response()
    response['service_version'] = 2
    response['replica_info'][_NEW_ROUTE_URL] = {
        'gpu_type': 'L4',
        'gpu_count': '1',
        'is_zero_cost': 'false',
    }
    publication = route_projection.RouteProjectionRepository(engine).publish(
        _publisher_identity(),
        2,
        response, {
            _ROUTE_URL: {
                'replica_id': 1,
                'replica_record_id': str(_RECORD_ID),
                'service_version': 1,
                'gpu_type': 'L4',
                'gpu_count': 1,
                'advertised': True,
                'alias_expires_at': None,
            },
            _NEW_ROUTE_URL: {
                'replica_id': 2,
                'replica_record_id': str(_NEW_RECORD_ID),
                'service_version': 2,
                'gpu_type': 'L4',
                'gpu_count': 1,
                'advertised': True,
                'alias_expires_at': None,
            },
        }, {str(_RECORD_ID), str(_NEW_RECORD_ID)},
        ttl_seconds=300)
    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)

    old_worker = repository.bind(
        _SERVICE_NAME, _SERVICE_HASH,
        _bind_payload(publication,
                      request_id='old-worker-request',
                      route_contract_service_version=2))
    new_worker = repository.bind(
        _SERVICE_NAME, _SERVICE_HASH,
        _bind_payload(publication,
                      request_id='new-worker-request',
                      route_contract_service_version=2,
                      selected_route_url=_NEW_ROUTE_URL))

    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                async_request_ledger_schema.serve_async_request_attempts_table.
                c.dispatch_binding).order_by(async_request_ledger_schema.
                                             serve_async_request_attempts_table.
                                             c.created_at)).scalars().all()
    bindings_by_replica = {row['replica_id']: row for row in rows}
    assert old_worker.dispatch_authorized is True
    assert new_worker.dispatch_authorized is True
    assert bindings_by_replica[1]['route_contract_service_version'] == 2
    assert bindings_by_replica[1]['selected_worker_service_version'] == 1
    assert bindings_by_replica[2]['route_contract_service_version'] == 2
    assert bindings_by_replica[2]['selected_worker_service_version'] == 2

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE_NAME).values(active_versions='[2]'))
    with pytest.raises(async_request_ledger.AsyncRequestLedgerConflict,
                       match='no longer active'):
        repository.bind(
            _SERVICE_NAME, _SERVICE_HASH,
            _bind_payload(publication,
                          request_id='inactive-worker-request',
                          route_contract_service_version=2))


def test_read_only_bind_lookup_creates_nothing_and_recovers_without_route(
        ledger_database) -> None:
    engine, publication = ledger_database
    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)
    lookup = {
        'protocol_version': 1,
        'request_id': _REQUEST_ID,
        'intent_sha256': _INTENT,
        'allow_new_attempt': False,
    }

    with pytest.raises(async_request_ledger.AsyncRequestLedgerNotFound):
        repository.lookup_current(_SERVICE_NAME, _SERVICE_HASH, lookup)
    with engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                async_request_ledger_schema.serve_async_requests_table)
        ).scalar_one() == 0

    bound = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                            _bind_payload(publication))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE_NAME).values(status='SHUTTING_DOWN'))

    recovered = repository.lookup_current(_SERVICE_NAME, _SERVICE_HASH, lookup)
    duplicate = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                                _bind_payload(publication))
    assert recovered.attempt_id == bound.attempt_id
    assert recovered.duplicate is True
    assert recovered.dispatch_authorized is False
    assert duplicate.attempt_id == bound.attempt_id
    assert duplicate.dispatch_authorized is False

    with pytest.raises(async_request_ledger.AsyncRequestLedgerConflict):
        repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                        _bind_payload(publication, request_id='new-request'))


def test_ambiguous_attempt_never_authorizes_replay(ledger_database) -> None:
    engine, publication = ledger_database
    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)

    bound = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                            _bind_payload(publication))
    ambiguous = repository.transition(_SERVICE_NAME, _SERVICE_HASH,
                                      _transition_payload(bound, 'ambiguous'))
    duplicate = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                                _bind_payload(publication))

    assert ambiguous.state == 'AMBIGUOUS'
    assert duplicate.attempt_id == bound.attempt_id
    assert duplicate.attempt_no == 1
    assert duplicate.dispatch_authorized is False
    with engine.connect() as connection:
        attempt_count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                async_request_ledger_schema.serve_async_request_attempts_table)
        ).scalar_one()
    assert attempt_count == 1


def test_terminal_receipt_is_attempt_fenced_and_idempotent(
        ledger_database) -> None:
    engine, publication = ledger_database
    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)
    bound = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                            _bind_payload(publication))

    terminal = repository.transition(
        _SERVICE_NAME, _SERVICE_HASH,
        _transition_payload(bound,
                            'terminal',
                            terminal_status='SUCCEEDED',
                            processing_time_us=123))
    duplicate = repository.transition(
        _SERVICE_NAME, _SERVICE_HASH,
        _transition_payload(terminal,
                            'terminal',
                            terminal_status='SUCCEEDED',
                            processing_time_us=123))
    assert terminal.state == 'SUCCEEDED'
    assert duplicate.duplicate is True
    summary = repository.summary(_SERVICE_NAME, _SERVICE_HASH)
    assert summary['operational_terminal_receipt_total'] == 1
    assert summary['operational_terminal_receipts_by_status']['SUCCEEDED'] == 1


def test_shutdown_blocks_bind_and_service_recreation_fences_transitions(
        ledger_database) -> None:
    engine, publication = ledger_database
    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)
    bound = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                            _bind_payload(publication))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE_NAME).values(status='SHUTTING_DOWN'))
    with pytest.raises(async_request_ledger.AsyncRequestLedgerConflict):
        repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                        _bind_payload(publication, request_id='new-request'))
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE_NAME).values(status='FAILED_CLEANUP'))
    with pytest.raises(async_request_ledger.AsyncRequestLedgerConflict):
        repository.bind(
            _SERVICE_NAME, _SERVICE_HASH,
            _bind_payload(publication, request_id='another-request'))

    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE_NAME).values(hash='replacement-hash', status='READY'))
    with pytest.raises(async_request_ledger.AsyncRequestLedgerConflict):
        repository.transition(_SERVICE_NAME, _SERVICE_HASH,
                              _transition_payload(bound, 'accepted'))


def test_database_constraint_rejects_unknown_attempt_state(
        ledger_database) -> None:
    engine, publication = ledger_database
    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)
    bound = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                            _bind_payload(publication))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.update(
                    async_request_ledger_schema.
                    serve_async_request_attempts_table).where(
                        async_request_ledger_schema.
                        serve_async_request_attempts_table.c.attempt_id ==
                        uuid.UUID(bound.attempt_id)).values(state='UNKNOWN',
                                                            revision=2))


def test_database_guards_reject_invalid_initial_request_and_attempt(
        ledger_database) -> None:
    engine, publication = ledger_database
    invalid_attempt_id = uuid.uuid4()
    invalid_request_key = hashlib.sha256(b'invalid-request').hexdigest()
    with pytest.raises(sqlalchemy.exc.IntegrityError,
                       match='must start at attempt one'):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.insert(async_request_ledger_schema.
                                  serve_async_requests_table).values(
                                      service_name=_SERVICE_NAME,
                                      service_hash=_SERVICE_HASH,
                                      request_key_sha256=invalid_request_key,
                                      intent_sha256=_INTENT,
                                      current_attempt_id=invalid_attempt_id,
                                      current_attempt_no=2))

    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)
    repository.bind(_SERVICE_NAME, _SERVICE_HASH, _bind_payload(publication))
    with engine.connect() as connection:
        valid_binding = connection.execute(
            sqlalchemy.select(
                async_request_ledger_schema.serve_async_request_attempts_table.
                c.dispatch_binding)).scalar_one()
    invalid_attempt_id = uuid.uuid4()
    with pytest.raises(sqlalchemy.exc.IntegrityError,
                       match='invalid initial state'):
        with engine.begin() as connection:
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            connection.execute(
                sqlalchemy.insert(async_request_ledger_schema.
                                  serve_async_requests_table).values(
                                      service_name=_SERVICE_NAME,
                                      service_hash=_SERVICE_HASH,
                                      request_key_sha256=invalid_request_key,
                                      intent_sha256=_INTENT,
                                      current_attempt_id=invalid_attempt_id,
                                      current_attempt_no=1,
                                      created_at=now,
                                      updated_at=now))
            connection.execute(
                sqlalchemy.insert(async_request_ledger_schema.
                                  serve_async_request_attempts_table).values(
                                      service_name=_SERVICE_NAME,
                                      service_hash=_SERVICE_HASH,
                                      request_key_sha256=invalid_request_key,
                                      attempt_id=invalid_attempt_id,
                                      attempt_no=1,
                                      state='ACCEPTED',
                                      revision=7,
                                      dispatch_binding=valid_binding,
                                      accepted_at=now,
                                      created_at=now,
                                      updated_at=now))


def test_only_predispatch_rejection_authorizes_successor(
        ledger_database) -> None:
    engine, publication = ledger_database
    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)
    bound = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                            _bind_payload(publication))

    rejected = repository.transition(_SERVICE_NAME, _SERVICE_HASH,
                                     _transition_payload(bound, 'rejected'))
    successor = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                                _bind_payload(publication))

    assert rejected.state == 'REJECTED_PRE_DISPATCH'
    assert successor.attempt_no == 2
    assert successor.dispatch_authorized is True
    with engine.connect() as connection:
        rows = connection.execute(
            sqlalchemy.select(
                async_request_ledger_schema.serve_async_request_attempts_table).
            order_by(
                async_request_ledger_schema.serve_async_request_attempts_table.
                c.attempt_no)).mappings().all()
    assert [row['state'] for row in rows
           ] == ['REJECTED_PRE_DISPATCH', 'DISPATCH_MAY_HAVE_OCCURRED']


def test_current_incarnation_receipts_cannot_be_deleted(
        ledger_database) -> None:
    engine, publication = ledger_database
    async_request_ledger.AsyncRequestLedgerRepository(engine).bind(
        _SERVICE_NAME, _SERVICE_HASH, _bind_payload(publication))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.delete(
                    async_request_ledger_schema.serve_async_requests_table))
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.delete(async_request_ledger_schema.
                                  serve_async_request_attempts_table))


def test_pointer_cannot_advance_from_an_active_attempt(ledger_database) -> None:
    engine, publication = ledger_database
    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)
    bound = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                            _bind_payload(publication))
    next_attempt_id = uuid.uuid4()
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            current = connection.execute(
                sqlalchemy.select(
                    async_request_ledger_schema.
                    serve_async_request_attempts_table).where(
                        async_request_ledger_schema.
                        serve_async_request_attempts_table.c.attempt_id ==
                        uuid.UUID(bound.attempt_id))).mappings().one()
            now = connection.execute(
                sqlalchemy.select(
                    sqlalchemy.func.clock_timestamp())).scalar_one()
            connection.execute(
                sqlalchemy.insert(
                    async_request_ledger_schema.
                    serve_async_request_attempts_table).values(
                        service_name=_SERVICE_NAME,
                        service_hash=_SERVICE_HASH,
                        request_key_sha256=bound.request_key_sha256,
                        attempt_id=next_attempt_id,
                        attempt_no=2,
                        state='DISPATCH_MAY_HAVE_OCCURRED',
                        revision=1,
                        dispatch_binding=current['dispatch_binding'],
                        created_at=now,
                        updated_at=now))
            connection.execute(
                sqlalchemy.update(async_request_ledger_schema.
                                  serve_async_requests_table).values(
                                      current_attempt_id=next_attempt_id,
                                      current_attempt_no=2,
                                      updated_at=now))


def test_summary_reports_exact_opted_in_states_and_terminal_counts(
        ledger_database) -> None:
    engine, publication = ledger_database
    repository = async_request_ledger.AsyncRequestLedgerRepository(engine)
    bound = repository.bind(_SERVICE_NAME, _SERVICE_HASH,
                            _bind_payload(publication))
    accepted = repository.transition(_SERVICE_NAME, _SERVICE_HASH,
                                     _transition_payload(bound, 'accepted'))
    accepted_summary = repository.summary(_SERVICE_NAME, _SERVICE_HASH)
    assert accepted_summary['source'] == 'postgresql_async_request_ledger'
    assert accepted_summary['coverage'] == 'partial'
    assert accepted_summary['state_counts']['ACCEPTED'] == 1

    repository.transition(
        _SERVICE_NAME, _SERVICE_HASH,
        _transition_payload(accepted,
                            'terminal',
                            terminal_status='SUCCEEDED',
                            processing_time_us=123))
    terminal_summary = repository.summary(_SERVICE_NAME, _SERVICE_HASH)
    assert terminal_summary['state_counts']['SUCCEEDED'] == 1
    assert terminal_summary['operational_terminal_receipt_total'] == 1


def test_summary_does_not_wait_for_a_service_row_writer(ledger_database):
    repository = async_request_ledger.AsyncRequestLedgerRepository(
        ledger_database)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    writer = ledger_database.connect()
    transaction = writer.begin()
    try:
        writer.execute(
            sqlalchemy.update(serve_state_schema.services_table).where(
                serve_state_schema.services_table.c.name ==
                _SERVICE_NAME).values(controller_pid=124))
        future = executor.submit(repository.summary, _SERVICE_NAME,
                                 _SERVICE_HASH)

        summary = future.result(timeout=2)

        assert summary['available'] is True
        assert summary['service_hash'] == _SERVICE_HASH
    finally:
        transaction.rollback()
        writer.close()
        executor.shutdown(wait=True)


def test_get_summary_fails_closed_before_schema(monkeypatch) -> None:
    engine = sqlalchemy.create_mock_engine('postgresql://', lambda *args: None)
    monkeypatch.setattr(async_request_ledger, 'schema_available',
                        lambda unused: False)

    summary = async_request_ledger.get_summary(_SERVICE_NAME, _SERVICE_HASH,
                                               engine)

    assert summary == async_request_ledger.unavailable_summary(
        'schema_unavailable')
