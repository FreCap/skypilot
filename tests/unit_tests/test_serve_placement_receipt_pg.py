"""PostgreSQL coverage for placement-normalization load receipts."""

# pylint: disable=protected-access,redefined-outer-name,unused-import
import dataclasses
import threading
import time
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from sqlalchemy import event
from test_serve_resource_actions_pg import empty_postgres
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

import sky
from sky.serve import placement_contract_normalization
from sky.serve import placement_normalization_authority
from sky.serve import placement_policy
from sky.serve import serve_state
from sky.serve import service_spec
from sky.server import constants as server_constants
from sky.server.requests import postgres_schema as request_postgres_schema
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.utils.db import db_utils
from sky.utils.db import migration_utils

pytestmark = pytest.mark.xdist_group(
    name='serve_placement_normalization_schema_040_pg')


@pytest.fixture
def serve040(empty_postgres):
    engine = empty_postgres
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.upgrade(config, '040')
    assert migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME) == '040'
    return engine


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


@dataclasses.dataclass(frozen=True)
class _PendingReceipt:
    service_name: str
    service_hash: str
    lifecycle_epoch: int
    owner: tuple[int, str]
    request: serve_state.PlacementNormalizationRequest
    run_id: uuid.UUID
    loaded_at: float


def _prepare_pending_receipt(engine: sqlalchemy.engine.Engine,
                             service_name: str) -> _PendingReceipt:
    service_hash = f'{service_name}-incarnation'
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
    return _PendingReceipt(service_name=service_name,
                           service_hash=service_hash,
                           lifecycle_epoch=lifecycle_epoch,
                           owner=owner,
                           request=request,
                           run_id=run_id,
                           loaded_at=manifest_completed_at + 1.0)


def _is_receipt_update(statement: str) -> bool:
    normalized = ' '.join(statement.split())
    target, separator, assignments = normalized.partition(' SET ')
    return (separator == ' SET ' and target in {
        'UPDATE services',
        'UPDATE public.services',
        'UPDATE "public".services',
        'UPDATE "public"."services"',
    } and assignments.startswith('placement_normalization_loaded_run_id='))


def test_postgres_normalization_receipt_locks_and_cas_exact_owner(
        serve040, monkeypatch):
    """A real pending ledger can be acknowledged through its PostgreSQL lock."""
    engine = serve040
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    monkeypatch.setattr(sky, '__commit__', 'a' * 40)
    pending = _prepare_pending_receipt(engine, 'svc-normalization-receipt-pg')

    lock_engine = db_utils.get_postgres_lock_engine(engine)
    executed_statements: list[str] = []
    start_gate_contender = threading.Event()
    gate_contender_started = threading.Event()
    gate_contender_acquired = threading.Event()
    gate_contender_error: list[BaseException] = []

    def capture_statement(_connection, _cursor, statement, _parameters,
                          _context, _executemany):
        executed_statements.append(statement)

    def contend_for_gate() -> None:
        try:
            assert start_gate_contender.wait(timeout=10)
            with engine.begin() as connection:
                gate_contender_started.set()
                connection.exec_driver_sql(
                    'LOCK TABLE public.placement_normalization_write_fence '
                    'IN ACCESS EXCLUSIVE MODE')
                gate_contender_acquired.set()
        except BaseException as error:  # pylint: disable=broad-except
            gate_contender_error.append(error)
            gate_contender_started.set()
            gate_contender_acquired.set()

    def assert_gate_is_held_through_update(_connection, _cursor, statement,
                                           _parameters, _context, _executemany):
        if not _is_receipt_update(statement):
            return
        start_gate_contender.set()
        assert gate_contender_started.wait(timeout=10)
        deadline = time.monotonic() + 10
        while True:
            with engine.connect() as observer:
                blocked = observer.execute(
                    sqlalchemy.text("""
                        SELECT count(*)
                        FROM pg_catalog.pg_locks AS held
                        JOIN pg_catalog.pg_class AS relation
                          ON relation.oid = held.relation
                        JOIN pg_catalog.pg_namespace AS namespace
                          ON namespace.oid = relation.relnamespace
                        WHERE namespace.nspname = 'public'
                          AND relation.relname =
                              'placement_normalization_write_fence'
                          AND held.mode = 'AccessExclusiveLock'
                          AND NOT held.granted
                        """)).scalar_one()
            if blocked == 1:
                break
            if gate_contender_error:
                raise gate_contender_error[0]
            assert time.monotonic() < deadline, (
                'competing gate lock did not block behind the receipt read')
            time.sleep(0.01)
        assert not gate_contender_acquired.is_set()

    gate_contender = threading.Thread(target=contend_for_gate, daemon=True)
    gate_contender.start()

    event.listen(lock_engine, 'before_cursor_execute', capture_statement)
    event.listen(lock_engine, 'after_cursor_execute',
                 assert_gate_is_held_through_update)
    try:
        assert not serve_state.acknowledge_placement_normalization_loaded(
            pending.service_name,
            pending.request,
            expected_service_hash=pending.service_hash,
            expected_controller_owner=(pending.owner[0] + 1, pending.owner[1]),
            image_commit='stale-owner',
            child_controller_pid=456,
            boot_id='b' * 32,
            loaded_at=pending.loaded_at)
        assert _loaded_receipt(engine, pending.service_name) == (None,) * 6

        stale_epoch_request = serve_state.PlacementNormalizationRequest(
            run_id=pending.request.run_id,
            recovery_version=pending.request.recovery_version,
            current_version=pending.request.current_version,
            lifecycle_epoch=pending.lifecycle_epoch + 1)
        assert not serve_state.acknowledge_placement_normalization_loaded(
            pending.service_name,
            stale_epoch_request,
            expected_service_hash=pending.service_hash,
            expected_controller_owner=pending.owner,
            image_commit='stale-epoch',
            child_controller_pid=456,
            boot_id='c' * 32,
            loaded_at=pending.loaded_at)
        assert _loaded_receipt(engine, pending.service_name) == (None,) * 6

        assert serve_state.acknowledge_placement_normalization_loaded(
            pending.service_name,
            pending.request,
            expected_service_hash=pending.service_hash,
            expected_controller_owner=pending.owner,
            image_commit='commit-a',
            child_controller_pid=456,
            boot_id='d' * 32,
            loaded_at=pending.loaded_at)
        first_postimage = _loaded_receipt(engine, pending.service_name)
        assert not serve_state.acknowledge_placement_normalization_loaded(
            pending.service_name,
            pending.request,
            expected_service_hash=pending.service_hash,
            expected_controller_owner=pending.owner,
            image_commit='commit-b',
            child_controller_pid=789,
            boot_id='e' * 32,
            loaded_at=pending.loaded_at + 10.0)
        assert _loaded_receipt(engine, pending.service_name) == first_postimage
    finally:
        event.remove(lock_engine, 'before_cursor_execute', capture_statement)
        event.remove(lock_engine, 'after_cursor_execute',
                     assert_gate_is_held_through_update)
        start_gate_contender.set()
        gate_contender.join(timeout=10)

    assert not gate_contender.is_alive()
    assert gate_contender_acquired.is_set()
    assert not gate_contender_error

    assert any('FOR UPDATE OF services' in ' '.join(statement.split())
               for statement in executed_statements)
    receipt_updates = [
        ' '.join(statement.split())
        for statement in executed_statements
        if _is_receipt_update(statement)
    ]
    assert len(receipt_updates) == 1
    assert receipt_updates[0].startswith((
        'UPDATE public.services SET ',
        'UPDATE "public".services SET ',
        'UPDATE "public"."services" SET ',
    ))
    for column in ('placement_normalization_loaded_run_id',
                   'placement_normalization_loaded_image_commit',
                   'placement_normalization_loaded_controller_pid',
                   'placement_normalization_loaded_controller_ip',
                   'placement_normalization_loaded_boot_id',
                   'placement_normalization_loaded_at'):
        assert f'{column} IS NULL' in receipt_updates[0]
    assert _loaded_receipt(engine, pending.service_name) == (
        pending.run_id,
        'commit-a',
        456,
        pending.owner[1],
        'd' * 32,
        pending.loaded_at,
    )
    assert serve_state.get_placement_normalization_request(
        pending.service_name,
        recovery_version=1,
        current_version=1,
        expected_service_hash=pending.service_hash,
        expected_controller_owner=pending.owner) is None


def test_postgres_normalization_receipt_rejects_tampered_authority(
        serve040, monkeypatch):
    """Authority drift aborts before any of the six receipt fields change."""
    engine = serve040
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    monkeypatch.setattr(sky, '__commit__', 'a' * 40)
    pending = _prepare_pending_receipt(
        engine, 'svc-normalization-receipt-tampered-authority')

    assertion = placement_normalization_authority.AUTHORITY_FUNCTION
    with engine.begin() as connection:
        connection.exec_driver_sql(f"""
            CREATE OR REPLACE FUNCTION public.{assertion}()
            RETURNS boolean
            LANGUAGE plpgsql
            VOLATILE
            PARALLEL UNSAFE
            SECURITY DEFINER
            SET search_path = pg_catalog, public
            AS $tampered$
            BEGIN
                RETURN TRUE;
            END;
            $tampered$
            """)

    with pytest.raises(
            RuntimeError,
            match=('Placement normalization receipt database authority is '
                   'absent or invalid')):
        serve_state.acknowledge_placement_normalization_loaded(
            pending.service_name,
            pending.request,
            expected_service_hash=pending.service_hash,
            expected_controller_owner=pending.owner,
            image_commit='must-not-persist',
            child_controller_pid=456,
            boot_id='f' * 32,
            loaded_at=pending.loaded_at)
    assert _loaded_receipt(engine, pending.service_name) == (None,) * 6


def test_operator_dry_run_rejects_temporary_inventory_shadow(serve040):
    """Dry-run proves authority before its first inventory-table read."""
    engine = serve040
    shadow_engine = sqlalchemy.create_engine(
        engine.url, poolclass=sqlalchemy.pool.StaticPool)
    try:
        with shadow_engine.begin() as connection:
            connection.exec_driver_sql(
                'CREATE TEMP TABLE version_specs (shadow integer)')

        with pytest.raises(
                placement_contract_normalization.NormalizationBlocker,
                match=('Placement-normalization database read authority is '
                       'absent or invalid')):
            placement_contract_normalization.run_operator(engine=shadow_engine,
                                                          mode=None,
                                                          row_bound=10)
    finally:
        shadow_engine.dispose()


def test_operator_dry_run_ignores_hostile_search_path(serve040):
    """The proved schema, not search_path, owns dry-run inventory reads."""
    engine = serve040
    hostile_schema = 'hostile_placement_operator'
    hostile_engine = sqlalchemy.create_engine(
        engine.url, poolclass=sqlalchemy.pool.StaticPool)
    try:
        with hostile_engine.begin() as connection:
            connection.exec_driver_sql(f'CREATE SCHEMA {hostile_schema}')
            connection.exec_driver_sql(
                f'CREATE TABLE {hostile_schema}.version_specs '
                '(shadow integer)')
            connection.exec_driver_sql(
                f'SET SESSION search_path = {hostile_schema}, public')

        result = placement_contract_normalization.run_operator(
            engine=hostile_engine, mode=None, row_bound=10)

        assert result.row_count == 0
        assert not result.classification_counts
    finally:
        hostile_engine.dispose()
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {hostile_schema} CASCADE')


def test_operator_external_database_evidence_ignores_hostile_tables(serve040):
    """Every default database evidence reader binds the canonical schema."""
    engine = serve040
    request_postgres_schema.metadata.create_all(engine)
    hostile_schema = 'hostile_placement_evidence'
    hostile_engine = sqlalchemy.create_engine(
        engine.url, poolclass=sqlalchemy.pool.StaticPool)
    mutation_name = (server_constants.REQUEST_NAME_PREFIX +
                     request_names.RequestName.SERVE_UP.value)
    active_status = requests_lib.RequestStatus.PENDING.value
    try:
        with hostile_engine.begin() as connection:
            connection.exec_driver_sql(f'CREATE SCHEMA {hostile_schema}')
            connection.exec_driver_sql(f"""
                CREATE TABLE {hostile_schema}.api_requests (
                    request_id text, name text, status text,
                    execution_generation bigint);
                INSERT INTO {hostile_schema}.api_requests
                    VALUES ('spoof-request', '{mutation_name}',
                            '{active_status}', 1);
                CREATE TABLE {hostile_schema}.api_server_instances (
                    instance_id uuid, role text, pod_uid text, ready boolean,
                    draining_at timestamptz, heartbeat_at timestamptz);
                INSERT INTO {hostile_schema}.api_server_instances
                    VALUES ('11111111-1111-4111-8111-111111111111', 'all',
                            'spoof-pod', true, NULL,
                            pg_catalog.clock_timestamp());
                CREATE TABLE {hostile_schema}.api_resource_actions (
                    action_id uuid, domain text, resource_type text,
                    resource_identity text, desired_generation bigint,
                    action_type text, immutable_spec jsonb,
                    immutable_spec_sha256 text);
                INSERT INTO {hostile_schema}.api_resource_actions
                    VALUES ('22222222-2222-4222-8222-222222222222', 'serve',
                            'replica', 'spoof', 1, 'launch', '{{}}'::jsonb,
                            '{'a' * 64}');
                CREATE TABLE
                    {hostile_schema}.serve_resource_action_shadow_samples (
                    would_be_action_id uuid, service_name text,
                    service_hash text, service_incarnation uuid,
                    replica_id integer, replica_incarnation uuid,
                    desired_generation bigint, action_type text,
                    resource_identity text, immutable_spec jsonb,
                    immutable_spec_sha256 text);
                SET SESSION search_path = {hostile_schema}, public;
                """)

        request_evidence = (placement_contract_normalization.
                            _active_serve_request_evidence(hostile_engine))
        api_instances = placement_contract_normalization._fresh_api_instances(
            hostile_engine)
        action_roots = (placement_contract_normalization.
                        _resource_action_root_rows(hostile_engine))

        assert request_evidence.count == 0
        assert api_instances == []
        assert action_roots == []
    finally:
        hostile_engine.dispose()
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {hostile_schema} CASCADE')


def test_operator_pre_external_read_rejects_wrong_authority(
        serve040, monkeypatch):
    """Apply refuses a wrong authority before invoking external evidence."""
    engine = serve040
    monkeypatch.setattr(sky, '__commit__', 'a' * 40)
    assertion = placement_normalization_authority.AUTHORITY_FUNCTION
    with engine.begin() as connection:
        connection.exec_driver_sql(f"""
            CREATE OR REPLACE FUNCTION public.{assertion}()
            RETURNS boolean
            LANGUAGE plpgsql
            VOLATILE
            PARALLEL UNSAFE
            SECURITY DEFINER
            SET search_path = pg_catalog, public
            AS $wrong_authority$
            BEGIN
                RETURN TRUE;
            END;
            $wrong_authority$
            """)

    external_calls: list[str] = []

    def unexpected_external_call(*_args, **_kwargs):
        external_calls.append('called')
        raise AssertionError('reader authority must precede external action')

    with pytest.raises(
            placement_contract_normalization.NormalizationBlocker,
            match=('Placement-normalization database read authority is '
                   'absent or invalid')):
        placement_contract_normalization.run_operator(
            engine=engine,
            mode=placement_contract_normalization.ApplyMode.SUPPORTED,
            row_bound=10,
            freeze_evidence_sha256='f' * 64,
            request_evidence_getter=unexpected_external_call,
            api_pod_checker=unexpected_external_call)
    assert not external_calls
