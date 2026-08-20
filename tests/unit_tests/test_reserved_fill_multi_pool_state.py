"""State-layer contracts for multi-pool reserved-capacity fill."""
# pylint: disable=protected-access,redefined-outer-name,unused-argument,unused-import
import contextlib
import datetime
from unittest import mock
import uuid

from alembic import command as alembic_command
import pytest
import sqlalchemy
from test_serve_resource_actions_pg import postgres_engine  # noqa: F401

from sky.serve import replica_managers
from sky.serve import serve_state
from sky.server.requests import postgres as request_postgres
from sky.server.requests import postgres_schema as request_postgres_schema
from sky.utils.db import migration_utils


def test_protocol_v2_state_rejects_sqlite(tmp_path):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "gate.db"}')
    try:
        with pytest.raises(RuntimeError, match='central PostgreSQL'):
            serve_state._require_reserved_fill_v2_postgresql(engine)
    finally:
        engine.dispose()


def test_service_teardown_takes_broker_lock_before_database(
        tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "teardown-lock.db"}')
    serve_state.Base.metadata.create_all(engine)
    monkeypatch.setattr(serve_state._db_manager, '_engine', engine)
    with engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc', status='READY'))
    broker_lock = mock.MagicMock()
    launch_authority_lock = mock.MagicMock()
    launch_authority_lock_id = serve_state._replica_launch_authority_lock_id(
        'svc', engine)
    with mock.patch.object(serve_state.locks,
                           'get_lock',
                           side_effect=[broker_lock,
                                        launch_authority_lock]) as get_lock:
        serve_state.remove_service('svc')
    engine.dispose()

    assert get_lock.call_args_list == [
        mock.call(serve_state.constants.RESERVED_FILL_BROKER_LOCK_ID),
        mock.call(launch_authority_lock_id, lock_type='filelock'),
    ]
    broker_lock.acquire.assert_called_once_with(blocking=True)
    launch_authority_lock.acquire.assert_called_once_with(blocking=True)


@pytest.fixture
def state_engine(postgres_engine, monkeypatch):  # noqa: F811
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    serve_state.Base.metadata.create_all(postgres_engine)
    config = migration_utils.get_alembic_config(postgres_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.stamp(config, '042')
    # create_all() installs the current table shape but, unlike the real
    # migration chain, does not seed the protocol singleton introduced by
    # Serve035.  Serve045 intentionally refuses to invent that authority row.
    alembic_command.upgrade(config, '044')
    with postgres_engine.begin() as connection:
        connection.execute(
            serve_state.reserved_fill_protocol_state_table.insert().values(
                id=1))
    alembic_command.upgrade(config, '045')
    monkeypatch.setattr(serve_state._db_manager, '_engine', postgres_engine)
    lock = mock.MagicMock()
    lock.acquire.return_value = contextlib.nullcontext()
    monkeypatch.setattr(serve_state.locks, 'get_lock',
                        mock.Mock(return_value=lock))
    with postgres_engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc',
            hash='service-hash',
            resource_scope='service-hash',
            controller_pid=17,
            controller_ip='10.0.0.17',
            status='READY'))
    return postgres_engine


def _edge(pool: str, position: int) -> dict[str, object]:
    return {
        'pool_key': f'v2:{pool}',
        'legacy_pool_key': f'["{pool}","h200"]',
        'pool_position': position,
        'access_context': pool,
        'physical_cluster_uid': f'uid-{pool}',
        'accelerator_names': ['h200'],
        'weight': 1.0,
        'floor_replicas': 1 if position == 0 else 0,
        'gpus_per_replica': 8,
        'holdings_fill': 0,
        'effective_cap': 2,
        'launchable': True,
    }


def test_recent_reserved_fill_writer_instances_are_database_wide(state_engine):
    request_postgres_schema.SERVER_INSTANCES.create(state_engine)
    now = datetime.datetime.now(datetime.timezone.utc)

    def instance(role, index, *, age_seconds=0, draining=False):
        pod_uid = str(uuid.uuid4())
        return {
            'instance_id': uuid.UUID(pod_uid),
            'role': role,
            'pod_name': f'{role}-{index}',
            'pod_uid': pod_uid,
            'pod_ip': f'10.0.0.{index + 1}',
            'version': f'version-{index}',
            'started_at': now - datetime.timedelta(minutes=1),
            'heartbeat_at': now - datetime.timedelta(seconds=age_seconds),
            'draining_at': now if draining else None,
            'ready': not draining,
            'health_detail': {},
            'supported_handlers': [],
            'supported_payload_versions': {},
            'request_storage_backend':
                (request_postgres.POSTGRES_REQUEST_STORAGE_BACKEND_TYPE),
            'request_queue_backend':
                (request_postgres.POSTGRES_REQUEST_QUEUE_BACKEND_TYPE),
            'execution_quiescence_capable': True,
        }

    live_all = instance('all', 0)
    live_draining_controller = instance('controller', 1, draining=True)
    live_executor = instance('executor', 2)
    live_api = instance('api', 3)
    stale_controller = instance('controller', 4, age_seconds=60)
    with state_engine.begin() as connection:
        connection.execute(request_postgres_schema.SERVER_INSTANCES.insert(), [
            live_all, live_draining_controller, live_executor, live_api,
            stale_controller
        ])

    observed = serve_state.get_recent_reserved_fill_writer_instances(20)

    assert observed == tuple(
        sorted((
            serve_state.ReservedFillWriterInstance(
                instance_id=str(live_all['instance_id']),
                role='all',
                pod_name=str(live_all['pod_name']),
                pod_uid=str(live_all['pod_uid']),
                version=str(live_all['version']),
                ready=True,
                draining=False,
                request_storage_backend=str(
                    live_all['request_storage_backend']),
                request_queue_backend=str(live_all['request_queue_backend']),
                execution_quiescence_capable=True),
            serve_state.ReservedFillWriterInstance(
                instance_id=str(live_api['instance_id']),
                role='api',
                pod_name=str(live_api['pod_name']),
                pod_uid=str(live_api['pod_uid']),
                version=str(live_api['version']),
                ready=True,
                draining=False,
                request_storage_backend=str(
                    live_api['request_storage_backend']),
                request_queue_backend=str(live_api['request_queue_backend']),
                execution_quiescence_capable=True),
            serve_state.ReservedFillWriterInstance(
                instance_id=str(live_draining_controller['instance_id']),
                role='controller',
                pod_name=str(live_draining_controller['pod_name']),
                pod_uid=str(live_draining_controller['pod_uid']),
                version=str(live_draining_controller['version']),
                ready=False,
                draining=True,
                request_storage_backend=str(
                    live_draining_controller['request_storage_backend']),
                request_queue_backend=str(
                    live_draining_controller['request_queue_backend']),
                execution_quiescence_capable=True),
            serve_state.ReservedFillWriterInstance(
                instance_id=str(live_executor['instance_id']),
                role='executor',
                pod_name=str(live_executor['pod_name']),
                pod_uid=str(live_executor['pod_uid']),
                version=str(live_executor['version']),
                ready=True,
                draining=False,
                request_storage_backend=str(
                    live_executor['request_storage_backend']),
                request_queue_backend=str(
                    live_executor['request_queue_backend']),
                execution_quiescence_capable=True),
        ),
               key=lambda item:
               (item.role, item.pod_uid or '', item.instance_id)))


def _demotion_edge(pool: str, position: int) -> dict[str, object]:
    """Return an edge with the production protocol-v2 pool-key encoding."""
    edge = _edge(pool, position)
    edge['pool_key'] = f'["v2","uid-{pool}","h200"]'
    return edge


def _activate_v2() -> bool:
    return serve_state.set_reserved_fill_protocol_version(
        2,
        expected_protocol_version=1,
        image_digest='sha256:' + 'a' * 64,
        deployment_generation='deployment-7',
        deployment_uid='deployment-uid-7',
        pod_inventory_count=2,
        pod_inventory_sha256='b' * 64,
        changed_at=10.0)


def _replace(edges,
             semantic_hash='semantic-a',
             heartbeat=100.0,
             service_name='svc',
             service_hash='service-hash',
             controller_owner=(17, '10.0.0.17')):
    return serve_state.replace_reserved_fill_claim_set(
        service_name,
        semantic_hash=semantic_hash,
        global_headroom=4,
        utilization_ceiling=3,
        utilization_state={'cap': 3},
        edges=edges,
        heartbeat_ts=heartbeat,
        expected_service_hash=service_hash,
        expected_controller_owner=controller_owner)


@pytest.mark.parametrize('override', [
    {
        'protocol_version': 2,
        'image_digest': None,
        'deployment_generation': None,
        'deployment_uid': None,
        'pod_inventory_count': None,
        'pod_inventory_sha256': None,
    },
    {
        'deployment_uid': None,
    },
    {
        'image_digest': 'sha256:short',
    },
    {
        'deployment_generation': '',
    },
    {
        'deployment_uid': '',
    },
    {
        'pod_inventory_count': 0,
    },
    {
        'pod_inventory_sha256': 'b' * 63,
    },
])
def test_protocol_singleton_database_rejects_incomplete_or_invalid_proof(
        state_engine, override):
    row = {
        'id': 1,
        'protocol_version': 1,
        'claim_generation': 0,
        'image_digest': 'sha256:' + 'a' * 64,
        'deployment_generation': '7',
        'deployment_uid': 'deployment-uid',
        'pod_inventory_count': 2,
        'pod_inventory_sha256': 'b' * 64,
        'changed_at': 10.0,
    }
    row.update(override)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with state_engine.begin() as connection:
            connection.execute(
                serve_state.reserved_fill_protocol_state_table.insert().values(
                    **row))


def test_protocol_gate_starts_v1_and_requires_activation_proof(state_engine):
    assert serve_state.get_reserved_fill_protocol_state(
    )['protocol_version'] == 1
    with pytest.raises(ValueError, match='sha256'):
        serve_state.set_reserved_fill_protocol_version(
            2,
            expected_protocol_version=1,
            image_digest='latest',
            deployment_generation='deployment-7')
    assert _activate_v2()
    state = serve_state.get_reserved_fill_protocol_state()
    assert state['protocol_version'] == 2
    assert state['claim_generation'] == 0
    assert state['deployment_generation'] == 'deployment-7'
    assert state['deployment_uid'] == 'deployment-uid-7'
    assert state['pod_inventory_count'] == 2
    assert state['pod_inventory_sha256'] == 'b' * 64


def test_complete_set_is_authoritative_and_never_unions_legacy(state_engine):
    assert _activate_v2()
    assert _replace([_edge('east', 0), _edge('phx', 1)]) == 1
    claim_set = serve_state.get_reserved_fill_service_claim_set('svc')
    assert claim_set is not None
    assert claim_set['integrity_valid']
    assert claim_set['utilization_state'] == {'cap': 3}
    assert [edge['access_context'] for edge in claim_set['edges']
           ] == ['east', 'phx']
    claims = serve_state.get_authoritative_reserved_fill_claims(
        expired_before=99.0)
    assert {(claim['pool_key'], claim['service_generation']) for claim in claims
           } == {('v2:east', 1), ('v2:phx', 1)}
    assert all(claim['demonstrated_need'] is None for claim in claims)

    # A legacy writer can move its projection, but strict v2 reads never union
    # or fall back to that incompatible representation.
    with state_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.reserved_fill_claims_table).where(
                serve_state.reserved_fill_claims_table.c.service_name ==
                'svc').values(pool_key='legacy-writer-moved'))
    assert len(serve_state.get_authoritative_reserved_fill_claims()) == 2
    assert serve_state.get_authoritative_reserved_fill_claims(
        pool_key='legacy-writer-moved') == []


def test_generation_changes_only_with_semantics_and_fences_edge_removal(
        state_engine):
    assert _activate_v2()
    edges = [_demotion_edge('east', 0), _demotion_edge('phx', 1)]
    assert _replace(edges) == 1
    assert _replace(edges, heartbeat=101.0) == 1
    assert _replace(edges, semantic_hash='semantic-b', heartbeat=102.0) == 2
    assert not serve_state.set_reserved_fill_protocol_version(
        1, expected_protocol_version=2, changed_at=103.0)

    assert serve_state.remove_authoritative_reserved_fill_claim(
        'svc', '["v2","uid-phx","h200"]', expected_service_generation=2)
    claim_set = serve_state.get_reserved_fill_service_claim_set('svc')
    assert claim_set is not None
    assert claim_set['generation'] == 3
    assert claim_set['edge_count'] == 1
    assert claim_set['edges'][0]['service_generation'] == 3
    assert serve_state.set_reserved_fill_protocol_version(
        1, expected_protocol_version=2, changed_at=104.0)
    legacy = serve_state.get_authoritative_reserved_fill_claims()
    assert [claim['pool_key'] for claim in legacy] == ['["east","h200"]']


@pytest.mark.parametrize('legacy_state', ('missing', 'divergent'))
def test_demotion_atomically_rebuilds_complete_legacy_projection(
        state_engine, legacy_state):
    assert _activate_v2()
    assert _replace([_demotion_edge('east', 0)]) == 1
    with state_engine.begin() as connection:
        if legacy_state == 'missing':
            connection.execute(
                sqlalchemy.delete(serve_state.reserved_fill_claims_table).where(
                    serve_state.reserved_fill_claims_table.c.service_name ==
                    'svc'))
        else:
            connection.execute(
                sqlalchemy.update(serve_state.reserved_fill_claims_table).where(
                    serve_state.reserved_fill_claims_table.c.service_name ==
                    'svc').values(pool_key='legacy-writer-moved',
                                  weight=99.0,
                                  floor_replicas=99,
                                  gpus_per_replica=1,
                                  holdings_fill=99,
                                  effective_cap=99,
                                  launchable=0,
                                  demonstrated_need=99,
                                  boot_hold=1,
                                  activity_ts=99.0,
                                  heartbeat_ts=99.0))

    assert serve_state.set_reserved_fill_protocol_version(
        1, expected_protocol_version=2, changed_at=101.0)

    state = serve_state.get_reserved_fill_protocol_state()
    assert state['protocol_version'] == 1
    with state_engine.connect() as connection:
        legacy = connection.execute(
            sqlalchemy.select(serve_state.reserved_fill_claims_table).where(
                serve_state.reserved_fill_claims_table.c.service_name ==
                'svc')).mappings().one()
    assert dict(legacy) == {
        'service_name': 'svc',
        'pool_key': '["east","h200"]',
        'weight': 1.0,
        'floor_replicas': 1,
        'gpus_per_replica': 8,
        'holdings_fill': 0,
        'effective_cap': 2,
        'launchable': 1,
        'demonstrated_need': None,
        'boot_hold': None,
        'activity_ts': None,
        'heartbeat_ts': 100.0,
    }


def test_demotion_rejects_extra_legacy_only_row_without_rebuilding(
        state_engine):
    assert _activate_v2()
    assert _replace([_demotion_edge('east', 0)]) == 1
    with state_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.reserved_fill_claims_table).where(
                serve_state.reserved_fill_claims_table.c.service_name ==
                'svc').values(pool_key='must-remain-on-failure'))
        connection.execute(
            serve_state.reserved_fill_claims_table.insert().values(
                service_name='legacy-only',
                pool_key='["east","h200"]',
                weight=1.0,
                floor_replicas=0,
                gpus_per_replica=8,
                holdings_fill=0,
                effective_cap=1,
                launchable=1,
                heartbeat_ts=100.0))

    assert not serve_state.set_reserved_fill_protocol_version(
        1, expected_protocol_version=2, changed_at=101.0)
    assert serve_state.get_reserved_fill_protocol_state(
    )['protocol_version'] == 2
    with state_engine.connect() as connection:
        legacy = connection.execute(
            sqlalchemy.select(
                serve_state.reserved_fill_claims_table)).mappings().all()
    assert {
        row['service_name']: row['pool_key'] for row in legacy
    } == {
        'svc': 'must-remain-on-failure',
        'legacy-only': '["east","h200"]',
    }


def test_demotion_rejects_malformed_authoritative_edge_without_rebuilding(
        state_engine):
    assert _activate_v2()
    assert _replace([_demotion_edge('east', 0)]) == 1
    with state_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(
                serve_state.reserved_fill_pool_claims_table).where(
                    serve_state.reserved_fill_pool_claims_table.c.service_name
                    == 'svc').values(physical_cluster_uid='different-uid'))
        connection.execute(
            sqlalchemy.update(serve_state.reserved_fill_claims_table).where(
                serve_state.reserved_fill_claims_table.c.service_name ==
                'svc').values(pool_key='must-remain-on-failure'))

    assert not serve_state.set_reserved_fill_protocol_version(
        1, expected_protocol_version=2, changed_at=101.0)
    assert serve_state.get_reserved_fill_protocol_state(
    )['protocol_version'] == 2
    with state_engine.connect() as connection:
        pool_key = connection.execute(
            sqlalchemy.select(
                serve_state.reserved_fill_claims_table.c.pool_key).where(
                    serve_state.reserved_fill_claims_table.c.service_name ==
                    'svc')).scalar_one()
    assert pool_key == 'must-remain-on-failure'


def test_demotion_projection_failure_rolls_back_rebuild_and_gate(state_engine):
    assert _activate_v2()
    assert _replace([_demotion_edge('east', 0)]) == 1
    with state_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.reserved_fill_claims_table).where(
                serve_state.reserved_fill_claims_table.c.service_name ==
                'svc').values(pool_key='must-remain-on-failure'))
        connection.exec_driver_sql("""
            CREATE FUNCTION reject_reserved_fill_demotion_projection()
            RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'injected demotion projection failure';
            END;
            $$ LANGUAGE plpgsql
        """)
        connection.exec_driver_sql("""
            CREATE TRIGGER reject_reserved_fill_demotion_projection
            BEFORE INSERT OR UPDATE ON reserved_fill_claims
            FOR EACH ROW EXECUTE FUNCTION reject_reserved_fill_demotion_projection()
        """)

    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='injected demotion projection failure'):
        serve_state.set_reserved_fill_protocol_version(
            1, expected_protocol_version=2, changed_at=101.0)

    assert serve_state.get_reserved_fill_protocol_state(
    )['protocol_version'] == 2
    with state_engine.connect() as connection:
        pool_key = connection.execute(
            sqlalchemy.select(
                serve_state.reserved_fill_claims_table.c.pool_key).where(
                    serve_state.reserved_fill_claims_table.c.service_name ==
                    'svc')).scalar_one()
    assert pool_key == 'must-remain-on-failure'


def test_corrupt_v2_set_fails_closed_without_legacy_fallback(state_engine):
    assert _activate_v2()
    assert _replace([_edge('east', 0), _edge('phx', 1)]) == 1
    with state_engine.begin() as connection:
        connection.execute(
            sqlalchemy.delete(
                serve_state.reserved_fill_pool_claims_table).where(
                    serve_state.reserved_fill_pool_claims_table.c.pool_key ==
                    'v2:phx'))
    assert serve_state.get_authoritative_reserved_fill_claims() == []


def test_owner_loss_rejects_complete_set_without_mutation(state_engine):
    assert _activate_v2()
    with mock.patch.object(serve_state,
                           '_lock_service_owner_row_in_session',
                           return_value=None):
        assert _replace([_edge('east', 0)]) is None
    assert serve_state.get_reserved_fill_service_claim_set('svc') is None


@pytest.mark.parametrize('resource_scope', [None, '', 'different-incarnation'])
def test_protocol_v2_unscoped_owner_withdraws_stale_claim_set(
        state_engine, resource_scope):
    assert _activate_v2()
    assert _replace([_edge('east', 0)]) == 1
    before_protocol = serve_state.get_reserved_fill_protocol_state()
    with state_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == 'svc').values(
                    resource_scope=resource_scope))

    assert _replace([_edge('east', 0)], heartbeat=101.0) is None

    assert serve_state.get_reserved_fill_service_claim_set('svc') is None
    assert serve_state.get_authoritative_reserved_fill_claims() == []
    after_protocol = serve_state.get_reserved_fill_protocol_state()
    assert after_protocol['protocol_version'] == 2
    assert (after_protocol['claim_generation'] ==
            before_protocol['claim_generation'])
    with state_engine.connect() as connection:
        # pylint: disable=not-callable
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state.reserved_fill_claims_table).where(
                    serve_state.reserved_fill_claims_table.c.service_name ==
                    'svc')).scalar_one() == 0


def test_protocol_v1_owner_fence_accepts_legacy_null_scope(state_engine):
    with state_engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(serve_state.services_table).where(
                serve_state.services_table.c.name == 'svc').values(
                    resource_scope=None))

    assert serve_state.upsert_reserved_fill_claim(
        'svc',
        pool_key='["legacy-context","h200"]',
        weight=1.0,
        floor_replicas=0,
        gpus_per_replica=8,
        holdings_fill=0,
        effective_cap=1,
        launchable=True,
        heartbeat_ts=100.0,
        expected_service_hash='service-hash',
        expected_controller_owner=(17, '10.0.0.17'))
    assert [
        row['service_name'] for row in serve_state.get_reserved_fill_claims()
    ] == ['svc']


def test_claim_set_and_legacy_projection_failure_roll_back_atomically(
        state_engine):
    """A projection SQL failure cannot expose a half-replaced v2 set."""
    assert _activate_v2()
    assert _replace([_edge('east', 0)]) == 1
    before_protocol = serve_state.get_reserved_fill_protocol_state()
    before_set = serve_state.get_reserved_fill_service_claim_set('svc')
    assert before_set is not None
    with state_engine.connect() as connection:
        before_legacy = connection.execute(
            sqlalchemy.select(serve_state.reserved_fill_claims_table).where(
                serve_state.reserved_fill_claims_table.c.service_name ==
                'svc')).mappings().one()

    # Exercise a real PostgreSQL statement failure at the final compatibility
    # projection write.  The transaction has already updated the singleton
    # generation, set marker, and normalized edges at this point; all of them
    # must roll back with the failed projection.
    with state_engine.begin() as connection:
        connection.exec_driver_sql("""
            CREATE FUNCTION reject_reserved_fill_projection()
            RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'injected legacy projection failure';
            END;
            $$ LANGUAGE plpgsql
        """)
        connection.exec_driver_sql("""
            CREATE TRIGGER reject_reserved_fill_projection
            BEFORE INSERT OR UPDATE ON reserved_fill_claims
            FOR EACH ROW EXECUTE FUNCTION reject_reserved_fill_projection()
        """)

    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='injected legacy projection failure'):
        _replace([_edge('east', 0), _edge('phx', 1)],
                 semantic_hash='semantic-b',
                 heartbeat=101.0)

    assert serve_state.get_reserved_fill_protocol_state() == before_protocol
    assert serve_state.get_reserved_fill_service_claim_set('svc') == before_set
    with state_engine.connect() as connection:
        after_legacy = connection.execute(
            sqlalchemy.select(serve_state.reserved_fill_claims_table).where(
                serve_state.reserved_fill_claims_table.c.service_name ==
                'svc')).mappings().one()
        phx_edges = connection.execute(
            sqlalchemy.select(
                serve_state.reserved_fill_pool_claims_table.c.pool_key).where(
                    serve_state.reserved_fill_pool_claims_table.c.service_name
                    == 'svc',
                    serve_state.reserved_fill_pool_claims_table.c.pool_key ==
                    'v2:phx')).all()
    assert dict(after_legacy) == dict(before_legacy)
    assert phx_edges == []


def test_disable_reenable_never_reuses_claim_generation(state_engine):
    assert _activate_v2()
    edge = _edge('east', 0)
    assert _replace([edge]) == 1
    assert serve_state.remove_reserved_fill_claim_set(
        'svc',
        expected_service_hash='service-hash',
        expected_controller_owner=(17, '10.0.0.17'))
    assert serve_state.get_reserved_fill_protocol_state(
    )['claim_generation'] == 1

    # The set is gone and its semantic hash is identical, but the singleton
    # counter survives disablement and allocates a new incarnation fence.
    assert _replace([edge], heartbeat=101.0) == 2
    assert serve_state.get_reserved_fill_protocol_state(
    )['claim_generation'] == 2


def test_service_name_reuse_advances_generation_without_rewriting_old_round(
        state_engine):
    assert _activate_v2()
    edge = _edge('east', 0)
    edge['pool_key'] = '["v2","uid-east","h200"]'
    pool_key = str(edge['pool_key'])
    old_decision_generation = _replace([edge])
    assert old_decision_generation == 1
    with state_engine.begin() as connection:
        connection.execute(
            serve_state.reserved_fill_rounds_table.insert().values(
                pool_key=pool_key,
                round_id=1,
                epoch=1,
                protocol_version=2,
                claim_generations='{"svc":1}'))
        connection.execute(
            serve_state.reserved_fill_lease_table.insert().values(id=1,
                                                                  epoch=9))

    # Full service teardown removes every per-service claim row but must not
    # reset the singleton allocation fence or rewrite an old broker round.
    serve_state.remove_service('svc')
    with state_engine.begin() as connection:
        connection.execute(serve_state.services_table.insert().values(
            name='svc',
            hash='successor-hash',
            resource_scope='successor-hash',
            controller_pid=23,
            controller_ip='10.0.0.23',
            status='READY'))
    successor_generation = _replace([edge],
                                    heartbeat=102.0,
                                    service_hash='successor-hash',
                                    controller_owner=(23, '10.0.0.23'))

    assert successor_generation == 2
    assert successor_generation != old_decision_generation
    claim_set = serve_state.get_reserved_fill_service_claim_set('svc')
    assert claim_set is not None
    assert claim_set['generation'] == successor_generation
    old_round = serve_state.get_reserved_fill_round(pool_key)
    assert old_round is not None
    assert old_round['claim_generations'] == '{"svc":1}'
    assert serve_state.get_reserved_fill_protocol_state(
    )['claim_generation'] == successor_generation


def test_standalone_protocol_v2_persistence_is_removed_and_leaks_nothing(
        state_engine):
    assert _activate_v2()
    edge = _demotion_edge('east', 0)
    pool_key = str(edge['pool_key'])
    generation = _replace([edge])
    assert generation == 1
    with state_engine.begin() as connection:
        connection.execute(
            serve_state.reserved_fill_rounds_table.insert().values(
                pool_key=pool_key,
                round_id=1,
                epoch=3,
                protocol_version=2,
                claim_generations='{"svc":1}'))
        connection.execute(
            serve_state.reserved_fill_lease_table.insert().values(id=1,
                                                                  epoch=11))

    location = replica_managers.spot_placer.Location(
        cloud=replica_managers.clouds.Kubernetes(),
        region='east',
        zone=None,
        accelerators={'H200': 1},
        use_spot=False)
    replica = replica_managers.ReplicaInfo(
        replica_id=1,
        cluster_name='svc-1',
        replica_port='8080',
        is_spot=False,
        location=location,
        version=1,
        resources_override=location.to_dict())
    replica.reserved_fill = True
    replica.reserved_fill_pool_key = pool_key
    replica.reserved_fill_service_generation = generation
    replica.reserved_fill_physical_cluster_uid = 'uid-east'
    persist_kwargs = {
        'pool_key': pool_key,
        'expected_epoch': 3,
        'expected_service_hash': 'service-hash',
        'expected_controller_owner': (17, '10.0.0.17'),
        'expected_protocol_version': 2,
        'expected_service_generation': generation,
        'expected_physical_cluster_uid': 'uid-east',
    }

    for lease_token in (None, 11):
        with pytest.raises(ValueError, match='protocol-v1 only'):
            serve_state.add_replica_if_round_epoch(
                'svc',
                1,
                replica,
                expected_lease_token=lease_token,
                **persist_kwargs)
        assert serve_state.get_replica_info_from_id('svc', 1) is None
