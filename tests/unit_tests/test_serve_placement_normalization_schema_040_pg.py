"""PostgreSQL contracts for placement-normalization schema revision 040."""
# pylint: disable=not-callable,protected-access,redefined-outer-name

import concurrent.futures
import importlib
import os
from pathlib import Path
import shutil
import threading
import time
import uuid

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy

from sky import global_user_state_schema
from sky.serve import placement_contract_normalization
from sky.serve import placement_normalization_authority
from sky.serve import serve_state_schema
from sky.utils.db import migration_utils

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
_SHA = 'a' * 64

# Each case rebuilds the complete Serve schema.  Keep the module on one xdist
# worker so concurrent full migrations cannot exhaust PostgreSQL's shared lock
# table under the repository-default 16-worker test command.
pytestmark = pytest.mark.xdist_group(
    name='serve_placement_normalization_schema_040_pg')


@pytest.fixture(scope='module')
def postgres_engine():
    pytest.importorskip('psycopg2')
    container = None
    admin_engine = None
    temporary_database = None
    if _POSTGRES_URL is None:
        if shutil.which('docker') is None:
            pytest.skip('docker unavailable; skipping PostgreSQL migration')
        testcontainers_postgres = pytest.importorskip('testcontainers.postgres')
        try:
            container = testcontainers_postgres.PostgresContainer('postgres:16')
            container.start()
        except Exception as e:  # pylint: disable=broad-except
            pytest.skip(f'could not start postgres container: {e}')
        assert container is not None
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_serve_040_{uuid.uuid4().hex}'
        admin_engine = sqlalchemy.create_engine(_POSTGRES_URL,
                                                isolation_level='AUTOCOMMIT')
        quoted = admin_engine.dialect.identifier_preparer.quote(
            temporary_database)
        try:
            with admin_engine.connect() as connection:
                connection.exec_driver_sql(f'CREATE DATABASE {quoted}')
        except Exception as e:  # pylint: disable=broad-except
            admin_engine.dispose()
            pytest.skip(f'could not create temporary postgres database: {e}')
        postgres_url = sqlalchemy.engine.make_url(_POSTGRES_URL).set(
            database=temporary_database).render_as_string(hide_password=False)
    engine = sqlalchemy.create_engine(postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()
        if temporary_database is not None:
            assert admin_engine is not None
            quoted = admin_engine.dialect.identifier_preparer.quote(
                temporary_database)
            with admin_engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text('SELECT pg_terminate_backend(pid) '
                                    'FROM pg_stat_activity '
                                    'WHERE datname = :database AND '
                                    'pid <> pg_backend_pid()'),
                    {'database': temporary_database})
                connection.exec_driver_sql(f'DROP DATABASE {quoted}')
            admin_engine.dispose()
        elif container is not None:
            container.stop()


def _migration_module():
    return importlib.import_module(
        'sky.schemas.db.serve_state.040_placement_normalization_immutability')


def _reset(engine: sqlalchemy.engine.Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')


def _config(engine: sqlalchemy.engine.Engine):
    return migration_utils.get_alembic_config(engine,
                                              migration_utils.SERVE_DB_NAME)


def _upgrade(engine: sqlalchemy.engine.Engine, revision: str) -> None:
    alembic_command.upgrade(_config(engine), revision)


def _revision(engine: sqlalchemy.engine.Engine) -> str | None:
    return migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)


def _wait_for_waiting_relation_lock(
    engine: sqlalchemy.engine.Engine,
    *,
    schema: str,
    relation: str,
    mode: str,
    timeout: float = 10,
) -> int:
    """Return the backend waiting for an exact relation lock."""
    deadline = time.monotonic() + timeout
    with engine.connect() as observer:
        while True:
            pids = observer.execute(
                sqlalchemy.text("""
                    SELECT held.pid
                    FROM pg_catalog.pg_locks AS held
                    JOIN pg_catalog.pg_class AS relation
                      ON relation.oid = held.relation
                    JOIN pg_catalog.pg_namespace AS namespace
                      ON namespace.oid = relation.relnamespace
                    JOIN pg_catalog.pg_stat_activity AS activity
                      ON activity.pid = held.pid
                    WHERE held.locktype = 'relation'
                      AND held.database = (
                          SELECT database_oid.oid
                          FROM pg_catalog.pg_database AS database_oid
                          WHERE database_oid.datname =
                                pg_catalog.current_database()
                      )
                      AND namespace.nspname = :schema
                      AND relation.relname = :relation
                      AND held.mode = :mode
                      AND NOT held.granted
                      AND activity.wait_event_type = 'Lock'
                    ORDER BY held.pid
                    """), {
                    'schema': schema,
                    'relation': relation,
                    'mode': mode,
                }).scalars().all()
            if pids:
                return pids[0]
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f'No backend waited for {mode} on '
                    f'{schema}.{relation} within {timeout} seconds.')
            time.sleep(0.02)


@pytest.fixture
def serve040(postgres_engine):
    _reset(postgres_engine)
    _upgrade(postgres_engine, '040')
    assert _revision(postgres_engine) == '040'
    return postgres_engine


def _run_values(run_id: uuid.UUID,
                *,
                protocol: int = 4,
                terminal: bool = False,
                row_count: int = 1) -> dict[str, object]:
    return {
        'run_id': run_id,
        'mode':
            ('retire_terminal_historical' if terminal else 'apply_supported'),
        'normalizer_version': f'{protocol}:{"a" * 40}',
        'schema_revision': '037',
        'release_version': 'test-release',
        'started_at': 1.0,
        'completed_at': 2.0,
        'row_bound': row_count,
        'row_count': row_count,
        'classification_counts': ({
            'historical_physical_per_gpu': row_count
        } if terminal else {
            'explicit_v2': row_count
        }),
        'pre_inventory_sha256': _SHA,
        'post_inventory_sha256': _SHA,
        'freeze_evidence_sha256': _SHA,
    }


def _row_values(run_id: uuid.UUID,
                *,
                terminal: bool = False,
                service_name: str = 'svc',
                version: int = 1) -> dict[str, object]:
    classification = ('historical_physical_per_gpu'
                      if terminal else 'explicit_v2')
    outcome = 'retired' if terminal else 'unchanged'
    return {
        'run_id': run_id,
        'service_name': service_name,
        'version': version,
        'classification': classification,
        'outcome': outcome,
        'original_spec_sha256': _SHA,
        'result_spec_sha256': _SHA,
        'original_row_sha256': _SHA,
        'result_row_sha256': _SHA,
        'original_column_sha256s': {},
        'result_column_sha256s': {},
        'contract_projection': None,
        'service_hash': f'{service_name}-hash',
        'service_lifecycle_epoch': 1,
        'dependency_facts': {},
    }


def _insert_run(connection: sqlalchemy.engine.Connection,
                *,
                terminal: bool = False,
                row_count: int = 1,
                protocol: int = 4) -> uuid.UUID:
    run_id = uuid.uuid4()
    connection.execute(
        serve_state_schema.placement_normalization_runs_table.insert(
        ).values(**_run_values(
            run_id, protocol=protocol, terminal=terminal, row_count=row_count)))
    return run_id


def _insert_manifest(connection: sqlalchemy.engine.Connection,
                     *,
                     terminal: bool = False) -> uuid.UUID:
    run_id = _insert_run(connection, terminal=terminal)
    connection.execute(
        serve_state_schema.placement_normalization_rows_table.insert().values(
            **_row_values(run_id, terminal=terminal)))
    return run_id


def test_serve040_lineage_and_postgresql_only() -> None:
    assert migration_utils.SERVE_VERSION == '062'
    assert placement_normalization_authority.RECOGNIZED_ADDITIVE_REVISIONS == (
        frozenset(f'{revision:03d}' for revision in range(40, 63)))
    sqlite = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(sqlite,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    revision = scripts.get_revision('040')
    assert Path(
        revision.path).name == ('040_placement_normalization_immutability.py')
    assert revision.down_revision == '039'
    with pytest.raises(RuntimeError, match='PostgreSQL-only'):
        alembic_command.upgrade(config, '040')


def test_serve040_upgrade_freezes_and_restores_hostile_search_path(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '039')
    migration = _migration_module()
    hostile_schema = f'hostile_serve040_{uuid.uuid4().hex}'
    with postgres_engine.begin() as connection:
        quoted_hostile_schema = connection.dialect.identifier_preparer.quote(
            hostile_schema)
        connection.exec_driver_sql(f'CREATE SCHEMA {quoted_hostile_schema}')
        connection.exec_driver_sql(
            f'CREATE DOMAIN {quoted_hostile_schema}.uuid AS text')
        connection.exec_driver_sql(f'CREATE FUNCTION {quoted_hostile_schema}.'
                                   'length(pg_catalog.text) RETURNS integer '
                                   'LANGUAGE SQL IMMUTABLE AS $$SELECT 999$$')

    hostile_search_path = f'public,{hostile_schema},pg_catalog'
    hostile_url = postgres_engine.url.update_query_dict(
        {'options': f'-csearch_path={hostile_search_path}'})
    hostile_engine = sqlalchemy.create_engine(hostile_url)
    try:
        with hostile_engine.connect() as connection:
            assert connection.execute(
                sqlalchemy.text(
                    "SELECT pg_catalog.current_setting('search_path')")
            ).scalar_one() == hostile_search_path
            assert connection.exec_driver_sql(
                "SELECT length('abc'::pg_catalog.text)").scalar_one() == 999

        _upgrade(hostile_engine, '040')
        assert _revision(hostile_engine) == '040'
        with hostile_engine.connect() as connection:
            assert connection.execute(
                sqlalchemy.text(
                    "SELECT pg_catalog.current_setting('search_path')")
            ).scalar_one() == hostile_search_path
            assert connection.exec_driver_sql(
                "SELECT length('abc'::pg_catalog.text)").scalar_one() == 999
            migration._verify_catalog(connection, 'public')
            assert connection.execute(
                sqlalchemy.text(
                    "SELECT pg_catalog.current_setting('search_path')")
            ).scalar_one() == hostile_search_path
            gate_uuid_type = connection.execute(
                sqlalchemy.text('SELECT pg_catalog.format_type('
                                'attribute.atttypid, attribute.atttypmod) '
                                'FROM pg_catalog.pg_attribute AS attribute '
                                'WHERE attribute.attrelid = '
                                "pg_catalog.to_regclass('public."
                                "placement_normalization_write_fence') "
                                "AND attribute.attname = 'latest_run_id'"))
            assert gate_uuid_type.scalar_one() == 'pg_catalog.uuid'
    finally:
        hostile_engine.dispose()
        with postgres_engine.begin() as connection:
            quoted_hostile_schema = (
                connection.dialect.identifier_preparer.quote(hostile_schema))
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {quoted_hostile_schema} CASCADE')


def test_serve040_catalog_and_open_runtime_authority_are_exact(
        serve040) -> None:
    migration = _migration_module()
    with serve040.connect() as connection:
        schema = migration._current_schema(connection)
        migration._verify_catalog(connection, schema)
        triggers = migration._trigger_rows(connection, schema)
        ri_triggers = migration._ri_trigger_rows(connection, schema)
        referenced_ri_triggers = [
            trigger for trigger in migration._ri_trigger_rows(
                connection, schema,
                tuple(migration._REFERENCED_AUTHORITY_FOREIGN_KEY_CONTRACTS))
            if trigger['proname'] in {
                semantic.function
                for semantic in migration._REFERENCED_SIDE_RI_TRIGGER_SEMANTICS
            }
        ]
        authority_internal_trigger_count = connection.execute(
            sqlalchemy.text(
                'SELECT count(*) '
                'FROM pg_catalog.pg_trigger AS trigger '
                'JOIN pg_catalog.pg_class AS relation '
                'ON relation.oid = trigger.tgrelid '
                'JOIN pg_catalog.pg_namespace AS namespace '
                'ON namespace.oid = relation.relnamespace '
                'WHERE namespace.nspname = :schema '
                'AND relation.relname = ANY(CAST(:relations AS text[])) '
                'AND trigger.tgisinternal'), {
                    'schema': schema,
                    'relations':
                        [migration._GATE, migration._RUNS, migration._ROWS],
                }).scalar_one()
        functions = migration._function_rows(connection, schema)
        authority = (placement_normalization_authority.
                     assert_reader_database_authority(connection))

    assert authority.schema == 'public'
    assert authority.is_open
    assert set(triggers) == set(migration._TRIGGER_CONTRACTS)
    assert triggers[migration._RUN_IMMUTABILITY_TRIGGER]['tgtype'] == 58
    assert triggers[migration._ROW_IMMUTABILITY_TRIGGER]['tgtype'] == 58
    assert triggers[migration._TERMINAL_ACTIVATION_TRIGGER]['tgtype'] == 5
    assert all(trigger['tgenabled'] == 'A' for trigger in triggers.values())
    assert len(ri_triggers) == migration._EXPECTED_RI_TRIGGER_COUNT == 12
    assert all(trigger['tgisinternal'] is True for trigger in ri_triggers)
    assert {trigger['tgenabled'] for trigger in ri_triggers} == {'O'}
    assert len({(trigger['foreign_key_name'], trigger['proname'])
                for trigger in ri_triggers}) == 12
    assert migration._EXPECTED_REFERENCED_AUTHORITY_RI_TRIGGER_COUNT == 6
    assert len(referenced_ri_triggers) == 6
    assert {trigger['tgenabled'] for trigger in referenced_ri_triggers} == {'O'}
    assert {
        (trigger['foreign_key_name'], trigger['proname'])
        for trigger in referenced_ri_triggers
    } == {(foreign_key_name, semantic.function)
          for foreign_key_name in
          migration._REFERENCED_AUTHORITY_FOREIGN_KEY_CONTRACTS
          for semantic in migration._REFERENCED_SIDE_RI_TRIGGER_SEMANTICS}
    assert migration._EXPECTED_AUTHORITY_RELATION_INTERNAL_TRIGGER_COUNT == 18
    assert authority_internal_trigger_count == 18
    lock_source = functions[migration._RUN_LOCK_FUNCTION]['prosrc']
    terminal_source = functions[
        migration._TERMINAL_ACTIVATION_FUNCTION]['prosrc']
    assert 'pg_try_advisory_xact_lock' in lock_source
    assert "normalizer_version ~ '^4:[0-9a-f]{40}$'" in terminal_source
    assert "'historical_physical_per_gpu'" in terminal_source


@pytest.mark.parametrize('statement', [
    "UPDATE placement_normalization_runs SET release_version = 'rewritten'",
    'DELETE FROM placement_normalization_runs',
    'TRUNCATE placement_normalization_runs CASCADE',
    "UPDATE placement_normalization_rows SET outcome = 'changed'",
    'DELETE FROM placement_normalization_rows',
    'TRUNCATE placement_normalization_rows',
])
def test_serve040_ledger_is_append_only(serve040, statement) -> None:
    with serve040.begin() as connection:
        _insert_manifest(connection)
    with pytest.raises(sqlalchemy.exc.DBAPIError, match='append-only'):
        with serve040.begin() as connection:
            connection.exec_driver_sql(statement)


def test_serve040_always_trigger_rejects_replica_role(serve040) -> None:
    with serve040.begin() as connection:
        _insert_manifest(connection)
    with pytest.raises(sqlalchemy.exc.DBAPIError, match='append-only'):
        with serve040.begin() as connection:
            connection.exec_driver_sql(
                "SET LOCAL session_replication_role = 'replica'")
            connection.exec_driver_sql(
                'DELETE FROM placement_normalization_rows')


def test_serve040_replica_bypass_orphan_fails_runtime_authority(
        serve040) -> None:
    orphan_run_id = uuid.uuid4()
    with serve040.begin() as connection:
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        connection.execute(
            serve_state_schema.placement_normalization_rows_table.insert(
            ).values(**_row_values(orphan_run_id)))

    with serve040.connect() as connection:
        assert connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.placement_normalization_rows_table)
        ).scalar_one() == 1
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='absent or invalid'):
            placement_normalization_authority.assert_reader_database_authority(
                connection)


def test_serve040_upgrade_rejects_preexisting_replica_bypass_orphan(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '039')
    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(
            "SET LOCAL session_replication_role = 'replica'")
        connection.execute(
            serve_state_schema.placement_normalization_rows_table.insert(
            ).values(**_row_values(uuid.uuid4())))

    with pytest.raises(RuntimeError, match='orphan placement-normalization'):
        _upgrade(postgres_engine, '040')
    assert _revision(postgres_engine) == '039'


@pytest.mark.parametrize('statement', [
    ('INSERT INTO placement_normalization_write_fence '
     '(singleton, generation) VALUES (false, 0)'),
    'DELETE FROM placement_normalization_write_fence',
    'TRUNCATE placement_normalization_write_fence',
    ('UPDATE placement_normalization_write_fence '
     'SET generation = generation'),
])
def test_serve040_gate_rejects_direct_dml(serve040, statement) -> None:
    with pytest.raises(sqlalchemy.exc.DBAPIError, match='migration-private'):
        with serve040.begin() as connection:
            connection.exec_driver_sql(statement)


def test_serve040_rejects_two_runs_in_one_top_level_transaction(
        serve040) -> None:
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='terminal placement-normalization'):
        with serve040.begin() as connection:
            _insert_run(connection, row_count=0)
            _insert_run(connection, row_count=0)
    with serve040.connect() as connection:
        count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.placement_normalization_runs_table)
        ).scalar_one()
    assert count == 0


def test_serve040_savepoint_run_fails_transaction_binding(serve040) -> None:
    with serve040.begin() as connection:
        with pytest.raises(sqlalchemy.exc.DBAPIError,
                           match='migration-private'):
            with connection.begin_nested():
                _insert_run(connection, row_count=0)
    with serve040.connect() as connection:
        count = connection.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(
                serve_state_schema.placement_normalization_runs_table)
        ).scalar_one()
    assert count == 0


def test_serve040_terminal_run_without_row_cannot_activate_later(
        serve040) -> None:
    with serve040.begin() as connection:
        run_id = _insert_run(connection, terminal=True)
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='terminal placement-normalization activation'):
        with serve040.begin() as connection:
            connection.execute(
                serve_state_schema.placement_normalization_rows_table.insert(
                ).values(**_row_values(run_id, terminal=True)))
    with serve040.connect() as connection:
        authority = (placement_normalization_authority.
                     assert_reader_database_authority(connection))
    assert authority.is_open


def test_serve040_terminal_activation_accepts_ordinary_and_two_candidates(
        serve040) -> None:
    with serve040.begin() as connection:
        run_id = _insert_run(connection, terminal=True, row_count=3)
        connection.execute(
            serve_state_schema.placement_normalization_rows_table.insert(), [
                _row_values(
                    run_id, terminal=False, service_name='ordinary', version=1),
                _row_values(run_id,
                            terminal=True,
                            service_name='candidate-a',
                            version=2),
                _row_values(run_id,
                            terminal=True,
                            service_name='candidate-b',
                            version=3),
            ])
    with serve040.connect() as connection:
        authority = (placement_normalization_authority.
                     assert_reader_database_authority(connection))
        assert authority.terminal_run_id == run_id
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='terminal'):
            (placement_normalization_authority.
             assert_downgrade_database_authority(connection))


def test_serve040_terminal_transaction_rejects_later_older_run(
        serve040) -> None:
    with serve040.begin() as connection:
        terminal_run_id = _insert_manifest(connection, terminal=True)
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='terminal placement-normalization'):
        with serve040.begin() as connection:
            _insert_run(connection, protocol=3, row_count=0)
    with serve040.connect() as connection:
        run_ids = set(
            connection.execute(
                sqlalchemy.select(
                    serve_state_schema.placement_normalization_runs_table.c.
                    run_id)).scalars())
    assert run_ids == {terminal_run_id}


@pytest.mark.parametrize(('isolation_level', 'expected_sqlstate'), [
    ('READ COMMITTED', '55000'),
    ('REPEATABLE READ', '40001'),
    ('SERIALIZABLE', '40001'),
])
def test_serve040_old_snapshot_after_terminal_commit_rejects_older_writer(
        serve040, isolation_level, expected_sqlstate) -> None:
    stale = serve040.connect().execution_options(
        isolation_level=isolation_level)
    transaction = stale.begin()
    try:
        # Pin a transaction snapshot before terminal activation.  Under READ
        # COMMITTED this statement snapshot expires; the other two levels must
        # fail through PostgreSQL's concurrent-update serialization check.
        assert stale.exec_driver_sql(
            'SELECT terminal_run_id '
            'FROM placement_normalization_write_fence').scalar_one() is None
        with serve040.begin() as terminal:
            terminal_run_id = _insert_manifest(terminal, terminal=True)
        with pytest.raises(sqlalchemy.exc.DBAPIError) as error:
            _insert_run(stale, protocol=3, row_count=0)
        assert error.value.orig.pgcode == expected_sqlstate
    finally:
        if transaction.is_active:
            transaction.rollback()
        stale.close()

    with serve040.connect() as connection:
        run_ids = set(
            connection.execute(
                sqlalchemy.select(
                    serve_state_schema.placement_normalization_runs_table.c.
                    run_id)).scalars())
    assert run_ids == {terminal_run_id}


@pytest.mark.parametrize('isolation_level', [
    'READ COMMITTED',
    'REPEATABLE READ',
    'SERIALIZABLE',
])
def test_serve040_overlapping_uncommitted_terminal_rejects_older_writer(
        serve040, isolation_level) -> None:
    terminal = serve040.connect()
    terminal_transaction = terminal.begin()
    contender = serve040.connect().execution_options(
        isolation_level=isolation_level)
    contender_transaction = contender.begin()
    try:
        terminal_run_id = _insert_manifest(terminal, terminal=True)
        contender.exec_driver_sql("SET LOCAL lock_timeout = '250ms'")
        contender.exec_driver_sql("SET LOCAL statement_timeout = '1s'")
        with pytest.raises(sqlalchemy.exc.DBAPIError,
                           match='normalization authority is busy') as error:
            _insert_run(contender, protocol=3, row_count=0)
        # 55000 proves the nonblocking advisory fence fired.  A blocking lock
        # path would instead produce lock-timeout or query-cancel SQLSTATE.
        assert error.value.orig.pgcode == '55000'
        contender_transaction.rollback()
        terminal_transaction.commit()
    finally:
        if contender_transaction.is_active:
            contender_transaction.rollback()
        contender.close()
        if terminal_transaction.is_active:
            terminal_transaction.rollback()
        terminal.close()

    with serve040.connect() as connection:
        run_ids = set(
            connection.execute(
                sqlalchemy.select(
                    serve_state_schema.placement_normalization_runs_table.c.
                    run_id)).scalars())
        authority = (placement_normalization_authority.
                     assert_reader_database_authority(connection))
    assert run_ids == {terminal_run_id}
    assert authority.terminal_run_id == terminal_run_id


def test_serve040_preterminal_downgrade_removes_only_040_catalog(
        serve040) -> None:
    with serve040.begin() as connection:
        run_id = _insert_manifest(connection)
    alembic_command.downgrade(_config(serve040), '039')
    assert _revision(serve040) == '039'
    migration = _migration_module()
    with serve040.connect() as connection:
        migration._assert_uninstalled(connection, 'public')
        surviving_ri_triggers = migration._ri_trigger_rows(
            connection, 'public', (migration._ROW_RUN_FOREIGN_KEY,))
        assert len(surviving_ri_triggers) == 4
        assert {trigger['tgenabled'] for trigger in surviving_ri_triggers
               } == {'O'}
    with serve040.begin() as connection:
        connection.execute(
            serve_state_schema.placement_normalization_runs_table.update().
            where(serve_state_schema.placement_normalization_runs_table.c.run_id
                  == run_id).values(release_version='rewritten'))


def test_serve040_terminal_downgrade_fails_closed(serve040) -> None:
    with serve040.begin() as connection:
        _insert_manifest(connection, terminal=True)
    with pytest.raises(RuntimeError, match='authority is terminal'):
        alembic_command.downgrade(_config(serve040), '039')
    assert _revision(serve040) == '040'


def test_serve040_reader_gate_lock_blocks_downgrade_until_transaction_end(
        serve040) -> None:
    downgrade_started = threading.Event()
    downgrade_finished = threading.Event()

    def _downgrade() -> None:
        downgrade_started.set()
        alembic_command.downgrade(_config(serve040), '039')
        downgrade_finished.set()

    reader = serve040.connect()
    try:
        authority = (placement_normalization_authority.
                     assert_reader_database_authority(reader))
        assert authority.is_open
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_downgrade)
            assert downgrade_started.wait(timeout=5)
            waiting_pid = _wait_for_waiting_relation_lock(
                serve040,
                schema=authority.schema,
                relation=placement_normalization_authority.AUTHORITY_GATE,
                mode='AccessExclusiveLock')
            assert waiting_pid > 0
            assert not downgrade_finished.is_set()
            reader.rollback()
            future.result(timeout=15)
    finally:
        reader.close()
    assert downgrade_finished.is_set()
    assert _revision(serve040) == '039'


def test_serve040_writer_preflight_after_completed_downgrade_leaks_no_lock(
        serve040) -> None:
    alembic_command.downgrade(_config(serve040), '039')
    assert _revision(serve040) == '039'

    lock_name = 'skyserve-placement-contract-normalization-v1'
    with serve040.connect() as connection:
        with pytest.raises(
                placement_contract_normalization.NormalizationBlocker,
                match='database write authority is absent or invalid'):
            placement_contract_normalization._acquire_writer_session_lock(
                connection)
        with connection.begin():
            leaked = connection.execute(
                sqlalchemy.text('SELECT pg_advisory_unlock('
                                'hashtextextended(:name, 0))'), {
                                    'name': lock_name
                                }).scalar_one()
    assert leaked is False


def test_serve040_runtime_authority_rejects_trigger_body_drift(
        serve040) -> None:
    migration = _migration_module()
    with serve040.begin() as connection:
        function = migration._qualified(connection, 'public',
                                        migration._IMMUTABILITY_FUNCTION)
        connection.exec_driver_sql(
            f'CREATE OR REPLACE FUNCTION {function}() RETURNS trigger '
            "LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = "
            "pg_catalog, public AS $$BEGIN RETURN NULL; END;$$")
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='absent or invalid'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


def test_serve040_runtime_authority_rejects_assertion_body_drift(
        serve040) -> None:
    migration = _migration_module()
    with serve040.begin() as connection:
        function = migration._qualified(connection, 'public',
                                        migration._RUNTIME_ASSERT_FUNCTION)
        connection.exec_driver_sql(
            f'CREATE OR REPLACE FUNCTION {function}() RETURNS boolean '
            'LANGUAGE plpgsql VOLATILE SECURITY DEFINER '
            'SET search_path = pg_catalog, public '
            'AS $$BEGIN RETURN TRUE; END;$$')
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='Revision-040 assertion'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


def test_serve040_runtime_authority_rejects_version_envelope_drift(
        serve040) -> None:
    with serve040.begin() as connection:
        connection.exec_driver_sql('ALTER TABLE alembic_version_serve_state_db '
                                   'ADD COLUMN rewritten boolean')
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='version relation envelope'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


@pytest.mark.parametrize(('statement', 'expected_message'), [
    (('GRANT SELECT (generation) ON '
      'placement_normalization_write_fence TO PUBLIC'), 'absent or invalid'),
    (('GRANT SELECT (release_version) ON '
      'placement_normalization_runs TO PUBLIC'), 'absent or invalid'),
    (('GRANT SELECT (classification) ON '
      'placement_normalization_rows TO PUBLIC'), 'absent or invalid'),
    (('GRANT SELECT (version_num) ON '
      'alembic_version_serve_state_db TO PUBLIC'), 'version relation envelope'),
])
def test_serve040_runtime_authority_rejects_column_acl(
        serve040, statement, expected_message) -> None:
    with serve040.begin() as connection:
        connection.exec_driver_sql(statement)
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match=expected_message):
            placement_normalization_authority.assert_reader_database_authority(
                connection)


@pytest.mark.parametrize(('index_name', 'expected_message'), [
    ('skyserve040_normalization_fence_pk', 'absent or invalid'),
    ('placement_normalization_runs_pkey', 'absent or invalid'),
    ('placement_normalization_rows_version_idx', 'absent or invalid'),
    ('alembic_version_serve_state_db_pkc', 'version relation envelope'),
])
def test_serve040_runtime_authority_rejects_index_storage_options(
        serve040, index_name, expected_message) -> None:
    with serve040.begin() as connection:
        index = connection.dialect.identifier_preparer.quote(index_name)
        connection.exec_driver_sql(f'ALTER INDEX {index} SET (fillfactor = 70)')
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match=expected_message):
            placement_normalization_authority.assert_reader_database_authority(
                connection)


@pytest.mark.parametrize('statement', [
    ('ALTER TABLE placement_normalization_write_fence '
     'ADD COLUMN unexpected boolean'),
    ('ALTER TABLE placement_normalization_runs '
     'ADD COLUMN unexpected boolean'),
    ('ALTER TABLE placement_normalization_rows '
     'ADD COLUMN unexpected boolean'),
    ('ALTER TABLE placement_normalization_runs ALTER COLUMN release_version '
     'TYPE varchar(64)'),
    ('ALTER TABLE placement_normalization_rows ALTER COLUMN '
     "contract_projection SET DEFAULT '{}'::jsonb"),
    ('ALTER TABLE placement_normalization_runs DROP CONSTRAINT '
     'ck_placement_normalization_run_mode'),
    ('ALTER TABLE placement_normalization_rows DROP CONSTRAINT '
     'fk_placement_normalization_rows_run'),
    'DROP INDEX placement_normalization_rows_version_idx',
    'GRANT SELECT ON placement_normalization_runs TO PUBLIC',
    'ALTER TABLE placement_normalization_rows ENABLE ROW LEVEL SECURITY',
    ('ALTER TABLE placement_normalization_runs DISABLE TRIGGER '
     'skyserve040_placement_normalization_runs_lock'),
])
def test_serve040_runtime_authority_rejects_catalog_drift(serve040,
                                                          statement) -> None:
    with serve040.begin() as connection:
        connection.exec_driver_sql(statement)
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='absent or invalid'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


def test_serve040_runtime_authority_rejects_internal_ri_trigger_tamper(
        serve040) -> None:
    migration = _migration_module()
    with serve040.begin() as connection:
        trigger = next(
            row for row in migration._ri_trigger_rows(connection, 'public')
            if row['foreign_key_name'] == migration._ROW_RUN_FOREIGN_KEY and
            row['proname'] == 'RI_FKey_check_ins')
        relation = migration._qualified(connection, 'public',
                                        trigger['source_relation'])
        trigger_name = connection.dialect.identifier_preparer.quote(
            trigger['tgname'])
        connection.exec_driver_sql(
            f'ALTER TABLE {relation} DISABLE TRIGGER {trigger_name}')

    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='absent or invalid'):
            placement_normalization_authority.assert_reader_database_authority(
                connection)


def test_serve040_runtime_authority_ignores_unrelated_same_name_fk(
        serve040) -> None:
    migration = _migration_module()
    with serve040.begin() as connection:
        connection.exec_driver_sql(
            'CREATE TABLE unrelated_parent (id uuid PRIMARY KEY)')
        connection.exec_driver_sql(
            'CREATE TABLE unrelated_child ('
            'parent_id uuid, '
            f'CONSTRAINT {migration._ROW_RUN_FOREIGN_KEY} '
            'FOREIGN KEY (parent_id) REFERENCES unrelated_parent(id))')
    with serve040.connect() as connection:
        authority = (placement_normalization_authority.
                     assert_reader_database_authority(connection))
    assert authority.is_open


def test_serve040_runtime_authority_rejects_unexpected_reference_to_runs(
        serve040) -> None:
    with serve040.begin() as connection:
        connection.exec_driver_sql(
            'CREATE TABLE unexpected_run_reference ('
            'run_id uuid REFERENCES placement_normalization_runs(run_id) '
            'ON DELETE RESTRICT)')
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='absent or invalid'):
            placement_normalization_authority.assert_reader_database_authority(
                connection)


def test_serve040_runtime_authority_rejects_same_name_fk_on_wrong_column(
        serve040) -> None:
    migration = _migration_module()
    foreign_key_name = ('fk_services_placement_normalization_requested_run')
    with serve040.begin() as connection:
        services = migration._qualified(connection, 'public',
                                        migration._SERVICES)
        runs = migration._qualified(connection, 'public', migration._RUNS)
        quoted_foreign_key_name = (
            connection.dialect.identifier_preparer.quote(foreign_key_name))
        connection.exec_driver_sql(f'ALTER TABLE {services} DROP CONSTRAINT '
                                   f'{quoted_foreign_key_name}')
        connection.exec_driver_sql(
            f'ALTER TABLE {services} ADD CONSTRAINT '
            f'{quoted_foreign_key_name} FOREIGN KEY '
            '(placement_normalization_loaded_run_id) '
            f'REFERENCES {runs}(run_id) ON DELETE RESTRICT')
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='absent or invalid'):
            placement_normalization_authority.assert_reader_database_authority(
                connection)


def test_serve040_runtime_authority_rejects_open_gate_with_terminal_row(
        serve040) -> None:
    migration = _migration_module()
    with serve040.begin() as connection:
        rows = migration._qualified(connection, 'public', migration._ROWS)
        trigger = connection.dialect.identifier_preparer.quote(
            migration._TERMINAL_ACTIVATION_TRIGGER)
        connection.exec_driver_sql(
            f'ALTER TABLE {rows} DISABLE TRIGGER {trigger}')
        run_id = _insert_manifest(connection, terminal=True)
        connection.exec_driver_sql(
            f'ALTER TABLE {rows} ENABLE ALWAYS TRIGGER {trigger}')
    with serve040.connect() as connection:
        terminal_run_id = connection.execute(
            sqlalchemy.text(
                'SELECT terminal_run_id '
                'FROM placement_normalization_write_fence')).scalar_one()
        assert terminal_run_id is None
        assert run_id is not None
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='absent or invalid'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


def test_serve040_runtime_authority_rejects_terminal_gate_without_tuple(
        serve040) -> None:
    migration = _migration_module()
    with serve040.begin() as connection:
        run_id = _insert_manifest(connection)
        gate = migration._qualified(connection, 'public', migration._GATE)
        trigger = connection.dialect.identifier_preparer.quote(
            migration._GATE_UPDATE_GUARD_TRIGGER)
        connection.exec_driver_sql(
            f'ALTER TABLE {gate} DISABLE TRIGGER {trigger}')
        connection.exec_driver_sql(
            f'UPDATE {gate} SET terminal_run_id = latest_run_id, '
            'terminal_xid = admitted_xid')
        connection.exec_driver_sql(
            f'ALTER TABLE {gate} ENABLE ALWAYS TRIGGER {trigger}')
    with serve040.connect() as connection:
        assert run_id is not None
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='absent or invalid'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


def test_serve040_runtime_authority_rejects_admission_xid_mismatch(
        serve040) -> None:
    migration = _migration_module()
    with serve040.begin() as connection:
        _insert_manifest(connection)
        gate = migration._qualified(connection, 'public', migration._GATE)
        trigger = connection.dialect.identifier_preparer.quote(
            migration._GATE_UPDATE_GUARD_TRIGGER)
        connection.exec_driver_sql(
            f'ALTER TABLE {gate} DISABLE TRIGGER {trigger}')
        connection.exec_driver_sql(
            f'UPDATE {gate} SET admitted_xid = '
            "((admitted_xid::text::bigint + 1)::text::xid8)")
        connection.exec_driver_sql(
            f'ALTER TABLE {gate} ENABLE ALWAYS TRIGGER {trigger}')
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='absent or invalid'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


@pytest.mark.parametrize('shadow_kind', ['relation', 'function'])
def test_serve040_runtime_authority_rejects_temp_shadows(serve040,
                                                         shadow_kind) -> None:
    with serve040.connect() as connection:
        if shadow_kind == 'relation':
            connection.exec_driver_sql('CREATE TEMP TABLE services (id int)')
        else:
            connection.exec_driver_sql(
                'CREATE FUNCTION pg_temp.'
                f'{placement_normalization_authority.AUTHORITY_FUNCTION}() '
                'RETURNS boolean LANGUAGE SQL AS $$SELECT true$$')
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='temporary authority shadows'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


def test_serve040_catalog_compilation_restores_hostile_search_path(
        serve040) -> None:
    migration = _migration_module()
    hostile_schema = f'hostile_serve040_{uuid.uuid4().hex}'
    with serve040.connect() as connection:
        quoted_hostile_schema = connection.dialect.identifier_preparer.quote(
            hostile_schema)
        connection.exec_driver_sql(f'CREATE SCHEMA {quoted_hostile_schema}')
        connection.exec_driver_sql('CREATE DOMAIN pg_temp.uuid AS text')
        connection.exec_driver_sql(
            f'CREATE FUNCTION {quoted_hostile_schema}.length(pg_catalog.text) '
            'RETURNS integer LANGUAGE SQL IMMUTABLE AS $$SELECT 999$$')
        hostile_search_path = (
            f'{quoted_hostile_schema}, pg_temp, public, pg_catalog')
        connection.exec_driver_sql(f'SET search_path = {hostile_search_path}')
        try:
            before = connection.execute(
                sqlalchemy.text(
                    "SELECT pg_catalog.current_setting('search_path')")
            ).scalar_one()
            assert before == hostile_search_path
            assert connection.exec_driver_sql(
                "SELECT length('abc'::pg_catalog.text)").scalar_one() == 999

            expected_body = migration.expected_runtime_assertion_body(
                connection, 'public')
            assert expected_body
            assert connection.execute(
                sqlalchemy.text(
                    "SELECT pg_catalog.current_setting('search_path')")
            ).scalar_one() == before
            authority = (placement_normalization_authority.
                         assert_reader_database_authority(connection))
            assert authority.is_open
            assert connection.execute(
                sqlalchemy.text(
                    "SELECT pg_catalog.current_setting('search_path')")
            ).scalar_one() == 'pg_catalog'
            assert connection.exec_driver_sql(
                "SELECT length('abc'::pg_catalog.text)").scalar_one() == 3
            assert migration.expected_runtime_assertion_body(
                connection, 'public') == expected_body
            assert connection.execute(
                sqlalchemy.text(
                    "SELECT pg_catalog.current_setting('search_path')")
            ).scalar_one() == 'pg_catalog'
        finally:
            connection.exec_driver_sql(
                f'DROP SCHEMA {quoted_hostile_schema} CASCADE')


def test_serve040_runtime_authority_rejects_wrong_revision(serve040) -> None:
    with serve040.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE alembic_version_serve_state_db SET version_num = '039'")
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='does not own one recognized additive'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


@pytest.mark.parametrize('revision', [
    '041', '042', '043', '044', '045', '046', '047', '048', '049', '050', '051',
    '052', '053', '054', '055', '056', '057', '058', '059', '060', '061', '062'
])
def test_serve040_runtime_authority_accepts_recognized_additive_head(
        serve040, revision: str) -> None:
    if int(revision) >= 55:
        # Serve055's FK target is owned by the global user-state lineage. The
        # production migration job establishes it before advancing Serve.
        global_user_state_schema.user_table.create(serve040, checkfirst=True)
    _upgrade(serve040, revision)

    with serve040.connect() as connection:
        authority = (placement_normalization_authority.
                     assert_reader_database_authority(connection))

    assert authority.schema == 'public'
    assert authority.is_open


def test_later_head_still_rejects_revision_040_function_drift(serve040) -> None:
    _upgrade(serve040, '052')
    function = placement_normalization_authority.AUTHORITY_FUNCTION
    with serve040.begin() as connection:
        connection.exec_driver_sql(
            f'CREATE OR REPLACE FUNCTION public.{function}() '
            'RETURNS boolean LANGUAGE plpgsql AS '
            '$$BEGIN RETURN TRUE; END;$$')

    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='Revision-040 assertion'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


def test_serve040_runtime_authority_rejects_unknown_later_head(
        serve040) -> None:
    with serve040.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE alembic_version_serve_state_db SET version_num = '999'")
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='does not own one recognized additive'):
            (placement_normalization_authority.assert_reader_database_authority(
                connection))


def test_serve040_runtime_authority_rejects_duplicate_revision_identity(
        serve040) -> None:
    function = placement_normalization_authority.AUTHORITY_FUNCTION
    try:
        with serve040.begin() as connection:
            connection.exec_driver_sql('CREATE SCHEMA duplicate_040')
            for relation in (placement_normalization_authority.VERSION_RELATION,
                             placement_normalization_authority.AUTHORITY_GATE,
                             placement_normalization_authority.RUNS_RELATION,
                             placement_normalization_authority.ROWS_RELATION):
                connection.exec_driver_sql(
                    f'CREATE TABLE duplicate_040.{relation} (value text)')
            connection.exec_driver_sql(
                'INSERT INTO duplicate_040.'
                f'{placement_normalization_authority.VERSION_RELATION} '
                "VALUES ('040')")
            connection.exec_driver_sql(
                f'CREATE FUNCTION duplicate_040.{function}() RETURNS boolean '
                'LANGUAGE plpgsql AS $$BEGIN RETURN TRUE; END;$$')
        with serve040.connect() as connection:
            with pytest.raises(placement_normalization_authority.
                               PlacementNormalizationAuthorityError,
                               match='exactly one persistent'):
                (placement_normalization_authority.
                 assert_reader_database_authority(connection))
    finally:
        with serve040.begin() as connection:
            connection.exec_driver_sql(
                'DROP SCHEMA IF EXISTS duplicate_040 CASCADE')


def test_writer_session_lock_proof_is_exact(serve040) -> None:
    lock_name = 'skyserve-placement-contract-normalization-v1'
    with serve040.connect() as connection:
        with pytest.raises(placement_normalization_authority.
                           PlacementNormalizationAuthorityError,
                           match='does not own'):
            placement_normalization_authority.assert_writer_session_lock(
                connection, lock_name)
        acquired = connection.execute(
            sqlalchemy.text('SELECT pg_try_advisory_lock('
                            'hashtextextended(:name, 0))'), {
                                'name': lock_name
                            }).scalar_one()
        assert acquired is True
        placement_normalization_authority.assert_writer_session_lock(
            connection, lock_name)
        assert connection.execute(
            sqlalchemy.text('SELECT pg_advisory_unlock('
                            'hashtextextended(:name, 0))'), {
                                'name': lock_name
                            }).scalar_one() is True
        connection.rollback()
        with connection.begin():
            assert connection.execute(
                sqlalchemy.text('SELECT pg_try_advisory_xact_lock('
                                'hashtextextended(:name, 0))'), {
                                    'name': lock_name
                                }).scalar_one() is True
            with pytest.raises(placement_normalization_authority.
                               PlacementNormalizationAuthorityError,
                               match='lost its session'):
                placement_normalization_authority.reassert_writer_session_lock(
                    connection, lock_name)
        with connection.begin():
            assert connection.execute(
                sqlalchemy.text('SELECT pg_try_advisory_lock('
                                'hashtextextended(:name, 0))'), {
                                    'name': lock_name
                                }).scalar_one() is True
            assert connection.execute(
                sqlalchemy.text('SELECT pg_try_advisory_xact_lock('
                                'hashtextextended(:name, 0))'), {
                                    'name': lock_name
                                }).scalar_one() is True
            placement_normalization_authority.reassert_writer_session_lock(
                connection, lock_name)
            assert connection.execute(
                sqlalchemy.text('SELECT pg_advisory_unlock('
                                'hashtextextended(:name, 0))'), {
                                    'name': lock_name
                                }).scalar_one() is True
