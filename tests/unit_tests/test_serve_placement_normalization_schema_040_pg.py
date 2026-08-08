"""PostgreSQL contracts for placement-normalization schema revision 040."""
# pylint: disable=protected-access,redefined-outer-name

import importlib
import os
from pathlib import Path
import shutil
import uuid

from alembic import command as alembic_command
from alembic import script as alembic_script
import pytest
import sqlalchemy

from sky.serve import serve_state_schema
from sky.utils.db import migration_utils

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
_SHA = 'a' * 64


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


def _upgrade(engine: sqlalchemy.engine.Engine, revision: str) -> None:
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         revision)


def _revision(engine: sqlalchemy.engine.Engine) -> str | None:
    return migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)


def _run_values(run_id: uuid.UUID, *, protocol: int, terminal: bool,
                row_count: int) -> dict[str, object]:
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


def _row_values(run_id: uuid.UUID, *, terminal: bool) -> dict[str, object]:
    classification = ('historical_physical_per_gpu'
                      if terminal else 'explicit_v2')
    outcome = 'retired' if terminal else 'unchanged'
    return {
        'run_id': run_id,
        'service_name': 'svc',
        'version': 1,
        'classification': classification,
        'outcome': outcome,
        'original_spec_sha256': _SHA,
        'result_spec_sha256': _SHA,
        'original_row_sha256': _SHA,
        'result_row_sha256': _SHA,
        'original_column_sha256s': {},
        'result_column_sha256s': {},
        'contract_projection': None,
        'service_hash': 'service-hash',
        'service_lifecycle_epoch': 1,
        'dependency_facts': {},
    }


def _insert_manifest(connection: sqlalchemy.engine.Connection, *,
                     terminal: bool) -> uuid.UUID:
    run_id = uuid.uuid4()
    connection.execute(
        serve_state_schema.placement_normalization_runs_table.insert().values(
            **_run_values(run_id,
                          protocol=4 if terminal else 3,
                          terminal=terminal,
                          row_count=1)))
    connection.execute(
        serve_state_schema.placement_normalization_rows_table.insert().values(
            **_row_values(run_id, terminal=terminal)))
    return run_id


def test_serve040_lineage_and_postgresql_only() -> None:
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


def test_serve040_trigger_catalog_is_exact(postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '040')
    assert _revision(postgres_engine) == '040'
    migration = _migration_module()
    with postgres_engine.connect() as connection:
        schema = migration._current_schema(connection)
        migration._verify_catalog(connection, schema)
        triggers = migration._trigger_rows(connection, schema)
        functions = migration._function_rows(connection, schema)

    assert set(triggers) == set(migration._TRIGGER_CONTRACTS)
    assert triggers[migration._RUN_IMMUTABILITY_TRIGGER]['tgtype'] == 26
    assert triggers[migration._ROW_IMMUTABILITY_TRIGGER]['tgtype'] == 26
    assert triggers[migration._TERMINAL_FENCE_TRIGGER]['tgtype'] == 6
    assert all(trigger['tgenabled'] == 'O' for trigger in triggers.values())
    terminal_source = functions[migration._TERMINAL_FENCE_FUNCTION]['prosrc']
    assert 'pg_advisory_xact_lock' in terminal_source
    assert "normalizer_version ~ '^4:[0-9a-f]{40}$'" in terminal_source
    assert "'historical_physical_per_gpu'" in terminal_source


@pytest.mark.parametrize(('table_name', 'statement'), [
    ('placement_normalization_runs',
     'UPDATE placement_normalization_runs SET release_version = '
     "'rewritten' WHERE FALSE"),
    ('placement_normalization_runs',
     'DELETE FROM placement_normalization_runs WHERE FALSE'),
    ('placement_normalization_rows',
     'UPDATE placement_normalization_rows SET outcome = '
     "'changed' WHERE FALSE"),
    ('placement_normalization_rows',
     'DELETE FROM placement_normalization_rows WHERE FALSE'),
])
def test_serve040_rejects_every_update_and_delete(postgres_engine, table_name,
                                                  statement) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '040')
    with postgres_engine.begin() as connection:
        _insert_manifest(connection, terminal=False)

    with pytest.raises(sqlalchemy.exc.DBAPIError, match='append-only'):
        with postgres_engine.begin() as connection:
            connection.exec_driver_sql(statement)
    with postgres_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.text(
                f'SELECT count(*) FROM {table_name}')).scalar_one() == 1


def test_serve040_coherent_protocol_downgrade_fails_at_first_mutation(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '040')
    with postgres_engine.begin() as connection:
        run_id = _insert_manifest(connection, terminal=True)

    with pytest.raises(sqlalchemy.exc.DBAPIError, match='append-only'):
        with postgres_engine.begin() as connection:
            connection.execute(
                serve_state_schema.placement_normalization_runs_table.update(
                ).where(serve_state_schema.placement_normalization_runs_table.c.
                        run_id == run_id).values(
                            normalizer_version=f'3:{"b" * 40}'))
            pytest.fail('row rewrite must not run after the run mutation')

    with postgres_engine.connect() as connection:
        persisted = connection.execute(
            sqlalchemy.select(
                serve_state_schema.placement_normalization_runs_table.c.
                normalizer_version).where(
                    serve_state_schema.placement_normalization_runs_table.c.
                    run_id == run_id)).scalar_one()
    assert persisted == f'4:{"a" * 40}'


def test_serve040_terminal_transaction_then_rejects_protocol3_run(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '040')
    with postgres_engine.begin() as connection:
        terminal_run_id = _insert_manifest(connection, terminal=True)

    later_run_id = uuid.uuid4()
    with pytest.raises(sqlalchemy.exc.DBAPIError,
                       match='terminal placement-normalization run forbids'):
        with postgres_engine.begin() as connection:
            connection.execute(
                serve_state_schema.placement_normalization_runs_table.insert(
                ).values(**_run_values(
                    later_run_id, protocol=3, terminal=False, row_count=0)))
    with postgres_engine.connect() as connection:
        run_ids = set(
            connection.execute(
                sqlalchemy.select(
                    serve_state_schema.placement_normalization_runs_table.c.
                    run_id)).scalars())
    assert run_ids == {terminal_run_id}


def test_serve040_downgrade_removes_only_trigger_catalog(
        postgres_engine) -> None:
    _reset(postgres_engine)
    _upgrade(postgres_engine, '040')
    with postgres_engine.begin() as connection:
        run_id = _insert_manifest(connection, terminal=True)

    config = migration_utils.get_alembic_config(postgres_engine,
                                                migration_utils.SERVE_DB_NAME)
    alembic_command.downgrade(config, '039')
    assert _revision(postgres_engine) == '039'
    migration = _migration_module()
    with postgres_engine.connect() as connection:
        schema = migration._current_schema(connection)
        migration._assert_uninstalled(connection, schema)

    with postgres_engine.begin() as connection:
        connection.execute(
            serve_state_schema.placement_normalization_runs_table.update().
            where(serve_state_schema.placement_normalization_runs_table.c.run_id
                  == run_id).values(release_version='rewritten'))
        connection.execute(
            serve_state_schema.placement_normalization_rows_table.delete().
            where(serve_state_schema.placement_normalization_rows_table.c.run_id
                  == run_id))
        connection.execute(
            serve_state_schema.placement_normalization_runs_table.delete().
            where(serve_state_schema.placement_normalization_runs_table.c.run_id
                  == run_id))
