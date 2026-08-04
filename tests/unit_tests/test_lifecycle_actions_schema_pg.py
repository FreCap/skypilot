"""Real-PostgreSQL tests for lifecycle-actions revision 001."""
# pylint: disable=protected-access,redefined-outer-name

import contextlib
import datetime
import importlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from unittest import mock
import uuid

from alembic import command as alembic_command
from alembic import migration
from alembic import operations
import pytest
import sqlalchemy

from sky.lifecycle_actions import _schema
from sky.lifecycle_actions import state
from sky.utils.db import migration_utils

_POSTGRES_URI = os.environ.get('SKYPILOT_TEST_POSTGRES_URI')
testcontainers_postgres = (None if _POSTGRES_URI is not None else
                           pytest.importorskip('testcontainers.postgres'))
pytest.importorskip('psycopg2')

pytestmark = [
    pytest.mark.skipif(
        _POSTGRES_URI is None and shutil.which('docker') is None,
        reason='docker unavailable; skipping lifecycle-actions PostgreSQL tests'
    ),
    pytest.mark.xdist_group(name='lifecycle_actions_schema_pg'),
]

_MIGRATION = importlib.import_module(
    'sky.schemas.db.lifecycle_actions.001_initial_schema')
_TABLES = {
    'lifecycle_store_identity',
    'lifecycle_ownership_scopes',
}
_VERSION_TABLE = 'alembic_version_lifecycle_actions_db'
_PILOT_KEY = {
    'domain': 'VOLUME',
    'operation_subset': 'KUBERNETES_PVC_OWNED_LIFECYCLE_V1',
    'store_mode': 'CENTRAL_POSTGRESQL',
}
_UUID4 = uuid.UUID('00000000-0000-4000-8000-000000000001')
_UUID1 = uuid.UUID('00000000-0000-1000-8000-000000000001')
_SAFE_IDENTIFIER = re.compile(r'^[a-z][a-z0-9_]*$')


def _quoted(identifier: str) -> str:
    assert _SAFE_IDENTIFIER.fullmatch(identifier)
    return f'"{identifier}"'


@pytest.fixture(scope='module')
def postgres_engine():
    container = None
    if _POSTGRES_URI is not None:
        root_engine = sqlalchemy.create_engine(_POSTGRES_URI)
    else:
        assert testcontainers_postgres is not None
        try:
            container = testcontainers_postgres.PostgresContainer('postgres:16')
            container.start()
        except Exception as e:  # pylint: disable=broad-except
            pytest.skip(f'could not start postgres container: {e}')
        root_engine = sqlalchemy.create_engine(container.get_connection_url())

    schema_name = f'lifecycle_actions_test_{uuid.uuid4().hex}'
    with root_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {_quoted(schema_name)}')
    test_url = root_engine.url.update_query_dict(
        {'options': f'-csearch_path={schema_name}'})
    engine = sqlalchemy.create_engine(test_url, pool_size=8, max_overflow=0)
    try:
        yield engine
    finally:
        engine.dispose()
        with root_engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {_quoted(schema_name)} CASCADE')
        root_engine.dispose()
        if container is not None:
            container.stop()


def _current_schema(engine: sqlalchemy.engine.Engine) -> str:
    with engine.connect() as connection:
        schema_name = connection.execute(
            sqlalchemy.text('SELECT current_schema()')).scalar_one()
    assert isinstance(schema_name, str)
    assert _SAFE_IDENTIFIER.fullmatch(schema_name)
    return schema_name


def _reset_schema(engine: sqlalchemy.engine.Engine) -> None:
    schema_name = _current_schema(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f'DROP SCHEMA {_quoted(schema_name)} CASCADE')
        connection.exec_driver_sql(f'CREATE SCHEMA {_quoted(schema_name)}')


def _migration_call(engine: sqlalchemy.engine.Engine, function) -> None:
    with engine.begin() as connection:
        context = migration.MigrationContext.configure(connection)
        with operations.Operations.context(context):
            function()


def _alembic_downgrade(engine: sqlalchemy.engine.Engine) -> None:
    config = migration_utils.get_alembic_config(
        engine, migration_utils.LIFECYCLE_ACTIONS_DB_NAME)
    alembic_command.downgrade(config, 'base')


def _catalog_shape(engine: sqlalchemy.engine.Engine) -> dict[str, tuple]:
    with engine.connect() as connection:
        tables = tuple(
            connection.execute(
                sqlalchemy.text("""
                    SELECT c.relname
                    FROM pg_catalog.pg_class AS c
                    JOIN pg_catalog.pg_namespace AS n
                      ON n.oid = c.relnamespace
                    WHERE n.nspname = current_schema()
                      AND c.relkind = 'r'
                      AND c.relname LIKE 'lifecycle_%'
                    ORDER BY c.relname
                """)).scalars())
        columns = tuple(
            tuple(row) for row in connection.execute(
                sqlalchemy.text("""
                    SELECT c.relname, a.attnum, a.attname,
                           pg_catalog.format_type(a.atttypid, a.atttypmod),
                           a.attnotnull,
                           pg_catalog.pg_get_expr(
                               d.adbin, d.adrelid, false)
                    FROM pg_catalog.pg_attribute AS a
                    JOIN pg_catalog.pg_class AS c
                      ON c.oid = a.attrelid
                    JOIN pg_catalog.pg_namespace AS n
                      ON n.oid = c.relnamespace
                    LEFT JOIN pg_catalog.pg_attrdef AS d
                      ON d.adrelid = a.attrelid
                     AND d.adnum = a.attnum
                    WHERE n.nspname = current_schema()
                      AND c.relkind = 'r'
                      AND c.relname LIKE 'lifecycle_%'
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY c.relname, a.attnum
                """)))
        constraints = tuple(
            tuple(row) for row in connection.execute(
                sqlalchemy.text("""
                    SELECT c.relname, k.conname, k.contype,
                           pg_catalog.pg_get_constraintdef(k.oid, false)
                    FROM pg_catalog.pg_constraint AS k
                    JOIN pg_catalog.pg_class AS c
                      ON c.oid = k.conrelid
                    JOIN pg_catalog.pg_namespace AS n
                      ON n.oid = c.relnamespace
                    WHERE n.nspname = current_schema()
                      AND c.relname LIKE 'lifecycle_%'
                    ORDER BY c.relname, k.conname
                """)))
        indexes = tuple(
            tuple(row) for row in connection.execute(
                sqlalchemy.text("""
                    SELECT tablename, indexname, indexdef
                    FROM pg_catalog.pg_indexes
                    WHERE schemaname = current_schema()
                      AND tablename LIKE 'lifecycle_%'
                    ORDER BY tablename, indexname
                """)))
    return {
        'tables': tables,
        'columns': columns,
        'constraints': constraints,
        'indexes': indexes,
    }


def _database_rows(engine: sqlalchemy.engine.Engine) -> dict[str, tuple]:
    with engine.connect() as connection:
        stores = tuple(
            tuple(row) for row in connection.execute(
                sqlalchemy.select(
                    _schema._STORE_IDENTITY.c.store_key,
                    _schema._STORE_IDENTITY.c.store_uuid,
                    _schema._STORE_IDENTITY.c.schema_version,
                    _schema._STORE_IDENTITY.c.writer_authority_digest,
                    _schema._STORE_IDENTITY.c.created_at,
                ).order_by(_schema._STORE_IDENTITY.c.store_key)))
        scopes = tuple(
            tuple(row) for row in connection.execute(
                sqlalchemy.select(
                    _schema._OWNERSHIP_SCOPES.c.domain,
                    _schema._OWNERSHIP_SCOPES.c.operation_subset,
                    _schema._OWNERSHIP_SCOPES.c.store_mode,
                    _schema._OWNERSHIP_SCOPES.c.routing_mode,
                    _schema._OWNERSHIP_SCOPES.c.minimum_lifecycle_version,
                    _schema._OWNERSHIP_SCOPES.c.ownership_epoch,
                    _schema._OWNERSHIP_SCOPES.c.authority_generation,
                    _schema._OWNERSHIP_SCOPES.c.writer_implementation_digest,
                    _schema._OWNERSHIP_SCOPES.c.
                    reconciler_implementation_digest,
                    _schema._OWNERSHIP_SCOPES.c.updated_at,
                ).order_by(_schema._OWNERSHIP_SCOPES.c.domain,
                           _schema._OWNERSHIP_SCOPES.c.operation_subset,
                           _schema._OWNERSHIP_SCOPES.c.store_mode)))
    return {'stores': stores, 'scopes': scopes}


def _database_state(engine: sqlalchemy.engine.Engine) -> dict[str, object]:
    return {
        'shape': _catalog_shape(engine),
        'rows': _database_rows(engine),
        'revision': migration_utils.get_current_alembic_revision(
            engine, migration_utils.LIFECYCLE_ACTIONS_DB_NAME),
    }


def _assert_exact_seed(engine: sqlalchemy.engine.Engine) -> uuid.UUID:
    rows = _database_rows(engine)
    assert len(rows['stores']) == 1
    assert len(rows['scopes']) == 1
    store = rows['stores'][0]
    assert store[0] == 'global'
    assert isinstance(store[1], uuid.UUID)
    assert store[1].version == 4
    assert store[1].variant == uuid.RFC_4122
    assert store[2:4] == (1, None)
    assert isinstance(store[4], datetime.datetime)
    assert store[4].utcoffset() is not None
    scope = rows['scopes'][0]
    assert scope[:9] == (
        'VOLUME',
        'KUBERNETES_PVC_OWNED_LIFECYCLE_V1',
        'CENTRAL_POSTGRESQL',
        'DARK',
        0,
        1,
        0,
        None,
        None,
    )
    assert isinstance(scope[9], datetime.datetime)
    assert scope[9].utcoffset() is not None
    return store[1]


def _replace_store(engine: sqlalchemy.engine.Engine,
                   **overrides: object) -> None:
    values: dict[str, object] = {
        'store_key': 'global',
        'store_uuid': _UUID4,
        'schema_version': 1,
        'writer_authority_digest': None,
    }
    values.update(overrides)
    with engine.begin() as connection:
        connection.execute(_schema._STORE_IDENTITY.delete())
        connection.execute(_schema._STORE_IDENTITY.insert().values(**values))


def _replace_scope(engine: sqlalchemy.engine.Engine,
                   **overrides: object) -> None:
    values: dict[str, object] = {
        **_PILOT_KEY,
        'routing_mode': 'DARK',
        'minimum_lifecycle_version': 0,
        'ownership_epoch': 1,
        'authority_generation': 0,
        'writer_implementation_digest': None,
        'reconciler_implementation_digest': None,
    }
    values.update(overrides)
    with engine.begin() as connection:
        connection.execute(_schema._OWNERSHIP_SCOPES.delete())
        connection.execute(_schema._OWNERSHIP_SCOPES.insert().values(**values))


def test_migration_matches_runtime_metadata_and_exact_table_set(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    _migration_call(postgres_engine, _MIGRATION.upgrade)
    migration_shape = _catalog_shape(postgres_engine)

    assert set(migration_shape['tables']) == _TABLES
    assert {row[1] for row in migration_shape['indexes']} == {
        'pk_lifecycle_store_identity',
        'pk_lifecycle_ownership_scopes',
    }
    assert all(
        'CREATE UNIQUE INDEX' in row[2] for row in migration_shape['indexes'])
    constraints = {row[1] for row in migration_shape['constraints']}
    assert 'ck_lifecycle_store_identity_m3s2_unsealed' in constraints
    assert 'ck_lifecycle_ownership_scopes_m3s2_inert' in constraints

    _reset_schema(postgres_engine)
    with postgres_engine.begin() as connection:
        _schema._METADATA.create_all(connection)
    assert _catalog_shape(postgres_engine) == migration_shape


def test_upgrade_seeds_exact_inert_foundation(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    _migration_call(postgres_engine, _MIGRATION.upgrade)

    _assert_exact_seed(postgres_engine)
    with postgres_engine.connect() as connection:
        assert connection.execute(
            sqlalchemy.text("""
                SELECT isfinite(created_at)
                FROM lifecycle_store_identity
            """)).scalar_one() is True
        assert connection.execute(
            sqlalchemy.text("""
                SELECT isfinite(updated_at)
                FROM lifecycle_ownership_scopes
            """)).scalar_one() is True


@pytest.mark.parametrize('overrides', [
    pytest.param({'store_key': 'other'}, id='non-global-key'),
    pytest.param({'store_uuid': _UUID1}, id='non-v4-uuid'),
    pytest.param({'schema_version': 2}, id='schema-version'),
    pytest.param({'writer_authority_digest': 'a' * 64}, id='well-formed-seal'),
    pytest.param({'writer_authority_digest': 'A' * 64}, id='uppercase-seal'),
    pytest.param({'writer_authority_digest': 'a' * 63}, id='short-seal'),
    pytest.param({'writer_authority_digest': 'g' * 64}, id='nonhex-seal'),
    pytest.param({'created_at': sqlalchemy.text("'infinity'::timestamptz")},
                 id='infinite-created-at'),
])
def test_store_identity_constraints(postgres_engine: sqlalchemy.engine.Engine,
                                    overrides: dict[str, object]) -> None:
    _reset_schema(postgres_engine)
    _migration_call(postgres_engine, _MIGRATION.upgrade)
    before = _database_rows(postgres_engine)

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        _replace_store(postgres_engine, **overrides)

    assert _database_rows(postgres_engine) == before


@pytest.mark.parametrize('overrides', [
    pytest.param({'domain': 'IMAGE'}, id='domain'),
    pytest.param({'operation_subset': 'OTHER'}, id='operation-subset'),
    pytest.param({'store_mode': 'SQLITE'}, id='store-mode'),
    pytest.param({'routing_mode': 'UNKNOWN'}, id='unknown-routing-mode'),
    pytest.param({'routing_mode': 'LEGACY_OPEN'}, id='legacy-open'),
    pytest.param({'routing_mode': 'DRAINING'}, id='draining'),
    pytest.param({'routing_mode': 'ACTION_OPEN'}, id='action-open'),
    pytest.param({'minimum_lifecycle_version': -1}, id='negative-version'),
    pytest.param({'minimum_lifecycle_version': 1}, id='positive-version'),
    pytest.param({'ownership_epoch': 0}, id='zero-epoch'),
    pytest.param({'ownership_epoch': 2}, id='changed-epoch'),
    pytest.param({'authority_generation': -1}, id='negative-generation'),
    pytest.param({'authority_generation': 1}, id='changed-generation'),
    pytest.param({'writer_implementation_digest': 'a' * 64},
                 id='well-formed-writer-digest'),
    pytest.param({'writer_implementation_digest': 'A' * 64},
                 id='malformed-writer-digest'),
    pytest.param({'reconciler_implementation_digest': 'b' * 64},
                 id='well-formed-reconciler-digest'),
    pytest.param({'reconciler_implementation_digest': 'bad'},
                 id='malformed-reconciler-digest'),
    pytest.param({'updated_at': sqlalchemy.text("'infinity'::timestamptz")},
                 id='infinite-updated-at'),
])
def test_scope_constraints(postgres_engine: sqlalchemy.engine.Engine,
                           overrides: dict[str, object]) -> None:
    _reset_schema(postgres_engine)
    _migration_call(postgres_engine, _MIGRATION.upgrade)
    before = _database_rows(postgres_engine)

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        _replace_scope(postgres_engine, **overrides)

    assert _database_rows(postgres_engine) == before


@pytest.mark.parametrize('mode', ['upgrade', 'bootstrap'])
def test_alembic_upgrade_bootstrap_verify_and_newer_additive_revision(
        postgres_engine: sqlalchemy.engine.Engine,
        mode: migration_utils.MigrationMode) -> None:
    _reset_schema(postgres_engine)

    state._initialize_schema(postgres_engine, mode=mode)

    assert migration_utils.get_current_alembic_revision(
        postgres_engine, migration_utils.LIFECYCLE_ACTIONS_DB_NAME) == '001'
    assert set(_catalog_shape(postgres_engine)['tables']) == _TABLES
    _assert_exact_seed(postgres_engine)
    state._initialize_schema(postgres_engine, mode='verify')

    with postgres_engine.begin() as connection:
        connection.exec_driver_sql(
            f"UPDATE {_VERSION_TABLE} SET version_num = '002'")
    state._initialize_schema(postgres_engine, mode='verify')


def test_verify_rejects_uninitialized_lineage(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)

    with pytest.raises(RuntimeError, match='revision uninitialized'):
        state._initialize_schema(postgres_engine, mode='verify')

    with postgres_engine.connect() as connection:
        assert not sqlalchemy.inspect(connection).has_table(_VERSION_TABLE)
        assert set(sqlalchemy.inspect(connection).get_table_names()).isdisjoint(
            _TABLES)


def test_preexisting_lifecycle_table_is_not_adopted_or_repaired(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    with postgres_engine.begin() as connection:
        _schema._STORE_IDENTITY.create(connection)

    with pytest.raises(sqlalchemy.exc.SQLAlchemyError):
        state._initialize_schema(postgres_engine, mode='upgrade')

    with postgres_engine.connect() as connection:
        inspector = sqlalchemy.inspect(connection)
        assert inspector.has_table(_schema._STORE_IDENTITY.name)
        assert not inspector.has_table(_schema._OWNERSHIP_SCOPES.name)
        assert not inspector.has_table(_VERSION_TABLE)
        count = connection.execute(
            sqlalchemy.text(
                'SELECT count(*) FROM lifecycle_store_identity')).scalar_one()
        assert count == 0


def test_independent_process_first_upgrade_converges_on_one_uuid(
        postgres_engine: sqlalchemy.engine.Engine, tmp_path: Path) -> None:
    _reset_schema(postgres_engine)
    start_marker = tmp_path / 'start'
    repository_root = Path(__file__).resolve().parents[2]
    script = """
import os
from pathlib import Path
import time
import sqlalchemy
from sky.lifecycle_actions import state

marker = Path(os.environ['LIFECYCLE_TEST_START_MARKER'])
deadline = time.monotonic() + 30
while not marker.exists():
    if time.monotonic() >= deadline:
        raise RuntimeError('timed out waiting for migration race marker')
    time.sleep(0.01)
engine = sqlalchemy.create_engine(os.environ['LIFECYCLE_TEST_DATABASE_URL'])
try:
    state._initialize_schema(engine, mode='upgrade')
    foundation = state._read_foundation_from_engine(engine)
    print(f'LIFECYCLE_STORE_UUID={foundation.store_identity.store_uuid}',
          flush=True)
finally:
    engine.dispose()
"""
    environment = os.environ.copy()
    environment.update({
        'LIFECYCLE_TEST_START_MARKER': str(start_marker),
        'LIFECYCLE_TEST_DATABASE_URL':
            postgres_engine.url.render_as_string(hide_password=False),
    })
    existing_pythonpath = environment.get('PYTHONPATH')
    environment['PYTHONPATH'] = (
        str(repository_root) if not existing_pythonpath else
        f'{repository_root}{os.pathsep}{existing_pythonpath}')
    processes = [
        subprocess.Popen(
            [sys.executable, '-c', script],
            cwd=repository_root,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) for _ in range(2)
    ]
    start_marker.touch()
    results: list[tuple[int, str, str]] = []
    try:
        for process in processes:
            stdout, stderr = process.communicate(timeout=120)
            results.append((process.returncode, stdout, stderr))
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)

    for returncode, stdout, stderr in results:
        assert returncode == 0, (f'child stderr:\n{stderr}\n'
                                 f'child stdout:\n{stdout}')
    observed_uuids = {
        line.split('=', 1)[1]
        for _, stdout, _ in results
        for line in stdout.splitlines()
        if line.startswith('LIFECYCLE_STORE_UUID=')
    }
    assert len(observed_uuids) == 1
    assert observed_uuids == {str(_assert_exact_seed(postgres_engine))}
    assert migration_utils.get_current_alembic_revision(
        postgres_engine, migration_utils.LIFECYCLE_ACTIONS_DB_NAME) == '001'


def _drop_constraint(connection: sqlalchemy.engine.Connection, table: str,
                     constraint: str) -> None:
    connection.exec_driver_sql(f'ALTER TABLE {_quoted(table)} DROP CONSTRAINT '
                               f'{_quoted(constraint)}')


def _corrupt_foundation(engine: sqlalchemy.engine.Engine, case: str) -> None:
    with engine.begin() as connection:
        if case == 'missing-store':
            connection.execute(_schema._STORE_IDENTITY.delete())
        elif case == 'extra-store':
            _drop_constraint(connection, _schema._STORE_IDENTITY.name,
                             'ck_lifecycle_store_identity_singleton')
            connection.execute(_schema._STORE_IDENTITY.insert().values(
                store_key='other',
                store_uuid=uuid.uuid4(),
                schema_version=1,
                writer_authority_digest=None,
            ))
        elif case == 'non-v4-store':
            _drop_constraint(connection, _schema._STORE_IDENTITY.name,
                             'ck_lifecycle_store_identity_uuid_v4')
            connection.execute(
                _schema._STORE_IDENTITY.update().values(store_uuid=_UUID1))
        elif case == 'sealed-store':
            _drop_constraint(connection, _schema._STORE_IDENTITY.name,
                             'ck_lifecycle_store_identity_m3s2_unsealed')
            connection.execute(_schema._STORE_IDENTITY.update().values(
                writer_authority_digest='a' * 64))
        elif case == 'missing-scope':
            connection.execute(_schema._OWNERSHIP_SCOPES.delete())
        elif case == 'extra-scope':
            _drop_constraint(connection, _schema._OWNERSHIP_SCOPES.name,
                             'ck_lifecycle_ownership_scopes_domain')
            connection.execute(_schema._OWNERSHIP_SCOPES.insert().values(
                domain='IMAGE',
                operation_subset=_PILOT_KEY['operation_subset'],
                store_mode=_PILOT_KEY['store_mode'],
                routing_mode='DARK',
                minimum_lifecycle_version=0,
                ownership_epoch=1,
                authority_generation=0,
                writer_implementation_digest=None,
                reconciler_implementation_digest=None,
            ))
        elif case == 'activated-scope':
            _drop_constraint(connection, _schema._OWNERSHIP_SCOPES.name,
                             'ck_lifecycle_ownership_scopes_m3s2_inert')
            connection.execute(_schema._OWNERSHIP_SCOPES.update().values(
                routing_mode='ACTION_OPEN'))
        elif case == 'changed-epoch':
            _drop_constraint(connection, _schema._OWNERSHIP_SCOPES.name,
                             'ck_lifecycle_ownership_scopes_m3s2_inert')
            connection.execute(
                _schema._OWNERSHIP_SCOPES.update().values(ownership_epoch=2))
        elif case == 'changed-generation':
            _drop_constraint(connection, _schema._OWNERSHIP_SCOPES.name,
                             'ck_lifecycle_ownership_scopes_m3s2_inert')
            connection.execute(_schema._OWNERSHIP_SCOPES.update().values(
                authority_generation=1))
        elif case == 'writer-digest':
            _drop_constraint(connection, _schema._OWNERSHIP_SCOPES.name,
                             'ck_lifecycle_ownership_scopes_m3s2_inert')
            connection.execute(_schema._OWNERSHIP_SCOPES.update().values(
                writer_implementation_digest='b' * 64))
        else:
            raise AssertionError(f'unknown corruption case: {case}')


def _read_public_foundation(
        engine: sqlalchemy.engine.Engine) -> state.FoundationSnapshot:
    with mock.patch.object(state._db_manager, 'get_engine',
                           return_value=engine):
        return state.read_foundation()


@pytest.mark.parametrize('case', [
    'missing-store',
    'extra-store',
    'non-v4-store',
    'sealed-store',
    'missing-scope',
    'activated-scope',
    'changed-epoch',
    'changed-generation',
    'writer-digest',
])
def test_runtime_verification_rejects_corruption_without_repair(
        postgres_engine: sqlalchemy.engine.Engine, case: str) -> None:
    _reset_schema(postgres_engine)
    state._initialize_schema(postgres_engine, mode='upgrade')
    _corrupt_foundation(postgres_engine, case)
    before = _database_state(postgres_engine)

    with mock.patch.object(state.uuid,
                           'uuid4',
                           side_effect=AssertionError(
                               'verification must not generate a UUID')):
        with pytest.raises(RuntimeError):
            state._initialize_schema(postgres_engine, mode='verify')

    assert _database_state(postgres_engine) == before


def test_runtime_verification_allows_future_additional_scope(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    state._initialize_schema(postgres_engine, mode='upgrade')
    expected = _read_public_foundation(postgres_engine)

    _corrupt_foundation(postgres_engine, 'extra-scope')

    state._initialize_schema(postgres_engine, mode='verify')
    assert _read_public_foundation(postgres_engine) == expected
    assert len(_database_rows(postgres_engine)['scopes']) == 2


@contextlib.contextmanager
def _cloned_schema_engine(engine: sqlalchemy.engine.Engine):
    source_schema = _current_schema(engine)
    clone_schema = f'lifecycle_actions_clone_{uuid.uuid4().hex}'
    copied_tables = (
        _schema._STORE_IDENTITY.name,
        _schema._OWNERSHIP_SCOPES.name,
        _VERSION_TABLE,
    )
    with engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA {_quoted(clone_schema)}')
        for table in copied_tables:
            connection.exec_driver_sql(
                f'CREATE TABLE {_quoted(clone_schema)}.{_quoted(table)} '
                f'(LIKE {_quoted(source_schema)}.{_quoted(table)} INCLUDING ALL)'
            )
            connection.exec_driver_sql(
                f'INSERT INTO {_quoted(clone_schema)}.{_quoted(table)} '
                f'SELECT * FROM {_quoted(source_schema)}.{_quoted(table)}')
    clone_url = engine.url.update_query_dict(
        {'options': f'-csearch_path={clone_schema}'})
    clone_engine = sqlalchemy.create_engine(clone_url,
                                            pool_size=4,
                                            max_overflow=0)
    try:
        yield clone_engine
    finally:
        clone_engine.dispose()
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS {_quoted(clone_schema)} CASCADE')


def test_post_revision_clone_preserves_store_identity(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    state._initialize_schema(postgres_engine, mode='upgrade')
    source = _read_public_foundation(postgres_engine)

    with _cloned_schema_engine(postgres_engine) as clone_engine:
        with mock.patch.object(state.uuid,
                               'uuid4',
                               side_effect=AssertionError(
                                   'clone verification must not reseed')):
            state._initialize_schema(clone_engine, mode='verify')
            clone = _read_public_foundation(clone_engine)

    assert clone == source
    assert clone.store_identity.store_uuid == source.store_identity.store_uuid


@pytest.mark.parametrize('missing_table', [
    _schema._STORE_IDENTITY.name,
    _schema._OWNERSHIP_SCOPES.name,
])
def test_stamped_clone_missing_seed_fails_without_repair(
        postgres_engine: sqlalchemy.engine.Engine, missing_table: str) -> None:
    _reset_schema(postgres_engine)
    state._initialize_schema(postgres_engine, mode='upgrade')

    with _cloned_schema_engine(postgres_engine) as clone_engine:
        table = (_schema._STORE_IDENTITY if missing_table
                 == _schema._STORE_IDENTITY.name else _schema._OWNERSHIP_SCOPES)
        with clone_engine.begin() as connection:
            connection.execute(table.delete())
        before = _database_state(clone_engine)
        with mock.patch.object(state.uuid,
                               'uuid4',
                               side_effect=AssertionError(
                                   'clone verification must not repair')):
            with pytest.raises(RuntimeError):
                state._initialize_schema(clone_engine, mode='verify')
        assert _database_state(clone_engine) == before


def test_guarded_downgrade_exact_seed_succeeds(
        postgres_engine: sqlalchemy.engine.Engine) -> None:
    _reset_schema(postgres_engine)
    state._initialize_schema(postgres_engine, mode='upgrade')
    _assert_exact_seed(postgres_engine)

    _alembic_downgrade(postgres_engine)

    assert migration_utils.get_current_alembic_revision(
        postgres_engine, migration_utils.LIFECYCLE_ACTIONS_DB_NAME) is None
    with postgres_engine.connect() as connection:
        table_names = set(sqlalchemy.inspect(connection).get_table_names())
    assert table_names.isdisjoint(_TABLES)
    assert _VERSION_TABLE in table_names


@pytest.mark.parametrize('case', [
    'missing-store',
    'extra-store',
    'non-v4-store',
    'sealed-store',
    'missing-scope',
    'extra-scope',
    'activated-scope',
    'changed-epoch',
    'changed-generation',
    'writer-digest',
])
def test_guarded_downgrade_failure_is_atomic(
        postgres_engine: sqlalchemy.engine.Engine, case: str) -> None:
    _reset_schema(postgres_engine)
    state._initialize_schema(postgres_engine, mode='upgrade')
    _corrupt_foundation(postgres_engine, case)
    before = _database_state(postgres_engine)

    with pytest.raises(
            RuntimeError,
            match='both tables contain exactly the revision-001 inert seeds'):
        _alembic_downgrade(postgres_engine)

    assert _database_state(postgres_engine) == before


@pytest.mark.parametrize('locked_table', sorted(_TABLES))
def test_guarded_downgrade_locks_both_tables(
        postgres_engine: sqlalchemy.engine.Engine, locked_table: str) -> None:
    _reset_schema(postgres_engine)
    state._initialize_schema(postgres_engine, mode='upgrade')
    before = _database_state(postgres_engine)
    with postgres_engine.connect() as holder:
        holder_transaction = holder.begin()
        try:
            holder.exec_driver_sql(
                f'LOCK TABLE {_quoted(locked_table)} IN ROW EXCLUSIVE MODE')
            with pytest.raises(sqlalchemy.exc.OperationalError):
                with postgres_engine.begin() as connection:
                    connection.exec_driver_sql(
                        "SET LOCAL lock_timeout = '100ms'")
                    context = migration.MigrationContext.configure(connection)
                    with operations.Operations.context(context):
                        _MIGRATION.downgrade()
        finally:
            holder_transaction.rollback()

    assert _database_state(postgres_engine) == before
