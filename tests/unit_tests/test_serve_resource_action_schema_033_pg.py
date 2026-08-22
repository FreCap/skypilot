"""Migration contracts for the combined SkyServe revision 033."""
# pylint: disable=not-callable,redefined-outer-name

import concurrent.futures
import datetime
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

from sky.serve import resource_action_state_schema as action_schema
from sky.serve import serve_state_schema
from sky.utils.db import migration_utils

_POSTGRES_URL = os.environ.get('SKYPILOT_TEST_POSTGRES_URL')
_UTC = datetime.timezone.utc
_BUCKET = datetime.datetime(2026, 8, 1, 1, 2, tzinfo=_UTC)
_DAY = datetime.datetime(2026, 8, 1, tzinfo=_UTC)
_SERVICE_UUID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_REPLICA_UUID = uuid.UUID('22222222-2222-4222-8222-222222222222')

_VERSION_TABLE = 'alembic_version_serve_state_db'
_RAW_ACTIVITY = 'serve_request_activity_history'
_DAILY_ACTIVITY = 'serve_request_activity_daily'
_RAW_PAIR_CONSTRAINT = 'serve_request_activity_history_classified_pair'
_DAILY_PAIR_CONSTRAINT = 'serve_request_activity_daily_classified_pair'

_SERVICE_ACTION_COLUMNS = (
    'resource_action_mode',
    'resource_action_mode_changed_at',
)
_REPLICA_ACTION_COLUMNS = (
    'replica_incarnation',
    'sky_cluster_record_uuid',
    'launch_action_id',
    'down_action_id',
    'launch_shadow_sample_id',
    'down_shadow_sample_id',
    'launch_shadow_coverage_id',
    'down_shadow_coverage_id',
    'desired_generation',
)
_EVIDENCE_TABLES = (
    'serve_resource_action_shadow_samples',
    'serve_resource_action_shadow_attempts',
    'serve_resource_action_worker_cohorts',
    'serve_resource_action_worker_cohort_refs',
    'serve_resource_action_shadow_coverage',
    'serve_resource_action_shadow_coverage_attempts',
)
_AUTHORITY_RELEASE_TABLES = (
    'serve_resource_action_authority_releases',
    'serve_resource_action_authority_release_cohorts',
)
_SERVICE_CHECKS = {
    'ck_services_resource_action_mode',
    'ck_services_resource_action_mode_timestamp',
}
_REPLICA_CHECKS = {
    'ck_replicas_resource_action_identity',
    'ck_replicas_resource_action_links',
    'ck_replicas_resource_action_launch_exclusive',
    'ck_replicas_resource_action_down_exclusive',
    'ck_replicas_resource_action_shadow_links',
}
_REPLICA_INDEXES = {
    'uq_replicas_ra_replica_incarnation': 'replica_incarnation',
    'uq_replicas_ra_sky_cluster_record_uuid': 'sky_cluster_record_uuid',
    'uq_replicas_ra_launch_action_id': 'launch_action_id',
    'uq_replicas_ra_down_action_id': 'down_action_id',
    'uq_replicas_ra_launch_shadow_sample': 'launch_shadow_sample_id',
    'uq_replicas_ra_down_shadow_sample': 'down_shadow_sample_id',
    'uq_replicas_ra_launch_shadow_coverage': 'launch_shadow_coverage_id',
    'uq_replicas_ra_down_shadow_coverage': 'down_shadow_coverage_id',
}


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
        postgres_url = container.get_connection_url()
    else:
        temporary_database = f'skypilot_serve_033_{uuid.uuid4().hex}'
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


def _reset_to_revision_031(engine: sqlalchemy.engine.Engine) -> None:
    if engine.dialect.name == 'postgresql':
        with engine.begin() as connection:
            connection.exec_driver_sql('DROP SCHEMA public CASCADE')
            connection.exec_driver_sql('CREATE SCHEMA public')
    metadata = sqlalchemy.MetaData()
    services = sqlalchemy.Table(
        'services', metadata,
        sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('status', sqlalchemy.Text, nullable=False))
    replicas = sqlalchemy.Table(
        'replicas', metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('status', sqlalchemy.Text, nullable=False))
    version = sqlalchemy.Table(
        _VERSION_TABLE, metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True))

    raw_activity = None
    daily_activity = None
    if engine.dialect.name == 'postgresql':
        raw_activity = sqlalchemy.Table(
            _RAW_ACTIVITY, metadata,
            sqlalchemy.Column('service_name', sqlalchemy.Text,
                              primary_key=True),
            sqlalchemy.Column('service_hash', sqlalchemy.Text,
                              primary_key=True),
            sqlalchemy.Column('reporter_session_id',
                              sqlalchemy.Text,
                              primary_key=True),
            sqlalchemy.Column('bucket_start',
                              sqlalchemy.DateTime(timezone=True),
                              primary_key=True),
            sqlalchemy.Column('observed_at',
                              sqlalchemy.DateTime(timezone=True),
                              nullable=False),
            sqlalchemy.Column('request_count',
                              sqlalchemy.Integer,
                              nullable=False),
            sqlalchemy.Column('rejected_count',
                              sqlalchemy.Integer,
                              nullable=False,
                              server_default='0'),
            sqlalchemy.Column('rejection_count_available',
                              sqlalchemy.Boolean,
                              nullable=False,
                              server_default=sqlalchemy.false()),
            sqlalchemy.CheckConstraint(
                'request_count >= 0',
                name='serve_request_activity_history_nonnegative'),
            sqlalchemy.CheckConstraint(
                'rejected_count >= 0',
                name='serve_request_activity_history_rejected_nonnegative'))
        daily_activity = sqlalchemy.Table(
            _DAILY_ACTIVITY, metadata,
            sqlalchemy.Column('day_start',
                              sqlalchemy.DateTime(timezone=True),
                              primary_key=True),
            sqlalchemy.Column('service_name', sqlalchemy.Text,
                              primary_key=True),
            sqlalchemy.Column('service_hash', sqlalchemy.Text,
                              primary_key=True),
            sqlalchemy.Column('first_bucket_start',
                              sqlalchemy.DateTime(timezone=True),
                              nullable=False),
            sqlalchemy.Column('last_bucket_start',
                              sqlalchemy.DateTime(timezone=True),
                              nullable=False),
            sqlalchemy.Column('request_count',
                              sqlalchemy.BigInteger,
                              nullable=False),
            sqlalchemy.Column('observed_at',
                              sqlalchemy.DateTime(timezone=True),
                              nullable=False),
            sqlalchemy.CheckConstraint(
                'request_count >= 0',
                name='serve_request_activity_daily_nonnegative'))

    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(services.insert().values(name='svc', status='READY'))
        connection.execute(replicas.insert().values(service_name='svc',
                                                    replica_id=7,
                                                    status='PENDING'))
        if raw_activity is not None:
            connection.execute(raw_activity.insert().values(
                service_name='svc',
                service_hash=str(_SERVICE_UUID),
                reporter_session_id='reporter-v1',
                bucket_start=_BUCKET,
                observed_at=_BUCKET + datetime.timedelta(seconds=30),
                request_count=9,
                rejected_count=2,
                rejection_count_available=True))
        if daily_activity is not None:
            connection.execute(daily_activity.insert().values(
                day_start=_DAY,
                service_name='svc',
                service_hash=str(_SERVICE_UUID),
                first_bucket_start=_BUCKET,
                last_bucket_start=_BUCKET,
                request_count=9,
                observed_at=_BUCKET + datetime.timedelta(minutes=1)))
        connection.execute(version.insert().values(version_num='031'))


def _upgrade(engine: sqlalchemy.engine.Engine, revision: str) -> None:
    migration_utils.safe_alembic_upgrade(engine, migration_utils.SERVE_DB_NAME,
                                         revision)


def _revision(engine: sqlalchemy.engine.Engine) -> str:
    revision = migration_utils.get_current_alembic_revision(
        engine, migration_utils.SERVE_DB_NAME)
    assert revision is not None
    return revision


def _stamp(engine: sqlalchemy.engine.Engine, revision: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(f'UPDATE {_VERSION_TABLE} '
                            'SET version_num = :revision'),
            {'revision': revision})


def _column_map(inspector: sqlalchemy.Inspector,
                table: str) -> dict[str, dict[str, object]]:
    return {
        str(column['name']): column for column in inspector.get_columns(table)
    }


def _check_names(inspector: sqlalchemy.Inspector, table: str) -> set[str]:
    return {
        str(constraint['name'])
        for constraint in inspector.get_check_constraints(table)
        if constraint['name'] is not None
    }


def _foreign_keys(
    inspector: sqlalchemy.Inspector, table: str
) -> dict[str, tuple[str, tuple[str, ...], tuple[str, ...], str | None]]:
    return {
        str(foreign_key['name']): (
            str(foreign_key['referred_table']),
            tuple(foreign_key['constrained_columns']),
            tuple(foreign_key['referred_columns']),
            foreign_key['options'].get('ondelete')
        ) for foreign_key in inspector.get_foreign_keys(table)
    }


def _normalized(value: object) -> str:
    return (''.join(str(value).lower().split()).replace('(', '').replace(
        ')', '').replace('::text', ''))


def _catalog_signature(engine: sqlalchemy.engine.Engine) -> tuple:
    inspector = sqlalchemy.inspect(engine)
    tables = []
    for table in sorted(inspector.get_table_names()):
        columns = tuple((str(column['name']), str(column['type']),
                         bool(column['nullable']), str(column['default']))
                        for column in inspector.get_columns(table))
        checks = tuple(
            sorted((str(constraint['name']), _normalized(constraint['sqltext']))
                   for constraint in inspector.get_check_constraints(table)))
        foreign_keys = tuple(
            sorted(
                (str(foreign_key['name']), str(foreign_key['referred_table']),
                 tuple(foreign_key['constrained_columns']),
                 tuple(foreign_key['referred_columns']),
                 foreign_key['options'].get('ondelete'))
                for foreign_key in inspector.get_foreign_keys(table)))
        indexes = tuple(
            sorted((str(index['name']), bool(index['unique']),
                    tuple(index['column_names']),
                    _normalized((index.get('dialect_options') or {}
                                ).get('postgresql_where')))
                   for index in inspector.get_indexes(table)))
        tables.append((table, columns, checks, foreign_keys, indexes))
    return tuple(tables)


def _ordinary_rows(engine: sqlalchemy.engine.Engine) -> tuple:
    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.text('SELECT name, status FROM services')).one()
        replica = connection.execute(
            sqlalchemy.text('SELECT service_name, replica_id, status '
                            'FROM replicas')).one()
    return tuple(service), tuple(replica)


def _install_legacy_reserved_fill_tables(
    engine: sqlalchemy.engine.Engine,
) -> tuple[sqlalchemy.Table, sqlalchemy.Table]:
    """Install the populated protocol-v1 tables present at revision 034."""
    metadata = sqlalchemy.MetaData()
    claims = sqlalchemy.Table(
        'reserved_fill_claims', metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('pool_key', sqlalchemy.Text),
        sqlalchemy.Column('weight', sqlalchemy.Float),
        sqlalchemy.Column('floor_replicas', sqlalchemy.Integer),
        sqlalchemy.Column('gpus_per_replica', sqlalchemy.Integer),
        sqlalchemy.Column('holdings_fill', sqlalchemy.Integer),
        sqlalchemy.Column('effective_cap', sqlalchemy.Integer),
        sqlalchemy.Column('launchable', sqlalchemy.Integer),
        sqlalchemy.Column('demonstrated_need', sqlalchemy.Integer),
        sqlalchemy.Column('boot_hold', sqlalchemy.Integer),
        sqlalchemy.Column('activity_ts', sqlalchemy.Float),
        sqlalchemy.Column('heartbeat_ts', sqlalchemy.Float))
    rounds = sqlalchemy.Table(
        'reserved_fill_rounds', metadata,
        sqlalchemy.Column('pool_key', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('round_id', sqlalchemy.Integer),
        sqlalchemy.Column('epoch', sqlalchemy.Integer))
    metadata.create_all(engine)
    return claims, rounds


def _classification_rows(engine: sqlalchemy.engine.Engine) -> tuple:
    with engine.connect() as connection:
        raw = connection.execute(
            sqlalchemy.text(
                'SELECT service_name, service_hash, reporter_session_id, '
                'bucket_start, observed_at, request_count, rejected_count, '
                'rejection_count_available, classified_request_count, '
                f'counted_rejected_count FROM {_RAW_ACTIVITY}')).one()
        daily = connection.execute(
            sqlalchemy.text(
                'SELECT day_start, service_name, service_hash, '
                'first_bucket_start, last_bucket_start, request_count, '
                'classified_request_count, counted_rejected_count, '
                'classified_first_bucket_start, '
                'classified_last_bucket_start, classification_incomplete, '
                f'observed_at FROM {_DAILY_ACTIVITY}')).one()
    return tuple(raw), tuple(daily)


def _assert_classification_catalog(engine: sqlalchemy.engine.Engine) -> None:
    inspector = sqlalchemy.inspect(engine)
    raw = _column_map(inspector, _RAW_ACTIVITY)
    daily = _column_map(inspector, _DAILY_ACTIVITY)
    assert set(raw) >= {
        'classified_request_count',
        'counted_rejected_count',
    }
    assert isinstance(raw['classified_request_count']['type'],
                      sqlalchemy.Integer)
    assert isinstance(raw['counted_rejected_count']['type'], sqlalchemy.Integer)
    assert raw['classified_request_count']['nullable']
    assert raw['counted_rejected_count']['nullable']
    assert raw['classified_request_count']['default'] is None
    assert raw['counted_rejected_count']['default'] is None
    assert _RAW_PAIR_CONSTRAINT in _check_names(inspector, _RAW_ACTIVITY)

    assert set(daily) >= {
        'classified_request_count',
        'counted_rejected_count',
        'classified_first_bucket_start',
        'classified_last_bucket_start',
        'classification_incomplete',
    }
    assert isinstance(daily['classified_request_count']['type'],
                      sqlalchemy.BigInteger)
    assert isinstance(daily['counted_rejected_count']['type'],
                      sqlalchemy.BigInteger)
    for name in ('classified_first_bucket_start',
                 'classified_last_bucket_start'):
        assert isinstance(daily[name]['type'], sqlalchemy.DateTime)
        assert daily[name]['type'].timezone
        assert daily[name]['nullable']
        assert daily[name]['default'] is None
    assert isinstance(daily['classification_incomplete']['type'],
                      sqlalchemy.Boolean)
    assert not daily['classification_incomplete']['nullable']
    assert _normalized(daily['classification_incomplete']['default']).replace(
        '::boolean', '') == 'false'
    assert _DAILY_PAIR_CONSTRAINT in _check_names(inspector, _DAILY_ACTIVITY)


def _set_exact_classification_rows(engine: sqlalchemy.engine.Engine) -> None:
    last_bucket = _BUCKET + datetime.timedelta(minutes=1)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(f'UPDATE {_RAW_ACTIVITY} SET '
                            'classified_request_count = 8, '
                            'counted_rejected_count = 2'))
        connection.execute(
            sqlalchemy.text(f'UPDATE {_DAILY_ACTIVITY} SET '
                            'classified_request_count = 8, '
                            'counted_rejected_count = 2, '
                            'classified_first_bucket_start = :first_bucket, '
                            'classified_last_bucket_start = :last_bucket, '
                            'classification_incomplete = false'), {
                                'first_bucket': _BUCKET,
                                'last_bucket': last_bucket,
                            })


def _assert_head_table_catalog(inspector: sqlalchemy.Inspector,
                               expected: sqlalchemy.Table) -> None:
    table = expected.name
    actual_columns = _column_map(inspector, table)
    assert tuple(actual_columns) == tuple(expected.c.keys())
    dialect = inspector.bind.dialect
    for expected_column in expected.columns:
        actual = actual_columns[expected_column.name]
        assert (str(actual['type'].compile(dialect=dialect)).upper() == str(
            expected_column.type.compile(dialect=dialect)).upper())
        assert bool(actual['nullable']) == bool(expected_column.nullable)
        assert ((actual['default'] is None) == (expected_column.server_default
                                                is None))

    expected_primary_key = next(
        constraint for constraint in expected.constraints
        if isinstance(constraint, sqlalchemy.PrimaryKeyConstraint))
    actual_primary_key = inspector.get_pk_constraint(table)
    assert actual_primary_key['name'] == expected_primary_key.name
    assert tuple(actual_primary_key['constrained_columns']) == tuple(
        column.name for column in expected_primary_key.columns)

    expected_checks = {
        str(constraint.name)
        for constraint in expected.constraints
        if isinstance(constraint, sqlalchemy.CheckConstraint)
    }
    assert _check_names(inspector, table) == expected_checks

    expected_uniques = {
        str(constraint.name):
            tuple(column.name for column in constraint.columns)
        for constraint in expected.constraints
        if isinstance(constraint, sqlalchemy.UniqueConstraint)
    }
    actual_uniques = {
        str(constraint['name']): tuple(constraint['column_names'])
        for constraint in inspector.get_unique_constraints(table)
    }
    assert actual_uniques == expected_uniques

    expected_foreign_keys = {
        str(constraint.name):
            (next(iter(constraint.elements)).column.table.name,
             tuple(element.parent.name
                   for element in constraint.elements),
             tuple(element.column.name
                   for element in constraint.elements), constraint.ondelete)
        for constraint in expected.constraints
        if isinstance(constraint, sqlalchemy.ForeignKeyConstraint)
    }
    assert _foreign_keys(inspector, table) == expected_foreign_keys

    actual_indexes = {
        str(index['name']): index for index in inspector.get_indexes(table)
    }
    for expected_index in expected.indexes:
        actual = actual_indexes[str(expected_index.name)]
        assert bool(actual['unique']) == bool(expected_index.unique)
        assert tuple(actual['column_names']) == tuple(
            column.name for column in expected_index.columns)
        expected_where = expected_index.dialect_options['postgresql']['where']
        actual_where = (actual.get('dialect_options') or
                        {}).get('postgresql_where')
        if expected_where is None:
            assert actual_where is None
        else:
            # PostgreSQL reflects ``IN (...)`` as ``= ANY (ARRAY[...])`` and
            # injects text casts. The exact named index, key columns, unique
            # bit, and existence of its partial predicate are stable.
            assert actual_where is not None


def _assert_final_postgres_catalog(engine: sqlalchemy.engine.Engine) -> None:
    inspector = sqlalchemy.inspect(engine)
    assert {
        table for table in inspector.get_table_names()
        if table.startswith('serve_resource_action_')
    } == set(_EVIDENCE_TABLES)
    assert set(_column_map(
        inspector, 'services')) == {'name', 'status', *_SERVICE_ACTION_COLUMNS}
    assert set(_column_map(inspector, 'replicas')) == {
        'service_name', 'replica_id', 'status', *_REPLICA_ACTION_COLUMNS
    }
    assert _check_names(inspector, 'services') == _SERVICE_CHECKS
    assert _check_names(inspector, 'replicas') == _REPLICA_CHECKS

    for table, expected_columns in (
        ('services', action_schema.service_columns()),
        ('replicas', action_schema.replica_columns() +
         action_schema.replica_coverage_columns()),
    ):
        actual_columns = _column_map(inspector, table)
        for expected in expected_columns:
            actual = actual_columns[expected.name]
            dialect = inspector.bind.dialect
            assert (str(actual['type'].compile(dialect=dialect)).upper() == str(
                expected.type.compile(dialect=dialect)).upper())
            assert bool(actual['nullable']) == bool(expected.nullable)
            assert ((actual['default'] is None) == (expected.server_default
                                                    is None))
    assert _normalized(
        _column_map(
            inspector,
            'services')['resource_action_mode']['default']) == "'legacy'"

    replica_indexes = {
        str(index['name']): index for index in inspector.get_indexes('replicas')
    }
    for name, column in _REPLICA_INDEXES.items():
        index = replica_indexes[name]
        assert index['unique']
        assert index['column_names'] == [column]
        assert _normalized((index.get('dialect_options') or
                            {}).get('postgresql_where')) == f'{column}isnotnull'

    for table in (
            action_schema.WORKER_COHORTS,
            action_schema.WORKER_COHORT_REFS,
            action_schema.SHADOW_COVERAGE,
            action_schema.SHADOW_SAMPLES,
            action_schema.SHADOW_ATTEMPTS,
            action_schema.SHADOW_COVERAGE_ATTEMPTS,
    ):
        _assert_head_table_catalog(inspector, table)

    capability = _column_map(
        inspector,
        action_schema.WORKER_COHORT_REFS.name)['preparation_capability_sha256']
    assert not capability['nullable']
    assert capability['default'] is None


def _install_old_feature_draft(engine: sqlalchemy.engine.Engine) -> None:
    """Install the empty, unshipped feature-032 catalog on PostgreSQL."""
    old_replica_columns = {
        'replica_incarnation': 'UUID',
        'desired_generation': 'BIGINT',
        'sky_cluster_record_uuid': 'UUID',
        'launch_action_id': 'UUID',
        'down_action_id': 'UUID',
        'launch_shadow_sample_id': 'UUID',
        'down_shadow_sample_id': 'UUID',
    }
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE services ADD COLUMN resource_action_mode TEXT "
            "NOT NULL DEFAULT 'legacy'")
        connection.exec_driver_sql(
            'ALTER TABLE services ADD COLUMN '
            'resource_action_mode_changed_at TIMESTAMPTZ')
        for name, sql_type in old_replica_columns.items():
            connection.exec_driver_sql(
                f'ALTER TABLE replicas ADD COLUMN {name} {sql_type}')
        connection.exec_driver_sql(
            "ALTER TABLE services ADD CONSTRAINT "
            "ck_services_resource_action_mode CHECK "
            "(resource_action_mode IN ('legacy', 'shadow', 'authoritative'))")
        connection.exec_driver_sql(
            'ALTER TABLE services ADD CONSTRAINT '
            'ck_services_resource_action_mode_timestamp CHECK '
            "(resource_action_mode = 'legacy' OR "
            'resource_action_mode_changed_at IS NOT NULL)')
        connection.exec_driver_sql(
            'ALTER TABLE replicas ADD CONSTRAINT '
            'ck_replicas_resource_action_identity CHECK '
            '((replica_incarnation IS NULL AND desired_generation IS NULL '
            'AND sky_cluster_record_uuid IS NULL) OR '
            '(replica_incarnation IS NOT NULL AND '
            'desired_generation IS NOT NULL AND desired_generation > 0 '
            'AND sky_cluster_record_uuid IS NOT NULL))')
        connection.exec_driver_sql(
            'ALTER TABLE replicas ADD CONSTRAINT '
            'ck_replicas_resource_action_links CHECK '
            '(replica_incarnation IS NOT NULL OR '
            '(launch_action_id IS NULL AND down_action_id IS NULL AND '
            'launch_shadow_sample_id IS NULL AND '
            'down_shadow_sample_id IS NULL))')
        connection.exec_driver_sql(
            'ALTER TABLE replicas ADD CONSTRAINT '
            'ck_replicas_resource_action_launch_exclusive CHECK '
            '(launch_action_id IS NULL OR launch_shadow_sample_id IS NULL)')
        connection.exec_driver_sql(
            'ALTER TABLE replicas ADD CONSTRAINT '
            'ck_replicas_resource_action_down_exclusive CHECK '
            '(down_action_id IS NULL OR down_shadow_sample_id IS NULL)')
        for name, column in tuple(_REPLICA_INDEXES.items())[:6]:
            connection.exec_driver_sql(
                f'CREATE UNIQUE INDEX {name} ON replicas ({column}) '
                f'WHERE {column} IS NOT NULL')
        action_schema.STAGED_SERVE033_METADATA.create_all(connection,
                                                          checkfirst=True)


def test_serve_alembic_lineage_has_033_through_action_history_039() -> None:
    engine = sqlalchemy.create_engine('sqlite://')
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)
    scripts = alembic_script.ScriptDirectory.from_config(config)
    revisions = list(scripts.walk_revisions())
    revision_ids = [revision.revision for revision in revisions]
    assert len(revision_ids) == len(set(revision_ids))
    assert scripts.get_heads() == ['057']
    revision_032 = scripts.get_revision('032')
    revision_033 = scripts.get_revision('033')
    revision_034 = scripts.get_revision('034')
    revision_035 = scripts.get_revision('035')
    revision_036 = scripts.get_revision('036')
    revision_037 = scripts.get_revision('037')
    revision_038 = scripts.get_revision('038')
    revision_039 = scripts.get_revision('039')
    assert Path(revision_032.path).name == (
        '032_serve_request_rejection_classification.py')
    assert revision_032.down_revision == '031'
    assert Path(
        revision_033.path).name == ('033_serve_resource_action_coverage.py')
    assert revision_033.down_revision == '032'
    assert Path(revision_034.path).name == ('034_authority_release_ledger.py')
    assert revision_034.down_revision == '033'
    assert Path(revision_035.path).name == ('035_multi_pool_reserved_fill.py')
    assert revision_035.down_revision == '034'
    assert Path(revision_036.path).name == ('036_version_controller_config.py')
    assert revision_036.down_revision == '035'
    assert Path(
        revision_037.path).name == ('037_placement_normalization_ledger.py')
    assert revision_037.down_revision == '036'
    assert Path(
        revision_038.path).name == ('038_serve_resource_action_authority.py')
    assert revision_038.down_revision == '037'
    assert Path(revision_039.path).name == (
        '039_serve_resource_action_execution_history.py')
    assert revision_039.down_revision == '038'
    assert migration_utils.SERVE_VERSION == '057'
    assert migration_utils.SERVE_NON_POSTGRES_VERSION == '037'


def test_staged_and_head_schema_aliases_are_disjoint_and_complete() -> None:
    assert set(action_schema.STAGED_SERVE033_METADATA.tables) == {
        'serve_resource_action_shadow_samples',
        'serve_resource_action_shadow_attempts',
    }
    assert (action_schema.STAGED_SHADOW_SAMPLES
            is action_schema.shadow_samples_table)
    assert (action_schema.STAGED_SHADOW_ATTEMPTS
            is action_schema.shadow_attempts_table)
    assert set(action_schema.RESOURCE_ACTION_STATE_METADATA.tables) == set(
        _EVIDENCE_TABLES)
    assert action_schema.SHADOW_SAMPLES is not action_schema.STAGED_SHADOW_SAMPLES
    assert action_schema.SHADOW_ATTEMPTS is not action_schema.STAGED_SHADOW_ATTEMPTS
    assert 'legacy_effect_trace' not in action_schema.STAGED_SHADOW_ATTEMPTS.c
    assert {'legacy_effect_trace', 'legacy_effect_trace_sha256'
           } <= set(action_schema.SHADOW_ATTEMPTS.c.keys())
    foreign_keys = {
        constraint.name: constraint
        for constraint in action_schema.SHADOW_SAMPLES.foreign_key_constraints
    }
    coverage = foreign_keys['fk_serve_ra_shadow_samples_coverage']
    assert coverage.ondelete == 'RESTRICT'
    assert next(iter(coverage.elements)).target_fullname == (
        'serve_resource_action_shadow_coverage.decision_id')


def test_sqlite_031_to_032_to_033_adds_only_portable_columns(tmp_path) -> None:
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "serve-033.sqlite"}')
    _reset_to_revision_031(engine)
    ordinary_rows = _ordinary_rows(engine)

    _upgrade(engine, '032')
    inspector = sqlalchemy.inspect(engine)
    assert _revision(engine) == '032'
    assert set(_column_map(inspector, 'services')) == {'name', 'status'}
    assert set(_column_map(
        inspector, 'replicas')) == {'service_name', 'replica_id', 'status'}
    assert not set(_EVIDENCE_TABLES).intersection(inspector.get_table_names())
    assert _ordinary_rows(engine) == ordinary_rows

    _upgrade(engine, '033')
    inspector = sqlalchemy.inspect(engine)
    assert _revision(engine) == '033'
    assert set(_column_map(
        inspector, 'services')) == {'name', 'status', *_SERVICE_ACTION_COLUMNS}
    assert set(_column_map(inspector, 'replicas')) == {
        'service_name', 'replica_id', 'status', *_REPLICA_ACTION_COLUMNS
    }
    assert not set(_EVIDENCE_TABLES).intersection(inspector.get_table_names())
    assert _ordinary_rows(engine) == ordinary_rows
    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.text('SELECT resource_action_mode, '
                            'resource_action_mode_changed_at FROM services')
        ).mappings().one()
        replica = connection.execute(
            sqlalchemy.text('SELECT ' + ', '.join(_REPLICA_ACTION_COLUMNS) +
                            ' FROM replicas')).mappings().one()
    assert service == {
        'resource_action_mode': 'legacy',
        'resource_action_mode_changed_at': None,
    }
    assert set(replica.values()) == {None}

    _upgrade(engine, '034')
    assert _revision(engine) == '034'
    assert not set(_AUTHORITY_RELEASE_TABLES).intersection(
        sqlalchemy.inspect(engine).get_table_names())
    assert _ordinary_rows(engine) == ordinary_rows


def test_postgres_fresh_031_through_upstream_032_and_033(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    ordinary_rows = _ordinary_rows(engine)

    _upgrade(engine, '033')

    assert _revision(engine) == '033'
    _assert_classification_catalog(engine)
    raw, daily = _classification_rows(engine)
    assert raw[5:10] == (9, 2, True, None, None)
    assert daily[5:11] == (9, None, None, None, None, True)
    assert _ordinary_rows(engine) == ordinary_rows
    _assert_final_postgres_catalog(engine)


def test_postgres_revision_034_adds_only_exact_release_ledger(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    ordinary_rows = _ordinary_rows(engine)

    _upgrade(engine, '034')

    assert _revision(engine) == '034'
    inspector = sqlalchemy.inspect(engine)
    assert set(_AUTHORITY_RELEASE_TABLES) <= set(inspector.get_table_names())
    assert tuple(
        inspector.get_pk_constraint(
            action_schema.AUTHORITY_RELEASES.name)['constrained_columns']) == (
                'namespace', 'helm_release_name')
    assert tuple(
        inspector.get_pk_constraint(
            action_schema.AUTHORITY_RELEASE_COHORTS.name)
        ['constrained_columns']) == ('namespace', 'helm_release_name',
                                     'cohort_suffix')
    assert _ordinary_rows(engine) == ordinary_rows


def test_postgres_revision_035_copies_legacy_claim_as_inert_shadow(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '034')
    claims, rounds = _install_legacy_reserved_fill_tables(engine)
    with engine.begin() as connection:
        connection.execute(claims.insert().values(service_name='svc',
                                                  pool_key='["ctx","h200"]',
                                                  weight=2.0,
                                                  floor_replicas=3,
                                                  gpus_per_replica=8,
                                                  holdings_fill=1,
                                                  effective_cap=7,
                                                  launchable=1,
                                                  demonstrated_need=2,
                                                  boot_hold=0,
                                                  activity_ts=99.0,
                                                  heartbeat_ts=100.0))
        connection.execute(rounds.insert().values(pool_key='["ctx","h200"]',
                                                  round_id=4,
                                                  epoch=5))

    _upgrade(engine, '035')

    assert _revision(engine) == '035'
    inspector = sqlalchemy.inspect(engine)
    assert {
        serve_state_schema.reserved_fill_protocol_state_table.name,
        serve_state_schema.reserved_fill_service_claim_sets_table.name,
        serve_state_schema.reserved_fill_pool_claims_table.name,
    } <= set(inspector.get_table_names())
    round_columns = {
        column['name']: column
        for column in inspector.get_columns('reserved_fill_rounds')
    }
    assert round_columns['protocol_version']['nullable'] is False
    assert round_columns['claim_generations']['nullable'] is False
    protocol_columns = {
        column['name']: column
        for column in inspector.get_columns('reserved_fill_protocol_state')
    }
    assert protocol_columns['claim_generation']['nullable'] is False
    with engine.connect() as connection:
        protocol = connection.execute(
            sqlalchemy.text('SELECT protocol_version, claim_generation FROM '
                            'reserved_fill_protocol_state WHERE id = 1')).one()
        claim_set = connection.execute(
            sqlalchemy.text('SELECT claim_set_state, generation, edge_count, '
                            'global_headroom FROM '
                            'reserved_fill_service_claim_sets WHERE '
                            "service_name = 'svc'")).one()
        edge = connection.execute(
            sqlalchemy.text('SELECT pool_key, legacy_pool_key, '
                            'service_generation, physical_cluster_uid, '
                            'demonstrated_need FROM reserved_fill_pool_claims '
                            "WHERE service_name = 'svc'")).one()
        migrated_round = connection.execute(
            sqlalchemy.text('SELECT protocol_version, claim_generations FROM '
                            'reserved_fill_rounds WHERE pool_key = '
                            "'[\"ctx\",\"h200\"]'")).one()
        retained_legacy = connection.execute(
            sqlalchemy.text('SELECT pool_key, weight, floor_replicas, '
                            'gpus_per_replica, holdings_fill, effective_cap, '
                            'launchable, demonstrated_need, boot_hold, '
                            'activity_ts, heartbeat_ts FROM '
                            'reserved_fill_claims WHERE '
                            "service_name = 'svc'")).one()
    assert protocol == (1, 0)
    assert claim_set == ('migration_shadow', 0, 1, 7)
    assert edge == ('["ctx","h200"]', '["ctx","h200"]', 0, None, 2)
    assert migrated_round == (1, '{}')
    assert retained_legacy == ('["ctx","h200"]', 2.0, 3, 8, 1, 7, 1, 2, 0, 99.0,
                               100.0)


def test_postgres_revision_036_adds_nullable_version_config_and_applied_receipt(
        postgres_engine) -> None:
    engine = postgres_engine
    with engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    metadata = sqlalchemy.MetaData()
    versions = sqlalchemy.Table(
        'version_specs',
        metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
        sqlalchemy.Column('yaml_content', sqlalchemy.Text),
    )
    alembic_version = sqlalchemy.Table(
        _VERSION_TABLE,
        metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True),
    )
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(versions.insert().values(
            service_name='svc',
            version=7,
            spec=b'opaque-spec',
            yaml_content='service: preserved'))
        connection.execute(alembic_version.insert().values(version_num='035'))

    _upgrade(engine, '036')

    assert _revision(engine) == '036'
    columns = _column_map(sqlalchemy.inspect(engine), 'version_specs')
    assert columns['controller_config']['nullable'] is True
    assert columns['controller_config_digest']['nullable'] is True
    assert columns['controller_config_snapshot_id']['nullable'] is True
    assert columns['controller_applied_at']['nullable'] is True
    assert isinstance(columns['controller_config']['type'],
                      sqlalchemy.LargeBinary)
    assert isinstance(columns['controller_config_digest']['type'],
                      sqlalchemy.Text)
    assert isinstance(columns['controller_config_snapshot_id']['type'],
                      sqlalchemy.Text)
    assert isinstance(columns['controller_applied_at']['type'],
                      sqlalchemy.Float)
    with engine.connect() as connection:
        row = connection.execute(
            sqlalchemy.text(
                'SELECT service_name, version, spec, yaml_content, '
                'controller_config, controller_config_digest, '
                'controller_config_snapshot_id, controller_applied_at '
                'FROM version_specs')).one()
    assert row.service_name == 'svc'
    assert row.version == 7
    assert bytes(row.spec) == b'opaque-spec'
    assert row.yaml_content == 'service: preserved'
    assert row.controller_config is None
    assert row.controller_config_digest is None
    assert row.controller_config_snapshot_id is None
    assert row.controller_applied_at is None


def test_postgres_revision_037_adds_normalization_ledger_without_rewriting_rows(
        postgres_engine) -> None:
    engine = postgres_engine
    with engine.begin() as connection:
        connection.exec_driver_sql('DROP SCHEMA public CASCADE')
        connection.exec_driver_sql('CREATE SCHEMA public')
    metadata = sqlalchemy.MetaData()
    services = sqlalchemy.Table(
        'services',
        metadata,
        sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('status', sqlalchemy.Text),
    )
    versions = sqlalchemy.Table(
        'version_specs',
        metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
        sqlalchemy.Column('yaml_content', sqlalchemy.Text),
    )
    alembic_version = sqlalchemy.Table(
        _VERSION_TABLE,
        metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True),
    )
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(services.insert().values(name='svc', status='READY'))
        connection.execute(versions.insert().values(
            service_name='svc',
            version=7,
            spec=b'opaque-spec',
            yaml_content='service: preserved'))
        connection.execute(alembic_version.insert().values(version_num='036'))

    _upgrade(engine, '037')

    assert _revision(engine) == '037'
    inspector = sqlalchemy.inspect(engine)
    assert {
        'placement_normalization_runs',
        'placement_normalization_rows',
    } <= set(inspector.get_table_names())
    service_columns = _column_map(inspector, 'services')
    assert {
        'placement_normalization_requested_run_id',
        'placement_normalization_loaded_run_id',
        'placement_normalization_loaded_image_commit',
        'placement_normalization_loaded_controller_pid',
        'placement_normalization_loaded_controller_ip',
        'placement_normalization_loaded_boot_id',
        'placement_normalization_loaded_at',
    } <= set(service_columns)
    version_columns = _column_map(inspector, 'version_specs')
    assert {
        'retired_yaml_content',
        'retired_at',
        'retirement_reason',
        'retirement_run_id',
    } <= set(version_columns)
    assert all(service_columns[name]['nullable'] for name in (
        'placement_normalization_requested_run_id',
        'placement_normalization_loaded_run_id',
        'placement_normalization_loaded_image_commit',
        'placement_normalization_loaded_controller_pid',
        'placement_normalization_loaded_controller_ip',
        'placement_normalization_loaded_boot_id',
        'placement_normalization_loaded_at',
    ))
    assert all(version_columns[name]['nullable'] for name in (
        'retired_yaml_content',
        'retired_at',
        'retirement_reason',
        'retirement_run_id',
    ))
    assert isinstance(
        service_columns['placement_normalization_requested_run_id']['type'],
        sqlalchemy.Uuid)
    assert isinstance(version_columns['retirement_run_id']['type'],
                      sqlalchemy.Uuid)
    assert 'ck_version_specs_retirement_all_or_none' in _check_names(
        inspector, 'version_specs')
    assert _foreign_keys(inspector, 'services') == {
        'fk_services_placement_normalization_loaded_run':
            ('placement_normalization_runs',
             ('placement_normalization_loaded_run_id',), ('run_id',), 'RESTRICT'
            ),
        'fk_services_placement_normalization_requested_run':
            ('placement_normalization_runs',
             ('placement_normalization_requested_run_id',), ('run_id',),
             'RESTRICT'),
    }
    assert _foreign_keys(inspector, 'version_specs') == {
        'fk_version_specs_retirement_run':
            ('placement_normalization_runs', ('retirement_run_id',),
             ('run_id',), 'RESTRICT')
    }
    assert tuple(
        inspector.get_pk_constraint('placement_normalization_runs')
        ['constrained_columns']) == ('run_id',)
    assert tuple(
        inspector.get_pk_constraint('placement_normalization_rows')
        ['constrained_columns']) == ('run_id', 'service_name', 'version')
    assert _foreign_keys(inspector, 'placement_normalization_rows') == {
        'fk_placement_normalization_rows_run':
            ('placement_normalization_runs', ('run_id',), ('run_id',),
             'RESTRICT')
    }
    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.text('SELECT * FROM services')).mappings().one()
        version = connection.execute(
            sqlalchemy.text('SELECT * FROM version_specs')).mappings().one()
    assert service['name'] == 'svc'
    assert service['status'] == 'READY'
    assert all(service[name] is None for name in (
        'placement_normalization_requested_run_id',
        'placement_normalization_loaded_run_id',
        'placement_normalization_loaded_image_commit',
        'placement_normalization_loaded_controller_pid',
        'placement_normalization_loaded_controller_ip',
        'placement_normalization_loaded_boot_id',
        'placement_normalization_loaded_at',
    ))
    assert version['service_name'] == 'svc'
    assert version['version'] == 7
    assert bytes(version['spec']) == b'opaque-spec'
    assert version['yaml_content'] == 'service: preserved'
    assert all(version[name] is None for name in (
        'retired_yaml_content',
        'retired_at',
        'retirement_reason',
        'retirement_run_id',
    ))

    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                sqlalchemy.text('UPDATE version_specs SET retired_at = 1 '
                                "WHERE service_name = 'svc' AND version = 7"))

    retirement_run_id = uuid.uuid4()
    with engine.begin() as connection:
        connection.execute(serve_state_schema.
                           placement_normalization_runs_table.insert().values(
                               run_id=retirement_run_id,
                               mode='retire_terminal_historical',
                               normalizer_version='1',
                               schema_revision='037',
                               release_version='test-release',
                               started_at=0.5,
                               completed_at=1.0,
                               row_bound=1,
                               row_count=1,
                               classification_counts={'historical': 1},
                               pre_inventory_sha256='a' * 64,
                               post_inventory_sha256='b' * 64,
                               freeze_evidence_sha256='c' * 64))
        connection.execute(
            sqlalchemy.text(
                'UPDATE version_specs SET yaml_content = NULL, '
                'retired_yaml_content = :yaml, retired_at = :retired_at, '
                'retirement_reason = :reason, '
                'retirement_run_id = :run_id '
                "WHERE service_name = 'svc' AND version = 7"), {
                    'yaml': 'service: preserved',
                    'retired_at': 1.0,
                    'reason': 'terminal historical contract',
                    'run_id': retirement_run_id,
                })
    with engine.connect() as connection:
        retired = connection.execute(
            sqlalchemy.text(
                'SELECT yaml_content, retired_yaml_content, retired_at, '
                'retirement_reason, retirement_run_id FROM version_specs '
                "WHERE service_name = 'svc' AND version = 7")).one()
    assert retired.yaml_content is None
    assert retired.retired_yaml_content == 'service: preserved'
    assert retired.retired_at == 1.0
    assert retired.retirement_reason == 'terminal historical contract'
    assert retired.retirement_run_id == retirement_run_id


def test_controller_local_sqlite_revision_037_adds_only_inert_state(
        tmp_path) -> None:
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "serve-037.sqlite"}')
    metadata = sqlalchemy.MetaData()
    services = sqlalchemy.Table(
        'services',
        metadata,
        sqlalchemy.Column('name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('status', sqlalchemy.Text),
    )
    versions = sqlalchemy.Table(
        'version_specs',
        metadata,
        sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('version', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('spec', sqlalchemy.LargeBinary),
        sqlalchemy.Column('yaml_content', sqlalchemy.Text),
    )
    alembic_version = sqlalchemy.Table(
        _VERSION_TABLE,
        metadata,
        sqlalchemy.Column('version_num',
                          sqlalchemy.String(32),
                          primary_key=True),
    )
    metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(services.insert().values(name='svc', status='READY'))
        connection.execute(versions.insert().values(
            service_name='svc',
            version=7,
            spec=b'opaque-spec',
            yaml_content='service: preserved'))
        connection.execute(alembic_version.insert().values(version_num='036'))

    _upgrade(engine, '037')

    assert _revision(engine) == '037'
    inspector = sqlalchemy.inspect(engine)
    assert {
        'placement_normalization_runs',
        'placement_normalization_rows',
    } <= set(inspector.get_table_names())
    assert {
        'placement_normalization_requested_run_id',
        'placement_normalization_loaded_run_id',
        'placement_normalization_loaded_image_commit',
        'placement_normalization_loaded_controller_pid',
        'placement_normalization_loaded_controller_ip',
        'placement_normalization_loaded_boot_id',
        'placement_normalization_loaded_at',
    } <= set(_column_map(inspector, 'services'))
    assert {
        'retired_yaml_content',
        'retired_at',
        'retirement_reason',
        'retirement_run_id',
    } <= set(_column_map(inspector, 'version_specs'))
    with engine.connect() as connection:
        service = connection.execute(
            sqlalchemy.text('SELECT name, status FROM services')).one()
        version = connection.execute(
            sqlalchemy.text('SELECT service_name, version, spec, '
                            'yaml_content FROM version_specs')).one()
    assert tuple(service) == ('svc', 'READY')
    assert version.service_name == 'svc'
    assert version.version == 7
    assert bytes(version.spec) == b'opaque-spec'
    assert version.yaml_content == 'service: preserved'


def test_postgres_revision_035_serializes_concurrent_legacy_writer_and_reupgrade(
        postgres_engine) -> None:
    """A v1 heartbeat committed during upgrade is copied, never half-read."""
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '034')
    claims, _ = _install_legacy_reserved_fill_tables(engine)
    with engine.begin() as connection:
        connection.execute(claims.insert().values(service_name='svc',
                                                  pool_key='["ctx","h200"]',
                                                  weight=1.0,
                                                  floor_replicas=0,
                                                  gpus_per_replica=8,
                                                  holdings_fill=1,
                                                  effective_cap=4,
                                                  launchable=1,
                                                  heartbeat_ts=100.0))

    writer = engine.connect()
    transaction = writer.begin()
    try:
        # Revision 035's shadow copy must wait for a writer that already owns
        # the legacy table, then observe the writer's committed heartbeat.
        writer.exec_driver_sql(
            'LOCK TABLE reserved_fill_claims IN ACCESS EXCLUSIVE MODE')
        started = threading.Event()

        def upgrade() -> None:
            started.set()
            _upgrade(engine, '035')

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(upgrade)
            assert started.wait(timeout=5)
            time.sleep(0.1)
            assert not future.done()
            writer.execute(
                sqlalchemy.update(claims).where(
                    claims.c.service_name == 'svc').values(effective_cap=7,
                                                           heartbeat_ts=222.0))
            transaction.commit()
            future.result(timeout=30)
    finally:
        if transaction.is_active:
            transaction.rollback()
        writer.close()

    with engine.connect() as connection:
        migrated = connection.execute(
            sqlalchemy.text(
                'SELECT normalized.effective_cap, normalized.heartbeat_ts, '
                'claim_set.claim_set_state, claim_set.generation '
                'FROM reserved_fill_pool_claims AS normalized JOIN '
                'reserved_fill_service_claim_sets AS claim_set USING '
                '(service_name) WHERE normalized.service_name = :service'), {
                    'service': 'svc'
                }).one()
    assert migrated == (7, 222.0, 'migration_shadow', 0)

    # An old image can continue issuing its original-column heartbeat after
    # the additive migration.  A later new-image startup re-running Alembic at
    # head must preserve that legacy write, and must not silently refresh the
    # inert migration shadow into apparent v2 authority.
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(claims).where(
                claims.c.service_name == 'svc').values(
                    pool_key='["moved-ctx","h200"]',
                    effective_cap=9,
                    heartbeat_ts=333.0))
        connection.execute(claims.insert().values(service_name='late-v1',
                                                  pool_key='["late-ctx","l4"]',
                                                  weight=2.0,
                                                  floor_replicas=1,
                                                  gpus_per_replica=1,
                                                  holdings_fill=0,
                                                  effective_cap=2,
                                                  launchable=1,
                                                  heartbeat_ts=444.0))
    before = _catalog_signature(engine)
    _upgrade(engine, '035')
    assert _revision(engine) == '035'
    assert _catalog_signature(engine) == before
    with engine.connect() as connection:
        legacy_rows = connection.execute(
            sqlalchemy.text('SELECT service_name, pool_key, effective_cap, '
                            'heartbeat_ts FROM reserved_fill_claims ORDER BY '
                            'service_name')).all()
        shadow_rows = connection.execute(
            sqlalchemy.text('SELECT service_name, pool_key, effective_cap, '
                            'heartbeat_ts FROM reserved_fill_pool_claims '
                            'ORDER BY service_name')).all()
    assert [tuple(row) for row in legacy_rows
           ] == [('late-v1', '["late-ctx","l4"]', 2, 444.0),
                 ('svc', '["moved-ctx","h200"]', 9, 333.0)]
    assert [tuple(row) for row in shadow_rows] == [('svc', '["ctx","h200"]', 7,
                                                    222.0)]


def test_postgres_revision_034_rejects_same_name_hostile_check(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '033')
    action_schema.RESOURCE_ACTION_AUTHORITY_RELEASE_METADATA.create_all(engine)
    releases = action_schema.AUTHORITY_RELEASES.name
    check = 'ck_serve_ra_authority_releases_revision'
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {releases} DROP CONSTRAINT {check}')
        # Same table, columns, constraint name, and default presence as the
        # shipped catalog, but a weakened expression.  Revision 034 must not
        # adopt it by name alone.
        connection.exec_driver_sql(
            f'ALTER TABLE {releases} ADD CONSTRAINT {check} '
            'CHECK (revision > 0)')

    with pytest.raises(RuntimeError, match='incompatible check constraints'):
        _upgrade(engine, '034')
    assert _revision(engine) == '033'


def test_postgres_revision_034_rejects_hostile_boolean_grouping(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '033')
    action_schema.RESOURCE_ACTION_AUTHORITY_RELEASE_METADATA.create_all(engine)
    releases = action_schema.AUTHORITY_RELEASES.name
    check = 'ck_serve_ra_authority_releases_inventories'
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {releases} DROP CONSTRAINT {check}')
        # This has the same tokens as the shipped expression if parentheses
        # are discarded, but the disabled branch bypasses every inventory
        # shape/hash check in the prefix.
        connection.exec_driver_sql(
            f'ALTER TABLE {releases} ADD CONSTRAINT {check} CHECK ('
            "(jsonb_typeof(live_manifests) = 'array' AND "
            "live_inventory_sha256 ~ '^[0-9a-f]{64}$' AND "
            "jsonb_typeof(tombstone_suffixes) = 'array' AND "
            "tombstone_inventory_sha256 ~ '^[0-9a-f]{64}$' AND "
            '(jsonb_array_length(live_manifests) + '
            'jsonb_array_length(tombstone_suffixes)) <= 256 AND '
            'enabled AND (jsonb_array_length(live_manifests) + '
            'jsonb_array_length(tombstone_suffixes)) > 0) OR '
            '(NOT enabled AND jsonb_array_length(live_manifests) = 0 AND '
            'jsonb_array_length(tombstone_suffixes) = 0))')

    with pytest.raises(RuntimeError, match='incompatible check constraints'):
        _upgrade(engine, '034')
    assert _revision(engine) == '033'


def test_postgres_revision_034_rejects_wrong_existing_default(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '033')
    action_schema.RESOURCE_ACTION_AUTHORITY_RELEASE_METADATA.create_all(engine)
    releases = action_schema.AUTHORITY_RELEASES.name
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {releases} ALTER COLUMN revision SET DEFAULT 2')

    with pytest.raises(RuntimeError, match='incompatible column'):
        _upgrade(engine, '034')
    assert _revision(engine) == '033'


def test_postgres_revision_034_rejects_existing_mutation_trigger(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '033')
    action_schema.RESOURCE_ACTION_AUTHORITY_RELEASE_METADATA.create_all(engine)
    releases = action_schema.AUTHORITY_RELEASES.name
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'CREATE FUNCTION hostile_release_update() RETURNS trigger '
            "LANGUAGE plpgsql AS 'BEGIN RETURN OLD; END'")
        connection.exec_driver_sql(
            f'CREATE TRIGGER hostile_release_update BEFORE UPDATE ON '
            f'{releases} FOR EACH ROW EXECUTE FUNCTION '
            'hostile_release_update()')

    with pytest.raises(RuntimeError, match='incompatible relation behavior'):
        _upgrade(engine, '034')
    assert _revision(engine) == '033'


def test_postgres_already_upstream_032_preserves_exact_classification(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    assert _revision(engine) == '032'
    _assert_classification_catalog(engine)
    assert not set(_SERVICE_ACTION_COLUMNS).intersection(
        _column_map(sqlalchemy.inspect(engine), 'services'))
    assert not set(_REPLICA_ACTION_COLUMNS).intersection(
        _column_map(sqlalchemy.inspect(engine), 'replicas'))
    _set_exact_classification_rows(engine)
    classification_rows = _classification_rows(engine)
    ordinary_rows = _ordinary_rows(engine)

    _upgrade(engine, '033')

    assert _classification_rows(engine) == classification_rows
    assert _ordinary_rows(engine) == ordinary_rows
    _assert_classification_catalog(engine)
    _assert_final_postgres_catalog(engine)


def test_abandoned_feature_032_stamp_without_classification_fails_before_ddl(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _install_old_feature_draft(engine)
    _stamp(engine, '032')
    before = _catalog_signature(engine)

    with pytest.raises(RuntimeError, match='exact shipped request-.*columns'):
        _upgrade(engine, '033')

    assert _revision(engine) == '032'
    assert _catalog_signature(engine) == before
    assert 'launch_shadow_coverage_id' not in _column_map(
        sqlalchemy.inspect(engine), 'replicas')


def test_missing_upstream_classification_constraint_fails_before_ddl(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {_DAILY_ACTIVITY} DROP CONSTRAINT '
            f'{_DAILY_PAIR_CONSTRAINT}')
    before = _catalog_signature(engine)

    with pytest.raises(RuntimeError, match=_DAILY_PAIR_CONSTRAINT):
        _upgrade(engine, '033')

    assert _revision(engine) == '032'
    assert _catalog_signature(engine) == before
    assert not set(_SERVICE_ACTION_COLUMNS).intersection(
        _column_map(sqlalchemy.inspect(engine), 'services'))


def test_corrupt_upstream_classification_default_fails_without_mutation(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {_DAILY_ACTIVITY} ALTER COLUMN '
            'classification_incomplete SET DEFAULT true')
    before_catalog = _catalog_signature(engine)
    before_classification = _classification_rows(engine)
    before_ordinary = _ordinary_rows(engine)

    with pytest.raises(RuntimeError, match='classification_incomplete'):
        _upgrade(engine, '033')

    assert _revision(engine) == '032'
    assert _catalog_signature(engine) == before_catalog
    assert _classification_rows(engine) == before_classification
    assert _ordinary_rows(engine) == before_ordinary


def test_weakened_same_name_upstream_check_fails_without_mutation(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f'ALTER TABLE {_RAW_ACTIVITY} DROP CONSTRAINT '
            f'{_RAW_PAIR_CONSTRAINT}')
        connection.exec_driver_sql(
            f'ALTER TABLE {_RAW_ACTIVITY} ADD CONSTRAINT '
            f'{_RAW_PAIR_CONSTRAINT} CHECK '
            '(classified_request_count IS NULL OR '
            'classified_request_count >= 0)')
    before_catalog = _catalog_signature(engine)
    before_classification = _classification_rows(engine)
    before_ordinary = _ordinary_rows(engine)

    with pytest.raises(RuntimeError, match=_RAW_PAIR_CONSTRAINT):
        _upgrade(engine, '033')

    assert _revision(engine) == '032'
    assert _catalog_signature(engine) == before_catalog
    assert _classification_rows(engine) == before_classification
    assert _ordinary_rows(engine) == before_ordinary


def test_every_nonempty_evidence_table_is_rejected_before_ddl(
        postgres_engine) -> None:
    engine = postgres_engine
    for table in _EVIDENCE_TABLES:
        _reset_to_revision_031(engine)
        _upgrade(engine, '032')
        quoted = engine.dialect.identifier_preparer.quote(table)
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f'CREATE TABLE {quoted} (marker INTEGER NOT NULL)')
            connection.exec_driver_sql(
                f'INSERT INTO {quoted} (marker) VALUES (1)')
        before = _catalog_signature(engine)

        with pytest.raises(RuntimeError) as error:
            _upgrade(engine, '033')

        assert 'nonempty resource-action evidence' in str(error.value)
        assert table in str(error.value)
        assert _revision(engine) == '032'
        assert _catalog_signature(engine) == before


def test_nonlegacy_service_state_is_rejected_before_ddl(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE services ADD COLUMN resource_action_mode TEXT "
            "NOT NULL DEFAULT 'legacy'")
        connection.exec_driver_sql(
            'ALTER TABLE services ADD COLUMN '
            'resource_action_mode_changed_at TIMESTAMPTZ')
        connection.execute(
            sqlalchemy.text('UPDATE services SET resource_action_mode = '
                            "'shadow', resource_action_mode_changed_at = "
                            'clock_timestamp()'))
    before = _catalog_signature(engine)

    with pytest.raises(RuntimeError, match='activated service'):
        _upgrade(engine, '033')

    assert _revision(engine) == '032'
    assert _catalog_signature(engine) == before


def test_every_nonnull_replica_action_column_is_rejected_before_ddl(
        postgres_engine) -> None:
    engine = postgres_engine
    for column in _REPLICA_ACTION_COLUMNS:
        _reset_to_revision_031(engine)
        _upgrade(engine, '032')
        sql_type = 'BIGINT' if column == 'desired_generation' else 'UUID'
        value = 1 if column == 'desired_generation' else _REPLICA_UUID
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f'ALTER TABLE replicas ADD COLUMN {column} {sql_type}')
            connection.execute(
                sqlalchemy.text(f'UPDATE replicas SET {column} = :value'),
                {'value': value})
        before = _catalog_signature(engine)

        with pytest.raises(RuntimeError, match='linked replica'):
            _upgrade(engine, '033')

        assert _revision(engine) == '032'
        assert _catalog_signature(engine) == before


def test_empty_old_two_table_hybrid_converges_to_final_catalog(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    classification_rows = _classification_rows(engine)
    ordinary_rows = _ordinary_rows(engine)
    _install_old_feature_draft(engine)
    assert set(_EVIDENCE_TABLES).intersection(
        sqlalchemy.inspect(engine).get_table_names()) == {
            action_schema.STAGED_SHADOW_SAMPLES.name,
            action_schema.STAGED_SHADOW_ATTEMPTS.name,
        }

    _upgrade(engine, '033')

    assert _revision(engine) == '033'
    assert _classification_rows(engine) == classification_rows
    assert _ordinary_rows(engine) == ordinary_rows
    _assert_final_postgres_catalog(engine)


def test_malformed_empty_worker_cohort_refs_is_recreated_exactly(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    classification_rows = _classification_rows(engine)
    ordinary_rows = _ordinary_rows(engine)
    with engine.begin() as connection:
        action_schema.RESOURCE_ACTION_STATE_METADATA.create_all(connection,
                                                                checkfirst=True)
        connection.exec_driver_sql(
            'ALTER TABLE serve_resource_action_worker_cohort_refs '
            'ALTER COLUMN preparation_capability_sha256 DROP NOT NULL')
    malformed = _column_map(
        sqlalchemy.inspect(engine),
        action_schema.WORKER_COHORT_REFS.name)['preparation_capability_sha256']
    assert malformed['nullable']

    _upgrade(engine, '033')

    assert _revision(engine) == '033'
    assert _classification_rows(engine) == classification_rows
    assert _ordinary_rows(engine) == ordinary_rows
    _assert_final_postgres_catalog(engine)
    repaired = _column_map(
        sqlalchemy.inspect(engine),
        action_schema.WORKER_COHORT_REFS.name)['preparation_capability_sha256']
    assert not repaired['nullable']
    assert repaired['default'] is None


def test_partial_worker_cohort_refs_without_primary_key_converges(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    classification_rows = _classification_rows(engine)
    ordinary_rows = _ordinary_rows(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            'CREATE TABLE serve_resource_action_worker_cohort_refs '
            '(decision_id UUID NOT NULL, cohort_id TEXT, '
            'preparation_capability_sha256 TEXT)')
    malformed_primary_key = sqlalchemy.inspect(engine).get_pk_constraint(
        action_schema.WORKER_COHORT_REFS.name)
    assert not malformed_primary_key['constrained_columns']

    _upgrade(engine, '033')

    assert _revision(engine) == '033'
    assert _classification_rows(engine) == classification_rows
    assert _ordinary_rows(engine) == ordinary_rows
    _assert_final_postgres_catalog(engine)


@pytest.mark.parametrize('partial_table', _EVIDENCE_TABLES)
def test_each_empty_partial_action_table_subset_converges(
        postgres_engine, partial_table: str) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '032')
    classification_rows = _classification_rows(engine)
    ordinary_rows = _ordinary_rows(engine)
    quoted = engine.dialect.identifier_preparer.quote(partial_table)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f'CREATE TABLE {quoted} (partial_marker INTEGER)')

    _upgrade(engine, '033')

    assert _revision(engine) == '033'
    assert _classification_rows(engine) == classification_rows
    assert _ordinary_rows(engine) == ordinary_rows
    _assert_final_postgres_catalog(engine)


def test_complete_lost_ack_catalog_converges_idempotently(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '033')
    before_catalog = _catalog_signature(engine)
    before_classification = _classification_rows(engine)
    before_ordinary = _ordinary_rows(engine)
    _stamp(engine, '032')

    _upgrade(engine, '033')

    assert _revision(engine) == '033'
    assert _catalog_signature(engine) == before_catalog
    assert _classification_rows(engine) == before_classification
    assert _ordinary_rows(engine) == before_ordinary
    _assert_final_postgres_catalog(engine)


def test_revision_033_refuses_downgrade_and_preserves_catalog(
        postgres_engine) -> None:
    engine = postgres_engine
    _reset_to_revision_031(engine)
    _upgrade(engine, '033')
    before = _catalog_signature(engine)
    config = migration_utils.get_alembic_config(engine,
                                                migration_utils.SERVE_DB_NAME)

    with pytest.raises(RuntimeError, match='cannot be downgraded'):
        alembic_command.downgrade(config, '032')

    assert _revision(engine) == '033'
    assert _catalog_signature(engine) == before
