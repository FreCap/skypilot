"""Add the guarded SkyServe resource-action catalog.

Revision ID: 033
Revises: 032
Create Date: 2026-08-01

"""
# pylint: disable=invalid-name
from collections.abc import Sequence
import typing

from alembic import op
import sqlalchemy as sa

from sky.serve import resource_action_state_schema
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '033'
down_revision: str | Sequence[str] | None = '032'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SERVICES = 'services'
_REPLICAS = 'replicas'
_RAW_ACTIVITY = 'serve_request_activity_history'
_DAILY_ACTIVITY = 'serve_request_activity_daily'

_RAW_PAIR_CONSTRAINT = 'serve_request_activity_history_classified_pair'
_DAILY_PAIR_CONSTRAINT = 'serve_request_activity_daily_classified_pair'

_RAW_PAIR_EXPRESSION = ('(classified_request_count IS NULL AND '
                        'counted_rejected_count IS NULL) OR '
                        '(classified_request_count IS NOT NULL AND '
                        'counted_rejected_count IS NOT NULL AND '
                        'classified_request_count >= 0 AND '
                        'counted_rejected_count >= 0 AND '
                        'counted_rejected_count <= classified_request_count)')
_DAILY_PAIR_EXPRESSION = (
    '(classified_request_count IS NULL AND '
    'counted_rejected_count IS NULL AND '
    'classified_first_bucket_start IS NULL AND '
    'classified_last_bucket_start IS NULL) OR '
    '(classified_request_count IS NOT NULL AND '
    'counted_rejected_count IS NOT NULL AND '
    'classified_request_count >= 0 AND '
    'counted_rejected_count >= 0 AND '
    'counted_rejected_count <= classified_request_count AND '
    'classified_first_bucket_start IS NOT NULL AND '
    'classified_last_bucket_start IS NOT NULL AND '
    'classified_first_bucket_start <= classified_last_bucket_start)')

_SERVICE_CHECKS = {
    'ck_services_resource_action_mode': "resource_action_mode IN ('legacy', 'shadow', 'authoritative')",
    'ck_services_resource_action_mode_timestamp':
        "resource_action_mode = 'legacy' OR "
        'resource_action_mode_changed_at IS NOT NULL',
}

_REPLICA_CHECKS = {
    'ck_replicas_resource_action_identity':
        '(replica_incarnation IS NULL AND desired_generation IS NULL AND '
        'sky_cluster_record_uuid IS NULL) OR '
        '(replica_incarnation IS NOT NULL AND '
        'desired_generation IS NOT NULL AND desired_generation > 0 AND '
        'sky_cluster_record_uuid IS NOT NULL)',
    'ck_replicas_resource_action_links':
        'replica_incarnation IS NOT NULL OR '
        '(launch_action_id IS NULL AND down_action_id IS NULL AND '
        'launch_shadow_coverage_id IS NULL AND '
        'down_shadow_coverage_id IS NULL AND '
        'launch_shadow_sample_id IS NULL AND down_shadow_sample_id IS NULL)',
    'ck_replicas_resource_action_launch_exclusive': 'launch_action_id IS NULL OR launch_shadow_coverage_id IS NULL',
    'ck_replicas_resource_action_down_exclusive': 'down_action_id IS NULL OR down_shadow_coverage_id IS NULL',
    'ck_replicas_resource_action_shadow_links':
        '(launch_shadow_sample_id IS NULL OR '
        '(launch_shadow_coverage_id IS NOT NULL AND '
        'launch_shadow_sample_id = launch_shadow_coverage_id)) AND '
        '(down_shadow_sample_id IS NULL OR '
        '(down_shadow_coverage_id IS NOT NULL AND '
        'down_shadow_sample_id = down_shadow_coverage_id))',
}

_REPLICA_UNIQUE_INDEXES = {
    'uq_replicas_ra_replica_incarnation': 'replica_incarnation',
    'uq_replicas_ra_sky_cluster_record_uuid': 'sky_cluster_record_uuid',
    'uq_replicas_ra_launch_action_id': 'launch_action_id',
    'uq_replicas_ra_down_action_id': 'down_action_id',
    'uq_replicas_ra_launch_shadow_sample': 'launch_shadow_sample_id',
    'uq_replicas_ra_down_shadow_sample': 'down_shadow_sample_id',
    'uq_replicas_ra_launch_shadow_coverage': 'launch_shadow_coverage_id',
    'uq_replicas_ra_down_shadow_coverage': 'down_shadow_coverage_id',
}

_ACTION_REPLICA_COLUMNS = tuple(
    _REPLICA_UNIQUE_INDEXES.values()) + ('desired_generation',)

_ColumnContract = tuple[str, bool, str | None]

_UPSTREAM_032_COLUMNS: dict[str, dict[str, _ColumnContract]] = {
    _RAW_ACTIVITY: {
        'classified_request_count': ('INTEGER', True, None),
        'counted_rejected_count': ('INTEGER', True, None),
    },
    _DAILY_ACTIVITY: {
        'classified_request_count': ('BIGINT', True, None),
        'counted_rejected_count': ('BIGINT', True, None),
        'classified_first_bucket_start':
            ('TIMESTAMP WITH TIME ZONE', True, None),
        'classified_last_bucket_start':
            ('TIMESTAMP WITH TIME ZONE', True, None),
        'classification_incomplete': ('BOOLEAN', False, 'false'),
    },
}

_PORTABLE_ACTION_COLUMNS: dict[str, dict[str, _ColumnContract]] = {
    _SERVICES: {
        'resource_action_mode': ('TEXT', False, "'legacy'"),
        'resource_action_mode_changed_at':
            ('TIMESTAMP WITH TIME ZONE', True, None),
    },
    _REPLICAS: {
        'replica_incarnation': ('UUID', True, None),
        'desired_generation': ('BIGINT', True, None),
        'sky_cluster_record_uuid': ('UUID', True, None),
        'launch_action_id': ('UUID', True, None),
        'down_action_id': ('UUID', True, None),
        'launch_shadow_sample_id': ('UUID', True, None),
        'down_shadow_sample_id': ('UUID', True, None),
        'launch_shadow_coverage_id': ('UUID', True, None),
        'down_shadow_coverage_id': ('UUID', True, None),
    },
}


def _column_map(bind: sa.engine.Connection,
                table: str) -> dict[str, dict[str, typing.Any]]:
    return {
        str(column['name']): column
        for column in sa.inspect(bind).get_columns(table)
    }


def _check_map(bind: sa.engine.Connection,
               table: str) -> dict[str, dict[str, typing.Any]]:
    return {
        str(constraint['name']): constraint
        for constraint in sa.inspect(bind).get_check_constraints(table)
        if constraint['name'] is not None
    }


def _normalized_sql(value: typing.Any) -> str:
    normalized = ''.join(character for character in str(value).lower()
                         if not character.isspace() and character not in '()"')
    # PostgreSQL's catalog renderer adds explicit text casts and rewrites
    # ``value IN (...)`` as ``value = ANY (ARRAY[...])``.  SQLAlchemy preserves
    # the source spelling when it compiles the matching metadata expression.
    return (normalized.replace('::text', '').replace('=anyarray[',
                                                     'in').replace(']', ''))


def _column_contract(
        bind: sa.engine.Connection,
        column: dict[str, typing.Any]) -> tuple[str, bool, str | None]:
    default = column['default']
    return (str(column['type'].compile(dialect=bind.dialect)).upper(),
            bool(column['nullable']),
            None if default is None else _normalized_sql(default))


def _assert_column_contract(
    bind: sa.engine.Connection,
    table: str,
    expected: dict[str, _ColumnContract],
    *,
    allow_missing: bool,
    error_prefix: str,
) -> None:
    columns = _column_map(bind, table)
    for name, contract in expected.items():
        column = columns.get(name)
        if column is None and allow_missing:
            continue
        if column is None or _column_contract(bind, column) != contract:
            raise RuntimeError(f'{error_prefix}; {table}.{name} is missing or '
                               'incompatible.')


def _assert_exact_check(bind: sa.engine.Connection, table: str, name: str,
                        expression: str, error_prefix: str) -> None:
    check = _check_map(bind, table).get(name)
    if (check is None or
            _normalized_sql(check['sqltext']) != _normalized_sql(expression)):
        raise RuntimeError(f'{error_prefix} {name!r}.')


def _assert_upstream_032_catalog() -> None:
    """Require the shipped request-classification revision-032 shape."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    error_prefix = ('SkyServe schema 033 requires the exact shipped request-'
                    'classification revision 032 columns')
    for table, expected_columns in _UPSTREAM_032_COLUMNS.items():
        if not inspector.has_table(table):
            raise RuntimeError(
                'SkyServe schema 033 requires the shipped request-'
                f'classification revision 032 table {table!r}.')
        _assert_column_contract(bind,
                                table,
                                expected_columns,
                                allow_missing=False,
                                error_prefix=error_prefix)
    check_error = ('SkyServe schema 033 requires the exact shipped request-'
                   'classification revision 032 constraint')
    _assert_exact_check(bind, _RAW_ACTIVITY, _RAW_PAIR_CONSTRAINT,
                        _RAW_PAIR_EXPRESSION, check_error)
    _assert_exact_check(bind, _DAILY_ACTIVITY, _DAILY_PAIR_CONSTRAINT,
                        _DAILY_PAIR_EXPRESSION, check_error)


def _has_rows(bind: sa.engine.Connection, table: str) -> bool:
    quoted = bind.dialect.identifier_preparer.quote(table)
    return bool(
        bind.execute(sa.text(
            f'SELECT EXISTS (SELECT 1 FROM {quoted} LIMIT 1)')).scalar_one())


def _assert_unactivated_action_state() -> None:
    """Refuse to invent action evidence while reconciling draft catalogs."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    evidence_tables = (
        resource_action_state_schema.STAGED_SHADOW_SAMPLES.name,
        resource_action_state_schema.STAGED_SHADOW_ATTEMPTS.name,
        resource_action_state_schema.WORKER_COHORTS.name,
        resource_action_state_schema.WORKER_COHORT_REFS.name,
        resource_action_state_schema.SHADOW_COVERAGE.name,
        resource_action_state_schema.SHADOW_COVERAGE_ATTEMPTS.name,
    )
    for table in evidence_tables:
        if inspector.has_table(table) and _has_rows(bind, table):
            raise RuntimeError(
                'SkyServe schema 033 cannot reconcile nonempty resource-'
                f'action evidence ({table}); a reviewed backfill is required.')

    service_columns = _column_map(bind, _SERVICES)
    service_predicates = []
    if 'resource_action_mode' in service_columns:
        service_predicates.append(
            "resource_action_mode IS DISTINCT FROM 'legacy'")
    if 'resource_action_mode_changed_at' in service_columns:
        service_predicates.append('resource_action_mode_changed_at IS NOT NULL')
    if service_predicates:
        predicate = ' OR '.join(service_predicates)
        if bind.execute(
                sa.text(f'SELECT EXISTS (SELECT 1 FROM services WHERE '
                        f'{predicate} LIMIT 1)')).scalar_one():
            raise RuntimeError(
                'SkyServe schema 033 cannot reconcile activated service '
                'resource-action state; a reviewed backfill is required.')

    replica_columns = _column_map(bind, _REPLICAS)
    present_action_columns = [
        column for column in _ACTION_REPLICA_COLUMNS
        if column in replica_columns
    ]
    if present_action_columns:
        predicate = ' OR '.join(
            f'{column} IS NOT NULL' for column in present_action_columns)
        if bind.execute(
                sa.text(f'SELECT EXISTS (SELECT 1 FROM replicas WHERE '
                        f'{predicate} LIMIT 1)')).scalar_one():
            raise RuntimeError(
                'SkyServe schema 033 cannot reconcile linked replica '
                'resource-action state; a reviewed backfill is required.')


def _assert_compatible_existing_action_columns() -> None:
    """Reject a hybrid whose retained portable columns changed meaning."""
    bind = op.get_bind()
    error_prefix = ('SkyServe schema 033 cannot reconcile an incompatible '
                    'portable resource-action column')
    for table, expected in _PORTABLE_ACTION_COLUMNS.items():
        _assert_column_contract(bind,
                                table,
                                expected,
                                allow_missing=True,
                                error_prefix=error_prefix)


def _add_missing_columns(table: str, columns: tuple[sa.Column, ...]) -> None:
    """Add columns without relying on current-base bootstrap metadata."""
    existing = set(_column_map(op.get_bind(), table))
    for column in columns:
        if column.name not in existing:
            op.add_column(table, column)
            existing.add(column.name)


def _ensure_postgres_common_contract() -> None:
    bind = op.get_bind()
    for table, expected_checks in ((_SERVICES, _SERVICE_CHECKS),
                                   (_REPLICAS, _REPLICA_CHECKS)):
        existing = set(_check_map(bind, table))
        for name, expression in expected_checks.items():
            if name in existing:
                op.drop_constraint(name, table, type_='check')
            op.create_check_constraint(name, table, expression)

    existing_indexes = {
        str(index['name']) for index in sa.inspect(bind).get_indexes(_REPLICAS)
    }
    for name, column in _REPLICA_UNIQUE_INDEXES.items():
        if name in existing_indexes:
            op.drop_index(name, table_name=_REPLICAS)
        op.create_index(name,
                        _REPLICAS, [column],
                        unique=True,
                        postgresql_where=sa.text(f'{column} IS NOT NULL'))


def _verify_postgres_common_contract() -> None:
    bind = op.get_bind()
    error_prefix = ('SkyServe schema 033 failed its reflected portable-column '
                    'postcondition')
    for table, expected in _PORTABLE_ACTION_COLUMNS.items():
        _assert_column_contract(bind,
                                table,
                                expected,
                                allow_missing=False,
                                error_prefix=error_prefix)
    for table, expected_checks in ((_SERVICES, _SERVICE_CHECKS),
                                   (_REPLICAS, _REPLICA_CHECKS)):
        for name, expression in expected_checks.items():
            _assert_exact_check(
                bind, table, name, expression,
                'SkyServe schema 033 failed its reflected check postcondition')

    indexes = {
        str(index['name']): index
        for index in sa.inspect(bind).get_indexes(_REPLICAS)
    }
    for name, column in _REPLICA_UNIQUE_INDEXES.items():
        index = indexes.get(name)
        where = None if index is None else (index.get('dialect_options') or
                                            {}).get('postgresql_where')
        if (index is None or not bool(index['unique']) or
                tuple(index['column_names']) != (column,) or
                _normalized_sql(where)
                != _normalized_sql(f'{column} IS NOT NULL')):
            raise RuntimeError(
                'SkyServe schema 033 failed its reflected replica-index '
                f'postcondition {name!r}.')


def _action_tables() -> tuple[sa.Table, ...]:
    metadata = resource_action_state_schema.RESOURCE_ACTION_STATE_METADATA
    return tuple(metadata.sorted_tables)


def _expected_foreign_keys(
    table: sa.Table
) -> dict[str, tuple[tuple[str, ...], str, tuple[str, ...], str | None]]:
    expected = {}
    for constraint in table.foreign_key_constraints:
        elements = tuple(constraint.elements)
        assert constraint.name is not None and elements
        expected[constraint.name] = (
            tuple(element.parent.name for element in elements),
            elements[0].column.table.name,
            tuple(element.column.name for element in elements),
            constraint.ondelete,
        )
    return expected


def _verify_postgres_action_table(table: sa.Table) -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    actual_columns = _column_map(bind, table.name)
    if tuple(actual_columns) != tuple(table.c.keys()):
        raise RuntimeError('SkyServe schema 033 failed its reflected column '
                           f'postcondition for {table.name!r}.')
    for column in table.columns:
        actual = actual_columns[column.name]
        expected_type = str(column.type.compile(dialect=bind.dialect)).upper()
        if (str(actual['type'].compile(dialect=bind.dialect)).upper()
                != expected_type or
                bool(actual['nullable']) != column.nullable or
            (actual['default'] is None) != (column.server_default is None)):
            raise RuntimeError(
                'SkyServe schema 033 failed its reflected column '
                f'postcondition for {table.name}.{column.name}.')

    expected_primary = table.primary_key
    actual_primary = inspector.get_pk_constraint(table.name)
    if (actual_primary.get('name') != expected_primary.name or tuple(
            actual_primary.get('constrained_columns') or
        ()) != tuple(column.name for column in expected_primary.columns)):
        raise RuntimeError('SkyServe schema 033 failed its reflected primary-'
                           f'key postcondition for {table.name!r}.')

    expected_checks = {
        str(constraint.name)
        for constraint in table.constraints
        if isinstance(constraint, sa.CheckConstraint)
    }
    if set(_check_map(bind, table.name)) != expected_checks:
        raise RuntimeError('SkyServe schema 033 failed its reflected check '
                           f'postcondition for {table.name!r}.')

    expected_uniques = {
        str(constraint.name):
            tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, sa.UniqueConstraint)
    }
    actual_uniques = {
        str(constraint['name']): tuple(constraint['column_names'])
        for constraint in inspector.get_unique_constraints(table.name)
    }
    if actual_uniques != expected_uniques:
        raise RuntimeError('SkyServe schema 033 failed its reflected unique '
                           f'postcondition for {table.name!r}.')

    actual_foreign_keys = {
        str(constraint['name']): (
            tuple(constraint['constrained_columns']),
            str(constraint['referred_table']),
            tuple(constraint['referred_columns']),
            constraint['options'].get('ondelete')
        ) for constraint in inspector.get_foreign_keys(table.name)
    }
    if actual_foreign_keys != _expected_foreign_keys(table):
        raise RuntimeError('SkyServe schema 033 failed its reflected foreign-'
                           f'key postcondition for {table.name!r}.')

    actual_indexes = {
        str(index['name']): index
        for index in inspector.get_indexes(table.name)
        if index.get('duplicates_constraint') is None
    }
    if set(actual_indexes) != {str(index.name) for index in table.indexes}:
        raise RuntimeError('SkyServe schema 033 failed its reflected index '
                           f'postcondition for {table.name!r}.')
    for expected in table.indexes:
        actual = actual_indexes[str(expected.name)]
        expected_where = expected.dialect_options['postgresql']['where']
        actual_where = (actual.get('dialect_options') or
                        {}).get('postgresql_where')
        if (bool(actual['unique']) != bool(expected.unique) or
                tuple(actual['column_names']) != tuple(
                    column.name for column in expected.columns) or
                _normalized_sql(actual_where)
                != _normalized_sql(expected_where)):
            raise RuntimeError('SkyServe schema 033 failed its reflected index '
                               f'postcondition {expected.name!r}: expected '
                               f'{expected_where!s}; found {actual_where!s}.')


def _replace_postgres_action_tables() -> None:
    """Replace the proven-empty draft graph with the exact head graph."""
    bind = op.get_bind()
    for table in reversed(_action_tables()):
        table.drop(bind, checkfirst=True)
    resource_action_state_schema.RESOURCE_ACTION_STATE_METADATA.create_all(
        bind, checkfirst=False)
    expected_names = {table.name for table in _action_tables()}
    actual_names = {
        name for name in sa.inspect(bind).get_table_names()
        if name.startswith('serve_resource_action_')
    }
    if actual_names != expected_names:
        raise RuntimeError('SkyServe schema 033 failed its reflected action-'
                           'table postcondition.')
    for table in _action_tables():
        _verify_postgres_action_table(table)


def upgrade() -> None:
    """Install inert portable columns and PostgreSQL action evidence."""
    bind = op.get_bind()
    is_postgres = (
        bind.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value)
    if is_postgres:
        # These are deliberately the first revision-033 operations. PostgreSQL
        # DDL is transactional, but the no-backfill decision is audited before
        # this migration mutates even the catalog.
        _assert_upstream_032_catalog()
        _assert_unactivated_action_state()
        _assert_compatible_existing_action_columns()

    _add_missing_columns(_SERVICES,
                         resource_action_state_schema.service_columns())
    _add_missing_columns(_REPLICAS,
                         resource_action_state_schema.replica_columns())
    _add_missing_columns(
        _REPLICAS, resource_action_state_schema.replica_coverage_columns())
    if not is_postgres:
        return

    _ensure_postgres_common_contract()
    _replace_postgres_action_tables()
    _verify_postgres_common_contract()


def downgrade() -> None:
    """Retain additive resource-action evidence on application rollback."""
    raise RuntimeError(
        'SkyServe schema 033 is additive and cannot be downgraded. Roll back '
        'the application against the retained schema instead.')
