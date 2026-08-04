"""Add the durable authority-worker Helm release ledger.

Revision ID: 034
Revises: 033
Create Date: 2026-08-03

"""
# pylint: disable=invalid-name
from collections.abc import Sequence
import re
import uuid

from alembic import op
import sqlalchemy as sa

from sky.serve import resource_action_state_schema
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '034'
down_revision: str | Sequence[str] | None = '033'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _normalized_pg_node_tree(value: str | None) -> str | None:
    """Remove only parser source offsets from a PostgreSQL expression tree."""
    if value is None:
        return None
    return re.sub(r' :location -?[0-9]+', '', value)


def _default_node_trees(bind: sa.engine.Connection,
                        relation: str) -> dict[str, str | None]:
    rows = bind.execute(
        sa.text('SELECT attribute.attname, definition.adbin::text '
                'FROM pg_catalog.pg_attribute AS attribute '
                'LEFT JOIN pg_catalog.pg_attrdef AS definition '
                'ON definition.adrelid = attribute.attrelid '
                'AND definition.adnum = attribute.attnum '
                'WHERE attribute.attrelid = pg_catalog.to_regclass(:relation) '
                'AND attribute.attnum > 0 AND NOT attribute.attisdropped'),
        {'relation': relation})
    return {
        str(name): _normalized_pg_node_tree(node_tree)
        for name, node_tree in rows
    }


def _column_semantic_catalog(
    bind: sa.engine.Connection,
    relation: str,
) -> dict[str, tuple[int, int, int, bool, str, str]]:
    rows = bind.execute(
        sa.text('SELECT attribute.attname, attribute.atttypid, '
                'attribute.atttypmod, attribute.attcollation, '
                'attribute.attnotnull, attribute.attidentity, '
                'attribute.attgenerated '
                'FROM pg_catalog.pg_attribute AS attribute '
                'WHERE attribute.attrelid = pg_catalog.to_regclass(:relation) '
                'AND attribute.attnum > 0 AND NOT attribute.attisdropped'),
        {'relation': relation})
    return {
        str(name): (int(type_oid), int(type_modifier), int(collation_oid),
                    bool(not_null), str(identity), str(generated))
        for (name, type_oid, type_modifier, collation_oid, not_null, identity,
             generated) in rows
    }


def _check_node_trees(
    bind: sa.engine.Connection,
    relation: str,
) -> dict[str, tuple[str, bool, bool]]:
    rows = bind.execute(
        sa.text('SELECT check_constraint.conname, '
                'check_constraint.conbin::text, '
                'check_constraint.convalidated, check_constraint.connoinherit '
                'FROM pg_catalog.pg_constraint AS check_constraint '
                'WHERE check_constraint.conrelid = '
                'pg_catalog.to_regclass(:relation) '
                "AND check_constraint.contype = 'c'"), {'relation': relation})
    return {
        str(name): (_normalized_pg_node_tree(node_tree) or
                    '', bool(validated), bool(no_inherit)
                   ) for name, node_tree, validated, no_inherit in rows
    }


def _expected_expression_catalogs(
    bind: sa.engine.Connection,
    table: sa.Table,
) -> tuple[dict[str, str | None], dict[str, tuple[str, bool, bool]], dict[
        str, tuple[int, int, int, bool, str, str]]]:
    """Have PostgreSQL parse the shipped defaults and CHECK expressions."""
    reference_metadata = sa.MetaData()
    reference_name = f'sky_ra034_expected_{uuid.uuid4().hex}'
    columns = []
    for column in table.columns:
        default = column.server_default
        columns.append(
            sa.Column(column.name,
                      column.type,
                      nullable=column.nullable,
                      server_default=None if default is None else default.arg))
    checks = [
        sa.CheckConstraint(str(constraint.sqltext), name=constraint.name)
        for constraint in table.constraints
        if isinstance(constraint, sa.CheckConstraint)
    ]
    reference = sa.Table(reference_name,
                         reference_metadata,
                         *columns,
                         *checks,
                         prefixes=['TEMPORARY'])
    reference.create(bind)
    catalogs = (_default_node_trees(bind, reference_name),
                _check_node_trees(bind, reference_name),
                _column_semantic_catalog(bind, reference_name))
    reference.drop(bind)
    return catalogs


def _expected_foreign_keys(
    table: sa.Table
) -> dict[str, tuple[tuple[str, ...], str | None, str, tuple[str, ...], str |
                     None, str | None, bool | None, str | None, str | None]]:
    expected: dict[str, tuple[tuple[str, ...], str | None, str, tuple[str, ...],
                              str | None, str | None, bool | None, str | None,
                              str | None]] = {}
    for constraint in table.foreign_key_constraints:
        elements = tuple(constraint.elements)
        expected[str(constraint.name)] = (
            tuple(element.parent.name for element in elements),
            None,
            elements[0].column.table.name,
            tuple(element.column.name for element in elements),
            constraint.onupdate,
            constraint.ondelete,
            constraint.deferrable,
            constraint.initially,
            constraint.match,
        )
    return expected


def _expected_noncheck_constraint_flags(
    table: sa.Table,) -> dict[str, tuple[str, bool, bool, bool]]:
    expected = {}
    for constraint in table.constraints:
        constraint_type = None
        if isinstance(constraint, sa.PrimaryKeyConstraint):
            constraint_type = 'p'
        elif isinstance(constraint, sa.UniqueConstraint):
            constraint_type = 'u'
        elif isinstance(constraint, sa.ForeignKeyConstraint):
            constraint_type = 'f'
        if constraint_type is not None:
            expected[str(
                constraint.name)] = (constraint_type,
                                     bool(constraint.deferrable),
                                     constraint.initially == 'DEFERRED', True)
    return expected


def _noncheck_constraint_flags(
    bind: sa.engine.Connection,
    relation: str,
) -> dict[str, tuple[str, bool, bool, bool]]:
    rows = bind.execute(
        sa.text('SELECT key_constraint.conname, key_constraint.contype, '
                'key_constraint.condeferrable, key_constraint.condeferred, '
                'key_constraint.convalidated '
                'FROM pg_catalog.pg_constraint AS key_constraint '
                'WHERE key_constraint.conrelid = '
                'pg_catalog.to_regclass(:relation) '
                "AND key_constraint.contype <> 'c'"), {'relation': relation})
    return {
        str(name):
            (str(kind), bool(deferrable), bool(deferred), bool(validated))
        for name, kind, deferrable, deferred, validated in rows
    }


def _relation_behavior(bind: sa.engine.Connection,
                       relation: str) -> tuple[object, ...]:
    row = bind.execute(
        sa.text('SELECT relation.relkind, relation.relpersistence, '
                'relation.relrowsecurity, relation.relforcerowsecurity, '
                'relation.relispartition, relation.relreplident, '
                'EXISTS (SELECT 1 FROM pg_catalog.pg_inherits '
                'WHERE inhrelid = relation.oid OR inhparent = relation.oid), '
                'EXISTS (SELECT 1 FROM pg_catalog.pg_trigger '
                'WHERE tgrelid = relation.oid AND NOT tgisinternal), '
                'EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite '
                'WHERE ev_class = relation.oid), '
                'EXISTS (SELECT 1 FROM pg_catalog.pg_policy '
                'WHERE polrelid = relation.oid) '
                'FROM pg_catalog.pg_class AS relation '
                'WHERE relation.oid = pg_catalog.to_regclass(:relation)'), {
                    'relation': relation
                }).one_or_none()
    if row is None:
        raise RuntimeError(
            f'SkyServe schema 034 could not resolve table {relation!r}.')
    return tuple(row)


_EXPECTED_RELATION_BEHAVIOR = ('r', 'p', False, False, False, 'd', False, False,
                               False, False)


def _verify_table(bind: sa.engine.Connection, table: sa.Table) -> None:
    """Fail closed if a pre-existing table is not the shipped contract."""
    inspector = sa.inspect(bind)
    (expected_defaults, expected_checks,
     expected_column_catalog) = _expected_expression_catalogs(bind, table)
    actual_defaults = _default_node_trees(bind, table.name)
    if _relation_behavior(bind, table.name) != _EXPECTED_RELATION_BEHAVIOR:
        raise RuntimeError('SkyServe schema 034 found incompatible relation '
                           f'behavior for {table.name!r}.')
    columns = {
        str(column['name']): column
        for column in inspector.get_columns(table.name)
    }
    if set(columns) != set(table.c.keys()):
        raise RuntimeError('SkyServe schema 034 found an incompatible column '
                           f'inventory for {table.name!r}.')
    for expected in table.columns:
        actual = columns[expected.name]
        expected_type = str(expected.type.compile(dialect=bind.dialect)).upper()
        actual_type = str(actual['type'].compile(dialect=bind.dialect)).upper()
        if (actual_type != expected_type or
                bool(actual['nullable']) != expected.nullable):
            raise RuntimeError('SkyServe schema 034 found an incompatible '
                               f'column {table.name}.{expected.name}.')
    if actual_defaults != expected_defaults:
        raise RuntimeError('SkyServe schema 034 found incompatible column '
                           f'defaults for {table.name!r}.')
    if _column_semantic_catalog(bind, table.name) != expected_column_catalog:
        raise RuntimeError('SkyServe schema 034 found incompatible column '
                           f'semantics for {table.name!r}.')

    primary = inspector.get_pk_constraint(table.name)
    if (primary.get('name') != table.primary_key.name or tuple(
            primary.get('constrained_columns') or
        ()) != tuple(column.name for column in table.primary_key.columns)):
        raise RuntimeError('SkyServe schema 034 found an incompatible primary '
                           f'key for {table.name!r}.')

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
        raise RuntimeError('SkyServe schema 034 found incompatible unique '
                           f'constraints for {table.name!r}.')
    if (_noncheck_constraint_flags(bind, table.name)
            != _expected_noncheck_constraint_flags(table)):
        raise RuntimeError('SkyServe schema 034 found incompatible key '
                           f'constraint behavior for {table.name!r}.')

    actual_checks = _check_node_trees(bind, table.name)
    if actual_checks != expected_checks:
        raise RuntimeError('SkyServe schema 034 found incompatible check '
                           f'constraints for {table.name!r}.')

    actual_foreign_keys = {
        str(constraint['name']): (
            tuple(constraint['constrained_columns']),
            constraint.get('referred_schema'),
            str(constraint['referred_table']),
            tuple(constraint['referred_columns']),
            (constraint.get('options') or {}).get('onupdate'),
            (constraint.get('options') or {}).get('ondelete'),
            (constraint.get('options') or {}).get('deferrable'),
            (constraint.get('options') or {}).get('initially'),
            (constraint.get('options') or {}).get('match'),
        ) for constraint in inspector.get_foreign_keys(table.name)
    }
    if actual_foreign_keys != _expected_foreign_keys(table):
        raise RuntimeError('SkyServe schema 034 found incompatible foreign '
                           f'keys for {table.name!r}.')


def upgrade() -> None:
    """Install the PostgreSQL-only immutable release/cohort bindings."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    metadata = (
        resource_action_state_schema.RESOURCE_ACTION_AUTHORITY_RELEASE_METADATA)
    metadata.create_all(bind, checkfirst=True)
    for table in metadata.sorted_tables:
        _verify_table(bind, table)


def downgrade() -> None:
    """Retain immutable release identities on application rollback."""
    raise RuntimeError(
        'SkyServe schema 034 is additive and cannot be downgraded. Roll back '
        'the application against the retained release ledger instead.')
