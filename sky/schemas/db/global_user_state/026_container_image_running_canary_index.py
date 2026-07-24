"""Fence qualification mutation and index active managed-image canaries.

Revision ID: 026
Revises: 025
Create Date: 2026-07-24

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '026'
down_revision: str | Sequence[str] | None = '025'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX = 'ix_container_image_operations_running_canary_revision'
_INDEX_TABLE = 'container_image_operations'
_INDEX_COLUMNS = ('result_id',)
_INDEX_PREDICATE = "kind = 'PROFILE_CANARY' AND state = 'RUNNING'"


def _normalize_predicate(predicate: str | None) -> str | None:
    if predicate is None:
        return None
    normalized = ''.join(predicate.replace('::text', '').split())
    return normalized.replace('(', '').replace(')', '')


def _postgres_index_state(
        bind: sqlalchemy.engine.Connection
) -> sqlalchemy.engine.RowMapping | None:
    """Returns the complete current-schema shape for the reserved index."""
    return bind.execute(
        sqlalchemy.text("""
            SELECT table_namespace.nspname AS table_schema,
                   table_class.relname AS table_name,
                   index_namespace.nspname AS index_schema,
                   index_row.indisvalid AS is_valid,
                   index_row.indisready AS is_ready,
                   index_row.indisunique AS is_unique,
                   index_row.indisprimary AS is_primary,
                   index_row.indisexclusion AS is_exclusion,
                   index_row.indexprs IS NULL AS is_expression_free,
                   access_method.amname AS access_method,
                   index_row.indnkeyatts AS key_count,
                   index_row.indnatts AS attribute_count,
                   ARRAY(
                       SELECT pg_get_indexdef(
                           index_row.indexrelid, key_position, TRUE)
                       FROM generate_series(
                           1, index_row.indnkeyatts) AS key_position
                   ) AS key_columns,
                   NOT EXISTS (
                       SELECT 1
                       FROM unnest(index_row.indoption::smallint[])
                            AS options(value)
                       WHERE value <> 0
                   ) AS has_default_ordering,
                   pg_get_expr(
                       index_row.indpred, index_row.indrelid, TRUE) AS predicate
            FROM pg_index AS index_row
            JOIN pg_class AS index_class
              ON index_class.oid = index_row.indexrelid
            JOIN pg_namespace AS index_namespace
              ON index_namespace.oid = index_class.relnamespace
            JOIN pg_class AS table_class
              ON table_class.oid = index_row.indrelid
            JOIN pg_namespace AS table_namespace
              ON table_namespace.oid = table_class.relnamespace
            JOIN pg_am AS access_method
              ON access_method.oid = index_class.relam
            WHERE index_namespace.nspname = current_schema()
              AND index_class.relname = :name
              AND index_class.relkind = 'i'
            """), {
            'name': _INDEX
        }).mappings().one_or_none()


def _postgres_shape_matches(index: sqlalchemy.engine.RowMapping) -> bool:
    return (str(index['table_schema']) == str(index['index_schema']) and
            str(index['table_name']) == _INDEX_TABLE and
            bool(index['is_valid']) and bool(index['is_ready']) and
            not bool(index['is_unique']) and not bool(index['is_primary']) and
            not bool(index['is_exclusion']) and
            bool(index['is_expression_free']) and
            str(index['access_method']) == 'btree' and
            int(index['key_count']) == len(_INDEX_COLUMNS) and
            int(index['attribute_count']) == len(_INDEX_COLUMNS) and
            tuple(index['key_columns'] or ()) == _INDEX_COLUMNS and
            bool(index['has_default_ordering']) and
            _normalize_predicate(str(
                index['predicate'])) == _normalize_predicate(_INDEX_PREDICATE))


def _ensure_index(bind: sqlalchemy.engine.Connection) -> None:
    """Creates the exact partial index, repairing interrupted PG residue."""
    index = _postgres_index_state(bind)
    if index is not None:
        if (str(index['table_schema']) != str(index['index_schema']) or
                str(index['table_name']) != _INDEX_TABLE):
            raise RuntimeError(
                f'Existing managed-image index {_INDEX!r} belongs to an '
                'unexpected table.')
        if not bool(index['is_valid']) or not bool(index['is_ready']):
            preparer = bind.dialect.identifier_preparer
            qualified_name = (
                f'{preparer.quote_schema(str(index["index_schema"]))}.'
                f'{preparer.quote(_INDEX)}')
            bind.exec_driver_sql(
                f'DROP INDEX CONCURRENTLY IF EXISTS {qualified_name}')
            index = None
        elif not _postgres_shape_matches(index):
            raise RuntimeError(
                f'Existing managed-image index {_INDEX!r} has an unexpected '
                'shape.')
    if index is not None:
        return
    op.create_index(_INDEX,
                    _INDEX_TABLE,
                    list(_INDEX_COLUMNS),
                    postgresql_where=sqlalchemy.text(_INDEX_PREDICATE),
                    postgresql_concurrently=True)


def _create_qualification_mutation_table() -> None:
    op.create_table(
        'container_image_qualification_mutation',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column(
            'owner_profile_revision_id',
            sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_profile_revisions.id'),
            nullable=False),
        sqlalchemy.Column('owner_target', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('owner_target_fingerprint',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('repository_arn', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('lifecycle_proof_id', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('mutation_lease_token', sqlalchemy.Text),
        sqlalchemy.Column('mutation_lease_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.CheckConstraint(
            "id = 'global'",
            name='ck_container_image_qualification_mutation_singleton'),
        sqlalchemy.CheckConstraint(
            "state IN ('DELETING', 'RESTORING')",
            name='ck_container_image_qualification_mutation_state'),
        sqlalchemy.CheckConstraint(
            "(state = 'DELETING' AND mutation_lease_token IS NOT NULL "
            'AND mutation_lease_expires_at IS NOT NULL '
            "AND mutation_lease_expires_at > updated_at) OR "
            "(state = 'RESTORING' AND mutation_lease_token IS NULL "
            'AND mutation_lease_expires_at IS NULL)',
            name='ck_container_image_qualification_mutation_lease'),
        sqlalchemy.CheckConstraint(
            "owner_target <> '' AND owner_target_fingerprint <> '' "
            "AND repository_arn <> '' AND runtime_digest <> '' "
            "AND lifecycle_proof_id <> '' AND updated_at >= 0",
            name='ck_container_image_qualification_mutation_identity'),
    )


def upgrade():
    """Add the catalog mutation barrier and bounded running-canary fence."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    # PostgreSQL forbids concurrent index DDL inside a transaction. Run the
    # potentially long history scan first, so table creation stays atomic and
    # a malformed reserved index cannot leave a partially-created table.
    with op.get_context().autocommit_block():
        _ensure_index(op.get_bind())
    _create_qualification_mutation_table()


def downgrade():
    """No-op for backward compatibility."""
    pass
