"""Add inert SkyServe decision coverage and worker-cohort retention.

Revision ID: 033
Revises: 032
Create Date: 2026-08-01

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.serve import resource_action_state_schema
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '033'
down_revision: str | Sequence[str] | None = '032'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REPLICAS = 'replicas'
_SHADOW_SAMPLES = 'serve_resource_action_shadow_samples'
_SHADOW_ATTEMPTS = 'serve_resource_action_shadow_attempts'
_SHADOW_COVERAGE = 'serve_resource_action_shadow_coverage'

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

_REPLICA_COVERAGE_INDEXES = {
    'uq_replicas_ra_launch_shadow_coverage': 'launch_shadow_coverage_id',
    'uq_replicas_ra_down_shadow_coverage': 'down_shadow_coverage_id',
}


def _add_missing_columns(table: str, columns: tuple[sa.Column, ...]) -> None:
    """Add columns without relying on current-base bootstrap metadata."""
    existing = {
        column['name']
        for column in sa.inspect(op.get_bind()).get_columns(table)
    }
    for column in columns:
        if column.name not in existing:
            op.add_column(table, column)
            existing.add(column.name)


def _assert_revision_032_shadow_is_empty() -> None:
    """Fail closed rather than invent coverage for preexisting evidence."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table in (_SHADOW_SAMPLES, _SHADOW_ATTEMPTS):
        if not inspector.has_table(table):
            raise RuntimeError(
                f'SkyServe schema 033 requires revision-032 table {table!r}.')
        quoted = bind.dialect.identifier_preparer.quote(table)
        if bind.execute(
                sa.text(f'SELECT EXISTS (SELECT 1 FROM {quoted} LIMIT 1)')
        ).scalar_one():
            raise RuntimeError(
                'SkyServe schema 033 cannot migrate nonempty revision-032 '
                f'shadow evidence ({table}); a reviewed evidence backfill is '
                'required.')


def _ensure_new_postgres_tables() -> None:
    bind = op.get_bind()
    tables = (
        resource_action_state_schema.WORKER_COHORTS,
        resource_action_state_schema.WORKER_COHORT_REFS,
        resource_action_state_schema.SHADOW_COVERAGE,
        resource_action_state_schema.SHADOW_COVERAGE_ATTEMPTS,
    )
    for table in tables:
        table.create(bind, checkfirst=True)
        # Table.create(checkfirst=True) skips indexes when adopting a table.
        for index in table.indexes:
            index.create(bind, checkfirst=True)


def _ensure_effect_trace_columns_and_check() -> None:
    _add_missing_columns(
        _SHADOW_ATTEMPTS,
        resource_action_state_schema.shadow_attempt_effect_trace_columns())
    check_name = 'ck_serve_ra_shadow_attempts_effect_trace'
    checks = {
        constraint['name'] for constraint in sa.inspect(
            op.get_bind()).get_check_constraints(_SHADOW_ATTEMPTS)
    }
    if check_name not in checks:
        op.create_check_constraint(
            check_name, _SHADOW_ATTEMPTS, '((legacy_effect_trace IS NULL AND '
            'legacy_effect_trace_sha256 IS NULL) OR '
            '(legacy_effect_trace IS NOT NULL AND '
            'legacy_effect_trace_sha256 IS NOT NULL AND '
            "jsonb_typeof(legacy_effect_trace) IS NOT DISTINCT FROM 'object' "
            'AND octet_length(CAST(legacy_effect_trace AS TEXT)) <= 65536 '
            "AND legacy_effect_trace_sha256 ~ '^[0-9a-f]{64}$'))")


def _ensure_shadow_parent_coverage_fk() -> None:
    name = 'fk_serve_ra_shadow_samples_coverage'
    foreign_keys = {
        foreign_key['name'] for foreign_key in sa.inspect(
            op.get_bind()).get_foreign_keys(_SHADOW_SAMPLES)
    }
    if name not in foreign_keys:
        op.create_foreign_key(name,
                              _SHADOW_SAMPLES,
                              _SHADOW_COVERAGE, ['would_be_action_id'],
                              ['decision_id'],
                              ondelete='RESTRICT')


def _ensure_postgres_replica_contract() -> None:
    bind = op.get_bind()
    existing_checks = {
        constraint['name']
        for constraint in sa.inspect(bind).get_check_constraints(_REPLICAS)
    }
    # Three revision-032 checks retain their names but gain coverage semantics.
    for name in ('ck_replicas_resource_action_links',
                 'ck_replicas_resource_action_launch_exclusive',
                 'ck_replicas_resource_action_down_exclusive'):
        if name in existing_checks:
            op.drop_constraint(name, _REPLICAS, type_='check')
            existing_checks.remove(name)
    for name, expression in _REPLICA_CHECKS.items():
        if name not in existing_checks:
            op.create_check_constraint(name, _REPLICAS, expression)

    existing_indexes = {
        index['name'] for index in sa.inspect(bind).get_indexes(_REPLICAS)
    }
    for name, column in _REPLICA_COVERAGE_INDEXES.items():
        if name not in existing_indexes:
            op.create_index(name,
                            _REPLICAS, [column],
                            unique=True,
                            postgresql_where=sa.text(f'{column} IS NOT NULL'))


def upgrade() -> None:
    """Add inert coverage links and PostgreSQL-only retained evidence."""
    bind = op.get_bind()
    is_postgres = (
        bind.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value)
    if is_postgres:
        # This is deliberately the first schema-033 operation. PostgreSQL DDL
        # is transactional, but no catalog mutation should precede the audit
        # that makes the no-backfill decision safe.
        _assert_revision_032_shadow_is_empty()

    _add_missing_columns(
        _REPLICAS, resource_action_state_schema.replica_coverage_columns())
    if not is_postgres:
        return

    _ensure_new_postgres_tables()
    _ensure_effect_trace_columns_and_check()
    _ensure_shadow_parent_coverage_fk()
    _ensure_postgres_replica_contract()


def downgrade() -> None:
    """Retain additive decision/worker evidence on application rollback."""
    raise RuntimeError(
        'SkyServe schema 033 is additive and cannot be downgraded. Roll back '
        'the application against the retained schema instead.')
