"""Add the inert SkyServe resource-action shadow journal.

Revision ID: 032
Revises: 031
Create Date: 2026-08-01

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.serve import resource_action_state_schema
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '032'
down_revision: str | Sequence[str] | None = '031'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SERVICES = 'services'
_REPLICAS = 'replicas'

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
        'launch_shadow_sample_id IS NULL AND down_shadow_sample_id IS NULL)',
    'ck_replicas_resource_action_launch_exclusive': 'launch_action_id IS NULL OR launch_shadow_sample_id IS NULL',
    'ck_replicas_resource_action_down_exclusive': 'down_action_id IS NULL OR down_shadow_sample_id IS NULL',
}

_REPLICA_UNIQUE_INDEXES = {
    'uq_replicas_ra_replica_incarnation': 'replica_incarnation',
    'uq_replicas_ra_sky_cluster_record_uuid': 'sky_cluster_record_uuid',
    'uq_replicas_ra_launch_action_id': 'launch_action_id',
    'uq_replicas_ra_down_action_id': 'down_action_id',
    'uq_replicas_ra_launch_shadow_sample': 'launch_shadow_sample_id',
    'uq_replicas_ra_down_shadow_sample': 'down_shadow_sample_id',
}


def _add_missing_columns(table: str, columns: tuple[sa.Column, ...]) -> None:
    """Add common columns without depending on revision-001 metadata age."""
    existing = {
        column['name']
        for column in sa.inspect(op.get_bind()).get_columns(table)
    }
    for column in columns:
        if column.name not in existing:
            op.add_column(table, column)
            existing.add(column.name)


def _ensure_postgres_common_constraints() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    service_checks = {
        constraint['name']
        for constraint in inspector.get_check_constraints(_SERVICES)
    }
    for name, expression in _SERVICE_CHECKS.items():
        if name not in service_checks:
            op.create_check_constraint(name, _SERVICES, expression)

    inspector = sa.inspect(bind)
    replica_checks = {
        constraint['name']
        for constraint in inspector.get_check_constraints(_REPLICAS)
    }
    for name, expression in _REPLICA_CHECKS.items():
        if name not in replica_checks:
            op.create_check_constraint(name, _REPLICAS, expression)

    inspector = sa.inspect(bind)
    replica_indexes = {
        index['name'] for index in inspector.get_indexes(_REPLICAS)
    }
    for name, column in _REPLICA_UNIQUE_INDEXES.items():
        if name not in replica_indexes:
            op.create_index(name,
                            _REPLICAS, [column],
                            unique=True,
                            postgresql_where=sa.text(f'{column} IS NOT NULL'))


def _ensure_postgres_shadow_tables() -> None:
    bind = op.get_bind()
    resource_action_state_schema.metadata.create_all(bind, checkfirst=True)
    # ``create_all(checkfirst=True)`` skips an existing table and its indexes.
    # Re-run index creation individually so an interrupted first migration can
    # safely converge on the complete catalog.
    for table in (resource_action_state_schema.shadow_samples_table,
                  resource_action_state_schema.shadow_attempts_table):
        for index in table.indexes:
            index.create(bind, checkfirst=True)


def upgrade() -> None:
    """Add inert common metadata and PostgreSQL-only shadow evidence."""
    _add_missing_columns(_SERVICES,
                         resource_action_state_schema.service_columns())
    _add_missing_columns(_REPLICAS,
                         resource_action_state_schema.replica_columns())

    if (op.get_bind().dialect.name
            != db_utils.SQLAlchemyDialect.POSTGRESQL.value):
        return
    _ensure_postgres_common_constraints()
    _ensure_postgres_shadow_tables()


def downgrade() -> None:
    """Retain additive common metadata and shadow evidence on rollback."""
    raise RuntimeError(
        'SkyServe schema 032 is additive and cannot be downgraded. Roll back '
        'the application against the retained schema instead.')
