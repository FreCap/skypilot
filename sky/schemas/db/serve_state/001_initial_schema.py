"""Initial schema for sky serve state database with backwards compatibility

Revision ID: 001
Revises:
Create Date: 2024-01-01 12:00:00.000000

"""
# pylint: disable=invalid-name,protected-access
import json

from alembic import op
import sqlalchemy as sa

from sky.serve import constants
from sky.serve import resource_action_m4_state_schema
from sky.serve.serve_state import Base
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None

_SERVE038_BOOTSTRAP_FACTORIES = {
    'services': resource_action_m4_state_schema.service_candidate_columns,
    'version_specs':
        resource_action_m4_state_schema.version_spec_identity_columns,
    'replicas': resource_action_m4_state_schema.replica_spec_identity_columns,
}
_POST_REVISION_001_COLUMNS = {
    'services': frozenset({
        'controller_incarnation',
        'controller_owner_epoch',
        'ordinary_launch_binding_capable',
        'ordinary_launch_binding_mode',
        'ordinary_launch_binding_epoch',
        'non_pool_launch_binding_capable',
        'non_pool_launch_controller_incarnation',
        'non_pool_launch_binding_protocol_version',
        'non_pool_launch_capability_profile_set_digest',
        'non_pool_launch_capability_cohort_epoch',
        'non_pool_launch_receipt_protocol_version',
        'route_source_mode',
        'route_source_epoch',
        'route_projection_capable',
        'route_projection_controller_incarnation',
        'route_projection_protocol_version',
    }),
    'replicas': frozenset({'ordinary_launch_association_id'}),
}
_POST_REVISION_001_CHECKS = {
    'services': frozenset({
        'serve049_route_source_mode_ck',
        'serve049_route_source_epoch_ck',
        'serve049_route_capability_shape_ck',
        'serve049_route_projected_capability_ck',
    }),
}


def _initial_metadata(bind: sa.engine.Connection) -> sa.MetaData:
    """Project the exact revision-001 catalog for the target dialect."""
    metadata = sa.MetaData()
    for table in Base.metadata.tables.values():
        table.to_metadata(metadata)
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        for table_name, factory in _SERVE038_BOOTSTRAP_FACTORIES.items():
            table = metadata.tables[table_name]
            for expected in factory():
                column = table.c.get(expected.name)
                if column is not None:
                    # These additive fields intentionally own no Base
                    # constraint, index, or FK. Removing them from this
                    # private clone leaves runtime Base metadata complete
                    # without leaking 038 into a fresh non-PostgreSQL
                    # historical schema.
                    table._columns.remove(column)
    # Later forward-only migrations own these columns. Strip them from the
    # revision-001 catalog on every dialect so a fresh PostgreSQL upgrade gets
    # exact migration defaults/constraints and SQLite stays at Serve037.
    for table_name, column_names in _POST_REVISION_001_COLUMNS.items():
        table = metadata.tables[table_name]
        future_checks = _POST_REVISION_001_CHECKS.get(table_name, frozenset())
        for constraint in tuple(table.constraints):
            if constraint.name in future_checks:
                table.constraints.remove(constraint)
        for column_name in column_names:
            column = table.c.get(column_name)
            if column is not None:
                table._columns.remove(column)
    return metadata


def _install_postgres_serve038_bootstrap_columns(
        bind: sa.engine.Connection) -> None:
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    inspector = sa.inspect(bind)
    for table, factory in _SERVE038_BOOTSTRAP_FACTORIES.items():
        existing = {
            str(column['name']) for column in inspector.get_columns(table)
        }
        for column in factory():
            if column.name not in existing:
                op.add_column(table, column)
                existing.add(column.name)


def upgrade():
    """Create initial schema and add all backwards compatibility columns"""
    with op.get_context().autocommit_block():
        # Create all tables with their current schema
        bind = op.get_bind()
        db_utils.add_all_tables_to_db_sqlalchemy(_initial_metadata(bind), bind)
        _install_postgres_serve038_bootstrap_columns(bind)

        # Add backwards compatibility columns using helper function that matches
        # original add_column_to_table_sqlalchemy behavior exactly
        db_utils.add_column_to_table_alembic('services',
                                             'requested_resources_str',
                                             sa.Text())
        db_utils.add_column_to_table_alembic(
            'services',
            'current_version',
            sa.Integer(),
            server_default=f'{constants.INITIAL_VERSION}')
        db_utils.add_column_to_table_alembic('services',
                                             'active_versions',
                                             sa.Text(),
                                             server_default=json.dumps([]))
        db_utils.add_column_to_table_alembic('services',
                                             'load_balancing_policy', sa.Text())
        db_utils.add_column_to_table_alembic('services',
                                             'tls_encrypted',
                                             sa.Integer(),
                                             server_default='0')
        db_utils.add_column_to_table_alembic('services',
                                             'pool',
                                             sa.Integer(),
                                             server_default='0')
        db_utils.add_column_to_table_alembic(
            'services',
            'controller_pid',
            sa.Integer(),
            value_to_replace_existing_entries=-1)
        db_utils.add_column_to_table_alembic('services', 'hash', sa.Text())
        db_utils.add_column_to_table_alembic('services', 'entrypoint',
                                             sa.Text())


def downgrade():
    """Drop all tables"""
    Base.metadata.drop_all(bind=op.get_bind())
