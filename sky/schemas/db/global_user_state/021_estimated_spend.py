"""Add workload attribution and estimated-spend rollup tables.

Revision ID: 021
Revises: 020
Create Date: 2026-07-10

"""
# pylint: disable=invalid-name
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from sky.global_user_state import Base
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '021'
down_revision: Union[str, Sequence[str], None] = '020'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_USAGE_UPDATED_INDEX = 'ix_cluster_history_usage_updated_at'


def upgrade():
    """Add source metadata and create the materialized rollup tables."""
    with op.get_context().autocommit_block():
        for table_name in ('clusters', 'cluster_history'):
            db_utils.add_column_to_table_alembic(table_name,
                                                 'workload_type',
                                                 sa.Text(),
                                                 server_default=None)
            db_utils.add_column_to_table_alembic(table_name,
                                                 'workload_id',
                                                 sa.Text(),
                                                 server_default=None)
            db_utils.add_column_to_table_alembic(table_name,
                                                 'workload_task_id',
                                                 sa.Integer(),
                                                 server_default=None)
        db_utils.add_column_to_table_alembic('cluster_history',
                                             'usage_updated_at',
                                             sa.Integer(),
                                             server_default='0')
        bind = op.get_bind()
        existing_indexes = {
            index['name']
            for index in sa.inspect(bind).get_indexes('cluster_history')
        }
        if _USAGE_UPDATED_INDEX not in existing_indexes:
            op.create_index(
                _USAGE_UPDATED_INDEX,
                'cluster_history', ['usage_updated_at'],
                postgresql_concurrently=(bind.dialect.name == 'postgresql'))
        db_utils.add_all_tables_to_db_sqlalchemy(Base.metadata, op.get_bind())


def downgrade():
    """No-op for backward compatibility."""
    pass
