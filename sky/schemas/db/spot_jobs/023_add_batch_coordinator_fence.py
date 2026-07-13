"""Add durable Batch coordinator and attempt ownership fences.

Revision ID: 023
Revises: 022
Create Date: 2026-07-10

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.jobs.state import Base
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '023'
down_revision: str | Sequence[str] | None = '022'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Add coordinator, attempt, and external worker ownership records."""
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic('job_info',
                                             'batch_coordinator_token',
                                             sa.Text(),
                                             server_default=None)
        db_utils.add_column_to_table_alembic('batch_state',
                                             'attempt_owner_token',
                                             sa.Text(),
                                             server_default=None)
        db_utils.add_table_to_db_sqlalchemy(Base.metadata, op.get_bind(),
                                            'batch_worker')


def downgrade():
    """No-op for forward-only Batch schema migrations."""
    pass
