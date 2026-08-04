"""Add durable outer-controller ownership to managed jobs.

Revision ID: 026
Revises: 025
Create Date: 2026-07-29

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

import sqlalchemy as sa

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '026'
down_revision: str | Sequence[str] | None = '025'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Add the instance and generation that own each pod-local PID."""
    db_utils.add_column_to_table_alembic('job_info',
                                         'controller_instance_id',
                                         sa.Text(),
                                         server_default=None)
    db_utils.add_column_to_table_alembic('job_info',
                                         'controller_generation',
                                         sa.BigInteger(),
                                         server_default=None)


def downgrade():
    """No-op for forward-only managed-jobs schema migrations."""
    pass
