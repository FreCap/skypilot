"""Add runtime-owned managed-job controller slot fencing.

Revision ID: 028
Revises: 027
Create Date: 2026-08-13

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

from sky.utils.db import db_utils

revision: str = '028'
down_revision: str | Sequence[str] | None = '027'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Add the fixed slot and disposable attempt that own each job."""
    db_utils.add_column_to_table_alembic('job_info',
                                         'controller_slot_id',
                                         sa.Integer(),
                                         server_default=None)
    db_utils.add_column_to_table_alembic('job_info',
                                         'controller_slot_attempt',
                                         sa.Text(),
                                         server_default=None)
    bind = op.get_bind()
    columns = {
        column['name']: column
        for column in sa.inspect(bind).get_columns('job_info')
    }
    quiescing_column = columns.get('controller_slot_quiescing')
    if quiescing_column is None:
        # This flag is an admission fence, so NULL must never become a third
        # state.  Add it with its final invariant in one DDL statement; the
        # server default backfills existing 027 rows on both SQLite and
        # PostgreSQL.
        op.add_column(
            'job_info',
            sa.Column('controller_slot_quiescing',
                      sa.Boolean(),
                      nullable=False,
                      server_default=sa.false()))
    elif quiescing_column['nullable']:
        # A partially applied or development-only nullable shape cannot be
        # adopted safely because runtime predicates deliberately match only
        # explicit false.  Fail closed instead of silently stamping 028.
        raise RuntimeError('controller_slot_quiescing has unexpected nullable '
                           'shape')


def downgrade():
    """No-op for forward-only managed-jobs schema migrations."""
    pass
