"""Index exact managed-job task identities.

Revision ID: 025
Revises: 024
Create Date: 2026-07-22

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '025'
down_revision: str | Sequence[str] | None = '024'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = 'ix_spot_job_task'


def upgrade():
    """Add the exact job-task lookup index if it is absent."""
    bind = op.get_bind()
    existing = {index['name'] for index in sa.inspect(bind).get_indexes('spot')}
    if _INDEX_NAME in existing:
        return
    with op.get_context().autocommit_block():
        op.create_index(
            _INDEX_NAME,
            'spot', ['spot_job_id', 'task_id'],
            postgresql_concurrently=(bind.dialect.name == 'postgresql'))


def downgrade():
    """No-op for forward-only managed-jobs schema migrations."""
    pass
