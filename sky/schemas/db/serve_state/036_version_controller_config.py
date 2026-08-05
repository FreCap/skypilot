"""Add sanitized controller configuration to each Serve version.

Revision ID: 036
Revises: 035
Create Date: 2026-08-05

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '036'
down_revision: str | Sequence[str] | None = '035'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add nullable snapshots without rewriting historical version rows."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table('version_specs'):
        raise RuntimeError(
            'Cannot add versioned controller configuration because the '
            'version_specs table is missing.')
    columns = {
        str(column['name']) for column in inspector.get_columns('version_specs')
    }
    additions = (
        sa.Column('controller_config', sa.LargeBinary(), nullable=True),
        sa.Column('controller_config_digest', sa.Text(), nullable=True),
        sa.Column('controller_config_snapshot_id', sa.Text(), nullable=True),
        sa.Column('controller_applied_at', sa.Float(), nullable=True),
    )
    for column in additions:
        if column.name not in columns:
            op.add_column('version_specs', column)


def downgrade() -> None:
    """Retain versioned recovery state during application rollback."""
    raise RuntimeError(
        'SkyServe schema 036 is additive and cannot be downgraded. Versioned '
        'controller configuration and applied receipts may be the only safe '
        'recovery inputs for an existing service version.')
