"""Persist SkyServe placement benches and rebalance stabilization.

Revision ID: 029
Revises: 028
Create Date: 2026-07-24

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

revision: str = '029'
down_revision: str | Sequence[str] | None = '028'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _json_column() -> sa.JSON:
    return sa.JSON(none_as_null=True).with_variant(
        postgresql.JSONB(none_as_null=True), 'postgresql')


def upgrade() -> None:
    """Add nullable, owner-fenced controller policy state."""
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic('services', 'spot_placement_state',
                                             _json_column())
        db_utils.add_column_to_table_alembic('services', 'cost_rebalance_state',
                                             _json_column())


def downgrade() -> None:
    # Additive operational evidence is safe for old binaries to ignore. Keep
    # it so a later forward deployment does not restart either cooldown.
    pass
