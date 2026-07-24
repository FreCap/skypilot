"""Add the immutable SkyServe placement catalog to service versions.

Revision ID: 028
Revises: 027
Create Date: 2026-07-24

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

revision: str = '028'
down_revision: str | Sequence[str] | None = '027'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add nullable catalog storage for new versions and lazy backfill."""
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic(
            'version_specs', 'placement_catalog',
            sa.JSON(none_as_null=True).with_variant(
                postgresql.JSONB(none_as_null=True), 'postgresql'))


def downgrade() -> None:
    # Additive operational metadata is safe for older binaries to ignore.
    # Keep it during rollback so a later forward deployment can reuse it.
    pass
