"""Add immutable controller and worker projections to service versions.

Revision ID: 043
Revises: 042
Create Date: 2026-08-12

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

revision: str = '043'
down_revision: str | Sequence[str] | None = '042'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add nullable JSONB-backed projection storage for new versions."""
    projection_type = sa.JSON(none_as_null=True).with_variant(
        postgresql.JSONB(none_as_null=True), 'postgresql')
    with op.get_context().autocommit_block():
        db_utils.add_column_to_table_alembic('version_specs',
                                             'controller_job_projection',
                                             projection_type)
        db_utils.add_column_to_table_alembic('version_specs',
                                             'controller_work_cache',
                                             projection_type)
        db_utils.add_column_to_table_alembic('version_specs',
                                             'worker_placement_projections',
                                             projection_type)
        # This applied revision is immutable. The abandoned broker projection
        # is absent from current metadata and runtime code, but retaining its
        # nullable physical column keeps rolling upgrades safe for older
        # binaries that may still select it.
        db_utils.add_column_to_table_alembic('version_specs', 'storage_broker',
                                             projection_type)


def downgrade() -> None:
    # Older binaries ignore these additive columns. Retaining them preserves
    # immutable version metadata across an application rollback.
    pass
