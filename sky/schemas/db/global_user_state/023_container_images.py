"""Add workspace-scoped managed container image state.

Revision ID: 023
Revises: 022
Create Date: 2026-07-13

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

from sky.global_user_state import Base
from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '023'
down_revision: str | Sequence[str] | None = '022'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CONTAINER_IMAGE_TABLE_NAMES = (
    'container_image_catalog',
    'container_image_workspace_catalogs',
    'container_image_profile_revisions',
    'container_images',
    'container_image_releases',
    'container_image_sources',
    'container_image_locations',
    'container_image_references',
)


def upgrade():
    """Create the image catalog and per-target preparation tables."""
    with op.get_context().autocommit_block():
        db_utils.add_all_tables_to_db_sqlalchemy(
            Base.metadata,
            op.get_bind(),
            reconcile_indexes_for=_CONTAINER_IMAGE_TABLE_NAMES)


def downgrade():
    """No-op for backward compatibility."""
    pass
