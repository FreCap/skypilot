"""Index exact service-version replica ownership.

Revision ID: 023
Revises: 022
Create Date: 2026-07-22

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '023'
down_revision: str | Sequence[str] | None = '022'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = 'replicas_service_version_idx'


def upgrade():
    """Add the exact service-version lookup index if it is absent."""
    bind = op.get_bind()
    existing = {
        index['name'] for index in sa.inspect(bind).get_indexes('replicas')
    }
    if _INDEX_NAME in existing:
        return
    with op.get_context().autocommit_block():
        op.create_index(
            _INDEX_NAME,
            'replicas', ['service_name', 'version'],
            postgresql_concurrently=(bind.dialect.name == 'postgresql'))


def downgrade():
    """No-op for forward-only Serve schema migrations."""
    pass
