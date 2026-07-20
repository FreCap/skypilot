"""Index managed-job API token associations for batch cleanup.

Revision ID: 024
Revises: 023
Create Date: 2026-07-20

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '024'
down_revision: str | Sequence[str] | None = '023'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = 'ix_api_access_tokens_token_id'


def upgrade():
    """Add the reverse-lookup index if an earlier schema lacks it."""
    bind = op.get_bind()
    existing_indexes = {
        index['name']
        for index in sa.inspect(bind).get_indexes('api_access_tokens')
    }
    if _INDEX_NAME in existing_indexes:
        return
    with op.get_context().autocommit_block():
        op.create_index(
            _INDEX_NAME,
            'api_access_tokens', ['token_id'],
            postgresql_concurrently=(bind.dialect.name == 'postgresql'))


def downgrade():
    """No-op for forward-only managed-jobs schema migrations."""
    pass
