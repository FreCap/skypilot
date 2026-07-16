"""Backfill the elected SkyServe version pointer.

Revision ID: 014
Revises: 013
Create Date: 2026-07-16

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy

# revision identifiers, used by Alembic.
revision: str = '014'
down_revision: str | Sequence[str] | None = '013'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade():
    """Point every existing service at its latest committed version."""
    services = sqlalchemy.table('services', sqlalchemy.column('name'),
                                sqlalchemy.column('current_version'))
    versions = sqlalchemy.table('version_specs',
                                sqlalchemy.column('service_name'),
                                sqlalchemy.column('version'),
                                sqlalchemy.column('yaml_content'))
    latest_committed = (sqlalchemy.select(
        sqlalchemy.func.max(versions.c.version)).where(
            versions.c.service_name == services.c.name,
            versions.c.yaml_content.is_not(None)).scalar_subquery())
    op.execute(
        sqlalchemy.update(services).where(sqlalchemy.exists().where(
            versions.c.service_name == services.c.name,
            versions.c.yaml_content.is_not(None))).values(
                current_version=latest_committed))


def downgrade():
    pass
