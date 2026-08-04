"""Add durable API server role-instance heartbeats.

Revision ID: 002
Revises: 001
Create Date: 2026-07-30

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '002'
down_revision: str | Sequence[str] | None = '001'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INSTANCES = 'api_server_instances'


def _require_postgresql() -> None:
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('The central API request store is PostgreSQL-only.')


def upgrade() -> None:
    """Create the role-instance readiness and compatibility table."""
    _require_postgresql()
    op.create_table(
        _INSTANCES,
        sqlalchemy.Column('instance_id',
                          postgresql.UUID(as_uuid=True),
                          primary_key=True),
        sqlalchemy.Column('role', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('pod_name', sqlalchemy.Text),
        sqlalchemy.Column('pod_uid', sqlalchemy.Text),
        sqlalchemy.Column('pod_ip', sqlalchemy.Text),
        sqlalchemy.Column('version', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('started_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('heartbeat_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.Column('draining_at', sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.Column('ready',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()),
        sqlalchemy.Column('health_detail',
                          postgresql.JSONB,
                          nullable=False,
                          server_default=sqlalchemy.text("'{}'::jsonb")),
        sqlalchemy.Column('supported_handlers',
                          postgresql.JSONB,
                          nullable=False,
                          server_default=sqlalchemy.text("'[]'::jsonb")),
        sqlalchemy.Column('supported_payload_versions',
                          postgresql.JSONB,
                          nullable=False,
                          server_default=sqlalchemy.text("'{}'::jsonb")),
        sqlalchemy.CheckConstraint(
            "role IN ('all', 'api', 'executor', 'controller')",
            name='ck_api_server_instances_role'),
    )
    op.create_index('ix_api_server_instances_role_heartbeat', _INSTANCES,
                    ['role', 'heartbeat_at'])
    op.create_index(
        'ix_api_server_instances_ready',
        _INSTANCES, ['role', 'ready'],
        postgresql_where=sqlalchemy.text('ready AND draining_at IS NULL'))


def downgrade() -> None:
    """Drop role instances without touching durable requests."""
    _require_postgresql()
    op.drop_table(_INSTANCES)
