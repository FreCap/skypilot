"""Admit the dedicated resource-action authority executor role.

Revision ID: 007
Revises: 006
Create Date: 2026-08-02

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '007'
down_revision: str | Sequence[str] | None = '006'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INSTANCES = 'api_server_instances'
_ROLE_CONSTRAINT = 'ck_api_server_instances_role'
_OLD_ROLE_CHECK = "role IN ('all', 'api', 'executor', 'controller')"
_NEW_ROLE_CHECK = (
    "role IN ('all', 'api', 'executor', 'controller', 'authority-worker')")


def _require_postgresql() -> None:
    if op.get_bind(
    ).dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('The central API request store is PostgreSQL-only.')


def upgrade() -> None:
    """Widen only the durable server-instance role check."""
    _require_postgresql()
    op.drop_constraint(_ROLE_CONSTRAINT, _INSTANCES, type_='check')
    op.create_check_constraint(_ROLE_CONSTRAINT, _INSTANCES, _NEW_ROLE_CHECK)


def downgrade() -> None:
    """Restore API006 only when no authority instance row remains."""
    _require_postgresql()
    bind = op.get_bind()
    bind.execute(
        sqlalchemy.text(f'LOCK TABLE {_INSTANCES} IN ACCESS EXCLUSIVE MODE'))
    authority_rows = bind.execute(
        sqlalchemy.text(f"SELECT COUNT(*) FROM {_INSTANCES} "
                        "WHERE role = 'authority-worker'")).scalar_one()
    if authority_rows:
        raise RuntimeError(
            'API request schema 007 cannot downgrade while authority-worker '
            'instance rows exist.')
    op.drop_constraint(_ROLE_CONSTRAINT, _INSTANCES, type_='check')
    op.create_check_constraint(_ROLE_CONSTRAINT, _INSTANCES, _OLD_ROLE_CHECK)
