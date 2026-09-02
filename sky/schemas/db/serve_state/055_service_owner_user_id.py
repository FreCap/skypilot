"""Add immutable service-owner tenant authority.

Revision ID: 055
Revises: 054
Create Date: 2026-08-20

Existing rows remain NULL until their elected controller attests the frozen
tenant under its exact incarnation fence. New rows write the owner with the
initial service/version transaction.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '055'
down_revision: str | Sequence[str] | None = '054'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FUNCTION = 'skyserve055_guard_service_owner'
_TRIGGER = 'skyserve055_service_owner_guard'
_OWNER_USER_FK = 'serve055_service_owner_user_fk'


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('Serve service-owner authority is PostgreSQL-only.')


def upgrade() -> None:
    """Install nullable, one-shot attestable tenant authority."""
    _require_postgresql()
    inspector = sa.inspect(op.get_bind())
    if (not inspector.has_table('users') or 'id' not in {
            str(column['name']) for column in inspector.get_columns('users')
    }):
        raise RuntimeError(
            'Serve055 requires the global user-state users(id) schema. '
            'Migrate global user state before the Serve database.')
    columns = {
        str(column['name']) for column in inspector.get_columns('services')
    }
    if 'owner_user_id' not in columns:
        op.add_column('services',
                      sa.Column('owner_user_id', sa.Text(), nullable=True))
    if 'owner_user_name' not in columns:
        op.add_column('services',
                      sa.Column('owner_user_name', sa.Text(), nullable=True))
    if 'serve055_owner_user_id_nonempty' not in {
            str(constraint['name'])
            for constraint in inspector.get_check_constraints('services')
            if constraint.get('name') is not None
    }:
        op.create_check_constraint(
            'serve055_owner_user_id_nonempty', 'services',
            '(owner_user_id IS NULL) = (owner_user_name IS NULL) AND '
            '(owner_user_id IS NULL OR (length(owner_user_id) > 0 AND '
            'length(owner_user_name) > 0))')
    if _OWNER_USER_FK not in {
            str(constraint['name'])
            for constraint in inspector.get_foreign_keys('services')
            if constraint.get('name') is not None
    }:
        op.create_foreign_key(_OWNER_USER_FK,
                              'services',
                              'users', ['owner_user_id'], ['id'],
                              ondelete='RESTRICT')
    op.execute(f'''
        CREATE FUNCTION {_FUNCTION}() RETURNS trigger AS $$
        BEGIN
            IF OLD.owner_user_id IS NOT NULL AND
               (NEW.owner_user_id IS DISTINCT FROM OLD.owner_user_id OR
                NEW.owner_user_name IS DISTINCT FROM OLD.owner_user_name) THEN
                RAISE EXCEPTION 'service owner identity is immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        CREATE TRIGGER {_TRIGGER}
        BEFORE UPDATE OF owner_user_id, owner_user_name ON services
        FOR EACH ROW EXECUTE FUNCTION {_FUNCTION}();
    ''')


def downgrade() -> None:
    """Preserve owner authority across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve055 is forward-only; service launch tenant authority is durable.')
