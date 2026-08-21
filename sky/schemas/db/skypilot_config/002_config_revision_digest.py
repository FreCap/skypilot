"""Add exact revision and digest fencing to central configuration.

Revision ID: 002
Revises: 001
Create Date: 2026-08-21

"""
# pylint: disable=invalid-name
import hashlib

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None

_IDENTITY_TRIGGER = 'skypilot_config_require_identity_advance'
_IDENTITY_FUNCTION = 'skypilot_config_require_identity_advance_fn'


def _columns(bind) -> set[str]:
    return {
        column['name'] for column in sa.inspect(bind).get_columns('config_yaml')
    }


def upgrade():
    """Backfill exact config identities before enforcing them on PostgreSQL."""
    bind = op.get_bind()
    columns = _columns(bind)
    if 'revision' not in columns:
        op.add_column('config_yaml',
                      sa.Column('revision', sa.BigInteger(), nullable=True))
    if 'digest' not in columns:
        op.add_column('config_yaml',
                      sa.Column('digest', sa.String(length=64), nullable=True))

    config_yaml = sa.table(
        'config_yaml',
        sa.column('key', sa.Text()),
        sa.column('value', sa.Text()),
        sa.column('revision', sa.BigInteger()),
        sa.column('digest', sa.String(length=64)),
    )
    rows = bind.execute(
        sa.select(config_yaml.c.key, config_yaml.c.value,
                  config_yaml.c.revision, config_yaml.c.digest)).all()
    for row in rows:
        if not isinstance(row.value, str):
            raise RuntimeError(
                f'Cannot fence config row {row.key!r} with a null value.')
        expected_digest = hashlib.sha256(row.value.encode('utf-8')).hexdigest()
        revision_value = row.revision
        if (not isinstance(revision_value, int) or
                isinstance(revision_value, bool) or revision_value < 1):
            revision_value = 1
        if row.revision != revision_value or row.digest != expected_digest:
            bind.execute(
                sa.update(config_yaml).where(
                    config_yaml.c.key == row.key).values(
                        revision=revision_value, digest=expected_digest))

    # Guarded HA is PostgreSQL-only.  Preserve compatibility with historical
    # standalone SQLite databases, whose ALTER COLUMN support is incomplete;
    # fresh SQLite schemas still receive the non-null current metadata shape.
    if bind.dialect.name == 'postgresql':
        op.alter_column('config_yaml',
                        'revision',
                        existing_type=sa.BigInteger(),
                        nullable=False)
        op.alter_column('config_yaml',
                        'digest',
                        existing_type=sa.String(length=64),
                        nullable=False)
        # Reject a pre-D1 writer that updates only ``value`` while old and new
        # pods overlap during a rolling upgrade. Every effective row change
        # must advance the identity exactly once; the application validates the
        # corresponding digest before publishing the row.
        op.execute(f'''
            CREATE OR REPLACE FUNCTION {_IDENTITY_FUNCTION}()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF (NEW.value IS DISTINCT FROM OLD.value
                    OR NEW.revision IS DISTINCT FROM OLD.revision
                    OR NEW.digest IS DISTINCT FROM OLD.digest)
                   AND NEW.revision <> OLD.revision + 1 THEN
                    RAISE EXCEPTION
                        'config_yaml updates must advance revision exactly once';
                END IF;
                RETURN NEW;
            END;
            $$
        ''')
        op.execute(f'''
            CREATE TRIGGER {_IDENTITY_TRIGGER}
            BEFORE UPDATE OF value, revision, digest ON config_yaml
            FOR EACH ROW
            EXECUTE FUNCTION {_IDENTITY_FUNCTION}()
        ''')


def downgrade():
    """Remove config fencing columns."""
    bind = op.get_bind()
    columns = _columns(bind)
    if bind.dialect.name == 'postgresql':
        op.execute(f'DROP TRIGGER IF EXISTS {_IDENTITY_TRIGGER} ON config_yaml')
        op.execute(f'DROP FUNCTION IF EXISTS {_IDENTITY_FUNCTION}()')
    if bind.dialect.name == 'sqlite':
        with op.batch_alter_table('config_yaml') as batch_op:
            if 'digest' in columns:
                batch_op.drop_column('digest')
            if 'revision' in columns:
                batch_op.drop_column('revision')
        return
    if 'digest' in columns:
        op.drop_column('config_yaml', 'digest')
    if 'revision' in columns:
        op.drop_column('config_yaml', 'revision')
