"""Add diagnostic ordinary-launch handoff history.

Revision ID: 041
Revises: 040
Create Date: 2026-08-11

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

from sky.serve import ordinary_launch_handoff
from sky.utils.db import db_utils

revision: str = '041'
down_revision: str | Sequence[str] | None = '040'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'serve_ordinary_launch_handoff_events'
_IMMUTABILITY_FUNCTION = 'skyserve041_reject_handoff_event_update'
_IMMUTABILITY_TRIGGER = 'skyserve041_handoff_events_immutable'


def upgrade() -> None:
    """Create central handoff telemetry only on PostgreSQL."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    ordinary_launch_handoff.serve_ordinary_launch_handoff_events_table.create(
        bind, checkfirst=True)
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {_IMMUTABILITY_FUNCTION}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            RAISE EXCEPTION
                'SkyServe ordinary-launch handoff events are append-only';
        END;
        $function$
    """)
    op.execute(f'DROP TRIGGER IF EXISTS {_IMMUTABILITY_TRIGGER} ON {_TABLE}')
    op.execute(f"""
        CREATE TRIGGER {_IMMUTABILITY_TRIGGER}
        BEFORE UPDATE OR TRUNCATE ON {_TABLE}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {_IMMUTABILITY_FUNCTION}()
    """)


def downgrade() -> None:
    # Evidence history is safe for an older binary to ignore.  Retain the
    # relation and update fence across application rollback so a later forward
    # deployment does not silently lose or rewrite the measurement window.
    pass
