"""Allow exact reserved-fill absence from a never-started provider call.

Revision ID: 053
Revises: 052
Create Date: 2026-08-20

Serve053 keeps the existing fail-closed Serve047 association guard and widens
one evidence-backed transition: a quiesced RESERVED_FILL launch whose exact
physical replica was proven absent may project when provider I/O never began.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op

revision: str = '053'
down_revision: str | Sequence[str] | None = '052'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ORDINARY_ASSOCIATION_GUARD_FUNCTION = (
    'skyserve042_guard_ordinary_association')


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError(
            'Reserved-fill absence projection is PostgreSQL-only.')


def upgrade() -> None:
    """Keep the SQL guard synchronized with application projection rules."""
    _require_postgresql()
    op.execute(f"""
        DO $migration$
        DECLARE
            definition text;
            source_fragment constant text :=
                'NEW.effect_phase IN (''PROVIDER_IO'', ''SERVICE_JOB_IO'')';
            replacement_fragment constant text :=
                'NEW.effect_phase IN (''NOT_STARTED'', ''PROVIDER_IO'', '
                '''SERVICE_JOB_IO'')';
        BEGIN
            SELECT pg_get_functiondef(
                '{_ORDINARY_ASSOCIATION_GUARD_FUNCTION}()'::regprocedure)
            INTO STRICT definition;
            IF strpos(definition, replacement_fragment) > 0 THEN
                RETURN;
            END IF;
            IF length(definition) -
                    length(replace(definition, source_fragment, '')) <>
                    length(source_fragment) THEN
                RAISE EXCEPTION
                    'Serve053 found an unexpected ordinary-launch guard';
            END IF;
            EXECUTE replace(definition, source_fragment,
                            replacement_fragment);
        END;
        $migration$;
    """)


def downgrade() -> None:
    """Preserve evidence authority across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve053 is forward-only; NOT_STARTED absence evidence may already '
        'have been projected.')
