"""Add retained provider-I/O and progress evidence to action attempts.

Revision ID: 006
Revises: 005
Create Date: 2026-08-01

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '006'
down_revision: str | Sequence[str] | None = '005'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ATTEMPTS = 'api_resource_action_attempts'


def _require_postgresql() -> None:
    if op.get_bind(
    ).dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError('Resource-action progress is PostgreSQL-only.')


def upgrade() -> None:
    """Add the API006 progress snapshot without inventing old evidence."""
    _require_postgresql()
    bind = op.get_bind()
    # Serialize the fail-closed inventory check with API005 writers.  Alembic's
    # transaction retains this lock through every following DDL statement, so
    # an attempt cannot appear between the count and the additive columns.
    bind.execute(
        sqlalchemy.text(f'LOCK TABLE {_ATTEMPTS} IN ACCESS EXCLUSIVE MODE'))
    attempt_count = bind.execute(
        sqlalchemy.text(f'SELECT COUNT(*) FROM {_ATTEMPTS}')).scalar_one()
    if attempt_count != 0:
        raise RuntimeError(
            'API request schema 006 cannot reconstruct the provider-I/O '
            'watermark for preexisting resource-action attempts. Apply a '
            'separately reviewed evidence backfill first.')

    op.add_column(
        _ATTEMPTS,
        sqlalchemy.Column('provider_io_boundary',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='NOT_STARTED'))
    op.add_column(
        _ATTEMPTS,
        sqlalchemy.Column('provider_progress',
                          postgresql.JSONB(none_as_null=True),
                          nullable=True))
    op.add_column(
        _ATTEMPTS,
        sqlalchemy.Column('provider_progress_sha256',
                          sqlalchemy.Text,
                          nullable=True))
    op.add_column(
        _ATTEMPTS,
        sqlalchemy.Column('provider_progress_revision',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'))
    op.create_check_constraint(
        'ck_api_resource_action_attempts_provider_io_boundary', _ATTEMPTS,
        "provider_io_boundary IN ('NOT_STARTED', 'INTENT_COMMITTED', "
        "'SUBMITTED_OR_AMBIGUOUS')")
    op.create_check_constraint(
        'ck_api_resource_action_attempts_boundary_alignment', _ATTEMPTS,
        "mutation_boundary = 'SETTLED' OR "
        'mutation_boundary = provider_io_boundary')
    op.create_check_constraint(
        'ck_api_resource_action_attempts_provider_id_io_state', _ATTEMPTS,
        "provider_io_boundary = 'SUBMITTED_OR_AMBIGUOUS' OR "
        'provider_operation_id IS NULL')
    op.create_check_constraint(
        'ck_api_resource_action_attempts_progress_shape', _ATTEMPTS,
        '((provider_progress IS NULL AND '
        'provider_progress_sha256 IS NULL AND '
        'provider_progress_revision = 0) OR '
        '(provider_progress IS NOT NULL AND '
        'provider_progress_sha256 IS NOT NULL AND '
        'provider_progress_revision > 0 AND '
        "jsonb_typeof(provider_progress) IS NOT DISTINCT FROM 'object' AND "
        'octet_length(CAST(provider_progress AS TEXT)) <= 65536 AND '
        "provider_progress_sha256 ~ '^[0-9a-f]{64}$'))")


def downgrade() -> None:
    """Retain additive provider progress across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'API request schema 006 is additive and cannot be downgraded. Roll '
        'back the application against the retained schema instead.')
