"""Add durable ordinary SkyServe launch binding capability.

Revision ID: 009
Revises: 008
Create Date: 2026-08-11

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

revision: str = '009'
down_revision: str | Sequence[str] | None = '008'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REQUESTS = 'api_requests'
_INSTANCES = 'api_server_instances'
_RETENTION_PINS = 'api_request_retention_pins'
_ASSOCIATION_INDEX = 'uq_api_requests_ordinary_launch_association'
_HANDLER_CONSTRAINT = 'ck_api_requests_ordinary_launch_handler'
_TERMINAL_CAUSE_CONSTRAINT = 'ck_api_requests_terminal_cause'
_PIN_KIND_CONSTRAINT = 'ck_api_request_retention_pins_kind'
_PIN_REQUEST_INDEX = 'ix_api_request_retention_pins_request'
_BOUND_HANDLER = 'sky.server.requests.ordinary_launch:launch'
_TERMINAL_CAUSES = (
    "'handler_succeeded', 'handler_failed', 'dispatcher_submit_failed', "
    "'explicit_cancel', 'coroutine_disconnected', "
    "'graceful_shutdown_retry', 'compatibility_restart', "
    "'controller_leadership_lost', 'execution_lease_expired', "
    "'precondition_failed', 'controller_reservation_conflict'")


def _require_postgresql() -> None:
    # Keep historical migrations independent from mutable runtime modules.
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('The central API request store is PostgreSQL-only.')


def upgrade() -> None:
    """Add fail-closed fleet and request-correlation evidence."""
    _require_postgresql()
    # False is intentionally retained as the server default.  API008 writers
    # remain insert-compatible during a rolling deployment and advertise no
    # authority to admit a bound request.
    op.add_column(
        _INSTANCES,
        sqlalchemy.Column('ordinary_launch_binding_capable',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()))
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('ordinary_launch_association_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=True))
    # Existing API008 terminal rows have no closed cause and remain NULL.  New
    # writers persist the cause in their terminal transaction; reducers treat
    # a NULL on a bound request as ambiguous instead of inventing evidence.
    op.add_column(
        _REQUESTS,
        sqlalchemy.Column('terminal_cause', sqlalchemy.Text, nullable=True))
    op.create_check_constraint(
        _TERMINAL_CAUSE_CONSTRAINT, _REQUESTS,
        "terminal_cause IS NULL OR (status IN ('SUCCEEDED', 'FAILED', "
        f"'CANCELLED') AND terminal_cause IN ({_TERMINAL_CAUSES}))")
    # A correlated request must resolve through the distinct fail-closed
    # handler, and that handler can never exist without exact correlation.
    op.create_check_constraint(
        _HANDLER_CONSTRAINT, _REQUESTS,
        '(ordinary_launch_association_id IS NULL) = '
        f"(handler_name <> '{_BOUND_HANDLER}')")
    op.create_index(_ASSOCIATION_INDEX,
                    _REQUESTS, ['ordinary_launch_association_id'],
                    unique=True,
                    postgresql_where=sqlalchemy.text(
                        'ordinary_launch_association_id IS NOT NULL'))
    # Pins are active-only: projection deletes the exact row.  RESTRICT is a
    # database-level backstop in addition to the candidate and final-delete
    # NOT EXISTS predicates in the request store.
    op.create_table(
        _RETENTION_PINS,
        sqlalchemy.Column('pin_kind', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('pin_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=False),
        sqlalchemy.Column('request_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('created_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.PrimaryKeyConstraint('pin_kind',
                                        'pin_id',
                                        name='pk_api_request_retention_pins'),
        sqlalchemy.ForeignKeyConstraint(
            ['request_id'], [f'{_REQUESTS}.request_id'],
            name='fk_api_request_retention_pins_request',
            ondelete='RESTRICT'),
        sqlalchemy.CheckConstraint('char_length(pin_kind) BETWEEN 1 AND 128',
                                   name=_PIN_KIND_CONSTRAINT),
    )
    op.create_index(_PIN_REQUEST_INDEX, _RETENTION_PINS, ['request_id'])


def downgrade() -> None:
    """Retain binding evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'API request schema 009 is additive and cannot be downgraded. Drain '
        'every bound request to terminal projection, then roll back the '
        'application against the retained schema.')
