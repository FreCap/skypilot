"""Allow exact AWS Spot paid cleanup after explicit cancellation.

Revision ID: 065
Revises: 064
Create Date: 2026-08-30

Serve063 admitted the canonical version-2 account-scoped AWS Spot pool and
provider-census receipt into the association constraints.  Its transition
guards, however, retained Serve060's GCP-only explicit-cancellation arm.  A
quiescent AWS launch with exact post-quiescence provider ``ABSENT`` evidence
could therefore not release its replica pointer.  Serve065 widens only those
three terminal guard fragments to the already-constrained AWS Spot shape.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '065'
down_revision: str | Sequence[str] | None = '064'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ASSOCIATION_GUARD_FUNCTION = 'skyserve042_guard_ordinary_association'
_REPLICA_GUARD_FUNCTION = 'skyserve042_guard_replica_binding'

_ASSOCIATION_OLD_TERMINAL_SOURCE = (
    "((OLD.terminal_status = 'FAILED' AND\n"
    "                  OLD.terminal_cause = 'handler_failed') OR\n"
    "                 (OLD.terminal_status = 'CANCELLED' AND\n"
    "                  OLD.terminal_cause = 'explicit_cancel' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'gcp' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'1' AND\n"
    "                  OLD.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true')) AND\n")
_ASSOCIATION_OLD_TERMINAL_REPLACEMENT = (
    "((OLD.terminal_status = 'FAILED' AND\n"
    "                  OLD.terminal_cause = 'handler_failed') OR\n"
    "                 (OLD.terminal_status = 'CANCELLED' AND\n"
    "                  OLD.terminal_cause = 'explicit_cancel' AND\n"
    "                  ((OLD.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'gcp' AND\n"
    "                    OLD.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'1' AND\n"
    "                    OLD.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true') OR\n"
    "                   (OLD.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'aws' AND\n"
    "                    OLD.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'2' AND\n"
    "                    OLD.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true' AND\n"
    "                    OLD.paid_capacity_pool_key::jsonb -> "
    "'provider_identity' ->> 'aws_account_id' ~ "
    "'^[0-9]{12}$')))) AND\n")

_ASSOCIATION_NEW_TERMINAL_SOURCE = (
    "((NEW.terminal_status = 'FAILED' AND\n"
    "                  NEW.terminal_cause = 'handler_failed') OR\n"
    "                 (NEW.terminal_status = 'CANCELLED' AND\n"
    "                  NEW.terminal_cause = 'explicit_cancel' AND\n"
    "                  NEW.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'gcp' AND\n"
    "                  NEW.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'1' AND\n"
    "                  NEW.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true')))")
_ASSOCIATION_NEW_TERMINAL_REPLACEMENT = (
    "((NEW.terminal_status = 'FAILED' AND\n"
    "                  NEW.terminal_cause = 'handler_failed') OR\n"
    "                 (NEW.terminal_status = 'CANCELLED' AND\n"
    "                  NEW.terminal_cause = 'explicit_cancel' AND\n"
    "                  ((NEW.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'gcp' AND\n"
    "                    NEW.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'1' AND\n"
    "                    NEW.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true') OR\n"
    "                   (NEW.paid_capacity_pool_key::jsonb ->> 'cloud' = "
    "'aws' AND\n"
    "                    NEW.paid_capacity_pool_key::jsonb ->> 'version' = "
    "'2' AND\n"
    "                    NEW.paid_capacity_pool_key::jsonb ->> 'use_spot' = "
    "'true' AND\n"
    "                    NEW.paid_capacity_pool_key::jsonb -> "
    "'provider_identity' ->> 'aws_account_id' ~ "
    "'^[0-9]{12}$')))))")

_REPLICA_TERMINAL_SOURCE = (
    "((association.terminal_status = 'FAILED' AND\n"
    "                      association.terminal_cause = 'handler_failed') OR\n"
    "                     (association.terminal_status = 'CANCELLED' AND\n"
    "                      association.terminal_cause = 'explicit_cancel' "
    "AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'cloud' = 'gcp' AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'version' = '1' AND\n"
    "                      association.paid_capacity_pool_key::jsonb ->> "
    "'use_spot' = 'true')) AND\n")
_REPLICA_TERMINAL_REPLACEMENT = (
    "((association.terminal_status = 'FAILED' AND\n"
    "                      association.terminal_cause = 'handler_failed') OR\n"
    "                     (association.terminal_status = 'CANCELLED' AND\n"
    "                      association.terminal_cause = 'explicit_cancel' "
    "AND\n"
    "                      ((association.paid_capacity_pool_key::jsonb ->> "
    "'cloud' = 'gcp' AND\n"
    "                        association.paid_capacity_pool_key::jsonb ->> "
    "'version' = '1' AND\n"
    "                        association.paid_capacity_pool_key::jsonb ->> "
    "'use_spot' = 'true') OR\n"
    "                       (association.paid_capacity_pool_key::jsonb ->> "
    "'cloud' = 'aws' AND\n"
    "                        association.paid_capacity_pool_key::jsonb ->> "
    "'version' = '2' AND\n"
    "                        association.paid_capacity_pool_key::jsonb ->> "
    "'use_spot' = 'true' AND\n"
    "                        association.paid_capacity_pool_key::jsonb -> "
    "'provider_identity' ->> 'aws_account_id' ~ "
    "'^[0-9]{12}$')))) AND\n")


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('Cancelled AWS paid cleanup is PostgreSQL-only.')


def _replace_function_fragment(function_name: str, source: str,
                               replacement: str) -> None:
    """Replace one exact fragment without owning historical function DDL."""
    bind = op.get_bind()
    definition = bind.execute(
        sa.text('SELECT pg_get_functiondef('
                'CAST(:signature AS regprocedure))'), {
                    'signature': f'{function_name}()'
                }).scalar_one()
    if replacement in definition:
        return
    if definition.count(source) != 1:
        raise RuntimeError(
            f'Serve065 found an unexpected {function_name} definition.')
    bind.exec_driver_sql(definition.replace(source, replacement))


def upgrade() -> None:
    """Install exact cancelled-AWS provider cleanup transitions."""
    _require_postgresql()
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_OLD_TERMINAL_SOURCE,
                               _ASSOCIATION_OLD_TERMINAL_REPLACEMENT)
    _replace_function_fragment(_ASSOCIATION_GUARD_FUNCTION,
                               _ASSOCIATION_NEW_TERMINAL_SOURCE,
                               _ASSOCIATION_NEW_TERMINAL_REPLACEMENT)
    _replace_function_fragment(_REPLICA_GUARD_FUNCTION,
                               _REPLICA_TERMINAL_SOURCE,
                               _REPLICA_TERMINAL_REPLACEMENT)


def downgrade() -> None:
    """Preserve cancelled AWS cleanup history across rollback."""
    _require_postgresql()
    raise RuntimeError(
        'Serve065 is forward-only; cancelled AWS allocations may already '
        'have been retired.')
