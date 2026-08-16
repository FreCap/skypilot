"""Add the generic non-pool SkyServe launch request envelope.

Revision ID: 011
Revises: 010
Create Date: 2026-08-16

The migration is deliberately additive for rolling compatibility. API010
writers omit every new nullable field and continue to advertise no generic
authority. Only a complete protocol-v2 tuple paired with the distinct generic
handler can describe a generic request.
"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy

revision: str = '011'
down_revision: str | Sequence[str] | None = '010'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_REQUESTS = 'api_requests'
_INSTANCES = 'api_server_instances'
_OLD_HANDLER_CONSTRAINT = 'ck_api_requests_ordinary_launch_handler'
_HANDLER_CONSTRAINT = 'ck_api_requests_non_pool_launch_handler'
_PROFILE_COMPLETE_CONSTRAINT = (
    'ck_api_requests_non_pool_launch_profile_complete')
_PROFILE_VALUES_CONSTRAINT = 'ck_api_requests_non_pool_launch_profile_values'
_INSTANCE_COMPLETE_CONSTRAINT = (
    'ck_api_server_instances_non_pool_launch_capability_complete')
_INSTANCE_VALUES_CONSTRAINT = (
    'ck_api_server_instances_non_pool_launch_capability_values')
_OLD_HANDLER = 'sky.server.requests.ordinary_launch:launch'
_GENERIC_HANDLER = 'sky.server.requests.non_pool_launch:launch'
_PROFILE_KINDS = (
    'ORDINARY_PAID',
    'ORDINARY_ZERO_COST',
    'RESERVED_FILL',
    'UNKNOWN_CAPACITY_REPLACEMENT',
    'COST_REBALANCE',
    'SYSTEM_OOM_RECOVERY',
)
_REQUEST_PROFILE_FIELDS = (
    'binding_protocol_version',
    'profile_kind',
    'profile_version',
    'profile_digest',
    'capability_cohort_epoch',
    'capability_profile_set_digest',
    'receipt_protocol_version',
)
_INSTANCE_CAPABILITY_FIELDS = (
    'non_pool_launch_binding_protocol_version',
    'non_pool_launch_capability_profile_set_digest',
    'non_pool_launch_capability_cohort_epoch',
    'non_pool_launch_receipt_protocol_version',
)


def _require_postgresql() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('The central API request store is PostgreSQL-only.')


def _num_nonnulls(fields: tuple[str, ...]) -> str:
    return f"num_nonnulls({', '.join(fields)})"


def upgrade() -> None:
    """Add fail-closed request and process capability identities."""
    _require_postgresql()
    for column in (
            sqlalchemy.Column('binding_protocol_version', sqlalchemy.Integer),
            sqlalchemy.Column('profile_kind', sqlalchemy.Text),
            sqlalchemy.Column('profile_version', sqlalchemy.Integer),
            sqlalchemy.Column('profile_digest', sqlalchemy.Text),
            sqlalchemy.Column('capability_cohort_epoch', sqlalchemy.BigInteger),
            sqlalchemy.Column('capability_profile_set_digest', sqlalchemy.Text),
            sqlalchemy.Column('receipt_protocol_version', sqlalchemy.Integer),
    ):
        op.add_column(_REQUESTS, column)

    request_nonnulls = _num_nonnulls(_REQUEST_PROFILE_FIELDS)
    op.create_check_constraint(
        _PROFILE_COMPLETE_CONSTRAINT, _REQUESTS,
        f'{request_nonnulls} IN (0, {len(_REQUEST_PROFILE_FIELDS)})')
    profile_kinds_sql = ', '.join(f"'{kind}'" for kind in _PROFILE_KINDS)
    op.create_check_constraint(
        _PROFILE_VALUES_CONSTRAINT, _REQUESTS,
        '(binding_protocol_version IS NULL OR '
        'binding_protocol_version = 2) AND '
        f'(profile_kind IS NULL OR profile_kind IN ({profile_kinds_sql})) AND '
        '(profile_version IS NULL OR profile_version = 1) AND '
        "(profile_digest IS NULL OR profile_digest ~ '^[0-9a-f]{64}$') AND "
        '(capability_cohort_epoch IS NULL OR '
        'capability_cohort_epoch > 0) AND '
        '(capability_profile_set_digest IS NULL OR '
        "capability_profile_set_digest ~ '^[0-9a-f]{64}$') AND "
        '(receipt_protocol_version IS NULL OR '
        'receipt_protocol_version = 1)')

    op.drop_constraint(_OLD_HANDLER_CONSTRAINT, _REQUESTS, type_='check')
    op.create_check_constraint(
        _HANDLER_CONSTRAINT, _REQUESTS,
        f"((handler_name = '{_OLD_HANDLER}' AND "
        'ordinary_launch_association_id IS NOT NULL AND '
        f'{request_nonnulls} = 0) OR '
        f"(handler_name = '{_GENERIC_HANDLER}' AND "
        'ordinary_launch_association_id IS NOT NULL AND '
        f'{request_nonnulls} = {len(_REQUEST_PROFILE_FIELDS)}) OR '
        f"(handler_name NOT IN ('{_OLD_HANDLER}', '{_GENERIC_HANDLER}') AND "
        'ordinary_launch_association_id IS NULL AND '
        f'{request_nonnulls} = 0))')

    op.add_column(
        _INSTANCES,
        sqlalchemy.Column('non_pool_launch_binding_capable',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()))
    for column in (
            sqlalchemy.Column('non_pool_launch_binding_protocol_version',
                              sqlalchemy.Integer),
            sqlalchemy.Column('non_pool_launch_capability_profile_set_digest',
                              sqlalchemy.Text),
            sqlalchemy.Column('non_pool_launch_capability_cohort_epoch',
                              sqlalchemy.BigInteger),
            sqlalchemy.Column('non_pool_launch_receipt_protocol_version',
                              sqlalchemy.Integer),
    ):
        op.add_column(_INSTANCES, column)

    instance_nonnulls = _num_nonnulls(_INSTANCE_CAPABILITY_FIELDS)
    op.create_check_constraint(
        _INSTANCE_COMPLETE_CONSTRAINT, _INSTANCES,
        '((NOT non_pool_launch_binding_capable AND '
        f'{instance_nonnulls} = 0) OR '
        '(non_pool_launch_binding_capable AND '
        f'{instance_nonnulls} = {len(_INSTANCE_CAPABILITY_FIELDS)}))')
    op.create_check_constraint(
        _INSTANCE_VALUES_CONSTRAINT, _INSTANCES,
        '(non_pool_launch_binding_protocol_version IS NULL OR '
        'non_pool_launch_binding_protocol_version = 2) AND '
        '(non_pool_launch_capability_profile_set_digest IS NULL OR '
        "non_pool_launch_capability_profile_set_digest ~ "
        "'^[0-9a-f]{64}$') AND "
        '(non_pool_launch_capability_cohort_epoch IS NULL OR '
        'non_pool_launch_capability_cohort_epoch > 0) AND '
        '(non_pool_launch_receipt_protocol_version IS NULL OR '
        'non_pool_launch_receipt_protocol_version = 1)')


def downgrade() -> None:
    """Retain generic identity evidence across application rollback."""
    _require_postgresql()
    raise RuntimeError(
        'API request schema 011 is additive and cannot be downgraded. Drain '
        'every generic non-pool request before rolling application code back.')
