"""Bind reserved-fill claims to one immutable worker projection version.

Revision ID: 046
Revises: 045
Create Date: 2026-08-13

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

revision: str = '046'
down_revision: str | Sequence[str] | None = '045'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CLAIM_SETS = 'reserved_fill_service_claim_sets'
_POOL_CLAIMS = 'reserved_fill_pool_claims'
_SERVICE_VERSION_CHECK = 'ck_reserved_fill_claim_set_service_version'
_WORKER_PROJECTION_MAP_CHECK = (
    'ck_reserved_fill_pool_worker_projection_sha256_map')


def upgrade() -> None:
    """Install the immutable version/projection claim boundary."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        return
    inspector = sa.inspect(bind)
    set_columns = {
        str(column['name']) for column in inspector.get_columns(_CLAIM_SETS)
    }
    if 'service_version' not in set_columns:
        op.add_column(_CLAIM_SETS,
                      sa.Column('service_version', sa.Integer(), nullable=True))
    edge_columns = {
        str(column['name']) for column in inspector.get_columns(_POOL_CLAIMS)
    }
    if 'worker_projection_sha256_by_accelerator' not in edge_columns:
        op.add_column(
            _POOL_CLAIMS,
            sa.Column('worker_projection_sha256_by_accelerator',
                      postgresql.JSONB(none_as_null=True),
                      nullable=True))

    set_checks = {
        str(check['name'])
        for check in sa.inspect(bind).get_check_constraints(_CLAIM_SETS)
        if check['name'] is not None
    }
    if _SERVICE_VERSION_CHECK not in set_checks:
        op.create_check_constraint(
            _SERVICE_VERSION_CHECK, _CLAIM_SETS,
            'service_version IS NULL OR service_version > 0')
    edge_checks = {
        str(check['name'])
        for check in sa.inspect(bind).get_check_constraints(_POOL_CLAIMS)
        if check['name'] is not None
    }
    if _WORKER_PROJECTION_MAP_CHECK not in edge_checks:
        op.create_check_constraint(
            _WORKER_PROJECTION_MAP_CHECK, _POOL_CLAIMS,
            "worker_projection_sha256_by_accelerator IS NULL OR "
            "(jsonb_typeof(worker_projection_sha256_by_accelerator) = 'object' "
            "AND worker_projection_sha256_by_accelerator <> '{}'::jsonb)")


def downgrade() -> None:
    raise RuntimeError(
        'Serve046 is forward-only. Preserve immutable reserved-fill worker '
        'projection authority and fix forward with a successor version.')
