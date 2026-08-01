"""Create the inert lifecycle-action ownership foundation.

Revision ID: 001
Revises:
Create Date: 2026-08-01

"""
# pylint: disable=invalid-name
from collections.abc import Sequence
import datetime
import uuid

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_STORE_IDENTITY = 'lifecycle_store_identity'
_OWNERSHIP_SCOPES = 'lifecycle_ownership_scopes'

_STORE_KEY = 'global'
_DOMAIN = 'VOLUME'
_OPERATION_SUBSET = 'KUBERNETES_PVC_OWNED_LIFECYCLE_V1'
_STORE_MODE = 'CENTRAL_POSTGRESQL'
_ROUTING_MODE = 'DARK'

_LOWERCASE_SHA256 = r"'^[0-9a-f]{64}$'"
_UUID_V4 = (r"'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
            r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$'")


def _require_postgresql() -> None:
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'The lifecycle-action ownership store is PostgreSQL-only.')


def upgrade() -> None:
    """Create and seed the revision-001 inert ownership foundation."""
    _require_postgresql()

    store_identity = op.create_table(
        _STORE_IDENTITY,
        sqlalchemy.Column('store_key', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('store_uuid',
                          postgresql.UUID(as_uuid=True),
                          nullable=False),
        sqlalchemy.Column('schema_version', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column('writer_authority_digest',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('created_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.PrimaryKeyConstraint('store_key',
                                        name='pk_lifecycle_store_identity'),
        sqlalchemy.CheckConstraint(
            "store_key = 'global'",
            name='ck_lifecycle_store_identity_singleton'),
        sqlalchemy.CheckConstraint(f'store_uuid::text ~ {_UUID_V4}',
                                   name='ck_lifecycle_store_identity_uuid_v4'),
        sqlalchemy.CheckConstraint(
            'schema_version = 1',
            name='ck_lifecycle_store_identity_schema_version'),
        sqlalchemy.CheckConstraint(
            'writer_authority_digest IS NULL OR '
            f'writer_authority_digest ~ {_LOWERCASE_SHA256}',
            name='ck_lifecycle_store_identity_writer_authority_format'),
        sqlalchemy.CheckConstraint(
            'writer_authority_digest IS NULL',
            name='ck_lifecycle_store_identity_m3s2_unsealed'),
        sqlalchemy.CheckConstraint(
            'isfinite(created_at)',
            name='ck_lifecycle_store_identity_created_at_finite'),
    )

    ownership_scopes = op.create_table(
        _OWNERSHIP_SCOPES,
        sqlalchemy.Column('domain', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('operation_subset', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('store_mode', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('routing_mode', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('minimum_lifecycle_version',
                          sqlalchemy.Integer,
                          nullable=False),
        sqlalchemy.Column('ownership_epoch',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('authority_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('writer_implementation_digest',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('reconciler_implementation_digest',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('updated_at',
                          sqlalchemy.DateTime(timezone=True),
                          nullable=False,
                          server_default=sqlalchemy.func.clock_timestamp()),
        sqlalchemy.PrimaryKeyConstraint('domain',
                                        'operation_subset',
                                        'store_mode',
                                        name='pk_lifecycle_ownership_scopes'),
        sqlalchemy.CheckConstraint("domain = 'VOLUME'",
                                   name='ck_lifecycle_ownership_scopes_domain'),
        sqlalchemy.CheckConstraint(
            "operation_subset = 'KUBERNETES_PVC_OWNED_LIFECYCLE_V1'",
            name='ck_lifecycle_ownership_scopes_operation_subset'),
        sqlalchemy.CheckConstraint(
            "store_mode = 'CENTRAL_POSTGRESQL'",
            name='ck_lifecycle_ownership_scopes_store_mode'),
        sqlalchemy.CheckConstraint(
            "routing_mode IN ('DARK', 'LEGACY_OPEN', 'DRAINING', "
            "'ACTION_OPEN')",
            name='ck_lifecycle_ownership_scopes_routing_mode'),
        sqlalchemy.CheckConstraint(
            'minimum_lifecycle_version >= 0',
            name='ck_lifecycle_ownership_scopes_minimum_version'),
        sqlalchemy.CheckConstraint(
            'ownership_epoch >= 1',
            name='ck_lifecycle_ownership_scopes_ownership_epoch'),
        sqlalchemy.CheckConstraint(
            'authority_generation >= 0',
            name='ck_lifecycle_ownership_scopes_authority_generation'),
        sqlalchemy.CheckConstraint(
            'writer_implementation_digest IS NULL OR '
            f'writer_implementation_digest ~ {_LOWERCASE_SHA256}',
            name='ck_lifecycle_ownership_scopes_writer_digest'),
        sqlalchemy.CheckConstraint(
            'reconciler_implementation_digest IS NULL OR '
            f'reconciler_implementation_digest ~ {_LOWERCASE_SHA256}',
            name='ck_lifecycle_ownership_scopes_reconciler_digest'),
        sqlalchemy.CheckConstraint(
            "routing_mode = 'DARK' AND "
            'minimum_lifecycle_version = 0 AND '
            'ownership_epoch = 1 AND '
            'authority_generation = 0 AND '
            'writer_implementation_digest IS NULL AND '
            'reconciler_implementation_digest IS NULL',
            name='ck_lifecycle_ownership_scopes_m3s2_inert'),
        sqlalchemy.CheckConstraint(
            'isfinite(updated_at)',
            name='ck_lifecycle_ownership_scopes_updated_at_finite'),
    )

    bind = op.get_bind()
    bind.execute(store_identity.insert().values(
        store_key=_STORE_KEY,
        store_uuid=uuid.uuid4(),
        schema_version=1,
        writer_authority_digest=None,
    ))
    bind.execute(ownership_scopes.insert().values(
        domain=_DOMAIN,
        operation_subset=_OPERATION_SUBSET,
        store_mode=_STORE_MODE,
        routing_mode=_ROUTING_MODE,
        minimum_lifecycle_version=0,
        ownership_epoch=1,
        authority_generation=0,
        writer_implementation_digest=None,
        reconciler_implementation_digest=None,
    ))


def _is_native_uuid4(value: object) -> bool:
    return (isinstance(value, uuid.UUID) and value.version == 4 and
            value.variant == uuid.RFC_4122)


def _is_aware_timestamp(value: object) -> bool:
    return (isinstance(value, datetime.datetime) and
            value.tzinfo is not None and value.utcoffset() is not None)


def downgrade() -> None:
    """Drop revision 001 only from the exact inert seed state."""
    _require_postgresql()
    bind = op.get_bind()
    bind.execute(
        sqlalchemy.text(f'LOCK TABLE {_OWNERSHIP_SCOPES}, {_STORE_IDENTITY} '
                        'IN ACCESS EXCLUSIVE MODE'))

    identity_rows = bind.execute(
        sqlalchemy.text(f'''
            SELECT store_key, store_uuid, schema_version,
                   writer_authority_digest, created_at,
                   isfinite(created_at) AS created_at_finite
            FROM {_STORE_IDENTITY}
        ''')).all()
    valid_identity = (len(identity_rows) == 1 and
                      identity_rows[0].store_key == _STORE_KEY and
                      _is_native_uuid4(identity_rows[0].store_uuid) and
                      identity_rows[0].schema_version == 1 and
                      identity_rows[0].writer_authority_digest is None and
                      _is_aware_timestamp(identity_rows[0].created_at) and
                      identity_rows[0].created_at_finite is True)

    scope_rows = bind.execute(
        sqlalchemy.text(f'''
            SELECT domain, operation_subset, store_mode, routing_mode,
                   minimum_lifecycle_version, ownership_epoch,
                   authority_generation, writer_implementation_digest,
                   reconciler_implementation_digest, updated_at,
                   isfinite(updated_at) AS updated_at_finite
            FROM {_OWNERSHIP_SCOPES}
        ''')).all()
    valid_scope = (len(scope_rows) == 1 and scope_rows[0].domain == _DOMAIN and
                   scope_rows[0].operation_subset == _OPERATION_SUBSET and
                   scope_rows[0].store_mode == _STORE_MODE and
                   scope_rows[0].routing_mode == _ROUTING_MODE and
                   scope_rows[0].minimum_lifecycle_version == 0 and
                   scope_rows[0].ownership_epoch == 1 and
                   scope_rows[0].authority_generation == 0 and
                   scope_rows[0].writer_implementation_digest is None and
                   scope_rows[0].reconciler_implementation_digest is None and
                   _is_aware_timestamp(scope_rows[0].updated_at) and
                   scope_rows[0].updated_at_finite is True)

    if not valid_identity or not valid_scope:
        raise RuntimeError(
            'Cannot downgrade the lifecycle-action ownership schema unless '
            'both tables contain exactly the revision-001 inert seeds.')

    bind.execute(
        sqlalchemy.text(f'''
            DELETE FROM {_OWNERSHIP_SCOPES}
            WHERE domain = :domain
              AND operation_subset = :operation_subset
              AND store_mode = :store_mode
        '''), {
            'domain': _DOMAIN,
            'operation_subset': _OPERATION_SUBSET,
            'store_mode': _STORE_MODE,
        })
    bind.execute(
        sqlalchemy.text(
            f'DELETE FROM {_STORE_IDENTITY} WHERE store_key = :store_key'),
        {'store_key': _STORE_KEY})
    op.drop_table(_OWNERSHIP_SCOPES)
    op.drop_table(_STORE_IDENTITY)
