"""SQLAlchemy metadata for the inert lifecycle-action foundation."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

_METADATA = sqlalchemy.MetaData()

_LOWERCASE_SHA256 = r"'^[0-9a-f]{64}$'"
_UUID_V4 = (r"'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
            r"[89ab][0-9a-f]{3}-[0-9a-f]{12}$'")

_STORE_IDENTITY = sqlalchemy.Table(
    'lifecycle_store_identity',
    _METADATA,
    sqlalchemy.Column('store_key', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('store_uuid',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('schema_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('writer_authority_digest', sqlalchemy.Text,
                      nullable=True),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.PrimaryKeyConstraint('store_key',
                                    name='pk_lifecycle_store_identity'),
    sqlalchemy.CheckConstraint("store_key = 'global'",
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

_OWNERSHIP_SCOPES = sqlalchemy.Table(
    'lifecycle_ownership_scopes',
    _METADATA,
    sqlalchemy.Column('domain', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('operation_subset', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('store_mode', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('routing_mode', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('minimum_lifecycle_version',
                      sqlalchemy.Integer,
                      nullable=False),
    sqlalchemy.Column('ownership_epoch', sqlalchemy.BigInteger, nullable=False),
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
    sqlalchemy.CheckConstraint("store_mode = 'CENTRAL_POSTGRESQL'",
                               name='ck_lifecycle_ownership_scopes_store_mode'),
    sqlalchemy.CheckConstraint(
        "routing_mode IN ('DARK', 'LEGACY_OPEN', 'DRAINING', 'ACTION_OPEN')",
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
