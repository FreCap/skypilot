"""PostgreSQL schema for provider-free SkyServe route snapshots."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

metadata = sqlalchemy.MetaData()

serve_route_snapshots_table = sqlalchemy.Table(
    'serve_route_snapshots',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('generation', sqlalchemy.BigInteger, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_lifecycle_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('controller_incarnation',
                      sqlalchemy.Uuid(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('controller_owner_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('controller_pid', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('controller_ip', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('protocol_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('content_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('response_payload',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('identity_payload',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.CheckConstraint('generation > 0',
                               name='serve049_route_generation_positive_ck'),
    sqlalchemy.CheckConstraint('service_lifecycle_epoch > 0',
                               name='serve049_route_lifecycle_positive_ck'),
    sqlalchemy.CheckConstraint('controller_owner_epoch > 0',
                               name='serve049_route_owner_positive_ck'),
    sqlalchemy.CheckConstraint(
        'controller_pid > 0 AND length(controller_ip) > 0',
        name='serve049_route_owner_address_ck'),
    sqlalchemy.CheckConstraint('service_version > 0',
                               name='serve049_route_version_positive_ck'),
    sqlalchemy.CheckConstraint('protocol_version = 1',
                               name='serve049_route_protocol_ck'),
    sqlalchemy.CheckConstraint("content_sha256 ~ '^[0-9a-f]{64}$'",
                               name='serve049_route_digest_ck'),
)

serve_route_heads_table = sqlalchemy.Table(
    'serve_route_heads',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('generation', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('refreshed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('valid_until',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.CheckConstraint('generation > 0',
                               name='serve049_route_head_positive_ck'),
    sqlalchemy.CheckConstraint('valid_until > refreshed_at',
                               name='serve049_route_head_expiry_ck'),
)
sqlalchemy.Index('ix_serve049_route_heads_fresh',
                 serve_route_heads_table.c.valid_until)
