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
    sqlalchemy.Column('producer_protocol_version',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='1'),
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
    sqlalchemy.CheckConstraint('producer_protocol_version IN (1, 2)',
                               name='serve051_route_producer_protocol_ck'),
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

# Serve051 separates provider-fenced endpoint resolution from provider-free
# readiness and full-snapshot composition.  Rows are keyed by both the service
# incarnation and immutable replica-record identity; a reused numeric replica
# ID can therefore never revive material from the row it replaced.
serve_route_replica_leases_table = sqlalchemy.Table(
    'serve_route_replica_leases',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('replica_id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('replica_record_id',
                      sqlalchemy.Uuid(as_uuid=True),
                      primary_key=True),
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
    sqlalchemy.Column('route_url', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('gpu_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('gpu_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('probe_method', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('readiness_path', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('probe_timeout_seconds',
                      sqlalchemy.Integer,
                      nullable=False),
    sqlalchemy.Column('probe_post_data', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('probe_headers', postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('async_occupancy', sqlalchemy.Boolean),
    sqlalchemy.Column('uses_logical_replicas',
                      sqlalchemy.Boolean,
                      nullable=False),
    sqlalchemy.Column('is_zero_cost', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('planned_capacity', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('route_allowed', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('requires_route_marker',
                      sqlalchemy.Boolean,
                      nullable=False),
    sqlalchemy.Column('route_marker_payload',
                      postgresql.JSONB(none_as_null=True)),
    sqlalchemy.Column('material_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('material_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('readiness_generation',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('ready',
                      sqlalchemy.Boolean,
                      nullable=False,
                      server_default=sqlalchemy.false()),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('resolved_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('observed_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('valid_until', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('revocation_generation',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('revoked_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('revocation_reason', sqlalchemy.Text),
    # The PostgreSQL migration owns the cross-metadata services FK.  Keeping
    # that reference out of this narrow runtime metadata lets SQLAlchemy sort
    # and inspect these projection tables without duplicating services here.
    sqlalchemy.CheckConstraint(
        'replica_id > 0 AND service_lifecycle_epoch > 0 AND '
        'controller_owner_epoch > 0 AND controller_pid > 0 AND '
        'service_version > 0',
        name='serve051_route_lease_owner_positive_ck'),
    sqlalchemy.CheckConstraint(
        'length(service_hash) > 0 AND length(controller_ip) > 0 AND '
        'length(route_url) > 0 AND length(gpu_type) > 0',
        name='serve051_route_lease_text_nonempty_ck'),
    sqlalchemy.CheckConstraint(
        "probe_method IN ('GET', 'POST') AND "
        "left(readiness_path, 1) = '/'",
        name='serve051_route_lease_probe_ck'),
    sqlalchemy.CheckConstraint(
        'gpu_count > 0 AND planned_capacity > 0 AND '
        'probe_timeout_seconds > 0 AND probe_timeout_seconds <= 86400',
        name='serve051_route_lease_capacity_positive_ck'),
    sqlalchemy.CheckConstraint("material_sha256 ~ '^[0-9a-f]{64}$'",
                               name='serve051_route_lease_digest_ck'),
    sqlalchemy.CheckConstraint(
        'material_generation > 0 AND readiness_generation >= 0 AND '
        'revocation_generation >= 0',
        name='serve051_route_lease_generation_ck'),
    sqlalchemy.CheckConstraint(
        '((observed_at IS NULL AND valid_until IS NULL AND NOT ready) OR '
        '(observed_at IS NOT NULL AND valid_until IS NOT NULL AND '
        'valid_until > observed_at))',
        name='serve051_route_lease_readiness_shape_ck'),
    sqlalchemy.CheckConstraint(
        '((revoked_at IS NULL AND revocation_reason IS NULL) OR '
        '(revoked_at IS NOT NULL AND length(revocation_reason) > 0))',
        name='serve051_route_lease_revocation_shape_ck'),
)
sqlalchemy.Index('ix_serve051_route_lease_candidates',
                 serve_route_replica_leases_table.c.service_name,
                 serve_route_replica_leases_table.c.service_hash,
                 serve_route_replica_leases_table.c.revoked_at,
                 serve_route_replica_leases_table.c.valid_until)
sqlalchemy.Index('ix_serve051_route_lease_replica',
                 serve_route_replica_leases_table.c.service_name,
                 serve_route_replica_leases_table.c.replica_id)
