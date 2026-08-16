"""PostgreSQL schema for ordered SkyServe capacity admission."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

metadata = sqlalchemy.MetaData()

serve_capacity_plans_table = sqlalchemy.Table(
    'serve_capacity_plans',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('generation', sqlalchemy.BigInteger, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('service_lifecycle_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('service_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('demand_source_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('demand_feed_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('route_generation', sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('route_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('route_source_epoch',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('protocol_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('content_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('payload',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
)

serve_capacity_plan_heads_table = sqlalchemy.Table(
    'serve_capacity_plan_heads',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('generation', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('demand_feed_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('receipt_watermark_sha256',
                      sqlalchemy.Text,
                      nullable=False),
    sqlalchemy.Column('refreshed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('valid_until',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
)
