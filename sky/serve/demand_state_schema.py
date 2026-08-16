"""PostgreSQL schema objects for controller-independent Serve demand."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

metadata = sqlalchemy.MetaData()

serve_demand_feed_generations_table = sqlalchemy.Table(
    'serve_demand_feed_generations',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('generation', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.CheckConstraint('generation > 0',
                               name='serve048_demand_generation_positive_ck'),
)

serve_lb_demand_reports_table = sqlalchemy.Table(
    'serve_lb_demand_reports',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('service_hash', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('reporter_session_id', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('lb_session_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('lb_slot', sqlalchemy.Text),
    sqlalchemy.Column('protocol_version', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('sequence', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('routing_version', sqlalchemy.Integer),
    sqlalchemy.Column('reporter_observed_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('received_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('valid_until',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
    sqlalchemy.Column('payload_sha256', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('complete', sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column('payload',
                      postgresql.JSONB(none_as_null=True),
                      nullable=False),
    sqlalchemy.CheckConstraint('protocol_version IN (1, 2)',
                               name='serve048_demand_protocol_ck'),
    sqlalchemy.CheckConstraint('sequence > 0',
                               name='serve048_demand_sequence_positive_ck'),
    sqlalchemy.CheckConstraint("payload_sha256 ~ '^[0-9a-f]{64}$'",
                               name='serve048_demand_digest_ck'),
    sqlalchemy.CheckConstraint('valid_until > received_at',
                               name='serve048_demand_expiry_ck'),
)
sqlalchemy.Index('ix_serve048_demand_reports_fresh',
                 serve_lb_demand_reports_table.c.service_name,
                 serve_lb_demand_reports_table.c.service_hash,
                 serve_lb_demand_reports_table.c.valid_until)
