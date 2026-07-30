"""SQLAlchemy schema for PostgreSQL operational events."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

metadata = sqlalchemy.MetaData()

RESOURCE_EVENTS = sqlalchemy.Table(
    'resource_events',
    metadata,
    sqlalchemy.Column('event_id',
                      postgresql.UUID(as_uuid=True),
                      primary_key=True),
    sqlalchemy.Column('event_sequence', sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column('occurred_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('phase', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('outcome', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('cause', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('message', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('actor_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('actor_name', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('actor_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_request_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_execution_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
)

RESOURCE_EVENT_TARGETS = sqlalchemy.Table(
    'resource_event_targets',
    metadata,
    sqlalchemy.Column('event_id',
                      postgresql.UUID(as_uuid=True),
                      sqlalchemy.ForeignKey('resource_events.event_id',
                                            ondelete='CASCADE'),
                      primary_key=True),
    sqlalchemy.Column('position', sqlalchemy.SmallInteger, primary_key=True),
    sqlalchemy.Column('target_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('target_id', sqlalchemy.Text),
    sqlalchemy.Column('target_name', sqlalchemy.Text, nullable=False),
)

REQUEST_STORE_METADATA = sqlalchemy.Table(
    'api_request_store_metadata',
    metadata,
    sqlalchemy.Column('key', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('value', postgresql.JSONB, nullable=False),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False),
)

CURSOR_AUTHORITY_METADATA_KEY = 'operational_event_cursor_authority_v1'
