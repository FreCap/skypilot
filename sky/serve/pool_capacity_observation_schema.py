"""PostgreSQL schema catalog for Serve physical-pool observations.

The authority columns deliberately live outside ``serve_state_schema.Base``.
Local/controller SQLite remains at Serve037 and must never bootstrap this
PostgreSQL-only observation authority.  Revision 044 adds nullable observation
and round-provenance columns, inactive authenticated allocation-publication
state, and a one-way reconciliation gate.  Existing context-keyed observations,
rounds, and claim sets remain inert legacy projections until explicit
activation.
"""

import sqlalchemy
from sqlalchemy.dialects import postgresql

metadata = sqlalchemy.MetaData()

IN_PROGRESS = 'IN_PROGRESS'
SUCCESS = 'SUCCESS'
BLACKOUT = 'BLACKOUT'
COMPLETED_STATUSES = (SUCCESS, BLACKOUT)

LEGACY_ACTIVE = 'LEGACY_ACTIVE'
SEQUENCED_ACTIVE = 'SEQUENCED_ACTIVE'
RECONCILIATION_GATE_STATES = (LEGACY_ACTIVE, SEQUENCED_ACTIVE)

_SHA256_PATTERN = '^[0-9a-f]{64}$'


def protocol_authority_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return fresh Serve044 sequencer and one-way gate columns."""
    return (
        sqlalchemy.Column('zero_cost_admission_sequence',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        # Ordinary zero-cost demand is not broker-partitioned.  Observations
        # and authenticated maps snapshot this independent high-water so any
        # later ordinary admission invalidates their fill authority without
        # making broker-disjoint fill rows invalidate one another.
        sqlalchemy.Column('ordinary_zero_cost_admission_sequence',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        # First successful sky.launch transitions have their own commit order.
        # Admission and materialization are independent event streams: a row
        # may be admitted before an observation but bind its pod afterward.
        sqlalchemy.Column('zero_cost_materialization_sequence',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('reconciliation_gate_state',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default=LEGACY_ACTIVE),
        sqlalchemy.Column('reconciliation_gate_generation',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
    )


def reclaim_authorization_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return fresh Serve045 generation-bound authorization columns.

    The writer rollout columns already exist in the protocol-v2 table.  They
    are repeated in this PostgreSQL catalog so the one-row authorization
    receipt can be decoded and compare-and-swapped through one typed table.
    Migration 045 adds only columns that are absent.
    """
    return (
        sqlalchemy.Column('reclaim_fleet_bundle_sha256',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('reclaim_policy_revision',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('reclaim_provider_inventory_sha256',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('reclaim_claim_scope_count',
                          sqlalchemy.BigInteger,
                          nullable=True),
        sqlalchemy.Column('reclaim_claim_scope_sha256',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('reclaim_evidence_sha256',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('reclaim_authorized_at',
                          sqlalchemy.Float,
                          nullable=True),
        sqlalchemy.Column('image_digest', sqlalchemy.Text, nullable=True),
        sqlalchemy.Column('deployment_generation',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('deployment_uid', sqlalchemy.Text, nullable=True),
        sqlalchemy.Column('pod_inventory_count',
                          sqlalchemy.Integer,
                          nullable=True),
        sqlalchemy.Column('pod_inventory_sha256',
                          sqlalchemy.Text,
                          nullable=True),
    )


def observation_authority_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return fresh PostgreSQL observation-authority columns."""
    return (
        sqlalchemy.Column('pool_key', sqlalchemy.Text, nullable=True),
        sqlalchemy.Column('physical_cluster_uid',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('accelerator_names',
                          postgresql.JSONB(none_as_null=True),
                          nullable=True),
        sqlalchemy.Column('access_context', sqlalchemy.Text, nullable=True),
        sqlalchemy.Column('observation_generation',
                          sqlalchemy.BigInteger,
                          nullable=True),
        sqlalchemy.Column('lease_token',
                          postgresql.UUID(as_uuid=True),
                          nullable=True),
        sqlalchemy.Column('lease_expires_at', sqlalchemy.Float, nullable=True),
        sqlalchemy.Column('observation_sequence',
                          sqlalchemy.BigInteger,
                          nullable=True),
        sqlalchemy.Column('ordinary_admission_sequence',
                          sqlalchemy.BigInteger,
                          nullable=True),
        sqlalchemy.Column('materialization_sequence',
                          sqlalchemy.BigInteger,
                          nullable=True),
        sqlalchemy.Column('observation_status', sqlalchemy.Text, nullable=True),
        sqlalchemy.Column('payload',
                          postgresql.JSONB(none_as_null=True),
                          nullable=True),
        # This digest covers the complete legacy projection, identity, lease,
        # sequence, timestamps, status, and typed payload.  It is intentionally
        # broader than the JSON payload despite the compatibility-era name.
        sqlalchemy.Column('payload_sha256', sqlalchemy.Text, nullable=True),
        sqlalchemy.Column('observed_at', sqlalchemy.Float, nullable=True),
        sqlalchemy.Column('valid_until', sqlalchemy.Float, nullable=True),
        sqlalchemy.Column('published_at', sqlalchemy.Float, nullable=True),
    )


def round_observation_provenance_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return nullable exact-observation provenance for broker rounds."""
    return (
        sqlalchemy.Column('observation_generation',
                          sqlalchemy.BigInteger,
                          nullable=True),
        sqlalchemy.Column('observation_sequence',
                          sqlalchemy.BigInteger,
                          nullable=True),
        sqlalchemy.Column('observation_materialization_sequence',
                          sqlalchemy.BigInteger,
                          nullable=True),
        sqlalchemy.Column('observation_payload_sha256',
                          sqlalchemy.Text,
                          nullable=True),
    )


def allocation_publication_columns() -> tuple[sqlalchemy.Column, ...]:
    """Return inactive authenticated planner-publication columns."""
    return (
        sqlalchemy.Column('allocation_generation',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('allocation_input_sha256',
                          sqlalchemy.Text,
                          nullable=True),
        sqlalchemy.Column('allocation_claim_generation',
                          sqlalchemy.BigInteger,
                          nullable=True),
        sqlalchemy.Column('allocation_map',
                          postgresql.JSONB(none_as_null=True),
                          nullable=True),
        sqlalchemy.Column('allocation_published_at',
                          sqlalchemy.Float,
                          nullable=True),
        sqlalchemy.Column('allocation_gate_generation',
                          sqlalchemy.BigInteger,
                          nullable=True),
    )


PROTOCOL_SEQUENCE_CHECK_NAME = 'ck_reserved_fill_admission_sequences'
PROTOCOL_SEQUENCE_CHECK_SQL = """
(
  zero_cost_admission_sequence >= 0
  AND ordinary_zero_cost_admission_sequence >= 0
  AND ordinary_zero_cost_admission_sequence <= zero_cost_admission_sequence
  AND zero_cost_materialization_sequence >= 0
)
""".strip()
RECONCILIATION_GATE_CHECK_NAME = 'ck_reserved_fill_reconciliation_gate'
RECONCILIATION_GATE_CHECK_SQL = f"""
(
  reconciliation_gate_state IN ('{LEGACY_ACTIVE}', '{SEQUENCED_ACTIVE}')
  AND reconciliation_gate_generation >= 0
)
""".strip()
RECLAIM_AUTHORIZATION_CHECK_NAME = ('ck_reserved_fill_reclaim_authorization')
RECLAIM_AUTHORIZATION_CHECK_SQL = f"""
(
  (
    reconciliation_gate_state = '{LEGACY_ACTIVE}'
    AND reclaim_fleet_bundle_sha256 IS NULL
    AND reclaim_policy_revision IS NULL
    AND reclaim_provider_inventory_sha256 IS NULL
    AND reclaim_claim_scope_count IS NULL
    AND reclaim_claim_scope_sha256 IS NULL
    AND reclaim_evidence_sha256 IS NULL
    AND reclaim_authorized_at IS NULL
  )
  OR
  (
    reconciliation_gate_state = '{SEQUENCED_ACTIVE}'
    AND protocol_version = 2
    AND reclaim_fleet_bundle_sha256 IS NOT NULL
    AND reclaim_fleet_bundle_sha256 ~ '{_SHA256_PATTERN}'
    AND reclaim_policy_revision IS NOT NULL
    AND octet_length(reclaim_policy_revision) BETWEEN 1 AND 1024
    AND reclaim_provider_inventory_sha256 IS NOT NULL
    AND reclaim_provider_inventory_sha256 ~ '{_SHA256_PATTERN}'
    AND reclaim_claim_scope_count IS NOT NULL
    AND reclaim_claim_scope_count >= 0
    AND reclaim_claim_scope_sha256 IS NOT NULL
    AND reclaim_claim_scope_sha256 ~ '{_SHA256_PATTERN}'
    AND reclaim_evidence_sha256 IS NOT NULL
    AND reclaim_evidence_sha256 ~ '{_SHA256_PATTERN}'
    AND reclaim_authorized_at IS NOT NULL
    AND reclaim_authorized_at >= 0
    AND image_digest IS NOT NULL
    AND image_digest ~ '^sha256:[0-9a-f]{{64}}$'
    AND deployment_generation IS NOT NULL
    AND octet_length(deployment_generation) BETWEEN 1 AND 1024
    AND deployment_uid IS NOT NULL
    AND octet_length(deployment_uid) BETWEEN 1 AND 1024
    AND pod_inventory_count IS NOT NULL
    AND pod_inventory_count > 0
    AND pod_inventory_sha256 IS NOT NULL
    AND pod_inventory_sha256 ~ '{_SHA256_PATTERN}'
  )
)
""".strip()

ROUND_OBSERVATION_PROVENANCE_CHECK_NAME = (
    'ck_reserved_fill_round_observation_provenance')
ROUND_OBSERVATION_PROVENANCE_CHECK_SQL = f"""
(
  (
    observation_generation IS NULL
    AND observation_sequence IS NULL
    AND observation_materialization_sequence IS NULL
    AND observation_payload_sha256 IS NULL
  )
  OR
  (
    observation_generation IS NOT NULL
    AND observation_generation > 0
    AND observation_sequence IS NOT NULL
    AND observation_sequence >= 0
    AND observation_materialization_sequence IS NOT NULL
    AND observation_materialization_sequence >= 0
    AND observation_payload_sha256 IS NOT NULL
    AND observation_payload_sha256 ~ '{_SHA256_PATTERN}'
  )
)
""".strip()

ALLOCATION_PUBLICATION_CHECK_NAME = (
    'ck_reserved_fill_claim_set_allocation_publication')
ALLOCATION_PUBLICATION_CHECK_SQL = f"""
(
  (
    allocation_generation = 0
    AND allocation_input_sha256 IS NULL
    AND allocation_claim_generation IS NULL
    AND allocation_map IS NULL
    AND allocation_published_at IS NULL
    AND allocation_gate_generation IS NULL
  )
  OR
  (
    allocation_generation > 0
    AND allocation_input_sha256 IS NOT NULL
    AND allocation_input_sha256 ~ '{_SHA256_PATTERN}'
    AND allocation_claim_generation IS NOT NULL
    AND allocation_claim_generation >= 0
    AND allocation_map IS NOT NULL
    AND jsonb_typeof(allocation_map) = 'object'
    AND octet_length(CAST(allocation_map AS TEXT)) <= 1048576
    AND allocation_published_at IS NOT NULL
    AND allocation_published_at >= 0
    AND allocation_gate_generation IS NOT NULL
    AND allocation_gate_generation >= 0
  )
)
""".strip()

OBSERVATION_IDENTITY_UNIQUE_NAME = (
    'uq_demand_capacity_observations_pool_generation')
OBSERVATION_POOL_GENERATION_INDEX_NAME = (
    'ix_demand_capacity_observations_pool_generation')
OBSERVATION_POOL_COMPLETED_INDEX_NAME = (
    'ix_demand_capacity_observations_pool_completed')
OBSERVATION_AUTHORITY_SHAPE_CHECK_NAME = (
    'ck_demand_capacity_observations_authority_shape')
OBSERVATION_AUTHORITY_SHAPE_CHECK_SQL = f"""
(
  (
    pool_key IS NULL
    AND physical_cluster_uid IS NULL
    AND accelerator_names IS NULL
    AND access_context IS NULL
    AND observation_generation IS NULL
    AND lease_token IS NULL
    AND lease_expires_at IS NULL
    AND observation_sequence IS NULL
    AND ordinary_admission_sequence IS NULL
    AND materialization_sequence IS NULL
    AND observation_status IS NULL
    AND payload IS NULL
    AND payload_sha256 IS NULL
    AND observed_at IS NULL
    AND valid_until IS NULL
    AND published_at IS NULL
  )
  OR
  (
    pool_key IS NOT NULL
    AND physical_cluster_uid IS NOT NULL
    AND accelerator_names IS NOT NULL
    AND access_context IS NOT NULL
    AND observation_generation IS NOT NULL
    AND lease_token IS NOT NULL
    AND lease_expires_at IS NOT NULL
    AND observation_sequence IS NOT NULL
    AND ordinary_admission_sequence IS NOT NULL
    AND materialization_sequence IS NOT NULL
    AND observation_status IN ('{IN_PROGRESS}', '{SUCCESS}', '{BLACKOUT}')
    AND observed_at IS NOT NULL
    AND valid_until IS NOT NULL
    AND (
      (
        observation_status = '{IN_PROGRESS}'
        AND payload IS NULL
        AND payload_sha256 IS NULL
        AND published_at IS NULL
      )
      OR
      (
        observation_status IN ('{SUCCESS}', '{BLACKOUT}')
        AND payload IS NOT NULL
        AND payload_sha256 IS NOT NULL
        AND published_at IS NOT NULL
      )
    )
  )
)
""".strip()
OBSERVATION_AUTHORITY_BOUNDS_CHECK_NAME = (
    'ck_demand_capacity_observations_authority_bounds')
OBSERVATION_AUTHORITY_BOUNDS_CHECK_SQL = f"""
(
  pool_key IS NULL
  OR
  (
    octet_length(pool_key) BETWEEN 1 AND 4096
    AND octet_length(physical_cluster_uid) BETWEEN 1 AND 1024
    AND octet_length(access_context) BETWEEN 1 AND 1024
    AND jsonb_typeof(accelerator_names) = 'array'
    AND jsonb_array_length(accelerator_names) BETWEEN 1 AND 64
    AND octet_length(CAST(accelerator_names AS TEXT)) <= 8192
    AND observation_generation > 0
    AND observation_sequence >= 0
    AND ordinary_admission_sequence >= 0
    AND ordinary_admission_sequence <= observation_sequence
    AND materialization_sequence >= 0
    AND lease_expires_at >= observed_at
    AND valid_until > observed_at
    AND (
      published_at IS NULL
      OR (completed_at >= observed_at AND published_at >= completed_at)
    )
    AND (payload IS NULL OR jsonb_typeof(payload) = 'object')
    AND (payload IS NULL OR octet_length(CAST(payload AS TEXT)) <= 16384)
    AND (payload_sha256 IS NULL OR payload_sha256 ~ '{_SHA256_PATTERN}')
  )
)
""".strip()

protocol_state_sequence_table = sqlalchemy.Table(
    'reserved_fill_protocol_state',
    metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('protocol_version', sqlalchemy.Integer, nullable=False),
    *protocol_authority_columns(),
    *reclaim_authorization_columns(),
)

reserved_fill_round_observation_table = sqlalchemy.Table(
    'reserved_fill_rounds',
    metadata,
    sqlalchemy.Column('pool_key', sqlalchemy.Text, primary_key=True),
    *round_observation_provenance_columns(),
    sqlalchemy.CheckConstraint(
        ROUND_OBSERVATION_PROVENANCE_CHECK_SQL,
        name=ROUND_OBSERVATION_PROVENANCE_CHECK_NAME,
    ),
)

reserved_fill_service_allocation_table = sqlalchemy.Table(
    'reserved_fill_service_claim_sets',
    metadata,
    sqlalchemy.Column('service_name', sqlalchemy.Text, primary_key=True),
    *allocation_publication_columns(),
    sqlalchemy.CheckConstraint(
        ALLOCATION_PUBLICATION_CHECK_SQL,
        name=ALLOCATION_PUBLICATION_CHECK_NAME,
    ),
)

demand_capacity_observations_v2_table = sqlalchemy.Table(
    'demand_capacity_observations',
    metadata,
    sqlalchemy.Column('context', sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column('snapshot_time', sqlalchemy.Float, nullable=False),
    sqlalchemy.Column('completed_at', sqlalchemy.Float, nullable=False),
    sqlalchemy.Column('availability', sqlalchemy.Text, nullable=True),
    *observation_authority_columns(),
    sqlalchemy.UniqueConstraint(
        'pool_key',
        'observation_generation',
        name=OBSERVATION_IDENTITY_UNIQUE_NAME,
    ),
    sqlalchemy.CheckConstraint(
        OBSERVATION_AUTHORITY_SHAPE_CHECK_SQL,
        name=OBSERVATION_AUTHORITY_SHAPE_CHECK_NAME,
    ),
    sqlalchemy.CheckConstraint(
        OBSERVATION_AUTHORITY_BOUNDS_CHECK_SQL,
        name=OBSERVATION_AUTHORITY_BOUNDS_CHECK_NAME,
    ),
)
sqlalchemy.Index(
    OBSERVATION_POOL_GENERATION_INDEX_NAME,
    demand_capacity_observations_v2_table.c.pool_key,
    demand_capacity_observations_v2_table.c.observation_generation.desc(),
)
sqlalchemy.Index(
    OBSERVATION_POOL_COMPLETED_INDEX_NAME,
    demand_capacity_observations_v2_table.c.pool_key,
    demand_capacity_observations_v2_table.c.observation_generation.desc(),
    postgresql_where=(demand_capacity_observations_v2_table.c.
                      observation_status.in_(COMPLETED_STATUSES)),
)
