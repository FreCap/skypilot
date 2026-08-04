"""SQLAlchemy metadata for physical-capacity projection revision 001."""

import sqlalchemy
from sqlalchemy.dialects import postgresql

METADATA = sqlalchemy.MetaData()

_ACTOR_TYPES = "('system', 'basic', 'sa', 'sso', 'legacy', 'unknown')"
_LOWERCASE_SHA256 = r"'^[0-9a-f]{64}$'"

PROJECTION_SCANS = sqlalchemy.Table(
    'capacity_projection_scans',
    METADATA,
    sqlalchemy.Column('scan_id', postgresql.UUID(as_uuid=True), nullable=False),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_partition_hash',
                      sqlalchemy.CHAR(64),
                      nullable=False),
    sqlalchemy.Column('cursor_schema_version',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='1'),
    sqlalchemy.Column('cursor', postgresql.JSONB, nullable=False),
    sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('controller_instance_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('controller_generation', sqlalchemy.BigInteger),
    sqlalchemy.Column('rows_seen',
                      sqlalchemy.BigInteger,
                      nullable=False,
                      server_default='0'),
    sqlalchemy.Column('finding_counts',
                      postgresql.JSONB,
                      nullable=False,
                      server_default=sqlalchemy.text("'{}'::jsonb")),
    sqlalchemy.Column('error_code', sqlalchemy.Text),
    sqlalchemy.Column('started_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('completed_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.PrimaryKeyConstraint('scan_id',
                                    name='pk_capacity_projection_scans'),
    sqlalchemy.UniqueConstraint(
        'scan_id',
        'workspace',
        name='uq_capacity_projection_scans_id_workspace'),
    sqlalchemy.UniqueConstraint(
        'scan_id',
        'workspace',
        'source_kind',
        name='uq_capacity_projection_scans_id_workspace_kind'),
    sqlalchemy.CheckConstraint(
        "source_kind IN "
        "('serve_service', 'serve_pool', 'managed_job_task')",
        name='ck_capacity_projection_scans_source_kind'),
    sqlalchemy.CheckConstraint(
        f'source_partition_hash ~ {_LOWERCASE_SHA256}',
        name='ck_capacity_projection_scans_partition_hash'),
    sqlalchemy.CheckConstraint(
        'cursor_schema_version >= 1',
        name='ck_capacity_projection_scans_cursor_version'),
    sqlalchemy.CheckConstraint(
        "jsonb_typeof(cursor) = 'object'",
        name='ck_capacity_projection_scans_cursor_object'),
    sqlalchemy.CheckConstraint("state IN ('running', 'completed', 'failed')",
                               name='ck_capacity_projection_scans_state'),
    sqlalchemy.CheckConstraint(
        '(controller_instance_id IS NULL AND '
        'controller_generation IS NULL) OR '
        '(controller_instance_id IS NOT NULL AND '
        'controller_generation IS NOT NULL)',
        name='ck_capacity_projection_scans_controller_pair'),
    sqlalchemy.CheckConstraint(
        'controller_generation IS NULL OR controller_generation > 0',
        name='ck_capacity_projection_scans_controller_generation'),
    sqlalchemy.CheckConstraint('rows_seen >= 0',
                               name='ck_capacity_projection_scans_rows_seen'),
    sqlalchemy.CheckConstraint(
        "jsonb_typeof(finding_counts) = 'object'",
        name='ck_capacity_projection_scans_finding_counts'),
    sqlalchemy.CheckConstraint(
        "(state = 'running' AND completed_at IS NULL) OR "
        "(state IN ('completed', 'failed') AND completed_at IS NOT NULL)",
        name='ck_capacity_projection_scans_terminal_time'),
    sqlalchemy.CheckConstraint(
        "(state = 'failed' AND error_code IS NOT NULL) OR "
        "(state != 'failed' AND error_code IS NULL)",
        name='ck_capacity_projection_scans_error_code'),
    sqlalchemy.CheckConstraint(
        'completed_at IS NULL OR completed_at >= started_at',
        name='ck_capacity_projection_scans_completed_order'),
)
sqlalchemy.Index('uq_capacity_projection_scans_running_partition',
                 PROJECTION_SCANS.c.workspace,
                 PROJECTION_SCANS.c.source_kind,
                 PROJECTION_SCANS.c.source_partition_hash,
                 unique=True,
                 postgresql_where=sqlalchemy.text("state = 'running'"))
sqlalchemy.Index('ix_capacity_projection_scans_completed',
                 PROJECTION_SCANS.c.workspace, PROJECTION_SCANS.c.source_kind,
                 PROJECTION_SCANS.c.completed_at.desc())
sqlalchemy.Index('ix_capacity_projection_scans_state_updated',
                 PROJECTION_SCANS.c.state, PROJECTION_SCANS.c.updated_at)

GROUPS = sqlalchemy.Table(
    'capacity_groups',
    METADATA,
    sqlalchemy.Column('group_id', postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('owner_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('owner_id', sqlalchemy.Text),
    sqlalchemy.Column('owner_incarnation', sqlalchemy.Text),
    sqlalchemy.Column('writer_fence_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('writer_controller_fingerprint', sqlalchemy.CHAR(64)),
    sqlalchemy.Column('writer_instance_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('writer_fence_epoch', sqlalchemy.BigInteger),
    sqlalchemy.Column('source_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_key', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_incarnation_hash',
                      sqlalchemy.CHAR(64),
                      nullable=False),
    sqlalchemy.Column('projection_confidence', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('current_intent_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('lifecycle_state',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='active'),
    sqlalchemy.Column('last_seen_scan_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('source_missing_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('retired_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('created_by_actor_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('updated_by_actor_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('created_by_actor_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('updated_by_actor_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.PrimaryKeyConstraint('group_id', name='pk_capacity_groups'),
    sqlalchemy.UniqueConstraint('group_id',
                                'workspace',
                                name='uq_capacity_groups_id_workspace'),
    sqlalchemy.UniqueConstraint('workspace',
                                'source_kind',
                                'source_key',
                                'source_incarnation_hash',
                                name='uq_capacity_groups_source_incarnation'),
    sqlalchemy.ForeignKeyConstraint(['last_seen_scan_id'],
                                    ['capacity_projection_scans.scan_id'],
                                    name='fk_capacity_groups_last_seen_scan',
                                    ondelete='SET NULL'),
    sqlalchemy.CheckConstraint(
        "owner_kind IN ('service', 'pool', 'managed_job_task')",
        name='ck_capacity_groups_owner_kind'),
    sqlalchemy.CheckConstraint(
        "writer_fence_kind IN "
        "('serve_lifecycle', 'controller_generation', 'legacy')",
        name='ck_capacity_groups_writer_fence_kind'),
    sqlalchemy.CheckConstraint(
        '('
        "writer_fence_kind = 'serve_lifecycle' "
        'AND writer_controller_fingerprint IS NOT NULL '
        f'AND writer_controller_fingerprint ~ {_LOWERCASE_SHA256} '
        'AND writer_instance_id IS NULL '
        'AND writer_fence_epoch IS NOT NULL '
        'AND writer_fence_epoch > 0'
        ') OR ('
        "writer_fence_kind = 'controller_generation' "
        'AND writer_controller_fingerprint IS NULL '
        'AND writer_instance_id IS NOT NULL '
        'AND writer_fence_epoch IS NOT NULL '
        'AND writer_fence_epoch > 0'
        ') OR ('
        "writer_fence_kind = 'legacy' "
        'AND writer_controller_fingerprint IS NULL '
        'AND writer_instance_id IS NULL '
        'AND writer_fence_epoch IS NULL'
        ')',
        name='ck_capacity_groups_writer_fence'),
    sqlalchemy.CheckConstraint(
        "source_kind IN "
        "('serve_service', 'serve_pool', 'managed_job_task')",
        name='ck_capacity_groups_source_kind'),
    sqlalchemy.CheckConstraint(
        f'source_incarnation_hash ~ {_LOWERCASE_SHA256}',
        name='ck_capacity_groups_source_incarnation_hash'),
    sqlalchemy.CheckConstraint(
        "projection_confidence IN ('exact', 'legacy', 'unknown')",
        name='ck_capacity_groups_projection_confidence'),
    sqlalchemy.CheckConstraint(
        'current_intent_generation >= 1',
        name='ck_capacity_groups_current_intent_generation'),
    sqlalchemy.CheckConstraint(
        "lifecycle_state IN ('active', 'retiring', 'retired')",
        name='ck_capacity_groups_lifecycle_state'),
    sqlalchemy.CheckConstraint(
        "(lifecycle_state = 'retired') = (retired_at IS NOT NULL)",
        name='ck_capacity_groups_retired_at'),
    sqlalchemy.CheckConstraint(f'created_by_actor_type IN {_ACTOR_TYPES}',
                               name='ck_capacity_groups_created_actor_type'),
    sqlalchemy.CheckConstraint(f'updated_by_actor_type IN {_ACTOR_TYPES}',
                               name='ck_capacity_groups_updated_actor_type'),
    sqlalchemy.CheckConstraint(
        "projection_confidence != 'exact' OR "
        '(owner_id IS NOT NULL AND owner_incarnation IS NOT NULL '
        "AND writer_fence_kind != 'legacy')",
        name='ck_capacity_groups_exact_confidence'),
)
sqlalchemy.Index('uq_capacity_groups_exact_owner',
                 GROUPS.c.workspace,
                 GROUPS.c.owner_kind,
                 GROUPS.c.owner_id,
                 GROUPS.c.owner_incarnation,
                 unique=True,
                 postgresql_where=sqlalchemy.text(
                     "projection_confidence = 'exact' "
                     "AND lifecycle_state != 'retired'"))

GROUP_INTENTS = sqlalchemy.Table(
    'capacity_group_intents',
    METADATA,
    sqlalchemy.Column('group_id', postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('intent_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('schema_version',
                      sqlalchemy.Integer,
                      nullable=False,
                      server_default='1'),
    sqlalchemy.Column('placement_contract', postgresql.JSONB, nullable=False),
    sqlalchemy.Column('placement_contract_hash',
                      sqlalchemy.CHAR(64),
                      nullable=False),
    sqlalchemy.Column('desired_count', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('topology', postgresql.JSONB, nullable=False),
    sqlalchemy.Column('intent_hash', sqlalchemy.CHAR(64), nullable=False),
    sqlalchemy.Column('source_fingerprint', sqlalchemy.CHAR(64),
                      nullable=False),
    sqlalchemy.Column('created_by_actor_id', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('created_by_actor_type', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.PrimaryKeyConstraint('group_id',
                                    'intent_generation',
                                    name='pk_capacity_group_intents'),
    sqlalchemy.UniqueConstraint(
        'group_id',
        'workspace',
        'intent_generation',
        name='uq_capacity_group_intents_workspace_generation'),
    sqlalchemy.ForeignKeyConstraint(
        ['group_id', 'workspace'],
        ['capacity_groups.group_id', 'capacity_groups.workspace'],
        name='fk_capacity_group_intents_group',
        ondelete='NO ACTION',
        deferrable=True,
        initially='DEFERRED'),
    sqlalchemy.CheckConstraint('intent_generation >= 1',
                               name='ck_capacity_group_intents_generation'),
    sqlalchemy.CheckConstraint('schema_version >= 1',
                               name='ck_capacity_group_intents_schema_version'),
    sqlalchemy.CheckConstraint(
        "jsonb_typeof(placement_contract) = 'object'",
        name='ck_capacity_group_intents_placement_object'),
    sqlalchemy.CheckConstraint(f'placement_contract_hash ~ {_LOWERCASE_SHA256}',
                               name='ck_capacity_group_intents_placement_hash'),
    sqlalchemy.CheckConstraint('desired_count >= 0',
                               name='ck_capacity_group_intents_desired_count'),
    sqlalchemy.CheckConstraint(
        "jsonb_typeof(topology) = 'object'",
        name='ck_capacity_group_intents_topology_object'),
    sqlalchemy.CheckConstraint(f'intent_hash ~ {_LOWERCASE_SHA256}',
                               name='ck_capacity_group_intents_intent_hash'),
    sqlalchemy.CheckConstraint(
        f'source_fingerprint ~ {_LOWERCASE_SHA256}',
        name='ck_capacity_group_intents_source_fingerprint'),
    sqlalchemy.CheckConstraint(f'created_by_actor_type IN {_ACTOR_TYPES}',
                               name='ck_capacity_group_intents_actor_type'),
)
sqlalchemy.Index('ix_capacity_group_intents_intent_hash',
                 GROUP_INTENTS.c.group_id, GROUP_INTENTS.c.intent_hash)

GROUPS.append_constraint(
    sqlalchemy.ForeignKeyConstraint(
        ['group_id', 'workspace', 'current_intent_generation'], [
            'capacity_group_intents.group_id',
            'capacity_group_intents.workspace',
            'capacity_group_intents.intent_generation',
        ],
        name='fk_capacity_groups_current_intent',
        ondelete='NO ACTION',
        deferrable=True,
        initially='DEFERRED'))

ALLOCATIONS = sqlalchemy.Table(
    'capacity_allocations',
    METADATA,
    sqlalchemy.Column('allocation_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('group_id', postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('created_by_intent_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('source_kind', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_key', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('source_incarnation_hash',
                      sqlalchemy.CHAR(64),
                      nullable=False),
    sqlalchemy.Column('identity_confidence', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('spec_schema_version', sqlalchemy.Integer),
    sqlalchemy.Column('physical_spec', postgresql.JSONB),
    sqlalchemy.Column('physical_spec_hash', sqlalchemy.CHAR(64)),
    sqlalchemy.Column('cluster_name', sqlalchemy.Text),
    sqlalchemy.Column('cluster_hash', sqlalchemy.Text),
    sqlalchemy.Column('lifecycle_state',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='active'),
    sqlalchemy.Column('retired_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('projection_state',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='current'),
    sqlalchemy.Column('observed_state',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='unknown'),
    sqlalchemy.Column('observation_certainty',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='legacy'),
    sqlalchemy.Column('observed_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('last_seen_scan_id', postgresql.UUID(as_uuid=True)),
    sqlalchemy.Column('source_missing_at', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.Column('updated_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.PrimaryKeyConstraint('allocation_id',
                                    name='pk_capacity_allocations'),
    sqlalchemy.UniqueConstraint(
        'group_id',
        'workspace',
        'allocation_id',
        name='uq_capacity_allocations_group_workspace_id'),
    sqlalchemy.UniqueConstraint(
        'group_id',
        'source_kind',
        'source_key',
        'source_incarnation_hash',
        name='uq_capacity_allocations_source_incarnation'),
    sqlalchemy.ForeignKeyConstraint(
        ['group_id', 'workspace'],
        ['capacity_groups.group_id', 'capacity_groups.workspace'],
        name='fk_capacity_allocations_group',
        ondelete='RESTRICT'),
    sqlalchemy.ForeignKeyConstraint(
        ['group_id', 'workspace', 'created_by_intent_generation'], [
            'capacity_group_intents.group_id',
            'capacity_group_intents.workspace',
            'capacity_group_intents.intent_generation',
        ],
        name='fk_capacity_allocations_birth_intent',
        ondelete='RESTRICT'),
    sqlalchemy.ForeignKeyConstraint(
        ['last_seen_scan_id'], ['capacity_projection_scans.scan_id'],
        name='fk_capacity_allocations_last_seen_scan',
        ondelete='SET NULL'),
    sqlalchemy.CheckConstraint('created_by_intent_generation >= 1',
                               name='ck_capacity_allocations_birth_generation'),
    sqlalchemy.CheckConstraint(
        "source_kind IN "
        "('serve_replica', 'pool_worker', 'managed_job_cluster')",
        name='ck_capacity_allocations_source_kind'),
    sqlalchemy.CheckConstraint(
        f'source_incarnation_hash ~ {_LOWERCASE_SHA256}',
        name='ck_capacity_allocations_source_incarnation_hash'),
    sqlalchemy.CheckConstraint(
        "identity_confidence IN ('exact', 'legacy', 'unknown')",
        name='ck_capacity_allocations_identity_confidence'),
    sqlalchemy.CheckConstraint(
        '(spec_schema_version IS NULL AND physical_spec IS NULL '
        'AND physical_spec_hash IS NULL) OR '
        '(spec_schema_version IS NOT NULL AND physical_spec IS NOT NULL '
        'AND physical_spec_hash IS NOT NULL)',
        name='ck_capacity_allocations_spec_triple'),
    sqlalchemy.CheckConstraint(
        'spec_schema_version IS NULL OR spec_schema_version >= 1',
        name='ck_capacity_allocations_spec_schema_version'),
    sqlalchemy.CheckConstraint(
        "physical_spec IS NULL OR jsonb_typeof(physical_spec) = 'object'",
        name='ck_capacity_allocations_spec_object'),
    sqlalchemy.CheckConstraint(
        f'physical_spec_hash IS NULL OR '
        f'physical_spec_hash ~ {_LOWERCASE_SHA256}',
        name='ck_capacity_allocations_spec_hash'),
    sqlalchemy.CheckConstraint(
        "identity_confidence != 'exact' OR "
        '(spec_schema_version IS NOT NULL AND physical_spec IS NOT NULL '
        'AND physical_spec_hash IS NOT NULL)',
        name='ck_capacity_allocations_exact_spec'),
    sqlalchemy.CheckConstraint(
        "lifecycle_state IN ('active', 'retiring', 'retired')",
        name='ck_capacity_allocations_lifecycle_state'),
    sqlalchemy.CheckConstraint(
        "(lifecycle_state = 'retired') = (retired_at IS NOT NULL)",
        name='ck_capacity_allocations_retired_at'),
    sqlalchemy.CheckConstraint(
        "projection_state IN ('current', 'source_missing', 'quarantined')",
        name='ck_capacity_allocations_projection_state'),
    sqlalchemy.CheckConstraint(
        "observed_state IN ('unknown', 'provisioning', 'up', 'stopped', "
        "'absent', 'failed', 'partial')",
        name='ck_capacity_allocations_observed_state'),
    sqlalchemy.CheckConstraint(
        "observation_certainty IN ('legacy', 'registry', 'provider')",
        name='ck_capacity_allocations_observation_certainty'),
)
sqlalchemy.Index(
    'uq_capacity_allocations_active_cluster_hash',
    ALLOCATIONS.c.cluster_hash,
    unique=True,
    postgresql_where=sqlalchemy.text(
        "cluster_hash IS NOT NULL AND lifecycle_state != 'retired'"))
sqlalchemy.Index('ix_capacity_allocations_group_lifecycle',
                 ALLOCATIONS.c.workspace, ALLOCATIONS.c.group_id,
                 ALLOCATIONS.c.lifecycle_state)
sqlalchemy.Index('ix_capacity_allocations_projection_missing',
                 ALLOCATIONS.c.workspace, ALLOCATIONS.c.projection_state,
                 ALLOCATIONS.c.source_missing_at)

ALLOCATION_DESIRES = sqlalchemy.Table(
    'capacity_allocation_desires',
    METADATA,
    sqlalchemy.Column('group_id', postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('intent_generation',
                      sqlalchemy.BigInteger,
                      nullable=False),
    sqlalchemy.Column('allocation_id',
                      postgresql.UUID(as_uuid=True),
                      nullable=False),
    sqlalchemy.Column('ordinal', sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column('desired_state', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('release_gate',
                      sqlalchemy.Text,
                      nullable=False,
                      server_default='blocked'),
    sqlalchemy.Column('reason_code', sqlalchemy.Text, nullable=False),
    sqlalchemy.Column('created_at',
                      sqlalchemy.DateTime(timezone=True),
                      nullable=False,
                      server_default=sqlalchemy.func.clock_timestamp()),
    sqlalchemy.PrimaryKeyConstraint('group_id',
                                    'intent_generation',
                                    'allocation_id',
                                    name='pk_capacity_allocation_desires'),
    sqlalchemy.UniqueConstraint(
        'group_id',
        'workspace',
        'intent_generation',
        'ordinal',
        name='uq_capacity_allocation_desires_workspace_ordinal'),
    sqlalchemy.ForeignKeyConstraint(
        ['group_id', 'workspace', 'intent_generation'], [
            'capacity_group_intents.group_id',
            'capacity_group_intents.workspace',
            'capacity_group_intents.intent_generation',
        ],
        name='fk_capacity_allocation_desires_intent',
        ondelete='RESTRICT'),
    sqlalchemy.ForeignKeyConstraint(
        ['group_id', 'workspace', 'allocation_id'], [
            'capacity_allocations.group_id',
            'capacity_allocations.workspace',
            'capacity_allocations.allocation_id',
        ],
        name='fk_capacity_allocation_desires_allocation',
        ondelete='RESTRICT'),
    sqlalchemy.CheckConstraint(
        'intent_generation >= 1',
        name='ck_capacity_allocation_desires_generation'),
    sqlalchemy.CheckConstraint('ordinal >= 0',
                               name='ck_capacity_allocation_desires_ordinal'),
    sqlalchemy.CheckConstraint(
        "desired_state IN ('present', 'stopped', 'absent')",
        name='ck_capacity_allocation_desires_desired_state'),
    sqlalchemy.CheckConstraint(
        "release_gate IN ('blocked', 'open')",
        name='ck_capacity_allocation_desires_release_gate'),
    sqlalchemy.CheckConstraint(
        "reason_code IN ('projection', 'carry_forward', 'scale_up', "
        "'replacement', 'recovery', 'scale_down', 'teardown')",
        name='ck_capacity_allocation_desires_reason_code'),
    sqlalchemy.CheckConstraint(
        "release_gate = 'blocked' OR desired_state = 'absent'",
        name='ck_capacity_allocation_desires_release_safety'),
)

__all__ = [
    'ALLOCATION_DESIRES',
    'ALLOCATIONS',
    'GROUP_INTENTS',
    'GROUPS',
    'METADATA',
    'PROJECTION_SCANS',
]
