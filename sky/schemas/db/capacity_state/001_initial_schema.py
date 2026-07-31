"""Create the read-only physical-capacity projection foundation.

Revision ID: 001
Revises:
Create Date: 2026-07-30

"""
# pylint: disable=invalid-name
from collections.abc import Sequence

from alembic import op
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_PROJECTION_SCANS = 'capacity_projection_scans'
_GROUPS = 'capacity_groups'
_GROUP_INTENTS = 'capacity_group_intents'
_ALLOCATIONS = 'capacity_allocations'
_ALLOCATION_DESIRES = 'capacity_allocation_desires'

_ACTOR_TYPES = "('system', 'basic', 'sa', 'sso', 'legacy', 'unknown')"
_LOWERCASE_SHA256 = r"'^[0-9a-f]{64}$'"

_FK_GROUPS_LAST_SEEN_SCAN = 'fk_capacity_groups_last_seen_scan'
_FK_GROUPS_CURRENT_INTENT = 'fk_capacity_groups_current_intent'
_FK_GROUP_INTENTS_GROUP = 'fk_capacity_group_intents_group'
_FK_ALLOCATIONS_GROUP = 'fk_capacity_allocations_group'
_FK_ALLOCATIONS_BIRTH_INTENT = 'fk_capacity_allocations_birth_intent'
_FK_ALLOCATIONS_LAST_SEEN_SCAN = 'fk_capacity_allocations_last_seen_scan'
_FK_DESIRES_INTENT = 'fk_capacity_allocation_desires_intent'
_FK_DESIRES_ALLOCATION = 'fk_capacity_allocation_desires_allocation'


def _require_postgresql() -> None:
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        raise RuntimeError(
            'The physical-capacity projection store is PostgreSQL-only.')


def upgrade() -> None:
    """Create revision 001 physical-capacity projection tables."""
    _require_postgresql()

    op.create_table(
        _PROJECTION_SCANS,
        sqlalchemy.Column('scan_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=False),
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
        sqlalchemy.Column('controller_instance_id',
                          postgresql.UUID(as_uuid=True)),
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
        sqlalchemy.CheckConstraint(
            "state IN ('running', 'completed', 'failed')",
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
        sqlalchemy.CheckConstraint(
            'rows_seen >= 0', name='ck_capacity_projection_scans_rows_seen'),
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
    op.create_index('uq_capacity_projection_scans_running_partition',
                    _PROJECTION_SCANS,
                    ['workspace', 'source_kind', 'source_partition_hash'],
                    unique=True,
                    postgresql_where=sqlalchemy.text("state = 'running'"))
    op.create_index(
        'ix_capacity_projection_scans_completed', _PROJECTION_SCANS,
        ['workspace', 'source_kind',
         sqlalchemy.text('completed_at DESC')])
    op.create_index('ix_capacity_projection_scans_state_updated',
                    _PROJECTION_SCANS, ['state', 'updated_at'])

    op.create_table(
        _GROUPS,
        sqlalchemy.Column('group_id',
                          postgresql.UUID(as_uuid=True),
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
        sqlalchemy.Column('projection_confidence',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('current_intent_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('lifecycle_state',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='active'),
        sqlalchemy.Column('last_seen_scan_id', postgresql.UUID(as_uuid=True)),
        sqlalchemy.Column('source_missing_at',
                          sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.Column('retired_at', sqlalchemy.DateTime(timezone=True)),
        sqlalchemy.Column('created_by_actor_id',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('updated_by_actor_id',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('created_by_actor_type',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('updated_by_actor_type',
                          sqlalchemy.Text,
                          nullable=False),
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
        sqlalchemy.UniqueConstraint(
            'workspace',
            'source_kind',
            'source_key',
            'source_incarnation_hash',
            name='uq_capacity_groups_source_incarnation'),
        sqlalchemy.ForeignKeyConstraint(['last_seen_scan_id'],
                                        [f'{_PROJECTION_SCANS}.scan_id'],
                                        name=_FK_GROUPS_LAST_SEEN_SCAN,
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
        sqlalchemy.CheckConstraint(
            f'created_by_actor_type IN {_ACTOR_TYPES}',
            name='ck_capacity_groups_created_actor_type'),
        sqlalchemy.CheckConstraint(
            f'updated_by_actor_type IN {_ACTOR_TYPES}',
            name='ck_capacity_groups_updated_actor_type'),
        sqlalchemy.CheckConstraint(
            "projection_confidence != 'exact' OR "
            '(owner_id IS NOT NULL AND owner_incarnation IS NOT NULL '
            "AND writer_fence_kind != 'legacy')",
            name='ck_capacity_groups_exact_confidence'),
    )
    op.create_index(
        'uq_capacity_groups_exact_owner',
        _GROUPS, ['workspace', 'owner_kind', 'owner_id', 'owner_incarnation'],
        unique=True,
        postgresql_where=sqlalchemy.text("projection_confidence = 'exact' "
                                         "AND lifecycle_state != 'retired'"))

    op.create_table(
        _GROUP_INTENTS,
        sqlalchemy.Column('group_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=False),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('intent_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('schema_version',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='1'),
        sqlalchemy.Column('placement_contract',
                          postgresql.JSONB,
                          nullable=False),
        sqlalchemy.Column('placement_contract_hash',
                          sqlalchemy.CHAR(64),
                          nullable=False),
        sqlalchemy.Column('desired_count', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column('topology', postgresql.JSONB, nullable=False),
        sqlalchemy.Column('intent_hash', sqlalchemy.CHAR(64), nullable=False),
        sqlalchemy.Column('source_fingerprint',
                          sqlalchemy.CHAR(64),
                          nullable=False),
        sqlalchemy.Column('created_by_actor_id',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('created_by_actor_type',
                          sqlalchemy.Text,
                          nullable=False),
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
            [f'{_GROUPS}.group_id', f'{_GROUPS}.workspace'],
            name=_FK_GROUP_INTENTS_GROUP,
            ondelete='NO ACTION',
            deferrable=True,
            initially='DEFERRED'),
        sqlalchemy.CheckConstraint('intent_generation >= 1',
                                   name='ck_capacity_group_intents_generation'),
        sqlalchemy.CheckConstraint(
            'schema_version >= 1',
            name='ck_capacity_group_intents_schema_version'),
        sqlalchemy.CheckConstraint(
            "jsonb_typeof(placement_contract) = 'object'",
            name='ck_capacity_group_intents_placement_object'),
        sqlalchemy.CheckConstraint(
            f'placement_contract_hash ~ {_LOWERCASE_SHA256}',
            name='ck_capacity_group_intents_placement_hash'),
        sqlalchemy.CheckConstraint(
            'desired_count >= 0',
            name='ck_capacity_group_intents_desired_count'),
        sqlalchemy.CheckConstraint(
            "jsonb_typeof(topology) = 'object'",
            name='ck_capacity_group_intents_topology_object'),
        sqlalchemy.CheckConstraint(
            f'intent_hash ~ {_LOWERCASE_SHA256}',
            name='ck_capacity_group_intents_intent_hash'),
        sqlalchemy.CheckConstraint(
            f'source_fingerprint ~ {_LOWERCASE_SHA256}',
            name='ck_capacity_group_intents_source_fingerprint'),
        sqlalchemy.CheckConstraint(f'created_by_actor_type IN {_ACTOR_TYPES}',
                                   name='ck_capacity_group_intents_actor_type'),
    )
    op.create_index('ix_capacity_group_intents_intent_hash', _GROUP_INTENTS,
                    ['group_id', 'intent_hash'])

    op.create_foreign_key(
        _FK_GROUPS_CURRENT_INTENT,
        _GROUPS,
        _GROUP_INTENTS, ['group_id', 'workspace', 'current_intent_generation'],
        ['group_id', 'workspace', 'intent_generation'],
        ondelete='NO ACTION',
        deferrable=True,
        initially='DEFERRED')

    op.create_table(
        _ALLOCATIONS,
        sqlalchemy.Column('allocation_id',
                          postgresql.UUID(as_uuid=True),
                          nullable=False),
        sqlalchemy.Column('group_id',
                          postgresql.UUID(as_uuid=True),
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
        sqlalchemy.Column('identity_confidence',
                          sqlalchemy.Text,
                          nullable=False),
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
        sqlalchemy.Column('source_missing_at',
                          sqlalchemy.DateTime(timezone=True)),
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
            [f'{_GROUPS}.group_id', f'{_GROUPS}.workspace'],
            name=_FK_ALLOCATIONS_GROUP,
            ondelete='RESTRICT'),
        sqlalchemy.ForeignKeyConstraint(
            ['group_id', 'workspace', 'created_by_intent_generation'], [
                f'{_GROUP_INTENTS}.group_id',
                f'{_GROUP_INTENTS}.workspace',
                f'{_GROUP_INTENTS}.intent_generation',
            ],
            name=_FK_ALLOCATIONS_BIRTH_INTENT,
            ondelete='RESTRICT'),
        sqlalchemy.ForeignKeyConstraint(['last_seen_scan_id'],
                                        [f'{_PROJECTION_SCANS}.scan_id'],
                                        name=_FK_ALLOCATIONS_LAST_SEEN_SCAN,
                                        ondelete='SET NULL'),
        sqlalchemy.CheckConstraint(
            'created_by_intent_generation >= 1',
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
    op.create_index(
        'uq_capacity_allocations_active_cluster_hash',
        _ALLOCATIONS, ['cluster_hash'],
        unique=True,
        postgresql_where=sqlalchemy.text(
            "cluster_hash IS NOT NULL AND lifecycle_state != 'retired'"))
    op.create_index('ix_capacity_allocations_group_lifecycle', _ALLOCATIONS,
                    ['workspace', 'group_id', 'lifecycle_state'])
    op.create_index('ix_capacity_allocations_projection_missing', _ALLOCATIONS,
                    ['workspace', 'projection_state', 'source_missing_at'])

    op.create_table(
        _ALLOCATION_DESIRES,
        sqlalchemy.Column('group_id',
                          postgresql.UUID(as_uuid=True),
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
                f'{_GROUP_INTENTS}.group_id',
                f'{_GROUP_INTENTS}.workspace',
                f'{_GROUP_INTENTS}.intent_generation',
            ],
            name=_FK_DESIRES_INTENT,
            ondelete='RESTRICT'),
        sqlalchemy.ForeignKeyConstraint(
            ['group_id', 'workspace', 'allocation_id'], [
                f'{_ALLOCATIONS}.group_id',
                f'{_ALLOCATIONS}.workspace',
                f'{_ALLOCATIONS}.allocation_id',
            ],
            name=_FK_DESIRES_ALLOCATION,
            ondelete='RESTRICT'),
        sqlalchemy.CheckConstraint(
            'intent_generation >= 1',
            name='ck_capacity_allocation_desires_generation'),
        sqlalchemy.CheckConstraint(
            'ordinal >= 0', name='ck_capacity_allocation_desires_ordinal'),
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


def downgrade() -> None:
    """Drop revision 001 only when all capacity tables are empty."""
    _require_postgresql()
    bind = op.get_bind()
    tables = (
        _ALLOCATION_DESIRES,
        _ALLOCATIONS,
        _GROUP_INTENTS,
        _GROUPS,
        _PROJECTION_SCANS,
    )
    bind.execute(
        sqlalchemy.text('LOCK TABLE ' + ', '.join(tables) +
                        ' IN ACCESS EXCLUSIVE MODE'))
    nonempty = [
        table for table in tables if int(
            bind.execute(sqlalchemy.text(
                f'SELECT COUNT(*) FROM {table}')).scalar_one())
    ]
    if nonempty:
        raise RuntimeError(
            'Cannot downgrade the physical-capacity projection schema while '
            f'tables contain rows: {", ".join(nonempty)}.')

    op.drop_constraint(_FK_GROUPS_CURRENT_INTENT, _GROUPS, type_='foreignkey')
    op.drop_constraint(_FK_GROUP_INTENTS_GROUP,
                       _GROUP_INTENTS,
                       type_='foreignkey')
    op.drop_table(_ALLOCATION_DESIRES)
    op.drop_table(_ALLOCATIONS)
    op.drop_table(_GROUP_INTENTS)
    op.drop_table(_GROUPS)
    op.drop_table(_PROJECTION_SCANS)
