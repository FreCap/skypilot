"""Add managed container image distribution state.

Revision ID: 023
Revises: 022
Create Date: 2026-07-13

"""
# pylint: disable=invalid-name
from collections.abc import Sequence
import time
import uuid

from alembic import op
import sqlalchemy

from sky.utils.db import db_utils

# revision identifiers, used by Alembic.
revision: str = '023'
down_revision: str | Sequence[str] | None = '022'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CATALOG_ROW_ID = 'authority'
_MIGRATION_LOCK = 'skypilot:global-user-state:023:container-images'
_TABLE_NAMES = (
    'container_image_catalog',
    'container_image_profile_revisions',
    'container_image_operations',
    'container_images',
    'container_image_sources',
    'container_image_publications',
    'container_image_provider_budgets',
    'container_image_registry_shards',
    'container_image_locations',
    'container_image_demands',
    'container_image_consumer_watermarks',
    'container_image_workers',
)
_DROP_TABLE_NAMES = (
    'container_image_workers',
    'container_image_consumer_watermarks',
    'container_image_demands',
    'container_image_publications',
    'container_image_locations',
    'container_image_registry_shards',
    'container_image_provider_budgets',
    'container_image_sources',
    'container_images',
    'container_image_operations',
    'container_image_profile_revisions',
    'container_image_catalog',
)


def _lock_migration() -> None:
    op.execute(
        sqlalchemy.text('SELECT pg_advisory_xact_lock(hashtext(:name))').
        bindparams(name=_MIGRATION_LOCK))


def _add_cluster_binding_columns() -> None:
    db_utils.add_column_to_table_alembic('clusters',
                                         'container_image_binding_known',
                                         sqlalchemy.Integer(),
                                         server_default='0')
    db_utils.add_column_to_table_alembic('clusters',
                                         'container_image_consumer_kind',
                                         sqlalchemy.Text())
    db_utils.add_column_to_table_alembic('clusters',
                                         'container_image_consumer_owner',
                                         sqlalchemy.Text())


def _drop_cluster_binding_columns() -> None:
    db_utils.drop_column_from_table_alembic('clusters',
                                            'container_image_consumer_owner')
    db_utils.drop_column_from_table_alembic('clusters',
                                            'container_image_consumer_kind')
    db_utils.drop_column_from_table_alembic('clusters',
                                            'container_image_binding_known')


def _create_tables() -> None:
    op.create_table(
        'container_image_catalog',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('authority_id',
                          sqlalchemy.Text,
                          nullable=False,
                          unique=True),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
    )
    op.create_table(
        'container_image_profile_revisions',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('profile', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('revision', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column('desired_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('config_hash', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('config_json', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('terraform_hash', sqlalchemy.Text),
        sqlalchemy.Column('physical_manifest_hash',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('attestations_json',
                          sqlalchemy.Text,
                          nullable=False,
                          server_default='{}'),
        sqlalchemy.Column('attestations_hash', sqlalchemy.Text),
        sqlalchemy.Column('qualified_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('failed_code', sqlalchemy.Text),
        sqlalchemy.Column('canary_window_day', sqlalchemy.Text),
        sqlalchemy.Column('canary_reserved_microusd',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('max_daily_canary_microusd',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint('workspace',
                                    'profile',
                                    'revision',
                                    name='uq_container_image_profile_revision'),
        sqlalchemy.UniqueConstraint(
            'workspace',
            'profile',
            'desired_generation',
            name='uq_container_image_profile_generation'),
        sqlalchemy.CheckConstraint(
            "state IN ('QUALIFYING', 'ACTIVE', 'FAILED', 'SUPERSEDED', "
            "'RETIRED')",
            name='ck_container_image_profile_revision_state'),
        sqlalchemy.CheckConstraint('revision > 0 AND desired_generation > 0',
                                   name='ck_container_image_profile_positive'),
        sqlalchemy.CheckConstraint(
            'canary_reserved_microusd >= 0 AND '
            'max_daily_canary_microusd >= 0 AND '
            'canary_reserved_microusd <= max_daily_canary_microusd',
            name='ck_container_image_profile_canary_budget'),
    )
    op.create_index('uq_container_image_profile_desired',
                    'container_image_profile_revisions',
                    ['workspace', 'profile'],
                    unique=True,
                    postgresql_where=sqlalchemy.text("state = 'QUALIFYING'"))
    op.create_index('uq_container_image_profile_active',
                    'container_image_profile_revisions',
                    ['workspace', 'profile'],
                    unique=True,
                    postgresql_where=sqlalchemy.text("state = 'ACTIVE'"))
    op.create_index('ix_container_image_profile_state',
                    'container_image_profile_revisions',
                    ['state', 'updated_at', 'id'])

    op.create_table(
        'container_image_operations',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('authority_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('scope', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('actor_hash', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('kind', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('idempotency_key', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('request_hash', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('result_kind', sqlalchemy.Text),
        sqlalchemy.Column('result_id', sqlalchemy.Text),
        sqlalchemy.Column('result_json', sqlalchemy.Text),
        sqlalchemy.Column('error_code', sqlalchemy.Text),
        sqlalchemy.Column('lease_token', sqlalchemy.Text),
        sqlalchemy.Column('lease_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('child_launch_id', sqlalchemy.Text),
        sqlalchemy.Column('teardown_deadline', sqlalchemy.BigInteger),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('terminal_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.UniqueConstraint(
            'authority_id',
            'scope',
            'actor_hash',
            'kind',
            'idempotency_key',
            name='uq_container_image_operation_idempotency'),
        sqlalchemy.CheckConstraint(
            "state IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')",
            name='ck_container_image_operation_state'),
        sqlalchemy.CheckConstraint(
            'length(idempotency_key) BETWEEN 16 AND 128',
            name='ck_container_image_operation_key_length'),
        sqlalchemy.CheckConstraint(
            "(kind = 'PROFILE_CANARY' AND state = 'RUNNING' AND lease_token "
            "IS NOT NULL AND lease_expires_at IS NOT NULL AND "
            "teardown_deadline IS NOT NULL) OR NOT (lease_token IS NOT NULL "
            "OR lease_expires_at IS NOT NULL OR child_launch_id IS NOT NULL "
            "OR teardown_deadline IS NOT NULL)",
            name='ck_container_image_operation_canary_lease'),
        sqlalchemy.CheckConstraint(
            "(state IN ('SUCCEEDED', 'FAILED') AND terminal_expires_at IS NOT "
            "NULL) OR (state IN ('PENDING', 'RUNNING') AND "
            "terminal_expires_at IS NULL)",
            name='ck_container_image_operation_terminal_expiry'),
    )
    op.create_index('ix_container_image_operations_lookup',
                    'container_image_operations', ['scope', 'updated_at', 'id'])
    op.create_index('ix_container_image_operations_canary_queue',
                    'container_image_operations',
                    ['state', 'lease_expires_at', 'id'],
                    postgresql_where=sqlalchemy.text(
                        "kind = 'PROFILE_CANARY' AND state = 'RUNNING'"))
    op.create_index(
        'ix_container_image_operations_expiry',
        'container_image_operations', ['terminal_expires_at', 'id'],
        postgresql_where=sqlalchemy.text('terminal_expires_at IS NOT NULL'))

    op.create_table(
        'container_images',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('platform', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('config_digest', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('manifest_media_type',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('manifest_size_bytes',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('declared_size_bytes',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('creator_user_hash', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('producer_kind', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('producer_spec_hash', sqlalchemy.Text),
        sqlalchemy.Column('builder_version', sqlalchemy.Text),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint(
            'workspace',
            'runtime_digest',
            'platform',
            name='uq_container_images_runtime_identity'),
        sqlalchemy.CheckConstraint(
            'manifest_size_bytes >= 0 AND declared_size_bytes >= 0',
            name='ck_container_images_nonnegative_sizes'),
    )
    op.create_index('ix_container_images_workspace_created', 'container_images',
                    ['workspace', 'created_at', 'id'])

    op.create_table(
        'container_image_sources',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('image_id',
                          sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_images.id'),
                          nullable=False),
        sqlalchemy.Column('source_ref', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('source_root_digest', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('source_root_media_type',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('requested_platform', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('selected_child_digest',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('source_auth_binding_id', sqlalchemy.Text),
        sqlalchemy.Column('source_auth_fingerprint', sqlalchemy.Text),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint('workspace',
                                    'source_ref',
                                    'requested_platform',
                                    name='uq_container_image_source_selection'),
    )
    op.create_index('ix_container_image_sources_image',
                    'container_image_sources', ['image_id', 'created_at', 'id'])

    op.create_table(
        'container_image_provider_budgets',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('provider', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('partition', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('account', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('region', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('api_family', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('applied_rate_milli',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('burst_milli', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('tokens_milli', sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('refilled_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('blocked_until', sqlalchemy.BigInteger),
        sqlalchemy.Column('throttle_count',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint('provider',
                                    'partition',
                                    'account',
                                    'region',
                                    'api_family',
                                    name='uq_container_image_provider_budget'),
        sqlalchemy.CheckConstraint(
            'applied_rate_milli > 0 AND burst_milli > 0 AND tokens_milli >= 0 '
            'AND tokens_milli <= burst_milli',
            name='ck_container_image_provider_budget_tokens'),
    )
    op.create_index('ix_container_image_provider_budgets_blocked',
                    'container_image_provider_budgets', ['blocked_until', 'id'])

    op.create_table(
        'container_image_registry_shards',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('profile', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column(
            'profile_revision_id', sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_profile_revisions.id')),
        sqlalchemy.Column('target_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('provider', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('partition', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('account', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('region', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('shard_generation',
                          sqlalchemy.Integer,
                          nullable=False),
        sqlalchemy.Column('shard_index', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column('target_fingerprint', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('physical_fingerprint',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('eviction_enabled',
                          sqlalchemy.Boolean,
                          nullable=False),
        sqlalchemy.Column('registry', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('repository_name', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('repository_arn', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('max_manifests',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('max_declared_bytes',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('reserved_manifests',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('reserved_declared_bytes',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('observed_manifests',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('max_in_flight', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column('in_flight',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('qualified_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('last_dispatch_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('inventory_epoch',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('inventory_cursor', sqlalchemy.Text),
        sqlalchemy.Column('inventory_started_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('inventory_completed_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('inventory_lease_token', sqlalchemy.Text),
        sqlalchemy.Column('inventory_lease_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint(
            'workspace',
            'profile',
            'target_id',
            'shard_generation',
            'shard_index',
            name='uq_container_image_registry_shard_slot'),
        sqlalchemy.UniqueConstraint(
            'physical_fingerprint',
            name='uq_container_image_registry_physical'),
        sqlalchemy.CheckConstraint(
            "state IN ('PENDING', 'READY', 'FULL', 'DRIFTED', 'DISABLED')",
            name='ck_container_image_registry_shard_state'),
        sqlalchemy.CheckConstraint(
            'shard_generation >= 0 AND shard_index >= 0 AND max_manifests > 0 '
            'AND max_declared_bytes > 0 AND reserved_manifests >= 0 AND '
            'reserved_manifests <= max_manifests AND '
            'reserved_declared_bytes >= 0 AND '
            'reserved_declared_bytes <= max_declared_bytes AND '
            'observed_manifests >= 0 AND max_in_flight > 0 AND in_flight >= 0 '
            'AND in_flight <= max_in_flight',
            name='ck_container_image_registry_shard_capacity'),
        sqlalchemy.CheckConstraint(
            '(inventory_lease_token IS NULL AND inventory_lease_expires_at IS '
            'NULL) OR (inventory_lease_token IS NOT NULL AND '
            'inventory_lease_expires_at IS NOT NULL)',
            name='ck_container_image_registry_inventory_lease'),
    )
    op.create_index('ix_container_image_registry_shard_dispatch',
                    'container_image_registry_shards', [
                        'workspace', 'profile', 'target_id', 'state',
                        'last_dispatch_at', 'id'
                    ])
    op.create_index('ix_container_image_registry_shard_inventory',
                    'container_image_registry_shards',
                    ['state', 'inventory_lease_expires_at', 'id'])

    op.create_table(
        'container_image_locations',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('image_id',
                          sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_images.id'),
                          nullable=False),
        sqlalchemy.Column(
            'shard_id',
            sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_registry_shards.id'),
            nullable=False),
        sqlalchemy.Column('target_fingerprint', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('physical_fingerprint',
                          sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('canonical', sqlalchemy.Boolean, nullable=False),
        sqlalchemy.Column(
            'canonical_location_id', sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_locations.id')),
        sqlalchemy.Column('target_ref', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('lease_kind', sqlalchemy.Text),
        sqlalchemy.Column('lease_token', sqlalchemy.Text),
        sqlalchemy.Column('lease_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('attempt_count',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('next_retry_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('error_code', sqlalchemy.Text),
        sqlalchemy.Column('last_verified_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('last_used_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('inventory_epoch_seen', sqlalchemy.BigInteger),
        sqlalchemy.Column('reserved_declared_bytes',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint('image_id',
                                    'target_fingerprint',
                                    'runtime_digest',
                                    name='uq_container_image_location_target'),
        sqlalchemy.UniqueConstraint(
            'shard_id',
            'target_ref',
            name='uq_container_image_location_target_ref'),
        sqlalchemy.CheckConstraint(
            "state IN ('PENDING', 'COPYING', 'VERIFYING', 'READY', 'FAILED', "
            "'MISSING', 'EVICTING', 'EVICTED', 'QUARANTINED')",
            name='ck_container_image_location_state'),
        sqlalchemy.CheckConstraint(
            '(canonical IS TRUE AND canonical_location_id IS NULL) OR '
            '(canonical IS FALSE AND canonical_location_id IS NOT NULL)',
            name='ck_container_image_location_canonical_relation'),
        sqlalchemy.CheckConstraint(
            "(state IN ('COPYING', 'VERIFYING', 'EVICTING') AND lease_kind IS "
            "NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT "
            "NULL) OR (state NOT IN ('COPYING', 'VERIFYING', 'EVICTING') AND "
            "lease_kind IS NULL AND lease_token IS NULL AND lease_expires_at "
            "IS NULL)",
            name='ck_container_image_location_lease'),
        sqlalchemy.CheckConstraint(
            "lease_kind IS NULL OR lease_kind IN ('COPY', 'VERIFY', 'EVICT', "
            "'DELETE')",
            name='ck_container_image_location_lease_kind'),
        sqlalchemy.CheckConstraint(
            "canonical IS FALSE OR state NOT IN ('EVICTING', 'EVICTED', "
            "'QUARANTINED')",
            name='ck_container_image_location_canonical_permanent'),
        sqlalchemy.CheckConstraint(
            'attempt_count >= 0 AND reserved_declared_bytes >= 0',
            name='ck_container_image_location_nonnegative'),
    )
    op.create_index('ix_container_image_locations_queue',
                    'container_image_locations',
                    ['state', 'next_retry_at', 'lease_expires_at', 'id'])
    op.create_index('ix_container_image_locations_shard_queue',
                    'container_image_locations',
                    ['shard_id', 'state', 'next_retry_at', 'updated_at', 'id'])
    op.create_index('ix_container_image_locations_shard_readiness',
                    'container_image_locations',
                    ['shard_id', 'state', 'updated_at', 'id'])
    op.create_index(
        'ix_container_image_locations_eviction',
        'container_image_locations', [
            'shard_id', 'state',
            sqlalchemy.text(
                'COALESCE(last_used_at, last_verified_at, created_at)'), 'id'
        ],
        postgresql_where=sqlalchemy.text('canonical IS FALSE'))
    op.create_index('ix_container_image_locations_canonical',
                    'container_image_locations',
                    ['canonical_location_id', 'state', 'id'])
    op.create_index('ix_container_image_locations_artifact',
                    'container_image_locations',
                    ['image_id', 'created_at', 'id'])
    op.create_index('ix_container_image_locations_failed_canonical',
                    'container_image_locations', ['updated_at', 'id'],
                    postgresql_where=sqlalchemy.text(
                        "canonical IS TRUE AND state = 'FAILED'"))

    op.create_table(
        'container_image_publications',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column(
            'operation_id',
            sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_operations.id'),
            nullable=False),
        sqlalchemy.Column(
            'profile_revision_id',
            sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_profile_revisions.id'),
            nullable=False),
        sqlalchemy.Column('requested_release', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('reservation_active',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.true()),
        sqlalchemy.Column('source_ref', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('source_root_digest', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('requested_platform', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('source_auth_binding_id', sqlalchemy.Text),
        sqlalchemy.Column('source_auth_fingerprint', sqlalchemy.Text),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('inspection_lease_token', sqlalchemy.Text),
        sqlalchemy.Column('inspection_lease_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('attempt_count',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('next_retry_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('error_code', sqlalchemy.Text),
        sqlalchemy.Column('image_id', sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_images.id')),
        sqlalchemy.Column('source_id', sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_image_sources.id')),
        sqlalchemy.Column(
            'canonical_location_id', sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_locations.id')),
        sqlalchemy.Column('reservation_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('record_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.CheckConstraint(
            "state IN ('PENDING', 'INSPECTING', 'READY', 'FAILED')",
            name='ck_container_image_publication_state'),
        sqlalchemy.CheckConstraint(
            "(state = 'INSPECTING' AND inspection_lease_token IS NOT NULL AND "
            "inspection_lease_expires_at IS NOT NULL) OR (state <> "
            "'INSPECTING' AND inspection_lease_token IS NULL AND "
            "inspection_lease_expires_at IS NULL)",
            name='ck_container_image_publication_inspection_lease'),
        sqlalchemy.CheckConstraint(
            '(canonical_location_id IS NULL AND image_id IS NULL AND source_id '
            'IS NULL) OR (canonical_location_id IS NOT NULL AND image_id IS '
            'NOT NULL AND source_id IS NOT NULL)',
            name='ck_container_image_publication_binding'),
        sqlalchemy.CheckConstraint(
            "state <> 'READY' OR (reservation_active IS TRUE AND image_id IS "
            "NOT NULL AND canonical_location_id IS NOT NULL)",
            name='ck_container_image_publication_ready'),
    )
    op.create_index(
        'uq_container_image_publication_release_reservation',
        'container_image_publications', ['workspace', 'requested_release'],
        unique=True,
        postgresql_where=sqlalchemy.text('reservation_active IS TRUE'))
    op.create_index(
        'ix_container_image_publications_inspection_queue',
        'container_image_publications',
        ['state', 'next_retry_at', 'inspection_lease_expires_at', 'id'])
    op.create_index('ix_container_image_publications_canonical_queue',
                    'container_image_publications',
                    ['canonical_location_id', 'state', 'id'])
    op.create_index('ix_container_image_publications_image',
                    'container_image_publications',
                    ['image_id', 'created_at', 'id'])
    op.create_index('ix_container_image_publications_ready_release',
                    'container_image_publications',
                    ['workspace', 'requested_release', 'image_id'],
                    postgresql_where=sqlalchemy.text("state = 'READY'"))
    op.create_index(
        'ix_container_image_publications_expiry',
        'container_image_publications', ['record_expires_at', 'id'],
        postgresql_where=sqlalchemy.text('record_expires_at IS NOT NULL'))

    op.create_table(
        'container_image_demands',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('authority_id', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('workspace', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('consumer_kind', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('consumer_owner', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('request_id', sqlalchemy.Text),
        sqlalchemy.Column('consumer_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('target_key', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('owner_epoch', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('retry_epoch',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('image_id',
                          sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_images.id'),
                          nullable=False),
        sqlalchemy.Column('runtime_digest', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column(
            'profile_revision_id',
            sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_profile_revisions.id'),
            nullable=False),
        sqlalchemy.Column('target_fingerprint', sqlalchemy.Text,
                          nullable=False),
        sqlalchemy.Column('location_id',
                          sqlalchemy.Text,
                          sqlalchemy.ForeignKey('container_image_locations.id'),
                          nullable=False),
        sqlalchemy.Column('placement_json', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('pull_plan_json', sqlalchemy.Text),
        sqlalchemy.Column('state', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('error_code', sqlalchemy.Text),
        sqlalchemy.Column('consumer_attached',
                          sqlalchemy.Boolean,
                          nullable=False,
                          server_default=sqlalchemy.false()),
        sqlalchemy.Column('first_terminal_observed_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('last_terminal_observed_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('terminal_observation_count',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('terminal_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('expires_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.UniqueConstraint('workspace',
                                    'consumer_kind',
                                    'consumer_owner',
                                    'consumer_generation',
                                    'target_key',
                                    name='uq_container_image_demand_identity'),
        sqlalchemy.CheckConstraint(
            "state IN ('WARMING', 'READY', 'FAILED', 'SUPERSEDED', 'RELEASED')",
            name='ck_container_image_demand_state'),
        sqlalchemy.CheckConstraint(
            'consumer_generation >= 0 AND owner_epoch >= 0 AND retry_epoch >= '
            '0 AND terminal_observation_count >= 0',
            name='ck_container_image_demand_nonnegative'),
        sqlalchemy.CheckConstraint(
            "(state = 'READY' AND pull_plan_json IS NOT NULL) OR state <> "
            "'READY'",
            name='ck_container_image_demand_ready_plan'),
    )
    op.create_index('ix_container_image_demands_location_fence',
                    'container_image_demands', ['location_id', 'state', 'id'])
    op.create_index('ix_container_image_demands_consumer',
                    'container_image_demands', [
                        'workspace', 'consumer_kind', 'consumer_owner',
                        'consumer_generation', 'target_key'
                    ])
    op.create_index('ix_container_image_demands_owner_epoch',
                    'container_image_demands',
                    ['consumer_kind', 'owner_epoch', 'state'])
    op.create_index(
        'ix_container_image_demands_cluster_request',
        'container_image_demands', ['request_id', 'state', 'id'],
        postgresql_where=sqlalchemy.text(
            "consumer_kind = 'cluster' AND consumer_attached IS false AND "
            'request_id IS NOT NULL'))
    op.create_index('ix_container_image_demands_terminal',
                    'container_image_demands', ['state', 'expires_at', 'id'])
    op.create_index('ix_container_image_demands_reconcile',
                    'container_image_demands', ['state', 'updated_at', 'id'],
                    postgresql_where=sqlalchemy.text(
                        "state IN ('WARMING', 'READY', 'FAILED')"))

    op.create_table(
        'container_image_consumer_watermarks',
        sqlalchemy.Column('workspace', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('consumer_kind', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('consumer_owner', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('controller_epoch', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('controller_sequence', sqlalchemy.BigInteger),
        sqlalchemy.Column('owner_epoch', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('max_seen_generation',
                          sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('max_terminal_generation',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='-1'),
        sqlalchemy.Column('owner_deleted_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('created_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('updated_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.CheckConstraint(
            '(controller_sequence IS NULL OR controller_sequence >= 0) AND '
            'owner_epoch >= 0 AND max_seen_generation >= 0 AND '
            'max_terminal_generation >= -1 AND '
            'max_terminal_generation <= max_seen_generation',
            name='ck_container_image_consumer_watermark_generation'),
        sqlalchemy.CheckConstraint(
            'length(controller_epoch) BETWEEN 1 AND 1024',
            name='ck_container_image_consumer_controller_epoch'),
    )
    op.create_index(
        'ix_container_image_consumer_watermarks_compaction',
        'container_image_consumer_watermarks', ['owner_deleted_at'],
        postgresql_where=sqlalchemy.text('owner_deleted_at IS NOT NULL'))

    op.create_table(
        'container_image_workers',
        sqlalchemy.Column('id', sqlalchemy.Text, primary_key=True),
        sqlalchemy.Column('kind', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('version', sqlalchemy.Text, nullable=False),
        sqlalchemy.Column('started_at', sqlalchemy.BigInteger, nullable=False),
        sqlalchemy.Column('heartbeat_at', sqlalchemy.BigInteger,
                          nullable=False),
        sqlalchemy.Column('last_success_at', sqlalchemy.BigInteger),
        sqlalchemy.Column('in_flight',
                          sqlalchemy.Integer,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('max_in_flight', sqlalchemy.Integer, nullable=False),
        sqlalchemy.Column(
            'grant_budget_id', sqlalchemy.Text,
            sqlalchemy.ForeignKey('container_image_provider_budgets.id')),
        sqlalchemy.Column('grant_tokens_milli',
                          sqlalchemy.BigInteger,
                          nullable=False,
                          server_default='0'),
        sqlalchemy.Column('grant_expires_at', sqlalchemy.BigInteger),
        sqlalchemy.CheckConstraint("kind IN ('COPY', 'LIFECYCLE', 'CANARY')",
                                   name='ck_container_image_worker_kind'),
        sqlalchemy.CheckConstraint(
            'in_flight >= 0 AND max_in_flight > 0 AND in_flight <= '
            'max_in_flight AND grant_tokens_milli >= 0',
            name='ck_container_image_worker_capacity'),
        sqlalchemy.CheckConstraint(
            '(grant_budget_id IS NULL AND grant_tokens_milli = 0 AND '
            'grant_expires_at IS NULL) OR (grant_budget_id IS NOT NULL AND '
            'grant_tokens_milli > 0 AND grant_expires_at IS NOT NULL)',
            name='ck_container_image_worker_grant'),
    )
    op.create_index('ix_container_image_workers_kind_heartbeat',
                    'container_image_workers', ['kind', 'heartbeat_at', 'id'])


def upgrade():
    """Bind cluster consumers and create PostgreSQL image-plane state."""
    bind = op.get_bind()
    is_postgres = (
        bind.dialect.name == db_utils.SQLAlchemyDialect.POSTGRESQL.value)
    if is_postgres:
        _lock_migration()
    _add_cluster_binding_columns()
    if not is_postgres:
        return
    _create_tables()
    bind.execute(
        sqlalchemy.text(
            'INSERT INTO container_image_catalog '
            '(id, authority_id, created_at) VALUES (:id, :authority, :created)'
        ), {
            'id': _CATALOG_ROW_ID,
            'authority': str(uuid.uuid4()),
            'created': int(time.time()),
        })


def downgrade():
    """Drop unshipped image state after a literal empty-state proof."""
    bind = op.get_bind()
    if bind.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
        _drop_cluster_binding_columns()
        return
    _lock_migration()
    for table_name in _TABLE_NAMES[1:]:
        count = bind.execute(
            sqlalchemy.text(f'SELECT count(*) FROM {table_name}')).scalar_one()
        if count != 0:
            raise RuntimeError(
                'Migration 023 downgrade requires all operational managed '
                f'image tables to be empty; {table_name} contains rows.')
    catalog_rows = bind.execute(
        sqlalchemy.text(
            'SELECT id, authority_id FROM container_image_catalog')).all()
    if (len(catalog_rows) != 1 or catalog_rows[0][0] != _CATALOG_ROW_ID or
            not catalog_rows[0][1]):
        raise RuntimeError(
            'Migration 023 downgrade requires exactly the expected catalog '
            'authority singleton.')
    bind.execute(
        sqlalchemy.text('DELETE FROM container_image_catalog WHERE id = :id'),
        {'id': _CATALOG_ROW_ID})
    for table_name in _DROP_TABLE_NAMES:
        op.drop_table(table_name)
    _drop_cluster_binding_columns()
