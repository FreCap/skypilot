"""Characterization tests for the SkyServe state schema foundation."""
# pylint: disable=protected-access
import os
import pathlib
import subprocess
import sys

import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from sky.serve import serve_state
from sky.serve import serve_state_schema


def test_serve_state_schema_uses_one_metadata_graph():
    expected_tables = {
        'demand_capacity_observations',
        'ephemeral_storage_cleanup_intents',
        'paid_capacity_claims',
        'paid_capacity_pools',
        'paid_capacity_waiters',
        'placement_normalization_rows',
        'placement_normalization_runs',
        'replicas',
        'reserved_fill_claims',
        'reserved_fill_lease',
        'reserved_fill_pool_claims',
        'reserved_fill_protocol_state',
        'reserved_fill_rounds',
        'reserved_fill_service_claim_sets',
        'serve_ha_recovery_script',
        'service_lifecycle_fences',
        'services',
        'version_specs',
    }
    table_objects = (
        serve_state.demand_capacity_observations_table,
        serve_state.ephemeral_storage_cleanup_intents_table,
        serve_state.paid_capacity_claims_table,
        serve_state.paid_capacity_pools_table,
        serve_state.paid_capacity_waiters_table,
        serve_state.placement_normalization_rows_table,
        serve_state.placement_normalization_runs_table,
        serve_state.replicas_table,
        serve_state.reserved_fill_claims_table,
        serve_state.reserved_fill_lease_table,
        serve_state.reserved_fill_pool_claims_table,
        serve_state.reserved_fill_protocol_state_table,
        serve_state.reserved_fill_rounds_table,
        serve_state.reserved_fill_service_claim_sets_table,
        serve_state.serve_ha_recovery_script_table,
        serve_state.service_lifecycle_fences_table,
        serve_state.services_table,
        serve_state.version_specs_table,
    )

    assert set(serve_state.Base.metadata.tables) == expected_tables
    assert all(
        table.metadata is serve_state.Base.metadata for table in table_objects)
    assert {
        table.name: table for table in table_objects
    } == serve_state.Base.metadata.tables
    assert not ({
        'serve_resource_action_shadow_samples',
        'serve_resource_action_shadow_attempts',
    } & set(serve_state.Base.metadata.tables))


def test_reserved_fill_protocol_owns_global_claim_generation():
    generation = (
        serve_state.reserved_fill_protocol_state_table.c.claim_generation)
    assert isinstance(generation.type, sqlalchemy.BigInteger)
    assert not generation.nullable
    assert str(generation.server_default.arg) == '0'


def test_reserved_fill_round_persists_optional_exact_card_feed():
    column = serve_state.reserved_fill_rounds_table.c.feed_by_accelerator
    assert isinstance(column.type, sqlalchemy.Text)
    # NULL distinguishes rounds written before exact-card telemetry from a
    # present empty allocation, which authorizes no shaped launch.
    assert column.nullable
    assert column.server_default is None


def test_version_specs_persists_nullable_controller_recovery_state():
    columns = serve_state.version_specs_table.c
    assert isinstance(columns.controller_config.type, sqlalchemy.LargeBinary)
    assert isinstance(columns.controller_config_digest.type, sqlalchemy.Text)
    assert isinstance(columns.controller_config_snapshot_id.type,
                      sqlalchemy.Text)
    assert columns.controller_config.nullable
    assert columns.controller_config_digest.nullable
    assert columns.controller_config_snapshot_id.nullable
    assert isinstance(columns.controller_applied_at.type, sqlalchemy.Float)
    assert columns.controller_applied_at.nullable


def test_services_persist_explicit_placement_normalization_receipts():
    columns = serve_state.services_table.c
    uuid_fields = (
        'placement_normalization_requested_run_id',
        'placement_normalization_loaded_run_id',
    )
    for field in uuid_fields:
        assert isinstance(columns[field].type, sqlalchemy.Uuid)
        assert columns[field].type.as_uuid
        assert columns[field].nullable
        foreign_key = next(iter(columns[field].foreign_keys))
        assert foreign_key.target_fullname == (
            'placement_normalization_runs.run_id')
        assert foreign_key.ondelete == 'RESTRICT'
    for field in ('placement_normalization_loaded_image_commit',
                  'placement_normalization_loaded_controller_ip',
                  'placement_normalization_loaded_boot_id'):
        assert isinstance(columns[field].type, sqlalchemy.Text)
        assert columns[field].nullable
    assert isinstance(
        columns.placement_normalization_loaded_controller_pid.type,
        sqlalchemy.Integer)
    assert isinstance(columns.placement_normalization_loaded_at.type,
                      sqlalchemy.Float)
    assert columns.placement_normalization_loaded_controller_pid.nullable
    assert columns.placement_normalization_loaded_at.nullable


def test_version_specs_retirement_state_is_explicit_and_atomic():
    columns = serve_state.version_specs_table.c
    assert isinstance(columns.retired_yaml_content.type, sqlalchemy.Text)
    assert isinstance(columns.retired_at.type, sqlalchemy.Float)
    assert isinstance(columns.retirement_reason.type, sqlalchemy.Text)
    assert isinstance(columns.retirement_run_id.type, sqlalchemy.Uuid)
    assert columns.retirement_run_id.type.as_uuid
    retirement_run_foreign_key = next(
        iter(columns.retirement_run_id.foreign_keys))
    assert retirement_run_foreign_key.target_fullname == (
        'placement_normalization_runs.run_id')
    assert retirement_run_foreign_key.ondelete == 'RESTRICT'
    for field in ('retired_yaml_content', 'retired_at', 'retirement_reason',
                  'retirement_run_id'):
        assert columns[field].nullable
    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in serve_state.version_specs_table.constraints
        if isinstance(constraint, sqlalchemy.CheckConstraint)
    }
    retirement = checks['ck_version_specs_retirement_all_or_none']
    assert 'retired_at IS NULL' in retirement
    assert 'retired_at IS NOT NULL' in retirement
    assert 'yaml_content IS NULL' in retirement
    assert 'retirement_run_id IS NOT NULL' in retirement


def test_placement_normalization_ledger_is_digest_only_and_run_scoped():
    runs = serve_state.placement_normalization_runs_table
    rows = serve_state.placement_normalization_rows_table
    assert tuple(
        column.name for column in runs.primary_key.columns) == ('run_id',)
    assert tuple(
        column.name for column in rows.primary_key.columns) == ('run_id',
                                                                'service_name',
                                                                'version')
    assert isinstance(runs.c.run_id.type, sqlalchemy.Uuid)
    assert isinstance(rows.c.run_id.type, sqlalchemy.Uuid)
    assert runs.c.run_id.type.as_uuid
    assert rows.c.run_id.type.as_uuid
    run_foreign_key = next(iter(rows.c.run_id.foreign_keys))
    assert run_foreign_key.target_fullname == (
        'placement_normalization_runs.run_id')
    assert run_foreign_key.constraint.name == (
        'fk_placement_normalization_rows_run')
    assert run_foreign_key.ondelete == 'RESTRICT'
    assert {
        'classification_counts',
        'pre_inventory_sha256',
        'post_inventory_sha256',
        'freeze_evidence_sha256',
    } <= set(runs.c.keys())
    assert {
        'classification',
        'outcome',
        'original_spec_sha256',
        'result_spec_sha256',
        'original_row_sha256',
        'result_row_sha256',
        'original_column_sha256s',
        'result_column_sha256s',
        'contract_projection',
        'service_hash',
        'service_lifecycle_epoch',
        'dependency_facts',
    } <= set(rows.c.keys())
    assert {
        'spec', 'yaml_content', 'submitted_yaml_content', 'controller_config'
    }.isdisjoint(rows.c.keys())
    assert isinstance(runs.c.classification_counts.type, sqlalchemy.JSON)
    for field in ('original_column_sha256s', 'result_column_sha256s',
                  'contract_projection', 'dependency_facts'):
        assert isinstance(rows.c[field].type, sqlalchemy.JSON)
    assert {
        'ck_placement_normalization_run_mode',
        'ck_placement_normalization_run_times',
        'ck_placement_normalization_run_row_bound',
        'ck_placement_normalization_run_digests',
    } <= {constraint.name for constraint in runs.constraints}
    assert {
        'ck_placement_normalization_row_outcome',
        'ck_placement_normalization_row_classification',
        'ck_placement_normalization_row_digests',
    } <= {constraint.name for constraint in rows.constraints}


def test_reserved_fill_protocol_persists_rollout_inventory_evidence():
    protocol = serve_state.reserved_fill_protocol_state_table.c
    assert isinstance(protocol.deployment_uid.type, sqlalchemy.Text)
    assert isinstance(protocol.pod_inventory_count.type, sqlalchemy.Integer)
    assert isinstance(protocol.pod_inventory_sha256.type, sqlalchemy.Text)
    assert protocol.deployment_uid.nullable
    assert protocol.pod_inventory_count.nullable
    assert protocol.pod_inventory_sha256.nullable
    constraint_names = {
        constraint.name for constraint in
        serve_state.reserved_fill_protocol_state_table.constraints
    }
    assert {
        'ck_reserved_fill_protocol_proof_all_or_none',
        'ck_reserved_fill_protocol_v2_has_proof',
        'ck_reserved_fill_protocol_image_digest',
        'ck_reserved_fill_protocol_deployment_generation',
        'ck_reserved_fill_protocol_deployment_uid',
        'ck_reserved_fill_protocol_pod_inventory_count',
        'ck_reserved_fill_protocol_pod_inventory_sha256',
    } <= constraint_names


def test_resource_action_existing_table_columns_are_dialect_portable():
    mode = serve_state.services_table.c.resource_action_mode
    changed_at = serve_state.services_table.c.resource_action_mode_changed_at
    assert not mode.nullable
    assert str(mode.server_default.arg) == 'legacy'
    assert isinstance(changed_at.type, sqlalchemy.DateTime)
    assert changed_at.type.timezone
    assert changed_at.type.compile(dialect=sqlite.dialect()) == 'DATETIME'
    assert (changed_at.type.compile(
        dialect=postgresql.dialect()) == 'TIMESTAMP WITH TIME ZONE')

    uuid_columns = {
        'replica_incarnation',
        'sky_cluster_record_uuid',
        'launch_action_id',
        'down_action_id',
        'launch_shadow_coverage_id',
        'down_shadow_coverage_id',
        'launch_shadow_sample_id',
        'down_shadow_sample_id',
        'ordinary_launch_association_id',
    }
    assert serve_state._ACTION_OWNED_REPLICA_COLUMNS == uuid_columns | {
        'desired_generation',
        'resource_action_spec_identity_sha256',
    }
    assert set(serve_state._LEGACY_REPLICA_ROW_COLUMNS).isdisjoint(
        serve_state._ACTION_OWNED_REPLICA_COLUMNS)
    assert set(serve_state._LEGACY_REPLICA_ROW_COLUMNS) <= set(
        serve_state.replicas_table.c.keys())
    for name in uuid_columns:
        column_type = serve_state.replicas_table.c[name].type
        assert isinstance(column_type, sqlalchemy.Uuid)
        assert column_type.as_uuid
        assert column_type.compile(dialect=sqlite.dialect()) == 'CHAR(32)'
        assert column_type.compile(dialect=postgresql.dialect()) == 'UUID'
    assert isinstance(serve_state.replicas_table.c.desired_generation.type,
                      sqlalchemy.BigInteger)


def test_controller_incarnation_uses_only_a_dialect_native_server_default():
    column = serve_state.services_table.c.controller_incarnation
    assert column.default is None
    assert column.server_default is not None
    postgres_default = str(
        column.server_default.arg.compile(dialect=postgresql.dialect()))
    sqlite_default = str(
        column.server_default.arg.compile(dialect=sqlite.dialect()))
    assert postgres_default == 'gen_random_uuid()'
    assert 'randomblob' in sqlite_default


def test_serve_state_database_manager_owns_historical_bootstrap():
    assert serve_state.Base is serve_state_schema.Base
    assert serve_state._db_manager is serve_state_schema._db_manager
    assert serve_state.create_table is serve_state_schema.create_table
    assert (serve_state.ensure_tables_initialized
            is serve_state_schema.ensure_tables_initialized)
    assert (serve_state.get_database_engine
            is serve_state_schema.get_database_engine)
    assert serve_state._db_manager._db_name == 'serve/services'
    assert serve_state._db_manager._create_table_fn is serve_state.create_table
    assert serve_state.create_table.__module__ == 'sky.serve.serve_state'
    assert (serve_state.ensure_tables_initialized.__module__ ==
            'sky.serve.serve_state')
    assert (
        serve_state.get_database_engine.__module__ == 'sky.serve.serve_state')


def test_schema_first_import_keeps_one_foundation():
    repo_root = pathlib.Path(__file__).parents[2]
    script = """
from sky.serve import serve_state_schema
schema_base = serve_state_schema.Base
schema_manager = serve_state_schema._db_manager
from sky.serve import serve_state
assert serve_state.Base is schema_base
assert serve_state._db_manager is schema_manager
assert serve_state.create_table is serve_state_schema.create_table
assert serve_state.Base.metadata is serve_state_schema.Base.metadata
"""
    env = os.environ.copy()
    env['PYTHONPATH'] = str(repo_root)
    subprocess.run([sys.executable, '-c', script],
                   cwd=repo_root,
                   env=env,
                   check=True)
