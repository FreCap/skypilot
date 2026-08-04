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
        'replicas',
        'reserved_fill_claims',
        'reserved_fill_lease',
        'reserved_fill_rounds',
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
        serve_state.replicas_table,
        serve_state.reserved_fill_claims_table,
        serve_state.reserved_fill_lease_table,
        serve_state.reserved_fill_rounds_table,
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
    }
    assert serve_state._ACTION_OWNED_REPLICA_COLUMNS == uuid_columns | {
        'desired_generation'
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
