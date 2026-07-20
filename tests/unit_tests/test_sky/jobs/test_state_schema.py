"""Characterization tests for the managed-jobs database schema facade."""

import importlib

from sky.jobs import state
from sky.jobs import state_schema
from sky.skylet import constants


def test_state_schema_table_registration_and_identity():
    expected_tables = {
        'spot': (state.spot_table, state_schema.spot_table),
        'job_info': (state.job_info_table, state_schema.job_info_table),
        'api_access_tokens':
            (state.api_access_token_table, state_schema.api_access_token_table),
        'ha_recovery_script': (state.ha_recovery_script_table,
                               state_schema.ha_recovery_script_table),
        'job_events': (state.job_events_table, state_schema.job_events_table),
        'batch_state':
            (state.batch_state_table, state_schema.batch_state_table),
        'batch_worker':
            (state.batch_worker_table, state_schema.batch_worker_table),
    }

    assert list(state.Base.metadata.tables) == list(expected_tables)
    assert state.Base is state_schema.Base
    for table_name, (facade_table, owner_table) in expected_tables.items():
        assert facade_table is owner_table
        assert state.Base.metadata.tables[table_name] is owner_table
        assert owner_table.metadata is state.Base.metadata


def test_state_schema_representative_constraints_and_defaults():
    assert state.spot_table.c.job_id.primary_key
    assert state.spot_table.c.status.index
    assert state.job_info_table.c.pool.index
    assert state.job_info_table.c.current_cluster_name.index
    assert str(state.job_info_table.c.priority.server_default.arg) == str(
        constants.DEFAULT_PRIORITY)
    assert str(state.job_info_table.c.execution.server_default.arg) == 'serial'
    assert [column.name for column in state.batch_state_table.primary_key
           ] == ['job_id', 'batch_idx']
    assert [column.name for column in state.batch_worker_table.primary_key
           ] == ['job_id', 'coordinator_token', 'worker_cluster']


def test_historical_migrations_keep_state_base_identity():
    migration_modules = [
        'sky.schemas.db.spot_jobs.001_initial_schema',
        'sky.schemas.db.spot_jobs.009_job_events',
        'sky.schemas.db.spot_jobs.016_add_api_access_token_id',
        'sky.schemas.db.spot_jobs.018_add_batch_state',
        'sky.schemas.db.spot_jobs.023_add_batch_coordinator_fence',
    ]

    for module_name in migration_modules:
        migration = importlib.import_module(module_name)
        assert migration.Base is state.Base
