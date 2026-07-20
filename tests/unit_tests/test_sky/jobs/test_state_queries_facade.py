"""Characterization tests for the managed-jobs query facade."""

# pylint: disable=protected-access

import inspect

from sky.jobs import state
from sky.jobs import state_queries
from sky.jobs import state_storage

_EXPECTED_PARAMETERS = {
    '_get_jobs_dict': ('r',),
    '_map_response_field_to_db_column': ('field',),
    'get_managed_jobs_total': (),
    'build_managed_jobs_with_filters_no_status_query': (
        'fields',
        'job_ids',
        'accessible_workspaces',
        'workspace_match',
        'name_match',
        'pool_match',
        'user_hashes',
        'skip_finished',
        'submitted_after',
        'submitted_before',
        'count_only',
        'count_unique_jobs',
        'status_count',
        'status_expr',
    ),
    'build_managed_jobs_with_filters_query': (
        'fields',
        'job_ids',
        'accessible_workspaces',
        'workspace_match',
        'name_match',
        'pool_match',
        'user_hashes',
        'statuses',
        'skip_finished',
        'submitted_after',
        'submitted_before',
        'count_only',
        'count_unique_jobs',
        'status_expr',
    ),
    'get_status_count_with_filters': (
        'fields',
        'job_ids',
        'accessible_workspaces',
        'workspace_match',
        'name_match',
        'pool_match',
        'user_hashes',
        'skip_finished',
        'submitted_after',
        'submitted_before',
        'status_expr',
    ),
    'get_status_counts': (),
    'get_status_counts_by_workspace_user_cloud': (),
    'get_managed_jobs_with_filters': (
        'fields',
        'job_ids',
        'accessible_workspaces',
        'workspace_match',
        'name_match',
        'pool_match',
        'user_hashes',
        'statuses',
        'skip_finished',
        'submitted_after',
        'submitted_before',
        'page',
        'limit',
        'sort_by',
        'sort_order',
        'status_expr',
    ),
}


def test_state_query_facade_signatures():
    actual = {
        name: tuple(inspect.signature(getattr(state, name)).parameters)
        for name in _EXPECTED_PARAMETERS
    }
    assert actual == _EXPECTED_PARAMETERS


def test_batch_progress_subquery_contract():
    assert state._batch_progress_subquery.name == 'batch_progress'
    assert tuple(state._batch_progress_subquery.c.keys()) == (
        'job_id',
        'batch_total_batches',
        'batch_completed_batches',
    )


def test_state_query_facade_uses_direct_aliases_and_shared_storage():
    for name in _EXPECTED_PARAMETERS:
        assert getattr(state, name) is getattr(state_queries, name)

    assert state._batch_progress_subquery is (
        state_queries._batch_progress_subquery)
    assert state_queries._db_manager is state_storage.db_manager
    assert state_queries.logger.name == state.logger.name == 'sky.jobs.state'
    assert state_queries.batch_state_table is state.batch_state_table
    assert state_queries.job_info_table is state.job_info_table
    assert state_queries.spot_table is state.spot_table
