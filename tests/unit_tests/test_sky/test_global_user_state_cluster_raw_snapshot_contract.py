"""Characterization tests for raw cluster snapshot read facades."""

# pylint: disable=protected-access

import inspect
import typing
from unittest import mock

import pytest
import sqlalchemy
from sqlalchemy import event

from sky import global_user_state
from sky.skylet import constants
from sky.utils.db import db_utils


class _MinimalHandle:
    """Just enough for global_user_state to persist a cluster."""

    launched_resources = None


def _fresh_db(tmp_path, monkeypatch) -> db_utils.DatabaseManager:
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    manager = db_utils.DatabaseManager(
        'state',
        global_user_state.create_table,
        post_init_fn=lambda _: global_user_state._sqlite_supports_returning(),
    )
    monkeypatch.setattr(global_user_state, '_db_manager', manager)
    return manager


def _add_cluster(name: str,
                 *,
                 is_managed: bool = False,
                 workload_type: str | None = None,
                 workload_id: str | None = None) -> None:
    global_user_state.add_or_update_cluster(
        cluster_name=name,
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=False,
        is_managed=is_managed,
        workload_type=workload_type,
        workload_id=workload_id,
    )


def _assert_facade_contract(
    function: typing.Callable[..., typing.Any],
    function_name: str,
    parameter_names: tuple[str, ...],
    parameter_kinds: tuple[inspect._ParameterKind, ...],
    defaults: tuple[typing.Any, ...],
    type_hints: dict[str, typing.Any],
) -> None:
    assert function.__module__ == 'sky.global_user_state'
    assert function.__name__ == function.__qualname__ == function_name
    signature = inspect.signature(function)
    assert tuple(signature.parameters) == parameter_names
    assert tuple(
        parameter.kind
        for parameter in signature.parameters.values()) == parameter_kinds
    assert tuple(parameter.default
                 for parameter in signature.parameters.values()) == defaults
    assert typing.get_type_hints(function) == type_hints


def test_raw_cluster_snapshot_facade_contracts() -> None:
    empty = inspect.Parameter.empty
    positional = inspect.Parameter.POSITIONAL_OR_KEYWORD
    keyword_only = inspect.Parameter.KEYWORD_ONLY

    _assert_facade_contract(
        global_user_state.get_cluster_status_fields,
        'get_cluster_status_fields',
        ('cluster_names', 'exclude_managed_clusters'),
        (positional, keyword_only),
        (empty, False),
        {
            'cluster_names': list[str] | None,
            'exclude_managed_clusters': bool,
            'return': dict[str, tuple[str | None, int | None]],
        },
    )
    _assert_facade_contract(
        global_user_state.get_cluster_status_fields_by_prefix,
        'get_cluster_status_fields_by_prefix',
        ('cluster_name_prefix', 'row_limit'),
        (positional, keyword_only),
        (empty, empty),
        {
            'cluster_name_prefix': str,
            'row_limit': int,
            'return': dict[str, tuple[str | None, int | None]],
        },
    )
    _assert_facade_contract(
        global_user_state.get_managed_cluster_status_fields,
        'get_managed_cluster_status_fields',
        ('workload_type',),
        (positional,),
        (empty,),
        {
            'workload_type': str,
            'return': dict[str, global_user_state.ManagedClusterStatusFields],
        },
    )
    _assert_facade_contract(
        global_user_state.get_managed_job_cluster_cleanup_candidates,
        'get_managed_job_cluster_cleanup_candidates',
        (),
        (),
        (),
        {'return': dict[str, str | None]},
    )
    _assert_facade_contract(
        global_user_state.get_cluster_refresh_fields,
        'get_cluster_refresh_fields',
        ('cluster_name',),
        (positional,),
        (empty,),
        {
            'cluster_name': str,
            'return': global_user_state.ClusterRefreshFields | None,
        },
    )
    assert global_user_state.ManagedClusterStatusFields.__module__ == (
        'sky.global_user_state')
    assert global_user_state.ManagedClusterStatusFields._fields == (
        'status', 'status_updated_at', 'cluster_hash')
    assert global_user_state.ClusterRefreshFields.__module__ == (
        'sky.global_user_state')
    assert global_user_state.ClusterRefreshFields._fields == (
        'status', 'status_updated_at', 'autostop', 'to_down', 'cluster_hash',
        'is_managed', 'workload_type')


def test_status_and_refresh_snapshots_ignore_corrupt_serialized_columns(
        tmp_path, monkeypatch) -> None:
    manager = _fresh_db(tmp_path, monkeypatch)
    _add_cluster('corrupt')
    engine = manager.get_engine()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(global_user_state.cluster_table).where(
                global_user_state.cluster_table.c.name == 'corrupt').values(
                    handle=b'not-a-pickle',
                    owner='not-json',
                    metadata='not-json',
                    autostop=17,
                    to_down=True,
                ))

    select_statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _capture_selects(_conn, _cursor, statement, *_args):
        if statement.lstrip().upper().startswith('SELECT'):
            select_statements.append(statement)

    try:
        with (mock.patch.object(global_user_state.pickle,
                                'loads',
                                side_effect=AssertionError('pickle decoded')),
              mock.patch.object(global_user_state.json,
                                'loads',
                                side_effect=AssertionError('json decoded')),
              mock.patch.object(global_user_state,
                                '_load_owner',
                                side_effect=AssertionError('owner decoded'))):
            status = global_user_state.get_cluster_status_fields(['corrupt'])
            refresh = global_user_state.get_cluster_refresh_fields('corrupt')
    finally:
        event.remove(engine, 'before_cursor_execute', _capture_selects)

    status_value = status['corrupt']
    assert status_value[0] == 'INIT'
    assert refresh is not None
    assert refresh[:4] == (*status_value, 17, True)
    assert refresh.cluster_hash
    assert not refresh.is_managed
    assert refresh.workload_type is None
    assert len(select_statements) == 2
    assert all(
        'clusters.handle' not in statement for statement in select_statements)
    assert all(
        'clusters.owner' not in statement for statement in select_statements)
    assert all(
        'clusters.metadata' not in statement for statement in select_statements)


def test_raw_snapshot_filters_ordering_and_legacy_attribution(
        tmp_path, monkeypatch) -> None:
    manager = _fresh_db(tmp_path, monkeypatch)
    _add_cluster('reserved_%b')
    _add_cluster('reserved_%a')
    _add_cluster('reserved-ordinary')
    _add_cluster('managed-service', is_managed=True, workload_type='service')
    _add_cluster('managed-unfenced', is_managed=True, workload_type='service')
    _add_cluster('managed-pool', is_managed=True, workload_type='pool')
    _add_cluster('managed-job',
                 is_managed=True,
                 workload_type='managed_job',
                 workload_id='job-1')
    _add_cluster('managed-legacy', is_managed=True)

    engine = manager.get_engine()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(global_user_state.cluster_table).where(
                global_user_state.cluster_table.c.name ==
                'managed-unfenced').values(cluster_hash=''))
    select_statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _capture_selects(_conn, _cursor, statement, *_args):
        if statement.lstrip().upper().startswith('SELECT'):
            select_statements.append(statement)

    try:
        prefix_status = (global_user_state.get_cluster_status_fields_by_prefix(
            'reserved_%', row_limit=2))
        managed_status = (
            global_user_state.get_managed_cluster_status_fields('service'))
        cleanup_candidates = (
            global_user_state.get_managed_job_cluster_cleanup_candidates())
    finally:
        event.remove(engine, 'before_cursor_execute', _capture_selects)

    assert list(prefix_status) == ['reserved_%a', 'reserved_%b']
    assert set(managed_status) == {'managed-service'}
    managed_service = managed_status['managed-service']
    assert type(managed_service) is global_user_state.ManagedClusterStatusFields
    assert managed_service.status == 'INIT'
    assert isinstance(managed_service.status_updated_at, int)
    assert managed_service.cluster_hash
    assert cleanup_candidates == {
        'managed-job': 'job-1',
        'managed-legacy': None,
    }
    assert len(select_statements) == 3

    for prefix, row_limit in [('', 1), ('reserved', 0), ('reserved', True)]:
        with pytest.raises(ValueError, match='nonempty cluster prefix'):
            global_user_state.get_cluster_status_fields_by_prefix(
                prefix, row_limit=row_limit)
    with pytest.raises(ValueError, match='exceeds'):
        global_user_state.get_cluster_status_fields_by_prefix('reserved_%',
                                                              row_limit=1)
