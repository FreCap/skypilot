"""Characterization tests for live-cluster control-plane read facades."""

# pylint: disable=protected-access

import inspect
import typing
from unittest import mock

import sqlalchemy
from sqlalchemy import event

from sky import global_user_state
from sky import models
from sky.skylet import constants
from sky.utils import status_lib
from sky.utils.db import db_utils


class _MinimalHandle:
    """Just enough for global_user_state to persist and project a handle."""

    launched_resources = None


def _fresh_db(tmp_path, monkeypatch):
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    manager = db_utils.DatabaseManager(
        'state',
        global_user_state.create_table,
        post_init_fn=lambda _: global_user_state._sqlite_supports_returning(),  # pylint: disable=protected-access
    )
    monkeypatch.setattr(global_user_state, '_db_manager', manager)
    return manager


def _add_cluster(name: str, *, ready: bool = True) -> None:
    global_user_state.add_or_update_cluster(
        cluster_name=name,
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=ready,
    )


def _set_cluster_user_hash(name: str, user_hash: str | None) -> None:
    engine = global_user_state._db_manager.get_engine()  # pylint: disable=protected-access
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(global_user_state.cluster_table).where(
                global_user_state.cluster_table.c.name == name).values(
                    user_hash=user_hash))


def test_cluster_control_plane_read_facade_contracts() -> None:
    exact = global_user_state.get_cluster_from_name
    assert exact.__module__ == 'sky.global_user_state'
    assert exact.__name__ == 'get_cluster_from_name'
    exact_signature = inspect.signature(exact)
    assert tuple(exact_signature.parameters) == (
        'cluster_name',
        'include_user_info',
        'summary_response',
    )
    assert exact_signature.parameters[
        'cluster_name'].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert all(
        exact_signature.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        for name in ('include_user_info', 'summary_response'))
    assert [
        parameter.default for parameter in exact_signature.parameters.values()
    ] == [inspect.Parameter.empty, True, False]
    assert typing.get_type_hints(exact) == {
        'cluster_name': str | None,
        'include_user_info': bool,
        'summary_response': bool,
        'return': dict[str, typing.Any] | None,
    }

    batched = global_user_state.get_clusters_from_names
    assert batched.__module__ == 'sky.global_user_state'
    assert batched.__name__ == 'get_clusters_from_names'
    batched_signature = inspect.signature(batched)
    assert tuple(batched_signature.parameters) == (
        'cluster_names',
        'include_user_info',
    )
    assert batched_signature.parameters[
        'cluster_names'].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert batched_signature.parameters[
        'include_user_info'].kind is inspect.Parameter.KEYWORD_ONLY
    assert [
        parameter.default
        for parameter in batched_signature.parameters.values()
    ] == [inspect.Parameter.empty, False]
    assert typing.get_type_hints(batched) == {
        'cluster_names': list[str],
        'include_user_info': bool,
        'return': dict[str, dict[str, typing.Any] | None],
    }


def test_exact_read_preserves_summary_verbose_projection_and_decode_budget(
        tmp_path, monkeypatch) -> None:
    manager = _fresh_db(tmp_path, monkeypatch)
    global_user_state.add_or_update_user(models.User(id='user-a', name='Alice'))
    _add_cluster('exact')
    _set_cluster_user_hash('exact', 'user-a')
    engine = manager.get_engine()
    statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _capture_selects(_conn, _cursor, statement, *_args):
        if statement.lstrip().upper().startswith('SELECT'):
            statements.append(statement)

    marker_event = object()
    with (mock.patch.object(global_user_state,
                            'get_terminal_or_last_status_change_event',
                            return_value=marker_event) as event_lookup,
          mock.patch.object(global_user_state.pickle,
                            'loads',
                            wraps=global_user_state.pickle.loads) as
          pickle_loads,
          mock.patch.object(global_user_state.json,
                            'loads',
                            wraps=global_user_state.json.loads) as json_loads,
          mock.patch.object(global_user_state,
                            '_load_owner',
                            wraps=global_user_state._load_owner) as load_owner):
        summary = global_user_state.get_cluster_from_name(
            'exact', include_user_info=True, summary_response=True)
        verbose = global_user_state.get_cluster_from_name(
            'exact', include_user_info=True, summary_response=False)

    assert summary is not None and verbose is not None
    assert summary['status'] is status_lib.ClusterStatus.UP
    assert summary['user_hash'] == 'user-a'
    assert summary['user_name'] == 'Alice'
    assert set(verbose) == set(summary) | {
        'last_creation_yaml',
        'last_creation_command',
        'last_event',
    }
    assert verbose['last_event'] is marker_event
    assert len(statements) == 2
    assert event_lookup.call_args_list == [mock.call(verbose['cluster_hash'])]
    assert pickle_loads.call_count == 2
    assert json_loads.call_count == 2
    assert load_owner.call_count == 2


def test_batched_read_preserves_cardinality_user_snapshot_and_decode_budget(
        tmp_path, monkeypatch) -> None:
    manager = _fresh_db(tmp_path, monkeypatch)
    monkeypatch.setattr(global_user_state, '_CLUSTER_IN_QUERY_CHUNK_SIZE', 2)
    global_user_state.add_or_update_user(models.User(id='user-a', name='Alice'))
    _add_cluster('explicit')
    _set_cluster_user_hash('explicit', 'user-a')
    _add_cluster('legacy')
    _set_cluster_user_hash('legacy', None)
    monkeypatch.setattr(global_user_state.common_utils, 'get_user_hash',
                        lambda: 'user-a')
    engine = manager.get_engine()
    statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _capture_selects(_conn, _cursor, statement, *_args):
        if statement.lstrip().upper().startswith('SELECT'):
            statements.append(statement)

    with (mock.patch.object(global_user_state.pickle,
                            'loads',
                            wraps=global_user_state.pickle.loads) as
          pickle_loads,
          mock.patch.object(global_user_state.json,
                            'loads',
                            wraps=global_user_state.json.loads) as json_loads,
          mock.patch.object(global_user_state,
                            '_load_owner',
                            wraps=global_user_state._load_owner) as load_owner):
        records = global_user_state.get_clusters_from_names(
            ['explicit', 'legacy', 'missing'], include_user_info=True)

    assert list(records) == ['explicit', 'legacy', 'missing']
    assert records['missing'] is None
    assert records['explicit'] is not None
    assert records['legacy'] is not None
    assert records['explicit']['user_hash'] == 'user-a'
    assert records['explicit']['user_name'] == 'Alice'
    assert records['legacy']['user_hash'] == 'user-a'
    assert records['legacy']['user_name'] == 'Alice'
    assert len(statements) == 3
    assert sum('FROM users' in statement for statement in statements) == 1
    assert pickle_loads.call_count == 2
    assert json_loads.call_count == 2
    assert load_owner.call_count == 2
