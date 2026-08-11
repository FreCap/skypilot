"""Characterization tests for the cluster-history report facade."""

import inspect
from types import SimpleNamespace
import typing

import sqlalchemy

from sky import global_user_state
from sky.skylet import constants
from sky.utils import status_lib
from sky.utils.db import db_utils


class _MinimalHandle:
    """Just enough for global_user_state.add_or_update_cluster to pickle."""

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


def test_history_report_facade_contract():
    function = global_user_state.get_clusters_from_history
    signature = inspect.signature(function)

    assert tuple(signature.parameters) == (
        'days',
        'abbreviate_response',
        'cluster_hashes',
        'cluster_names',
        'exclude_managed_clusters',
    )
    assert [parameter.default for parameter in signature.parameters.values()
           ] == [None, False, None, None, False]
    assert typing.get_type_hints(function) == {
        'days': int | None,
        'abbreviate_response': bool,
        'cluster_hashes': list[str] | None,
        'cluster_names': list[str] | None,
        'exclude_managed_clusters': bool,
        'return': list[dict[str, typing.Any]],
    }
    assert function.__module__ == 'sky.global_user_state'
    assert function.__name__ == 'get_clusters_from_history'


def test_history_report_batches_enrichment_and_preserves_projection(
        tmp_path, monkeypatch):
    clock = {'now': 200}
    monkeypatch.setattr(global_user_state.time, 'time', lambda: clock['now'])
    monkeypatch.setattr(global_user_state.common_utils, 'get_user_hash',
                        lambda: 'fallback-user')
    manager = _fresh_db(tmp_path, monkeypatch)
    global_user_state.add_or_update_cluster(
        cluster_name='regular',
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=True,
    )
    engine = manager.get_engine()
    with global_user_state.orm.Session(engine) as session:
        session.query(global_user_state.cluster_history_table).update({
            global_user_state.cluster_history_table.c.user_hash: None,
            global_user_state.cluster_history_table.c.usage_intervals:
                global_user_state.pickle.dumps([(200, None)]),
            global_user_state.cluster_history_table.c.workspace: 'history-workspace',
            global_user_state.cluster_history_table.c.node_names: '[["node-a"]]',
        })
        session.commit()

    user_calls = []
    event_calls = []

    def get_users(user_hashes):
        user_calls.append(user_hashes)
        return {'fallback-user': SimpleNamespace(name='Fallback User')}

    marker_event = object()

    def get_events(cluster_hashes):
        event_calls.append(cluster_hashes)
        return {next(iter(cluster_hashes)): marker_event}

    monkeypatch.setattr(global_user_state, 'get_users', get_users)
    monkeypatch.setattr(global_user_state,
                        '_get_last_or_terminal_cluster_event_multiple',
                        get_events)
    clock['now'] = 260
    statements = []

    def before_cursor_execute(_conn, _cursor, statement, _parameters, _context,
                              _executemany):
        statements.append(statement)

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            before_cursor_execute)
    try:
        records = global_user_state.get_clusters_from_history()
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute',
                                before_cursor_execute)

    assert len([
        statement for statement in statements
        if statement.lstrip().upper().startswith('SELECT')
    ]) == 1
    history_select = statements[0].lower()
    assert 'last_creation_yaml' not in history_select
    assert 'last_creation_command' not in history_select
    assert user_calls == [{'fallback-user'}]
    assert len(event_calls) == 1
    assert len(event_calls[0]) == 1
    assert len(records) == 1
    record = records[0]
    assert record['name'] == 'regular'
    assert record['duration'] == 60
    assert record['status'] is status_lib.ClusterStatus.UP
    assert record['user_hash'] == 'fallback-user'
    assert record['user_name'] == 'Fallback User'
    assert record['workspace'] == 'history-workspace'
    assert record['last_event'] is marker_event
    assert record['node_names'] == 'node-a'
    assert record['last_creation_yaml'] is None
    assert record['last_creation_command'] is None


def test_history_report_preserves_lookback_identifier_or_and_sorting(
        tmp_path, monkeypatch):
    clock = {'now': 200_000}
    monkeypatch.setattr(global_user_state.time, 'time', lambda: clock['now'])
    manager = _fresh_db(tmp_path, monkeypatch)
    cluster_hashes = {}
    for name in ('active', 'recent', 'old'):
        cluster_hashes[name] = global_user_state.add_or_update_cluster(
            cluster_name=name,
            cluster_handle=_MinimalHandle(),
            requested_resources=set(),
            ready=True,
        )

    engine = manager.get_engine()
    with global_user_state.orm.Session(engine) as session:
        session.query(
            global_user_state.cluster_history_table
        ).filter_by(cluster_hash=cluster_hashes['recent']).update({
            global_user_state.cluster_history_table.c.last_activity_time: 150_000,
            global_user_state.cluster_history_table.c.launched_at: 150_000,
        })
        session.query(
            global_user_state.cluster_history_table
        ).filter_by(cluster_hash=cluster_hashes['old']).update({
            global_user_state.cluster_history_table.c.last_activity_time: 100_000,
            global_user_state.cluster_history_table.c.launched_at: 100_000,
        })
        session.query(global_user_state.cluster_table).filter(
            global_user_state.cluster_table.c.name.in_(
                ('recent', 'old'))).delete(synchronize_session=False)
        session.commit()

    records = global_user_state.get_clusters_from_history(days=1)
    assert {record['name'] for record in records} == {'active', 'recent'}

    records = global_user_state.get_clusters_from_history(
        cluster_hashes=[cluster_hashes['active']], cluster_names=['old'])
    assert [record['name'] for record in records] == ['active', 'old']


def test_history_report_preserves_corrupt_pickle_fallback(
        tmp_path, monkeypatch):
    manager = _fresh_db(tmp_path, monkeypatch)
    cluster_hash = global_user_state.add_or_update_cluster(
        cluster_name='corrupt',
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=True,
    )
    engine = manager.get_engine()
    with global_user_state.orm.Session(engine) as session:
        session.query(
            global_user_state.cluster_history_table
        ).filter_by(cluster_hash=cluster_hash).update({
            global_user_state.cluster_history_table.c.usage_intervals: b'not-a-pickle',
            global_user_state.cluster_history_table.c.launched_resources: b'not-a-pickle',
        })
        session.commit()

    records = global_user_state.get_clusters_from_history(
        cluster_hashes=[cluster_hash])
    assert len(records) == 1
    assert records[0]['usage_intervals'] == []
    assert records[0]['duration'] == 0
    assert records[0]['resources'] is None
    assert records[0]['priority'] is None
    assert records[0]['priority_class'] is None
