"""Unit tests for the batched cluster-lookup helpers in global_user_state.

These helpers exist so pool dashboard (and any future caller iterating many
replicas) can avoid the per-name DB round-trip that would otherwise show up
as a double N+1 inside ReplicaInfo.to_info_dict.
"""
# pylint: disable=protected-access
from unittest import mock

import sqlalchemy
from sqlalchemy import event

from sky import global_user_state
from sky import models
from sky.skylet import constants
from sky.utils.db import db_utils


def _fresh_db(tmp_path, monkeypatch):
    """Point the global state DB at a tmp sqlite file (mirrors the helper in
    test_global_user_state_cluster_events.py)."""
    monkeypatch.setenv(constants.SKY_RUNTIME_DIR_ENV_VAR_KEY, str(tmp_path))
    monkeypatch.setattr(
        global_user_state,
        '_db_manager',
        db_utils.DatabaseManager(
            'state',
            global_user_state.create_table,
            post_init_fn=lambda _: global_user_state._sqlite_supports_returning(
            ),
        ),
    )


class _MinimalHandle:
    """Just enough for global_user_state.add_or_update_cluster to pickle."""
    launched_resources = None


def _add_cluster(name: str,
                 *,
                 is_managed: bool = False,
                 ready: bool = False,
                 workload_type: str | None = None,
                 workload_id: str | None = None) -> None:
    global_user_state.add_or_update_cluster(
        cluster_name=name,
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=ready,
        is_managed=is_managed,
        workload_type=workload_type,
        workload_id=workload_id,
    )


def _set_cluster_user_hash(name: str, user_hash: str | None) -> None:
    engine = global_user_state._db_manager.get_engine()
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.update(global_user_state.cluster_table).where(
                global_user_state.cluster_table.c.name == name).values(
                    user_hash=user_hash))


def test_get_clusters_from_names_empty_input_returns_empty(
        tmp_path, monkeypatch):
    """Empty input must NOT hit the DB and must return {} so callers can
    safely pass empty lists from comprehensions."""
    _fresh_db(tmp_path, monkeypatch)
    # No mocks needed — the function returns before touching the engine.
    assert not global_user_state.get_clusters_from_names([])


def test_get_clusters_from_names_returns_record_per_name(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('alive-1')
    _add_cluster('alive-2')

    result = global_user_state.get_clusters_from_names(['alive-1', 'alive-2'])

    assert set(result.keys()) == {'alive-1', 'alive-2'}
    assert result['alive-1'] is not None
    assert result['alive-2'] is not None
    assert result['alive-1']['name'] == 'alive-1'
    assert result['alive-2']['name'] == 'alive-2'
    # The batched helper intentionally only supports the summary shape, so
    # the verbose-only fields are never present.
    assert 'last_creation_yaml' not in result['alive-1']
    assert 'last_creation_command' not in result['alive-1']
    assert 'last_event' not in result['alive-1']


def test_get_clusters_from_names_missing_names_become_none(
        tmp_path, monkeypatch):
    """Names that don't exist in the cluster table must appear in the result
    mapped to None — callers (e.g. _get_service_status) rely on this to know
    which replicas need a handle fallback lookup."""
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('alive-1')

    result = global_user_state.get_clusters_from_names(
        ['alive-1', 'gone-1', 'gone-2'])

    assert result['alive-1'] is not None
    assert result['gone-1'] is None
    assert result['gone-2'] is None


def test_get_handles_from_cluster_names_chunks_large_input(
        tmp_path, monkeypatch):
    """`get_handles_from_cluster_names` shares the IN-chunking knob with
    `get_clusters_from_names`. Same setup: shrink chunk size to 2, insert 5
    clusters, verify all five names resolve through the loop."""
    _fresh_db(tmp_path, monkeypatch)
    monkeypatch.setattr(global_user_state, '_CLUSTER_IN_QUERY_CHUNK_SIZE', 2)
    names = [f'c-{i}' for i in range(5)]
    for name in names:
        _add_cluster(name)
    engine = global_user_state._db_manager.get_engine()
    select_statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _count_selects(_conn, _cursor, statement, *_args):
        if statement.lstrip().upper().startswith('SELECT'):
            select_statements.append(statement)

    # Missing names just don't appear in the result (this helper doesn't
    # None-fill, matching its existing contract).
    try:
        result = global_user_state.get_handles_from_cluster_names(
            set(names) | {'missing'})
    finally:
        event.remove(engine, 'before_cursor_execute', _count_selects)

    assert set(result.keys()) == set(names)
    for name in names:
        # Each handle round-trips through pickle.loads, so we only assert it
        # came back as the right type rather than identity.
        assert isinstance(result[name], _MinimalHandle), name
    assert len(select_statements) == 3


def test_get_handles_from_cluster_names_retries_transient_db_failure(
        tmp_path, monkeypatch):
    """JobGroup recovery must not lose the single-row helper's retry budget
    when it switches to the batched handle snapshot."""
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('alive-1')
    real_session = global_user_state.orm.Session
    attempts = 0

    def flaky_session(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sqlalchemy.exc.OperationalError(statement='SELECT handle',
                                                  params={},
                                                  orig=Exception('db blip'))
        return real_session(*args, **kwargs)

    monkeypatch.setattr(global_user_state.orm, 'Session', flaky_session)
    monkeypatch.setattr(global_user_state.db_retries.time, 'sleep',
                        lambda _delay: None)

    result = global_user_state.get_handles_from_cluster_names({'alive-1'})

    assert attempts == 2
    assert isinstance(result['alive-1'], _MinimalHandle)


def test_get_clusters_from_names_chunks_large_input(tmp_path, monkeypatch):
    """Names beyond a single batch must still be resolved. Shrink the chunk
    size to 2 so we exercise the loop with only 5 clusters."""
    _fresh_db(tmp_path, monkeypatch)
    monkeypatch.setattr(global_user_state, '_CLUSTER_IN_QUERY_CHUNK_SIZE', 2)
    names = [f'c-{i}' for i in range(5)]
    for name in names:
        _add_cluster(name)
    # Also include a missing name to confirm the None-fill behavior survives
    # chunking.
    queried = names + ['missing']

    result = global_user_state.get_clusters_from_names(queried)

    assert set(result.keys()) == set(queried)
    for name in names:
        assert result[name] is not None, name
        assert result[name]['name'] == name
    assert result['missing'] is None


def test_get_cluster_status_fields_has_chunk_bounded_query_count(
        tmp_path, monkeypatch):
    """Status snapshots use one SELECT per chunk, never one per name."""
    _fresh_db(tmp_path, monkeypatch)
    monkeypatch.setattr(global_user_state, '_CLUSTER_IN_QUERY_CHUNK_SIZE', 2)
    names = [f'c-{i}' for i in range(5)]
    for name in names:
        _add_cluster(name)

    engine = global_user_state._db_manager.get_engine()
    select_statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _count_selects(_conn, _cursor, statement, *_args):
        if statement.lstrip().upper().startswith('SELECT'):
            select_statements.append(statement)

    try:
        assert global_user_state.get_cluster_status_fields([]) == {}
        assert not select_statements
        result = global_user_state.get_cluster_status_fields(names +
                                                             ['missing'])
    finally:
        event.remove(engine, 'before_cursor_execute', _count_selects)

    assert set(result) == set(names)
    assert all(status == 'INIT' for status, _ in result.values())
    assert len(select_statements) == 3


def test_get_cluster_status_fields_all_unmanaged_uses_one_select(
        tmp_path, monkeypatch):
    """The refresh sweep can select and order unmanaged clusters from one
    plain-column snapshot instead of reading names first."""
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('user-a')
    _add_cluster('managed-a', is_managed=True)
    _add_cluster('user-b')

    engine = global_user_state._db_manager.get_engine()
    select_statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _count_selects(_conn, _cursor, statement, *_args):
        if statement.lstrip().upper().startswith('SELECT'):
            select_statements.append(statement)

    try:
        result = global_user_state.get_cluster_status_fields(
            None, exclude_managed_clusters=True)
    finally:
        event.remove(engine, 'before_cursor_execute', _count_selects)

    assert set(result) == {'user-a', 'user-b'}
    assert len(select_statements) == 1


def test_get_managed_cluster_status_fields_filters_workload_type(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('user-service', workload_type='service')
    _add_cluster('managed-service', is_managed=True, workload_type='service')
    _add_cluster('managed-pool', is_managed=True, workload_type='pool')
    _add_cluster('managed-job', is_managed=True, workload_type='managed_job')
    _add_cluster('managed-legacy', is_managed=True)

    result = global_user_state.get_managed_cluster_status_fields('service')

    assert set(result) == {'managed-service'}
    assert result['managed-service'][0] == 'INIT'


def test_get_managed_job_cluster_cleanup_candidates_includes_legacy(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('user-cluster')
    _add_cluster('managed-service', is_managed=True, workload_type='service')
    _add_cluster('managed-pool', is_managed=True, workload_type='pool')
    _add_cluster('managed-job',
                 is_managed=True,
                 workload_type='managed_job',
                 workload_id='42')
    _add_cluster('managed-legacy', is_managed=True)

    result = global_user_state.get_managed_job_cluster_cleanup_candidates()

    assert result == {
        'managed-job': '42',
        'managed-legacy': None,
    }


def test_get_clusters_from_names_matches_single_helper(tmp_path, monkeypatch):
    """Batched record for an existing cluster must match the single-name
    helper field-for-field in summary mode, so callers can swap one for the
    other without subtle diffs."""
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('alive-1')

    batched = global_user_state.get_clusters_from_names(
        ['alive-1'], include_user_info=False)['alive-1']
    single = global_user_state.get_cluster_from_name('alive-1',
                                                     include_user_info=False,
                                                     summary_response=True)

    # handle is a freshly unpickled instance each call (no __eq__ defined on
    # _MinimalHandle), so identity comparison fails. Verify the rest match
    # field-for-field, and the handle type is at least the same.
    assert batched is not None and single is not None
    assert set(batched.keys()) == set(single.keys())
    for key in batched:
        if key == 'handle':
            assert type(batched[key]) is type(single[key])  # noqa: E721
        else:
            assert batched[key] == single[key], f'mismatch on {key}'


def test_get_clusters_current_user_filter_includes_legacy_null_user_hash(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    global_user_state.add_or_update_user(models.User(id='user-a', name='Alice'))
    global_user_state.add_or_update_user(models.User(id='user-b', name='Bob'))
    _add_cluster('owned-by-user-a', ready=True)
    _set_cluster_user_hash('owned-by-user-a', 'user-a')
    _add_cluster('legacy-null-user', ready=True)
    _set_cluster_user_hash('legacy-null-user', None)

    engine = global_user_state._db_manager.get_engine()
    select_statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _count_selects(_conn, _cursor, statement, *_args):
        if statement.lstrip().upper().startswith('SELECT'):
            select_statements.append(statement)

    try:
        with mock.patch('sky.global_user_state.common_utils.get_user_hash',
                        return_value='user-a'):
            records = global_user_state.get_clusters(
                user_hashes_filter={'user-a'}, summary_response=True)
    finally:
        event.remove(engine, 'before_cursor_execute', _count_selects)

    names = {record['name'] for record in records}
    assert names == {'legacy-null-user', 'owned-by-user-a'}
    legacy_record = next(
        record for record in records if record['name'] == 'legacy-null-user')
    assert legacy_record['user_hash'] == 'user-a'
    assert legacy_record['user_name'] == 'Alice'
    assert len(select_statements) == 2


def test_get_clusters_other_user_filter_excludes_legacy_null_user_hash(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    global_user_state.add_or_update_user(models.User(id='user-a', name='Alice'))
    global_user_state.add_or_update_user(models.User(id='user-b', name='Bob'))
    _add_cluster('owned-by-user-b', ready=True)
    _set_cluster_user_hash('owned-by-user-b', 'user-b')
    _add_cluster('legacy-null-user', ready=True)
    _set_cluster_user_hash('legacy-null-user', None)

    engine = global_user_state._db_manager.get_engine()
    select_statements = []

    @event.listens_for(engine, 'before_cursor_execute')
    def _count_selects(_conn, _cursor, statement, *_args):
        if statement.lstrip().upper().startswith('SELECT'):
            select_statements.append(statement)

    try:
        with mock.patch('sky.global_user_state.common_utils.get_user_hash',
                        return_value='user-a'):
            records = global_user_state.get_clusters(
                user_hashes_filter={'user-b'}, summary_response=True)
    finally:
        event.remove(engine, 'before_cursor_execute', _count_selects)

    assert [record['name'] for record in records] == ['owned-by-user-b']
    assert len(select_statements) == 1
