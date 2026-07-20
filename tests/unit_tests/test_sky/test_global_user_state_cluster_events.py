"""Unit tests for cluster_event accessors in global_user_state."""
# pylint: disable=protected-access
import time

import sqlalchemy
from sqlalchemy import orm

from sky import global_user_state
from sky.skylet import constants
from sky.utils import status_lib
from sky.utils.db import db_utils


def _fresh_db(tmp_path, monkeypatch):
    """Point the global state DB at a tmp sqlite file (mirrors the helper in
    test_global_user_state_check_results.py)."""
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
    """Minimal handle that satisfies get_clusters' attribute access."""
    launched_resources = None


def _add_cluster(name: str) -> str:
    """Create a minimal cluster row so add_cluster_event can find a hash.

    Returns the cluster_hash.
    """
    global_user_state.add_or_update_cluster(
        cluster_name=name,
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=False,
    )
    return global_user_state._get_hash_for_existing_cluster(name)


def test_launch_progress_excluded_from_last_event_helper(tmp_path, monkeypatch):
    """Adding a LAUNCH_PROGRESS event must not change the value returned by
    _get_last_or_terminal_cluster_event_multiple, which feeds the existing
    last_event field."""
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c1')

    # 1. Write a STATUS_CHANGE event first (older).
    global_user_state.add_cluster_event(
        'c1',
        new_status=None,
        reason='status-change-reason',
        event_type=global_user_state.ClusterEventType.STATUS_CHANGE,
        transitioned_at=1,
    )
    # 2. Then a newer LAUNCH_PROGRESS event.
    global_user_state.add_cluster_event(
        'c1',
        new_status=None,
        reason='launch-progress-reason',
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
        transitioned_at=2,
    )

    result = global_user_state._get_last_or_terminal_cluster_event_multiple(
        {cluster_hash})
    # The helper must skip the LAUNCH_PROGRESS row and return the
    # STATUS_CHANGE reason.
    assert result == {cluster_hash: 'status-change-reason'}


def test_launch_progress_retention_cleans_old_rows(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('c2')

    # Insert one ancient event and one recent one.
    now = int(time.time())
    global_user_state.add_cluster_event(
        'c2',
        new_status=None,
        reason='old-launch-step',
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
        transitioned_at=now - 24 * 3600,  # 24h ago
    )
    global_user_state.add_cluster_event(
        'c2',
        new_status=None,
        reason='recent-launch-step',
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
        transitioned_at=now,
    )

    # Retention = 1h → old row should be deleted, recent row should remain.
    global_user_state.cleanup_cluster_events_with_retention(
        retention_hours=1,
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
    )

    cluster_hash = global_user_state._get_hash_for_existing_cluster('c2')
    remaining = global_user_state.get_last_cluster_event(
        cluster_hash,
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
    )
    assert remaining == 'recent-launch-step'


def test_get_last_event_of_type_multiple(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    h1 = _add_cluster('a')
    h2 = _add_cluster('b')

    global_user_state.add_cluster_event(
        'a',
        new_status=None,
        reason='a-old',
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
        transitioned_at=1,
    )
    global_user_state.add_cluster_event(
        'a',
        new_status=None,
        reason='a-new',
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
        transitioned_at=2,
    )
    global_user_state.add_cluster_event(
        'b',
        new_status=None,
        reason='b-only',
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
        transitioned_at=5,
    )
    # Wrong-type row for 'b' to verify the filter:
    global_user_state.add_cluster_event(
        'b',
        new_status=None,
        reason='b-status-change',
        event_type=global_user_state.ClusterEventType.STATUS_CHANGE,
        transitioned_at=10,
    )

    result = global_user_state.get_last_cluster_event_of_type_multiple(
        {h1, h2},
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
    )
    assert result == {h1: 'a-new', h2: 'b-only'}


def test_get_clusters_populates_launch_status_reason_for_init(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('init-cluster')
    # add_or_update_cluster default leaves the row in INIT.
    global_user_state.add_cluster_event(
        'init-cluster',
        new_status=None,
        reason='Launching (1 pod(s) pending due to Pulling)',
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
        transitioned_at=int(time.time()),
    )

    # Both summary and full responses must carry the field.
    for summary in (True, False):
        records = global_user_state.get_clusters(summary_response=summary)
        match = [r for r in records if r['name'] == 'init-cluster']
        assert len(match) == 1
        assert match[0]['status'] is status_lib.ClusterStatus.INIT
        assert match[0]['launch_status_reason'] == (
            'Launching (1 pod(s) pending due to Pulling)')


def test_get_clusters_no_launch_status_reason_for_up(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('up-cluster')
    # Bring the cluster to UP.
    global_user_state.add_or_update_cluster(
        cluster_name='up-cluster',
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=True,
    )
    # Even with a LAUNCH_PROGRESS event present, an UP cluster's field
    # must be None.
    global_user_state.add_cluster_event(
        'up-cluster',
        new_status=None,
        reason='stale-launch-step',
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS,
        transitioned_at=int(time.time()),
    )
    records = global_user_state.get_clusters(summary_response=False)
    match = [r for r in records if r['name'] == 'up-cluster']
    assert len(match) == 1
    assert match[0]['status'] is status_lib.ClusterStatus.UP
    assert match[0].get('launch_status_reason') is None


def test_get_cluster_events_multiple_types_merged_and_ordered(
        tmp_path, monkeypatch):
    """get_cluster_events accepts a list of types and merges them in one query,
    ordered oldest-to-newest, with limit applied across the combined set."""
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c')

    for reason, event_type, ts in [
        ('Provisioning', global_user_state.ClusterEventType.STATUS_CHANGE, 100),
        ('Launching (pulling)',
         global_user_state.ClusterEventType.LAUNCH_PROGRESS, 200),
        ('Cluster provisioned',
         global_user_state.ClusterEventType.STATUS_CHANGE, 300),
            # A DEBUG row that must be excluded by the type filter.
        ('debug-noise', global_user_state.ClusterEventType.DEBUG, 250),
    ]:
        global_user_state.add_cluster_event('c',
                                            new_status=None,
                                            reason=reason,
                                            event_type=event_type,
                                            transitioned_at=ts)

    both = [
        global_user_state.ClusterEventType.STATUS_CHANGE,
        global_user_state.ClusterEventType.LAUNCH_PROGRESS,
    ]
    # Merged across both types, oldest-to-newest, DEBUG excluded.
    assert global_user_state.get_cluster_events(cluster_name=None,
                                                cluster_hash=cluster_hash,
                                                event_type=both) == [
                                                    'Provisioning',
                                                    'Launching (pulling)',
                                                    'Cluster provisioned'
                                                ]
    # A single type still behaves as before.
    assert global_user_state.get_cluster_events(
        cluster_name=None,
        cluster_hash=cluster_hash,
        event_type=global_user_state.ClusterEventType.LAUNCH_PROGRESS) == [
            'Launching (pulling)'
        ]
    # limit keeps the most recent N across the combined set (still ASC).
    assert global_user_state.get_cluster_events(
        cluster_name=None, cluster_hash=cluster_hash, event_type=both,
        limit=2) == ['Launching (pulling)', 'Cluster provisioned']


def test_get_cluster_events_by_names_global_limit_after_teardown(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('task-a')
    _add_cluster('task-b')
    for name, reason, event_type, transitioned_at in [
        ('task-a', 'a-old', global_user_state.ClusterEventType.STATUS_CHANGE,
         100),
        ('task-a', 'a-new', global_user_state.ClusterEventType.LAUNCH_PROGRESS,
         200),
        ('task-b', 'b-newest', global_user_state.ClusterEventType.STATUS_CHANGE,
         300),
        ('task-b', 'debug-noise', global_user_state.ClusterEventType.DEBUG,
         400),
    ]:
        global_user_state.add_cluster_event(name,
                                            new_status=None,
                                            reason=reason,
                                            event_type=event_type,
                                            transitioned_at=transitioned_at)
    global_user_state.remove_cluster('task-a', terminate=True)
    global_user_state.remove_cluster('task-b', terminate=True)

    # Put the two real names in separate 500-name query chunks. The duplicate
    # also proves input de-duplication does not duplicate returned events.
    cluster_names = (['task-a'] + [f'unused-{i}' for i in range(499)] +
                     ['task-b', 'task-a'])
    event_types = [
        global_user_state.ClusterEventType.STATUS_CHANGE,
        global_user_state.ClusterEventType.LAUNCH_PROGRESS,
    ]
    events = global_user_state.get_cluster_events_by_names(cluster_names,
                                                           event_types,
                                                           limit=2)

    assert events == [{
        'reason': 'b-newest',
        'transitioned_at': 300,
    }, {
        'reason': 'a-new',
        'transitioned_at': 200,
    }]
    # SQLAlchemy's existing negative-limit behavior is unbounded. Do not add a
    # second Python negative slice when merging multiple query chunks.
    assert global_user_state.get_cluster_events_by_names(
        cluster_names, event_types, limit=-1) == [{
            'reason': 'b-newest',
            'transitioned_at': 300,
        }, {
            'reason': 'a-new',
            'transitioned_at': 200,
        }, {
            'reason': 'a-old',
            'transitioned_at': 100,
        }]


def test_get_cluster_events_by_names_bounds_query_count(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    engine = global_user_state._db_manager.get_engine()
    counter = {'selects': 0}

    def _before_cursor_execute(conn, cursor, statement, parameters, context,
                               executemany):
        del conn, cursor, parameters, context, executemany
        lowered = statement.lower()
        if (lowered.lstrip().startswith('select') and
                'cluster_events' in lowered):
            counter['selects'] += 1

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            _before_cursor_execute)
    event_types = [global_user_state.ClusterEventType.STATUS_CHANGE]

    assert global_user_state.get_cluster_events_by_names([], event_types) == []
    assert global_user_state.get_cluster_events_by_names(['unused'], [],
                                                         limit=10) == []
    assert global_user_state.get_cluster_events_by_names(['unused'],
                                                         event_types,
                                                         limit=0) == []
    assert counter['selects'] == 0

    names = [f'task-{i}' for i in range(20)]
    assert global_user_state.get_cluster_events_by_names(names,
                                                         event_types,
                                                         limit=10) == []
    assert counter['selects'] == 1

    counter['selects'] = 0
    names = [f'task-{i}' for i in range(501)]
    assert global_user_state.get_cluster_events_by_names(names,
                                                         event_types,
                                                         limit=10) == []
    assert counter['selects'] == 2


def _count_cluster_table_selects(engine):
    """Attach a listener counting SELECTs that touch the clusters table."""
    counter = {'n': 0}

    def _before_cursor_execute(conn, cursor, statement, parameters, context,
                               executemany):
        del conn, cursor, parameters, context, executemany
        lowered = statement.lower()
        if lowered.lstrip().startswith('select') and 'clusters' in lowered:
            counter['n'] += 1

    sqlalchemy.event.listen(engine, 'before_cursor_execute',
                            _before_cursor_execute)
    return counter


def test_add_cluster_event_single_cluster_read(tmp_path, monkeypatch):
    """add_cluster_event must read the cluster row exactly once.

    Regression: it used to issue a hash pre-fetch plus two identical
    full-row queries (three SELECTs on the clusters table) per event.
    """
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('c-single')

    engine = global_user_state._db_manager.get_engine()
    counter = _count_cluster_table_selects(engine)
    global_user_state.add_cluster_event(
        'c-single',
        new_status=status_lib.ClusterStatus.UP,
        reason='single-read',
        event_type=global_user_state.ClusterEventType.STATUS_CHANGE,
    )
    assert counter['n'] == 1


def test_add_cluster_event_records_starting_status(tmp_path, monkeypatch):
    """starting_status must reflect the cluster's current status at insert."""
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c-status')
    global_user_state.set_cluster_status('c-status',
                                         status_lib.ClusterStatus.UP)

    global_user_state.add_cluster_event(
        'c-status',
        new_status=status_lib.ClusterStatus.STOPPED,
        reason='stopping',
        event_type=global_user_state.ClusterEventType.STATUS_CHANGE,
    )

    engine = global_user_state._db_manager.get_engine()
    with orm.Session(engine) as session:
        row = session.query(global_user_state.cluster_event_table).filter_by(
            cluster_hash=cluster_hash).first()
    assert row is not None
    assert row.starting_status == status_lib.ClusterStatus.UP.value
    assert row.ending_status == status_lib.ClusterStatus.STOPPED.value


def test_add_cluster_event_missing_cluster_is_noop(tmp_path, monkeypatch):
    """Events for unknown clusters are silently dropped, not inserted."""
    _fresh_db(tmp_path, monkeypatch)

    global_user_state.add_cluster_event(
        'no-such-cluster',
        new_status=None,
        reason='ghost',
        event_type=global_user_state.ClusterEventType.STATUS_CHANGE,
    )

    engine = global_user_state._db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(global_user_state.cluster_event_table).count()
    assert count == 0


def test_add_cluster_event_nop_if_duplicate_still_single_read(
        tmp_path, monkeypatch):
    """The duplicate short-circuit still works with the single-read path."""
    _fresh_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c-dup')

    for _ in range(2):
        global_user_state.add_cluster_event(
            'c-dup',
            new_status=None,
            reason='same-reason',
            event_type=global_user_state.ClusterEventType.STATUS_CHANGE,
            nop_if_duplicate=True,
        )

    engine = global_user_state._db_manager.get_engine()
    with orm.Session(engine) as session:
        count = session.query(global_user_state.cluster_event_table).filter_by(
            cluster_hash=cluster_hash).count()
    assert count == 1
