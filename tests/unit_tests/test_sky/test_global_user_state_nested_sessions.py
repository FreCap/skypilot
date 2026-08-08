"""Regression tests for nested synchronous DB checkouts in global_user_state.

PR #927 set the synchronous PostgreSQL ``QueuePool`` to ``max_overflow=0`` and
lets ``pool_size`` fall to 1 (it is a hard 1 for the API server main process,
which calls ``db_utils.set_max_connections(1)``). Every ``state``/``spot``/
``serve`` module shares that one process-local pool. Any single logical
operation that opens a second ``orm.Session`` on the shared engine while the
first is still holding its connection therefore needs two concurrent
connections; with ``max_overflow=0`` the second checkout blocks until
``pool_timeout`` and raises ``sqlalchemy.exc.TimeoutError``.

Three public operations did exactly that before the fix:

* ``add_cluster_event(nop_if_duplicate=True)`` -> ``get_last_cluster_event``
* ``remove_cluster`` -> ``_get_cluster_usage_intervals`` /
  ``_set_cluster_usage_intervals``
* ``get_clusters_from_names(include_user_info=True)`` -> batched ``get_users``

Each test drives one of these against a real ``QueuePool(pool_size=1,
max_overflow=0)`` engine. Pre-fix the nested checkout self-deadlocks and raises
``sqlalchemy.exc.TimeoutError`` after ``pool_timeout``; after threading the
caller's session through the nested helper the operation completes on a single
connection.
"""
# pylint: disable=protected-access
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy import pool

from sky import global_user_state

# Keep the pre-fix deadlock's failure fast but well clear of scheduling jitter.
_POOL_TIMEOUT_S = 1.0


class _SingleConnManager:
    """Stub DatabaseManager exposing a strict one-connection QueuePool engine."""

    def __init__(self, engine: sqlalchemy.engine.Engine) -> None:
        self._engine = engine

    def get_engine(self) -> sqlalchemy.engine.Engine:
        return self._engine


def _single_connection_state_db(tmp_path, monkeypatch):
    """Point global_user_state at a QueuePool(pool_size=1, max_overflow=0) db.

    Mirrors the production synchronous PostgreSQL pool policy (strict, no
    overflow) that PR #927 introduced, using SQLite so the test needs no
    database daemon. The advisory-lock helper is a no-op off PostgreSQL and
    SQLite ignores ``FOR UPDATE``, so ``remove_cluster`` still exercises its
    nested usage-interval helpers here.
    """
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "state.db"}',
        poolclass=pool.QueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=_POOL_TIMEOUT_S,
        connect_args={'check_same_thread': False},
    )
    global_user_state.create_table(engine)
    monkeypatch.setattr(global_user_state, '_db_manager',
                        _SingleConnManager(engine))
    return engine


class _MinimalHandle:
    """Minimal handle that satisfies get_clusters' attribute access."""
    launched_resources = None


def _add_cluster(name: str) -> str:
    global_user_state.add_or_update_cluster(
        cluster_name=name,
        cluster_handle=_MinimalHandle(),
        requested_resources=set(),
        ready=False,
    )
    return global_user_state._get_hash_for_existing_cluster(name)


def test_add_cluster_event_nop_if_duplicate_uses_single_connection(
        tmp_path, monkeypatch):
    """nop_if_duplicate reads the last event while holding the write session."""
    _single_connection_state_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c1')

    # First write: get_last_cluster_event runs nested inside add_cluster_event.
    global_user_state.add_cluster_event(
        'c1',
        new_status=None,
        reason='reason-1',
        event_type=global_user_state.ClusterEventType.STATUS_CHANGE,
        nop_if_duplicate=True,
        transitioned_at=1,
    )
    # Second identical write must be suppressed as a duplicate (proves the
    # nested read actually returned the prior event, not just that it did not
    # deadlock).
    global_user_state.add_cluster_event(
        'c1',
        new_status=None,
        reason='reason-1',
        event_type=global_user_state.ClusterEventType.STATUS_CHANGE,
        nop_if_duplicate=True,
        transitioned_at=2,
    )
    assert global_user_state.get_last_cluster_event(
        cluster_hash,
        global_user_state.ClusterEventType.STATUS_CHANGE) == 'reason-1'


def test_remove_cluster_uses_single_connection(tmp_path, monkeypatch):
    """remove_cluster reads and rewrites usage intervals while holding its
    row-mutation session."""
    _single_connection_state_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c2')
    # Populate a usage interval so remove_cluster exercises BOTH nested helpers
    # (_get_cluster_usage_intervals and _set_cluster_usage_intervals).
    global_user_state._set_cluster_usage_intervals(cluster_hash, [(1, None)])

    global_user_state.remove_cluster('c2', terminate=True)

    assert global_user_state.get_cluster_from_name('c2') is None


def test_get_clusters_from_names_with_user_info_uses_single_connection(
        tmp_path, monkeypatch):
    """include_user_info resolves each row's user while holding the batch
    session."""
    _single_connection_state_db(tmp_path, monkeypatch)
    _add_cluster('c3')

    result = global_user_state.get_clusters_from_names(['c3'],
                                                       include_user_info=True)

    assert set(result) == {'c3'}
    assert result['c3'] is not None
    assert result['c3']['name'] == 'c3'


def test_set_cluster_usage_intervals_defers_commit_to_supplied_session(
        tmp_path, monkeypatch):
    """When a session is supplied the write must join the caller's transaction
    and NOT commit early (else remove_cluster's locks/atomicity break)."""
    engine = _single_connection_state_db(tmp_path, monkeypatch)
    cluster_hash = _add_cluster('c4')
    global_user_state._set_cluster_usage_intervals(cluster_hash, [(1, 2)])

    # Stage a write through a caller-owned session, then roll back: because the
    # helper must not commit, the change must not persist.
    with orm.Session(engine) as session:
        global_user_state._set_cluster_usage_intervals(cluster_hash, [(9, 9)],
                                                       session=session)
        session.rollback()
    assert global_user_state._get_cluster_usage_intervals(cluster_hash) == [(1,
                                                                             2)]

    # Without a session the helper owns the commit and the write persists.
    global_user_state._set_cluster_usage_intervals(cluster_hash, [(5, 6)])
    assert global_user_state._get_cluster_usage_intervals(cluster_hash) == [(5,
                                                                             6)]
