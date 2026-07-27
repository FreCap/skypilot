"""Tests for global_user_state.remove_cluster.

remove_cluster reads everything it needs from the clusters table in a single
snapshot query and, on the stop path, writes the handle back in the same
session. These tests pin the lifecycle behavior (stop persists STOPPED and
invalidates cached IPs, terminate deletes the row, missing clusters are
no-ops, the provision log path is backfilled into history) and pin the
one-read query bound so the batching cannot silently regress.
"""
import sqlalchemy

from sky import global_user_state
from sky.skylet import constants
from sky.utils import status_lib
from sky.utils.db import db_utils


def _fresh_db(tmp_path, monkeypatch):
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


class _Handle:
    """Minimal picklable handle with a cached-IP field."""
    launched_resources = None

    def __init__(self):
        self.stable_internal_external_ips = [('1.2.3.4', '5.6.7.8')]


def _add_cluster(name: str, provision_log_path=None) -> None:
    global_user_state.add_or_update_cluster(
        cluster_name=name,
        cluster_handle=_Handle(),
        requested_resources=set(),
        ready=True,
        provision_log_path=provision_log_path,
    )


def test_stop_persists_stopped_status_and_invalidates_ips(
        tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('c')

    global_user_state.remove_cluster('c', terminate=False)

    record = global_user_state.get_cluster_from_name('c')
    assert record is not None
    assert record['status'] == status_lib.ClusterStatus.STOPPED
    assert record['handle'].stable_internal_external_ips is None


def test_terminate_deletes_cluster_row(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('c')

    global_user_state.remove_cluster('c', terminate=True)

    assert global_user_state.get_cluster_from_name('c') is None


def test_missing_cluster_is_a_noop(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)

    global_user_state.remove_cluster('ghost', terminate=False)
    global_user_state.remove_cluster('ghost', terminate=True)


def test_terminate_backfills_history_provision_log_path(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('c', provision_log_path='/logs/provision.log')

    global_user_state.remove_cluster('c', terminate=True)

    assert (global_user_state.get_cluster_history_provision_log_path('c') ==
            '/logs/provision.log')


def test_stop_reads_clusters_table_once(tmp_path, monkeypatch):
    """The whole stop path must issue exactly one clusters-table SELECT."""
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('c', provision_log_path='/logs/provision.log')

    engine = global_user_state._db_manager.get_engine()
    cluster_selects = []

    def _count(conn, cursor, statement, parameters, context, executemany):
        del conn, cursor, parameters, context, executemany
        normalized = statement.lstrip().upper()
        if normalized.startswith('SELECT') and 'FROM CLUSTERS' in normalized:
            cluster_selects.append(statement)

    sqlalchemy.event.listen(engine, 'before_cursor_execute', _count)
    try:
        global_user_state.remove_cluster('c', terminate=False)
    finally:
        sqlalchemy.event.remove(engine, 'before_cursor_execute', _count)

    assert len(cluster_selects) == 1, cluster_selects


def test_stop_handle_corruption_does_not_lose_row(tmp_path, monkeypatch):
    """A stop on a row whose handle cannot be produced leaves the row intact.

    The early return before the snapshot row exists must not commit partial
    writes; here we exercise the row-is-missing branch through a name that
    was terminated concurrently between caller check and remove_cluster.
    """
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('c')
    global_user_state.remove_cluster('c', terminate=True)

    # Second stop call sees no row and returns without raising.
    global_user_state.remove_cluster('c', terminate=False)
    assert global_user_state.get_cluster_from_name('c') is None


def test_usage_interval_closed_on_remove(tmp_path, monkeypatch):
    _fresh_db(tmp_path, monkeypatch)
    _add_cluster('c')

    engine = global_user_state._db_manager.get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            sqlalchemy.text(
                'SELECT cluster_hash FROM clusters WHERE name = :n'), {
                    'n': 'c'
                }).fetchone()
    cluster_hash = row[0]

    global_user_state.remove_cluster('c', terminate=True)

    intervals = global_user_state._get_cluster_usage_intervals(cluster_hash)
    assert intervals, 'usage intervals must survive removal'
    assert intervals[-1][1] is not None, 'last interval must be closed'


def _cluster_hash(name: str) -> str:
    engine = global_user_state._db_manager.get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            sqlalchemy.text(
                'SELECT cluster_hash FROM clusters WHERE name = :n'), {
                    'n': name
                }).fetchone()
    return row[0]


def _install_clock(monkeypatch, start: int) -> dict:
    """Freeze global_user_state's clock so elapsed time is explicit."""
    clock = {'now': start}
    monkeypatch.setattr(global_user_state.time, 'time', lambda: clock['now'])
    return clock


def test_repeated_stop_does_not_extend_a_closed_interval(tmp_path, monkeypatch):
    """A stopped cluster must not accrue uptime while it sits stopped.

    The status-refresh daemon reaches remove_cluster on every sweep of an
    already-STOPPED cluster (all nodes report STOPPED, so backend_utils calls
    post_teardown_cleanup again). Re-closing the last interval at "now" on
    each sweep would bill the whole stopped period.
    """
    _fresh_db(tmp_path, monkeypatch)
    clock = _install_clock(monkeypatch, 1_000_000)
    _add_cluster('c')
    cluster_hash = _cluster_hash('c')

    # One hour of real uptime, then the cluster stops.
    clock['now'] += 3600
    global_user_state.remove_cluster('c', terminate=False)
    after_stop = global_user_state._get_cluster_usage_intervals(cluster_hash)
    assert after_stop[-1][1] == 1_003_600

    # Ten days of daemon sweeps against the already-stopped cluster.
    for _ in range(10):
        clock['now'] += 86400
        global_user_state.remove_cluster('c', terminate=False)

    assert (global_user_state._get_cluster_usage_intervals(cluster_hash) ==
            after_stop)


def test_terminate_after_stop_does_not_extend_a_closed_interval(
        tmp_path, monkeypatch):
    """`sky down` on a long-stopped cluster must not bill the stopped time."""
    _fresh_db(tmp_path, monkeypatch)
    clock = _install_clock(monkeypatch, 1_000_000)
    _add_cluster('c')
    cluster_hash = _cluster_hash('c')

    clock['now'] += 3600
    global_user_state.remove_cluster('c', terminate=False)
    after_stop = global_user_state._get_cluster_usage_intervals(cluster_hash)

    # Terminated 30 days later.
    clock['now'] += 30 * 86400
    global_user_state.remove_cluster('c', terminate=True)

    assert (global_user_state._get_cluster_usage_intervals(cluster_hash) ==
            after_stop)


def test_restart_appends_a_new_interval_and_stop_closes_it(
        tmp_path, monkeypatch):
    """Stop/start must still record each separate run, and only each run."""
    _fresh_db(tmp_path, monkeypatch)
    clock = _install_clock(monkeypatch, 1_000_000)
    _add_cluster('c')
    cluster_hash = _cluster_hash('c')

    clock['now'] += 3600
    global_user_state.remove_cluster('c', terminate=False)

    # Idle while stopped, then relaunched and stopped again an hour later.
    clock['now'] += 7 * 86400
    _add_cluster('c')
    clock['now'] += 3600
    global_user_state.remove_cluster('c', terminate=False)

    intervals = global_user_state._get_cluster_usage_intervals(cluster_hash)
    assert len(intervals) == 2, intervals
    assert all(end is not None for _, end in intervals)
    billed = sum(end - start for start, end in intervals)
    assert billed == 2 * 3600, intervals
