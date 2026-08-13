"""Tests for request-state recovery across API server restarts.

The API server used to wipe the whole request DB and logs on every startup
(``reset_db_and_logs``), so any restart -- hard crashes included -- destroyed
queued PENDING/WAITING requests and left clients polling in-flight requests
with a 404. ``recover_db_and_logs`` replaces the wipe: it first retires the
explicit legacy daemon inventory, marks interrupted rows CANCELLED +
should_retry, preserves queued rows for re-enqueue, and falls back to the
legacy wipe when recovery fails or is explicitly disabled.
"""
# pylint: disable=protected-access
# pylint: disable=redefined-outer-name,unused-argument
import unittest.mock as mock

import pytest
import pytest_asyncio

from sky.server import daemons
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import requests as requests_lib
from sky.server.requests.requests import RequestStatus


def _dummy():
    return None


@pytest_asyncio.fixture()
async def isolated_database(tmp_path):
    temp_db_path = tmp_path / 'requests.db'
    temp_log_path = tmp_path / 'logs'
    temp_log_path.mkdir()
    with mock.patch('sky.server.constants.API_SERVER_REQUEST_DB_PATH',
                    str(temp_db_path)):
        with mock.patch('sky.server.constants.REQUEST_LOG_PATH_PREFIX',
                        str(temp_log_path)):
            await requests_lib.close_db_async()
            yield temp_db_path
            await requests_lib.close_db_async()


@pytest.fixture()
def isolated_legacy_logs(tmp_path, monkeypatch):
    monkeypatch.setattr(requests_lib, 'LEGACY_REQUEST_LOG_PATH_PREFIX',
                        str(tmp_path / 'legacy_logs'))


def _make_request(request_id: str,
                  status: RequestStatus,
                  created_at: float = 0.0,
                  schedule_type=requests_lib.ScheduleType.LONG,
                  ignore_return_value: bool = False,
                  retryable: bool = False) -> requests_lib.Request:
    return requests_lib.Request(request_id=request_id,
                                name='sky.launch',
                                entrypoint=_dummy,
                                request_body=payloads.RequestBody(),
                                status=status,
                                created_at=created_at,
                                user_id='test-user',
                                schedule_type=schedule_type,
                                ignore_return_value=ignore_return_value,
                                retryable=retryable)


@pytest.mark.asyncio
async def test_enqueue_flags_survive_insert_read_roundtrip(isolated_database):
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-flags',
                      RequestStatus.PENDING,
                      ignore_return_value=True,
                      retryable=True))
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-defaults', RequestStatus.PENDING))

    record = requests_lib.get_request('req-flags')
    assert record.ignore_return_value is True
    assert record.retryable is True
    record = requests_lib.get_request('req-defaults')
    assert record.ignore_return_value is False
    assert record.retryable is False


@pytest.mark.asyncio
async def test_recovery_reconciles_each_status(isolated_database,
                                               isolated_legacy_logs):
    daemon_id = next(iter(daemons.LEGACY_REQUEST_DAEMON_IDS))
    seed = [
        _make_request('req-pending', RequestStatus.PENDING),
        _make_request('req-waiting-retryable',
                      RequestStatus.WAITING,
                      retryable=True),
        _make_request('req-waiting-not-retryable', RequestStatus.WAITING),
        _make_request('req-waiting-legacy-null', RequestStatus.WAITING),
        _make_request('req-running', RequestStatus.RUNNING),
        _make_request('req-succeeded', RequestStatus.SUCCEEDED),
        _make_request('req-failed', RequestStatus.FAILED),
        _make_request('req-cancelled', RequestStatus.CANCELLED),
        _make_request(daemon_id, RequestStatus.RUNNING, retryable=True),
    ]
    for request in seed:
        assert await requests_lib.create_if_not_exists_async(request)
    # Simulate a row written by an older server without the retryable
    # column (NULL instead of 0).
    with requests_lib._DB.conn:
        requests_lib._DB.conn.execute(
            f'UPDATE {requests_lib.REQUEST_TABLE} SET retryable = NULL '
            'WHERE request_id = ?', ('req-waiting-legacy-null',))

    # Recovery ran and completed: the caller may re-enqueue queued rows.
    assert requests_lib.recover_db_and_logs() is True

    # Legacy daemon rows are retired before generic request recovery.
    assert requests_lib.get_request(daemon_id) is None
    # Interrupted rows get the client retry signal.
    for request_id in ('req-running', 'req-waiting-not-retryable',
                       'req-waiting-legacy-null'):
        record = requests_lib.get_request(request_id)
        assert record.status == RequestStatus.CANCELLED, request_id
        assert record.should_retry is True, request_id
        assert record.finished_at is not None, request_id
    # Queued rows are preserved for re-enqueue.
    record = requests_lib.get_request('req-pending')
    assert record.status == RequestStatus.PENDING
    assert record.should_retry is False
    record = requests_lib.get_request('req-waiting-retryable')
    assert record.status == RequestStatus.WAITING
    assert record.should_retry is False
    # Terminal rows are untouched.
    record = requests_lib.get_request('req-succeeded')
    assert record.status == RequestStatus.SUCCEEDED
    assert record.should_retry is False
    record = requests_lib.get_request('req-failed')
    assert record.status == RequestStatus.FAILED
    record = requests_lib.get_request('req-cancelled')
    assert record.status == RequestStatus.CANCELLED


@pytest.mark.asyncio
async def test_reenqueue_recovered_requests_in_created_at_order(
        isolated_database, monkeypatch):
    user_suffix_id = 'user-selected-daemon'
    seed = [
        _make_request('req-newer',
                      RequestStatus.PENDING,
                      created_at=2.0,
                      schedule_type=requests_lib.ScheduleType.LONG,
                      ignore_return_value=True),
        _make_request('req-older',
                      RequestStatus.WAITING,
                      created_at=1.0,
                      schedule_type=requests_lib.ScheduleType.SHORT,
                      retryable=True),
        _make_request('req-done', RequestStatus.SUCCEEDED, created_at=0.5),
        # Not retryable: never replayed, even if recovery somehow left it
        # in WAITING instead of flipping it to CANCELLED.
        _make_request('req-waiting-not-retryable',
                      RequestStatus.WAITING,
                      created_at=0.3),
        _make_request(user_suffix_id, RequestStatus.PENDING, created_at=0.1),
    ]
    for request in seed:
        assert await requests_lib.create_if_not_exists_async(request)

    puts = []

    class _StubQueue:

        def __init__(self, schedule_type):
            self._schedule_type = schedule_type

        def put(self, item):
            puts.append((self._schedule_type, item))

    monkeypatch.setattr(executor, '_get_queue', _StubQueue)

    executor.reenqueue_recovered_requests()

    assert puts == [
        (requests_lib.ScheduleType.LONG, (user_suffix_id, False, False)),
        (requests_lib.ScheduleType.SHORT, ('req-older', False, True)),
        (requests_lib.ScheduleType.LONG, ('req-newer', True, False)),
    ]


@pytest.mark.asyncio
async def test_reset_env_var_forces_full_wipe(isolated_database,
                                              isolated_legacy_logs,
                                              monkeypatch):
    monkeypatch.setattr(requests_lib.bs, 'get_blob_storage', mock.Mock)
    monkeypatch.setenv(requests_lib.RESET_REQUESTS_ON_STARTUP_ENV_VAR, '1')
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-healthy', RequestStatus.PENDING))

    # The wipe path signals that recovery did NOT run, so the server must
    # not re-enqueue anything.
    assert requests_lib.recover_db_and_logs() is False

    assert requests_lib.get_request_tasks(
        requests_lib.RequestTaskFilter()) == []


def test_plugin_request_backend_falls_back_to_wipe_without_reenqueue(
        monkeypatch):
    # A plugin RequestBackend owns its own restart semantics via
    # reset_on_startup() (a no-op by default); the sqlite-level recovery
    # would not see its rows, so the legacy reset path must be taken AND the
    # surviving, never-reconciled rows must not be replayed.
    plugin_backend = mock.Mock(spec=requests_lib.request_storage.RequestBackend)
    # The plugin backend still holds a queued row after the (no-op) wipe: if
    # the server re-enqueued unconditionally, this row would be replayed.
    plugin_backend.query_requests.return_value = [
        _make_request('req-plugin-pending', RequestStatus.PENDING)
    ]
    monkeypatch.setattr(requests_lib.request_storage, '_storage_backend',
                        plugin_backend)
    wipe = mock.Mock()
    monkeypatch.setattr(requests_lib, 'reset_db_and_logs', wipe)
    puts = []
    monkeypatch.setattr(
        executor, '_get_queue',
        lambda schedule_type: mock.Mock(put=lambda item: puts.append(
            (schedule_type, item))))

    # Mirror the server startup sequence: re-enqueue only if recovery ran.
    recovered = requests_lib.recover_db_and_logs()
    if recovered:
        executor.reenqueue_recovered_requests()

    wipe.assert_called_once()
    assert recovered is False
    # The queued plugin row was never reconciled by recovery, so nothing may
    # be enqueued.
    assert not puts


@pytest.mark.asyncio
async def test_reenqueue_does_not_reserve_daemon_suffix(isolated_database,
                                                        monkeypatch):
    user_suffix_id = 'user-selected-daemon'
    assert user_suffix_id not in daemons.LEGACY_REQUEST_DAEMON_IDS
    seed = [
        _make_request(user_suffix_id, RequestStatus.PENDING, created_at=1.0),
        _make_request('req-user-pending', RequestStatus.PENDING,
                      created_at=2.0),
    ]
    for request in seed:
        assert await requests_lib.create_if_not_exists_async(request)

    puts = []
    monkeypatch.setattr(executor, '_get_queue',
                        lambda schedule_type: mock.Mock(put=puts.append))

    executor.reenqueue_recovered_requests()

    assert [item[0] for item in puts] == [user_suffix_id, 'req-user-pending']


def test_legacy_daemon_inventory_is_explicit_and_complete():
    runtime_ids = {daemon.id for daemon in daemons.RUNTIME_DAEMONS}
    assert runtime_ids < daemons.LEGACY_REQUEST_DAEMON_IDS
    assert daemons.LEGACY_REQUEST_DAEMON_IDS - runtime_ids == {
        'managed-job-status-refresh-daemon'
    }


@pytest.mark.asyncio
async def test_corrupted_db_falls_back_to_wipe(isolated_database,
                                               isolated_legacy_logs,
                                               monkeypatch):
    monkeypatch.setattr(requests_lib.bs, 'get_blob_storage', mock.Mock)
    isolated_database.write_bytes(b'this is not a sqlite database at all')

    # Must not raise: a corrupted DB may never block startup. The fallback
    # wipe signals that recovery did not run (no re-enqueue).
    assert requests_lib.recover_db_and_logs() is False

    # The corrupted file was wiped and replaced with a fresh, empty DB that
    # accepts new writes.
    assert requests_lib.get_request_tasks(
        requests_lib.RequestTaskFilter()) == []
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-new', RequestStatus.PENDING))


# ---------------------------------------------------------------------------
# Interrupted-launch replay: launches are re-runnable by construction until
# their cluster reaches UP (the run section is only submitted after the UP
# row write), so recovery requeues them instead of cancelling, and graceful
# shutdown leaves them RUNNING for recovery instead of waiting/cancelling.
# ---------------------------------------------------------------------------

from sky.utils import status_lib  # pylint: disable=wrong-import-position

_LAUNCH_NAME = requests_lib.REPLAYABLE_REQUEST_NAMES[0]


def _make_launch_request(request_id: str,
                         status: RequestStatus,
                         cluster_name: str,
                         retryable: bool = False) -> requests_lib.Request:
    req = _make_request(request_id, status, retryable=retryable)
    req.name = _LAUNCH_NAME
    req.cluster_name = cluster_name
    return req


def _patch_cluster_statuses(monkeypatch, statuses):
    # Mirrors global_user_state.get_cluster_status_fields: returns the raw
    # (status, status_updated_at) columns and omits unknown cluster names.
    calls = []

    def _get_status_fields(cluster_names):
        calls.append(list(cluster_names))
        return {
            name: (statuses[name].value, None)
            for name in cluster_names
            if statuses.get(name) is not None
        }

    monkeypatch.setattr(requests_lib.global_user_state,
                        'get_cluster_status_fields', _get_status_fields)
    return calls


@pytest.mark.asyncio
async def test_recovery_requeues_interrupted_launches(isolated_database,
                                                      isolated_legacy_logs,
                                                      monkeypatch):
    seed = [
        # Interrupted mid-provision: cluster still INIT -> requeue.
        _make_launch_request('req-launch-init', RequestStatus.RUNNING,
                             'cluster-init'),
        # Died before the cluster row was written: pre-provision side
        # effects (e.g. storage creation) have no established re-run
        # semantics -> client-retry path, not a replay.
        _make_launch_request('req-launch-no-row', RequestStatus.RUNNING,
                             'cluster-missing'),
        # Cluster reached UP: job submission may have happened -> the
        # generic CANCELLED + should_retry path, never a replay.
        _make_launch_request('req-launch-up', RequestStatus.RUNNING,
                             'cluster-up'),
        # Non-retryable WAITING launch never started this attempt -> requeue.
        _make_launch_request('req-launch-waiting', RequestStatus.WAITING,
                             'cluster-init'),
        # Retryable WAITING launch is already queued for a full re-run;
        # recovery must leave it to the normal re-enqueue path.
        _make_launch_request('req-launch-waiting-retryable',
                             RequestStatus.WAITING,
                             'cluster-init',
                             retryable=True),
    ]
    for request in seed:
        assert await requests_lib.create_if_not_exists_async(request)
    calls = _patch_cluster_statuses(
        monkeypatch, {
            'cluster-init': status_lib.ClusterStatus.INIT,
            'cluster-missing': None,
            'cluster-up': status_lib.ClusterStatus.UP,
        })

    assert requests_lib.recover_db_and_logs() is True

    # All statuses are resolved in a single batched lookup, regardless of
    # how many launch rows or distinct clusters are being recovered.
    assert len(calls) == 1
    assert set(calls[0]) == {'cluster-init', 'cluster-missing', 'cluster-up'}

    for request_id in ('req-launch-init', 'req-launch-waiting'):
        record = requests_lib.get_request(request_id)
        assert record.status == RequestStatus.PENDING, request_id
        assert record.should_retry is False, request_id
        assert record.pid is None, request_id
        assert record.finished_at is None, request_id
    for request_id in ('req-launch-up', 'req-launch-no-row'):
        record = requests_lib.get_request(request_id)
        assert record.status == RequestStatus.CANCELLED, request_id
        assert record.should_retry is True, request_id
    record = requests_lib.get_request('req-launch-waiting-retryable')
    assert record.status == RequestStatus.WAITING
    assert record.should_retry is False


@pytest.mark.asyncio
async def test_requeued_launch_is_reenqueued(isolated_database,
                                             isolated_legacy_logs, monkeypatch):
    # End-to-end at the recovery layer: a requeued launch must be picked up
    # by the executor re-enqueue pass like any PENDING row.
    assert await requests_lib.create_if_not_exists_async(
        _make_launch_request('req-launch-init', RequestStatus.RUNNING,
                             'cluster-init'))
    _patch_cluster_statuses(monkeypatch,
                            {'cluster-init': status_lib.ClusterStatus.INIT})
    assert requests_lib.recover_db_and_logs() is True

    puts = []

    class _StubQueue:

        def __init__(self, schedule_type):
            self._schedule_type = schedule_type

        def put(self, item):
            puts.append((self._schedule_type, item))

    monkeypatch.setattr(executor, '_get_queue', _StubQueue)

    executor.reenqueue_recovered_requests()

    assert puts == [(requests_lib.ScheduleType.LONG, ('req-launch-init', False,
                                                      False))]


def test_replayable_names_match_persisted_request_names():
    # Request rows persist REQUEST_NAME_PREFIX + RequestName (see
    # executor request creation); a bare enum value here would silently
    # disable replay for every real launch row.
    from sky.server import constants as server_constants
    from sky.server.requests import request_names
    valid_names = {n.value for n in request_names.RequestName}
    for name in requests_lib.REPLAYABLE_REQUEST_NAMES:
        assert name.startswith(server_constants.REQUEST_NAME_PREFIX), name
        assert name[len(server_constants.REQUEST_NAME_PREFIX):] in \
            valid_names, name


@pytest.mark.asyncio
async def test_cluster_status_lookup_failure_disqualifies_only_that_row(
        isolated_database, isolated_legacy_logs, monkeypatch):
    # A failing cluster-status lookup must fall back to the client-retry
    # path for that launch only -- never abort recovery into the full-wipe
    # fallback, which would discard unrelated queued rows.
    seed = [
        _make_launch_request('req-launch-bad-lookup', RequestStatus.RUNNING,
                             'cluster-boom'),
        _make_request('req-pending', RequestStatus.PENDING),
    ]
    for request in seed:
        assert await requests_lib.create_if_not_exists_async(request)

    def _boom(cluster_names):
        raise RuntimeError('cluster-state DB unavailable')

    monkeypatch.setattr(requests_lib.global_user_state,
                        'get_cluster_status_fields', _boom)

    assert requests_lib.recover_db_and_logs() is True

    record = requests_lib.get_request('req-launch-bad-lookup')
    assert record.status == RequestStatus.CANCELLED
    assert record.should_retry is True
    record = requests_lib.get_request('req-pending')
    assert record.status == RequestStatus.PENDING
