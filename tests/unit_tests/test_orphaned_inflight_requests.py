"""Regression test: in-flight requests dropped on API-server restart must be
surfaced, not silently lost.

``requests.reset_db_and_logs`` runs on every API-server startup and wipes the
request DB + logs; the executor child processes that ran those requests died
with the previous process. A request still PENDING/WAITING/RUNNING -- e.g. a
long provisioning launch -- was therefore silently dropped: the caller saw the
request vanish, and any half-provisioned cluster leaked until a later status
refresh. The boltz long-worker-pool widening admits more concurrent launches,
so each restart orphans proportionally more of them.

``_log_orphaned_inflight_requests`` is the minimal mitigation: detect and loudly
log the orphaned requests before the wipe so the drop is alertable and the
leaked clusters reconcilable. These tests pin that behavior, including that a
scan failure never blocks startup.
"""
# pylint: disable=protected-access
import types
import unittest.mock as mock

import pytest
import pytest_asyncio

from sky.server.requests import payloads
from sky.server.requests import requests as requests_lib
from sky.server.requests import storage as request_storage


class _FakeReq:

    def __init__(self, request_id, name, status_value, cluster_name=None):
        self.request_id = request_id
        self.name = name
        self.status = types.SimpleNamespace(value=status_value)
        self.cluster_name = cluster_name


class _FakeBackend:

    def __init__(self, reqs):
        self._reqs = reqs
        self.filters = []

    def query_requests(self, req_filter):
        self.filters.append(req_filter)
        return self._reqs


def _capture_warnings(monkeypatch):
    warnings = []
    monkeypatch.setattr(requests_lib.logger, 'warning',
                        lambda msg, *a, **k: warnings.append(str(msg)))
    return warnings


def test_logs_each_orphaned_inflight_request(monkeypatch):
    reqs = [
        _FakeReq('req-1', 'sky.launch', 'RUNNING', 'cluster-a'),
        _FakeReq('req-2', 'sky.down', 'PENDING'),
    ]
    backend = _FakeBackend(reqs)
    monkeypatch.setattr(request_storage, 'get_request_backend', lambda: backend)
    warnings = _capture_warnings(monkeypatch)

    requests_lib._log_orphaned_inflight_requests()

    # One summary warning plus one warning per orphaned request.
    assert len(warnings) == 1 + len(reqs)
    # The scan must filter to only active (in-flight) statuses, and must not
    # select the pickled columns, whose decode can fail across an upgrade.
    req_filter = backend.filters[0]
    assert req_filter.status == requests_lib.RequestStatus.active_statuses()
    assert set(
        req_filter.fields) == {'request_id', 'name', 'status', 'cluster_name'}


def test_no_request_id_suffix_is_hidden(monkeypatch):
    # Legacy daemon rows are retired before generic recovery reaches this
    # helper. If ordering regresses, do not silently hide a finite request just
    # because its user-selected ID has the historical suffix.
    daemon_reqs = [_FakeReq('user-selected-daemon', 'sky.launch', 'RUNNING')]
    monkeypatch.setattr(request_storage, 'get_request_backend',
                        lambda: _FakeBackend(daemon_reqs))
    warnings = _capture_warnings(monkeypatch)

    requests_lib._log_orphaned_inflight_requests()

    assert len(warnings) == 2


def test_no_orphans_no_warning(monkeypatch):
    monkeypatch.setattr(request_storage, 'get_request_backend',
                        lambda: _FakeBackend([]))
    warnings = _capture_warnings(monkeypatch)

    requests_lib._log_orphaned_inflight_requests()

    assert not warnings, 'no in-flight requests -> no warning'


def test_scan_failure_does_not_block_startup(monkeypatch):

    def _boom():
        raise RuntimeError('request DB schema incompatible')

    monkeypatch.setattr(request_storage, 'get_request_backend', _boom)
    warnings = _capture_warnings(monkeypatch)

    # Must not raise -- a scan failure may never block API-server startup.
    requests_lib._log_orphaned_inflight_requests()

    assert not warnings


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
            yield
            await requests_lib.close_db_async()


def _make_request(request_id: str,
                  status: requests_lib.RequestStatus) -> requests_lib.Request:
    return requests_lib.Request(request_id=request_id,
                                name='sky.launch',
                                entrypoint=_dummy,
                                request_body=payloads.RequestBody(),
                                status=status,
                                created_at=0.0,
                                user_id='test-user')


@pytest.mark.asyncio
async def test_reset_db_and_logs_reinitializes_backend(isolated_database,
                                                       tmp_path, monkeypatch):
    # The pre-wipe orphan scan initializes the DB handle against the old
    # database file; reset_db_and_logs must not leave that handle bound to
    # the unlinked file, or post-reset reads would serve phantom pre-restart
    # rows and the fresh database would never be created on this thread.
    monkeypatch.setattr(requests_lib, 'LEGACY_REQUEST_LOG_PATH_PREFIX',
                        str(tmp_path / 'legacy_logs'))
    monkeypatch.setattr(requests_lib.bs, 'get_blob_storage', mock.Mock)
    warnings = _capture_warnings(monkeypatch)
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-old', requests_lib.RequestStatus.RUNNING))
    # reset_db_and_logs() is a synchronous startup-only API. Close the async
    # test connection first so its aiosqlite worker is not orphaned when the
    # reset drops and recreates the module-level handle.
    await requests_lib.close_db_async()

    requests_lib.reset_db_and_logs()

    # The scan ran before the wipe: summary + one orphaned request.
    assert len(warnings) == 2
    # The backend now serves the re-created, empty database, and new
    # requests can be written to it.
    filt = requests_lib.RequestTaskFilter()
    assert requests_lib.get_request_tasks(filt) == []
    assert await requests_lib.create_if_not_exists_async(
        _make_request('req-new', requests_lib.RequestStatus.PENDING))


# ---------------------------------------------------------------------------
# surface_interrupted_cluster_launches: INIT clusters whose in-flight work
# died with the previous server run (or with the pod's disk on a redeploy)
# get a cluster event, so the wedge is explained instead of silent.
# ---------------------------------------------------------------------------


def _patch_init_clusters(monkeypatch, names):
    monkeypatch.setattr(requests_lib.global_user_state,
                        'get_cluster_names_by_status', lambda status: names)


def _capture_cluster_events(monkeypatch):
    events = []

    def _add_event(cluster_name, new_status, reason, event_type, **kwargs):
        events.append({
            'cluster_name': cluster_name,
            'new_status': new_status,
            'event_type': event_type,
            'kwargs': kwargs,
        })

    monkeypatch.setattr(requests_lib.global_user_state, 'add_cluster_event',
                        _add_event)
    return events


def test_surface_flags_init_clusters_without_active_request(monkeypatch):
    _patch_init_clusters(monkeypatch, ['wedged-a', 'resuming-b'])
    # 'resuming-b' still has a surviving active request row (it will be
    # re-enqueued), so only 'wedged-a' must be flagged.
    backend = _FakeBackend(
        [_FakeReq('req-1', 'sky.launch', 'PENDING', 'resuming-b')])
    monkeypatch.setattr(request_storage, 'get_request_backend', lambda: backend)
    events = _capture_cluster_events(monkeypatch)

    requests_lib.surface_interrupted_cluster_launches()

    assert [e['cluster_name'] for e in events] == ['wedged-a']
    event = events[0]
    # The event must not change the cluster status, must be deduplicated
    # across restart storms, and must surface on the status page (only
    # TERMINAL/STATUS_CHANGE events are shown there).
    assert event['new_status'] is None
    assert event['event_type'] == (
        requests_lib.global_user_state.ClusterEventType.STATUS_CHANGE)
    assert event['kwargs'].get('nop_if_duplicate') is True
    # The request scan must be scoped to the INIT clusters and to in-flight
    # statuses, and must avoid the pickled columns (their decode can fail
    # across an upgrade).
    req_filter = backend.filters[0]
    assert req_filter.status == requests_lib.RequestStatus.active_statuses()
    assert req_filter.cluster_names == ['wedged-a', 'resuming-b']
    assert set(req_filter.fields) == {'request_id', 'cluster_name'}


def test_surface_no_init_clusters_skips_request_scan(monkeypatch):
    _patch_init_clusters(monkeypatch, [])
    scanned = []
    monkeypatch.setattr(request_storage, 'get_request_backend',
                        lambda: scanned.append(True))
    events = _capture_cluster_events(monkeypatch)

    requests_lib.surface_interrupted_cluster_launches()

    assert not scanned
    assert not events


def test_surface_failure_does_not_block_startup(monkeypatch):
    _patch_init_clusters(monkeypatch, ['wedged-a'])

    def _boom():
        raise RuntimeError('request DB schema incompatible')

    monkeypatch.setattr(request_storage, 'get_request_backend', _boom)
    events = _capture_cluster_events(monkeypatch)

    # Must not raise -- surfacing is best effort.
    requests_lib.surface_interrupted_cluster_launches()

    assert not events


def test_surface_event_write_failure_does_not_block_startup(monkeypatch):
    _patch_init_clusters(monkeypatch, ['wedged-a'])
    monkeypatch.setattr(request_storage, 'get_request_backend',
                        lambda: _FakeBackend([]))

    def _add_event(*args, **kwargs):
        raise RuntimeError('database is locked')

    monkeypatch.setattr(requests_lib.global_user_state, 'add_cluster_event',
                        _add_event)

    # Must not raise -- surfacing is best effort.
    requests_lib.surface_interrupted_cluster_launches()


@pytest.mark.asyncio
async def test_surface_scan_against_real_backend(isolated_database,
                                                 monkeypatch):
    # Exercise the real sqlite query path (cluster_names filter combined with
    # a fields projection), which the fake-backend tests above bypass.
    req = _make_request('req-live', requests_lib.RequestStatus.PENDING)
    req.cluster_name = 'resuming-b'
    assert await requests_lib.create_if_not_exists_async(req)

    _patch_init_clusters(monkeypatch, ['wedged-a', 'resuming-b'])
    events = _capture_cluster_events(monkeypatch)

    requests_lib.surface_interrupted_cluster_launches()

    assert [e['cluster_name'] for e in events] == ['wedged-a']
