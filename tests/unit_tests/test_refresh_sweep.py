"""Tests for the background cluster status refresh sweep.

The sweep in ``backend_utils.refresh_cluster_records`` must be fault
isolated (one cluster's failure cannot abort the refresh of the others),
must dispatch clusters most in need of reconciliation first (INIT, then
stalest), and must honor the ``SKYPILOT_CLUSTER_REFRESH_PARALLELISM`` env
override for its thread count.
"""
# pylint: disable=protected-access
import threading

from sky.backends import backend_utils
from sky.utils import status_lib


def _record(status, status_updated_at):
    return {'status': status, 'status_updated_at': status_updated_at}


def _fail_request_task_lookup(*args, **kwargs):
    del args, kwargs
    raise AssertionError('refresh must not query request tasks')


class TestRefreshFaultIsolation:
    """One cluster's failure must not abort the sweep."""

    def test_refresh_cluster_returns_sentinel_on_unexpected_error(
            self, monkeypatch):
        error = RuntimeError('boom')

        def _raise(*args, **kwargs):
            raise error

        monkeypatch.setattr(backend_utils, 'refresh_cluster_record', _raise)
        record = backend_utils._refresh_cluster('c1',
                                                force_refresh_statuses=None)
        assert record is not None
        assert record['status'] == 'UNKNOWN'
        assert record['error'] is error

    def test_sweep_covers_all_clusters_despite_one_failing(self, monkeypatch):
        cluster_names = ['c-ok-1', 'c-bad', 'c-ok-2']
        attempted = []
        attempted_lock = threading.Lock()

        def _fake_refresh_cluster_record(cluster_name, **kwargs):
            del kwargs
            with attempted_lock:
                attempted.append(cluster_name)
            if cluster_name == 'c-bad':
                raise RuntimeError('flaky cluster')
            return _record(status_lib.ClusterStatus.UP, 1)

        monkeypatch.setattr(backend_utils, 'refresh_cluster_record',
                            _fake_refresh_cluster_record)

        def _snapshot(names=None, *, exclude_managed_clusters=False):
            del exclude_managed_clusters
            selected_names = cluster_names if names is None else names
            return {
                name: (status_lib.ClusterStatus.UP.value, 1)
                for name in selected_names
            }

        monkeypatch.setattr(backend_utils.global_user_state,
                            'get_cluster_status_fields', _snapshot)
        monkeypatch.setattr('sky.server.requests.requests.get_request_tasks',
                            _fail_request_task_lookup)

        # Must not raise, and every cluster must have been attempted.
        backend_utils.refresh_cluster_records()
        assert sorted(attempted) == sorted(cluster_names)

    def test_sweep_reuses_status_snapshot_for_names_and_ordering(
            self, monkeypatch):
        """The normal sweep path reads one consistent unmanaged snapshot."""
        attempted = []
        refresh_kwargs = []
        snapshot_calls = []

        def _snapshot(cluster_names=None, *, exclude_managed_clusters=False):
            snapshot_calls.append((cluster_names, exclude_managed_clusters))
            return {
                'up-stale': (status_lib.ClusterStatus.UP.value, 1),
                'init-fresh': (status_lib.ClusterStatus.INIT.value, 2),
                'launching': (status_lib.ClusterStatus.INIT.value, 0),
            }

        def _unexpected_names_read(*args, **kwargs):
            del args, kwargs
            raise AssertionError('normal path must not reread cluster names')

        def _fake_refresh_cluster_record(cluster_name, **kwargs):
            attempted.append(cluster_name)
            refresh_kwargs.append(kwargs)
            return _record(status_lib.ClusterStatus.UP, 1)

        monkeypatch.setattr(backend_utils.global_user_state,
                            'get_cluster_status_fields', _snapshot)
        monkeypatch.setattr(backend_utils.global_user_state,
                            'get_cluster_names', _unexpected_names_read)
        monkeypatch.setattr('sky.server.requests.requests.get_request_tasks',
                            _fail_request_task_lookup)
        monkeypatch.setattr(backend_utils, 'refresh_cluster_record',
                            _fake_refresh_cluster_record)
        monkeypatch.setattr(backend_utils, '_get_cluster_refresh_parallelism',
                            lambda: 1)

        backend_utils.refresh_cluster_records()

        assert snapshot_calls == [(None, True)]
        assert attempted == ['launching', 'init-fresh', 'up-stale']
        assert all(kwargs['cluster_status_lock_timeout'] == 0
                   for kwargs in refresh_kwargs)

    def test_sweep_covers_all_clusters_when_snapshot_fails(self, monkeypatch):
        """A status-snapshot failure falls back to the names-only sweep."""
        cluster_names = ['c-1', 'c-2', 'c-3']
        attempted = []
        attempted_lock = threading.Lock()

        def _fake_refresh_cluster_record(cluster_name, **kwargs):
            del kwargs
            with attempted_lock:
                attempted.append(cluster_name)
            return _record(status_lib.ClusterStatus.UP, 1)

        def _raise(names=None, *, exclude_managed_clusters=False):
            del names, exclude_managed_clusters
            raise RuntimeError('corrupt row')

        monkeypatch.setattr(backend_utils, 'refresh_cluster_record',
                            _fake_refresh_cluster_record)
        monkeypatch.setattr(backend_utils.global_user_state,
                            'get_cluster_names',
                            lambda exclude_managed_clusters: cluster_names)
        monkeypatch.setattr(backend_utils.global_user_state,
                            'get_cluster_status_fields', _raise)
        monkeypatch.setattr('sky.server.requests.requests.get_request_tasks',
                            _fail_request_task_lookup)

        backend_utils.refresh_cluster_records()
        assert sorted(attempted) == sorted(cluster_names)

    def test_sweep_preserves_source_order_when_ordering_fails(
            self, monkeypatch):
        """The fallback path keeps DB/source order without request-table I/O."""
        cluster_names = ['c-2', 'c-1', 'c-3', 'launching']
        attempted = []
        attempted_lock = threading.Lock()

        def _fake_refresh_cluster_record(cluster_name, **kwargs):
            del kwargs
            with attempted_lock:
                attempted.append(cluster_name)
            return _record(status_lib.ClusterStatus.UP, 1)

        def _raise(names=None, *, exclude_managed_clusters=False):
            del names, exclude_managed_clusters
            raise RuntimeError('corrupt row')

        monkeypatch.setattr(backend_utils, 'refresh_cluster_record',
                            _fake_refresh_cluster_record)
        monkeypatch.setattr(backend_utils.global_user_state,
                            'get_cluster_names',
                            lambda exclude_managed_clusters: cluster_names)
        monkeypatch.setattr(backend_utils.global_user_state,
                            'get_cluster_status_fields', _raise)
        monkeypatch.setattr('sky.server.requests.requests.get_request_tasks',
                            _fail_request_task_lookup)
        # Pin the sweep to one thread: the ordering contract is about
        # submission order, which is only observable through execution order
        # when the pool is serial.
        monkeypatch.setattr(backend_utils, '_get_cluster_refresh_parallelism',
                            lambda: 1)

        backend_utils.refresh_cluster_records()
        assert attempted == cluster_names


class TestRefreshParallelismKnob:
    """Env override for the sweep's thread count."""

    _ENV_VAR = backend_utils.CLUSTER_REFRESH_PARALLELISM_ENV_VAR

    def test_valid_override(self, monkeypatch):
        monkeypatch.setenv(self._ENV_VAR, '7')
        assert backend_utils._get_cluster_refresh_parallelism() == 7

    def test_unset_uses_default(self, monkeypatch):
        monkeypatch.delenv(self._ENV_VAR, raising=False)
        monkeypatch.setattr(backend_utils.subprocess_utils,
                            'get_parallel_threads',
                            lambda cloud_str=None: 42)
        assert backend_utils._get_cluster_refresh_parallelism() == 42

    def test_invalid_overrides_use_default(self, monkeypatch):
        monkeypatch.setattr(backend_utils.subprocess_utils,
                            'get_parallel_threads',
                            lambda cloud_str=None: 42)
        for bad_value in ('abc', '0', '-3', ''):
            monkeypatch.setenv(self._ENV_VAR, bad_value)
            assert backend_utils._get_cluster_refresh_parallelism() == 42

    def test_invalid_override_warns_once(self, monkeypatch):
        """The refresh daemon calls this every sweep; warn once, not always."""
        monkeypatch.setattr(backend_utils.subprocess_utils,
                            'get_parallel_threads',
                            lambda cloud_str=None: 42)
        monkeypatch.setattr(backend_utils,
                            '_warned_invalid_refresh_parallelism', set())
        warning_calls = []
        monkeypatch.setattr(backend_utils.logger, 'warning',
                            lambda *args, **kwargs: warning_calls.append(1))
        monkeypatch.setenv(self._ENV_VAR, 'not-a-number')
        assert backend_utils._get_cluster_refresh_parallelism() == 42
        assert backend_utils._get_cluster_refresh_parallelism() == 42
        assert len(warning_calls) == 1


class TestRefreshOrdering:
    """Clusters most in need of reconciliation are dispatched first."""

    def test_init_first_then_stalest(self, monkeypatch):
        # Raw (status, status_updated_at) column values, as returned by
        # global_user_state.get_cluster_status_fields.
        status_fields = {
            'up-stale': (status_lib.ClusterStatus.UP.value, 50),
            'init-fresh': (status_lib.ClusterStatus.INIT.value, 200),
            'stopped-fresh': (status_lib.ClusterStatus.STOPPED.value, 300),
            'init-stale': (status_lib.ClusterStatus.INIT.value, 100),
            'up-no-timestamp': (status_lib.ClusterStatus.UP.value, None),
        }
        monkeypatch.setattr(
            backend_utils.global_user_state, 'get_cluster_status_fields',
            lambda names: {
                name: status_fields[name]
                for name in names
                if name in status_fields
            })

        ordered = backend_utils._sort_clusters_for_refresh([
            'stopped-fresh', 'up-no-timestamp', 'up-stale', 'init-fresh',
            'deleted', 'init-stale'
        ])
        # INIT clusters first (stalest first), then the rest by ascending
        # status_updated_at; missing timestamps/records sort as stalest.
        assert ordered[:2] == ['init-stale', 'init-fresh']
        assert set(ordered[2:4]) == {'up-no-timestamp', 'deleted'}
        assert ordered[4:] == ['up-stale', 'stopped-fresh']

    def test_ordering_failure_falls_back_to_original_order(self, monkeypatch):
        """Ordering is best-effort: if its data source raises, the helper
        returns the input list unchanged instead of raising."""

        def _raise(names):
            del names
            raise RuntimeError('corrupt row')

        monkeypatch.setattr(backend_utils.global_user_state,
                            'get_cluster_status_fields', _raise)
        cluster_names = ['c-2', 'c-1', 'c-3']
        assert backend_utils._sort_clusters_for_refresh(
            cluster_names) == cluster_names
