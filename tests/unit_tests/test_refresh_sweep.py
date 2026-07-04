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
        monkeypatch.setattr(backend_utils.global_user_state,
                            'get_cluster_names',
                            lambda exclude_managed_clusters: cluster_names)
        monkeypatch.setattr(
            backend_utils.global_user_state, 'get_clusters_from_names',
            lambda names, **kwargs:
            {name: _record(status_lib.ClusterStatus.UP, 1) for name in names})
        monkeypatch.setattr(backend_utils.requests_lib, 'get_request_tasks',
                            lambda req_filter: [])

        # Must not raise, and every cluster must have been attempted.
        backend_utils.refresh_cluster_records()
        assert sorted(attempted) == sorted(cluster_names)


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


class TestRefreshOrdering:
    """Clusters most in need of reconciliation are dispatched first."""

    def test_init_first_then_stalest(self, monkeypatch):
        records = {
            'up-stale': _record(status_lib.ClusterStatus.UP, 50),
            'init-fresh': _record(status_lib.ClusterStatus.INIT, 200),
            'stopped-fresh': _record(status_lib.ClusterStatus.STOPPED, 300),
            'init-stale': _record(status_lib.ClusterStatus.INIT, 100),
            'up-no-timestamp': _record(status_lib.ClusterStatus.UP, None),
        }
        monkeypatch.setattr(
            backend_utils.global_user_state, 'get_clusters_from_names',
            lambda names, **kwargs: {name: records.get(name) for name in names})

        ordered = backend_utils._sort_clusters_for_refresh([
            'stopped-fresh', 'up-no-timestamp', 'up-stale', 'init-fresh',
            'deleted', 'init-stale'
        ])
        # INIT clusters first (stalest first), then the rest by ascending
        # status_updated_at; missing timestamps/records sort as stalest.
        assert ordered[:2] == ['init-stale', 'init-fresh']
        assert set(ordered[2:4]) == {'up-no-timestamp', 'deleted'}
        assert ordered[4:] == ['up-stale', 'stopped-fresh']
