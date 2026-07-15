"""Tests for the summary-only serve status path (SERVE_VERSION 6).

The dashboard splits the service page into a fast summary query
(`summary_only=True` -> per-status replica counts, no replica_info) and a
slower full query. These tests pin the server-side contract.
"""
# pylint: disable=redefined-outer-name,unused-argument
from unittest import mock

import pytest

from sky.serve import serve_state
from sky.serve import serve_utils
from sky.server.requests import payloads


def _fake_replica(status: serve_state.ReplicaStatus):
    info = mock.MagicMock()
    info.status = status
    return info


@pytest.fixture
def patched_state(monkeypatch):
    """Patch the DB/controller surface of _get_service_status."""
    record = {
        'name': 'svc',
        'pool': False,
        'controller_port': 30001,
        'version': 1,
        'hash': 'incarnation-a',
        # Present on the latest-version join; keeps the service_yaml
        # branch off the storage-read fallback.
        'yaml_content': 'run: echo hi\n',
    }
    monkeypatch.setattr(serve_state, 'get_service_from_name',
                        lambda name: dict(record))
    replicas = [
        _fake_replica(serve_state.ReplicaStatus.READY),
        _fake_replica(serve_state.ReplicaStatus.READY),
        _fake_replica(serve_state.ReplicaStatus.PROVISIONING),
        _fake_replica(serve_state.ReplicaStatus.FAILED_PROBING),
    ]
    monkeypatch.setattr(serve_state, 'get_replica_infos',
                        lambda name: list(replicas))
    monkeypatch.setattr(
        serve_state, 'get_replica_status_counts', lambda name: {
            'READY': 2,
            'PROVISIONING': 1,
            'FAILED_PROBING': 1,
        })
    autoscaler_resp = mock.MagicMock()
    autoscaler_resp.json.return_value = {'target_num_replicas': 4}
    monkeypatch.setattr(serve_utils, '_get_to_controller_with_retry',
                        lambda *a, **k: autoscaler_resp)
    return replicas


class TestGetServiceStatusSummary:
    """_get_service_status summary contract."""

    def test_pool_status_keeps_yaml_by_default(self, monkeypatch):
        record = {
            'name': 'pool-a',
            'pool': True,
            'controller_port': 30001,
            'version': 1,
            'hash': 'incarnation-a',
        }
        monkeypatch.setattr(serve_state, 'get_service_from_name',
                            lambda name: dict(record))
        yaml_dump = mock.Mock(return_value='rendered-pool-yaml')
        monkeypatch.setattr(serve_utils, 'get_yaml_content',
                            lambda *a, **k: 'pool: yaml')
        monkeypatch.setattr(
            serve_utils.yaml_utils, 'read_yaml_str', lambda content: {
                'resources': {
                    'cpus': 1
                },
                'service': {
                    'pool': {
                        'replicas': 2
                    }
                },
                'run': 'echo hi',
            })
        monkeypatch.setattr(serve_utils.yaml_utils, 'dump_yaml_str', yaml_dump)

        result = serve_utils._get_service_status(  # pylint: disable=protected-access
            'pool-a',
            pool=True,
            with_replica_info=False,
            with_target_num_replicas=False)

        assert result is not None
        assert result['pool_yaml'] == 'rendered-pool-yaml'
        yaml_dump.assert_called_once()

    def test_pool_status_can_skip_yaml_when_not_requested(self, monkeypatch):
        record = {
            'name': 'pool-a',
            'pool': True,
            'controller_port': 30001,
            'version': 1,
            'hash': 'incarnation-a',
        }
        monkeypatch.setattr(serve_state, 'get_service_from_name',
                            lambda name: dict(record))
        get_yaml = mock.Mock(return_value='pool: yaml')
        read_yaml = mock.Mock()
        monkeypatch.setattr(serve_utils, 'get_yaml_content', get_yaml)
        monkeypatch.setattr(serve_utils.yaml_utils, 'read_yaml_str', read_yaml)

        result = serve_utils._get_service_status(  # pylint: disable=protected-access
            'pool-a',
            pool=True,
            with_replica_info=False,
            with_yaml=False,
            with_target_num_replicas=False)

        assert result is not None
        assert 'pool_yaml' not in result
        get_yaml.assert_not_called()
        read_yaml.assert_not_called()

    def test_summary_returns_counts_without_replica_info(self, patched_state):
        record = serve_utils._get_service_status(  # pylint: disable=protected-access
            'svc',
            pool=False,
            with_replica_info=False,
            with_replica_counts=True,
            with_target_num_replicas=False)
        assert record is not None
        assert 'replica_info' not in record
        assert 'target_num_replicas' not in record
        assert record['replica_status_counts'] == {
            'READY': 2,
            'PROVISIONING': 1,
            'FAILED_PROBING': 1,
        }

    def test_summary_skips_target_fetch_when_not_requested(
            self, patched_state, monkeypatch):
        autoscaler = mock.Mock()
        monkeypatch.setattr(serve_utils, '_get_to_controller_with_retry',
                            autoscaler)
        record = serve_utils._get_service_status(  # pylint: disable=protected-access
            'svc',
            pool=False,
            with_replica_info=False,
            with_replica_counts=True,
            with_target_num_replicas=False)
        assert record is not None
        autoscaler.assert_not_called()
        assert 'target_num_replicas' not in record

    def test_summary_can_opt_in_target_fetch(self, patched_state, monkeypatch):
        autoscaler_resp = mock.MagicMock()
        autoscaler_resp.json.return_value = {
            'target_num_replicas': 4,
            'recent_request_count': 30,
            'request_window_seconds': 60,
            'requests_per_second': 0.5,
            'in_flight_total': 2,
            'queue_depth': 1,
            'rejected_in_window': 3,
            'report_age_seconds': 4.0,
            'committed_version': 7,
            'applied_version': 6,
            'update_apply_pending': True,
            'update_apply_lag_seconds': 12,
            'update_apply_error': 'manager lock unavailable',
            'update_apply_failures': 2,
        }
        autoscaler = mock.Mock(return_value=autoscaler_resp)
        monkeypatch.setattr(serve_utils, '_get_to_controller_with_retry',
                            autoscaler)
        record = serve_utils._get_service_status(  # pylint: disable=protected-access
            'svc',
            pool=False,
            with_replica_info=False,
            with_replica_counts=True,
            with_target_num_replicas=True)
        assert record is not None
        autoscaler.assert_called_once()
        assert record['target_num_replicas'] == 4
        assert record['recent_request_count'] == 30
        assert record['request_window_seconds'] == 60
        assert record['requests_per_second'] == 0.5
        assert record['in_flight_requests'] == 2
        assert record['request_queue_depth'] == 1
        assert record['rejected_requests'] == 3
        assert record['request_stats_age_seconds'] == 4.0
        assert record['committed_version'] == 7
        assert record['applied_version'] == 6
        assert record['update_apply_pending'] is True
        assert record['update_apply_lag_seconds'] == 12
        assert record['update_apply_error'] == 'manager lock unavailable'
        assert record['update_apply_failures'] == 2

    def test_default_call_has_no_counts(self, patched_state, monkeypatch):
        # Internal callers that only want the service row
        # (with_replica_info=False, counts not requested) must not pay
        # for a replica scan at all.
        called = []

        def _tracking_get_replica_status_counts(name):
            called.append(name)
            return {}

        monkeypatch.setattr(serve_state, 'get_replica_status_counts',
                            _tracking_get_replica_status_counts)
        record = serve_utils._get_service_status(  # pylint: disable=protected-access
            'svc',
            pool=False,
            with_replica_info=False)
        assert record is not None
        assert 'replica_status_counts' not in record
        assert 'replica_info' not in record
        assert not called

    def test_default_skips_target_fetch(self, patched_state, monkeypatch):
        # The controller HTTP fetch is opt-in: control/liveness callers
        # (HA recovery sweep, termination, registration polling) call
        # _get_service_status without the flag and must never block on a
        # possibly-dead controller's connect timeout.
        autoscaler = mock.Mock()
        monkeypatch.setattr(serve_utils, '_get_to_controller_with_retry',
                            autoscaler)
        record = serve_utils._get_service_status(  # pylint: disable=protected-access
            'svc',
            pool=False,
            with_replica_info=False,
            with_yaml=False)
        assert record is not None
        autoscaler.assert_not_called()
        assert 'target_num_replicas' not in record

    def test_full_status_unaffected(self, patched_state, monkeypatch):
        # with_replica_info=True keeps the original full contract.
        monkeypatch.setattr(serve_state, 'get_replica_infos', lambda name: [])
        record = serve_utils._get_service_status(  # pylint: disable=protected-access
            'svc',
            pool=False,
            with_replica_info=True)
        assert record is not None
        assert record['replica_info'] == []
        assert 'replica_status_counts' not in record


class TestUpdateServiceStatusLookup:
    """The update fence must not serialize the full replica fleet."""

    def test_update_uses_service_row_only(self, monkeypatch):
        get_status = mock.Mock(return_value={'hash': 'incarnation-a'})
        response = mock.Mock(status_code=200)
        response.json.return_value = {'message': 'update accepted'}
        post = mock.Mock(return_value=response)
        monkeypatch.setattr(serve_utils, '_get_service_status', get_status)
        monkeypatch.setattr(serve_utils, '_post_to_controller_with_retry', post)

        serve_utils.update_service_encoded(
            'svc',
            version=2,
            mode='rolling',
            pool=False,
            expected_service_hash='incarnation-a')

        get_status.assert_called_once_with('svc',
                                           pool=False,
                                           with_replica_info=False,
                                           with_yaml=False)
        post.assert_called_once()


class TestGetServiceStatusPickledSummary:
    """summary_only propagation through the pickled path."""

    def test_summary_only_defaults_to_no_target_fetch(self, patched_state,
                                                      monkeypatch):
        monkeypatch.setattr(serve_state,
                            'get_glob_service_names',
                            lambda names, pool=None: ['svc'])
        autoscaler = mock.Mock()
        monkeypatch.setattr(serve_utils, '_get_to_controller_with_retry',
                            autoscaler)
        statuses = serve_utils.get_service_status_pickled(None,
                                                          pool=False,
                                                          summary_only=True)
        assert len(statuses) == 1
        decoded = serve_utils.unpickle_service_status(statuses)[0]
        assert 'replica_info' not in decoded
        assert decoded['replica_status_counts']['READY'] == 2
        assert 'target_num_replicas' not in decoded
        autoscaler.assert_not_called()

    def test_summary_only_can_opt_in_target_fetch(self, patched_state,
                                                  monkeypatch):
        monkeypatch.setattr(serve_state,
                            'get_glob_service_names',
                            lambda names, pool=None: ['svc'])
        statuses = serve_utils.get_service_status_pickled(
            None,
            pool=False,
            summary_only=True,
            include_target_num_replicas=True)
        decoded = serve_utils.unpickle_service_status(statuses)[0]
        assert decoded['target_num_replicas'] == 4

    def test_default_is_full(self, patched_state, monkeypatch):
        monkeypatch.setattr(serve_state,
                            'get_glob_service_names',
                            lambda names, pool=None: ['svc'])
        monkeypatch.setattr(serve_state, 'get_replica_infos', lambda name: [])
        statuses = serve_utils.get_service_status_pickled(None, pool=False)
        decoded = serve_utils.unpickle_service_status(statuses)[0]
        assert decoded['replica_info'] == []
        assert 'replica_status_counts' not in decoded
        # Full (non-summary) status is the user-facing rendering path and
        # still opts into the controller autoscaler fetch by default.
        assert decoded['target_num_replicas'] == 4

    def test_summary_only_filters_name_scan_by_mode(self, monkeypatch):
        mixed_names = [
            'serve-a',
            'pool-a',
            'serve-b',
            'pool-b',
            'serve-c',
        ]
        scanned = []

        def _get_glob_service_names(names, pool=None):
            if pool is None:
                return list(mixed_names)
            return [
                name for name in mixed_names if name.startswith('pool-') == pool
            ]

        def _fake_status(name, *, pool, **kwargs):
            del kwargs
            scanned.append(name)
            return {'name': name, 'status': 'READY', 'pool': pool}

        monkeypatch.setattr(serve_state, 'get_glob_service_names',
                            _get_glob_service_names)
        monkeypatch.setattr(serve_utils, '_get_service_status', _fake_status)

        statuses = serve_utils.get_service_status_pickled(None,
                                                          pool=False,
                                                          summary_only=True)

        # The scan fans out to a thread pool; completion order is
        # scheduling-dependent, so only the scanned *set* is the contract.
        assert sorted(scanned) == ['serve-a', 'serve-b', 'serve-c']
        assert len(statuses) == 3


class TestServeModeFilteredSweeps:
    """Long-running sweeps should enumerate only the requested mode."""

    def test_update_service_status_filters_name_scan_by_mode(self, monkeypatch):
        mixed_names = ['serve-a', 'pool-a', 'serve-b', 'pool-b']
        scanned = []
        status_updates = []

        def _get_liveness_snapshots(pool):
            records = [{
                'name': name,
                'status': serve_state.ServiceStatus.READY,
                'controller_pid': None,
                'controller_ip': None,
                'hash': f'hash-{name}',
                'resource_scope': 'scope-a',
            } for name in mixed_names if name.startswith('pool-') == pool]
            scanned.extend(record['name'] for record in records)
            return records

        monkeypatch.setattr(serve_state, 'get_service_liveness_snapshots',
                            _get_liveness_snapshots)
        monkeypatch.setattr(
            serve_state, 'set_service_status_and_active_versions_if_owner',
            lambda *args, **kwargs: status_updates.append((args, kwargs)))

        serve_utils.update_service_status(pool=False)

        assert scanned == ['serve-a', 'serve-b']
        assert len(status_updates) == 2

    def test_terminate_services_filters_wildcard_scan_by_mode(
            self, monkeypatch):
        mixed_names = ['serve-a', 'pool-a', 'serve-b', 'pool-b']
        scanned = []

        def _get_glob_service_names(names, pool=None):
            if pool is None:
                return list(mixed_names)
            return [
                name for name in mixed_names if name.startswith('pool-') == pool
            ]

        def _fake_status(name, *, pool, **kwargs):
            del kwargs
            scanned.append(name)
            return {
                'name': name,
                'status': serve_state.ServiceStatus.SHUTTING_DOWN,
                'pool': pool,
            }

        monkeypatch.setattr(serve_state, 'get_glob_service_names',
                            _get_glob_service_names)
        monkeypatch.setattr(serve_utils, '_get_service_status', _fake_status)

        message = serve_utils.terminate_services(None, purge=False, pool=False)

        assert scanned == ['serve-a', 'serve-b']
        assert message == 'No service to terminate.'


class TestCodegenVersionGating:
    """ServeCodeGen gates status kwargs on the matching SERVE_VERSION."""

    def test_summary_only_gated_on_serve_version_6(self):
        code = serve_utils.ServeCodeGen.get_service_status(['svc'],
                                                           pool=False,
                                                           summary_only=True)
        # Old controllers (< v6) must never receive the kwarg; the gate
        # is evaluated remotely against the controller's own version.
        assert ('kwargs.update({"summary_only": True}) '
                'if serve_version >= 6 else None') in code

    def test_target_fetch_override_gated_on_serve_version_7(self):
        code = serve_utils.ServeCodeGen.get_service_status(
            ['svc'],
            pool=False,
            summary_only=True,
            include_target_num_replicas=False)
        assert ('kwargs.update({"include_target_num_replicas": False}) '
                'if serve_version >= 7 else None') in code

    def test_default_summary_only_false(self):
        code = serve_utils.ServeCodeGen.get_service_status(['svc'], pool=False)
        assert 'summary_only": False' in code
        assert 'include_target_num_replicas' not in code


class TestServeStatusBodyDefault:
    """API payload backward-compatibility default."""

    def test_summary_only_defaults_false(self):
        # Old clients omit the field entirely; the server must default
        # to the full payload.
        body = payloads.ServeStatusBody(service_names=None)
        assert body.summary_only is False
        assert body.include_target_num_replicas is None
