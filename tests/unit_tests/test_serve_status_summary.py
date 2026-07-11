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
    autoscaler_resp = mock.MagicMock()
    autoscaler_resp.json.return_value = {'target_num_replicas': 4}
    monkeypatch.setattr(serve_utils, '_get_to_controller_with_retry',
                        lambda *a, **k: autoscaler_resp)
    return replicas


class TestGetServiceStatusSummary:
    """_get_service_status summary contract."""

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
        autoscaler_resp.json.return_value = {'target_num_replicas': 4}
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

    def test_default_call_has_no_counts(self, patched_state, monkeypatch):
        # Internal callers that only want the service row
        # (with_replica_info=False, counts not requested) must not pay
        # for a replica scan at all.
        called = []

        def _tracking_get_replica_infos(name):
            called.append(name)
            return []

        monkeypatch.setattr(serve_state, 'get_replica_infos',
                            _tracking_get_replica_infos)
        record = serve_utils._get_service_status(  # pylint: disable=protected-access
            'svc',
            pool=False,
            with_replica_info=False)
        assert record is not None
        assert 'replica_status_counts' not in record
        assert 'replica_info' not in record
        assert not called

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


class TestGetServiceStatusPickledSummary:
    """summary_only propagation through the pickled path."""

    def test_summary_only_defaults_to_no_target_fetch(self, patched_state,
                                                      monkeypatch):
        monkeypatch.setattr(serve_state, 'get_glob_service_names',
                            lambda names: ['svc'])
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
        monkeypatch.setattr(serve_state, 'get_glob_service_names',
                            lambda names: ['svc'])
        statuses = serve_utils.get_service_status_pickled(
            None,
            pool=False,
            summary_only=True,
            include_target_num_replicas=True)
        decoded = serve_utils.unpickle_service_status(statuses)[0]
        assert decoded['target_num_replicas'] == 4

    def test_default_is_full(self, patched_state, monkeypatch):
        monkeypatch.setattr(serve_state, 'get_glob_service_names',
                            lambda names: ['svc'])
        monkeypatch.setattr(serve_state, 'get_replica_infos', lambda name: [])
        statuses = serve_utils.get_service_status_pickled(None, pool=False)
        decoded = serve_utils.unpickle_service_status(statuses)[0]
        assert decoded['replica_info'] == []
        assert 'replica_status_counts' not in decoded


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
