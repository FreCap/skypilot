"""Tests for stream_replica_logs' replica-status lookups.

The launch-log follow loop polls the target replica's status via
``should_stop`` on every iteration. These tests pin that each poll is a
single-row ``get_replica_info_from_id`` lookup rather than a full
``get_replica_infos`` scan, which unpickles every replica of the service
per poll and is O(replicas) wasted work at fleet scale.
"""
# pylint: disable=redefined-outer-name,unused-argument
from unittest import mock

import pytest

from sky.serve import serve_state
from sky.serve import serve_utils


def _fake_replica(status: serve_state.ReplicaStatus,
                  cluster_name: str = 'svc-1'):
    info = mock.MagicMock()
    info.status = status
    info.cluster_name = cluster_name
    return info


@pytest.fixture
def patched_env(monkeypatch, tmp_path):
    """Route service logs to tmp_path and stub the service-level checks."""
    monkeypatch.setattr(serve_state,
                        'get_service_controller_owner',
                        lambda name, require_version=False: {
                            'pool': False,
                            'resource_scope': None,
                            'status': serve_state.ServiceStatus.READY,
                        })
    monkeypatch.setattr(
        serve_state, 'get_service_from_name', lambda name:
        (_ for _ in ()).throw(
            AssertionError('stream_replica_logs should not '
                           'load the full service row')))
    monkeypatch.setattr(serve_utils,
                        'generate_remote_service_dir_name',
                        lambda name, scope=None: str(tmp_path))
    # Launch log exists (empty); main replica log absent, so the launch
    # log branch with the status poll loop is taken.
    (tmp_path / 'replica_1_launch.log').write_text('')
    return tmp_path


class TestStreamReplicaLogsStatusLookup:
    """Status polls in the launch-log follow loop are single-row queries."""

    def test_poll_uses_single_row_lookup(self, monkeypatch, patched_env):
        """Every status poll must be one single-row query, no full scans."""
        single_row_calls = []

        def fake_get_replica_info_from_id(service_name, replica_id):
            single_row_calls.append((service_name, replica_id))
            return _fake_replica(serve_state.ReplicaStatus.PROVISIONING)

        monkeypatch.setattr(serve_state, 'get_replica_info_from_id',
                            fake_get_replica_info_from_id)

        def fail_full_scan(service_name):
            raise AssertionError(
                'stream_replica_logs must not scan all replicas '
                '(get_replica_infos) to read one replica\'s status.')

        monkeypatch.setattr(serve_state, 'get_replica_infos', fail_full_scan)

        # Simulate a follow loop that polls should_stop several times.
        polls = 5

        def fake_follow(f, cluster_name, should_stop, stop_on_eof):
            for _ in range(polls):
                should_stop()
            yield from ()

        monkeypatch.setattr(serve_utils,
                            '_follow_logs_with_provision_expanding',
                            fake_follow)

        msg = serve_utils.stream_replica_logs('svc',
                                              replica_id=1,
                                              follow=False,
                                              tail=None,
                                              pool=False)
        # Still PROVISIONING and not following -> early exit, no error.
        assert msg == ''
        # 1 initial cluster-name lookup + `polls` in the follow loop +
        # 1 final check after the loop. Each is a single-row query.
        assert len(single_row_calls) == polls + 2
        assert all(call == ('svc', 1) for call in single_row_calls)

    def test_missing_replica_raises(self, monkeypatch, patched_env):
        """A vanished replica still surfaces the not-found error."""
        monkeypatch.setattr(serve_state, 'get_replica_info_from_id',
                            lambda service_name, replica_id: None)
        monkeypatch.setattr(serve_state, 'get_replica_infos', lambda name: [])
        monkeypatch.setattr(
            serve_utils, '_follow_logs_with_provision_expanding',
            lambda f, cluster_name, should_stop, stop_on_eof: iter(()))
        with pytest.raises(ValueError, match='1'):
            serve_utils.stream_replica_logs('svc',
                                            replica_id=1,
                                            follow=False,
                                            tail=None,
                                            pool=False)

    def test_resource_scope_comes_from_owner_lookup(self, monkeypatch,
                                                    patched_env):
        monkeypatch.setattr(
            serve_state, 'get_replica_info_from_id', lambda service_name,
            replica_id: _fake_replica(serve_state.ReplicaStatus.PROVISIONING))
        monkeypatch.setattr(
            serve_utils, '_follow_logs_with_provision_expanding',
            lambda f, cluster_name, should_stop, stop_on_eof: iter(()))
        owner_lookup = mock.Mock(
            return_value={
                'pool': False,
                'resource_scope': 'team-a',
                'status': serve_state.ServiceStatus.READY,
            })
        monkeypatch.setattr(serve_state, 'get_service_controller_owner',
                            owner_lookup)

        serve_utils.stream_replica_logs('svc',
                                        replica_id=1,
                                        follow=False,
                                        tail=None,
                                        pool=False)

        owner_lookup.assert_called_once_with('svc', require_version=True)
