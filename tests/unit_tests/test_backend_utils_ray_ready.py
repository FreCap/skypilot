"""Tests for multi-node Ray readiness lifecycle handling."""

import contextlib
from unittest import mock

import pytest

from sky.backends import backend_utils


@contextlib.contextmanager
def _safe_status(_message):
    status = mock.MagicMock()
    yield status


@pytest.fixture(name='ray_ready_dependencies')
def _ray_ready_dependencies(monkeypatch):
    runner = mock.MagicMock()
    runner.run.return_value = (0, 'workers still booting', '')
    monkeypatch.setattr(backend_utils, '_query_head_ip_with_retries',
                        mock.MagicMock(return_value='127.0.0.1'))
    monkeypatch.setattr(backend_utils.global_user_state,
                        'get_cluster_yaml_dict',
                        mock.MagicMock(return_value={}))
    monkeypatch.setattr(backend_utils, 'ssh_credential_from_yaml',
                        mock.MagicMock(return_value={}))
    monkeypatch.setattr(backend_utils.command_runner, 'SSHCommandRunner',
                        mock.MagicMock(return_value=runner))
    monkeypatch.setattr(backend_utils.rich_utils, 'safe_status', _safe_status)
    return runner


def _fake_time(*, monotonic_values):
    fake = mock.MagicMock()
    fake.monotonic.side_effect = monotonic_values
    fake.time.side_effect = AssertionError('read wall time for an interval')
    return fake


def test_wait_until_ray_cluster_ready_returns_when_workers_ready(
        monkeypatch, ray_ready_dependencies):
    monkeypatch.setattr(backend_utils, '_count_healthy_nodes_from_ray',
                        mock.MagicMock(return_value=(1, 1)))
    fake_time = _fake_time(monotonic_values=[100.0])
    monkeypatch.setattr(backend_utils, 'time', fake_time)

    result = backend_utils.wait_until_ray_cluster_ready('/tmp/cluster.yaml', 2,
                                                        '/tmp/launch.log')

    assert result == (True, None)
    ray_ready_dependencies.run.assert_called_once()
    fake_time.monotonic.assert_called_once_with()
    fake_time.sleep.assert_not_called()
    fake_time.time.assert_not_called()


def test_wait_until_ray_cluster_ready_uses_monotonic_progress_timeout(
        monkeypatch, ray_ready_dependencies):
    monkeypatch.setattr(backend_utils, '_count_healthy_nodes_from_ray',
                        mock.MagicMock(return_value=(0, 0)))
    fake_time = _fake_time(monotonic_values=[100.0, 101.0])
    fake_time.sleep.side_effect = AssertionError(
        'slept after the progress timeout expired')
    monkeypatch.setattr(backend_utils, 'time', fake_time)

    result = backend_utils.wait_until_ray_cluster_ready(
        '/tmp/cluster.yaml',
        2,
        '/tmp/launch.log',
        nodes_launching_progress_timeout=0.5)

    assert result == (False, None)
    ray_ready_dependencies.run.assert_called_once()
    assert fake_time.monotonic.call_count == 2
    fake_time.sleep.assert_not_called()
    fake_time.time.assert_not_called()


def test_wait_until_ray_cluster_ready_resets_progress_timeout(
        monkeypatch, ray_ready_dependencies):
    monkeypatch.setattr(backend_utils, '_count_healthy_nodes_from_ray',
                        mock.MagicMock(side_effect=[(1, 0), (1, 0), (1, 0)]))
    fake_time = _fake_time(monotonic_values=[100.0, 105.0, 105.5, 106.1])
    monkeypatch.setattr(backend_utils, 'time', fake_time)

    result = backend_utils.wait_until_ray_cluster_ready(
        '/tmp/cluster.yaml',
        2,
        '/tmp/launch.log',
        nodes_launching_progress_timeout=1)

    assert result == (False, None)
    assert ray_ready_dependencies.run.call_count == 3
    assert fake_time.monotonic.call_count == 4
    assert fake_time.sleep.call_count == 2
    fake_time.time.assert_not_called()
