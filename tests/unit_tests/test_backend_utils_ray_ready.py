"""Tests for multi-node Ray readiness lifecycle handling."""

import asyncio
import contextlib
from unittest import mock

import pytest

from sky import exceptions
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


def _install_fake_time(monkeypatch, fake_time):
    monkeypatch.setattr(backend_utils, 'time', fake_time)
    monkeypatch.setattr(backend_utils.context_utils, 'sleep_with_cancellation',
                        fake_time.sleep)


class _RunnerFactory:
    """Creates isolated SSH runner mocks and records their kwargs."""

    def __init__(self) -> None:
        self.instances: list[mock.MagicMock] = []

    def __call__(self, *args, **kwargs):
        del args
        runner = mock.MagicMock()
        runner.kwargs = kwargs

        def _run(command, **_run_kwargs):
            if 'whoami' in command:
                return 0, 'docker-user\n', ''
            return 0, 'workers still booting', ''

        runner.run.side_effect = _run
        self.instances.append(runner)
        return runner


def test_get_docker_user_uses_passed_ssh_credentials(monkeypatch):
    factory = _RunnerFactory()
    ssh_credentials = {
        'ssh_user': 'snapshot-user',
        'ssh_private_key': '/tmp/snapshot-key',
        'ssh_control_name': 'snapshot-cluster',
        'ssh_proxy_command': None,
    }
    monkeypatch.setattr(backend_utils.command_runner, 'SSHCommandRunner',
                        factory)
    monkeypatch.setattr(
        backend_utils, 'ssh_credential_from_yaml',
        mock.MagicMock(side_effect=AssertionError('re-read YAML snapshot')))

    docker_user = backend_utils.get_docker_user('127.0.0.1',
                                                '/tmp/cluster.yaml',
                                                ssh_credentials=ssh_credentials)

    assert docker_user == 'docker-user'
    assert len(factory.instances) == 1
    assert factory.instances[0].kwargs == {
        'node': ('127.0.0.1', 22),
        **ssh_credentials,
    }


def test_ssh_credential_from_yaml_uses_passed_config_snapshot(monkeypatch):
    config = {
        'cluster_name': 'snapshot-cluster',
        'auth': {
            'ssh_user': 'snapshot-user',
            'ssh_private_key': '/tmp/snapshot-key',
        },
        'provider': {
            'module': 'sky.provision.aws'
        },
    }
    monkeypatch.setattr(
        backend_utils.global_user_state, 'get_cluster_yaml_dict',
        mock.MagicMock(side_effect=AssertionError('re-read YAML snapshot')))

    ssh_credentials = backend_utils.ssh_credential_from_yaml(
        '/tmp/cluster.yaml', config=config)

    assert ssh_credentials == {
        'ssh_user': 'snapshot-user',
        'ssh_private_key': '/tmp/snapshot-key',
        'ssh_control_name': 'snapshot-cluster',
        'ssh_proxy_command': None,
    }


def test_query_head_ip_rejects_empty_retry_budget():
    with pytest.raises(exceptions.FetchClusterInfoError):
        # pylint: disable=protected-access
        backend_utils._query_head_ip_with_retries('/tmp/cluster.yaml', 0)


def test_wait_until_ray_cluster_ready_returns_when_workers_ready(
        monkeypatch, ray_ready_dependencies):
    monkeypatch.setattr(backend_utils, '_count_healthy_nodes_from_ray',
                        mock.MagicMock(return_value=(1, 1)))
    fake_time = _fake_time(monotonic_values=[100.0])
    _install_fake_time(monkeypatch, fake_time)

    result = backend_utils.wait_until_ray_cluster_ready('/tmp/cluster.yaml', 2,
                                                        '/tmp/launch.log')

    assert result == (True, None)
    ray_ready_dependencies.run.assert_called_once()
    fake_time.monotonic.assert_not_called()
    fake_time.sleep.assert_not_called()
    fake_time.time.assert_not_called()


def test_wait_until_ray_cluster_ready_reuses_single_yaml_snapshot(monkeypatch):
    factory = _RunnerFactory()
    config = {
        'cluster_name': 'snapshot-cluster',
        'docker': {},
        'auth': {
            'ssh_user': 'primary-user',
            'ssh_private_key': '/tmp/primary-key',
        },
        'provider': {
            'module': 'sky.provision.aws'
        },
    }
    mutated = {
        'cluster_name': 'mutated-cluster',
        'docker': {},
        'auth': {
            'ssh_user': 'mutated-user',
            'ssh_private_key': '/tmp/mutated-key',
        },
        'provider': {
            'module': 'sky.provision.aws'
        },
    }
    yaml_reader = mock.MagicMock(side_effect=[config, mutated, mutated])
    monkeypatch.setattr(backend_utils, '_query_head_ip_with_retries',
                        mock.MagicMock(return_value='127.0.0.1'))
    monkeypatch.setattr(backend_utils.global_user_state,
                        'get_cluster_yaml_dict', yaml_reader)
    monkeypatch.setattr(backend_utils.command_runner, 'SSHCommandRunner',
                        factory)
    monkeypatch.setattr(backend_utils, '_count_healthy_nodes_from_ray',
                        mock.MagicMock(return_value=(1, 1)))
    monkeypatch.setattr(backend_utils.rich_utils, 'safe_status', _safe_status)

    result = backend_utils.wait_until_ray_cluster_ready('/tmp/cluster.yaml', 2,
                                                        '/tmp/launch.log')

    assert result == (True, 'docker-user')
    assert yaml_reader.call_count == 1
    assert len(factory.instances) == 2
    docker_runner = factory.instances[0]
    status_runner = factory.instances[1]
    assert docker_runner.kwargs == {
        'node': ('127.0.0.1', 22),
        'ssh_user': 'primary-user',
        'ssh_private_key': '/tmp/primary-key',
        'ssh_control_name': 'snapshot-cluster',
        'ssh_proxy_command': None,
    }
    assert status_runner.kwargs == {
        **docker_runner.kwargs,
        'docker_user': 'docker-user',
    }


def test_wait_until_ray_cluster_ready_uses_monotonic_progress_timeout(
        monkeypatch, ray_ready_dependencies):
    monkeypatch.setattr(backend_utils, '_count_healthy_nodes_from_ray',
                        mock.MagicMock(return_value=(0, 0)))
    fake_time = _fake_time(monotonic_values=[100.0, 101.0])
    fake_time.sleep.side_effect = AssertionError(
        'slept after the progress timeout expired')
    _install_fake_time(monkeypatch, fake_time)

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


def test_wait_until_ray_cluster_ready_clamps_final_progress_sleep(
        monkeypatch, ray_ready_dependencies):
    monkeypatch.setattr(backend_utils, '_count_healthy_nodes_from_ray',
                        mock.MagicMock(return_value=(0, 0)))
    fake_time = _fake_time(monotonic_values=[100.0, 100.25, 100.5, 100.75])
    _install_fake_time(monkeypatch, fake_time)

    result = backend_utils.wait_until_ray_cluster_ready(
        '/tmp/cluster.yaml',
        2,
        '/tmp/launch.log',
        nodes_launching_progress_timeout=0.5)

    assert result == (False, None)
    assert ray_ready_dependencies.run.call_count == 2
    fake_time.sleep.assert_called_once_with(0.25)
    fake_time.time.assert_not_called()


def test_wait_until_ray_cluster_ready_accepts_ready_deadline_snapshot(
        monkeypatch, ray_ready_dependencies):
    monkeypatch.setattr(backend_utils, '_count_healthy_nodes_from_ray',
                        mock.MagicMock(side_effect=[(0, 0), (1, 1)]))
    fake_time = _fake_time(monotonic_values=[100.0, 100.25])
    _install_fake_time(monkeypatch, fake_time)

    result = backend_utils.wait_until_ray_cluster_ready(
        '/tmp/cluster.yaml',
        2,
        '/tmp/launch.log',
        nodes_launching_progress_timeout=0.5)

    assert result == (True, None)
    assert ray_ready_dependencies.run.call_count == 2
    fake_time.sleep.assert_called_once_with(0.25)
    fake_time.time.assert_not_called()


def test_wait_until_ray_cluster_ready_resets_progress_timeout(
        monkeypatch, ray_ready_dependencies):
    monkeypatch.setattr(
        backend_utils, '_count_healthy_nodes_from_ray',
        mock.MagicMock(side_effect=[(1, 0), (1, 0), (1, 1), (1, 1), (1, 1)]))
    fake_time = _fake_time(
        monotonic_values=[100.0, 100.0, 100.8, 100.9, 101.5, 102.0])
    _install_fake_time(monkeypatch, fake_time)

    result = backend_utils.wait_until_ray_cluster_ready(
        '/tmp/cluster.yaml',
        4,
        '/tmp/launch.log',
        nodes_launching_progress_timeout=1)

    assert result == (False, None)
    assert ray_ready_dependencies.run.call_count == 5
    assert fake_time.monotonic.call_count == 6
    assert fake_time.sleep.call_count == 4
    assert [call.args[0] for call in fake_time.sleep.call_args_list
           ] == pytest.approx([1, 0.2, 1, 0.4])
    fake_time.time.assert_not_called()


def test_wait_until_ray_cluster_ready_without_timeout_skips_clock(
        monkeypatch, ray_ready_dependencies):
    monkeypatch.setattr(backend_utils, '_count_healthy_nodes_from_ray',
                        mock.MagicMock(side_effect=[(0, 0), (1, 1)]))
    fake_time = _fake_time(
        monotonic_values=AssertionError('read an unused progress clock'))
    _install_fake_time(monkeypatch, fake_time)

    result = backend_utils.wait_until_ray_cluster_ready('/tmp/cluster.yaml', 2,
                                                        '/tmp/launch.log')

    assert result == (True, None)
    assert ray_ready_dependencies.run.call_count == 2
    fake_time.monotonic.assert_not_called()
    fake_time.sleep.assert_called_once_with(10)
    fake_time.time.assert_not_called()


def test_wait_until_ray_cluster_ready_cancellation_stops_before_next_probe(
        monkeypatch, ray_ready_dependencies):
    monkeypatch.setattr(backend_utils, '_count_healthy_nodes_from_ray',
                        mock.MagicMock(return_value=(0, 0)))
    ray_ready_dependencies.run.side_effect = [
        (0, 'workers still booting', ''),
        AssertionError('SSH status probed after cancellation'),
    ]
    wait = mock.Mock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr(backend_utils.context_utils,
                        'sleep_with_cancellation',
                        wait,
                        raising=False)
    monkeypatch.setattr(backend_utils.time, 'sleep', mock.Mock())

    with pytest.raises(asyncio.CancelledError):
        backend_utils.wait_until_ray_cluster_ready('/tmp/cluster.yaml', 2,
                                                   '/tmp/launch.log')

    wait.assert_called_once_with(10)
    ray_ready_dependencies.run.assert_called_once()
