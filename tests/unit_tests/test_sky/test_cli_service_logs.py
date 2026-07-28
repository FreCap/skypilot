"""Characterization tests for shared Serve and pool CLI log handling."""

import contextlib
import pickle
from unittest import mock

import click
from click import testing as cli_testing
import pytest

from sky import serve as serve_lib
from sky.client.cli import command


def test_service_log_handler_keeps_facade_identity():
    handler = command._handle_serve_logs

    assert handler.__name__ == '_handle_serve_logs'
    assert handler.__module__ == command.__name__
    assert pickle.loads(pickle.dumps(handler)) is handler
    assert handler.__globals__['logger'] is command.logger


def _isolate_sync_down_ui(monkeypatch):
    monkeypatch.setattr(command.rich_utils, 'client_status',
                        lambda *_args, **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(command.logger, 'info', mock.Mock())


def test_service_logs_sync_down_defaults_to_all_components(
        monkeypatch, tmp_path):
    _isolate_sync_down_ui(monkeypatch)
    monkeypatch.setattr(command.constants, 'SKY_LOGS_DIRECTORY', str(tmp_path))
    monkeypatch.setattr(command.sky_logging, 'get_run_timestamp',
                        lambda: 'timestamp')
    sync_down = mock.Mock()
    monkeypatch.setattr(command.serve_lib, 'sync_down_logs', sync_down)

    command._handle_serve_logs('service-name',
                               follow=True,
                               controller=False,
                               load_balancer=False,
                               replica_ids=(),
                               sync_down=True,
                               tail=25,
                               pool=False)

    sync_down.assert_called_once()
    args, kwargs = sync_down.call_args
    assert args == ('service-name',
                    str(tmp_path / 'service' / 'service-name_timestamp'))
    assert set(kwargs.pop('targets')) == {
        serve_lib.ServiceComponent.CONTROLLER,
        serve_lib.ServiceComponent.LOAD_BALANCER,
        serve_lib.ServiceComponent.REPLICA,
    }
    assert kwargs == {'replica_ids': [], 'tail': 25}
    assert (tmp_path / 'service' / 'service-name_timestamp').is_dir()


def test_pool_logs_sync_down_defaults_to_controller_and_workers(
        monkeypatch, tmp_path):
    _isolate_sync_down_ui(monkeypatch)
    monkeypatch.setattr(command.constants, 'SKY_LOGS_DIRECTORY', str(tmp_path))
    monkeypatch.setattr(command.sky_logging, 'get_run_timestamp',
                        lambda: 'timestamp')
    sync_down = mock.Mock()
    monkeypatch.setattr(command.managed_jobs, 'pool_sync_down_logs', sync_down)

    command._handle_serve_logs('pool-name',
                               follow=True,
                               controller=False,
                               load_balancer=False,
                               replica_ids=(),
                               sync_down=True,
                               tail=None,
                               pool=True)

    sync_down.assert_called_once()
    args, kwargs = sync_down.call_args
    assert args == ('pool-name', str(tmp_path / 'pool' / 'pool-name_timestamp'))
    assert set(kwargs.pop('targets')) == {
        serve_lib.ServiceComponent.CONTROLLER,
        serve_lib.ServiceComponent.REPLICA,
    }
    assert kwargs == {'worker_ids': [], 'tail': None}
    assert (tmp_path / 'pool' / 'pool-name_timestamp').is_dir()


def test_service_logs_tail_disables_follow(monkeypatch):
    warning = mock.Mock()
    tail_logs = mock.Mock()
    monkeypatch.setattr(command.logger, 'warning', warning)
    monkeypatch.setattr(command.serve_lib, 'tail_logs', tail_logs)

    command._handle_serve_logs('service-name',
                               follow=True,
                               controller=True,
                               load_balancer=False,
                               replica_ids=(),
                               sync_down=False,
                               tail=10,
                               pool=False)

    warning.assert_called_once()
    tail_logs.assert_called_once_with(
        'service-name',
        target=serve_lib.ServiceComponent.CONTROLLER,
        replica_id=None,
        follow=False,
        tail=10)


def test_pool_logs_tail_dispatches_worker(monkeypatch):
    tail_logs = mock.Mock()
    monkeypatch.setattr(command.managed_jobs, 'pool_tail_logs', tail_logs)

    command._handle_serve_logs('pool-name',
                               follow=False,
                               controller=False,
                               load_balancer=False,
                               replica_ids=(3,),
                               sync_down=False,
                               tail=None,
                               pool=True)

    tail_logs.assert_called_once_with('pool-name',
                                      target=serve_lib.ServiceComponent.REPLICA,
                                      worker_id=3,
                                      follow=False,
                                      tail=None)


@pytest.mark.parametrize(('kwargs', 'message'), [
    ({
        'tail': -1
    }, '--tail must be a non-negative integer.'),
    ({
        'controller': False,
        'replica_ids': ()
    }, 'Specify a target to tail:'),
    ({
        'controller': True,
        'load_balancer': True
    }, 'Can only tail logs from one target at a time'),
    ({
        'controller': False,
        'replica_ids': (1, 2)
    }, 'Can only tail logs from a single replica at a time'),
])
def test_service_logs_reject_invalid_target_selection(kwargs, message):
    call_kwargs = {
        'service_name': 'service-name',
        'follow': False,
        'controller': True,
        'load_balancer': False,
        'replica_ids': (),
        'sync_down': False,
        'tail': None,
        'pool': False,
    }
    call_kwargs.update(kwargs)

    with pytest.raises(click.UsageError, match=message):
        command._handle_serve_logs(**call_kwargs)


def test_service_and_pool_log_help_is_stable():
    runner = cli_testing.CliRunner()

    serve_result = runner.invoke(command.serve, ['logs', '--help'])
    pool_result = runner.invoke(command.jobs, ['pool', 'logs', '--help'])

    assert serve_result.exit_code == 0
    assert 'Usage: serve logs [OPTIONS] SERVICE_NAME [REPLICA_IDS]...' in (
        serve_result.output)
    assert '--load-balancer' in serve_result.output
    assert pool_result.exit_code == 0
    assert 'Usage: jobs pool logs [OPTIONS] POOL_NAME [WORKER_IDS]...' in (
        pool_result.output)
    assert '--load-balancer' not in pool_result.output
