"""Characterization tests for the CLI service-status response gateway."""

# pylint: disable=protected-access

import inspect
import pickle
import subprocess
import sys
import textwrap
import typing
from unittest import mock

import click
import pytest

from sky import exceptions
from sky.client.cli import command
from sky.client.cli import service_status
from sky.server import common as server_common
from sky.utils import status_lib


def _call_handler(*,
                  records,
                  service_names=None,
                  show_all=False,
                  show_endpoint=False,
                  pool=False,
                  is_called_by_user=True):
    with mock.patch.object(command.sdk, 'get', return_value=records) as get, \
         mock.patch.object(command.serve_lib,
                           'format_service_table',
                           return_value='TABLE') as format_table, \
         mock.patch.object(command.usage_lib.messages.usage,
                           'set_internal') as set_internal:
        result = command._handle_services_request(
            'request-id',
            service_names=service_names,
            show_all=show_all,
            show_endpoint=show_endpoint,
            pool=pool,
            is_called_by_user=is_called_by_user)
        get.assert_called_once_with('request-id')
    return result, format_table, set_internal


def test_historical_function_contract_and_pickle_identity():
    handler = command._handle_services_request
    assert handler is service_status._handle_services_request
    assert handler.__module__ == 'sky.client.cli.command'
    assert handler.__qualname__ == '_handle_services_request'
    signature = inspect.signature(handler)
    assert list(signature.parameters) == [
        'request_id', 'service_names', 'show_all', 'show_endpoint', 'pool',
        'is_called_by_user'
    ]
    assert signature.parameters['request_id'].annotation == (
        server_common.RequestId[list[dict[str, typing.Any]]])
    assert signature.parameters['service_names'].annotation == (list[str] |
                                                                None)
    assert signature.parameters['pool'].default is False
    assert signature.parameters['is_called_by_user'].default is False
    assert signature.return_annotation == tuple[int | None, str]
    assert pickle.loads(pickle.dumps(handler)) is handler


@pytest.mark.parametrize('module_order', [
    ('sky.client.cli.command', 'sky.client.cli.service_status'),
    ('sky.client.cli.service_status', 'sky.client.cli.command'),
])
def test_historical_function_contract_in_fresh_process(module_order):
    program = textwrap.dedent(f'''\
        import importlib
        import pickle

        for module_name in {module_order!r}:
            importlib.import_module(module_name)

        from sky.client.cli import command
        from sky.client.cli import service_status

        handler = command._handle_services_request
        assert handler is service_status._handle_services_request
        assert handler.__module__ == 'sky.client.cli.command'
        assert handler.__qualname__ == '_handle_services_request'
        assert pickle.loads(pickle.dumps(handler)) is handler
    ''')
    subprocess.run([sys.executable, '-c', program], check=True)


def test_table_projection_preserves_counts_arguments_and_missing_names():
    records = [{'name': 'present'}]
    (num_services, msg), format_table, set_internal = _call_handler(
        records=records,
        service_names=['present', 'missing'],
        show_all=True,
        pool=True)

    assert num_services == 1
    assert msg == "TABLE\n\nPool 'missing' not found."
    format_table.assert_called_once_with(records, True, True)
    set_internal.assert_not_called()


def test_internal_call_marks_usage_once():
    (_,
     msg), format_table, set_internal = _call_handler(records=[],
                                                      is_called_by_user=False)

    assert msg == 'TABLE'
    format_table.assert_called_once_with([], False, False)
    set_internal.assert_called_once_with()


@pytest.mark.parametrize(('records', 'expected'), [
    ([{
        'endpoint': 'https://example.test'
    }], 'https://example.test'),
    ([{
        'endpoint': None
    }], '-'),
])
def test_single_endpoint_projection(records, expected):
    (num_services, msg), format_table, _ = _call_handler(records=records,
                                                         show_endpoint=True)

    assert num_services == 1
    assert msg == expected
    format_table.assert_not_called()


@pytest.mark.parametrize(('records', 'message'), [
    ([], 'No service found.'),
    ([{
        'endpoint': 'one'
    }, {
        'endpoint': 'two'
    }], '2 services found.'),
])
def test_endpoint_projection_requires_exactly_one_service(records, message):
    with pytest.raises(click.UsageError) as exc_info:
        _call_handler(records=records, show_endpoint=True)
    assert message in str(exc_info.value)


def test_cluster_not_up_preserves_hint_without_controller_probe():
    error = exceptions.ClusterNotUpError('controller unavailable')
    error.cluster_status = None
    with mock.patch.object(command.sdk, 'get', side_effect=error), \
         mock.patch.object(command.sdk, 'status') as status:
        num_services, msg = command._handle_services_request(
            'request-id',
            service_names=None,
            show_all=False,
            show_endpoint=False,
            is_called_by_user=True)

    assert num_services is None
    assert 'controller unavailable' in msg
    assert 'sky serve -h' in msg
    status.assert_not_called()


@pytest.mark.parametrize(('pool', 'prefix'), [
    (False, 'sky-serve-controller-*'),
    (True, 'sky-jobs-controller-*'),
])
def test_stopped_controller_fallback_uses_matching_controller(pool, prefix):
    with mock.patch.object(
            command.sdk,
            'get',
            side_effect=[RuntimeError('transport failed'), [{
                'status': status_lib.ClusterStatus.STOPPED
            }]]), \
         mock.patch.object(command.sdk,
                           'status',
                           return_value='controller-request') as status:
        num_services, msg = command._handle_services_request(
            'request-id',
            service_names=None,
            show_all=False,
            show_endpoint=False,
            pool=pool,
            is_called_by_user=True)

    assert num_services is None
    assert msg
    status.assert_called_once_with(cluster_names=[prefix], all_users=True)
