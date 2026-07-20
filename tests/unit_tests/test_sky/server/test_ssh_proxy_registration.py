"""Characterization tests for cluster SSH proxy route registration."""

# pylint: disable=protected-access

import asyncio
import inspect
import json
import pickle
import struct
from types import SimpleNamespace
from unittest import mock

import fastapi
import pytest

from sky import clouds
from sky import core
from sky.metrics import utils as metrics_utils
from sky.server import constants as server_constants
from sky.server import server
from sky.server import ssh_proxy
from sky.server import websocket_utils
from sky.utils import context_utils
from sky.utils import status_lib


def test_cluster_ssh_proxy_routes_preserve_order_and_callable_identity():
    registered_routes = []
    for route in server.app.routes:
        original_router = getattr(route, 'original_router', None)
        if original_router is None:
            registered_routes.append(route)
        else:
            registered_routes.extend(original_router.routes)

    route_paths = [getattr(route, 'path', None) for route in registered_routes]
    kubernetes_index = route_paths.index('/kubernetes-pod-ssh-proxy')

    assert route_paths[kubernetes_index:kubernetes_index + 4] == [
        '/kubernetes-pod-ssh-proxy',
        '/slurm-job-ssh-proxy',
        '/ssh-interactive-auth',
        '/all_contexts',
    ]

    routes = registered_routes[kubernetes_index:kubernetes_index + 2]
    assert routes[0].endpoint is server.kubernetes_pod_ssh_proxy
    assert routes[1].endpoint is server.slurm_job_ssh_proxy
    assert server.SSHMessageType is websocket_utils.SSHMessageType


def test_cluster_ssh_proxy_facade_preserves_signatures_and_pickle_identity():
    expected_signatures = {
        '_get_cluster_and_validate':
            ("(cluster_name: str, cloud_type: type[sky.clouds.cloud.Cloud]) -> "
             "'backends.CloudVmRayResourceHandle'"),
        'kubernetes_pod_ssh_proxy':
            ('(websocket: starlette.websockets.WebSocket, cluster_name: str, '
             'client_version: int | None = None, no_redirect: int | None = '
             'None) -> None'),
        'slurm_job_ssh_proxy':
            ('(websocket: starlette.websockets.WebSocket, cluster_name: str, '
             'worker: int = 0, client_version: int | None = None) -> None'),
    }

    for name, expected_signature in expected_signatures.items():
        symbol = getattr(server, name)
        assert symbol.__module__ == 'sky.server.server'
        assert str(inspect.signature(symbol)) == expected_signature
        assert pickle.loads(pickle.dumps(symbol)) is symbol


@pytest.mark.asyncio
async def test_cluster_validation_uses_one_summary_status_read():
    handle = SimpleNamespace(launched_resources=SimpleNamespace(
        cloud=clouds.Kubernetes()))
    cluster_record = {
        'status': status_lib.ClusterStatus.AUTOSTOPPING,
        'handle': handle,
    }

    with mock.patch.object(
            context_utils,
            'to_thread_with_executor',
            new=mock.AsyncMock(return_value=[cluster_record])) as status_read:
        result = await server._get_cluster_and_validate('cluster',
                                                        clouds.Kubernetes)

    assert result is handle
    args, kwargs = status_read.await_args
    assert args[1:] == (core.status, 'cluster')
    assert kwargs == {'all_users': True, 'summary_response': True}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('records', 'status_code'),
    [
        ([], 404),
        ([{
            'status': status_lib.ClusterStatus.STOPPED,
            'handle': None,
        }], 400),
    ],
)
async def test_cluster_validation_preserves_error_mapping(records, status_code):
    with mock.patch.object(context_utils,
                           'to_thread_with_executor',
                           new=mock.AsyncMock(return_value=records)):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            await server._get_cluster_and_validate('cluster', clouds.Kubernetes)

    assert exc_info.value.status_code == status_code


@pytest.mark.asyncio
async def test_kubernetes_proxy_redirect_short_circuits_cluster_lookup():
    websocket = mock.AsyncMock(spec=fastapi.WebSocket)
    redirect_info = {'url': 'wss://redirect.example.test'}
    redirect_hook = mock.AsyncMock(return_value=redirect_info)

    with mock.patch.object(websocket_utils, 'ssh_redirect_hook', redirect_hook), \
         mock.patch.object(core, 'status') as status:
        await server.kubernetes_pod_ssh_proxy(
            websocket,
            'cluster',
            client_version=server_constants.MIN_SSH_REDIRECT_PROTOCOL_VERSION,
        )

    redirect_hook.assert_awaited_once_with(websocket, 'cluster')
    websocket.accept.assert_awaited_once_with()
    websocket.send_bytes.assert_awaited_once_with(
        struct.pack('!B', server.SSHMessageType.REDIRECT) +
        json.dumps(redirect_info).encode())
    websocket.close.assert_awaited_once_with()
    status.assert_not_called()


def _metric_mocks():
    connection_gauge = mock.Mock()
    connection_metric = mock.Mock()
    connection_metric.labels.return_value = connection_gauge
    close_counter = mock.Mock()
    close_metric = mock.Mock()
    close_metric.labels.return_value = close_counter
    return connection_metric, connection_gauge, close_metric, close_counter


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('process_exited', 'ssh_failed', 'expected_reason'),
    [
        (True, False, 'KubectlPortForwardExit'),
        (False, False, 'ClientClosed'),
        (False, True, 'SSHToPodDisconnected'),
    ],
)
async def test_kubernetes_proxy_records_one_terminal_close(
        process_exited, ssh_failed, expected_reason):
    websocket = mock.AsyncMock(spec=fastapi.WebSocket)
    runner = mock.Mock()
    runner.port_forward_command.return_value = ['kubectl', 'port-forward']
    handle = SimpleNamespace(
        head_ssh_port=22,
        get_command_runners=mock.Mock(return_value=[runner]),
    )
    proc = SimpleNamespace(
        stdout=object(),
        terminate=mock.Mock(
            side_effect=ProcessLookupError if process_exited else None),
        wait=mock.Mock(return_value=0),
        kill=mock.Mock(),
    )
    stdout_reader = SimpleNamespace(
        readline=mock.AsyncMock(
            return_value=b'Forwarding from 127.0.0.1:41234 -> 22\n'),
        read=mock.AsyncMock(return_value=b''),
    )
    event_loop = SimpleNamespace(
        run_in_executor=mock.AsyncMock(side_effect=[proc, 0]),
        connect_read_pipe=mock.AsyncMock(),
    )
    backend_reader = SimpleNamespace(read=mock.AsyncMock(return_value=b''))
    backend_writer = SimpleNamespace(write=mock.Mock(),
                                     drain=mock.AsyncMock(),
                                     close=mock.Mock())
    (connection_metric, connection_gauge, close_metric,
     close_counter) = _metric_mocks()

    with mock.patch.object(ssh_proxy, '_get_cluster_and_validate',
                           new=mock.AsyncMock(return_value=handle)), \
         mock.patch.object(ssh_proxy, '_KUBECTL_PATH', '/usr/bin/kubectl'), \
         mock.patch.object(asyncio, 'get_running_loop',
                           return_value=event_loop), \
         mock.patch.object(asyncio, 'StreamReader',
                           return_value=stdout_reader), \
         mock.patch.object(asyncio, 'StreamReaderProtocol'), \
         mock.patch.object(asyncio, 'open_connection',
                           new=mock.AsyncMock(return_value=(backend_reader,
                                                           backend_writer))), \
         mock.patch.object(websocket_utils, 'run_websocket_proxy',
                           new=mock.AsyncMock(return_value=ssh_failed)), \
         mock.patch.object(metrics_utils,
                           'SKY_APISERVER_WEBSOCKET_CONNECTIONS',
                           connection_metric), \
         mock.patch.object(metrics_utils,
                           'SKY_APISERVER_WEBSOCKET_CLOSED_TOTAL',
                           close_metric):
        await server.kubernetes_pod_ssh_proxy(websocket, 'cluster')

    connection_gauge.inc.assert_called_once_with()
    connection_gauge.dec.assert_called_once_with()
    close_metric.labels.assert_called_once_with(pid=mock.ANY,
                                                reason=expected_reason)
    close_counter.inc.assert_called_once_with()
    assert event_loop.run_in_executor.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('process_exited', 'ssh_failed', 'expected_reason'),
    [
        (True, False, 'SrunProcessExit'),
        (False, False, 'ClientClosed'),
        (False, True, 'SSHToSlurmJobDisconnected'),
    ],
)
async def test_slurm_proxy_records_one_terminal_close(process_exited,
                                                      ssh_failed,
                                                      expected_reason):
    websocket = mock.AsyncMock(spec=fastapi.WebSocket)
    head = SimpleNamespace(tags={'job_id': '42'})
    node = SimpleNamespace(tags={'node': 'node-0'})
    cluster_info = SimpleNamespace(
        provider_config={
            'ssh': {
                'hostname': 'login',
                'port': 22,
                'user': 'sky',
            }
        },
        get_head_instance=mock.Mock(return_value=head),
        instances={'instance': (node,)},
    )
    handle = SimpleNamespace(
        cached_cluster_info=cluster_info,
        launched_resources=SimpleNamespace(extract_docker_image=mock.Mock(
            return_value=None)),
        cluster_name_on_cloud='cluster-on-cloud',
    )
    proc = SimpleNamespace(
        stdin=SimpleNamespace(write=mock.Mock(),
                              drain=mock.AsyncMock(),
                              close=mock.Mock()),
        stdout=SimpleNamespace(read=mock.AsyncMock(return_value=b'')),
        stderr=SimpleNamespace(readline=mock.AsyncMock(return_value=b'')),
        terminate=mock.Mock(
            side_effect=ProcessLookupError if process_exited else None),
    )
    login_runner = mock.Mock()
    login_runner.ssh_base_command.return_value = ['ssh']
    (connection_metric, connection_gauge, close_metric,
     close_counter) = _metric_mocks()

    with mock.patch.object(ssh_proxy, '_get_cluster_and_validate',
                           new=mock.AsyncMock(return_value=handle)), \
         mock.patch.object(ssh_proxy.command_runner, 'SSHCommandRunner',
                           return_value=login_runner), \
         mock.patch.object(ssh_proxy.slurm_utils, 'srun_sshd_command',
                           return_value='srun-sshd'), \
         mock.patch.object(asyncio, 'create_subprocess_shell',
                           new=mock.AsyncMock(return_value=proc)), \
         mock.patch.object(websocket_utils, 'run_websocket_proxy',
                           new=mock.AsyncMock(return_value=ssh_failed)), \
         mock.patch.object(ssh_proxy.env_options.Options.SHOW_DEBUG_INFO,
                           'get', return_value=False), \
         mock.patch.object(metrics_utils,
                           'SKY_APISERVER_WEBSOCKET_CONNECTIONS',
                           connection_metric), \
         mock.patch.object(metrics_utils,
                           'SKY_APISERVER_WEBSOCKET_CLOSED_TOTAL',
                           close_metric):
        await server.slurm_job_ssh_proxy(websocket, 'cluster')

    connection_gauge.inc.assert_called_once_with()
    connection_gauge.dec.assert_called_once_with()
    close_metric.labels.assert_called_once_with(pid=mock.ANY,
                                                reason=expected_reason)
    close_counter.inc.assert_called_once_with()
