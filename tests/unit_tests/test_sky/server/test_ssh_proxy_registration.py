"""Characterization tests for cluster SSH proxy route registration."""

# pylint: disable=protected-access

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
from sky.server import constants as server_constants
from sky.server import server
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
