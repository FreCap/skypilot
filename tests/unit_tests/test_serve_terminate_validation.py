"""Tests for destructive Serve controller request validation."""
# pylint: disable=protected-access

from unittest import mock

import fastapi
import pytest

from sky.serve import constants
from sky.serve import controller
from sky.serve import serve_rpc_utils
from sky.serve import serve_utils


@pytest.mark.asyncio
async def test_payload_requires_valid_json():
    request = mock.Mock()
    request.json = mock.AsyncMock(side_effect=ValueError('invalid JSON'))

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await controller._read_terminate_replica_payload(request)
    assert exc_info.value.status_code == 400


@pytest.mark.parametrize('request_data', [None, [], 'replica', 1, True])
def test_payload_requires_a_json_object(request_data):
    with pytest.raises(fastapi.HTTPException) as exc_info:
        controller._validate_terminate_replica_payload(request_data)
    assert exc_info.value.status_code == 400


@pytest.mark.parametrize('replica_id', [True, False, '1', 1.0, None])
def test_replica_id_requires_an_exact_integer(replica_id):
    with pytest.raises(fastapi.HTTPException) as exc_info:
        controller._validate_terminate_replica_payload({
            'replica_id': replica_id,
            'purge': False,
        })
    assert exc_info.value.status_code == 400


@pytest.mark.parametrize('purge', ['false', 0, 1, None])
def test_purge_requires_an_exact_boolean(purge):
    with pytest.raises(fastapi.HTTPException) as exc_info:
        controller._validate_terminate_replica_payload({
            'replica_id': 1,
            'purge': purge,
        })
    assert exc_info.value.status_code == 400


def test_valid_terminate_payload_is_preserved():
    assert controller._validate_terminate_replica_payload({
        'replica_id': 1,
        'purge': True,
    }) == (1, True)


def test_missing_replica_returns_not_found(monkeypatch):
    lookup = mock.Mock(return_value=None)
    monkeypatch.setattr(controller.serve_state, 'get_replica_info_from_id',
                        lookup)

    with pytest.raises(fastapi.HTTPException) as exc_info:
        controller._get_replica_info_for_termination('service', 17)
    assert exc_info.value.status_code == 404
    lookup.assert_called_once_with('service', 17)


def test_already_terminating_replica_remains_conflict(monkeypatch):
    replica_info = mock.Mock(
        status=controller.serve_state.ReplicaStatus.SHUTTING_DOWN)
    monkeypatch.setattr(controller.serve_state, 'get_replica_info_from_id',
                        mock.Mock(return_value=replica_info))
    replica_manager = mock.Mock()

    response = controller._terminate_replica_sync('service', replica_manager,
                                                  17, False)

    assert response.status_code == 409
    replica_manager.scale_down.assert_not_called()


def test_failed_replica_requires_purge(monkeypatch):
    replica_info = mock.Mock(
        status=controller.serve_state.ReplicaStatus.FAILED_PROVISION)
    monkeypatch.setattr(controller.serve_state, 'get_replica_info_from_id',
                        mock.Mock(return_value=replica_info))
    replica_manager = mock.Mock()

    response = controller._terminate_replica_sync('service', replica_manager,
                                                  17, False)

    assert response.status_code == 409
    replica_manager.scale_down.assert_not_called()


def test_failed_replica_can_still_be_purged(monkeypatch):
    replica_info = mock.Mock(
        status=controller.serve_state.ReplicaStatus.FAILED_PROVISION)
    monkeypatch.setattr(controller.serve_state, 'get_replica_info_from_id',
                        mock.Mock(return_value=replica_info))
    replica_manager = mock.Mock()

    response = controller._terminate_replica_sync('service', replica_manager,
                                                  17, True)

    assert response.status_code == 200
    replica_manager.scale_down.assert_called_once_with(17, purge=True)


@pytest.mark.parametrize('body, status_code', [
    ({
        'detail': 'Replica 17 does not exist.'
    }, 404),
    ({
        'detail': 'replica_id must be an integer.'
    }, 400),
    ({
        'message': 'Internal error.'
    }, 500),
])
def test_client_surfaces_controller_error_bodies(monkeypatch, body,
                                                 status_code):
    """The client must tolerate both FastAPI HTTPException bodies
    ({'detail': ...}) and the controller's generic handler bodies
    ({'message': ...}) without crashing."""
    monkeypatch.setattr(serve_utils, '_get_service_status',
                        mock.Mock(return_value={'hash': 'h'}))
    monkeypatch.setattr(serve_utils.serve_state, 'get_replica_info_from_id',
                        mock.Mock(return_value=object()))
    resp = mock.Mock(status_code=status_code)
    resp.json.return_value = body
    monkeypatch.setattr(serve_utils, '_post_to_controller_with_retry',
                        mock.Mock(return_value=resp))

    with pytest.raises(ValueError):
        serve_utils.terminate_replica('service', 17, purge=False)


def test_client_surfaces_non_json_error_body(monkeypatch):
    monkeypatch.setattr(serve_utils, '_get_service_status',
                        mock.Mock(return_value={'hash': 'h'}))
    monkeypatch.setattr(serve_utils.serve_state, 'get_replica_info_from_id',
                        mock.Mock(return_value=object()))
    resp = mock.Mock(status_code=502, text='bad gateway')
    resp.json.side_effect = ValueError('not json')
    monkeypatch.setattr(serve_utils, '_post_to_controller_with_retry',
                        mock.Mock(return_value=resp))

    with pytest.raises(ValueError):
        serve_utils.terminate_replica('service', 17, purge=False)


def test_client_uses_termination_acceptance_timeout(monkeypatch):
    monkeypatch.setattr(serve_utils, '_get_service_status',
                        mock.Mock(return_value={'hash': 'h'}))
    monkeypatch.setattr(serve_utils.serve_state, 'get_replica_info_from_id',
                        mock.Mock(return_value=object()))
    response = mock.Mock(status_code=200)
    response.json.return_value = {'message': 'scheduled'}
    post = mock.Mock(return_value=response)
    monkeypatch.setattr(serve_utils, '_post_to_controller_with_retry', post)

    assert serve_utils.terminate_replica('service', 17,
                                         purge=False) == 'scheduled'

    post.assert_called_once_with(
        'service',
        'h',
        '/controller/terminate_replica',
        json={
            'replica_id': 17,
            'purge': False,
        },
        timeout=(serve_utils._CONTROLLER_HTTP_TIMEOUT_SECONDS[0],
                 constants.TERMINATE_REPLICA_TIMEOUT_SECONDS))
    assert serve_utils._CONTROLLER_HTTP_RETRY_ATTEMPTS == 1


def test_rpc_runner_terminates_without_transport_replay(monkeypatch):
    handle = mock.Mock(is_grpc_enabled_with_flag=True)
    client = mock.Mock()
    client.terminate_replica.return_value = mock.Mock(message='scheduled')
    monkeypatch.setattr(serve_rpc_utils.backends, 'SkyletClient',
                        mock.Mock(return_value=client))
    request = mock.Mock()
    monkeypatch.setattr(serve_rpc_utils.servev1_pb2, 'TerminateReplicaRequest',
                        mock.Mock(return_value=request))
    invoke = mock.Mock(side_effect=lambda operation, max_attempts: operation())
    monkeypatch.setattr(serve_rpc_utils.backend_utils,
                        'invoke_skylet_with_retries', invoke)

    assert serve_rpc_utils.RpcRunner.terminate_replica(
        handle, 'service', 17, purge=False) == 'scheduled'

    invoke.assert_called_once()
    assert invoke.call_args.kwargs == {'max_attempts': 1}
    client.terminate_replica.assert_called_once_with(request)
