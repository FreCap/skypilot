"""Non-PostgreSQL contracts for ordinary Serve launch admission."""
# pylint: disable=protected-access

import asyncio
import json
import types
from unittest import mock
import uuid

import fastapi
from fastapi.testclient import TestClient
import pytest

from sky import models
from sky.client import sdk
from sky.serve import constants as serve_constants
from sky.serve import ordinary_launch_binding
from sky.server import common as server_common
from sky.server import constants as server_constants
from sky.server import core_middleware
from sky.server import server
from sky.server.requests import ordinary_launch as ordinary_launch_request
from sky.server.requests import payloads

_SUBMISSION_UUID = uuid.UUID('11111111-1111-4111-8111-111111111111')
_REPLICA_RECORD_ID = '22222222-2222-4222-8222-222222222222'
_CONTROLLER_INCARNATION = '55555555-5555-4555-8555-555555555555'


def _launch_body(*, retry_until_up: bool = True) -> payloads.LaunchBody:
    return payloads.LaunchBody(
        task='name: task\nresources:\n  cpus: 2\n',
        cluster_name='svc-3',
        retry_until_up=retry_until_up,
        is_launched_by_sky_serve_controller=True,
        extra_launch_context={
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'svc-hash',
            serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_VERSION_KEY: 2,
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_PID_KEY: 123,
            serve_constants.REPLICA_LAUNCH_FENCE_CONTROLLER_IP_KEY: '10.0.0.2',
            ordinary_launch_binding.REPLICA_ID_KEY: 3,
            ordinary_launch_binding.REPLICA_RECORD_ID_KEY: _REPLICA_RECORD_ID,
            ordinary_launch_binding.LIFECYCLE_EPOCH_KEY: 4,
            ordinary_launch_binding.BINDING_EPOCH_KEY: 2,
            ordinary_launch_binding.CONTROLLER_INCARNATION_KEY: _CONTROLLER_INCARNATION,
            ordinary_launch_binding.CONTROLLER_OWNER_EPOCH_KEY: 5,
        },
        env_vars={
            'SKYPILOT_USER_ID': 'submitted-owner',
            'SKYPILOT_USER': 'Submitted Owner',
        })


def _prepared_launch() -> sdk.PreparedLaunchRequest:
    body = _launch_body()
    canonical = json.dumps(json.loads(body.model_dump_json()),
                           sort_keys=True,
                           separators=(',', ':'),
                           ensure_ascii=False,
                           allow_nan=False).encode('utf-8')
    return sdk.PreparedLaunchRequest(submitted_bytes=canonical)


def _binding_response(
        request_id: str = '33333333-3333-4333-8333-333333333333') -> mock.Mock:
    response = mock.Mock()
    response.status_code = 200
    response.json.return_value = {
        'submission_uuid': str(_SUBMISSION_UUID),
        'association_id': '44444444-4444-4444-8444-444444444444',
        'request_id': request_id,
        'launch_generation': 1,
        'created': True,
    }
    return response


def test_sdk_reuses_submission_uuid_and_reads_request_id_from_body(
        monkeypatch: pytest.MonkeyPatch) -> None:
    prepared = _prepared_launch()
    calls: list[dict[str, object]] = []

    def _request(method: str, path: str, **kwargs):
        assert method == 'POST'
        assert path == server_constants.ORDINARY_LAUNCH_BINDING_PATH
        calls.append(json.loads(json.dumps(kwargs['json'])))
        # A transport may retain or mutate its input.  Every retry must still
        # decode a fresh envelope from the immutable prepared launch bytes.
        kwargs['json'].clear()
        return _binding_response()

    monkeypatch.setattr(server_common, 'make_authenticated_request', _request)

    first = sdk.submit_prepared_ordinary_launch_request(prepared,
                                                        _SUBMISSION_UUID)
    second = sdk.submit_prepared_ordinary_launch_request(
        prepared, str(_SUBMISSION_UUID))

    assert first == second == '33333333-3333-4333-8333-333333333333'
    assert calls == [{
        'submission_uuid': str(_SUBMISSION_UUID),
        'launch': json.loads(prepared.submitted_bytes),
    }] * 2


def test_sdk_rejects_noncanonical_or_mismatched_submission_uuid(
        monkeypatch: pytest.MonkeyPatch) -> None:
    prepared = _prepared_launch()
    request = mock.Mock(return_value=_binding_response())
    monkeypatch.setattr(server_common, 'make_authenticated_request', request)

    with pytest.raises(ValueError, match='canonical UUID'):
        sdk.submit_prepared_ordinary_launch_request(prepared,
                                                    f'{{{_SUBMISSION_UUID}}}')
    request.assert_not_called()

    response = _binding_response()
    response.json.return_value['submission_uuid'] = str(uuid.uuid4())
    request.return_value = response
    with pytest.raises(RuntimeError, match='different submission UUID'):
        sdk.submit_prepared_ordinary_launch_request(prepared, _SUBMISSION_UUID)


def test_deterministic_ids_are_retry_stable_and_scope_separated() -> None:
    first = server._derive_ordinary_launch_binding_ids(_SUBMISSION_UUID,
                                                       'tenant-a',
                                                       'workspace-a')
    assert first == server._derive_ordinary_launch_binding_ids(
        _SUBMISSION_UUID, 'tenant-a', 'workspace-a')
    assert first != server._derive_ordinary_launch_binding_ids(
        _SUBMISSION_UUID, 'tenant-b', 'workspace-a')
    assert first != server._derive_ordinary_launch_binding_ids(
        _SUBMISSION_UUID, 'tenant-a', 'workspace-b')
    assert first != server._derive_ordinary_launch_binding_ids(
        uuid.uuid4(), 'tenant-a', 'workspace-a')
    assert all(str(uuid.UUID(value)) == value for value in first)


def test_authenticated_tenant_scope_ignores_submitted_identity() -> None:
    launch_body = _launch_body()
    assert server._resolve_ordinary_launch_tenant_id(
        launch_body, models.User(id='AUTH-OWNER',
                                 name='Auth Owner')) == ('auth-owner')
    assert server._resolve_ordinary_launch_tenant_id(launch_body,
                                                     None) == 'submitted-owner'


def test_transport_request_id_cannot_be_replaced_by_handler(
        monkeypatch: pytest.MonkeyPatch) -> None:
    app = fastapi.FastAPI()
    app.add_middleware(core_middleware.RequestIDMiddleware)

    @app.get('/adopt')
    async def _adopt(request: fastapi.Request):
        request.state.request_id = 'durable-operation-id'
        return {'request_id': request.state.request_id}

    monkeypatch.setattr(core_middleware.requests_lib, 'get_new_request_id',
                        lambda: 'transport-attempt-id')
    response = TestClient(app).get('/adopt')

    assert response.status_code == 200
    assert response.json() == {'request_id': 'durable-operation-id'}
    assert response.headers['X-Skypilot-Request-ID'] == 'transport-attempt-id'


def test_missing_transactional_integration_fails_closed(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server, '_ordinary_launch_request_postgres',
                        types.SimpleNamespace())
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingUnavailable,
                       match='not installed'):
        server._bind_and_enqueue_ordinary_launch(mock.Mock(), mock.Mock())


def test_invalid_transactional_integration_result_fails_closed(
        monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        server, '_ordinary_launch_request_postgres',
        types.SimpleNamespace(bind_and_enqueue_ordinary_launch=lambda *_: None))
    with pytest.raises(ordinary_launch_binding.OrdinaryLaunchBindingUnavailable,
                       match='invalid admission result'):
        server._bind_and_enqueue_ordinary_launch(mock.Mock(), mock.Mock())


@pytest.mark.parametrize('server_owned_key', [
    ordinary_launch_binding.SUBMISSION_ID_KEY,
    ordinary_launch_binding.ASSOCIATION_ID_KEY,
    ordinary_launch_binding.LAUNCH_GENERATION_KEY,
    ordinary_launch_binding.BOUND_REQUEST_ID_KEY,
    ordinary_launch_binding.INPUT_DIGEST_KEY,
    ordinary_launch_binding.OWNER_REVISION_KEY,
])
def test_endpoint_rejects_server_owned_launch_context(
        server_owned_key: str) -> None:
    launch_body = _launch_body()
    launch_body.extra_launch_context[server_owned_key] = str(_SUBMISSION_UUID)
    submission = server._OrdinaryServeLaunchSubmission(
        submission_uuid=str(_SUBMISSION_UUID), launch=launch_body)
    request = types.SimpleNamespace(state=types.SimpleNamespace(
        request_id='transport-attempt-id', auth_user=None))

    with pytest.raises(fastapi.HTTPException) as raised:
        asyncio.run(server.ordinary_serve_launch(submission, request))

    assert raised.value.status_code == 409
    assert 'server-generated' in raised.value.detail


def test_endpoint_uses_derived_ids_distinct_handler_and_no_generic_retry(
        monkeypatch: pytest.MonkeyPatch) -> None:
    launch_body = _launch_body(retry_until_up=True)
    submission = server._OrdinaryServeLaunchSubmission(
        submission_uuid=str(_SUBMISSION_UUID), launch=launch_body)
    auth_user = models.User(id='tenant-a', name='Tenant A')
    request = types.SimpleNamespace(state=types.SimpleNamespace(
        request_id='transport-attempt-id', auth_user=auth_user))
    association_id, request_id = server._derive_ordinary_launch_binding_ids(
        _SUBMISSION_UUID, auth_user.id, 'workspace-a')
    observed: dict[str, object] = {}

    async def _build_request_async(**kwargs):
        observed.update(kwargs)
        return types.SimpleNamespace(request_id=kwargs['request_id'],
                                     request_body=kwargs['request_body'],
                                     log_path=mock.Mock())

    def _bind_request(request_task, identity):
        del request_task
        assert str(identity.association_id) == association_id
        assert identity.request_id == request_id
        return ordinary_launch_binding.BindingAdmission(
            disposition=ordinary_launch_binding.AdmissionDisposition.
            EXISTING_EXACT,
            association_id=association_id,
            request_id=request_id,
            launch_generation=7,
            owner_revision=2,
            resolution=ordinary_launch_binding.Resolution.BOUND,
            effect_phase=ordinary_launch_binding.EffectPhase.NOT_STARTED)

    monkeypatch.setattr(server.serve_state,
                        'get_service_config_recovery_identity',
                        lambda _service_name: ('svc-hash', 'workspace-a'))
    monkeypatch.setattr(server.executor, 'build_request_async',
                        _build_request_async)
    monkeypatch.setattr(server, '_bind_and_enqueue_ordinary_launch',
                        _bind_request)

    response = asyncio.run(server.ordinary_serve_launch(submission, request))

    assert observed['request_id'] == request_id
    assert observed['func'] is ordinary_launch_request.launch
    assert observed['retryable'] is False
    assert observed['should_enqueue'] is True
    assert observed['request_body'].retry_until_up is True
    assert observed['precondition'].request_id == request_id
    assert observed['precondition'].association_id == association_id
    assert request.state.request_id == 'transport-attempt-id'
    assert response.model_dump(mode='json') == {
        'submission_uuid': str(_SUBMISSION_UUID),
        'association_id': association_id,
        'request_id': request_id,
        'launch_generation': 7,
        'created': False,
    }
