"""Raw HTTP tests for the no-enqueue launch identity canonicalizer."""

import dataclasses
import hashlib
from unittest import mock

import fastapi
from fastapi import testclient
import pytest
import sqlalchemy

from sky import models
from sky.serve import resource_action_state
from sky.serve import resource_actions as actions
from sky.server.requests import launch_identity
from sky.server.requests import resource_actions as kernel_actions

_SERVICE_UUID = '11111111-1111-4111-8111-111111111111'
_REPLICA_UUID = '22222222-2222-4222-8222-222222222222'
_CAPABILITY = '12' * 32
_PATH = '/internal/resource-actions/v1/launch-identity/canonicalize'


def _typed_request(
    *,
    capability: str = _CAPABILITY
) -> actions.ProviderLaunchIdentityCanonicalizationRequestV1:
    identity = actions.ProviderResourceIdentityV1.from_value({
        'service_hash': _SERVICE_UUID,
        'service_incarnation': _SERVICE_UUID,
        'replica_id': 7,
        'replica_incarnation': _REPLICA_UUID,
        'desired_generation': 3,
    })
    canonical_input = actions.ProviderLaunchIdentityCanonicalizationInputV1(
        version=1,
        contract='api_server_effective_launch_identity_v1',
        service_name='svc',
        resource_identity=identity,
        prepared_original_user='prepared@example.com',
        prepared_user_hash='prepared-hash')
    context = actions.ProviderLaunchIdentityCanonicalizationContextV1(
        version=1,
        decision_id=identity.action_identity(
            kernel_actions.ActionKind.LAUNCH).action_id,
        cohort_id='authority-v1',
        action_type=kernel_actions.ActionKind.LAUNCH,
        controller_owner_fence='123:10.0.0.1',
        lifecycle_epoch=4,
        preparation_reference_revision=1,
        reference_state=actions.WorkerCohortReferenceState.PREPARING,
        preparation_capability_sha256=hashlib.sha256(
            bytes.fromhex(_CAPABILITY)).hexdigest(),
        input=canonical_input,
        input_sha256=canonical_input.sha256)
    return actions.ProviderLaunchIdentityCanonicalizationRequestV1(
        version=1,
        context=context,
        context_sha256=context.sha256,
        preparation_capability=capability)


class _FakeStore:

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.requests = []

    def validate_launch_identity_canonicalization(self, request) -> None:
        self.requests.append(request)
        if self.error is not None:
            raise self.error


def _client(monkeypatch,
            store: _FakeStore,
            auth_user: models.User | None = None) -> testclient.TestClient:
    app = fastapi.FastAPI()

    @app.middleware('http')
    async def _identity_state(request, call_next):
        request.state.auth_user = auth_user
        return await call_next(request)

    app.include_router(launch_identity.router)
    monkeypatch.setattr(launch_identity, '_state_store', lambda: store)
    return testclient.TestClient(app)


def _post(client: testclient.TestClient,
          body: bytes,
          headers: dict[str, str] | None = None):
    effective_headers = {'Content-Type': 'application/json'}
    if headers is not None:
        effective_headers.update(headers)
    return client.post(_PATH, content=body, headers=effective_headers)


def test_endpoint_returns_exact_no_auth_proof_without_enqueue(
        monkeypatch) -> None:
    store = _FakeStore()
    request = _typed_request()
    response = _post(_client(monkeypatch, store), request.canonical_bytes)

    assert response.status_code == 200
    assert response.headers['content-type'] == 'application/json'
    typed_response = (
        actions.ProviderLaunchIdentityCanonicalizationResponseV1.from_value(
            response.json()))
    assert response.content == typed_response.canonical_bytes
    assert typed_response.decision_id == request.context.decision_id
    assert typed_response.context_sha256 == request.context_sha256
    assert typed_response.proof.effective_original_user == (
        'prepared@example.com')
    assert typed_response.proof.effective_user_hash == 'prepared-hash'
    assert _CAPABILITY.encode() not in response.content
    assert store.requests == [request]


def test_endpoint_authenticated_identity_replaces_both_submitted_values(
        monkeypatch) -> None:
    store = _FakeStore()
    auth_user = models.User(id='AUTH-HASH',
                            name='Authenticated User',
                            user_type=models.UserType.SSO.value)
    response = _post(_client(monkeypatch, store, auth_user),
                     _typed_request().canonical_bytes)

    assert response.status_code == 200
    proof = actions.ProviderLaunchIdentityCanonicalizationResponseV1.from_value(
        response.json()).proof
    assert proof.effective_original_user == 'Authenticated User'
    assert proof.effective_user_hash == 'auth-hash'


@pytest.mark.parametrize('headers', [
    {
        'Content-Type': 'application/json; charset=utf-8'
    },
    {
        'Content-Type': 'text/json'
    },
    {
        'Content-Encoding': 'identity'
    },
    {
        'Content-Encoding': 'gzip'
    },
])
def test_endpoint_requires_exact_unencoded_json(monkeypatch, headers) -> None:
    store = _FakeStore()
    response = _post(_client(monkeypatch, store),
                     _typed_request().canonical_bytes, headers)
    assert response.status_code == 415
    assert not store.requests


@pytest.mark.parametrize('mutate', [
    lambda body: body + b'\n',
    lambda body: body.replace(b'{"context":', b'{"extra":null,"context":', 1),
    lambda body: body.replace(b'{"context":', b'{"context":null,"context":', 1),
    lambda body: b'\xff' + body,
])
def test_endpoint_rejects_noncanonical_or_malformed_bytes(monkeypatch,
                                                          mutate) -> None:
    store = _FakeStore()
    response = _post(_client(monkeypatch, store),
                     mutate(_typed_request().canonical_bytes))
    assert response.status_code == 400
    assert not store.requests


def test_endpoint_enforces_preparse_body_limit(monkeypatch) -> None:
    store = _FakeStore()
    client = _client(monkeypatch, store)
    assert _post(client, b' ' * 65_536).status_code == 400
    assert _post(client, b' ' * 65_537).status_code == 413
    response = _post(client,
                     _typed_request().canonical_bytes,
                     {'Content-Length': '65537'})
    assert response.status_code == 413
    assert not store.requests


@pytest.mark.parametrize('error,status', [
    (resource_action_state.PreparationCapabilityMismatch('mismatch'), 403),
    (kernel_actions.ClaimLost('stale'), 409),
    (kernel_actions.InvariantViolation('invalid row'), 409),
    (sqlalchemy.exc.OperationalError('select', {}, RuntimeError('down')), 503),
    (sqlalchemy.exc.TimeoutError('pool exhausted'), 503),
])
def test_endpoint_maps_store_failures_exactly(monkeypatch, error,
                                              status: int) -> None:
    store = _FakeStore(error)
    response = _post(_client(monkeypatch, store),
                     _typed_request(capability='34' * 32).canonical_bytes)
    assert response.status_code == status
    assert len(store.requests) == 1


@pytest.mark.parametrize('auth_user,prepared_hash', [
    (models.User(id='valid-hash', name='nón-ascii'), 'prepared-hash'),
    (None, '-invalid'),
])
def test_endpoint_rejects_invalid_effective_identity_after_read_validation(
        monkeypatch, auth_user, prepared_hash: str) -> None:
    store = _FakeStore()
    request = _typed_request()
    if auth_user is None:
        changed_input = dataclasses.replace(request.context.input,
                                            prepared_user_hash=prepared_hash)
        changed_context = dataclasses.replace(request.context,
                                              input=changed_input,
                                              input_sha256=changed_input.sha256)
        request = dataclasses.replace(request,
                                      context=changed_context,
                                      context_sha256=changed_context.sha256)
    response = _post(_client(monkeypatch, store, auth_user),
                     request.canonical_bytes)
    assert response.status_code == 400
    assert len(store.requests) == 1


def test_endpoint_missing_auth_middleware_state_fails_closed(
        monkeypatch) -> None:
    store = _FakeStore()
    app = fastapi.FastAPI()
    app.include_router(launch_identity.router)
    monkeypatch.setattr(launch_identity, '_state_store', lambda: store)
    client = testclient.TestClient(app, raise_server_exceptions=False)

    response = _post(client, _typed_request().canonical_bytes)

    assert response.status_code == 500
    assert not store.requests


def test_endpoint_never_calls_request_executor(monkeypatch) -> None:
    store = _FakeStore()
    forbidden = mock.Mock(side_effect=AssertionError('must not enqueue'))
    monkeypatch.setattr('sky.server.requests.executor.prepare_request_async',
                        forbidden)
    response = _post(_client(monkeypatch, store),
                     _typed_request().canonical_bytes)
    assert response.status_code == 200
    forbidden.assert_not_called()
