"""Dark Serve039 stateful authority-preflight evaluator tests."""

# pylint: disable=protected-access

from unittest import mock
import uuid

import pytest
import test_serve_resource_action_preflight_v2 as fixtures

from sky.serve import resource_action_authority_state
from sky.serve import resource_action_preflight_v2 as preflight_v2
from sky.serve import resource_action_provider_preflight_v2 as evaluator_v2
from sky.serve import serve_state_schema
from sky.server import runtime

_WORKER_ID = uuid.UUID('11111111-1111-4111-8111-111111111111')


@pytest.mark.parametrize(
    'request_factory,response_type',
    [(fixtures._launch_request,
      preflight_v2.ProviderLaunchAuthorityPreflightResponseV2),
     (fixtures._down_request,
      preflight_v2.ProviderDownAuthorityPreflightResponseV2)])
def test_trusted_request_returns_only_kind_matched_typed_unavailable(
        request_factory, response_type) -> None:
    request = request_factory()
    calls = []

    def validate_trust(candidate, worker_instance_id):
        calls.append((candidate, worker_instance_id))
        return True

    evaluator = evaluator_v2.InitialProviderPreflightEvaluatorV2(
        validate_trust, str(_WORKER_ID))
    response = evaluator(request)

    assert type(response) is response_type
    assert response is not None
    response.validate_request(request)
    assert response.resolved_cohort is None
    assert response.execution_capsule is None
    assert response.executor_policy_proof is None
    assert response.worker_identity is None
    assert calls == [(request, _WORKER_ID)]


def test_untrusted_request_remains_transport_unavailable() -> None:
    request = fixtures._launch_request()
    evaluator = evaluator_v2.InitialProviderPreflightEvaluatorV2(
        lambda candidate, worker_instance_id: False, _WORKER_ID)

    assert evaluator(request) is None


@pytest.mark.parametrize('result', [None, 0, 1, object()])
def test_validator_result_must_be_an_exact_boolean(result) -> None:
    evaluator = evaluator_v2.InitialProviderPreflightEvaluatorV2(
        lambda candidate, worker_instance_id: result, _WORKER_ID)

    with pytest.raises(TypeError, match='must return a Boolean'):
        evaluator(fixtures._launch_request())


@pytest.mark.parametrize('worker_instance_id', [
    1,
    '11111111111141118111111111111111',
    '11111111-1111-4111-8111-11111111111A',
])
def test_worker_identity_is_exact_and_canonical(worker_instance_id) -> None:
    with pytest.raises((TypeError, ValueError)):
        evaluator_v2.InitialProviderPreflightEvaluatorV2(
            lambda candidate, worker_id: True, worker_instance_id)


def test_evaluator_rejects_crossed_request_type_before_validation() -> None:
    called = False

    def validate_trust(candidate, worker_instance_id):
        nonlocal called
        del candidate, worker_instance_id
        called = True
        return True

    evaluator = evaluator_v2.InitialProviderPreflightEvaluatorV2(
        validate_trust, _WORKER_ID)
    with pytest.raises(TypeError, match='invalid type'):
        evaluator(object())
    assert not called


def test_runtime_builder_uses_only_locked_postgres_trust(monkeypatch) -> None:
    database = mock.sentinel.database
    store = mock.Mock()
    store_type = mock.Mock(return_value=store)
    monkeypatch.setattr(serve_state_schema,
                        'get_authority_preflight_database_engine',
                        lambda: database)
    monkeypatch.setattr(resource_action_authority_state,
                        'ServeResourceActionAuthorityStore', store_type)
    request = fixtures._launch_request()

    evaluator = runtime._build_authority_preflight_evaluator_v2(  # pylint: disable=protected-access
        str(_WORKER_ID))
    response = evaluator(request)

    assert response is not None
    response.validate_request(request)
    store_type.assert_called_once_with(database)
    store.validate_preparing_reference_for_preflight.assert_called_once_with(
        worker_instance_id=_WORKER_ID,
        expected_manifest=request.expected_cohort_manifest,
        resource_identity=request.seed.resource_identity,
        action_kind=request.action_kind,
        launch_identity_context=(
            request.seed.source.identity_canonicalization.context))

    store.reset_mock()
    down_request = fixtures._down_request()
    down_response = evaluator(down_request)
    assert down_response is not None
    down_response.validate_request(down_request)
    store.validate_preparing_reference_for_preflight.assert_called_once_with(
        worker_instance_id=_WORKER_ID,
        expected_manifest=down_request.expected_cohort_manifest,
        resource_identity=down_request.seed.resource_identity,
        action_kind=down_request.action_kind,
        launch_identity_context=None)

    store.validate_preparing_reference_for_preflight.side_effect = (
        resource_action_authority_state.AuthorityStateConflict('closed'))
    assert evaluator(request) is None
    assert runtime._AUTHORITY_WORKER_SYNC_POSTGRES_CONNECTION_BUDGET == 3
