"""Closed registration and SQL routing tests for authority executors."""
# pylint: disable=protected-access

import json
from unittest import mock
import uuid

import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.server.requests import authority_worker
from sky.server.requests import postgres as request_postgres
from sky.server.requests import registry
from sky.server.requests import resource_actions as kernel_actions


def _handler() -> None:
    pass


def _claim_config() -> authority_worker.AuthorityWorkerClaimConfig:
    identity = {
        'version': 1,
        'manifest': {
            'cohort_id': 'authority-v1',
        },
        'manifest_sha256': 'a' * 64,
        'deployment_uid': 'deployment-uid-v1',
        'service_account_uid': 'service-account-uid-v1',
    }
    identity_bytes = kernel_actions.canonical_json_bytes(identity)
    return authority_worker.AuthorityWorkerClaimConfig(
        routing=authority_worker.AuthorityWorkerRoutingConfig(
            cohort_id='authority-v1',
            namespace='skypilot-system',
            deployment_name='skypilot-authority-v1',
            service_account_name='skypilot-authority-v1',
            image='registry.example/authority@sha256:' + '1' * 64),
        active_cohort_id='authority-v1',
        cohort_identity_bytes=identity_bytes,
        cohort_identity_sha256=kernel_actions.canonical_sha256(identity),
        deployment_uid='deployment-uid-v1',
        lifecycle_state='ACCEPTING')


@pytest.fixture(name='isolated_registry')
def fixture_isolated_registry(monkeypatch):
    monkeypatch.setattr(registry, '_HANDLERS', {})
    monkeypatch.setattr(registry, '_HANDLER_NAMES_BY_IDENTITY', {})
    monkeypatch.setattr(registry, '_BUILTINS_REGISTERED', True)


def test_private_registration_requires_exact_closed_metadata(
        isolated_registry) -> None:
    del isolated_registry
    private_name = registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST[0]
    with pytest.raises(ValueError, match='requires the resource-action'):
        registry.register_handler(_handler, name=private_name)
    with pytest.raises(ValueError, match='closed to'):
        registry.register_handler(
            _handler,
            name='plugin:public',
            claim_scope=registry.HandlerClaimScope.RESOURCE_ACTION_AUTHORITY)
    with pytest.raises(ValueError, match='NORMAL, NEVER'):
        registry.register_handler(
            _handler,
            name=private_name,
            execution_class=registry.ExecutionClass.CONTROLLER,
            claim_scope=registry.HandlerClaimScope.RESOURCE_ACTION_AUTHORITY)

    registration = registry.register_handler(
        _handler,
        name=private_name,
        claim_scope=registry.HandlerClaimScope.RESOURCE_ACTION_AUTHORITY)
    assert registration.claim_scope is (
        registry.HandlerClaimScope.RESOURCE_ACTION_AUTHORITY)
    with pytest.raises(ValueError, match='requires the resource-action'):
        registry.register_handler(_handler, name=private_name)


def test_supported_handlers_exclude_private_from_ordinary_roles(
        isolated_registry) -> None:
    del isolated_registry
    registry.register_handler(_handler, name='plugin:public')
    for index, name in enumerate(
            registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST):

        def private_handler(index=index) -> None:
            del index

        registry.register_handler(
            private_handler,
            name=name,
            claim_scope=registry.HandlerClaimScope.RESOURCE_ACTION_AUTHORITY)

    assert request_postgres._supported_handlers('executor') == ['plugin:public']
    assert request_postgres._supported_handlers('all') == ['plugin:public']
    assert request_postgres._supported_handlers('authority-worker') == sorted(
        registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST)


def _compiled_predicates(backend: request_postgres.PostgresQueueBackend) -> str:
    statement = sqlalchemy.select(request_postgres.REQUESTS.c.request_id).where(
        *backend._role_predicates())
    compiled = statement.compile(dialect=postgresql.dialect())
    return f'{compiled}\n{compiled.params!r}'


def test_ordinary_queue_excludes_private_without_serve_schema(
        monkeypatch) -> None:
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'executor')
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       str(uuid.uuid4()))
    backend = request_postgres.PostgresQueueBackend('short',
                                                    execution_classes=frozenset(
                                                        {'normal'}))
    sql = _compiled_predicates(backend)
    assert 'handler_name NOT IN' in sql
    assert 'serve_resource_action_worker_cohorts' not in sql
    assert 'serve_resource_action_worker_cohort_refs' not in sql


def test_authority_queue_requires_full_action_and_shadow_cohort_joins(
        monkeypatch) -> None:
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'authority-worker')
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       str(uuid.uuid4()))
    backend = request_postgres.PostgresQueueBackend(
        'short',
        execution_classes=frozenset({'normal'}),
        authority_claim_config=_claim_config())
    sql = _compiled_predicates(backend)
    for fragment in (
            'serve_resource_action_worker_cohorts',
            'serve_resource_action_worker_cohort_refs',
            'serve_resource_action_shadow_coverage',
            'api_resource_actions',
            'api_resource_action_attempts',
            'ACTION_ACTIVE',
            'SHADOW_ACTIVE',
            'executor_cohort',
            authority_worker.SHADOW_ROUTING_PAYLOAD_KEY,
    ):
        assert fragment in sql


def test_authority_role_cannot_construct_an_unfiltered_queue(
        monkeypatch) -> None:
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'authority-worker')
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       str(uuid.uuid4()))
    with pytest.raises(ValueError, match='resolved claim configuration'):
        request_postgres.PostgresQueueBackend('short',
                                              execution_classes=frozenset(
                                                  {'normal'}))


def test_routing_config_requires_canonical_exact_bytes(tmp_path) -> None:
    value = {
        'version': 1,
        'cohort_id': 'authority-v1',
        'namespace': 'skypilot-system',
        'deployment_name': 'skypilot-authority-v1',
        'service_account_name': 'skypilot-authority-v1',
        'container_name': 'skypilot-authority-worker',
        'image': 'registry.example/authority@sha256:' + '1' * 64,
        'claim_contract': 'frozen_action_cohort_join_v1',
        'handler_allowlist': list(
            registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST),
    }
    path = tmp_path / 'routing.json'
    path.write_bytes(kernel_actions.canonical_json_bytes(value))
    environ = {authority_worker.ROUTING_CONFIG_PATH_ENV_VAR: str(path)}
    parsed = authority_worker.load_routing_config(environ)
    assert parsed.cohort_id == 'authority-v1'

    path.write_text(json.dumps(value) + '\n', encoding='utf-8')
    with pytest.raises(RuntimeError, match='not canonical'):
        authority_worker.load_routing_config(environ)


def test_authority_resolution_fails_closed_when_serve033_is_missing(
        tmp_path) -> None:
    value = {
        'version': 1,
        'cohort_id': 'authority-v1',
        'namespace': 'skypilot-system',
        'deployment_name': 'skypilot-authority-v1',
        'service_account_name': 'skypilot-authority-v1',
        'container_name': 'skypilot-authority-worker',
        'image': 'registry.example/authority@sha256:' + '1' * 64,
        'claim_contract': 'frozen_action_cohort_join_v1',
        'handler_allowlist': list(
            registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST),
    }
    path = tmp_path / 'routing.json'
    path.write_bytes(kernel_actions.canonical_json_bytes(value))
    environ = {
        authority_worker.ROUTING_CONFIG_PATH_ENV_VAR: str(path),
        authority_worker.ACTIVE_COHORT_ENV_VAR: 'authority-v1',
    }
    result = mock.Mock()
    result.scalar_one.return_value = None
    connection = mock.Mock()
    connection.execute.return_value = result

    with pytest.raises(RuntimeError, match='requires Serve033 table'):
        authority_worker.resolve_claim_config(connection, environ=environ)
