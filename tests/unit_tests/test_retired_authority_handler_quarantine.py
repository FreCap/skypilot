"""Compatibility quarantine for retired authority-only action handlers."""
# pylint: disable=protected-access

import uuid

import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql

from sky.server.requests import postgres as request_postgres
from sky.server.requests import registry


@pytest.fixture(name='isolated_registry')
def fixture_isolated_registry(monkeypatch):
    monkeypatch.setattr(registry, '_HANDLERS', {})
    monkeypatch.setattr(registry, '_HANDLER_NAMES_BY_IDENTITY', {})
    monkeypatch.setattr(registry, '_BUILTINS_REGISTERED', True)


def _public_handler() -> None:
    pass


def test_private_inventory_remains_registered_and_fail_closed(
        isolated_registry) -> None:
    del isolated_registry
    registry._register_resource_action_authority_handlers()

    registrations = tuple(
        registration for registration in registry.registered_handlers()
        if registration.claim_scope is
        registry.HandlerClaimScope.RESOURCE_ACTION_AUTHORITY)
    assert tuple(registration.name for registration in registrations) == (
        registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST)
    assert len(registrations) == 4
    for registration in registrations:
        assert registration.execution_class is registry.ExecutionClass.NORMAL
        assert registration.replay_policy is registry.ReplayPolicy.NEVER
        assert registration.cancellation_policy is (
            registry.CancellationPolicy.FENCED_PROCESS)
        assert registration.aliases == ()
        with pytest.raises(RuntimeError, match='has been retired'):
            registration.func(untrusted='payload')


def test_ordinary_roles_never_advertise_private_handlers(
        isolated_registry) -> None:
    del isolated_registry
    registry.register_handler(_public_handler, name='plugin:public')
    registry._register_resource_action_authority_handlers()

    private_names = set(registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST)
    for role in ('api', 'executor', 'controller', 'all'):
        assert private_names.isdisjoint(
            request_postgres._supported_handlers(role))
    assert request_postgres._supported_handlers('executor') == ['plugin:public']
    assert request_postgres._supported_handlers('all') == ['plugin:public']


@pytest.mark.parametrize('execution_classes', [frozenset({'normal'}), None])
def test_ordinary_queue_predicates_exclude_every_private_handler(
        monkeypatch, execution_classes) -> None:
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'executor')
    monkeypatch.setenv(request_postgres.SERVER_INSTANCE_ID_ENV_VAR,
                       str(uuid.uuid4()))
    backend = request_postgres.PostgresQueueBackend(
        'short', execution_classes=execution_classes)
    statement = sqlalchemy.select(request_postgres.REQUESTS.c.request_id).where(
        *backend._role_predicates())
    sql = str(
        statement.compile(dialect=postgresql.dialect(),
                          compile_kwargs={'literal_binds': True}))

    assert 'handler_name NOT IN' in sql
    for handler_name in registry.RESOURCE_ACTION_AUTHORITY_HANDLER_ALLOWLIST:
        assert handler_name in sql
    assert 'serve_resource_action_worker_cohorts' not in sql
    assert 'serve_resource_action_worker_cohort_refs' not in sql
