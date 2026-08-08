"""Final removal proofs for the retired authority compatibility surface."""

# pylint: disable=protected-access

import dataclasses
import importlib.util
import inspect
import json
import pathlib

import pytest
import sqlalchemy
from sqlalchemy.dialects import postgresql
import yaml

from sky.server import plugins
from sky.server.requests import postgres as request_postgres
from sky.server.requests import registry
from sky.server.requests.serializers import decoders
from sky.server.requests.serializers import encoders

_PRIVATE_HANDLER_NAMES = (
    'serve_shadow_candidate_launch',
    'serve_shadow_candidate_down',
    'serve_resource_action_launch',
    'serve_resource_action_down',
)
_ROOT = pathlib.Path(__file__).parents[2]


@pytest.mark.parametrize('handler_name', _PRIVATE_HANDLER_NAMES)
def test_private_authority_handler_is_not_registered(handler_name: str) -> None:
    with pytest.raises(ValueError, match='Unknown durable request handler'):
        registry.resolve_handler(handler_name)


def test_private_authority_modules_and_claim_scope_are_removed() -> None:
    assert importlib.util.find_spec(
        'sky.serve.resource_action_handlers') is None
    assert importlib.util.find_spec(
        'sky.serve.resource_action_progress') is None
    assert 'claim_scope' not in {
        field.name for field in dataclasses.fields(registry.HandlerRegistration)
    }
    assert 'claim_scope' not in inspect.signature(
        plugins.ExtensionContext.register_request_handler).parameters


@pytest.mark.parametrize('handler_name', _PRIVATE_HANDLER_NAMES)
def test_private_request_name_uses_default_codec(handler_name: str) -> None:
    request_name = f'sky.{handler_name}'
    assert encoders.get_encoder(request_name) is encoders.default_encoder
    assert decoders.get_decoder(request_name) is decoders.default_decode_handler


@pytest.mark.parametrize('execution_classes', [frozenset({'normal'}), None])
def test_ordinary_queue_has_no_retired_handler_exclusion(
        monkeypatch, execution_classes) -> None:
    monkeypatch.setenv(request_postgres.SERVER_ROLE_ENV_VAR, 'executor')
    backend = request_postgres.PostgresQueueBackend(
        'short', execution_classes=execution_classes)
    statement = sqlalchemy.select(request_postgres.REQUESTS.c.request_id).where(
        *backend._role_predicates())
    sql = str(
        statement.compile(dialect=postgresql.dialect(),
                          compile_kwargs={'literal_binds': True}))

    for handler_name in _PRIVATE_HANDLER_NAMES:
        assert handler_name not in sql
    assert 'handler_name NOT IN' not in sql


def test_retired_helm_values_are_absent_from_defaults_and_schema() -> None:
    values = yaml.safe_load((_ROOT / 'charts/skypilot/values.yaml').read_text())
    schema = json.loads(
        (_ROOT / 'charts/skypilot/values.schema.json').read_text())

    assert 'resourceActions' not in values
    assert 'resourceActions' not in schema['properties']
