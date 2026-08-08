"""Real-PostgreSQL/TLS proof for the isolated V2 preflight pool."""
# pylint: disable=protected-access,redefined-outer-name,unused-import

import concurrent.futures
import json
import pathlib
import threading
import time

import pytest
import sqlalchemy
import test_serve_resource_action_preflight_transport as v1_transport
import test_serve_resource_action_preflight_v2 as v2_fixtures
import test_serve_resource_action_preflight_v2_transport as v2_transport
from test_serve_resource_action_state_pg import postgres_engine

from sky.serve import constants
from sky.serve import resource_action_preflight_v2 as preflight_v2
from sky.serve import resource_actions
from sky.server import authority_preflight
from sky.utils.db import db_utils


def test_isolated_pool_saturation_fails_closed_without_second_connection(
        postgres_engine: sqlalchemy.engine.Engine, tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch) -> None:
    """The real size-one pool composes with TLS deadline and then recovers."""

    monkeypatch.setenv('IS_SKYPILOT_SERVER', 'true')
    monkeypatch.setenv(
        'SKYPILOT_DB_CONNECTION_URI',
        postgres_engine.url.render_as_string(hide_password=False))
    monkeypatch.setattr(db_utils, '_postgres_engine_cache', {})
    monkeypatch.setattr(db_utils, '_max_connections', 1)
    engine = db_utils.get_engine(
        'serve/services',
        engine_namespace=db_utils.AUTHORITY_PREFLIGHT_ENGINE_NAMESPACE)
    assert isinstance(engine, sqlalchemy.engine.Engine)

    opened_connections: list[None] = []

    def record_connect(_dbapi_connection, _connection_record) -> None:
        opened_connections.append(None)

    sqlalchemy.event.listen(engine, 'connect', record_connect)
    attempts: list[None] = []
    evaluation_entered = threading.Event()
    allow_pool_checkout = threading.Event()

    def evaluate_v2(request):
        attempts.append(None)
        evaluation_entered.set()
        assert allow_pool_checkout.wait(timeout=2)
        with engine.connect() as connection:
            assert connection.execute(
                sqlalchemy.text('SELECT 1')).scalar_one() == 1
        return (preflight_v2.ProviderLaunchAuthorityPreflightResponseV2.
                unavailable(request))

    tls_directory, ca_file = v1_transport._tls_tree(tmp_path)
    token_file = tmp_path / 'tokens'
    token_file.write_text(v2_transport._TOKEN + '\n', encoding='ascii')
    monkeypatch.setenv(
        constants.RESOURCE_ACTION_PREFLIGHT_AUTH_TOKENS_FILE_ENV_VAR,
        str(token_file))
    for env_name in (constants.LB_SYNC_AUTH_TOKENS_FILE_ENV_VAR,
                     constants.CONTROLLER_ADMIN_AUTH_TOKENS_FILE_ENV_VAR,
                     constants.LB_AUTH_TOKENS_FILE_ENV_VAR,
                     constants.CONTROLLER_AUTH_TOKEN_ENV_VAR,
                     constants.LB_AUTH_TOKEN_ENV_VAR):
        monkeypatch.delenv(env_name, raising=False)
    server = authority_preflight.AuthorityPreflightServer(
        '127.0.0.1',
        0,
        v2_transport._SERVICE_DNS,
        resource_actions.ProviderLaunchAuthorityPreflightResponseV1.unavailable,
        on_transport_invalid=lambda: None,
        evaluator_v2=evaluate_v2,
        tls_directory=str(tls_directory))
    request = v2_fixtures._launch_request()
    server.start()
    try:
        with engine.connect() as held_connection:
            settings = held_connection.execute(
                sqlalchemy.text(
                    "SELECT current_setting('application_name'), "
                    "current_setting('statement_timeout'), "
                    "current_setting('lock_timeout'), "
                    "current_setting('idle_in_transaction_session_timeout')")
            ).one()
            assert tuple(settings) == ('skypilot-authority-preflight', '3500ms',
                                       '750ms', '4s')
            assert len(opened_connections) == 1
            assert engine.pool.checkedout() == 1
            wire = v2_transport._wire(
                request.canonical_bytes,
                constants.RESOURCE_ACTION_PREFLIGHT_PATH_V2, server.bound_port)
            started = time.monotonic()
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                first = pool.submit(v1_transport._exchange, server.bound_port,
                                    ca_file, wire)
                assert evaluation_entered.wait(2)
                others = [
                    pool.submit(v1_transport._exchange, server.bound_port,
                                ca_file, wire) for _ in range(7)
                ]
                other_responses = [item.result(timeout=2) for item in others]
                allow_pool_checkout.set()
                responses = [first.result(timeout=2), *other_responses]
            elapsed = time.monotonic() - started
            expected = resource_actions.canonical_json_bytes({
                'version': 2,
                'code': 'cohort_unavailable',
            })
            assert [
                v1_transport._assert_response(response, 503)
                for response in responses
            ] == [expected] * 8
            assert elapsed < authority_preflight._REQUEST_DEADLINE_SECONDS
            assert attempts == [None]
            assert len(opened_connections) == 1
            assert engine.pool.checkedout() == 1

        response = v1_transport._exchange(server.bound_port, ca_file, wire)
        body = v1_transport._assert_response(response, 200)
        parsed = (
            preflight_v2.provider_authority_preflight_response_from_value_v2(
                json.loads(body)))
        parsed.validate_request(request)
        assert attempts == [None, None]
        assert len(opened_connections) == 1
        assert engine.pool.checkedout() == 0
    finally:
        allow_pool_checkout.set()
        server.stop()
        sqlalchemy.event.remove(engine, 'connect', record_connect)
        engine.dispose()
