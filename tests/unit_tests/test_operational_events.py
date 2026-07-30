"""Unit tests for the actor-aware operational event plane."""
# pylint: disable=protected-access,redefined-outer-name

import json
from unittest import mock
import uuid

from click import testing as click_testing
import fastapi
from fastapi import testclient
import pydantic
import pytest

import sky
from sky import exceptions
from sky.client.cli import events as events_cli
from sky.events import api_models
from sky.events import client as events_client
from sky.server import constants as server_constants
from sky.server.events import cursors
from sky.server.events import models
from sky.server.events import server as events_server
from sky.server.events import store
from sky.server.requests import request_names


def _event(
    event_id: str = '00000000-0000-0000-0000-000000000001'
) -> api_models.OperationalEvent:
    return api_models.OperationalEvent(
        id=event_id,
        occurred_at='2026-07-30T12:00:00Z',
        kind=api_models.EventKind.CLUSTER_LAUNCH,
        phase=api_models.EventPhase.TERMINAL,
        outcome=api_models.EventOutcome.SUCCEEDED,
        cause=api_models.EventCause.HANDLER_SUCCEEDED,
        message='Cluster launch succeeded.',
        workspace='research',
        actor=api_models.EventActor(id='alice-id',
                                    name='alice@example.com',
                                    type=api_models.EventActorType.SSO),
        request_id='request-1',
        execution_generation=1,
        targets=[
            api_models.EventTarget(type=api_models.EventTargetType.CLUSTER,
                                   id='cluster-hash',
                                   name='trainer')
        ],
    )


def _unwrapped(function):
    while hasattr(function, '__wrapped__'):
        function = function.__wrapped__
    return function


def test_event_context_is_closed_bounded_and_safely_rendered():
    context = models.initial_context(
        request_names.RequestName.CLUSTER_LAUNCH,
        actor_name='alice@example.com',
        actor_type='sso',
        cluster_name='trainer',
    )
    assert context == {
        'version': 1,
        'kind': 'cluster.launch',
        'actor_name': 'alice@example.com',
        'actor_type': 'sso',
        'workspace': None,
        'targets': [{
            'type': 'cluster',
            'id': None,
            'name': 'trainer',
        }],
    }
    parsed = models.EventContext.model_validate(context)
    assert parsed.with_workspace('research').workspace == 'research'
    assert parsed.with_primary_target_id('hash').targets[0].id == 'hash'
    with pytest.raises(pydantic.ValidationError):
        parsed.with_workspace('x' * (models.MAX_EVENT_STRING_LENGTH + 1))
    with pytest.raises(pydantic.ValidationError):
        models.EventContext.model_validate({
            **context,
            'actor_type': 'owner',
        })
    with pytest.raises(pydantic.ValidationError):
        models.EventTargetContext(type='volume', name='target')

    secret = 'provider-secret-error'
    message = models.safe_message(
        api_models.EventKind.CLUSTER_LAUNCH,
        api_models.EventOutcome.FAILED,
        api_models.EventCause.HANDLER_FAILED,
    )
    assert message == 'Cluster launch failed.'
    assert secret not in message


def test_signed_cursor_binds_principal_permissions_filters_and_direction():
    authority_key = cursors.derive_key(str(uuid.uuid4()))
    bindings = cursors.CursorBindings(
        principal_id='alice',
        is_admin=False,
        workspaces=('research',),
        filters={'kind': ['cluster.launch']},
    )
    cursor_state = cursors.CursorState(position=41, high_watermark=50)
    cursor = cursors.issue(authority_key, bindings,
                           api_models.TraversalDirection.OLDER, cursor_state)
    assert cursors.verify(cursor, authority_key, bindings,
                          api_models.TraversalDirection.OLDER) == cursor_state

    payload, signature = cursor.split('.')
    tampered = f'{"A" if payload[0] != "A" else "B"}{payload[1:]}.{signature}'
    with pytest.raises(cursors.StaleCursorError):
        cursors.verify(tampered, authority_key, bindings,
                       api_models.TraversalDirection.OLDER)
    with pytest.raises(cursors.StaleCursorError):
        cursors.verify(
            cursor, authority_key,
            cursors.CursorBindings(principal_id='bob',
                                   is_admin=False,
                                   workspaces=('research',),
                                   filters=bindings.filters),
            api_models.TraversalDirection.OLDER)
    with pytest.raises(cursors.StaleCursorError):
        cursors.verify(cursor, authority_key, bindings,
                       api_models.TraversalDirection.NEWER)


@pytest.fixture
def event_api(monkeypatch):
    app = fastapi.FastAPI()
    app.include_router(events_server.router, prefix='/events')

    @app.middleware('http')
    async def initialize_auth_user(request, call_next):
        request.state.auth_user = None
        return await call_next(request)

    scope = store.AuthorizationScope(principal_id='local',
                                     is_admin=True,
                                     effective_workspaces=None)
    monkeypatch.setattr(events_server, '_authorization_scope',
                        lambda request, workspaces: scope)
    return testclient.TestClient(app)


def test_event_api_has_closed_unavailable_and_stale_cursor_errors(
        event_api, monkeypatch):
    monkeypatch.setattr(
        events_server.store, 'list_events',
        mock.Mock(side_effect=store.OperationalEventsUnavailableError(
            'PostgreSQL is required.')))
    response = event_api.get('/events')
    assert response.status_code == 503
    assert response.json()['detail'] == {
        'code': api_models.OPERATIONAL_EVENTS_UNAVAILABLE,
        'message': 'PostgreSQL is required.',
    }

    monkeypatch.setattr(
        events_server.store, 'list_events',
        mock.Mock(side_effect=cursors.StaleCursorError('Invalid cursor.')))
    response = event_api.get('/events?cursor=bad')
    assert response.status_code == 409
    assert response.json()['detail']['code'] == (
        api_models.STALE_OPERATIONAL_EVENT_CURSOR)


def test_event_api_rejects_unbounded_and_ambiguous_filters(event_api):
    response = event_api.get('/events?target_id=hash')
    assert response.status_code == 422
    response = event_api.get('/events?' +
                             '&'.join('workspace=x' for _ in range(17)))
    assert response.status_code == 422
    response = event_api.get(
        '/events?since=2026-07-31T00:00:00Z&until=2026-07-30T00:00:00Z')
    assert response.status_code == 422


def test_event_authorization_scope_intersects_current_workspace_access(
        monkeypatch):
    request = mock.Mock()
    request.state.auth_user = mock.Mock(id='alice')
    monkeypatch.setattr(events_server.server_common,
                        'refresh_workspace_state_for_sync_handler', mock.Mock())
    monkeypatch.setattr(
        events_server.permission.permission_service,
        'get_user_roles',
        mock.Mock(return_value=['viewer']),
    )
    monkeypatch.setattr(
        events_server.workspaces_core,
        'get_accessible_workspace_names_for_user',
        mock.Mock(return_value={'research', 'staging'}),
    )

    scope = events_server._authorization_scope(request, ('private', 'research'))
    assert scope == store.AuthorizationScope(
        principal_id='alice',
        is_admin=False,
        effective_workspaces=('research',),
    )
    (events_server.workspaces_core.get_accessible_workspace_names_for_user.
     assert_called_once_with('alice', roles=['viewer']))

    events_server.permission.permission_service.get_user_roles.return_value = [
        'admin'
    ]
    scope = events_server._authorization_scope(request, ())
    assert scope == store.AuthorizationScope(principal_id='alice',
                                             is_admin=True,
                                             effective_workspaces=None)


def test_direct_client_encodes_repeated_filters_and_closed_errors(monkeypatch):
    response = mock.Mock()
    response.status_code = 200
    response.json.return_value = api_models.EventsPage(
        items=[_event()],
        poll_cursor='poll',
        has_more=False,
    ).model_dump(mode='json')
    request = mock.Mock(return_value=response)
    monkeypatch.setattr(events_client.server_common,
                        'make_authenticated_request', request)
    page = _unwrapped(events_client.list_events)(
        cluster='trainer',
        workspaces=['research', 'staging'],
        kinds=[api_models.EventKind.CLUSTER_LAUNCH],
        actor_types=[api_models.EventActorType.SSO],
        direction=api_models.TraversalDirection.OLDER,
    )
    assert page.items[0].request_id == 'request-1'
    params = request.call_args.kwargs['params']
    assert params.count(('workspace', 'research')) == 1
    assert params.count(('workspace', 'staging')) == 1
    assert ('target_type', 'cluster') in params
    assert ('target_name', 'trainer') in params
    assert ('kind', 'cluster.launch') in params

    response.status_code = 503
    response.json.return_value = {
        'detail': {
            'code': api_models.OPERATIONAL_EVENTS_UNAVAILABLE,
            'message': 'PostgreSQL is required.',
        }
    }
    with pytest.raises(exceptions.OperationalEventsUnavailableError,
                       match='PostgreSQL is required'):
        _unwrapped(events_client.list_events)()


def test_public_facade_and_old_server_version_gate(monkeypatch):
    assert sky.events.list is events_client.list_events
    health = mock.Mock()
    monkeypatch.setattr(events_client.server_common,
                        'check_server_healthy_or_start_fn', health)
    monkeypatch.setattr(
        events_client.versions,
        'get_remote_api_version',
        lambda: server_constants.MIN_OPERATIONAL_EVENTS_API_VERSION - 1,
    )
    with pytest.raises(exceptions.APINotSupportedError) as exc_info:
        events_client.list_events()
    assert 'Function list_events' in str(exc_info.value)
    assert 'Upgrade the remote server' in str(exc_info.value)
    health.assert_called_once()


def test_events_cli_json_and_lossless_watch(monkeypatch):
    runner = click_testing.CliRunner()
    normal_page = api_models.EventsPage(items=[_event()],
                                        poll_cursor='poll',
                                        has_more=False)
    list_events = mock.Mock(return_value=normal_page)
    monkeypatch.setattr(events_cli.sdk, 'list_events', list_events)
    result = runner.invoke(events_cli.events, ['--format', 'json'])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload['items'][0]['request_id'] == 'request-1'

    first_new = _event('00000000-0000-0000-0000-000000000002')
    first_new.request_id = 'request-2'
    second_new = _event('00000000-0000-0000-0000-000000000003')
    second_new.request_id = 'request-3'
    list_events.reset_mock()
    list_events.side_effect = [
        api_models.EventsPage(items=[], poll_cursor='poll-0', has_more=False),
        api_models.EventsPage(items=[first_new],
                              next_cursor='next-1',
                              poll_cursor='poll-1',
                              has_more=True),
        api_models.EventsPage(items=[second_new],
                              poll_cursor='poll-2',
                              has_more=False),
    ]
    sleeps = iter([None, KeyboardInterrupt()])

    def sleep(_):
        outcome = next(sleeps)
        if isinstance(outcome, BaseException):
            raise outcome

    monkeypatch.setattr(events_cli.time, 'sleep', sleep)
    result = runner.invoke(events_cli.events, ['--watch', '--format', 'json'])
    assert result.exit_code == 0, result.output
    lines = [json.loads(line) for line in result.output.splitlines() if line]
    assert [line['request_id'] for line in lines] == ['request-2', 'request-3']
    assert list_events.call_args_list[1].kwargs['cursor'] == 'poll-0'
    assert list_events.call_args_list[2].kwargs['cursor'] == 'next-1'
