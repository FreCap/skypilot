"""Authorization tests for persisted API request access."""

import pathlib
import types
from unittest import mock

import fastapi
import pytest

from sky import models
from sky.server import constants as server_constants
from sky.server import server
from sky.server.requests import access as request_access
from sky.server.requests import payloads
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.skylet import constants
from sky.users import rbac


def _make_request(
    auth_user: models.User | None,
    *,
    controller_origin: tuple[str, int] | None = None,
    headers: dict[str, str] | None = None,
) -> fastapi.Request:
    raw_headers = []
    if headers is not None:
        raw_headers = [(key.lower().encode(), value.encode())
                       for key, value in headers.items()]
    request = fastapi.Request({
        'type': 'http',
        'http_version': '1.1',
        'method': 'GET',
        'scheme': 'http',
        'path': '/',
        'raw_path': b'/',
        'query_string': b'',
        'headers': raw_headers,
        'client': ('127.0.0.1', 12345),
        'server': ('testserver', 80),
        'state': {},
    })
    request.state.auth_user = auth_user
    request.state.request_id = 'access-control-request'
    request.state.controller_origin = controller_origin
    return request


def _set_roles(monkeypatch: pytest.MonkeyPatch, roles: list[str]) -> None:
    monkeypatch.setattr(server.permission.permission_service, 'get_user_roles',
                        lambda _user_id: roles)


async def _empty_stream():
    for chunk in iter(()):
        yield chunk


@pytest.mark.parametrize(
    ('auth_user', 'roles', 'owner_user_id', 'can_cancel'),
    [
        (None, [], None, True),
        (models.User('admin'), [rbac.RoleName.ADMIN.value], None, True),
        (models.User('user'), [rbac.RoleName.USER.value], 'user', True),
        (models.User('viewer'), [rbac.RoleName.VIEWER.value], 'viewer', False),
        (models.User('admin-viewer'),
         [rbac.RoleName.ADMIN.value, rbac.RoleName.VIEWER.value], None, True),
    ],
    ids=['local', 'admin', 'user', 'viewer', 'admin-and-viewer'],
)
def test_resolve_request_access(auth_user, roles, owner_user_id, can_cancel):
    scope = request_access.resolve_request_access(auth_user, roles)

    assert scope.owner_user_id == owner_user_id
    assert scope.can_cancel is can_cancel
    assert scope.can_access_all_users is (owner_user_id is None)
    assert scope.can_stream_arbitrary_log_path is (owner_user_id is None)


@pytest.mark.asyncio
async def test_controller_generation_state_does_not_elevate_user(monkeypatch):
    user = models.User('ordinary-user')
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    request = _make_request(
        user,
        controller_origin=(instance_id, 22),
        headers={
            server_constants.CONTROLLER_INSTANCE_ID_HEADER: instance_id,
            server_constants.CONTROLLER_GENERATION_HEADER: '22',
        })
    _set_roles(monkeypatch, [rbac.RoleName.USER.value])

    scope = await server._request_access_scope(  # pylint: disable=protected-access
        request)

    assert request.headers[
        server_constants.CONTROLLER_GENERATION_HEADER] == '22'
    assert request.state.controller_origin == (instance_id, 22)
    assert scope.owner_user_id == user.id
    assert not scope.can_access_all_users
    assert not scope.can_stream_arbitrary_log_path


@pytest.mark.asyncio
async def test_role_lookup_failure_stops_before_request_storage(monkeypatch):
    request = _make_request(models.User('ordinary-user'))

    def fail_role_lookup(_user_id):
        raise RuntimeError('role storage unavailable')

    monkeypatch.setattr(server.permission.permission_service, 'get_user_roles',
                        fail_role_lookup)
    get_with_prefix = mock.AsyncMock()
    monkeypatch.setattr(requests_lib, 'get_requests_async_with_prefix',
                        get_with_prefix)

    with pytest.raises(RuntimeError, match='role storage unavailable'):
        await server.api_get(request, 'request-prefix')

    get_with_prefix.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('requested_id', ['owned-request-id', 'owned-req'])
async def test_api_get_scopes_exact_and_prefix_before_polling(
        monkeypatch, requested_id):
    user = models.User('owner-user')
    request = _make_request(user)
    _set_roles(monkeypatch, [rbac.RoleName.USER.value])
    full_request_id = 'owned-request-id'
    calls = []

    async def get_with_prefix(request_id_prefix, fields=None, user_id=None):
        calls.append(('scope', request_id_prefix, fields, user_id))
        return [types.SimpleNamespace(request_id=full_request_id)]

    async def get_status(request_id):
        calls.append(('status', request_id))
        return requests_lib.StatusWithMsg(
            status=requests_lib.RequestStatus.SUCCEEDED)

    encoded_request = object()
    request_record = mock.Mock(should_retry=False)
    request_record.get_error.return_value = None
    request_record.encode.return_value = encoded_request

    async def get_request(request_id):
        calls.append(('full', request_id))
        return request_record

    monkeypatch.setattr(requests_lib, 'get_requests_async_with_prefix',
                        get_with_prefix)
    monkeypatch.setattr(requests_lib, 'get_request_status_async', get_status)
    monkeypatch.setattr(requests_lib, 'get_request_async', get_request)

    result = await server.api_get(request, requested_id)

    assert result is encoded_request
    assert calls == [
        ('scope', requested_id, ['request_id'], user.id),
        ('status', full_request_id),
        ('full', full_request_id),
    ]


@pytest.mark.asyncio
async def test_api_get_should_retry_marks_the_exact_expanded_request(
        monkeypatch):
    user = models.User('owner-user')
    request = _make_request(user)
    _set_roles(monkeypatch, [rbac.RoleName.USER.value])
    full_request_id = 'owned-request-id'

    async def get_with_prefix(_request_id_prefix, fields=None, user_id=None):
        del fields, user_id
        return [types.SimpleNamespace(request_id=full_request_id)]

    async def get_status(_request_id):
        return requests_lib.StatusWithMsg(
            status=requests_lib.RequestStatus.CANCELLED)

    request_record = mock.Mock(
        should_retry=True,
        execution_generation=1,
        execution_quiescence_required=False,
        execution_quiesced_generation=None,
        execution_quiesced_at=None,
    )
    monkeypatch.setattr(requests_lib, 'get_requests_async_with_prefix',
                        get_with_prefix)
    monkeypatch.setattr(requests_lib, 'get_request_status_async', get_status)
    monkeypatch.setattr(requests_lib, 'get_request_async',
                        mock.AsyncMock(return_value=request_record))

    with pytest.raises(fastapi.HTTPException) as exc:
        await server.api_get(request, 'owned-req')

    assert exc.value.status_code == 503
    assert exc.value.headers == {
        server_constants.REQUEST_RESULT_RETRY_REQUIRED_HEADER: full_request_id,
    }
    assert exc.value.detail == (
        f'Request {full_request_id!r} should be retried')


@pytest.mark.asyncio
async def test_api_get_withholds_retry_marker_until_exact_quiescence_receipt(
        monkeypatch):
    user = models.User('owner-user')
    request = _make_request(user)
    _set_roles(monkeypatch, [rbac.RoleName.USER.value])
    full_request_id = 'owned-request-id'

    async def get_with_prefix(_request_id_prefix, fields=None, user_id=None):
        del fields, user_id
        return [types.SimpleNamespace(request_id=full_request_id)]

    async def get_status(_request_id):
        return requests_lib.StatusWithMsg(
            status=requests_lib.RequestStatus.CANCELLED)

    pending_quiescence = types.SimpleNamespace(
        should_retry=True,
        execution_generation=7,
        execution_quiescence_required=True,
        execution_quiesced_generation=None,
        execution_quiesced_at=None,
    )
    proven_quiescence = types.SimpleNamespace(
        should_retry=True,
        execution_generation=7,
        execution_quiescence_required=True,
        execution_quiesced_generation=7,
        execution_quiesced_at=123.0,
    )
    get_request = mock.AsyncMock(
        side_effect=[pending_quiescence, proven_quiescence])
    sleep = mock.AsyncMock()
    monkeypatch.setattr(requests_lib, 'get_requests_async_with_prefix',
                        get_with_prefix)
    monkeypatch.setattr(requests_lib, 'get_request_status_async', get_status)
    monkeypatch.setattr(requests_lib, 'get_request_async', get_request)
    monkeypatch.setattr(server.asyncio, 'sleep', sleep)

    with pytest.raises(fastapi.HTTPException) as exc:
        await server.api_get(request, 'owned-req')

    assert exc.value.status_code == 503
    assert exc.value.headers == {
        server_constants.REQUEST_RESULT_RETRY_REQUIRED_HEADER: full_request_id,
    }
    assert get_request.await_count == 2
    sleep.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('requested_id', ['foreign-request-id', 'foreign-req'])
async def test_api_get_hides_foreign_exact_and_prefix_before_payload_reads(
        monkeypatch, requested_id):
    user = models.User('owner-user')
    request = _make_request(user)
    _set_roles(monkeypatch, [rbac.RoleName.USER.value])
    status = mock.AsyncMock()
    get_request = mock.AsyncMock()

    async def get_with_prefix(request_id_prefix, fields=None, user_id=None):
        assert request_id_prefix == requested_id
        assert fields == ['request_id']
        assert user_id == user.id
        return None

    monkeypatch.setattr(requests_lib, 'get_requests_async_with_prefix',
                        get_with_prefix)
    monkeypatch.setattr(requests_lib, 'get_request_status_async', status)
    monkeypatch.setattr(requests_lib, 'get_request_async', get_request)

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await server.api_get(request, requested_id)

    assert exc_info.value.status_code == 404
    status.assert_not_awaited()
    get_request.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('requested_id', ['owned-req', None])
async def test_stream_scopes_explicit_and_latest_request(
        monkeypatch, requested_id):
    user = models.User('owner-user')
    request = _make_request(user)
    _set_roles(monkeypatch, [rbac.RoleName.USER.value])
    full_request_id = 'owned-request-id'
    expand = mock.AsyncMock(return_value=full_request_id)
    latest = mock.AsyncMock(return_value=full_request_id)
    request_task = types.SimpleNamespace(
        request_id=full_request_id,
        log_path=pathlib.Path('/tmp/owned-request.log'),
        schedule_type=requests_lib.ScheduleType.SHORT)
    get_request = mock.AsyncMock(return_value=request_task)
    provider = mock.Mock()
    provider.log_stream.return_value = _empty_stream()
    monkeypatch.setattr(server, 'get_expanded_request_id', expand)
    monkeypatch.setattr(requests_lib, 'get_latest_request_id_async', latest)
    monkeypatch.setattr(requests_lib, 'get_request_async', get_request)
    monkeypatch.setattr(server.log_provider, 'get_log_provider',
                        lambda: provider)

    response = await server.stream(request,
                                   request_id=requested_id,
                                   follow=False,
                                   format='plain')

    assert isinstance(response, fastapi.responses.StreamingResponse)
    if requested_id is None:
        latest.assert_awaited_once_with(user.id)
        expand.assert_not_awaited()
    else:
        expand.assert_awaited_once_with(requested_id, user.id)
        latest.assert_not_awaited()
    get_request.assert_awaited_once_with(full_request_id,
                                         fields=['request_id', 'schedule_type'])
    provider.log_stream.assert_called_once_with(
        request_id=full_request_id,
        log_path=pathlib.Path('/tmp/owned-request.log'),
        plain_logs=True,
        tail=None,
        follow=False,
        polling_interval=server.stream_utils.DEFAULT_POLL_INTERVAL)


@pytest.mark.asyncio
async def test_stream_raw_log_path_denies_user_even_with_controller_state(
        monkeypatch):
    user = models.User('owner-user')
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    request = _make_request(
        user,
        controller_origin=(instance_id, 22),
        headers={
            server_constants.CONTROLLER_INSTANCE_ID_HEADER: instance_id,
            server_constants.CONTROLLER_GENERATION_HEADER: '22',
        })
    _set_roles(monkeypatch, [rbac.RoleName.USER.value])
    resolve_log_path = mock.Mock()
    monkeypatch.setattr(server, '_resolve_stream_log_path', resolve_log_path)

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await server.stream(request,
                            log_path='server-owned.log',
                            follow=False,
                            format='plain')

    assert exc_info.value.status_code == 403
    resolve_log_path.assert_not_called()


@pytest.mark.asyncio
async def test_stream_raw_log_path_allows_admin(monkeypatch, tmp_path):
    admin = models.User('admin-user')
    request = _make_request(admin)
    _set_roles(monkeypatch, [rbac.RoleName.ADMIN.value])
    log_path = tmp_path / 'server-owned.log'
    log_path.write_text('output', encoding='utf-8')
    log_streamer = mock.Mock(return_value=_empty_stream())
    monkeypatch.setattr(constants, 'SKY_LOGS_DIRECTORY', str(tmp_path))
    monkeypatch.setattr(server.stream_utils, 'log_streamer', log_streamer)

    response = await server.stream(request,
                                   log_path=log_path.name,
                                   follow=False,
                                   format='plain')

    assert isinstance(response, fastapi.responses.StreamingResponse)
    log_streamer.assert_called_once_with(
        request_id=None,
        log_path=log_path.resolve(),
        plain_logs=True,
        tail=None,
        follow=False,
        polling_interval=server.stream_utils.DEFAULT_POLL_INTERVAL)


@pytest.mark.asyncio
async def test_cancel_canonicalizes_forged_user_id(monkeypatch):
    user = models.User('owner-user')
    instance_id = '96d9d1f6-8ba4-402b-85f5-27db321fd504'
    request = _make_request(user, controller_origin=(instance_id, 22))
    _set_roles(monkeypatch, [rbac.RoleName.USER.value])
    schedule = mock.AsyncMock()
    monkeypatch.setattr(server.executor, 'schedule_request_async', schedule)
    body = payloads.RequestCancelBody(request_ids=['owned-prefix'],
                                      user_id='victim-user',
                                      env_vars={})

    await server.api_cancel(request, body)

    schedule.assert_awaited_once()
    scheduled = schedule.await_args.kwargs
    assert scheduled['request_id'] == request.state.request_id
    assert scheduled['request_name'] == request_names.RequestName.API_CANCEL
    assert scheduled['request_body'].request_ids == ['owned-prefix']
    assert scheduled['request_body'].user_id == user.id
    assert scheduled['func'] is requests_lib.kill_requests_with_prefix
    assert scheduled['auth_user'] is user
    assert body.user_id == 'victim-user'


@pytest.mark.asyncio
async def test_cancel_denies_viewer_before_scheduling(monkeypatch):
    viewer = models.User('viewer-user')
    request = _make_request(viewer)
    _set_roles(monkeypatch, [rbac.RoleName.VIEWER.value])
    schedule = mock.AsyncMock()
    monkeypatch.setattr(server.executor, 'schedule_request_async', schedule)
    body = payloads.RequestCancelBody(request_ids=['request-prefix'],
                                      user_id=viewer.id,
                                      env_vars={})

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await server.api_cancel(request, body)

    assert exc_info.value.status_code == 403
    schedule.assert_not_awaited()
