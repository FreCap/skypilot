"""Tests for non-blocking API stream path validation."""

import asyncio
from unittest import mock

import fastapi
import pytest

from sky.server import server
from sky.skylet import constants


@pytest.mark.asyncio
async def test_stream_resolves_user_log_path_off_event_loop(tmp_path):
    log_path = tmp_path / 'request.log'
    log_path.write_text('output', encoding='utf-8')
    request = mock.MagicMock(spec=fastapi.Request)
    request.state.auth_user = None
    original_to_thread = asyncio.to_thread

    with mock.patch.object(constants, 'SKY_LOGS_DIRECTORY', str(tmp_path)), \
         mock.patch.object(server.asyncio,
                           'to_thread', wraps=original_to_thread) as to_thread:
        response = await server.stream(request,
                                       log_path=log_path.name,
                                       follow=False,
                                       format='plain')

    assert isinstance(response, fastapi.responses.StreamingResponse)
    to_thread.assert_awaited_once_with(
        server._resolve_stream_log_path,  # pylint: disable=protected-access
        log_path.name)


@pytest.mark.parametrize(
    ('log_path', 'status_code'),
    [('../outside.log', 400), ('missing.log', 404)],
)
def test_stream_log_path_preserves_validation_errors(tmp_path, log_path,
                                                     status_code):
    with mock.patch.object(constants, 'SKY_LOGS_DIRECTORY', str(tmp_path)):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            server._resolve_stream_log_path(  # pylint: disable=protected-access
                log_path)

    assert exc_info.value.status_code == status_code


def test_stream_log_path_preserves_api_server_missing_error(tmp_path):
    api_server_log = tmp_path / 'api-server.log'
    with mock.patch.object(constants, 'API_SERVER_LOGS', str(api_server_log)):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            server._resolve_stream_log_path(  # pylint: disable=protected-access
                str(api_server_log))

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail.startswith('Server log file does not exist.')
