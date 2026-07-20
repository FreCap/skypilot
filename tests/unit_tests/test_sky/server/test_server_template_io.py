"""Tests for non-blocking API server template reads."""

import pathlib
from unittest import mock

import pytest

from sky.server import server


@pytest.mark.asyncio
async def test_html_template_read_runs_off_event_loop():
    with mock.patch.object(server.asyncio,
                           'to_thread',
                           new_callable=mock.AsyncMock,
                           return_value='template content') as to_thread:
        assert await server._read_html_template(  # pylint: disable=protected-access
            'token_page.html') == 'template content'

    to_thread.assert_awaited_once()
    read_text = to_thread.await_args.args[0]
    expected_path = pathlib.Path(server.__file__).parent / 'html' / \
        'token_page.html'
    assert read_text.__self__ == expected_path
    assert read_text.__func__ is pathlib.Path.read_text
    assert to_thread.await_args.kwargs == {'encoding': 'utf-8'}


@pytest.mark.asyncio
async def test_token_preserves_missing_template_response():
    request = mock.Mock()
    with mock.patch.object(
            server,
            '_generate_auth_token',
            return_value='token'), mock.patch.object(
                server, '_get_auth_user_header', return_value=None), \
            mock.patch.object(
                server,
                '_read_html_template',
                new_callable=mock.AsyncMock,
                side_effect=FileNotFoundError('missing')):
        with pytest.raises(server.fastapi.HTTPException) as exc_info:
            await server.token(request)

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == 'Token page template not found.'
