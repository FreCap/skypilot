"""Tests for non-blocking dashboard filesystem access."""

import asyncio
import os
from unittest import mock

import fastapi
import pytest

from sky.server import constants as server_constants
from sky.server import dashboard


@pytest.mark.asyncio
async def test_serve_dashboard_html_reads_off_event_loop(tmp_path):
    """HTML filesystem checks and reads run in worker threads."""
    dashboard_dir = tmp_path / 'dashboard'
    dashboard_dir.mkdir()
    html_path = dashboard_dir / 'clusters.html'
    html_path.write_text('<head></head><script>run()</script>',
                         encoding='utf-8')
    request = mock.MagicMock(spec=fastapi.Request)
    original_to_thread = asyncio.to_thread

    with mock.patch.object(server_constants, 'DASHBOARD_DIR',
                           str(dashboard_dir)), \
         mock.patch.object(dashboard.csp_utils,
                           'generate_nonce', return_value='test-nonce'), \
         mock.patch.object(dashboard.asyncio,
                           'to_thread', wraps=original_to_thread) as to_thread:
        response = await dashboard.serve_dashboard(request,
                                                   full_path='clusters')

    assert response.status_code == 200
    assert b'nonce="test-nonce"' in response.body
    assert request.state.csp_nonce == 'test-nonce'
    assert any(call.args == (os.path.isfile, str(html_path))
               for call in to_thread.await_args_list)
    assert any(
        getattr(call.args[0], '__name__', None) == '_serve_html_with_nonce' and
        call.args[1:] == (request, str(html_path))
        for call in to_thread.await_args_list)
