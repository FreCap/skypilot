"""Tests for non-blocking debug dump path validation."""

import asyncio
from unittest import mock

import fastapi
import pytest

from sky.server import server


@pytest.mark.asyncio
async def test_debug_dump_path_resolution_runs_off_event_loop(tmp_path):
    dump_path = tmp_path / 'dump.zip'
    dump_path.write_bytes(b'dump')
    original_to_thread = asyncio.to_thread

    with mock.patch.object(server.debug_utils, 'DEBUG_DUMP_DIR', str(tmp_path)), \
         mock.patch.object(server.asyncio,
                           'to_thread', wraps=original_to_thread) as to_thread:
        response = await server.download_debug_dump(dump_path.name)

    assert isinstance(response, fastapi.responses.FileResponse)
    assert response.path == dump_path
    to_thread.assert_awaited_once_with(
        server._resolve_debug_dump_path,  # pylint: disable=protected-access
        dump_path.name)


@pytest.mark.parametrize(
    ('dump_filename', 'status_code'),
    [('../outside.zip', 403), ('missing.zip', 404)],
)
def test_debug_dump_path_preserves_validation_errors(tmp_path, dump_filename,
                                                     status_code):
    with mock.patch.object(server.debug_utils, 'DEBUG_DUMP_DIR', str(tmp_path)):
        with pytest.raises(fastapi.HTTPException) as exc_info:
            server._resolve_debug_dump_path(  # pylint: disable=protected-access
                dump_filename)

    assert exc_info.value.status_code == status_code
