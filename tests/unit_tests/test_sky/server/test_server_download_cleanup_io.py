"""Tests for non-blocking download temporary-directory cleanup."""

import asyncio
import os
import time
from unittest import mock

import pytest

from sky.server import server


def test_download_tmp_cleanup_removes_only_stale_directories(tmp_path):
    user_dir = tmp_path / 'user'
    user_dir.mkdir()
    stale_dir = user_dir / 'stale'
    stale_dir.mkdir()
    fresh_dir = user_dir / 'fresh'
    fresh_dir.mkdir()
    non_directory = user_dir / 'keep.txt'
    non_directory.write_text('keep', encoding='utf-8')
    stale_time = time.time() - server.bs.GC_GRACE_SECONDS - 1
    os.utime(stale_dir, (stale_time, stale_time))
    backend = mock.Mock()
    backend.download_tmp_base_dir.return_value = str(tmp_path)

    with mock.patch.object(server.bs, 'get_blob_storage', return_value=backend):
        server._cleanup_download_tmp_once()  # pylint: disable=protected-access

    assert not stale_dir.exists()
    assert fresh_dir.is_dir()
    assert non_directory.is_file()


@pytest.mark.asyncio
async def test_download_tmp_cleanup_runs_off_event_loop():
    with mock.patch.object(
            server.asyncio,
            'sleep',
            new_callable=mock.AsyncMock,
            side_effect=[None, asyncio.CancelledError]), \
         mock.patch.object(server.asyncio,
                           'to_thread',
                           new_callable=mock.AsyncMock) as to_thread:
        with pytest.raises(asyncio.CancelledError):
            await server.cleanup_download_tmp()

    to_thread.assert_awaited_once_with(server._cleanup_download_tmp_once)  # pylint: disable=protected-access
