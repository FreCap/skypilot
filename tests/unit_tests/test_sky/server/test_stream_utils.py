"""Tests for server-side log streaming helpers."""

# pylint: disable=protected-access
import pathlib

import aiofiles
import pytest

from sky.server import stream_utils


@pytest.mark.asyncio
async def test_rewind_if_log_truncated(tmp_path: pathlib.Path):
    log_path = tmp_path / 'request.log'
    log_path.write_bytes(b'old output that the follower consumed\n')

    async with aiofiles.open(log_path, 'rb') as log_file:
        assert await log_file.read() != b''
        log_path.write_bytes(b'new output\n')

        assert await stream_utils._rewind_if_log_truncated(log_file, log_path)
        assert await log_file.read() == b'new output\n'
