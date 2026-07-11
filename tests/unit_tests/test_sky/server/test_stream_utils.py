"""Tests for server-side log streaming helpers."""

# pylint: disable=protected-access
import asyncio
import fcntl
import pathlib
import types

import aiofiles
import pytest

from sky.server import stream_utils
from sky.utils import context


@pytest.mark.asyncio
async def test_rewind_if_log_truncated(tmp_path: pathlib.Path):
    log_path = tmp_path / 'request.log'
    log_path.write_bytes(b'old output that the follower consumed\n')

    async with aiofiles.open(log_path, 'rb') as log_file:
        assert await log_file.read() != b''
        log_path.write_bytes(b'new output\n')

        assert await stream_utils._rewind_if_log_truncated(log_file, log_path)
        assert await log_file.read() == b'new output\n'


@pytest.mark.asyncio
async def test_rewind_if_log_truncated_and_regrown_past_offset(
        tmp_path: pathlib.Path):
    log_path = tmp_path / 'request.log'
    log_path.write_bytes(b'O' * 100)

    async with aiofiles.open(log_path, 'rb') as log_file:
        assert len(await log_file.read(90)) == 90
        previous_marker = stream_utils._request_log_marker(log_file)
        assert previous_marker is None

        bounded_log = context._TruncatingLogFile(log_path, max_bytes=128)
        bounded_log.write('N' * 128)
        bounded_log.flush()
        bounded_log.close()
        assert log_path.stat().st_size >= 90

        marker, rewound, lost_prefix = (
            await stream_utils._rewind_if_log_generation_changed(
                log_file, previous_marker))
        assert marker is not None
        assert rewound
        assert lost_prefix
        content = await log_file.read()
        assert b'Earlier request output was truncated' in content
        assert content.endswith(b'N')


@pytest.mark.asyncio
async def test_rollover_retains_window_without_replaying_consumed_bytes(
        tmp_path: pathlib.Path):
    log_path = tmp_path / 'request.log'
    old_output = b'O' * 450
    log_path.write_bytes(old_output)

    async with aiofiles.open(log_path, 'rb') as log_file:
        assert await log_file.read() == old_output
        previous_marker = stream_utils._request_log_marker(log_file)

        bounded_log = context._TruncatingLogFile(log_path, max_bytes=512)
        bounded_log.write('N' * 100)
        bounded_log.close()

        marker, rewound, lost_prefix = (
            await stream_utils._rewind_if_log_generation_changed(
                log_file, previous_marker))
        assert marker is not None
        assert rewound
        assert not lost_prefix
        assert await log_file.read() == b'N' * 100

        bounded_log = context._TruncatingLogFile(log_path, max_bytes=512)
        bounded_log.write('P' * 250)
        bounded_log.close()
        assert await log_file.read() == b'P' * 250

        bounded_log = context._TruncatingLogFile(log_path, max_bytes=512)
        bounded_log.write('Q' * 10)
        bounded_log.close()
        marker, rewound, lost_prefix = (
            await
            stream_utils._rewind_if_log_generation_changed(log_file, marker))
        assert marker is not None
        assert rewound
        assert not lost_prefix
        assert await log_file.read() == b'Q' * 10

    retained = log_path.read_bytes()
    assert len(retained) <= 512
    assert retained.endswith(b'P' * 129 + b'Q' * 10)


@pytest.mark.asyncio
async def test_cancelled_follower_does_not_leak_waiting_file_lock(
        tmp_path: pathlib.Path):
    log_path = tmp_path / 'request.log'
    log_path.write_bytes(b'output')
    blocker = log_path.open('a')
    fcntl.flock(blocker.fileno(), fcntl.LOCK_EX)

    async with aiofiles.open(log_path, 'rb') as log_file:
        read_task = asyncio.create_task(
            stream_utils._read_request_log_chunk(log_file, None, log_path))
        await asyncio.sleep(0.05)
        read_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read_task

        fcntl.flock(blocker.fileno(), fcntl.LOCK_UN)
        await asyncio.sleep(0.05)
        fcntl.flock(log_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(log_file.fileno(), fcntl.LOCK_UN)
    blocker.close()


@pytest.mark.asyncio
async def test_tail_snapshot_maps_following_rollover_without_replay(
        tmp_path: pathlib.Path):
    log_path = tmp_path / 'request.log'
    old_output = b'O' * 449 + b'\n'
    log_path.write_bytes(old_output)

    async with aiofiles.open(log_path, 'rb') as log_file:
        chunks = stream_utils._tail_log_file(log_file,
                                             tail=1,
                                             follow=False,
                                             log_path=log_path)
        assert await chunks.__anext__() == old_output.decode()

        bounded_log = context._TruncatingLogFile(log_path, max_bytes=512)
        bounded_log.write('N' * 100)
        bounded_log.close()

        remaining = [chunk async for chunk in chunks]
        assert ''.join(remaining) == 'N' * 100


@pytest.mark.asyncio
async def test_terminal_status_drains_final_write(tmp_path: pathlib.Path,
                                                  monkeypatch):
    log_path = tmp_path / 'request.log'
    log_path.write_text('old\n', encoding='utf-8')
    status_calls = 0

    async def terminal_after_final_write(_request_id):
        nonlocal status_calls
        status_calls += 1
        if status_calls == 1:
            bounded_log = context._TruncatingLogFile(log_path, max_bytes=512)
            bounded_log.write('final\n')
            bounded_log.close()
        return types.SimpleNamespace(
            status=stream_utils.requests_lib.RequestStatus.SUCCEEDED)

    monkeypatch.setattr(stream_utils.requests_lib, 'get_request_status_async',
                        terminal_after_final_write)
    async with aiofiles.open(log_path, 'rb') as log_file:
        chunks = [
            chunk async for chunk in stream_utils._tail_log_file(
                log_file,
                request_id='request-id',
                follow=True,
                polling_interval=0,
                log_path=log_path)
        ]

    assert ''.join(chunks) == 'old\nfinal\n'
