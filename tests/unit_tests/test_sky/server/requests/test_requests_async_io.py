"""Tests for non-blocking request-cleanup filesystem access."""

from unittest import mock

import pytest

from sky.server.requests import requests


@pytest.mark.asyncio
async def test_legacy_directory_cleanup_runs_off_event_loop():
    with mock.patch.object(requests.asyncio,
                           'to_thread',
                           new_callable=mock.AsyncMock) as to_thread:
        await requests._cleanup_legacy_directory_if_empty()  # pylint: disable=protected-access

    to_thread.assert_awaited_once_with(
        requests._cleanup_legacy_directory_if_empty_sync)  # pylint: disable=protected-access


def test_legacy_directory_cleanup_ignores_path_probe_errors():
    with mock.patch.object(requests.pathlib.Path,
                           'exists',
                           side_effect=OSError('filesystem unavailable')), \
         mock.patch.object(requests.logger, 'debug') as debug:
        requests._cleanup_legacy_directory_if_empty_sync()  # pylint: disable=protected-access

    debug.assert_called_once()
    assert 'filesystem unavailable' in debug.call_args.args[0]
