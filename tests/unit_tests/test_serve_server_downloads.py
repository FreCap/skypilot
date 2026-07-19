"""Tests for SkyServe download staging paths."""

import pathlib
import types
from unittest import mock

import pytest

from sky.serve.server import server
from sky.skylet import constants


@pytest.mark.asyncio
async def test_download_staging_uses_authenticated_user(tmp_path):
    """A body-supplied identity cannot select another user's staging root."""
    trusted_root = tmp_path / 'trusted'
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(auth_user=types.SimpleNamespace(
            id='trusted-user'),
                                    request_id='request-id'))
    body = mock.MagicMock()
    body.env_vars = {constants.USER_ID_ENV_VAR: 'spoofed-user'}
    body.service_name = 'service'
    blob_storage = mock.Mock()
    blob_storage.download_tmp_dir.return_value = str(trusted_root)

    with mock.patch.object(server.server_common.bs,
                           'get_blob_storage',
                           return_value=blob_storage), mock.patch.object(
                               server.executor,
                               'schedule_request_async',
                               new_callable=mock.AsyncMock) as schedule:
        await server.download_logs(request, body)

    blob_storage.download_tmp_dir.assert_called_once_with('trusted-user')
    local_dir = pathlib.Path(body.local_dir)
    assert trusted_root in local_dir.parents
    schedule.assert_awaited_once()
