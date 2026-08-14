"""Tests for the direct dashboard infrastructure summary."""

from unittest import mock

import fastapi
import pytest

from sky import core
from sky import models
from sky.server import infra_dashboard
from sky.utils import common_utils
from sky.utils import context


def _request(auth_user: models.User | None) -> mock.MagicMock:
    request = mock.MagicMock(spec=fastapi.Request)
    request.state.auth_user = auth_user
    return request


def test_infra_summary_is_direct_scoped_and_deterministic():
    user = models.User(id='alice', name='alice')
    with mock.patch.object(
            infra_dashboard.server_common,
            'refresh_workspace_state_for_sync_handler') as refresh, \
         mock.patch.object(infra_dashboard.workspaces_core,
                           'get_accessible_workspace_names',
                           return_value={'workspace-b', 'workspace-a'}), \
         mock.patch.object(
             infra_dashboard.core,
             'enabled_clouds_batch',
             return_value={
                 'workspace-a': ['aws', 'kubernetes/context-a'],
                 'workspace-b': ['ssh/pool-b'],
             }) as enabled_clouds_batch:
        response = infra_dashboard.get_infra_summary(_request(user))

    refresh.assert_called_once_with()
    enabled_clouds_batch.assert_called_once_with(
        ['workspace-a', 'workspace-b'], expand=True)
    assert response == {
        'version': 1,
        'workspaces': [{
            'name': 'workspace-a',
            'infrastructure': ['aws', 'kubernetes/context-a'],
        }, {
            'name': 'workspace-b',
            'infrastructure': ['ssh/pool-b'],
        }],
    }


def test_infra_summary_requires_authentication():
    with pytest.raises(fastapi.HTTPException) as exc_info:
        infra_dashboard.get_infra_summary(_request(None))
    assert exc_info.value.status_code == 401


def test_enabled_clouds_batch_propagates_user_to_workspace_threads():
    user = models.User(id='alice', name='alice')

    def enabled_clouds(*, workspace, expand):
        assert expand is True
        return [f'{common_utils.get_current_user().id}/{workspace}']

    with context.initialize(), \
         mock.patch.object(core.workspaces_core,
                           'workspaces_for_user',
                           return_value={'a': {}, 'b': {}}), \
         mock.patch.object(core, 'enabled_clouds', side_effect=enabled_clouds):
        common_utils.set_current_user(user)
        response = core.enabled_clouds_batch(['a', 'b'], expand=True)

    assert response == {'a': ['alice/a'], 'b': ['alice/b']}
