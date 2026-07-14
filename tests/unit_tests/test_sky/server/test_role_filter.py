"""Unit tests for role-aware API request body filters."""

from unittest import mock

import fastapi
import pytest

from sky import models
from sky.server.requests import payloads
from sky.server.requests import role_filter
from sky.users import rbac
from sky.utils import common as common_lib


def _viewer_request():
    request = mock.Mock(spec=fastapi.Request)
    auth_user = mock.Mock()
    auth_user.id = 'viewer-bob'
    request.state.auth_user = auth_user
    return request


def _user_request():
    request = mock.Mock(spec=fastapi.Request)
    auth_user = mock.Mock()
    auth_user.id = 'user-alice'
    request.state.auth_user = auth_user
    return request


def _anonymous_request():
    request = mock.Mock(spec=fastapi.Request)
    request.state.auth_user = None
    return request


def _auth_user(user_id: str = 'user-alice') -> models.User:
    return models.User(id=user_id, name=user_id)


def _launch_body(task: str,
                 override_config: dict | None = None) -> payloads.LaunchBody:
    return payloads.LaunchBody(
        task=task,
        cluster_name='test-cluster',
        override_skypilot_config=override_config or {},
    )


@pytest.mark.parametrize(
    'task',
    [
        # User-authored task YAML.
        'config:\n  kubernetes:\n    pod_config:\n      spec: {}\n',
        # SDK-serialized task YAML.
        'resources:\n  _cluster_config_overrides:\n    kubernetes:\n'
        '      pod_config:\n        spec: {}\n',
        # Per-context SSH configuration uses the same arbitrary pod-spec
        # surface.
        'resources:\n  any_of:\n    - _cluster_config_overrides:\n'
        '        ssh:\n          context_configs:\n            cluster-a:\n'
        '              pod_config: {}\n',
    ],
)
@mock.patch.object(role_filter.permission, 'permission_service')
def test_reject_non_admin_task_pod_config(mock_svc, task):
    mock_svc.get_user_roles.return_value = [rbac.RoleName.USER.value]

    with pytest.raises(fastapi.HTTPException) as exc_info:
        role_filter.reject_non_admin_pod_config(_auth_user(),
                                                _launch_body(task))

    assert exc_info.value.status_code == 403


@mock.patch.object(role_filter.permission, 'permission_service')
def test_reject_non_admin_client_pod_config(mock_svc):
    mock_svc.get_user_roles.return_value = [rbac.RoleName.USER.value]
    body = _launch_body(
        'run: echo safe', {
            'workspaces': {
                'research': {
                    'kubernetes': {
                        'context_configs': {
                            'research': {
                                'pod_config': {
                                    'spec': {}
                                }
                            }
                        }
                    }
                }
            }
        })

    with pytest.raises(fastapi.HTTPException) as exc_info:
        role_filter.reject_non_admin_pod_config(_auth_user(), body)

    assert exc_info.value.status_code == 403


@mock.patch.object(role_filter.permission, 'permission_service')
def test_allow_admin_task_pod_config(mock_svc):
    mock_svc.get_user_roles.return_value = [rbac.RoleName.ADMIN.value]
    body = _launch_body(
        'config:\n  kubernetes:\n    pod_config:\n      spec: {}\n')

    role_filter.reject_non_admin_pod_config(_auth_user('admin'), body)


def test_allow_internal_task_pod_config():
    body = _launch_body(
        'config:\n  kubernetes:\n    pod_config:\n      spec: {}\n')

    role_filter.reject_non_admin_pod_config(None, body)


@mock.patch.object(role_filter.permission, 'permission_service')
def test_allow_non_admin_task_without_pod_config(mock_svc):
    body = _launch_body(
        'envs:\n  pod_config: harmless\nconfig:\n  kubernetes:\n'
        '    provision_timeout: 15\n')

    role_filter.reject_non_admin_pod_config(_auth_user(), body)

    mock_svc.get_user_roles.assert_not_called()


@mock.patch.object(role_filter.permission, 'permission_service')
def test_reject_client_pod_config_for_non_task_request(mock_svc):
    mock_svc.get_user_roles.return_value = [rbac.RoleName.USER.value]
    body = payloads.StatusBody(
        override_skypilot_config={'kubernetes': {
            'pod_config': {}
        }})

    with pytest.raises(fastapi.HTTPException) as exc_info:
        role_filter.reject_non_admin_pod_config(_auth_user(), body)

    assert exc_info.value.status_code == 403


@mock.patch.object(role_filter.permission, 'permission_service')
def test_force_viewer_status_body_for_viewer(mock_svc):
    enforcer = mock.Mock()
    enforcer.get_roles_for_user.return_value = [rbac.RoleName.VIEWER.value]
    mock_svc._ensure_enforcer.return_value = enforcer

    body = payloads.StatusBody(
        refresh=common_lib.StatusRefreshMode.FORCE,
        include_credentials=True,
    )
    out = role_filter.force_viewer_status_body(_viewer_request(), body)

    assert out.refresh == common_lib.StatusRefreshMode.NONE
    assert out.include_credentials is False


@mock.patch.object(role_filter.permission, 'permission_service')
def test_force_viewer_status_body_for_user_unchanged(mock_svc):
    enforcer = mock.Mock()
    enforcer.get_roles_for_user.return_value = [rbac.RoleName.USER.value]
    mock_svc._ensure_enforcer.return_value = enforcer

    body = payloads.StatusBody(
        refresh=common_lib.StatusRefreshMode.FORCE,
        include_credentials=True,
    )
    out = role_filter.force_viewer_status_body(_user_request(), body)

    # Regular user — body must be unchanged.
    assert out.refresh == common_lib.StatusRefreshMode.FORCE
    assert out.include_credentials is True


def test_force_viewer_status_body_anonymous_unchanged():
    body = payloads.StatusBody(
        refresh=common_lib.StatusRefreshMode.FORCE,
        include_credentials=True,
    )
    out = role_filter.force_viewer_status_body(_anonymous_request(), body)
    # Anonymous (no auth_user) is treated like non-viewer.
    assert out.refresh == common_lib.StatusRefreshMode.FORCE
    assert out.include_credentials is True


@mock.patch.object(role_filter.permission, 'permission_service')
def test_force_viewer_jobs_queue_body(mock_svc):
    enforcer = mock.Mock()
    enforcer.get_roles_for_user.return_value = [rbac.RoleName.VIEWER.value]
    mock_svc._ensure_enforcer.return_value = enforcer

    body = payloads.JobsQueueBody(refresh=True)
    out = role_filter.force_viewer_jobs_queue_body(_viewer_request(), body)
    assert out.refresh is False


@mock.patch.object(role_filter.permission, 'permission_service')
def test_force_viewer_jobs_queue_v2_body(mock_svc):
    enforcer = mock.Mock()
    enforcer.get_roles_for_user.return_value = [rbac.RoleName.VIEWER.value]
    mock_svc._ensure_enforcer.return_value = enforcer

    body = payloads.JobsQueueV2Body(refresh=True)
    out = role_filter.force_viewer_jobs_queue_v2_body(_viewer_request(), body)
    assert out.refresh is False


@mock.patch.object(role_filter.permission, 'permission_service')
def test_force_viewer_jobs_logs_body(mock_svc):
    enforcer = mock.Mock()
    enforcer.get_roles_for_user.return_value = [rbac.RoleName.VIEWER.value]
    mock_svc._ensure_enforcer.return_value = enforcer

    body = payloads.JobsLogsBody(refresh=True)
    out = role_filter.force_viewer_jobs_logs_body(_viewer_request(), body)
    assert out.refresh is False


@mock.patch.object(role_filter.permission, 'permission_service')
def test_force_viewer_jobs_download_logs_body(mock_svc):
    enforcer = mock.Mock()
    enforcer.get_roles_for_user.return_value = [rbac.RoleName.VIEWER.value]
    mock_svc._ensure_enforcer.return_value = enforcer

    body = payloads.JobsDownloadLogsBody(name='job', job_id=1, refresh=True)
    out = role_filter.force_viewer_jobs_download_logs_body(
        _viewer_request(), body)
    assert out.refresh is False


@mock.patch.object(role_filter.permission, 'permission_service')
def test_force_viewer_volume_refresh(mock_svc):
    enforcer = mock.Mock()
    enforcer.get_roles_for_user.return_value = [rbac.RoleName.VIEWER.value]
    mock_svc._ensure_enforcer.return_value = enforcer

    out = role_filter.force_viewer_volume_refresh(_viewer_request(),
                                                  refresh=True)
    assert out is False


@mock.patch.object(role_filter.permission, 'permission_service')
def test_force_viewer_volume_refresh_user_unchanged(mock_svc):
    enforcer = mock.Mock()
    enforcer.get_roles_for_user.return_value = [rbac.RoleName.USER.value]
    mock_svc._ensure_enforcer.return_value = enforcer

    out = role_filter.force_viewer_volume_refresh(_user_request(), refresh=True)
    # Non-viewer is unaffected.
    assert out is True
