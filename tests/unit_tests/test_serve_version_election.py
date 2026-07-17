"""Tests for retained SkyServe versions and admin election."""
# pylint: disable=protected-access
import contextlib
import types
from unittest import mock

import pytest

from sky.serve import serve_utils
from sky.serve.server import impl
from sky.serve.server import server


def test_version_admin_api_rejects_non_admin():
    request = types.SimpleNamespace(state=types.SimpleNamespace(
        auth_user=types.SimpleNamespace(id='user')))
    with mock.patch.object(server.permission.permission_service,
                           'get_user_roles',
                           return_value=['user']), \
         pytest.raises(server.fastapi.HTTPException) as error:
        server._require_admin(request)

    assert error.value.status_code == 403


def test_version_admin_api_allows_auth_disabled_local_server():
    request = types.SimpleNamespace(state=types.SimpleNamespace(auth_user=None))
    server._require_admin(request)


def test_version_history_marks_elected_and_active_versions():
    record = {
        'pool': False,
        'elected_version': 3,
        'active_versions': [2, 3],
    }
    with mock.patch.object(server.serve_state,
                           'get_service_from_name',
                           return_value=record), \
         mock.patch.object(server.serve_state,
                           'get_version_records',
                           return_value=[{
                               'version': version,
                               'spec': mock.Mock(
                                   autoscaling_policy_str=mock.Mock(
                                       return_value=f'policy-{version}')),
                               'yaml_content': f'yaml-{version}',
                               'created_at': 1000.0 + version,
                               'created_by': f'user-{version}',
                           } for version in (1, 2, 3)]), \
         mock.patch.object(server.debug_dump_helpers,
                           'redact_task_yaml',
                           side_effect=lambda yaml: f'redacted-{yaml}'):
        history = server._service_version_history('svc')

    assert history['elected_version'] == 3
    assert history['active_versions'] == [2, 3]
    assert history['versions'] == [{
        'version': 3,
        'yaml_content': 'redacted-yaml-3',
        'created_at': 1003.0,
        'created_by': 'user-3',
        'policy': 'policy-3',
        'elected': True,
        'active': True,
    }, {
        'version': 2,
        'yaml_content': 'redacted-yaml-2',
        'created_at': 1002.0,
        'created_by': 'user-2',
        'policy': 'policy-2',
        'elected': False,
        'active': True,
    }, {
        'version': 1,
        'yaml_content': 'redacted-yaml-1',
        'created_at': 1001.0,
        'created_by': 'user-1',
        'policy': 'policy-1',
        'elected': False,
        'active': False,
    }]


def test_elect_version_reuses_safe_update_path():
    task = mock.Mock()
    lifecycle_lock = contextlib.nullcontext()
    record = {
        'hash': 'service-hash',
        'pool': False,
        'elected_version': 3,
    }
    with mock.patch.object(impl.filelock,
                           'FileLock',
                           return_value=contextlib.nullcontext()), \
         mock.patch.object(impl.serve_utils,
                           'get_service_lifecycle_lock',
                           return_value=lifecycle_lock), \
         mock.patch.object(impl.serve_state,
                           'get_service_from_name',
                           return_value=record), \
         mock.patch.object(impl.serve_state,
                           'get_yaml_content',
                           return_value='service: old'), \
         mock.patch.object(impl.task_lib.Task,
                           'from_yaml_str',
                           return_value=task), \
         mock.patch.object(impl, '_update_impl') as update:
        impl.elect_version('svc', 1, 'service-hash', 3)

    update.assert_called_once_with(task,
                                   'svc',
                                   serve_utils.UpdateMode.ROLLING,
                                   pool=False,
                                   lifecycle_lock=lifecycle_lock,
                                   reuse_task_storage_scope=True)


def test_elect_version_rejects_stale_service_incarnation():
    with mock.patch.object(impl.filelock,
                           'FileLock',
                           return_value=contextlib.nullcontext()), \
         mock.patch.object(impl.serve_utils,
                           'get_service_lifecycle_lock',
                           return_value=contextlib.nullcontext()), \
         mock.patch.object(impl.serve_state,
                           'get_service_from_name',
                           return_value={
                               'hash': 'replacement',
                               'pool': False,
                           }), \
         pytest.raises(RuntimeError, match='changed before'):
        impl.elect_version('svc', 1, 'original', None)


def test_elect_version_rejects_stale_elected_version():
    with mock.patch.object(impl.filelock,
                           'FileLock',
                           return_value=contextlib.nullcontext()), \
         mock.patch.object(impl.serve_utils,
                           'get_service_lifecycle_lock',
                           return_value=contextlib.nullcontext()), \
         mock.patch.object(impl.serve_state,
                           'get_service_from_name',
                           return_value={
                               'hash': 'service-hash',
                               'pool': False,
                               'elected_version': 4,
                           }), \
         mock.patch.object(impl, '_update_impl') as update, \
         pytest.raises(RuntimeError, match='changed before'):
        impl.elect_version('svc', 1, 'service-hash', 3)

    update.assert_not_called()
