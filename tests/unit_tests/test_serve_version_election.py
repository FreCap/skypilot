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
                           'get_version_yaml_contents',
                           return_value={1: 'yaml-1', 2: 'yaml-2', 3: 'yaml-3'}), \
         mock.patch.object(server.debug_dump_helpers,
                           'redact_task_yaml',
                           side_effect=lambda yaml: f'redacted-{yaml}'):
        history = server._service_version_history('svc')

    assert history['elected_version'] == 3
    assert history['active_versions'] == [2, 3]
    assert history['versions'] == [{
        'version': 3,
        'yaml_content': 'redacted-yaml-3',
        'elected': True,
        'active': True,
    }, {
        'version': 2,
        'yaml_content': 'redacted-yaml-2',
        'elected': False,
        'active': True,
    }, {
        'version': 1,
        'yaml_content': 'redacted-yaml-1',
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
                           'get_yaml_contents',
                           return_value={
                               1: 'service: old',
                               3: 'service: current',
                           }), \
         mock.patch.object(impl.task_lib.Task,
                           'from_yaml_str',
                           return_value=task), \
         mock.patch.object(impl, '_update_impl') as update:
        impl.elect_version('svc', 1, 'service-hash')

    update.assert_called_once_with(task,
                                   'svc',
                                   serve_utils.UpdateMode.ROLLING,
                                   pool=False,
                                   lifecycle_lock=lifecycle_lock)


def test_elect_version_rejects_configuration_that_is_already_elected():
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
                           'get_yaml_contents',
                           return_value={
                               1: 'service: same',
                               3: 'service: same',
                           }), \
         mock.patch.object(impl, '_update_impl') as update, \
         pytest.raises(ValueError, match='already has the configuration'):
        impl.elect_version('svc', 1, 'service-hash')

    update.assert_not_called()


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
        impl.elect_version('svc', 1, 'original')
