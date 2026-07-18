"""Regression tests for user-initiated cluster stop events."""
from unittest import mock

import pytest

from sky import backends
from sky import core
from sky import exceptions
from sky import global_user_state
from sky.utils import status_lib


def _patch_stop(monkeypatch, backend):
    handle = mock.create_autospec(backends.CloudVmRayResourceHandle,
                                  instance=True)
    handle.launched_resources = mock.MagicMock()
    get_handle = mock.MagicMock(return_value=handle)
    get_backend = mock.MagicMock(return_value=backend)
    add_event = mock.MagicMock()
    monkeypatch.setattr(global_user_state, 'get_handle_from_cluster_name',
                        get_handle)
    monkeypatch.setattr(core.backend_utils, 'get_backend_from_handle',
                        get_backend)
    monkeypatch.setattr(global_user_state, 'add_cluster_event', add_event)
    monkeypatch.setattr(core.usage_lib,
                        'record_cluster_name_for_current_operation',
                        mock.MagicMock())
    monkeypatch.setattr(core, '_maybe_run_stop_hooks', mock.MagicMock())
    return handle, get_handle, get_backend, add_event


def test_stop_records_event_after_successful_teardown(monkeypatch):
    backend = mock.MagicMock()
    handle, get_handle, get_backend, add_event = _patch_stop(
        monkeypatch, backend)
    order = []
    backend.teardown.side_effect = lambda *args, **kwargs: order.append(
        'teardown')
    add_event.side_effect = lambda *args, **kwargs: order.append('event')

    core.stop('cluster')

    assert order == ['teardown', 'event']
    get_handle.assert_called_once_with('cluster')
    get_backend.assert_called_once_with(handle)
    backend.teardown.assert_called_once_with(handle,
                                             terminate=False,
                                             purge=False)
    add_event.assert_called_once_with(
        'cluster', status_lib.ClusterStatus.STOPPED,
        'Cluster was stopped by user.',
        global_user_state.ClusterEventType.STATUS_CHANGE)


def test_stop_failure_does_not_record_success_event(monkeypatch):
    backend = mock.MagicMock()
    _, _, _, add_event = _patch_stop(monkeypatch, backend)
    backend.teardown.side_effect = RuntimeError('stop failed')

    with pytest.raises(RuntimeError, match='stop failed'):
        core.stop('cluster')

    add_event.assert_not_called()


def test_unsupported_stop_does_not_record_event_or_teardown(monkeypatch):
    backend = mock.create_autospec(backends.CloudVmRayBackend, instance=True)
    handle, _, _, add_event = _patch_stop(monkeypatch, backend)
    handle.launched_resources.cloud.check_features_are_supported.side_effect = (
        exceptions.NotSupportedError('stop unsupported'))

    with pytest.raises(exceptions.NotSupportedError, match='Stopping cluster'):
        core.stop('cluster')

    add_event.assert_not_called()
    backend.teardown.assert_not_called()
