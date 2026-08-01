"""Characterization tests for the controller history facade."""

import asyncio
import inspect
from unittest import mock

from sky.serve import controller

_HISTORY_METHODS = (
    '_persist_request_history',
    '_record_request_history',
    '_persist_response_time_history',
    '_record_response_time_history',
    '_persist_prediction_time_history',
    '_record_prediction_time_history',
    '_persist_autoscaler_history',
    '_record_autoscaler_history',
    '_get_accelerator_history_breakdown',
)


def _controller() -> controller.SkyServeController:
    instance = object.__new__(controller.SkyServeController)
    instance._service_name = 'svc'  # pylint: disable=protected-access
    instance._service_hash = 'service-hash'  # pylint: disable=protected-access
    instance._history_session_id = 'history-session'  # pylint: disable=protected-access
    instance._applied_version = 3  # pylint: disable=protected-access
    return instance


def test_history_methods_are_direct_patchable_bindings():
    instance = _controller()
    for method_name in _HISTORY_METHODS:
        descriptor = inspect.getattr_static(controller.SkyServeController,
                                            method_name)
        assert inspect.isfunction(descriptor)
        assert descriptor.__module__ == controller.__name__
        assert descriptor.__qualname__ == f'SkyServeController.{method_name}'
        assert getattr(instance, method_name).__self__ is instance

    request_data = {'request_history': {}}
    recorder = mock.Mock(return_value=True)
    instance._record_request_history = recorder  # pylint: disable=protected-access

    assert asyncio.run(
        instance._persist_request_history(  # pylint: disable=protected-access
            request_data))
    recorder.assert_called_once_with(request_data)


def test_history_writer_uses_controller_module_patch_surface_once():
    instance = _controller()
    request_data = {
        'lb_session_id': 'lb-a',
        'request_history_session_id': 'a' * 32,
        'request_history': {
            'bucket_seconds': 60,
            'buckets': [],
        },
    }

    with mock.patch.object(controller.serve_history,
                           'record_request_activity',
                           return_value=1) as writer:
        assert instance._record_request_history(  # pylint: disable=protected-access
            request_data)

    writer.assert_called_once_with(
        'svc',
        'service-hash',
        f"lb-a:{'a' * 32}",
        request_data['request_history'],
    )


def test_autoscaler_history_samples_once_and_forwards_snapshot():
    instance = _controller()
    autoscaler = mock.Mock()
    autoscaler.info.return_value = {
        'fill_target': 4,
        'in_flight_total': 2,
        'queue_depth': 1,
    }
    autoscaler.get_final_target_num_replicas.return_value = 3
    autoscaler.configured_accelerator_shapes = {}
    instance._autoscaler = autoscaler  # pylint: disable=protected-access
    replica_counts = {
        'replica_unit': 'physical_backend',
        'ready_replicas': 2,
        'total_replicas': 5,
    }
    capacity_hint = {'provisioning_replicas': 3}

    with mock.patch.object(controller.time, 'time', return_value=123.0) as now, \
         mock.patch.object(controller.serve_history,
                           'record_autoscaler_snapshot',
                           return_value=1) as writer:
        asyncio.run(
            instance._persist_autoscaler_history(  # pylint: disable=protected-access
                replica_counts, capacity_hint))

    now.assert_called_once_with()
    writer.assert_called_once_with(
        'svc',
        'service-hash',
        'history-session',
        version=3,
        replica_unit='physical_backend',
        demand_target=3,
        capacity_target=4,
        ready_capacity=2,
        provisioning_capacity=3,
        total_capacity=5,
        peak_in_flight=2,
        peak_queue_depth=1,
        accelerator_breakdown=None,
        timestamp=123.0,
    )
