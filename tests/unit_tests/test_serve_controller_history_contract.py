"""Characterization tests for the controller history facade."""

import asyncio
import inspect
import pickle
import time
import types
from unittest import mock

import pytest

from sky.serve import controller

_HISTORY_METHODS = (
    '_persist_request_history',
    '_record_request_history',
    '_persist_request_classification_history',
    '_record_request_classification_history',
    '_persist_response_time_history',
    '_record_response_time_history',
    '_persist_prediction_time_history',
    '_record_prediction_time_history',
    '_persist_autoscaler_history',
    '_record_autoscaler_history',
    '_get_accelerator_history_breakdown',
)

_EXTRACTED_CONTROLLER_GLOBAL_METHODS = (
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


def test_extracted_history_methods_keep_controller_dependency_patch_surface():
    instance = _controller()
    for method_name in _EXTRACTED_CONTROLLER_GLOBAL_METHODS:
        descriptor = inspect.getattr_static(controller.SkyServeController,
                                            method_name)
        assert descriptor.__globals__ is vars(controller)
        assert getattr(controller.controller_history, method_name) is descriptor
        assert pickle.loads(pickle.dumps(descriptor)) is descriptor

    request_data = {
        'lb_session_id': 'lb-a',
        'request_history_session_id': 'a' * 32,
        'request_history': {
            'bucket_seconds': 60,
            'buckets': [],
        },
    }
    original_writer = mock.Mock(return_value=1)
    replacement_writer = mock.Mock(return_value=1)
    replacement_history = types.SimpleNamespace(
        record_request_activity=replacement_writer)
    with mock.patch.object(controller.controller_history.serve_history,
                           'record_request_activity', original_writer), \
         mock.patch.object(controller, 'serve_history', replacement_history):
        assert instance._record_request_history(  # pylint: disable=protected-access
            request_data)

    replacement_writer.assert_called_once()
    original_writer.assert_not_called()

    original_time = mock.Mock(return_value=111.0)
    replacement_time = mock.Mock(return_value=222.0)
    instance._record_autoscaler_history = mock.Mock(return_value=1)  # pylint: disable=protected-access
    with mock.patch.object(controller.controller_history.time, 'time',
                           original_time), \
         mock.patch.object(controller, 'time',
                           types.SimpleNamespace(time=replacement_time)):
        asyncio.run(
            instance._persist_autoscaler_history(  # pylint: disable=protected-access
                {
                    'replica_unit': 'physical_backend',
                    'ready_replicas': 0,
                    'total_replicas': 0,
                }, {'provisioning_replicas': 0}))

    replacement_time.assert_called_once_with()
    original_time.assert_not_called()
    assert instance._record_autoscaler_history.call_args.args[-1] == 222.0  # pylint: disable=protected-access


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


def test_malformed_v1_does_not_promote_generic_request_history():
    instance = _controller()
    request_data = {
        'lb_session_id': 'lb-a',
        'request_history_session_id': 'a' * 32,
        'request_history': {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': 120,
                'request_count': 1,
                'rejected_count': 0,
            }],
        },
        'request_classification_history': {
            'classification_version': 1,
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': 120,
                'classified_request_count': 1,
                'counted_rejected_count': 2,
            }],
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
    # The independent validator drops malformed current-v1 history, but the
    # generic arrival write above remains nullable instead of advertising
    # support from the version field alone.
    assert asyncio.run(
        instance._persist_request_classification_history(  # pylint: disable=protected-access
            request_data))


def test_classification_writer_uses_independent_snapshot():
    instance = _controller()
    request_data = {
        'lb_session_id': 'lb-a',
        'request_history_session_id': 'a' * 32,
        'request_classification_history': {
            'classification_version': 1,
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': 120,
                'classified_request_count': 2,
                'counted_rejected_count': 1,
            }],
        },
        'request_history': {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': 120,
                'request_count': 2,
                'rejected_count': 0,
            }],
        },
    }

    with mock.patch.object(controller.serve_history,
                           'record_request_classification',
                           return_value=1) as writer:
        assert instance._record_request_classification_history(  # pylint: disable=protected-access
            request_data)

    writer.assert_called_once_with(
        'svc',
        'service-hash',
        f"lb-a:{'a' * 32}",
        request_data['request_classification_history'],
        request_history=request_data['request_history'],
    )


def test_valid_classification_persists_with_malformed_arrival_snapshot():
    instance = _controller()
    bucket_start = int(time.time() // 60) * 60
    request_data = {
        'lb_session_id': 'lb-a',
        'request_history_session_id': 'a' * 32,
        'request_classification_history': {
            'classification_version': 1,
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': bucket_start,
                'classified_request_count': 2,
                'counted_rejected_count': 1,
            }],
        },
        'request_history': {
            'bucket_seconds': 60,
            'buckets': 'not-a-list',
        },
    }
    engine = mock.MagicMock()
    connection = engine.begin.return_value.__enter__.return_value

    with mock.patch.object(controller.serve_history,
                           '_postgres_engine',
                           return_value=engine):
        accepted = asyncio.run(
            instance._persist_request_classification_history(  # pylint: disable=protected-access
                request_data))

    assert accepted is True
    engine.begin.assert_called_once_with()
    connection.execute.assert_called_once_with(mock.ANY)


def test_future_classification_version_is_not_acknowledged():
    instance = _controller()
    request_data = {
        'lb_session_id': 'lb-a',
        'request_history_session_id': 'a' * 32,
        'request_classification_history': {
            'classification_version': 2,
            'bucket_seconds': 60,
            'buckets': [],
        },
    }

    with mock.patch.object(controller.serve_history,
                           'record_request_classification') as writer:
        accepted = asyncio.run(
            instance._persist_request_classification_history(  # pylint: disable=protected-access
                request_data))

    assert accepted is False
    writer.assert_not_called()


def test_current_classification_commits_before_arrival_history():
    instance = _controller()
    request_data = {
        'request_classification_history': {
            'classification_version': 1,
        },
    }

    async def exercise():
        events = []
        classification_started = asyncio.Event()
        classification_release = asyncio.Event()

        async def persist_classification(_request_data):
            events.append('classification_started')
            classification_started.set()
            await classification_release.wait()
            events.append('classification_committed')
            return True

        async def persist_request(_request_data):
            events.append('request_written')
            return True

        with (mock.patch.object(instance,
                                '_persist_request_classification_history',
                                side_effect=persist_classification) as
              classification_writer,
              mock.patch.object(instance,
                                '_persist_request_history',
                                side_effect=persist_request) as request_writer):
            task = asyncio.create_task(
                instance._persist_request_histories(  # pylint: disable=protected-access
                    request_data))
            await classification_started.wait()
            assert events == ['classification_started']
            request_writer.assert_not_awaited()
            classification_release.set()
            result = await task

        assert result == (True, True)
        classification_writer.assert_awaited_once_with(request_data)
        request_writer.assert_awaited_once_with(request_data)
        assert events == [
            'classification_started',
            'classification_committed',
            'request_written',
        ]

    asyncio.run(exercise())


def test_current_classification_failure_retains_both_histories():
    instance = _controller()
    request_data = {
        'request_classification_history': {
            'classification_version': 1,
        },
    }
    with (mock.patch.object(instance,
                            '_persist_request_classification_history',
                            new=mock.AsyncMock(return_value=False)) as
          classification_writer,
          mock.patch.object(instance,
                            '_persist_request_history',
                            new=mock.AsyncMock(return_value=True)) as
          request_writer):
        result = asyncio.run(
            instance._persist_request_histories(  # pylint: disable=protected-access
                request_data))

    assert result == (False, False)
    classification_writer.assert_awaited_once_with(request_data)
    request_writer.assert_not_awaited()


@pytest.mark.parametrize(
    ('classification_history', 'classification_result'),
    [(None, True), ({
        'classification_version': 2,
    }, False)],
)
def test_noncurrent_classification_keeps_arrival_ack_independent(
        classification_history, classification_result):
    instance = _controller()
    request_data = {'request_classification_history': classification_history}
    with (mock.patch.object(
            instance,
            '_persist_request_classification_history',
            new=mock.AsyncMock(return_value=classification_result)) as
          classification_writer,
          mock.patch.object(instance,
                            '_persist_request_history',
                            new=mock.AsyncMock(return_value=True)) as
          request_writer):
        result = asyncio.run(
            instance._persist_request_histories(  # pylint: disable=protected-access
                request_data))

    assert result == (True, classification_result)
    classification_writer.assert_awaited_once_with(request_data)
    request_writer.assert_awaited_once_with(request_data)


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
