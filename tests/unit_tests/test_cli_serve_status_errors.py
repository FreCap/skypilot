"""Tests for _handle_services_request's transport-error handling.

A failed status fetch (e.g. an ingress 504 on a slow response) must never
be rendered as a normal-looking "No live services." table: in
consolidation mode there is no controller cluster record, so the old
`not records` fallback misreported a live fleet as nonexistent. The hint
is only valid when a controller cluster record exists and is STOPPED
(autostopped VM-mode controller).
"""
from unittest import mock

import pytest

from sky.client.cli import command
from sky.utils import status_lib


def _run_handler(controller_records):
    with mock.patch.object(command.sdk, 'get',
                           side_effect=[RuntimeError('504 gateway timeout'),
                                        controller_records]), \
         mock.patch.object(command.sdk, 'status', return_value='req-ctrl'):
        return command._handle_services_request('req-svc',
                                                service_names=None,
                                                show_all=False,
                                                show_endpoint=False,
                                                is_called_by_user=True)


def test_transport_error_with_no_controller_record_raises():
    # Consolidation mode: no controller cluster exists — the transport
    # error must surface, not an empty table.
    with pytest.raises(RuntimeError):
        _run_handler(controller_records=[])


def test_stopped_controller_still_shows_hint():
    num, msg = _run_handler(controller_records=[{
        'status': status_lib.ClusterStatus.STOPPED
    }])
    assert num is None
    assert 'No live services' in msg


def test_up_controller_with_transport_error_raises():
    with pytest.raises(RuntimeError):
        _run_handler(controller_records=[{
            'status': status_lib.ClusterStatus.UP
        }])


def _run_handler_pool(controller_records):
    captured = {}

    def _status(cluster_names, all_users):
        captured['cluster_names'] = cluster_names
        del all_users
        return 'req-ctrl'

    with mock.patch.object(command.sdk, 'get',
                           side_effect=[RuntimeError('504 gateway timeout'),
                                        controller_records]), \
         mock.patch.object(command.sdk, 'status', side_effect=_status):
        result = command._handle_services_request('req-pool',
                                                  service_names=None,
                                                  show_all=False,
                                                  show_endpoint=False,
                                                  pool=True,
                                                  is_called_by_user=True)
    return result, captured


def test_pool_fallback_probes_jobs_controller():
    (num, msg), captured = _run_handler_pool(controller_records=[{
        'status': status_lib.ClusterStatus.STOPPED
    }])
    # Pools run on the JOBS controller: the fallback must probe it, not
    # the serve controller.
    assert captured['cluster_names'] == ['sky-jobs-controller-*']
    assert num is None
    assert 'pool' in msg.lower() or 'job' in msg.lower()


def test_pool_transport_error_with_no_record_raises():
    with pytest.raises(RuntimeError):
        _run_handler_pool(controller_records=[])
