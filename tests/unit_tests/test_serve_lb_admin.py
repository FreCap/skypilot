"""Tests for the admin-only, LB-only SkyServe topology operation."""
import asyncio
import contextlib
import types
from unittest import mock

import pydantic
import pytest

from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve.server import core
from sky.serve.server import impl
from sky.serve.server import server
from sky.server.requests import payloads
from sky.server.requests import request_names


def test_public_payload_requires_a_strict_boolean():
    with pytest.raises(pydantic.ValidationError):
        payloads.ServeLoadBalancerHighAvailabilityBody(enabled=1)


def test_admin_route_schedules_fenced_lb_only_operation():
    request = types.SimpleNamespace(state=types.SimpleNamespace(
        request_id='request-id', auth_user=types.SimpleNamespace(id='admin')))
    body = payloads.ServeLoadBalancerHighAvailabilityBody(enabled=True)

    with mock.patch.object(server, '_require_admin'), \
         mock.patch.object(server.serve_state,
                           'get_service_from_name',
                           return_value={
                               'pool': False,
                               'hash': 'incarnation-a',
                           }), \
         mock.patch.object(server.executor,
                           'schedule_request_async',
                           new_callable=mock.AsyncMock) as schedule:
        asyncio.run(
            server.set_load_balancer_high_availability(request, 'svc', body))

    kwargs = schedule.call_args.kwargs
    assert kwargs['func'] is core.set_load_balancer_high_availability
    assert (kwargs['request_name']
            is request_names.RequestName.SERVE_LB_HIGH_AVAILABILITY)
    assert kwargs['request_body'].service_name == 'svc'
    assert kwargs['request_body'].enabled is True
    assert kwargs['request_body'].expected_service_hash == 'incarnation-a'


def test_impl_changes_only_lb_topology_without_allocating_version():
    lifecycle_lock = contextlib.nullcontext()
    record = {
        'hash': 'incarnation-a',
        'pool': False,
        'status': serve_state.ServiceStatus.READY,
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
         mock.patch.object(impl.serve_utils,
                           'is_consolidation_mode',
                           return_value=True), \
         mock.patch.object(impl.serve_utils,
                           'lifecycle_lock_is_valid',
                           return_value=True), \
         mock.patch.object(impl.serve_utils,
                           'get_service_lifecycle_epoch',
                           return_value=9), \
         mock.patch.object(
             impl.serve_utils,
             'set_load_balancer_high_availability_encoded') as submit, \
         mock.patch.object(impl.serve_state, 'add_version') as add_version:
        impl.set_load_balancer_high_availability('svc', True, 'incarnation-a')

    submit.assert_called_once_with('svc',
                                   True,
                                   expected_service_hash='incarnation-a',
                                   expected_lifecycle_epoch=9)
    add_version.assert_not_called()


def test_impl_rejects_replaced_service_before_controller_call():
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
                               'status': serve_state.ServiceStatus.READY,
                           }), \
         mock.patch.object(
             impl.serve_utils,
             'set_load_balancer_high_availability_encoded') as submit, \
         pytest.raises(RuntimeError, match='changed before'):
        impl.set_load_balancer_high_availability('svc', True, 'original')

    submit.assert_not_called()


def test_encoded_controller_call_carries_both_lifecycle_fences():
    response = mock.Mock(status_code=200)
    with mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value={'hash': 'incarnation-a'}), \
         mock.patch.object(serve_utils,
                           '_post_to_controller_with_retry',
                           return_value=response) as post:
        serve_utils.set_load_balancer_high_availability_encoded(
            'svc', True, 'incarnation-a', 11)

    assert post.call_args.args[:3] == (
        'svc', 'incarnation-a',
        '/controller/set_load_balancer_high_availability')
    assert post.call_args.kwargs['json'] == {
        'enabled': True,
        'service_hash': 'incarnation-a',
        'lifecycle_epoch': 11,
    }
