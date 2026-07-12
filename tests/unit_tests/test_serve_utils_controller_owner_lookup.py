"""Focused hot-path tests for serve_utils owner lookups."""

from unittest import mock

from sky.serve import serve_state
from sky.serve import serve_utils


def test_set_service_status_from_replica_prefers_controller_owner_lookup():
    record = {
        'status': serve_state.ServiceStatus.READY,
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
    }

    with mock.patch.object(serve_state,
                           'get_service_from_name',
                           side_effect=AssertionError(
                               'full service read should not be used')), \
         mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as set_st:
        serve_utils.set_service_status_and_active_versions_from_replica(
            'svc', [], serve_utils.UpdateMode.ROLLING)

    set_st.assert_called_once()
