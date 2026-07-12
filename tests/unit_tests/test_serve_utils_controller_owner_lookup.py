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


def test_missing_service_row_raises_without_full_read_fallback():
    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=None), \
         mock.patch.object(serve_state,
                           'get_service_from_name') as full_read, \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as set_st:
        try:
            serve_utils.set_service_status_and_active_versions_from_replica(
                'svc', [], serve_utils.UpdateMode.ROLLING)
            raised = False
        except ValueError:
            raised = True

    assert raised
    full_read.assert_not_called()
    set_st.assert_not_called()


def test_versionless_legacy_row_is_refused_without_status_write():
    # Legacy rows predate the hash column: the owner lookup returns the row
    # (NULL hash) while the latest-version join in get_service_from_name
    # drops it. The write must be refused without raising and without
    # touching the service status.
    record = {
        'status': serve_state.ServiceStatus.READY,
        'hash': None,
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
    }

    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as set_st:
        serve_utils.set_service_status_and_active_versions_from_replica(
            'svc', [], serve_utils.UpdateMode.ROLLING)

    set_st.assert_not_called()
