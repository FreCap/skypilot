"""Focused hot-path tests for serve_utils owner lookups."""

from unittest import mock

import pytest

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


def test_missing_service_row_raises_after_compatibility_fallback():
    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=None), \
         mock.patch.object(serve_state,
                           'get_service_from_name',
                           return_value=None) as full_read, \
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
    full_read.assert_called_once_with('svc')
    set_st.assert_not_called()


def test_versionless_nonnull_hash_row_is_rejected():
    # Interrupted pre-atomic registration can leave a versionless row with a
    # durable hash, so hash presence alone cannot identify a valid service.
    record = {
        'status': serve_state.ServiceStatus.READY,
        'hash': 'orphan',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
    }

    def owner_lookup(unused_name, require_version=False):
        return None if require_version else record

    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           side_effect=owner_lookup), \
         mock.patch.object(serve_state,
                           'get_service_from_name',
                           return_value=None) as full_read, \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as set_st:
        with pytest.raises(ValueError, match='old version'):
            serve_utils.set_service_status_and_active_versions_from_replica(
                'svc', [], serve_utils.UpdateMode.ROLLING)

    full_read.assert_called_once_with('svc')
    set_st.assert_not_called()
