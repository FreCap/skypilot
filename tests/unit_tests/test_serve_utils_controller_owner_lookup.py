"""Focused hot-path tests for serve_utils owner lookups."""
# pylint: disable=protected-access

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


def test_missing_service_row_raises_without_joined_fallback():
    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=None), \
         mock.patch.object(serve_state,
                           'get_service_from_name',
                           side_effect=AssertionError(
                               'missing rows should fail from the owner '
                               'snapshot alone')), \
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
    set_st.assert_not_called()


def test_versionless_nonnull_hash_row_is_rejected():

    def owner_lookup(unused_name, require_version=False):
        assert require_version
        return None

    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           side_effect=owner_lookup), \
         mock.patch.object(serve_state,
                           'get_service_from_name',
                           side_effect=AssertionError(
                               'versionless rows should not trigger a full '
                               'service reread')), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as set_st:
        with pytest.raises(ValueError, match='old version'):
            serve_utils.set_service_status_and_active_versions_from_replica(
                'svc', [], serve_utils.UpdateMode.ROLLING)

    set_st.assert_not_called()


def test_health_check_prefers_controller_owner_lookup():
    record = {
        'status': serve_state.ServiceStatus.READY,
        'pool': False,
    }

    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record) as owner_lookup, \
         mock.patch.object(serve_state,
                           'get_service_from_name',
                           side_effect=AssertionError(
                               'health check should not load the full '
                               'service row')), \
         mock.patch.object(serve_utils.yaml_utils,
                           'read_yaml_str',
                           side_effect=AssertionError(
                               'health check should not parse YAML')):
        assert serve_utils._check_service_status_healthy('svc',
                                                         pool=False) is None

    owner_lookup.assert_called_once_with('svc', require_version=True)


def test_health_check_rejects_pool_mismatch_without_full_service_read():
    record = {
        'status': serve_state.ServiceStatus.READY,
        'pool': True,
    }

    with mock.patch.object(serve_state,
                           'get_service_controller_owner',
                           return_value=record) as owner_lookup, \
         mock.patch.object(serve_state,
                           'get_service_from_name',
                           side_effect=AssertionError(
                               'pool mismatch should be detected from the '
                               'owner lookup alone')), \
         mock.patch.object(serve_utils.yaml_utils,
                           'read_yaml_str',
                           side_effect=AssertionError(
                               'pool mismatch should not parse YAML')):
        assert (serve_utils._check_service_status_healthy(
            'svc', pool=False) == "Service 'svc' does not exist.")

    owner_lookup.assert_called_once_with('svc', require_version=True)
