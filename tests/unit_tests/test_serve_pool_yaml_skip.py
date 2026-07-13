"""Tests for skipping pool YAML on liveness-only status paths."""

from unittest import mock

import pytest

from sky.serve import serve_state
from sky.serve import serve_utils


def test_ha_recovery_pool_liveness_skips_pool_yaml(tmp_path):
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc']), \
         mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value=None) as get_status, \
         mock.patch.object(serve_state,
                           'get_latest_committed_version',
                           return_value=None), \
         mock.patch.object(serve_state,
                           'get_service_mode_and_hash',
                           return_value=(True, 'orphan-hash')), \
         mock.patch.object(
             serve_state,
             'mark_unrecoverable_service_for_cleanup',
             return_value=True), \
         mock.patch.object(
             serve_utils,
             '_snapshot_in_flight_start_service_incarnations',
             return_value=set()), \
         mock.patch.object(
             serve_utils.skylet_constants,
             'HA_PERSISTENT_RECOVERY_LOG_PATH',
             str(tmp_path / 'recovery_{}.log')), \
         mock.patch.object(serve_utils.command_runner,
                           'LocalProcessCommandRunner'):
        serve_utils.ha_recovery_for_consolidation_mode(pool=True)

    get_status.assert_called_once_with('svc',
                                       pool=True,
                                       with_replica_info=False,
                                       with_yaml=False)


def test_update_pool_status_liveness_skips_pool_yaml():
    record = {
        'status': serve_state.ServiceStatus.READY,
        'controller_pid': 123,
        'controller_ip': '127.0.0.1',
        'controller_job_id': None,
        'hash': 'incarnation-a',
        'resource_scope': 'scope-a',
    }
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['pool-a']), \
         mock.patch.object(serve_utils,
                           '_get_service_status',
                           return_value=record) as get_status, \
         mock.patch.object(serve_utils,
                           '_controller_process_alive',
                           return_value=True):
        serve_utils.update_service_status(pool=True)

    get_status.assert_called_once_with('pool-a',
                                       pool=True,
                                       with_replica_info=False,
                                       with_yaml=False,
                                       with_target_num_replicas=False)


@pytest.mark.parametrize(
    ('controller_pid', 'controller_alive', 'expected_status_writes'),
    [(123, True, 0), (None, False, 1)],
)
def test_update_status_liveness_skips_target_fetch(controller_pid,
                                                   controller_alive,
                                                   expected_status_writes):
    record = {
        'name': 'serve-a',
        'pool': False,
        'status': serve_state.ServiceStatus.READY,
        'controller_pid': controller_pid,
        'controller_ip': '127.0.0.1',
        'controller_job_id': None,
        'hash': 'incarnation-a',
        'resource_scope': 'scope-a',
    }
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['serve-a']), \
         mock.patch.object(serve_state,
                           'get_service_from_name',
                           return_value=record), \
         mock.patch.object(
             serve_utils, '_get_to_controller_with_retry') as target_fetch, \
         mock.patch.object(serve_utils,
                           '_controller_process_alive',
                           return_value=controller_alive), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as status_write:
        serve_utils.update_service_status(pool=False)

    target_fetch.assert_not_called()
    assert status_write.call_count == expected_status_writes
    if expected_status_writes:
        status_write.assert_called_once_with(
            'serve-a',
            'incarnation-a',
            controller_pid,
            '127.0.0.1',
            serve_state.ServiceStatus.CONTROLLER_FAILED,
            expected_status=serve_state.ServiceStatus.READY)
