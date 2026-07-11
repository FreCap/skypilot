"""Tests for skipping pool YAML on liveness-only status paths."""

from unittest import mock

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
                                       with_pool_yaml=False)


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
                                       with_pool_yaml=False)
