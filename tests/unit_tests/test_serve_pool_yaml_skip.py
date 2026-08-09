"""Tests for skipping pool YAML on liveness-only status paths."""

# Fixture imports are referenced indirectly by pytest, and the helper imports
# keep these call-site tests on the real DB-backed read paths they cover.
# pylint: disable=unused-import
from unittest import mock

import pytest
from test_serve_state import _add_minimal_service
from test_serve_state import _count_sql_statements
from test_serve_state import _mock_serve_db

from sky.serve import serve_state
from sky.serve import serve_utils


def test_ha_recovery_pool_liveness_skips_pool_yaml(tmp_path):
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc']), \
         mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[]) as snapshots, \
         mock.patch.object(serve_utils,
                           '_get_service_status') as get_status, \
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

    snapshots.assert_called_once_with(pool=True)
    # The sweep must never fall back to the per-service joined read.
    get_status.assert_not_called()


def test_update_pool_status_liveness_skips_pool_yaml():
    record = {
        'name': 'pool-a',
        'status': serve_state.ServiceStatus.READY,
        'controller_pid': 123,
        'controller_ip': '127.0.0.1',
        'controller_job_id': None,
        'hash': 'incarnation-a',
        'resource_scope': 'scope-a',
    }
    with mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[record]) as snapshot, \
         mock.patch.object(serve_utils,
                           '_get_service_status') as full_status, \
         mock.patch.object(serve_utils,
                           '_controller_process_alive',
                           return_value=True):
        serve_utils.update_service_status(pool=True)

    snapshot.assert_called_once_with(pool=True)
    full_status.assert_not_called()


@pytest.mark.parametrize(('pool', 'noun'), [(False, 'service'), (True, 'pool')])
def test_terminate_services_skips_display_yaml(pool, noun):
    record = {
        'name': 'target-a',
        'pool': pool,
        'status': serve_state.ServiceStatus.SHUTTING_DOWN,
        'hash': 'incarnation-a',
        'resource_scope': 'scope-a',
    }
    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['target-a']), \
         mock.patch.object(serve_state,
                           'get_service_status_snapshot',
                           return_value=record), \
         mock.patch.object(serve_state,
                           'get_service_from_name',
                           side_effect=AssertionError(
                               'termination must not read the full '
                               'service row')), \
         mock.patch.object(serve_utils,
                           'get_yaml_content',
                           side_effect=AssertionError(
                               'termination must not load display YAML')) \
                 as get_yaml:
        message = serve_utils.terminate_services(['target-a'],
                                                 purge=False,
                                                 pool=pool)

    get_yaml.assert_not_called()
    assert message == f'No {noun} to terminate.'


def test_terminate_services_uses_status_snapshot_without_loading_spec(
        _mock_serve_db, monkeypatch):
    assert _add_minimal_service('svc-shutdown',
                                service_hash='incarnation-a') is True
    serve_state.set_service_status_and_active_versions(
        'svc-shutdown',
        serve_state.ServiceStatus.SHUTTING_DOWN,
        active_versions=[],
    )

    def fail_if_spec_loaded(*args, **kwargs):
        del args, kwargs
        raise AssertionError('termination must not deserialize the latest spec')

    monkeypatch.setattr(serve_state.pickle, 'loads', fail_if_spec_loaded)

    with mock.patch.object(serve_state,
                           'get_glob_service_names',
                           return_value=['svc-shutdown']), \
         _count_sql_statements(_mock_serve_db) as counts:
        message = serve_utils.terminate_services(['svc-shutdown'],
                                                 purge=False,
                                                 pool=False)

    assert counts['n'] == 1, counts
    assert message == 'No service to terminate.'


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
                           'get_service_liveness_snapshots',
                           return_value=[record]) as snapshot, \
         mock.patch.object(
             serve_utils, '_get_to_controller_with_retry') as target_fetch, \
         mock.patch.object(serve_utils,
                           '_controller_process_alive',
                           return_value=controller_alive), \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as status_write:
        serve_utils.update_service_status(pool=False)

    snapshot.assert_called_once_with(pool=False)
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


@pytest.mark.parametrize('terminal_status',
                         serve_state.ServiceStatus.terminal_statuses())
def test_update_status_preserves_terminal_lifecycle_state(terminal_status):
    record = {
        'name': 'serve-a',
        'status': terminal_status,
        'controller_pid': None,
        'controller_ip': '127.0.0.1',
        'controller_job_id': None,
        'hash': 'incarnation-a',
        'resource_scope': 'scope-a',
    }
    with mock.patch.object(serve_state,
                           'get_service_liveness_snapshots',
                           return_value=[record]), \
         mock.patch.object(serve_utils,
                           '_controller_process_alive') as process_alive, \
         mock.patch.object(
             serve_state,
             'set_service_status_and_active_versions_if_owner') as status_write:
        serve_utils.update_service_status(pool=False)

    process_alive.assert_not_called()
    status_write.assert_not_called()
