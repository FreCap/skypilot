"""Tests for mode-filtered SkyServe HA recovery sweeps."""

import contextlib
from unittest import mock

from sky.serve import serve_state
from sky.serve import serve_utils


def _mode_names(unused_patterns=None, pool=None):
    del unused_patterns
    if pool is None:
        return ['service-a', 'pool-a']
    return ['pool-a'] if pool else ['service-a']


def _service_record():
    return {
        'controller_pid': None,
        'controller_ip': None,
        'hash': 'service-hash',
        'resource_scope': 'service-hash',
        'status': serve_state.ServiceStatus.READY,
    }


def _run_sweep(tmp_path, pool):
    with contextlib.ExitStack() as stack:
        names = stack.enter_context(
            mock.patch.object(serve_state,
                              'get_glob_service_names',
                              side_effect=_mode_names))
        get_status = stack.enter_context(
            mock.patch.object(serve_utils,
                              '_get_service_status',
                              return_value=_service_record()))
        committed = stack.enter_context(
            mock.patch.object(serve_state, 'get_latest_committed_version'))
        identity = stack.enter_context(
            mock.patch.object(serve_state, 'get_service_mode_and_hash'))
        retire = stack.enter_context(
            mock.patch.object(serve_state,
                              'mark_unrecoverable_service_for_cleanup'))
        stack.enter_context(
            mock.patch.object(serve_state,
                              'get_ha_recovery_script',
                              return_value=None))
        stack.enter_context(
            mock.patch.object(serve_utils,
                              '_snapshot_in_flight_start_service_incarnations',
                              return_value=set()))
        stack.enter_context(
            mock.patch.object(serve_utils.command_runner,
                              'LocalProcessCommandRunner'))
        stack.enter_context(
            mock.patch.object(serve_utils.skylet_constants,
                              'HA_PERSISTENT_RECOVERY_LOG_PATH',
                              str(tmp_path / 'recovery_{}.log')))
        reconcile = None
        if not pool:
            reconcile = stack.enter_context(
                mock.patch('sky.serve.lb_k8s.reconcile_lb_objects'))
        serve_utils.ha_recovery_for_consolidation_mode(pool=pool)
    return names, get_status, committed, identity, retire, reconcile


def test_pool_sweep_skips_service_rows_before_status_reads(tmp_path):
    names, get_status, committed, identity, retire, _ = _run_sweep(tmp_path,
                                                                   pool=True)
    names.assert_called_once_with(None, pool=True)
    get_status.assert_called_once_with('pool-a',
                                       pool=True,
                                       with_replica_info=False,
                                       with_yaml=False)
    committed.assert_not_called()
    identity.assert_not_called()
    retire.assert_not_called()


def test_service_sweep_reconciles_only_live_service_names(tmp_path):
    names, get_status, committed, identity, retire, reconcile = _run_sweep(
        tmp_path, pool=False)
    names.assert_called_once_with(None, pool=False)
    get_status.assert_called_once_with('service-a',
                                       pool=False,
                                       with_replica_info=False,
                                       with_yaml=False)
    committed.assert_not_called()
    identity.assert_not_called()
    retire.assert_not_called()
    assert reconcile is not None
    reconcile.assert_called_once_with({'service-a'})
