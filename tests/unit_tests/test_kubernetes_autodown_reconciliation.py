"""Regression tests for Kubernetes autodown reconciliation."""
# pylint: disable=protected-access
import time
from unittest import mock

import pytest

from sky import backends
from sky import clouds
from sky.backends import backend_utils
from sky.skylet import constants
from sky.skylet import events
from sky.utils import status_lib


def _handle(hooks=None):
    handle = mock.Mock(spec=backends.CloudVmRayResourceHandle)
    handle.cluster_name = 'cluster'
    handle.cluster_name_on_cloud = 'cluster-abcd1234'
    handle.cluster_yaml = '/fake/cluster.yaml'
    handle.launched_nodes = 1
    handle.num_ips_per_node = 1
    handle.launched_resources = mock.Mock(unsafe=True)
    handle.launched_resources.cloud = clouds.Kubernetes()
    handle.launched_resources.hooks = hooks
    handle.launched_resources.use_spot = False
    handle.provision_runtime_metadata = mock.Mock(has_ray=False)
    return handle


def _record():
    return {
        'status': status_lib.ClusterStatus.AUTOSTOPPING,
        'autostop': 10,
        'to_down': True,
        'cluster_hash': 'cluster-hash',
        'launched_at': time.time() - 7200,
        # Deliberately fresh. Reconciliation age comes from the transition
        # event, not this cache-freshness timestamp.
        'status_updated_at': int(time.time()),
    }


def _live_pods():
    return {'cluster-abcd1234-head': (status_lib.ClusterStatus.UP, None)}


def test_durable_event_reconciles_immediately():
    handle = _handle()
    record = _record()
    provider_config = {'namespace': 'default'}

    with mock.patch.object(
            backend_utils.k8s_instance,
            'get_cluster_autostop_event',
            return_value={'transitioned_at': int(time.time())}) as get_event, \
         mock.patch.object(
             backend_utils.global_user_state,
             'get_last_status_change_times',
             side_effect=AssertionError('event fast path must not query age')), \
         mock.patch.object(backend_utils.provision_lib,
                           'terminate_instances') as terminate:
        reconciled = backend_utils._maybe_reconcile_stalled_kubernetes_autodown(
            handle, record, _live_pods(), lambda: {'provider': provider_config})

    assert reconciled
    get_event.assert_called_once_with(provider_config,
                                      'cluster-abcd1234',
                                      since=record['launched_at'])
    terminate.assert_called_once_with(provider_name='Kubernetes',
                                      cluster_name_on_cloud='cluster-abcd1234',
                                      provider_config=provider_config)


def test_status_refresh_invokes_reconciler_before_health_probe():
    handle = _handle()
    record = _record()
    record['handle'] = handle
    provider_config = {'namespace': 'default'}
    external_failure = mock.Mock()
    external_failure.get.return_value = []

    with mock.patch.object(backend_utils,
                           '_query_cluster_status_via_cloud_api',
                           return_value=_live_pods()), \
         mock.patch.object(backend_utils, 'ExternalFailureSource',
                           external_failure), \
         mock.patch.object(backend_utils.global_user_state,
                           'get_cluster_yaml_dict',
                           return_value={'provider': provider_config}), \
         mock.patch.object(
             backend_utils.k8s_instance,
             'get_cluster_autostop_event',
             return_value={'transitioned_at': int(time.time())}), \
         mock.patch.object(backend_utils.provision_lib,
                           'terminate_instances') as terminate, \
         mock.patch.object(backend_utils.global_user_state,
                           'add_cluster_event'), \
         mock.patch.object(backend_utils.global_user_state,
                           'set_cluster_status',
                           return_value=int(time.time())):
        result = backend_utils._update_cluster_status('cluster',
                                                      record,
                                                      retry_if_missing=False)

    assert result is record
    assert result['status'] == status_lib.ClusterStatus.AUTOSTOPPING
    terminate.assert_called_once_with(provider_name='Kubernetes',
                                      cluster_name_on_cloud='cluster-abcd1234',
                                      provider_config=provider_config)


def test_fresh_transition_waits_for_hook_grace():
    handle = _handle()
    record = _record()
    transitioned_at = int(time.time()) - 60

    with mock.patch.object(backend_utils.k8s_instance,
                           'get_cluster_autostop_event',
                           return_value=None), \
         mock.patch.object(
             backend_utils.global_user_state,
             'get_first_status_change_time_since',
             return_value=transitioned_at), \
         mock.patch.object(backend_utils.provision_lib,
                           'terminate_instances') as terminate:
        reconciled = backend_utils._maybe_reconcile_stalled_kubernetes_autodown(
            handle, record, _live_pods(), lambda: {'provider': {}})

    assert not reconciled
    terminate.assert_not_called()


def test_old_transition_uses_declared_down_hook_grace():
    hooks = [
        {
            'run': 'backup',
            'events': ['down'],
            'timeout': 4000,
        },
        {
            'run': 'notify',
            'events': ['down', 'stop'],
            'timeout': 500,
        },
        {
            'run': 'ignore-for-down',
            'events': ['preemption'],
            'timeout': 9999,
        },
        {
            'run': 'defaults-to-all-events',
            'timeout': 200,
        },
    ]
    handle = _handle(hooks)
    record = _record()
    grace = (4700 +
             backend_utils._KUBERNETES_AUTODOWN_RECONCILIATION_BUFFER_SECONDS)
    transitioned_at = int(time.time()) - grace - 1

    with mock.patch.object(backend_utils.k8s_instance,
                           'get_cluster_autostop_event',
                           return_value=None), \
         mock.patch.object(
             backend_utils.global_user_state,
             'get_first_status_change_time_since',
             return_value=transitioned_at) as anchor, \
         mock.patch.object(backend_utils.provision_lib,
                           'terminate_instances') as terminate:
        reconciled = backend_utils._maybe_reconcile_stalled_kubernetes_autodown(
            handle, record, _live_pods(), lambda: {'provider': {}})

    assert reconciled
    assert backend_utils._kubernetes_autodown_reconciliation_grace_seconds(
        handle) == grace
    # The lookback must be scoped to the current launch generation.
    anchor.assert_called_once_with('cluster-hash',
                                   status_lib.ClusterStatus.AUTOSTOPPING,
                                   record['launched_at'])
    terminate.assert_called_once()


@pytest.mark.parametrize(('record_update', 'cloud', 'node_statuses'), [
    ({
        'status': status_lib.ClusterStatus.UP
    }, clouds.Kubernetes(), _live_pods()),
    ({
        'autostop': -1
    }, clouds.Kubernetes(), _live_pods()),
    ({
        'to_down': False
    }, clouds.Kubernetes(), _live_pods()),
    ({}, clouds.AWS(), _live_pods()),
    ({}, clouds.Kubernetes(), {}),
    ({}, clouds.Kubernetes(), {
        'cluster-abcd1234-head': (status_lib.ClusterStatus.UP, None),
        'unexpected-pod': (status_lib.ClusterStatus.UP, None),
    }),
])
def test_reconciliation_scope_gates(record_update, cloud, node_statuses):
    handle = _handle()
    handle.launched_resources.cloud = cloud
    record = _record()
    record.update(record_update)

    with mock.patch.object(
            backend_utils.k8s_instance,
            'get_cluster_autostop_event',
            side_effect=AssertionError('gated records must not query k8s')), \
         mock.patch.object(backend_utils.provision_lib,
                           'terminate_instances') as terminate:
        reconciled = backend_utils._maybe_reconcile_stalled_kubernetes_autodown(
            handle, record, node_statuses, lambda: {'provider': {}})

    assert not reconciled
    terminate.assert_not_called()


def test_termination_failure_propagates_for_later_sweep_retry():
    handle = _handle()
    record = _record()

    with mock.patch.object(
            backend_utils.k8s_instance,
            'get_cluster_autostop_event',
            return_value={'transitioned_at': int(time.time())}), \
         mock.patch.object(
             backend_utils.provision_lib,
             'terminate_instances',
             side_effect=RuntimeError('kubernetes delete failed')):
        with pytest.raises(RuntimeError, match='kubernetes delete failed'):
            backend_utils._maybe_reconcile_stalled_kubernetes_autodown(
                handle, record, _live_pods(), lambda: {'provider': {}})


def test_new_provisioner_autodown_does_not_gate_delete_on_ray_stop():
    stop_event = events.StopEvent.__new__(events.StopEvent)
    autostop_config = mock.Mock(down=True)
    cluster_config = {
        'cluster_name': 'cluster-abcd1234',
        'max_workers': 0,
        'provider': {},
    }
    cloud = clouds.Kubernetes()

    with mock.patch.dict(events.os.environ, {}, clear=False), \
         mock.patch.object(events.autostop_lib,
                           'set_autostopping_started'), \
         mock.patch.object(stop_event, '_execute_hook_if_present') as hooks, \
         mock.patch.object(events.subprocess, 'run') as subprocess_run, \
         mock.patch('sky.provision.terminate_instances') as terminate, \
         mock.patch(
             'sky.provision.kubernetes.instance.emit_autostop_event_best_effort'
         ) as emit_event:
        stop_event._stop_cluster_with_new_provisioner(autostop_config,
                                                      cluster_config,
                                                      'Kubernetes', cloud)

    hooks.assert_called_once_with(autostop_config)
    subprocess_run.assert_not_called()
    emit_event.assert_called_once_with({}, 'cluster-abcd1234')
    terminate.assert_called_once_with(provider_name='Kubernetes',
                                      cluster_name_on_cloud='cluster-abcd1234',
                                      provider_config={})


def test_default_grace_covers_legacy_dynamic_hook_timeout():
    handle = _handle()
    grace = backend_utils._kubernetes_autodown_reconciliation_grace_seconds(
        handle)
    assert grace == (
        constants.DEFAULT_HOOK_TIMEOUT_SECONDS +
        backend_utils._KUBERNETES_AUTODOWN_RECONCILIATION_BUFFER_SECONDS)
