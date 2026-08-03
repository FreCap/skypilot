"""Probe-round bookkeeping must be one batched write, flushed pre-teardown.

At ~1k replicas on Postgres, per-replica upserts under the manager lock
exceed the 10s probe period by themselves. Batching is safe ONLY because
the whole round runs under the lock (no interleaving change) and ONLY if
the batch lands before _terminate_replica re-reads the row (probe
mutations like first_ready_time=-1.0 drive the failure classification).
"""
# pylint: disable=protected-access
import threading
import unittest
from unittest import mock

from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import system_recovery_state


def _replica_info(replica_id, probe_result):
    info = mock.Mock()
    info.replica_id = replica_id
    info.cluster_name = f'svc-replica-{replica_id}'
    info.version = 1
    info.url = f'http://10.0.0.{replica_id}:8080'
    info.is_spot = False
    info.status_property.should_track_service_status.return_value = True
    info.status_property.first_ready_time = 1.0
    info.first_consecutive_failure_time = None
    info.first_not_ready_time = None
    # This fixture models an ordinary replica.  Recovery fields must be
    # explicit: an implicit child Mock for quarantine is deliberately treated
    # as fail-closed production state and schedules legacy teardown.
    info.system_recovery_quarantine = None
    info.system_recovery_disposition = (
        system_recovery_state.SystemRecoveryDisposition.ORDINARY)
    info.system_recovery = None
    info.probe = mock.Mock(return_value=(info, probe_result, 2.0))
    info.probe_pool = mock.Mock(return_value=(info, probe_result, 2.0))
    return info


class TestProbeRoundBatching(unittest.TestCase):
    """Probe rounds snapshot shared state and persist bookkeeping in bulk."""

    def _make_manager(self):
        manager = object.__new__(replica_managers.SkyPilotReplicaManager)
        manager._ownership_lost = threading.Event()
        manager.lock = threading.Lock()
        manager._service_name = 'svc'
        manager._is_pool = False
        manager._uptime = 1.0
        manager._tick_version_spec_cache = {}
        manager._get_readiness_path = mock.Mock(return_value='/')
        manager._get_post_data = mock.Mock(return_value=None)
        manager._get_readiness_timeout_seconds = mock.Mock(return_value=15)
        manager._get_readiness_headers = mock.Mock(return_value=None)
        manager._get_initial_delay_seconds = mock.Mock(return_value=1200)
        manager._consecutive_failure_threshold_timeout = mock.Mock(
            return_value=0)
        manager._handle_preemption = mock.Mock(return_value=False)
        manager._cloud_instance_looks_alive = mock.Mock(return_value=True)
        manager._terminate_replica = mock.Mock()
        manager._resolve_probe_urls = mock.Mock(
            side_effect=lambda infos:
            {info.replica_id: info.url for info in infos})
        return manager

    def test_probe_round_passes_pre_resolved_url(self):
        manager = self._make_manager()
        info = _replica_info(1, True)

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(serve_state, 'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            manager._probe_all_replicas()

        info.probe.assert_called_once_with('/',
                                           None,
                                           15,
                                           None,
                                           'http://10.0.0.1:8080',
                                           request_started_callback=mock.ANY)
        self.assertTrue(
            callable(info.probe.call_args.kwargs['request_started_callback']))

    def test_probe_url_resolution_batches_cluster_state(self):
        manager = self._make_manager()
        del manager._resolve_probe_urls
        infos = [_replica_info(1, True), _replica_info(2, True)]
        handles = []
        cluster_records = {}
        for info in infos:
            handle = mock.Mock()
            handle.cluster_yaml = f'/tmp/{info.cluster_name}.yaml'
            handles.append(handle)
            cluster_records[info.cluster_name] = {'handle': handle}
            info.handle.return_value = handle
            info._resolve_url.return_value = info.url

        yaml_configs = [{
            'provider': {
                'context': f'context-{info.replica_id}'
            }
        } for info in infos]
        with mock.patch.object(
                replica_managers.global_user_state,
                'get_clusters_from_names',
                return_value=cluster_records) as get_clusters, \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_yaml_dict_multiple',
                 return_value=yaml_configs) as get_yamls:
            urls = manager._resolve_probe_urls(infos)

        self.assertEqual(urls, {
            1: 'http://10.0.0.1:8080',
            2: 'http://10.0.0.2:8080',
        })
        get_clusters.assert_called_once_with(['svc-replica-1', 'svc-replica-2'])
        get_yamls.assert_called_once_with(
            ['/tmp/svc-replica-1.yaml', '/tmp/svc-replica-2.yaml'])
        for info, handle, provider_config in zip(infos, handles, yaml_configs):
            info._resolve_url.assert_called_once_with(
                cluster_record=cluster_records[info.cluster_name],
                handle=handle,
                provider_config=provider_config['provider'])

    def test_probe_url_resolution_dedupes_shared_cluster_yaml_reads(self):
        manager = self._make_manager()
        del manager._resolve_probe_urls
        infos = [_replica_info(1, True), _replica_info(2, True)]
        shared_yaml = '/tmp/shared.yaml'
        shared_provider = {'provider': {'context': 'shared'}}
        cluster_records = {}
        handles = []
        for info in infos:
            handle = mock.Mock()
            handle.cluster_yaml = shared_yaml
            handles.append(handle)
            cluster_records[info.cluster_name] = {'handle': handle}
            info.handle.return_value = handle
            info._resolve_url.return_value = info.url

        with mock.patch.object(replica_managers.global_user_state,
                               'get_clusters_from_names',
                               return_value=cluster_records), mock.patch.object(
                                   replica_managers.global_user_state,
                                   'get_cluster_yaml_dict_multiple',
                                   return_value=[shared_provider]) as get_yamls:
            urls = manager._resolve_probe_urls(infos)

        self.assertEqual(urls, {
            1: 'http://10.0.0.1:8080',
            2: 'http://10.0.0.2:8080',
        })
        get_yamls.assert_called_once_with([shared_yaml])
        for info, handle in zip(infos, handles):
            info._resolve_url.assert_called_once_with(
                cluster_record=cluster_records[info.cluster_name],
                handle=handle,
                provider_config=shared_provider['provider'])

    def test_probe_reuses_supplied_url_without_resolving_again(self):
        info = object.__new__(replica_managers.ReplicaInfo)
        info.replica_id = 7
        response = mock.Mock(status_code=200)
        with mock.patch.object(replica_managers.replica_tls.requests,
                               'get',
                               return_value=response) as request:
            probed_info, ready, _ = info.probe('/health', None, 15, None,
                                               'http://10.0.0.7:8080')

        self.assertIs(probed_info, info)
        self.assertTrue(ready)
        request.assert_called_once_with('http://10.0.0.7:8080/health',
                                        headers=None,
                                        timeout=15)

    def test_single_batch_write_flushed_before_teardown(self):
        manager = self._make_manager()
        # Replica 1 healthy; replica 2 fails with an elapsed consecutive
        # failure threshold (0s) -> teardown this round.
        infos = [_replica_info(1, True), _replica_info(2, False)]
        calls = []

        def _persist_batch(*_args, **_kwargs):
            calls.append('batch')
            return True

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=infos), \
             mock.patch.object(serve_state, 'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(
                 serve_state, 'add_or_update_replicas',
                side_effect=_persist_batch) as mock_batch, \
             mock.patch.object(
                serve_state, 'add_or_update_replica',
                side_effect=AssertionError(
                    'probe round must not issue per-replica upserts')), \
             mock.patch.object(serve_state, 'get_replica_infos_from_ids',
                               return_value={}), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            manager._terminate_replica.side_effect = (
                lambda *a, **k: calls.append('teardown'))
            manager._probe_all_replicas()

        self.assertEqual(mock_batch.call_count, 1)
        self.assertEqual(mock_batch.call_args.kwargs,
                         {'expected_replica_exists': True})
        written = mock_batch.call_args.args[1]
        self.assertEqual(sorted(rid for rid, _ in written), [1, 2])
        self.assertEqual(calls, ['batch', 'teardown'])

    def test_preloads_distinct_version_specs_in_one_query(self):
        manager = self._make_manager()
        infos = [
            _replica_info(1, True),
            _replica_info(2, True),
            _replica_info(3, True),
        ]
        infos[0].version = 2
        infos[1].version = 1
        infos[2].version = 2
        specs = {
            version: mock.Mock(readiness_path=f'/ready/{version}',
                               post_data=None,
                               readiness_timeout_seconds=15,
                               readiness_headers=None) for version in (1, 2)
        }
        for method_name in ('_get_readiness_path', '_get_post_data',
                            '_get_readiness_timeout_seconds',
                            '_get_readiness_headers'):
            delattr(manager, method_name)

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=infos), \
             mock.patch.object(serve_state, 'get_specs',
                               return_value=specs) as mock_get_specs, \
             mock.patch.object(
                 serve_state, 'get_spec',
                 side_effect=AssertionError(
                     'a probe round must not read specs one at a time')), \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            manager._probe_all_replicas()

        mock_get_specs.assert_called_once_with('svc', [1, 2])
        self.assertEqual(manager._tick_version_spec_cache, specs)

    def test_missing_version_fails_before_any_probe(self):
        manager = self._make_manager()
        info = _replica_info(1, True)
        info.version = 7

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(serve_state, 'get_specs',
                               return_value={7: None}), \
             mock.patch.object(
                 serve_state, 'add_or_update_replicas') as persist:
            with self.assertRaisesRegex(ValueError, 'Version 7 not found'):
                manager._probe_all_replicas()

        info.probe.assert_not_called()
        persist.assert_not_called()

    def test_pool_probe_skips_service_spec_lookup(self):
        manager = self._make_manager()
        manager._is_pool = True
        info = _replica_info(1, True)

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(serve_state, 'get_specs') as mock_get_specs, \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            manager._probe_all_replicas()

        mock_get_specs.assert_not_called()
        info.probe_pool.assert_called_once_with()

    def test_probe_round_returns_fleet_snapshot_without_second_full_read(self):
        manager = self._make_manager()
        tracked = _replica_info(1, True)
        untracked = _replica_info(2, True)
        untracked.status_property.should_track_service_status.return_value = (
            False)

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=[tracked, untracked]) as full_read, \
             mock.patch.object(serve_state, 'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(
                 serve_state, 'get_replica_infos_from_ids',
                 side_effect=AssertionError(
                     'no teardown -> no keyed re-read')), \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            snapshot = manager._probe_all_replicas()

        # One full-fleet deserialization per round, and the returned snapshot
        # covers the whole fleet (untracked replicas included).
        self.assertEqual(full_read.call_count, 1)
        self.assertEqual(snapshot, [tracked, untracked])

    def test_probe_round_rereads_torn_down_rows_and_drops_missing(self):
        manager = self._make_manager()
        # Replicas 2 and 3 fail past the (0s) consecutive-failure threshold
        # -> teardown this round. Replica 2's row is rewritten by teardown;
        # replica 3's row is removed entirely.
        infos = [
            _replica_info(1, True),
            _replica_info(2, False),
            _replica_info(3, False),
        ]
        refreshed_2 = _replica_info(2, False)
        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=infos), \
             mock.patch.object(serve_state, 'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(
                 serve_state, 'get_replica_infos_from_ids',
                 return_value={2: refreshed_2}) as keyed_read, \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            snapshot = manager._probe_all_replicas()

        keyed_read.assert_called_once_with('svc', [2, 3])
        self.assertEqual(snapshot, [infos[0], refreshed_2])

    def test_prober_tick_feeds_snapshot_to_status_update(self):
        manager = self._make_manager()
        manager._update_mode = mock.Mock()
        info = _replica_info(1, True)

        class _StopLoop(BaseException):
            pass

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=[info]) as full_read, \
             mock.patch.object(serve_state, 'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'set_service_status_and_active_versions_from_replica'
             ) as status_update, \
             mock.patch.object(manager._ownership_lost,
                               'wait', side_effect=_StopLoop) as ownership_wait:
            manager._get_endpoint_probe_interval_seconds = mock.Mock(
                return_value=10)
            with self.assertRaises(_StopLoop):
                manager._replica_prober()

        # The tick must not re-read the fleet for the status update: exactly
        # one full read (inside the probe round), and the status update is
        # fed the round's returned snapshot.
        self.assertEqual(full_read.call_count, 1)
        status_update.assert_called_once_with('svc', [info],
                                              manager._update_mode,
                                              target_num_replicas=None)
        ownership_wait.assert_called_once_with(10)

    def test_no_tracked_replicas_skips_probe_round_work(self):
        manager = self._make_manager()
        info = _replica_info(1, True)
        info.status_property.should_track_service_status.return_value = False

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(serve_state, 'get_specs') as mock_get_specs, \
             mock.patch.object(serve_state,
                               'add_or_update_replicas') as persist, \
             mock.patch.object(replica_managers.mp_pool,
                               'ThreadPool') as thread_pool:
            manager._probe_all_replicas()

        mock_get_specs.assert_not_called()
        persist.assert_not_called()
        thread_pool.assert_not_called()
