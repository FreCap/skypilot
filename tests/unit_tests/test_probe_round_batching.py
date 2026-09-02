"""Probe-round bookkeeping must be one batched write, flushed pre-teardown.

At ~1k replicas on Postgres, per-replica upserts under the manager lock
exceed the 10s probe period by themselves. Remote evidence is collected
without the fleet lock; an exact lifecycle re-read, reduction, and batched
write then happen under the lock before durable teardown is scheduled.
"""
# pylint: disable=protected-access
import ast
import contextlib
import inspect
import os
import textwrap
import threading
import unittest
from unittest import mock
import uuid

from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import system_recovery_state


def _replica_info(replica_id, probe_result):
    info = mock.Mock()
    info.replica_id = replica_id
    info.replica_record_id = str(uuid.UUID(int=replica_id))
    info.cluster_name = f'svc-replica-{replica_id}'
    info.version = 1
    info.url = f'http://10.0.0.{replica_id}:8080'
    info.is_spot = False
    info.status_property.should_track_service_status.return_value = True
    info.status_property.service_ready_now = probe_result
    info.status_property.first_ready_time = 1.0
    info.status_property.preempted = False
    info.status_property.purged = False
    info.status_property.is_scale_down = False
    info.status_property.sky_down_status = None
    info.status_property.wait_for_idle_before_termination = False
    info.is_terminal = False
    info.first_consecutive_failure_time = None
    info.first_not_ready_time = None
    # This fixture models an ordinary replica.  Recovery fields must be
    # explicit: an implicit child Mock for quarantine is deliberately treated
    # as fail-closed production state and schedules legacy teardown.
    info.system_recovery_quarantine = None
    info.system_recovery_disposition = (
        system_recovery_state.SystemRecoveryDisposition.ORDINARY)
    info.system_recovery = None
    info.system_recovery_revision = 0
    info.probe = mock.Mock(return_value=(info, probe_result, 2.0))
    info.probe_pool = mock.Mock(return_value=(info, probe_result, 2.0))
    return info


class TestProbeRoundBatching(unittest.TestCase):
    """Probe rounds snapshot shared state and persist bookkeeping in bulk."""

    def test_readiness_executor_is_bounded_reused_and_replaced_after_shutdown(
            self):
        manager = self._make_manager()
        first = mock.Mock()
        second = mock.Mock()
        with mock.patch.object(replica_managers.subprocess_utils,
                               'ContextThreadPoolExecutor',
                               side_effect=[first, second]) as executor_factory:
            self.assertIs(manager._get_readiness_executor(), first)
            self.assertIs(manager._get_readiness_executor(), first)
            executor_factory.assert_called_once_with(
                max_workers=manager._PROBE_ROUND_MAX_PARALLELISM,
                thread_name_prefix='serve-readiness')

            manager._shutdown_readiness_executor()
            first.shutdown.assert_called_once_with(wait=True,
                                                   cancel_futures=True)
            self.assertIs(manager._get_readiness_executor(), second)
            self.assertEqual(executor_factory.call_count, 2)
            manager._shutdown_readiness_executor()
            second.shutdown.assert_called_once_with(wait=True,
                                                    cancel_futures=True)

    def test_changed_only_rollout_flag_is_default_off_and_strict(self):
        env_var = (replica_managers._CHANGED_ONLY_READINESS_PERSISTENCE_ENV_VAR)
        for value, expected in ((None, False), ('false', False),
                                ('FALSE', False), ('true', True), ('TRUE',
                                                                   True)):
            with self.subTest(value=value), mock.patch.dict(os.environ, {},
                                                            clear=True):
                if value is not None:
                    os.environ[env_var] = value
                self.assertEqual(
                    replica_managers.
                    _changed_only_readiness_persistence_enabled(), expected)

        with mock.patch.dict(os.environ, {env_var: 'yes'}, clear=True), \
             mock.patch.object(replica_managers.logger, 'warning') as warning:
            self.assertFalse(
                replica_managers._changed_only_readiness_persistence_enabled())
        warning.assert_called_once()

    def _make_manager(self):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._ownership_lost = threading.Event()
        manager.lock = threading.Lock()
        manager._service_name = 'svc'
        manager._changed_only_readiness_persistence = False
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
        manager._cloud_instance_looks_alive = mock.Mock(
            return_value=(replica_managers._PreemptionPrefilterResult(
                replica_managers._PreemptionPrefilterDisposition.
                LIVE_OR_UNPROVEN)))
        manager._apply_confirmed_preemption = mock.Mock()
        manager._terminate_replica = mock.Mock()
        manager._resolve_probe_urls = mock.Mock(
            side_effect=lambda infos, **_kwargs:
            {info.replica_id: info.url for info in infos})
        manager._route_projection_publisher = None
        return manager

    def _run_probe_round(self, manager, infos, *, refreshed=None):
        if refreshed is None:
            refreshed = {info.replica_id: info for info in infos}
        with mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=infos), \
             mock.patch.object(serve_state,
                               'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(serve_state,
                               'get_replica_info_from_id',
                               side_effect=lambda _service_name, replica_id:
                               next((info for info in infos
                                     if info.replica_id == replica_id), None)), \
             mock.patch.object(serve_state,
                               'get_replica_infos_from_ids',
                               return_value=refreshed), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_clusters_from_names',
                 return_value={}), \
             mock.patch.object(serve_state, 'set_service_uptime'), \
             mock.patch.object(manager, '_persist_replicas') as persist:
            snapshot = manager._probe_all_replicas()
        return persist, snapshot

    @staticmethod
    def _start_capturing_thread(target, errors):

        def _run():
            try:
                target()
            except BaseException as error:  # pylint: disable=broad-except
                errors.append(error)

        thread = threading.Thread(target=_run)
        thread.start()
        return thread

    def test_blocked_readiness_does_not_block_concrete_scale_up_batch(self):
        manager = self._make_manager()
        info = _replica_info(1, True)
        http_entered = threading.Event()
        release_http = threading.Event()
        scale_completed = threading.Event()
        errors = []

        def _probe(*_args, request_started_callback=None, **_kwargs):
            self.assertIsNotNone(request_started_callback)
            request_started_callback(1.0)
            http_entered.set()
            self.assertTrue(release_http.wait(timeout=5))
            return info, True, 2.0

        def _scale_batch(*_args, **_kwargs):
            scale_completed.set()
            return []

        info.probe = mock.Mock(side_effect=_probe)
        manager._scale_up_batch_locked = mock.Mock(side_effect=_scale_batch)
        with mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(serve_state,
                               'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(
                 serve_state,
                 'get_replica_infos_from_ids',
                 return_value={1: info}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_clusters_from_names',
                 return_value={}), \
             mock.patch.object(serve_state, 'set_service_uptime'), \
             mock.patch.object(manager, '_persist_replicas'):
            probe_thread = self._start_capturing_thread(
                manager._probe_all_replicas, errors)
            self.assertTrue(http_entered.wait(timeout=5))
            scale_thread = self._start_capturing_thread(
                lambda: manager.scale_up_batch([None]), errors)
            try:
                self.assertTrue(scale_completed.wait(timeout=2),
                                'scale_up_batch was blocked by readiness HTTP')
            finally:
                release_http.set()
                probe_thread.join(timeout=5)
                scale_thread.join(timeout=5)

        self.assertFalse(probe_thread.is_alive())
        self.assertFalse(scale_thread.is_alive())
        self.assertEqual(errors, [])
        manager._scale_up_batch_locked.assert_called_once_with([None], None)

    def test_stale_http_evidence_has_no_write_route_or_teardown_effects(self):
        cases = ('revision', 'teardown', 'same-id-recreation')
        for case in cases:
            with self.subTest(case=case):
                manager = self._make_manager()
                manager._changed_only_readiness_persistence = True
                opening = _replica_info(1, True)
                opening.status_property.service_ready_now = False
                fresh = _replica_info(1, True)
                fresh.status_property.service_ready_now = False
                if case == 'revision':
                    fresh.system_recovery_revision += 1
                elif case == 'teardown':
                    fresh.status_property.sky_down_status = (
                        replica_managers.common_utils.ProcessStatus.SCHEDULED)
                else:
                    fresh.replica_record_id = str(uuid.UUID(int=1001))

                current = {'row': opening}
                http_entered = threading.Event()
                release_http = threading.Event()
                errors = []
                route_material = object()

                def _probe(*_args, request_started_callback=None, **_kwargs):
                    self.assertIsNotNone(request_started_callback)
                    request_started_callback(1.0)
                    http_entered.set()
                    self.assertTrue(release_http.wait(timeout=5))
                    return opening, True, 2.0

                def _resolve(infos, **kwargs):
                    self.assertEqual(infos, [opening])
                    kwargs['resolved_route_material'][1] = route_material
                    return {1: opening.url}

                def _full_read(_service_name):
                    return [current['row']]

                def _keyed_read(_service_name, replica_ids):
                    return ({1: current['row']} if 1 in replica_ids else {})

                opening.probe = mock.Mock(side_effect=_probe)
                manager._resolve_probe_urls = mock.Mock(side_effect=_resolve)
                with mock.patch.object(serve_state,
                                       'get_replica_infos',
                                       side_effect=_full_read), \
                     mock.patch.object(serve_state,
                                       'get_specs',
                                       return_value={1: mock.Mock()}), \
                     mock.patch.object(serve_state,
                                       'get_replica_infos_from_ids',
                                       side_effect=_keyed_read), \
                     mock.patch.object(
                         replica_managers.global_user_state,
                         'get_clusters_from_names',
                         return_value={}), \
                     mock.patch.object(manager,
                                       '_persist_replicas') as persist, \
                     mock.patch.object(
                         manager,
                         '_write_resolved_route_materials') as write_routes, \
                     mock.patch.object(
                         manager,
                         '_issue_system_recovery_route') as issue_route, \
                     mock.patch.object(
                         manager,
                         '_prepare_probe_teardown_intent') as prepare_down, \
                     mock.patch.object(
                         manager,
                         '_launch_completion_state') as wake_cleanup, \
                     mock.patch.object(serve_state, 'set_service_uptime'):
                    probe_thread = self._start_capturing_thread(
                        manager._probe_all_replicas, errors)
                    self.assertTrue(http_entered.wait(timeout=5))
                    lock_acquired = False
                    try:
                        lock_acquired = manager.lock.acquire(timeout=2)
                        self.assertTrue(
                            lock_acquired,
                            'readiness HTTP retained the manager lock')
                        current['row'] = fresh
                    finally:
                        if lock_acquired:
                            manager.lock.release()
                        release_http.set()
                        probe_thread.join(timeout=5)

                self.assertFalse(probe_thread.is_alive())
                self.assertEqual(errors, [])
                persist.assert_not_called()
                write_routes.assert_not_called()
                issue_route.assert_not_called()
                prepare_down.assert_not_called()
                wake_cleanup.assert_not_called()
                manager._terminate_replica.assert_not_called()
                self.assertEqual(
                    manager._last_probe_route_result.resolved_routes, {})
                self.assertFalse(manager._last_probe_route_result.complete)

    def test_recovery_revision_change_after_bulk_reread_is_side_effect_free(
            self):
        manager = self._make_manager()
        manager._changed_only_readiness_persistence = True
        manager._service_hash = 'incarnation-a'
        manager._controller_owner = (100, '10.0.0.1')

        candidate = _replica_info(1, True)
        candidate.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.CANDIDATE)
        candidate.service_job_id = None
        capable = _replica_info(2, True)
        capable.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.CAPABLE)
        capable.system_recovery = mock.Mock(
            state=system_recovery_state.ControllerRecoveryState.ARMED)

        exact_candidate = _replica_info(1, True)
        exact_candidate.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.CANDIDATE)
        exact_candidate.service_job_id = None
        exact_capable = _replica_info(2, True)
        exact_capable.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.CAPABLE)
        exact_capable.system_recovery = capable.system_recovery

        newer_candidate = _replica_info(1, True)
        newer_candidate.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.CANDIDATE)
        newer_candidate.service_job_id = None
        newer_candidate.system_recovery_revision = 1
        newer_capable = _replica_info(2, True)
        newer_capable.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.CAPABLE)
        newer_capable.system_recovery = capable.system_recovery
        newer_capable.system_recovery_revision = 1

        manager._candidate_release_monotonic_deadlines = {1: 123.0}
        manager._system_recovery_status_initialized = {2}
        manager._system_recovery_route_generation = mock.Mock(return_value=None)
        opening = [candidate, capable]
        exact = {1: exact_candidate, 2: exact_capable}
        newer = {1: newer_candidate, 2: newer_capable}

        with mock.patch.object(serve_state,
                               'get_replica_infos',
                               side_effect=[opening, list(newer.values())]), \
             mock.patch.object(serve_state,
                               'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(serve_state,
                               'get_replica_infos_from_ids',
                               return_value=exact), \
             mock.patch.object(serve_state,
                               'get_replica_info_from_id',
                               side_effect=lambda _service_name, replica_id:
                               newer[replica_id]), \
             mock.patch.object(
                 serve_state,
                 'get_service_controller_owner',
                 return_value={
                     'hash': 'incarnation-a',
                     'controller_pid': 100,
                     'controller_ip': '10.0.0.1',
                     'lifecycle_epoch': 7,
                 }), \
             mock.patch.object(
                 serve_state,
                 'patch_replica_system_recovery') as recovery_patch, \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_clusters_from_names',
                 return_value={}), \
             mock.patch.object(manager,
                               '_persist_replicas') as persist, \
             mock.patch.object(
                 manager,
                 '_write_resolved_route_materials') as write_routes, \
             mock.patch.object(
                 manager,
                 '_issue_system_recovery_route') as issue_route, \
             mock.patch.object(
                 manager,
                 '_prepare_probe_teardown_intent') as prepare_down, \
             mock.patch.object(
                 manager,
                 '_launch_completion_state') as wake_cleanup, \
             mock.patch.object(serve_state, 'set_service_uptime'):
            snapshot = manager._probe_all_replicas()

        self.assertEqual(snapshot, list(newer.values()))
        recovery_patch.assert_not_called()
        persist.assert_not_called()
        write_routes.assert_not_called()
        issue_route.assert_not_called()
        prepare_down.assert_not_called()
        wake_cleanup.assert_not_called()
        self.assertEqual(manager._candidate_release_monotonic_deadlines,
                         {1: 123.0})
        self.assertEqual(manager._system_recovery_status_initialized, {2})
        self.assertFalse(manager._last_probe_route_result.complete)

    def test_closing_snapshot_includes_concurrent_add_and_excludes_delete(self):
        manager = self._make_manager()
        removed = _replica_info(1, True)
        survivor = _replica_info(2, True)
        added = _replica_info(3, True)
        fleet = {'rows': [removed, survivor]}
        http_entered = threading.Event()
        release_http = threading.Event()
        errors = []
        result = {}

        def _blocked_probe(*_args, request_started_callback=None, **_kwargs):
            self.assertIsNotNone(request_started_callback)
            request_started_callback(1.0)
            http_entered.set()
            self.assertTrue(release_http.wait(timeout=5))
            return removed, True, 2.0

        def _full_read(_service_name):
            return list(fleet['rows'])

        def _keyed_read(_service_name, replica_ids):
            requested = set(replica_ids)
            return {
                info.replica_id: info
                for info in fleet['rows']
                if info.replica_id in requested
            }

        def _probe_round():
            result['snapshot'] = manager._probe_all_replicas()

        removed.probe = mock.Mock(side_effect=_blocked_probe)
        with mock.patch.object(serve_state,
                               'get_replica_infos',
                               side_effect=_full_read) as full_read, \
             mock.patch.object(serve_state,
                               'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=_keyed_read), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_clusters_from_names',
                 return_value={}), \
            mock.patch.object(manager, '_persist_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            probe_thread = self._start_capturing_thread(_probe_round, errors)
            self.assertTrue(http_entered.wait(timeout=5))
            lock_acquired = False
            try:
                lock_acquired = manager.lock.acquire(timeout=2)
                self.assertTrue(lock_acquired,
                                'readiness HTTP retained the manager lock')
                fleet['rows'] = [survivor, added]
            finally:
                if lock_acquired:
                    manager.lock.release()
                release_http.set()
                probe_thread.join(timeout=5)

        self.assertFalse(probe_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(full_read.call_count, 2)
        self.assertEqual([info.replica_id for info in result['snapshot']],
                         [2, 3])
        added.probe.assert_not_called()

    def test_v2_readiness_http_releases_opposite_provider_phase(self):
        manager = self._make_manager()
        del manager._resolve_probe_urls
        info = _replica_info(1, True)
        cleanup_fence = (
            replica_managers.reserved_capacity.ProtocolV2CleanupFence(
                kubernetes_context='ctx', physical_cluster_uid='physical-uid'))
        http_entered = threading.Event()
        release_http = threading.Event()
        errors = []
        resolver_admissions = []
        fresh_gate = replica_managers.provider_phase._ProviderPhaseGate(
            register_at_fork=False)
        handle = mock.Mock(
            spec=replica_managers.backends.CloudVmRayResourceHandle)
        handle.launched_resources = mock.Mock(accelerators={'L4': 1})
        info._resolve_url = mock.Mock(return_value=info.url)

        def _probe(*_args, request_started_callback=None, **_kwargs):
            self.assertIsNotNone(request_started_callback)
            request_started_callback(1.0)
            http_entered.set()
            self.assertTrue(release_http.wait(timeout=5))
            return info, True, 2.0

        def _provider_fence(*_args, phase_admission=None, **_kwargs):
            if phase_admission is not None:
                resolver_admissions.append(phase_admission)
            return contextlib.nullcontext()

        info.probe = mock.Mock(side_effect=_probe)
        with mock.patch.object(serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(serve_state,
                               'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(
                 serve_state,
                 'get_replica_infos_from_ids',
                 return_value={1: info}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_clusters_from_names',
                 return_value={info.cluster_name: {'handle': handle}}), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'get_provider_configs_for_handles',
                 return_value={1: {'context': 'ctx'}}), \
             mock.patch.object(
                 replica_managers.reserved_capacity,
                 'parse_protocol_v2_cleanup_fence',
                 return_value=cleanup_fence), \
             mock.patch.object(
                 replica_managers.reserved_capacity,
                 'protocol_v2_provider_fence',
                 side_effect=_provider_fence), \
             mock.patch.object(
                 replica_managers.reserved_capacity,
                 'protocol_v2_provider_batch_fences',
                 side_effect=lambda *_args, **_kwargs: contextlib.nullcontext(
                     {})), \
             mock.patch.object(replica_managers.provider_phase,
                               '_PROVIDER_PHASE_GATE', fresh_gate), \
             mock.patch.object(manager, '_persist_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            probe_thread = self._start_capturing_thread(
                manager._probe_all_replicas, errors)
            self.assertTrue(http_entered.wait(timeout=5))
            try:
                with replica_managers.provider_phase.try_provider_phase(
                        replica_managers.provider_phase.ProviderPhaseMode.
                        AMBIENT_LEGACY):
                    pass
            finally:
                release_http.set()
                probe_thread.join(timeout=5)

        self.assertFalse(probe_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(resolver_admissions)
        self.assertTrue(
            all(admission.mode ==
                replica_managers.provider_phase.ProviderPhaseMode.V2_FENCED
                for admission in resolver_admissions))
        info._resolve_url.assert_called_once()

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

        provider_configs = [{
            'context': f'context-{info.replica_id}'
        } for info in infos]
        yaml_strings = [
            'provider:\n  context: context-' + str(info.replica_id) + '\n'
            for info in infos
        ]
        with mock.patch.object(
                replica_managers.global_user_state,
                'get_clusters_from_names',
                return_value=cluster_records) as get_clusters, \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_yaml_str_multiple',
                 return_value=yaml_strings) as get_yamls:
            urls = manager._resolve_probe_urls(infos)

        self.assertEqual(urls, {
            1: 'http://10.0.0.1:8080',
            2: 'http://10.0.0.2:8080',
        })
        get_clusters.assert_called_once_with(['svc-replica-1', 'svc-replica-2'])
        get_yamls.assert_called_once_with(
            ['/tmp/svc-replica-1.yaml', '/tmp/svc-replica-2.yaml'])
        for info, handle, provider_config in zip(infos, handles,
                                                 provider_configs):
            info._resolve_url.assert_called_once_with(
                cluster_record=cluster_records[info.cluster_name],
                handle=handle,
                provider_config=provider_config)

    def test_probe_url_resolution_dedupes_shared_cluster_yaml_reads(self):
        manager = self._make_manager()
        del manager._resolve_probe_urls
        infos = [_replica_info(1, True), _replica_info(2, True)]
        shared_yaml = '/tmp/shared.yaml'
        shared_provider = {'context': 'shared'}
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
                                   'get_cluster_yaml_str_multiple',
                                   return_value=[
                                       'provider:\n  context: shared\n'
                                   ]) as get_yamls:
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
                provider_config=shared_provider)

    def test_probe_reuses_supplied_url_without_resolving_again(self):
        info = object.__new__(replica_managers.ReplicaInfo)
        info.replica_id = 7
        response = mock.Mock(status_code=200)
        with mock.patch.object(replica_managers.replica_tls.requests,
                               'get',
                               return_value=response) as request:
            probed_info, ready, _ = info.probe(
                '/health', None, 15, None, resolved_url='http://10.0.0.7:8080')

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
                               return_value={
                                   info.replica_id: info for info in infos
                               }), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            manager._probe_all_replicas()

        self.assertEqual(mock_batch.call_count, 1)
        self.assertEqual(mock_batch.call_args.kwargs, {
            'expected_replica_exists': True,
            'guard_launch_exclusion': False,
        })
        written = mock_batch.call_args.args[1]
        self.assertEqual(sorted(rid for rid, _ in written), [1, 2])
        self.assertEqual(calls, ['batch'])
        manager._terminate_replica.assert_not_called()
        self.assertEqual(infos[1].status_property.sky_down_status,
                         replica_managers.common_utils.ProcessStatus.SCHEDULED)

    def test_enabled_mode_omits_stable_ordinary_rows(self):
        manager = self._make_manager()
        manager._changed_only_readiness_persistence = True
        manager._consecutive_failure_threshold_timeout.return_value = 1000
        ready = _replica_info(1, True)
        not_ready = _replica_info(2, False)
        not_ready.first_not_ready_time = 1.0
        not_ready.first_consecutive_failure_time = 1.0

        persist, snapshot = self._run_probe_round(manager, [ready, not_ready])

        persist.assert_not_called()
        self.assertEqual(snapshot, [ready, not_ready])

    def test_disabled_and_enabled_preemption_keep_separate_write_contracts(
            self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                manager = self._make_manager()
                manager._changed_only_readiness_persistence = enabled
                manager._is_interruptible_replica = mock.Mock(return_value=True)
                manager._cloud_instance_looks_alive.return_value = (
                    replica_managers._PreemptionPrefilterResult(
                        replica_managers._PreemptionPrefilterDisposition.
                        INTERRUPTED))
                info = _replica_info(1, False)

                persist, _ = self._run_probe_round(manager, [info])

                manager._apply_confirmed_preemption.assert_called_once_with(
                    info, None, persist_placement=False)
                self.assertEqual(persist.call_count, 1)
                persisted = persist.call_args.args[0]
                self.assertEqual(persisted, [(1, info)])
                self.assertEqual(
                    info.status_property.sky_down_status,
                    replica_managers.common_utils.ProcessStatus.SCHEDULED)

    def test_exact_kubernetes_absence_wave_schedules_in_one_probe_round(self):
        manager = self._make_manager()
        manager._is_interruptible_replica = mock.Mock(return_value=True)
        infos = [
            _replica_info(replica_id, False) for replica_id in range(1, 65)
        ]
        proofs = {
            info.replica_id: replica_managers._ExactKubernetesAbsenceProof(
                cleanup_fence=(
                    replica_managers.reserved_capacity.ProtocolV2CleanupFence(
                        kubernetes_context='ctx',
                        physical_cluster_uid='physical-uid')),
                cluster_name=info.cluster_name,
                replica_record_id=str(
                    uuid.UUID(int=info.replica_id))) for info in infos
        }
        for info in infos:
            info.replica_record_id = proofs[info.replica_id].replica_record_id

        manager._cloud_instance_looks_alive.side_effect = lambda info, **_: (
            replica_managers._PreemptionPrefilterResult(
                replica_managers._PreemptionPrefilterDisposition.
                EXACT_KUBERNETES_ABSENT, proofs[info.replica_id]))

        self._run_probe_round(manager, infos)

        self.assertEqual(manager._apply_confirmed_preemption.call_count, 64)

    def test_enabled_mode_persists_each_readiness_fingerprint_transition(self):
        cases = ({
            'name': 'false-to-true',
            'probe_result': True,
            'before': (False, 1.0, 1.0, None),
            'after': (True, 1.0, 1.0, None),
        }, {
            'name': 'true-to-false',
            'probe_result': False,
            'before': (True, 1.0, 1.0, 1.0),
            'after': (False, 1.0, 1.0, 1.0),
        }, {
            'name': 'first-ready',
            'probe_result': True,
            'before': (True, None, 1.0, None),
            'after': (True, 2.0, 1.0, None),
        }, {
            'name': 'first-not-ready',
            'probe_result': False,
            'before': (False, None, None, None),
            'after': (False, None, 2.0, None),
        }, {
            'name': 'failure-window-start',
            'probe_result': False,
            'before': (False, 1.0, 1.0, None),
            'after': (False, 1.0, 1.0, 2.0),
        }, {
            'name': 'failure-window-clear',
            'probe_result': True,
            'before': (True, 1.0, 1.0, 1.0),
            'after': (True, 1.0, 1.0, None),
        })
        for replica_id, case in enumerate(cases, start=1):
            with self.subTest(case=case['name']):
                manager = self._make_manager()
                manager._changed_only_readiness_persistence = True
                manager._consecutive_failure_threshold_timeout.return_value = (
                    1000)
                info = _replica_info(replica_id, case['probe_result'])
                (info.status_property.service_ready_now,
                 info.status_property.first_ready_time,
                 info.first_not_ready_time,
                 info.first_consecutive_failure_time) = case['before']

                persist, _ = self._run_probe_round(manager, [info])

                persist.assert_called_once_with([(replica_id, info)])
                self.assertEqual(
                    manager._readiness_persistence_fingerprint(info),
                    case['after'])

    def test_enabled_mode_persists_only_changed_ids_in_mixed_round(self):
        manager = self._make_manager()
        manager._changed_only_readiness_persistence = True
        manager._consecutive_failure_threshold_timeout.return_value = 1000
        stable_ready = _replica_info(1, True)
        stable_not_ready = _replica_info(2, False)
        stable_not_ready.first_not_ready_time = 1.0
        stable_not_ready.first_consecutive_failure_time = 1.0
        became_not_ready = _replica_info(3, False)
        became_not_ready.status_property.service_ready_now = True
        became_not_ready.first_not_ready_time = 1.0
        became_not_ready.first_consecutive_failure_time = 1.0
        first_failure = _replica_info(4, False)
        first_failure.status_property.first_ready_time = None

        persist, _ = self._run_probe_round(
            manager,
            [stable_ready, stable_not_ready, became_not_ready, first_failure])

        persist.assert_called_once()
        self.assertEqual(
            [replica_id for replica_id, _ in persist.call_args.args[0]], [3, 4])

    def test_nonordinary_before_or_after_state_forces_stable_write(self):
        for eligibility in ((False, True), (True, False)):
            with self.subTest(eligibility=eligibility):
                manager = self._make_manager()
                manager._changed_only_readiness_persistence = True
                manager._is_changed_only_readiness_persistence_eligible = (
                    mock.Mock(side_effect=eligibility))
                info = _replica_info(1, True)

                persist, _ = self._run_probe_round(manager, [info])

                persist.assert_called_once_with([(1, info)])

    def test_recovery_and_quarantine_rows_are_unconditional_writes(self):
        for state in ('candidate', 'capable', 'quarantined', 'active-recovery'):
            with self.subTest(state=state):
                manager = self._make_manager()
                manager._changed_only_readiness_persistence = True
                manager._consecutive_failure_threshold_timeout.return_value = (
                    1000)
                manager._system_recovery_route_registry = mock.Mock()
                manager._system_recovery_status_initialized = set()
                manager._suspend_system_recovery_route_if_unroutable = (
                    mock.Mock(return_value=None))
                manager._reconcile_system_recovery_status = mock.Mock(
                    return_value=False)
                info = _replica_info(1, state == 'active-recovery')
                if state == 'candidate':
                    info.system_recovery_disposition = (
                        system_recovery_state.SystemRecoveryDisposition.
                        CANDIDATE)
                    info.service_job_id = None
                    info.first_not_ready_time = 1.0
                    info.first_consecutive_failure_time = 1.0
                    manager._reduce_candidate_probe = mock.Mock(
                        return_value=(info, False, False, False))
                elif state == 'capable':
                    info.system_recovery_disposition = (
                        system_recovery_state.SystemRecoveryDisposition.CAPABLE)
                    info.system_recovery = mock.Mock(
                        state=system_recovery_state.ControllerRecoveryState.
                        ARMED)
                    info.first_not_ready_time = 1.0
                    info.first_consecutive_failure_time = 1.0
                    manager._reduce_capable_probe = mock.Mock(
                        return_value=(info,
                                      system_recovery_state.RecoveryReduction(
                                          state=info.system_recovery,
                                          changed=False,
                                          force_off_route=False,
                                          clear_probe_failure_window=False,
                                          mark_ready=False,
                                          schedule_legacy_teardown=False),
                                      False))
                elif state == 'quarantined':
                    info.system_recovery_quarantine = object()
                    info.first_not_ready_time = 1.0
                    info.first_consecutive_failure_time = 1.0
                else:
                    info.system_recovery = object()

                persist, _ = self._run_probe_round(manager, [info])

                persist.assert_called_once_with([(1, info)])

    def test_route_suspension_only_round_still_uses_persistence_path(self):
        manager = self._make_manager()
        manager._changed_only_readiness_persistence = True
        manager._consecutive_failure_threshold_timeout.return_value = 1000
        info = _replica_info(1, False)
        info.first_not_ready_time = 1.0
        info.first_consecutive_failure_time = 1.0
        suspension = object()
        manager._suspend_system_recovery_route_if_unroutable = mock.Mock(
            return_value=suspension)

        persist, _ = self._run_probe_round(manager, [info])

        persist.assert_called_once_with([], route_suspensions=[suspension])

    def test_route_suspension_only_persistence_requests_owner_fence(self):
        manager = self._make_manager()
        manager._service_hash = 'incarnation-a'
        manager._controller_owner = (100, '10.0.0.1')
        suspension = mock.Mock(replica_id=1)
        route_registry = mock.Mock()
        manager._route_lease_registry = mock.Mock(return_value=route_registry)

        with mock.patch.object(serve_state,
                               'add_or_update_replicas',
                               return_value=True) as persist:
            manager._persist_replicas(  # pylint: disable=unexpected-keyword-arg
                [],
                route_suspensions=[suspension])

        persist.assert_called_once_with('svc', [],
                                        expected_service_hash='incarnation-a',
                                        expected_controller_owner=(100,
                                                                   '10.0.0.1'),
                                        validate_fence_on_empty=True,
                                        expected_replica_exists=True,
                                        guard_launch_exclusion=False)
        route_registry.commit_suspension.assert_called_once_with(suspension)

    def test_initial_delay_sentinel_persists_before_teardown(self):
        manager = self._make_manager()
        manager._changed_only_readiness_persistence = True
        manager._get_initial_delay_seconds.return_value = 1
        info = _replica_info(1, False)
        info.status_property.first_ready_time = None
        info.first_not_ready_time = 0.0

        persist, _ = self._run_probe_round(manager, [info])

        persist.assert_called_once_with([(1, info)])
        self.assertEqual(info.status_property.first_ready_time, -1.0)
        self.assertEqual(info.status_property.sky_down_status,
                         replica_managers.common_utils.ProcessStatus.SCHEDULED)
        manager._terminate_replica.assert_not_called()

    def test_durable_failure_threshold_tears_down_without_rewriting_row(self):
        manager = self._make_manager()
        manager._changed_only_readiness_persistence = True
        manager._consecutive_failure_threshold_timeout.return_value = 1
        info = _replica_info(1, False)
        info.first_not_ready_time = 0.0
        info.first_consecutive_failure_time = 0.0

        persist, _ = self._run_probe_round(manager, [info])

        persist.assert_called_once_with([(1, info)])
        self.assertEqual(info.status_property.sky_down_status,
                         replica_managers.common_utils.ProcessStatus.SCHEDULED)
        manager._terminate_replica.assert_not_called()

    def test_fingerprint_and_ordinary_assignment_surface_stay_exact(self):
        info = _replica_info(1, True)
        info.status_property.service_ready_now = False
        info.status_property.first_ready_time = 3.0
        info.first_not_ready_time = 4.0
        info.first_consecutive_failure_time = 5.0
        self.assertEqual(
            replica_managers.SkyPilotReplicaManager.
            _readiness_persistence_fingerprint(info), (False, 3.0, 4.0, 5.0))

        assignments = set()
        for method in (
                replica_managers.SkyPilotReplicaManager._probe_all_replicas,
                replica_managers.SkyPilotReplicaManager.
                _probe_all_replicas_with_snapshot):
            source = textwrap.dedent(inspect.getsource(inspect.unwrap(method)))
            for node in ast.walk(ast.parse(source)):
                if not isinstance(node, ast.Assign):
                    continue
                for target in node.targets:
                    target_source = ast.unparse(target)
                    if target_source.startswith('info.'):
                        assignments.add(target_source)
        self.assertEqual(
            assignments, {
                'info.status_property.service_ready_now',
                'info.status_property.first_ready_time',
                'info.first_not_ready_time',
                'info.first_consecutive_failure_time',
            })

    def test_changed_only_eligibility_is_fail_closed_for_recovery_state(self):
        eligible = (replica_managers.SkyPilotReplicaManager.
                    _is_changed_only_readiness_persistence_eligible)
        info = _replica_info(1, True)
        info.system_recovery_launch_intent = object()
        self.assertTrue(eligible(info))

        for mutation in ('candidate', 'capable', 'quarantine', 'recovery'):
            with self.subTest(mutation=mutation):
                info = _replica_info(1, True)
                if mutation == 'candidate':
                    info.system_recovery_disposition = (
                        system_recovery_state.SystemRecoveryDisposition.
                        CANDIDATE)
                elif mutation == 'capable':
                    info.system_recovery_disposition = (
                        system_recovery_state.SystemRecoveryDisposition.CAPABLE)
                elif mutation == 'quarantine':
                    info.system_recovery_quarantine = object()
                else:
                    info.system_recovery = object()
                self.assertFalse(eligible(info))

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

    def test_missing_version_defers_only_matching_replica(self):
        manager = self._make_manager()
        missing = _replica_info(1, True)
        missing.version = 7
        healthy = _replica_info(2, True)
        healthy.version = 1

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=[missing, healthy]), \
             mock.patch.object(serve_state, 'get_specs',
                               return_value={1: mock.Mock(), 7: None}), \
             mock.patch.object(serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: missing, 2: healthy}), \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            manager._probe_all_replicas()

        missing.probe.assert_not_called()
        healthy.probe.assert_called_once()

    def test_corrupt_version_spec_defers_only_matching_replica(self):
        manager = self._make_manager()
        corrupt = _replica_info(1, True)
        corrupt.version = 7
        healthy = _replica_info(2, True)
        healthy.version = 1
        healthy_spec = mock.Mock()

        def _get_spec(_service_name, version):
            if version == 7:
                raise ValueError('corrupt pickle')
            return healthy_spec

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=[corrupt, healthy]), \
             mock.patch.object(serve_state,
                               'get_specs',
                               side_effect=ValueError('batch decode failed')), \
             mock.patch.object(serve_state,
                               'get_spec',
                               side_effect=_get_spec), \
             mock.patch.object(serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: corrupt, 2: healthy}), \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            manager._probe_all_replicas()

        corrupt.probe.assert_not_called()
        healthy.probe.assert_called_once()
        self.assertEqual(manager._tick_version_spec_cache, {1: healthy_spec})

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
        info.probe_pool.assert_called_once_with(
            provider_phase_admission=mock.ANY)

    def test_probe_round_returns_final_full_fleet_snapshot(self):
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
                 return_value={tracked.replica_id: tracked}), \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            snapshot = manager._probe_all_replicas()

        # The opening read defines probe work; the closing read is the
        # publication snapshot and covers untracked/concurrently-added rows.
        self.assertEqual(full_read.call_count, 2)
        self.assertEqual(snapshot, [tracked, untracked])

    def test_probe_round_final_read_reflects_deleted_rows(self):
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
        with mock.patch.object(serve_state,
                               'get_replica_infos',
                               side_effect=[infos,
                                            [infos[0], refreshed_2]]) as full_read, \
             mock.patch.object(serve_state, 'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(
                 serve_state, 'get_replica_infos_from_ids',
                 return_value={info.replica_id: info
                               for info in infos}) as keyed_read, \
             mock.patch.object(serve_state, 'add_or_update_replicas'), \
             mock.patch.object(serve_state, 'set_service_uptime'):
            snapshot = manager._probe_all_replicas()

        keyed_read.assert_called_once_with('svc', [1, 2, 3])
        self.assertEqual(full_read.call_count, 2)
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
             mock.patch.object(manager._manager_daemon_stop,
                               'wait', side_effect=_StopLoop) as daemon_wait:
            manager._get_endpoint_probe_interval_seconds = mock.Mock(
                return_value=10)
            with self.assertRaises(_StopLoop):
                manager._replica_prober()

        # The tick uses the probe round's closing publication snapshot rather
        # than performing a third read for status publication.
        self.assertEqual(full_read.call_count, 2)
        status_update.assert_called_once_with('svc', [info],
                                              manager._update_mode,
                                              target_num_replicas=None)
        daemon_wait.assert_called_once_with(10)

    def test_no_tracked_replicas_skips_probe_round_work(self):
        manager = self._make_manager()
        info = _replica_info(1, True)
        info.status_property.should_track_service_status.return_value = False

        with mock.patch.object(serve_state, 'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(serve_state, 'get_specs') as mock_get_specs, \
             mock.patch.object(serve_state,
                               'add_or_update_replicas') as persist, \
             mock.patch.object(manager,
                               '_get_readiness_executor') as get_executor:
            manager._probe_all_replicas()

        mock_get_specs.assert_not_called()
        persist.assert_not_called()
        get_executor.assert_not_called()
