"""Tests for sky/serve/replica_managers.py.

Covers:
- `SkyPilotReplicaManager.__init__` startup ordering: the daemon threads
  (especially `_job_status_fetcher`) must NOT race the main thread for
  `self.lock` before `_recover_replica_operations` runs.
- `launch_cluster` retry scoping: resource availability (capacity) failures
  are capped by `availability_max_retry` while other errors keep the
  `max_retry` in-place attempts.
- `_launch_replica` passing `availability_max_retry=1` only for spot
  replicas managed by a spot placer.
"""
# pylint: disable=protected-access,import-outside-toplevel,reimported
# pylint: disable=unused-argument,invalid-name,line-too-long
# pylint: disable=missing-class-docstring,unnecessary-dunder-call
import asyncio
import collections.abc
import contextlib
import copy
import dataclasses
import functools
import json
import logging
import math
import os
import queue
import threading
import time
import types
from unittest import mock
import uuid

import pytest
from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky import clouds
from sky import exceptions
from sky import skypilot_config
from sky.provision import common as provision_common
from sky.serve import capacity_admission
from sky.serve import ordinary_launch_binding
from sky.serve import ordinary_launch_handoff
from sky.serve import paid_capacity
from sky.serve import placement_policy
from sky.serve import replica_managers
from sky.serve import route_projection
from sky.serve import serve_utils
from sky.serve import service_spec
from sky.serve import system_recovery_route_lease
from sky.serve import system_recovery_state
from sky.serve import system_recovery_state as recovery_state
from sky.server.requests import postgres as request_postgres
from sky.server.requests import requests as api_requests
from sky.skylet import job_lib
from sky.utils import common_utils
from sky.utils import config_utils
from sky.utils import context as sky_context
from sky.utils import controller_utils
from sky.utils import thread_utils

_DISABLED_PLACEMENT_CONTRACT = placement_policy.resolve_fresh_contract(
    None, pool=False)
_LOGICAL_PLACEMENT_CONTRACT = placement_policy.resolve_fresh_contract(
    placement_policy.CAPACITY_AWARE_SPOT_PLACER, pool=False)


def _binding_authority(mode=ordinary_launch_binding.BindingMode.LEGACY,
                       binding_epoch=1,
                       *,
                       generic=False):
    return ordinary_launch_binding.ControllerBindingAuthority(
        service_name='svc',
        service_hash='hash',
        service_workspace='default',
        service_lifecycle_epoch=1,
        controller_pid=123,
        controller_ip='127.0.0.1',
        controller_incarnation=uuid.UUID(
            '00000000-0000-4000-8000-000000000123'),
        controller_owner_epoch=7,
        capable=True,
        binding_mode=mode,
        binding_epoch=binding_epoch,
        non_pool_capable=generic,
        non_pool_binding_protocol_version=(
            ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION
            if generic else None),
        non_pool_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()
            if generic else None),
        non_pool_capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH
            if generic else None),
        non_pool_receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION
            if generic else None))


def _bound_non_pool_context(
) -> ordinary_launch_binding.BoundNonPoolLaunchContext:
    profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
        ordinary_launch_binding.NonPoolLaunchProfileKind.ORDINARY_PAID,
        authorization_reference='paid:test',
        authorization_generation=1,
        authorization_payload={'pool': 'paid'})
    return ordinary_launch_binding.BoundNonPoolLaunchContext(
        association_id=uuid.UUID('11111111-1111-4111-8111-111111111111'),
        request_id='request-1',
        service_name='svc',
        replica_id=3,
        replica_record_id=uuid.UUID('22222222-2222-4222-8222-222222222222'),
        launch_generation=1,
        input_digest='a' * 64,
        profile=profile,
        capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
        capability_profile_set_digest=(
            ordinary_launch_binding.supported_non_pool_profile_set_digest()),
        receipt_protocol_version=(
            ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION))


def test_non_pool_provider_reconciliation_is_scheduled_without_inline_io():
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    authority = _binding_authority(ordinary_launch_binding.BindingMode.BOUND,
                                   binding_epoch=2,
                                   generic=True)
    manager._ordinary_launch_binding_authority = authority
    info = types.SimpleNamespace(replica_id=3)
    context = _bound_non_pool_context()
    worker = mock.Mock()

    with mock.patch.object(replica_managers.thread_utils,
                           'SafeThread',
                           return_value=worker) as thread_constructor:
        manager._schedule_non_pool_provider_reconciliation(info, context)

    thread_constructor.assert_called_once()
    worker_args = thread_constructor.call_args.kwargs
    assert worker_args['target'] is (
        replica_managers.non_pool_launch_reconciliation.reconcile)
    assert worker_args['name'] == 'replica-3-provider-reconciliation'
    assert worker_args['daemon'] is True
    assert worker_args['args'][:3] == (context, info, authority)
    assert callable(worker_args['args'][3])
    worker.start.assert_called_once_with()


def _physical_service_spec_mock() -> mock.Mock:
    return mock.Mock(spot_placer=None,
                     placement_contract=_DISABLED_PLACEMENT_CONTRACT)


def _canonical_paid_pool_key(region='us-east-1'):
    location = make_location(region,
                             accelerators={'L4': 1},
                             use_spot=True,
                             cloud_name='AWS',
                             instance_type='g6.xlarge')
    return paid_capacity.pool_key(location, workspace='w', num_nodes=1)


def test_replica_manager_rejects_legacy_service_without_workspace():
    with mock.patch.object(replica_managers.serve_state,
                           'get_service_from_name',
                           return_value={
                               'workspace': None,
                               'hash': 'incarnation-a'
                           }), \
         mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[]), \
         mock.patch.object(replica_managers.serve_utils.global_user_state,
                           'get_clusters_from_names',
                           return_value={}), \
         pytest.raises(RuntimeError, match='durable workspace'):
        replica_managers.ReplicaManager('legacy-service',
                                        mock.MagicMock(),
                                        version=1)


def test_action_aware_manager_snapshots_the_current_lifecycle_fence():
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager._service_hash = 'incarnation-a'
    manager._controller_owner = (123, '10.0.0.1')
    current_owner = {
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'lifecycle_epoch': 17,
        'status': replica_managers.serve_state.ServiceStatus.READY,
    }

    with mock.patch.object(replica_managers.serve_state,
                           'get_service_controller_owner',
                           return_value=current_owner):
        assert manager._resource_action_fence_kwargs() == {
            'expected_controller_owner': (123, '10.0.0.1'),
            'expected_lifecycle_epoch': 17,
        }
    assert manager._db_fence_kwargs() == {
        'expected_service_hash': 'incarnation-a',
        'expected_controller_owner': (123, '10.0.0.1'),
    }


def test_legacy_replica_writes_omit_an_unknown_lifecycle_epoch():
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_hash = None
    manager._controller_owner = None

    assert manager._resource_action_fence_kwargs() is None
    assert manager._db_fence_kwargs() == {}


@pytest.mark.parametrize('epoch', [None, 0, -1, True, '17'])
def test_action_fence_rejects_an_invalid_current_epoch(epoch):
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager._service_hash = 'incarnation-a'
    manager._controller_owner = (123, '10.0.0.1')
    owner = {
        'hash': 'incarnation-a',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'lifecycle_epoch': epoch,
        'status': replica_managers.serve_state.ServiceStatus.READY,
    }

    with mock.patch.object(replica_managers.serve_state,
                           'get_service_controller_owner',
                           return_value=owner):
        assert manager._resource_action_fence_kwargs() is None


def test_action_fence_uses_a_new_epoch_after_a_same_owner_update():
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager._service_hash = 'incarnation-a'
    manager._controller_owner = (123, '10.0.0.1')

    def _owner(epoch):
        return {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'lifecycle_epoch': epoch,
            'status': replica_managers.serve_state.ServiceStatus.READY,
        }

    with mock.patch.object(replica_managers.serve_state,
                           'get_service_controller_owner',
                           side_effect=[_owner(17), _owner(18)]):
        first = manager._resource_action_fence_kwargs()
        second = manager._resource_action_fence_kwargs()

    assert first is not None
    assert second is not None
    assert first['expected_lifecycle_epoch'] == 17
    assert second['expected_lifecycle_epoch'] == 18


def test_action_fence_defers_when_the_current_owner_is_unavailable():
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager._service_hash = 'incarnation-a'
    manager._controller_owner = (123, '10.0.0.1')

    with mock.patch.object(replica_managers.serve_state,
                           'get_service_controller_owner',
                           side_effect=RuntimeError('database restarting')):
        assert manager._resource_action_fence_kwargs() is None


class TestSkyPilotReplicaManagerInitOrdering:
    """`SkyPilotReplicaManager.__init__` must (1) hand the manager lock to
    the recovery pass BEFORE any daemon thread can grab it — otherwise
    `_job_status_fetcher`'s per-replica SSH walk can starve recovery — and
    (2) NOT block on recovery finishing: at fleet scale recovery re-drives
    hundreds of interrupted launches and runs for minutes, and a blocking
    __init__ kept uvicorn from binding within _start's 60s readiness window
    (`_bail_on_boot_failure` -> os._exit -> daemon respawn -> recovery from
    scratch: a controller crash-loop, observed live at ~860 rows / ~520
    interrupted launches)."""

    def _build(self,
               recovery_body,
               started_records,
               resource_scope=None,
               supervisor_calls=None):
        import threading as threading_mod

        with mock.patch.object(
                replica_managers.ReplicaManager, '__init__',
                lambda self_, service_name, spec, version: None), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_yaml_content',
                 return_value='dummy: yaml'), \
             mock.patch(
                 'sky.serve.replica_managers.load_task_with_service_spec',
                 return_value=mock.MagicMock()), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.'
                 'get_placement_catalog',
                 return_value={}), \
             mock.patch(
                 'sky.serve.replica_managers.spot_placer.SpotPlacer.from_task',
                 return_value=None), \
             mock.patch.object(
                 replica_managers.SkyPilotReplicaManager,
                 '_recover_replica_operations', recovery_body), \
             mock.patch(
                 'sky.serve.replica_managers.thread_utils.'
                 'start_supervised_thread') as mock_supervised:

            def _record(target, *_args, **_kwargs):
                started_records.append(getattr(target, '__name__',
                                               repr(target)))
                if supervisor_calls is not None:
                    supervisor_calls.append((target, _kwargs))
                return mock.Mock()

            mock_supervised.side_effect = _record

            # Base __init__ is stubbed; provide the attrs it would set.
            def _patched_base_init(self_, service_name, spec, version):
                self_.lock = threading_mod.Lock()
                self_._service_name = service_name
                self_._next_replica_id = 1
                self_._uptime = None
                self_.latest_version = version
                self_._update_mode = None
                self_._is_pool = False
                self_._resource_scope = None

            with mock.patch.object(replica_managers.ReplicaManager, '__init__',
                                   _patched_base_init):
                mgr = replica_managers.SkyPilotReplicaManager(
                    service_name='svc',
                    spec=mock.MagicMock(),
                    version=1,
                    resource_scope=resource_scope)
            return mgr

    def test_incarnation_scope_survives_base_initialization(self):
        mgr = self._build(lambda self_: None, [], 'incarnation-a')
        assert mgr._resource_scope == 'incarnation-a'

    def test_lock_is_held_by_recovery_when_daemons_start(self):
        import threading as threading_mod
        release = threading_mod.Event()
        lock_state_at_daemon_start = []

        def _slow_recovery(self_):
            release.wait(timeout=10)

        started = []
        mgr = self._build(_slow_recovery, started)
        # __init__ returned while recovery is still running (non-blocking
        # boot), and the manager lock was already held when it returned —
        # so any daemon started afterwards cannot win it.
        assert mgr.lock.locked() is True
        assert len(started) == 5
        lock_state_at_daemon_start.append(mgr.lock.locked())
        release.set()
        # Recovery finishes and releases the lock.
        for _ in range(100):
            if not mgr.lock.locked():
                break
            import time as time_mod
            time_mod.sleep(0.05)
        assert mgr.lock.locked() is False

    def test_failed_recovery_releases_manager_lock_during_backoff(self):
        import threading as threading_mod

        manager_ref = []
        backoff_lock_states = []
        backoff_observed = threading_mod.Event()
        original_wait = threading_mod.Event.wait

        def _failing_recovery(self_):
            manager_ref.append(self_)
            raise RuntimeError('one exact association is unavailable')

        def _observe_wait(event, timeout=None):
            if timeout == 30 and manager_ref:
                backoff_lock_states.append(manager_ref[0].lock.locked())
                backoff_observed.set()
                # End the background recovery thread after observing its first
                # backoff; no wall-clock sleep is needed in this unit test.
                return True
            return original_wait(event, timeout)

        with mock.patch.object(threading_mod.Event,
                               'wait',
                               autospec=True,
                               side_effect=_observe_wait):
            mgr = self._build(_failing_recovery, [])
            assert original_wait(backoff_observed, 5)

        assert backoff_lock_states == [False]
        assert mgr.lock.locked() is False

    def test_all_daemon_threads_are_started(self):
        started = []
        self._build(lambda self_: None, started)
        assert '_thread_pool_refresher' in started
        assert '_job_status_fetcher' in started
        assert '_replica_prober' in started
        assert '_system_recovery_route_prober' in started
        assert '_zero_cost_actuation_dispatcher' in started

    def test_all_daemon_threads_share_manager_stop_event(self):
        calls = []
        mgr = self._build(lambda self_: None, [], supervisor_calls=calls)

        assert len(calls) == 5
        assert all(kwargs['stop_event'] is mgr._manager_daemon_stop
                   for _, kwargs in calls)

    def test_legacy_per_gpu_yaml_uses_persisted_physical_semantics(self):
        legacy_yaml = """
resources:
  cpus: 1
  ports: 8080
  accelerators: A100:1
  use_spot: true
service:
  readiness_probe: /health
  replica_policy:
    min_replicas: 1
    max_replicas: 8
    target_concurrency_per_replica: 2
    spot_placer: dynamic_fallback_per_gpu
run: echo hi
"""
        # This committed YAML predates implicit logical replicas and is
        # intentionally invalid under the current service policy because it
        # lacks the required async-occupancy signal. Recovery must load
        # resources around the persisted physical spec rather than applying
        # today's hidden default to historical state.
        with pytest.raises(ValueError,
                           match='graceful_drain_async_occupancy: true'):
            replica_managers.task_lib.Task.from_yaml_str(legacy_yaml)

        persisted_spec = mock.MagicMock()
        persisted_spec.pool = False
        persisted_spec.uses_logical_replicas = False
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_service_from_name',
                return_value={'workspace': 'default'}), \
             mock.patch(
                'sky.serve.replica_managers.serve_state.get_yaml_content',
                return_value=legacy_yaml), \
             mock.patch(
                 'sky.serve.replica_managers.task_lib.Task.from_yaml_str',
                 side_effect=AssertionError('must not reparse service')), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.'
                 'get_placement_catalog',
                 return_value={}), \
             mock.patch(
                 'sky.serve.replica_managers.spot_placer.SpotPlacer.from_task',
                 return_value=None), \
             mock.patch.object(
                 replica_managers.SkyPilotReplicaManager,
                 '_recover_replica_operations'), \
             mock.patch(
                 'sky.serve.replica_managers.thread_utils.'
                 'start_supervised_thread'):
            manager = replica_managers.SkyPilotReplicaManager(
                service_name='svc', spec=persisted_spec, version=7)

        assert manager._uses_logical_replicas is False
        assert manager._version_specs == {7: persisted_spec}
        assert manager._default_planned_capacity == 1


class TestBackgroundDutyOwnershipLifecycle:

    @staticmethod
    def _stopped_manager():
        mgr = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        mgr._ownership_lost = threading.Event()
        mgr._ownership_lost.set()
        return mgr

    @pytest.mark.parametrize(('loop_name', 'work_name'), [
        ('_thread_pool_refresher', '_refresh_thread_pool'),
        ('_job_status_fetcher', '_fetch_job_status'),
    ])
    def test_stopped_duty_does_not_start_new_round(self, loop_name, work_name):
        mgr = self._stopped_manager()
        work = mock.Mock()
        setattr(mgr, work_name, work)

        with mock.patch.object(
                replica_managers.time,
                'sleep',
                side_effect=AssertionError('stale duty tried to sleep')):
            getattr(mgr, loop_name)()

        work.assert_not_called()

    def test_stopped_prober_does_not_start_new_round(self):
        mgr = self._stopped_manager()
        mgr._probe_all_replicas = mock.Mock()
        mgr._service_name = 'svc'
        mgr._update_mode = None
        mgr._tick_version_spec_cache = {}
        mgr._db_fence_kwargs = mock.Mock(return_value={})
        mgr._get_endpoint_probe_interval_seconds = mock.Mock(return_value=1)

        with mock.patch.object(
                replica_managers.serve_utils,
                'set_service_status_and_active_versions_from_replica'), \
             mock.patch.object(
                replica_managers.time,
                'sleep',
                side_effect=AssertionError('stale prober tried to sleep')):
            mgr._replica_prober()

        mgr._probe_all_replicas.assert_not_called()

    def test_ownership_loss_after_probe_skips_interval_spec_read(self):
        mgr = self._stopped_manager()
        mgr._ownership_lost = threading.Event()
        mgr._manager_daemon_stop = threading.Event()
        mgr._update_recovery_required = False
        mgr._status_epoch_lock = threading.Lock()
        mgr._status_epoch_generation = 0
        mgr._tick_version_spec_cache = {}
        mgr._service_name = 'svc'
        mgr._update_mode = None
        mgr._db_fence_kwargs = mock.Mock(return_value={})

        def _finish_probe_after_ownership_loss():
            mgr._ownership_lost.set()
            return []

        mgr._probe_all_replicas = mock.Mock(
            side_effect=_finish_probe_after_ownership_loss)
        interval_lookup = mock.Mock(
            side_effect=AssertionError('stale prober read interval spec'))
        mgr._get_endpoint_probe_interval_seconds = interval_lookup

        with mock.patch.object(
                replica_managers.serve_utils,
                'set_service_status_and_active_versions_from_replica'
        ) as status_write:
            mgr._replica_prober()

        mgr._probe_all_replicas.assert_called_once_with()
        status_write.assert_not_called()
        interval_lookup.assert_not_called()
        assert mgr._manager_daemon_stop.is_set()

    def test_delayed_job_result_cannot_mutate_after_update_recovery_fence(self):
        mgr = _make_manager()
        mgr._ownership_lost = threading.Event()
        mgr._manager_daemon_stop = threading.Event()
        mgr._update_recovery_required = False
        mgr._is_pool = False
        info = mock.Mock()
        info.replica_id = 7
        info.system_recovery_disposition = (
            recovery_state.SystemRecoveryDisposition.ORDINARY)
        result_started = threading.Event()
        release_result = threading.Event()

        class _DelayedFailedResult:

            def get(self):
                result_started.set()
                assert release_result.wait(timeout=5)
                return {1: job_lib.JobStatus.FAILED}

        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()
        reducer = threading.Thread(target=mgr._handle_job_status_results,
                                   args=([(info, _DelayedFailedResult())],))
        reducer.start()
        assert result_started.wait(timeout=5)

        # Simulate delayed SIGTERM: the child remains alive after the partial
        # runtime transition, but its manager fence is already irreversible.
        mgr.fence_launches_for_update_recovery()
        assert mgr._manager_daemon_stop.is_set()
        assert not mgr._ownership_lost.is_set()
        release_result.set()
        reducer.join(timeout=5)

        assert not reducer.is_alive()
        mgr._persist_replica.assert_not_called()
        mgr._terminate_replica.assert_not_called()

    def test_update_recovery_fence_guards_direct_termination(self):
        mgr = _make_manager()
        mgr._ownership_lost = threading.Event()
        mgr._manager_daemon_stop = threading.Event()
        mgr.fence_launches_for_update_recovery()

        with mock.patch.object(
                mgr,
                '_legacy_mutation_runtime_state',
                side_effect=AssertionError('fenced termination reached state')):
            mgr._terminate_replica(7,
                                   sync_down_logs=True,
                                   replica_drain_delay_seconds=0)

        assert not mgr._ownership_lost.is_set()

    def test_delayed_probe_result_cannot_reduce_after_update_recovery_fence(
            self):
        mgr = _make_manager()
        mgr._ownership_lost = threading.Event()
        mgr._manager_daemon_stop = threading.Event()
        mgr._update_recovery_required = False
        mgr._is_pool = True
        mgr._uptime = None
        probe_started = threading.Event()
        release_probe = threading.Event()
        info = mock.Mock()
        info.replica_id = 9
        info.cluster_name = 'svc-9'
        info.status_property.should_track_service_status.return_value = True

        def _delayed_probe_pool(**_kwargs):
            probe_started.set()
            assert release_probe.wait(timeout=5)
            return info, True, 123.0

        info.probe_pool.side_effect = _delayed_probe_pool
        mgr._persist_replicas = mock.Mock()
        mgr._terminate_replica = mock.Mock()
        result = []

        def _run_probe():
            result.append(
                mgr._probe_all_replicas_with_snapshot(
                    [info], phase_admission=mock.sentinel.phase_admission))

        probe_thread = threading.Thread(target=_run_probe)
        with mock.patch.object(replica_managers.backends,
                               'CloudVmRayBackend'), \
             mock.patch.object(replica_managers.serve_state,
                               'set_service_uptime') as set_uptime:
            probe_thread.start()
            assert probe_started.wait(timeout=5)
            mgr.fence_launches_for_update_recovery()
            release_probe.set()
            probe_thread.join(timeout=5)

        assert not probe_thread.is_alive()
        assert result == [[info]]
        set_uptime.assert_not_called()
        mgr._persist_replicas.assert_not_called()
        mgr._terminate_replica.assert_not_called()
        assert not mgr._ownership_lost.is_set()

    def test_update_recovery_fence_blocks_route_issuance(self):
        mgr = _make_manager()
        mgr._ownership_lost = threading.Event()
        mgr._manager_daemon_stop = threading.Event()
        registry = mock.Mock()
        mgr._system_recovery_route_registry = registry
        mgr.fence_launches_for_update_recovery()

        issued = mgr._issue_system_recovery_route(mock.Mock(), 'http://replica',
                                                  1.0, None)

        assert not issued
        registry.issue.assert_not_called()

    def test_prober_uses_authoritative_autoscaler_target_for_status(self):
        mgr = self._stopped_manager()
        mgr._ownership_lost = threading.Event()
        mgr._manager_daemon_stop = mock.Mock(spec=threading.Event)
        mgr._manager_daemon_stop.is_set.return_value = False
        mgr._manager_daemon_stop.wait.return_value = True
        mgr._probe_all_replicas = mock.Mock(return_value=[])
        mgr._target_num_replicas_lock = threading.Lock()
        mgr._target_num_replicas = 0
        mgr._target_num_replicas_generation = 0
        mgr._status_epoch_lock = threading.Lock()
        mgr._status_epoch_generation = 0
        mgr.latest_version = 1
        mgr._service_name = 'svc'
        mgr._update_mode = serve_utils.UpdateMode.ROLLING
        mgr._tick_version_spec_cache = {}
        mgr._db_fence_kwargs = mock.Mock(return_value={})
        mgr._get_endpoint_probe_interval_seconds = mock.Mock(return_value=1)

        with mock.patch.object(
                replica_managers.serve_utils,
                'set_service_status_and_active_versions_from_replica'
        ) as set_st:
            mgr._replica_prober()

        set_st.assert_called_once_with('svc', [],
                                       serve_utils.UpdateMode.ROLLING,
                                       target_num_replicas=0)

    @pytest.mark.parametrize(('complete', 'expected_calls'), [(True, 1),
                                                              (False, 0)])
    def test_prober_publishes_only_the_same_complete_probe_snapshot(
            self, complete, expected_calls):
        mgr = self._stopped_manager()
        mgr._ownership_lost = threading.Event()
        mgr._manager_daemon_stop = mock.Mock(spec=threading.Event)
        mgr._manager_daemon_stop.is_set.return_value = False
        mgr._manager_daemon_stop.wait.return_value = True
        mgr._status_epoch_lock = threading.Lock()
        mgr._status_epoch_generation = 0
        mgr._tick_version_spec_cache = {}
        mgr._get_endpoint_probe_interval_seconds = mock.Mock(return_value=1)
        snapshot = []
        route_result = replica_managers.ProbeRouteResult(
            replica_infos=snapshot,
            resolved_routes={},
            identity_verified_replica_ids=set(),
            complete=complete)

        def _probe():
            mgr._last_probe_route_result = route_result
            return snapshot

        mgr._probe_all_replicas = mock.Mock(side_effect=_probe)
        mgr._set_service_status_from_replica_infos = mock.Mock()
        publisher = mock.Mock()
        mgr._route_projection_publisher = publisher

        mgr._replica_prober()

        mgr._set_service_status_from_replica_infos.assert_called_once_with(
            snapshot, expected_status_epoch=0)
        assert publisher.call_count == expected_calls
        if complete:
            publisher.assert_called_once_with(route_result)

    def test_provider_resolution_writes_material_without_publication(self):
        mgr = self._stopped_manager()
        writer = mock.Mock()
        publisher = mock.Mock()
        mgr._route_material_writer = writer
        mgr._route_projection_publisher = publisher
        mgr._get_version_spec = mock.Mock(return_value=types.SimpleNamespace(
            readiness_path='/health',
            readiness_timeout_seconds=15,
            post_data=None,
            readiness_headers={'X-Probe': 'serve'},
            graceful_drain_async_occupancy=True,
            uses_logical_replicas=False))
        mgr.system_recovery_allows_routing = mock.Mock(return_value=True)
        mgr.system_recovery_route_marker = mock.Mock(return_value=None)
        info = types.SimpleNamespace(
            replica_id=1,
            replica_record_id=('00000000-0000-4000-8000-000000000001'),
            version=2,
            is_zero_cost=False,
            planned_capacity=3,
            system_recovery_disposition=(
                system_recovery_state.SystemRecoveryDisposition.ORDINARY))
        resolved = route_projection.ResolvedRouteMaterial(
            'http://10.0.0.1:8000', 'L4', 1)

        mgr._write_resolved_route_materials([info], {1: resolved})

        writer.assert_called_once()
        entries = writer.call_args.args[0]
        assert len(entries) == 1
        assert entries[0][0] is info
        material = entries[0][1]
        assert material.route == resolved
        assert material.readiness_path == '/health'
        assert material.planned_capacity == 3
        publisher.assert_not_called()

    def test_autoscaler_target_publication_is_version_fenced(self):
        mgr = replica_managers.ReplicaManager.__new__(
            replica_managers.ReplicaManager)
        mgr.lock = threading.Lock()
        mgr._target_num_replicas_lock = threading.Lock()
        mgr.latest_version = 2
        mgr._target_num_replicas = None
        mgr._target_num_replicas_generation = 0

        assert not mgr.publish_target_num_replicas(0, expected_version=1)
        assert mgr.get_target_num_replicas() is None
        assert mgr.publish_target_num_replicas(0, expected_version=2)
        assert mgr.get_target_num_replicas() == 0

        with pytest.raises(ValueError, match='nonnegative integer'):
            mgr.publish_target_num_replicas(-1, expected_version=2)
        with pytest.raises(ValueError, match='nonnegative integer'):
            mgr.publish_target_num_replicas(True, expected_version=2)

    def test_status_write_serializes_version_transition(self):
        mgr = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        mgr._target_num_replicas_lock = threading.Lock()
        mgr._target_num_replicas = 0
        mgr._target_num_replicas_generation = 0
        mgr._status_epoch_lock = threading.Lock()
        mgr._status_epoch_generation = 0
        mgr.latest_version = 1
        mgr._service_name = 'svc'
        mgr._update_mode = serve_utils.UpdateMode.ROLLING
        mgr._db_fence_kwargs = mock.Mock(return_value={})
        status_write_entered = threading.Event()
        release_status_write = threading.Event()
        version_transition_started = threading.Event()
        version_transitioned = threading.Event()

        def _write_status(*args, **kwargs):
            del args, kwargs
            status_write_entered.set()
            assert release_status_write.wait(timeout=5)

        def _transition_version():
            version_transition_started.set()
            mgr._transition_status_epoch_for_version(
                2, serve_utils.UpdateMode.BLUE_GREEN)
            version_transitioned.set()

        writer = threading.Thread(
            target=mgr._set_service_status_from_replica_infos, args=([],))
        transition = threading.Thread(target=_transition_version)
        with mock.patch.object(
                replica_managers.serve_utils,
                'set_service_status_and_active_versions_from_replica',
                side_effect=_write_status) as set_st:
            writer.start()
            assert status_write_entered.wait(timeout=5)
            transition.start()
            assert version_transition_started.wait(timeout=5)
            assert not version_transitioned.wait(timeout=0.1)
            release_status_write.set()
            writer.join(timeout=5)
            transition.join(timeout=5)

        assert not writer.is_alive()
        assert not transition.is_alive()
        set_st.assert_called_once_with('svc', [],
                                       serve_utils.UpdateMode.ROLLING,
                                       target_num_replicas=0)
        assert mgr.latest_version == 2
        assert mgr._update_mode == serve_utils.UpdateMode.BLUE_GREEN
        assert mgr.get_target_num_replicas() is None

    def test_prober_discards_snapshot_when_status_epoch_changes(self):
        mgr = self._stopped_manager()
        mgr._ownership_lost = threading.Event()
        mgr._manager_daemon_stop = mock.Mock(spec=threading.Event)
        mgr._manager_daemon_stop.is_set.return_value = False
        mgr._manager_daemon_stop.wait.return_value = True
        mgr._target_num_replicas_lock = threading.Lock()
        mgr._target_num_replicas = 0
        mgr._target_num_replicas_generation = 0
        mgr._status_epoch_lock = threading.Lock()
        mgr._status_epoch_generation = 0
        mgr.latest_version = 1
        mgr._service_name = 'svc'
        mgr._update_mode = serve_utils.UpdateMode.ROLLING
        mgr._tick_version_spec_cache = {}
        mgr._db_fence_kwargs = mock.Mock(return_value={})
        mgr._get_endpoint_probe_interval_seconds = mock.Mock(return_value=1)

        def _probe_and_transition():
            mgr._transition_status_epoch_for_version(
                2, serve_utils.UpdateMode.BLUE_GREEN)
            return []

        mgr._probe_all_replicas = mock.Mock(side_effect=_probe_and_transition)
        with mock.patch.object(
                replica_managers.serve_utils,
                'set_service_status_and_active_versions_from_replica'
        ) as set_st:
            mgr._replica_prober()

        set_st.assert_not_called()

    def test_status_write_retries_same_version_target_change_once(self):
        mgr = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        mgr._target_num_replicas_lock = threading.Lock()
        mgr._target_num_replicas = 0
        mgr._target_num_replicas_generation = 0
        mgr._status_epoch_lock = threading.Lock()
        mgr._status_epoch_generation = 0
        mgr.latest_version = 1
        mgr._service_name = 'svc'
        mgr._update_mode = serve_utils.UpdateMode.ROLLING
        mgr._db_fence_kwargs = mock.Mock(return_value={})

        def _write_status(*args, **kwargs):
            del args, kwargs
            if set_st.call_count == 1:
                assert mgr.publish_target_num_replicas(1, expected_version=1)

        with mock.patch.object(
                replica_managers.serve_utils,
                'set_service_status_and_active_versions_from_replica',
                side_effect=_write_status) as set_st:
            mgr._set_service_status_from_replica_infos([])

        assert set_st.call_args_list == [
            mock.call('svc', [],
                      serve_utils.UpdateMode.ROLLING,
                      target_num_replicas=0),
            mock.call('svc', [],
                      serve_utils.UpdateMode.ROLLING,
                      target_num_replicas=1),
        ]

    def test_status_write_converges_after_repeated_target_changes(self):
        mgr = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        mgr.latest_version = 1
        mgr._service_name = 'svc'
        mgr._target_num_replicas = 0
        mgr._update_mode = serve_utils.UpdateMode.ROLLING
        mgr._db_fence_kwargs = mock.Mock(return_value={})
        fallback_publisher_started = threading.Event()
        fallback_publisher_done = threading.Event()
        fallback_publishers = []

        def _write_status(*args, **kwargs):
            del args, kwargs
            if set_st.call_count == 1:
                assert mgr.publish_target_num_replicas(1, expected_version=1)
            elif set_st.call_count == 2:
                assert mgr.publish_target_num_replicas(0, expected_version=1)
            else:

                def _publish_after_fallback():
                    fallback_publisher_started.set()
                    assert mgr.publish_target_num_replicas(1,
                                                           expected_version=1)
                    fallback_publisher_done.set()

                publisher = threading.Thread(target=_publish_after_fallback)
                fallback_publishers.append(publisher)
                publisher.start()
                assert fallback_publisher_started.wait(timeout=5)
                assert not fallback_publisher_done.wait(timeout=0.1)

        with mock.patch.object(
                replica_managers.serve_utils,
                'set_service_status_and_active_versions_from_replica',
                side_effect=_write_status) as set_st:
            mgr._set_service_status_from_replica_infos([])

        assert set_st.call_args_list == [
            mock.call('svc', [],
                      serve_utils.UpdateMode.ROLLING,
                      target_num_replicas=0),
            mock.call('svc', [],
                      serve_utils.UpdateMode.ROLLING,
                      target_num_replicas=1),
            mock.call('svc', [],
                      serve_utils.UpdateMode.ROLLING,
                      target_num_replicas=0),
        ]
        fallback_publishers[0].join(timeout=5)
        assert not fallback_publishers[0].is_alive()
        assert fallback_publisher_done.is_set()

    def test_ownership_loss_interrupts_interval_before_next_round(self):
        mgr = self._stopped_manager()
        mgr._ownership_lost = threading.Event()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._launch_completion_event = mock.Mock(spec=threading.Event)

        def _stop_after_wait(_timeout):
            mgr._ownership_lost.set()
            return True

        mgr._launch_completion_event.wait.side_effect = _stop_after_wait
        mgr._refresh_thread_pool = mock.Mock()

        with mock.patch.object(
                replica_managers.time,
                'sleep',
                side_effect=AssertionError('interval must be interruptible')):
            mgr._thread_pool_refresher()

        mgr._refresh_thread_pool.assert_called_once_with()
        mgr._launch_completion_event.clear.assert_called_once_with()
        mgr._launch_completion_event.wait.assert_called_once_with(
            replica_managers._PROCESS_POOL_REFRESH_INTERVAL)

    def test_launch_completion_is_joined_before_refresh_reduction(self):
        mgr = self._stopped_manager()
        mgr._ownership_lost = threading.Event()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()

        completion_event = threading.Event()
        completion_notified = threading.Event()
        release_completion_callback = threading.Event()

        def _publish_then_block():
            completion_event.set()
            completion_notified.set()
            assert release_completion_callback.wait(timeout=5)

        callback_event = mock.Mock(spec=threading.Event)
        callback_event.set.side_effect = _publish_then_block
        callback_event.clear.side_effect = completion_event.clear
        callback_event.wait.side_effect = completion_event.wait
        mgr._launch_completion_event = callback_event
        completion_queue, _ = mgr._launch_completion_state()

        refresh_finished = threading.Event()

        def _refresh_after_join():
            # The launch callback publishes while Thread.is_alive() is still
            # true. The queued notification must therefore be joined before
            # the reducer scans the pool.
            assert not launch_worker.is_alive()
            refresh_finished.set()
            mgr._ownership_lost.set()

        mgr._refresh_thread_pool = mock.Mock(side_effect=_refresh_after_join)
        launch_worker = replica_managers._ReplicaLaunchThread(
            target=lambda: None,
            replica_id=7,
            completion_queue=completion_queue,
            completion_event=callback_event)
        mgr._launch_thread_pool[7] = launch_worker
        launch_worker.start()
        refresher = threading.Thread(target=mgr._thread_pool_refresher)

        try:
            assert completion_notified.wait(timeout=5)
            assert launch_worker.is_alive()
            refresher.start()
            assert not refresh_finished.wait(timeout=0.1)
        finally:
            release_completion_callback.set()

        assert refresh_finished.wait(timeout=5)
        launch_worker.join(timeout=5)
        refresher.join(timeout=5)
        assert not launch_worker.is_alive()
        assert not refresher.is_alive()
        mgr._refresh_thread_pool.assert_called_once_with()


class TestGetResourcesPorts:

    def test_infers_common_port_from_heterogeneous_resources(self):
        yaml_content = """
resources:
  any_of:
    - cpus: 1
      ports: 8080
    - cpus: 2
      ports: 8080
service:
  readiness_probe: /health
  replica_policy:
    min_replicas: 1
    max_replicas: 2
    target_qps_per_replica: 1
run: echo hi
"""

        assert replica_managers._get_resources_ports(yaml_content) == '8080'

    def test_authoritative_spec_can_omit_service_port(self):
        yaml_content = """
resources:
  cpus: 1
  ports: 8080
service:
  readiness_probe: /health
  replica_policy:
    min_replicas: 1
    max_replicas: 2
    target_qps_per_replica: 1
run: echo hi
"""
        task = replica_managers.task_lib.Task.from_yaml_str(yaml_content)
        assert task.service is not None
        assert task.service.ports is None

        assert replica_managers._get_resources_ports(yaml_content,
                                                     task.service) == '8080'

    def test_rejects_inconsistent_resource_ports(self):
        yaml_content = """
resources:
  any_of:
    - cpus: 1
      ports: 8080
    - cpus: 2
      ports: 8081
service:
  readiness_probe: /health
  replica_policy:
    min_replicas: 1
    max_replicas: 2
    target_qps_per_replica: 1
run: echo hi
"""

        with pytest.raises(ValueError, match='multiple ports'):
            replica_managers._get_resources_ports(yaml_content)


def _make_manager(service_name='svc', next_replica_id=1):
    """Allocate the real runtime interface without the I/O-bearing init."""
    mgr = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    mgr.lock = threading.RLock()
    mgr._service_name = service_name
    mgr._next_replica_id = next_replica_id
    mgr.latest_version = 1
    mgr.yaml_content = 'resources: {}'
    mgr._launch_thread_pool = {}
    mgr._down_thread_pool = {}
    mgr._failed_cleanup_retry_attempts = {}
    mgr._failed_cleanup_retry_at = {}
    mgr._tick_version_spec_cache = {}
    mgr._spot_placer = None
    mgr._pending_version = None
    mgr._uses_logical_replicas = False
    mgr._version_specs = {1: mock.Mock()}
    mgr._logical_exact_accelerator_shapes = {}
    mgr._logical_reconcile_snapshot = None
    mgr._logical_target = None
    mgr._logical_state_lock = threading.RLock()
    mgr._replica_to_logical_launch_fence = thread_utils.ThreadSafeDict()
    mgr._logical_controller_epoch = 'test-controller-epoch'
    mgr._wait_for_idle_trackers = {}
    mgr._recovering_logical_retirement_ids = set()
    mgr._logical_retirement_recovery_deadline = None
    mgr._logical_retirement_reactivation_generation = None
    return mgr


def _fake_replica_info(replica_id, status=None):
    info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                        cluster_name=f'svc-{replica_id}',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    if status is None:
        # Most callers need an inert existing row only for identity/accounting.
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        expected_status = (
            replica_managers.serve_state.ReplicaStatus.FAILED_CLEANUP)
    elif status == replica_managers.serve_state.ReplicaStatus.PENDING:
        expected_status = status
    elif status == replica_managers.serve_state.ReplicaStatus.PROVISIONING:
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.RUNNING)
        expected_status = status
    elif status == replica_managers.serve_state.ReplicaStatus.READY:
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.service_ready_now = True
        info.status_property.first_ready_time = 1.0
        expected_status = status
    else:
        raise ValueError(f'Unsupported fake replica status: {status!r}')
    assert info.status == expected_status
    return info


class TestBoundOrdinaryLaunchManagerIntegration:

    @staticmethod
    def _projection(disposition,
                    status='SUCCEEDED',
                    *,
                    projected=True,
                    error=None,
                    cancel_reason=None):
        return types.SimpleNamespace(disposition=disposition,
                                     projected=projected,
                                     cancel_reason=cancel_reason,
                                     request=types.SimpleNamespace(
                                         status=status, error=error))

    def test_pre_effect_terminal_is_typed_failure_without_sdk_wait(self):
        reduce_exact = mock.Mock(return_value=self._projection(
            'PRE_EFFECT_TERMINAL', status='CANCELLED'))
        with mock.patch.object(replica_managers.sdk,
                               'stream_and_get') as stream_and_get, \
             pytest.raises(
                 replica_managers.
                 _BoundOrdinaryLaunchPreEffectTerminalError):
            replica_managers._wait_for_bound_ordinary_launch(
                replica_id=1,
                cluster_name='svc-1',
                request_id='request-id',
                stream_logs=True,
                launch_cloud=clouds.AWS(),
                reduce_exact=reduce_exact,
                cancel_exact=mock.Mock(),
                replica_to_launch_cancelled=(thread_utils.ThreadSafeDict()))

        stream_and_get.assert_not_called()
        reduce_exact.assert_called_once_with(None, None)

    def test_recovered_cancel_intent_is_redelivered_before_sdk_wait(self):
        recovered = self._projection('ADOPT_ACTIVE',
                                     status='RUNNING',
                                     projected=False,
                                     cancel_reason='superseded-version')
        projected = self._projection('PRE_EFFECT_TERMINAL',
                                     status='CANCELLED',
                                     cancel_reason='superseded-version')
        reduce_exact = mock.Mock(return_value=recovered)
        cancel_exact = mock.Mock(return_value=projected)
        with mock.patch.object(replica_managers.sdk,
                               'stream_and_get') as stream_and_get, \
             pytest.raises(replica_managers._ReplicaLaunchSupersededError,
                           match='superseded-version'):
            replica_managers._wait_for_bound_ordinary_launch(
                replica_id=1,
                cluster_name='svc-1',
                request_id='request-id',
                stream_logs=True,
                launch_cloud=clouds.AWS(),
                reduce_exact=reduce_exact,
                cancel_exact=cancel_exact,
                replica_to_launch_cancelled=(thread_utils.ThreadSafeDict()))

        stream_and_get.assert_not_called()
        cancel_exact.assert_called_once_with('superseded-version')

    def test_recovered_cancel_already_projected_is_not_redelivered(self):
        projected = self._projection('PRE_EFFECT_TERMINAL',
                                     status='CANCELLED',
                                     cancel_reason='superseded-version')
        reduce_exact = mock.Mock(return_value=projected)
        cancel_exact = mock.Mock()
        with mock.patch.object(replica_managers.sdk,
                               'stream_and_get') as stream_and_get, \
             pytest.raises(replica_managers._ReplicaLaunchSupersededError,
                           match='superseded-version'):
            replica_managers._wait_for_bound_ordinary_launch(
                replica_id=1,
                cluster_name='svc-1',
                request_id='request-id',
                stream_logs=True,
                launch_cloud=clouds.AWS(),
                reduce_exact=reduce_exact,
                cancel_exact=cancel_exact,
                replica_to_launch_cancelled=(thread_utils.ThreadSafeDict()))

        stream_and_get.assert_not_called()
        cancel_exact.assert_not_called()

    def test_live_waiter_polls_same_controller_expiry_to_terminal(self):
        active = self._projection('ADOPT_ACTIVE',
                                  status='RUNNING',
                                  projected=False)
        expired = self._projection('PRE_EFFECT_TERMINAL', status='CANCELLED')
        reduce_exact = mock.Mock(side_effect=[active, expired])
        waiter = mock.Mock()
        waiter.is_alive.return_value = True
        waiter.exception = None
        with mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=waiter), \
             pytest.raises(
                 replica_managers.
                 _BoundOrdinaryLaunchPreEffectTerminalError):
            replica_managers._wait_for_bound_ordinary_launch(
                replica_id=1,
                cluster_name='svc-1',
                request_id='request-id',
                stream_logs=False,
                launch_cloud=None,
                reduce_exact=reduce_exact,
                cancel_exact=mock.Mock(),
                replica_to_launch_cancelled=(thread_utils.ThreadSafeDict()))

        assert reduce_exact.call_args_list == [
            mock.call(None, None),
            mock.call(None, None),
        ]
        waiter.start.assert_called_once()
        waiter.join.assert_not_called()

    def test_ambiguous_projection_never_falls_through_to_cleanup(self):
        reduce_exact = mock.Mock(return_value=self._projection(
            'AMBIGUOUS', status='FAILED', projected=False))
        with pytest.raises(replica_managers._BoundOrdinaryLaunchUnresolvedError,
                           match='durably ambiguous'):
            replica_managers._wait_for_bound_ordinary_launch(
                replica_id=1,
                cluster_name='svc-1',
                request_id='request-id',
                stream_logs=False,
                launch_cloud=None,
                reduce_exact=reduce_exact,
                cancel_exact=mock.Mock(),
                replica_to_launch_cancelled=(thread_utils.ThreadSafeDict()))

    @pytest.mark.parametrize('reason', ['capacity', 'quota'])
    def test_waiter_raises_typed_durable_capacity_error(self, reason):
        error = (_capacity_error() if reason == 'capacity' else _quota_error())
        durable_error = {
            'object': error,
            'type': type(error).__name__,
            'message': str(error),
        }
        reduce_exact = mock.Mock(return_value=self._projection(
            'PROJECTED', status='FAILED', error=durable_error))

        with mock.patch.object(
                replica_managers.cloud_vm_ray_backend,
                'classify_resources_unavailable_error',
                return_value=reason), \
             pytest.raises(replica_managers._ReplicaLaunchCapacityError) as exc:
            replica_managers._wait_for_bound_ordinary_launch(
                replica_id=1,
                cluster_name='svc-1',
                request_id='request-id',
                stream_logs=False,
                launch_cloud=mock.sentinel.cloud,
                reduce_exact=reduce_exact,
                cancel_exact=mock.Mock(),
                replica_to_launch_cancelled=(thread_utils.ThreadSafeDict()))

        assert exc.value.reason == reason

    def test_teardown_waits_for_exact_cancel_projection(self):
        manager = _make_manager()
        manager._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND, binding_epoch=2)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        initial = self._projection('ADOPT_ACTIVE', projected=False)
        initial.context = types.SimpleNamespace(request_id='request-id')
        waiting = self._projection('WAIT_QUIESCENCE',
                                   status='CANCELLED',
                                   projected=False)
        projected = self._projection('PROJECTED', status='CANCELLED')
        reduce_exact = mock.Mock(return_value=projected)
        cancel_exact = mock.Mock(return_value=waiting)
        with mock.patch.object(
                replica_managers.request_postgres,
                'lookup_bound_ordinary_launch_cancel_target',
                return_value=initial), \
             mock.patch.object(
                 manager,
                 '_bound_ordinary_launch_callbacks',
                 return_value=(mock.Mock(), reduce_exact, cancel_exact)), \
             mock.patch.object(replica_managers.time, 'sleep'):
            manager._settle_bound_ordinary_launch_for_teardown(info)

        cancel_exact.assert_called_once_with('replica-teardown')
        reduce_exact.assert_called_once_with(None, None)

    @pytest.mark.parametrize(('previous_mode', 'replacement_mode'),
                             [(ordinary_launch_binding.BindingMode.LEGACY,
                               ordinary_launch_binding.BindingMode.BOUND),
                              (ordinary_launch_binding.BindingMode.BOUND,
                               ordinary_launch_binding.BindingMode.LEGACY)])
    def test_binding_transition_installs_authority_under_closed_gate(
            self, previous_mode, replacement_mode):
        manager = _make_manager()
        previous = _binding_authority(previous_mode)
        replacement = _binding_authority(replacement_mode, binding_epoch=2)
        manager._ordinary_launch_binding_authority = previous

        with manager.ordinary_launch_binding_transition() as install:
            assert manager._ordinary_launch_binding_transition_in_progress.is_set(
            )
            assert not manager._ordinary_binding_profile_launch_is_authorized()
            install(replacement)

        assert not manager._ordinary_launch_binding_transition_in_progress.is_set(
        )
        assert manager._ordinary_launch_binding_authority is replacement

    @pytest.mark.parametrize(
        'worker_marker', ['ordinary_legacy_launch', 'bound_ordinary_launch'])
    def test_binding_transition_rejects_unsettled_eligible_worker(
            self, worker_marker):
        manager = _make_manager()
        manager._ordinary_launch_binding_authority = _binding_authority()
        worker = replica_managers._ReplicaLaunchThread(
            target=lambda: None,
            replica_id=1,
            completion_queue=queue.SimpleQueue(),
            completion_event=threading.Event(),
            **{worker_marker: True})
        manager._launch_thread_pool[1] = worker

        with pytest.raises(replica_managers._BoundOrdinaryLaunchUnresolvedError,
                           match='local eligible workers'):
            with manager.ordinary_launch_binding_transition():
                pass

        assert not manager._ordinary_launch_binding_transition_in_progress.is_set(
        )

    def test_deterministic_http_rejection_is_not_a_lost_ack(self):
        response = mock.Mock(status_code=409)
        rejected = replica_managers.requests.exceptions.HTTPError(
            response=response)
        assert not replica_managers._bound_submission_may_have_committed(
            rejected)
        assert replica_managers._bound_submission_may_have_committed(
            replica_managers.requests.exceptions.ConnectionError())

    def test_zero_cost_launch_is_not_bound_profile(self):
        manager = _make_manager()
        manager._is_pool = False
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PENDING)
        info.is_zero_cost = True
        assert not manager._is_ordinary_launch_binding_profile(info, {})

    @pytest.mark.parametrize(('field', 'value'), [
        ('reserved_fill', True),
        ('is_zero_cost', True),
        ('unknown_capacity_replacement', True),
        ('cost_rebalance_for_replica_id', 7),
        ('system_recovery_launch_intent', mock.sentinel.intent),
        ('system_recovery', mock.sentinel.recovery),
        ('system_recovery_quarantine', mock.sentinel.quarantine),
    ])
    def test_excluded_profile_fence_names_exact_persisted_replica(
            self, field, value):
        manager = _make_manager()
        manager._is_pool = False
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PENDING)
        setattr(info, field, value)
        fence = {'owner-fence': True}

        excluded = manager._binding_excluded_launch_fence_context(info, fence)

        assert excluded is not None
        normalized = (replica_managers.serve_state.
                      normalize_binding_excluded_launch_context(excluded))
        assert normalized == {
            replica_managers.serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_PROFILE_KEY:
                replica_managers.serve_constants.
                ORDINARY_LAUNCH_BINDING_EXCLUDED_PERSISTED_PROFILE,
            replica_managers.serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_ID_KEY:
                info.replica_id,
            replica_managers.serve_constants.ORDINARY_LAUNCH_BINDING_EXCLUDED_REPLICA_RECORD_ID_KEY:
                info.replica_record_id,
        }

    def test_pre_effect_projection_stays_pending_and_reuses_claim(self):
        manager = _make_manager()
        manager._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        info.paid_capacity_pool_key = 'pool-a'
        projection = types.SimpleNamespace(
            locked_replica_info=info,
            request=types.SimpleNamespace(error=None),
            status=types.SimpleNamespace(value='CANCELLED'),
            pre_effect_terminal=True,
            paid_capacity_pool_key='pool-a',
            context=types.SimpleNamespace(association_id=uuid.uuid4()))

        with mock.patch.object(
                replica_managers.serve_state,
                'update_replica_for_bound_ordinary_launch_in_transaction',
                return_value=True) as update:
            assert manager._project_bound_ordinary_launch(
                None, mock.sentinel.connection, projection)

        assert info.status == replica_managers.serve_state.ReplicaStatus.PENDING
        assert update.call_args.kwargs['provider_launch_succeeded'] is False
        assert update.call_args.kwargs['paid_capacity_pool_key'] == 'pool-a'
        assert (update.call_args.kwargs['paid_capacity_outcome'] ==
                paid_capacity.LaunchOutcome.OTHER_FAILURE)

    def test_system_oom_projection_binds_exact_request_and_job_atomically(self):
        manager = _make_manager()
        manager._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND, generic=True)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        intent = recovery_state.SystemRecoveryLaunchIntent(
            version=1,
            controller_contract_version=2,
            recovery_authorization_version=3,
            recovery_authorization_profile_id='profile-v3',
            recovery_authorization_sha256='a' * 64,
            runtime_profile_version=2,
            expected_runtime_capability=(
                recovery_state.SYSTEM_RECOVERY_CAPABILITY),
            service_hash='hash',
            replica_id=1,
            launch_generation=9,
            launch_nonce='b' * 64,
            workspace='default',
            resource_envelope_sha256='c' * 64,
            task_sha256='d' * 64,
            runtime_image_digest=f'sha256:{"e" * 64}',
            owned_container_spec_sha256='f' * 64,
            execution_envelope_sha256='1' * 64)
        info.system_recovery_launch_intent = intent
        info.system_recovery_disposition = (
            recovery_state.SystemRecoveryDisposition.CANDIDATE)
        info.system_recovery_revision = 1
        profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
            ordinary_launch_binding.NonPoolLaunchProfileKind.
            SYSTEM_OOM_RECOVERY,
            authorization_reference=f'system-oom:{intent.launch_nonce}',
            authorization_generation=intent.launch_generation,
            authorization_payload={'intent': intent.to_dict()})
        context = ordinary_launch_binding.BoundNonPoolLaunchContext(
            association_id=uuid.uuid4(),
            request_id='request-id',
            service_name='svc',
            replica_id=1,
            replica_record_id=uuid.UUID(info.replica_record_id),
            launch_generation=1,
            input_digest='a' * 64,
            profile=profile,
            capability_cohort_epoch=1,
            capability_profile_set_digest=(
                ordinary_launch_binding.supported_non_pool_profile_set_digest()
            ),
            receipt_protocol_version=1)
        projection = types.SimpleNamespace(
            locked_replica_info=info,
            request=types.SimpleNamespace(error=None),
            status=types.SimpleNamespace(value='SUCCEEDED'),
            service_job_id=41,
            pre_effect_terminal=False,
            paid_capacity_pool_key=None,
            context=context)

        with mock.patch.object(
                replica_managers.serve_state,
                'update_replica_for_bound_ordinary_launch_in_transaction',
                return_value=True) as update:
            assert manager._project_bound_ordinary_launch(
                None, mock.sentinel.connection, projection)

        assert info.launch_request_id == 'request-id'
        assert info.service_job_id == 41
        assert info.system_recovery_revision == 2
        update.assert_called_once()

    def test_reserved_fill_absence_projects_failed_without_materialization(
            self):
        manager = _make_manager()
        manager._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND, generic=True)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        info.reserved_fill = True
        info.is_zero_cost = True
        profile = ordinary_launch_binding.NonPoolLaunchProfile.create(
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL,
            authorization_reference='reserved-fill:' + 'a' * 64,
            authorization_generation=1,
            authorization_payload={'physical_cluster_uid': 'uid-a'})
        context = ordinary_launch_binding.BoundNonPoolLaunchContext(
            association_id=uuid.uuid4(),
            request_id='request-id',
            service_name='svc',
            replica_id=1,
            replica_record_id=uuid.UUID(info.replica_record_id),
            launch_generation=1,
            input_digest='a' * 64,
            profile=profile,
            capability_cohort_epoch=1,
            capability_profile_set_digest=(
                ordinary_launch_binding.supported_non_pool_profile_set_digest()
            ),
            receipt_protocol_version=1)
        projection = types.SimpleNamespace(
            locked_replica_info=info,
            request=types.SimpleNamespace(error=None),
            status=types.SimpleNamespace(value='SUCCEEDED'),
            service_job_id=None,
            pre_effect_terminal=False,
            paid_capacity_pool_key=None,
            provider_evidence=(ordinary_launch_binding.ProviderEvidence.ABSENT),
            context=context)

        with mock.patch.object(
                replica_managers.serve_state,
                'update_replica_for_bound_ordinary_launch_in_transaction',
                return_value=True) as update:
            assert manager._project_bound_ordinary_launch(
                None, mock.sentinel.connection, projection)

        assert (info.status_property.sky_launch_status ==
                common_utils.ProcessStatus.FAILED)
        assert update.call_args.kwargs['provider_launch_succeeded'] is False
        assert update.call_args.kwargs['paid_capacity_pool_key'] is None
        assert update.call_args.kwargs['paid_capacity_outcome'] is None

    def test_generic_pre_effect_result_retires_intent_for_fresh_planning(self):
        manager = _make_manager()
        authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND, generic=True)
        manager._ordinary_launch_binding_authority = authority
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PENDING)
        retirement = ordinary_launch_binding.PreAdmissionRetirement(
            ordinary_launch_binding.PreAdmissionRetirementDisposition.RETIRED,
            ordinary_launch_binding.NonPoolLaunchProfileKind.COST_REBALANCE)

        with mock.patch.object(
                ordinary_launch_binding,
                'retire_pre_admission_non_pool_launch_intent',
                return_value=retirement) as retire, \
             mock.patch.object(manager, '_launch_replica') as launch:
            assert manager._redrive_bound_ordinary_launch_after_pre_effect(info)

        retire.assert_called_once_with(authority, 1, info.replica_record_id)
        launch.assert_not_called()
        assert manager._scale_reconciliation_event.is_set()

    @pytest.mark.parametrize('reason', ['capacity', 'quota'])
    def test_projection_classifies_decoded_durable_capacity_error(self, reason):
        manager = _make_manager()
        manager._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        error = (_capacity_error() if reason == 'capacity' else _quota_error())
        projection = types.SimpleNamespace(
            locked_replica_info=info,
            request=types.SimpleNamespace(
                error={
                    'object': error,
                    'type': type(error).__name__,
                    'message': str(error),
                }),
            status=types.SimpleNamespace(value='FAILED'),
            pre_effect_terminal=False,
            paid_capacity_pool_key='pool-a',
            context=types.SimpleNamespace(association_id=uuid.uuid4()))
        outcome = (paid_capacity.LaunchOutcome.CAPACITY_FAILURE if reason
                   == 'capacity' else paid_capacity.LaunchOutcome.QUOTA_FAILURE)

        with mock.patch.object(
                replica_managers.cloud_vm_ray_backend,
                'classify_resources_unavailable_error',
                return_value=reason), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'update_replica_for_bound_ordinary_launch_in_transaction',
                 return_value=True) as update:
            assert manager._project_bound_ordinary_launch(
                mock.sentinel.cloud, mock.sentinel.connection, projection)

        assert update.call_args.kwargs['paid_capacity_outcome'] is outcome
        assert info.status_property.failed_spot_availability is True

    def test_success_projection_preserves_interrupted_teardown(self):
        manager = _make_manager()
        manager._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.INTERRUPTED)
        projection = types.SimpleNamespace(
            locked_replica_info=info,
            request=types.SimpleNamespace(error=None),
            status=types.SimpleNamespace(value='SUCCEEDED'),
            pre_effect_terminal=False,
            paid_capacity_pool_key=None,
            context=types.SimpleNamespace(association_id=uuid.uuid4()))

        with mock.patch.object(
                replica_managers.serve_state,
                'update_replica_for_bound_ordinary_launch_in_transaction',
                return_value=True) as update:
            assert manager._project_bound_ordinary_launch(
                None, mock.sentinel.connection, projection)
        assert (info.status_property.sky_launch_status ==
                common_utils.ProcessStatus.INTERRUPTED)
        assert update.call_args.kwargs['provider_launch_succeeded'] is True

    def test_exact_success_decoder_requires_matching_vm_handle(self):
        request = mock.Mock()
        handle = object.__new__(
            replica_managers.backends.CloudVmRayResourceHandle)
        handle.cluster_name = 'svc-1'
        request.get_return_value.return_value = (17, handle)
        row = {'status': 'SUCCEEDED'}
        with mock.patch.object(request_postgres,
                               '_request_from_mapping',
                               return_value=request):
            assert request_postgres._request_service_job_id(row, 'svc-1') == 17
            with pytest.raises(
                    ordinary_launch_binding.OrdinaryLaunchBindingConflict,
                    match='malformed or mismatched'):
                request_postgres._request_service_job_id(row, 'other-cluster')

    def test_nonprojected_reduction_carries_exact_context(self):
        association_id = uuid.uuid4()
        record_id = uuid.uuid4()
        association = {
            'association_id': association_id,
            'request_id': 'request-id',
            'service_name': 'svc',
            'replica_id': 1,
            'replica_record_id': record_id,
            'launch_generation': 2,
            'input_digest': 'a' * 64,
            'service_job_id': None,
        }
        facts = mock.Mock(spec=request_postgres.BoundOrdinaryLaunchRequestFacts)
        reduction = request_postgres._reduction_result(
            request_postgres.OrdinaryLaunchReductionDisposition.ADOPT_ACTIVE,
            facts, association)
        assert reduction.context.association_id == association_id
        assert reduction.context.replica_record_id == record_id
        assert not reduction.projected

    def test_stable_submission_advances_after_pre_effect_settlement(self):
        record_id = '00000000-0000-4000-8000-000000000001'
        predecessor = {
            'submission_id': uuid.uuid4(),
            'launch_generation': 1,
            'resolution':
                ordinary_launch_binding.Resolution.PRE_EFFECT_TERMINAL.value,
            'cancel_reason': None,
        }
        connection = mock.MagicMock()
        one_or_none = (
            connection.execute.return_value.mappings.return_value.one_or_none)
        one_or_none.side_effect = [None, predecessor, predecessor]
        engine = mock.MagicMock()
        engine.connect.return_value.__enter__.return_value = connection
        with mock.patch.object(request_postgres,
                               'initialize_and_get_db',
                               return_value=engine):
            generation_1 = (
                request_postgres.stable_bound_ordinary_launch_submission_id(
                    'svc', 1, record_id))
            generation_2 = (
                request_postgres.stable_bound_ordinary_launch_submission_id(
                    'svc', 1, record_id))
            generation_2_retry = (
                request_postgres.stable_bound_ordinary_launch_submission_id(
                    'svc', 1, record_id))
        assert generation_1 != generation_2
        assert generation_2_retry == generation_2

    def test_stable_submission_rejects_cancelled_pre_effect_predecessor(self):
        record_id = '00000000-0000-4000-8000-000000000001'
        predecessor = {
            'submission_id': uuid.uuid4(),
            'launch_generation': 1,
            'resolution':
                ordinary_launch_binding.Resolution.PRE_EFFECT_TERMINAL.value,
            'cancel_reason': 'superseded-version',
        }
        connection = mock.MagicMock()
        (connection.execute.return_value.mappings.return_value.one_or_none.
         return_value) = predecessor
        engine = mock.MagicMock()
        engine.connect.return_value.__enter__.return_value = connection
        with mock.patch.object(request_postgres,
                               'initialize_and_get_db',
                               return_value=engine), \
             pytest.raises(ordinary_launch_binding.
                           OrdinaryLaunchBindingConflict,
                           match='cancelled pre-effect'):
            request_postgres.stable_bound_ordinary_launch_submission_id(
                'svc', 1, record_id)

    def test_cancelled_pre_effect_projection_is_durably_interrupted(self):
        manager = _make_manager()
        manager._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        projection = types.SimpleNamespace(
            locked_replica_info=info,
            request=types.SimpleNamespace(error=None),
            status=types.SimpleNamespace(value='CANCELLED'),
            pre_effect_terminal=True,
            cancel_reason='superseded-version',
            paid_capacity_pool_key=None,
            context=types.SimpleNamespace(association_id=uuid.uuid4()))

        with mock.patch.object(
                replica_managers.serve_state,
                'update_replica_for_bound_ordinary_launch_in_transaction',
                return_value=True) as update:
            assert manager._project_bound_ordinary_launch(
                None, mock.sentinel.connection, projection)
        assert (info.status_property.sky_launch_status ==
                common_utils.ProcessStatus.INTERRUPTED)
        assert update.call_args.kwargs['provider_launch_succeeded'] is False

    def test_pre_effect_cancel_releases_paid_claim(self):
        settle_claim = (request_postgres.
                        _settle_projected_paid_capacity_claim_in_transaction)
        with mock.patch.object(
                request_postgres.ordinary_launch_binding,
                'release_projected_paid_capacity_claim_in_connection',
                return_value=True) as release:
            assert settle_claim(mock.sentinel.connection,
                                mock.sentinel.context, {'cancel_reason': None},
                                pre_effect=True)
            release.assert_not_called()
            assert settle_claim(mock.sentinel.connection,
                                mock.sentinel.context,
                                {'cancel_reason': 'replica-teardown'},
                                pre_effect=True)
        release.assert_called_once_with(mock.sentinel.connection,
                                        mock.sentinel.context)


def _stamp_protocol_v2_fill(info,
                            context='phx',
                            physical_uid='phx-uid',
                            card='H200',
                            generation=9):
    info.reserved_fill = True
    info.reserved_fill_pool_key = (
        replica_managers.reserved_capacity_broker.make_pool_key(
            context,
            card,
            protocol_version=(
                replica_managers.reserved_capacity_broker.PROTOCOL_V2),
            physical_cluster_uid=physical_uid))
    info.reserved_fill_service_generation = generation
    info.reserved_fill_physical_cluster_uid = physical_uid
    info.reserved_fill_kubernetes_context = context
    info.location = {
        'cloud': 'Kubernetes',
        'region': context,
        'accelerators': {
            card: 1,
        },
    }
    info.resources_override = {
        'cloud': 'Kubernetes',
        'region': context,
        'accelerators': {
            card: 1,
        },
    }
    return info


def _protocol_v2_handle(info, context='phx'):
    handle = mock.Mock(spec=replica_managers.backends.CloudVmRayResourceHandle)
    handle.cluster_name = info.cluster_name
    handle.cluster_yaml = '/tmp/protocol-v2-cluster.yaml'
    handle.launched_resources = mock.Mock(cloud=clouds.Kubernetes(),
                                          region=context)
    return handle


def test_probe_url_v2_group_reuses_one_outer_physical_fence():
    infos = []
    records = {}
    for replica_id in (1, 2):
        info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                            cluster_name=f'svc-{replica_id}',
                                            replica_port='8080',
                                            is_spot=False,
                                            location=None,
                                            version=1,
                                            resources_override=None)
        _stamp_protocol_v2_fill(info)
        handle = _protocol_v2_handle(info)
        infos.append(info)
        records[info.cluster_name] = {
            'name': info.cluster_name,
            'handle': handle,
        }
    mgr = _make_manager()
    mgr._persist_replica = mock.Mock()
    active_scopes = 0
    physical_uid_reads = 0

    @contextlib.contextmanager
    def _physical_fence(context, physical_uid):
        nonlocal active_scopes, physical_uid_reads
        assert (context, physical_uid) == ('phx', 'phx-uid')
        if active_scopes == 0:
            physical_uid_reads += 1
        active_scopes += 1
        try:
            yield
        finally:
            active_scopes -= 1

    with mock.patch.object(replica_managers.global_user_state,
                           'get_clusters_from_names',
                           return_value=records), \
         mock.patch.object(replica_managers.serve_utils,
                           'get_provider_configs_for_handles',
                           return_value={}), \
         mock.patch.object(replica_managers.kubernetes_adaptor,
                           'physical_cluster_uid_fence',
                           side_effect=_physical_fence), \
         mock.patch.object(replica_managers.backend_utils,
                           'get_endpoints',
                           return_value={8080: '10.0.0.1:8080'}):
        urls = mgr._resolve_probe_urls(infos)

    assert urls == {
        1: 'http://10.0.0.1:8080',
        2: 'http://10.0.0.1:8080',
    }
    assert physical_uid_reads == 1


def test_probe_url_conflicting_uids_for_one_context_fail_closed_as_a_wave():
    infos = []
    records = {}
    for replica_id, physical_uid in ((1, 'phx-uid-a'), (2, 'phx-uid-b')):
        info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                            cluster_name=f'svc-{replica_id}',
                                            replica_port='8080',
                                            is_spot=False,
                                            location=None,
                                            version=1,
                                            resources_override=None)
        _stamp_protocol_v2_fill(info, physical_uid=physical_uid)
        infos.append(info)
        records[info.cluster_name] = {
            'name': info.cluster_name,
            'handle': _protocol_v2_handle(info),
        }
    mgr = _make_manager()
    mgr._record_provider_identity_uncertain = mock.Mock()
    identity_rejected = set()

    def _provider_configs(handles):
        # Conflicting handles are removed before provider-config resolution;
        # no winner may be selected by fence scheduling order.
        assert handles == {}
        return {}

    with mock.patch.object(replica_managers.global_user_state,
                           'get_clusters_from_names',
                           return_value=records), \
         mock.patch.object(replica_managers.serve_utils,
                           'get_provider_configs_for_handles',
                           side_effect=_provider_configs), \
         mock.patch.object(replica_managers.kubernetes_adaptor,
                           'physical_cluster_uid_fence') as provider_fence, \
         mock.patch.object(replica_managers.backend_utils,
                           'get_endpoints') as endpoints:
        urls = mgr._resolve_probe_urls(
            infos, identity_rejected_replica_ids=identity_rejected)

    assert urls == {1: None, 2: None}
    assert identity_rejected == {1, 2}
    assert mgr._record_provider_identity_uncertain.call_count == 2
    provider_fence.assert_not_called()
    endpoints.assert_not_called()


def test_v2_job_status_batch_reuses_one_physical_uid_proof():
    infos = []
    records = {}
    for replica_id in (1, 2):
        info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                            cluster_name=f'svc-{replica_id}',
                                            replica_port='-',
                                            is_spot=False,
                                            location=None,
                                            version=1,
                                            resources_override=None)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        _stamp_protocol_v2_fill(info)
        handle = _protocol_v2_handle(info)
        infos.append(info)
        records[info.cluster_name] = {
            'name': info.cluster_name,
            'handle': handle,
        }
    mgr = _make_manager()
    mgr._is_pool = True
    backend = mock.Mock()
    backend.get_job_status.return_value = {1: job_lib.JobStatus.RUNNING}
    active_scopes = 0
    physical_uid_reads = 0
    scope_lock = threading.Lock()

    @contextlib.contextmanager
    def _physical_fence(context, physical_uid):
        nonlocal active_scopes, physical_uid_reads
        assert (context, physical_uid) == ('phx', 'phx-uid')
        with scope_lock:
            if active_scopes == 0:
                physical_uid_reads += 1
            active_scopes += 1
        try:
            yield
        finally:
            with scope_lock:
                active_scopes -= 1

    def _consume(results):
        for _, result in results:
            result.get()

    mgr._handle_job_status_results = _consume
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=infos), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_clusters_from_names',
                           return_value=records), \
         mock.patch.object(replica_managers.backends,
                           'CloudVmRayBackend',
                           return_value=backend), \
         mock.patch.object(replica_managers.kubernetes_adaptor,
                           'physical_cluster_uid_fence',
                           side_effect=_physical_fence):
        mgr._fetch_job_status()

    assert physical_uid_reads == 1
    assert backend.get_job_status.call_count == 2


def test_ordinary_kubernetes_fleet_skips_remote_job_status_at_840():
    """Ordinary Kubernetes liveness must not scale remote execs with fleet."""
    infos = []
    records = {}
    physical_pools = (('east', 'east-uid', 'A100-80GB'), ('phx', 'phx-uid',
                                                          'H200'))
    for replica_id in range(1, 841):
        context, physical_uid, card = physical_pools[replica_id % 2]
        info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                            cluster_name=f'svc-{replica_id}',
                                            replica_port='8080',
                                            is_spot=False,
                                            location=None,
                                            version=1,
                                            resources_override=None)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        _stamp_protocol_v2_fill(info,
                                context=context,
                                physical_uid=physical_uid,
                                card=card)
        infos.append(info)
        records[info.cluster_name] = {
            'name': info.cluster_name,
            'handle': _protocol_v2_handle(info, context=context),
        }

    mgr = _make_manager()
    mgr._is_pool = False
    mgr._handle_job_status_results = mock.Mock()
    backend = replica_managers.backends.CloudVmRayBackend()
    backend.get_job_status = mock.Mock(
        side_effect=AssertionError('ordinary Kubernetes status used exec'))
    backend.get_job_status_with_system_recovery = mock.Mock(
        side_effect=AssertionError('ordinary row used recovery status'))

    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=infos), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_clusters_from_names',
                           return_value=records) as get_clusters, \
         mock.patch.object(replica_managers.backends,
                           'CloudVmRayBackend',
                           return_value=backend), \
         mock.patch.object(
             replica_managers.kubernetes_adaptor,
             'physical_cluster_uid_fence') as physical_uid_fence, \
         mock.patch.object(replica_managers.provider_phase,
                           'provider_phase') as provider_phase:
        mgr._fetch_job_status()

    get_clusters.assert_called_once()
    assert len(get_clusters.call_args.args[0]) == 840
    backend.get_job_status.assert_not_called()
    backend.get_job_status_with_system_recovery.assert_not_called()
    physical_uid_fence.assert_not_called()
    provider_phase.assert_not_called()
    mgr._handle_job_status_results.assert_not_called()


def test_kubernetes_system_recovery_retains_exact_remote_job_status():
    """The ordinary Kubernetes capability cannot weaken recovery evidence."""
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    _stamp_protocol_v2_fill(info)
    info.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.CANDIDATE)
    info.service_job_id = 17
    handle = _protocol_v2_handle(info)
    records = {info.cluster_name: {'name': info.cluster_name, 'handle': handle}}
    mgr = _make_manager()
    mgr._is_pool = False
    backend = replica_managers.backends.CloudVmRayBackend()
    backend.get_job_status = mock.Mock()
    backend.get_job_status_with_system_recovery = mock.Mock(return_value=({
        17: job_lib.JobStatus.RUNNING
    }, {}, {
        17: job_lib.JobSystemRecoveryDetailStatus.ABSENT
    }))

    def _consume(results):
        for _, result in results:
            result.get()

    mgr._handle_job_status_results = mock.Mock(side_effect=_consume)
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[info]), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_clusters_from_names',
                           return_value=records), \
         mock.patch.object(replica_managers.backends,
                           'CloudVmRayBackend',
                           return_value=backend), \
         mock.patch.object(replica_managers.kubernetes_adaptor,
                           'physical_cluster_uid_fence',
                           return_value=contextlib.nullcontext()):
        mgr._fetch_job_status()

    backend.get_job_status.assert_not_called()
    backend.get_job_status_with_system_recovery.assert_called_once_with(
        handle, [17], stream_logs=False)
    mgr._handle_job_status_results.assert_called_once()


@pytest.mark.parametrize(('endpoint_ready', 'expected_termination'),
                         [(True, False), (False, True)])
def test_ordinary_kubernetes_endpoint_owns_detached_job_liveness(
        endpoint_ready, expected_termination):
    """A detached ordinary job is governed only by its endpoint lifecycle."""
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    info.status_property.first_ready_time = 1.0
    info.status_property.service_ready_now = True
    info.first_consecutive_failure_time = 10.0
    _stamp_protocol_v2_fill(info)
    handle = _protocol_v2_handle(info)
    records = {info.cluster_name: {'name': info.cluster_name, 'handle': handle}}
    info.probe = mock.Mock(return_value=(info, endpoint_ready, 40.0))

    mgr = _make_manager()
    mgr._is_pool = False
    mgr._uptime = 1.0
    mgr._update_recovery_required = False
    mgr._tick_version_spec_cache = {}
    mgr._resolve_probe_urls = mock.Mock(
        return_value={info.replica_id: 'http://10.0.0.1:8080'})
    mgr._get_readiness_path = mock.Mock(return_value='/health')
    mgr._get_post_data = mock.Mock(return_value=None)
    mgr._get_readiness_timeout_seconds = mock.Mock(return_value=15)
    mgr._get_readiness_headers = mock.Mock(return_value=None)
    mgr._is_interruptible_replica = mock.Mock(return_value=False)
    mgr._consecutive_failure_threshold_timeout = mock.Mock(return_value=30)
    mgr._persist_replicas = mock.Mock()
    mgr._terminate_replica = mock.Mock()
    mgr._changed_only_readiness_persistence = False
    mgr._handle_job_status_results = mock.Mock()

    # Model an already-exited detached job. If exact status were consulted it
    # would be terminal, but ordinary Kubernetes liveness must not consult it.
    backend = replica_managers.backends.CloudVmRayBackend()
    backend.get_job_status = mock.Mock(
        return_value={1: job_lib.JobStatus.FAILED})
    backend.get_job_status_with_system_recovery = mock.Mock(
        side_effect=AssertionError('ordinary row used recovery status'))

    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[info]), \
         mock.patch.object(replica_managers.serve_state,
                           'get_specs',
                           return_value={1: mock.Mock()}), \
         mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos_from_ids',
                           return_value={1: info}), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_clusters_from_names',
                           return_value=records), \
         mock.patch.object(replica_managers.backends,
                           'CloudVmRayBackend',
                           return_value=backend), \
         mock.patch.object(replica_managers.provider_phase,
                           'join_provider_phase',
                           return_value=contextlib.nullcontext()):
        mgr._fetch_job_status()
        snapshot = mgr._probe_all_replicas_with_snapshot(
            [info], phase_admission=mock.sentinel.phase_admission)

    backend.get_job_status.assert_not_called()
    backend.get_job_status_with_system_recovery.assert_not_called()
    mgr._handle_job_status_results.assert_not_called()
    assert info.status_property.service_ready_now is endpoint_ready
    if expected_termination:
        mgr._terminate_replica.assert_called_once_with(
            info.replica_id, sync_down_logs=True, replica_drain_delay_seconds=0)
        assert snapshot == [info]
    else:
        mgr._terminate_replica.assert_not_called()
        assert info.first_consecutive_failure_time is None
        assert snapshot == [info]


def test_exact_non_kubernetes_job_status_does_not_hold_kubernetes_phase():
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='-',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    handle = mock.Mock(spec=replica_managers.backends.CloudVmRayResourceHandle)
    handle.cluster_name = info.cluster_name
    handle.launched_resources = mock.Mock(cloud=clouds.GCP(),
                                          region='us-central1')
    records = {
        info.cluster_name: {
            'name': info.cluster_name,
            'handle': handle,
        }
    }
    mgr = _make_manager()
    mgr._is_pool = True
    backend = mock.Mock()
    status_started = threading.Event()
    release_status = threading.Event()

    def _get_job_status(exact_handle, *_args, **_kwargs):
        assert exact_handle is handle
        status_started.set()
        assert release_status.wait(timeout=5)
        return {1: job_lib.JobStatus.RUNNING}

    backend.get_job_status.side_effect = _get_job_status
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[info]), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_clusters_from_names',
                           return_value=records), \
         mock.patch.object(replica_managers.backends,
                           'CloudVmRayBackend',
                           return_value=backend):
        fetch_thread = threading.Thread(target=mgr._fetch_job_status)
        fetch_thread.start()
        assert status_started.wait(timeout=5)
        try:
            with replica_managers.provider_phase.try_provider_phase(
                    replica_managers.provider_phase.ProviderPhaseMode.V2_FENCED
            ):
                pass
        finally:
            release_status.set()
            fetch_thread.join(timeout=5)

    assert not fetch_thread.is_alive()
    backend.get_job_status.assert_called_once_with(handle, [1],
                                                   stream_logs=False)


def test_non_kubernetes_job_status_error_takes_phase_before_manager_lock():
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='-',
                                        is_spot=True,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    handle = mock.Mock(spec=replica_managers.backends.CloudVmRayResourceHandle)
    handle.cluster_name = info.cluster_name
    handle.launched_resources = mock.Mock(cloud=clouds.GCP(),
                                          region='us-central1')
    records = {
        info.cluster_name: {
            'name': info.cluster_name,
            'handle': handle,
        }
    }
    mgr = _make_manager()
    mgr._is_pool = True
    backend = mock.Mock()
    status_raised = threading.Event()
    preemption_checked = threading.Event()
    errors = []

    def _get_job_status(*_args, **_kwargs):
        status_raised.set()
        raise exceptions.CommandError(255, 'get_job_status', 'ssh failed', None)

    backend.get_job_status.side_effect = _get_job_status

    def _handle_preemption(fresh):
        assert fresh is info
        lease = (replica_managers.provider_phase._PROVIDER_PHASE_GATE.
                 _current_lease())
        assert lease is not None
        assert lease.mode == (
            replica_managers.provider_phase.ProviderPhaseMode.AMBIENT_LEGACY)
        preemption_checked.set()
        return False

    mgr._handle_preemption = mock.Mock(side_effect=_handle_preemption)

    def _fetch():
        try:
            mgr._fetch_job_status()
        except BaseException as error:  # pylint: disable=broad-exception-caught
            errors.append(error)

    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[info]), \
         mock.patch.object(replica_managers.serve_state,
                           'get_replica_info_from_id',
                           return_value=info), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_clusters_from_names',
                           return_value=records), \
         mock.patch.object(replica_managers.backends,
                           'CloudVmRayBackend',
                           return_value=backend):
        fetch_thread = threading.Thread(target=_fetch)
        try:
            with replica_managers.provider_phase.provider_phase(
                    replica_managers.provider_phase.ProviderPhaseMode.V2_FENCED
            ):
                fetch_thread.start()
                assert status_raised.wait(timeout=5)
                gate = replica_managers.provider_phase._PROVIDER_PHASE_GATE
                with gate._condition:
                    assert gate._condition.wait_for(lambda: any(
                        waiter.mode == replica_managers.provider_phase.
                        ProviderPhaseMode.AMBIENT_LEGACY
                        for waiter in gate._queue),
                                                    timeout=5)
                # Error reduction is waiting for its phase, not holding the
                # manager lock beneath that wait.
                assert mgr.lock.acquire(blocking=False)
                mgr.lock.release()
                assert not preemption_checked.is_set()
            # Exiting V2 above admits the queued error reducer.
            assert preemption_checked.wait(timeout=5)
        finally:
            if fetch_thread.ident is not None:
                fetch_thread.join(timeout=5)

    assert not errors
    assert not fetch_thread.is_alive()
    mgr._handle_preemption.assert_called_once_with(info)


def test_v2_cloud_liveness_uid_mismatch_is_unknown_not_preempted():
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    _stamp_protocol_v2_fill(info)
    handle = _protocol_v2_handle(info)
    mgr = _make_manager()
    provider_fence = mock.MagicMock()
    provider_fence.return_value.__enter__.side_effect = (
        exceptions.KubernetesPhysicalClusterIdentityError('UID mismatch'))

    with mock.patch.object(
            replica_managers.global_user_state,
            'get_handle_from_cluster_name',
            return_value=handle), \
         mock.patch.object(replica_managers.kubernetes_adaptor,
                           'physical_cluster_uid_fence', provider_fence), \
         mock.patch.object(
             replica_managers.backend_utils,
             'query_cluster_instance_statuses') as query_statuses:
        assert mgr._cloud_instance_looks_alive(info) is None

    query_statuses.assert_not_called()
    assert info.status_property.preempted is False


def test_v2_forced_preemption_uid_mismatch_never_refreshes_or_marks_loss():
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='8080',
                                        is_spot=False,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    _stamp_protocol_v2_fill(info)
    handle = _protocol_v2_handle(info)
    mgr = _make_manager()
    mgr._spot_placer = mock.Mock()
    mgr._is_interruptible_replica = mock.Mock(return_value=True)
    provider_fence = mock.MagicMock()
    provider_fence.return_value.__enter__.side_effect = (
        exceptions.KubernetesPhysicalClusterIdentityError('UID mismatch'))

    with mock.patch.object(
            replica_managers.global_user_state,
            'get_handle_from_cluster_name',
            return_value=handle), \
         mock.patch.object(replica_managers.kubernetes_adaptor,
                           'physical_cluster_uid_fence', provider_fence), \
         mock.patch.object(
             replica_managers.backend_utils,
             'refresh_cluster_status_handle') as refresh_status, \
         pytest.raises(exceptions.KubernetesPhysicalClusterIdentityError,
                       match='UID mismatch'):
        mgr._handle_preemption(info)

    refresh_status.assert_not_called()
    assert info.status_property.preempted is False


def _system_recovery_replica(
    replica_id,
    disposition=recovery_state.SystemRecoveryDisposition.ORDINARY,
):
    info = replica_managers.ReplicaInfo(replica_id, f'svc-{replica_id}', '8080',
                                        False, None, 1, None)
    info.system_recovery_disposition = disposition
    if disposition == recovery_state.SystemRecoveryDisposition.CAPABLE:
        info.system_recovery = recovery_state.ReplicaSystemRecovery(
            state=recovery_state.ControllerRecoveryState.ARMED,
            job_id=9,
            capability=recovery_state.SYSTEM_RECOVERY_CAPABILITY,
            original_attempt_id='11111111-1111-4111-8111-111111111111',
            replacement_attempt_id=None,
            node_boot_id='boot-id',
            remote_phase=recovery_state.RemoteRecoveryPhase.ARMED,
            occurrence_count=0,
            armed_at=10.0)
    return info


def test_invalid_recovery_job_rows_terminate_in_v2_then_ambient_phases():
    ordinary = _system_recovery_replica(
        2, recovery_state.SystemRecoveryDisposition.CANDIDATE)
    fenced = _system_recovery_replica(
        1, recovery_state.SystemRecoveryDisposition.CANDIDATE)
    _stamp_protocol_v2_fill(fenced)
    for info in (ordinary, fenced):
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.service_job_id = 0
    ordinary_handle = mock.Mock(
        spec=replica_managers.backends.CloudVmRayResourceHandle)
    ordinary_handle.cluster_name = ordinary.cluster_name
    ordinary_handle.launched_resources = mock.Mock(cloud=clouds.AWS(),
                                                   region='us-east-1')
    fenced_handle = _protocol_v2_handle(fenced)
    records = {
        ordinary.cluster_name: {
            'name': ordinary.cluster_name,
            'handle': ordinary_handle,
        },
        fenced.cluster_name: {
            'name': fenced.cluster_name,
            'handle': fenced_handle,
        },
    }
    mgr = _make_manager()
    mgr._is_pool = False
    events = []

    @contextlib.contextmanager
    def _phase(mode):
        events.append(f'{mode.value}-enter')
        admission = mock.Mock(mode=mode)
        try:
            yield admission
        finally:
            events.append(f'{mode.value}-exit')

    @contextlib.contextmanager
    def _batch(representatives, *, phase_admission):
        del phase_admission
        assert list(representatives) == [('phx', 'phx-uid')]
        events.append('batch-enter')
        try:
            yield {}
        finally:
            events.append('batch-exit')

    mgr._terminate_replica = mock.Mock(side_effect=lambda replica_id, **_kwargs:
                                       events.append(f'terminate-{replica_id}'))
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[ordinary, fenced]), \
         mock.patch.object(replica_managers.serve_state,
                           'get_replica_info_from_id',
                           side_effect=lambda _service_name, replica_id:
                           fenced if replica_id == 1 else ordinary), \
         mock.patch.object(replica_managers.global_user_state,
                           'get_clusters_from_names',
                           return_value=records), \
         mock.patch.object(replica_managers.provider_phase,
                           'provider_phase',
                           side_effect=_phase), \
         mock.patch.object(
             replica_managers.reserved_capacity,
             'protocol_v2_provider_batch_fences',
             side_effect=_batch):
        mgr._fetch_job_status()

    assert events == [
        'v2-fenced-enter', 'batch-enter', 'terminate-1', 'batch-exit',
        'v2-fenced-exit', 'ambient-legacy-enter', 'terminate-2',
        'ambient-legacy-exit'
    ]


def test_system_recovery_process_guards_are_pruned_to_live_dispositions():
    manager = _make_manager()
    manager._candidate_release_monotonic_deadlines = {
        1: 101.0,
        2: 102.0,
        3: 103.0,
        99: 199.0,
    }
    manager._system_recovery_status_initialized = {1, 2, 3, 99}
    candidate = _system_recovery_replica(
        1, recovery_state.SystemRecoveryDisposition.CANDIDATE)
    capable = _system_recovery_replica(
        2, recovery_state.SystemRecoveryDisposition.CAPABLE)
    ordinary = _system_recovery_replica(3)

    manager._prune_system_recovery_process_guards(
        [candidate, capable, ordinary])

    assert manager._candidate_release_monotonic_deadlines == {1: 101.0}
    assert manager._system_recovery_status_initialized == {2}


def test_candidate_guard_is_dropped_on_concurrent_capable_promotion():
    manager = _make_manager()
    manager._candidate_release_monotonic_deadlines = {1: 101.0}
    candidate = _system_recovery_replica(
        1, recovery_state.SystemRecoveryDisposition.CANDIDATE)
    promoted = _system_recovery_replica(
        1, recovery_state.SystemRecoveryDisposition.CAPABLE)
    manager._patch_system_recovery_with_latest = mock.Mock(
        return_value=promoted)

    updated, off_route, teardown = manager._reduce_candidate_probe(
        candidate,
        succeeded=True,
        probe_started_at=100.0,
        probe_monotonic_started_at=100.0,
        exact_job_nonterminal=True,
        exact_detail_absent=True)

    assert updated is promoted
    assert off_route is True
    assert teardown is False
    assert not manager._candidate_release_monotonic_deadlines


def test_capable_status_guard_is_dropped_on_concurrent_exhaustion():
    manager = _make_manager()
    capable = _system_recovery_replica(
        2, recovery_state.SystemRecoveryDisposition.CAPABLE)
    exhausted = dataclasses.replace(
        capable.system_recovery,
        state=recovery_state.ControllerRecoveryState.EXHAUSTED,
        completed_at=100.0)
    capable.system_recovery = exhausted
    manager._system_recovery_status_initialized = {2}
    manager._patch_system_recovery_with_latest = mock.Mock(return_value=capable)

    _, reduction = manager._reduce_capable_probe(capable,
                                                 succeeded=False,
                                                 probe_started_at=100.0)

    assert reduction is None
    assert manager._system_recovery_status_initialized == set()


def _remote_recovery_detail(
    phase: job_lib.JobSystemRecoveryPhase = job_lib.JobSystemRecoveryPhase.
    ARMED,
) -> job_lib.JobSystemRecoveryInfo:
    replacement_attempt_id = None
    event_id = None
    reason = None
    occurred_at = None
    deadline_at = None
    occurrence_count = 0
    if phase != job_lib.JobSystemRecoveryPhase.ARMED:
        replacement_attempt_id = '22222222-2222-4222-8222-222222222222'
        event_id = '33333333-3333-4333-8333-333333333333'
        reason = 'RAY_NODE_OOM'
        occurred_at = 20.0
        deadline_at = 140.0
        occurrence_count = 1
    return job_lib.JobSystemRecoveryInfo(
        capability=recovery_state.SYSTEM_RECOVERY_CAPABILITY,
        phase=phase,
        original_attempt_id='11111111-1111-4111-8111-111111111111',
        replacement_attempt_id=replacement_attempt_id,
        task_index=0,
        node_boot_id='boot-id',
        occurrence_count=occurrence_count,
        armed_at=10.0,
        updated_at=30.0,
        event_id=event_id,
        reason=reason,
        occurred_at=occurred_at,
        deadline_at=deadline_at)


def test_route_issuance_evidence_must_exactly_match_armed_attempt():
    manager = _make_manager()
    manager._system_recovery_route_epoch = (
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
    info = _system_recovery_replica(
        2, recovery_state.SystemRecoveryDisposition.CAPABLE)
    info.service_job_id = 9

    assert manager._system_recovery_route_evidence_matches(
        info, job_lib.JobStatus.RUNNING, _remote_recovery_detail(),
        job_lib.JobSystemRecoveryDetailStatus.PRESENT)
    assert not manager._system_recovery_route_evidence_matches(
        info, job_lib.JobStatus.RUNNING,
        _remote_recovery_detail(job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED),
        job_lib.JobSystemRecoveryDetailStatus.PRESENT)
    assert not manager._system_recovery_route_evidence_matches(
        info, job_lib.JobStatus.FAILED, _remote_recovery_detail(),
        job_lib.JobSystemRecoveryDetailStatus.PRESENT)


def test_recovered_route_requires_exact_replacement_attempt_evidence():
    manager = _make_manager()
    manager._system_recovery_route_epoch = (
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
    info = _system_recovery_replica(
        2, recovery_state.SystemRecoveryDisposition.CAPABLE)
    info.service_job_id = 9
    assert info.system_recovery is not None
    info.system_recovery = dataclasses.replace(
        info.system_recovery,
        state=recovery_state.ControllerRecoveryState.RECOVERED,
        remote_phase=recovery_state.RemoteRecoveryPhase.RETRY_SUBMITTED,
        occurrence_count=1,
        replacement_attempt_id='22222222-2222-4222-8222-222222222222',
        event_id='33333333-3333-4333-8333-333333333333',
        reason='RAY_NODE_OOM',
        started_at=20.0,
        deadline=140.0,
        retry_submitted_adopted_at=25.0,
        completed_at=30.0)

    detail = _remote_recovery_detail(
        job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED)
    assert manager._system_recovery_route_evidence_matches(
        info, job_lib.JobStatus.RUNNING, detail,
        job_lib.JobSystemRecoveryDetailStatus.PRESENT)
    mismatched = dataclasses.replace(
        detail, replacement_attempt_id='44444444-4444-4444-8444-444444444444')
    assert not manager._system_recovery_route_evidence_matches(
        info, job_lib.JobStatus.RUNNING, mismatched,
        job_lib.JobSystemRecoveryDetailStatus.PRESENT)


def test_route_registry_prunes_recreated_numeric_replica_identity():
    manager = _make_manager()
    manager._system_recovery_route_epoch = (
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
    manager._system_recovery_route_registry = (
        system_recovery_route_lease.ManagerRouteLeaseRegistry(
            clock=lambda: 1.0))
    old = _system_recovery_replica(
        2, recovery_state.SystemRecoveryDisposition.CAPABLE)
    generation = manager._system_recovery_route_generation(old)
    assert generation is not None
    assert manager._route_lease_registry().issue(2, generation,
                                                 'http://replica:8080',
                                                 '/ready', None, None, 0.0)
    replacement = _system_recovery_replica(
        2, recovery_state.SystemRecoveryDisposition.CAPABLE)
    assert replacement.replica_record_id != old.replica_record_id
    manager._route_lease_registry().prune({2: replacement.replica_record_id})
    assert manager._route_lease_registry().marker(2, generation,
                                                  'http://replica:8080') is None


def test_route_prober_connector_does_not_queue_targets_above_aiohttp_default(
        monkeypatch):
    manager = _make_manager()
    manager._ownership_lost = threading.Event()
    target_count = 101
    targets = [
        types.SimpleNamespace(method='GET',
                              probe_url=f'http://replica-{index}:8080/ready',
                              headers=None,
                              post_data=None) for index in range(target_count)
    ]
    results = []

    class _Registry:

        def probe_targets(self):
            return targets

        def record_probe_result(self, target, *, request_started_at, succeeded):
            results.append((target, request_started_at, succeeded))
            if len(results) == target_count:
                manager._ownership_lost.set()

    registry = _Registry()
    manager._route_lease_registry = lambda: registry
    concurrency = {'current': 0, 'peak': 0}

    class _Response:

        status = 200

        def __init__(self, session):
            self._session = session

        async def __aenter__(self):
            await self._session.semaphore.acquire()
            concurrency['current'] += 1
            concurrency['peak'] = max(concurrency['peak'],
                                      concurrency['current'])
            if concurrency['current'] == target_count:
                self._session.all_started.set()
            await asyncio.wait_for(self._session.all_started.wait(), timeout=1)
            return self

        async def __aexit__(self, *_args):
            concurrency['current'] -= 1
            self._session.semaphore.release()

    class _Session:

        def __init__(self, *, connector):
            assert connector.limit == (replica_managers.serve_constants.
                                       SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS)
            self._connector = connector
            self.semaphore = asyncio.Semaphore(connector.limit)
            self.all_started = asyncio.Event()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            await self._connector.close()

        def request(self, *_args, **_kwargs):
            return _Response(self)

    monkeypatch.setattr(replica_managers.aiohttp, 'ClientSession', _Session)

    asyncio.run(manager._system_recovery_route_probe_loop())

    assert concurrency['peak'] == target_count
    assert len(results) == target_count
    assert all(succeeded for _, _, succeeded in results)


class TestOrderedRouteIssuanceWorker:

    _ROUTE_URL = 'http://10.0.0.2:8080'

    @staticmethod
    def _ready_info(replica_id, *, capable):
        disposition = (recovery_state.SystemRecoveryDisposition.CAPABLE
                       if capable else
                       recovery_state.SystemRecoveryDisposition.ORDINARY)
        info = _system_recovery_replica(replica_id, disposition)
        info.service_job_id = 9
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.first_ready_time = 1.0
        return info

    @staticmethod
    def _routable_reduction(info):
        return recovery_state.RecoveryReduction(
            state=info.system_recovery,
            changed=False,
            force_off_route=False,
            clear_probe_failure_window=False,
            mark_ready=True,
            schedule_legacy_teardown=False)

    def _manager(self, capable):
        manager = _make_manager()
        manager._is_pool = False
        manager._uptime = 1.0
        manager._system_recovery_route_epoch = (
            'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
        manager._system_recovery_status_initialized = {capable.replica_id}
        manager._system_recovery_route_registry = (
            system_recovery_route_lease.ManagerRouteLeaseRegistry(
                clock=lambda: 150.0))
        manager._get_readiness_path = mock.Mock(return_value='/health')
        manager._get_post_data = mock.Mock(return_value=None)
        manager._get_readiness_timeout_seconds = mock.Mock(return_value=15)
        manager._get_readiness_headers = mock.Mock(return_value=None)
        manager._is_interruptible_replica = mock.Mock(return_value=False)
        manager._consecutive_failure_threshold_timeout = mock.Mock(
            return_value=1000)
        manager._reduce_capable_probe = mock.Mock(
            side_effect=lambda info, **_kwargs:
            (info, self._routable_reduction(info)))
        manager._reconcile_system_recovery_status = mock.Mock(
            return_value=False)
        manager._persist_replicas = mock.Mock()
        return manager

    def _run(self, manager, infos, capable, status_result):
        handle = object()
        capable.handle = mock.Mock(return_value=handle)
        manager._resolve_probe_urls = mock.Mock(
            return_value={info.replica_id: self._ROUTE_URL for info in infos})
        spec = mock.Mock(readiness_path='/health',
                         post_data=None,
                         readiness_headers=None)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=infos), \
             mock.patch.object(replica_managers.serve_state,
                               'get_specs',
                               return_value={1: spec}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_clusters_from_names',
                 return_value={capable.cluster_name: {'handle': handle}}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=capable), \
             mock.patch.object(
                 replica_managers.backends.CloudVmRayBackend,
                 'get_job_status_with_system_recovery',
                 side_effect=status_result) as status_fetch, \
             mock.patch.object(replica_managers,
                               'time',
                               mock.Mock(wraps=time)) as manager_time:
            manager_time.monotonic.return_value = 1.0
            result = manager._probe_all_replicas()
        return result, status_fetch

    def test_later_fast_worker_issues_before_earlier_future_drains(self):
        ordinary = self._ready_info(1, capable=False)
        capable = self._ready_info(2, capable=True)
        manager = self._manager(capable)
        issued = threading.Event()
        ordering = []
        ordinary_observation = {'issued_before_return': False}
        registry = manager._route_lease_registry()
        original_issue = registry.issue

        def _issue(*args, **kwargs):
            ordering.append('issue')
            result = original_issue(*args, **kwargs)
            issued.set()
            return result

        registry.issue = mock.Mock(side_effect=_issue)

        def _ordinary_probe(*_args, request_started_callback=None, **_kwargs):
            assert request_started_callback is not None
            request_started_callback(2.0)
            ordinary_observation['issued_before_return'] = issued.wait(1)
            return ordinary, False, 200.0

        def _capable_probe(*_args, request_started_callback=None, **_kwargs):
            assert request_started_callback is not None
            request_started_callback(100.0)
            ordering.append('readiness_response')
            return capable, True, 200.0

        ordinary.probe = mock.Mock(side_effect=_ordinary_probe)
        capable.probe = mock.Mock(side_effect=_capable_probe)

        def _status(*_args, **_kwargs):
            ordering.append('status')
            return ({
                9: job_lib.JobStatus.RUNNING
            }, {
                9: _remote_recovery_detail()
            }, {
                9: job_lib.JobSystemRecoveryDetailStatus.PRESENT
            })

        self._run(manager, [ordinary, capable], capable, _status)

        assert ordinary_observation['issued_before_return']
        assert ordering == ['readiness_response', 'status', 'issue']
        targets = registry.probe_targets()
        assert len(targets) == 1
        assert targets[0].replica_id == capable.replica_id
        # Registry time is 150. Submission time is 1; only the callback's
        # exact HTTP start (100 + the 60s lease) can still be admitted.
        assert capable.status_property.service_ready_now

    def test_adopted_replacement_issues_before_earlier_future_drains(self):
        ordinary = self._ready_info(1, capable=False)
        capable = self._ready_info(2, capable=True)
        assert capable.system_recovery is not None
        capable.system_recovery = dataclasses.replace(
            capable.system_recovery,
            state=recovery_state.ControllerRecoveryState.RETRY_SUBMITTED,
            remote_phase=recovery_state.RemoteRecoveryPhase.RETRY_SUBMITTED,
            replacement_attempt_id='22222222-2222-4222-8222-222222222222',
            event_id='33333333-3333-4333-8333-333333333333',
            reason='RAY_NODE_OOM',
            occurrence_count=1,
            started_at=20.0,
            deadline=240.0,
            retry_submitted_adopted_at=100.0)
        manager = self._manager(capable)
        issued = threading.Event()
        ordering = []
        ordinary_observation = {'issued_before_return': False}
        registry = manager._route_lease_registry()
        original_issue = registry.issue

        def _issue(*args, **kwargs):
            ordering.append('issue')
            result = original_issue(*args, **kwargs)
            issued.set()
            return result

        registry.issue = mock.Mock(side_effect=_issue)

        def _ordinary_probe(*_args, request_started_callback=None, **_kwargs):
            assert request_started_callback is not None
            request_started_callback(2.0)
            ordinary_observation['issued_before_return'] = issued.wait(1)
            return ordinary, False, 200.0

        def _replacement_probe(*_args,
                               request_started_callback=None,
                               **_kwargs):
            assert request_started_callback is not None
            request_started_callback(100.0)
            ordering.append('readiness_response')
            return capable, True, 200.0

        ordinary.probe = mock.Mock(side_effect=_ordinary_probe)
        capable.probe = mock.Mock(side_effect=_replacement_probe)

        def _status(*_args, **_kwargs):
            ordering.append('status')
            detail = _remote_recovery_detail(
                job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED)
            return ({
                9: job_lib.JobStatus.RUNNING
            }, {
                9: detail
            }, {
                9: job_lib.JobSystemRecoveryDetailStatus.PRESENT
            })

        def _adopt(_info, *_evidence):
            assert capable.system_recovery is not None
            capable.system_recovery = dataclasses.replace(
                capable.system_recovery,
                state=recovery_state.ControllerRecoveryState.RECOVERED,
                completed_at=201.0)
            return False

        manager._reconcile_system_recovery_status = mock.Mock(
            side_effect=_adopt)
        self._run(manager, [ordinary, capable], capable, _status)

        assert ordinary_observation['issued_before_return']
        assert ordering == ['readiness_response', 'status', 'issue']
        targets = registry.probe_targets()
        assert len(targets) == 1
        assert targets[0].generation.recovery_state == 'RECOVERED'
        assert capable.status_property.service_ready_now

    @pytest.mark.parametrize('invoke_start_callback', [False, True])
    def test_missing_start_or_malformed_status_cannot_issue(
            self, invoke_start_callback):
        capable = self._ready_info(2, capable=True)
        manager = self._manager(capable)

        def _probe(*_args, request_started_callback=None, **_kwargs):
            if invoke_start_callback:
                assert request_started_callback is not None
                request_started_callback(100.0)
            return capable, True, 200.0

        capable.probe = mock.Mock(side_effect=_probe)
        self._run(manager, [capable], capable, lambda *_args, **_kwargs: None)

        assert manager._route_lease_registry().probe_targets() == []
        assert not capable.status_property.service_ready_now

    @pytest.mark.parametrize(('adopted_at', 'expected_issued'), [(200.0, False),
                                                                 (100.0, True)])
    def test_retry_submitted_requires_probe_strictly_after_durable_adoption(
            self, adopted_at, expected_issued):
        capable = self._ready_info(2, capable=True)
        assert capable.system_recovery is not None
        capable.system_recovery = dataclasses.replace(
            capable.system_recovery,
            state=recovery_state.ControllerRecoveryState.RETRY_SUBMITTED,
            remote_phase=recovery_state.RemoteRecoveryPhase.RETRY_SUBMITTED,
            replacement_attempt_id='22222222-2222-4222-8222-222222222222',
            event_id='33333333-3333-4333-8333-333333333333',
            reason='RAY_NODE_OOM',
            occurrence_count=1,
            started_at=20.0,
            deadline=140.0,
            retry_submitted_adopted_at=adopted_at)
        manager = self._manager(capable)

        def _probe(*_args, request_started_callback=None, **_kwargs):
            assert request_started_callback is not None
            request_started_callback(100.0)
            return capable, True, 200.0

        capable.probe = mock.Mock(side_effect=_probe)

        def _adopt(_info, *_evidence):
            assert capable.system_recovery is not None
            capable.system_recovery = dataclasses.replace(
                capable.system_recovery,
                state=recovery_state.ControllerRecoveryState.RECOVERED,
                completed_at=201.0,
                retry_submitted_adopted_at=adopted_at)
            return False

        manager._reconcile_system_recovery_status = mock.Mock(
            side_effect=_adopt)
        detail = _remote_recovery_detail(
            job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED)
        self._run(
            manager, [capable], capable, lambda *_args, **_kwargs: ({
                9: job_lib.JobStatus.RUNNING
            }, {
                9: detail
            }, {
                9: job_lib.JobSystemRecoveryDetailStatus.PRESENT
            }))

        assert bool(manager._route_lease_registry().probe_targets()) is (
            expected_issued)
        assert capable.status_property.service_ready_now is expected_issued


class TestProbeRouteSuspensionTransaction:

    _ROUTE_URL = 'http://10.0.0.1:8080'
    _SERVICE_HASH = 'incarnation-a'
    _CONTROLLER_OWNER = (123, '10.0.0.10')

    @staticmethod
    def _recovered_info(replica_id):
        info = _system_recovery_replica(
            replica_id, recovery_state.SystemRecoveryDisposition.CAPABLE)
        assert info.system_recovery is not None
        info.system_recovery = dataclasses.replace(
            info.system_recovery,
            state=recovery_state.ControllerRecoveryState.RECOVERED,
            remote_phase=recovery_state.RemoteRecoveryPhase.RETRY_SUBMITTED,
            replacement_attempt_id=(
                f'22222222-2222-4222-8222-{replica_id:012d}'),
            occurrence_count=1,
            event_id=f'33333333-3333-4333-8333-{replica_id:012d}',
            reason='RAY_NODE_OOM',
            started_at=20.0,
            deadline=140.0,
            retry_submitted_adopted_at=25.0,
            completed_at=30.0)
        info.service_job_id = 9
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.first_ready_time = 1.0
        info.status_property.service_ready_now = True
        info.probe = mock.Mock(return_value=(info, False, 100.0))
        return info

    @staticmethod
    def _off_route_reduction(info):
        return recovery_state.RecoveryReduction(
            state=info.system_recovery,
            changed=False,
            force_off_route=True,
            clear_probe_failure_window=False,
            mark_ready=False,
            schedule_legacy_teardown=False)

    def _manager(self, infos):
        manager = _make_manager()
        manager._is_pool = False
        manager._uptime = 1.0
        manager._service_hash = self._SERVICE_HASH
        manager._controller_owner = self._CONTROLLER_OWNER
        manager._system_recovery_route_epoch = (
            'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
        manager._system_recovery_status_initialized = {
            info.replica_id for info in infos
        }
        manager._system_recovery_route_registry = (
            system_recovery_route_lease.ManagerRouteLeaseRegistry(
                clock=lambda: 10.0))
        manager._resolve_probe_urls = mock.Mock(
            return_value={info.replica_id: self._ROUTE_URL for info in infos})
        manager._get_readiness_path = mock.Mock(return_value='/health')
        manager._get_post_data = mock.Mock(return_value=None)
        manager._get_readiness_timeout_seconds = mock.Mock(return_value=15)
        manager._get_readiness_headers = mock.Mock(return_value=None)
        manager._is_interruptible_replica = mock.Mock(return_value=False)
        manager._consecutive_failure_threshold_timeout = mock.Mock(
            return_value=1000)
        manager._reduce_capable_probe = mock.Mock(
            side_effect=lambda info, **_kwargs:
            (info, self._off_route_reduction(info)))
        return manager

    def _owner_record(self):
        return {
            'hash': self._SERVICE_HASH,
            'controller_pid': self._CONTROLLER_OWNER[0],
            'controller_ip': self._CONTROLLER_OWNER[1],
            'lifecycle_epoch': 1,
            'status': replica_managers.serve_state.ServiceStatus.READY,
        }

    def _durable_copy(self, info):
        copied = self._recovered_info(info.replica_id)
        copied.replica_record_id = info.replica_record_id
        copied.system_recovery_revision = info.system_recovery_revision
        return copied

    def _off_route_copy(self, info):
        off_route = self._durable_copy(info)
        off_route.status_property.service_ready_now = False
        return off_route

    def _activate_route(self, manager, info):
        registry = manager._route_lease_registry()
        generation = manager._system_recovery_route_generation(info)
        assert generation is not None
        assert registry.issue(info.replica_id, generation, self._ROUTE_URL,
                              '/health', None, None, 10.0)
        target = next(target for target in registry.probe_targets()
                      if target.replica_id == info.replica_id)
        registry.record_probe_result(target,
                                     request_started_at=10.0,
                                     succeeded=True)
        assert registry.marker(info.replica_id, generation,
                               self._ROUTE_URL) is not None
        assert registry.heartbeat_payload()['entries']
        return registry, generation

    def _run_probe(self, manager, infos, persist, readback_infos=None):
        if readback_infos is None:
            readback_infos = {}
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=infos), \
             mock.patch.object(replica_managers.serve_state,
                               'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_clusters_from_names',
                               return_value={}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=self._owner_record()), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value=readback_infos), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replicas',
                               side_effect=persist):
            return manager._probe_all_replicas()

    def test_exception_before_batch_commit_restores_exact_route(self):
        info = self._recovered_info(1)
        durable = self._durable_copy(info)
        manager = self._manager([info])
        registry, generation = self._activate_route(manager, info)
        marker_before = registry.marker(1, generation, self._ROUTE_URL)
        assert marker_before is not None

        def _persist(_service_name, updates, **_kwargs):
            assert updates == [(1, info)]
            assert registry.marker(1, generation, self._ROUTE_URL) is None
            assert registry.heartbeat_payload()['entries'] == []
            assert registry.probe_targets() == []
            raise RuntimeError('database unavailable')

        with mock.patch.object(registry,
                               'suspend_record',
                               wraps=registry.suspend_record) as suspend, \
             pytest.raises(RuntimeError):
            self._run_probe(manager, [info], _persist, {1: durable})

        suspend.assert_called_once_with(1, info.replica_record_id)
        assert registry.marker(1, generation, self._ROUTE_URL) == marker_before
        assert len(registry.heartbeat_payload()['entries']) == 1
        assert len(registry.probe_targets()) == 1
        assert not registry.is_retired(1, generation)

    def test_false_batch_result_permanently_retires_route(self):
        info = self._recovered_info(1)
        manager = self._manager([info])
        registry, generation = self._activate_route(manager, info)

        def _persist(_service_name, _updates, **_kwargs):
            assert registry.marker(1, generation, self._ROUTE_URL) is None
            return False

        with pytest.raises(RuntimeError, match='ownership changed'):
            self._run_probe(manager, [info], _persist,
                            {1: self._durable_copy(info)})

        assert registry.marker(1, generation, self._ROUTE_URL) is None
        assert registry.is_retired(1, generation)

    def test_commit_then_raise_batch_permanently_retires_route(self):
        info = self._recovered_info(1)
        manager = self._manager([info])
        registry, generation = self._activate_route(manager, info)

        def _persist(_service_name, _updates, **_kwargs):
            assert registry.marker(1, generation, self._ROUTE_URL) is None
            raise RuntimeError('connection lost after commit')

        with pytest.raises(RuntimeError, match='connection lost after commit'):
            # The probe mutates this object before the DB call, modeling the
            # committed row returned by the ambiguity readback.
            self._run_probe(manager, [info], _persist, {1: info})

        assert registry.marker(1, generation, self._ROUTE_URL) is None
        assert registry.is_retired(1, generation)

    def test_exception_before_single_commit_restores_proven_route(self):
        durable = self._recovered_info(1)
        update = self._off_route_copy(durable)
        manager = self._manager([durable])
        registry, generation = self._activate_route(manager, durable)
        marker_before = registry.marker(1, generation, self._ROUTE_URL)

        def _persist(*_args, **_kwargs):
            assert registry.marker(1, generation, self._ROUTE_URL) is None
            raise RuntimeError('write did not commit')

        with mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica',
                               side_effect=_persist), \
             mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=self._owner_record()), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: durable}), \
             pytest.raises(RuntimeError, match='write did not commit'):
            manager._persist_replica(1, update)

        assert registry.marker(1, generation, self._ROUTE_URL) == marker_before
        assert not registry.is_retired(1, generation)

    @pytest.mark.parametrize('readback_case', [
        'missing',
        'changed_record',
        'off_route',
        'teardown',
        'malformed',
        'read_error',
        'owner_changed',
    ])
    def test_single_commit_ambiguity_retires_unproven_route(
            self, readback_case):
        durable = self._recovered_info(1)
        update = self._off_route_copy(durable)
        manager = self._manager([durable])
        registry, generation = self._activate_route(manager, durable)
        owner = self._owner_record()
        readback = {1: durable}
        readback_side_effect = None
        if readback_case == 'missing':
            readback = {}
        elif readback_case == 'changed_record':
            changed = self._durable_copy(durable)
            changed.replica_record_id = ('44444444-4444-4444-8444-444444444444')
            readback = {1: changed}
        elif readback_case == 'off_route':
            readback = {1: update}
        elif readback_case == 'teardown':
            teardown = self._durable_copy(durable)
            teardown.status_property.is_scale_down = True
            readback = {1: teardown}
        elif readback_case == 'malformed':
            readback = None
        elif readback_case == 'read_error':
            readback_side_effect = RuntimeError('readback failed')
        else:
            owner = {**owner, 'controller_pid': 999}

        with mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica',
                               side_effect=RuntimeError(
                                   'connection lost after possible commit')), \
             mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=owner), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value=readback,
                               side_effect=readback_side_effect), \
             pytest.raises(RuntimeError,
                           match='connection lost after possible commit'):
            manager._persist_replica(1, update)

        assert registry.marker(1, generation, self._ROUTE_URL) is None
        assert registry.is_retired(1, generation)

    def test_cancelled_ambiguity_owner_read_retires_route_and_reraises(self):
        durable = self._recovered_info(1)
        update = self._off_route_copy(durable)
        manager = self._manager([durable])
        registry, generation = self._activate_route(manager, durable)

        with mock.patch.object(
                replica_managers.serve_state,
                'add_or_update_replica',
                side_effect=RuntimeError('connection lost after write')), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_service_controller_owner',
                 side_effect=asyncio.CancelledError), \
             pytest.raises(asyncio.CancelledError):
            manager._persist_replica(1, update)

        assert registry.marker(1, generation, self._ROUTE_URL) is None
        assert registry.is_retired(1, generation)

    def test_cancelled_ambiguity_row_check_retires_route_and_reraises(self):
        durable = self._recovered_info(1)
        update = self._off_route_copy(durable)
        manager = self._manager([durable])
        registry, generation = self._activate_route(manager, durable)

        with mock.patch.object(
                replica_managers.serve_state,
                'add_or_update_replica',
                side_effect=RuntimeError('connection lost after write')), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_service_controller_owner',
                 return_value=self._owner_record()), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos_from_ids',
                 return_value={1: durable}), \
             mock.patch.object(
                 manager,
                 '_system_recovery_route_generation',
                 side_effect=asyncio.CancelledError), \
             pytest.raises(asyncio.CancelledError):
            manager._persist_replica(1, update)

        assert registry.marker(1, generation, self._ROUTE_URL) is None
        assert registry.is_retired(1, generation)

    def test_commit_then_raise_recovery_patch_retires_route(self):
        durable = self._recovered_info(1)
        update = self._durable_copy(durable)
        manager = self._manager([durable])
        registry, generation = self._activate_route(manager, durable)

        def _transition(info):
            info.status_property.service_ready_now = False
            return True

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=update), \
             mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=self._owner_record()), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: update}), \
             mock.patch.object(replica_managers.serve_state,
                               'patch_replica_system_recovery',
                               side_effect=RuntimeError(
                                   'connection lost after patch commit')), \
             pytest.raises(RuntimeError, match='after patch commit'):
            manager._patch_system_recovery_with_latest(1, _transition)

        assert registry.marker(1, generation, self._ROUTE_URL) is None
        assert registry.is_retired(1, generation)

    def test_commit_then_raise_delete_retires_route(self):
        durable = self._recovered_info(1)
        manager = self._manager([durable])
        registry, generation = self._activate_route(manager, durable)

        with mock.patch.object(replica_managers.serve_state,
                               'remove_replica',
                               side_effect=RuntimeError(
                                   'connection lost after delete commit')), \
             mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=self._owner_record()), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={}), \
             pytest.raises(RuntimeError, match='after delete commit'):
            manager._remove_replica(1, durable.replica_record_id)

        assert registry.marker(1, generation, self._ROUTE_URL) is None
        assert registry.is_retired(1, generation)

    def test_batch_delete_exception_before_commit_restores_proven_routes(self):
        durable = self._recovered_info(1)
        manager = self._manager([durable])
        registry, generation = self._activate_route(manager, durable)
        marker_before = registry.marker(1, generation, self._ROUTE_URL)

        with mock.patch.object(replica_managers.serve_state,
                               'remove_replicas',
                               side_effect=RuntimeError(
                                   'batch delete did not commit')), \
             mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=self._owner_record()), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: durable}), \
             pytest.raises(RuntimeError, match='did not commit'):
            manager._remove_replicas([durable])

        assert registry.marker(1, generation, self._ROUTE_URL) == marker_before
        assert not registry.is_retired(1, generation)

    def test_successful_batch_permanently_retires_route(self):
        info = self._recovered_info(1)
        manager = self._manager([info])
        registry, generation = self._activate_route(manager, info)

        def _persist(_service_name, updates, **_kwargs):
            assert updates == [(1, info)]
            assert registry.marker(1, generation, self._ROUTE_URL) is None
            assert registry.heartbeat_payload()['entries'] == []
            assert registry.probe_targets() == []
            return True

        with mock.patch.object(registry,
                               'suspend_record',
                               wraps=registry.suspend_record) as suspend:
            self._run_probe(manager, [info], _persist)

        suspend.assert_called_once_with(1, info.replica_record_id)
        assert registry.marker(1, generation, self._ROUTE_URL) is None
        assert registry.heartbeat_payload()['entries'] == []
        assert registry.probe_targets() == []
        assert registry.is_retired(1, generation)

    def test_exception_before_batch_restores_prior_suspension(self):
        first = self._recovered_info(1)
        second = self._recovered_info(2)
        manager = self._manager([first, second])
        registry, generation = self._activate_route(manager, first)
        marker_before = registry.marker(1, generation, self._ROUTE_URL)
        assert marker_before is not None
        manager._reduce_capable_probe.side_effect = [
            (first, self._off_route_reduction(first)),
            RuntimeError('second reduction failed'),
        ]
        persist = mock.Mock(return_value=True)

        with mock.patch.object(registry,
                               'suspend_record',
                               wraps=registry.suspend_record) as suspend, \
             pytest.raises(RuntimeError, match='second reduction failed'):
            self._run_probe(manager, [first, second], persist)

        persist.assert_not_called()
        suspend.assert_called_once_with(1, first.replica_record_id)
        assert registry.marker(1, generation, self._ROUTE_URL) == marker_before
        assert len(registry.heartbeat_payload()['entries']) == 1
        assert len(registry.probe_targets()) == 1
        assert not registry.is_retired(1, generation)


class TestLaunchCancellationWait:

    @staticmethod
    def _run(monkeypatch,
             launch_thread,
             *,
             on_sleep=None,
             forbid_wall_clock=False):
        manager = _make_manager()
        manager._is_pool = False
        manager._resource_scope = None
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        manager._launch_thread_pool[1] = launch_thread
        # Stop after the launch wait. Down-thread creation is exercised by
        # the cleanup tests below and would only obscure these timing checks.
        manager._down_thread_pool[1] = mock.Mock()
        manager._persist_replica = mock.Mock()
        info = replica_managers.ReplicaInfo(1, 'svc-1', '8080', False, None, 1,
                                            None)

        now = [0.0]
        sleeps = []

        def _sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds
            if on_sleep is not None:
                on_sleep(manager, len(sleeps))

        fake_time = mock.Mock(wraps=replica_managers.time)
        fake_time.monotonic.side_effect = lambda: now[0]
        fake_time.sleep.side_effect = _sleep
        if forbid_wall_clock:
            fake_time.time.side_effect = AssertionError('wall clock consulted')
        monkeypatch.setattr(replica_managers, 'time', fake_time)
        monkeypatch.setattr(replica_managers,
                            '_WAIT_LAUNCH_THREAD_TIMEOUT_SECONDS', 0.15)
        get_info = mock.Mock(return_value=info)
        monkeypatch.setattr(replica_managers.serve_state,
                            'get_replica_info_from_id', get_info)
        cancel = mock.Mock()
        monkeypatch.setattr(replica_managers.sdk, 'api_cancel', cancel)

        manager._terminate_replica(1,
                                   sync_down_logs=False,
                                   replica_drain_delay_seconds=0,
                                   is_scale_down=True)
        manager._persist_replica.assert_called_once_with(1, info)
        get_info.assert_called_once_with('svc', 1)
        launch_thread.join.assert_called_once_with()
        return manager, sleeps, cancel, fake_time

    def test_wait_uses_monotonic_clock_and_clamps_final_sleep(
            self, monkeypatch):
        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = True

        _, sleeps, cancel, fake_time = self._run(monkeypatch,
                                                 launch_thread,
                                                 forbid_wall_clock=True)

        assert sleeps == pytest.approx([0.1, 0.05])
        assert fake_time.monotonic.call_count == 4
        cancel.assert_not_called()

    def test_request_published_at_deadline_is_cancelled(self, monkeypatch):
        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = True

        def _publish_request(manager, sleep_count):
            if sleep_count == 2:
                manager._replica_to_request_id[1] = 'request-1'

        _, sleeps, cancel, fake_time = self._run(monkeypatch,
                                                 launch_thread,
                                                 on_sleep=_publish_request)

        assert sleeps == pytest.approx([0.1, 0.05])
        assert fake_time.monotonic.call_count == 3
        cancel.assert_called_once_with('request-1')

    def test_cancellation_acknowledgement_stops_wait(self, monkeypatch):
        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = True

        def _acknowledge(manager, _sleep_count):
            manager._replica_to_launch_cancelled.pop(1)

        _, sleeps, cancel, fake_time = self._run(monkeypatch,
                                                 launch_thread,
                                                 on_sleep=_acknowledge)

        assert sleeps == [0.1]
        assert fake_time.monotonic.call_count == 2
        cancel.assert_not_called()

    def test_launch_thread_completion_stops_without_sleep(self, monkeypatch):
        launch_thread = mock.Mock()
        launch_thread.is_alive.side_effect = [True, False]

        _, sleeps, cancel, fake_time = self._run(monkeypatch, launch_thread)

        assert not sleeps
        assert fake_time.monotonic.call_count == 1
        cancel.assert_not_called()


def _accepted_launch_result(
    replica_id,
    planned_capacity=1,
    funding=replica_managers._ReplicaLaunchFunding.PAID,
):
    return replica_managers._ReplicaLaunchResult(
        replica_id=replica_id,
        planned_capacity=planned_capacity,
        funding=funding)


def _record_launch(launched):
    """A _launch_replica side_effect that records the allocated replica id.

    Returns the explicit production acceptance result: the id allocator only
    advances past an id whose launch was actually enqueued.
    """

    def _side_effect(replica_id, _resources_override, **_kwargs):
        launched.append(replica_id)
        return _accepted_launch_result(replica_id)

    return _side_effect


def test_confirm_logical_bridge_capacity_is_durable_and_monotonic():
    mgr = _make_manager()
    mgr._uses_logical_replicas = True
    info = replica_managers.ReplicaInfo(replica_id=1,
                                        cluster_name='svc-1',
                                        replica_port='8080',
                                        is_spot=True,
                                        location=None,
                                        version=1,
                                        resources_override=None)
    persisted = []
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[info]), \
         mock.patch.object(mgr,
                           '_persist_replicas',
                           side_effect=persisted.extend):
        confirmed = mgr.confirm_logical_bridge_capacities({1: 8})

    assert confirmed == {1: 8}
    assert info.to_storage_dict()['replica_info_version'] == (
        replica_managers.ReplicaInfo._VERSION)
    assert info.planned_capacity == 8
    assert info.logical_bridge_capacity_verified is True
    assert persisted == [(1, info)]

    # A later smaller runtime observation must affect ready capacity only. It
    # cannot shrink the durable upper bound or cause another DB write.
    persisted.clear()
    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[info]), \
         mock.patch.object(mgr,
                           '_persist_replicas',
                           side_effect=persisted.extend):
        confirmed = mgr.confirm_logical_bridge_capacities({1: 4})

    assert confirmed == {1: 8}
    assert info.planned_capacity == 8
    assert not persisted


class TestReplicaIdSeededOnRecovery:
    """`_recover_replica_operations` must advance `_next_replica_id` past every
    persisted replica id.

    A fresh ReplicaManager starts `_next_replica_id` at 1. On a controller
    respawn (consolidation-mode pod restart re-running `_start`, or the
    in-place controller-respawn path) a brand-new ReplicaManager is built,
    resetting the allocator to 1 while replicas 1..N survive in the DB. The
    next `scale_up` would then reuse a live id, and `add_or_update_replica`
    (upsert on (service_name, replica_id)) would overwrite the surviving
    replica's persisted ReplicaInfo and re-launch its live serving cluster.
    Seeding the allocator from durable state prevents the collision.
    """

    def test_seeds_past_max_existing_id(self):
        mgr = _make_manager(next_replica_id=1)
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=[
                    _fake_replica_info(1),
                    _fake_replica_info(2),
                    _fake_replica_info(5),
                ]):
            mgr._recover_replica_operations()
        # max existing id is 5 -> next must be 6, NOT 1 (the reset value).
        assert mgr._next_replica_id == 6

    def test_first_run_keeps_id_at_one(self):
        # No replicas yet (first `up`, not a recovery) -> allocator unchanged.
        mgr = _make_manager(next_replica_id=1)
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=[]):
            mgr._recover_replica_operations()
        assert mgr._next_replica_id == 1


class TestScaleUpDoesNotClobberLiveReplica:
    """Defensive guard: `scale_up` must never allocate an id that still has a
    durable replica row, even if the allocator somehow drifted."""

    def test_allocates_fresh_id_normally(self):
        mgr = _make_manager(next_replica_id=6)
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids',
                return_value=set()), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up()
        assert launched == [6]
        assert mgr._next_replica_id == 7

    def test_skips_ids_with_existing_rows(self):
        # _next_replica_id points at 6, but 6 and 7 still have live rows;
        # 8 is free. scale_up must skip 6 and 7 and launch 8.
        mgr = _make_manager(next_replica_id=6)
        launched = []

        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids',
                return_value={6, 7}), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up()
        assert launched == [8]
        assert mgr._next_replica_id == 9

    def test_spot_policy_refresh_precedes_scale_up_selection(self):
        mgr = _make_manager(next_replica_id=1)
        mgr._spot_placer = mock.Mock()
        refreshed = False

        def _refresh():
            nonlocal refreshed
            refreshed = True

        def _launch(*_args, **_kwargs):
            assert refreshed
            return None

        mgr._spot_placer.refresh_workspace_policy.side_effect = _refresh
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_ids',
                return_value=set()), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr.scale_up()

        mgr._spot_placer.refresh_workspace_policy.assert_called_once_with()


class TestVersionSpecMemoizedPerProbeRound:
    """`_get_version_spec` reads each version's spec from the DB at most once
    per probe round.

    The readiness prober resolves the spec for every replica 4x per tick
    (path / post_data / headers / timeout), each a `serve_state.get_spec`
    (SQL SELECT + pickle.loads). The tick-scoped `_tick_version_spec_cache`
    collapses those 4*N reads into one per distinct version, and is reset
    each probe round so a rewritten spec is never served stale across rounds.
    """

    def test_memoizes_within_a_round_and_rereads_after_reset(self):
        mgr = _make_manager()
        calls = []

        def _get_spec(_service_name, version):
            calls.append(version)
            return mock.Mock()

        with mock.patch('sky.serve.replica_managers.serve_state.get_spec',
                        side_effect=_get_spec):
            # One round: 4 lookups for v1 + 2 for v2 -> 1 DB read per version.
            for _ in range(4):
                mgr._get_version_spec(1)
            for _ in range(2):
                mgr._get_version_spec(2)
            assert calls == [1, 2]
            # New round: the cache is reset (as _probe_all_replicas /
            # _replica_prober do) -> the version is re-read from the DB.
            mgr._tick_version_spec_cache = {}
            mgr._get_version_spec(1)
            assert calls == [1, 2, 1]

    def test_raises_when_version_missing(self):
        mgr = _make_manager()
        with mock.patch('sky.serve.replica_managers.serve_state.get_spec',
                        return_value=None):
            with pytest.raises(ValueError):
                mgr._get_version_spec(99)
        # A missing version must not be cached as a hit.
        assert 99 not in mgr._tick_version_spec_cache


def _capacity_error() -> exceptions.ResourcesUnavailableError:
    provider_error = RuntimeError('no provider capacity')
    provider_error.response = {
        'Error': {
            'Code': 'InsufficientInstanceCapacity',
            'Message': 'no provider capacity',
        }
    }
    attempt = exceptions.ResourcesUnavailableError('zone exhausted')
    attempt.__cause__ = provider_error
    return exceptions.ResourcesUnavailableError('no capacity',
                                                failover_history=[attempt])


def _quota_error() -> exceptions.ResourcesUnavailableError:
    provider_error = RuntimeError('provider quota exhausted')
    provider_error.response = {
        'Error': {
            'Code': 'VcpuLimitExceeded',
            'Message': 'provider quota exhausted',
        }
    }
    attempt = exceptions.ResourcesUnavailableError('region quota exhausted')
    attempt.__cause__ = provider_error
    return exceptions.ResourcesUnavailableError('no quota',
                                                failover_history=[attempt])


def test_launch_worker_uses_its_frozen_controller_config(monkeypatch):
    observed = {}
    monkeypatch.setattr(os, 'environ',
                        sky_context.ContextualEnviron(os.environ))
    manager = _make_manager()
    generation_one = config_utils.Config({
        'active_workspace': 'generation-one',
        'kubernetes': {
            'allowed_contexts': ['east']
        },
    })
    generation_two = config_utils.Config({
        'active_workspace': 'generation-two',
        'kubernetes': {
            'allowed_contexts': ['phx']
        },
    })
    completions: queue.SimpleQueue[int] = queue.SimpleQueue()
    completion_event = threading.Event()

    def _observe(*_args, launch_label, generation_guard, **_kwargs):
        observed[launch_label] = {
            'config': skypilot_config.to_dict(),
            'path': os.environ.get(skypilot_config.ENV_VAR_SKYPILOT_CONFIG),
            'context': sky_context.get(),
            'guard': generation_guard(),
        }

    def _make_worker(replica_id, label, config, path, expected_version):
        with skypilot_config.replace_skypilot_config_in_memory(config):
            monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, path)
            return replica_managers._ReplicaLaunchThread(
                target=(replica_managers.
                        launch_cluster_with_frozen_controller_config),
                replica_id=replica_id,
                completion_queue=completions,
                completion_event=completion_event,
                kwargs={
                    'launch_label': label,
                    'generation_guard': lambda: manager.
                                        _queued_launch_generation_decision(
                                            expected_version),
                    'frozen_controller_config': skypilot_config.to_dict(),
                    'frozen_controller_config_path': os.environ.get(
                        skypilot_config.ENV_VAR_SKYPILOT_CONFIG),
                })

    # Queue v1 while C1 is live, but do not start the worker yet.
    old_worker = _make_worker(1, 'old', generation_one,
                              '/tmp/generation-one.yaml', 1)
    # Publish the manager/config generation C2 before either worker starts.
    manager.latest_version = 2
    current_worker = _make_worker(2, 'current', generation_two,
                                  '/tmp/generation-two.yaml', 2)
    monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG,
                       '/tmp/generation-two.yaml')

    with skypilot_config.replace_skypilot_config_in_memory(generation_two), \
         mock.patch.object(replica_managers,
                           'launch_cluster',
                           side_effect=_observe):
        old_worker.start()
        current_worker.start()
        old_worker.join(timeout=2)
        current_worker.join(timeout=2)
        assert not old_worker.is_alive()
        assert not current_worker.is_alive()
        assert skypilot_config.get_active_workspace() == 'generation-two'
        assert os.environ[skypilot_config.ENV_VAR_SKYPILOT_CONFIG] == (
            '/tmp/generation-two.yaml')

    assert old_worker.exception is None
    assert current_worker.exception is None
    assert observed['old']['config']['active_workspace'] == 'generation-one'
    assert observed['old']['config']['kubernetes']['allowed_contexts'] == [
        'east'
    ]
    assert observed['old']['path'] == '/tmp/generation-one.yaml'
    assert observed['old']['context'] is not None
    assert observed['old']['guard'] == (False, 'manager-version-changed')
    assert observed['current']['config']['active_workspace'] == (
        'generation-two')
    assert observed['current']['config']['kubernetes']['allowed_contexts'] == [
        'phx'
    ]
    assert observed['current']['path'] == '/tmp/generation-two.yaml'
    assert observed['current']['context'] is not None
    assert observed['current']['guard'] == (True, 'authorized')
    assert {completions.get_nowait(), completions.get_nowait()} == {1, 2}


class TestLaunchClusterRetry:
    """`launch_cluster` must fail fast ONLY on resource availability
    (capacity) failures when `availability_max_retry` caps them; other
    (transient) errors must keep the `max_retry` in-place attempts."""

    def test_generic_bound_launch_uses_only_non_pool_admission(self, tmp_path):
        task = mock.MagicMock()
        resource = mock.MagicMock()
        resource.cloud = clouds.AWS()
        task.resources = {resource}
        prepared = types.SimpleNamespace(body=types.SimpleNamespace(
            model_dump_json=lambda: '{}'))
        submission_uuid = '11111111-1111-4111-8111-111111111111'
        fence = {
            replica_managers.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc',
            replica_managers.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_HASH_KEY: 'hash',
        }
        request_ids = thread_utils.ThreadSafeDict()
        with mock.patch.object(replica_managers,
                               '_build_replica_launch_task',
                               return_value=task), \
             mock.patch.object(replica_managers.sdk,
                               'prepare_launch_request',
                               return_value=prepared), \
             mock.patch.object(
                 replica_managers.sdk,
                 'submit_prepared_non_pool_launch_request',
                 return_value='request-id') as submit_generic, \
             mock.patch.object(
                 replica_managers.sdk,
                 'submit_prepared_ordinary_launch_request') as submit_ordinary, \
             mock.patch.object(replica_managers.sdk, 'launch') as launch, \
             mock.patch.object(replica_managers,
                               '_wait_for_bound_ordinary_launch') as wait:
            replica_managers.launch_cluster(
                replica_id=1,
                yaml_content='dummy: yaml',
                cluster_name='svc-1',
                log_file=str(tmp_path / 'launch.log'),
                replica_to_request_id=request_ids,
                replica_to_launch_cancelled=thread_utils.ThreadSafeDict(),
                launch_fence=fence,
                ordinary_launch_submission_uuid=submission_uuid,
                non_pool_launch_profile_kind=(
                    ordinary_launch_binding.NonPoolLaunchProfileKind.
                    ORDINARY_PAID.value),
                inspect_bound_ordinary_launch=mock.Mock(),
                reduce_bound_ordinary_launch=mock.Mock(),
                cancel_bound_ordinary_launch=mock.Mock())

        submit_generic.assert_called_once_with(
            prepared, submission_uuid, ordinary_launch_binding.
            NonPoolLaunchProfileKind.ORDINARY_PAID.value)
        submit_ordinary.assert_not_called()
        launch.assert_not_called()
        assert request_ids[1] == 'request-id'
        wait.assert_called_once()
        assert (wait.call_args.kwargs['api_auth_token_provider']
                is replica_managers._required_controller_admin_auth_tokens)

    def test_reserved_fill_profile_requires_exact_physical_fence(
            self, tmp_path):
        task = mock.MagicMock()
        task.resources = {mock.MagicMock()}
        with mock.patch.object(replica_managers,
                               '_build_replica_launch_task',
                               return_value=task), \
             pytest.raises(replica_managers._BoundOrdinaryLaunchUnresolvedError,
                           match='present together'):
            replica_managers.launch_cluster(
                replica_id=1,
                yaml_content='dummy: yaml',
                cluster_name='svc-1',
                log_file=str(tmp_path / 'launch.log'),
                replica_to_request_id=thread_utils.ThreadSafeDict(),
                replica_to_launch_cancelled=thread_utils.ThreadSafeDict(),
                launch_fence={
                    replica_managers.serve_constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc'
                },
                ordinary_launch_submission_uuid=(
                    '11111111-1111-4111-8111-111111111111'),
                non_pool_launch_profile_kind=(
                    ordinary_launch_binding.NonPoolLaunchProfileKind.
                    RESERVED_FILL.value),
                inspect_bound_ordinary_launch=mock.Mock(),
                reduce_bound_ordinary_launch=mock.Mock(),
                cancel_bound_ordinary_launch=mock.Mock())

    def _run_launch_cluster(self,
                            tmp_path,
                            stream_side_effects,
                            *,
                            backoff_seconds=0,
                            replica_to_launch_cancelled=None,
                            **kwargs):
        """Run launch_cluster with a mocked SDK.

        Each element of stream_side_effects is one launch attempt: an
        exception to raise from sdk.stream_and_get, or None for success.
        Returns (mock_sdk, mock_terminate, raised RuntimeError or None).
        """
        observed_workspaces = kwargs.pop('observed_workspaces', None)
        launch_side_effect = kwargs.pop('launch_side_effect', None)
        terminate_side_effect = kwargs.pop('terminate_side_effect', None)
        cancel_side_effect = kwargs.pop('cancel_side_effect', None)
        api_status_results = kwargs.pop('api_status_results', [])
        raised = None
        task = mock.MagicMock()
        resource = mock.MagicMock()
        resource.cloud = clouds.AWS()
        task.resources = {resource}
        with mock.patch(
                'sky.serve.replica_managers.task_lib.Task.from_yaml_str',
                return_value=task), \
             mock.patch('sky.serve.replica_managers.usage_lib'), \
             mock.patch('sky.serve.replica_managers.sdk') as mock_sdk, \
             mock.patch('sky.serve.replica_managers.terminate_cluster'
                       ) as mock_terminate, \
             mock.patch.object(
                 ordinary_launch_handoff,
                 'observe_terminal_nonblocking') as mock_observe_terminal, \
             mock.patch('sky.serve.replica_managers.common_utils.Backoff'
                       ) as mock_backoff:

            def _observe_terminal(request_id, *, lookup, emit):
                status = lookup(request_id)
                try:
                    terminal_status = ordinary_launch_handoff.TerminalStatus(
                        status)
                except (TypeError, ValueError):
                    return True
                emit(terminal_status)
                return True

            mock_observe_terminal.side_effect = _observe_terminal
            mock_backoff.return_value.current_backoff.return_value = (
                backoff_seconds)
            if terminate_side_effect is not None:
                mock_terminate.side_effect = terminate_side_effect
            if cancel_side_effect is not None:
                mock_sdk.api_cancel.side_effect = cancel_side_effect
            mock_sdk.api_status.return_value = api_status_results
            if launch_side_effect is not None:
                mock_sdk.launch.side_effect = launch_side_effect
            elif observed_workspaces is None:
                mock_sdk.launch.return_value = 'request-id'
            else:

                def _launch(*_args, **_kwargs):
                    observed_workspaces.append(
                        skypilot_config.get_active_workspace())
                    return 'request-id'

                mock_sdk.launch.side_effect = _launch
            mock_sdk.stream_and_get.side_effect = stream_side_effects
            if replica_to_launch_cancelled is None:
                replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
            try:
                replica_managers.launch_cluster(
                    replica_id=1,
                    yaml_content='dummy: yaml',
                    cluster_name='svc-1',
                    log_file=str(tmp_path / 'launch.log'),
                    replica_to_request_id=thread_utils.ThreadSafeDict(),
                    replica_to_launch_cancelled=replica_to_launch_cancelled,
                    **kwargs)
            except (RuntimeError, exceptions.RequestCancelled) as e:
                raised = e
        return mock_sdk, mock_terminate, raised

    def test_retry_backoff_uses_monotonic_bounded_sleeps(self, tmp_path):
        now = 0.0
        sleeps = []

        def _monotonic():
            return now

        def _sleep(seconds):
            nonlocal now
            sleeps.append(seconds)
            now += seconds

        fake_time = mock.Mock(wraps=replica_managers.time)
        fake_time.time.side_effect = AssertionError('wall clock used')
        fake_time.monotonic.side_effect = _monotonic
        fake_time.sleep.side_effect = _sleep
        with mock.patch('sky.serve.replica_managers.time', fake_time):
            mock_sdk, _, raised = self._run_launch_cluster(
                tmp_path, [RuntimeError('transient'), None],
                backoff_seconds=0.15)

        assert raised is None
        assert mock_sdk.launch.call_count == 2
        assert sleeps == pytest.approx([0.1, 0.05])
        assert now == pytest.approx(0.15)

    def test_ordinary_launch_events_follow_exact_request_result(self, tmp_path):
        events = []
        result_handle = object()

        mock_sdk, _, raised = self._run_launch_cluster(
            tmp_path, [(17, result_handle)],
            api_status_results=[
                types.SimpleNamespace(request_id='request-id',
                                      status='SUCCEEDED')
            ],
            ordinary_launch_event=lambda kind, request_id, job_id, status:
            events.append((kind, request_id, job_id, status)))

        assert raised is None
        mock_sdk.api_status.assert_called_once_with(
            request_ids=['request-id'],
            fields=['request_id', 'status'],
            _exact_request_ids=True,
            _use_body=True,
            _request_timeout_seconds=(
                ordinary_launch_handoff.TERMINAL_STATUS_LOOKUP_TIMEOUT_SECONDS),
            _retry_on_server_unavailable=False)
        assert events == [
            (ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED, 'request-id',
             None, None),
            (ordinary_launch_handoff.EventKind.API_TERMINAL, 'request-id', None,
             ordinary_launch_handoff.TerminalStatus.SUCCEEDED),
            (ordinary_launch_handoff.EventKind.SERVICE_JOB_OBSERVED,
             'request-id', 17, None),
        ]

    def test_ordinary_handoff_context_is_carried_with_launch_fence(
            self, tmp_path):
        fence = {
            'sky_serve_service_name': 'svc',
            'sky_serve_service_hash': 'incarnation-a',
        }
        handoff = {
            'context_version': 1,
            'service_name': 'svc',
            'service_version': 2,
            'replica_id': 7,
            'replica_record_id': '11111111-1111-4111-8111-111111111111',
            'controller_route_epoch': ('22222222-2222-4222-8222-222222222222'),
            'input_digest': 'a' * 64,
        }

        mock_sdk, _, raised = self._run_launch_cluster(
            tmp_path, [None],
            launch_fence=fence,
            ordinary_launch_handoff_context=handoff)

        assert raised is None
        launch_context = mock_sdk.launch.call_args.kwargs[
            '_extra_launch_context']
        assert launch_context['sky_serve_service_hash'] == 'incarnation-a'
        assert launch_context[replica_managers.serve_constants.
                              ORDINARY_LAUNCH_HANDOFF_CONTEXT_KEY] == handoff
        # Context assembly must not mutate the durable fence or caller-owned
        # diagnostic dictionary.
        assert (
            replica_managers.serve_constants.ORDINARY_LAUNCH_HANDOFF_CONTEXT_KEY
            not in fence)
        assert handoff == {
            'context_version': 1,
            'service_name': 'svc',
            'service_version': 2,
            'replica_id': 7,
            'replica_record_id': '11111111-1111-4111-8111-111111111111',
            'controller_route_epoch': ('22222222-2222-4222-8222-222222222222'),
            'input_digest': 'a' * 64,
        }

    @pytest.mark.parametrize('status', ['SUCCEEDED', 'FAILED', 'CANCELLED'])
    def test_stream_error_records_only_observed_terminal_status(
            self, tmp_path, status):
        events = []

        _, _, raised = self._run_launch_cluster(
            tmp_path, [RuntimeError('transport lost')],
            max_retry=1,
            api_status_results=[
                types.SimpleNamespace(request_id='request-id', status=status)
            ],
            ordinary_launch_event=lambda kind, request_id, job_id,
            terminal_status: events.append(
                (kind, request_id, job_id, terminal_status)))

        assert isinstance(raised, RuntimeError)
        assert events == [
            (ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED, 'request-id',
             None, None),
            (ordinary_launch_handoff.EventKind.API_TERMINAL, 'request-id', None,
             ordinary_launch_handoff.TerminalStatus(status)),
        ]

    @pytest.mark.parametrize('api_status_results', [
        [types.SimpleNamespace(request_id='request-id', status='RUNNING')],
        [types.SimpleNamespace(request_id='different', status='FAILED')],
        [],
    ])
    def test_transport_error_does_not_misclassify_nonterminal_or_inexact_status(
            self, tmp_path, api_status_results):
        events = []

        _, _, raised = self._run_launch_cluster(
            tmp_path, [RuntimeError('transport lost')],
            max_retry=1,
            api_status_results=api_status_results,
            ordinary_launch_event=lambda kind, request_id, job_id,
            terminal_status: events.append(
                (kind, request_id, job_id, terminal_status)))

        assert isinstance(raised, RuntimeError)
        assert events == [(ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
                           'request-id', None, None)]

    def test_retry_backoff_stops_on_cancellation(self, tmp_path):
        now = 0.0
        sleeps = []
        cancelled = thread_utils.ThreadSafeDict()

        def _monotonic():
            return now

        def _sleep(seconds):
            nonlocal now
            sleeps.append(seconds)
            now += seconds
            cancelled[1] = True

        fake_time = mock.Mock(wraps=replica_managers.time)
        fake_time.monotonic.side_effect = _monotonic
        fake_time.sleep.side_effect = _sleep
        with mock.patch('sky.serve.replica_managers.time', fake_time):
            mock_sdk, mock_terminate, raised = self._run_launch_cluster(
                tmp_path, [RuntimeError('transient')],
                backoff_seconds=1,
                replica_to_launch_cancelled=cancelled)

        assert raised is None
        assert mock_sdk.launch.call_count == 1
        assert mock_terminate.call_count == 1
        assert sleeps == [0.1]
        assert 1 not in cancelled

    def test_capacity_failure_fails_fast_with_availability_max_retry(
            self, tmp_path):
        """One capacity failure with availability_max_retry=1 must raise
        immediately (no in-place retry of the same exhausted location)."""
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [_capacity_error()] * 3, availability_max_retry=1)
        assert isinstance(raised, replica_managers._ReplicaLaunchCapacityError)
        assert mock_sdk.launch.call_count == 1
        mock_terminate.assert_not_called()

    def test_quota_failure_reports_before_controller_cleanup(self, tmp_path):
        """A terminal typed quota failure follows the same fast path."""
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [_quota_error()] * 3, availability_max_retry=1)
        assert isinstance(raised, replica_managers._ReplicaLaunchCapacityError)
        assert raised.reason == 'quota'
        assert mock_sdk.launch.call_count == 1
        mock_terminate.assert_not_called()

    def test_terminal_capacity_failure_yields_to_lifecycle_cancellation(
            self, tmp_path):
        """Scale-down/cancellation owns cleanup if it wins before feedback."""
        cancelled = thread_utils.ThreadSafeDict()

        def _cancel_then_fail(*_args, **_kwargs):
            cancelled[1] = True
            raise _capacity_error()

        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path,
            _cancel_then_fail,
            availability_max_retry=1,
            replica_to_launch_cancelled=cancelled)

        assert raised is None
        assert mock_sdk.launch.call_count == 1
        mock_terminate.assert_not_called()
        assert 1 not in cancelled

    def test_legacy_service_policy_does_not_block_recovered_launch(
            self, tmp_path):
        legacy_yaml = """
resources:
  cpus: 1
  ports: 8080
  accelerators: A100:1
  use_spot: true
service:
  readiness_probe: /health
  replica_policy:
    min_replicas: 1
    max_replicas: 8
    target_concurrency_per_replica: 2
    spot_placer: dynamic_fallback_per_gpu
run: echo hi
"""
        persisted_spec = mock.MagicMock()
        with mock.patch('sky.serve.replica_managers.usage_lib'), \
             mock.patch('sky.serve.replica_managers.sdk') as mock_sdk:
            mock_sdk.launch.return_value = 'request-id'
            mock_sdk.stream_and_get.return_value = None
            replica_managers.launch_cluster(
                replica_id=1,
                yaml_content=legacy_yaml,
                cluster_name='svc-1',
                log_file=str(tmp_path / 'launch.log'),
                replica_to_request_id=thread_utils.ThreadSafeDict(),
                replica_to_launch_cancelled=thread_utils.ThreadSafeDict(),
                service_spec=persisted_spec)

        mock_sdk.launch.assert_called_once()
        launched_task = mock_sdk.launch.call_args.args[0]
        assert launched_task.service is persisted_spec

    def test_transient_failures_keep_in_place_retries(self, tmp_path):
        """Transient (non-availability) errors must still be retried in
        place even when availability_max_retry=1, so a one-off blip does
        not poison the placer location."""
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path,
            [RuntimeError('transient'),
             RuntimeError('transient'), None],
            availability_max_retry=1)
        assert raised is None
        assert mock_sdk.launch.call_count == 3
        assert mock_sdk.stream_and_get.call_count == 3
        assert mock_terminate.call_count == 2

    def test_launch_worker_enters_durable_service_workspace(self, tmp_path):
        observed_workspaces = []
        _, _, raised = self._run_launch_cluster(
            tmp_path, [None],
            workspace='research',
            observed_workspaces=observed_workspaces)
        assert raised is None
        assert observed_workspaces == ['research']

    def test_capacity_failures_default_to_max_retry(self, tmp_path):
        """Without availability_max_retry, capacity failures keep the
        default max_retry in-place attempts."""
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [_capacity_error()] * 3)
        assert isinstance(raised, replica_managers._ReplicaLaunchCapacityError)
        assert mock_sdk.launch.call_count == 3
        # Retriable attempts are cleaned synchronously; terminal typed
        # feedback returns directly to the manager.
        assert mock_terminate.call_count == 2

    def test_generic_failure_never_becomes_shared_capacity_evidence(
            self, tmp_path):
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [RuntimeError('transient')] * 3)
        assert type(raised) is RuntimeError
        assert mock_sdk.launch.call_count == 3
        assert mock_terminate.call_count == 3

    def test_cleanup_uncertainty_keeps_synchronous_controller_cleanup(
            self, tmp_path):
        """Provisioner cleanup uncertainty must never take the typed path."""
        cleanup_error = provision_common.StopFailoverError(
            'provider cleanup could not be verified')
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [cleanup_error] * 3)
        assert type(raised) is RuntimeError
        assert mock_sdk.launch.call_count == 3
        assert mock_terminate.call_count == 3

    def test_exact_override_collapses_any_of_to_one_resource(self, tmp_path):
        task = mock.MagicMock()
        resources = [mock.Mock(name='first'), mock.Mock(name='second')]
        task.resources = resources
        pinned = mock.Mock(name='pinned')
        resources[0].copy.return_value = pinned
        persisted_spec = mock.sentinel.persisted_spec

        with mock.patch(
                'sky.serve.replica_managers.load_task_with_service_spec',
                return_value=task) as load_task, \
             mock.patch('sky.serve.replica_managers.usage_lib'), \
             mock.patch('sky.serve.replica_managers.sdk') as mock_sdk:
            mock_sdk.launch.return_value = 'request-id'
            replica_managers.launch_cluster(
                replica_id=1,
                yaml_content='dummy: yaml',
                cluster_name='svc-1',
                log_file=str(tmp_path / 'launch.log'),
                replica_to_request_id=thread_utils.ThreadSafeDict(),
                replica_to_launch_cancelled=thread_utils.ThreadSafeDict(),
                resources_override={'region': 'us-east-1'},
                exact_resources_override=True,
                service_spec=persisted_spec)

        load_task.assert_called_once_with('dummy: yaml', persisted_spec)
        resources[0].copy.assert_called_once_with(region='us-east-1')
        resources[1].copy.assert_not_called()
        task.set_resources.assert_called_once_with(pinned)
        mock_sdk.launch.assert_called_once()

    def test_authoritative_prelaunch_guard_rejects_cloud_mutation(
            self, tmp_path):
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [None], pre_launch_guard=lambda: False)
        assert raised is not None
        assert 'ownership was lost' in str(raised)
        mock_sdk.launch.assert_not_called()
        mock_terminate.assert_not_called()

    def test_update_recovery_fence_rejects_cloud_mutation(self, tmp_path):
        manager = _make_manager()
        manager.fence_launches_for_update_recovery()

        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [None],
            pre_launch_guard=manager._service_is_launch_authorized)

        assert raised is not None
        assert 'ownership was lost' in str(raised)
        mock_sdk.launch.assert_not_called()
        mock_terminate.assert_not_called()

    def test_update_recovery_fence_rejects_publish_and_scale_down(self):
        manager = _make_manager()
        manager._terminate_replica = mock.Mock()
        manager.fence_launches_for_update_recovery()

        assert not manager.publish_target_num_replicas(3, expected_version=1)
        manager.scale_down(1, expected_version=1)

        assert manager.get_target_num_replicas() is None
        manager._terminate_replica.assert_not_called()

    def test_superseded_logical_guard_rejects_first_cloud_mutation(
            self, tmp_path):
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [None],
            cloud_launch_guard=lambda: (False, 'replica-not-authorized'))
        assert isinstance(raised,
                          replica_managers._ReplicaLaunchSupersededError)
        assert 'reason=replica-not-authorized' in str(raised)
        mock_sdk.launch.assert_not_called()
        mock_terminate.assert_not_called()

    def test_cloud_guard_rechecks_before_each_retry(self, tmp_path):
        cloud_guard = mock.Mock(
            side_effect=[(True, 'authorized'), (False, 'pool-retargeted')])
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [RuntimeError('transient')],
            cloud_launch_guard=cloud_guard)

        assert isinstance(raised,
                          replica_managers._ReplicaLaunchSupersededError)
        assert 'reason=pool-retargeted' in str(raised)
        assert cloud_guard.call_count == 2
        assert mock_sdk.launch.call_count == 1
        assert mock_sdk.stream_and_get.call_count == 1
        # The first transient attempt is cleaned before authority is checked
        # again; the rejected second attempt performs no cloud mutation.
        mock_terminate.assert_called_once()

    def test_legacy_boolean_cloud_guard_remains_compatible(self, tmp_path):
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [None], cloud_launch_guard=lambda: False)
        assert isinstance(raised,
                          replica_managers._ReplicaLaunchSupersededError)
        assert 'reason=guard-rejected' in str(raised)
        mock_sdk.launch.assert_not_called()
        mock_terminate.assert_not_called()

    def test_unfenced_external_lb_fails_once_before_api_request(self, tmp_path):
        with mock.patch.object(replica_managers.serve_utils,
                               'is_external_load_balancer_mode',
                               return_value=True):
            mock_sdk, mock_terminate, raised = self._run_launch_cluster(
                tmp_path, [None], launch_fence=None)

        assert isinstance(raised,
                          replica_managers._UnfencedExternalLbLaunchError)
        mock_sdk.launch.assert_not_called()
        mock_terminate.assert_not_called()

    def test_wire_launch_fence_rejection_is_terminal_without_cleanup(
            self, tmp_path):
        fence_error = exceptions.ReservedFillLaunchFenceError(
            'durable pool generation changed')
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [fence_error, None])

        assert raised is fence_error
        assert mock_sdk.launch.call_count == 1
        assert mock_sdk.stream_and_get.call_count == 1
        mock_terminate.assert_not_called()

    def test_provider_generation_cancellation_is_terminal_without_cleanup(
            self, tmp_path):
        cancelled = exceptions.ServeReplicaLaunchFenceError(
            'durable generation changed')
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [cancelled, None], supersession_guard=lambda: True)

        assert raised is cancelled
        assert mock_sdk.launch.call_count == 1
        assert mock_sdk.stream_and_get.call_count == 1
        mock_terminate.assert_not_called()

    def test_generic_request_cancellation_keeps_cleanup_and_retry(
            self, tmp_path):
        cancelled = exceptions.RequestCancelled('ordinary cancellation')
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [cancelled, None])

        assert raised is None
        assert mock_sdk.launch.call_count == 2
        assert mock_sdk.stream_and_get.call_count == 2
        mock_terminate.assert_called_once()

    def test_provider_cancellation_maps_changed_guard_to_supersession(
            self, tmp_path):
        cancelled = exceptions.ServeReplicaLaunchFenceError(
            'durable generation changed')
        supersession_guard = mock.Mock(
            side_effect=[(True, 'authorized'), (False,
                                                'manager-version-changed')])
        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [cancelled, None], supersession_guard=supersession_guard)

        assert isinstance(raised,
                          replica_managers._ReplicaLaunchSupersededError)
        assert 'manager-version-changed' in str(raised)
        assert mock_sdk.launch.call_count == 1
        assert mock_sdk.stream_and_get.call_count == 1
        mock_terminate.assert_not_called()

    def test_failed_launch_cleanup_stops_after_controller_replacement(
            self, tmp_path):
        cleanup_guard = mock.Mock(return_value=False)
        real_terminate_cluster = replica_managers.terminate_cluster

        def _refuse_stale_cleanup(*_args, **kwargs):
            guard = kwargs['continue_guard']
            assert guard is cleanup_guard
            return real_terminate_cluster(*_args, **kwargs)

        with mock.patch.object(
                replica_managers.global_user_state,
                'get_cluster_from_name',
                return_value={
                    'name': 'svc-1',
                    'workspace': None,
                }), \
             mock.patch('sky.core.down') as core_down:
            mock_sdk, mock_terminate, raised = self._run_launch_cluster(
                tmp_path, [RuntimeError('transient'), None],
                cleanup_continue_guard=cleanup_guard,
                terminate_side_effect=_refuse_stale_cleanup)

        assert isinstance(raised, RuntimeError)
        assert 'service lifecycle ownership was lost' in str(raised)
        assert mock_sdk.launch.call_count == 1
        assert mock_sdk.stream_and_get.call_count == 1
        mock_terminate.assert_called_once()
        cleanup_guard.assert_called_once_with()
        core_down.assert_not_called()

    def test_inflight_owner_watchdog_cancels_request(self, tmp_path):
        allowed = threading.Event()
        watchdog_observed_loss = threading.Event()
        allowed.set()

        def _continue_guard():
            if allowed.is_set():
                return True
            watchdog_observed_loss.set()
            return False

        def _block_while_watchdog_runs(_request_id):
            allowed.clear()
            assert watchdog_observed_loss.wait(timeout=5)
            raise RuntimeError('request cancelled')

        with mock.patch(
                'sky.serve.replica_managers.'
                '_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS', 0.01):
            mock_sdk, _, raised = self._run_launch_cluster(
                tmp_path,
                _block_while_watchdog_runs,
                continue_guard=_continue_guard)

        assert raised is not None
        assert 'ownership loss' in str(raised)
        mock_sdk.api_cancel.assert_called_once_with('request-id')

    def test_owner_loss_success_race_records_one_cancellation_request(
            self, tmp_path):
        allowed = threading.Event()
        watchdog_observed_loss = threading.Event()
        allowed.set()
        events = []

        def _continue_guard():
            if allowed.is_set():
                return True
            watchdog_observed_loss.set()
            return False

        def _complete_while_watchdog_cancels(unused_request_id):
            allowed.clear()
            assert watchdog_observed_loss.wait(timeout=5)
            return (17, object())

        with mock.patch(
                'sky.serve.replica_managers.'
                '_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS', 0.01):
            mock_sdk, _, raised = self._run_launch_cluster(
                tmp_path,
                _complete_while_watchdog_cancels,
                continue_guard=_continue_guard,
                ordinary_launch_event=lambda kind, request_id, job_id, status:
                events.append((kind, request_id, job_id, status)))

        assert raised is not None
        assert 'ownership was lost' in str(raised)
        # The watchdog and the post-success authority check both request
        # cancellation, but the lower-bound evidence records the intent once.
        assert mock_sdk.api_cancel.call_count == 2
        cancellation_requests = [
            event for event in events if event[0] ==
            ordinary_launch_handoff.EventKind.OWNER_LOSS_CANCEL_REQUESTED
        ]
        assert cancellation_requests == [
            (ordinary_launch_handoff.EventKind.OWNER_LOSS_CANCEL_REQUESTED,
             'request-id', None, None)
        ]

    def test_inflight_version_supersession_keeps_manager_healthy(
            self, tmp_path):
        manager = _make_manager()
        manager._ownership_lost = threading.Event()
        manager._manager_daemon_stop = threading.Event()
        manager._update_recovery_required = False
        supersession_observed = threading.Event()
        cancelled = thread_utils.ThreadSafeDict()

        def _generation_guard():
            decision = manager._queued_launch_generation_decision(1)
            if not decision[0]:
                supersession_observed.set()
            return decision

        def _block_while_watchdog_runs(_request_id):
            manager._pending_version = 2
            assert supersession_observed.wait(timeout=5)
            raise RuntimeError('request cancelled')

        with mock.patch(
                'sky.serve.replica_managers.'
                '_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS', 0.01):
            mock_sdk, _, raised = self._run_launch_cluster(
                tmp_path,
                _block_while_watchdog_runs,
                supersession_guard=_generation_guard,
                replica_to_launch_cancelled=cancelled)

        assert isinstance(raised,
                          replica_managers._ReplicaLaunchSupersededError)
        assert 'newer-version-pending' in str(raised)
        mock_sdk.api_cancel.assert_called_once_with('request-id')
        assert not manager._ownership_lost.is_set()
        assert not manager._manager_daemon_stop.is_set()
        assert not cancelled

        manager.latest_version = 2
        manager._pending_version = None
        next_mock_sdk, _, next_raised = self._run_launch_cluster(
            tmp_path, [None],
            supersession_guard=functools.partial(
                manager._queued_launch_generation_decision, 2))
        assert next_raised is None
        next_mock_sdk.launch.assert_called_once()

    def test_inflight_supersession_retries_transient_cancel_failure(
            self, tmp_path):
        manager = _make_manager()
        manager._update_recovery_required = False
        cancellation_succeeded = threading.Event()
        cancel_attempts = 0

        def _cancel(_request_id):
            nonlocal cancel_attempts
            cancel_attempts += 1
            if cancel_attempts == 1:
                raise RuntimeError('transient cancel transport failure')
            cancellation_succeeded.set()

        def _block_until_cancelled(_request_id):
            manager._pending_version = 2
            assert cancellation_succeeded.wait(timeout=5)
            raise RuntimeError('request cancelled')

        with mock.patch(
                'sky.serve.replica_managers.'
                '_LAUNCH_OWNER_WATCH_INTERVAL_SECONDS', 0.01):
            mock_sdk, _, raised = self._run_launch_cluster(
                tmp_path,
                _block_until_cancelled,
                supersession_guard=functools.partial(
                    manager._queued_launch_generation_decision, 1),
                cancel_side_effect=_cancel)

        assert isinstance(raised,
                          replica_managers._ReplicaLaunchSupersededError)
        assert mock_sdk.api_cancel.call_count == 2

    @staticmethod
    def _recovery_handle(cluster_name='svc-1'):
        handle = mock.Mock(
            spec=replica_managers.backends.CloudVmRayResourceHandle)
        handle.cluster_name = cluster_name
        return handle

    def test_recovery_request_captures_exact_returned_job(self, tmp_path):
        get_bound = mock.Mock(return_value='request-id')
        persist_job = mock.Mock(return_value=True)
        demote = mock.Mock(return_value=True)
        recovery_context = {'closed': 'context'}
        handle = self._recovery_handle()

        mock_sdk, _, raised = self._run_launch_cluster(
            tmp_path, [(7, handle)],
            system_recovery_launch_context=recovery_context,
            get_bound_system_recovery_request_id=get_bound,
            persist_system_recovery_job_id=persist_job,
            demote_system_recovery_candidate=demote)

        assert raised is None
        assert mock_sdk.launch.call_count == 1
        assert (mock_sdk.launch.call_args.kwargs['_extra_launch_context'] ==
                recovery_context)
        get_bound.assert_called_once_with()
        persist_job.assert_called_once_with('request-id', 7)
        demote.assert_not_called()

    def test_lost_launch_response_adopts_only_bound_request(self, tmp_path):
        get_bound = mock.Mock(return_value='bound-request')
        persist_job = mock.Mock(return_value=True)
        demote = mock.Mock(return_value=True)
        handle = self._recovery_handle()

        mock_sdk, _, raised = self._run_launch_cluster(
            tmp_path, [(11, handle)],
            launch_side_effect=RuntimeError('response lost'),
            system_recovery_launch_context={'closed': 'context'},
            get_bound_system_recovery_request_id=get_bound,
            persist_system_recovery_job_id=persist_job,
            demote_system_recovery_candidate=demote)

        assert raised is None
        mock_sdk.stream_and_get.assert_called_once_with('bound-request')
        persist_job.assert_called_once_with('bound-request', 11)
        demote.assert_not_called()

    def test_request_mismatch_demotes_before_one_ordinary_retry(self, tmp_path):
        get_bound = mock.Mock(return_value='different-request')
        persist_job = mock.Mock(return_value=True)
        demote = mock.Mock(return_value=True)

        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [None],
            launch_side_effect=['request-id', 'ordinary-request'],
            system_recovery_launch_context={'closed': 'context'},
            get_bound_system_recovery_request_id=get_bound,
            persist_system_recovery_job_id=persist_job,
            demote_system_recovery_candidate=demote)

        assert raised is None
        assert mock_sdk.launch.call_count == 2
        first_call, second_call = mock_sdk.launch.call_args_list
        assert '_extra_launch_context' in first_call.kwargs
        assert '_extra_launch_context' not in second_call.kwargs
        demote.assert_called_once_with()
        persist_job.assert_not_called()
        mock_terminate.assert_called_once()

    def test_demotion_failure_refuses_second_launch_request(self, tmp_path):
        demote = mock.Mock(return_value=False)

        mock_sdk, mock_terminate, raised = self._run_launch_cluster(
            tmp_path, [None],
            system_recovery_launch_context={'closed': 'context'},
            get_bound_system_recovery_request_id=mock.Mock(
                return_value='different-request'),
            persist_system_recovery_job_id=mock.Mock(return_value=True),
            demote_system_recovery_candidate=demote)

        assert isinstance(raised,
                          replica_managers._SystemRecoveryLaunchCaptureError)
        assert mock_sdk.launch.call_count == 1
        demote.assert_called_once_with()
        mock_terminate.assert_not_called()

    @pytest.mark.parametrize('malformed_result',
                             [None, (True, object()), (1, object())])
    def test_malformed_recovery_result_demotes_before_retry(
            self, tmp_path, malformed_result):
        get_bound = mock.Mock(return_value='request-id')
        persist_job = mock.Mock(return_value=True)
        demote = mock.Mock(return_value=True)
        events = []
        handoff = {
            'context_version': 1,
            'service_name': 'svc',
            'service_version': 2,
            'replica_id': 1,
            'replica_record_id': '11111111-1111-4111-8111-111111111111',
            'controller_route_epoch': ('22222222-2222-4222-8222-222222222222'),
            'input_digest': 'a' * 64,
        }

        mock_sdk, _, raised = self._run_launch_cluster(
            tmp_path, [malformed_result, None],
            launch_side_effect=['request-id', 'ordinary-request'],
            system_recovery_launch_context={'closed': 'context'},
            get_bound_system_recovery_request_id=get_bound,
            persist_system_recovery_job_id=persist_job,
            demote_system_recovery_candidate=demote,
            ordinary_launch_handoff_context=handoff,
            ordinary_launch_event=lambda kind, request_id, job_id, status:
            events.append((kind, request_id, job_id, status)))

        assert raised is None
        assert mock_sdk.launch.call_count == 2
        first_context = mock_sdk.launch.call_args_list[0].kwargs[
            '_extra_launch_context']
        second_context = mock_sdk.launch.call_args_list[1].kwargs[
            '_extra_launch_context']
        # The recovery protocol is a closed exact-key contract. Diagnostic
        # identity appears only after durable demotion makes the retry ordinary.
        assert first_context == {'closed': 'context'}
        assert second_context == {
            replica_managers.serve_constants.ORDINARY_LAUNCH_HANDOFF_CONTEXT_KEY: handoff,
        }
        demote.assert_called_once_with()
        persist_job.assert_not_called()
        # The bound recovery request is intentionally excluded.  Once durable
        # demotion succeeds, the subsequent ordinary request is observable.
        assert events == [
            (ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
             'ordinary-request', None, None),
        ]

    def test_demoted_candidate_callback_requires_explicit_retry_marker(self):
        route_epoch = '22222222-2222-4222-8222-222222222222'
        record_id = '11111111-1111-4111-8111-111111111111'
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._is_pool = False
        manager._service_name = 'svc'
        manager._ordinary_launch_handoff_route_epoch = route_epoch
        info = mock.Mock()
        info.reserved_fill = False
        info.system_recovery_disposition = (
            recovery_state.SystemRecoveryDisposition.CANDIDATE)
        info.version = 2
        info.replica_id = 1
        info.replica_record_id = record_id

        with mock.patch.object(ordinary_launch_handoff,
                               'emit_event') as emit_event:
            manager._emit_ordinary_launch_handoff_event(
                info,
                ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
                'initial-recovery-request',
                input_digest='a' * 64)
            manager._emit_ordinary_launch_handoff_event(
                info,
                ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
                'ordinary-request',
                input_digest='a' * 64,
                allow_demoted_candidate=True)

        emit_event.assert_called_once_with(
            event_kind=ordinary_launch_handoff.EventKind.REQUEST_PUBLISHED,
            service_name='svc',
            service_version=2,
            replica_id=1,
            replica_record_id=record_id,
            controller_route_epoch=route_epoch,
            ordinary_request_id='ordinary-request',
            service_job_id=None,
            terminal_status=None,
            input_digest='a' * 64)


class TestLaunchReplicaAvailabilityMaxRetry:
    """`_launch_replica` must cap availability failures at one attempt only
    when a placement policy owns failover."""

    def _launch_replica(self,
                        use_spot: bool,
                        with_placer: bool,
                        kubernetes_only: bool = False):
        # pylint: disable=protected-access
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager.yaml_content = 'dummy: yaml'
        manager.latest_version = 1
        manager._version_specs = {1: mock.Mock()}
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        placer = None
        if with_placer:
            if kubernetes_only:
                a100 = make_location('prod_research_cluster_eks',
                                     accelerators={'A100-80GB': 1},
                                     use_spot=False,
                                     cloud_name='Kubernetes')
                h200 = make_location('prod_research_cluster_eks',
                                     accelerators={'H200': 1},
                                     use_spot=False,
                                     cloud_name='Kubernetes')
                placer = make_placer({a100: 0.0, h200: 0.0})
            else:
                placer = mock.Mock()
                placer.active_locations.return_value = []
                placer.ranked_active_locations.return_value = []
                placer.zero_cost_locations.return_value = []
                location = mock.Mock()
                location.to_dict.return_value = {'zone': 'z'}
                placer.select_next_location.return_value = location
        manager._spot_placer = placer

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=use_spot), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos',
                 return_value=[]), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state'
                 '.add_or_update_replica'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers.ReplicaInfo') as replica_info, \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'
                       ) as mock_thread:
            replica_info.return_value.replica_record_id = (
                '00000000-0000-4000-8000-000000000001')
            manager._launch_replica(replica_id=1)
        return mock_thread.call_args

    def test_spot_with_placer_fails_fast_on_availability(self):
        call = self._launch_replica(use_spot=True, with_placer=True)
        assert call.kwargs['kwargs']['availability_max_retry'] == 1
        assert call.kwargs['kwargs']['exact_resources_override'] is True
        assert callable(call.kwargs['kwargs']['pre_launch_guard'])
        assert callable(call.kwargs['kwargs']['continue_guard'])
        assert callable(call.kwargs['kwargs']['cleanup_continue_guard'])
        handoff = call.kwargs['kwargs']['ordinary_launch_handoff_context']
        assert handoff['context_version'] == 1
        assert handoff['service_name'] == 'svc'
        assert handoff['service_version'] == 1
        assert handoff['replica_id'] == 1
        assert handoff['replica_record_id'] == (
            '00000000-0000-4000-8000-000000000001')
        assert handoff['controller_route_epoch']
        assert len(handoff['input_digest']) == 64
        # retry_until_up must be False: failover is owned by the placer.
        assert call.kwargs['args'][-1] is False

    def test_spot_without_placer_keeps_default_retries(self):
        call = self._launch_replica(use_spot=True, with_placer=False)
        assert call.kwargs['kwargs']['availability_max_retry'] is None
        assert call.kwargs['args'][-1] is True

    def test_non_spot_with_placer_keeps_default_retries(self):
        """A non-spot (on-demand fallback) replica keeps the default
        retries even when the service has a spot placer."""
        call = self._launch_replica(use_spot=False, with_placer=True)
        assert call.kwargs['kwargs']['availability_max_retry'] is None
        assert call.kwargs['args'][-1] is True

    def test_digest_failure_omits_telemetry_without_blocking_initial_launch(
            self):
        with mock.patch.object(ordinary_launch_handoff,
                               'redacted_input_digest',
                               return_value=None) as digest:
            call = self._launch_replica(use_spot=False, with_placer=False)

        digest.assert_called_once()
        launch_kwargs = call.kwargs['kwargs']
        assert 'ordinary_launch_handoff_context' not in launch_kwargs
        assert 'ordinary_launch_event' not in launch_kwargs

    def test_non_spot_kubernetes_only_placer_owns_failover(self):
        call = self._launch_replica(use_spot=False,
                                    with_placer=True,
                                    kubernetes_only=True)

        assert call.kwargs['kwargs']['availability_max_retry'] == 1
        assert call.kwargs['kwargs']['exact_resources_override'] is True
        assert call.kwargs['args'][-1] is False
        resources_override = call.kwargs['args'][-2]
        assert resources_override['use_spot'] is False
        assert resources_override['accelerators'] in ({
            'A100-80GB': 1
        }, {
            'H200': 1
        })

    def test_non_spot_kubernetes_only_batch_takes_placement_snapshot(self):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        a100 = make_location('prod_research_cluster_eks',
                             accelerators={'A100-80GB': 1},
                             use_spot=False,
                             cloud_name='Kubernetes')
        h200 = make_location('prod_research_cluster_eks',
                             accelerators={'H200': 1},
                             use_spot=False,
                             cloud_name='Kubernetes')
        manager._spot_placer = make_placer({a100: 0.0, h200: 0.0})

        assert manager._batch_needs_placement_snapshot([None])


class TestUpdateVersionHoldsManagerLock:
    """`update_version` must serialize on the manager lock.

    It runs on the controller's HTTP-handler thread while the autoscaler /
    prober daemon threads hold `self.lock` for their own read-modify-write
    cycles; without the lock a concurrent `scale_up` can read a torn
    (latest_version, yaml_content) pair and replica-row upserts can be lost.
    """

    def test_update_version_blocks_until_lock_released(self):
        mgr = _make_manager()
        mgr.lock = threading.Lock()
        mgr.latest_version = 5
        entered = threading.Event()
        done = threading.Event()

        def _call():
            entered.set()
            # version <= latest_version returns right after the lock is
            # acquired, so completion is a proxy for lock acquisition.
            mgr.update_version(1,
                               mock.Mock(),
                               update_mode=serve_utils.UpdateMode.ROLLING)
            done.set()

        thread = threading.Thread(target=_call, daemon=True)
        with mgr.lock:  # simulate a daemon thread mid read-modify-write
            thread.start()
            assert entered.wait(timeout=5)
            assert not done.wait(timeout=0.5)
        assert done.wait(timeout=5)
        thread.join(timeout=5)


class TestUpdateVersionBatchesPriorVersionYamls:
    """`update_version` should reuse old YAMLs per distinct version."""

    @pytest.fixture(autouse=True)
    def _explicit_prior_specs(self):

        def _get_specs(_service_name, versions):
            return {
                version: service_spec.SkyServiceSpec(
                    readiness_path='/',
                    initial_delay_seconds=0,
                    readiness_timeout_seconds=15,
                    endpoint_probe_interval_seconds=10,
                    lb_stream_timeout_seconds=30,
                    min_replicas=1) for version in versions
            }

        with mock.patch.object(replica_managers.serve_state,
                               'get_specs',
                               side_effect=_get_specs):
            yield

    def test_reuses_preflight_spot_placer_for_new_task(self):
        mgr = _make_manager()
        assert mgr.publish_target_num_replicas(
            3, expected_version=mgr.latest_version)
        old_placer = mock.Mock(name='old_placer')
        new_placer = mock.Mock(name='new_placer')
        mgr._spot_placer = old_placer
        spec = mock.Mock(
            spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
            placement_contract=_LOGICAL_PLACEMENT_CONTRACT)
        new_task = mock.Mock(name='new_task', resources=[])
        new_yaml = ('resources: {accelerators: L4:1}\n'
                    'file_mounts: {}\n'
                    'service: {readiness_probe: /}\n')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=new_yaml), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[]), \
             mock.patch.object(
                 replica_managers,
                 'load_task_with_service_spec',
                 return_value=new_task) as parse_task, \
             mock.patch.object(
                 replica_managers.spot_placer.SpotPlacer,
                 'from_task',
                 return_value=new_placer) as build_placer:
            mgr.update_version(2,
                               spec,
                               update_mode=serve_utils.UpdateMode.ROLLING,
                               new_spot_placer=new_placer)

        parse_task.assert_called_once_with(new_yaml, spec)
        build_placer.assert_not_called()
        new_placer.inherit_preemption_state.assert_called_once_with(old_placer)
        assert mgr._spot_placer is new_placer
        assert mgr.get_target_num_replicas() is None

    def test_reuses_distinct_old_version_yamls(self):
        mgr = _make_manager()
        mgr.latest_version = 2
        mgr.yaml_content = 'old: yaml'
        mgr._update_mode = None
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))

        info1 = mock.Mock(replica_id=1, version=1, is_terminal=False)
        info2 = mock.Mock(replica_id=2, version=1, is_terminal=False)
        info3 = mock.Mock(replica_id=3, version=2, is_terminal=False)
        terminal = mock.Mock(replica_id=4, version=1, is_terminal=True)
        replica_infos = [info1, info2, info3, terminal]

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=('resources: {}\n'
                              'file_mounts: {}\n'
                              'service: {readiness_probe: /}\n')
        ) as get_new_yaml, \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={
                     1: ('resources: {}\n'
                         'file_mounts: {}\n'
                         'service: {readiness_probe: /}\n'),
                     2: ('resources: {cpus: 2}\n'
                         'file_mounts: {}\n'
                         'service: {readiness_probe: /}\n'),
                 }) as get_old_yamls, \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=replica_infos):
            mgr.update_version(3,
                               _physical_service_spec_mock(),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        get_new_yaml.assert_called_once_with('svc', 3)
        get_old_yamls.assert_called_once_with('svc', [1, 2])
        assert persisted == [(1, 3), (2, 3)]
        assert info1.version == 3
        assert info2.version == 3
        assert info3.version == 2
        assert terminal.version == 1

    def test_capable_replica_is_not_relabelled_to_new_readiness_contract(self):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        mgr._update_mode = None
        mgr._persist_replica = mock.Mock()
        info = mock.Mock(replica_id=1, version=1, is_terminal=False)
        info.system_recovery_disposition = (
            recovery_state.SystemRecoveryDisposition.CAPABLE)
        yaml_content = ('resources: {}\n'
                        'file_mounts: {}\n'
                        'service: {readiness_probe: /new-health}\n')

        with mock.patch.object(replica_managers.serve_state,
                               'get_yaml_content',
                               return_value=yaml_content), \
             mock.patch.object(replica_managers.serve_state,
                               'get_yaml_contents',
                               return_value={1: yaml_content}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]):
            mgr.update_version(2,
                               _physical_service_spec_mock(),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert info.version == 1
        mgr._persist_replica.assert_not_called()

    def test_missing_old_version_yaml_fails_before_persisting(self):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        mgr._update_mode = None
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))
        replica_infos = [
            mock.Mock(replica_id=1, version=1, is_terminal=False),
            mock.Mock(replica_id=2, version=2, is_terminal=False),
        ]

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=('resources: {}\n'
                              'file_mounts: {}\n'
                              'service: {readiness_probe: /}\n')), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={
                     1: ('resources: {}\n'
                         'file_mounts: {}\n'
                         'service: {readiness_probe: /}\n'),
                 }), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=replica_infos):
            with pytest.raises(
                    ValueError,
                    match='yaml content not found for svc version 2'):
                mgr.update_version(3,
                                   _physical_service_spec_mock(),
                                   update_mode=serve_utils.UpdateMode.ROLLING)

        assert not persisted
        assert [info.version for info in replica_infos] == [1, 2]

    def test_no_prior_nonterminal_versions_skips_yaml_lookup(self):
        for replica_infos in (
            [],
            [mock.Mock(replica_id=1, version=1, is_terminal=True)],
        ):
            mgr = _make_manager()
            mgr.latest_version = 1
            mgr.yaml_content = 'old: yaml'
            mgr._update_mode = None

            with mock.patch.object(
                    replica_managers.serve_state,
                    'get_yaml_content',
                    return_value=('resources: {}\n'
                                  'file_mounts: {}\n'
                                  'service: {readiness_probe: /}\n')), \
                 mock.patch.object(
                     replica_managers.serve_state,
                     'get_yaml_contents') as get_old_yamls, \
                 mock.patch.object(
                     replica_managers.serve_state,
                     'get_replica_infos',
                     return_value=replica_infos):
                mgr.update_version(2,
                                   _physical_service_spec_mock(),
                                   update_mode=serve_utils.UpdateMode.ROLLING)

            get_old_yamls.assert_not_called()

    @pytest.mark.parametrize('old_scope_id,new_scope_id', [
        ('old-scope', 'new-scope'),
        ('old-scope', None),
    ])
    def test_reuses_replica_when_only_empty_storage_scope_changes(
            self, old_scope_id, new_scope_id):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        mgr._update_mode = None
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))
        info = mock.Mock(replica_id=1, version=1, is_terminal=False)

        def _yaml(scope_id):
            metadata = ''
            if scope_id is not None:
                metadata = ('_metadata:\n'
                            '  sky_serve_ephemeral_storage_scope:\n'
                            '    resource_scope: incarnation\n'
                            f'    scope_id: {scope_id}\n'
                            f'    storage_generation: {scope_id}-generation\n'
                            '    storage_mounts: []\n')
            return ('resources: {}\n'
                    'file_mounts: {}\n'
                    'volumes: {}\n'
                    'service: {readiness_probe: /}\n' + metadata)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=_yaml(new_scope_id)), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={1: _yaml(old_scope_id)}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]):
            mgr.update_version(2,
                               _physical_service_spec_mock(),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert persisted == [(1, 2)]
        assert info.version == 2

    def test_reuses_replica_when_only_git_commit_changes(self, caplog):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))
        info = mock.Mock(replica_id=1, version=1, is_terminal=False)

        def _yaml(git_commit):
            return ('resources: {}\n'
                    'file_mounts: {}\n'
                    'secrets: {TOKEN: stable-secret}\n'
                    'service: {readiness_probe: /}\n'
                    f'_metadata: {{git_commit: {git_commit}}}\n')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=_yaml('new-commit')), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={1: _yaml('old-commit')}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]), \
             caplog.at_level(logging.INFO):
            mgr.update_version(2,
                               _physical_service_spec_mock(),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert persisted == [(1, 2)]
        assert info.version == 2
        assert 'stable-secret' not in caplog.text

    def test_secret_change_forces_replacement_without_logging_values(
            self, caplog):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))
        info = mock.Mock(replica_id=1, version=1, is_terminal=False)

        def _yaml(secret):
            return ('resources: {}\n'
                    'file_mounts: {}\n'
                    f'secrets: {{TOKEN: {secret}}}\n'
                    'service: {readiness_probe: /}\n')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=_yaml('new-secret')), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={1: _yaml('old-secret')}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]), \
             caplog.at_level(logging.INFO):
            mgr.update_version(2,
                               _physical_service_spec_mock(),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert not persisted
        assert info.version == 1
        assert 'runtime config changed' in caplog.text
        assert 'old-secret' not in caplog.text
        assert 'new-secret' not in caplog.text

    def test_storage_scope_with_owned_mount_still_forces_replacement(self):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        mgr._update_mode = None
        persisted = []
        mgr._persist_replica = lambda replica_id, info: persisted.append(
            (replica_id, info.version))
        info = mock.Mock(replica_id=1, version=1, is_terminal=False)

        def _yaml(scope_id):
            return ('resources: {}\n'
                    'file_mounts: {}\n'
                    'volumes: {}\n'
                    'service: {readiness_probe: /}\n'
                    '_metadata:\n'
                    '  sky_serve_ephemeral_storage_scope:\n'
                    '    resource_scope: incarnation\n'
                    f'    scope_id: {scope_id}\n'
                    f'    storage_generation: {scope_id}-generation\n'
                    '    storage_mounts: [/data]\n')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_yaml_content',
                return_value=_yaml('new-scope')), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={1: _yaml('old-scope')}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]):
            mgr.update_version(2,
                               _physical_service_spec_mock(),
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert not persisted
        assert info.version == 1

    def test_logical_update_reuses_shape_placer_from_preflight(self):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        old_placer = mock.Mock(name='old_placer')
        mgr._spot_placer = old_placer
        new_location = types.SimpleNamespace(accelerators={'L4': 4})
        new_placer = mock.Mock(name='new_placer')
        new_placer.active_locations.return_value = [new_location]
        new_task = types.SimpleNamespace(resources=[new_location], num_nodes=1)
        spec = types.SimpleNamespace(
            uses_logical_replicas=True,
            spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
            placement_contract=_LOGICAL_PLACEMENT_CONTRACT,
        )
        yaml_content = ('resources: {}\n'
                        'file_mounts: {}\n'
                        'service: {readiness_probe: /}\n')

        with mock.patch.object(replica_managers.serve_state,
                               'get_yaml_content',
                               return_value=yaml_content), \
             mock.patch.object(replica_managers,
                               'load_task_with_service_spec',
                               return_value=new_task), \
             mock.patch.object(replica_managers.spot_placer.SpotPlacer,
                               'from_task',
                               return_value=new_placer) as build_placer, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]):
            mgr.update_version(2,
                               spec,
                               update_mode=serve_utils.UpdateMode.ROLLING,
                               new_spot_placer=new_placer)

        build_placer.assert_not_called()
        new_placer.inherit_preemption_state.assert_called_once_with(old_placer)
        assert mgr._spot_placer is new_placer
        assert mgr._default_planned_capacity == 4
        assert mgr.latest_version == 2

    def test_logical_update_hands_off_uncommitted_retirement(self):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        old_epoch = mgr._logical_controller_epoch
        old_placer = mock.Mock(name='old_placer')
        mgr._spot_placer = old_placer
        mgr._persist_replica = mock.Mock()

        retiring = replica_managers.ReplicaInfo(replica_id=1,
                                                cluster_name='svc-1',
                                                replica_port='8080',
                                                is_spot=True,
                                                location=None,
                                                version=1,
                                                resources_override=None,
                                                planned_capacity=1)
        retiring.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        retiring.status_property.service_ready_now = True
        retiring.status_property.is_scale_down = True
        retiring.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        retiring.status_property.wait_for_idle_before_termination = True
        retiring.status_property.drain_cap_seconds = 3900
        retiring.status_property.drain_started_at = (
            replica_managers.time.time() - 30)
        retiring.status_property.logical_retirement_version = 1
        retiring.status_property.logical_retirement_controller_epoch = old_epoch
        retiring.status_property.logical_retirement_generation = 4
        retiring.status_property.logical_retirement_target_capacity = 1
        retiring.status_property.logical_retirement_confirmed_generation = None
        retiring.status_property.logical_retirement_bounded_deadline = False
        retiring.status_property.logical_retirement_committed = False
        mgr._wait_for_idle_trackers = {
            1: (mock.Mock(return_value=False),
                replica_managers.time.monotonic() + 300)
        }

        survivor = replica_managers.ReplicaInfo(replica_id=2,
                                                cluster_name='svc-2',
                                                replica_port='8080',
                                                is_spot=True,
                                                location=None,
                                                version=1,
                                                resources_override=None,
                                                planned_capacity=1)
        survivor.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        survivor.status_property.service_ready_now = True

        location = types.SimpleNamespace(accelerators={'L4': 1})
        new_task = types.SimpleNamespace(resources=[location], num_nodes=1)
        new_placer = mock.Mock(name='new_placer')
        new_placer.active_locations.return_value = [location]
        spec = types.SimpleNamespace(
            uses_logical_replicas=True,
            spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
            placement_contract=_LOGICAL_PLACEMENT_CONTRACT)
        yaml_content = ('resources: {}\n'
                        'file_mounts: {}\n'
                        'service: {readiness_probe: /}\n')

        with mock.patch.object(replica_managers.serve_state,
                               'get_yaml_content',
                               return_value=yaml_content), \
             mock.patch.object(replica_managers.serve_state,
                               'get_yaml_contents',
                               return_value={1: yaml_content}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_specs',
                               return_value={1: spec}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(replica_managers,
                               'load_task_with_service_spec',
                               return_value=new_task), \
             mock.patch.object(replica_managers.spot_placer.SpotPlacer,
                               'from_task',
                               return_value=new_placer):
            mgr.update_version(2,
                               spec,
                               update_mode=serve_utils.UpdateMode.ROLLING,
                               new_spot_placer=new_placer)

        assert mgr.latest_version == 2
        assert mgr._logical_controller_epoch != old_epoch
        assert mgr._recovering_logical_retirement_ids == {1}
        assert retiring.status_property.is_scale_down
        assert retiring.version == 2
        assert retiring.status_property.logical_retirement_version == 2
        assert (retiring.status_property.logical_retirement_controller_epoch ==
                old_epoch)
        assert survivor.version == 2

        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=2,
                generation=5,
                observed_slots_by_replica_id={2: 1},
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (2, 5, 1)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert mgr._recovering_logical_retirement_ids == {1}
        assert retiring.status_property.is_scale_down
        assert retiring.status_property.logical_retirement_version == 2
        assert (retiring.status_property.logical_retirement_controller_epoch ==
                mgr._logical_controller_epoch)
        assert retiring.status_property.logical_retirement_generation == 5
        assert retiring.status_property.logical_retirement_target_capacity == 1

        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, generation=6)
        mgr._logical_target = (2, 6, 1)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()
        assert not mgr._recovering_logical_retirement_ids

    def test_logical_update_hands_off_bounded_precommit_retirement(self):
        """A budget-delayed bounded drain stays off route across an update."""
        mgr = _make_manager()
        mgr.latest_version = 2
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()
        old_epoch = mgr._logical_controller_epoch

        retiring = replica_managers.ReplicaInfo(replica_id=1,
                                                cluster_name='svc-1',
                                                replica_port='8080',
                                                is_spot=True,
                                                location=None,
                                                version=1,
                                                resources_override=None,
                                                planned_capacity=1)
        status = retiring.status_property
        status.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
        status.service_ready_now = True
        status.is_scale_down = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.drain_cap_seconds = 3900
        status.drain_started_at = replica_managers.time.time() - 100
        status.wait_for_idle_before_termination = False
        status.logical_retirement_version = 2
        status.logical_retirement_controller_epoch = old_epoch
        status.logical_retirement_generation = 4
        status.logical_retirement_target_capacity = 1
        status.logical_retirement_confirmed_generation = 4
        status.logical_retirement_bounded_deadline = True
        status.logical_retirement_committed = False
        tracker_deadline = replica_managers.time.monotonic() - 1
        mgr._wait_for_idle_trackers = {
            1: (mock.Mock(return_value=False), tracker_deadline)
        }

        handed_off = mgr._handoff_logical_retirements_for_version_update(
            [retiring])

        assert handed_off == {1}
        assert mgr._logical_controller_epoch != old_epoch
        assert mgr._recovering_logical_retirement_ids == {1}
        assert retiring.status_property.is_scale_down
        assert retiring.status_property.logical_retirement_version == 2

        survivor = replica_managers.ReplicaInfo(replica_id=2,
                                                cluster_name='svc-2',
                                                replica_port='8080',
                                                is_spot=True,
                                                location=None,
                                                version=3,
                                                resources_override=None,
                                                planned_capacity=1)
        survivor.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        survivor.status_property.service_ready_now = True
        mgr.latest_version = 3
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=3,
                generation=5,
                observed_slots_by_replica_id={2: 1},
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (3, 5, 1)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()

        status = retiring.status_property
        assert mgr._recovering_logical_retirement_ids == {1}
        assert status.is_scale_down
        assert status.logical_retirement_version == 3
        assert status.logical_retirement_controller_epoch == (
            mgr._logical_controller_epoch)
        assert status.logical_retirement_generation == 5
        assert status.logical_retirement_confirmed_generation == 5
        assert status.logical_retirement_bounded_deadline
        assert mgr._wait_for_idle_trackers[1][1] == tracker_deadline
        mgr._terminate_replica.assert_not_called()

        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, generation=6)
        mgr._logical_target = (3, 6, 1)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert not mgr._recovering_logical_retirement_ids
        assert status.is_scale_down
        assert status.logical_retirement_bounded_deadline
        mgr._terminate_replica.assert_not_called()

    def test_logical_update_aborts_budget_queued_uncommitted_victim(self):
        """Pin abort+reselect for a budget-queued (post-idle-proof) victim.

        A victim whose down worker is already queued behind the termination
        budget (SCHEDULED, wait_for_idle cleared, commit bit not yet
        persisted) is intentionally NOT handed off by an in-process update:
        it is safe to abort and reselect from fresh new-version evidence.
        """
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        old_epoch = mgr._logical_controller_epoch
        mgr._spot_placer = mock.Mock(name='old_placer')
        mgr._persist_replica = mock.Mock()

        queued = replica_managers.ReplicaInfo(replica_id=1,
                                              cluster_name='svc-1',
                                              replica_port='8080',
                                              is_spot=True,
                                              location=None,
                                              version=1,
                                              resources_override=None,
                                              planned_capacity=1)
        queued.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        queued.status_property.service_ready_now = True
        queued.status_property.is_scale_down = True
        queued.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        # Idle proof already consumed: the durable wait bit was cleared when
        # the down worker was scheduled, but admission has not yet persisted
        # the irreversible commit bit.
        queued.status_property.wait_for_idle_before_termination = False
        queued.status_property.logical_retirement_version = 1
        queued.status_property.logical_retirement_controller_epoch = old_epoch
        queued.status_property.logical_retirement_generation = 4
        queued.status_property.logical_retirement_target_capacity = 1
        queued.status_property.logical_retirement_confirmed_generation = 4
        queued.status_property.logical_retirement_bounded_deadline = False
        queued.status_property.logical_retirement_committed = False
        queued_down = mock.Mock()
        queued_down.is_alive.return_value = False
        mgr._down_thread_pool = {1: queued_down}
        mgr._wait_for_idle_trackers = {
            1: (mock.Mock(return_value=True),
                replica_managers.time.monotonic() + 300)
        }

        survivor = replica_managers.ReplicaInfo(replica_id=2,
                                                cluster_name='svc-2',
                                                replica_port='8080',
                                                is_spot=True,
                                                location=None,
                                                version=1,
                                                resources_override=None,
                                                planned_capacity=1)
        survivor.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        survivor.status_property.service_ready_now = True

        location = types.SimpleNamespace(accelerators={'L4': 1})
        new_task = types.SimpleNamespace(resources=[location], num_nodes=1)
        new_placer = mock.Mock(name='new_placer')
        new_placer.active_locations.return_value = [location]
        spec = types.SimpleNamespace(
            uses_logical_replicas=True,
            spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER,
            placement_contract=_LOGICAL_PLACEMENT_CONTRACT)
        yaml_content = ('resources: {}\n'
                        'file_mounts: {}\n'
                        'service: {readiness_probe: /}\n')

        with mock.patch.object(replica_managers.serve_state,
                               'get_yaml_content',
                               return_value=yaml_content), \
             mock.patch.object(replica_managers.serve_state,
                               'get_yaml_contents',
                               return_value={1: yaml_content}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_specs',
                               return_value={1: spec}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[queued, survivor]), \
             mock.patch.object(replica_managers,
                               'load_task_with_service_spec',
                               return_value=new_task), \
             mock.patch.object(replica_managers.spot_placer.SpotPlacer,
                               'from_task',
                               return_value=new_placer):
            mgr.update_version(2,
                               spec,
                               update_mode=serve_utils.UpdateMode.ROLLING,
                               new_spot_placer=new_placer)

        # The queued victim is outside the handoff: no epoch rotation, no
        # recovery entry, and the relabel leaves the old selection fence.
        assert mgr.latest_version == 2
        assert mgr._logical_controller_epoch == old_epoch
        assert not mgr._recovering_logical_retirement_ids
        # SHUTTING_DOWN is terminal, and only handed-off victims are eligible
        # for the runtime-equivalent relabel; the queued victim keeps its old
        # version and its old selection fence.
        assert queued.version == 1
        assert queued.status_property.logical_retirement_version == 1
        assert queued.status_property.is_scale_down

        # Post-apply, the refresh pass aborts the stale selection and returns
        # the backend to routing so vNext can reselect it from fresh evidence.
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=2,
                generation=5,
                observed_slots_by_replica_id={2: 1},
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (2, 5, 1)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: queued}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={queued.cluster_name: ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        assert not queued.status_property.is_scale_down
        assert queued.status_property.sky_down_status is None
        assert queued.status_property.logical_retirement_version is None
        assert 1 not in mgr._down_thread_pool

    def test_logical_update_rejects_multi_node_service_before_mutation(self):
        mgr = _make_manager()
        mgr.latest_version = 1
        mgr.yaml_content = 'old: yaml'
        old_placer = mock.Mock(name='old_placer')
        mgr._spot_placer = old_placer
        location = types.SimpleNamespace(accelerators={'A100': 8})
        new_task = types.SimpleNamespace(resources=[location], num_nodes=2)
        new_placer = mock.Mock(name='new_placer')
        new_placer.active_locations.return_value = [location]
        spec = types.SimpleNamespace(uses_logical_replicas=True)

        with mock.patch.object(replica_managers.serve_state,
                               'get_yaml_content',
                               return_value='new: yaml'), \
             mock.patch.object(replica_managers,
                               'load_task_with_service_spec',
                               return_value=new_task), \
             mock.patch.object(replica_managers.spot_placer.SpotPlacer,
                               'from_task',
                               return_value=new_placer), \
             pytest.raises(ValueError, match='only single-node services'):
            mgr.update_version(2,
                               spec,
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert mgr.latest_version == 1
        assert mgr.yaml_content == 'old: yaml'
        assert mgr._spot_placer is old_placer

    def test_logical_manager_rejects_physical_update_before_mutation(self):
        mgr = _make_manager()
        mgr.latest_version = 2
        mgr.yaml_content = 'logical: yaml'
        mgr._uses_logical_replicas = True
        physical = types.SimpleNamespace(uses_logical_replicas=False)

        with mock.patch.object(replica_managers.serve_state,
                               'get_yaml_content',
                               return_value='physical: yaml'), \
             pytest.raises(ValueError, match='back to physical'):
            mgr.update_version(3,
                               physical,
                               update_mode=serve_utils.UpdateMode.ROLLING)

        assert mgr.latest_version == 2
        assert mgr.yaml_content == 'logical: yaml'


class TestLaunchOwnershipFence:
    """A stale manager must never start work that was only queued locally."""

    @staticmethod
    def _pending_info(replica_id=1):
        info = mock.Mock()
        info.replica_id = replica_id
        info.replica_record_id = (f'00000000-0000-4000-8000-{replica_id:012d}')
        info.status = replica_managers.serve_state.ReplicaStatus.PENDING
        info.status_property = mock.Mock()
        info.created_at = 100.0
        return info

    @staticmethod
    def _owned_manager():
        mgr = _make_manager()
        mgr._service_hash = 'incarnation-a'
        mgr._controller_owner = (101, '10.0.0.1')
        mgr._ownership_lost = threading.Event()
        return mgr

    @classmethod
    def _queued_manager(cls, replica_ids):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()

        infos = {}
        for replica_id in replica_ids:
            thread = mock.Mock()
            thread.is_alive.return_value = False
            thread.format_exc = None
            mgr._launch_thread_pool[replica_id] = thread
            mgr._replica_to_request_id[replica_id] = f'req-{replica_id}'
            mgr._replica_to_launch_cancelled[replica_id] = False
            infos[replica_id] = cls._pending_info(replica_id)
        return mgr, infos

    def test_recovering_exact_owner_may_launch_from_controller_failed(self):
        mgr = self._owned_manager()
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'status':
                replica_managers.serve_state.ServiceStatus.CONTROLLER_FAILED,
        }
        with mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=owner):
            assert mgr._service_is_launch_authorized()
        assert not mgr._ownership_lost.is_set()

    def test_shutting_down_exact_owner_is_fenced(self):
        mgr = self._owned_manager()
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'status': replica_managers.serve_state.ServiceStatus.SHUTTING_DOWN,
        }
        with mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=owner):
            assert not mgr._service_is_launch_authorized()
        assert mgr._ownership_lost.is_set()

    def test_same_owner_epoch_advance_does_not_cancel_the_manager(self):
        mgr = self._owned_manager()
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'lifecycle_epoch': 18,
            'status': replica_managers.serve_state.ServiceStatus.READY,
        }
        with mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               return_value=owner):
            assert mgr._service_is_launch_authorized()
        assert not mgr._ownership_lost.is_set()

    def test_nonconsolidated_controller_omits_api_local_fence(self):
        mgr = self._owned_manager()
        mgr._enforce_launch_fence = False
        assert mgr._replica_launch_fence_context() is None

    def test_queued_launch_is_fenced_by_manager_and_recovery_epochs(self):
        mgr = self._owned_manager()
        mgr.latest_version = 1
        mgr._pending_version = None
        assert mgr._queued_launch_generation_decision(1) == (True, 'authorized')

        mgr._pending_version = 2
        assert mgr._queued_launch_generation_decision(1) == (
            False, 'newer-version-pending')
        mgr._pending_version = None
        mgr.latest_version = 2
        assert mgr._queued_launch_generation_decision(1) == (
            False, 'manager-version-changed')

        mgr.fence_launches_for_update_recovery()
        assert mgr._queued_launch_generation_decision(2) == (
            False, 'controller-update-recovery-required')
        assert not mgr._service_is_launch_authorized()

    def test_transient_owner_lookup_fails_attempt_without_latching_loss(self):
        mgr = self._owned_manager()
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'status': replica_managers.serve_state.ServiceStatus.READY,
        }
        with mock.patch.object(
                replica_managers.serve_state,
                'get_service_controller_owner',
                side_effect=[RuntimeError('database restarting'), owner]):
            # The current launch fails closed, but the manager remains able to
            # prove ownership and recover on the next check.
            assert not mgr._service_is_launch_authorized()
            assert not mgr._ownership_lost.is_set()
            assert mgr._service_is_launch_authorized()
        assert not mgr._ownership_lost.is_set()

    def test_owner_watchdog_retries_transient_lookup(self):
        mgr = self._owned_manager()
        current_owner = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
            'status': replica_managers.serve_state.ServiceStatus.READY,
        }
        replacement_owner = {
            **current_owner,
            'controller_pid': 202,
        }
        with mock.patch.object(
                replica_managers.serve_state,
                'get_service_controller_owner',
                side_effect=[
                    RuntimeError('database restarting'), current_owner,
                    replacement_owner
                ]) as get_owner, \
             mock.patch.object(
                 replica_managers,
                 '_SERVICE_OWNER_WATCH_INTERVAL_SECONDS',
                 0):
            mgr._service_owner_watchdog()

        assert get_owner.call_count == 3
        assert mgr._ownership_lost.is_set()

    def test_transient_lookup_defers_queued_launch_instead_of_discarding(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = False
        mgr._launch_thread_pool[1] = launch_thread
        info = self._pending_info()

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=None), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: info for rid in ids}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        launch_thread.start.assert_not_called()
        assert mgr._launch_thread_pool[1] is launch_thread
        persist.assert_not_called()

    def test_transient_lookup_is_shared_across_queued_launches(self):
        mgr, infos = self._queued_manager([1, 2, 3])

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=None) as authorize, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        assert authorize.call_count == 1
        for launch_thread in mgr._launch_thread_pool.values():
            launch_thread.start.assert_not_called()
        assert len(mgr._launch_thread_pool) == 3
        for replica_id in (1, 2, 3):
            assert replica_id in mgr._launch_thread_pool
        persist.assert_not_called()

    def test_stale_queued_launch_is_discarded_without_deleting_row(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = False
        mgr._launch_thread_pool[1] = launch_thread
        info = self._pending_info()

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=False), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: info for rid in ids}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        launch_thread.start.assert_not_called()
        assert 1 not in mgr._launch_thread_pool
        persist.assert_not_called()

    def test_stale_lookup_is_shared_across_queued_launches(self):
        mgr, infos = self._queued_manager([1, 2, 3])

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=False) as authorize, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        assert authorize.call_count == 1
        assert len(mgr._launch_thread_pool) == 0
        assert len(mgr._replica_to_request_id) == 0
        assert len(mgr._replica_to_launch_cancelled) == 0
        persist.assert_not_called()

    def test_completed_launch_wave_uses_one_batch_before_cleanup(self):
        mgr, infos = self._queued_manager([1, 2])
        for info in infos.values():
            info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        mgr._launch_thread_pool[1].format_exc = None
        mgr._launch_thread_pool[2].format_exc = 'no capacity'
        events = []

        def _persist(updates):
            events.append(
                ('persist', [replica_id for replica_id, _ in updates]))

        def _terminate(replica_id, **_kwargs):
            events.append(('terminate', replica_id))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value=infos), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=list(infos.values())), \
             mock.patch.object(mgr,
                               '_persist_replicas',
                               side_effect=_persist) as persist, \
             mock.patch.object(mgr,
                               '_terminate_replica',
                               side_effect=_terminate), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        persist.assert_called_once_with([(1, infos[1]), (2, infos[2])])
        assert events == [('persist', [1, 2]), ('terminate', 2)]
        assert len(mgr._launch_thread_pool) == 0
        assert len(mgr._replica_to_request_id) == 0

    def test_bound_pre_effect_completion_queues_generation_successor(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        error = replica_managers._BoundOrdinaryLaunchPreEffectTerminalError(
            'pre-effect')

        def _fail():
            raise error

        old_thread = replica_managers._ReplicaLaunchThread(
            target=_fail,
            replica_id=1,
            completion_queue=queue.SimpleQueue(),
            completion_event=threading.Event(),
            bound_ordinary_launch=True)
        old_thread.start()
        old_thread.join()
        mgr._launch_thread_pool[1] = old_thread
        mgr._replica_to_request_id[1] = 'generation-1-request'
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PENDING)
        info.paid_capacity_pool_key = 'pool-a'
        successor = mock.Mock(name='generation-2-worker')

        def _redrive(redrive_info):
            assert redrive_info is info
            mgr._launch_thread_pool[1] = successor
            return True

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value={1: info}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[]), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch',
                 return_value=None), \
             mock.patch.object(
                 mgr,
                 '_redrive_bound_ordinary_launch_after_pre_effect',
                 side_effect=_redrive) as redrive, \
             mock.patch.object(mgr,
                               '_emit_ordinary_launch_handoff_event'), \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        redrive.assert_called_once_with(info)
        assert mgr._launch_thread_pool[1] is successor
        assert 1 not in mgr._replica_to_request_id
        terminate.assert_not_called()

    def test_current_owner_redrives_finished_unresolved_bound_worker(self):
        """A local lost acknowledgement is not an ownership-loss detach."""
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        error = replica_managers._BoundOrdinaryLaunchUnresolvedError(
            'response lost before pointer readback')

        def _fail():
            raise error

        old_thread = replica_managers._ReplicaLaunchThread(
            target=_fail,
            replica_id=1,
            completion_queue=queue.SimpleQueue(),
            completion_event=threading.Event(),
            bound_ordinary_launch=True)
        old_thread.start()
        old_thread.join()
        mgr._launch_thread_pool[1] = old_thread
        mgr._replica_to_request_id[1] = 'unconfirmed-request'
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PENDING)
        successor = mock.Mock(name='same-controller-successor')

        def _redrive(redrive_info):
            assert redrive_info is info
            mgr._launch_thread_pool[1] = successor
            return True

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value={1: info}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[]), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch',
                 return_value=None), \
             mock.patch.object(
                 mgr,
                 '_redrive_bound_ordinary_launch_after_pre_effect',
                 side_effect=_redrive) as redrive, \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        redrive.assert_called_once_with(info)
        assert mgr._launch_thread_pool[1] is successor
        assert 1 not in mgr._replica_to_request_id
        terminate.assert_not_called()

    def test_finished_unresolved_bound_teardown_reenters_exact_cleanup(self):
        """A pointerless finished waiter cannot strand teardown intent."""
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        error = replica_managers._BoundOrdinaryLaunchUnresolvedError(
            'pointer cleared while the teardown worker was settling')

        def _fail():
            raise error

        old_thread = replica_managers._ReplicaLaunchThread(
            target=_fail,
            replica_id=1,
            completion_queue=queue.SimpleQueue(),
            completion_event=threading.Event(),
            bound_ordinary_launch=True)
        old_thread.start()
        old_thread.join()
        mgr._launch_thread_pool[1] = old_thread
        mgr._replica_to_request_id[1] = 'settled-request'
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.INTERRUPTED)
        info.status_property.is_scale_down = True
        info.status_property.purged = False
        info.status_property.drain_cap_seconds = 47
        assert info.status == (
            replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value={1: info}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[]), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch',
                 return_value=None), \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        launch.assert_not_called()
        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          purge=False,
                                          is_scale_down=True,
                                          in_flight_drain_cap_seconds=47)
        assert 1 not in mgr._launch_thread_pool
        assert 1 not in mgr._replica_to_request_id

    def test_bound_parent_running_write_cannot_overwrite_child_projection(self):
        """The child projection wins a start-vs-bookkeeping race."""
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PENDING)
        events = []
        launch_thread = mock.Mock(spec=replica_managers._ReplicaLaunchThread)
        launch_thread.bound_ordinary_launch = True
        launch_thread.ident = None
        launch_thread.is_alive.return_value = False
        launch_thread.exception = None
        launch_thread.format_exc = None
        launch_thread.start.side_effect = lambda: events.append(
            'child-projected')
        mgr._launch_thread_pool[1] = launch_thread

        def _mark_running(*_args, **_kwargs):
            events.append('parent-conditional-running')
            return False

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                side_effect=lambda _service, ids: ({1: info} if ids else {})), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[]), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch') as inspect, \
             mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=True), \
             mock.patch.object(
                 mgr,
                 '_logical_pending_launch_admission',
                 return_value=(False, None, set())), \
             mock.patch.object(
                 replica_managers.controller_utils,
                 'in_flight_launch_count',
                 return_value=0), \
             mock.patch.object(replica_managers.controller_utils,
                               'can_provision',
                               return_value=True), \
             mock.patch.object(replica_managers.filelock, 'FileLock'), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'mark_bound_replica_launch_running_if_active',
                 side_effect=_mark_running) as mark_running, \
             mock.patch.object(mgr, '_persist_replica') as persist, \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        inspect.assert_not_called()
        launch_thread.start.assert_called_once_with()
        mark_running.assert_called_once_with('svc', 1, info.replica_record_id)
        assert events == ['child-projected', 'parent-conditional-running']
        assert info.status == replica_managers.serve_state.ReplicaStatus.PENDING
        persist.assert_not_called()

    def test_finished_launch_cleanup_orders_v2_across_outcome_types(self):
        mgr, infos = self._queued_manager([1, 2])
        ordinary = infos[1]
        ordinary.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        ordinary.reserved_fill = False
        ordinary.reserved_fill_pool_key = None
        ordinary.reserved_fill_service_generation = None
        ordinary.reserved_fill_physical_cluster_uid = None
        ordinary.reserved_fill_kubernetes_context = None
        fenced = infos[2]
        fenced.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        _stamp_protocol_v2_fill(fenced)

        ordinary_thread = mgr._launch_thread_pool[1]
        ordinary_thread.format_exc = 'superseded'
        ordinary_thread.exception = (
            replica_managers._ReplicaLaunchSupersededError('superseded'))
        fenced_thread = mgr._launch_thread_pool[2]
        fenced_thread.format_exc = 'launch failed'
        fenced_thread.exception = RuntimeError('launch failed')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value=infos), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=list(infos.values())), \
             mock.patch.object(
                 replica_managers.paid_capacity,
                 'persist_completed_launches',
                 return_value=None), \
             mock.patch.object(mgr, '_persist_replicas'), \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        assert [call.args[0] for call in terminate.call_args_list] == [2, 1]

    def test_completed_launch_batch_failure_keeps_workers_for_retry(self):
        mgr, infos = self._queued_manager([1, 2])
        for info in infos.values():
            info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        mgr._launch_thread_pool[1].format_exc = None
        mgr._launch_thread_pool[2].format_exc = 'no capacity'

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value=infos), \
             mock.patch.object(mgr,
                               '_persist_replicas',
                               side_effect=RuntimeError(
                                   'database unavailable')), \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             pytest.raises(RuntimeError, match='database unavailable'):
            mgr._refresh_thread_pool()

        assert {
            replica_id for replica_id, _ in mgr._launch_thread_pool.items()
        } == {1, 2}
        assert {
            replica_id for replica_id, _ in mgr._replica_to_request_id.items()
        } == {1, 2}
        terminate.assert_not_called()

    def test_unfenced_external_lb_failure_stops_replica_churn(self):
        mgr, infos = self._queued_manager([1])
        info = infos[1]
        info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        launch_thread = mgr._launch_thread_pool[1]
        launch_thread.format_exc = 'missing durable owner fence'
        launch_thread.exception = (
            replica_managers._UnfencedExternalLbLaunchError('unfenced'))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value=infos), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]), \
             mock.patch.object(mgr, '_persist_replicas') as persist, \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        assert info.status_property.user_app_failed is True
        assert (info.status_property.sky_launch_status ==
                common_utils.ProcessStatus.FAILED)
        persist.assert_called_once_with([(1, info)])
        terminate.assert_called_once_with(1,
                                          sync_down_logs=True,
                                          replica_drain_delay_seconds=0)

        terminal = replica_managers.ReplicaStatusProperty(
            sky_launch_status=common_utils.ProcessStatus.FAILED,
            sky_down_status=common_utils.ProcessStatus.SUCCEEDED,
            user_app_failed=True)
        assert (terminal.to_replica_status() ==
                replica_managers.serve_state.ReplicaStatus.FAILED)
        assert terminal.unrecoverable_failure() is True

    def test_superseded_fill_launch_releases_pin_and_schedules_cleanup(self):
        mgr, infos = self._queued_manager([1])
        info = infos[1]
        info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        info.reserved_fill = True
        location = mock.Mock()
        info.get_spot_location.return_value = location
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        mgr._spot_placer = placer
        launch_thread = mgr._launch_thread_pool[1]
        launch_thread.format_exc = 'pool retargeted'
        launch_thread.exception = (
            replica_managers._ReplicaLaunchSupersededError('retargeted'))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value=infos), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]), \
             mock.patch.object(mgr, '_persist_replicas') as persist, \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             mock.patch.object(mgr,
                               '_persist_spot_placement_state_if_dirty'), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        placer.release_retry.assert_called_once_with(location)
        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True,
                                          in_flight_drain_cap_seconds=0)
        persist.assert_not_called()

    def test_unfenced_external_lb_failure_does_not_bench_spot_location(self):
        mgr, infos = self._queued_manager([1])
        info = infos[1]
        info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        info.status_property.failed_spot_availability = False
        location = mock.Mock()
        info.get_spot_location.return_value = location
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        mgr._spot_placer = placer
        launch_thread = mgr._launch_thread_pool[1]
        launch_thread.format_exc = 'missing durable owner fence'
        launch_thread.exception = (
            replica_managers._UnfencedExternalLbLaunchError('unfenced'))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value=infos), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos',
                 return_value=[info]), \
             mock.patch.object(mgr, '_persist_replicas'), \
             mock.patch.object(mgr, '_terminate_replica'), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'):
            mgr._refresh_thread_pool()

        assert info.status_property.user_app_failed is True
        assert info.status_property.failed_spot_availability is False
        placer.set_active.assert_not_called()
        placer.set_preemptive.assert_not_called()

    def test_unrecoverable_failure_check_does_not_log_per_replica(self):
        status = replica_managers.ReplicaStatusProperty(
            sky_launch_status=common_utils.ProcessStatus.FAILED,
            sky_down_status=common_utils.ProcessStatus.SUCCEEDED,
            user_app_failed=True)

        with mock.patch.object(replica_managers, 'logger') as logger:
            results = [status.unrecoverable_failure() for _ in range(2_159)]

        assert all(results)
        assert logger.mock_calls == []

    def test_safe_thread_exposes_captured_exception(self):
        error = RuntimeError('typed failure')

        def fail():
            raise error

        launch_thread = thread_utils.SafeThread(target=fail)
        launch_thread.run()

        assert launch_thread.exception is error
        assert launch_thread.format_exc is not None

    def test_authorized_lookup_is_shared_across_queued_launches(self, tmp_path):
        mgr, infos = self._queued_manager([1, 2, 3])
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        placer.is_launch_admissible.return_value = True
        mgr._spot_placer = placer
        for info in infos.values():
            info.get_spot_location.return_value = location

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=True) as authorize, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_provision',
                               return_value=True), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        assert authorize.call_count == 1
        for info in infos.values():
            assert (info.status_property.sky_launch_status ==
                    common_utils.ProcessStatus.RUNNING)
        for launch_thread in mgr._launch_thread_pool.values():
            launch_thread.start.assert_called_once_with()
        placer.is_launch_admissible.assert_has_calls(
            [mock.call(location, selected_at=100.0)] * 3)
        placer.refresh_workspace_policy.assert_called_once_with()
        assert placer.mock_calls.index(
            mock.call.refresh_workspace_policy()) < placer.mock_calls.index(
                mock.call.is_launch_admissible(location, selected_at=100.0))
        assert persist.call_count == 3

    def test_fresh_exact_target_supersedes_excess_before_thread_start(
            self, tmp_path):
        mgr, infos = self._queued_manager([1, 2])
        mgr._uses_logical_replicas = True
        mgr._logical_exact_accelerator_shapes = {'L4': 1}
        fence = (1, 7, 1, (('L4', 1),), (('L4', 1),))
        mgr._logical_target = fence
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        for replica_id, info in infos.items():
            info.replica_id = replica_id
            info.version = 1
            info.reserved_fill = False
            info.unknown_capacity_replacement = False
            info.cost_rebalance_for_replica_id = None

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=True), \
             mock.patch.object(mgr,
                               '_logical_pending_launch_admission',
                               return_value=(True, fence, {1})), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils,
                               'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_provision',
                               return_value=True), \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             mock.patch.object(mgr, '_persist_replica'):
            mgr._refresh_thread_pool()

        mgr._launch_thread_pool[1].start.assert_called_once_with()
        mgr._launch_thread_pool[2].start.assert_not_called()
        terminate.assert_called_once_with(2,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True,
                                          in_flight_drain_cap_seconds=0)
        assert mgr._replica_to_logical_launch_fence[1] == fence

    def test_consumed_retry_is_admitted_once(self, tmp_path):
        mgr, infos = self._queued_manager([1])
        launch_thread = mgr._launch_thread_pool[1]
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        placer.is_active_location.return_value = False
        placer.is_launch_admissible.return_value = True
        mgr._spot_placer = placer
        infos[1].get_spot_location.return_value = location

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=True), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value=infos), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils,
                               'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_provision',
                               return_value=True), \
             mock.patch.object(mgr, '_remove_replica') as remove, \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        placer.is_launch_admissible.assert_called_once_with(location,
                                                            selected_at=100.0)
        placer.is_active_location.assert_not_called()
        launch_thread.start.assert_called_once_with()
        remove.assert_not_called()
        persist.assert_called_once_with(1, infos[1])

    def test_benched_placement_discards_queued_wave(self):
        mgr, infos = self._queued_manager([1, 2, 3])
        launch_threads = list(mgr._launch_thread_pool.values())
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        placer.is_launch_admissible.return_value = False
        mgr._spot_placer = placer
        for info in infos.values():
            info.get_spot_location.return_value = location

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=True), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_remove_replica') as remove, \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        placer.is_launch_admissible.assert_has_calls(
            [mock.call(location, selected_at=100.0)] * 3)
        assert remove.call_args_list == [
            mock.call(1, infos[1].replica_record_id),
            mock.call(2, infos[2].replica_record_id),
            mock.call(3, infos[3].replica_record_id)
        ]
        for launch_thread in launch_threads:
            launch_thread.start.assert_not_called()
        assert len(mgr._launch_thread_pool) == 0
        assert len(mgr._replica_to_request_id) == 0
        assert len(mgr._replica_to_launch_cancelled) == 0
        persist.assert_not_called()

    def test_failure_overrides_sibling_success_before_queue_admission(self):
        mgr, infos = self._queued_manager([1, 2, 3])
        launch_threads = list(mgr._launch_thread_pool.values())
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        mgr._spot_placer = placer
        for info in infos.values():
            info.get_spot_location.return_value = location
            info.created_at = 123.0
        infos[
            1].status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        infos[
            2].status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        launch_threads[0].format_exc = 'no capacity'
        launch_threads[0].exception = (
            replica_managers._ReplicaLaunchCapacityError('no capacity',
                                                         reason='capacity'))
        launch_threads[1].format_exc = None

        with mock.patch.object(mgr,
                               '_service_launch_authorization',
                               return_value=True), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               {rid: infos[rid] for rid in ids}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_remove_replica') as remove, \
             mock.patch.object(mgr, '_persist_replicas'), \
             mock.patch.object(mgr, '_terminate_replica'):
            mgr._refresh_thread_pool()

        placer.set_active.assert_not_called()
        placer.set_preemptive.assert_called_once_with(location,
                                                      reason='capacity')
        remove.assert_called_once_with(3, infos[3].replica_record_id)
        launch_threads[2].start.assert_not_called()
        assert len(mgr._launch_thread_pool) == 0

    def test_success_reactivation_uses_launch_selection_time(self):
        mgr, infos = self._queued_manager([1])
        launch_thread = mgr._launch_thread_pool[1]
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        mgr._spot_placer = placer
        info = infos[1]
        info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        info.created_at = 123.0
        info.get_spot_location.return_value = location
        launch_thread.format_exc = None

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value=infos), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr, '_persist_replicas'), \
             mock.patch.object(mgr, '_terminate_replica'):
            mgr._refresh_thread_pool()

        placer.set_active.assert_called_once_with(location, selected_at=123.0)
        placer.set_preemptive.assert_not_called()

    @pytest.mark.parametrize('failure_stage', ('persist', 'cleanup'))
    def test_launch_failure_benches_before_fallible_cleanup(
            self, failure_stage):
        mgr, infos = self._queued_manager([1, 2])
        failed_thread = mgr._launch_thread_pool[1]
        failed_thread.format_exc = 'no capacity'
        failed_thread.exception = (replica_managers._ReplicaLaunchCapacityError(
            'no capacity', reason='capacity'))
        infos[
            1].status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        location = mock.Mock()
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        mgr._spot_placer = placer
        for info in infos.values():
            info.get_spot_location.return_value = location

        events = []
        placer.set_preemptive.side_effect = lambda _location, **_kwargs: (
            events.append('bench'))

        def _persist(*_args, **_kwargs):
            events.append('persist')
            if failure_stage == 'persist':
                raise RuntimeError('persist unavailable')

        def _fail_cleanup(*_args, **_kwargs):
            events.append('cleanup')
            if failure_stage == 'cleanup':
                raise RuntimeError('cleanup unavailable')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                side_effect=lambda _svc, ids: {
                    rid: infos[rid]
                    for rid in ids
                }), \
             mock.patch.object(mgr,
                               '_persist_replicas',
                               side_effect=_persist), \
             mock.patch.object(mgr,
                               '_terminate_replica',
                               side_effect=_fail_cleanup), \
             pytest.raises(RuntimeError,
                           match=f'{failure_stage} unavailable'):
            mgr._refresh_thread_pool()

        expected = ['bench', 'persist']
        if failure_stage == 'cleanup':
            expected.append('cleanup')
        assert events == expected
        placer.set_preemptive.assert_called_once_with(location,
                                                      reason='capacity')
        assert 2 in mgr._launch_thread_pool
        mgr._launch_thread_pool[2].start.assert_not_called()

    def test_old_version_metadata_is_retained(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._spot_placer = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        info = mock.Mock(version=2)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup') as reconcile:
            mgr._refresh_thread_pool()

        reconcile.assert_called_once_with([info])


class TestCloudInstanceLooksAlive:
    """The spot-preemption pre-filter must be cloud-API-only and
    conservative: it decides whether a failed readiness probe warrants the
    expensive full `_handle_preemption` path (forced refresh under the
    manager lock). During a fleet cold start every not-yet-listening spot
    replica fails its probe, so the common case must be one cheap provider
    call confirming the instance is up."""

    @staticmethod
    def _spot_info():
        info = mock.Mock()
        info.is_spot = True
        info.cluster_name = 'svc-1'
        info.replica_id = 1
        return info

    def _run(self, handle, statuses=None, side_effect=None):
        mgr = _make_manager()
        with mock.patch(
                'sky.serve.replica_managers.global_user_state.'
                'get_handle_from_cluster_name',
                return_value=handle), \
             mock.patch(
                 'sky.serve.replica_managers.backend_utils.'
                 'query_cluster_instance_statuses',
                 return_value=statuses,
                 side_effect=side_effect) as query:
            result = mgr._cloud_instance_looks_alive(self._spot_info())
        return result, query

    @staticmethod
    def _handle(launched_nodes=1):
        handle = mock.Mock(
            spec=replica_managers.backends.CloudVmRayResourceHandle)
        handle.launched_nodes = launched_nodes
        return handle

    def test_running_instance_counts_as_alive(self):
        from sky.utils import status_lib
        result, query = self._run(
            self._handle(),
            statuses={'i-1': (status_lib.ClusterStatus.UP, None)})
        assert result is True
        query.assert_called_once()

    def test_partially_up_multinode_counts_as_dead(self):
        # Mirrors the full refresh's partial-cluster semantics: a 2-node
        # replica with only 1 instance UP is abnormal, not alive.
        from sky.utils import status_lib
        result, _ = self._run(
            self._handle(launched_nodes=2),
            statuses={'i-1': (status_lib.ClusterStatus.UP, None)})
        assert result is False

    def test_multinode_with_stopped_member_counts_as_dead(self):
        from sky.utils import status_lib
        result, _ = self._run(self._handle(launched_nodes=2),
                              statuses={
                                  'i-1': (status_lib.ClusterStatus.UP, None),
                                  'i-2': (status_lib.ClusterStatus.STOPPED,
                                          'preempted'),
                              })
        assert result is False

    def test_no_instances_counts_as_dead(self):
        result, _ = self._run(self._handle(), statuses={})
        assert result is False

    def test_stopped_instance_counts_as_dead(self):
        from sky.utils import status_lib
        result, _ = self._run(
            self._handle(),
            statuses={'i-1': (status_lib.ClusterStatus.STOPPED, 'preempted')})
        assert result is False

    def test_provider_error_counts_as_alive(self):
        # A transient provider error must not stampede a cold-starting
        # fleet into forced refreshes.
        result, _ = self._run(self._handle(),
                              side_effect=RuntimeError('throttled'))
        assert result is True

    def test_missing_handle_routes_to_full_path(self):
        # No handle -> NOT alive, so the full _handle_preemption (which
        # logs and handles the missing-handle case) runs.
        result, query = self._run(handle=None)
        assert result is False
        query.assert_not_called()


class TestInfrastructureInterruptionRecovery:
    """Research Kubernetes pods use the recoverable spot lifecycle.

    Every service pod on a configured zero-cost research location is
    low-priority and reclaimable, including ordinary demand pods. Reclamation
    must replace the backend without benching the still-healthy research pool.
    """

    @staticmethod
    def _location(*, cloud='Kubernetes', region='research-ctx', use_spot=False):
        return replica_managers.spot_placer.Location.from_pickleable({
            'cloud': cloud,
            'region': region,
            'zone': None,
            'accelerators': {
                'A100' if cloud == 'Kubernetes' else 'L4': 1
            },
            'use_spot': use_spot,
        })

    @staticmethod
    def _info(location, *, is_spot=False, ready=False):
        info = replica_managers.ReplicaInfo(replica_id=1,
                                            cluster_name='svc-1',
                                            replica_port='8080',
                                            is_spot=is_spot,
                                            location=location,
                                            version=1,
                                            resources_override=None)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.reserved_fill = False
        if ready:
            info.status_property.first_ready_time = 1.0
            info.status_property.service_ready_now = True
        return info

    @staticmethod
    def _handle():
        return mock.Mock(
            spec=replica_managers.backends.CloudVmRayResourceHandle)

    def _manager(self, zero_cost):
        manager = _make_manager()
        manager._spot_placer = mock.Mock()
        manager._spot_placer.zero_cost_locations.return_value = [zero_cost]
        return manager

    def test_non_fill_research_replica_is_interruptible(self):
        research = self._location()
        manager = self._manager(research)
        info = self._info(research)

        assert info.reserved_fill is False
        assert manager._is_interruptible_replica(info) is True

    def test_unrelated_nonspot_replica_is_not_interruptible(self):
        research = self._location()
        unrelated = self._location(cloud='AWS', region='us-east-1')
        manager = self._manager(research)

        assert manager._is_interruptible_replica(self._info(unrelated)) is False

    def test_reclaimed_research_replica_reuses_recoverable_lifecycle(self):
        research = self._location()
        manager = self._manager(research)
        info = self._info(research)

        with mock.patch.object(
                replica_managers.global_user_state,
                'get_handle_from_cluster_name',
                return_value=self._handle()), \
             mock.patch.object(
                 replica_managers.backend_utils,
                 'refresh_cluster_status_handle',
                 return_value=(None, None)), \
             mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_terminate_replica') as terminate:
            assert manager._handle_preemption(info) is True

        assert info.status_property.preempted is True
        persist.assert_called_once_with(1, info)
        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True)
        manager._spot_placer.set_preemptive.assert_not_called()

        # A reclaimed backend before first readiness must not brick the
        # version as an unrecoverable application failure.
        info.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        assert info.status_property.unrecoverable_failure() is False

    def test_running_research_replica_is_not_reclaimed(self):
        research = self._location()
        manager = self._manager(research)
        info = self._info(research)
        from sky.utils import status_lib

        with mock.patch.object(
                replica_managers.global_user_state,
                'get_handle_from_cluster_name',
                return_value=self._handle()), \
             mock.patch.object(
                 replica_managers.backend_utils,
                 'refresh_cluster_status_handle',
                 return_value=(status_lib.ClusterStatus.UP, None)), \
             mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_terminate_replica') as terminate:
            assert manager._handle_preemption(info) is False

        persist.assert_not_called()
        terminate.assert_not_called()

    @pytest.mark.parametrize('is_spot', [False, True])
    def test_missing_handle_recovers_interrupted_replica(self, is_spot):
        research = self._location()
        manager = self._manager(research)
        location = (self._location(cloud='AWS',
                                   region='us-east-1',
                                   use_spot=True) if is_spot else research)
        info = self._info(location, is_spot=is_spot, ready=True)

        with mock.patch.object(
                replica_managers.global_user_state,
                'get_handle_from_cluster_name',
                return_value=None), \
             mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_terminate_replica') as terminate:
            assert manager._handle_preemption(info) is True

        assert info.status_property.preempted is True
        persist.assert_called_once_with(1, info)
        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True)
        if is_spot:
            manager._spot_placer.set_preemptive.assert_called_once_with(
                location, reason='preempted')
        else:
            manager._spot_placer.set_preemptive.assert_not_called()

    def test_spot_interruption_still_benches_location(self):
        research = self._location()
        spot = self._location(cloud='AWS', region='us-east-1', use_spot=True)
        manager = self._manager(research)
        info = self._info(spot, is_spot=True)

        with mock.patch.object(
                replica_managers.global_user_state,
                'get_handle_from_cluster_name',
                return_value=self._handle()), \
             mock.patch.object(
                 replica_managers.backend_utils,
                 'refresh_cluster_status_handle',
                 return_value=(None, None)), \
             mock.patch.object(manager, '_persist_replica'), \
             mock.patch.object(manager, '_terminate_replica'):
            assert manager._handle_preemption(info) is True

        manager._spot_placer.set_preemptive.assert_called_once_with(
            spot, reason='preempted')

    @pytest.mark.parametrize('changed_only', [False, True])
    def test_failed_research_probe_enters_interruption_prefilter(
            self, changed_only):
        research = self._location()
        manager = self._manager(research)
        info = self._info(research)
        info.probe = mock.Mock(return_value=(info, False, 100.0))
        manager._is_pool = False
        manager._uptime = 1.0
        manager._tick_version_spec_cache = {}
        manager._resolve_probe_urls = mock.Mock(
            return_value={1: 'http://10.0.0.1:8080'})
        manager._get_readiness_path = mock.Mock(return_value='/health')
        manager._get_post_data = mock.Mock(return_value=None)
        manager._get_readiness_timeout_seconds = mock.Mock(return_value=15)
        manager._get_readiness_headers = mock.Mock(return_value=None)
        manager._cloud_instance_looks_alive = mock.Mock(return_value=False)
        manager._handle_preemption = mock.Mock(return_value=True)
        manager._persist_replicas = mock.Mock()
        manager._changed_only_readiness_persistence = changed_only

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_specs',
                               return_value={1: mock.Mock()}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={}):
            manager._probe_all_replicas()

        manager._cloud_instance_looks_alive.assert_called_once_with(
            info, phase_admission=mock.ANY)
        manager._handle_preemption.assert_called_once_with(info)
        if changed_only:
            manager._persist_replicas.assert_not_called()
        else:
            manager._persist_replicas.assert_called_once_with([])

    @pytest.mark.parametrize('persisted_intent', [False, True])
    def test_recovery_redrives_reclaimed_research_replica_without_bench(
            self, persisted_intent):
        research = self._location()
        manager = self._manager(research)
        info = self._info(research, ready=True)
        info.status_property.preempted = persisted_intent

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={}), \
             mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_terminate_replica') as terminate:
            manager._recover_replica_operations()

        assert info.status_property.preempted is True
        if persisted_intent:
            persist.assert_not_called()
        else:
            persist.assert_called_once_with(1, info)
        terminate.assert_called_once()
        manager._spot_placer.set_preemptive.assert_not_called()


class TestScaleUpBatch:
    """A batch of scale-ups must run under ONE manager-lock acquisition:
    the probe round holds the lock for tens of seconds per round on large
    fleets, so per-replica acquisitions trickle through the gaps and
    become the fleet-scale launch bottleneck (measured live at a
    1000-target fleet)."""

    class _CountingLock:

        def __init__(self):
            self.acquisitions = 0

        def __enter__(self):
            self.acquisitions += 1
            return self

        def __exit__(self, *args):
            return False

    def test_batch_launches_all_with_one_lock_acquisition(self):
        mgr = _make_manager(next_replica_id=1)
        lock = self._CountingLock()
        mgr.lock = lock
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids',
                return_value=set()) as id_scan, \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up_batch([None, {'use_spot': True}, None])
        assert launched == [1, 2, 3]
        assert mgr._next_replica_id == 4
        assert lock.acquisitions == 1
        # The collision guard reads the id set ONCE per batch, not once per
        # replica launched.
        assert id_scan.call_count == 1

    def test_single_scale_up_unchanged(self):
        mgr = _make_manager(next_replica_id=7)
        lock = self._CountingLock()
        mgr.lock = lock
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids',
                return_value=set()), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up()
        assert launched == [7]
        assert lock.acquisitions == 1

    def test_batch_skips_ids_with_existing_rows(self):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        launched = []

        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids',
                return_value={2}), \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up_batch([None, None])
        assert launched == [1, 3]

    def test_spot_batch_reuses_one_replica_snapshot(self):
        """K placer launches must scan/unpickle the N-row table once.

        The shared list must also accumulate each newly enqueued replica so
        reserved-capacity accounting sees in-wave reservations. Cost-first
        placement must not scan the N existing rows for location load. The
        current service must come from the same global snapshot used for
        cross-service capacity; combining a separate local read with a later
        global read can mix two database states in one placement decision.
        """
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr._spot_placer = mock.Mock()
        mgr._spot_placer.active_locations.return_value = []
        mgr._spot_placer.ranked_active_locations.return_value = []
        mgr._spot_placer.zero_cost_locations.return_value = []
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=True)
        mgr.yaml_content = 'dummy: yaml'
        initial = [_fake_replica_info(40), _fake_replica_info(41)]
        for info in initial:
            info.get_spot_location = mock.Mock(wraps=info.get_spot_location)
        stale_local = [_fake_replica_info(99)]
        snapshots = []
        reservation_lock = mock.MagicMock()

        def _launch(replica_id,
                    _resources_override,
                    existing_replica_infos=None,
                    **_kwargs):
            assert existing_replica_infos is not None
            snapshots.append(
                (existing_replica_infos, len(existing_replica_infos)))
            existing_replica_infos.append(_fake_replica_info(replica_id))
            return _accepted_launch_result(replica_id)

        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids') as id_scan, \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos',
                 return_value=stale_local) as local_scan, \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.'
                 'get_replica_infos_grouped',
                 return_value={'svc': list(initial)}) as grouped_scan, \
             mock.patch.object(replica_managers.locks,
                               'get_lock',
                               return_value=reservation_lock), \
             mock.patch.object(mgr,
                               '_build_zero_cost_demand_budget',
                               return_value=None), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr.scale_up_batch([{'use_spot': True}] * 3)

        local_scan.assert_not_called()
        grouped_scan.assert_called_once_with()
        # The id set is derived from the placement snapshot; no second query.
        id_scan.assert_not_called()
        assert [size for _, size in snapshots] == [2, 3, 4]
        assert all(snapshot is snapshots[0][0] for snapshot, _ in snapshots)
        for info in initial:
            info.get_spot_location.assert_not_called()
        mgr._spot_placer.refresh_workspace_policy.assert_called_once_with()

    def test_productive_profile_raises_physical_window_without_crossing_cap(
            self, monkeypatch):
        location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        mgr = _make_manager(next_replica_id=1)
        mgr._spot_placer = make_placer({location: 1.0})
        mgr._spot_placer.num_nodes = 1
        mgr._workspace = 'w'
        mgr._service_hash = 'hash'
        mgr._controller_owner = (1, '10.0.0.1')
        mgr._pending_version = None
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        mgr.yaml_content = 'resources:\n  use_spot: true\n'
        mgr._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        mgr._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        key = paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        monkeypatch.setenv(
            paid_capacity._SERVICE_LIMIT_PROFILES_ENV_VAR,
            json.dumps({
                'version': 1,
                'profiles': [{
                    'workspace': 'w',
                    'service_name': 'svc',
                    'service_hash': 'hash',
                    'max_launch_window': 24,
                }],
            }))

        with mock.patch.object(
                paid_capacity, 'central_authority_available',
                return_value=True), mock.patch.object(
                    replica_managers.serve_state,
                    'get_replica_infos',
                    return_value=[]), mock.patch.object(
                        replica_managers.serve_state,
                        'get_paid_capacity_pool_states',
                        return_value={
                            key: {
                                'remaining': 32,
                                'admission_state': 'active',
                                'admission_limit': 32,
                                'last_success_at': 100,
                            }
                        }), mock.patch.object(
                            paid_capacity,
                            'try_persist_claim',
                            return_value=paid_capacity.ClaimResult.ACQUIRED
                        ) as claim, mock.patch(
                            'sky.serve.replica_managers._should_use_spot',
                            return_value=True), mock.patch(
                                'sky.serve.replica_managers.'
                                '_get_resources_ports',
                                return_value='8080'), mock.patch.object(
                                    replica_managers, '_ReplicaLaunchThread'):
            mgr._scale_up_batch_locked([{'use_spot': True}] * 30)

        budget = claim.call_args.kwargs['budget']
        assert budget.service_claim_limit == 24
        assert budget.service_remaining == 0
        assert claim.call_count == 24
        assert mgr._next_replica_id == 25
        assert len(mgr._launch_thread_pool) == 24

    def test_spot_batch_defers_when_shared_reservation_lock_is_busy(self):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr._spot_placer = mock.Mock()
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=True)
        mgr.yaml_content = 'resources:\n  use_spot: true\n'
        reservation_lock = mock.Mock()
        reservation_lock.acquire.side_effect = replica_managers.locks.LockTimeout(
            'busy')

        with mock.patch.object(replica_managers.locks,
                               'get_lock',
                               return_value=reservation_lock) as get_lock, \
             mock.patch.object(mgr, '_scale_up_batch_locked') as scale_locked:
            mgr.scale_up_batch([{'use_spot': True}] * 3)

        get_lock.assert_called_once_with(replica_managers.serve_constants.
                                         DEMAND_CAPACITY_RESERVATION_LOCK_ID)
        reservation_lock.acquire.assert_called_once_with(blocking=False)
        scale_locked.assert_not_called()

    def test_paid_only_spot_batch_does_not_take_shared_capacity_lock(self):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr._spot_placer = mock.Mock()
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        mgr.yaml_content = 'resources:\n  use_spot: true\n'

        with mock.patch.object(replica_managers.locks,
                               'get_lock') as get_lock, \
             mock.patch.object(mgr, '_scale_up_batch_locked') as scale_locked:
            mgr.scale_up_batch([{'use_spot': True}] * 3)

        get_lock.assert_not_called()
        scale_locked.assert_called_once_with([{'use_spot': True}] * 3, None)

    @pytest.mark.parametrize('active_paid', [False, True])
    def test_zero_cost_batch_uses_shared_capacity_lock_even_without_active_paid(
            self, active_paid):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        zero = replica_managers.spot_placer.Location.from_pickleable({
            'cloud': 'Kubernetes',
            'region': 'research-ctx',
            'zone': None,
            'accelerators': {
                'A100': 8
            },
            'use_spot': False,
        })
        paid = replica_managers.spot_placer.Location.from_pickleable({
            'cloud': 'AWS',
            'region': 'us-east-1',
            'zone': None,
            'accelerators': {
                'A100': 8
            },
            'use_spot': True,
        })
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [zero]
        placer.active_locations.return_value = ([zero, paid]
                                                if active_paid else [zero])
        mgr._spot_placer = placer
        mgr.yaml_content = 'resources:\n  use_spot: true\n'
        reservation_lock = mock.MagicMock()

        assert mgr._uses_shared_zero_cost_demand_budget()
        with mock.patch.object(replica_managers.locks,
                               'get_lock',
                               return_value=reservation_lock) as get_lock, \
             mock.patch.object(mgr, '_scale_up_batch_locked') as scale_locked:
            mgr.scale_up_batch([{'use_spot': True}] * 100)

        get_lock.assert_called_once_with(replica_managers.serve_constants.
                                         DEMAND_CAPACITY_RESERVATION_LOCK_ID)
        reservation_lock.acquire.assert_called_once_with(blocking=False)
        scale_locked.assert_called_once()

    def test_on_demand_batch_does_not_add_replica_scan(self):
        """Explicit on-demand pins do not ask the placer for a location."""
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr._spot_placer = mock.Mock()
        mgr.yaml_content = 'dummy: yaml'
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids', return_value=set()), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos'
             ) as scan, \
             mock.patch.object(mgr, '_launch_replica',
                               side_effect=_record_launch(launched)):
            mgr.scale_up_batch([{'use_spot': False}] * 3)
        assert launched == [1, 2, 3]
        scan.assert_not_called()

    def test_batch_yields_to_newer_pending_version(self):
        """A committed update must not wait behind the rest of a huge wave."""
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr.latest_version = 4
        mgr._pending_version = None
        launched = []

        def _launch(replica_id,
                    resources_override,
                    existing_replica_infos=None):
            del resources_override, existing_replica_infos
            launched.append(replica_id)
            if replica_id == 2:
                mgr.notify_version_pending(6)
            return _accepted_launch_result(replica_id)

        with mock.patch(
                'sky.serve.replica_managers.serve_state.'
                'get_replica_ids', return_value=set()), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr.scale_up_batch([None] * 500)

        assert launched == [1, 2]
        assert mgr._next_replica_id == 3
        assert mgr._pending_version == 6

    def test_stale_physical_batch_cannot_cross_logical_update(self):
        mgr = _make_manager(next_replica_id=1)
        mgr.lock = self._CountingLock()
        mgr.latest_version = 5
        launched = []

        with mock.patch.object(mgr,
                               '_launch_replica',
                               side_effect=_record_launch(launched)), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_ids') \
                     as id_scan:
            mgr.scale_up_batch([None] * 100, expected_version=4)

        assert not launched
        id_scan.assert_not_called()

    def test_stale_physical_scale_down_cannot_cross_logical_update(self):
        mgr = _make_manager()
        mgr.latest_version = 5
        mgr._terminate_replica = mock.Mock()

        mgr.scale_down(1, expected_version=4)

        mgr._terminate_replica.assert_not_called()

    def test_pending_version_signal_clears_only_matching_update(self):
        mgr = _make_manager()
        mgr._pending_version = None
        mgr.notify_version_pending(6)
        mgr.notify_version_pending(7)
        mgr.clear_pending_version(6)
        assert mgr._pending_version == 7
        mgr.clear_pending_version(7)
        assert mgr._pending_version is None


class TestLogicalPendingLaunchAdmission:

    @staticmethod
    def _info(replica_id,
              card,
              status,
              *,
              zero_cost=False,
              reserved_fill=False,
              created_at=None):
        info = replica_managers.ReplicaInfo(
            replica_id=replica_id,
            cluster_name=f'svc-{replica_id}',
            replica_port='8080',
            is_spot=True,
            location=None,
            version=1,
            resources_override={'accelerators': {
                card: 1
            }},
            planned_capacity=1)
        info.created_at = (float(replica_id)
                           if created_at is None else created_at)
        info.is_zero_cost = zero_cost
        info.reserved_fill = reserved_fill
        if status == replica_managers.serve_state.ReplicaStatus.READY:
            info.status_property.sky_launch_status = (
                common_utils.ProcessStatus.SUCCEEDED)
            info.status_property.service_ready_now = True
            info.status_property.first_ready_time = 1.0
        elif status == replica_managers.serve_state.ReplicaStatus.PROVISIONING:
            info.status_property.sky_launch_status = (
                common_utils.ProcessStatus.RUNNING)
        else:
            assert status == replica_managers.serve_state.ReplicaStatus.PENDING
        assert info.status == status
        return info

    @staticmethod
    def _manager(target_by_card):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_exact_accelerator_shapes = {
            'L4': 1,
            'A100': 1,
            'A100-80GB': 1,
        }
        generation = 7
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=generation,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        shapes = (('L4', 1), ('A100', 1), ('A100-80GB', 1))
        target = sum(target_by_card.values())
        mgr._logical_target = (1, generation, target,
                               tuple(target_by_card.items()), shapes)
        return mgr

    def test_recovered_paid_a100_wave_is_excluded_when_ready_covers_target(
            self):
        mgr = self._manager({'L4': 3, 'A100': 2})
        infos = [
            self._info(1, 'A100',
                       replica_managers.serve_state.ReplicaStatus.READY),
            self._info(2, 'A100',
                       replica_managers.serve_state.ReplicaStatus.READY),
            self._info(3, 'A100',
                       replica_managers.serve_state.ReplicaStatus.READY),
            self._info(4, 'A100',
                       replica_managers.serve_state.ReplicaStatus.PENDING),
            self._info(5, 'A100',
                       replica_managers.serve_state.ReplicaStatus.PENDING),
            self._info(6,
                       'A100',
                       replica_managers.serve_state.ReplicaStatus.PENDING,
                       zero_cost=True,
                       reserved_fill=True),
            self._info(7, 'L4',
                       replica_managers.serve_state.ReplicaStatus.READY),
            self._info(8, 'L4',
                       replica_managers.serve_state.ReplicaStatus.PENDING),
            self._info(9, 'L4',
                       replica_managers.serve_state.ReplicaStatus.PENDING),
        ]

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=infos):
            applicable, fence, authorized = (
                mgr._logical_pending_launch_admission())

        assert applicable
        assert fence == mgr._logical_target
        assert authorized == {6, 8, 9}

    def test_zero_cost_demand_pending_wins_last_target_slot(self):
        mgr = self._manager({'A100': 4})
        infos = [
            self._info(replica_id, 'A100',
                       replica_managers.serve_state.ReplicaStatus.READY)
            for replica_id in (1, 2, 3)
        ]
        infos.extend([
            self._info(4,
                       'A100',
                       replica_managers.serve_state.ReplicaStatus.PENDING,
                       created_at=1.0),
            self._info(5,
                       'A100',
                       replica_managers.serve_state.ReplicaStatus.PENDING,
                       zero_cost=True,
                       created_at=2.0),
        ])

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=infos):
            _, _, authorized = mgr._logical_pending_launch_admission()

        assert authorized == {5}

    def test_a100_variants_have_independent_pending_budgets(self):
        mgr = self._manager({'A100': 1, 'A100-80GB': 1})
        infos = [
            self._info(1, 'A100',
                       replica_managers.serve_state.ReplicaStatus.READY),
            self._info(2, 'A100',
                       replica_managers.serve_state.ReplicaStatus.PENDING),
            self._info(3, 'A100-80GB',
                       replica_managers.serve_state.ReplicaStatus.PENDING),
        ]

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=infos):
            _, _, authorized = mgr._logical_pending_launch_admission()

        assert authorized == {3}

    def test_boolean_rebalance_id_cannot_bypass_pending_budget(self):
        mgr = self._manager({'A100': 1})
        ready = self._info(1, 'A100',
                           replica_managers.serve_state.ReplicaStatus.READY)
        pending = self._info(2, 'A100',
                             replica_managers.serve_state.ReplicaStatus.PENDING)
        pending.cost_rebalance_for_replica_id = True

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[ready, pending]):
            _, _, authorized = mgr._logical_pending_launch_admission()

        assert authorized == set()

    def test_incomplete_exact_target_defers_without_reading_fleet(self):
        mgr = self._manager({'A100': 1})
        mgr._logical_target = None

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos') as get_infos:
            applicable, fence, authorized = (
                mgr._logical_pending_launch_admission())

        assert applicable
        assert fence is None
        assert authorized == set()
        get_infos.assert_not_called()

    def test_final_cloud_guard_rechecks_newly_ready_capacity(self):
        mgr = self._manager({'A100': 1})
        candidate = self._info(
            1, 'A100', replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        mgr._replica_to_logical_launch_fence[1] = mgr._logical_target

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[candidate]):
            assert mgr._queued_logical_launch_fence_holds(1)

        ready = self._info(2, 'A100',
                           replica_managers.serve_state.ReplicaStatus.READY)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[candidate, ready]):
            assert not mgr._queued_logical_launch_fence_holds(1)

    def test_single_card_unpinned_candidate_is_authorized(self):
        mgr = self._manager({'L4': 1})
        shapes = (('L4', 1),)
        mgr._logical_exact_accelerator_shapes = dict(shapes)
        mgr._logical_target = (1, 7, 1, shapes, shapes)
        candidate = self._info(
            1, 'L4', replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        candidate.resources_override = None
        mgr._replica_to_logical_launch_fence[1] = mgr._logical_target

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[candidate]):
            allowed, reason, admission = (
                mgr._queued_logical_launch_fence_decision(1))
        assert allowed
        assert reason == 'authorized'
        assert admission is not None
        assert admission.authorized_ids == frozenset({1})
        assert 'candidate=(1,' in admission.details

    def test_single_card_unpinned_ready_supply_blocks_duplicate(self):
        mgr = self._manager({'L4': 1})
        shapes = (('L4', 1),)
        mgr._logical_exact_accelerator_shapes = dict(shapes)
        mgr._logical_target = (1, 7, 1, shapes, shapes)
        ready = self._info(1, 'L4',
                           replica_managers.serve_state.ReplicaStatus.READY)
        ready.resources_override = None
        candidate = self._info(
            2, 'L4', replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        candidate.resources_override = None
        mgr._replica_to_logical_launch_fence[2] = mgr._logical_target

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[ready, candidate]):
            allowed, reason, admission = (
                mgr._queued_logical_launch_fence_decision(2))
        assert not allowed
        assert reason == 'replica-not-authorized'
        assert admission is not None
        assert admission.authorized_ids == frozenset()
        assert "baseline={'L4': 1}" in admission.details

    def test_multi_card_unpinned_candidate_remains_ambiguous(self):
        mgr = self._manager({'A100': 1})
        candidate = self._info(
            1, 'A100', replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        candidate.resources_override = None
        mgr._replica_to_logical_launch_fence[1] = mgr._logical_target

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[candidate]):
            assert not mgr._queued_logical_launch_fence_holds(1)

    def test_final_cloud_guard_accepts_equivalent_newer_generation(self):
        mgr = self._manager({'A100': 1})
        candidate = self._info(
            1, 'A100', replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        stored_fence = mgr._logical_target
        assert stored_fence is not None
        mgr._replica_to_logical_launch_fence[1] = stored_fence
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, generation=8)
        mgr._logical_target = (1, 8, 1, (('A100', 1),), stored_fence[4])

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[candidate]):
            assert replica_managers._logical_target_intent_preserved(
                mgr._logical_target, stored_fence)
            assert mgr._queued_logical_launch_fence_holds(1)

    def test_final_cloud_guard_rejects_changed_newer_target(self):
        mgr = self._manager({'A100': 1})
        candidate = self._info(
            1, 'A100', replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        stored_fence = mgr._logical_target
        assert stored_fence is not None
        mgr._replica_to_logical_launch_fence[1] = stored_fence
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, generation=8)
        mgr._logical_target = (1, 8, 2, (('A100', 2),), stored_fence[4])

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[candidate]):
            assert not mgr._queued_logical_launch_fence_holds(1)

    def test_final_cloud_guard_rejects_older_generation(self):
        mgr = self._manager({'A100': 1})
        candidate = self._info(
            1, 'A100', replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        current_fence = mgr._logical_target
        assert current_fence is not None
        stored_fence = (current_fence[0], current_fence[1] + 1,
                        current_fence[2], current_fence[3], current_fence[4])
        mgr._replica_to_logical_launch_fence[1] = stored_fence

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[candidate]):
            assert not replica_managers._logical_target_intent_preserved(
                current_fence, stored_fence)
            assert not mgr._queued_logical_launch_fence_holds(1)


class TestLogicalCapacityPlanning:
    """One manager operation packs whole backend shapes to a slot target."""

    @pytest.mark.parametrize('accelerators,expected', [
        ({
            'L4': 1
        }, 1),
        ({
            'A100': 4
        }, 4),
        ({
            'A100-80GB': 8
        }, 8),
        ({
            'L4': 0.5
        }, None),
        ({
            'L4': 1,
            'A100': 1
        }, None),
        (None, None),
    ])
    def test_v1_capacity_requires_one_whole_gpu_shape(self, accelerators,
                                                      expected):
        assert replica_managers._whole_gpu_capacity(accelerators) == expected

    @pytest.mark.parametrize('accelerators,expected', [
        ([{
            'A100': 8
        }, {
            'A100': 8
        }], 8),
        ([{
            'A100': 8
        }, None], None),
        ([{
            'A100': 8
        }, {
            'L4': 4
        }], None),
    ])
    def test_default_capacity_requires_every_resource_to_share_one_width(
            self, accelerators, expected):
        resources = [
            types.SimpleNamespace(accelerators=value) for value in accelerators
        ]
        assert replica_managers._uniform_whole_gpu_capacity(
            resources) == expected

    def test_v1_logical_capacity_rejects_multi_node_service(self):
        with pytest.raises(ValueError, match='only single-node services'):
            replica_managers._validate_logical_capacity_sources(
                default_capacity=8, placer=None, num_nodes=2)

    def test_atomic_logical_state_rejects_new_snapshot_with_old_target(self):
        mgr = _make_manager()
        mgr._update_recovery_required = False
        old_snapshot = replica_managers.LogicalReconcileSnapshot(
            version=1,
            generation=5,
            observed_slots_by_replica_id={10: 1},
            in_flight_by_replica_id={10: 0},
            unknown_replica_ids=frozenset(),
            received_at=replica_managers.time.monotonic())
        mgr._logical_target = (1, 5, 1)
        mgr._logical_reconcile_snapshot = old_snapshot
        new_snapshot = dataclasses.replace(old_snapshot,
                                           generation=6,
                                           observed_slots_by_replica_id={10: 2})

        assert not mgr.publish_logical_reconcile_state((1, 5, 1), new_snapshot)
        assert mgr._logical_target == (1, 5, 1)
        assert mgr._logical_reconcile_snapshot is old_snapshot

        assert mgr.publish_logical_reconcile_state((1, 6, 2), new_snapshot)
        assert mgr._logical_target == (1, 6, 2)
        assert mgr._logical_reconcile_snapshot.generation == 6
        assert mgr._logical_reconcile_snapshot.observed_slots_by_replica_id == {
            10: 2
        }

    def test_durable_logical_publication_never_renews_authority_deadline(
            self, monkeypatch):
        now = [100.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        mgr = _make_manager()
        mgr._update_recovery_required = False
        authority = types.SimpleNamespace(deadline_monotonic=101.0)
        snapshot = replica_managers.LogicalReconcileSnapshot(
            version=1,
            generation=5,
            observed_slots_by_replica_id={10: 1},
            in_flight_by_replica_id={10: 0},
            unknown_replica_ids=frozenset(),
            received_at=1.0,
            authority=authority)

        assert mgr.publish_logical_reconcile_state((1, 5, 1), snapshot)
        published = mgr._logical_reconcile_snapshot
        assert published is not None
        assert published.authority is authority
        now[0] = 101.1
        assert not mgr._logical_snapshot_is_fresh(published)

        replay = dataclasses.replace(snapshot, received_at=now[0])
        assert not mgr.publish_logical_reconcile_state((1, 5, 1), replay)
        assert mgr._logical_reconcile_snapshot is published

    def test_legacy_half_publishers_reject_nonadvancing_evidence(self):
        mgr = _make_manager()
        mgr._update_recovery_required = False
        old_snapshot = replica_managers.LogicalReconcileSnapshot(
            version=1,
            generation=7,
            observed_slots_by_replica_id={10: 1},
            in_flight_by_replica_id={10: 0},
            unknown_replica_ids=frozenset(),
            received_at=replica_managers.time.monotonic())
        mgr._logical_target = (1, 7, 1)
        mgr._logical_reconcile_snapshot = old_snapshot
        old_state = mgr._logical_reconcile_state

        mgr.update_logical_reconcile_snapshot(
            version=1,
            generation=7,
            observed_slots_by_replica_id={10: 2},
            in_flight_by_replica_id={10: 1},
            unknown_replica_ids=set())
        assert mgr._logical_reconcile_state is old_state

        mgr.update_logical_reconcile_snapshot(
            version=1,
            generation=6,
            observed_slots_by_replica_id={10: 3},
            in_flight_by_replica_id={10: 2},
            unknown_replica_ids=set())
        assert mgr._logical_reconcile_state is old_state

        mgr.publish_logical_target(1, 6, 0)
        assert mgr._logical_reconcile_state is old_state

        mgr.update_logical_reconcile_snapshot(
            version=1,
            generation=8,
            observed_slots_by_replica_id={10: 2},
            in_flight_by_replica_id={10: 0},
            unknown_replica_ids=set())
        advanced_state = mgr._logical_reconcile_state
        assert advanced_state is not old_state
        assert advanced_state.target == (1, 7, 1)
        assert advanced_state.snapshot is not None
        assert advanced_state.snapshot.generation == 8

    def test_same_generation_replay_never_exposes_mixed_pair(self):

        class _BlockingMapping(collections.abc.Mapping):

            def __init__(self, values, started, release):
                self._values = values
                self._started = started
                self._release = release

            def __getitem__(self, key):
                return self._values[key]

            def __iter__(self):
                self._started.set()
                assert self._release.wait(timeout=5)
                return iter(self._values)

            def __len__(self):
                return len(self._values)

        mgr = _make_manager()
        mgr._update_recovery_required = False
        mgr.latest_version = 1
        old_snapshot = replica_managers.LogicalReconcileSnapshot(
            version=1,
            generation=7,
            observed_slots_by_replica_id={10: 1},
            in_flight_by_replica_id={10: 0},
            unknown_replica_ids=frozenset(),
            received_at=replica_managers.time.monotonic())
        mgr._logical_target = (1, 7, 1)
        mgr._logical_reconcile_snapshot = old_snapshot
        old_state = mgr._logical_reconcile_state
        copy_started = threading.Event()
        release_copy = threading.Event()
        replay_snapshot = dataclasses.replace(
            old_snapshot,
            observed_slots_by_replica_id=
            _BlockingMapping(  # type: ignore[arg-type]
                {10: 2}, copy_started, release_copy))
        results = []

        publisher = threading.Thread(target=lambda: results.append(
            mgr.publish_logical_reconcile_state((1, 7, 2), replay_snapshot)))
        publisher.start()
        assert copy_started.wait(timeout=5)
        try:
            observed_during_publish = mgr._logical_reconcile_state
            assert observed_during_publish is old_state
            assert observed_during_publish.target == (1, 7, 1)
            assert observed_during_publish.snapshot is old_snapshot
            assert mgr._logical_target_fence_holds(
                1, 7, 1, logical_state=observed_during_publish)
            assert not mgr._logical_target_fence_holds(
                1, 7, 2, logical_state=observed_during_publish)
        finally:
            release_copy.set()
            publisher.join(timeout=5)

        assert not publisher.is_alive()
        assert results == [True]
        replay_state = mgr._logical_reconcile_state
        assert replay_state is not old_state
        assert replay_state.target == (1, 7, 2)
        assert replay_state.snapshot is not None
        assert replay_state.snapshot.observed_slots_by_replica_id == {10: 2}
        assert mgr._logical_target_fence_holds(1,
                                               7,
                                               2,
                                               logical_state=replay_state)

    def test_plans_complete_shapes_until_target_is_covered(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 7, 9)
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=True)
        widths = iter([8, 4])
        planned = []

        def _append_shape(_override, _used_ids, existing, _budget,
                          logical_reconcile_fence):
            assert logical_reconcile_fence == (1, 7, 9)
            width = next(widths)
            info = mock.Mock(replica_id=len(existing) + 1,
                             is_terminal=False,
                             is_ready=False,
                             version=1,
                             planned_capacity=width)
            existing.append(info)
            planned.append(width)
            return _accepted_launch_result(info.replica_id, width)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_grouped',
                               return_value={}), \
             mock.patch.object(
                 mgr, '_build_zero_cost_demand_budget',
                 return_value=None) as build_zero_cost_budget, \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_append_shape):
            mgr.scale_up_to_logical_capacity(target_capacity=9,
                                             version=1,
                                             reconcile_generation=7)

        assert planned == [8, 4]
        assert build_zero_cost_budget.call_args.kwargs[
            'demand_count_override'] == 9

    def test_productive_profile_raises_logical_window_without_crossing_cap(
            self, monkeypatch):
        location = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._spot_placer = make_placer({location: 1.0})
        mgr._spot_placer.num_nodes = 1
        mgr._workspace = 'w'
        mgr._service_hash = 'hash'
        mgr._controller_owner = (1, '10.0.0.1')
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr.publish_logical_target(1, 7, 30, (('L4', 30),), (('L4', 1),))
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        key = paid_capacity.pool_key(location, workspace='w', num_nodes=1)
        monkeypatch.setenv(
            paid_capacity._SERVICE_LIMIT_PROFILES_ENV_VAR,
            json.dumps({
                'version': 1,
                'profiles': [{
                    'workspace': 'w',
                    'service_name': 'svc',
                    'service_hash': 'hash',
                    'max_launch_window': 24,
                }],
            }))
        launched = []
        seen_budgets = []

        def _append_one(resources_override, _used_ids, existing,
                        _zero_cost_budget, *, paid_location_launch_budget,
                        **_kwargs):
            selected = paid_capacity.select_location(
                mgr._spot_placer,
                paid_location_launch_budget,
                allowed_locations={location})
            assert selected is location
            paid_capacity.debit(paid_location_launch_budget, selected)
            info = mock.Mock(replica_id=len(existing) + 1,
                             is_terminal=False,
                             is_ready=False,
                             version=1,
                             planned_capacity=1,
                             resources_override=resources_override)
            info.status_property.is_scale_down = False
            info.get_spot_location.return_value = location
            existing.append(info)
            launched.append(info)
            seen_budgets.append(paid_location_launch_budget)
            return _accepted_launch_result(info.replica_id)

        with mock.patch.object(paid_capacity,
                               'central_authority_available',
                               return_value=True), mock.patch.object(
                                   replica_managers.serve_state,
                                   'get_replica_infos',
                                   return_value=[]), mock.patch.object(
                                       replica_managers.serve_state,
                                       'get_paid_capacity_pool_states',
                                       return_value={
                                           key: {
                                               'remaining': 32,
                                               'admission_state': 'active',
                                               'admission_limit': 32,
                                               'last_success_at': 100,
                                           }
                                       }), mock.patch.object(
                                           mgr,
                                           '_scale_up_one_locked',
                                           side_effect=_append_one):
            mgr.scale_up_to_logical_capacity(
                target_capacity=30,
                version=1,
                reconcile_generation=7,
                target_capacity_by_accelerator={'L4': 30},
                accelerator_shapes={'L4': 1})

        assert len(launched) == 24
        assert len({id(budget) for budget in seen_budgets}) == 1
        assert seen_budgets[0].service_claim_limit == 24
        assert seen_budgets[0].service_remaining == 0

    def test_paid_only_logical_scale_up_skips_shared_capacity_lock(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 3, 8)
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)

        with mock.patch.object(replica_managers.locks,
                               'get_lock') as get_lock, \
             mock.patch.object(
                 mgr, '_scale_up_to_logical_capacity_locked') as scale_locked:
            mgr.scale_up_to_logical_capacity(target_capacity=8,
                                             version=1,
                                             reconcile_generation=3)

        get_lock.assert_not_called()
        scale_locked.assert_called_once_with(8, 1, 3,
                                             mgr._logical_reconcile_snapshot,
                                             ())

    def test_paid_envelope_skips_to_zero_cost_exact_card(self):
        paid_l4 = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        zero_a100 = make_location('research', {'A100': 8},
                                  use_spot=False,
                                  cloud_name='Kubernetes')
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._spot_placer = make_placer({paid_l4: 1.0, zero_a100: 0.0})
        mgr._spot_placer.num_nodes = 1
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr.publish_logical_target(1, 3, 9, (('L4', 1), ('A100', 8)),
                                   (('L4', 1), ('A100', 8)))
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        budget = paid_capacity.LaunchBudget(
            remaining_by_location={paid_l4: 4},
            pool_key_by_location={paid_l4: 'paid-l4'},
            states_by_pool_key={},
            globally_managed=True,
            service_remaining=0)

        def _launch_zero_cost(resources_override, _used_ids, existing,
                              _zero_budget, **_kwargs):
            assert resources_override == {'accelerators': {'A100': 8}}
            info = mock.Mock(replica_id=1,
                             is_terminal=False,
                             is_ready=False,
                             version=1,
                             planned_capacity=8)
            info.status_property.is_scale_down = False
            info.get_spot_location.return_value = zero_a100
            existing.append(info)
            return _accepted_launch_result(
                info.replica_id, 8,
                replica_managers._ReplicaLaunchFunding.ZERO_COST)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(paid_capacity,
                               'build_launch_budget',
                               return_value=budget), \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_launch_zero_cost) as launch:
            mgr.scale_up_to_logical_capacity(target_capacity=9,
                                             version=1,
                                             reconcile_generation=3,
                                             target_capacity_by_accelerator={
                                                 'L4': 1,
                                                 'A100': 8,
                                             },
                                             accelerator_shapes={
                                                 'L4': 1,
                                                 'A100': 8,
                                             })

        launch.assert_called_once()

    def test_exhausted_paid_envelope_keeps_zero_cost_logical_launches(self):
        zero = make_location('research', {'L4': 1},
                             use_spot=False,
                             cloud_name='Kubernetes')
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._spot_placer = make_placer({zero: 0.0})
        mgr._spot_placer.num_nodes = 1
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 3, 1)
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        budget = paid_capacity.LaunchBudget(remaining_by_location={},
                                            pool_key_by_location={},
                                            states_by_pool_key={},
                                            globally_managed=True,
                                            service_remaining=0)
        launched = []

        def _append_zero_cost(_override, _used_ids, existing, _zero_cost_budget,
                              *, logical_reconcile_fence,
                              paid_location_launch_budget):
            assert logical_reconcile_fence == (1, 3, 1)
            assert paid_location_launch_budget is budget
            info = mock.Mock(replica_id=1,
                             is_terminal=False,
                             is_ready=False,
                             version=1,
                             planned_capacity=1,
                             resources_override=None)
            info.status_property.is_scale_down = False
            info.get_spot_location.return_value = zero
            info.is_zero_cost = True
            existing.append(info)
            launched.append(info)
            return _accepted_launch_result(
                info.replica_id,
                funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), mock.patch.object(
                                   paid_capacity,
                                   'build_launch_budget',
                                   return_value=budget), mock.patch.object(
                                       mgr,
                                       '_scale_up_one_locked',
                                       side_effect=_append_zero_cost):
            mgr.scale_up_to_logical_capacity(target_capacity=1,
                                             version=1,
                                             reconcile_generation=3)

        assert len(launched) == 1

    def test_logical_scale_up_honors_each_exact_card_target(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr.publish_logical_target(1, 7, 9, (('L4', 1), ('A100', 8)),
                                   (('L4', 1), ('A100', 8)))
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        overrides = []
        priorities = []

        def _append_shape(resources_override,
                          _used_ids,
                          existing,
                          _budget,
                          logical_reconcile_fence,
                          launch_priority=0):
            del logical_reconcile_fence
            overrides.append(resources_override)
            priorities.append(launch_priority)
            _, width = next(iter(resources_override['accelerators'].items()))
            info = mock.Mock(replica_id=len(existing) + 1,
                             is_terminal=False,
                             is_ready=False,
                             version=1,
                             planned_capacity=width,
                             resources_override=resources_override)
            info.status_property.is_scale_down = False
            info.get_spot_location.return_value = None
            existing.append(info)
            return _accepted_launch_result(info.replica_id, width)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_append_shape):
            mgr.scale_up_to_logical_capacity(target_capacity=9,
                                             version=1,
                                             reconcile_generation=7,
                                             target_capacity_by_accelerator={
                                                 'L4': 1,
                                                 'A100': 8,
                                             },
                                             accelerator_shapes={
                                                 'L4': 1,
                                                 'A100': 8,
                                             },
                                             launch_priority_by_accelerator={
                                                 'L4': 20,
                                                 'A100': 50,
                                             })

        assert overrides == [{
            'accelerators': {
                'L4': 1
            }
        }, {
            'accelerators': {
                'A100': 8
            }
        }]
        assert priorities == [20, 50]

    def test_logical_no_progress_does_not_buy_unfunded_retained_card(self):
        mgr = _make_manager()
        mgr.latest_version = 2
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=2,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr.publish_logical_target(2, 7, 2, (('L4', 1), ('A100', 1)),
                                   (('L4', 1), ('A100', 1)))
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        attempts = []

        def _no_progress(resources_override, *_args, **kwargs):
            attempts.append((next(iter(resources_override['accelerators'])),
                             kwargs['paid_launch_allowed']))
            return None

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_no_progress):
            mgr.scale_up_to_logical_capacity(
                target_capacity=2,
                version=2,
                reconcile_generation=7,
                target_capacity_by_accelerator={
                    'L4': 1,
                    'A100': 1,
                },
                accelerator_shapes={
                    'L4': 1,
                    'A100': 1,
                },
                launch_priority_by_accelerator={
                    'L4': 0,
                    'A100': 0,
                },
                cold_launch_authority_by_accelerator={'L4': 1})

        # The A100 target is retained only to replace old-version serving
        # capacity.  The manager may still probe its exact zero-cost pool, but
        # it must not buy A100 after the cheapest L4 placement makes no
        # progress for the same flexible work.
        assert attempts == [('L4', True), ('A100', False)]

        attempts.clear()
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_no_progress):
            mgr.scale_up_to_logical_capacity(
                target_capacity=2,
                version=2,
                reconcile_generation=7,
                target_capacity_by_accelerator={
                    'L4': 1,
                    'A100': 1,
                },
                accelerator_shapes={
                    'L4': 1,
                    'A100': 1,
                },
                launch_priority_by_accelerator={
                    'L4': 0,
                    'A100': 50,
                },
                cold_launch_authority_by_accelerator={
                    'L4': 1,
                    'A100': 1,
                })

        # A100-only or hard-floor work carries independent authority and must
        # still be attempted after an unrelated L4 placement failure.
        assert attempts == [('L4', True), ('A100', True)]

    def test_logical_paid_authority_debits_only_paid_launch_results(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr.publish_logical_target(1, 7, 3, (('L4', 3),), (('L4', 1),))
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        fundings = iter([
            replica_managers._ReplicaLaunchFunding.ZERO_COST,
            replica_managers._ReplicaLaunchFunding.PAID,
            replica_managers._ReplicaLaunchFunding.ZERO_COST,
        ])
        paid_permissions = []

        def _accept(resources_override, _used_ids, existing, _budget, *,
                    logical_reconcile_fence, paid_launch_allowed):
            del logical_reconcile_fence
            paid_permissions.append(paid_launch_allowed)
            funding = next(fundings)
            replica_id = len(existing) + 1
            info = mock.Mock(replica_id=replica_id,
                             is_terminal=False,
                             is_ready=False,
                             version=1,
                             planned_capacity=1,
                             resources_override=resources_override)
            info.status_property.is_scale_down = False
            info.get_spot_location.return_value = None
            existing.append(info)
            return _accepted_launch_result(replica_id, funding=funding)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_accept):
            mgr.scale_up_to_logical_capacity(
                target_capacity=3,
                version=1,
                reconcile_generation=7,
                target_capacity_by_accelerator={'L4': 3},
                accelerator_shapes={'L4': 1},
                cold_launch_authority_by_accelerator={'L4': 1})

        # The first accepted launch is zero-cost and consumes no paid
        # authority. The second is paid and closes the paid path, while the
        # third may still probe the exact zero-cost pool.
        assert paid_permissions == [True, True, False]

    @pytest.mark.parametrize('authority', [{
        'A100': 1
    }, {
        'L4': True
    }, {
        'L4': -1
    }, {
        'L4': 2
    }])
    def test_malformed_logical_paid_authority_fails_closed(self, authority):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr.publish_logical_target(1, 7, 1, (('L4', 1),), (('L4', 1),))
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), mock.patch.object(
                                   mgr, '_scale_up_one_locked') as launch:
            mgr.scale_up_to_logical_capacity(
                target_capacity=1,
                version=1,
                reconcile_generation=7,
                target_capacity_by_accelerator={'L4': 1},
                accelerator_shapes={'L4': 1},
                cold_launch_authority_by_accelerator=authority)

        launch.assert_not_called()

    def test_logical_scale_up_keeps_full_fence_but_honors_launch_budget(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr.publish_logical_target(1, 7, 50, (('L4', 50),), (('L4', 1),))
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=True)
        launched_ids = []
        reservation_lock = mock.MagicMock()

        def _append_shape(resources_override, _used_ids, existing, _budget,
                          logical_reconcile_fence):
            assert logical_reconcile_fence == (1, 7, 50, (('L4', 50),), (('L4',
                                                                          1),))
            launched_ids.append(len(existing) + 1)
            info = mock.Mock(replica_id=len(existing) + 1,
                             is_terminal=False,
                             is_ready=False,
                             version=1,
                             planned_capacity=1,
                             resources_override=resources_override)
            info.status_property.is_scale_down = False
            info.get_spot_location.return_value = None
            existing.append(info)
            return _accepted_launch_result(info.replica_id)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_grouped',
                               return_value={}), \
             mock.patch.object(replica_managers.locks,
                               'get_lock',
                               return_value=reservation_lock), \
             mock.patch.object(mgr,
                               '_build_zero_cost_demand_budget',
                               return_value=None) as build_zero_cost_budget, \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_append_shape):
            mgr.scale_up_to_logical_capacity(
                target_capacity=50,
                version=1,
                reconcile_generation=7,
                target_capacity_by_accelerator={'L4': 50},
                accelerator_shapes={'L4': 1},
                launch_budget=10)

        assert len(launched_ids) == 10
        assert build_zero_cost_budget.call_args.kwargs[
            'demand_count_override'] == 10

    def test_logical_scale_up_zero_budget_skips_shared_capacity_lock(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr.publish_logical_target(1, 7, 50, (('L4', 50),), (('L4', 1),))
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=True)

        with mock.patch.object(replica_managers.locks,
                               'get_lock') as get_lock, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_grouped') as grouped:
            mgr.scale_up_to_logical_capacity(
                target_capacity=50,
                version=1,
                reconcile_generation=7,
                target_capacity_by_accelerator={'L4': 50},
                accelerator_shapes={'L4': 1},
                launch_budget=0)

        get_lock.assert_not_called()
        grouped.assert_not_called()

    def test_logical_launch_budget_counts_result_width_not_snapshot_drift(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        old = mock.Mock(replica_id=100,
                        is_terminal=False,
                        is_ready=True,
                        version=1,
                        planned_capacity=1,
                        resources_override={'accelerators': {
                            'L4': 1
                        }})
        old.status_property.is_scale_down = False
        old.get_spot_location.return_value = None
        existing = [old]
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={100: 0},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr.publish_logical_target(1, 7, 5)
        launched_ids = []

        def _append_one(_resources_override, _used_ids, infos, _budget,
                        logical_reconcile_fence):
            assert logical_reconcile_fence == (1, 7, 5)
            if not launched_ids:
                # Concurrent LB publication makes an existing ready row gain
                # one observed slot. That is not launch progress for this
                # manager call and must not consume its budget.
                mgr._logical_reconcile_snapshot = (
                    replica_managers.LogicalReconcileSnapshot(
                        version=1,
                        generation=7,
                        observed_slots_by_replica_id={100: 1},
                        in_flight_by_replica_id={},
                        unknown_replica_ids=frozenset(),
                        received_at=replica_managers.time.monotonic()))
            replica_id = 101 + len(launched_ids)
            launched_ids.append(replica_id)
            info = mock.Mock(replica_id=replica_id,
                             is_terminal=False,
                             is_ready=False,
                             version=1,
                             planned_capacity=1,
                             resources_override=None)
            info.status_property.is_scale_down = False
            info.get_spot_location.return_value = None
            infos.append(info)
            return _accepted_launch_result(replica_id)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=existing), \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_append_one):
            mgr.scale_up_to_logical_capacity(target_capacity=5,
                                             version=1,
                                             reconcile_generation=7,
                                             launch_budget=2)

        assert launched_ids == [101, 102]

    def test_saturated_exact_card_does_not_block_other_card_target(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr.publish_logical_target(1, 7, 2, (('L4', 1), ('A100', 1)),
                                   (('L4', 1), ('A100', 1)))
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        attempted_cards = []

        def _launch(resources_override, _used_ids, existing, _budget,
                    logical_reconcile_fence):
            del logical_reconcile_fence
            card = next(iter(resources_override['accelerators']))
            attempted_cards.append(card)
            if card == 'L4':
                return None
            info = mock.Mock(replica_id=1,
                             is_terminal=False,
                             is_ready=False,
                             version=1,
                             planned_capacity=1,
                             resources_override=resources_override)
            info.status_property.is_scale_down = False
            info.get_spot_location.return_value = None
            existing.append(info)
            return _accepted_launch_result(info.replica_id)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_launch):
            mgr.scale_up_to_logical_capacity(target_capacity=2,
                                             version=1,
                                             reconcile_generation=7,
                                             target_capacity_by_accelerator={
                                                 'L4': 1,
                                                 'A100': 1,
                                             },
                                             accelerator_shapes={
                                                 'L4': 1,
                                                 'A100': 1,
                                             })

        assert attempted_cards == ['L4', 'A100']

    def test_unknown_capacity_replacement_launch_is_durably_attributed(self):
        mgr = _make_manager()
        mgr._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND, generic=True)
        mgr._uses_logical_replicas = True
        original = self._ready_backend(1, 8)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=9,
                observed_slots_by_replica_id={1: 0},
                in_flight_by_replica_id={1: 0},
                unknown_replica_ids=frozenset({1}),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 9, 8)
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=True)
        launches = []
        authorizations = []
        reservation_lock = mock.MagicMock()
        stale_replacement = self._ready_backend(2, 8)
        stale_replacement.unknown_capacity_replacement = True

        def _append_replacement(
                _override,
                _used_ids,
                existing,
                _budget,
                logical_reconcile_fence,
                logical_reconcile_fence_requires_exact_generation=False,
                unknown_capacity_replacement=False,
                unknown_capacity_replacement_authorization=None):
            assert logical_reconcile_fence_requires_exact_generation is True
            launches.append(unknown_capacity_replacement)
            authorizations.append(unknown_capacity_replacement_authorization)
            existing.append(
                replica_managers.ReplicaInfo(replica_id=2,
                                             cluster_name='svc-2',
                                             replica_port='8080',
                                             is_spot=False,
                                             location=None,
                                             version=1,
                                             resources_override=None,
                                             planned_capacity=8))
            return _accepted_launch_result(2, 8)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[original, stale_replacement
                                            ]) as local_scan, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_grouped',
                               return_value={'svc': [original]
                                            }) as grouped_scan, \
             mock.patch.object(replica_managers.locks,
                               'get_lock',
                               return_value=reservation_lock), \
             mock.patch.object(mgr,
                               '_build_zero_cost_demand_budget',
                               return_value=None), \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_append_replacement):
            mgr.scale_up_to_logical_capacity(target_capacity=8,
                                             version=1,
                                             reconcile_generation=9,
                                             replace_unknown_replica_ids=(1,))

        assert launches == [True]
        assert authorizations == [
            ordinary_launch_binding.build_replacement_planner_authorization(
                ordinary_launch_binding.NonPoolLaunchProfileKind.
                UNKNOWN_CAPACITY_REPLACEMENT,
                mgr._ordinary_launch_binding_authority,
                predecessor_replica_id=1,
                predecessor_record_id=original.replica_record_id,
                predecessor_service_version=1,
                observation_generation=9,
                observation_service_version=1,
                target_capacity=8)
        ]
        local_scan.assert_not_called()
        grouped_scan.assert_called_once_with()

    def test_existing_zero_capacity_replacement_prevents_recursive_launch(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        original = self._ready_backend(1, 8)
        replacement = self._ready_backend(2, 8)
        replacement.unknown_capacity_replacement = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=9,
                observed_slots_by_replica_id={
                    1: 0,
                    2: 0
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 9, 8)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[original, replacement]), \
             mock.patch.object(mgr, '_scale_up_one_locked') as launch:
            mgr.scale_up_to_logical_capacity(target_capacity=8,
                                             version=1,
                                             reconcile_generation=9,
                                             replace_unknown_replica_ids=(1,))

        launch.assert_not_called()

    def test_known_capacity_clears_replacement_incident_marker(self):
        mgr = _make_manager()
        replacement = self._ready_backend(2, 8)
        replacement.unknown_capacity_replacement = True
        mgr._unknown_capacity_replacement_ids = {2}
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=10,
                observed_slots_by_replica_id={2: 8},
                in_flight_by_replica_id={2: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._persist_replica = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={2: replacement}):
            mgr._clear_known_unknown_capacity_replacements()

        assert replacement.unknown_capacity_replacement is False
        assert not mgr._unknown_capacity_replacement_ids
        mgr._persist_replica.assert_called_once_with(2, replacement)

    def test_zero_capacity_keeps_replacement_incident_marker(self):
        mgr = _make_manager()
        replacement = self._ready_backend(2, 8)
        replacement.unknown_capacity_replacement = True
        mgr._unknown_capacity_replacement_ids = {2}
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=10,
                observed_slots_by_replica_id={2: 0},
                in_flight_by_replica_id={2: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._persist_replica = mock.Mock()

        mgr._clear_known_unknown_capacity_replacements()

        assert replacement.unknown_capacity_replacement is True
        assert mgr._unknown_capacity_replacement_ids == {2}
        mgr._persist_replica.assert_not_called()

    def test_stale_generation_persists_no_backend_prefix(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=8,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 8, 9)

        with mock.patch.object(mgr, '_scale_up_one_locked') as launch:
            mgr.scale_up_to_logical_capacity(target_capacity=9,
                                             version=1,
                                             reconcile_generation=7)

        launch.assert_not_called()

    def test_newer_snapshot_does_not_starve_current_scale_up_target(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        newer_snapshot = replica_managers.LogicalReconcileSnapshot(
            version=1,
            generation=8,
            observed_slots_by_replica_id={},
            in_flight_by_replica_id={},
            unknown_replica_ids=frozenset(),
            received_at=replica_managers.time.monotonic())
        mgr._logical_reconcile_snapshot = newer_snapshot
        # Generation 7 is still the current published autoscaler decision.
        # Only the independently arriving capacity snapshot advanced.
        mgr._logical_target = (1, 7, 9)
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)

        with mock.patch.object(
                mgr, '_scale_up_to_logical_capacity_locked') as scale_locked:
            mgr.scale_up_to_logical_capacity(target_capacity=9,
                                             version=1,
                                             reconcile_generation=7)

        scale_locked.assert_called_once_with(9, 1, 7, newer_snapshot, ())

    def test_newer_snapshot_drops_recovered_unknown_replacement(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        newer_snapshot = replica_managers.LogicalReconcileSnapshot(
            version=1,
            generation=8,
            observed_slots_by_replica_id={1: 8},
            in_flight_by_replica_id={1: 0},
            unknown_replica_ids=frozenset(),
            received_at=replica_managers.time.monotonic())
        mgr._logical_reconcile_snapshot = newer_snapshot
        mgr._logical_target = (1, 7, 8)
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)

        with mock.patch.object(
                mgr, '_scale_up_to_logical_capacity_locked') as scale_locked:
            mgr.scale_up_to_logical_capacity(target_capacity=8,
                                             version=1,
                                             reconcile_generation=7,
                                             replace_unknown_replica_ids=(1,))

        scale_locked.assert_called_once_with(8, 1, 7, newer_snapshot, ())

    def test_newer_snapshot_capacity_stops_rest_of_scale_up_batch(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        original = self._ready_backend(1, 8)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=7,
                observed_slots_by_replica_id={1: 0},
                in_flight_by_replica_id={1: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 7, 16)
        mgr._uses_shared_zero_cost_demand_budget = mock.Mock(return_value=False)
        launches = []

        def _append_shape(_override, _used_ids, existing, _budget,
                          logical_reconcile_fence):
            launches.append(8)
            existing.append(
                replica_managers.ReplicaInfo(replica_id=2,
                                             cluster_name='svc-2',
                                             replica_port='8080',
                                             is_spot=False,
                                             location=None,
                                             version=1,
                                             resources_override=None,
                                             planned_capacity=8))
            # The original backend recovers while the first placement runs.
            mgr._logical_reconcile_snapshot = (
                replica_managers.LogicalReconcileSnapshot(
                    version=1,
                    generation=8,
                    observed_slots_by_replica_id={1: 8},
                    in_flight_by_replica_id={1: 0},
                    unknown_replica_ids=frozenset(),
                    received_at=replica_managers.time.monotonic()))
            return _accepted_launch_result(2, 8)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[original]), \
             mock.patch.object(mgr,
                               '_scale_up_one_locked',
                               side_effect=_append_shape):
            mgr.scale_up_to_logical_capacity(target_capacity=16,
                                             version=1,
                                             reconcile_generation=7)

        assert launches == [8]

    @staticmethod
    def _ready_backend(replica_id, width):
        info = replica_managers.ReplicaInfo(replica_id=replica_id,
                                            cluster_name=f'svc-{replica_id}',
                                            replica_port='8080',
                                            is_spot=True,
                                            location=None,
                                            version=1,
                                            resources_override=None,
                                            planned_capacity=width)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        info.status_property.service_ready_now = True
        info.status_property.first_ready_time = 1.0
        return info

    def _pending_logical_retirement(self, retiring_version=9):
        mgr = _make_manager()
        mgr.latest_version = 10
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._wait_for_idle_trackers = {}
        mgr._lb_in_flight_report = None
        retiring = self._ready_backend(9, 1)
        retiring.version = retiring_version
        survivor = self._ready_backend(10, 1)
        survivor.version = 10
        status = retiring.status_property
        status.is_scale_down = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.wait_for_idle_before_termination = True
        status.logical_retirement_version = 10
        status.logical_retirement_controller_epoch = 'test-controller-epoch'
        status.logical_retirement_generation = 4
        status.logical_retirement_target_capacity = 1
        status.logical_retirement_confirmed_generation = None
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=10,
                generation=5,
                observed_slots_by_replica_id={10: 1},
                in_flight_by_replica_id={
                    9: 0,
                    10: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (10, 5, 1)
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()
        return mgr, retiring, survivor

    def test_accepted_retirement_survives_covered_target_growth(self):
        mgr, retiring, survivor = self._pending_logical_retirement()
        second_survivor = self._ready_backend(11, 1)
        second_survivor.version = 10
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            observed_slots_by_replica_id={
                10: 1,
                11: 1,
            },
            in_flight_by_replica_id={
                9: 0,
                10: 0,
                11: 0,
            })
        mgr._logical_target = (10, 5, 2)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[retiring, survivor, second_survivor]):
            mgr._finish_logical_retirement(9, retiring)

        mgr._terminate_replica.assert_called_once_with(
            9,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)
        assert retiring.status_property.is_scale_down

    def test_newer_snapshot_finishes_current_target_retirement(self):
        mgr, retiring, survivor = self._pending_logical_retirement()
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, generation=6)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._finish_logical_retirement(9, retiring)

        mgr._terminate_replica.assert_called_once_with(
            9,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)

    def test_newer_snapshot_target_growth_reactivates_retirement(self):
        mgr, retiring, survivor = self._pending_logical_retirement()
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, generation=6)
        mgr._logical_target = (10, 5, 2)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._finish_logical_retirement(9, retiring)

        mgr._terminate_replica.assert_not_called()
        mgr._persist_replica.assert_called_once_with(9, retiring)
        assert not retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status is None

    def test_target_growth_reactivates_before_provisioning_is_ready(self):
        mgr, retiring, survivor = self._pending_logical_retirement()
        provisioning = replica_managers.ReplicaInfo(replica_id=11,
                                                    cluster_name='svc-11',
                                                    replica_port='8080',
                                                    is_spot=True,
                                                    location=None,
                                                    version=10,
                                                    resources_override=None,
                                                    planned_capacity=4)
        mgr._logical_target = (10, 5, 5)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor, provisioning]):
            mgr._finish_logical_retirement(9, retiring)

        mgr._terminate_replica.assert_not_called()
        mgr._persist_replica.assert_called_once_with(9, retiring)
        assert not retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status is None
        assert not retiring.status_property.wait_for_idle_before_termination

    def test_target_growth_reactivates_victim_before_idle(self):
        mgr, retiring, survivor = self._pending_logical_retirement()
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            in_flight_by_replica_id={
                9: 3,
                10: 0,
            })
        mgr._logical_target = (10, 5, 2)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._finish_logical_retirement(9, retiring)

        mgr._terminate_replica.assert_not_called()
        mgr._persist_replica.assert_called_once_with(9, retiring)
        assert not retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status is None
        assert not retiring.status_property.wait_for_idle_before_termination

    def test_target_growth_reactivates_after_committed_capacity_fails(self):
        mgr, retiring, survivor = self._pending_logical_retirement()
        failed = replica_managers.ReplicaInfo(replica_id=11,
                                              cluster_name='svc-11',
                                              replica_port='8080',
                                              is_spot=True,
                                              location=None,
                                              version=10,
                                              resources_override=None,
                                              planned_capacity=4)
        failed.status_property.sky_launch_status = (
            common_utils.ProcessStatus.FAILED)
        mgr._logical_target = (10, 5, 5)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor, failed]):
            mgr._finish_logical_retirement(9, retiring)

        mgr._terminate_replica.assert_not_called()
        mgr._persist_replica.assert_called_once_with(9, retiring)
        assert not retiring.status_property.is_scale_down
        assert not retiring.status_property.wait_for_idle_before_termination

    def test_target_growth_reactivates_after_latest_backend_degrades(self):
        mgr, retiring, survivor = self._pending_logical_retirement()
        degraded = self._ready_backend(11, 4)
        degraded.version = 10
        degraded.status_property.service_ready_now = False
        mgr._logical_target = (10, 5, 5)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor, degraded]):
            mgr._finish_logical_retirement(9, retiring)

        mgr._terminate_replica.assert_not_called()
        mgr._persist_replica.assert_called_once_with(9, retiring)
        assert not retiring.status_property.is_scale_down
        assert not retiring.status_property.wait_for_idle_before_termination

    def test_target_growth_reactivates_when_latest_backend_unobservable(self):
        mgr, retiring, survivor = self._pending_logical_retirement()
        unobservable = self._ready_backend(11, 4)
        unobservable.version = 10
        mgr._logical_target = (10, 5, 5)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor, unobservable]):
            mgr._finish_logical_retirement(9, retiring)

        mgr._terminate_replica.assert_not_called()
        mgr._persist_replica.assert_called_once_with(9, retiring)
        assert not retiring.status_property.is_scale_down
        assert not retiring.status_property.wait_for_idle_before_termination

    def test_target_growth_reactivates_when_latest_backend_unknown(self):
        mgr, retiring, survivor = self._pending_logical_retirement()
        unknown = self._ready_backend(11, 4)
        unknown.version = 10
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            observed_slots_by_replica_id={
                10: 1,
                11: 4,
            },
            unknown_replica_ids=frozenset({11}))
        mgr._logical_target = (10, 5, 5)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor, unknown]):
            mgr._finish_logical_retirement(9, retiring)

        mgr._terminate_replica.assert_not_called()
        mgr._persist_replica.assert_called_once_with(9, retiring)
        assert not retiring.status_property.is_scale_down
        assert not retiring.status_property.wait_for_idle_before_termination

    def test_target_growth_reactivates_only_capacity_shortfall(self):
        mgr, first_retiring, first_survivor = (
            self._pending_logical_retirement())
        second_retiring = self._ready_backend(8, 1)
        second_retiring.version = 9
        second_status = second_retiring.status_property
        first_status = first_retiring.status_property
        second_status.is_scale_down = True
        second_status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        second_status.wait_for_idle_before_termination = True
        second_status.logical_retirement_version = 10
        second_status.logical_retirement_controller_epoch = (
            'test-controller-epoch')
        second_status.logical_retirement_generation = 4
        second_status.logical_retirement_target_capacity = 1
        second_status.logical_retirement_confirmed_generation = None
        second_survivor = self._ready_backend(11, 1)
        second_survivor.version = 10
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            observed_slots_by_replica_id={
                10: 1,
                11: 1,
            },
            in_flight_by_replica_id={
                8: 0,
                9: 0,
                10: 0,
                11: 0,
            })
        mgr._logical_target = (10, 5, 3)
        fleet = [
            first_retiring, second_retiring, first_survivor, second_survivor
        ]

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=fleet):
            mgr._finish_logical_retirement(9, first_retiring)
            mgr._finish_logical_retirement(8, second_retiring)

        assert not first_status.is_scale_down
        assert first_status.sky_down_status is None
        assert second_status.is_scale_down
        mgr._terminate_replica.assert_called_once_with(
            8,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)

    def _recoverable_logical_retirement(self,
                                        replica_id,
                                        width=1,
                                        confirmed_generation=None,
                                        bounded_precommit=False,
                                        admission_precommit=False):
        assert not (bounded_precommit and admission_precommit)
        info = self._ready_backend(replica_id, width)
        status = info.status_property
        status.is_scale_down = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.drain_cap_seconds = 3900
        status.drain_started_at = replica_managers.time.time() - 100
        status.wait_for_idle_before_termination = not (bounded_precommit or
                                                       admission_precommit)
        status.logical_retirement_version = 2 if bounded_precommit else 1
        status.logical_retirement_controller_epoch = 'old-controller-epoch'
        status.logical_retirement_generation = 4
        status.logical_retirement_target_capacity = 1
        status.logical_retirement_confirmed_generation = (
            4 if bounded_precommit or admission_precommit else
            confirmed_generation)
        status.logical_retirement_bounded_deadline = bounded_precommit
        status.logical_retirement_committed = False
        return info

    def _logical_recovery_manager(self, candidates, survivor, target=1):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._recovering_logical_retirement_ids = {
            info.replica_id for info in candidates
        }
        mgr._logical_retirement_recovery_deadline = (
            replica_managers.time.monotonic() + 120)
        mgr._wait_for_idle_trackers = {
            info.replica_id: (mock.Mock(return_value=False),
                              replica_managers.time.monotonic() + 300
                             ) for info in candidates
        }
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=5,
                observed_slots_by_replica_id={
                    survivor.replica_id: survivor.planned_capacity
                },
                in_flight_by_replica_id={
                    **{
                        info.replica_id: 0 for info in candidates
                    },
                    survivor.replica_id: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 5, target)
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()
        return mgr

    def test_recovery_gate_keeps_old_epoch_retirement_off_route_without_proof(
            self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        mgr._logical_reconcile_snapshot = None
        mgr._logical_target = None
        mgr._logical_retirement_recovery_deadline = (
            replica_managers.time.monotonic() - 1)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        assert (retiring.status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert retiring.status_property.logical_retirement_controller_epoch == (
            'old-controller-epoch')
        assert 1 in mgr._recovering_logical_retirement_ids
        mgr._persist_replica.assert_not_called()
        mgr._terminate_replica.assert_not_called()

    def test_pending_version_freezes_uncommitted_retirement(self):
        mgr, retiring, _ = self._pending_logical_retirement()
        mgr._wait_for_idle_trackers = {
            retiring.replica_id: (mock.Mock(return_value=True),
                                  replica_managers.time.monotonic() + 300)
        }
        mgr.notify_version_pending(11)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value={retiring.replica_id: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        assert retiring.status_property.is_scale_down
        assert retiring.status_property.wait_for_idle_before_termination
        assert retiring.status_property.logical_retirement_version == 10
        mgr._persist_replica.assert_not_called()
        mgr._terminate_replica.assert_not_called()

    def test_pending_version_does_not_abort_committed_retirement(self):
        mgr, retiring, _ = self._pending_logical_retirement()
        status = retiring.status_property
        status.wait_for_idle_before_termination = False
        status.logical_retirement_confirmed_generation = 4
        status.logical_retirement_committed = True
        queued_down = mock.Mock()
        queued_down.is_alive.return_value = False
        mgr._down_thread_pool = {retiring.replica_id: queued_down}
        mgr._wait_for_idle_trackers = {
            retiring.replica_id: (mock.Mock(return_value=True),
                                  replica_managers.time.monotonic() + 300)
        }
        mgr.notify_version_pending(11)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value={retiring.replica_id: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        assert retiring.status_property.is_scale_down
        assert retiring.status_property.logical_retirement_committed
        mgr._persist_replica.assert_not_called()
        mgr._terminate_replica.assert_not_called()

    def test_dropped_update_resumes_frozen_retirement_without_epoch_rotation(
            self):
        """A dropped update (clear without apply) lifts the freeze in place."""
        mgr, retiring, survivor = self._pending_logical_retirement()
        old_epoch = mgr._logical_controller_epoch
        mgr._wait_for_idle_trackers = {
            retiring.replica_id: (mock.Mock(return_value=True),
                                  replica_managers.time.monotonic() + 300)
        }
        mgr.notify_version_pending(11)
        mgr.clear_pending_version(11)

        assert mgr._pending_version is None
        assert mgr._logical_controller_epoch == old_epoch
        assert not mgr._recovering_logical_retirement_ids

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value={retiring.replica_id: retiring}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_ready_replica_infos',
                 return_value=[retiring, survivor]), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        # The retirement resumes normally: the drained victim is admitted for
        # termination under its original (unrotated) epoch and selection.
        assert retiring.status_property.is_scale_down
        assert (retiring.status_property.logical_retirement_controller_epoch ==
                old_epoch)
        mgr._terminate_replica.assert_called_once_with(
            retiring.replica_id,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)

    def test_recovery_under_pending_update_defers_then_covers_only_shortfall(
            self):
        """Recovery holds during a pending update; then reactivates only
        current-version capacity needed to cover the vNext shortfall."""
        mgr = _make_manager()
        mgr.latest_version = 2
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()

        def handed_off_victim(replica_id, version):
            info = self._ready_backend(replica_id, 1)
            info.version = version
            status = info.status_property
            status.is_scale_down = True
            status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
            status.wait_for_idle_before_termination = True
            status.logical_retirement_version = version
            status.logical_retirement_controller_epoch = 'old-controller-epoch'
            status.logical_retirement_generation = 4
            status.logical_retirement_target_capacity = 2
            status.logical_retirement_confirmed_generation = None
            status.logical_retirement_bounded_deadline = False
            status.logical_retirement_committed = False
            return info

        # Lowest id is an outdated (v1) victim: it must be skipped by the
        # shortfall reactivation even though it is scanned first.
        outdated = handed_off_victim(1, version=1)
        relabelled_a = handed_off_victim(2, version=2)
        relabelled_b = handed_off_victim(3, version=2)
        survivor = self._ready_backend(4, 1)
        survivor.version = 2
        mgr._recovering_logical_retirement_ids = {1, 2, 3}
        mgr._wait_for_idle_trackers = {
            replica_id: (mock.Mock(return_value=False),
                         replica_managers.time.monotonic() + 300
                        ) for replica_id in (1, 2, 3)
        }
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=2,
                generation=5,
                observed_slots_by_replica_id={4: 1},
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0,
                    4: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        # Fresh vNext evidence: target 2 while only the survivor (1 slot) is
        # routed, so the shortfall is exactly one slot.
        mgr._logical_target = (2, 5, 2)
        fleet = [outdated, relabelled_a, relabelled_b, survivor]

        # Phase 1: another, newer update is still pending; recovery must not
        # act on it at all.
        mgr.notify_version_pending(3)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=fleet):
            mgr._reconcile_recovering_logical_retirements()
        assert mgr._recovering_logical_retirement_ids == {1, 2, 3}
        assert outdated.status_property.is_scale_down
        assert relabelled_a.status_property.is_scale_down
        assert relabelled_b.status_property.is_scale_down

        # Phase 2: the pending update is cleared; reactivate only the one
        # current-version slot needed to cover the shortfall.
        mgr.clear_pending_version(3)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=fleet):
            mgr._reconcile_recovering_logical_retirements()

        assert not relabelled_a.status_property.is_scale_down
        assert relabelled_a.status_property.logical_retirement_version is None
        assert relabelled_b.status_property.is_scale_down
        assert outdated.status_property.is_scale_down
        assert mgr._recovering_logical_retirement_ids == {1, 3}
        mgr._terminate_replica.assert_not_called()

    @pytest.mark.parametrize(('bounded_precommit', 'admission_precommit'), [
        (False, False),
        (True, False),
        (False, True),
    ])
    def test_recovery_pass_indexes_valid_uncommitted_retirement(
            self, bounded_precommit, admission_precommit):
        retiring = self._recoverable_logical_retirement(
            1,
            bounded_precommit=bounded_precommit,
            admission_precommit=admission_precommit)
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._register_wait_for_idle = mock.Mock()
        recovered_url = 'http://old-backend'

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_yaml_contents',
                               return_value={}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={}), \
             mock.patch.object(mgr,
                               '_resolve_probe_urls',
                               return_value={1: recovered_url}) as resolve_urls:
            mgr._recover_replica_operations()

        resolve_urls.assert_called_once_with([retiring],
                                             phase_admission=mock.ANY)
        mgr._register_wait_for_idle.assert_called_once_with(
            retiring, replica_url=recovered_url)
        assert mgr._recovering_logical_retirement_ids == {1}
        assert mgr._logical_retirement_recovery_deadline is not None

    def test_recovery_drain_url_batch_failure_falls_back_per_replica(self):
        retiring = self._recoverable_logical_retirement(1)
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._register_wait_for_idle = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_yaml_contents',
                               return_value={}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={}), \
             mock.patch.object(mgr,
                               '_resolve_probe_urls',
                               side_effect=RuntimeError('snapshot failed')):
            mgr._recover_replica_operations()

        kwargs = mgr._register_wait_for_idle.call_args.kwargs
        assert (kwargs['replica_url']
                is replica_managers._REPLICA_URL_NOT_PROVIDED)

    def test_recovery_busy_ambient_phase_defers_without_provider_lookup(self):
        retiring = self._recoverable_logical_retirement(1)
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._register_wait_for_idle = mock.Mock()
        mgr._resolve_probe_urls = mock.Mock()
        phase_ready = threading.Event()
        release_phase = threading.Event()

        def _hold_v2_phase():
            with replica_managers.provider_phase.provider_phase(
                    replica_managers.provider_phase.ProviderPhaseMode.V2_FENCED
            ):
                phase_ready.set()
                assert release_phase.wait(timeout=5)

        holder = threading.Thread(target=_hold_v2_phase)
        holder.start()
        assert phase_ready.wait(timeout=5)
        try:
            with mock.patch.object(replica_managers.serve_state,
                                   'get_replica_infos',
                                   return_value=[retiring]), \
                 mock.patch.object(replica_managers.serve_state,
                                   'get_yaml_contents',
                                   return_value={}), \
                 mock.patch.object(
                     replica_managers.global_user_state,
                     'get_cluster_status_fields',
                     return_value={}):
                started = time.monotonic()
                mgr._recover_replica_operations()
                assert time.monotonic() - started < 1
        finally:
            release_phase.set()
            holder.join(timeout=5)

        assert not holder.is_alive()
        mgr._resolve_probe_urls.assert_not_called()
        mgr._register_wait_for_idle.assert_called_once_with(retiring,
                                                            replica_url=None)

    def test_recovery_v2_strict_drain_skips_endpoint_provider_lookup(self):
        retiring = _stamp_protocol_v2_fill(
            self._recoverable_logical_retirement(1))
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        mgr._register_wait_for_idle = mock.Mock()
        mgr._resolve_probe_urls = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_yaml_contents',
                               return_value={}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}), \
             mock.patch.object(
                 replica_managers.kubernetes_adaptor,
                 'physical_cluster_uid_fence') as provider_fence:
            mgr._recover_replica_operations()

        mgr._resolve_probe_urls.assert_not_called()
        provider_fence.assert_not_called()
        mgr._register_wait_for_idle.assert_called_once_with(retiring,
                                                            replica_url=None)

    def test_wait_idle_reduces_v2_before_ambient_url_resolution(self):
        mgr = _make_manager()
        mgr._is_pool = False
        ordinary = self._ready_backend(1, 1)
        fenced = _stamp_protocol_v2_fill(self._ready_backend(2, 1))
        for info in (ordinary, fenced):
            info.status_property.wait_for_idle_before_termination = True
        deadline = replica_managers.time.monotonic() + 60
        mgr._wait_for_idle_trackers = {
            1: (None, deadline),
            2: (mock.Mock(return_value=True), deadline),
        }
        events = []

        def _resolve(infos, **_kwargs):
            events.append(('resolve', [info.replica_id for info in infos]))
            return {info.replica_id: 'http://ordinary' for info in infos}

        def _register(info, *, deadline, replica_url):
            del replica_url
            mgr._wait_for_idle_trackers[info.replica_id] = (mock.Mock(
                return_value=False), deadline)

        def _terminate(replica_id, **_kwargs):
            events.append(('terminate', replica_id))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value={1: ordinary, 2: fenced}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={
                     ordinary.cluster_name: ('UP', 1),
                     fenced.cluster_name: ('UP', 1),
                 }), \
             mock.patch.object(mgr,
                               '_resolve_probe_urls',
                               side_effect=_resolve), \
             mock.patch.object(mgr,
                               '_register_wait_for_idle',
                               side_effect=_register), \
             mock.patch.object(mgr, '_persist_replica'), \
             mock.patch.object(mgr,
                               '_terminate_replica',
                               side_effect=_terminate):
            mgr._refresh_wait_for_idle()

        assert events == [('terminate', 2), ('resolve', [1])]

    def test_fresh_zero_paid_retirement_is_off_route_without_deadline(self):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._service_hash = 'svc-hash'
        mgr._controller_owner = (123, '10.0.0.5')
        mgr._update_recovery_required = False
        retiring = self._ready_backend(1, 1)
        authority = replica_managers.paid_retirement.FreshZeroAuthority(
            service_hash='svc-hash',
            demand_source_epoch=2,
            demand_feed_generation=7,
            capacity_plan_generation=9,
            capacity_plan_sha256='a' * 64,
            route_generation=11)
        record = {
            'replica_record_id': retiring.replica_record_id,
            'route_url': 'http://replica:8000',
        }
        mgr._register_wait_for_idle = mock.Mock()

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_info_from_id',
                return_value=retiring), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'admit_paid_retirement',
                 return_value=record) as admit:
            changed = mgr.reconcile_fresh_zero_paid_retirements(
                authority, [retiring])

        assert changed
        assert retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status == (
            common_utils.ProcessStatus.SCHEDULED)
        assert retiring.status_property.drain_cap_seconds is None
        assert retiring.status_property.drain_started_at is None
        assert retiring.status_property.wait_for_idle_before_termination
        admit.assert_called_once_with('svc',
                                      1,
                                      retiring,
                                      authority,
                                      requires_idle_proof=True,
                                      expected_service_hash='svc-hash',
                                      expected_controller_owner=(123,
                                                                 '10.0.0.5'))
        mgr._register_wait_for_idle.assert_called_once_with(
            retiring, deadline=math.inf, replica_url='http://replica:8000')

    @pytest.mark.parametrize('idle', [False, True])
    def test_exact_paid_retirement_never_uses_elapsed_time(self, idle):
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._service_hash = 'svc-hash'
        mgr._controller_owner = (123, '10.0.0.5')
        mgr._drain_proof_stats_value = mock.Mock()
        retiring = self._ready_backend(1, 1)
        retiring.status_property.is_scale_down = True
        retiring.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        retiring.status_property.wait_for_idle_before_termination = True
        retiring.status_property.drain_cap_seconds = None
        mgr._wait_for_idle_trackers = {
            1: (mock.Mock(return_value=idle), math.inf)
        }
        record = {
            'service_hash': 'svc-hash',
            'replica_record_id': retiring.replica_record_id,
            'demand_source_epoch': 2,
            'demand_feed_generation': 7,
            'capacity_plan_generation': 9,
            'capacity_plan_sha256': 'a' * 64,
            'route_generation': 11,
            'route_url': 'http://replica:8000',
            'state': (replica_managers.paid_retirement.PaidRetirementState.
                      ACTIVE.value),
        }
        mgr._terminate_replica = mock.Mock()

        with mock.patch.object(
                replica_managers.paid_retirement,
                'list_for_service',
                return_value={1: record}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos_from_ids',
                 return_value={1: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'commit_paid_retirement',
                 return_value=True) as commit:
            mgr._refresh_wait_for_idle()

        if idle:
            commit.assert_called_once()
            mgr._terminate_replica.assert_called_once_with(
                1,
                sync_down_logs=False,
                replica_drain_delay_seconds=0,
                is_scale_down=True,
                in_flight_drain_cap_seconds=0)
        else:
            commit.assert_not_called()
            mgr._terminate_replica.assert_not_called()
            assert mgr._wait_for_idle_trackers[1][1] == math.inf

    def test_new_positive_generation_cancels_only_active_retirement(self):
        mgr = _make_manager()
        mgr._service_hash = 'svc-hash'
        mgr._controller_owner = (123, '10.0.0.5')
        mgr._update_recovery_required = False
        retiring = self._ready_backend(1, 1)
        retiring.status_property.is_scale_down = True
        retiring.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        retiring.status_property.wait_for_idle_before_termination = True
        mgr._wait_for_idle_trackers = {1: (mock.Mock(), math.inf)}
        record = {
            'replica_record_id': retiring.replica_record_id,
            'state': (replica_managers.paid_retirement.PaidRetirementState.
                      ACTIVE.value),
        }

        with mock.patch.object(
                replica_managers.paid_retirement,
                'list_for_service',
                return_value={1: record}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_infos_from_ids',
                 return_value={1: retiring}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'cancel_paid_retirement',
                 return_value=True) as cancel:
            changed = mgr.cancel_uncommitted_paid_retirements('svc-hash', 8)

        assert changed
        assert not retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status is None
        assert not retiring.status_property.wait_for_idle_before_termination
        assert 1 not in mgr._wait_for_idle_trackers
        cancel.assert_called_once_with('svc',
                                       1,
                                       retiring,
                                       8,
                                       expected_service_hash='svc-hash',
                                       expected_controller_owner=(123,
                                                                  '10.0.0.5'))

    def test_initial_v2_strict_drain_uid_mismatch_stays_off_route(self):
        retiring = _stamp_protocol_v2_fill(
            self._recoverable_logical_retirement(1))
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()
        handle = _protocol_v2_handle(retiring)
        provider_fence = mock.MagicMock()
        provider_fence.return_value.__enter__.side_effect = (
            exceptions.KubernetesPhysicalClusterIdentityError('UID mismatch'))

        with mock.patch.object(
                replica_managers.global_user_state,
                'get_handle_from_cluster_name',
                return_value=handle), \
             mock.patch.object(replica_managers.kubernetes_adaptor,
                               'physical_cluster_uid_fence', provider_fence), \
             mock.patch.object(replica_managers.backend_utils,
                               'get_endpoints') as endpoints:
            mgr._register_wait_for_idle(retiring)

        endpoints.assert_not_called()
        assert retiring.status_property.service_ready_now is False
        assert retiring.status_property.sky_down_status == (
            common_utils.ProcessStatus.SCHEDULED)
        assert mgr._wait_for_idle_trackers[retiring.replica_id][0] is None
        mgr._persist_replica.assert_called_once_with(retiring.replica_id,
                                                     retiring)
        mgr._terminate_replica.assert_not_called()

    def test_retry_v2_strict_drain_uid_mismatch_stays_off_route(self):
        retiring = _stamp_protocol_v2_fill(
            self._recoverable_logical_retirement(1))
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()
        mgr._wait_for_idle_trackers = {
            retiring.replica_id: (None, replica_managers.time.monotonic() + 300)
        }
        handle = _protocol_v2_handle(retiring)
        provider_fence = mock.MagicMock()
        provider_fence.return_value.__enter__.side_effect = (
            exceptions.KubernetesPhysicalClusterIdentityError('UID mismatch'))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value={retiring.replica_id: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_clusters_from_names',
                 return_value={
                     retiring.cluster_name: {
                         'name': retiring.cluster_name,
                         'handle': handle,
                     }
                 }), \
             mock.patch.object(replica_managers.serve_utils,
                               'get_provider_configs_for_handles',
                               return_value={}), \
             mock.patch.object(replica_managers.kubernetes_adaptor,
                               'physical_cluster_uid_fence', provider_fence), \
             mock.patch.object(replica_managers.backend_utils,
                               'get_endpoints') as endpoints:
            mgr._refresh_wait_for_idle()

        endpoints.assert_not_called()
        assert retiring.status_property.service_ready_now is False
        assert retiring.status_property.sky_down_status == (
            common_utils.ProcessStatus.SCHEDULED)
        assert mgr._wait_for_idle_trackers[retiring.replica_id][0] is None
        mgr._persist_replica.assert_called_once_with(retiring.replica_id,
                                                     retiring)
        mgr._terminate_replica.assert_not_called()

    def test_retry_v2_strict_drain_batches_group_identity_failure(self):
        retiring = []
        records = {}
        for replica_id in (1, 2):
            info = _stamp_protocol_v2_fill(
                self._recoverable_logical_retirement(replica_id))
            retiring.append(info)
            records[info.cluster_name] = {
                'name': info.cluster_name,
                'handle': _protocol_v2_handle(info),
            }
        mgr = _make_manager()
        mgr._is_pool = False
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()
        deadline = replica_managers.time.monotonic() + 300
        mgr._wait_for_idle_trackers = {
            info.replica_id: (None, deadline) for info in retiring
        }
        provider_fence = mock.MagicMock()
        provider_fence.return_value.__enter__.side_effect = (
            exceptions.KubernetesPhysicalClusterIdentityError('UID mismatch'))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value={info.replica_id: info for info in retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={info.cluster_name: ('UP', 1)
                               for info in retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_clusters_from_names',
                 return_value=records), \
             mock.patch.object(replica_managers.serve_utils,
                               'get_provider_configs_for_handles',
                               return_value={}), \
             mock.patch.object(replica_managers.kubernetes_adaptor,
                               'physical_cluster_uid_fence', provider_fence), \
             mock.patch.object(replica_managers.backend_utils,
                               'get_endpoints') as endpoints:
            mgr._refresh_wait_for_idle()

        endpoints.assert_not_called()
        # Durable validation constructs a context for each row, but only the
        # one shared group scope is entered and contacts the provider.
        assert provider_fence.return_value.__enter__.call_count == 1
        for info in retiring:
            assert info.status_property.service_ready_now is False
            assert mgr._wait_for_idle_trackers[info.replica_id][0] is None
        assert mgr._provider_identity_uncertain_replica_ids() == {1, 2}
        assert mgr._persist_replica.call_count == 2
        mgr._terminate_replica.assert_not_called()

    @pytest.mark.parametrize('confirmed_generation', [None, 4])
    def test_recovery_adopts_old_epoch_retirement_and_preserves_deadline(
            self, confirmed_generation):
        retiring = self._recoverable_logical_retirement(
            1, confirmed_generation=confirmed_generation)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        drain_started_at = retiring.status_property.drain_started_at
        tracker_deadline = mgr._wait_for_idle_trackers[1][1]

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]) as fleet_read:
            mgr._reconcile_recovering_logical_retirements()

        fleet_read.assert_called_once_with('svc')
        status = retiring.status_property
        assert (retiring.status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert status.logical_retirement_version == 1
        assert status.logical_retirement_controller_epoch == (
            'test-controller-epoch')
        assert status.logical_retirement_generation == 5
        assert status.logical_retirement_target_capacity == 1
        assert status.logical_retirement_confirmed_generation is None
        assert status.logical_retirement_committed is False
        assert status.drain_started_at == drain_started_at
        assert mgr._wait_for_idle_trackers[1][1] == tracker_deadline
        assert mgr._recovering_logical_retirement_ids == {1}
        mgr._persist_replica.assert_called_once_with(1, retiring)
        mgr._terminate_replica.assert_not_called()

    def test_recovery_adoption_waits_for_newer_generation(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()
            mgr._reconcile_recovering_logical_retirements()
            assert mgr._recovering_logical_retirement_ids == {1}

            mgr._logical_reconcile_snapshot = dataclasses.replace(
                mgr._logical_reconcile_snapshot, generation=6)
            mgr._logical_target = (1, 6, 1)
            mgr._reconcile_recovering_logical_retirements()

        assert not mgr._recovering_logical_retirement_ids
        mgr._persist_replica.assert_called_once_with(1, retiring)
        mgr._terminate_replica.assert_not_called()

    def test_newer_snapshots_do_not_starve_recovery_adoption_and_release(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, generation=6)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()
            assert mgr._recovering_logical_retirement_ids == {1}
            assert retiring.status_property.logical_retirement_generation == 6

            mgr._logical_reconcile_snapshot = dataclasses.replace(
                mgr._logical_reconcile_snapshot, generation=7)
            mgr._reconcile_recovering_logical_retirements()

        assert not mgr._recovering_logical_retirement_ids
        mgr._persist_replica.assert_called_once_with(1, retiring)
        mgr._terminate_replica.assert_not_called()

    def test_recovery_adoption_blocks_same_generation_admission(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()
        tracker, _ = mgr._wait_for_idle_trackers[1]
        mgr._wait_for_idle_trackers[1] = (tracker,
                                          replica_managers.time.monotonic() - 1)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        assert mgr._recovering_logical_retirement_ids == {1}
        assert not retiring.is_ready
        mgr._terminate_replica.assert_not_called()

    def test_recovery_gate_blocks_queued_shutdown_admission(self, tmp_path):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        mgr._resource_scope = None
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        down_thread.format_exc = None
        mgr._down_thread_pool[1] = down_thread

        with mock.patch.object(mgr, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 mgr, '_reconcile_recovering_logical_retirements'), \
             mock.patch.object(
                 mgr, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _service, ids:
                               ({1: retiring} if ids else {})), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils,
                               'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True) as can_terminate:
            mgr._refresh_thread_pool()

        can_terminate.assert_not_called()
        down_thread.start.assert_not_called()
        assert mgr._recovering_logical_retirement_ids == {1}
        assert not retiring.is_ready
        assert (retiring.status_property.sky_down_status ==
                common_utils.ProcessStatus.SCHEDULED)
        assert (retiring.status_property.logical_retirement_controller_epoch ==
                'old-controller-epoch')
        mgr._persist_replica.assert_not_called()

    def test_adopted_recovery_timeout_stays_off_route_without_newer_generation(
            self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()
            mgr._logical_retirement_recovery_deadline = (
                replica_managers.time.monotonic() - 1)
            mgr._reconcile_recovering_logical_retirements()

        assert not retiring.is_ready
        assert mgr._recovering_logical_retirement_ids == {1}
        mgr._persist_replica.assert_called_once_with(1, retiring)

    def test_recovery_reactivates_only_target_shortfall_then_adopts_remainder(
            self):
        candidates = [
            self._recoverable_logical_retirement(replica_id)
            for replica_id in (1, 2, 3)
        ]
        survivor = self._ready_backend(10, 1)
        mgr = self._logical_recovery_manager(candidates, survivor, target=3)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert candidates[0].is_ready
        assert candidates[1].is_ready
        assert (candidates[2].status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert mgr._recovering_logical_retirement_ids == {3}
        assert mgr._logical_retirement_reactivation_generation == 5

        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=6,
                observed_slots_by_replica_id={
                    1: 1,
                    2: 1,
                    10: 1,
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0,
                    10: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 6, 3)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert (candidates[2].status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert candidates[2].status_property.logical_retirement_generation == 6
        assert mgr._recovering_logical_retirement_ids == {3}

        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, generation=7)
        mgr._logical_target = (1, 7, 3)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()
        assert not mgr._recovering_logical_retirement_ids

    def test_recovery_reactivates_more_after_prior_candidate_stays_unobserved(
            self):
        candidates = [
            self._recoverable_logical_retirement(replica_id)
            for replica_id in (1, 2)
        ]
        survivor = self._ready_backend(10, 1)
        mgr = self._logical_recovery_manager(candidates, survivor, target=2)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()
        assert candidates[0].is_ready
        assert mgr._recovering_logical_retirement_ids == {2}

        # A newer generation still cannot observe the first reactivation.
        # Recompute the shortfall and release one more candidate rather than
        # leaving the service under-covered until the timeout.
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=6,
                observed_slots_by_replica_id={10: 1},
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    10: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 6, 2)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert candidates[1].is_ready
        assert not mgr._recovering_logical_retirement_ids

    def test_recovery_timeout_with_fresh_coverage_waits_newer_generation(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        mgr._logical_retirement_recovery_deadline = (
            replica_managers.time.monotonic() - 1)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert (retiring.status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert retiring.status_property.logical_retirement_controller_epoch == (
            'test-controller-epoch')
        assert mgr._recovering_logical_retirement_ids == {1}
        mgr._persist_replica.assert_called_once_with(1, retiring)

    def test_recovery_timeout_without_fresh_evidence_stays_off_route(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        mgr._logical_reconcile_snapshot = None
        mgr._logical_target = None
        expired_deadline = replica_managers.time.monotonic() - 1
        mgr._logical_retirement_recovery_deadline = expired_deadline

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert (retiring.status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert retiring.status_property.logical_retirement_controller_epoch == (
            'old-controller-epoch')
        assert mgr._recovering_logical_retirement_ids == {1}
        assert mgr._logical_retirement_recovery_deadline > expired_deadline
        mgr._persist_replica.assert_not_called()

    def test_recovery_reactivates_old_version_before_provisioning_is_ready(
            self):
        retiring = self._recoverable_logical_retirement(1)
        provisioning = self._ready_backend(2, 4)
        provisioning.version = 2
        provisioning.status_property.sky_launch_status = (
            common_utils.ProcessStatus.RUNNING)
        provisioning.status_property.service_ready_now = False
        provisioning.status_property.first_ready_time = None
        mgr = self._logical_recovery_manager([retiring], provisioning, target=4)
        mgr.latest_version = 2
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            version=2,
            observed_slots_by_replica_id={},
            in_flight_by_replica_id={
                1: 0,
                2: 0
            })
        mgr._logical_target = (2, 5, 4)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, provisioning]):
            mgr._reconcile_recovering_logical_retirements()

        assert retiring.is_ready
        assert not retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status is None
        assert not mgr._recovering_logical_retirement_ids
        mgr._persist_replica.assert_called_once_with(1, retiring)

    def test_exact_recovery_reactivates_before_same_card_is_ready(self):
        retiring = self._recoverable_logical_retirement(1)
        retiring.resources_override = {'accelerators': {'L4': 1}}
        provisioning = self._ready_backend(2, 1)
        provisioning.version = 2
        provisioning.resources_override = {'accelerators': {'L4': 1}}
        provisioning.status_property.sky_launch_status = (
            common_utils.ProcessStatus.RUNNING)
        provisioning.status_property.service_ready_now = False
        provisioning.status_property.first_ready_time = None
        mgr = self._logical_recovery_manager([retiring], provisioning)
        mgr.latest_version = 2
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            version=2,
            observed_slots_by_replica_id={},
            in_flight_by_replica_id={
                1: 0,
                2: 0,
            })
        mgr._logical_target = (2, 5, 1, (('L4', 1),), (('L4', 1),))

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, provisioning]):
            mgr._reconcile_recovering_logical_retirements()

        assert retiring.is_ready
        assert not retiring.status_property.is_scale_down
        assert not mgr._recovering_logical_retirement_ids
        mgr._persist_replica.assert_called_once_with(1, retiring)

    def test_recovery_shortfall_reactivation_is_bounded_per_generation(self):
        candidates = [
            self._recoverable_logical_retirement(replica_id)
            for replica_id in range(1, 26)
        ]
        survivor = self._ready_backend(100, 1)
        survivor.version = 2
        mgr = self._logical_recovery_manager(candidates, survivor, target=25)
        mgr.latest_version = 2
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            version=2,
            observed_slots_by_replica_id={100: 1},
            in_flight_by_replica_id={
                **{
                    info.replica_id: 0 for info in candidates
                },
                100: 0,
            })
        mgr._logical_target = (2, 5, 25)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert sum(info.is_ready for info in candidates) == 20
        assert mgr._recovering_logical_retirement_ids == set(range(21, 26))
        assert mgr._logical_retirement_reactivation_generation == 5

    def test_recovery_partial_failure_consumes_generation_wave(self):
        candidates = [
            self._recoverable_logical_retirement(replica_id)
            for replica_id in range(1, 26)
        ]
        survivor = self._ready_backend(100, 1)
        survivor.version = 2
        mgr = self._logical_recovery_manager(candidates, survivor, target=25)
        mgr.latest_version = 2
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            version=2,
            observed_slots_by_replica_id={100: 1},
            in_flight_by_replica_id={
                **{
                    info.replica_id: 0 for info in candidates
                }, 100: 0
            })
        mgr._logical_target = (2, 5, 25)
        mgr._persist_replica.side_effect = [None] * 19 + [
            RuntimeError('database unavailable')
        ]

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]), \
             pytest.raises(RuntimeError, match='database unavailable'):
            mgr._reconcile_recovering_logical_retirements()

        assert mgr._persist_replica.call_count == 20
        assert mgr._logical_retirement_reactivation_generation == 5
        assert mgr._recovering_logical_retirement_ids == set(range(20, 26))

        mgr._persist_replica.reset_mock(side_effect=True)
        mgr._persist_replica.side_effect = None
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]):
            mgr._reconcile_recovering_logical_retirements()
        mgr._persist_replica.assert_not_called()

    def test_recovery_adoption_persist_failure_stays_off_route_for_retry(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)
        mgr._persist_replica.side_effect = RuntimeError('database unavailable')

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()

        assert (retiring.status ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        assert retiring.status_property.logical_retirement_controller_epoch == (
            'old-controller-epoch')
        assert 1 in mgr._recovering_logical_retirement_ids
        mgr._terminate_replica.assert_not_called()

    def test_recovery_bulk_adoption_reads_fleet_once(self):
        candidates = [
            self._recoverable_logical_retirement(replica_id)
            for replica_id in range(1, 201)
        ]
        survivor = self._ready_backend(1000, 1)
        mgr = self._logical_recovery_manager(candidates, survivor)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=candidates + [survivor]) as scan:
            mgr._reconcile_recovering_logical_retirements()

        scan.assert_called_once_with('svc')
        assert mgr._persist_replica.call_count == 200
        assert mgr._recovering_logical_retirement_ids == set(range(1, 201))

    def test_adopted_expired_same_version_retirement_reactivates_safely(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        mgr = self._logical_recovery_manager([retiring], survivor)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()
            mgr._logical_reconcile_snapshot = dataclasses.replace(
                mgr._logical_reconcile_snapshot, generation=6)
            mgr._logical_target = (1, 6, 1)
            mgr._reconcile_recovering_logical_retirements()
        tracker, _ = mgr._wait_for_idle_trackers[1]
        mgr._wait_for_idle_trackers[1] = (tracker,
                                          replica_managers.time.monotonic() - 1)
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            in_flight_by_replica_id={
                1: 1,
                2: 0,
            })

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: retiring}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        assert retiring.is_ready
        mgr._terminate_replica.assert_not_called()

    def test_manager_rechecks_ready_coverage_before_accepting_retirement(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        eight = self._ready_backend(1, 8)
        four = self._ready_backend(2, 4)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={
                    1: 8,
                    2: 4
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=four), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[eight, four]):
            mgr._logical_target = (1, 3, 9)
            mgr.scale_down_logically(2, 9, 1, 3)
            defer.assert_not_called()

            mgr._logical_target = (1, 3, 8)
            mgr.scale_down_logically(2, 8, 1, 3)

        defer.assert_called_once_with(2,
                                      logical_retirement=(1, 3, 8),
                                      replica_info=four,
                                      replica_url=None)

    def test_manager_rejects_same_total_wrong_card_retirement(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        l4 = self._ready_backend(1, 1)
        a100 = self._ready_backend(2, 1)
        l4.resources_override = {'accelerators': {'L4': 1}}
        a100.resources_override = {'accelerators': {'A100': 1}}
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={
                    1: 1,
                    2: 1,
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        target_by_card = (('L4', 1),)
        shapes = (('L4', 1), ('A100', 1))
        mgr._logical_target = (1, 3, 1, target_by_card, shapes)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[l4, a100]):
            mgr.scale_down_logically(1, 1, 1, 3, target_by_card, shapes)

        defer.assert_not_called()

    def test_manager_accepts_exact_card_zero_target_retirement(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        l4 = self._ready_backend(1, 1)
        l4.resources_override = {'accelerators': {'L4': 1}}
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={1: 1},
                in_flight_by_replica_id={1: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        shapes = (('L4', 1), ('A100', 1))
        mgr._logical_target = (1, 3, 0, (), shapes)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[l4]):
            mgr.scale_down_logically(1, 0, 1, 3, (), shapes)

        defer.assert_called_once_with(1,
                                      logical_retirement=(1, 3, 0),
                                      replica_info=l4,
                                      replica_url=None)

    def test_manager_retires_old_card_removed_from_exact_catalog(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        old_a100 = self._ready_backend(1, 1)
        old_a100.version = 0
        old_a100.resources_override = {'accelerators': {'A100': 1}}
        h100 = self._ready_backend(2, 1)
        h100.resources_override = {'accelerators': {'H100': 1}}
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={
                    1: 1,
                    2: 1,
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        target_by_card = (('H100', 1),)
        shapes = (('H100', 1),)
        mgr._logical_target = (1, 3, 1, target_by_card, shapes)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[old_a100, h100]):
            mgr.scale_down_logically(1, 1, 1, 3, target_by_card, shapes)

        defer.assert_called_once_with(1,
                                      logical_retirement=(1, 3, 1),
                                      replica_info=old_a100,
                                      replica_url=None)

    def test_invalidate_logical_target_blocks_recovery_adoption(self):
        retiring = self._recoverable_logical_retirement(1)
        survivor = self._ready_backend(2, 1)
        retiring.resources_override = {'accelerators': {'L4': 1}}
        survivor.resources_override = {'accelerators': {'A100': 1}}
        mgr = self._logical_recovery_manager([retiring], survivor)
        mgr._logical_target = (1, 5, 1, (('L4', 1),), (('L4', 1), ('A100', 1)))

        mgr.invalidate_logical_target()
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()

        mgr._persist_replica.assert_not_called()
        assert retiring.status_property.logical_retirement_controller_epoch == (
            'old-controller-epoch')
        assert 1 in mgr._recovering_logical_retirement_ids

    @pytest.mark.parametrize('victim_still_present', [True, False])
    def test_logical_retirement_uses_one_fleet_snapshot(self,
                                                        victim_still_present):
        """Victim resolution and capacity proof use the same fleet snapshot."""
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        terminal_victim = self._ready_backend(1, 4)
        terminal_victim.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        peer = self._ready_backend(2, 8)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={
                    1: 4,
                    2: 8
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 3, 4)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_info_from_id') as point_read, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=([terminal_victim, peer]
                                             if victim_still_present else
                                             [peer])) as scan:
            mgr.scale_down_logically(1, 4, 1, 3)

        point_read.assert_not_called()
        scan.assert_called_once_with('svc')
        defer.assert_not_called()

    @pytest.mark.parametrize('victim_missing', [True, False])
    def test_terminal_or_missing_retirement_uses_one_fleet_scan(
            self, victim_missing):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        terminal_victim = self._ready_backend(1, 4)
        terminal_victim.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=3,
                observed_slots_by_replica_id={1: 4},
                in_flight_by_replica_id={1: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 3, 0)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_info_from_id') as point_read, mock.patch.object(
                    replica_managers.serve_state,
                    'get_replica_infos',
                    return_value=([] if victim_missing else [terminal_victim
                                                            ])) as scan:
            mgr.scale_down_logically(1, 0, 1, 3)

        point_read.assert_not_called()
        scan.assert_called_once_with('svc')

    def test_stale_logical_scale_down_batch_reads_no_fleet(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 0)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos') as scan, \
             mock.patch.object(mgr, '_defer_scale_down_until_idle') as defer, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr.scale_down_logically_batch([1, 2, 3], 0, 1, 3)

        scan.assert_not_called()
        defer.assert_not_called()
        terminate.assert_not_called()

    def test_newer_snapshot_does_not_starve_current_scale_down_target(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        backends = [self._ready_backend(replica_id, 4) for replica_id in (1, 2)]
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=5,
                observed_slots_by_replica_id={
                    1: 4,
                    2: 4
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        # Generation 4 is still the current published autoscaler decision.
        # Only the independently arriving capacity snapshot advanced.
        mgr._logical_target = (1, 4, 4)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=backends) as scan:
            mgr.scale_down_logically_batch([1], 4, 1, 4)

        scan.assert_called_once_with('svc')
        defer.assert_called_once_with(1,
                                      logical_retirement=(1, 4, 4),
                                      replica_info=backends[0],
                                      replica_url=None)

    def test_newer_snapshot_rechecks_scale_down_victim_idle(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        backends = [self._ready_backend(replica_id, 4) for replica_id in (1, 2)]
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=5,
                observed_slots_by_replica_id={
                    1: 4,
                    2: 4
                },
                in_flight_by_replica_id={
                    1: 1,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 4)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=backends) as scan, \
             mock.patch.object(mgr,
                               '_defer_scale_down_until_idle') as defer:
            mgr.scale_down_logically_batch([1], 4, 1, 4)

        scan.assert_called_once_with('svc')
        defer.assert_not_called()

    def test_pending_version_rejects_logical_scale_down_batch(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._pending_version = 2
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 0)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos') as scan, \
             mock.patch.object(mgr, '_defer_scale_down_until_idle') as defer, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr.scale_down_logically_batch([1], 0, 1, 4)

        scan.assert_not_called()
        defer.assert_not_called()
        terminate.assert_not_called()

    def test_logical_scale_down_batch_scans_once_and_stops_at_target(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        backends = [
            self._ready_backend(replica_id, 4) for replica_id in (1, 2, 3)
        ]
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={
                    1: 4,
                    2: 4,
                    3: 4
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 4)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=backends) as scan:
            mgr.scale_down_logically_batch([1, 2, 3], 4, 1, 4)

        scan.assert_called_once_with('svc')
        assert defer.call_args_list == [
            mock.call(1,
                      logical_retirement=(1, 4, 4),
                      replica_info=backends[0],
                      replica_url=None),
            mock.call(2,
                      logical_retirement=(1, 4, 4),
                      replica_info=backends[1],
                      replica_url=None),
        ]

    def test_logical_scale_down_batch_uses_exact_observed_contribution(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        degraded = self._ready_backend(1, 8)
        survivor = self._ready_backend(2, 8)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={
                    1: 4,
                    2: 8
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 8)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[degraded, survivor]):
            mgr.scale_down_logically_batch([1, 2], 8, 1, 4)

        defer.assert_called_once_with(1,
                                      logical_retirement=(1, 4, 8),
                                      replica_info=degraded,
                                      replica_url=None)

    def test_logical_scale_down_batch_counts_ready_old_coverage(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        old_backends = [
            self._ready_backend(replica_id, 8) for replica_id in (1, 2, 3)
        ]
        for backend in old_backends:
            backend.version = 0
        latest = self._ready_backend(100, 1)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={100: 1},
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0,
                    100: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 3)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=old_backends + [latest]):
            mgr.scale_down_logically_batch([1, 2, 3], 3, 1, 4)

        defer.assert_called_once_with(1,
                                      logical_retirement=(1, 4, 3),
                                      replica_info=old_backends[0],
                                      replica_url=None)

    def test_logical_ready_capacity_excludes_already_retiring_old_backends(
            self):
        active_old = self._ready_backend(1, 8)
        active_old.version = 0
        retiring_old = self._ready_backend(2, 8)
        retiring_old.version = 0
        retiring_old.status_property.is_scale_down = True
        latest = self._ready_backend(100, 2)
        snapshot = replica_managers.LogicalReconcileSnapshot(
            version=1,
            generation=4,
            observed_slots_by_replica_id={100: 2},
            in_flight_by_replica_id={
                1: 0,
                2: 0,
                100: 0,
            },
            unknown_replica_ids=frozenset(),
            received_at=replica_managers.time.monotonic())

        capacity = replica_managers.SkyPilotReplicaManager._logical_ready_capacity(
            [active_old, retiring_old, latest], snapshot, 1, frozenset())

        assert capacity == 3

    def test_logical_scale_down_batch_skips_duplicates_and_retiring_rows(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        first = self._ready_backend(1, 1)
        already_retiring = self._ready_backend(2, 1)
        already_retiring.status_property.is_scale_down = True
        survivor = self._ready_backend(3, 1)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={
                    1: 1,
                    2: 1,
                    3: 1
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 1)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[first, already_retiring,
                                             survivor]):
            mgr.scale_down_logically_batch([1, 1, 2, 3], 1, 1, 4)

        defer.assert_called_once_with(1,
                                      logical_retirement=(1, 4, 1),
                                      replica_info=first,
                                      replica_url=None)

    def test_logical_scale_down_batch_aborts_after_acceptance_error(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        backends = [
            self._ready_backend(replica_id, 1) for replica_id in (1, 2, 3)
        ]
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={
                    1: 1,
                    2: 1,
                    3: 1
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 0)
        defer = mock.Mock(side_effect=[None, RuntimeError('persist failed')])
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=backends) as scan, \
             pytest.raises(RuntimeError, match='persist failed'):
            mgr.scale_down_logically_batch([1, 2, 3], 0, 1, 4)

        scan.assert_called_once_with('svc')
        assert defer.call_args_list == [
            mock.call(1,
                      logical_retirement=(1, 4, 0),
                      replica_info=backends[0],
                      replica_url=None),
            mock.call(2,
                      logical_retirement=(1, 4, 0),
                      replica_info=backends[1],
                      replica_url=None),
        ]

    def test_logical_scale_down_batch_handles_unserved_and_outdated_victims(
            self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        unserved = self._ready_backend(1, 4)
        unserved.status_property.service_ready_now = False
        unserved.status_property.first_ready_time = None
        survivor = self._ready_backend(2, 4)
        outdated = self._ready_backend(3, 8)
        outdated.version = 0
        outdated.status_property.service_ready_now = False
        outdated.status_property.first_ready_time = None
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={2: 4},
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 4)
        terminate = mock.Mock()
        mgr._terminate_replica = terminate

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[unserved, survivor, outdated]):
            mgr.scale_down_logically_batch([1, 3], 4, 1, 4)

        assert [call.args[0] for call in terminate.call_args_list] == [1, 3]

    def test_logical_scale_down_orders_v2_drain_before_ordinary_immediate(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        ordinary_unserved = self._ready_backend(1, 1)
        ordinary_unserved.status_property.service_ready_now = False
        ordinary_unserved.status_property.first_ready_time = None
        fenced_served = _stamp_protocol_v2_fill(self._ready_backend(2, 1))
        survivor = self._ready_backend(3, 1)
        backends = [ordinary_unserved, fenced_served, survivor]
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={
                    1: 0,
                    2: 1,
                    3: 1,
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0,
                    3: 0,
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 1)
        events = []

        def _resolve(infos, **_kwargs):
            events.append(('v2-resolve', [info.replica_id for info in infos]))
            return {info.replica_id: 'http://fenced' for info in infos}

        def _defer(replica_id, **_kwargs):
            events.append(('v2-defer', replica_id))

        def _terminate(replica_id, **_kwargs):
            events.append(('ordinary-terminate', replica_id))

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=backends), \
             mock.patch.object(mgr,
                               '_resolve_probe_urls',
                               side_effect=_resolve), \
             mock.patch.object(mgr,
                               '_defer_scale_down_until_idle',
                               side_effect=_defer), \
             mock.patch.object(mgr,
                               '_terminate_replica',
                               side_effect=_terminate):
            # The ordinary never-served victim intentionally appears first.
            mgr.scale_down_logically_batch([1, 2], 1, 1, 4)

        assert events == [('v2-resolve', [2]), ('v2-defer', 2),
                          ('ordinary-terminate', 1)]

    def test_logical_scale_down_batches_absent_finished_launch_cleanup(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._service_hash = 'incarnation-a'
        mgr._controller_owner = (101, '10.0.0.1')
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        victims = [
            self._ready_backend(replica_id, 1) for replica_id in range(1, 16)
        ]
        for victim in victims:
            victim.status_property.first_ready_time = None
        finished_launch = mock.Mock()
        finished_launch.is_alive.return_value = False
        mgr._launch_thread_pool[1] = finished_launch
        mgr._replica_to_request_id[1] = 'request-1'
        mgr._replica_to_launch_cancelled[1] = True
        mgr._replica_to_logical_launch_fence[1] = (1, 4)
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 0)
        mgr._terminate_replica = mock.Mock()

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=victims), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'get_existing_replica_cluster_names',
                 return_value=set()) as cluster_inventory, \
             mock.patch.object(replica_managers.serve_state,
                               'remove_replicas',
                               return_value=True) as remove_batch, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id') as point_read, \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'cluster_with_name_exists') as cluster_exists, \
             mock.patch.object(replica_managers.serve_state,
                               'remove_replica') as remove_one:
            mgr.scale_down_logically_batch(list(range(1, 16)), 0, 1, 4)

        cluster_inventory.assert_called_once_with(victims)
        remove_batch.assert_called_once_with(
            'svc',
            list(range(1, 16)),
            'incarnation-a',
            expected_controller_owner=(101, '10.0.0.1'),
            expected_replica_record_ids={
                victim.replica_id: victim.replica_record_id
                for victim in victims
            })
        mgr._terminate_replica.assert_not_called()
        point_read.assert_not_called()
        cluster_exists.assert_not_called()
        remove_one.assert_not_called()
        assert 1 not in mgr._launch_thread_pool
        assert 1 not in mgr._replica_to_request_id
        assert 1 not in mgr._replica_to_launch_cancelled
        assert 1 not in mgr._replica_to_logical_launch_fence

    def test_logical_scale_down_batch_preserves_live_cleanup_paths(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._service_hash = 'incarnation-a'
        mgr._controller_owner = (101, '10.0.0.1')
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        victims = [
            self._ready_backend(replica_id, 1) for replica_id in (1, 2, 3, 4)
        ]
        for victim in victims:
            victim.status_property.first_ready_time = None
        finished_launch = mock.Mock()
        finished_launch.is_alive.return_value = False
        live_launch = mock.Mock()
        live_launch.is_alive.return_value = True
        mgr._launch_thread_pool[1] = finished_launch
        mgr._launch_thread_pool[3] = live_launch
        mgr._down_thread_pool[4] = mock.Mock()
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 0)
        mgr._terminate_replica = mock.Mock()

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=victims), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'get_existing_replica_cluster_names',
                 return_value={victims[1].cluster_name}) as cluster_inventory, \
             mock.patch.object(replica_managers.serve_state,
                               'remove_replicas',
                               return_value=True) as remove_batch:
            mgr.scale_down_logically_batch([1, 2, 3, 4], 0, 1, 4)

        cluster_inventory.assert_called_once_with(victims[:2])
        remove_batch.assert_called_once_with(
            'svc', [1],
            'incarnation-a',
            expected_controller_owner=(101, '10.0.0.1'),
            expected_replica_record_ids={1: victims[0].replica_record_id})
        assert [call.args[0] for call in mgr._terminate_replica.call_args_list
               ] == [2, 3, 4]
        assert 1 not in mgr._launch_thread_pool
        assert 3 in mgr._launch_thread_pool
        assert 4 in mgr._down_thread_pool

    def test_logical_scale_down_batch_keeps_tracking_on_fence_loss(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._service_hash = 'incarnation-a'
        mgr._controller_owner = (101, '10.0.0.1')
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        victim = self._ready_backend(1, 1)
        victim.status_property.first_ready_time = None
        finished_launch = mock.Mock()
        finished_launch.is_alive.return_value = False
        mgr._launch_thread_pool[1] = finished_launch
        mgr._replica_to_request_id[1] = 'request-1'
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 0)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[victim]), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'get_existing_replica_cluster_names',
                 return_value=set()), \
             mock.patch.object(replica_managers.serve_state,
                               'remove_replicas',
                               return_value=False), \
             pytest.raises(RuntimeError, match='incarnation changed'):
            mgr.scale_down_logically_batch([1], 0, 1, 4)

        assert 1 in mgr._launch_thread_pool
        assert 1 in mgr._replica_to_request_id

    def test_logical_scale_down_batch_excludes_served_replica(self):
        # A replica that ever reached ready owns a real cloud cluster and must
        # never enter the durable batch row-delete, even when the one-shot
        # cluster inventory momentarily omits its cluster. The batch path only
        # drops the row and the paid-capacity claim; it does not terminate the
        # cluster. So a served victim must stay on the graceful drain path
        # (`_defer_scale_down_until_idle`), guarded independently by the
        # pre-pass `first_ready_time is not None` exclusion and by the
        # main-loop `has_served` branch. Every other batch test resets
        # `first_ready_time` to None, so neither guard is otherwise exercised.
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._service_hash = 'incarnation-a'
        mgr._controller_owner = (101, '10.0.0.1')
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        never_served = self._ready_backend(1, 1)
        never_served.status_property.first_ready_time = None
        # Keep the default first_ready_time (1.0): this victim has served.
        served = self._ready_backend(2, 1)
        finished_launch = mock.Mock()
        finished_launch.is_alive.return_value = False
        mgr._launch_thread_pool[1] = finished_launch
        mgr._replica_to_request_id[1] = 'request-1'
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={2: 1},
                in_flight_by_replica_id={2: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 0)
        mgr._terminate_replica = mock.Mock()
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[never_served, served]), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'get_existing_replica_cluster_names',
                 return_value=set()) as cluster_inventory, \
             mock.patch.object(replica_managers.serve_state,
                               'remove_replicas',
                               return_value=True) as remove_batch:
            mgr.scale_down_logically_batch([1, 2], 0, 1, 4)

        # The served victim is filtered out before the cluster inventory read,
        # so only the never-served victim is a batch-delete candidate.
        cluster_inventory.assert_called_once_with([never_served])
        # Only the never-served row is dropped in the fenced batch delete.
        remove_batch.assert_called_once_with(
            'svc', [1],
            'incarnation-a',
            expected_controller_owner=(101, '10.0.0.1'),
            expected_replica_record_ids={1: never_served.replica_record_id})
        # The served victim takes the graceful drain path, never a hard delete.
        assert [call.args[0] for call in defer.call_args_list] == [2]
        mgr._terminate_replica.assert_not_called()
        assert 1 not in mgr._launch_thread_pool
        assert 1 not in mgr._replica_to_request_id

    def test_logical_scale_down_batch_matches_sequential_singletons(self):

        def _run(batch: bool):
            mgr = _make_manager()
            mgr._uses_logical_replicas = True
            backends = {
                replica_id: self._ready_backend(replica_id, width)
                for replica_id, width in ((1, 2), (2, 1), (3, 1), (4, 1),
                                          (5, 1), (6, 8), (7, 4))
            }
            backends[6].version = 0
            mgr._logical_reconcile_snapshot = (
                replica_managers.LogicalReconcileSnapshot(
                    version=1,
                    generation=4,
                    observed_slots_by_replica_id={
                        1: 2,
                        2: 1,
                        3: 1,
                        5: 0,
                        6: 8,
                        7: 4,
                    },
                    in_flight_by_replica_id={
                        1: 0,
                        2: 1,
                        3: 0,
                        4: 0,
                        5: 0,
                        6: 0,
                        7: 0,
                    },
                    unknown_replica_ids=frozenset({3}),
                    received_at=replica_managers.time.monotonic()))
            mgr._logical_target = (1, 4, 4)
            accepted = []

            def _defer(replica_id,
                       logical_retirement,
                       *,
                       replica_info,
                       replica_url=None):
                assert logical_retirement == (1, 4, 4)
                assert replica_info is backends[replica_id]
                assert replica_url is None
                accepted.append(replica_id)
                backends[replica_id].status_property.is_scale_down = True

            mgr._defer_scale_down_until_idle = _defer
            victim_ids = [2, 3, 4, 5, 6, 1, 7]
            with mock.patch.object(
                    replica_managers.serve_state,
                    'get_replica_infos',
                    side_effect=lambda _service: list(backends.values())):
                if batch:
                    mgr.scale_down_logically_batch(victim_ids, 4, 1, 4)
                else:
                    for replica_id in victim_ids:
                        mgr.scale_down_logically(replica_id, 4, 1, 4)
            return accepted

        assert _run(batch=True) == _run(batch=False) == [5, 6, 1]

    def test_zero_capacity_rebalance_replacement_cannot_retire_incumbent(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        victim = self._ready_backend(1, 8)
        replacement = self._ready_backend(2, 8)
        snapshot = replica_managers.LogicalReconcileSnapshot(
            version=1,
            generation=3,
            observed_slots_by_replica_id={
                1: 8,
                2: 0
            },
            in_flight_by_replica_id={
                1: 0,
                2: 0
            },
            unknown_replica_ids=frozenset(),
            received_at=replica_managers.time.monotonic())
        mgr._logical_reconcile_snapshot = snapshot
        mgr._logical_target = (1, 3, 8)
        defer = mock.Mock()
        mgr._defer_scale_down_until_idle = defer

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=victim), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[victim, replacement]):
            mgr.scale_down_logically(1, 8, 1, 3)
            defer.assert_not_called()

            snapshot.observed_slots_by_replica_id[2] = 8
            mgr.scale_down_logically(1, 8, 1, 3)

        defer.assert_called_once_with(1,
                                      logical_retirement=(1, 3, 8),
                                      replica_info=victim,
                                      replica_url=None)

    def test_controller_restart_aborts_persisted_retirement(self):
        mgr = _make_manager()
        retiring = self._ready_backend(1, 8)
        retiring.status_property.logical_retirement_version = 1
        retiring.status_property.logical_retirement_controller_epoch = (
            'prior-controller-epoch')
        retiring.status_property.logical_retirement_generation = 3
        retiring.status_property.logical_retirement_target_capacity = 0

        assert mgr._logical_retirement_state(retiring) == 'abort'

    def test_successful_logical_retirement_persists_confirmation_before_down(
            self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        retiring = self._ready_backend(1, 4)
        retiring_peer = self._ready_backend(3, 4)
        survivor = self._ready_backend(2, 8)
        for info in (retiring, retiring_peer):
            status = info.status_property
            status.is_scale_down = True
            status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
            status.wait_for_idle_before_termination = True
            status.logical_retirement_version = 1
            status.logical_retirement_controller_epoch = 'test-controller-epoch'
            status.logical_retirement_generation = 4
            status.logical_retirement_target_capacity = 8
            status.logical_retirement_confirmed_generation = None
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=5,
                # The controller removes SHUTTING_DOWN replicas from its
                # URL-to-ID translation before this post-retirement report.
                observed_slots_by_replica_id={2: 8},
                in_flight_by_replica_id={2: 0},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 5, 8)
        mgr._wait_for_idle_trackers = {
            1: (mock.Mock(return_value=True),
                replica_managers.time.monotonic() + 60),
            3: (mock.Mock(return_value=True),
                replica_managers.time.monotonic() + 60),
        }
        persisted = []

        def _record_persisted(replica_id, info):
            status = info.status_property
            persisted.append(
                (replica_id, status.logical_retirement_confirmed_generation,
                 status.wait_for_idle_before_termination))

        mgr._persist_replica = mock.Mock(side_effect=_record_persisted)
        mgr._terminate_replica = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={
                                   1: retiring,
                                   3: retiring_peer,
                               }), \
             mock.patch.object(replica_managers.serve_state,
                               'get_ready_replica_infos',
                               return_value=[retiring, retiring_peer,
                                             survivor]) as scan, \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={
                     'svc-1': ('UP', 1),
                     'svc-3': ('UP', 1),
                 }):
            mgr._refresh_wait_for_idle()

        scan.assert_called_once_with('svc')
        assert persisted[-2:] == [(1, 5, True), (3, 5, True)]
        assert mgr._terminate_replica.call_args_list == [
            mock.call(1,
                      sync_down_logs=False,
                      replica_drain_delay_seconds=0,
                      is_scale_down=True,
                      in_flight_drain_cap_seconds=0),
            mock.call(3,
                      sync_down_logs=False,
                      replica_drain_delay_seconds=0,
                      is_scale_down=True,
                      in_flight_drain_cap_seconds=0),
        ]
        assert {1, 3}.issubset(mgr._wait_for_idle_trackers)

    @pytest.mark.parametrize('retiring_version,should_terminate', [(9, True),
                                                                   (10, False)])
    def test_unknown_then_absent_logical_retirement_deadline_is_bounded_only_for_outdated_backend(
            self, monkeypatch, retiring_version, should_terminate):
        now = [100.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        mgr, retiring, survivor = self._pending_logical_retirement(
            retiring_version=retiring_version)
        tracker = replica_managers._ReplicaDrainTracker(mgr,
                                                        'http://old-backend',
                                                        drain_started=now[0])
        mgr._wait_for_idle_trackers = {9: (tracker, 160.0)}

        def _refresh():
            with mock.patch.object(replica_managers.serve_state,
                                   'get_replica_infos_from_ids',
                                   return_value={9: retiring}), \
                 mock.patch.object(replica_managers.serve_state,
                                   'get_ready_replica_infos',
                                   return_value=[retiring, survivor]), \
                 mock.patch.object(
                     replica_managers.global_user_state,
                     'get_cluster_status_fields',
                     return_value={'svc-9': ('UP', 1)}):
                mgr._refresh_wait_for_idle()

        # Reproduce the production migration: the old nginx backend is first
        # occupancy-UNKNOWN, then disappears from every LB overlay. Absence
        # cannot clear the tracker's UNKNOWN taint before the deadline.
        now[0] = 110.0
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, received_at=now[0])
        mgr._lb_in_flight_report = (now[0], {
            'http://old-backend': 0
        }, set(), {'http://old-backend'}, set(), 'lb-session')
        _refresh()
        mgr._terminate_replica.assert_not_called()

        now[0] = 120.0
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, received_at=now[0])
        mgr._lb_in_flight_report = (now[0], {}, set(), set(), set(),
                                    'lb-session')
        _refresh()
        mgr._terminate_replica.assert_not_called()

        now[0] = 160.0
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, received_at=now[0])
        mgr._lb_in_flight_report = (now[0], {}, set(), set(), set(),
                                    'lb-session')
        _refresh()

        if should_terminate:
            mgr._terminate_replica.assert_called_once_with(
                9,
                sync_down_logs=False,
                replica_drain_delay_seconds=0,
                is_scale_down=True,
                in_flight_drain_cap_seconds=0)
            assert retiring.status_property.is_scale_down
        else:
            mgr._terminate_replica.assert_not_called()
            assert not retiring.status_property.is_scale_down
        assert (retiring.status_property.logical_retirement_bounded_deadline
                is should_terminate)
        assert 9 not in mgr._wait_for_idle_trackers

    @pytest.mark.parametrize('guard', [
        'stale_snapshot',
        'pending_update',
        'target_growth',
        'unknown_replacement',
        'insufficient_replacement',
    ])
    def test_outdated_deadline_never_bypasses_logical_coverage_fences(
            self, monkeypatch, guard):
        now = [200.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        mgr, retiring, survivor = self._pending_logical_retirement()
        snapshot = mgr._logical_reconcile_snapshot
        if guard == 'stale_snapshot':
            snapshot = dataclasses.replace(snapshot, received_at=0.0)
        elif guard == 'pending_update':
            mgr._pending_version = 11
        elif guard == 'target_growth':
            mgr._logical_target = (10, 5, 2)
        elif guard == 'unknown_replacement':
            snapshot = dataclasses.replace(snapshot,
                                           unknown_replica_ids=frozenset({10}))
        elif guard == 'insufficient_replacement':
            snapshot.observed_slots_by_replica_id[10] = 0
        mgr._logical_reconcile_snapshot = snapshot
        retiring.status_property.drain_started_at = 1234.5
        mgr._lb_in_flight_report = (now[0], {}, set(), set(), set(),
                                    'lb-session')
        mgr._wait_for_idle_trackers = {
            9: (mock.Mock(return_value=False), now[0])
        }

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={9: retiring}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_ready_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={'svc-9': ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        mgr._terminate_replica.assert_not_called()
        if guard in ('stale_snapshot', 'pending_update'):
            assert retiring.status_property.wait_for_idle_before_termination
            assert 9 in mgr._wait_for_idle_trackers
        else:
            assert not retiring.status_property.is_scale_down
            assert retiring.status_property.drain_started_at is None
            assert 9 not in mgr._wait_for_idle_trackers

    def test_outdated_bounded_retirement_retries_scheduling_failure(
            self, monkeypatch):
        now = [200.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        mgr, retiring, survivor = self._pending_logical_retirement()
        tracker = mock.Mock(return_value=False)
        mgr._wait_for_idle_trackers = {9: (tracker, now[0])}
        mgr._terminate_replica = mock.Mock(
            side_effect=[RuntimeError('database unavailable'), None])

        def _refresh():
            with mock.patch.object(replica_managers.serve_state,
                                   'get_replica_infos_from_ids',
                                   return_value={9: retiring}), \
                 mock.patch.object(replica_managers.serve_state,
                                   'get_ready_replica_infos',
                                   return_value=[retiring, survivor]), \
                 mock.patch.object(
                     replica_managers.global_user_state,
                     'get_cluster_status_fields',
                     return_value={'svc-9': ('UP', 1)}):
                mgr._refresh_wait_for_idle()

        with pytest.raises(RuntimeError, match='database unavailable'):
            _refresh()
        assert retiring.status_property.wait_for_idle_before_termination
        assert retiring.status_property.logical_retirement_bounded_deadline
        assert 9 in mgr._wait_for_idle_trackers

        _refresh()
        assert mgr._terminate_replica.call_count == 2
        assert 9 not in mgr._wait_for_idle_trackers

    @pytest.mark.parametrize(
        'bounded_deadline,confirmed_generation,should_start', [
            (False, 5, False),
            (True, 5, True),
            (True, None, False),
            (True, True, False),
            (True, '5', False),
        ])
    def test_only_bounded_outdated_confirmation_bypasses_late_victim_occupancy(
            self, monkeypatch, tmp_path, bounded_deadline, confirmed_generation,
            should_start):
        now = [200.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        mgr, retiring, survivor = self._pending_logical_retirement()
        retiring.status_property.wait_for_idle_before_termination = False
        retiring.status_property.logical_retirement_confirmed_generation = (
            confirmed_generation)
        retiring.status_property.logical_retirement_bounded_deadline = (
            bounded_deadline)
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            generation=6,
            in_flight_by_replica_id={
                9: 1,
                10: 0
            },
            received_at=now[0])
        mgr._logical_target = (10, 6, 1)
        mgr._launch_thread_pool = thread_utils.ThreadSafeDict()
        mgr._down_thread_pool = thread_utils.ThreadSafeDict()
        mgr._replica_to_request_id = thread_utils.ThreadSafeDict()
        mgr._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        mgr._down_thread_pool[9] = down_thread

        with mock.patch.object(mgr, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 mgr, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _svc, ids:
                               ({9: retiring} if ids else {})), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True), \
             mock.patch.object(mgr, '_persist_replica') as persist:
            mgr._refresh_thread_pool()

        if should_start:
            down_thread.start.assert_called_once_with()
            assert (retiring.status_property.sky_down_status ==
                    common_utils.ProcessStatus.RUNNING)
            assert retiring.status_property.logical_retirement_committed
            persist.assert_called_once_with(9, retiring)
        else:
            down_thread.start.assert_not_called()
            assert (retiring.status_property.sky_down_status ==
                    common_utils.ProcessStatus.SCHEDULED)
            persist.assert_not_called()

    def _run_durable_logical_down_admission(self, tmp_path, commit_result):
        mgr, retiring, survivor = self._pending_logical_retirement()
        mgr._service_hash = 'hash'
        mgr._controller_owner = (123, '127.0.0.1')
        status = retiring.status_property
        status.wait_for_idle_before_termination = False
        status.logical_retirement_confirmed_generation = 5
        authority = types.SimpleNamespace(deadline_monotonic=math.inf)
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, generation=6, authority=authority)
        mgr._logical_target = (10, 6, 1)
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        mgr._down_thread_pool = thread_utils.ThreadSafeDict({9: down_thread})
        commit_effect = (commit_result if callable(commit_result) else
                         lambda *_args, **_kwargs: commit_result)

        with mock.patch.object(mgr, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 mgr, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={9: retiring}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(replica_managers.serve_state,
                               'commit_logical_retirement',
                               side_effect=commit_effect) as commit, \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True):
            mgr._refresh_thread_pool()
        return mgr, retiring, down_thread, commit

    def test_durable_logical_commit_precedes_worker_start(self, tmp_path):

        def _commit(*_args, **_kwargs):
            # The typed repository returns the exact committed row.
            retiring = _args[2]
            retiring.status_property.logical_retirement_confirmed_generation = 6
            retiring.status_property.logical_retirement_committed = True
            retiring.status_property.sky_down_status = (
                common_utils.ProcessStatus.RUNNING)
            return replica_managers.serve_state.LogicalRetirementCommitResult(
                replica_managers.serve_state.LogicalRetirementCommitState.
                COMMITTED, retiring)

        mgr, _, down_thread, commit = (self._run_durable_logical_down_admission(
            tmp_path, _commit))

        commit.assert_called_once()
        down_thread.start.assert_called_once_with()
        mgr._persist_replica.assert_not_called()

    def test_ambiguous_logical_commit_never_starts_original_worker(
            self, tmp_path):
        ambiguous = replica_managers.serve_state.LogicalRetirementCommitResult(
            replica_managers.serve_state.LogicalRetirementCommitState.AMBIGUOUS)
        mgr, retiring, down_thread, commit = (
            self._run_durable_logical_down_admission(tmp_path, ambiguous))

        commit.assert_called_once()
        down_thread.start.assert_not_called()
        assert 9 not in mgr._down_thread_pool
        assert mgr._ambiguous_logical_retirement_commit_ids == {9}
        assert mgr._logical_reconcile_state == (
            replica_managers._LogicalReconcileState(target=None, snapshot=None))

        # A fresh durable readback, never the lost acknowledgement, decides
        # whether and how to reconstruct cleanup.
        retiring.status_property.logical_retirement_confirmed_generation = 6
        retiring.status_property.logical_retirement_committed = True
        retiring.status_property.sky_down_status = (
            common_utils.ProcessStatus.RUNNING)
        mgr._terminate_replica = mock.Mock()
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={9: retiring}):
            mgr._reconcile_ambiguous_logical_retirement_commits()

        mgr._terminate_replica.assert_called_once_with(
            9,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)
        assert not mgr._ambiguous_logical_retirement_commit_ids

    def test_ambiguous_logical_commit_uncommitted_readback_requeues(
            self, tmp_path):
        ambiguous = replica_managers.serve_state.LogicalRetirementCommitResult(
            replica_managers.serve_state.LogicalRetirementCommitState.AMBIGUOUS)
        mgr, retiring, down_thread, _ = (
            self._run_durable_logical_down_admission(tmp_path, ambiguous))

        down_thread.start.assert_not_called()
        assert 9 not in mgr._down_thread_pool
        assert mgr._ambiguous_logical_retirement_commit_ids == {9}
        # The commit did not land. Exact readback retains the reversible
        # admission precommit; reconstructing its unstarted worker does not
        # authorize start because the manager authority was revoked.
        mgr._terminate_replica = mock.Mock()
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={9: retiring}):
            mgr._reconcile_ambiguous_logical_retirement_commits()

        mgr._terminate_replica.assert_called_once_with(
            9,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)
        assert not mgr._ambiguous_logical_retirement_commit_ids

    @pytest.mark.parametrize('scenario', [
        'outdated_bounded_start',
        'same_version_abort',
        'target_growth_abort',
    ])
    def test_budget_delayed_logical_admission_retains_original_deadline(
            self, monkeypatch, tmp_path, scenario):
        now = [100.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        retiring_version = 10 if scenario == 'same_version_abort' else 9
        mgr, retiring, survivor = self._pending_logical_retirement(
            retiring_version=retiring_version)
        mgr._terminate_replica = types.MethodType(
            replica_managers.SkyPilotReplicaManager._terminate_replica, mgr)
        mgr._resource_scope = None
        tracker = replica_managers._ReplicaDrainTracker(mgr,
                                                        'http://old-backend',
                                                        drain_started=90.0)
        mgr._wait_for_idle_trackers = {9: (tracker, 160.0)}
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        down_thread.format_exc = None
        mgr._persist_replica = mock.Mock()

        def _infos_from_ids(_service_name, replica_ids):
            infos = {9: retiring, 10: survivor}
            return {
                replica_id: infos[replica_id]
                for replica_id in replica_ids
                if replica_id in infos
            }

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_ready_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_info_with_resource_action_identity',
                 return_value=(retiring, None)), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=_infos_from_ids), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={
                     retiring.cluster_name: ('UP', 1),
                     survivor.cluster_name: ('UP', 1)
                 }), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'cluster_with_name_exists',
                 return_value=True), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value=str(tmp_path / 'replica.log')), \
             mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=down_thread), \
             mock.patch.object(
                 mgr, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(
                 controller_utils,
                 'can_terminate',
                 side_effect=lambda *_args, **_kwargs: now[0] >= 200.0):
            # Explicit idle proves the victim drained, but the global
            # terminate budget cannot admit its already-scheduled worker.
            mgr._lb_in_flight_report = (now[0], {
                'http://old-backend': 0
            }, set(), set(), {'http://old-backend'}, 'lb-session')
            mgr._refresh_thread_pool()
            down_thread.start.assert_not_called()
            assert 9 in mgr._down_thread_pool
            assert 9 in mgr._wait_for_idle_trackers
            assert not retiring.status_property.wait_for_idle_before_termination
            assert not (
                retiring.status_property.logical_retirement_bounded_deadline)
            assert not retiring.status_property.logical_retirement_committed

            # A late busy report invalidates the ordinary idle proof. The
            # original deadline must still promote only an outdated backend;
            # a same-version or uncovered grown-target retirement is cancelled.
            now[0] = 200.0
            mgr._logical_reconcile_snapshot = dataclasses.replace(
                mgr._logical_reconcile_snapshot,
                in_flight_by_replica_id={
                    9: 1,
                    10: 0
                },
                received_at=now[0])
            mgr._lb_in_flight_report = (now[0], {
                'http://old-backend': 1
            }, set(), set(), {'http://old-backend'}, 'lb-session')
            if scenario == 'target_growth_abort':
                mgr._logical_target = (10, 5, 2)
            mgr._refresh_thread_pool()

        if scenario == 'outdated_bounded_start':
            down_thread.start.assert_called_once_with()
            assert retiring.status_property.is_scale_down
            assert (retiring.status_property.sky_down_status ==
                    common_utils.ProcessStatus.RUNNING)
            assert (
                retiring.status_property.logical_retirement_bounded_deadline)
            assert retiring.status_property.logical_retirement_committed
        else:
            down_thread.start.assert_not_called()
            assert not retiring.status_property.is_scale_down
            assert retiring.status_property.sky_down_status is None
            assert 9 not in mgr._down_thread_pool
        assert 9 not in mgr._wait_for_idle_trackers

    def _recover_logical_teardown(self,
                                  tmp_path,
                                  down_status,
                                  bounded_deadline,
                                  committed=True,
                                  current_target=1):
        mgr, retiring, survivor = self._pending_logical_retirement()
        mgr._terminate_replica = types.MethodType(
            replica_managers.SkyPilotReplicaManager._terminate_replica, mgr)
        mgr._logical_controller_epoch = 'new-controller-epoch'
        mgr._logical_target = (10, 5, current_target)
        mgr._resource_scope = None
        status = retiring.status_property
        status.logical_retirement_controller_epoch = 'old-controller-epoch'
        status.wait_for_idle_before_termination = False
        status.logical_retirement_confirmed_generation = 5
        status.logical_retirement_bounded_deadline = bounded_deadline
        status.logical_retirement_committed = committed
        status.sky_down_status = down_status
        if committed is None:
            mgr._logical_reconcile_snapshot = dataclasses.replace(
                mgr._logical_reconcile_snapshot,
                authority=types.SimpleNamespace(deadline_monotonic=math.inf))
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        down_thread.format_exc = None
        mgr._persist_replica = mock.Mock()

        def _infos_from_ids(_service_name, replica_ids):
            infos = {9: retiring, 10: survivor}
            return {
                replica_id: infos[replica_id]
                for replica_id in replica_ids
                if replica_id in infos
            }

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=retiring), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_info_with_resource_action_identity',
                 return_value=(retiring, None)), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=_infos_from_ids), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={
                     retiring.cluster_name: ('UP', 1),
                     survivor.cluster_name: ('UP', 1)
                 }), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'cluster_with_name_exists',
                 return_value=True), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value=str(tmp_path / 'replica.log')), \
             mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=down_thread), \
             mock.patch.object(mgr, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 mgr, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True):
            mgr._recover_replica_operations()
            # FAILED cleanup is reconciled at the end of the first refresh
            # and admitted on the next one. SCHEDULED/RUNNING recovery has a
            # worker ready for admission immediately.
            if down_status == common_utils.ProcessStatus.FAILED:
                mgr._refresh_thread_pool()
            mgr._refresh_thread_pool()

        return mgr, retiring, down_thread

    @pytest.mark.parametrize('bounded_deadline', [False, True])
    @pytest.mark.parametrize('down_status', [
        common_utils.ProcessStatus.SCHEDULED,
        common_utils.ProcessStatus.RUNNING,
        common_utils.ProcessStatus.FAILED,
    ])
    def test_recovery_never_reactivates_committed_logical_teardown(
            self, tmp_path, bounded_deadline, down_status):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path, down_status, bounded_deadline)

        down_thread.start.assert_called_once_with()
        assert retiring.status_property.is_scale_down
        assert (retiring.status_property.sky_down_status ==
                common_utils.ProcessStatus.RUNNING)
        assert retiring.status_property.logical_retirement_version is None
        assert (retiring.status_property.logical_retirement_controller_epoch
                is None)
        assert (retiring.status_property.logical_retirement_generation is None)
        assert (retiring.status_property.logical_retirement_target_capacity
                is None)
        assert (retiring.status_property.logical_retirement_confirmed_generation
                is None)
        assert not retiring.status_property.logical_retirement_bounded_deadline
        assert not retiring.status_property.logical_retirement_committed
        assert 9 in mgr._down_thread_pool

    def test_restart_recovers_ambiguous_admission_precommit_after_n_plus_one(
            self, tmp_path):
        """A crash before process-local readback cannot lose worker requeue."""
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path,
            common_utils.ProcessStatus.SCHEDULED,
            bounded_deadline=False,
            committed=False)

        down_thread.start.assert_not_called()
        status = retiring.status_property
        assert status.is_scale_down
        assert status.sky_down_status == common_utils.ProcessStatus.SCHEDULED
        assert status.logical_retirement_version == 10
        assert (status.logical_retirement_controller_epoch ==
                mgr._logical_controller_epoch)
        assert status.logical_retirement_generation == 5
        assert status.logical_retirement_confirmed_generation is None
        assert not status.logical_retirement_bounded_deadline
        assert status.wait_for_idle_before_termination
        assert not status.logical_retirement_committed
        assert mgr._recovering_logical_retirement_ids == {9}
        assert 9 not in mgr._down_thread_pool

        # Adoption consumes generation N. Only a genuine N+1 releases the row
        # to the ordinary idle-proof path, which reconstructs an unstarted
        # worker rather than advertising the backend or invoking a provider
        # directly from restart recovery.
        survivor = self._ready_backend(10, 1)
        survivor.version = 10
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot, generation=6)
        mgr._logical_target = (10, 6, 1)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]):
            mgr._reconcile_recovering_logical_retirements()
        assert not mgr._recovering_logical_retirement_ids

        mgr._wait_for_idle_trackers[9] = (mock.Mock(return_value=True),
                                          replica_managers.time.monotonic() +
                                          60)
        mgr._terminate_replica = mock.Mock()
        with mock.patch.object(replica_managers.paid_retirement,
                               'list_for_service',
                               return_value={}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={9: retiring}), \
             mock.patch.object(replica_managers.serve_state,
                               'get_ready_replica_infos',
                               return_value=[survivor]), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={retiring.cluster_name: ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        mgr._terminate_replica.assert_called_once_with(
            9,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)
        assert status.logical_retirement_confirmed_generation == 6
        assert not status.logical_retirement_committed

    def test_recovery_adopts_legacy_ambiguous_as_reversible_precommit(
            self, tmp_path):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path,
            common_utils.ProcessStatus.SCHEDULED,
            bounded_deadline=False,
            committed=None)

        down_thread.start.assert_not_called()
        assert retiring.status_property.is_scale_down
        assert (retiring.status_property.sky_down_status ==
                common_utils.ProcessStatus.SCHEDULED)
        assert retiring.status_property.logical_retirement_version == 10
        assert retiring.status_property.wait_for_idle_before_termination
        assert retiring.status_property.logical_retirement_committed is False
        assert (retiring.status_property.logical_retirement_generation ==
                mgr._logical_reconcile_snapshot.generation)
        assert 9 not in mgr._legacy_uncertain_logical_retirement_ids
        assert 9 not in mgr._down_thread_pool

    def test_newer_snapshot_adopts_legacy_ambiguous_retirement(self):
        mgr, retiring, _ = self._pending_logical_retirement()
        status = retiring.status_property
        status.logical_retirement_controller_epoch = 'old-controller-epoch'
        status.wait_for_idle_before_termination = False
        status.logical_retirement_confirmed_generation = 5
        status.logical_retirement_committed = None
        mgr._legacy_uncertain_logical_retirement_ids = {9}
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            generation=6,
            authority=types.SimpleNamespace(deadline_monotonic=math.inf))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value={9: retiring}), \
             mock.patch.object(mgr, '_register_wait_for_idle') as register:
            mgr._reconcile_legacy_uncertain_logical_retirements()

        assert status.logical_retirement_committed is False
        assert status.wait_for_idle_before_termination
        assert status.logical_retirement_generation == 6
        assert status.logical_retirement_confirmed_generation is None
        assert 9 not in mgr._legacy_uncertain_logical_retirement_ids
        register.assert_called_once_with(retiring, replica_url=None)
        mgr._terminate_replica.assert_not_called()

    def test_legacy_normalization_lost_ack_adopts_exact_durable_readback(self):
        mgr, retiring, _ = self._pending_logical_retirement()
        status = retiring.status_property
        status.logical_retirement_controller_epoch = 'old-controller-epoch'
        status.wait_for_idle_before_termination = False
        status.logical_retirement_confirmed_generation = 5
        status.logical_retirement_committed = None
        mgr._legacy_uncertain_logical_retirement_ids = {9}
        mgr._logical_reconcile_snapshot = dataclasses.replace(
            mgr._logical_reconcile_snapshot,
            generation=6,
            authority=types.SimpleNamespace(deadline_monotonic=math.inf))
        stored = {}

        def _commit_then_lose_ack(_replica_id, info):
            stored['info'] = copy.deepcopy(info)
            raise RuntimeError('lost acknowledgement')

        mgr._persist_replica.side_effect = _commit_then_lose_ack
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={9: retiring}):
            mgr._reconcile_legacy_uncertain_logical_retirements()

        assert mgr._legacy_uncertain_logical_retirement_ids == {9}
        durable = stored['info']
        assert durable.status_property.logical_retirement_committed is False
        assert durable.status_property.wait_for_idle_before_termination
        mgr._persist_replica.reset_mock(side_effect=True)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={9: durable}), \
             mock.patch.object(mgr, '_register_wait_for_idle') as register:
            mgr._reconcile_legacy_uncertain_logical_retirements()

        assert not mgr._legacy_uncertain_logical_retirement_ids
        register.assert_called_once_with(durable, replica_url=None)
        mgr._terminate_replica.assert_not_called()

    @pytest.mark.parametrize('down_status', [
        common_utils.ProcessStatus.RUNNING,
        common_utils.ProcessStatus.FAILED,
    ])
    def test_recovery_finishes_intrinsically_committed_legacy_teardown(
            self, tmp_path, down_status):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path, down_status, bounded_deadline=False, committed=None)

        down_thread.start.assert_called_once_with()
        assert retiring.status_property.is_scale_down
        assert (retiring.status_property.sky_down_status ==
                common_utils.ProcessStatus.RUNNING)
        assert retiring.status_property.logical_retirement_version is None
        assert 9 in mgr._down_thread_pool

    def test_recovery_normalizes_legacy_ambiguous_retirement_even_if_uncovered(
            self, tmp_path):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path,
            common_utils.ProcessStatus.SCHEDULED,
            bounded_deadline=False,
            committed=None,
            current_target=2)

        down_thread.start.assert_not_called()
        assert retiring.status_property.is_scale_down
        assert (retiring.status_property.sky_down_status ==
                common_utils.ProcessStatus.SCHEDULED)
        assert retiring.status_property.logical_retirement_committed is False
        assert retiring.status_property.wait_for_idle_before_termination
        assert (retiring.status_property.logical_retirement_generation ==
                mgr._logical_reconcile_snapshot.generation)
        assert 9 not in mgr._legacy_uncertain_logical_retirement_ids
        assert 9 not in mgr._down_thread_pool

    def test_recovery_revalidates_unadmitted_bounded_retirement_after_growth(
            self, tmp_path):
        mgr, retiring, down_thread = self._recover_logical_teardown(
            tmp_path,
            common_utils.ProcessStatus.SCHEDULED,
            bounded_deadline=True,
            committed=False,
            current_target=2)

        down_thread.start.assert_not_called()
        assert not retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status is None
        assert retiring.status_property.logical_retirement_version is None
        assert not retiring.status_property.logical_retirement_committed
        assert 9 not in mgr._down_thread_pool

    @pytest.mark.parametrize('malformed_field,malformed_value', [
        ('logical_retirement_confirmed_generation', None),
        ('logical_retirement_confirmed_generation', True),
        ('logical_retirement_confirmed_generation', '5'),
        ('logical_retirement_bounded_deadline', 'true'),
        ('logical_retirement_committed', 'true'),
        ('logical_retirement_version', True),
        ('logical_retirement_controller_epoch', ''),
    ])
    def test_recovery_keeps_unconfirmed_or_malformed_teardown_fail_closed(
            self, tmp_path, malformed_field, malformed_value):
        mgr, retiring, survivor = self._pending_logical_retirement()
        mgr._terminate_replica = types.MethodType(
            replica_managers.SkyPilotReplicaManager._terminate_replica, mgr)
        mgr._logical_controller_epoch = 'new-controller-epoch'
        mgr._resource_scope = None
        status = retiring.status_property
        status.logical_retirement_controller_epoch = 'old-controller-epoch'
        status.wait_for_idle_before_termination = False
        status.logical_retirement_confirmed_generation = 5
        status.logical_retirement_bounded_deadline = False
        status.logical_retirement_committed = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        setattr(status, malformed_field, malformed_value)
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        down_thread.format_exc = None
        mgr._persist_replica = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=retiring), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_info_with_resource_action_identity',
                 return_value=(retiring, None)), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={9: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={
                     retiring.cluster_name: ('UP', 1),
                     survivor.cluster_name: ('UP', 1)
                 }), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'cluster_with_name_exists',
                 return_value=True), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value=str(tmp_path / 'replica.log')), \
             mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=down_thread), \
             mock.patch.object(mgr, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 mgr, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(mgr, '_reconcile_failed_cleanup'), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True):
            mgr._recover_replica_operations()
            mgr._refresh_thread_pool()

        down_thread.start.assert_not_called()
        assert not retiring.status_property.is_scale_down
        assert retiring.status_property.sky_down_status is None
        assert 9 not in mgr._down_thread_pool

    def test_logical_retirement_retries_termination_scheduling_failure(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        mgr._is_pool = False
        retiring = self._ready_backend(1, 4)
        survivor = self._ready_backend(2, 8)
        status = retiring.status_property
        status.is_scale_down = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.wait_for_idle_before_termination = True
        status.logical_retirement_version = 1
        status.logical_retirement_controller_epoch = 'test-controller-epoch'
        status.logical_retirement_generation = 4
        status.logical_retirement_target_capacity = 8
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=5,
                observed_slots_by_replica_id={
                    1: 0,
                    2: 8
                },
                in_flight_by_replica_id={
                    1: 0,
                    2: 0
                },
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 5, 8)
        tracker = mock.Mock(return_value=True)
        mgr._wait_for_idle_trackers = {
            1: (tracker, replica_managers.time.monotonic() + 60)
        }
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock(
            side_effect=[RuntimeError('database unavailable'), None])

        def _refresh():
            with mock.patch.object(replica_managers.serve_state,
                                   'get_replica_infos_from_ids',
                                   return_value={1: retiring}), \
                 mock.patch.object(replica_managers.serve_state,
                                   'get_ready_replica_infos',
                                   return_value=[retiring, survivor]), \
                 mock.patch.object(
                     replica_managers.global_user_state,
                     'get_cluster_status_fields',
                     return_value={'svc-1': ('UP', 1)}):
                mgr._refresh_wait_for_idle()

        with pytest.raises(RuntimeError, match='database unavailable'):
            _refresh()

        assert status.wait_for_idle_before_termination
        assert 1 in mgr._wait_for_idle_trackers
        _refresh()
        assert mgr._terminate_replica.call_count == 2
        assert 1 in mgr._wait_for_idle_trackers

    @pytest.mark.parametrize('confirmed_generation', [None, 5])
    def test_restart_before_or_after_confirmation_never_terminates_stale_intent(
            self, confirmed_generation):
        mgr = _make_manager()
        mgr._logical_controller_epoch = 'new-controller-epoch'
        mgr._is_pool = False
        retiring = self._ready_backend(1, 4)
        status = retiring.status_property
        status.is_scale_down = True
        status.sky_down_status = common_utils.ProcessStatus.SCHEDULED
        status.wait_for_idle_before_termination = True
        status.logical_retirement_version = 1
        status.logical_retirement_controller_epoch = 'old-controller-epoch'
        status.logical_retirement_generation = 4
        status.logical_retirement_target_capacity = 8
        status.logical_retirement_confirmed_generation = confirmed_generation
        mgr._wait_for_idle_trackers = {
            1: (mock.Mock(return_value=True),
                replica_managers.time.monotonic() + 60)
        }
        mgr._persist_replica = mock.Mock()
        mgr._terminate_replica = mock.Mock()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: retiring}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={'svc-1': ('UP', 1)}):
            mgr._refresh_wait_for_idle()

        mgr._terminate_replica.assert_not_called()
        assert not status.is_scale_down
        assert status.logical_retirement_confirmed_generation is None
        assert 1 not in mgr._wait_for_idle_trackers

    def test_pending_retirements_reserve_capacity_sequentially(self):
        mgr = _make_manager()
        mgr._uses_logical_replicas = True
        backends = [
            replica_managers.ReplicaInfo(replica_id=replica_id,
                                         cluster_name=f'svc-{replica_id}',
                                         replica_port='8080',
                                         is_spot=True,
                                         location=None,
                                         version=1,
                                         resources_override=None,
                                         planned_capacity=4)
            for replica_id in (1, 2, 3)
        ]
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=4,
                observed_slots_by_replica_id={},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        mgr._logical_target = (1, 4, 4)
        terminated = []

        def _terminate(replica_id, **_kwargs):
            terminated.append(replica_id)
            info = backends[replica_id - 1]
            info.status_property.is_scale_down = True
            info.status_property.sky_down_status = (
                common_utils.ProcessStatus.SCHEDULED)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_info_from_id',
                side_effect=lambda _service, replica_id: backends[
                    replica_id - 1]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=backends), \
             mock.patch.object(mgr,
                               '_terminate_replica',
                               side_effect=_terminate):
            for replica_id in (1, 2, 3):
                mgr.scale_down_logically(replica_id, 4, 1, 4)

        assert terminated == [1, 2]


class TestLaunchReplicaSnapshotAccumulation:
    """Bulk launches must preserve in-wave reserved-capacity accounting.

    Recovery re-drive passes a single existing_replica_infos snapshot
    across a whole wave of launches; without appending each newly placed
    replica, later launches can overbook the same zero-cost capacity.
    """

    def _make_manager(self, placer):
        # pylint: disable=protected-access
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager.yaml_content = 'dummy: yaml'
        manager.latest_version = 1
        manager._version_specs = {1: mock.Mock()}
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        placer.active_locations.return_value = []
        placer.ranked_active_locations.return_value = []
        placer.zero_cost_locations.return_value = []
        manager._spot_placer = placer
        return manager

    def test_wave_launches_do_not_feed_load_to_placer(self):
        # pylint: disable=protected-access
        placer = mock.Mock()
        location = mock.Mock()
        location.to_dict.return_value = {'zone': 'z'}
        placer.select_next_location.return_value = location
        manager = self._make_manager(placer)
        shared_snapshot = []

        def _fake_replica_info_ctor(replica_id, *_args, **_kwargs):
            info = mock.Mock()
            info.replica_id = replica_id
            info.replica_record_id = (
                f'00000000-0000-4000-8000-{replica_id:012d}')
            info.is_spot = True
            return info

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers.ReplicaInfo',
                        side_effect=_fake_replica_info_ctor), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state'
                 '.add_or_update_replica'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch(
                 'sky.serve.replica_managers._ReplicaLaunchThread') as thread:
            manager._launch_replica(replica_id=1,
                                    existing_replica_infos=shared_snapshot)
            manager._launch_replica(replica_id=2,
                                    existing_replica_infos=shared_snapshot)

        # The snapshot accumulated both newly placed replicas...
        assert len(shared_snapshot) == 2
        assert [info.replica_id for info in shared_snapshot] == [1, 2]
        # Placement is cheapest-first and independent of fleet load.
        assert placer.select_next_location.call_count == 2
        assert all(not call.args
                   for call in placer.select_next_location.call_args_list)
        assert thread.call_count == 2

    def test_fresh_scan_path_does_not_leak_appends(self):
        # pylint: disable=protected-access
        placer = mock.Mock()
        location = mock.Mock()
        location.to_dict.return_value = {'zone': 'z'}
        placer.select_next_location.return_value = location
        manager = self._make_manager(placer)

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos',
                 return_value=[]) as mock_scan, \
             mock.patch(
                 'sky.serve.replica_managers.serve_state'
                 '.add_or_update_replica'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            manager._launch_replica(replica_id=1)
        # Without a caller-provided snapshot each launch scans fresh state.
        assert mock_scan.call_count == 1

    def test_recovery_preserves_exact_spot_location(self):
        # A recovered spot row already owns its cluster name. Selecting a new
        # location would create a resource mismatch and overwrite the only
        # durable identity available to cleanup.
        placer = mock.Mock()
        manager = self._make_manager(placer)
        resources_override = {
            'cloud': 'AWS',
            'region': 'ap-northeast-1',
            'zone': 'ap-northeast-1a',
            'accelerators': {
                'L4': 1
            },
            'use_spot': True,
        }
        persisted = []
        prior_record_id = '11111111-1111-4111-8111-111111111111'
        prior_created_at = 123.5

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch.object(manager,
                               '_persist_replica',
                               side_effect=lambda _rid, info: persisted.append(
                                   info)), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            manager._launch_replica(replica_id=1463,
                                    resources_override=resources_override,
                                    existing_replica_infos=[],
                                    recovering_existing_replica=True,
                                    prior_replica_record_id=prior_record_id,
                                    prior_created_at=prior_created_at,
                                    prior_version=1,
                                    prior_yaml_content='resources: {}')

        placer.select_next_location.assert_not_called()
        placer.select_next_zero_cost_location.assert_not_called()
        assert len(persisted) == 1
        assert persisted[0].resources_override == resources_override
        assert persisted[0].get_spot_location() == (
            replica_managers.spot_placer.Location.from_resources_override(
                resources_override))
        assert persisted[0].replica_record_id == prior_record_id
        assert persisted[0].created_at == prior_created_at

    def test_logical_recovery_preserves_persisted_capacity(self):
        placer = mock.Mock()
        manager = self._make_manager(placer)
        manager._uses_logical_replicas = True
        manager._default_planned_capacity = 1
        manager._version_specs[7] = mock.Mock()
        resources_override = {
            'cloud': 'AWS',
            'region': 'us-east-1',
            'zone': 'us-east-1a',
            'accelerators': {
                'L4': 1
            },
            'use_spot': True,
        }
        persisted = []

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch.object(
                 manager,
                 '_persist_replica',
                 side_effect=lambda _rid, info: persisted.append(info)), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            manager._launch_replica(replica_id=1,
                                    resources_override=resources_override,
                                    existing_replica_infos=[],
                                    recovering_existing_replica=True,
                                    prior_planned_capacity=8,
                                    prior_version=7,
                                    prior_yaml_content='resources: {}')

        assert len(persisted) == 1
        assert persisted[0].planned_capacity == 8
        assert persisted[0].version == 7

    def test_recovery_uses_original_version_yaml_during_unit_transition(self):
        placer = mock.Mock()
        manager = self._make_manager(placer)
        manager.latest_version = 8
        manager.yaml_content = 'resources:\n  accelerators: A100:8\n'
        manager._uses_logical_replicas = True
        prior_spec = mock.Mock()
        manager._version_specs[7] = prior_spec
        persisted = []
        thread = mock.Mock()

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=False), \
             mock.patch.object(
                 manager,
                 '_persist_replica',
                 side_effect=lambda _rid, info: persisted.append(info)), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils'
                 '.generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080') as get_ports, \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread',
                        return_value=thread) as safe_thread:
            manager._launch_replica(
                replica_id=1,
                resources_override={'accelerators': {
                    'L4': 1
                }},
                existing_replica_infos=[],
                recovering_existing_replica=True,
                prior_planned_capacity=1,
                prior_version=7,
                prior_yaml_content='resources:\n  accelerators: L4:1\n')

        assert len(persisted) == 1
        assert persisted[0].version == 7
        assert persisted[0].planned_capacity == 1
        assert safe_thread.call_args.kwargs['args'][1] == (
            'resources:\n  accelerators: L4:1\n')
        get_ports.assert_called_once_with('resources:\n  accelerators: L4:1\n',
                                          prior_spec)


class TestFailedCleanupReconciliation:

    @staticmethod
    def _info(replica_id=1, version=1):
        info = replica_managers.ReplicaInfo(replica_id, f'svc-{replica_id}',
                                            '8080', False, None, version, None)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        return info

    @pytest.mark.parametrize('terminal_kind', [
        'scale_down',
        'purge',
        'preempted',
        'outdated',
        'spot_availability',
    ])
    def test_failed_down_never_removes_durable_row(self, terminal_kind):
        manager = _make_manager()
        info = self._info(version=0 if terminal_kind == 'outdated' else 1)
        if terminal_kind == 'scale_down':
            info.status_property.is_scale_down = True
        elif terminal_kind == 'purge':
            info.status_property.purged = True
        elif terminal_kind == 'preempted':
            info.status_property.preempted = True
        elif terminal_kind == 'spot_availability':
            info.status_property.failed_spot_availability = True

        with mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_remove_replica') as remove, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        return_value=100):
            manager._handle_sky_down_finish(info, 'provider error')

        persist.assert_called_once_with(1, info)
        remove.assert_not_called()
        assert (info.status_property.sky_down_status ==
                common_utils.ProcessStatus.FAILED)
        assert manager._failed_cleanup_retry_attempts == {1: 1}
        assert manager._failed_cleanup_retry_at == {1: 160}

    def test_raw_preempted_down_failure_is_reconciled(self):
        # PREEMPTED intentionally wins derived status, so retry eligibility
        # must also inspect the raw failed-down field.
        manager = _make_manager()
        info = self._info()
        info.status_property.preempted = True
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
        assert info.status == replica_managers.serve_state.ReplicaStatus.PREEMPTED

        with mock.patch.object(manager, '_terminate_replica') as terminate, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        return_value=100):
            manager._reconcile_failed_cleanup([info])

        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True,
                                          purge=False,
                                          in_flight_drain_cap_seconds=0)

    def test_cleanup_retry_wave_schedules_v2_before_ordinary(self):
        manager = _make_manager()
        ordinary = self._info(1)
        ordinary.status_property.sky_down_status = (
            common_utils.ProcessStatus.FAILED)
        fenced = _stamp_protocol_v2_fill(self._info(2))
        fenced.status_property.sky_down_status = (
            common_utils.ProcessStatus.FAILED)

        with mock.patch.object(manager, '_terminate_replica') as terminate, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        return_value=100):
            manager._reconcile_failed_cleanup([ordinary, fenced])

        assert [call.args[0] for call in terminate.call_args_list] == [2, 1]

    def test_provider_failure_does_not_repeat_consumed_drain(self):
        manager = _make_manager()
        info = self._info()
        info.status_property.is_scale_down = True
        info.status_property.drain_cap_seconds = 600
        info.status_property.drain_started_at = 10.0
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED

        with mock.patch.object(manager, '_terminate_replica') as terminate, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        return_value=100):
            manager._reconcile_failed_cleanup([info])

        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True,
                                          purge=False,
                                          in_flight_drain_cap_seconds=0)

    def test_legacy_failed_row_is_rejected_before_cleanup(self):
        info = self._info()
        info.status_property.is_scale_down = True
        info.status_property.drain_cap_seconds = 600
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
        legacy_state = info.to_storage_dict()
        legacy_state['replica_info_version'] = 13
        legacy_state['status_property'].pop('drain_started_at')

        with pytest.raises(ValueError, match='invalid top-level shape'):
            replica_managers.ReplicaInfo.from_storage_dict(legacy_state)

    def test_cleanup_retry_respects_capped_backoff_deadline(self):
        manager = _make_manager()
        info = self._info()
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
        manager._failed_cleanup_retry_at[1] = 200

        with mock.patch.object(manager, '_terminate_replica') as terminate, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        side_effect=[199, 200]):
            manager._reconcile_failed_cleanup([info])
            terminate.assert_not_called()
            manager._reconcile_failed_cleanup([info])

        terminate.assert_called_once()

    def test_cleanup_retry_delay_is_capped(self):
        manager = _make_manager()
        manager._failed_cleanup_retry_attempts[1] = 100

        with mock.patch('sky.serve.replica_managers.time.monotonic',
                        return_value=100):
            manager._schedule_failed_cleanup_retry(1)

        assert manager._failed_cleanup_retry_attempts == {1: 101}
        assert manager._failed_cleanup_retry_at == {
            1: 100 + replica_managers._FAILED_CLEANUP_RETRY_MAX_SECONDS
        }

    def test_successful_absent_cleanup_clears_retry_and_removes_old_row(self):
        manager = _make_manager()
        info = self._info(version=0)
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED
        manager._failed_cleanup_retry_attempts[1] = 3
        manager._failed_cleanup_retry_at[1] = 500

        with mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch.object(manager, '_remove_replica') as remove:
            manager._handle_sky_down_finish(info, format_exc=None)

        remove.assert_called_once_with(1, info.replica_record_id)
        persist.assert_not_called()
        assert not manager._failed_cleanup_retry_attempts
        assert not manager._failed_cleanup_retry_at

    def test_synchronous_reconcile_error_is_backed_off(self):
        manager = _make_manager()
        info = self._info()
        info.status_property.sky_down_status = common_utils.ProcessStatus.FAILED

        with mock.patch.object(manager,
                               '_terminate_replica',
                               side_effect=RuntimeError('database error')), \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        side_effect=[100, 100]):
            manager._reconcile_failed_cleanup([info])

        assert manager._failed_cleanup_retry_attempts == {1: 1}
        assert manager._failed_cleanup_retry_at == {1: 160}

    def test_finished_down_worker_survives_completion_persist_error(self):
        manager = _make_manager()
        manager._is_pool = False
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        down_thread = mock.Mock()
        down_thread.is_alive.return_value = False
        down_thread.format_exc = 'provider error'
        manager._down_thread_pool[1] = down_thread
        info = self._info()
        info.status_property.sky_down_status = common_utils.ProcessStatus.RUNNING

        with mock.patch.object(manager, '_refresh_wait_for_idle'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _service, ids:
                               ({1: info} if ids else {})), \
             mock.patch.object(manager,
                               '_persist_replica',
                               side_effect=RuntimeError('database error')), \
             pytest.raises(RuntimeError, match='database error'):
            manager._refresh_thread_pool()

        assert manager._down_thread_pool[1] is down_thread

    @pytest.mark.parametrize('server_committed', [False, True])
    def test_ambiguous_down_admission_write_replaces_unstarted_worker(
            self, tmp_path, server_committed):
        manager, retiring, survivor = (
            TestLogicalCapacityPlanning()._pending_logical_retirement())
        manager._terminate_replica = types.MethodType(
            replica_managers.SkyPilotReplicaManager._terminate_replica, manager)
        manager._resource_scope = None
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        retiring.status_property.wait_for_idle_before_termination = False
        retiring.status_property.logical_retirement_confirmed_generation = 5
        original_thread = mock.Mock()
        original_thread.is_alive.return_value = False
        original_thread.format_exc = None
        fresh_thread = mock.Mock()
        fresh_thread.is_alive.return_value = False
        fresh_thread.format_exc = None
        manager._down_thread_pool[9] = original_thread
        durable = {
            9: replica_managers.ReplicaInfo.from_storage_dict(
                retiring.to_storage_dict())
        }
        persist_calls = [0]

        def _persist(replica_id, info):
            persist_calls[0] += 1
            if persist_calls[0] == 1:
                if server_committed:
                    durable[replica_id] = (
                        replica_managers.ReplicaInfo.from_storage_dict(
                            info.to_storage_dict()))
                raise RuntimeError('ambiguous database write')
            durable[replica_id] = (
                replica_managers.ReplicaInfo.from_storage_dict(
                    info.to_storage_dict()))

        with mock.patch.object(manager, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 manager, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=lambda _service, ids:
                               ({9: retiring} if ids else {})), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               side_effect=lambda _service, replica_id:
                               durable.get(replica_id)), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_info_with_resource_action_identity',
                 side_effect=lambda _service, replica_id:
                 ((info, None) if (info := durable.get(replica_id)) is not None
                  else None)), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[retiring, survivor]), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value=str(tmp_path / 'replica.log')), \
             mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=fresh_thread), \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils, 'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True), \
             mock.patch.object(manager,
                               '_persist_replica',
                               side_effect=_persist), \
             pytest.raises(RuntimeError, match='ambiguous database write'):
            manager._refresh_thread_pool()

        original_thread.start.assert_not_called()
        assert manager._down_thread_pool[9] is fresh_thread
        assert (durable[9].status_property.sky_down_status ==
                common_utils.ProcessStatus.SCHEDULED)
        if server_committed:
            assert durable[9].status_property.logical_retirement_version is None
        else:
            assert durable[9].status_property.logical_retirement_version == 10
            assert not durable[9].status_property.logical_retirement_committed

    @pytest.mark.parametrize('failed_state_persist_raises', [False, True])
    def test_down_worker_start_failure_retries_committed_cleanup(
            self, tmp_path, failed_state_persist_raises):
        manager, retiring, survivor = (
            TestLogicalCapacityPlanning()._pending_logical_retirement())
        manager._terminate_replica = types.MethodType(
            replica_managers.SkyPilotReplicaManager._terminate_replica, manager)
        manager._resource_scope = None
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        retiring.status_property.wait_for_idle_before_termination = False
        retiring.status_property.logical_retirement_confirmed_generation = 5
        retiring.status_property.drain_cap_seconds = 600
        retiring.status_property.drain_started_at = 10.0
        original_thread = mock.Mock()
        original_thread.is_alive.return_value = False
        original_thread.format_exc = None
        original_thread.start.side_effect = RuntimeError('thread start failed')
        fresh_thread = mock.Mock()
        fresh_thread.is_alive.return_value = False
        fresh_thread.format_exc = None
        manager._down_thread_pool[9] = original_thread
        manager._wait_for_idle_trackers[9] = (None, 999)
        durable = {
            9: replica_managers.ReplicaInfo.from_storage_dict(
                retiring.to_storage_dict())
        }
        clock = [100]
        failed_persist_attempted = [False]

        def _clone(info):
            return replica_managers.ReplicaInfo.from_storage_dict(
                info.to_storage_dict())

        def _persist(replica_id, info):
            if (failed_state_persist_raises and
                    not failed_persist_attempted[0] and
                    info.status_property.sky_down_status
                    == common_utils.ProcessStatus.SCHEDULED):
                failed_persist_attempted[0] = True
                raise RuntimeError('failed-state database write')
            durable[replica_id] = _clone(info)

        def _read_many(_service, replica_ids):
            return {
                replica_id: _clone(durable[replica_id])
                for replica_id in replica_ids
                if replica_id in durable
            }

        def _read_all(_service):
            return [_clone(durable[9]), _clone(survivor)]

        with mock.patch.object(
                manager, '_reconcile_legacy_uncertain_logical_retirements'), \
             mock.patch.object(manager, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 manager, '_clear_known_unknown_capacity_replacements'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               side_effect=_read_many), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               side_effect=lambda _service, replica_id:
                               _clone(durable[replica_id])), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_info_with_resource_action_identity',
                 side_effect=lambda _service, replica_id:
                 (_clone(durable[replica_id]), None)), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               side_effect=_read_all), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value=str(tmp_path / 'replica.log')), \
             mock.patch.object(replica_managers.thread_utils,
                               'SafeThread',
                               return_value=fresh_thread) as safe_thread_factory, \
             mock.patch.object(controller_utils, 'get_resources_lock_path',
                               return_value=str(tmp_path / 'resources.lock')), \
             mock.patch.object(controller_utils,
                               'in_flight_launch_count',
                               return_value=0), \
             mock.patch.object(controller_utils,
                               'can_terminate',
                               return_value=True), \
             mock.patch.object(manager,
                               '_persist_replica',
                               side_effect=_persist), \
             mock.patch.object(manager, '_remove_replica') as remove, \
             mock.patch('sky.serve.replica_managers.time.monotonic',
                        side_effect=lambda: clock[0]), \
             mock.patch('sky.serve.replica_managers.time.time',
                        side_effect=lambda: clock[0]):
            if failed_state_persist_raises:
                with pytest.raises(RuntimeError,
                                   match='failed-state database write'):
                    manager._refresh_thread_pool()
            else:
                manager._refresh_thread_pool()

            original_thread.start.assert_called_once_with()
            assert 9 not in manager._down_thread_pool
            assert 9 not in manager._wait_for_idle_trackers
            assert manager._failed_cleanup_retry_attempts == {9: 1}
            assert manager._failed_cleanup_retry_at == {9: 160}
            expected_durable_status = (common_utils.ProcessStatus.RUNNING
                                       if failed_state_persist_raises else
                                       common_utils.ProcessStatus.SCHEDULED)
            assert (durable[9].status_property.sky_down_status ==
                    expected_durable_status)
            assert durable[9].status_property.logical_retirement_committed
            assert not durable[9].is_ready
            remove.assert_not_called()

            # Once the retry deadline arrives, the durable commitment is
            # detached from the obsolete selection epoch and a new,
            # idempotent cleanup worker is installed. It is admitted on the
            # next tick without ever making the backend READY again.
            clock[0] = 160
            manager._refresh_thread_pool()
            assert manager._down_thread_pool[9] is fresh_thread
            assert (durable[9].status_property.sky_down_status ==
                    common_utils.ProcessStatus.SCHEDULED)
            assert (durable[9].status_property.logical_retirement_version
                    is None)
            assert not durable[9].is_ready
            assert manager._down_thread_pool[9] is fresh_thread
            assert (safe_thread_factory.call_args.kwargs['kwargs']
                    ['drain_deadline'] == 610)

            manager._refresh_thread_pool()

        fresh_thread.start.assert_called_once_with()
        assert (durable[9].status_property.sky_down_status ==
                common_utils.ProcessStatus.RUNNING)
        assert not durable[9].is_ready
        assert manager._down_thread_pool[9] is fresh_thread
        remove.assert_not_called()

    def test_log_sync_failure_does_not_block_cleanup(self):
        manager = _make_manager()
        manager._is_pool = False
        manager._resource_scope = None
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        info = self._info()

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_info_with_resource_action_identity',
                 return_value=(info, None)), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value='/tmp/replica.log'), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_launch_log_file_name',
                               return_value='/tmp/launch.log'), \
             mock.patch('sky.serve.replica_managers.os.path.exists',
                        return_value=True), \
             mock.patch('builtins.open', side_effect=OSError('disk error')), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(manager, '_persist_replica') as persist, \
             mock.patch('sky.serve.replica_managers.thread_utils.SafeThread',
                        return_value=mock.Mock()) as safe_thread:
            manager._terminate_replica(1,
                                       sync_down_logs=True,
                                       replica_drain_delay_seconds=0)

        persist.assert_called_once_with(1, info)
        safe_thread.assert_called_once()
        assert 1 in manager._down_thread_pool
        assert (info.status_property.sky_down_status ==
                common_utils.ProcessStatus.SCHEDULED)

    def test_down_worker_guard_allows_shutdown_but_rejects_new_owner(self):
        manager = _make_manager()
        manager._is_pool = False
        manager._resource_scope = None
        manager._service_hash = 'service-hash-a'
        manager._controller_owner = (123, '10.0.0.1')
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        info = self._info()
        same_owner = {
            'hash': 'service-hash-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'status': replica_managers.serve_state.ServiceStatus.SHUTTING_DOWN,
        }
        replacement_owner = {
            **same_owner,
            'controller_pid': 456,
            'controller_ip': '10.0.0.2',
        }

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_info_with_resource_action_identity',
                 return_value=(info, None)), \
             mock.patch.object(replica_managers.serve_utils,
                               'generate_replica_log_file_name',
                               return_value='/tmp/replica.log'), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(manager, '_persist_replica'), \
             mock.patch('sky.serve.replica_managers.thread_utils.SafeThread',
                        return_value=mock.Mock()) as safe_thread:
            manager._terminate_replica(1,
                                       sync_down_logs=False,
                                       replica_drain_delay_seconds=0,
                                       is_scale_down=True)

        guard = safe_thread.call_args.kwargs['kwargs']['continue_guard']
        with mock.patch.object(replica_managers.serve_state,
                               'get_service_controller_owner',
                               side_effect=[same_owner, replacement_owner]):
            assert guard() is True
            assert guard() is False


class TestPaidLocationLaunchBudget:

    @staticmethod
    def _manager(costs):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager.yaml_content = 'resources:\n  use_spot: true\n'
        manager.latest_version = 1
        manager._version_specs = {1: mock.Mock()}
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        manager._spot_placer = make_placer(costs)
        manager._spot_placer.num_nodes = 1
        manager._workspace = 'default'
        manager._service_hash = None
        manager._controller_owner = None
        return manager

    @staticmethod
    def _info(replica_id, location, status):
        info = replica_managers.ReplicaInfo(
            replica_id=replica_id,
            cluster_name=f'svc-{replica_id}',
            replica_port='8080',
            is_spot=True,
            location=location,
            version=1,
            resources_override=location.to_dict())
        if status == replica_managers.serve_state.ReplicaStatus.PROVISIONING:
            info.status_property.sky_launch_status = (
                common_utils.ProcessStatus.RUNNING)
        elif status == replica_managers.serve_state.ReplicaStatus.STARTING:
            info.status_property.sky_launch_status = (
                common_utils.ProcessStatus.SUCCEEDED)
        elif status == replica_managers.serve_state.ReplicaStatus.READY:
            info.status_property.sky_launch_status = (
                common_utils.ProcessStatus.SUCCEEDED)
            info.status_property.service_ready_now = True
        else:
            assert status == replica_managers.serve_state.ReplicaStatus.PENDING
        assert info.status == status
        return info

    @pytest.mark.parametrize('is_zero_cost', [False, True])
    def test_accepted_launch_reports_explicit_funding(self, is_zero_cost):
        location = make_location(
            'research' if is_zero_cost else 'us-east-1', {'L4': 4},
            use_spot=not is_zero_cost,
            cloud_name='Kubernetes' if is_zero_cost else 'AWS')
        manager = self._manager({location: 0.0 if is_zero_cost else 1.0})
        manager._next_replica_id = 7
        manager._uses_logical_replicas = True
        manager._default_planned_capacity = 4
        manager._logical_exact_accelerator_shapes = {}
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        manager._persist_new_replica = mock.Mock()
        existing = []

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            result = manager._scale_up_one_locked(
                {'accelerators': {
                    'L4': 4
                }},
                set(),
                existing,
                paid_launch_allowed=not is_zero_cost)

        expected_funding = (replica_managers._ReplicaLaunchFunding.ZERO_COST
                            if is_zero_cost else
                            replica_managers._ReplicaLaunchFunding.PAID)
        assert result == _accepted_launch_result(7, 4, expected_funding)
        assert manager._next_replica_id == 8
        assert len(existing) == 1
        assert existing[0].is_zero_cost is is_zero_cost

    def test_launch_without_paid_authority_rejects_paid_only_location(self):
        paid = make_location('us-east-1', {'L4': 4}, cloud_name='AWS')
        manager = self._manager({paid: 1.0})
        manager._next_replica_id = 7
        manager._uses_logical_replicas = True
        manager._default_planned_capacity = 4
        manager._logical_exact_accelerator_shapes = {}
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        manager._persist_new_replica = mock.Mock()
        existing = []

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True):
            result = manager._scale_up_one_locked({'accelerators': {
                'L4': 4
            }},
                                                  set(),
                                                  existing,
                                                  paid_launch_allowed=False)

        assert result is None
        assert manager._next_replica_id == 7
        assert not existing
        manager._persist_new_replica.assert_not_called()

    def test_paid_launch_binds_exact_planner_tuple_before_persistence(self):
        paid = make_location('us-east-1', {'L4': 4}, cloud_name='AWS')
        manager = self._manager({paid: 1.0})
        manager._next_replica_id = 7
        manager._uses_logical_replicas = True
        manager._default_planned_capacity = 4
        manager._logical_exact_accelerator_shapes = {}
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        manager._persist_new_replica = mock.Mock()
        authority = capacity_admission.PaidLaunchAuthority(
            service_name='svc',
            service_hash='svc-hash',
            generation=8,
            content_sha256='a' * 64,
            demand_feed_generation=9,
            demand_source_epoch=3,
            paid_residual_by_accelerator=(('l4', 4),))

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'), \
             mock.patch.object(
                 paid_capacity,
                 'try_persist_claim',
                 return_value=paid_capacity.ClaimResult.ACQUIRED) as persist:
            result = manager._scale_up_one_locked(
                {'accelerators': {
                    'L4': 4
                }},
                set(), [],
                paid_launch_authority=authority)

        assert result == _accepted_launch_result(
            7, 4, replica_managers._ReplicaLaunchFunding.PAID)
        assert persist.call_args.kwargs['capacity_plan_claim'] == {
            'capacity_plan_generation': 8,
            'capacity_plan_sha256': 'a' * 64,
            'demand_feed_generation': 9,
            'demand_source_epoch': 3,
            'capacity_plan_accelerator': 'l4',
            'capacity_plan_units': 4,
        }

    def test_paid_plan_race_releases_selection_without_row_or_thread(self):
        paid = make_location('us-east-1', {'L4': 4}, cloud_name='AWS')
        manager = self._manager({paid: 1.0})
        manager._next_replica_id = 7
        manager._uses_logical_replicas = True
        manager._default_planned_capacity = 4
        manager._logical_exact_accelerator_shapes = {}
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        manager._persist_new_replica = mock.Mock()
        authority = capacity_admission.PaidLaunchAuthority(
            service_name='svc',
            service_hash='svc-hash',
            generation=8,
            content_sha256='a' * 64,
            demand_feed_generation=9,
            demand_source_epoch=3,
            paid_residual_by_accelerator=(('l4', 1),))

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'), \
             mock.patch.object(paid_capacity,
                               'try_persist_claim') as persist, \
             mock.patch.object(
                 manager, '_release_unstarted_location_retry') as release:
            result = manager._scale_up_one_locked(
                {'accelerators': {
                    'L4': 4
                }},
                set(), [],
                paid_launch_authority=authority)

        assert result is None
        release.assert_called_once_with(paid)
        persist.assert_not_called()
        manager._persist_new_replica.assert_not_called()

    def test_env_override_and_invalid_fallback(self, monkeypatch):
        paid_capacity._parse_positive_int.cache_clear()
        monkeypatch.delenv(paid_capacity._BASE_LIMIT_ENV_VAR, raising=False)
        assert paid_capacity.base_limit() == 4

        monkeypatch.setenv(paid_capacity._BASE_LIMIT_ENV_VAR, '7')
        assert paid_capacity.base_limit() == 7

        monkeypatch.setenv(paid_capacity._BASE_LIMIT_ENV_VAR, '0')
        assert paid_capacity.base_limit() == 4
        paid_capacity._parse_positive_int.cache_clear()

    def test_only_pending_and_provisioning_rows_consume_window(self):
        cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        other = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
        zero = make_location('research', {'L4': 1},
                             use_spot=False,
                             cloud_name='Kubernetes')
        manager = self._manager({cheap: 1.0, other: 2.0, zero: 0.0})
        infos = [
            self._info(1, cheap,
                       replica_managers.serve_state.ReplicaStatus.PENDING),
            self._info(2, cheap,
                       replica_managers.serve_state.ReplicaStatus.PROVISIONING),
            self._info(3, other,
                       replica_managers.serve_state.ReplicaStatus.STARTING),
            self._info(4, other,
                       replica_managers.serve_state.ReplicaStatus.READY),
        ]

        with mock.patch.object(paid_capacity,
                               'legacy_local_limit',
                               return_value=2):
            budget = paid_capacity.build_launch_budget(
                manager._spot_placer,
                workspace='default',
                existing_replica_infos=infos,
                globally_managed=False)

        assert budget.remaining_by_location == {cheap: 0, other: 2}

    def test_cheapest_location_fills_cohort_then_spills(self):
        cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({cheap: 1.0, expensive: 2.0})
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        persisted = []
        manager._persist_new_replica = mock.Mock(
            side_effect=lambda _replica_id, info: persisted.append(info))

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(paid_capacity,
                               'legacy_local_limit',
                               return_value=2), \
             mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            manager._scale_up_batch_locked([{'use_spot': True}] * 5)

        assert [info.get_spot_location().region for info in persisted
               ] == ['us-east-1', 'us-east-1', 'us-west-2', 'us-west-2']
        assert manager._next_replica_id == 5
        assert len(manager._launch_thread_pool) == 4
        assert all(replica_id in manager._launch_thread_pool
                   for replica_id in (1, 2, 3, 4))

    def test_default_window_spills_after_four_unresolved_launches(
            self, monkeypatch):
        monkeypatch.delenv(paid_capacity._BASE_LIMIT_ENV_VAR, raising=False)
        paid_capacity._parse_positive_int.cache_clear()
        cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({cheap: 1.0, expensive: 2.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        budget = paid_capacity.LaunchBudget(remaining_by_location={
            cheap: 4,
            expensive: 4
        },
                                            pool_key_by_location={
                                                cheap: 'cheap',
                                                expensive: 'expensive'
                                            },
                                            states_by_pool_key={},
                                            globally_managed=True)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(paid_capacity,
                               'build_launch_budget',
                               return_value=budget), \
             mock.patch.object(
                 paid_capacity,
                 'try_persist_claim',
                 return_value=paid_capacity.ClaimResult.ACQUIRED) as claim, \
             mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            manager._scale_up_batch_locked([{'use_spot': True}] * 5)

        assert [
            call.kwargs['location'].region for call in claim.call_args_list
        ] == ['us-east-1'] * 4 + ['us-west-2']

    def test_global_frontier_caps_cold_400_wave_at_two_pools(self):
        primary = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        hedge = make_location('us-west-2', {'L4': 4}, cloud_name='AWS')
        third = make_location('eu-west-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({primary: 1.0, hedge: 2.0, third: 3.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        keys = {
            location: paid_capacity.pool_key(
                location, workspace='default',
                num_nodes=1) for location in (primary, hedge, third)
        }
        budget = paid_capacity.LaunchBudget(
            remaining_by_location={
                primary: 4,
                hedge: 4,
                third: 4,
            },
            pool_key_by_location=keys,
            states_by_pool_key={},
            globally_managed=True,
            frontier_limit=2,
            frontier_key_by_location={
                location: paid_capacity.frontier_key(location)
                for location in keys
            })

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(paid_capacity,
                               'build_launch_budget',
                               return_value=budget), \
             mock.patch.object(
                 paid_capacity,
                 'try_persist_claim',
                 return_value=paid_capacity.ClaimResult.ACQUIRED) as claim, \
             mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            manager._scale_up_batch_locked([{'use_spot': True}] * 400)

        claimed_locations = [
            call.kwargs['location'] for call in claim.call_args_list
        ]
        assert claimed_locations[:4] == [primary] * 4
        assert len(claimed_locations) == 8
        assert len(set(claimed_locations)) == 2
        assert len(set(claimed_locations) & {hedge, third}) == 1
        assert budget.feedback_deferred_frontiers == {('l4',)}
        assert manager._next_replica_id == 9
        assert len(manager._launch_thread_pool) == 8

    def test_aged_full_frontier_opens_only_one_target_backed_third_pool(self):
        primary = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        hedge = make_location('us-west-2', {'L4': 4}, cloud_name='AWS')
        third = make_location('eu-west-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({primary: 1.0, hedge: 2.0, third: 3.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        locations = (primary, hedge, third)
        keys = {
            location: paid_capacity.pool_key(
                location, workspace='default',
                num_nodes=1) for location in locations
        }
        budget = paid_capacity.LaunchBudget(
            remaining_by_location={
                primary: 0,
                hedge: 0,
                third: 4
            },
            pool_key_by_location=keys,
            states_by_pool_key={},
            globally_managed=True,
            service_remaining=8,
            frontier_limit=2,
            max_frontier_limit=3,
            frontier_feedback_delay_seconds=30,
            frontier_key_by_location={
                location: ('l4',) for location in locations
            },
            failure_domain_by_location={
                location: paid_capacity.failure_domain(location)
                for location in locations
            },
            owned_pool_keys_by_frontier={('l4',): {keys[primary], keys[hedge]}},
            newest_claimed_at_by_pool_key={
                keys[primary]: 0,
                keys[hedge]: 0,
            })

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(paid_capacity,
                               'build_launch_budget',
                               return_value=budget), \
             mock.patch.object(
                 paid_capacity,
                 'try_persist_claim',
                 return_value=paid_capacity.ClaimResult.ACQUIRED) as claim, \
             mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            manager._scale_up_batch_locked([{'use_spot': True}] * 400)

        assert [call.kwargs['location'] for call in claim.call_args_list
               ] == [third] * 4
        assert budget.frontier_limit_overrides == {('l4',): 3}
        assert budget.service_remaining == 4
        assert manager._next_replica_id == 5
        assert len(manager._launch_thread_pool) == 4

    def test_atomic_frontier_rejection_persists_nothing(self):
        primary = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({primary: 1.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        key = paid_capacity.pool_key(primary, workspace='default', num_nodes=1)
        budget = paid_capacity.LaunchBudget(
            remaining_by_location={primary: 4},
            pool_key_by_location={primary: key},
            states_by_pool_key={},
            globally_managed=True,
            frontier_limit=2,
            frontier_key_by_location={primary: ('l4',)})

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(paid_capacity,
                               'build_launch_budget',
                               return_value=budget), \
             mock.patch.object(
                 paid_capacity,
                 'try_persist_claim',
                 return_value=paid_capacity.ClaimResult.FEEDBACK_PENDING), \
             mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            manager._scale_up_batch_locked([{'use_spot': True}] * 400)

        assert budget.feedback_deferred_frontiers == {('l4',)}
        assert budget.remaining_by_location == {primary: 4}
        assert manager._next_replica_id == 1
        assert not manager._launch_thread_pool

    def test_exact_card_subsets_keep_independent_paid_windows(self):
        cheap_l4 = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        expensive_l4 = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
        a100 = make_location('eu-west-1', {'A100': 1}, cloud_name='AWS')
        manager = self._manager({
            cheap_l4: 1.0,
            expensive_l4: 2.0,
            a100: 3.0,
        })
        budget = paid_capacity.LaunchBudget(
            {
                cheap_l4: 1,
                expensive_l4: 1,
                a100: 1,
            }, {}, {}, False)
        l4_locations = {cheap_l4, expensive_l4}

        first_l4 = paid_capacity.select_location(manager._spot_placer,
                                                 budget,
                                                 allowed_locations=l4_locations)
        paid_capacity.debit(budget, first_l4)
        second_l4 = paid_capacity.select_location(
            manager._spot_placer, budget, allowed_locations=l4_locations)
        selected_a100 = paid_capacity.select_location(manager._spot_placer,
                                                      budget,
                                                      allowed_locations={a100})

        assert (first_l4, second_l4, selected_a100) == (cheap_l4, expensive_l4,
                                                        a100)

    def test_saturated_wave_persists_nothing(self):
        cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({cheap: 1.0, expensive: 2.0})
        unresolved = [
            self._info(1, cheap,
                       replica_managers.serve_state.ReplicaStatus.PENDING),
            self._info(2, expensive,
                       replica_managers.serve_state.ReplicaStatus.PROVISIONING),
        ]
        manager._next_replica_id = 3
        manager._persist_new_replica = mock.Mock()
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)

        with mock.patch.object(paid_capacity,
                               'legacy_local_limit',
                               return_value=1), \
             mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'
                       ) as safe_thread:
            budget = paid_capacity.build_launch_budget(
                manager._spot_placer,
                workspace='default',
                existing_replica_infos=unresolved,
                globally_managed=False)
            launched = manager._scale_up_one_locked(
                None, {1, 2}, unresolved, paid_location_launch_budget=budget)

        assert not launched
        assert len(unresolved) == 2
        assert manager._next_replica_id == 3
        manager._persist_new_replica.assert_not_called()
        safe_thread.assert_not_called()

    def test_globally_saturated_pool_spills_to_next_paid_pool(self):
        cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({cheap: 1.0, expensive: 2.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        budget = paid_capacity.LaunchBudget(remaining_by_location={
            cheap: 60,
            expensive: 60
        },
                                            pool_key_by_location={
                                                cheap: 'cheap',
                                                expensive: 'expensive'
                                            },
                                            states_by_pool_key={},
                                            globally_managed=True)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(paid_capacity,
                               'build_launch_budget',
                               return_value=budget), \
             mock.patch.object(
                 paid_capacity,
                 'select_location',
                 wraps=paid_capacity.select_location) as select_location, \
             mock.patch.object(
                 paid_capacity,
                 'try_persist_claim',
                 side_effect=[
                     paid_capacity.ClaimResult.SATURATED,
                     paid_capacity.ClaimResult.ACQUIRED,
                 ]) as claim, \
             mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            manager._scale_up_batch_locked([{
                'use_spot': True
            }] * 2,
                                           launch_priority=20)

        assert [call.kwargs['location'] for call in claim.call_args_list
               ] == [cheap, expensive]
        assert [call.kwargs['priority'] for call in claim.call_args_list
               ] == [20, 20]
        assert select_location.call_count == 2
        assert budget.remaining_by_location[cheap] == 0
        assert manager._next_replica_id == 2
        assert len(manager._launch_thread_pool) == 1

    def test_authoritative_service_saturation_stops_paid_wave(self):
        cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({cheap: 1.0, expensive: 2.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        budget = paid_capacity.LaunchBudget(remaining_by_location={
            cheap: 4,
            expensive: 4,
        },
                                            pool_key_by_location={
                                                cheap: 'cheap',
                                                expensive: 'expensive',
                                            },
                                            states_by_pool_key={},
                                            globally_managed=True,
                                            service_remaining=1)

        with mock.patch.object(
                paid_capacity,
                'try_persist_claim',
                return_value=paid_capacity.ClaimResult.
                SERVICE_SATURATED) as claim, mock.patch(
                    'sky.serve.replica_managers._should_use_spot',
                    return_value=True), mock.patch(
                        'sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), mock.patch(
                            'sky.serve.replica_managers._ReplicaLaunchThread'
                        ) as launch_thread:
            launched = manager._scale_up_one_locked(
                None,
                set(), [],
                paid_location_launch_budget=budget,
                launch_priority=20)

        assert not launched
        assert claim.call_count == 1
        assert budget.service_remaining == 0
        assert paid_capacity.select_location(manager._spot_placer,
                                             budget) is None
        assert manager._next_replica_id == 1
        assert not manager._launch_thread_pool
        # Launch workers are now materialized only after admission and the
        # optional durable recovery-candidate transition both succeed.
        launch_thread.assert_not_called()

    def test_service_envelope_stops_large_physical_wave(self):
        cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({cheap: 1.0, expensive: 2.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        budget = paid_capacity.LaunchBudget(remaining_by_location={
            cheap: 4,
            expensive: 4,
        },
                                            pool_key_by_location={
                                                cheap: 'cheap',
                                                expensive: 'expensive',
                                            },
                                            states_by_pool_key={},
                                            globally_managed=True,
                                            service_remaining=2)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), mock.patch.object(
                                   paid_capacity,
                                   'build_launch_budget',
                                   return_value=budget), mock.patch.object(
                                       paid_capacity,
                                       'try_persist_claim',
                                       return_value=paid_capacity.ClaimResult.
                                       ACQUIRED) as claim, mock.patch(
                                           'sky.serve.replica_managers.'
                                           '_should_use_spot',
                                           return_value=True), mock.patch(
                                               'sky.serve.replica_managers.'
                                               '_get_resources_ports',
                                               return_value='8080'), mock.patch(
                                                   'sky.serve.'
                                                   'replica_managers.'
                                                   '_ReplicaLaunchThread'):
            manager._scale_up_batch_locked([{'use_spot': True}] * 20)

        assert claim.call_count == 2
        assert budget.service_remaining == 0
        assert manager._next_replica_id == 3
        assert len(manager._launch_thread_pool) == 2

    def test_paid_envelope_only_blocks_fresh_paid_launches(self):
        paid = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({paid: 1.0})
        budget = paid_capacity.LaunchBudget(remaining_by_location={paid: 4},
                                            pool_key_by_location={paid: 'paid'},
                                            states_by_pool_key={},
                                            globally_managed=True,
                                            service_remaining=0)

        assert manager._paid_service_envelope_blocks_launch(
            budget, {'use_spot': True})
        assert not manager._paid_service_envelope_blocks_launch(
            budget, {'use_spot': False})
        assert not manager._paid_service_envelope_blocks_launch(
            budget, {
                replica_managers.serve_constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY: True
            })
        assert not manager._paid_service_envelope_blocks_launch(
            budget, {
                replica_managers.serve_constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY: 7
            })

    @pytest.mark.parametrize('reserved_fill', [False, True])
    def test_paid_envelope_does_not_block_zero_cost_physical_launch(
            self, reserved_fill):
        zero = make_location('research', {'L4': 1},
                             use_spot=False,
                             cloud_name='Kubernetes')
        paid = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({zero: 0.0, paid: 1.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._persist_new_replica = mock.Mock()
        manager._uses_shared_zero_cost_demand_budget = mock.Mock(
            return_value=False)
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        budget = paid_capacity.LaunchBudget(remaining_by_location={paid: 4},
                                            pool_key_by_location={paid: 'paid'},
                                            states_by_pool_key={},
                                            globally_managed=True,
                                            service_remaining=0)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(paid_capacity,
                               'build_launch_budget',
                               return_value=budget), \
             mock.patch.object(
                 paid_capacity, 'try_persist_claim') as claim, \
             mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread') \
                     as thread:
            resources_override = {'use_spot': True}
            if reserved_fill:
                resources_override = {
                    replica_managers.serve_constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY: True
                }
            manager._scale_up_batch_locked([resources_override])

        claim.assert_not_called()
        manager._persist_new_replica.assert_called_once()
        persisted = manager._persist_new_replica.call_args.args[1]
        assert persisted.reserved_fill is reserved_fill
        assert persisted.is_zero_cost is True
        persisted_location = persisted.get_spot_location()
        assert persisted_location is not None
        assert persisted_location.region == zero.region
        assert persisted_location.use_spot is False
        assert manager._next_replica_id == 2
        assert len(manager._launch_thread_pool) == 1
        if reserved_fill:
            # A pinned fill never asks the API request queue to replay a
            # BrokenProcessPool generation whose original worker is ambiguous.
            assert thread.call_args.kwargs['args'][-1] is False

    def test_initial_exhausted_envelope_memoizes_paid_override(self):
        paid = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({paid: 1.0})
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._uses_shared_zero_cost_demand_budget = mock.Mock(
            return_value=False)
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        paid_key = paid_capacity.pool_key(paid,
                                          workspace='default',
                                          num_nodes=1)
        budget = paid_capacity.LaunchBudget(
            remaining_by_location={paid: 4},
            pool_key_by_location={paid: paid_key},
            states_by_pool_key={},
            globally_managed=True,
            service_remaining=0)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), mock.patch.object(
                                   paid_capacity,
                                   'build_launch_budget',
                                   return_value=budget), mock.patch.object(
                                       manager,
                                       '_scale_up_one_locked',
                                       wraps=manager._scale_up_one_locked
                                   ) as scale, mock.patch(
                                       'sky.serve.replica_managers.'
                                       '_should_use_spot',
                                       return_value=True):
            manager._scale_up_batch_locked([{'use_spot': True}] * 400)

        assert scale.call_count == 1
        assert budget.stop_sequence == 1
        assert manager._next_replica_id == 1
        assert not manager._launch_thread_pool

    @pytest.mark.parametrize('preexisting_stop', ['frontier', 'priority'])
    def test_preexisting_paid_stop_memoizes_matching_override(
            self, preexisting_stop):
        paid = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({paid: 1.0})
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._uses_shared_zero_cost_demand_budget = mock.Mock(
            return_value=False)
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        paid_key = paid_capacity.pool_key(paid,
                                          workspace='default',
                                          num_nodes=1)
        budget = paid_capacity.LaunchBudget(
            remaining_by_location={paid: 4},
            pool_key_by_location={paid: paid_key},
            states_by_pool_key={},
            globally_managed=True,
            service_remaining=16,
            frontier_limit=2,
            frontier_key_by_location={paid: ('l4',)})
        if preexisting_stop == 'frontier':
            budget.feedback_deferred_frontiers.add(('l4',))
        else:
            budget.priority_deferred_pool_keys.add(paid_key)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), mock.patch.object(
                                   paid_capacity,
                                   'build_launch_budget',
                                   return_value=budget), mock.patch.object(
                                       manager,
                                       '_scale_up_one_locked',
                                       wraps=manager._scale_up_one_locked
                                   ) as scale, mock.patch(
                                       'sky.serve.replica_managers.'
                                       '_should_use_spot',
                                       return_value=True):
            manager._scale_up_batch_locked([{'use_spot': True}] * 400)

        assert scale.call_count == 1
        assert budget.stop_sequence == 1
        assert manager._next_replica_id == 1
        assert not manager._launch_thread_pool

    def test_exhausted_paid_envelope_keeps_zero_cost_physical_launches(self):
        zero = make_location('research', {'L4': 1},
                             use_spot=False,
                             cloud_name='Kubernetes')
        paid = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({zero: 0.0, paid: 1.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._persist_new_replica = mock.Mock()
        manager._uses_shared_zero_cost_demand_budget = mock.Mock(
            return_value=False)
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        paid_key = paid_capacity.pool_key(paid,
                                          workspace='default',
                                          num_nodes=1)
        budget = paid_capacity.LaunchBudget(
            remaining_by_location={paid: 4},
            pool_key_by_location={paid: paid_key},
            states_by_pool_key={},
            globally_managed=True,
            service_remaining=0)

        with mock.patch.object(
                replica_managers.serve_state, 'get_replica_infos',
                return_value=[]), mock.patch.object(
                    paid_capacity, 'build_launch_budget',
                    return_value=budget), mock.patch.object(
                        paid_capacity,
                        'try_persist_claim') as claim, mock.patch(
                            'sky.serve.replica_managers.'
                            '_should_use_spot',
                            return_value=True), mock.patch(
                                'sky.serve.replica_managers.'
                                '_get_resources_ports',
                                return_value='8080'), mock.patch(
                                    'sky.serve.'
                                    'replica_managers.'
                                    '_ReplicaLaunchThread'):
            manager._scale_up_batch_locked([{'use_spot': True}] * 2)

        claim.assert_not_called()
        assert manager._persist_new_replica.call_count == 2
        assert manager._next_replica_id == 3
        assert len(manager._launch_thread_pool) == 2

    @pytest.mark.parametrize('exempt_kind',
                             ['reserved_fill', 'fresh_rebalance'])
    def test_exhausted_paid_envelope_scans_later_special_override(
            self, exempt_kind):
        zero = make_location('research', {'A100': 1},
                             use_spot=False,
                             cloud_name='Kubernetes')
        paid = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({zero: 0.0, paid: 1.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._persist_new_replica = mock.Mock()
        manager._uses_shared_zero_cost_demand_budget = mock.Mock(
            return_value=False)
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        paid_key = paid_capacity.pool_key(paid,
                                          workspace='default',
                                          num_nodes=1)
        budget = paid_capacity.LaunchBudget(
            remaining_by_location={paid: 4},
            pool_key_by_location={paid: paid_key},
            states_by_pool_key={},
            globally_managed=True,
            service_remaining=0)
        paid_only = {
            'accelerators': {
                'L4': 1
            },
            'use_spot': True,
        }
        if exempt_kind == 'reserved_fill':
            exempt = {
                replica_managers.serve_constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY: True
            }
        else:
            exempt = paid.to_dict()
            exempt[replica_managers.serve_constants.
                   COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY] = 7

        with mock.patch.object(
                replica_managers.serve_state, 'get_replica_infos',
                return_value=[]), mock.patch.object(
                    paid_capacity, 'build_launch_budget',
                    return_value=budget), mock.patch.object(
                        paid_capacity,
                        'try_persist_claim') as claim, mock.patch(
                            'sky.serve.replica_managers.'
                            '_should_use_spot',
                            return_value=True), mock.patch(
                                'sky.serve.replica_managers.'
                                '_get_resources_ports',
                                return_value='8080'), mock.patch(
                                    'sky.serve.'
                                    'replica_managers.'
                                    '_ReplicaLaunchThread'):
            manager._scale_up_batch_locked([paid_only, exempt])

        if exempt_kind == 'reserved_fill':
            claim.assert_not_called()
            manager._persist_new_replica.assert_called_once()
            persisted = manager._persist_new_replica.call_args.args[1]
            assert persisted.reserved_fill
            assert persisted.is_zero_cost
            assert replica_managers.spot_placer.locations_match_placement(
                persisted.get_spot_location(), zero)
            assert manager._next_replica_id == 2
            assert len(manager._launch_thread_pool) == 1
        else:
            # A fresh cost replacement is location-pinned but not yet
            # admitted. It must not bypass a full paid service envelope.
            claim.assert_not_called()
            manager._persist_new_replica.assert_not_called()
            assert manager._next_replica_id == 1
            assert not manager._launch_thread_pool

    @pytest.mark.parametrize(
        'claim_result',
        [
            paid_capacity.ClaimResult.FEEDBACK_PENDING,
            paid_capacity.ClaimResult.HIGHER_PRIORITY_WAITING,
        ],
    )
    def test_paid_deferral_scans_later_zero_cost_fill(self, claim_result):
        zero = make_location('research', {'L4': 1},
                             use_spot=False,
                             cloud_name='Kubernetes')
        paid = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({zero: 0.0, paid: 1.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._persist_new_replica = mock.Mock()
        manager._uses_shared_zero_cost_demand_budget = mock.Mock(
            return_value=False)
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=True)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        paid_key = paid_capacity.pool_key(paid,
                                          workspace='default',
                                          num_nodes=1)
        budget = paid_capacity.LaunchBudget(
            remaining_by_location={paid: 4},
            pool_key_by_location={paid: paid_key},
            states_by_pool_key={},
            globally_managed=True,
            service_remaining=16,
            frontier_limit=2,
            frontier_key_by_location={paid: ('l4',)})
        fill = {
            replica_managers.serve_constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY: True
        }

        with mock.patch.object(
                replica_managers.serve_state, 'get_replica_infos',
                return_value=[]), mock.patch.object(
                    paid_capacity, 'build_launch_budget',
                    return_value=budget), mock.patch.object(
                        paid_capacity,
                        'try_persist_claim',
                        return_value=claim_result) as claim, mock.patch(
                            'sky.serve.replica_managers.'
                            '_should_use_spot',
                            return_value=True), mock.patch(
                                'sky.serve.replica_managers.'
                                '_get_resources_ports',
                                return_value='8080'), mock.patch(
                                    'sky.serve.replica_managers.'
                                    '_ReplicaLaunchThread') as launch_thread:
            manager._scale_up_batch_locked([{'use_spot': True}, fill])

        claim.assert_called_once()
        manager._persist_new_replica.assert_called_once()
        persisted = manager._persist_new_replica.call_args.args[1]
        assert persisted.reserved_fill
        assert persisted.is_zero_cost
        assert replica_managers.spot_placer.locations_match_placement(
            persisted.get_spot_location(), zero)
        assert manager._next_replica_id == 2
        assert len(manager._launch_thread_pool) == 1
        launch_thread.assert_called()
        if claim_result == paid_capacity.ClaimResult.FEEDBACK_PENDING:
            assert budget.feedback_deferred_frontiers == {('l4',)}
            assert not budget.priority_deferred_pool_keys
        else:
            assert budget.priority_deferred_pool_keys == {paid_key}
            assert not budget.feedback_deferred_frontiers

    def test_feedback_deferral_rechecks_fresh_pinned_rebalance(self):
        paid = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({paid: 1.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._persist_new_replica = mock.Mock()
        manager._uses_shared_zero_cost_demand_budget = mock.Mock(
            return_value=False)
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        paid_key = paid_capacity.pool_key(paid,
                                          workspace='default',
                                          num_nodes=1)
        budget = paid_capacity.LaunchBudget(
            remaining_by_location={paid: 4},
            pool_key_by_location={paid: paid_key},
            states_by_pool_key={},
            globally_managed=True,
            service_remaining=16,
            frontier_limit=2,
            frontier_key_by_location={paid: ('l4',)})
        pinned = paid.to_dict()
        pinned[replica_managers.serve_constants.
               COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY] = 7

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), mock.patch.object(
                                   paid_capacity,
                                   'build_launch_budget',
                                   return_value=budget), mock.patch.object(
                                       paid_capacity,
                                       'try_persist_claim',
                                       return_value=paid_capacity.ClaimResult.
                                       FEEDBACK_PENDING) as claim, mock.patch(
                                           'sky.serve.replica_managers.'
                                           '_should_use_spot',
                                           return_value=True), mock.patch(
                                               'sky.serve.replica_managers.'
                                               '_get_resources_ports',
                                               return_value='8080'), mock.patch(
                                                   'sky.serve.'
                                                   'replica_managers.'
                                                   '_ReplicaLaunchThread'):
            manager._scale_up_batch_locked([{'use_spot': True}, pinned])

        # The first generic demand and the later fresh pinned replacement
        # each require admission. The marker pins location; only a durable
        # recovery row with an existing claim bypasses new admission.
        assert claim.call_count == 2
        manager._persist_new_replica.assert_not_called()
        assert budget.feedback_deferred_frontiers == {('l4',)}
        assert manager._next_replica_id == 1
        assert not manager._launch_thread_pool

    def test_priority_deferral_does_not_exhaust_or_spill_pool(self):
        cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({cheap: 1.0, expensive: 2.0})
        manager._service_hash = 'hash'
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._persist_new_replica = mock.Mock()
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        budget = paid_capacity.LaunchBudget(remaining_by_location={
            cheap: 1,
            expensive: 1,
        },
                                            pool_key_by_location={
                                                cheap: 'cheap',
                                                expensive: 'expensive',
                                            },
                                            states_by_pool_key={},
                                            globally_managed=True)

        with mock.patch.object(
                paid_capacity,
                'try_persist_claim',
                return_value=paid_capacity.ClaimResult.
                HIGHER_PRIORITY_WAITING), \
             mock.patch.object(paid_capacity, 'exhaust') as exhaust, \
             mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            launched = manager._scale_up_one_locked(
                None,
                set(), [],
                paid_location_launch_budget=budget,
                launch_priority=20)

        assert not launched
        assert budget.remaining_by_location == {cheap: 1, expensive: 1}
        assert budget.priority_deferred_pool_keys == {'cheap'}
        assert paid_capacity.select_location(manager._spot_placer,
                                             budget) is None
        exhaust.assert_not_called()
        manager._persist_new_replica.assert_not_called()

    def test_priority_deferred_large_wave_claims_same_pool_once(self):
        cheap = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        expensive = make_location('us-west-2', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({cheap: 1.0, expensive: 2.0})
        manager._service_hash = 'hash'
        manager._controller_owner = (1, '10.0.0.1')
        manager._next_replica_id = 1
        manager._pending_version = None
        manager._demand_should_skip_zero_cost = mock.Mock(return_value=False)
        manager._demand_should_skip_saturated_zero_cost = mock.Mock(
            return_value=False)
        budget = paid_capacity.LaunchBudget(remaining_by_location={
            cheap: 60,
            expensive: 60
        },
                                            pool_key_by_location={
                                                cheap: 'cheap',
                                                expensive: 'expensive'
                                            },
                                            states_by_pool_key={},
                                            globally_managed=True)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(paid_capacity,
                               'build_launch_budget',
                               return_value=budget), \
             mock.patch.object(
                 paid_capacity,
                 'select_location',
                 wraps=paid_capacity.select_location) as select_location, \
             mock.patch.object(
                 paid_capacity,
                 'try_persist_claim',
                 return_value=paid_capacity.ClaimResult.
                 HIGHER_PRIORITY_WAITING) as claim, \
             mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            manager._scale_up_batch_locked([{
                'use_spot': True
            }] * 300,
                                           launch_priority=20)

        claim.assert_called_once()
        select_location.assert_called_once()
        assert budget.priority_deferred_pool_keys == {'cheap'}
        assert manager._next_replica_id == 1
        assert len(manager._launch_thread_pool) == 0

    def test_unsatisfiable_exact_card_still_raises(self):
        l4 = make_location('us-east-1', {'L4': 1}, cloud_name='AWS')
        manager = self._manager({l4: 1.0})

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=True), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'
                       ) as safe_thread, \
             pytest.raises(ValueError,
                           match='matches exact accelerator override'):
            manager._launch_replica(
                replica_id=1,
                resources_override={'accelerators': {
                    'A100': 1
                }},
                existing_replica_infos=[])

        safe_thread.assert_not_called()


class TestZeroCostDemandProbeBudget:

    @staticmethod
    def _manager(zero_cost, active):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = zero_cost
        placer.active_locations.return_value = active
        manager._spot_placer = placer
        return manager

    @staticmethod
    def _info(location, *, ready=False, terminal=False):
        info = mock.Mock()
        info.is_ready = ready
        info.is_terminal = terminal
        info.get_spot_location.return_value = location
        return info

    def test_spills_after_each_active_shape_fills_probe_budget(self):
        zero_a = object()
        zero_b = object()
        paid = object()
        manager = self._manager([zero_a, zero_b], [zero_a, zero_b, paid])
        per_location = (
            replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION)
        infos = ([self._info(zero_a) for _ in range(per_location)] +
                 [self._info(zero_b) for _ in range(per_location)])

        with mock.patch(
                'sky.serve.replica_managers.spot_placer.'
                'locations_match_placement',
                side_effect=lambda a, b: a is b):
            assert manager._demand_should_skip_saturated_zero_cost(infos)
            assert not manager._demand_should_skip_saturated_zero_cost(
                infos[:-1])

    def test_ready_terminal_and_benched_rows_do_not_consume_budget(self):
        active_zero = object()
        benched_zero = object()
        paid = object()
        manager = self._manager([active_zero, benched_zero],
                                [active_zero, paid])
        per_location = (
            replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION)
        infos = [
            self._info(active_zero, ready=True),
            self._info(active_zero, terminal=True),
            *[self._info(benched_zero) for _ in range(per_location)],
            *[self._info(active_zero) for _ in range(per_location - 1)],
        ]

        with mock.patch(
                'sky.serve.replica_managers.spot_placer.'
                'locations_match_placement',
                side_effect=lambda a, b: a is b):
            assert not manager._demand_should_skip_saturated_zero_cost(infos)
            infos.append(self._info(active_zero))
            assert manager._demand_should_skip_saturated_zero_cost(infos)

    @staticmethod
    def _location(cloud, region, gpu, *, use_spot, count=1):
        return replica_managers.spot_placer.Location.from_pickleable({
            'cloud': cloud,
            'region': region,
            'zone': None,
            'accelerators': {
                gpu: count
            },
            'use_spot': use_spot,
        })

    @staticmethod
    def _observations(values):
        return {
            key: replica_managers.reserved_capacity.FreeGpuObservation(
                value, 100.0
                if value is not None else None) for key, value in values.items()
        }

    def test_large_batch_uses_all_223_measured_gpus_then_spills(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        manager._spot_placer.select_next_zero_cost_location.return_value = zero

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 223
                               })) as query:
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 223}
        for _ in range(27):
            assert manager._select_budgeted_zero_cost_location(budget) == zero
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 7}
        assert manager._select_budgeted_zero_cost_location(budget) is None
        query.assert_called_once_with([zero])

    def test_pending_rows_are_debited_from_measured_free_slots(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        pending = [self._info(zero) for _ in range(3)]
        for info in pending:
            info.status = replica_managers.serve_state.ReplicaStatus.PENDING

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 223
                               })):
            budget = manager._build_zero_cost_demand_budget(
                pending, [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 220}

    def test_measured_gpu_budget_debits_complete_backend_widths(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        manager._spot_placer.select_next_zero_cost_location.return_value = zero

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 23
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        assert manager._select_budgeted_zero_cost_location(budget) == zero
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 15}
        assert manager._select_budgeted_zero_cost_location(budget) == zero
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 7}
        assert manager._select_budgeted_zero_cost_location(budget) is None

    def test_pending_multi_gpu_rows_debit_measured_gpu_slots(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        pending = [self._info(zero) for _ in range(2)]
        for info in pending:
            info.status = replica_managers.serve_state.ReplicaStatus.PENDING
            info.planned_capacity = 8

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 23
                               })):
            budget = manager._build_zero_cost_demand_budget(
                pending, [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 7}

    def test_pending_rows_from_peer_service_share_the_same_gpu_budget(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        peer_pending = self._info(zero)
        peer_pending.status = replica_managers.serve_state.ReplicaStatus.PENDING
        peer_pending.planned_capacity = 8

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 223
                               })):
            budget = manager._build_zero_cost_demand_budget(
                [], [None] * 500, capacity_replica_infos=[peer_pending])

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 215}

    def test_peer_becoming_ready_after_snapshot_is_still_debited(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        peer = self._info(zero, ready=True)
        peer.created_at = 50.0
        peer.planned_capacity = 8
        peer.status_property.first_ready_time = 101.0

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 223
                               })):
            budget = manager._build_zero_cost_demand_budget(
                [], [None] * 500, capacity_replica_infos=[peer])

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 215}

    def test_blackout_budget_remains_bounded_in_backend_attempts(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        manager._spot_placer.select_next_zero_cost_location.return_value = zero

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): None
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        attempts = replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION
        for _ in range(attempts):
            assert manager._select_budgeted_zero_cost_location(budget) == zero
        assert manager._select_budgeted_zero_cost_location(budget) is None

    def test_measurement_blackout_falls_back_to_probe_budget(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])
        unresolved = [self._info(zero) for _ in range(2)]
        for info in unresolved:
            info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): None
                               })):
            budget = manager._build_zero_cost_demand_budget(
                unresolved, [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 2}

    def test_mixed_measured_and_blackout_contexts_budget_independently(self):
        # One context measures successfully (measured slots minus PENDING
        # debits); the other fails (None) and falls back to the bounded
        # per-location probe allowance minus unresolved rows. The two
        # budgets are computed independently in the same snapshot.
        zero_a = self._location('Kubernetes', 'ctx-a', 'A100', use_spot=False)
        zero_b = self._location('Kubernetes', 'ctx-b', 'A100', use_spot=False)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero_a, zero_b], [zero_a, zero_b, paid])
        pending_a = [self._info(zero_a) for _ in range(2)]
        for info in pending_a:
            info.status = replica_managers.serve_state.ReplicaStatus.PENDING
        unresolved_b = [self._info(zero_b)]
        for info in unresolved_b:
            info.status = (
                replica_managers.serve_state.ReplicaStatus.PROVISIONING)

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('ctx-a', 'a100'): 7,
                                   ('ctx-b', 'a100'): None
                               })):
            budget = manager._build_zero_cost_demand_budget(
                pending_a + unresolved_b, [None] * 500)

        assert budget is not None
        per_location = (
            replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION)
        assert budget.remaining_by_pool == {
            ('ctx-a', 'a100'): 5,
            ('ctx-b', 'a100'): per_location - 1,
        }
        assert budget.measured_by_pool == {
            ('ctx-a', 'a100'): 7,
            ('ctx-b', 'a100'): None,
        }

    def test_equal_unknown_budgets_alternate_across_contexts(self):
        zero_a = self._location('Kubernetes', 'ctx-a', 'A100', use_spot=False)
        zero_b = self._location('Kubernetes', 'ctx-b', 'A100', use_spot=False)
        manager = self._manager([zero_a, zero_b], [zero_a, zero_b])
        manager._spot_placer.select_next_zero_cost_location.side_effect = (
            lambda *, allowed_locations: min(
                allowed_locations, key=lambda location: location.region))
        attempts = (
            replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION)
        budget = replica_managers._ZeroCostDemandBudget(
            remaining_by_pool={
                ('ctx-a', 'a100'): attempts,
                ('ctx-b', 'a100'): attempts,
            },
            measured_by_pool={
                ('ctx-a', 'a100'): None,
                ('ctx-b', 'a100'): None,
            })

        selected = [
            manager._select_budgeted_zero_cost_location(budget)
            for _ in range(4)
        ]

        assert selected == [zero_a, zero_b, zero_a, zero_b]
        assert budget.remaining_by_pool == {
            ('ctx-a', 'a100'): attempts - 2,
            ('ctx-b', 'a100'): attempts - 2,
        }

    def test_two_hundred_measured_slots_are_distributed_by_capacity(self):
        zero_a = self._location('Kubernetes', 'ctx-a', 'A100', use_spot=False)
        zero_b = self._location('Kubernetes', 'ctx-b', 'A100', use_spot=False)
        manager = self._manager([zero_a, zero_b], [zero_a, zero_b])
        manager._spot_placer.select_next_zero_cost_location.side_effect = (
            lambda *, allowed_locations: min(
                allowed_locations, key=lambda location: location.region))
        budget = replica_managers._ZeroCostDemandBudget(
            remaining_by_pool={
                ('ctx-a', 'a100'): 120,
                ('ctx-b', 'a100'): 80,
            },
            measured_by_pool={
                ('ctx-a', 'a100'): 120,
                ('ctx-b', 'a100'): 80,
            })

        selected = [
            manager._select_budgeted_zero_cost_location(budget)
            for _ in range(200)
        ]

        assert selected[:4] == [zero_a, zero_b, zero_a, zero_b]
        assert selected[:128].count(zero_a) == 64
        assert selected[:128].count(zero_b) == 64
        assert selected.count(zero_a) == 120
        assert selected.count(zero_b) == 80
        assert manager._select_budgeted_zero_cost_location(budget) is None

    def test_accelerators_in_same_context_have_independent_gpu_budgets(self):
        a100 = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        h100 = self._location('Kubernetes',
                              'research-ctx',
                              'H100',
                              use_spot=False,
                              count=8)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([a100, h100], [a100, h100, paid])

        with mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 223,
                                   ('research-ctx', 'h100'): 16,
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {
            ('research-ctx', 'a100'): 223,
            ('research-ctx', 'h100'): 16,
        }

    def test_targeted_zero_cost_selection_keeps_a100_variants_exact(self):
        a100 = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False)
        a100_80gb = self._location('Kubernetes',
                                   'research-ctx',
                                   'A100-80GB',
                                   use_spot=False)
        manager = self._manager([a100, a100_80gb], [a100, a100_80gb])
        manager._spot_placer.select_next_zero_cost_location.side_effect = (
            lambda *, allowed_locations: next(iter(allowed_locations)))
        budget = replica_managers._ZeroCostDemandBudget(
            remaining_by_pool={
                ('research-ctx', 'a100'): 1,
                ('research-ctx', 'a100-80gb'): 1,
            },
            measured_by_pool={
                ('research-ctx', 'a100'): 1,
                ('research-ctx', 'a100-80gb'): 1,
            })

        selected = manager._select_budgeted_zero_cost_location(
            budget, {a100_80gb})

        assert selected == a100_80gb
        assert budget.remaining_by_pool == {
            ('research-ctx', 'a100'): 1,
            ('research-ctx', 'a100-80gb'): 0,
        }

    def test_successful_zero_snapshot_does_not_speculate(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False)
        paid = self._location('AWS', 'us-east-1', 'L4', use_spot=True)
        manager = self._manager([zero], [zero, paid])

        with mock.patch.object(
                replica_managers,
                '_kubernetes_context_has_configured_autoscaler',
                return_value=False), \
             mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 0
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 0}
        assert manager._select_budgeted_zero_cost_location(budget) is None

    def test_zero_cost_only_pool_still_builds_authoritative_budget(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False,
                              count=8)
        manager = self._manager([zero], [zero])

        with mock.patch.object(
                replica_managers,
                '_kubernetes_context_has_configured_autoscaler',
                return_value=False), \
             mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 0
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): 0}

    def test_zero_snapshot_with_configured_autoscaler_gets_bounded_probes(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False)
        manager = self._manager([zero], [zero])

        with mock.patch.object(
                replica_managers,
                '_kubernetes_context_has_configured_autoscaler',
                return_value=True), \
             mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 0
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        attempts = (
            replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION)
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): attempts}
        assert budget.measured_by_pool == {('research-ctx', 'a100'): None}

    def test_kubernetes_only_zero_snapshot_gets_reclamation_probes(self):
        zero = self._location('Kubernetes',
                              'research-ctx',
                              'A100',
                              use_spot=False)
        manager = self._manager([zero], [zero])

        with mock.patch.object(
                replica_managers,
                '_placer_has_only_non_spot_kubernetes_gpu_locations',
                return_value=True), \
             mock.patch.object(
                 replica_managers,
                 '_kubernetes_context_has_configured_autoscaler',
                 return_value=False), \
             mock.patch.object(replica_managers.reserved_capacity,
                               'get_cached_free_gpus_by_pool',
                               return_value=self._observations({
                                   ('research-ctx', 'a100'): 0
                               })):
            budget = manager._build_zero_cost_demand_budget([], [None] * 500)

        assert budget is not None
        attempts = (
            replica_managers._ZERO_COST_SPECULATIVE_LAUNCHES_PER_LOCATION)
        assert budget.remaining_by_pool == {('research-ctx', 'a100'): attempts}
        assert budget.measured_by_pool == {('research-ctx', 'a100'): None}


class TestRecoveryRetryAndIsolation:
    """A failed recovery pass must retry (previously a recovery exception
    failed the boot and the HA daemon retried via respawn; the recovery
    thread must not die silently and strand un-redriven replicas), and one
    bad replica must not abort re-driving the rest."""

    def test_orphaned_spot_intent_persist_is_owner_fenced(self):
        mgr = _make_manager()
        mgr._service_hash = 'incarnation-a'
        mgr._controller_owner = (123, '10.0.0.1')
        mgr._spot_placer = None

        info = mock.MagicMock()
        info.replica_id = 1
        info.cluster_name = 'svc-1-incarnation'
        info.is_spot = True
        info.status = replica_managers.serve_state.ReplicaStatus.READY
        info.status_property.preempted = False
        info.status_property.is_scale_down = False
        info.status_property.purged = False
        info.get_spot_location.return_value = None

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'add_or_update_replica',
                 return_value=True) as persist, \
             mock.patch.object(mgr, '_terminate_replica'):
            mgr._recover_replica_operations()

        persist.assert_called_once_with('svc',
                                        1,
                                        info,
                                        expected_service_hash='incarnation-a',
                                        expected_controller_owner=(123,
                                                                   '10.0.0.1'),
                                        expected_replica_exists=True,
                                        guard_launch_exclusion=False)

    def test_one_bad_launch_does_not_strand_the_rest(self):
        mgr = _make_manager(next_replica_id=1)
        launched = []

        def _launch(replica_id,
                    resources_override=None,
                    existing_replica_infos=None,
                    recovering_existing_replica=False,
                    **_kwargs):
            del resources_override, existing_replica_infos
            assert recovering_existing_replica
            if replica_id == 2:
                raise RuntimeError('boom')
            launched.append(replica_id)

        infos = [
            _fake_replica_info(
                i,
                status=replica_managers.serve_state.ReplicaStatus.PROVISIONING)
            for i in (1, 2, 3)
        ]
        for info in infos:
            info.resources_override = None
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr._recover_replica_operations()
        # Replica 2 failed; 1 and 3 still re-driven.
        assert launched == [1, 3]

    def test_digest_failure_omits_telemetry_without_skipping_restart_redrive(
            self):
        mgr = _make_manager()
        info = _fake_replica_info(
            1, status=replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        info.resources_override = {'typed': mock.sentinel.value}

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(ordinary_launch_handoff,
                               'redacted_input_digest',
                               return_value=None) as digest, \
             mock.patch.object(
                 mgr, '_emit_ordinary_launch_handoff_event') as emit_event, \
             mock.patch.object(mgr, '_launch_replica') as launch:
            mgr._recover_replica_operations()

        digest.assert_called_once_with(mgr.yaml_content,
                                       info.resources_override)
        emit_event.assert_not_called()
        launch.assert_called_once()
        assert launch.call_args.args == (1,)
        assert launch.call_args.kwargs['recovering_existing_replica'] is True

    def test_restart_redrives_pointer_cleared_pre_effect_row(self):
        """A crash after projection still reaches generation+1 admission."""
        mgr = _make_manager()
        mgr._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PENDING)
        info.resources_override = {'region': 'us-east-1'}
        info.paid_capacity_pool_key = 'pool-a'

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch',
                 return_value=None) as inspect, \
             mock.patch.object(mgr, '_launch_replica') as launch:
            mgr._recover_replica_operations()

        inspect.assert_called_once_with('svc', 1, info.replica_record_id)
        launch.assert_called_once()
        args, kwargs = launch.call_args
        assert args == (1,)
        assert kwargs['recovering_existing_replica'] is True
        assert kwargs['prior_replica_record_id'] == info.replica_record_id
        assert kwargs['prior_paid_capacity_pool_key'] == 'pool-a'

    def test_generic_restart_retires_pointerless_pre_admission_intent(self):
        """A planner row without an action is discarded, never retyped."""
        mgr = _make_manager()
        authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND, generic=True)
        mgr._ordinary_launch_binding_authority = authority
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PENDING)
        info.resources_override = {'region': 'us-east-1'}
        retirement = ordinary_launch_binding.PreAdmissionRetirement(
            ordinary_launch_binding.PreAdmissionRetirementDisposition.RETIRED,
            ordinary_launch_binding.NonPoolLaunchProfileKind.RESERVED_FILL)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch',
                 return_value=None) as inspect, \
             mock.patch.object(
                 ordinary_launch_binding,
                 'retire_pre_admission_non_pool_launch_intent',
                 return_value=retirement) as retire, \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr._recover_replica_operations()

        inspect.assert_called_once_with('svc', 1, info.replica_record_id)
        retire.assert_called_once_with(authority, 1, info.replica_record_id)
        launch.assert_not_called()
        terminate.assert_not_called()

    def test_recovered_bound_adopter_freezes_replica_version_guard(self):
        """A recovered waiter remains fenced to its persisted version."""
        mgr = _make_manager()
        mgr._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND, generic=True)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        info.version = 1
        info.resources_override = None
        reduction = types.SimpleNamespace(context=types.SimpleNamespace(
            request_id='request-v1'))
        recovery_task = types.SimpleNamespace(
            resources=[types.SimpleNamespace(cloud=clouds.AWS())])
        worker = mock.Mock(spec=replica_managers._ReplicaLaunchThread)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch',
                 return_value=reduction), \
             mock.patch.object(replica_managers,
                               '_build_replica_launch_task',
                               return_value=recovery_task), \
             mock.patch.object(
                 mgr,
                 '_bound_ordinary_launch_callbacks',
                 return_value=(mock.Mock(), mock.Mock(), mock.Mock())), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch.object(replica_managers,
                               '_ReplicaLaunchThread',
                               return_value=worker) as launch_thread, \
             mock.patch.object(mgr, '_launch_replica') as launch:
            mgr._recover_replica_operations()

        worker.start.assert_called_once_with()
        launch.assert_not_called()
        frozen_guard = launch_thread.call_args.kwargs['kwargs'][
            'supersession_guard']
        assert frozen_guard() == (True, 'authorized')
        mgr.latest_version = 2
        assert frozen_guard() == (False, 'manager-version-changed')

    def test_transient_bound_inspection_retries_same_controller_adoption(self):
        """A transient startup read cannot strand the only exact waiter."""
        mgr = _make_manager()
        mgr._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND, generic=True)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        info.resources_override = None
        reduction = types.SimpleNamespace(context=types.SimpleNamespace(
            request_id='request-after-retry'))
        recovery_task = types.SimpleNamespace(
            resources=[types.SimpleNamespace(cloud=clouds.AWS())])
        worker = mock.Mock(spec=replica_managers._ReplicaLaunchThread)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch',
                 side_effect=[RuntimeError('transient database read'),
                              reduction]) as inspect, \
             mock.patch.object(replica_managers,
                               '_build_replica_launch_task',
                               return_value=recovery_task), \
             mock.patch.object(
                 mgr,
                 '_bound_ordinary_launch_callbacks',
                 return_value=(mock.Mock(), mock.Mock(), mock.Mock())), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch.object(replica_managers,
                               '_ReplicaLaunchThread',
                               return_value=worker):
            with pytest.raises(RuntimeError,
                               match='recovery remains incomplete'):
                mgr._recover_replica_operations()
            mgr._recover_replica_operations()

        assert inspect.call_count == 2
        worker.start.assert_called_once_with()
        assert mgr._launch_thread_pool[1] is worker
        assert mgr._replica_to_request_id[1] == 'request-after-retry'

    def test_pointerless_old_version_recovery_enters_teardown(self):
        mgr = _make_manager()
        mgr.latest_version = 2
        mgr._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PENDING)
        info.version = 1
        info.resources_override = None

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_yaml_contents',
                 return_value={1: 'version: 1'}), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch',
                 return_value=None) as inspect, \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr._recover_replica_operations()

        inspect.assert_not_called()
        launch.assert_not_called()
        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True,
                                          in_flight_drain_cap_seconds=0)

    def test_interrupted_bound_projection_recovers_into_teardown_only(self):
        mgr = _make_manager()
        mgr._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND)
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.INTERRUPTED)
        info.status_property.is_scale_down = True
        assert info.status == (
            replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch') as inspect, \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr._recover_replica_operations()

        inspect.assert_not_called()
        launch.assert_not_called()
        terminate.assert_called_once()
        assert terminate.call_args.args == (1,)
        assert terminate.call_args.kwargs['sync_down_logs'] is False
        assert terminate.call_args.kwargs['replica_drain_delay_seconds'] == 0
        assert terminate.call_args.kwargs['purge'] is False
        assert terminate.call_args.kwargs['is_scale_down'] is True

    def test_newer_pending_version_tears_down_active_bound_request(self):
        mgr = _make_manager()
        mgr._ordinary_launch_binding_authority = _binding_authority(
            ordinary_launch_binding.BindingMode.BOUND)
        mgr._pending_version = 2
        info = _fake_replica_info(
            1, replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        info.version = 1
        reduction = types.SimpleNamespace(context=types.SimpleNamespace(
            request_id='request-v1'))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.request_postgres,
                 'inspect_bound_ordinary_launch',
                 return_value=reduction) as inspect, \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr._recover_replica_operations()

        inspect.assert_called_once_with('svc', 1, info.replica_record_id)
        launch.assert_not_called()
        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True,
                                          in_flight_drain_cap_seconds=0)

    def test_newer_pending_version_stops_stale_recovery_wave(self):
        mgr = _make_manager(next_replica_id=1)
        launched = []
        infos = [
            _fake_replica_info(
                i,
                status=replica_managers.serve_state.ReplicaStatus.PROVISIONING)
            for i in (1, 2, 3)
        ]
        for info in infos:
            info.version = 1
            info.resources_override = None

        def _launch(replica_id, **_kwargs):
            launched.append(replica_id)
            mgr.notify_version_pending(2)

        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr._recover_replica_operations()

        assert launched == [1]

    def test_bound_candidate_adopter_uses_joinable_launch_worker(self):
        """A restarted exact-request waiter must notify the refresher."""
        mgr = _make_manager()
        candidate = _fake_replica_info(
            1, status=replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        candidate.cluster_name = 'svc-1'
        candidate.system_recovery_disposition = (
            recovery_state.SystemRecoveryDisposition.CANDIDATE)
        candidate.system_recovery_launch_intent = mock.sentinel.intent
        candidate.launch_request_id = 'request-1'
        candidate.service_job_id = None
        worker = mock.Mock(spec=replica_managers._ReplicaLaunchThread)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[candidate]), \
             mock.patch.object(
                 mgr,
                 '_initialize_system_recovery_process_guards',
                 side_effect=lambda infos: infos), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch.object(
                 replica_managers,
                 '_ReplicaLaunchThread',
                 return_value=worker) as launch_thread:
            mgr._recover_replica_operations()

        runtime = mgr._legacy_mutation_runtime_state()
        launch_thread.assert_called_once_with(
            target=replica_managers.adopt_system_recovery_launch,
            replica_id=1,
            completion_queue=runtime.launch_completion_queue,
            completion_event=runtime.launch_completion_event,
            args=(1, 'svc-1', '/tmp/launch.log', 'request-1', mock.ANY))
        assert runtime.launch_thread_pool[1] is worker
        assert runtime.replica_to_request_id[1] == 'request-1'
        worker.start.assert_called_once_with()

    @pytest.mark.parametrize('status', [
        replica_managers.serve_state.ReplicaStatus.PENDING,
        replica_managers.serve_state.ReplicaStatus.PROVISIONING,
    ])
    def test_recovery_tears_down_interrupted_fill_without_redrive(self, status):
        mgr = _make_manager()
        fill_row = _fake_replica_info(1, status=status)
        fill_row.resources_override = None
        fill_row.reserved_fill = True
        demand_row = _fake_replica_info(2, status=status)
        demand_row.resources_override = {
            'region': 'us-east-1',
            'accelerators': {
                'A100': 1,
            },
        }
        demand_row.reserved_fill = False
        demand_row.paid_capacity_pool_key = 'exact-paid-pool'

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[fill_row, demand_row]), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'quiesce_service_replica_launch_requests',
                 return_value=True) as quiesce, \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr._recover_replica_operations()

        quiesce.assert_called_once_with(
            'svc', [fill_row],
            continue_guard=mgr._service_is_launch_authorized,
            include_terminal_history=False)
        terminate.assert_called_once_with(1,
                                          sync_down_logs=False,
                                          replica_drain_delay_seconds=0,
                                          is_scale_down=True,
                                          in_flight_drain_cap_seconds=0)
        launch.assert_called_once()
        assert launch.call_args.args[0] == 2
        assert launch.call_args.kwargs['recovering_existing_replica'] is True
        assert (launch.call_args.kwargs['resources_override'] ==
                demand_row.resources_override)
        assert (launch.call_args.kwargs['prior_paid_capacity_pool_key'] ==
                'exact-paid-pool')

    def test_recovery_quiesces_accepted_fill_launch_before_teardown(self):
        mgr = _make_manager()
        mgr._resource_scope = 'incarnation-a'
        mgr._service_hash = 'incarnation-a'
        fill_row = _fake_replica_info(
            1, status=replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        fill_row.cluster_name = (
            replica_managers.serve_utils.generate_replica_cluster_name(
                'svc', 1, 'incarnation-a'))
        _stamp_protocol_v2_fill(fill_row, generation=7)
        request_terminal = False
        request_quiesced = False
        quiescence_polls = 0
        events = []

        def _request(status, *, quiesced_generation=None, quiesced_at=None):
            return types.SimpleNamespace(
                request_id='launch-request',
                name='sky.launch',
                cluster_name=fill_row.cluster_name,
                execution_generation=7,
                status=status,
                execution_quiescence_required=True,
                execution_quiesced_generation=quiesced_generation,
                execution_quiesced_at=quiesced_at)

        def _query(req_filter):
            nonlocal quiescence_polls, request_quiesced
            if req_filter.request_ids is None:
                assert req_filter == api_requests.RequestTaskFilter(
                    cluster_names=[fill_row.cluster_name],
                    include_request_names=['sky.launch'],
                    execution_quiescence_candidates_only=True,
                    fields=[
                        'request_id', 'name', 'cluster_name',
                        'execution_generation', 'status',
                        'execution_quiescence_required',
                        'execution_quiesced_generation', 'execution_quiesced_at'
                    ],
                    sort=True)
                events.append('discovery-status')
                if request_quiesced:
                    return []
                if request_terminal:
                    return [_request(api_requests.RequestStatus.CANCELLED)]
                return [_request(api_requests.RequestStatus.RUNNING)]
            assert req_filter == api_requests.RequestTaskFilter(
                request_ids=['launch-request'],
                fields=[
                    'request_id', 'name', 'cluster_name', 'status',
                    'execution_generation', 'execution_quiescence_required',
                    'execution_quiesced_generation', 'execution_quiesced_at'
                ],
                sort=True)
            events.append('quiescence-status')
            quiescence_polls += 1
            if quiescence_polls == 1:
                return [_request(api_requests.RequestStatus.CANCELLED)]
            request_quiesced = True
            return [
                _request(api_requests.RequestStatus.CANCELLED,
                         quiesced_generation=7,
                         quiesced_at=1.0)
            ]

        def _cancel(request_ids, *, user_id):
            nonlocal request_terminal
            assert request_ids == ['launch-request']
            assert user_id is None
            events.append('cancel')
            request_terminal = True
            return request_ids

        def _terminate(*_args, **_kwargs):
            assert request_quiesced
            events.append('terminate')

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[fill_row]), \
             mock.patch.object(api_requests,
                               'get_request_tasks',
                               side_effect=_query), \
             mock.patch.object(api_requests,
                               'kill_requests_exact',
                               side_effect=_cancel), \
             mock.patch.object(
                 request_postgres,
                 'require_builtin_execution_quiescence_backends'), \
             mock.patch.object(replica_managers.serve_utils.sdk,
                               'api_status',
                               side_effect=AssertionError), \
             mock.patch.object(replica_managers.serve_utils.sdk,
                               'api_cancel',
                               side_effect=AssertionError), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 '_LAUNCH_QUIESCE_POLL_SECONDS', 0), \
             mock.patch.object(mgr,
                               '_service_is_launch_authorized',
                               return_value=True), \
             mock.patch.object(mgr,
                               '_terminate_replica',
                               side_effect=_terminate):
            mgr._recover_replica_operations()

        assert events == [
            'discovery-status', 'cancel', 'quiescence-status',
            'quiescence-status', 'discovery-status', 'terminate'
        ]

    def test_recovery_batches_interrupted_fill_quiescence(self):
        mgr = _make_manager()
        fill_rows = [
            _fake_replica_info(
                replica_id,
                status=replica_managers.serve_state.ReplicaStatus.PENDING)
            for replica_id in (1, 2)
        ]
        for info in fill_rows:
            info.resources_override = None
            info.reserved_fill = True

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=fill_rows), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'quiesce_service_replica_launch_requests',
                 return_value=True) as quiesce, \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr._recover_replica_operations()

        quiesce.assert_called_once_with(
            'svc',
            fill_rows,
            continue_guard=mgr._service_is_launch_authorized,
            include_terminal_history=False)
        assert [call.args[0] for call in terminate.call_args_list] == [1, 2]
        launch.assert_not_called()

    def test_recovery_partitions_legacy_and_protocol_v2_fill_barriers(self):
        mgr = _make_manager()
        mgr._resource_scope = 'incarnation-a'
        mgr._service_hash = 'incarnation-a'
        legacy = _fake_replica_info(
            1, status=replica_managers.serve_state.ReplicaStatus.PENDING)
        legacy.cluster_name = 'svc-1'
        legacy.resources_override = None
        legacy.reserved_fill = True
        legacy.reserved_fill_pool_key = (
            replica_managers.reserved_capacity_broker.make_pool_key(
                'east', 'L4'))
        legacy.reserved_fill_service_generation = 0
        legacy.reserved_fill_physical_cluster_uid = None
        current = _fake_replica_info(
            2, status=replica_managers.serve_state.ReplicaStatus.PENDING)
        current.cluster_name = (
            replica_managers.serve_utils.generate_replica_cluster_name(
                'svc', 2, 'incarnation-a'))
        _stamp_protocol_v2_fill(current)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[legacy, current]), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'quiesce_service_replica_launch_requests',
                 return_value=True) as quiesce, \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr._recover_replica_operations()

        assert quiesce.call_args_list == [
            mock.call('svc', [current],
                      continue_guard=mgr._service_is_launch_authorized,
                      include_terminal_history=True),
            mock.call('svc', [legacy],
                      continue_guard=mgr._service_is_launch_authorized,
                      include_terminal_history=False),
        ]
        assert [call.args[0] for call in terminate.call_args_list] == [2, 1]
        launch.assert_not_called()

    def test_recovery_redrives_v2_teardown_before_ordinary(self):
        mgr = _make_manager()
        ordinary = replica_managers.ReplicaInfo(replica_id=1,
                                                cluster_name='svc-1',
                                                replica_port='8080',
                                                is_spot=False,
                                                location=None,
                                                version=1,
                                                resources_override=None)
        ordinary.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        ordinary.status_property.preempted = True
        fenced = replica_managers.ReplicaInfo(replica_id=2,
                                              cluster_name='svc-2',
                                              replica_port='8080',
                                              is_spot=False,
                                              location=None,
                                              version=1,
                                              resources_override=None)
        fenced.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        fenced.status_property.preempted = True
        _stamp_protocol_v2_fill(fenced)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[ordinary, fenced]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_yaml_contents',
                               return_value={}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={}), \
             mock.patch.object(mgr, '_terminate_replica') as terminate:
            mgr._recover_replica_operations()

        assert [call.args[0] for call in terminate.call_args_list] == [2, 1]

    @pytest.mark.parametrize(('marker', 'carry_v2_identity'), [
        (False, True),
        ('yes', False),
    ])
    def test_recovery_rejects_rows_that_could_bypass_fill_classification(
            self, marker, carry_v2_identity):
        mgr = _make_manager()
        info = _fake_replica_info(
            1, status=replica_managers.serve_state.ReplicaStatus.PENDING)
        info.resources_override = None
        if carry_v2_identity:
            _stamp_protocol_v2_fill(info)
        info.reserved_fill = marker

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[info]), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'quiesce_service_replica_launch_requests') as quiesce, \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             pytest.raises(RuntimeError,
                           match='validate interrupted reserved-fill'):
            mgr._recover_replica_operations()

        quiesce.assert_not_called()
        launch.assert_not_called()
        terminate.assert_not_called()

    @pytest.mark.parametrize('scope,cluster_matches', [
        (None, True),
        ('incarnation-a', False),
    ])
    def test_recovery_rejects_unscoped_or_misnamed_protocol_v2_fill(
            self, scope, cluster_matches):
        mgr = _make_manager()
        mgr._resource_scope = scope
        mgr._service_hash = 'incarnation-a'
        fill_row = _fake_replica_info(
            1, status=replica_managers.serve_state.ReplicaStatus.PENDING)
        expected_name = (
            replica_managers.serve_utils.generate_replica_cluster_name(
                'svc', 1, 'incarnation-a'))
        fill_row.cluster_name = (expected_name
                                 if cluster_matches else 'svc-1-wrong-scope')
        _stamp_protocol_v2_fill(fill_row)

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[fill_row]), \
             mock.patch.object(
                 replica_managers.serve_utils,
                 'quiesce_service_replica_launch_requests') as quiesce, \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             pytest.raises(RuntimeError, match='validate interrupted'):
            mgr._recover_replica_operations()

        quiesce.assert_not_called()
        launch.assert_not_called()
        terminate.assert_not_called()

    def test_recovery_retains_fill_when_launch_quiescence_is_uncertain(self):
        mgr = _make_manager()
        fill_row = _fake_replica_info(
            1, status=replica_managers.serve_state.ReplicaStatus.PENDING)
        fill_row.resources_override = None
        fill_row.reserved_fill = True

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[fill_row]), \
             mock.patch.object(replica_managers.serve_utils.sdk,
                               'api_status',
                               side_effect=RuntimeError('status unavailable')), \
             mock.patch.object(mgr, '_launch_replica') as launch, \
             mock.patch.object(mgr, '_terminate_replica') as terminate, \
             pytest.raises(RuntimeError,
                           match='Could not quiesce interrupted'):
            mgr._recover_replica_operations()

        launch.assert_not_called()
        terminate.assert_not_called()

    def test_provisioning_redrive_reenters_current_exact_card_budget(self):
        """Recovery cannot turn stale PROVISIONING intent into a new launch."""
        mgr = _make_manager()
        mgr.yaml_content = 'dummy: yaml'
        mgr.latest_version = 1
        mgr._uses_logical_replicas = True
        mgr._logical_exact_accelerator_shapes = {'A100': 1}
        mgr._spot_placer = None
        mgr._replica_to_request_id = {}
        mgr._replica_to_launch_cancelled = {}
        interrupted = _fake_replica_info(
            1, status=replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        interrupted.resources_override = {'accelerators': {'A100': 1}}
        interrupted.reserved_fill = False
        interrupted.is_zero_cost = False
        interrupted.unknown_capacity_replacement = False
        interrupted.cost_rebalance_for_replica_id = None
        persisted: dict[int, replica_managers.ReplicaInfo] = {}

        def _persist(_service_name, replica_id, info, **_kwargs):
            persisted[replica_id] = info

        with mock.patch('sky.serve.replica_managers._should_use_spot',
                        return_value=False), \
             mock.patch('sky.serve.replica_managers._get_resources_ports',
                        return_value='8080'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_utils.'
                 'generate_replica_launch_log_file_name',
                 return_value='/tmp/launch.log'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.get_replica_infos',
                 return_value=[interrupted]), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state.'
                 'add_or_update_replica',
                 side_effect=_persist), \
             mock.patch('sky.serve.replica_managers._ReplicaLaunchThread'):
            mgr._recover_replica_operations()

        recovered = persisted[1]
        assert recovered.status == (
            replica_managers.serve_state.ReplicaStatus.PENDING)

        ready = replica_managers.ReplicaInfo(
            replica_id=2,
            cluster_name='svc-2',
            replica_port='8080',
            is_spot=False,
            location=None,
            version=1,
            resources_override={'accelerators': {
                'A100': 1
            }})
        ready.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        ready.status_property.service_ready_now = True
        ready.status_property.first_ready_time = 1.0
        assert ready.status == replica_managers.serve_state.ReplicaStatus.READY

        fence = (1, 8, 1, (('A100', 1),), (('A100', 1),))
        mgr._logical_target = fence
        mgr._logical_reconcile_snapshot = (
            replica_managers.LogicalReconcileSnapshot(
                version=1,
                generation=8,
                observed_slots_by_replica_id={2: 1},
                in_flight_by_replica_id={},
                unknown_replica_ids=frozenset(),
                received_at=replica_managers.time.monotonic()))
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[ready, recovered]):
            applicable, target_fence, authorized = (
                mgr._logical_pending_launch_admission())

        assert applicable
        assert target_fence == fence
        assert authorized == set()

    def test_reentry_with_enqueued_threads_is_tolerated(self):
        mgr = _make_manager(next_replica_id=1)
        mgr._launch_thread_pool = {7: mock.Mock()}
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=[]):
            # Previously an assert; on a retry pass this must not raise.
            mgr._recover_replica_operations()


class TestRecoverySingleSnapshot:
    """The recovery pass must read the replica table exactly once.

    It previously fetched (and unpickled) the whole table three times: once
    for the snapshot and once per `get_replicas_at_status(PROVISIONING /
    PENDING)` call. Beyond the wasted O(3 x rows) work at fleet scale, the
    reads could diverge: the re-drive list, the id-allocator seed, and the
    `existing_replica_infos` snapshot passed to `_launch_replica` must all
    describe the same durable state.
    """

    @staticmethod
    def _statuses(*statuses):
        return [
            _fake_replica_info(i + 1, status=status)
            for i, status in enumerate(statuses)
        ]

    def test_replica_table_is_read_exactly_once(self):
        mgr = _make_manager()
        infos = self._statuses(
            replica_managers.serve_state.ReplicaStatus.PROVISIONING,
            replica_managers.serve_state.ReplicaStatus.PENDING,
            replica_managers.serve_state.ReplicaStatus.READY,
        )
        for info in infos:
            info.resources_override = None
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos) as scan, \
             mock.patch.object(mgr, '_launch_replica'):
            mgr._recover_replica_operations()
        assert scan.call_count == 1

    def test_provisioning_redriven_before_pending(self):
        # PROVISIONING replicas were previously launched and may hold live
        # cloud resources; they must win the bounded launch queue over
        # PENDING ones regardless of row order.
        mgr = _make_manager()
        infos = self._statuses(
            replica_managers.serve_state.ReplicaStatus.PENDING,
            replica_managers.serve_state.ReplicaStatus.PROVISIONING,
            replica_managers.serve_state.ReplicaStatus.PENDING,
            replica_managers.serve_state.ReplicaStatus.PROVISIONING,
        )
        for info in infos:
            info.resources_override = None
        launched = []
        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos), \
             mock.patch.object(
                 mgr, '_launch_replica',
                 side_effect=lambda replica_id, **_: launched.append(
                     replica_id)):
            mgr._recover_replica_operations()
        assert launched == [2, 4, 1, 3]

    def test_launch_redrive_reuses_the_snapshot(self):
        # `existing_replica_infos` handed to each re-driven launch must be
        # the same object as the recovery snapshot (no per-launch re-scan).
        mgr = _make_manager()
        infos = self._statuses(
            replica_managers.serve_state.ReplicaStatus.PROVISIONING)
        infos[0].resources_override = None
        seen_snapshots = []
        seen_recovery_modes = []

        def _launch(_replica_id,
                    existing_replica_infos=None,
                    recovering_existing_replica=False,
                    **_kwargs):
            seen_snapshots.append(existing_replica_infos)
            seen_recovery_modes.append(recovering_existing_replica)

        with mock.patch(
                'sky.serve.replica_managers.serve_state.get_replica_infos',
                return_value=infos), \
             mock.patch.object(mgr, '_launch_replica', side_effect=_launch):
            mgr._recover_replica_operations()
        assert seen_snapshots == [infos]
        assert seen_snapshots[0] is infos
        assert seen_recovery_modes == [True]


class TestRefreshThreadPoolUnfencedLaunch:
    """`_refresh_thread_pool` must convert an unfenced external-LB launch
    failure into one unrecoverable replica and keep that control-plane
    failure out of spot-placement evidence.

    This guards the manager-side half of the fix in PR #524: the client-side
    pre-check raises `_UnfencedExternalLbLaunchError`, but only this pass turns
    it into `user_app_failed` (so the autoscaler stops appending rows) and
    excludes it from `failed_spot_locations` / `failed_spot_availability` (so a
    missing owner fence does not bench an otherwise-usable location). Generic
    launch failures are also not availability evidence.
    """

    def test_empty_refresh_does_not_touch_paid_capacity_authority(self):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager.latest_version = 1
        manager._is_pool = False
        manager.lock = threading.Lock()
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        manager._spot_placer = None

        with mock.patch.object(
                manager, '_reconcile_legacy_uncertain_logical_retirements'), \
             mock.patch.object(
                 manager, '_reconcile_recovering_logical_retirements'), \
             mock.patch.object(manager, '_refresh_wait_for_idle'), \
             mock.patch.object(
                 manager, '_clear_known_unknown_capacity_replacements'), \
             mock.patch(
                 'sky.serve.replica_managers.serve_state'
                 '.get_replica_infos_from_ids',
                 return_value={}), \
             mock.patch.object(
                 replica_managers.paid_capacity,
                 'persist_completed_launches') as persist_paid_outcomes:
            manager._refresh_thread_pool()

        persist_paid_outcomes.assert_not_called()

    def _run(self,
             thread_exception,
             *,
             paid_pool_key=None,
             paid_persist_result=None):
        replica_id = 7
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager.latest_version = 1
        manager._is_pool = False
        manager.lock = threading.Lock()
        manager._launch_thread_pool = thread_utils.ThreadSafeDict()
        manager._down_thread_pool = thread_utils.ThreadSafeDict()
        manager._replica_to_request_id = thread_utils.ThreadSafeDict()
        manager._replica_to_launch_cancelled = thread_utils.ThreadSafeDict()
        manager._scale_reconciliation_event = mock.Mock(spec=threading.Event)

        launch_thread = mock.Mock()
        launch_thread.is_alive.return_value = False
        launch_thread.format_exc = 'boom traceback'
        launch_thread.exception = thread_exception
        manager._launch_thread_pool[replica_id] = launch_thread
        manager._replica_to_request_id[replica_id] = 'req'

        location = mock.Mock(name='location')
        placer = mock.Mock()
        placer.resolve_location.return_value = location
        manager._spot_placer = placer

        info = mock.Mock()
        info.status = replica_managers.serve_state.ReplicaStatus.PROVISIONING
        info.status_property = replica_managers.ReplicaStatusProperty()
        info.get_spot_location.return_value = location
        info.created_at = 100.0
        info.paid_capacity_pool_key = paid_pool_key

        persisted = []
        terminated = []
        events = []

        def _persist(updates):
            persisted.extend(updates)

        def _persist_paid_outcomes(**_kwargs):
            events.append('persist_outcome')
            return paid_persist_result

        def _wake_reconciliation():
            events.append('wake')

        def _terminate(rid, **_kwargs):
            events.append('terminate')
            terminated.append(rid)

        manager._scale_reconciliation_event.set.side_effect = (
            _wake_reconciliation)
        refresh_error = None
        try:
            with mock.patch.object(
                    manager,
                    '_reconcile_legacy_uncertain_logical_retirements'), \
                 mock.patch.object(
                     manager, '_reconcile_recovering_logical_retirements'), \
                 mock.patch.object(manager, '_refresh_wait_for_idle'), \
                 mock.patch.object(
                     manager, '_clear_known_unknown_capacity_replacements'), \
                 mock.patch.object(
                     manager, '_persist_replicas',
                     side_effect=_persist), \
                 mock.patch.object(
                     replica_managers.paid_capacity,
                     'persist_completed_launches',
                     side_effect=_persist_paid_outcomes) as persist_paid_outcomes, \
                 mock.patch.object(
                     manager, '_terminate_replica',
                     side_effect=_terminate), \
                 mock.patch.object(manager, '_reconcile_failed_cleanup'), \
                 mock.patch(
                     'sky.serve.replica_managers.serve_state.get_replica_infos',
                     return_value=[]), \
                 mock.patch(
                     'sky.serve.replica_managers.serve_state'
                     '.get_replica_infos_from_ids',
                     return_value={replica_id: info}), \
                 mock.patch.object(replica_managers.logger,
                                   'warning') as warning:
                manager._refresh_thread_pool()
        except RuntimeError as e:
            refresh_error = e

        return (info, placer, terminated, persist_paid_outcomes, warning,
                events, manager._scale_reconciliation_event, refresh_error)

    def test_unfenced_failure_is_unrecoverable_and_not_benched(self):
        (info, placer, terminated, persist_paid_outcomes, _, events,
         reconciliation_event, refresh_error) = self._run(
             replica_managers._UnfencedExternalLbLaunchError('no fence'))
        # Unrecoverable so the autoscaler stops recreating replica rows.
        assert info.status_property.user_app_failed is True
        assert info.status_property.unrecoverable_failure() is True
        # A missing owner fence is a control-plane failure, not a location
        # problem: the location must not be benched.
        placer.set_preemptive.assert_not_called()
        assert info.status_property.failed_spot_availability is False
        assert persist_paid_outcomes.call_args.kwargs['outcomes'] == {
            7: replica_managers.paid_capacity.LaunchOutcome.OTHER_FAILURE
        }
        assert terminated == [7]
        assert events == ['persist_outcome', 'terminate']
        reconciliation_event.set.assert_not_called()
        assert refresh_error is None

    def test_generic_failure_does_not_bench_location(self):
        (info, placer, terminated, persist_paid_outcomes, _, events,
         reconciliation_event,
         refresh_error) = self._run(RuntimeError('transient'))
        # An ordinary launch failure remains recoverable but says nothing
        # about provider inventory.
        assert info.status_property.user_app_failed is False
        placer.set_preemptive.assert_not_called()
        placer.release_retry.assert_called_once_with(mock.ANY)
        assert info.status_property.failed_spot_availability is False
        assert persist_paid_outcomes.call_args.kwargs['outcomes'] == {
            7: replica_managers.paid_capacity.LaunchOutcome.OTHER_FAILURE
        }
        assert terminated == [7]
        assert events == ['persist_outcome', 'terminate']
        reconciliation_event.set.assert_not_called()
        assert refresh_error is None

    def test_typed_capacity_failure_resets_shared_pool_evidence(self):
        (info, placer, terminated, persist_paid_outcomes, warning, events,
         reconciliation_event, refresh_error) = self._run(
             replica_managers._ReplicaLaunchCapacityError('exhausted',
                                                          reason='capacity'))
        assert info.status_property.user_app_failed is False
        placer.set_preemptive.assert_called_once_with(mock.ANY,
                                                      reason='capacity')
        assert info.status_property.failed_spot_availability is True
        assert persist_paid_outcomes.call_args.kwargs['outcomes'] == {
            7: replica_managers.paid_capacity.LaunchOutcome.CAPACITY_FAILURE
        }
        warning.assert_called_once()
        message = warning.call_args.args[0]
        assert ('failure wave: capacity_failures=1, quota_failures=0, '
                'exact_pools=unknown') in message
        assert 'boom traceback' not in message
        assert terminated == [7]
        assert events == ['persist_outcome', 'terminate']
        reconciliation_event.set.assert_not_called()
        assert refresh_error is None

    def test_typed_quota_failure_uses_regional_bench(self):
        (info, placer, terminated, persist_paid_outcomes, warning, events,
         reconciliation_event, refresh_error) = self._run(
             replica_managers._ReplicaLaunchCapacityError('quota exhausted',
                                                          reason='quota'))

        placer.set_quota_limited.assert_called_once_with(mock.ANY)
        placer.set_preemptive.assert_not_called()
        assert info.status_property.failed_spot_availability is True
        assert persist_paid_outcomes.call_args.kwargs['outcomes'] == {
            7: replica_managers.paid_capacity.LaunchOutcome.QUOTA_FAILURE
        }
        warning.assert_called_once()
        assert terminated == [7]
        assert events == ['persist_outcome', 'terminate']
        reconciliation_event.set.assert_not_called()
        assert refresh_error is None

    @pytest.mark.parametrize('reason', ['capacity', 'quota'])
    def test_committed_typed_paid_failure_wakes_after_persistence(self, reason):
        pool_key = _canonical_paid_pool_key()
        (info, _, terminated, _, warning, events, reconciliation_event,
         refresh_error) = self._run(
             replica_managers._ReplicaLaunchCapacityError('unavailable',
                                                          reason=reason),
             paid_pool_key=pool_key,
             paid_persist_result=paid_capacity.CompletedLaunchPersistence(
                 ownership_valid=True, applied_pool_keys=frozenset({pool_key})))

        assert info.paid_capacity_pool_key == pool_key
        warning.assert_called_once()
        assert 'exact_pools=1' in warning.call_args.args[0]
        reconciliation_event.set.assert_called_once_with()
        assert terminated == [7]
        assert events == ['persist_outcome', 'wake', 'terminate']
        assert refresh_error is None

    @pytest.mark.parametrize(
        ('thread_exception', 'paid_pool_key', 'paid_persist_result'), [
            (RuntimeError('generic'), 'paid-pool',
             paid_capacity.CompletedLaunchPersistence(
                 ownership_valid=True,
                 applied_pool_keys=frozenset({'paid-pool'}))),
            (replica_managers._ReplicaLaunchCapacityError(
                'unpaid capacity', reason='capacity'), None,
             paid_capacity.CompletedLaunchPersistence(ownership_valid=True)),
            (replica_managers._ReplicaLaunchCapacityError(
                'legacy capacity', reason='capacity'), 'paid-pool', None),
        ],
        ids=['generic', 'unpaid', 'legacy-persistence'])
    def test_non_authoritative_outcome_does_not_wake(self, thread_exception,
                                                     paid_pool_key,
                                                     paid_persist_result):
        (_, _, terminated, _, _, events, reconciliation_event,
         refresh_error) = self._run(thread_exception,
                                    paid_pool_key=paid_pool_key,
                                    paid_persist_result=paid_persist_result)

        reconciliation_event.set.assert_not_called()
        assert terminated == [7]
        assert events == ['persist_outcome', 'terminate']
        assert refresh_error is None

    @pytest.mark.parametrize(
        ('paid_pool_key', 'applied_pool_keys'), [
            ('paid-pool', frozenset({'paid-pool'})),
            (_canonical_paid_pool_key(), frozenset()),
            (_canonical_paid_pool_key(),
             frozenset({_canonical_paid_pool_key('us-west-2')})),
        ],
        ids=['malformed-identity', 'missing-claim', 'different-claim'])
    def test_unapplied_or_noncanonical_paid_failure_does_not_wake(
            self, paid_pool_key, applied_pool_keys):
        (_, _, terminated, _, _, events, reconciliation_event,
         refresh_error) = self._run(
             replica_managers._ReplicaLaunchCapacityError('unavailable',
                                                          reason='capacity'),
             paid_pool_key=paid_pool_key,
             paid_persist_result=paid_capacity.CompletedLaunchPersistence(
                 ownership_valid=True, applied_pool_keys=applied_pool_keys))

        reconciliation_event.set.assert_not_called()
        assert terminated == [7]
        assert events == ['persist_outcome', 'terminate']
        assert refresh_error is None

    def test_failed_paid_outcome_persistence_does_not_wake(self):
        (_, _, terminated, _, _, events, reconciliation_event,
         refresh_error) = self._run(
             replica_managers._ReplicaLaunchCapacityError('ownership changed',
                                                          reason='capacity'),
             paid_pool_key='paid-pool',
             paid_persist_result=paid_capacity.CompletedLaunchPersistence(
                 ownership_valid=False))

        reconciliation_event.set.assert_not_called()
        assert not terminated
        assert events == ['persist_outcome']
        assert isinstance(refresh_error, RuntimeError)
        assert 'ownership changed' in str(refresh_error)
