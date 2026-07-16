"""Tests for sky/serve/controller.py.

Currently focused on `SkyServeController._get_lb_replica_info`, which builds
the `/controller/load_balancer_sync` response. Resolving a replica's url and
gpu_type is expensive (cluster handle fetch + endpoint query), so both must
be resolved at most once per replica lifetime and cached; the cache must be
pruned when a replica leaves the ready set.
"""
# pylint: disable=missing-class-docstring,protected-access
import asyncio
import json
import threading
import types
from typing import Dict, Optional
from unittest import mock

import pytest

from sky.serve import controller
from sky.serve import serve_state
from sky.serve import serve_utils


def test_run_controller_preserves_authoritative_launch_fence_bit(monkeypatch):
    """The child override must not turn a remote-DB fence into a local one."""
    monkeypatch.delenv(controller.constants.OVERRIDE_CONSOLIDATION_MODE,
                       raising=False)
    controller_instance = mock.Mock()
    constructor = mock.Mock(return_value=controller_instance)
    monkeypatch.setattr(controller, 'SkyServeController', constructor)
    monkeypatch.setattr(controller.context_utils, 'hijack_sys_attrs',
                        mock.Mock())

    controller.run_controller('pool', mock.Mock(), 1, '127.0.0.1', 20001,
                              'fingerprint', None, 'incarnation-a', 123,
                              '10.0.0.1', False)

    assert constructor.call_args.args[-1] is False
    controller_instance.run.assert_called_once_with()


class _FakeHandle:
    """Stub for the resource handle returned by ReplicaInfo.handle()."""

    def __init__(self, accelerators: Optional[Dict[str, int]],
                 cluster_yaml: str) -> None:
        self.launched_resources = mock.Mock()
        self.launched_resources.accelerators = accelerators
        self.cluster_yaml = cluster_yaml


class _FakeReplicaInfo:
    """ReplicaInfo stub that counts expensive url/handle resolutions."""

    def __init__(self,
                 replica_id: int,
                 status: serve_state.ReplicaStatus,
                 version: int = 1,
                 url: Optional[str] = None,
                 accelerators: Optional[Dict[str, int]] = None,
                 handle_is_none: bool = False) -> None:
        self.replica_id = replica_id
        self.cluster_name = f'replica-{replica_id}'
        self.status = status
        self.version = version
        self._url = url
        self._accelerators = accelerators
        self._handle_is_none = handle_is_none
        self.url_resolutions = 0
        self.handle_resolutions = 0
        self.last_provider_config = None

    @property
    def url(self) -> Optional[str]:
        self.url_resolutions += 1
        return self._url

    @property
    def is_ready(self) -> bool:
        return self.status == serve_state.ReplicaStatus.READY

    @property
    def is_terminal(self) -> bool:
        return self.status in serve_state.ReplicaStatus.terminal_statuses()

    def handle(self, cluster_record=None) -> Optional[_FakeHandle]:
        del cluster_record
        self.handle_resolutions += 1
        if self._handle_is_none:
            return None
        return _FakeHandle(self._accelerators, f'{self.cluster_name}.yaml')

    def _resolve_url(self,
                     cluster_record=None,
                     handle=None,
                     provider_config=None) -> Optional[str]:
        del cluster_record, handle
        self.last_provider_config = provider_config
        self.url_resolutions += 1
        return self._url


def _make_controller() -> controller.SkyServeController:
    # Bypass __init__: it builds a real replica manager and autoscaler.
    ctrl = controller.SkyServeController.__new__(controller.SkyServeController)
    ctrl._service_name = 'svc'  # pylint: disable=protected-access
    ctrl._resource_scope = None  # pylint: disable=protected-access
    ctrl._lb_replica_cache = {}  # pylint: disable=protected-access
    ctrl._lb_translation_cache = {}  # pylint: disable=protected-access
    ctrl._lb_sync_lock = None  # pylint: disable=protected-access
    ctrl._routing_spec = None  # pylint: disable=protected-access
    ctrl._reserved_capacity_fill_enabled = False  # pylint: disable=protected-access
    return ctrl


def test_recovery_rejects_physical_spec_after_durable_logical_activation():
    physical = mock.MagicMock()
    physical.uses_logical_replicas = False
    with mock.patch.object(
            controller.serve_state,
            'service_uses_logical_replica_semantics',
            return_value=True), \
         mock.patch.object(controller.replica_managers,
                           'SkyPilotReplicaManager') as manager, \
         pytest.raises(RuntimeError, match='activation fence disagrees'):
        controller.SkyServeController('svc',
                                      physical,
                                      version=3,
                                      host='localhost',
                                      port=8000,
                                      controller_owner_fingerprint='owner-a',
                                      service_hash='incarnation-a')
    manager.assert_not_called()


class _FakeSpec:
    """Minimal SkyServiceSpec stub exposing the routing-spec properties."""

    def __init__(self,
                 load_balancing_policy,
                 target_qps_per_replica,
                 lb_stream_timeout_seconds,
                 lb_retriable_status_codes=None,
                 lb_max_retries=None,
                 lb_retry_initial_backoff_seconds=None) -> None:
        self.load_balancing_policy = load_balancing_policy
        self.target_qps_per_replica = target_qps_per_replica
        self.lb_stream_timeout_seconds = lb_stream_timeout_seconds
        self.lb_retriable_status_codes = lb_retriable_status_codes
        self.lb_max_retries = lb_max_retries
        self.lb_retry_initial_backoff_seconds = (
            lb_retry_initial_backoff_seconds)


class TestGetRoutingSpec:
    """The load_balancer_sync response ships the routing config so a running
    external LB picks up `sky serve update` changes without a re-roll."""

    def test_routing_spec_sourced_from_controller_memory(self):
        ctrl = _make_controller()
        spec = _FakeSpec(load_balancing_policy='instance_aware_least_load',
                         target_qps_per_replica={'L4': 2.5},
                         lb_stream_timeout_seconds=120,
                         lb_retriable_status_codes=[503],
                         lb_max_retries=3,
                         lb_retry_initial_backoff_seconds=0.5)
        ctrl._routing_spec = ctrl._build_routing_spec(spec)  # pylint: disable=protected-access
        routing_spec = ctrl._get_routing_spec()  # pylint: disable=protected-access
        assert routing_spec == {
            'load_balancing_policy_name': 'instance_aware_least_load',
            'target_qps_per_replica': {
                'L4': 2.5
            },
            # _FakeSpec has no concurrency knob; getattr resolves None.
            'target_concurrency_per_replica': None,
            'stream_timeout_seconds': 120,
            'retriable_status_codes': [503],
            'max_retries': 3,
            'retry_initial_backoff_seconds': 0.5,
            'request_queue': None,
        }

    def test_routing_spec_repeated_calls_do_not_hit_db(self):
        ctrl = _make_controller()
        ctrl._routing_spec = ctrl._build_routing_spec(  # pylint: disable=protected-access
            _FakeSpec(load_balancing_policy='round_robin',
                      target_qps_per_replica=None,
                      lb_stream_timeout_seconds=30))
        with mock.patch.object(controller.serve_state,
                               'get_service_from_name') as get_service, \
             mock.patch.object(controller.serve_state, 'get_spec') as get_spec:
            assert ctrl._get_routing_spec() is not None  # pylint: disable=protected-access
            assert ctrl._get_routing_spec() is not None  # pylint: disable=protected-access
        get_service.assert_not_called()
        get_spec.assert_not_called()

    def test_routing_spec_none_when_uninitialized(self):
        ctrl = _make_controller()
        assert ctrl._get_routing_spec() is None  # pylint: disable=protected-access

    def test_apply_service_update_keeps_old_spec_until_runtime_transition(self):
        ctrl = _make_controller()
        old_spec = _FakeSpec(load_balancing_policy='round_robin',
                             target_qps_per_replica=None,
                             lb_stream_timeout_seconds=30)
        new_spec = _FakeSpec(load_balancing_policy='instance_aware_least_load',
                             target_qps_per_replica={'L4': 2.5},
                             lb_stream_timeout_seconds=90)
        ctrl._routing_spec = ctrl._build_routing_spec(old_spec)  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._autoscaler = mock.MagicMock()  # pylint: disable=protected-access
        ctrl._seed_fill_zero_cost_locations = mock.Mock()  # pylint: disable=protected-access
        ctrl._start_reserved_capacity_poller_if_needed = mock.Mock()  # pylint: disable=protected-access

        entered_runtime_transition = threading.Event()
        resume_runtime_transition = threading.Event()

        def _block_runtime_transition(*_args, **_kwargs):
            entered_runtime_transition.set()
            assert resume_runtime_transition.wait(timeout=5)

        ctrl._replica_manager.update_version.side_effect = (  # pylint: disable=protected-access
            _block_runtime_transition)

        new_autoscaler = mock.MagicMock()
        with mock.patch.object(controller.serve_state,
                               'get_service_from_name',
                               return_value={'version': 2}), \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=new_spec), \
             mock.patch.object(controller.autoscalers.Autoscaler,
                               'from_spec',
                               return_value=new_autoscaler):
            updater = threading.Thread(target=ctrl._apply_service_update,
                                       args=(2, new_spec, mock.sentinel.mode))
            updater.start()
            assert entered_runtime_transition.wait(timeout=5)
            ctrl._replica_manager.notify_version_pending.assert_called_once_with(  # pylint: disable=line-too-long
                2)
            ctrl._replica_manager.clear_pending_version.assert_not_called()
            # The DB already points at the new version, but the controller has
            # not finished applying it locally yet. Syncs must keep serving the
            # old routing spec until the runtime transition completes.
            assert ctrl._get_routing_spec() == {  # pylint: disable=protected-access
                'load_balancing_policy_name': 'round_robin',
                'target_qps_per_replica': None,
                'target_concurrency_per_replica': None,
                'stream_timeout_seconds': 30,
                'retriable_status_codes': None,
                'max_retries': None,
                'retry_initial_backoff_seconds': None,
                'request_queue': None,
            }
            resume_runtime_transition.set()
            updater.join(timeout=5)

        assert not updater.is_alive()
        ctrl._replica_manager.clear_pending_version.assert_called_once_with(  # pylint: disable=line-too-long
            2)
        assert ctrl._get_routing_spec() == {  # pylint: disable=protected-access
            'load_balancing_policy_name': 'instance_aware_least_load',
            'target_qps_per_replica': {
                'L4': 2.5
            },
            'target_concurrency_per_replica': None,
            'stream_timeout_seconds': 90,
            'retriable_status_codes': None,
            'max_retries': None,
            'retry_initial_backoff_seconds': None,
            'request_queue': None,
        }


def _make_update_controller() -> controller.SkyServeController:
    ctrl = _make_controller()
    ctrl._service_hash = 'incarnation-a'  # pylint: disable=protected-access
    ctrl._controller_owner = (123, '10.0.0.1')  # pylint: disable=protected-access
    ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
    ctrl._update_condition = threading.Condition()  # pylint: disable=protected-access
    ctrl._pending_update = None  # pylint: disable=protected-access
    ctrl._committed_version = 1  # pylint: disable=protected-access
    ctrl._applied_version = 1  # pylint: disable=protected-access
    ctrl._update_apply_error = None  # pylint: disable=protected-access
    ctrl._update_apply_failures = 0  # pylint: disable=protected-access
    ctrl._update_still_authorized = mock.Mock(  # pylint: disable=protected-access
        return_value=True)
    return ctrl


class TestServiceUpdateReconciler:

    def test_per_gpu_service_rejects_update_to_physical_semantics(self):
        ctrl = _make_update_controller()
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='logical')
        legacy_spec = types.SimpleNamespace(replica_unit='physical_backend')

        with mock.patch.object(controller.serve_state,
                               'add_or_update_version') as commit:
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, legacy_spec, 'service: changed',
                serve_utils.UpdateMode.ROLLING, 'incarnation-a', 7)

        assert response.status_code == 400
        assert 'dynamic_fallback_per_gpu' in json.loads(
            response.body)['message']
        commit.assert_not_called()

    def test_logical_service_rejects_blue_green_update(self):
        ctrl = _make_update_controller()
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='logical')
        logical_spec = types.SimpleNamespace(replica_unit='logical')

        with mock.patch.object(controller.serve_state,
                               'add_or_update_version') as commit:
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, logical_spec, 'service: changed',
                serve_utils.UpdateMode.BLUE_GREEN, 'incarnation-a', 7)

        assert response.status_code == 400
        assert 'rolling updates' in json.loads(response.body)['message']
        commit.assert_not_called()

    def test_content_conflict_returns_409_without_scheduling(self):
        ctrl = _make_update_controller()
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access
        with mock.patch.object(controller.serve_state,
                               'add_or_update_version',
                               return_value=serve_state.VersionCommitResult.
                               CONTENT_CONFLICT) as commit:
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, mock.sentinel.spec, 'service: changed',
                serve_utils.UpdateMode.ROLLING, 'incarnation-a', 7)

        assert response.status_code == 409
        assert 'already committed with different content' in json.loads(
            response.body)['message']
        ctrl._record_committed_update.assert_not_called()  # pylint: disable=protected-access
        commit.assert_called_once_with('svc',
                                       2,
                                       mock.sentinel.spec,
                                       'service: changed',
                                       expected_service_hash='incarnation-a',
                                       expected_lifecycle_epoch=7,
                                       expected_controller_owner=(123,
                                                                  '10.0.0.1'))

    def test_stale_version_returns_409_without_scheduling(self):
        ctrl = _make_update_controller()
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access
        with mock.patch.object(
                controller.serve_state,
                'add_or_update_version',
                return_value=serve_state.VersionCommitResult.STALE_VERSION):
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, mock.sentinel.spec, 'service: stale',
                serve_utils.UpdateMode.ROLLING, 'incarnation-a', 7)

        assert response.status_code == 409
        assert 'superseded' in json.loads(response.body)['message']
        ctrl._record_committed_update.assert_not_called()  # pylint: disable=protected-access

    def test_durable_activation_fence_rejects_racing_physical_commit(self):
        ctrl = _make_update_controller()
        # Runtime is still physical while a previously committed logical
        # version is waiting for the manager lock. The durable parent-row
        # fence, not the currently published autoscaler, must reject v3.
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='physical_backend')
        physical = types.SimpleNamespace(replica_unit='physical_backend',
                                         uses_logical_replicas=False)
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access
        with mock.patch.object(controller.serve_state,
                               'add_or_update_version',
                               return_value=serve_state.VersionCommitResult.
                               SEMANTIC_CONFLICT) as commit:
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                3, physical, 'service: physical',
                serve_utils.UpdateMode.ROLLING, 'incarnation-a', 7)

        assert response.status_code == 400
        assert 'dynamic_fallback_per_gpu' in json.loads(
            response.body)['message']
        commit.assert_called_once()
        ctrl._record_committed_update.assert_not_called()  # pylint: disable=protected-access

    def test_apply_revalidates_logical_to_physical_transition(self):
        ctrl = _make_update_controller()
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='logical')
        physical = types.SimpleNamespace(uses_logical_replicas=False)

        with pytest.raises(ValueError, match='Refusing to apply'):
            ctrl._apply_service_update(  # pylint: disable=protected-access
                3, physical, serve_utils.UpdateMode.ROLLING)

        ctrl._replica_manager.update_version.assert_not_called()  # pylint: disable=protected-access

    def test_multi_node_logical_update_is_rejected_before_db_commit(self):
        ctrl = _make_update_controller()
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='physical_backend')
        logical = types.SimpleNamespace(replica_unit='logical',
                                        uses_logical_replicas=True)
        update_task = types.SimpleNamespace(service=logical, num_nodes=2)

        with mock.patch.object(controller.task_lib.Task,
                               'from_yaml_str',
                               return_value=update_task), \
             mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value=None), \
             mock.patch.object(controller.serve_state,
                               'add_or_update_version') as commit:
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, logical, 'service: logical', serve_utils.UpdateMode.ROLLING,
                'incarnation-a', 7)

        assert response.status_code == 400
        assert 'only single-node services' in json.loads(
            response.body)['message']
        commit.assert_not_called()

    def test_legacy_per_gpu_retry_applies_authoritative_physical_spec(self):
        ctrl = _make_update_controller()
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='physical_backend')
        caller = types.SimpleNamespace(replica_unit='logical',
                                       uses_logical_replicas=True)
        persisted = types.SimpleNamespace(replica_unit='physical_backend',
                                          uses_logical_replicas=False)
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value='service: same-legacy-yaml'), \
             mock.patch.object(
                 controller.serve_state,
                 'add_or_update_version',
                 return_value=serve_state.VersionCommitResult.
                 IDEMPOTENT_RETRY), \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=persisted) as get_spec:
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                1, caller, 'service: same-legacy-yaml',
                serve_utils.UpdateMode.BLUE_GREEN, 'incarnation-a', 7)

        assert response.status_code == 200
        get_spec.assert_called_once_with('svc', 1)
        ctrl._record_committed_update.assert_called_once_with(  # pylint: disable=protected-access
            1, persisted, serve_utils.UpdateMode.BLUE_GREEN)

    def test_exact_legacy_yaml_is_loaded_before_current_parser(self):
        ctrl = _make_update_controller()
        persisted = types.SimpleNamespace(replica_unit='physical_backend',
                                          uses_logical_replicas=False)
        with mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value='old-invalid-yaml'), \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=persisted), \
             mock.patch.object(
                 controller.serve.SkyServiceSpec,
                 'from_yaml_str',
                 side_effect=AssertionError('current parser must not run')):
            loaded = ctrl._load_service_for_update(  # pylint: disable=protected-access
                2, 'old-invalid-yaml')

        assert loaded is persisted

    def test_older_physical_retry_is_idempotent_after_logical_activation(self):
        ctrl = _make_update_controller()
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='logical')
        ctrl._committed_version = 3  # pylint: disable=protected-access
        ctrl._applied_version = 3  # pylint: disable=protected-access
        persisted = types.SimpleNamespace(replica_unit='physical_backend',
                                          uses_logical_replicas=False)

        with mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value='service: physical-v2'), \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=persisted), \
             mock.patch.object(
                 controller.serve_state,
                 'add_or_update_version',
                 return_value=serve_state.VersionCommitResult.
                 IDEMPOTENT_RETRY):
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, persisted, 'service: physical-v2',
                serve_utils.UpdateMode.ROLLING, 'incarnation-a', 7)

        assert response.status_code == 200
        assert ctrl._pending_update is None  # pylint: disable=protected-access
        assert ctrl._applied_version == 3  # pylint: disable=protected-access
        ctrl._replica_manager.notify_version_pending.assert_not_called()  # pylint: disable=protected-access

    def test_exact_logical_retry_skips_current_topology_parser(self):
        ctrl = _make_update_controller()
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='logical')
        persisted = types.SimpleNamespace(replica_unit='logical',
                                          uses_logical_replicas=True)
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value='old-logical-yaml'), \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=persisted), \
             mock.patch.object(
                 controller.task_lib.Task,
                 'from_yaml_str',
                 side_effect=AssertionError('current parser must not run')), \
             mock.patch.object(
                 controller.serve_state,
                 'add_or_update_version',
                 return_value=serve_state.VersionCommitResult.
                 IDEMPOTENT_RETRY):
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, persisted, 'old-logical-yaml',
                serve_utils.UpdateMode.ROLLING, 'incarnation-a', 7)

        assert response.status_code == 200
        ctrl._record_committed_update.assert_called_once_with(  # pylint: disable=protected-access
            2, persisted, serve_utils.UpdateMode.ROLLING)

    def test_second_commit_does_not_wait_for_first_apply(self):
        ctrl = _make_update_controller()
        first_apply_started = threading.Event()
        release_first_apply = threading.Event()
        applied_versions = []

        def _apply(version, *_args):
            applied_versions.append(version)
            if version == 2:
                first_apply_started.set()
                assert release_first_apply.wait(timeout=5)

        ctrl._apply_service_update = mock.Mock(  # pylint: disable=protected-access
            side_effect=_apply)
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING)
        worker = threading.Thread(target=ctrl._reconcile_pending_update_once)  # pylint: disable=protected-access
        worker.start()
        assert first_apply_started.wait(timeout=5)

        # Regression: the old handler held _update_lock through the blocked
        # apply, so this second durable commit could not be recorded.
        ctrl._record_committed_update(  # pylint: disable=protected-access
            3, mock.sentinel.spec_v3, serve_utils.UpdateMode.ROLLING)
        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['committed_version'] == 3
        assert status['applied_version'] == 1
        assert status['update_apply_pending']

        release_first_apply.set()
        worker.join(timeout=5)
        assert not worker.is_alive()
        assert ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access
        assert applied_versions == [2, 3]
        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['committed_version'] == 3
        assert status['applied_version'] == 3
        assert not status['update_apply_pending']

    def test_commits_coalesce_before_apply(self):
        ctrl = _make_update_controller()
        ctrl._apply_service_update = mock.Mock()  # pylint: disable=protected-access
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING)
        ctrl._record_committed_update(  # pylint: disable=protected-access
            3, mock.sentinel.spec_v3, serve_utils.UpdateMode.ROLLING)

        assert ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access
        ctrl._apply_service_update.assert_called_once_with(  # pylint: disable=line-too-long,protected-access
            3, mock.sentinel.spec_v3, serve_utils.UpdateMode.ROLLING)
        assert ctrl._get_update_status()['applied_version'] == 3  # pylint: disable=protected-access

    def test_duplicate_commit_does_not_replace_in_flight_update(self):
        ctrl = _make_update_controller()
        ctrl._apply_service_update = mock.Mock()  # pylint: disable=protected-access
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.original_spec, serve_utils.UpdateMode.ROLLING)
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.retry_spec, serve_utils.UpdateMode.ROLLING)

        assert ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access
        ctrl._apply_service_update.assert_called_once_with(  # pylint: disable=line-too-long,protected-access
            2, mock.sentinel.original_spec, serve_utils.UpdateMode.ROLLING)
        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['applied_version'] == 2
        assert not status['update_apply_pending']

    def test_failed_apply_retries_same_committed_version(self):
        ctrl = _make_update_controller()
        ctrl._apply_service_update = mock.Mock(  # pylint: disable=protected-access
            side_effect=[RuntimeError('transient failure'), None])
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING)

        assert not ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access
        failed_status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert failed_status['applied_version'] == 1
        assert failed_status['update_apply_pending']
        assert failed_status['update_apply_failures'] == 1
        assert 'transient failure' in failed_status['update_apply_error']

        assert ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access
        recovered_status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert recovered_status['applied_version'] == 2
        assert not recovered_status['update_apply_pending']
        assert recovered_status['update_apply_error'] is None

    def test_newer_commit_resets_previous_apply_failure(self):
        ctrl = _make_update_controller()
        ctrl._apply_service_update = mock.Mock(  # pylint: disable=protected-access
            side_effect=RuntimeError('transient failure'))
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING)
        assert not ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access

        ctrl._record_committed_update(  # pylint: disable=protected-access
            3, mock.sentinel.spec_v3, serve_utils.UpdateMode.ROLLING)
        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['committed_version'] == 3
        assert status['update_apply_error'] is None
        assert status['update_apply_failures'] == 0

    def test_superseded_apply_failure_does_not_taint_newer_version(self):
        ctrl = _make_update_controller()

        def _fail_after_newer_commit(*_args):
            ctrl._record_committed_update(  # pylint: disable=protected-access
                3, mock.sentinel.spec_v3, serve_utils.UpdateMode.ROLLING)
            raise RuntimeError('superseded failure')

        ctrl._apply_service_update = mock.Mock(  # pylint: disable=protected-access
            side_effect=_fail_after_newer_commit)
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING)

        assert not ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access
        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['committed_version'] == 3
        assert status['applied_version'] == 1
        assert status['update_apply_pending']
        assert status['update_apply_error'] is None
        assert status['update_apply_failures'] == 0

    def test_terminal_service_drops_pending_apply(self):
        ctrl = _make_update_controller()
        ctrl._update_still_authorized.return_value = False  # pylint: disable=protected-access
        ctrl._apply_service_update = mock.Mock()  # pylint: disable=protected-access
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING)

        assert ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access
        ctrl._apply_service_update.assert_not_called()  # pylint: disable=protected-access
        assert not ctrl._get_update_status()['update_apply_pending']  # pylint: disable=protected-access


def _sync_full(ctrl: controller.SkyServeController,
               infos,
               active_versions=(1,),
               async_occupancy_by_version=None):
    """Returns the full (replica_info, num_ready) tuple."""
    record = {'active_versions': list(active_versions)}
    with mock.patch.object(controller.serve_state,
                           'get_service_runtime_snapshot',
                           return_value=record), \
         mock.patch.object(
             controller.global_user_state,
             'get_clusters_from_names',
             side_effect=lambda names: {name: {'handle': mock.sentinel.handle}
                                        for name in names}), \
         mock.patch.object(
             controller.global_user_state,
             'get_cluster_yaml_dict_multiple',
             side_effect=lambda paths: [{'provider': {'path': path}}
                                        for path in paths]):
        return ctrl._get_lb_replica_info(  # pylint: disable=protected-access
            infos, async_occupancy_by_version)


def _sync(ctrl: controller.SkyServeController, infos,
          active_versions=(1,)) -> Dict[str, Dict[str, str]]:
    return _sync_full(ctrl, infos, active_versions)[0]


class TestGetLbReplicaInfo:
    """Tests for the (url, gpu_type, gpu_count) per-replica cache behind
    the /controller/load_balancer_sync response."""

    def test_resolves_url_and_gpu_type_for_ready_replicas(self):
        ctrl = _make_controller()
        infos = [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.READY,
                             url='http://2.2.2.2:8080',
                             accelerators={'A100': 8}),
        ]
        assert _sync(ctrl, infos) == {
            'http://1.1.1.1:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            },
            'http://2.2.2.2:8080': {
                'gpu_type': 'A100',
                'gpu_count': '8'
            },
        }

    def test_async_occupancy_declaration_is_per_replica_version(self):
        ctrl = _make_controller()
        old = _FakeReplicaInfo(1,
                               serve_state.ReplicaStatus.READY,
                               version=1,
                               url='http://1.1.1.1:8080',
                               accelerators={'L4': 1})
        new = _FakeReplicaInfo(2,
                               serve_state.ReplicaStatus.READY,
                               version=2,
                               url='http://2.2.2.2:8080',
                               accelerators={'L4': 1})
        replica_info, _ = _sync_full(ctrl, [old, new], (1, 2), {
            1: True,
            2: False
        })
        assert replica_info['http://1.1.1.1:8080']['async_occupancy'] == 'true'
        assert replica_info['http://2.2.2.2:8080']['async_occupancy'] == 'false'

    def test_unspecified_async_occupancy_declaration_is_omitted(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        replica_info, _ = _sync_full(ctrl, [info], (1,), {1: None})
        assert 'async_occupancy' not in replica_info['http://1.1.1.1:8080']

    def test_resolution_happens_at_most_once_per_replica(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        first = _sync(ctrl, [info])
        second = _sync(ctrl, [info])
        assert first == second
        # url and handle must be resolved on the first sync only.
        assert info.url_resolutions == 1
        assert info.handle_resolutions == 1

    def test_cold_sync_batches_cluster_records_and_warm_sync_skips_lookup(self):
        ctrl = _make_controller()
        infos = [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.READY,
                             url='http://2.2.2.2:8080',
                             accelerators={'L4': 1}),
        ]
        with mock.patch.object(
                controller.serve_state,
                'get_service_runtime_snapshot',
                return_value={'active_versions': [1]}), mock.patch.object(
                    controller.global_user_state,
                    'get_clusters_from_names',
                    return_value={
                        info.cluster_name: {
                            'handle': mock.sentinel.handle
                        } for info in infos
                    }) as get_clusters, mock.patch.object(
                        controller.global_user_state,
                        'get_cluster_yaml_dict_multiple',
                        return_value=[{
                            'provider': {
                                'replica': info.replica_id
                            }
                        } for info in infos]) as get_yamls:
            first = ctrl._get_lb_replica_info(  # pylint: disable=protected-access
                infos, None)
            second = ctrl._get_lb_replica_info(  # pylint: disable=protected-access
                infos, None)

        assert first == second
        get_clusters.assert_called_once_with(['replica-1', 'replica-2'])
        get_yamls.assert_called_once_with(['replica-1.yaml', 'replica-2.yaml'])
        assert infos[0].last_provider_config == {'replica': 1}
        assert infos[1].last_provider_config == {'replica': 2}

    def test_uses_runtime_snapshot_not_joined_service_read(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        with mock.patch.object(
                controller.serve_state,
                'get_service_from_name',
                side_effect=AssertionError(
                    'lb sync must not use joined service reads')), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}):
            assert _sync(ctrl, [info]) == {
                'http://1.1.1.1:8080': {
                    'gpu_type': 'L4',
                    'gpu_count': '1'
                }
            }

    def test_ownership_fence_reads_runtime_snapshot_fields(self):
        # The fence compares the exact field names returned by
        # get_service_runtime_snapshot; a renamed/dropped key would either
        # disable the fence or make it fire on every sync.
        ctrl = _make_controller()
        ctrl._service_hash = 'hash-a'  # pylint: disable=protected-access
        ctrl._controller_owner = (111, '10.0.0.1')  # pylint: disable=protected-access
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        owned = {
            'hash': 'hash-a',
            'controller_pid': 111,
            'controller_ip': '10.0.0.1',
            'active_versions': [1],
        }

        def sync_with(snapshot):
            with mock.patch.object(controller.serve_state,
                                   'get_service_runtime_snapshot',
                                   return_value=snapshot), \
                 mock.patch.object(
                     controller.global_user_state,
                     'get_clusters_from_names',
                     side_effect=lambda names: {
                         name: {'handle': mock.sentinel.handle}
                         for name in names
                     }), \
                 mock.patch.object(
                     controller.global_user_state,
                     'get_cluster_yaml_dict_multiple',
                     side_effect=lambda paths: [{'provider': {}}
                                                for _ in paths]):
                return ctrl._get_lb_replica_info(  # pylint: disable=protected-access
                    [info], None)

        replica_info, _ = sync_with(owned)
        assert list(replica_info) == ['http://1.1.1.1:8080']

        for stale in (dict(owned, hash='hash-b'), dict(owned,
                                                       controller_pid=222),
                      dict(owned, controller_ip='10.0.0.2')):
            with pytest.raises(RuntimeError, match='ownership changed'):
                sync_with(stale)

    def test_not_ready_replicas_are_never_resolved(self):
        ctrl = _make_controller()
        provisioning = _FakeReplicaInfo(1,
                                        serve_state.ReplicaStatus.PROVISIONING)
        ready = _FakeReplicaInfo(2,
                                 serve_state.ReplicaStatus.READY,
                                 url='http://2.2.2.2:8080',
                                 accelerators={'L4': 1})
        result = _sync(ctrl, [provisioning, ready])
        assert result == {
            'http://2.2.2.2:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            }
        }
        assert provisioning.url_resolutions == 0
        assert provisioning.handle_resolutions == 0

    def test_inactive_version_replicas_are_excluded(self):
        ctrl = _make_controller()
        outdated = _FakeReplicaInfo(1,
                                    serve_state.ReplicaStatus.READY,
                                    version=1,
                                    url='http://1.1.1.1:8080',
                                    accelerators={'L4': 1})
        current = _FakeReplicaInfo(2,
                                   serve_state.ReplicaStatus.READY,
                                   version=2,
                                   url='http://2.2.2.2:8080',
                                   accelerators={'L4': 1})
        result = _sync(ctrl, [outdated, current], active_versions=(2,))
        assert result == {
            'http://2.2.2.2:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            }
        }
        assert outdated.url_resolutions == 0

    def test_unknown_gpu_type_when_unresolvable(self):
        """Both unresolvable cases (no handle yet, no accelerators) must
        fall back to 'unknown' instead of dropping the replica."""
        ctrl = _make_controller()
        no_handle = _FakeReplicaInfo(1,
                                     serve_state.ReplicaStatus.READY,
                                     url='http://1.1.1.1:8080',
                                     handle_is_none=True)
        no_accelerators = _FakeReplicaInfo(2,
                                           serve_state.ReplicaStatus.READY,
                                           url='http://2.2.2.2:8080',
                                           accelerators=None)
        assert _sync(ctrl, [no_handle, no_accelerators]) == {
            'http://1.1.1.1:8080': {
                'gpu_type': 'unknown',
                'gpu_count': '1'
            },
            'http://2.2.2.2:8080': {
                'gpu_type': 'unknown',
                'gpu_count': '1'
            },
        }

    def test_ready_replica_without_url_is_skipped_not_crashed(self):
        """A READY replica whose endpoint is briefly unresolvable (e.g.
        no head IP mid-recovery) must be skipped for the sync round,
        not crash load_balancer_sync (this was an assert)."""
        ctrl = _make_controller()
        no_url = _FakeReplicaInfo(1,
                                  serve_state.ReplicaStatus.READY,
                                  url=None,
                                  accelerators={'L4': 1})
        ok = _FakeReplicaInfo(2,
                              serve_state.ReplicaStatus.READY,
                              url='http://2.2.2.2:8080',
                              accelerators={'L4': 1})
        assert _sync(ctrl, [no_url, ok]) == {
            'http://2.2.2.2:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            }
        }
        # Not cached: it must be re-resolved on the next sync.
        assert 1 not in ctrl._lb_replica_cache  # pylint: disable=protected-access

    def test_cache_pruned_when_replica_leaves_ready_set(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        _sync(ctrl, [info])

        # The replica gets preempted: it must be dropped from the response
        # and pruned from the cache.
        preempted = _FakeReplicaInfo(1,
                                     serve_state.ReplicaStatus.NOT_READY,
                                     url='http://1.1.1.1:8080')
        assert not _sync(ctrl, [preempted])

        # The replica recovers with a new endpoint: it must be re-resolved
        # instead of served from a stale cache entry.
        recovered = _FakeReplicaInfo(1,
                                     serve_state.ReplicaStatus.READY,
                                     url='http://3.3.3.3:8080',
                                     accelerators={'L4': 1})
        assert _sync(ctrl, [recovered]) == {
            'http://3.3.3.3:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            }
        }
        assert recovered.url_resolutions == 1


class TestNumReadyReplicas:
    """The sync response reports the count of READY, active replicas -- which
    can exceed the size of the resolved url map when endpoints are transiently
    unresolvable. The load balancer uses this to distinguish an authoritative
    zero from a spurious empty map and avoid blanking a healthy ready set."""

    def test_counts_ready_replicas_even_when_urls_unresolvable(self):
        # Both replicas are READY but their endpoints don't resolve this round:
        # the map is empty yet num_ready is 2 (the transient-empty signal).
        ctrl = _make_controller()
        infos = [
            _FakeReplicaInfo(1, serve_state.ReplicaStatus.READY, url=None),
            _FakeReplicaInfo(2, serve_state.ReplicaStatus.READY, url=None),
        ]
        replica_info, num_ready = _sync_full(ctrl, infos)
        assert not replica_info
        assert num_ready == 2

    def test_zero_when_no_ready_replicas(self):
        # A genuine authoritative zero: nothing READY -> empty map, count 0.
        ctrl = _make_controller()
        infos = [
            _FakeReplicaInfo(1, serve_state.ReplicaStatus.PROVISIONING),
            _FakeReplicaInfo(2, serve_state.ReplicaStatus.NOT_READY),
        ]
        replica_info, num_ready = _sync_full(ctrl, infos)
        assert not replica_info
        assert num_ready == 0

    def test_counts_only_active_version_ready_replicas(self):
        # Outdated-version and not-ready replicas are excluded from the count.
        ctrl = _make_controller()
        infos = [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             version=1,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.READY,
                             version=2,
                             url=None),
            _FakeReplicaInfo(3, serve_state.ReplicaStatus.PROVISIONING),
        ]
        replica_info, num_ready = _sync_full(ctrl, infos, active_versions=(2,))
        # Only replica 2 is active+READY; its url is unresolvable this round.
        assert not replica_info
        assert num_ready == 1


class TestAutoscalerRuntimeSnapshot:

    def test_run_autoscaler_uses_runtime_snapshot_for_active_versions(self):
        ctrl = _make_controller()
        ctrl._autoscaler = mock.Mock()  # pylint: disable=protected-access
        ctrl._autoscaler.generate_scaling_decisions.return_value = []
        ctrl._autoscaler.get_decision_interval.return_value = 0
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_from_name',
                 side_effect=AssertionError(
                     'autoscaler must not use joined service reads')), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [2]}), \
             mock.patch.object(controller.time,
                               'sleep',
                               side_effect=StopIteration):
            try:
                ctrl._run_autoscaler()  # pylint: disable=protected-access
            except StopIteration:
                pass

        ctrl._autoscaler.generate_scaling_decisions.assert_called_once_with([],
                                                                            [2])


class TestTranslateInFlight:
    """The LB reports in-flight work keyed by replica url; the autoscaler
    consumes it keyed by replica id. The controller inverts its
    (id -> url) sync cache to translate."""

    def _synced_controller(self):
        ctrl = _make_controller()
        infos = [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.READY,
                             url='http://2.2.2.2:8080',
                             accelerators={'L4': 1}),
        ]
        _sync(ctrl, infos)
        return ctrl

    def test_urls_translated_to_replica_ids(self):
        ctrl = self._synced_controller()
        translated = ctrl._translate_in_flight({  # pylint: disable=protected-access
            'http://1.1.1.1:8080': 3,
            'http://2.2.2.2:8080': 0,
        })
        assert translated == {1: 3, 2: 0}

    def test_unknown_url_is_dropped(self):
        # A url the controller never resolved (or whose replica went
        # terminal) has no live id to attribute the work to.
        ctrl = self._synced_controller()
        translated = ctrl._translate_in_flight({  # pylint: disable=protected-access
            'http://1.1.1.1:8080': 2,
            'http://9.9.9.9:8080': 7,
        })
        assert translated == {1: 2}

    def test_blipped_replica_stays_translatable(self):
        # A replica demoted from READY (probe blip) mid-job must stay
        # translatable while nonterminal: dropping it would erase its
        # in-flight unit AND make it read as an idle scale-down victim.
        ctrl = self._synced_controller()
        _sync(ctrl, [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.NOT_READY,
                             url='http://2.2.2.2:8080',
                             accelerators={'L4': 1}),
        ])
        translated = ctrl._translate_in_flight({  # pylint: disable=protected-access
            'http://1.1.1.1:8080': 0,
            'http://2.2.2.2:8080': 1,
        })
        assert translated == {1: 0, 2: 1}

    def test_shutting_down_replica_pruned_from_translation(self):
        # A retiring replica's in-flight is pinned to it (never re-routed):
        # counting it as outstanding demand would launch phantom
        # replacement capacity for the whole drain window. The retirement
        # drain matches the LB's raw url-keyed report instead, so it does
        # not depend on this translation.
        ctrl = self._synced_controller()
        _sync(ctrl, [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.SHUTTING_DOWN,
                             url='http://2.2.2.2:8080',
                             accelerators={'L4': 1}),
        ])
        translated = ctrl._translate_in_flight(
            {  # pylint: disable=protected-access
                'http://2.2.2.2:8080': 1,
            })
        assert translated == {}

    def test_gone_terminal_replica_pruned_from_translation(self):
        ctrl = self._synced_controller()
        _sync(ctrl, [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.FAILED,
                             url='http://2.2.2.2:8080',
                             accelerators={'L4': 1}),
        ])
        translated = ctrl._translate_in_flight(
            {  # pylint: disable=protected-access
                'http://2.2.2.2:8080': 1,
            })
        assert translated == {}

    def test_none_passes_through(self):
        # None means the LB sent no gauge (old LB / non-tracking policy);
        # the autoscaler must see None, not an empty (fresh-looking) dict.
        ctrl = self._synced_controller()
        assert ctrl._translate_in_flight(None) is None  # pylint: disable=protected-access


class TestUnknownAsyncOccupancy:

    def test_declared_ready_requires_sample_proof_not_envelope_zero(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        _sync(ctrl, [info])
        # A numeric envelope entry (even explicit zero) is intentionally not an
        # input: only occupancy_sampled_urls can prove async idleness.
        unknown = ctrl._unknown_async_replica_ids(  # pylint: disable=protected-access
            [info], {1: True}, [], [])
        assert unknown == {1}
        unknown = ctrl._unknown_async_replica_ids(  # pylint: disable=protected-access
            [info], {1: True}, ['http://1.1.1.1:8080'], [])
        assert unknown == set()

    def test_cold_controller_keeps_declared_not_ready_unknown(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(2,
                                serve_state.ReplicaStatus.NOT_READY,
                                url='http://2.2.2.2:8080')
        # A cold controller cannot translate the LB's retained URL yet. It must
        # fail closed from the durable per-version declaration.
        unknown = ctrl._unknown_async_replica_ids(  # pylint: disable=protected-access
            [info], {1: True}, ['http://2.2.2.2:8080'], None)
        assert unknown == {2}

    def test_raw_probe_miss_overrides_sample_if_payload_is_inconsistent(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        _sync(ctrl, [info])
        unknown = ctrl._unknown_async_replica_ids(  # pylint: disable=protected-access
            [info], {1: True}, ['http://1.1.1.1:8080'], ['http://1.1.1.1:8080'])
        assert unknown == {1}

    def test_lb_pod_report_has_independent_demand_and_drain_authority(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        _sync(ctrl, [info])
        # One Kubernetes snapshot distinguishes the Pod receiving new traffic
        # from every Pod that may still own a long-running stream.
        cases = [
            (controller.lb_k8s.LbPodAuthority(set(), set()), 'lb-a',
             (False, False, False)),
            (controller.lb_k8s.LbPodAuthority({'lb-a', 'lb-b'},
                                              {'lb-a', 'lb-b'}), 'lb-a',
             (True, False, False)),
            (controller.lb_k8s.LbPodAuthority({'lb-b'}, {'lb-a', 'lb-b'}),
             'lb-a', (True, False, False)),
            (None, 'lb-a', (False, False, False)),
            (controller.lb_k8s.LbPodAuthority({'lb-a'}, {'lb-a'}), None,
             (False, False, False)),
            # Normal steady state: one reporter owns both authority levels.
            (controller.lb_k8s.LbPodAuthority({'lb-a'}, {'lb-a'}), 'lb-a',
             (True, True, True)),
            # New Ready Pod plus an old terminating Pod: demand stays live,
            # but neither zero gauges nor drain metadata are service-wide.
            (controller.lb_k8s.LbPodAuthority({'lb-a'}, {'lb-a', 'lb-old'}),
             'lb-a', (True, True, False)),
            # The sole terminating Pod can finish proving its streams drained,
            # but it no longer owns new demand.
            (controller.lb_k8s.LbPodAuthority(set(), {'lb-a'}), 'lb-a',
             (True, False, True)),
        ]
        for pod_authority, reporter, expected in cases:
            with mock.patch.object(controller.lb_k8s,
                                   'get_lb_pod_authority',
                                   return_value=pod_authority):
                assert ctrl._lb_report_authority(  # pylint: disable=protected-access
                    reporter) == expected
        with mock.patch.object(controller.lb_k8s,
                               'get_lb_pod_authority',
                               side_effect=RuntimeError('adapter failed')):
            assert ctrl._lb_report_authority(  # pylint: disable=protected-access
                'lb-a') == (False, False, False)

        report_is_authoritative = False
        unknown = ctrl._unknown_async_replica_ids(  # pylint: disable=protected-access
            [info], {1: None}, ['http://1.1.1.1:8080'], [],
            force_all_live_unknown=not report_is_authoritative)
        assert unknown == {1}

        # A zero/off-route view must not complete a drain while another Pod is
        # live, the UID mismatches, or Kubernetes liveness is unknown.
        # routing_urls=None makes the drain tracker fall back to its bounded
        # deadline regardless of the clean-looking gauge.
        in_flight, routing_urls = ctrl._lb_drain_report_view(  # pylint: disable=protected-access
            {
                'in_flight': {
                    'http://1.1.1.1:8080': 0
                },
                'routing_urls': [],
            }, report_is_authoritative)
        manager = types.SimpleNamespace(_lb_in_flight_report=None)
        controller.replica_managers.ReplicaManager.update_lb_in_flight(
            manager,
            in_flight,
            routing_urls,
            unknown_urls=[],
            draining_urls=[],
            lb_session_id='lb-a')
        tracker = controller.replica_managers._ReplicaDrainTracker(  # pylint: disable=protected-access
            manager,
            'http://1.1.1.1:8080',
            drain_started=0)
        assert not tracker()

        # Once Kubernetes reports the sole matching Pod, occupancy and drain
        # proofs are accepted immediately; no arbitrary heartbeat TTL applies.
        report_is_authoritative = True
        unknown = ctrl._unknown_async_replica_ids(  # pylint: disable=protected-access
            [info], {1: None}, ['http://1.1.1.1:8080'], [],
            force_all_live_unknown=not report_is_authoritative)
        assert unknown == set()
        in_flight, routing_urls = ctrl._lb_drain_report_view(  # pylint: disable=protected-access
            {
                'in_flight': {
                    'http://1.1.1.1:8080': 0
                },
                'routing_urls': [],
            }, report_is_authoritative)
        assert in_flight == {'http://1.1.1.1:8080': 0}
        assert routing_urls == []


class _StatefulDemandAutoscaler:
    """Small stateful collector exposing every LB-controlled demand field."""

    def __init__(self) -> None:
        self.replica_unit = 'physical_backend'
        self.latest_version = 1
        self.request_timestamps = [101]
        self.in_flight_by_replica_id = {1: 9}
        self.unknown_in_flight_replica_ids = {1}
        self.queue_depth = 7
        self.rejected_in_window = 5
        self.collect_calls = 0
        self.reports = []

    def collect_request_information(self, report) -> None:
        self.collect_calls += 1
        self.reports.append(report)
        self.request_timestamps.extend(report['timestamps'])
        self.in_flight_by_replica_id = report['in_flight_by_replica_id']
        self.unknown_in_flight_replica_ids = set(
            report['unknown_in_flight_replica_ids'])
        self.queue_depth = report['queue_depth']
        self.rejected_in_window = report['rejected_in_window']

    def snapshot(self):
        return (list(self.request_timestamps),
                dict(self.in_flight_by_replica_id),
                set(self.unknown_in_flight_replica_ids), self.queue_depth,
                self.rejected_in_window, self.collect_calls)


class _StatefulReplicaManager:
    """Small stateful drain-report sink used to detect any mutation."""

    def __init__(self) -> None:
        self.report = ({
            'http://trusted:8080': 4
        }, ['http://trusted:8080'], ['http://trusted:8080'],
                       ['http://trusted:8080'], 'trusted-session')
        self.update_calls = 0
        self.logical_snapshot = None

    def update_lb_in_flight(self, in_flight, routing_urls, unknown_urls,
                            draining_urls, lb_session_id) -> None:
        self.update_calls += 1
        self.report = (in_flight, routing_urls, unknown_urls, draining_urls,
                       lb_session_id)

    def snapshot(self):
        return self.report, self.update_calls

    def update_logical_reconcile_snapshot(self, **kwargs) -> None:
        self.logical_snapshot = kwargs


class TestAuthoritativeLbReportIngestion:
    """Demand and drain mutate only at their respective authority levels."""

    _URL = 'http://1.1.1.1:8080'

    def _controller_and_report(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _StatefulDemandAutoscaler()  # pylint: disable=protected-access
        ctrl._replica_manager = _StatefulReplicaManager()  # pylint: disable=protected-access
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: (self._URL, 'L4', 1)
        }
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url=self._URL,
                                accelerators={'L4': 1})
        report = {
            'lb_session_id': 'lb-a',
            'request_aggregator': {
                'timestamps': [201, 202]
            },
            'in_flight': {
                self._URL: 2
            },
            'occupancy_sampled_urls': [],
            'unknown_in_flight_urls': [self._URL],
            'queue_depth': 11,
            'rejected_in_window': 13,
            'routing_urls': [self._URL],
            'draining_urls': [self._URL],
        }
        return ctrl, info, report

    def test_wrong_ambiguous_and_unavailable_reporters_change_nothing(self):
        outcomes = [
            (controller.lb_k8s.LbPodAuthority({'lb-other'},
                                              {'lb-other'}), False),
            # Multiple Ready Pods make even demand last-writer-wins.
            (controller.lb_k8s.LbPodAuthority({'lb-a', 'lb-other'},
                                              {'lb-a', 'lb-other'}), True),
            (controller.lb_k8s.LbPodAuthority(set(), set()), False),
            (None, False),  # Kubernetes lookup failed closed.
            (RuntimeError('adapter failed'),
             False),  # Unexpected adapter failure.
        ]
        for outcome, expected_accepted in outcomes:
            ctrl, info, report = self._controller_and_report()
            autoscaler_before = ctrl._autoscaler.snapshot()  # pylint: disable=protected-access
            drain_before = ctrl._replica_manager.snapshot()  # pylint: disable=protected-access
            kwargs = ({
                'side_effect': outcome
            } if isinstance(outcome, Exception) else {
                'return_value': outcome
            })
            with mock.patch.object(controller.lb_k8s, 'get_lb_pod_authority',
                                   **kwargs):
                accepted = asyncio.run(
                    ctrl._ingest_load_balancer_report(  # pylint: disable=protected-access
                        report, [info], {1: True}))

            assert accepted is expected_accepted
            # Timestamps, in-flight/occupancy, queue depth, and rejected
            # demand retain the last trusted snapshot.
            assert ctrl._autoscaler.snapshot() == autoscaler_before  # pylint: disable=protected-access
            # Routing, unknown/draining sets, and LB session retain the last
            # trusted drain snapshot (even its freshness is not advanced).
            assert ctrl._replica_manager.snapshot() == drain_before  # pylint: disable=protected-access

    def test_wrong_service_uid_cannot_enumerate_replica_urls(self):
        ctrl, _, report = self._controller_and_report()
        with mock.patch.object(
                controller.lb_k8s,
                'get_lb_pod_authority',
                return_value=controller.lb_k8s.LbPodAuthority(
                    {'other-service-pod'},
                    {'other-service-pod'})), mock.patch.object(
                        controller.serve_state,
                        'get_replica_infos') as get_replica_infos:
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))

        assert response.status_code == 503
        assert response.body == b''
        # Membership is checked before even reading/resolving replica records.
        get_replica_infos.assert_not_called()

    def test_sole_matching_reporter_updates_all_demand_and_drain_state(self):
        ctrl, info, report = self._controller_and_report()
        with mock.patch.object(controller.lb_k8s,
                               'get_lb_pod_authority',
                               return_value=controller.lb_k8s.LbPodAuthority(
                                   {'lb-a'}, {'lb-a'})):
            accepted = asyncio.run(
                ctrl._ingest_load_balancer_report(  # pylint: disable=protected-access
                    report, [info], {1: True}))

        assert accepted is True
        assert ctrl._autoscaler.snapshot() == (  # pylint: disable=protected-access
            [101, 201, 202], {
                1: 2
            }, {1}, 11, 13, 1)
        assert ctrl._replica_manager.snapshot() == (  # pylint: disable=protected-access
            ({
                self._URL: 2
            }, [self._URL], [self._URL], [self._URL], 'lb-a'), 1)

    def test_logical_report_publishes_capacity_and_demand_as_one_generation(
            self):
        ctrl, info, report = self._controller_and_report()
        ctrl._autoscaler.replica_unit = 'logical'  # pylint: disable=protected-access
        report['total_slots_by_url'] = {self._URL: 8}
        report['occupancy_sampled_urls'] = [self._URL]
        report['unknown_in_flight_urls'] = []

        accepted = asyncio.run(
            ctrl._ingest_load_balancer_report(  # pylint: disable=protected-access
                report, [info], {1: True},
                authority=(True, True, True)))

        assert accepted is True
        assert ctrl._replica_manager.logical_snapshot == {  # pylint: disable=protected-access
            'version': 1,
            'generation': 1,
            'observed_slots_by_replica_id': {
                1: 8
            },
            'in_flight_by_replica_id': {
                1: 2
            },
            'unknown_replica_ids': set(),
        }

    def test_sole_ready_reporter_keeps_demand_during_terminating_overlap(self):
        ctrl, info, report = self._controller_and_report()
        with mock.patch.object(controller.lb_k8s,
                               'get_lb_pod_authority',
                               return_value=controller.lb_k8s.LbPodAuthority(
                                   {'lb-a'}, {'lb-a', 'lb-old'})):
            accepted = asyncio.run(
                ctrl._ingest_load_balancer_report(  # pylint: disable=protected-access
                    report, [info], {1: True}))

        assert accepted is True
        # QPS and positive demand continue through an arbitrarily long old-Pod
        # termination, while all live replicas are fail-closed as busy.
        assert ctrl._autoscaler.snapshot() == (  # pylint: disable=protected-access
            [101, 201, 202], {
                1: 2
            }, {1}, 11, 13, 1)
        assert ctrl._autoscaler.reports[-1][  # pylint: disable=protected-access
            'unknown_capacity_replica_ids'] == []
        # The reporter's clean-looking drain fields are not copied. A blocking
        # view also invalidates any still-fresh proof from before the rollout.
        assert ctrl._replica_manager.snapshot() == (  # pylint: disable=protected-access
            ({}, None, [], [], 'lb-a'), 1)

    def test_sole_terminating_reporter_updates_drain_but_not_demand(self):
        ctrl, info, report = self._controller_and_report()
        # If demand parsing accidentally runs, this malformed block would fail.
        report['request_aggregator'] = None
        autoscaler_before = ctrl._autoscaler.snapshot()  # pylint: disable=protected-access
        with mock.patch.object(controller.lb_k8s,
                               'get_lb_pod_authority',
                               return_value=controller.lb_k8s.LbPodAuthority(
                                   set(), {'lb-a'})):
            accepted = asyncio.run(
                ctrl._ingest_load_balancer_report(  # pylint: disable=protected-access
                    report, [info], {1: True}))

        assert accepted is True
        assert ctrl._autoscaler.snapshot() == autoscaler_before  # pylint: disable=protected-access
        assert ctrl._replica_manager.snapshot() == (  # pylint: disable=protected-access
            ({
                self._URL: 2
            }, [self._URL], [self._URL], [self._URL], 'lb-a'), 1)

    def test_load_balancer_sync_batches_version_spec_reads(self):
        ctrl, _, report = self._controller_and_report()
        infos = [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             version=1,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.READY,
                             version=2,
                             url='http://2.2.2.2:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(3,
                             serve_state.ReplicaStatus.READY,
                             version=3,
                             url='http://3.3.3.3:8080',
                             accelerators={'L4': 1}),
        ]
        observed = {}
        event_loop_thread = threading.get_ident()

        def _capture_lb_replica_info(replica_infos, async_occupancy_by_version):
            del replica_infos
            observed['sync_thread'] = threading.get_ident()
            observed['sync'] = dict(async_occupancy_by_version)
            return ({}, 0)

        async def _capture_ingest(request_data,
                                  replica_infos,
                                  async_occupancy_by_version,
                                  authority=None):
            del request_data, replica_infos, authority
            observed['ingest'] = dict(async_occupancy_by_version)
            return True

        ctrl._get_lb_replica_info = _capture_lb_replica_info  # pylint: disable=protected-access
        ctrl._ingest_load_balancer_report = _capture_ingest  # pylint: disable=protected-access

        def _capture_capacity_hint(replica_infos, logical_versions):
            observed['logical_versions'] = set(logical_versions)
            return {'n': len(replica_infos)}

        ctrl._get_capacity_hint = _capture_capacity_hint  # pylint: disable=protected-access
        ctrl._routing_spec = {'policy': 'round_robin'}  # pylint: disable=protected-access

        with mock.patch.object(controller.lb_k8s,
                               'get_lb_pod_authority',
                               return_value=controller.lb_k8s.LbPodAuthority(
                                   {'lb-a'}, {'lb-a'})), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=infos), \
             mock.patch.object(
                 controller.serve_state,
                 'get_specs',
                 return_value={
                     1: types.SimpleNamespace(
                         graceful_drain_async_occupancy=False),
                     3: types.SimpleNamespace(
                         graceful_drain_async_occupancy=True,
                         uses_logical_replicas=True),
                 }) as get_specs, \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               side_effect=AssertionError(
                                   'per-version reads should be batched')):
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))

        assert response.status_code == 200
        get_specs.assert_called_once_with('svc', [1, 2, 3])
        assert observed['sync'] == {1: False, 2: None, 3: True}
        assert observed['ingest'] == {1: False, 2: None, 3: True}
        assert observed['logical_versions'] == {3}
        assert observed['sync_thread'] != event_loop_thread


class _FakeAutoscaler:
    """Autoscaler stub for the capacity-hint computation."""

    def __init__(self,
                 target,
                 recomputed,
                 latest_version=1,
                 replica_unit='physical_backend') -> None:
        self._target = target
        self._recomputed = recomputed
        self.latest_version = latest_version
        self.max_replicas = 20
        self.replica_unit = replica_unit

    def get_final_target_num_replicas(self) -> int:
        return self._target

    def has_recomputed_with_fresh_data(self) -> bool:
        return self._recomputed


class TestGetCapacityHint:
    """capacity_hint rides the sync response so the data plane can see
    capacity that is already on the way (provisioning) and the fleet's
    intended size (target)."""

    def _replicas(self):
        return [
            # Latest version: one READY, two provisioning-ish, one
            # terminal (must not count anywhere).
            _FakeReplicaInfo(1, serve_state.ReplicaStatus.READY, version=2),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.PROVISIONING,
                             version=2),
            _FakeReplicaInfo(3, serve_state.ReplicaStatus.STARTING, version=2),
            _FakeReplicaInfo(4,
                             serve_state.ReplicaStatus.SHUTTING_DOWN,
                             version=2),
            # Old version replicas never count.
            _FakeReplicaInfo(5, serve_state.ReplicaStatus.READY, version=1),
        ]

    def test_provisioning_counts_latest_nonterminal_not_ready(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=5,
            recomputed=True,
            latest_version=2)
        hint = ctrl._get_capacity_hint(  # pylint: disable=protected-access
            self._replicas(),
            logical_versions=set())
        assert hint == {
            'replica_unit': 'physical_backend',
            'provisioning_replicas': 2,
            'target_num_replicas': 5,
            'max_replicas': 20,
            'configured_max_replicas': 20,
        }

    def test_stale_autoscaler_reports_at_least_live_fleet(self):
        # A rebuilt controller (target reset to min_replicas, no demand
        # report yet) must not tell the platform the fleet wants to
        # shrink: while stale, target is floored at the latest-version
        # nonterminal count.
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=1,
            recomputed=False,
            latest_version=2)
        hint = ctrl._get_capacity_hint(  # pylint: disable=protected-access
            self._replicas(),
            logical_versions=set())
        assert hint == {
            'replica_unit': 'physical_backend',
            'provisioning_replicas': 2,
            'target_num_replicas': 3,
            'max_replicas': 20,
            'configured_max_replicas': 20,
        }

    def test_stale_max_rule_keeps_larger_target(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=10,
            recomputed=False,
            latest_version=2)
        hint = ctrl._get_capacity_hint(  # pylint: disable=protected-access
            self._replicas(),
            logical_versions=set())
        assert hint['target_num_replicas'] == 10
        assert hint['max_replicas'] == 20

    def test_logical_hint_sums_persisted_backend_widths(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=9,
            recomputed=False,
            latest_version=2,
            replica_unit='logical')
        replicas = self._replicas()
        replicas[0].planned_capacity = 8
        replicas[1].planned_capacity = 4
        replicas[2].planned_capacity = 1
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: ('http://eight', 'A100', 8),
            2: ('http://four', 'A100', 4),
            3: ('http://one', 'A100', 1),
        }

        hint = ctrl._get_capacity_hint(  # pylint: disable=protected-access
            replicas, logical_versions={2})

        assert hint == {
            'replica_unit': 'logical_slot',
            'provisioning_replicas': 5,
            # Before a fresh demand tick, never advertise a target below the
            # persisted 8 + 4 + 1 logical slots.
            'target_num_replicas': 13,
            'max_replicas': 20,
            'configured_max_replicas': 20,
            'planned_capacity_by_url': {
                'http://eight': 8,
                'http://four': 4,
                'http://one': 1,
            },
            'logical_replica_urls': [
                'http://eight',
                'http://four',
                'http://one',
            ],
        }

    def test_logical_hint_excludes_physical_bridge_backends(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=8,
            recomputed=True,
            latest_version=2,
            replica_unit='logical')
        physical = _FakeReplicaInfo(1,
                                    serve_state.ReplicaStatus.READY,
                                    version=1)
        logical = _FakeReplicaInfo(2,
                                   serve_state.ReplicaStatus.READY,
                                   version=2)
        logical.planned_capacity = 8
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: ('http://physical', 'A100', 8),
            2: ('http://logical', 'A100', 8),
        }

        hint = ctrl._get_capacity_hint(  # pylint: disable=protected-access
            [physical, logical],
            logical_versions={2})

        assert hint['planned_capacity_by_url'] == {'http://logical': 8}
        assert hint['logical_replica_urls'] == ['http://logical']


class TestReservedCapacityPollerStart:
    """Poller lifecycle: seeded, idempotent, inert without a placer."""

    def _controller_with(self, placer):
        ctrl = _make_controller()
        ctrl._replica_manager = mock.Mock()
        ctrl._replica_manager.spot_placer = placer
        ctrl._autoscaler = mock.Mock()
        ctrl._reserved_capacity_poller_started = False
        ctrl._reserved_capacity_poller_lock = threading.Lock()
        return ctrl

    def test_starts_thread_once(self):
        # Idempotent: one poller thread across repeated calls (boot +
        # any number of fill-enabling updates). Location seeding is
        # handled separately by _seed_fill_zero_cost_locations.
        placer = mock.Mock()
        ctrl = self._controller_with(placer)
        with mock.patch.object(controller.thread_utils,
                               'start_supervised_thread') as start_mock:
            ctrl._start_reserved_capacity_poller_if_needed()
            ctrl._start_reserved_capacity_poller_if_needed()
        assert start_mock.call_count == 1

    def test_without_placer_is_inert(self):
        ctrl = self._controller_with(placer=None)
        with mock.patch.object(controller.thread_utils,
                               'start_supervised_thread') as start_mock:
            ctrl._start_reserved_capacity_poller_if_needed()
        start_mock.assert_not_called()
        # Not marked started: a later update that adds a placer (new
        # service version) may still start it.
        assert ctrl._reserved_capacity_poller_started is False


class TestSeedFillZeroCostLocations:
    """The constructor-time seed is best-effort, never fatal."""

    def test_seed_failure_does_not_propagate(self):
        # zero_cost_locations() can hit a LIVE K8s feasibility check; an
        # unreachable context at boot must not crash-loop the controller
        # through __init__ -- the first successful poll re-seeds.
        ctrl = _make_controller()
        placer = mock.Mock()
        placer.zero_cost_locations.side_effect = RuntimeError('api down')
        ctrl._replica_manager = mock.Mock()
        ctrl._replica_manager.spot_placer = placer
        autoscaler = mock.Mock()
        autoscaler.reserved_capacity_fill = True
        ctrl._seed_fill_zero_cost_locations(autoscaler)
        autoscaler.seed_zero_cost_locations.assert_not_called()


class TestLbSyncBlockingReadsOffLoop:
    """`/lb/sync` DB reads must run in the executor, not on the event loop.

    On a large replica table `get_replica_infos` / `get_specs` / the
    ownership-fence reads are the handler's blocking calls; running them on
    the FastAPI event loop stalls the controller liveness and ownership
    probes served by the same loop (the same invariant that already keeps
    `_lb_report_authority` and `_get_lb_replica_info` in the executor).
    """

    def _run_sync(self):
        ctrl = _make_controller()
        # Arm the ownership fence so _owns_current_service reads the DB.
        ctrl._service_hash = 'incarnation-a'  # pylint: disable=protected-access
        ctrl._controller_owner = (101, '10.0.0.1')  # pylint: disable=protected-access

        read_threads = []

        def _record(result):

            def _side_effect(*_args, **_kwargs):
                read_threads.append(threading.get_ident())
                return result

            return _side_effect

        owner_row = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
        }

        async def _ingest(*_args, **_kwargs):
            return True

        loop_thread = []

        async def _drive():
            loop_thread.append(threading.get_ident())
            return await ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                {'lb_session_id': 'lb-a'})

        with mock.patch.object(controller.serve_state,
                               'get_service_controller_owner',
                               side_effect=_record(owner_row)), \
             mock.patch.object(controller.serve_state, 'get_replica_infos',
                               side_effect=_record([])), \
             mock.patch.object(controller.serve_state, 'get_specs',
                               side_effect=_record({})), \
             mock.patch.object(ctrl, '_lb_report_authority',
                               return_value=(True, True, True)), \
             mock.patch.object(ctrl, '_get_lb_replica_info',
                               return_value=([], 0)), \
             mock.patch.object(ctrl, '_ingest_load_balancer_report',
                               side_effect=_ingest), \
             mock.patch.object(ctrl, '_get_capacity_hint',
                               return_value={}):
            response = asyncio.run(_drive())
        return response, read_threads, loop_thread[0]

    def test_db_reads_run_off_the_event_loop(self):
        response, read_threads, loop_thread = self._run_sync()
        assert response.status_code == 200
        # 2 ownership-fence reads (entry + pre-side-effect) + replica rows +
        # specs all happened -- and nothing more: the fences are the sync
        # handler's per-request DB cost, so extra redundant reads here are a
        # hot-path regression.
        assert len(read_threads) == 4
        # ...and none of them on the event-loop thread.
        assert all(tid != loop_thread for tid in read_threads)

    def test_sync_response_shape_preserved(self):
        response, _, _ = self._run_sync()
        assert response.status_code == 200
        body = json.loads(response.body)
        assert set(body) == {
            'replica_info', 'num_ready_replicas', 'routing_spec',
            'capacity_hint'
        }


class TestLbSyncOwnershipFences:
    """The two remaining fences gate entry and the first side effect.

    Consolidating the per-await fences into these two must not weaken the
    incarnation fencing: a controller that lost its DB row may neither
    mutate autoscaler/replica-manager state nor disclose routing.
    """

    def _make_fenced_controller(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'incarnation-a'  # pylint: disable=protected-access
        ctrl._controller_owner = (101, '10.0.0.1')  # pylint: disable=protected-access
        return ctrl

    def _sync(self, ctrl, owner_rows):
        """Drive one sync; each fence read pops the next owner row."""
        ingest_calls = []

        async def _ingest(*args, **kwargs):
            ingest_calls.append((args, kwargs))
            return True

        with mock.patch.object(controller.serve_state,
                               'get_service_controller_owner',
                               side_effect=owner_rows), \
             mock.patch.object(controller.serve_state, 'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(controller.serve_state, 'get_specs',
                               return_value={}), \
             mock.patch.object(ctrl, '_lb_report_authority',
                               return_value=(True, True, True)), \
             mock.patch.object(ctrl, '_get_lb_replica_info',
                               return_value=([], 0)), \
             mock.patch.object(ctrl, '_ingest_load_balancer_report',
                               side_effect=_ingest), \
             mock.patch.object(ctrl, '_get_capacity_hint',
                               return_value={}):
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    {'lb_session_id': 'lb-a'}))
        return response, ingest_calls

    def test_non_owner_rejected_at_entry(self):
        ctrl = self._make_fenced_controller()
        stolen = {
            'hash': 'incarnation-b',
            'controller_pid': 202,
            'controller_ip': '10.0.0.2',
        }
        response, ingest_calls = self._sync(ctrl, owner_rows=[stolen])
        assert response.status_code == 503
        assert not ingest_calls

    def test_ownership_lost_mid_sync_blocks_mutation_and_disclosure(self):
        # Owner at entry, row stolen while the replica snapshot was read:
        # the pre-side-effect fence must reject before the report is
        # ingested or any routing info is returned.
        ctrl = self._make_fenced_controller()
        owned = {
            'hash': 'incarnation-a',
            'controller_pid': 101,
            'controller_ip': '10.0.0.1',
        }
        stolen = {
            'hash': 'incarnation-b',
            'controller_pid': 202,
            'controller_ip': '10.0.0.2',
        }
        response, ingest_calls = self._sync(ctrl, owner_rows=[owned, stolen])
        assert response.status_code == 503
        assert not ingest_calls

    def test_ingest_never_awaits_with_authority_provided(self):
        # The fence consolidation relies on this: with `authority` passed,
        # _ingest_load_balancer_report must run to completion without
        # yielding, so nothing can interleave between the fence, the
        # mutation, and the disclosure.
        ctrl = _make_controller()
        ctrl._replica_manager = mock.Mock()
        ctrl._autoscaler = mock.Mock()
        coro = ctrl._ingest_load_balancer_report(  # pylint: disable=protected-access
            {'lb_session_id': 'lb-a'}, [], {},
            authority=(True, True, True))
        # Driving the coroutine by hand: a bare send(None) must finish it
        # in one step (StopIteration) -- any await would suspend instead.
        with pytest.raises(StopIteration) as stop:
            coro.send(None)
        assert stop.value.value is True
