"""Tests for sky/serve/controller.py.

Currently focused on `SkyServeController._get_lb_replica_info`, which builds
the `/controller/load_balancer_sync` response. Resolving a replica's url and
gpu_type is expensive (cluster handle fetch + endpoint query), so both must
be resolved at most once per replica lifetime and cached; the cache must be
pruned when a replica leaves the ready set.
"""
# pylint: disable=missing-class-docstring,protected-access
import asyncio
import concurrent.futures
import contextlib
import hashlib
import json
import os
import pathlib
import threading
import time
import types
from typing import Dict, Optional
from unittest import mock

from fastapi import testclient as fastapi_testclient
import pytest

from sky import exceptions
from sky import skypilot_config
from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import controller
from sky.serve import drain_observability
from sky.serve import paid_retirement
from sky.serve import placement_policy
from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import system_recovery_route_lease
from sky.serve import system_recovery_state
from sky.utils import yaml_utils


@pytest.fixture(autouse=True)
def _restore_consolidation_override():
    """Keep in-process controller markers scoped to each test."""
    marker = controller.constants.OVERRIDE_CONSOLIDATION_MODE
    original = os.environ.pop(marker, None)
    original_process_title = controller.setproctitle.getproctitle()
    restore_process_title = controller.setproctitle.setproctitle
    original_metrics_role = (
        controller.db_utils._postgres_connection_metrics_process_role_override)
    controller.db_utils._postgres_connection_metrics_process_role_override = None
    yield
    os.environ.pop(marker, None)
    if original is not None:
        os.environ[marker] = original
    restore_process_title(original_process_title)
    controller.db_utils._postgres_connection_metrics_process_role_override = (
        original_metrics_role)


def test_update_ignores_stale_submitted_yaml_without_request_declaration():
    with mock.patch('builtins.open') as open_file:
        submitted = controller._read_declared_submitted_yaml(  # pylint: disable=protected-access
            {}, 'svc', 5, 'scope')

    assert submitted is None
    open_file.assert_not_called()


def test_update_reads_submitted_yaml_declared_by_request():
    with mock.patch.object(
            controller.serve_utils,
            'generate_submitted_task_yaml_file_name',
            return_value='/tmp/submitted.yaml'), mock.patch(
                'builtins.open',
                mock.mock_open(read_data='service:\n  min_replicas: 2\n')):
        submitted = controller._read_declared_submitted_yaml(  # pylint: disable=protected-access
            {'has_submitted_yaml': True}, 'svc', 5, 'scope')

    assert submitted == 'service:\n  min_replicas: 2\n'


def test_missing_declared_submitted_yaml_does_not_block_update(caplog):
    with mock.patch.object(
            controller.serve_utils,
            'generate_submitted_task_yaml_file_name',
            return_value='/tmp/missing-submitted.yaml'), mock.patch(
                'builtins.open', side_effect=FileNotFoundError):
        submitted = controller._read_declared_submitted_yaml(  # pylint: disable=protected-access
            {'has_submitted_yaml': True}, 'svc', 5, 'scope')

    assert submitted is None
    assert 'is unavailable' in caplog.text


def test_run_controller_sets_connection_metric_role_before_initialization(
        monkeypatch):
    initialization_order = []
    monkeypatch.setattr(
        controller.setproctitle, 'setproctitle',
        lambda title: initialization_order.append(('process-title', title)))
    monkeypatch.setattr(
        controller.db_utils, 'set_postgres_connection_metrics_process_role',
        lambda role: initialization_order.append(('metrics-role', role)))
    monkeypatch.setattr(controller.context_utils, 'hijack_sys_attrs',
                        lambda: initialization_order.append(('context', None)))
    controller_instance = mock.Mock()

    def _construct_controller(*_args, **_kwargs):
        initialization_order.append(('controller-construction', None))
        return controller_instance

    monkeypatch.setattr(controller, 'SkyServeController', _construct_controller)

    controller.run_controller('pool',
                              mock.Mock(),
                              1,
                              '127.0.0.1',
                              20001,
                              'fingerprint',
                              service_hash='incarnation-a')

    assert initialization_order == [
        ('process-title', 'sky.serve.controller --service-name pool '
         '--service-incarnation incarnation-a'),
        ('metrics-role', 'serve-controller'),
        ('context', None),
        ('controller-construction', None),
    ]
    controller_instance.run.assert_called_once_with()


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


def test_run_controller_threads_exact_binding_authority(monkeypatch):
    controller_instance = mock.Mock()
    constructor = mock.Mock(return_value=controller_instance)
    monkeypatch.setattr(controller, 'SkyServeController', constructor)
    monkeypatch.setattr(controller.context_utils, 'hijack_sys_attrs',
                        mock.Mock())
    authority = mock.sentinel.controller_binding_authority

    controller.run_controller('svc', mock.Mock(), 1, '127.0.0.1', 20001,
                              'fingerprint', None, 'incarnation-a', 123,
                              '10.0.0.1', False, None, authority)

    assert constructor.call_args.kwargs['controller_binding_authority'] is (
        authority)
    controller_instance.run.assert_called_once_with()


def test_controller_validates_binding_authority_before_manager_construction(
        monkeypatch):
    manager = mock.Mock()
    monkeypatch.setattr(controller.replica_managers, 'SkyPilotReplicaManager',
                        manager)
    validator = mock.Mock(side_effect=RuntimeError('stale authority'))
    monkeypatch.setattr(controller.ordinary_launch_binding,
                        'validate_controller_authority', validator)

    with pytest.raises(RuntimeError, match='stale authority'):
        controller.SkyServeController(
            'svc',
            mock.Mock(),
            version=1,
            host='127.0.0.1',
            port=20001,
            controller_owner_fingerprint='fingerprint',
            service_hash='incarnation-a',
            controller_pid=123,
            controller_ip='10.0.0.1',
            controller_binding_authority=mock.sentinel.authority)

    validator.assert_called_once()
    manager.assert_not_called()


def test_run_controller_uses_parent_owner_for_child_cutover_fence(monkeypatch):
    """The child fence must compare the durable parent owner, not its PID."""
    actual_controller_class = controller.SkyServeController
    parent_pid = os.getpid() + 1000
    observed = {}

    class _WiredController:

        def __init__(self, service_name, _spec, _version, _host, _port,
                     _fingerprint, _scope, service_hash, controller_pid,
                     controller_ip, _enforce_launch_fence):
            self.actual = actual_controller_class.__new__(
                actual_controller_class)
            self.actual._service_name = service_name
            self.actual._service_hash = service_hash
            self.actual._controller_owner = (controller_pid, controller_ip)

        def run(self):
            observed['fence'] = self.actual._lb_cutover_fence()

    monkeypatch.setattr(controller, 'SkyServeController', _WiredController)
    monkeypatch.setattr(controller.context_utils, 'hijack_sys_attrs',
                        mock.Mock())
    monkeypatch.setattr(
        controller.serve_state, 'get_service_controller_owner',
        lambda *_args, **_kwargs: {
            'hash': 'incarnation-a',
            'controller_pid': parent_pid,
            'controller_ip': '10.0.0.1',
            'lifecycle_epoch': 7,
            'lb_ha_enabled': True,
        })

    controller.run_controller('svc', mock.Mock(), 1, '127.0.0.1', 20001,
                              'fingerprint', None, 'incarnation-a', parent_pid,
                              '10.0.0.1', False)

    assert observed['fence'] == ('incarnation-a', (parent_pid, '10.0.0.1'), 7)


def test_recovery_lease_child_dependencies_require_sync_and_owner_tokens(
        monkeypatch):
    monkeypatch.setattr(controller.serve_utils,
                        'get_lb_sync_auth_tokens',
                        lambda required=False: ('sync-token',)
                        if required else ())
    sync_dependency = controller._make_auth_dependency(  # pylint: disable=protected-access
        sync=True, required=True)
    owner_dependency = controller._make_controller_owner_dependency(  # pylint: disable=protected-access
        'owner-fingerprint')

    with pytest.raises(controller.fastapi.HTTPException) as missing_sync:
        asyncio.run(sync_dependency(None))
    assert missing_sync.value.status_code == 401
    asyncio.run(sync_dependency('Bearer sync-token'))

    with pytest.raises(controller.fastapi.HTTPException) as wrong_owner:
        asyncio.run(owner_dependency('wrong-owner'))
    assert wrong_owner.value.status_code == 409
    asyncio.run(owner_dependency('owner-fingerprint'))


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
        self.replica_record_id = (f'00000000-0000-0000-0000-{replica_id:012d}')
        self.cluster_name = f'replica-{replica_id}'
        self.status = status
        self.version = version
        self._url = url
        self._accelerators = accelerators
        self._handle_is_none = handle_is_none
        self.url_resolutions = 0
        self.handle_resolutions = 0
        self.last_provider_config = None
        self.planned_capacity = 1
        self.logical_bridge_capacity_verified = False
        self.resources_override = None
        self.reserved_fill = False
        self.unknown_capacity_replacement = False
        self.created_at = None
        self.cost_rebalance_for_replica_id = None
        self.status_property = types.SimpleNamespace(
            is_scale_down=False,
            sky_down_status=None,
            first_ready_time=None,
            sky_launch_status=None,
        )
        # Most routing tests exercise the protocol's legacy/unknown omission
        # shape. Tests for persisted provenance override this with a bool.
        self.is_zero_cost = None
        self.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.ORDINARY)

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
    ctrl._service_hash = None  # pylint: disable=protected-access
    ctrl._controller_owner = None  # pylint: disable=protected-access
    ctrl._history_session_id = 'test-session'  # pylint: disable=protected-access
    ctrl._lb_replica_cache = {}  # pylint: disable=protected-access
    ctrl._lb_replica_cache_record_ids = {}  # pylint: disable=protected-access
    ctrl._lb_translation_cache = {}  # pylint: disable=protected-access
    ctrl._lb_translation_cache_record_ids = {}  # pylint: disable=protected-access
    ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
        yaml_content=None,
        spot_placer=None,
        system_recovery_allows_routing=lambda _info: True,
        system_recovery_route_marker=lambda _info, _url: None,
        retire_system_recovery_route=lambda _info: None)
    ctrl._autoscaler = None  # pylint: disable=protected-access
    ctrl._lb_sync_lock = None  # pylint: disable=protected-access
    ctrl._lb_role_lock = None  # pylint: disable=protected-access
    ctrl._lb_role_snapshot_task = None  # pylint: disable=protected-access
    ctrl._lb_role_snapshot_key = None  # pylint: disable=protected-access
    ctrl._lb_role_executor = concurrent.futures.ThreadPoolExecutor(  # pylint: disable=protected-access
        max_workers=2,
        thread_name_prefix='test-skyserve-ha-role')
    ctrl._lb_role_snapshot_read_executor = (  # pylint: disable=protected-access
        concurrent.futures.ThreadPoolExecutor(
            max_workers=4, thread_name_prefix='test-skyserve-ha-role-snapshot'))
    ctrl._lb_demand_lock = None  # pylint: disable=protected-access
    ctrl._lb_ha_enabled = False  # pylint: disable=protected-access
    ctrl._lb_session_ledger = None  # pylint: disable=protected-access
    ctrl._lb_expected_occupancy_urls = set()  # pylint: disable=protected-access
    ctrl._lb_occupancy_contract_known = False  # pylint: disable=protected-access
    ctrl._lb_last_demand_snapshot = None  # pylint: disable=protected-access
    ctrl._lb_demand_handoff = controller.lb_ha.DemandHandoff(  # pylint: disable=protected-access
        constants.LB_DEMAND_HANDOFF_SECONDS)
    ctrl._routing_spec = None  # pylint: disable=protected-access
    ctrl._applied_version = 1  # pylint: disable=protected-access
    ctrl._routing_state_lock = threading.RLock()  # pylint: disable=protected-access
    ctrl._route_projection_contract_lock = threading.Lock()  # pylint: disable=protected-access
    ctrl._route_projection_contract = (1, {})  # pylint: disable=protected-access
    ctrl._actuation_epoch_lock = threading.RLock()  # pylint: disable=protected-access
    ctrl._actuation_generation = 0  # pylint: disable=protected-access
    ctrl._actuation_stop = threading.Event()  # pylint: disable=protected-access
    ctrl._update_reconciler_stop = threading.Event()  # pylint: disable=protected-access
    ctrl._update_recovery_required = False  # pylint: disable=protected-access
    ctrl._reconcile_generation = 0  # pylint: disable=protected-access
    ctrl._durable_demand_snapshot = None  # pylint: disable=protected-access
    ctrl._ordinary_launch_binding_authority = None  # pylint: disable=protected-access
    ctrl._is_pool = False  # pylint: disable=protected-access
    ctrl._reserved_capacity_fill_enabled = False  # pylint: disable=protected-access
    ctrl._scale_reconcile_coordinator = (  # pylint: disable=protected-access
        controller.scale_reconciliation.ScaleReconcileCoordinator(
            ctrl._reconcile_scale_once))
    return ctrl


def test_route_projection_contract_is_independent_and_immutable():
    ctrl = _make_controller()
    routing_spec = {'policy': {'name': 'round_robin'}}
    ctrl._publish_route_projection_contract(  # pylint: disable=protected-access
        2, routing_spec)
    routing_spec['policy']['name'] = 'mutated'

    version, first = ctrl._get_route_projection_contract()  # pylint: disable=protected-access
    first['policy']['name'] = 'also-mutated'
    assert version == 2
    assert ctrl._get_route_projection_contract() == (  # pylint: disable=protected-access
        2, {
            'policy': {
                'name': 'round_robin'
            }
        })


def test_incremental_route_compose_ignores_autoscaler_epoch_lock():
    ctrl = _make_controller()
    ctrl._ordinary_launch_binding_authority = object()  # pylint: disable=protected-access
    ctrl._publish_route_projection_contract(  # pylint: disable=protected-access
        2, {'policy': 'round_robin'})
    repository = mock.Mock()
    finished = threading.Event()

    def _compose():
        ctrl._compose_incremental_route_projection()  # pylint: disable=protected-access
        finished.set()

    with mock.patch.object(controller.route_projection,
                           'RouteProjectionRepository',
                           return_value=repository), \
         mock.patch.object(controller.route_projection,
                           'publisher_identity_from_authority',
                           return_value=object()), \
         mock.patch.object(
             controller.serve_state,
             'get_spec',
             return_value=types.SimpleNamespace(
                 endpoint_probe_interval_seconds=5)):
        # Simulate an indefinitely blocked cost/provider planning pass.  The
        # protocol-2 composer must use only its independent contract snapshot.
        with ctrl._routing_state_lock:  # pylint: disable=protected-access
            worker = threading.Thread(target=_compose)
            worker.start()
            assert finished.wait(timeout=5)
        worker.join(timeout=5)

    assert not worker.is_alive()
    compose = repository.compose_incremental_snapshot
    assert compose.call_count == 1
    assert compose.call_args.args[1:3] == (2, {'policy': 'round_robin'})


def _explicit_placement_contract_spec():
    spec = types.SimpleNamespace(_spot_placer=None, _pool=False)
    contract = placement_policy.resolve_fresh_contract(None, False)
    spec.__dict__.update(contract.persisted_fields())
    return spec


def test_cost_rebalance_state_persistence_is_owner_fenced():
    ctrl = _make_controller()
    ctrl._service_hash = 'incarnation-a'
    ctrl._controller_owner = (123, '10.0.0.1')
    scaler = mock.Mock()
    scaler.cost_rebalance_state_dirty = True
    scaler.dump_cost_rebalance_state.return_value = {
        'version': 1,
        'candidates': [],
    }

    with mock.patch.object(controller.serve_state,
                           'set_service_cost_rebalance_state',
                           return_value=True) as persist:
        assert ctrl._persist_cost_rebalance_state(scaler)

    persist.assert_called_once_with('svc', 'incarnation-a', (123, '10.0.0.1'),
                                    scaler.dump_cost_rebalance_state())
    scaler.mark_cost_rebalance_state_persisted.assert_called_once_with()


def test_cost_rebalance_state_db_error_suppresses_only_economic_work():
    ctrl = _make_controller()
    ctrl._service_hash = 'incarnation-a'
    ctrl._controller_owner = (123, '10.0.0.1')
    scaler = mock.Mock()
    scaler.cost_rebalance_state_dirty = True
    scaler.dump_cost_rebalance_state.return_value = {'version': 1}

    with mock.patch.object(controller.serve_state,
                           'set_service_cost_rebalance_state',
                           side_effect=RuntimeError('database unavailable')):
        assert not ctrl._persist_cost_rebalance_state(scaler)

    scaler.mark_cost_rebalance_state_persisted.assert_not_called()


def test_controller_startup_acknowledges_pending_normalization(monkeypatch):
    ctrl = _make_controller()
    ctrl._service_hash = 'incarnation-a'
    ctrl._controller_owner = (123, '10.0.0.1')
    ctrl._committed_version = 3
    ctrl._history_session_id = 'a' * 32
    request = serve_state.PlacementNormalizationRequest(
        run_id=controller.uuid.uuid4(),
        recovery_version=2,
        current_version=3,
        lifecycle_epoch=7)
    read_request = mock.Mock(return_value=request)
    acknowledge = mock.Mock(return_value=True)
    monkeypatch.setattr(controller.serve_state,
                        'get_placement_normalization_request', read_request)
    monkeypatch.setattr(controller.serve_state,
                        'acknowledge_placement_normalization_loaded',
                        acknowledge)
    monkeypatch.setattr(controller, 'sky_commit', 'commit-a')
    monkeypatch.setattr(controller.os, 'getpid', lambda: 456)

    ctrl._acknowledge_pending_placement_normalization(
        _explicit_placement_contract_spec(), 2)

    read_request.assert_called_once_with('svc',
                                         recovery_version=2,
                                         current_version=3,
                                         expected_service_hash='incarnation-a',
                                         expected_controller_owner=(123,
                                                                    '10.0.0.1'))
    acknowledge.assert_called_once_with('svc',
                                        request,
                                        expected_service_hash='incarnation-a',
                                        expected_controller_owner=(123,
                                                                   '10.0.0.1'),
                                        image_commit='commit-a',
                                        child_controller_pid=456,
                                        boot_id='a' * 32)


def test_controller_startup_fails_when_normalization_receipt_cas_is_stale():
    ctrl = _make_controller()
    ctrl._service_hash = 'incarnation-a'
    ctrl._controller_owner = (123, '10.0.0.1')
    ctrl._committed_version = 1
    request = serve_state.PlacementNormalizationRequest(
        run_id=controller.uuid.uuid4(),
        recovery_version=1,
        current_version=1,
        lifecycle_epoch=7)
    with mock.patch.object(controller.serve_state,
                           'get_placement_normalization_request',
                           return_value=request), mock.patch.object(
                               controller.serve_state,
                               'acknowledge_placement_normalization_loaded',
                               return_value=False), pytest.raises(
                                   RuntimeError, match='Could not acknowledge'):
        ctrl._acknowledge_pending_placement_normalization(
            _explicit_placement_contract_spec(), 1)


def test_controller_startup_rejects_fieldless_placement_contract():
    ctrl = _make_controller()
    fieldless = types.SimpleNamespace(_spot_placer=None,
                                      _pool=False,
                                      _uses_logical_replicas=False)
    with mock.patch.object(
            controller.serve_state,
            'get_placement_normalization_request') as read_request, \
            pytest.raises(RuntimeError, match='fieldless legacy'):
        ctrl._acknowledge_pending_placement_normalization(fieldless, 1)
    read_request.assert_not_called()


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
                 lb_retry_initial_backoff_seconds=None,
                 target_concurrency_per_replica=None,
                 lb_request_queue=None,
                 reserved_capacity_fill=False,
                 uses_logical_replicas=False) -> None:
        self.load_balancing_policy = load_balancing_policy
        self.target_qps_per_replica = target_qps_per_replica
        self.lb_stream_timeout_seconds = lb_stream_timeout_seconds
        self.lb_retriable_status_codes = lb_retriable_status_codes
        self.lb_max_retries = lb_max_retries
        self.lb_retry_initial_backoff_seconds = (
            lb_retry_initial_backoff_seconds)
        self.target_concurrency_per_replica = (target_concurrency_per_replica)
        self.lb_request_queue = lb_request_queue
        self.reserved_capacity_fill = reserved_capacity_fill
        self.uses_logical_replicas = uses_logical_replicas


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
            'target_concurrency_per_replica': None,
            'stream_timeout_seconds': 120,
            'retriable_status_codes': [503],
            'max_retries': 3,
            'retry_initial_backoff_seconds': 0.5,
            'request_queue': None,
        }

    def test_routing_spec_preserves_scalar_qps(self):
        ctrl = _make_controller()
        spec = _FakeSpec(load_balancing_policy='round_robin',
                         target_qps_per_replica=2.5,
                         lb_stream_timeout_seconds=30)

        routing_spec = ctrl._build_routing_spec(spec)  # pylint: disable=protected-access

        assert routing_spec['target_qps_per_replica'] == 2.5

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

    def test_concurrency_autoscaler_advertises_exact_card_capability(self):
        ctrl = _make_controller()
        ctrl._autoscaler = mock.Mock(  # pylint: disable=protected-access
            spec=controller.autoscalers.ConcurrencyAutoscaler)
        spec = _FakeSpec(load_balancing_policy='instance_aware_least_load',
                         target_qps_per_replica=None,
                         target_concurrency_per_replica=1,
                         lb_stream_timeout_seconds=120)
        with mock.patch.object(ctrl,
                               '_configured_accelerators',
                               return_value=['L4', 'A100']):
            routing_spec = ctrl._build_routing_spec(spec)  # pylint: disable=protected-access

        assert routing_spec['target_concurrency_per_replica'] == 1
        assert routing_spec['request_accelerator_compatibility_version'] == 1
        assert routing_spec['configured_accelerators'] == ['L4', 'A100']

    def test_concurrency_least_load_withholds_exact_card_capability(self):
        ctrl = _make_controller()
        ctrl._autoscaler = mock.Mock(  # pylint: disable=protected-access
            spec=controller.autoscalers.ConcurrencyAutoscaler)
        spec = _FakeSpec(load_balancing_policy='least_load',
                         target_qps_per_replica=None,
                         target_concurrency_per_replica=1,
                         lb_stream_timeout_seconds=120)
        with mock.patch.object(ctrl,
                               '_configured_accelerators',
                               return_value=['L4', 'A100'
                                            ]) as configured_accelerators:
            routing_spec = ctrl._build_routing_spec(spec)  # pylint: disable=protected-access

        assert routing_spec['target_concurrency_per_replica'] == 1
        assert 'request_accelerator_compatibility_version' not in routing_spec
        assert 'configured_accelerators' not in routing_spec
        configured_accelerators.assert_not_called()
        ctrl._configure_instance_aware_accelerators(spec)  # pylint: disable=protected-access
        ctrl._autoscaler.set_configured_accelerator_shapes.assert_called_once_with(  # pylint: disable=line-too-long
            {})

    def test_compatibility_report_requires_applied_routing_version(self):
        ctrl = _make_controller()
        report = {
            'routing_version': ctrl._applied_version,  # pylint: disable=protected-access
            'in_flight': {},
            'queue_depth': 0,
            'rejected_in_window': 0,
            'rejected_in_recent_window': 0,
            'unknown_in_flight_urls': [],
            'queued_requests_by_compatibility': [],
            'rejected_requests_by_compatibility': [],
        }

        assert ctrl._compatibility_demand_report_is_complete(  # pylint: disable=protected-access
            report)
        report['routing_version'] -= 1
        assert not ctrl._compatibility_demand_report_is_complete(  # pylint: disable=protected-access
            report)
        del report['routing_version']
        assert not ctrl._compatibility_demand_report_is_complete(  # pylint: disable=protected-access
            report)

    def test_only_instance_aware_autoscaler_advertises_exact_card_capability(
            self):
        ctrl = _make_controller()
        spec = _FakeSpec(load_balancing_policy='instance_aware_least_load',
                         target_qps_per_replica={'A100': 1.0},
                         lb_stream_timeout_seconds=30)
        with mock.patch.object(ctrl,
                               '_configured_accelerators',
                               return_value=['A100']):
            ctrl._autoscaler = mock.Mock(  # pylint: disable=protected-access
                spec=controller.autoscalers.RequestRateAutoscaler)
            legacy = ctrl._build_routing_spec(spec)  # pylint: disable=protected-access
            assert 'request_accelerator_compatibility_version' not in legacy

            ctrl._autoscaler = mock.Mock(  # pylint: disable=protected-access
                spec=controller.autoscalers.InstanceAwareRequestRateAutoscaler)
            capable = ctrl._build_routing_spec(spec)  # pylint: disable=protected-access
            assert capable['request_accelerator_compatibility_version'] == 1
            assert capable['configured_accelerators'] == ['A100']

    def test_controller_feeds_exact_task_gpu_counts_to_autoscaler(self):
        ctrl = _make_controller()
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            yaml_content='service: {}',
            spot_placer=None)
        ctrl._autoscaler = mock.Mock(  # pylint: disable=protected-access
            spec=controller.autoscalers.InstanceAwareRequestRateAutoscaler)
        task = types.SimpleNamespace(resources=[
            types.SimpleNamespace(accelerators={'A100': 8}),
            types.SimpleNamespace(accelerators={'A100-80GB': 1}),
        ])
        spec = types.SimpleNamespace(
            min_replicas_by_accelerator={},
            load_balancing_policy='instance_aware_least_load')
        with mock.patch.object(controller.replica_managers,
                               'load_task_with_service_spec',
                               return_value=task):
            ctrl._configure_instance_aware_accelerators(  # pylint: disable=protected-access
                spec)
        ctrl._autoscaler.set_configured_accelerator_shapes.assert_called_once_with(  # pylint: disable=line-too-long
            {
                'A100': 8,
                'A100-80GB': 1,
            })

    def test_unordered_any_of_without_placer_withholds_compatibility(self):
        ctrl = _make_controller()
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            yaml_content='service: {}',
            spot_placer=None)
        l4 = mock.Mock(accelerators={'L4': 1})
        a100 = mock.Mock(accelerators={'A100': 1})
        task = types.SimpleNamespace(resources={l4, a100})
        spec = types.SimpleNamespace(min_replicas_by_accelerator={},
                                     target_qps_per_replica={
                                         'L4': 1.0,
                                         'A100': 1.0,
                                     })
        with mock.patch.object(controller.replica_managers,
                               'load_task_with_service_spec',
                               return_value=task):
            configured = ctrl._configured_accelerators(  # pylint: disable=protected-access
                spec)

        assert not configured

    def test_configured_catalog_uses_nominal_cost_order_when_card_benched(self):
        ctrl = _make_controller()
        l4_location = mock.Mock(accelerators={'L4': 1})
        a100_location = mock.Mock(accelerators={'A100': 1})
        placer = mock.Mock()
        placer.active_locations.return_value = [a100_location]
        placer.known_location_costs.return_value = {
            l4_location: 1.0,
            a100_location: 2.0,
        }
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            yaml_content='service: {}',
            spot_placer=placer)
        task = types.SimpleNamespace(resources=[
            types.SimpleNamespace(accelerators={'L4': 1}),
            types.SimpleNamespace(accelerators={'A100': 1}),
        ])
        spec = types.SimpleNamespace(min_replicas_by_accelerator={},
                                     target_qps_per_replica={
                                         'L4': 1.0,
                                         'A100': 1.0,
                                     })
        with mock.patch.object(controller.replica_managers,
                               'load_task_with_service_spec',
                               return_value=task):
            configured = ctrl._configured_accelerators(  # pylint: disable=protected-access
                spec)

        assert configured == ['L4', 'A100']
        placer.known_location_costs.assert_called_once_with()
        placer.cost_per_hour.assert_not_called()

    def test_configured_catalog_preserves_order_when_price_is_unknown(self):
        ctrl = _make_controller()
        l4_location = mock.Mock(accelerators={'L4': 1})
        a100_location = mock.Mock(accelerators={'A100': 1})
        placer = mock.Mock()
        placer.known_location_costs.return_value = {
            l4_location: float('inf'),
            a100_location: 2.0,
        }
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            yaml_content='service: {}',
            spot_placer=placer)
        task = types.SimpleNamespace(resources=[
            types.SimpleNamespace(accelerators={'L4': 1}),
            types.SimpleNamespace(accelerators={'A100': 1}),
        ])
        spec = types.SimpleNamespace(min_replicas_by_accelerator={},
                                     target_qps_per_replica={
                                         'L4': 1.0,
                                         'A100': 1.0,
                                     })
        with mock.patch.object(controller.replica_managers,
                               'load_task_with_service_spec',
                               return_value=task):
            configured = ctrl._configured_accelerators(  # pylint: disable=protected-access
                spec)

        assert configured == ['L4', 'A100']

    def test_configured_catalog_preserves_order_when_one_location_is_uncached(
            self):
        ctrl = _make_controller()
        l4_paid = mock.Mock(accelerators={'L4': 1})
        l4_uncached = mock.Mock(accelerators={'L4': 1})
        a100_paid = mock.Mock(accelerators={'A100': 1})
        costs = {
            l4_paid: 2.0,
            l4_uncached: float('inf'),
            a100_paid: 1.0,
        }
        placer = mock.Mock()
        placer.known_location_costs.return_value = costs
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            yaml_content='service: {}',
            spot_placer=placer)
        task = types.SimpleNamespace(resources=[
            types.SimpleNamespace(accelerators={'L4': 1}),
            types.SimpleNamespace(accelerators={'A100': 1}),
        ])
        spec = types.SimpleNamespace(min_replicas_by_accelerator={},
                                     target_qps_per_replica={
                                         'L4': 1.0,
                                         'A100': 1.0,
                                     })
        with mock.patch.object(controller.replica_managers,
                               'load_task_with_service_spec',
                               return_value=task):
            configured = ctrl._configured_accelerators(  # pylint: disable=protected-access
                spec)

        assert configured == ['L4', 'A100']
        placer.known_location_costs.assert_called_once_with()
        placer.cost_per_hour.assert_not_called()

    def test_prebind_accelerator_configuration_never_resolves_provider_cost(
            self):
        ctrl = _make_controller()
        l4_location = mock.Mock(accelerators={'L4': 1})
        a100_location = mock.Mock(accelerators={'A100': 1})
        placer = mock.Mock()
        placer.known_location_costs.return_value = {
            l4_location: float('inf'),
            a100_location: float('inf'),
        }
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            yaml_content='service: {}',
            spot_placer=placer)
        ctrl._autoscaler = mock.Mock(  # pylint: disable=protected-access
            spec=controller.autoscalers.ConcurrencyAutoscaler)
        task = types.SimpleNamespace(resources=[
            types.SimpleNamespace(accelerators={'L4': 1}),
            types.SimpleNamespace(accelerators={'A100': 1}),
        ])
        spec = _FakeSpec(load_balancing_policy='instance_aware_least_load',
                         target_qps_per_replica=None,
                         target_concurrency_per_replica=1,
                         lb_stream_timeout_seconds=120)
        spec.min_replicas_by_accelerator = {}
        with mock.patch.object(controller.replica_managers,
                               'load_task_with_service_spec',
                               return_value=task):
            ctrl._configure_instance_aware_accelerators(  # pylint: disable=protected-access
                spec)
            routing_spec = ctrl._build_routing_spec(spec)  # pylint: disable=protected-access

        assert routing_spec['configured_accelerators'] == ['L4', 'A100']
        assert placer.known_location_costs.call_count == 2
        placer.cost_per_hour.assert_not_called()

    def test_routing_spec_none_when_uninitialized(self):
        ctrl = _make_controller()
        assert ctrl._get_routing_spec() is None  # pylint: disable=protected-access

    @pytest.mark.parametrize('new_target_qps', [2.5, {'L4': 2.5}])
    def test_apply_service_update_keeps_old_spec_until_runtime_transition(
            self, new_target_qps):
        ctrl = _make_controller()
        old_spec = _FakeSpec(load_balancing_policy='round_robin',
                             target_qps_per_replica=None,
                             lb_stream_timeout_seconds=30)
        new_spec = _FakeSpec(load_balancing_policy='instance_aware_least_load',
                             target_qps_per_replica=new_target_qps,
                             lb_stream_timeout_seconds=90)
        ctrl._routing_spec = ctrl._build_routing_spec(old_spec)  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._autoscaler = mock.MagicMock()  # pylint: disable=protected-access
        ctrl._autoscaler.replica_unit = 'physical'  # pylint: disable=protected-access
        ctrl._mark_controller_applied_version = mock.Mock(  # pylint: disable=protected-access
            return_value=True)
        ctrl._seed_fill_zero_cost_locations = mock.Mock()  # pylint: disable=protected-access
        ctrl._start_reserved_capacity_poller_if_needed = mock.Mock()  # pylint: disable=protected-access

        entered_runtime_transition = threading.Event()
        resume_runtime_transition = threading.Event()

        def _block_runtime_transition(*_args, **_kwargs):
            assert ctrl.get_actuation_generation() == 1
            assert not ctrl._actuation_epoch_lock._is_owned()  # pylint: disable=protected-access
            entered_runtime_transition.set()
            assert resume_runtime_transition.wait(timeout=5)

        ctrl._replica_manager.update_version.side_effect = (  # pylint: disable=protected-access
            _block_runtime_transition)

        new_autoscaler = mock.MagicMock()
        candidate_placer = mock.sentinel.candidate_placer
        with mock.patch.object(
                controller.replica_managers,
                'validate_service_update_preflight',
                return_value=candidate_placer), \
             mock.patch.object(controller.serve_state,
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
            assert ctrl._applied_version == 1  # pylint: disable=protected-access
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
        ctrl._replica_manager.update_version.assert_called_once_with(  # pylint: disable=line-too-long
            2,
            new_spec,
            update_mode=mock.sentinel.mode,
            new_spot_placer=candidate_placer)
        assert ctrl._applied_version == 2  # pylint: disable=protected-access
        ctrl._mark_controller_applied_version.assert_called_once_with(  # pylint: disable=line-too-long,protected-access
            2)
        assert ctrl._get_routing_spec() == {  # pylint: disable=protected-access
            'load_balancing_policy_name': 'instance_aware_least_load',
            'target_qps_per_replica': new_target_qps,
            'target_concurrency_per_replica': None,
            'stream_timeout_seconds': 90,
            'retriable_status_codes': None,
            'max_retries': None,
            'retry_initial_backoff_seconds': None,
            'request_queue': None,
        }


def _make_update_controller() -> controller.SkyServeController:
    ctrl = _make_controller()
    ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
        replica_unit='physical_backend')
    ctrl._service_hash = 'incarnation-a'  # pylint: disable=protected-access
    ctrl._controller_owner = (123, '10.0.0.1')  # pylint: disable=protected-access
    ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
    ctrl._update_condition = threading.Condition()  # pylint: disable=protected-access
    ctrl._pending_update = None  # pylint: disable=protected-access
    ctrl._applying_update = None  # pylint: disable=protected-access
    ctrl._update_recovery_required = False  # pylint: disable=protected-access
    ctrl._update_reconciler_stop = threading.Event()  # pylint: disable=protected-access
    ctrl._committed_version = 1  # pylint: disable=protected-access
    ctrl._applied_version = 1  # pylint: disable=protected-access
    ctrl._update_apply_error = None  # pylint: disable=protected-access
    ctrl._update_apply_failures = 0  # pylint: disable=protected-access
    ctrl._quarantined_version = None  # pylint: disable=protected-access
    ctrl._quarantined_at = None  # pylint: disable=protected-access
    ctrl._quarantine_reason = None  # pylint: disable=protected-access
    ctrl._update_still_authorized = mock.Mock(  # pylint: disable=protected-access
        return_value=True)
    ctrl._mark_controller_applied_version = mock.Mock(  # pylint: disable=protected-access
        return_value=True)
    return ctrl


def _make_update_spec(
        replica_unit: str = 'physical_backend') -> types.SimpleNamespace:
    """Build the explicit service interface consumed by update tests."""
    policy_name = (placement_policy.CAPACITY_AWARE_SPOT_PLACER
                   if replica_unit == 'logical' else None)
    return types.SimpleNamespace(
        replica_unit=replica_unit,
        uses_logical_replicas=(replica_unit == 'logical'),
        spot_placer=policy_name,
        placement_contract=placement_policy.resolve_fresh_contract(policy_name,
                                                                   pool=False),
    )


def _make_legacy_physical_per_gpu_spec() -> controller.serve.SkyServiceSpec:
    """Build the real fieldless contract written before logical activation."""
    current = controller.serve.SkyServiceSpec(
        readiness_path='/health',
        initial_delay_seconds=1,
        readiness_timeout_seconds=2,
        endpoint_probe_interval_seconds=3,
        lb_stream_timeout_seconds=4,
        min_replicas=0,
        max_replicas=8,
        target_concurrency_per_replica=1,
        graceful_drain_async_occupancy=True,
        spot_placer=placement_policy.CAPACITY_AWARE_SPOT_PLACER)
    legacy_state = dict(current.__dict__)
    for field in placement_policy.CONTRACT_FIELDS:
        legacy_state.pop(field)
    legacy_state.pop(placement_policy.ROLLBACK_REPLICA_UNIT_FIELD, None)
    restored = controller.serve.SkyServiceSpec.__new__(
        controller.serve.SkyServiceSpec)
    restored.__setstate__(legacy_state)
    return restored


def _make_autoscaler_spec(**overrides) -> types.SimpleNamespace:
    """Build the explicit SkyServiceSpec interface used by autoscalers."""
    values = {
        'min_replicas': 0,
        'min_replicas_by_accelerator': {},
        'max_replicas': 20,
        'num_overprovision': None,
        'replica_unit': 'physical_backend',
        'target_qps_per_replica': None,
        'target_concurrency_per_replica': None,
        'pool': False,
        'use_ondemand_fallback': False,
        'queue_length_threshold': None,
        'upscale_delay_seconds': None,
        'downscale_delay_seconds': None,
        'reserved_capacity_fill': False,
        'reserved_fill_floor_replicas': 0,
        'reserved_fill_weight': 1.0,
        'reserved_fill_utilization_gate': False,
        'cost_rebalance': False,
        'cost_rebalance_min_savings_fraction': 0.3,
        'cost_rebalance_max_parallel_replacements': 1,
        'cost_rebalance_stabilization_seconds': 300.0,
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


def _make_prepared_controller_config(
    version: int,
    *,
    staged_path: str | None = None,
    source_is_staged: bool = True
) -> controller._PreparedControllerConfig:  # pylint: disable=protected-access
    config = mock.Mock(name=f'config_v{version}')
    config.get_nested.return_value = 'research'
    return controller._PreparedControllerConfig(  # pylint: disable=protected-access
        config=config,
        service_name='svc',
        live_path=f'/tmp/config.yaml.v{version}',
        staged_path=(staged_path or f'/tmp/config.yaml.v{version}.staged'),
        recovery_script=f'recovery-v{version}',
        version=version,
        snapshot_id=f'{version:x}' * 64,
        source_digest=f'{version + 1:x}' * 64,
        durable_bytes=f'durable-v{version}'.encode(),
        durable_digest=f'{version + 2:x}' * 64,
        source_is_staged=source_is_staged,
        source_is_live=False,
        legacy_snapshot=None)


def test_orphaned_config_stage_sweeper_serializes_with_update_handler(
        monkeypatch):
    ctrl = _make_update_controller()
    ctrl._update_lock = threading.Lock()  # pylint: disable=protected-access
    observed_lock = []

    def _gc(service_name, resource_scope):
        assert service_name == 'svc'
        assert resource_scope is None
        observed_lock.append(ctrl._update_lock.locked())  # pylint: disable=protected-access
        ctrl._update_reconciler_stop.set()  # pylint: disable=protected-access
        return [2]

    monkeypatch.setattr(controller.serve_utils,
                        'gc_orphaned_staged_controller_configs', _gc)

    ctrl._run_orphaned_config_stage_sweeper()  # pylint: disable=protected-access

    assert observed_lock == [True]


def _register_update_test_routes(
        ctrl: controller.SkyServeController,
        monkeypatch: pytest.MonkeyPatch) -> fastapi_testclient.TestClient:
    """Register real FastAPI routes without starting controller threads."""

    async def _allow_request():
        return None

    monkeypatch.setenv(controller.constants.OVERRIDE_CONSOLIDATION_MODE, 'true')
    monkeypatch.setattr(controller, '_make_auth_dependency',
                        lambda **unused_kwargs: _allow_request)
    monkeypatch.setattr(controller, '_make_controller_owner_dependency',
                        lambda unused_fingerprint: _allow_request)
    monkeypatch.setattr(controller.serve_utils,
                        'get_lb_sync_auth_tokens',
                        lambda required=False: {'test-token'})
    monkeypatch.setattr(controller.serve_utils,
                        'get_controller_admin_auth_tokens',
                        lambda required=False: {'test-token'})
    monkeypatch.setattr(controller.thread_utils, 'start_supervised_thread',
                        lambda *unused_args, **unused_kwargs: None)
    monkeypatch.setattr(controller.uvicorn, 'run',
                        lambda *unused_args, **unused_kwargs: None)
    monkeypatch.setattr(controller.os, '_exit', lambda unused_code: None)
    ctrl._app = controller.fastapi.FastAPI()  # pylint: disable=protected-access
    ctrl._is_pool = False  # pylint: disable=protected-access
    ctrl._controller_owner_fingerprint = 'owner'  # pylint: disable=protected-access
    ctrl._update_lock = threading.Lock()  # pylint: disable=protected-access
    ctrl._host = '127.0.0.1'  # pylint: disable=protected-access
    ctrl._port = 30000  # pylint: disable=protected-access
    ctrl._reserved_capacity_fill_enabled = False  # pylint: disable=protected-access
    ctrl._incremental_route_projection_enabled = False  # pylint: disable=protected-access
    ctrl.run()
    return fastapi_testclient.TestClient(ctrl._app)  # pylint: disable=protected-access


def _binding_authority(mode: str, epoch: int, *, generic: bool = False):
    binding = controller.ordinary_launch_binding
    return binding.ControllerBindingAuthority(
        service_name='svc',
        service_hash='incarnation-a',
        service_workspace='workspace-a',
        service_lifecycle_epoch=4,
        controller_pid=123,
        controller_ip='10.0.0.1',
        controller_incarnation=controller.uuid.UUID(
            '33333333-3333-4333-8333-333333333333'),
        controller_owner_epoch=6,
        capable=True,
        binding_mode=binding.BindingMode(mode),
        binding_epoch=epoch,
        non_pool_capable=generic,
        non_pool_binding_protocol_version=(
            binding.NON_POOL_BINDING_PROTOCOL_VERSION if generic else None),
        non_pool_profile_set_digest=(
            binding.supported_non_pool_profile_set_digest()
            if generic else None),
        non_pool_capability_cohort_epoch=(
            binding.NON_POOL_CAPABILITY_COHORT_EPOCH if generic else None),
        non_pool_receipt_protocol_version=(
            binding.NON_POOL_RECEIPT_PROTOCOL_VERSION if generic else None))


def _resident_placement_page():
    return {
        'available': True,
        'enabled': True,
        'pagination_version': 1,
        'page_offset': 0,
        'next_offset': None,
        'total_locations': 1,
        'locations': [{
            'cloud': 'Kubernetes',
            'region': 'research',
        }],
        'truncated': False,
    }


def test_placement_route_default_is_resident_only(monkeypatch):
    ctrl = _make_update_controller()
    placer = mock.Mock()
    placer.placement_snapshot.return_value = _resident_placement_page()
    ctrl._replica_manager.spot_placer = placer  # pylint: disable=protected-access
    ctrl._replica_manager.workspace = 'workspace-a'  # pylint: disable=protected-access
    get_replicas = mock.Mock()
    build_budget = mock.Mock()
    monkeypatch.setattr(controller.serve_state, 'get_replica_infos',
                        get_replicas)
    monkeypatch.setattr(controller.paid_capacity, 'build_launch_budget',
                        build_budget)

    client = _register_update_test_routes(ctrl, monkeypatch)
    response = client.get(constants.CONTROLLER_PLACEMENT_ENDPOINT_PATH,
                          params={'limit': 100})

    assert response.status_code == 200
    assert response.json() == _resident_placement_page()
    get_replicas.assert_not_called()
    build_budget.assert_not_called()
    placer.placement_snapshot.assert_called_once_with(
        limit=100, offset=0, paid_admission_by_location=None)


def test_placement_route_replica_read_failure_preserves_resident_page(
        monkeypatch):
    ctrl = _make_update_controller()
    placer = mock.Mock()
    placer.placement_snapshot.return_value = _resident_placement_page()
    ctrl._replica_manager.spot_placer = placer  # pylint: disable=protected-access
    ctrl._replica_manager.workspace = 'workspace-a'  # pylint: disable=protected-access
    get_replicas = mock.Mock(side_effect=RuntimeError('database unavailable'))
    build_budget = mock.Mock()
    monkeypatch.setattr(controller.serve_state, 'get_replica_infos',
                        get_replicas)
    monkeypatch.setattr(controller.paid_capacity, 'build_launch_budget',
                        build_budget)

    client = _register_update_test_routes(ctrl, monkeypatch)
    response = client.get(constants.CONTROLLER_PLACEMENT_ENDPOINT_PATH,
                          params={
                              'limit': 100,
                              'include_paid_admission': True,
                          })

    assert response.status_code == 200
    assert response.json() == _resident_placement_page()
    get_replicas.assert_called_once_with('svc')
    build_budget.assert_not_called()
    placer.placement_snapshot.assert_called_once_with(
        limit=100, offset=0, paid_admission_by_location=None)


def test_placement_route_opt_in_includes_paid_admission(monkeypatch):
    ctrl = _make_update_controller()
    admission = {'location-a': {'state': 'allowed'}}

    def _placement_snapshot(*, limit, offset, paid_admission_by_location):
        assert limit == 25
        assert offset == 50
        return {
            **_resident_placement_page(),
            'page_offset': offset,
            'locations': [{
                'cloud': 'Kubernetes',
                'region': 'research',
                'paid_admission': paid_admission_by_location['location-a'],
            }],
        }

    placer = mock.Mock()
    placer.placement_snapshot.side_effect = _placement_snapshot
    ctrl._replica_manager.spot_placer = placer  # pylint: disable=protected-access
    ctrl._replica_manager.workspace = 'workspace-a'  # pylint: disable=protected-access
    replicas = [mock.sentinel.replica]
    budget = mock.sentinel.budget
    get_replicas = mock.Mock(return_value=replicas)
    build_budget = mock.Mock(return_value=budget)
    admission_snapshot = mock.Mock(return_value=admission)
    monkeypatch.setattr(controller.serve_state, 'get_replica_infos',
                        get_replicas)
    monkeypatch.setattr(controller.paid_capacity, 'build_launch_budget',
                        build_budget)
    monkeypatch.setattr(controller.paid_capacity,
                        'admission_snapshot_by_location', admission_snapshot)

    client = _register_update_test_routes(ctrl, monkeypatch)
    response = client.get(constants.CONTROLLER_PLACEMENT_ENDPOINT_PATH,
                          params={
                              'limit': 25,
                              'offset': 50,
                              'include_paid_admission': True,
                          })

    assert response.status_code == 200
    assert response.json()['locations'][0]['paid_admission'] == {
        'state': 'allowed'
    }
    get_replicas.assert_called_once_with('svc')
    build_budget.assert_called_once_with(placer,
                                         workspace='workspace-a',
                                         existing_replica_infos=replicas,
                                         globally_managed=True,
                                         service_name='svc',
                                         service_hash='incarnation-a')
    admission_snapshot.assert_called_once_with(budget)


def test_binding_promotion_refreshes_controller_and_manager_in_transition_epoch(
        monkeypatch):
    ctrl = _make_update_controller()
    previous = _binding_authority('legacy', 5)
    refreshed = _binding_authority('bound', 6)
    ctrl._ordinary_launch_binding_authority = previous  # pylint: disable=protected-access
    installed = []

    @contextlib.contextmanager
    def _transition():
        assert ctrl.get_actuation_generation() == 1
        assert not ctrl._actuation_epoch_lock._is_owned()  # pylint: disable=protected-access
        yield installed.append

    ctrl._replica_manager.ordinary_launch_binding_transition.side_effect = (  # pylint: disable=protected-access
        _transition)
    promote = mock.Mock(return_value=6)
    monkeypatch.setattr(controller.request_postgres,
                        'promote_ordinary_launch_binding_service', promote)

    @contextlib.contextmanager
    def _refresh(authority):
        assert authority is previous
        yield refreshed

    monkeypatch.setattr(controller.ordinary_launch_binding,
                        'refresh_controller_authority', _refresh)
    client = _register_update_test_routes(ctrl, monkeypatch)
    response = client.post(
        constants.CONTROLLER_ORDINARY_LAUNCH_BINDING_ENDPOINT_PATH,
        json={
            'mode': 'bound',
            'expected_service_hash': 'incarnation-a',
            'expected_binding_epoch': 5,
        })

    assert response.status_code == 200
    assert response.json() == {
        'binding_mode': 'bound',
        'binding_epoch': 6,
    }
    promote.assert_called_once_with(previous)
    assert installed == [refreshed]
    assert ctrl._ordinary_launch_binding_authority is refreshed  # pylint: disable=protected-access
    assert ctrl.get_actuation_generation() == 2


@pytest.mark.parametrize('source,target,promote_name', [
    ('bound', 'generic', 'promote_non_pool_launch_binding_service'),
    ('generic', 'bound', 'demote_non_pool_launch_binding_service'),
])
def test_generic_binding_transition_refreshes_exact_capability_tuple(
        monkeypatch, source, target, promote_name):
    ctrl = _make_update_controller()
    previous = _binding_authority('bound', 5, generic=source == 'generic')
    refreshed = _binding_authority('bound', 6, generic=target == 'generic')
    ctrl._ordinary_launch_binding_authority = previous  # pylint: disable=protected-access
    installed = []

    @contextlib.contextmanager
    def _transition():
        yield installed.append

    ctrl._replica_manager.ordinary_launch_binding_transition.side_effect = (  # pylint: disable=protected-access
        _transition)
    transition = mock.Mock(return_value=6)
    monkeypatch.setattr(controller.request_postgres, promote_name, transition)

    @contextlib.contextmanager
    def _refresh(authority):
        assert authority is previous
        yield refreshed

    monkeypatch.setattr(controller.ordinary_launch_binding,
                        'refresh_controller_authority', _refresh)
    client = _register_update_test_routes(ctrl, monkeypatch)
    response = client.post(
        constants.CONTROLLER_ORDINARY_LAUNCH_BINDING_ENDPOINT_PATH,
        json={
            'mode': target,
            'expected_service_hash': 'incarnation-a',
            'expected_binding_epoch': 5,
        })

    assert response.status_code == 200
    assert response.json() == {
        'binding_mode': target,
        'binding_epoch': 6,
    }
    transition.assert_called_once_with(previous)
    assert installed == [refreshed]
    assert ctrl._ordinary_launch_binding_authority is refreshed  # pylint: disable=protected-access
    assert refreshed.generic_launches_required is (target == 'generic')


def test_binding_transition_lost_response_retry_uses_source_epoch(monkeypatch):
    ctrl = _make_update_controller()
    installed = _binding_authority('bound', 6)
    ctrl._ordinary_launch_binding_authority = installed  # pylint: disable=protected-access
    promote = mock.Mock()
    monkeypatch.setattr(controller.request_postgres,
                        'promote_ordinary_launch_binding_service', promote)
    client = _register_update_test_routes(ctrl, monkeypatch)

    response = client.post(
        constants.CONTROLLER_ORDINARY_LAUNCH_BINDING_ENDPOINT_PATH,
        json={
            'mode': 'bound',
            'expected_service_hash': 'incarnation-a',
            'expected_binding_epoch': 5,
        })

    assert response.status_code == 200
    assert response.json() == {
        'binding_mode': 'bound',
        'binding_epoch': 6,
    }
    promote.assert_not_called()
    ctrl._replica_manager.ordinary_launch_binding_transition.assert_not_called(  # pylint: disable=protected-access
    )


def test_binding_transition_rejects_nonadjacent_epoch_retry(monkeypatch):
    ctrl = _make_update_controller()
    ctrl._ordinary_launch_binding_authority = _binding_authority(  # pylint: disable=protected-access
        'bound', 8)
    client = _register_update_test_routes(ctrl, monkeypatch)

    response = client.post(
        constants.CONTROLLER_ORDINARY_LAUNCH_BINDING_ENDPOINT_PATH,
        json={
            'mode': 'bound',
            'expected_service_hash': 'incarnation-a',
            'expected_binding_epoch': 5,
        })

    assert response.status_code == 409
    assert 'adjacent-epoch retry' in response.json()['message']


def test_binding_promotion_refresh_failure_keeps_local_authority(monkeypatch):
    ctrl = _make_update_controller()
    previous = _binding_authority('legacy', 5)
    ctrl._ordinary_launch_binding_authority = previous  # pylint: disable=protected-access
    installed = []

    @contextlib.contextmanager
    def _transition():
        yield installed.append

    ctrl._replica_manager.ordinary_launch_binding_transition.side_effect = (  # pylint: disable=protected-access
        _transition)
    monkeypatch.setattr(controller.request_postgres,
                        'promote_ordinary_launch_binding_service',
                        lambda _authority: 6)

    def _fail_refresh(_authority):
        raise controller.ordinary_launch_binding.OrdinaryLaunchBindingConflict(
            'owner changed')

    monkeypatch.setattr(controller.ordinary_launch_binding,
                        'refresh_controller_authority', _fail_refresh)
    client = _register_update_test_routes(ctrl, monkeypatch)
    response = client.post(
        constants.CONTROLLER_ORDINARY_LAUNCH_BINDING_ENDPOINT_PATH,
        json={
            'mode': 'bound',
            'expected_service_hash': 'incarnation-a',
            'expected_binding_epoch': 5,
        })

    assert response.status_code == 409
    assert not installed
    assert ctrl._ordinary_launch_binding_authority is previous  # pylint: disable=protected-access


def test_atomic_capacity_promotion_holds_one_fence_until_manager_install(
        monkeypatch):
    ctrl = _make_update_controller()
    authority = _binding_authority('bound', 6, generic=True)
    ctrl._ordinary_launch_binding_authority = authority  # pylint: disable=protected-access

    epochs = types.SimpleNamespace(demand_source_epoch=1,
                                   zero_cost_actuation_epoch=1)

    def _promote(installed_authority, demand_epoch, actuation_epoch):
        assert installed_authority is authority
        assert (demand_epoch, actuation_epoch) == (0, 0)
        assert ctrl.get_actuation_generation() == 1
        assert ctrl._routing_state_lock._is_owned()  # pylint: disable=protected-access
        return epochs

    def _install():
        assert ctrl.get_actuation_generation() == 1
        assert ctrl._routing_state_lock._is_owned()  # pylint: disable=protected-access

    promote = mock.Mock(side_effect=_promote)
    ctrl._replica_manager.install_durable_zero_cost_actuation.side_effect = (  # pylint: disable=protected-access
        _install)
    monkeypatch.setattr(controller.request_postgres,
                        'promote_capacity_authorities_service', promote)
    client = _register_update_test_routes(ctrl, monkeypatch)

    response = client.post(
        constants.CONTROLLER_CAPACITY_AUTHORITY_ENDPOINT_PATH,
        json={
            'expected_service_hash': 'incarnation-a',
            'expected_demand_source_epoch': 0,
            'expected_zero_cost_actuation_epoch': 0,
        })

    assert response.status_code == 200
    assert response.json() == {
        'demand_source_mode': 'durable',
        'demand_source_epoch': 1,
        'reserved_fill_actuation_mode': 'DURABLE_INTENT',
        'reserved_fill_actuation_epoch': 1,
    }
    promote.assert_called_once_with(authority, 0, 0)
    ctrl._replica_manager.install_durable_zero_cost_actuation.assert_called_once_with(  # pylint: disable=line-too-long,protected-access
    )
    assert ctrl.get_actuation_generation() == 2


def test_separate_demand_transition_is_deprecated(monkeypatch):
    ctrl = _make_update_controller()
    transition = mock.Mock()
    ctrl._transition_demand_source = transition  # type: ignore[method-assign]  # pylint: disable=protected-access
    client = _register_update_test_routes(ctrl, monkeypatch)

    response = client.post(constants.CONTROLLER_DEMAND_SOURCE_ENDPOINT_PATH,
                           json={
                               'mode': 'durable',
                               'expected_service_hash': 'incarnation-a',
                               'expected_source_epoch': 0,
                           })

    assert response.status_code == 409
    assert 'atomic capacity-authority endpoint' in response.json()['message']
    transition.assert_not_called()


def test_atomic_capacity_install_failure_fences_child_for_recovery(monkeypatch):
    ctrl = _make_update_controller()
    authority = _binding_authority('bound', 6, generic=True)
    ctrl._ordinary_launch_binding_authority = authority  # pylint: disable=protected-access
    epochs = types.SimpleNamespace(demand_source_epoch=1,
                                   zero_cost_actuation_epoch=1)
    monkeypatch.setattr(controller.request_postgres,
                        'promote_capacity_authorities_service',
                        lambda *_args: epochs)
    ctrl._replica_manager.install_durable_zero_cost_actuation.side_effect = (  # pylint: disable=protected-access
        RuntimeError('local install failed'))
    recovery = mock.Mock()
    monkeypatch.setattr(ctrl, '_schedule_supervised_recovery', recovery)
    client = _register_update_test_routes(ctrl, monkeypatch)

    response = client.post(
        constants.CONTROLLER_CAPACITY_AUTHORITY_ENDPOINT_PATH,
        json={
            'expected_service_hash': 'incarnation-a',
            'expected_demand_source_epoch': 0,
            'expected_zero_cost_actuation_epoch': 0,
        })

    assert response.status_code == 409
    assert 'supervised recovery scheduled' in response.json()['message']
    assert ctrl._update_recovery_required  # pylint: disable=protected-access
    assert ctrl._actuation_stop.is_set()  # pylint: disable=protected-access
    assert ctrl.get_actuation_generation() == 1
    ctrl._replica_manager.fence_launches_for_update_recovery.assert_called_once_with(  # pylint: disable=line-too-long,protected-access
    )
    recovery.assert_called_once_with()


@pytest.mark.parametrize('path,body,expected_status', [
    ('/controller/update_service', {
        'version': 2,
        'has_config_snapshot': True,
    }, 400),
    (constants.CONTROLLER_CONFIG_UPDATE_ENDPOINT_PATH, {
        'version': 2,
    }, 400),
    ('/controller/update_service', {
        'version': 2,
    }, 409),
])
def test_atomic_config_route_matrix_rejects_mixed_protocol(
        monkeypatch, path, body, expected_status):
    ctrl = _make_update_controller()
    client = _register_update_test_routes(ctrl, monkeypatch)
    response = client.post(path, json=body)
    assert response.status_code == expected_status


def test_atomic_config_route_reaches_prepare_and_commit(monkeypatch, tmp_path):
    ctrl = _make_update_controller()
    task_yaml = tmp_path / 'task.yaml'
    task_yaml.write_text('service: {}\n')
    monkeypatch.setattr(controller.serve_utils, 'generate_task_yaml_file_name',
                        lambda *unused_args, **unused_kwargs: str(task_yaml))
    prepared = mock.sentinel.prepared_config
    service_spec = mock.sentinel.service_spec
    ctrl._prepare_controller_config_update = mock.Mock(  # pylint: disable=protected-access
        return_value=prepared)
    ctrl._load_service_for_update = mock.Mock(  # pylint: disable=protected-access
        return_value=service_spec)
    ctrl._commit_service_update = mock.Mock(  # pylint: disable=protected-access
        return_value=controller.responses.JSONResponse(
            content={'message': 'Success'}, status_code=200))
    client = _register_update_test_routes(ctrl, monkeypatch)
    response = client.post(constants.CONTROLLER_CONFIG_UPDATE_ENDPOINT_PATH,
                           json={
                               'version': 2,
                               'mode': serve_utils.UpdateMode.ROLLING.value,
                               'has_config_snapshot': True,
                               'lifecycle_epoch': 7,
                               'config_snapshot_digest': 'a' * 64,
                               'config_snapshot_id': 'b' * 64,
                           })
    assert response.status_code == 200
    ctrl._prepare_controller_config_update.assert_called_once_with(  # pylint: disable=protected-access
        2, 'a' * 64, 'b' * 64)
    ctrl._commit_service_update.assert_called_once()


class TestServiceUpdateReconciler:

    def test_config_snapshot_commit_enqueues_without_publishing(self):
        ctrl = _make_update_controller()
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access
        prepared = _make_prepared_controller_config(2)
        order = []
        ctrl._install_controller_config = mock.Mock(  # pylint: disable=protected-access
            side_effect=lambda _prepared: order.append('install'))

        def _commit(*_args, **kwargs):
            assert kwargs['ha_recovery_script'] == 'recovery-v2'
            order.append('commit')
            return serve_state.VersionCommitResult.COMMITTED

        with mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value=None), mock.patch.object(
                                   controller.serve_state,
                                   'add_or_update_version',
                                   side_effect=_commit):
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2,
                _make_update_spec(),
                'service: changed',
                serve_utils.UpdateMode.ROLLING,
                'incarnation-a',
                7,
                prepared_config=prepared)
        assert response.status_code == 200
        assert order == ['commit']
        ctrl._install_controller_config.assert_not_called()  # pylint: disable=protected-access
        ctrl._record_committed_update.assert_called_once_with(  # pylint: disable=protected-access
            2, mock.ANY, serve_utils.UpdateMode.ROLLING, prepared)

    def test_staged_config_cannot_move_service_workspace(self, tmp_path):
        ctrl = _make_update_controller()
        ctrl._resource_scope = 'scope-a'  # pylint: disable=protected-access
        ctrl._replica_manager.workspace = 'research'  # pylint: disable=protected-access
        staged = tmp_path / 'config.staged'
        staged.write_text('active_workspace: other\n'
                          'workspaces:\n'
                          '  research: {}\n'
                          '  other: {}\n')
        live = tmp_path / 'config.yaml'
        live.write_text('active_workspace: research\n'
                        'workspaces: {research: {}}\n')
        digest = hashlib.sha256(staged.read_bytes()).hexdigest()
        recovery_script = ('export SKYPILOT_CONFIG=/tmp/config.yaml\n'
                           '/usr/bin/python \\\n'
                           '  -u -m sky.serve.service \\\n'
                           '  --service-name svc\n')
        with mock.patch.object(
                controller.serve_utils,
                'generate_remote_config_yaml_file_name',
                return_value=str(tmp_path / 'config.yaml')), \
             mock.patch.object(
                 controller.serve_utils,
                 'generate_staged_config_yaml_file_name',
                 return_value=str(staged)), \
             mock.patch.object(controller.serve_state,
                               'get_ha_recovery_script',
                               return_value=recovery_script), \
             pytest.raises(RuntimeError, match='expected.*research'):
            ctrl._prepare_controller_config_update(  # pylint: disable=protected-access
                2, digest, 'c' * 64)
        assert staged.stat().st_mode & 0o777 == 0o600

    def test_legacy_backfill_cannot_move_service_workspace(self, tmp_path):
        ctrl = _make_update_controller()
        ctrl._resource_scope = 'scope-a'  # pylint: disable=protected-access
        ctrl._replica_manager.workspace = 'research'  # pylint: disable=protected-access
        staged = tmp_path / 'config.staged'
        staged.write_text('active_workspace: research\n'
                          'workspaces: {research: {}}\n')
        live = tmp_path / 'config.yaml'
        live.write_text('active_workspace: other\n'
                        'workspaces:\n'
                        '  research: {}\n'
                        '  other: {}\n')
        digest = hashlib.sha256(staged.read_bytes()).hexdigest()
        recovery_script = ('export SKYPILOT_CONFIG=/tmp/config.yaml\n'
                           '/usr/bin/python \\\n'
                           '  -u -m sky.serve.service \\\n'
                           '  --service-name svc\n')
        with mock.patch.object(
                controller.serve_utils,
                'generate_remote_config_yaml_file_name',
                return_value=str(live)), \
             mock.patch.object(
                 controller.serve_utils,
                 'generate_staged_config_yaml_file_name',
                 return_value=str(staged)), \
             mock.patch.object(controller.serve_state,
                               'get_ha_recovery_script',
                               return_value=recovery_script), \
             mock.patch.object(
                 controller.serve_state,
                 'add_or_update_version') as commit_version, \
             mock.patch.object(
                 controller.serve_state,
                 'set_ha_recovery_script') as update_recovery_script, \
             pytest.raises(RuntimeError,
                           match='Legacy controller config snapshot is invalid'):
            ctrl._prepare_controller_config_update(  # pylint: disable=protected-access
                2, digest, 'c' * 64)

        commit_version.assert_not_called()
        update_recovery_script.assert_not_called()
        assert ctrl._committed_version == 1  # pylint: disable=protected-access

    def test_guarded_fresh_update_refuses_predecessor_local_backfill(
            self, tmp_path, monkeypatch):
        ctrl = _make_update_controller()
        ctrl._resource_scope = 'scope-a'  # pylint: disable=protected-access
        ctrl._replica_manager.workspace = 'research'  # pylint: disable=protected-access
        staged = tmp_path / 'config.staged'
        staged.write_text('active_workspace: research\n'
                          'workspaces: {research: {}}\n')
        digest = hashlib.sha256(staged.read_bytes()).hexdigest()
        recovery_script = ('export SKYPILOT_CONFIG=/tmp/config.yaml\n'
                           '/usr/bin/python -u -m sky.serve.service '
                           '--service-name svc\n')
        monkeypatch.setenv('SKYPILOT_API_REQUEST_BACKEND', 'postgres')
        monkeypatch.setenv('SKYPILOT_API_SERVER_ROLE', 'controller')
        monkeypatch.setenv('SKYPILOT_API_SERVER_STORAGE_ENABLED', 'false')

        with mock.patch.object(
                controller.serve_utils,
                'generate_versioned_config_yaml_file_name',
                return_value=str(tmp_path / 'config.yaml')), \
             mock.patch.object(
                 controller.serve_utils,
                 'generate_staged_config_yaml_file_name',
                 return_value=str(staged)), \
             mock.patch.object(controller.serve_state,
                               'get_ha_recovery_script',
                               return_value=recovery_script), \
             mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value=None), \
             mock.patch.object(controller.serve_state,
                               'get_version_controller_config',
                               return_value=None), \
             mock.patch.object(
                 controller.serve_utils,
                 'generate_remote_config_yaml_file_name',
                 side_effect=AssertionError(
                     'predecessor config path derived')) as legacy_path, \
             mock.patch.object(
                 controller.serve_state,
                 'get_recovery_version_spec',
                 side_effect=AssertionError(
                     'legacy recovery row queried')) as legacy_row, \
             pytest.raises(RuntimeError,
                           match='committed PostgreSQL controller config'):
            ctrl._prepare_controller_config_update(  # pylint: disable=protected-access
                2, digest, 'c' * 64)

        legacy_path.assert_not_called()
        legacy_row.assert_not_called()

    def test_delayed_lower_version_skips_legacy_activation(self, tmp_path):
        ctrl = _make_update_controller()
        ctrl._resource_scope = 'scope-a'  # pylint: disable=protected-access
        ctrl._applied_version = 3  # pylint: disable=protected-access
        ctrl._replica_manager.workspace = 'research'  # pylint: disable=protected-access
        staged = tmp_path / 'config.v2.staged'
        staged.write_text('active_workspace: research\n'
                          'workspaces: {research: {}}\n')
        digest = hashlib.sha256(staged.read_bytes()).hexdigest()
        durable = serve_utils.sanitize_ha_recovery_config_bytes(
            staged.read_bytes())
        existing_snapshot = (durable, hashlib.sha256(durable).hexdigest(),
                             'd' * 64)
        recovery_script = ('export SKYPILOT_CONFIG=/tmp/config.yaml.v3\n'
                           '/usr/bin/python \\\n'
                           '  -u -m sky.serve.service \\\n'
                           '  --service-name svc\n')

        with mock.patch.object(
                controller.serve_utils,
                'generate_versioned_config_yaml_file_name',
                return_value=str(tmp_path / 'config.v2')), \
             mock.patch.object(
                 controller.serve_utils,
                 'generate_staged_config_yaml_file_name',
                 return_value=str(staged)), \
             mock.patch.object(controller.serve_state,
                               'get_ha_recovery_script',
                               return_value=recovery_script), \
             mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value=None), \
             mock.patch.object(
                 controller.serve_state,
                 'get_version_controller_config',
                 return_value=existing_snapshot):
            prepared = ctrl._prepare_controller_config_update(  # pylint: disable=protected-access
                2, digest, 'c' * 64)

        assert prepared.legacy_snapshot is None
        service = _make_update_spec()
        with mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value=None), \
             mock.patch.object(controller.serve_state,
                               'get_placement_catalog',
                               return_value=None), \
             mock.patch.object(
                 controller.serve_state,
                 'add_or_update_version',
                 return_value=serve_state.VersionCommitResult.STALE_VERSION
             ) as commit_version:
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2,
                service,
                'service: {}',
                serve_utils.UpdateMode.ROLLING,
                'incarnation-a',
                7,
                prepared_config=prepared)

        assert response.status_code == 409
        assert (
            commit_version.call_args.kwargs['legacy_controller_config_snapshot']
            is None)
        assert (
            commit_version.call_args.kwargs['legacy_controller_applied_version']
            is None)

    def test_staged_config_digest_mismatch_precedes_commit_and_install(
            self, tmp_path):
        ctrl = _make_update_controller()
        ctrl._resource_scope = 'scope-a'  # pylint: disable=protected-access
        staged = tmp_path / 'config.staged'
        staged.write_text('active_workspace: research\n')
        ctrl._install_controller_config = mock.Mock()  # pylint: disable=protected-access
        with mock.patch.object(
                controller.serve_utils,
                'generate_remote_config_yaml_file_name',
                return_value=str(tmp_path / 'config.yaml')), \
             mock.patch.object(
                 controller.serve_utils,
                 'generate_staged_config_yaml_file_name',
                 return_value=str(staged)), \
             mock.patch.object(controller.serve_state,
                               'get_ha_recovery_script',
                               return_value='legacy'), \
             mock.patch.object(
                 controller.serve_state,
                 'add_or_update_version') as commit, \
             pytest.raises(RuntimeError, match='digest does not match'):
            ctrl._prepare_controller_config_update(  # pylint: disable=protected-access
                2, '0' * 64, 'c' * 64)
        commit.assert_not_called()
        ctrl._install_controller_config.assert_not_called()  # pylint: disable=protected-access

    def test_invalid_sanitized_projection_precedes_commit(self, tmp_path):
        ctrl = _make_update_controller()
        ctrl._resource_scope = 'scope-a'  # pylint: disable=protected-access
        ctrl._replica_manager.workspace = 'research'  # pylint: disable=protected-access
        staged = tmp_path / 'config.staged'
        staged.write_text('active_workspace: research\n'
                          'workspaces: {research: {}}\n')
        live = tmp_path / 'config.yaml'
        live.write_text('active_workspace: research\n'
                        'workspaces: {research: {}}\n')
        digest = hashlib.sha256(staged.read_bytes()).hexdigest()
        recovery_script = ('export SKYPILOT_CONFIG=/tmp/config.yaml\n'
                           '/usr/bin/python \\\n'
                           '  -u -m sky.serve.service \\\n'
                           '  --service-name svc\n')
        invalid_projection = (b'active_workspace: research\n'
                              b'workspaces: {}\n')

        with mock.patch.object(
                controller.serve_utils,
                'generate_remote_config_yaml_file_name',
                return_value=str(live)), \
             mock.patch.object(
                 controller.serve_utils,
                 'generate_staged_config_yaml_file_name',
                 return_value=str(staged)), \
             mock.patch.object(controller.serve_state,
                               'get_ha_recovery_script',
                               return_value=recovery_script), \
             mock.patch.object(
                 controller.serve_utils,
                 'sanitize_ha_recovery_config_bytes',
                 return_value=invalid_projection), \
             mock.patch.object(
                 controller.serve_state,
                 'add_or_update_version') as commit_version, \
             pytest.raises(RuntimeError,
                           match='Durable controller config snapshot is invalid'):
            ctrl._prepare_controller_config_update(  # pylint: disable=protected-access
                2, digest, 'c' * 64)

        commit_version.assert_not_called()
        assert ctrl._committed_version == 1  # pylint: disable=protected-access

    def test_committed_retry_requires_raw_receipt_digest_and_preserves_raw(
            self, tmp_path):
        ctrl = _make_update_controller()
        ctrl._resource_scope = 'scope-a'  # pylint: disable=protected-access
        ctrl._replica_manager.workspace = 'research'  # pylint: disable=protected-access
        live = tmp_path / 'config.yaml'
        staged = tmp_path / 'config.staged'
        secret = 'same-pod-raw-config-sentinel'
        raw_bytes = (
            'active_workspace: research\n'
            'workspaces: {research: {}}\n'
            'kubernetes: {allowed_contexts: [east, phx]}\n'
            'docker:\n'
            f'  run_options: ["--env=TOKEN={secret}"]\n').encode('utf-8')
        live.write_bytes(raw_bytes)
        source_digest = hashlib.sha256(raw_bytes).hexdigest()
        wrong_digest = hashlib.sha256(b'a different API request').hexdigest()
        snapshot_id = 'c' * 64
        serve_utils.write_config_snapshot_receipt(str(live), 2, snapshot_id,
                                                  source_digest)
        durable_bytes = serve_utils.sanitize_ha_recovery_config_bytes(raw_bytes)
        durable_digest = hashlib.sha256(durable_bytes).hexdigest()
        recovery_script = ('export SKYPILOT_CONFIG=/tmp/config.yaml\n'
                           '/usr/bin/python \\\n'
                           '  -u -m sky.serve.service \\\n'
                           '  --service-name svc\n')

        with mock.patch.object(
                controller.serve_utils,
                'generate_versioned_config_yaml_file_name',
                return_value=str(live)), \
             mock.patch.object(
                 controller.serve_utils,
                 'generate_staged_config_yaml_file_name',
                 return_value=str(staged)), \
             mock.patch.object(controller.serve_state,
                               'get_ha_recovery_script',
                               return_value=recovery_script), \
             mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value='service: {}'), \
             mock.patch.object(
                 controller.serve_state,
                 'get_version_controller_config',
                 return_value=(durable_bytes, durable_digest, snapshot_id)):
            with pytest.raises(RuntimeError,
                               match='raw controller config receipt'):
                ctrl._prepare_controller_config_update(  # pylint: disable=protected-access
                    2, wrong_digest, snapshot_id)

            prepared = ctrl._prepare_controller_config_update(  # pylint: disable=protected-access
                2, source_digest, snapshot_id)

        assert prepared.source_is_staged is False
        assert prepared.source_is_live is True
        assert prepared.source_digest == source_digest
        assert prepared.durable_bytes == durable_bytes
        assert secret.encode('utf-8') not in prepared.durable_bytes
        assert prepared.config.get_nested(('docker', 'run_options'),
                                          None) == [f'--env=TOKEN={secret}']

    @pytest.mark.parametrize('receipt_state', ['missing', 'mismatched'])
    def test_committed_retry_without_exact_receipt_ignores_raw_and_never_mints(
            self, tmp_path, receipt_state):
        ctrl = _make_update_controller()
        ctrl._resource_scope = 'scope-a'  # pylint: disable=protected-access
        ctrl._replica_manager.workspace = 'research'  # pylint: disable=protected-access
        live = tmp_path / 'config.yaml.v2'
        staged = tmp_path / 'config.yaml.v2.staged'
        secret = 'unauthorized-retry-raw-secret'
        raw_bytes = (
            'active_workspace: research\n'
            'workspaces: {research: {}}\n'
            'kubernetes: {allowed_contexts: [east, phx]}\n'
            'docker:\n'
            f'  run_options: ["--env=TOKEN={secret}"]\n').encode('utf-8')
        staged.write_bytes(raw_bytes)
        source_digest = hashlib.sha256(raw_bytes).hexdigest()
        snapshot_id = 'c' * 64
        if receipt_state == 'mismatched':
            serve_utils.write_config_snapshot_receipt(str(staged), 2, 'f' * 64,
                                                      'e' * 64)
        durable_bytes = serve_utils.sanitize_ha_recovery_config_bytes(raw_bytes)
        durable_digest = hashlib.sha256(durable_bytes).hexdigest()
        recovery_script = ('export SKYPILOT_CONFIG=/tmp/config.yaml.v2\n'
                           '/usr/bin/python \\\n'
                           '  -u -m sky.serve.service \\\n'
                           '  --service-name svc\n')

        with mock.patch.object(
                controller.serve_utils,
                'generate_versioned_config_yaml_file_name',
                return_value=str(live)), \
             mock.patch.object(
                 controller.serve_utils,
                 'generate_staged_config_yaml_file_name',
                 return_value=str(staged)), \
             mock.patch.object(controller.serve_state,
                               'get_ha_recovery_script',
                               return_value=recovery_script), \
             mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value='service: {}'), \
             mock.patch.object(
                 controller.serve_state,
                 'get_version_controller_config',
                 return_value=(durable_bytes, durable_digest, snapshot_id)), \
             mock.patch.object(
                 controller.serve_utils,
                 'write_config_snapshot_receipt') as write_receipt:
            prepared = ctrl._prepare_controller_config_update(  # pylint: disable=protected-access
                2, source_digest, snapshot_id)

        assert prepared.source_is_staged is False
        assert prepared.source_is_live is False
        assert prepared.durable_bytes == durable_bytes
        assert secret.encode() not in prepared.durable_bytes
        assert prepared.config.get_nested(('docker', 'run_options'),
                                          None) is None
        write_receipt.assert_not_called()
        receipt = serve_utils.get_config_snapshot_receipt(str(staged))
        if receipt_state == 'missing':
            assert receipt is None
        else:
            assert receipt == {
                'version': 2,
                'snapshot_id': 'f' * 64,
                'source_digest': 'e' * 64,
            }

    def test_staged_config_secrets_are_not_logged(self, tmp_path, caplog):
        ctrl = _make_update_controller()
        ctrl._resource_scope = 'scope-a'  # pylint: disable=protected-access
        ctrl._replica_manager.workspace = 'research'  # pylint: disable=protected-access
        staged = tmp_path / 'config.staged'
        secret = 'controller-log-secret-sentinel'
        staged.write_text('active_workspace: research\n'
                          'workspaces: {research: {}}\n'
                          'docker:\n'
                          f'  run_options: ["--env=TOKEN={secret}"]\n')
        live = tmp_path / 'config.yaml'
        live.write_text('active_workspace: research\n'
                        'workspaces: {research: {}}\n')
        digest = hashlib.sha256(staged.read_bytes()).hexdigest()
        legacy = ('/usr/bin/python \\\n'
                  '  -u -m sky.serve.service \\\n'
                  '  --service-name svc\n')
        with mock.patch.object(
                controller.serve_utils,
                'generate_remote_config_yaml_file_name',
                return_value=str(tmp_path / 'config.yaml')), \
             mock.patch.object(
                 controller.serve_utils,
                 'generate_staged_config_yaml_file_name',
                 return_value=str(staged)), \
             mock.patch.object(controller.serve_state,
                               'get_ha_recovery_script',
                               return_value=legacy), \
             caplog.at_level('DEBUG'):
            ctrl._prepare_controller_config_update(  # pylint: disable=protected-access
                2, digest, 'c' * 64)
        assert secret not in caplog.text

    def test_config_transition_failure_schedules_supervised_recovery(self):
        ctrl = _make_update_controller()
        prepared = _make_prepared_controller_config(2)
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='physical_backend')
        ctrl._run_with_prepared_config = mock.Mock(  # pylint: disable=protected-access
            side_effect=lambda _prepared, callback: callback())
        ctrl._install_controller_config = mock.Mock(  # pylint: disable=protected-access
            side_effect=OSError('install failed'))

        def _transition(*_args, install_config, **_kwargs):
            install_config()

        ctrl._replica_manager.update_version.side_effect = _transition  # pylint: disable=protected-access
        with mock.patch.object(
                controller.replica_managers,
                'validate_service_update_preflight',
                return_value=mock.sentinel.placer), \
             mock.patch.object(
                 ctrl,
                 '_schedule_supervised_recovery') as schedule_recovery, \
             pytest.raises(controller.ServiceUpdateRequiresRecoveryError,
                           match='install failed'):
            ctrl._apply_service_update(  # pylint: disable=protected-access
                2, types.SimpleNamespace(uses_logical_replicas=False),
                serve_utils.UpdateMode.ROLLING, prepared)

        ctrl._replica_manager.update_version.assert_called_once()  # pylint: disable=protected-access
        assert callable(ctrl._replica_manager.update_version.call_args.kwargs[  # pylint: disable=protected-access
            'install_config'])
        ctrl._install_controller_config.assert_called_once_with(prepared)  # pylint: disable=protected-access
        ctrl._replica_manager.fence_launches_for_update_recovery.assert_called_once_with()  # pylint: disable=line-too-long,protected-access
        assert ctrl._update_reconciler_stop.is_set()  # pylint: disable=protected-access
        assert ctrl._get_actuation_stop().is_set()  # pylint: disable=protected-access
        schedule_recovery.assert_called_once_with()
        ctrl._replica_manager.clear_pending_version.assert_not_called()  # pylint: disable=protected-access

    def test_partial_runtime_transition_stops_all_autoscaler_actuation(self):
        ctrl = _make_update_controller()
        prepared = _make_prepared_controller_config(2)
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='physical_backend')
        ctrl._run_with_prepared_config = mock.Mock(  # pylint: disable=protected-access
            side_effect=lambda _prepared, callback: callback())
        ctrl._install_controller_config = mock.Mock()  # pylint: disable=protected-access

        def _manager_transition(*_args, install_config, **_kwargs):
            install_config()
            ctrl._replica_manager.latest_version = 2  # pylint: disable=protected-access

        ctrl._replica_manager.update_version.side_effect = _manager_transition  # pylint: disable=protected-access
        with mock.patch.object(
                controller.replica_managers,
                'validate_service_update_preflight',
                return_value=mock.sentinel.placer), \
             mock.patch.object(
                 controller.autoscalers.Autoscaler,
                 'from_spec',
                 side_effect=RuntimeError('autoscaler rebuild failed')), \
             mock.patch.object(
                 ctrl,
                 '_schedule_supervised_recovery') as schedule_recovery, \
             pytest.raises(controller.ServiceUpdateRequiresRecoveryError,
                           match='autoscaler rebuild failed'):
            ctrl._apply_service_update(  # pylint: disable=protected-access
                2, types.SimpleNamespace(uses_logical_replicas=False),
                serve_utils.UpdateMode.ROLLING, prepared)

        assert ctrl._get_actuation_stop().is_set()  # pylint: disable=protected-access
        schedule_recovery.assert_called_once_with()
        ctrl._run_autoscaler()  # pylint: disable=protected-access
        ctrl._replica_manager.clear_scale_reconciliation_signal.assert_not_called()  # pylint: disable=line-too-long,protected-access
        ctrl._replica_manager.publish_target_num_replicas.assert_not_called()  # pylint: disable=line-too-long,protected-access
        ctrl._replica_manager.scale_up_batch.assert_not_called()  # pylint: disable=protected-access
        ctrl._replica_manager.scale_up_to_logical_capacity.assert_not_called()  # pylint: disable=line-too-long,protected-access
        ctrl._replica_manager.scale_down.assert_not_called()  # pylint: disable=protected-access
        ctrl._replica_manager.scale_down_logically_batch.assert_not_called()  # pylint: disable=line-too-long,protected-access

    def test_install_atomically_replaces_config_and_removes_old_keys(
            self, tmp_path, monkeypatch):
        live = tmp_path / 'config.yaml'
        staged = tmp_path / 'config.yaml.v2.staged'
        old_bytes = (b'active_workspace: old\nworkspaces: {old: {}}\n'
                     b'kubernetes: {allowed_contexts: [old]}\n')
        new_bytes = (b'active_workspace: research\n'
                     b'workspaces: {research: {}}\n'
                     b'kubernetes: {allowed_contexts: [east, phx]}\n')
        live.write_bytes(old_bytes)
        staged.write_bytes(new_bytes)
        snapshot_id = 'c' * 64
        source_digest = hashlib.sha256(new_bytes).hexdigest()
        serve_utils.write_config_snapshot_receipt(str(staged), 2, snapshot_id,
                                                  source_digest)
        durable_bytes = serve_utils.sanitize_ha_recovery_config_bytes(new_bytes)
        durable_digest = hashlib.sha256(durable_bytes).hexdigest()
        recovery_script = ('export SKYPILOT_CONFIG=/tmp/config.yaml\n'
                           '/usr/bin/python \\\n'
                           '  -u -m sky.serve.service \\\n'
                           '  --service-name svc\n')
        new_config = skypilot_config.parse_and_validate_config_bytes(
            new_bytes, 'test config', log_config=False)
        old_config = skypilot_config.parse_and_validate_config_bytes(
            old_bytes, 'old test config', log_config=False)
        monkeypatch.setattr(
            skypilot_config,
            '_global_config_context',  # pylint: disable=protected-access
            skypilot_config.ConfigContext(config=old_config))
        monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, str(live))
        monkeypatch.setattr(
            skypilot_config, 'reload_config',
            mock.Mock(side_effect=AssertionError(
                'reload must not expose an empty config')))
        prepared = controller._PreparedControllerConfig(  # pylint: disable=protected-access
            config=new_config,
            service_name='svc',
            live_path=str(live),
            staged_path=str(staged),
            recovery_script=recovery_script,
            version=2,
            snapshot_id=snapshot_id,
            source_digest=source_digest,
            durable_bytes=durable_bytes,
            durable_digest=durable_digest,
            source_is_staged=True,
            source_is_live=False,
            legacy_snapshot=None)

        controller.SkyServeController._install_controller_config(  # pylint: disable=protected-access
            prepared)

        assert live.read_bytes() == new_bytes
        assert skypilot_config.get_active_workspace() == 'research'
        assert skypilot_config.get_nested(('kubernetes', 'allowed_contexts'),
                                          None) == ['east', 'phx']
        assert 'old' not in skypilot_config.to_dict().get('workspaces', {})

    @pytest.mark.parametrize('explicit', [True, False, None])
    def test_ha_update_round_trip_never_migrates_from_yaml(self, explicit):
        """A YAML value cannot move a service off its durable LB mode.

        high_availability is ignored on parse, so every update inherits the
        durable mode. Migration is only the explicit admin path, covered by
        test_lb_only_admin_change_reuses_current_committed_spec.
        """
        service_config = {}
        if explicit is not None:
            service_config['load_balancer'] = {
                'high_availability': explicit,
            }
        original = controller.serve.SkyServiceSpec.from_yaml_config(
            service_config)
        yaml_content = yaml_utils.dump_yaml_str(
            {'service': original.to_yaml_config()})
        round_tripped = controller.serve.SkyServiceSpec.from_yaml_str(
            yaml_content)
        ctrl = _make_update_controller()
        ctrl._lb_ha_enabled = False  # pylint: disable=protected-access
        ctrl._transition_load_balancer_mode = mock.Mock()  # pylint: disable=protected-access
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value=None), mock.patch.object(
                                   controller.serve_state,
                                   'add_or_update_version',
                                   return_value=serve_state.VersionCommitResult.
                                   COMMITTED) as commit:
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, round_tripped, yaml_content, serve_utils.UpdateMode.ROLLING,
                'incarnation-a', 7)

        assert response.status_code == 200
        ctrl._transition_load_balancer_mode.assert_not_called()  # pylint: disable=protected-access
        assert not commit.call_args.args[2].lb_high_availability

    def test_lb_only_admin_change_reuses_current_committed_spec(self):
        ctrl = _make_update_controller()
        current_spec = mock.sentinel.current_spec
        ctrl._transition_load_balancer_mode = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=current_spec):
            ctrl._set_load_balancer_high_availability(  # pylint: disable=protected-access
                True, 'incarnation-a', 7)

        ctrl._transition_load_balancer_mode.assert_called_once_with(  # pylint: disable=protected-access
            True,
            current_spec,
            expected_service_hash='incarnation-a',
            expected_lifecycle_epoch=7)

    def test_legacy_to_ha_transition_keeps_dedicated_role_executor(self):
        ctrl = _make_update_controller()
        ctrl._lb_ha_enabled = False  # pylint: disable=protected-access
        ctrl._owns_current_service = mock.Mock(  # pylint: disable=protected-access
            return_value=True)
        target_spec = types.SimpleNamespace(lb_stream_timeout_seconds=30,
                                            graceful_drain_seconds=60)
        disabled = controller.lb_ha.LbCutoverState(
            enabled=False,
            active_slot=None,
            generation=0,
            pending_slot=None,
            phase=controller.lb_ha.LbCutoverPhase.STABLE,
            lifecycle_epoch=7)
        stable = controller.lb_ha.LbCutoverState(
            enabled=True,
            active_slot=controller.lb_ha.LbSlot.A,
            generation=1,
            pending_slot=None,
            phase=controller.lb_ha.LbCutoverPhase.STABLE,
            lifecycle_epoch=7)
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'lifecycle_epoch': 7,
        }
        role_executor = ctrl._lb_role_executor  # pylint: disable=protected-access
        snapshot_read_executor = (  # pylint: disable=protected-access
            ctrl._lb_role_snapshot_read_executor)
        try:
            with mock.patch.object(controller.serve_state,
                                   'get_lb_cutover_state',
                                   side_effect=[disabled, stable]), \
                 mock.patch.object(controller.serve_state,
                                   'get_service_controller_owner',
                                   return_value=owner), \
                 mock.patch.object(controller.serve_state,
                                   'begin_lb_ha_migration',
                                   return_value=True), \
                 mock.patch.object(controller.lb_k8s,
                                   'require_lb_ha_runtime'), \
                 mock.patch.object(
                     controller.lb_k8s,
                     'lb_termination_grace_period_seconds',
                     return_value=90), \
                 mock.patch.object(controller.lb_k8s,
                                   'prepare_lb_mode_transition'):
                ctrl._transition_load_balancer_mode(  # pylint: disable=protected-access
                    True,
                    target_spec,
                    expected_service_hash='incarnation-a',
                    expected_lifecycle_epoch=7)

            assert ctrl._lb_ha_enabled is True  # pylint: disable=protected-access
            assert ctrl._lb_role_executor is role_executor  # pylint: disable=protected-access
            assert (  # pylint: disable=protected-access
                ctrl._lb_role_snapshot_read_executor is snapshot_read_executor)

            release_default = threading.Event()
            default_started = threading.Event()

            def block_default_executor():
                default_started.set()
                assert release_default.wait(timeout=5)

            def snapshot_thread_name(*_args):
                return threading.current_thread().name

            async def read_snapshot():
                loop = asyncio.get_running_loop()
                default_executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix='blocked-default')
                loop.set_default_executor(default_executor)
                blocker = loop.run_in_executor(None, block_default_executor)
                while not default_started.is_set():
                    await asyncio.sleep(0)
                try:
                    result = await asyncio.wait_for(
                        ctrl._get_shared_stable_lb_role_snapshot(  # pylint: disable=protected-access
                            loop, ('incarnation-a', (123, '10.0.0.1'), 7),
                            stable, owner),
                        timeout=1)
                    assert not blocker.done()
                    return result
                finally:
                    release_default.set()
                    await blocker
                    default_executor.shutdown(wait=True, cancel_futures=True)

            with mock.patch.object(controller.lb_k8s,
                                   'get_lb_role_snapshot',
                                   side_effect=snapshot_thread_name):
                snapshot = asyncio.run(read_snapshot())

            assert snapshot.error is None
            assert snapshot.snapshot.startswith('test-skyserve-ha-role')
        finally:
            role_executor.shutdown(wait=True, cancel_futures=True)
            snapshot_read_executor.shutdown(wait=True, cancel_futures=True)

    def test_lb_mode_retry_resumes_interrupted_migration(self):
        ctrl = _make_update_controller()
        ctrl._lb_ha_enabled = True  # pylint: disable=protected-access
        ctrl._lb_session_ledger = mock.Mock()  # pylint: disable=protected-access
        ctrl._lb_occupancy_contract_known = True  # pylint: disable=protected-access
        ctrl._lb_last_demand_snapshot = None  # pylint: disable=protected-access
        ctrl._resource_scope = None  # pylint: disable=protected-access
        ctrl._owns_current_service = mock.Mock(  # pylint: disable=protected-access
            return_value=True)
        target_spec = types.SimpleNamespace(lb_stream_timeout_seconds=30,
                                            graceful_drain_seconds=60)
        migrating = controller.lb_ha.LbCutoverState(
            enabled=True,
            active_slot=controller.lb_ha.LbSlot.A,
            generation=1,
            pending_slot=None,
            phase=controller.lb_ha.LbCutoverPhase.MIGRATING,
            lifecycle_epoch=7)
        stable = controller.lb_ha.LbCutoverState(
            enabled=True,
            active_slot=controller.lb_ha.LbSlot.A,
            generation=1,
            pending_slot=None,
            phase=controller.lb_ha.LbCutoverPhase.STABLE,
            lifecycle_epoch=7)
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'lifecycle_epoch': 7,
        }

        with mock.patch.object(controller.serve_state,
                               'get_lb_cutover_state',
                               side_effect=[migrating, stable]), \
             mock.patch.object(controller.serve_state,
                               'get_service_controller_owner',
                               return_value=owner), \
             mock.patch.object(controller.serve_state,
                               'begin_lb_ha_migration') as begin, \
             mock.patch.object(controller.lb_k8s,
                               'require_lb_ha_runtime'), \
             mock.patch.object(controller.lb_k8s,
                               'lb_termination_grace_period_seconds',
                               return_value=90), \
             mock.patch.object(controller.lb_k8s,
                               'prepare_lb_mode_transition') as prepare:
            ctrl._transition_load_balancer_mode(  # pylint: disable=protected-access
                True,
                target_spec,
                expected_service_hash='incarnation-a',
                expected_lifecycle_epoch=7)

        begin.assert_not_called()
        prepare.assert_called_once()

    def test_lb_mode_change_rejects_stale_lifecycle_before_mutation(self):
        ctrl = _make_update_controller()
        ctrl._lb_ha_enabled = False  # pylint: disable=protected-access
        state = controller.lb_ha.LbCutoverState(
            enabled=False,
            active_slot=None,
            generation=0,
            pending_slot=None,
            phase=controller.lb_ha.LbCutoverPhase.STABLE,
            lifecycle_epoch=8)
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'lifecycle_epoch': 8,
        }

        with mock.patch.object(controller.serve_state,
                               'get_lb_cutover_state',
                               return_value=state), \
             mock.patch.object(controller.serve_state,
                               'get_service_controller_owner',
                               return_value=owner), \
             mock.patch.object(controller.serve_state,
                               'begin_lb_ha_migration') as begin, \
             pytest.raises(RuntimeError, match='lifecycle changed'):
            ctrl._transition_load_balancer_mode(  # pylint: disable=protected-access
                True,
                mock.Mock(),
                expected_service_hash='incarnation-a',
                expected_lifecycle_epoch=7)

        begin.assert_not_called()

    def test_lb_mode_retry_resumes_interrupted_rollback(self):
        ctrl = _make_update_controller()
        role_executor = ctrl._lb_role_executor  # pylint: disable=protected-access
        ctrl._lb_ha_enabled = True  # pylint: disable=protected-access
        ctrl._lb_session_ledger = mock.Mock()  # pylint: disable=protected-access
        ctrl._lb_last_demand_snapshot = mock.Mock()  # pylint: disable=protected-access
        ctrl._resource_scope = None  # pylint: disable=protected-access
        ctrl._owns_current_service = mock.Mock(  # pylint: disable=protected-access
            return_value=True)
        target_spec = types.SimpleNamespace(lb_stream_timeout_seconds=30,
                                            graceful_drain_seconds=60)
        rolling_back = controller.lb_ha.LbCutoverState(
            enabled=True,
            active_slot=controller.lb_ha.LbSlot.A,
            generation=2,
            pending_slot=None,
            phase=controller.lb_ha.LbCutoverPhase.ROLLING_BACK,
            lifecycle_epoch=7)
        stable = controller.lb_ha.LbCutoverState(
            enabled=False,
            active_slot=None,
            generation=0,
            pending_slot=None,
            phase=controller.lb_ha.LbCutoverPhase.STABLE,
            lifecycle_epoch=7)
        owner = {
            'hash': 'incarnation-a',
            'controller_pid': 123,
            'controller_ip': '10.0.0.1',
            'lifecycle_epoch': 7,
        }

        with mock.patch.object(controller.serve_state,
                               'get_lb_cutover_state',
                               side_effect=[rolling_back, stable]), \
             mock.patch.object(controller.serve_state,
                               'get_service_controller_owner',
                               return_value=owner), \
             mock.patch.object(controller.serve_state,
                               'begin_lb_ha_rollback') as begin, \
             mock.patch.object(controller.lb_k8s,
                               'lb_termination_grace_period_seconds',
                               return_value=90), \
             mock.patch.object(controller.lb_k8s,
                               'prepare_lb_mode_transition') as prepare:
            ctrl._transition_load_balancer_mode(  # pylint: disable=protected-access
                False,
                target_spec,
                expected_service_hash='incarnation-a',
                expected_lifecycle_epoch=7)

        begin.assert_not_called()
        prepare.assert_called_once()
        assert ctrl._lb_ha_enabled is False  # pylint: disable=protected-access
        assert ctrl._lb_session_ledger is None  # pylint: disable=protected-access
        assert ctrl._lb_role_executor is role_executor  # pylint: disable=protected-access

    def test_per_gpu_service_rejects_update_to_physical_semantics(self):
        ctrl = _make_update_controller()
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='logical')
        legacy_spec = _make_update_spec()

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
        logical_spec = _make_update_spec('logical')

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
        spec = _make_update_spec()
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access
        with mock.patch.object(controller.serve_state,
                               'add_or_update_version',
                               return_value=serve_state.VersionCommitResult.
                               CONTENT_CONFLICT) as commit:
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, spec, 'service: changed', serve_utils.UpdateMode.ROLLING,
                'incarnation-a', 7)

        assert response.status_code == 409
        assert 'already committed with different content' in json.loads(
            response.body)['message']
        ctrl._record_committed_update.assert_not_called()  # pylint: disable=protected-access
        commit.assert_called_once_with('svc',
                                       2,
                                       spec,
                                       'service: changed',
                                       submitted_yaml_content=None,
                                       expected_service_hash='incarnation-a',
                                       expected_lifecycle_epoch=7,
                                       expected_controller_owner=(123,
                                                                  '10.0.0.1'),
                                       controller_job_projection=None,
                                       controller_work_cache=None,
                                       worker_placement_projections=None)

    def test_stale_version_returns_409_without_scheduling(self):
        ctrl = _make_update_controller()
        spec = _make_update_spec()
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access
        with mock.patch.object(
                controller.serve_state,
                'add_or_update_version',
                return_value=serve_state.VersionCommitResult.STALE_VERSION):
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, spec, 'service: stale', serve_utils.UpdateMode.ROLLING,
                'incarnation-a', 7)

        assert response.status_code == 409
        assert 'superseded' in json.loads(response.body)['message']
        ctrl._record_committed_update.assert_not_called()  # pylint: disable=protected-access

    def test_kueue_admission_hold_returns_409_without_scheduling(self):
        ctrl = _make_update_controller()
        spec = _make_update_spec()
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access
        with mock.patch.object(
                controller.serve_state,
                'add_or_update_version',
                return_value=(
                    serve_state.VersionCommitResult.KUEUE_ADMISSION_HOLD)):
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, spec, 'service: held', serve_utils.UpdateMode.ROLLING,
                'incarnation-a', 7)

        assert response.status_code == 409
        assert 'waiting for Kueue admission' in json.loads(
            response.body)['message']
        ctrl._record_committed_update.assert_not_called()  # pylint: disable=protected-access

    def test_durable_activation_fence_rejects_racing_physical_commit(self):
        ctrl = _make_update_controller()
        # Runtime is still physical while a previously committed logical
        # version is waiting for the manager lock. The durable parent-row
        # fence, not the currently published autoscaler, must reject v3.
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='physical_backend')
        physical = _make_update_spec()
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
        logical = _make_update_spec('logical')
        update_task = types.SimpleNamespace(service=logical, num_nodes=2)

        with mock.patch.object(controller.task_lib.Task,
                               'from_yaml_str',
                               return_value=update_task), \
             mock.patch.object(
                 controller.replica_managers,
                 'load_task_with_service_spec',
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

    def test_legacy_per_gpu_retry_acknowledges_authoritative_physical_spec(
            self):
        ctrl = _make_update_controller()
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='physical_backend')
        caller = _make_update_spec('logical')
        persisted = _make_update_spec()
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
                               return_value=persisted) as get_spec, \
             mock.patch.object(
                 controller.serve_state,
                 'get_placement_projection_record',
                 return_value=(True, None, None, None)):
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                1, caller, 'service: same-legacy-yaml',
                serve_utils.UpdateMode.BLUE_GREEN, 'incarnation-a', 7)

        assert response.status_code == 200
        get_spec.assert_called_once_with('svc', 1)
        ctrl._record_committed_update.assert_not_called()  # pylint: disable=protected-access

    def test_exact_legacy_yaml_is_loaded_before_current_parser(self):
        ctrl = _make_update_controller()
        persisted = _make_update_spec()
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
        persisted = _make_legacy_physical_per_gpu_spec()
        placement_catalog = {
            'schema_version': 1,
            'entries': [{
                'cloud': 'kubernetes',
                'region': 'legacy-context',
            }],
        }

        with mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value='service: physical-v2'), \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=persisted), \
             mock.patch.object(controller.serve_state,
                               'get_placement_catalog',
                               return_value=placement_catalog), \
             mock.patch.object(
                 controller.serve_state,
                 'get_placement_projection_record',
                 return_value=(True, None, None, None)), \
             mock.patch.object(controller.task_lib.Task,
                               'from_yaml_str') as parse_task, \
             mock.patch.object(
                 controller.serve_state,
                 'add_or_update_version',
                 return_value=serve_state.VersionCommitResult.
                 IDEMPOTENT_RETRY) as commit:
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, persisted, 'service: physical-v2',
                serve_utils.UpdateMode.ROLLING, 'incarnation-a', 7)

        assert response.status_code == 200
        assert persisted.placement_contract.is_legacy_physical_per_gpu
        parse_task.assert_not_called()
        assert commit.call_args.kwargs['placement_catalog'] is placement_catalog
        assert ctrl._pending_update is None  # pylint: disable=protected-access
        assert ctrl._applied_version == 3  # pylint: disable=protected-access
        ctrl._replica_manager.notify_version_pending.assert_not_called()  # pylint: disable=protected-access

    def test_exact_logical_retry_skips_current_topology_parser(self):
        ctrl = _make_update_controller()
        ctrl._autoscaler = types.SimpleNamespace(  # pylint: disable=protected-access
            replica_unit='logical')
        persisted = _make_update_spec('logical')
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value='old-logical-yaml'), \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=persisted), \
             mock.patch.object(
                 controller.serve_state,
                 'get_placement_catalog',
                 return_value={
                     'schema_version': 1,
                     'entries': [],
                 }), \
             mock.patch.object(
                 controller.serve_state,
                 'get_placement_projection_record',
                 return_value=(True, None, None, None)), \
             mock.patch.object(controller.task_lib.Task,
                               'from_yaml_str') as parse_task, \
             mock.patch.object(
                 controller.serve_state,
                 'add_or_update_version',
                 return_value=serve_state.VersionCommitResult.
                 IDEMPOTENT_RETRY):
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2, persisted, 'old-logical-yaml',
                serve_utils.UpdateMode.ROLLING, 'incarnation-a', 7)

        assert response.status_code == 200
        parse_task.assert_not_called()
        ctrl._record_committed_update.assert_not_called()  # pylint: disable=protected-access

    @pytest.mark.parametrize('controller_state',
                             ['newer_applied', 'newer_pending', 'quarantined'])
    def test_delayed_config_retry_is_ack_only_in_every_runtime_state(
            self, tmp_path, controller_state):
        ctrl = _make_update_controller()
        pending_before = None
        if controller_state == 'newer_applied':
            ctrl._committed_version = 3  # pylint: disable=protected-access
            ctrl._applied_version = 3  # pylint: disable=protected-access
        elif controller_state == 'newer_pending':
            ctrl._committed_version = 3  # pylint: disable=protected-access
            prepared_v3 = _make_prepared_controller_config(
                3, source_is_staged=False)
            pending_before = controller._PendingServiceUpdate(  # pylint: disable=protected-access
                3, mock.sentinel.spec_v3, serve_utils.UpdateMode.ROLLING,
                time.time(), prepared_v3)
            ctrl._pending_update = pending_before  # pylint: disable=protected-access
        else:
            ctrl._committed_version = 2  # pylint: disable=protected-access
            ctrl._quarantined_version = 2  # pylint: disable=protected-access
            ctrl._quarantine_reason = 'invalid v2'  # pylint: disable=protected-access

        staged = tmp_path / 'config.yaml.v2.staged'
        staged.write_bytes(b'raw retry')
        serve_utils.write_config_snapshot_receipt(str(staged), 2, '2' * 64,
                                                  '3' * 64)
        prepared_v2 = _make_prepared_controller_config(2,
                                                       staged_path=str(staged))
        persisted = _make_update_spec()
        ctrl._record_committed_update = mock.Mock()  # pylint: disable=protected-access
        ctrl._install_controller_config = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.reset_mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_yaml_content',
                               return_value='service: immutable-v2'), \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=persisted), \
             mock.patch.object(
                 controller.serve_state,
                 'get_placement_projection_record',
                 return_value=(True, None, None, None)), \
             mock.patch.object(
                 controller.serve_state,
                 'add_or_update_version',
                 return_value=serve_state.VersionCommitResult.
                 IDEMPOTENT_RETRY):
            response = ctrl._commit_service_update(  # pylint: disable=protected-access
                2,
                persisted,
                'service: immutable-v2',
                serve_utils.UpdateMode.ROLLING,
                'incarnation-a',
                7,
                prepared_config=prepared_v2)

        assert response.status_code == 200
        ctrl._record_committed_update.assert_not_called()  # pylint: disable=protected-access
        ctrl._install_controller_config.assert_not_called()  # pylint: disable=protected-access
        ctrl._replica_manager.notify_version_pending.assert_not_called()  # pylint: disable=protected-access
        assert ctrl._pending_update is pending_before  # pylint: disable=protected-access
        assert not staged.exists()
        assert not pathlib.Path(
            serve_utils.generate_config_snapshot_receipt_file_name(
                str(staged))).exists()

    def test_blocked_v2_apply_and_v3_commit_keep_exact_prepared_configs(self):
        ctrl = _make_update_controller()
        first_apply_started = threading.Event()
        release_first_apply = threading.Event()
        applied_updates = []
        prepared_v2 = _make_prepared_controller_config(2,
                                                       source_is_staged=False)
        prepared_v3 = _make_prepared_controller_config(3,
                                                       source_is_staged=False)

        def _apply(version, _service, _mode, prepared_config):
            applied_updates.append((version, prepared_config))
            if version == 2:
                first_apply_started.set()
                assert release_first_apply.wait(timeout=5)

        ctrl._apply_service_update = mock.Mock(  # pylint: disable=protected-access
            side_effect=_apply)
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING,
            prepared_v2)
        worker = threading.Thread(target=ctrl._reconcile_pending_update_once)  # pylint: disable=protected-access
        worker.start()
        assert first_apply_started.wait(timeout=5)
        assert ctrl._applying_update.prepared_config is prepared_v2  # pylint: disable=protected-access

        # Regression: the old handler held _update_lock through the blocked
        # apply, so this second durable commit could not be recorded.
        ctrl._record_committed_update(  # pylint: disable=protected-access
            3, mock.sentinel.spec_v3, serve_utils.UpdateMode.ROLLING,
            prepared_v3)
        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['committed_version'] == 3
        assert status['applied_version'] == 1
        assert status['update_apply_pending']
        assert ctrl._pending_update.prepared_config is prepared_v3  # pylint: disable=protected-access
        assert ctrl._applying_update.prepared_config is prepared_v2  # pylint: disable=protected-access

        release_first_apply.set()
        worker.join(timeout=5)
        assert not worker.is_alive()
        assert ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access
        assert applied_updates == [(2, prepared_v2), (3, prepared_v3)]
        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['committed_version'] == 3
        assert status['applied_version'] == 3
        assert not status['update_apply_pending']

    def test_commits_coalesce_and_remove_unused_raw_stage(self, tmp_path):
        ctrl = _make_update_controller()
        ctrl._apply_service_update = mock.Mock()  # pylint: disable=protected-access
        staged_v2 = tmp_path / 'config.yaml.v2.staged'
        staged_v2.write_bytes(b'unused raw v2')
        serve_utils.write_config_snapshot_receipt(str(staged_v2), 2, '2' * 64,
                                                  '3' * 64)
        prepared_v2 = _make_prepared_controller_config(
            2, staged_path=str(staged_v2))
        prepared_v3 = _make_prepared_controller_config(3,
                                                       source_is_staged=False)
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING,
            prepared_v2)
        ctrl._record_committed_update(  # pylint: disable=protected-access
            3, mock.sentinel.spec_v3, serve_utils.UpdateMode.ROLLING,
            prepared_v3)

        assert not staged_v2.exists()
        assert not pathlib.Path(
            serve_utils.generate_config_snapshot_receipt_file_name(
                str(staged_v2))).exists()

        assert ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access
        ctrl._apply_service_update.assert_called_once_with(  # pylint: disable=line-too-long,protected-access
            3, mock.sentinel.spec_v3, serve_utils.UpdateMode.ROLLING,
            prepared_v3)
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
            2, mock.sentinel.original_spec, serve_utils.UpdateMode.ROLLING,
            None)
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

    def test_deterministic_failure_quarantines_and_keeps_old_runtime(self):
        ctrl = _make_update_controller()
        ctrl._apply_service_update = mock.Mock(  # pylint: disable=protected-access
            side_effect=controller.DeterministicServiceUpdateError(
                'invalid ingress port'))
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING)

        with mock.patch.object(controller.serve_state,
                               'quarantine_version',
                               return_value=True) as quarantine, \
             mock.patch.object(
                 ctrl,
                 '_schedule_supervised_recovery') as schedule_recovery:
            assert ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access

        quarantine.assert_called_once()
        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['committed_version'] == 2
        assert status['applied_version'] == 1
        assert not status['update_apply_pending']
        assert status['quarantined_version'] == 2
        assert 'invalid ingress port' in status['quarantine_reason']
        ctrl._replica_manager.clear_pending_version.assert_called_with(2)  # pylint: disable=line-too-long
        schedule_recovery.assert_not_called()

    def test_never_ready_runtime_failure_is_durably_quarantined(self):
        ctrl = _make_update_controller()
        ctrl._committed_version = 2  # pylint: disable=protected-access
        ctrl._applied_version = 2  # pylint: disable=protected-access
        failure = autoscalers.UnrecoverableRolloutFailure(
            version=2, reason='Version 2 never became ready.')

        with mock.patch.object(controller.time, 'time', return_value=123.0), \
             mock.patch.object(controller.serve_state,
                               'quarantine_version',
                               return_value=True) as quarantine:
            assert ctrl._quarantine_unrecoverable_rollout(  # pylint: disable=protected-access
                failure)

        quarantine.assert_called_once_with(
            'svc',
            2,
            failure.reason,
            quarantined_at=123.0,
            expected_service_hash='incarnation-a',
            expected_controller_owner=(123, '10.0.0.1'))
        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['committed_version'] == 2
        assert status['applied_version'] == 2
        assert status['quarantined_version'] == 2
        assert status['update_apply_failures'] == 1
        assert status['update_apply_error'] == failure.reason

    def test_stale_runtime_failure_cannot_quarantine_newer_applied_version(
            self):
        ctrl = _make_update_controller()
        ctrl._applied_version = 3  # pylint: disable=protected-access
        failure = autoscalers.UnrecoverableRolloutFailure(
            version=2, reason='Version 2 never became ready.')

        with mock.patch.object(controller.serve_state,
                               'quarantine_version') as quarantine:
            assert not ctrl._quarantine_unrecoverable_rollout(  # pylint: disable=protected-access
                failure)

        quarantine.assert_not_called()

    def test_failed_quarantine_write_preserves_pending_fence(self):
        ctrl = _make_update_controller()
        ctrl._apply_service_update = mock.Mock(  # pylint: disable=protected-access
            side_effect=controller.DeterministicServiceUpdateError(
                'invalid ingress port'))
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING)

        with mock.patch.object(controller.serve_state,
                               'quarantine_version',
                               return_value=False):
            assert not ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access

        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['update_apply_pending']
        assert status['quarantined_version'] is None
        ctrl._replica_manager.notify_version_pending.assert_called_with(2)  # pylint: disable=line-too-long

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

        # The failed version vanished when version 3 replaced it, so the
        # reconciler must loop immediately instead of sleeping before it can
        # apply the newer committed version.
        assert ctrl._reconcile_pending_update_once()  # pylint: disable=protected-access
        status = ctrl._get_update_status()  # pylint: disable=protected-access
        assert status['committed_version'] == 3
        assert status['applied_version'] == 1
        assert status['update_apply_pending']
        assert status['update_apply_error'] is None
        assert status['update_apply_failures'] == 0

    def test_newer_commit_during_retry_handoff_skips_backoff(self):
        ctrl = _make_update_controller()
        notify_calls = []

        def _commit_newer_during_failed_retry(version):
            notify_calls.append(version)
            if notify_calls == [2, 2]:
                ctrl._record_committed_update(  # pylint: disable=protected-access
                    3, mock.sentinel.spec_v3, serve_utils.UpdateMode.ROLLING)

        applied_versions = []

        def _apply(version, *_args):
            applied_versions.append(version)
            if version == 2:
                raise RuntimeError('failed before replacement commit')

        ctrl._replica_manager.notify_version_pending.side_effect = (  # pylint: disable=protected-access
            _commit_newer_during_failed_retry)
        ctrl._apply_service_update = mock.Mock(  # pylint: disable=protected-access
            side_effect=_apply)
        ctrl._record_committed_update(  # pylint: disable=protected-access
            2, mock.sentinel.spec_v2, serve_utils.UpdateMode.ROLLING)

        reconcile_once = ctrl._reconcile_pending_update_once  # pylint: disable=protected-access

        def _stop_after_newer_apply(*, wait=False):
            converged = reconcile_once(wait=wait)
            if applied_versions == [2, 3]:
                raise RuntimeError('stop after newer apply')
            return converged

        ctrl._reconcile_pending_update_once = mock.Mock(  # pylint: disable=protected-access
            side_effect=_stop_after_newer_apply)
        with mock.patch.object(
                controller.time,
                'sleep',
                side_effect=AssertionError('newer update hit retry backoff')), \
             pytest.raises(RuntimeError, match='stop after newer apply'):
            ctrl._run_update_reconciler()  # pylint: disable=protected-access

        assert notify_calls == [2, 2, 3]
        assert applied_versions == [2, 3]

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
             side_effect=lambda names: {
                 name: {
                     'handle': _FakeHandle(None, f'{name}.yaml')
                 }
                 for name in names
             }), \
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

    def test_capable_route_requires_and_emits_exact_marker(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080/',
                                accelerators={'L4': 1})
        info.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.CAPABLE)
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            system_recovery_allows_routing=lambda _info: True,
            system_recovery_route_marker=lambda _info, url:
            (system_recovery_route_lease.RouteMarker('1', 'a' * 32)
             if url == 'http://1.1.1.1:8080' else None),
            retire_system_recovery_route=lambda _info: None)

        result = _sync(ctrl, [info])
        assert result == {
            'http://1.1.1.1:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1',
                constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_VERSION,
                constants.SYSTEM_RECOVERY_ROUTE_REPLICA_ID_KEY: '1',
                constants.SYSTEM_RECOVERY_ROUTE_TOKEN_KEY: 'a' * 32,
            }
        }

        ctrl._replica_manager.system_recovery_route_marker = (
            lambda _info, _url: None)
        replica_info, num_ready = _sync_full(ctrl, [info])
        assert replica_info == {
            'http://1.1.1.1:8080': {
                constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
            }
        }
        assert num_ready == 1

    @pytest.mark.parametrize('unusable_url', [None, 'not-a-route-url'])
    def test_capable_unusable_url_retires_prior_generation(self, unusable_url):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        info.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.CAPABLE)
        retire = mock.Mock()
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            system_recovery_allows_routing=lambda _info: True,
            system_recovery_route_marker=lambda _info, _url:
            (system_recovery_route_lease.RouteMarker('1', 'a' * 32)),
            retire_system_recovery_route=retire)

        assert list(_sync(ctrl, [info])) == ['http://1.1.1.1:8080']
        retire.assert_not_called()

        # Force endpoint re-resolution as happens after the READY cache is
        # invalidated.  A malformed/missing replacement must stop heartbeat
        # renewal for the retained URL A even though this heavyweight snapshot
        # is intentionally treated as transiently empty by the LB.
        ctrl._lb_replica_cache.clear()  # pylint: disable=protected-access
        ctrl._lb_replica_cache_record_ids.clear()  # pylint: disable=protected-access
        info._url = unusable_url
        replica_info, num_ready = _sync_full(ctrl, [info])

        assert not replica_info
        assert num_ready == 1
        retire.assert_called_once_with(info)

    def test_invalid_ordinary_url_is_withheld(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://user:password@example.com:80',
                                accelerators={'L4': 1})

        replica_info, num_ready = _sync_full(ctrl, [info])

        assert not replica_info
        assert num_ready == 1

    @pytest.mark.parametrize('capable_url,ordinary_url,canonical_url', [
        ('HTTP://Example.COM:80/', 'http://example.com', 'http://example.com'),
        ('https://Example.COM:443', 'https://example.com/',
         'https://example.com'),
        ('http://[2001:0db8:0:0::1]:80', 'http://[2001:db8::1]/',
         'http://[2001:db8::1]'),
    ])
    def test_transport_alias_capable_ordinary_collision_is_fenced(
            self, capable_url, ordinary_url, canonical_url):
        ctrl = _make_controller()
        capable = _FakeReplicaInfo(1,
                                   serve_state.ReplicaStatus.READY,
                                   url=capable_url,
                                   accelerators={'L4': 1})
        capable.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.CAPABLE)
        ordinary = _FakeReplicaInfo(2,
                                    serve_state.ReplicaStatus.READY,
                                    url=ordinary_url,
                                    accelerators={'L4': 1})
        retire = mock.Mock()
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            system_recovery_allows_routing=lambda _info: True,
            system_recovery_route_marker=lambda info, _url:
            (system_recovery_route_lease.RouteMarker(str(
                info.replica_id), f'{info.replica_id:032x}')),
            retire_system_recovery_route=retire)

        assert _sync(ctrl, [capable, ordinary]) == {
            canonical_url: {
                constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
            }
        }
        retire.assert_called_once_with(capable)

    @pytest.mark.parametrize('capable_first', [False, True])
    def test_normalized_capable_ordinary_collision_is_fenced(
            self, capable_first):
        ctrl = _make_controller()
        capable = _FakeReplicaInfo(1,
                                   serve_state.ReplicaStatus.READY,
                                   url='http://1.1.1.1:8080/',
                                   accelerators={'L4': 1})
        capable.system_recovery_disposition = (
            system_recovery_state.SystemRecoveryDisposition.CAPABLE)
        ordinary = _FakeReplicaInfo(2,
                                    serve_state.ReplicaStatus.READY,
                                    url='http://1.1.1.1:8080',
                                    accelerators={'L4': 1})
        retired = set()

        def _marker(info, _url):
            if info.replica_id in retired:
                return None
            return system_recovery_route_lease.RouteMarker(
                str(info.replica_id), f'{info.replica_id:032x}')

        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            system_recovery_allows_routing=lambda _info: True,
            system_recovery_route_marker=_marker,
            retire_system_recovery_route=(
                lambda info: retired.add(info.replica_id)))
        infos = ([capable, ordinary] if capable_first else [ordinary, capable])

        replica_info, num_ready = _sync_full(ctrl, infos)
        assert replica_info == {
            'http://1.1.1.1:8080': {
                constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
            }
        }
        assert num_ready == 2
        assert retired == {1}

        # Removing the duplicate cannot rehabilitate the same capable
        # generation: collision retirement leaves an explicit fence.
        assert _sync(ctrl, [capable]) == {
            'http://1.1.1.1:8080': {
                constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
            }
        }

    def test_two_capable_rows_with_distinct_tokens_are_fenced(self):
        ctrl = _make_controller()
        infos = []
        retired = set()
        for replica_id in (1, 2):
            info = _FakeReplicaInfo(replica_id,
                                    serve_state.ReplicaStatus.READY,
                                    url='http://1.1.1.1:8080',
                                    accelerators={'L4': 1})
            info.system_recovery_disposition = (
                system_recovery_state.SystemRecoveryDisposition.CAPABLE)
            infos.append(info)
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            system_recovery_allows_routing=lambda _info: True,
            system_recovery_route_marker=lambda info, _url:
            (system_recovery_route_lease.RouteMarker(str(
                info.replica_id), f'{info.replica_id:032x}')),
            retire_system_recovery_route=(
                lambda info: retired.add(info.replica_id)))

        assert _sync(ctrl, infos) == {
            'http://1.1.1.1:8080': {
                constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
                    constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
            }
        }
        assert retired == {1, 2}

    def test_cache_rejects_recreated_same_numeric_replica_row(self):
        ctrl = _make_controller()
        old = _FakeReplicaInfo(1,
                               serve_state.ReplicaStatus.READY,
                               url='http://1.1.1.1:8080',
                               accelerators={'L4': 1})
        assert list(_sync(ctrl, [old])) == ['http://1.1.1.1:8080']

        replacement = _FakeReplicaInfo(1,
                                       serve_state.ReplicaStatus.READY,
                                       url='http://2.2.2.2:8080',
                                       accelerators={'A100': 8})
        replacement.replica_record_id = ('00000000-0000-0000-0000-000000000099')
        assert _sync(ctrl, [replacement]) == {
            'http://2.2.2.2:8080': {
                'gpu_type': 'A100',
                'gpu_count': '8'
            }
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

    def test_cold_sync_dedupes_shared_cluster_yaml_reads(self):
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
        shared_yaml = '/tmp/shared.yaml'
        shared_provider = {'provider': {'shared': True}}
        for info in infos:
            info.handle = mock.Mock(  # type: ignore[method-assign]
                return_value=_FakeHandle(info._accelerators, shared_yaml))

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
                    }), mock.patch.object(controller.global_user_state,
                                          'get_cluster_yaml_dict_multiple',
                                          return_value=[shared_provider
                                                       ]) as get_yamls:
            ctrl._get_lb_replica_info(  # pylint: disable=protected-access
                infos, None)

        get_yamls.assert_called_once_with([shared_yaml])
        assert infos[0].last_provider_config == {'shared': True}
        assert infos[1].last_provider_config == {'shared': True}

    def test_v2_cold_sync_batches_one_physical_fence_per_pool(self):
        ctrl = _make_controller()
        infos = [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'H200': 8}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.READY,
                             url='http://2.2.2.2:8080',
                             accelerators={'H200': 8}),
        ]
        cleanup_fence = controller.reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='research-usw2-h200',
            physical_cluster_uid='uid-a')
        active_fence_depth = 0
        fence_calls = []
        fence_entries = 0

        class _PhysicalFence:

            def __enter__(self):
                nonlocal active_fence_depth, fence_entries
                assert active_fence_depth == 0
                active_fence_depth += 1
                fence_entries += 1

            def __exit__(self, _exc_type, _exc_value, _traceback):
                nonlocal active_fence_depth
                active_fence_depth -= 1

        def _provider_fence(info, *, handle=None):
            # Each durable row proves its own handle before the physical pool
            # fence is entered once for the group.
            assert handle is not None
            fence_calls.append(info.replica_id)
            return _PhysicalFence()

        for info in infos:

            def _resolve_url(*,
                             cluster_record=None,
                             handle=None,
                             provider_config=None,
                             _info=info):
                del cluster_record, handle, provider_config
                assert active_fence_depth == 1
                return _info._url

            info._resolve_url = mock.Mock(  # type: ignore[method-assign]
                side_effect=_resolve_url)

        with mock.patch.object(controller.reserved_capacity,
                               'parse_protocol_v2_cleanup_fence',
                               return_value=cleanup_fence), mock.patch.object(
                                   controller.reserved_capacity,
                                   'protocol_v2_provider_fence',
                                   side_effect=_provider_fence):
            replica_info, num_ready = _sync_full(ctrl, infos)

        assert set(replica_info) == {
            'http://1.1.1.1:8080', 'http://2.2.2.2:8080'
        }
        assert num_ready == 2
        assert fence_calls == [1, 2]
        assert fence_entries == 1
        assert active_fence_depth == 0

    def test_840_replica_sync_provider_work_is_bounded_by_physical_pools(self):
        """LB heartbeats do no per-replica provider I/O at fleet scale."""
        ctrl = _make_controller()
        infos = [
            _FakeReplicaInfo(replica_id,
                             serve_state.ReplicaStatus.READY,
                             url=f'http://10.0.{replica_id // 256}.'
                             f'{replica_id % 256}:8080',
                             accelerators={'H200': 8})
            for replica_id in range(1, 841)
        ]
        fences = {
            id(info): controller.reserved_capacity.ProtocolV2CleanupFence(
                kubernetes_context=('east'
                                    if info.replica_id <= 420 else 'phx'),
                physical_cluster_uid=('east-uid' if info.replica_id <= 420 else
                                      'phx-uid')) for info in infos
        }
        physical_provider_entries = []

        @contextlib.contextmanager
        def _physical_fence(info):
            cleanup_fence = fences[id(info)]
            physical_provider_entries.append(
                (cleanup_fence.kubernetes_context,
                 cleanup_fence.physical_cluster_uid))
            yield

        def _provider_fence(info, *, handle=None):
            assert handle is not None
            return _physical_fence(info)

        with mock.patch.object(
                controller.reserved_capacity,
                'parse_protocol_v2_cleanup_fence',
                side_effect=lambda info: fences[id(info)]), \
             mock.patch.object(controller.reserved_capacity,
                               'protocol_v2_provider_fence',
                               side_effect=_provider_fence):
            cold_routes, cold_ready = _sync_full(ctrl, infos)
            warm_routes, warm_ready = _sync_full(ctrl, infos)

        assert cold_ready == warm_ready == 840
        assert len(cold_routes) == len(warm_routes) == 840
        # One live UID proof per physical pool per heartbeat, independent of
        # the 840 logical backends. Endpoint resolution is incremental: only
        # the cold pass resolves each URL.
        assert physical_provider_entries == [('east', 'east-uid'),
                                             ('phx', 'phx-uid'),
                                             ('east', 'east-uid'),
                                             ('phx', 'phx-uid')]
        assert all(info.url_resolutions == 1 for info in infos)

    def test_mixed_sync_completes_v2_phase_before_ambient(self):
        ctrl = _make_controller()
        ordinary = _FakeReplicaInfo(1,
                                    serve_state.ReplicaStatus.READY,
                                    url='http://1.1.1.1:8080',
                                    accelerators={'L4': 1})
        fenced = _FakeReplicaInfo(2,
                                  serve_state.ReplicaStatus.READY,
                                  url='http://2.2.2.2:8080',
                                  accelerators={'H200': 8})
        cleanup_fence = controller.reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='research-usw2-h200',
            physical_cluster_uid='uid-a')
        active_mode = None
        phase_entries = []

        @contextlib.contextmanager
        def _phase(mode):
            nonlocal active_mode
            assert active_mode is None
            active_mode = mode
            phase_entries.append(mode)
            try:
                yield mock.sentinel.admission
            finally:
                active_mode = None

        @contextlib.contextmanager
        def _physical_fence():
            assert (active_mode ==
                    controller.provider_phase.ProviderPhaseMode.V2_FENCED)
            yield

        def _parse(info):
            return cleanup_fence if info is fenced else None

        def _ordinary_resolve(**_kwargs):
            assert (active_mode ==
                    controller.provider_phase.ProviderPhaseMode.AMBIENT_LEGACY)
            return ordinary._url

        ordinary._resolve_url = mock.Mock(  # type: ignore[method-assign]
            side_effect=_ordinary_resolve)
        with mock.patch.object(controller.reserved_capacity,
                               'parse_protocol_v2_cleanup_fence',
                               side_effect=_parse), mock.patch.object(
                                   controller.reserved_capacity,
                                   'protocol_v2_provider_fence',
                                   return_value=_physical_fence()), \
             mock.patch.object(controller.provider_phase,
                               'provider_phase', side_effect=_phase):
            replica_info, num_ready = _sync_full(ctrl, [ordinary, fenced])

        assert phase_entries == [
            controller.provider_phase.ProviderPhaseMode.V2_FENCED,
            controller.provider_phase.ProviderPhaseMode.AMBIENT_LEGACY,
        ]
        assert set(replica_info) == {
            'http://1.1.1.1:8080', 'http://2.2.2.2:8080'
        }
        assert num_ready == 2

    def test_phase_timeout_does_not_publish_partial_route_caches(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'H200': 8})
        cleanup_fence = controller.reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='research-usw2-h200',
            physical_cluster_uid='uid-a')
        old_replica_cache = {9: ('http://old:8080', 'L4', 1)}
        old_record_ids = {9: '00000000-0000-0000-0000-000000000009'}
        ctrl._lb_replica_cache = dict(old_replica_cache)
        ctrl._lb_replica_cache_record_ids = dict(old_record_ids)
        ctrl._lb_translation_cache = dict(old_replica_cache)
        ctrl._lb_translation_cache_record_ids = dict(old_record_ids)
        timed_out = mock.MagicMock()
        timed_out.__enter__.side_effect = exceptions.ProviderPhaseTimeoutError(
            'busy')

        with mock.patch.object(controller.reserved_capacity,
                               'parse_protocol_v2_cleanup_fence',
                               return_value=cleanup_fence), mock.patch.object(
                                   controller.reserved_capacity,
                                   'protocol_v2_provider_fence',
                                   return_value=contextlib.nullcontext()), \
             mock.patch.object(controller.provider_phase,
                               'provider_phase', return_value=timed_out), \
             pytest.raises(exceptions.ProviderPhaseTimeoutError):
            _sync_full(ctrl, [info])

        assert ctrl._lb_replica_cache == old_replica_cache
        assert ctrl._lb_replica_cache_record_ids == old_record_ids
        assert ctrl._lb_translation_cache == old_replica_cache
        assert ctrl._lb_translation_cache_record_ids == old_record_ids

    @pytest.mark.parametrize('warm_cache', [False, True])
    def test_v2_retarget_clears_cold_or_warm_route_authoritatively(
            self, warm_cache):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'H200': 8})
        cleanup_fence = controller.reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='research-usw2-h200',
            physical_cluster_uid='uid-a')
        observed_uid = 'uid-a' if warm_cache else 'uid-b'
        provider_fence_calls = 0

        class _PhysicalFence:

            def __enter__(self):
                if observed_uid != cleanup_fence.physical_cluster_uid:
                    raise exceptions.KubernetesPhysicalClusterIdentityError(
                        'context was retargeted')

            def __exit__(self, _exc_type, _exc_value, _traceback):
                return None

        def _provider_fence(_info, *, handle=None):
            nonlocal provider_fence_calls
            assert handle is not None
            provider_fence_calls += 1
            return _PhysicalFence()

        with mock.patch.object(controller.reserved_capacity,
                               'parse_protocol_v2_cleanup_fence',
                               return_value=cleanup_fence), mock.patch.object(
                                   controller.reserved_capacity,
                                   'protocol_v2_provider_fence',
                                   side_effect=_provider_fence):
            if warm_cache:
                initial_info, initial_count = _sync_full(ctrl, [info])
                assert list(initial_info) == ['http://1.1.1.1:8080']
                assert initial_count == 1
                assert info.replica_id in ctrl._lb_replica_cache
                observed_uid = 'uid-b'

            replica_info, num_ready = _sync_full(ctrl, [info])

        # A physical retarget is authoritative absence, not a transient URL
        # miss: returning zero makes the LB apply the empty set immediately.
        assert not replica_info
        assert num_ready == 0
        assert info.replica_id not in ctrl._lb_replica_cache
        assert provider_fence_calls == (2 if warm_cache else 1)
        assert info.url_resolutions == (1 if warm_cache else 0)

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

    @staticmethod
    def _durable_snapshot(*,
                          generation=3,
                          request_generation=None,
                          observed_slots=None,
                          in_flight=None,
                          unknown_replica_ids=None,
                          fresh_aggregate_zero=False):
        observed_slots = dict(observed_slots or {})
        in_flight = dict(in_flight or {})
        unknown_replica_ids = set(unknown_replica_ids or ())
        read_started_monotonic = time.monotonic()
        return types.SimpleNamespace(
            service_name='svc',
            service_hash='svc-hash',
            demand_source_epoch=1,
            demand_feed_generation=generation,
            route_generation=4,
            route_sha256='a' * 64,
            route_source_epoch=2,
            receipt_watermark=[{
                'reporter_session_id': 'lb-a',
                'sequence': 5,
                'payload_sha256': 'b' * 64,
            }],
            request_information={
                'timestamps': [],
                'compatibility_demand_complete': True,
                'in_flight_by_replica_id': in_flight,
                'unknown_in_flight_replica_ids': sorted(unknown_replica_ids),
                'observed_slots_by_replica_id': observed_slots,
                'unknown_capacity_replica_ids': sorted(unknown_replica_ids),
                'reconcile_generation': (generation if request_generation
                                         is None else request_generation),
                'queue_depth': 0,
                'rejected_in_window': 0,
                'rejected_in_recent_window': 0,
            },
            normalized_demand={'queue_depth': 0},
            fresh_aggregate_zero=fresh_aggregate_zero,
            reconcile_authority=types.SimpleNamespace(
                read_started_monotonic=read_started_monotonic,
                deadline_monotonic=read_started_monotonic + 60))

    @staticmethod
    def _durable_autoscaler(target=1):
        scaler = mock.Mock()
        scaler.latest_version = 1
        scaler.replica_unit = 'physical_backend'
        scaler.reserved_capacity_fill = False
        scaler.has_recomputed_with_fresh_data.return_value = True
        scaler.get_final_target_num_replicas.return_value = target
        scaler.unrecoverable_rollout_failure = None
        scaler.current_launch_priority.return_value = 50
        scaler.configured_accelerator_shapes = {'L4': 1}
        scaler.capacity_target_by_accelerator = {'L4': target}
        scaler.capacity_target_complete = True
        scaler.info.return_value = {
            'demand_target_by_accelerator': {
                'L4': target
            }
        }
        return scaler

    @staticmethod
    def _logical_durable_autoscaler(target=0, *, emit_scale_up=False):
        scaler = mock.Mock(spec=autoscalers.ConcurrencyAutoscaler)
        scaler.latest_version = 1
        scaler.replica_unit = 'logical'
        scaler.reserved_capacity_fill = False
        scaler.has_recomputed_with_fresh_data.return_value = True
        scaler.get_final_target_num_replicas.return_value = target
        scaler.unrecoverable_rollout_failure = None
        scaler.current_launch_priority.return_value = 50
        scaler.configured_accelerator_shapes = {'L4': 1}
        scaler.capacity_target_by_accelerator = {'L4': target}
        scaler.capacity_target_complete = True
        scaler.target_num_replicas_by_accelerator = {'L4': target}
        scaler.warm_retention_target_by_accelerator = {}
        scaler.cold_launch_authority_by_accelerator = {}
        scaler.reconcile_generation = 0
        scaler.logical_target_state = None
        scaler.info.return_value = {
            'demand_target_by_accelerator': {
                'L4': target
            }
        }

        def _collect(report):
            scaler.reconcile_generation = report['reconcile_generation']

        def _generate(_replica_infos, _active_versions):
            if not emit_scale_up:
                scaler.logical_target_state = (scaler.latest_version,
                                               scaler.reconcile_generation,
                                               target)
                return []
            target_by_accelerator = (('L4', target),)
            accelerator_shapes = (('L4', 1),)
            scaler.logical_target_state = (scaler.latest_version,
                                           scaler.reconcile_generation, target,
                                           target_by_accelerator,
                                           accelerator_shapes)
            return [
                autoscalers.AutoscalerDecision(
                    autoscalers.AutoscalerDecisionOperator.SCALE_UP,
                    autoscalers.LogicalScaleTarget(
                        version=scaler.latest_version,
                        reconcile_generation=scaler.reconcile_generation,
                        target_capacity=target,
                        target_capacity_by_accelerator=(target_by_accelerator),
                        accelerator_shapes=accelerator_shapes,
                        cold_launch_authority_by_accelerator=()))
            ]

        scaler.collect_request_information.side_effect = _collect
        scaler.generate_scaling_decisions.side_effect = _generate
        return scaler

    @staticmethod
    def _run_promoted_reconciles(ctrl, snapshots):
        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               side_effect=list(snapshots)), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}), \
             mock.patch.object(ctrl,
                               '_persist_cost_rebalance_state',
                               return_value=True), \
             mock.patch.object(ctrl,
                               '_publish_ordered_paid_authority',
                               return_value=object()):
            for _ in snapshots:
                ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

    @staticmethod
    def _reserved_fill_allocation(*, grant: int = 1, accelerator: str = 'L4'):
        location = reserved_fill_planner.LocationSnapshot(
            cloud='Kubernetes',
            region='context-a',
            zone=None,
            accelerators=((accelerator, 1),),
            use_spot=False)
        pool_key = reserved_capacity_broker.make_pool_key(
            'context-a',
            accelerator,
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='uid-a')
        snapshot = reserved_fill_planner.PoolFillSnapshot(
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            pool_key=pool_key,
            physical_cluster_uid='uid-a',
            service_generation=7,
            worker_projection_sha256_by_accelerator=((accelerator.casefold(),
                                                      'e' * 64),),
            edge_cap=grant,
            broker_slot_width=1,
            free_slots=grant,
            free_slots_by_accelerator=((accelerator.casefold(), grant),),
            grant=grant,
            grant_epoch=23,
            observation_generation=13,
            observation_sequence=17,
            ordinary_zero_cost_admission_sequence=17,
            valid_until=time.time() + 60,
            locations=(location,))
        allocation = reserved_fill_planner.AuthenticatedAllocationMap.create(
            allocation_generation=5,
            allocation_claim_generation=11,
            service_version=1,
            ordinary_zero_cost_admission_sequence_high_water=17,
            reconciliation_gate_generation=29,
            reclaim_fleet_bundle_sha256='c' * 64,
            reclaim_policy_revision='reclaim-v1',
            reclaim_provider_inventory_sha256='d' * 64,
            pool_snapshots=(snapshot,))
        return allocation, pool_key

    def test_ordered_target_uses_supply_aware_cross_card_allocation(self):
        scaler = self._durable_autoscaler(65)
        scaler.configured_accelerator_shapes = {
            'L4': 1,
            'A100': 1,
            'H200': 1,
        }
        scaler.capacity_target_by_accelerator = {
            'A100': 20,
            'H200': 45,
        }
        scaler.info.return_value = {'demand_target_by_accelerator': {'L4': 65,}}

        assert controller.SkyServeController._ordered_capacity_target(  # pylint: disable=protected-access
            scaler) == {
                'l4': 0,
                'a100': 20,
                'h200': 45,
            }

    def test_ordered_target_rejects_incomplete_exact_card_state(self):
        scaler = self._durable_autoscaler(1)
        scaler.capacity_target_complete = False

        assert controller.SkyServeController._ordered_capacity_target(  # pylint: disable=protected-access
            scaler) is None

    @pytest.mark.parametrize(
        ('reserved_fill', 'sequenced_reserved_fill', 'target', 'force_zero',
         'expected_mode'), [
             (True, True, 1, False, 'ALLOCATION_BOUND'),
             (True, True, 0, True, 'UNBOUND_ZERO_REVOCATION'),
             (True, False, 0, True, 'NOT_APPLICABLE'),
             (False, False, 0, True, 'NOT_APPLICABLE'),
         ])
    def test_ordered_paid_publication_uses_explicit_allocation_mode(
            self, reserved_fill, sequenced_reserved_fill, target, force_zero,
            expected_mode):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._durable_demand_snapshot = self._durable_snapshot()  # pylint: disable=protected-access
        ctrl._ordinary_launch_binding_authority = types.SimpleNamespace(  # pylint: disable=protected-access
            service_lifecycle_epoch=3)
        scaler = self._durable_autoscaler(target)
        scaler.reserved_capacity_fill = reserved_fill
        allocation, _ = self._reserved_fill_allocation()
        repository = mock.Mock()
        expected = object()
        repository.publish.return_value = expected
        repository.project_reserved_supply.return_value = (
            controller.capacity_admission.ReservedSupplyProjection(
                pending_zero_cost_capacity_by_accelerator={'l4': 0},
                allocation_reserved_capacity_by_accelerator={'l4': target},
                economic_replica_infos=(),
                economic_kueue_capacity_by_replica_id={},
                economic_capacity_graph_sha256='f' * 64))
        scaler.supports_reserved_supply_economic_target.return_value = True
        scaler.economic_capacity_target_by_accelerator.return_value = {
            'l4': target
        }

        with mock.patch.object(controller.capacity_admission,
                               'CapacityAdmissionRepository',
                               return_value=repository):
            result = ctrl._publish_ordered_paid_authority(  # pylint: disable=protected-access
                scaler,
                1,
                sequenced_reserved_fill=sequenced_reserved_fill,
                force_zero=force_zero,
                reserved_fill_allocation_map=(
                    allocation if sequenced_reserved_fill else None))

        assert result is expected
        plan = repository.publish.call_args.args[0]
        assert plan.reserved_fill_authority.mode.value == expected_mode
        if expected_mode == 'ALLOCATION_BOUND':
            assert plan.reserved_fill_authority.allocation == allocation.identity
        else:
            assert plan.reserved_fill_authority.allocation is None

    def test_ordered_paid_publication_uses_compatibility_aware_tail_target(
            self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._durable_demand_snapshot = self._durable_snapshot()  # pylint: disable=protected-access
        ctrl._ordinary_launch_binding_authority = types.SimpleNamespace(  # pylint: disable=protected-access
            service_lifecycle_epoch=3)
        scaler = self._durable_autoscaler(1)
        scaler.reserved_capacity_fill = True
        scaler.configured_accelerator_shapes = {'L4': 1, 'H200': 1}
        scaler.capacity_target_by_accelerator = {'L4': 1}
        scaler.economic_capacity_target_by_accelerator.return_value = {
            'H200': 1
        }
        allocation, _ = self._reserved_fill_allocation(accelerator='H200')
        repository = mock.Mock()
        repository.project_reserved_supply.return_value = (
            controller.capacity_admission.ReservedSupplyProjection(
                pending_zero_cost_capacity_by_accelerator={
                    'l4': 0,
                    'h200': 0,
                },
                allocation_reserved_capacity_by_accelerator={
                    'l4': 0,
                    'h200': 1,
                },
                economic_replica_infos=(),
                economic_kueue_capacity_by_replica_id={},
                economic_capacity_graph_sha256='f' * 64))
        scaler.supports_reserved_supply_economic_target.return_value = True
        repository.publish.return_value = object()

        with mock.patch.object(controller.capacity_admission,
                               'CapacityAdmissionRepository',
                               return_value=repository):
            result = ctrl._publish_ordered_paid_authority(  # pylint: disable=protected-access
                scaler,
                1,
                sequenced_reserved_fill=True,
                reserved_fill_allocation_map=allocation)

        assert result is repository.publish.return_value
        plan = repository.publish.call_args.args[0]
        assert plan.capacity_target_by_accelerator == {
            'l4': 0,
            'h200': 1,
        }
        assert plan.allocation_reserved_capacity_by_accelerator == {
            'l4': 0,
            'h200': 1,
        }
        # Economic accounting never mutates the ordinary actuation target.
        assert scaler.capacity_target_by_accelerator == {'L4': 1}

    def test_ordered_paid_publication_rejects_unsupported_economic_target(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._durable_demand_snapshot = self._durable_snapshot()  # pylint: disable=protected-access
        ctrl._ordinary_launch_binding_authority = types.SimpleNamespace(  # pylint: disable=protected-access
            service_lifecycle_epoch=3)
        scaler = self._durable_autoscaler(1)
        scaler.reserved_capacity_fill = True
        scaler.supports_reserved_supply_economic_target.return_value = False
        allocation, _ = self._reserved_fill_allocation()
        repository = mock.Mock()

        with mock.patch.object(controller.capacity_admission,
                               'CapacityAdmissionRepository',
                               return_value=repository):
            result = ctrl._publish_ordered_paid_authority(  # pylint: disable=protected-access
                scaler,
                1,
                sequenced_reserved_fill=True,
                reserved_fill_allocation_map=allocation)

        assert result is None
        repository.project_reserved_supply.assert_not_called()
        repository.publish.assert_not_called()
        scaler.economic_capacity_target_by_accelerator.assert_not_called()

    def test_promoted_durable_demand_unavailable_suppresses_all_actuation(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        scaler = self._logical_durable_autoscaler()
        scaler.get_ready_replica_capacity.side_effect = (
            lambda info: info.planned_capacity)
        scaler.is_replica_on_zero_cost_location.return_value = False
        ctrl._autoscaler = scaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        ctrl._replica_counts_snapshot = {  # pylint: disable=protected-access
            'ready_replicas': 372,
            'total_replicas': 376,
            'ready_replicas_by_accelerator': {
                'L4': 372,
            },
            'total_replicas_by_accelerator': {
                'L4': 376,
            },
            'committed_capacity': 373,
        }
        ready = _FakeReplicaInfo(1,
                                 serve_state.ReplicaStatus.READY,
                                 accelerators={'A100': 4})
        ready.planned_capacity = 4
        ready.resources_override = {'accelerators': {'A100': 4}}
        provisioning = _FakeReplicaInfo(2,
                                        serve_state.ReplicaStatus.PROVISIONING,
                                        accelerators={'L4': 1})
        provisioning.resources_override = {'accelerators': {'L4': 1}}

        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               return_value=None), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[ready,
                                             provisioning]) as get_replicas, \
             mock.patch.object(ctrl,
                               '_publish_ordered_paid_authority') as publish:
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        get_replicas.assert_called_once_with('svc')
        counts = ctrl._replica_counts_snapshot  # pylint: disable=protected-access
        assert counts is not None
        assert counts['ready_replicas'] == 4
        assert counts['total_replicas'] == 5
        assert counts['physical_ready_replicas'] == 1
        assert counts['physical_total_replicas'] == 2
        assert counts['ready_replicas_by_accelerator'] == {'A100': 4}
        assert counts['provisioning_replicas_by_accelerator'] == {'L4': 1}
        assert counts['total_replicas_by_accelerator'] == {
            'A100': 4,
            'L4': 1,
        }
        assert counts['zero_cost_ready_replicas_by_accelerator'] == {}
        assert counts['zero_cost_total_replicas_by_accelerator'] == {}
        assert counts['replica_unit'] == 'logical_slot'
        assert counts['committed_capacity'] == 5
        assert counts['provisioning_capacity'] == 1
        publish.assert_not_called()
        ctrl._autoscaler.collect_request_information.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.update_logical_reconcile_snapshot.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.publish_logical_reconcile_state.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.publish_target_num_replicas.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_up_batch.assert_not_called()
        ctrl._replica_manager.invalidate_logical_reconcile_state.assert_called_once_with()  # pylint: disable=line-too-long

    def test_promoted_durable_snapshot_read_error_revokes_manager_authority(
            self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._durable_demand_snapshot = object()  # pylint: disable=protected-access
        ctrl._autoscaler = self._logical_durable_autoscaler()  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               side_effect=RuntimeError('database unavailable')), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]) as get_replicas:
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        get_replicas.assert_called_once_with('svc')
        assert ctrl._durable_demand_snapshot is None  # pylint: disable=protected-access
        ctrl._replica_manager.invalidate_logical_reconcile_state.assert_called_once_with()  # pylint: disable=line-too-long

    def test_promoted_logical_request_generation_mismatch_fails_closed(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        scaler = self._logical_durable_autoscaler(target=2)
        ctrl._autoscaler = scaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None

        self._run_promoted_reconciles(ctrl, [
            self._durable_snapshot(generation=68023, request_generation=68022)
        ])

        scaler.collect_request_information.assert_not_called()
        ctrl._replica_manager.publish_logical_reconcile_state.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.publish_target_num_replicas.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_up_batch.assert_not_called()
        ctrl._replica_manager.scale_up_to_logical_capacity.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_down_logically_batch.assert_not_called()  # pylint: disable=line-too-long
        assert ctrl._reconcile_generation == 0  # pylint: disable=protected-access

    def test_promoted_logical_autoscaler_generation_echo_mismatch_fails_closed(
            self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        scaler = self._logical_durable_autoscaler(target=2)

        def _collect_with_wrong_echo(report):
            scaler.reconcile_generation = report['reconcile_generation'] + 1

        scaler.collect_request_information.side_effect = (
            _collect_with_wrong_echo)
        ctrl._autoscaler = scaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None

        self._run_promoted_reconciles(
            ctrl, [self._durable_snapshot(generation=68023)])

        scaler.collect_request_information.assert_called_once()
        ctrl._replica_manager.publish_logical_reconcile_state.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.publish_target_num_replicas.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_up_batch.assert_not_called()
        ctrl._replica_manager.scale_up_to_logical_capacity.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_down_logically_batch.assert_not_called()  # pylint: disable=line-too-long
        assert ctrl._reconcile_generation == 0  # pylint: disable=protected-access

    def test_promoted_logical_snapshot_uses_exact_feed_and_target_generation(
            self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        scaler = self._logical_durable_autoscaler(target=2)
        ctrl._autoscaler = scaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        published = []

        def _capture_state(target, snapshot, _retirement_floor,
                           _retirement_shelter):
            assert ctrl._routing_state_lock._is_owned()  # pylint: disable=protected-access
            published.append((target, snapshot))
            return True

        ctrl._replica_manager.publish_logical_reconcile_state.side_effect = (  # pylint: disable=line-too-long,protected-access
            _capture_state)
        snapshot = self._durable_snapshot(generation=68023,
                                          observed_slots={
                                              11: 2,
                                              12: 1
                                          },
                                          in_flight={
                                              11: 1,
                                              12: 0
                                          },
                                          unknown_replica_ids={13})

        self._run_promoted_reconciles(ctrl, [snapshot])

        assert scaler.reconcile_generation == 68023
        assert len(published) == 1
        target, manager_snapshot = published[0]
        assert target == (1, 68023, 2)
        assert manager_snapshot.version == 1
        assert manager_snapshot.generation == 68023
        assert manager_snapshot.observed_slots_by_replica_id == {
            11: 2,
            12: 1,
        }
        assert manager_snapshot.in_flight_by_replica_id == {
            11: 1,
            12: 0,
        }
        assert manager_snapshot.unknown_replica_ids == frozenset({13})
        ctrl._replica_manager.publish_logical_target.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.update_logical_reconcile_snapshot.assert_not_called()  # pylint: disable=line-too-long
        # The process-local race fence advances independently and must never
        # stamp manager evidence.
        assert ctrl._reconcile_generation == 1  # pylint: disable=protected-access

    def test_promoted_logical_snapshot_replay_cannot_look_newer(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._autoscaler = self._logical_durable_autoscaler()  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        first = self._durable_snapshot(generation=68023)
        newer = self._durable_snapshot(generation=68024)

        self._run_promoted_reconciles(ctrl, [first, first, newer])

        state_calls = ctrl._replica_manager.publish_logical_reconcile_state.call_args_list  # pylint: disable=line-too-long
        snapshot_generations = [call.args[1].generation for call in state_calls]
        target_generations = [call.args[0][1] for call in state_calls]
        assert snapshot_generations == [68023, 68023, 68024]
        assert target_generations == [68023, 68023, 68024]
        assert ctrl._reconcile_generation == 3  # pylint: disable=protected-access

    def test_promoted_logical_snapshot_bridge_adds_no_teardown_path(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._autoscaler = self._logical_durable_autoscaler()  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None

        self._run_promoted_reconciles(
            ctrl, [self._durable_snapshot(generation=68023)])

        ctrl._replica_manager.publish_logical_reconcile_state.assert_called_once()  # pylint: disable=line-too-long
        ctrl._replica_manager.update_logical_reconcile_snapshot.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.publish_logical_target.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.reconcile_fresh_zero_paid_retirements.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.cancel_uncommitted_paid_retirements.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_down_logically_batch.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_down.assert_not_called()
        ctrl._replica_manager._terminate_replica.assert_not_called()  # pylint: disable=protected-access

    def test_promoted_logical_planning_failure_keeps_snapshot_private(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._autoscaler = self._logical_durable_autoscaler()  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None

        def _fail_planning(*_args, **_kwargs):
            ctrl._replica_manager.publish_logical_reconcile_state.assert_not_called()  # pylint: disable=line-too-long
            ctrl._replica_manager.update_logical_reconcile_snapshot.assert_not_called()  # pylint: disable=line-too-long
            return None

        with mock.patch.object(ctrl,
                               '_plan_scale_reconciliation',
                               side_effect=_fail_planning):
            self._run_promoted_reconciles(
                ctrl, [self._durable_snapshot(generation=68024)])

        ctrl._replica_manager.publish_logical_reconcile_state.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.update_logical_reconcile_snapshot.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.publish_logical_target.assert_not_called()  # pylint: disable=line-too-long

    def test_promoted_logical_publish_rejection_stops_later_actuation(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._autoscaler = self._logical_durable_autoscaler(  # pylint: disable=protected-access
            target=2, emit_scale_up=True)
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        ctrl._replica_manager.publish_logical_reconcile_state.return_value = (
            False)

        self._run_promoted_reconciles(
            ctrl, [self._durable_snapshot(generation=68023)])

        ctrl._replica_manager.publish_target_num_replicas.assert_called_once_with(  # pylint: disable=line-too-long
            2, expected_version=1)
        ctrl._replica_manager.publish_logical_reconcile_state.assert_called_once()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_up_batch.assert_not_called()
        ctrl._replica_manager.scale_up_to_logical_capacity.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_down_logically_batch.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_down.assert_not_called()

    def test_promoted_logical_final_currentness_race_stops_publication(self):

        class _RaceOnArmedEnter:

            def __init__(self, callback):
                self._lock = threading.RLock()
                self._callback = callback
                self._armed = False
                self.fired = False

            def arm(self):
                self._armed = True

            def __enter__(self):
                if self._armed and not self.fired:
                    self.fired = True
                    self._callback()
                self._lock.acquire()
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                del exc_type, exc_value, traceback
                self._lock.release()

        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._autoscaler = self._logical_durable_autoscaler(  # pylint: disable=protected-access
            target=2, emit_scale_up=True)
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None

        def _advance_actuation_epoch():
            with ctrl._actuation_epoch_lock:  # pylint: disable=protected-access
                ctrl._actuation_generation += 1  # pylint: disable=protected-access

        routing_lock = _RaceOnArmedEnter(_advance_actuation_epoch)
        ctrl._routing_state_lock = routing_lock  # pylint: disable=protected-access

        def _arm_final_race(*_args, **_kwargs):
            routing_lock.arm()
            return True

        ctrl._replica_manager.publish_target_num_replicas.side_effect = (
            _arm_final_race)

        self._run_promoted_reconciles(
            ctrl, [self._durable_snapshot(generation=68023)])

        assert routing_lock.fired
        ctrl._replica_manager.publish_logical_reconcile_state.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_up_batch.assert_not_called()
        ctrl._replica_manager.scale_up_to_logical_capacity.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_down_logically_batch.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_down.assert_not_called()

    def test_unknown_central_demand_mode_never_revives_legacy_actuation(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._autoscaler = self._durable_autoscaler()  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.capacity_admission,
                               'get_service_source_mode',
                               return_value=None), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos') as get_replicas:
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        get_replicas.assert_not_called()
        ctrl._replica_manager.publish_target_num_replicas.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_up_batch.assert_not_called()
        ctrl._replica_manager.invalidate_logical_reconcile_state.assert_called_once_with()  # pylint: disable=line-too-long

    def test_promoted_zero_cost_commit_forces_replan_before_paid_plan(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._autoscaler = self._durable_autoscaler()  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        accepted = object()
        ctrl._replica_manager.scale_up_batch.return_value = [accepted]
        decision = autoscalers.AutoscalerDecision(
            autoscalers.AutoscalerDecisionOperator.SCALE_UP,
            {'accelerators': {
                'L4': 1
            }})

        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               return_value=self._durable_snapshot()), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}), \
             mock.patch.object(
                 autoscalers,
                 'generate_controller_scaling_decisions',
                 return_value=[decision]), \
             mock.patch.object(ctrl,
                               '_persist_cost_rebalance_state',
                               return_value=True), \
             mock.patch.object(ctrl,
                               '_publish_ordered_paid_authority') as publish, \
             mock.patch.object(ctrl, '_notify_scale_reconcile') as notify:
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        ctrl._replica_manager.scale_up_batch.assert_called_once_with(
            [{
                'accelerators': {
                    'L4': 1
                }
            }],
            expected_version=1,
            launch_priority=50,
            paid_launch_allowed=False)
        publish.assert_not_called()
        notify.assert_called_once_with()

    def test_promoted_reserved_fill_commits_before_demand_snapshot(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._controller_owner_fingerprint = 'owner'  # pylint: disable=protected-access
        scaler = self._durable_autoscaler()
        scaler.reserved_capacity_fill = True
        scaler.replica_unit = 'logical'
        scaler.max_replicas = 64
        scaler.reserved_fill_materialized_capacity.return_value = 44
        scaler.reserved_fill_rotation_anchor.return_value = None
        ctrl._autoscaler = scaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        location = reserved_fill_planner.LocationSnapshot(cloud='Kubernetes',
                                                          region='phx',
                                                          zone=None,
                                                          accelerators=(('H200',
                                                                         1),),
                                                          use_spot=False)
        pool_key = reserved_capacity_broker.make_pool_key(
            'phx',
            'H200',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='uid-phx')
        pool = reserved_fill_planner.PoolFillSnapshot(
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            pool_key=pool_key,
            physical_cluster_uid='uid-phx',
            service_generation=1,
            worker_projection_sha256_by_accelerator=(('h200', 'e' * 64),),
            edge_cap=11,
            broker_slot_width=1,
            free_slots=11,
            free_slots_by_accelerator=(('h200', 11),),
            grant=11,
            grant_epoch=1,
            observation_generation=1,
            observation_sequence=1,
            ordinary_zero_cost_admission_sequence=1,
            valid_until=time.time() + 60,
            locations=(location,))
        allocation = reserved_fill_planner.AuthenticatedAllocationMap.create(
            allocation_generation=1,
            allocation_claim_generation=1,
            service_version=1,
            ordinary_zero_cost_admission_sequence_high_water=1,
            reconciliation_gate_generation=1,
            reclaim_fleet_bundle_sha256='c' * 64,
            reclaim_policy_revision='policy',
            reclaim_provider_inventory_sha256='d' * 64,
            pool_snapshots=(pool,))
        receipt = mock.Mock(accepted=(object(),), deferred=())
        receipt.accepted_rotation_anchor.return_value = None
        call_order = []
        accepted_plans = []

        def _pending(*_args, **_kwargs):
            call_order.append('pending')
            return controller.zero_cost_actuation.PendingFillSnapshot(
                capacity=0, debits=())

        def _replicas(_service_name):
            call_order.append('replicas')
            return []

        def _accept_plan(fill_plan):
            call_order.append('accept')
            accepted_plans.append(fill_plan)
            return receipt

        ctrl._replica_manager.pending_reserved_fill_snapshot.side_effect = (
            _pending)
        ctrl._replica_manager.accept_reserved_fill.side_effect = _accept_plan

        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               return_value=None) as read_demand, \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               side_effect=_replicas), \
             mock.patch.object(ctrl,
                               '_read_sequenced_reserved_fill_allocation',
                               return_value=(True, allocation)), \
             mock.patch.object(ctrl,
                               '_publish_ordered_paid_authority') as publish, \
             mock.patch.object(
                 controller.provider_phase,
                 'provider_phase') as provider_phase, \
             mock.patch.object(
                 ctrl,
                 '_notify_scale_reconcile',
                 side_effect=lambda: call_order.append('notify')) as notify:
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        assert call_order == ['pending', 'replicas', 'accept', 'notify']
        assert len(accepted_plans) == 1
        assert len(accepted_plans[0].intents) == 11
        scaler.reserved_fill_materialized_capacity.assert_called_once_with([])
        receipt.validate_for_plan.assert_called_once_with(accepted_plans[0])
        read_demand.assert_not_called()
        publish.assert_not_called()
        provider_phase.assert_not_called()
        ctrl._replica_manager.scale_up_batch.assert_not_called()
        ctrl._replica_manager.scale_up_to_logical_capacity.assert_not_called()
        ctrl._replica_manager.scale_down_logically_batch.assert_not_called()
        ctrl._replica_manager.reconcile_fresh_zero_paid_retirements.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.publish_target_num_replicas.assert_not_called()
        ctrl._replica_manager.invalidate_logical_reconcile_state.assert_called_once_with()  # pylint: disable=line-too-long
        notify.assert_called_once_with()

    def test_fill_headroom_includes_global_pending_capacity(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._controller_owner_fingerprint = 'owner'  # pylint: disable=protected-access
        scaler = self._logical_durable_autoscaler()
        scaler.max_replicas = 55
        scaler.reserved_fill_materialized_capacity.return_value = 44
        scaler.reserved_fill_rotation_anchor.return_value = None
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        allocation, pool_key = self._reserved_fill_allocation(grant=55)

        def _pending_debit(count):
            return (reserved_fill_planner.CommittedFillDebit(
                allocation_generation=allocation.allocation_generation,
                allocation_input_sha256=allocation.allocation_input_sha256,
                allocation_claim_generation=(
                    allocation.allocation_claim_generation),
                pool_key=pool_key,
                accelerator='l4',
                replica_slots=count),)

        ctrl._replica_manager.pending_reserved_fill_snapshot.side_effect = [
            controller.zero_cost_actuation.PendingFillSnapshot(
                capacity=2, debits=_pending_debit(2)),
            controller.zero_cost_actuation.PendingFillSnapshot(
                capacity=11, debits=_pending_debit(11)),
        ]
        accepted_plans = []

        def _accept(plan):
            accepted_plans.append(plan)
            receipt = mock.Mock(accepted=(object(),), deferred=())
            receipt.accepted_rotation_anchor.return_value = None
            return receipt

        ctrl._replica_manager.accept_reserved_fill.side_effect = _accept

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]):
            assert ctrl._accept_sequenced_reserved_fill(  # pylint: disable=protected-access
                allocation, scaler, 1, 9)
            assert len(accepted_plans) == 1
            assert len(accepted_plans[0].intents) == 9

            # Those accepted grants now consume global service headroom, even
            # if their incarnation or allocation generation is no longer the
            # current exact-card debit identity.
            assert not ctrl._accept_sequenced_reserved_fill(  # pylint: disable=protected-access
                allocation, scaler, 1, 10)

        assert len(accepted_plans) == 1
        scaler.reserved_fill_materialized_capacity.assert_has_calls(
            [mock.call([]), mock.call([])])

    def test_fill_reads_pending_before_replica_materialization_handoff(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._controller_owner_fingerprint = 'owner'  # pylint: disable=protected-access
        scaler = self._logical_durable_autoscaler()
        scaler.max_replicas = 10
        scaler.reserved_fill_materialized_capacity.side_effect = len
        scaler.reserved_fill_rotation_anchor.return_value = None
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        allocation, pool_key = self._reserved_fill_allocation()
        provider_location = types.SimpleNamespace(accelerators={'L4': 1})
        committed = types.SimpleNamespace(
            replica_id=41,
            status_property=types.SimpleNamespace(sky_down_status=None),
            reserved_fill=True,
            reserved_fill_allocation_generation=(
                allocation.allocation_generation),
            reserved_fill_allocation_input_sha256=(
                allocation.allocation_input_sha256),
            reserved_fill_allocation_claim_generation=(
                allocation.allocation_claim_generation),
            reserved_fill_reconciliation_gate_generation=(
                allocation.reconciliation_gate_generation),
            reserved_fill_reclaim_fleet_bundle_sha256=(
                allocation.reclaim_fleet_bundle_sha256),
            reserved_fill_reclaim_policy_revision=(
                allocation.reclaim_policy_revision),
            reserved_fill_reclaim_provider_inventory_sha256=(
                allocation.reclaim_provider_inventory_sha256),
            reserved_fill_pool_key=pool_key,
            reserved_fill_service_generation=7,
            reserved_fill_physical_cluster_uid='uid-a',
            get_spot_location=lambda: provider_location)
        pending_debit = reserved_fill_planner.CommittedFillDebit(
            allocation_generation=allocation.allocation_generation,
            allocation_input_sha256=allocation.allocation_input_sha256,
            allocation_claim_generation=(
                allocation.allocation_claim_generation),
            pool_key=pool_key,
            accelerator='l4',
            replica_slots=1)
        materialized = False
        read_order = []

        def _pending(*_args, **_kwargs):
            nonlocal materialized
            result = (controller.zero_cost_actuation.PendingFillSnapshot(
                capacity=0, debits=()) if materialized else
                      controller.zero_cost_actuation.PendingFillSnapshot(
                          capacity=1, debits=(pending_debit,)))
            read_order.append('pending')
            materialized = True
            return result

        def _replicas(_service_name):
            nonlocal materialized
            result = [committed] if materialized else []
            read_order.append('replicas')
            materialized = True
            return result

        ctrl._replica_manager.pending_reserved_fill_snapshot.side_effect = (
            _pending)
        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               side_effect=_replicas):
            assert not ctrl._accept_sequenced_reserved_fill(  # pylint: disable=protected-access
                allocation, scaler, 1, 9)

        assert read_order == ['pending', 'replicas']
        ctrl._replica_manager.accept_reserved_fill.assert_not_called()

    def test_fill_malformed_materialized_capacity_fails_closed(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        scaler = self._logical_durable_autoscaler()
        scaler.max_replicas = 10
        scaler.reserved_fill_materialized_capacity.side_effect = ValueError(
            'conflicting accelerator shapes')
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.pending_reserved_fill_snapshot.return_value = (
            controller.zero_cost_actuation.PendingFillSnapshot(capacity=0,
                                                               debits=()))
        allocation, _ = self._reserved_fill_allocation()
        ordinary = types.SimpleNamespace(
            reserved_fill=False,
            status_property=types.SimpleNamespace(sky_down_status=None))

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[ordinary]):
            assert not ctrl._accept_sequenced_reserved_fill(  # pylint: disable=protected-access
                allocation, scaler, 1, 9)

        ctrl._replica_manager.accept_reserved_fill.assert_not_called()

    def test_unknown_demand_after_empty_prefill_blocks_other_actuation(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        scaler = self._logical_durable_autoscaler(target=2, emit_scale_up=True)
        scaler.reserved_capacity_fill = True
        scaler.max_replicas = 20
        ctrl._autoscaler = scaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        allocation = object()

        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               return_value=None) as read_demand, \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]) as get_replicas, \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot') as get_runtime, \
             mock.patch.object(ctrl,
                               '_read_sequenced_reserved_fill_allocation',
                               return_value=(True, allocation)), \
             mock.patch.object(ctrl,
                               '_accept_sequenced_reserved_fill',
                               return_value=False) as accept, \
             mock.patch.object(ctrl,
                               '_publish_ordered_paid_authority') as publish:
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        accept.assert_called_once_with(allocation, scaler, 1, 0)
        read_demand.assert_called_once_with('svc', 'svc-hash')
        get_replicas.assert_called_once_with('svc')
        get_runtime.assert_not_called()
        publish.assert_not_called()
        ctrl._replica_manager.publish_target_num_replicas.assert_not_called()
        ctrl._replica_manager.publish_logical_reconcile_state.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_up_batch.assert_not_called()
        ctrl._replica_manager.scale_up_to_logical_capacity.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.scale_down_logically_batch.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.reconcile_fresh_zero_paid_retirements.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.cancel_uncommitted_paid_retirements.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.invalidate_logical_reconcile_state.assert_called_once_with()  # pylint: disable=line-too-long

    def test_promoted_paid_launch_uses_post_zero_cost_plan_authority(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._autoscaler = self._durable_autoscaler()  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        ctrl._replica_manager.scale_up_batch.side_effect = [[], []]
        authority = mock.Mock()
        decision = autoscalers.AutoscalerDecision(
            autoscalers.AutoscalerDecisionOperator.SCALE_UP,
            {'accelerators': {
                'L4': 1
            }})

        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               return_value=self._durable_snapshot()), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}), \
             mock.patch.object(
                 autoscalers,
                 'generate_controller_scaling_decisions',
                 return_value=[decision]), \
             mock.patch.object(ctrl,
                               '_persist_cost_rebalance_state',
                               return_value=True), \
             mock.patch.object(ctrl,
                               '_publish_ordered_paid_authority',
                               return_value=authority) as publish:
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        assert ctrl._replica_manager.scale_up_batch.call_args_list == [
            mock.call([{
                'accelerators': {
                    'L4': 1
                }
            }],
                      expected_version=1,
                      launch_priority=50,
                      paid_launch_allowed=False),
            mock.call([{
                'accelerators': {
                    'L4': 1
                }
            }],
                      expected_version=1,
                      launch_priority=50,
                      paid_launch_authority=authority),
        ]
        publish.assert_called_once_with(  # pylint: disable=protected-access
            ctrl._autoscaler,
            1,
            sequenced_reserved_fill=False,
            reserved_fill_allocation_map=None)

    def test_promoted_zero_target_still_publishes_revoking_plan(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._autoscaler = self._durable_autoscaler(0)  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        authority = mock.Mock()

        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               return_value=self._durable_snapshot()), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}), \
             mock.patch.object(
                 autoscalers,
                 'generate_controller_scaling_decisions',
                 return_value=[]), \
             mock.patch.object(ctrl,
                               '_persist_cost_rebalance_state',
                               return_value=True), \
             mock.patch.object(ctrl,
                               '_publish_ordered_paid_authority',
                               return_value=authority) as publish:
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        publish.assert_called_once_with(  # pylint: disable=protected-access
            ctrl._autoscaler,
            1,
            sequenced_reserved_fill=False,
            reserved_fill_allocation_map=None)
        ctrl._replica_manager.scale_up_batch.assert_not_called()

    def test_fresh_aggregate_zero_retires_paid_before_suppressing_scale_up(
            self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        scaler = self._durable_autoscaler(1)
        ctrl._autoscaler = scaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        authority = types.SimpleNamespace(generation=6, content_sha256='c' * 64)
        snapshot = self._durable_snapshot(fresh_aggregate_zero=True)
        decision = autoscalers.AutoscalerDecision(
            autoscalers.AutoscalerDecisionOperator.SCALE_UP,
            {'accelerators': {
                'L4': 1
            }})

        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               return_value=snapshot), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               side_effect=[[], []]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}), \
             mock.patch.object(
                 autoscalers,
                 'generate_controller_scaling_decisions',
                 return_value=[decision]), \
             mock.patch.object(ctrl,
                               '_persist_cost_rebalance_state',
                               return_value=True), \
             mock.patch.object(ctrl,
                               '_publish_ordered_paid_authority',
                               return_value=authority) as publish:
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        scaler.clear_paid_launch_authority_for_fresh_zero.assert_called_once_with(
        )
        publish.assert_called_once_with(scaler,
                                        1,
                                        sequenced_reserved_fill=False,
                                        force_zero=True)
        retirement_authority = (
            ctrl._replica_manager.reconcile_fresh_zero_paid_retirements.
            call_args.args[0])
        assert retirement_authority == paid_retirement.FreshZeroAuthority(
            service_hash='svc-hash',
            demand_source_epoch=1,
            demand_feed_generation=3,
            capacity_plan_generation=6,
            capacity_plan_sha256='c' * 64,
            route_generation=4)
        ctrl._replica_manager.scale_up_batch.assert_not_called()

    def test_newer_positive_demand_cancels_uncommitted_paid_retirement(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        ctrl._autoscaler = self._durable_autoscaler(0)  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        snapshot = self._durable_snapshot()
        snapshot.normalized_demand['recent_request_count'] = 1

        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               return_value=snapshot), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}), \
             mock.patch.object(
                 autoscalers,
                 'generate_controller_scaling_decisions',
                 return_value=[]), \
             mock.patch.object(ctrl,
                               '_persist_cost_rebalance_state',
                               return_value=True), \
             mock.patch.object(ctrl,
                               '_publish_ordered_paid_authority',
                               return_value=mock.Mock()):
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        ctrl._replica_manager.cancel_uncommitted_paid_retirements.assert_called_once_with(
            'svc-hash', 3)

    def test_sequenced_gate_selects_authenticated_map_without_fallback(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'incarnation'  # pylint: disable=protected-access
        ctrl._controller_owner = (7, '10.0.0.7')  # pylint: disable=protected-access
        gate_repository = mock.Mock()
        gate_repository.read_reconciliation_gate.return_value = (
            types.SimpleNamespace(sequenced_active=True))
        allocation_repository = mock.Mock()
        allocation = object()
        allocation_repository.read_current.return_value = allocation
        ctrl._reserved_fill_observation_repository = gate_repository  # pylint: disable=protected-access
        ctrl._reserved_fill_allocation_repository = allocation_repository  # pylint: disable=protected-access

        selected, observed = ctrl._read_sequenced_reserved_fill_allocation()  # pylint: disable=protected-access

        assert selected is True
        assert observed is allocation
        allocation_repository.read_current.assert_called_once_with(
            'svc', 'incarnation', (7, '10.0.0.7'))

    def test_sequenced_gate_missing_map_withholds_legacy_fill(self):
        ctrl = _make_controller()
        ctrl._service_hash = 'incarnation'  # pylint: disable=protected-access
        ctrl._controller_owner = (7, '10.0.0.7')  # pylint: disable=protected-access
        gate_repository = mock.Mock()
        gate_repository.read_reconciliation_gate.return_value = (
            types.SimpleNamespace(sequenced_active=True))
        allocation_repository = mock.Mock()
        allocation_repository.read_current.return_value = None
        ctrl._reserved_fill_observation_repository = gate_repository  # pylint: disable=protected-access
        ctrl._reserved_fill_allocation_repository = allocation_repository  # pylint: disable=protected-access

        selected, observed = ctrl._read_sequenced_reserved_fill_allocation()  # pylint: disable=protected-access

        assert selected is True
        assert observed is None

    def test_sequenced_missing_allocation_revokes_paid_and_launches_nothing(
            self):
        ctrl = _make_controller()
        ctrl._service_hash = 'svc-hash'  # pylint: disable=protected-access
        scaler = self._durable_autoscaler(target=1)
        scaler.reserved_capacity_fill = True
        scaler.max_replicas = 20
        scaler.sequenced_reserved_fill_holdings.return_value = ()
        scaler.sequenced_reserved_fill_planning.return_value = (
            contextlib.nullcontext())
        ctrl._autoscaler = scaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        decision = autoscalers.AutoscalerDecision(
            autoscalers.AutoscalerDecisionOperator.SCALE_UP,
            {'accelerators': {
                'L4': 1
            }})
        zero_authority = object()

        with mock.patch.object(
                controller.capacity_admission,
                'get_service_source_mode',
                return_value=(controller.capacity_admission.DemandSourceMode.
                              DURABLE_FEED, 1)), \
             mock.patch.object(controller.demand_state,
                               'get_autoscaling_snapshot',
                               return_value=self._durable_snapshot()), \
             mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}), \
             mock.patch.object(
                 autoscalers,
                 'generate_controller_scaling_decisions',
                 return_value=[decision]), \
             mock.patch.object(
                 ctrl,
                 '_read_sequenced_reserved_fill_allocation',
                 return_value=(True, None)), \
             mock.patch.object(
                 ctrl,
                 '_plan_scale_reconciliation',
                 return_value=None) as plan, \
             mock.patch.object(
                 ctrl,
                 '_publish_ordered_paid_authority',
                 return_value=zero_authority) as publish:
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        publish.assert_called_once_with(scaler,
                                        1,
                                        sequenced_reserved_fill=True,
                                        force_zero=True)
        plan.assert_not_called()
        ctrl._replica_manager.scale_up_batch.assert_not_called()
        ctrl._replica_manager.scale_up_to_logical_capacity.assert_not_called()
        ctrl._replica_manager.publish_target_num_replicas.assert_not_called()
        ctrl._replica_manager.invalidate_logical_reconcile_state.assert_called_once_with()  # pylint: disable=line-too-long

    def test_reconcile_derives_shelter_before_sequenced_planning(self):
        ctrl = _make_controller()
        decision_autoscaler = mock.Mock()
        decision_autoscaler.latest_version = 2
        decision_autoscaler.reserved_capacity_fill = True
        decision_autoscaler.replica_unit = 'physical_backend'
        decision_autoscaler.max_replicas = 20
        decision_autoscaler.generate_scaling_decisions.return_value = [
            autoscalers.AutoscalerDecision(
                autoscalers.AutoscalerDecisionOperator.SCALE_DOWN, 7)
        ]
        decision_autoscaler.has_recomputed_with_fresh_data.return_value = False
        decision_autoscaler.sequenced_reserved_fill_holdings.return_value = ()
        sequenced_context = mock.MagicMock()
        decision_autoscaler.sequenced_reserved_fill_planning.return_value = (
            sequenced_context)
        ctrl._autoscaler = decision_autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.spot_placer = None
        allocation = mock.Mock()
        shelter = mock.Mock(target_capacity=0, authority_current=True)

        with mock.patch.object(controller.reserved_fill_planner,
                               'derive_sequenced_retirement_shelter',
                               return_value=shelter) as derive:
            plan = ctrl._plan_scale_reconciliation(  # pylint: disable=protected-access
                decision_autoscaler,
                2,
                0,
                0,
                0, [], [2],
                None,
                sequenced_reserved_fill=True,
                sequenced_reserved_fill_allocation=allocation)

        derive.assert_called_once_with(
            allocation=allocation,
            holdings=(),
            service_version=2,
            max_capacity=20,
            capacity_unit=(
                controller.reserved_fill_planner.FillCapacityUnit.PHYSICAL))
        decision_autoscaler.sequenced_reserved_fill_planning.assert_called_once_with(
        )
        sequenced_context.__enter__.assert_called_once_with()
        sequenced_context.__exit__.assert_called_once()
        assert plan is not None
        assert plan[0] == []

    def test_run_autoscaler_uses_runtime_snapshot_for_active_versions(self):
        ctrl = _make_controller()
        ctrl._autoscaler = mock.Mock()  # pylint: disable=protected-access
        ctrl._autoscaler.latest_version = 2
        ctrl._autoscaler.reserved_capacity_fill = False
        ctrl._autoscaler.generate_scaling_decisions.return_value = []
        ctrl._autoscaler.has_recomputed_with_fresh_data.return_value = True
        ctrl._autoscaler.get_final_target_num_replicas.return_value = 0
        ctrl._autoscaler.get_decision_interval.return_value = 0
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access

        call_order = []

        def _clear_reconciliation_signal():
            call_order.append('clear')

        def _get_replica_infos(_service_name):
            call_order.append('replica-read')
            return []

        def _get_runtime_snapshot(_service_name, *, require_version):
            assert require_version
            call_order.append('runtime-read')
            return {'active_versions': [2]}

        def _wait_for_reconciliation(interval):
            call_order.append('wait')
            assert interval == 0
            raise StopIteration

        ctrl._replica_manager.clear_scale_reconciliation_signal.side_effect = (  # pylint: disable=line-too-long
            _clear_reconciliation_signal)
        ctrl._replica_manager.wait_for_scale_reconciliation.side_effect = (  # pylint: disable=line-too-long
            _wait_for_reconciliation)

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               side_effect=_get_replica_infos), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_from_name',
                 side_effect=AssertionError(
                     'autoscaler must not use joined service reads')), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 side_effect=_get_runtime_snapshot):
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        ctrl._autoscaler.generate_scaling_decisions.assert_called_once_with([],
                                                                            [2])
        ctrl._replica_manager.publish_target_num_replicas.assert_called_once_with(  # pylint: disable=line-too-long
            0, expected_version=2)
        assert call_order == ['replica-read', 'runtime-read']
        ctrl._replica_manager.clear_scale_reconciliation_signal.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.wait_for_scale_reconciliation.assert_not_called()  # pylint: disable=line-too-long

    def test_run_autoscaler_withholds_rebuilt_blind_target(self):
        ctrl = _make_controller()
        ctrl._autoscaler = mock.Mock()  # pylint: disable=protected-access
        ctrl._autoscaler.latest_version = 2
        ctrl._autoscaler.reserved_capacity_fill = False
        ctrl._autoscaler.generate_scaling_decisions.return_value = []
        ctrl._autoscaler.has_recomputed_with_fresh_data.return_value = False
        ctrl._autoscaler.get_final_target_num_replicas.return_value = 0
        ctrl._autoscaler.get_decision_interval.return_value = 0
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.wait_for_scale_reconciliation.side_effect = (  # pylint: disable=line-too-long
            StopIteration)

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [2]}):
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        ctrl._replica_manager.publish_target_num_replicas.assert_called_once_with(  # pylint: disable=line-too-long
            None, expected_version=2)
        ctrl._autoscaler.get_final_target_num_replicas.assert_not_called()
        ctrl._replica_manager.clear_scale_reconciliation_signal.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.wait_for_scale_reconciliation.assert_not_called()  # pylint: disable=line-too-long

    def test_run_autoscaler_does_not_publish_legacy_fill_as_demand_target(self):
        ctrl = _make_controller()
        ctrl._autoscaler = mock.Mock()  # pylint: disable=protected-access
        ctrl._autoscaler.latest_version = 2
        ctrl._autoscaler.reserved_capacity_fill = True
        ctrl._autoscaler.fill_target = 3
        ctrl._autoscaler.generate_scaling_decisions.return_value = []
        ctrl._autoscaler.has_recomputed_with_fresh_data.return_value = True
        ctrl._autoscaler.get_final_target_num_replicas.return_value = 0
        ctrl._autoscaler.get_decision_interval.return_value = 0
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.wait_for_scale_reconciliation.side_effect = (  # pylint: disable=line-too-long
            StopIteration)

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [2]}):
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        ctrl._replica_manager.publish_target_num_replicas.assert_called_once_with(  # pylint: disable=line-too-long
            0, expected_version=2)
        ctrl._replica_manager.clear_scale_reconciliation_signal.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.wait_for_scale_reconciliation.assert_not_called()  # pylint: disable=line-too-long

    def test_run_autoscaler_ignores_disabled_stale_fill_target(self):
        ctrl = _make_controller()
        ctrl._autoscaler = mock.Mock()  # pylint: disable=protected-access
        ctrl._autoscaler.latest_version = 2
        ctrl._autoscaler.reserved_capacity_fill = False
        ctrl._autoscaler.fill_target = 3
        ctrl._autoscaler.generate_scaling_decisions.return_value = []
        ctrl._autoscaler.has_recomputed_with_fresh_data.return_value = True
        ctrl._autoscaler.get_final_target_num_replicas.return_value = 0
        ctrl._autoscaler.get_decision_interval.return_value = 0
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.wait_for_scale_reconciliation.side_effect = (  # pylint: disable=line-too-long
            StopIteration)

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [2]}):
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        ctrl._replica_manager.publish_target_num_replicas.assert_called_once_with(  # pylint: disable=line-too-long
            0, expected_version=2)
        ctrl._replica_manager.clear_scale_reconciliation_signal.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.wait_for_scale_reconciliation.assert_not_called()  # pylint: disable=line-too-long

    def test_notify_during_reconcile_is_not_lost(self):
        ctrl = _make_controller()
        ctrl._autoscaler = mock.Mock()  # pylint: disable=protected-access
        ctrl._autoscaler.latest_version = 2
        ctrl._autoscaler.reserved_capacity_fill = False
        ctrl._autoscaler.generate_scaling_decisions.return_value = []
        ctrl._autoscaler.has_recomputed_with_fresh_data.return_value = False
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access

        first_reconcile_started = threading.Event()
        release_first_reconcile = threading.Event()
        second_reconcile_seen = threading.Event()
        reads = 0

        def _read_replicas(_service_name):
            nonlocal reads
            reads += 1
            if reads == 1:
                first_reconcile_started.set()
                assert release_first_reconcile.wait(timeout=5)
            elif reads == 2:
                second_reconcile_seen.set()
                ctrl._scale_reconcile_coordinator.stop()  # pylint: disable=protected-access
            return []

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               side_effect=_read_replicas), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [2]}):
            worker = threading.Thread(target=ctrl._run_autoscaler)  # pylint: disable=protected-access
            worker.start()
            assert first_reconcile_started.wait(timeout=5)
            ctrl._notify_scale_reconcile()  # pylint: disable=protected-access
            release_first_reconcile.set()
            assert second_reconcile_seen.wait(timeout=5)
            worker.join(timeout=5)

        assert not worker.is_alive()
        assert reads == 2
        ctrl._replica_manager.clear_scale_reconciliation_signal.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.wait_for_scale_reconciliation.assert_not_called()  # pylint: disable=line-too-long

    def test_slow_manager_actuation_does_not_hold_update_epoch_lock(self):
        ctrl = _make_controller()
        decision_autoscaler = mock.Mock()
        decision_autoscaler.latest_version = 2
        decision_autoscaler.reserved_capacity_fill = False
        decision_autoscaler.has_recomputed_with_fresh_data.return_value = False
        decision_autoscaler.generate_scaling_decisions.return_value = [
            autoscalers.AutoscalerDecision(
                autoscalers.AutoscalerDecisionOperator.SCALE_UP, None)
        ]
        ctrl._autoscaler = decision_autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        manager_entered = threading.Event()
        release_manager = threading.Event()

        def _slow_scale_up(*_args, **_kwargs):
            manager_entered.set()
            assert release_manager.wait(timeout=5)

        ctrl._replica_manager.scale_up_batch.side_effect = _slow_scale_up  # pylint: disable=line-too-long,protected-access
        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [2]}):
            reconcile = threading.Thread(
                target=ctrl._reconcile_scale_once,  # pylint: disable=protected-access
                args=(0,))
            reconcile.start()
            assert manager_entered.wait(timeout=5)

            transition_acquired = threading.Event()
            transition_generation = []

            def _begin_update():
                transition_generation.append(ctrl._begin_actuation_transition())  # pylint: disable=protected-access
                transition_acquired.set()

            update = threading.Thread(target=_begin_update)
            update.start()
            assert transition_acquired.wait(timeout=1)
            assert transition_generation == [1]
            release_manager.set()
            reconcile.join(timeout=5)
            update.join(timeout=5)

        assert not reconcile.is_alive()
        assert not update.is_alive()
        ctrl._finish_actuation_transition(1)  # pylint: disable=protected-access

    def test_blocking_decision_preload_does_not_hold_routing_epoch_lock(self):
        """PostgreSQL/provider input reads cannot freeze controller HTTP."""
        ctrl = _make_controller()
        decision_autoscaler = mock.Mock()
        decision_autoscaler.latest_version = 2
        decision_autoscaler.reserved_capacity_fill = False
        decision_autoscaler.has_recomputed_with_fresh_data.return_value = False
        decision_autoscaler.generate_scaling_decisions.return_value = []
        preload_started = threading.Event()
        release_preload = threading.Event()
        decision_inputs = object()

        def _blocking_preload(_autoscaler, _replicas):
            preload_started.set()
            assert release_preload.wait(timeout=5)
            return decision_inputs

        ctrl._autoscaler = decision_autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [2]}), \
             mock.patch.object(
                 controller.serve_state,
                 'get_scale_planning_state_fingerprint',
                 return_value='stable'), \
             mock.patch.object(
                 autoscalers,
                 'controller_prepares_scaling_decision_inputs',
                 return_value=True), \
             mock.patch.object(
                 autoscalers,
                 'prepare_controller_scaling_decision_inputs',
                 side_effect=_blocking_preload) as prepare_inputs, \
             mock.patch.object(
                 autoscalers,
                 'generate_controller_scaling_decisions',
                 return_value=[]) as generate_decisions, \
             mock.patch.object(
                 ctrl, '_persist_cost_rebalance_state', return_value=True):
            reconcile = threading.Thread(
                target=ctrl._reconcile_scale_once,  # pylint: disable=protected-access
                args=(0,))
            reconcile.start()
            assert preload_started.wait(timeout=5)
            acquired = ctrl._routing_state_lock.acquire(timeout=1)  # pylint: disable=protected-access
            assert acquired
            if acquired:
                ctrl._routing_state_lock.release()  # pylint: disable=protected-access
            release_preload.set()
            reconcile.join(timeout=5)

        assert not reconcile.is_alive()
        prepare_inputs.assert_called_once_with(decision_autoscaler, [])
        generate_decisions.assert_called_once_with(decision_autoscaler, [], [2],
                                                   decision_inputs)

    def test_built_in_historical_spec_read_precedes_routing_epoch_lock(self):
        """The real built-in preload cannot move its spec read under lock."""
        ctrl = _make_controller()
        spec = _make_autoscaler_spec(target_qps_per_replica={'A100': 10.0})
        decision_autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', spec, version=2)
        old_spec = _make_autoscaler_spec(target_qps_per_replica={'L4': 0.1})
        old = mock.Mock()
        old.replica_id = 1
        old.version = 1
        old.is_terminal = False
        decision_autoscaler._gpu_shape_cache[1] = ('L4', 1)  # pylint: disable=protected-access
        decision_autoscaler._replica_cost_cache[1] = 0.0  # pylint: disable=protected-access
        preload_started = threading.Event()
        release_preload = threading.Event()

        def _blocking_get_specs(service_name, versions):
            assert service_name == 'svc'
            assert versions == [1]
            preload_started.set()
            assert release_preload.wait(timeout=5)
            return {1: old_spec}

        ctrl._autoscaler = decision_autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[old]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1, 2]}), \
             mock.patch.object(
                 controller.serve_state,
                 'get_scale_planning_state_fingerprint',
                 return_value='stable'), \
             mock.patch.object(controller.serve_state,
                               'get_specs',
                               side_effect=_blocking_get_specs) as get_specs, \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               side_effect=AssertionError) as get_spec, \
             mock.patch.object(ctrl, '_get_replica_counts', return_value={}), \
             mock.patch.object(
                 ctrl,
                 '_get_free_reserved_slots_by_accelerator',
                 return_value={}), \
             mock.patch.object(decision_autoscaler,
                               '_generate_scaling_decisions_locked',
                               return_value=[]), \
             mock.patch.object(
                 ctrl, '_persist_cost_rebalance_state', return_value=True):
            reconcile = threading.Thread(
                target=ctrl._reconcile_scale_once,  # pylint: disable=protected-access
                args=(0,))
            reconcile.start()
            assert preload_started.wait(timeout=5)
            acquired = ctrl._routing_state_lock.acquire(timeout=1)  # pylint: disable=protected-access
            assert acquired
            if acquired:
                ctrl._routing_state_lock.release()  # pylint: disable=protected-access
            release_preload.set()
            reconcile.join(timeout=5)

        assert not reconcile.is_alive()
        get_specs.assert_called_once_with('svc', [1])
        get_spec.assert_not_called()

    def test_demand_and_generation_publish_atomically(self):
        """A planner cannot observe the new generation before its gauges."""

        class _TrackingRLock:

            def __init__(self):
                self._lock = threading.RLock()
                self.acquire_attempted = threading.Event()

            def __enter__(self):
                self.acquire_attempted.set()
                self._lock.acquire()
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                del exc_type, exc_value, traceback
                self._lock.release()

        ctrl = _make_controller()
        lock = _TrackingRLock()
        ctrl._routing_state_lock = lock  # pylint: disable=protected-access
        ctrl._autoscaler = mock.Mock()  # pylint: disable=protected-access
        ctrl._autoscaler.replica_unit = 'physical_backend'  # pylint: disable=protected-access
        request_data = {'request_aggregator': {'timestamps': []}}
        result = []

        def _publish():
            result.append(
                ctrl._apply_prepared_load_balancer_report(  # pylint: disable=protected-access
                    request_data, request_data, [], {}, (True, True, True), {},
                    False))

        with lock:
            lock.acquire_attempted.clear()
            publisher = threading.Thread(target=_publish)
            publisher.start()
            assert lock.acquire_attempted.wait(timeout=5)
            assert ctrl._reconcile_generation == 0  # pylint: disable=protected-access
            ctrl._autoscaler.collect_request_information.assert_not_called()  # pylint: disable=line-too-long,protected-access
            assert ctrl._scale_reconcile_coordinator.generation == 0  # pylint: disable=protected-access

        publisher.join(timeout=5)
        assert not publisher.is_alive()
        assert result == [True]
        assert ctrl._reconcile_generation == 1  # pylint: disable=protected-access
        report = ctrl._autoscaler.collect_request_information.call_args.args[0]  # pylint: disable=line-too-long,protected-access
        assert report['reconcile_generation'] == 1
        assert ctrl._scale_reconcile_coordinator.generation == 1  # pylint: disable=protected-access

    def test_changed_durable_snapshot_discards_blocked_preload(self):
        """A manager mutation cannot be planned from pre-mutation rows."""
        ctrl = _make_controller()
        decision_autoscaler = mock.Mock()
        decision_autoscaler.latest_version = 2
        decision_autoscaler.reserved_capacity_fill = False
        preload_started = threading.Event()
        release_preload = threading.Event()

        def _blocking_preload(_autoscaler, _replicas):
            preload_started.set()
            assert release_preload.wait(timeout=5)
            return object()

        ctrl._autoscaler = decision_autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [2]}), \
             mock.patch.object(
                 controller.serve_state,
                 'get_scale_planning_state_fingerprint',
                 side_effect=['before-mutation', 'after-mutation']), \
             mock.patch.object(
                 autoscalers,
                 'controller_prepares_scaling_decision_inputs',
                 return_value=True), \
             mock.patch.object(
                 autoscalers,
                 'prepare_controller_scaling_decision_inputs',
                 side_effect=_blocking_preload), \
             mock.patch.object(
                 autoscalers,
                 'generate_controller_scaling_decisions') as generate:
            reconcile = threading.Thread(
                target=ctrl._reconcile_scale_once,  # pylint: disable=protected-access
                args=(0,))
            reconcile.start()
            assert preload_started.wait(timeout=5)
            release_preload.set()
            reconcile.join(timeout=5)

        assert not reconcile.is_alive()
        generate.assert_not_called()
        ctrl._replica_manager.publish_target_num_replicas.assert_not_called()  # pylint: disable=line-too-long
        assert ctrl._scale_reconcile_coordinator.generation == 1  # pylint: disable=protected-access

    def test_new_demand_generation_discards_blocked_preload(self):
        """A newer LB report cannot be combined with an older fleet read."""
        ctrl = _make_controller()
        decision_autoscaler = mock.Mock()
        decision_autoscaler.latest_version = 2
        decision_autoscaler.reserved_capacity_fill = False
        preload_started = threading.Event()
        release_preload = threading.Event()

        def _blocking_preload(_autoscaler, _replicas):
            preload_started.set()
            assert release_preload.wait(timeout=5)
            return object()

        ctrl._autoscaler = decision_autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [2]}), \
             mock.patch.object(
                 controller.serve_state,
                 'get_scale_planning_state_fingerprint',
                 return_value='stable'), \
             mock.patch.object(
                 autoscalers,
                 'controller_prepares_scaling_decision_inputs',
                 return_value=True), \
             mock.patch.object(
                 autoscalers,
                 'prepare_controller_scaling_decision_inputs',
                 side_effect=_blocking_preload), \
             mock.patch.object(
                 autoscalers,
                 'generate_controller_scaling_decisions') as generate:
            reconcile = threading.Thread(
                target=ctrl._reconcile_scale_once,  # pylint: disable=protected-access
                args=(0,))
            reconcile.start()
            assert preload_started.wait(timeout=5)
            assert ctrl._apply_prepared_load_balancer_report(  # pylint: disable=protected-access
                {'request_aggregator': {
                    'timestamps': []
                }}, {'request_aggregator': {
                    'timestamps': []
                }}, [], {}, (True, True, True), {}, False)
            release_preload.set()
            reconcile.join(timeout=5)

        assert not reconcile.is_alive()
        generate.assert_not_called()
        ctrl._replica_manager.publish_target_num_replicas.assert_not_called()  # pylint: disable=line-too-long

    def test_incomplete_exact_logical_tick_revokes_prior_target(self):
        ctrl = _make_controller()
        decision_autoscaler = mock.Mock(spec=autoscalers.ConcurrencyAutoscaler)
        decision_autoscaler.latest_version = 1
        decision_autoscaler.replica_unit = 'logical'
        decision_autoscaler.reserved_capacity_fill = False
        decision_autoscaler.target_num_replicas_by_accelerator = {}
        decision_autoscaler.has_recomputed_with_fresh_data.return_value = False
        decision_autoscaler.cost_rebalance_state_dirty = False
        decision_autoscaler.unrecoverable_rollout_failure = None
        decision_autoscaler.logical_target_state = None
        decision_autoscaler.configured_accelerator_shapes = {
            'L4': 1,
            'A100': 1,
        }
        decision_autoscaler.generate_scaling_decisions.return_value = []
        decision_autoscaler.get_decision_interval.return_value = 0
        ctrl._autoscaler = decision_autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.wait_for_scale_reconciliation.side_effect = (  # pylint: disable=line-too-long
            StopIteration)

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}):
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        ctrl._replica_manager.invalidate_logical_target.assert_called_once_with(  # pylint: disable=line-too-long
        )
        ctrl._replica_manager.clear_scale_reconciliation_signal.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.wait_for_scale_reconciliation.assert_not_called()  # pylint: disable=line-too-long

    def test_logical_scale_up_forwards_explicit_paid_authority(self):
        ctrl = _make_controller()
        decision_autoscaler = mock.Mock()
        decision_autoscaler.latest_version = 2
        decision_autoscaler.get_decision_interval.return_value = 0
        logical_target = autoscalers.LogicalScaleTarget(
            version=2,
            reconcile_generation=7,
            target_capacity=66,
            target_capacity_by_accelerator=(('L4', 18), ('A100-80GB', 2),
                                            ('H200', 46)),
            accelerator_shapes=(('L4', 1), ('A100-80GB', 1), ('H200', 1)),
            cold_launch_authority_by_accelerator=(('L4', 14),))
        decision_autoscaler.generate_scaling_decisions.return_value = [
            autoscalers.AutoscalerDecision(
                autoscalers.AutoscalerDecisionOperator.SCALE_UP, logical_target)
        ]
        ctrl._autoscaler = decision_autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.wait_for_scale_reconciliation.side_effect = (  # pylint: disable=line-too-long
            StopIteration)

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1, 2]}):
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        ctrl._replica_manager.scale_up_to_logical_capacity.assert_called_once_with(  # pylint: disable=line-too-long
            66,
            2,
            7,
            launch_priority=constants.LB_REQUEST_PRIORITY_MIN,
            cold_launch_authority_by_accelerator={'L4': 14},
            target_capacity_by_accelerator={
                'L4': 18,
                'A100-80GB': 2,
                'H200': 46,
            },
            accelerator_shapes={
                'L4': 1,
                'A100-80GB': 1,
                'H200': 1,
            })

    def test_logical_scale_down_waves_are_batched_without_reordering(self):
        ctrl = _make_controller()
        decision_autoscaler = mock.Mock()
        decision_autoscaler.latest_version = 1
        decision_autoscaler.get_decision_interval.return_value = 0
        decision_autoscaler.current_launch_priority.return_value = 50

        def _logical_down(replica_id, generation=7):
            return autoscalers.AutoscalerDecision(
                autoscalers.AutoscalerDecisionOperator.SCALE_DOWN,
                autoscalers.LogicalScaleDownTarget(
                    version=1,
                    reconcile_generation=generation,
                    target_capacity=4,
                    replica_id=replica_id))

        decision_autoscaler.generate_scaling_decisions.return_value = [
            _logical_down(1),
            _logical_down(2),
            autoscalers.AutoscalerDecision(
                autoscalers.AutoscalerDecisionOperator.SCALE_UP, None),
            _logical_down(3),
            autoscalers.AutoscalerDecision(
                autoscalers.AutoscalerDecisionOperator.SCALE_DOWN, 99),
            _logical_down(4, generation=8),
        ]
        ctrl._autoscaler = decision_autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.wait_for_scale_reconciliation.side_effect = (  # pylint: disable=line-too-long
            StopIteration)

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}):
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        actuation_calls = [
            call for call in ctrl._replica_manager.method_calls  # pylint: disable=protected-access
            if call[0] in ('scale_down_logically_batch', 'scale_up_batch',
                           'scale_down')
        ]
        assert actuation_calls == [
            mock.call.scale_down_logically_batch([1, 2], 4, 1, 7),
            mock.call.scale_up_batch([None],
                                     expected_version=1,
                                     launch_priority=50),
            mock.call.scale_down_logically_batch([3], 4, 1, 7),
            mock.call.scale_down(99, wait_for_idle=False, expected_version=1),
            mock.call.scale_down_logically_batch([4], 4, 1, 8),
        ]
        ctrl._replica_manager.clear_scale_reconciliation_signal.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.wait_for_scale_reconciliation.assert_not_called()  # pylint: disable=line-too-long

    def test_instance_aware_physical_batches_keep_per_card_priority(self):
        ctrl = _make_controller()
        spec = _make_autoscaler_spec(target_qps_per_replica={
            'L4': 1.0,
            'A100': 2.0,
        },)
        decision_autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler(
            'svc', spec, version=1)
        l4_override = {'accelerators': {'L4': 1}}
        a100_override = {'accelerators': {'A100': 1}}
        decision_autoscaler.generate_scaling_decisions = mock.Mock(
            return_value=[
                autoscalers.AutoscalerDecision(
                    autoscalers.AutoscalerDecisionOperator.SCALE_UP,
                    l4_override),
                autoscalers.AutoscalerDecision(
                    autoscalers.AutoscalerDecisionOperator.SCALE_UP,
                    a100_override),
            ])
        decision_autoscaler.current_launch_priority = mock.Mock(return_value=50)
        decision_autoscaler.current_launch_priorities_by_accelerator = (
            mock.Mock(return_value={
                'L4': 20,
                'A100': 50,
            }))
        decision_autoscaler.get_decision_interval = mock.Mock(return_value=0)
        ctrl._autoscaler = decision_autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._replica_manager.wait_for_scale_reconciliation.side_effect = (  # pylint: disable=line-too-long
            StopIteration)

        with mock.patch.object(controller.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(
                 controller.serve_state,
                 'get_service_runtime_snapshot',
                 return_value={'active_versions': [1]}), \
             mock.patch.object(
                 ctrl,
                 '_get_free_reserved_slots_by_accelerator',
                 return_value={}):
            ctrl._reconcile_scale_once(0)  # pylint: disable=protected-access

        assert ctrl._replica_manager.scale_up_batch.call_args_list == [  # pylint: disable=line-too-long
            mock.call([l4_override], expected_version=1, launch_priority=20),
            mock.call([a100_override], expected_version=1, launch_priority=50),
        ]
        decision_autoscaler.current_launch_priorities_by_accelerator.assert_called_once_with(  # pylint: disable=line-too-long
            ['L4', 'A100'])
        ctrl._replica_manager.clear_scale_reconciliation_signal.assert_not_called()  # pylint: disable=line-too-long
        ctrl._replica_manager.wait_for_scale_reconciliation.assert_not_called()  # pylint: disable=line-too-long


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

    def test_sole_selected_legacy_pod_retains_migration_stream_authority(self):
        ctrl = _make_controller()
        ctrl._lb_ha_enabled = True  # pylint: disable=protected-access
        authority = controller.lb_k8s.LbPodAuthority(
            ready_nonterminating_uids={'legacy', 'slot-a', 'slot-b'},
            live_uids={'legacy', 'slot-a', 'slot-b'},
            slot_by_uid={
                'slot-a': controller.lb_ha.LbSlot.A,
                'slot-b': controller.lb_ha.LbSlot.B,
            },
            selected_slot=None,
            legacy_uids={'legacy'})
        state = controller.lb_ha.LbCutoverState(
            enabled=True,
            active_slot=controller.lb_ha.LbSlot.A,
            generation=1,
            pending_slot=None,
            phase=controller.lb_ha.LbCutoverPhase.MIGRATING,
            lifecycle_epoch=9)

        with mock.patch.object(controller.lb_k8s,
                               'get_lb_pod_authority',
                               return_value=authority), \
             mock.patch.object(controller.serve_state,
                               'get_lb_cutover_state',
                               return_value=state):
            assert ctrl._lb_report_authority('legacy') == (True, True, True)
            assert ctrl._lb_report_authority('slot-a') == (True, False, False)

    def test_ha_apply_preserves_selected_legacy_drain_report(self):
        ctrl = _make_controller()
        ctrl._lb_ha_enabled = True  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        request = {
            'lb_session_id': 'legacy',
            'in_flight': {
                'http://replica': 3,
            },
            'routing_urls': ['http://replica'],
            'unknown_in_flight_urls': [],
            'draining_urls': [],
        }

        accepted = ctrl._apply_load_balancer_report(request, [], {},
                                                    (True, False, True), {})

        assert accepted
        ctrl._replica_manager.update_lb_in_flight.assert_called_once_with(
            {'http://replica': 3}, ['http://replica'], [], [], 'legacy')

    def test_malformed_demand_does_not_publish_partial_drain_state(self):
        ctrl = _make_controller()
        ctrl._lb_ha_enabled = False  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._autoscaler = _StatefulDemandAutoscaler()  # pylint: disable=protected-access
        request = {
            'lb_session_id': 'lb-a',
            'request_aggregator': {},
            # A reporter-controlled list cannot be translated as the
            # protocol's url-keyed gauge.
            'in_flight': [],
            'routing_urls': [],
        }

        with pytest.raises(AttributeError):
            ctrl._apply_load_balancer_report(  # pylint: disable=protected-access
                request, [], {}, (True, True, True), {})

        ctrl._replica_manager.update_lb_in_flight.assert_not_called()


class _StatefulDemandAutoscaler:
    """Small stateful collector exposing every LB-controlled demand field."""

    def __init__(self) -> None:
        self.replica_unit = 'physical_backend'
        self.latest_version = 1
        self.max_replicas = 100
        self.target_num_replicas = 1
        self.min_replicas_by_accelerator = {}
        self.target_num_replicas_by_accelerator = {}
        self.warm_retention_target_by_accelerator = {}
        self.cold_launch_authority_by_accelerator = {}
        self.reserved_capacity_fill = False
        self.fill_target = 0
        self.request_timestamps = [101]
        self.in_flight_by_replica_id = {1: 9}
        self.unknown_in_flight_replica_ids = {1}
        self.queue_depth = 7
        self.rejected_in_window = 5
        self.collect_calls = 0
        self.reports = []

    def get_final_target_num_replicas(self):
        return self.target_num_replicas

    def has_recomputed_with_fresh_data(self):
        return True

    def is_replica_on_zero_cost_location(self, _info):
        return False

    def get_ready_replica_capacity(self, info):
        return info.planned_capacity

    def info(self):
        return {
            'target_num_replicas': self.target_num_replicas,
            'in_flight_total': sum(self.in_flight_by_replica_id.values()),
            'queue_depth': self.queue_depth,
        }

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
        self.spot_placer = None
        self.yaml_content = None
        self.report = ({
            'http://trusted:8080': 4
        }, ['http://trusted:8080'], ['http://trusted:8080'],
                       ['http://trusted:8080'], 'trusted-session')
        self.update_calls = 0
        self.logical_snapshot = None
        self.confirmed_bridge_capacities = []

    def update_lb_in_flight(self, in_flight, routing_urls, unknown_urls,
                            draining_urls, lb_session_id) -> None:
        self.update_calls += 1
        self.report = (in_flight, routing_urls, unknown_urls, draining_urls,
                       lb_session_id)

    def snapshot(self):
        return self.report, self.update_calls

    def update_logical_reconcile_snapshot(self, **kwargs) -> None:
        self.logical_snapshot = kwargs

    def confirm_logical_bridge_capacities(self, capacities):
        self.confirmed_bridge_capacities.append(dict(capacities))
        return dict(capacities)


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
            'queued_requests_by_compatibility': [{
                'priority': 50,
                'compatible_accelerators': ['A100'],
                'count': 3,
            }],
            'in_flight': {
                self._URL: 2
            },
            'occupancy_sampled_urls': [],
            'unknown_in_flight_urls': [self._URL],
            'queue_depth': 11,
            'rejected_in_window': 13,
            'rejected_in_recent_window': 8,
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
        report['request_history'] = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': 120,
                'request_count': 1,
            }],
        }
        with mock.patch.object(
                controller.lb_k8s,
                'get_lb_pod_authority',
                return_value=controller.lb_k8s.LbPodAuthority(
                    {'other-service-pod'},
                    {'other-service-pod'})), mock.patch.object(
                        controller.serve_state,
                        'get_replica_infos') as get_replica_infos, \
             mock.patch.object(controller.serve_history,
                               'record_request_activity') as record_history:
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))

        assert response.status_code == 503
        assert response.body == b''
        # Membership is checked before even reading/resolving replica records.
        get_replica_infos.assert_not_called()
        record_history.assert_not_called()

    def test_request_history_uses_process_incarnation_and_service_hash(self):
        ctrl, _, report = self._controller_and_report()
        ctrl._service_hash = 'service-hash'  # pylint: disable=protected-access
        report['request_history_session_id'] = 'a' * 32
        report['request_history'] = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': 120,
                'request_count': 3,
            }],
        }

        with mock.patch.object(controller.serve_history,
                               'record_request_activity') as record_history:
            accepted = ctrl._record_request_history(  # pylint: disable=protected-access
                report)

        assert accepted is True
        record_history.assert_called_once_with(
            'svc',
            'service-hash',
            f"lb-a:{'a' * 32}",
            report['request_history'],
        )

    def test_response_time_history_uses_separate_persistence_contract(self):
        ctrl, _, report = self._controller_and_report()
        ctrl._service_hash = 'service-hash'  # pylint: disable=protected-access
        report['request_history_session_id'] = 'a' * 32
        report['response_time_history'] = {
            'bucket_seconds': 60,
            'histogram_version': 1,
            'buckets': [{
                'bucket_start': 120,
                'status_class_counts': {
                    '2xx': [1] + [0] * 15,
                },
            }],
        }

        with mock.patch.object(controller.serve_history,
                               'record_response_times') as record_history:
            accepted = ctrl._record_response_time_history(  # pylint: disable=protected-access
                report)

        assert accepted is True
        record_history.assert_called_once_with(
            'svc',
            'service-hash',
            f"lb-a:{'a' * 32}",
            report['response_time_history'],
        )

    @pytest.mark.parametrize(
        ('history_error', 'history_accepted'),
        [(RuntimeError('database unavailable'), False),
         (ValueError('malformed snapshot'), True)],
    )
    def test_response_time_history_failure_is_observability_only(
            self, history_error, history_accepted):
        ctrl, _, report = self._controller_and_report()
        ctrl._service_hash = 'service-hash'  # pylint: disable=protected-access
        report['request_history_session_id'] = 'a' * 32
        report['response_time_history'] = {'histogram_version': 1}

        with mock.patch.object(controller.serve_history,
                               'record_response_times',
                               side_effect=history_error):
            accepted = asyncio.run(
                ctrl._persist_response_time_history(  # pylint: disable=protected-access
                    report))

        assert accepted is history_accepted

    def test_prediction_time_history_uses_separate_persistence_contract(self):
        ctrl, _, report = self._controller_and_report()
        ctrl._service_hash = 'service-hash'  # pylint: disable=protected-access
        report['request_history_session_id'] = 'a' * 32
        report['prediction_time_history'] = {
            'bucket_seconds': 60,
            'histogram_version': 1,
            'buckets': [{
                'bucket_start': 120,
                'outcome_counts': {
                    'succeeded': [1] + [0] * 15,
                },
            }],
        }

        with mock.patch.object(controller.serve_history,
                               'record_prediction_times') as record_history:
            accepted = ctrl._record_prediction_time_history(  # pylint: disable=protected-access
                report)

        assert accepted is True
        record_history.assert_called_once_with(
            'svc',
            'service-hash',
            f"lb-a:{'a' * 32}",
            report['prediction_time_history'],
        )

    def test_free_reserved_slots_never_resolves_provider_costs(self):
        ctrl = _make_controller()
        location = types.SimpleNamespace(accelerators={'A100': 1})
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [location]
        ctrl._replica_manager = types.SimpleNamespace(  # pylint: disable=protected-access
            spot_placer=placer)

        with mock.patch.object(controller.reserved_capacity,
                               'zero_cost_pool_shapes',
                               return_value={}), mock.patch.object(
                                   controller.reserved_capacity,
                                   'get_cached_free_gpus_by_pool',
                                   return_value={}):
            assert not ctrl._get_free_reserved_slots_by_accelerator(  # pylint: disable=protected-access
            )

        placer.zero_cost_locations.assert_called_once_with()

    @pytest.mark.parametrize(
        ('history_error', 'history_accepted'),
        [(RuntimeError('database unavailable'), False),
         (ValueError('malformed snapshot'), True)],
    )
    def test_prediction_time_history_failure_is_observability_only(
            self, history_error, history_accepted):
        ctrl, _, report = self._controller_and_report()
        ctrl._service_hash = 'service-hash'  # pylint: disable=protected-access
        report['request_history_session_id'] = 'a' * 32
        report['prediction_time_history'] = {'histogram_version': 1}

        with mock.patch.object(controller.serve_history,
                               'record_prediction_times',
                               side_effect=history_error):
            accepted = asyncio.run(
                ctrl._persist_prediction_time_history(  # pylint: disable=protected-access
                    report))

        assert accepted is history_accepted

    def test_autoscaler_history_distinguishes_demand_and_fill_targets(self):
        ctrl, _, _ = self._controller_and_report()
        ctrl._service_hash = 'service-hash'  # pylint: disable=protected-access
        ctrl._history_session_id = 'c' * 32  # pylint: disable=protected-access
        ctrl._applied_version = 3  # pylint: disable=protected-access
        autoscaler = mock.Mock()
        autoscaler.get_final_target_num_replicas.return_value = 7
        autoscaler.info.return_value = {
            'fill_target': 12,
            'in_flight_total': 5,
            'queue_depth': 4,
        }
        ctrl._autoscaler = autoscaler  # pylint: disable=protected-access
        replica_counts = {
            'replica_unit': 'physical_backend',
            'ready_replicas': 9,
            'total_replicas': 14,
        }
        capacity_hint = {'provisioning_replicas': 3}

        with mock.patch.object(controller.serve_history,
                               'record_autoscaler_snapshot',
                               return_value=1) as record_history:
            written = ctrl._record_autoscaler_history(  # pylint: disable=protected-access
                replica_counts, capacity_hint)

        assert written == 1
        record_history.assert_called_once_with(
            'svc',
            'service-hash',
            'c' * 32,
            version=3,
            replica_unit='physical_backend',
            demand_target=7,
            capacity_target=12,
            ready_capacity=9,
            provisioning_capacity=3,
            total_capacity=14,
            peak_in_flight=5,
            peak_queue_depth=4,
            accelerator_breakdown=None,
            timestamp=None,
        )

    def test_autoscaler_history_keeps_exact_card_capacity_distinct(self):
        ctrl, _, _ = self._controller_and_report()
        autoscaler = mock.Mock()
        autoscaler.configured_accelerator_shapes = {
            'A100': 1,
            'A100-80GB': 1,
        }
        autoscaler.has_recomputed_with_fresh_data.return_value = True
        autoscaler.target_num_replicas = 3
        autoscaler.target_num_replicas_by_accelerator = {
            'A100': 1,
            'A100-80GB': 2,
        }
        autoscaler.min_replicas_by_accelerator = {'A100-80GB': 1}
        autoscaler.warm_retention_target_by_accelerator = {'A100': 1}
        autoscaler.cold_launch_authority_by_accelerator = {'A100-80GB': 1}
        ctrl._autoscaler = autoscaler  # pylint: disable=protected-access

        breakdown = ctrl._get_accelerator_history_breakdown(  # pylint: disable=protected-access
            {
                'ready_replicas_by_accelerator': {
                    'A100': 1,
                    'A100-80GB': 1,
                },
                'provisioning_replicas_by_accelerator': {'A100-80GB': 1},
                'total_replicas_by_accelerator': {
                    'A100': 1,
                    'A100-80GB': 2,
                },
                'zero_cost_ready_replicas_by_accelerator': {'A100': 1},
                'fill_target_by_accelerator': {'A100': 1},
                'free_reserved_slots_by_accelerator': {'A100': 2},
            }, 1)

        assert breakdown == {
            'capacity_semantics_version': 2,
            'configured_accelerators': ['A100', 'A100-80GB'],
            'min_replicas': {
                'A100-80GB': 1
            },
            'demand_target': {
                'A100': 1,
                'A100-80GB': 2
            },
            'warm_retention_target': {
                'A100': 1
            },
            'cold_launch_authority': {
                'A100-80GB': 1
            },
            'ready_capacity': {
                'A100': 1,
                'A100-80GB': 1
            },
            'provisioning_capacity': {
                'A100-80GB': 1
            },
            'total_capacity': {
                'A100': 1,
                'A100-80GB': 2
            },
            'zero_cost_ready_capacity': {
                'A100': 1
            },
            'fill_target': {
                'A100': 1
            },
            'free_reserved_slots': {
                'A100': 2
            },
        }

    def test_exact_fill_overlay_withholds_unattributed_stale_grant(self):
        ctrl, _, _ = self._controller_and_report()
        autoscaler = mock.Mock()
        autoscaler.reserved_capacity_fill = True
        autoscaler.fill_target = 3
        autoscaler.target_num_replicas_by_accelerator = {
            'A100': 0,
            'A100-80GB': 0,
        }
        ctrl._autoscaler = autoscaler  # pylint: disable=protected-access

        fill = ctrl._get_fill_target_by_accelerator(  # pylint: disable=protected-access
            {'A100-80GB': 1}, {'A100-80GB': 1})

        assert not fill

    def test_history_withholds_exact_cards_for_unattributed_fill(self):
        ctrl, _, _ = self._controller_and_report()
        autoscaler = mock.Mock()
        autoscaler.configured_accelerator_shapes = {
            'A100': 1,
            'A100-80GB': 1,
        }
        autoscaler.has_recomputed_with_fresh_data.return_value = True
        autoscaler.target_num_replicas = 2
        autoscaler.target_num_replicas_by_accelerator = {
            'A100': 1,
            'A100-80GB': 1,
        }
        autoscaler.min_replicas_by_accelerator = {}
        ctrl._autoscaler = autoscaler  # pylint: disable=protected-access

        breakdown = ctrl._get_accelerator_history_breakdown(  # pylint: disable=protected-access
            {
                'ready_replicas_by_accelerator': {
                    'A100': 1,
                    'A100-80GB': 1,
                },
                'total_replicas_by_accelerator': {
                    'A100': 1,
                    'A100-80GB': 1,
                },
            }, 1)

        assert breakdown is None

    @pytest.mark.parametrize('session_id', [None, '', 'not-a-uuid', 'G' * 32])
    def test_request_history_rejects_invalid_process_session(self, session_id):
        ctrl, _, report = self._controller_and_report()
        ctrl._service_hash = 'service-hash'  # pylint: disable=protected-access
        report['request_history_session_id'] = session_id
        report['request_history'] = {
            'bucket_seconds': 60,
            'buckets': [],
        }

        with pytest.raises(ValueError, match='reporter session'):
            ctrl._record_request_history(  # pylint: disable=protected-access
                report)

    @pytest.mark.parametrize(
        ('history_error', 'history_accepted'),
        [(RuntimeError('database unavailable'), False),
         (ValueError('malformed snapshot'), True)],
    )
    def test_request_history_failure_does_not_fail_routing_sync(
            self, history_error, history_accepted):
        ctrl, info, report = self._controller_and_report()
        ctrl._service_hash = 'service-hash'  # pylint: disable=protected-access
        ctrl._routing_spec = {'policy': 'round_robin'}  # pylint: disable=protected-access
        report['request_history_session_id'] = 'a' * 32
        report['request_history'] = {
            'bucket_seconds': 60,
            'buckets': [{
                'bucket_start': 120,
                'request_count': 3,
            }],
        }

        with mock.patch.object(
                ctrl, '_owns_current_service', return_value=True), \
             mock.patch.object(
                 controller.lb_k8s,
                 'get_lb_pod_authority',
                 return_value=controller.lb_k8s.LbPodAuthority(
                     {'lb-a'}, {'lb-a'})), \
             mock.patch.object(
                 ctrl,
                 '_snapshot_replica_occupancy',
                 return_value=([info], {
                     1: True
                 }, set())), \
             mock.patch.object(
                 ctrl, '_get_lb_replica_info', return_value=({}, 1)), \
             mock.patch.object(
                 ctrl, '_get_capacity_hint', return_value={}), \
             mock.patch.object(
                 ctrl,
                 '_record_request_history',
                 side_effect=history_error):
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))

        assert response.status_code == 200
        assert (json.loads(response.body)['request_history_accepted']
                is history_accepted)

    def test_provider_phase_timeout_aborts_lb_sync_before_publication(self):
        ctrl, info, report = self._controller_and_report()
        ctrl._service_hash = 'service-hash'  # pylint: disable=protected-access
        persist_history = mock.Mock()

        with mock.patch.object(
                ctrl, '_owns_current_service', return_value=True), \
             mock.patch.object(
                 controller.lb_k8s,
                 'get_lb_pod_authority',
                 return_value=controller.lb_k8s.LbPodAuthority(
                     {'lb-a'}, {'lb-a'})), \
             mock.patch.object(
                 ctrl,
                 '_snapshot_replica_occupancy',
                 return_value=([info], {
                     1: True
                 }, set())), \
             mock.patch.object(
                 ctrl,
                 '_get_lb_replica_info',
                 side_effect=exceptions.ProviderPhaseTimeoutError('busy')), \
             mock.patch.object(ctrl,
                               '_persist_request_histories', persist_history):
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))

        assert response.status_code == 503
        persist_history.assert_not_called()

    def test_v1_classification_failure_retains_both_history_snapshots(self):
        ctrl, info, report = self._controller_and_report()
        ctrl._service_hash = 'service-hash'  # pylint: disable=protected-access
        ctrl._routing_spec = {'policy': 'round_robin'}  # pylint: disable=protected-access
        report.update({
            'request_history_session_id': 'a' * 32,
            'request_history': {
                'bucket_seconds': 60,
                'buckets': [{
                    'bucket_start': 120,
                    'request_count': 3,
                }],
            },
            'request_classification_history': {
                'classification_version': 1,
                'bucket_seconds': 60,
                'buckets': [],
            },
        })

        with mock.patch.object(
                ctrl, '_owns_current_service', return_value=True), \
             mock.patch.object(
                 controller.lb_k8s,
                 'get_lb_pod_authority',
                 return_value=controller.lb_k8s.LbPodAuthority(
                     {'lb-a'}, {'lb-a'})), \
             mock.patch.object(
                 ctrl,
                 '_snapshot_replica_occupancy',
                 return_value=([info], {
                     1: True
                 }, set())), \
             mock.patch.object(
                 ctrl, '_get_lb_replica_info', return_value=({}, 1)), \
             mock.patch.object(
                 ctrl, '_get_capacity_hint', return_value={}), \
             mock.patch.object(
                 ctrl, '_record_request_history', return_value=True), \
             mock.patch.object(
                 ctrl,
                 '_record_request_classification_history',
                 side_effect=RuntimeError('database unavailable')):
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))

        assert response.status_code == 200
        body = json.loads(response.body)
        assert body['request_classification_history_accepted'] is False
        assert body['request_history_accepted'] is False

    def test_sync_with_mid_snapshot_update_withholds_mixed_routing_epoch(self):
        ctrl, info, report = self._controller_and_report()
        ctrl._routing_spec = {'catalog': ['A100']}  # pylint: disable=protected-access

        def _finish_replica_snapshot(*_args, **_kwargs):
            with ctrl._routing_state_lock:  # pylint: disable=protected-access
                ctrl._routing_spec = {'catalog': ['H100']}  # pylint: disable=protected-access
                ctrl._applied_version = 2  # pylint: disable=protected-access
            return {}, 1

        with mock.patch.object(
                ctrl, '_owns_current_service', return_value=True), \
             mock.patch.object(
                 ctrl, '_lb_report_authority', return_value=(True, True, True)), \
             mock.patch.object(
                 ctrl,
                 '_snapshot_replica_occupancy',
                 return_value=([info], {
                     1: True
                 }, set())), \
             mock.patch.object(
                 ctrl,
                 '_get_lb_replica_info',
                 side_effect=_finish_replica_snapshot), \
             mock.patch.object(
                 ctrl, '_get_capacity_hint', return_value={}):
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))

        body = json.loads(response.body)
        assert body['service_version'] == 2
        assert body['routing_spec'] is None

    def test_live_max_surge_reporter_persists_request_history(self):
        ctrl, info, report = self._controller_and_report()
        ctrl._routing_spec = {'policy': 'round_robin'}  # pylint: disable=protected-access
        record_history = mock.Mock(return_value=True)
        ctrl._record_request_history = record_history  # pylint: disable=protected-access

        with mock.patch.object(
                ctrl, '_owns_current_service', return_value=True), \
             mock.patch.object(
                 controller.lb_k8s,
                 'get_lb_pod_authority',
                 return_value=controller.lb_k8s.LbPodAuthority(
                     {'lb-a', 'lb-b'}, {'lb-a', 'lb-b'})), \
             mock.patch.object(
                 ctrl,
                 '_snapshot_replica_occupancy',
                 return_value=([info], {
                     1: True
                 }, set())), \
             mock.patch.object(
                 ctrl, '_get_lb_replica_info', return_value=({}, 1)), \
             mock.patch.object(
                 ctrl, '_get_capacity_hint', return_value={}):
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))

        assert response.status_code == 200
        assert json.loads(response.body)['request_history_accepted'] is True
        record_history.assert_called_once_with(report)

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

    def test_old_report_cannot_cross_catalog_publication_epoch(self):
        ctrl, info, report = self._controller_and_report()
        report.update({
            'routing_version': 1,
            'rejected_requests_by_compatibility': [],
        })
        reached_epoch_fence = threading.Event()
        errors = []

        def _unknown_ids(*_args, **_kwargs):
            reached_epoch_fence.set()
            return {1}

        def _ingest():
            try:
                ctrl._apply_load_balancer_report(  # pylint: disable=protected-access
                    report, [info], {1: True}, (True, True, True), {})
            except BaseException as exc:  # pylint: disable=broad-except
                errors.append(exc)

        # Hold the same publication lock used by _apply_service_update. The
        # old report reaches the fence under version 1, but cannot validate or
        # collect until the version-2 catalog epoch is visible.
        with mock.patch.object(ctrl,
                               '_unknown_async_replica_ids',
                               side_effect=_unknown_ids):
            with ctrl._routing_state_lock:  # pylint: disable=protected-access
                reporter = threading.Thread(target=_ingest)
                reporter.start()
                assert reached_epoch_fence.wait(timeout=5)
                ctrl._applied_version = 2  # pylint: disable=protected-access
            reporter.join(timeout=5)

        assert not reporter.is_alive()
        assert not errors
        assert ctrl._autoscaler.reports[-1][  # pylint: disable=protected-access
            'compatibility_demand_complete'] is False

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

    def test_logical_report_adopts_bridge_width_only_after_bounded_lb_proof(
            self):
        ctrl, info, report = self._controller_and_report()
        ctrl._autoscaler.replica_unit = 'logical'  # pylint: disable=protected-access
        ctrl._autoscaler.latest_version = 2  # pylint: disable=protected-access
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: (self._URL, 'L4', 8)
        }
        report['total_slots_by_url'] = {self._URL: 64}
        report['occupancy_sampled_urls'] = [self._URL]
        report['unknown_in_flight_urls'] = []

        accepted = asyncio.run(
            ctrl._ingest_load_balancer_report(  # pylint: disable=protected-access,too-many-function-args
                report, [info], {1: True}, (True, True, True), {2}))

        assert accepted is True
        # The local router's report is independently capped by the launched
        # shape before it can affect durable or autoscaler state.
        assert ctrl._replica_manager.confirmed_bridge_capacities == [{1: 8}]  # pylint: disable=protected-access
        assert info.planned_capacity == 8
        assert info.logical_bridge_capacity_verified is True
        assert ctrl._autoscaler.reports[-1][  # pylint: disable=protected-access
            'observed_slots_by_replica_id'] == {
                1: 8
            }

        # The durable proof suppresses persistence work on later heartbeats.
        accepted = asyncio.run(
            ctrl._ingest_load_balancer_report(  # pylint: disable=protected-access,too-many-function-args
                report, [info], {1: True}, (True, True, True), {2}))
        assert accepted is True
        assert ctrl._replica_manager.confirmed_bridge_capacities == [{1: 8}]  # pylint: disable=protected-access

    def test_sync_confirms_empty_logical_version_bridge_with_hardware_cap(self):
        ctrl, info, report = self._controller_and_report()
        ctrl._autoscaler.replica_unit = 'logical'  # pylint: disable=protected-access
        ctrl._autoscaler.latest_version = 2  # pylint: disable=protected-access
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: (self._URL, 'L4', 8)
        }
        report['total_slots_by_url'] = {self._URL: 64}
        report['occupancy_sampled_urls'] = [self._URL]
        report['unknown_in_flight_urls'] = []

        with mock.patch.object(
                ctrl, '_owns_current_service', return_value=True), \
             mock.patch.object(
                 ctrl,
                 '_lb_report_authority',
                 return_value=(True, True, True)), \
             mock.patch.object(
                 ctrl,
                 '_snapshot_replica_occupancy',
                 return_value=([info], {
                     1: True
                 }, set())), \
             mock.patch.object(
                 ctrl, '_get_lb_replica_info', return_value=({}, 1)), \
             mock.patch.object(
                 ctrl,
                 '_persist_request_history',
                 new=mock.AsyncMock(return_value=True)), \
             mock.patch.object(
                 ctrl, '_get_capacity_hint', return_value={}):
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))

        assert response.status_code == 200
        assert ctrl._replica_manager.confirmed_bridge_capacities == [{1: 8}]  # pylint: disable=protected-access
        assert info.planned_capacity == 8
        assert info.logical_bridge_capacity_verified is True
        assert ctrl._autoscaler.reports[-1][  # pylint: disable=protected-access
            'observed_slots_by_replica_id'] == {
                1: 8
            }

    @pytest.mark.parametrize('cached', [
        None,
        ('http://1.1.1.1:8080', 'unknown', 8),
        ('http://1.1.1.1:8080', 'L4', 0),
    ])
    def test_unresolved_bridge_hardware_clamps_to_durable_width(self, cached):
        ctrl, info, _ = self._controller_and_report()
        ctrl._autoscaler.replica_unit = 'logical'  # pylint: disable=protected-access
        ctrl._lb_translation_cache = ({  # pylint: disable=protected-access
            1: cached
        } if cached is not None else {})
        observed_slots = {1: 64}

        candidates = ctrl._logical_bridge_capacity_candidates(  # pylint: disable=protected-access
            [info], set(), observed_slots)

        assert not candidates
        assert observed_slots == {1: 1}

    @pytest.mark.parametrize(('reported', 'expected'), [(64, 4), (2, 2)])
    def test_unresolved_bridge_hardware_caps_at_verified_durable_width(
            self, reported, expected):
        ctrl, info, _ = self._controller_and_report()
        ctrl._autoscaler.replica_unit = 'logical'  # pylint: disable=protected-access
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: (self._URL, 'unknown', 8)
        }
        info.planned_capacity = 4
        info.logical_bridge_capacity_verified = True
        observed_slots = {1: reported}

        candidates = ctrl._logical_bridge_capacity_candidates(  # pylint: disable=protected-access
            [info], set(), observed_slots)

        assert not candidates
        assert observed_slots == {1: expected}

    @pytest.mark.parametrize(
        ('planned', 'verified', 'reported', 'expected'),
        [(1, False, 64, 1), (4, True, 64, 4), (4, True, 2, 2)],
    )
    def test_sync_bounds_unknown_bridge_hardware_before_autoscaling(
            self, planned, verified, reported, expected):
        ctrl, info, report = self._controller_and_report()
        ctrl._autoscaler.replica_unit = 'logical'  # pylint: disable=protected-access
        ctrl._autoscaler.latest_version = 2  # pylint: disable=protected-access
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: (self._URL, 'unknown', 8)
        }
        info.planned_capacity = planned
        info.logical_bridge_capacity_verified = verified
        report['total_slots_by_url'] = {self._URL: reported}
        report['occupancy_sampled_urls'] = [self._URL]
        report['unknown_in_flight_urls'] = []

        with mock.patch.object(
                ctrl, '_owns_current_service', return_value=True), \
             mock.patch.object(
                 ctrl,
                 '_lb_report_authority',
                 return_value=(True, True, True)), \
             mock.patch.object(
                 ctrl,
                 '_snapshot_replica_occupancy',
                 return_value=([info], {
                     1: True
                 }, set())), \
             mock.patch.object(
                 ctrl, '_get_lb_replica_info', return_value=({}, 1)), \
             mock.patch.object(
                 ctrl,
                 '_persist_request_history',
                 new=mock.AsyncMock(return_value=True)), \
             mock.patch.object(
                 ctrl, '_get_capacity_hint', return_value={}):
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))

        assert response.status_code == 200
        assert ctrl._replica_manager.confirmed_bridge_capacities == []  # pylint: disable=protected-access
        assert info.planned_capacity == planned
        assert info.logical_bridge_capacity_verified is verified
        assert ctrl._autoscaler.reports[-1][  # pylint: disable=protected-access
            'observed_slots_by_replica_id'] == {
                1: expected
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
        assert ctrl._autoscaler.reports[-1][  # pylint: disable=protected-access
            'queued_requests_by_compatibility'] == [{
                'priority': 50,
                'compatible_accelerators': ['A100'],
                'count': 3,
            }]
        # An old LB omitting the new rejection-profile gauge must not unlock
        # card-specific target changes during a mixed-version rollout.
        assert ctrl._autoscaler.reports[-1][  # pylint: disable=protected-access
            'compatibility_demand_complete'] is False
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

        async def _capture_confirm(replica_infos, logical_versions,
                                   observed_slots):
            del replica_infos, observed_slots
            observed['ingest_logical_versions'] = set(logical_versions)

        def _capture_ingest(request_data, effective_request_data, replica_infos,
                            async_occupancy_by_version, authority,
                            observed_slots, ha_enabled):
            del (request_data, effective_request_data, replica_infos, authority,
                 observed_slots, ha_enabled)
            observed['ingest'] = dict(async_occupancy_by_version)
            return True

        ctrl._get_lb_replica_info = _capture_lb_replica_info  # pylint: disable=protected-access
        ctrl._confirm_logical_bridge_capacities = _capture_confirm  # pylint: disable=protected-access
        ctrl._apply_prepared_load_balancer_report = _capture_ingest  # pylint: disable=protected-access

        def _capture_capacity_hint(replica_infos,
                                   logical_versions,
                                   replica_counts=None):
            observed['hint_replica_counts'] = replica_counts
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
                         graceful_drain_async_occupancy=False,
                         uses_logical_replicas=False),
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
        assert observed['ingest_logical_versions'] == {3}
        assert observed['logical_versions'] == {3}
        assert observed['sync_thread'] != event_loop_thread
        # The sync handler must hand the hint the same counts dict it
        # snapshotted, so the fleet is aggregated exactly once per sync.
        assert observed['hint_replica_counts'] is ctrl._replica_counts_snapshot  # pylint: disable=protected-access
        assert observed['hint_replica_counts'] is not None


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
        self.min_replicas_by_accelerator = {}
        self.target_num_replicas_by_accelerator = {}
        self.warm_retention_target_by_accelerator = {}
        self.cold_launch_authority_by_accelerator = {}
        self.reserved_capacity_fill = False
        self.fill_target = 0

    def get_final_target_num_replicas(self) -> int:
        return self._target

    def has_recomputed_with_fresh_data(self) -> bool:
        return self._recomputed

    def is_replica_on_zero_cost_location(self, _info) -> bool:
        return False

    def get_ready_replica_capacity(self, info) -> int:
        return info.planned_capacity


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
            'ready_replicas': 2,
            'total_replicas': 5,
            'failed_replicas': 0,
            'physical_ready_replicas': 2,
            'physical_total_replicas': 5,
            'physical_failed_replicas': 0,
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
            'ready_replicas': 2,
            'total_replicas': 5,
            'failed_replicas': 0,
            'physical_ready_replicas': 2,
            'physical_total_replicas': 5,
            'physical_failed_replicas': 0,
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

    def test_stale_target_floor_excludes_reserved_fill(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=1,
            recomputed=False,
            latest_version=2)
        replicas = self._replicas()
        replicas[1].reserved_fill = True

        hint = ctrl._get_capacity_hint(  # pylint: disable=protected-access
            replicas,
            logical_versions=set())

        # READY replica 1 and STARTING replica 3 are demand-owned. The
        # PROVISIONING fill row remains visible as capacity on the way, but it
        # cannot raise the rebuilt-blind demand target.
        assert hint['target_num_replicas'] == 2
        assert hint['provisioning_replicas'] == 2
        assert hint['total_replicas'] == 5

    def test_capacity_hint_reuses_precomputed_replica_counts(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=5,
            recomputed=True,
            latest_version=2)
        replicas = self._replicas()
        expected = ctrl._get_replica_counts(replicas)  # pylint: disable=protected-access
        with mock.patch.object(ctrl, '_get_replica_counts') as counts_mock:
            hint = ctrl._get_capacity_hint(  # pylint: disable=protected-access
                replicas,
                logical_versions=set(),
                replica_counts=dict(expected))
        counts_mock.assert_not_called()
        assert hint['replica_unit'] == 'physical_backend'
        for key, value in expected.items():
            if key != 'replica_unit':
                assert hint[key] == value

    def test_capacity_hint_computes_counts_once_when_not_provided(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=5,
            recomputed=True,
            latest_version=2)
        replicas = self._replicas()
        real_counts = ctrl._get_replica_counts  # pylint: disable=protected-access
        with mock.patch.object(ctrl,
                               '_get_replica_counts',
                               side_effect=real_counts) as counts_mock:
            hint = ctrl._get_capacity_hint(  # pylint: disable=protected-access
                replicas,
                logical_versions=set())
        assert counts_mock.call_count == 1
        assert hint['total_replicas'] == 5

    def test_exact_card_committed_unready_membership_is_exhaustive(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=4,
            recomputed=True,
            latest_version=2)
        replicas = [
            _FakeReplicaInfo(replica_id, status, version=2)
            for replica_id, status in enumerate(serve_state.ReplicaStatus,
                                                start=1)
        ]
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            info.replica_id: (f'http://{info.replica_id}', info.status.value, 1)
            for info in replicas
        }
        committed_unready_statuses = {
            serve_state.ReplicaStatus.PENDING,
            serve_state.ReplicaStatus.PROVISIONING,
            serve_state.ReplicaStatus.STARTING,
            serve_state.ReplicaStatus.NOT_READY,
        }
        assert set(
            serve_state.ReplicaStatus) == {info.status for info in replicas}

        counts = ctrl._get_replica_counts(  # pylint: disable=protected-access
            replicas)

        assert counts['provisioning_replicas_by_accelerator'] == {
            status.value: 1 for status in committed_unready_statuses
        }

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
            # One READY x8 logical backend plus the old physical bridge row.
            'ready_replicas': 9,
            'total_replicas': 15,
            'failed_replicas': 0,
            'physical_ready_replicas': 2,
            'physical_total_replicas': 5,
            'physical_failed_replicas': 0,
            'ready_replicas_by_accelerator': {
                'A100': 8,
            },
            'provisioning_replicas_by_accelerator': {
                'A100': 5,
            },
            'total_replicas_by_accelerator': {
                'A100': 13,
            },
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
        assert hint['ready_replicas'] == 9
        assert hint['physical_ready_replicas'] == 2

    def test_stale_logical_target_floor_excludes_fill_width(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=1,
            recomputed=False,
            latest_version=2,
            replica_unit='logical')
        replicas = self._replicas()
        replicas[0].planned_capacity = 8
        replicas[1].planned_capacity = 4
        replicas[1].reserved_fill = True
        replicas[2].planned_capacity = 1
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: ('http://eight', 'A100', 8),
            2: ('http://four', 'A100', 4),
            3: ('http://one', 'A100', 1),
        }

        hint = ctrl._get_capacity_hint(  # pylint: disable=protected-access
            replicas, logical_versions={2})

        assert hint['target_num_replicas'] == 9
        assert hint['provisioning_replicas'] == 5
        assert hint['total_replicas'] == 15
        assert hint['planned_capacity_by_url'] == {
            'http://eight': 8,
            'http://four': 4,
            'http://one': 1,
        }

    def test_logical_hint_includes_lb_verified_physical_bridge(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=8,
            recomputed=True,
            latest_version=2,
            replica_unit='logical')
        physical = _FakeReplicaInfo(1,
                                    serve_state.ReplicaStatus.READY,
                                    version=1)
        physical.planned_capacity = 8
        physical.logical_bridge_capacity_verified = True
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: ('http://physical', 'L4', 8),
        }

        hint = ctrl._get_capacity_hint(  # pylint: disable=protected-access
            [physical],
            logical_versions=set())

        assert hint['planned_capacity_by_url'] == {'http://physical': 8}
        assert hint['logical_replica_urls'] == ['http://physical']
        assert hint['ready_replicas'] == 8
        assert hint['physical_ready_replicas'] == 1

    def test_logical_counts_preserve_capacity_and_physical_failures(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=4,
            recomputed=True,
            latest_version=2,
            replica_unit='logical')
        ready = _FakeReplicaInfo(1, serve_state.ReplicaStatus.READY, version=2)
        ready.planned_capacity = 4
        failed = _FakeReplicaInfo(2,
                                  serve_state.ReplicaStatus.FAILED_PROVISION,
                                  version=2)
        failed.planned_capacity = 8
        bridge = _FakeReplicaInfo(3, serve_state.ReplicaStatus.READY, version=1)

        counts = ctrl._get_replica_counts(  # pylint: disable=protected-access
            [ready, failed, bridge])

        assert counts == {
            'replica_unit': 'logical_slot',
            'ready_replicas': 5,
            'total_replicas': 5,
            'failed_replicas': 8,
            'physical_ready_replicas': 2,
            'physical_total_replicas': 2,
            'physical_failed_replicas': 1,
        }

    def test_logical_ready_count_uses_observed_router_capacity(self):
        ctrl = _make_controller()
        autoscaler = _FakeAutoscaler(target=8,
                                     recomputed=True,
                                     latest_version=2,
                                     replica_unit='logical')
        autoscaler.get_ready_replica_capacity = lambda info: (
            7 if info.replica_id == 1 else 1)
        ctrl._autoscaler = autoscaler  # pylint: disable=protected-access
        logical = _FakeReplicaInfo(1,
                                   serve_state.ReplicaStatus.READY,
                                   version=2)
        logical.planned_capacity = 8
        bridge = _FakeReplicaInfo(2, serve_state.ReplicaStatus.READY, version=1)

        counts = ctrl._get_replica_counts(  # pylint: disable=protected-access
            [logical, bridge])

        assert counts['ready_replicas'] == 8
        assert counts['total_replicas'] == 9
        assert counts['physical_ready_replicas'] == 2

    def test_zero_cost_exact_cards_include_legacy_location_match(self):
        ctrl = _make_controller()
        autoscaler = _FakeAutoscaler(target=3,
                                     recomputed=True,
                                     latest_version=2)
        autoscaler.is_replica_on_zero_cost_location = (
            lambda info: info.replica_id == 1)
        ctrl._autoscaler = autoscaler  # pylint: disable=protected-access
        legacy = _FakeReplicaInfo(1, serve_state.ReplicaStatus.READY, version=2)
        legacy.is_zero_cost = False
        persisted = _FakeReplicaInfo(2,
                                     serve_state.ReplicaStatus.READY,
                                     version=2)
        persisted.is_zero_cost = True
        unknown = _FakeReplicaInfo(3,
                                   serve_state.ReplicaStatus.READY,
                                   version=2)
        unknown.is_zero_cost = False
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: ('http://legacy', 'A100-80GB', 1),
            2: ('http://persisted', 'A100', 1),
            3: ('http://unknown', 'H100', 1),
        }

        counts = ctrl._get_replica_counts(  # pylint: disable=protected-access
            [legacy, persisted, unknown])

        assert counts['zero_cost_ready_replicas_by_accelerator'] == {
            'A100-80GB': 1,
            'A100': 1,
        }
        assert counts['zero_cost_total_replicas_by_accelerator'] == {
            'A100-80GB': 1,
            'A100': 1,
        }


class TestReservedCapacityPollerStart:
    """Poller lifecycle: seeded, idempotent, inert without a placer."""

    def _controller_with(self, placer):
        ctrl = _make_controller()
        ctrl._replica_manager = mock.Mock()
        ctrl._replica_manager.spot_placer = placer
        ctrl._autoscaler = mock.Mock()
        ctrl._service_hash = None
        ctrl._controller_owner = None
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
                               'start_supervised_thread') as start_mock, \
             mock.patch.object(controller.reserved_capacity,
                               'poller_loop') as poller_loop:
            ctrl._start_reserved_capacity_poller_if_needed()
            ctrl._start_reserved_capacity_poller_if_needed()
            assert start_mock.call_count == 1
            names = [call.args[1] for call in start_mock.call_args_list]
            assert names == ['reserved-capacity-poller']
            start_mock.call_args_list[0].args[0]()
            poller_loop.assert_called_once()
            poller_kwargs = poller_loop.call_args.kwargs
            assert (poller_kwargs['actuation_epoch_lock']
                    is ctrl._get_actuation_epoch_lock())
            generation = poller_kwargs['get_actuation_generation']()
            assert generation == ctrl.get_actuation_generation()
            assert poller_kwargs['actuation_generation_is_current'](generation)
            reconcile_generation = ctrl._scale_reconcile_coordinator.generation
            poller_kwargs['notify_reconcile']()
            assert (ctrl._scale_reconcile_coordinator.generation ==
                    reconcile_generation + 1)
            assert 'wake_event' not in poller_kwargs
            poller_ambiguity_callback = (poller_kwargs['on_ambiguous_boundary'])
            assert poller_ambiguity_callback.__self__ is ctrl
            assert (poller_ambiguity_callback.__func__
                    is ctrl._handle_reclaim_proof_boundary_ambiguity.__func__)

    def test_without_placer_is_inert(self):
        ctrl = self._controller_with(placer=None)
        with mock.patch.object(controller.thread_utils,
                               'start_supervised_thread') as start_mock:
            ctrl._start_reserved_capacity_poller_if_needed()
        start_mock.assert_not_called()
        # Not marked started: a later update that adds a placer (new
        # service version) may still start it.
        assert ctrl._reserved_capacity_poller_started is False

    def test_ambiguous_reclaim_boundary_fences_and_restarts_controller(self):
        ctrl = self._controller_with(placer=mock.Mock())
        ctrl._fence_actuation_for_update_recovery = mock.Mock()
        ctrl._schedule_supervised_recovery = mock.Mock()
        error = controller.request_process.AmbiguousBoundaryError(
            'unproven provider family')

        ctrl._handle_reclaim_proof_boundary_ambiguity(error)

        ctrl._fence_actuation_for_update_recovery.assert_called_once_with()
        ctrl._schedule_supervised_recovery.assert_called_once_with()


class TestSeedFillZeroCostLocations:
    """The constructor-time seed is best-effort, never fatal."""

    def test_seed_failure_does_not_propagate(self):
        # A malformed/corrupt catalog view must not crash-loop the controller;
        # later reconciliation can still surface and repair the version.
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

    On a large replica table, replica/spec/ownership reads and the cached
    reserved-capacity observation read are blocking calls. Running them on the
    FastAPI event loop stalls the controller liveness and ownership probes
    served by the same loop (the same invariant that already keeps
    `_lb_report_authority` and `_get_lb_replica_info` in the executor).
    """

    def _run_sync(self):
        ctrl = _make_controller()
        ctrl._autoscaler = mock.Mock(replica_unit='physical')  # pylint: disable=protected-access
        location = types.SimpleNamespace(cloud='Kubernetes',
                                         region='research-context',
                                         accelerators={'L4': 1})
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [location]
        ctrl._replica_manager = types.SimpleNamespace(spot_placer=placer)  # pylint: disable=protected-access
        ctrl._replica_manager.update_lb_in_flight = mock.Mock()  # pylint: disable=protected-access
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

        def _ingest(*_args, **_kwargs):
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
             mock.patch.object(
                 controller.reserved_capacity,
                 'get_cached_free_gpus_by_pool',
                 side_effect=_record({})), \
             mock.patch.object(ctrl, '_lb_report_authority',
                               return_value=(True, True, True)), \
             mock.patch.object(ctrl, '_get_lb_replica_info',
                               return_value=([], 0)), \
             mock.patch.object(ctrl, '_apply_prepared_load_balancer_report',
                               side_effect=_ingest), \
             mock.patch.object(ctrl, '_get_capacity_hint',
                               return_value={}):
            response = asyncio.run(_drive())
        return response, read_threads, loop_thread[0]

    def test_db_reads_run_off_the_event_loop(self):
        response, read_threads, loop_thread = self._run_sync()
        assert response.status_code == 200
        # Four ownership fences (entry, serialized head, pre-tail, and final
        # drain/disclosure) plus replica rows, specs, and one batched
        # reserved-capacity observation read all happened -- and nothing more.
        assert len(read_threads) == 7
        # ...and none of them on the event-loop thread.
        assert all(tid != loop_thread for tid in read_threads)

    def test_sync_response_shape_preserved(self):
        response, _, _ = self._run_sync()
        assert response.status_code == 200
        body = json.loads(response.body)
        assert set(body) == {
            'replica_info', 'num_ready_replicas', 'routing_spec',
            'capacity_hint', 'request_history_accepted',
            'request_classification_history_accepted',
            'response_time_history_accepted',
            'prediction_time_history_accepted',
            'queued_compatibility_demand_supported', 'service_version'
        }
        assert body['queued_compatibility_demand_supported'] is True
        assert body['service_version'] == 1


class TestLbSyncThreePhase:
    """Concurrency and cancellation boundaries for LB report ingestion."""

    @staticmethod
    def _controller(runtime_tail):
        ctrl = _make_controller()
        ctrl._lb_ha_enabled = True  # pylint: disable=protected-access
        ctrl._autoscaler = mock.Mock(
            replica_unit='logical',  # pylint: disable=protected-access
            latest_version=1)
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._owns_current_service = mock.Mock(return_value=True)  # pylint: disable=protected-access
        ctrl._lb_report_authority = mock.Mock(  # pylint: disable=protected-access
            return_value=(True, True, False))
        ctrl._snapshot_replica_occupancy = mock.Mock(  # pylint: disable=protected-access
            return_value=([], {}, None))
        ctrl._get_lb_replica_info = mock.Mock(return_value=({}, 0))  # pylint: disable=protected-access
        ctrl._get_replica_counts = mock.Mock(return_value={})  # pylint: disable=protected-access
        ctrl._get_capacity_hint = mock.Mock(return_value={})  # pylint: disable=protected-access
        ctrl._persist_request_history = mock.AsyncMock(return_value=True)  # pylint: disable=protected-access
        ctrl._persist_response_time_history = mock.AsyncMock(  # pylint: disable=protected-access
            return_value=True)
        ctrl._persist_prediction_time_history = mock.AsyncMock(  # pylint: disable=protected-access
            return_value=True)
        ctrl._persist_autoscaler_history = mock.AsyncMock(  # pylint: disable=protected-access
            return_value=True)
        ctrl._prepare_load_balancer_report = mock.Mock(  # pylint: disable=protected-access
            side_effect=lambda request, _: (True, request, True))
        ctrl._apply_prepared_load_balancer_report = runtime_tail  # pylint: disable=protected-access
        ctrl._load_balancer_disclosure_is_authorized = mock.Mock(  # pylint: disable=protected-access
            return_value=True)
        return ctrl

    def test_runtime_tail_keeps_event_loop_and_role_lock_responsive(self):
        tail_started = threading.Event()
        release_tail = threading.Event()
        fallback_released = threading.Event()
        test_finished = threading.Event()
        tail_threads = []

        def blocking_tail(*_args):
            tail_threads.append(threading.get_ident())
            tail_started.set()
            assert release_tail.wait(timeout=2)
            return True

        ctrl = self._controller(blocking_tail)

        def release_if_event_loop_is_blocked():
            if (tail_started.wait(timeout=1) and
                    not test_finished.wait(timeout=0.5)):
                fallback_released.set()
                release_tail.set()

        fallback = threading.Thread(target=release_if_event_loop_is_blocked)
        fallback.start()

        async def drive():
            loop_thread = threading.get_ident()
            sync = asyncio.create_task(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    {'lb_session_id': 'active'}))
            started = await asyncio.wait_for(asyncio.to_thread(
                tail_started.wait, 1),
                                             timeout=2)
            assert started
            # This is the same scheduling property the constant-time health
            # route relies on.
            await asyncio.wait_for(asyncio.sleep(0), timeout=0.1)
            assert not sync.done()
            assert tail_threads == [tail_threads[0]]
            assert tail_threads[0] != loop_thread
            role_lock = ctrl._lb_role_lock  # pylint: disable=protected-access
            if role_lock is None:
                role_lock = asyncio.Lock()
                ctrl._lb_role_lock = role_lock  # pylint: disable=protected-access

            async def role_heartbeat():
                async with role_lock:
                    pass

            await asyncio.wait_for(role_heartbeat(), timeout=0.1)
            release_tail.set()
            return await asyncio.wait_for(sync, timeout=2)

        try:
            response = asyncio.run(drive())
        finally:
            test_finished.set()
            release_tail.set()
            fallback.join(timeout=1)

        assert response.status_code == 200
        assert not fallback_released.is_set()
        assert not fallback.is_alive()

    def test_non_ha_cancellation_finishes_drain_publication_after_tail(self):
        tail_started = threading.Event()
        release_tail = threading.Event()
        drain_published = threading.Event()

        def blocking_tail(*_args):
            tail_started.set()
            assert release_tail.wait(timeout=2)
            return True

        ctrl = self._controller(blocking_tail)
        ctrl._lb_ha_enabled = False  # pylint: disable=protected-access
        ctrl._lb_report_authority.return_value = (True, True, True)  # pylint: disable=protected-access
        ctrl._prepare_load_balancer_report.side_effect = (  # pylint: disable=protected-access
            lambda request, _: (True, request, False))

        def publish_drain(_request_data):
            drain_published.set()
            return True

        ctrl._apply_authoritative_load_balancer_drain_report = publish_drain  # pylint: disable=protected-access

        async def drive():
            sync = asyncio.create_task(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    {'lb_session_id': 'legacy'}))
            started = await asyncio.wait_for(asyncio.to_thread(
                tail_started.wait, 1),
                                             timeout=2)
            assert started
            sync.cancel()
            await asyncio.sleep(0)
            assert not sync.done()
            assert not drain_published.is_set()
            release_tail.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(sync, timeout=2)

        try:
            asyncio.run(drive())
        finally:
            release_tail.set()

        assert drain_published.is_set()

    def test_non_ha_cancellation_waits_for_drain_role_lock(self):
        drain_published = threading.Event()

        class BlockingRoleLock:
            """Expose cancellation while final drain waits for role ordering."""

            def __init__(self):
                self.waiting = asyncio.Event()
                self.allow = asyncio.Event()
                self.release_calls = 0

            async def acquire(self):
                self.waiting.set()
                await self.allow.wait()
                return True

            def release(self):
                self.release_calls += 1

        ctrl = self._controller(mock.Mock(return_value=True))
        ctrl._lb_ha_enabled = False  # pylint: disable=protected-access
        ctrl._lb_report_authority.return_value = (True, True, True)  # pylint: disable=protected-access
        ctrl._prepare_load_balancer_report.side_effect = (  # pylint: disable=protected-access
            lambda request, _: (True, request, False))

        def publish_drain(_request_data):
            drain_published.set()
            return True

        ctrl._apply_authoritative_load_balancer_drain_report = publish_drain  # pylint: disable=protected-access

        async def drive():
            role_lock = BlockingRoleLock()
            ctrl._lb_role_lock = role_lock  # pylint: disable=protected-access
            sync = asyncio.create_task(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    {'lb_session_id': 'legacy'}))
            await asyncio.wait_for(role_lock.waiting.wait(), timeout=2)
            sync.cancel()
            await asyncio.sleep(0)
            assert not sync.done()
            assert not drain_published.is_set()
            role_lock.allow.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(sync, timeout=2)
            assert role_lock.release_calls == 1

        asyncio.run(drive())
        assert drain_published.is_set()

    def test_repeated_cancellation_keeps_phase_lock_until_worker_finishes(self):
        worker_started = threading.Event()
        release_worker = threading.Event()

        def blocking_worker():
            worker_started.set()
            assert release_worker.wait(timeout=2)
            return True

        ctrl = _make_controller()

        async def drive():
            phase_lock = asyncio.Lock()
            contender_entered = asyncio.Event()

            async def guarded_worker():
                async with phase_lock:
                    loop = asyncio.get_running_loop()
                    operation = loop.run_in_executor(None, blocking_worker)
                    await ctrl._await_executor_operation(  # pylint: disable=protected-access
                        operation, 'test worker')

            async def contender():
                async with phase_lock:
                    contender_entered.set()

            guarded = asyncio.create_task(guarded_worker())
            started = await asyncio.wait_for(asyncio.to_thread(
                worker_started.wait, 1),
                                             timeout=2)
            assert started
            guarded.cancel()
            await asyncio.sleep(0)
            guarded.cancel()
            waiting = asyncio.create_task(contender())
            await asyncio.sleep(0.02)
            assert not contender_entered.is_set()
            assert not guarded.done()
            release_worker.set()
            with pytest.raises(asyncio.CancelledError):
                await guarded
            await asyncio.wait_for(waiting, timeout=1)
            assert contender_entered.is_set()

        try:
            asyncio.run(drive())
        finally:
            release_worker.set()

    def test_cancelled_executor_future_does_not_spin(self):
        ctrl = _make_controller()

        async def drive():
            operation = asyncio.get_running_loop().create_future()
            operation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    ctrl._await_executor_operation(  # pylint: disable=protected-access
                        operation, 'cancelled test worker'),
                    timeout=0.1)

        asyncio.run(drive())


class TestLbSyncOwnershipFences:
    """Every async phase revalidates ownership before its side effects.

    A controller that loses its DB row may neither enter the serialized
    handoff head, mutate the contended runtime tail, publish drain state, nor
    disclose routing.
    """

    def _make_fenced_controller(self):
        ctrl = _make_controller()
        ctrl._autoscaler = mock.Mock(replica_unit='physical')  # pylint: disable=protected-access
        ctrl._replica_manager = mock.Mock()  # pylint: disable=protected-access
        ctrl._service_hash = 'incarnation-a'  # pylint: disable=protected-access
        ctrl._controller_owner = (101, '10.0.0.1')  # pylint: disable=protected-access
        return ctrl

    def _sync(self, ctrl, owner_rows, request_data=None):
        """Drive one sync; each fence read pops the next owner row."""
        ingest_calls = []
        history_calls = []

        def _ingest(*args, **kwargs):
            ingest_calls.append(('runtime', args, kwargs))
            return True

        def _publish_drain(*args, **kwargs):
            if not ctrl._owns_current_service():  # pylint: disable=protected-access
                return False
            ingest_calls.append(('drain', args, kwargs))
            return True

        def _record_history(data):
            history_calls.append(data)
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
             mock.patch.object(ctrl, '_apply_prepared_load_balancer_report',
                               side_effect=_ingest), \
             mock.patch.object(
                 ctrl,
                 '_apply_authoritative_load_balancer_drain_report',
                 side_effect=_publish_drain), \
             mock.patch.object(ctrl, '_record_request_history',
                               side_effect=_record_history), \
             mock.patch.object(ctrl, '_get_capacity_hint',
                               return_value={}):
            response = asyncio.run(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    request_data or {'lb_session_id': 'lb-a'}))
        return response, ingest_calls, history_calls

    def test_non_owner_rejected_at_entry(self):
        ctrl = self._make_fenced_controller()
        stolen = {
            'hash': 'incarnation-b',
            'controller_pid': 202,
            'controller_ip': '10.0.0.2',
        }
        response, ingest_calls, _ = self._sync(ctrl, owner_rows=[stolen])
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
        response, ingest_calls, _ = self._sync(ctrl, owner_rows=[owned, stolen])
        assert response.status_code == 503
        assert not ingest_calls

    def test_ownership_lost_during_history_write_blocks_disclosure(self):
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
        request_data = {
            'lb_session_id': 'lb-a',
            'request_history_session_id': 'a' * 32,
            'request_history': {
                'bucket_seconds': 60,
                'buckets': [],
            },
        }

        response, ingest_calls, history_calls = self._sync(
            ctrl, owner_rows=[owned, stolen], request_data=request_data)

        assert response.status_code == 503
        assert response.body == b''
        assert history_calls == [request_data]
        assert not ingest_calls

    def test_ownership_lost_after_head_blocks_runtime_tail(self):
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

        response, ingest_calls, _ = self._sync(
            ctrl, owner_rows=[owned, owned, stolen])

        assert response.status_code == 503
        assert not ingest_calls

    def test_ownership_lost_after_runtime_blocks_drain_and_disclosure(self):
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

        response, ingest_calls, _ = self._sync(
            ctrl, owner_rows=[owned, owned, owned, stolen])

        assert response.status_code == 503
        assert [phase for phase, _, _ in ingest_calls] == ['runtime']

    def test_ownership_lost_during_bridge_confirmation_blocks_side_effects(
            self):
        ctrl = self._make_fenced_controller()
        autoscaler = _StatefulDemandAutoscaler()
        autoscaler.replica_unit = 'logical'
        autoscaler.latest_version = 2
        replica_manager = _StatefulReplicaManager()
        ctrl._autoscaler = autoscaler  # pylint: disable=protected-access
        ctrl._replica_manager = replica_manager  # pylint: disable=protected-access
        url = 'http://1.1.1.1:8080'
        ctrl._lb_translation_cache = {  # pylint: disable=protected-access
            1: (url, 'L4', 8)
        }
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                version=1,
                                url=url,
                                accelerators={'L4': 8})
        report = {
            'lb_session_id': 'lb-a',
            'total_slots_by_url': {
                url: 8
            },
        }
        owns_service = [True]
        confirm_started = threading.Event()
        release_confirm = threading.Event()
        confirmed = []

        def _confirm(capacities):
            confirmed.append(dict(capacities))
            confirm_started.set()
            assert release_confirm.wait(timeout=5)
            return dict(capacities)

        replica_manager.confirm_logical_bridge_capacities = _confirm
        autoscaler_before = autoscaler.snapshot()
        drain_before = replica_manager.snapshot()

        async def _drive():
            task = asyncio.create_task(
                ctrl._handle_load_balancer_sync(  # pylint: disable=protected-access
                    report))
            for _ in range(500):
                if confirm_started.is_set():
                    break
                await asyncio.sleep(0.01)
            assert confirm_started.is_set()
            owns_service[0] = False
            release_confirm.set()
            return await task

        with mock.patch.object(
                ctrl,
                '_owns_current_service',
                side_effect=lambda: owns_service[0]), \
             mock.patch.object(
                 ctrl,
                 '_lb_report_authority',
                 return_value=(True, True, True)), \
             mock.patch.object(
                 ctrl,
                 '_snapshot_replica_occupancy',
                 return_value=([info], {
                     1: True
                 }, {2})), \
             mock.patch.object(
                 ctrl, '_get_lb_replica_info', return_value=({
                     'secret': {}
                 }, 1)), \
             mock.patch.object(
                 ctrl,
                 '_persist_request_history',
                 new=mock.AsyncMock(return_value=True)):
            response = asyncio.run(_drive())

        assert response.status_code == 503
        assert response.body == b''
        assert confirmed == [{1: 8}]
        assert autoscaler.snapshot() == autoscaler_before
        assert replica_manager.snapshot() == drain_before

    def test_history_only_sync_never_ingests_demand_or_discloses_routes(self):
        ctrl = self._make_fenced_controller()
        request_data = {
            'lb_session_id': 'lb-a',
            'request_history_session_id': 'a' * 32,
            'request_history': {
                'bucket_seconds': 60,
                'buckets': [],
            },
        }
        ctrl._ingest_load_balancer_report = mock.AsyncMock()  # pylint: disable=protected-access

        with mock.patch.object(ctrl,
                               '_owns_current_service',
                               return_value=True), \
             mock.patch.object(ctrl,
                               '_lb_report_authority',
                               return_value=(True, False, False)), \
             mock.patch.object(
                 ctrl,
                 '_persist_request_history',
                 new=mock.AsyncMock(return_value=True)) as persist:
            response = asyncio.run(
                ctrl._handle_load_balancer_request_history_sync(  # pylint: disable=protected-access
                    request_data))

        assert response.status_code == 200
        assert json.loads(response.body) == {
            'request_history_accepted': True,
            'request_classification_history_accepted': True,
            'response_time_history_accepted': True,
            'prediction_time_history_accepted': True,
        }
        persist.assert_awaited_once_with(request_data)
        ctrl._ingest_load_balancer_report.assert_not_awaited()  # pylint: disable=protected-access

    def test_history_only_sync_rejects_nonmember_before_persistence(self):
        ctrl = self._make_fenced_controller()
        persist = mock.AsyncMock(return_value=True)

        with mock.patch.object(ctrl,
                               '_owns_current_service',
                               return_value=True), \
             mock.patch.object(ctrl,
                               '_lb_report_authority',
                               return_value=(False, False, False)), \
             mock.patch.object(
                 ctrl, '_persist_request_history', new=persist):
            response = asyncio.run(
                ctrl._handle_load_balancer_request_history_sync(  # pylint: disable=protected-access
                    {'lb_session_id': 'other-service-pod'}))

        assert response.status_code == 503
        assert response.body == b''
        persist.assert_not_awaited()

    def test_apply_load_balancer_report_is_synchronous(self):
        # The final ownership fence relies on runtime mutation and routing
        # disclosure having no await between them.
        ctrl = _make_controller()
        ctrl._replica_manager = mock.Mock()
        ctrl._autoscaler = mock.Mock()
        accepted = ctrl._apply_load_balancer_report(  # pylint: disable=protected-access
            {'lb_session_id': 'lb-a'}, [], {},
            authority=(True, True, True),
            observed_slots={})
        assert accepted is True


class TestDrainProofStats:
    """Milestone 0 counters: which cost actually dominates on an LB restart."""

    def _stats(self):
        return drain_observability.DrainProofStats()

    def test_starts_empty(self):
        snapshot = self._stats().snapshot()
        assert snapshot['deadline_expiry_without_proof'] == 0
        assert snapshot['proved_drained'] == 0
        assert snapshot['logical_aborts_total'] == 0
        assert snapshot['blind_capacity_rounds'] == 0

    def test_counts_the_two_competing_outcomes_separately(self):
        stats = self._stats()
        stats.record_proved_drained()
        stats.record_proved_drained()
        stats.record_deadline_expiry_without_proof()
        snapshot = stats.snapshot()
        assert snapshot['proved_drained'] == 2
        assert snapshot['deadline_expiry_without_proof'] == 1

    def test_abort_reasons_are_bounded(self):
        # Reasons come from call sites, never user input, but an unbounded
        # Counter key would still leak in a long-lived controller.
        stats = self._stats()
        stats.record_logical_abort(
            drain_observability.ABORT_REASON_TARGET_COVERAGE)
        stats.record_logical_abort('something nobody defined')
        snapshot = stats.snapshot()
        assert snapshot['logical_aborts'] == {
            drain_observability.ABORT_REASON_TARGET_COVERAGE: 1,
            drain_observability.ABORT_REASON_OTHER: 1,
        }
        assert snapshot['logical_aborts_total'] == 2

    def test_blind_capacity_only_counts_rounds_that_skipped(self):
        stats = self._stats()
        stats.record_blind_ready_capacity(0)
        stats.record_blind_ready_capacity(77)
        snapshot = stats.snapshot()
        assert snapshot['blind_capacity_rounds'] == 1
        assert snapshot['blind_capacity_skipped_replicas'] == 77

    @pytest.mark.parametrize(('reason', 'expected'), [
        ('replacement capacity still covers the target',
         drain_observability.ABORT_REASON_TARGET_COVERAGE),
        ('the bounded rolling-update coverage fence changed',
         drain_observability.ABORT_REASON_TARGET_COVERAGE),
        ('post-routing idle proof timed out',
         drain_observability.ABORT_REASON_IDLE_PROOF_TIMEOUT),
        ('the current target or controller fence changed',
         drain_observability.ABORT_REASON_FENCE_CHANGED),
        ('some unmapped reason', drain_observability.ABORT_REASON_OTHER),
    ])
    def test_reason_classifier_covers_the_live_call_sites(
            self, reason, expected):
        assert replica_managers._classify_abort_reason(reason) == expected
