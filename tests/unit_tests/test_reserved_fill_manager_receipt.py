"""Focused tests for typed reserved-fill admission receipts."""
# pylint: disable=protected-access

import contextlib
import dataclasses
import threading
import time
from types import SimpleNamespace
from unittest import mock
import uuid

import pytest

from sky import clouds
from sky.serve import controller
from sky.serve import ordinary_launch_binding
from sky.serve import replica_managers
from sky.serve import reserved_capacity_broker
from sky.serve import reserved_fill_planner
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.serve import zero_cost_actuation
from sky.utils import common_utils

_SERVICE_HASH = 'service-incarnation'
_CONTROLLER_PID = 41
_CONTROLLER_IP = '10.0.0.7'
_CONTROLLER_PORT = 8123
_CONTROLLER_INCARNATION = uuid.UUID('7d1f78d1-27f2-4b6c-913f-49ad42e444b0')
_CONTROLLER_OWNER_EPOCH = 4
_RECONCILIATION_GATE_GENERATION = 29
_RECLAIM_FLEET_BUNDLE_SHA256 = 'c' * 64
_RECLAIM_POLICY_REVISION = 'kueue-reclaim-v1'
_RECLAIM_PROVIDER_INVENTORY_SHA256 = 'd' * 64
_WORKER_PROJECTION_SHA256 = 'e' * 64
_OWNER_FINGERPRINT = serve_utils.make_controller_owner_fingerprint(
    _SERVICE_HASH, _CONTROLLER_PID, _CONTROLLER_IP, _CONTROLLER_PORT)


def _snapshot(
    context_name: str,
    physical_uid: str,
    free_slots: int,
    *,
    accelerator_count: int = 1,
    valid_until: float | None = None,
) -> reserved_fill_planner.PoolFillSnapshot:
    location = spot_placer.Location(cloud=clouds.Kubernetes(),
                                    region=context_name,
                                    zone=None,
                                    accelerators={'A100': accelerator_count},
                                    use_spot=False)
    pool_key = reserved_capacity_broker.make_pool_key(
        context_name,
        'A100',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid=physical_uid)
    return reserved_fill_planner.PoolFillSnapshot.from_mapping({
        'protocol_version': reserved_capacity_broker.PROTOCOL_V2,
        'pool_key': pool_key,
        'physical_cluster_uid': physical_uid,
        'service_generation': 7,
        'worker_projection_sha256_by_accelerator': {
            'a100': _WORKER_PROJECTION_SHA256,
        },
        'edge_cap': free_slots,
        'broker_slot_width': accelerator_count,
        'free_slots': free_slots,
        'free_slots_by_accelerator': {
            'a100': free_slots
        },
        'grant': free_slots,
        'grant_epoch': 23,
        'observation_generation': 13,
        'observation_sequence': 17,
        'ordinary_zero_cost_admission_sequence': 17,
        'valid_until':
            (time.time() + 60.0 if valid_until is None else valid_until),
        'zero_cost_location_keys': [location.to_pickleable()],
    })


def _plan(
    snapshots: tuple[reserved_fill_planner.PoolFillSnapshot, ...],
    *,
    capacity_unit: reserved_fill_planner.FillCapacityUnit = (
        reserved_fill_planner.FillCapacityUnit.PHYSICAL),
    committed_fill_debits: tuple[reserved_fill_planner.CommittedFillDebit,
                                 ...] = (),
) -> reserved_fill_planner.FillPlan:
    allocation_map = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=19,
        ordinary_zero_cost_admission_sequence_high_water=(
            snapshots[0].ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=snapshots)
    return reserved_fill_planner.ReservedFillPlanner.plan(
        policy_revision=2,
        reconcile_generation=3,
        allocation_map=allocation_map,
        service_incarnation=_SERVICE_HASH,
        service_version=19,
        controller_owner=_OWNER_FINGERPRINT,
        max_replicas=100,
        planned_replicas=0,
        capacity_unit=capacity_unit,
        committed_fill_debits=committed_fill_debits)


def _manager(*,
             maximum: int = 100,
             logical: bool = False) -> replica_managers.SkyPilotReplicaManager:
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'  # pylint: disable=protected-access
    manager._service_hash = _SERVICE_HASH  # pylint: disable=protected-access
    manager._resource_scope = _SERVICE_HASH  # pylint: disable=protected-access
    manager._controller_owner = (  # pylint: disable=protected-access
        _CONTROLLER_PID, _CONTROLLER_IP)
    manager._enforce_launch_fence = True  # pylint: disable=protected-access
    manager.latest_version = 19
    manager._uses_logical_replicas = logical  # pylint: disable=protected-access
    manager._is_pool = False  # pylint: disable=protected-access
    manager._spot_placer = None  # pylint: disable=protected-access
    manager._pending_version = None  # pylint: disable=protected-access
    manager._version_specs = {  # pylint: disable=protected-access
        19: SimpleNamespace(max_replicas=maximum, min_replicas=0)
    }
    manager._ordinary_launch_binding_authority = (  # pylint: disable=protected-access
        ordinary_launch_binding.ControllerBindingAuthority(
            service_name='svc',
            service_hash=_SERVICE_HASH,
            service_workspace='default',
            service_lifecycle_epoch=3,
            controller_pid=_CONTROLLER_PID,
            controller_ip=_CONTROLLER_IP,
            controller_incarnation=_CONTROLLER_INCARNATION,
            controller_owner_epoch=_CONTROLLER_OWNER_EPOCH,
            capable=True,
            binding_mode=ordinary_launch_binding.BindingMode.BOUND,
            binding_epoch=1))
    return manager


def _owner_record(**updates):
    record = {
        'hash': _SERVICE_HASH,
        'controller_pid': _CONTROLLER_PID,
        'controller_ip': _CONTROLLER_IP,
        'controller_port': _CONTROLLER_PORT,
        'status': serve_state.ServiceStatus.READY,
    }
    record.update(updates)
    return record


@contextlib.contextmanager
def _physical_fence():
    yield


@pytest.fixture(autouse=True)
def _isolate_demand_capacity_lock():
    """Keep receipt tests independent under pytest-xdist.

    Production deliberately serializes ordinary zero-cost demand and reserved
    fill with one fleet-wide lock.  These tests exercise the admission logic
    behind that boundary, so sharing the real file lock across workers makes
    otherwise unrelated cases defer each other nondeterministically.
    """
    demand_lock = mock.Mock()
    demand_lock.acquire.return_value = contextlib.nullcontext()
    real_get_lock = replica_managers.locks.get_lock

    def get_lock(lock_id, *args, **kwargs):
        if lock_id == replica_managers.serve_constants.DEMAND_CAPACITY_RESERVATION_LOCK_ID:
            return demand_lock
        return real_get_lock(lock_id, *args, **kwargs)

    with mock.patch.object(replica_managers.locks,
                           'get_lock',
                           side_effect=get_lock):
        yield


def test_accept_reserved_fill_returns_exact_rows_and_preflights_in_parallel(
) -> None:
    manager = _manager()
    east = _snapshot('east-context', 'uid-east', 1)
    west = _snapshot('west-context', 'uid-west', 1)
    plan = _plan((east, west))
    barrier = threading.Barrier(2)
    entered: list[tuple[str, str]] = []
    observed_overrides = []

    @contextlib.contextmanager
    def physical_fence(context_name, physical_uid, **kwargs):
        assert kwargs == {'wait_for_initializer': False}
        assert not manager.lock.locked()
        entered.append((context_name, physical_uid))
        barrier.wait(timeout=2)
        yield

    def accept_one(resources_override, _used_ids, _infos, **kwargs):
        assert manager.lock.locked()
        assert 'provider_phase_admission' in kwargs
        assert kwargs['require_preinitialized_physical_fence'] is True
        observed_overrides.append(resources_override)
        return replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
            replica_id=100 + len(observed_overrides),
            planned_capacity=1,
            funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access

    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    replica_managers.reserved_capacity_broker,
                    'current_epoch',
                    return_value=23), mock.patch.object(
                        replica_managers.kubernetes_adaptor,
                        'physical_cluster_uid_fence',
                        side_effect=physical_fence), mock.patch.object(
                            manager,
                            '_scale_up_one_locked',
                            side_effect=accept_one):
        receipt = manager.accept_reserved_fill(plan)

    assert set(entered) == {('east-context', 'uid-east'),
                            ('west-context', 'uid-west')}
    assert [(item.intent_idempotency_key, item.replica_id)
            for item in receipt.accepted] == [
                (plan.intents[0].idempotency_key, 101),
                (plan.intents[1].idempotency_key, 102),
            ]
    assert not receipt.deferred
    assert receipt.authority_current
    for intent, override in zip(plan.intents, observed_overrides):
        assert override['accelerators'] == {
            intent.accelerator: intent.accelerator_count
        }
        assert override['_reserved_fill_pool_key'] == intent.pool_key
        assert override['_reserved_fill_physical_cluster_uid'] == (
            intent.physical_cluster_uid)
        assert override['_reserved_fill_allowed_locations'] == list(
            intent.allowed_location_keys())
        assert override['_reserved_fill_allocation_generation'] == (
            intent.allocation_generation)
        assert override['_reserved_fill_allocation_input_sha256'] == (
            intent.allocation_input_sha256)
        assert override['_reserved_fill_allocation_claim_generation'] == (
            intent.allocation_claim_generation)
        assert override['_reserved_fill_reconciliation_gate_generation'] == (
            intent.reconciliation_gate_generation)
        assert override['_reserved_fill_reclaim_fleet_bundle_sha256'] == (
            intent.reclaim_fleet_bundle_sha256)
        assert override['_reserved_fill_reclaim_policy_revision'] == (
            intent.reclaim_policy_revision)
        assert override['_reserved_fill_reclaim_provider_inventory_sha256'] == (
            intent.reclaim_provider_inventory_sha256)
        assert override['_reserved_fill_observation_generation'] == (
            intent.observation_generation)
        assert override['_reserved_fill_observation_sequence'] == (
            intent.observation_sequence)
        assert override['_reserved_fill_ordinary_admission_sequence'] == (
            intent.ordinary_zero_cost_admission_sequence)
        assert override['_reserved_fill_intent_idempotency_key'] == (
            intent.idempotency_key)
        assert '_zero_cost_admission_sequence' not in override


def test_durable_accept_publishes_grants_without_provider_or_replica_io(
) -> None:
    manager = _manager(maximum=7)
    manager._reserved_fill_actuation_mode = (  # pylint: disable=protected-access
        zero_cost_actuation.ActuationMode.DURABLE_INTENT)
    plan = _plan((_snapshot('east-context', 'uid-east', 1),))
    receipt = reserved_fill_planner.FillCommitResult(
        accepted=(reserved_fill_planner.AcceptedFillIntent(
            plan.intents[0].idempotency_key, None),),
        deferred=(),
        authority_current=True)
    repository = mock.Mock()
    repository.grant_plan.return_value = receipt
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access

    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.provider_phase,
                'try_provider_phase') as provider_admission, mock.patch.object(
                    replica_managers.serve_state,
                    'get_replica_infos') as replica_read:
        actual = manager.accept_reserved_fill(plan)

    assert actual == receipt
    repository.grant_plan.assert_called_once_with(
        'svc',
        plan,
        max_capacity=7,
        expected_controller_incarnation=_CONTROLLER_INCARNATION,
        expected_controller_owner_epoch=_CONTROLLER_OWNER_EPOCH)
    provider_admission.assert_not_called()
    replica_read.assert_not_called()


def test_durable_accept_without_controller_authority_fails_closed() -> None:
    manager = _manager(maximum=7)
    manager._reserved_fill_actuation_mode = (  # pylint: disable=protected-access
        zero_cost_actuation.ActuationMode.DURABLE_INTENT)
    manager._ordinary_launch_binding_authority = None  # pylint: disable=protected-access
    plan = _plan((_snapshot('east-context', 'uid-east', 1),))
    repository = mock.Mock()
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access

    with mock.patch.object(replica_managers.serve_state,
                           'get_service_controller_owner',
                           return_value=_owner_record()):
        receipt = manager.accept_reserved_fill(plan)

    assert not receipt.accepted
    assert receipt.authority_current is False
    assert len(receipt.deferred) == 1
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.LOST_OWNER)
    assert receipt.deferred[0].detail == (
        'durable controller authority is unavailable')
    repository.grant_plan.assert_not_called()


def test_durable_pool_executor_does_not_serialize_independent_pools() -> None:
    manager = _manager()
    manager.lock = threading.Lock()
    manager._workspace = 'workspace-a'  # pylint: disable=protected-access
    manager._zero_cost_actuation_executor_id = (  # pylint: disable=protected-access
        mock.sentinel.executor_id)
    manager._scale_reconciliation_event = threading.Event()  # pylint: disable=protected-access
    east = _snapshot('east-context', 'uid-east', 1)
    west = _snapshot('west-context', 'uid-west', 1)
    plan = _plan((east, west))
    leases = {
        intent.pool_key: SimpleNamespace(intent=intent)
        for intent in plan.intents
    }
    repository = mock.Mock()
    repository.lease_next.side_effect = (
        lambda **kwargs: leases[kwargs['pool_key']])
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access
    blocked_entered = threading.Event()
    release_blocked = threading.Event()
    healthy_committed = threading.Event()

    def start_preflight(intents, _admission, _workspace):
        intent = intents[0]
        context_name = intent.allowed_locations[0].region
        if context_name == 'east-context':
            blocked_entered.set()
            assert release_blocked.wait(timeout=2)
        return SimpleNamespace(
            preflights={
                (context_name, intent.physical_cluster_uid): SimpleNamespace(
                    error=None)
            })

    def scale_one(resources_override, *_args, **_kwargs):
        if resources_override[
                replica_managers.serve_constants.
                RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY] == 'uid-west':
            healthy_committed.set()
        return replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
            replica_id=91,
            planned_capacity=1,
            funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access

    provider_admission = contextlib.contextmanager(lambda: iter(
        (mock.sentinel.admission,)))
    with mock.patch.object(
            manager,
            '_zero_cost_actuation_authority_current',
            return_value=True), mock.patch.object(
                replica_managers.provider_phase,
                'try_provider_phase',
                side_effect=lambda *_args, **_kwargs: provider_admission()), \
            mock.patch.object(
                manager,
                '_start_reserved_fill_physical_preflights',
                side_effect=start_preflight), mock.patch.object(
                    manager,
                    '_release_reserved_fill_physical_preflights'), mock.patch.object(
                        replica_managers.serve_state,
                        'get_replica_infos',
                        return_value=[]), mock.patch.object(
                       manager,
                       '_scale_up_one_locked',
                       side_effect=scale_one):
        blocked = threading.Thread(target=manager._actuate_zero_cost_pool,
                                   args=(east.pool_key,))
        healthy = threading.Thread(target=manager._actuate_zero_cost_pool,
                                   args=(west.pool_key,))
        blocked.start()
        assert blocked_entered.wait(timeout=1)
        healthy.start()
        assert healthy_committed.wait(timeout=1)
        release_blocked.set()
        blocked.join(timeout=2)
        healthy.join(timeout=2)

    assert not blocked.is_alive()
    assert not healthy.is_alive()
    assert repository.lease_next.call_count == 2
    repository.release_retryable.assert_not_called()
    repository.terminate.assert_not_called()


def test_unknown_actuation_mode_fails_closed_before_provider_io() -> None:
    manager = _manager()
    manager._reserved_fill_actuation_mode = None  # pylint: disable=protected-access
    plan = _plan((_snapshot('east-context', 'uid-east', 1),))

    with mock.patch.object(replica_managers.provider_phase,
                           'try_provider_phase') as provider_admission:
        receipt = manager.accept_reserved_fill(plan)

    provider_admission.assert_not_called()
    assert not receipt.accepted
    assert receipt.authority_current is False
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.LOST_OWNER)


def test_locked_fill_dispatch_requires_the_preinitialized_capture() -> None:
    manager = _manager()
    manager._next_replica_id = 1  # pylint: disable=protected-access
    plan = _plan((_snapshot('east-context', 'uid-east', 1),))
    override = manager._reserved_fill_override(plan.intents[0])
    existing = []
    manager._launch_replica = mock.Mock(return_value=None)  # pylint: disable=protected-access

    result = manager._scale_up_one_locked(  # pylint: disable=protected-access
        override,
        set(),
        existing,
        provider_phase_admission=mock.sentinel.admission,
        require_preinitialized_physical_fence=True)

    assert result is None
    manager._launch_replica.assert_called_once_with(  # pylint: disable=protected-access
        1,
        override,
        existing_replica_infos=existing,
        provider_phase_admission=mock.sentinel.admission,
        require_preinitialized_physical_fence=True)


def test_physical_preflight_batch_uses_one_absolute_deadline() -> None:
    blocked_contexts = ('east-context', 'phx-context', 'west-context')
    intents = tuple(
        SimpleNamespace(allowed_locations=(SimpleNamespace(
            region=context_name),),
                        physical_cluster_uid=f'uid-{index}')
        for index, context_name in enumerate(blocked_contexts))
    release_initializers = threading.Event()

    @contextlib.contextmanager
    def blocked_fence(*_args, **_kwargs):
        release_initializers.wait(timeout=2)
        yield

    with mock.patch.object(
            replica_managers,
            '_RESERVED_FILL_PHYSICAL_PREFLIGHT_TIMEOUT_SECONDS',
            0.05), mock.patch.object(
                replica_managers,
                '_RESERVED_FILL_PHYSICAL_PREFLIGHT_RELEASE_TIMEOUT_SECONDS',
                0.05), mock.patch.object(
                    replica_managers.skypilot_config,
                    'local_active_workspace_ctx',
                    side_effect=lambda _workspace: contextlib.nullcontext(
                    )), mock.patch.object(
                        replica_managers.provider_phase,
                        'join_provider_phase',
                        side_effect=lambda _admission, **_kwargs: contextlib.
                        nullcontext()), mock.patch.object(
                            replica_managers.kubernetes_adaptor,
                            'physical_cluster_uid_fence',
                            side_effect=blocked_fence):
        started = time.monotonic()
        manager_class = replica_managers.SkyPilotReplicaManager
        start_preflights = (  # pylint: disable=protected-access
            manager_class._start_reserved_fill_physical_preflights)
        batch = start_preflights(intents, mock.sentinel.admission, 'workspace')
        elapsed = time.monotonic() - started
        release_preflights = (  # pylint: disable=protected-access
            manager_class._release_reserved_fill_physical_preflights)
        release_started = time.monotonic()
        release_preflights(batch)
        release_elapsed = time.monotonic() - release_started
        release_initializers.set()
        for thread in batch.threads:
            thread.join(timeout=0.5)

    # The old sequential join consumed roughly 3 * 0.05 seconds here.
    assert elapsed < 0.11
    assert release_elapsed < 0.11
    assert all(
        isinstance(preflight.error, TimeoutError)
        for preflight in batch.preflights.values())
    assert all(not thread.is_alive() for thread in batch.threads)


def test_slow_preflight_cancels_real_provider_children_and_reopens_gate(
) -> None:
    intents = tuple(
        SimpleNamespace(allowed_locations=(SimpleNamespace(region=context),),
                        physical_cluster_uid=f'uid-{index}')
        for index, context in enumerate(('east-context', 'west-context')))
    entered = 0
    entered_lock = threading.Lock()
    both_entered = threading.Event()

    @contextlib.contextmanager
    def slow_fence(*_args, **_kwargs):
        nonlocal entered
        with entered_lock:
            entered += 1
            if entered == len(intents):
                both_entered.set()
        while True:
            (replica_managers.kubernetes_adaptor.
             raise_if_api_call_deadline_exceeded())
            time.sleep(0.002)
        yield  # pragma: no cover

    manager_class = replica_managers.SkyPilotReplicaManager
    start_preflights = (  # pylint: disable=protected-access
        manager_class._start_reserved_fill_physical_preflights)
    release_preflights = (  # pylint: disable=protected-access
        manager_class._release_reserved_fill_physical_preflights)
    mode = replica_managers.provider_phase.ProviderPhaseMode
    with mock.patch.object(
            replica_managers,
            '_RESERVED_FILL_PHYSICAL_PREFLIGHT_TIMEOUT_SECONDS',
            0.1), mock.patch.object(
                replica_managers,
                '_RESERVED_FILL_PHYSICAL_PREFLIGHT_RELEASE_TIMEOUT_SECONDS',
                0.1), mock.patch.object(
                    replica_managers.skypilot_config,
                    'local_active_workspace_ctx',
                    side_effect=lambda _workspace: contextlib.nullcontext(
                    )), mock.patch.object(replica_managers.kubernetes_adaptor,
                                          'physical_cluster_uid_fence',
                                          side_effect=slow_fence):
        started = time.monotonic()
        with replica_managers.provider_phase.try_provider_phase(
                mode.V2_FENCED, child_drain_timeout_seconds=0.1) as admission:
            batch = start_preflights(intents, admission, 'workspace')
            assert both_entered.is_set()
            release_preflights(batch)
        elapsed = time.monotonic() - started

    assert elapsed < 0.5
    assert all(not thread.is_alive() for thread in batch.threads)
    assert all(
        isinstance(preflight.error, TimeoutError)
        for preflight in batch.preflights.values())
    # This uses the real root/child gate. An opposite phase entering now proves
    # that every canceled child released its user before root cleanup returned.
    with replica_managers.provider_phase.try_provider_phase(
            mode.AMBIENT_LEGACY):
        pass


def test_preflight_timeout_cancels_only_the_blocked_context() -> None:
    intents = tuple(
        SimpleNamespace(allowed_locations=(SimpleNamespace(region=context),),
                        physical_cluster_uid=uid)
        for context, uid in (('blocked-context', 'uid-blocked'),
                             ('healthy-context', 'uid-healthy')))
    unblock = threading.Event()
    healthy_active = threading.Event()
    healthy_exited = threading.Event()

    @contextlib.contextmanager
    def physical_fence(context_name, _physical_uid, **_kwargs):
        if context_name == 'blocked-context':
            unblock.wait(timeout=2)
        else:
            healthy_active.set()
        try:
            yield
        finally:
            if context_name == 'healthy-context':
                healthy_exited.set()

    manager_class = replica_managers.SkyPilotReplicaManager
    start_preflights = (  # pylint: disable=protected-access
        manager_class._start_reserved_fill_physical_preflights)
    release_preflights = (  # pylint: disable=protected-access
        manager_class._release_reserved_fill_physical_preflights)
    with mock.patch.object(
            replica_managers,
            '_RESERVED_FILL_PHYSICAL_PREFLIGHT_TIMEOUT_SECONDS',
            0.05), mock.patch.object(
                replica_managers,
                '_RESERVED_FILL_PHYSICAL_PREFLIGHT_RELEASE_TIMEOUT_SECONDS',
                0.1), mock.patch.object(
                    replica_managers.skypilot_config,
                    'local_active_workspace_ctx',
                    side_effect=lambda _workspace: contextlib.nullcontext()), \
            mock.patch.object(
                replica_managers.provider_phase,
                'join_provider_phase',
                side_effect=lambda _admission, **_kwargs: contextlib.
                nullcontext()), mock.patch.object(
                    replica_managers.kubernetes_adaptor,
                    'physical_cluster_uid_fence',
                    side_effect=physical_fence):
        batch = start_preflights(intents, mock.sentinel.admission, 'workspace')
        blocked = batch.preflights[('blocked-context', 'uid-blocked')]
        healthy = batch.preflights[('healthy-context', 'uid-healthy')]
        assert isinstance(blocked.error, TimeoutError)
        assert blocked.cancellation.is_set()
        assert healthy.error is None
        assert not healthy.cancellation.is_set()
        assert healthy_active.is_set()
        # Capture ownership does not expire with the initialization deadline.
        time.sleep(0.06)
        assert not healthy_exited.is_set()
        unblock.set()
        release_preflights(batch)

    for thread in batch.threads:
        thread.join(timeout=0.5)
    assert all(not thread.is_alive() for thread in batch.threads)
    assert healthy_exited.is_set()


def test_bad_first_context_does_not_consume_headroom_or_starve_healthy_tail(
) -> None:
    manager = _manager(maximum=1)
    east = _snapshot('east-context', 'uid-east', 1)
    west = _snapshot('west-context', 'uid-west', 1)
    plan = _plan((east, west))
    preflight_contexts = []

    @contextlib.contextmanager
    def physical_fence(context_name, _physical_uid, **_kwargs):
        preflight_contexts.append(context_name)
        if context_name == 'east-context':
            raise (replica_managers.exceptions.
                   KubernetesPhysicalClusterIdentityError('retargeted context'))
        yield

    launched = replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
        replica_id=91,
        planned_capacity=1,
        funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    replica_managers.reserved_capacity_broker,
                    'current_epoch',
                    return_value=23), mock.patch.object(
                        replica_managers.kubernetes_adaptor,
                        'physical_cluster_uid_fence',
                        side_effect=physical_fence), mock.patch.object(
                            manager,
                            '_scale_up_one_locked',
                            return_value=launched) as scale_one:
        receipt = manager.accept_reserved_fill(plan)

    receipt.validate_for_plan(plan)
    assert set(preflight_contexts) == {'east-context', 'west-context'}
    assert receipt.accepted == (reserved_fill_planner.AcceptedFillIntent(
        plan.intents[1].idempotency_key, 91),)
    assert [item.intent for item in receipt.deferred] == [plan.intents[0]]
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.PHYSICAL_CLUSTER_UID_MISMATCH)
    assert not receipt.authority_current
    scale_one.assert_called_once()


def test_stale_first_intent_does_not_starve_fresh_tail() -> None:
    manager = _manager(maximum=1)
    east = _snapshot('east-context', 'uid-east', 1, valid_until=time.time() - 1)
    west = _snapshot('west-context', 'uid-west', 1)
    plan = _plan((east, west))
    launched = replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
        replica_id=92,
        planned_capacity=1,
        funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    replica_managers.reserved_capacity_broker,
                    'current_epoch',
                    return_value=23), mock.patch.object(
                        replica_managers.kubernetes_adaptor,
                        'physical_cluster_uid_fence',
                        side_effect=lambda *_args, **_kwargs: _physical_fence(
                        )), mock.patch.object(manager,
                                              '_scale_up_one_locked',
                                              return_value=launched):
        receipt = manager.accept_reserved_fill(plan)

    assert receipt.accepted == (reserved_fill_planner.AcceptedFillIntent(
        plan.intents[1].idempotency_key, 92),)
    assert [item.intent for item in receipt.deferred] == [plan.intents[0]]
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.STALE_OBSERVATION)
    assert not receipt.authority_current


def test_changed_epoch_first_intent_does_not_starve_current_tail() -> None:
    manager = _manager(maximum=1)
    east = _snapshot('east-context', 'uid-east', 1)
    west = _snapshot('west-context', 'uid-west', 1)
    plan = _plan((east, west))
    current_by_pool = {
        east.pool_key: 24,
        west.pool_key: 23,
    }
    launched = replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
        replica_id=93,
        planned_capacity=1,
        funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    replica_managers.reserved_capacity_broker,
                    'current_epoch',
                    side_effect=lambda pool_key: current_by_pool[pool_key]), \
            mock.patch.object(
                replica_managers.kubernetes_adaptor,
                'physical_cluster_uid_fence',
                side_effect=lambda *_args, **_kwargs: _physical_fence()), \
            mock.patch.object(manager,
                              '_scale_up_one_locked',
                              return_value=launched):
        receipt = manager.accept_reserved_fill(plan)

    assert receipt.accepted == (reserved_fill_planner.AcceptedFillIntent(
        plan.intents[1].idempotency_key, 93),)
    assert [item.intent for item in receipt.deferred] == [plan.intents[0]]
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.CHANGED_EPOCH)
    assert not receipt.authority_current


def test_provider_target_failure_is_sparse_but_provider_root_stops_tail(
) -> None:
    manager = _manager(maximum=2)
    east = _snapshot('east-context', 'uid-east', 1)
    west = _snapshot('west-context', 'uid-west', 1)
    plan = _plan((east, west))
    target_busy = (
        replica_managers.exceptions.KubernetesPhysicalClusterFenceBusyError(
            'capture disappeared', 'east-context', 0))
    launched = replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
        replica_id=94,
        planned_capacity=1,
        funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access
    common_patches = (
        mock.patch.object(replica_managers.serve_state,
                          'get_service_controller_owner',
                          return_value=_owner_record()),
        mock.patch.object(replica_managers.serve_state,
                          'get_replica_infos',
                          return_value=[]),
        mock.patch.object(replica_managers.reserved_capacity_broker,
                          'current_epoch',
                          return_value=23),
        mock.patch.object(
            replica_managers.kubernetes_adaptor,
            'physical_cluster_uid_fence',
            side_effect=lambda *_args, **_kwargs: _physical_fence()),
    )
    with contextlib.ExitStack() as stack:
        for patcher in common_patches:
            stack.enter_context(patcher)
        stack.enter_context(
            mock.patch.object(manager,
                              '_scale_up_one_locked',
                              side_effect=(target_busy, launched)))
        receipt = manager.accept_reserved_fill(plan)

    assert receipt.accepted == (reserved_fill_planner.AcceptedFillIntent(
        plan.intents[1].idempotency_key, 94),)
    assert [item.intent for item in receipt.deferred] == [plan.intents[0]]
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.PROVIDER_QUEUE_BACKPRESSURE)
    assert receipt.authority_current

    manager = _manager(maximum=2)
    with contextlib.ExitStack() as stack:
        for patcher in (
                mock.patch.object(replica_managers.serve_state,
                                  'get_service_controller_owner',
                                  return_value=_owner_record()),
                mock.patch.object(replica_managers.serve_state,
                                  'get_replica_infos',
                                  return_value=[]),
                mock.patch.object(replica_managers.reserved_capacity_broker,
                                  'current_epoch',
                                  return_value=23),
                mock.patch.object(
                    replica_managers.kubernetes_adaptor,
                    'physical_cluster_uid_fence',
                    side_effect=lambda *_args, **_kwargs: _physical_fence()),
                mock.patch.object(manager,
                                  '_scale_up_one_locked',
                                  side_effect=replica_managers.exceptions.
                                  ProviderPhaseBusyError('root retired'))):
            stack.enter_context(patcher)
        receipt = manager.accept_reserved_fill(plan)

    assert not receipt.accepted
    assert [item.intent for item in receipt.deferred] == list(plan.intents)
    assert {item.reason for item in receipt.deferred} == {
        reserved_fill_planner.DeferredFillReason.PROVIDER_QUEUE_BACKPRESSURE
    }


def test_accepted_row_spends_exact_headroom_and_defers_global_tail() -> None:
    manager = _manager(maximum=1)
    plan = _plan((_snapshot('east-context', 'uid-east', 3),))
    launched = replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
        replica_id=95,
        planned_capacity=1,
        funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access
    with mock.patch.object(replica_managers.serve_state,
                           'get_service_controller_owner',
                           return_value=_owner_record()), mock.patch.object(
                               replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), mock.patch.object(
                                   replica_managers.reserved_capacity_broker,
                                   'current_epoch',
                                   return_value=23), mock.patch.object(
                                       replica_managers.kubernetes_adaptor,
                                       'physical_cluster_uid_fence',
                                       side_effect=lambda *_args, **_kwargs:
                                       _physical_fence()), mock.patch.object(
                                           manager,
                                           '_scale_up_one_locked',
                                           return_value=launched) as scale_one:
        receipt = manager.accept_reserved_fill(plan)

    assert receipt.accepted == (reserved_fill_planner.AcceptedFillIntent(
        plan.intents[0].idempotency_key, 95),)
    assert [item.intent for item in receipt.deferred] == list(plan.intents[1:])
    assert {item.reason for item in receipt.deferred} == {
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED
    }
    scale_one.assert_called_once()


def test_newer_pending_version_defers_without_provider_preflight() -> None:
    manager = _manager()
    manager._pending_version = 20  # pylint: disable=protected-access
    plan = _plan((_snapshot('east-context', 'uid-east', 2),))
    with mock.patch.object(replica_managers.serve_state,
                           'get_service_controller_owner',
                           return_value=_owner_record()), mock.patch.object(
                               replica_managers.kubernetes_adaptor,
                               'physical_cluster_uid_fence') as physical_fence:
        receipt = manager.accept_reserved_fill(plan)

    physical_fence.assert_not_called()
    assert not receipt.accepted
    assert [item.intent for item in receipt.deferred] == list(plan.intents)
    assert {item.reason for item in receipt.deferred
           } == {reserved_fill_planner.DeferredFillReason.SUPERSEDED_POLICY}
    assert not receipt.authority_current


@pytest.mark.parametrize('pending_version', [18, 19])
def test_equal_or_older_pending_version_does_not_fence_plan(
        pending_version) -> None:
    manager = _manager()
    manager._pending_version = pending_version  # pylint: disable=protected-access
    plan = _plan((_snapshot('east-context', 'uid-east', 1),))
    launched = replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
        replica_id=96,
        planned_capacity=1,
        funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    replica_managers.reserved_capacity_broker,
                    'current_epoch',
                    return_value=23), mock.patch.object(
                        replica_managers.kubernetes_adaptor,
                        'physical_cluster_uid_fence',
                        side_effect=lambda *_args, **_kwargs: _physical_fence(
                        )), mock.patch.object(manager,
                                              '_scale_up_one_locked',
                                              return_value=launched):
        receipt = manager.accept_reserved_fill(plan)

    assert receipt.accepted == (reserved_fill_planner.AcceptedFillIntent(
        plan.intents[0].idempotency_key, 96),)
    assert not receipt.deferred
    assert receipt.authority_current


def test_newer_pending_version_after_first_persist_stops_global_tail() -> None:
    manager = _manager(maximum=3)
    plan = _plan((_snapshot('east-context', 'uid-east', 3),))
    launch_calls = 0

    def accept_first(*_args, **_kwargs):
        nonlocal launch_calls
        launch_calls += 1
        manager._pending_version = 20  # pylint: disable=protected-access
        return replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
            replica_id=97,
            planned_capacity=1,
            funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access

    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    replica_managers.reserved_capacity_broker,
                    'current_epoch',
                    return_value=23), mock.patch.object(
                        replica_managers.kubernetes_adaptor,
                        'physical_cluster_uid_fence',
                        side_effect=lambda *_args, **_kwargs: _physical_fence(
                        )), mock.patch.object(manager,
                                              '_scale_up_one_locked',
                                              side_effect=accept_first):
        receipt = manager.accept_reserved_fill(plan)

    assert launch_calls == 1
    assert receipt.accepted == (reserved_fill_planner.AcceptedFillIntent(
        plan.intents[0].idempotency_key, 97),)
    assert [item.intent for item in receipt.deferred] == list(plan.intents[1:])
    assert {item.reason for item in receipt.deferred
           } == {reserved_fill_planner.DeferredFillReason.SUPERSEDED_POLICY}
    assert not receipt.authority_current


def test_partial_admission_reports_exact_prefix_and_every_tail() -> None:
    manager = _manager()
    plan = _plan((_snapshot('east-context', 'uid-east', 3),))
    launch_results = [
        replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
            replica_id=71,
            planned_capacity=1,
            funding=replica_managers._ReplicaLaunchFunding.ZERO_COST),  # pylint: disable=protected-access
        None,
    ]
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    replica_managers.reserved_capacity_broker,
                    'current_epoch',
                    return_value=23), mock.patch.object(
                        replica_managers.kubernetes_adaptor,
                        'physical_cluster_uid_fence',
                        side_effect=lambda *args, **kwargs: _physical_fence(
                        )), mock.patch.object(manager,
                                              '_scale_up_one_locked',
                                              side_effect=launch_results):
        receipt = manager.accept_reserved_fill(plan)

    receipt.validate_for_plan(plan)
    assert receipt.accepted == (reserved_fill_planner.AcceptedFillIntent(
        plan.intents[0].idempotency_key, 71),)
    assert [item.intent for item in receipt.deferred] == list(plan.intents[1:])
    assert {item.reason for item in receipt.deferred} == {
        reserved_fill_planner.DeferredFillReason.ADMISSION_SEQUENCE_CHANGED
    }
    assert not receipt.authority_current


def test_changed_epoch_after_prefix_marks_authority_stale() -> None:
    manager = _manager()
    plan = _plan((_snapshot('east-context', 'uid-east', 2),))
    accepted = replica_managers._ReplicaLaunchResult(
        replica_id=81,
        planned_capacity=1,
        funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    replica_managers.reserved_capacity_broker,
                    'current_epoch',
                    side_effect=(23, 24)), mock.patch.object(
                        replica_managers.kubernetes_adaptor,
                        'physical_cluster_uid_fence',
                        side_effect=lambda *args, **kwargs: _physical_fence(
                        )), mock.patch.object(manager,
                                              '_scale_up_one_locked',
                                              side_effect=(accepted, None)):
        receipt = manager.accept_reserved_fill(plan)

    assert receipt.accepted[0].replica_id == 81
    assert [item.intent for item in receipt.deferred] == [plan.intents[1]]
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.CHANGED_EPOCH)
    assert not receipt.authority_current


def test_owner_mismatch_defers_without_provider_preflight() -> None:
    manager = _manager()
    plan = _plan((_snapshot('east-context', 'uid-east', 2),))
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record(controller_pid=99)), mock.patch.object(
                replica_managers.kubernetes_adaptor,
                'physical_cluster_uid_fence') as physical_fence:
        receipt = manager.accept_reserved_fill(plan)

    physical_fence.assert_not_called()
    assert not receipt.accepted
    assert len(receipt.deferred) == len(plan.intents)
    assert {item.reason for item in receipt.deferred
           } == {reserved_fill_planner.DeferredFillReason.LOST_OWNER}
    assert not receipt.authority_current


def test_zero_headroom_skips_preflight_but_partial_headroom_preflights_all(
) -> None:
    physical_manager = _manager(maximum=2)
    physical_plan = _plan((_snapshot('east-context', 'uid-east', 2),))
    occupying = [
        SimpleNamespace(replica_id=index, is_terminal=False, planned_capacity=1)
        for index in (1, 2)
    ]
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=occupying), mock.patch.object(
                    replica_managers.kubernetes_adaptor,
                    'physical_cluster_uid_fence') as physical_fence:
        receipt = physical_manager.accept_reserved_fill(physical_plan)
    physical_fence.assert_not_called()
    assert {item.reason for item in receipt.deferred} == {
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED
    }

    logical_manager = _manager(maximum=3, logical=True)
    logical_plan = _plan(
        (_snapshot('east-context', 'uid-east', 1, accelerator_count=2),),
        capacity_unit=reserved_fill_planner.FillCapacityUnit.LOGICAL)
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[
                    SimpleNamespace(replica_id=1,
                                    is_terminal=False,
                                    planned_capacity=2)
                ]), mock.patch.object(
                    replica_managers.reserved_capacity_broker,
                    'current_epoch',
                    return_value=23), mock.patch.object(
                        replica_managers.kubernetes_adaptor,
                        'physical_cluster_uid_fence') as physical_fence:
        receipt = logical_manager.accept_reserved_fill(logical_plan)
    physical_fence.assert_called_once_with('east-context',
                                           'uid-east',
                                           wait_for_initializer=False)
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)


@pytest.mark.parametrize('cleanup_status', [
    serve_state.ReplicaStatus.SHUTTING_DOWN,
    serve_state.ReplicaStatus.FAILED_CLEANUP,
])
def test_cleanup_unproven_drainer_still_consumes_service_headroom(
        cleanup_status) -> None:
    manager = _manager(maximum=1)
    plan = _plan((_snapshot('east-context', 'uid-east', 1),))
    down_status = (common_utils.ProcessStatus.RUNNING
                   if cleanup_status is serve_state.ReplicaStatus.SHUTTING_DOWN
                   else common_utils.ProcessStatus.FAILED)
    drainer = SimpleNamespace(
        replica_id=1,
        is_terminal=True,
        status=cleanup_status,
        status_property=SimpleNamespace(sky_down_status=down_status),
        planned_capacity=1)
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[drainer]), mock.patch.object(
                    replica_managers.kubernetes_adaptor,
                    'physical_cluster_uid_fence') as physical_fence:
        receipt = manager.accept_reserved_fill(plan)

    physical_fence.assert_not_called()
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.MAX_REPLICAS_EXHAUSTED)


def test_cleanup_proven_terminal_row_releases_service_headroom() -> None:
    manager = _manager(maximum=1)
    plan = _plan((_snapshot('east-context', 'uid-east', 1),))
    cleaned = SimpleNamespace(
        replica_id=1,
        is_terminal=True,
        status=serve_state.ReplicaStatus.FAILED,
        status_property=SimpleNamespace(
            sky_down_status=common_utils.ProcessStatus.SUCCEEDED),
        planned_capacity=1)

    with mock.patch.object(replica_managers.serve_state,
                           'get_replica_infos',
                           return_value=[cleaned]):
        headroom, infos, used_ids = (
            manager._reserved_fill_fleet_headroom_locked(plan))

    assert headroom == 1
    assert infos == [cleaned]
    assert used_ids == {1}


def test_same_allocation_drainer_is_debited_until_cleanup_proven() -> None:
    snapshot = _snapshot('east-context', 'uid-east', 1)
    allocation = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=19,
        ordinary_zero_cost_admission_sequence_high_water=(
            snapshot.ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=(snapshot,))
    location = snapshot.locations[0].to_location()
    drainer = SimpleNamespace(
        reserved_fill=True,
        is_terminal=True,
        status=serve_state.ReplicaStatus.SHUTTING_DOWN,
        status_property=SimpleNamespace(
            sky_down_status=common_utils.ProcessStatus.RUNNING),
        reserved_fill_allocation_generation=allocation.allocation_generation,
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
        reserved_fill_worker_projection_sha256=(_WORKER_PROJECTION_SHA256),
        reserved_fill_pool_key=snapshot.pool_key,
        reserved_fill_service_generation=snapshot.service_generation,
        reserved_fill_physical_cluster_uid=snapshot.physical_cluster_uid,
        get_spot_location=lambda: location)

    debits = controller.SkyServeController._committed_reserved_fill_debits(
        allocation, [drainer])

    assert len(debits) == 1
    assert debits[0].replica_slots == 1
    assert not _plan((snapshot,), committed_fill_debits=debits).intents

    drainer.status_property.sky_down_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    released_debits = (
        controller.SkyServeController._committed_reserved_fill_debits(
            allocation, [drainer]))

    assert not released_debits
    assert len(
        _plan((snapshot,), committed_fill_debits=released_debits).intents) == 1


def test_committed_and_pending_fill_debits_are_coalesced() -> None:
    snapshot = _snapshot('east-context', 'uid-east', 4)
    allocation = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=19,
        ordinary_zero_cost_admission_sequence_high_water=(
            snapshot.ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=(snapshot,))
    committed = reserved_fill_planner.CommittedFillDebit(
        allocation_generation=allocation.allocation_generation,
        allocation_input_sha256=allocation.allocation_input_sha256,
        allocation_claim_generation=allocation.allocation_claim_generation,
        pool_key=snapshot.pool_key,
        accelerator='A100',
        replica_slots=1)
    pending = dataclasses.replace(committed, replica_slots=2)

    debits = (
        controller.SkyServeController._coalesce_committed_reserved_fill_debits(
            (committed,), (pending,)))

    assert len(debits) == 1
    assert debits[0].accelerator == 'a100'
    assert debits[0].replica_slots == 3
    assert len(_plan((snapshot,), committed_fill_debits=debits).intents) == 1


def test_committed_fill_debits_do_not_coalesce_across_allocations() -> None:
    snapshot = _snapshot('east-context', 'uid-east', 2)
    allocation = reserved_fill_planner.AuthenticatedAllocationMap.create(
        allocation_generation=5,
        allocation_claim_generation=11,
        service_version=19,
        ordinary_zero_cost_admission_sequence_high_water=(
            snapshot.ordinary_zero_cost_admission_sequence),
        reconciliation_gate_generation=_RECONCILIATION_GATE_GENERATION,
        reclaim_fleet_bundle_sha256=_RECLAIM_FLEET_BUNDLE_SHA256,
        reclaim_policy_revision=_RECLAIM_POLICY_REVISION,
        reclaim_provider_inventory_sha256=(_RECLAIM_PROVIDER_INVENTORY_SHA256),
        pool_snapshots=(snapshot,))
    current = reserved_fill_planner.CommittedFillDebit(
        allocation_generation=allocation.allocation_generation,
        allocation_input_sha256=allocation.allocation_input_sha256,
        allocation_claim_generation=allocation.allocation_claim_generation,
        pool_key=snapshot.pool_key,
        accelerator='a100',
        replica_slots=1)
    stale = dataclasses.replace(current, allocation_generation=6)

    debits = (
        controller.SkyServeController._coalesce_committed_reserved_fill_debits(
            (current,), (stale,)))

    assert len(debits) == 2
    with pytest.raises(ValueError, match='different authenticated'):
        _plan((snapshot,), committed_fill_debits=debits)


def test_stale_observation_is_locally_deferred_after_parallel_preflight(
) -> None:
    manager = _manager()
    stale = _snapshot('east-context',
                      'uid-east',
                      1,
                      valid_until=time.time() - 1.0)
    plan = _plan((stale,))
    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                replica_managers.reserved_capacity_broker,
                'current_epoch',
                return_value=23), mock.patch.object(
                    replica_managers.kubernetes_adaptor,
                    'physical_cluster_uid_fence',
                    side_effect=lambda *_args, **_kwargs: _physical_fence()), \
            mock.patch.object(
                        replica_managers.provider_phase,
                        'try_provider_phase',
                        wraps=replica_managers.provider_phase.
                        try_provider_phase) as provider_admission:
        receipt = manager.accept_reserved_fill(plan)

    provider_admission.assert_called_once()
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.STALE_OBSERVATION)
    assert not receipt.authority_current


def test_rejects_a_tampered_typed_plan_at_boundary() -> None:
    manager = _manager()
    plan = _plan((_snapshot('east-context', 'uid-east', 1),))
    tampered_intent = dataclasses.replace(plan.intents[0])
    object.__setattr__(tampered_intent, 'physical_cluster_uid', 'wrong-uid')
    tampered_plan = dataclasses.replace(plan, intents=(tampered_intent,))

    with pytest.raises(ValueError, match='does not match'):
        manager.accept_reserved_fill(tampered_plan)
