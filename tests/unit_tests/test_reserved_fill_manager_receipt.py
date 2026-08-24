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
from sky.server.requests import reserved_fill_admission
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
            binding_epoch=2,
            non_pool_capable=True,
            non_pool_binding_protocol_version=(
                ordinary_launch_binding.NON_POOL_BINDING_PROTOCOL_VERSION),
            non_pool_profile_set_digest=(
                ordinary_launch_binding.supported_non_pool_profile_set_digest()
            ),
            non_pool_capability_cohort_epoch=(
                ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH),
            non_pool_receipt_protocol_version=(
                ordinary_launch_binding.NON_POOL_RECEIPT_PROTOCOL_VERSION)))
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
    allocation_repository = mock.Mock()
    allocation_repository.read_current.return_value = SimpleNamespace(
        allocation_generation=plan.allocation_generation,
        allocation_input_sha256=plan.allocation_input_sha256,
        allocation_claim_generation=plan.allocation_claim_generation)

    with mock.patch.object(
            replica_managers.serve_state,
            'get_service_controller_owner',
            return_value=_owner_record()), mock.patch.object(
                replica_managers.reserved_fill_allocation,
                'ReservedFillAllocationRepository',
                return_value=allocation_repository), mock.patch.object(
                    replica_managers.provider_phase, 'try_provider_phase'
                ) as provider_admission, mock.patch.object(
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
    allocation_repository.read_current.assert_called_once_with(
        'svc', _SERVICE_HASH, (_CONTROLLER_PID, _CONTROLLER_IP))
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


def test_durable_accept_previous_capability_cohort_fails_before_grant() -> None:
    manager = _manager(maximum=7)
    manager._reserved_fill_actuation_mode = (  # pylint: disable=protected-access
        zero_cost_actuation.ActuationMode.DURABLE_INTENT)
    manager._ordinary_launch_binding_authority = dataclasses.replace(  # pylint: disable=protected-access
        manager._ordinary_launch_binding_authority,
        non_pool_capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH - 1))
    plan = _plan((_snapshot('east-context', 'uid-east', 1),))
    repository = mock.Mock()
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access

    with mock.patch.object(replica_managers.serve_state,
                           'get_service_controller_owner') as owner_read, \
         mock.patch.object(replica_managers.provider_phase,
                           'try_provider_phase') as provider_admission:
        receipt = manager.accept_reserved_fill(plan)

    assert not receipt.accepted
    assert receipt.authority_current is False
    assert len(receipt.deferred) == 1
    assert receipt.deferred[0].reason is (
        reserved_fill_planner.DeferredFillReason.LOST_OWNER)
    assert receipt.deferred[0].detail == (
        'the current generic launch capability cohort is unavailable')
    repository.grant_plan.assert_not_called()
    owner_read.assert_not_called()
    provider_admission.assert_not_called()


def test_previous_cohort_intent_retries_before_provider_or_materialization(
) -> None:
    manager = _manager()
    manager._reserved_fill_actuation_mode = (  # pylint: disable=protected-access
        zero_cost_actuation.ActuationMode.DURABLE_INTENT)
    manager._ordinary_launch_binding_authority = dataclasses.replace(  # pylint: disable=protected-access
        manager._ordinary_launch_binding_authority,
        non_pool_capability_cohort_epoch=(
            ordinary_launch_binding.NON_POOL_CAPABILITY_COHORT_EPOCH - 1))
    manager._zero_cost_actuation_executor_id = (  # pylint: disable=protected-access
        mock.sentinel.executor_id)
    intent = _plan((_snapshot('east-context', 'uid-east', 1),)).intents[0]
    lease = SimpleNamespace(intent=intent)
    repository = mock.Mock()
    repository.lease_batch.return_value = (lease,)
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access

    with mock.patch.object(replica_managers.provider_phase,
                           'try_provider_phase') as provider_admission, \
         mock.patch.object(
             manager, '_start_reserved_fill_physical_preflights') as preflight, \
         mock.patch.object(manager, '_scale_up_one_locked') as materialize:
        manager._actuate_zero_cost_pool(intent.pool_key)

    repository.release_retryable.assert_called_once_with(
        lease, 'controller_authority_unavailable')
    repository.terminate.assert_not_called()
    provider_admission.assert_not_called()
    preflight.assert_not_called()
    materialize.assert_not_called()


def test_current_cohort_intent_has_pre_provider_actuation_authority() -> None:
    manager = _manager()
    manager._reserved_fill_actuation_mode = (  # pylint: disable=protected-access
        zero_cost_actuation.ActuationMode.DURABLE_INTENT)
    manager._update_recovery_required = False  # pylint: disable=protected-access
    manager._ownership_lost = threading.Event()  # pylint: disable=protected-access
    intent = _plan((_snapshot('east-context', 'uid-east', 1),)).intents[0]

    with mock.patch.object(replica_managers.serve_state,
                           'get_service_controller_owner',
                           return_value=_owner_record()):
        assert manager._zero_cost_actuation_authority_current(intent)  # pylint: disable=protected-access


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
    repository.lease_batch.side_effect = (lambda **kwargs:
                                          (leases[kwargs['pool_key']],))
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
    assert repository.lease_batch.call_count == 2
    repository.release_retryable.assert_not_called()
    repository.terminate.assert_not_called()


def test_durable_pool_executor_leases_one_just_in_time_quantum() -> None:
    manager = _manager()

    class RecordingLock:
        """Record materializations performed in each critical section."""

        def __init__(self) -> None:
            self._lock = threading.Lock()
            self.chunk_sizes: list[int] = []
            self._current_chunk_size = 0

        def __enter__(self):
            self._lock.acquire()
            self._current_chunk_size = 0
            return self

        def record_materialization(self) -> None:
            self._current_chunk_size += 1

        def __exit__(self, *_args) -> None:
            self.chunk_sizes.append(self._current_chunk_size)
            self._lock.release()

    recording_lock = RecordingLock()
    manager.lock = recording_lock
    manager._workspace = 'workspace-a'  # pylint: disable=protected-access
    manager._zero_cost_actuation_executor_id = (  # pylint: disable=protected-access
        mock.sentinel.executor_id)
    manager._scale_reconciliation_event = threading.Event()  # pylint: disable=protected-access
    lease_batch_size = replica_managers._ZERO_COST_ACTUATION_QUANTUM
    plan = _plan((_snapshot('east-context', 'uid-east', lease_batch_size),))
    pool_key = plan.intents[0].pool_key
    lane_event = mock.Mock(spec=threading.Event)
    manager._zero_cost_actuation_event = lane_event  # pylint: disable=protected-access
    manager._zero_cost_actuation_lane_lock = threading.Lock()  # pylint: disable=protected-access
    manager._zero_cost_actuation_lanes = {  # pylint: disable=protected-access
        pool_key: threading.current_thread()
    }

    def assert_lane_released_before_signal() -> None:
        assert pool_key not in manager._zero_cost_actuation_lanes  # pylint: disable=protected-access

    lane_event.set.side_effect = assert_lane_released_before_signal
    leases = tuple(SimpleNamespace(intent=intent) for intent in plan.intents)
    repository = mock.Mock()
    repository.lease_batch.return_value = leases
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access
    preflights = SimpleNamespace(
        preflights={('east-context', 'uid-east'): SimpleNamespace(error=None)})

    next_replica_id = 100

    def materialize(_resources_override, _used_replica_ids, *_args, **_kwargs):
        nonlocal next_replica_id
        recording_lock.record_materialization()
        replica_id = next_replica_id
        next_replica_id += 1
        return replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
            replica_id=replica_id,
            planned_capacity=1,
            funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access

    with mock.patch.object(
            manager,
            '_zero_cost_actuation_authority_current',
            return_value=True), mock.patch.object(
                replica_managers.provider_phase,
                'try_provider_phase',
                return_value=contextlib.nullcontext(mock.sentinel.admission)), \
            mock.patch.object(
                manager,
                '_start_reserved_fill_physical_preflights',
                return_value=preflights) as start_preflights, mock.patch.object(
                    manager,
                    '_release_reserved_fill_physical_preflights') as release, \
            mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]) as get_replica_infos, mock.patch.object(
                    manager,
                    '_scale_up_one_locked',
                    side_effect=materialize) as scale_one:
        manager._actuate_zero_cost_pool(pool_key)

    repository.lease_batch.assert_called_once_with(
        service_name='svc',
        pool_key=pool_key,
        owner=mock.sentinel.executor_id,
        lease_seconds=mock.ANY,
        max_leases=lease_batch_size)
    start_preflights.assert_called_once_with(
        tuple(intent for intent in plan.intents), mock.sentinel.admission,
        'workspace-a')
    release.assert_called_once_with(preflights)
    assert scale_one.call_count == lease_batch_size
    assert recording_lock.chunk_sizes == [lease_batch_size]
    assert get_replica_infos.call_count == len(recording_lock.chunk_sizes)
    repository.release_retryable.assert_not_called()
    repository.terminate.assert_not_called()
    lane_event.set.assert_called_once_with()
    assert pool_key not in manager._zero_cost_actuation_lanes  # pylint: disable=protected-access


def test_durable_pool_executor_bounds_manager_lock_to_one_quantum() -> None:
    manager = _manager()
    start_second_pool = threading.Event()
    second_pool_waiting = threading.Event()
    second_pool_acquired = threading.Event()

    class InterleavingLock:
        """Force the waiting second pool to acquire after the first chunk."""

        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._owner = ''
            self._current_chunk_size = 0
            self._waited_after_first_chunk = False
            self.second_pool_interleaved = False
            self.chunks: list[tuple[str, int]] = []

        def __enter__(self):
            thread_name = threading.current_thread().name
            if thread_name == 'actuation-pool-b':
                second_pool_waiting.set()
            self._lock.acquire()
            self._owner = thread_name
            self._current_chunk_size = 0
            if thread_name == 'actuation-pool-b':
                second_pool_acquired.set()
            return self

        def record_materialization(self) -> None:
            self._current_chunk_size += 1

        def __exit__(self, *_args) -> None:
            owner = self._owner
            self.chunks.append((owner, self._current_chunk_size))
            wait_for_second_pool = (owner == 'actuation-pool-a' and
                                    not self._waited_after_first_chunk)
            if wait_for_second_pool:
                self._waited_after_first_chunk = True
            self._lock.release()
            if wait_for_second_pool:
                self.second_pool_interleaved = second_pool_acquired.wait(
                    timeout=2)

    interleaving_lock = InterleavingLock()
    manager.lock = interleaving_lock
    manager._workspace = 'workspace-a'  # pylint: disable=protected-access
    manager._zero_cost_actuation_executor_id = (  # pylint: disable=protected-access
        mock.sentinel.executor_id)
    manager._scale_reconciliation_event = threading.Event()  # pylint: disable=protected-access
    manager._zero_cost_actuation_event = threading.Event()  # pylint: disable=protected-access
    manager._zero_cost_actuation_lane_lock = threading.Lock()  # pylint: disable=protected-access
    manager._zero_cost_actuation_lanes = {}  # pylint: disable=protected-access
    first_pool_size = replica_managers._ZERO_COST_ACTUATION_QUANTUM
    plan = _plan(
        (_snapshot('east-context', 'uid-east',
                   first_pool_size), _snapshot('west-context', 'uid-west', 1)))
    leases_by_pool: dict[str, tuple[SimpleNamespace, ...]] = {}
    for intent in plan.intents:
        leases_by_pool.setdefault(intent.pool_key, tuple())
        leases_by_pool[intent.pool_key] += (SimpleNamespace(intent=intent),)
    first_pool_key = reserved_capacity_broker.make_pool_key(
        'east-context',
        'A100',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='uid-east')
    second_pool_key = reserved_capacity_broker.make_pool_key(
        'west-context',
        'A100',
        protocol_version=reserved_capacity_broker.PROTOCOL_V2,
        physical_cluster_uid='uid-west')
    assert len(leases_by_pool[first_pool_key]) == first_pool_size
    assert len(leases_by_pool[second_pool_key]) == 1
    repository = mock.Mock()
    repository.lease_batch.side_effect = (
        lambda **kwargs: leases_by_pool[kwargs['pool_key']])
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access
    launch_order: list[str] = []
    next_replica_id = 200
    second_pool_was_waiting: list[bool] = []

    def start_preflights(intents, _admission, _workspace):
        return SimpleNamespace(
            preflights={
                (intent.allowed_locations[0].region, intent.physical_cluster_uid):
                    SimpleNamespace(error=None) for intent in intents
            })

    def materialize(resources_override, _used_replica_ids, *_args, **_kwargs):
        nonlocal next_replica_id
        interleaving_lock.record_materialization()
        physical_uid = resources_override[
            replica_managers.serve_constants.
            RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY]
        launch_order.append(physical_uid)
        if (physical_uid == 'uid-east' and launch_order.count('uid-east')
                == replica_managers._ZERO_COST_ACTUATION_QUANTUM):
            start_second_pool.set()
            second_pool_was_waiting.append(second_pool_waiting.wait(timeout=2))
        replica_id = next_replica_id
        next_replica_id += 1
        return replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
            replica_id=replica_id,
            planned_capacity=1,
            funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access

    worker_errors: list[BaseException] = []

    def run_pool(pool_key: str) -> None:
        try:
            manager._actuate_zero_cost_pool(pool_key)
        except BaseException as error:  # pylint: disable=broad-except
            worker_errors.append(error)

    def run_second_pool() -> None:
        if not start_second_pool.wait(timeout=2):
            worker_errors.append(TimeoutError('first pool did not yield'))
            return
        run_pool(second_pool_key)

    with mock.patch.object(
            manager,
            '_zero_cost_actuation_authority_current',
            return_value=True), mock.patch.object(
                replica_managers.provider_phase,
                'try_provider_phase',
                side_effect=lambda *_args, **_kwargs: contextlib.nullcontext(
                    mock.sentinel.admission)), mock.patch.object(
                        manager,
                        '_start_reserved_fill_physical_preflights',
                        side_effect=start_preflights) as start_preflight, \
            mock.patch.object(
                manager, '_release_reserved_fill_physical_preflights'), \
            mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    manager,
                    '_scale_up_one_locked',
                    side_effect=materialize):
        second_pool = threading.Thread(target=run_second_pool,
                                       name='actuation-pool-b')
        first_pool = threading.Thread(target=run_pool,
                                      args=(first_pool_key,),
                                      name='actuation-pool-a')
        second_pool.start()
        first_pool.start()
        first_pool.join(timeout=5)
        second_pool.join(timeout=5)

    assert not first_pool.is_alive()
    assert not second_pool.is_alive()
    assert not worker_errors
    assert second_pool_was_waiting == [True]
    assert interleaving_lock.second_pool_interleaved
    assert launch_order == ['uid-east'] * 4 + ['uid-west']
    assert interleaving_lock.chunks == [
        ('actuation-pool-a', 4),
        ('actuation-pool-b', 1),
    ]
    assert max(chunk_size for _, chunk_size in interleaving_lock.chunks) <= 4
    assert sorted(
        len(call.args[0]) for call in start_preflight.call_args_list) == [
            1, first_pool_size
        ]
    assert repository.lease_batch.call_count == 2
    assert all(call.kwargs['max_leases'] ==
               replica_managers._ZERO_COST_ACTUATION_QUANTUM
               for call in repository.lease_batch.call_args_list)


def test_dispatcher_hands_off_full_window_one_quantum_at_a_time() -> None:
    manager = _manager()
    manager.lock = threading.Lock()
    manager._workspace = 'workspace-a'  # pylint: disable=protected-access
    manager._zero_cost_actuation_executor_id = (  # pylint: disable=protected-access
        mock.sentinel.executor_id)
    manager._scale_reconciliation_event = threading.Event()  # pylint: disable=protected-access
    manager._zero_cost_actuation_lane_lock = threading.Lock()  # pylint: disable=protected-access
    manager._zero_cost_actuation_lanes = {}  # pylint: disable=protected-access

    window_size = zero_cost_actuation.MAX_ACTUATION_LEASE_BATCH_SIZE
    plan = _plan((_snapshot('east-context', 'uid-east', window_size),))
    pool_key = plan.intents[0].pool_key
    pending = list(plan.intents)
    repository_lock = threading.Lock()
    lease_calls: list[tuple[threading.Thread, tuple[SimpleNamespace, ...]]] = []
    first_lane: list[threading.Thread] = []
    second_lease_started = threading.Event()
    second_started_while_first_alive: list[bool] = []
    repository = mock.Mock()

    def actionable_pool_keys(**_kwargs):
        with repository_lock:
            return (pool_key,) if pending else ()

    def lease_batch(**kwargs):
        current = threading.current_thread()
        with repository_lock:
            intents = tuple(pending[:kwargs['max_leases']])
            del pending[:len(intents)]
        leases = tuple(SimpleNamespace(intent=intent) for intent in intents)
        lease_calls.append((current, leases))
        if not first_lane:
            first_lane.append(current)
        elif len(lease_calls) == 2:
            second_started_while_first_alive.append(first_lane[0].is_alive())
            second_lease_started.set()
        return leases

    repository.actionable_pool_keys.side_effect = actionable_pool_keys
    repository.lease_batch.side_effect = lease_batch
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access

    lane_absent_before_signal: list[bool] = []
    first_handoff_observed_second_lane: list[bool] = []

    class HandoffEvent(threading.Event):
        """Hold the first lane tail open while the dispatcher hands off."""

        def set(self) -> None:
            current = threading.current_thread()
            if first_lane and current is first_lane[0]:
                with manager._zero_cost_actuation_lane_lock:  # pylint: disable=protected-access
                    lane_absent_before_signal.append(
                        manager._zero_cost_actuation_lanes.get(pool_key)  # pylint: disable=protected-access
                        is not current)
                super().set()
                first_handoff_observed_second_lane.append(
                    second_lease_started.wait(timeout=2))
                return
            super().set()

    handoff_event = HandoffEvent()
    manager._zero_cost_actuation_event = handoff_event  # pylint: disable=protected-access
    stop_dispatcher = threading.Event()
    manager._manager_daemon_should_stop = stop_dispatcher.is_set  # pylint: disable=protected-access

    active_provider_phases = 0
    max_active_provider_phases = 0
    provider_phase_lock = threading.Lock()

    @contextlib.contextmanager
    def provider_admission(*_args, **_kwargs):
        nonlocal active_provider_phases, max_active_provider_phases
        with provider_phase_lock:
            active_provider_phases += 1
            max_active_provider_phases = max(max_active_provider_phases,
                                             active_provider_phases)
        try:
            yield mock.sentinel.admission
        finally:
            with provider_phase_lock:
                active_provider_phases -= 1

    processed: list[str] = []
    full_window_processed = threading.Event()

    def materialize(resources_override, _used_replica_ids, *_args, **_kwargs):
        processed.append(resources_override[
            replica_managers.serve_constants.
            RESERVED_FILL_INTENT_IDEMPOTENCY_KEY_OVERRIDE_KEY])
        if len(processed) == window_size:
            stop_dispatcher.set()
            full_window_processed.set()
        return replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
            replica_id=len(processed),
            planned_capacity=1,
            funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access

    def start_preflights(intents, _admission, _workspace):
        return SimpleNamespace(
            preflights={
                (intent.allowed_locations[0].region, intent.physical_cluster_uid):
                    SimpleNamespace(error=None) for intent in intents
            })

    dispatcher = threading.Thread(
        target=manager._zero_cost_actuation_dispatcher,
        name='zero-cost-dispatcher')  # pylint: disable=protected-access
    completed_full_window = False
    with mock.patch.object(
            manager,
            '_zero_cost_actuation_authority_current',
            return_value=True), mock.patch.object(
                zero_cost_actuation,
                'get_service_mode',
                return_value=zero_cost_actuation.ActuationMode.DURABLE_INTENT), \
            mock.patch.object(
                replica_managers.provider_phase,
                'try_provider_phase',
                side_effect=provider_admission), mock.patch.object(
                    manager,
                    '_start_reserved_fill_physical_preflights',
                    side_effect=start_preflights), mock.patch.object(
                        manager,
                        '_release_reserved_fill_physical_preflights'), \
            mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    manager,
                    '_scale_up_one_locked',
                    side_effect=materialize):
        dispatcher.start()
        try:
            completed_full_window = full_window_processed.wait(timeout=5)
            dispatcher.join(timeout=5)
        finally:
            if dispatcher.is_alive():
                stop_dispatcher.set()
                threading.Event.set(handoff_event)
                dispatcher.join(timeout=2)

    assert completed_full_window
    assert not dispatcher.is_alive()
    for lane, _ in lease_calls:
        lane.join(timeout=2)
        assert not lane.is_alive()
    assert repository.lease_batch.call_count == window_size // (
        replica_managers._ZERO_COST_ACTUATION_QUANTUM)
    assert all(call.kwargs['max_leases'] ==
               replica_managers._ZERO_COST_ACTUATION_QUANTUM
               for call in repository.lease_batch.call_args_list)
    assert [len(leases) for _, leases in lease_calls] == [4] * 8
    assert len({id(lane) for lane, _ in lease_calls}) == 8
    assert processed == [intent.idempotency_key for intent in plan.intents]
    assert not pending
    assert lane_absent_before_signal == [True]
    assert first_handoff_observed_second_lane == [True]
    assert second_started_while_first_alive == [True]
    assert max_active_provider_phases == 1
    assert pool_key not in manager._zero_cost_actuation_lanes  # pylint: disable=protected-access
    repository.release_retryable.assert_not_called()
    repository.terminate.assert_not_called()


def test_durable_pool_executor_continues_after_per_intent_ambiguity() -> None:
    manager = _manager()
    manager.lock = threading.Lock()
    manager._workspace = 'workspace-a'  # pylint: disable=protected-access
    manager._zero_cost_actuation_executor_id = (  # pylint: disable=protected-access
        mock.sentinel.executor_id)
    manager._scale_reconciliation_event = threading.Event()  # pylint: disable=protected-access
    manager._zero_cost_actuation_event = threading.Event()  # pylint: disable=protected-access
    plan = _plan((_snapshot('east-context', 'uid-east', 3),))
    leases = tuple(SimpleNamespace(intent=intent) for intent in plan.intents)
    repository = mock.Mock()
    repository.lease_batch.return_value = leases
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access
    preflights = SimpleNamespace(
        preflights={('east-context', 'uid-east'): SimpleNamespace(error=None)})
    result = replica_managers._ReplicaLaunchResult(  # pylint: disable=protected-access
        replica_id=101,
        planned_capacity=1,
        funding=replica_managers._ReplicaLaunchFunding.ZERO_COST)  # pylint: disable=protected-access

    with mock.patch.object(
            manager,
            '_zero_cost_actuation_authority_current',
            return_value=True), mock.patch.object(
                replica_managers.provider_phase,
                'try_provider_phase',
                return_value=contextlib.nullcontext(mock.sentinel.admission)), \
            mock.patch.object(
                manager,
                '_start_reserved_fill_physical_preflights',
                return_value=preflights), mock.patch.object(
                    manager,
                    '_release_reserved_fill_physical_preflights'), \
            mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    manager,
                    '_scale_up_one_locked',
                    side_effect=(reserved_fill_admission.AdmissionAmbiguousError(
                        'first acknowledgement lost'), result, result)) as scale_one:
        manager._actuate_zero_cost_pool(plan.intents[0].pool_key)

    assert scale_one.call_count == 3
    repository.release_retryable.assert_not_called()
    repository.terminate.assert_not_called()


def test_shared_preflight_failure_releases_every_uncommitted_batch_lease(
) -> None:
    manager = _manager()
    manager.lock = threading.Lock()
    manager._workspace = 'workspace-a'  # pylint: disable=protected-access
    manager._zero_cost_actuation_executor_id = (  # pylint: disable=protected-access
        mock.sentinel.executor_id)
    manager._scale_reconciliation_event = threading.Event()  # pylint: disable=protected-access
    manager._zero_cost_actuation_event = threading.Event()  # pylint: disable=protected-access
    plan = _plan((_snapshot('east-context', 'uid-east', 3),))
    leases = tuple(SimpleNamespace(intent=intent) for intent in plan.intents)
    repository = mock.Mock()
    repository.lease_batch.return_value = leases
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access
    preflights = SimpleNamespace(
        preflights={
            ('east-context', 'uid-east'): SimpleNamespace(
                error=replica_managers.exceptions.ProviderPhaseBusyError(
                    'physical fence busy'))
        })

    with mock.patch.object(
            manager,
            '_zero_cost_actuation_authority_current',
            return_value=True), mock.patch.object(
                replica_managers.provider_phase,
                'try_provider_phase',
                return_value=contextlib.nullcontext(mock.sentinel.admission)), \
            mock.patch.object(
                manager,
                '_start_reserved_fill_physical_preflights',
                return_value=preflights), mock.patch.object(
                    manager,
                    '_release_reserved_fill_physical_preflights'), \
            mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    manager, '_scale_up_one_locked') as scale_one:
        manager._actuate_zero_cost_pool(plan.intents[0].pool_key)

    assert repository.release_retryable.call_args_list == [
        mock.call(lease, 'ProviderPhaseBusyError') for lease in leases
    ]
    repository.terminate.assert_not_called()
    scale_one.assert_not_called()


def test_per_intent_physical_fence_busy_retries_without_terminalizing() -> None:
    manager = _manager()
    manager.lock = threading.Lock()
    manager._workspace = 'workspace-a'  # pylint: disable=protected-access
    manager._zero_cost_actuation_executor_id = (  # pylint: disable=protected-access
        mock.sentinel.executor_id)
    intent = _plan((_snapshot('east-context', 'uid-east', 1),)).intents[0]
    lease = SimpleNamespace(intent=intent)
    repository = mock.Mock()
    repository.lease_batch.return_value = (lease,)
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access
    preflights = SimpleNamespace(
        preflights={('east-context', 'uid-east'): SimpleNamespace(error=None)})
    busy = replica_managers.exceptions.KubernetesPhysicalClusterFenceBusyError(
        'another capture is active', 'east-context', 7)

    with mock.patch.object(
            manager,
            '_zero_cost_actuation_authority_current',
            return_value=True), mock.patch.object(
                replica_managers.provider_phase,
                'try_provider_phase',
                return_value=contextlib.nullcontext(mock.sentinel.admission)), \
            mock.patch.object(
                manager,
                '_start_reserved_fill_physical_preflights',
                return_value=preflights), mock.patch.object(
                    manager,
                    '_release_reserved_fill_physical_preflights'), \
            mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    manager, '_scale_up_one_locked', side_effect=busy):
        manager._actuate_zero_cost_pool(intent.pool_key)

    repository.release_retryable.assert_called_once_with(
        lease, 'KubernetesPhysicalClusterFenceBusyError')
    repository.terminate.assert_not_called()


def test_dispatcher_does_not_erase_publication_during_durable_scan() -> None:
    manager = _manager()
    manager._zero_cost_actuation_event = threading.Event()  # pylint: disable=protected-access
    manager._zero_cost_actuation_lane_lock = threading.Lock()  # pylint: disable=protected-access
    manager._zero_cost_actuation_lanes = {}  # pylint: disable=protected-access
    manager._manager_daemon_should_stop = mock.Mock(  # pylint: disable=protected-access
        side_effect=(False, True))
    repository = mock.Mock()

    def publish_while_scanning(**_kwargs):
        manager._zero_cost_actuation_event.set()  # pylint: disable=protected-access
        return ()

    repository.actionable_pool_keys.side_effect = publish_while_scanning
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access

    with mock.patch.object(
            zero_cost_actuation,
            'get_service_mode',
            return_value=zero_cost_actuation.ActuationMode.DURABLE_INTENT):
        manager._zero_cost_actuation_dispatcher()

    assert manager._zero_cost_actuation_event.is_set()  # pylint: disable=protected-access
    repository.actionable_pool_keys.assert_called_once_with(service_name='svc')


def test_ambiguous_atomic_admission_preserves_intent_without_cleanup() -> None:
    manager = _manager()
    manager.lock = threading.Lock()
    manager._workspace = 'workspace-a'  # pylint: disable=protected-access
    manager._zero_cost_actuation_executor_id = (  # pylint: disable=protected-access
        mock.sentinel.executor_id)
    manager._scale_reconciliation_event = threading.Event()  # pylint: disable=protected-access
    intent = _plan((_snapshot('east-context', 'uid-east', 1),)).intents[0]
    lease = SimpleNamespace(intent=intent)
    repository = mock.Mock()
    repository.lease_batch.return_value = (lease,)
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access
    preflights = SimpleNamespace(
        preflights={('east-context', 'uid-east'): SimpleNamespace(error=None)})
    provider_admission = contextlib.nullcontext(mock.sentinel.admission)

    with mock.patch.object(
            manager,
            '_zero_cost_actuation_authority_current',
            return_value=True), mock.patch.object(
                replica_managers.provider_phase,
                'try_provider_phase',
                return_value=provider_admission), \
            mock.patch.object(
                manager,
                '_start_reserved_fill_physical_preflights',
                return_value=preflights), mock.patch.object(
                    manager,
                    '_release_reserved_fill_physical_preflights') as release, \
            mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    manager,
                    '_scale_up_one_locked',
                    side_effect=(reserved_fill_admission.
                                 AdmissionAmbiguousError('commit unknown'))):
        manager._actuate_zero_cost_pool(intent.pool_key)

    repository.release_retryable.assert_not_called()
    repository.terminate.assert_not_called()
    release.assert_called_once_with(preflights)
    assert manager._scale_reconciliation_event.is_set()  # pylint: disable=protected-access


def test_rejected_atomic_admission_releases_intent_for_retry() -> None:
    manager = _manager()
    manager.lock = threading.Lock()
    manager._workspace = 'workspace-a'  # pylint: disable=protected-access
    manager._zero_cost_actuation_executor_id = (  # pylint: disable=protected-access
        mock.sentinel.executor_id)
    intent = _plan((_snapshot('east-context', 'uid-east', 1),)).intents[0]
    lease = SimpleNamespace(intent=intent)
    repository = mock.Mock()
    repository.lease_batch.return_value = (lease,)
    manager._zero_cost_actuation_repository = repository  # pylint: disable=protected-access
    preflights = SimpleNamespace(
        preflights={('east-context', 'uid-east'): SimpleNamespace(error=None)})

    with mock.patch.object(
            manager,
            '_zero_cost_actuation_authority_current',
            return_value=True), mock.patch.object(
                replica_managers.provider_phase,
                'try_provider_phase',
                return_value=contextlib.nullcontext(mock.sentinel.admission)), \
            mock.patch.object(
                manager,
                '_start_reserved_fill_physical_preflights',
                return_value=preflights), mock.patch.object(
                    manager,
                    '_release_reserved_fill_physical_preflights'), \
            mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos',
                return_value=[]), mock.patch.object(
                    manager, '_scale_up_one_locked', return_value=None):
        manager._actuate_zero_cost_pool(intent.pool_key)

    repository.release_retryable.assert_called_once_with(
        lease, 'replica_commit_deferred')
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


def test_rejects_a_tampered_typed_plan_at_boundary() -> None:
    manager = _manager()
    plan = _plan((_snapshot('east-context', 'uid-east', 1),))
    tampered_intent = dataclasses.replace(plan.intents[0])
    object.__setattr__(tampered_intent, 'physical_cluster_uid', 'wrong-uid')
    tampered_plan = dataclasses.replace(plan, intents=(tampered_intent,))

    with pytest.raises(ValueError, match='does not match'):
        manager.accept_reserved_fill(tampered_plan)
