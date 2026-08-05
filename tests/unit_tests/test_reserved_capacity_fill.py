"""Unit tests for the reserved-capacity fill overlay.

Opt-in (replica_policy.reserved_capacity_fill): the autoscaler additionally
scales up onto FREE zero-cost capacity reported by a controller poller,
bounded by max_replicas. The demand target and the controller's capacity
hint stay demand-only; every free-slot scale-up carries a sentinel override
that the launch path pins to zero-cost ACTIVE locations (skipping entirely
when none is available -- fill must never spill to paid capacity).
"""
# pylint: disable=protected-access
import contextlib
import dataclasses
import threading
import time
import types
import unittest
from unittest import mock

from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer as _make_placer

from sky import backends
from sky import clouds
from sky import exceptions
from sky.adaptors import kubernetes as kubernetes_adaptor
from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import provider_phase
from sky.serve import replica_managers
from sky.serve import reserved_capacity
from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import service_spec
from sky.serve import spot_placer
from sky.utils import locks

_SCALE_UP = autoscalers.AutoscalerDecisionOperator.SCALE_UP
_SCALE_DOWN = autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
_FILL_KEY = constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY
_EPOCH_KEY = constants.RESERVED_FILL_GRANT_EPOCH_OVERRIDE_KEY
_PROTOCOL_KEY = constants.RESERVED_FILL_PROTOCOL_VERSION_OVERRIDE_KEY
_GENERATION_KEY = constants.RESERVED_FILL_SERVICE_GENERATION_OVERRIDE_KEY
_POOL_KEY = constants.RESERVED_FILL_POOL_KEY_OVERRIDE_KEY

# Pickleable form of the zero-cost k8s location, as handed to
# collect_reserved_capacity by the poller (Location.to_pickleable()).
_K8S_KEY = {
    'cloud': 'Kubernetes',
    'region': 'research-ctx',
    'zone': None,
    'accelerators': {
        'A100': 1
    },
    'use_spot': False,
    'image_id': None,
    'disk_tier': None,
}


def _spec(min_replicas=1, max_replicas=10, fill=True):
    return types.SimpleNamespace(min_replicas=min_replicas,
                                 max_replicas=max_replicas,
                                 num_overprovision=None,
                                 target_qps_per_replica=None,
                                 upscale_delay_seconds=None,
                                 downscale_delay_seconds=None,
                                 reserved_capacity_fill=fill)


def _make_autoscaler(**spec_kwargs):
    # target_qps_per_replica=None makes the demand target a constant
    # min_replicas, so every fill effect is isolated from demand math.
    return autoscalers.RequestRateAutoscaler('svc',
                                             _spec(**spec_kwargs),
                                             version=1)


def _replica(replica_id,
             location_key=None,
             status=serve_state.ReplicaStatus.READY,
             version=1,
             created_at=None):
    # created_at=None mirrors a pre-upgrade pickled row (treated as older
    # than any fill snapshot); pass a float to model a row created at a
    # known time relative to the snapshot.
    info = mock.Mock()
    info.replica_id = replica_id
    info.version = version
    info.status = status
    info.is_terminal = status in serve_state.ReplicaStatus.terminal_statuses()
    info.is_ready = status == serve_state.ReplicaStatus.READY
    info.cluster_name = f'cluster-{replica_id}'
    info.created_at = created_at
    info.status_property.unrecoverable_failure.return_value = False
    info.get_spot_location.return_value = (
        spot_placer.Location.from_pickleable(location_key))
    return info


def _feed(autoscaler, free_slots, keys=(_K8S_KEY,), timestamp=None, polls=2):
    # polls=2 by default: an increase only takes effect after two
    # consecutive snapshots (damping).
    ts = time.time() if timestamp is None else timestamp
    for _ in range(polls):
        autoscaler.collect_reserved_capacity(free_slots, list(keys), ts)


def _decisions(autoscaler, replicas):
    return autoscaler.generate_scaling_decisions(replicas, [1])


def _ups(decisions):
    return [d for d in decisions if d.operator == _SCALE_UP]


def _downs(decisions):
    return [d for d in decisions if d.operator == _SCALE_DOWN]


def _stale_timestamp():
    return time.time() - (reserved_capacity.poll_interval_seconds() *
                          constants.RESERVED_CAPACITY_STALE_AFTER_INTERVALS +
                          30)


def _protocol_v2_cleanup_info(**overrides):
    values = {
        'cluster_name': 'svc-1',
        'reserved_fill': True,
        'reserved_fill_pool_key': reserved_capacity_broker.make_pool_key(
            'research-ctx',
            'H200',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='physical-a'),
        'reserved_fill_service_generation': 1,
        'reserved_fill_physical_cluster_uid': 'physical-a',
        'reserved_fill_kubernetes_context': 'research-ctx',
        'location': {
            'cloud': 'Kubernetes',
            'region': 'research-ctx',
            'accelerators': {
                'H200': 1,
            },
        },
        'resources_override': {
            'cloud': 'Kubernetes',
            'region': 'research-ctx',
            'accelerators': {
                'H200': 1,
            },
        },
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


class TestProtocolV2CleanupFence(unittest.TestCase):
    """Durable cleanup authority is exact or fails closed."""

    def test_exact_v2_and_whole_float_counts_are_accepted(self):
        info = _protocol_v2_cleanup_info()
        info.location['accelerators']['H200'] = 1.0
        info.resources_override['accelerators']['H200'] = 1.0

        fence = reserved_capacity.parse_protocol_v2_cleanup_fence(info)

        self.assertEqual(
            fence,
            reserved_capacity.ProtocolV2CleanupFence(
                kubernetes_context='research-ctx',
                physical_cluster_uid='physical-a'))

    def test_legacy_and_ordinary_rows_need_no_physical_fence(self):
        ordinary = types.SimpleNamespace(reserved_fill=False)
        legacy = types.SimpleNamespace(reserved_fill=True,
                                       reserved_fill_pool_key=None,
                                       reserved_fill_service_generation=None,
                                       reserved_fill_physical_cluster_uid=None,
                                       reserved_fill_kubernetes_context=None)
        self.assertIsNone(
            reserved_capacity.parse_protocol_v2_cleanup_fence(ordinary))
        self.assertIsNone(
            reserved_capacity.parse_protocol_v2_cleanup_fence(legacy))

    def test_partial_authority_on_non_fill_row_is_rejected(self):
        info = types.SimpleNamespace(
            reserved_fill=False,
            reserved_fill_physical_cluster_uid='physical-a')
        with self.assertRaises(
                exceptions.KubernetesPhysicalClusterIdentityError):
            reserved_capacity.parse_protocol_v2_cleanup_fence(info)

    def test_reserved_fill_marker_must_be_an_exact_bool(self):
        for marker in (1, 0, 'yes', None):
            with self.subTest(marker=marker):
                info = types.SimpleNamespace(reserved_fill=marker)
                with self.assertRaises(
                        exceptions.KubernetesPhysicalClusterIdentityError):
                    reserved_capacity.parse_protocol_v2_cleanup_fence(info)

    def test_v2_resource_pin_must_be_exact(self):
        mutations = (
            lambda info: info.location['accelerators'].__setitem__('H200', 1.5),
            lambda info: info.resources_override['accelerators'].__setitem__(
                'H200', True),
            lambda info: info.resources_override.__setitem__(
                'region', 'retargeted-context'),
            lambda info: info.resources_override.__setitem__(
                'accelerators', {'A100': 1}),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                info = _protocol_v2_cleanup_info()
                mutate(info)
                with self.assertRaises(
                        exceptions.KubernetesPhysicalClusterIdentityError):
                    reserved_capacity.parse_protocol_v2_cleanup_fence(info)

    @staticmethod
    def _handle(context='research-ctx', cloud=None, cluster_name='svc-1'):
        handle = mock.Mock(spec=backends.CloudVmRayResourceHandle)
        handle.cluster_name = cluster_name
        handle.launched_resources = types.SimpleNamespace(cloud=cloud or
                                                          clouds.Kubernetes(),
                                                          region=context)
        return handle

    def test_provider_fence_validates_handle_before_entering_uid_fence(self):
        info = _protocol_v2_cleanup_info()
        handle = self._handle()
        uid_fence = mock.MagicMock()
        uid_fence.return_value.__enter__.return_value = None

        with mock.patch.object(kubernetes_adaptor, 'physical_cluster_uid_fence',
                               uid_fence):
            with reserved_capacity.protocol_v2_provider_fence(info, handle):
                pass

        uid_fence.assert_called_once_with('research-ctx', 'physical-a')

    def test_provider_fence_enters_matching_phase_and_streaming_can_opt_out(
            self):
        info = _protocol_v2_cleanup_info()
        handle = self._handle()
        entered_modes = []

        @contextlib.contextmanager
        def _phase(mode):
            entered_modes.append(mode)
            yield types.SimpleNamespace(mode=mode)

        with mock.patch.object(reserved_capacity.provider_phase,
                               'provider_phase', side_effect=_phase), \
             mock.patch.object(kubernetes_adaptor,
                               'physical_cluster_uid_fence',
                               return_value=contextlib.nullcontext()):
            with reserved_capacity.protocol_v2_provider_fence(info, handle):
                pass
            with reserved_capacity.protocol_v2_provider_fence(
                    info, handle, include_provider_phase=False):
                pass
            with reserved_capacity.protocol_v2_provider_fence(
                    types.SimpleNamespace(reserved_fill=False), None):
                pass

        self.assertEqual(entered_modes, [
            provider_phase.ProviderPhaseMode.V2_FENCED,
            provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
        ])

    def test_ordinary_phase_classifier_requires_exact_real_cloud_handle(self):
        ordinary_mode = provider_phase.ProviderPhaseMode.AMBIENT_LEGACY
        for cloud in (clouds.AWS(), clouds.GCP()):
            with self.subTest(cloud=cloud):
                self.assertIsNone(
                    reserved_capacity.ordinary_provider_phase_mode(
                        self._handle(cloud=cloud), 'svc-1'))
        for handle in (
                None,
                object(),
                self._handle(),
                self._handle(cloud=object()),
                self._handle(cluster_name='replacement', cloud=clouds.GCP()),
        ):
            with self.subTest(handle=handle):
                self.assertEqual(
                    reserved_capacity.ordinary_provider_phase_mode(
                        handle, 'svc-1'), ordinary_mode)

    def test_ordinary_fence_rejects_wrong_admission_even_for_non_kubernetes(
            self):
        admission = types.SimpleNamespace(
            mode=provider_phase.ProviderPhaseMode.V2_FENCED)
        ordinary = types.SimpleNamespace(reserved_fill=False)
        with self.assertRaises(exceptions.ProviderPhaseMisuseError):
            reserved_capacity.protocol_v2_provider_fence(
                ordinary,
                self._handle(cloud=clouds.GCP()),
                phase_admission=admission)

    def test_provider_fence_rejects_replaced_handle_without_provider_call(self):
        info = _protocol_v2_cleanup_info()
        handles = (
            None,
            self._handle(cluster_name='replacement'),
            self._handle(context='replacement-context'),
            self._handle(cloud=clouds.AWS()),
        )
        with mock.patch.object(kubernetes_adaptor,
                               'physical_cluster_uid_fence') as uid_fence:
            for handle in handles:
                with self.subTest(handle=handle), self.assertRaises(
                        exceptions.KubernetesPhysicalClusterIdentityError):
                    reserved_capacity.protocol_v2_provider_fence(info, handle)
        uid_fence.assert_not_called()

    def test_provider_batch_keeps_one_uid_proof_alive_for_all_workers(self):
        first = _protocol_v2_cleanup_info(cluster_name='svc-1')
        second = _protocol_v2_cleanup_info(cluster_name='svc-2')
        first_handle = self._handle(cluster_name='svc-1')
        second_handle = self._handle(cluster_name='svc-2')
        key = ('research-ctx', 'physical-a')
        active = 0
        uid_reads = 0
        lock = threading.Lock()

        @contextlib.contextmanager
        def _uid_fence(context, physical_uid):
            nonlocal active, uid_reads
            self.assertEqual((context, physical_uid), key)
            with lock:
                if active == 0:
                    uid_reads += 1
                active += 1
            try:
                yield
            finally:
                with lock:
                    active -= 1

        with mock.patch.object(kubernetes_adaptor,
                               'physical_cluster_uid_fence',
                               side_effect=_uid_fence):
            with reserved_capacity.protocol_v2_provider_batch_fences(
                {key: (first, first_handle)}) as failures:
                self.assertEqual(failures, {})
                with reserved_capacity.protocol_v2_provider_fence(
                        first, first_handle):
                    pass
                with reserved_capacity.protocol_v2_provider_fence(
                        second, second_handle):
                    pass

        self.assertEqual(uid_reads, 1)

    def test_provider_batch_owner_explicitly_joins_root_admission(self):
        info = _protocol_v2_cleanup_info()
        handle = self._handle()
        key = ('research-ctx', 'physical-a')
        admission = types.SimpleNamespace(
            mode=provider_phase.ProviderPhaseMode.V2_FENCED)
        root_thread_id = threading.get_ident()
        join_thread_ids = []

        @contextlib.contextmanager
        def _phase(_mode):
            yield admission

        @contextlib.contextmanager
        def _join(candidate):
            self.assertIs(candidate, admission)
            join_thread_ids.append(threading.get_ident())
            yield candidate

        with mock.patch.object(reserved_capacity.provider_phase,
                               'provider_phase', side_effect=_phase), \
             mock.patch.object(reserved_capacity.provider_phase,
                               'join_provider_phase', side_effect=_join), \
             mock.patch.object(kubernetes_adaptor,
                               'physical_cluster_uid_fence',
                               return_value=contextlib.nullcontext()):
            with reserved_capacity.protocol_v2_provider_batch_fences(
                {key: (info, handle)}) as failures:
                self.assertEqual(failures, {})

        self.assertEqual(len(join_thread_ids), 1)
        self.assertNotEqual(join_thread_ids[0], root_thread_id)


class TestFlagOff(unittest.TestCase):
    """Flag off: decisions identical to a fill-less autoscaler."""

    def test_disabled_is_passthrough_even_with_snapshot(self):
        replicas = [_replica(1, _K8S_KEY), _replica(2)]
        disabled = _make_autoscaler(fill=False)
        # Even a (spuriously) fed snapshot must not alter decisions.
        _feed(disabled, 5)
        control_spec = _spec()
        del control_spec.reserved_capacity_fill  # pre-flag spec object
        control = autoscalers.RequestRateAutoscaler('svc',
                                                    control_spec,
                                                    version=1)
        got = _decisions(disabled, replicas)
        expected = _decisions(control, replicas)
        self.assertEqual([(d.operator, d.target) for d in got],
                         [(d.operator, d.target) for d in expected])

    def test_disabled_overlay_returns_same_object(self):
        autoscaler = _make_autoscaler(fill=False)
        decisions = [
            autoscalers.AutoscalerDecision(_SCALE_UP, None),
        ]
        self.assertIs(autoscaler._apply_reserved_capacity_fill([], decisions),
                      decisions)

    def test_disabled_info_has_no_fill_keys(self):
        info = _make_autoscaler(fill=False).info()
        self.assertNotIn('fill_target', info)
        self.assertNotIn('fill_free_slots', info)
        self.assertNotIn('fill_snapshot_age', info)


class TestSentinelScaleUps(unittest.TestCase):
    """Reserved fill carries ONLY the sentinel; demand ups do not."""

    def test_surplus_ups_sentinel_demand_ups_plain(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        _feed(autoscaler, 3)
        decisions = _decisions(autoscaler, [])
        ups = _ups(decisions)
        self.assertEqual(len(ups), 4)  # 1 demand (to min) + all 3 free slots
        plain = [d for d in ups if d.target is None]
        sentinel = [d for d in ups if d.target == {_FILL_KEY: True}]
        self.assertEqual(len(plain), 1)
        self.assertEqual(len(sentinel), 3)
        # Sentinel override carries NOTHING else.
        for decision in sentinel:
            self.assertEqual(set(decision.target), {_FILL_KEY})
        self.assertEqual(len(_downs(decisions)), 0)

    def test_max_replicas_clamps_fill_target(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=4)
        _feed(autoscaler, 100)
        decisions = _decisions(autoscaler, [])
        ups = _ups(decisions)
        self.assertEqual(len(ups), 4)
        self.assertEqual(len([d for d in ups if d.target == {
            _FILL_KEY: True
        }]), 3)
        self.assertEqual(autoscaler.info()['fill_target'], 4)


class TestLogicalReplicaFill(unittest.TestCase):
    """Logical fleets account one-GPU fill candidates in slot units."""

    def test_paid_backend_width_is_counted_in_logical_units(self):
        spec = service_spec.SkyServiceSpec(
            readiness_path='/health',
            initial_delay_seconds=60,
            readiness_timeout_seconds=30,
            endpoint_probe_interval_seconds=10,
            lb_stream_timeout_seconds=60,
            min_replicas=1,
            max_replicas=10,
            target_concurrency_per_replica=1,
            graceful_drain_async_occupancy=True,
            spot_placer=spot_placer.CAPACITY_AWARE_SPOT_PLACER,
            reserved_capacity_fill=True)
        autoscaler = autoscalers.ConcurrencyAutoscaler('svc', spec)
        autoscaler.seed_zero_cost_locations([_K8S_KEY])
        _feed(autoscaler, 5)
        paid_location = make_location('us-east-1',
                                      accelerators={'L4': 4},
                                      cloud_name='AWS')
        paid = _replica(1, paid_location.to_pickleable())
        paid.planned_capacity = 4

        fill_ups = _ups(autoscaler._apply_reserved_capacity_fill([paid], []))

        self.assertEqual(len(fill_ups), 5)
        self.assertTrue(all(up.target[_FILL_KEY] for up in fill_ups))


class TestDemandIndependentFill(unittest.TestCase):
    """Fresh reserved slots launch regardless of demand-target ordering."""

    def test_paid_fleet_satisfying_larger_demand_does_not_block_fill(self):
        autoscaler = _make_autoscaler(min_replicas=5, max_replicas=10)
        paid_replicas = [_replica(replica_id) for replica_id in range(1, 6)]
        _feed(autoscaler, 2)

        decisions = _decisions(autoscaler, paid_replicas)

        self.assertEqual(len(_fill_ups(decisions)), 2)
        self.assertEqual(len(_downs(decisions)), 0)
        self.assertEqual(autoscaler.get_final_target_num_replicas(), 5)
        self.assertEqual(autoscaler.info()['fill_target'], 2)

    def test_planned_demand_reserves_hard_ceiling_headroom(self):
        autoscaler = _make_autoscaler(min_replicas=9, max_replicas=10)
        paid_replicas = [_replica(replica_id) for replica_id in range(1, 9)]
        _feed(autoscaler, 5)

        decisions = _decisions(autoscaler, paid_replicas)

        plain_ups = [
            decision for decision in _ups(decisions) if decision.target is None
        ]
        self.assertEqual(len(plain_ups), 1)
        self.assertEqual(len(_fill_ups(decisions)), 1)

    def test_old_versions_count_against_hard_ceiling(self):
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=2,
                                                             max_replicas=6),
                                                       version=2)
        replicas = [
            _replica(1, version=1),
            _replica(2, version=1),
            _replica(3, version=1),
            _replica(4, version=2),
        ]
        _feed(autoscaler, 5)
        demand_up = autoscalers.AutoscalerDecision(_SCALE_UP, None)
        ordinary_decisions = [demand_up]

        decisions = autoscaler._apply_reserved_capacity_fill(
            replicas, ordinary_decisions)

        self.assertEqual(len(_fill_ups(decisions)), 1)
        self.assertEqual(len(_ups(decisions)), 2)
        self.assertEqual(ordinary_decisions, [demand_up])

    def test_at_hard_ceiling_waits_for_headroom(self):
        autoscaler = _make_autoscaler(min_replicas=5, max_replicas=10)
        paid_replicas = [_replica(replica_id) for replica_id in range(1, 11)]
        _feed(autoscaler, 3)

        decisions = autoscaler._apply_reserved_capacity_fill(paid_replicas, [])

        self.assertEqual(_fill_ups(decisions), [])


class TestScaleDownSuppression(unittest.TestCase):
    """Fill-covered surplus is not scaled down; uncovered surplus is."""

    def test_covered_surplus_suppressed_to_effective_target(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [
            _replica(1, _K8S_KEY),
            _replica(2, _K8S_KEY),
            _replica(3),  # paid
        ]
        _feed(autoscaler, 0)
        decisions = _decisions(autoscaler, replicas)
        # Demand target 1, fill target 2 (2 zero-cost + 0 free): only ONE
        # of the two demand scale-downs survives (down to the effective
        # target of 2), and no fill ups are emitted.
        self.assertEqual(len(_downs(decisions)), 1)
        self.assertEqual(len(_ups(decisions)), 0)

    def test_scale_down_resumes_when_fill_gone(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        # Zero-cost replicas evicted; snapshot reports 0 free.
        replicas = [_replica(3), _replica(4)]
        _feed(autoscaler, 0)
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(len(_downs(decisions)), 1)  # back to demand target
        self.assertEqual(len(_ups(decisions)), 0)


class TestMultiTickSlotSpending(unittest.TestCase):
    """A free slot is spent when the launch decision is emitted.

    Fill launches persist replica rows immediately, so zero_cost_count
    grows on the very next tick while the poller snapshot only refreshes
    on its interval: without spending, the same static snapshot would be
    re-consumed every tick, compounding the fill fleet.
    """

    def test_static_snapshot_not_reconsumed_across_ticks(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=10)
        _feed(autoscaler, 4)
        replicas: list = []
        next_id = 1
        total_fill_ups = 0
        for _ in range(3):
            decisions = _decisions(autoscaler, replicas)
            fill_ups = [
                d for d in _ups(decisions) if d.target == {
                    _FILL_KEY: True
                }
            ]
            total_fill_ups += len(fill_ups)
            # Emitted launches persist immediately: visible as zero-cost
            # nonterminal replicas on the next tick, before any new poll.
            for _ in fill_ups:
                replicas.append(_replica(next_id, _K8S_KEY))
                next_id += 1
        self.assertEqual(total_fill_ups, 4)
        self.assertEqual(len(replicas), 4)

    def test_spent_slots_not_regranted_by_single_stale_poll(self):
        # The raw-poll memory is deducted too: one poll still reporting
        # the pre-launch level (pods pending) must not re-raise the
        # damped value past the two-poll damping.
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=10)
        _feed(autoscaler, 4)
        decisions = _decisions(autoscaler, [])
        self.assertEqual(len(_ups(decisions)), 4)
        _feed(autoscaler, 4, polls=1)  # stale raw: pods not scheduled yet
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)


def _fill_ups(decisions):
    return [d for d in _ups(decisions) if d.target == {_FILL_KEY: True}]


class TestVersionAwareLaunchBaseline(unittest.TestCase):
    """Old-version zero-cost replicas never inflate fill launches.

    The zero-cost count feeding the launch target is latest-version-only;
    otherwise a rolling update's draining old fleet compounds fill launches
    every tick. All old-version rows still count against hard-ceiling headroom.
    """

    def test_rolling_update_zero_free_no_fill_launches(self):
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=2,
                                                             max_replicas=20),
                                                       version=2)
        _feed(autoscaler, 0)
        # 5 old-version zero-cost replicas still draining; the reserved
        # capacity they occupy polls as 0 free.
        replicas = [_replica(i, _K8S_KEY, version=1) for i in range(1, 6)]
        next_id = 100
        for _ in range(3):
            decisions = autoscaler.generate_scaling_decisions(replicas, [1])
            self.assertEqual(len(_fill_ups(decisions)), 0)
            # Demand launches persist rows immediately at the latest
            # version, pinned to the zero-cost location.
            for decision in _ups(decisions):
                if decision.target != {_FILL_KEY: True}:
                    replicas.append(
                        _replica(next_id,
                                 _K8S_KEY,
                                 status=serve_state.ReplicaStatus.PROVISIONING,
                                 version=2))
                    next_id += 1

    def test_old_zero_cost_replicas_do_not_inflate_free_slot_fill(self):
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=1,
                                                             max_replicas=20),
                                                       version=2)
        _feed(autoscaler, 3)
        replicas = [_replica(i, _K8S_KEY, version=1) for i in range(1, 6)]
        decisions = autoscaler.generate_scaling_decisions(replicas, [1])
        # Latest zero-cost 0 + 3 free slots: all three slots launch,
        # independently of the demand launch, and are NOT inflated to
        # 5 old + 3 free by the draining old-version fleet.
        self.assertEqual(len(_fill_ups(decisions)), 3)


class TestAllVersionOccupancyDebit(unittest.TestCase):
    """The occupancy debit spans ALL versions, not just the latest.

    An old-version zero-cost replica still PROVISIONING has an unbound
    pod: its slot polls free, yet the row holds a claim on it. If only
    latest-version rows were debited, a fill launch would collide with
    that claim and fail on capacity.
    """

    def test_old_version_provisioning_row_debits_spendable(self):
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=1,
                                                             max_replicas=20),
                                                       version=2)
        _feed(autoscaler, 3)
        replicas = [
            _replica(1,
                     _K8S_KEY,
                     status=serve_state.ReplicaStatus.PROVISIONING,
                     version=1)
        ]
        decisions = autoscaler.generate_scaling_decisions(replicas, [1])
        # Spendable 3 - 1 (old-version pending claim) = 2;
        # fill_target_launch = 0 latest zero-cost + 2 spendable. Both truly
        # free slots launch independently of the demand target (a latest-only
        # occupancy debit would emit 3, one onto the claimed slot).
        self.assertEqual(len(_fill_ups(decisions)), 2)


class TestPendingReplicasOccupySlots(unittest.TestCase):
    """Not-yet-READY fill replicas keep their slots spent across polls.

    Launch threads can queue past the two-poll damping window; while the
    pods are invisible to the poller, raw polls keep reporting the
    pre-launch free level and damping re-raises the damped value. The
    pending rows must be treated as occupying those slots.
    """

    def test_unchanged_polls_with_pending_rows_emit_no_more_fills(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=20)
        _feed(autoscaler, 5)
        first = _decisions(autoscaler, [])
        self.assertEqual(len(_fill_ups(first)), 5)
        # Rows persist immediately, pods not created yet (not READY).
        replicas = [
            _replica(i, _K8S_KEY, status=serve_state.ReplicaStatus.PROVISIONING)
            for i in range(1, 6)
        ]
        # Two more polls still see the pre-launch level: damping re-raises
        # the damped free value, but the 5 pending rows occupy the slots.
        _feed(autoscaler, 5, polls=2)
        second = _decisions(autoscaler, replicas)
        self.assertEqual(len(_fill_ups(second)), 0)

    def test_steady_state_once_pending_turn_ready(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=20)
        _feed(autoscaler, 5)
        self.assertEqual(len(_fill_ups(_decisions(autoscaler, []))), 5)
        replicas = [
            _replica(i, _K8S_KEY, status=serve_state.ReplicaStatus.PROVISIONING)
            for i in range(1, 6)
        ]
        _feed(autoscaler, 5, polls=2)  # pods still invisible
        self.assertEqual(len(_fill_ups(_decisions(autoscaler, replicas))), 0)
        # Pods bind and turn READY; the poller now sees the slots taken
        # (decrease applies immediately).
        replicas = [_replica(i, _K8S_KEY) for i in range(1, 6)]
        _feed(autoscaler, 0, polls=1)
        for _ in range(2):
            decisions = _decisions(autoscaler, replicas)
            # Steady state: no new fills, no scale-down of the fill
            # fleet (no oscillation).
            self.assertEqual(decisions, [])


class TestVictimAwareSuppression(unittest.TestCase):
    """Only zero-cost victims are sheltered; paid downs always pass."""

    def test_paid_down_passes_zero_cost_suppressed(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [
            _replica(1, _K8S_KEY),
            _replica(2, _K8S_KEY),
            # Paid replica with the HIGHEST id: first victim in the
            # subclass's newest-first ordering, so a victim-blind
            # surplus keep would shelter it while killing zero-cost.
            _replica(3),
        ]
        _feed(autoscaler, 2)
        decisions = _decisions(autoscaler, replicas)
        # Demand target 1 -> victims [3, 2]; surplus (fill_target 4 -
        # demand 1 = 3) covers both, but only the zero-cost victim (2)
        # may be sheltered: the paid down (3) must pass through.
        self.assertEqual([d.target for d in _downs(decisions)], [3])


class TestTailSuppression(unittest.TestCase):
    """Partial surplus shelters the LEAST-preferred zero-cost victims."""

    def test_partial_surplus_keeps_ready_kills_provisioning(self):
        # Victims are emitted most-preferred-first (PROVISIONING before
        # READY). With surplus 1 covering only one of the two zero-cost
        # victims, the shelter must go to the READY replica serving
        # traffic -- a prefix keep would shelter the warming one and
        # kill the serving one.
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [
            _replica(1),  # paid READY (oldest): survives as the target.
            _replica(2, _K8S_KEY),  # zero-cost READY
            _replica(3, _K8S_KEY,
                     status=serve_state.ReplicaStatus.PROVISIONING),
        ]
        # Fresh snapshot with 0 free slots: fill_target = zc_count (2),
        # demand 1 -> surplus 1.
        _feed(autoscaler, 0)
        decisions = _decisions(autoscaler, replicas)
        # Demand victims: [3 (PROVISIONING), 2 (READY, newest)]. Tail
        # suppression shelters 2; the PROVISIONING one is retired.
        self.assertEqual([d.target for d in _downs(decisions)], [3])


class TestLocationFromResourcesOverride(unittest.TestCase):
    """Re-driven pinned launches must recover their location."""

    def test_roundtrip_from_inlined_to_dict(self):
        location = spot_placer.Location.from_pickleable(_K8S_KEY)
        override = {'some_user_key': 1}
        override.update(location.to_dict())
        rebuilt = spot_placer.Location.from_resources_override(override)
        self.assertIsNotNone(rebuilt)
        assert rebuilt is not None
        self.assertEqual(rebuilt.region, location.region)
        self.assertEqual(rebuilt.accelerators, location.accelerators)
        self.assertIs(rebuilt.use_spot, False)

    def test_non_pinned_override_returns_none(self):
        self.assertIsNone(spot_placer.Location.from_resources_override(None))
        self.assertIsNone(spot_placer.Location.from_resources_override({}))
        self.assertIsNone(
            spot_placer.Location.from_resources_override({'use_spot': True}))


class TestPostSnapshotZeroCostDebit(unittest.TestCase):
    """Zero-cost rows created after the snapshot occupy free slots.

    A DEMAND launch placed on the zero-cost tier that binds AND turns
    READY within one inter-poll gap escapes the not-READY debit, yet the
    slot it sits on was counted free when the snapshot was taken: it
    must be subtracted from the spendable level regardless of readiness.
    """

    def test_ready_within_gap_debits_slot(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        ts = time.time()
        _feed(autoscaler, 3, timestamp=ts)
        # Demand launch landed zero-cost and turned READY after the
        # snapshot, before the next poll.
        replicas = [_replica(1, _K8S_KEY, created_at=ts + 1)]
        decisions = _decisions(autoscaler, replicas)
        # Spendable 3 - 1 occupied = 2 fill ups (not 3).
        self.assertEqual(len(_fill_ups(decisions)), 2)
        self.assertEqual(len(_ups(decisions)), 2)
        self.assertEqual(len(_downs(decisions)), 0)

    def test_pre_snapshot_ready_row_not_debited(self):
        # A READY row older than the snapshot has a bound pod the poll
        # already excluded: debiting it again would double-subtract.
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        ts = time.time()
        _feed(autoscaler, 3, timestamp=ts)
        replicas = [_replica(1, _K8S_KEY, created_at=ts - 100)]
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(len(_fill_ups(decisions)), 3)

    def test_row_without_created_at_treated_as_pre_snapshot(self):
        # Pre-upgrade pickled rows carry created_at=None: always-debiting
        # them would under-fill for their whole lifetime.
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        _feed(autoscaler, 3)
        replicas = [_replica(1, _K8S_KEY, created_at=None)]
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(len(_fill_ups(decisions)), 3)

    def test_not_ready_and_post_snapshot_debited_once(self):
        # A row that is BOTH not READY and created after the snapshot
        # matches both clauses of the debit rule but occupies one slot:
        # it must be subtracted exactly once.
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        ts = time.time()
        _feed(autoscaler, 3, timestamp=ts)
        replicas = [
            _replica(1,
                     _K8S_KEY,
                     status=serve_state.ReplicaStatus.PROVISIONING,
                     created_at=ts + 1)
        ]
        decisions = _decisions(autoscaler, replicas)
        # Spendable 3 - 1 = 2 (double subtraction would leave 1).
        self.assertEqual(len(_fill_ups(decisions)), 2)


class TestBootSeeding(unittest.TestCase):
    """Respawn: a seeded location set protects the fill fleet at tick 0.

    A respawned controller's autoscaler starts with empty fill state and
    can tick before the first (slow) poll; seeding only the zero-cost
    location set makes suppression work immediately while granting no
    free slots.
    """

    def _fleet(self):
        return [
            _replica(1, _K8S_KEY),
            _replica(2, _K8S_KEY),
            # Paid replica with the highest id: first demand victim.
            _replica(3),
        ]

    def test_seeded_fresh_autoscaler_shelters_fill_on_first_tick(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        autoscaler.seed_zero_cost_locations([_K8S_KEY])
        # First tick, before any collect_reserved_capacity: the paid
        # down passes, the zero-cost victim is sheltered.
        decisions = _decisions(autoscaler, self._fleet())
        self.assertEqual([d.target for d in _downs(decisions)], [3])
        # No free slots granted by seeding: no fill launches either.
        self.assertEqual(len(_ups(decisions)), 0)
        self.assertIsNone(autoscaler._fill_snapshot_time)
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)

    def test_unseeded_fresh_autoscaler_terminates_fleet(self):
        # The failure mode the seed prevents: with an empty location set
        # every zero-cost victim is fair game on the first tick.
        autoscaler = _make_autoscaler(min_replicas=1)
        decisions = _decisions(autoscaler, self._fleet())
        self.assertEqual(len(_downs(decisions)), 2)

    def test_seed_never_overwrites_loaded_locations(self):
        old = _make_autoscaler()
        _feed(old, 3)
        fresh = _make_autoscaler()
        fresh.load_dynamic_states(old.dump_dynamic_states())
        fresh.seed_zero_cost_locations([dict(_K8S_KEY, region='other-ctx')])
        self.assertEqual(fresh._fill_zero_cost_locations,
                         [spot_placer.Location.from_pickleable(_K8S_KEY)])


class TestControllerSeeding(unittest.TestCase):
    """Controller-side seeding: boot / update_service swap wiring."""

    def _make_controller(self, autoscaler, placer):
        # pylint: disable=import-outside-toplevel
        from sky.serve import controller as controller_lib
        ctrl = controller_lib.SkyServeController.__new__(
            controller_lib.SkyServeController)
        ctrl._autoscaler = autoscaler
        ctrl._replica_manager = types.SimpleNamespace(spot_placer=placer)
        return ctrl

    def _placer(self):
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [
            spot_placer.Location.from_pickleable(_K8S_KEY)
        ]
        return placer

    def test_swap_without_fill_state_gets_seeded(self):
        # update_service swap where the old autoscaler's dump carried no
        # fill state (build predating the feature): the replacement must
        # still shelter the fill fleet on its first tick.
        new = _make_autoscaler(min_replicas=1)
        new.load_dynamic_states({
            'latest_version_ever_ready': 1,
            'request_timestamps': [],
        })
        ctrl = self._make_controller(new, self._placer())
        ctrl._seed_fill_zero_cost_locations(new)
        replicas = [
            _replica(1, _K8S_KEY),
            _replica(2, _K8S_KEY),
            _replica(3),
        ]
        decisions = _decisions(new, replicas)
        self.assertEqual([d.target for d in _downs(decisions)], [3])
        self.assertIsNone(new._fill_snapshot_time)

    def test_flag_off_does_not_seed(self):
        autoscaler = _make_autoscaler(fill=False)
        placer = self._placer()
        ctrl = self._make_controller(autoscaler, placer)
        ctrl._seed_fill_zero_cost_locations(autoscaler)
        self.assertEqual(autoscaler._fill_zero_cost_locations, [])
        placer.zero_cost_locations.assert_not_called()

    def test_no_placer_does_not_seed(self):
        autoscaler = _make_autoscaler()
        ctrl = self._make_controller(autoscaler, placer=None)
        ctrl._seed_fill_zero_cost_locations(autoscaler)
        self.assertEqual(autoscaler._fill_zero_cost_locations, [])

    def test_seed_failure_is_swallowed_and_leaves_unseeded(self):
        # A malformed or incomplete placer during a rolling upgrade must not
        # propagate out of the seed (it runs inside controller __init__).
        autoscaler = _make_autoscaler()
        placer = mock.Mock()
        placer.zero_cost_locations.side_effect = ValueError(
            'catalog unavailable')
        ctrl = self._make_controller(autoscaler, placer)
        ctrl._seed_fill_zero_cost_locations(autoscaler)  # must not raise
        self.assertEqual(autoscaler._fill_zero_cost_locations, [])


class TestDumpLoadFillState(unittest.TestCase):
    """Fill state survives the update_service autoscaler swap."""

    def test_roundtrip_suppresses_pre_poll(self):
        old = _make_autoscaler(min_replicas=1)
        _feed(old, 3)
        fresh = _make_autoscaler(min_replicas=1)
        fresh.load_dynamic_states(old.dump_dynamic_states())
        self.assertEqual(fresh.info()['fill_free_slots'], 3)
        # Before any poll reaches the fresh instance, it must already
        # protect the fill fleet: paid victim passes, zero-cost victim
        # is suppressed.
        replicas = [
            _replica(1, _K8S_KEY),
            _replica(2, _K8S_KEY),
            _replica(3),
        ]
        decisions = _decisions(fresh, replicas)
        self.assertEqual([d.target for d in _downs(decisions)], [3])

    def test_load_tolerates_dump_without_fill_state(self):
        fresh = _make_autoscaler()
        # Dump shape from a build predating the fill feature.
        fresh.load_dynamic_states({
            'latest_version_ever_ready': 1,
            'request_timestamps': [],
        })
        self.assertEqual(fresh.info()['fill_free_slots'], 0)
        self.assertIsNone(fresh.info()['fill_snapshot_age'])


class TestRelaxedZeroCostMatching(unittest.TestCase):
    """Matching ignores image_id/disk_tier; legacy rows match by region."""

    def setUp(self):
        self.autoscaler = _make_autoscaler(min_replicas=1)
        _feed(self.autoscaler, 0)

    def test_legacy_shape_less_row_matches(self):
        legacy = _replica(1, {
            'cloud': 'Kubernetes',
            'region': 'research-ctx',
            'zone': None,
        })
        self.assertTrue(self.autoscaler._replica_on_zero_cost_location(legacy))

    def test_image_changed_row_matches(self):
        image_changed = _replica(
            2,
            dict(_K8S_KEY, image_id={None: 'docker:model:v2'},
                 disk_tier='high'))
        self.assertTrue(
            self.autoscaler._replica_on_zero_cost_location(image_changed))

    def test_different_shape_or_region_does_not_match(self):
        other_gpu = _replica(3, dict(_K8S_KEY, accelerators={'H100': 1}))
        other_region = _replica(4, dict(_K8S_KEY, region='other-ctx'))
        self.assertFalse(
            self.autoscaler._replica_on_zero_cost_location(other_gpu))
        self.assertFalse(
            self.autoscaler._replica_on_zero_cost_location(other_region))


class TestPollerFlagOff(unittest.TestCase):
    """Poller skips the expensive query when the live flag is off."""

    class _Stop(Exception):
        pass

    def test_pre_set_stop_event_performs_no_poll(self):
        autoscaler = _make_autoscaler(fill=True)
        placer = mock.Mock()
        stop_event = threading.Event()
        stop_event.set()

        reserved_capacity.poller_loop(lambda: autoscaler,
                                      lambda: placer,
                                      stop_event=stop_event)

        placer.zero_cost_locations.assert_not_called()

    def test_complete_cycle_is_serialized_with_update_epoch(self):
        autoscaler = _make_autoscaler(fill=True)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = []
        stop_event = threading.Event()
        actuation_lock = threading.RLock()
        cycle_started = threading.Event()
        release_cycle = threading.Event()
        update_acquired = threading.Event()

        def _slow_cycle(*_args, **_kwargs):
            cycle_started.set()
            self.assertTrue(release_cycle.wait(timeout=5))

        with mock.patch.object(
                reserved_capacity.reserved_capacity_broker,
                'get_protocol_version',
                return_value=reserved_capacity_broker.PROTOCOL_V2), \
             mock.patch.object(reserved_capacity,
                               '_broker_cycle_v2',
                               side_effect=_slow_cycle):
            poller = threading.Thread(
                target=reserved_capacity.poller_loop,
                args=(lambda: autoscaler, lambda: placer),
                kwargs={
                    'service_name': 'svc',
                    'stop_event': stop_event,
                    'actuation_epoch_lock': actuation_lock,
                })
            poller.start()
            self.assertTrue(cycle_started.wait(timeout=5))

            def _update_epoch():
                with actuation_lock:
                    update_acquired.set()
                    stop_event.set()

            updater = threading.Thread(target=_update_epoch)
            updater.start()
            self.assertFalse(update_acquired.wait(timeout=0.05))
            release_cycle.set()
            self.assertTrue(update_acquired.wait(timeout=5))
            updater.join(timeout=5)
            poller.join(timeout=5)

        self.assertFalse(updater.is_alive())
        self.assertFalse(poller.is_alive())

    def test_stop_while_waiting_for_update_epoch_skips_cycle(self):
        autoscaler = _make_autoscaler(fill=True)
        placer = mock.Mock()
        stop_event = threading.Event()
        lock_entered = threading.Event()
        release_lock = threading.Event()

        class _BlockedEpoch:

            def __enter__(self):
                lock_entered.set()
                if not release_lock.wait(timeout=5):
                    raise AssertionError('test did not release epoch lock')

            def __exit__(self, *_args):
                return False

        blocked_epoch = _BlockedEpoch()
        poller = threading.Thread(target=reserved_capacity.poller_loop,
                                  args=(lambda: autoscaler, lambda: placer),
                                  kwargs={
                                      'service_name': 'svc',
                                      'stop_event': stop_event,
                                      'actuation_epoch_lock': blocked_epoch,
                                  })
        poller.start()
        self.assertTrue(lock_entered.wait(timeout=5))
        stop_event.set()
        release_lock.set()
        poller.join(timeout=5)

        self.assertFalse(poller.is_alive())
        placer.zero_cost_locations.assert_not_called()

    def _run_one_cycle(self, autoscaler):
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = []
        with mock.patch.object(reserved_capacity,
                               'query_free_slots',
                               return_value=0) as query, \
             mock.patch.object(reserved_capacity.time,
                               'sleep',
                               side_effect=self._Stop):
            with self.assertRaises(self._Stop):
                reserved_capacity.poller_loop(lambda: autoscaler,
                                              lambda: placer)
        return placer, query

    def test_flag_off_sleeps_without_querying(self):
        autoscaler = _make_autoscaler(fill=False)
        placer, query = self._run_one_cycle(autoscaler)
        query.assert_not_called()
        placer.zero_cost_locations.assert_not_called()
        self.assertIsNone(autoscaler._fill_snapshot_time)

    def test_flag_on_queries_and_feeds(self):
        autoscaler = _make_autoscaler(fill=True)
        _, query = self._run_one_cycle(autoscaler)
        query.assert_called_once()
        self.assertIsNotNone(autoscaler._fill_snapshot_time)

    def test_snapshot_time_predates_the_query(self):
        # A zero-cost row created WHILE the (slow) availability query
        # runs occupies a slot the query may still count free; the
        # post-snapshot debit (created_at > snapshot_time) only catches
        # it if the snapshot timestamp predates the query, not follows
        # it.
        autoscaler = _make_autoscaler(fill=True)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = []
        pre_query_time = None

        def _slow_query(zero_cost):
            del zero_cost
            nonlocal pre_query_time
            pre_query_time = time.time()
            return 0

        with mock.patch.object(reserved_capacity,
                               'query_free_slots',
                               side_effect=_slow_query), \
             mock.patch.object(reserved_capacity.time,
                               'sleep',
                               side_effect=self._Stop):
            with self.assertRaises(self._Stop):
                reserved_capacity.poller_loop(lambda: autoscaler,
                                              lambda: placer)
        self.assertIsNotNone(autoscaler._fill_snapshot_time)
        self.assertLessEqual(autoscaler._fill_snapshot_time, pre_query_time)


class TestPollerClaimLifecycle(unittest.TestCase):
    """Disabling fill (or losing the placer) withdraws the broker claim
    immediately -- a disabled service must not keep absorbing entitlement
    until its claim TTL expires."""

    class _Stop(Exception):
        pass

    def test_protocol_v2_dispatches_complete_set_cycle(self):
        autoscaler = _make_autoscaler(fill=True)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = []
        with mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'get_protocol_version',
                               return_value=2), \
             mock.patch.object(reserved_capacity,
                               '_broker_cycle_v2') as cycle_v2, \
             mock.patch.object(reserved_capacity.time,
                               'sleep',
                               side_effect=self._Stop):
            with self.assertRaises(self._Stop):
                reserved_capacity.poller_loop(lambda: autoscaler,
                                              lambda: placer,
                                              service_name='svc')
        cycle_v2.assert_called_once()

    def test_disable_transition_removes_claim_once(self):
        autoscaler = _make_autoscaler(fill=True)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = []
        cycles = {'n': 0}

        def _sleep(_seconds):
            cycles['n'] += 1
            if cycles['n'] == 1:
                # An update turns the flag off on the live autoscaler.
                autoscaler.reserved_capacity_fill = False
            if cycles['n'] >= 3:
                raise self._Stop()

        with mock.patch.object(reserved_capacity,
                               '_broker_cycle') as broker_cycle, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'get_protocol_version',
                               return_value=1), \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'remove_claim') as remove_claim, \
             mock.patch.object(reserved_capacity.time,
                               'sleep',
                               side_effect=_sleep):
            with self.assertRaises(self._Stop):
                reserved_capacity.poller_loop(lambda: autoscaler,
                                              lambda: placer,
                                              service_name='svc')
        broker_cycle.assert_called_once()
        # Withdrawn exactly once on the disable transition, not re-spammed
        # on every subsequent disabled cycle.
        remove_claim.assert_called_once_with('svc')

    def test_cycle_failure_after_enable_still_withdraws_on_disable(self):
        # A broker cycle can die AFTER upserting its claim (e.g. the
        # round query raises), so the claim-may-exist flag must be set
        # BEFORE the cycle runs -- otherwise a subsequent disable would
        # skip remove_claim and leave a ghost claim absorbing entitlement
        # for the whole claim TTL.
        autoscaler = _make_autoscaler(fill=False)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = []
        cycles = {'n': 0}

        def _sleep(_seconds):
            cycles['n'] += 1
            if cycles['n'] == 1:
                # The first disabled cycle consumed the initial flag; an
                # update now re-enables fill.
                autoscaler.reserved_capacity_fill = True
            elif cycles['n'] == 2:
                # The (failed) enabled cycle ran; disable again.
                autoscaler.reserved_capacity_fill = False
            elif cycles['n'] >= 3:
                raise self._Stop()

        with mock.patch.object(
                reserved_capacity,
                '_broker_cycle',
                side_effect=RuntimeError('cycle died')) as broker_cycle, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'get_protocol_version',
                               return_value=1), \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'remove_claim') as remove_claim, \
             mock.patch.object(reserved_capacity.time,
                               'sleep',
                               side_effect=_sleep):
            with self.assertRaises(self._Stop):
                reserved_capacity.poller_loop(lambda: autoscaler,
                                              lambda: placer,
                                              service_name='svc')
        broker_cycle.assert_called_once()
        # Once for the initial disabled observation, and AGAIN after the
        # failed enabled cycle: the possibly-upserted claim is withdrawn
        # instead of ghosting until the TTL.
        self.assertEqual(remove_claim.call_count, 2)
        remove_claim.assert_called_with('svc')


class TestMultiPoolBrokerCycle(unittest.TestCase):
    """One v2 poll publishes a complete service generation."""

    def test_cycle_reads_replicas_and_utilization_once_and_partitions_budget(
            self):
        east = spot_placer.Location.from_pickleable(
            dict(_K8S_KEY, region='east-context'))
        phx_key = dict(_K8S_KEY, region='phx-context')
        phx_key['accelerators'] = {'H200': 1}
        phx = spot_placer.Location.from_pickleable(phx_key)
        placer = mock.Mock()
        placer.active_locations.return_value = [east, phx]
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        autoscaler.reserved_fill_utilization_gate = True
        demand_sample = mock.Mock()
        demand_sample.demonstrated_need.return_value = 4
        demand_sample.boot_hold.return_value = False
        autoscaler.fill_demand_sample = mock.Mock(return_value=demand_sample)

        allocations = [
            reserved_capacity_broker.Allocation(
                grant=1,
                feed=1,
                round_id=1,
                epoch=3,
                snapshot_time=time.time(),
                protocol_version=2,
                service_generation=1,
                observed_free=4,
                observed_free_by_accelerator={'a100': 4},
                observed_at=time.time()),
            reserved_capacity_broker.Allocation(
                grant=1,
                feed=1,
                round_id=1,
                epoch=5,
                snapshot_time=time.time(),
                protocol_version=2,
                service_generation=1,
                observed_free=7,
                observed_free_by_accelerator={'h200': 7},
                observed_at=time.time()),
        ]
        pending_allocations = iter(allocations)
        active_phase = []

        @contextlib.contextmanager
        def _phase(mode):
            self.assertFalse(active_phase)
            active_phase.append(mode)
            try:
                yield types.SimpleNamespace(mode=mode)
            finally:
                active_phase.pop()

        def _run_round(*_args, **_kwargs):
            # Provider admission must precede the broker's round lock/callback.
            self.assertEqual(active_phase,
                             [provider_phase.ProviderPhaseMode.V2_FENCED])
            return next(pending_allocations)

        with mock.patch.object(
                reserved_capacity,
                'get_kubernetes_physical_cluster_uid',
                side_effect=['east-uid', 'phx-uid']), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_replica_infos',
                               return_value=[]) as get_replicas, \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_reserved_fill_service_claim_set',
                               return_value=None), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_reserved_fill_round',
                               return_value=None), \
             mock.patch.object(reserved_capacity_broker,
                               'replace_claim_set',
                               return_value=1) as replace, \
             mock.patch.object(reserved_capacity_broker,
                               'run_round_if_stale',
                               side_effect=_run_round) as run_round, \
             mock.patch.object(reserved_capacity.provider_phase,
                               'provider_phase', side_effect=_phase), \
             mock.patch.object(
                 reserved_capacity,
                 '_record_allocation_observation') as record_observation:
            reserved_capacity._broker_cycle_v2(autoscaler, placer, 'svc',
                                               [east, phx], 'service-hash',
                                               (123, 'controller-ip'))

        get_replicas.assert_called_once_with('svc')
        autoscaler.fill_demand_sample.assert_called_once_with([])
        replace.assert_called_once()
        edges = replace.call_args.kwargs['edges']
        self.assertEqual([edge['access_context'] for edge in edges],
                         ['east-context', 'phx-context'])
        # Both never-observed, launchable pools get exactly one discovery
        # slot; no pool independently receives the service's full headroom.
        self.assertEqual([edge['effective_cap'] for edge in edges], [1, 1])
        self.assertEqual(run_round.call_count, 2)
        for call in run_round.call_args_list:
            self.assertEqual(call.kwargs['expected_protocol_version'], 2)
            self.assertEqual(call.kwargs['expected_service_generation'], 1)
            self.assertEqual(call.kwargs['lock_timeout_seconds'], 0)
        self.assertEqual(record_observation.call_count, 2)
        self.assertEqual(record_observation.call_args_list[0].args,
                         (placer, (east,), allocations[0]))
        self.assertEqual(record_observation.call_args_list[1].args,
                         (placer, (phx,), allocations[1]))
        self.assertEqual(set(autoscaler.info()['fill_by_pool']),
                         {edges[0]['pool_key'], edges[1]['pool_key']})

    def _assert_one_pool_round_failure_isolated(self, *, lock_timeout: bool):
        east = spot_placer.Location.from_pickleable(
            dict(_K8S_KEY, region='east-context'))
        phx_key = dict(_K8S_KEY, region='phx-context')
        phx_key['accelerators'] = {'H200': 1}
        phx = spot_placer.Location.from_pickleable(phx_key)
        placer = mock.Mock()
        placer.active_locations.return_value = [east, phx]
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=10)
        now = time.time()
        east_allocation = reserved_capacity_broker.Allocation(
            grant=1,
            feed=1,
            round_id=1,
            epoch=3,
            snapshot_time=now,
            protocol_version=2,
            service_generation=1)
        phx_allocation = reserved_capacity_broker.Allocation(
            grant=1,
            feed=1,
            round_id=1,
            epoch=5,
            snapshot_time=now,
            protocol_version=2,
            service_generation=1)
        phx_next = dataclasses.replace(phx_allocation,
                                       round_id=2,
                                       epoch=6,
                                       snapshot_time=now + 1)
        outcomes = iter((east_allocation, phx_allocation, 'fail', phx_next))
        real_run_round = reserved_capacity_broker.run_round_if_stale

        def _run_round(*args, **kwargs):
            outcome = next(outcomes)
            if outcome != 'fail':
                return outcome
            if not lock_timeout:
                raise RuntimeError('deterministic transient round failure')
            timeout_lock = mock.MagicMock()
            timeout_lock.__enter__.side_effect = locks.LockTimeout(
                'deterministic test timeout')
            with mock.patch.object(reserved_capacity_broker.locks,
                                   'get_lock',
                                   return_value=timeout_lock):
                return real_run_round(*args, **kwargs)

        with mock.patch.object(
                reserved_capacity,
                'get_kubernetes_physical_cluster_uid',
                side_effect=['east-uid', 'phx-uid', 'east-uid', 'phx-uid']), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_reserved_fill_service_claim_set',
                               return_value=None), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_reserved_fill_round',
                               return_value=None), \
             mock.patch.object(reserved_capacity_broker,
                               'replace_claim_set',
                               return_value=1), \
             mock.patch.object(reserved_capacity_broker,
                               'run_round_if_stale',
                               side_effect=_run_round):
            for _ in range(2):
                reserved_capacity._broker_cycle_v2(autoscaler, placer, 'svc',
                                                   [east, phx], 'service-hash',
                                                   (123, 'controller-ip'))

        east_pool = reserved_capacity_broker.make_pool_key(
            'east-context',
            'a100',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='east-uid')
        phx_pool = reserved_capacity_broker.make_pool_key(
            'phx-context',
            'h200',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='phx-uid')
        east_state = autoscaler._fill_pool_states[east_pool]
        phx_state = autoscaler._fill_pool_states[phx_pool]
        # The failed edge keeps only non-launching shelter from the prior
        # exact generation.  The healthy peer independently advances.
        self.assertEqual(east_state.shelter_grant, 1)
        self.assertEqual(east_state.grant, 0)
        self.assertEqual(east_state.free_slots, 0)
        self.assertIsNone(east_state.grant_epoch)
        self.assertEqual(phx_state.shelter_grant, 1)
        self.assertEqual(phx_state.grant, 1)
        self.assertEqual(phx_state.free_slots, 1)
        self.assertEqual(phx_state.grant_epoch, 6)

        east_row = _replica(1, east.to_pickleable())
        east_row.reserved_fill = True
        east_row.reserved_fill_pool_key = east_pool
        east_row.reserved_fill_service_generation = 1
        east_row.reserved_fill_physical_cluster_uid = 'east-uid'
        decisions = autoscaler._apply_reserved_capacity_fill(
            [east_row], [autoscalers.AutoscalerDecision(_SCALE_DOWN, 1)])
        # The failed pool's existing row remains sheltered, while only the
        # healthy pool may emit a fresh launch.
        self.assertEqual(_downs(decisions), [])
        fill_pools = [
            decision.target[_POOL_KEY] for decision in _ups(decisions) if
            isinstance(decision.target, dict) and decision.target.get(_FILL_KEY)
        ]
        self.assertEqual(fill_pools, [phx_pool])

    def test_failed_round_preserves_only_pool_local_shelter(self):
        self._assert_one_pool_round_failure_isolated(lock_timeout=False)

    def test_round_lock_timeout_preserves_only_pool_local_shelter(self):
        self._assert_one_pool_round_failure_isolated(lock_timeout=True)


class TestStaleSnapshot(unittest.TestCase):
    """Stale snapshot: 0 free contribution, existing fill still protected."""

    def test_stale_free_slots_contribute_zero(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [_replica(1, _K8S_KEY), _replica(2, _K8S_KEY)]
        _feed(autoscaler, 5, timestamp=_stale_timestamp())
        decisions = _decisions(autoscaler, replicas)
        # fill_target = 2 zero-cost + 0 (stale): exactly covers the
        # existing fill replicas -- no new fill ups, no victimization of
        # the live fill fleet just because the poller died.
        self.assertEqual(decisions, [])
        self.assertEqual(autoscaler.info()['fill_target'], 2)

    def test_fresh_snapshot_contributes(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [_replica(1, _K8S_KEY), _replica(2, _K8S_KEY)]
        _feed(autoscaler, 5)
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(
            len([d for d in _ups(decisions) if d.target == {
                _FILL_KEY: True
            }]), 5)  # fill_target 7 - current 2


class TestIncreaseDamping(unittest.TestCase):
    """Increases need two consecutive polls; decreases apply at once."""

    def test_two_poll_increase_immediate_decrease(self):
        autoscaler = _make_autoscaler()

        def free_slots():
            return autoscaler.info()['fill_free_slots']

        _feed(autoscaler, 5, polls=1)
        self.assertEqual(free_slots(), 0)  # first sight of an increase
        _feed(autoscaler, 5, polls=1)
        self.assertEqual(free_slots(), 5)  # persisted across two polls
        _feed(autoscaler, 8, polls=1)
        self.assertEqual(free_slots(), 5)  # new increase: wait again
        _feed(autoscaler, 9, polls=1)
        self.assertEqual(free_slots(), 8)  # min of the two above-5 polls
        _feed(autoscaler, 2, polls=1)
        self.assertEqual(free_slots(), 2)  # decrease: immediate

    def test_transient_spike_never_acted_on(self):
        autoscaler = _make_autoscaler()
        _feed(autoscaler, 0, polls=2)
        _feed(autoscaler, 10, polls=1)
        _feed(autoscaler, 0, polls=1)
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)


class TestMultiPoolAutoscaler(unittest.TestCase):
    """Protocol-v2 feed and launch authority stays pool-local."""

    def setUp(self):
        self.east = make_location('east-context',
                                  accelerators={'L4': 1},
                                  cloud_name='Kubernetes',
                                  use_spot=False)
        self.phx = make_location('phx-context',
                                 accelerators={'H200': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        self.east_pool = reserved_capacity_broker.make_pool_key(
            'east-context',
            'l4',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='east-uid')
        self.phx_pool = reserved_capacity_broker.make_pool_key(
            'phx-context',
            'h200',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='phx-uid')

    @staticmethod
    def _exact_card_autoscaler(shapes):
        spec = service_spec.SkyServiceSpec(readiness_path='/health',
                                           initial_delay_seconds=60,
                                           readiness_timeout_seconds=30,
                                           endpoint_probe_interval_seconds=10,
                                           lb_stream_timeout_seconds=60,
                                           min_replicas=1,
                                           max_replicas=5,
                                           target_concurrency_per_replica=1,
                                           reserved_capacity_fill=True)
        autoscaler = autoscalers.ConcurrencyAutoscaler('svc', spec)
        autoscaler.set_configured_accelerator_shapes(shapes)
        return autoscaler

    def _snapshots(self, generation=1, east_feed=2, phx_feed=2):
        timestamp = time.time()
        return {
            self.east_pool: {
                'protocol_version': 2,
                'pool_key': self.east_pool,
                'physical_cluster_uid': 'east-uid',
                'service_generation': generation,
                'edge_cap': 2,
                'zero_cost_location_keys': [self.east.to_pickleable()],
                'free_slots': east_feed,
                'free_slots_by_accelerator': {
                    'l4': east_feed
                },
                'grant': 2,
                'grant_epoch': 11,
                'timestamp': timestamp,
            },
            self.phx_pool: {
                'protocol_version': 2,
                'pool_key': self.phx_pool,
                'physical_cluster_uid': 'phx-uid',
                'service_generation': generation,
                'edge_cap': 2,
                'zero_cost_location_keys': [self.phx.to_pickleable()],
                'free_slots': phx_feed,
                'free_slots_by_accelerator': {
                    'h200': phx_feed
                },
                'grant': 2,
                'grant_epoch': 17,
                'timestamp': timestamp,
            },
        }

    def test_launches_carry_exact_pool_generation_uid_and_locations(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=5)
        snapshots = self._snapshots()
        autoscaler.collect_reserved_capacity_pools(snapshots)
        autoscaler.collect_reserved_capacity_pools(snapshots)

        fill_ups = [
            decision for decision in _ups(_decisions(autoscaler, []))
            if decision.target is not None
        ]
        self.assertEqual(len(fill_ups), 4)
        by_pool = {self.east_pool: 0, self.phx_pool: 0}
        for decision in fill_ups:
            override = decision.target
            pool_key = override[_POOL_KEY]
            by_pool[pool_key] += 1
            self.assertEqual(override[_PROTOCOL_KEY], 2)
            self.assertEqual(override[_GENERATION_KEY], 1)
            expected_uid = ('east-uid'
                            if pool_key == self.east_pool else 'phx-uid')
            self.assertEqual(
                override[
                    constants.RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY],
                expected_uid)
            self.assertEqual(
                len(override[
                    constants.RESERVED_FILL_ALLOWED_LOCATIONS_OVERRIDE_KEY]), 1)
            expected_card = ('L4' if pool_key == self.east_pool else 'H200')
            self.assertEqual(override['accelerators'], {expected_card: 1})
        self.assertEqual(by_pool, {self.east_pool: 2, self.phx_pool: 2})

    def test_exact_measurement_selects_available_card_in_mixed_pool(self):
        mixed_l4 = make_location('phx-context',
                                 accelerators={'L4': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        mixed_h200 = make_location('phx-context',
                                   accelerators={'H200': 1},
                                   cloud_name='Kubernetes',
                                   use_spot=False)
        mixed_pool = reserved_capacity_broker.make_pool_key(
            'phx-context', ('L4', 'H200'),
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='mixed-uid')
        snapshots = {
            mixed_pool: {
                'protocol_version': 2,
                'pool_key': mixed_pool,
                'physical_cluster_uid': 'mixed-uid',
                'service_generation': 1,
                'edge_cap': 2,
                'zero_cost_location_keys': [
                    mixed_l4.to_pickleable(),
                    mixed_h200.to_pickleable(),
                ],
                'free_slots': 1,
                # L4 is the first stable location, but only H200 is measured
                # free.  Aggregate-only emission used to pick L4 here.
                'free_slots_by_accelerator': {
                    'h200': 1
                },
                'grant': 2,
                'grant_epoch': 19,
                'timestamp': time.time(),
            }
        }
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=3)
        autoscaler.collect_reserved_capacity_pools(snapshots)
        autoscaler.collect_reserved_capacity_pools(snapshots)

        fill_ups = [
            decision for decision in _ups(_decisions(autoscaler, [])) if
            isinstance(decision.target, dict) and decision.target.get(_FILL_KEY)
        ]
        self.assertEqual(len(fill_ups), 1)
        self.assertEqual(fill_ups[0].target['accelerators'], {'H200': 1})

    def test_old_v2_round_without_exact_split_remains_compatible(self):
        snapshots = self._snapshots(east_feed=1, phx_feed=0)
        for snapshot in snapshots.values():
            snapshot.pop('free_slots_by_accelerator')
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=3)
        autoscaler.collect_reserved_capacity_pools(snapshots)
        autoscaler.collect_reserved_capacity_pools(snapshots)

        fill_ups = [
            decision for decision in _ups(_decisions(autoscaler, [])) if
            isinstance(decision.target, dict) and decision.target.get(_FILL_KEY)
        ]
        self.assertEqual(len(fill_ups), 1)
        self.assertNotIn('accelerators', fill_ups[0].target)

    def test_malformed_exact_card_feed_fails_snapshot_ingestion(self):
        snapshots = self._snapshots(east_feed=2, phx_feed=0)
        snapshots[self.east_pool]['free_slots_by_accelerator'] = {'l4': 1}
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=3)
        with self.assertRaisesRegex(ValueError, 'must sum'):
            autoscaler.collect_reserved_capacity_pools(snapshots)

    def test_demand_debits_only_the_compatible_accelerator_pool(self):
        autoscaler = self._exact_card_autoscaler({'L4': 1, 'H200': 1})
        autoscaler.set_free_reserved_slots_by_accelerator({'H200': 1})
        snapshots = self._snapshots()
        autoscaler.collect_reserved_capacity_pools(snapshots)
        autoscaler.collect_reserved_capacity_pools(snapshots)
        demand = autoscalers.AutoscalerDecision(_SCALE_UP,
                                                {'accelerators': {
                                                    'H200': 1
                                                }})

        decisions = autoscaler._apply_reserved_capacity_fill([], [demand])

        fill_by_pool = {self.east_pool: 0, self.phx_pool: 0}
        for decision in decisions:
            if (not isinstance(decision.target, dict) or
                    not decision.target.get(_FILL_KEY)):
                continue
            fill_by_pool[decision.target[_POOL_KEY]] += 1
        # The H200 demand launch may consume PHX's reported slot. It must not
        # debit the earlier L4 pool and then authorize duplicate H200 fill.
        self.assertEqual(fill_by_pool, {self.east_pool: 2, self.phx_pool: 1})

    def test_same_card_demand_conservatively_debits_every_possible_pool(self):
        east_h200 = make_location('east-context',
                                  accelerators={'H200': 1},
                                  cloud_name='Kubernetes',
                                  use_spot=False)
        east_h200_pool = reserved_capacity_broker.make_pool_key(
            'east-context',
            'h200',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='east-uid')
        timestamp = time.time()
        snapshots = {
            east_h200_pool: {
                'protocol_version': 2,
                'pool_key': east_h200_pool,
                'physical_cluster_uid': 'east-uid',
                'service_generation': 1,
                'edge_cap': 2,
                'zero_cost_location_keys': [east_h200.to_pickleable()],
                'free_slots': 2,
                'free_slots_by_accelerator': {
                    'h200': 2
                },
                'grant': 2,
                'grant_epoch': 11,
                'timestamp': timestamp,
            },
            self.phx_pool: {
                'protocol_version': 2,
                'pool_key': self.phx_pool,
                'physical_cluster_uid': 'phx-uid',
                'service_generation': 1,
                'edge_cap': 2,
                'zero_cost_location_keys': [self.phx.to_pickleable()],
                'free_slots': 2,
                'free_slots_by_accelerator': {
                    'h200': 2
                },
                'grant': 2,
                'grant_epoch': 17,
                'timestamp': timestamp,
            },
        }
        autoscaler = self._exact_card_autoscaler({'H200': 1})
        autoscaler.set_free_reserved_slots_by_accelerator({'H200': 1})
        autoscaler.collect_reserved_capacity_pools(snapshots)
        autoscaler.collect_reserved_capacity_pools(snapshots)
        demand = autoscalers.AutoscalerDecision(_SCALE_UP,
                                                {'accelerators': {
                                                    'H200': 1
                                                }})

        decisions = autoscaler._apply_reserved_capacity_fill([], [demand])

        fill_by_pool = {east_h200_pool: 0, self.phx_pool: 0}
        for decision in decisions:
            if (not isinstance(decision.target, dict) or
                    not decision.target.get(_FILL_KEY)):
                continue
            fill_by_pool[decision.target[_POOL_KEY]] += 1
        # The ordinary decision has no context yet. Withholding one slot from
        # both compatible pools is conservative, but prevents over-issuing no
        # matter which context placement eventually selects.
        self.assertEqual(fill_by_pool, {east_h200_pool: 1, self.phx_pool: 1})

    def test_generation_change_invalidates_old_feed_and_restarts_damping(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=5)
        snapshots = self._snapshots(generation=1)
        autoscaler.collect_reserved_capacity_pools(snapshots)
        autoscaler.collect_reserved_capacity_pools(snapshots)
        self.assertEqual(
            len([
                decision for decision in _ups(_decisions(autoscaler, []))
                if decision.target is not None
            ]), 4)

        autoscaler.collect_reserved_capacity_pools(
            self._snapshots(generation=2))
        self.assertEqual([
            decision for decision in _ups(_decisions(autoscaler, []))
            if decision.target is not None
        ], [])
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)

    def test_shelter_carry_requires_exact_generation_and_uid_and_clips(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=5)
        autoscaler.collect_reserved_capacity_pools(self._snapshots())

        self.assertEqual(
            autoscaler.get_reserved_capacity_pool_shelter_grant(
                self.east_pool,
                service_generation=1,
                physical_cluster_uid='east-uid',
                edge_cap=1), 1)
        self.assertEqual(
            autoscaler.get_reserved_capacity_pool_shelter_grant(
                self.east_pool,
                service_generation=2,
                physical_cluster_uid='east-uid',
                edge_cap=2), 0)
        self.assertEqual(
            autoscaler.get_reserved_capacity_pool_shelter_grant(
                self.east_pool,
                service_generation=1,
                physical_cluster_uid='replacement-uid',
                edge_cap=2), 0)

    def test_dynamic_restore_shelters_holdings_but_authorizes_no_feed(self):
        old = _make_autoscaler(min_replicas=1, max_replicas=5)
        snapshots = self._snapshots()
        old.collect_reserved_capacity_pools(snapshots)
        old.collect_reserved_capacity_pools(snapshots)
        restored = _make_autoscaler(min_replicas=1, max_replicas=5)
        restored.load_dynamic_states(old.dump_dynamic_states())
        east_row = _replica(1, self.east.to_pickleable())
        east_row.reserved_fill = True
        phx_row = _replica(2, self.phx.to_pickleable())
        phx_row.reserved_fill = True

        decisions = _decisions(restored, [east_row, phx_row])
        self.assertEqual(_ups(decisions), [])
        self.assertEqual(_downs(decisions), [])
        self.assertEqual(restored.info()['fill_free_slots'], 0)

    def test_update_preserves_only_the_last_real_grant_for_shelter(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=5)
        snapshots = self._snapshots(east_feed=2, phx_feed=0)
        snapshots[self.east_pool]['grant'] = 1
        snapshots[self.phx_pool]['grant'] = 0
        autoscaler.collect_reserved_capacity_pools(snapshots)
        autoscaler.collect_reserved_capacity_pools(snapshots)

        autoscaler.update_version(2, _spec(min_replicas=0, max_replicas=5),
                                  serve_utils.DEFAULT_UPDATE_MODE)

        state = autoscaler._fill_pool_states[self.east_pool]
        self.assertEqual(state.edge_cap, 2)
        self.assertEqual(state.shelter_grant, 1)
        self.assertEqual(state.grant, 0)
        self.assertEqual(state.free_slots, 0)
        self.assertIsNone(state.grant_epoch)
        rows = [
            _replica(1, self.east.to_pickleable()),
            _replica(2, self.east.to_pickleable())
        ]
        for row in rows:
            row.reserved_fill = True
            row.reserved_fill_pool_key = self.east_pool
            row.reserved_fill_service_generation = 1
            row.reserved_fill_physical_cluster_uid = 'east-uid'
        ordinary = [
            autoscalers.AutoscalerDecision(_SCALE_DOWN, row.replica_id)
            for row in rows
        ]
        decisions = autoscaler._apply_reserved_capacity_fill(rows, ordinary)
        # One holding remains sheltered by the real grant. Expanding shelter to
        # edge_cap would incorrectly suppress both scale-downs.
        self.assertEqual(len(_downs(decisions)), 1)
        self.assertEqual(_ups(decisions), [])

    def test_dynamic_restore_uses_real_grant_as_nonlaunching_shelter(self):
        old = _make_autoscaler(min_replicas=0, max_replicas=5)
        snapshots = self._snapshots(east_feed=2, phx_feed=0)
        snapshots[self.east_pool]['grant'] = 1
        snapshots[self.phx_pool]['grant'] = 0
        old.collect_reserved_capacity_pools(snapshots)
        old.collect_reserved_capacity_pools(snapshots)
        dumped = old.dump_dynamic_states()
        self.assertEqual(
            dumped['reserved_capacity_fill_state']['pools'][self.east_pool]
            ['shelter_grant'], 1)

        restored = _make_autoscaler(min_replicas=0, max_replicas=5)
        restored.load_dynamic_states(dumped)

        state = restored._fill_pool_states[self.east_pool]
        self.assertEqual(state.shelter_grant, 1)
        self.assertEqual(state.grant, 0)
        self.assertEqual(state.free_slots, 0)
        self.assertIsNone(state.grant_epoch)
        rows = [
            _replica(1, self.east.to_pickleable()),
            _replica(2, self.east.to_pickleable())
        ]
        for row in rows:
            row.reserved_fill = True
            row.reserved_fill_pool_key = self.east_pool
            row.reserved_fill_service_generation = 1
            row.reserved_fill_physical_cluster_uid = 'east-uid'
        ordinary = [
            autoscalers.AutoscalerDecision(_SCALE_DOWN, row.replica_id)
            for row in rows
        ]
        decisions = restored._apply_reserved_capacity_fill(rows, ordinary)
        self.assertEqual(len(_downs(decisions)), 1)
        self.assertEqual(_ups(decisions), [])

    def test_dynamic_restore_invalid_shelter_grant_fails_closed(self):
        old = _make_autoscaler(min_replicas=0, max_replicas=5)
        snapshots = self._snapshots(east_feed=2, phx_feed=0)
        old.collect_reserved_capacity_pools(snapshots)
        old.collect_reserved_capacity_pools(snapshots)
        for malformed in ('missing', None, True, -1, '2'):
            with self.subTest(malformed=malformed):
                dumped = old.dump_dynamic_states()
                raw_pool = dumped['reserved_capacity_fill_state']['pools'][
                    self.east_pool]
                if malformed == 'missing':
                    raw_pool.pop('shelter_grant')
                else:
                    raw_pool['shelter_grant'] = malformed
                restored = _make_autoscaler(min_replicas=0, max_replicas=5)
                restored.load_dynamic_states(dumped)
                state = restored._fill_pool_states[self.east_pool]
                self.assertEqual(state.shelter_grant, 0)
                self.assertEqual(state.grant, 0)
                self.assertEqual(state.free_slots, 0)
                self.assertIsNone(state.grant_epoch)

    def test_explicit_contradictory_provenance_gets_no_holding_or_shelter(self):
        cases = {
            'partial': {
                'reserved_fill_pool_key': self.east_pool,
            },
            'unknown-pool': {
                'reserved_fill_pool_key': 'retired-pool',
                'reserved_fill_service_generation': 1,
                'reserved_fill_physical_cluster_uid': 'east-uid',
            },
            'future-generation': {
                'reserved_fill_pool_key': self.east_pool,
                'reserved_fill_service_generation': 3,
                'reserved_fill_physical_cluster_uid': 'east-uid',
            },
            'uid-mismatch': {
                'reserved_fill_pool_key': self.east_pool,
                'reserved_fill_service_generation': 1,
                'reserved_fill_physical_cluster_uid': 'replacement-uid',
            },
        }
        for name, provenance in cases.items():
            with self.subTest(name=name):
                autoscaler = _make_autoscaler(min_replicas=0, max_replicas=5)
                autoscaler.collect_reserved_capacity_pools(
                    self._snapshots(generation=2, east_feed=0, phx_feed=0))
                row = _replica(1, self.east.to_pickleable())
                row.reserved_fill = True
                for field, value in provenance.items():
                    setattr(row, field, value)

                holdings = autoscaler.count_zero_cost_holdings_by_pool([row])
                self.assertEqual(holdings[self.east_pool], (0, 0))
                decisions = autoscaler._apply_reserved_capacity_fill(
                    [row], [autoscalers.AutoscalerDecision(_SCALE_DOWN, 1)])
                self.assertEqual(len(_downs(decisions)), 1)

    def test_retargeted_explicit_row_does_not_fall_back_by_location(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=5)
        autoscaler.collect_reserved_capacity_pools(
            self._snapshots(generation=2, east_feed=0, phx_feed=0))
        # The row claims PHX provenance but its persisted placement is east.
        # Neither the claimed pool nor the coincidentally matching east pool
        # may adopt it.
        row = _replica(1, self.east.to_pickleable())
        row.reserved_fill = True
        row.reserved_fill_pool_key = self.phx_pool
        row.reserved_fill_service_generation = 1
        row.reserved_fill_physical_cluster_uid = 'phx-uid'

        holdings = autoscaler.count_zero_cost_holdings_by_pool([row])
        self.assertEqual(holdings[self.east_pool], (0, 0))
        self.assertEqual(holdings[self.phx_pool], (0, 0))
        decisions = autoscaler._apply_reserved_capacity_fill(
            [row], [autoscalers.AutoscalerDecision(_SCALE_DOWN, 1)])
        self.assertEqual(len(_downs(decisions)), 1)

    def test_older_launch_generation_remains_valid_pool_provenance(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=5)
        autoscaler.collect_reserved_capacity_pools(
            self._snapshots(generation=2, east_feed=0, phx_feed=0))
        row = _replica(1, self.east.to_pickleable())
        row.reserved_fill = True
        row.reserved_fill_pool_key = self.east_pool
        row.reserved_fill_service_generation = 1
        row.reserved_fill_physical_cluster_uid = 'east-uid'

        holdings = autoscaler.count_zero_cost_holdings_by_pool([row])
        self.assertEqual(holdings[self.east_pool], (1, 0))
        decisions = autoscaler._apply_reserved_capacity_fill(
            [row], [autoscalers.AutoscalerDecision(_SCALE_DOWN, 1)])
        self.assertEqual(_downs(decisions), [])

    def test_provenance_free_legacy_row_keeps_location_fallback(self):
        autoscaler = _make_autoscaler(min_replicas=0, max_replicas=5)
        autoscaler.collect_reserved_capacity_pools(
            self._snapshots(generation=2, east_feed=0, phx_feed=0))
        row = _replica(1, self.east.to_pickleable())
        row.reserved_fill = True

        holdings = autoscaler.count_zero_cost_holdings_by_pool([row])
        self.assertEqual(holdings[self.east_pool], (1, 0))
        decisions = autoscaler._apply_reserved_capacity_fill(
            [row], [autoscalers.AutoscalerDecision(_SCALE_DOWN, 1)])
        self.assertEqual(_downs(decisions), [])

    def test_protocol_demotion_clears_v2_map_and_accepts_v1_snapshots(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=5)
        snapshots = self._snapshots()
        autoscaler.collect_reserved_capacity_pools(snapshots)
        autoscaler.collect_reserved_capacity_pools(snapshots)
        self.assertIn('fill_by_pool', autoscaler.info())

        autoscaler.collect_reserved_capacity(0, [_K8S_KEY],
                                             time.time(),
                                             protocol_version=1)
        autoscaler.collect_reserved_capacity(2, [_K8S_KEY],
                                             time.time(),
                                             protocol_version=1)
        autoscaler.collect_reserved_capacity(2, [_K8S_KEY],
                                             time.time(),
                                             protocol_version=1)
        self.assertNotIn('fill_by_pool', autoscaler.info())
        self.assertEqual(autoscaler.info()['fill_free_slots'], 2)


class TestCapacityHintDemandOnly(unittest.TestCase):
    """Fill never inflates the demand target the capacity hint reports."""

    def test_final_target_unchanged_by_fill(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        _feed(autoscaler, 5)
        decisions = _decisions(autoscaler, [])
        self.assertGreater(len(_ups(decisions)), 1)  # fill engaged
        self.assertEqual(autoscaler.get_final_target_num_replicas(), 1)
        info = autoscaler.info()
        self.assertEqual(info['target_num_replicas'], 1)
        self.assertEqual(info['fill_target'], 5)


def _make_location(region, cost_marker, use_spot=False):
    del cost_marker  # readability only; cost set via _make_placer
    return make_location(region, accelerators={'A100': 1}, use_spot=use_spot)


def _make_manager(placer):
    """Bare SkyPilotReplicaManager wired for the launch-path tests."""
    if placer is not None:
        # Mock placers do not implement SpotPlacer's retry-state transitions;
        # start them clean so unrelated durable-state persistence does not
        # turn a consumed/released retry assertion into a database fixture.
        placer.retry_state_dirty = False
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    # Production managers persist protocol-v2 fills only under an immutable
    # service-incarnation scope.  Keep this synthetic launch-path manager
    # faithful to that invariant so the tests reach the pool/UID/epoch fences
    # they are intended to exercise.
    manager._service_hash = 'service-hash'
    manager._resource_scope = 'service-hash'
    manager._controller_owner = (123, '10.0.0.1')
    manager._enforce_launch_fence = True
    manager.yaml_content = 'unused: patched helpers below'
    manager._spot_placer = placer
    manager._launch_thread_pool = {}
    manager._replica_to_request_id = {}
    manager._replica_to_launch_cancelled = {}
    manager._fill_skip_last_log_time = 0.0
    manager._next_replica_id = 7
    manager.latest_version = 1
    return manager


class TestZeroCostSelection(unittest.TestCase):
    """select_next_zero_cost_location: zero-cost ACTIVE or nothing."""

    def setUp(self):
        self.k8s = _make_location('research-ctx', 'free')
        self.paid = _make_location('us-east-1', 'paid', use_spot=True)
        self.placer = _make_placer({self.k8s: 0.0, self.paid: 0.2})

    def test_returns_active_zero_cost(self):
        self.assertEqual(self.placer.select_next_zero_cost_location(), self.k8s)

    def test_benched_zero_cost_returns_none_never_paid(self):
        with mock.patch.object(spot_placer.time, 'time', return_value=1000.0):
            self.placer.set_preemptive(self.k8s)
            self.assertIsNone(self.placer.select_next_zero_cost_location())

    def test_no_zero_cost_at_all_returns_none(self):
        placer = _make_placer({self.paid: 0.2})
        self.assertIsNone(placer.select_next_zero_cost_location())

    def test_enumeration_includes_benched(self):
        with mock.patch.object(spot_placer.time, 'time', return_value=1000.0):
            self.placer.set_preemptive(self.k8s)
            self.assertIn(self.k8s, self.placer.zero_cost_locations())

    def test_catalog_enumeration_ignores_unknown_paid_candidates(self):
        self.placer.location2cost.pop(self.paid)
        self.placer.location2status.update({
            _make_location(f'paid-region-{index}', 'paid', use_spot=True):
                spot_placer.LocationStatus.ACTIVE for index in range(1058)
        })
        self.assertEqual(self.placer.zero_cost_locations(), [self.k8s])

    def test_equal_cost_reuses_first_candidate(self):
        other = _make_location('research-ctx-2', 'free')
        placer = _make_placer({self.k8s: 0.0, other: 0.0, self.paid: 0.2})
        selected = placer.select_next_zero_cost_location()
        self.assertEqual(selected, self.k8s)


class TestProtocolV2DurableLaunchFence(unittest.TestCase):
    """Durable fill context accepts only a complete, coherent v2 tuple."""

    def setUp(self):
        self.pool_key = reserved_capacity_broker.make_pool_key(
            'phx-context', ['H200', 'L4'],
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='physical-uid')

    def _context(self):
        return reserved_capacity.make_protocol_v2_launch_fence(
            pool_key=self.pool_key,
            service_generation=7,
            physical_cluster_uid='physical-uid',
            kubernetes_context='phx-context',
            accelerator='H200',
            accelerator_count=1)

    def test_round_trip_canonicalizes_accelerator(self):
        context = self._context()
        fence = reserved_capacity.parse_protocol_v2_launch_fence(context)
        self.assertIsNotNone(fence)
        assert fence is not None
        self.assertEqual(fence.protocol_version, 2)
        self.assertEqual(fence.pool_key, self.pool_key)
        self.assertEqual(fence.service_generation, 7)
        self.assertEqual(fence.physical_cluster_uid, 'physical-uid')
        self.assertEqual(fence.kubernetes_context, 'phx-context')
        self.assertEqual(fence.accelerator, 'h200')
        self.assertEqual(fence.accelerator_count, 1)
        self.assertEqual(
            context[constants.RESERVED_FILL_LAUNCH_ACCELERATOR_KEY], 'h200')

    def test_ordinary_context_has_no_fill_fence(self):
        self.assertIsNone(
            reserved_capacity.parse_protocol_v2_launch_fence(
                {constants.REPLICA_LAUNCH_FENCE_SERVICE_NAME_KEY: 'svc'}))

    def test_partial_or_unknown_prefixed_context_is_rejected(self):
        for context in ({
                constants.RESERVED_FILL_LAUNCH_POOL_KEY: self.pool_key,
        }, {
                **self._context(),
                f'{constants.RESERVED_FILL_LAUNCH_FENCE_PREFIX}unknown': True,
        }):
            with self.subTest(context=context), self.assertRaises(ValueError):
                reserved_capacity.parse_protocol_v2_launch_fence(context)

    def test_malformed_or_contradictory_context_is_rejected(self):
        cases = []
        for key, value in (
            (constants.RESERVED_FILL_LAUNCH_PROTOCOL_VERSION_KEY, True),
            (constants.RESERVED_FILL_LAUNCH_SERVICE_GENERATION_KEY, 0),
            (constants.RESERVED_FILL_LAUNCH_ACCELERATOR_COUNT_KEY, 1.0),
            (constants.RESERVED_FILL_LAUNCH_KUBERNETES_CONTEXT_KEY, ''),
            (constants.RESERVED_FILL_LAUNCH_PHYSICAL_CLUSTER_UID_KEY,
             'replacement-uid'),
            (constants.RESERVED_FILL_LAUNCH_ACCELERATOR_KEY, 'A100'),
        ):
            context = self._context()
            context[key] = value
            cases.append((key, context))
        for key, context in cases:
            with self.subTest(key=key), self.assertRaises(ValueError):
                reserved_capacity.parse_protocol_v2_launch_fence(context)


class TestFillLaunchPath(unittest.TestCase):
    """Sentinel launches pin zero-cost-only; aborts leak nothing."""

    @staticmethod
    def _v2_override(location, *, epoch=3, gpu='H200', exact_shape=None):
        pool = reserved_capacity_broker.make_pool_key(
            location.region,
            gpu,
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='physical-uid')
        override = {
            _FILL_KEY: True,
            _PROTOCOL_KEY: 2,
            _GENERATION_KEY: 7,
            _POOL_KEY: pool,
            _EPOCH_KEY: epoch,
            constants.RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY: 'physical-uid',
            constants.RESERVED_FILL_ALLOWED_LOCATIONS_OVERRIDE_KEY: [
                location.to_pickleable()
            ],
        }
        if exact_shape is not None:
            override['accelerators'] = exact_shape
        return override

    @staticmethod
    def _launch_v2(manager, override):
        with provider_phase.provider_phase(
                provider_phase.ProviderPhaseMode.V2_FENCED) as admission:
            return manager._launch_replica(7,
                                           override,
                                           provider_phase_admission=admission)

    def test_v2_batch_keeps_one_lock_and_defers_phase_to_each_item(self):
        manager = _make_manager(None)
        events = []

        class _Lock:

            def __enter__(self):
                events.append('lock-enter')

            def __exit__(self, *_args):
                events.append('lock-exit')

        manager.lock = _Lock()
        manager._batch_needs_placement_snapshot = mock.Mock(return_value=False)
        manager._scale_up_batch_locked = mock.Mock(
            side_effect=lambda *_args, **_kwargs: events.append('scale'))
        override = {
            _FILL_KEY: True,
            _PROTOCOL_KEY: reserved_capacity_broker.PROTOCOL_V2,
        }

        with mock.patch.object(provider_phase, 'provider_phase') as phase, \
             mock.patch.object(provider_phase,
                               'try_provider_phase') as try_phase:
            manager.scale_up_batch([override])

        self.assertEqual(events, ['lock-enter', 'scale', 'lock-exit'])
        self.assertEqual(manager._scale_up_batch_locked.call_count, 1)
        self.assertNotIn('provider_phase_admission',
                         manager._scale_up_batch_locked.call_args.kwargs)
        # The mocked batch body owns item dispatch. The public wrapper must
        # never wait for a phase while holding or before acquiring its lock.
        phase.assert_not_called()
        try_phase.assert_not_called()

    def test_v2_batch_drops_conflicting_uids_and_preserves_unrelated(self):
        phx = make_location('phx-context',
                            accelerators={'H200': 1},
                            cloud_name='Kubernetes',
                            use_spot=False)
        east = make_location('east-context',
                             accelerators={'H200': 1},
                             cloud_name='Kubernetes',
                             use_spot=False)
        phx_a = self._v2_override(phx)
        phx_b = self._v2_override(phx)
        phx_b[constants.RESERVED_FILL_PHYSICAL_CLUSTER_UID_OVERRIDE_KEY] = (
            'physical-uid-b')
        phx_b[_POOL_KEY] = reserved_capacity_broker.make_pool_key(
            phx.region,
            'H200',
            protocol_version=reserved_capacity_broker.PROTOCOL_V2,
            physical_cluster_uid='physical-uid-b')
        ordinary = {'use_spot': True}
        east_v2 = self._v2_override(east)
        overrides = [phx_a, ordinary, phx_b, east_v2]

        manager = _make_manager(None)
        manager.lock = threading.RLock()
        manager._batch_needs_placement_snapshot = mock.Mock(return_value=False)
        manager._scale_up_batch_locked = mock.Mock()
        manager._log_fill_skip = mock.Mock()

        manager.scale_up_batch(overrides)

        manager._scale_up_batch_locked.assert_called_once()
        self.assertEqual(manager._scale_up_batch_locked.call_args.args[0],
                         [ordinary, east_v2])
        self.assertEqual(manager._log_fill_skip.call_count, 2)
        # Batch filtering never mutates the caller-owned decision list.
        self.assertEqual(overrides, [phx_a, ordinary, phx_b, east_v2])

    def test_v2_missing_epoch_fails_before_persist(self):
        location = make_location('phx-context',
                                 accelerators={'H200': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [location]
        manager = _make_manager(placer)
        override = self._v2_override(location)
        override.pop(_EPOCH_KEY)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica') as persist:
            self.assertFalse(self._launch_v2(manager, override))
        persist.assert_not_called()

    def test_v2_batch_item_requests_zero_wait_phase_at_persist_seam(self):
        manager = _make_manager(None)
        override = {
            _FILL_KEY: True,
            _PROTOCOL_KEY: reserved_capacity_broker.PROTOCOL_V2,
        }
        manager._launch_replica = mock.Mock(return_value=False)

        self.assertFalse(manager._scale_up_one_locked(override, set()))

        manager._launch_replica.assert_called_once_with(
            manager._next_replica_id,
            override,
            try_provider_phase_admission=True)

    def test_v2_batch_busy_phase_leaks_no_row_or_launch_thread(self):
        location = make_location('phx-context',
                                 accelerators={'H200': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [location]
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)

        class _BusyPhase:

            def __enter__(self):
                raise exceptions.ProviderPhaseBusyError('ambient waiter queued')

            def __exit__(self, *_args):
                return False

        def _busy_phase(mode):
            self.assertEqual(mode, provider_phase.ProviderPhaseMode.V2_FENCED)
            return _BusyPhase()

        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(reserved_capacity_broker,
                               'current_epoch',
                               return_value=3), \
             mock.patch.object(provider_phase,
                               'try_provider_phase',
                               side_effect=_busy_phase), \
             mock.patch.object(kubernetes_adaptor,
                               'physical_cluster_uid_fence') as physical, \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica') as persist, \
             mock.patch.object(replica_managers,
                               '_ReplicaLaunchThread') as launch_thread:
            with self.assertRaises(exceptions.ProviderPhaseBusyError):
                manager._launch_replica(7,
                                        self._v2_override(location),
                                        try_provider_phase_admission=True)

        physical.assert_not_called()
        persist.assert_not_called()
        launch_thread.assert_not_called()
        self.assertNotIn(7, manager._launch_thread_pool)
        placer.release_retry.assert_called_once_with(location)

    def test_v2_batch_yields_to_fifo_ambient_between_items(self):
        location = make_location('phx-context',
                                 accelerators={'H200': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [location]
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
        existing = []

        class _CountingLock:
            """Records manager-lock acquisition by thread name."""

            def __init__(self):
                self._lock = threading.Lock()
                self.entries = []

            def __enter__(self):
                self._lock.acquire()
                self.entries.append(threading.current_thread().name)
                return self

            def __exit__(self, *_args):
                self._lock.release()

        manager.lock = _CountingLock()
        manager._batch_needs_placement_snapshot = mock.Mock(return_value=True)
        manager._uses_shared_zero_cost_demand_budget = mock.Mock(
            return_value=False)
        first_persist = threading.Event()
        release_first = threading.Event()
        ambient_entered = threading.Event()
        ambient_got_manager = threading.Event()
        release_ambient = threading.Event()
        batch_done = threading.Event()
        errors = []
        persist_count = 0

        def _persist(*_args, **_kwargs):
            nonlocal persist_count
            persist_count += 1
            self.assertEqual(persist_count, 1,
                             'later item barged past the ambient FIFO root')
            first_persist.set()
            self.assertTrue(release_first.wait(timeout=5))
            return True

        def _ambient():
            try:
                with provider_phase.provider_phase(
                        provider_phase.ProviderPhaseMode.AMBIENT_LEGACY):
                    ambient_entered.set()
                    with manager.lock:
                        ambient_got_manager.set()
                    self.assertTrue(release_ambient.wait(timeout=5))
            except BaseException as error:  # pylint: disable=broad-exception-caught
                errors.append(error)

        def _batch(overrides):
            try:
                manager.scale_up_batch(overrides)
            except BaseException as error:  # pylint: disable=broad-exception-caught
                errors.append(error)
            finally:
                batch_done.set()

        @contextlib.contextmanager
        def _physical(context, uid, *, wait_for_initializer=True):
            self.assertEqual((context, uid, wait_for_initializer),
                             ('phx-context', 'physical-uid', False))
            yield

        overrides = [
            self._v2_override(location),
            self._v2_override(location),
            self._v2_override(location),
        ]
        real_try = provider_phase.try_provider_phase
        try_modes = []

        def _record_try(mode):
            try_modes.append(mode)
            return real_try(mode)

        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=existing), \
             mock.patch.object(replica_managers.paid_capacity,
                               'build_launch_budget',
                               return_value=None), \
             mock.patch.object(reserved_capacity_broker,
                               'current_epoch',
                               return_value=3), \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica',
                               side_effect=_persist) as persist, \
             mock.patch.object(kubernetes_adaptor,
                               'physical_cluster_uid_fence',
                               side_effect=_physical), \
             mock.patch.object(replica_managers,
                               '_ReplicaLaunchThread',
                               return_value=object()) as launch_thread, \
             mock.patch.object(provider_phase,
                               'try_provider_phase',
                               side_effect=_record_try):
            batch = threading.Thread(target=_batch,
                                     args=(overrides,),
                                     name='batch')
            ambient = threading.Thread(target=_ambient, name='ambient')
            batch.start()
            try:
                self.assertTrue(first_persist.wait(timeout=5))
                ambient.start()
                gate = provider_phase._PROVIDER_PHASE_GATE
                with gate._condition:
                    self.assertTrue(
                        gate._condition.wait_for(
                            lambda: any(waiter.mode == provider_phase.
                                        ProviderPhaseMode.AMBIENT_LEGACY
                                        for waiter in gate._queue),
                            timeout=5))
                release_first.set()
                self.assertTrue(batch_done.wait(timeout=5))
                self.assertTrue(ambient_entered.wait(timeout=5))
                self.assertTrue(ambient_got_manager.wait(timeout=5))
            finally:
                release_first.set()
                release_ambient.set()
                batch.join(timeout=5)
                if ambient.ident is not None:
                    ambient.join(timeout=5)

        self.assertEqual(errors, [])
        self.assertFalse(batch.is_alive())
        self.assertFalse(ambient.is_alive())
        self.assertEqual(manager.lock.entries.count('batch'), 1)
        self.assertEqual(try_modes, [
            provider_phase.ProviderPhaseMode.V2_FENCED,
            provider_phase.ProviderPhaseMode.V2_FENCED,
        ])
        persist.assert_called_once()
        launch_thread.assert_called_once()
        self.assertEqual(list(manager._launch_thread_pool), [7])
        self.assertEqual([info.replica_id for info in existing], [7])
        self.assertEqual(manager._next_replica_id, 8)

    def test_v2_batch_physical_initializer_is_zero_wait_and_retires_phase(self):
        location = make_location('phx-context',
                                 accelerators={'H200': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [location]
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
        physical_busy = exceptions.KubernetesPhysicalClusterFenceBusyError(
            'initializer busy', 'phx-context', 0)

        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(reserved_capacity_broker,
                               'current_epoch',
                               return_value=3), \
             mock.patch.object(
                 kubernetes_adaptor,
                 'physical_cluster_uid_fence',
                 side_effect=physical_busy) as physical, \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica') as persist, \
             mock.patch.object(replica_managers,
                               '_ReplicaLaunchThread') as launch_thread:
            with self.assertRaises(exceptions.ProviderPhaseBusyError):
                manager._launch_replica(7,
                                        self._v2_override(location),
                                        try_provider_phase_admission=True)

        physical.assert_called_once_with('phx-context',
                                         'physical-uid',
                                         wait_for_initializer=False)
        persist.assert_not_called()
        launch_thread.assert_not_called()
        self.assertNotIn(7, manager._launch_thread_pool)
        placer.release_retry.assert_called_once_with(location)
        # The failed item must retire its root before returning.
        with provider_phase.try_provider_phase(
                provider_phase.ProviderPhaseMode.AMBIENT_LEGACY):
            pass

    def test_v2_pool_key_rejects_selected_accelerator_mismatch(self):
        location = make_location('phx-context',
                                 accelerators={'A100': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [location]
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(reserved_capacity_broker,
                               'current_epoch',
                               return_value=3), \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica') as persist:
            self.assertFalse(
                self._launch_v2(manager, self._v2_override(location,
                                                           gpu='H200')))
        persist.assert_not_called()
        placer.release_retry.assert_called_once_with(location)

    def test_v2_uid_retarget_releases_consumed_retry(self):
        location = make_location('phx-context',
                                 accelerators={'H200': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [location]
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(reserved_capacity_broker,
                               'current_epoch',
                               return_value=3), \
             mock.patch.object(
                 kubernetes_adaptor,
                 'physical_cluster_uid_fence',
                 side_effect=exceptions.
                 KubernetesPhysicalClusterIdentityError('retargeted')), \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica') as persist:
            self.assertFalse(
                self._launch_v2(manager, self._v2_override(location)))
        persist.assert_not_called()
        placer.release_retry.assert_called_once_with(location)

    def test_v2_exact_shape_rejects_placer_returning_other_card(self):
        l4 = make_location('phx-context',
                           accelerators={'L4': 1},
                           cloud_name='Kubernetes',
                           use_spot=False)
        h200 = make_location('phx-context',
                             accelerators={'H200': 1},
                             cloud_name='Kubernetes',
                             use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [l4, h200]
        # Deliberately violate the allowed-location contract: the launch path
        # must independently enforce the exact shape before persistence.
        placer.select_next_zero_cost_location.return_value = l4
        manager = _make_manager(placer)
        override = self._v2_override(h200, exact_shape={'H200': 1})
        override[constants.RESERVED_FILL_ALLOWED_LOCATIONS_OVERRIDE_KEY] = [
            l4.to_pickleable(),
            h200.to_pickleable(),
        ]
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(reserved_capacity_broker,
                               'current_epoch',
                               return_value=3), \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica') as persist:
            self.assertFalse(self._launch_v2(manager, override))
        persist.assert_not_called()
        placer.release_retry.assert_called_once_with(l4)

    def test_v2_persist_fence_releases_consumed_retry(self):
        location = make_location('phx-context',
                                 accelerators={'H200': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [location]
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(reserved_capacity_broker,
                               'current_epoch',
                               return_value=3), \
             mock.patch.object(
                 kubernetes_adaptor,
                 'physical_cluster_uid_fence',
                 return_value=contextlib.nullcontext()), \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica',
                               return_value=False):
            self.assertFalse(
                self._launch_v2(manager, self._v2_override(location)))
        placer.release_retry.assert_called_once_with(location)

    def test_v2_thread_construction_failure_persists_nothing(self):
        location = make_location('phx-context',
                                 accelerators={'H200': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [location]
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(reserved_capacity_broker,
                               'current_epoch',
                               return_value=3), \
             mock.patch.object(
                 kubernetes_adaptor,
                 'physical_cluster_uid_fence',
                 return_value=contextlib.nullcontext()), \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica') as persist, \
             mock.patch.object(replica_managers,
                               '_ReplicaLaunchThread',
                               side_effect=RuntimeError('cannot freeze')):
            with self.assertRaisesRegex(RuntimeError, 'cannot freeze'):
                self._launch_v2(manager, self._v2_override(location))

        persist.assert_not_called()
        placer.release_retry.assert_called_once_with(location)
        self.assertNotIn(7, manager._launch_thread_pool)

    def test_v2_launch_persists_every_authority_field(self):
        location = make_location('phx-context',
                                 accelerators={'H200': 1},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [location]
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
        override = self._v2_override(location, exact_shape={'H200': 1})
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(reserved_capacity_broker,
                               'current_epoch',
                               return_value=3), \
             mock.patch.object(
                 kubernetes_adaptor,
                 'physical_cluster_uid_fence',
                 return_value=contextlib.nullcontext()), \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica',
                               return_value=True) as persist:
            self.assertTrue(self._launch_v2(manager, override))
        info = persist.call_args.args[2]
        self.assertEqual(info.reserved_fill_pool_key, override[_POOL_KEY])
        self.assertEqual(info.reserved_fill_service_generation, 7)
        self.assertEqual(info.reserved_fill_physical_cluster_uid,
                         'physical-uid')
        self.assertEqual(info.resources_override['accelerators'], {'H200': 1})
        self.assertEqual(persist.call_args.kwargs['expected_epoch'], 3)
        self.assertEqual(
            persist.call_args.kwargs['expected_service_generation'], 7)

    def test_v2_launch_capture_and_queued_static_pin_guard(self):
        location = make_location('phx-context',
                                 accelerators={'H200': 1.0},
                                 cloud_name='Kubernetes',
                                 use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [location]
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
        # Catalog shapes may represent a whole count as an integral float.
        # The immutable expected pin canonicalizes it while still comparing
        # against the actual queued override below.
        override = self._v2_override(location)
        events = []

        @contextlib.contextmanager
        def _physical_fence(context, physical_uid):
            self.assertEqual((context, physical_uid),
                             ('phx-context', 'physical-uid'))
            events.append('physical-enter')
            try:
                yield
            finally:
                events.append('physical-exit')

        def _persist(*_args, **_kwargs):
            self.assertEqual(events, ['physical-enter', 'thread'])
            events.append('persist')
            return True

        def _thread(*_args, **_kwargs):
            self.assertEqual(events, ['physical-enter'])
            events.append('thread')
            return mock.Mock()

        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(reserved_capacity_broker,
                               'current_epoch',
                               return_value=3), \
             mock.patch.object(
                 kubernetes_adaptor,
                 'physical_cluster_uid_fence',
                 side_effect=_physical_fence) as physical_fence, \
             mock.patch.object(reserved_capacity_broker,
                               'persist_fill_replica',
                               side_effect=_persist), \
             mock.patch.object(replica_managers,
                               '_ReplicaLaunchThread',
                               side_effect=_thread) as launch_thread, \
             mock.patch.object(
                 reserved_capacity,
                 'get_kubernetes_physical_cluster_uid') as ambient_uid:
            self.assertTrue(self._launch_v2(manager, override))

            thread_call = launch_thread.call_args.kwargs
            durable_fence = thread_call['kwargs']['launch_fence']
            self.assertIsNotNone(durable_fence)
            parsed_fence = reserved_capacity.parse_protocol_v2_launch_fence(
                durable_fence)
            self.assertIsNotNone(parsed_fence)
            self.assertEqual(parsed_fence.pool_key, override[_POOL_KEY])
            self.assertEqual(parsed_fence.service_generation, 7)
            self.assertEqual(parsed_fence.physical_cluster_uid, 'physical-uid')
            self.assertEqual(parsed_fence.kubernetes_context, 'phx-context')
            self.assertEqual(parsed_fence.accelerator, 'h200')
            self.assertEqual(parsed_fence.accelerator_count, 1)
            cloud_guard = thread_call['kwargs']['cloud_launch_guard']
            self.assertIsNotNone(cloud_guard)
            self.assertEqual(cloud_guard(), (True, 'authorized'))
            physical_fence.assert_called_once_with('phx-context',
                                                   'physical-uid')
            self.assertEqual(
                events,
                ['physical-enter', 'thread', 'persist', 'physical-exit'])
            ambient_uid.assert_not_called()

            # The guard reads the same override mapping launch_cluster will
            # build its Task from, while retaining an immutable expected pin.
            # A queued mutation fails provider-free. The executor later proves
            # the immutable durable tuple for every provider attempt.
            queued_override = thread_call['args'][6]
            queued_override['accelerators'] = {'H200': 2}
            self.assertEqual(cloud_guard(),
                             (False, 'fill-accelerator-shape-mismatch'))
            physical_fence.assert_called_once()

    def test_abort_creates_no_record_and_keeps_id(self):
        placer = mock.Mock()
        placer.select_next_zero_cost_location.return_value = None
        manager = _make_manager(placer)
        override = {_FILL_KEY: True}
        # use_spot=False: the sentinel alone must force the placer path
        # (zero-cost k8s entries are non-spot).
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            manager._scale_up_one_locked(override, set())
        add_mock.assert_not_called()
        self.assertEqual(manager._next_replica_id, 7)
        self.assertEqual(manager._launch_thread_pool, {})
        placer.select_next_location.assert_not_called()
        # The caller's dict is not mutated (pop happens on a copy).
        self.assertIn(_FILL_KEY, override)

    def test_success_pins_location_and_strips_sentinel(self):
        location = _make_location('research-ctx', 'free')
        placer = mock.Mock()
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            manager._scale_up_one_locked({_FILL_KEY: True}, set())
        add_mock.assert_called_once()
        info = add_mock.call_args[0][2]
        self.assertNotIn(_FILL_KEY, info.resources_override)
        # Launch pinned to the zero-cost location (non-spot k8s).
        self.assertIs(info.resources_override['use_spot'], False)
        self.assertEqual(info.resources_override['region'], 'research-ctx')
        self.assertIs(info.is_spot, False)
        self.assertEqual(manager._next_replica_id, 8)
        # The persisted row carries its creation time from the start
        # (PROVISIONING included): the fill overlay's post-snapshot debit
        # relies on it.
        self.assertIsNotNone(info.created_at)

    def test_targeted_fill_keeps_a100_variants_exact(self):
        a100 = make_location('research-ctx',
                             accelerators={'A100': 1},
                             use_spot=False)
        a100_80gb = make_location('research-ctx',
                                  accelerators={'A100-80GB': 1},
                                  use_spot=False)
        placer = mock.Mock()
        placer.active_locations.return_value = [a100, a100_80gb]
        placer.select_next_zero_cost_location.return_value = a100_80gb
        manager = _make_manager(placer)
        override = {
            _FILL_KEY: True,
            'accelerators': {
                'A100-80GB': 1
            },
        }

        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            manager._scale_up_one_locked(override, set())

        placer.select_next_zero_cost_location.assert_called_once_with(
            allowed_locations={a100_80gb})
        info = add_mock.call_args[0][2]
        self.assertEqual(info.resources_override['accelerators'],
                         {'A100-80GB': 1})

    def test_redriven_pinned_launch_keeps_location(self):
        # Controller crash mid-PENDING: the launch is re-driven with the
        # persisted override (location inlined, sentinel already
        # stripped, use_spot=False). The placer selection path is
        # skipped, but the upserted row must keep the pinned location --
        # location=None would permanently drop the replica from fill
        # accounting.
        location = spot_placer.Location.from_pickleable(_K8S_KEY)
        placer = mock.Mock()
        manager = _make_manager(placer)
        persisted_override = dict(location.to_dict())
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            launched = manager._launch_replica(7, persisted_override)
        self.assertTrue(launched)
        placer.select_next_location.assert_not_called()
        placer.select_next_zero_cost_location.assert_not_called()
        info = add_mock.call_args[0][2]
        recovered = info.get_spot_location()
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered.region, 'research-ctx')
        self.assertIs(info.is_spot, False)

    def test_sentinel_without_placer_aborts(self):
        manager = _make_manager(placer=None)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock:
            launched = manager._launch_replica(7, {_FILL_KEY: True})
        self.assertFalse(launched)
        add_mock.assert_not_called()


class TestDemandLaunchBudget(unittest.TestCase):
    """Demand launches obey measured free-GPU capacity."""

    def test_v2_grant_saturates_only_its_physical_pool(self):
        east = make_location('east-context',
                             accelerators={'A100': 1},
                             cloud_name='Kubernetes',
                             use_spot=False)
        phx = make_location('phx-context',
                            accelerators={'A100': 1},
                            cloud_name='Kubernetes',
                            use_spot=False)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [east, phx]
        manager = _make_manager(placer)
        east_replica = _replica(1, east.to_pickleable())
        grants = {
            'east-pool': reserved_capacity_broker.CachedPoolGrant(
                grant=1,
                access_context='east-context',
                accelerator_names=('a100',),
                physical_cluster_uid='east-uid',
                service_generation=1),
            'phx-pool': reserved_capacity_broker.CachedPoolGrant(
                grant=2,
                access_context='phx-context',
                accelerator_names=('a100',),
                physical_cluster_uid='phx-uid',
                service_generation=1),
        }
        with mock.patch.object(reserved_capacity_broker,
                               'get_cached_pool_grants',
                               return_value=grants):
            saturated = manager._demand_saturated_zero_cost_locations(
                [east_replica])
        self.assertEqual(saturated, {east})

    def test_exhausted_zero_cost_only_budget_persists_no_replica(self):
        location = _make_location('research-ctx', 'free')
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [location]
        placer.active_locations.return_value = [location]
        placer.ranked_active_locations.return_value = [location]
        placer.select_next_location.return_value = location
        manager = _make_manager(placer)
        budget = replica_managers._ZeroCostDemandBudget(
            {('research-ctx', 'a100'): 0}, {('research-ctx', 'a100'): 0})

        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=True), \
             mock.patch.object(reserved_capacity_broker,
                               'get_cached_grant',
                               return_value=None), \
             mock.patch.object(manager, '_persist_replica') as persist:
            launched = manager._scale_up_one_locked(
                None, set(), [], zero_cost_demand_budget=budget)

        self.assertFalse(launched)
        persist.assert_not_called()


class TestQueryFreeSlots(unittest.TestCase):
    """Poller free-slot math: slots are per-replica GPUs; unknown (-1)
    availability is a measurement blackout (the standalone sum reads it
    as 0 free for the cycle, but it is never a *successful* 0)."""

    def _k8s_location(self, region='research-ctx', gpu='A100', count=1):
        return spot_placer.Location.from_pickleable({
            'cloud': 'Kubernetes',
            'region': region,
            'zone': None,
            'accelerators': {
                gpu: count
            },
            'use_spot': False,
        })

    def setUp(self):
        with reserved_capacity._PHYSICAL_CLUSTER_UID_CACHE_LOCK:
            reserved_capacity._PHYSICAL_CLUSTER_UID_CACHE.clear()
            reserved_capacity._PHYSICAL_CLUSTER_UID_LOOKUP_GENERATIONS.clear()

    def test_global_pool_budget_retains_holdings_before_water_filling(self):
        budgets = reserved_capacity.allocate_fill_pool_budgets(
            3, 3, (reserved_capacity.FillPoolBudgetInput(
                2, 10), reserved_capacity.FillPoolBudgetInput(2, 10)))
        self.assertEqual(
            [(budget.edge_cap, budget.edge_floor) for budget in budgets],
            [(2, 2), (1, 1)])

    def test_global_pool_budget_water_fills_with_stable_remainder(self):
        budgets = reserved_capacity.allocate_fill_pool_budgets(
            7, 2, (reserved_capacity.FillPoolBudgetInput(
                0, 1), reserved_capacity.FillPoolBudgetInput(
                    0, 10), reserved_capacity.FillPoolBudgetInput(0, 10)))
        self.assertEqual([budget.edge_cap for budget in budgets], [1, 3, 3])
        self.assertEqual([budget.edge_floor for budget in budgets], [1, 1, 0])

    def test_global_pool_budget_does_not_invent_capacity(self):
        budgets = reserved_capacity.allocate_fill_pool_budgets(
            10, 10, (reserved_capacity.FillPoolBudgetInput(
                0, 2), reserved_capacity.FillPoolBudgetInput(0, 1)))
        self.assertEqual([budget.edge_cap for budget in budgets], [2, 1])
        self.assertEqual(sum(budget.edge_cap for budget in budgets), 3)

    def test_context_groups_keep_first_position_and_canonical_shapes(self):
        locations = [
            self._k8s_location(region='ctx-b', gpu='h200', count=8),
            self._k8s_location(region='ctx-a', gpu='H100', count=2),
            self._k8s_location(region='ctx-b', gpu='H200', count=8),
            self._k8s_location(region='ctx-a', gpu='A100', count=2),
        ]
        groups = reserved_capacity.group_zero_cost_fill_pools(locations)
        self.assertEqual([group.context for group in groups],
                         ['ctx-b', 'ctx-a'])
        self.assertEqual(groups[0].position, 0)
        self.assertEqual(groups[0].shapes, (('h200', 8),))
        self.assertEqual(groups[0].locations, (locations[0], locations[2]))
        self.assertEqual(groups[1].position, 1)
        self.assertEqual(groups[1].shapes, (('a100', 2), ('h100', 2)))
        self.assertEqual(groups[1].gpus_per_replica, 2)

    def test_context_groups_allow_different_physical_widths(self):
        groups = reserved_capacity.group_zero_cost_fill_pools([
            self._k8s_location(region='ctx-a', gpu='A100', count=1),
            self._k8s_location(region='ctx-b', gpu='H200', count=8),
        ])
        self.assertEqual([group.gpus_per_replica for group in groups], [1, 8])

    def test_context_group_rejects_mixed_widths(self):
        with self.assertRaisesRegex(ValueError,
                                    'one GPU count within each Kubernetes'):
            reserved_capacity.group_zero_cost_fill_pools([
                self._k8s_location(region='ctx-a', gpu='A100', count=1),
                self._k8s_location(region='ctx-a', gpu='H100', count=2),
            ])

    def test_physical_uid_is_cached_for_at_most_one_poll_interval(self):
        core_api = mock.Mock()
        core_api.read_namespace.side_effect = [
            types.SimpleNamespace(metadata=types.SimpleNamespace(
                uid='uid-first')),
            types.SimpleNamespace(metadata=types.SimpleNamespace(
                uid='uid-second')),
        ]
        with mock.patch.object(reserved_capacity.kubernetes,
                               'core_api',
                               return_value=core_api), \
             mock.patch.object(reserved_capacity,
                               'poll_interval_seconds',
                               return_value=10), \
             mock.patch.object(reserved_capacity.time,
                               'monotonic',
                               side_effect=[100, 101, 105, 111, 112]):
            first = reserved_capacity.get_kubernetes_physical_cluster_uid('ctx')
            cached = reserved_capacity.get_kubernetes_physical_cluster_uid(
                'ctx')
            refreshed = reserved_capacity.get_kubernetes_physical_cluster_uid(
                'ctx')
        self.assertEqual((first, cached, refreshed),
                         ('uid-first', 'uid-first', 'uid-second'))
        self.assertEqual(core_api.read_namespace.call_count, 2)
        core_api.read_namespace.assert_called_with(
            'kube-system',
            _request_timeout=reserved_capacity.kubernetes.API_TIMEOUT)

    def test_force_refresh_bypasses_physical_uid_cache(self):
        core_api = mock.Mock()
        core_api.read_namespace.side_effect = [
            types.SimpleNamespace(metadata=types.SimpleNamespace(uid='uid-a')),
            types.SimpleNamespace(metadata=types.SimpleNamespace(uid='uid-b')),
        ]
        with mock.patch.object(reserved_capacity.kubernetes,
                               'core_api',
                               return_value=core_api), \
             mock.patch.object(reserved_capacity,
                               'poll_interval_seconds',
                               return_value=10), \
             mock.patch.object(reserved_capacity.time,
                               'monotonic',
                               side_effect=[100, 101, 102, 103]):
            self.assertEqual(
                reserved_capacity.get_kubernetes_physical_cluster_uid('ctx'),
                'uid-a')
            self.assertEqual(
                reserved_capacity.get_kubernetes_physical_cluster_uid(
                    'ctx', force_refresh=True), 'uid-b')
        self.assertEqual(core_api.read_namespace.call_count, 2)

    def test_physical_uid_busy_waits_once_deadline_then_rereads_ambient(self):
        busy = exceptions.KubernetesPhysicalClusterFenceBusyError(
            'captured', 'ctx', 7)
        core_api = mock.Mock()
        core_api.read_namespace.side_effect = [
            busy,
            types.SimpleNamespace(metadata=types.SimpleNamespace(
                uid='uid-new')),
        ]
        with mock.patch.object(reserved_capacity.kubernetes,
                               'core_api',
                               return_value=core_api), \
             mock.patch.object(
                 reserved_capacity.kubernetes,
                 'wait_for_physical_cluster_uid_fence_retirement',
                 return_value=True) as wait_for_retirement, \
             mock.patch.object(reserved_capacity,
                               'poll_interval_seconds',
                               return_value=10), \
             mock.patch.object(reserved_capacity.time,
                               'monotonic',
                               side_effect=[100, 101, 102]):
            uid = reserved_capacity.get_kubernetes_physical_cluster_uid('ctx')

        self.assertEqual(uid, 'uid-new')
        self.assertEqual(core_api.read_namespace.call_count, 2)
        wait_for_retirement.assert_called_once_with('ctx', 131, 7)

    def test_physical_uid_busy_timeout_fails_closed_without_reread(self):
        busy = exceptions.KubernetesPhysicalClusterFenceBusyError(
            'captured', 'ctx', 11)
        core_api = mock.Mock()
        core_api.read_namespace.side_effect = busy
        with mock.patch.object(reserved_capacity.kubernetes,
                               'core_api',
                               return_value=core_api), \
             mock.patch.object(
                 reserved_capacity.kubernetes,
                 'wait_for_physical_cluster_uid_fence_retirement',
                 return_value=False) as wait_for_retirement, \
             mock.patch.object(reserved_capacity.time,
                               'monotonic',
                               side_effect=[100, 101]):
            uid = reserved_capacity.get_kubernetes_physical_cluster_uid('ctx')

        self.assertIsNone(uid)
        self.assertEqual(core_api.read_namespace.call_count, 1)
        wait_for_retirement.assert_called_once_with('ctx', 131, 11)

    def test_stale_forced_uid_lookup_returns_newer_cached_identity(self):
        first_started = threading.Event()
        release_first = threading.Event()
        call_lock = threading.Lock()
        call_count = 0

        def read_namespace(*_args, **_kwargs):
            nonlocal call_count
            with call_lock:
                call_count += 1
                ordinal = call_count
            if ordinal == 1:
                first_started.set()
                self.assertTrue(release_first.wait(timeout=5))
                uid = 'uid-before-retarget'
            else:
                uid = 'uid-after-retarget'
            return types.SimpleNamespace(metadata=types.SimpleNamespace(
                uid=uid))

        core_api = mock.Mock()
        core_api.read_namespace.side_effect = read_namespace
        stale_result = []

        def stale_forced_lookup():
            stale_result.append(
                reserved_capacity.get_kubernetes_physical_cluster_uid(
                    'ctx', force_refresh=True))

        with mock.patch.object(reserved_capacity.kubernetes,
                               'core_api',
                               return_value=core_api), \
             mock.patch.object(reserved_capacity,
                               'poll_interval_seconds',
                               return_value=10):
            stale_thread = threading.Thread(target=stale_forced_lookup)
            stale_thread.start()
            self.assertTrue(first_started.wait(timeout=5))
            current = reserved_capacity.get_kubernetes_physical_cluster_uid(
                'ctx', force_refresh=True)
            release_first.set()
            stale_thread.join(timeout=5)

        self.assertFalse(stale_thread.is_alive())
        self.assertEqual(current, 'uid-after-retarget')
        # The older network response must not let a launch compare against
        # uid-before-retarget after a newer forced observation completed.
        self.assertEqual(stale_result, ['uid-after-retarget'])

    def test_expired_physical_uid_is_not_used_after_lookup_failure(self):
        core_api = mock.Mock()
        core_api.read_namespace.side_effect = [
            types.SimpleNamespace(metadata=types.SimpleNamespace(uid='uid-a')),
            RuntimeError('unreachable'),
        ]
        with mock.patch.object(reserved_capacity.kubernetes,
                               'core_api',
                               return_value=core_api), \
             mock.patch.object(reserved_capacity,
                               'poll_interval_seconds',
                               return_value=10), \
             mock.patch.object(reserved_capacity.time,
                               'monotonic',
                               side_effect=[100, 101, 111]):
            self.assertEqual(
                reserved_capacity.get_kubernetes_physical_cluster_uid('ctx'),
                'uid-a')
            self.assertIsNone(
                reserved_capacity.get_kubernetes_physical_cluster_uid('ctx'))

    def test_pool_discovery_deduplicates_context_aliases_by_physical_uid(self):
        locations = [
            self._k8s_location(region='ctx-first', gpu='H200'),
            self._k8s_location(region='ctx-alias', gpu='h200'),
        ]
        with mock.patch.object(reserved_capacity,
                               'get_kubernetes_physical_cluster_uid',
                               side_effect=['physical-uid', 'physical-uid']):
            pools = reserved_capacity.discover_fill_pool_specs(locations)
        self.assertEqual(len(pools), 1)
        self.assertEqual(pools[0].context, 'ctx-first')
        self.assertEqual(
            pools[0].pool_key,
            reserved_capacity_broker.make_pool_key(
                'ctx-first',
                'h200',
                protocol_version=reserved_capacity_broker.PROTOCOL_V2,
                physical_cluster_uid='physical-uid'))

    def test_pool_discovery_isolates_failed_identity_lookup(self):
        locations = [
            self._k8s_location(region='ctx-failed', gpu='A100'),
            self._k8s_location(region='ctx-healthy', gpu='H200'),
        ]
        with mock.patch.object(reserved_capacity,
                               'get_kubernetes_physical_cluster_uid',
                               side_effect=[None, 'healthy-uid']):
            pools = reserved_capacity.discover_fill_pool_specs(locations)
        self.assertEqual([pool.context for pool in pools], ['ctx-healthy'])

    def test_free_gpus_divided_by_replica_size(self):
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 5
                               })):
            self.assertEqual(
                reserved_capacity.query_free_slots(
                    [self._k8s_location(count=2)]), 2)

    def test_pool_shapes_preserve_exact_whole_gpu_counts(self):
        shapes = reserved_capacity.zero_cost_pool_shapes([
            self._k8s_location(gpu='A100', count=2.0),
            self._k8s_location(gpu='H100', count=1),
        ])
        self.assertEqual(shapes, {
            ('research-ctx', 'a100'): 2,
            ('research-ctx', 'h100'): 1,
        })

    def test_pool_shapes_reject_fractional_and_non_finite_counts(self):
        for count in (0.5, 1.5, float('nan'), float('inf')):
            with self.subTest(count=count):
                self.assertEqual(
                    reserved_capacity.zero_cost_pool_shapes(
                        [self._k8s_location(count=count)]), {})

    def test_unknown_availability_counts_zero(self):
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': -1
                               })):
            self.assertEqual(
                reserved_capacity.query_free_slots([self._k8s_location()]), 0)

    def test_unknown_availability_is_measurement_blackout(self):
        # A swallowed cluster-wide failure (e.g. pod-list 403) surfaces
        # as {'A100': -1}: that is NOT an authoritative 0-free
        # measurement. It must read as a blackout (free_slots=None) so
        # the broker's blind-round semantics engage -- grants floored at
        # holdings, feed 0 -- instead of letting a new claimant or
        # weight change redistribute grants while availability is
        # unknown. gpu_names still carries the seen names: an unknown
        # reading is not a phantom pool either.
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': -1
                               })):
            observation = reserved_capacity.query_pool_observation(
                'research-ctx', 'A100', 1)
        self.assertIsNone(observation.free_slots)
        self.assertEqual(observation.gpu_names, ('A100',))

    def test_any_negative_availability_blacks_out_the_whole_pool(self):
        # Partial unknowns poison the sum the same way: one -1 among
        # positive counts means the pool's free level is not measurable.
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 3,
                                   'A100-80GB': -1
                               })):
            observation = reserved_capacity.query_pool_observation(
                'research-ctx', 'A100', 1)
        self.assertIsNone(observation.free_slots)
        self.assertEqual(set(observation.gpu_names), {'A100', 'A100-80GB'})

    def test_group_observation_sums_requested_accelerators_once(self):
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 3,
                                   'A100-80GB': 4,
                                   'H100': 9,
                               })) as query:
            observation = reserved_capacity.query_pool_group_observation(
                'research-ctx', {
                    'a100': 1,
                    'a100-80gb': 2,
                })
        self.assertEqual(observation.free_slots, 5)
        self.assertEqual(set(observation.gpu_names), {'A100', 'A100-80GB'})
        self.assertEqual(observation.free_slots_by_accelerator,
                         (('a100', 3), ('a100-80gb', 2)))
        query.assert_called_once()

    def test_protocol_v2_group_observation_runs_inside_physical_uid_fence(self):
        entered = False

        @contextlib.contextmanager
        def _uid_fence(context, expected_uid):
            nonlocal entered
            self.assertEqual(context, 'research-ctx')
            self.assertEqual(expected_uid, 'physical-uid')
            entered = True
            try:
                yield
            finally:
                entered = False

        def _query(**kwargs):
            self.assertTrue(
                entered, 'realtime availability escaped its physical UID fence')
            self.assertEqual(kwargs['region_filter'], 'research-ctx')
            return ({}, {}, {'A100': 3})

        with mock.patch.object(reserved_capacity.kubernetes,
                               'physical_cluster_uid_fence',
                               side_effect=_uid_fence) as uid_fence, \
             mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               side_effect=_query) as query:
            observation = reserved_capacity.query_pool_group_observation(
                'research-ctx', {'a100': 1},
                expected_physical_cluster_uid='physical-uid')

        self.assertEqual(observation.free_slots, 3)
        uid_fence.assert_called_once_with('research-ctx', 'physical-uid')
        query.assert_called_once()

    def test_protocol_v2_group_identity_mismatch_is_blackout(self):

        @contextlib.contextmanager
        def _uid_mismatch(*_args, **_kwargs):
            raise exceptions.KubernetesPhysicalClusterIdentityError(
                'context was retargeted')
            yield  # pragma: no cover  # pylint: disable=unreachable

        with mock.patch.object(reserved_capacity.kubernetes,
                               'physical_cluster_uid_fence',
                               side_effect=_uid_mismatch), \
             mock.patch.object(
                 reserved_capacity.kubernetes_catalog,
                 'list_accelerators_realtime') as query:
            observation = reserved_capacity.query_pool_group_observation(
                'research-ctx', {'a100': 1},
                expected_physical_cluster_uid='physical-uid')

        self.assertIsNone(observation.free_slots)
        query.assert_not_called()

    def test_same_shape_different_counts_use_largest_deterministically(self):
        # A100:1 and A100:8 entries over one pool draw from the same
        # free GPUs: the LARGEST per-replica size wins regardless of
        # any_of entry order (first-seen-wins would let YAML order
        # change the fill level).
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 8
                               })):
            for locations in ([
                    self._k8s_location(count=1),
                    self._k8s_location(count=8)
            ], [self._k8s_location(count=8),
                    self._k8s_location(count=1)]):
                self.assertEqual(reserved_capacity.query_free_slots(locations),
                                 1)

    def test_same_shape_counted_once_and_non_k8s_skipped(self):
        aws = spot_placer.Location.from_pickleable({
            'cloud': 'AWS',
            'region': 'us-east-1',
            'zone': None,
            'accelerators': {
                'L4': 1
            },
            'use_spot': True,
        })
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 3
                               })) as query:
            total = reserved_capacity.query_free_slots(
                [self._k8s_location(),
                 self._k8s_location(), aws])
        self.assertEqual(total, 3)
        query.assert_called_once()

    def test_demand_snapshot_queries_two_shapes_in_context_once(self):
        locations = [
            self._k8s_location(gpu='A100'),
            self._k8s_location(gpu='A100-80GB'),
        ]
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 220,
                                   'A100-80GB': 3,
                                   'L4': 99,
                               })) as query:
            free = reserved_capacity.query_free_slots_by_context(locations)

        self.assertEqual(free, {'research-ctx': 223})
        query.assert_called_once_with(gpus_only=True,
                                      name_filter=None,
                                      region_filter='research-ctx',
                                      quantity_filter=None,
                                      case_sensitive=False,
                                      require_price=False)

    def test_demand_snapshot_pools_per_shape_and_dedupes_to_largest(self):
        # Heterogeneous shapes in one context pool per shape: each shape
        # contributes available // per_replica and the context budget is
        # the sum. Duplicate (context, gpu) entries dedupe to the LARGEST
        # per-replica count (conservative), so A100 x2 and A100 x8 read
        # as a single 8-GPU shape: 220 // 8 + 3 // 1 = 30.
        locations = [
            self._k8s_location(gpu='A100', count=2),
            self._k8s_location(gpu='A100', count=8),
            self._k8s_location(gpu='A100-80GB', count=1),
        ]
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': 220,
                                   'A100-80GB': 3,
                               })) as query:
            free = reserved_capacity.query_free_slots_by_context(locations)
        self.assertEqual(free, {'research-ctx': 30})
        query.assert_called_once()

    def test_demand_snapshot_distinguishes_unknown_from_zero(self):
        locations = [self._k8s_location(gpu='A100')]
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {
                                   'A100': -1
                               })):
            self.assertEqual(
                reserved_capacity.query_free_slots_by_context(locations),
                {'research-ctx': None})
        with mock.patch.object(reserved_capacity.kubernetes_catalog,
                               'list_accelerators_realtime',
                               return_value=({}, {}, {})):
            self.assertEqual(
                reserved_capacity.query_free_slots_by_context(locations),
                {'research-ctx': 0})

    def test_shared_demand_cache_returns_raw_gpus_per_accelerator(self):
        locations = [
            self._k8s_location(gpu='A100', count=8),
            self._k8s_location(gpu='H100', count=8),
        ]
        row = {
            'context': 'research-ctx',
            'snapshot_time': 100.0,
            # A slow query completed much later. Freshness must use completion
            # while planner debits retain the query-start snapshot.
            'completed_at': 200.0,
            'availability': '{"a100": 223, "h100": 16}',
        }
        with mock.patch.object(reserved_capacity.time,
                               'time',
                               return_value=201.0), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_demand_capacity_observations',
                               return_value={'research-ctx': row}), \
             mock.patch.object(reserved_capacity,
                               '_schedule_demand_capacity_refresh') as schedule:
            observations = reserved_capacity.get_cached_free_gpus_by_pool(
                locations)

        self.assertEqual(observations[('research-ctx', 'a100')].free_gpus, 223)
        self.assertEqual(observations[('research-ctx', 'h100')].free_gpus, 16)
        self.assertEqual(observations[('research-ctx', 'a100')].snapshot_time,
                         100.0)
        schedule.assert_called_once_with(set())

    def test_demand_cache_freshness_honors_configured_poll_interval(self):
        location = self._k8s_location(gpu='A100', count=8)
        row = {
            'context': 'research-ctx',
            'snapshot_time': 90.0,
            'completed_at': 100.0,
            'availability': '{"a100": 8}',
        }
        env_var = constants.RESERVED_CAPACITY_POLL_INTERVAL_ENV_VAR
        with mock.patch.dict(reserved_capacity.os.environ, {env_var: '300'}), \
             mock.patch.object(reserved_capacity.time,
                               'time',
                               return_value=220.0), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_demand_capacity_observations',
                               return_value={'research-ctx': row}), \
             mock.patch.object(reserved_capacity,
                               '_schedule_demand_capacity_refresh') as schedule:
            observations = reserved_capacity.get_cached_free_gpus_by_pool(
                [location])

        self.assertEqual(observations[('research-ctx', 'a100')].free_gpus, 8)
        schedule.assert_called_once_with(set())

    def test_stale_shared_demand_cache_schedules_without_querying_inline(self):
        location = self._k8s_location(gpu='A100', count=8)
        with mock.patch.object(reserved_capacity.serve_state,
                               'get_demand_capacity_observations',
                               return_value={}), \
             mock.patch.object(reserved_capacity,
                               '_schedule_demand_capacity_refresh') as schedule, \
             mock.patch.object(
                 reserved_capacity.kubernetes_catalog,
                 'list_accelerators_realtime') as query:
            observations = reserved_capacity.get_cached_free_gpus_by_pool(
                [location])

        self.assertIsNone(observations[('research-ctx', 'a100')].free_gpus)
        schedule.assert_called_once_with({'research-ctx'})
        query.assert_not_called()

    def test_background_refresh_publishes_one_raw_context_observation(self):
        lock = mock.MagicMock()
        lock.acquire.return_value = mock.MagicMock()
        with mock.patch.object(reserved_capacity.locks,
                               'get_lock',
                               return_value=lock), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_demand_capacity_observations',
                               return_value={}), \
             mock.patch.object(
                 reserved_capacity.kubernetes_catalog,
                 'list_accelerators_realtime',
                 return_value=({}, {}, {
                     'A100': 223,
                     'H100': 16,
                 })) as query, \
             mock.patch.object(
                 reserved_capacity.serve_state,
                 'upsert_demand_capacity_observation') as publish:
            reserved_capacity._refresh_demand_capacity_contexts(
                {'research-ctx'})

        query.assert_called_once_with(gpus_only=True,
                                      name_filter=None,
                                      region_filter='research-ctx',
                                      quantity_filter=None,
                                      case_sensitive=False,
                                      require_price=False)
        context, snapshot_time, completed_at, availability = publish.call_args.args
        self.assertEqual(context, 'research-ctx')
        self.assertIsInstance(snapshot_time, float)
        self.assertIsInstance(completed_at, float)
        self.assertGreaterEqual(completed_at, snapshot_time)
        self.assertEqual(availability, {'a100': 223, 'h100': 16})

    def test_background_refresh_caches_query_failure(self):
        lock = mock.MagicMock()
        lock.acquire.return_value = mock.MagicMock()
        with mock.patch.object(reserved_capacity.locks,
                               'get_lock',
                               return_value=lock), \
             mock.patch.object(reserved_capacity.serve_state,
                               'get_demand_capacity_observations',
                               return_value={}), \
             mock.patch.object(
                 reserved_capacity.kubernetes_catalog,
                 'list_accelerators_realtime',
                 side_effect=RuntimeError('API unavailable')), \
             mock.patch.object(
                 reserved_capacity.serve_state,
                 'upsert_demand_capacity_observation') as publish:
            reserved_capacity._refresh_demand_capacity_contexts(
                {'research-ctx'})

        context, snapshot_time, completed_at, availability = publish.call_args.args
        self.assertEqual(context, 'research-ctx')
        self.assertIsInstance(snapshot_time, float)
        self.assertIsInstance(completed_at, float)
        self.assertGreaterEqual(completed_at, snapshot_time)
        self.assertIsNone(availability)


class TestDemandCapacityRefreshScheduling(unittest.TestCase):
    """Worker launch ownership follows the actual worker lifecycle."""

    def setUp(self):
        self._old_running = reserved_capacity._DEMAND_REFRESH_RUNNING
        self._old_pending = set(
            reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS)
        reserved_capacity._DEMAND_REFRESH_RUNNING = False
        reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS.clear()

    def tearDown(self):
        reserved_capacity._DEMAND_REFRESH_RUNNING = self._old_running
        reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS.clear()
        reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS.update(
            self._old_pending)

    def test_launch_failure_releases_ownership_and_preserves_pending(self):
        worker = mock.Mock()

        def _fail_start():
            # Model another reconciliation coalescing work after launch
            # ownership was reserved but before Thread.start() failed.
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-b'})
            raise RuntimeError("can't start new thread")

        worker.start.side_effect = _fail_start
        with mock.patch.object(reserved_capacity.threading,
                               'Thread',
                               return_value=worker), \
             mock.patch.object(reserved_capacity.logger, 'error') as error:
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-a'})

        self.assertFalse(reserved_capacity._DEMAND_REFRESH_RUNNING)
        self.assertEqual(reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS,
                         {'ctx-a', 'ctx-b'})
        error.assert_called_once()

    def test_next_schedule_retries_all_pending_contexts(self):
        worker = mock.Mock()
        starts = 0

        def _start():
            nonlocal starts
            starts += 1
            if starts == 1:
                raise RuntimeError("can't start new thread")

        worker.start.side_effect = _start
        with mock.patch.object(reserved_capacity.threading,
                               'Thread',
                               return_value=worker):
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-a'})
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-b'})

        self.assertEqual(starts, 2)
        self.assertTrue(reserved_capacity._DEMAND_REFRESH_RUNNING)
        self.assertEqual(reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS,
                         {'ctx-a', 'ctx-b'})

    def test_successful_launch_remains_single_flight(self):
        worker = mock.Mock()
        with mock.patch.object(reserved_capacity.threading,
                               'Thread',
                               return_value=worker) as thread:
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-a'})
            reserved_capacity._schedule_demand_capacity_refresh({'ctx-b'})

        thread.assert_called_once_with(
            target=reserved_capacity._demand_capacity_refresh_worker,
            name='serve-demand-capacity-refresh',
            daemon=True)
        worker.start.assert_called_once_with()
        self.assertTrue(reserved_capacity._DEMAND_REFRESH_RUNNING)
        self.assertEqual(reserved_capacity._DEMAND_REFRESH_PENDING_CONTEXTS,
                         {'ctx-a', 'ctx-b'})


class TestCostFeasibilityDegradation(unittest.TestCase):
    """Unavailable prices are represented completely in the catalog."""

    def test_unavailable_price_is_materialized_as_infinity(self):
        location = _make_location('paid-region', 'missing-price')
        materialized = mock.Mock()
        materialized.get_cost.side_effect = ValueError(
            "No SpotPrice found for instance type 'gr6.8xlarge'.")
        resources = mock.Mock()
        resources.copy.return_value = materialized
        task = types.SimpleNamespace(resources=[resources], num_nodes=1)

        with mock.patch.object(spot_placer,
                               '_get_possible_location_from_task',
                               return_value=[location]):
            catalog = spot_placer.PlacementCatalog.from_task(task)

        self.assertEqual(catalog.costs(), {location: float('inf')})
        materialized.get_cost.assert_called_once_with(seconds=3600)

    def test_kubernetes_zero_cost_seed_avoids_live_reclassification(self):
        location = spot_placer.Location.from_pickleable(_K8S_KEY)
        task = types.SimpleNamespace(resources=[mock.Mock()], num_nodes=1)
        with mock.patch.object(spot_placer,
                               '_get_possible_location_from_task',
                               return_value=[location]):
            placer = spot_placer.SpotPlacer(task)
        placer.resources.copy = mock.Mock(
            side_effect=AssertionError('must not query cluster feasibility'))

        for _ in range(100):
            self.assertEqual(placer.zero_cost_locations(), [location])

        placer.resources.copy.assert_not_called()

    def test_missing_spot_price_does_not_block_zero_cost_enumeration(self):
        placer = spot_placer.DynamicFallbackSpotPlacer.__new__(
            spot_placer.DynamicFallbackSpotPlacer)
        free = _make_location('research-ctx', 'free')
        missing_price = _make_location('paid-region', 'missing-price')
        placer.location2status = {
            free: spot_placer.LocationStatus.ACTIVE,
            missing_price: spot_placer.LocationStatus.ACTIVE,
        }
        placer.location2cost = {free: 0.0, missing_price: float('inf')}

        self.assertEqual(placer.zero_cost_locations(), [free])
        self.assertEqual(placer.location2cost[missing_price], float('inf'))

    def test_missing_price_candidate_does_not_hide_priced_candidate(self):
        missing = _make_location('paid-region', 'missing-price')
        priced = _make_location('other-paid-region', 'priced')
        placer = _make_placer({
            missing: float('inf'),
            priced: 0.42,
        })

        self.assertEqual(placer.cost_per_hour(missing), float('inf'))
        self.assertEqual(placer.cost_per_hour(priced), 0.42)


def _feed_broker(autoscaler,
                 free_slots,
                 grant,
                 epoch=None,
                 pool_key=None,
                 polls=2):
    """Feed a broker-shaped snapshot (feed + grant + epoch + pool key)."""
    ts = time.time()
    for _ in range(polls):
        autoscaler.collect_reserved_capacity(free_slots, [_K8S_KEY],
                                             ts,
                                             grant=grant,
                                             grant_epoch=epoch,
                                             grant_pool_key=pool_key)


class TestGrantCeiling(unittest.TestCase):
    """Broker grant ceiling: caps fill, strips over-grant shelter."""

    def test_grant_none_is_identity(self):
        with_none = _make_autoscaler(min_replicas=1)
        _feed_broker(with_none, 5, grant=None)
        control = _make_autoscaler(min_replicas=1)
        _feed(control, 5)
        got = _decisions(with_none, [])
        expected = _decisions(control, [])
        self.assertEqual([(d.operator, d.target) for d in got],
                         [(d.operator, d.target) for d in expected])

    def test_ceiling_caps_fill_launches(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        _feed_broker(autoscaler, 5, grant=2)
        decisions = _decisions(autoscaler, [])
        sentinel = [d for d in _ups(decisions) if d.target is not None]
        # Without the ceiling the feed of 5 would fund 5 fill ups. The grant
        # of 2 allows exactly two fill replicas, independent of demand.
        self.assertEqual(len(sentinel), 2)

    def test_snap_back_reclaim_shrinks_borrower(self):
        # The load-bearing broker actuator: the #108 fill target is
        # structurally >= holdings, so only the ceiling can strip the
        # borrower's surplus of its scale-down shelter and let the normal
        # graceful scale-down return the machines.
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [_replica(i, _K8S_KEY) for i in range(1, 5)]
        _feed_broker(autoscaler, 0, grant=2)
        decisions = _decisions(autoscaler, replicas)
        # Demand target 1, holdings 4, ceiling 2: fill shelters only ONE
        # of the three demand scale-downs; two pass through and the
        # borrower actually shrinks toward its grant.
        self.assertEqual(len(_downs(decisions)), 2)
        self.assertEqual(len(_ups(decisions)), 0)
        self.assertEqual(autoscaler.info()['fill_target'], 2)

    def test_demand_placed_rows_exempt_from_ceiling(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        fill_rows = [_replica(1, _K8S_KEY), _replica(2, _K8S_KEY)]
        demand_rows = [_replica(3, _K8S_KEY), _replica(4, _K8S_KEY)]
        for row in demand_rows:
            row.reserved_fill = False
        _feed_broker(autoscaler, 0, grant=0)
        decisions = _decisions(autoscaler, fill_rows + demand_rows)
        # Ceiling = grant 0 + 2 demand-placed rows: the demand rows are
        # demand-protected (not broker property), only the two FILL rows
        # are over-ceiling and lose their shelter.
        self.assertEqual(autoscaler.info()['fill_target'], 2)
        self.assertEqual(len(_downs(decisions)), 2)

    def test_old_demand_rows_do_not_inflate_launch_ceiling(self):
        # Rolling update: 3 OLD-version demand-placed zero-cost rows are
        # still draining. The launch-side ceiling must count only
        # LATEST-version demand-placed rows (here 0), so fill launches
        # stay capped at the grant: ceiling 2 produces exactly 2 fill ups,
        # independent of demand. An all-version launch ceiling (2 + 3 old
        # rows) would fund 5 fill ups, overshooting the grant by the old rows'
        # count.
        autoscaler = autoscalers.RequestRateAutoscaler('svc',
                                                       _spec(min_replicas=1,
                                                             max_replicas=20),
                                                       version=2)
        _feed_broker(autoscaler, 5, grant=2)
        old_demand = [_replica(i, _K8S_KEY, version=1) for i in range(1, 4)]
        for row in old_demand:
            row.reserved_fill = False
        decisions = autoscaler.generate_scaling_decisions(old_demand, [1])
        self.assertEqual(len(_fill_ups(decisions)), 2)
        # The target/shelter-side ceiling keeps the all-version count
        # (grant 2 + 3 demand-placed rows): existing rows keep their
        # exemption regardless of version.
        self.assertEqual(autoscaler.info()['fill_target'], 5)

    def test_stale_broker_snapshot_still_shelters_holdings(self):
        # Ceiling + staleness compose: a dead poller decays the feed to 0
        # but holdings at-or-under the ceiling keep their shelter.
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [_replica(1, _K8S_KEY), _replica(2, _K8S_KEY)]
        ts = _stale_timestamp()
        autoscaler.collect_reserved_capacity(5, [_K8S_KEY],
                                             ts,
                                             grant=2,
                                             grant_epoch=1)
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(decisions, [])


class TestGrantEpochPlumbing(unittest.TestCase):
    """Fill scale-ups carry the grant epoch only when a broker supplied it."""

    def test_epoch_and_pool_key_attached_to_sentinel_override(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        pool = reserved_capacity_broker.make_pool_key('research-ctx', 'a100')
        _feed_broker(autoscaler, 3, grant=5, epoch=7, pool_key=pool)
        sentinel = [
            d for d in _ups(_decisions(autoscaler, [])) if d.target is not None
        ]
        self.assertTrue(sentinel)
        for decision in sentinel:
            self.assertEqual(
                decision.target, {
                    _FILL_KEY: True,
                    _EPOCH_KEY: 7,
                    _POOL_KEY: pool,
                    _PROTOCOL_KEY: reserved_capacity_broker.PROTOCOL_V1,
                    _GENERATION_KEY: 0,
                })

    def test_no_epoch_means_pre_broker_decision_shape(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        _feed(autoscaler, 3)
        sentinel = [
            d for d in _ups(_decisions(autoscaler, [])) if d.target is not None
        ]
        self.assertTrue(sentinel)
        for decision in sentinel:
            self.assertEqual(decision.target, {_FILL_KEY: True})

    def test_grant_not_persisted_in_dynamic_state_dump(self):
        # Grants are DB-authoritative: a swapped-in autoscaler must get
        # them from the next poll, never from a stale dump.
        autoscaler = _make_autoscaler(min_replicas=1)
        _feed_broker(autoscaler, 3, grant=5, epoch=7, pool_key='pool')
        dump = autoscaler.dump_dynamic_states()
        fill_state = dump['reserved_capacity_fill_state']
        self.assertNotIn('fill_grant', fill_state)
        self.assertNotIn('fill_grant_epoch', fill_state)
        self.assertNotIn('fill_grant_pool_key', fill_state)
        restored = _make_autoscaler(min_replicas=1)
        restored.load_dynamic_states(dump)
        self.assertEqual(restored.info()['fill_free_slots'], 0)
        # Location identity still shelters live pool rows during the swap.
        self.assertEqual(restored._fill_zero_cost_locations,
                         [spot_placer.Location.from_pickleable(_K8S_KEY)])


class TestEpochFencedLaunch(unittest.TestCase):
    """A fill launch carrying a superseded epoch skips, leaking nothing.

    Broker-stamped launches persist through the broker's
    persist_fill_replica (the epoch recheck atomic with the row upsert
    and mutually excluded with in-flight rounds -- the pre-check alone is
    TOCTOU); un-stamped launches keep the plain persist.
    """

    def _launch(self,
                pool_epochs,
                carried_epoch=7,
                carried_pool='pool-b',
                persist_epoch_current=True):
        """pool_epochs: pool_key -> current round epoch (the fence read).

        persist_epoch_current: what the atomic persist-time recheck
        reports (False = a new round published between the pre-check and
        the persist).
        """
        location = _make_location('research-ctx', 'free')
        placer = mock.Mock()
        placer.select_next_zero_cost_location.return_value = location
        manager = _make_manager(placer)
        override = {_FILL_KEY: True, _EPOCH_KEY: carried_epoch}
        if carried_pool is not None:
            override[_POOL_KEY] = carried_pool
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=False), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.reserved_capacity_broker,
                               'current_epoch',
                               side_effect=pool_epochs.get) as epoch_mock, \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica') as add_mock, \
             mock.patch.object(replica_managers.reserved_capacity_broker,
                               'persist_fill_replica',
                               return_value=persist_epoch_current
                              ) as fenced_add_mock:
            launched = manager._launch_replica(7, override)
        return launched, add_mock, epoch_mock, fenced_add_mock

    def test_stale_epoch_skips_without_persisting_a_row(self):
        launched, add_mock, epoch_mock, fenced_add = self._launch({'pool-b': 8})
        self.assertFalse(launched)
        add_mock.assert_not_called()
        fenced_add.assert_not_called()
        # The fence reads the CARRIED pool's round epoch, not a global one.
        epoch_mock.assert_called_once_with('pool-b')

    def test_current_epoch_launches_and_strips_the_keys(self):
        launched, add_mock, _, fenced_add = self._launch({'pool-b': 7})
        self.assertTrue(launched)
        # Broker-stamped: persisted through the atomic epoch-rechecking
        # path, carrying the pool key and the carried epoch.
        add_mock.assert_not_called()
        fenced_add.assert_called_once()
        self.assertEqual(fenced_add.call_args.kwargs['pool_key'], 'pool-b')
        self.assertEqual(fenced_add.call_args.kwargs['expected_epoch'], 7)
        info = fenced_add.call_args[0][2]
        self.assertNotIn(_EPOCH_KEY, info.resources_override)
        self.assertNotIn(_POOL_KEY, info.resources_override)
        self.assertNotIn(_FILL_KEY, info.resources_override)
        self.assertTrue(info.reserved_fill)

    def test_peer_pool_epoch_bump_does_not_fence(self):
        # Cross-pool isolation at the fence: pool A's epoch moved (8) but
        # this launch carries pool B's still-current epoch (7) -- it must
        # launch. Pool B's own stale epoch (6 vs 7) still fences.
        launched, _, _, fenced_add = self._launch({'pool-a': 8, 'pool-b': 7})
        self.assertTrue(launched)
        fenced_add.assert_called_once()
        fenced, _, _, fenced_persist = self._launch({
            'pool-a': 8,
            'pool-b': 7
        },
                                                    carried_epoch=6)
        self.assertFalse(fenced)
        fenced_persist.assert_not_called()

    def test_missing_round_fails_open(self):
        # No round row for the pool (current_epoch None): there is no
        # newer allocation to defer to -- proceed rather than deadlock
        # fill forever (add_replica_if_round_epoch fails open the same
        # way at persist time).
        launched, add_mock, _, fenced_add = self._launch({})
        self.assertTrue(launched)
        add_mock.assert_not_called()
        fenced_add.assert_called_once()

    def test_epoch_without_pool_key_fails_open(self):
        # Defensive: the epoch is only meaningful against its pool's
        # round; a sentinel missing the pool key (never emitted by the
        # autoscaler) must not fence -- and without a pool to recheck it
        # takes the plain persist.
        launched, add_mock, epoch_mock, fenced_add = self._launch(
            {'pool-b': 8}, carried_pool=None)
        self.assertTrue(launched)
        add_mock.assert_called_once()
        fenced_add.assert_not_called()
        epoch_mock.assert_not_called()

    def test_round_published_between_precheck_and_persist_skips(self):
        # TOCTOU closed: the pre-check passed (carried epoch 7 is
        # current) but a new round published before the persist; the
        # atomic recheck reports stale and the launch must skip without
        # leaking a row or a launch thread.
        launched, add_mock, _, fenced_add = self._launch(
            {'pool-b': 7}, persist_epoch_current=False)
        self.assertFalse(launched)
        add_mock.assert_not_called()
        fenced_add.assert_called_once()


class TestDemandPlacementGate(unittest.TestCase):
    """Zero-cost holdings >= grant: NEW demand launches skip the free tier."""

    def _launch_demand(self, grant, replicas):
        zero_cost_location = spot_placer.Location.from_pickleable(_K8S_KEY)
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = [zero_cost_location]
        # This fixture isolates the broker grant gate.  Keep the independent
        # speculative-placement gate inert by reporting no active locations.
        placer.active_locations.return_value = []
        placer.ranked_active_locations.return_value = []
        selected = _make_location('us-east-1', 'paid', use_spot=True)
        placer.select_next_location.return_value = selected
        manager = _make_manager(placer)
        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=True), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.reserved_capacity_broker,
                               'get_cached_grant',
                               return_value=grant), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=replicas), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica'):
            manager._launch_replica(9)
        return placer

    def test_holdings_at_grant_skip_zero_cost_preference(self):
        placer = self._launch_demand(1, [_replica(1, _K8S_KEY)])
        placer.select_next_location.assert_called_once()
        self.assertTrue(
            placer.select_next_location.call_args.kwargs.get(
                'skip_zero_cost_preference'))

    def test_holdings_under_grant_keep_preference(self):
        placer = self._launch_demand(5, [_replica(1, _K8S_KEY)])
        placer.select_next_location.assert_called_once()
        self.assertEqual(placer.select_next_location.call_args.kwargs, {})

    def test_no_grant_is_inert(self):
        # Both "no cached entry" and the single-claimant fast path's None
        # grant surface as None from the grant cache: the gate stays inert.
        placer = self._launch_demand(None, [_replica(1, _K8S_KEY)])
        self.assertEqual(placer.select_next_location.call_args.kwargs, {})


class TestPlacerSkipZeroCostPreference(unittest.TestCase):
    """skip_zero_cost_preference excludes the free tier while paid exists."""

    def setUp(self):
        self.k8s = _make_location('research-ctx', 'free')
        self.paid = _make_location('us-east-1', 'paid', use_spot=True)
        self.placer = _make_placer({self.k8s: 0.0, self.paid: 0.2})

    def test_default_prefers_zero_cost(self):
        selected = self.placer.select_next_location()
        self.assertEqual(selected, self.k8s)

    def test_skip_selects_paid(self):
        selected = self.placer.select_next_location(
            skip_zero_cost_preference=True)
        self.assertEqual(selected, self.paid)

    def test_skip_excludes_zero_cost(self):
        selected = self.placer.select_next_location(
            skip_zero_cost_preference=True)
        self.assertEqual(selected, self.paid)

    def test_skip_with_zero_cost_only_set_still_serves(self):
        # The gate throttles placement preference, never availability: a
        # zero-cost-only candidate set must still yield a location.
        placer = _make_placer({self.k8s: 0.0})
        selected = placer.select_next_location(skip_zero_cost_preference=True)
        self.assertEqual(selected, self.k8s)


class TestBrokerPollerCycle(unittest.TestCase):
    """The broker cycle claims, reads the round, and feeds grant + epoch."""

    def _run_cycle(self,
                   allocation,
                   zero_cost=None,
                   replica_infos=(),
                   utilization_gate=False):
        autoscaler = _make_autoscaler(min_replicas=1)
        autoscaler.reserved_fill_utilization_gate = utilization_gate
        if zero_cost is None:
            zero_cost = [spot_placer.Location.from_pickleable(_K8S_KEY)]
        placer = mock.Mock()
        placer.zero_cost_locations.return_value = zero_cost
        placer.active_locations.return_value = list(zero_cost)
        keys = [location.to_pickleable() for location in zero_cost]
        with mock.patch.object(reserved_capacity.serve_state,
                               'get_replica_infos',
                               return_value=list(replica_infos)), \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'upsert_claim') as upsert_mock, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'remove_claim') as remove_mock, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'run_round_if_stale',
                               return_value=allocation) as round_mock:
            reserved_capacity._broker_cycle(autoscaler, placer, 'svc',
                                            zero_cost, keys)
        return autoscaler, upsert_mock, remove_mock, round_mock

    def test_gated_writer_without_telemetry_publishes_armed_blind_signal(self):
        allocation = reserved_capacity_broker.Allocation(grant=0,
                                                         feed=0,
                                                         round_id=1,
                                                         epoch=1,
                                                         snapshot_time=1.0)
        _, upsert_mock, _, _ = self._run_cycle(allocation,
                                               utilization_gate=True)
        self.assertEqual(upsert_mock.call_args.kwargs['activity'], {
            'demonstrated_need': None,
            'boot_hold': False,
        })

    def test_explicitly_ungated_writer_omits_utilization(self):
        allocation = reserved_capacity_broker.Allocation(grant=None,
                                                         feed=0,
                                                         round_id=1,
                                                         epoch=1,
                                                         snapshot_time=1.0)
        _, upsert_mock, _, _ = self._run_cycle(allocation,
                                               utilization_gate=False)
        self.assertIsNone(upsert_mock.call_args.kwargs['activity'])

    def test_unseeded_autoscaler_still_heartbeats_holdings(self):
        # Respawn path whose best-effort boot seed failed: the cycle must
        # seed the location set itself before counting, or the heartbeat
        # under-reports holdings as 0 and the broker reads a holdings
        # SHRINK -- cutting peers' grants past the down-damping on a pure
        # reporting artifact.
        holder = _replica(1, _K8S_KEY)
        holder.reserved_fill = True
        allocation = reserved_capacity_broker.Allocation(grant=3,
                                                         feed=0,
                                                         round_id=1,
                                                         epoch=1,
                                                         snapshot_time=1.0)
        _, upsert_mock, _, _ = self._run_cycle(allocation,
                                               replica_infos=[holder])
        self.assertEqual(upsert_mock.call_args.kwargs['holdings_fill'], 1)

    def test_allocation_feeds_grant_and_epoch(self):
        allocation = reserved_capacity_broker.Allocation(grant=3,
                                                         feed=2,
                                                         round_id=1,
                                                         epoch=4,
                                                         snapshot_time=1.0)
        autoscaler, upsert_mock, _, round_mock = self._run_cycle(allocation)
        upsert_mock.assert_called_once()
        self.assertEqual(
            upsert_mock.call_args.kwargs['pool_key'],
            reserved_capacity_broker.make_pool_key('research-ctx', 'a100'))
        round_mock.assert_called_once()
        self.assertEqual(round_mock.call_args.kwargs['lock_timeout_seconds'], 0)
        self.assertEqual(autoscaler._fill_grant, 3)
        self.assertEqual(autoscaler._fill_grant_epoch, 4)
        self.assertEqual(
            autoscaler._fill_grant_pool_key,
            reserved_capacity_broker.make_pool_key('research-ctx', 'a100'))
        self.assertEqual(autoscaler._fill_snapshot_time, 1.0)

    def test_no_allocation_feeds_zero_without_grant(self):
        with mock.patch.object(
                reserved_capacity,
                '_record_allocation_observation') as record_observation:
            autoscaler, _, _, _ = self._run_cycle(allocation=None)
        record_observation.assert_not_called()
        self.assertIsNone(autoscaler._fill_grant)
        self.assertIsNotNone(autoscaler._fill_snapshot_time)
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)

    def test_allocation_records_only_committed_observation(self):
        allocation = reserved_capacity_broker.Allocation(
            grant=3,
            feed=2,
            round_id=1,
            epoch=4,
            snapshot_time=1.0,
            observed_free=7,
            observed_free_by_accelerator={'a100': 7},
            observed_at=1.0)
        with mock.patch.object(
                reserved_capacity,
                '_record_allocation_observation') as record_observation:
            self._run_cycle(allocation)
        record_observation.assert_called_once()
        self.assertIs(record_observation.call_args.args[2], allocation)

    def test_same_context_accelerators_share_one_broker_group(self):
        other = dict(_K8S_KEY, accelerators={'H100': 1})
        zero_cost = [
            spot_placer.Location.from_pickleable(_K8S_KEY),
            spot_placer.Location.from_pickleable(other),
        ]
        autoscaler, upsert_mock, remove_mock, round_mock = self._run_cycle(
            allocation=None, zero_cost=zero_cost)
        upsert_mock.assert_called_once()
        self.assertEqual(
            upsert_mock.call_args.kwargs['pool_key'],
            reserved_capacity_broker.make_pool_key('research-ctx',
                                                   ('a100', 'h100')))
        remove_mock.assert_not_called()
        round_mock.assert_called_once()
        self.assertIsNone(autoscaler._fill_grant)

    def test_multiple_contexts_withdraw_claim_and_feed_zero(self):
        other = dict(_K8S_KEY, region='other-ctx', accelerators={'H100': 1})
        zero_cost = [
            spot_placer.Location.from_pickleable(_K8S_KEY),
            spot_placer.Location.from_pickleable(other),
        ]
        autoscaler, upsert_mock, remove_mock, round_mock = self._run_cycle(
            allocation=None, zero_cost=zero_cost)
        remove_mock.assert_called_once_with('svc')
        upsert_mock.assert_not_called()
        round_mock.assert_not_called()
        self.assertIsNone(autoscaler._fill_grant)
        # The location set is still seeded: existing holdings keep their
        # scale-down shelter even while fill is inactive.
        self.assertIsNotNone(autoscaler._fill_snapshot_time)

    def test_logical_multi_gpu_shape_withdraws_claim_and_feeds_zero(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        logical_placer = (
            spot_placer.CapacityAwareDynamicFallbackSpotPlacer.__new__(
                spot_placer.CapacityAwareDynamicFallbackSpotPlacer))
        location_data = dict(_K8S_KEY, accelerators={'A100': 2})
        zero_cost = [spot_placer.Location.from_pickleable(location_data)]
        keys = [location.to_pickleable() for location in zero_cost]

        with mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'remove_claim') as remove_mock, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'upsert_claim') as upsert_mock, \
             mock.patch.object(reserved_capacity.reserved_capacity_broker,
                               'run_round_if_stale') as round_mock:
            reserved_capacity._broker_cycle(autoscaler, logical_placer, 'svc',
                                            zero_cost, keys)

        remove_mock.assert_called_once_with('svc')
        upsert_mock.assert_not_called()
        round_mock.assert_not_called()
        self.assertEqual(autoscaler.info()['fill_free_slots'], 0)


class TestReplicaManagerInitIntact(unittest.TestCase):
    """Real __init__ must fully run (fill fixtures bypass it via __new__).

    Pins the regression class where an addition spliced into the middle
    of ReplicaManager.__init__ silently truncated it.
    """

    def test_base_init_sets_version_fields_and_placer_accessor(self):
        spec = types.SimpleNamespace(pool=False,
                                     readiness_headers=None,
                                     readiness_path='/health',
                                     initial_delay_seconds=60,
                                     endpoint_probe_interval_seconds=10,
                                     post_data=None)
        # This regression test exercises the initializer itself, not persisted
        # service lookup.  Do not couple it to the developer's live Serve DB.
        with mock.patch.object(replica_managers.serve_state,
                               'get_service_from_name',
                               return_value=None):
            manager = replica_managers.ReplicaManager('svc', spec, version=3)
        self.assertEqual(manager.latest_version, 3)
        self.assertIsNone(manager.spot_placer)


if __name__ == '__main__':
    unittest.main()


class TestFillDemandSample(unittest.TestCase):
    """The gate's detailed signal is available only with fresh telemetry."""

    def _autoscaler(self):
        spec = service_spec.SkyServiceSpec(
            readiness_path='/health',
            initial_delay_seconds=60,
            readiness_timeout_seconds=30,
            endpoint_probe_interval_seconds=10,
            lb_stream_timeout_seconds=60,
            min_replicas=1,
            max_replicas=100,
            target_concurrency_per_replica=1,
            reserved_capacity_fill={'utilization_gate': True})
        autoscaler = autoscalers.ConcurrencyAutoscaler('svc', spec)
        autoscaler.seed_zero_cost_locations([_K8S_KEY])
        return autoscaler

    def _fill_replica(self, replica_id, **kwargs):
        info = _replica(replica_id, _K8S_KEY, **kwargs)
        info.reserved_fill = True
        return info

    def test_no_sample_without_a_fresh_report(self):
        # The poller converts this lack of utilization proof to fresh NULL
        # need (armed-but-blind) for a gated service; the autoscaler projection
        # itself stays explicit about having no detailed sample.
        autoscaler = self._autoscaler()
        self.assertIsNone(autoscaler.fill_demand_sample([]))

    def test_base_autoscaler_class_has_no_detailed_sample(self):
        spec = service_spec.SkyServiceSpec(readiness_path='/health',
                                           initial_delay_seconds=60,
                                           readiness_timeout_seconds=30,
                                           endpoint_probe_interval_seconds=10,
                                           lb_stream_timeout_seconds=60,
                                           min_replicas=1,
                                           max_replicas=10,
                                           target_qps_per_replica=1.0,
                                           reserved_capacity_fill=True)
        autoscaler = autoscalers.RequestRateAutoscaler('svc', spec)
        self.assertIsNone(autoscaler.fill_demand_sample([]))

    def test_unknown_occupancy_counts_per_replica_not_per_service(self):
        # The decisive property. A service-level "any unknown occupancy"
        # boolean would let 3 flapping replicas out of 77 pin the whole
        # fleet busy forever, making the gate inert on exactly the service
        # it exists for.
        autoscaler = self._autoscaler()
        replicas = [self._fill_replica(i) for i in range(1, 78)]
        autoscaler._in_flight_by_replica_id = {}
        autoscaler._unknown_in_flight_replica_ids = set()
        autoscaler._report_received_at = time.time()
        autoscaler._replica_is_busy = lambda info: info.replica_id <= 3

        sample = autoscaler.fill_demand_sample(replicas)

        self.assertIsNotNone(sample)
        self.assertEqual(sample.busy_fill_holdings, 3)
        self.assertEqual(sample.demonstrated_need(), 3)

    def test_pre_ready_fill_rows_hold_a_release_step(self):
        autoscaler = self._autoscaler()
        booting = self._fill_replica(
            1, status=serve_state.ReplicaStatus.PROVISIONING)
        autoscaler._in_flight_by_replica_id = {}
        autoscaler._unknown_in_flight_replica_ids = set()
        autoscaler._report_received_at = time.time()

        sample = autoscaler.fill_demand_sample([booting])

        self.assertIsNotNone(sample)
        self.assertEqual(sample.pre_ready_fill_holdings, 1)
        self.assertTrue(sample.boot_hold())
        self.assertEqual(sample.demonstrated_need(), 1)

    def test_fully_idle_fleet_demonstrates_no_need(self):
        autoscaler = self._autoscaler()
        replicas = [self._fill_replica(i) for i in range(1, 10)]
        autoscaler._in_flight_by_replica_id = {}
        autoscaler._unknown_in_flight_replica_ids = set()
        autoscaler._report_received_at = time.time()
        autoscaler._replica_is_busy = lambda info: False

        sample = autoscaler.fill_demand_sample(replicas)

        self.assertIsNotNone(sample)
        self.assertEqual(sample.demonstrated_need(), 0)
        self.assertFalse(sample.boot_hold())

    def test_demand_placed_zero_cost_rows_are_not_counted(self):
        # They are demand-protected and exempt from the grant ceiling, so
        # counting them would inflate need by capacity the gate can never
        # reclaim anyway.
        autoscaler = self._autoscaler()
        demand_row = _replica(1, _K8S_KEY)
        demand_row.reserved_fill = False
        autoscaler._in_flight_by_replica_id = {}
        autoscaler._unknown_in_flight_replica_ids = set()
        autoscaler._report_received_at = time.time()
        autoscaler._replica_is_busy = lambda info: True

        sample = autoscaler.fill_demand_sample([demand_row])

        self.assertIsNotNone(sample)
        self.assertEqual(sample.busy_fill_holdings, 0)


class TestOutstandingWorkPartsExtraction(unittest.TestCase):
    """The pure variant must not clobber decision-owned observability."""

    def test_pure_variant_assigns_nothing(self):
        spec = service_spec.SkyServiceSpec(readiness_path='/health',
                                           initial_delay_seconds=60,
                                           readiness_timeout_seconds=30,
                                           endpoint_probe_interval_seconds=10,
                                           lb_stream_timeout_seconds=60,
                                           min_replicas=1,
                                           max_replicas=10,
                                           target_concurrency_per_replica=1,
                                           reserved_capacity_fill=True)
        autoscaler = autoscalers.ConcurrencyAutoscaler('svc', spec)
        autoscaler._in_flight_by_replica_id = {}
        autoscaler._unknown_in_flight_replica_ids = set()
        autoscaler._weighted_queue_work = -1.0
        autoscaler._rejected_concurrency = -1.0

        autoscaler._outstanding_work_parts([])

        self.assertEqual(autoscaler._weighted_queue_work, -1.0)
        self.assertEqual(autoscaler._rejected_concurrency, -1.0)

        total = autoscaler._outstanding_work([])

        self.assertEqual(autoscaler._weighted_queue_work, 0.0)
        self.assertEqual(autoscaler._rejected_concurrency, 0.0)
        self.assertEqual(total, 0.0)
