"""Cost-aware SkyServe replica replacement safety tests."""
# pylint: disable=protected-access
import threading
import types
from unittest import mock

import pytest
from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import service_spec
from sky.utils import common_utils


def _spec(**overrides):
    values = {
        'min_replicas': 2,
        'max_replicas': 2,
        'num_overprovision': None,
        'target_concurrency_per_replica': 1.0,
        'upscale_delay_seconds': 10,
        'downscale_delay_seconds': 10,
        'cost_rebalance': True,
        'cost_rebalance_min_savings_fraction': 0.3,
        'cost_rebalance_max_parallel_replacements': 1,
        'cost_rebalance_stabilization_seconds': 0.0,
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


class _Replica:
    """Minimal real-behavior replica double for autoscaler decisions."""

    def __init__(self,
                 replica_id,
                 location,
                 cost,
                 gpu_count=1,
                 status=serve_state.ReplicaStatus.READY,
                 replacement_for=None):
        self.replica_id = replica_id
        self.cluster_name = f'cluster-{replica_id}'
        self.version = 1
        self.status = status
        self.is_terminal = status in serve_state.ReplicaStatus.terminal_statuses(
        )
        self.is_ready = status == serve_state.ReplicaStatus.READY
        self.cost_rebalance_for_replica_id = replacement_for
        self.status_property = types.SimpleNamespace(
            sky_launch_status=common_utils.ProcessStatus.SUCCEEDED,
            unrecoverable_failure=lambda: False)
        self._location = location
        launched_resources = types.SimpleNamespace(
            accelerators={next(iter(location.accelerators)): gpu_count},
            get_cost=lambda seconds: cost * seconds / 3600)
        self._handle = types.SimpleNamespace(
            launched_resources=launched_resources)

    def get_spot_location(self):
        return self._location

    def handle(self, cluster_record=None):
        del cluster_record
        return self._handle


def _autoscaler(spec=None):
    scaler = autoscalers.ConcurrencyAutoscaler('svc',
                                               spec or _spec(),
                                               version=1)
    scaler.latest_version_ever_ready = 1
    return scaler


def _instance_aware_autoscaler():
    spec = _spec(target_qps_per_replica={'L4': 1.0})
    scaler = autoscalers.InstanceAwareRequestRateAutoscaler('svc',
                                                            spec,
                                                            version=1)
    scaler.latest_version_ever_ready = 1
    return scaler


def _report(scaler, replicas):
    scaler.collect_request_information({
        'timestamps': [],
        'in_flight_by_replica_id': {
            replica.replica_id: 1 for replica in replicas if replica.is_ready
        },
        'queue_depth': 0,
        'rejected_in_window': 0,
        'unknown_in_flight_replica_ids': [],
    })


def _decisions(scaler, replicas):
    return scaler.generate_scaling_decisions(replicas, [1])


def _status_property(
        wait_for_idle: bool) -> replica_managers.ReplicaStatusProperty:
    status_property = replica_managers.ReplicaStatusProperty()
    status_property.wait_for_idle_before_termination = wait_for_idle
    return status_property


class TestCostRebalanceSpec:
    """Configuration validation and serialization remain compatible."""

    _YAML = """
service:
  readiness_probe: /
  replica_policy:
    min_replicas: 1
    max_replicas: 2
    target_concurrency_per_replica: 1
    spot_placer: dynamic_fallback
    cost_rebalance:
      min_savings_fraction: 0.3
      max_parallel_replacements: 8
      stabilization_seconds: 300
"""

    def test_round_trip(self):
        spec = service_spec.SkyServiceSpec.from_yaml_str(self._YAML)
        assert spec.cost_rebalance
        assert spec.cost_rebalance_min_savings_fraction == pytest.approx(0.3)
        assert spec.cost_rebalance_max_parallel_replacements == 8
        assert spec.cost_rebalance_stabilization_seconds == 300
        copied = spec.copy()
        assert copied.to_yaml_config()['replica_policy']['cost_rebalance'] == {
            'min_savings_fraction': 0.3,
            'max_parallel_replacements': 8,
            'stabilization_seconds': 300,
        }

    @pytest.mark.parametrize('field,value,match', [
        ('min_savings_fraction', 0, 'min_savings_fraction'),
        ('min_savings_fraction', float('nan'), 'min_savings_fraction'),
        ('max_parallel_replacements', 0, 'max_parallel_replacements'),
        ('stabilization_seconds', -1, 'stabilization_seconds'),
    ])
    def test_programmatic_validation(self, field, value, match):
        base = service_spec.SkyServiceSpec.from_yaml_str(self._YAML)
        config = {
            'min_savings_fraction': 0.3,
            'max_parallel_replacements': 1,
            'stabilization_seconds': 0,
        }
        config[field] = value
        with pytest.raises(ValueError, match=match):
            base.copy(cost_rebalance=config)

    def test_programmatic_validation_requires_object(self):
        base = service_spec.SkyServiceSpec.from_yaml_str(self._YAML)
        with pytest.raises(ValueError, match='must be an object'):
            base.copy(cost_rebalance=True)


class TestEconomicDecisions:
    """Economic replacements honor cost, capacity, and pair lifecycle."""

    def _fleet(self, candidate_cost=0.0, incumbent_gpus=1, candidate_gpus=1):
        paid = make_location('paid',
                             accelerators={'L4': incumbent_gpus},
                             use_spot=True)
        cheap = make_location('research',
                              accelerators={'A100': candidate_gpus},
                              use_spot=False)
        placer = make_placer({paid: 1.0, cheap: candidate_cost})
        replicas = [
            _Replica(1, paid, 1.0, gpu_count=incumbent_gpus),
            _Replica(2, paid, 1.0, gpu_count=incumbent_gpus),
        ]
        return placer, paid, cheap, replicas

    def test_exact_threshold_launches_pinned_replacement(self):
        scaler = _autoscaler()
        placer, _, cheap, replicas = self._fleet(candidate_cost=0.7)
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)

        decisions = _decisions(scaler, replicas)

        launches = [
            d for d in decisions
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        assert len(launches) == 1
        assert launches[
            0].reason == autoscalers.AutoscalerDecisionReason.COST_REBALANCE
        assert launches[0].target['region'] == cheap.region
        assert launches[0].target[
            constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY] == 1

    def test_below_threshold_does_not_launch(self):
        scaler = _autoscaler()
        placer, _, _, replicas = self._fleet(candidate_cost=0.701)
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)
        assert not [
            d for d in _decisions(scaler, replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]

    def test_candidate_costs_resolve_before_decision_lock(self):
        scaler = _autoscaler()
        placer, paid, cheap, replicas = self._fleet(candidate_cost=0.0)
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)
        costs = {paid: 1.0, cheap: 0.0}

        def _resolve_with_lock_probe(location):
            acquired = []

            def _probe():
                got_lock = scaler._logical_state_lock.acquire(timeout=1)
                acquired.append(got_lock)
                if got_lock:
                    scaler._logical_state_lock.release()

            probe = threading.Thread(target=_probe)
            probe.start()
            probe.join(timeout=2)
            assert not probe.is_alive()
            assert acquired == [True]
            return costs[location]

        with mock.patch.object(placer,
                               'cost_per_hour',
                               side_effect=_resolve_with_lock_probe) as resolve:
            decisions = _decisions(scaler, replicas)

        launches = [
            decision for decision in decisions if decision.operator ==
            autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        assert len(launches) == 1
        assert launches[0].target['region'] == cheap.region
        assert resolve.call_count == len(costs)

    @pytest.mark.parametrize('autoscaler_factory,lock_name', [
        (_autoscaler, '_logical_state_lock'),
        (_instance_aware_autoscaler, '_instance_state_lock'),
    ])
    def test_price_resolution_does_not_block_demand_ingestion(
            self, autoscaler_factory, lock_name):
        scaler = autoscaler_factory()
        placer, paid, cheap, _ = self._fleet(candidate_cost=0.0)
        scaler.set_spot_placer(placer)
        costs = {paid: 1.0, cheap: 0.0}
        resolution_started = threading.Event()
        release_resolution = threading.Event()
        ingestion_finished = threading.Event()
        decision_errors = []
        ingestion_errors = []

        def _blocking_cost(location):
            resolution_started.set()
            if not release_resolution.wait(timeout=5):
                raise TimeoutError('test did not release cost resolution')
            return costs[location]

        def _decide():
            try:
                scaler.generate_scaling_decisions([], [1])
            except Exception as error:  # pylint: disable=broad-except
                decision_errors.append(error)

        def _ingest():
            try:
                scaler.collect_request_information({'timestamps': []})
            except Exception as error:  # pylint: disable=broad-except
                ingestion_errors.append(error)
            finally:
                ingestion_finished.set()

        with mock.patch.object(placer,
                               'cost_per_hour',
                               side_effect=_blocking_cost), mock.patch.object(
                                   scaler,
                                   '_generate_scaling_decisions_locked',
                                   return_value=[]):
            decision_thread = threading.Thread(target=_decide)
            decision_thread.start()
            assert resolution_started.wait(timeout=5)
            ingestion_thread = threading.Thread(target=_ingest)
            ingestion_thread.start()
            assert ingestion_finished.wait(timeout=1), lock_name
            release_resolution.set()
            decision_thread.join(timeout=5)
            ingestion_thread.join(timeout=5)

        assert not decision_thread.is_alive()
        assert not ingestion_thread.is_alive()
        assert not decision_errors
        assert not ingestion_errors
        assert scaler._cost_rebalance_costs_for_tick is None

    def test_candidate_cost_failure_does_not_abort_other_candidate(self):
        scaler = _autoscaler()
        paid = make_location('paid', accelerators={'L4': 1}, use_spot=True)
        broken = make_location('broken',
                               accelerators={'A100': 1},
                               use_spot=False)
        cheap = make_location('research',
                              accelerators={'A100': 1},
                              use_spot=False)
        placer = make_placer({paid: 1.0, broken: 0.1, cheap: 0.2})
        scaler.set_spot_placer(placer)
        replicas = [_Replica(1, paid, 1.0), _Replica(2, paid, 1.0)]
        _report(scaler, replicas)

        def _cost(location):
            if location is broken:
                raise RuntimeError('catalog unavailable')
            return placer.location2cost[location]

        with mock.patch.object(placer, 'cost_per_hour', side_effect=_cost):
            decisions = _decisions(scaler, replicas)

        launches = [
            decision for decision in decisions if decision.operator ==
            autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        assert len(launches) == 1
        assert launches[0].target['region'] == cheap.region

    def test_disabled_policy_does_not_warm_candidate_costs(self):
        scaler = _autoscaler()
        placer, _, _, replicas = self._fleet(candidate_cost=0.0)
        scaler.set_spot_placer(placer)
        scaler.cost_rebalance = False
        _report(scaler, replicas)

        with mock.patch.object(
                placer,
                'cost_per_hour',
                side_effect=AssertionError(
                    'disabled policy must not warm candidate costs')):
            _decisions(scaler, replicas)

    @pytest.mark.parametrize('autoscaler_factory', [
        _autoscaler,
        _instance_aware_autoscaler,
    ])
    def test_cost_snapshot_is_cleared_after_decision_failure(
            self, autoscaler_factory):
        scaler = autoscaler_factory()
        placer, _, _, _ = self._fleet(candidate_cost=0.0)
        scaler.set_spot_placer(placer)

        with mock.patch.object(scaler,
                               '_generate_scaling_decisions_locked',
                               side_effect=RuntimeError('decision failed')):
            with pytest.raises(RuntimeError, match='decision failed'):
                scaler.generate_scaling_decisions([], [1])

        assert scaler._cost_rebalance_costs_for_tick is None

    def test_lower_capacity_candidate_is_rejected(self):
        scaler = _autoscaler()
        placer, _, _, replicas = self._fleet(candidate_cost=0.1,
                                             incumbent_gpus=2,
                                             candidate_gpus=1)
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)
        assert not [
            d for d in _decisions(scaler, replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]

    def test_exact_card_catalog_blocks_cross_card_replacement(self):
        scaler = _autoscaler()
        scaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        placer, _, _, replicas = self._fleet(candidate_cost=0.0)
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)

        assert not [
            decision for decision in _decisions(scaler, replicas) if
            decision.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]

    def test_exact_card_catalog_allows_same_card_replacement(self):
        scaler = _autoscaler()
        scaler.set_configured_accelerator_shapes({'L4': 1})
        paid = make_location('paid', accelerators={'L4': 1}, use_spot=True)
        cheap = make_location('research',
                              accelerators={'L4': 1},
                              use_spot=False)
        scaler.set_spot_placer(make_placer({paid: 1.0, cheap: 0.0}))
        replicas = [_Replica(1, paid, 1.0), _Replica(2, paid, 1.0)]
        _report(scaler, replicas)

        launches = [
            decision for decision in _decisions(scaler, replicas) if
            decision.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        assert len(launches) == 1
        assert launches[0].target['region'] == cheap.region

    def test_persisted_cross_card_pair_keeps_incumbent(self):
        scaler = _autoscaler()
        scaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        paid = make_location('paid', accelerators={'L4': 1}, use_spot=True)
        cheap = make_location('research',
                              accelerators={'A100': 1},
                              use_spot=False)
        scaler.set_spot_placer(make_placer({paid: 1.0, cheap: 0.0}))
        victim = _Replica(1, paid, 1.0)
        peer = _Replica(2, paid, 1.0)
        replacement = _Replica(3, cheap, 0.0, replacement_for=1)
        replicas = [victim, peer, replacement]
        _report(scaler, replicas)

        strict = [
            decision for decision in _decisions(scaler, replicas)
            if decision.reason ==
            autoscalers.AutoscalerDecisionReason.COST_REBALANCE
        ]
        assert [(decision.operator, decision.target) for decision in strict
               ] == [(autoscalers.AutoscalerDecisionOperator.SCALE_DOWN, 3)]

    def test_stabilization_requires_continuous_eligibility(self, monkeypatch):
        now = [100.0]
        monkeypatch.setattr(autoscalers.time, 'monotonic', lambda: now[0])
        scaler = _autoscaler(_spec(cost_rebalance_stabilization_seconds=300.0))
        placer, _, _, replicas = self._fleet()
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)
        assert not [
            d for d in _decisions(scaler, replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        now[0] = 399.0
        assert not [
            d for d in _decisions(scaler, replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        now[0] = 400.0
        assert len([
            d for d in _decisions(scaler, replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]) == 1

    def test_demand_decision_resets_stabilization(self, monkeypatch):
        now = [100.0]
        monkeypatch.setattr(autoscalers.time, 'monotonic', lambda: now[0])
        scaler = _autoscaler(_spec(cost_rebalance_stabilization_seconds=300.0))
        placer, _, _, replicas = self._fleet()
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)
        assert not _decisions(scaler, replicas)

        now[0] = 200.0
        ordinary = autoscalers.AutoscalerDecision(
            autoscalers.AutoscalerDecisionOperator.SCALE_UP, None)
        assert not scaler._generate_cost_rebalance_decisions(
            replicas, [ordinary])

        now[0] = 400.0
        assert not _decisions(scaler, replicas)
        now[0] = 700.0
        assert len([
            d for d in _decisions(scaler, replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]) == 1

    def test_full_slot_still_resets_discontinuous_eligibility(
            self, monkeypatch):
        now = [100.0]
        monkeypatch.setattr(autoscalers.time, 'monotonic', lambda: now[0])
        scaler = _autoscaler(_spec(cost_rebalance_stabilization_seconds=300.0))
        placer, _, cheap, replicas = self._fleet()
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)
        assert not _decisions(scaler, replicas)

        replacement = _Replica(3,
                               cheap,
                               0.0,
                               status=serve_state.ReplicaStatus.STARTING,
                               replacement_for=1)
        all_replicas = replicas + [replacement]
        now[0] = 150.0
        assert not _decisions(scaler, all_replicas)

        # The only cheap location becomes unavailable while the existing
        # pair occupies the sole replacement slot. This must break the
        # continuity window even though no new launch can be emitted.
        now[0] = 200.0
        placer.set_preemptive(cheap)
        assert not _decisions(scaler, all_replicas)

        now[0] = 250.0
        placer.set_active(cheap)
        replacement.status = serve_state.ReplicaStatus.FAILED_PROVISION
        replacement.is_terminal = True
        assert not _decisions(scaler, all_replicas)

        now[0] = 400.0
        assert not [
            d for d in _decisions(scaler, all_replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        now[0] = 550.0
        assert len([
            d for d in _decisions(scaler, all_replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]) == 1

    def test_transient_cost_lookup_failure_is_not_cached(self):
        scaler = _autoscaler()
        _, _, _, replicas = self._fleet()
        replica = replicas[0]
        replica.handle = mock.Mock(
            side_effect=[RuntimeError('transient'), replica._handle])

        assert scaler._get_hourly_cost_from_replica_info(replica) == 0.0
        assert scaler._get_hourly_cost_from_replica_info(replica) == 1.0
        assert replica.handle.call_count == 2

    def test_incumbent_drains_only_after_replacement_ready(self):
        scaler = _autoscaler()
        placer, _, cheap, replicas = self._fleet()
        scaler.set_spot_placer(placer)
        replacement = _Replica(3,
                               cheap,
                               0.0,
                               status=serve_state.ReplicaStatus.STARTING,
                               replacement_for=1)
        all_replicas = replicas + [replacement]
        _report(scaler, replicas)
        assert not [
            d for d in _decisions(scaler, all_replicas)
            if d.reason == autoscalers.AutoscalerDecisionReason.COST_REBALANCE
        ]

        replacement.status = serve_state.ReplicaStatus.READY
        replacement.is_ready = True
        _report(scaler, all_replicas)
        strict = [
            d for d in _decisions(scaler, all_replicas)
            if d.reason == autoscalers.AutoscalerDecisionReason.COST_REBALANCE
        ]
        assert [(d.operator, d.target) for d in strict
               ] == [(autoscalers.AutoscalerDecisionOperator.SCALE_DOWN, 1)]

    def test_failed_replacement_never_drains_incumbent(self):
        scaler = _autoscaler()
        placer, _, cheap, replicas = self._fleet()
        scaler.set_spot_placer(placer)
        failed = _Replica(3,
                          cheap,
                          0.0,
                          status=serve_state.ReplicaStatus.FAILED_PROVISION,
                          replacement_for=1)
        _report(scaler, replicas)
        assert not [
            d for d in _decisions(scaler, replicas + [failed])
            if (d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
                and d.target == 1)
        ]

    def test_persisted_pair_resumes_after_autoscaler_rebuild(self):
        placer, _, cheap, replicas = self._fleet()
        replacement = _Replica(3, cheap, 0.0, replacement_for=1)
        rebuilt = _autoscaler()
        rebuilt.set_spot_placer(placer)
        _report(rebuilt, replicas + [replacement])
        strict = [
            d for d in _decisions(rebuilt, replicas + [replacement])
            if d.reason == autoscalers.AutoscalerDecisionReason.COST_REBALANCE
        ]
        assert [(d.operator, d.target) for d in strict
               ] == [(autoscalers.AutoscalerDecisionOperator.SCALE_DOWN, 1)]

    def test_parallel_replacements_are_bounded(self):
        spec = _spec(min_replicas=3,
                     max_replicas=3,
                     cost_rebalance_max_parallel_replacements=2)
        scaler = _autoscaler(spec)
        paid = make_location('paid', accelerators={'L4': 1}, use_spot=True)
        cheap_a = make_location('research-a',
                                accelerators={'A100': 1},
                                use_spot=False)
        cheap_b = make_location('research-b',
                                accelerators={'A100': 1},
                                use_spot=False)
        scaler.set_spot_placer(
            make_placer({
                paid: 1.0,
                cheap_a: 0.0,
                cheap_b: 0.0,
            }))
        replicas = [_Replica(i, paid, 1.0) for i in (1, 2, 3)]
        _report(scaler, replicas)
        launches = [
            d for d in _decisions(scaler, replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        assert sorted(launch.target['region'] for launch in launches) == [
            'research-a', 'research-b'
        ]

    def test_location_load_is_computed_once_per_tick(self):
        replica_count = 50
        paid = make_location('paid', accelerators={'L4': 1}, use_spot=True)
        cheap_locations = [
            make_location(f'research-{i}',
                          accelerators={'A100': 1},
                          use_spot=False) for i in range(9)
        ]
        locations = [paid, *cheap_locations]
        scaler = _autoscaler(
            _spec(min_replicas=replica_count,
                  max_replicas=replica_count,
                  cost_rebalance_max_parallel_replacements=1))
        scaler.set_spot_placer(
            make_placer({
                paid: 1.0,
                **{
                    location: 0.0 for location in cheap_locations
                },
            }))
        replicas = [
            _Replica(replica_id, paid, 1.0)
            for replica_id in range(replica_count)
        ]
        _report(scaler, replicas)

        matcher = autoscalers.spot_placer.locations_match_placement
        with mock.patch.object(autoscalers.spot_placer,
                               'locations_match_placement',
                               wraps=matcher) as matches:
            launches = [
                decision for decision in _decisions(scaler, replicas)
                if decision.operator ==
                autoscalers.AutoscalerDecisionOperator.SCALE_UP
            ]

        assert len(launches) == 1
        # One fleet-load pass, one candidate pass per replica, and one update
        # for the launched location.  The previous implementation rebuilt the
        # fleet load for every incumbent and exceeded 25,000 comparisons here.
        assert matches.call_count <= len(locations) * (2 * replica_count + 1)

    def test_disabled_policy_does_not_downgrade_pending_strict_drain(self):
        scaler = _autoscaler()
        placer, _, cheap, replicas = self._fleet()
        scaler.set_spot_placer(placer)
        scaler.cost_rebalance = False
        replacement = _Replica(3, cheap, 0.0, replacement_for=1)
        all_replicas = replicas + [replacement]
        _report(scaler, all_replicas)

        first = [
            d for d in _decisions(scaler, all_replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
        ]
        assert [(d.target, d.reason) for d in first
               ] == [(3, autoscalers.AutoscalerDecisionReason.COST_REBALANCE)]

        replacement.status = serve_state.ReplicaStatus.SHUTTING_DOWN
        replacement.is_ready = False
        replacement.status_property.sky_down_status = (
            common_utils.ProcessStatus.SCHEDULED)
        assert not [
            d for d in _decisions(scaler, all_replicas)
            if (d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_DOWN
                and d.target == 3)
        ]


class TestPinnedReplacementLaunch:
    """Actuation pins the selected location and durably records the pair."""

    def test_pair_metadata_is_persisted_and_internal_marker_is_stripped(self):
        paid = make_location('paid', accelerators={'L4': 1}, use_spot=True)
        cheap = make_location('research',
                              accelerators={'A100': 1},
                              use_spot=False)
        placer = make_placer({paid: 1.0, cheap: 0.0})
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._resource_scope = None
        manager._spot_placer = placer
        manager.yaml_content = 'resources: {}'
        manager.latest_version = 1
        manager._launch_thread_pool = {}
        manager._replica_to_request_id = {}
        manager._replica_to_launch_cancelled = {}
        manager._persist_replica = mock.Mock()
        override = cheap.to_dict()
        override[constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY] = 7

        with mock.patch.object(replica_managers, '_should_use_spot'), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.spot_placer.Location,
                               'from_resources_override',
                               return_value=cheap), \
             mock.patch.object(replica_managers.thread_utils, 'SafeThread'):
            assert manager._launch_replica(8, override)

        info = manager._persist_replica.call_args.args[1]
        assert info.cost_rebalance_for_replica_id == 7
        assert constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY not in info.resources_override
        assert info.resources_override['region'] == 'research'
        assert info.is_spot is False

    def test_invalid_recovery_pin_retires_persisted_replacement(self):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._resource_scope = None
        manager._spot_placer = None
        manager.yaml_content = 'resources: {}'
        manager._launch_thread_pool = {}
        manager._terminate_replica = mock.Mock()

        with mock.patch.object(replica_managers,
                               '_should_use_spot',
                               return_value=True):
            assert not manager._launch_replica(
                8, {'region': 'research'},
                prior_cost_rebalance_for_replica_id=7,
                recovering_existing_replica=True,
                prior_version=1,
                prior_yaml_content='resources: {}')

        manager._terminate_replica.assert_called_once_with(
            8,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)


def _recovery_manager():
    """Bare manager wired with just the state the recovery pass reads."""
    manager = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    manager._service_name = 'svc'
    manager.latest_version = 1
    manager.yaml_content = 'resources: {}'
    manager._is_pool = False
    manager._lb_in_flight_report = None
    manager._spot_placer = None
    manager._launch_thread_pool = {}
    manager._down_thread_pool = {}
    manager._wait_for_idle_trackers = {}
    manager._terminate_replica = mock.Mock()
    manager._persist_replica = mock.Mock()
    manager._launch_replica = mock.Mock()
    return manager


def _pending_row(replica_id, replacement_for):
    status_property = replica_managers.ReplicaStatusProperty()
    row = types.SimpleNamespace(
        replica_id=replica_id,
        cluster_name=f'cluster-{replica_id}',
        version=1,
        is_spot=False,
        status=serve_state.ReplicaStatus.PENDING,
        status_property=status_property,
        resources_override={'region': 'research'},
        reserved_fill=False,
    )
    if replacement_for is not _ABSENT:
        row.cost_rebalance_for_replica_id = replacement_for
    return row


_ABSENT = object()


class TestRecoveryRedrive:
    """Recovery re-drives keep (or safely drop) the persisted pairing."""

    def _recover(self, row):
        manager = _recovery_manager()
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[row]), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_status_fields',
                               return_value={}):
            manager._recover_replica_operations()
        return manager

    def test_pairing_is_forwarded_to_the_redriven_launch(self):
        row = _pending_row(3, replacement_for=7)
        manager = self._recover(row)
        kwargs = manager._launch_replica.call_args.kwargs
        assert kwargs['prior_cost_rebalance_for_replica_id'] == 7
        assert kwargs['resources_override'] == {'region': 'research'}

    def test_pre_field_row_redrives_without_pairing_kwarg(self):
        row = _pending_row(3, replacement_for=_ABSENT)
        manager = self._recover(row)
        kwargs = manager._launch_replica.call_args.kwargs
        assert 'prior_cost_rebalance_for_replica_id' not in kwargs

    def test_ordinary_row_redrives_without_pairing_kwarg(self):
        row = _pending_row(3, replacement_for=None)
        manager = self._recover(row)
        kwargs = manager._launch_replica.call_args.kwargs
        assert 'prior_cost_rebalance_for_replica_id' not in kwargs


class TestPinnedLaunchFailClosed:
    """A pinned replacement launch is skipped when its location is gone."""

    def _manager(self, placer):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._resource_scope = None
        manager._spot_placer = placer
        manager.yaml_content = 'resources: {}'
        manager.latest_version = 1
        manager._launch_thread_pool = {}
        manager._replica_to_request_id = {}
        manager._replica_to_launch_cancelled = {}
        manager._persist_replica = mock.Mock()
        return manager

    def _launch(self, manager, location):
        override = location.to_dict()
        override[constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY] = 7
        with mock.patch.object(replica_managers, '_should_use_spot'), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.spot_placer.Location,
                               'from_resources_override',
                               return_value=location), \
             mock.patch.object(replica_managers.thread_utils, 'SafeThread'):
            return manager._launch_replica(8, override)

    def test_benched_location_skips_launch_without_a_replica_row(self):
        cheap = make_location('research',
                              accelerators={'A100': 1},
                              use_spot=False)
        placer = make_placer({cheap: 0.0})
        # Bench without a timestamp so TTL decay (env-overridable) cannot
        # flip the location back to ACTIVE mid-test.
        placer.location2status[cheap] = (
            replica_managers.spot_placer.LocationStatus.PREEMPTED)
        manager = self._manager(placer)
        assert self._launch(manager, cheap) is False
        manager._persist_replica.assert_not_called()
        assert not manager._launch_thread_pool

    def test_unknown_location_skips_launch_without_a_replica_row(self):
        cheap = make_location('research',
                              accelerators={'A100': 1},
                              use_spot=False)
        other = make_location('elsewhere',
                              accelerators={'A100': 1},
                              use_spot=False)
        manager = self._manager(make_placer({other: 0.0}))
        assert self._launch(manager, cheap) is False
        manager._persist_replica.assert_not_called()
        assert not manager._launch_thread_pool


class TestWaitForIdleRecovery:
    """A strict economic drain survives a controller restart intact."""

    def test_restart_reregisters_tracker_and_still_requires_idle_proof(self):
        manager = _recovery_manager()
        status_property = replica_managers.ReplicaStatusProperty(
            sky_launch_status=common_utils.ProcessStatus.SUCCEEDED,
            sky_down_status=common_utils.ProcessStatus.SCHEDULED,
            is_scale_down=True)
        status_property.wait_for_idle_before_termination = True
        info = types.SimpleNamespace(
            replica_id=1,
            cluster_name='cluster-1',
            url='http://replica',
            is_spot=False,
            status=serve_state.ReplicaStatus.SHUTTING_DOWN,
            status_property=status_property)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_status_fields',
                               return_value={}):
            manager._recover_replica_operations()

        # Not re-driven into a bounded drain: recovery only re-registers
        # the zero-occupancy wait.
        manager._terminate_replica.assert_not_called()
        assert manager._wait_for_idle_trackers[1] is not None

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: info}), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_status_fields',
                               return_value={'cluster-1': ('UP', 1)}):
            # No LB report yet: termination stays inadmissible.
            manager._refresh_wait_for_idle()
            manager._terminate_replica.assert_not_called()

            # Occupied report: still inadmissible.
            manager._lb_in_flight_report = (replica_managers.time.monotonic(), {
                'http://replica': 1
            }, {'http://replica'}, set(), set(), 'lb-1')
            manager._refresh_wait_for_idle()
            manager._terminate_replica.assert_not_called()

            # Fresh explicit-idle report: the drain is finally admitted.
            manager._lb_in_flight_report = (replica_managers.time.monotonic(), {
                'http://replica': 0
            }, set(), set(), set(), 'lb-1')
            manager._refresh_wait_for_idle()

        assert not status_property.wait_for_idle_before_termination
        manager._terminate_replica.assert_called_once_with(
            1,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)


class TestPolicyDisabledPairCompletion:
    """Disabling the policy keeps the incumbent and drains the replacement."""

    def _pair(self):
        paid = make_location('paid', accelerators={'L4': 1}, use_spot=True)
        cheap = make_location('research',
                              accelerators={'A100': 1},
                              use_spot=False)
        incumbents = [_Replica(1, paid, 1.0), _Replica(2, paid, 1.0)]
        replacement = _Replica(3, cheap, 0.0, replacement_for=1)
        return incumbents, replacement

    def test_ready_replacement_is_strictly_drained_not_the_incumbent(self):
        scaler = _autoscaler(_spec(cost_rebalance=False))
        incumbents, replacement = self._pair()
        decisions = scaler._generate_cost_rebalance_decisions(
            incumbents + [replacement], [])
        assert [(d.operator, d.target, d.reason) for d in decisions
               ] == [(autoscalers.AutoscalerDecisionOperator.SCALE_DOWN, 3,
                      autoscalers.AutoscalerDecisionReason.COST_REBALANCE)]

    def test_unready_replacement_is_scaled_down_with_ordinary_drain(self):
        scaler = _autoscaler(_spec(cost_rebalance=False))
        incumbents, replacement = self._pair()
        replacement.status = serve_state.ReplicaStatus.STARTING
        replacement.is_ready = False
        decisions = scaler._generate_cost_rebalance_decisions(
            incumbents + [replacement], [])
        assert [(d.operator, d.target, d.reason) for d in decisions] == [
            (autoscalers.AutoscalerDecisionOperator.SCALE_DOWN, 3, None)
        ]


class TestStrictDrain:
    """Economic retirement cannot terminate work with unknown occupancy."""

    def test_refresh_snapshots_rows_and_cluster_liveness_once(self):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._wait_for_idle_trackers = {
            replica_id: (mock.Mock(return_value=False), float('inf'))
            for replica_id in range(52)
        }
        manager._persist_replica = mock.Mock()
        manager._terminate_replica = mock.Mock()
        infos = {
            replica_id: types.SimpleNamespace(
                replica_id=replica_id,
                cluster_name=f'cluster-{replica_id}',
                status_property=_status_property(True))
            for replica_id in range(51)
        }
        # Row 50 disappeared. Row 51 still exists but its durable drain intent
        # was cancelled. Neither should remain tracked or be terminated.
        infos.pop(50)
        infos[51] = types.SimpleNamespace(
            replica_id=51,
            cluster_name='cluster-51',
            status_property=_status_property(False))
        live_clusters = {
            f'cluster-{replica_id}': ('UP', 1)
            for replica_id in range(50)
            if replica_id != 1
        }

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                return_value=infos) as replica_snapshot, \
             mock.patch.object(
                 replica_managers.serve_state,
                 'get_replica_info_from_id',
                 side_effect=AssertionError('per-replica row read')), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value=live_clusters) as cluster_snapshot, \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'cluster_with_name_exists',
                 side_effect=AssertionError('per-cluster existence read')):
            manager._refresh_wait_for_idle()

        replica_snapshot.assert_called_once_with('svc', list(range(52)))
        cluster_snapshot.assert_called_once_with(
            [f'cluster-{replica_id}' for replica_id in range(50)])
        assert set(manager._wait_for_idle_trackers) == (set(range(50)) - {1})
        manager._persist_replica.assert_called_once_with(1, infos[1])
        manager._terminate_replica.assert_called_once_with(
            1,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)

    def test_refresh_snapshot_failure_preserves_tracker_for_retry(self):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        tracked = (mock.Mock(return_value=False), float('inf'))
        manager._wait_for_idle_trackers = {1: tracked}
        manager._persist_replica = mock.Mock()
        manager._terminate_replica = mock.Mock()
        info = types.SimpleNamespace(replica_id=1,
                                     cluster_name='cluster-1',
                                     status_property=_status_property(True))

        with mock.patch.object(
                replica_managers.serve_state,
                'get_replica_infos_from_ids',
                side_effect=RuntimeError('replica snapshot failed')):
            with pytest.raises(RuntimeError, match='replica snapshot failed'):
                manager._refresh_wait_for_idle()
        assert manager._wait_for_idle_trackers == {1: tracked}

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: info}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 side_effect=RuntimeError('cluster snapshot failed')):
            with pytest.raises(RuntimeError, match='cluster snapshot failed'):
                manager._refresh_wait_for_idle()
        assert manager._wait_for_idle_trackers == {1: tracked}
        manager._persist_replica.assert_not_called()
        manager._terminate_replica.assert_not_called()

    def test_logical_idle_proof_timeout_aborts_instead_of_force_killing(
            self, monkeypatch):
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: 100.0)
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._logical_state_lock = threading.RLock()
        manager._logical_controller_epoch = 'test-controller-epoch'
        manager._logical_reconcile_snapshot = None
        manager._logical_target = None
        manager._wait_for_idle_trackers = {
            1: (mock.Mock(return_value=False), 99.0)
        }
        manager._persist_replica = mock.Mock()
        manager._terminate_replica = mock.Mock()
        status = replica_managers.ReplicaStatusProperty(
            sky_launch_status=common_utils.ProcessStatus.SUCCEEDED,
            sky_down_status=common_utils.ProcessStatus.SCHEDULED,
            is_scale_down=True,
            wait_for_idle_before_termination=True,
            logical_retirement_version=1,
            logical_retirement_controller_epoch='test-controller-epoch',
            logical_retirement_generation=4,
            logical_retirement_target_capacity=8)
        info = types.SimpleNamespace(replica_id=1,
                                     cluster_name='cluster-1',
                                     version=1,
                                     status_property=status)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: info}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={'cluster-1': ('UP', 1)}):
            manager._refresh_wait_for_idle()

        manager._terminate_replica.assert_not_called()
        assert not status.wait_for_idle_before_termination
        assert status.sky_down_status is None
        assert status.logical_retirement_version is None
        assert 1 not in manager._wait_for_idle_trackers

    def test_stale_logical_retirement_aborts_before_idle(self, monkeypatch):
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: 100.0)
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._logical_state_lock = threading.RLock()
        manager._logical_controller_epoch = 'new-controller-epoch'
        manager._logical_reconcile_snapshot = None
        manager._logical_target = None
        manager._wait_for_idle_trackers = {
            1: (mock.Mock(return_value=False), 200.0)
        }
        manager._persist_replica = mock.Mock()
        manager._terminate_replica = mock.Mock()
        status = replica_managers.ReplicaStatusProperty(
            sky_launch_status=common_utils.ProcessStatus.SUCCEEDED,
            sky_down_status=common_utils.ProcessStatus.SCHEDULED,
            is_scale_down=True,
            wait_for_idle_before_termination=True,
            logical_retirement_version=1,
            logical_retirement_controller_epoch='old-controller-epoch',
            logical_retirement_generation=4,
            logical_retirement_target_capacity=8)
        info = types.SimpleNamespace(replica_id=1,
                                     cluster_name='cluster-1',
                                     status_property=status)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: info}), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={'cluster-1': ('UP', 1)}):
            manager._refresh_wait_for_idle()

        manager._terminate_replica.assert_not_called()
        assert status.sky_down_status is None
        assert not status.is_scale_down
        assert status.logical_retirement_version is None
        assert 1 not in manager._wait_for_idle_trackers

    def test_off_route_intent_precedes_zero_occupancy_termination(self):
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._is_pool = False
        manager._wait_for_idle_trackers = {}
        manager._lb_in_flight_report = None
        manager._persist_replica = mock.Mock()
        manager._terminate_replica = mock.Mock()
        info = types.SimpleNamespace(
            replica_id=1,
            cluster_name='cluster-1',
            url='http://replica',
            status_property=replica_managers.ReplicaStatusProperty(
                sky_launch_status=common_utils.ProcessStatus.SUCCEEDED))

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: info}), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_status_fields',
                               return_value={'cluster-1': ('UP', 1)}):
            manager._defer_scale_down_until_idle(1)
            assert info.status_property.wait_for_idle_before_termination
            assert info.status_property.sky_down_status == common_utils.ProcessStatus.SCHEDULED
            manager._terminate_replica.assert_not_called()

            received = replica_managers.time.monotonic()
            manager._lb_in_flight_report = (received, {
                'http://replica': 1
            }, {'http://replica'}, set(), set(), 'lb-1')
            manager._refresh_wait_for_idle()
            manager._terminate_replica.assert_not_called()

            manager._lb_in_flight_report = (replica_managers.time.monotonic(), {
                'http://replica': 0
            }, set(), {'http://replica'}, set(), 'lb-1')
            manager._refresh_wait_for_idle()
            manager._terminate_replica.assert_not_called()

            manager._lb_in_flight_report = (replica_managers.time.monotonic(), {
                'http://replica': 0
            }, set(), set(), set(), 'lb-1')
            manager._refresh_wait_for_idle()

        assert not info.status_property.wait_for_idle_before_termination
        manager._terminate_replica.assert_called_once_with(
            1,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=0)

    def test_lb_restart_falls_back_to_bounded_drain_at_deadline(
            self, monkeypatch):
        now = [100.0]
        monkeypatch.setattr(replica_managers.time, 'monotonic', lambda: now[0])
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._is_pool = False
        manager._wait_for_idle_trackers = {}
        manager._lb_in_flight_report = None
        manager._persist_replica = mock.Mock()
        manager._terminate_replica = mock.Mock()
        manager._resolve_drain_cap_seconds = mock.Mock(return_value=600)
        info = types.SimpleNamespace(
            replica_id=1,
            cluster_name='cluster-1',
            url='http://replica',
            status_property=replica_managers.ReplicaStatusProperty(
                sky_launch_status=common_utils.ProcessStatus.SUCCEEDED))

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos_from_ids',
                               return_value={1: info}), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_status_fields',
                               return_value={'cluster-1': ('UP', 1)}):
            manager._defer_scale_down_until_idle(1)
            now[0] = 110.0
            manager._lb_in_flight_report = (110.0, {
                'http://replica': 1
            }, {'http://replica'}, set(), set(), 'lb-old')
            manager._refresh_wait_for_idle()
            now[0] = 120.0
            manager._lb_in_flight_report = (120.0, {}, set(), set(), set(),
                                            'lb-new')
            manager._refresh_wait_for_idle()
            manager._terminate_replica.assert_not_called()

            now[0] = 700.0
            manager._refresh_wait_for_idle()

        assert not info.status_property.wait_for_idle_before_termination
        manager._terminate_replica.assert_called_once_with(
            1,
            sync_down_logs=False,
            replica_drain_delay_seconds=0,
            is_scale_down=True,
            in_flight_drain_cap_seconds=600)
