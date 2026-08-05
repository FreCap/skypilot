"""Cost-aware SkyServe replica replacement safety tests."""
# pylint: disable=protected-access
import threading
import types
from unittest import mock

import pytest
from spot_placer_test_utils import make_location
from spot_placer_test_utils import make_placer

from sky import clouds
from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import replica_managers
from sky.serve import serve_state
from sky.serve import service_spec
from sky.serve import spot_placer
from sky.serve import system_recovery_state
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

    def test_programmatic_true_uses_defaults(self):
        base = service_spec.SkyServiceSpec.from_yaml_str(self._YAML)
        enabled = base.copy(cost_rebalance=True)
        assert enabled.cost_rebalance
        assert enabled.cost_rebalance_min_savings_fraction == pytest.approx(0.3)
        assert enabled.cost_rebalance_max_parallel_replacements == 1
        assert enabled.cost_rebalance_stabilization_seconds == 300

    def test_placer_default_and_explicit_opt_out(self):
        config = self._YAML.replace(
            """    cost_rebalance:
      min_savings_fraction: 0.3
      max_parallel_replacements: 8
      stabilization_seconds: 300
""", '')
        implicit = service_spec.SkyServiceSpec.from_yaml_str(config)
        assert implicit.cost_rebalance
        assert 'cost_rebalance' not in implicit.to_yaml_config(
        )['replica_policy']

        disabled = implicit.copy(cost_rebalance=False)
        assert not disabled.cost_rebalance
        assert disabled.to_yaml_config(
        )['replica_policy']['cost_rebalance'] is False

        explicit_null = service_spec.SkyServiceSpec.from_yaml_str(
            config.replace(
                '    spot_placer: dynamic_fallback\n',
                '    spot_placer: dynamic_fallback\n'
                '    cost_rebalance: null\n'))
        assert explicit_null.cost_rebalance

        legacy_state = dict(implicit.__dict__)
        legacy_state.pop('_cost_rebalance')
        restored = service_spec.SkyServiceSpec.__new__(
            service_spec.SkyServiceSpec)
        restored.__setstate__(legacy_state)
        assert restored.cost_rebalance

        without_placer = service_spec.SkyServiceSpec.from_yaml_str(
            config.replace('    spot_placer: dynamic_fallback\n', ''))
        assert not without_placer.cost_rebalance
        assert not without_placer.copy(cost_rebalance=False).cost_rebalance
        with pytest.raises(ValueError, match='requires spot_placer'):
            without_placer.copy(cost_rebalance=True)


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

    def test_unpriced_candidate_does_not_rebalance(self):
        scaler = _autoscaler()
        placer, _, cheap, replicas = self._fleet(candidate_cost=0.0)
        placer.location2cost[cheap] = float('inf')
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)

        decisions = _decisions(scaler, replicas)

        assert not [
            decision for decision in decisions if decision.operator ==
            autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]

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
        monkeypatch.setattr(autoscalers.time, 'time', lambda: now[0])
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

    def test_stabilization_survives_restart(self, monkeypatch):
        paid = spot_placer.Location(cloud=clouds.AWS(),
                                    region='us-east-1',
                                    zone='us-east-1a',
                                    accelerators={'L4': 1},
                                    use_spot=True,
                                    instance_type='g6.xlarge')
        cheap = spot_placer.Location(cloud=clouds.AWS(),
                                     region='us-west-2',
                                     zone='us-west-2a',
                                     accelerators={'L4': 1},
                                     use_spot=True,
                                     instance_type='g6.xlarge')
        placer = make_placer({paid: 1.0, cheap: 0.5})
        replicas = [_Replica(1, paid, 1.0), _Replica(2, paid, 1.0)]
        now = [100.0]
        monkeypatch.setattr(autoscalers.time, 'time', lambda: now[0])
        spec = _spec(cost_rebalance_stabilization_seconds=300.0)
        scaler = _autoscaler(spec)
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)
        assert not _decisions(scaler, replicas)
        state = scaler.dump_cost_rebalance_state()

        now[0] = 399.0
        restored = _autoscaler(spec)
        restored.load_cost_rebalance_state(state)
        restored.set_spot_placer(placer)
        _report(restored, replicas)
        assert not _decisions(restored, replicas)
        now[0] = 400.0
        assert len([
            decision for decision in _decisions(restored, replicas) if
            decision.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]) == 1

    def test_reserved_fill_keeps_zero_cost_candidates_broker_only(self):
        scaler = _autoscaler(_spec(reserved_capacity_fill=True))
        placer, _, _, replicas = self._fleet(candidate_cost=0.0)
        scaler.set_spot_placer(placer)
        _report(scaler, replicas)

        assert not [
            decision for decision in _decisions(scaler, replicas) if
            decision.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]

        for location in placer.location2cost:
            if placer.location2cost[location] == 0:
                placer.location2cost[location] = 0.5
        assert len([
            decision for decision in _decisions(scaler, replicas) if
            decision.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]) == 1

    def test_demand_decision_resets_stabilization(self, monkeypatch):
        now = [100.0]
        monkeypatch.setattr(autoscalers.time, 'time', lambda: now[0])
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
        monkeypatch.setattr(autoscalers.time, 'time', lambda: now[0])
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
        manager._persist_new_replica = mock.Mock()
        override = cheap.to_dict()
        override[constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY] = 7

        with mock.patch.object(replica_managers, '_should_use_spot'), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.spot_placer.Location,
                               'from_resources_override',
                               return_value=cheap), \
             mock.patch.object(
                 replica_managers,
                 '_ReplicaLaunchThread') as launch_thread_cls:
            assert manager._launch_replica(8, override)

        manager._persist_new_replica.assert_called_once_with(8, mock.ANY)
        info = manager._persist_new_replica.call_args.args[1]
        assert info.cost_rebalance_for_replica_id == 7
        assert constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY not in info.resources_override
        assert info.resources_override['region'] == 'research'
        assert info.is_spot is False

        launch_thread_cls.assert_called_once()
        construction = launch_thread_cls.call_args
        assert construction.args == ()
        assert set(construction.kwargs) == {
            'target',
            'replica_id',
            'completion_queue',
            'completion_event',
            'args',
            'kwargs',
        }
        runtime = manager._legacy_mutation_runtime_state()
        assert (
            construction.kwargs['target']
            is replica_managers.launch_cluster_with_frozen_controller_config)
        assert construction.kwargs['replica_id'] == 8
        assert (construction.kwargs['completion_queue']
                is runtime.launch_completion_queue)
        assert (construction.kwargs['completion_event']
                is runtime.launch_completion_event)
        assert construction.kwargs['args'] == (
            8,
            manager.yaml_content,
            info.cluster_name,
            mock.ANY,
            runtime.replica_to_request_id,
            runtime.replica_to_launch_cancelled,
            info.resources_override,
            False,
        )
        launch_kwargs = construction.kwargs['kwargs']
        assert set(launch_kwargs) == {
            'availability_max_retry',
            'exact_resources_override',
            'pre_launch_guard',
            'cloud_launch_guard',
            'supersession_guard',
            'continue_guard',
            'cleanup_continue_guard',
            'launch_fence',
            'service_spec',
            'service_name',
            'workspace',
            'frozen_controller_config',
            'frozen_controller_config_path',
        }
        assert launch_kwargs['availability_max_retry'] == 1
        assert launch_kwargs['exact_resources_override'] is True
        pre_launch_guard = launch_kwargs['pre_launch_guard']
        assert pre_launch_guard.__self__ is manager
        assert (pre_launch_guard.__func__
                is type(manager)._service_is_launch_authorized)
        assert launch_kwargs['cloud_launch_guard']() == (True, 'authorized')
        assert launch_kwargs['supersession_guard']() == (True, 'authorized')
        continue_guard = launch_kwargs['continue_guard']
        assert continue_guard.__self__ is manager
        assert (continue_guard.__func__
                is type(manager)._launch_owner_watchdog_allows_continue)
        cleanup_continue_guard = launch_kwargs['cleanup_continue_guard']
        assert cleanup_continue_guard.__self__ is manager
        assert (cleanup_continue_guard.__func__
                is type(manager)._service_is_cleanup_authorized)
        assert launch_kwargs['launch_fence'] is None
        assert launch_kwargs['service_spec'] is None
        assert launch_kwargs['service_name'] == 'svc'
        assert isinstance(launch_kwargs['workspace'], str)
        assert launch_kwargs['frozen_controller_config'] is not None
        launch_thread = launch_thread_cls.return_value
        assert manager._launch_thread_pool[8] is launch_thread
        launch_thread.start.assert_not_called()

    def test_paid_replacement_acquires_exact_pool_claim(self):
        paid = make_location('paid',
                             accelerators={'L4': 1},
                             use_spot=True,
                             instance_type='g6.xlarge')
        cheap = make_location('cheap',
                              accelerators={'L4': 1},
                              use_spot=True,
                              instance_type='g6.xlarge')
        placer = make_placer({paid: 1.0, cheap: 0.5})
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._service_hash = 'incarnation-a'
        manager._controller_owner = (123, '10.0.0.1')
        manager._resource_scope = None
        manager._spot_placer = placer
        manager.yaml_content = 'resources: {}'
        manager.latest_version = 1
        manager._launch_thread_pool = {}
        manager._replica_to_request_id = {}
        manager._replica_to_launch_cancelled = {}
        manager._persist_replica = mock.Mock()
        budget = replica_managers.paid_capacity.LaunchBudget(
            remaining_by_location={cheap: 1},
            pool_key_by_location={cheap: 'exact-pool'},
            states_by_pool_key={},
            globally_managed=True,
            service_remaining=1)
        override = cheap.to_dict()
        override[constants.COST_REBALANCE_FOR_REPLICA_OVERRIDE_KEY] = 7

        with mock.patch.object(replica_managers, '_should_use_spot'), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.spot_placer.Location,
                               'from_resources_override',
                               return_value=cheap), \
             mock.patch.object(replica_managers.thread_utils, 'SafeThread'), \
             mock.patch.object(
                 replica_managers.paid_capacity,
                 'try_persist_claim',
                 return_value=replica_managers.paid_capacity.ClaimResult.
                 ACQUIRED) as persist_claim:
            assert manager._launch_replica(8,
                                           override,
                                           existing_replica_infos=[],
                                           paid_location_launch_budget=budget)

        assert persist_claim.call_args.kwargs['location'] == cheap
        claimed_info = persist_claim.call_args.kwargs['replica_info']
        assert claimed_info.cost_rebalance_for_replica_id == 7
        assert budget.remaining_by_location[cheap] == 0
        assert budget.service_remaining == 0
        manager._persist_replica.assert_not_called()

    def test_recovered_paid_replacement_reuses_existing_claim(self):
        paid = make_location('paid',
                             accelerators={'L4': 1},
                             use_spot=True,
                             instance_type='g6.xlarge')
        cheap = make_location('cheap',
                              accelerators={'L4': 1},
                              use_spot=True,
                              instance_type='g6.xlarge')
        placer = make_placer({paid: 1.0, cheap: 0.5})
        manager = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        manager._service_name = 'svc'
        manager._resource_scope = None
        manager._spot_placer = placer
        manager.latest_version = 1
        manager._launch_thread_pool = {}
        manager._replica_to_request_id = {}
        manager._replica_to_launch_cancelled = {}
        manager._persist_replica = mock.Mock()

        with mock.patch.object(replica_managers, '_should_use_spot'), \
             mock.patch.object(replica_managers,
                               '_get_resources_ports',
                               return_value='8080'), \
             mock.patch.object(replica_managers.spot_placer.Location,
                               'from_resources_override',
                               return_value=cheap), \
             mock.patch.object(replica_managers.thread_utils, 'SafeThread'), \
             mock.patch.object(
                 replica_managers.paid_capacity,
                 'build_launch_budget') as build_budget, \
             mock.patch.object(
                 replica_managers.paid_capacity,
                 'try_persist_claim') as persist_claim:
            assert manager._launch_replica(
                8,
                cheap.to_dict(),
                prior_cost_rebalance_for_replica_id=7,
                prior_paid_capacity_pool_key='exact-pool',
                recovering_existing_replica=True,
                prior_version=1,
                prior_yaml_content='resources: {}')

        build_budget.assert_not_called()
        persist_claim.assert_not_called()
        info = manager._persist_replica.call_args.args[1]
        assert info.cost_rebalance_for_replica_id == 7
        assert info.paid_capacity_pool_key == 'exact-pool'
        assert info.location == cheap.to_pickleable()

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
        replica_record_id=(f'00000000-0000-4000-8000-{replica_id:012d}'),
        system_recovery_quarantine=None,
        system_recovery_disposition=(
            system_recovery_state.SystemRecoveryDisposition.ORDINARY),
        candidate_ready_observed_at=None,
        system_recovery=None,
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
            status_property=status_property,
            replica_record_id='00000000-0000-4000-8000-000000000001',
            system_recovery_quarantine=None,
            system_recovery_disposition=(
                system_recovery_state.SystemRecoveryDisposition.ORDINARY),
            candidate_ready_observed_at=None,
            system_recovery=None)

        with mock.patch.object(manager,
                               '_resolve_probe_urls',
                               return_value={1: 'http://replica'}), \
             mock.patch.object(replica_managers.serve_state,
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
        manager._resolve_probe_urls = mock.Mock(
            return_value={1: 'http://replica'})
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
