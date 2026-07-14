"""Cost-aware SkyServe replica replacement safety tests."""
# pylint: disable=protected-access
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

    def handle(self):
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
        cheap = make_location('research',
                              accelerators={'A100': 1},
                              use_spot=False)
        scaler.set_spot_placer(make_placer({paid: 1.0, cheap: 0.0}))
        replicas = [_Replica(i, paid, 1.0) for i in (1, 2, 3)]
        _report(scaler, replicas)
        launches = [
            d for d in _decisions(scaler, replicas)
            if d.operator == autoscalers.AutoscalerDecisionOperator.SCALE_UP
        ]
        assert len(launches) == 2

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
                prior_cost_rebalance_for_replica_id=7)

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
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True):
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
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True):
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
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True):
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
