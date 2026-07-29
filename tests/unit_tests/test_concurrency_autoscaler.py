"""Unit tests for sky.serve.autoscalers.ConcurrencyAutoscaler.

The concurrency autoscaler sizes the fleet by OUTSTANDING WORK (in-flight
+ queued + recently-rejected jobs, reported by the LB as gauges) instead
of request rate. Physical targets pack demand onto per-GPU capacities;
logical targets divide demand by the per-GPU saturation knob and publish GPU
slots. Neither mode shrinks while its demand signal is stale (a rebuilt
controller must not mass-retire a live fleet before the first LB sync).
"""
import math
import threading
# pylint: disable=protected-access
import time
import types
import unittest
from unittest import mock

from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.serve import spot_placer
from sky.utils import common_utils

_SCALE_UP = autoscalers.AutoscalerDecisionOperator.SCALE_UP
_SCALE_DOWN = autoscalers.AutoscalerDecisionOperator.SCALE_DOWN


def _spec(knob=1.0,
          min_replicas=0,
          max_replicas=20,
          upscale_delay_seconds=None,
          downscale_delay_seconds=None,
          replica_unit='physical_backend',
          target_utilization_percentage=100,
          expected_request_duration_seconds=None,
          initial_provision_lead_time_seconds=None,
          adaptive_demand_estimation=None,
          max_scale_up_rate_percentage=None,
          scale_up_rate_min_replicas=None,
          scale_up_rate_period_seconds=None,
          max_scale_down_rate_percentage=100,
          min_replicas_by_accelerator=None,
          num_overprovision=None,
          adaptive_scale_up=None,
          lb_request_queue=None,
          reserved_capacity_fill=False):
    # Default delays resolve to one decision interval -> hysteresis
    # thresholds of 1 tick, so most tests observe target changes on the
    # first post-snap recompute.
    interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
    return types.SimpleNamespace(
        min_replicas=min_replicas,
        min_replicas_by_accelerator=(min_replicas_by_accelerator or {}),
        max_replicas=max_replicas,
        num_overprovision=num_overprovision,
        target_concurrency_per_replica=knob,
        replica_unit=replica_unit,
        target_utilization_percentage=target_utilization_percentage,
        expected_request_duration_seconds=expected_request_duration_seconds,
        initial_provision_lead_time_seconds=(
            initial_provision_lead_time_seconds),
        adaptive_demand_estimation=adaptive_demand_estimation,
        max_scale_up_rate_percentage=max_scale_up_rate_percentage,
        scale_up_rate_min_replicas=scale_up_rate_min_replicas,
        scale_up_rate_period_seconds=scale_up_rate_period_seconds,
        adaptive_scale_up=adaptive_scale_up,
        lb_request_queue=lb_request_queue,
        reserved_capacity_fill=reserved_capacity_fill,
        max_scale_down_rate_percentage=max_scale_down_rate_percentage,
        upscale_delay_seconds=(upscale_delay_seconds if upscale_delay_seconds
                               is not None else interval),
        downscale_delay_seconds=(downscale_delay_seconds
                                 if downscale_delay_seconds is not None else
                                 interval))


def _make_autoscaler(**spec_kwargs):
    return autoscalers.ConcurrencyAutoscaler('svc',
                                             _spec(**spec_kwargs),
                                             version=1)


def _replica(replica_id,
             gpu_count=1,
             card='L4',
             status=serve_state.ReplicaStatus.READY,
             version=1,
             planned_capacity=None,
             reserved_fill=False):
    info = mock.Mock()
    info.replica_id = replica_id
    info.version = version
    info.status = status
    info.is_terminal = status in serve_state.ReplicaStatus.terminal_statuses()
    info.is_ready = status == serve_state.ReplicaStatus.READY
    info.cluster_name = f'cluster-{replica_id}'
    info.planned_capacity = (gpu_count
                             if planned_capacity is None else planned_capacity)
    info.unknown_capacity_replacement = False
    info.reserved_fill = reserved_fill
    info.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    info.status_property.is_scale_down = False
    info.status_property.unrecoverable_failure.return_value = False
    info.resources_override = {'accelerators': {card: gpu_count}}
    info.handle.return_value.launched_resources.accelerators = {card: gpu_count}
    return info


def _report(autoscaler,
            in_flight,
            queue_depth=0,
            rejected=0,
            recent_rejected=None,
            timestamps=(),
            unknown=(),
            unknown_capacity=None,
            observed_slots=None,
            generation=1,
            compatibility_profiles=None,
            queued_profiles=None,
            rejected_profiles=None,
            compatibility_complete=False,
            queue_depth_by_priority=None,
            rejected_by_priority=None,
            recent_rejected_by_priority=None,
            unique_arrivals_60s=None,
            unique_arrivals_300s=None,
            headerless_arrivals_60s=None,
            headerless_arrivals_300s=None,
            arrival_tracking_saturated=False,
            pressure_report_is_floored=False,
            prediction_time_history=None):
    report = {
        'timestamps': list(timestamps),
        'in_flight_by_replica_id': in_flight,
        'queue_depth': queue_depth,
        'rejected_in_window': rejected,
        'unknown_in_flight_replica_ids': list(unknown),
        'observed_slots_by_replica_id': dict(observed_slots or {}),
        'unknown_capacity_replica_ids':
            list(unknown if unknown_capacity is None else unknown_capacity),
        'reconcile_generation': generation,
        'compatibility_profiles': list(compatibility_profiles or []),
        'queued_requests_by_compatibility': list(queued_profiles or []),
        'rejected_requests_by_compatibility': list(rejected_profiles or []),
        'compatibility_demand_complete': compatibility_complete,
    }
    if recent_rejected is not None:
        report['rejected_in_recent_window'] = recent_rejected
    if queue_depth_by_priority is not None:
        report['queue_depth_by_priority'] = queue_depth_by_priority
    if rejected_by_priority is not None:
        report['rejected_in_window_by_priority'] = rejected_by_priority
    if recent_rejected_by_priority is not None:
        report['rejected_in_recent_window_by_priority'] = (
            recent_rejected_by_priority)
    if unique_arrivals_60s is not None:
        report['unique_job_arrivals_60s'] = unique_arrivals_60s
    if unique_arrivals_300s is not None:
        report['unique_job_arrivals_300s'] = unique_arrivals_300s
    if headerless_arrivals_60s is not None:
        report['headerless_arrivals_60s'] = headerless_arrivals_60s
    if headerless_arrivals_300s is not None:
        report['headerless_arrivals_300s'] = headerless_arrivals_300s
    if arrival_tracking_saturated:
        report['offered_arrival_tracking_saturated'] = True
    if pressure_report_is_floored:
        report['pressure_report_is_floored'] = True
    if prediction_time_history is not None:
        report['prediction_time_history'] = prediction_time_history
    autoscaler.collect_request_information(report)


def _decisions(autoscaler, replicas, active_versions=(1,)):
    return autoscaler.generate_scaling_decisions(replicas,
                                                 list(active_versions))


def _scale_downs(decisions):
    return sorted(d.target for d in decisions if d.operator == _SCALE_DOWN)


def _scale_ups(decisions):
    return [d for d in decisions if d.operator == _SCALE_UP]


class TestFromSpecSelection(unittest.TestCase):
    """The concurrency knob selects ConcurrencyAutoscaler (pool first)."""

    def test_concurrency_knob_selects_concurrency_autoscaler(self):
        spec = _spec(knob=2.0)
        spec.pool = False
        spec.use_ondemand_fallback = False
        spec.target_qps_per_replica = None
        autoscaler = autoscalers.Autoscaler.from_spec('svc', spec, version=3)
        self.assertIsInstance(autoscaler, autoscalers.ConcurrencyAutoscaler)
        self.assertEqual(autoscaler.latest_version, 3)

    def test_pool_wins_over_concurrency_knob(self):
        spec = _spec(knob=2.0)
        spec.pool = True
        with mock.patch.object(autoscalers,
                               'QueueLengthAutoscaler') as mock_cls:
            autoscalers.Autoscaler.from_spec('svc', spec, version=1)
        mock_cls.assert_called_once()

    def test_spec_without_knob_attribute_falls_through(self):
        # from_spec must stay robust against spec objects predating the
        # knob (e.g. unpickled from old DB rows): no attribute at all.
        spec = types.SimpleNamespace(min_replicas=1,
                                     max_replicas=2,
                                     num_overprovision=None,
                                     pool=False,
                                     use_ondemand_fallback=False,
                                     target_qps_per_replica=2.0,
                                     upscale_delay_seconds=None,
                                     downscale_delay_seconds=None)
        autoscaler = autoscalers.Autoscaler.from_spec('svc', spec)
        self.assertIsInstance(autoscaler, autoscalers.RequestRateAutoscaler)
        self.assertNotIsInstance(autoscaler, autoscalers.ConcurrencyAutoscaler)

    def test_none_knob_falls_through(self):
        spec = _spec(knob=None)
        spec.pool = False
        spec.use_ondemand_fallback = False
        spec.target_qps_per_replica = 2.0
        autoscaler = autoscalers.Autoscaler.from_spec('svc', spec)
        self.assertIsInstance(autoscaler, autoscalers.RequestRateAutoscaler)


class TestColdPaidCardOrdering(unittest.TestCase):
    """Cold-card ordering reads only the complete centralized catalog."""

    def _order(self, costs):
        placer = mock.Mock()
        placer.known_locations.return_value = list(costs)
        placer.cost_per_hour.side_effect = costs.get
        order = autoscalers._order_cold_paid_cards(['L4', 'A100'], placer,
                                                   lambda _: 1, lambda location:
                                                   (location, 1))
        self.assertEqual(placer.cost_per_hour.call_count, len(costs))
        return order

    def test_uses_catalog_costs_without_provider_resolution(self):
        self.assertEqual(self._order({'L4': 2.0, 'A100': 0.0}), ['L4', 'A100'])

    def test_unpriced_catalog_entry_preserves_service_order(self):
        self.assertEqual(self._order({
            'L4': float('inf'),
            'A100': 1.0
        }), ['L4', 'A100'])

    def test_unpriced_other_location_of_paid_card_preserves_service_order(self):
        a100_paid = object()
        a100_unpriced = object()
        l4_paid = object()
        costs = {
            a100_paid: 4.0,
            a100_unpriced: float('inf'),
            l4_paid: 2.0,
        }
        cards = {
            a100_paid: 'A100',
            a100_unpriced: 'A100',
            l4_paid: 'L4',
        }
        placer = mock.Mock()
        placer.known_locations.return_value = list(costs)
        placer.cost_per_hour.side_effect = costs.get

        order = autoscalers._order_cold_paid_cards(['A100', 'L4'], placer,
                                                   lambda _: 1, lambda location:
                                                   (cards[location], 1))

        self.assertEqual(order, ['A100', 'L4'])
        self.assertEqual(placer.cost_per_hour.call_count, len(costs))


class TestTargetMath(unittest.TestCase):
    """target ~= pack(outstanding onto knob x gpu_count capacities)."""

    def _recompute(self, autoscaler, replicas):
        autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

    def test_per_gpu_capacity_scaling(self):
        # knob=2 per GPU: a 4-GPU replica absorbs 8 concurrent jobs.
        autoscaler = _make_autoscaler(knob=2.0)
        replicas = [_replica(1, gpu_count=4)]
        _report(autoscaler, in_flight={1: 8})
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 1)

    def test_per_gpu_capacity_overflow_adds_replica(self):
        autoscaler = _make_autoscaler(knob=2.0)
        replicas = [_replica(1, gpu_count=4)]
        _report(autoscaler, in_flight={1: 8}, queue_depth=1)
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 2)

    def test_heterogeneous_packing_largest_first(self):
        # Capacities [4, 1]: 5 outstanding fit exactly onto 2 replicas.
        autoscaler = _make_autoscaler(knob=1.0)
        replicas = [_replica(1, gpu_count=4), _replica(2, gpu_count=1)]
        _report(autoscaler, in_flight={1: 4, 2: 1})
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 2)

    def test_heterogeneous_remainder_sized_by_best_live_capacity(self):
        # 6 outstanding onto [4, 1]: remainder 1 sized by the BEST live
        # capacity (4), so ONE more replica -- not one per unit.
        autoscaler = _make_autoscaler(knob=1.0)
        replicas = [_replica(1, gpu_count=4), _replica(2, gpu_count=1)]
        _report(autoscaler, in_flight={1: 4, 2: 1}, queue_depth=1)
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 3)

    def test_reject_and_queue_pressure_counts_without_fleet(self):
        # Empty fleet: remainder sized by knob x 1 so scale-from-zero
        # works; queued + rejected jobs are demand.
        autoscaler = _make_autoscaler(knob=1.0)
        _report(autoscaler, in_flight={}, queue_depth=2, rejected=3)
        self._recompute(autoscaler, [])
        self.assertEqual(autoscaler.target_num_replicas, 5)

    def test_logical_saturation_divides_all_outstanding_work(self):
        autoscaler = _make_autoscaler(knob=2,
                                      replica_unit='logical',
                                      min_replicas=0)
        replicas = [
            _replica(1, planned_capacity=1),
            _replica(2, planned_capacity=1),
        ]
        _report(autoscaler,
                in_flight={
                    1: 1,
                    2: 0
                },
                queue_depth=2,
                rejected=4,
                unknown=(2,))

        self._recompute(autoscaler, replicas)

        # The unknown slot contributes its full two-work retention capacity:
        # (1 in flight + 2 queued + 4 rejected + 2 unknown) / 2 per GPU.
        self.assertEqual(autoscaler.target_num_replicas, 5)

    def test_logical_duration_normalizes_only_rejected_pressure(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=100,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        _report(autoscaler, in_flight={1: 30}, queue_depth=10, rejected=120)

        self._recompute(autoscaler, [_replica(1)])

        # Running and queued work stay current state. Rejections retained for
        # 360 seconds contribute 120 * 30 / 360 = 10 concurrent jobs. At 90%
        # target utilization, ceil(50 / 0.9) = 56 GPUs.
        self.assertEqual(autoscaler._rejected_concurrency, 10)
        self.assertEqual(autoscaler.target_num_replicas, 56)

    def test_duration_normalized_rejections_still_drive_scale_up(self):
        autoscaler = _make_autoscaler(
            knob=1,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        _report(autoscaler, in_flight={}, rejected=36)

        self._recompute(autoscaler, [])

        # 36 retained rejects represent 3 concurrent jobs, not zero pressure.
        self.assertEqual(autoscaler._rejected_concurrency, 3)
        self.assertEqual(autoscaler.target_num_replicas, 4)

    def test_recent_rejection_rate_drives_spiky_scale_up(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=200,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        _report(autoscaler, in_flight={}, rejected=120, recent_rejected=120)

        self._recompute(autoscaler, [])

        # The six-minute retained floor is 10 concurrent jobs, but 120 new
        # rejects in one minute imply 60. The spike-responsive value wins.
        self.assertEqual(autoscaler._rejected_concurrency, 60)
        self.assertEqual(autoscaler.target_num_replicas, 67)

    def test_unknown_async_occupancy_adds_full_capacity_floor(self):
        # Two declared async replicas missed their occupancy probes. Their
        # envelope zeros cannot erase potentially-full work; two additional
        # rejected jobs need two replacement slots on top of that floor.
        autoscaler = _make_autoscaler(knob=1.0)
        replicas = [_replica(1), _replica(2)]
        _report(autoscaler, in_flight={1: 0, 2: 0}, rejected=2, unknown=(1, 2))
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 4)

    def test_unknown_floor_uses_each_versions_multi_gpu_capacity(self):
        autoscaler = _make_autoscaler(knob=1.0)
        autoscaler.update_version(2, _spec(knob=3.0),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        old = _replica(1, gpu_count=2, version=1)  # 1 * 2 = 2
        new = _replica(2, gpu_count=1, version=2)  # 3 * 1 = 3
        _report(autoscaler, in_flight={1: 0, 2: 0}, unknown=(1, 2))
        self.assertEqual(autoscaler._outstanding_work([old, new]), 5)

    def test_logical_unknown_fleet_does_not_create_utilization_scale_up(self):
        # A controller/LB role handoff can conservatively mark every ready
        # replica occupancy-unknown for one report. That is a retention floor,
        # not observed saturation: utilization headroom must not inflate 10
        # existing slots into ceil(10 / 0.9) == 12 phantom demand slots.
        autoscaler = _make_autoscaler(
            knob=1.0,
            max_replicas=20,
            replica_unit='logical',
            target_utilization_percentage=90,
        )
        autoscaler.set_configured_accelerator_shapes({'L4': 1})
        replicas = [_replica(replica_id) for replica_id in range(1, 11)]
        _report(autoscaler,
                in_flight={replica_id: 0 for replica_id in range(1, 11)},
                unknown=range(1, 11),
                compatibility_complete=True)

        decisions = _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler._outstanding_work(replicas), 9)
        self.assertEqual(autoscaler.target_num_replicas, 10)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 10})
        self.assertFalse(any(d.operator == _SCALE_UP for d in decisions))

    def test_logical_unknown_fleet_holds_across_utilization_settings(self):
        # The retention contract must hold for every legal utilization, not
        # only the ones whose capacity divides the fleet exactly in binary
        # floating point. ten 0.9-work floors sum to exactly 9.0, but three
        # 0.7-work floors sum to 2.1 and 2.1 / 0.7 is 3.0000000000000004, so
        # a bare ceil turns an all-unknown handoff report into the phantom
        # scale-up this retention floor exists to prevent.
        for utilization, fleet_size in ((70, 3), (95, 3), (97, 3), (85, 13),
                                        (60, 7), (70, 7)):
            with self.subTest(utilization=utilization, fleet=fleet_size):
                autoscaler = _make_autoscaler(
                    knob=1.0,
                    max_replicas=200,
                    replica_unit='logical',
                    target_utilization_percentage=utilization,
                )
                ids = range(1, fleet_size + 1)
                replicas = [_replica(replica_id) for replica_id in ids]
                _report(autoscaler,
                        in_flight={replica_id: 0 for replica_id in ids},
                        unknown=ids)

                decisions = _decisions(autoscaler, replicas)

                self.assertEqual(autoscaler.target_num_replicas, fleet_size)
                self.assertFalse(any(
                    d.operator == _SCALE_UP for d in decisions))

    def test_logical_unknown_multi_gpu_fleet_holds_materialized_slots(self):
        # The same tail appears once the per-replica width is above one, and
        # it must not inflate a six-slot fleet into a seventh slot.
        autoscaler = _make_autoscaler(
            knob=1.0,
            max_replicas=200,
            replica_unit='logical',
            target_utilization_percentage=70,
        )
        replicas = [
            _replica(replica_id, gpu_count=2) for replica_id in (1, 2, 3)
        ]
        _report(autoscaler, in_flight={1: 0, 2: 0, 3: 0}, unknown=(1, 2, 3))

        decisions = _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler.target_num_replicas, 6)
        self.assertFalse(any(d.operator == _SCALE_UP for d in decisions))

    def test_logical_unknown_floor_keeps_headroom_for_observed_work(self):
        autoscaler = _make_autoscaler(
            knob=1.0,
            max_replicas=20,
            replica_unit='logical',
            target_utilization_percentage=90,
        )
        replicas = [_replica(1), _replica(2)]
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0
                },
                queue_depth=1,
                unknown=(1, 2))

        self._recompute(autoscaler, replicas)

        # Two unknown slots retain themselves at 1.8 work. The one observed
        # queued job still receives normal utilization headroom, producing a
        # four-slot target rather than hiding behind the uncertainty floor.
        self.assertAlmostEqual(autoscaler._outstanding_work(replicas), 2.8)
        self.assertEqual(autoscaler.target_num_replicas, 4)

    def test_logical_unknown_missing_row_uses_adjusted_fallback(self):
        autoscaler = _make_autoscaler(
            knob=1.0,
            max_replicas=20,
            replica_unit='logical',
            target_utilization_percentage=90,
        )
        _report(autoscaler, in_flight={}, unknown=(101,))

        self._recompute(autoscaler, [])

        self.assertEqual(autoscaler._outstanding_work([]), 0.9)
        self.assertEqual(autoscaler.target_num_replicas, 1)

    def test_zero_outstanding_scales_to_min(self):
        autoscaler = _make_autoscaler(knob=1.0, min_replicas=0)
        replicas = [_replica(1), _replica(2)]
        _report(autoscaler, in_flight={1: 0, 2: 0})
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 0)

    def test_target_clipped_to_max_replicas(self):
        autoscaler = _make_autoscaler(knob=1.0, max_replicas=3)
        _report(autoscaler, in_flight={}, queue_depth=100)
        self._recompute(autoscaler, [])
        self.assertEqual(autoscaler.target_num_replicas, 3)

    def test_first_fresh_recompute_snaps_then_hysteresis_gates(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = _make_autoscaler(knob=1.0,
                                      min_replicas=1,
                                      upscale_delay_seconds=2 * interval)
        replicas = [_replica(1)]
        _report(autoscaler, in_flight={1: 1}, queue_depth=2)
        # First recompute with fresh data: snap (no hysteresis wait).
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 3)
        # Subsequent raise is gated by the 2-tick upscale threshold.
        _report(autoscaler, in_flight={1: 1}, queue_depth=4)
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 3)
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 5)

    def test_first_fresh_downscale_honors_delay(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = _make_autoscaler(
            knob=1.0,
            min_replicas=1,
            downscale_delay_seconds=2 * interval,
        )
        replicas = [_replica(i) for i in (1, 2, 3)]
        autoscaler.target_num_replicas = 3
        _report(autoscaler, in_flight={1: 0, 2: 0, 3: 0})

        # The first fresh report consumes the construction/update snap, but a
        # lower target still needs the configured sustained-idle evidence.
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 3)
        self.assertEqual(autoscaler.downscale_counter, 1)
        self._recompute(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 1)

    def test_priority_patience_weights_retained_queue_work(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
            # Opt out of the assumed provisioning lead to exercise pure
            # deadline discounting.
            initial_provision_lead_time_seconds=0,
            lb_request_queue={
                'timeout_seconds': 20,
                'timeout_seconds_by_priority': [{
                    'min_priority': 0,
                    'timeout_seconds': 600,
                }, {
                    'min_priority': 50,
                    'timeout_seconds': 60,
                }],
            },
        )
        _report(autoscaler,
                in_flight={},
                queue_depth=110,
                queue_depth_by_priority={
                    0: 100,
                    50: 10,
                })

        self._recompute(autoscaler, [])

        # 100 * 30/600 + 10 * 30/60 = 10 units of draining work.
        self.assertEqual(autoscaler._weighted_queue_work, 10)
        self.assertEqual(autoscaler.target_num_replicas, 12)

    def test_launch_priority_uses_highest_active_demand(self):
        autoscaler = _make_autoscaler(knob=1, max_replicas=1000)
        _report(autoscaler,
                in_flight={},
                queue_depth=101,
                queue_depth_by_priority={
                    20: 100,
                    50: 1,
                },
                queued_profiles=[{
                    'priority': 20,
                    'compatible_accelerators': ['L4'],
                    'count': 100,
                }, {
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': 1,
                }],
                compatibility_complete=True)

        self.assertEqual(autoscaler.current_launch_priority(), 50)

    def test_launch_priority_defaults_and_clamps(self):
        autoscaler = _make_autoscaler(knob=1)
        self.assertEqual(autoscaler.current_launch_priority(),
                         constants.LB_REQUEST_PRIORITY_MIN)
        autoscaler._queue_depth_by_priority = {1000: 1}
        autoscaler._launch_priority_report_received_at = time.time()
        self.assertEqual(autoscaler.current_launch_priority(),
                         constants.LB_REQUEST_PRIORITY_MAX)

    def test_launch_priority_is_specific_to_compatible_accelerator(self):
        autoscaler = _make_autoscaler(knob=1)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                queued_profiles=[{
                    'priority': 20,
                    'compatible_accelerators': ['L4'],
                    'count': 100,
                }, {
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': 1,
                }],
                compatibility_complete=True)

        assert autoscaler.current_launch_priorities_by_accelerator(
            ['L4', 'A100']) == {
                'L4': 20,
                'A100': 50,
            }
        assert isinstance(
            autoscaler.queued_compatibility_profiles[0]
            ['compatible_accelerators'], tuple)

    def test_launch_priority_missing_compatibility_applies_to_every_card(self):
        autoscaler = _make_autoscaler(knob=1)
        autoscaler._launch_priority_report_received_at = time.time()
        autoscaler.queued_compatibility_profiles = [{
            'priority': 50,
            'count': 1,
        }]

        assert autoscaler.current_launch_priorities_by_accelerator(
            ['A100', 'A100-80GB']) == {
                'A100': 50,
                'A100-80GB': 50,
            }

    def test_launch_priority_does_not_fall_back_across_excluded_card(self):
        autoscaler = _make_autoscaler(knob=1)
        autoscaler._launch_priority_report_received_at = time.time()
        autoscaler._queue_depth_by_priority = {50: 1}
        autoscaler.queued_compatibility_profiles = [{
            'priority': 50,
            'compatible_accelerators': ['A100'],
            'count': 1,
        }]

        assert autoscaler.current_launch_priorities_by_accelerator(['L4']) == {
            'L4': constants.LB_REQUEST_PRIORITY_MIN,
        }

    def test_launch_priority_evidence_expires(self):
        autoscaler = _make_autoscaler(knob=1)
        with mock.patch.object(autoscalers.time, 'time', return_value=100):
            _report(autoscaler,
                    in_flight={},
                    queue_depth=1,
                    queue_depth_by_priority={50: 1},
                    queued_profiles=[{
                        'priority': 50,
                        'compatible_accelerators': ['A100'],
                        'count': 1,
                    }],
                    compatibility_complete=True)
            assert autoscaler.current_launch_priority() == 50
            assert autoscaler.current_launch_priorities_by_accelerator(
                ['A100']) == {
                    'A100': 50,
                }

        expired_at = 101 + autoscaler._staleness_threshold_seconds()
        with mock.patch.object(autoscalers.time,
                               'time',
                               return_value=expired_at):
            assert autoscaler.current_launch_priority() == (
                constants.LB_REQUEST_PRIORITY_MIN)
            assert autoscaler.current_launch_priorities_by_accelerator(
                ['A100']) == {
                    'A100': constants.LB_REQUEST_PRIORITY_MIN,
                }

    def test_default_assumed_lead_removes_deadline_discount(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
            lb_request_queue={
                'timeout_seconds': 20,
                'timeout_seconds_by_priority': [{
                    'min_priority': 0,
                    'timeout_seconds': 600,
                }, {
                    'min_priority': 50,
                    'timeout_seconds': 60,
                }],
            },
        )
        _report(autoscaler,
                in_flight={},
                queue_depth=110,
                queue_depth_by_priority={
                    0: 100,
                    50: 10,
                })

        self._recompute(autoscaler, [])

        # The default assumes a 600s provisioning lead, which consumes the
        # whole 600s timeout budget: every queued request must be planned
        # for now, not discounted against a deadline that new capacity
        # cannot meet.
        self.assertEqual(autoscaler.configured_provision_lead_seconds,
                         constants.AUTOSCALER_DEFAULT_PROVISION_LEAD_SECONDS)
        self.assertEqual(autoscaler._weighted_queue_work, 110)

    def test_provision_lead_shrinks_queue_patience(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            target_utilization_percentage=100,
            expected_request_duration_seconds=30,
            initial_provision_lead_time_seconds=540,
            lb_request_queue={
                'timeout_seconds': 20,
                'timeout_seconds_by_priority': [{
                    'min_priority': 0,
                    'timeout_seconds': 600,
                }, {
                    'min_priority': 50,
                    'timeout_seconds': 60,
                }],
            },
        )
        _report(autoscaler,
                in_flight={},
                queue_depth=110,
                queue_depth_by_priority={
                    0: 100,
                    50: 10,
                })

        self._recompute(autoscaler, [])

        # New capacity starts serving only after the 540-second lead, so
        # the 600-second timeout leaves a 60-second budget: 100 * 30/60.
        # The 60-second timeout's budget floors at the request duration
        # itself: 10 * 30/30.
        self.assertEqual(autoscaler._weighted_queue_work, 60)
        self.assertEqual(autoscaler.target_num_replicas, 60)

    def test_priority_patience_falls_back_to_aggregate_queue(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
            lb_request_queue={
                'timeout_seconds': 20,
                'timeout_seconds_by_priority': [{
                    'min_priority': 0,
                    'timeout_seconds': 600,
                }],
            },
        )
        _report(autoscaler, in_flight={}, queue_depth=110)

        self._recompute(autoscaler, [])

        self.assertEqual(autoscaler._weighted_queue_work, 110)
        self.assertEqual(autoscaler.target_num_replicas, 123)

    def test_partial_priority_map_cannot_erase_ha_aggregate_floor(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            expected_request_duration_seconds=30,
            lb_request_queue={
                'timeout_seconds': 20,
                'timeout_seconds_by_priority': [{
                    'min_priority': 0,
                    'timeout_seconds': 600,
                }],
            },
        )
        _report(autoscaler,
                in_flight={},
                queue_depth=7,
                queue_depth_by_priority={})

        self._recompute(autoscaler, [])

        self.assertEqual(autoscaler._weighted_queue_work, 7)
        self.assertEqual(autoscaler.target_num_replicas, 7)

    def test_deduplicated_arrival_floor_uses_short_and_long_windows(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        _report(autoscaler,
                in_flight={},
                unique_arrivals_60s=120,
                unique_arrivals_300s=300,
                headerless_arrivals_60s=0,
                headerless_arrivals_300s=0)

        self._recompute(autoscaler, [])

        # The one-minute floor is 60 work units and dominates the five-minute
        # floor of 34.5. At 90% target utilization this requires 67 slots.
        self.assertEqual(autoscaler._arrival_floor_target, 67)
        self.assertEqual(autoscaler.target_num_replicas, 67)

        _report(autoscaler,
                in_flight={},
                unique_arrivals_60s=0,
                unique_arrivals_300s=600,
                headerless_arrivals_60s=0,
                headerless_arrivals_300s=0)
        autoscaler._set_target_num_replicas_with_concurrency_logic([])
        # 15% headroom keeps 69 work units for the five-minute burst, or 77
        # slots at 90% target utilization.
        self.assertEqual(autoscaler._arrival_floor_target, 77)
        self.assertEqual(autoscaler.target_num_replicas, 77)


class TestExactAcceleratorCompatibility(unittest.TestCase):
    """Concurrency demand keeps exact-card scheduling and accounting."""

    @staticmethod
    def _profile(priority, cards, count, recent_count=None):
        profile = {
            'priority': priority,
            'compatible_accelerators': cards,
            'count': count,
        }
        if recent_count is not None:
            profile['recent_count'] = recent_count
        return profile

    @classmethod
    def _arrival_profile(cls, priority, cards, count, timestamp=None):
        profile = cls._profile(priority, cards, count)
        profile['timestamp'] = time.time() if timestamp is None else timestamp
        return profile

    @staticmethod
    def _instance_aware_autoscaler():
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        spec = types.SimpleNamespace(
            min_replicas=0,
            min_replicas_by_accelerator={},
            max_replicas=100,
            num_overprovision=None,
            target_qps_per_replica={
                'L4': 1.0,
                'A100': 1.0,
            },
            upscale_delay_seconds=2 * interval,
            downscale_delay_seconds=2 * interval,
        )
        autoscaler = autoscalers.InstanceAwareRequestRateAutoscaler('svc',
                                                                    spec,
                                                                    version=1)
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
        })
        autoscaler.target_num_replicas = 10
        autoscaler.target_num_replicas_by_accelerator = {'L4': 10}
        autoscaler._snap_target_on_next_recompute = False
        return autoscaler

    def test_qps_launch_priority_uses_tuple_backed_queue_and_rejections(self):
        autoscaler = self._instance_aware_autoscaler()
        autoscaler.collect_request_information({
            'timestamps': [],
            'compatibility_profiles': [],
            'queued_requests_by_compatibility': [{
                'priority': 20,
                'compatible_accelerators': ['L4'],
                'count': 100,
            }],
            'rejected_requests_by_compatibility': [{
                'priority': 50,
                'compatible_accelerators': ['A100'],
                'recent_count': 1,
            }],
            'compatibility_demand_complete': True,
        })

        self.assertEqual(
            autoscaler.current_launch_priorities_by_accelerator(['L4', 'A100']),
            {
                'L4': 20,
                'A100': 50,
            })
        self.assertIsInstance(
            autoscaler.queued_compatibility_profiles[0]
            ['compatible_accelerators'], tuple)
        self.assertIsInstance(
            autoscaler.rejected_compatibility_profiles[0]
            ['compatible_accelerators'], tuple)

    def test_logical_exact_card_preserves_production_arrival_floor(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        l4 = _replica(1, gpu_count=40, card='L4', planned_capacity=40)
        _report(
            autoscaler,
            in_flight={1: 40},
            observed_slots={1: 40},
            compatibility_profiles=[
                self._arrival_profile(50, ['L4', 'A100'], 126)
            ],
            compatibility_complete=True,
            unique_arrivals_60s=126,
            unique_arrivals_300s=126,
            headerless_arrivals_60s=0,
            headerless_arrivals_300s=0,
        )

        _decisions(autoscaler, [l4])

        # 126 arrivals/minute at 30 seconds imply 63 concurrent jobs. At 90%
        # utilization that is 70 slots. Exact-card allocation previously
        # replaced this floor with the 45-slot in-flight-only candidate.
        self.assertEqual(autoscaler._arrival_floor_target, 70)
        self.assertEqual(autoscaler.target_num_replicas, 70)
        self.assertEqual(
            sum(autoscaler.target_num_replicas_by_accelerator.values()), 70)

    def test_arrival_gap_preserves_a100_only_compatibility(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        l4 = _replica(1, gpu_count=40, card='L4', planned_capacity=40)
        _report(
            autoscaler,
            in_flight={1: 40},
            observed_slots={1: 40},
            compatibility_profiles=[self._arrival_profile(50, ['A100'], 126)],
            compatibility_complete=True,
            unique_arrivals_60s=126,
            unique_arrivals_300s=126,
            headerless_arrivals_60s=0,
            headerless_arrivals_300s=0,
        )

        _decisions(autoscaler, [l4])

        # The complete accepted-arrival evidence says the current wave is
        # A100-only, so demand attribution stays entirely on A100. The running
        # L4 work remains visible separately as warm retention and actuation
        # does not preempt it.
        self.assertEqual(autoscaler._arrival_floor_target, 70)
        self.assertEqual(autoscaler.target_num_replicas, 70)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 70})
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator,
                         {'L4': 40})

    def test_queued_profile_shapes_never_admitted_arrival_gap(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=3,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=3600,
        )
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'L40S': 1,
            'A100-80GB': 1,
        })
        l4 = _replica(1, card='L4', planned_capacity=1)
        _report(
            autoscaler,
            in_flight={1: 0},
            observed_slots={1: 1},
            queue_depth=1,
            queued_profiles=[self._profile(50, ['L40S', 'A100-80GB'], 1)],
            compatibility_complete=True,
            unique_arrivals_60s=1,
            unique_arrivals_300s=1,
            headerless_arrivals_60s=0,
            headerless_arrivals_300s=0,
        )

        decisions = _decisions(autoscaler, [l4])

        self.assertEqual(autoscaler._arrival_floor_target, 3)
        self.assertEqual(autoscaler.target_num_replicas, 3)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L40S': 3})
        scale_ups = _scale_ups(decisions)
        self.assertEqual(len(scale_ups), 1)
        target = scale_ups[0].target
        self.assertIsInstance(target, autoscalers.LogicalScaleTarget)
        self.assertEqual(dict(target.target_capacity_by_accelerator),
                         {'L40S': 3})

    def _equal_rate_mixed_evidence_autoscaler(self, duration):
        """Ten L4 arrivals and ten queued A100 arrivals in one window."""
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1200,
            replica_unit='logical',
            target_utilization_percentage=100,
            expected_request_duration_seconds=duration,
        )
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100-80GB': 1})
        replicas = [
            _replica(i, card='L4', planned_capacity=1) for i in range(1, 11)
        ]
        _report(
            autoscaler,
            in_flight={i: 1 for i in range(1, 11)},
            observed_slots={i: 1 for i in range(1, 11)},
            queue_depth=10,
            queued_profiles=[self._profile(50, ['A100-80GB'], 10)],
            compatibility_profiles=[self._arrival_profile(50, ['L4'], 10)],
            compatibility_complete=True,
            # Offered arrivals are recorded before admission, so the aggregate
            # contains both the admitted L4 and still-queued A100 requests.
            unique_arrivals_60s=20,
            unique_arrivals_300s=20,
            headerless_arrivals_60s=0,
            headerless_arrivals_300s=0,
        )
        return autoscaler, replicas

    def test_arrival_gap_evidence_uses_offered_arrival_count_units(self):
        autoscaler, _ = self._equal_rate_mixed_evidence_autoscaler(3600)
        shaped = autoscaler._arrival_compatibility_work(1200.0, 20.0)
        work_by_card = {}
        for _, compatible, work in shaped:
            work_by_card[compatible] = work_by_card.get(compatible, 0.0) + work
        l4_work = work_by_card[('L4',)]
        a100_work = work_by_card[('A100-80GB',)]
        self.assertAlmostEqual(l4_work / a100_work, 1.0, places=6)

    def test_equal_offered_rates_preserve_blocked_card_share(self):
        autoscaler, replicas = self._equal_rate_mixed_evidence_autoscaler(3600)
        _decisions(autoscaler, replicas)
        self.assertEqual(autoscaler._arrival_work(), 1200.0)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {
            'L4': 600,
            'A100-80GB': 600,
        })

    def test_duration_does_not_change_equal_rate_card_share(self):
        shares = []
        for duration in (300, 3600):
            autoscaler, replicas = (
                self._equal_rate_mixed_evidence_autoscaler(duration))
            _decisions(autoscaler, replicas)
            target = autoscaler.target_num_replicas_by_accelerator
            total = sum(target.values())
            self.assertGreater(total, 0)
            shares.append(target.get('A100-80GB', 0) / total)
        self.assertEqual(shares, [0.5, 0.5])

    def test_queued_evidence_keeps_priority_off_the_lower_tier(self):
        # The queued gauge carries the waiter's priority into the
        # arrival-gap split. Dropping it inverts the allocation: a
        # low-priority A100 waiter would take half the ceiling away from
        # the high-priority L40S waiter that the gap belongs to.
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=4,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=3600,
        )
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'L40S': 1,
            'A100-80GB': 1,
        })
        l4 = _replica(1, card='L4', planned_capacity=1)
        _report(
            autoscaler,
            in_flight={1: 0},
            observed_slots={1: 1},
            queue_depth=2,
            queued_profiles=[
                self._profile(90, ['L40S'], 1),
                self._profile(10, ['A100-80GB'], 1),
            ],
            compatibility_complete=True,
            unique_arrivals_60s=2,
            unique_arrivals_300s=2,
            headerless_arrivals_60s=0,
            headerless_arrivals_300s=0,
        )

        _decisions(autoscaler, [l4])

        self.assertEqual(autoscaler.target_num_replicas, 4)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L40S': 4})

    def test_queued_evidence_shapes_the_gap_without_growing_it(self):
        # The queued gauge is compatibility/priority evidence only. It
        # must never add magnitude: the shaped work has to sum back to
        # exactly the gap it was handed, however deep the queue is.
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=20,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=3600,
        )
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'L40S': 1})
        autoscaler.compatibility_profiles = [
            self._arrival_profile(50, ['L4'], 1)
        ]
        autoscaler.queued_compatibility_profiles = [{
            'priority': 50,
            'compatible_accelerators': ('L40S',),
            'count': 999,
        }]

        shaped = autoscaler._arrival_compatibility_work(60.0, 10.0)

        self.assertAlmostEqual(sum(work for _, _, work in shaped), 50.0)

    def test_downscale_hold_preserves_exact_card_for_capacity_retry(self):
        autoscaler = _make_autoscaler(
            knob=1,
            min_replicas=0,
            max_replicas=1500,
            replica_unit='logical',
            target_utilization_percentage=100,
            max_scale_up_rate_percentage=20,
            scale_up_rate_min_replicas=1,
            scale_up_rate_period_seconds=60,
            downscale_delay_seconds=900,
        )
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'L40S': 1,
            'A100-80GB': 1,
        })
        profile = self._profile(0, ['L40S', 'A100-80GB'], 1)

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0), \
             mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0):
            _report(autoscaler,
                    in_flight={},
                    queue_depth=1,
                    queued_profiles=[profile],
                    compatibility_complete=True,
                    generation=1)
            first = _decisions(autoscaler, [])

        with mock.patch.object(autoscalers.time, 'time', return_value=161.0), \
             mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=161.0):
            _report(autoscaler,
                    in_flight={},
                    queue_depth=0,
                    queued_profiles=[],
                    compatibility_complete=True,
                    generation=2)
            retry = _decisions(autoscaler, [])

        self.assertEqual(
            dict(_scale_ups(first)[0].target.target_capacity_by_accelerator),
            {'L40S': 1})
        self.assertEqual(autoscaler._raw_target_num_replicas, 0)
        self.assertEqual(autoscaler.target_num_replicas, 1)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L40S': 1})
        self.assertEqual(
            dict(_scale_ups(retry)[0].target.target_capacity_by_accelerator),
            {'L40S': 1})

    def test_smaller_fresh_demand_reassigns_only_nonheld_slots(self):
        autoscaler = _make_autoscaler(
            knob=1,
            min_replicas=0,
            max_replicas=10,
            replica_unit='logical',
            target_utilization_percentage=100,
            downscale_delay_seconds=900,
        )
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'L40S': 1,
        })
        autoscaler.target_num_replicas = 3
        autoscaler.target_num_replicas_by_accelerator = {'L40S': 3}
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[self._profile(0, ['L4'], 1)],
                compatibility_complete=True)

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0), \
             mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0):
            decisions = _decisions(autoscaler, [])

        self.assertEqual(autoscaler._raw_target_num_replicas, 1)
        self.assertEqual(autoscaler.target_num_replicas, 3)
        self.assertEqual(
            dict(
                _scale_ups(decisions)[0].target.target_capacity_by_accelerator),
            {
                'L4': 1,
                'L40S': 2,
            })

    def test_rate_limited_downscale_keeps_remaining_exact_card(self):
        autoscaler = _make_autoscaler(
            knob=1,
            min_replicas=0,
            max_replicas=10,
            replica_unit='logical',
            target_utilization_percentage=100,
            downscale_delay_seconds=20,
            max_scale_down_rate_percentage=33,
        )
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'L40S': 1,
        })
        autoscaler.target_num_replicas = 3
        autoscaler.target_num_replicas_by_accelerator = {'L40S': 3}
        autoscaler._snap_target_on_next_recompute = False
        replicas = [_replica(i, card='L40S') for i in range(1, 4)]
        idle = {replica.replica_id: 0 for replica in replicas}

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0):
            _report(autoscaler,
                    in_flight=idle,
                    queue_depth=0,
                    queued_profiles=[],
                    compatibility_complete=True,
                    generation=1)
            _decisions(autoscaler, replicas)

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=111.0):
            _report(autoscaler,
                    in_flight=idle,
                    queue_depth=0,
                    queued_profiles=[],
                    compatibility_complete=True,
                    generation=2)
            _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler._raw_target_num_replicas, 0)
        self.assertEqual(autoscaler.target_num_replicas, 2)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L40S': 2})

    def test_attributed_work_above_arrivals_adds_no_arrival_gap(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        a100 = _replica(1, gpu_count=80, card='A100', planned_capacity=80)
        _report(
            autoscaler,
            in_flight={1: 80},
            observed_slots={1: 80},
            compatibility_profiles=[self._arrival_profile(50, ['A100'], 126)],
            compatibility_complete=True,
            unique_arrivals_60s=126,
            unique_arrivals_300s=126,
            headerless_arrivals_60s=0,
            headerless_arrivals_300s=0,
        )

        _decisions(autoscaler, [a100])

        # Eighty units of attributed work already exceed the 63-unit arrival
        # estimate, so there is no arrival gap. Accepted-arrival compatibility
        # keeps the demand map on A100 while warm retention records the actual
        # occupied A100 slots independently.
        self.assertEqual(autoscaler.target_num_replicas, 89)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 89})
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator,
                         {'A100': 80})

    def test_retained_arrival_floor_uses_300_second_profiles(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(
            autoscaler,
            in_flight={},
            compatibility_profiles=[
                self._arrival_profile(50, ['A100'],
                                      600,
                                      timestamp=time.time() - 120)
            ],
            compatibility_complete=True,
            unique_arrivals_60s=0,
            unique_arrivals_300s=600,
            headerless_arrivals_60s=0,
            headerless_arrivals_300s=0,
        )
        # A subsequent complete report exercises collection-time pruning. The
        # 120-second profile must survive for the retained arrival window.
        _report(autoscaler,
                in_flight={},
                compatibility_profiles=[],
                compatibility_complete=True,
                unique_arrivals_60s=0,
                unique_arrivals_300s=600,
                headerless_arrivals_60s=0,
                headerless_arrivals_300s=0,
                generation=2)

        _decisions(autoscaler, [])

        self.assertEqual(autoscaler._arrival_floor_target, 77)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 77})

    def test_unknown_arrival_compatibility_holds_without_guessing_card(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1000,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                compatibility_profiles=[],
                compatibility_complete=True,
                unique_arrivals_60s=126,
                unique_arrivals_300s=126,
                headerless_arrivals_60s=0,
                headerless_arrivals_300s=0)

        decisions = _decisions(autoscaler, [])

        self.assertEqual(autoscaler.target_num_replicas, 70)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {})
        self.assertEqual(_scale_ups(decisions), [])

    def test_arrival_floor_respects_max_and_wave_ceiling(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=50,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(
            autoscaler,
            in_flight={},
            compatibility_profiles=[self._arrival_profile(50, ['A100'], 1000)],
            compatibility_complete=True,
            unique_arrivals_60s=1000,
            unique_arrivals_300s=1000,
            headerless_arrivals_60s=0,
            headerless_arrivals_300s=0,
        )

        candidate, complete = (
            autoscaler._calculate_concurrency_target_by_accelerator(
                [], target_ceiling=20))
        _decisions(autoscaler, [])

        self.assertTrue(complete)
        self.assertEqual(candidate, {'A100': 20})
        self.assertEqual(autoscaler._arrival_floor_target, 50)
        self.assertEqual(autoscaler.target_num_replicas, 50)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 50})

    def test_stale_arrival_profiles_cannot_retarget_unbacked_card(self):
        autoscaler = _make_autoscaler(
            knob=1,
            max_replicas=1,
            replica_unit='logical',
            expected_request_duration_seconds=60,
        )
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        l4 = _replica(1, card='L4', planned_capacity=1)
        now = 1000.0
        with mock.patch.object(autoscalers.time, 'time', return_value=now), \
                mock.patch.object(
                    autoscaler,
                    '_cold_paid_card_order',
                    return_value=['L4', 'A100']):
            _report(
                autoscaler,
                in_flight={1: 0},
                observed_slots={1: 1},
                compatibility_profiles=[
                    self._arrival_profile(50, ['L4'], 100, timestamp=now - 250),
                    self._arrival_profile(20, ['A100'], 1, timestamp=now),
                ],
                compatibility_complete=True,
                unique_arrivals_60s=0,
                unique_arrivals_300s=1,
                headerless_arrivals_60s=0,
                headerless_arrivals_300s=0,
            )
            _decisions(autoscaler, [l4])
            self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                             {'L4': 1})

            # The report is stale after 70 seconds. The older L4 arrival has
            # also fallen outside the 300-second retained evidence window,
            # leaving only A100 evidence. Mark the adopted L4 supply unbacked:
            # actuation may replace that safe adopted L4 target, but must not
            # reshape it to A100 from stale arrival evidence.
            l4.status_property.preempted = True
            with mock.patch.object(autoscalers.time,
                                   'time',
                                   return_value=now + 70):
                decisions = _decisions(autoscaler, [l4])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})
        self.assertIsNotNone(autoscaler.logical_target_state)
        assert autoscaler.logical_target_state is not None
        self.assertEqual(autoscaler.logical_target_state[3], (('L4', 1),))
        for decision in _scale_ups(decisions):
            target = decision.target
            self.assertIsInstance(target, autoscalers.LogicalScaleTarget)
            self.assertEqual(dict(target.target_capacity_by_accelerator),
                             {'L4': 1})

    def test_physical_scale_from_zero_uses_exact_override(self):
        autoscaler = _make_autoscaler(max_replicas=2)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[self._profile(50, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, [])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})
        self.assertEqual([decision.target for decision in decisions], [{
            'accelerators': {
                'A100': 1
            }
        }])
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator, {})
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator,
                         {'A100': 1})

    def test_physical_inflight_overflow_cold_starts_cheapest_card(self):
        autoscaler = _make_autoscaler(max_replicas=4)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        a100 = _replica(1, card='A100')
        _report(autoscaler,
                in_flight={1: 2},
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, [a100])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {
            'A100': 1,
            'L4': 1,
        })
        self.assertEqual([decision.target for decision in decisions], [{
            'accelerators': {
                'L4': 1
            }
        }])
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator,
                         {'A100': 1})
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator,
                         {'L4': 1})

    def test_production_shaped_overflow_adds_only_one_l4(self):
        autoscaler = _make_autoscaler(max_replicas=200)
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
            'A100-80GB': 1,
        })
        replicas = (
            [_replica(replica_id, card='A100') for replica_id in range(1, 122)
            ] + [
                _replica(replica_id, card='A100-80GB')
                for replica_id in range(122, 127)
            ] +
            [_replica(replica_id, card='L4') for replica_id in range(127, 142)])
        in_flight = {replica.replica_id: 1 for replica in replicas}
        # Match the incident shape: 126 A100, 2 A100-80GB, and 14 L4 work
        # units against 121, 5, and 15 materialized slots respectively. The
        # aggregate fleet is short only one slot.
        for replica_id in range(1, 6):
            in_flight[replica_id] += 1
        for replica_id in range(122, 125):
            in_flight[replica_id] = 0
        in_flight[127] = 0
        _report(autoscaler,
                in_flight=in_flight,
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {
            'A100': 121,
            'A100-80GB': 2,
            'L4': 19,
        })
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator, {
            'A100': 121,
            'A100-80GB': 2,
            'L4': 14,
        })
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator,
                         {'L4': 1})
        self.assertEqual(
            autoscaler.info()['cold_launch_authority_by_accelerator'],
            {'L4': 1})
        scale_ups = _scale_ups(decisions)
        self.assertEqual(len(scale_ups), 1)
        self.assertEqual(scale_ups[0].target, {'accelerators': {'L4': 1}})
        self.assertEqual(_scale_downs(decisions), [])

    def test_explicit_constrained_queue_still_cold_starts_exact_card(self):
        autoscaler = _make_autoscaler(max_replicas=2)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        a100 = _replica(1, card='A100')
        _report(autoscaler,
                in_flight={1: 2},
                queue_depth=1,
                queued_profiles=[self._profile(50, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, [a100])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 2})
        # The exact-card queue entry has priority 50. It consumes the only
        # remaining target slot before flexible in-flight overflow, whose
        # synthetic compatibility profile deliberately has priority 0.
        self.assertEqual([decision.target for decision in decisions], [{
            'accelerators': {
                'A100': 1
            }
        }])
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator,
                         {'A100': 1})
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator,
                         {'A100': 1})

    def test_logical_unknown_inflight_overflow_preserves_total_work(self):
        autoscaler = _make_autoscaler(max_replicas=5, replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        a100 = _replica(1, card='A100', planned_capacity=1)
        _report(autoscaler,
                in_flight={1: 2},
                unknown=(1,),
                observed_slots={1: 1},
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)

        fixed, overflow, complete = (
            autoscaler._fixed_concurrency_work_by_accelerator([a100]))
        decisions = _decisions(autoscaler, [a100])

        self.assertTrue(complete)
        self.assertEqual(fixed, {'A100': 1.0})
        self.assertEqual(overflow, 2.0)
        self.assertEqual(sum(fixed.values()) + overflow, 3.0)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 3})
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator,
                         {'A100': 1})
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator,
                         {'L4': 2})
        self.assertEqual(len(decisions), 1)
        self.assertIsInstance(decisions[0].target,
                              autoscalers.LogicalScaleTarget)
        self.assertEqual(decisions[0].target.target_capacity, 3)
        self.assertEqual(
            dict(decisions[0].target.target_capacity_by_accelerator), {
                'A100': 1,
                'L4': 2,
            })

    def test_logical_compatible_inflight_demand_stays_on_cheapest_card(self):
        autoscaler = _make_autoscaler(max_replicas=2, replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        a100 = _replica(1, card='A100', planned_capacity=1)
        _report(
            autoscaler,
            in_flight={1: 1},
            observed_slots={1: 1},
            compatibility_profiles=[
                self._arrival_profile(50, ['L4', 'A100'], 1)
            ],
            compatibility_complete=True,
        )

        decisions = _decisions(autoscaler, [a100])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator,
                         {'A100': 1})
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator, {})
        self.assertEqual(decisions, [])

    def test_logical_idle_floor_demand_stays_on_cheapest_card(self):
        autoscaler = _make_autoscaler(min_replicas=6,
                                      max_replicas=200,
                                      replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
            'A100-80GB': 1,
        })
        replicas = [
            _replica(replica_id, card='A100', planned_capacity=1)
            for replica_id in range(1, 56)
        ] + [
            _replica(replica_id, card='A100-80GB', planned_capacity=1)
            for replica_id in range(56, 78)
        ] + [
            _replica(replica_id, card='L4', planned_capacity=1)
            for replica_id in range(78, 201)
        ]
        _report(autoscaler,
                in_flight={info.replica_id: 0 for info in replicas},
                observed_slots={info.replica_id: 1 for info in replicas},
                compatibility_complete=True)

        _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler.target_num_replicas, 6)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 6})
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator, {})
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator, {})

    def test_physical_zero_demand_retires_last_exact_card(self):
        autoscaler = _make_autoscaler(max_replicas=2)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['L4'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)
        _decisions(autoscaler, [])

        l4 = _replica(1, card='L4')
        _report(autoscaler,
                in_flight={1: 0},
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)
        decisions = _decisions(autoscaler, [l4])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {})
        self.assertEqual(_scale_downs(decisions), [1])

    def test_logical_zero_demand_retires_last_exact_card(self):
        autoscaler = _make_autoscaler(max_replicas=2, replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['L4'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)
        _decisions(autoscaler, [])

        l4 = _replica(1, card='L4', planned_capacity=1)
        _report(autoscaler,
                in_flight={1: 0},
                observed_slots={1: 1},
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)
        decisions = _decisions(autoscaler, [l4])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {})
        downs = [
            decision.target
            for decision in decisions
            if decision.operator == _SCALE_DOWN
        ]
        self.assertEqual(len(downs), 1)
        self.assertIsInstance(downs[0], autoscalers.LogicalScaleDownTarget)
        self.assertEqual(downs[0].replica_id, 1)
        self.assertEqual(downs[0].target_capacity_by_accelerator, ())
        self.assertEqual(downs[0].accelerator_shapes, (('L4', 1), ('A100', 1)))

    def test_physical_card_migration_drains_zero_target_card_when_ready(self):
        autoscaler = _make_autoscaler(max_replicas=2)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['L4'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)
        _decisions(autoscaler, [])

        l4 = _replica(1, card='L4')
        _report(autoscaler,
                in_flight={1: 0},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)
        scale_up = _decisions(autoscaler, [l4])
        self.assertEqual([decision.target for decision in scale_up], [{
            'accelerators': {
                'A100': 1
            }
        }])

        a100 = _replica(2, card='A100')
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0
                },
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=3)
        scale_down = _decisions(autoscaler, [l4, a100])
        self.assertEqual(_scale_downs(scale_down), [1])

    def test_reserved_fill_stays_independent_then_replaces_paid_capacity(self):
        autoscaler = _make_autoscaler(max_replicas=10,
                                      reserved_capacity_fill=True)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        now = time.time()
        reserved_key = {
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
        autoscaler.set_free_reserved_slots_by_accelerator({'A100': 2})
        for _ in range(2):
            autoscaler.collect_reserved_capacity(2, [reserved_key], now)

        paid = [_replica(replica_id, card='L4') for replica_id in range(1, 6)]
        for info in paid:
            info.is_zero_cost = False
            info.reserved_fill = False
            info.created_at = now - 10
            info.get_spot_location.return_value = None
        _report(autoscaler,
                in_flight={info.replica_id: 1 for info in paid},
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)

        first = _decisions(autoscaler, paid)
        fill_ups = [
            decision for decision in first if decision.operator == _SCALE_UP and
            isinstance(decision.target, dict) and
            decision.target.get(constants.RESERVED_CAPACITY_FILL_OVERRIDE_KEY)
        ]

        self.assertEqual(autoscaler.target_num_replicas, 5)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 5})
        self.assertEqual(len(fill_ups), 2)

        reserved = [_replica(replica_id, card='A100') for replica_id in (6, 7)]
        for info in reserved:
            info.is_zero_cost = True
            info.reserved_fill = True
            info.created_at = now - 10
            info.get_spot_location.return_value = (
                spot_placer.Location.from_pickleable(reserved_key))
            info.handle.return_value.launched_resources.get_cost.return_value = 0
        autoscaler.collect_reserved_capacity(0, [reserved_key], now + 1)
        autoscaler.set_free_reserved_slots_by_accelerator({})
        _report(autoscaler,
                in_flight={info.replica_id: 0 for info in [*paid, *reserved]},
                queue_depth=5,
                queued_profiles=[self._profile(50, ['L4', 'A100'], 5)],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)

        second = _decisions(autoscaler, [*paid, *reserved])
        scale_downs = _scale_downs(second)

        self.assertEqual(autoscaler.target_num_replicas, 5)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 5})
        self.assertEqual(len(scale_downs), 2)
        self.assertTrue(set(scale_downs).issubset({1, 2, 3, 4, 5}))

    def test_logical_rolling_update_uses_old_exact_card_evidence(self):
        autoscaler = _make_autoscaler(knob=1,
                                      min_replicas=1,
                                      max_replicas=64,
                                      replica_unit='logical',
                                      reserved_capacity_fill=True,
                                      max_scale_up_rate_percentage=20,
                                      scale_up_rate_min_replicas=10,
                                      scale_up_rate_period_seconds=60)
        catalog = {
            'L4': 1,
            'A100': 1,
            'A100-80GB': 1,
        }
        autoscaler.set_configured_accelerator_shapes(catalog)
        now = time.time()
        old = [_replica(i, card='L4', version=1) for i in range(1, 58)]
        for info in old:
            info.created_at = now - 10
            info.get_spot_location.return_value = None
        in_flight = {info.replica_id: 1 for info in old}
        observed_slots = {info.replica_id: 1 for info in old}
        _report(autoscaler,
                in_flight=in_flight,
                observed_slots=observed_slots,
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)
        self.assertEqual(_decisions(autoscaler, old), [])
        self.assertEqual(autoscaler.target_num_replicas, 57)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 57})

        autoscaler.update_version_and_accelerator_shapes(
            2,
            _spec(knob=1,
                  min_replicas=1,
                  max_replicas=64,
                  replica_unit='logical',
                  reserved_capacity_fill=True,
                  max_scale_up_rate_percentage=20,
                  scale_up_rate_min_replicas=10,
                  scale_up_rate_period_seconds=60),
            serve_utils.UpdateMode.ROLLING, catalog)
        reserved_keys = [{
            'cloud': 'Kubernetes',
            'region': 'research-ctx',
            'zone': None,
            'accelerators': {
                card: 1
            },
            'use_spot': False,
            'image_id': None,
            'disk_tier': None,
        } for card in ('A100', 'A100-80GB')]
        autoscaler.set_free_reserved_slots_by_accelerator({'A100': 7})
        for _ in range(2):
            autoscaler.collect_reserved_capacity(7, reserved_keys, now, grant=7)
        _report(autoscaler,
                in_flight=in_flight,
                observed_slots=observed_slots,
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)

        decisions = _decisions(autoscaler, old, active_versions=(1,))

        self.assertEqual(_scale_downs(decisions), [])
        scale_ups = _scale_ups(decisions)
        self.assertEqual(len(scale_ups), 1)
        # The aggregate target is no longer pinned below the old fleet it is
        # replacing: 57 slots of live work keep the demand target at 57
        # across the version boundary. Replacement pacing is unchanged and
        # still owned by launch_budget, which stays at one wave minimum.
        self.assertEqual(
            scale_ups[0].target,
            autoscalers.LogicalScaleTarget(
                version=2,
                reconcile_generation=2,
                target_capacity=57,
                launch_budget=10,
                target_capacity_by_accelerator=(('L4', 57),),
                accelerator_shapes=(('L4', 1), ('A100', 1), ('A100-80GB', 1)),
                launch_priority_by_accelerator=(('L4', 0),)))
        self.assertEqual(autoscaler.info()['fill_target'], 7)

    def test_free_reserved_slot_cannot_back_paid_rollout_authority(self):
        autoscaler = _make_autoscaler(max_replicas=20, replica_unit='logical')
        autoscaler.latest_version = 2
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
        })
        autoscaler.target_num_replicas = 10
        autoscaler.target_num_replicas_by_accelerator = {'L4': 10}
        autoscaler._snap_target_on_next_recompute = False
        autoscaler.set_free_reserved_slots_by_accelerator({'A100': 5})

        old_a100 = [
            _replica(replica_id, card='A100', version=1)
            for replica_id in range(1, 6)
        ]
        latest_l4 = [
            _replica(replica_id,
                     card='L4',
                     version=2,
                     status=serve_state.ReplicaStatus.PROVISIONING)
            for replica_id in range(11, 16)
        ]
        replicas = [*old_a100, *latest_l4]
        _report(autoscaler,
                in_flight={
                    replica.replica_id: int(replica.version == 1)
                    for replica in replicas
                },
                observed_slots={replica.replica_id: 1 for replica in old_a100},
                compatibility_complete=True)

        decisions = autoscaler._generate_logical_scaling_decisions(
            replicas, latest_l4)

        self.assertEqual(autoscaler.warm_retention_target_by_accelerator,
                         {'A100': 5})
        self.assertEqual(autoscaler._logical_actuation_target_by_accelerator,
                         {'L4': 10})
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator,
                         {'L4': 5})
        target = _scale_ups(decisions)[0].target
        self.assertIsInstance(target, autoscalers.LogicalScaleTarget)
        self.assertEqual(dict(target.target_capacity_by_accelerator),
                         {'L4': 10})

    def test_rollout_preserves_exact_adopted_card(self):
        autoscaler = _make_autoscaler(max_replicas=20, replica_unit='logical')
        autoscaler.latest_version = 2
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
        })
        autoscaler.target_num_replicas = 10
        autoscaler.target_num_replicas_by_accelerator = {'A100': 10}
        autoscaler._snap_target_on_next_recompute = False

        old_a100 = [
            _replica(replica_id, card='A100', version=1)
            for replica_id in range(1, 6)
        ]
        latest_l4 = [
            _replica(replica_id,
                     card='L4',
                     version=2,
                     status=serve_state.ReplicaStatus.PROVISIONING)
            for replica_id in range(11, 16)
        ]
        replicas = [*old_a100, *latest_l4]
        _report(autoscaler,
                in_flight={replica.replica_id: 1 for replica in old_a100},
                observed_slots={replica.replica_id: 1 for replica in old_a100},
                compatibility_complete=True)

        target, complete = autoscaler._actuation_target_by_accelerator(replicas)

        self.assertTrue(complete)
        self.assertEqual(target, {'A100': 10})

    @staticmethod
    def _reserved_fill_shelter_inputs(autoscaler, grant=None):
        now = time.time()
        reserved_keys = [{
            'cloud': 'Kubernetes',
            'region': 'research-ctx',
            'zone': None,
            'accelerators': {
                card: 1
            },
            'use_spot': False,
            'image_id': None,
            'disk_tier': None,
        } for card in ('A100', 'A100-80GB')]
        autoscaler.collect_reserved_capacity(0, reserved_keys, now, grant=grant)

        paid = [_replica(replica_id, card='L4') for replica_id in (1, 2)]
        reserved = [
            *[_replica(replica_id, card='A100') for replica_id in (3, 4, 5)],
            *[
                _replica(replica_id, card='A100-80GB')
                for replica_id in (6, 7, 8)
            ],
        ]
        location_by_card = {
            card: spot_placer.Location.from_pickleable(key)
            for card, key in zip(('A100', 'A100-80GB'), reserved_keys)
        }
        for info in paid:
            info.created_at = now - 10
            info.get_spot_location.return_value = None
        for info in reserved:
            card = next(iter(info.resources_override['accelerators']))
            info.created_at = now - 10
            info.reserved_fill = True
            info.get_spot_location.return_value = location_by_card[card]
        ordinary = [
            autoscalers.AutoscalerDecision(_SCALE_DOWN, replica_id)
            for replica_id in (4, 5, 6, 7, 8)
        ]
        return [*paid, *reserved], ordinary

    def test_reserved_fill_shelter_ignores_demand_on_other_cards(self):
        autoscaler = _make_autoscaler(max_replicas=20,
                                      reserved_capacity_fill=True)
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
            'A100-80GB': 1,
        })
        autoscaler.target_num_replicas = 3
        autoscaler.target_num_replicas_by_accelerator = {
            'L4': 2,
            'A100': 1,
            'A100-80GB': 0,
        }
        autoscaler._compatibility_demand_complete = True
        replicas, ordinary = self._reserved_fill_shelter_inputs(autoscaler)
        decisions = autoscaler._apply_reserved_capacity_fill(replicas, ordinary)

        # Fill owns all six A100-family holdings. Only one of them overlaps
        # A100 demand; L4 demand cannot consume the other five units of
        # A100-family shelter. The legacy aggregate subtraction (6 - 3)
        # would incorrectly drain two reserved replicas and relaunch them.
        self.assertEqual(_scale_downs(decisions), [])

    def test_lower_fill_grant_shelters_existing_cards_before_free_supply(self):
        autoscaler = _make_autoscaler(max_replicas=20,
                                      reserved_capacity_fill=True)
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
            'A100-80GB': 1,
        })
        autoscaler.target_num_replicas = 3
        autoscaler.target_num_replicas_by_accelerator = {
            'L4': 2,
            'A100': 1,
            'A100-80GB': 0,
        }
        autoscaler._compatibility_demand_complete = True
        replicas, ordinary = self._reserved_fill_shelter_inputs(autoscaler,
                                                                grant=4)
        decisions = autoscaler._apply_reserved_capacity_fill(replicas, ordinary)

        # The reduced grant retains the three existing A100s and one existing
        # A100-80GB. A100 demand overlaps one retained A100, so the shelter is
        # two A100s plus one A100-80GB. Exactly two A100-80GB victims drain.
        self.assertEqual(_scale_downs(decisions), [6, 7])

    def test_incomplete_exact_card_target_uses_aggregate_fill_shelter(self):
        autoscaler = _make_autoscaler(max_replicas=20,
                                      reserved_capacity_fill=True)
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
            'A100-80GB': 1,
        })
        autoscaler.target_num_replicas = 3
        autoscaler.target_num_replicas_by_accelerator = {}
        replicas, ordinary = self._reserved_fill_shelter_inputs(autoscaler,
                                                                grant=4)

        decisions = autoscaler._apply_reserved_capacity_fill(replicas, ordinary)

        # The aggregate fallback shelters only fill_target - demand_target = 1
        # victim, so the fleet converges to the broker's grant ceiling of 4.
        self.assertEqual(_scale_downs(decisions), [4, 5, 6, 7])

    def test_unattributed_overprovision_uses_aggregate_fill_shelter(self):
        autoscaler = _make_autoscaler(max_replicas=20,
                                      num_overprovision=1,
                                      reserved_capacity_fill=True)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        autoscaler.target_num_replicas = 3
        autoscaler.target_num_replicas_by_accelerator = {
            'L4': 2,
            'A100': 1,
        }
        autoscaler._compatibility_demand_complete = True

        self.assertEqual(autoscaler.get_final_target_num_replicas(), 4)
        self.assertIsNone(autoscaler._exact_card_fill_shelter([], 5))

    def test_num_overprovision_keeps_exact_card_scale_up_shaped(self):
        autoscaler = _make_autoscaler(max_replicas=2, num_overprovision=1)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['L4'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, [])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})
        self.assertEqual([decision.target for decision in decisions], [{
            'accelerators': {
                'L4': 1
            }
        }, {
            'accelerators': {
                'L4': 1
            }
        }])

    def test_disabling_exact_card_catalog_clears_compatibility_state(self):
        autoscaler = _make_autoscaler(max_replicas=2)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)
        shaped = _decisions(autoscaler, [])
        self.assertEqual(shaped[0].target, {'accelerators': {'A100': 1}})

        autoscaler.set_configured_accelerator_shapes({})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)
        aggregate = _decisions(autoscaler, [])

        self.assertFalse(autoscaler._compatibility_demand_complete)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {})
        self.assertEqual([decision.target for decision in aggregate], [None])

    def test_logical_same_total_card_migration_obeys_wave_limit(self):
        autoscaler = _make_autoscaler(max_replicas=10, replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        replicas = [_replica(i, card='L4') for i in range(1, 11)]
        occupancy = {info.replica_id: 0 for info in replicas}
        slots = {info.replica_id: 1 for info in replicas}
        _report(autoscaler,
                in_flight=occupancy,
                observed_slots=slots,
                queue_depth=10,
                queued_profiles=[self._profile(20, ['L4'], 10)],
                rejected_profiles=[],
                compatibility_complete=True)
        _decisions(autoscaler, replicas)
        autoscaler.max_scale_up_rate_percentage = 50
        autoscaler.scale_up_rate_min_replicas = 1
        autoscaler.scale_up_rate_period_seconds = 60

        _report(autoscaler,
                in_flight=occupancy,
                observed_slots=slots,
                queue_depth=10,
                queued_profiles=[self._profile(20, ['A100'], 10)],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)
        decisions = _decisions(autoscaler, replicas)

        # Demand attribution changes immediately. Only the private actuation
        # target is wave-limited while replacement A100s are cold.
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 10})
        self.assertEqual(len(decisions), 1)
        target = decisions[0].target
        self.assertIsInstance(target, autoscalers.LogicalScaleTarget)
        self.assertEqual(dict(target.target_capacity_by_accelerator), {
            'L4': 5,
            'A100': 5,
        })

        a100_replicas = [_replica(i, card='A100') for i in range(11, 16)]
        transition_replicas = replicas + a100_replicas
        transition_occupancy = {
            info.replica_id: 0 for info in transition_replicas
        }
        transition_slots = {info.replica_id: 1 for info in transition_replicas}
        autoscaler._last_scale_up_wave_at = 100.0
        with mock.patch.object(autoscalers.time, 'time', return_value=120.0):
            _report(autoscaler,
                    in_flight=transition_occupancy,
                    observed_slots=transition_slots,
                    queue_depth=10,
                    queued_profiles=[self._profile(20, ['A100'], 10)],
                    rejected_profiles=[],
                    compatibility_complete=True,
                    generation=3)
            cooldown = _decisions(autoscaler, transition_replicas)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 10})
        self.assertEqual(cooldown, [])

        with mock.patch.object(autoscalers.time, 'time', return_value=161.0):
            _report(autoscaler,
                    in_flight=transition_occupancy,
                    observed_slots=transition_slots,
                    queue_depth=10,
                    queued_profiles=[self._profile(20, ['A100'], 10)],
                    rejected_profiles=[],
                    compatibility_complete=True,
                    generation=4)
            second_wave = _decisions(autoscaler, transition_replicas)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 10})
        self.assertEqual(len(second_wave), 1)
        self.assertEqual(
            dict(second_wave[0].target.target_capacity_by_accelerator),
            {'A100': 10})

    def test_logical_floor_card_migration_obeys_wave_limit(self):
        autoscaler = _make_autoscaler(min_replicas=10,
                                      max_replicas=10,
                                      min_replicas_by_accelerator={'A100': 10},
                                      replica_unit='logical',
                                      max_scale_up_rate_percentage=50,
                                      scale_up_rate_min_replicas=1,
                                      scale_up_rate_period_seconds=60)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        # Model a recovered pre-floor target. The first fresh reconciliation
        # must migrate it in waves, not request the full new floor at once.
        autoscaler.target_num_replicas_by_accelerator = {'L4': 10}
        replicas = [_replica(i, card='L4') for i in range(1, 11)]
        occupancy = {info.replica_id: 0 for info in replicas}
        slots = {info.replica_id: 1 for info in replicas}
        _report(autoscaler,
                in_flight=occupancy,
                observed_slots=slots,
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 10})
        self.assertEqual(len(decisions), 1)
        self.assertEqual(
            dict(decisions[0].target.target_capacity_by_accelerator), {
                'L4': 5,
                'A100': 5,
            })

    def test_logical_cold_floor_advances_only_after_each_wave_commits(self):
        autoscaler = _make_autoscaler(min_replicas=10,
                                      max_replicas=10,
                                      min_replicas_by_accelerator={'A100': 10},
                                      replica_unit='logical',
                                      max_scale_up_rate_percentage=10,
                                      scale_up_rate_min_replicas=1,
                                      scale_up_rate_period_seconds=60)
        autoscaler.set_configured_accelerator_shapes({'A100': 1})

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            _report(autoscaler,
                    in_flight={},
                    queued_profiles=[],
                    rejected_profiles=[],
                    compatibility_complete=True)
            first_wave = _decisions(autoscaler, [])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})
        self.assertEqual(
            dict(first_wave[0].target.target_capacity_by_accelerator),
            {'A100': 1})

        a100 = _replica(1, card='A100', planned_capacity=1)
        with mock.patch.object(autoscalers.time, 'time', return_value=120.0):
            _report(autoscaler,
                    in_flight={1: 0},
                    observed_slots={1: 1},
                    queued_profiles=[],
                    rejected_profiles=[],
                    compatibility_complete=True,
                    generation=2)
            cooldown = _decisions(autoscaler, [a100])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})
        self.assertEqual(cooldown, [])

        with mock.patch.object(autoscalers.time, 'time', return_value=161.0):
            _report(autoscaler,
                    in_flight={1: 0},
                    observed_slots={1: 1},
                    queued_profiles=[],
                    rejected_profiles=[],
                    compatibility_complete=True,
                    generation=3)
            second_wave = _decisions(autoscaler, [a100])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 2})
        self.assertEqual(
            dict(second_wave[0].target.target_capacity_by_accelerator),
            {'A100': 2})

    def test_physical_exact_card_stale_report_never_scales_down(self):
        autoscaler = _make_autoscaler(max_replicas=2)
        autoscaler.set_configured_accelerator_shapes({'L4': 1})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['L4'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)
        _decisions(autoscaler, [])
        autoscaler._report_received_at = (
            time.time() - autoscaler._staleness_threshold_seconds() - 1)
        replicas = [_replica(1, card='L4'), _replica(2, card='L4')]

        decisions = _decisions(autoscaler, replicas)

        self.assertEqual(decisions, [])

    def test_running_work_is_not_preempted_by_high_priority_backlog(self):
        autoscaler = _make_autoscaler(max_replicas=1)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        running_l4 = _replica(1, card='L4')
        _report(autoscaler,
                in_flight={1: 1},
                queue_depth=1,
                queued_profiles=[self._profile(50, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, [running_l4])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator,
                         {'L4': 1})
        self.assertEqual(decisions, [])

    def test_flexible_backlog_reuses_spare_capacity_on_running_card(self):
        autoscaler = _make_autoscaler(knob=2, max_replicas=2)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 4})
        running_a100 = _replica(1, gpu_count=4, card='A100')
        _report(autoscaler,
                in_flight={1: 4},
                queue_depth=4,
                queued_profiles=[self._profile(20, ['L4', 'A100'], 4)],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, [running_a100])

        # One A100 backend has capacity 2 * 4 = 8 and already carries four
        # requests, so the four queued requests need no cold L4 launch.
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})
        self.assertEqual(decisions, [])

    def test_ready_reserved_card_suppresses_launch_without_owning_demand(self):
        autoscaler = _make_autoscaler(max_replicas=1)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        reserved_a100 = _replica(1, card='A100')
        reserved_a100.is_zero_cost = True
        _report(autoscaler,
                in_flight={1: 0},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['L4', 'A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, [reserved_a100])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator, {})
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator, {})
        self.assertEqual(decisions, [])

    def test_retiring_warm_card_does_not_authorize_paid_replacement(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = _make_autoscaler(max_replicas=2,
                                      replica_unit='logical',
                                      upscale_delay_seconds=4 * interval,
                                      downscale_delay_seconds=300)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        l4 = _replica(1, card='L4', planned_capacity=1)
        a100 = _replica(2, card='A100', planned_capacity=1)
        replicas = [l4, a100]
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0
                },
                observed_slots={
                    1: 1,
                    2: 1
                },
                queue_depth=2,
                queued_profiles=[self._profile(20, ['L4', 'A100'], 2)],
                rejected_profiles=[],
                compatibility_complete=True)
        self.assertEqual(_decisions(autoscaler, replicas), [])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 2})

        # Model an operator retirement or reclaimed warm slot. Demand remains
        # attributed to L4 and the supply-aware launch fence must replace the
        # retiring A100 with L4 capacity.
        a100.status_property.is_scale_down = True
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0
                },
                observed_slots={
                    1: 1,
                    2: 1
                },
                queue_depth=2,
                queued_profiles=[self._profile(20, ['L4', 'A100'], 2)],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)

        decisions = _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 2})
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].operator, _SCALE_UP)
        self.assertEqual(
            dict(decisions[0].target.target_capacity_by_accelerator), {'L4': 2})

        # A completed drain can delete the A100 row. The next tick must retain
        # the L4 cold fence.
        _report(autoscaler,
                in_flight={1: 0},
                observed_slots={1: 1},
                queue_depth=2,
                queued_profiles=[self._profile(20, ['L4', 'A100'], 2)],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=3)
        decisions = _decisions(autoscaler, [l4])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 2})
        self.assertEqual(len(decisions), 1)
        self.assertEqual(
            dict(decisions[0].target.target_capacity_by_accelerator), {'L4': 2})

    def test_retiring_warm_card_can_be_replaced_for_constrained_demand(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = _make_autoscaler(max_replicas=1,
                                      replica_unit='logical',
                                      upscale_delay_seconds=2 * interval,
                                      downscale_delay_seconds=300)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        a100 = _replica(1, card='A100', planned_capacity=1)
        _report(autoscaler,
                in_flight={1: 0},
                observed_slots={1: 1},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)
        self.assertEqual(_decisions(autoscaler, [a100]), [])

        a100.status_property.is_scale_down = True
        _report(autoscaler,
                in_flight={1: 0},
                observed_slots={1: 1},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)

        decisions = _decisions(autoscaler, [a100])

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].operator, _SCALE_UP)
        self.assertEqual(
            dict(decisions[0].target.target_capacity_by_accelerator),
            {'A100': 1})

    def test_reclaimed_floor_card_uses_returned_reserved_slot(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = _make_autoscaler(max_replicas=2,
                                      replica_unit='logical',
                                      min_replicas_by_accelerator={'A100': 1},
                                      upscale_delay_seconds=4 * interval,
                                      downscale_delay_seconds=300)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        l4 = _replica(1, card='L4', planned_capacity=1)
        a100 = _replica(2, card='A100', planned_capacity=1)
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0
                },
                observed_slots={
                    1: 1,
                    2: 1
                },
                queue_depth=2,
                queued_profiles=[self._profile(20, ['L4', 'A100'], 2)],
                rejected_profiles=[],
                compatibility_complete=True)
        self.assertEqual(_decisions(autoscaler, [l4, a100]), [])
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {
            'L4': 1,
            'A100': 1,
        })

        # After the A100 row is deleted, the returned reserved slot backs the
        # A100 floor. Reconciliation must request that exact zero-cost slot,
        # not move the floor or duplicate flexible demand onto L4.
        autoscaler.set_free_reserved_slots_by_accelerator({'A100': 1})
        _report(autoscaler,
                in_flight={1: 0},
                observed_slots={1: 1},
                queue_depth=2,
                queued_profiles=[self._profile(20, ['L4', 'A100'], 2)],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)
        decisions = _decisions(autoscaler, [l4])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {
            'L4': 1,
            'A100': 1,
        })
        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].operator, _SCALE_UP)
        self.assertEqual(
            dict(decisions[0].target.target_capacity_by_accelerator), {
                'L4': 1,
                'A100': 1,
            })

    def test_floor_claims_reserved_slot_before_fill(self):
        autoscaler = _make_autoscaler(max_replicas=1,
                                      replica_unit='logical',
                                      min_replicas_by_accelerator={'A100': 1},
                                      reserved_capacity_fill=True)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        now = time.time()
        reserved_key = {
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
        autoscaler.set_free_reserved_slots_by_accelerator({'A100': 1})
        for _ in range(2):
            autoscaler.collect_reserved_capacity(1, [reserved_key], now)
        _report(autoscaler,
                in_flight={},
                observed_slots={},
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, [])

        self.assertEqual(len(decisions), 1)
        self.assertIsInstance(decisions[0].target,
                              autoscalers.LogicalScaleTarget)
        self.assertEqual(
            dict(decisions[0].target.target_capacity_by_accelerator),
            {'A100': 1})

    def test_preempted_logical_card_does_not_suppress_replacement(self):
        autoscaler = _make_autoscaler(max_replicas=1, replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'A100': 1})
        preempted = _replica(1, card='A100', planned_capacity=1)
        preempted.status_property.preempted = True
        _report(autoscaler,
                in_flight={1: 0},
                observed_slots={1: 1},
                queue_depth=1,
                queued_profiles=[self._profile(50, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, [preempted])

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].operator, _SCALE_UP)
        self.assertEqual(
            dict(decisions[0].target.target_capacity_by_accelerator),
            {'A100': 1})

    def test_preempted_physical_card_does_not_suppress_replacement(self):
        autoscaler = _make_autoscaler(max_replicas=1)
        autoscaler.set_configured_accelerator_shapes({'A100': 1})
        preempted = _replica(1, card='A100')
        preempted.status_property.preempted = True
        _report(autoscaler,
                in_flight={1: 0},
                queue_depth=1,
                queued_profiles=[self._profile(50, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, [preempted])

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].operator, _SCALE_UP)
        self.assertEqual(decisions[0].target, {'accelerators': {'A100': 1}})

    def test_zero_cost_only_card_does_not_precede_paid_fallback(self):
        a100_location = types.SimpleNamespace(accelerators={'A100': 1})
        l4_location = types.SimpleNamespace(accelerators={'L4': 1})
        placer = mock.Mock()
        placer.known_locations.return_value = [a100_location, l4_location]
        placer.cost_per_hour.side_effect = (
            lambda location: 0.0 if location is a100_location else 1.0)

        flexible = _make_autoscaler(max_replicas=1, replica_unit='logical')
        flexible.set_configured_accelerator_shapes({'A100': 1, 'L4': 1})
        flexible.set_spot_placer(placer)
        _report(flexible,
                in_flight={},
                observed_slots={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100', 'L4'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        _decisions(flexible, [])

        self.assertEqual(flexible.target_num_replicas_by_accelerator, {'L4': 1})

        exact = _make_autoscaler(max_replicas=1, replica_unit='logical')
        exact.set_configured_accelerator_shapes({'A100': 1, 'L4': 1})
        exact.set_spot_placer(placer)
        _report(exact,
                in_flight={},
                observed_slots={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        _decisions(exact, [])

        self.assertEqual(exact.target_num_replicas_by_accelerator, {'A100': 1})

    def test_inconclusive_zero_cost_card_preserves_service_order(self):
        a100_zero = types.SimpleNamespace(accelerators={'A100': 1})
        a100_unpriced = types.SimpleNamespace(accelerators={'A100': 1})
        l4_location = types.SimpleNamespace(accelerators={'L4': 1})
        placer = mock.Mock()
        placer.known_locations.return_value = [
            a100_zero, a100_unpriced, l4_location
        ]
        placer.cost_per_hour.side_effect = (
            lambda location: 0.0 if location is a100_zero else float('inf')
            if location is a100_unpriced else 1.0)
        autoscaler = _make_autoscaler(max_replicas=1, replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'A100': 1, 'L4': 1})
        autoscaler.set_spot_placer(placer)
        _report(autoscaler,
                in_flight={},
                observed_slots={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100', 'L4'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        _decisions(autoscaler, [])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 1})

    def test_partial_nominal_prices_preserve_service_order(self):
        autoscaler = _make_autoscaler(max_replicas=1, replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        l4_location = types.SimpleNamespace(accelerators={'L4': 1})
        a100_location = types.SimpleNamespace(accelerators={'A100': 1})
        placer = mock.Mock()
        placer.known_locations.return_value = [l4_location, a100_location]
        placer.cost_per_hour.side_effect = (lambda location: float('inf')
                                            if location is l4_location else 2.0)
        autoscaler.set_spot_placer(placer)
        _report(autoscaler,
                in_flight={},
                observed_slots={},
                queue_depth=1,
                queued_profiles=[self._profile(20, ['L4', 'A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        _decisions(autoscaler, [])

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 1})

    def test_rejection_profiles_preserve_aggregate_duration_math(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=100,
                                      replica_unit='logical',
                                      expected_request_duration_seconds=30)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                rejected=120,
                recent_rejected=60,
                rejected_profiles=[
                    self._profile(20, ['L4'], 60, 0),
                    self._profile(50, ['A100'], 60, 60),
                ],
                compatibility_complete=True)

        _decisions(autoscaler, [])

        # Aggregate rejection work remains max(120*30/360, 60*30/60)=30.
        self.assertEqual(autoscaler._rejected_concurrency, 30)
        # Independent exact-card rounding may add one slot while preserving
        # the aggregate rejection-work signal itself.
        self.assertEqual(
            sum(autoscaler.target_num_replicas_by_accelerator.values()), 31)
        self.assertGreater(
            autoscaler.target_num_replicas_by_accelerator.get('A100', 0),
            autoscaler.target_num_replicas_by_accelerator.get('L4', 0))

    def test_logical_target_carries_card_slots_and_physical_shapes(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=9,
                                      replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 8})
        _report(autoscaler,
                in_flight={},
                queue_depth=9,
                queued_profiles=[
                    self._profile(50, ['A100'], 8),
                    self._profile(20, ['L4'], 1),
                ],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=7)

        decisions = _decisions(autoscaler, [])

        self.assertEqual(len(decisions), 1)
        target = decisions[0].target
        self.assertIsInstance(target, autoscalers.LogicalScaleTarget)
        self.assertEqual(dict(target.target_capacity_by_accelerator), {
            'L4': 1,
            'A100': 8,
        })
        self.assertEqual(dict(target.accelerator_shapes), {
            'L4': 1,
            'A100': 8,
        })

    def test_incomplete_mixed_version_report_holds_restart_fence(self):
        autoscaler = _make_autoscaler(min_replicas=1, max_replicas=10)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        replicas = [_replica(1, card='L4'), _replica(2, card='A100')]
        _report(autoscaler, in_flight={1: 0, 2: 0})

        decisions = _decisions(autoscaler, replicas)

        self.assertEqual(decisions, [])
        self.assertTrue(autoscaler._snap_target_on_next_recompute)
        self.assertEqual(autoscaler.target_num_replicas, 1)

    def test_fill_restart_survives_old_to_new_lb_report_handoff(self):
        autoscaler = _make_autoscaler(max_replicas=1000, replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        replicas = [
            _replica(i + 1, card='L4', reserved_fill=True) for i in range(159)
        ]
        idle = {replica.replica_id: 0 for replica in replicas}

        # The selected old LB is authoritative for aggregate demand, but its
        # older wire protocol cannot prove exact-card attribution. Keep the
        # restart fence armed and do not turn fill capacity into demand.
        _report(autoscaler, in_flight=idle, compatibility_complete=False)
        self.assertEqual(_decisions(autoscaler, replicas), [])
        self.assertTrue(autoscaler._snap_target_on_next_recompute)
        self.assertEqual(autoscaler.target_num_replicas, 0)

        # The first complete report from the upgraded active LB supersedes the
        # incomplete snapshot. Orange follows observed traffic, not the 159
        # fill-origin slots that remain usable capacity.
        _report(autoscaler,
                in_flight=idle,
                queue_depth=17,
                queued_profiles=[self._profile(20, ['L4'], 17)],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=2)
        _decisions(autoscaler, replicas)

        self.assertFalse(autoscaler._snap_target_on_next_recompute)
        self.assertEqual(autoscaler._raw_target_num_replicas, 17)
        self.assertEqual(autoscaler.target_num_replicas, 17)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 17})

    def test_logical_restart_seeds_card_map_before_downscale(self):
        autoscaler = _make_autoscaler(max_replicas=20,
                                      replica_unit='logical',
                                      downscale_delay_seconds=300,
                                      max_scale_down_rate_percentage=50)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        replicas = ([_replica(i, card='L4') for i in range(1, 7)] +
                    [_replica(i, card='A100') for i in range(7, 11)])
        idle = {replica.replica_id: 0 for replica in replicas}
        slots = {replica.replica_id: 1 for replica in replicas}
        _report(autoscaler,
                in_flight=idle,
                observed_slots=slots,
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock:
            first = _decisions(autoscaler, replicas)
            self.assertEqual(first, [])
            self.assertEqual(autoscaler.target_num_replicas, 10)
            self.assertEqual(
                sum(autoscaler.target_num_replicas_by_accelerator.values()), 10)
            self.assertGreaterEqual(
                autoscaler.target_num_replicas_by_accelerator['A100'], 1)

            # The reconstructed map is a baseline, not an upscale. It must
            # not lower the aggregate early or restart the quiet window.
            started_at = autoscaler._downscale_started_at
            clock.return_value = 120.0
            second = _decisions(autoscaler, replicas)
            self.assertEqual(second, [])
            self.assertEqual(autoscaler.target_num_replicas, 10)
            self.assertEqual(autoscaler._downscale_started_at, started_at)

            clock.return_value = 380.0
            _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler.target_num_replicas, 5)
        self.assertEqual(
            sum(autoscaler.target_num_replicas_by_accelerator.values()), 5)
        self.assertGreaterEqual(
            autoscaler.target_num_replicas_by_accelerator['A100'], 1)

    def test_all_compatible_restart_does_not_rebuild_a100_demand(self):
        autoscaler = _make_autoscaler(max_replicas=1000,
                                      replica_unit='logical',
                                      downscale_delay_seconds=300,
                                      max_scale_up_rate_percentage=20,
                                      scale_up_rate_min_replicas=10,
                                      scale_up_rate_period_seconds=60,
                                      max_scale_down_rate_percentage=50)
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
            'A100-80GB': 1,
        })

        # Production recovery shape: 47 demand-owned slots survive across all
        # three cards, while the rest of the large warm fleet is independent
        # reserved-fill supply. The controller-local card map starts empty.
        replicas = []
        replica_id = 1
        demand_owned_left = {'L4': 20, 'A100': 18, 'A100-80GB': 9}
        for card, count in [('L4', 122), ('A100', 55), ('A100-80GB', 22)]:
            for _ in range(count):
                replicas.append(
                    _replica(replica_id,
                             card=card,
                             reserved_fill=demand_owned_left[card] <= 0))
                demand_owned_left[card] -= 1
                replica_id += 1
        idle = {replica.replica_id: 0 for replica in replicas}
        slots = {replica.replica_id: 1 for replica in replicas}
        _report(autoscaler,
                in_flight=idle,
                observed_slots=slots,
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)

        decisions = _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler.target_num_replicas, 47)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'L4': 47})
        self.assertEqual(autoscaler.warm_retention_target_by_accelerator, {})
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator, {})
        self.assertEqual(_scale_ups(decisions), [])

    def test_logical_ramped_restart_does_not_stall_empty_card_map(self):
        autoscaler = _make_autoscaler(max_replicas=20,
                                      replica_unit='logical',
                                      downscale_delay_seconds=300,
                                      max_scale_up_rate_percentage=50,
                                      scale_up_rate_min_replicas=1,
                                      scale_up_rate_period_seconds=60,
                                      max_scale_down_rate_percentage=50)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        replicas = ([_replica(i, card='L4') for i in range(1, 7)] +
                    [_replica(i, card='A100') for i in range(7, 11)])
        idle = {replica.replica_id: 0 for replica in replicas}
        slots = {replica.replica_id: 1 for replica in replicas}
        _report(autoscaler,
                in_flight=idle,
                observed_slots=slots,
                queue_depth=1,
                queued_profiles=[self._profile(20, ['A100'], 1)],
                rejected_profiles=[],
                compatibility_complete=True)

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock, mock.patch.object(
                                   autoscalers.time,
                                   'time',
                                   return_value=1000.0):
            _decisions(autoscaler, replicas)
            started_at = autoscaler._downscale_started_at
            self.assertEqual(
                sum(autoscaler.target_num_replicas_by_accelerator.values()), 10)

            clock.return_value = 120.0
            _decisions(autoscaler, replicas)
            self.assertEqual(autoscaler._downscale_started_at, started_at)
            self.assertEqual(
                sum(autoscaler.target_num_replicas_by_accelerator.values()), 10)

            clock.return_value = 380.0
            _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler.target_num_replicas, 5)
        self.assertEqual(
            sum(autoscaler.target_num_replicas_by_accelerator.values()), 5)

    def test_logical_ramped_restart_retries_unspent_card_migration_wave(self):
        autoscaler = _make_autoscaler(max_replicas=10,
                                      replica_unit='logical',
                                      downscale_delay_seconds=300,
                                      max_scale_up_rate_percentage=50,
                                      scale_up_rate_min_replicas=1,
                                      scale_up_rate_period_seconds=60)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        replicas = [_replica(i, card='L4') for i in range(1, 11)]
        idle = {replica.replica_id: 0 for replica in replicas}
        slots = {replica.replica_id: 1 for replica in replicas}

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0), mock.patch.object(
                                   autoscalers.time,
                                   'time',
                                   return_value=1000.0) as clock:
            _report(autoscaler,
                    in_flight=idle,
                    observed_slots=slots,
                    queue_depth=10,
                    queued_profiles=[self._profile(20, ['A100'], 10)],
                    rejected_profiles=[],
                    compatibility_complete=True)
            first = _decisions(autoscaler, replicas)

            self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                             {'A100': 10})
            self.assertEqual(len(first), 1)
            self.assertEqual(
                dict(first[0].target.target_capacity_by_accelerator), {
                    'L4': 5,
                    'A100': 5,
                })
            self.assertEqual(first[0].target.launch_budget, 5)

            clock.return_value = 1020.0
            _report(autoscaler,
                    in_flight=idle,
                    observed_slots=slots,
                    queue_depth=10,
                    queued_profiles=[self._profile(20, ['A100'], 10)],
                    rejected_profiles=[],
                    compatibility_complete=True,
                    generation=2)
            cooldown = _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 10})
        self.assertEqual(len(cooldown), 1)
        self.assertEqual(
            dict(cooldown[0].target.target_capacity_by_accelerator),
            {'A100': 10})
        self.assertEqual(cooldown[0].target.launch_budget, 5)

    def test_request_rate_lower_aggregate_ignores_positive_card_delta(self):
        autoscaler = self._instance_aware_autoscaler()
        candidate = {'L4': 4, 'A100': 5}

        with mock.patch.object(autoscaler,
                               '_calculate_target_by_accelerator',
                               return_value=candidate):
            autoscaler._set_target_num_replicas_with_instance_aware_logic([])
            self.assertEqual(autoscaler.target_num_replicas, 10)
            self.assertEqual(autoscaler.downscale_counter, 1)
            self.assertEqual(autoscaler.upscale_counter, 0)

            autoscaler._set_target_num_replicas_with_instance_aware_logic([])

        self.assertEqual(autoscaler.target_num_replicas, 9)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         candidate)

    def test_request_rate_equal_aggregate_card_shift_remains_upscale(self):
        autoscaler = self._instance_aware_autoscaler()
        candidate = {'L4': 5, 'A100': 5}

        with mock.patch.object(autoscaler,
                               '_calculate_target_by_accelerator',
                               return_value=candidate):
            autoscaler._set_target_num_replicas_with_instance_aware_logic([])
            self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                             {'L4': 10})
            self.assertEqual(autoscaler.upscale_counter, 1)
            self.assertEqual(autoscaler.downscale_counter, 0)

            autoscaler._set_target_num_replicas_with_instance_aware_logic([])

        self.assertEqual(autoscaler.target_num_replicas, 10)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         candidate)


class TestSignalGap(unittest.TestCase):
    """No shrink of any kind while the demand report is stale."""

    def test_fresh_autoscaler_starts_stale(self):
        autoscaler = _make_autoscaler()
        self.assertFalse(autoscaler.has_fresh_demand_report())

    def test_report_without_in_flight_does_not_unlock(self):
        # An old LB ships only timestamps: still signal-stale.
        autoscaler = _make_autoscaler()
        autoscaler.collect_request_information({'timestamps': [time.time()]})
        self.assertFalse(autoscaler.has_fresh_demand_report())

    def test_report_ages_out(self):
        autoscaler = _make_autoscaler()
        _report(autoscaler, in_flight={1: 1})
        self.assertTrue(autoscaler.has_fresh_demand_report())
        stale_at = (time.time() +
                    3 * constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS + 1)
        with mock.patch.object(autoscalers.time, 'time', return_value=stale_at):
            self.assertFalse(autoscaler.has_fresh_demand_report())

    def test_mid_tick_fresh_report_cannot_unlock_scale_down(self):
        # TOCTOU guard: freshness is snapshotted once per tick. If the
        # first fresh report lands DURING the tick (after the recompute
        # took the stale path, leaving the rebuilt-blind min target),
        # the scale-down guards must still see the tick as stale --
        # otherwise current > blind-target mass-kills idle replicas
        # that the very next tick's snap would have kept.
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [_replica(i) for i in (1, 2, 3)]
        original = (autoscaler._set_target_num_replicas_with_concurrency_logic)

        def _report_mid_tick(replica_infos):
            original(replica_infos)
            # Fresh report (all idle) arrives between the recompute and
            # the scale-down guards.
            _report(autoscaler, in_flight={1: 0, 2: 0, 3: 0})

        with mock.patch.object(
                autoscaler,
                '_set_target_num_replicas_with_concurrency_logic',
                side_effect=_report_mid_tick):
            decisions = _decisions(autoscaler, replicas)
        self.assertEqual(_scale_downs(decisions), [])
        # The next full tick sees the report from its start and may act.
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(len(_scale_downs(decisions)), 2)

    def test_stale_arrival_floor_prunes_old_timestamps(self):
        # Once syncs stop, collect_request_information never runs again
        # to prune the window; the stale-branch recompute must prune it
        # itself or arrivals long outside the window keep asserting a
        # floor.
        autoscaler = _make_autoscaler(min_replicas=0)
        _report(autoscaler, in_flight={}, timestamps=[time.time()] * 7)
        replicas: list = []
        stale_at = (time.time() + autoscaler.qps_window_size +
                    3 * constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS + 2)
        with mock.patch.object(autoscalers.time, 'time', return_value=stale_at):
            _decisions(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 0)

    def test_recomputed_with_fresh_data_flips_on_tick_not_on_report(self):
        # The first report flips freshness on the sync thread, but the
        # target stays at the rebuilt-blind minimum until the decision
        # tick consumes the snap -- the capacity hint floors until then.
        autoscaler = _make_autoscaler(min_replicas=1)
        self.assertFalse(autoscaler.has_recomputed_with_fresh_data())
        _report(autoscaler, in_flight={1: 1})
        self.assertTrue(autoscaler.has_fresh_demand_report())
        self.assertFalse(autoscaler.has_recomputed_with_fresh_data())
        _decisions(autoscaler, [_replica(1)])
        self.assertTrue(autoscaler.has_recomputed_with_fresh_data())

    def test_no_scale_down_while_stale(self):
        # Rebuilt-controller scenario: target=min_replicas, live fleet of
        # 3 -- nothing may be retired before the first fresh report.
        autoscaler = _make_autoscaler(min_replicas=1)
        replicas = [_replica(i) for i in (1, 2, 3)]
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(decisions, [])

    def test_no_rolling_drain_while_stale(self):
        autoscaler = _make_autoscaler(min_replicas=1)
        autoscaler.update_version(2, _spec(knob=1.0, min_replicas=1),
                                  serve_utils.UpdateMode.ROLLING)
        old_replicas = [_replica(i, version=1) for i in (1, 2, 3)]
        new_ready = [_replica(4, version=2)]
        self.assertEqual(
            autoscaler._select_outdated_replicas_to_scale_down(
                old_replicas + new_ready, [1, 2]), [])

    def test_arrival_floor_scales_up_while_stale(self):
        autoscaler = _make_autoscaler(knob=1.0, min_replicas=1)
        replicas = [_replica(1)]
        now = time.time()
        autoscaler.collect_request_information({'timestamps': [now - 1] * 5})
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 5)
        self.assertEqual(len(_scale_ups(decisions)), 4)
        self.assertEqual(_scale_downs(decisions), [])

    def test_logical_arrival_floor_uses_saturation_target(self):
        autoscaler = _make_autoscaler(knob=2,
                                      min_replicas=1,
                                      replica_unit='logical')
        replicas = [_replica(1)]
        now = time.time()
        autoscaler.collect_request_information({'timestamps': [now - 1] * 5})

        decisions = _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler.target_num_replicas, 3)
        self.assertEqual(len(_scale_ups(decisions)), 1)
        self.assertEqual(_scale_ups(decisions)[0].target.target_capacity, 3)

    def test_logical_arrival_floor_uses_duration_and_utilization(self):
        autoscaler = _make_autoscaler(
            knob=1,
            min_replicas=0,
            max_replicas=100,
            replica_unit='logical',
            target_utilization_percentage=90,
            expected_request_duration_seconds=30,
        )
        now = time.time()
        autoscaler.collect_request_information({'timestamps': [now - 1] * 60})

        decisions = _decisions(autoscaler, [])

        # 60 arrivals / minute at 30 seconds each imply 30 concurrent jobs;
        # 90% target utilization reserves four additional slots.
        self.assertEqual(autoscaler.target_num_replicas, 34)
        self.assertEqual(_scale_ups(decisions)[0].target.target_capacity, 34)


class TestLogicalScalingWaves(unittest.TestCase):
    """Logical demand changes are adopted in bounded, timed waves."""

    @staticmethod
    def _ramped_autoscaler(**kwargs):
        return _make_autoscaler(knob=1,
                                min_replicas=0,
                                max_replicas=1000,
                                replica_unit='logical',
                                max_scale_up_rate_percentage=20,
                                scale_up_rate_min_replicas=10,
                                scale_up_rate_period_seconds=60,
                                **kwargs)

    def test_zero_to_burst_starts_with_ten_slots(self):
        autoscaler = self._ramped_autoscaler()
        _report(autoscaler, in_flight={}, queue_depth=1000)

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic([])

        self.assertEqual(autoscaler._raw_target_num_replicas, 1000)
        self.assertEqual(autoscaler.target_num_replicas, 10)
        self.assertEqual(autoscaler._last_scale_up_wave_at, 100.0)

    def test_next_wave_waits_a_minute_and_counts_committed_slots(self):
        autoscaler = self._ramped_autoscaler()
        _report(autoscaler, in_flight={}, queue_depth=1000)
        replicas = [_replica(i + 1) for i in range(10)]
        autoscaler.target_num_replicas = 10
        autoscaler._snap_target_on_next_recompute = False
        autoscaler._last_scale_up_wave_at = 100.0

        with mock.patch.object(autoscalers.time, 'time', return_value=159.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
        self.assertEqual(autoscaler.target_num_replicas, 10)

        with mock.patch.object(autoscalers.time, 'time', return_value=160.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
        self.assertEqual(autoscaler.target_num_replicas, 20)

    def test_pending_target_is_not_reduced_when_committed_capacity_lags(self):
        autoscaler = self._ramped_autoscaler()
        autoscaler.target_num_replicas = 20
        autoscaler._snap_target_on_next_recompute = False
        autoscaler._last_scale_up_wave_at = 100.0
        _report(autoscaler, in_flight={}, queue_depth=1000)
        replicas = [_replica(i + 1) for i in range(10)]

        with mock.patch.object(autoscalers.time, 'time', return_value=160.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        self.assertEqual(autoscaler.target_num_replicas, 20)

    def test_restart_adopts_old_version_capacity_before_latest_wave(self):
        autoscaler = self._ramped_autoscaler()
        autoscaler.latest_version = 2
        replicas = [_replica(i + 1, version=1) for i in range(50)]
        _report(autoscaler, in_flight={}, queue_depth=1000)

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        self.assertEqual(autoscaler._raw_target_num_replicas, 1000)
        # The restart baseline is still adopted from the old-version fleet
        # (50), and saturated demand is no longer deferred behind it: one
        # wave minimum lands in the same tick. The wave rate itself remains
        # latest-version based, so this is a bounded +10, not a jump to the
        # raw target.
        self.assertEqual(autoscaler.target_num_replicas, 60)

    def test_restart_total_excludes_fill_retiring_and_pending_rows(self):
        autoscaler = self._ramped_autoscaler()
        replicas = [
            _replica(1, version=1),
            _replica(2, version=1, reserved_fill=True),
            _replica(3, version=2),
            _replica(4,
                     version=2,
                     status=serve_state.ReplicaStatus.PROVISIONING),
        ]
        replicas[2].status_property.is_scale_down = True
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0,
                    3: 0
                },
                observed_slots={
                    1: 1,
                    2: 1,
                    3: 1
                })

        self.assertEqual(
            autoscaler._total_ready_demand_owned_logical_capacity(replicas), 1)

    def test_restart_target_retries_unspent_wave_during_cooldown(self):
        autoscaler = self._ramped_autoscaler(downscale_delay_seconds=300)
        autoscaler.latest_version = 2
        autoscaler.set_configured_accelerator_shapes({'L4': 1})
        replicas = [_replica(i + 1, card='L4', version=1) for i in range(50)]
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas},
                queue_depth=1,
                observed_slots={replica.replica_id: 1 for replica in replicas},
                queued_profiles=[{
                    'priority': 20,
                    'compatible_accelerators': ['L4'],
                    'count': 1,
                }],
                compatibility_complete=True)

        with mock.patch.object(autoscalers.time, 'time',
                               return_value=100.0) as clock:
            decisions = _decisions(autoscaler, replicas, active_versions=(1, 2))

            clock.return_value = 160.0
            _report(
                autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas},
                queue_depth=1,
                observed_slots={replica.replica_id: 1 for replica in replicas},
                queued_profiles=[{
                    'priority': 20,
                    'compatible_accelerators': ['L4'],
                    'count': 1,
                }],
                compatibility_complete=True,
                generation=2)
            next_wave = _decisions(autoscaler, replicas, active_versions=(1, 2))

            clock.return_value = 180.0
            _report(
                autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas},
                queue_depth=1,
                observed_slots={replica.replica_id: 1 for replica in replicas},
                queued_profiles=[{
                    'priority': 20,
                    'compatible_accelerators': ['L4'],
                    'count': 1,
                }],
                compatibility_complete=True,
                generation=3)
            cooldown = _decisions(autoscaler, replicas, active_versions=(1, 2))

        self.assertEqual(autoscaler.target_num_replicas, 50)
        self.assertEqual(len(_scale_ups(decisions)), 1)
        target = _scale_ups(decisions)[0].target
        self.assertEqual(target.target_capacity, 50)
        self.assertEqual(target.launch_budget, 10)
        self.assertEqual(dict(target.target_capacity_by_accelerator),
                         {'L4': 50})
        self.assertEqual(_scale_ups(next_wave)[0].target.launch_budget, 10)
        self.assertEqual(_scale_ups(cooldown)[0].target.launch_budget, 10)
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator,
                         {'L4': 10})

    def test_restart_target_spends_completed_wave_during_cooldown(self):
        autoscaler = self._ramped_autoscaler(downscale_delay_seconds=300)
        autoscaler.latest_version = 2
        autoscaler.set_configured_accelerator_shapes({'L4': 1})
        old = [_replica(i + 1, card='L4', version=1) for i in range(50)]
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in old},
                queue_depth=1,
                observed_slots={replica.replica_id: 1 for replica in old},
                queued_profiles=[{
                    'priority': 20,
                    'compatible_accelerators': ['L4'],
                    'count': 1,
                }],
                compatibility_complete=True)

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            first = _decisions(autoscaler, old, active_versions=(1, 2))

        latest = [
            _replica(100 + i,
                     card='L4',
                     version=2,
                     status=serve_state.ReplicaStatus.PROVISIONING)
            for i in range(10)
        ]
        replicas = old + latest
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas},
                queue_depth=1,
                observed_slots={replica.replica_id: 1 for replica in old},
                queued_profiles=[{
                    'priority': 20,
                    'compatible_accelerators': ['L4'],
                    'count': 1,
                }],
                compatibility_complete=True,
                generation=2)
        with mock.patch.object(autoscalers.time, 'time', return_value=120.0):
            cooldown = _decisions(autoscaler, replicas, active_versions=(1, 2))

        self.assertEqual(_scale_ups(first)[0].target.launch_budget, 10)
        self.assertEqual(_scale_ups(cooldown)[0].target.launch_budget, 0)
        self.assertEqual(autoscaler.cold_launch_authority_by_accelerator, {})

    def test_held_target_completes_exact_card_map_past_current_wave(self):
        autoscaler = self._ramped_autoscaler()
        autoscaler.latest_version = 2
        autoscaler.set_configured_accelerator_shapes({'L4': 1})
        old = [_replica(i + 1, card='L4', version=1) for i in range(50)]

        limited, added = autoscaler._limit_logical_actuation_transition(
            {'L4': 50}, 50, old, wave_budget=10)

        self.assertEqual(limited, {'L4': 50})
        self.assertEqual(added, 50)

    def test_twenty_percent_dominates_floor_for_large_fleet(self):
        autoscaler = self._ramped_autoscaler()
        autoscaler.target_num_replicas = 100
        _report(autoscaler, in_flight={}, queue_depth=1000)
        replicas = [_replica(i + 1) for i in range(100)]

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        self.assertEqual(autoscaler.target_num_replicas, 120)

    def test_card_mix_shift_does_not_restart_aggregate_downscale_window(self):
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=300,
            max_scale_down_rate_percentage=50,
            target_utilization_percentage=90,
        )
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
        })
        autoscaler.target_num_replicas = 340
        autoscaler.target_num_replicas_by_accelerator = {'L4': 340}
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={},
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)

        candidate = {'L4': 103, 'A100': 106}

        def calculate_target(*_args, **kwargs):
            result = dict(candidate)
            adopted = kwargs.get('min_replicas_override')
            if adopted is not None and sum(result.values()) < adopted:
                result['L4'] += adopted - sum(result.values())
            return result, True

        with mock.patch.object(
                autoscaler,
                '_calculate_concurrency_target_by_accelerator',
                side_effect=calculate_target), mock.patch.object(
                    autoscaler, '_outstanding_work', return_value=188.1
                ), mock.patch.object(
                    autoscaler,
                    '_latest_committed_logical_capacity',
                    return_value=340), mock.patch.object(
                        autoscaler,
                        '_latest_demand_owned_logical_capacity',
                        return_value=340), mock.patch.object(
                            autoscaler,
                            '_provisioning_logical_capacity',
                            return_value=0), mock.patch.object(
                                autoscaler,
                                '_provisioning_demand_owned_logical_capacity',
                                return_value=0), mock.patch.object(
                                    autoscalers.time,
                                    'monotonic',
                                    return_value=100.0
                                ) as monotonic, mock.patch.object(
                                    autoscalers.time,
                                    'time',
                                    return_value=1000.0) as wall_clock:
            autoscaler._set_target_num_replicas_with_concurrency_logic([])

            self.assertEqual(autoscaler._raw_target_num_replicas, 209)
            self.assertEqual(autoscaler.target_num_replicas, 340)
            started_at = autoscaler._downscale_started_at
            self.assertEqual(started_at, 80.0)
            self.assertGreater(
                autoscaler.target_num_replicas_by_accelerator['A100'], 0)

            monotonic.return_value = 120.0
            wall_clock.return_value = 1020.0
            autoscaler._set_target_num_replicas_with_concurrency_logic([])

        self.assertEqual(autoscaler.target_num_replicas, 340)
        self.assertEqual(autoscaler._downscale_started_at, started_at)

    def test_card_mix_shift_accepts_capped_aggregate_downscale(self):
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=300,
            max_scale_down_rate_percentage=50,
            target_utilization_percentage=100,
        )
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
        })
        autoscaler.target_num_replicas = 340
        autoscaler.target_num_replicas_by_accelerator = {'L4': 340}
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={},
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)

        candidate = {'A100': 106}

        def calculate_target(*_args, **kwargs):
            result = dict(candidate)
            adopted = kwargs.get('min_replicas_override')
            if adopted is not None and sum(result.values()) < adopted:
                result['L4'] = adopted - sum(result.values())
            return result, True

        with mock.patch.object(
                autoscaler,
                '_calculate_concurrency_target_by_accelerator',
                side_effect=calculate_target), mock.patch.object(
                    autoscaler, '_outstanding_work', return_value=106.0
                ), mock.patch.object(
                    autoscaler,
                    '_latest_committed_logical_capacity',
                    return_value=340), mock.patch.object(
                        autoscaler,
                        '_latest_demand_owned_logical_capacity',
                        return_value=340), mock.patch.object(
                            autoscaler,
                            '_provisioning_logical_capacity',
                            return_value=0), mock.patch.object(
                                autoscaler,
                                '_provisioning_demand_owned_logical_capacity',
                                return_value=0), mock.patch.object(
                                    autoscalers.time,
                                    'monotonic',
                                    return_value=100.0
                                ) as monotonic, mock.patch.object(
                                    autoscalers.time,
                                    'time',
                                    return_value=1000.0) as wall_clock:
            autoscaler._set_target_num_replicas_with_concurrency_logic([])
            monotonic.return_value = 380.0
            wall_clock.return_value = 1280.0
            autoscaler._set_target_num_replicas_with_concurrency_logic([])

        self.assertEqual(autoscaler._raw_target_num_replicas, 106)
        self.assertEqual(autoscaler.target_num_replicas, 170)
        self.assertEqual(
            sum(autoscaler.target_num_replicas_by_accelerator.values()), 170)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator['A100'],
                         106)
        self.assertEqual(autoscaler._last_scale_down_allowance, 170)

    def test_sustained_pressure_uses_adaptive_wave_without_skipping_pacing(
            self):
        autoscaler = self._ramped_autoscaler(
            adaptive_scale_up={
                'max_scale_up_rate_percentage': 100,
                'scale_up_rate_min_replicas': 50,
                'pressure_observations': 2,
                'hold_seconds': 120,
            })
        replicas = [_replica(i + 1) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0), mock.patch.object(
                                   autoscalers.time,
                                   'time',
                                   return_value=1000.0) as wall_clock:
            _report(autoscaler, in_flight={}, queue_depth=100)
            _report(autoscaler, in_flight={}, queue_depth=200)
            _report(autoscaler, in_flight={}, queue_depth=500)
            self.assertTrue(autoscaler._adaptive_scale_up_active())

            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 200)

            # Adaptive mode changes wave size, not the shared 60-second timer.
            wall_clock.return_value = 1059.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 200)

    def test_wave_base_counts_old_version_fleet_during_rollout(self):
        autoscaler = self._ramped_autoscaler()
        _report(autoscaler, in_flight={}, queue_depth=1000)
        old_fleet = [_replica(i + 1, version=0) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic(
                old_fleet)

        # Latest-version committed capacity is zero mid-rollout. The target
        # ceiling counts the saturated 100-slot serving fleet, so the wave
        # lands at 100 + max(10, 20% of 0) = 110. A latest-only ceiling
        # would freeze the target at 100 for the whole rollout, which is the
        # production incident. The wave rate stays latest-version based so
        # rollout replacement pacing is unchanged.
        self.assertEqual(autoscaler.target_num_replicas, 110)

    def test_saturated_plateau_progresses_adaptive_waves_to_max(self):
        """Incident regression: a flat saturated queue must keep doubling.

        2026-07-22: the queue pinned at its cap, the strictly-increasing
        pressure rule disarmed adaptive scale-up, and the latest-only wave
        base froze the target. With both fixes the fleet ramps
        130 -> 200 -> 400 -> 800 -> max while the queue stays flat.
        """
        autoscaler = self._ramped_autoscaler(
            adaptive_scale_up={
                'max_scale_up_rate_percentage': 100,
                'scale_up_rate_min_replicas': 50,
                'pressure_observations': 2,
                'hold_seconds': 120,
            })
        fleet = [_replica(i + 1) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0):
            _report(autoscaler, in_flight={}, queue_depth=2000)
            with mock.patch.object(autoscalers.time, 'time',
                                   return_value=100.0):
                autoscaler._set_target_num_replicas_with_concurrency_logic(
                    fleet)
            # First wave rides the base rate: 100 + max(10, 20% of 100).
            self.assertEqual(autoscaler.target_num_replicas, 120)

            # The queue holds perfectly flat; saturation arms adaptive.
            _report(autoscaler, in_flight={}, queue_depth=2000)
            _report(autoscaler, in_flight={}, queue_depth=2000)
            self.assertTrue(autoscaler._adaptive_scale_up_active())

            expected = 100
            for wave, wave_time in enumerate((161.0, 222.0, 283.0, 344.0)):
                expected = min(1000, 2 * expected)
                _report(autoscaler, in_flight={}, queue_depth=2000)
                with mock.patch.object(autoscalers.time,
                                       'time',
                                       return_value=wave_time):
                    (autoscaler._set_target_num_replicas_with_concurrency_logic(
                        fleet))
                self.assertEqual(autoscaler.target_num_replicas, expected,
                                 f'wave {wave}')
                # Launched capacity commits (STARTING rows) before the next
                # wave, so each adaptive wave doubles the fleet.
                fleet = fleet + [
                    _replica(len(fleet) + i + 1,
                             status=serve_state.ReplicaStatus.STARTING)
                    for i in range(expected - len(fleet))
                ]

    def test_queue_plateau_sustains_pressure_streak(self):
        autoscaler = self._ramped_autoscaler(
            adaptive_scale_up={
                'max_scale_up_rate_percentage': 100,
                'scale_up_rate_min_replicas': 50,
                'pressure_observations': 2,
                'hold_seconds': 120,
            })

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0):
            _report(autoscaler, in_flight={}, queue_depth=1500)
            _report(autoscaler, in_flight={}, queue_depth=1560)
            self.assertEqual(autoscaler._pressure_streak, 1)
            # A queue pinned flat at its cap is saturation, not relief.
            _report(autoscaler, in_flight={}, queue_depth=1560)
            self.assertEqual(autoscaler._pressure_streak, 2)
            self.assertTrue(autoscaler._adaptive_scale_up_active())

    def test_draining_queue_resets_pressure_streak(self):
        autoscaler = self._ramped_autoscaler(
            adaptive_scale_up={
                'max_scale_up_rate_percentage': 100,
                'scale_up_rate_min_replicas': 50,
                'pressure_observations': 2,
                'hold_seconds': 120,
            })

        _report(autoscaler, in_flight={}, queue_depth=1500)
        _report(autoscaler, in_flight={}, queue_depth=1560)
        self.assertEqual(autoscaler._pressure_streak, 1)
        _report(autoscaler, in_flight={}, queue_depth=900)
        self.assertEqual(autoscaler._pressure_streak, 0)
        self.assertFalse(autoscaler._adaptive_scale_up_active())

    def test_small_flat_queue_below_wave_minimum_is_not_pressure(self):
        autoscaler = self._ramped_autoscaler(
            adaptive_scale_up={
                'max_scale_up_rate_percentage': 100,
                'scale_up_rate_min_replicas': 50,
                'pressure_observations': 2,
                'hold_seconds': 120,
            })

        _report(autoscaler, in_flight={}, queue_depth=2)
        _report(autoscaler, in_flight={}, queue_depth=3)
        self.assertEqual(autoscaler._pressure_streak, 1)
        # Flat trickle queues below scale_up_rate_min_replicas stay
        # non-latching so they cannot starve downscale.
        _report(autoscaler, in_flight={}, queue_depth=3)
        self.assertEqual(autoscaler._pressure_streak, 0)

    def test_floored_handoff_report_cannot_complete_pressure_streak(self):
        autoscaler = self._ramped_autoscaler(
            adaptive_scale_up={
                'max_scale_up_rate_percentage': 100,
                'scale_up_rate_min_replicas': 50,
                'pressure_observations': 2,
                'hold_seconds': 120,
            })

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0):
            _report(autoscaler, in_flight={}, queue_depth=100)
            _report(autoscaler, in_flight={}, queue_depth=110)
            self.assertEqual(autoscaler._pressure_streak, 1)
            _report(autoscaler,
                    in_flight={},
                    queue_depth=120,
                    pressure_report_is_floored=True)
            self.assertEqual(autoscaler._pressure_streak, 0)
            _report(autoscaler, in_flight={}, queue_depth=130)
            self.assertEqual(autoscaler._pressure_streak, 1)
            self.assertFalse(autoscaler._adaptive_scale_up_active())
            _report(autoscaler, in_flight={}, queue_depth=140)
            self.assertTrue(autoscaler._adaptive_scale_up_active())

    def test_stable_rejection_population_is_not_repeated_pressure(self):
        autoscaler = self._ramped_autoscaler(
            adaptive_scale_up={
                'max_scale_up_rate_percentage': 100,
                'scale_up_rate_min_replicas': 50,
                'pressure_observations': 2,
                'hold_seconds': 120,
            })

        _report(autoscaler, in_flight={}, recent_rejected=10)
        _report(autoscaler, in_flight={}, recent_rejected=11)
        self.assertTrue(autoscaler._pressure_latched)
        self.assertEqual(autoscaler._pressure_streak, 1)
        _report(autoscaler, in_flight={}, recent_rejected=11)
        self.assertEqual(autoscaler._pressure_streak, 0)
        self.assertFalse(autoscaler._adaptive_scale_up_active())

    def test_new_pressure_vetoes_downscale_once_then_requires_new_delta(self):
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=300,
            max_scale_down_rate_percentage=50,
        )
        replicas = [_replica(i + 1) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas})

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock:
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            started_at = autoscaler._downscale_started_at
            self.assertIsNotNone(started_at)
            _report(autoscaler,
                    in_flight={replica.replica_id: 0 for replica in replicas},
                    queue_depth=1)
            clock.return_value = 380.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

            self.assertEqual(autoscaler.target_num_replicas, 100)
            self.assertEqual(autoscaler._downscale_veto_reason, 'queue_depth')
            self.assertEqual(autoscaler._downscale_started_at, started_at)
            self.assertEqual(autoscaler._downscale_elapsed_seconds(), 300.0)
            self.assertEqual(autoscaler.info()['downscale_veto_budget'], 2)

            # The unchanged nonzero queue is demand in the target, but it is
            # not another pressure delta. The already elapsed proof accepts
            # the next wave without waiting through another 300-second window.
            _report(autoscaler,
                    in_flight={replica.replica_id: 0 for replica in replicas},
                    queue_depth=1)
            clock.return_value = 400.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        self.assertEqual(autoscaler.target_num_replicas, 50)

    def test_trickle_pressure_cannot_starve_downscale_forever(self):
        """Regression: trickle deltas add at most two decision ticks."""
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=300,
            max_scale_down_rate_percentage=50,
        )
        replicas = [_replica(i + 1) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False
        idle = {replica.replica_id: 0 for replica in replicas}
        _report(autoscaler, in_flight=idle)

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock:
            # The full delay elapses once, then a trickle delta defers tick 1.
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            started_at = autoscaler._downscale_started_at
            self.assertIsNotNone(started_at)
            _report(autoscaler, in_flight=idle, queue_depth=1)
            clock.return_value = 380.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            self.assertEqual(autoscaler._downscale_veto_streak, 1)
            self.assertEqual(autoscaler._downscale_started_at, started_at)
            self.assertEqual(autoscaler._downscale_elapsed_seconds(), 300.0)

            # A fresh delta defers tick 2 without restarting the delay.
            _report(autoscaler, in_flight=idle, queue_depth=2)
            clock.return_value = 400.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            self.assertEqual(autoscaler._downscale_veto_streak, 2)
            self.assertEqual(autoscaler._downscale_started_at, started_at)
            self.assertEqual(autoscaler._downscale_elapsed_seconds(), 320.0)

            # A third fresh delta cannot defer the accepted wave.
            _report(autoscaler, in_flight=idle, queue_depth=3)
            clock.return_value = 420.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        self.assertEqual(autoscaler.target_num_replicas, 50)
        self.assertEqual(autoscaler._downscale_veto_streak, 0)

    def test_upscale_episode_end_refreshes_veto_budget(self):
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=300,
            max_scale_down_rate_percentage=50,
        )
        replicas = [_replica(i + 1) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False
        idle = {replica.replica_id: 0 for replica in replicas}
        _report(autoscaler, in_flight=idle)

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock, mock.patch.object(
                                   autoscalers.time,
                                   'time',
                                   return_value=1000.0):
            # Exhaust the veto budget with two post-delay decision ticks.
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            _report(autoscaler, in_flight=idle, queue_depth=1)
            clock.return_value = 380.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            _report(autoscaler, in_flight=idle, queue_depth=2)
            clock.return_value = 400.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler._downscale_veto_streak, 2)

            # Genuine burst: raw target rises above the adopted target and
            # ends the downscale episode, refreshing the budget.
            _report(autoscaler, in_flight=idle, queue_depth=500)
            clock.return_value = 420.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertGreater(autoscaler.target_num_replicas, 100)
            self.assertEqual(autoscaler._downscale_veto_streak, 0)

            # The next episode gets a fresh veto: a first elapsed window
            # with a latched delta must hold the fleet again.
            adopted = autoscaler.target_num_replicas
            _report(autoscaler, in_flight=idle)
            clock.return_value = 440.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            _report(autoscaler, in_flight=idle, queue_depth=1)
            clock.return_value = 720.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, adopted)
            self.assertEqual(autoscaler._downscale_veto_streak, 1)

    def test_pressure_reports_coalesce_into_one_tick_deferral(self):
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=300,
            max_scale_down_rate_percentage=50,
        )
        replicas = [_replica(i + 1) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False
        idle = {replica.replica_id: 0 for replica in replicas}
        _report(autoscaler, in_flight=idle)

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock:
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            started_at = autoscaler._downscale_started_at
            _report(autoscaler, in_flight=idle, queue_depth=1)
            _report(autoscaler, in_flight=idle, queue_depth=2)
            _report(autoscaler, in_flight=idle, queue_depth=3)

            clock.return_value = 380.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            self.assertEqual(autoscaler._downscale_veto_streak, 1)
            self.assertEqual(autoscaler._downscale_started_at, started_at)

            # Three positive reports coalesced into the consumed boolean
            # latch. Without another report delta, the next tick downscales.
            clock.return_value = 400.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        self.assertEqual(autoscaler.target_num_replicas, 50)

    def test_stale_arrival_floor_obeys_scale_up_wave(self):
        autoscaler = self._ramped_autoscaler()
        now = time.time()
        autoscaler.collect_request_information({'timestamps': [now - 1] * 100})

        decisions = _decisions(autoscaler, [])

        self.assertEqual(autoscaler._raw_target_num_replicas, 100)
        self.assertEqual(autoscaler.target_num_replicas, 10)
        self.assertEqual(_scale_ups(decisions)[0].target.target_capacity, 10)

    def test_downscale_takes_one_fifty_percent_wave_per_full_delay(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=2 * interval,
            max_scale_down_rate_percentage=50,
        )
        replicas = [_replica(i + 1) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas})

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock:
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            clock.return_value = 119.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            clock.return_value = 120.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 50)

            converged_replicas = replicas[:50]
            _report(autoscaler,
                    in_flight={
                        replica.replica_id: 0 for replica in converged_replicas
                    })
            clock.return_value = 200.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(
                converged_replicas)
            self.assertEqual(autoscaler.target_num_replicas, 50)
            clock.return_value = 220.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(
                converged_replicas)
            self.assertEqual(autoscaler.target_num_replicas, 25)

    def test_rebuilt_target_uses_committed_fleet_as_downscale_baseline(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=2 * interval,
            max_scale_down_rate_percentage=50,
        )
        replicas = [_replica(i + 1) for i in range(100)]
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas})

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock:
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            clock.return_value = 119.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            clock.return_value = 120.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 50)

    def test_downscale_target_does_not_rebound_while_retirement_lags(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=2 * interval,
            max_scale_down_rate_percentage=50,
        )
        replicas = [_replica(i + 1) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas})

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock:
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            clock.return_value = 120.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 50)

            # Actuation is asynchronous, so committed capacity can still
            # report the pre-wave fleet on the next tick. That must not undo
            # the adopted target while the retirement batch catches up.
            clock.return_value = 121.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 50)

    def test_downscale_uses_elapsed_time_when_decision_ticks_are_slow(self):
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=300,
            max_scale_down_rate_percentage=50,
        )
        replicas = [_replica(i + 1) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas})

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock:
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            clock.return_value = 250.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            clock.return_value = 379.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            clock.return_value = 380.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 50)

    def test_fill_origin_does_not_become_restart_or_downscale_demand(self):
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=300,
            max_scale_down_rate_percentage=50,
        )
        demand = [_replica(i + 1) for i in range(60)]
        fill = [_replica(61 + i, reserved_fill=True) for i in range(100)]
        replicas = demand + fill
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas},
                queue_depth=10)

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock:
            # Restart reconstruction protects only the 60 demand-origin
            # slots. The 100 fill slots remain capacity, not orange demand.
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 60)
            self.assertEqual(
                autoscaler._latest_committed_logical_capacity(replicas), 160)

            # After one complete quiet window, the 50% wave is 30 demand
            # slots, not 80 slots derived from the demand+fill fleet.
            clock.return_value = 380.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        self.assertEqual(autoscaler.target_num_replicas, 30)
        self.assertEqual(autoscaler._last_scale_down_allowance, 30)

    def test_all_fill_restart_adopts_only_observed_demand(self):
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=300,
            max_scale_down_rate_percentage=50,
        )
        replicas = [_replica(i + 1, reserved_fill=True) for i in range(159)]
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas},
                queue_depth=17)

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        self.assertEqual(autoscaler._raw_target_num_replicas, 17)
        self.assertEqual(autoscaler.target_num_replicas, 17)
        self.assertFalse(autoscaler._snap_target_on_next_recompute)

    def test_fill_pending_does_not_enlarge_demand_cancellation_budget(self):
        autoscaler = _make_autoscaler(
            knob=1,
            min_replicas=0,
            max_replicas=1000,
            replica_unit='logical',
            max_scale_down_rate_percentage=50,
        )
        pending = serve_state.ReplicaStatus.PENDING
        demand = [_replica(i + 1, status=pending) for i in range(10)]
        fill = [
            _replica(11 + i, status=pending, reserved_fill=True)
            for i in range(100)
        ]
        replicas = demand + fill
        autoscaler.target_num_replicas = 110
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler, in_flight={})

        decisions = _decisions(autoscaler, replicas)
        victims = {
            decision.target.replica_id
            for decision in decisions
            if decision.operator == _SCALE_DOWN
        }

        self.assertEqual(autoscaler.target_num_replicas, 5)
        self.assertEqual(autoscaler._pending_retention_floor, 5)
        self.assertEqual(autoscaler._last_pending_allowance, 5)
        self.assertEqual(len(victims & set(range(1, 11))), 5)
        self.assertEqual(autoscaler._pending_budget_spent, 5)

    def test_fill_capacity_still_sizes_demand_scale_up_wave(self):
        autoscaler = self._ramped_autoscaler()
        replicas = [_replica(i + 1, reserved_fill=True) for i in range(159)]
        autoscaler.target_num_replicas = 17
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas},
                queue_depth=1000)

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        # Total committed capacity still supplies the 20% wave basis. The
        # accounting split prevents paid backfill; it does not pretend that
        # already-live compatible capacity is absent during a real burst.
        self.assertEqual(autoscaler.target_num_replicas, 191)

    def test_downscale_elapsed_window_resets_on_rebound_and_stale_signal(self):
        autoscaler = self._ramped_autoscaler(
            downscale_delay_seconds=300,
            max_scale_down_rate_percentage=50,
        )
        replicas = [_replica(i + 1) for i in range(100)]
        autoscaler.target_num_replicas = 100
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in replicas})

        with mock.patch.object(autoscalers.time,
                               'monotonic',
                               return_value=100.0) as clock:
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            clock.return_value = 250.0
            autoscaler._tick_fresh = False
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            autoscaler._tick_fresh = None
            self.assertIsNone(autoscaler._downscale_started_at)

            clock.return_value = 260.0
            _report(autoscaler,
                    in_flight={replica.replica_id: 0 for replica in replicas})
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            clock.return_value = 539.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)

            _report(autoscaler,
                    in_flight={replica.replica_id: 0 for replica in replicas},
                    queue_depth=100)
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertIsNone(autoscaler._downscale_started_at)

            clock.return_value = 600.0
            _report(autoscaler,
                    in_flight={replica.replica_id: 0 for replica in replicas})
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            clock.return_value = 879.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            clock.return_value = 880.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 50)

    def test_arrival_floor_never_lowers_target(self):
        autoscaler = _make_autoscaler(knob=1.0, min_replicas=1)
        autoscaler.target_num_replicas = 7
        now = time.time()
        autoscaler.collect_request_information({'timestamps': [now - 1] * 2})
        autoscaler._set_target_num_replicas_with_concurrency_logic(
            [_replica(1)])
        self.assertEqual(autoscaler.target_num_replicas, 7)

    def test_snap_waits_for_fresh_data_then_unlocks_scale_down(self):
        autoscaler = _make_autoscaler(knob=1.0, min_replicas=1)
        replicas = [_replica(i) for i in (1, 2, 3)]
        # Stale tick: the one-shot snap must NOT be consumed.
        _decisions(autoscaler, replicas)
        self.assertTrue(autoscaler._snap_target_on_next_recompute)
        # Fresh all-idle report: snap applies, scale-down flows.
        _report(autoscaler, in_flight={1: 0, 2: 0, 3: 0})
        decisions = _decisions(autoscaler, replicas)
        self.assertFalse(autoscaler._snap_target_on_next_recompute)
        self.assertEqual(autoscaler.target_num_replicas, 1)
        self.assertEqual(len(_scale_downs(decisions)), 2)


class TestLogicalReplicaSemantics(unittest.TestCase):
    """Logical targets are GPU slots; physical shapes remain indivisible."""

    def test_cost_rebalance_location_capacity_stays_in_gpu_slots(self):
        autoscaler = _make_autoscaler(knob=2, replica_unit='logical')
        location = mock.Mock()
        with mock.patch.object(autoscaler,
                               '_location_gpu_shape',
                               return_value=('L4', 8)):
            self.assertEqual(
                autoscaler._cost_rebalance_location_capacity(location), 8)

    def test_scale_from_zero_emits_one_capacity_target(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=30,
                                      replica_unit='logical')
        _report(autoscaler, in_flight={}, queue_depth=17, generation=4)
        decisions = _decisions(autoscaler, [])

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].operator, _SCALE_UP)
        self.assertEqual(
            decisions[0].target,
            autoscalers.LogicalScaleTarget(version=1,
                                           reconcile_generation=4,
                                           target_capacity=17))

    def test_published_target_keeps_the_generation_used_by_its_tick(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=30,
                                      replica_unit='logical')
        _report(autoscaler, in_flight={}, queue_depth=5, generation=4)
        _decisions(autoscaler, [])
        self.assertEqual(autoscaler.logical_target_state, (1, 4, 5))

        # A newer sync must not relabel the already computed target. The next
        # decision tick will publish a new target for generation 5.
        _report(autoscaler, in_flight={}, queue_depth=9, generation=5)
        self.assertEqual(autoscaler.logical_target_state, (1, 4, 5))
        _decisions(autoscaler, [])
        self.assertEqual(autoscaler.logical_target_state, (1, 5, 9))

    def test_incomplete_exact_report_revokes_logical_target(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=30,
                                      replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=4)
        _decisions(autoscaler, [])
        self.assertIsNotNone(autoscaler.logical_target_state)

        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=False,
                generation=5)
        _decisions(autoscaler, [])

        self.assertIsNone(autoscaler.logical_target_state)

    def test_existing_eight_slot_backend_emits_one_capacity_target(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=30,
                                      replica_unit='logical')
        backend = _replica(1, gpu_count=8, planned_capacity=8)
        _report(autoscaler,
                in_flight={1: 8},
                queue_depth=9,
                observed_slots={1: 8},
                generation=9)
        decisions = _decisions(autoscaler, [backend])

        self.assertEqual(len(decisions), 1)
        self.assertEqual(
            decisions[0].target,
            autoscalers.LogicalScaleTarget(version=1,
                                           reconcile_generation=9,
                                           target_capacity=17))

    def test_indivisible_eight_slot_overhang_is_stable_at_target_five(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=20,
                                      replica_unit='logical')
        backend = _replica(1, gpu_count=8, planned_capacity=8)
        _report(autoscaler,
                in_flight={1: 0},
                queue_depth=5,
                observed_slots={1: 8})

        self.assertEqual(_decisions(autoscaler, [backend]), [])
        self.assertEqual(autoscaler.target_num_replicas, 5)

    def test_downscale_limits_ready_fleet_and_pending_cohort_independently(
            self):
        autoscaler = _make_autoscaler(
            knob=1,
            min_replicas=0,
            max_replicas=1000,
            replica_unit='logical',
            max_scale_down_rate_percentage=50,
        )
        ready = [_replica(i + 1) for i in range(124)]
        pending = [
            _replica(125 + i, status=serve_state.ReplicaStatus.PENDING)
            for i in range(109)
        ]
        replicas = ready + pending
        autoscaler.target_num_replicas = 233
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in ready},
                queue_depth=129,
                observed_slots={replica.replica_id: 1 for replica in ready})

        decisions = _decisions(autoscaler, replicas)
        pending_downs = [
            decision.target.replica_id
            for decision in decisions
            if decision.operator == _SCALE_DOWN and
            decision.target.replica_id >= 125
        ]

        self.assertEqual(autoscaler.target_num_replicas, 129)
        self.assertEqual(autoscaler._pending_retention_floor, 54)
        self.assertEqual(len(pending_downs), 55)
        self.assertFalse(
            any(decision.target.replica_id < 125 for decision in decisions))

        # Reconciliation can run repeatedly before the first cancellation
        # finishes. Once those victims are marked, the frozen 54-slot floor
        # prevents a second tick from spending another 50% of the remainder.
        for replica in pending:
            if replica.replica_id in pending_downs:
                replica.status_property.is_scale_down = True
        self.assertEqual(_decisions(autoscaler, replicas), [])

    def test_pending_budget_skips_indivisible_victim_that_would_overspend(self):
        autoscaler = _make_autoscaler(
            knob=1,
            min_replicas=0,
            max_replicas=100,
            replica_unit='logical',
            max_scale_down_rate_percentage=50,
        )
        ready = [_replica(i + 1) for i in range(6)]
        pending_one = _replica(7,
                               status=serve_state.ReplicaStatus.PENDING,
                               planned_capacity=1)
        pending_four = _replica(8,
                                gpu_count=4,
                                status=serve_state.ReplicaStatus.PENDING,
                                planned_capacity=4)
        replicas = ready + [pending_one, pending_four]
        autoscaler.target_num_replicas = 11
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler,
                in_flight={replica.replica_id: 0 for replica in ready},
                queue_depth=8,
                observed_slots={replica.replica_id: 1 for replica in ready})

        decisions = _decisions(autoscaler, replicas)

        self.assertEqual(autoscaler._pending_retention_floor, 2)
        self.assertEqual([decision.target.replica_id for decision in decisions],
                         [7])

    def test_downscale_preserves_required_replacement_card_pending(self):
        autoscaler = _make_autoscaler(
            knob=1,
            min_replicas=0,
            max_replicas=100,
            replica_unit='logical',
            max_scale_down_rate_percentage=50,
        )
        autoscaler.set_configured_accelerator_shapes({
            'L4': 1,
            'A100': 1,
        })
        pending = serve_state.ReplicaStatus.PENDING
        old_card = [
            _replica(i + 1, card='L4', status=pending) for i in range(4)
        ]
        replacement_card = [
            _replica(i + 5, card='A100', status=pending) for i in range(3)
        ]
        replicas = old_card + replacement_card
        autoscaler.target_num_replicas = 7
        autoscaler.target_num_replicas_by_accelerator = {'L4': 7}
        autoscaler._snap_target_on_next_recompute = False
        _report(
            autoscaler,
            in_flight={},
            queue_depth=3,
            queued_profiles=[{
                'priority': 50,
                'compatible_accelerators': ['A100'],
                'count': 3,
            }],
            rejected_profiles=[],
            compatibility_complete=True,
        )

        decisions = _decisions(autoscaler, replicas)
        victims = {
            decision.target.replica_id
            for decision in decisions
            if decision.operator == _SCALE_DOWN
        }

        self.assertEqual(autoscaler.target_num_replicas, 3)
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator,
                         {'A100': 3})
        self.assertEqual(victims, {1, 2, 3, 4})
        self.assertTrue(victims.isdisjoint({5, 6, 7}))
        self.assertEqual(autoscaler._pending_retention_floor, 3)
        self.assertEqual(autoscaler._pending_budget_spent, 4)

    def test_scale_down_removes_only_backend_with_safe_coverage(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=20,
                                      replica_unit='logical')
        eight = _replica(1, gpu_count=8, planned_capacity=8)
        four = _replica(2, gpu_count=4, planned_capacity=4)
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0
                },
                queue_depth=8,
                observed_slots={
                    1: 8,
                    2: 4
                },
                generation=6)
        decisions = _decisions(autoscaler, [eight, four])

        downs = [d for d in decisions if d.operator == _SCALE_DOWN]
        self.assertEqual(len(downs), 1)
        self.assertEqual(
            downs[0].target,
            autoscalers.LogicalScaleDownTarget(version=1,
                                               reconcile_generation=6,
                                               target_capacity=8,
                                               replica_id=2))

    def test_cost_rebalance_retirement_keeps_logical_capacity_fence(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=20,
                                      replica_unit='logical')
        autoscaler.cost_rebalance = True
        victim = _replica(1, gpu_count=8, planned_capacity=8)
        replacement = _replica(2, gpu_count=8, planned_capacity=8)
        replacement.cost_rebalance_for_replica_id = 1
        victim.status_property.sky_down_status = None
        replacement.status_property.sky_down_status = None
        _report(autoscaler,
                in_flight={
                    1: 8,
                    2: 0
                },
                observed_slots={
                    1: 8,
                    2: 0
                },
                generation=7)

        decisions = _decisions(autoscaler, [victim, replacement])
        rebalance_downs = [
            decision for decision in decisions if decision.reason ==
            autoscalers.AutoscalerDecisionReason.COST_REBALANCE
        ]
        self.assertEqual(len(rebalance_downs), 1)
        self.assertEqual(
            rebalance_downs[0].target,
            autoscalers.LogicalScaleDownTarget(version=1,
                                               reconcile_generation=7,
                                               target_capacity=8,
                                               replica_id=1))

    def test_retiring_cost_rebalance_replacement_cannot_drain_incumbent(self):
        for retiring_field in ('preempted', 'is_scale_down'):
            with self.subTest(retiring_field=retiring_field):
                autoscaler = _make_autoscaler(knob=1,
                                              max_replicas=20,
                                              replica_unit='logical')
                autoscaler.cost_rebalance = True
                victim = _replica(1, gpu_count=8, planned_capacity=8)
                replacement = _replica(2, gpu_count=8, planned_capacity=8)
                replacement.cost_rebalance_for_replica_id = 1
                victim.status_property.sky_down_status = None
                replacement.status_property.sky_down_status = None
                setattr(replacement.status_property, retiring_field, True)

                with mock.patch.object(autoscaler,
                                       '_cost_rebalance_location_is_compatible',
                                       return_value=True):
                    decisions = autoscaler._generate_cost_rebalance_decisions(
                        [victim, replacement], [])

                self.assertNotIn(1, [
                    decision.target
                    for decision in decisions
                    if isinstance(decision.target, int)
                ])

    def test_cost_rebalance_cross_card_pair_retires_replacement(self):
        autoscaler = _make_autoscaler(knob=1,
                                      min_replicas=1,
                                      max_replicas=2,
                                      min_replicas_by_accelerator={'A100': 1},
                                      replica_unit='logical')
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        autoscaler.cost_rebalance = True
        victim = _replica(1, card='A100', planned_capacity=1)
        replacement = _replica(2, card='L4', planned_capacity=1)
        victim.get_spot_location.return_value = types.SimpleNamespace(
            accelerators={'A100': 1})
        replacement.get_spot_location.return_value = types.SimpleNamespace(
            accelerators={'L4': 1})
        replacement.cost_rebalance_for_replica_id = 1
        victim.status_property.sky_down_status = None
        replacement.status_property.sky_down_status = None
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0
                },
                observed_slots={
                    1: 1,
                    2: 1
                },
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True,
                generation=8)

        decisions = _decisions(autoscaler, [victim, replacement])
        rebalance_downs = [
            decision for decision in decisions if decision.reason ==
            autoscalers.AutoscalerDecisionReason.COST_REBALANCE
        ]
        self.assertEqual(len(rebalance_downs), 1)
        self.assertEqual(rebalance_downs[0].target.replica_id, 2)
        self.assertEqual(
            dict(rebalance_downs[0].target.target_capacity_by_accelerator),
            {'A100': 1})

    def test_capacities_eight_and_four_are_stable_at_target_nine(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=20,
                                      replica_unit='logical')
        backends = [
            _replica(1, gpu_count=8, planned_capacity=8),
            _replica(2, gpu_count=4, planned_capacity=4),
        ]
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0
                },
                queue_depth=9,
                observed_slots={
                    1: 8,
                    2: 4
                })

        self.assertEqual(_decisions(autoscaler, backends), [])

    def test_unknown_capacity_uses_planned_width_for_launch_suppression(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=20,
                                      replica_unit='logical')
        backend = _replica(1, gpu_count=8, planned_capacity=8)
        _report(autoscaler,
                in_flight={1: 0},
                unknown=(1,),
                observed_slots={1: 0})

        self.assertEqual(_decisions(autoscaler, [backend]), [])
        self.assertEqual(autoscaler.get_ready_replica_capacity(backend), 8)
        self.assertEqual(autoscaler._ready_capacity(backend), 0)
        self.assertEqual(autoscaler._committed_capacity(backend), 8)

    def test_persistent_unknown_capacity_emits_one_bounded_replacement_wave(
            self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=8,
                                      replica_unit='logical')
        original = _replica(1, gpu_count=8, planned_capacity=8)
        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            _report(autoscaler,
                    in_flight={1: 0},
                    unknown=(1,),
                    observed_slots={1: 0},
                    generation=1)
            self.assertEqual(_decisions(autoscaler, [original]), [])

        deadline = (100.0 +
                    constants.LOGICAL_UNKNOWN_CAPACITY_REPLACEMENT_SECONDS + 1)
        with mock.patch.object(autoscalers.time, 'time', return_value=deadline):
            decisions = _decisions(autoscaler, [original])

        self.assertEqual(decisions, [
            autoscalers.AutoscalerDecision(
                _SCALE_UP,
                autoscalers.LogicalScaleTarget(
                    version=1,
                    reconcile_generation=1,
                    target_capacity=8,
                    replace_unknown_replica_ids=(1,)))
        ])

        replacement = _replica(2, gpu_count=8, planned_capacity=8)
        replacement.unknown_capacity_replacement = True
        with mock.patch.object(autoscalers.time,
                               'time',
                               return_value=deadline + 1):
            _report(autoscaler,
                    in_flight={
                        1: 0,
                        2: 0
                    },
                    unknown=(1, 2),
                    observed_slots={
                        1: 0,
                        2: 0
                    },
                    generation=2)
            # The replacement's unknown-work floor overlaps the original;
            # it does not recursively raise the target or authorize a second
            # replacement wave, including at max_replicas.
            self.assertEqual(_decisions(autoscaler, [original, replacement]),
                             [])

    def test_unknown_replacement_stays_protected_when_original_recovers(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=8,
                                      replica_unit='logical')
        original = _replica(1, gpu_count=8, planned_capacity=8)
        replacement = _replica(2, gpu_count=8, planned_capacity=8)
        replacement.unknown_capacity_replacement = True
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0
                },
                unknown=(2,),
                observed_slots={1: 8},
                generation=3)

        # Target remains 8 from the replacement's possible work and the only
        # proven-ready backend cannot be retired underneath it.
        self.assertEqual(_decisions(autoscaler, [original, replacement]), [])

    def test_recovered_original_retires_idle_zero_capacity_replacement(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=8,
                                      replica_unit='logical')
        original = _replica(1, gpu_count=8, planned_capacity=8)
        replacement = _replica(2, gpu_count=8, planned_capacity=8)
        replacement.unknown_capacity_replacement = True
        _report(autoscaler,
                in_flight={
                    1: 0,
                    2: 0
                },
                queue_depth=8,
                observed_slots={
                    1: 8,
                    2: 0
                },
                generation=4)

        decisions = _decisions(autoscaler, [original, replacement])

        self.assertEqual(
            [target.replica_id for target in _scale_downs(decisions)], [2])

    def test_positive_replacement_retires_timed_out_zero_capacity_original(
            self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=8,
                                      replica_unit='logical')
        original = _replica(1, gpu_count=8, planned_capacity=8)
        replacement = _replica(2, gpu_count=8, planned_capacity=8)
        replacement.unknown_capacity_replacement = True
        timeout = constants.LOGICAL_UNKNOWN_CAPACITY_REPLACEMENT_SECONDS
        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            _report(autoscaler,
                    in_flight={1: 0},
                    queue_depth=8,
                    observed_slots={1: 0},
                    generation=1)
            self.assertEqual(_decisions(autoscaler, [original]), [])
        with mock.patch.object(autoscalers.time,
                               'time',
                               return_value=100.0 + timeout + 1):
            _report(autoscaler,
                    in_flight={
                        1: 0,
                        2: 0
                    },
                    queue_depth=8,
                    observed_slots={
                        1: 0,
                        2: 8
                    },
                    generation=2)
            decisions = _decisions(autoscaler, [original, replacement])

        self.assertEqual(
            [target.replica_id for target in _scale_downs(decisions)], [1])

    def test_valid_zero_capacity_emits_only_one_bounded_replacement_wave(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=8,
                                      replica_unit='logical')
        original = _replica(1, gpu_count=8, planned_capacity=8)
        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            _report(autoscaler,
                    in_flight={1: 0},
                    queue_depth=8,
                    observed_slots={1: 0},
                    generation=1)
            self.assertEqual(_decisions(autoscaler, [original]), [])

        timeout = constants.LOGICAL_UNKNOWN_CAPACITY_REPLACEMENT_SECONDS
        with mock.patch.object(autoscalers.time,
                               'time',
                               return_value=100.0 + timeout + 1):
            _report(autoscaler,
                    in_flight={1: 0},
                    queue_depth=8,
                    observed_slots={1: 0},
                    generation=2)
            decisions = _decisions(autoscaler, [original])
        self.assertEqual(decisions[0].target.replace_unknown_replica_ids, (1,))

        replacement = _replica(2, gpu_count=8, planned_capacity=8)
        replacement.unknown_capacity_replacement = True
        for generation in range(3, 7):
            now = 100.0 + generation * (timeout + 1)
            with mock.patch.object(autoscalers.time, 'time', return_value=now):
                _report(autoscaler,
                        in_flight={
                            1: 0,
                            2: 0
                        },
                        queue_depth=8,
                        observed_slots={
                            1: 0,
                            2: 0
                        },
                        generation=generation)
                self.assertEqual(
                    _decisions(autoscaler, [original, replacement]), [])

    def test_rollout_overlap_unknown_never_starts_replacement_timer(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=8,
                                      replica_unit='logical')
        backend = _replica(1, gpu_count=8, planned_capacity=8)
        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            _report(autoscaler,
                    in_flight={1: 0},
                    unknown=(1,),
                    unknown_capacity=(),
                    observed_slots={1: 0},
                    generation=1)

        deadline = (100.0 +
                    constants.LOGICAL_UNKNOWN_CAPACITY_REPLACEMENT_SECONDS + 1)
        with mock.patch.object(autoscalers.time, 'time', return_value=deadline):
            self.assertEqual(_decisions(autoscaler, [backend]), [])

    def test_recovered_replacement_is_eligible_in_a_later_outage(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=8,
                                      replica_unit='logical')
        recovered = _replica(2, gpu_count=8, planned_capacity=8)
        # ReplicaManager clears this incident marker after a known sample.
        recovered.unknown_capacity_replacement = False
        with mock.patch.object(autoscalers.time, 'time', return_value=200.0):
            _report(autoscaler,
                    in_flight={2: 0},
                    unknown=(2,),
                    observed_slots={2: 0},
                    generation=4)
            self.assertEqual(_decisions(autoscaler, [recovered]), [])
        deadline = (200.0 +
                    constants.LOGICAL_UNKNOWN_CAPACITY_REPLACEMENT_SECONDS + 1)
        with mock.patch.object(autoscalers.time, 'time', return_value=deadline):
            decisions = _decisions(autoscaler, [recovered])

        self.assertEqual(decisions[0].target.replace_unknown_replica_ids, (2,))

    def test_retiring_backend_does_not_suppress_replacement_capacity(self):
        autoscaler = _make_autoscaler(knob=1,
                                      max_replicas=20,
                                      replica_unit='logical')
        retiring = _replica(1,
                            gpu_count=8,
                            status=serve_state.ReplicaStatus.SHUTTING_DOWN,
                            planned_capacity=8)
        retiring.status_property.is_scale_down = True
        live = _replica(2, gpu_count=4, planned_capacity=4)
        _report(autoscaler,
                in_flight={2: 0},
                queue_depth=8,
                observed_slots={2: 4},
                generation=12)

        self.assertEqual(_decisions(autoscaler, [retiring, live]), [
            autoscalers.AutoscalerDecision(
                _SCALE_UP,
                autoscalers.LogicalScaleTarget(
                    version=1, reconcile_generation=12, target_capacity=8))
        ])


class TestDrainAwareDownscale(unittest.TestCase):
    """READY victims require fresh in_flight == 0; missing entry = busy."""

    def test_unknown_async_replica_is_busy_despite_envelope_zero(self):
        autoscaler = _make_autoscaler(knob=5.0, min_replicas=1)
        replicas = [_replica(1), _replica(2)]
        _report(autoscaler, in_flight={1: 0, 2: 0}, unknown=(2,))
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 1)
        self.assertEqual(_scale_downs(decisions), [1])

    def test_only_idle_ready_replicas_are_victims(self):
        # Capacity 5/replica, 5 outstanding -> target 1 of 3 replicas.
        # Replica 1 idle, replica 2 busy, replica 3 MISSING from the
        # report (=> busy): only replica 1 may be killed; the second
        # requested kill waits.
        autoscaler = _make_autoscaler(knob=5.0, min_replicas=1)
        replicas = [_replica(i) for i in (1, 2, 3)]
        _report(autoscaler, in_flight={1: 0, 2: 5})
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 1)
        self.assertEqual(_scale_downs(decisions), [1])

    def test_all_busy_means_no_scale_down(self):
        autoscaler = _make_autoscaler(knob=5.0, min_replicas=1)
        replicas = [_replica(i) for i in (1, 2)]
        _report(autoscaler, in_flight={1: 2, 2: 3})
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(_scale_downs(decisions), [])

    def test_pending_upscale_does_not_emit_opposite_scale_down(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = _make_autoscaler(
            knob=1.0,
            min_replicas=1,
            upscale_delay_seconds=3 * interval,
        )
        replicas = [_replica(i) for i in (1, 2, 3)]
        autoscaler._snap_target_on_next_recompute = False
        _report(autoscaler, in_flight={1: 1, 2: 0, 3: 0}, rejected=2)

        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(autoscaler.target_num_replicas, 1)
        self.assertEqual(autoscaler.upscale_counter, 1)
        self.assertTrue(autoscaler._upscale_pending)
        self.assertEqual(_scale_downs(decisions), [])

    def test_equal_capacity_victims_shed_paid_before_zero_cost(self):
        # Cost tiebreak (mirrors the instance-aware ordering): among
        # idle victims of equal status and capacity, the EXPENSIVE
        # replica dies first -- otherwise the routine reclaim cycle
        # (evict fill -> demand relaunches on paid spot -> fill returns
        # zero-cost with newest ids -> demand drops) always kills the
        # newest (zero-cost) replicas and settles into paying for spot
        # while free reserved slots idle.
        autoscaler = _make_autoscaler(knob=1.0, min_replicas=1)
        paid = _replica(1)
        paid.handle.return_value.launched_resources.get_cost.return_value = 2.0
        free = _replica(2)
        free.handle.return_value.launched_resources.get_cost.return_value = 0.0
        # Outstanding 1 -> target 1 -> one victim; both idle. Without
        # the cost key the -replica_id tiebreak would kill the newest
        # (id 2, the zero-cost one).
        _report(autoscaler, in_flight={1: 0, 2: 0})
        decisions = _decisions(autoscaler, [paid, free])
        self.assertEqual(_scale_downs(decisions), [1])

    def test_not_ready_missing_from_report_is_busy(self):
        # A NOT_READY replica WAS serving: for async fast-ack work the
        # LB probe only covers the routable set, so a blipped replica's
        # running jobs may be unreported entirely. Missing => busy, same
        # as READY. (PROVISIONING-family replicas stay idle-when-missing
        # -- they never served.)
        autoscaler = _make_autoscaler(knob=5.0, min_replicas=1)
        blipped = _replica(2, status=serve_state.ReplicaStatus.NOT_READY)
        replicas = [_replica(1), blipped]
        _report(autoscaler, in_flight={1: 0})
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(_scale_downs(decisions), [1])

    def test_probe_blipped_replica_with_work_is_not_a_victim(self):
        # A replica demoted from READY mid-job (probe blip) still shows
        # in-flight work via the controller's sticky url translation; it
        # must not inherit the non-READY kill-first eligibility.
        autoscaler = _make_autoscaler(knob=5.0, min_replicas=1)
        blipped = _replica(2, status=serve_state.ReplicaStatus.NOT_READY)
        replicas = [_replica(1), blipped]
        _report(autoscaler, in_flight={1: 0, 2: 5})
        decisions = _decisions(autoscaler, replicas)
        self.assertEqual(_scale_downs(decisions), [1])

    def test_non_ready_replicas_keep_kill_first_preference(self):
        # A PROVISIONING replica carries no jobs: it is eligible without
        # an in-flight entry and dies before the idle READY one.
        autoscaler = _make_autoscaler(knob=5.0, min_replicas=1)
        provisioning = _replica(3,
                                status=serve_state.ReplicaStatus.PROVISIONING)
        replicas = [_replica(1), _replica(2), provisioning]
        # Outstanding 5 -> target 1; replica 1 idle, replica 2 busy.
        _report(autoscaler, in_flight={1: 0, 2: 5})
        decisions = autoscaler.generate_scaling_decisions(replicas, [1])
        down_ids = [d.target for d in decisions if d.operator == _SCALE_DOWN]
        # Both eligible victims selected, provisioning first.
        self.assertEqual(down_ids, [3, 1])


class TestRollingDrain(unittest.TestCase):
    """Capacity-aware old-version retirement in concurrency units."""

    def _mid_update(self, knob=1.0, target=2):
        autoscaler = _make_autoscaler(knob=knob, min_replicas=1)
        autoscaler.update_version(2, _spec(knob=knob, min_replicas=1),
                                  serve_utils.UpdateMode.ROLLING)
        autoscaler.target_num_replicas = target
        return autoscaler

    def test_pending_upscale_keeps_old_provisioning_capacity(self):
        interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
        autoscaler = _make_autoscaler(
            knob=1.0,
            min_replicas=1,
            upscale_delay_seconds=3 * interval,
        )
        autoscaler.update_version(
            2,
            _spec(
                knob=1.0,
                min_replicas=1,
                upscale_delay_seconds=3 * interval,
            ),
            serve_utils.UpdateMode.ROLLING,
        )
        autoscaler._snap_target_on_next_recompute = False
        old = [
            _replica(1, version=1),
            _replica(
                2,
                version=1,
                status=serve_state.ReplicaStatus.PROVISIONING,
            ),
            _replica(
                3,
                version=1,
                status=serve_state.ReplicaStatus.PROVISIONING,
            ),
        ]
        _report(autoscaler, in_flight={1: 1}, rejected=2)

        decisions = _decisions(autoscaler, old, active_versions=(1,))
        self.assertTrue(autoscaler._upscale_pending)
        self.assertEqual(_scale_downs(decisions), [])
        self.assertEqual(len(_scale_ups(decisions)), 1)

    def test_keeps_old_capacity_covering_shortfall_prefers_idle_victims(self):
        autoscaler = self._mid_update(target=2)
        old = [_replica(i, version=1) for i in (1, 2, 3)]
        new_ready = _replica(4, version=2)
        # Outstanding 3 (2 in-flight + 1 queued); ready latest covers 1
        # -> shortfall 2 -> keep two old replicas. Busy replica 1 is
        # preferentially KEPT (killing it wastes a job), so the idle
        # replica 3 is the victim.
        _report(autoscaler, in_flight={1: 1, 2: 0, 3: 0, 4: 1}, queue_depth=1)
        retired = autoscaler._select_outdated_replicas_to_scale_down(
            old + [new_ready], [1, 2])
        self.assertEqual(retired, [3])

    def test_all_idle_old_retired_once_enough_latest_ready(self):
        autoscaler = self._mid_update(target=1)
        old = [_replica(i, version=1) for i in (1, 2, 3)]
        new_ready = _replica(4, version=2)
        _report(autoscaler, in_flight={1: 0, 2: 0, 3: 0, 4: 1})
        retired = autoscaler._select_outdated_replicas_to_scale_down(
            old + [new_ready], [1, 2])
        self.assertEqual(sorted(retired), [1, 2, 3])

    def test_preempted_latest_physical_replica_cannot_cover_rolling_drain(self):
        autoscaler = self._mid_update(target=1)
        old = _replica(1, version=1)
        preempted = _replica(2, version=2)
        preempted.status_property.preempted = True
        _report(autoscaler, in_flight={1: 0, 2: 0})

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            [old, preempted], [1, 2])

        self.assertEqual(retired, [])

    def test_terminal_branch_never_retires_busy_old_replicas(self):
        # Enough ready latest replicas is NOT a license to abort
        # in-progress hour-long jobs: busy old replicas (including
        # READY ones missing from the report) wait for a later tick.
        autoscaler = self._mid_update(target=1)
        busy = _replica(1, version=1)
        idle = _replica(2, version=1)
        missing = _replica(3, version=1)  # READY, not in report => busy
        new_ready = _replica(4, version=2)
        _report(autoscaler, in_flight={1: 1, 2: 0, 4: 1})
        retired = autoscaler._select_outdated_replicas_to_scale_down(
            [busy, idle, missing, new_ready], [1, 2])
        self.assertEqual(retired, [2])

    def test_shortfall_branch_never_retires_busy_old_replicas(self):
        # Even when coverage math says the busy replica is surplus, it
        # is kept: a probe-blipped non-READY old replica with reported
        # work is protected the same way.
        autoscaler = self._mid_update(target=2)
        blipped = _replica(1,
                           version=1,
                           status=serve_state.ReplicaStatus.NOT_READY)
        idle = _replica(2, version=1)
        new_ready = _replica(3, version=2)
        # Outstanding 1, ready latest covers it -> shortfall <= 0, floor
        # keeps one old; the blipped-busy replica must not be the one
        # retired to satisfy the count.
        _report(autoscaler, in_flight={1: 1, 2: 0, 3: 1})
        retired = autoscaler._select_outdated_replicas_to_scale_down(
            [blipped, idle, new_ready], [1, 2])
        self.assertNotIn(1, retired)

    def test_unknown_old_replica_is_kept_before_idle_coverage(self):
        autoscaler = self._mid_update(target=2)
        idle = _replica(1, version=1)
        unknown = _replica(2, version=1)
        new_ready = _replica(3, version=2)
        _report(autoscaler, in_flight={1: 0, 2: 0, 3: 1}, unknown=(2,))
        retired = autoscaler._select_outdated_replicas_to_scale_down(
            [idle, unknown, new_ready], [1, 2])
        # Busy-first coverage must keep the unknown replica and retire the
        # truly idle one; adding unknowns only in the final safety pass would
        # retain both and stall the rollout.
        self.assertEqual(retired, [1])

    def test_count_floor_keeps_standby_on_zero_demand(self):
        # Zero outstanding work but no ready latest replica: the
        # base-class count floor (target - ready_new) keeps the standby.
        autoscaler = self._mid_update(target=1)
        old = [_replica(1, version=1)]
        _report(autoscaler, in_flight={1: 0})
        self.assertEqual(
            autoscaler._select_outdated_replicas_to_scale_down(old, [1]), [])

    def _logical_mid_update(self,
                            target,
                            raw_target,
                            update_mode=serve_utils.UpdateMode.ROLLING):
        autoscaler = _make_autoscaler(knob=1.0,
                                      min_replicas=1,
                                      max_replicas=1000,
                                      replica_unit='logical')
        autoscaler.update_version(
            2,
            _spec(knob=1.0,
                  min_replicas=1,
                  max_replicas=1000,
                  replica_unit='logical'), update_mode)
        autoscaler.target_num_replicas = target
        autoscaler._raw_target_num_replicas = raw_target
        return autoscaler

    def test_logical_rollout_retires_before_latest_reaches_target(self):
        autoscaler = self._logical_mid_update(target=40, raw_target=40)
        autoscaler._upscale_pending = True
        old = [_replica(i, version=1) for i in range(1, 41)]
        new_ready = _replica(101, version=2, planned_capacity=5)
        _report(autoscaler,
                in_flight={
                    **{
                        info.replica_id: 0 for info in old
                    },
                    101: 0,
                },
                observed_slots={101: 5})

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            old + [new_ready], [1, 2])

        # Five observed new slots permit five conservative old-backend
        # retirements. The rollout does not wait for all 40 new slots or for
        # the adopted scale-up wave to catch raw demand.
        self.assertEqual(len(retired), 5)
        self.assertTrue(
            set(retired).issubset({info.replica_id for info in old}))

    def test_logical_exact_card_rollout_retires_matching_partial_coverage(self):
        autoscaler = self._logical_mid_update(target=40, raw_target=40)
        autoscaler.set_configured_accelerator_shapes({'L4': 1})
        autoscaler.target_num_replicas_by_accelerator = {'L4': 40}
        old = [_replica(i, version=1, card='L4') for i in range(1, 41)]
        new_ready = _replica(101, version=2, card='L4', planned_capacity=5)
        _report(autoscaler,
                in_flight={
                    **{
                        info.replica_id: 0 for info in old
                    },
                    101: 0,
                },
                observed_slots={101: 5},
                compatibility_profiles=[{
                    'priority': 50,
                    'compatible_accelerators': ['L4'],
                    'count': 40,
                }],
                compatibility_complete=True)

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            old + [new_ready], [1, 2])

        self.assertEqual(len(retired), 5)
        self.assertTrue(
            set(retired).issubset({info.replica_id for info in old}))

    def test_logical_exact_card_rollout_keeps_uncovered_old_card(self):
        autoscaler = self._logical_mid_update(target=40, raw_target=40)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        autoscaler.target_num_replicas_by_accelerator = {'L4': 40}
        old = [_replica(i, version=1, card='L4') for i in range(1, 41)]
        new_ready = _replica(101, version=2, card='A100', planned_capacity=5)
        _report(autoscaler,
                in_flight={
                    **{
                        info.replica_id: 0 for info in old
                    },
                    101: 0,
                },
                observed_slots={101: 5},
                compatibility_profiles=[{
                    'priority': 50,
                    'compatible_accelerators': ['L4'],
                    'count': 40,
                }],
                compatibility_complete=True)

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            old + [new_ready], [1, 2])

        self.assertEqual(retired, [])

    def test_logical_exact_card_rollout_retires_removed_old_card(self):
        autoscaler = self._logical_mid_update(target=40, raw_target=40)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        autoscaler.target_num_replicas_by_accelerator = {'A100': 40}
        old = [_replica(i, version=1, card='L4') for i in range(1, 41)]
        new_ready = _replica(101, version=2, card='A100', planned_capacity=5)
        _report(autoscaler,
                in_flight={
                    **{
                        info.replica_id: 0 for info in old
                    },
                    101: 0,
                },
                observed_slots={101: 5},
                compatibility_profiles=[{
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': 40,
                }],
                compatibility_complete=True)

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            old + [new_ready], [1, 2])

        self.assertEqual(len(retired), 5)
        self.assertTrue(
            set(retired).issubset({info.replica_id for info in old}))

    def test_preempted_latest_logical_capacity_cannot_cover_rolling_drain(self):
        autoscaler = self._logical_mid_update(target=5, raw_target=5)
        old = [_replica(i, version=1) for i in range(1, 6)]
        preempted = _replica(101, version=2, planned_capacity=5)
        preempted.status_property.preempted = True
        _report(autoscaler,
                in_flight={
                    **{
                        info.replica_id: 0 for info in old
                    },
                    101: 0,
                },
                observed_slots={101: 5})

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            [*old, preempted], [1, 2])

        self.assertEqual(retired, [])

    def test_logical_blue_green_waits_for_complete_latest_target(self):
        autoscaler = self._logical_mid_update(
            target=40,
            raw_target=40,
            update_mode=serve_utils.UpdateMode.BLUE_GREEN)
        old = [_replica(i, version=1) for i in range(1, 41)]
        new_ready = _replica(101, version=2, planned_capacity=5)
        _report(autoscaler,
                in_flight={
                    **{
                        info.replica_id: 0 for info in old
                    },
                    101: 0,
                },
                observed_slots={101: 5})

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            old + [new_ready], [1, 2])

        self.assertEqual(retired, [])

    def test_logical_rollout_batches_proven_old_excess(self):
        autoscaler = self._logical_mid_update(target=40, raw_target=40)
        old = [_replica(i, version=1) for i in range(1, 101)]
        new_ready = _replica(101, version=2, planned_capacity=5)
        _report(autoscaler,
                in_flight={
                    **{
                        info.replica_id: 0 for info in old
                    },
                    101: 0,
                },
                observed_slots={101: 5})

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            old + [new_ready], [1, 2])

        # Sixty-five old READY backends are proven excess, but one decision
        # tick removes at most the bounded physical batch.
        self.assertEqual(len(retired), 20)

    def test_logical_rollout_preserves_raw_demand_and_drops_nonready(self):
        autoscaler = self._logical_mid_update(target=10, raw_target=40)
        ready_old = [_replica(i, version=1) for i in range(1, 36)]
        nonready_old = [
            _replica(i,
                     version=1,
                     status=serve_state.ReplicaStatus.PROVISIONING)
            for i in range(36, 41)
        ]
        new_ready = _replica(101, version=2, planned_capacity=5)
        _report(autoscaler,
                in_flight={
                    **{
                        info.replica_id: 0 for info in ready_old
                    },
                    101: 0,
                },
                observed_slots={101: 5})

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            ready_old + nonready_old + [new_ready], [1, 2])

        # The adopted rollout ramp is only 10, but raw demand keeps all 35
        # READY old backends as the coverage floor. Never-served old launches
        # add no coverage and are retired first.
        self.assertEqual(set(retired),
                         {info.replica_id for info in nonready_old})

    def test_logical_rollout_protects_busy_and_unknown_old_backends(self):
        autoscaler = self._logical_mid_update(target=1, raw_target=1)
        old = [_replica(i, version=1) for i in range(1, 31)]
        new_ready = _replica(101, version=2)
        _report(autoscaler,
                in_flight={
                    **{
                        info.replica_id: 0 for info in old
                    },
                    1: 1,
                    101: 0,
                },
                unknown=(2,),
                observed_slots={101: 1})

        retired = autoscaler._select_outdated_replicas_to_scale_down(
            old + [new_ready], [1, 2])

        self.assertEqual(len(retired), 20)
        self.assertNotIn(1, retired)
        self.assertNotIn(2, retired)


class TestUpdateVersion(unittest.TestCase):
    """Version updates re-read the knob; stale versions are inert."""

    def test_new_version_updates_knob_and_arms_snap(self):
        autoscaler = _make_autoscaler(knob=1.0)
        autoscaler._snap_target_on_next_recompute = False
        autoscaler.update_version(2, _spec(knob=3.0),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        self.assertEqual(autoscaler.latest_version, 2)
        self.assertEqual(autoscaler.target_concurrency_per_replica, 3.0)
        self.assertTrue(autoscaler._snap_target_on_next_recompute)

    def test_stale_version_does_not_mutate_state(self):
        autoscaler = _make_autoscaler(knob=1.0)
        autoscaler._snap_target_on_next_recompute = False
        autoscaler.update_version(1, _spec(knob=9.0),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        self.assertEqual(autoscaler.latest_version, 1)
        self.assertEqual(autoscaler.target_concurrency_per_replica, 1.0)
        self.assertFalse(autoscaler._snap_target_on_next_recompute)

    def test_update_reclips_target_to_new_bounds(self):
        autoscaler = _make_autoscaler(knob=1.0, max_replicas=20)
        autoscaler.target_num_replicas = 12
        autoscaler.update_version(2, _spec(knob=1.0, max_replicas=5),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        self.assertEqual(autoscaler.target_num_replicas, 5)

    def test_update_resets_logical_downscale_elapsed_window(self):
        autoscaler = _make_autoscaler(knob=1.0,
                                      replica_unit='logical',
                                      downscale_delay_seconds=300)
        autoscaler._downscale_started_at = 123.0
        autoscaler.downscale_counter = 4

        autoscaler.update_version(
            2,
            _spec(knob=1.0, replica_unit='logical',
                  downscale_delay_seconds=300), serve_utils.DEFAULT_UPDATE_MODE)

        self.assertIsNone(autoscaler._downscale_started_at)
        self.assertEqual(autoscaler.downscale_counter, 0)

    def test_ramped_update_does_not_inherit_old_version_target(self):
        autoscaler = _make_autoscaler(
            knob=1.0,
            min_replicas=1,
            max_replicas=1000,
            replica_unit='logical',
            max_scale_up_rate_percentage=20,
            scale_up_rate_min_replicas=10,
            scale_up_rate_period_seconds=60,
        )
        autoscaler.target_num_replicas = 1000
        autoscaler._last_scale_up_wave_at = 50.0
        autoscaler._logical_scale_up_wave_ceiling = 1010

        autoscaler.update_version(
            2,
            _spec(knob=1.0,
                  min_replicas=1,
                  max_replicas=1000,
                  replica_unit='logical',
                  max_scale_up_rate_percentage=20,
                  scale_up_rate_min_replicas=10,
                  scale_up_rate_period_seconds=60),
            serve_utils.DEFAULT_UPDATE_MODE)

        self.assertEqual(autoscaler.target_num_replicas, 0)
        self.assertIsNone(autoscaler._logical_scale_up_wave_ceiling)
        _report(autoscaler, in_flight={}, queue_depth=1000)
        autoscaler._last_scale_up_wave_at = None
        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic([])
        self.assertEqual(autoscaler._raw_target_num_replicas, 1000)
        self.assertEqual(autoscaler.target_num_replicas, 10)

    def test_ramped_update_completes_ramp_to_raw_demand_target(self):
        # A wave-limited rolling update must (a) bound the first wave
        # instead of inheriting the old version's large target and
        # (b) terminate: as committed capacity catches up each wave, the
        # target strictly increases until it reaches the raw demand target.
        ramp_kwargs = dict(knob=1.0,
                           min_replicas=1,
                           max_replicas=1000,
                           replica_unit='logical',
                           max_scale_up_rate_percentage=20,
                           scale_up_rate_min_replicas=10,
                           scale_up_rate_period_seconds=60)
        autoscaler = _make_autoscaler(**ramp_kwargs)
        autoscaler.target_num_replicas = 1000
        autoscaler.update_version(2, _spec(**ramp_kwargs),
                                  serve_utils.DEFAULT_UPDATE_MODE)

        _report(autoscaler, in_flight={}, queue_depth=1000)
        now = 100.0
        with mock.patch.object(autoscalers.time, 'time', return_value=now):
            autoscaler._set_target_num_replicas_with_concurrency_logic([])
        # First wave is bounded, not the inherited 1000.
        self.assertEqual(autoscaler._raw_target_num_replicas, 1000)
        self.assertEqual(autoscaler.target_num_replicas, 10)

        # Successive waves: commit the granted capacity, advance the wave
        # timer, and recompute. 20% growth from 10 reaches 1000 within
        # 22 waves (10, 20, ..., 50, then x1.2 per wave).
        for _ in range(21):
            previous_target = autoscaler.target_num_replicas
            replicas = [
                _replica(i + 1, version=2)
                for i in range(autoscaler.target_num_replicas)
            ]
            _report(autoscaler, in_flight={}, queue_depth=1000)
            now += 60.0
            with mock.patch.object(autoscalers.time, 'time', return_value=now):
                autoscaler._set_target_num_replicas_with_concurrency_logic(
                    replicas)
            self.assertGreater(autoscaler.target_num_replicas, previous_target)
            if autoscaler.target_num_replicas == 1000:
                break
        self.assertEqual(autoscaler.target_num_replicas, 1000)

    def test_version_and_policy_downgrade_clear_catalog_atomically(self):
        autoscaler = _make_autoscaler(knob=1.0)
        autoscaler.set_configured_accelerator_shapes({'L4': 1, 'A100': 1})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[{
                    'priority': 20,
                    'compatible_accelerators': ['A100'],
                    'count': 1,
                }],
                rejected_profiles=[],
                compatibility_complete=True)
        downgraded = _spec(knob=1.0)
        downgraded.load_balancing_policy = 'least_load'

        autoscaler.update_version_and_accelerator_shapes(
            2, downgraded, serve_utils.DEFAULT_UPDATE_MODE, {})

        self.assertEqual(autoscaler.latest_version, 2)
        self.assertEqual(autoscaler.configured_accelerator_shapes, {})
        self.assertEqual(autoscaler.target_num_replicas_by_accelerator, {})
        self.assertFalse(autoscaler._compatibility_demand_complete)
        self.assertEqual(autoscaler.queued_compatibility_profiles, [])

    def test_catalog_change_waits_for_new_compatibility_report(self):
        autoscaler = _make_autoscaler(knob=1.0)
        autoscaler.set_configured_accelerator_shapes({'A100': 1})
        _report(autoscaler,
                in_flight={},
                queue_depth=1,
                queued_profiles=[{
                    'priority': 20,
                    'compatible_accelerators': ['A100'],
                    'count': 1,
                }],
                rejected_profiles=[],
                compatibility_complete=True)
        initial = _decisions(autoscaler, [])
        self.assertEqual(initial[0].target, {'accelerators': {'A100': 1}})
        updated = _spec(knob=1.0)
        updated.load_balancing_policy = 'instance_aware_least_load'

        autoscaler.update_version_and_accelerator_shapes(
            2, updated, serve_utils.DEFAULT_UPDATE_MODE, {'H100': 1})
        decisions = _decisions(autoscaler, [], active_versions=(2,))

        self.assertEqual(decisions, [])
        self.assertEqual(autoscaler.configured_accelerator_shapes, {'H100': 1})
        self.assertEqual(autoscaler.queued_compatibility_profiles, [])
        self.assertFalse(autoscaler._compatibility_demand_complete)

    def test_concurrency_to_qps_catalog_change_drops_old_card_gauge(self):
        old = _make_autoscaler(knob=1.0)
        old.set_configured_accelerator_shapes({'A100': 1})
        now = time.time()
        _report(old,
                in_flight={},
                queue_depth=1,
                timestamps=[now] * 60,
                queued_profiles=[{
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': 60,
                }],
                rejected_profiles=[],
                compatibility_complete=True)
        qps_spec = types.SimpleNamespace(min_replicas=0,
                                         min_replicas_by_accelerator={},
                                         max_replicas=4,
                                         num_overprovision=None,
                                         target_qps_per_replica={'H100': 1.0},
                                         upscale_delay_seconds=0,
                                         downscale_delay_seconds=0)
        replacement = autoscalers.InstanceAwareRequestRateAutoscaler('svc',
                                                                     qps_spec,
                                                                     version=2)

        replacement.load_dynamic_states(old.dump_dynamic_states())
        self.assertEqual(replacement.configured_accelerator_shapes, {'A100': 1})
        replacement.set_configured_accelerator_shapes({'H100': 1})

        # The aggregate arrival history survives and still scales the service,
        # but the A100-only queue gauge cannot be interpreted under H100.
        self.assertEqual(replacement.queued_compatibility_profiles, [])
        self.assertFalse(replacement._compatibility_demand_complete)
        replacement._set_target_num_replicas_with_instance_aware_logic([])
        self.assertEqual(replacement.target_num_replicas, 1)
        self.assertEqual(replacement.target_num_replicas_by_accelerator,
                         {'H100': 1})

        # A delayed report from the old routing version remains aggregate-only
        # and cannot re-arm the cleared exact-card profile.
        replacement.collect_request_information({
            'timestamps': [now],
            'compatibility_profiles': [],
            'queued_requests_by_compatibility': [{
                'priority': 50,
                'compatible_accelerators': ['A100'],
                'count': 60,
            }],
            'compatibility_demand_complete': False,
        })
        self.assertEqual(replacement.queued_compatibility_profiles, [])

    def test_concurrency_to_qps_same_catalog_keeps_arrival_constraints(self):
        old = _make_autoscaler(knob=1.0)
        catalog = {'A100': 1, 'H100': 1}
        old.set_configured_accelerator_shapes(catalog)
        now = time.time()
        _report(old,
                in_flight={},
                timestamps=[now] * 60,
                compatibility_profiles=[{
                    'timestamp': now,
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': 60,
                }],
                queued_profiles=[],
                rejected_profiles=[],
                compatibility_complete=True)
        qps_spec = types.SimpleNamespace(min_replicas=0,
                                         min_replicas_by_accelerator={},
                                         max_replicas=4,
                                         num_overprovision=None,
                                         target_qps_per_replica={
                                             'A100': 1.0,
                                             'H100': 1.0,
                                         },
                                         upscale_delay_seconds=0,
                                         downscale_delay_seconds=0)
        replacement = autoscalers.InstanceAwareRequestRateAutoscaler('svc',
                                                                     qps_spec,
                                                                     version=2)
        snapshot = old.dump_dynamic_states()

        replacement.load_dynamic_states(dict(snapshot))
        replacement.set_configured_accelerator_shapes(catalog)
        replacement._set_target_num_replicas_with_instance_aware_logic([])

        self.assertEqual(replacement.target_num_replicas, 1)
        self.assertEqual(replacement.target_num_replicas_by_accelerator,
                         {'A100': 1})
        decisions = replacement._generate_scaling_decisions([])
        self.assertEqual(decisions[0].target, {'accelerators': {'A100': 1}})

        legacy_snapshot = dict(snapshot)
        legacy_snapshot.pop('compatibility_profiles')
        legacy = autoscalers.InstanceAwareRequestRateAutoscaler('svc',
                                                                qps_spec,
                                                                version=2)
        legacy.load_dynamic_states(legacy_snapshot)
        legacy.set_configured_accelerator_shapes(catalog)
        legacy._set_target_num_replicas_with_instance_aware_logic([])
        self.assertFalse(legacy._compatibility_demand_complete)
        self.assertEqual(legacy.target_num_replicas, 1)
        self.assertEqual(
            sum(legacy.target_num_replicas_by_accelerator.values()), 1)

    def test_concurrency_to_qps_incomplete_state_uses_all_aggregate_arrivals(
            self):
        old = _make_autoscaler(knob=1.0, max_replicas=20)
        catalog = {'A100': 1, 'H100': 1}
        old.set_configured_accelerator_shapes(catalog)
        now = time.time()
        _report(old,
                in_flight={},
                timestamps=[now] * 60,
                compatibility_profiles=[{
                    'timestamp': now,
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': 60,
                }],
                compatibility_complete=True)
        # An old/mixed-version report adds aggregate arrivals but cannot prove
        # their exact constraints. The handoff must not size from only the
        # earlier A100 profile and silently discard these 600 arrivals.
        _report(old,
                in_flight={},
                timestamps=[now] * 600,
                compatibility_complete=False)
        qps_spec = types.SimpleNamespace(min_replicas=0,
                                         min_replicas_by_accelerator={},
                                         max_replicas=20,
                                         num_overprovision=None,
                                         target_qps_per_replica={
                                             'A100': 1.0,
                                             'H100': 1.0,
                                         },
                                         upscale_delay_seconds=0,
                                         downscale_delay_seconds=0)
        replacement = autoscalers.InstanceAwareRequestRateAutoscaler('svc',
                                                                     qps_spec,
                                                                     version=2)

        replacement.load_dynamic_states(old.dump_dynamic_states())
        replacement.set_configured_accelerator_shapes(catalog)
        replacement._set_target_num_replicas_with_instance_aware_logic([])

        self.assertFalse(replacement._compatibility_demand_complete)
        self.assertEqual(replacement.target_num_replicas, 11)
        self.assertEqual(
            sum(replacement.target_num_replicas_by_accelerator.values()), 11)

    def test_concurrency_to_qps_complete_handoff_keeps_unmatched_arrivals(self):
        old = _make_autoscaler(knob=1.0, max_replicas=20)
        catalog = {'A100': 1, 'H100': 1}
        old.set_configured_accelerator_shapes(catalog)
        now = time.time()
        _report(old,
                in_flight={},
                timestamps=[now] * 60,
                compatibility_profiles=[{
                    'timestamp': now,
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': 60,
                }],
                compatibility_complete=True)
        _report(old,
                in_flight={},
                timestamps=[now] * 600,
                compatibility_complete=False)
        _report(old,
                in_flight={},
                timestamps=[now] * 60,
                compatibility_profiles=[{
                    'timestamp': now,
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': 60,
                }],
                compatibility_complete=True)
        qps_spec = types.SimpleNamespace(min_replicas=0,
                                         min_replicas_by_accelerator={},
                                         max_replicas=20,
                                         num_overprovision=None,
                                         target_qps_per_replica={
                                             'A100': 1.0,
                                             'H100': 1.0,
                                         },
                                         upscale_delay_seconds=0,
                                         downscale_delay_seconds=0)
        replacement = autoscalers.InstanceAwareRequestRateAutoscaler('svc',
                                                                     qps_spec,
                                                                     version=2)

        replacement.load_dynamic_states(old.dump_dynamic_states())
        replacement.set_configured_accelerator_shapes(catalog)
        replacement._set_target_num_replicas_with_instance_aware_logic([])

        self.assertTrue(replacement._compatibility_demand_complete)
        self.assertEqual(replacement.target_num_replicas, 12)
        self.assertEqual(
            sum(replacement.target_num_replicas_by_accelerator.values()), 12)
        self.assertGreaterEqual(
            replacement.target_num_replicas_by_accelerator['A100'], 2)

    def test_qps_aggregate_fallback_composes_with_per_card_floor(self):
        old = _make_autoscaler(knob=1.0)
        old.set_configured_accelerator_shapes({'A100': 1})
        now = time.time()
        _report(old,
                in_flight={},
                timestamps=[now] * 120,
                compatibility_profiles=[{
                    'timestamp': now,
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': 120,
                }],
                compatibility_complete=True)
        qps_spec = types.SimpleNamespace(
            min_replicas=0,
            min_replicas_by_accelerator={'H100': 1},
            max_replicas=4,
            num_overprovision=None,
            target_qps_per_replica={'H100': 1.0},
            upscale_delay_seconds=0,
            downscale_delay_seconds=0)
        replacement = autoscalers.InstanceAwareRequestRateAutoscaler('svc',
                                                                     qps_spec,
                                                                     version=2)

        replacement.load_dynamic_states(old.dump_dynamic_states())
        replacement.set_configured_accelerator_shapes({'H100': 1})
        replacement._set_target_num_replicas_with_instance_aware_logic([])

        self.assertFalse(replacement._compatibility_demand_complete)
        self.assertEqual(replacement.target_num_replicas, 2)
        self.assertEqual(replacement.target_num_replicas_by_accelerator,
                         {'H100': 2})

    def test_old_version_replicas_keep_their_launch_knob(self):
        # A knob-raising update must not inflate old replicas' capacity:
        # the rolling drain sizes the kept old set by capacity, and
        # overstating it retires replicas the new fleet cannot replace.
        autoscaler = _make_autoscaler(knob=1.0)
        autoscaler.update_version(2, _spec(knob=2.0),
                                  serve_utils.DEFAULT_UPDATE_MODE)
        old = _replica(1, gpu_count=4, version=1)
        new = _replica(2, gpu_count=4, version=2)
        self.assertEqual(autoscaler._replica_capacity(old), 4.0)
        self.assertEqual(autoscaler._replica_capacity(new), 8.0)

    def test_unknown_version_knob_rehydrates_from_spec(self):
        # Rebuilt autoscaler (controller restart mid-rolling-update):
        # version-1 entry is gone; the durable per-version spec restores
        # the old replicas' true capacity.
        autoscaler = autoscalers.ConcurrencyAutoscaler('svc',
                                                       _spec(knob=2.0),
                                                       version=2)
        old = _replica(1, gpu_count=1, version=1)
        with mock.patch.object(autoscalers.serve_state,
                               'get_spec',
                               return_value=_spec(knob=0.5)):
            self.assertEqual(autoscaler._replica_capacity(old), 0.5)
        # Memoized: later ticks don't re-read the spec.
        with mock.patch.object(autoscalers.serve_state,
                               'get_spec',
                               side_effect=AssertionError):
            self.assertEqual(autoscaler._replica_capacity(old), 0.5)

    def test_unavailable_version_spec_falls_back_to_latest_knob(self):
        autoscaler = autoscalers.ConcurrencyAutoscaler('svc',
                                                       _spec(knob=2.0),
                                                       version=2)
        old = _replica(1, gpu_count=1, version=1)
        with mock.patch.object(autoscalers.serve_state,
                               'get_spec',
                               return_value=None):
            self.assertEqual(autoscaler._replica_capacity(old), 2.0)

    def test_version_fallback_read_once_per_tick_and_retries(self):
        autoscaler = autoscalers.ConcurrencyAutoscaler('svc',
                                                       _spec(knob=2.0),
                                                       version=2)
        old = _replica(1, gpu_count=1, version=1)
        state = {'recovered': False}

        def _get_spec(*_args):
            if not state['recovered']:
                raise RuntimeError('state store unavailable')
            return _spec(knob=0.5)

        def _resolve_repeatedly(*_args):
            return [autoscaler._replica_capacity(old) for _ in range(3)]

        with mock.patch.object(autoscalers.serve_state,
                               'get_spec',
                               side_effect=_get_spec) as mock_get, \
             mock.patch.object(autoscaler,
                               '_generate_scaling_decisions_locked',
                               side_effect=_resolve_repeatedly):
            self.assertEqual(autoscaler.generate_scaling_decisions([], [2]),
                             [2.0, 2.0, 2.0])
            mock_get.assert_called_once_with('svc', 1)

            state['recovered'] = True
            self.assertEqual(autoscaler.generate_scaling_decisions([], [2]),
                             [0.5, 0.5, 0.5])
            self.assertEqual(mock_get.call_count, 2)


class TestDynamicStates(unittest.TestCase):
    """The in-process autoscaler swap must carry the demand report."""

    def test_round_trip_preserves_fresh_report(self):
        source = _make_autoscaler(knob=1.0)
        _report(source,
                in_flight={1: 2},
                queue_depth=1,
                rejected=1,
                recent_rejected=1,
                timestamps=[time.time()])
        loaded = _make_autoscaler(knob=1.0)
        loaded.load_dynamic_states(source.dump_dynamic_states())
        self.assertTrue(loaded.has_fresh_demand_report())
        self.assertEqual(loaded._outstanding_work(), 4)
        self.assertEqual(loaded._rejected_in_recent_window, 1)
        self.assertEqual(len(loaded.request_timestamps), 1)

    def test_dump_is_atomic_with_authoritative_demand_ingestion(self):
        source = _make_autoscaler(knob=1.0)
        source.set_configured_accelerator_shapes({'A100': 1, 'H100': 1})
        _report(source,
                in_flight={},
                queued_profiles=[{
                    'priority': 50,
                    'compatible_accelerators': ['A100'],
                    'count': 1,
                }],
                rejected_profiles=[],
                compatibility_complete=True)
        dump_entered = threading.Event()
        resume_dump = threading.Event()
        report_started = threading.Event()
        dumped = []
        errors = []
        original_dump = source._dump_dynamic_states_locked

        def _blocking_dump():
            dump_entered.set()
            assert resume_dump.wait(timeout=5)
            return original_dump()

        def _dump():
            try:
                dumped.append(source.dump_dynamic_states())
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(exc)

        def _replace_report():
            report_started.set()
            try:
                _report(source,
                        in_flight={},
                        queued_profiles=[{
                            'priority': 50,
                            'compatible_accelerators': ['H100'],
                            'count': 1,
                        }],
                        rejected_profiles=[],
                        compatibility_complete=True)
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(exc)

        with mock.patch.object(source,
                               '_dump_dynamic_states_locked',
                               side_effect=_blocking_dump):
            dump_thread = threading.Thread(target=_dump)
            dump_thread.start()
            self.assertTrue(dump_entered.wait(timeout=5))
            report_thread = threading.Thread(target=_replace_report)
            report_thread.start()
            self.assertTrue(report_started.wait(timeout=5))
            report_thread.join(timeout=0.05)
            self.assertTrue(report_thread.is_alive())
            resume_dump.set()
            dump_thread.join(timeout=5)
            report_thread.join(timeout=5)

        self.assertFalse(dump_thread.is_alive())
        self.assertFalse(report_thread.is_alive())
        self.assertFalse(errors)
        self.assertEqual(
            dumped[0]['queued_compatibility_profiles'][0]
            ['compatible_accelerators'], ['A100'])
        self.assertTrue(dumped[0]['compatibility_demand_complete'])
        self.assertEqual(dumped[0]['configured_accelerator_shapes'], {
            'A100': 1,
            'H100': 1,
        })
        self.assertEqual(
            source.queued_compatibility_profiles[0]['compatible_accelerators'],
            ('H100',))

    def test_old_report_reads_as_stale_after_load(self):
        source = _make_autoscaler(knob=1.0)
        _report(source, in_flight={1: 2})
        source._report_received_at = (
            time.time() - 3 * constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS - 1)
        loaded = _make_autoscaler(knob=1.0)
        loaded.load_dynamic_states(source.dump_dynamic_states())
        self.assertFalse(loaded.has_fresh_demand_report())

    def test_load_from_request_rate_dump_stays_stale(self):
        # Autoscaler type change on update: RequestRateAutoscaler only
        # dumps request_timestamps -- the concurrency autoscaler must
        # start signal-stale, not crash.
        loaded = _make_autoscaler(knob=1.0)
        loaded.load_dynamic_states({
            'latest_version_ever_ready': 1,
            'request_timestamps': [time.time()],
        })
        self.assertFalse(loaded.has_fresh_demand_report())
        self.assertEqual(len(loaded.request_timestamps), 1)

    def test_round_trip_preserves_scale_up_wave_timer(self):
        source = _make_autoscaler(
            knob=1,
            replica_unit='logical',
            max_scale_up_rate_percentage=20,
            scale_up_rate_min_replicas=10,
            scale_up_rate_period_seconds=60,
        )
        source._last_scale_up_wave_at = 123.0
        source._logical_scale_up_wave_ceiling = 17
        loaded = _make_autoscaler(
            knob=1,
            replica_unit='logical',
            max_scale_up_rate_percentage=20,
            scale_up_rate_min_replicas=10,
            scale_up_rate_period_seconds=60,
        )

        loaded.load_dynamic_states(source.dump_dynamic_states())

        self.assertEqual(loaded._last_scale_up_wave_at, 123.0)
        self.assertIsNone(loaded._logical_scale_up_wave_ceiling)


class TestInfo(unittest.TestCase):
    """info() exposes the demand gauges for `sky serve status`."""

    def test_info_before_any_report(self):
        autoscaler = _make_autoscaler(knob=1.0)
        info = autoscaler.info()
        self.assertIsNone(info['in_flight_total'])
        self.assertIsNone(info['report_age_seconds'])

    def test_info_after_report(self):
        autoscaler = _make_autoscaler(knob=1.0)
        _report(autoscaler, in_flight={1: 2, 2: 3}, queue_depth=1, rejected=4)
        info = autoscaler.info()
        self.assertEqual(info['in_flight_total'], 5)
        self.assertEqual(info['queue_depth'], 1)
        self.assertEqual(info['rejected_in_window'], 4)
        self.assertIsNone(info['rejected_in_recent_window'])
        self.assertIsNotNone(info['report_age_seconds'])
        self.assertGreaterEqual(info['report_age_seconds'], 0)


class TestSharedGpuShapeResolver(unittest.TestCase):
    """Both shape-aware autoscalers use ONE resolution implementation."""

    def test_pending_exact_override_never_queries_cluster_table(self):
        autoscaler = _make_autoscaler(knob=1.0)
        info = _replica(1, card='L4')
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.RUNNING)
        info.handle.side_effect = AssertionError('unexpected cluster lookup')

        self.assertEqual(autoscaler._get_gpu_shape_from_replica_info(info),
                         ('L4', 1))
        self.assertNotIn(1, autoscaler._gpu_shape_cache)

        info.resources_override = {'accelerators': {'A100': 4}}
        self.assertEqual(autoscaler._get_gpu_shape_from_replica_info(info),
                         ('A100', 4))
        self.assertNotIn(1, autoscaler._gpu_shape_cache)

    def test_concurrency_uses_post_launch_only_cache(self):
        autoscaler = _make_autoscaler(knob=1.0)
        info = _replica(1, gpu_count=4)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.RUNNING)
        self.assertEqual(autoscaler._get_gpu_shape_from_replica_info(info),
                         ('L4', 4))
        # Mid-launch resolution must NOT be memoized: failover can still
        # change the accelerators.
        self.assertNotIn(1, autoscaler._gpu_shape_cache)
        info.status_property.sky_launch_status = (
            common_utils.ProcessStatus.SUCCEEDED)
        self.assertEqual(autoscaler._get_gpu_shape_from_replica_info(info),
                         ('L4', 4))
        self.assertIn(1, autoscaler._gpu_shape_cache)

    def test_instance_aware_shares_the_mixin_implementation(self):
        self.assertIs(
            autoscalers.InstanceAwareRequestRateAutoscaler.
            _get_gpu_shape_from_replica_info,
            autoscalers._GpuShapeResolverMixin._get_gpu_shape_from_replica_info)
        self.assertIs(
            autoscalers.ConcurrencyAutoscaler._get_gpu_shape_from_replica_info,
            autoscalers._GpuShapeResolverMixin._get_gpu_shape_from_replica_info)

    def test_shape_cache_pruned_to_live_replicas_on_tick(self):
        autoscaler = _make_autoscaler(knob=1.0, min_replicas=1)
        autoscaler._gpu_shape_cache = {99: ('L4', 1)}
        replicas = [_replica(1)]
        _report(autoscaler, in_flight={1: 1})
        autoscaler.generate_scaling_decisions(replicas, [1])
        self.assertNotIn(99, autoscaler._gpu_shape_cache)

    def test_shape_preload_does_not_hold_demand_state_lock(self):
        autoscaler = _make_autoscaler(knob=1.0)
        preload_started = threading.Event()
        release_preload = threading.Event()
        decision_errors = []

        def _blocked_preload(_):
            preload_started.set()
            if not release_preload.wait(timeout=5):
                raise TimeoutError('test did not release shape preload')
            return {}

        def _decide():
            try:
                autoscaler.generate_scaling_decisions([], [1])
            except Exception as error:  # pylint: disable=broad-except
                decision_errors.append(error)

        with mock.patch.object(autoscaler,
                               '_resolve_gpu_shape_handles',
                               side_effect=_blocked_preload):
            decision_thread = threading.Thread(target=_decide)
            decision_thread.start()
            self.assertTrue(preload_started.wait(timeout=5))
            acquired = autoscaler._logical_state_lock.acquire(timeout=1)
            self.assertTrue(acquired)
            if acquired:
                autoscaler._logical_state_lock.release()
            release_preload.set()
            decision_thread.join(timeout=5)

        self.assertFalse(decision_thread.is_alive())
        self.assertEqual(decision_errors, [])


if __name__ == '__main__':
    unittest.main()


def _histogram(counts_by_index, bucket_start=None, outcome='succeeded'):
    """Build one LB prediction-time report from {bucket_index: count}."""
    if bucket_start is None:
        bucket_seconds = constants.LB_REQUEST_HISTORY_BUCKET_SECONDS
        bucket_start = int(time.time() // bucket_seconds) * bucket_seconds
    counts = [0] * constants.LB_PREDICTION_TIME_BUCKET_COUNT
    for index, count in counts_by_index.items():
        counts[index] = count
    return {
        'bucket_seconds': constants.LB_REQUEST_HISTORY_BUCKET_SECONDS,
        'histogram_version': constants.LB_PREDICTION_TIME_HISTOGRAM_VERSION,
        'buckets': [{
            'bucket_start': bucket_start,
            'outcome_counts': {
                outcome: counts
            },
        }],
    }


class TestAdaptiveDemandEstimation(unittest.TestCase):
    """Measured duration and lead supersede configuration when trusted."""

    @staticmethod
    def _autoscaler(**kwargs):
        kwargs.setdefault('adaptive_demand_estimation', True)
        kwargs.setdefault('initial_provision_lead_time_seconds', 540)
        return _make_autoscaler(knob=1,
                                min_replicas=0,
                                max_replicas=1000,
                                replica_unit='logical',
                                expected_request_duration_seconds=30,
                                **kwargs)

    # Bucket index 8 spans (30s, 60s]; its geometric midpoint, which is
    # what the estimator uses, is sqrt(30*60) = 42.43s.
    _SIXTY_SECOND_BUCKET = 8
    _SIXTY_SECOND_BUCKET_REPRESENTATIVE = math.sqrt(30.0 * 60.0)

    def test_measured_duration_supersedes_config(self):
        autoscaler = self._autoscaler()
        self.assertEqual(autoscaler.effective_request_duration_seconds, 30)

        _report(autoscaler,
                in_flight={},
                prediction_time_history=_histogram(
                    {self._SIXTY_SECOND_BUCKET: 50}))

        self.assertAlmostEqual(autoscaler.effective_request_duration_seconds,
                               self._SIXTY_SECOND_BUCKET_REPRESENTATIVE)

    def test_bucket_representative_is_the_geometric_midpoint(self):
        """Wide log-scale buckets must not inflate the estimate.

        Measured against production: with 97% of requests in the (10s, 30s]
        bucket, taking the upper bound rather than the midpoint inflated the
        estimate 1.70x, which would silently oversize every fleet.
        """
        bounds = list(constants.LB_PREDICTION_TIME_BUCKET_UPPER_BOUNDS_SECONDS)

        # (10, 30] -> sqrt(300), not 30.
        self.assertAlmostEqual(
            autoscalers._prediction_bucket_representative(7, bounds),
            math.sqrt(10.0 * 30.0))
        # The first bucket starts at zero, where a geometric mean degenerates.
        self.assertAlmostEqual(
            autoscalers._prediction_bucket_representative(0, bounds),
            bounds[0] / 2.0)
        # The final bucket is unbounded above, so its lower bound is the
        # only honest floor.
        self.assertAlmostEqual(
            autoscalers._prediction_bucket_representative(len(bounds), bounds),
            bounds[-1])

    def test_measured_duration_needs_enough_samples(self):
        autoscaler = self._autoscaler()
        _report(autoscaler,
                in_flight={},
                prediction_time_history=_histogram(
                    {self._SIXTY_SECOND_BUCKET: 3}))

        # Three completions cannot redefine the sizing constant.
        self.assertEqual(autoscaler.effective_request_duration_seconds, 30)
        self.assertEqual(autoscaler._measured_duration_samples, 3)

    def test_measured_duration_ignored_when_feature_disabled(self):
        autoscaler = self._autoscaler(adaptive_demand_estimation=False)
        _report(autoscaler,
                in_flight={},
                prediction_time_history=_histogram(
                    {self._SIXTY_SECOND_BUCKET: 50}))

        self.assertEqual(autoscaler.effective_request_duration_seconds, 30)
        self.assertEqual(autoscaler.effective_provision_lead_seconds, 540)

    def test_stale_measurement_falls_back_to_config(self):
        autoscaler = self._autoscaler()
        _report(autoscaler,
                in_flight={},
                prediction_time_history=_histogram(
                    {self._SIXTY_SECOND_BUCKET: 50}))
        self.assertAlmostEqual(autoscaler.effective_request_duration_seconds,
                               self._SIXTY_SECOND_BUCKET_REPRESENTATIVE)

        autoscaler._measured_duration_at = (
            time.time() - constants.AUTOSCALER_ADAPTIVE_SAMPLE_MAX_AGE_SECONDS -
            1)

        self.assertEqual(autoscaler.effective_request_duration_seconds, 30)

    def test_repeated_histogram_report_is_not_double_counted(self):
        autoscaler = self._autoscaler()
        histogram = _histogram({self._SIXTY_SECOND_BUCKET: 50})

        _report(autoscaler, in_flight={}, prediction_time_history=histogram)
        _report(autoscaler, in_flight={}, prediction_time_history=histogram)

        # The load balancer re-reports a bucket until it is durably
        # accepted; only the delta may count.
        self.assertEqual(autoscaler._measured_duration_samples, 50)

    def test_histogram_version_mismatch_is_dropped(self):
        autoscaler = self._autoscaler()
        histogram = _histogram({self._SIXTY_SECOND_BUCKET: 50})
        histogram['histogram_version'] = (
            constants.LB_PREDICTION_TIME_HISTOGRAM_VERSION + 1)

        _report(autoscaler, in_flight={}, prediction_time_history=histogram)

        self.assertEqual(autoscaler._measured_duration_samples, 0)
        self.assertEqual(autoscaler.effective_request_duration_seconds, 30)

    def test_failed_outcomes_do_not_define_service_time(self):
        autoscaler = self._autoscaler()
        _report(autoscaler,
                in_flight={},
                prediction_time_history=_histogram({0: 50}, outcome='failed'))

        self.assertEqual(autoscaler._measured_duration_samples, 0)
        self.assertEqual(autoscaler.effective_request_duration_seconds, 30)

    def test_auto_seed_is_used_until_enough_samples(self):
        autoscaler = self._autoscaler(
            initial_provision_lead_time_seconds='auto')

        self.assertEqual(autoscaler.effective_provision_lead_seconds,
                         constants.AUTOSCALER_DEFAULT_PROVISION_LEAD_SECONDS)

        replicas = []
        for index in range(8):
            replica = _replica(index + 1)
            replica.created_at = 1000.0
            replica.status_property.first_ready_time = 1000.0 + 60 * (index + 1)
            replicas.append(replica)
        autoscaler._observe_provision_leads(replicas)

        # Measurement replaces the assumption once the service has proven
        # its own launch latency.
        self.assertEqual(autoscaler.effective_provision_lead_seconds, 420.0)

    def test_unset_lead_defaults_to_auto_seed(self):
        autoscaler = self._autoscaler(initial_provision_lead_time_seconds=None)

        self.assertEqual(autoscaler.effective_provision_lead_seconds,
                         constants.AUTOSCALER_DEFAULT_PROVISION_LEAD_SECONDS)

    def test_explicit_zero_lead_is_honored(self):
        autoscaler = self._autoscaler(initial_provision_lead_time_seconds=0)

        # An explicit 0 is a declaration, not an absent value.
        self.assertEqual(autoscaler.effective_provision_lead_seconds, 0.0)

    def test_adaptive_estimation_is_on_by_default(self):
        autoscaler = _make_autoscaler(knob=1,
                                      replica_unit='logical',
                                      expected_request_duration_seconds=30)

        self.assertTrue(autoscaler.adaptive_demand_estimation)

    def test_measured_lead_supersedes_config(self):
        autoscaler = self._autoscaler()
        replicas = []
        for index in range(8):
            replica = _replica(index + 1)
            replica.created_at = 1000.0
            replica.status_property.first_ready_time = 1000.0 + 60 * (index + 1)
            replicas.append(replica)

        autoscaler._observe_provision_leads(replicas)

        # Eight samples of 60..480s; the p75 index is 6 -> 420s.
        self.assertEqual(autoscaler.effective_provision_lead_seconds, 420.0)

    def test_lead_needs_enough_samples_and_ignores_never_ready(self):
        autoscaler = self._autoscaler()
        ready = _replica(1)
        ready.created_at = 1000.0
        ready.status_property.first_ready_time = 1600.0
        never_ready = _replica(2)
        never_ready.created_at = 1000.0
        never_ready.status_property.first_ready_time = -1

        autoscaler._observe_provision_leads([ready, never_ready])

        self.assertEqual(autoscaler._provision_lead_samples, [600.0])
        self.assertEqual(autoscaler.effective_provision_lead_seconds, 540)

    def test_lead_sample_is_taken_once_per_replica(self):
        autoscaler = self._autoscaler()
        replica = _replica(1)
        replica.created_at = 1000.0
        replica.status_property.first_ready_time = 1600.0

        autoscaler._observe_provision_leads([replica])
        autoscaler._observe_provision_leads([replica])

        self.assertEqual(autoscaler._provision_lead_samples, [600.0])

    def test_estimates_survive_controller_restart(self):
        autoscaler = self._autoscaler()
        _report(autoscaler,
                in_flight={},
                prediction_time_history=_histogram(
                    {self._SIXTY_SECOND_BUCKET: 50}))
        replicas = []
        for index in range(8):
            replica = _replica(index + 1)
            replica.created_at = 1000.0
            replica.status_property.first_ready_time = 1000.0 + 60 * (index + 1)
            replicas.append(replica)
        autoscaler._observe_provision_leads(replicas)

        restored = self._autoscaler()
        restored.load_dynamic_states(autoscaler.dump_dynamic_states())

        self.assertAlmostEqual(restored.effective_request_duration_seconds,
                               self._SIXTY_SECOND_BUCKET_REPRESENTATIVE)
        self.assertEqual(restored.effective_provision_lead_seconds, 420.0)

    def test_measured_values_drive_queue_sizing(self):
        autoscaler = self._autoscaler(
            target_utilization_percentage=100,
            lb_request_queue={
                'timeout_seconds': 20,
                'timeout_seconds_by_priority': [{
                    'min_priority': 0,
                    'timeout_seconds': 600,
                }],
            },
        )
        _report(autoscaler,
                in_flight={},
                queue_depth=100,
                queue_depth_by_priority={0: 100},
                prediction_time_history=_histogram(
                    {self._SIXTY_SECOND_BUCKET: 50}))

        autoscaler._set_target_num_replicas_with_concurrency_logic([])

        # The measured 42.4s duration exceeds the 60s budget left by the
        # configured 540s lead by more than half, so each queued request
        # weighs 42.4/60; the configured 30s would have weighed 0.5.
        self.assertAlmostEqual(
            autoscaler._weighted_queue_work,
            100 * self._SIXTY_SECOND_BUCKET_REPRESENTATIVE / 60.0)
