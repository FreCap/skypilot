"""Unit tests for sky.serve.autoscalers.ConcurrencyAutoscaler.

The concurrency autoscaler sizes the fleet by OUTSTANDING WORK (in-flight
+ queued + recently-rejected jobs, reported by the LB as gauges) instead
of request rate. Physical targets pack demand onto per-GPU capacities;
logical targets divide demand by the per-GPU saturation knob and publish GPU
slots. Neither mode shrinks while its demand signal is stale (a rebuilt
controller must not mass-retire a live fleet before the first LB sync).
"""
# pylint: disable=protected-access
import time
import types
import unittest
from unittest import mock

from sky.serve import autoscalers
from sky.serve import constants
from sky.serve import serve_state
from sky.serve import serve_utils
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
          max_scale_up_rate_percentage=None,
          scale_up_rate_min_replicas=None,
          scale_up_rate_period_seconds=None,
          max_scale_down_rate_percentage=100,
          adaptive_scale_up=None,
          lb_request_queue=None):
    # Default delays resolve to one decision interval -> hysteresis
    # thresholds of 1 tick, so most tests observe target changes on the
    # first post-snap recompute.
    interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
    return types.SimpleNamespace(
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        num_overprovision=None,
        target_concurrency_per_replica=knob,
        replica_unit=replica_unit,
        target_utilization_percentage=target_utilization_percentage,
        expected_request_duration_seconds=expected_request_duration_seconds,
        max_scale_up_rate_percentage=max_scale_up_rate_percentage,
        scale_up_rate_min_replicas=scale_up_rate_min_replicas,
        scale_up_rate_period_seconds=scale_up_rate_period_seconds,
        adaptive_scale_up=adaptive_scale_up,
        lb_request_queue=lb_request_queue,
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
             status=serve_state.ReplicaStatus.READY,
             version=1,
             planned_capacity=None):
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
    info.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    info.status_property.is_scale_down = False
    info.status_property.unrecoverable_failure.return_value = False
    info.handle.return_value.launched_resources.accelerators = {'L4': gpu_count}
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
            queue_depth_by_priority=None,
            rejected_by_priority=None,
            recent_rejected_by_priority=None,
            unique_arrivals_60s=None,
            unique_arrivals_300s=None,
            headerless_arrivals_60s=None,
            headerless_arrivals_300s=None,
            arrival_tracking_saturated=False,
            pressure_report_is_floored=False):
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

        # (1 in flight + 2 queued + 4 rejected + 1 unknown) / 2 per GPU.
        self.assertEqual(autoscaler.target_num_replicas, 4)

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

    def test_twenty_percent_dominates_floor_for_large_fleet(self):
        autoscaler = self._ramped_autoscaler()
        autoscaler.target_num_replicas = 100
        _report(autoscaler, in_flight={}, queue_depth=1000)
        replicas = [_replica(i + 1) for i in range(100)]

        with mock.patch.object(autoscalers.time, 'time', return_value=100.0):
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        self.assertEqual(autoscaler.target_num_replicas, 120)

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
            _report(autoscaler,
                    in_flight={replica.replica_id: 0 for replica in replicas},
                    queue_depth=1)
            clock.return_value = 380.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

            self.assertEqual(autoscaler.target_num_replicas, 100)
            self.assertEqual(autoscaler._downscale_veto_reason, 'queue_depth')
            self.assertIsNone(autoscaler._downscale_started_at)

            # The unchanged nonzero queue is demand in the target, but it is
            # not another pressure delta and cannot veto the next quiet window.
            _report(autoscaler,
                    in_flight={replica.replica_id: 0 for replica in replicas},
                    queue_depth=1)
            clock.return_value = 400.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            clock.return_value = 680.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)

        self.assertEqual(autoscaler.target_num_replicas, 50)

    def test_trickle_pressure_cannot_starve_downscale_forever(self):
        """Regression: a tiny positive delta every quiet window must not
        restart the downscale delay indefinitely (boltz-l4-fleet pinned at
        144 replicas while the demand target was 3-8)."""
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
            # Window 1: timer starts, a trickle delta latches, veto #1.
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            _report(autoscaler, in_flight=idle, queue_depth=1)
            clock.return_value = 380.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            self.assertEqual(autoscaler._downscale_veto_streak, 1)

            # Window 2: another tiny delta, veto #2 (still within the cap).
            clock.return_value = 400.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            _report(autoscaler, in_flight=idle, queue_depth=2)
            clock.return_value = 700.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, 100)
            self.assertEqual(autoscaler._downscale_veto_streak, 2)

            # Window 3: yet another delta, but the cap is exhausted: the
            # elapsed window must adopt the downscale.
            clock.return_value = 720.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            _report(autoscaler, in_flight=idle, queue_depth=3)
            clock.return_value = 1020.0
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
            # Exhaust the veto budget with two trickle windows.
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            _report(autoscaler, in_flight=idle, queue_depth=1)
            clock.return_value = 380.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            clock.return_value = 400.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            _report(autoscaler, in_flight=idle, queue_depth=2)
            clock.return_value = 700.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler._downscale_veto_streak, 2)

            # Genuine burst: raw target rises above the adopted target and
            # ends the downscale episode, refreshing the budget.
            _report(autoscaler, in_flight=idle, queue_depth=500)
            clock.return_value = 720.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertGreater(autoscaler.target_num_replicas, 100)
            self.assertEqual(autoscaler._downscale_veto_streak, 0)

            # The next episode gets a fresh veto: a first elapsed window
            # with a latched delta must hold the fleet again.
            adopted = autoscaler.target_num_replicas
            _report(autoscaler, in_flight=idle)
            clock.return_value = 740.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            _report(autoscaler, in_flight=idle, queue_depth=1)
            clock.return_value = 1040.0
            autoscaler._set_target_num_replicas_with_concurrency_logic(replicas)
            self.assertEqual(autoscaler.target_num_replicas, adopted)
            self.assertEqual(autoscaler._downscale_veto_streak, 1)

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

        self.assertEqual(autoscaler.target_num_replicas, 1)
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
        loaded = _make_autoscaler(
            knob=1,
            replica_unit='logical',
            max_scale_up_rate_percentage=20,
            scale_up_rate_min_replicas=10,
            scale_up_rate_period_seconds=60,
        )

        loaded.load_dynamic_states(source.dump_dynamic_states())

        self.assertEqual(loaded._last_scale_up_wave_at, 123.0)


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


if __name__ == '__main__':
    unittest.main()
