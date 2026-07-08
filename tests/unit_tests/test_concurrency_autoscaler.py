"""Unit tests for sky.serve.autoscalers.ConcurrencyAutoscaler.

The concurrency autoscaler sizes the fleet by OUTSTANDING WORK (in-flight
+ queued + recently-rejected jobs, reported by the LB as gauges) instead
of request rate, packs demand onto per-GPU capacities (knob x gpu_count),
and never shrinks the fleet while its demand signal is stale (a rebuilt
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
          downscale_delay_seconds=None):
    # Default delays resolve to one decision interval -> hysteresis
    # thresholds of 1 tick, so most tests observe target changes on the
    # first post-snap recompute.
    interval = constants.AUTOSCALER_DEFAULT_DECISION_INTERVAL_SECONDS
    return types.SimpleNamespace(
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        num_overprovision=None,
        target_concurrency_per_replica=knob,
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
             version=1):
    info = mock.Mock()
    info.replica_id = replica_id
    info.version = version
    info.status = status
    info.is_terminal = status in serve_state.ReplicaStatus.terminal_statuses()
    info.is_ready = status == serve_state.ReplicaStatus.READY
    info.cluster_name = f'cluster-{replica_id}'
    info.status_property.sky_launch_status = (
        common_utils.ProcessStatus.SUCCEEDED)
    info.status_property.unrecoverable_failure.return_value = False
    info.handle.return_value.launched_resources.accelerators = {'L4': gpu_count}
    return info


def _report(autoscaler, in_flight, queue_depth=0, rejected=0, timestamps=()):
    autoscaler.collect_request_information({
        'timestamps': list(timestamps),
        'in_flight_by_replica_id': in_flight,
        'queue_depth': queue_depth,
        'rejected_in_window': rejected,
    })


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


class TestDrainAwareDownscale(unittest.TestCase):
    """READY victims require fresh in_flight == 0; missing entry = busy."""

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

    def test_count_floor_keeps_standby_on_zero_demand(self):
        # Zero outstanding work but no ready latest replica: the
        # base-class count floor (target - ready_new) keeps the standby.
        autoscaler = self._mid_update(target=1)
        old = [_replica(1, version=1)]
        _report(autoscaler, in_flight={1: 0})
        self.assertEqual(
            autoscaler._select_outdated_replicas_to_scale_down(old, [1]), [])


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
                timestamps=[time.time()])
        loaded = _make_autoscaler(knob=1.0)
        loaded.load_dynamic_states(source.dump_dynamic_states())
        self.assertTrue(loaded.has_fresh_demand_report())
        self.assertEqual(loaded._outstanding_work(), 4)
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
