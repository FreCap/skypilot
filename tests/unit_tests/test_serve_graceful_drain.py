"""Tests for the in-flight-aware graceful drain on replica retirement.

Retiring a replica (autoscaler scale-down, including rolling-update
retirement of outdated replicas) must wait for in-flight requests to
finish -- bounded by the per-service `graceful_drain_seconds` cap --
instead of sleeping a fixed 120s and then killing whatever is still
running.
"""
# pylint: disable=protected-access
import threading
from unittest import mock

import jsonschema
import pytest

from sky.serve import replica_managers
from sky.serve import service_spec as service_spec_lib
from sky.utils import schemas


class _FakeClock:

    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture(name='clock')
def _clock(monkeypatch):
    clock = _FakeClock()
    monkeypatch.setattr(replica_managers.time, 'monotonic', clock.time)
    monkeypatch.setattr(replica_managers.time, 'sleep', clock.sleep)
    return clock


class TestWaitForDrain:

    def test_no_predicate_is_bounded_sleep(self, clock):
        replica_managers._wait_for_drain(clock.now + 120, None)
        assert clock.sleeps == [120]

    def test_past_deadline_returns_immediately(self, clock):
        replica_managers._wait_for_drain(clock.now - 1, lambda: True)
        assert not clock.sleeps
        replica_managers._wait_for_drain(clock.now - 1, None)
        assert clock.sleeps == [0]

    def test_drained_predicate_exits_early(self, clock):
        replica_managers._wait_for_drain(clock.now + 120, lambda: True)
        assert not clock.sleeps  # First check fires before any sleep.

    def test_never_drained_waits_to_deadline(self, clock):
        replica_managers._wait_for_drain(clock.now + 10, lambda: False)
        assert sum(clock.sleeps) == pytest.approx(10)

    def test_drain_after_some_polls(self, clock):
        drained_at = clock.now + 6
        replica_managers._wait_for_drain(clock.now + 120,
                                         lambda: clock.now >= drained_at)
        assert sum(clock.sleeps) == pytest.approx(6)

    def test_predicate_failure_is_contained(self, clock):

        def _boom():
            raise RuntimeError('gauge unavailable')

        replica_managers._wait_for_drain(clock.now + 10, _boom)  # No raise.
        assert sum(clock.sleeps) == pytest.approx(10)

    def test_terminate_cluster_stays_contextual(self):
        # Regression: a refactor once left @context.contextual on a helper
        # instead of terminate_cluster, so down threads lost their context
        # and every teardown died on the context assert.
        assert (
            replica_managers.terminate_cluster.__name__ == 'terminate_cluster')
        assert hasattr(replica_managers.terminate_cluster, '__wrapped__')


def _manager(is_pool=False):
    rm = replica_managers.ReplicaManager.__new__(
        replica_managers.ReplicaManager)
    rm._service_name = 'svc'
    rm._is_pool = is_pool
    rm._lb_in_flight_report = None
    rm.lock = threading.Lock()
    return rm


class TestReplicaDrainTracker:

    URL = 'http://r1:8080'
    OTHER = 'http://r2:8080'

    @staticmethod
    def _report(received_at,
                in_flight,
                routing_urls,
                unknown_urls=frozenset(),
                draining_urls=frozenset(),
                session='lb-1'):
        return (received_at, in_flight, routing_urls, unknown_urls,
                draining_urls, session)

    def _tracker(self, rm, drain_started=1000.0):
        return replica_managers._ReplicaDrainTracker(rm, self.URL,
                                                     drain_started)

    def test_no_report_means_not_drained(self):
        rm = _manager()
        assert not self._tracker(rm)()

    def test_cold_lb_report_never_seen_is_not_trusted(self):
        # A restarted LB loses its draining/occupancy overlays and ships
        # empty sets: absence without a prior acknowledgement of the url
        # must NOT read as drained (seen-then-clean).
        rm = _manager()
        rm._lb_in_flight_report = self._report(1001.0, {}, set())
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            assert not self._tracker(rm)()

    def test_seen_in_routing_then_clean_is_drained(self):
        rm = _manager()
        tracker = self._tracker(rm)
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            rm._lb_in_flight_report = self._report(1001.0, {},
                                                   {self.URL, self.OTHER})
            assert not tracker()  # Seen, but still routed.
            rm._lb_in_flight_report = self._report(1021.0, {}, {self.OTHER})
            assert tracker()  # Clean after having been seen.

    def test_explicit_zero_is_seen_and_clean_at_once(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(1001.0, {self.URL: 0},
                                               {self.OTHER})
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            assert self._tracker(rm)()

    def test_seen_via_draining_then_clean_is_drained(self):
        rm = _manager()
        tracker = self._tracker(rm)
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            rm._lb_in_flight_report = self._report(1001.0, {self.URL: 2},
                                                   set(),
                                                   draining_urls={self.URL})
            assert not tracker()  # Seen draining with work in flight.
            rm._lb_in_flight_report = self._report(1021.0, {}, set())
            assert tracker()

    def test_report_predating_drain_is_not_trusted(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(999.0, {self.URL: 0}, set())
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            assert not self._tracker(rm)()

    def test_old_lb_without_routing_view_blocks_drain(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(1001.0, {self.URL: 0}, None)
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            assert not self._tracker(rm)()

    def test_stale_report_means_not_drained(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(1001.0, {self.URL: 0}, set())
        stale_at = (1001.0 +
                    replica_managers._IN_FLIGHT_REPORT_STALENESS_SECONDS + 1)
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=stale_at):
            assert not self._tracker(rm)()

    def test_still_routed_replica_is_not_drained(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(1001.0, {self.URL: 0},
                                               {self.URL})
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            assert not self._tracker(rm)()

    def test_nonzero_in_flight_is_not_drained(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(1001.0, {self.URL: 3},
                                               {self.OTHER})
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            assert not self._tracker(rm)()

    def test_unknown_occupancy_blocks_drain(self):
        rm = _manager()
        tracker = self._tracker(rm)
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            rm._lb_in_flight_report = self._report(1001.0, {},
                                                   set(),
                                                   unknown_urls={self.URL})
            assert not tracker()  # Seen but occupancy-unknown.
            rm._lb_in_flight_report = self._report(1021.0, {self.URL: 0}, set())
            assert tracker()  # Post-retirement explicit idle.

    def test_unrelated_urls_cannot_block_drain(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(1001.0, {
            self.URL: 0,
            self.OTHER: 5
        }, {self.OTHER},
                                               unknown_urls={'http://x:1'})
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            assert self._tracker(rm)()

    def test_seen_does_not_survive_lb_restart(self):
        # A new LB incarnation ships empty overlays: the old incarnation's
        # acknowledgement must not combine with the new one's clean-looking
        # report.
        rm = _manager()
        tracker = self._tracker(rm)
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            rm._lb_in_flight_report = self._report(1001.0, {}, {self.URL},
                                                   session='lb-old')
            assert not tracker()  # Seen (routed) by the old LB.
            rm._lb_in_flight_report = self._report(1021.0, {},
                                                   set(),
                                                   session='lb-new')
            assert not tracker()  # Clean but never seen by the new LB.
            rm._lb_in_flight_report = self._report(1041.0, {self.URL: 0},
                                                   set(),
                                                   session='lb-new')
            assert tracker()  # Explicit idle from the new LB.

    def test_unknown_taint_requires_explicit_idle(self):
        # Once occupancy was unproven, later ABSENCE may just be the LB's
        # off-ready retention expiring: only an explicit idle entry can
        # complete the drain.
        rm = _manager()
        tracker = self._tracker(rm)
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1005.0):
            rm._lb_in_flight_report = self._report(1001.0, {},
                                                   set(),
                                                   unknown_urls={self.URL})
            assert not tracker()
            rm._lb_in_flight_report = self._report(1021.0, {}, set())
            assert not tracker()  # Absent after unknown: still tainted.
            rm._lb_in_flight_report = self._report(1041.0, {self.URL: 0}, set())
            assert tracker()  # Explicit post-retirement idle clears it.

    def test_update_none_keeps_previous_report(self):
        rm = _manager()
        rm.update_lb_in_flight({self.URL: 2}, [self.URL], [], [], 'lb-1')
        first = rm._lb_in_flight_report
        assert first is not None
        rm.update_lb_in_flight(None, None, None, None, None)
        assert rm._lb_in_flight_report is first


def _scale_down_manager(spec_drain, is_pool=False, spec_error=None):
    rm = replica_managers.SkyPilotReplicaManager.__new__(
        replica_managers.SkyPilotReplicaManager)
    rm._service_name = 'svc'
    rm._is_pool = is_pool
    rm._lb_in_flight_report = None
    rm._spot_placer = None
    rm.lock = threading.Lock()
    rm._terminate_replica = mock.Mock()
    spec = mock.Mock()
    spec.graceful_drain_seconds = spec_drain
    if spec_error is not None:
        rm._get_version_spec = mock.Mock(side_effect=spec_error)
    else:
        rm._get_version_spec = mock.Mock(return_value=spec)
    return rm


class TestScaleDownWiring:

    def _run(self, rm):
        info = mock.Mock()
        info.version = 3
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info):
            rm.scale_down(7)
        return rm._terminate_replica.call_args.kwargs

    def test_spec_cap_used(self):
        kwargs = self._run(_scale_down_manager(spec_drain=600))
        assert kwargs['in_flight_drain_cap_seconds'] == 600

    def test_unset_spec_uses_default_cap(self):
        kwargs = self._run(_scale_down_manager(spec_drain=None))
        assert kwargs['in_flight_drain_cap_seconds'] == (
            replica_managers._DEFAULT_DRAIN_SECONDS)

    def test_spec_failure_falls_back_to_default(self):
        kwargs = self._run(
            _scale_down_manager(spec_drain=None,
                                spec_error=ValueError('version gone')))
        assert kwargs['in_flight_drain_cap_seconds'] == (
            replica_managers._DEFAULT_DRAIN_SECONDS)

    def test_purge_bypasses_drain(self):
        # A purge forcefully cleans up an already-failed replica; it must
        # not wait out the graceful cap.
        rm = _scale_down_manager(spec_drain=1800)
        info = mock.Mock()
        info.version = 3
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info):
            rm.scale_down(7, purge=True)
        kwargs = rm._terminate_replica.call_args.kwargs
        assert kwargs['in_flight_drain_cap_seconds'] is None

    def test_zero_disables_drain(self):
        kwargs = self._run(_scale_down_manager(spec_drain=0))
        assert kwargs['in_flight_drain_cap_seconds'] == 0


class TestRecoveryRedrive:
    """A scale-down retirement interrupted by a controller restart must
    re-enter a full bounded drain, not re-drive with no drain."""

    def _redrive(self,
                 is_scale_down,
                 purged=False,
                 persisted_cap=None,
                 preempted=False,
                 derived_status=None):
        rm = _scale_down_manager(spec_drain=600)
        rm._launch_thread_pool = {}
        rm._down_thread_pool = {}
        sp = mock.Mock()
        sp.is_scale_down = is_scale_down
        sp.purged = purged
        sp.preempted = preempted
        # None models a legacy row without a persisted cap (a bare Mock
        # attribute would read as a truthy persisted value).
        sp.drain_cap_seconds = persisted_cap
        info = mock.Mock()
        info.replica_id = 7
        info.status_property = sp
        info.status = (derived_status or
                       replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replicas_at_status',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info):
            rm._recover_replica_operations()
        return rm._terminate_replica.call_args.kwargs

    def test_scale_down_redrive_reenters_bounded_drain(self):
        kwargs = self._redrive(is_scale_down=True)
        assert kwargs['in_flight_drain_cap_seconds'] == 600

    def test_purged_redrive_keeps_immediate_teardown(self):
        kwargs = self._redrive(is_scale_down=True, purged=True)
        assert kwargs['in_flight_drain_cap_seconds'] is None

    def test_failure_teardown_redrive_keeps_immediate_teardown(self):
        kwargs = self._redrive(is_scale_down=False)
        assert kwargs['in_flight_drain_cap_seconds'] is None
        # Failure teardowns are left in the record; _terminate_replica
        # asserts such rows sync logs down. A False here would trip that
        # assert into the recovery catch and strand the replica in
        # SHUTTING_DOWN forever.
        assert kwargs['sync_down_logs'] is True

    def test_scale_down_redrive_skips_log_sync(self):
        kwargs = self._redrive(is_scale_down=True)
        assert kwargs['sync_down_logs'] is False

    @pytest.mark.parametrize(
        'derived_status',
        [
            replica_managers.serve_state.ReplicaStatus.PREEMPTED,
            # Crash after persisting preempted=True but before scheduling
            # sky.down: status derivation has not reached PREEMPTED yet.
            replica_managers.serve_state.ReplicaStatus.NOT_READY,
        ])
    def test_preempted_redrive_forces_immediate_scale_down(
            self, derived_status):
        kwargs = self._redrive(is_scale_down=False,
                               persisted_cap=450,
                               preempted=True,
                               derived_status=derived_status)
        assert kwargs['is_scale_down'] is True
        assert kwargs['sync_down_logs'] is False
        assert kwargs['in_flight_drain_cap_seconds'] is None

    def test_persisted_preemption_rebuilds_spot_bench(self):
        rm = _scale_down_manager(spec_drain=600)
        rm._launch_thread_pool = {}
        rm._down_thread_pool = {}
        rm._spot_placer = mock.Mock()
        sp = mock.Mock(is_scale_down=True,
                       purged=False,
                       preempted=True,
                       drain_cap_seconds=None)
        info = mock.Mock(
            replica_id=7,
            cluster_name='svc-7',
            is_spot=True,
            status_property=sp,
            status=replica_managers.serve_state.ReplicaStatus.PREEMPTED)
        location = mock.sentinel.location
        info.get_spot_location.return_value = location
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replicas_at_status',
                               return_value=[]):
            rm._recover_replica_operations()

        rm._spot_placer.set_preemptive.assert_called_once_with(location)

    def test_preemption_refresh_crash_window_recovers_missing_cluster_row(self):
        # The cloud-status refresh already removed the cluster row, but the
        # controller crashed before persisting preempted=True.
        rm = _scale_down_manager(spec_drain=600)
        rm._launch_thread_pool = {}
        rm._down_thread_pool = {}
        rm._spot_placer = mock.Mock()
        sp = mock.Mock(is_scale_down=False,
                       purged=False,
                       preempted=False,
                       drain_cap_seconds=None)
        info = mock.Mock(
            replica_id=7,
            cluster_name='svc-7',
            is_spot=True,
            status_property=sp,
            status=replica_managers.serve_state.ReplicaStatus.NOT_READY)
        location = mock.sentinel.location
        info.get_spot_location.return_value = location
        writes = []
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replicas_at_status',
                               return_value=[]), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_status_fields',
                               return_value={}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'add_or_update_replica',
                 side_effect=lambda *args: writes.append(sp.preempted)):
            rm._recover_replica_operations()

        assert writes == [True]
        rm._spot_placer.set_preemptive.assert_called_once_with(location)
        kwargs = rm._terminate_replica.call_args.kwargs
        assert kwargs['is_scale_down'] is True
        assert kwargs['sync_down_logs'] is False
        assert kwargs['in_flight_drain_cap_seconds'] is None

    def test_active_spot_with_cluster_row_is_not_misclassified(self):
        rm = _scale_down_manager(spec_drain=600)
        rm._launch_thread_pool = {}
        rm._down_thread_pool = {}
        sp = mock.Mock(preempted=False)
        info = mock.Mock(
            replica_id=7,
            cluster_name='svc-7',
            is_spot=True,
            status_property=sp,
            status=replica_managers.serve_state.ReplicaStatus.READY)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replicas_at_status',
                               return_value=[]), \
             mock.patch.object(
                 replica_managers.global_user_state,
                 'get_cluster_status_fields',
                 return_value={'svc-7': ('UP', mock.sentinel.updated_at)}):
            rm._recover_replica_operations()

        rm._terminate_replica.assert_not_called()

    def test_persisted_cap_reused_exactly_over_resolver(self):
        # The spec here resolves to 600; the persisted cap (written when
        # the retirement was scheduled) must win.
        kwargs = self._redrive(is_scale_down=True, persisted_cap=450)
        assert kwargs['in_flight_drain_cap_seconds'] == 450

    def test_persisted_zero_cap_is_reused_not_re_resolved(self):
        kwargs = self._redrive(is_scale_down=True, persisted_cap=0)
        assert kwargs['in_flight_drain_cap_seconds'] == 0

    def test_pre_field_row_falls_back_to_resolver(self):
        # An unpickled row from before the field existed has no
        # drain_cap_seconds attribute at all; getattr must default it.
        kwargs = self._redrive(is_scale_down=True)
        del kwargs  # Re-run with the attribute genuinely absent.
        rm = _scale_down_manager(spec_drain=600)
        rm._launch_thread_pool = {}
        rm._down_thread_pool = {}
        sp = mock.Mock()
        sp.is_scale_down = True
        sp.purged = False
        sp.preempted = False
        del sp.drain_cap_seconds
        info = mock.Mock()
        info.replica_id = 7
        info.status_property = sp
        info.status = (replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replicas_at_status',
                               return_value=[]), \
             mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info):
            rm._recover_replica_operations()
        kwargs = rm._terminate_replica.call_args.kwargs
        assert kwargs['in_flight_drain_cap_seconds'] == 600


class TestTerminateReplicaDrainAssembly:
    """Exercise the REAL _terminate_replica drain assembly (no mock of
    the method itself): deadline anchored after the SCHEDULED persist,
    tracker built only for non-pool replicas with a resolvable url, and
    the kwargs actually reaching the terminate thread."""

    def _assemble(self, is_pool=False, url='http://r1:8080', url_error=None):
        return self._assemble_impl(is_pool=is_pool,
                                   url=url,
                                   url_error=url_error)

    def _assemble_impl(self,
                       is_pool=False,
                       url='http://r1:8080',
                       url_error=None,
                       cap=300,
                       interrupted_launch=False):
        """Build a real manager and run the real _terminate_replica."""
        rm = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        rm._service_name = 'svc'
        rm._is_pool = is_pool
        rm._lb_in_flight_report = None
        rm.lock = threading.Lock()
        rm._launch_thread_pool = {}
        rm._down_thread_pool = {}
        rm._replica_to_request_id = {}
        rm._replica_to_launch_cancelled = {}
        if interrupted_launch:
            finished_launch = mock.Mock()
            finished_launch.is_alive.return_value = False
            rm._launch_thread_pool = {7: finished_launch}
            rm._replica_to_request_id = {7: 'req-7'}
        info = mock.Mock()
        info.cluster_name = 'svc-7-abc'
        info.status_property = replica_managers.ReplicaStatusProperty()
        if url_error is not None:
            type(info).url = mock.PropertyMock(side_effect=url_error)
        else:
            type(info).url = mock.PropertyMock(return_value=url)
        captured = {}

        class _FakeThread:

            def __init__(self, target, args=(), kwargs=None):
                captured['target'] = target
                captured['args'] = args
                captured['kwargs'] = kwargs or {}

        writes = []

        def _snapshot_write(_service_name, _replica_id, written_info):
            writes.append((written_info.status_property.sky_launch_status,
                           written_info.status_property.sky_down_status,
                           written_info.status_property.drain_cap_seconds))

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica',
                               side_effect=_snapshot_write), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(replica_managers.thread_utils, 'SafeThread',
                               _FakeThread):
            rm._terminate_replica(7,
                                  sync_down_logs=False,
                                  replica_drain_delay_seconds=0,
                                  is_scale_down=True,
                                  in_flight_drain_cap_seconds=cap)
        captured['writes'] = writes
        return captured

    def test_scheduled_write_persists_the_cap(self):
        # The cap must land in the same write as SCHEDULED so recovery
        # after a crash reuses it exactly (no re-resolution window).
        captured = self._assemble_impl(cap=450)
        scheduled = [
            w for w in captured['writes']
            if w[1] is replica_managers.common_utils.ProcessStatus.SCHEDULED
        ]
        assert scheduled and scheduled[0][2] == 450

    def test_interrupted_launch_write_persists_the_cap(self):
        # The INTERRUPTED row already derives SHUTTING_DOWN, so a crash
        # between it and the SCHEDULED write must also leave the cap.
        captured = self._assemble_impl(cap=450, interrupted_launch=True)
        first = captured['writes'][0]
        assert first == (
            replica_managers.common_utils.ProcessStatus.INTERRUPTED, None, 450)

    def test_deadline_and_tracker_reach_the_thread(self):
        before = replica_managers.time.monotonic()
        captured = self._assemble()
        kwargs = captured['kwargs']
        assert isinstance(kwargs['drain_complete'],
                          replica_managers._ReplicaDrainTracker)
        # Deadline anchored ~now (persist time) plus the cap.
        assert before + 300 <= kwargs['drain_deadline'] <= (
            replica_managers.time.monotonic() + 300 + 1)

    def test_zero_cap_skips_assembly_entirely(self):
        kwargs = self._assemble_impl(cap=0)['kwargs']
        assert kwargs['drain_deadline'] is None
        assert kwargs['drain_complete'] is None

    def test_pool_gets_bounded_sleep_only(self):
        kwargs = self._assemble(is_pool=True)['kwargs']
        assert kwargs['drain_complete'] is None
        assert kwargs['drain_deadline'] is not None

    def test_unresolvable_url_falls_back_to_bounded_sleep(self):
        kwargs = self._assemble(url_error=RuntimeError('no handle'))['kwargs']
        assert kwargs['drain_complete'] is None
        assert kwargs['drain_deadline'] is not None

    def test_url_none_falls_back_to_bounded_sleep(self):
        kwargs = self._assemble(url=None)['kwargs']
        assert kwargs['drain_complete'] is None


class TestStatusDerivationForRecovery:
    """Pin the real to_replica_status() combinations the recovery scan
    depends on: which teardown rows actually derive SHUTTING_DOWN (and
    are re-driven) vs PREEMPTED (invisible to the scan -- a pre-existing
    recovery gap documented outside this PR)."""

    @staticmethod
    def _props(**kwargs):
        props = replica_managers.ReplicaStatusProperty(
            sky_launch_status=replica_managers.common_utils.ProcessStatus.
            SUCCEEDED,
            sky_down_status=replica_managers.common_utils.ProcessStatus.
            SCHEDULED)
        for key, value in kwargs.items():
            setattr(props, key, value)
        return props

    def test_scale_down_row_derives_shutting_down(self):
        props = self._props(is_scale_down=True)
        assert (props.to_replica_status() ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)

    def test_purged_row_derives_shutting_down(self):
        props = self._props(is_scale_down=True, purged=True)
        assert (props.to_replica_status() ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)

    def test_failure_teardown_row_derives_shutting_down(self):
        props = self._props(user_app_failed=True)
        assert (props.to_replica_status() ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)

    def test_interrupted_launch_scale_down_derives_shutting_down(self):
        props = self._props(is_scale_down=True,
                            sky_launch_status=replica_managers.common_utils.
                            ProcessStatus.INTERRUPTED)
        assert (props.to_replica_status() ==
                replica_managers.serve_state.ReplicaStatus.SHUTTING_DOWN)

    def test_preempted_row_derives_preempted_not_shutting_down(self):
        # PREEMPTED wins the derivation, so the SHUTTING_DOWN recovery
        # scan never sees these rows: recovery of an interrupted preempted
        # teardown is a pre-existing gap, and the recovery branch must not
        # pretend to handle it.
        props = self._props(is_scale_down=True, preempted=True)
        assert (props.to_replica_status() ==
                replica_managers.serve_state.ReplicaStatus.PREEMPTED)


class TestSpecField:

    _BASE = {
        'readiness_probe': '/health',
        'replicas': 1,
    }

    def test_schema_accepts_field(self):
        config = dict(self._BASE, graceful_drain_seconds=300)
        jsonschema.validate(config, schemas.get_service_schema())

    def test_schema_and_yaml_round_trip_async_occupancy_declaration(self):
        config = dict(self._BASE, graceful_drain_async_occupancy=True)
        jsonschema.validate(config, schemas.get_service_schema())
        spec = service_spec_lib.SkyServiceSpec.from_yaml_config(config)
        assert spec.graceful_drain_async_occupancy is True
        assert spec.to_yaml_config()['graceful_drain_async_occupancy'] is True
        assert spec.copy().graceful_drain_async_occupancy is True
        assert spec.copy(graceful_drain_async_occupancy=False
                        ).graceful_drain_async_occupancy is False

    def test_async_occupancy_declaration_backfills_old_pickles(self):
        spec = service_spec_lib.SkyServiceSpec.from_yaml_config(dict(
            self._BASE))
        state = spec.__dict__.copy()
        del state['_graceful_drain_async_occupancy']
        restored = service_spec_lib.SkyServiceSpec.__new__(
            service_spec_lib.SkyServiceSpec)
        restored.__setstate__(state)
        assert restored.graceful_drain_async_occupancy is None

    def test_schema_rejects_negative(self):
        config = dict(self._BASE, graceful_drain_seconds=-1)
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(config, schemas.get_service_schema())

    def test_yaml_round_trip(self):
        spec = service_spec_lib.SkyServiceSpec.from_yaml_config(
            dict(self._BASE, graceful_drain_seconds=300))
        assert spec.graceful_drain_seconds == 300
        assert spec.to_yaml_config()['graceful_drain_seconds'] == 300

    def test_unset_defaults_to_none(self):
        spec = service_spec_lib.SkyServiceSpec.from_yaml_config(dict(
            self._BASE))
        assert spec.graceful_drain_seconds is None
        assert 'graceful_drain_seconds' not in spec.to_yaml_config()

    def test_setstate_backfills_old_pickles(self):
        spec = service_spec_lib.SkyServiceSpec.from_yaml_config(dict(
            self._BASE))
        state = spec.__dict__.copy()
        del state['_graceful_drain_seconds']
        restored = service_spec_lib.SkyServiceSpec.__new__(
            service_spec_lib.SkyServiceSpec)
        restored.__setstate__(state)
        assert restored.graceful_drain_seconds is None

    def test_constructor_rejects_negative(self):
        spec = service_spec_lib.SkyServiceSpec.from_yaml_config(dict(
            self._BASE))
        with pytest.raises(ValueError):
            spec.copy(graceful_drain_seconds=-1)

    def test_bounded_by_lb_occupancy_retention(self):
        # A drain longer than the LB's off-ready occupancy retention would
        # lose the unknown protection partway through.
        from sky.serve import constants as serve_constants
        limit = serve_constants.LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS
        spec = service_spec_lib.SkyServiceSpec.from_yaml_config(dict(
            self._BASE))
        assert spec.copy(
            graceful_drain_seconds=limit).graceful_drain_seconds == limit
        with pytest.raises(ValueError):
            spec.copy(graceful_drain_seconds=limit + 1)
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(
                dict(self._BASE, graceful_drain_seconds=limit + 1),
                schemas.get_service_schema())

    def test_hour_scale_job_cap_fits_under_the_bound(self):
        # A fleet whose async jobs run up to 3600s needs a cap strictly
        # above 3600 (a job admitted at retirement runs its full length
        # into the drain); the bound must keep accommodating ~3900.
        config = dict(self._BASE, graceful_drain_seconds=3900)
        jsonschema.validate(config, schemas.get_service_schema())
        spec = service_spec_lib.SkyServiceSpec.from_yaml_config(config)
        assert spec.graceful_drain_seconds == 3900

    def test_copy_preserves_and_overrides(self):
        spec = service_spec_lib.SkyServiceSpec.from_yaml_config(
            dict(self._BASE, graceful_drain_seconds=300))
        assert spec.copy().graceful_drain_seconds == 300
        assert spec.copy(graceful_drain_seconds=60).graceful_drain_seconds == 60
