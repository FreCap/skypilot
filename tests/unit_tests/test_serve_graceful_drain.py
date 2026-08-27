"""Tests for the in-flight-aware graceful drain on replica retirement.

Retiring a replica (autoscaler scale-down, including rolling-update
retirement of outdated replicas) must wait for in-flight requests to
finish -- bounded by the per-service `graceful_drain_seconds` cap --
instead of sleeping a fixed 120s and then killing whatever is still
running.
"""
# pylint: disable=missing-class-docstring,protected-access
import contextlib
import threading
from unittest import mock

import jsonschema
import pytest

from sky import exceptions
from sky import skypilot_config
from sky.serve import constants as serve_constants
from sky.serve import replica_managers
from sky.serve import reserved_capacity
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


def _protocol_v2_cluster_record(workspace='mt_hybrid'):
    handle = replica_managers.cloud_vm_ray_backend.CloudVmRayResourceHandle.__new__(
        replica_managers.cloud_vm_ray_backend.CloudVmRayResourceHandle)
    handle.cluster_name = 'svc-1'
    handle.cluster_name_on_cloud = 'svc-1-cloud'
    handle.launched_resources = mock.Mock(
        cloud=replica_managers.clouds.Kubernetes(), region='phx-context')
    return {
        'workspace': workspace,
        'handle': handle,
        'cluster_hash': 'cluster-generation-a',
    }


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

    def test_terminate_retry_stops_when_cleanup_loses_ownership(self):
        context = mock.MagicMock()
        ownership = iter([True, False])
        with mock.patch.object(replica_managers.context,
                               'get',
                               return_value=context), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_from_name',
                               return_value=None), \
             mock.patch.object(replica_managers.usage_lib.messages.usage,
                               'set_internal'), \
             mock.patch('sky.core.down',
                        side_effect=RuntimeError('first request failed')) \
                 as down, \
             mock.patch.object(replica_managers.common_utils.Backoff,
                               'current_backoff',
                               return_value=0), \
             mock.patch.object(replica_managers.time, 'sleep'):
            with pytest.raises(RuntimeError, match='ownership was lost'):
                replica_managers.terminate_cluster.__wrapped__(
                    'svc-1', 0, continue_guard=lambda: next(ownership))

        down.assert_called_once_with('svc-1',
                                     _expected_cluster_record_uuid=None,
                                     _expected_cluster_record_handle=None)

    def test_terminate_pins_recorded_workspace_on_each_retry(self):
        context = mock.MagicMock()
        observed_workspaces = []

        def _down(cluster_name,
                  *,
                  _expected_cluster_record_uuid=None,
                  _expected_cluster_record_handle=None):
            assert cluster_name == 'svc-1'
            assert _expected_cluster_record_uuid is None
            assert _expected_cluster_record_handle is None
            observed_workspaces.append(skypilot_config.get_active_workspace())
            if len(observed_workspaces) == 1:
                raise RuntimeError('transient down failure')

        with mock.patch.object(replica_managers.context,
                               'get',
                               return_value=context), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_from_name',
                               return_value={
                                   'name': 'svc-1',
                                   'workspace': 'mt_hybrid',
                               }), \
             mock.patch.object(replica_managers.usage_lib.messages.usage,
                               'set_internal'), \
             mock.patch('sky.core.down', side_effect=_down), \
             mock.patch.object(replica_managers.common_utils.Backoff,
                               'current_backoff',
                               return_value=0), \
             mock.patch.object(replica_managers.time, 'sleep'):
            with skypilot_config.local_active_workspace_ctx('default'):
                replica_managers.terminate_cluster.__wrapped__('svc-1',
                                                               0,
                                                               max_retry=2)

        assert observed_workspaces == ['mt_hybrid', 'mt_hybrid']

    def test_terminate_missing_record_downs_without_workspace_ctx(self):
        context = mock.MagicMock()
        observed_workspaces = []

        def _down(cluster_name,
                  *,
                  _expected_cluster_record_uuid=None,
                  _expected_cluster_record_handle=None):
            assert _expected_cluster_record_uuid is None
            assert _expected_cluster_record_handle is None
            observed_workspaces.append(skypilot_config.get_active_workspace())
            raise exceptions.ClusterDoesNotExist(cluster_name)

        with mock.patch.object(replica_managers.context,
                               'get',
                               return_value=context), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_from_name',
                               return_value=None), \
             mock.patch.object(replica_managers.usage_lib.messages.usage,
                               'set_internal'), \
             mock.patch('sky.core.down', side_effect=_down), \
             mock.patch.object(replica_managers.time, 'sleep'):
            with skypilot_config.local_active_workspace_ctx('default'):
                # No cluster record: teardown must not enter a workspace
                # context, and ClusterDoesNotExist must return cleanly
                # without retries.
                replica_managers.terminate_cluster.__wrapped__('svc-1',
                                                               0,
                                                               max_retry=1)

        assert observed_workspaces == ['default']

    def test_protocol_v2_missing_record_is_cleanup_uncertainty(self):
        context = mock.MagicMock()
        down = mock.MagicMock()
        absence_error = exceptions.KubernetesPhysicalClusterIdentityError(
            'post-teardown physical absence is unproven')
        cleanup_fence = reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='phx-context', physical_cluster_uid='physical-a')
        with mock.patch.object(replica_managers.context,
                               'get',
                               return_value=context), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_from_name',
                               return_value=None), \
             mock.patch('sky.core.down', down), \
             mock.patch.object(
                 replica_managers,
                 '_wait_for_post_teardown_physical_absence',
                 side_effect=absence_error) as wait_for_absence, \
             pytest.raises(
                 exceptions.KubernetesPhysicalClusterIdentityError,
                 match='physical absence is unproven'):
            replica_managers.terminate_cluster.__wrapped__(
                'svc-1', 0, cleanup_fence=cleanup_fence)

        down.assert_not_called()
        wait_for_absence.assert_called_once()

    def test_protocol_v2_capture_runs_inside_recorded_workspace(self):
        context = mock.MagicMock()
        observed_workspaces = []
        cleanup_fence = reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='phx-context', physical_cluster_uid='physical-a')

        @contextlib.contextmanager
        def _fence(_context, _uid):
            observed_workspaces.append(skypilot_config.get_active_workspace())
            yield

        with mock.patch.object(replica_managers.context,
                               'get',
                               return_value=context), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_from_name',
                               return_value=_protocol_v2_cluster_record()), \
             mock.patch.object(
                 replica_managers.kubernetes_adaptor,
                 'physical_cluster_uid_fence',
                 side_effect=_fence), \
             mock.patch.object(replica_managers.usage_lib.messages.usage,
                               'set_internal'), \
             mock.patch('sky.core.down'), \
             mock.patch.object(
                 replica_managers,
                 '_wait_for_post_teardown_physical_absence') as wait_for_absence:
            with skypilot_config.local_active_workspace_ctx('default'):
                replica_managers.terminate_cluster.__wrapped__(
                    'svc-1', 0, cleanup_fence=cleanup_fence)

        assert observed_workspaces == ['mt_hybrid']
        wait_for_absence.assert_called_once()

    def test_protocol_v2_rejects_mismatched_durable_handle_before_capture(self):
        context = mock.MagicMock()
        cleanup_fence = reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='phx-context', physical_cluster_uid='physical-a')
        record = _protocol_v2_cluster_record()
        record['handle'].launched_resources.region = 'retargeted-context'
        with mock.patch.object(replica_managers.context,
                               'get',
                               return_value=context), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_from_name',
                               return_value=record), \
             mock.patch.object(
                 replica_managers.kubernetes_adaptor,
                 'physical_cluster_uid_fence') as provider_fence, \
             mock.patch('sky.core.down') as down, \
             mock.patch.object(replica_managers.time, 'sleep'), \
             pytest.raises(
                 exceptions.KubernetesPhysicalClusterIdentityError,
                 match='handle does not match'):
            replica_managers.terminate_cluster.__wrapped__(
                'svc-1', 0, cleanup_fence=cleanup_fence)

        provider_fence.assert_not_called()
        down.assert_not_called()

    def test_protocol_v2_rejects_handle_without_launched_resources(self):
        context = mock.MagicMock()
        cleanup_fence = reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='phx-context', physical_cluster_uid='physical-a')
        record = _protocol_v2_cluster_record()
        record['handle'].launched_resources = None
        with mock.patch.object(replica_managers.context,
                               'get',
                               return_value=context), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_from_name',
                               return_value=record), \
             mock.patch.object(
                 replica_managers.kubernetes_adaptor,
                 'physical_cluster_uid_fence') as provider_fence, \
             mock.patch('sky.core.down') as down, \
             pytest.raises(
                 exceptions.KubernetesPhysicalClusterIdentityError,
                 match='handle does not match'):
            replica_managers.terminate_cluster.__wrapped__(
                'svc-1', 0, cleanup_fence=cleanup_fence)

        provider_fence.assert_not_called()
        down.assert_not_called()

    def test_protocol_v2_cluster_disappearing_is_not_success(self):
        context = mock.MagicMock()
        down = mock.MagicMock(
            side_effect=exceptions.ClusterDoesNotExist('svc-1'))
        cleanup_fence = reserved_capacity.ProtocolV2CleanupFence(
            kubernetes_context='phx-context', physical_cluster_uid='physical-a')
        absence_error = exceptions.KubernetesPhysicalClusterIdentityError(
            'post-teardown physical absence is unproven')
        with mock.patch.object(replica_managers.context,
                               'get',
                               return_value=context), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_from_name',
                               return_value=_protocol_v2_cluster_record()), \
             mock.patch.object(
                 replica_managers.kubernetes_adaptor,
                 'physical_cluster_uid_fence',
                 return_value=contextlib.nullcontext()), \
             mock.patch.object(replica_managers.usage_lib.messages.usage,
                               'set_internal'), \
             mock.patch('sky.core.down', down), \
             mock.patch.object(
                 replica_managers,
                 '_wait_for_post_teardown_physical_absence',
                 side_effect=absence_error) as wait_for_absence, \
             pytest.raises(
                 exceptions.KubernetesPhysicalClusterIdentityError,
                 match='physical absence is unproven'):
            replica_managers.terminate_cluster.__wrapped__(
                'svc-1', 0, cleanup_fence=cleanup_fence)

        down.assert_called_once()
        wait_for_absence.assert_called_once()

    def test_terminate_plain_value_error_retries_then_raises(self):
        # ValueErrors other than ClusterDoesNotExist must NOT be treated
        # as "already terminated": they retry and ultimately fail loudly.
        context = mock.MagicMock()
        down = mock.MagicMock(side_effect=ValueError('malformed handle'))

        with mock.patch.object(replica_managers.context,
                               'get',
                               return_value=context), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_from_name',
                               return_value={
                                   'name': 'svc-1',
                                   'workspace': 'mt_hybrid',
                               }), \
             mock.patch.object(replica_managers.usage_lib.messages.usage,
                               'set_internal'), \
             mock.patch('sky.core.down', down), \
             mock.patch.object(replica_managers.common_utils.Backoff,
                               'current_backoff',
                               return_value=0), \
             mock.patch.object(replica_managers.time, 'sleep'):
            with pytest.raises(RuntimeError, match='Failed to terminate'):
                replica_managers.terminate_cluster.__wrapped__('svc-1',
                                                               0,
                                                               max_retry=2)

        assert down.call_count == 2


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

    def test_fresh_pre_drain_routing_report_seeds_seen(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(999.0, {}, {self.URL})
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1000.0):
            tracker = self._tracker(rm)
            # The report only acknowledges the backend. It predates the drain
            # and cannot itself prove the backend idle.
            assert not tracker()
            rm._lb_in_flight_report = self._report(1001.0, {}, set())
            assert tracker()

    def test_wave_seed_freshness_is_frozen_before_serial_admission(self):
        rm = _manager()
        seed_report = self._report(999.0, {}, {self.URL})
        # Registration happens much later in a large serial retirement wave.
        # Freshness belongs to the instant the wave captured the report, not
        # the instant this tail tracker was finally constructed.
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1100.0):
            tracker = replica_managers._ReplicaDrainTracker(
                rm,
                self.URL,
                1100.0,
                seed_report=seed_report,
                seed_report_captured_at=1000.0)
        rm._lb_in_flight_report = self._report(1101.0, {}, set())
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1102.0):
            assert tracker()

    def test_seed_report_at_drain_start_cannot_finish_drain(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(1000.0, {}, {self.URL})
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1000.0):
            tracker = self._tracker(rm)
            # Exercise the normal blocked predicate, not the pre-drain guard.
            assert not tracker()
            rm._lb_in_flight_report = self._report(1001.0, {}, set())
            assert tracker()

    def test_session_change_clears_pre_drain_seed(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(999.0, {}, {self.URL},
                                               session='lb-a')
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1000.0):
            tracker = self._tracker(rm)
            rm._lb_in_flight_report = self._report(1001.0, {},
                                                   set(),
                                                   session='lb-b')
            assert not tracker()
            rm._lb_in_flight_report = self._report(1002.0, {}, {self.URL},
                                                   session='lb-b')
            assert not tracker()
            rm._lb_in_flight_report = self._report(1003.0, {},
                                                   set(),
                                                   session='lb-b')
            assert tracker()

    def test_absent_pre_drain_report_does_not_seed_seen(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(999.0, {}, {self.OTHER})
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1000.0):
            tracker = self._tracker(rm)
            rm._lb_in_flight_report = self._report(1001.0, {}, {self.OTHER})
            assert not tracker()

    def test_fresh_pre_drain_unknown_report_seeds_taint(self):
        rm = _manager()
        rm._lb_in_flight_report = self._report(999.0, {}, {self.URL},
                                               unknown_urls={self.URL})
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1000.0):
            tracker = self._tracker(rm)
            rm._lb_in_flight_report = self._report(1001.0, {}, set())
            assert not tracker()
            rm._lb_in_flight_report = self._report(1002.0, {self.URL: 0}, set())
            assert tracker()

    def test_stale_pre_drain_report_does_not_seed_seen(self):
        rm = _manager()
        stale_at = (1000.0 -
                    replica_managers._IN_FLIGHT_REPORT_STALENESS_SECONDS - 1)
        rm._lb_in_flight_report = self._report(stale_at, {}, {self.URL})
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=1000.0):
            tracker = self._tracker(rm)
            rm._lb_in_flight_report = self._report(1001.0, {}, set())
            assert not tracker()

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


class TestResolveDrainCapInfoReuse:
    """Passing an in-hand ReplicaInfo must skip the redundant DB read."""

    def test_in_hand_info_skips_db_read(self):
        rm = _scale_down_manager(spec_drain=600)
        info = mock.Mock()
        info.version = 3
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id') as db_read:
            cap = rm._resolve_drain_cap_seconds(7, info)
        db_read.assert_not_called()
        assert cap == 600
        rm._get_version_spec.assert_called_once_with(3)

    def test_no_info_still_reads_db(self):
        rm = _scale_down_manager(spec_drain=600)
        info = mock.Mock()
        info.version = 3
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info) as db_read:
            cap = rm._resolve_drain_cap_seconds(7)
        db_read.assert_called_once_with('svc', 7)
        assert cap == 600

    def test_spec_failure_with_in_hand_info_falls_back(self):
        rm = _scale_down_manager(spec_drain=None,
                                 spec_error=ValueError('version gone'))
        info = mock.Mock()
        info.version = 3
        cap = rm._resolve_drain_cap_seconds(7, info)
        assert cap == replica_managers._DEFAULT_DRAIN_SECONDS


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
    """A recovered retirement must re-enter its remaining bounded drain."""

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
                               return_value=[info]):
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

        def _record_recovered_preemption(_service_name, _replica_id, _info, *,
                                         expected_replica_exists,
                                         **_fence_kwargs):
            assert expected_replica_exists is True
            writes.append(sp.preempted)
            return True

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_infos',
                               return_value=[info]), \
             mock.patch.object(replica_managers.global_user_state,
                               'get_cluster_status_fields',
                               return_value={}), \
             mock.patch.object(
                 replica_managers.serve_state,
                 'add_or_update_replica',
                 side_effect=_record_recovered_preemption):
            rm._recover_replica_operations()

        assert writes == [True]
        rm._spot_placer.set_preemptive.assert_called_once_with(location)
        kwargs = rm._terminate_replica.call_args.kwargs
        assert kwargs['is_scale_down'] is True
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

    def test_materialized_pre_field_row_falls_back_to_resolver(self):
        # ReplicaInfo's decode boundary materializes a missing legacy cap as
        # None. Recovery consumes that explicit value and resolves the spec.
        kwargs = self._redrive(is_scale_down=True, persisted_cap=None)
        assert kwargs['in_flight_drain_cap_seconds'] == 600


class TestTerminateReplicaDrainAssembly:
    """Exercise the REAL _terminate_replica drain assembly (no mock of
    the method itself): deadline anchored after the SCHEDULED persist,
    no provider-backed probe lookup under the manager lock, and the kwargs
    actually reaching the terminate thread."""

    def _assemble(self, is_pool=False, url='http://r1:8080', url_error=None):
        return self._assemble_impl(is_pool=is_pool,
                                   url=url,
                                   url_error=url_error)

    def _assemble_impl(self,
                       is_pool=False,
                       url='http://r1:8080',
                       url_error=None,
                       cap=300,
                       interrupted_launch=False,
                       started_at=None,
                       wall_now=1000.0,
                       monotonic_now=2000.0):
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
        if url_error is not None:
            rm._resolve_probe_urls = mock.Mock(side_effect=url_error)
        else:
            rm._resolve_probe_urls = mock.Mock(return_value={7: url})
        info = mock.Mock()
        info.cluster_name = 'svc-7-abc'
        info.replica_record_id = '00000000-0000-4000-8000-000000000007'
        info.status_property = replica_managers.ReplicaStatusProperty()
        info.status_property.drain_started_at = started_at
        if interrupted_launch:
            finished_launch = replica_managers._ReplicaLaunchThread.__new__(
                replica_managers._ReplicaLaunchThread)
            finished_launch.replica_record_id = info.replica_record_id
            finished_launch.service_hash = rm._service_hash
            finished_launch.controller_owner = rm._controller_owner
            finished_launch.teardown_requested = mock.Mock()
            rm._launch_thread_pool = {7: finished_launch}
            rm._replica_to_request_id = {7: 'req-7'}
        if url_error is not None:
            type(info).url = mock.PropertyMock(side_effect=url_error)
        else:
            type(info).url = mock.PropertyMock(return_value=url)
        captured = {}

        class _FakeThread:

            def __init__(self, *args, **kwargs):
                captured['thread_args'] = args
                captured['thread_kwargs'] = kwargs
                captured['target'] = kwargs['target']
                captured['args'] = kwargs.get('args', ())
                captured['kwargs'] = kwargs.get('kwargs', {})

        writes = []

        def _snapshot_write(_service_name, _replica_id, written_info, *,
                            expected_replica_exists, **_fence_kwargs):
            assert expected_replica_exists is True
            writes.append((written_info.status_property.sky_launch_status,
                           written_info.status_property.sky_down_status,
                           written_info.status_property.drain_cap_seconds,
                           written_info.status_property.drain_started_at))
            return True

        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(
                replica_managers.serve_state,
                'get_replica_info_with_resource_action_identity',
                return_value=(info, None)), \
             mock.patch.object(replica_managers.serve_state,
                               'add_or_update_replica',
                               side_effect=_snapshot_write), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(replica_managers, '_ReplicaDownThread',
                               _FakeThread), \
             mock.patch.object(replica_managers.time,
                               'time', return_value=wall_now), \
             mock.patch.object(replica_managers.time,
                               'monotonic', return_value=monotonic_now):
            rm._terminate_replica(7,
                                  replica_drain_delay_seconds=0,
                                  is_scale_down=True,
                                  in_flight_drain_cap_seconds=cap)
        captured['writes'] = writes
        captured['resolve_probe_urls'] = rm._resolve_probe_urls
        return captured

    def test_scheduled_write_persists_the_cap(self):
        # The cap must land in the same write as SCHEDULED so recovery
        # after a crash reuses it exactly (no re-resolution window).
        captured = self._assemble_impl(cap=450)
        scheduled = [
            w for w in captured['writes']
            if w[1] is replica_managers.common_utils.ProcessStatus.SCHEDULED
        ]
        assert scheduled and scheduled[0][2:] == (450, 1000.0)

    def test_interrupted_launch_write_persists_the_cap(self):
        # The INTERRUPTED row already derives SHUTTING_DOWN, so a crash
        # between it and the SCHEDULED write must also leave the cap.
        captured = self._assemble_impl(cap=450, interrupted_launch=True)
        first = captured['writes'][0]
        assert first == (
            replica_managers.common_utils.ProcessStatus.INTERRUPTED, None, 450,
            1000.0)

    def test_deadline_reaches_thread_without_probe_tracker(self):
        captured = self._assemble()
        kwargs = captured['kwargs']
        assert kwargs['drain_complete'] is None
        assert kwargs['drain_deadline'] == 2300.0
        captured['resolve_probe_urls'].assert_not_called()

    def test_recovery_consumes_only_remaining_cap(self):
        first = self._assemble_impl(cap=600,
                                    started_at=700.0,
                                    wall_now=1000.0,
                                    monotonic_now=2000.0)
        second = self._assemble_impl(cap=600,
                                     started_at=700.0,
                                     wall_now=1100.0,
                                     monotonic_now=3000.0)

        assert first['kwargs']['drain_deadline'] == 2300.0
        assert second['kwargs']['drain_deadline'] == 3200.0
        assert first['writes'][-1][3] == second['writes'][-1][3] == 700.0

    def test_expired_recovery_deadline_is_immediate(self):
        captured = self._assemble_impl(cap=300,
                                       started_at=100.0,
                                       wall_now=1000.0,
                                       monotonic_now=2000.0)
        assert captured['kwargs']['drain_deadline'] == 2000.0

    def test_far_future_timestamp_gets_one_durable_full_cap(self):
        captured = self._assemble_impl(cap=300,
                                       started_at=2000.0,
                                       wall_now=1000.0,
                                       monotonic_now=3000.0)
        assert captured['writes'][-1][3] == 1000.0
        assert captured['kwargs']['drain_deadline'] == 3300.0

    def test_small_future_clock_skew_fails_closed(self):
        captured = self._assemble_impl(cap=300,
                                       started_at=1100.0,
                                       wall_now=1000.0,
                                       monotonic_now=3000.0)
        assert captured['writes'][-1][3] == 1100.0
        assert captured['kwargs']['drain_deadline'] == 3300.0

    def test_zero_cap_skips_assembly_entirely(self):
        kwargs = self._assemble_impl(cap=0)['kwargs']
        assert kwargs['drain_deadline'] is None
        assert kwargs['drain_complete'] is None

    def test_pool_gets_bounded_sleep_only(self):
        kwargs = self._assemble(is_pool=True)['kwargs']
        assert kwargs['drain_complete'] is None
        assert kwargs['drain_deadline'] is not None

    def test_provider_cleanup_does_not_resolve_probe_url(self):
        captured = self._assemble(url_error=RuntimeError('no handle'))
        kwargs = captured['kwargs']
        assert kwargs['drain_complete'] is None
        assert kwargs['drain_deadline'] is not None
        captured['resolve_probe_urls'].assert_not_called()

    def test_url_none_falls_back_to_bounded_sleep(self):
        kwargs = self._assemble(url=None)['kwargs']
        assert kwargs['drain_complete'] is None


class TestRecoveredStrictDrainDeadline:

    @staticmethod
    def _manager_and_info(started_at):
        rm = replica_managers.SkyPilotReplicaManager.__new__(
            replica_managers.SkyPilotReplicaManager)
        rm._service_name = 'svc'
        rm._is_pool = False
        rm._wait_for_idle_trackers = {}
        rm._lb_in_flight_report = None
        rm._persist_replica = mock.Mock()
        rm._resolve_probe_urls = mock.Mock(return_value={7: 'http://r7:8080'})
        info = mock.Mock(replica_id=7, cluster_name='svc-7')
        info.status_property = replica_managers.ReplicaStatusProperty(
            drain_cap_seconds=600, drain_started_at=started_at)
        type(info).url = mock.PropertyMock(return_value='http://r7:8080')
        return rm, info

    def test_recovery_reuses_durable_start_and_defers_url_resolution(self):
        rm, info = self._manager_and_info(700.0)
        with mock.patch.object(replica_managers.time,
                               'time', return_value=1000.0), \
             mock.patch.object(replica_managers.time,
                               'monotonic', return_value=2000.0):
            rm._register_wait_for_idle(info)

        state = rm._wait_for_idle_trackers[7]
        assert state.deadline == 2300.0
        assert info.status_property.drain_started_at == 700.0
        rm._persist_replica.assert_not_called()
        assert state.tracker is None
        assert state.needs_url_resolution
        rm._resolve_probe_urls.assert_not_called()

    def test_recovery_uses_pre_resolved_url_without_property_lookup(self):
        rm, info = self._manager_and_info(700.0)
        type(info).url = mock.PropertyMock(
            side_effect=AssertionError('unexpected per-replica lookup'))
        with mock.patch.object(replica_managers.time,
                               'time', return_value=1000.0), \
             mock.patch.object(replica_managers.time,
                               'monotonic', return_value=2000.0):
            rm._register_wait_for_idle(info,
                                       replica_url='http://batched-r7:8080')

        state = rm._wait_for_idle_trackers[7]
        tracker = state.tracker
        assert tracker is not None
        assert state.deadline == 2300.0
        assert not state.needs_url_resolution
        rm._lb_in_flight_report = (2001.0, {
            'http://batched-r7:8080': 0
        }, set(), set(), set(), 'lb-a')
        with mock.patch.object(replica_managers.time,
                               'monotonic',
                               return_value=2001.0):
            assert tracker()

    def test_legacy_row_backfill_is_durable_before_tracker_registration(self):
        rm, info = self._manager_and_info(None)
        with mock.patch.object(replica_managers.time,
                               'time', return_value=1000.0), \
             mock.patch.object(replica_managers.time,
                               'monotonic', return_value=2000.0):
            rm._register_wait_for_idle(info)

        assert info.status_property.drain_started_at == 1000.0
        rm._persist_replica.assert_called_once_with(7, info)
        assert rm._wait_for_idle_trackers[7].deadline == 2600.0

    def test_backfill_failure_does_not_admit_in_memory_drain(self):
        rm, info = self._manager_and_info(None)
        rm._persist_replica.side_effect = RuntimeError('db unavailable')
        with mock.patch.object(replica_managers.time,
                               'time', return_value=1000.0), \
             mock.patch.object(replica_managers.time,
                               'monotonic', return_value=2000.0), \
             pytest.raises(RuntimeError, match='db unavailable'):
            rm._register_wait_for_idle(info)

        assert not rm._wait_for_idle_trackers

    def test_deferred_off_route_write_atomically_stamps_start(self):
        rm, info = self._manager_and_info(None)
        rm._logical_controller_epoch = 'epoch-a'
        rm._resolve_drain_cap_seconds = mock.Mock(return_value=600)
        rm._register_wait_for_idle = mock.Mock()
        order = mock.Mock()
        rm._persist_replica = order.persist
        rm._register_wait_for_idle = order.register
        with mock.patch.object(replica_managers.serve_state,
                               'get_replica_info_from_id',
                               return_value=info), \
             mock.patch.object(replica_managers.global_user_state,
                               'cluster_with_name_exists',
                               return_value=True), \
             mock.patch.object(replica_managers.time,
                               'time', return_value=1000.0):
            rm._defer_scale_down_until_idle(7, (3, 4, 8))

        assert info.status_property.drain_cap_seconds == 600
        assert info.status_property.drain_started_at == 1000.0
        assert order.mock_calls == [
            mock.call.persist(7, info),
            mock.call.register(
                info, replica_url=replica_managers._REPLICA_URL_NOT_PROVIDED)
        ]


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
