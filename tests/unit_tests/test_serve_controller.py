"""Tests for sky/serve/controller.py.

Currently focused on `SkyServeController._get_lb_replica_info`, which builds
the `/controller/load_balancer_sync` response. Resolving a replica's url and
gpu_type is expensive (cluster handle fetch + endpoint query), so both must
be resolved at most once per replica lifetime and cached; the cache must be
pruned when a replica leaves the ready set.
"""
import threading
from typing import Dict, Optional
from unittest import mock

from sky.serve import controller
from sky.serve import serve_state


class _FakeHandle:
    """Stub for the resource handle returned by ReplicaInfo.handle()."""

    def __init__(self, accelerators: Optional[Dict[str, int]]) -> None:
        self.launched_resources = mock.Mock()
        self.launched_resources.accelerators = accelerators


class _FakeReplicaInfo:
    """ReplicaInfo stub that counts expensive url/handle resolutions."""

    def __init__(self,
                 replica_id: int,
                 status: serve_state.ReplicaStatus,
                 version: int = 1,
                 url: Optional[str] = None,
                 accelerators: Optional[Dict[str, int]] = None,
                 handle_is_none: bool = False) -> None:
        self.replica_id = replica_id
        self.status = status
        self.version = version
        self._url = url
        self._accelerators = accelerators
        self._handle_is_none = handle_is_none
        self.url_resolutions = 0
        self.handle_resolutions = 0

    @property
    def url(self) -> Optional[str]:
        self.url_resolutions += 1
        return self._url

    @property
    def is_ready(self) -> bool:
        return self.status == serve_state.ReplicaStatus.READY

    @property
    def is_terminal(self) -> bool:
        return self.status in serve_state.ReplicaStatus.terminal_statuses()

    def handle(self) -> Optional[_FakeHandle]:
        self.handle_resolutions += 1
        if self._handle_is_none:
            return None
        return _FakeHandle(self._accelerators)


def _make_controller() -> controller.SkyServeController:
    # Bypass __init__: it builds a real replica manager and autoscaler.
    ctrl = controller.SkyServeController.__new__(controller.SkyServeController)
    ctrl._service_name = 'svc'  # pylint: disable=protected-access
    ctrl._lb_replica_cache = {}  # pylint: disable=protected-access
    ctrl._lb_translation_cache = {}  # pylint: disable=protected-access
    return ctrl


class _FakeSpec:
    """Minimal SkyServiceSpec stub exposing the routing-spec properties."""

    def __init__(self,
                 load_balancing_policy,
                 target_qps_per_replica,
                 lb_stream_timeout_seconds,
                 lb_retriable_status_codes=None,
                 lb_max_retries=None,
                 lb_retry_initial_backoff_seconds=None) -> None:
        self.load_balancing_policy = load_balancing_policy
        self.target_qps_per_replica = target_qps_per_replica
        self.lb_stream_timeout_seconds = lb_stream_timeout_seconds
        self.lb_retriable_status_codes = lb_retriable_status_codes
        self.lb_max_retries = lb_max_retries
        self.lb_retry_initial_backoff_seconds = (
            lb_retry_initial_backoff_seconds)


class TestGetRoutingSpec:
    """The load_balancer_sync response ships the routing config so a running
    external LB picks up `sky serve update` changes without a re-roll."""

    def test_routing_spec_sourced_from_latest_version_spec(self):
        ctrl = _make_controller()
        spec = _FakeSpec(load_balancing_policy='instance_aware_least_load',
                         target_qps_per_replica={'L4': 2.5},
                         lb_stream_timeout_seconds=120,
                         lb_retriable_status_codes=[503],
                         lb_max_retries=3,
                         lb_retry_initial_backoff_seconds=0.5)
        with mock.patch.object(controller.serve_state,
                               'get_service_from_name',
                               return_value={'version': 7}), \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=spec) as get_spec:
            routing_spec = ctrl._get_routing_spec()  # pylint: disable=protected-access
        # Sourced from the latest (current) version's spec.
        get_spec.assert_called_once_with('svc', 7)
        assert routing_spec == {
            'load_balancing_policy_name': 'instance_aware_least_load',
            'target_qps_per_replica': {
                'L4': 2.5
            },
            # _FakeSpec has no concurrency knob; getattr resolves None.
            'target_concurrency_per_replica': None,
            'stream_timeout_seconds': 120,
            'retriable_status_codes': [503],
            'max_retries': 3,
            'retry_initial_backoff_seconds': 0.5,
        }

    def test_routing_spec_none_when_spec_unavailable(self):
        ctrl = _make_controller()
        with mock.patch.object(controller.serve_state,
                               'get_service_from_name',
                               return_value={'version': 3}), \
             mock.patch.object(controller.serve_state,
                               'get_spec',
                               return_value=None):
            assert ctrl._get_routing_spec() is None  # pylint: disable=protected-access


def _sync(ctrl: controller.SkyServeController, infos,
          active_versions=(1,)) -> Dict[str, Dict[str, str]]:
    record = {'active_versions': list(active_versions)}
    with mock.patch.object(controller.serve_state,
                           'get_service_from_name',
                           return_value=record):
        return ctrl._get_lb_replica_info(infos)  # pylint: disable=protected-access


class TestGetLbReplicaInfo:
    """Tests for the (url, gpu_type, gpu_count) per-replica cache behind
    the /controller/load_balancer_sync response."""

    def test_resolves_url_and_gpu_type_for_ready_replicas(self):
        ctrl = _make_controller()
        infos = [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.READY,
                             url='http://2.2.2.2:8080',
                             accelerators={'A100': 8}),
        ]
        assert _sync(ctrl, infos) == {
            'http://1.1.1.1:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            },
            'http://2.2.2.2:8080': {
                'gpu_type': 'A100',
                'gpu_count': '8'
            },
        }

    def test_resolution_happens_at_most_once_per_replica(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        first = _sync(ctrl, [info])
        second = _sync(ctrl, [info])
        assert first == second
        # url and handle must be resolved on the first sync only.
        assert info.url_resolutions == 1
        assert info.handle_resolutions == 1

    def test_not_ready_replicas_are_never_resolved(self):
        ctrl = _make_controller()
        provisioning = _FakeReplicaInfo(1,
                                        serve_state.ReplicaStatus.PROVISIONING)
        ready = _FakeReplicaInfo(2,
                                 serve_state.ReplicaStatus.READY,
                                 url='http://2.2.2.2:8080',
                                 accelerators={'L4': 1})
        result = _sync(ctrl, [provisioning, ready])
        assert result == {
            'http://2.2.2.2:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            }
        }
        assert provisioning.url_resolutions == 0
        assert provisioning.handle_resolutions == 0

    def test_inactive_version_replicas_are_excluded(self):
        ctrl = _make_controller()
        outdated = _FakeReplicaInfo(1,
                                    serve_state.ReplicaStatus.READY,
                                    version=1,
                                    url='http://1.1.1.1:8080',
                                    accelerators={'L4': 1})
        current = _FakeReplicaInfo(2,
                                   serve_state.ReplicaStatus.READY,
                                   version=2,
                                   url='http://2.2.2.2:8080',
                                   accelerators={'L4': 1})
        result = _sync(ctrl, [outdated, current], active_versions=(2,))
        assert result == {
            'http://2.2.2.2:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            }
        }
        assert outdated.url_resolutions == 0

    def test_unknown_gpu_type_when_unresolvable(self):
        """Both unresolvable cases (no handle yet, no accelerators) must
        fall back to 'unknown' instead of dropping the replica."""
        ctrl = _make_controller()
        no_handle = _FakeReplicaInfo(1,
                                     serve_state.ReplicaStatus.READY,
                                     url='http://1.1.1.1:8080',
                                     handle_is_none=True)
        no_accelerators = _FakeReplicaInfo(2,
                                           serve_state.ReplicaStatus.READY,
                                           url='http://2.2.2.2:8080',
                                           accelerators=None)
        assert _sync(ctrl, [no_handle, no_accelerators]) == {
            'http://1.1.1.1:8080': {
                'gpu_type': 'unknown',
                'gpu_count': '1'
            },
            'http://2.2.2.2:8080': {
                'gpu_type': 'unknown',
                'gpu_count': '1'
            },
        }

    def test_ready_replica_without_url_is_skipped_not_crashed(self):
        """A READY replica whose endpoint is briefly unresolvable (e.g.
        no head IP mid-recovery) must be skipped for the sync round,
        not crash load_balancer_sync (this was an assert)."""
        ctrl = _make_controller()
        no_url = _FakeReplicaInfo(1,
                                  serve_state.ReplicaStatus.READY,
                                  url=None,
                                  accelerators={'L4': 1})
        ok = _FakeReplicaInfo(2,
                              serve_state.ReplicaStatus.READY,
                              url='http://2.2.2.2:8080',
                              accelerators={'L4': 1})
        assert _sync(ctrl, [no_url, ok]) == {
            'http://2.2.2.2:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            }
        }
        # Not cached: it must be re-resolved on the next sync.
        assert 1 not in ctrl._lb_replica_cache  # pylint: disable=protected-access

    def test_cache_pruned_when_replica_leaves_ready_set(self):
        ctrl = _make_controller()
        info = _FakeReplicaInfo(1,
                                serve_state.ReplicaStatus.READY,
                                url='http://1.1.1.1:8080',
                                accelerators={'L4': 1})
        _sync(ctrl, [info])

        # The replica gets preempted: it must be dropped from the response
        # and pruned from the cache.
        preempted = _FakeReplicaInfo(1,
                                     serve_state.ReplicaStatus.NOT_READY,
                                     url='http://1.1.1.1:8080')
        assert not _sync(ctrl, [preempted])

        # The replica recovers with a new endpoint: it must be re-resolved
        # instead of served from a stale cache entry.
        recovered = _FakeReplicaInfo(1,
                                     serve_state.ReplicaStatus.READY,
                                     url='http://3.3.3.3:8080',
                                     accelerators={'L4': 1})
        assert _sync(ctrl, [recovered]) == {
            'http://3.3.3.3:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            }
        }
        assert recovered.url_resolutions == 1


class TestTranslateInFlight:
    """The LB reports in-flight work keyed by replica url; the autoscaler
    consumes it keyed by replica id. The controller inverts its
    (id -> url) sync cache to translate."""

    def _synced_controller(self):
        ctrl = _make_controller()
        infos = [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.READY,
                             url='http://2.2.2.2:8080',
                             accelerators={'L4': 1}),
        ]
        _sync(ctrl, infos)
        return ctrl

    def test_urls_translated_to_replica_ids(self):
        ctrl = self._synced_controller()
        translated = ctrl._translate_in_flight({  # pylint: disable=protected-access
            'http://1.1.1.1:8080': 3,
            'http://2.2.2.2:8080': 0,
        })
        assert translated == {1: 3, 2: 0}

    def test_unknown_url_is_dropped(self):
        # A url the controller never resolved (or whose replica went
        # terminal) has no live id to attribute the work to.
        ctrl = self._synced_controller()
        translated = ctrl._translate_in_flight({  # pylint: disable=protected-access
            'http://1.1.1.1:8080': 2,
            'http://9.9.9.9:8080': 7,
        })
        assert translated == {1: 2}

    def test_blipped_replica_stays_translatable(self):
        # A replica demoted from READY (probe blip) mid-job must stay
        # translatable while nonterminal: dropping it would erase its
        # in-flight unit AND make it read as an idle scale-down victim.
        ctrl = self._synced_controller()
        _sync(ctrl, [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.NOT_READY,
                             url='http://2.2.2.2:8080',
                             accelerators={'L4': 1}),
        ])
        translated = ctrl._translate_in_flight({  # pylint: disable=protected-access
            'http://1.1.1.1:8080': 0,
            'http://2.2.2.2:8080': 1,
        })
        assert translated == {1: 0, 2: 1}

    def test_terminal_replica_pruned_from_translation(self):
        ctrl = self._synced_controller()
        _sync(ctrl, [
            _FakeReplicaInfo(1,
                             serve_state.ReplicaStatus.READY,
                             url='http://1.1.1.1:8080',
                             accelerators={'L4': 1}),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.SHUTTING_DOWN,
                             url='http://2.2.2.2:8080',
                             accelerators={'L4': 1}),
        ])
        translated = ctrl._translate_in_flight(
            {  # pylint: disable=protected-access
                'http://2.2.2.2:8080': 1,
            })
        assert translated == {}

    def test_none_passes_through(self):
        # None means the LB sent no gauge (old LB / non-tracking policy);
        # the autoscaler must see None, not an empty (fresh-looking) dict.
        ctrl = self._synced_controller()
        assert ctrl._translate_in_flight(None) is None  # pylint: disable=protected-access


class _FakeAutoscaler:
    """Autoscaler stub for the capacity-hint computation."""

    def __init__(self, target, recomputed, latest_version=1) -> None:
        self._target = target
        self._recomputed = recomputed
        self.latest_version = latest_version

    def get_final_target_num_replicas(self) -> int:
        return self._target

    def has_recomputed_with_fresh_data(self) -> bool:
        return self._recomputed


class TestGetCapacityHint:
    """capacity_hint rides the sync response so the data plane can see
    capacity that is already on the way (provisioning) and the fleet's
    intended size (target)."""

    def _replicas(self):
        return [
            # Latest version: one READY, two provisioning-ish, one
            # terminal (must not count anywhere).
            _FakeReplicaInfo(1, serve_state.ReplicaStatus.READY, version=2),
            _FakeReplicaInfo(2,
                             serve_state.ReplicaStatus.PROVISIONING,
                             version=2),
            _FakeReplicaInfo(3, serve_state.ReplicaStatus.STARTING, version=2),
            _FakeReplicaInfo(4,
                             serve_state.ReplicaStatus.SHUTTING_DOWN,
                             version=2),
            # Old version replicas never count.
            _FakeReplicaInfo(5, serve_state.ReplicaStatus.READY, version=1),
        ]

    def test_provisioning_counts_latest_nonterminal_not_ready(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=5,
            recomputed=True,
            latest_version=2)
        hint = ctrl._get_capacity_hint(self._replicas())  # pylint: disable=protected-access
        assert hint == {'provisioning_replicas': 2, 'target_num_replicas': 5}

    def test_stale_autoscaler_reports_at_least_live_fleet(self):
        # A rebuilt controller (target reset to min_replicas, no demand
        # report yet) must not tell the platform the fleet wants to
        # shrink: while stale, target is floored at the latest-version
        # nonterminal count.
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=1,
            recomputed=False,
            latest_version=2)
        hint = ctrl._get_capacity_hint(self._replicas())  # pylint: disable=protected-access
        assert hint == {'provisioning_replicas': 2, 'target_num_replicas': 3}

    def test_stale_max_rule_keeps_larger_target(self):
        ctrl = _make_controller()
        ctrl._autoscaler = _FakeAutoscaler(  # pylint: disable=protected-access
            target=10,
            recomputed=False,
            latest_version=2)
        hint = ctrl._get_capacity_hint(self._replicas())  # pylint: disable=protected-access
        assert hint['target_num_replicas'] == 10


class TestReservedCapacityPollerStart:
    """Poller lifecycle: seeded, idempotent, inert without a placer."""

    def _controller_with(self, placer):
        ctrl = _make_controller()
        ctrl._replica_manager = mock.Mock()
        ctrl._replica_manager._spot_placer = placer
        ctrl._autoscaler = mock.Mock()
        ctrl._reserved_capacity_poller_started = False
        ctrl._reserved_capacity_poller_lock = threading.Lock()
        return ctrl

    def test_starts_thread_once(self):
        # Idempotent: one poller thread across repeated calls (boot +
        # any number of fill-enabling updates). Location seeding is
        # handled separately by _seed_fill_zero_cost_locations.
        placer = mock.Mock()
        ctrl = self._controller_with(placer)
        with mock.patch.object(controller.thread_utils,
                               'start_supervised_thread') as start_mock:
            ctrl._start_reserved_capacity_poller_if_needed()
            ctrl._start_reserved_capacity_poller_if_needed()
        assert start_mock.call_count == 1

    def test_without_placer_is_inert(self):
        ctrl = self._controller_with(placer=None)
        with mock.patch.object(controller.thread_utils,
                               'start_supervised_thread') as start_mock:
            ctrl._start_reserved_capacity_poller_if_needed()
        start_mock.assert_not_called()
        # Not marked started: a later update that adds a placer (new
        # service version) may still start it.
        assert ctrl._reserved_capacity_poller_started is False


class TestSeedFillZeroCostLocations:
    """The constructor-time seed is best-effort, never fatal."""

    def test_seed_failure_does_not_propagate(self):
        # zero_cost_locations() can hit a LIVE K8s feasibility check; an
        # unreachable context at boot must not crash-loop the controller
        # through __init__ -- the first successful poll re-seeds.
        ctrl = _make_controller()
        placer = mock.Mock()
        placer.zero_cost_locations.side_effect = RuntimeError('api down')
        ctrl._replica_manager = mock.Mock()
        ctrl._replica_manager._spot_placer = placer
        autoscaler = mock.Mock()
        autoscaler.reserved_capacity_fill = True
        ctrl._seed_fill_zero_cost_locations(autoscaler)
        autoscaler.seed_zero_cost_locations.assert_not_called()
