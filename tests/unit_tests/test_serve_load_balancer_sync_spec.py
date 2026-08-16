"""Tests for the external LB applying a controller-fetched routing spec.

`SkyServeLoadBalancer._apply_routing_spec` is how `sky serve update` reaches a
running load balancer without a re-roll: the load-balancing policy, per-replica
target QPS, and stream timeout arrive over the sync channel and are applied
live. These tests exercise that handler on parsed state (which policy object is
active, its target-qps map, the stored stream timeout) -- not on any logging.
"""
# pylint: disable=invalid-name,protected-access
import asyncio
import types
from unittest import mock

import pytest

from sky.serve import constants
from sky.serve import load_balancer
from sky.serve import load_balancing_policies as lb_policies


def _make_lb(policy_name=None) -> load_balancer.SkyServeLoadBalancer:
    lb = load_balancer.SkyServeLoadBalancer(controller_url='http://ctrl:8001',
                                            load_balancer_port=8890)
    if policy_name is not None:
        lb._apply_routing_spec({'load_balancing_policy_name': policy_name})
    return lb


def test_defaults_until_synced():
    # The external LB uses safe defaults until controller sync supplies the
    # real routing spec; there is no launch-time seed.
    lb = _make_lb()
    assert (lb._load_balancing_policy_name == lb_policies.DEFAULT_LB_POLICY)
    assert (lb._stream_timeout_seconds == constants.DEFAULT_LB_STREAM_TIMEOUT)


def test_policy_swap_rebuilds_and_updates_name():
    lb = _make_lb()  # default: least_load
    assert not isinstance(lb._load_balancing_policy,
                          lb_policies.InstanceAwareLeastLoadPolicy)
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'instance_aware_least_load',
        'target_qps_per_replica': {
            'L4': 2.5
        },
        'stream_timeout_seconds': 90,
    })
    # Policy object swapped to the instance-aware policy...
    assert isinstance(lb._load_balancing_policy,
                      lb_policies.InstanceAwareLeastLoadPolicy)
    assert lb._load_balancing_policy_name == 'instance_aware_least_load'
    # ...target QPS applied to the (now instance-aware) policy...
    assert (lb._load_balancing_policy.target_qps_per_accelerator == {'L4': 2.5})
    # ...and the stream timeout is live.
    assert lb._stream_timeout_seconds == 90


def test_policy_not_rebuilt_when_name_unchanged():
    lb = _make_lb('instance_aware_least_load')
    policy_obj = lb._load_balancing_policy
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'instance_aware_least_load',
        'target_qps_per_replica': {
            'A100': 4.0
        },
        'stream_timeout_seconds': 30,
    })
    # Same policy name -> the object is not rebuilt (identity preserved),
    # but target QPS / stream timeout still update in place.
    assert lb._load_balancing_policy is policy_obj
    assert (lb._load_balancing_policy.target_qps_per_accelerator == {
        'A100': 4.0
    })
    assert lb._stream_timeout_seconds == 30


def test_target_qps_ignored_for_non_instance_aware_policy():
    lb = _make_lb('round_robin')
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'round_robin',
        'target_qps_per_replica': {
            'L4': 2.5
        },
        'stream_timeout_seconds': 60,
    })
    # A non-instance-aware policy has no target-qps state to set; only the
    # stream timeout applies.
    assert lb._load_balancing_policy_name == 'round_robin'
    assert not hasattr(lb._load_balancing_policy, 'target_qps_per_accelerator')
    assert lb._stream_timeout_seconds == 60


def test_concurrency_knob_sets_uniform_per_gpu_weight():
    # A concurrency-sized service ships no QPS dict; the knob weights
    # replicas per-GPU so bigger replicas absorb proportional load.
    lb = _make_lb('instance_aware_least_load')
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'instance_aware_least_load',
        'target_qps_per_replica': None,
        'target_concurrency_per_replica': 2.0,
        'stream_timeout_seconds': 60,
    })
    policy = lb._load_balancing_policy
    assert policy._get_target_qps_for_accelerator('L4', 4) == 8.0
    assert policy._get_target_qps_for_accelerator('A100', 1) == 2.0


def test_concurrency_update_clears_stale_qps_weights():
    # v1 (QPS dict) -> v2 (concurrency knob): keeping the old dict would
    # normalize routing with obsolete per-accelerator targets forever.
    lb = _make_lb('instance_aware_least_load')
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'instance_aware_least_load',
        'target_qps_per_replica': {
            'L4': 0.1,
            'A100': 10.0
        },
        'stream_timeout_seconds': 60,
    })
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'instance_aware_least_load',
        'target_qps_per_replica': None,
        'target_concurrency_per_replica': 1.0,
        'stream_timeout_seconds': 60,
    })
    policy = lb._load_balancing_policy
    assert policy.target_qps_per_accelerator == {}
    # Uniform per-GPU weighting replaced the stale dict wholesale.
    assert policy._get_target_qps_for_accelerator('L4', 1) == 1.0
    assert policy._get_target_qps_for_accelerator('A100', 1) == 1.0


def test_qps_dict_update_clears_per_gpu_default():
    # The reverse switch (concurrency -> QPS dict) must also not mix
    # weighting modes: the concrete dict wins.
    lb = _make_lb('instance_aware_least_load')
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'instance_aware_least_load',
        'target_qps_per_replica': None,
        'target_concurrency_per_replica': 1.0,
        'stream_timeout_seconds': 60,
    })
    lb._apply_routing_spec({
        'load_balancing_policy_name': 'instance_aware_least_load',
        'target_qps_per_replica': {
            'L4': 2.5
        },
        'stream_timeout_seconds': 60,
    })
    policy = lb._load_balancing_policy
    assert policy._default_per_gpu_qps is None
    assert policy._get_target_qps_for_accelerator('L4', 1) == 2.5


def test_target_qps_resolution_memoized_per_shape():
    policy = lb_policies.InstanceAwareLeastLoadPolicy()
    policy.set_target_qps_per_accelerator({'L4:1': 2.0})

    with mock.patch.object(
            policy,
            '_resolve_target_qps_for_accelerator',
            wraps=policy._resolve_target_qps_for_accelerator) as resolve:
        assert policy._get_target_qps_for_accelerator('L4', 4) == 8.0
        assert policy._get_target_qps_for_accelerator('L4', 4) == 8.0
        assert policy._get_target_qps_for_accelerator('L4', 1) == 2.0
        assert policy._get_target_qps_for_accelerator('L4', 1) == 2.0

    assert resolve.call_args_list == [
        mock.call('L4', 4),
        mock.call('L4', 1),
    ]


def test_target_qps_cache_invalidated_by_qps_update():
    policy = lb_policies.InstanceAwareLeastLoadPolicy()
    policy.set_target_qps_per_accelerator({'L4': 1.0})
    assert policy._get_target_qps_for_accelerator('L4', 2) == 2.0

    policy.set_target_qps_per_accelerator({'L4': 3.0})
    with mock.patch.object(
            policy,
            '_resolve_target_qps_for_accelerator',
            wraps=policy._resolve_target_qps_for_accelerator) as resolve:
        assert policy._get_target_qps_for_accelerator('L4', 2) == 6.0
        assert policy._get_target_qps_for_accelerator('L4', 2) == 6.0

    resolve.assert_called_once_with('L4', 2)


def test_target_qps_update_copies_input():
    policy = lb_policies.InstanceAwareLeastLoadPolicy()
    target_qps = {'L4': 1.0}
    policy.set_target_qps_per_accelerator(target_qps)

    target_qps['L4'] = 3.0

    assert policy.target_qps_per_accelerator == {'L4': 1.0}
    assert policy._get_target_qps_for_accelerator('L4', 2) == 2.0


def test_target_qps_cache_invalidated_by_mode_switch():
    policy = lb_policies.InstanceAwareLeastLoadPolicy()
    policy.set_target_qps_per_accelerator({'L4': 1.0})
    assert policy._get_target_qps_for_accelerator('L4', 4) == 4.0

    policy.set_default_per_gpu_target(2.5)
    with mock.patch.object(
            policy,
            '_resolve_target_qps_for_accelerator',
            wraps=policy._resolve_target_qps_for_accelerator) as resolve:
        assert policy._get_target_qps_for_accelerator('L4', 4) == 10.0
        assert policy._get_target_qps_for_accelerator('L4', 4) == 10.0

    resolve.assert_called_once_with('L4', 4)


def test_ready_replicas_repopulated_after_swap():
    # After a policy swap the new object is empty; a subsequent
    # set_ready_replicas (what the sync loop does) fully initializes it.
    lb = _make_lb()  # least_load
    lb._apply_routing_spec(
        {'load_balancing_policy_name': 'instance_aware_least_load'})
    urls = ['http://a:8080', 'http://b:8080']
    lb._load_balancing_policy.set_ready_replicas(urls)
    assert set(lb._load_balancing_policy.ready_replicas) == set(urls)
    # Load map initialized for each replica (not short-circuited).
    assert all(lb._load_balancing_policy.load_map[u] == 0 for u in urls)


class TestKeepReadySetOnEmptySync:
    """A 2xx sync with an empty url map must not blank a healthy ready set when
    the controller still reports READY replicas (they were transiently
    unresolvable) -- otherwise the LB 503s all live traffic on a controller
    restart / launch-storm blip. A genuine zero or an older controller (no
    count) still applies the empty map."""

    def _lb_with_ready(self, urls):
        lb = _make_lb()  # least_load
        lb._load_balancing_policy.set_ready_replicas(urls)
        return lb

    def test_keep_when_empty_map_but_controller_has_ready(self):
        lb = self._lb_with_ready(['http://a:8080'])
        # empty urls + num_ready > 0 + a set to protect -> keep it.
        assert lb._should_keep_ready_set_on_empty_sync([], 2) is True

    def test_blank_on_authoritative_zero(self):
        lb = self._lb_with_ready(['http://a:8080'])
        # Controller confirms zero READY replicas -> apply the empty map.
        assert lb._should_keep_ready_set_on_empty_sync([], 0) is False

    def test_blank_when_count_absent_old_controller(self):
        lb = self._lb_with_ready(['http://a:8080'])
        # Older controller omits the count (None) -> preserve prior behavior.
        assert lb._should_keep_ready_set_on_empty_sync([], None) is False

    def test_no_keep_when_current_set_already_empty(self):
        lb = self._lb_with_ready([])
        # Nothing to protect (e.g. first sync) -> don't special-case.
        assert lb._should_keep_ready_set_on_empty_sync([], 3) is False

    def test_no_keep_when_map_non_empty(self):
        lb = self._lb_with_ready(['http://a:8080'])
        # Normal sync with resolvable urls -> apply as usual.
        assert lb._should_keep_ready_set_on_empty_sync(['http://b:8080'],
                                                       1) is False


class _FakeResp:
    """Async-context-manager stub for aiohttp's response."""

    def __init__(self, body, on_enter=None) -> None:
        self._body = body
        self._on_enter = on_enter
        self.status = 200

    async def __aenter__(self):
        if self._on_enter is not None:
            self._on_enter()
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    def raise_for_status(self) -> None:
        pass

    async def json(self):
        return self._body


class _FakeSession:
    """Async-context-manager stub for aiohttp.ClientSession."""

    def __init__(self, resp: _FakeResp) -> None:
        self._resp = resp
        self.post_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    def post(self, *args, **kwargs) -> _FakeResp:
        self.post_calls.append((args, kwargs))
        return self._resp


def _run_one_sync(lb: load_balancer.SkyServeLoadBalancer,
                  body,
                  on_response_enter=None) -> None:
    """Drive a single _sync_with_controller_once with a mocked controller
    response carrying `body`."""
    session = _FakeSession(_FakeResp(body, on_enter=on_response_enter))
    with mock.patch.object(load_balancer.aiohttp,
                           'ClientSession',
                           return_value=session), \
         mock.patch.object(load_balancer.serve_utils,
                           'get_lb_sync_auth_tokens',
                           return_value=('sync-token',)), \
         mock.patch.object(lb,
                           '_get_lb_session_id',
                           return_value='test-pod-uid'):
        asyncio.run(lb._sync_with_controller_once())


def test_large_ready_set_is_not_emitted_to_info_logs():
    lb = _make_lb()
    urls = [f'http://replica-{index}:8080' for index in range(2_159)]
    body = {
        'replica_info': {
            url: {} for url in urls
        },
        'num_ready_replicas': len(urls),
        'routing_spec': {
            'load_balancing_policy_name': 'least_load',
            'stream_timeout_seconds': 90,
        },
    }

    with mock.patch.object(load_balancer.logger, 'info') as info:
        _run_one_sync(lb, body)

    info.assert_not_called()


def test_durable_demand_sync_posts_directly_and_acknowledges_history():
    lb = _make_lb()
    lb._service_hash = 'service-hash-a'
    lb._routing_version = 3
    lb._configured_accelerators = ('L4',)
    lb._request_accelerator_compatibility_version = 1
    lb._request_aggregator.add(
        types.SimpleNamespace(_skyserve_compatible_accelerators=['L4'],
                              _skyserve_request_priority=50))
    response = _FakeResp({
        'generation': 7,
        'request_history_accepted': True,
        'request_classification_history_accepted': True,
        'prediction_time_history_accepted': True,
    })
    session = _FakeSession(response)

    with mock.patch.object(load_balancer.aiohttp,
                           'ClientSession',
                           return_value=session), \
         mock.patch.object(load_balancer.serve_utils,
                           'get_lb_sync_auth_tokens',
                           return_value=('sync-token',)), \
         mock.patch.object(lb,
                           '_get_lb_session_id',
                           return_value='test-pod-uid'):
        asyncio.run(lb._sync_demand_feed_once())

    assert len(session.post_calls) == 1
    args, kwargs = session.post_calls[0]
    assert args == ('http://ctrl:8001/demand',)
    assert kwargs['headers'] == {
        'Authorization': 'Bearer sync-token',
        constants.SERVICE_HASH_HEADER: 'service-hash-a',
    }
    assert kwargs['json']['protocol_version'] == 1
    assert kwargs['json']['sequence'] == 1
    assert kwargs['json']['configured_accelerators'] == ['L4']
    assert kwargs['json']['demand_window']['buckets'][0]['request_count'] == 1
    assert kwargs['timeout'].total == constants.LB_DEMAND_REPORT_TIMEOUT_SECONDS
    assert lb._request_aggregator.request_history_snapshot() is None


def test_queue_demand_capability_negotiates_and_downgrades():
    lb = _make_lb()
    routing_spec = {
        'load_balancing_policy_name': 'least_load',
        'stream_timeout_seconds': 90,
    }
    _run_one_sync(
        lb, {
            'replica_info': {},
            'num_ready_replicas': 0,
            'routing_spec': routing_spec,
            'queued_compatibility_demand_supported': True,
        })
    assert lb._queued_compatibility_demand_supported is True

    # Missing means an older controller, including a rollback after a new
    # controller had already enabled the gauge-only path.
    _run_one_sync(lb, {
        'replica_info': {},
        'num_ready_replicas': 0,
        'routing_spec': routing_spec,
    })
    assert lb._queued_compatibility_demand_supported is False


def test_projected_route_fence_advances_only_after_full_route_apply():
    lb = _make_lb()
    digest = 'b' * 64
    _run_one_sync(
        lb, {
            'replica_info': {
                'http://replica:8080': {
                    'gpu_type': 'L4',
                    'gpu_count': '1',
                }
            },
            'num_ready_replicas': 1,
            'routing_spec': {
                'load_balancing_policy_name': 'least_load',
            },
            'service_version': 7,
            'route_projection_generation': 11,
            'route_projection_sha256': digest,
            'route_source_epoch': 3,
        })

    assert lb._routing_version == 7
    assert lb._route_projection_generation == 11
    assert lb._route_projection_sha256 == digest
    assert lb._route_source_epoch == 3
    report, _, _, _ = lb._build_demand_report()
    assert report['route_projection_generation'] == 11
    assert report['route_projection_sha256'] == digest
    assert report['route_source_epoch'] == 3

    # A complete but temporarily unresolvable next generation retains both
    # the already-applied routes and the exact acknowledgement fence.
    _run_one_sync(
        lb, {
            'replica_info': {},
            'num_ready_replicas': 1,
            'routing_spec': {
                'load_balancing_policy_name': 'least_load',
            },
            'service_version': 8,
            'route_projection_generation': 12,
            'route_projection_sha256': 'c' * 64,
            'route_source_epoch': 3,
        })
    assert lb._routing_version == 7
    assert lb._route_projection_generation == 11
    assert lb._route_projection_sha256 == digest


def test_partial_projected_route_fence_is_rejected_without_advancing():
    lb = _make_lb()

    with pytest.raises(ValueError, match='fence is malformed'):
        _run_one_sync(
            lb, {
                'replica_info': {},
                'num_ready_replicas': 0,
                'routing_spec': {
                    'load_balancing_policy_name': 'least_load',
                },
                'service_version': 7,
                'route_projection_generation': 1,
            })

    assert lb._routing_version is None
    assert lb._route_projection_generation is None


def test_request_routing_does_not_emit_per_attempt_logs():
    policy = lb_policies.RoundRobinPolicy()
    policy.set_ready_replicas(['http://replica:8080'])
    request = mock.Mock()

    with mock.patch.object(lb_policies, 'logger') as logger:
        selected = [policy.select_replica(request) for _ in range(2_159)]

    assert selected == ['http://replica:8080'] * 2_159
    assert logger.mock_calls == []


class TestSyncOnceEmptyMapWiring:
    """Integration coverage for the empty-sync guard inside
    _sync_with_controller_once: the pure predicate is proven elsewhere; these
    assert the method actually preserves vs blanks the ready set end to end."""

    def test_empty_map_with_ready_count_preserves_set(self):
        lb = _make_lb()
        urls = ['http://a:8080', 'http://b:8080']
        lb._apply_routing_spec({
            'load_balancing_policy_name': 'instance_aware_least_load',
            'request_accelerator_compatibility_version': 1,
            'configured_accelerators': ['A100'],
            'stream_timeout_seconds': 90,
        })
        lb._routing_version = 1
        lb._load_balancing_policy.set_ready_replicas(urls)
        lb._ready = True
        _run_one_sync(
            lb, {
                'replica_info': {},
                'num_ready_replicas': 2,
                'routing_spec': {
                    'load_balancing_policy_name': 'instance_aware_least_load',
                    'request_accelerator_compatibility_version': 1,
                    'configured_accelerators': ['H100'],
                    'stream_timeout_seconds': 90,
                },
                'service_version': 2,
            })
        # Spurious empty sync: the healthy set survives...
        assert set(lb._load_balancing_policy.ready_replicas) == set(urls)
        # ...and the LB still marks itself synced.
        assert lb._ready is True
        # The response was not applied as a coherent route/catalog snapshot,
        # so its version cannot be echoed on the next demand report.
        assert lb._routing_version == 1
        assert lb._configured_accelerators == ('A100',)

    def test_empty_map_authoritative_zero_blanks_set(self):
        lb = _make_lb()
        lb._load_balancing_policy.set_ready_replicas(['http://a:8080'])
        _run_one_sync(
            lb, {
                'replica_info': {},
                'num_ready_replicas': 0,
                'routing_spec': {
                    'load_balancing_policy_name': 'least_load',
                    'stream_timeout_seconds': 90,
                },
            })
        # Genuine zero -> the set is blanked (prior behavior).
        assert not lb._load_balancing_policy.ready_replicas
        assert lb._ready is True


class TestMissingRoutingSpecReadiness:
    """A route snapshot is publishable only with its matching routing spec."""

    def test_cold_lb_stays_unready_and_acknowledges_batch(self):
        lb = _make_lb()
        lb._request_aggregator.timestamps.extend([1, 2, 3])

        _run_one_sync(lb, {
            'replica_info': {
                'http://a:8080': {}
            },
            'num_ready_replicas': 1,
            'routing_spec': None,
            'capacity_hint': {
                'provisioning_replicas': 1,
                'target_num_replicas': 2,
            },
        },
                      on_response_enter=lambda: lb._request_aggregator.
                      timestamps.append(4))

        assert lb._ready is False
        assert lb._last_sync_time is None
        assert lb._capacity_hint is None
        assert not lb._load_balancing_policy.ready_replicas
        # The controller returned 2xx and has already ingested [1, 2, 3]. An
        # incomplete response must not replay that batch or clear the concurrent
        # arrival recorded while the response was in flight.
        assert lb._request_aggregator.to_dict()['timestamps'] == [4]

    def test_next_complete_spec_makes_cold_lb_ready(self):
        lb = _make_lb()
        incomplete = {
            'replica_info': {
                'http://a:8080': {}
            },
            'num_ready_replicas': 1,
            'routing_spec': None,
        }
        _run_one_sync(lb, incomplete)

        _run_one_sync(
            lb, {
                'replica_info': {
                    'http://a:8080': {}
                },
                'num_ready_replicas': 1,
                'routing_spec': {
                    'load_balancing_policy_name': 'round_robin',
                    'stream_timeout_seconds': 90,
                },
            })

        assert lb._ready is True
        assert lb._load_balancing_policy_name == 'round_robin'
        assert lb._stream_timeout_seconds == 90
        assert lb._load_balancing_policy.ready_replicas == ['http://a:8080']

    def test_warm_lb_keeps_last_valid_spec_routes_and_readiness(self):
        lb = _make_lb()
        _run_one_sync(
            lb, {
                'replica_info': {
                    'http://old:8080': {}
                },
                'num_ready_replicas': 1,
                'routing_spec': {
                    'load_balancing_policy_name': 'round_robin',
                    'stream_timeout_seconds': 90,
                },
                'capacity_hint': {
                    'provisioning_replicas': 0,
                    'target_num_replicas': 1,
                },
            })
        last_complete_sync = lb._last_sync_time
        last_complete_capacity = lb._capacity_hint

        _run_one_sync(
            lb, {
                'replica_info': {
                    'http://new:8080': {}
                },
                'num_ready_replicas': 1,
                'routing_spec': None,
                'capacity_hint': {
                    'provisioning_replicas': 2,
                    'target_num_replicas': 3,
                },
            })

        assert lb._ready is True
        assert lb._last_sync_time == last_complete_sync
        assert lb._capacity_hint == last_complete_capacity
        assert lb._load_balancing_policy_name == 'round_robin'
        assert lb._stream_timeout_seconds == 90
        assert lb._load_balancing_policy.ready_replicas == ['http://old:8080']
