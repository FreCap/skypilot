"""Scale and observability regressions for SkyServe external LB HA."""

import asyncio
import json
from unittest import mock

from load_balancer_test_utils import publish_current_occupancy_snapshot

from sky.serve import constants
from sky.serve import lb_ha_observability as lb_ha_obs
from sky.serve import load_balancer


def _role_payload(replica_count: int) -> dict:
    lb = load_balancer.SkyServeLoadBalancer('http://controller',
                                            8890,
                                            service_hash='incarnation',
                                            lb_slot='a')
    urls = [
        f'http://10.{index // 65536}.{(index // 256) % 256}.{index % 256}:8080'
        for index in range(replica_count)
    ]
    with lb._client_pool_lock:  # pylint: disable=protected-access
        lb._load_balancing_policy.set_ready_replicas(  # pylint: disable=protected-access
            urls)
        publish_current_occupancy_snapshot(
            lb,
            occupancy={url: 0 for url in urls},
            total_slots={url: 1 for url in urls},
            free_slots={url: 1 for url in urls},
            dispatch_generation_by_url={url: 1 for url in urls})
    return lb._ha_role_payload()  # pylint: disable=protected-access


def test_full_role_and_routing_payload_sizes_cover_fleet_scale(monkeypatch):
    monkeypatch.setenv(constants.LB_POD_UID_ENV_VAR, 'payload-sizing-pod')
    role_sizes = []
    routing_sizes = []
    for replica_count in (100, 500, 1000):
        role_payload = _role_payload(replica_count)
        assert len(role_payload['routing_urls']) == replica_count
        assert len(role_payload['async_occupancy']) == replica_count
        assert 'request_body' not in role_payload
        assert 'request_queue' not in role_payload
        role_sizes.append(len(json.dumps(role_payload).encode('utf-8')))

        routing_payload = {
            'replica_info': {
                url: {
                    'version': 1,
                    'gpu_type': 'L4',
                    'gpu_count': '1',
                    'async_occupancy': 'true',
                } for url in role_payload['routing_urls']
            },
            'routing_spec': {
                'load_balancing_policy_name': 'instance_aware_least_load',
                'target_qps_per_replica': {
                    'L4': 0.1,
                },
                'retriable_status_codes': [429, 503],
            },
        }
        routing_sizes.append(len(json.dumps(routing_payload).encode('utf-8')))

    assert role_sizes == sorted(role_sizes)
    assert routing_sizes == sorted(routing_sizes)
    # Keep the deterministic 1,000-URL envelopes bounded well below the
    # controller proxy's request limits. This is a regression guard, not an
    # optimization trigger; representative runtime latency remains the gate.
    assert role_sizes[-1] < 1024 * 1024
    assert routing_sizes[-1] < 1024 * 1024


def test_role_payload_excludes_retained_sample_after_probe_miss(monkeypatch):
    monkeypatch.setenv(constants.LB_POD_UID_ENV_VAR, 'retained-sample-pod')
    lb = load_balancer.SkyServeLoadBalancer('http://controller',
                                            8890,
                                            service_hash='incarnation',
                                            lb_slot='a')
    url = 'http://worker:8080'
    lb._load_balancing_policy.set_ready_replicas([url])  # pylint: disable=protected-access
    results = [(0, 4, 4), None]

    async def _fetch(session, selected_url):
        del session
        assert selected_url == url
        return results.pop(0)

    lb._fetch_replica_occupancy = _fetch  # pylint: disable=protected-access
    asyncio.run(lb._probe_replica_occupancy_once())  # pylint: disable=protected-access
    asyncio.run(lb._probe_replica_occupancy_once())  # pylint: disable=protected-access

    with lb._client_pool_lock:  # pylint: disable=protected-access
        assert lb._effective_replica_free_slots_locked() == {url: 4}  # pylint: disable=protected-access
    payload = lb._ha_role_payload()  # pylint: disable=protected-access
    assert payload['async_occupancy'] == {}
    assert payload['occupancy_sample_generation'] == {}
    assert payload['occupancy_sample_age_seconds'] == {}
    assert payload['unknown_in_flight_urls'] == [url]


def test_active_role_transition_invalidates_then_immediately_reprobes(
        monkeypatch):
    monkeypatch.setenv(constants.LB_POD_UID_ENV_VAR, 'promotion-pod')
    lb = load_balancer.SkyServeLoadBalancer('http://controller',
                                            8890,
                                            service_hash='incarnation',
                                            lb_slot='a')
    url = 'http://worker:8080'
    lb._load_balancing_policy.set_ready_replicas([url])  # pylint: disable=protected-access
    publish_current_occupancy_snapshot(lb,
                                       occupancy={url: 0},
                                       total_slots={url: 4},
                                       free_slots={url: 4})

    class _RoleResponse:
        """Minimal successful controller role response."""

        status = 200
        headers = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            del exc
            return False

        async def json(self):
            return {
                'role': 'ACTIVE',
                'generation': 2,
                'outcome': 'success',
            }

    class _RoleSession:
        """Minimal aiohttp session returning the role response."""

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            del exc
            return False

        def post(self, *args, **kwargs):
            del args, kwargs
            return _RoleResponse()

    async def _assert_invalidated_then_probe():
        assert lb._replica_occupancy == {}  # pylint: disable=protected-access
        assert lb._replica_total_slots == {}  # pylint: disable=protected-access
        assert lb._replica_free_slots == {}  # pylint: disable=protected-access
        assert lb._occupancy_sample_time == {}  # pylint: disable=protected-access
        assert lb._lb_role.value == 'ACTIVE'  # pylint: disable=protected-access
        assert lb._lb_role_generation == 2  # pylint: disable=protected-access
        assert lb._occupancy_role_epoch == 1  # pylint: disable=protected-access

    reprobe = mock.AsyncMock(side_effect=_assert_invalidated_then_probe)
    monkeypatch.setattr(load_balancer.aiohttp, 'ClientSession', _RoleSession)
    monkeypatch.setattr(load_balancer.serve_utils, 'get_lb_sync_auth_tokens',
                        lambda required: ('token',))
    monkeypatch.setattr(lb, '_probe_replica_occupancy_once', reprobe)

    asyncio.run(lb._sync_role_with_controller_once())  # pylint: disable=protected-access

    reprobe.assert_awaited_once_with()


def test_runtime_stats_are_bounded_and_classify_unknown_outcomes():
    stats = lb_ha_obs.LbHaRuntimeStats()
    stats.record_role(payload_bytes=100,
                      total_seconds=0.2,
                      outcome=lb_ha_obs.LbRoleOutcome.SUCCESS.value,
                      status_code=200,
                      controller_observation={
                          'controller': {
                              'phases_seconds': {
                                  'kubernetes_pod_authority': 0.01,
                              }
                          }
                      })
    stats.record_role(payload_bytes=300,
                      total_seconds=0.4,
                      outcome='unbounded-service-specific-error',
                      status_code=503,
                      controller_observation=None)
    stats.record_probe(total_seconds=1.5,
                       attempted=500,
                       succeeded=490,
                       connections_created=480)

    snapshot = stats.snapshot()
    assert snapshot['role']['payload_bytes'] == {
        'count': 2,
        'last': 300.0,
        'mean': 200.0,
        'max': 300.0,
        'p50_recent': 100.0,
        'p99_recent': 300.0,
    }
    assert snapshot['role']['outcomes'] == {
        'invalid_response': 1,
        'success': 1,
    }
    assert sum(snapshot['role']['total_seconds']['histogram']['counts']) == 2
    latency_bounds = snapshot['role']['total_seconds']['histogram'][
        'upper_bounds']
    assert len(latency_bounds) > 100
    assert all(current / previous <= 1.101
               for previous, current in zip(latency_bounds, latency_bounds[1:]))
    kubernetes_histogram = snapshot['role']['controller']['phases_seconds'][
        'kubernetes_pod_authority']['histogram']
    assert sum(kubernetes_histogram['counts']) == 1
    assert snapshot['role']['last_outcome'] == 'invalid_response'
    assert snapshot['role']['failure_streak_active']
    assert snapshot['probe']['last'] == {
        'total_seconds': 1.5,
        'attempted': 500,
        'succeeded': 490,
        'unknown': 10,
        'connections_created': 480,
    }
    serialized = json.dumps(snapshot)
    assert 'service' not in serialized
    assert 'replica_url' not in serialized


def test_role_failure_recovery_retains_worst_streak(monkeypatch):
    monotonic_times = iter((0, 20, 30, 31, 32))
    monkeypatch.setattr(lb_ha_obs.time, 'monotonic',
                        lambda: next(monotonic_times))
    stats = lb_ha_obs.LbHaRuntimeStats()
    stats.record_role(
        payload_bytes=100,
        total_seconds=0.1,
        outcome=lb_ha_obs.LbRoleOutcome.ROUTING_NOT_CONVERGED.value,
        status_code=503,
        controller_observation=None)
    stats.record_role(payload_bytes=100,
                      total_seconds=0.1,
                      outcome=lb_ha_obs.LbRoleOutcome.SUCCESS.value,
                      status_code=200,
                      controller_observation=None)
    stats.record_role(
        payload_bytes=100,
        total_seconds=0.1,
        outcome=lb_ha_obs.LbRoleOutcome.ROUTING_NOT_CONVERGED.value,
        status_code=503,
        controller_observation=None)
    stats.record_role(payload_bytes=100,
                      total_seconds=0.1,
                      outcome=lb_ha_obs.LbRoleOutcome.SUCCESS.value,
                      status_code=200,
                      controller_observation=None)

    role = stats.snapshot()['role']
    assert role['last_outcome'] == 'success'
    assert not role['failure_streak_active']
    assert role['last_failure_recovery_seconds'] == 1
    assert role['max_failure_recovery_seconds'] == 20


def test_empty_probe_round_is_observable_without_creating_connections():
    lb = load_balancer.SkyServeLoadBalancer('http://controller', 8890)
    asyncio.run(lb._probe_replica_occupancy_once())  # pylint: disable=protected-access

    probe = lb._ha_stats().snapshot()['probe']  # pylint: disable=protected-access
    assert probe['rounds'] == 1
    assert probe['attempted'] == 0
    assert probe['connections_created'] == 0
