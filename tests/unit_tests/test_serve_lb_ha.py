"""Deterministic tests for SkyServe external load balancer HA."""
# pylint: disable=protected-access,unexpected-keyword-arg
import asyncio
import json
import os
import threading
from unittest import mock

import pytest

from sky.serve import constants as serve_constants
from sky.serve import controller
from sky.serve import lb_ha
from sky.serve import lb_k8s


def _state(phase: lb_ha.LbCutoverPhase,
           active: lb_ha.LbSlot = lb_ha.LbSlot.A,
           pending: lb_ha.LbSlot | None = None,
           generation: int = 1) -> lb_ha.LbCutoverState:
    return lb_ha.LbCutoverState(enabled=True,
                                active_slot=active,
                                generation=generation,
                                pending_slot=pending,
                                phase=phase,
                                lifecycle_epoch=7)


def _report(http: dict[str, int], asynchronous: dict[str, int],
            generations: dict[str, int], ages: dict[str,
                                                    float], **override) -> dict:
    report = {
        'local_in_flight': sum(http.values()),
        'http_in_flight': http,
        'async_occupancy': asynchronous,
        'occupancy_sample_generation': generations,
        'occupancy_sample_age_seconds': ages,
        'routing_urls': sorted(set(http) | set(asynchronous)),
        'unknown_in_flight_urls': [],
        'draining_urls': [],
    }
    report.update(override)
    return report


def test_cutover_roles_cover_stable_preparing_and_draining():
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    assert stable.role_for(lb_ha.LbSlot.A) is lb_ha.LbRole.ACTIVE
    assert stable.role_for(lb_ha.LbSlot.B) is lb_ha.LbRole.STANDBY

    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    assert preparing.role_for(lb_ha.LbSlot.A) is lb_ha.LbRole.ACTIVE
    assert preparing.role_for(lb_ha.LbSlot.B) is lb_ha.LbRole.ARMED

    draining = _state(lb_ha.LbCutoverPhase.DRAINING,
                      active=lb_ha.LbSlot.B,
                      pending=lb_ha.LbSlot.A,
                      generation=2)
    assert draining.role_for(lb_ha.LbSlot.A) is lb_ha.LbRole.DRAINING
    assert draining.role_for(lb_ha.LbSlot.B) is lb_ha.LbRole.ACTIVE


def test_promotion_requires_every_async_sample_to_be_fresh_and_valid():
    required = {'http://replica-a', 'http://replica-b'}
    generations = {'http://replica-a': 3, 'http://replica-b': 7}
    ages = {'http://replica-a': 1.0, 'http://replica-b': 14.9}
    assert lb_ha.occupancy_samples_are_promotable(required, generations, ages,
                                                  15)
    assert not lb_ha.occupancy_samples_are_promotable(
        required, generations, {'http://replica-a': 1.0}, 15)
    assert not lb_ha.occupancy_samples_are_promotable(required, generations, {
        'http://replica-a': 1.0,
        'http://replica-b': 15.1
    }, 15)
    assert lb_ha.occupancy_samples_are_promotable(set(), {}, {}, 15)


def test_controller_promotion_refuses_unknown_empty_occupancy_contract():
    ctrl = _role_controller()
    ctrl._lb_occupancy_contract_known = False

    assert not ctrl._lb_promotion_report_is_current(
        _role_request('standby', lb_ha.LbSlot.B))

    ctrl._lb_occupancy_contract_known = True
    assert ctrl._lb_promotion_report_is_current(
        _role_request('standby', lb_ha.LbSlot.B))


def test_session_ledger_sums_http_but_uses_freshest_async_observation():
    ledger = lb_ha.LbSessionLedger(max_session_age_seconds=10,
                                   max_occupancy_age_seconds=5)
    assert ledger.update('active',
                         lb_ha.LbSlot.A,
                         lb_ha.LbRole.ACTIVE,
                         4,
                         _report({'replica': 2}, {'replica': 3}, {'replica': 8},
                                 {'replica': 2.0}),
                         now=100)
    assert ledger.update('draining',
                         lb_ha.LbSlot.B,
                         lb_ha.LbRole.DRAINING,
                         4,
                         _report({'replica': 1}, {'replica': 4}, {'replica': 9},
                                 {'replica': 1.0}),
                         now=101)

    aggregate = ledger.aggregate({'active', 'draining'}, now=102)
    assert aggregate.complete
    # Three distinct HTTP envelopes plus one replica-global async sample.
    assert aggregate.in_flight == {'replica': 7}
    assert aggregate.occupancy_sampled_urls == ['replica']


def test_session_ledger_uses_conservative_max_for_equal_async_samples():
    ledger = lb_ha.LbSessionLedger(max_session_age_seconds=10,
                                   max_occupancy_age_seconds=5)
    assert ledger.update('a',
                         lb_ha.LbSlot.A,
                         lb_ha.LbRole.ACTIVE,
                         2,
                         _report({}, {'replica': 3}, {'replica': 6},
                                 {'replica': 1.0}),
                         now=100)
    assert ledger.update('b',
                         lb_ha.LbSlot.B,
                         lb_ha.LbRole.DRAINING,
                         2,
                         _report({}, {'replica': 5}, {'replica': 6},
                                 {'replica': 2.0}),
                         now=101)

    aggregate = ledger.aggregate({'a', 'b'}, now=102)
    assert aggregate.in_flight == {'replica': 5}


def test_session_ledger_fails_closed_for_missing_or_stale_evidence():
    ledger = lb_ha.LbSessionLedger(max_session_age_seconds=10,
                                   max_occupancy_age_seconds=5)
    missing = ledger.aggregate({'unknown'}, now=50)
    assert not missing.complete
    assert missing.routing_urls is None

    assert not ledger.update('invalid',
                             lb_ha.LbSlot.A,
                             lb_ha.LbRole.ACTIVE,
                             1,
                             _report({}, {'replica': 0}, {}, {'replica': 1.0}),
                             now=50)
    assert ledger.update('stale',
                         lb_ha.LbSlot.A,
                         lb_ha.LbRole.ACTIVE,
                         1,
                         _report({}, {'replica': 0}, {'replica': 1},
                                 {'replica': 6.0}),
                         now=50)
    stale = ledger.aggregate({'stale'}, now=51)
    assert stale.complete
    assert not stale.in_flight
    assert stale.unknown_urls == ['replica']


def test_session_ledger_requires_applied_drain_role_and_generation():
    ledger = lb_ha.LbSessionLedger(max_session_age_seconds=10,
                                   max_occupancy_age_seconds=5)
    assert ledger.update('former-active',
                         lb_ha.LbSlot.A,
                         lb_ha.LbRole.DRAINING,
                         3,
                         _report({}, {}, {}, {},
                                 applied_role='ACTIVE',
                                 applied_generation=2),
                         now=50)

    stale_role = ledger.aggregate({'former-active'},
                                  now=51,
                                  required_applied_role=lb_ha.LbRole.DRAINING,
                                  required_applied_generation=3)
    assert not stale_role.complete

    assert ledger.update('former-active',
                         lb_ha.LbSlot.A,
                         lb_ha.LbRole.DRAINING,
                         3,
                         _report({}, {}, {}, {},
                                 applied_role='DRAINING',
                                 applied_generation=3),
                         now=52)
    acknowledged = ledger.aggregate({'former-active'},
                                    now=53,
                                    required_applied_role=lb_ha.LbRole.DRAINING,
                                    required_applied_generation=3)
    assert acknowledged.complete


def test_role_timeout_headroom_does_not_weaken_report_freshness():
    assert serve_constants.LB_ROLE_HEARTBEAT_TIMEOUT_SECONDS == 8
    assert serve_constants.LB_ROLE_REPORT_MAX_AGE_SECONDS == 6
    ledger = lb_ha.LbSessionLedger(
        max_session_age_seconds=serve_constants.LB_ROLE_REPORT_MAX_AGE_SECONDS,
        max_occupancy_age_seconds=10)
    assert ledger.update('active',
                         lb_ha.LbSlot.A,
                         lb_ha.LbRole.ACTIVE,
                         1,
                         _report({}, {}, {}, {}),
                         now=100)

    assert ledger.aggregate({'active'}, now=106).complete
    assert not ledger.aggregate({'active'}, now=106.001).complete


def test_demand_handoff_holds_then_expires_previous_active_floor():
    handoff = lb_ha.DemandHandoff(5)
    handoff.begin(
        8,
        lb_ha.DemandSnapshot((10, 20),
                             7,
                             3,
                             rejected_in_recent_window=2,
                             queue_depth_by_priority={
                                 '0': 5,
                                 '50': 2,
                             },
                             rejected_in_window_by_priority={'50': 3},
                             unique_job_arrivals_60s=9,
                             unique_job_arrivals_300s=20,
                             offered_arrival_tracking_saturated=True))
    cold = {
        'request_aggregator': {
            'timestamps': [20, 30],
        },
        'queue_depth': 1,
        'rejected_in_window': 0,
        'rejected_in_recent_window': 0,
        'queue_depth_by_priority': {
            '0': 3,
            '80': 4,
        },
        'rejected_in_window_by_priority': {
            '50': 1
        },
        'unique_job_arrivals_60s': 10,
        'unique_job_arrivals_300s': 12,
    }
    floored = handoff.apply(8, cold, complete_authoritative_report=True, now=1)
    assert floored['request_aggregator']['timestamps'] == [10, 20, 30]
    assert floored['queue_depth'] == 7
    assert floored['rejected_in_window'] == 3
    assert floored['rejected_in_recent_window'] == 2
    assert floored['queue_depth_by_priority'] == {
        '0': 5,
        '50': 2,
        '80': 4,
    }
    assert floored['rejected_in_window_by_priority'] == {'50': 3}
    assert floored['unique_job_arrivals_60s'] == 10
    assert floored['unique_job_arrivals_300s'] == 20
    assert floored['offered_arrival_tracking_saturated'] is True
    assert floored['pressure_report_is_floored'] is True
    assert handoff.apply(8, cold, True, now=5.9)['queue_depth'] == 7
    assert handoff.apply(8, cold, True, now=6) == cold


def test_demand_handoff_unions_old_and_new_in_flight_evidence():
    handoff = lb_ha.DemandHandoff(5)
    handoff.begin(
        8,
        lb_ha.DemandSnapshot((),
                             0,
                             0,
                             in_flight={'old': 2},
                             unknown_in_flight_urls=('unknown-old',)))

    floored = handoff.apply(
        8, {
            'in_flight': {
                'old': 1,
                'new': 3,
            },
            'unknown_in_flight_urls': ['unknown-new'],
        }, False)

    assert floored['in_flight'] == {'old': 2, 'new': 3}
    assert floored['unknown_in_flight_urls'] == ['unknown-new', 'unknown-old']


def test_demand_handoff_preserves_compatibility_arrivals_and_queue_floors():
    old_request = {
        'routing_version': 1,
        'request_aggregator': {
            'timestamps': [10],
            'compatibility_profiles': [{
                'timestamp': 10,
                'priority': 50,
                'compatible_accelerators': ['A100'],
                'count': 2,
            }],
        },
        'queued_requests_by_compatibility': [{
            'priority': 50,
            'compatible_accelerators': ['A100'],
            'count': 3,
        }],
        'rejected_requests_by_compatibility': [{
            'priority': 50,
            'compatible_accelerators': ['A100'],
            'count': 5,
            'recent_count': 2,
        }],
    }
    current_request = {
        'routing_version': 1,
        'request_aggregator': {
            'timestamps': [20],
            'compatibility_profiles': [{
                'timestamp': 20,
                'priority': 20,
                'compatible_accelerators': ['L4', 'A100'],
                'count': 1,
            }],
        },
        'queued_requests_by_compatibility': [{
            'priority': 50,
            'compatible_accelerators': ['A100'],
            'count': 1,
        }, {
            'priority': 20,
            'compatible_accelerators': ['L4', 'A100'],
            'count': 4,
        }],
        'rejected_requests_by_compatibility': [{
            'priority': 50,
            'compatible_accelerators': ['A100'],
            'count': 3,
            'recent_count': 3,
        }],
    }
    snapshot = lb_ha.DemandSnapshot.from_request(old_request)
    assert lb_ha.DemandSnapshot.from_dict(snapshot.to_dict()) == snapshot

    handoff = lb_ha.DemandHandoff(60)
    handoff.begin(8, snapshot)
    floored = handoff.apply(8, current_request, True, now=1)

    assert floored['request_aggregator']['timestamps'] == [10, 20]
    assert floored['request_aggregator']['compatibility_profiles'] == [
        old_request['request_aggregator']['compatibility_profiles'][0],
        current_request['request_aggregator']['compatibility_profiles'][0],
    ]
    assert floored['queued_requests_by_compatibility'] == [{
        'priority': 50,
        'compatible_accelerators': ['A100'],
        'count': 3,
    }, {
        'priority': 20,
        'compatible_accelerators': ['L4', 'A100'],
        'count': 4,
    }]
    assert floored['rejected_requests_by_compatibility'] == [{
        'priority': 50,
        'compatible_accelerators': ['A100'],
        'count': 5,
        'recent_count': 3,
    }]

    next_request = {
        **current_request,
        'request_aggregator': {
            'timestamps': [],
            'compatibility_profiles': [],
        },
    }
    repeated = handoff.apply(8, next_request, True, now=21)
    assert repeated['request_aggregator'] == next_request['request_aggregator']
    assert repeated['queued_requests_by_compatibility'] == floored[
        'queued_requests_by_compatibility']
    assert repeated['rejected_requests_by_compatibility'] == floored[
        'rejected_requests_by_compatibility']


def test_demand_handoff_drops_exact_profiles_from_an_old_routing_version():
    snapshot = lb_ha.DemandSnapshot.from_request({
        'routing_version': 1,
        'request_aggregator': {
            'timestamps': [10],
            'compatibility_profiles': [{
                'timestamp': 10,
                'priority': 50,
                'compatible_accelerators': ['A100'],
                'count': 2,
            }],
        },
        'queued_requests_by_compatibility': [{
            'priority': 50,
            'compatible_accelerators': ['A100'],
            'count': 3,
        }],
        'rejected_requests_by_compatibility': [{
            'priority': 50,
            'compatible_accelerators': ['A100'],
            'count': 4,
        }],
    })
    current = {
        'routing_version': 2,
        'request_aggregator': {
            'timestamps': [20],
            'compatibility_profiles': [{
                'timestamp': 20,
                'priority': 20,
                'compatible_accelerators': ['H100'],
                'count': 1,
            }],
        },
        'queued_requests_by_compatibility': [{
            'priority': 20,
            'compatible_accelerators': ['H100'],
            'count': 1,
        }],
        'rejected_requests_by_compatibility': [],
    }

    floored = snapshot.floor(current)

    # Aggregate safety evidence still crosses the handoff, but card-specific
    # evidence from the old catalog does not.
    assert floored['request_aggregator']['timestamps'] == [10, 20]
    assert floored['request_aggregator']['compatibility_profiles'] == [
        current['request_aggregator']['compatibility_profiles'][0]
    ]
    assert floored['queued_requests_by_compatibility'] == (
        current['queued_requests_by_compatibility'])
    assert not floored['rejected_requests_by_compatibility']
    assert floored['routing_version'] == 2


def test_complete_demand_report_does_not_require_all_occupancy_samples():
    report = {
        'in_flight': {
            'http://replica': 1,
        },
        'queue_depth': 0,
        'rejected_in_window': 3,
        'rejected_in_recent_window': 1,
        'unknown_in_flight_urls': ['http://unsampled'],
        'queued_requests_by_compatibility': [],
        'rejected_requests_by_compatibility': [],
        'occupancy_sampled_urls': ['http://replica'],
    }

    assert controller.SkyServeController._lb_demand_report_is_complete(report)


def test_empty_complete_report_starts_window_but_preserves_floor():
    # A freshly promoted active LB may send a well-formed but empty report
    # (no in-flight, nothing sampled yet). It counts as complete and starts
    # the expiry window, but the handoff demand floor must survive until the
    # window elapses instead of being dropped immediately.
    empty_report = {
        'in_flight': {},
        'queue_depth': 0,
        'rejected_in_window': 0,
        'rejected_in_recent_window': 0,
        'unknown_in_flight_urls': [],
        'occupancy_sampled_urls': [],
        'queued_requests_by_compatibility': [],
        'rejected_requests_by_compatibility': [],
    }
    assert controller.SkyServeController._lb_demand_report_is_complete(
        empty_report)

    handoff = lb_ha.DemandHandoff(60)
    handoff.begin(
        3,
        lb_ha.DemandSnapshot((100,),
                             4,
                             2,
                             rejected_in_recent_window=1,
                             in_flight={'http://replica': 5},
                             unknown_in_flight_urls=('http://unprobed',)))

    floored = handoff.apply(3,
                            dict(empty_report),
                            complete_authoritative_report=True,
                            now=1000)
    # Floor preserved within the window: old demand evidence still visible.
    assert floored['in_flight'] == {'http://replica': 5}
    assert floored['unknown_in_flight_urls'] == ['http://unprobed']
    assert floored['queue_depth'] == 4
    assert floored['rejected_in_window'] == 2
    assert floored['rejected_in_recent_window'] == 1

    # Still floored just before expiry.
    late = handoff.apply(3, dict(empty_report), True, now=1059.9)
    assert late['in_flight'] == {'http://replica': 5}

    # Only after the full window does the empty report take effect.
    expired = handoff.apply(3, dict(empty_report), True, now=1060)
    assert expired == empty_report


def test_incomplete_demand_report_preserves_handoff_floor():
    complete = {
        'in_flight': {},
        'queue_depth': 0,
        'rejected_in_window': 0,
        'rejected_in_recent_window': 0,
        'unknown_in_flight_urls': [],
        'queued_requests_by_compatibility': [],
        'rejected_requests_by_compatibility': [],
    }
    for field in complete:
        report = dict(complete)
        report[field] = None
        assert not controller.SkyServeController._lb_demand_report_is_complete(
            report)

    malformed_queue_report = dict(complete)
    malformed_queue_report['queued_requests_by_compatibility'] = [{
        'priority': 50,
        'compatible_accelerators': ['A100'],
        'count': -1,
    }]
    assert not controller.SkyServeController._lb_demand_report_is_complete(
        malformed_queue_report)


def test_demand_snapshot_preserves_real_load_balancer_timestamps():
    snapshot = lb_ha.DemandSnapshot.from_request({
        'request_aggregator': {
            'timestamps': [10.25, 20.75],
        },
    })

    assert snapshot.timestamps == (10.25, 20.75)


def test_ha_kubernetes_contract_has_single_slot_selector_and_disruption_guard():
    service = lb_k8s._build_service_dict('service',
                                         'service-lb',
                                         'service-lb-a',
                                         service_hash='incarnation',
                                         active_slot=lb_ha.LbSlot.A,
                                         cutover_generation=3)
    assert service['spec']['type'] == 'LoadBalancer'
    assert service['spec']['externalTrafficPolicy'] == 'Cluster'
    assert service['spec']['selector'] == {
        lb_k8s.LB_SLOT_LABEL_KEY: 'a',
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
    }
    assert service['metadata']['annotations'][
        lb_k8s.CUTOVER_GENERATION_ANNOTATION_KEY] == '3'

    pdb = lb_k8s._build_pdb_dict('service', 'service-lb-pdb', 'incarnation',
                                 {'uid': 'api-deployment'})
    assert pdb['spec']['minAvailable'] == 1
    assert pdb['spec']['selector']['matchLabels'] == {
        lb_k8s.SERVE_LB_LABEL_KEY: 'service',
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
    }


def test_ha_slots_require_cross_host_placement_and_prefer_cross_zone():
    runtime = lb_k8s._lb_pod_runtime_fields({}, 'service', 'incarnation',
                                            lb_ha.LbSlot.B)
    anti_affinity = runtime['affinity']['podAntiAffinity']
    required = anti_affinity['requiredDuringSchedulingIgnoredDuringExecution']
    preferred = anti_affinity['preferredDuringSchedulingIgnoredDuringExecution']
    assert required[-1]['topologyKey'] == 'kubernetes.io/hostname'
    assert required[-1]['labelSelector']['matchLabels'] == {
        lb_k8s.SERVE_LB_LABEL_KEY: 'service',
        lb_k8s.SERVICE_HASH_LABEL_KEY: 'incarnation',
    }
    assert preferred[-1]['weight'] == 100
    assert preferred[-1]['podAffinityTerm'][
        'topologyKey'] == 'topology.kubernetes.io/zone'


def _role_owner_record(
    fence: tuple[str, tuple[int | None, str | None], int],
    state: lb_ha.LbCutoverState,
) -> dict[str, object]:
    return {
        'hash': fence[0],
        'controller_pid': fence[1][0],
        'controller_ip': fence[1][1],
        'lifecycle_epoch': fence[2],
        'resource_scope': None,
        'lb_ha_enabled': state.enabled,
        'lb_active_slot':
            (state.active_slot.value if state.active_slot is not None else None
            ),
        'lb_cutover_generation': state.generation,
        'lb_pending_slot': (
            state.pending_slot.value if state.pending_slot is not None else None
        ),
        'lb_cutover_phase': state.phase.value,
        'lb_drain_started_at': state.drain_started_at,
    }


def _role_controller() -> controller.SkyServeController:
    ctrl = controller.SkyServeController.__new__(controller.SkyServeController)
    ctrl._service_name = 'service'
    ctrl._resource_scope = None
    ctrl._lb_ha_enabled = True
    ctrl._lb_role_lock = None
    ctrl._lb_role_snapshot_task = None
    ctrl._lb_role_snapshot_key = None
    ctrl._lb_demand_lock = None
    ctrl._lb_session_ledger = lb_ha.LbSessionLedger(10, 10)
    ctrl._lb_expected_occupancy_urls = set()
    ctrl._lb_occupancy_contract_known = True
    ctrl._lb_last_demand_snapshot = None
    ctrl._lb_demand_handoff = lb_ha.DemandHandoff(5)
    ctrl._lb_drain_timeout_seconds = 60
    ctrl._applied_version = 1
    ctrl._routing_state_lock = threading.RLock()
    ctrl._owns_current_service = mock.Mock(return_value=True)
    ctrl._controller_owner = (123, '10.0.0.1')
    ctrl._controller_owner_fingerprint = 'controller-owner-fingerprint'
    ctrl._lb_cutover_fence = mock.Mock(return_value=('incarnation',
                                                     (123, '10.0.0.1'), 7))

    def database_snapshot():
        fence = ctrl._lb_cutover_fence()
        if fence is None:
            return None
        state = controller.serve_state.get_lb_cutover_state('service')
        if state is None:
            return None
        owner = _role_owner_record(fence, state)
        return controller._LbRoleDatabaseSnapshot(fence, state, owner)

    ctrl._lb_role_database_snapshot = mock.Mock(side_effect=database_snapshot)
    ctrl._replica_manager = mock.Mock()
    ctrl._publish_ha_drain_view = mock.Mock()
    ctrl._finish_ha_drain_if_safe = mock.Mock(return_value=False)
    return ctrl


@pytest.fixture(autouse=True)
def _adapt_existing_role_handler_mocks(monkeypatch):
    """Keep saga tests focused while the handler consumes one snapshot."""

    def read_snapshot(service_name,
                      unused_fence,
                      state,
                      unused_owner,
                      unused_timings=None):
        authority = lb_k8s.get_lb_pod_authority(service_name)
        if authority is None:
            return None
        try:
            if state.phase in (lb_ha.LbCutoverPhase.MIGRATING,
                               lb_ha.LbCutoverPhase.ROLLING_BACK):
                routing = lb_k8s.get_lb_service_transition_routing(service_name)
            else:
                routing = lb_k8s.get_lb_service_routing(service_name)
        except Exception as e:  # pylint: disable=broad-except
            raise lb_k8s.LbRoleSnapshotRoutingError(str(e)) from e
        return lb_k8s.LbRoleSnapshot(authority, routing)

    monkeypatch.setattr(controller.lb_k8s, 'get_lb_role_snapshot',
                        read_snapshot)


def _configure_sync_controller(ctrl: controller.SkyServeController,
                               runtime_tail,
                               prepare_head=None) -> None:
    ctrl._lb_sync_lock = None
    ctrl._lb_replica_cache = {}
    ctrl._lb_translation_cache = {}
    ctrl._routing_spec = None
    ctrl._reserved_capacity_fill_enabled = False
    ctrl._snapshot_replica_occupancy = mock.Mock(return_value=([], {}, None))
    ctrl._get_lb_replica_info = mock.Mock(return_value=({}, 0))
    ctrl._get_replica_counts = mock.Mock(return_value={})
    ctrl._get_capacity_hint = mock.Mock(return_value={})
    ctrl._persist_request_history = mock.AsyncMock(return_value=True)
    ctrl._persist_response_time_history = mock.AsyncMock(return_value=True)
    ctrl._persist_prediction_time_history = mock.AsyncMock(return_value=True)
    ctrl._persist_autoscaler_history = mock.AsyncMock(return_value=True)
    ctrl._lb_report_authority = mock.Mock(return_value=(True, True, False))
    ctrl._prepare_load_balancer_report = (
        prepare_head or
        mock.Mock(side_effect=lambda request, _: (True, request, True)))
    ctrl._apply_prepared_load_balancer_report = runtime_tail
    ctrl._load_balancer_disclosure_is_authorized = mock.Mock(return_value=True)


def test_role_cutover_captures_sync_head_snapshot_while_tail_is_blocked():
    ctrl = _role_controller()
    snapshot = lb_ha.DemandSnapshot((10, 20), 4, 2)
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    routing = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1', 'runtime-new')
    tail_started = threading.Event()
    release_tail = threading.Event()

    def prepare_head(request_data, _authority):
        ctrl._lb_last_demand_snapshot = snapshot
        return True, request_data, True

    def blocking_tail(*_args):
        tail_started.set()
        assert release_tail.wait(timeout=2)
        return True

    _configure_sync_controller(ctrl, blocking_tail, prepare_head)

    async def drive():
        sync = asyncio.create_task(
            ctrl._handle_load_balancer_sync({'lb_session_id': 'active'}))
        started = await asyncio.wait_for(asyncio.to_thread(
            tail_started.wait, 1),
                                         timeout=2)
        assert started
        role = await asyncio.wait_for(ctrl._handle_load_balancer_role(
            _role_request('standby', lb_ha.LbSlot.B)),
                                      timeout=1)
        assert not sync.done()
        release_tail.set()
        return role, await asyncio.wait_for(sync, timeout=2)

    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=_authority()), mock.patch.object(
                               controller.lb_k8s,
                               'get_lb_service_routing',
                               return_value=routing), mock.patch.object(
                                   controller.serve_state,
                                   'get_lb_cutover_state',
                                   return_value=stable), mock.patch.object(
                                       controller.serve_state,
                                       'begin_lb_cutover',
                                       return_value=preparing) as begin:
        try:
            role_response, sync_response = asyncio.run(drive())
        finally:
            release_tail.set()

    assert role_response.status_code == 200
    assert sync_response.status_code == 200
    assert json.loads(role_response.body)['role'] == 'ARMED'
    assert begin.call_args.args[-1] is snapshot


def test_ordinary_role_heartbeat_does_not_wait_for_sync_demand_head():
    ctrl = _role_controller()
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    routing = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1')
    head_started = threading.Event()
    release_head = threading.Event()

    def blocking_head(request_data, _authority):
        head_started.set()
        assert release_head.wait(timeout=2)
        return True, request_data, True

    _configure_sync_controller(ctrl, mock.Mock(return_value=True),
                               blocking_head)

    async def drive():
        sync = asyncio.create_task(
            ctrl._handle_load_balancer_sync({'lb_session_id': 'active'}))
        started = await asyncio.wait_for(asyncio.to_thread(
            head_started.wait, 1),
                                         timeout=2)
        assert started
        role = await asyncio.wait_for(ctrl._handle_load_balancer_role(
            _role_request('active', lb_ha.LbSlot.A)),
                                      timeout=1)
        assert not sync.done()
        release_head.set()
        return role, await asyncio.wait_for(sync, timeout=2)

    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=_authority()), mock.patch.object(
                               controller.lb_k8s,
                               'get_lb_service_routing',
                               return_value=routing), mock.patch.object(
                                   controller.serve_state,
                                   'get_lb_cutover_state',
                                   return_value=stable):
        try:
            role_response, sync_response = asyncio.run(drive())
        finally:
            release_head.set()

    assert role_response.status_code == 200
    assert sync_response.status_code == 200


def test_role_heartbeat_uses_one_shared_authority_snapshot():
    ctrl = _role_controller()
    ctrl._controller_owner = (123, '10.0.0.1')
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    routing = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1', 'runtime-new')
    snapshot = lb_k8s.LbRoleSnapshot(_authority(), routing)

    def read_snapshot(service_name, fence, state, owner, timings):
        assert service_name == 'service'
        assert fence == ('incarnation', (123, '10.0.0.1'), 7)
        assert state == stable
        assert owner['lb_cutover_generation'] == 1
        timings['snapshot_pod_list'] = 0.02
        timings['snapshot_service_read'] = 0.03
        return snapshot

    with (mock.patch.object(controller.lb_k8s,
                            'get_lb_role_snapshot',
                            side_effect=read_snapshot) as snapshot_read,
          mock.patch.object(
              controller.lb_k8s,
              'get_lb_pod_authority',
              side_effect=AssertionError('duplicate Pod authority read')),
          mock.patch.object(
              controller.lb_k8s,
              'get_lb_service_routing',
              side_effect=AssertionError('duplicate Service routing read')),
          mock.patch.object(controller.serve_state,
                            'get_lb_cutover_state',
                            return_value=stable)):
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))

    assert response.status_code == 200
    body = json.loads(response.body)
    assert body['role'] == lb_ha.LbRole.ACTIVE.value
    assert body['observability']['phases_seconds'][
        'postgresql_role_state_read'] >= 0
    assert body['observability']['phases_seconds'][
        'snapshot_pod_list'] == pytest.approx(0.02)
    assert body['observability']['phases_seconds'][
        'snapshot_service_read'] == pytest.approx(0.03)
    snapshot_read.assert_called_once()
    ctrl._owns_current_service.assert_not_called()
    assert response.headers[
        serve_constants.LB_ROLE_CONTROLLER_OWNER_VERIFIED_HEADER] == (
            ctrl._controller_owner_fingerprint)
    assert ctrl._lb_role_database_snapshot.call_count == 2
    assert ctrl._lb_cutover_fence.call_count == 2


@pytest.mark.parametrize(
    ('error', 'outcome'),
    [(lb_k8s.LbRoleSnapshotStateMismatchError('owner changed'),
      'cutover_state_unavailable'),
     (lb_k8s.LbRoleSnapshotRoutingError('Service malformed'),
      'routing_unavailable')])
def test_role_snapshot_errors_keep_deterministic_outcomes(error, outcome):
    ctrl = _role_controller()
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    with mock.patch.object(controller.lb_k8s,
                           'get_lb_role_snapshot',
                           side_effect=error), mock.patch.object(
                               controller.serve_state,
                               'get_lb_cutover_state',
                               return_value=stable):
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))

    assert response.status_code == 503
    assert json.loads(response.body)['outcome'] == outcome


def test_concurrent_slot_heartbeats_keep_shared_snapshot_fencing():
    ctrl = _role_controller()
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    authority = lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'active', 'standby'},
        live_uids={'active', 'standby'},
        slot_by_uid={
            'active': lb_ha.LbSlot.A,
            'standby': lb_ha.LbSlot.B,
        },
        selected_slot=lb_ha.LbSlot.A)
    snapshot = lb_k8s.LbRoleSnapshot(
        authority, lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1'))
    observed_fences = []
    snapshot_started = threading.Event()
    release_snapshot = threading.Event()
    backend_urls = [f'http://replica-{index}' for index in range(143)]
    large_report = _report({url: 1 for url in backend_urls},
                           {url: 1 for url in backend_urls},
                           {url: 1 for url in backend_urls},
                           {url: 0.0 for url in backend_urls})

    def read_snapshot(unused_name, fence, state, unused_owner, unused_timings):
        observed_fences.append((fence, state))
        snapshot_started.set()
        assert release_snapshot.wait(timeout=2)
        return snapshot

    async def run_both_slots():
        active_request = _role_request('active', lb_ha.LbSlot.A)
        standby_request = _role_request('standby', lb_ha.LbSlot.B)
        active_request.update(large_report)
        standby_request.update(large_report)
        active_task = asyncio.create_task(
            ctrl._handle_load_balancer_role(active_request))
        while not snapshot_started.is_set():
            await asyncio.sleep(0.001)
        standby_task = asyncio.create_task(
            ctrl._handle_load_balancer_role(standby_request))
        while ctrl._lb_cutover_fence.call_count < 2:
            await asyncio.sleep(0.001)
        await asyncio.sleep(0.01)
        release_snapshot.set()
        return await asyncio.gather(active_task, standby_task)

    with mock.patch.object(
            controller.lb_k8s, 'get_lb_role_snapshot',
            side_effect=read_snapshot) as snapshot_read, mock.patch.object(
                controller.serve_state,
                'get_lb_cutover_state',
                return_value=stable):
        try:
            active_response, standby_response = asyncio.run(run_both_slots())
        finally:
            release_snapshot.set()

    assert json.loads(active_response.body)['role'] == 'ACTIVE'
    assert json.loads(standby_response.body)['role'] == 'STANDBY'
    assert snapshot_read.call_count == 1
    assert observed_fences == [
        (('incarnation', (123, '10.0.0.1'), 7), stable),
    ]
    assert ctrl._lb_cutover_fence.call_count == 4


@pytest.mark.parametrize('changed_key', ['fence', 'state', 'owner'])
def test_concurrent_stable_snapshots_with_different_keys_never_share(
        changed_key):
    ctrl = _role_controller()
    fence = ('incarnation', (123, '10.0.0.1'), 7)
    changed_fence = ('other-incarnation', (123, '10.0.0.1'), 7)
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    changed_state = _state(lb_ha.LbCutoverPhase.STABLE, generation=2)
    snapshot = lb_k8s.LbRoleSnapshot(
        _authority(), lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1'))
    snapshot_barrier = threading.Barrier(2)

    def read_snapshot(*_args):
        snapshot_barrier.wait(timeout=2)
        return snapshot

    async def run_different_keys():
        loop = asyncio.get_running_loop()
        owner = _role_owner_record(fence, stable)
        first = asyncio.create_task(
            ctrl._get_shared_stable_lb_role_snapshot(loop, fence, stable,
                                                     owner))
        if changed_key == 'fence':
            second_key = (changed_fence, stable,
                          _role_owner_record(changed_fence, stable))
        elif changed_key == 'state':
            second_key = (fence, changed_state,
                          _role_owner_record(fence, changed_state))
        else:
            second_key = (fence, stable,
                          dict(owner, resource_scope='successor-scope'))
        second = asyncio.create_task(
            ctrl._get_shared_stable_lb_role_snapshot(loop, *second_key))
        return await asyncio.gather(first, second)

    with mock.patch.object(controller.lb_k8s,
                           'get_lb_role_snapshot',
                           side_effect=read_snapshot) as snapshot_read:
        reads = asyncio.run(run_different_keys())

    assert [read.snapshot for read in reads] == [snapshot, snapshot]
    assert snapshot_read.call_count == 2
    assert ctrl._lb_role_snapshot_task is None
    assert ctrl._lb_role_snapshot_key is None


def test_completed_stable_snapshot_is_never_reused():
    ctrl = _role_controller()
    fence = ('incarnation', (123, '10.0.0.1'), 7)
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    snapshot = lb_k8s.LbRoleSnapshot(
        _authority(), lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1'))
    owner = _role_owner_record(fence, stable)

    async def read_twice():
        loop = asyncio.get_running_loop()
        first = await ctrl._get_shared_stable_lb_role_snapshot(
            loop, fence, stable, owner)
        second = await ctrl._get_shared_stable_lb_role_snapshot(
            loop, fence, stable, owner)
        return first, second

    with mock.patch.object(controller.lb_k8s,
                           'get_lb_role_snapshot',
                           return_value=snapshot) as snapshot_read:
        reads = asyncio.run(read_twice())

    assert [read.snapshot for read in reads] == [snapshot, snapshot]
    assert snapshot_read.call_count == 2


def test_old_snapshot_completion_cannot_clear_new_key_task():
    ctrl = _role_controller()
    first_fence = ('incarnation', (123, '10.0.0.1'), 7)
    second_fence = ('other-incarnation', (123, '10.0.0.1'), 7)
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    snapshot = lb_k8s.LbRoleSnapshot(
        _authority(), lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1'))
    first_started = threading.Event()
    second_started = threading.Event()
    release_first = threading.Event()
    release_second = threading.Event()
    first_owner = _role_owner_record(first_fence, stable)
    second_owner = _role_owner_record(second_fence, stable)

    def read_snapshot(unused_name, fence, *_args):
        if fence == first_fence:
            first_started.set()
            assert release_first.wait(timeout=2)
        else:
            assert fence == second_fence
            second_started.set()
            assert release_second.wait(timeout=2)
        return snapshot

    async def complete_in_order():
        loop = asyncio.get_running_loop()
        first = asyncio.create_task(
            ctrl._get_shared_stable_lb_role_snapshot(loop, first_fence, stable,
                                                     first_owner))
        while not first_started.is_set():
            await asyncio.sleep(0.001)
        second = asyncio.create_task(
            ctrl._get_shared_stable_lb_role_snapshot(loop, second_fence, stable,
                                                     second_owner))
        while not second_started.is_set():
            await asyncio.sleep(0.001)
        release_first.set()
        await first
        await asyncio.sleep(0)
        assert ctrl._lb_role_snapshot_key == (second_fence, stable,
                                              tuple(sorted(
                                                  second_owner.items())))
        assert ctrl._lb_role_snapshot_task is not None
        assert not ctrl._lb_role_snapshot_task.done()
        release_second.set()
        await second

    with mock.patch.object(controller.lb_k8s,
                           'get_lb_role_snapshot',
                           side_effect=read_snapshot):
        try:
            asyncio.run(complete_in_order())
        finally:
            release_first.set()
            release_second.set()

    assert ctrl._lb_role_snapshot_task is None
    assert ctrl._lb_role_snapshot_key is None


def test_cancelled_stable_snapshot_waiter_does_not_poison_peer():
    ctrl = _role_controller()
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    snapshot = lb_k8s.LbRoleSnapshot(
        _authority(), lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1'))
    snapshot_started = threading.Event()
    release_snapshot = threading.Event()

    def read_snapshot(*_args):
        snapshot_started.set()
        assert release_snapshot.wait(timeout=2)
        return snapshot

    async def cancel_then_join():
        first = asyncio.create_task(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))
        while not snapshot_started.is_set():
            await asyncio.sleep(0.001)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        peer = asyncio.create_task(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))
        while ctrl._lb_cutover_fence.call_count < 2:
            await asyncio.sleep(0.001)
        await asyncio.sleep(0.01)
        release_snapshot.set()
        return await peer

    with mock.patch.object(
            controller.lb_k8s, 'get_lb_role_snapshot',
            side_effect=read_snapshot) as snapshot_read, mock.patch.object(
                controller.serve_state,
                'get_lb_cutover_state',
                return_value=stable):
        try:
            response = asyncio.run(cancel_then_join())
        finally:
            release_snapshot.set()

    assert response.status_code == 200
    assert snapshot_read.call_count == 1


@pytest.mark.parametrize(
    ('error', 'outcome'),
    [(lb_k8s.LbRoleSnapshotStateMismatchError('owner changed'),
      'cutover_state_unavailable'),
     (lb_k8s.LbRoleSnapshotRoutingError('Service malformed'),
      'routing_unavailable')])
def test_concurrent_stable_snapshot_errors_are_shared(error, outcome):
    ctrl = _role_controller()
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    snapshot_started = threading.Event()
    release_snapshot = threading.Event()

    def read_snapshot(*_args):
        snapshot_started.set()
        assert release_snapshot.wait(timeout=2)
        raise error

    async def run_both_slots():
        first = asyncio.create_task(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))
        while not snapshot_started.is_set():
            await asyncio.sleep(0.001)
        second = asyncio.create_task(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B)))
        while ctrl._lb_cutover_fence.call_count < 2:
            await asyncio.sleep(0.001)
        await asyncio.sleep(0.01)
        release_snapshot.set()
        return await asyncio.gather(first, second)

    with mock.patch.object(
            controller.lb_k8s, 'get_lb_role_snapshot',
            side_effect=read_snapshot) as snapshot_read, mock.patch.object(
                controller.serve_state,
                'get_lb_cutover_state',
                return_value=stable):
        try:
            responses = asyncio.run(run_both_slots())
        finally:
            release_snapshot.set()

    assert [json.loads(response.body)['outcome'] for response in responses
           ] == [outcome, outcome]
    assert snapshot_read.call_count == 1


@pytest.mark.parametrize('changed_authority', ['fence', 'state'])
def test_stable_role_prefetch_fails_closed_if_authority_changes(
        changed_authority):
    ctrl = _role_controller()
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    snapshot = lb_k8s.LbRoleSnapshot(
        _authority(), lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1'))
    fence = ('incarnation', (123, '10.0.0.1'), 7)
    changed_fence = ('incarnation', (456, '10.0.0.2'), 7)
    if changed_authority == 'fence':
        ctrl._lb_cutover_fence.side_effect = [fence, changed_fence]
        states = [stable, stable]
    else:
        ctrl._lb_cutover_fence.side_effect = [fence, fence]
        states = [stable, preparing]
    ctrl._lb_session_ledger.update = mock.Mock(
        side_effect=AssertionError('stale prefetch reached decision tail'))

    with mock.patch.object(
            controller.lb_k8s, 'get_lb_role_snapshot',
            return_value=snapshot) as snapshot_read, mock.patch.object(
                controller.serve_state,
                'get_lb_cutover_state',
                side_effect=states):
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))

    assert response.status_code == 503
    assert json.loads(response.body)['outcome'] == 'cutover_state_unavailable'
    snapshot_read.assert_called_once_with('service', fence, stable, mock.ANY,
                                          mock.ANY)
    ctrl._lb_session_ledger.update.assert_not_called()


def test_stable_role_prefetch_rejects_nonfence_owner_record_change():
    ctrl = _role_controller()
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    fence = ('incarnation', (123, '10.0.0.1'), 7)
    owner = {
        'hash': 'incarnation',
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'lifecycle_epoch': 7,
        'resource_scope': None,
        'lb_ha_enabled': True,
        'lb_active_slot': 'a',
        'lb_cutover_generation': 1,
        'lb_pending_slot': None,
        'lb_cutover_phase': 'STABLE',
        'lb_drain_started_at': None,
    }
    changed_owner = dict(owner, resource_scope='successor-scope')
    ctrl._lb_role_database_snapshot.side_effect = [
        controller._LbRoleDatabaseSnapshot(fence, stable, owner),
        controller._LbRoleDatabaseSnapshot(fence, stable, changed_owner),
    ]
    ctrl._lb_session_ledger.update = mock.Mock(
        side_effect=AssertionError('changed owner reached decision tail'))
    snapshot = lb_k8s.LbRoleSnapshot(
        _authority(), lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1'))

    with mock.patch.object(controller.lb_k8s,
                           'get_lb_role_snapshot',
                           return_value=snapshot):
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))

    assert response.status_code == 503
    assert json.loads(response.body)['outcome'] == 'cutover_state_unavailable'
    ctrl._lb_session_ledger.update.assert_not_called()


def test_nonstable_role_snapshot_stays_behind_transition_lock():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)

    async def drive(snapshot_read):
        role_lock = asyncio.Lock()
        ctrl._lb_role_lock = role_lock
        await role_lock.acquire()
        role = asyncio.create_task(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))
        await asyncio.sleep(0.05)
        snapshot_read.assert_not_called()
        role_lock.release()
        return await asyncio.wait_for(role, timeout=2)

    with mock.patch.object(
            controller.lb_k8s, 'get_lb_role_snapshot',
            return_value=None) as snapshot_read, mock.patch.object(
                controller.serve_state,
                'get_lb_cutover_state',
                return_value=preparing):
        response = asyncio.run(drive(snapshot_read))

    assert response.status_code == 503
    assert json.loads(response.body)['outcome'] == 'pod_authority_unavailable'
    snapshot_read.assert_called_once()


def test_steady_ha_sync_disclosure_does_not_wait_for_role_lock():
    ctrl = _role_controller()
    _configure_sync_controller(ctrl, mock.Mock(return_value=True))

    async def drive():
        role_lock = asyncio.Lock()
        ctrl._lb_role_lock = role_lock
        await role_lock.acquire()
        try:
            return await asyncio.wait_for(ctrl._handle_load_balancer_sync(
                {'lb_session_id': 'active'}),
                                          timeout=1)
        finally:
            role_lock.release()

    response = asyncio.run(drive())

    assert response.status_code == 200


def test_cancelled_cutover_publishes_blocking_view_after_local_handoff():
    ctrl = _role_controller()
    snapshot = lb_ha.DemandSnapshot((10, 20), 4, 2)
    ctrl._lb_last_demand_snapshot = snapshot
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    routing = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1', 'runtime-new')
    transition_started = threading.Event()
    release_transition = threading.Event()

    def blocking_begin(*_args):
        transition_started.set()
        assert release_transition.wait(timeout=2)
        return preparing

    async def drive():
        role = asyncio.create_task(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B)))
        started = await asyncio.wait_for(asyncio.to_thread(
            transition_started.wait, 1),
                                         timeout=2)
        assert started
        role.cancel()
        await asyncio.sleep(0)
        assert not role.done()
        release_transition.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(role, timeout=2)

    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=_authority()), mock.patch.object(
                               controller.lb_k8s,
                               'get_lb_service_routing',
                               return_value=routing), mock.patch.object(
                                   controller.serve_state,
                                   'get_lb_cutover_state',
                                   return_value=stable), mock.patch.object(
                                       controller.serve_state,
                                       'begin_lb_cutover',
                                       side_effect=blocking_begin):
        try:
            asyncio.run(drive())
        finally:
            release_transition.set()

    assert ctrl._lb_demand_handoff.generation == 2
    assert ctrl._lb_demand_handoff.snapshot is snapshot
    assert ctrl._replica_manager.update_lb_in_flight.call_args_list == [
        mock.call({}, None, [], [], 'ha-transition-1'),
        mock.call({}, None, [], [], 'ha-transition-2'),
    ]


def test_cutover_fence_accepts_parent_owner_from_controller_wiring():
    ctrl = _role_controller()
    del ctrl.__dict__['_lb_cutover_fence']
    parent_pid = os.getpid() + 1000
    ctrl._controller_owner = (parent_pid, '10.0.0.1')
    owner = {
        'hash': 'incarnation',
        'controller_pid': parent_pid,
        'controller_ip': '10.0.0.1',
        'lifecycle_epoch': 7,
        'lb_ha_enabled': True,
    }
    state = _state(lb_ha.LbCutoverPhase.STABLE)
    routing = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1')
    with mock.patch.object(
            controller.serve_state,
            'get_service_controller_owner',
            return_value=owner), mock.patch.object(
                controller.serve_state,
                'get_lb_cutover_state',
                return_value=state), mock.patch.object(
                    controller.lb_k8s,
                    'get_lb_pod_authority',
                    return_value=_authority()), mock.patch.object(
                        controller.lb_k8s,
                        'get_lb_service_routing',
                        return_value=routing):
        assert ctrl._lb_cutover_fence() == ('incarnation', (parent_pid,
                                                            '10.0.0.1'), 7)
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))

    assert response.status_code == 200
    assert json.loads(response.body)['role'] == 'ACTIVE'


def test_role_database_snapshot_reads_owner_and_complete_state_once():
    ctrl = _role_controller()
    del ctrl.__dict__['_lb_role_database_snapshot']
    owner = {
        'hash': 'incarnation',
        'status': controller.serve_state.ServiceStatus.READY,
        'controller_pid': 123,
        'controller_ip': '10.0.0.1',
        'controller_port': 20001,
        'lifecycle_epoch': 7,
        'pool': False,
        'resource_scope': 'scope',
        'lb_ha_enabled': True,
        'lb_active_slot': 'a',
        'lb_cutover_generation': 3,
        'lb_pending_slot': None,
        'lb_cutover_phase': 'STABLE',
        'lb_drain_started_at': 123.5,
    }
    owner_read = mock.Mock(return_value=owner)

    with mock.patch.object(controller.serve_state,
                           'get_service_controller_owner', owner_read):
        snapshot = ctrl._lb_role_database_snapshot()

    assert snapshot == controller._LbRoleDatabaseSnapshot(
        ('incarnation', (123, '10.0.0.1'), 7),
        lb_ha.LbCutoverState(True, lb_ha.LbSlot.A, 3, None,
                             lb_ha.LbCutoverPhase.STABLE, 7, 123.5), owner)
    owner_read.assert_called_once_with('service', include_lb_state=True)


def _authority(*, target_ready: bool = True) -> lb_k8s.LbPodAuthority:
    ready = {'active', 'standby'} if target_ready else {'active'}
    return lb_k8s.LbPodAuthority(ready_nonterminating_uids=ready,
                                 live_uids={'active', 'standby'},
                                 slot_by_uid={
                                     'active': lb_ha.LbSlot.A,
                                     'standby': lb_ha.LbSlot.B,
                                 },
                                 selected_slot=lb_ha.LbSlot.A,
                                 digest_by_uid={
                                     'active': 'sha256:old',
                                     'standby': 'sha256:new',
                                 },
                                 revision_by_uid={
                                     'active': 'runtime-old',
                                     'standby': 'runtime-new',
                                 })


def _role_request(session_id: str,
                  slot: lb_ha.LbSlot,
                  armed_generation: int | None = None) -> dict:
    return {
        'lb_session_id': session_id,
        'lb_slot': slot.value,
        'routing_version': 1,
        'armed_generation': armed_generation,
        **_report({}, {}, {}, {}, routing_urls=[]),
    }


def test_role_saga_arms_then_patches_and_commits_selected_standby():
    ctrl = _role_controller()
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    draining = _state(lb_ha.LbCutoverPhase.DRAINING,
                      active=lb_ha.LbSlot.B,
                      pending=lb_ha.LbSlot.A,
                      generation=2)
    routing_old = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1',
                                          'runtime-new')
    with mock.patch.object(
            controller.lb_k8s, 'get_lb_pod_authority',
            return_value=_authority()), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_routing',
                return_value=routing_old), mock.patch.object(
                    controller.serve_state,
                    'get_lb_cutover_state',
                    side_effect=[
                        stable, stable, preparing, preparing, draining
                    ]), mock.patch.object(
                        controller.serve_state,
                        'begin_lb_cutover',
                        return_value=preparing) as begin, mock.patch.object(
                            controller.lb_k8s,
                            'patch_lb_service_active_slot',
                            return_value=True) as patch, mock.patch.object(
                                controller.serve_state,
                                'commit_lb_cutover',
                                return_value=True) as commit:
        armed_response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B)))
        active_response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B, 2)))

    assert json.loads(armed_response.body)['role'] == 'ARMED'
    assert json.loads(active_response.body)['role'] == 'ACTIVE'
    begin.assert_called_once()
    patch.assert_called_once()
    commit.assert_called_once()


def test_role_saga_recovers_selector_patch_before_database_commit():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    draining = _state(lb_ha.LbCutoverPhase.DRAINING,
                      active=lb_ha.LbSlot.B,
                      pending=lb_ha.LbSlot.A,
                      generation=2)
    routing_target = lb_k8s.LbServiceRouting(lb_ha.LbSlot.B, 2, 'rv-2')
    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=_authority()), mock.patch.object(
                               controller.lb_k8s,
                               'get_lb_service_routing',
                               return_value=routing_target), mock.patch.object(
                                   controller.serve_state,
                                   'get_lb_cutover_state',
                                   side_effect=[
                                       preparing, preparing, draining
                                   ]), mock.patch.object(
                                       controller.lb_k8s,
                                       'patch_lb_service_active_slot'
                                   ) as patch, mock.patch.object(
                                       controller.serve_state,
                                       'commit_lb_cutover',
                                       return_value=True) as commit:
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B, 2)))

    assert json.loads(response.body)['role'] == 'ACTIVE'
    patch.assert_not_called()
    commit.assert_called_once()


def test_role_saga_fails_closed_when_recovery_commit_cas_is_rejected():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    routing_target = lb_k8s.LbServiceRouting(lb_ha.LbSlot.B, 2, 'rv-2')
    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=_authority()), mock.patch.object(
                               controller.lb_k8s,
                               'get_lb_service_routing',
                               return_value=routing_target), mock.patch.object(
                                   controller.serve_state,
                                   'get_lb_cutover_state',
                                   return_value=preparing), mock.patch.object(
                                       controller.serve_state,
                                       'commit_lb_cutover',
                                       return_value=False):
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B, 2)))

    assert response.status_code == 503
    assert json.loads(response.body)['outcome'] == 'transition_inconsistent'
    ctrl._replica_manager.update_lb_in_flight.assert_called_once_with(
        {}, None, [], [], 'ha-transition-2')
    ctrl._publish_ha_drain_view.assert_not_called()


def test_role_saga_fails_closed_when_patched_selector_commit_is_rejected():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    routing_old = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1')
    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=_authority()), mock.patch.object(
                               controller.lb_k8s,
                               'get_lb_service_routing',
                               return_value=routing_old), mock.patch.object(
                                   controller.serve_state,
                                   'get_lb_cutover_state',
                                   return_value=preparing), mock.patch.object(
                                       controller.lb_k8s,
                                       'patch_lb_service_active_slot',
                                       return_value=True), mock.patch.object(
                                           controller.serve_state,
                                           'commit_lb_cutover',
                                           return_value=False):
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B, 2)))

    assert response.status_code == 503
    assert json.loads(response.body)['outcome'] == 'transition_inconsistent'
    ctrl._replica_manager.update_lb_in_flight.assert_called_once_with(
        {}, None, [], [], 'ha-transition-2')
    ctrl._publish_ha_drain_view.assert_not_called()


def test_role_saga_keeps_blocking_view_when_recovery_commit_raises():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    routing_target = lb_k8s.LbServiceRouting(lb_ha.LbSlot.B, 2, 'rv-2')
    with mock.patch.object(
            controller.lb_k8s, 'get_lb_pod_authority',
            return_value=_authority()), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_routing',
                return_value=routing_target), mock.patch.object(
                    controller.serve_state,
                    'get_lb_cutover_state',
                    return_value=preparing), mock.patch.object(
                        controller.serve_state,
                        'commit_lb_cutover',
                        side_effect=RuntimeError('ambiguous commit')):
        with pytest.raises(RuntimeError, match='ambiguous commit'):
            asyncio.run(
                ctrl._handle_load_balancer_role(
                    _role_request('standby', lb_ha.LbSlot.B, 2)))

    ctrl._replica_manager.update_lb_in_flight.assert_called_once_with(
        {}, None, [], [], 'ha-transition-2')
    ctrl._publish_ha_drain_view.assert_not_called()


def test_role_saga_keeps_blocking_view_when_post_selector_commit_raises():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    routing_old = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1')
    with mock.patch.object(
            controller.lb_k8s, 'get_lb_pod_authority',
            return_value=_authority()), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_routing',
                return_value=routing_old), mock.patch.object(
                    controller.serve_state,
                    'get_lb_cutover_state',
                    return_value=preparing), mock.patch.object(
                        controller.lb_k8s,
                        'patch_lb_service_active_slot',
                        return_value=True), mock.patch.object(
                            controller.serve_state,
                            'commit_lb_cutover',
                            side_effect=RuntimeError('ambiguous commit')):
        with pytest.raises(RuntimeError, match='ambiguous commit'):
            asyncio.run(
                ctrl._handle_load_balancer_role(
                    _role_request('standby', lb_ha.LbSlot.B, 2)))

    ctrl._replica_manager.update_lb_in_flight.assert_called_once_with(
        {}, None, [], [], 'ha-transition-2')
    ctrl._publish_ha_drain_view.assert_not_called()


def test_deferred_cancellation_wins_when_followup_commit_raises():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    routing_old = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1')
    patch_started = threading.Event()
    release_patch = threading.Event()

    def blocking_patch(*_args):
        patch_started.set()
        assert release_patch.wait(timeout=2)
        return True

    async def drive():
        role = asyncio.create_task(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B, 2)))
        started = await asyncio.wait_for(asyncio.to_thread(
            patch_started.wait, 1),
                                         timeout=2)
        assert started
        role.cancel()
        await asyncio.sleep(0)
        assert not role.done()
        release_patch.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(role, timeout=2)

    with mock.patch.object(
            controller.lb_k8s, 'get_lb_pod_authority',
            return_value=_authority()), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_routing',
                return_value=routing_old), mock.patch.object(
                    controller.serve_state,
                    'get_lb_cutover_state',
                    return_value=preparing), mock.patch.object(
                        controller.lb_k8s,
                        'patch_lb_service_active_slot',
                        side_effect=blocking_patch), mock.patch.object(
                            controller.serve_state,
                            'commit_lb_cutover',
                            side_effect=RuntimeError(
                                'commit failed')) as commit:
        try:
            asyncio.run(drive())
        finally:
            release_patch.set()

    commit.assert_called_once()
    ctrl._replica_manager.update_lb_in_flight.assert_called_once_with(
        {}, None, [], [], 'ha-transition-2')
    ctrl._publish_ha_drain_view.assert_not_called()


def test_role_saga_keeps_blocking_view_when_post_commit_read_is_cancelled():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    draining = _state(lb_ha.LbCutoverPhase.DRAINING,
                      active=lb_ha.LbSlot.B,
                      pending=lb_ha.LbSlot.A,
                      generation=2)
    routing_target = lb_k8s.LbServiceRouting(lb_ha.LbSlot.B, 2, 'rv-2')
    read_started = threading.Event()
    release_read = threading.Event()
    reads = []

    def blocking_post_commit_read(_service_name):
        reads.append(threading.get_ident())
        if len(reads) < 3:
            return preparing
        read_started.set()
        assert release_read.wait(timeout=2)
        return draining

    async def drive():
        role = asyncio.create_task(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B, 2)))
        started = await asyncio.wait_for(asyncio.to_thread(
            read_started.wait, 1),
                                         timeout=2)
        assert started
        role.cancel()
        release_read.set()
        with pytest.raises(asyncio.CancelledError):
            await role

    with mock.patch.object(
            controller.lb_k8s, 'get_lb_pod_authority',
            return_value=_authority()), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_routing',
                return_value=routing_target), mock.patch.object(
                    controller.serve_state,
                    'get_lb_cutover_state',
                    side_effect=blocking_post_commit_read), mock.patch.object(
                        controller.serve_state,
                        'commit_lb_cutover',
                        return_value=True):
        try:
            asyncio.run(drive())
        finally:
            release_read.set()

    assert len(reads) == 3
    ctrl._replica_manager.update_lb_in_flight.assert_called_once_with(
        {}, None, [], [], 'ha-transition-2')
    ctrl._publish_ha_drain_view.assert_not_called()


def test_recovery_cancellation_while_waiting_for_demand_lock_stays_blocked():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    routing_target = lb_k8s.LbServiceRouting(lb_ha.LbSlot.B, 2, 'rv-2')

    async def drive():
        demand_lock = asyncio.Lock()
        await demand_lock.acquire()
        ctrl._lb_demand_lock = demand_lock
        role = asyncio.create_task(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B, 2)))
        try:

            async def wait_for_invalidation():
                while not ctrl._replica_manager.update_lb_in_flight.called:
                    await asyncio.sleep(0)

            await asyncio.wait_for(wait_for_invalidation(), timeout=1)
            assert not role.done()
            role.cancel()
            with pytest.raises(asyncio.CancelledError):
                await role
        finally:
            demand_lock.release()

    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=_authority()), mock.patch.object(
                               controller.lb_k8s,
                               'get_lb_service_routing',
                               return_value=routing_target), mock.patch.object(
                                   controller.serve_state,
                                   'get_lb_cutover_state',
                                   return_value=preparing), mock.patch.object(
                                       controller.serve_state,
                                       'commit_lb_cutover') as commit:
        asyncio.run(drive())

    ctrl._replica_manager.update_lb_in_flight.assert_called_once_with(
        {}, None, [], [], 'ha-transition-2')
    commit.assert_not_called()
    ctrl._publish_ha_drain_view.assert_not_called()


def test_role_saga_aborts_unselected_target_that_lost_readiness():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    stable = _state(lb_ha.LbCutoverPhase.STABLE, generation=2)
    routing_old = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1')
    with mock.patch.object(
            controller.lb_k8s,
            'get_lb_pod_authority',
            return_value=_authority(target_ready=False)), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_routing',
                return_value=routing_old), mock.patch.object(
                    controller.lb_k8s,
                    'patch_lb_service_aborted_generation',
                    return_value=True), mock.patch.object(
                        controller.serve_state,
                        'get_lb_cutover_state',
                        side_effect=[preparing, preparing,
                                     stable]), mock.patch.object(
                                         controller.serve_state,
                                         'abort_lb_cutover_preparation',
                                         return_value=True) as abort:
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))

    assert json.loads(response.body)['role'] == 'ACTIVE'
    abort.assert_called_once()


def test_role_saga_recovers_abort_patch_before_database_commit():
    ctrl = _role_controller()
    preparing = _state(lb_ha.LbCutoverPhase.PREPARING,
                       pending=lb_ha.LbSlot.B,
                       generation=2)
    stable = _state(lb_ha.LbCutoverPhase.STABLE, generation=2)
    routing_advanced = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 2, 'rv-2')
    with mock.patch.object(
            controller.lb_k8s,
            'get_lb_pod_authority',
            return_value=_authority(target_ready=False)), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_routing',
                return_value=routing_advanced), mock.patch.object(
                    controller.lb_k8s, 'patch_lb_service_aborted_generation'
                ) as patch, mock.patch.object(
                    controller.serve_state,
                    'get_lb_cutover_state',
                    side_effect=[preparing, preparing,
                                 stable]), mock.patch.object(
                                     controller.serve_state,
                                     'abort_lb_cutover_preparation',
                                     return_value=True) as abort:
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))

    assert json.loads(response.body)['generation'] == 2
    patch.assert_not_called()
    abort.assert_called_once()


def test_rollout_evidence_requires_both_ready_slots_on_same_revision():
    stable = _state(lb_ha.LbCutoverPhase.STABLE)
    authority = _authority()

    drifted = controller.SkyServeController._lb_ha_rollout_evidence(
        authority, stable, 'runtime-new')
    assert not drifted['slots_converged']
    assert drifted['slots']['a'] == {
        'ready': True,
        'revisions': ['runtime-old'],
    }
    assert drifted['slots']['b'] == {
        'ready': True,
        'revisions': ['runtime-new'],
    }

    authority = authority._replace(revision_by_uid={
        'active': 'runtime-new',
        'standby': 'runtime-new',
    })
    converged = controller.SkyServeController._lb_ha_rollout_evidence(
        authority, stable, 'runtime-new')
    assert converged['slots_converged']
    assert converged['selected_slot'] == 'a'

    unready = authority._replace(ready_nonterminating_uids={'active'})
    assert not controller.SkyServeController._lb_ha_rollout_evidence(
        unready, stable, 'runtime-new')['slots_converged']


def test_planned_upgrade_never_repromotes_old_former_active_revision():
    ctrl = _role_controller()
    stable = _state(lb_ha.LbCutoverPhase.STABLE,
                    active=lb_ha.LbSlot.B,
                    generation=2)
    authority = lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'new-active', 'old-former-active'},
        live_uids={'new-active', 'old-former-active'},
        slot_by_uid={
            'new-active': lb_ha.LbSlot.B,
            'old-former-active': lb_ha.LbSlot.A,
        },
        selected_slot=lb_ha.LbSlot.B,
        revision_by_uid={
            'new-active': 'runtime-new',
            'old-former-active': 'runtime-old',
        })
    routing = lb_k8s.LbServiceRouting(lb_ha.LbSlot.B, 2, 'rv-2', 'runtime-new')
    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=authority), mock.patch.object(
                               controller.lb_k8s,
                               'get_lb_service_routing',
                               return_value=routing), mock.patch.object(
                                   controller.serve_state,
                                   'get_lb_cutover_state',
                                   return_value=stable), mock.patch.object(
                                       controller.serve_state,
                                       'begin_lb_cutover') as begin:
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('old-former-active', lb_ha.LbSlot.A)))

    assert json.loads(response.body)['role'] == 'STANDBY'
    begin.assert_not_called()


def test_legacy_selected_role_heartbeat_cannot_publish_idle_slot_drain_view():
    ctrl = _role_controller()
    ctrl._lb_session_ledger.aggregate = mock.Mock()

    controller.SkyServeController._publish_ha_drain_view(
        ctrl,
        _authority(),
        _state(lb_ha.LbCutoverPhase.MIGRATING),
        legacy_selected=True)

    ctrl._lb_session_ledger.aggregate.assert_not_called()


def test_migration_tail_blocks_clean_slot_drain_view_after_selector_move():
    ctrl = _role_controller()
    ctrl._replica_manager = mock.Mock()
    ctrl._lb_session_ledger.update('active', lb_ha.LbSlot.A,
                                   lb_ha.LbRole.ACTIVE, 1,
                                   _report({}, {}, {}, {}, routing_urls=[]))
    authority = _authority()._replace(live_uids={'active', 'standby', 'legacy'},
                                      legacy_uids={'legacy'})

    controller.SkyServeController._publish_ha_drain_view(
        ctrl, authority, _state(lb_ha.LbCutoverPhase.MIGRATING))

    ctrl._replica_manager.update_lb_in_flight.assert_called_once_with(
        {}, None, [], [], 'ha-generation-1')


def test_migration_selector_patch_blocks_tail_in_same_role_heartbeat():
    ctrl = _role_controller()
    migrating = _state(lb_ha.LbCutoverPhase.MIGRATING, generation=1)
    authority = lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'active', 'standby', 'legacy'},
        live_uids={'active', 'standby', 'legacy'},
        slot_by_uid={
            'active': lb_ha.LbSlot.A,
            'standby': lb_ha.LbSlot.B,
        },
        selected_slot=None,
        legacy_uids={'legacy'})
    routing = lb_k8s.LbServiceTransitionRouting(None, True, None, 'rv-1')
    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=authority), mock.patch.object(
                               controller.lb_k8s,
                               'get_lb_service_transition_routing',
                               return_value=routing), mock.patch.object(
                                   controller.serve_state,
                                   'get_lb_cutover_state',
                                   return_value=migrating), mock.patch.object(
                                       controller.lb_k8s,
                                       'patch_lb_service_migration_to_slot',
                                       return_value=True) as patch:
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))

    assert json.loads(response.body)['role'] == 'ACTIVE'
    patch.assert_called_once()
    ctrl._publish_ha_drain_view.assert_called_once_with(authority, migrating,
                                                        False)


def test_rollback_legacy_report_cannot_supersede_live_slot_drain_view():
    ctrl = _role_controller()
    rolling_back = _state(lb_ha.LbCutoverPhase.ROLLING_BACK,
                          active=lb_ha.LbSlot.B,
                          generation=5)
    authority = lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'active', 'standby', 'legacy'},
        live_uids={'active', 'standby', 'legacy'},
        slot_by_uid={
            'active': lb_ha.LbSlot.B,
            'standby': lb_ha.LbSlot.A,
        },
        legacy_uids={'legacy'},
        selected_slot=None)
    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=authority), mock.patch.object(
                               controller.serve_state,
                               'get_lb_cutover_state',
                               return_value=rolling_back):
        assert ctrl._lb_report_authority('legacy') == (True, True, False)

    authority = authority._replace(ready_nonterminating_uids={'legacy'},
                                   live_uids={'legacy'},
                                   slot_by_uid={})
    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=authority), mock.patch.object(
                               controller.serve_state,
                               'get_lb_cutover_state',
                               return_value=rolling_back):
        assert ctrl._lb_report_authority('legacy') == (True, True, True)


def test_rollback_prepatch_keeps_terminating_migration_tail_in_drain_union():
    ctrl = _role_controller()
    ctrl._replica_manager = mock.Mock()
    ctrl._lb_session_ledger.update('active', lb_ha.LbSlot.A,
                                   lb_ha.LbRole.ACTIVE, 5,
                                   _report({}, {}, {}, {}, routing_urls=[]))
    authority = lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'active', 'standby'},
        live_uids={'active', 'standby', 'legacy-tail'},
        slot_by_uid={
            'active': lb_ha.LbSlot.A,
            'standby': lb_ha.LbSlot.B,
        },
        selected_slot=lb_ha.LbSlot.A,
        legacy_uids={'legacy-tail'},
        terminating_uids={'legacy-tail'})
    rolling_back = _state(lb_ha.LbCutoverPhase.ROLLING_BACK,
                          active=lb_ha.LbSlot.A,
                          generation=5)

    controller.SkyServeController._publish_ha_drain_view(ctrl,
                                                         authority,
                                                         rolling_back,
                                                         legacy_selected=False)

    ctrl._replica_manager.update_lb_in_flight.assert_called_once_with(
        {}, None, [], [], 'ha-generation-5')


def test_migration_commits_without_blocking_role_on_cleanup():
    ctrl = _role_controller()
    migrating = _state(lb_ha.LbCutoverPhase.MIGRATING, generation=1)
    stable = _state(lb_ha.LbCutoverPhase.STABLE, generation=1)
    authority = lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'active', 'standby'},
        live_uids={'active', 'standby'},
        slot_by_uid={
            'active': lb_ha.LbSlot.A,
            'standby': lb_ha.LbSlot.B,
        },
        legacy_uids={'legacy'},
        selected_slot=lb_ha.LbSlot.A,
        digest_by_uid={})
    routing = lb_k8s.LbServiceTransitionRouting(lb_ha.LbSlot.A, False, 1,
                                                'rv-2')
    with mock.patch.object(
            controller.lb_k8s, 'get_lb_pod_authority',
            return_value=authority), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_transition_routing',
                return_value=routing), mock.patch.object(
                    controller.serve_state,
                    'get_lb_cutover_state',
                    side_effect=[
                        migrating, migrating, stable
                    ]), mock.patch.object(
                        controller.serve_state,
                        'finish_lb_ha_migration',
                        return_value=True) as finish, mock.patch.object(
                            controller.lb_k8s,
                            'cleanup_lb_mode_transition') as cleanup:
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('active', lb_ha.LbSlot.A)))

    assert json.loads(response.body)['phase'] == 'STABLE'
    finish.assert_called_once()
    cleanup.assert_not_called()


def test_rollback_commits_without_blocking_role_on_cleanup():
    ctrl = _role_controller()
    rolling_back = _state(lb_ha.LbCutoverPhase.ROLLING_BACK,
                          active=lb_ha.LbSlot.B,
                          generation=5)
    authority = lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'active', 'standby', 'legacy'},
        live_uids={'active', 'standby', 'legacy'},
        slot_by_uid={
            'active': lb_ha.LbSlot.B,
            'standby': lb_ha.LbSlot.A,
        },
        legacy_uids={'legacy'},
        selected_slot=None,
        digest_by_uid={})
    routing = lb_k8s.LbServiceTransitionRouting(None, True, None, 'rv-7')
    with mock.patch.object(
            controller.lb_k8s, 'get_lb_pod_authority',
            return_value=authority), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_transition_routing',
                return_value=routing), mock.patch.object(
                    controller.serve_state,
                    'get_lb_cutover_state',
                    return_value=rolling_back), mock.patch.object(
                        controller.serve_state,
                        'finish_lb_ha_rollback',
                        return_value=True) as finish, mock.patch.object(
                            controller.lb_k8s,
                            'cleanup_lb_mode_transition') as cleanup:
        request = _role_request('active', lb_ha.LbSlot.B)
        request.update(applied_role='DRAINING', applied_generation=5)
        response = asyncio.run(ctrl._handle_load_balancer_role(request))

    assert json.loads(response.body)['role'] == 'DRAINING'
    assert not ctrl._lb_ha_enabled
    finish.assert_called_once()
    cleanup.assert_not_called()


def test_rollback_waits_for_former_active_local_work_to_drain():
    ctrl = _role_controller()
    rolling_back = _state(lb_ha.LbCutoverPhase.ROLLING_BACK,
                          active=lb_ha.LbSlot.B,
                          generation=5)
    authority = lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'active', 'standby', 'legacy'},
        live_uids={'active', 'standby', 'legacy'},
        slot_by_uid={
            'active': lb_ha.LbSlot.B,
            'standby': lb_ha.LbSlot.A,
        },
        legacy_uids={'legacy'},
        selected_slot=None,
        digest_by_uid={})
    routing = lb_k8s.LbServiceTransitionRouting(None, True, None, 'rv-7')
    request = _role_request('active', lb_ha.LbSlot.B)
    request.update(local_in_flight=1,
                   applied_role='DRAINING',
                   applied_generation=5)
    with mock.patch.object(
            controller.lb_k8s, 'get_lb_pod_authority',
            return_value=authority), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_transition_routing',
                return_value=routing), mock.patch.object(
                    controller.serve_state,
                    'get_lb_cutover_state',
                    return_value=rolling_back), mock.patch.object(
                        controller.serve_state,
                        'finish_lb_ha_rollback') as finish:
        response = asyncio.run(ctrl._handle_load_balancer_role(request))

    assert json.loads(response.body)['role'] == 'DRAINING'
    assert ctrl._lb_ha_enabled
    finish.assert_not_called()
    ctrl._publish_ha_drain_view.assert_called_once_with(authority, rolling_back,
                                                        True)


def test_draining_uses_local_work_not_replica_global_async_occupancy():
    ctrl = _role_controller()
    ctrl._lb_session_ledger.update(
        'former-active', lb_ha.LbSlot.A, lb_ha.LbRole.DRAINING, 2,
        _report({}, {'replica': 7}, {'replica': 3}, {'replica': 1.0},
                applied_role='DRAINING',
                applied_generation=2))
    authority = lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'active', 'former-active'},
        live_uids={'active', 'former-active'},
        slot_by_uid={
            'active': lb_ha.LbSlot.B,
            'former-active': lb_ha.LbSlot.A,
        })
    draining = _state(lb_ha.LbCutoverPhase.DRAINING,
                      active=lb_ha.LbSlot.B,
                      pending=lb_ha.LbSlot.A,
                      generation=2)

    with mock.patch.object(controller.serve_state,
                           'finish_lb_cutover_drain',
                           return_value=True) as finish:
        assert controller.SkyServeController._finish_ha_drain_if_safe(
            ctrl, authority, draining, ('incarnation', (123, '10.0.0.1'), 7))

    finish.assert_called_once()


def test_draining_waits_until_former_active_applies_drain_role():
    ctrl = _role_controller()
    ctrl._lb_session_ledger.update(
        'former-active', lb_ha.LbSlot.A, lb_ha.LbRole.DRAINING, 2,
        _report({}, {}, {}, {}, applied_role='ACTIVE', applied_generation=1))
    authority = lb_k8s.LbPodAuthority(
        ready_nonterminating_uids={'active', 'former-active'},
        live_uids={'active', 'former-active'},
        slot_by_uid={
            'active': lb_ha.LbSlot.B,
            'former-active': lb_ha.LbSlot.A,
        })
    draining = _state(lb_ha.LbCutoverPhase.DRAINING,
                      active=lb_ha.LbSlot.B,
                      pending=lb_ha.LbSlot.A,
                      generation=2)

    with mock.patch.object(controller.serve_state,
                           'finish_lb_cutover_drain') as finish:
        assert not controller.SkyServeController._finish_ha_drain_if_safe(
            ctrl, authority, draining, ('incarnation', (123, '10.0.0.1'), 7))

    finish.assert_not_called()


def test_role_failures_have_deterministic_outcome_categories():
    ctrl = _role_controller()

    invalid = asyncio.run(ctrl._handle_load_balancer_role({'lb_slot': 'a'}))
    assert invalid.status_code == 503
    assert json.loads(invalid.body)['outcome'] == 'invalid_report'

    ctrl._owns_current_service.return_value = False
    ctrl._lb_cutover_fence.return_value = None
    stale_owner = asyncio.run(
        ctrl._handle_load_balancer_role(_role_request('active',
                                                      lb_ha.LbSlot.A)))
    assert stale_owner.status_code == 503
    assert json.loads(stale_owner.body)['outcome'] == 'controller_not_owner'


def test_role_observability_measures_second_slot_lock_queueing():
    ctrl = _role_controller()
    state = _state(lb_ha.LbCutoverPhase.STABLE)
    routing = lb_k8s.LbServiceRouting(lb_ha.LbSlot.A, 1, 'rv-1')

    class ObservableLock:
        """Async lock that exposes when a contender begins waiting."""

        def __init__(self):
            self._lock = asyncio.Lock()
            self.waiting = asyncio.Event()

        async def acquire(self):
            await self._lock.acquire()

        def release(self):
            self._lock.release()

        async def __aenter__(self):
            self.waiting.set()
            await self._lock.acquire()

        async def __aexit__(self, *_args):
            self._lock.release()

    async def run_queued_heartbeat():
        ctrl._lb_role_lock = ObservableLock()
        await ctrl._lb_role_lock.acquire()
        heartbeat = asyncio.create_task(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B)))
        await asyncio.wait_for(ctrl._lb_role_lock.waiting.wait(), timeout=1)
        await asyncio.sleep(0.02)
        ctrl._lb_role_lock.release()
        return await heartbeat

    with mock.patch.object(controller.lb_k8s,
                           'get_lb_pod_authority',
                           return_value=_authority()), mock.patch.object(
                               controller.lb_k8s,
                               'get_lb_service_routing',
                               return_value=routing), mock.patch.object(
                                   controller.serve_state,
                                   'get_lb_cutover_state',
                                   return_value=state):
        response = asyncio.run(run_queued_heartbeat())

    body = json.loads(response.body)
    assert response.status_code == 200
    assert body['outcome'] == 'success'
    observation = body['observability']
    assert observation['lock_wait_seconds'] >= 0.01
    assert observation['lock_hold_seconds'] >= 0
    assert observation['phases_seconds']['kubernetes_role_snapshot'] >= 0
    assert observation['phases_seconds']['postgresql_role_state_read'] >= 0
