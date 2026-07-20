"""Deterministic tests for SkyServe external load balancer HA."""
# pylint: disable=protected-access,unexpected-keyword-arg
import asyncio
import json
import os
from unittest import mock

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
                                lifecycle_epoch=9)


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


def test_demand_handoff_holds_then_expires_previous_active_floor():
    handoff = lb_ha.DemandHandoff(5)
    handoff.begin(
        8, lb_ha.DemandSnapshot((10, 20), 7, 3, rejected_in_recent_window=2))
    cold = {
        'request_aggregator': {
            'timestamps': [20, 30],
        },
        'queue_depth': 1,
        'rejected_in_window': 0,
        'rejected_in_recent_window': 0,
    }
    floored = handoff.apply(8, cold, complete_authoritative_report=True, now=1)
    assert floored['request_aggregator']['timestamps'] == [10, 20, 30]
    assert floored['queue_depth'] == 7
    assert floored['rejected_in_window'] == 3
    assert floored['rejected_in_recent_window'] == 2
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


def test_complete_demand_report_does_not_require_all_occupancy_samples():
    report = {
        'in_flight': {
            'http://replica': 1,
        },
        'queue_depth': 0,
        'rejected_in_window': 3,
        'rejected_in_recent_window': 1,
        'unknown_in_flight_urls': ['http://unsampled'],
        'occupancy_sampled_urls': ['http://replica'],
    }

    assert controller.SkyServeController._lb_demand_report_is_complete(report)


def test_incomplete_demand_report_preserves_handoff_floor():
    complete = {
        'in_flight': {},
        'queue_depth': 0,
        'rejected_in_window': 0,
        'rejected_in_recent_window': 0,
        'unknown_in_flight_urls': [],
    }
    for field in complete:
        report = dict(complete)
        report[field] = None
        assert not controller.SkyServeController._lb_demand_report_is_complete(
            report)


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


def _role_controller() -> controller.SkyServeController:
    ctrl = controller.SkyServeController.__new__(controller.SkyServeController)
    ctrl._service_name = 'service'
    ctrl._resource_scope = None
    ctrl._lb_ha_enabled = True
    ctrl._lb_role_lock = None
    ctrl._lb_session_ledger = lb_ha.LbSessionLedger(10, 10)
    ctrl._lb_expected_occupancy_urls = set()
    ctrl._lb_occupancy_contract_known = True
    ctrl._lb_last_demand_snapshot = None
    ctrl._lb_demand_handoff = lb_ha.DemandHandoff(5)
    ctrl._lb_drain_timeout_seconds = 60
    ctrl._applied_version = 1
    ctrl._owns_current_service = mock.Mock(return_value=True)
    ctrl._lb_cutover_fence = mock.Mock(return_value=('incarnation',
                                                     (123, '10.0.0.1'), 7))
    ctrl._publish_ha_drain_view = mock.Mock()
    ctrl._finish_ha_drain_if_safe = mock.Mock(return_value=False)
    return ctrl


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
                        stable, preparing, draining
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
    with mock.patch.object(
            controller.lb_k8s, 'get_lb_pod_authority',
            return_value=_authority()), mock.patch.object(
                controller.lb_k8s,
                'get_lb_service_routing',
                return_value=routing_target), mock.patch.object(
                    controller.serve_state,
                    'get_lb_cutover_state',
                    side_effect=[preparing, draining]), mock.patch.object(
                        controller.lb_k8s, 'patch_lb_service_active_slot'
                    ) as patch, mock.patch.object(controller.serve_state,
                                                  'commit_lb_cutover',
                                                  return_value=True) as commit:
        response = asyncio.run(
            ctrl._handle_load_balancer_role(
                _role_request('standby', lb_ha.LbSlot.B, 2)))

    assert json.loads(response.body)['role'] == 'ACTIVE'
    patch.assert_not_called()
    commit.assert_called_once()


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
                        side_effect=[preparing, stable]), mock.patch.object(
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
                    side_effect=[preparing, stable]), mock.patch.object(
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
                    side_effect=[migrating, stable]), mock.patch.object(
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
    assert observation['phases_seconds']['kubernetes_pod_authority'] >= 0
    assert observation['phases_seconds']['postgresql_cutover_state_read'] >= 0
    assert observation['phases_seconds']['kubernetes_service_routing_read'] >= 0
