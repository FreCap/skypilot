"""Process-local manager registry tests for recovery route leases."""
# pylint: disable=protected-access,use-implicit-booleaness-not-comparison

import math

import pytest

from sky.serve import constants
from sky.serve import system_recovery_route_lease as route_lease


class _Clock:

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _NoAggregateValuesScanDict(dict):
    """Mapping guard proving identity advancement does no global value walk."""

    def values(self):
        raise AssertionError(
            'ordered identity advancement scanned every tombstone set')


def _generation(*,
                recovered: bool = False,
                controller: int = 1,
                record: int = 2,
                attempt: int = 3) -> route_lease.RouteGeneration:
    return route_lease.RouteGeneration(
        controller_epoch=f'00000000-0000-0000-0000-{controller:012d}',
        replica_record_id=f'00000000-0000-0000-0000-{record:012d}',
        event_id=(f'00000000-0000-0000-0000-{4:012d}' if recovered else None),
        attempt_id=f'00000000-0000-0000-0000-{attempt:012d}',
        recovery_state='RECOVERED' if recovered else 'ARMED')


def _issue(registry: route_lease.ManagerRouteLeaseRegistry,
           generation: route_lease.RouteGeneration,
           started_at: float = 0.0) -> None:
    assert registry.issue(7,
                          generation,
                          'http://replica:8000/',
                          '/ready',
                          None, {'X-Probe': 'yes'},
                          normal_probe_started_at=started_at)


@pytest.mark.parametrize(('value', 'expected'), [
    ('HTTP://Replica.Example.COM:80/', 'http://replica.example.com'),
    ('HTTPS://Replica.Example.COM:443', 'https://replica.example.com'),
    ('https://Replica.Example.COM:8443/', 'https://replica.example.com:8443'),
    ('http://192.0.2.1:80/', 'http://192.0.2.1'),
    ('http://[2001:0DB8:0000:0000:0000:0000:0000:0001]:80/',
     'http://[2001:db8::1]'),
])
def test_route_url_is_transport_canonical(value: str, expected: str) -> None:
    assert route_lease.normalize_route_url(value) == expected


@pytest.mark.parametrize('value', [
    'http://user@example.com',
    'http://example.com:',
    'http://example.com:0',
    'http://example.com:65536',
    'http://::1',
    'http://[::1',
    'http://example..com',
    'http://127.000.000.001',
    'http://0x7f000001',
    'http://0x7f.0.0.1',
    'http://0177.0.0.1',
    'http://127.1',
    'http://example.com/path',
    'http://example.com?',
    'http://example.com#',
])
def test_route_url_rejects_malformed_or_ambiguous_authority(value: str) -> None:
    with pytest.raises(route_lease.RouteLeaseError):
        route_lease.normalize_route_url(value)


def test_registry_treats_transport_aliases_as_the_same_route() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation()
    assert registry.issue(7,
                          generation,
                          'HTTP://Replica.Example.COM:80/',
                          '/ready',
                          None,
                          None,
                          normal_probe_started_at=clock.now)

    target = registry.probe_targets()[0]
    assert target.route_url == 'http://replica.example.com'
    assert target.probe_url == 'http://replica.example.com/ready'
    marker = registry.marker(7, generation, 'http://REPLICA.EXAMPLE.COM:80')
    assert marker is not None
    assert not registry.needs_issuance(7, generation,
                                       'http://replica.example.com')


def test_issued_but_never_activated_token_expires_irreversibly() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation()
    _issue(registry, generation)

    marker = registry.marker(7, generation, 'http://replica:8000')
    assert marker is not None
    assert registry.heartbeat_payload()['entries'] == []
    target = registry.probe_targets()[0]

    # Simulate a controller/prober pause through cleanup and replay.  A first
    # success from the replacement cannot activate the old heavy-sync token.
    clock.now = constants.SYSTEM_RECOVERY_ROUTE_LEASE_SECONDS + 23
    registry.record_probe_result(target,
                                 request_started_at=clock.now,
                                 succeeded=True)
    assert registry.marker(7, generation, target.route_url) is None
    assert registry.heartbeat_payload()['entries'] == []
    assert registry.is_retired(7, generation)
    assert not registry.issue(7,
                              generation,
                              target.route_url,
                              '/ready',
                              None,
                              None,
                              normal_probe_started_at=clock.now)


def test_activated_expiry_cannot_be_resurrected() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation()
    _issue(registry, generation)
    target = registry.probe_targets()[0]
    clock.now = 2
    registry.record_probe_result(target, request_started_at=1, succeeded=True)
    assert registry.heartbeat_payload(
    )['entries'][0]['remaining_seconds'] == pytest.approx(59)

    clock.now = 62
    registry.record_probe_result(target, request_started_at=62, succeeded=True)
    assert registry.heartbeat_payload()['entries'] == []
    assert registry.is_retired(7, generation)


def test_one_failure_can_recover_but_two_failures_retire() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation()
    _issue(registry, generation)
    target = registry.probe_targets()[0]
    clock.now = 1
    registry.record_probe_result(target, request_started_at=1, succeeded=True)
    clock.now = 5
    registry.record_probe_result(target, request_started_at=5, succeeded=False)
    clock.now = 10
    registry.record_probe_result(target, request_started_at=10, succeeded=True)
    assert registry.heartbeat_payload(
    )['entries'][0]['remaining_seconds'] == pytest.approx(60)

    clock.now = 15
    registry.record_probe_result(target, request_started_at=15, succeeded=False)
    clock.now = 20
    registry.record_probe_result(target, request_started_at=20, succeeded=False)
    assert registry.is_retired(7, generation)
    assert registry.heartbeat_payload()['entries'] == []


def test_exact_recovered_generation_may_replace_retired_original() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    original = _generation()
    _issue(registry, original)
    original_marker = registry.marker(7, original, 'http://replica:8000')
    assert original_marker is not None
    registry.deactivate(7, original)
    assert registry.is_retired(7, original)

    recovered = _generation(recovered=True, attempt=5)
    _issue(registry, recovered, started_at=clock.now)
    marker = registry.marker(7, recovered, 'http://replica:8000')
    assert marker is not None
    assert marker.route_token != original_marker.route_token


def test_stale_retired_generation_cannot_displace_new_generation() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    original = _generation()
    recovered = _generation(recovered=True, attempt=5)

    _issue(registry, original)
    _issue(registry, recovered)
    recovered_marker = registry.marker(7, recovered, 'http://replica:8000')
    assert recovered_marker is not None
    assert registry.is_retired(7, original)

    assert not registry.issue(7,
                              original,
                              'http://replica:8000',
                              '/ready',
                              None,
                              None,
                              normal_probe_started_at=clock.now)
    assert registry.marker(7, recovered,
                           'http://replica:8000') == recovered_marker


def test_same_generation_url_change_retires_instead_of_rotating() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation()
    _issue(registry, generation)
    assert not registry.issue(7,
                              generation,
                              'http://other:8000',
                              '/ready',
                              None,
                              None,
                              normal_probe_started_at=1)
    assert registry.is_retired(7, generation)
    assert registry.marker(7, generation, 'http://replica:8000') is None


def test_url_change_detection_is_observational_until_postcommit_issue() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    original = _generation()
    _issue(registry, original)
    target = registry.probe_targets()[0]
    registry.record_probe_result(target,
                                 request_started_at=clock.now,
                                 succeeded=True)
    assert registry.heartbeat_payload()['entries']

    marker = registry.marker(7, original, 'http://replica:8000')
    payload = registry.heartbeat_payload()
    targets = registry.probe_targets()

    assert registry.needs_issuance(7, original, 'http://other:8000')
    assert registry.marker(7, original, 'http://replica:8000') == marker
    assert registry.heartbeat_payload() == payload
    assert registry.probe_targets() == targets
    assert not registry.is_retired(7, original)

    # The postcommit mutation owns irreversible retirement.
    assert not registry.issue(7,
                              original,
                              'http://other:8000',
                              '/ready',
                              None,
                              None,
                              normal_probe_started_at=clock.now)

    recovered = _generation(recovered=True, attempt=5)
    _issue(registry, recovered)
    assert registry.marker(7, recovered, 'http://replica:8000') is not None


def test_snapshot_url_mismatch_retires_same_generation() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation()
    _issue(registry, generation)

    assert registry.marker(7, generation, 'http://other:8000') is None
    assert registry.is_retired(7, generation)
    assert registry.heartbeat_payload()['entries'] == []


def test_closed_marker_and_heartbeat_validation() -> None:
    token = 'a' * 32
    present, marker = route_lease.parse_route_marker({
        constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY:
            constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_VERSION,
        constants.SYSTEM_RECOVERY_ROUTE_REPLICA_ID_KEY: '7',
        constants.SYSTEM_RECOVERY_ROUTE_TOKEN_KEY: token,
    })
    assert present
    assert marker == route_lease.RouteMarker('7', token)
    fenced, fenced_marker = route_lease.parse_route_marker({
        constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
            constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
    })
    assert fenced
    assert fenced_marker is None
    mixed_fence, mixed_marker = route_lease.parse_route_marker({
        constants.SYSTEM_RECOVERY_ROUTE_FENCE_KEY:
            constants.SYSTEM_RECOVERY_ROUTE_FENCE_VERSION,
        constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_KEY:
            constants.SYSTEM_RECOVERY_ROUTE_LEASE_MARKER_VERSION,
        constants.SYSTEM_RECOVERY_ROUTE_REPLICA_ID_KEY: '7',
        constants.SYSTEM_RECOVERY_ROUTE_TOKEN_KEY: token,
    })
    assert mixed_fence
    assert mixed_marker is None
    assert route_lease.validate_heartbeat_payload({
        'version': constants.SYSTEM_RECOVERY_ROUTE_LEASE_PROTOCOL_VERSION,
        'entries': [{
            'replica_id': '7',
            'route_token': token,
            'remaining_seconds': 12.5,
        }]
    }) == {
        marker: 12.5
    }

    invalid_payloads = [
        {
            'version': 1,
            'entries': [],
            'extra': True
        },
        {
            'version': 1,
            'entries': [{}]
        },
        {
            'version': 1,
            'entries': [{
                'replica_id': '07',
                'route_token': token,
                'remaining_seconds': 1,
            }]
        },
        {
            'version': 1,
            'entries': [{
                'replica_id': '7',
                'route_token': token,
                'remaining_seconds': math.inf,
            }]
        },
    ]
    for payload in invalid_payloads:
        with pytest.raises(route_lease.RouteLeaseError):
            route_lease.validate_heartbeat_payload(payload)


def test_prune_removes_old_same_numeric_row_identity() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation(record=2)
    _issue(registry, generation)
    registry.prune({7: '00000000-0000-0000-0000-000000000099'})
    assert registry.marker(7, generation, 'http://replica:8000') is None
    assert registry.heartbeat_payload()['entries'] == []


def test_exact_record_deactivation_does_not_revoke_recreated_row() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation(record=99)
    _issue(registry, generation)

    registry.deactivate_record(7, '00000000-0000-0000-0000-000000000002')
    assert registry.marker(7, generation, 'http://replica:8000') is not None
    registry.deactivate_record(7, '00000000-0000-0000-0000-000000000099')
    assert registry.marker(7, generation, 'http://replica:8000') is None


def test_new_record_observation_retires_old_numeric_id_target() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    old = _generation(record=2)
    _issue(registry, old)

    registry.observe_record_identity(7, '00000000-0000-0000-0000-000000000099')
    assert registry.is_retired(7, old)
    assert registry.heartbeat_payload()['entries'] == []


def test_recreated_record_rejects_stale_old_observation_without_revocation(
) -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    old = _generation(record=2)
    _issue(registry, old)
    registry.deactivate(7, old)
    assert 7 in registry._retired

    new_record_id = '00000000-0000-0000-0000-000000000099'
    registry.observe_record_identity(7, new_record_id)
    # The exact old generation is pruned; one bounded row-identity tombstone
    # prevents delayed insertion callbacks or fleet snapshots from rewinding
    # the live identity.
    assert 7 not in registry._targets
    assert 7 not in registry._retired
    assert registry._retired_record_ids[7] == {old.replica_record_id}
    assert registry.is_retired(7, old)

    recreated = _generation(record=99, attempt=8)
    _issue(registry, recreated)
    recreated_marker = registry.marker(7, recreated, 'http://replica:8000')
    assert recreated_marker is not None

    registry.observe_record_identity(7, old.replica_record_id)
    assert registry.marker(7, recreated,
                           'http://replica:8000') == recreated_marker
    registry.prune({7: old.replica_record_id})
    assert registry.marker(7, recreated,
                           'http://replica:8000') == recreated_marker

    assert registry.needs_issuance(7, old, 'http://replica:8000')
    assert not registry.issue(7,
                              old,
                              'http://replica:8000',
                              '/ready',
                              None,
                              None,
                              normal_probe_started_at=clock.now)
    assert registry.marker(7, recreated,
                           'http://replica:8000') == recreated_marker


def test_invalid_same_record_generation_cannot_revoke_live_generation() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    armed = _generation(attempt=3)
    _issue(registry, armed)
    armed_marker = registry.marker(7, armed, 'http://replica:8000')
    assert armed_marker is not None

    other_armed = _generation(attempt=6)
    assert registry.needs_issuance(7, other_armed, 'http://replica:8000')
    assert not registry.issue(7,
                              other_armed,
                              'http://replica:8000',
                              '/ready',
                              None,
                              None,
                              normal_probe_started_at=clock.now)
    assert registry.marker(7, armed, 'http://replica:8000') == armed_marker

    recovered = _generation(recovered=True, attempt=8)
    _issue(registry, recovered)
    recovered_marker = registry.marker(7, recovered, 'http://replica:8000')
    assert recovered_marker is not None

    invalid_recovered = _generation(recovered=True, attempt=9)
    for stale in (armed, other_armed, invalid_recovered):
        assert registry.needs_issuance(7, stale, 'http://replica:8000')
        assert not registry.issue(7,
                                  stale,
                                  'http://replica:8000',
                                  '/ready',
                                  None,
                                  None,
                                  normal_probe_started_at=clock.now)
        assert registry.is_retired(7, stale)
        assert registry.marker(7, recovered,
                               'http://replica:8000') == recovered_marker


def test_registry_targets_and_tombstones_are_bounded(monkeypatch) -> None:
    cap = 3
    monkeypatch.setattr(constants, 'SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS', cap)
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generations = {}
    for replica_id in range(1, cap + 1):
        generation = _generation(record=replica_id, attempt=100 + replica_id)
        generations[replica_id] = generation
        assert registry.issue(replica_id,
                              generation,
                              f'http://replica-{replica_id}:8000',
                              '/ready',
                              None,
                              None,
                              normal_probe_started_at=clock.now)
        registry.deactivate(replica_id, generation)

    assert len(registry._live_record_ids) == cap
    assert len(registry._targets) == cap
    assert len(registry._retired) == cap
    assert len(registry._retired_record_ids) <= cap
    assert len(registry._blocked_record_ids) <= cap

    overflow = _generation(record=cap + 1, attempt=200)
    assert not registry.issue(cap + 1,
                              overflow,
                              'http://overflow:8000',
                              '/ready',
                              None,
                              None,
                              normal_probe_started_at=clock.now)
    registry.observe_record_identity(cap + 2,
                                     f'00000000-0000-0000-0000-{cap + 2:012d}')
    assert len(registry._live_record_ids) == cap
    assert len(registry._targets) == cap
    assert len(registry._retired) == cap
    assert len(registry._retired_record_ids) <= cap
    assert len(registry._blocked_record_ids) <= cap

    retained = generations[1]
    registry.prune({1: retained.replica_record_id})
    assert set(registry._live_record_ids) == {1}
    assert set(registry._targets) == {1}
    assert set(registry._retired) == {1}
    assert registry.issue(cap + 1,
                          overflow,
                          'http://overflow:8000',
                          '/ready',
                          None,
                          None,
                          normal_probe_started_at=clock.now)
    assert len(registry._live_record_ids) <= cap
    assert len(registry._targets) <= cap
    assert len(registry._retired) <= cap
    assert sum(
        len(record_ids)
        for record_ids in registry._retired_record_ids.values()) <= cap
    assert len(registry._blocked_record_ids) <= cap


def test_max_cardinality_record_advances_do_not_scan_all_tombstones() -> None:
    cap = constants.SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS
    registry = route_lease.ManagerRouteLeaseRegistry(_Clock())
    registry._retired_record_ids = _NoAggregateValuesScanDict()
    current_record_ids = {}

    # Recreate every numeric replica once at the production protocol bound.
    # The mapping guard makes this a structural complexity assertion: the old
    # implementation called values() for each advance and therefore performed
    # 1 + ... + N set visits.
    for replica_id in range(1, cap + 1):
        old_record_id = _generation(record=replica_id).replica_record_id
        current_record_id = _generation(record=cap +
                                        replica_id).replica_record_id
        registry.observe_record_identity(replica_id, old_record_id)
        registry.observe_record_identity(replica_id, current_record_id)
        current_record_ids[replica_id] = current_record_id

    assert registry._retired_record_id_count == cap
    assert sum(
        len(record_ids)
        for record_ids in dict.values(registry._retired_record_ids)) == cap

    # The incrementally maintained count preserves the same fail-closed bound.
    overflow_record_id = _generation(record=2 * cap + 1).replica_record_id
    registry.observe_record_identity(1, overflow_record_id)
    assert registry._blocked_record_ids == {1}
    assert registry._retired_record_id_count == cap

    # Pruning one numeric identity releases exactly its tombstones and permits a
    # later ordered history to consume the freed global slot.
    current_record_ids.pop(1)
    registry.prune(current_record_ids)
    assert registry._retired_record_id_count == cap - 1
    registry.observe_record_identity(1, overflow_record_id)
    final_record_id = _generation(record=3 * cap + 1).replica_record_id
    registry.observe_record_identity(1, final_record_id)
    assert registry._retired_record_id_count == cap
    assert sum(
        len(record_ids)
        for record_ids in dict.values(registry._retired_record_ids)) == cap


def test_repeated_recreation_tombstone_overflow_blocks_only_that_id(
        monkeypatch) -> None:
    cap = 3
    monkeypatch.setattr(constants, 'SYSTEM_RECOVERY_ROUTE_MAX_REPLICAS', cap)
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)

    def issue(replica_id: int, generation: route_lease.RouteGeneration) -> None:
        assert registry.issue(replica_id,
                              generation,
                              f'http://replica-{replica_id}:8000',
                              '/ready',
                              None,
                              None,
                              normal_probe_started_at=clock.now)

    current = _generation(record=10, attempt=110)
    stable = _generation(record=50, attempt=150)
    issue(1, current)
    issue(2, stable)
    stable_marker = registry.marker(2, stable, 'http://replica-2:8000')
    assert stable_marker is not None

    old_generations = [current]
    for record_id in (11, 12, 13):
        candidate = _generation(record=record_id, attempt=100 + record_id)
        registry.observe_record_identity(1, candidate.replica_record_id)
        issue(1, candidate)
        old_generations.append(candidate)
        current = candidate
    current_marker = registry.marker(1, current, 'http://replica-1:8000')
    assert current_marker is not None
    assert sum(
        len(record_ids)
        for record_ids in registry._retired_record_ids.values()) == cap

    # Even the oldest delayed exact observation cannot regress a repeatedly
    # recreated numeric ID or revoke its newest target.
    oldest = old_generations[0]
    registry.observe_record_identity(1, oldest.replica_record_id)
    registry.prune({
        1: oldest.replica_record_id,
        2: stable.replica_record_id,
    })
    assert registry.marker(1, current,
                           'http://replica-1:8000') == current_marker
    assert registry.marker(2, stable, 'http://replica-2:8000') == stable_marker

    overflow = _generation(record=14, attempt=114)
    registry.observe_record_identity(1, overflow.replica_record_id)
    assert registry._blocked_record_ids == {1}
    assert registry.marker(1, current, 'http://replica-1:8000') is None
    assert not registry.issue(1,
                              overflow,
                              'http://replica-1:8000',
                              '/ready',
                              None,
                              None,
                              normal_probe_started_at=clock.now)
    assert registry.is_retired(1, overflow)
    assert registry.marker(2, stable, 'http://replica-2:8000') == stable_marker

    # A later exact snapshot cannot bypass the poison without first pruning
    # this numeric ID entirely; every global collection stays under the cap.
    registry.prune({
        1: overflow.replica_record_id,
        2: stable.replica_record_id,
    })
    assert registry._blocked_record_ids == {1}
    assert len(registry._live_record_ids) <= cap
    assert len(registry._targets) <= cap
    assert len(registry._retired) <= cap
    assert len(registry._retired_record_ids) <= cap
    assert sum(
        len(record_ids)
        for record_ids in registry._retired_record_ids.values()) <= cap
    assert len(registry._blocked_record_ids) <= cap
    assert registry.marker(2, stable, 'http://replica-2:8000') == stable_marker


def test_route_suspension_is_reversible_only_while_exact_token_is_live(
) -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation()
    _issue(registry, generation)
    target = registry.probe_targets()[0]
    registry.record_probe_result(target,
                                 request_started_at=clock.now,
                                 succeeded=True)
    marker = registry.marker(7, generation, target.route_url)
    assert marker is not None

    suspension = registry.suspend_record(
        7, '00000000-0000-0000-0000-000000000002')
    assert suspension is not None
    assert registry.marker(7, generation, target.route_url) is None
    assert registry.heartbeat_payload()['entries'] == []
    assert registry.probe_targets() == []
    registry.record_probe_result(target,
                                 request_started_at=clock.now + 1,
                                 succeeded=True)

    registry.rollback_suspension(suspension)
    assert registry.marker(7, generation, target.route_url) == marker

    suspension = registry.suspend_record(
        7, '00000000-0000-0000-0000-000000000002')
    assert suspension is not None
    registry.commit_suspension(suspension)
    assert registry.is_retired(7, generation)
    assert registry.marker(7, generation, target.route_url) is None


def test_rollback_rejects_late_pre_suspension_probe_without_renewal() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation()
    _issue(registry, generation)
    old_target = registry.probe_targets()[0]

    clock.now = 5
    registry.record_probe_result(old_target,
                                 request_started_at=clock.now,
                                 succeeded=True)
    clock.now = 10
    registry.record_probe_result(old_target,
                                 request_started_at=clock.now,
                                 succeeded=False)
    marker_before = registry.marker(7, generation, old_target.route_url)
    state_before = registry._targets[7]
    exact_state_before = (state_before.deadline, state_before.active,
                          state_before.activated,
                          state_before.consecutive_failures)
    assert marker_before is not None
    assert exact_state_before == (65, True, True, 1)

    suspension = registry.suspend_record(
        7, '00000000-0000-0000-0000-000000000002')
    assert suspension is not None
    assert suspension.probe_epoch == old_target.probe_epoch + 1
    registry.rollback_suspension(suspension)

    # This completion was captured before the hold.  Were it accepted after
    # rollback, it would extend the deadline to 71 and clear the prior miss.
    clock.now = 11
    registry.record_probe_result(old_target,
                                 request_started_at=clock.now,
                                 succeeded=True)
    state_after = registry._targets[7]
    assert registry.marker(7, generation, old_target.route_url) == marker_before
    assert (state_after.deadline, state_after.active, state_after.activated,
            state_after.consecutive_failures) == exact_state_before
    replacement_target = registry.probe_targets()[0]
    assert replacement_target.probe_epoch == suspension.probe_epoch
    assert registry.heartbeat_payload(
    )['entries'][0]['remaining_seconds'] == pytest.approx(54)


def test_route_suspension_rollback_cannot_restore_expired_token() -> None:
    clock = _Clock()
    registry = route_lease.ManagerRouteLeaseRegistry(clock)
    generation = _generation()
    _issue(registry, generation)
    suspension = registry.suspend_record(
        7, '00000000-0000-0000-0000-000000000002')
    assert suspension is not None

    clock.now = constants.SYSTEM_RECOVERY_ROUTE_LEASE_SECONDS
    registry.rollback_suspension(suspension)
    assert registry.is_retired(7, generation)
    assert registry.heartbeat_payload()['entries'] == []
