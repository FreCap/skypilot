"""Tests for the pure SkyServe system-recovery state machine."""

import dataclasses
from typing import Any

import pytest

from sky.serve import system_recovery_state as recovery_state

_ORIGINAL_ID = '11111111-1111-4111-8111-111111111111'
_REPLACEMENT_ID = '22222222-2222-4222-8222-222222222222'
_EVENT_ID = '33333333-3333-4333-8333-333333333333'
_CAPABILITY = recovery_state.SYSTEM_RECOVERY_CAPABILITY


def _intent(replica_id: int = 7) -> recovery_state.SystemRecoveryLaunchIntent:
    digest = 'a' * 64
    return recovery_state.SystemRecoveryLaunchIntent(
        version=1,
        controller_contract_version=2,
        recovery_authorization_version=3,
        recovery_authorization_profile_id='boltz-l4-v3',
        recovery_authorization_sha256=digest,
        runtime_profile_version=2,
        expected_runtime_capability=_CAPABILITY,
        service_hash='service-hash',
        replica_id=replica_id,
        launch_generation=replica_id,
        launch_nonce='b' * 64,
        workspace='default',
        resource_envelope_sha256=digest,
        task_sha256=digest,
        runtime_image_digest=f'sha256:{digest}',
        owned_container_spec_sha256=digest,
        execution_envelope_sha256=digest)


def _observation(
    phase: recovery_state.RemoteRecoveryPhase,
    *,
    occurrence_count: int = 1,
) -> recovery_state.RecoveryObservation:
    if phase == recovery_state.RemoteRecoveryPhase.ARMED:
        return recovery_state.RecoveryObservation(
            job_id=9,
            capability=_CAPABILITY,
            phase=phase,
            original_attempt_id=_ORIGINAL_ID,
            replacement_attempt_id=None,
            node_boot_id='boot-id',
            occurrence_count=0,
            armed_at=10.0,
            updated_at=11.0)
    replacement_attempt_id = None
    if phase in (recovery_state.RemoteRecoveryPhase.RESUBMITTING,
                 recovery_state.RemoteRecoveryPhase.RETRY_SUBMITTED,
                 recovery_state.RemoteRecoveryPhase.EXHAUSTED):
        replacement_attempt_id = _REPLACEMENT_ID
    return recovery_state.RecoveryObservation(
        job_id=9,
        capability=_CAPABILITY,
        phase=phase,
        original_attempt_id=_ORIGINAL_ID,
        replacement_attempt_id=replacement_attempt_id,
        node_boot_id='boot-id',
        occurrence_count=occurrence_count,
        armed_at=10.0,
        updated_at=20.0,
        event_id=_EVENT_ID,
        reason='RAY_NODE_OOM',
        occurred_at=15.0,
        deadline_at=135.0)


def _reduce(
    current: recovery_state.ReplicaSystemRecovery | None,
    observation: recovery_state.RecoveryObservation | None,
    **kwargs,
) -> recovery_state.RecoveryReduction:
    return recovery_state.reduce_remote_observation(
        current,
        observation,
        now=kwargs.pop('now', 20.0),
        controller_grace_seconds=kwargs.pop('controller_grace_seconds', 300.0),
        **kwargs)


def test_launch_intent_is_closed_and_strictly_versioned() -> None:
    intent = _intent()
    assert recovery_state.SystemRecoveryLaunchIntent.from_dict(
        intent.to_dict()) == intent
    assert dataclasses.replace(intent, workspace='')
    with pytest.raises(recovery_state.RecoveryStateError):
        dataclasses.replace(intent, recovery_authorization_profile_id='')

    invalid_updates: tuple[dict[str, Any], ...] = ({
        'controller_contract_version': 1
    }, {
        'recovery_authorization_version': 2
    }, {
        'runtime_profile_version': 3
    }, {
        'launch_nonce': 'A' * 64
    }, {
        'runtime_image_digest': 'a' * 64
    })
    for update in invalid_updates:
        with pytest.raises(recovery_state.RecoveryStateError):
            dataclasses.replace(intent, **update)

    payload = intent.to_dict()
    payload['unexpected'] = True
    with pytest.raises(recovery_state.RecoveryStateError,
                       match='invalid fields'):
        recovery_state.SystemRecoveryLaunchIntent.from_dict(payload)


def test_removed_runtime_v1_capability_is_rejected() -> None:
    with pytest.raises(recovery_state.RecoveryStateError,
                       match='Unknown recovery capability'):
        dataclasses.replace(
            _observation(recovery_state.RemoteRecoveryPhase.ARMED),
            capability='subreaper-v1+local-docker-empty-inventory-v1')


@pytest.mark.parametrize(
    ('phase', 'controller_state', 'off_route', 'teardown'), [
        (recovery_state.RemoteRecoveryPhase.ARMED,
         recovery_state.ControllerRecoveryState.ARMED, False, False),
        (recovery_state.RemoteRecoveryPhase.WAITING_CLEANUP,
         recovery_state.ControllerRecoveryState.RECOVERING, True, False),
        (recovery_state.RemoteRecoveryPhase.WAITING_MEMORY,
         recovery_state.ControllerRecoveryState.RECOVERING, True, False),
        (recovery_state.RemoteRecoveryPhase.RESUBMITTING,
         recovery_state.ControllerRecoveryState.RECOVERING, True, False),
        (recovery_state.RemoteRecoveryPhase.RETRY_SUBMITTED,
         recovery_state.ControllerRecoveryState.RETRY_SUBMITTED, True, False),
        (recovery_state.RemoteRecoveryPhase.EXHAUSTED,
         recovery_state.ControllerRecoveryState.EXHAUSTED, True, True),
    ])
def test_every_valid_first_phase_is_adopted(
        phase: recovery_state.RemoteRecoveryPhase,
        controller_state: recovery_state.ControllerRecoveryState,
        off_route: bool, teardown: bool) -> None:
    reduction = _reduce(None, _observation(phase))

    assert reduction.state is not None
    assert reduction.state.state == controller_state
    assert reduction.force_off_route is off_route
    assert reduction.schedule_legacy_teardown is teardown
    assert recovery_state.ReplicaSystemRecovery.from_dict(
        reduction.state.to_dict()) == reduction.state


def test_eventless_exhausted_is_a_valid_first_remote_phase() -> None:
    observation = recovery_state.RecoveryObservation(
        job_id=9,
        capability=_CAPABILITY,
        phase=recovery_state.RemoteRecoveryPhase.EXHAUSTED,
        original_attempt_id=_ORIGINAL_ID,
        replacement_attempt_id=None,
        node_boot_id='boot-id',
        occurrence_count=0,
        armed_at=10.0,
        updated_at=20.0)

    reduction = _reduce(None, observation)

    assert reduction.state is not None
    assert reduction.state.state == (
        recovery_state.ControllerRecoveryState.EXHAUSTED)
    assert reduction.schedule_legacy_teardown
    assert recovery_state.ReplicaSystemRecovery.from_dict(
        reduction.state.to_dict()) == reduction.state


def test_terminal_teardown_and_quarantine_are_absorbing() -> None:
    exhausted = _reduce(
        None, _observation(recovery_state.RemoteRecoveryPhase.EXHAUSTED)).state
    assert exhausted is not None
    retry = _observation(recovery_state.RemoteRecoveryPhase.RETRY_SUBMITTED)

    terminal = _reduce(exhausted, retry, now=30.0)
    assert terminal.state == exhausted
    assert terminal.schedule_legacy_teardown

    quarantine = _reduce(None, retry, quarantined=True)
    assert quarantine.state is None
    assert quarantine.force_off_route
    assert quarantine.schedule_legacy_teardown

    teardown = _reduce(None, retry, teardown_intent=True)
    assert teardown.state is not None
    assert teardown.state.state == recovery_state.ControllerRecoveryState.EXHAUSTED
    assert teardown.schedule_legacy_teardown


def test_only_fresh_post_adoption_probe_marks_recovered_ready() -> None:
    retry = _reduce(None,
                    _observation(
                        recovery_state.RemoteRecoveryPhase.RETRY_SUBMITTED),
                    now=30.0).state
    assert retry is not None

    stale = recovery_state.reduce_probe_result(retry,
                                               succeeded=True,
                                               probe_started_at=30.0,
                                               now=31.0,
                                               was_ready=True,
                                               detection_window_seconds=35.0)
    assert stale.force_off_route
    assert not stale.mark_ready

    fresh = recovery_state.reduce_probe_result(retry,
                                               succeeded=True,
                                               probe_started_at=30.1,
                                               now=31.0,
                                               was_ready=True,
                                               detection_window_seconds=35.0)
    assert fresh.state is not None
    assert fresh.state.state == recovery_state.ControllerRecoveryState.RECOVERED
    assert fresh.mark_ready
    assert fresh.clear_probe_failure_window
    assert not fresh.force_off_route


def test_candidate_release_requires_wall_monotonic_and_same_cycle_freshness(
) -> None:
    first = recovery_state.reduce_candidate_readiness(
        recovery_state.SystemRecoveryDisposition.CANDIDATE,
        None,
        None,
        succeeded=True,
        probe_started_at=90.0,
        now=100.0,
        monotonic_guard_satisfied=False,
        exact_job_nonterminal=True,
        exact_detail_absent=True)
    assert first.record_application_readiness
    assert first.force_off_route
    assert first.candidate_ready_observed_at == 100.0
    assert first.ordinary_release_not_before == 135.0

    for overrides in (
        {
            'probe_started_at': 135.0
        },
        {
            'now': 134.9
        },
        {
            'monotonic_guard_satisfied': False
        },
        {
            'exact_job_nonterminal': False
        },
        {
            'exact_detail_absent': False
        },
    ):
        args: dict[str, Any] = {
            'succeeded': True,
            'probe_started_at': 135.1,
            'now': 136.0,
            'monotonic_guard_satisfied': True,
            'exact_job_nonterminal': True,
            'exact_detail_absent': True,
        }
        args.update(overrides)
        held = recovery_state.reduce_candidate_readiness(
            first.disposition, first.candidate_ready_observed_at,
            first.ordinary_release_not_before, **args)
        assert held.disposition == (
            recovery_state.SystemRecoveryDisposition.CANDIDATE)
        assert held.force_off_route

    released = recovery_state.reduce_candidate_readiness(
        first.disposition,
        first.candidate_ready_observed_at,
        first.ordinary_release_not_before,
        succeeded=True,
        probe_started_at=135.1,
        now=136.0,
        monotonic_guard_satisfied=True,
        exact_job_nonterminal=True,
        exact_detail_absent=True)
    assert released.disposition == (
        recovery_state.SystemRecoveryDisposition.ORDINARY)
    assert released.mark_ready
    assert not released.force_off_route


def test_revision_is_nonnegative_and_strictly_increasing() -> None:
    assert recovery_state.next_recovery_revision(0) == 1
    assert recovery_state.next_recovery_revision(11) == 12
    for invalid in (True, -1, 1.0, '1'):
        with pytest.raises(recovery_state.RecoveryStateError):
            recovery_state.next_recovery_revision(invalid)


def test_oversized_durations_fail_as_typed_validation_errors() -> None:
    with pytest.raises(recovery_state.RecoveryStateError):
        _reduce(None,
                _observation(recovery_state.RemoteRecoveryPhase.ARMED),
                controller_grace_seconds=10**10000)
