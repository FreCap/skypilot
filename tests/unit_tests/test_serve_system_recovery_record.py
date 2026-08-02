"""ReplicaInfo system-recovery storage compatibility contract tests."""

import copy
import logging
import pickle
import uuid

import pytest

from sky.serve import replica_info
from sky.serve import serve_state
from sky.serve import system_recovery_state as recovery_state
from sky.utils import common_utils

_ORIGINAL_ID = '11111111-1111-4111-8111-111111111111'
_REPLACEMENT_ID = '22222222-2222-4222-8222-222222222222'
_EVENT_ID = '33333333-3333-4333-8333-333333333333'


def _replica() -> replica_info.ReplicaInfo:
    info = replica_info.ReplicaInfo(replica_id=7,
                                    cluster_name='svc-7',
                                    replica_port='8080',
                                    is_spot=False,
                                    location=None,
                                    version=3,
                                    resources_override=None)
    info.status_property.sky_launch_status = common_utils.ProcessStatus.SUCCEEDED
    info.status_property.first_ready_time = 100.0
    info.status_property.service_ready_now = True
    return info


def _intent() -> recovery_state.SystemRecoveryLaunchIntent:
    digest = 'a' * 64
    return recovery_state.SystemRecoveryLaunchIntent(
        version=1,
        controller_contract_version=2,
        recovery_authorization_version=3,
        recovery_authorization_profile_id='boltz-l4-v3',
        recovery_authorization_sha256=digest,
        runtime_profile_version=2,
        expected_runtime_capability=(recovery_state.SYSTEM_RECOVERY_CAPABILITY),
        service_hash='service-hash',
        replica_id=7,
        launch_generation=7,
        launch_nonce='b' * 64,
        workspace='default',
        resource_envelope_sha256=digest,
        task_sha256=digest,
        runtime_image_digest=f'sha256:{digest}',
        owned_container_spec_sha256=digest,
        execution_envelope_sha256=digest)


def _retry_state() -> recovery_state.ReplicaSystemRecovery:
    observation = recovery_state.RecoveryObservation(
        job_id=9,
        capability=recovery_state.SYSTEM_RECOVERY_CAPABILITY,
        phase=recovery_state.RemoteRecoveryPhase.RETRY_SUBMITTED,
        original_attempt_id=_ORIGINAL_ID,
        replacement_attempt_id=_REPLACEMENT_ID,
        node_boot_id='boot-id',
        occurrence_count=1,
        armed_at=10.0,
        updated_at=20.0,
        event_id=_EVENT_ID,
        reason='RAY_NODE_OOM',
        occurred_at=15.0,
        deadline_at=135.0)
    reduced = recovery_state.reduce_remote_observation(
        None, observation, now=20.0, controller_grace_seconds=300.0)
    assert reduced.state is not None
    return reduced.state


def test_current_capable_record_round_trip_is_lossless() -> None:
    info = _replica()
    info.system_recovery_launch_intent = _intent()
    info.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.CAPABLE)
    info.launch_request_id = 'request-1'
    info.service_job_id = 9
    info.candidate_ready_observed_at = 100.0
    info.ordinary_release_not_before = 135.0
    info.system_recovery_revision = 4
    info.system_recovery = _retry_state()

    state = info.to_storage_dict()
    restored = replica_info.ReplicaInfo.from_storage_dict(state)

    assert state['replica_info_version'] == 14
    assert restored.to_storage_dict() == state
    assert restored.system_recovery_launch_intent == _intent()
    assert restored.system_recovery_revision == 4
    assert restored.system_recovery == _retry_state()
    assert not restored.is_ready
    assert pickle.loads(pickle.dumps(info)).to_storage_dict() == state


def test_v13_exact_additive_field_list_and_random_record_identity() -> None:
    assert replica_info.SYSTEM_RECOVERY_STORAGE_FIELDS == (
        'system_recovery_launch_intent',
        'system_recovery_disposition',
        'launch_request_id',
        'service_job_id',
        'candidate_ready_observed_at',
        'ordinary_release_not_before',
        'system_recovery_revision',
        'system_recovery',
        'system_recovery_quarantine',
    )
    assert replica_info.V13_ADDITIVE_STORAGE_FIELDS == (
        'replica_record_id',
        *replica_info.SYSTEM_RECOVERY_STORAGE_FIELDS,
    )
    first = _replica()
    second = _replica()

    assert str(uuid.UUID(first.replica_record_id)) == first.replica_record_id
    assert str(uuid.UUID(second.replica_record_id)) == second.replica_record_id
    assert uuid.UUID(first.replica_record_id).version == 4
    assert uuid.UUID(second.replica_record_id).version == 4
    assert first.replica_record_id != second.replica_record_id


def test_v12_and_older_rows_default_to_ordinary() -> None:
    state = _replica().to_storage_dict()
    state['replica_id'] = 1
    state['cluster_name'] = 'svc-1'
    state['created_at'] = 123.5
    state['replica_info_version'] = 12
    untrusted_record_id = state['replica_record_id']
    state['system_recovery_disposition'] = 'CAPABLE'
    state['system_recovery_revision'] = 'untrusted-old-value'

    restored = replica_info.ReplicaInfo.from_storage_dict(state)
    replay_state = copy.deepcopy(state)
    replay_state['replica_record_id'] = str(uuid.uuid4())
    replay = replica_info.ReplicaInfo.from_storage_dict(replay_state)

    assert restored.system_recovery_disposition == (
        recovery_state.SystemRecoveryDisposition.ORDINARY)
    assert restored.system_recovery_revision == 0
    assert restored.system_recovery is None
    assert restored.system_recovery_quarantine is None
    assert restored.replica_record_id == replay.replica_record_id
    assert restored.replica_record_id != untrusted_record_id
    assert (
        restored.replica_record_id == '5b71cc7f-a36e-5c16-a0c7-de59389ead0e')
    legacy_state = copy.deepcopy(state)
    legacy_state['created_at'] = None
    assert (replica_info.ReplicaInfo.from_storage_dict(legacy_state).
            replica_record_id == '6f7d7c8f-8eac-5728-a487-b46516e74ba7')
    rewritten = restored.to_storage_dict()
    assert rewritten['replica_info_version'] == 14
    assert rewritten['replica_record_id'] == restored.replica_record_id


def test_v12_pickle_and_json_derive_the_same_transition_identity() -> None:
    source = _replica()
    source.created_at = 123.5
    json_state = source.to_storage_dict()
    json_state['replica_info_version'] = 12
    for field in replica_info.V13_ADDITIVE_STORAGE_FIELDS:
        json_state.pop(field, None)
    from_json = replica_info.ReplicaInfo.from_storage_dict(json_state)

    pickle_state = copy.deepcopy(source.__dict__)
    pickle_state['_version'] = 12
    for field in replica_info.V13_ADDITIVE_STORAGE_FIELDS:
        pickle_state.pop(field, None)
    from_pickle = replica_info.ReplicaInfo.__new__(replica_info.ReplicaInfo)
    from_pickle.__setstate__(pickle_state)

    assert from_pickle.replica_record_id == from_json.replica_record_id
    assert uuid.UUID(from_pickle.replica_record_id).version == 5
    assert from_pickle.to_storage_dict()['replica_info_version'] == 14
    assert (pickle.loads(pickle.dumps(from_pickle)).replica_record_id ==
            from_json.replica_record_id)


def test_all_fields_absent_v13_quarantines_json_pickle_and_memory() -> None:
    source = _replica()
    source.created_at = 123.5
    missing = source.to_storage_dict()
    missing['replica_info_version'] = 13
    for field in replica_info.V13_ADDITIVE_STORAGE_FIELDS:
        missing.pop(field)

    from_json = replica_info.ReplicaInfo.from_storage_dict(missing)
    pickle_state = copy.deepcopy(source.__dict__)
    pickle_state['_version'] = 13
    for field in replica_info.V13_ADDITIVE_STORAGE_FIELDS:
        pickle_state.pop(field)
    from_pickle = replica_info.ReplicaInfo.__new__(replica_info.ReplicaInfo)
    from_pickle.__setstate__(pickle_state)

    in_memory = _replica()
    for field in replica_info.V13_ADDITIVE_STORAGE_FIELDS:
        delattr(in_memory, field)
    serialized = in_memory.to_storage_dict()

    expected = recovery_state.SystemRecoveryQuarantine(
        recovery_state.RecoveryQuarantineReason.PARTIAL_V13_BUNDLE)
    for restored in (from_json, from_pickle, in_memory):
        assert restored.system_recovery_quarantine == expected
        assert not restored.is_ready
        assert set(replica_info.V13_ADDITIVE_STORAGE_FIELDS).issubset(
            restored.to_storage_dict())
    for restored in (from_json, from_pickle):
        assert restored.status == serve_state.ReplicaStatus.FAILED_CLEANUP
    assert in_memory.status == serve_state.ReplicaStatus.NOT_READY
    assert from_pickle.replica_record_id == from_json.replica_record_id
    assert serialized['system_recovery_quarantine'] == expected.to_dict()

    v12 = copy.deepcopy(missing)
    v12['replica_info_version'] = 12
    v12_restored = replica_info.ReplicaInfo.from_storage_dict(v12)
    assert v12_restored.system_recovery_quarantine is None
    assert v12_restored.system_recovery_disposition == (
        recovery_state.SystemRecoveryDisposition.ORDINARY)
    assert v12_restored.replica_record_id == from_json.replica_record_id

def test_future_replica_info_versions_are_quarantined() -> None:
    missing = _replica().to_storage_dict()
    for field in replica_info.V13_ADDITIVE_STORAGE_FIELDS:
        missing.pop(field)
    future = copy.deepcopy(missing)
    future['replica_info_version'] = 15
    future_restored = replica_info.ReplicaInfo.from_storage_dict(future)
    assert future_restored.system_recovery_quarantine == (
        recovery_state.SystemRecoveryQuarantine(
            recovery_state.RecoveryQuarantineReason.INCONSISTENT_V13_BUNDLE))

    future_complete = _replica().to_storage_dict()
    future_complete['replica_info_version'] = 15
    assert (
        replica_info.ReplicaInfo.from_storage_dict(future_complete).
        system_recovery_quarantine == recovery_state.SystemRecoveryQuarantine(
            recovery_state.RecoveryQuarantineReason.INCONSISTENT_V13_BUNDLE))

    future_pickle = copy.deepcopy(vars(_replica()))
    future_pickle['_version'] = 15
    for field in replica_info.V13_ADDITIVE_STORAGE_FIELDS:
        future_pickle.pop(field)
    future_pickle_restored = replica_info.ReplicaInfo.__new__(
        replica_info.ReplicaInfo)
    future_pickle_restored.__setstate__(future_pickle)
    assert future_pickle_restored.system_recovery_quarantine == (
        recovery_state.SystemRecoveryQuarantine(
            recovery_state.RecoveryQuarantineReason.INCONSISTENT_V13_BUNDLE))


def test_partial_v13_is_reason_only_quarantine_and_does_not_abort_read(
        caplog) -> None:
    partial = _replica().to_storage_dict()
    partial['replica_info_version'] = 13
    partial.pop('service_job_id')
    partial['system_recovery_launch_intent'] = {'raw-secret': 'do-not-log'}

    with caplog.at_level(logging.WARNING):
        restored = replica_info.ReplicaInfo.from_storage_dict(partial)

    assert restored.system_recovery_quarantine == (
        recovery_state.SystemRecoveryQuarantine(
            recovery_state.RecoveryQuarantineReason.PARTIAL_V13_BUNDLE))
    assert not restored.is_ready
    assert restored.status == serve_state.ReplicaStatus.FAILED_CLEANUP
    assert 'PARTIAL_V13_BUNDLE' in caplog.text
    assert 'raw-secret' not in caplog.text
    assert 'do-not-log' not in caplog.text

    # A quarantined record itself is a complete, reason-only v13 shape.
    round_trip = replica_info.ReplicaInfo.from_storage_dict(
        restored.to_storage_dict())
    assert round_trip.system_recovery_quarantine == (
        restored.system_recovery_quarantine)


def test_partial_in_memory_v13_bundle_is_not_silently_demoted() -> None:
    info = _replica()
    info._version = 13  # pylint: disable=protected-access
    del info.service_job_id

    state = info.to_storage_dict()

    assert state['system_recovery_quarantine'] == {
        'reason':
            recovery_state.RecoveryQuarantineReason.PARTIAL_V13_BUNDLE.value
    }
    assert not info.is_ready


def test_v13_missing_only_record_id_is_partial_and_quarantined() -> None:
    partial = _replica().to_storage_dict()
    partial['replica_info_version'] = 13
    partial.pop('replica_record_id')

    restored = replica_info.ReplicaInfo.from_storage_dict(partial)

    assert restored.system_recovery_quarantine == (
        recovery_state.SystemRecoveryQuarantine(
            recovery_state.RecoveryQuarantineReason.PARTIAL_V13_BUNDLE))
    assert str(uuid.UUID(
        restored.replica_record_id)) == restored.replica_record_id
    assert 'replica_record_id' in restored.to_storage_dict()


def test_malformed_and_inconsistent_complete_bundles_quarantine() -> None:
    malformed = _replica().to_storage_dict()
    malformed['system_recovery_revision'] = True
    malformed_restored = replica_info.ReplicaInfo.from_storage_dict(malformed)
    assert malformed_restored.system_recovery_quarantine == (
        recovery_state.SystemRecoveryQuarantine(
            recovery_state.RecoveryQuarantineReason.INCONSISTENT_V13_BUNDLE))

    inconsistent = _replica().to_storage_dict()
    inconsistent['system_recovery_disposition'] = 'CAPABLE'
    inconsistent_restored = replica_info.ReplicaInfo.from_storage_dict(
        inconsistent)
    assert inconsistent_restored.system_recovery_quarantine == (
        recovery_state.SystemRecoveryQuarantine(
            recovery_state.RecoveryQuarantineReason.INCONSISTENT_V13_BUNDLE))

    oversized = _replica().to_storage_dict()
    oversized['candidate_ready_observed_at'] = 10**10000
    oversized['ordinary_release_not_before'] = 10**10000
    oversized_restored = replica_info.ReplicaInfo.from_storage_dict(oversized)
    assert oversized_restored.system_recovery_quarantine == (
        recovery_state.SystemRecoveryQuarantine(
            recovery_state.RecoveryQuarantineReason.INCONSISTENT_V13_BUNDLE))

    malformed_identity = _replica().to_storage_dict()
    malformed_identity['replica_record_id'] = 'NOT-A-CANONICAL-UUID'
    identity_restored = replica_info.ReplicaInfo.from_storage_dict(
        malformed_identity)
    assert identity_restored.system_recovery_quarantine == (
        recovery_state.SystemRecoveryQuarantine(
            recovery_state.RecoveryQuarantineReason.INCONSISTENT_V13_BUNDLE))
    assert (str(uuid.UUID(identity_restored.replica_record_id)) ==
            identity_restored.replica_record_id)


def test_recovery_field_copy_preserves_latest_and_increments_exactly_once(
) -> None:
    latest = _replica()
    latest.system_recovery_revision = 3
    stale_whole_row = copy.deepcopy(latest)
    stale_whole_row.system_recovery_revision = 99

    replica_info.copy_system_recovery_fields(latest, stale_whole_row)
    assert stale_whole_row.system_recovery_revision == 3

    desired = copy.deepcopy(latest)
    desired.system_recovery_launch_intent = _intent()
    desired.system_recovery_disposition = (
        recovery_state.SystemRecoveryDisposition.CANDIDATE)
    replica_info.copy_system_recovery_fields(desired,
                                             latest,
                                             increment_revision=True)
    assert latest.system_recovery_revision == 4
    assert latest.system_recovery_disposition == (
        recovery_state.SystemRecoveryDisposition.CANDIDATE)

    stale_desired = copy.deepcopy(desired)
    stale_desired.system_recovery_revision = 3
    try:
        replica_info.copy_system_recovery_fields(stale_desired,
                                                 latest,
                                                 increment_revision=True)
    except recovery_state.RecoveryStateError:
        pass
    else:
        raise AssertionError('stale recovery transition was accepted')

    crossed_record = copy.deepcopy(latest)
    crossed_record.replica_record_id = str(uuid.uuid4())
    for increment_revision in (False, True):
        with pytest.raises(recovery_state.RecoveryStateError,
                           match='record identity'):
            replica_info.copy_system_recovery_fields(
                crossed_record, latest, increment_revision=increment_revision)
