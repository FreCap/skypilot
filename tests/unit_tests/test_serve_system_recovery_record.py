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

    assert state['replica_info_version'] == 18
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


@pytest.mark.parametrize('version', [-1, 0, 15, 16, 19])
def test_runtime_json_decoder_rejects_unsupported_version(version: int) -> None:
    state = _replica().to_storage_dict()
    state['replica_info_version'] = version

    with pytest.raises(ValueError,
                       match='Unsupported ReplicaInfo storage version'):
        replica_info.ReplicaInfo.from_storage_dict(state)


@pytest.mark.parametrize('version', [12, 13])
def test_runtime_json_decoder_rejects_supported_version_with_wrong_shape(
        version: int) -> None:
    state = _replica().to_storage_dict()
    state['replica_info_version'] = version

    with pytest.raises(ValueError, match='invalid top-level shape'):
        replica_info.ReplicaInfo.from_storage_dict(state)


def test_rejected_legacy_json_never_logs_payload(caplog) -> None:
    partial = _replica().to_storage_dict()
    partial['replica_info_version'] = 13
    partial.pop('service_job_id')
    partial['system_recovery_launch_intent'] = {'raw-secret': 'do-not-log'}

    with caplog.at_level(logging.WARNING):
        with pytest.raises(ValueError, match='invalid top-level shape'):
            replica_info.ReplicaInfo.from_storage_dict(partial)

    assert 'raw-secret' not in caplog.text
    assert 'do-not-log' not in caplog.text


def test_pre_v17_in_memory_record_is_not_writable() -> None:
    info = _replica()
    info._version = 13  # pylint: disable=protected-access
    with pytest.raises(ValueError, match='v17 collision records'):
        info.to_storage_dict()


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


@pytest.mark.parametrize('case', ['malformed', 'inconsistent'])
def test_pure_decoder_never_logs_quarantined_row_identity_or_payload(
        caplog, case: str) -> None:
    sentinel_id = 987654321
    sentinel_payload = 'pure-decoder-recovery-payload-sentinel'
    state = _replica().to_storage_dict()
    state['replica_id'] = sentinel_id
    if case == 'malformed':
        state['system_recovery_launch_intent'] = {
            'raw-secret': sentinel_payload,
        }
    else:
        state['system_recovery_disposition'] = 'CAPABLE'
        state['launch_request_id'] = sentinel_payload

    with caplog.at_level(logging.WARNING):
        restored = replica_info.ReplicaInfo.from_storage_dict(state)

    assert restored.system_recovery_quarantine is not None
    assert str(sentinel_id) not in caplog.text
    assert sentinel_payload not in caplog.text


def test_runtime_row_decoder_owns_quarantine_warning(caplog) -> None:
    sentinel_id = 987654321
    sentinel_payload = 'runtime-recovery-payload-sentinel'
    state = _replica().to_storage_dict()
    state['replica_id'] = sentinel_id
    state['system_recovery_disposition'] = 'CAPABLE'
    state['launch_request_id'] = sentinel_payload

    with caplog.at_level(logging.WARNING):
        restored = serve_state.decode_replica_state_for_authority(1, state)

    assert restored.system_recovery_quarantine is not None
    assert f'replica {sentinel_id} (' in caplog.text
    assert sentinel_payload not in caplog.text


def test_runtime_row_decoder_logs_identifier_safe_unknown_status(
        caplog) -> None:
    sentinel_id = 987654321
    sentinel_payload = 'runtime-unknown-status-payload-sentinel'
    state = _replica().to_storage_dict()
    state['replica_id'] = sentinel_id
    state['cluster_name'] = sentinel_payload
    status = state['status_property']
    assert isinstance(status, dict)
    status['sky_launch_status'] = common_utils.ProcessStatus.RUNNING.value
    status['sky_down_status'] = common_utils.ProcessStatus.SUCCEEDED.value

    with caplog.at_level(logging.ERROR):
        restored = serve_state.decode_replica_state_for_authority(1, state)

    assert restored.status is serve_state.ReplicaStatus.UNKNOWN
    assert 'projected UNKNOWN status' in caplog.text
    assert str(sentinel_id) not in caplog.text
    assert sentinel_payload not in caplog.text


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
