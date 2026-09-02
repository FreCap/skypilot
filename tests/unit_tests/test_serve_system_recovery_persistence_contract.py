"""Pure tests for typed system-recovery persistence values."""

import dataclasses
from types import SimpleNamespace
import uuid

import pytest

from sky.serve import system_recovery_persistence
from sky.serve import system_recovery_state


def _write(**overrides):
    replica_id = overrides.pop('replica_id', 7)
    record_id = overrides.pop('replica_record_id', str(uuid.uuid4()))
    version = overrides.pop('service_version', 3)
    revision = overrides.pop('expected_revision', 2)
    desired = overrides.pop(
        'desired_info',
        SimpleNamespace(
            replica_id=replica_id,
            replica_record_id=record_id,
            version=version,
            system_recovery_launch_intent=None,
            system_recovery_disposition=(
                system_recovery_state.SystemRecoveryDisposition.ORDINARY),
            launch_request_id=None,
            service_job_id=None,
            candidate_ready_observed_at=None,
            ordinary_release_not_before=None,
            system_recovery_revision=revision,
            system_recovery=None,
            system_recovery_quarantine=None))
    assert not overrides
    return system_recovery_persistence.ReplicaObservationWrite(
        replica_id=replica_id,
        replica_record_id=record_id,
        service_version=version,
        expected_revision=revision,
        desired_info=desired)


def test_write_is_frozen_and_carries_one_exact_row_key() -> None:
    write = _write()

    assert (write.desired_recovery.system_recovery_revision ==
            write.expected_revision)
    with pytest.raises(dataclasses.FrozenInstanceError):
        write.expected_revision = 3


@pytest.mark.parametrize(('overrides', 'message'), [
    ({
        'replica_id': True
    }, 'replica_id must be a positive integer'),
    ({
        'replica_id': 0
    }, 'replica_id must be a positive integer'),
    ({
        'replica_record_id': 'not-a-uuid'
    }, 'canonical UUID string'),
    ({
        'service_version': 0
    }, 'service_version must be a positive integer'),
    ({
        'expected_revision': True
    }, 'expected_revision must be a nonnegative integer'),
    ({
        'expected_revision': -1
    }, 'expected_revision must be a nonnegative integer'),
])
def test_write_rejects_malformed_expected_key(overrides, message) -> None:
    with pytest.raises(ValueError, match=message):
        _write(**overrides)


@pytest.mark.parametrize(('desired_field', 'desired_value', 'message'), [
    ('replica_id', 8, 'desired_info must match replica_id'),
    ('replica_record_id', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
     'desired_info must match replica_record_id'),
    ('version', 4, 'desired_info must match service_version'),
    ('system_recovery_revision', 3,
     'desired_info must carry expected_revision'),
])
def test_write_rejects_desired_state_for_another_snapshot(
        desired_field, desired_value, message) -> None:
    record_id = str(uuid.uuid4())
    desired = SimpleNamespace(replica_id=7,
                              replica_record_id=record_id,
                              version=3,
                              system_recovery_revision=2)
    setattr(desired, desired_field, desired_value)

    with pytest.raises(ValueError, match=message):
        _write(replica_record_id=record_id, desired_info=desired)
