"""Unit tests for Skylet's node-local job database."""

import dataclasses
import json
import sqlite3

import pytest

from sky.skylet import job_lib
from sky.skylet import system_oom_recovery
from sky.utils import message_utils
from sky.utils.db import db_utils


@pytest.fixture
def job_database(tmp_path, monkeypatch):
    database = db_utils.SQLiteConn(str(tmp_path / 'jobs.db'),
                                   job_lib.create_table)
    monkeypatch.setattr(job_lib, '_DB', database)
    monkeypatch.setattr(job_lib.constants, 'SKY_LOGS_DIRECTORY',
                        str(tmp_path / 'logs'))
    yield database
    database.conn.close()


def _add_running_job() -> int:
    job_id, _ = job_lib.add_job('service', 'user', 'run-ts', 'CPU:1')
    job_lib.set_job_started(job_id)
    return job_id


def _armed_info() -> job_lib.JobSystemRecoveryInfo:
    return job_lib.JobSystemRecoveryInfo(
        capability=system_oom_recovery.CAPABILITY_V2,
        phase=job_lib.JobSystemRecoveryPhase.ARMED,
        original_attempt_id='attempt-0',
        replacement_attempt_id=None,
        task_index=0,
        node_boot_id='boot-id',
        occurrence_count=0,
        armed_at=100.0,
        updated_at=100.0,
    )


def _waiting_cleanup_info() -> job_lib.JobSystemRecoveryInfo:
    return dataclasses.replace(
        _armed_info(),
        phase=job_lib.JobSystemRecoveryPhase.WAITING_CLEANUP,
        event_id='event-1',
        reason='RAY_NODE_OOM',
        occurrence_count=1,
        occurred_at=101.0,
        updated_at=101.0,
        deadline_at=221.0,
        summary='Ray killed the service worker for host memory pressure.',
    )


def test_skylet_lib_version_includes_system_recovery_api():
    assert job_lib.constants.SKYLET_VERSION == '44'
    assert job_lib.constants.SKYLET_LIB_VERSION == 9
    assert job_lib.JOB_SYSTEM_RECOVERY_API_VERSION == 1


def test_protobuf_optional_replacement_presence_and_validation():
    armed = _armed_info()
    absent = armed.to_protobuf()
    assert not absent.HasField('replacement_attempt_id')
    assert job_lib.JobSystemRecoveryInfo.from_protobuf(absent) == armed

    replacement = dataclasses.replace(
        _waiting_cleanup_info(),
        phase=job_lib.JobSystemRecoveryPhase.RESUBMITTING,
        replacement_attempt_id='attempt-1')
    present = replacement.to_protobuf()
    assert present.HasField('replacement_attempt_id')
    assert job_lib.JobSystemRecoveryInfo.from_protobuf(present) == replacement

    malformed_empty = armed.to_protobuf()
    malformed_empty.replacement_attempt_id = ''
    with pytest.raises(ValueError, match='replacement_attempt_id'):
        job_lib.JobSystemRecoveryInfo.from_protobuf(malformed_empty)

    unknown_phase = armed.to_protobuf()
    unknown_phase.phase = 99
    with pytest.raises(ValueError, match='Unknown'):
        job_lib.JobSystemRecoveryInfo.from_protobuf(unknown_phase)

    invalid_timestamp = armed.to_protobuf()
    invalid_timestamp.updated_at = float('nan')
    with pytest.raises(ValueError, match='finite timestamp'):
        job_lib.JobSystemRecoveryInfo.from_protobuf(invalid_timestamp)


def test_recovery_info_rejects_boolean_schema_and_scalar_fields():
    payload = _armed_info().to_dict()
    payload['schema_version'] = True
    with pytest.raises(ValueError, match='schema version'):
        job_lib.JobSystemRecoveryInfo.from_dict(payload)

    for field in ('task_index', 'occurrence_count', 'armed_at', 'updated_at'):
        payload = _armed_info().to_dict()
        payload[field] = True
        with pytest.raises(ValueError, match=field):
            job_lib.JobSystemRecoveryInfo.from_dict(payload)

    payload = _armed_info().to_dict()
    payload['unexpected'] = 'field'
    with pytest.raises(ValueError, match='invalid fields'):
        job_lib.JobSystemRecoveryInfo.from_dict(payload)


@pytest.mark.parametrize('protobuf_status', [0, 99])
def test_zero_or_unknown_detail_status_is_malformed(protobuf_status):
    assert not hasattr(job_lib.JobSystemRecoveryDetailStatus, 'UNSPECIFIED')
    assert job_lib.JobSystemRecoveryDetailStatus.from_protobuf(
        protobuf_status) == job_lib.JobSystemRecoveryDetailStatus.MALFORMED


def test_add_job_uses_exact_inserted_row_for_duplicate_timestamp(
        tmp_path, monkeypatch):
    database = db_utils.SQLiteConn(str(tmp_path / 'jobs.db'),
                                   job_lib.create_table)
    monkeypatch.setattr(job_lib, '_DB', database)
    monkeypatch.setattr(job_lib.constants, 'SKY_LOGS_DIRECTORY',
                        str(tmp_path / 'logs'))

    try:
        first_id, first_log_dir = job_lib.add_job('first', 'user', 'same-run',
                                                  'CPU:1')
        second_id, second_log_dir = job_lib.add_job('second', 'user',
                                                    'same-run', 'CPU:1')

        assert (first_id, second_id) == (1, 2)
        rows = database.cursor.execute(
            'SELECT job_id, job_name, run_timestamp, log_dir '
            'FROM jobs ORDER BY job_id').fetchall()
        assert rows == [
            (first_id, 'first', 'same-run', first_log_dir),
            (second_id, 'second', 'same-run', second_log_dir),
        ]
    finally:
        database.conn.close()


def test_companion_table_preserves_legacy_jobs_layout(job_database):
    columns = [
        row[1] for row in job_database.cursor.execute('PRAGMA table_info(jobs)')
    ]
    assert columns == [
        'job_id', 'job_name', 'username', 'submitted_at', 'status',
        'run_timestamp', 'start_at', 'end_at', 'resources', 'pid', 'log_dir',
        'metadata', 'exit_codes'
    ]

    # This is the positional shape used by a pre-recovery skylet. An extra
    # column on ``jobs`` would make this downgrade-path insert fail.
    job_database.cursor.execute(
        "INSERT INTO jobs VALUES "
        "(null, 'legacy', 'user', 1, 'INIT', 'legacy-run', null, null, "
        "null, 0, null, '{}', null)")
    job_database.conn.commit()
    assert job_database.cursor.execute(
        "SELECT job_name FROM jobs WHERE run_timestamp='legacy-run'").fetchone(
        ) == ('legacy',)


def test_system_recovery_transition_and_atomic_exhaustion(job_database):
    job_id = _add_running_job()
    armed = _armed_info()
    waiting_cleanup = _waiting_cleanup_info()
    waiting_memory = dataclasses.replace(
        waiting_cleanup,
        phase=job_lib.JobSystemRecoveryPhase.WAITING_MEMORY,
        updated_at=102.0,
    )
    resubmitting = dataclasses.replace(
        waiting_memory,
        phase=job_lib.JobSystemRecoveryPhase.RESUBMITTING,
        replacement_attempt_id='attempt-1',
        updated_at=103.0,
    )
    retry_submitted = dataclasses.replace(
        resubmitting,
        phase=job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED,
        updated_at=104.0,
    )
    exhausted = dataclasses.replace(
        retry_submitted,
        phase=job_lib.JobSystemRecoveryPhase.EXHAUSTED,
        occurrence_count=2,
        updated_at=105.0,
        summary='The replacement attempt also exceeded host memory.',
    )

    assert job_lib.arm_job_system_recovery(job_id, armed)
    assert job_lib.arm_job_system_recovery(job_id, armed)
    assert job_lib.get_job_system_recovery_info(job_id) == armed
    assert job_lib.transition_job_system_recovery(
        job_id, job_lib.JobSystemRecoveryPhase.ARMED, waiting_cleanup)
    assert not job_lib.transition_job_system_recovery(
        job_id, job_lib.JobSystemRecoveryPhase.ARMED, waiting_memory)
    assert job_lib.transition_job_system_recovery(
        job_id, job_lib.JobSystemRecoveryPhase.WAITING_CLEANUP, waiting_memory)
    with job_lib.job_status_lock(job_id):
        assert job_lib.transition_job_system_recovery_no_lock(
            job_id, job_lib.JobSystemRecoveryPhase.WAITING_MEMORY, resubmitting)
        assert job_lib.transition_job_system_recovery_no_lock(
            job_id, job_lib.JobSystemRecoveryPhase.RESUBMITTING,
            retry_submitted)
    assert job_lib.exhaust_job_system_recovery(
        job_id, job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED, exhausted)

    assert job_lib.get_status(job_id) == job_lib.JobStatus.FAILED
    assert job_lib.get_job_system_recovery_info(job_id) == exhausted
    # The terminal write is idempotent for the exact same record.
    assert job_lib.exhaust_job_system_recovery(
        job_id, job_lib.JobSystemRecoveryPhase.RETRY_SUBMITTED, exhausted)


def test_system_recovery_rejects_identity_change(job_database):
    job_id = _add_running_job()
    armed = _armed_info()
    assert job_lib.arm_job_system_recovery(job_id, armed)

    wrong_identity = dataclasses.replace(_waiting_cleanup_info(),
                                         node_boot_id='different-boot')
    with pytest.raises(ValueError, match='identity field changed'):
        job_lib.transition_job_system_recovery(
            job_id, job_lib.JobSystemRecoveryPhase.ARMED, wrong_identity)
    assert job_lib.get_job_system_recovery_info(job_id) == armed


def test_cancellation_wins_over_system_recovery_exhaustion(job_database):
    job_id = _add_running_job()
    armed = _armed_info()
    waiting = _waiting_cleanup_info()
    assert job_lib.arm_job_system_recovery(job_id, armed)
    assert job_lib.transition_job_system_recovery(
        job_id, job_lib.JobSystemRecoveryPhase.ARMED, waiting)
    job_lib.set_status(job_id, job_lib.JobStatus.CANCELLED)
    exhausted = dataclasses.replace(
        waiting,
        phase=job_lib.JobSystemRecoveryPhase.EXHAUSTED,
        updated_at=102.0,
    )

    assert not job_lib.exhaust_job_system_recovery(
        job_id, job_lib.JobSystemRecoveryPhase.WAITING_CLEANUP, exhausted)
    assert job_lib.get_status(job_id) == job_lib.JobStatus.CANCELLED
    assert job_lib.get_job_system_recovery_info(job_id) == waiting


def test_exhaustion_rolls_back_recovery_if_status_write_fails(job_database):
    job_id = _add_running_job()
    armed = _armed_info()
    waiting = _waiting_cleanup_info()
    assert job_lib.arm_job_system_recovery(job_id, armed)
    assert job_lib.transition_job_system_recovery(
        job_id, job_lib.JobSystemRecoveryPhase.ARMED, waiting)
    job_database.cursor.execute("""
        CREATE TRIGGER reject_failed_status
        BEFORE UPDATE OF status ON jobs
        WHEN NEW.status = 'FAILED'
        BEGIN
          SELECT RAISE(ABORT, 'injected status failure');
        END
    """)
    job_database.conn.commit()
    exhausted = dataclasses.replace(
        waiting,
        phase=job_lib.JobSystemRecoveryPhase.EXHAUSTED,
        updated_at=102.0,
    )

    with pytest.raises(sqlite3.IntegrityError, match='injected status failure'):
        job_lib.exhaust_job_system_recovery(
            job_id, job_lib.JobSystemRecoveryPhase.WAITING_CLEANUP, exhausted)
    assert job_lib.get_status(job_id) == job_lib.JobStatus.RUNNING
    assert job_lib.get_job_system_recovery_info(job_id) == waiting


def test_status_payload_new_and_legacy_fail_closed(job_database):
    job_id = _add_running_job()
    armed = _armed_info()
    assert job_lib.arm_job_system_recovery(job_id, armed)

    payload = job_lib.get_statuses_with_system_recovery_payload([job_id])
    statuses, infos, detail_statuses = (
        job_lib.load_statuses_with_system_recovery_payload(payload))
    assert statuses == {job_id: job_lib.JobStatus.RUNNING}
    assert infos == {job_id: armed}
    assert detail_statuses == {
        job_id: job_lib.JobSystemRecoveryDetailStatus.PRESENT
    }

    legacy_payload = message_utils.encode_payload(
        {job_id: job_lib.JobStatus.RUNNING.value})
    statuses, infos, detail_statuses = (
        job_lib.load_statuses_with_system_recovery_payload(legacy_payload))
    assert statuses == {job_id: job_lib.JobStatus.RUNNING}
    assert infos == {}
    assert detail_statuses == {
        job_id: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
    }


@pytest.mark.parametrize('detail_statuses', [{}, {'7': 'UNSPECIFIED'}])
def test_missing_or_legacy_ssh_detail_status_is_malformed(detail_statuses):
    payload = message_utils.encode_payload({
        'version': 1,
        'job_statuses': {
            7: job_lib.JobStatus.RUNNING.value
        },
        'system_recovery_infos': {},
        'system_recovery_detail_statuses': detail_statuses,
    })

    statuses, infos, statuses_by_job = (
        job_lib.load_statuses_with_system_recovery_payload(payload))

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    assert infos == {}
    assert statuses_by_job == {
        7: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
    }


def test_malformed_recovery_info_does_not_hide_status(job_database):
    job_id = _add_running_job()
    job_database.cursor.execute(
        'INSERT INTO job_system_recovery(job_id, info_json) VALUES (?, ?)',
        (job_id, '{not-json'))
    job_database.conn.commit()

    assert job_lib.get_job_system_recovery_info(job_id) is None
    payload = job_lib.get_statuses_with_system_recovery_payload([job_id])
    statuses, infos, detail_statuses = (
        job_lib.load_statuses_with_system_recovery_payload(payload))
    assert statuses == {job_id: job_lib.JobStatus.RUNNING}
    assert infos == {}
    assert detail_statuses == {
        job_id: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
    }


def test_boolean_ssh_envelope_version_is_malformed_not_legacy():
    payload = message_utils.encode_payload({
        'version': True,
        'job_statuses': {
            7: job_lib.JobStatus.RUNNING.value
        },
        'system_recovery_infos': {},
        'system_recovery_detail_statuses': {
            7: job_lib.JobSystemRecoveryDetailStatus.ABSENT.value
        },
    })

    statuses, infos, detail_statuses = (
        job_lib.load_statuses_with_system_recovery_payload(payload))

    assert statuses == {7: job_lib.JobStatus.RUNNING}
    assert infos == {}
    assert detail_statuses == {
        7: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
    }


def test_huge_timestamp_detail_is_malformed_without_hiding_status(job_database):
    job_id = _add_running_job()
    payload = _armed_info().to_dict()
    payload['updated_at'] = 10**400
    job_database.cursor.execute(
        'INSERT INTO job_system_recovery(job_id, info_json) VALUES (?, ?)',
        (job_id, json.dumps(payload)))
    job_database.conn.commit()

    statuses_payload = job_lib.get_statuses_with_system_recovery_payload(
        [job_id])
    statuses, infos, detail_statuses = (
        job_lib.load_statuses_with_system_recovery_payload(statuses_payload))

    assert statuses == {job_id: job_lib.JobStatus.RUNNING}
    assert infos == {}
    assert detail_statuses == {
        job_id: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
    }


def test_ssh_detail_query_failure_preserves_ordinary_status(
        job_database, monkeypatch):
    job_id = _add_running_job()

    def _fail_detail_query(_job_ids):
        raise RuntimeError('detail query failed')

    monkeypatch.setattr(job_lib, 'get_job_system_recovery_details',
                        _fail_detail_query)

    payload = job_lib.get_statuses_with_system_recovery_payload([job_id])
    statuses, infos, detail_statuses = (
        job_lib.load_statuses_with_system_recovery_payload(payload))

    assert statuses == {job_id: job_lib.JobStatus.RUNNING}
    assert infos == {}
    assert detail_statuses == {
        job_id: job_lib.JobSystemRecoveryDetailStatus.MALFORMED
    }
