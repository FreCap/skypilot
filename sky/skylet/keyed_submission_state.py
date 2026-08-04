"""Dormant persistence foundation for future keyed Skylet submissions.

This module owns only additive SQLite tables and indexes. Skylet version 40 has
no producer for these rows. Persistent mutation fences and the reducer that can
satisfy them land together in the later keyed-protocol slice.
"""

import sqlite3

KEYED_SUBMISSION_SCHEMA_VERSION = 1


class KeyedSubmissionSchemaError(RuntimeError):
    """An owned SQLite object exists with an incompatible definition."""


_KEYED_SUBMISSIONS_TABLE = 'keyed_submissions'
_KEYED_SUBMISSION_CONTAINMENTS_TABLE = 'keyed_submission_containments'
_KEYED_SUBMISSION_SEALS_TABLE = 'keyed_submission_seals'

_TABLE_DDLS = {
    _KEYED_SUBMISSIONS_TABLE: """\
CREATE TABLE IF NOT EXISTS keyed_submissions (
  username TEXT NOT NULL,
  submission_key TEXT NOT NULL,
  job_id INTEGER NOT NULL CHECK (job_id > 0),
  add_digest TEXT NOT NULL,
  queue_digest TEXT,
  service_hash TEXT NOT NULL,
  worker_incarnation TEXT NOT NULL,
  lifecycle_fence_operation_id TEXT NOT NULL,
  lifecycle_fence_identity_digest TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN (
    'UNQUEUED', 'QUEUED', 'LAUNCHING', 'STARTED', 'CANCELLING',
    'COMPLETING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'QUARANTINED',
    'INSERTING', 'UPDATING', 'RECEIPTING', 'STATUS_UPDATING', 'DELETING')),
  driver_token TEXT,
  containment_plan BLOB NOT NULL,
  containment_plan_digest TEXT NOT NULL,
  provisional_root_pid INTEGER CHECK (
    provisional_root_pid IS NULL OR provisional_root_pid > 0),
  provisional_process_start_identity BLOB,
  local_supervisor_operation_id TEXT,
  local_cgroup_scope_uuid TEXT NOT NULL,
  supervisor_launch_receipt_digest TEXT,
  cancel_operation_id TEXT,
  cancel_digest TEXT,
  cancel_origin_state TEXT CHECK (
    cancel_origin_state IS NULL OR
    cancel_origin_state IN ('QUEUED', 'LAUNCHING', 'STARTED')),
  cancel_grace_policy_version TEXT CHECK (
    cancel_grace_policy_version IS NULL OR
    cancel_grace_policy_version = 'TERM_10S_THEN_HARD_KILL_V1'),
  cancel_host_boot_id TEXT,
  cancel_deadline_boottime_ns INTEGER CHECK (
    cancel_deadline_boottime_ns IS NULL OR
    cancel_deadline_boottime_ns >= 0),
  cancel_phase TEXT CHECK (
    cancel_phase IS NULL OR cancel_phase IN (
      'INTENT', 'GRACE_ENTERED', 'HARD_KILL_ENTERED', 'OWNERS_RETIRED')),
  completion_operation_id TEXT,
  completion_identity_digest TEXT,
  pending_legacy_status TEXT CHECK (
    pending_legacy_status IS NULL OR pending_legacy_status IN (
      'SUCCEEDED', 'FAILED', 'FAILED_SETUP', 'FAILED_DRIVER')),
  supervisor_outcome_receipt_digest TEXT,
  completion_phase TEXT CHECK (
    completion_phase IS NULL OR completion_phase IN (
      'OUTCOME_RECORDED', 'OWNERS_SEALED', 'OWNERS_RETIRED')),
  terminal_legacy_status TEXT CHECK (
    terminal_legacy_status IS NULL OR terminal_legacy_status IN (
      'SUCCEEDED', 'FAILED', 'FAILED_SETUP', 'FAILED_DRIVER', 'CANCELLED')),
  PRIMARY KEY (username, submission_key),
  CHECK ((provisional_root_pid IS NULL) =
         (provisional_process_start_identity IS NULL)),
  CHECK (state NOT IN (
    'UPDATING', 'LAUNCHING', 'STARTED', 'RECEIPTING', 'COMPLETING',
    'SUCCEEDED', 'FAILED') OR local_supervisor_operation_id IS NOT NULL),
  CHECK (state NOT IN (
    'UPDATING', 'LAUNCHING', 'STARTED', 'RECEIPTING', 'COMPLETING',
    'SUCCEEDED', 'FAILED') OR driver_token IS NOT NULL),
  CHECK (cancel_origin_state IS NULL OR cancel_origin_state = 'QUEUED' OR
         (local_supervisor_operation_id IS NOT NULL AND
          driver_token IS NOT NULL)),
  CHECK (
    (cancel_operation_id IS NULL AND cancel_digest IS NULL AND
     cancel_origin_state IS NULL AND cancel_grace_policy_version IS NULL AND
     cancel_host_boot_id IS NULL AND
     cancel_deadline_boottime_ns IS NULL AND cancel_phase IS NULL) OR
    (cancel_operation_id IS NOT NULL AND cancel_digest IS NOT NULL AND
     cancel_origin_state IS NOT NULL AND
     cancel_grace_policy_version IS NOT NULL AND
     cancel_host_boot_id IS NOT NULL AND
     cancel_deadline_boottime_ns IS NOT NULL AND cancel_phase IS NOT NULL)),
  CHECK (state != 'CANCELLING' OR cancel_operation_id IS NOT NULL),
  CHECK (
    (completion_operation_id IS NULL AND
     completion_identity_digest IS NULL AND pending_legacy_status IS NULL AND
     supervisor_outcome_receipt_digest IS NULL AND completion_phase IS NULL) OR
    (completion_operation_id IS NOT NULL AND
     completion_identity_digest IS NOT NULL AND
     pending_legacy_status IS NOT NULL AND
     supervisor_outcome_receipt_digest IS NOT NULL AND
     completion_phase IS NOT NULL)),
  CHECK (state NOT IN ('COMPLETING', 'SUCCEEDED', 'FAILED') OR
         completion_operation_id IS NOT NULL),
  CHECK (state NOT IN ('COMPLETING', 'SUCCEEDED', 'FAILED') OR
         cancel_operation_id IS NULL),
  CHECK (state != 'CANCELLED' OR completion_operation_id IS NULL),
  CHECK (
    (state = 'SUCCEEDED' AND terminal_legacy_status IS 'SUCCEEDED') OR
    (state = 'FAILED' AND terminal_legacy_status IS NOT NULL AND
     terminal_legacy_status IN ('FAILED', 'FAILED_SETUP', 'FAILED_DRIVER')) OR
    (state = 'CANCELLED' AND terminal_legacy_status IS 'CANCELLED') OR
    (state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND
     terminal_legacy_status IS NULL))
)""",
    _KEYED_SUBMISSION_CONTAINMENTS_TABLE: """\
CREATE TABLE IF NOT EXISTS keyed_submission_containments (
  username TEXT NOT NULL,
  submission_key TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  kind TEXT NOT NULL CHECK (kind = 'LOCAL_CGROUP_V2_DIRECT_V1'),
  implementation_digest TEXT NOT NULL,
  owner_uuid TEXT NOT NULL,
  identity_digest TEXT NOT NULL,
  provider_native_owner_locator BLOB,
  state TEXT NOT NULL CHECK (state IN (
    'PLANNED', 'PREPARED', 'LAUNCHED', 'SEALED', 'EMPTY', 'RETIRED',
    'QUARANTINED')),
  prepare_receipt BLOB,
  prepare_receipt_digest TEXT,
  launch_receipt BLOB,
  launch_receipt_digest TEXT,
  effect_receipt BLOB,
  effect_receipt_digest TEXT,
  seal_receipt BLOB,
  seal_receipt_digest TEXT,
  empty_receipt BLOB,
  empty_receipt_digest TEXT,
  retirement_receipt BLOB,
  retirement_receipt_digest TEXT,
  PRIMARY KEY (username, submission_key, ordinal),
  CHECK ((prepare_receipt IS NULL) = (prepare_receipt_digest IS NULL)),
  CHECK ((launch_receipt IS NULL) = (launch_receipt_digest IS NULL)),
  CHECK ((effect_receipt IS NULL) = (effect_receipt_digest IS NULL)),
  CHECK ((seal_receipt IS NULL) = (seal_receipt_digest IS NULL)),
  CHECK ((empty_receipt IS NULL) = (empty_receipt_digest IS NULL)),
  CHECK ((retirement_receipt IS NULL) =
         (retirement_receipt_digest IS NULL)),
  CHECK (state != 'PREPARED' OR prepare_receipt IS NOT NULL),
  CHECK (state != 'LAUNCHED' OR
         (prepare_receipt IS NOT NULL AND launch_receipt IS NOT NULL)),
  CHECK (state != 'SEALED' OR seal_receipt IS NOT NULL),
  CHECK (state != 'EMPTY' OR
         (seal_receipt IS NOT NULL AND empty_receipt IS NOT NULL)),
  CHECK (state != 'RETIRED' OR retirement_receipt IS NOT NULL)
)""",
    _KEYED_SUBMISSION_SEALS_TABLE: """\
CREATE TABLE IF NOT EXISTS keyed_submission_seals (
  username TEXT NOT NULL,
  submission_key TEXT NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('ADD', 'QUEUE')),
  expected_add_digest TEXT NOT NULL,
  job_id INTEGER,
  queue_digest TEXT,
  state TEXT NOT NULL CHECK (state = 'SEALED_ABSENT'),
  created_at REAL NOT NULL CHECK (created_at >= 0),
  PRIMARY KEY (username, submission_key, phase),
  CHECK ((phase = 'ADD' AND job_id IS NULL AND queue_digest IS NULL) OR
         (phase = 'QUEUE' AND job_id IS NOT NULL AND job_id > 0 AND
          queue_digest IS NOT NULL))
)""",
}

_INDEX_DDLS = {
    'keyed_submissions_job_id_uq': """\
CREATE UNIQUE INDEX IF NOT EXISTS keyed_submissions_job_id_uq
  ON keyed_submissions(job_id)""",
    'keyed_submissions_state_idx': """\
CREATE INDEX IF NOT EXISTS keyed_submissions_state_idx
  ON keyed_submissions(state, job_id)""",
    'keyed_submission_containments_owner_uuid_uq': """\
CREATE UNIQUE INDEX IF NOT EXISTS keyed_submission_containments_owner_uuid_uq
  ON keyed_submission_containments(owner_uuid)""",
    'keyed_submission_containments_state_idx': """\
CREATE INDEX IF NOT EXISTS keyed_submission_containments_state_idx
  ON keyed_submission_containments(state, username, submission_key)""",
    'keyed_submission_containments_local_owner_uq': """\
CREATE UNIQUE INDEX IF NOT EXISTS keyed_submission_containments_local_owner_uq
  ON keyed_submission_containments(username, submission_key)
  WHERE kind = 'LOCAL_CGROUP_V2_DIRECT_V1'""",
}

_ASCII_WHITESPACE = frozenset(' \t\n\r\v\f')


def _canonicalize_sql(sql: str) -> str:
    """Canonicalize owned DDL exactly as specified by schema version 1."""
    create_guards = (
        'CREATE TABLE IF NOT EXISTS ',
        'CREATE UNIQUE INDEX IF NOT EXISTS ',
        'CREATE INDEX IF NOT EXISTS ',
    )
    for guarded_prefix in create_guards:
        if sql.startswith(guarded_prefix):
            sql = sql.replace(' IF NOT EXISTS', '', 1)
            break

    canonical_chars: list[str] = []
    in_literal = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if char == "'":
            canonical_chars.append(char)
            if in_literal and i + 1 < len(sql) and sql[i + 1] == "'":
                canonical_chars.append("'")
                i += 2
                continue
            in_literal = not in_literal
        elif in_literal or char not in _ASCII_WHITESPACE:
            canonical_chars.append(char)
        i += 1

    if canonical_chars and canonical_chars[-1] == ';':
        canonical_chars.pop()
    return ''.join(canonical_chars)


def _owned_objects() -> list[tuple[str, str, str]]:
    return ([('table', name, ddl) for name, ddl in _TABLE_DDLS.items()] +
            [('index', name, ddl) for name, ddl in _INDEX_DDLS.items()])


def _validate_owned_objects(cursor: sqlite3.Cursor, *,
                            allow_missing: bool) -> None:
    for expected_type, expected_name, expected_sql in _owned_objects():
        rows = cursor.execute(
            """\
SELECT type, name, sql
FROM sqlite_master
WHERE name = ? COLLATE NOCASE
""", (expected_name,)).fetchall()
        if not rows:
            if allow_missing:
                continue
            raise KeyedSubmissionSchemaError(
                f'Missing keyed submission schema object: {expected_name}')
        if len(rows) != 1:
            raise KeyedSubmissionSchemaError(
                f'Ambiguous keyed submission schema object: {expected_name}')
        actual_type, actual_name, actual_sql = rows[0]
        if (actual_type != expected_type or actual_name != expected_name or
                actual_sql is None or _canonicalize_sql(actual_sql)
                != _canonicalize_sql(expected_sql)):
            raise KeyedSubmissionSchemaError(
                f'Incompatible keyed submission schema object: {expected_name}')


def schema_is_available(cursor: sqlite3.Cursor) -> bool:
    """Return whether all code-owned schema objects have their exact shape.

    Database access errors deliberately propagate. Only a reserved-name or
    owned-object definition mismatch is represented as an unavailable schema.
    """
    try:
        _validate_owned_objects(cursor, allow_missing=False)
    except KeyedSubmissionSchemaError:
        return False
    return True


def create_tables(cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> bool:
    """Create or validate the dormant keyed-submission schema atomically.

    Returns:
        True when the complete owned schema is available. False for an owned
        object name or shape conflict; ordinary v40 job initialization may
        continue in that case.

    Raises:
        RuntimeError: If called from an active transaction.
        sqlite3.DatabaseError: For database I/O, corruption, or legacy-table
            failures.
    """
    if conn.in_transaction:
        raise RuntimeError('Keyed submission schema installation requires no '
                           'active transaction.')

    transaction_started = False
    try:
        cursor.execute('BEGIN IMMEDIATE')
        transaction_started = True

        # Validate reserved names before dependent DDL. A conflicting table is
        # keyed-schema unavailability, not an incidental index creation error.
        _validate_owned_objects(cursor, allow_missing=True)
        for ddl in _TABLE_DDLS.values():
            cursor.execute(ddl)
        for ddl in _INDEX_DDLS.values():
            cursor.execute(ddl)
        _validate_owned_objects(cursor, allow_missing=False)
        conn.commit()
        return True
    except KeyedSubmissionSchemaError:
        if transaction_started:
            conn.rollback()
        return False
    except Exception:
        if transaction_started:
            conn.rollback()
        raise
