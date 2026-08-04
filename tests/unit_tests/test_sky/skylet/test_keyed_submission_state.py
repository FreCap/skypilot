"""Tests for the dormant keyed-submission SQLite schema."""

# The schema's code-owned DDL and canonicalizer are intentionally compared to
# the canonical design contract.
# pylint: disable=protected-access

import concurrent.futures
import pathlib
import sqlite3

import pytest

from sky.skylet import constants
from sky.skylet import job_lib
from sky.skylet import keyed_submission_state

_LEGACY_JOB_COLUMNS = (
    'job_id',
    'job_name',
    'username',
    'submitted_at',
    'status',
    'run_timestamp',
    'start_at',
    'end_at',
    'resources',
    'pid',
    'log_dir',
    'metadata',
    'exit_codes',
)
_LEGACY_PENDING_COLUMNS = ('job_id', 'run_cmd', 'submit', 'created_time')
_OWNED_OBJECTS = {
    ('table', 'keyed_submissions'),
    ('table', 'keyed_submission_containments'),
    ('table', 'keyed_submission_seals'),
    ('index', 'keyed_submissions_job_id_uq'),
    ('index', 'keyed_submissions_state_idx'),
    ('index', 'keyed_submission_containments_owner_uuid_uq'),
    ('index', 'keyed_submission_containments_state_idx'),
    ('index', 'keyed_submission_containments_local_owner_uq'),
}


def _connect(db_path) -> sqlite3.Connection:
    return sqlite3.connect(str(db_path), timeout=30)


def _create_legacy_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""\
CREATE TABLE jobs (
  job_id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_name TEXT,
  username TEXT,
  submitted_at FLOAT,
  status TEXT,
  run_timestamp TEXT CANDIDATE KEY,
  start_at FLOAT DEFAULT -1,
  end_at FLOAT DEFAULT NULL,
  resources TEXT DEFAULT NULL,
  pid INTEGER DEFAULT -1,
  log_dir TEXT DEFAULT NULL,
  metadata TEXT DEFAULT '{}',
  exit_codes TEXT DEFAULT NULL
);
CREATE TABLE pending_jobs (
  job_id INTEGER,
  run_cmd TEXT,
  submit INTEGER,
  created_time INTEGER
);
""")


def _legacy_shape(conn: sqlite3.Connection,
                  table: str) -> list[tuple[object, ...]]:
    return conn.execute(f'PRAGMA table_info({table})').fetchall()


def _insert_legacy_job(conn: sqlite3.Connection,
                       job_id: int,
                       *,
                       name: str = 'job') -> None:
    conn.execute(
        """\
INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", (job_id, name, 'user', float(job_id), 'INIT', f'run-{job_id}', None, None,
      'CPU:1', 0, None, '{}', None))


def _insert_submission(cursor: sqlite3.Cursor, **overrides) -> None:
    values = {
        'username': 'user',
        'submission_key': 'submission-1',
        'job_id': 1,
        'add_digest': 'add-digest',
        'service_hash': 'service',
        'worker_incarnation': 'worker',
        'lifecycle_fence_operation_id': 'fence-operation',
        'lifecycle_fence_identity_digest': 'fence-digest',
        'state': 'UNQUEUED',
        'containment_plan': b'plan',
        'containment_plan_digest': 'plan-digest',
        'local_cgroup_scope_uuid': 'scope',
    }
    values.update(overrides)
    columns = ', '.join(values)
    placeholders = ', '.join('?' for _ in values)
    cursor.execute(
        f'INSERT INTO keyed_submissions ({columns}) '
        f'VALUES ({placeholders})', tuple(values.values()))


def _insert_containment(cursor: sqlite3.Cursor, **overrides) -> None:
    values = {
        'username': 'user',
        'submission_key': 'submission-1',
        'ordinal': 0,
        'kind': 'LOCAL_CGROUP_V2_DIRECT_V1',
        'implementation_digest': 'implementation',
        'owner_uuid': 'owner-1',
        'identity_digest': 'identity',
        'state': 'PLANNED',
    }
    values.update(overrides)
    columns = ', '.join(values)
    placeholders = ', '.join('?' for _ in values)
    cursor.execute(
        f'INSERT INTO keyed_submission_containments ({columns}) '
        f'VALUES ({placeholders})', tuple(values.values()))


def _initialize(db_path) -> sqlite3.Connection:
    conn = _connect(db_path)
    job_lib.create_table(conn.cursor(), conn)
    return conn


def test_fresh_database_has_exact_passive_schema(tmp_path):
    conn = _initialize(tmp_path / 'jobs.db')
    try:
        assert constants.SKYLET_VERSION == '44'
        assert keyed_submission_state.KEYED_SUBMISSION_SCHEMA_VERSION == 1
        assert keyed_submission_state.schema_is_available(conn.cursor())
        assert tuple(row[1] for row in _legacy_shape(conn, 'jobs')) == (
            _LEGACY_JOB_COLUMNS)
        assert tuple(row[1] for row in _legacy_shape(conn, 'pending_jobs')) == (
            _LEGACY_PENDING_COLUMNS)

        owned_objects = set(
            conn.execute("""\
SELECT type, name
FROM sqlite_master
WHERE name LIKE 'keyed_submission%'
""").fetchall())
        assert owned_objects == _OWNED_OBJECTS
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger'"
        ).fetchone() == (0,)
        for table in ('keyed_submissions', 'keyed_submission_containments',
                      'keyed_submission_seals'):
            assert conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone() == (
                0,)

        index_rows = {
            row[1]: (row[2], row[4]) for row in conn.execute(
                'PRAGMA index_list(keyed_submission_containments)')
        }
        assert index_rows['keyed_submission_containments_local_owner_uq'] == (1,
                                                                              1)
    finally:
        conn.close()


def test_sql_canonicalization_preserves_quoted_bytes():
    guarded = """\
CREATE TABLE IF NOT EXISTS sample (
  value TEXT CHECK (value = 'a  b'' c')
);
"""
    catalog = "CREATE TABLE sample(value TEXT CHECK(value='a  b'' c'))"

    assert keyed_submission_state._canonicalize_sql(guarded) == (
        keyed_submission_state._canonicalize_sql(catalog))
    assert keyed_submission_state._canonicalize_sql(guarded) == (
        "CREATETABLEsample(valueTEXTCHECK(value='a  b'' c'))")


def test_owned_ddl_matches_canonical_design_sql():
    repo_root = pathlib.Path(job_lib.__file__).resolve().parents[2]
    design = (repo_root /
              'docs/designs/provider-lifecycle-actuation.md').read_text(
                  encoding='utf-8')
    sql_fence = chr(96) * 3
    marker = f'The exact v1 side-table layout is:\n\n{sql_fence}sql\n'
    design_sql = design.split(marker, maxsplit=1)[1].split(f'\n{sql_fence}',
                                                           maxsplit=1)[0]
    design_statements = [
        statement for statement in design_sql.strip().split(';\n') if statement
    ]
    module_statements = [
        *keyed_submission_state._TABLE_DDLS.values(),
        *keyed_submission_state._INDEX_DDLS.values(),
    ]

    canonicalize = keyed_submission_state._canonicalize_sql
    assert {canonicalize(sql) for sql in module_statements
           } == {canonicalize(sql) for sql in design_statements}


def test_upgrade_preserves_populated_v40_tables(tmp_path):
    conn = _connect(tmp_path / 'jobs.db')
    try:
        _create_legacy_tables(conn)
        _insert_legacy_job(conn, 1, name='first')
        _insert_legacy_job(conn, 2, name='second')
        conn.executemany('INSERT INTO pending_jobs VALUES (?, ?, ?, ?)', [
            (1, 'echo first', 0, 10),
            (1, 'echo duplicate', 0, 11),
            (2, 'echo second', 12, 12),
        ])
        conn.commit()

        before_shapes = {
            table: _legacy_shape(conn, table)
            for table in ('jobs', 'pending_jobs')
        }
        before_jobs = conn.execute(
            'SELECT * FROM jobs ORDER BY job_id').fetchall()
        before_pending = conn.execute(
            'SELECT * FROM pending_jobs ORDER BY rowid').fetchall()

        job_lib.create_table(conn.cursor(), conn)

        assert keyed_submission_state.schema_is_available(conn.cursor())
        assert {
            table: _legacy_shape(conn, table)
            for table in ('jobs', 'pending_jobs')
        } == before_shapes
        assert conn.execute(
            'SELECT * FROM jobs ORDER BY job_id').fetchall() == before_jobs
        assert conn.execute('SELECT * FROM pending_jobs ORDER BY rowid'
                           ).fetchall() == before_pending
    finally:
        conn.close()


def test_v40_positional_writes_remain_unrestricted(tmp_path):
    conn = _initialize(tmp_path / 'jobs.db')
    try:
        _insert_legacy_job(conn, 1)
        conn.executemany('INSERT INTO pending_jobs VALUES (?, ?, ?, ?)', [
            (1, 'echo one', 0, 10),
            (1, 'echo duplicate', 0, 11),
        ])
        conn.execute(
            """\
UPDATE jobs
SET status = ?, pid = ?, start_at = ?, end_at = ?
WHERE job_id = ?
""", ('RUNNING', 123, 20.0, None, 1))
        conn.execute('UPDATE pending_jobs SET submit = ? WHERE job_id = ?',
                     (21, 1))
        conn.execute(
            """\
DELETE FROM pending_jobs
WHERE rowid = (
  SELECT MIN(rowid) FROM pending_jobs WHERE job_id = ?
)
""", (1,))
        conn.commit()

        assert conn.execute(
            'SELECT status, pid, start_at, end_at FROM jobs WHERE job_id = 1'
        ).fetchone() == ('RUNNING', 123, 20.0, None)
        assert conn.execute(
            'SELECT run_cmd, submit FROM pending_jobs WHERE job_id = 1'
        ).fetchall() == [('echo duplicate', 21)]
        for table in ('keyed_submissions', 'keyed_submission_containments',
                      'keyed_submission_seals'):
            assert conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone() == (
                0,)
    finally:
        conn.close()


def test_schema_installation_is_idempotent(tmp_path):
    conn = _initialize(tmp_path / 'jobs.db')
    try:
        assert keyed_submission_state.create_tables(conn.cursor(), conn)
        assert keyed_submission_state.create_tables(conn.cursor(), conn)
        assert keyed_submission_state.schema_is_available(conn.cursor())
        owned_objects = set(
            conn.execute("""\
SELECT type, name
FROM sqlite_master
WHERE name LIKE 'keyed_submission%'
""").fetchall())
        assert owned_objects == _OWNED_OBJECTS
    finally:
        conn.close()


def test_schema_installation_is_concurrent_create_or_validate(tmp_path):
    db_path = tmp_path / 'jobs.db'
    conn = _connect(db_path)
    _create_legacy_tables(conn)
    conn.close()

    def install() -> bool:
        worker_conn = _connect(db_path)
        try:
            return keyed_submission_state.create_tables(worker_conn.cursor(),
                                                        worker_conn)
        finally:
            worker_conn.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: install(), range(16)))

    assert results == [True] * 16
    conn = _connect(db_path)
    try:
        assert keyed_submission_state.schema_is_available(conn.cursor())
    finally:
        conn.close()


def test_wrong_owned_shape_disables_only_keyed_schema(tmp_path, caplog):
    conn = _connect(tmp_path / 'jobs.db')
    try:
        _create_legacy_tables(conn)
        _insert_legacy_job(conn, 1)
        conn.execute('CREATE TABLE keyed_submissions (username TEXT NOT NULL)')
        conn.commit()

        # Legacy initialization tolerates the reserved-name collision. Every
        # other object from the attempted side-schema transaction rolls back.
        job_lib.create_table(conn.cursor(), conn)

        assert 'Keyed submission schema is unavailable' in caplog.text
        assert not keyed_submission_state.schema_is_available(conn.cursor())
        assert not keyed_submission_state.create_tables(conn.cursor(), conn)
        present_owned_objects = set(
            conn.execute("""\
SELECT type, name
FROM sqlite_master
WHERE name LIKE 'keyed_submission%'
""").fetchall())
        assert present_owned_objects == {('table', 'keyed_submissions')}

        _insert_legacy_job(conn, 2)
        conn.execute('INSERT INTO pending_jobs VALUES (?, ?, ?, ?)',
                     (2, 'echo still-legacy', 0, 20))
        conn.commit()
        assert conn.execute(
            'SELECT job_id FROM jobs ORDER BY job_id').fetchall() == [(1,),
                                                                      (2,)]
    finally:
        conn.close()


@pytest.mark.parametrize('overrides', [
    {
        'job_id': 0,
        'submission_key': 'bad-job-id',
    },
    {
        'job_id': 2,
        'submission_key': 'bad-state',
        'state': 'UNKNOWN',
    },
    {
        'job_id': 2,
        'submission_key': 'half-process',
        'provisional_root_pid': 123,
    },
    {
        'job_id': 2,
        'submission_key': 'launch-without-owner',
        'state': 'LAUNCHING',
    },
    {
        'job_id': 2,
        'submission_key': 'cancel-without-identity',
        'state': 'CANCELLING',
    },
])
def test_submission_check_constraints(tmp_path, overrides):
    conn = _initialize(tmp_path / 'jobs.db')
    try:
        with pytest.raises(sqlite3.IntegrityError):
            _insert_submission(conn.cursor(), **overrides)
    finally:
        conn.close()


def test_submission_primary_key_unique_job_and_terminal_constraints(tmp_path):
    conn = _initialize(tmp_path / 'jobs.db')
    try:
        _insert_submission(conn.cursor())
        conn.commit()

        with pytest.raises(sqlite3.IntegrityError):
            _insert_submission(conn.cursor(),
                               submission_key='submission-1',
                               job_id=2)
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_submission(conn.cursor(),
                               submission_key='other-key',
                               job_id=1)
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_submission(conn.cursor(),
                               submission_key='bad-terminal',
                               job_id=2,
                               state='CANCELLED')
        conn.rollback()

        _insert_submission(conn.cursor(),
                           submission_key='cancelled',
                           job_id=2,
                           state='CANCELLED',
                           terminal_legacy_status='CANCELLED')
        conn.commit()
    finally:
        conn.close()


def test_completion_and_cancel_are_mutually_exclusive(tmp_path):
    conn = _initialize(tmp_path / 'jobs.db')
    try:
        with pytest.raises(sqlite3.IntegrityError):
            _insert_submission(
                conn.cursor(),
                submission_key='competing-owners',
                job_id=2,
                state='COMPLETING',
                driver_token='driver',
                local_supervisor_operation_id='supervisor',
                cancel_operation_id='cancel',
                cancel_digest='cancel-digest',
                cancel_origin_state='STARTED',
                cancel_grace_policy_version='TERM_10S_THEN_HARD_KILL_V1',
                cancel_host_boot_id='boot',
                cancel_deadline_boottime_ns=10,
                cancel_phase='INTENT',
                completion_operation_id='completion',
                completion_identity_digest='completion-digest',
                pending_legacy_status='SUCCEEDED',
                supervisor_outcome_receipt_digest='outcome',
                completion_phase='OUTCOME_RECORDED')
    finally:
        conn.close()


def test_containment_constraints_and_unique_owners(tmp_path):
    conn = _initialize(tmp_path / 'jobs.db')
    try:
        _insert_containment(conn.cursor())
        conn.commit()

        with pytest.raises(sqlite3.IntegrityError):
            _insert_containment(conn.cursor(),
                                submission_key='submission-2',
                                owner_uuid='owner-1')
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_containment(conn.cursor(), ordinal=1, owner_uuid='owner-2')
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_containment(conn.cursor(),
                                submission_key='bad-kind',
                                owner_uuid='owner-3',
                                kind='UNQUALIFIED')
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_containment(conn.cursor(),
                                submission_key='half-receipt',
                                owner_uuid='owner-4',
                                prepare_receipt=b'prepared')
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_containment(conn.cursor(),
                                submission_key='missing-prepared',
                                owner_uuid='owner-5',
                                state='PREPARED')
        conn.rollback()

        _insert_containment(conn.cursor(),
                            submission_key='prepared',
                            owner_uuid='owner-6',
                            state='PREPARED',
                            prepare_receipt=b'prepared',
                            prepare_receipt_digest='prepared-digest')
        conn.commit()
    finally:
        conn.close()


def test_seal_constraints_and_primary_key(tmp_path):
    conn = _initialize(tmp_path / 'jobs.db')
    try:
        conn.execute(
            """\
INSERT INTO keyed_submission_seals
  (username, submission_key, phase, expected_add_digest, state, created_at)
VALUES (?, ?, ?, ?, ?, ?)
""", ('user', 'submission-1', 'ADD', 'add-digest', 'SEALED_ABSENT', 1.0))
        conn.commit()

        invalid_rows = [
            ('user', 'bad-phase', 'OTHER', 'add', None, None, 'SEALED_ABSENT',
             1.0),
            ('user', 'bad-state', 'ADD', 'add', None, None, 'PRESENT', 1.0),
            ('user', 'bad-queue', 'QUEUE', 'add', None, None, 'SEALED_ABSENT',
             1.0),
            ('user', 'bad-time', 'ADD', 'add', None, None, 'SEALED_ABSENT',
             -1.0),
        ]
        for row in invalid_rows:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    'INSERT INTO keyed_submission_seals VALUES '
                    '(?, ?, ?, ?, ?, ?, ?, ?)', row)
            conn.rollback()

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """\
INSERT INTO keyed_submission_seals
  (username, submission_key, phase, expected_add_digest, state, created_at)
VALUES (?, ?, ?, ?, ?, ?)
""", ('user', 'submission-1', 'ADD', 'add-digest', 'SEALED_ABSENT', 1.0))
        conn.rollback()

        conn.execute(
            'INSERT INTO keyed_submission_seals VALUES '
            '(?, ?, ?, ?, ?, ?, ?, ?)', ('user', 'submission-2', 'QUEUE', 'add',
                                         2, 'queue', 'SEALED_ABSENT', 2.0))
        conn.commit()
    finally:
        conn.close()
