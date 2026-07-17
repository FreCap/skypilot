"""Unit tests for Skylet's node-local job database."""

from sky.skylet import job_lib
from sky.utils.db import db_utils


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
