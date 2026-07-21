"""Tests for managed-job log garbage collection."""

# pylint: disable=protected-access

import builtins
import pathlib
import shutil
from unittest import mock

from sky.jobs import log_gc


def _task(job_id: int, local_log_file: pathlib.Path) -> dict:
    return {
        'job_id': job_id,
        'task_id': 0,
        'local_log_file': str(local_log_file),
    }


def test_controller_cleanup_isolates_file_failure(monkeypatch, tmp_path,
                                                  caplog):
    paths = {job_id: tmp_path / f'{job_id}.log' for job_id in (1, 2, 3)}
    for path in paths.values():
        path.write_text('old log', encoding='utf-8')

    get_logs = mock.Mock(return_value=[{'job_id': job_id} for job_id in paths])
    set_cleaned = mock.Mock()
    monkeypatch.setattr(log_gc.managed_job_state,
                        'get_controller_logs_to_clean', get_logs)
    monkeypatch.setattr(log_gc.managed_job_state, 'set_controller_logs_cleaned',
                        set_cleaned)
    monkeypatch.setattr(log_gc.managed_job_utils, 'controller_log_file_for_job',
                        lambda job_id: str(paths[job_id]))

    original_open = builtins.open

    def _open(path, *args, **kwargs):
        if pathlib.Path(path) == paths[2]:
            raise OSError('read-only filesystem')
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, 'open', _open)

    # The selected batch is full even though one row failed. Keep the normal
    # continuation signal so the caller can discover more rows immediately;
    # the failed row is recorded so later rounds page past it.
    failed: set = set()
    assert not log_gc._clean_controller_logs_with_retention(
        60, batch_size=3, failed_job_ids=failed)

    get_logs.assert_called_once_with(60, batch_size=3, exclude_job_ids=failed)
    assert failed == {2}
    set_cleaned.assert_called_once()
    assert set_cleaned.call_args.kwargs['job_ids'] == [1, 3]
    assert 'Failed to clean controller logs for job 2' in caplog.text
    assert paths[2].read_text(encoding='utf-8') == 'old log'
    assert 'Controller log has been cleaned' in paths[3].read_text(
        encoding='utf-8')


def test_controller_cleanup_uses_one_batch_timestamp(monkeypatch, tmp_path):
    existing = tmp_path / 'existing.log'
    existing.write_text('old log', encoding='utf-8')
    missing = tmp_path / 'missing.log'
    get_logs = mock.Mock(return_value=[{'job_id': 1}, {'job_id': 2}])
    set_cleaned = mock.Mock()
    clock = mock.Mock(return_value=123.0)
    monkeypatch.setattr(log_gc.managed_job_state,
                        'get_controller_logs_to_clean', get_logs)
    monkeypatch.setattr(log_gc.managed_job_state, 'set_controller_logs_cleaned',
                        set_cleaned)
    monkeypatch.setattr(
        log_gc.managed_job_utils, 'controller_log_file_for_job',
        lambda job_id: str(existing if job_id == 1 else missing))
    monkeypatch.setattr(log_gc.time, 'time', clock)
    # logging.LogRecord also calls the process-global time.time(); isolate the
    # cleanup implementation's explicit clock reads from logging internals.
    monkeypatch.setattr(log_gc.logger, 'info', mock.Mock())

    assert log_gc._clean_controller_logs_with_retention(60, batch_size=10)

    assert clock.call_count == 1
    set_cleaned.assert_called_once_with(job_ids=[1, 2], logs_cleaned_at=123.0)
    timestamp = log_gc.datetime.fromtimestamp(123.0).strftime(
        '%Y-%m-%d %H:%M:%S')
    assert timestamp in existing.read_text(encoding='utf-8')


def test_task_cleanup_isolates_unlink_failure(monkeypatch, tmp_path, caplog):
    paths = {job_id: tmp_path / str(job_id) / 'run.log' for job_id in (1, 2, 3)}
    for path in paths.values():
        path.parent.mkdir()
        path.write_text('old log', encoding='utf-8')
        task_dir = path.parent / 'tasks'
        task_dir.mkdir()
        (task_dir / 'run.log').write_text('old task log', encoding='utf-8')

    get_logs = mock.Mock(
        return_value=[_task(job_id, path) for job_id, path in paths.items()])
    set_cleaned = mock.Mock()
    monkeypatch.setattr(log_gc.managed_job_state, 'get_task_logs_to_clean',
                        get_logs)
    monkeypatch.setattr(log_gc.managed_job_state, 'set_task_logs_cleaned',
                        set_cleaned)
    original_unlink = pathlib.Path.unlink

    def _unlink(path, *args, **kwargs):
        if path == paths[2]:
            raise OSError('permission denied')
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, 'unlink', _unlink)

    failed: set = set()
    assert log_gc._clean_task_logs_with_retention(60,
                                                  batch_size=10,
                                                  failed_tasks=failed)

    get_logs.assert_called_once_with(60, batch_size=10, exclude_tasks=failed)
    assert failed == {(2, 0)}
    set_cleaned.assert_called_once()
    assert set_cleaned.call_args.kwargs['tasks'] == [(1, 0), (3, 0)]
    assert 'Failed to clean task logs for job 2, task 0' in caplog.text
    assert paths[2].exists()
    assert not paths[3].exists()


def test_task_cleanup_observes_directory_failure_and_missing_is_success(
        monkeypatch, tmp_path, caplog):
    failed = tmp_path / 'failed' / 'run.log'
    succeeded = tmp_path / 'succeeded' / 'run.log'
    missing = tmp_path / 'missing' / 'run.log'
    for path in (failed, succeeded):
        path.parent.mkdir()
        path.write_text('old log', encoding='utf-8')
        task_dir = path.parent / 'tasks'
        task_dir.mkdir()
        (task_dir / 'run.log').write_text('old task log', encoding='utf-8')

    get_logs = mock.Mock(return_value=[
        _task(1, failed),
        _task(2, missing),
        _task(3, succeeded),
    ])
    set_cleaned = mock.Mock()
    monkeypatch.setattr(log_gc.managed_job_state, 'get_task_logs_to_clean',
                        get_logs)
    monkeypatch.setattr(log_gc.managed_job_state, 'set_task_logs_cleaned',
                        set_cleaned)
    original_rmtree = shutil.rmtree
    calls = []

    def _rmtree(path, *, ignore_errors=False):
        path = pathlib.Path(path)
        calls.append((path, ignore_errors))
        if path == failed.parent / 'tasks':
            raise OSError('filesystem busy')
        return original_rmtree(path, ignore_errors=ignore_errors)

    monkeypatch.setattr(log_gc.shutil, 'rmtree', _rmtree)

    assert log_gc._clean_task_logs_with_retention(60, batch_size=10)

    set_cleaned.assert_called_once()
    assert set_cleaned.call_args.kwargs['tasks'] == [(2, 0), (3, 0)]
    assert all(not ignore_errors for _, ignore_errors in calls)
    assert 'Failed to clean task logs for job 1, task 0' in caplog.text
    assert (failed.parent / 'tasks').exists()
    assert not (succeeded.parent / 'tasks').exists()


def test_controller_cleanup_all_failed_full_batch_pages_past_failures(
        monkeypatch, tmp_path):
    paths = {job_id: tmp_path / f'{job_id}.log' for job_id in (1, 2)}
    for path in paths.values():
        path.write_text('old log', encoding='utf-8')
    get_logs = mock.Mock(return_value=[{'job_id': job_id} for job_id in paths])
    set_cleaned = mock.Mock()
    monkeypatch.setattr(log_gc.managed_job_state,
                        'get_controller_logs_to_clean', get_logs)
    monkeypatch.setattr(log_gc.managed_job_state, 'set_controller_logs_cleaned',
                        set_cleaned)
    monkeypatch.setattr(log_gc.managed_job_utils, 'controller_log_file_for_job',
                        lambda job_id: str(paths[job_id]))

    original_open = builtins.open

    def _open(path, *args, **kwargs):
        if pathlib.Path(path) in paths.values():
            raise OSError('read-only filesystem')
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, 'open', _open)

    # A full batch where every row failed continues the pass, but the failed
    # rows are recorded so the next round selects past them instead of
    # re-attempting the same rows in a sleepless loop.
    failed: set = set()
    assert not log_gc._clean_controller_logs_with_retention(
        60, batch_size=2, failed_job_ids=failed)
    assert set_cleaned.call_args.kwargs['job_ids'] == []
    assert failed == set(paths)

    # The next round excludes the failed rows; a short batch ends the pass.
    get_logs.return_value = []
    assert log_gc._clean_controller_logs_with_retention(60,
                                                        batch_size=2,
                                                        failed_job_ids=failed)
    assert get_logs.call_args.kwargs['exclude_job_ids'] == failed


def test_task_cleanup_all_failed_full_batch_pages_past_failures(
        monkeypatch, tmp_path):
    log_files = [tmp_path / f'{job_id}' / 'run.log' for job_id in (1, 2)]
    for log_file in log_files:
        log_file.parent.mkdir()
        log_file.write_text('old log', encoding='utf-8')
    get_tasks = mock.Mock(return_value=[
        _task(idx + 1, log_file) for idx, log_file in enumerate(log_files)
    ])
    set_cleaned = mock.Mock()
    monkeypatch.setattr(log_gc.managed_job_state, 'get_task_logs_to_clean',
                        get_tasks)
    monkeypatch.setattr(log_gc.managed_job_state, 'set_task_logs_cleaned',
                        set_cleaned)
    monkeypatch.setattr(
        pathlib.Path, 'unlink',
        mock.Mock(side_effect=PermissionError('operation not permitted')))

    failed: set = set()
    assert not log_gc._clean_task_logs_with_retention(
        60, batch_size=2, failed_tasks=failed)
    assert set_cleaned.call_args.kwargs['tasks'] == []
    assert failed == {(1, 0), (2, 0)}

    get_tasks.return_value = []
    assert log_gc._clean_task_logs_with_retention(60,
                                                  batch_size=2,
                                                  failed_tasks=failed)
    assert get_tasks.call_args.kwargs['exclude_tasks'] == failed


def test_controller_cleanup_stuck_row_attempted_once_per_pass(
        monkeypatch, tmp_path):
    """A persistently failing row is attempted once and then paged past,
    so it cannot starve rows behind it in the same pass."""
    paths = {job_id: tmp_path / f'{job_id}.log' for job_id in (1, 2, 3)}
    for path in paths.values():
        path.write_text('old log', encoding='utf-8')

    # Round 1 selects a full batch (stuck row 1 + row 2); round 2 must be
    # called with row 1 excluded and serves row 3 as a short batch.
    def _get_logs(_retention, batch_size, exclude_job_ids):
        del batch_size
        if not exclude_job_ids:
            return [{'job_id': 1}, {'job_id': 2}]
        assert exclude_job_ids == {1}
        return [{'job_id': 3}]

    set_cleaned = mock.Mock()
    monkeypatch.setattr(log_gc.managed_job_state,
                        'get_controller_logs_to_clean', _get_logs)
    monkeypatch.setattr(log_gc.managed_job_state, 'set_controller_logs_cleaned',
                        set_cleaned)
    monkeypatch.setattr(log_gc.managed_job_utils, 'controller_log_file_for_job',
                        lambda job_id: str(paths[job_id]))

    original_open = builtins.open
    attempts = []

    def _open(path, *args, **kwargs):
        if pathlib.Path(path) == paths[1]:
            attempts.append(path)
            raise OSError('read-only filesystem')
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, 'open', _open)

    failed: set = set()
    assert not log_gc._clean_controller_logs_with_retention(
        60, batch_size=2, failed_job_ids=failed)
    assert log_gc._clean_controller_logs_with_retention(60,
                                                        batch_size=2,
                                                        failed_job_ids=failed)

    assert len(attempts) == 1
    assert failed == {1}
    assert [c.kwargs['job_ids'] for c in set_cleaned.call_args_list] == [[2],
                                                                         [3]]
    assert 'Controller log has been cleaned' in paths[3].read_text(
        encoding='utf-8')
