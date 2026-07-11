"""Focused tests for Sky Batch recovery and bounded-memory reduction."""

# pylint: disable=protected-access,redefined-outer-name

import importlib
import shutil
import types
from unittest import mock

from alembic import migration
from alembic import operations
import pytest
import sqlalchemy

from sky.batch import coordinator
from sky.batch import io_formats
from sky.batch import utils
from sky.batch import worker
from sky.jobs import controller as jobs_controller
from sky.jobs import state
from sky.utils.db import migration_utils


@pytest.fixture
def batch_state_db(tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(
        f'sqlite:///{tmp_path / "batch-state.db"}')
    monkeypatch.setattr(state._db_manager, '_engine', engine)
    state.Base.metadata.create_all(engine)
    yield
    engine.dispose()


def test_batch_attempt_fences_stale_transitions_and_persists_backoff(
        batch_state_db):
    del batch_state_db
    state.save_batch_states(7, [[0, 9]])

    attempt_1 = state.claim_batch(7, 0, 'worker-a', lease_duration=10, now=100)
    assert attempt_1 == 1
    assert state.claim_batch(7, 0, 'worker-b', lease_duration=10,
                             now=100) is None
    assert not state.set_batch_attempt_status(7, 0, 2, 'COMPLETED', now=101)
    assert state.renew_batch_lease(7, 0, 1, lease_duration=10, now=105)

    assert state.set_batch_attempt_status(7,
                                          0,
                                          1,
                                          'PENDING',
                                          retry_count=1,
                                          next_retry_at=120,
                                          now=106)
    assert state.claim_batch(7, 0, 'worker-b', lease_duration=10,
                             now=119) is None
    attempt_2 = state.claim_batch(7, 0, 'worker-b', lease_duration=10, now=120)
    assert attempt_2 == 2
    assert not state.set_batch_attempt_status(7, 0, 1, 'COMPLETED', now=121)
    assert state.set_batch_attempt_status(7, 0, 2, 'COMPLETED', now=121)

    record = state.get_batch_states(7)[0]
    assert record['status'] == 'COMPLETED'
    assert record['attempt_id'] == 2
    assert record['retry_count'] == 1
    assert record['lease_expires_at'] is None
    assert record['next_retry_at'] is None


def test_expired_batch_attempt_is_reclaimed_once(batch_state_db):
    del batch_state_db
    state.save_batch_states(8, [[0, 4]])
    assert state.claim_batch(8, 0, 'worker-a', lease_duration=10, now=100) == 1

    assert not state.requeue_expired_batch_attempts(8, now=109)
    assert state.requeue_expired_batch_attempts(8, now=110) == [0]
    assert not state.requeue_expired_batch_attempts(8, now=110)
    assert state.claim_batch(8, 0, 'worker-b', lease_duration=10, now=110) == 2


def test_schema_022_upgrades_existing_batch_state_table(tmp_path):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "old.db"}')
    old_metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        'batch_state', old_metadata,
        sqlalchemy.Column('job_id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('batch_idx', sqlalchemy.Integer, primary_key=True))
    old_metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            sqlalchemy.text(
                'INSERT INTO batch_state (job_id, batch_idx) VALUES (1, 0)'))

    schema_022 = importlib.import_module(
        'sky.schemas.db.spot_jobs.022_add_batch_attempt_leases')
    with engine.connect() as connection:
        context = migration.MigrationContext.configure(connection)
        with operations.Operations.context(context):
            schema_022.upgrade()

    columns = {
        column['name']
        for column in sqlalchemy.inspect(engine).get_columns('batch_state')
    }
    assert {'attempt_id', 'lease_expires_at', 'next_retry_at'} <= columns
    with engine.connect() as connection:
        attempt_id = connection.execute(
            sqlalchemy.text('SELECT attempt_id FROM batch_state')).scalar_one()
    assert attempt_id == 0


def test_spot_jobs_database_targets_batch_attempt_migration(
        tmp_path, monkeypatch):
    engine = sqlalchemy.create_engine(f'sqlite:///{tmp_path / "target.db"}')
    upgrade = mock.Mock()
    monkeypatch.setattr(migration_utils, 'safe_alembic_upgrade', upgrade)

    state.create_table(engine)

    upgrade.assert_called_once_with(engine, migration_utils.SPOT_JOBS_DB_NAME,
                                    '022')
    assert migration_utils.SPOT_JOBS_VERSION == '022'
    engine.dispose()


def _make_coordinator(job_id=1):
    return coordinator.BatchCoordinator(dataset_path='s3://bucket/input.jsonl',
                                        output_path='s3://bucket/output.jsonl',
                                        batch_size=4,
                                        pool_name='pool',
                                        serialized_fn='serialized',
                                        input_format_dict={
                                            'format': 'json',
                                            'path': 's3://bucket/input.jsonl',
                                        },
                                        output_formats_dict=[{
                                            'format': 'json',
                                            'path': 's3://bucket/output.jsonl',
                                        }],
                                        job_id=job_id)


def test_inline_coordinator_does_not_replace_process_signal_handler():
    with mock.patch.object(coordinator.signal, 'signal') as install_handler:
        _make_coordinator()
    install_handler.assert_not_called()


def test_custom_writer_without_attempt_hooks_fails_before_dispatch():

    class _LegacyWriter(io_formats.OutputWriter):
        """Writer implementing only the pre-fencing contract."""

        def upload_batch(self, results, start_idx, end_idx, job_id):
            del results, start_idx, end_idx, job_id
            return self.path

        def reduce_results(self, job_id):
            del job_id

        def cleanup(self, job_id):
            del job_id

    writer = _LegacyWriter('s3://bucket/output')
    with pytest.raises(ValueError, match='upload_batch_attempt'):
        writer.validate_attempt_fencing()


def test_coordinator_rejects_pool_with_old_worker_runtime(monkeypatch):
    batch_coordinator = _make_coordinator()
    monkeypatch.setattr(
        batch_coordinator, '_fetch_pool_status', lambda: {
            'replica_info': [{
                'name': 'old-worker',
                'status': 'READY',
                'replica_info_version': 5,
            }]
        })

    with pytest.raises(RuntimeError, match='Recreate pool'):
        batch_coordinator._get_ready_workers()


def test_pending_queue_honors_retry_time(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator._enqueue_batch(3, ready_at=120)

    monkeypatch.setattr(coordinator.time, 'time', lambda: 119)
    assert batch_coordinator._pop_ready_batch() == (None, 1)
    monkeypatch.setattr(coordinator.time, 'time', lambda: 120)
    assert batch_coordinator._pop_ready_batch() == (3, 0)


def test_worker_commands_are_scoped_to_coordinator_token():
    batch_coordinator = _make_coordinator()
    batch_coordinator.batches = [[0, 3]]

    notify_code = batch_coordinator._generate_notify_code(0, attempt_id=4)
    shutdown_code = batch_coordinator._generate_shutdown_code()
    token_header = ('X-Sky-Batch-Worker-Token: '
                    f'{batch_coordinator._worker_token}')
    assert token_header in notify_code
    assert token_header in shutdown_code
    assert '"attempt_id": 4' in notify_code


def test_worker_rejects_control_from_stale_coordinator(monkeypatch):
    monkeypatch.setattr(worker, '_worker_token', 'current-token')
    handler = object.__new__(worker._WorkerHandler)
    handler.headers = {'X-Sky-Batch-Worker-Token': 'stale-token'}
    handler._send_json = mock.Mock()

    assert not handler._is_authorized()
    handler._send_json.assert_called_once_with(
        409, {'error': 'stale batch coordinator'})


def test_worker_shutdown_cancels_only_owned_job(monkeypatch):
    batch_coordinator = _make_coordinator()
    monkeypatch.setattr(coordinator.sdk, 'exec', mock.Mock(return_value='exec'))
    monkeypatch.setattr(coordinator.sdk, 'get', mock.Mock())
    cancel = mock.Mock(return_value='cancel')
    monkeypatch.setattr(coordinator.sdk, 'cancel', cancel)
    monkeypatch.setattr(coordinator.time, 'sleep', mock.Mock())

    batch_coordinator._shutdown_worker('worker-a', worker_job_id=17)

    cancel.assert_called_once_with('worker-a', job_ids=[17])


def test_expired_worker_batch_rejects_late_save(monkeypatch):
    item = worker._BatchItem([{'value': 1}], 0, 0, 0)
    monkeypatch.setattr(worker, '_current_batch', item)
    worker._expire_batch(item, 'timed out')

    assert item.done_event.is_set()
    assert item.error == 'timed out'
    assert worker._current_batch is None
    with pytest.raises(RuntimeError, match='without a current batch'):
        worker.save_results([{'value': 2}])


def test_worker_uploads_to_attempt_scoped_writer(monkeypatch):
    item = worker._BatchItem([{'value': 1}], 0, 0, 0, attempt_id=7)
    output_writer = mock.Mock()
    output_writer.upload_batch_attempt.return_value = 'attempt-path'
    monkeypatch.setattr(worker, '_current_batch', item)
    monkeypatch.setattr(worker, '_output_formats', [output_writer])
    monkeypatch.setattr(worker, '_job_id', 'job-1')

    worker.save_results([{'value': 2}])

    output_writer.upload_batch_attempt.assert_called_once_with([{
        'value': 2
    }], 0, 0, 'job-1', 7)


def test_json_writer_reduces_only_completed_attempts(monkeypatch):
    writer = io_formats.JsonWriter('s3://bucket/output.jsonl')
    save = mock.Mock()
    monkeypatch.setattr(utils, 'save_jsonl_to_cloud', save)

    attempt_path = writer.upload_batch_attempt([{'value': 1}], 0, 3, 'job-1', 4)
    assert attempt_path == ('s3://bucket/.sky_batch_tmp/job-1/attempts/4/'
                            'batch_00000000-00000003.jsonl')
    save.assert_called_once_with([{'value': 1}], attempt_path)

    concatenate = mock.Mock()
    monkeypatch.setattr(utils, 'concatenate_batch_files_to_output', concatenate)
    writer.reduce_attempt_results('job-1', [(0, 3, 4), (4, 5, 2)])
    concatenate.assert_called_once_with('s3://bucket/output.jsonl', [
        ('s3://bucket/.sky_batch_tmp/job-1/attempts/4/'
         'batch_00000000-00000003.jsonl'),
        ('s3://bucket/.sky_batch_tmp/job-1/attempts/2/'
         'batch_00000004-00000005.jsonl'),
    ])


def test_image_writer_promotes_only_winning_attempt(monkeypatch):

    class _Image:

        def save(self, buffer, format):  # pylint: disable=redefined-builtin
            assert format == 'PNG'
            buffer.write(b'png')

    writer = io_formats.ImageWriter('s3://bucket/images/')
    upload = mock.Mock()
    copy = mock.Mock()
    delete = mock.Mock()
    monkeypatch.setattr(utils, 'upload_bytes_to_cloud', upload)
    monkeypatch.setattr(utils, 'copy_cloud_file', copy)
    monkeypatch.setattr(utils, 'delete_cloud_prefix', delete)

    writer.upload_batch_attempt([{'image': _Image()}], 3, 3, 'job-1', 5)
    attempt_path = ('s3://bucket/images/.sky_batch_tmp/job-1/attempts/5/images/'
                    '00000003.png')
    upload.assert_called_once_with(b'png', attempt_path)

    writer.reduce_attempt_results('job-1', [(3, 3, 5), (4, 4, 0)])
    copy.assert_called_once_with(attempt_path,
                                 's3://bucket/images/00000003.png')
    writer.cleanup('job-1')
    delete.assert_called_once_with('s3://bucket/images/.sky_batch_tmp/job-1/')


def test_coordinator_reduces_winners_before_separate_cleanup(monkeypatch):
    batch_coordinator = _make_coordinator()
    batch_coordinator.batches = [[0, 3], [4, 5]]
    output_writer = mock.Mock()
    output_writer.path = 's3://bucket/output.jsonl'
    batch_coordinator._output_formats = [output_writer]
    monkeypatch.setattr(
        coordinator.managed_job_state, 'get_batch_states',
        mock.Mock(return_value=[{
            'batch_idx': 0,
            'start_idx': 0,
            'end_idx': 3,
            'status': 'COMPLETED',
            'attempt_id': 4,
        }, {
            'batch_idx': 1,
            'start_idx': 4,
            'end_idx': 5,
            'status': 'COMPLETED',
            'attempt_id': 2,
        }]))

    batch_coordinator._reduce_results()

    output_writer.reduce_attempt_results.assert_called_once_with(
        '1', [(0, 3, 4), (4, 5, 2)])
    output_writer.cleanup.assert_not_called()

    batch_coordinator.cleanup()
    output_writer.cleanup.assert_called_once_with('1')


@pytest.mark.asyncio
async def test_batch_cleanup_runs_after_durable_success(monkeypatch):
    events = []
    batch_coordinator = mock.Mock()
    batch_coordinator.run.side_effect = lambda: events.append('run')
    batch_coordinator.cleanup.side_effect = lambda: events.append('cleanup')
    monkeypatch.setattr(jobs_controller.batch_coordinator, 'BatchCoordinator',
                        mock.Mock(return_value=batch_coordinator))
    monkeypatch.setattr(
        jobs_controller.managed_job_state, 'get_latest_task_id_status_async',
        mock.AsyncMock(return_value=(0, state.ManagedJobStatus.RUNNING)))

    async def _set_succeeded(**kwargs):
        del kwargs
        events.append('succeeded')

    monkeypatch.setattr(jobs_controller.managed_job_state,
                        'set_succeeded_async', _set_succeeded)
    task = mock.Mock()
    task.metadata = {
        'batch_dataset_path': 's3://bucket/input.jsonl',
        'batch_output_path': 's3://bucket/output.jsonl',
        'batch_size': 4,
        'batch_pool_name': 'pool',
        'batch_serialized_fn': 'serialized',
        'batch_input_format': {},
        'batch_output_formats': [],
    }
    controller_instance = types.SimpleNamespace(_job_id=1)

    succeeded = await jobs_controller.JobController._run_batch_coordinator_task(
        controller_instance,
        task_id=0,
        task=task,
        callback_func=mock.Mock(),
        is_resume=True)

    assert succeeded
    assert events == ['run', 'succeeded', 'cleanup']


def test_json_reduction_streams_files_without_loading_rows(
        tmp_path, monkeypatch):
    first = tmp_path / 'first.jsonl'
    second = tmp_path / 'second.jsonl'
    first.write_bytes(b'{"idx": 0}\n{"idx": 1}\n')
    second.write_bytes(b'{"idx": 2}\n')
    cloud_files = {
        's3://bucket/first.jsonl': first,
        's3://bucket/second.jsonl': second,
    }
    uploaded = tmp_path / 'uploaded.jsonl'

    monkeypatch.setattr(utils, 'list_batch_files', lambda *_: list(cloud_files))

    def _download(cloud_path, local_path):
        shutil.copyfile(cloud_files[cloud_path], local_path)

    def _upload(local_path, cloud_path):
        del cloud_path
        shutil.copyfile(local_path, uploaded)

    monkeypatch.setattr(utils, 'download_file_from_cloud', _download)
    monkeypatch.setattr(utils, 'upload_file_to_cloud', _upload)
    monkeypatch.setattr(
        utils, 'load_jsonl_from_cloud',
        mock.Mock(side_effect=AssertionError('must not load batches into RAM')))

    utils.concatenate_batches_to_output('s3://bucket/output.jsonl', 'job-1')

    assert uploaded.read_bytes() == first.read_bytes() + second.read_bytes()


def test_s3_attempt_cleanup_deletes_one_page_at_a_time(monkeypatch):
    paginator = mock.Mock()
    paginator.paginate.return_value = [{
        'Contents': [{
            'Key': 'tmp/a'
        }, {
            'Key': 'tmp/b'
        }]
    }, {
        'Contents': [{
            'Key': 'tmp/c'
        }]
    }]
    s3 = mock.Mock()
    s3.get_paginator.return_value = paginator
    monkeypatch.setattr(utils.aws, 'client', mock.Mock(return_value=s3))

    utils.delete_cloud_prefix('s3://bucket/tmp/')

    assert s3.delete_objects.call_count == 2
    s3.delete_objects.assert_any_call(Bucket='bucket',
                                      Delete={
                                          'Objects': [{
                                              'Key': 'tmp/a'
                                          }, {
                                              'Key': 'tmp/b'
                                          }],
                                          'Quiet': True,
                                      })
