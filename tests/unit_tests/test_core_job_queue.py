"""Focused tests for cluster job queue decoding."""
# pylint: disable=protected-access
from types import SimpleNamespace
from unittest import mock

from sky import core
from sky import exceptions
from sky.schemas.generated import jobsv1_pb2
from sky.skylet import job_lib
from sky.utils import message_utils


def _job(job_id: int, user_id: str, *, include_timestamps: bool = False):
    kwargs = {
        'job_id': job_id,
        'job_name': f'job-{job_id}',
        'username': user_id,
        'submitted_at': float(job_id),
        'status': jobsv1_pb2.JOB_STATUS_RUNNING,
        'run_timestamp': f'run-{job_id}',
        'resources': '1x CPU',
        'log_path': f'/logs/{job_id}',
    }
    if include_timestamps:
        kwargs.update(start_at=10.0, end_at=20.0)
    return jobsv1_pb2.JobInfo(**kwargs)


def _grpc_queue(response):
    handle = SimpleNamespace(
        is_grpc_enabled_with_flag=True,
        cluster_name='test-cluster',
        get_grpc_channel=mock.Mock(return_value='channel'),
    )
    client = mock.Mock()
    client.get_job_queue.return_value = response
    return handle, client


def _encoded_queue_payload():
    return message_utils.encode_payload([{
        'job_id': 1,
        'job_name': 'job-1',
        'username': 'user-a',
        'submitted_at': 1.0,
        'status': 'RUNNING',
        'run_timestamp': 'run-1',
        'start_at': 10.0,
        'end_at': 20.0,
        'resources': '1x CPU',
        'log_path': '/logs/1',
    }, {
        'job_id': 2,
        'job_name': 'job-2',
        'username': 'user-a',
        'submitted_at': 2.0,
        'status': 'RUNNING',
        'run_timestamp': 'run-2',
        'start_at': None,
        'end_at': None,
        'resources': '1x CPU',
        'log_path': '/logs/2',
    }, {
        'job_id': 3,
        'job_name': 'job-3',
        'username': 'missing',
        'submitted_at': 3.0,
        'status': 'SUCCEEDED',
        'run_timestamp': 'run-3',
        'start_at': None,
        'end_at': None,
        'resources': '1x CPU',
        'log_path': '/logs/3',
    }])


def test_load_job_queue_batches_user_resolution_and_preserves_jobs():
    users = {'user-a': SimpleNamespace(name='Alice')}

    with mock.patch.object(job_lib.global_user_state,
                           'get_users',
                           return_value=users) as get_users, \
         mock.patch.object(job_lib.global_user_state,
                           'get_user',
                           side_effect=users.get) as get_user:
        jobs = job_lib.load_job_queue(_encoded_queue_payload())

    get_users.assert_called_once_with({'user-a', 'missing'})
    get_user.assert_not_called()
    assert [job['job_id'] for job in jobs] == [1, 2, 3]
    assert [job['username'] for job in jobs] == ['Alice', 'Alice', None]
    assert [job['user_hash'] for job in jobs] == ['user-a', 'user-a', 'missing']
    assert jobs[0]['status'] is job_lib.JobStatus.RUNNING
    assert jobs[2]['status'] is job_lib.JobStatus.SUCCEEDED
    assert jobs[0]['start_at'] == 10.0
    assert jobs[0]['end_at'] == 20.0
    assert jobs[1]['start_at'] is None
    assert jobs[1]['end_at'] is None


def test_grpc_queue_batches_user_resolution_and_preserves_jobs():
    response = jobsv1_pb2.GetJobQueueResponse(jobs=[
        _job(1, 'user-a', include_timestamps=True),
        _job(2, 'user-a'),
        _job(3, 'missing'),
        _job(4, ''),
    ])
    handle, client = _grpc_queue(response)
    users = {'user-a': SimpleNamespace(name='Alice')}

    with mock.patch.object(core.cloud_vm_ray_backend,
                           'SkyletClient',
                           return_value=client), \
         mock.patch.object(core.global_user_state,
                           'get_users',
                           return_value=users) as get_users, \
         mock.patch.object(
             core.global_user_state,
             'get_user',
             side_effect=users.get) as get_user:
        jobs = core._get_job_queue(handle, mock.Mock(), 'current-user', True)

    get_users.assert_called_once_with({'user-a', 'missing'})
    get_user.assert_not_called()
    assert [job['job_id'] for job in jobs] == [1, 2, 3, 4]
    assert [job['username'] for job in jobs] == ['Alice', 'Alice', None, None]
    assert [job['user_hash'] for job in jobs
           ] == ['user-a', 'user-a', 'missing', '']
    assert jobs[0]['status'] is job_lib.JobStatus.RUNNING
    assert jobs[0]['start_at'] == 10.0
    assert jobs[0]['end_at'] == 20.0
    assert jobs[1]['start_at'] is None
    assert jobs[1]['end_at'] is None


def test_grpc_queue_skips_user_read_for_empty_response():
    handle, client = _grpc_queue(jobsv1_pb2.GetJobQueueResponse())

    with mock.patch.object(core.cloud_vm_ray_backend,
                           'SkyletClient',
                           return_value=client), \
         mock.patch.object(core.global_user_state, 'get_users') as get_users, \
         mock.patch.object(core.global_user_state, 'get_user') as get_user:
        jobs = core._get_job_queue(handle, mock.Mock(), None, False)

    assert jobs == []
    get_users.assert_not_called()
    get_user.assert_not_called()


def test_grpc_queue_failure_still_falls_back_to_ssh():
    handle, client = _grpc_queue(jobsv1_pb2.GetJobQueueResponse())
    client.get_job_queue.side_effect = exceptions.SkyletUnavailableError(
        'unavailable')
    backend = mock.Mock()
    backend.run_on_head.return_value = (0, _encoded_queue_payload(), '')
    users = {'user-a': SimpleNamespace(name='Alice')}

    with mock.patch.object(core.cloud_vm_ray_backend,
                           'SkyletClient',
                           return_value=client), \
         mock.patch.object(core.global_user_state,
                           'get_users',
                           return_value=users) as get_users, \
         mock.patch.object(core.global_user_state,
                           'get_user',
                           side_effect=users.get) as get_user:
        jobs = core._get_job_queue(handle, backend, 'current-user', True)

    get_users.assert_called_once_with({'user-a', 'missing'})
    get_user.assert_not_called()
    assert [job['job_id'] for job in jobs] == [1, 2, 3]
    assert [job['username'] for job in jobs] == ['Alice', 'Alice', None]
