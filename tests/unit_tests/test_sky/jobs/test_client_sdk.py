"""Unit tests for managed jobs client SDK helpers."""
import gzip
import pickle
from unittest import mock

import pytest

from sky.jobs.client import sdk as jobs_sdk
from sky.jobs.client import sdk_async as jobs_sdk_async


class _Response:
    """Minimal streaming response used by log download tests."""

    def __init__(self,
                 *,
                 chunks=(),
                 headers=None,
                 ok=True,
                 status_code=200,
                 iter_error=None):
        self._chunks = chunks
        self.headers = headers or {}
        self.ok = ok
        self.status_code = status_code
        self.iter_error = iter_error
        self.chunk_sizes = []
        self.close_calls = 0

    def iter_content(self, *, chunk_size):
        self.chunk_sizes.append(chunk_size)
        yield from self._chunks
        if self.iter_error is not None:
            raise self.iter_error

    def close(self):
        self.close_calls += 1


class _ImmediateThread:
    """Runs a background target synchronously for deterministic tests."""

    def __init__(self, *, target, daemon):
        self._target = target
        self.daemon = daemon

    def start(self):
        self._target()


def test_queue_version_2_dispatches_to_queue_v2():
    raw_queue = jobs_sdk.queue.__wrapped__.__wrapped__

    with mock.patch.object(jobs_sdk, 'queue_v2',
                           return_value='request-id-v2') as mock_queue_v2:
        result = raw_queue(refresh=True,
                           skip_finished=True,
                           all_users=True,
                           job_ids=[1, 2],
                           version=2)

    assert result == 'request-id-v2'
    mock_queue_v2.assert_called_once_with(refresh=True,
                                          skip_finished=True,
                                          all_users=True,
                                          job_ids=[1, 2])


def test_queue_version_1_warns_and_uses_legacy_endpoint():
    raw_queue = jobs_sdk.queue.__wrapped__.__wrapped__

    with mock.patch.object(jobs_sdk.server_common,
                           'make_authenticated_request',
                           return_value='response') as mock_request, \
         mock.patch.object(jobs_sdk.server_common,
                           'get_request_id',
                           return_value='request-id-v1') as mock_get_request_id, \
         mock.patch.object(jobs_sdk.logger, 'warning') as mock_warning:
        result = raw_queue(refresh=False,
                           skip_finished=True,
                           all_users=False,
                           job_ids=[3],
                           version=1)

    assert result == 'request-id-v1'
    mock_warning.assert_called_once()
    assert 'is deprecated and will be removed in v0.13' in mock_warning.call_args.args[
        0]
    mock_request.assert_called_once()
    args, kwargs = mock_request.call_args
    assert args == ('POST', '/jobs/queue')
    assert kwargs['json']['refresh'] is False
    assert kwargs['json']['skip_finished'] is True
    assert kwargs['json']['all_users'] is False
    assert kwargs['json']['job_ids'] == [3]
    mock_get_request_id.assert_called_once_with(response='response')


def test_queue_invalid_version_raises():
    raw_queue = jobs_sdk.queue.__wrapped__.__wrapped__

    with pytest.raises(ValueError, match='Must be 1 or 2'):
        raw_queue(refresh=False, version=3)


@pytest.mark.asyncio
async def test_async_queue_passes_version_through():

    async def mock_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    with mock.patch('sky.jobs.client.sdk_async.asyncio.to_thread',
                    side_effect=mock_to_thread), \
         mock.patch.object(jobs_sdk, 'queue',
                           return_value='request-id') as mock_queue, \
         mock.patch.object(jobs_sdk_async.sdk_async,
                           '_stream_and_get',
                           new=mock.AsyncMock(
                               return_value='queue-result')) as mock_stream:
        result = await jobs_sdk_async.queue(refresh=True,
                                            skip_finished=False,
                                            all_users=True,
                                            job_ids=[9],
                                            version=2)

    assert result == 'queue-result'
    mock_queue.assert_called_once_with(True, False, True, [9], 2)
    mock_stream.assert_called_once_with(
        'request-id', jobs_sdk_async.sdk_async.DEFAULT_STREAM_CONFIG)


def test_download_logs_streaming_decompresses_and_preserves_layout(tmp_path):
    raw_download = jobs_sdk.download_logs_streaming.__wrapped__.__wrapped__
    compressed = gzip.compress(b'first line\nsecond line\n')
    dispatch = _Response(
        chunks=(b'dispatch',),
        headers={jobs_sdk.server_constants.STREAM_REQUEST_HEADER: 'request-1'})
    stream = _Response(chunks=(compressed[:7], compressed[7:]),
                       headers={'Content-Type': 'application/gzip'})

    with mock.patch.object(jobs_sdk.server_common,
                           'make_authenticated_request',
                           side_effect=(dispatch, stream)) as mock_request, \
         mock.patch.object(jobs_sdk.log_download.threading, 'Thread',
                           side_effect=_ImmediateThread) as mock_thread:
        result = raw_download(name='training',
                              job_id=7,
                              refresh=True,
                              controller=True,
                              local_dir=str(tmp_path))

    expected_dir = tmp_path / 'managed_jobs' / 'managed-controller-7'
    assert result == {7: str(expected_dir)}
    assert (expected_dir /
            'controller.log').read_bytes() == (b'first line\nsecond line\n')
    assert dispatch.chunk_sizes == [64 * 1024]
    assert stream.chunk_sizes == [64 * 1024]
    assert dispatch.close_calls == 1
    assert stream.close_calls == 1
    mock_thread.assert_called_once_with(target=mock.ANY, daemon=True)
    assert mock_request.call_count == 2
    dispatch_call, stream_call = mock_request.call_args_list
    assert dispatch_call.args == ('POST', '/jobs/logs')
    assert {
        key: dispatch_call.kwargs['json'][key] for key in (
            'name',
            'job_id',
            'follow',
            'controller',
            'refresh',
            'tail',
            'tail_offset',
            'task',
        )
    } == {
        'name': 'training',
        'job_id': 7,
        'follow': False,
        'controller': True,
        'refresh': True,
        'tail': None,
        'tail_offset': None,
        'task': None,
    }
    assert dispatch_call.kwargs['stream'] is True
    assert dispatch_call.kwargs['timeout'] == (5, None)
    assert stream_call.args == (
        'GET', '/api/stream?request_id=request-1&format=plain&compress=gz')
    assert stream_call.kwargs == {'stream': True, 'timeout': (5, None)}


def test_download_logs_streaming_plain_latest_and_empty_cleanup(tmp_path):
    raw_download = jobs_sdk.download_logs_streaming.__wrapped__.__wrapped__

    def run(chunks):
        dispatch = _Response(headers={'X-SkyPilot-Request-ID': 'request-2'})
        stream = _Response(chunks=chunks,
                           headers={'Content-Type': 'text/plain'})
        with mock.patch.object(jobs_sdk.server_common,
                               'make_authenticated_request',
                               side_effect=(dispatch, stream)), \
             mock.patch.object(jobs_sdk.log_download.threading, 'Thread',
                               _ImmediateThread):
            result = raw_download(name=None,
                                  job_id=None,
                                  refresh=False,
                                  controller=False,
                                  local_dir=str(tmp_path))
        assert dispatch.close_calls == 1
        assert stream.close_calls == 1
        return result

    expected_dir = tmp_path / 'managed_jobs' / 'managed-job-latest'
    assert run((b'plain log\n',)) == {0: str(expected_dir)}
    assert (expected_dir / 'run.log').read_bytes() == b'plain log\n'
    (expected_dir / 'run.log').unlink()
    expected_dir.rmdir()

    assert run(()) is None
    assert not expected_dir.exists()


@pytest.mark.parametrize(('dispatch', 'stream', 'message'), [
    (_Response(ok=False, status_code=503), None,
     'Failed to dispatch /jobs/logs: HTTP 503'),
    (_Response(), None,
     '/jobs/logs response missing X-SkyPilot-Request-ID header'),
    (_Response(headers={'X-SkyPilot-Request-ID': 'request-3'}),
     _Response(ok=False,
               status_code=502), 'Failed to attach to /api/stream: HTTP 502'),
])
def test_download_logs_streaming_transport_errors(tmp_path, dispatch, stream,
                                                  message):
    raw_download = jobs_sdk.download_logs_streaming.__wrapped__.__wrapped__
    responses = (dispatch,) if stream is None else (dispatch, stream)

    with mock.patch.object(jobs_sdk.server_common,
                           'make_authenticated_request',
                           side_effect=responses), \
         mock.patch.object(jobs_sdk.log_download.threading, 'Thread',
                           side_effect=_ImmediateThread) as mock_thread, \
         pytest.raises(RuntimeError, match=message):
        raw_download(name='job',
                     job_id=1,
                     refresh=False,
                     controller=False,
                     local_dir=str(tmp_path))
    assert dispatch.close_calls == 1
    if stream is not None:
        assert stream.close_calls == 1
    mock_thread.assert_not_called()


def test_download_logs_streaming_attach_error_cancels_dispatch(tmp_path):
    raw_download = jobs_sdk.download_logs_streaming.__wrapped__.__wrapped__
    dispatch = _Response(
        headers={'X-SkyPilot-Request-ID': 'request-attach-error'})

    with mock.patch.object(jobs_sdk.server_common,
                           'make_authenticated_request',
                           side_effect=(dispatch, OSError('attach failed'))), \
         mock.patch.object(jobs_sdk.log_download.threading,
                           'Thread') as mock_thread, \
         pytest.raises(OSError, match='attach failed'):
        raw_download(name='job',
                     job_id=1,
                     refresh=False,
                     controller=False,
                     local_dir=str(tmp_path))

    assert dispatch.close_calls == 1
    mock_thread.assert_not_called()


def test_download_logs_streaming_thread_start_error_closes_responses(tmp_path):
    raw_download = jobs_sdk.download_logs_streaming.__wrapped__.__wrapped__
    dispatch = _Response(
        headers={'X-SkyPilot-Request-ID': 'request-thread-error'})
    stream = _Response(headers={'Content-Type': 'text/plain'})
    thread = mock.Mock()
    thread.start.side_effect = RuntimeError('thread start failed')

    with mock.patch.object(jobs_sdk.server_common,
                           'make_authenticated_request',
                           side_effect=(dispatch, stream)) as mock_request, \
         mock.patch.object(jobs_sdk.log_download.threading,
                           'Thread', return_value=thread), \
         pytest.raises(RuntimeError, match='thread start failed'):
        raw_download(name='job',
                     job_id=1,
                     refresh=False,
                     controller=False,
                     local_dir=str(tmp_path))

    assert mock_request.call_count == 2
    assert dispatch.close_calls == 1
    assert stream.close_calls == 1


def test_download_logs_streaming_failure_removes_partial_file(tmp_path):
    raw_download = jobs_sdk.download_logs_streaming.__wrapped__.__wrapped__
    dispatch = _Response(headers={'X-SkyPilot-Request-ID': 'request-partial'})
    stream = _Response(chunks=(b'partial log\n',),
                       headers={'Content-Type': 'text/plain'},
                       iter_error=OSError('stream interrupted'))

    with mock.patch.object(jobs_sdk.server_common,
                           'make_authenticated_request',
                           side_effect=(dispatch, stream)), \
         mock.patch.object(jobs_sdk.log_download.threading, 'Thread',
                           _ImmediateThread), \
         pytest.raises(OSError, match='stream interrupted'):
        raw_download(name='job',
                     job_id=1,
                     refresh=False,
                     controller=False,
                     local_dir=str(tmp_path))

    job_dir = tmp_path / 'managed_jobs' / 'managed-job-1'
    assert not job_dir.exists()
    assert dispatch.close_calls == 1
    assert stream.close_calls == 1


def test_download_logs_maps_remote_paths_to_local_paths():
    raw_download = jobs_sdk.download_logs.__wrapped__.__wrapped__
    remote_paths = {'7': '/remote/job-7', '9': '/remote/job-9'}
    local_paths = {
        '/remote/job-7': '/local/job-7',
        '/remote/job-9': '/local/job-9',
    }

    with mock.patch.object(jobs_sdk.server_common,
                           'make_authenticated_request',
                           return_value='response') as mock_request, \
         mock.patch.object(jobs_sdk.server_common,
                           'get_request_id',
                           return_value='request-id') as mock_get_request_id, \
         mock.patch.object(jobs_sdk.sdk,
                           'stream_and_get',
                           return_value=remote_paths) as mock_stream, \
         mock.patch.object(jobs_sdk.client_common,
                           'download_logs_from_api_server',
                           return_value=local_paths) as mock_download:
        result = raw_download(name='job',
                              job_id=7,
                              refresh=True,
                              controller=False,
                              local_dir='/logs')

    assert result == {7: '/local/job-7', 9: '/local/job-9'}
    request_call = mock_request.call_args
    assert request_call.args == ('POST', '/jobs/download_logs')
    assert {
        key: request_call.kwargs['json'][key]
        for key in ('name', 'job_id', 'refresh', 'controller', 'local_dir')
    } == {
        'name': 'job',
        'job_id': 7,
        'refresh': True,
        'controller': False,
        'local_dir': '/logs',
    }
    assert request_call.kwargs['timeout'] == (5, None)
    mock_get_request_id.assert_called_once_with('response')
    mock_stream.assert_called_once_with('request-id')
    mock_download.assert_called_once()
    assert list(mock_download.call_args.args[0]) == list(remote_paths.values())


@pytest.mark.parametrize('function_name',
                         ['download_logs_streaming', 'download_logs'])
def test_download_log_facade_identity(function_name):
    function = getattr(jobs_sdk, function_name)
    assert function.__module__ == 'sky.jobs.client.sdk'
    assert pickle.loads(pickle.dumps(function)) is function
