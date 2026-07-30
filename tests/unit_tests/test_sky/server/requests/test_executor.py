"""Unit tests for sky.server.requests.executor module."""
# pylint: disable=import-outside-toplevel,missing-class-docstring
# pylint: disable=protected-access,redefined-outer-name,reimported
# pylint: disable=unused-argument,unused-import
import asyncio
import concurrent.futures
import functools
import os
import pathlib
import queue as queue_lib
import threading
import time
from unittest import mock

import fastapi
import pytest

from sky import exceptions
from sky import models
from sky import skypilot_config
from sky.container_images import errors as container_image_errors
from sky.serve import constants as serve_constants
from sky.server import config as server_config
from sky.server import constants as server_constants
from sky.server import daemons as server_daemons
from sky.server.requests import continue_condition as continue_condition_lib
from sky.server.requests import executor
from sky.server.requests import payloads
from sky.server.requests import process
from sky.server.requests import request_names
from sky.server.requests import requests as requests_lib
from sky.server.requests import role_filter
from sky.skylet import constants
from sky.utils import context_utils


def _make_server_config(
        queue_backend: server_config.QueueBackend
) -> server_config.ServerConfig:
    worker_config = server_config.WorkerConfig(garanteed_parallelism=1,
                                               burstable_parallelism=0,
                                               num_db_connections_per_worker=0)
    return server_config.ServerConfig(num_server_workers=1,
                                      long_worker_config=worker_config,
                                      short_worker_config=worker_config,
                                      num_db_connections_per_worker=0,
                                      queue_backend=queue_backend)


def test_queue_backend_factory_accessors_distinguish_default(monkeypatch):
    monkeypatch.setattr(executor.queue_base, '_queue_backend_factory', None)
    monkeypatch.delenv('SKYPILOT_API_REQUEST_BACKEND', raising=False)

    assert executor.queue_base.get_registered_queue_backend_factory() is None
    assert isinstance(executor.queue_base.get_queue_backend_factory(),
                      executor.queue_base.MultiprocessingQueueFactory)

    registered_factory = mock.MagicMock()
    monkeypatch.setattr(executor.queue_base, '_queue_backend_factory',
                        registered_factory)

    assert (executor.queue_base.get_registered_queue_backend_factory()
            is registered_factory)
    assert executor.queue_base.get_queue_backend_factory() is registered_factory


def test_spawned_process_resolves_postgres_queue_from_environment(monkeypatch):
    from sky.server.requests import postgres as request_postgres

    monkeypatch.setattr(executor.queue_base, '_queue_backend_factory', None)
    monkeypatch.setenv('SKYPILOT_API_REQUEST_BACKEND', 'postgres')

    factory = executor.queue_base.get_queue_backend_factory()

    assert isinstance(factory, request_postgres.PostgresQueueFactory)


@pytest.mark.asyncio
async def test_non_durable_schedule_preserves_legacy_queue_tuple(monkeypatch):
    backend = mock.Mock(uses_durable_queue=False)
    queue = mock.Mock()
    queue.put_async = mock.AsyncMock()
    request = mock.Mock(request_id='legacy-request',
                        schedule_type=requests_lib.ScheduleType.SHORT)
    monkeypatch.setattr(executor.request_storage, 'get_request_backend',
                        lambda: backend)
    monkeypatch.setattr(executor, '_get_queue', lambda _schedule_type: queue)

    await executor.schedule_prepared_request(request,
                                             ignore_return_value=True,
                                             retryable=True)

    queue.put_async.assert_awaited_once_with(('legacy-request', True, True))


@pytest.mark.parametrize(
    ('queue_backend', 'expected_factory_name', 'unexpected_factory_name'), [
        (server_config.QueueBackend.LOCAL, 'LocalQueueFactory',
         'MultiprocessingQueueFactory'),
        (server_config.QueueBackend.MULTIPROCESSING,
         'MultiprocessingQueueFactory', 'LocalQueueFactory'),
    ])
def test_start_honors_configured_queue_backend(monkeypatch, queue_backend,
                                               expected_factory_name,
                                               unexpected_factory_name):
    factory = mock.MagicMock()
    factory.start.return_value = None
    expected_factory = mock.MagicMock(return_value=factory)
    unexpected_factory = mock.MagicMock()
    worker = mock.MagicMock()
    worker_constructor = mock.MagicMock(return_value=worker)

    monkeypatch.setattr(executor.queue_base,
                        'get_registered_queue_backend_factory', lambda: None)
    monkeypatch.setattr(executor.queue_base, expected_factory_name,
                        expected_factory)
    monkeypatch.setattr(executor.queue_base, unexpected_factory_name,
                        unexpected_factory)
    monkeypatch.setattr(executor, 'RequestWorker', worker_constructor)
    monkeypatch.setattr(executor, '_queue_factory', None)

    queue_server, workers = executor.start(_make_server_config(queue_backend))

    assert queue_server is None
    assert getattr(executor, '_queue_factory') is factory
    assert workers == [worker, worker]
    expected_factory.assert_called_once_with()
    unexpected_factory.assert_not_called()
    factory.start.assert_called_once_with()
    assert worker_constructor.call_count == 2
    assert worker.run_in_background.call_count == 2


def test_start_prefers_registered_queue_backend(monkeypatch):
    queue_server = mock.sentinel.queue_server
    registered_factory = mock.MagicMock()
    registered_factory.start.return_value = queue_server
    local_factory = mock.MagicMock()
    multiprocessing_factory = mock.MagicMock()
    worker = mock.MagicMock()

    monkeypatch.setattr(executor.queue_base,
                        'get_registered_queue_backend_factory',
                        lambda: registered_factory)
    monkeypatch.setattr(executor.queue_base, 'LocalQueueFactory', local_factory)
    monkeypatch.setattr(executor.queue_base, 'MultiprocessingQueueFactory',
                        multiprocessing_factory)
    monkeypatch.setattr(executor, 'RequestWorker',
                        mock.MagicMock(return_value=worker))
    monkeypatch.setattr(executor, '_queue_factory', None)

    result, _ = executor.start(
        _make_server_config(server_config.QueueBackend.LOCAL))

    assert result is queue_server
    assert getattr(executor, '_queue_factory') is registered_factory
    local_factory.assert_not_called()
    multiprocessing_factory.assert_not_called()
    registered_factory.start.assert_called_once_with()


def test_worker_preserves_executor_construction_error(monkeypatch):
    worker = executor.RequestWorker(
        schedule_type=requests_lib.ScheduleType.LONG,
        config=server_config.WorkerConfig(garanteed_parallelism=1,
                                          burstable_parallelism=0,
                                          num_db_connections_per_worker=0))
    monkeypatch.setattr(executor, '_get_queue', mock.MagicMock())
    monkeypatch.setattr(
        executor.process, 'BurstableExecutor',
        mock.MagicMock(side_effect=RuntimeError('create failed')))

    with pytest.raises(RuntimeError, match='create failed'):
        worker.run()


def test_task_monitor_heartbeats_durable_claim(monkeypatch):
    worker = executor.RequestWorker(
        schedule_type=requests_lib.ScheduleType.SHORT,
        config=server_config.WorkerConfig(garanteed_parallelism=1,
                                          burstable_parallelism=0,
                                          num_db_connections_per_worker=0))
    heartbeat_seen = threading.Event()
    backend = mock.Mock()
    backend.claim_heartbeat_interval_seconds = 0.01

    def heartbeat(claim):
        heartbeat_seen.set()
        return True

    backend.heartbeat_claim.side_effect = heartbeat
    monkeypatch.setattr(executor.request_storage, 'get_request_backend',
                        lambda: backend)
    monkeypatch.setattr(worker, '_handle_task_result',
                        lambda *_args: heartbeat_seen.wait(timeout=1))
    future = concurrent.futures.Future()
    future.set_result(None)
    item = executor.queue_base.QueueItem('heartbeat-request',
                                         False,
                                         False,
                                         execution_generation=7,
                                         claim_token='claim-token')

    worker.handle_task_result(future, item)

    backend.heartbeat_claim.assert_called()
    claim = backend.heartbeat_claim.call_args.args[0]
    assert claim.request_id == item.request_id
    assert claim.execution_generation == item.execution_generation
    assert claim.claim_token == item.claim_token
    assert executor.request_storage.active_execution_claim() is None


@pytest.fixture()
def isolated_database(tmp_path):
    """Create an isolated DB and logs directory per-test."""
    temp_db_path = tmp_path / 'requests.db'
    temp_log_path = tmp_path / 'logs'
    temp_log_path.mkdir()

    with mock.patch('sky.server.constants.API_SERVER_REQUEST_DB_PATH',
                    str(temp_db_path)):
        with mock.patch('sky.server.constants.REQUEST_LOG_PATH_PREFIX',
                        str(temp_log_path)):
            requests_lib._DB = None
            yield
            if requests_lib._DB is not None:
                asyncio.run(requests_lib.close_db_async())


@pytest.mark.asyncio
@mock.patch.object(role_filter.permission, 'permission_service')
async def test_prepare_request_rejects_non_admin_pod_config(mock_svc):
    mock_svc.get_user_roles.return_value = ['user']
    body = payloads.LaunchBody(
        task='config:\n  kubernetes:\n    pod_config: {}\n',
        cluster_name='test-cluster',
        override_skypilot_config={},
    )

    with pytest.raises(fastapi.HTTPException) as exc_info:
        await executor.prepare_request_async(
            request_id='test-request',
            request_name=request_names.RequestName.CLUSTER_LAUNCH,
            request_body=body,
            func=lambda: None,
            auth_user=models.User(id='user-alice', name='user-alice'),
        )

    assert exc_info.value.status_code == 403


@pytest.fixture()
def mock_skypilot_config(tmp_path):
    config_content = """
aws:
  labels:
    key: value
  vpc_name: skypilot-vpc
"""
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(config_content)

    with mock.patch('sky.skypilot_config._GLOBAL_CONFIG_PATH',
                    str(config_file)):
        skypilot_config.reload_config()
        yield str(config_file)
        # Reload config again to restore original state
        skypilot_config.reload_config()


@pytest.fixture()
def mock_global_user_state():
    mock_user = mock.Mock()
    mock_user.id = 'test-user-id'
    mock_user.name = 'test-user'

    def mock_add_or_update_user(user,
                                allow_duplicate_name=True,
                                return_user=False):
        if return_user:
            return True, mock_user
        return True

    with mock.patch('sky.global_user_state.add_or_update_user',
                    side_effect=mock_add_or_update_user), \
         mock.patch('sky.global_user_state.get_user') as mock_get_user:
        mock_get_user.return_value = mock_user
        yield


@pytest.fixture()
def hijacked_sys_attrs():
    """Hijack os.environ and sys.stdout/stderr to be context-aware."""
    # Save original values
    original_environ = os.environ
    original_stdout = __import__('sys').stdout
    original_stderr = __import__('sys').stderr

    # Hijack
    context_utils.hijack_sys_attrs()

    yield

    # Restore (for test isolation)
    import sys
    setattr(os, 'environ', original_environ)
    setattr(sys, 'stdout', original_stdout)
    setattr(sys, 'stderr', original_stderr)


_SLOW_ENTRYPOINT_STARTED = threading.Event()
_SLOW_ENTRYPOINT_RELEASE = threading.Event()
_SLOW_ENTRYPOINT_FINISHED = threading.Event()


def slow_entrypoint():
    """Dummy entrypoint function for testing."""
    _SLOW_ENTRYPOINT_STARTED.set()
    _SLOW_ENTRYPOINT_RELEASE.wait(timeout=5)
    _SLOW_ENTRYPOINT_FINISHED.set()
    return 'success'


def exiting_entrypoint():
    """Dummy entrypoint that exits its worker thread."""
    raise SystemExit('Entry point exited')


async def _wait_for_threading_event(event: threading.Event) -> None:
    while not event.is_set():
        await asyncio.sleep(0.01)


_MANAGED_IMAGE_LOG_SECRET = 'synthetic-provider-secret'


class _ManagedImageFailureBody(payloads.RequestBody):
    task: str
    typed_image_error: bool


def _managed_image_failure(*, typed_image_error: bool) -> BaseException:
    message = f'provider token={_MANAGED_IMAGE_LOG_SECRET}'
    if not typed_image_error:
        return RuntimeError(message)
    marker = container_image_errors.ContainerImageError(
        'IMAGE_RESOLUTION_FAILED')
    return exceptions.ResourcesUnavailableError(message,
                                                failover_history=[marker])


def _managed_image_failure_entrypoint(*, typed_image_error: bool, **_):
    raise _managed_image_failure(typed_image_error=typed_image_error)


def _managed_image_failure_request(
        request_id: str, *, legacy_image_id: bool,
        typed_image_error: bool) -> requests_lib.Request:
    if legacy_image_id:
        task = 'resources:\n  image_id: docker:ubuntu\n'
    else:
        task = ('resources:\n  container_image: '
                'ubuntu@sha256:' + 'a' * 64 + '\n')
    return requests_lib.Request(
        request_id=request_id,
        name=(server_constants.REQUEST_NAME_PREFIX +
              request_names.RequestName.CLUSTER_LAUNCH.value),
        status=requests_lib.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='test-user-id',
        entrypoint=_managed_image_failure_entrypoint,
        request_body=_ManagedImageFailureBody(
            task=task, typed_image_error=typed_image_error),
    )


def _assert_managed_image_failure_is_value_free(request_id: str,
                                                error_log: mock.Mock) -> None:
    rendered_log = str(error_log.call_args_list)
    assert _MANAGED_IMAGE_LOG_SECRET not in rendered_log
    assert 'IMAGE_RESOLUTION_FAILED' in rendered_log
    stored = requests_lib.get_request(request_id)
    assert stored is not None
    error = stored.get_error()
    assert error is not None
    assert _MANAGED_IMAGE_LOG_SECRET not in error['message']
    assert _MANAGED_IMAGE_LOG_SECRET not in getattr(error['object'],
                                                    'stacktrace', '')


def _assert_ordinary_failure_is_preserved(request_id: str,
                                          error_log: mock.Mock) -> None:
    assert _MANAGED_IMAGE_LOG_SECRET in str(error_log.call_args_list)
    stored = requests_lib.get_request(request_id)
    assert stored is not None
    error = stored.get_error()
    assert error is not None
    assert _MANAGED_IMAGE_LOG_SECRET in error['message']


def test_managed_image_marker_search_is_cycle_safe_and_bounded(
        monkeypatch: pytest.MonkeyPatch) -> None:
    cyclic = RuntimeError('ordinary failure')
    cyclic.__cause__ = cyclic
    assert container_image_errors.find_safe_error(cyclic) is None

    marker = container_image_errors.ContainerImageError(
        'IMAGE_RESOLUTION_FAILED')
    middle = RuntimeError('middle wrapper')
    middle.__cause__ = marker
    outer = RuntimeError('outer wrapper')
    outer.__cause__ = middle
    monkeypatch.setattr(container_image_errors, '_MAX_NESTED_ERRORS', 2)
    assert container_image_errors.find_safe_error(outer) is None

    outer.__cause__ = marker
    assert container_image_errors.find_safe_error(outer) is marker


@pytest.mark.asyncio
async def test_execute_request_coroutine_ctx_cancelled_on_cancellation():
    """Test that context is always cancelled when execute_request_coroutine
    is cancelled."""
    _SLOW_ENTRYPOINT_STARTED.clear()
    _SLOW_ENTRYPOINT_RELEASE.clear()
    _SLOW_ENTRYPOINT_FINISHED.clear()

    # Create a mock request
    request = requests_lib.Request(
        request_id='test-request-id',
        name='test-request-name',
        status=requests_lib.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='test-user-id',
        entrypoint=slow_entrypoint,
        request_body=payloads.RequestBody(),
    )

    # Mock the context and its methods
    mock_ctx = mock.Mock()
    mock_ctx.vars = {}
    mock_ctx.is_canceled.return_value = False
    mock_ctx.cancel.side_effect = _SLOW_ENTRYPOINT_RELEASE.set
    mock_set_cancelled = mock.AsyncMock()

    with mock.patch('sky.utils.context.initialize'), \
         mock.patch('sky.utils.context.get', return_value=mock_ctx), \
         mock.patch.object(requests_lib, 'update_status_async',
                           new_callable=mock.AsyncMock), \
         mock.patch.object(requests_lib, 'get_request_log_storage_usage',
                           return_value=mock.Mock(hard_free_bytes=1024)), \
         mock.patch.object(requests_lib, 'get_request_status_async',
                           new_callable=mock.AsyncMock,
                           return_value=requests_lib.StatusWithMsg(
                               requests_lib.RequestStatus.RUNNING)), \
         mock.patch.object(requests_lib, 'set_request_cancelled_async',
                           mock_set_cancelled):

        task = executor.execute_request_in_coroutine(request)

        await asyncio.wait_for(
            _wait_for_threading_event(_SLOW_ENTRYPOINT_STARTED), timeout=1)
        await task.cancel()
        await asyncio.wait_for(
            _wait_for_threading_event(_SLOW_ENTRYPOINT_FINISHED), timeout=1)
        # Verify the context is actually cancelled
        mock_ctx.cancel.assert_called()
        mock_set_cancelled.assert_awaited_once_with(request.request_id)
        assert task.task.cancelled()


@pytest.mark.asyncio
@pytest.mark.parametrize('legacy_image_id', [False, True])
@pytest.mark.parametrize('typed_image_error', [False, True])
async def test_coroutine_sanitizes_only_typed_image_boundary_failures(
        isolated_database, legacy_image_id, typed_image_error):
    request_id = 'managed-image-coroutine-failure'
    request = _managed_image_failure_request(
        request_id,
        legacy_image_id=legacy_image_id,
        typed_image_error=typed_image_error)
    assert await requests_lib.create_if_not_exists_async(request)
    future = asyncio.get_running_loop().create_future()
    future.set_exception(
        _managed_image_failure(typed_image_error=typed_image_error))
    mock_ctx = mock.Mock()
    mock_ctx.redirect_log.return_value = None
    with mock.patch('sky.utils.context.initialize'), \
         mock.patch('sky.utils.context.get', return_value=mock_ctx), \
         mock.patch.object(executor, 'get_request_thread_executor'), \
         mock.patch.object(context_utils,
                           'to_thread_with_executor',
                           return_value=future), \
         mock.patch.object(executor.logger, 'error') as error_log:
        await executor._execute_request_coroutine(request)

    if typed_image_error:
        _assert_managed_image_failure_is_value_free(request_id, error_log)
    else:
        _assert_ordinary_failure_is_preserved(request_id, error_log)


@pytest.mark.asyncio
@pytest.mark.parametrize('legacy_image_id', [False, True])
@pytest.mark.parametrize('typed_image_error', [False, True])
async def test_process_sanitizes_only_typed_image_boundary_failures(
        mock_fd_operations, legacy_image_id, typed_image_error):
    del mock_fd_operations
    request_id = 'managed-image-process-failure'
    request = _managed_image_failure_request(
        request_id,
        legacy_image_id=legacy_image_id,
        typed_image_error=typed_image_error)
    assert await requests_lib.create_if_not_exists_async(request)
    with mock.patch.object(executor.logger, 'error') as error_log:
        executor._request_execution_wrapper(request_id,
                                            ignore_return_value=False)

    if typed_image_error:
        _assert_managed_image_failure_is_value_free(request_id, error_log)
    else:
        _assert_ordinary_failure_is_preserved(request_id, error_log)


@pytest.mark.asyncio
async def test_coroutine_task_cancel_cancels_and_joins_child():
    child_started = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def child_task() -> None:
        child_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_finished.set()

    child = asyncio.create_task(child_task())
    await asyncio.wait_for(child_started.wait(), timeout=1)

    await executor.CoroutineTask(child).cancel()

    assert cleanup_finished.is_set()
    assert child.cancelled()


@pytest.mark.asyncio
async def test_coroutine_task_cancel_joins_cleanup_through_repeated_cancel():
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def child_task() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanup_started.set()
            await release_cleanup.wait()
            cleanup_finished.set()
            raise

    child = asyncio.create_task(child_task())
    cancel_task = asyncio.create_task(executor.CoroutineTask(child).cancel())
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)

    cancel_task.cancel()
    await asyncio.sleep(0)
    assert not cancel_task.done()

    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(cancel_task, timeout=1)
    assert cleanup_finished.is_set()
    assert child.cancelled()


@pytest.mark.asyncio
async def test_coroutine_task_cancel_does_not_require_task_cancelling(
        monkeypatch):

    class LegacyTask:
        """Task-like current task from before asyncio.Task.cancelling()."""

    monkeypatch.setattr(executor.asyncio, 'current_task', LegacyTask)
    child = asyncio.create_task(asyncio.Event().wait())

    await executor.CoroutineTask(child).cancel()

    assert child.cancelled()


@pytest.mark.asyncio
async def test_coroutine_task_cancel_propagates_child_cleanup_error():
    child_started = asyncio.Event()

    async def child_task() -> None:
        child_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise RuntimeError('child cleanup failed') from None

    child = asyncio.create_task(child_task())
    await asyncio.wait_for(child_started.wait(), timeout=1)

    with pytest.raises(RuntimeError, match='child cleanup failed'):
        await executor.CoroutineTask(child).cancel()

    assert child.done()
    assert isinstance(child.exception(), RuntimeError)


@pytest.mark.asyncio
async def test_coroutine_task_cancel_preserves_racing_child_cleanup_error():
    child_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    unhandled_contexts = []

    async def child_task() -> None:
        child_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanup_started.set()
            await release_cleanup.wait()
            raise RuntimeError('racing child cleanup failed') from None

    loop = asyncio.get_running_loop()
    previous_exception_handler = loop.get_exception_handler()
    loop.set_exception_handler(
        lambda _loop, context: unhandled_contexts.append(context))
    child = asyncio.create_task(child_task())
    try:
        await asyncio.wait_for(child_started.wait(), timeout=1)
        cancel_task = asyncio.create_task(
            executor.CoroutineTask(child).cancel())
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)

        cancel_task.cancel()
        await asyncio.sleep(0)
        assert not cancel_task.done()

        release_cleanup.set()
        with pytest.raises(RuntimeError, match='racing child cleanup failed'):
            await asyncio.wait_for(cancel_task, timeout=1)
        # Let cancelled shield callbacks run before inspecting the handler.
        await asyncio.sleep(0)

        assert child.done()
        assert isinstance(child.exception(), RuntimeError)
        assert not unhandled_contexts
    finally:
        release_cleanup.set()
        loop.set_exception_handler(previous_exception_handler)


@pytest.mark.asyncio
async def test_execute_request_coroutine_fails_if_storage_probe_fails():
    request = requests_lib.Request(
        request_id='storage-probe-failure',
        name='sky.logs',
        status=requests_lib.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='test-user-id',
        entrypoint=mock.Mock(),
        request_body=payloads.RequestBody(),
    )
    mock_ctx = mock.Mock()
    mock_failure = mock.AsyncMock()

    with mock.patch('sky.utils.context.initialize'), \
         mock.patch('sky.utils.context.get', return_value=mock_ctx), \
         mock.patch.object(requests_lib, 'update_status_async',
                           new_callable=mock.AsyncMock), \
         mock.patch.object(requests_lib, 'get_request_log_storage_usage',
                           side_effect=OSError('statvfs failed')), \
         mock.patch.object(requests_lib, 'set_request_failed_async',
                           mock_failure):
        task = executor.execute_request_in_coroutine(request)
        await task.task

    mock_failure.assert_awaited_once()
    request.entrypoint.assert_not_called()


@pytest.mark.asyncio
async def test_execute_request_coroutine_records_thread_exit_as_failure():
    request = requests_lib.Request(
        request_id='thread-exit-failure',
        name='sky.logs',
        status=requests_lib.RequestStatus.PENDING,
        created_at=time.time(),
        user_id='test-user-id',
        entrypoint=exiting_entrypoint,
        request_body=payloads.RequestBody(),
    )
    mock_ctx = mock.Mock()
    mock_ctx.vars = {}
    mock_ctx.redirect_log.return_value = mock.sentinel.original_output
    mock_failure = mock.AsyncMock()

    with mock.patch('sky.utils.context.initialize'), \
         mock.patch('sky.utils.context.get', return_value=mock_ctx), \
         mock.patch.object(requests_lib, 'update_status_async',
                           new_callable=mock.AsyncMock), \
         mock.patch.object(requests_lib, 'get_request_log_storage_usage',
                           return_value=mock.Mock(hard_free_bytes=1024)), \
         mock.patch.object(requests_lib, 'get_request_status_async',
                           new_callable=mock.AsyncMock,
                           return_value=requests_lib.StatusWithMsg(
                               requests_lib.RequestStatus.RUNNING)), \
         mock.patch.object(requests_lib, 'set_request_failed_async',
                           mock_failure):
        task = executor.execute_request_in_coroutine(request)
        await asyncio.wait_for(task.task, timeout=2)

    mock_failure.assert_awaited_once()
    error = mock_failure.await_args.args[1]
    assert isinstance(error, RuntimeError)
    assert isinstance(error.__cause__, SystemExit)
    mock_ctx.cancel.assert_called_once_with()


CALLED_FLAG = [False]


def flag_entrypoint():
    CALLED_FLAG[0] = True
    return 'ok'


@pytest.mark.asyncio
async def test_api_cancel_race_condition(isolated_database):
    """Cancel before execution: wrapper must no-op and not run entrypoint."""
    CALLED_FLAG[0] = False
    req = requests_lib.Request(request_id='race-cancel-before',
                               name='test-request',
                               entrypoint=flag_entrypoint,
                               request_body=payloads.RequestBody(),
                               status=requests_lib.RequestStatus.PENDING,
                               created_at=0.0,
                               user_id='test-user')

    assert await requests_lib.create_if_not_exists_async(req) is True

    # Cancel the request before the executor starts.
    cancelled = await requests_lib.kill_request_async('race-cancel-before')
    assert cancelled is True

    # Execute wrapper should detect CANCELLED and return immediately.
    executor._request_execution_wrapper('race-cancel-before',
                                        ignore_return_value=False)

    # Verify entrypoint was not invoked and status remains CANCELLED.
    assert CALLED_FLAG[0] is False
    updated = requests_lib.get_request('race-cancel-before')
    assert updated is not None
    assert updated.status == requests_lib.RequestStatus.CANCELLED


@pytest.mark.asyncio
async def test_waiting_request_is_executed_not_skipped(mock_fd_operations,
                                                       mock_global_user_state,
                                                       mock_skypilot_config):
    """A dequeued WAITING request must execute, not be skipped.

    WAITING is the state a request is parked in while waiting to resume (retry
    backoff or an external continue-condition). When such a request is
    re-enqueued and dequeued, the execution wrapper must pick it up and run it
    just like PENDING. Regression test: the guard previously accepted only
    PENDING, so a re-enqueued WAITING request was silently dropped and stranded.
    """
    req = requests_lib.Request(request_id='waiting-executes',
                               name='test',
                               entrypoint=_success_entrypoint,
                               request_body=payloads.RequestBody(),
                               status=requests_lib.RequestStatus.WAITING,
                               created_at=0.0,
                               user_id='test-user')
    await requests_lib.create_if_not_exists_async(req)

    executor._request_execution_wrapper('waiting-executes',
                                        ignore_return_value=False)

    # The request ran to completion instead of being skipped while WAITING.
    updated = requests_lib.get_request('waiting-executes')
    assert updated is not None
    assert updated.status == requests_lib.RequestStatus.SUCCEEDED
    assert updated.return_value == 'success'


def _test_isolation_worker_fn(expected_env_a: str, expected_env_b: str,
                              expected_labels: dict, **kwargs):
    """Worker that verifies it sees the correct env vars and config overrides."""
    # TEST_VAR_A: Should see the overridden value (not main process value)
    assert os.environ.get('TEST_VAR_A') == expected_env_a, (
        f"Expected TEST_VAR_A={expected_env_a}, got {os.environ.get('TEST_VAR_A')}"
    )

    # TEST_VAR_B: Should see the new value set by this request
    assert os.environ.get('TEST_VAR_B') == expected_env_b, (
        f"Expected TEST_VAR_B={expected_env_b}, got {os.environ.get('TEST_VAR_B')}"
    )

    # Assert config override works - should see the overridden value, not the base.
    actual_labels = skypilot_config.get_nested(('aws', 'labels'),
                                               default_value={})
    assert actual_labels == expected_labels, (
        f"Expected labels={expected_labels}, got {actual_labels}")

    # Assert base config from mock_skypilot_config is still there.
    assert skypilot_config.get_nested(('aws', 'vpc_name'),
                                      default_value=None) == 'skypilot-vpc'

    return


def _subprocess_initializer(db_path: str, log_path: str, config_path: str):
    """Initializer for subprocess workers to set up the same mocks as the main test process."""
    from sky import skypilot_config
    from sky.server import constants as server_constants
    from sky.server.requests import requests as requests_lib

    server_constants.API_SERVER_REQUEST_DB_PATH = db_path
    server_constants.REQUEST_LOG_PATH_PREFIX = log_path
    requests_lib._DB = None
    skypilot_config._GLOBAL_CONFIG_PATH = config_path


class TestIsolationBody(payloads.RequestBody):
    """Request body for isolation tests with expected values."""
    expected_env_a: str
    expected_env_b: str
    expected_labels: dict


@pytest.mark.parametrize('execution_mode', ['coroutine', 'process_executor'])
@pytest.mark.asyncio
async def test_execute_with_isolated_env_and_config(isolated_database,
                                                    mock_global_user_state,
                                                    mock_skypilot_config,
                                                    hijacked_sys_attrs,
                                                    execution_mode, tmp_path):
    """Test that multiple concurrent requests see independent env vars and config.

    Args:
        execution_mode: Either 'coroutine' (for execute_request_in_coroutine)
                       or 'process_executor' (for _request_execution_wrapper).
    """
    proc_executor = None
    if execution_mode == 'process_executor':
        # Get the paths that were set up by the fixtures, as we need to re-apply
        # the same patches in the subprocess created by the BurstableExecutor.
        db_path = server_constants.API_SERVER_REQUEST_DB_PATH
        log_path_prefix = server_constants.REQUEST_LOG_PATH_PREFIX

        proc_executor = process.BurstableExecutor(
            garanteed_workers=5,
            burst_workers=0,
            initializer=_subprocess_initializer,
            initargs=(db_path, log_path_prefix, mock_skypilot_config))

    # Set TEST_VAR_A in main process that workers will override
    os.environ['TEST_VAR_A'] = 'init'

    # Capture main process state before spawning any workers to verify no leakage.
    env_before = dict(os.environ)
    config_before = skypilot_config.to_dict()

    async def spawn_request(request_id: str, env_a: str, env_b: str):
        """Spawn a request with env and config overrides."""
        expected_labels = {
            'key': 'value',  # From mock_skypilot_config
            'request_id': request_id,  # From override_skypilot_config
        }
        request_body = TestIsolationBody(
            env_vars={
                'TEST_VAR_A': env_a,  # Override env var from main process
                'TEST_VAR_B': env_b,  # New env var
                constants.USER_ID_ENV_VAR: f'user-{request_id}',
                constants.USER_ENV_VAR: f'user-{request_id}',
            },
            override_skypilot_config={
                'aws': {
                    'labels': {
                        'request_id': request_id,
                    }
                }
            },
            # Expected values to assert from inside the worker's context.
            expected_env_a=env_a,
            expected_env_b=env_b,
            expected_labels=expected_labels)

        request = await executor.prepare_request_async(
            request_id=request_id,
            request_name='test.isolation',
            request_body=request_body,
            func=_test_isolation_worker_fn,
            schedule_type=requests_lib.ScheduleType.SHORT)

        if execution_mode == 'coroutine':
            task = executor.execute_request_in_coroutine(request)
            await task.task
        else:
            # Submit to shared executor like production code
            fut = proc_executor.submit_until_success(
                executor._request_execution_wrapper, request_id, False)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, fut.result)

        completed_request = requests_lib.get_request(request_id)
        assert completed_request is not None
        if completed_request.status != requests_lib.RequestStatus.SUCCEEDED:
            error_info = completed_request.get_error()
            error_msg = f"Request {request_id} failed (mode={execution_mode}), status={completed_request.status}"
            if error_info:
                error_msg += f"\nError type: {error_info['type']}\nError message: {error_info['message']}\nError traceback:\n{error_info.get('traceback', 'N/A')}"
            else:
                error_msg += "\nNo error info available"
            pytest.fail(error_msg)
        return completed_request.get_return_value()

    # Spawn 5 concurrent requests with different env vars
    tasks = [
        spawn_request('1', 'a', 'one'),
        spawn_request('2', 'b', 'two'),
        spawn_request('3', 'c', 'three'),
        spawn_request('4', 'd', 'four'),
        spawn_request('5', 'e', 'five'),
    ]

    try:
        results = await asyncio.gather(*tasks)
        assert len(results) == 5

        # Verify main process state is unchanged.
        env_after = dict(os.environ)
        config_after = skypilot_config.to_dict()

        assert env_after == env_before, (
            "Environment leaked into main process! "
            f"Added: {set(env_after.keys()) - set(env_before.keys())}, "
            f"Removed: {set(env_before.keys()) - set(env_after.keys())}, "
            f"Changed: {[k for k in env_before if env_before.get(k) != env_after.get(k)]}"
        )

        assert config_after == config_before, (
            "Config leaked into main process! "
            f"Before: {config_before}, After: {config_after}")
    finally:
        # Shutdown the executor if we created one
        if proc_executor is not None:
            proc_executor.shutdown()
        os.environ.pop('TEST_VAR_A', None)


FAKE_FD_START = 100000


def _get_saved_fd_close_count(close_calls: list[int], created_fds: set) -> int:
    """Get the number of close() calls for file descriptors we created."""
    return sum(1 for fd in close_calls if fd in created_fds)


def _assert_no_double_close(close_calls: list[int], created_fds: set) -> None:
    """Verify that no file descriptor was closed more than once."""
    from collections import Counter
    our_close_calls = [fd for fd in close_calls if fd in created_fds]
    close_counts = Counter(our_close_calls)
    duplicates = {fd: count for fd, count in close_counts.items() if count > 1}
    assert not duplicates, (
        f'File descriptors closed multiple times (double-close bug!): '
        f'{duplicates}. Our FD close calls: {our_close_calls}, '
        f'All close calls: {close_calls}')


@pytest.fixture
def mock_fd_operations(isolated_database, monkeypatch):
    """Fixture that mocks all file descriptor operations for testing."""
    import os
    import pathlib
    import sys

    dup_calls = []
    dup2_calls = []
    close_calls = []
    created_fds = set()
    next_fake_fd = FAKE_FD_START

    def mock_dup(fd):
        """Mock os.dup() - saves fd and returns a unique fake fd."""
        nonlocal next_fake_fd
        dup_calls.append(fd)
        fake_fd = next_fake_fd
        created_fds.add(fake_fd)
        next_fake_fd += 1
        return fake_fd

    def mock_dup2(fd, fd2):
        """Mock os.dup2() - just records the call."""
        dup2_calls.append((fd, fd2))

    def mock_close(fd):
        """Mock os.close() - records which fds are being closed."""
        close_calls.append(fd)

    monkeypatch.setattr(os, 'dup', mock_dup)
    monkeypatch.setattr(os, 'dup2', mock_dup2)
    monkeypatch.setattr(os, 'close', mock_close)

    monkeypatch.setattr(sys.stdout, 'fileno', lambda: 1)
    monkeypatch.setattr(sys.stderr, 'fileno', lambda: 2)

    # Mock pathlib.Path.open() to return a mock file object with fileno()
    # This is needed because the executor opens a log file and redirects stdout/stderr to it.
    class MockFile:

        def __init__(self):
            self.closed = False

        def fileno(self):
            # Random large fd to not conflict with our mock fds.
            return 999

        def write(self, data):
            pass

        def flush(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.closed = True

    monkeypatch.setattr(pathlib.Path, 'open',
                        lambda self, *args, **kwargs: MockFile())

    monkeypatch.setattr(
        'sky.server.requests.executor.subprocess_utils.kill_children_processes',
        lambda: None)

    return {
        'dup_calls': dup_calls,
        'dup2_calls': dup2_calls,
        'close_calls': close_calls,
        'created_fds': created_fds,
        'dup_count': lambda: len(dup_calls),
        'close_count': lambda: len(close_calls),
    }


def _success_entrypoint():
    return 'success'


@pytest.mark.parametrize(
    ('request_name', 'expected_calls'),
    [
        ('sky.status', []),
        ('sky.launch', [mock.call('request-id', 'cluster')]),
        ('sky.down', [
            mock.call('request-id', 'cluster'),
            mock.call('request-id', 'cluster'),
        ]),
    ],
)
def test_capture_event_target_only_wraps_opted_in_lifecycle_operations(
        monkeypatch, request_name, expected_calls):
    enrich = mock.Mock()
    monkeypatch.setattr(executor, '_enrich_event_target_id', enrich)
    with executor._capture_event_target('request-id', request_name, 'cluster'):
        pass
    assert enrich.call_args_list == expected_calls


def test_capture_event_target_after_failed_launch(monkeypatch):
    enrich = mock.Mock()
    monkeypatch.setattr(executor, '_enrich_event_target_id', enrich)
    with pytest.raises(RuntimeError, match='failed'):
        with executor._capture_event_target('request-id', 'sky.launch',
                                            'cluster'):
            raise RuntimeError('failed')
    enrich.assert_called_once_with('request-id', 'cluster')


def _retryable_error_entrypoint():
    from sky import exceptions
    raise exceptions.ExecutionRetryableError('Simulated retryable error',
                                             hint='This is a test',
                                             retry_wait_seconds=0)


def _cancelled_retryable_error_entrypoint():
    with requests_lib.update_request('test-cancelled-retryable') as request:
        assert request is not None
        request.status = requests_lib.RequestStatus.CANCELLED
    raise exceptions.ExecutionRetryableError('Cancelled before retry handoff',
                                             hint='This must not retry',
                                             retry_wait_seconds=0)


def _general_error_entrypoint():
    raise ValueError('Simulated error')


def _keyboard_interrupt_entrypoint():
    raise KeyboardInterrupt()


def _dummy_entrypoint_for_retry_test():
    """Dummy entrypoint for retry test that can be pickled."""
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize('test_case', [
    pytest.param(
        {
            'request_id': 'test-success',
            'entrypoint': _success_entrypoint,
            'expected_exception': None,
        },
        id='success'),
    pytest.param(
        {
            'request_id': 'test-retryable',
            'entrypoint': _retryable_error_entrypoint,
            'expected_exception': exceptions.ExecutionRetryableError,
        },
        id='retryable_error'),
    pytest.param(
        {
            'request_id': 'test-cancelled-retryable',
            'entrypoint': _cancelled_retryable_error_entrypoint,
            'expected_exception': None,
        },
        id='cancelled_retryable_error'),
    pytest.param(
        {
            'request_id': 'test-error',
            'entrypoint': _general_error_entrypoint,
            'expected_exception': None,
        },
        id='general_exception'),
    pytest.param(
        {
            'request_id': 'test-interrupt',
            'entrypoint': _keyboard_interrupt_entrypoint,
            'expected_exception': None,
        },
        id='keyboard_interrupt'),
])
async def test_stdout_stderr_restoration(mock_fd_operations, test_case):
    """Test stdout and stderr fd handling across different execution paths."""
    req = requests_lib.Request(request_id=test_case['request_id'],
                               name='test',
                               entrypoint=test_case['entrypoint'],
                               request_body=payloads.RequestBody(),
                               status=requests_lib.RequestStatus.PENDING,
                               created_at=0.0,
                               user_id='test-user')
    await requests_lib.create_if_not_exists_async(req)

    if test_case['expected_exception'] is not None:
        with pytest.raises(test_case['expected_exception']):
            executor._request_execution_wrapper(test_case['request_id'],
                                                ignore_return_value=False)
    else:
        executor._request_execution_wrapper(test_case['request_id'],
                                            ignore_return_value=False)

    # Verify file descriptors were saved (dup called for stdout + stderr)
    assert mock_fd_operations['dup_count']() == 2, (
        f'Expected os.dup() to be called exactly twice (stdout + stderr), '
        f'but got {mock_fd_operations["dup_count"]()} calls')

    # Verify file descriptors were closed exactly once
    assert _get_saved_fd_close_count(
        mock_fd_operations['close_calls'],
        mock_fd_operations['created_fds']) == 2, (
            f'Expected os.close() to be called exactly twice, '
            f'but got {mock_fd_operations["close_count"]()} calls. '
            f'Close calls: {mock_fd_operations["close_calls"]}')

    # Verify no double-close
    _assert_no_double_close(mock_fd_operations['close_calls'],
                            mock_fd_operations['created_fds'])


@pytest.mark.asyncio
@pytest.mark.parametrize('initial_status', [
    requests_lib.RequestStatus.PENDING,
    requests_lib.RequestStatus.RUNNING,
])
async def test_request_worker_retries_after_broken_process_pool(
        isolated_database, monkeypatch, initial_status):
    """A retryable pool crash must leave the request executable."""
    request_id = f'test-broken-pool-retry-{initial_status.value.lower()}'
    request = requests_lib.Request(
        request_id=request_id,
        name='test-request',
        entrypoint=_dummy_entrypoint_for_retry_test,
        request_body=payloads.RequestBody(),
        status=initial_status,
        created_at=time.time(),
        user_id='test-user',
        pid=(123
             if initial_status == requests_lib.RequestStatus.RUNNING else None),
        retryable=True,
    )
    await requests_lib.create_if_not_exists_async(request)

    queue_items = []
    request_queue = mock.Mock()
    request_queue.put.side_effect = queue_items.append
    monkeypatch.setattr(executor, '_get_queue', lambda _: request_queue)
    worker = executor.RequestWorker(
        schedule_type=requests_lib.ScheduleType.LONG,
        config=server_config.WorkerConfig(garanteed_parallelism=1,
                                          burstable_parallelism=0,
                                          num_db_connections_per_worker=0))
    pool_error = concurrent.futures.process.BrokenProcessPool('worker died')
    fut = concurrent.futures.Future()
    fut.set_exception(pool_error)
    request_element = (request_id, False, True)

    worker.handle_task_result(fut, request_element)

    updated = requests_lib.get_request(request_id,
                                       fields=['status', 'pid', 'status_msg'])
    assert updated is not None
    assert updated.status == requests_lib.RequestStatus.WAITING
    assert updated.pid is None
    assert updated.status_msg is not None
    assert queue_items == [request_element]
    # Model the next worker dequeue. The retry must pass the same atomic gate
    # used by _request_execution_wrapper instead of being rejected as terminal.
    assert requests_lib.try_mark_running(request_id, pid=456)


@pytest.mark.asyncio
async def test_request_worker_fails_nonretryable_broken_pool_request(
        isolated_database, monkeypatch):
    request_id = 'test-broken-pool-nonretryable'
    request = requests_lib.Request(
        request_id=request_id,
        name='test-request',
        entrypoint=_dummy_entrypoint_for_retry_test,
        request_body=payloads.RequestBody(),
        status=requests_lib.RequestStatus.RUNNING,
        created_at=time.time(),
        user_id='test-user',
        retryable=False,
    )
    await requests_lib.create_if_not_exists_async(request)

    request_queue = mock.Mock()
    monkeypatch.setattr(executor, '_get_queue', lambda _: request_queue)
    worker = executor.RequestWorker(
        schedule_type=requests_lib.ScheduleType.LONG,
        config=server_config.WorkerConfig(garanteed_parallelism=1,
                                          burstable_parallelism=0,
                                          num_db_connections_per_worker=0))
    fut = concurrent.futures.Future()
    fut.set_exception(
        concurrent.futures.process.BrokenProcessPool('worker died'))

    worker.handle_task_result(fut, (request_id, False, False))

    updated = requests_lib.get_request(request_id, fields=['status', 'error'])
    assert updated is not None
    assert updated.status == requests_lib.RequestStatus.FAILED
    assert updated.get_error() is not None
    request_queue.put.assert_not_called()


@pytest.mark.asyncio
async def test_request_worker_does_not_requeue_cancelled_broken_pool_request(
        isolated_database, monkeypatch):
    """Cancellation must win over a concurrent broken-pool callback."""
    request_id = 'test-broken-pool-cancelled'
    request = requests_lib.Request(
        request_id=request_id,
        name='test-request',
        entrypoint=_dummy_entrypoint_for_retry_test,
        request_body=payloads.RequestBody(),
        status=requests_lib.RequestStatus.CANCELLED,
        created_at=time.time(),
        user_id='test-user',
        retryable=True,
    )
    await requests_lib.create_if_not_exists_async(request)

    request_queue = mock.Mock()
    monkeypatch.setattr(executor, '_get_queue', lambda _: request_queue)
    worker = executor.RequestWorker(
        schedule_type=requests_lib.ScheduleType.LONG,
        config=server_config.WorkerConfig(garanteed_parallelism=1,
                                          burstable_parallelism=0,
                                          num_db_connections_per_worker=0))
    fut = concurrent.futures.Future()
    fut.set_exception(
        concurrent.futures.process.BrokenProcessPool('worker died'))

    worker.handle_task_result(fut, (request_id, False, True))

    updated = requests_lib.get_request(request_id, fields=['status'])
    assert updated is not None
    assert updated.status == requests_lib.RequestStatus.CANCELLED
    request_queue.put.assert_not_called()


@pytest.mark.asyncio
async def test_request_worker_retry_execution_retryable_error(
        isolated_database, monkeypatch):
    """Test that RequestWorker retries requests when ExecutionRetryableError is raised."""
    # Create a request in the database
    request_id = 'test-retry-request'
    request = requests_lib.Request(
        request_id=request_id,
        name='test-request',
        entrypoint=
        _dummy_entrypoint_for_retry_test,  # Won't be called in this test
        request_body=payloads.RequestBody(),
        status=requests_lib.RequestStatus.RUNNING,
        created_at=time.time(),
        user_id='test-user',
    )
    await requests_lib.create_if_not_exists_async(request)

    # Create a mock queue that tracks puts
    queue_items = []
    mock_queue = queue_lib.Queue()

    class MockRequestQueue:

        def __init__(self, queue):
            self.queue = queue

        def get(self):
            try:
                return self.queue.get(block=False)
            except queue_lib.Empty:
                return None

        def put(self, item):
            queue_items.append(item)
            self.queue.put(item)

    request_queue = MockRequestQueue(mock_queue)

    # Mock _get_queue to return our mock queue
    def mock_get_queue(schedule_type):
        return request_queue

    monkeypatch.setattr(executor, '_get_queue', mock_get_queue)

    # Mock only the executor module's view of time. Patching time.sleep on the
    # shared module object also intercepts FileLock and watchdog sleeps in
    # background threads, making their polling mutate this test's assertions
    # and race database teardown.
    # Capture the request status and queue length observed *at the moment*
    # the backoff sleep happens, to pin the ordering: the request must be
    # PENDING (not RUNNING) and not yet re-enqueued before we wait.
    sleep_calls = []
    status_at_sleep = []
    status_msg_at_sleep = []
    queue_len_at_sleep = []

    def mock_sleep(seconds):
        sleep_calls.append(seconds)
        observed = requests_lib.get_request(request_id,
                                            fields=['status', 'status_msg'])
        status_at_sleep.append(observed.status if observed else None)
        status_msg_at_sleep.append(observed.status_msg if observed else None)
        queue_len_at_sleep.append(len(queue_items))

    executor_time = mock.Mock(wraps=time)
    executor_time.sleep.side_effect = mock_sleep
    monkeypatch.setattr(executor, 'time', executor_time)

    # Create a mock executor that tracks submit_until_success calls
    submit_calls = []

    class MockExecutor:

        def submit_until_success(self, fn, *args, **kwargs):
            submit_calls.append((fn, args, kwargs))
            # Return a future that immediately completes (does nothing)
            fut = concurrent.futures.Future()
            fut.set_result(None)
            return fut

    mock_executor = MockExecutor()

    # Create a RequestWorker
    worker = executor.RequestWorker(
        schedule_type=requests_lib.ScheduleType.LONG,
        config=server_config.WorkerConfig(garanteed_parallelism=1,
                                          burstable_parallelism=0,
                                          num_db_connections_per_worker=0))

    # Create a future that raises ExecutionRetryableError
    retryable_error = exceptions.ExecutionRetryableError(
        'Failed to provision all possible launchable resources.',
        hint='Retry after 30s',
        retry_wait_seconds=30)
    fut = concurrent.futures.Future()
    fut.set_exception(retryable_error)

    # Create request_element tuple
    request_element = (request_id, False, True
                      )  # (request_id, ignore_return_value, retryable)

    # Call handle_task_result - this should catch the exception and reschedule
    worker.handle_task_result(fut, request_element)

    # Verify the request was put back on the queue
    assert queue_items == [
        request_element
    ], (f'Expected {request_element} to be put on queue, got {queue_items[0]}')

    # Verify time.sleep was called with the retry wait time (first call should be 30)
    assert sleep_calls == [
        30
    ], (f'Expected first time.sleep call to be 30 seconds, got {sleep_calls[0]}'
       )

    # Verify the status was set to WAITING *before* the backoff wait, and that
    # the request was not yet re-enqueued at that point. This guards the
    # failover-safety ordering: a server interrupted mid-wait leaves the
    # request in WAITING (recoverable) rather than a stuck RUNNING orphan.
    assert status_at_sleep == [
        requests_lib.RequestStatus.WAITING
    ], (f'Expected status to be WAITING at sleep time, got {status_at_sleep}')
    assert queue_len_at_sleep == [
        0
    ], ('Expected request to be re-enqueued only after the wait, but it was '
        f'already on the queue at sleep time: {queue_len_at_sleep}')

    # The status message during the backoff should surface both the retry
    # reason (from the exception message) and the wait time.
    assert len(status_msg_at_sleep) == 1
    msg = status_msg_at_sleep[0]
    assert msg is not None
    assert 'Failed to provision all possible launchable resources' in msg, (
        f'Expected retry reason in status_msg, got: {msg!r}')
    assert 'retrying in 30s' in msg, (
        f'Expected wait time in status_msg, got: {msg!r}')

    # The request stays WAITING through the backoff and re-enqueue; a worker
    # flips it to RUNNING only when it picks the request back up.
    updated_request = requests_lib.get_request(request_id, fields=['status'])
    assert updated_request is not None
    assert updated_request.status == requests_lib.RequestStatus.WAITING, (
        f'Expected request status to be WAITING, got {updated_request.status}')

    # Call process_request - it should pick up the request from the queue
    # and call submit_until_success
    worker.process_request(mock_executor, request_queue)

    # Verify submit_until_success was called
    assert len(submit_calls) == 1, (
        f'Expected submit_until_success to be called once, got {len(submit_calls)} calls'
    )


class _PauseHarness:
    """Bundles the worker, queue, and request id for pause/watch tests."""

    def __init__(self, worker, request_id, queue_items, sleep_calls):
        self.worker = worker
        self.request_id = request_id
        self.queue_items = queue_items
        self.sleep_calls = sleep_calls

    def run(self, condition, retry_wait_seconds=30):
        """Drive handle_task_result with an ExecutionPausedError."""
        paused_error = exceptions.ExecutionPausedError(
            'Waiting on external admission.',
            hint='Will resume when admitted',
            retry_wait_seconds=retry_wait_seconds,
            continue_condition=condition)
        fut = concurrent.futures.Future()
        fut.set_exception(paused_error)
        request_element = (self.request_id, False, True)
        self.worker.handle_task_result(fut, request_element)
        return request_element


@pytest.fixture()
def pause_harness(isolated_database, monkeypatch):
    """A RequestWorker wired to an in-memory queue, watching time.sleep."""
    request_id = 'test-pause-request'
    request = requests_lib.Request(
        request_id=request_id,
        name='test-request',
        entrypoint=_dummy_entrypoint_for_retry_test,
        request_body=payloads.RequestBody(),
        status=requests_lib.RequestStatus.RUNNING,
        created_at=time.time(),
        user_id='test-user',
    )
    asyncio.run(requests_lib.create_if_not_exists_async(request))

    queue_items = []
    backing = queue_lib.Queue()

    class MockRequestQueue:

        def get(self):
            try:
                return backing.get(block=False)
            except queue_lib.Empty:
                return None

        def put(self, item):
            queue_items.append(item)
            backing.put(item)

    request_queue = MockRequestQueue()
    monkeypatch.setattr(executor, '_get_queue',
                        lambda schedule_type: request_queue)

    # Capture (and skip) the fixed fallback sleep so the tests run instantly.
    sleep_calls = []

    def mock_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr('time.sleep', mock_sleep)

    worker = executor.RequestWorker(
        schedule_type=requests_lib.ScheduleType.LONG,
        config=server_config.WorkerConfig(garanteed_parallelism=1,
                                          burstable_parallelism=0,
                                          num_db_connections_per_worker=0))
    return _PauseHarness(worker, request_id, queue_items, sleep_calls)


class _RecordingCondition(continue_condition_lib.ContinueCondition):
    """A ContinueCondition whose wait() returns a fixed verdict.

    Records the wait() arguments so tests can assert the executor
    drives the condition's interface correctly.
    """

    def __init__(self, verdict: bool):
        self._verdict = verdict
        self.calls = []

    def wait(self, *, is_cancelled, fallback_wait_seconds) -> bool:
        self.calls.append({
            'is_cancelled': is_cancelled(),
            'fallback_wait_seconds': fallback_wait_seconds,
        })
        return self._verdict


def test_pause_reschedules_when_wait_returns_true(pause_harness):
    """The executor calls condition.wait() and reschedules when it returns True.

    The wait policy (poll interval, backoff, deadline, fallback) lives in the
    condition; the executor just acts on the boolean verdict.
    """
    condition = _RecordingCondition(verdict=True)

    request_element = pause_harness.run(condition, retry_wait_seconds=30)

    assert pause_harness.queue_items == [request_element]
    # wait() was given the fallback and a working cancellation check.
    assert condition.calls == [{
        'is_cancelled': False,
        'fallback_wait_seconds': 30
    }]
    # During the pause the request is WAITING with a "waiting to resume" msg.
    updated = requests_lib.get_request(pause_harness.request_id,
                                       fields=['status', 'status_msg'])
    assert updated.status == requests_lib.RequestStatus.WAITING
    assert 'waiting to resume' in updated.status_msg


def test_pause_dropped_when_wait_returns_false(pause_harness):
    """A condition.wait() returning False drops the request (no reschedule)."""
    condition = _RecordingCondition(verdict=False)

    pause_harness.run(condition)

    assert pause_harness.queue_items == []


def test_pause_does_not_resurrect_cancelled_request(pause_harness):
    """A cancellation committed before retry handoff remains terminal."""
    with requests_lib.update_request(pause_harness.request_id) as request:
        assert request is not None
        request.status = requests_lib.RequestStatus.CANCELLED
    condition = _RecordingCondition(verdict=True)

    pause_harness.run(condition)

    assert not condition.calls
    assert pause_harness.queue_items == []
    request = requests_lib.get_request(pause_harness.request_id,
                                       fields=['status'])
    assert request is not None
    assert request.status == requests_lib.RequestStatus.CANCELLED


def test_pause_drops_request_removed_before_retry(pause_harness):
    """Request retention may remove a terminal row before retry handoff."""
    asyncio.run(
        requests_lib._delete_requests(  # pylint: disable=protected-access
            [pause_harness.request_id]))
    condition = _RecordingCondition(verdict=True)

    pause_harness.run(condition)

    assert not condition.calls
    assert pause_harness.queue_items == []


def test_pause_base_condition_does_fixed_fallback_wait(pause_harness):
    """The base ContinueCondition just waits the fallback, then reschedules."""
    condition = continue_condition_lib.ContinueCondition()

    request_element = pause_harness.run(condition, retry_wait_seconds=30)

    assert pause_harness.sleep_calls == [30]
    assert pause_harness.queue_items == [request_element]
    updated = requests_lib.get_request(pause_harness.request_id,
                                       fields=['status'])
    assert updated.status == requests_lib.RequestStatus.WAITING


def test_pause_base_condition_dropped_if_cancelled_during_wait(
        pause_harness, monkeypatch):
    """The base condition drops the request if it is cancelled while waiting.

    Also exercises the is_cancelled check, which the base wait()
    consults after the fallback sleep.
    """

    def cancel_on_sleep(seconds):
        pause_harness.sleep_calls.append(seconds)
        with requests_lib.update_request(pause_harness.request_id) as r:
            r.status = requests_lib.RequestStatus.CANCELLED

    monkeypatch.setattr('time.sleep', cancel_on_sleep)

    pause_harness.run(continue_condition_lib.ContinueCondition())

    assert pause_harness.queue_items == []


def test_retry_dropped_if_cancelled_during_backoff(pause_harness, monkeypatch):
    """A fixed-backoff retry cannot resurrect a concurrent cancellation."""

    def cancel_on_sleep(seconds):
        pause_harness.sleep_calls.append(seconds)
        with requests_lib.update_request(pause_harness.request_id) as request:
            assert request is not None
            request.status = requests_lib.RequestStatus.CANCELLED

    monkeypatch.setattr('time.sleep', cancel_on_sleep)
    retryable_error = exceptions.ExecutionRetryableError('Transient failure',
                                                         hint='Retry after 30s',
                                                         retry_wait_seconds=30)
    fut = concurrent.futures.Future()
    fut.set_exception(retryable_error)
    request_element = (pause_harness.request_id, False, True)

    pause_harness.worker.handle_task_result(fut, request_element)

    assert pause_harness.sleep_calls == [30]
    assert pause_harness.queue_items == []
    request = requests_lib.get_request(pause_harness.request_id,
                                       fields=['status'])
    assert request is not None
    assert request.status == requests_lib.RequestStatus.CANCELLED


def test_pause_marks_executor_free_before_wait(pause_harness, monkeypatch):
    """The freed worker process is accounted for before the pause wait runs.

    The worker process is released the instant the future completes (it raised
    ExecutionPausedError), so the free-executor gauge must be incremented
    before the - potentially long-lived - pause wait, not after the request
    reschedules. Otherwise the gauge under-reports idle executors for the whole
    duration of the pause.

    This snapshots the gauge's inc() count at the moment condition.wait() is
    entered; on the old code (increment in a post-wait finally) it would be 0.
    """
    gauge = mock.Mock()
    monkeypatch.setattr(executor.metrics_utils, 'METRICS_ENABLED', True)
    monkeypatch.setattr(executor.metrics_utils, 'SKY_APISERVER_LONG_EXECUTORS',
                        gauge)

    inc_count_at_wait = []

    class _GaugeWatchingCondition(continue_condition_lib.ContinueCondition):

        def wait(self, *, is_cancelled, fallback_wait_seconds) -> bool:
            del is_cancelled, fallback_wait_seconds
            inc_count_at_wait.append(gauge.inc.call_count)
            return True

    request_element = pause_harness.run(_GaugeWatchingCondition(),
                                        retry_wait_seconds=30)

    # The slot was marked free (inc()'d) before the wait began ...
    assert inc_count_at_wait == [1]
    # ... and exactly once overall: no lingering post-wait finally double-counts.
    assert gauge.inc.call_count == 1
    # Sanity: the request still rescheduled as before.
    assert pause_harness.queue_items == [request_element]


def test_resolve_blob_valid(tmp_path, monkeypatch):
    """Test that resolve_blob_dir returns the shared extraction dir."""
    blob_id = 'a' * 64
    user_hash = 'testuser'

    # Set up directory structure under tmp_path
    blobs_dir = tmp_path / user_hash / 'file_mounts' / 'blobs'
    blobs_dir.mkdir(parents=True)

    # Pre-create the shared extraction dir (simulating upload-time extraction)
    extraction_dir = blobs_dir / blob_id
    extraction_dir.mkdir()
    (extraction_dir / 'hello.txt').write_text('hello world')

    # Monkeypatch API_SERVER_CLIENT_DIR to point to tmp_path
    monkeypatch.setattr('sky.server.common.API_SERVER_CLIENT_DIR',
                        pathlib.Path(tmp_path))

    from sky.server import common as server_common
    result = server_common.resolve_blob_dir(blob_id, user_hash)

    # Verify the shared extraction directory is returned
    result_path = pathlib.Path(result)
    assert result_path.exists()
    assert (result_path / 'hello.txt').exists()
    assert (result_path / 'hello.txt').read_text() == 'hello world'

    # Verify the path is the shared extraction dir, not per-request
    assert result_path == extraction_dir


def test_resolve_blob_invalid_id(tmp_path, monkeypatch):
    """Test that resolve_blob_dir raises ValueError for invalid blob IDs."""
    monkeypatch.setattr('sky.server.common.API_SERVER_CLIENT_DIR',
                        pathlib.Path(tmp_path))

    with pytest.raises(ValueError, match='Invalid file_mounts_blob_id'):
        from sky.server import common as server_common
        server_common.resolve_blob_dir('not-a-hash', 'testuser')


@pytest.fixture()
def stub_override_request_env_deps(monkeypatch):
    """Stub out reload/permissions/user upsert for override_request_env tests.

    Lets the tests assert only what override_request_env_and_config does to
    os.environ, without needing a real DB / context / permission backend.
    """
    monkeypatch.setattr('sky.server.common.reload_for_new_request', mock.Mock())
    monkeypatch.setattr(
        'sky.workspaces.core.reject_request_for_unauthorized_workspace',
        mock.Mock())
    monkeypatch.setattr('sky.server.requests.requests.set_event_workspace',
                        mock.Mock(return_value=True))

    fake_user = mock.Mock()
    fake_user.id = 'client-user-id'
    fake_user.name = 'client-user'

    def fake_add_or_update_user(user, return_user=False, **kwargs):
        if return_user:
            return True, fake_user
        return True

    monkeypatch.setattr('sky.global_user_state.add_or_update_user',
                        fake_add_or_update_user)


def test_override_env_skipped_for_daemon_request(stub_override_request_env_deps,
                                                 monkeypatch):
    """Daemon request_ids must NOT have their persisted env_vars overlaid.

    Reproduces SKY-5502: a daemon row in PG carrying stale downward-API
    values from a previous deployment generation must not clobber the
    current pod's os.environ.
    """
    # Seed the "current pod" env with realistic downward-API values.
    monkeypatch.setenv('SKYPILOT_POD_MEMORY_BYTES_LIMIT', str(300 * 1024**3))
    monkeypatch.setenv('SKYPILOT_APISERVER_UUID', 'current-pod-uuid')

    # Persisted daemon body has STALE values from a now-dead pod.
    body = payloads.RequestBody(
        env_vars={
            'SKYPILOT_POD_MEMORY_BYTES_LIMIT': str(100 * 1024 * 1024),
            'SKYPILOT_APISERVER_UUID': 'stale-pod-uuid',
            constants.USER_ID_ENV_VAR: 'irrelevant',
            constants.USER_ENV_VAR: 'irrelevant',
        })

    # Pick a real daemon id so daemons.is_daemon_request_id returns True.
    daemon_id = server_daemons.INTERNAL_REQUEST_DAEMONS[0].id

    with executor.override_request_env_and_config(body,
                                                  request_id=daemon_id,
                                                  request_name='daemon'):
        assert os.environ['SKYPILOT_POD_MEMORY_BYTES_LIMIT'] == str(
            300 * 1024**3), ('daemon override clobbered the current pod env')
        assert os.environ['SKYPILOT_APISERVER_UUID'] == 'current-pod-uuid'


def test_override_env_applied_for_client_request(stub_override_request_env_deps,
                                                 monkeypatch):
    """Regression guard: client requests must still have env_vars applied."""
    monkeypatch.setenv('SKYPILOT_POD_MEMORY_BYTES_LIMIT', str(300 * 1024**3))

    body = payloads.RequestBody(
        env_vars={
            'SKYPILOT_POD_MEMORY_BYTES_LIMIT': str(100 * 1024 * 1024),
            constants.USER_ID_ENV_VAR: 'client-user-id',
            constants.USER_ENV_VAR: 'client-user',
        })

    with executor.override_request_env_and_config(
            body, request_id='not-a-daemon-uuid', request_name='sky.launch'):
        assert os.environ['SKYPILOT_POD_MEMORY_BYTES_LIMIT'] == str(100 * 1024 *
                                                                    1024)


def test_override_env_blocks_when_event_workspace_loses_execution_fence(
        stub_override_request_env_deps, monkeypatch):
    """An opted-in mutation cannot start after its audit fence is lost."""
    monkeypatch.setattr('sky.server.requests.requests.set_event_workspace',
                        mock.Mock(return_value=False))
    body = payloads.RequestBody(
        env_vars={
            constants.USER_ID_ENV_VAR: 'client-user-id',
            constants.USER_ENV_VAR: 'client-user',
        })

    with pytest.raises(RuntimeError, match='lost its execution fence'):
        with executor.override_request_env_and_config(
                body, request_id='not-a-daemon-uuid',
                request_name='sky.launch'):
            pytest.fail('The mutation body must not start.')


def test_override_env_skips_event_context_write_for_unrelated_request(
        stub_override_request_env_deps, monkeypatch):
    set_workspace = mock.Mock(return_value=True)
    monkeypatch.setattr(
        'sky.server.requests.requests.set_event_workspace',
        set_workspace,
    )
    body = payloads.RequestBody(
        env_vars={
            constants.USER_ID_ENV_VAR: 'client-user-id',
            constants.USER_ENV_VAR: 'client-user',
        })

    with executor.override_request_env_and_config(
            body, request_id='not-a-daemon-uuid', request_name='sky.status'):
        pass
    set_workspace.assert_not_called()


def test_override_env_rejects_server_owned_client_values(
        stub_override_request_env_deps, monkeypatch):
    """A crafted request body cannot replace deployment-owned capabilities."""
    capability = serve_constants.EXTERNAL_LB_ENABLED_ENV_VAR
    server_prefixed = f'{constants.SKYPILOT_SERVER_ENV_VAR_PREFIX}TEST_ONLY'
    role = 'SKYPILOT_API_SERVER_ROLE'
    pod_uid = 'SKYPILOT_POD_UID'
    endpoint = constants.SKY_API_SERVER_URL_ENV_VAR
    service_account_token = constants.SERVICE_ACCOUNT_TOKEN_ENV_VAR
    monkeypatch.setenv(capability, 'true')
    monkeypatch.setenv(server_prefixed, 'server-value')
    monkeypatch.setenv(role, 'controller')
    monkeypatch.setenv(pod_uid, 'current-pod')
    monkeypatch.setenv(endpoint, 'http://stable-api-service')
    monkeypatch.setenv(service_account_token, 'sky_server_token')

    body = payloads.RequestBody(
        env_vars={
            capability: 'false',
            server_prefixed: 'client-value',
            role: 'api',
            pod_uid: 'stale-pod',
            endpoint: 'http://client-controlled-endpoint',
            service_account_token: 'sky_client_token',
            constants.USER_ID_ENV_VAR: 'client-user-id',
            constants.USER_ENV_VAR: 'client-user',
        })

    with executor.override_request_env_and_config(
            body, request_id='not-a-daemon-uuid', request_name='sky.launch'):
        assert os.environ[capability] == 'true'
        assert os.environ[server_prefixed] == 'server-value'
        assert os.environ[role] == 'controller'
        assert os.environ[pod_uid] == 'current-pod'
        assert os.environ[endpoint] == 'http://stable-api-service'
        assert os.environ[service_account_token] == 'sky_server_token'

    assert capability not in body.env_vars
    assert server_prefixed not in body.env_vars
    assert role not in body.env_vars
    assert pod_uid not in body.env_vars
    assert endpoint not in body.env_vars
    assert service_account_token not in body.env_vars


def test_controller_execution_environment_uses_claim_fence(monkeypatch):
    """A claimed request sees its own durable controller generation."""
    monkeypatch.setenv('SKYPILOT_SERVER_CONTROLLER_GENERATION', 'old')
    monkeypatch.setenv('SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID', 'old-owner')

    with executor._controller_execution_environment(7, 'new-owner'):
        assert os.environ['SKYPILOT_SERVER_CONTROLLER_GENERATION'] == '7'
        assert os.environ[
            'SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID'] == 'new-owner'

    assert os.environ['SKYPILOT_SERVER_CONTROLLER_GENERATION'] == 'old'
    assert os.environ['SKYPILOT_SERVER_CONTROLLER_INSTANCE_ID'] == 'old-owner'


def test_daemon_env_mutations_reverted_on_exit(stub_override_request_env_deps,
                                               monkeypatch):
    """Daemon env mutations inside the with block must be reverted on exit.

    Daemons (e.g. InternalRequestDaemon.run_event) set
    SKYPILOT_DISABLE_LOGGING from inside the with block. If that mutation
    leaked, the next request handled by the same worker would inherit it.
    """
    monkeypatch.setenv('SKYPILOT_PRE_EXISTING', 'before')
    monkeypatch.delenv('SKYPILOT_NEW_VAR', raising=False)

    body = payloads.RequestBody(
        env_vars={
            constants.USER_ID_ENV_VAR: 'irrelevant',
            constants.USER_ENV_VAR: 'irrelevant',
        })

    daemon_id = server_daemons.INTERNAL_REQUEST_DAEMONS[0].id

    with executor.override_request_env_and_config(body,
                                                  request_id=daemon_id,
                                                  request_name='daemon'):
        os.environ['SKYPILOT_NEW_VAR'] = 'inside'
        del os.environ['SKYPILOT_PRE_EXISTING']

    assert 'SKYPILOT_NEW_VAR' not in os.environ
    assert os.environ['SKYPILOT_PRE_EXISTING'] == 'before'


def test_resolve_blob_missing_file(tmp_path, monkeypatch):
    """Test that resolve_blob_dir raises FileNotFoundError when blob is missing."""
    blob_id = 'b' * 64

    monkeypatch.setattr('sky.server.common.API_SERVER_CLIENT_DIR',
                        pathlib.Path(tmp_path))

    from sky.server import common as server_common
    with pytest.raises(FileNotFoundError, match='Blob not found'):
        server_common.resolve_blob_dir(blob_id, 'testuser')


# Gated SIGTERM handler tests.


@pytest.fixture()
def reset_sigterm_gate():
    """Reset module-level flag (shared across tests in same process)."""
    import signal as _signal
    original_handler = _signal.getsignal(_signal.SIGTERM)
    executor._in_request_execution = False
    yield
    executor._in_request_execution = False
    _signal.signal(_signal.SIGTERM, original_handler)


def test_gated_sigterm_handler_raises_when_active(reset_sigterm_gate):
    import signal as _signal
    executor._in_request_execution = True
    with pytest.raises(KeyboardInterrupt):
        executor._gated_sigterm_handler(_signal.SIGTERM, None)


def test_gated_sigterm_handler_swallows_when_idle(reset_sigterm_gate):
    import signal as _signal
    executor._in_request_execution = False
    # Must not raise; pool would break if SIGTERM escapes _process_worker.
    executor._gated_sigterm_handler(_signal.SIGTERM, None)


@pytest.mark.asyncio
async def test_wrapper_clears_in_request_execution_after_success(
        isolated_database, reset_sigterm_gate):
    req = requests_lib.Request(request_id='gate-cleared-on-success',
                               name='test',
                               entrypoint=_gate_clears_after_success_entrypoint,
                               request_body=payloads.RequestBody(),
                               status=requests_lib.RequestStatus.PENDING,
                               created_at=0.0,
                               user_id='test-user')
    assert await requests_lib.create_if_not_exists_async(req) is True

    executor._request_execution_wrapper('gate-cleared-on-success',
                                        ignore_return_value=False)

    assert executor._in_request_execution is False


def _gate_clears_after_success_entrypoint():
    return 'ok'


def _install_gated_handler_in_worker():
    import signal as _signal

    from sky.server.requests import executor as _executor
    _signal.signal(_signal.SIGTERM, _executor._gated_sigterm_handler)
    _executor._in_request_execution = False


def _worker_pid():
    return os.getpid()


def _identity(x):
    return x


def test_idle_worker_survives_sigterm_with_gated_handler():
    """Regression: SIGTERM to an idle worker must not break the pool.

    With the bare _sigterm_handler (always raises KI), the post-SIGTERM
    submit below fails with BrokenProcessPool.
    """
    import signal as _signal
    with concurrent.futures.ProcessPoolExecutor(
            max_workers=2,
            initializer=_install_gated_handler_in_worker) as pool:
        pid = pool.submit(_worker_pid).result(timeout=10)
        os.kill(pid, _signal.SIGTERM)

        # Poll-submit until signal delivers; bug surfaces on first attempt.
        deadline = time.time() + 5
        last_exc = None
        while time.time() < deadline:
            try:
                result = pool.submit(_identity, 'alive').result(timeout=5)
                assert result == 'alive'
                last_exc = None
                break
            except concurrent.futures.process.BrokenProcessPool as e:
                last_exc = e
                break
            except Exception as e:  # pylint: disable=broad-except
                last_exc = e
                time.sleep(0.1)
        assert last_exc is None, (
            f'Pool should remain usable after SIGTERM to an idle worker, '
            f'but got: {type(last_exc).__name__}: {last_exc}')

        for i in range(3):
            assert pool.submit(_identity, i).result(timeout=5) == i


# ---- Workspace resolution info log -------------------------------------
#
# `override_request_env_and_config` writes an INFO-level log when the
# resolver picks a workspace implicitly (preferred / default-fallback /
# single-membership) for a resource-creating request. The launch flow
# streams that log back to the CLI, so the user sees which workspace
# their cluster / job ended up in.
#
# The tests below are unit-level: they stub the resolver + permission
# check and verify the log gating logic, not the resolver itself
# (covered in test_resolve_workspace_for_user.py).


def _resolution(workspace, source):
    """Build a fake WorkspaceResolution for tests below."""
    from sky.workspaces import core as workspaces_core
    return workspaces_core.WorkspaceResolution(workspace=workspace,
                                               source=source)


@pytest.fixture()
def resolver_log_deps(monkeypatch, stub_override_request_env_deps):
    """Pin the resolver gate to ON and stub the resolver itself."""
    monkeypatch.setattr(
        'sky.server.requests.executor._should_apply_workspace_resolver',
        lambda is_daemon, client_api_version: True)
    return monkeypatch


def _run_override(request_name: str, resolution):
    """Drive override_request_env_and_config with a mocked resolution.

    Returns the list of `logger.info` calls (so tests can assert on the
    "Using workspace ..." line without depending on log capture).
    """
    from sky.workspaces import core as workspaces_core
    body = payloads.RequestBody(
        env_vars={
            constants.USER_ID_ENV_VAR: 'client-user-id',
            constants.USER_ENV_VAR: 'client-user',
        })
    info_calls: list[str] = []
    with mock.patch.object(workspaces_core,
                           'resolve_workspace_for_user',
                           return_value=resolution), \
         mock.patch.object(executor.logger, 'info',
                           side_effect=lambda msg, *a, **k: info_calls.append(
                               msg)):
        with executor.override_request_env_and_config(
                body, request_id='not-a-daemon-uuid',
                request_name=request_name):
            pass
    return info_calls


def test_resolution_log_fires_for_launch_with_implicit_source(
        resolver_log_deps):
    """`sky launch` with no explicit workspace → resolver picks via
    preferred / default-fallback / single-membership. The user sees
    "Using workspace 'X' (source: …)" so they know which workspace
    SkyPilot stamped onto the cluster row.

    The request_name passed to override_request_env_and_config is the
    name as stored on the task row — `REQUEST_NAME_PREFIX + <enum>`.
    `prepare_request_async` does the prefixing once at enqueue time."""
    from sky.workspaces import constants as workspace_constants
    info_calls = _run_override(
        request_name=(server_constants.REQUEST_NAME_PREFIX + 'launch'),
        resolution=_resolution(
            'team-a', workspace_constants.WORKSPACE_SOURCE_SINGLE_MEMBERSHIP))
    matching = [m for m in info_calls if 'Using workspace' in m]
    assert len(matching) == 1, (
        f'Expected one resolution-log line, got: {info_calls}')
    msg = matching[0]
    assert "'team-a'" in msg
    assert workspace_constants.WORKSPACE_SOURCE_SINGLE_MEMBERSHIP in msg


def test_resolution_log_fires_for_jobs_launch_with_preferred(resolver_log_deps):
    """Same log path for managed jobs — `sky jobs launch` is also a
    resource-creating verb (writes job_info.workspace), users need to
    see which workspace it landed in."""
    from sky.workspaces import constants as workspace_constants
    info_calls = _run_override(
        request_name=(server_constants.REQUEST_NAME_PREFIX + 'jobs.launch'),
        resolution=_resolution('team-b',
                               workspace_constants.WORKSPACE_SOURCE_PREFERRED))
    matching = [m for m in info_calls if 'Using workspace' in m]
    assert len(matching) == 1
    assert "'team-b'" in matching[0]


def test_resolution_log_silent_when_source_default_fallback(resolver_log_deps):
    """Landing on 'default' via default-fallback is the pre-existing
    silent behavior — every user without a preferred who has access to
    'default' lands there. Surfacing that in the log on every launch
    would clutter the common case while telling the user nothing new
    (they're already used to landing on 'default').

    Revert check: drop DEFAULT_FALLBACK from
    `_SILENT_WORKSPACE_RESOLUTION_SOURCES` and this test flips to a
    log-firing assertion failure."""
    from sky.workspaces import constants as workspace_constants
    info_calls = _run_override(
        request_name=(server_constants.REQUEST_NAME_PREFIX + 'launch'),
        resolution=_resolution(
            'default', workspace_constants.WORKSPACE_SOURCE_DEFAULT_FALLBACK))
    assert not [m for m in info_calls if 'Using workspace' in m
               ], (f'DEFAULT_FALLBACK must not log; got: {info_calls}')


def test_resolution_log_silent_when_source_explicit(resolver_log_deps):
    """When `active_workspace` was explicitly set (--workspace flag or
    a config file), the user already named the workspace — repeating
    it in the log is noise. EXPLICIT must not trigger the line."""
    from sky.workspaces import constants as workspace_constants
    info_calls = _run_override(
        request_name=(server_constants.REQUEST_NAME_PREFIX + 'launch'),
        resolution=_resolution('team-c',
                               workspace_constants.WORKSPACE_SOURCE_EXPLICIT))
    assert not [m for m in info_calls if 'Using workspace' in m
               ], (f'EXPLICIT source must not log; got: {info_calls}')


def test_resolution_log_silent_for_non_resource_creating_request(
        resolver_log_deps):
    """`sky status` / `sky queue` etc. resolve the same way but don't
    persist the workspace onto durable state. Logging there would be
    noise on commands the user runs frequently. The whitelist
    (_RESOURCE_CREATING_REQUEST_NAMES_FOR_RESOLUTION_LOG) is what
    keeps this scoped — extend it when adding a new resource-creating
    verb (SERVE_UP, etc.)."""
    from sky.workspaces import constants as workspace_constants
    info_calls = _run_override(
        request_name=(server_constants.REQUEST_NAME_PREFIX + 'status'),
        resolution=_resolution(
            'team-a', workspace_constants.WORKSPACE_SOURCE_SINGLE_MEMBERSHIP))
    assert not [m for m in info_calls if 'Using workspace' in m], (
        f'`status` must not surface the resolution log; got: {info_calls}')


def test_resolution_log_silent_for_bare_launch_without_prefix(
        resolver_log_deps):
    """Protocol lock: the runtime `request_name` (read off
    `request_task.name`) is always the prefixed form
    (`'sky.launch'`); the raw enum value `'launch'` never reaches
    `override_request_env_and_config`. If a future refactor drops the
    prefix in the whitelist, this test would START passing the log
    line and then break BOTH this case AND production — keep this case
    locking the prefix in.

    Revert check: drop `REQUEST_NAME_PREFIX +` from the whitelist and
    this test still passes (no log) but the prefixed tests above flip
    to failing — the two sides together pin the contract."""
    from sky.workspaces import constants as workspace_constants
    info_calls = _run_override(
        request_name='launch',
        resolution=_resolution(
            'team-a', workspace_constants.WORKSPACE_SOURCE_SINGLE_MEMBERSHIP))
    assert not [m for m in info_calls if 'Using workspace' in m], (
        f'bare `launch` (without prefix) must not match the whitelist; '
        f'got: {info_calls}')
