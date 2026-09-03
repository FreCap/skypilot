"""Unit tests for ``sky.jobs.utils``."""

# pylint: disable=protected-access,unused-argument
# pylint: disable=import-outside-toplevel,use-implicit-booleaness-not-comparison

import asyncio
import concurrent.futures
import os
import pathlib
import tempfile
import threading
import time
from unittest import mock

import pytest
import requests

from sky import exceptions
from sky.backends import cloud_vm_ray_backend
from sky.client import request_results
from sky.exceptions import ClusterDoesNotExist
from sky.jobs import state
from sky.jobs import utils
from sky.utils import controller_utils
from sky.utils import message_utils
from sky.utils import status_lib

# String path for mock.patch — can't use the constant directly because
# mock.patch needs the dotted path to the attribute being patched.
_SIGNAL_FILE_CONST = (
    'sky.jobs.constants.JOBS_CONSOLIDATION_RELOADED_SIGNAL_FILE')


def _make_cancel_status_check_info(
        job_id: int,
        status: state.ManagedJobStatus,
        workspace: str = 'default') -> dict[int, dict]:
    return {
        job_id: {
            'workspace': workspace,
            'schedule_state': state.ManagedJobScheduleState.ALIVE,
            'controller_pid': 1000 + job_id,
            'controller_pid_started_at': float(job_id),
            'controller_instance_id': None,
            'controller_generation': None,
            'controller_slot_id': None,
            'controller_slot_attempt': None,
            'controller_slot_quiescing': False,
            'pool': None,
            '_latest_task_id': 0,
            '_latest_task_status': status,
            '_latest_task_has_nonterminal': not status.is_terminal(),
            'all_tasks_terminal': status.is_terminal(),
        }
    }


@pytest.fixture(autouse=True)
def _clear_consolidation_mode_caches():
    """Keep request-scoped consolidation state from leaking across tests."""
    override_env = controller_utils.constants.OVERRIDE_CONSOLIDATION_MODE
    os.environ.pop(override_env, None)
    utils.is_consolidation_mode.cache_clear()
    controller_utils._effective_jobs_consolidation_with_warnings.cache_clear()
    yield
    os.environ.pop(override_env, None)
    utils.is_consolidation_mode.cache_clear()
    controller_utils._effective_jobs_consolidation_with_warnings.cache_clear()


@mock.patch('sky.jobs.utils.context_utils.sleep_with_cancellation')
@mock.patch('sky.jobs.utils.sdk._get_request_result_for_reconciliation')
@mock.patch('sky.jobs.utils.sdk.down')
@mock.patch('sky.usage.usage_lib.messages.usage.set_internal')
def test_terminate_cluster_unknown_error_never_replays_down(
        mock_set_internal, mock_sdk_down, mock_get_result, mock_sleep) -> None:
    mock_sdk_down.return_value = 'request-1'
    mock_get_result.side_effect = [
        ValueError('Mock error 1'),
        ValueError('Mock error 2'),
        None,
    ]

    utils.terminate_cluster('test-cluster')

    mock_sdk_down.assert_called_once_with('test-cluster',
                                          graceful=False,
                                          graceful_timeout=None)
    assert mock_get_result.call_args_list == [
        mock.call('request-1'),
        mock.call('request-1'),
        mock.call('request-1'),
    ]
    assert mock_set_internal.call_count == 1
    assert mock_sleep.call_count == 2


@mock.patch('sky.jobs.utils.sdk._get_request_result_for_reconciliation')
@mock.patch('sky.jobs.utils.sdk.down', return_value='request-1')
@mock.patch('sky.usage.usage_lib.messages.usage.set_internal')
def test_terminate_cluster_handles_concurrent_cluster_removal(
        mock_set_internal, mock_sdk_down, mock_get_result) -> None:
    """A cluster removed before execution remains an idempotent no-op."""
    missing = ClusterDoesNotExist('test-cluster')
    mock_get_result.side_effect = exceptions.RequestResultApplicationError(
        'request-1', missing)

    # Call should succeed silently
    utils.terminate_cluster('test-cluster')

    mock_sdk_down.assert_called_once_with('test-cluster',
                                          graceful=False,
                                          graceful_timeout=None)
    mock_get_result.assert_called_once_with('request-1')
    assert mock_set_internal.call_count == 1


def test_terminate_cluster_rejects_mismatched_application_error_provenance(
) -> None:
    termination_state = utils.ClusterTerminationState(request_id='request-1')
    mismatched = exceptions.RequestResultApplicationError(
        'different-request', ClusterDoesNotExist('test-cluster'))

    with mock.patch('sky.jobs.utils.sdk.down') as down, mock.patch(
            'sky.jobs.utils.sdk._get_request_result_for_reconciliation',
            side_effect=mismatched) as get_result, mock.patch(
                'sky.jobs.utils.context_utils.sleep_with_cancellation'):
        with pytest.raises(RuntimeError, match='Failed to terminate'):
            utils.terminate_cluster('test-cluster',
                                    max_retry=1,
                                    request_state=termination_state)

    down.assert_not_called()
    get_result.assert_called_once_with('request-1')
    assert termination_state.request_id == 'request-1'
    assert not termination_state.completed


def test_terminate_cluster_reconciles_lost_ack_without_duplicate_down() -> None:
    """A lost result ACK is recovered through the original request ID.

    The down request has already removed the provider resource when the first
    result read loses its response.  Reconciliation must read that same request
    again; submitting another down request would duplicate the mutation.
    """
    provider_state = {'present': True, 'result_reads': 0}

    def submit_down(*_args, **_kwargs):
        assert provider_state['present'], 'down mutation was submitted twice'
        return 'down-request'

    response = mock.MagicMock(status_code=200)
    response.headers = {}
    response.json.return_value = {'request_id': 'down-request'}
    request = mock.MagicMock()
    request.request_id = 'down-request'
    request.status = request_results.requests_lib.RequestStatus.SUCCEEDED
    request.get_error.return_value = None
    request.get_return_value.return_value = None

    def get_down_result(method, path, **_kwargs):
        assert method == 'GET'
        assert path == '/api/get?request_id=down-request'
        provider_state['result_reads'] += 1
        if provider_state['result_reads'] <= 3:
            provider_state['present'] = False
            raise requests.exceptions.ReadTimeout(
                'down completed but result ACK was lost')
        assert not provider_state['present']
        return response

    with mock.patch(
            'sky.jobs.utils.sdk.down',
            side_effect=submit_down) as down, mock.patch(
                'sky.client.request_results.server_common.'
                'make_authenticated_request',
                side_effect=get_down_result) as fetch_result, mock.patch(
                    'sky.client.request_results.payloads.RequestPayload',
                    return_value=mock.sentinel.payload), mock.patch(
                        'sky.client.request_results.requests_lib.Request.'
                        'decode',
                        return_value=request), mock.patch(
                            'sky.client.request_results.context_utils.'
                            'sleep_with_cancellation'), mock.patch(
                                'sky.jobs.utils.context_utils.'
                                'sleep_with_cancellation'):
        utils.terminate_cluster('test-cluster', max_retry=2)

    down.assert_called_once_with('test-cluster',
                                 graceful=False,
                                 graceful_timeout=None)
    assert fetch_result.call_count == 4
    assert provider_state == {'present': False, 'result_reads': 4}


def test_terminate_cluster_exhaustion_fails_closed_with_pending_request_id(
) -> None:
    termination_state = utils.ClusterTerminationState()
    unavailable = exceptions.RequestResultUnavailableError(
        'request-1', 'result endpoint remained unreachable')

    with mock.patch('sky.jobs.utils.sdk.down',
                    return_value='request-1') as down, mock.patch(
                        'sky.jobs.utils.sdk.'
                        '_get_request_result_for_reconciliation',
                        side_effect=unavailable) as get_result, mock.patch(
                            'sky.jobs.utils.context_utils.'
                            'sleep_with_cancellation'):
        with pytest.raises(RuntimeError, match='Failed to terminate'):
            utils.terminate_cluster('test-cluster',
                                    max_retry=2,
                                    request_state=termination_state)

    down.assert_called_once()
    assert get_result.call_args_list == [
        mock.call('request-1'),
        mock.call('request-1'),
    ]
    assert termination_state.request_id == 'request-1'
    assert not termination_state.completed


def test_terminate_cluster_retry_log_preserves_caught_traceback() -> None:
    unavailable = exceptions.RequestResultUnavailableError(
        'request-1', 'lost result acknowledgement')

    with mock.patch('sky.jobs.utils.sdk.down',
                    return_value='request-1'), mock.patch(
                        'sky.jobs.utils.sdk.'
                        '_get_request_result_for_reconciliation',
                        side_effect=unavailable), mock.patch(
                            'sky.jobs.utils.context_utils.'
                            'sleep_with_cancellation'), mock.patch(
                                'sky.jobs.utils.logger.error') as log_error:
        with pytest.raises(RuntimeError, match='Failed to terminate'):
            utils.terminate_cluster('test-cluster', max_retry=2)

    messages = [str(call.args[0]) for call in log_error.call_args_list]
    assert any('RequestResultUnavailableError' in message and
               'lost result acknowledgement' in message for message in messages)
    assert all('NoneType: None' not in message for message in messages)


def test_terminate_cluster_authoritative_retry_submits_one_new_down() -> None:
    retry_original = exceptions.RequestResultShouldRetryError('request-1')

    with mock.patch('sky.jobs.utils.sdk.down',
                    side_effect=['request-1', 'request-2']) as down, mock.patch(
                        'sky.jobs.utils.sdk.'
                        '_get_request_result_for_reconciliation',
                        side_effect=[retry_original, None]) as get_result, \
            mock.patch('sky.jobs.utils.context_utils.'
                       'sleep_with_cancellation'):
        utils.terminate_cluster('test-cluster')

    assert down.call_count == 2
    assert get_result.call_args_list == [
        mock.call('request-1'),
        mock.call('request-2'),
    ]


def test_terminate_cluster_decoded_operation_failure_can_retry() -> None:
    provider_error = ValueError('provider rejected the first down')
    decoded_failure = exceptions.RequestResultApplicationError(
        'request-1', provider_error)

    with mock.patch('sky.jobs.utils.sdk.down',
                    side_effect=['request-1', 'request-2']) as down, mock.patch(
                        'sky.jobs.utils.sdk.'
                        '_get_request_result_for_reconciliation',
                        side_effect=[decoded_failure, None]) as get_result, \
            mock.patch('sky.jobs.utils.context_utils.'
                       'sleep_with_cancellation'):
        utils.terminate_cluster('test-cluster')

    assert down.call_count == 2
    assert get_result.call_args_list == [
        mock.call('request-1'),
        mock.call('request-2'),
    ]


def test_terminate_cluster_cancellation_during_backoff_never_replays_down(
) -> None:
    unavailable = exceptions.RequestResultUnavailableError(
        'request-1', 'lost result acknowledgement')

    with mock.patch('sky.jobs.utils.sdk.down',
                    return_value='request-1') as down, mock.patch(
                        'sky.jobs.utils.sdk.'
                        '_get_request_result_for_reconciliation',
                        side_effect=unavailable), mock.patch(
                            'sky.jobs.utils.context_utils.'
                            'sleep_with_cancellation',
                            side_effect=asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            utils.terminate_cluster('test-cluster')

    down.assert_called_once()


def test_terminate_cluster_completed_state_is_idempotent() -> None:
    termination_state = utils.ClusterTerminationState()

    with mock.patch('sky.jobs.utils.sdk.down',
                    return_value='request-1') as down, mock.patch(
                        'sky.jobs.utils.sdk.'
                        '_get_request_result_for_reconciliation',
                        return_value=None) as get_result:
        utils.terminate_cluster('test-cluster', request_state=termination_state)
        utils.terminate_cluster('test-cluster', request_state=termination_state)

    down.assert_called_once()
    get_result.assert_called_once_with('request-1')
    assert termination_state.completed


def test_terminate_cluster_serializes_reconciliation_for_shared_state() -> None:
    """Concurrent cleanup owners must not race on one request state."""
    termination_state = utils.ClusterTerminationState()
    first_result_read_started = threading.Event()
    release_first_result_read = threading.Event()
    second_result_read_started = threading.Event()
    result_read_count = 0
    result_read_count_lock = threading.Lock()

    def get_result(_request_id):
        nonlocal result_read_count
        with result_read_count_lock:
            result_read_count += 1
            call_number = result_read_count
        if call_number == 1:
            first_result_read_started.set()
            assert release_first_result_read.wait(timeout=5)
        else:
            second_result_read_started.set()

    with mock.patch('sky.jobs.utils.sdk.down',
                    return_value='request-1') as down, mock.patch(
                        'sky.jobs.utils.sdk.'
                        '_get_request_result_for_reconciliation',
                        side_effect=get_result) as get_result_mock:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(utils.terminate_cluster,
                                    'test-cluster',
                                    request_state=termination_state)
            assert first_result_read_started.wait(timeout=5)
            second = executor.submit(utils.terminate_cluster,
                                     'test-cluster',
                                     request_state=termination_state)
            try:
                assert not second_result_read_started.wait(timeout=0.5)
            finally:
                release_first_result_read.set()
            first.result(timeout=5)
            second.result(timeout=5)

    down.assert_called_once()
    get_result_mock.assert_called_once_with('request-1')
    assert termination_state.completed


@pytest.mark.asyncio
async def test_event_callback_pool_job_uses_one_context_helper():
    task = mock.MagicMock()
    task.event_callback = 'echo callback'
    task.envs = {}
    task.name = 'pool-task'

    with mock.patch(
            'sky.jobs.utils.managed_job_state.get_pool_and_current_cluster_name',
            return_value=('pool-a', 'replica-a')) as get_context, \
         mock.patch('sky.jobs.utils.managed_job_state.get_pool_from_job_id',
                    side_effect=AssertionError('stale point pool read')), \
         mock.patch('sky.jobs.utils.managed_job_state.get_pool_submit_info',
                    side_effect=AssertionError('stale point submit read')), \
         mock.patch('sky.jobs.utils.generate_managed_job_cluster_name',
                    side_effect=AssertionError('pool jobs must not use task '
                                               'cluster fallback')), \
         mock.patch('sky.jobs.utils.log_lib.run_bash_command_with_log',
                    return_value=0) as run:
        callback = utils.event_callback_func(job_id=42, task_id=0, task=task)
        await callback('RUNNING')

    get_context.assert_called_once_with(42)
    run.assert_called_once()
    env_vars = run.call_args.kwargs['env_vars']
    assert env_vars['JOB_ID'] == '42'
    assert env_vars['JOB_STATUS'] == 'RUNNING'
    assert env_vars['CLUSTER_NAME'] == 'replica-a'
    assert env_vars['TASK_NAME'] == 'pool-task'
    assert env_vars['EVENT_TYPE'] == 'Spot'


@pytest.mark.asyncio
@mock.patch('sky.jobs.utils.logger')
@mock.patch('sky.global_user_state.get_cluster_handle_status_from_name')
async def test_get_job_status_timeout(mock_get_cluster, mock_logger):
    """Test that get_job_status returns error reason on timeout.

    Note: get_job_status no longer retries - it returns (None, reason) on
    transient errors. The retry logic is now in controller.py.
    """
    mock_handle = mock.MagicMock(
        spec=cloud_vm_ray_backend.CloudVmRayResourceHandle)
    mock_get_cluster.return_value = (mock_handle, status_lib.ClusterStatus.UP)

    mock_backend = mock.MagicMock(spec=cloud_vm_ray_backend.CloudVmRayBackend)

    timeout_override = 0.5  # seconds

    def slow_get_job_status(*args, **kwargs):
        """Simulates get_job_status call that hangs past the timeout."""
        time.sleep(timeout_override * 10)
        return {1: None}

    mock_backend.get_job_status = slow_get_job_status

    start_time = time.time()

    # Patch the timeout so the test passes quickly
    with mock.patch.object(utils, '_JOB_STATUS_FETCH_TIMEOUT_SECONDS',
                           timeout_override):
        job_status, error_reason = await utils.get_job_status(
            backend=mock_backend, cluster_name='test-cluster', job_id=1)

    # Should return (None, reason) tuple on timeout
    assert job_status is None, 'Expected None job status when timeout occurs'
    assert error_reason is not None, 'Expected error reason when timeout occurs'
    assert f'timed out after {timeout_override}s' in error_reason

    elapsed_time = time.time() - start_time
    slow_call_duration = timeout_override * 10
    assert timeout_override <= elapsed_time < slow_call_duration / 2, (
        'Expected timeout well before the blocking backend call finished, '
        f'but took {elapsed_time}s')

    # Verify only one attempt was made (no retry in get_job_status)
    # === Checking the job status... ===
    assert mock_logger.info.call_count == 1


@pytest.mark.asyncio
@mock.patch('sky.jobs.utils.logger')
@mock.patch('sky.global_user_state.get_cluster_handle_status_from_name')
async def test_get_job_status_returns_error_reason_on_failure(
        mock_get_cluster, mock_logger):
    """Test that get_job_status returns error reason on transient failures."""
    mock_handle = mock.MagicMock(
        spec=cloud_vm_ray_backend.CloudVmRayResourceHandle)
    mock_get_cluster.return_value = (mock_handle, status_lib.ClusterStatus.UP)

    mock_backend = mock.MagicMock(spec=cloud_vm_ray_backend.CloudVmRayBackend)

    def failing_get_job_status(*args, **kwargs):
        """Simulates get_job_status that fails with asyncio.TimeoutError."""
        raise asyncio.TimeoutError('Connection failed')

    mock_backend.get_job_status = failing_get_job_status

    job_status, error_reason = await utils.get_job_status(
        backend=mock_backend, cluster_name='test-cluster', job_id=1)

    # Should return (None, reason) tuple on failure
    assert job_status is None, 'Expected None job status on failure'
    assert error_reason is not None, 'Expected error reason on failure'
    assert 'timed out' in error_reason

    # Verify only one attempt was made (no retry in get_job_status)
    assert mock_logger.info.call_count == 1


@pytest.mark.asyncio
@mock.patch('sky.jobs.utils.logger')
@mock.patch('sky.global_user_state.get_cluster_handle_status_from_name')
async def test_get_job_status_skips_backend_when_cluster_not_up(
        mock_get_cluster, mock_logger):
    """A non-UP cluster row should short-circuit before the remote RPC."""
    mock_handle = mock.MagicMock(
        spec=cloud_vm_ray_backend.CloudVmRayResourceHandle)
    mock_get_cluster.return_value = (mock_handle,
                                     status_lib.ClusterStatus.STOPPED)
    mock_backend = mock.MagicMock(spec=cloud_vm_ray_backend.CloudVmRayBackend)
    mock_backend.get_job_status.side_effect = AssertionError(
        'backend.get_job_status should not run for STOPPED clusters')

    job_status, error_reason = await utils.get_job_status(
        backend=mock_backend, cluster_name='test-cluster', job_id=1)

    assert job_status is None
    assert error_reason == 'Cluster is not UP-like (STOPPED)'
    mock_get_cluster.assert_called_once_with('test-cluster')
    mock_backend.get_job_status.assert_not_called()
    assert mock_logger.info.call_count == 2


@mock.patch('sky.utils.controller_utils.warn_jobs_consolidation_mode_intent')
@mock.patch('sky.utils.controller_utils.logger')
@mock.patch('sky.utils.controller_utils.skypilot_config')
def test_consolidation_mode_warning_without_restart(mock_config, mock_logger,
                                                    mock_validate):
    """Test that a warning is printed when consolidation mode is enabled
    in config but the signal file doesn't exist (server not restarted).

    Signal-read + config-vs-signal warning now live in controller_utils
    (since both managed-jobs and pool readers share the same helper).
    """
    # Clear the LRU caches on both the wrapper and the shared helper.
    utils.is_consolidation_mode.cache_clear()
    controller_utils._effective_jobs_consolidation_with_warnings.cache_clear()

    # Mock config to return True for consolidation mode
    mock_config.get_nested.return_value = True

    with tempfile.TemporaryDirectory() as tmpdir:
        signal_file = pathlib.Path(tmpdir) / 'consolidation_signal'
        # Signal file does not exist — server hasn't been restarted

        with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)), \
             mock.patch.dict('os.environ',
                             {'IS_SKYPILOT_SERVER': '1'}):
            result = utils.is_consolidation_mode()

            # Signal file is source of truth — returns False
            assert result is False

            # Verify warning was logged about config mismatch
            assert mock_logger.warning.call_count == 1
            warning_msg = mock_logger.warning.call_args[0][0]
            assert 'enabled' in warning_msg
            assert 'not been restarted' in warning_msg


def test_job_recovery_skips_autostopping():
    """Verify job recovery logic treats AUTOSTOPPING like UP (no recovery)."""
    # AUTOSTOPPING should be treated as UP-like (not preempted)
    # Recovery logic should skip AUTOSTOPPING (similar to UP)
    up_status = status_lib.ClusterStatus.UP
    autostopping_status = status_lib.ClusterStatus.AUTOSTOPPING
    stopped_status = status_lib.ClusterStatus.STOPPED

    # AUTOSTOPPING should be in the same category as UP for recovery purposes
    recovery_skip_statuses = {
        up_status,
        autostopping_status,
    }

    assert up_status in recovery_skip_statuses
    assert autostopping_status in recovery_skip_statuses
    assert stopped_status not in recovery_skip_statuses


# ======== Graceful cancel tests ========


@mock.patch('sky.jobs.utils.sdk._get_request_result_for_reconciliation')
@mock.patch('sky.jobs.utils.sdk.down', return_value='request-1')
@mock.patch('sky.usage.usage_lib.messages.usage.set_internal')
def test_terminate_cluster_graceful(mock_set_internal, mock_sdk_down,
                                    mock_get_result) -> None:
    """Test terminate_cluster submits and awaits a graceful down request."""
    utils.terminate_cluster('test-cluster', graceful=True, graceful_timeout=120)

    mock_sdk_down.assert_called_once_with('test-cluster',
                                          graceful=True,
                                          graceful_timeout=120)
    mock_get_result.assert_called_once_with('request-1')
    assert mock_set_internal.call_count == 1


@mock.patch('sky.jobs.utils.sdk._get_request_result_for_reconciliation')
@mock.patch('sky.jobs.utils.sdk.down', return_value='request-1')
@mock.patch('sky.usage.usage_lib.messages.usage.set_internal')
def test_terminate_cluster_graceful_no_timeout(mock_set_internal, mock_sdk_down,
                                               mock_get_result) -> None:
    """Test terminate_cluster with graceful=True but no timeout."""
    utils.terminate_cluster('test-cluster', graceful=True)

    mock_sdk_down.assert_called_once_with('test-cluster',
                                          graceful=True,
                                          graceful_timeout=None)
    mock_get_result.assert_called_once_with('request-1')


def test_cancel_signal_file_no_graceful():
    """Test that cancel_jobs_by_id writes an empty signal file (touch)
    for non-graceful cancels on the new controller."""
    snapshot = state.JobCancellationState(state.ManagedJobStatus.RUNNING,
                                          'default')
    initial_info = _make_cancel_status_check_info(
        42, state.ManagedJobStatus.RUNNING)
    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch('sky.jobs.constants.CONSOLIDATED_SIGNAL_PATH', tmpdir):
            with mock.patch(
                    'sky.jobs.state.get_jobs_status_check_summary',
                    side_effect=[initial_info, _make_cancel_status_check_info(
                        42, snapshot.status)]), \
                 mock.patch(
                    'sky.jobs.utils.update_managed_jobs_statuses'), \
                 mock.patch('sky.jobs.state.set_pending_cancelled'):
                utils.cancel_jobs_by_id(job_ids=[42],
                                        current_workspace='default',
                                        graceful=False)

                signal_file = pathlib.Path(tmpdir) / '42'
                assert signal_file.exists()
                content = signal_file.read_text(encoding='utf-8')
                assert content == '', (
                    f'Expected empty file for non-graceful, got: {content!r}')


def test_cancel_pending_wrong_workspace_is_not_mutated():
    initial_info = _make_cancel_status_check_info(
        42, state.ManagedJobStatus.PENDING, workspace='team-b')
    with mock.patch('sky.jobs.state.get_jobs_status_check_summary',
                    return_value=initial_info), \
         mock.patch('sky.jobs.state.set_pending_cancelled') as set_cancelled, \
         mock.patch('sky.jobs.utils.update_managed_jobs_statuses') as refresh:
        result = utils.cancel_jobs_by_id(job_ids=[42],
                                         current_workspace='team-a')

    assert result.startswith('No job to cancel.')
    set_cancelled.assert_not_called()
    refresh.assert_not_called()


def test_cancel_skips_job_that_finishes_during_status_refresh(tmp_path):
    succeeded = state.JobCancellationState(state.ManagedJobStatus.SUCCEEDED,
                                           'default')
    initial_info = _make_cancel_status_check_info(
        42, state.ManagedJobStatus.RUNNING)
    with mock.patch('sky.jobs.constants.CONSOLIDATED_SIGNAL_PATH', tmp_path), \
         mock.patch('sky.jobs.state.get_jobs_status_check_summary',
                    side_effect=[initial_info, _make_cancel_status_check_info(
                        42, succeeded.status)]) as summaries, \
         mock.patch('sky.jobs.state.set_pending_cancelled') as set_cancelled, \
         mock.patch('sky.jobs.utils.update_managed_jobs_statuses') as refresh:
        result = utils.cancel_jobs_by_id(job_ids=[42],
                                         current_workspace='default')

    assert result == 'No job to cancel.'
    assert not (tmp_path / '42').exists()
    assert summaries.call_count == 2
    set_cancelled.assert_not_called()
    refresh.assert_called_once_with([42], jobs_info=initial_info)


def test_cancel_refreshed_pending_job_reuses_atomic_finalizer(tmp_path):
    pending = state.JobCancellationState(state.ManagedJobStatus.PENDING,
                                         'default')
    initial_info = _make_cancel_status_check_info(
        42, state.ManagedJobStatus.RUNNING)
    with mock.patch('sky.jobs.constants.CONSOLIDATED_SIGNAL_PATH', tmp_path), \
         mock.patch('sky.jobs.state.get_jobs_status_check_summary',
                    side_effect=[initial_info, _make_cancel_status_check_info(
                        42, pending.status)]) as summaries, \
         mock.patch('sky.jobs.state.set_pending_cancelled',
                    return_value=True) as set_cancelled, \
         mock.patch('sky.jobs.utils.update_managed_jobs_statuses') as refresh, \
         mock.patch('sky.jobs.utils.filelock.FileLock',
                    side_effect=AssertionError(
                        'refreshed pending cancel must not write a signal')):
        result = utils.cancel_jobs_by_id(job_ids=[42],
                                         current_workspace='default')

    assert result == 'Job with ID 42 is scheduled to be cancelled.'
    assert summaries.call_count == 2
    assert set_cancelled.call_args_list == [mock.call(42)]
    refresh.assert_called_once_with([42], jobs_info=initial_info)
    assert not (tmp_path / '42').exists()


def test_cancel_refreshed_pending_job_still_signals_after_claim_race(tmp_path):
    pending = state.JobCancellationState(state.ManagedJobStatus.PENDING,
                                         'default')
    initial_info = _make_cancel_status_check_info(
        42, state.ManagedJobStatus.RUNNING)
    with mock.patch('sky.jobs.constants.CONSOLIDATED_SIGNAL_PATH', tmp_path), \
         mock.patch('sky.jobs.state.get_jobs_status_check_summary',
                    side_effect=[initial_info, _make_cancel_status_check_info(
                        42, pending.status)]) as summaries, \
         mock.patch('sky.jobs.state.set_pending_cancelled',
                    return_value=False) as set_cancelled, \
         mock.patch('sky.jobs.utils.update_managed_jobs_statuses') as refresh:
        result = utils.cancel_jobs_by_id(job_ids=[42],
                                         current_workspace='default')

    assert result == 'Job with ID 42 is scheduled to be cancelled.'
    assert summaries.call_count == 2
    assert set_cancelled.call_args_list == [mock.call(42)]
    refresh.assert_called_once_with([42], jobs_info=initial_info)
    assert (tmp_path / '42').exists()


def test_cancel_batches_state_reads_for_multiple_running_jobs(tmp_path):
    job_ids = list(range(1, 21))
    running = state.JobCancellationState(state.ManagedJobStatus.RUNNING,
                                         'default')
    initial_info = {
        job_id: _make_cancel_status_check_info(job_id,
                                               state.ManagedJobStatus.RUNNING)
                [job_id] for job_id in job_ids
    }
    refreshed_info = {
        job_id: _make_cancel_status_check_info(job_id, running.status)[job_id]
        for job_id in job_ids
    }
    with mock.patch('sky.jobs.constants.CONSOLIDATED_SIGNAL_PATH', tmp_path), \
         mock.patch('sky.jobs.state.get_jobs_status_check_summary',
                    side_effect=[initial_info, refreshed_info]) as status_info, \
         mock.patch('sky.jobs.state.get_status') as point_status, \
         mock.patch('sky.jobs.state.get_workspace') as point_workspace, \
         mock.patch('sky.jobs.utils.update_managed_jobs_statuses') as refresh:
        result = utils.cancel_jobs_by_id(job_ids=job_ids,
                                         current_workspace='default')

    assert result.startswith('Jobs with IDs 1, 2, 3')
    assert status_info.call_args_list == [
        mock.call(job_ids), mock.call(job_ids)
    ]
    # All live jobs must be refreshed in a single batched sweep, not one
    # sweep per job, and the refresh must reuse the first snapshot instead of
    # issuing a second pre-refresh lifecycle read.
    assert refresh.call_args_list == [
        mock.call(job_ids, jobs_info=initial_info)
    ]
    point_status.assert_not_called()
    point_workspace.assert_not_called()
    assert all((tmp_path / str(job_id)).exists() for job_id in job_ids)


def test_cancel_signal_file_graceful():
    """Test that cancel_jobs_by_id writes 'graceful' to signal file."""
    snapshot = state.JobCancellationState(state.ManagedJobStatus.RUNNING,
                                          'default')
    initial_info = _make_cancel_status_check_info(
        42, state.ManagedJobStatus.RUNNING)
    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch('sky.jobs.constants.CONSOLIDATED_SIGNAL_PATH', tmpdir):
            with mock.patch(
                    'sky.jobs.state.get_jobs_status_check_summary',
                    side_effect=[initial_info, _make_cancel_status_check_info(
                        42, snapshot.status)]), \
                 mock.patch(
                    'sky.jobs.utils.update_managed_jobs_statuses'), \
                 mock.patch('sky.jobs.state.set_pending_cancelled'):
                utils.cancel_jobs_by_id(job_ids=[42],
                                        current_workspace='default',
                                        graceful=True)

                signal_file = pathlib.Path(tmpdir) / '42'
                assert signal_file.exists()
                content = signal_file.read_text(encoding='utf-8')
                assert content == 'graceful'


def test_cancel_signal_file_graceful_with_timeout():
    """Test that cancel_jobs_by_id writes 'graceful:<timeout>' to signal
    file."""
    snapshot = state.JobCancellationState(state.ManagedJobStatus.RUNNING,
                                          'default')
    initial_info = _make_cancel_status_check_info(
        42, state.ManagedJobStatus.RUNNING)
    with tempfile.TemporaryDirectory() as tmpdir:
        with mock.patch('sky.jobs.constants.CONSOLIDATED_SIGNAL_PATH', tmpdir):
            with mock.patch(
                    'sky.jobs.state.get_jobs_status_check_summary',
                    side_effect=[initial_info, _make_cancel_status_check_info(
                        42, snapshot.status)]), \
                 mock.patch(
                    'sky.jobs.utils.update_managed_jobs_statuses'), \
                 mock.patch('sky.jobs.state.set_pending_cancelled'):
                utils.cancel_jobs_by_id(job_ids=[42],
                                        current_workspace='default',
                                        graceful=True,
                                        graceful_timeout=300)

                signal_file = pathlib.Path(tmpdir) / '42'
                assert signal_file.exists()
                content = signal_file.read_text(encoding='utf-8')
                assert content == 'graceful:300'


@mock.patch('sky.utils.subprocess_utils.run_in_parallel')
@mock.patch('sky.backends.task_codegen.TaskCodeGen.get_rclone_flush_script')
def test_graceful_job_cancel_calls_flush(mock_flush_script, mock_run_parallel):
    """Test _graceful_job_cancel cancels jobs then flushes on all nodes."""
    from sky import core as sky_core

    mock_flush_script.return_value = 'echo flush'

    mock_runner = mock.MagicMock()
    mock_handle = mock.MagicMock(
        spec=cloud_vm_ray_backend.CloudVmRayResourceHandle)
    mock_handle.get_command_runners.return_value = [mock_runner]
    mock_backend = mock.MagicMock(spec=cloud_vm_ray_backend.CloudVmRayBackend)

    # Simulate successful flush
    mock_run_parallel.return_value = [(0, 0, '', '')]

    sky_core._graceful_job_cancel(mock_handle, mock_backend, 'test-cluster')

    # Verify jobs were cancelled
    mock_backend.cancel_jobs.assert_called_once_with(mock_handle,
                                                     jobs=None,
                                                     cancel_all=True)

    # Verify flush was run in parallel
    mock_run_parallel.assert_called_once()
    _, kwargs = mock_run_parallel.call_args
    assert kwargs['num_threads'] == 1


@mock.patch('sky.utils.subprocess_utils.run_in_parallel')
@mock.patch('sky.backends.task_codegen.TaskCodeGen.get_rclone_flush_script')
def test_graceful_job_cancel_with_timeout(mock_flush_script, mock_run_parallel):
    """Test _graceful_job_cancel wraps flush script with timeout."""
    from sky import core as sky_core

    mock_flush_script.return_value = 'echo flush'

    mock_runner = mock.MagicMock()
    mock_handle = mock.MagicMock(
        spec=cloud_vm_ray_backend.CloudVmRayResourceHandle)
    mock_handle.get_command_runners.return_value = [mock_runner]
    mock_backend = mock.MagicMock(spec=cloud_vm_ray_backend.CloudVmRayBackend)

    mock_run_parallel.return_value = [(0, 0, '', '')]

    sky_core._graceful_job_cancel(mock_handle,
                                  mock_backend,
                                  'test-cluster',
                                  timeout=60)

    # The flush function passed to run_in_parallel should wrap with timeout.
    # We verify by checking the call was made (the timeout wrapping happens
    # inside the closure).
    mock_run_parallel.assert_called_once()
    mock_flush_script.assert_called_once()


@mock.patch('sky.utils.subprocess_utils.run_in_parallel')
@mock.patch('sky.backends.task_codegen.TaskCodeGen.get_rclone_flush_script')
def test_graceful_job_cancel_wrong_backend_skips(mock_flush_script,
                                                 mock_run_parallel):
    """Test _graceful_job_cancel skips for non-CloudVmRay backends."""
    from sky import core as sky_core

    mock_handle = mock.MagicMock()  # not CloudVmRayResourceHandle
    mock_backend = mock.MagicMock()  # not CloudVmRayBackend

    sky_core._graceful_job_cancel(mock_handle, mock_backend, 'test-cluster')

    # Should not attempt flush
    mock_flush_script.assert_not_called()
    mock_run_parallel.assert_not_called()


@mock.patch('sky.utils.subprocess_utils.run_in_parallel')
@mock.patch('sky.backends.task_codegen.TaskCodeGen.get_rclone_flush_script')
def test_graceful_job_cancel_handles_flush_timeout(mock_flush_script,
                                                   mock_run_parallel):
    """Test _graceful_job_cancel handles timeout exit code (124)."""
    from sky import core as sky_core

    mock_flush_script.return_value = 'echo flush'

    mock_runner = mock.MagicMock()
    mock_handle = mock.MagicMock(
        spec=cloud_vm_ray_backend.CloudVmRayResourceHandle)
    mock_handle.get_command_runners.return_value = [mock_runner]
    mock_backend = mock.MagicMock(spec=cloud_vm_ray_backend.CloudVmRayBackend)

    # Simulate timeout on flush (exit code 124)
    mock_run_parallel.return_value = [(0, 124, '', 'timed out')]

    # Should not raise - graceful cancel handles errors
    sky_core._graceful_job_cancel(mock_handle,
                                  mock_backend,
                                  'test-cluster',
                                  timeout=10)

    mock_backend.cancel_jobs.assert_called_once()


@mock.patch('sky.utils.subprocess_utils.run_in_parallel')
@mock.patch('sky.backends.task_codegen.TaskCodeGen.get_rclone_flush_script')
def test_graceful_job_cancel_multi_node(mock_flush_script, mock_run_parallel):
    """Test _graceful_job_cancel flushes on all nodes in parallel."""
    from sky import core as sky_core

    mock_flush_script.return_value = 'echo flush'

    runners = [mock.MagicMock() for _ in range(3)]
    mock_handle = mock.MagicMock(
        spec=cloud_vm_ray_backend.CloudVmRayResourceHandle)
    mock_handle.get_command_runners.return_value = runners
    mock_backend = mock.MagicMock(spec=cloud_vm_ray_backend.CloudVmRayBackend)

    mock_run_parallel.return_value = [
        (0, 0, '', ''),
        (1, 0, '', ''),
        (2, 0, '', ''),
    ]

    sky_core._graceful_job_cancel(mock_handle, mock_backend, 'test-cluster')

    _, kwargs = mock_run_parallel.call_args
    assert kwargs['num_threads'] == 3


class TestPopulateJobRecordFromHandle:
    """Tests for _populate_job_record_from_handle."""

    def test_populate_job_record_sets_network_fields(self):
        """Test that network fields are set in the job record."""
        # Create a minimal mock handle with required attributes
        mock_handle = mock.MagicMock()
        mock_handle.stable_internal_external_ips = [('10.0.0.1', '35.1.2.3')]
        mock_handle.cluster_name_on_cloud = 'test-cluster'
        mock_handle.launched_nodes = 1
        mock_handle.launched_resources = mock.MagicMock()
        mock_handle.launched_resources.cloud = mock.MagicMock()
        mock_handle.launched_resources.cloud.__str__ = lambda self: 'AWS'
        mock_handle.launched_resources.region = 'us-east-1'
        mock_handle.launched_resources.zone = 'us-east-1a'
        mock_handle.launched_resources.accelerators = None
        mock_handle.launched_resources.labels = {}
        mock_handle.cached_cluster_info = None  # Non-K8s cluster

        job: dict[str, object] = {}

        # Mock the resources_utils function
        with mock.patch(
                'sky.jobs.utils.resources_utils.get_readable_resources_repr',
                return_value=('1x[CPU:1]', '1x[CPU:1+]')):
            utils._populate_job_record_from_handle(job=job,
                                                   cluster_name='test-cluster',
                                                   handle=mock_handle)

        # Check network fields are set
        assert 'internal_external_ips' in job
        assert job['internal_external_ips'] == [('10.0.0.1', '35.1.2.3')]
        assert 'internal_services' in job
        assert job['internal_services'] is None  # Non-K8s cluster

        # Check other fields are also set
        assert job['cluster_resources'] == '1x[CPU:1]'
        assert job['cloud'] == 'AWS'
        assert job['region'] == 'us-east-1'

    def test_populate_job_record_sets_internal_services(self):
        """Test that K8s internal_svc entries are extracted."""
        # Create a mock handle for a K8s cluster
        mock_handle = mock.MagicMock()
        mock_handle.stable_internal_external_ips = [('10.0.0.1', '10.0.0.1')]
        mock_handle.cluster_name_on_cloud = 'test-cluster'
        mock_handle.launched_nodes = 1
        mock_handle.launched_resources = mock.MagicMock()
        mock_handle.launched_resources.cloud = mock.MagicMock()
        mock_handle.launched_resources.cloud.__str__ = lambda self: 'Kubernetes'
        mock_handle.launched_resources.region = None
        mock_handle.launched_resources.zone = None
        mock_handle.launched_resources.accelerators = None
        mock_handle.launched_resources.labels = {}

        # Create mock cluster info with K8s internal_svc
        mock_instance_info = mock.MagicMock()
        mock_instance_info.internal_svc = 'pod-0.svc.cluster.local'
        mock_handle.cached_cluster_info = mock.MagicMock()
        mock_handle.cached_cluster_info.provider_name = 'kubernetes'
        mock_handle.cached_cluster_info.instances = {
            'pod-0': [mock_instance_info]
        }

        job: dict[str, object] = {}

        # Mock the resources_utils function
        with mock.patch(
                'sky.jobs.utils.resources_utils.get_readable_resources_repr',
                return_value=('1x[CPU:1]', '1x[CPU:1+]')):
            utils._populate_job_record_from_handle(job=job,
                                                   cluster_name='test-cluster',
                                                   handle=mock_handle)

        # Check K8s internal_svc is extracted
        assert 'internal_services' in job
        assert job['internal_services'] == {'pod-0': 'pod-0.svc.cluster.local'}


class TestClusterHandleFields:
    """Tests for _CLUSTER_HANDLE_FIELDS configuration."""

    def test_network_fields_in_cluster_handle_fields(self):
        """Test that network fields are in _CLUSTER_HANDLE_FIELDS."""
        assert 'internal_external_ips' in utils._CLUSTER_HANDLE_FIELDS
        assert 'internal_services' in utils._CLUSTER_HANDLE_FIELDS

    def test_cluster_handle_not_required_excludes_network_fields(self):
        """Test that _cluster_handle_not_required returns False when network fields are present."""
        fields_with_ips = ['job_id', 'status', 'internal_external_ips']
        assert not utils._cluster_handle_not_required(fields_with_ips)

        fields_with_k8s = ['job_id', 'status', 'internal_services']
        assert not utils._cluster_handle_not_required(fields_with_k8s)

    def test_cluster_handle_not_required_without_handle_fields(self):
        """Test that _cluster_handle_not_required returns True without handle fields."""
        fields_without_handle = ['job_id', 'status', 'job_name']
        assert utils._cluster_handle_not_required(fields_without_handle)


# ======== Consolidation mode tests ========


class TestIsConsolidationMode:
    """Tests for is_consolidation_mode() with None sentinel."""

    def test_no_signal_returns_false(self):
        """No signal file => False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            signal_file = pathlib.Path(tmpdir) / 'signal'
            with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)):
                assert utils.is_consolidation_mode() is False

    def test_signal_exists_returns_true(self):
        """Signal file exists => True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            signal_file = pathlib.Path(tmpdir) / 'signal'
            signal_file.touch()
            with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)):
                assert utils.is_consolidation_mode() is True


class TestSetupConsolidationModeOnStartup:
    """Tests for setup_consolidation_mode_on_startup()."""

    @mock.patch('sky.jobs.utils.skypilot_config')
    def test_explicit_true_touches_signal(self, mock_config):
        """Config explicitly True => signal file created."""
        mock_config.get_nested.return_value = True
        with tempfile.TemporaryDirectory() as tmpdir:
            signal_file = pathlib.Path(tmpdir) / 'signal'
            with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)):
                utils.setup_consolidation_mode_on_startup(deploy=True)
                assert signal_file.exists()

    @mock.patch('sky.jobs.utils.skypilot_config')
    def test_explicit_false_removes_signal(self, mock_config):
        """Config explicitly False => signal file removed."""
        mock_config.get_nested.return_value = False
        with tempfile.TemporaryDirectory() as tmpdir:
            signal_file = pathlib.Path(tmpdir) / 'signal'
            signal_file.touch()
            with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)):
                utils.setup_consolidation_mode_on_startup(deploy=False)
                assert not signal_file.exists()

    @mock.patch('sky.jobs.utils.global_user_state')
    @mock.patch('sky.jobs.utils.skypilot_config')
    def test_fresh_deploy_auto_enables(self, mock_config, mock_gus):
        """Deploy mode, no controllers in DB, config None => signal created."""
        mock_config.get_nested.return_value = None
        mock_gus.get_cluster_names_start_with.return_value = []
        with tempfile.TemporaryDirectory() as tmpdir:
            signal_file = pathlib.Path(tmpdir) / 'signal'
            with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)):
                utils.setup_consolidation_mode_on_startup(deploy=True)
                assert signal_file.exists()

    @mock.patch('sky.jobs.utils.global_user_state')
    @mock.patch('sky.jobs.utils.skypilot_config')
    def test_existing_controllers_no_auto_enable(self, mock_config, mock_gus):
        """Deploy mode, controllers in DB, config None => signal NOT created."""
        mock_config.get_nested.return_value = None
        mock_gus.get_cluster_names_start_with.return_value = [
            'sky-jobs-controller-abc12345'
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            signal_file = pathlib.Path(tmpdir) / 'signal'
            with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)):
                utils.setup_consolidation_mode_on_startup(deploy=True)
                assert not signal_file.exists()

    @mock.patch('sky.jobs.utils.skypilot_config')
    def test_local_server_no_auto_enable(self, mock_config):
        """Local server (deploy=False), config None => signal NOT created."""
        mock_config.get_nested.return_value = None
        with tempfile.TemporaryDirectory() as tmpdir:
            signal_file = pathlib.Path(tmpdir) / 'signal'
            with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)):
                utils.setup_consolidation_mode_on_startup(deploy=False)
                assert not signal_file.exists()

    @mock.patch('sky.jobs.utils.global_user_state')
    @mock.patch('sky.jobs.utils.skypilot_config')
    def test_cleans_signal_when_controllers_exist(self, mock_config, mock_gus):
        """Previous signal + controllers exist => signal cleaned up."""
        mock_config.get_nested.return_value = None
        mock_gus.get_cluster_names_start_with.return_value = [
            'sky-jobs-controller-abc12345'
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            signal_file = pathlib.Path(tmpdir) / 'signal'
            signal_file.touch()  # Pre-existing signal
            with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)):
                utils.setup_consolidation_mode_on_startup(deploy=True)
                assert not signal_file.exists()

    @mock.patch('sky.jobs.utils.skypilot_config')
    def test_local_server_cleans_stale_signal(self, mock_config):
        """Local server with stale signal from previous deploy => cleaned."""
        mock_config.get_nested.return_value = None
        with tempfile.TemporaryDirectory() as tmpdir:
            signal_file = pathlib.Path(tmpdir) / 'signal'
            signal_file.touch()  # Stale signal from previous deploy
            with mock.patch(_SIGNAL_FILE_CONST, str(signal_file)):
                utils.setup_consolidation_mode_on_startup(deploy=False)
                assert not signal_file.exists()


class TestCollectDebugDumpManifestParallel:
    """Test that collect_debug_dump_manifest works correctly with many jobs."""

    NUM_JOBS = 200

    def _mock_get_managed_job_tasks(self, job_id):
        return [{'user_yaml': f'name: job-{job_id}', 'status': 'RUNNING'}]

    def _mock_get_job_events(self, job_id, limit=None):  # pylint: disable=unused-argument
        return [{
            'spot_job_id': job_id,
            'task_id': 0,
            'new_status': 'RUNNING',
            'code': None,
            'reason': None,
            'timestamp': '2026-01-01',
        }]

    def _mock_get_all_task_ids_names_statuses_logs(self, job_id):
        return [(0, f'task-{job_id}', 'RUNNING', f'/tmp/log-{job_id}', None)]

    def _mock_get_pool_submit_info(self, job_id):
        # Every 10 jobs share a cluster to test dedup
        cluster_idx = (job_id - 1) // 10
        return f'cluster-{cluster_idx}', job_id

    def _mock_get_cluster_from_name(self, cluster_name):
        return {
            'name': cluster_name,
            'cluster_hash': f'hash-{cluster_name}',
            'handle': None,
        }

    def test_collection_uses_late_bound_facade_helpers(self):
        """Facade helper patches continue to control manifest collection."""

        def collect_job(job_id):
            return ([{
                'relative_path': f'managed_jobs/{job_id}/job_info.json',
                'content': str(job_id),
            }], [], [], 'shared-cluster', {f'controller-{job_id}'})

        with (mock.patch.object(utils,
                                '_collect_job_debug_manifest',
                                side_effect=collect_job) as mock_collect_job,
              mock.patch.object(utils, '_collect_cluster_debug_manifest') as
              mock_collect_cluster,
              mock.patch.object(utils, '_collect_controller_system_log_paths')
              as mock_collect_controller_logs):
            result = utils.collect_debug_dump_manifest([1, 2])

        assert [item['content'] for item in result['inline_data']] == ['1', '2']
        assert not result['file_paths']
        assert not result['errors']
        assert mock_collect_job.call_args_list == [mock.call(1), mock.call(2)]
        mock_collect_cluster.assert_called_once_with('shared-cluster',
                                                     'managed_jobs/1',
                                                     result['inline_data'],
                                                     result['errors'])
        mock_collect_controller_logs.assert_called_once_with(
            result['file_paths'], result['errors'],
            {'controller-1', 'controller-2'})

    @mock.patch('sky.jobs.utils.debug_dump_helpers.get_cluster_events_data')
    @mock.patch('sky.jobs.utils.debug_dump_helpers.serialize_cluster_record')
    @mock.patch('sky.jobs.utils.global_user_state.get_cluster_from_name')
    @mock.patch('sky.jobs.utils.managed_job_state.get_pool_submit_info')
    @mock.patch('sky.jobs.utils.managed_job_state'
                '.get_all_task_ids_names_statuses_logs')
    @mock.patch('sky.jobs.utils.managed_job_state.get_job_events')
    @mock.patch('sky.jobs.utils.managed_job_state.get_managed_job_tasks')
    @mock.patch('sky.jobs.utils.debug_dump_helpers.redact_task_yaml')
    def test_parallel_collection_correctness(
        self,
        mock_redact,
        mock_get_tasks,
        mock_get_events,
        mock_get_task_ids,
        mock_get_pool,
        mock_get_cluster,
        mock_serialize,
        mock_cluster_events,
    ):
        """All jobs collected, cluster info deduplicated, no data lost."""
        mock_redact.side_effect = lambda y: y
        mock_get_tasks.side_effect = self._mock_get_managed_job_tasks
        mock_get_events.side_effect = self._mock_get_job_events
        mock_get_task_ids.side_effect = (
            self._mock_get_all_task_ids_names_statuses_logs)
        mock_get_pool.side_effect = self._mock_get_pool_submit_info
        mock_get_cluster.side_effect = self._mock_get_cluster_from_name
        mock_serialize.side_effect = lambda r: {'name': r['name']}
        mock_cluster_events.return_value = []

        job_ids = list(range(1, self.NUM_JOBS + 1))
        result = utils.collect_debug_dump_manifest(job_ids)

        # Every job should produce job_info + job_events = 2 inline items
        job_inline = [
            p for p in result['inline_data']
            if '/clusters/' not in p['relative_path']
        ]
        assert len(job_inline) == self.NUM_JOBS * 2, (
            f'Expected {self.NUM_JOBS * 2} job inline items, '
            f'got {len(job_inline)}')

        # Cluster info should be deduplicated: 200 jobs / 10 per cluster = 20
        cluster_inline = [
            p for p in result['inline_data']
            if '/clusters/' in p['relative_path']
        ]
        expected_clusters = self.NUM_JOBS // 10
        assert len(cluster_inline) == expected_clusters, (
            f'Expected {expected_clusters} cluster info entries, '
            f'got {len(cluster_inline)}')

        # No errors
        assert len(result['errors']) == 0

    @mock.patch('sky.jobs.utils.debug_dump_helpers.get_cluster_events_data')
    @mock.patch('sky.jobs.utils.debug_dump_helpers.serialize_cluster_record')
    @mock.patch('sky.jobs.utils.global_user_state.get_cluster_from_name')
    @mock.patch('sky.jobs.utils.managed_job_state.get_pool_submit_info')
    @mock.patch('sky.jobs.utils.managed_job_state'
                '.get_all_task_ids_names_statuses_logs')
    @mock.patch('sky.jobs.utils.managed_job_state.get_job_events')
    @mock.patch('sky.jobs.utils.managed_job_state.get_managed_job_tasks')
    @mock.patch('sky.jobs.utils.debug_dump_helpers.redact_task_yaml')
    def test_partial_failures_isolated(
        self,
        mock_redact,
        mock_get_tasks,
        mock_get_events,
        mock_get_task_ids,
        mock_get_pool,
        mock_get_cluster,
        mock_serialize,
        mock_cluster_events,
    ):
        """A failing job doesn't break collection for other jobs."""
        mock_redact.side_effect = lambda y: y

        def flaky_get_tasks(job_id):
            if job_id % 3 == 0:
                raise RuntimeError(f'DB error for job {job_id}')
            return self._mock_get_managed_job_tasks(job_id)

        mock_get_tasks.side_effect = flaky_get_tasks
        mock_get_events.side_effect = self._mock_get_job_events
        mock_get_task_ids.side_effect = (
            self._mock_get_all_task_ids_names_statuses_logs)
        mock_get_pool.side_effect = self._mock_get_pool_submit_info
        mock_get_cluster.side_effect = self._mock_get_cluster_from_name
        mock_serialize.side_effect = lambda r: {'name': r['name']}
        mock_cluster_events.return_value = []

        job_ids = list(range(1, self.NUM_JOBS + 1))
        result = utils.collect_debug_dump_manifest(job_ids)

        # Failing jobs should produce errors, not crash
        failing_jobs = [j for j in job_ids if j % 3 == 0]
        assert len(result['errors']) == len(failing_jobs)

        # Non-failing jobs should still have their data
        ok_jobs = [j for j in job_ids if j % 3 != 0]
        job_info_items = [
            p for p in result['inline_data']
            if p['relative_path'].endswith('/job_info.json')
        ]
        assert len(job_info_items) == len(ok_jobs)


class TestControllerSystemLogScoping:
    """Scope managed_jobs/controller_system/*.log to the controllers that
    actually ran the requested jobs.

    The unscoped behavior (glob controller_*.log) dragged thousands of
    unrelated controller-process logs into every dump.
    """

    _UUID_A = '4cfc2dc5-5b4e-47eb-a517-079aa7ba6757'
    _UUID_B = '276636dc-a8dd-4210-86f1-31f43b4f9d05'

    @staticmethod
    def _job_log_head(uuids):
        lines = [
            'Starting job loop for 1',
            '  log_file=/tmp/1.log',
            '  pool=None',
        ]
        for u in uuids:
            lines.append(f'From controller {u}')
            lines.append('  pid=27476')
        return '\n'.join(lines) + '\n'

    def _setup_logs_dir(self, tmpdir, jobid_log_contents, controller_uuids):
        """Build a fake controller logs dir.

        ``jobid_log_contents`` maps job_id -> string content for <jobid>.log.
        ``controller_uuids`` is the iterable of controller UUIDs whose
        ``controller_<uuid>.log`` should exist on disk.
        """
        for jid, content in jobid_log_contents.items():
            (pathlib.Path(tmpdir) / f'{jid}.log').write_text(content)
        for u in controller_uuids:
            (pathlib.Path(tmpdir) / f'controller_{u}.log').write_text('hi')
        return str(tmpdir)

    def test_extracts_single_uuid(self):
        """One "From controller" line → UUID set with one element."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._setup_logs_dir(tmpdir,
                                 {1: self._job_log_head([self._UUID_A])}, [])
            with mock.patch(
                    'sky.jobs.utils.managed_job_constants'
                    '.JOBS_CONTROLLER_LOGS_DIR', tmpdir):
                with mock.patch(
                        'sky.jobs.utils.managed_job_state'
                        '.get_managed_job_tasks',
                        return_value=[]):
                    with mock.patch(
                            'sky.jobs.utils.managed_job_state'
                            '.get_job_events',
                            return_value=[]):
                        with mock.patch(
                                'sky.jobs.utils.managed_job_state'
                                '.get_all_task_ids_names_statuses_logs',
                                return_value=[]):
                            with mock.patch(
                                    'sky.jobs.utils.managed_job_state'
                                    '.get_pool_submit_info',
                                    return_value=(None, None)):
                                _, _, _, _, uuids = (
                                    utils._collect_job_debug_manifest(1))
        assert uuids == {self._UUID_A}

    def test_extracts_multiple_uuids_for_ha_recovered_job(self):
        """An HA-recovered job has multiple "From controller" lines."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._setup_logs_dir(
                tmpdir, {1: self._job_log_head([self._UUID_A, self._UUID_B])},
                [])
            with mock.patch(
                    'sky.jobs.utils.managed_job_constants'
                    '.JOBS_CONTROLLER_LOGS_DIR', tmpdir):
                with mock.patch(
                        'sky.jobs.utils.managed_job_state'
                        '.get_managed_job_tasks',
                        return_value=[]):
                    with mock.patch(
                            'sky.jobs.utils.managed_job_state'
                            '.get_job_events',
                            return_value=[]):
                        with mock.patch(
                                'sky.jobs.utils.managed_job_state'
                                '.get_all_task_ids_names_statuses_logs',
                                return_value=[]):
                            with mock.patch(
                                    'sky.jobs.utils.managed_job_state'
                                    '.get_pool_submit_info',
                                    return_value=(None, None)):
                                _, _, _, _, uuids = (
                                    utils._collect_job_debug_manifest(1))
        assert uuids == {self._UUID_A, self._UUID_B}

    def test_extracts_ha_recovery_uuid_far_from_head(self):
        """HA recovery appends a second "From controller …" line after
        an arbitrary amount of intervening output (the per-job log is
        opened in append mode at sky/utils/context.py:146). A 16 KB-only
        head read would miss it; the scan must traverse the whole file.
        """
        gap_bytes = 200 * 1024  # 200 KB of intervening status output
        content = (
            f'Starting job loop for 1\nFrom controller {self._UUID_A}\n'
            # Realistic-ish filler: many short status lines.
            + ('Status check: still running\n' * (gap_bytes // 28)) +
            f'=== Recovery ===\nFrom controller {self._UUID_B}\n')
        with tempfile.TemporaryDirectory() as tmpdir:
            (pathlib.Path(tmpdir) / '1.log').write_text(content)
            assert (pathlib.Path(tmpdir) / '1.log').stat().st_size > 16 * 1024
            with mock.patch(
                    'sky.jobs.utils.managed_job_constants'
                    '.JOBS_CONTROLLER_LOGS_DIR', tmpdir):
                with mock.patch(
                        'sky.jobs.utils.managed_job_state'
                        '.get_managed_job_tasks',
                        return_value=[]):
                    with mock.patch(
                            'sky.jobs.utils.managed_job_state'
                            '.get_job_events',
                            return_value=[]):
                        with mock.patch(
                                'sky.jobs.utils.managed_job_state'
                                '.get_all_task_ids_names_statuses_logs',
                                return_value=[]):
                            with mock.patch(
                                    'sky.jobs.utils.managed_job_state'
                                    '.get_pool_submit_info',
                                    return_value=(None, None)):
                                _, _, _, _, uuids = (
                                    utils._collect_job_debug_manifest(1))
        assert uuids == {self._UUID_A, self._UUID_B}

    def test_missing_job_log_returns_empty_set(self):
        """No <jobid>.log → empty UUID set, no exception."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch(
                    'sky.jobs.utils.managed_job_constants'
                    '.JOBS_CONTROLLER_LOGS_DIR', tmpdir):
                with mock.patch(
                        'sky.jobs.utils.managed_job_state'
                        '.get_managed_job_tasks',
                        return_value=[]):
                    with mock.patch(
                            'sky.jobs.utils.managed_job_state'
                            '.get_job_events',
                            return_value=[]):
                        with mock.patch(
                                'sky.jobs.utils.managed_job_state'
                                '.get_all_task_ids_names_statuses_logs',
                                return_value=[]):
                            with mock.patch(
                                    'sky.jobs.utils.managed_job_state'
                                    '.get_pool_submit_info',
                                    return_value=(None, None)):
                                _, _, errs, _, uuids = (
                                    utils._collect_job_debug_manifest(1))
        assert uuids == set()
        assert errs == []

    def test_collect_controller_system_with_empty_uuids_emits_no_files(self):
        """Empty relevant_uuids must NOT fall back to globbing the dir.

        This is the regression we are fixing — globbing dragged in 8 000+
        unrelated controller process logs.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            self._setup_logs_dir(tmpdir, {}, [self._UUID_A, self._UUID_B])
            file_paths: list = []
            errors: list = []
            with mock.patch(
                    'sky.jobs.utils.managed_job_constants'
                    '.JOBS_CONTROLLER_LOGS_DIR', tmpdir):
                utils._collect_controller_system_log_paths(
                    file_paths, errors, set())
        assert file_paths == []
        assert errors == []

    def test_collect_controller_system_filters_to_relevant_uuids(self):
        """Only UUIDs in the relevant set become file_paths entries.

        ``relevant_uuids`` includes a UUID that isn't on disk → silently
        skipped. The on-disk-but-not-relevant UUID is also skipped.
        """
        missing_uuid = '00000000-0000-0000-0000-000000000000'
        with tempfile.TemporaryDirectory() as tmpdir:
            self._setup_logs_dir(tmpdir, {},
                                 [self._UUID_A, self._UUID_B])  # both on disk
            file_paths: list = []
            errors: list = []
            with mock.patch(
                    'sky.jobs.utils.managed_job_constants'
                    '.JOBS_CONTROLLER_LOGS_DIR', tmpdir):
                utils._collect_controller_system_log_paths(
                    file_paths, errors, {self._UUID_A, missing_uuid})
        # Only the on-disk + relevant UUID survives.
        rel_paths = sorted(p['relative_path'] for p in file_paths)
        assert rel_paths == [
            f'managed_jobs/controller_system/controller_{self._UUID_A}.log'
        ]
        assert errors == []


class TestCleanupExpiredApiAccessTokens:
    """Unit tests for the expired managed-job token sweep."""

    @staticmethod
    def _token(token_id: str, name: str, expires_at):
        return {
            'token_id': token_id,
            'token_name': name,
            'expires_at': expires_at,
        }

    @mock.patch('sky.global_user_state.delete_service_account_token')
    @mock.patch('sky.global_user_state.'
                'get_expired_service_account_tokens_by_name_prefix')
    def test_deletes_only_managed_job_shaped_names(self, mock_get_expired,
                                                   mock_delete_token):
        now = int(time.time())
        mock_get_expired.return_value = [
            # Looks like a real managed-job token: prefix + 8 hex suffix.
            self._token('tok-a', 'managed-job-myjob-abcdef01', now - 60),
            # Multi-segment job name, still ends in 8 hex chars.
            self._token('tok-b', 'managed-job-bench-burst-0028-fea61234',
                        now - 60),
            # Prefix matches but the suffix isn't 8 hex chars: skip.
            self._token('tok-c', 'managed-job-user-named-something', now - 60),
            # Suffix is 8 chars but contains non-hex letters: skip.
            self._token('tok-d', 'managed-job-foo-zzzzzzzz', now - 60),
        ]

        removed = utils.cleanup_expired_api_access_tokens()

        assert removed == 2
        deleted_tokens = sorted(
            c.args[0] for c in mock_delete_token.call_args_list)
        assert deleted_tokens == ['tok-a', 'tok-b']

    @mock.patch('sky.global_user_state.delete_service_account_token')
    @mock.patch('sky.global_user_state.'
                'get_expired_service_account_tokens_by_name_prefix')
    def test_token_delete_failure_is_skipped(self, mock_get_expired,
                                             mock_delete_token):
        now = int(time.time())
        mock_get_expired.return_value = [
            self._token('tok-a', 'managed-job-myjob-abcdef01', now - 60),
        ]
        mock_delete_token.side_effect = RuntimeError('db down')

        # Token revocation failed: report zero so the next sweep can retry.
        removed = utils.cleanup_expired_api_access_tokens()
        assert removed == 0

    @mock.patch('sky.global_user_state.'
                'get_expired_service_account_tokens_by_name_prefix')
    def test_no_expired_tokens_is_noop(self, mock_get_expired):
        mock_get_expired.return_value = []
        assert utils.cleanup_expired_api_access_tokens() == 0


class TestStreamControllerLogs:
    """Characterize the controller-local branch of stream_logs()."""

    def test_non_following_missing_log_is_noop(self, tmp_path):
        missing_log = tmp_path / 'missing.log'

        with mock.patch.object(utils,
                               'controller_log_file_for_job',
                               return_value=str(missing_log)):
            message, exit_code = utils.stream_logs(job_id=42,
                                                   job_name=None,
                                                   controller=True,
                                                   follow=False)

        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED

    def test_non_following_full_log_filters_relayed_payload(
            self, tmp_path, capsys):
        log_path = tmp_path / '42.log'
        log_path.write_text('first\n' + message_utils.encode_payload('hidden') +
                            'last\n',
                            encoding='utf-8')

        with mock.patch.object(utils,
                               'controller_log_file_for_job',
                               return_value=str(log_path)):
            message, exit_code = utils.stream_logs(job_id=42,
                                                   job_name=None,
                                                   controller=True,
                                                   follow=False)

        assert capsys.readouterr().out == 'first\nlast\n'
        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED

    def test_payload_filter_hook_remains_late_bound(self, tmp_path, capsys):
        log_path = tmp_path / '42.log'
        log_path.write_text('hidden-control\nvisible\n', encoding='utf-8')

        with (mock.patch.object(utils,
                                'controller_log_file_for_job',
                                return_value=str(log_path)),
              mock.patch.object(
                  utils,
                  '_is_relayed_status_payload_line',
                  side_effect=lambda line: line.startswith('hidden')) as
              is_payload):
            message, exit_code = utils.stream_logs(job_id=42,
                                                   job_name=None,
                                                   controller=True,
                                                   follow=False)

        assert capsys.readouterr().out == 'visible\n'
        assert is_payload.call_args_list == [
            mock.call('hidden-control\n'),
            mock.call('visible\n'),
        ]
        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED

    def test_non_following_tail_honors_offset(self, tmp_path, capsys):
        log_path = tmp_path / '42.log'
        log_path.write_text('zero\none\ntwo\nthree\nfour\n', encoding='utf-8')

        with mock.patch.object(utils,
                               'controller_log_file_for_job',
                               return_value=str(log_path)):
            message, exit_code = utils.stream_logs(job_id=42,
                                                   job_name=None,
                                                   controller=True,
                                                   follow=False,
                                                   tail=2,
                                                   tail_offset=1)

        assert capsys.readouterr().out == 'two\nthree\n'
        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED

    @mock.patch('sky.jobs.utils.managed_job_state.'
                'get_managed_jobs_with_filters')
    def test_controller_name_lookup_includes_terminal_jobs(
            self, mock_get_jobs, tmp_path):
        mock_get_jobs.return_value = ([{
            'job_id': 17,
            'job_name': 'finished',
            'status': state.ManagedJobStatus.SUCCEEDED,
        }], None)
        log_path = tmp_path / '17.log'
        log_path.touch()

        with mock.patch.object(utils,
                               'controller_log_file_for_job',
                               return_value=str(log_path)) as resolve_log:
            message, exit_code = utils.stream_logs(job_id=None,
                                                   job_name='finished',
                                                   controller=True,
                                                   follow=False)

        mock_get_jobs.assert_called_once_with(
            name_match='finished', fields=['job_id', 'job_name', 'status'])
        resolve_log.assert_called_once_with(17)
        assert message == ''
        assert exit_code == exceptions.JobExitCode.SUCCEEDED
