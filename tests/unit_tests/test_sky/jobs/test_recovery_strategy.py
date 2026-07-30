"""Unit tests for sky.jobs.recovery_strategy helpers."""
# pylint: disable=protected-access
import contextlib
import os
from unittest import mock

import pytest

from sky.container_images import consumers as container_image_consumers
from sky.jobs import recovery_strategy
from sky.jobs import runtime as managed_job_runtime
from sky.jobs import utils as managed_job_utils
from sky.server import common as server_common
from sky.skylet import constants
from sky.skylet import job_lib
from sky.utils import status_lib


def test_explicit_controller_uses_existing_api_service(monkeypatch):
    endpoint = 'http://skypilot-api-service.skypilot.svc'
    monkeypatch.setenv(recovery_strategy._SERVER_ROLE_ENV_VAR, 'controller')
    monkeypatch.setenv(constants.SKY_API_SERVER_URL_ENV_VAR, endpoint)
    server_common.get_server_url.cache_clear()
    server_common.is_api_server_local.cache_clear()
    health_check = mock.Mock()
    api_start = mock.Mock()
    monkeypatch.setattr(server_common, 'check_server_healthy', health_check)
    monkeypatch.setattr(recovery_strategy.sdk, 'api_start', api_start)

    try:
        recovery_strategy._ensure_api_server_for_nested_request()
    finally:
        server_common.get_server_url.cache_clear()
        server_common.is_api_server_local.cache_clear()

    health_check.assert_called_once_with(endpoint)
    api_start.assert_not_called()


def test_explicit_controller_rejects_loopback_api(monkeypatch):
    monkeypatch.setenv(recovery_strategy._SERVER_ROLE_ENV_VAR, 'controller')
    monkeypatch.setenv(constants.SKY_API_SERVER_URL_ENV_VAR,
                       server_common.DEFAULT_SERVER_URL)
    server_common.get_server_url.cache_clear()
    server_common.is_api_server_local.cache_clear()
    api_start = mock.Mock()
    monkeypatch.setattr(recovery_strategy.sdk, 'api_start', api_start)

    try:
        with pytest.raises(RuntimeError, match='requires a non-local'):
            recovery_strategy._ensure_api_server_for_nested_request()
    finally:
        server_common.get_server_url.cache_clear()
        server_common.is_api_server_local.cache_clear()

    api_start.assert_not_called()


def test_compatibility_role_bootstraps_local_api_and_restores_endpoint(
        monkeypatch):
    remote_endpoint = 'https://client.example.com'
    monkeypatch.setenv(recovery_strategy._SERVER_ROLE_ENV_VAR, 'all')
    monkeypatch.setenv(constants.SKY_API_SERVER_URL_ENV_VAR, remote_endpoint)
    monkeypatch.setattr(recovery_strategy, 'ENV_VARS_TO_CLEAR',
                        (constants.SKY_API_SERVER_URL_ENV_VAR,))
    monkeypatch.setattr(server_common.skypilot_config,
                        'get_nested',
                        lambda _, default_value=None: default_value)
    server_common.get_server_url.cache_clear()
    server_common.is_api_server_local.cache_clear()
    assert server_common.get_server_url() == remote_endpoint

    def assert_local_bootstrap():
        assert constants.SKY_API_SERVER_URL_ENV_VAR not in os.environ
        assert server_common.is_api_server_local()

    monkeypatch.setattr(recovery_strategy.sdk, 'api_start',
                        assert_local_bootstrap)

    try:
        recovery_strategy._ensure_api_server_for_nested_request()
        assert os.environ[
            constants.SKY_API_SERVER_URL_ENV_VAR] == remote_endpoint
        assert server_common.get_server_url() == remote_endpoint
    finally:
        server_common.get_server_url.cache_clear()
        server_common.is_api_server_local.cache_clear()


@pytest.mark.asyncio
async def test_launch_persists_recovery_generation_in_inner_context(
        monkeypatch):
    executor = recovery_strategy.StrategyExecutor.__new__(
        recovery_strategy.StrategyExecutor)
    executor.job_id = 42
    executor.task_id = 0
    executor.pool = None
    executor.cluster_name = 'job-cluster'
    executor.file_mounts_blob_id = None
    executor.dag = mock.sentinel.dag
    executor.starting = set()
    executor.starting_lock = mock.sentinel.lock
    executor.starting_signal = mock.sentinel.signal
    executor.extra_launch_context = mock.Mock(
        return_value={'strategy_epoch': 'stable'})
    launch = mock.Mock(return_value='launch-request')
    executor._launch_in_workspace = launch
    executor._wait_until_job_starts_on_cluster = mock.AsyncMock(
        return_value=123.0)

    @contextlib.asynccontextmanager
    async def scheduled_launch(*args, **kwargs):
        del args, kwargs
        yield

    monkeypatch.setattr(recovery_strategy.scheduler, 'scheduled_launch',
                        scheduled_launch)
    monkeypatch.setattr(recovery_strategy.state,
                        'get_image_recovery_generation_async',
                        mock.AsyncMock(return_value=3))
    monkeypatch.setattr(recovery_strategy.sdk, 'api_start', mock.Mock())
    monkeypatch.setattr(recovery_strategy.sdk, 'stream_and_get', mock.Mock())
    monkeypatch.setattr(recovery_strategy.global_user_state,
                        'get_handle_from_cluster_name',
                        mock.Mock(return_value=None))
    monkeypatch.setattr(recovery_strategy.usage_lib.messages.usage,
                        'set_internal', mock.Mock())
    monkeypatch.setattr(recovery_strategy.logger, 'debug', mock.Mock())
    monkeypatch.setattr(recovery_strategy, 'ENV_VARS_TO_CLEAR', ())

    assert await executor._launch(max_retry=1) == 123.0
    launch.assert_called_once()
    context = launch.call_args.kwargs['_extra_launch_context']
    assert context == {
        'strategy_epoch': 'stable',
        container_image_consumers.MANAGED_JOB_RECOVERY_GENERATION_KEY: 3,
    }


def test_is_oom_failure_detects_oomkilled():
    exc = RuntimeError(
        'Failed to run setup commands on an instance. (exit code 1). '
        'Pod p terminated: OOMKilled (exit code 137).')
    assert recovery_strategy._is_oom_failure(exc) is True


def test_is_oom_failure_detects_out_of_memory_phrase():
    assert recovery_strategy._is_oom_failure(
        RuntimeError('The container ran out of memory.')) is True


def test_is_oom_failure_is_case_insensitive():
    assert recovery_strategy._is_oom_failure(
        RuntimeError('reason: oomkilled')) is True


def test_is_oom_failure_false_for_unrelated():
    assert recovery_strategy._is_oom_failure(
        RuntimeError('/bin/bash: line 1: conda: command not found')) is False


class TestPoolRecoveryCancellation:
    """Pool recovery targets one job without terminating shared capacity."""

    @staticmethod
    def _executor():
        executor = recovery_strategy.StrategyExecutor.__new__(
            recovery_strategy.StrategyExecutor)
        executor.cluster_name = 'pool-cluster'
        executor.pool = 'pool'
        executor.job_id_on_pool_cluster = 7
        return executor

    @pytest.mark.asyncio
    async def test_cancels_only_assigned_pool_job(self, monkeypatch):
        executor = self._executor()
        lookup = mock.Mock(return_value=mock.sentinel.handle)
        cancel = mock.Mock(return_value='cancel-request')
        get = mock.Mock()
        monkeypatch.setattr(recovery_strategy.global_user_state,
                            'get_handle_from_cluster_name', lookup)
        monkeypatch.setattr(recovery_strategy.sdk, 'cancel', cancel)
        monkeypatch.setattr(recovery_strategy.sdk, 'get', get)
        monkeypatch.setattr(recovery_strategy.usage_lib.messages.usage,
                            'set_internal', mock.Mock())

        await executor._try_cancel_jobs()

        lookup.assert_called_once_with('pool-cluster')
        cancel.assert_called_once_with(
            cluster_name='pool-cluster',
            job_ids=[7],
            _try_cancel_if_cluster_is_init=True,
        )
        get.assert_called_once_with('cancel-request')

    @pytest.mark.asyncio
    async def test_cancel_failure_preserves_shared_pool(self, monkeypatch):
        executor = self._executor()
        cancel = mock.Mock(side_effect=RuntimeError('cancel failed'))
        terminate_cluster = mock.Mock()
        monkeypatch.setattr(
            recovery_strategy.global_user_state,
            'get_handle_from_cluster_name',
            mock.Mock(return_value=mock.sentinel.handle),
        )
        monkeypatch.setattr(recovery_strategy.sdk, 'cancel', cancel)
        monkeypatch.setattr(recovery_strategy.managed_job_utils,
                            'terminate_cluster', terminate_cluster)
        monkeypatch.setattr(recovery_strategy.usage_lib.messages.usage,
                            'set_internal', mock.Mock())

        await executor._try_cancel_jobs()

        cancel.assert_called_once()
        terminate_cluster.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancel_failure_terminates_dedicated_cluster(
            self, monkeypatch):
        executor = self._executor()
        executor.pool = None
        cancel = mock.Mock(side_effect=RuntimeError('cancel failed'))
        terminate_cluster = mock.Mock()
        monkeypatch.setattr(
            recovery_strategy.global_user_state,
            'get_handle_from_cluster_name',
            mock.Mock(return_value=mock.sentinel.handle),
        )
        monkeypatch.setattr(recovery_strategy.sdk, 'cancel', cancel)
        monkeypatch.setattr(recovery_strategy.managed_job_utils,
                            'terminate_cluster', terminate_cluster)
        monkeypatch.setattr(recovery_strategy.usage_lib.messages.usage,
                            'set_internal', mock.Mock())

        await executor._try_cancel_jobs()

        cancel.assert_called_once_with(
            cluster_name='pool-cluster',
            all=True,
            _try_cancel_if_cluster_is_init=True,
        )
        terminate_cluster.assert_called_once_with('pool-cluster')


class TestSubmittedTimestampHandleSnapshot:
    """Submitted-at runtime fallback cannot mix cluster-handle epochs."""

    @staticmethod
    def _executor():
        executor = recovery_strategy.StrategyExecutor.__new__(
            recovery_strategy.StrategyExecutor)
        executor.cluster_name = 'cluster'
        executor.backend = mock.MagicMock()
        executor.job_id_on_pool_cluster = 7
        return executor

    @staticmethod
    def _patch_running_job(monkeypatch):
        refresh = mock.Mock(return_value=(status_lib.ClusterStatus.UP, None))
        get_status = mock.AsyncMock(return_value=(job_lib.JobStatus.RUNNING,
                                                  None))
        monkeypatch.setattr(recovery_strategy.backend_utils,
                            'refresh_cluster_status_handle', refresh)
        monkeypatch.setattr(managed_job_utils, 'get_job_status', get_status)
        monkeypatch.setattr(recovery_strategy.asyncio, 'sleep',
                            mock.AsyncMock())
        return refresh, get_status

    @pytest.mark.asyncio
    async def test_runtime_fallback_reuses_one_handle(self, monkeypatch):
        executor = self._executor()
        handle = mock.MagicMock()
        lookup = mock.Mock(return_value=handle)

        def timestamp_for_handle(_backend, selected, _job_id, **_):
            assert selected is handle
            return 123.0

        get_timestamp = mock.Mock(side_effect=timestamp_for_handle)
        refresh, get_status = self._patch_running_job(monkeypatch)
        monkeypatch.setattr(managed_job_runtime, 'is_registered', lambda: True)
        monkeypatch.setattr(managed_job_runtime, 'get_job_submitted_at',
                            lambda selected, _: None)
        monkeypatch.setattr(recovery_strategy.global_user_state,
                            'get_handle_from_cluster_name', lookup)
        monkeypatch.setattr(managed_job_utils, 'get_job_timestamp',
                            get_timestamp)

        result = await executor._wait_until_job_starts_on_cluster()

        assert result == 123.0
        lookup.assert_called_once_with('cluster')
        get_timestamp.assert_called_once_with(executor.backend,
                                              handle,
                                              7,
                                              get_end_time=False)
        refresh.assert_called_once()
        get_status.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_missing_handle_retries_whole_snapshot(self, monkeypatch):
        executor = self._executor()
        lookup = mock.Mock(return_value=None)
        get_timestamp = mock.Mock(return_value=123.0)
        refresh, get_status = self._patch_running_job(monkeypatch)
        monkeypatch.setattr(managed_job_runtime, 'is_registered', lambda: True)
        monkeypatch.setattr(managed_job_runtime, 'get_job_submitted_at',
                            lambda selected, _: None)
        monkeypatch.setattr(recovery_strategy.global_user_state,
                            'get_handle_from_cluster_name', lookup)
        monkeypatch.setattr(managed_job_utils, 'get_job_timestamp',
                            get_timestamp)

        result = await executor._wait_until_job_starts_on_cluster()

        assert result is None
        assert lookup.call_count == recovery_strategy.MAX_JOB_CHECKING_RETRY
        get_timestamp.assert_not_called()
        assert refresh.call_count == recovery_strategy.MAX_JOB_CHECKING_RETRY
        assert get_status.await_count == recovery_strategy.MAX_JOB_CHECKING_RETRY


class TestJobStartWaitPacing:
    """Every retry of the job-start wait loop must be paced by the gap sleep.

    The loop's contract is MAX_JOB_CHECKING_RETRY polls spread over
    MAX_JOB_CHECKING_RETRY * JOB_STARTED_STATUS_CHECK_GAP_SECONDS of
    wall-clock time. Transient-error paths that `continue` must not skip
    the pacing sleep, or the whole budget burns instantly while the
    cluster is flaky and triggers a premature relaunch.
    """

    @staticmethod
    def _executor():
        executor = recovery_strategy.StrategyExecutor.__new__(
            recovery_strategy.StrategyExecutor)
        executor.cluster_name = 'cluster'
        executor.backend = mock.MagicMock()
        executor.job_id_on_pool_cluster = 7
        return executor

    @staticmethod
    def _patch_sleep(monkeypatch):
        sleep = mock.AsyncMock()
        monkeypatch.setattr(recovery_strategy.asyncio, 'sleep', sleep)
        return sleep

    @pytest.mark.asyncio
    async def test_refresh_failure_retries_are_paced(self, monkeypatch):
        executor = self._executor()
        sleep = self._patch_sleep(monkeypatch)
        refresh = mock.Mock(side_effect=RuntimeError('network flake'))
        monkeypatch.setattr(recovery_strategy.backend_utils,
                            'refresh_cluster_status_handle', refresh)

        result = await executor._wait_until_job_starts_on_cluster()

        assert result is None
        assert refresh.call_count == recovery_strategy.MAX_JOB_CHECKING_RETRY
        # One pacing sleep between each pair of polls (none before the
        # first poll).
        assert sleep.await_count == recovery_strategy.MAX_JOB_CHECKING_RETRY - 1

    @pytest.mark.asyncio
    async def test_transient_job_status_retries_are_paced(self, monkeypatch):
        executor = self._executor()
        sleep = self._patch_sleep(monkeypatch)
        refresh = mock.Mock(return_value=(status_lib.ClusterStatus.UP, None))
        get_status = mock.AsyncMock(return_value=(None, 'ssh timed out'))
        monkeypatch.setattr(recovery_strategy.backend_utils,
                            'refresh_cluster_status_handle', refresh)
        monkeypatch.setattr(managed_job_utils, 'get_job_status', get_status)

        result = await executor._wait_until_job_starts_on_cluster()

        assert result is None
        assert get_status.await_count == recovery_strategy.MAX_JOB_CHECKING_RETRY
        assert sleep.await_count == recovery_strategy.MAX_JOB_CHECKING_RETRY - 1

    @pytest.mark.asyncio
    async def test_init_wait_still_paced_and_no_leading_sleep(
            self, monkeypatch):
        executor = self._executor()
        sleep = self._patch_sleep(monkeypatch)
        refresh = mock.Mock(return_value=(status_lib.ClusterStatus.UP, None))
        get_status = mock.AsyncMock(return_value=(job_lib.JobStatus.INIT, None))
        monkeypatch.setattr(recovery_strategy.backend_utils,
                            'refresh_cluster_status_handle', refresh)
        monkeypatch.setattr(managed_job_utils, 'get_job_status', get_status)

        result = await executor._wait_until_job_starts_on_cluster()

        assert result is None
        assert sleep.await_count == recovery_strategy.MAX_JOB_CHECKING_RETRY - 1

    @pytest.mark.asyncio
    async def test_preemption_breaks_without_sleeping(self, monkeypatch):
        executor = self._executor()
        sleep = self._patch_sleep(monkeypatch)
        refresh = mock.Mock(return_value=(None, None))
        monkeypatch.setattr(recovery_strategy.backend_utils,
                            'refresh_cluster_status_handle', refresh)

        result = await executor._wait_until_job_starts_on_cluster()

        assert result is None
        refresh.assert_called_once()
        sleep.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_immediate_success_never_sleeps(self, monkeypatch):
        executor = self._executor()
        sleep = self._patch_sleep(monkeypatch)
        refresh = mock.Mock(return_value=(status_lib.ClusterStatus.UP, None))
        get_status = mock.AsyncMock(return_value=(job_lib.JobStatus.RUNNING,
                                                  None))
        handle = mock.MagicMock()
        monkeypatch.setattr(recovery_strategy.backend_utils,
                            'refresh_cluster_status_handle', refresh)
        monkeypatch.setattr(managed_job_utils, 'get_job_status', get_status)
        monkeypatch.setattr(managed_job_runtime, 'is_registered', lambda: False)
        monkeypatch.setattr(recovery_strategy.global_user_state,
                            'get_handle_from_cluster_name',
                            mock.Mock(return_value=handle))
        monkeypatch.setattr(managed_job_utils, 'get_job_timestamp',
                            mock.Mock(return_value=42.0))

        result = await executor._wait_until_job_starts_on_cluster()

        assert result == 42.0
        sleep.assert_not_awaited()
