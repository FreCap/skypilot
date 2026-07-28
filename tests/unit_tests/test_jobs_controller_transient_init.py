"""Regression tests for transient INIT confirmation in managed jobs."""
# pylint: disable=protected-access

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from sky.jobs import controller as controller_lib
from sky.jobs.controller import JobController
from sky.skylet import job_lib
from sky.utils import status_lib


class TestTransientInitConfirmation:
    """Tests INIT confirmation when job-status fetch is transiently unavailable."""

    class ExpectedRecovery(Exception):
        """Stops the monitor immediately after it enters recovery."""

    class NoRecovery(Exception):
        """Raised when the monitor loops without ever reaching recovery."""

    @staticmethod
    def _make_controller() -> JobController:
        controller = JobController.__new__(JobController)
        controller._job_id = 1
        controller._pool = None
        controller._backend = MagicMock()
        return controller

    async def _run_until_recovery(self, *, num_nodes, statuses,
                                  refresh_statuses):
        controller = self._make_controller()
        task = MagicMock(name='task')
        task.num_nodes = num_nodes
        executor = MagicMock()
        executor.recover = AsyncMock(side_effect=self.ExpectedRecovery)
        get_status = AsyncMock(side_effect=statuses)
        refresh_cluster = MagicMock(
            side_effect=[(status, None) for status in refresh_statuses])
        sleep = AsyncMock()
        monotonic = MagicMock(
            side_effect=AssertionError('INIT confirmation used retry timer'))
        wall_clock = MagicMock()
        fake_time = SimpleNamespace(monotonic=monotonic, time=wall_clock)
        set_recovering = AsyncMock()

        with patch.object(controller_lib, 'time', fake_time), patch.object(
                controller_lib.asyncio, 'sleep', new=sleep), patch.object(
                    controller_lib.backend_utils,
                    'async_check_network_connection',
                    new=AsyncMock()), patch.object(
                        controller_lib.managed_job_utils,
                        'get_job_status',
                        new=get_status), patch.object(
                            controller_lib.global_user_state,
                            'get_cluster_events',
                            return_value=[]), patch.object(
                                controller_lib.backend_utils,
                                'refresh_cluster_status_handle',
                                new=refresh_cluster), patch.object(
                                    controller_lib.managed_job_state,
                                    'set_recovering_async',
                                    new=set_recovering), pytest.raises(
                                        self.ExpectedRecovery):
            await controller._monitor_one_task(
                task_id=0,
                task=task,
                cluster_name='test-cluster',
                executor=executor,
                callback_func=AsyncMock(),
            )

        return (sleep, get_status, refresh_cluster, monotonic, wall_clock,
                set_recovering, executor)

    @pytest.mark.asyncio
    async def test_multinode_transient_init_requires_confirmation(self):
        threshold = controller_lib._NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
        results = await self._run_until_recovery(
            num_nodes=16,
            statuses=[(None, 'transient')] * threshold,
            refresh_statuses=[status_lib.ClusterStatus.INIT] * threshold,
        )
        (sleep, get_status, refresh_cluster, monotonic, wall_clock,
         set_recovering, executor) = results

        assert [
            call.args[0] for call in sleep.await_args_list
        ] == [controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS
             ] * threshold
        assert get_status.await_count == threshold
        assert refresh_cluster.call_count == threshold
        monotonic.assert_not_called()
        wall_clock.assert_not_called()
        set_recovering.assert_awaited_once()
        executor.recover.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_single_node_transient_init_waits_after_running_status(self):
        threshold = controller_lib._NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
        with patch.object(controller_lib.logger, 'info') as log:
            results = await self._run_until_recovery(
                num_nodes=1,
                statuses=[(job_lib.JobStatus.RUNNING, None)] +
                [(None, 'transient')] * threshold,
                refresh_statuses=[status_lib.ClusterStatus.INIT] * threshold,
            )
        (sleep, get_status, refresh_cluster, monotonic, wall_clock,
         set_recovering, executor) = results

        confirmation_logs = [
            call.args[0]
            for call in log.call_args_list
            if 'waiting for confirmation' in call.args[0]
        ]
        assert len(confirmation_logs) == threshold - 1
        for observation, message in enumerate(confirmation_logs, start=1):
            assert (f'({observation}/{threshold} consecutive observations)'
                    in message)
        assert [
            call.args[0] for call in sleep.await_args_list
        ] == [controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS
             ] * (threshold + 1)
        assert get_status.await_count == threshold + 1
        assert refresh_cluster.call_count == threshold
        monotonic.assert_not_called()
        wall_clock.assert_not_called()
        set_recovering.assert_awaited_once()
        executor.recover.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_single_node_running_status_resets_init_confirmation(self):
        threshold = controller_lib._NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
        statuses = ([(job_lib.JobStatus.RUNNING, None), (None, 'transient'),
                     (job_lib.JobStatus.RUNNING, None)] +
                    [(None, 'transient')] * threshold)
        with patch.object(controller_lib.logger, 'info') as log:
            results = await self._run_until_recovery(
                num_nodes=1,
                statuses=statuses,
                refresh_statuses=[status_lib.ClusterStatus.INIT] *
                (threshold + 1),
            )
        (sleep, get_status, refresh_cluster, monotonic, wall_clock,
         set_recovering, executor) = results

        confirmation_logs = [
            call.args[0]
            for call in log.call_args_list
            if 'waiting for confirmation' in call.args[0]
        ]
        expected_observations = [1] + list(range(1, threshold))
        assert len(confirmation_logs) == len(expected_observations)
        for observation, message in zip(expected_observations,
                                        confirmation_logs,
                                        strict=True):
            assert (f'({observation}/{threshold} consecutive observations)'
                    in message)
        assert sleep.await_count == len(statuses)
        assert get_status.await_count == len(statuses)
        assert refresh_cluster.call_count == threshold + 1
        monotonic.assert_not_called()
        wall_clock.assert_not_called()
        set_recovering.assert_awaited_once()
        executor.recover.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_flapping_cluster_still_recovers_within_the_budget(self):
        # A cluster that alternates UP/INIT resets the confirmation streak on
        # every UP tick, so the wall-clock status-fetch budget is the only
        # backstop left. Clearing that budget on each not-UP tick restarts the
        # clock forever: the job then never recovers and never fails.
        threshold = controller_lib._NOT_UP_CONFIRMATIONS_BEFORE_RECOVERY
        for period in range(2, threshold + 4):
            refreshes = await self._run_flapping_until_recovery(period=period)
            budget = (controller_lib.managed_job_utils.
                      JOB_STATUS_FETCH_TOTAL_TIMEOUT_SECONDS)
            gap = (
                controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS)
            # Recovery must arrive on the budget, not on the streak.
            assert refreshes <= budget / gap + threshold, (
                f'no recovery within the budget for INIT every {period} '
                f'refreshes: took {refreshes}')

    async def _run_flapping_until_recovery(self, *, period, max_refreshes=400):
        """Drive the monitor with a flapping cluster and a dead status fetch."""
        controller = self._make_controller()
        task = MagicMock(name='task')
        task.num_nodes = 16
        executor = MagicMock()
        executor.recover = AsyncMock(side_effect=self.ExpectedRecovery)

        clock = {'now': 1000.0}
        refreshes = {'count': 0}

        async def advancing_sleep(seconds, *args, **kwargs):
            del args, kwargs
            clock['now'] += float(seconds)

        def flapping_refresh(*args, **kwargs):
            del args, kwargs
            refreshes['count'] += 1
            if refreshes['count'] > max_refreshes:
                raise self.NoRecovery(
                    f'no recovery after {max_refreshes} refreshes '
                    f'({clock["now"] - 1000.0:.0f}s simulated)')
            handle = MagicMock(launched_resources=MagicMock(
                need_cleanup_after_preemption_or_failure=lambda: False))
            status = (status_lib.ClusterStatus.INIT if refreshes['count'] %
                      period == 0 else status_lib.ClusterStatus.UP)
            return (status, handle)

        fake_time = SimpleNamespace(monotonic=lambda: clock['now'],
                                    time=lambda: clock['now'])

        with patch.object(controller_lib, 'time', fake_time), patch.object(
                controller_lib.asyncio,
                'sleep',
                new=AsyncMock(side_effect=advancing_sleep)), patch.object(
                    controller_lib.backend_utils,
                    'async_check_network_connection',
                    new=AsyncMock()), patch.object(
                        controller_lib.managed_job_utils,
                        'get_job_status',
                        new=AsyncMock(
                            return_value=(None, 'transient'))), patch.object(
                                controller_lib.global_user_state,
                                'get_cluster_events',
                                return_value=[]), patch.object(
                                    controller_lib.backend_utils,
                                    'refresh_cluster_status_handle',
                                    new=flapping_refresh), patch.object(
                                        controller_lib.managed_job_state,
                                        'set_recovering_async',
                                        new=AsyncMock()), pytest.raises(
                                            self.ExpectedRecovery):
            await controller._monitor_one_task(
                task_id=0,
                task=task,
                cluster_name='test-cluster',
                executor=executor,
                callback_func=AsyncMock(),
            )

        return refreshes['count']

    @pytest.mark.asyncio
    async def test_transient_stopped_cluster_recovers_immediately(self):
        results = await self._run_until_recovery(
            num_nodes=16,
            statuses=[(None, 'transient')],
            refresh_statuses=[status_lib.ClusterStatus.STOPPED],
        )
        (sleep, get_status, refresh_cluster, monotonic, wall_clock,
         set_recovering, executor) = results

        assert [call.args[0] for call in sleep.await_args_list] == [
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS
        ]
        assert get_status.await_count == 1
        assert refresh_cluster.call_count == 1
        monotonic.assert_not_called()
        wall_clock.assert_not_called()
        set_recovering.assert_awaited_once()
        executor.recover.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_single_node_transient_init_without_running_status_recovers_immediately(
            self):
        results = await self._run_until_recovery(
            num_nodes=1,
            statuses=[(None, 'transient')],
            refresh_statuses=[status_lib.ClusterStatus.INIT],
        )
        (sleep, get_status, refresh_cluster, monotonic, wall_clock,
         set_recovering, executor) = results

        assert [call.args[0] for call in sleep.await_args_list] == [
            controller_lib.managed_job_utils.JOB_STATUS_CHECK_GAP_SECONDS
        ]
        assert get_status.await_count == 1
        assert refresh_cluster.call_count == 1
        monotonic.assert_not_called()
        wall_clock.assert_not_called()
        set_recovering.assert_awaited_once()
        executor.recover.assert_awaited_once()
