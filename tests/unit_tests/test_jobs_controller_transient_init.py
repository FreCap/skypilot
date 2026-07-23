"""Regression tests for transient INIT confirmation in managed jobs."""
# pylint: disable=protected-access

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from sky.jobs import controller as controller_lib
from sky.jobs.controller import JobController
from sky.utils import status_lib


class TestTransientInitConfirmation:
    """Tests INIT confirmation when job-status fetch is transiently unavailable."""

    class ExpectedRecovery(Exception):
        """Stops the monitor immediately after it enters recovery."""

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
