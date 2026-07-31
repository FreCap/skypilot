"""Managed-job controller-log follow lifecycle tests."""

# pylint: disable=protected-access

from unittest import mock

import pytest

from sky import exceptions
from sky.jobs import controller_log_stream
from sky.jobs import state as managed_job_state
from sky.jobs import utils as jobs_utils


@pytest.mark.parametrize(('status', 'schedule_state', 'expected'), [
    (None, None, False),
    (managed_job_state.ManagedJobStatus.RUNNING, None, False),
    (managed_job_state.ManagedJobStatus.SUCCEEDED, None, True),
    (managed_job_state.ManagedJobStatus.SUCCEEDED,
     managed_job_state.ManagedJobScheduleState.ALIVE, False),
    (managed_job_state.ManagedJobStatus.SUCCEEDED,
     managed_job_state.ManagedJobScheduleState.DONE, True),
    (managed_job_state.ManagedJobStatus.RUNNING,
     managed_job_state.ManagedJobScheduleState.DONE, True),
])
def test_follow_completion_boundaries(status, schedule_state, expected):
    follow_state = managed_job_state.ControllerLogFollowState(
        status, schedule_state)

    assert controller_log_stream._controller_log_follow_is_complete(
        follow_state) is expected


def test_follow_waits_for_done_schedule_state(tmp_path, capsys):
    log_path = tmp_path / '42.log'
    log_path.write_text('complete\n', encoding='utf-8')

    with (mock.patch.object(jobs_utils,
                            'controller_log_file_for_job',
                            return_value=str(log_path)),
          mock.patch.object(
              managed_job_state,
              'get_controller_log_follow_state',
              side_effect=[
                  managed_job_state.ControllerLogFollowState(
                      managed_job_state.ManagedJobStatus.SUCCEEDED,
                      managed_job_state.ManagedJobScheduleState.ALIVE),
                  managed_job_state.ControllerLogFollowState(
                      managed_job_state.ManagedJobStatus.SUCCEEDED,
                      managed_job_state.ManagedJobScheduleState.DONE),
              ]) as get_follow_state,
          mock.patch.object(controller_log_stream.time, 'sleep') as sleep):
        message, exit_code = jobs_utils.stream_logs(job_id=42,
                                                    job_name=None,
                                                    controller=True,
                                                    follow=True)

    assert capsys.readouterr().out == 'complete\n'
    assert get_follow_state.call_args_list == [mock.call(42), mock.call(42)]
    assert sleep.call_args_list == [
        mock.call(jobs_utils.log_lib.SKY_LOG_TAILING_GAP_SECONDS),
        mock.call(1 + jobs_utils.log_lib.SKY_LOG_TAILING_GAP_SECONDS),
    ]
    assert 'Job finished (status: ManagedJobStatus.SUCCEEDED).' in message
    assert exit_code == exceptions.JobExitCode.SUCCEEDED
