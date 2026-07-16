"""Tests for the managed jobs pool-down CLI lifecycle."""

from unittest import mock

from click import testing as cli_testing

from sky.client.cli import command


def _run_pool_down_cancel_wait(queue_snapshots, clock):
    queue_result_version = mock.Mock()
    queue_result_version.v2.return_value = False
    with mock.patch.object(command, 'time', clock), \
         mock.patch.object(
             command.cli_utils,
             'get_managed_job_queue',
             return_value=('queue-request', queue_result_version),
         ) as get_queue, \
         mock.patch.object(
             command.sdk,
             'stream_and_get',
             side_effect=queue_snapshots,
         ), \
         mock.patch.object(
             command.managed_jobs,
             'cancel',
             return_value='cancel-request',
         ) as cancel, \
         mock.patch.object(command.sdk, 'get') as get, \
         mock.patch.object(
             command.managed_jobs,
             'pool_down',
             return_value='pool-down-request',
         ) as pool_down, \
         mock.patch.object(command, '_async_call_or_wait') as wait:
        result = cli_testing.CliRunner().invoke(command.jobs_pool_down,
                                                ['test-pool', '-y'])
    return result, get_queue, cancel, get, pool_down, wait


def _running_pool_job():
    return {
        'job_id': 1,
        'status': command.ManagedJobStatus.RUNNING,
        'pool': 'test-pool',
    }


def test_pool_down_cancel_wait_terminal_snapshot_wins_at_deadline():
    clock = mock.Mock()
    clock.time.side_effect = [0, 299, 301]
    clock.monotonic.side_effect = [0, 299]

    result, get_queue, cancel, get, pool_down, wait = (
        _run_pool_down_cancel_wait(
            [[_running_pool_job()], [_running_pool_job()], []], clock))

    assert result.exit_code == 0, result.output
    assert 'All jobs cancelled.' in result.output
    assert 'Warning: Timeout waiting' not in result.output
    clock.sleep.assert_called_once_with(1)
    clock.time.assert_not_called()
    assert clock.monotonic.call_count == 2
    assert get_queue.call_count == 3
    cancel.assert_called_once_with(job_ids=[1])
    get.assert_called_once_with('cancel-request')
    pool_down.assert_called_once_with(('test-pool',), all=False, purge=False)
    wait.assert_called_once_with('pool-down-request', False,
                                 'sky.jobs.pool_down')


def test_pool_down_cancel_wait_times_out_from_remaining_snapshot():
    clock = mock.Mock()
    clock.time.side_effect = [0, 299, 301, 301]
    clock.monotonic.side_effect = [100, 399, 400]

    result, get_queue, cancel, get, pool_down, wait = (
        _run_pool_down_cancel_wait(
            [[_running_pool_job()], [_running_pool_job()],
             [_running_pool_job()]], clock))

    assert result.exit_code == 0, result.output
    assert 'Warning: Timeout waiting' in result.output
    assert 'All jobs cancelled.' not in result.output
    clock.sleep.assert_called_once_with(1)
    clock.time.assert_not_called()
    assert clock.monotonic.call_count == 3
    assert get_queue.call_count == 3
    cancel.assert_called_once_with(job_ids=[1])
    get.assert_called_once_with('cancel-request')
    pool_down.assert_called_once_with(('test-pool',), all=False, purge=False)
    wait.assert_called_once_with('pool-down-request', False,
                                 'sky.jobs.pool_down')


def test_pool_down_cancel_wait_immediate_terminal_snapshot():
    clock = mock.Mock()
    clock.time.side_effect = [0, 0]
    clock.monotonic.return_value = 100

    result, get_queue, cancel, get, pool_down, wait = (
        _run_pool_down_cancel_wait([[_running_pool_job()], []], clock))

    assert result.exit_code == 0, result.output
    assert 'All jobs cancelled.' in result.output
    assert 'Warning: Timeout waiting' not in result.output
    clock.sleep.assert_not_called()
    clock.time.assert_not_called()
    clock.monotonic.assert_called_once_with()
    assert get_queue.call_count == 2
    cancel.assert_called_once_with(job_ids=[1])
    get.assert_called_once_with('cancel-request')
    pool_down.assert_called_once_with(('test-pool',), all=False, purge=False)
    wait.assert_called_once_with('pool-down-request', False,
                                 'sky.jobs.pool_down')
