"""Tests for the cluster job logs CLI."""
from unittest import mock

from click.testing import CliRunner

from sky import exceptions
from sky.client.cli import command


def test_status_without_job_id_queries_latest_job():
    """An omitted variadic Click argument maps to the SDK's None sentinel."""
    request_id = object()
    with mock.patch.object(command.sdk,
                           'job_status',
                           return_value=request_id) as mock_job_status, \
         mock.patch.object(command.sdk,
                           'stream_and_get',
                           return_value={None: None}) as mock_stream_and_get:
        result = CliRunner().invoke(command.logs, ['--status', 'test-cluster'])

    assert result.exit_code == exceptions.JobExitCode.NOT_FOUND
    mock_job_status.assert_called_once_with('test-cluster', None)
    mock_stream_and_get.assert_called_once_with(request_id)
