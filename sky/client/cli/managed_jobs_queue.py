"""Managed-jobs queue CLI input and output translation."""

import concurrent.futures
import datetime
import json
import time
import traceback
from typing import Any

import click
import colorama

from sky import exceptions
from sky import jobs as managed_jobs
from sky.client import sdk
from sky.client.cli import click_utils
from sky.client.cli import flags
from sky.client.cli import table_utils
from sky.client.cli import utils as cli_utils
from sky.jobs.state import ManagedJobStatus
from sky.schemas.api import responses
from sky.server import common as server_common
from sky.usage import usage_lib
from sky.utils import common
from sky.utils import common_utils
from sky.utils import controller_utils
from sky.utils import env_options
from sky.utils import resources_utils
from sky.utils import rich_utils
from sky.utils import status_lib

_DocumentedCodeCommand = click_utils.DocumentedCodeCommand

# The maximum number of in-progress managed jobs to show in the status
# command.
_NUM_MANAGED_JOBS_TO_SHOW_IN_STATUS = 5
_NUM_MANAGED_JOBS_TO_SHOW = 50
_DEFAULT_MANAGED_JOB_FIELDS_TO_GET = [
    'job_id', 'task_id', 'workspace', 'job_name', 'task_name', 'resources',
    'submitted_at', 'end_at', 'job_duration', 'recovery_count', 'status',
    'pool', 'is_primary_in_job_group', 'batch_total_batches',
    'batch_completed_batches'
]
_VERBOSE_MANAGED_JOB_FIELDS_TO_GET = _DEFAULT_MANAGED_JOB_FIELDS_TO_GET + [
    'current_cluster_name', 'job_id_on_pool_cluster', 'start_at', 'infra',
    'cloud', 'region', 'zone', 'cluster_resources', 'schedule_state', 'details',
    'failure_reason', 'metadata'
]
_USER_NAME_FIELD = ['user_name']
_USER_HASH_FIELD = ['user_hash']


def _handle_jobs_queue_request(
    request_id: server_common.RequestId[list[responses.ManagedJobRecord] |
                                        tuple[list[responses.ManagedJobRecord],
                                              int, dict[str, int], int]],
    show_all: bool,
    show_user: bool,
    max_num_jobs_to_show: int | None,
    pool_status_request_id: server_common.RequestId[list[dict[str, Any]]] |
    None = None,
    is_called_by_user: bool = False,
    only_in_progress: bool = False,
    queue_result_version: cli_utils.QueueResultVersion = cli_utils.
    QueueResultVersion.V1,
) -> tuple[int | None, str]:
    """Get the in-progress managed jobs.

    Args:
        request_id: The request ID for managed jobs.
        pool_status_request_id: The request ID for pool status, or None.
        show_all: Show all information of each job (e.g., region, price).
        show_user: Show the user who submitted the job.
        max_num_jobs_to_show: If not None, limit the number of jobs to show to
            this number, which is mainly used by `sky status`
            and `sky jobs queue`.
        is_called_by_user: If this function is called by user directly, or an
            internal call.
        only_in_progress: If True, only return the number of in-progress jobs.
        queue_result_version: The version of the queue result.

    Returns:
        A tuple of (num_in_progress_jobs, msg). If num_in_progress_jobs is None,
        it means there is an error when querying the managed jobs. In this case,
        msg contains the error message. Otherwise, msg contains the formatted
        managed job table.
    """
    # TODO(SKY-980): remove unnecessary fallbacks on the client side.
    num_in_progress_jobs: int | None = None
    msg = ''
    status_counts: dict[str, int] | None = None
    pool_status_result = None
    try:
        if not is_called_by_user:
            usage_lib.messages.usage.set_internal()
        # Call both stream_and_get functions in parallel
        def get_jobs_queue_result():
            return sdk.stream_and_get(request_id)

        def get_pool_status_result():
            if pool_status_request_id is not None:
                try:
                    return sdk.stream_and_get(pool_status_request_id)
                except Exception:  # pylint: disable=broad-except
                    # If getting pool status fails, just continue without it
                    return None
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            jobs_future = executor.submit(get_jobs_queue_result)
            pool_status_future = executor.submit(get_pool_status_result)

            result = jobs_future.result()
            pool_status_result = pool_status_future.result()

        if queue_result_version.v2():
            managed_jobs_, total, status_counts, _ = result
            if only_in_progress:
                num_in_progress_jobs = 0
                if status_counts:
                    for status_value, count in status_counts.items():
                        status_enum = managed_jobs.ManagedJobStatus(
                            status_value)
                        if not status_enum.is_terminal():
                            num_in_progress_jobs += count
            else:
                num_in_progress_jobs = total
        else:
            managed_jobs_ = result
            num_in_progress_jobs = len(
                set(job['job_id'] for job in managed_jobs_))
    except exceptions.ClusterNotUpError as e:
        controller_status = e.cluster_status
        msg = str(e)
        if controller_status is None:
            msg += (f' (See: {colorama.Style.BRIGHT}sky jobs -h'
                    f'{colorama.Style.RESET_ALL})')
        elif (controller_status == status_lib.ClusterStatus.STOPPED and
              is_called_by_user):
            msg += (f' (See finished managed jobs: {colorama.Style.BRIGHT}'
                    f'sky jobs queue --refresh{colorama.Style.RESET_ALL})')
    except RuntimeError as e:
        try:
            # Check the controller status again, as the RuntimeError is likely
            # due to the controller being autostopped when querying the jobs.
            # Since we are client-side, we may not know the exact name of the
            # controller, so use the prefix with a wildcard.
            # Query status of the controller cluster.
            records = sdk.get(
                sdk.status(cluster_names=[common.JOB_CONTROLLER_PREFIX + '*'],
                           all_users=True))
            if (not records or
                    records[0]['status'] == status_lib.ClusterStatus.STOPPED):
                controller = controller_utils.Controllers.JOBS_CONTROLLER.value
                msg = controller.default_hint_if_non_existent
        except Exception:  # pylint: disable=broad-except
            # This is to an best effort to find the latest controller status to
            # print more helpful message, so we can ignore any exception to
            # print the original error.
            pass
        if not msg:
            msg = (
                'Failed to query managed jobs due to connection '
                'issues. Try again later. '
                f'Details: {common_utils.format_exception(e, use_bracket=True)}'
            )
    except Exception as e:  # pylint: disable=broad-except
        msg = ''
        if env_options.Options.SHOW_DEBUG_INFO.get():
            msg += traceback.format_exc()
            msg += '\n'
        msg += ('Failed to query managed jobs: '
                f'{common_utils.format_exception(e, use_bracket=True)}')
    else:
        msg = table_utils.format_job_table(
            managed_jobs_,
            pool_status=pool_status_result,
            show_all=show_all,
            show_user=show_user,
            max_jobs=max_num_jobs_to_show,
            status_counts=status_counts,
        )
    return num_in_progress_jobs, msg


# Value the ``-s``/``--status`` option takes when given with no argument. It is
# a deprecated alias for ``--skip-finished`` (see jobs_queue).
# TODO(kevin): remove in 0.15.0, after which a bare ``-s`` is invalid and ``-s``
# is solely the short flag for ``--status``.
_SKIP_FINISHED_SENTINEL = '__skip_finished__'


class StatusList(click.Choice):
    """Comma-separated, case-insensitive choices.

    Returns a list so a single ``--status FAILED,FAILED_SETUP`` and a repeated
    ``--status FAILED --status FAILED_SETUP`` are both accepted; with
    ``multiple=True`` the option then yields a tuple of lists to flatten.
    """

    def convert(self, value, param, ctx):
        # A bare ``-s`` yields the sentinel; pass it through unvalidated so the
        # handler can treat it as --skip-finished.
        if value == _SKIP_FINISHED_SENTINEL:
            return [value]
        return [
            super(StatusList, self).convert(v.strip(), param, ctx)
            for v in value.split(',')
            if v.strip()
        ]


# Accepted absolute date/time formats for --after / --before. ISO date and
# date-time (space or 'T' separator) first; US m-d-Y for familiarity. Naive
# values are interpreted in the local timezone.
_SUBMITTED_AT_DATETIME_FORMATS = (
    '%Y-%m-%d',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%dT%H:%M',
    '%m-%d-%Y',
    '%m-%d-%Y %H:%M:%S',
)


def _parse_datetime_to_epoch(value: str) -> float:
    """Parses an absolute date/time string into an epoch timestamp (seconds).

    Naive datetimes are interpreted in the local timezone.

    Raises:
        ValueError: if the value matches none of the accepted formats.
    """
    for fmt in _SUBMITTED_AT_DATETIME_FORMATS:
        try:
            return datetime.datetime.strptime(value,
                                              fmt).astimezone().timestamp()
        except ValueError:
            continue
    raise ValueError(f'Invalid date/time {value!r}. Use e.g. "2026-01-13", '
                     '"2026-01-13 15:30:00", or "2026-01-13T15:30:00".')


@click.command('queue', cls=_DocumentedCodeCommand)
@flags.config_option(expose_value=False)
@flags.verbose_option()
@click.option(
    '--limit',
    '-l',
    default=_NUM_MANAGED_JOBS_TO_SHOW,
    type=int,
    required=False,
    help=(f'Number of jobs to show, default is {_NUM_MANAGED_JOBS_TO_SHOW},'
          f' use "-a/--all" to show all jobs.'))
@click.option(
    '--refresh',
    '-r',
    default=False,
    is_flag=True,
    required=False,
    help='Query the latest statuses, restarting the jobs controller if stopped.'
)
@click.option('--skip-finished',
              default=False,
              is_flag=True,
              required=False,
              help='Show only pending/running jobs\' information.')
@click.option('-s',
              '--status',
              'statuses',
              is_flag=False,
              flag_value=_SKIP_FINISHED_SENTINEL,
              multiple=True,
              type=StatusList([s.value for s in ManagedJobStatus],
                              case_sensitive=False),
              required=False,
              help='Filter by status, comma-separated '
              '(e.g. -s FAILED,FAILED_SETUP). A bare -s (no value) is a '
              'deprecated alias for --skip-finished.')
@click.option(
    '--since',
    default=None,
    type=str,
    required=False,
    help=('Show only jobs submitted within this time window, relative to now '
          '(e.g. "30m", "48h", "7d", "2w"). A bare number is seconds. '
          'Mutually exclusive with --after.'))
@click.option(
    '--after',
    default=None,
    type=str,
    required=False,
    help=('Show only jobs submitted at or after this absolute local time '
          '(e.g. "2026-01-13" or "2026-01-13 15:30:00"). Mutually exclusive '
          'with --since.'))
@click.option(
    '--before',
    default=None,
    type=str,
    required=False,
    help=('Show only jobs submitted at or before this absolute local time '
          '(e.g. "2026-01-13" or "2026-01-13 15:30:00").'))
@flags.all_users_option('Show jobs from all users.')
@flags.all_option('Show all jobs.')
@flags.output_format_option()
@usage_lib.entrypoint
# pylint: disable=redefined-builtin
def jobs_queue(verbose: bool,
               refresh: bool,
               skip_finished: bool,
               statuses: tuple[list[str], ...],
               since: str | None,
               after: str | None,
               before: str | None,
               all_users: bool,
               all: bool,
               limit: int,
               output_format: str = 'table'):
    """Show statuses of managed jobs.

    Each managed jobs can have one of the following statuses:

    - ``PENDING``: Job is waiting for a free slot on the jobs controller to be
      accepted.

    - ``STARTING``: Job is starting (provisioning a cluster for the job).

    - ``RUNNING``: Job is running.

    - ``RECOVERING``: The cluster of the job is recovering from a preemption.

    - ``SUCCEEDED``: Job succeeded.

    - ``CANCELLING``: Job was requested to be cancelled by the user, and the
      cancellation is in progress.

    - ``CANCELLED``: Job was cancelled by the user.

    - ``FAILED``: Job failed due to an error from the job itself.

    - ``FAILED_SETUP``: Job failed due to an error from the job's ``setup``
      commands.

    - ``FAILED_PRECHECKS``: Job failed due to an error from our prechecks such
      as invalid cluster names or an infeasible resource is specified.

    - ``FAILED_NO_RESOURCE``: Job failed due to resources being unavailable
      after a maximum number of retries.

    - ``FAILED_CONTROLLER``: Job failed due to an unexpected error in the spot
      controller.

    If the job failed, either due to user code or resource unavailability, the
    error log can be found with ``sky jobs logs --controller``, e.g.:

    .. code-block:: bash

      sky jobs logs --controller job_id

    This also shows the logs for provisioning and any preemption and recovery
    attempts.

    (Tip) To fetch job statuses every 60 seconds, use ``watch``:

    .. code-block:: bash

      watch -n60 sky jobs queue

    (Tip) To show only the latest 10 jobs, use ``-l/--limit 10``:

    .. code-block:: bash

      sky jobs queue -l 10

    (Tip) To filter by status, use ``-s``/``--status`` (comma-separated):

    .. code-block:: bash

      sky jobs queue -s FAILED,FAILED_SETUP

    (Tip) To show only active (pending/running) jobs, use ``--skip-finished``:

    .. code-block:: bash

      sky jobs queue --skip-finished

    (Tip) To show only jobs submitted in the last 7 days, use ``--since``:

    .. code-block:: bash

      sky jobs queue --since 7d

    (Tip) To filter by an absolute submission window, use ``--after`` and/or
    ``--before``:

    .. code-block:: bash

      sky jobs queue --after 2026-01-01 --before 2026-01-31

    """
    status_filter = [status for group in statuses for status in group]
    # TODO(kevin): remove in 0.15.0, along with _SKIP_FINISHED_SENTINEL and the
    # flag_value on -s/--status.
    if _SKIP_FINISHED_SENTINEL in status_filter:
        click.secho(
            'Warning: `-s` without a value is a deprecated alias for '
            '`--skip-finished` and will be removed in 0.15.0. Use '
            '`--skip-finished`, or pass a status (e.g. `-s RUNNING`).',
            fg='yellow',
            err=True)
        skip_finished = True
        status_filter = [
            s for s in status_filter if s != _SKIP_FINISHED_SENTINEL
        ]
    status_filter = status_filter or None
    if since is not None and after is not None:
        raise click.UsageError(
            '--since and --after are mutually exclusive: --since is a relative '
            'window from now, --after is an absolute lower bound.')
    submitted_after = None
    if since is not None:
        try:
            since_seconds = resources_utils.parse_time_seconds(since)
        except ValueError as e:
            raise click.BadParameter(str(e), param_hint='--since') from e
        submitted_after = time.time() - since_seconds
    elif after is not None:
        try:
            submitted_after = _parse_datetime_to_epoch(after)
        except ValueError as e:
            raise click.BadParameter(str(e), param_hint='--after') from e
    submitted_before = None
    if before is not None:
        try:
            submitted_before = _parse_datetime_to_epoch(before)
        except ValueError as e:
            raise click.BadParameter(str(e), param_hint='--before') from e
    if output_format != flags.OUTPUT_FORMAT_JSON:
        click.secho('Fetching managed job statuses...', fg='cyan')
    with rich_utils.client_status('[cyan]Checking managed jobs[/]'):
        max_num_jobs_to_show = (limit if not all else None)
        fields = _DEFAULT_MANAGED_JOB_FIELDS_TO_GET
        if verbose:
            fields = _VERBOSE_MANAGED_JOB_FIELDS_TO_GET
        if all_users:
            fields = fields + _USER_NAME_FIELD
            if verbose:
                fields = fields + _USER_HASH_FIELD
        # Call both cli_utils.get_managed_job_queue and managed_jobs.pool_status
        # in parallel
        def get_managed_jobs_queue():
            return cli_utils.get_managed_job_queue(
                refresh=refresh,
                skip_finished=skip_finished,
                all_users=all_users,
                limit=max_num_jobs_to_show,
                fields=fields,
                statuses=status_filter,
                submitted_after=submitted_after,
                submitted_before=submitted_before)

        def get_pool_status():
            try:
                return managed_jobs.pool_status(pool_names=None)
            except Exception:  # pylint: disable=broad-except
                # If pool_status fails, we'll just skip the worker information
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            managed_jobs_future = executor.submit(get_managed_jobs_queue)
            pool_status_future = executor.submit(get_pool_status)

            (managed_jobs_request_id,
             queue_result_version) = managed_jobs_future.result()
            pool_status_request_id = pool_status_future.result()

        if output_format == flags.OUTPUT_FORMAT_JSON:
            result = sdk.stream_and_get(managed_jobs_request_id)
            if queue_result_version.v2():
                managed_jobs_, _, _, _ = result
            else:
                managed_jobs_ = result
            click.echo(
                json.dumps([r.model_dump(mode='json') for r in managed_jobs_],
                           indent=2))
            return

        num_jobs, msg = _handle_jobs_queue_request(
            managed_jobs_request_id,
            pool_status_request_id=pool_status_request_id,
            show_all=verbose,
            show_user=all_users,
            max_num_jobs_to_show=max_num_jobs_to_show,
            is_called_by_user=True,
            queue_result_version=queue_result_version,
        )
    if not skip_finished:
        in_progress_only_hint = ''
    else:
        in_progress_only_hint = ' (showing in-progress jobs only)'
    click.echo(f'{colorama.Fore.CYAN}{colorama.Style.BRIGHT}'
               f'Managed jobs{colorama.Style.RESET_ALL}'
               f'{in_progress_only_hint}\n{msg}')
    if max_num_jobs_to_show and num_jobs and max_num_jobs_to_show < num_jobs:
        click.echo(
            f'{colorama.Fore.CYAN}'
            f'Only showing the latest {max_num_jobs_to_show} '
            f'managed jobs'
            f'(use --limit to show more managed jobs or '
            f'--all to show all managed jobs) {colorama.Style.RESET_ALL} ')
