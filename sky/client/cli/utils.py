"""Utility functions for the CLI."""
import enum
import typing

import click

from sky import exceptions
from sky import jobs as managed_jobs
from sky import sky_logging
from sky.schemas.api import responses
from sky.server import common as server_common
from sky.utils import infra_utils
from sky.utils import ux_utils

logger = sky_logging.init_logger(__name__)


def handle_infra_cloud_region_zone_options(infra: str | None, cloud: str | None,
                                           region: str | None,
                                           zone: str | None):
    """Handle the backward compatibility for --infra and --cloud/region/zone.

    Returns:
        cloud, region, zone
    """
    if cloud is not None or region is not None or zone is not None:
        click.secho(
            'The --cloud, --region, and --zone options are deprecated. '
            'Use --infra instead.',
            fg='yellow')
        if infra is not None:
            with ux_utils.print_exception_no_traceback():
                raise ValueError('Cannot specify both --infra and '
                                 '--cloud, --region, or --zone.')

    if infra is not None:
        infra_info = infra_utils.InfraInfo.from_str(infra)
        # Convert None to '*' to ensure proper override behavior.
        cloud = infra_info.cloud if infra_info.cloud is not None else '*'
        region = infra_info.region if infra_info.region is not None else '*'
        zone = infra_info.zone if infra_info.zone is not None else '*'
    return cloud, region, zone


class QueueResultVersion(enum.Enum):
    """The version of the queue result.

    V1: The old version of the queue result.
        - job_records (List[responses.ManagedJobRecord]): A list of dicts,
           with each dict containing the information of a job.
    V2: The new version of the queue result.
        - job_records (List[responses.ManagedJobRecord]): A list of dicts,
           with each dict containing the information of a job.
        - total (int): Total number of jobs after filter.
        - status_counts (Dict[str, int]): Status counts after filter.
        - total_no_filter (int): Total number of jobs before filter.
    """
    V1 = 'v1'
    V2 = 'v2'

    def v2(self) -> bool:
        return self == QueueResultVersion.V2


def get_managed_job_queue(
    refresh: bool,
    skip_finished: bool = False,
    all_users: bool = False,
    job_ids: list[int] | None = None,
    limit: int | None = None,
    fields: list[str] | None = None,
    statuses: list[str] | None = None,
    submitted_after: float | None = None,
    submitted_before: float | None = None,
) -> tuple[server_common.RequestId[list[responses.ManagedJobRecord] | tuple[
        list[responses.ManagedJobRecord], int, dict[str, int], int]],
           QueueResultVersion]:
    """Gets statuses of managed jobs.

    Please refer to sky.cli.job_queue for documentation.

    Args:
        refresh: Whether to restart the jobs controller if it is stopped.
        skip_finished: Whether to skip finished jobs.
        all_users: Whether to show all users' jobs.
        job_ids: IDs of the managed jobs to show.
        limit: Number of jobs to show.
        fields: Fields to get for the managed jobs.
        statuses: Only return jobs whose status is in this list.
        submitted_after: Only show jobs submitted at or after this epoch time
            (seconds).
        submitted_before: Only show jobs submitted at or before this epoch
            time (seconds).

    Returns:
        - the request ID of the queue request
        - the version of the queue result

    Request Raises:
        sky.exceptions.ClusterNotUpError: the jobs controller is not up or
          does not exist.
        RuntimeError: if failed to get the managed jobs with ssh.
    """
    try:
        return typing.cast(
            server_common.RequestId[list[responses.ManagedJobRecord] |
                                    tuple[list[responses.ManagedJobRecord], int,
                                          dict[str, int], int]],
            managed_jobs.queue_v2(
                refresh,
                skip_finished,
                all_users,
                job_ids,
                limit,
                fields,
                statuses=statuses,
                submitted_after=submitted_after,
                submitted_before=submitted_before)), QueueResultVersion.V2
    except exceptions.APINotSupportedError:
        if statuses is not None:
            logger.warning(
                'Filtering by status is not supported in your API server. '
                'Please upgrade to a newer API server to use --status. '
                'Showing all jobs.')
        if submitted_after is not None or submitted_before is not None:
            logger.warning(
                'Filtering by submission time is not supported in your API '
                'server. Please upgrade to a newer API server to use '
                '--since/--after/--before. Showing all jobs.')
        return managed_jobs.queue(refresh, skip_finished, all_users,
                                  job_ids), QueueResultVersion.V1
