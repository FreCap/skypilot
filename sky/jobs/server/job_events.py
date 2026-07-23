"""Managed-job event timeline projection."""

import datetime
from typing import Any

from sky import global_user_state
from sky import sky_logging
from sky.jobs import state as managed_job_state
from sky.jobs import utils as managed_job_utils

# Preserve the historical logger name used before this implementation moved
# behind sky.jobs.server.core.get_job_events.
logger = sky_logging.init_logger('sky.jobs.server.core')


def _get_job_cluster_names(job_id: int,
                           task_id: int | None = None) -> list[str]:
    """Reconstruct the underlying cluster name(s) for a managed job.

    Mirrors the derivation used by the controller (see ``jobs/controller.py``):
    a non-pool task's cluster is named deterministically from the *task* name
    (``task.name``) and the job id. The task name is the right key here: for a
    multi-task pipeline the job-level (DAG) name is shared across tasks, but
    each task launches its own cluster named from ``task.name``
    (``dag_utils`` sets ``task.name = f'{dag.name}-{task_id}'``). Pool tasks are
    skipped, since their cluster is shared across jobs and its events are not
    attributable to a single job.

    Returns a de-duplicated list of cluster names (a multi-task pipeline uses
    one cluster per task).
    """
    cluster_names: list[str] = []
    for task in managed_job_state.get_job_event_task_contexts(job_id):
        if task_id is not None and task.get('task_id') != task_id:
            continue
        if task.get('pool') is not None:
            continue
        # 'task_name' is the per-task name (spot.task_name); 'job_name' is the
        # job-level/DAG name, which is shared across a pipeline's tasks and so
        # would reconstruct the wrong cluster name for multi-task jobs.
        task_name = task.get('task_name')
        if not task_name:
            continue
        cluster_names.append(
            managed_job_utils.generate_managed_job_cluster_name(
                task_name, job_id))
    # De-duplicate while preserving order.
    return list(dict.fromkeys(cluster_names))


def get_job_events(
    job_id: int,
    task_id: int | None = None,
    limit: int | None = 10,
    include_cluster_events: bool = False,
) -> list[dict[str, Any]]:
    """Build a managed-job event timeline, optionally with cluster events."""
    events = managed_job_state.get_job_events(job_id=job_id,
                                              task_id=task_id,
                                              limit=limit)
    if not include_cluster_events:
        return events

    try:
        cluster_names = _get_job_cluster_names(job_id, task_id)
    except Exception as e:  # pylint: disable=broad-except
        # The merge is best-effort: never fail the job-events request because
        # the cluster name(s) could not be reconstructed.
        logger.debug(f'Failed to resolve cluster name(s) for job {job_id}: {e}')
        return events
    if not cluster_names:
        return events

    # STATUS_CHANGE carries the launch/setup milestone sequence (provisioning,
    # runtime setup, file-mount syncing, ...); LAUNCH_PROGRESS carries the
    # finer-grained sub-status (e.g. pods pending due to image pulling).
    event_types = [
        global_user_state.ClusterEventType.STATUS_CHANGE,
        global_user_state.ClusterEventType.LAUNCH_PROGRESS,
    ]
    try:
        cluster_events = global_user_state.get_cluster_events_by_names(
            cluster_names, event_types, limit=limit)
    except Exception as e:  # pylint: disable=broad-except
        # The merge is best-effort: never fail the job-events request because
        # cluster events could not be read.
        logger.debug(f'Failed to read cluster events for job {job_id}: {e}')
        cluster_events = []

    # Match the timezone of the existing job-event timestamps so the merged
    # cluster events serialize consistently. Postgres returns tz-aware
    # datetimes while SQLite returns naive ones; mixing the two in one list
    # makes the client interpret some timestamps in the wrong timezone.
    # transitioned_at is a UTC epoch, so fromtimestamp(tz=...) yields the
    # correct instant in whichever timezone the job events use.
    tz = events[0]['timestamp'].tzinfo if events else None
    for cluster_event in cluster_events:
        events.append({
            'spot_job_id': job_id,
            'task_id': None,
            # These happen while the job is launching its cluster.
            'new_status': managed_job_state.ManagedJobStatus.STARTING,
            'code': None,
            'reason': cluster_event['reason'],
            'timestamp': datetime.datetime.fromtimestamp(
                cluster_event['transitioned_at'], tz=tz),
        })

    # Every event's 'timestamp' is a datetime (job events from the DB, cluster
    # events converted above). datetime.timestamp() gives a comparable epoch.
    events.sort(key=lambda event: event['timestamp'].timestamp(), reverse=True)
    if limit is not None:
        events = events[:limit]
    return events
