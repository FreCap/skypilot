"""Shared managed-jobs naming helpers."""

from sky.jobs import constants as managed_job_constants
from sky.utils import common_utils


def generate_managed_job_cluster_name(task_name: str, job_id: int) -> str:
    """Generate managed job cluster name."""
    # Truncate the task name to 30 chars to avoid the cluster name being too
    # long after appending the job id, which will cause another truncation in
    # the underlying sky.launch, hiding the `job_id` in the cluster name.
    cluster_name = common_utils.make_cluster_name_on_cloud(
        task_name,
        managed_job_constants.JOBS_CLUSTER_NAME_PREFIX_LENGTH,
        add_user_hash=False)
    return f'{cluster_name}-{job_id}'
