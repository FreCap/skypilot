"""Managed-job queue transport for Kubernetes controller pods."""

from typing import Any

from sky import exceptions
from sky import provision as provision_lib
from sky.jobs import utils as managed_job_utils
from sky.provision import common as provision_common
from sky.utils import subprocess_utils

MANAGED_JOB_FIELDS_FOR_QUEUE_KUBERNETES = [
    'job_id',
    'task_id',
    'workspace',
    'job_name',
    'task_name',
    'resources',
    'submitted_at',
    'end_at',
    'job_duration',
    'recovery_count',
    'status',
    'pool',
    'current_cluster_name',
    'job_id_on_pool_cluster',
    'start_at',
    'infra',
    'cloud',
    'region',
    'zone',
    'cluster_resources',
    'schedule_state',
    'details',
    'failure_reason',
    'metadata',
    'user_name',
    'user_hash',
]


def queue_from_kubernetes_pod(
        pod_name: str,
        context: str | None = None,
        skip_finished: bool = False) -> list[dict[str, Any]]:
    """Gets the jobs queue from a specific controller pod.

    Args:
        pod_name (str): The name of the controller pod to query for jobs.
        context (Optional[str]): The Kubernetes context to use. If None, the
            current context is used.
        skip_finished (bool): If True, does not return finished jobs.

    Returns:
        [
            {
                'job_id': int,
                'job_name': str,
                'resources': str,
                'submitted_at': (float) timestamp of submission,
                'end_at': (float) timestamp of end,
                'duration': (float) duration in seconds,
                'recovery_count': (int) Number of retries,
                'status': (sky.jobs.ManagedJobStatus) of the job,
                'cluster_resources': (str) resources of the cluster,
                'region': (str) region of the cluster,
            }
        ]

    Raises:
        RuntimeError: If there's an error fetching the managed jobs.
    """
    # Create dummy cluster info to get the command runner.
    provider_config = {'context': context}
    instances = {
        pod_name: [
            provision_common.InstanceInfo(instance_id=pod_name,
                                          internal_ip='',
                                          external_ip='',
                                          tags={})
        ]
    }  # Internal IP is not required for Kubernetes
    cluster_info = provision_common.ClusterInfo(provider_name='kubernetes',
                                                head_instance_id=pod_name,
                                                provider_config=provider_config,
                                                instances=instances)
    managed_jobs_runner = provision_lib.get_command_runners(
        'kubernetes', cluster_info)[0]

    code = managed_job_utils.ManagedJobCodeGen.get_job_table(
        skip_finished=skip_finished,
        fields=MANAGED_JOB_FIELDS_FOR_QUEUE_KUBERNETES)
    returncode, job_table_payload, stderr = managed_jobs_runner.run(
        code,
        require_outputs=True,
        separate_stderr=True,
        stream_logs=False,
    )
    try:
        subprocess_utils.handle_returncode(returncode,
                                           code,
                                           'Failed to fetch managed jobs',
                                           job_table_payload + stderr,
                                           stream_logs=False)
    except exceptions.CommandError as e:
        raise RuntimeError(str(e)) from e

    jobs, _, result_type, _, _ = managed_job_utils.load_managed_job_queue(
        job_table_payload)

    if result_type == managed_job_utils.ManagedJobQueueResultType.DICT:
        return jobs

    # Backward compatibility for old jobs controller without filtering
    # TODO(hailong): remove this after 0.12.0
    if skip_finished:
        # Filter out the finished jobs. If a multi-task job is partially
        # finished, we will include all its tasks.
        non_finished_tasks = list(
            filter(lambda job: not job['status'].is_terminal(), jobs))
        non_finished_job_ids = {job['job_id'] for job in non_finished_tasks}
        jobs = list(
            filter(lambda job: job['job_id'] in non_finished_job_ids, jobs))
    return jobs
